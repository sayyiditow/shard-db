/* Task 3f — request-level commit batching (two-epoch waves, per-shard
 * writer gates). docs/plans/2026-09-05-request-level-commit-batching.md.
 *
 * Red on base: SlotcaskBulkShardInput / slotcask_bulk_request_execute /
 * SHARD_TEST_PHASE_REQ_PUBLISHED / the state-bearing hook signatures do
 * not exist yet (compile-red).
 *
 * Covers:
 *   1. multi-window retention via the req-published pause (marker files
 *      only — no same-shard mutation while the coordinator holds the
 *      gate),
 *   2. payload-flush failure → -1, no markers, nothing committed,
 *   3. commit-flush failure → EINPROGRESS, retained markers, gate replay
 *      (golden) / fail-closed (corrupt),
 *   4. D5: M-phase fallback (OLD-derived) payloads sync before the
 *      marker and survive gate replay,
 *   5. post-M failure semantics (finalize retry converges; both attempts
 *      failing retains the marker and reports EINPROGRESS),
 *   6. concurrency: disjoint-shard overlap, shared-shard serialization,
 *      touched/untouched admission table, reader visibility,
 *   7. inline/limited-pool execution (nested + 2-worker pool, 8 shards).
 */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "shard_test_ctl.h"

#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);

#define RF_SPLITS 8

/* ── shared scaffold (mirrors test-commit-phase-metrics) ─────────────── */

static void rf_cleanup_dir(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    system(cmd);
}

typedef struct {
    SlotcaskDb db;
    char base[PATH_MAX];
} RfDb;

static int rf_db_open(RfDb *w) {
    slotcask_init(64, 64);
    char b[] = "/tmp/shard-db-req-flush-XXXXXX";
    if (!mkdtemp(b)) return -1;
    snprintf(w->base, sizeof(w->base), "%s", b);
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", w->base);
    snprintf(k, sizeof(k), "%s/data/kf", w->base);
    if (mkdir(d, 0755) != 0 || mkdir(k, 0755) != 0) return -1;
    memset(&w->db, 0, sizeof(w->db));
    if (slotcask_open(&w->db, w->base, RF_SPLITS, 1, 64) != 0) return -1;
    w->db.bulk_commit_window = 16;
    shard_test_ctl_reset();
    return 0;
}

static void rf_db_close(RfDb *w) {
    slotcask_close(&w->db);
    rf_cleanup_dir(w->base);
    slotcask_shutdown();
    shard_test_ctl_reset();
}

/* rt_marker_scan lifted verbatim from test_window_release_routes.c. */
static int rt_marker_scan(const char *base) {
    char kdir[PATH_MAX];
    snprintf(kdir, sizeof(kdir), "%s/data/kf", base);
    DIR *d = opendir(kdir);
    if (!d) return -1;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        size_t L = strlen(e->d_name);
        if (L > 11 && strcmp(e->d_name + L - 11, "_marker.dat") == 0) n++;
    }
    closedir(d);
    return n;
}

/* Collect full marker paths (exact-path identity — names are never
 * reconstructed from shard/batch). */
static int rf_marker_paths(const char *base, char paths[][PATH_MAX], int max) {
    char kdir[PATH_MAX];
    snprintf(kdir, sizeof(kdir), "%s/data/kf", base);
    DIR *d = opendir(kdir);
    if (!d) return -1;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        size_t L = strlen(e->d_name);
        if (L > 11 && strcmp(e->d_name + L - 11, "_marker.dat") == 0) {
            if (n < max)
                snprintf(paths[n], PATH_MAX, "%s/%s", kdir, e->d_name);
            n++;
        }
    }
    closedir(d);
    return n;
}

static int rf_shard_of(const char *key) {
    uint8_t h[16];
    compute_hash_raw(key, strlen(key), h);
    return compute_record_shard(h, RF_SPLITS);
}

/* Fill `out` with a key that hashes to shard 0 (probed suffix). */
static void rf_key_shard0(const char *prefix, int i, char *out, size_t cap) {
    int attempt = 0;
    for (;; attempt++) {
        snprintf(out, cap, "%s-%d-%d", prefix, i, attempt);
        if (rf_shard_of(out) == 0) return;
    }
}

/* ── window hooks ────────────────────────────────────────────────────── */

static int rf_noop_prepare(SlotcaskBulkRec *recs, const size_t *active,
                           size_t nactive, void *ctx,
                           void **out_window_state) {
    (void)recs; (void)active; (void)nactive; (void)ctx;
    *out_window_state = NULL;
    return 0;
}
static int rf_noop_apply(SlotcaskBulkRec *recs, const size_t *active,
                         size_t nactive, void *ctx, void *window_state) {
    (void)recs; (void)active; (void)nactive; (void)ctx;
    (void)window_state;
    return 0;
}
static void rf_noop_terminal(void *ctx, void *window_state) {
    (void)ctx; (void)window_state;
}

/* Counting apply hook: fails its first call, succeeds afterwards
 * (finalize's idempotent retry must converge) — or always fails. */
typedef struct { int calls; int fail_first; int fail_all; } RfApplyCtl;
static int rf_ctl_apply(SlotcaskBulkRec *recs, const size_t *active,
                        size_t nactive, void *ctx, void *window_state) {
    (void)recs; (void)active; (void)nactive; (void)window_state;
    RfApplyCtl *c = ctx;
    c->calls++;
    if (c->fail_all) return -1;
    if (c->fail_first && c->calls == 1) return -1;
    return 0;
}

/* value_compute that rewrites the payload (OLD-derived branch → M-phase
 * fallback staging → D5 pre-marker sync). Stamps 'D5' at the start of
 * the caller-provided scratch buffer. */
static int rf_d5_value_compute(const SlotcaskOldRecord *old,
                               SlotcaskBulkRec *rec) {
    (void)old;
    if (rec->vlen < 3) return -1;
    memcpy((void *)rec->value, "D5", 2);
    return 0;
}

/* ── batch + request drivers ─────────────────────────────────────────── */

typedef struct {
    char (*keys)[24];
    char (*vals)[24];
    SlotcaskBulkRec *recs;
    SlotcaskBulkRec *perm;   /* per-batch permutation target — must stay
                                 alive for the whole request: inputs point
                                 into it, including while the request is
                                 parked at the req-published pause */
    size_t n;
} RfBatch;

/* Fill n records; every key hashes to want_shard (probed suffix), or
 * spreads across shards i % RF_SPLITS when want_shard < 0. */
static void rf_batch_fill(RfBatch *b, int prefix, size_t n,
                          const char *val, int want_shard) {
    for (size_t i = 0; i < n; i++) {
        int shard = want_shard >= 0 ? want_shard : (int)(i % RF_SPLITS);
        int attempt = 0;
        for (;; attempt++) {
            snprintf(b->keys[i], sizeof(b->keys[i]), "rf-%d-%04zu-%d",
                     prefix, i, attempt);
            if (rf_shard_of(b->keys[i]) == shard) break;
        }
        snprintf(b->vals[i], sizeof(b->vals[i]), "%s-%04zu", val, i);
        memset(&b->recs[i], 0, sizeof(b->recs[i]));
        b->recs[i].key = b->keys[i];
        b->recs[i].klen = strlen(b->keys[i]);
        b->recs[i].value = b->vals[i];
        b->recs[i].vlen = strlen(b->vals[i]);
    }
    b->n = n;
}

static void rf_fill_opts(SlotcaskBulkOpts *o, RfApplyCtl *ctl) {
    memset(o, 0, sizeof(*o));
    o->has_indexed_fields = 1;
    o->prepare_window = rf_noop_prepare;
    o->apply_window = ctl ? rf_ctl_apply : rf_noop_apply;
    o->commit_done = rf_noop_terminal;
    o->release_window = rf_noop_terminal;
    o->abort_window = rf_noop_terminal;
    o->bulk_hook_ctx = ctl;
}

/* Build one input per touched shard (records bucketed by their real kf
 * shard, ascending) and run one deferred request. */
static int rf_run_request(RfDb *w, RfBatch *b, SlotcaskBulkOpts *opts) {
    SlotcaskBulkShardInput inputs[RF_SPLITS];
    size_t ninputs = 0;
    size_t bucket_start[RF_SPLITS], bucket_n[RF_SPLITS];
    for (int s = 0; s < RF_SPLITS; s++) bucket_n[s] = 0;
    for (size_t i = 0; i < b->n; i++) bucket_n[rf_shard_of(b->keys[i])]++;
    size_t run = 0;
    for (int s = 0; s < RF_SPLITS; s++) {
        bucket_start[s] = run;
        if (bucket_n[s] == 0) continue;
        /* Records are not contiguous per shard when want_shard < 0;
           permute a copy so each input's recs slice is one shard's. */
        run += bucket_n[s];
    }
    if (run != b->n) return -1;
    /* Stable partition into the batch's OWN permutation buffer: inputs
       point into it for the request's whole life (including while parked
       at the pause), so it must never be shared between requests. */
    if (!b->perm) return -1;
    size_t cursors[RF_SPLITS];
    for (int s = 0; s < RF_SPLITS; s++) cursors[s] = bucket_start[s];
    for (size_t i = 0; i < b->n; i++) {
        int s = rf_shard_of(b->keys[i]);
        b->perm[cursors[s]++] = b->recs[i];
    }
    for (int s = 0; s < RF_SPLITS; s++) {
        if (bucket_n[s] == 0) continue;
        inputs[ninputs].kf_shard_id = s;
        inputs[ninputs].recs = b->perm + bucket_start[s];
        inputs[ninputs].nrecs = bucket_n[s];
        inputs[ninputs].kind = SLOTCASK_BULK_INPUT_UPSERT;
        inputs[ninputs].opts.upsert = *opts;
        inputs[ninputs].rc = 0;
        ninputs++;
    }
    return slotcask_bulk_request_execute(&w->db, inputs, ninputs);
}

static int rf_record_visible(RfDb *w, const char *key, const char *want) {
    void *v = NULL; size_t vl = 0;
    int rc = slotcask_get(&w->db, key, strlen(key), &v, &vl);
    if (rc != 0) return 0;
    int ok = vl == strlen(want) && memcmp(v, want, vl) == 0;
    free(v);
    return ok;
}

static void rf_wait_pause_hit(void) {
    for (int i = 0; i < 30000 && atomic_load(&g_shard_test_pause_hits) == 0; i++)
        usleep(1000);
}
static void rf_release_pause(void) {
    atomic_store(&g_shard_test_pause_release, 1);
}

typedef struct {
    RfDb *w;
    RfBatch *b;
    int done;
    int rc;
} RfReqThr;

static void *rf_req_thread(void *raw) {
    RfReqThr *a = raw;
    SlotcaskBulkOpts opts;
    rf_fill_opts(&opts, NULL);
    a->rc = rf_run_request(a->w, a->b, &opts);
    a->done = 1;
    return NULL;
}

typedef struct {
    RfDb *w;
    const char *key;
    const char *val;
    int kind;             /* 0 upsert, 1 insert, 2 delete, 3 bulk-upsert,
                             4 bulk-delete, 5 pregrow */
    int shard;
    int done;
    int rc;
} RfWriterThr;

static void *rf_writer_thread(void *raw) {
    RfWriterThr *a = raw;
    if (a->kind == 0 || a->kind == 1) {
        SlotcaskUpsertOpts so;
        memset(&so, 0, sizeof(so));
        so.if_not_exists = (a->kind == 1);
        a->rc = slotcask_upsert_with_hooks(&a->w->db, 0, a->key,
                                           strlen(a->key), a->val,
                                           strlen(a->val), &so, NULL);
    } else if (a->kind == 2) {
        SlotcaskDeleteOpts dopt;
        memset(&dopt, 0, sizeof(dopt));
        a->rc = slotcask_delete_with_hooks(&a->w->db, a->key,
                                           strlen(a->key), &dopt, NULL);
    } else if (a->kind == 3 || a->kind == 4) {
        SlotcaskBulkRec rec;
        memset(&rec, 0, sizeof(rec));
        rec.key = a->key; rec.klen = strlen(a->key);
        rec.value = a->val; rec.vlen = a->val ? strlen(a->val) : 0;
        if (a->kind == 3) {
            SlotcaskBulkOpts o;
            memset(&o, 0, sizeof(o));
            a->rc = slotcask_bulk_upsert_in_kfshard(&a->w->db, a->shard,
                                                    &rec, 1, &o);
        } else {
            SlotcaskBulkDeleteOpts o;
            memset(&o, 0, sizeof(o));
            a->rc = slotcask_bulk_delete_in_kfshard(&a->w->db, a->shard,
                                                    &rec, 1, &o);
        }
    } else {
        a->rc = slotcask_pregrow_kf(&a->w->db, 64);
    }
    a->done = 1;
    return NULL;
}

/* Flip one bit inside slot 0's checksum-covered region (new_offset),
 * leaving the stored checksum untouched → the reader must fail closed. */
static int rf_flip_bit_bad_checksum(const char *path) {
    int fd = open(path, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return -1;
    size_t off = 16 + offsetof(KfMarkerSlot, new_offset);
    uint8_t byte;
    ssize_t nr = pread(fd, &byte, 1, (off_t)off);
    if (nr != 1) { close(fd); return -1; }
    byte ^= 0x01;
    ssize_t nw = pwrite(fd, &byte, 1, (off_t)off);
    int rc = (nw == 1 && fsync(fd) == 0) ? 0 : -1;
    close(fd);
    return rc;
}

/* Waits up to ms for *flag != 0. Returns 1 when the flag set. */
static int rf_wait_flag(const int *flag, int ms) {
    for (int i = 0; i < ms; i++) {
        if (__atomic_load_n(flag, __ATOMIC_SEQ_CST)) return 1;
        usleep(1000);
    }
    return 0;
}

/* Nested-task body for scenario 7a (named function — C, not a lambda). */
typedef struct { RfDb *w; RfBatch *b; int done; int rc; } RfNest;
static void *rf_nested_req_task(void *raw) {
    RfNest *n = raw;
    SlotcaskBulkOpts opts;
    rf_fill_opts(&opts, NULL);
    n->rc = rf_run_request(n->w, n->b, &opts);
    n->done = 1;
    return NULL;
}

/* ══════════════════════════════════════════════════════════════════════ */

static int test_request_flush_batching_run(void) {
    RfDb w;
    ASSERT_EQ_INT(rf_db_open(&w), 0, "open request-flush db");
    if (t_ctx->failed) return 1;

    /* ── Scenario 1: multi-window retention via the req-published pause ── */
    {
        static RfBatch b;
        static char keys[40][24], vals[40][24];
        static SlotcaskBulkRec recs[40];
        static SlotcaskBulkRec perm0[40];
        b.keys = keys; b.vals = vals; b.recs = recs; b.perm = perm0;
        rf_batch_fill(&b, 0, 40, "v0", 0);

        uint64_t w0 = __atomic_load_n(&g_db->commit_windows_total,
                                      __ATOMIC_RELAXED);
        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_REQ_PUBLISHED;
        g_shard_test_pause_occurrence = 1;

        RfReqThr ta = { .w = &w, .b = &b, .done = 0, .rc = 0 };
        pthread_t th;
        ASSERT_EQ_INT(pthread_create(&th, NULL, rf_req_thread, &ta), 0,
                      "spawn request thread");
        rf_wait_pause_hit();
        ASSERT_TRUE(atomic_load(&g_shard_test_pause_hits) >= 1,
                    "req-published pause hit");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 3,
                      "3 windows published at pause");
        rf_release_pause();
        pthread_join(th, NULL);
        ASSERT_EQ_INT(ta.rc, 0, "deferred request rc 0");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "markers cleared after join");

        uint64_t w1 = __atomic_load_n(&g_db->commit_windows_total,
                                      __ATOMIC_RELAXED);
        ASSERT_TRUE(w1 == w0 + 3, "commit_windows_total +3");
        int visible = 1;
        for (size_t i = 0; i < b.n && visible; i++)
            visible = rf_record_visible(&w, b.keys[i], b.vals[i]);
        ASSERT_TRUE(visible, "all records readable after join");
    }

    /* ── Scenario 2: payload-flush failure → no markers, not committed ── */
    {
        static RfBatch b;
        static char keys[8][24], vals[8][24];
        static SlotcaskBulkRec recs[8];
        static SlotcaskBulkRec perm1[8];
        b.keys = keys; b.vals = vals; b.recs = recs; b.perm = perm1;
        rf_batch_fill(&b, 1, 8, "v1", 0);

        shard_test_ctl_reset();
        g_shard_test_fail_phase = SHARD_TEST_PHASE_P;
        g_shard_test_fail_occurrence = 1;

        SlotcaskBulkOpts opts;
        rf_fill_opts(&opts, NULL);
        ASSERT_EQ_INT(rf_run_request(&w, &b, &opts), -1,
                      "payload flush failure rc");
        g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "no markers after P failure");
        ASSERT_TRUE(!rf_record_visible(&w, b.keys[0], b.vals[0]),
                     "record not committed after P failure");
        ASSERT_EQ_INT(b.perm[0].status, -1,
                      "record status -1 after P failure");
    }

    /* ── Scenario 3: commit-flush failure → retained → gate replay ── */
    {
        static RfBatch b;
        static char keys[8][24], vals[8][24];
        static SlotcaskBulkRec recs[8];
        static SlotcaskBulkRec perm2[8];
        b.keys = keys; b.vals = vals; b.recs = recs; b.perm = perm2;
        rf_batch_fill(&b, 2, 8, "v2", 0);

        SlotcaskBulkOpts opts;
        rf_fill_opts(&opts, NULL);

        shard_test_ctl_reset();
        g_shard_test_fail_phase = SHARD_TEST_PHASE_K;
        g_shard_test_fail_occurrence = 1;
        g_shard_test_fail_sticky = 1;
        ASSERT_EQ_INT(rf_run_request(&w, &b, &opts), -1,
                      "commit-flush failure rc");
        ASSERT_EQ_INT(errno, EINPROGRESS, "errno EINPROGRESS");
        g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
        g_shard_test_fail_sticky = 0;
        ASSERT_EQ_INT(rt_marker_scan(w.base), 1, "marker retained");

        /* Golden follow-up single-record write on the shard: the gate
           replays the retained marker, converges, and clears it. */
        char fk[24], fv[24];
        rf_key_shard0("rf-follow", 2, fk, sizeof(fk));
        snprintf(fv, sizeof(fv), "follow");
        SlotcaskUpsertOpts so;
        memset(&so, 0, sizeof(so));
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, fk, strlen(fk),
                                                 fv, strlen(fv), &so, NULL),
                      0, "golden follow-up write succeeds");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "retained marker cleared");
        ASSERT_TRUE(rf_record_visible(&w, b.keys[0], b.vals[0]),
                    "replayed record visible");

        /* Corrupt-marker case: another retained marker, corrupted via the
           exact-path test accessor → the follow-up write fails closed and
           the marker stays. */
        shard_test_ctl_reset();
        g_shard_test_fail_phase = SHARD_TEST_PHASE_K;
        g_shard_test_fail_occurrence = 1;
        g_shard_test_fail_sticky = 1;
        static RfBatch b2;
        static char keys2[4][24], vals2[4][24];
        static SlotcaskBulkRec recs2[4];
        static SlotcaskBulkRec perm3[4];
        b2.keys = keys2; b2.vals = vals2; b2.recs = recs2; b2.perm = perm3;
        rf_batch_fill(&b2, 3, 4, "v3", 0);
        ASSERT_EQ_INT(rf_run_request(&w, &b2, &opts), -1,
                      "second commit-flush failure rc");
        g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
        g_shard_test_fail_sticky = 0;
        char paths[4][PATH_MAX];
        ASSERT_EQ_INT(rf_marker_paths(w.base, paths, 4), 1, "one retained");
        ASSERT_EQ_INT(rf_flip_bit_bad_checksum(paths[0]), 0,
                      "flip checksum-covered byte");
        rf_key_shard0("rf-follow", 3, fk, sizeof(fk));
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, fk, strlen(fk),
                                                 fv, strlen(fv), &so, NULL),
                      -1, "corrupt marker: follow-up fails closed");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 1,
                      "corrupt marker retained");

        /* Operator recovery: a corrupt marker fails closed forever — the
           documented action is to investigate and remove it. Do that here
           so later scenarios start clean. */
        ASSERT_EQ_INT(unlink(paths[0]), 0, "operator removes corrupt marker");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "clean after recovery");
    }

    /* ── Scenario 4: D5 — OLD-derived (computed) payloads survive replay ── */
    {
        static RfBatch b;
        static char keys[4][24], vals[4][24];
        static SlotcaskBulkRec recs[4];
        static SlotcaskBulkRec perm4[4];
        b.keys = keys; b.vals = vals; b.recs = recs; b.perm = perm4;
        rf_batch_fill(&b, 4, 4, "v4", 0);

        shard_test_ctl_reset();
        g_shard_test_fail_phase = SHARD_TEST_PHASE_K;
        g_shard_test_fail_occurrence = 1;
        g_shard_test_fail_sticky = 1;
        SlotcaskBulkOpts opts;
        rf_fill_opts(&opts, NULL);
        opts.value_compute = rf_d5_value_compute;
        opts.value_rewrites_payload = 1;
        ASSERT_EQ_INT(rf_run_request(&w, &b, &opts), -1,
                      "D5 request commit-flush failure rc");
        g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
        g_shard_test_fail_sticky = 0;

        /* Follow-up write drives gate replay; the computed payload must
           surface intact. */
        char fk[24], fv[24];
        rf_key_shard0("rf-follow", 4, fk, sizeof(fk));
        snprintf(fv, sizeof(fv), "follow4");
        SlotcaskUpsertOpts so;
        memset(&so, 0, sizeof(so));
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, fk, strlen(fk),
                                                 fv, strlen(fv), &so, NULL),
                      0, "D5 follow-up write succeeds");
        void *v = NULL; size_t vl = 0;
        ASSERT_EQ_INT(slotcask_get(&w.db, b.keys[0], strlen(b.keys[0]),
                                   &v, &vl), 0, "D5 record visible");
        ASSERT_TRUE(vl >= 2 && v && memcmp(v, "D5", 2) == 0,
                    "computed payload intact after replay");
        free(v);
    }

    /* ── Scenario 5: post-M failure semantics (finalize retry) ── */
    {
        /* 5a: first finalize attempt fails, idempotent retry converges —
           request reports success, marker cleared. */
        shard_test_ctl_reset();
        char mpaths0[8][PATH_MAX];
        ASSERT_EQ_INT(rf_marker_paths(w.base, mpaths0, 8), 0,
                      "5a starts with no retained markers");
        static RfBatch b;
        static char keys[4][24], vals[4][24];
        static SlotcaskBulkRec recs[4];
        static SlotcaskBulkRec perm5[4];
        b.keys = keys; b.vals = vals; b.recs = recs; b.perm = perm5;
        rf_batch_fill(&b, 5, 4, "v5", 0);

        RfApplyCtl ctl;
        memset(&ctl, 0, sizeof(ctl));
        ctl.fail_first = 1;
        SlotcaskBulkOpts opts;
        rf_fill_opts(&opts, &ctl);
        ASSERT_EQ_INT(rf_run_request(&w, &b, &opts), 0,
                      "finalize retry converges");
        ASSERT_TRUE(ctl.calls >= 2, "apply retried");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "marker cleared after retry");

        /* 5b: both attempts fail → EINPROGRESS, marker retained,
           input rc -2. */
        shard_test_ctl_reset();
        memset(&ctl, 0, sizeof(ctl));
        ctl.fail_all = 1;
        static RfBatch b2;
        static char keys2[4][24], vals2[4][24];
        static SlotcaskBulkRec recs2[4];
        b2.keys = keys2; b2.vals = vals2; b2.recs = recs2;
        rf_batch_fill(&b2, 6, 4, "v6", 0);
        SlotcaskBulkOpts opts2;
        rf_fill_opts(&opts2, &ctl);
        SlotcaskBulkShardInput in;
        memset(&in, 0, sizeof(in));
        in.kf_shard_id = 0;
        in.recs = b2.recs; in.nrecs = b2.n;
        in.kind = SLOTCASK_BULK_INPUT_UPSERT;
        in.opts.upsert = opts2;
        ASSERT_EQ_INT(slotcask_bulk_request_execute(&w.db, &in, 1), -1,
                      "both finalize attempts fail rc");
        ASSERT_EQ_INT(errno, EINPROGRESS, "both-fail errno EINPROGRESS");
        ASSERT_EQ_INT(in.rc, -2, "retained window reports rc -2");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 1,
                      "marker retained after both fail");

        /* Cleanup for later scenarios: replay the retained marker. */
        char fk[24], fv[24];
        rf_key_shard0("rf-follow", 5, fk, sizeof(fk));
        snprintf(fv, sizeof(fv), "follow5");
        SlotcaskUpsertOpts so;
        memset(&so, 0, sizeof(so));
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, fk, strlen(fk),
                                                 fv, strlen(fv), &so, NULL),
                      0, "5b follow-up replay converges");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "5b marker cleared");
    }

    /* ── Scenario 6: concurrency (disjoint overlap, shared-shard gate,
     *    admission table, reader visibility) ── */
    {
        static RfBatch ba;
        static char keysa[8][24], valsa[8][24];
        static SlotcaskBulkRec recsa[8];
        static SlotcaskBulkRec perm7[8];
        ba.keys = keysa; ba.vals = valsa; ba.recs = recsa; ba.perm = perm7;
        rf_batch_fill(&ba, 7, 8, "v7", 0);       /* all records on shard 0 */
        pthread_t tha, thb;

        /* 6a: request B on disjoint shard 1 completes while A is paused. */
        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_REQ_PUBLISHED;
        g_shard_test_pause_occurrence = 1;
        RfReqThr ta = { .w = &w, .b = &ba, .done = 0, .rc = 0 };
        ASSERT_EQ_INT(pthread_create(&tha, NULL, rf_req_thread, &ta), 0,
                      "spawn A");
        rf_wait_pause_hit();

        static RfBatch bb;
        static char keysb[8][24], valsb[8][24];
        static SlotcaskBulkRec recsb[8];
        bb.keys = keysb; bb.vals = valsb; bb.recs = recsb;
        rf_batch_fill(&bb, 8, 8, "v8", 1);       /* all records on shard 1 */
        SlotcaskBulkShardInput inb;
        memset(&inb, 0, sizeof(inb));
        inb.kf_shard_id = 1;
        inb.recs = bb.recs; inb.nrecs = bb.n;
        inb.kind = SLOTCASK_BULK_INPUT_UPSERT;
        SlotcaskBulkOpts ob;
        rf_fill_opts(&ob, NULL);
        inb.opts.upsert = ob;
        ASSERT_EQ_INT(slotcask_bulk_request_execute(&w.db, &inb, 1), 0,
                      "B on disjoint shard completes while A paused");
        ASSERT_TRUE(rf_record_visible(&w, bb.keys[0], bb.vals[0]),
                    "B's record readable while A paused");
        rf_release_pause();
        pthread_join(tha, NULL);
        ASSERT_EQ_INT(ta.rc, 0, "A converges after release");

        /* 6b: a request sharing shard 0 serializes on the gate. */
        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_REQ_PUBLISHED;
        g_shard_test_pause_occurrence = 1;
        static RfBatch bc;
        static char keysc[8][24], valsc[8][24];
        static SlotcaskBulkRec recsc[8];
        static SlotcaskBulkRec perm9[8];
        bc.keys = keysc; bc.vals = valsc; bc.recs = recsc; bc.perm = perm9;
        rf_batch_fill(&bc, 9, 8, "v9", 0);
        RfReqThr ta2 = { .w = &w, .b = &ba, .done = 0, .rc = 0 };
        ASSERT_EQ_INT(pthread_create(&tha, NULL, rf_req_thread, &ta2), 0,
                      "spawn A (shard 0)");
        rf_wait_pause_hit();
        RfReqThr tb = { .w = &w, .b = &bc, .done = 0, .rc = 0 };
        ASSERT_EQ_INT(pthread_create(&thb, NULL, rf_req_thread, &tb), 0,
                      "spawn B (shard 0, must block on gate)");
        ASSERT_TRUE(!rf_wait_flag(&tb.done, 400),
                     "B does not complete its stage wave while A paused");
        rf_release_pause();
        pthread_join(tha, NULL);
        pthread_join(thb, NULL);
        ASSERT_EQ_INT(tb.rc, 0, "B completes after A ends");

        /* 6c/6d + admission table: ordinary writers to the touched shard
           queue on the gate; single upsert/insert/delete, both legacy
           bulk entries, and pregrow. */
        static const struct {
            const char *name;
            int kind;
        } apis[] = {
            { "single upsert", 0 },
            { "insert-only",   1 },
            { "single delete", 2 },
            { "legacy bulk upsert", 3 },
            { "legacy bulk delete", 4 },
            { "pregrow",       5 },
        };
        for (size_t ai = 0; ai < sizeof(apis) / sizeof(apis[0]); ai++) {
            char wk[24], wv[24];
            int attempt = 0;
            for (;; attempt++) {   /* probe the writer key onto shard 0 */
                snprintf(wk, sizeof(wk), "rf-writer-%02zu-%d", ai, attempt);
                if (rf_shard_of(wk) == 0) break;
            }
            snprintf(wv, sizeof(wv), "wv-%02zu", ai);
            if (apis[ai].kind == 2) {
                SlotcaskUpsertOpts sso;
                memset(&sso, 0, sizeof(sso));
                ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, wk,
                                                         strlen(wk), wv,
                                                         strlen(wv), &sso,
                                                         NULL), 0,
                              "seed key for delete admission");
            }
            shard_test_ctl_reset();
            g_shard_test_pause_phase = SHARD_TEST_PHASE_REQ_PUBLISHED;
            g_shard_test_pause_occurrence = 1;
            RfReqThr ta3 = { .w = &w, .b = &ba, .done = 0, .rc = 0 };
            ASSERT_EQ_INT(pthread_create(&tha, NULL, rf_req_thread, &ta3),
                          0, "spawn A for admission table");
            rf_wait_pause_hit();
            ASSERT_TRUE(atomic_load(&g_shard_test_pause_hits) >= 1,
                        "A paused inside the admission iteration");
            RfWriterThr wr = { .w = &w, .key = wk, .val = wv,
                               .kind = apis[ai].kind, .shard = 0,
                               .done = 0, .rc = 0 };
            ASSERT_EQ_INT(pthread_create(&thb, NULL, rf_writer_thread, &wr),
                          0, apis[ai].name);
            ASSERT_TRUE(!rf_wait_flag(&wr.done, 400),
                         "writer to touched shard queues on the gate");
            rf_release_pause();
            pthread_join(tha, NULL);
            pthread_join(thb, NULL);
            ASSERT_EQ_INT(wr.rc, 0, apis[ai].name);
        }

        /* 6e: readers between waves see only coherent old/new records. */
        shard_test_ctl_reset();
        char rk[24];
        int rk_shard = -1, rattempt = 0;
        for (;; rattempt++) {   /* probe a reader key onto a fixed shard */
            snprintf(rk, sizeof(rk), "rf-reader-%d", rattempt);
            rk_shard = rf_shard_of(rk);
            if (rk_shard >= 0) break;    /* any shard works; keep it */
        }
        SlotcaskUpsertOpts sso;
        memset(&sso, 0, sizeof(sso));
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, rk, strlen(rk),
                                                 "old", 3, &sso, NULL), 0,
                      "seed reader key");
        static RfBatch bd;
        static char keysd[4][24], valsd[4][24];
        static SlotcaskBulkRec recsd[4];
        static SlotcaskBulkRec perm10[4];
        bd.keys = keysd; bd.vals = valsd; bd.recs = recsd; bd.perm = perm10;
        rf_batch_fill(&bd, 10, 4, "v10", rk_shard);
        snprintf(bd.keys[0], sizeof(bd.keys[0]), "%s", rk);
        snprintf(bd.vals[0], sizeof(bd.vals[0]), "new");
        bd.recs[0].key = bd.keys[0]; bd.recs[0].klen = strlen(bd.keys[0]);
        bd.recs[0].value = bd.vals[0]; bd.recs[0].vlen = strlen(bd.vals[0]);
        g_shard_test_pause_phase = SHARD_TEST_PHASE_REQ_PUBLISHED;
        g_shard_test_pause_occurrence = 1;
        RfReqThr te = { .w = &w, .b = &bd, .done = 0, .rc = 0 };
        ASSERT_EQ_INT(pthread_create(&tha, NULL, rf_req_thread, &te), 0,
                      "spawn reader-visibility request");
        rf_wait_pause_hit();
        ASSERT_TRUE(rf_record_visible(&w, rk, "old"),
                    "between waves the reader sees the OLD record");
        rf_release_pause();
        pthread_join(tha, NULL);
        ASSERT_TRUE(rf_record_visible(&w, rk, "new"),
                    "after finalize the reader sees the NEW record");
    }

    /* ── Scenario 7: inline/limited-pool execution ── */
    {
        parallel_pool_init(2);

        static RfBatch b;
        static char keys[64][24], vals[64][24];
        static SlotcaskBulkRec recs[64];
        static SlotcaskBulkRec perm11[64];
        b.keys = keys; b.vals = vals; b.recs = recs; b.perm = perm11;
        rf_batch_fill(&b, 11, 64, "v11", -1);   /* 8 records × 8 shards */

        /* 7a: the request runs from inside an outer pool task with more
           shard inputs than workers — nested phases force inline and the
           request must complete without deadlock. */
        static RfNest nest;
        nest.w = &w; nest.b = &b; nest.done = 0; nest.rc = 0;
        parallel_for_io(rf_nested_req_task, &nest, 1, sizeof(nest));
        ASSERT_TRUE(nest.done, "nested request completed");
        ASSERT_EQ_INT(nest.rc, 0, "nested request rc 0");

        /* 7b: two-worker pool, eight shard inputs, phases join without
           one resident waiter per shard. */
        static RfBatch b2;
        static char keys2[64][24], vals2[64][24];
        static SlotcaskBulkRec recs2[64];
        static SlotcaskBulkRec perm12[64];
        b2.keys = keys2; b2.vals = vals2; b2.recs = recs2; b2.perm = perm12;
        rf_batch_fill(&b2, 12, 64, "v12", -1);
        SlotcaskBulkOpts opts;
        rf_fill_opts(&opts, NULL);
        ASSERT_EQ_INT(rf_run_request(&w, &b2, &opts), 0,
                      "8-shard request on a 2-worker pool");
        ASSERT_TRUE(rf_record_visible(&w, b2.keys[63], b2.vals[63]),
                    "last record of the 8-shard request visible");

        parallel_pool_shutdown();
    }

    rf_db_close(&w);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-request-flush-batching", test_request_flush_batching_run)
