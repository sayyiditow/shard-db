/* F9: the bulk planning gate reads ReqShard.res_count (a running count of
 * cross-window reservations) instead of scanning the reservation map.
 * Invariant I1: res_count == number of used buckets (0 when the map is NULL).
 * TEST_BUILD verifies it against a full scan at every window start, chain seed
 * and shard release (g_shard_test_res_checks / _nonempty / _mismatch).
 * bulk_commit_window = 16 makes each request span several windows, so later
 * windows plan while earlier windows' reservations are live; the chain cases
 * (shard 0 of 8 and shard 300 of 512) also exercise the seed, where a later
 * member's map is replaced by owner + own reservations. Runs in-process, own
 * tmpdir. */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "shard_test_ctl.h"

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);

#define BRC_TIER 64
#define BRC_MAXN 48

/* ── shared scaffold (mirrors test-request-flush-batching) ───────────── */

static void brc_rm_rf(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    system(cmd);
}

typedef struct {
    SlotcaskDb db;
    char base[PATH_MAX];
    int splits;
    int db_opened;
} Brc;

static void brc_ctl_reset(void) {
    shard_test_ctl_reset();
    atomic_store(&g_shard_test_kf_initial_slots, BRC_TIER);
}

static int brc_open(Brc *w, int splits, int window) {
    memset(w, 0, sizeof(*w));
    w->splits = splits;
    atomic_store(&g_shard_test_kf_initial_slots, BRC_TIER);
    slotcask_init(64, 64);
    char b[] = "/tmp/shard-db-res-count-XXXXXX";
    if (!mkdtemp(b)) return -1;
    snprintf(w->base, sizeof(w->base), "%s", b);
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", w->base);
    snprintf(k, sizeof(k), "%s/data/kf", w->base);
    if (mkdir(d, 0755) != 0 || mkdir(k, 0755) != 0) return -1;
    memset(&w->db, 0, sizeof(w->db));
    if (slotcask_open(&w->db, w->base, splits, 1, 64) != 0) return -1;
    w->db_opened = 1;
    /* window < batch size makes every request span several windows. */
    w->db.bulk_commit_window = window;
    brc_ctl_reset();
    return 0;
}

static void brc_close(Brc *w) {
    if (w->db_opened) slotcask_close(&w->db);
    brc_rm_rf(w->base);
    slotcask_shutdown();
    shard_test_ctl_reset();
}

static int brc_shard_of(Brc *w, const char *key) {
    uint8_t h[16];
    compute_hash_raw(key, strlen(key), h);
    return compute_record_shard(h, w->splits);
}

/* ── window hooks (copied from rf_noop_prepare/apply/terminal) ───────── */

static int brc_noop_prepare(SlotcaskBulkRec *recs, const size_t *active,
                            size_t nactive, void *ctx,
                            void **out_window_state) {
    (void)recs; (void)active; (void)nactive; (void)ctx;
    *out_window_state = NULL;
    return 0;
}
static int brc_noop_apply(SlotcaskBulkRec *recs, const size_t *active,
                          size_t nactive, void *ctx, void *window_state) {
    (void)recs; (void)active; (void)nactive; (void)ctx;
    (void)window_state;
    return 0;
}
static void brc_noop_terminal(void *ctx, void *window_state) {
    (void)ctx; (void)window_state;
}

static void brc_fill_opts(SlotcaskBulkOpts *o) {
    memset(o, 0, sizeof(*o));
    o->has_indexed_fields = 1;
    o->prepare_window = brc_noop_prepare;
    o->apply_window = brc_noop_apply;
    o->commit_done = brc_noop_terminal;
    o->release_window = brc_noop_terminal;
    o->abort_window = brc_noop_terminal;
}

/* ── batch + request drivers ─────────────────────────────────────────── */

typedef struct {
    char keys[BRC_MAXN][24];
    char vals[BRC_MAXN][24];
    SlotcaskBulkRec recs[BRC_MAXN];
    size_t n;
} BrcBatch;

/* Fill n records; every key hashes to `shard` (probed attempt suffix).
 * Keys are unique across batches via different `prefix`. Batch A uses val
 * "vA", batch B "vB". */
static void brc_batch_fill(Brc *w, BrcBatch *b, int prefix, size_t n,
                           int shard, const char *val) {
    for (size_t i = 0; i < n; i++) {
        int attempt = 0;
        for (;; attempt++) {
            snprintf(b->keys[i], sizeof(b->keys[i]), "bc-%d-%04zu-%d",
                     prefix, i, attempt);
            if (brc_shard_of(w, b->keys[i]) == shard) break;
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

typedef struct {
    Brc *w;
    SlotcaskBulkShardInput in;
    int rc;
    int err;
} BrcReq;

static void brc_req_init(BrcReq *r, Brc *w, BrcBatch *b, int shard) {
    memset(r, 0, sizeof(*r));
    r->w = w;
    r->in.kf_shard_id = shard;
    r->in.recs = b->recs;
    r->in.nrecs = b->n;
    r->in.kind = SLOTCASK_BULK_INPUT_UPSERT;
    brc_fill_opts(&r->in.opts.upsert);
    r->rc = 0;
}

static void *brc_req_thread(void *arg) {
    BrcReq *r = arg;
    errno = 0;
    r->rc = slotcask_bulk_request_execute(&r->w->db, &r->in, 1);
    r->err = errno;
    return NULL;
}

/* rt_marker_scan copied from test-request-flush-batching. */
static int brc_marker_scan(const char *base) {
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

static void brc_wait_hits(int n) {
    for (int i = 0; i < 30000 && atomic_load(&g_shard_test_pause_hits) < n; i++)
        usleep(1000);
}

/* rf_record_visible renamed: exact length + bytes match. */
static int brc_visible(Brc *w, const char *key, const char *want) {
    void *v = NULL; size_t vl = 0;
    int rc = slotcask_get(&w->db, key, strlen(key), &v, &vl);
    if (rc != 0) return 0;
    int ok = vl == strlen(want) && memcmp(v, want, vl) == 0;
    free(v);
    return ok;
}

#define BRC_WINDOW 16

/* Same choreography as test-bulk-chain-pregrow: owner parks at PRE_MERGE,
 * joiner queues, release -> one 2-member chain. Never re-zero
 * g_shard_test_pause_hits between the two waits. */
static int brc_chain(int splits, int shard, size_t na, size_t nb) {
    Brc *w = calloc(1, sizeof(*w));
    BrcBatch *ba = calloc(1, sizeof(*ba));
    BrcBatch *bb = calloc(1, sizeof(*bb));
    BrcReq *ra = calloc(1, sizeof(*ra));
    BrcReq *rb = calloc(1, sizeof(*rb));
    ASSERT_NOT_NULL(w, "alloc");
    ASSERT_NOT_NULL(ba, "alloc");
    ASSERT_NOT_NULL(bb, "alloc");
    ASSERT_NOT_NULL(ra, "alloc");
    ASSERT_NOT_NULL(rb, "alloc");
    if (!w || !ba || !bb || !ra || !rb) {
        free(ra); free(rb); free(ba); free(bb); free(w);
        return 1;
    }
    pthread_t tha = 0, thb = 0;
    if (brc_open(w, splits, BRC_WINDOW) != 0) {
        ASSERT_TRUE(0, "brc_open succeeds");
        brc_close(w);
        free(ra); free(rb); free(ba); free(bb); free(w);
        return 1;
    }
    brc_batch_fill(w, ba, 1, na, shard, "vA");
    brc_batch_fill(w, bb, 2, nb, shard, "vB");
    brc_ctl_reset();   /* resets the pause seams AND the F9 counters */
    g_shard_test_pause_phase = SHARD_TEST_PHASE_PRE_MERGE;
    g_shard_test_pause_occurrence = 1;
    brc_req_init(ra, w, ba, shard);
    if (pthread_create(&tha, NULL, brc_req_thread, ra) != 0) {
        ASSERT_TRUE(0, "spawn A (committer)");
        atomic_store(&g_shard_test_pause_release, 1);
        brc_close(w);
        free(ra); free(rb); free(ba); free(bb); free(w);
        return 1;
    }
    brc_wait_hits(1);
    ASSERT_TRUE(atomic_load(&g_shard_test_pause_hits) >= 1,
                "owner parked at the PRE_MERGE seam holding the gate");
    g_shard_test_pause_phase = SHARD_TEST_PHASE_GATE_WAIT;
    g_shard_test_pause_occurrence = 1;
    brc_req_init(rb, w, bb, shard);
    if (pthread_create(&thb, NULL, brc_req_thread, rb) != 0) {
        ASSERT_TRUE(0, "spawn B (joiner)");
        atomic_store(&g_shard_test_pause_release, 1);
        pthread_join(tha, NULL);
        brc_close(w);
        free(ra); free(rb); free(ba); free(bb); free(w);
        return 1;
    }
    brc_wait_hits(2);
    ASSERT_TRUE(atomic_load(&g_shard_test_pause_hits) >= 2,
                "joiner queued before the owner's take");
    atomic_store(&g_shard_test_pause_release, 1);
    pthread_join(tha, NULL);
    pthread_join(thb, NULL);

    ASSERT_EQ_INT(ra->rc, 0, "owner request ok");
    ASSERT_EQ_INT(rb->rc, 0, "second request ok");
    ASSERT_EQ_INT((int)atomic_load(&g_shard_test_bulk_chains), 1,
                  "one chain merged both requests (the seed path ran)");
    /* Derived on this tree: 4 window-start checks (owner w1 empty, owner w2,
       joiner w1 seeded, joiner w2) + 1 seed + 2 releases = 7 checks, 6 of them
       non-empty. Bounds are deliberately looser; the actuals go in the
       evidence file. An unseeded joiner would leave only 2 non-empty
       window-start checks, so >= 3 also proves the seed. */
    ASSERT_TRUE(atomic_load(&g_shard_test_res_checks) >= 4,
                "verifier ran at every window start (non-vacuous)");
    ASSERT_TRUE(atomic_load(&g_shard_test_res_checks_nonempty) >= 3,
                "verifier saw non-empty maps (cross-window and seeded)");
    ASSERT_EQ_INT((int)atomic_load(&g_shard_test_res_mismatch), 0,
                  "res_count tracks the reservation map (no drift)");
    int all_visible = 1;
    for (size_t i = 0; i < ba->n; i++)
        if (!brc_visible(w, ba->keys[i], ba->vals[i])) all_visible = 0;
    for (size_t i = 0; i < bb->n; i++)
        if (!brc_visible(w, bb->keys[i], bb->vals[i])) all_visible = 0;
    ASSERT_TRUE(all_visible, "all records visible");
    ASSERT_EQ_INT(brc_marker_scan(w->base), 0, "no markers retained");
    brc_close(w);
    free(ra); free(rb); free(ba); free(bb); free(w);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* One request, three windows (16 + 16 + 8), no chain partner. */
static int brc_single(int splits, int shard, size_t n) {
    Brc *w = calloc(1, sizeof(*w));
    BrcBatch *ba = calloc(1, sizeof(*ba));
    BrcReq *ra = calloc(1, sizeof(*ra));
    ASSERT_NOT_NULL(w, "alloc");
    ASSERT_NOT_NULL(ba, "alloc");
    ASSERT_NOT_NULL(ra, "alloc");
    if (!w || !ba || !ra) { free(ra); free(ba); free(w); return 1; }
    if (brc_open(w, splits, BRC_WINDOW) != 0) {
        ASSERT_TRUE(0, "brc_open succeeds");
        brc_close(w);
        free(ra); free(ba); free(w);
        return 1;
    }
    brc_batch_fill(w, ba, 1, n, shard, "vS");
    brc_ctl_reset();
    brc_req_init(ra, w, ba, shard);
    brc_req_thread(ra);   /* synchronous: no pause seam armed */
    ASSERT_EQ_INT(ra->rc, 0, "request ok");
    /* Derived: 3 window-start checks (empty, 16, 32 reserved) + 1 release. */
    ASSERT_TRUE(atomic_load(&g_shard_test_res_checks) >= 3,
                "verifier ran at every window start (non-vacuous)");
    ASSERT_TRUE(atomic_load(&g_shard_test_res_checks_nonempty) >= 2,
                "verifier saw non-empty maps (cross-window)");
    ASSERT_EQ_INT((int)atomic_load(&g_shard_test_res_mismatch), 0,
                  "res_count tracks the reservation map (no drift)");
    int all_visible = 1;
    for (size_t i = 0; i < ba->n; i++)
        if (!brc_visible(w, ba->keys[i], ba->vals[i])) all_visible = 0;
    ASSERT_TRUE(all_visible, "all records visible");
    ASSERT_EQ_INT(brc_marker_scan(w->base), 0, "no markers retained");
    brc_close(w);
    free(ra); free(ba); free(w);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_bulk_res_count_chain(void)         { return brc_chain(8, 0, 30, 30); }
static int test_bulk_res_count_chain_shard300(void){ return brc_chain(512, 300, 30, 30); }
static int test_bulk_res_count_single_windows(void){ return brc_single(512, 300, 40); }

TEST_REGISTER("test-bulk-res-count-chain", test_bulk_res_count_chain)
TEST_REGISTER("test-bulk-res-count-chain-shard300", test_bulk_res_count_chain_shard300)
TEST_REGISTER("test-bulk-res-count-single-windows", test_bulk_res_count_single_windows)
