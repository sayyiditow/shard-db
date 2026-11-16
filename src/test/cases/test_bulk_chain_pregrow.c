/* F7c — deterministic chain-merge pre-grow coverage (in-process slotcask).
 *
 * What is pinned and why: a joiner admitted into a bulk commit chain is
 * staged with `chain_pending` = the owner's (and earlier joiners') record
 * count, so its pre-grow projects the WHOLE chain's records into the shard
 * and sizes the kf table before publish (planning inside a chain may not
 * resplit). Removing the `chain_pending` term makes the joiner project only
 * its own records, and the chain commits into a table sized for the owner
 * alone — breaching the 75% load invariant (60 records in 64 slots = 93.75%
 * load). The observable here is the shard's kf capacity (64 at the owner's
 * park, 128 after the chain) plus `g_shard_test_bulk_chains`, via the
 * proven PRE_MERGE / GATE_WAIT pause seams (test-request-flush-batching
 * scenario 13's choreography). The racy wire case test-bitmap-resplit-chain
 * stays an oracle smoke test only; THIS file is the deterministic proof.
 *
 * Runs in-process, own tmpdir.
 *
 * Why these numbers (tier 64, shard starts empty):
 *   Owner 30 records: 30*4 = 120 < 192 (= 64*3) → no growth, capacity 64;
 *   max_chain_records = 64 - 0 = 64. Joiner 30 ≤ 64-30 → admitted; its
 *   stage projects 0+30+30 = 60 → 240 ≥ 192 → doubles to 128 (240 < 384,
 *   so it stops there). Without `chain_pending` it projects 30 and the
 *   chain commits 60 records into 64 slots (93.75% load). A joiner of 40
 *   > 64-30 = 34 is not admitted (admission cap): two chains; the second
 *   request's own stage projects 30+40 = 70 → 280 ≥ 192 → capacity 128.
 */
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

#define BCP_TIER 64
#define BCP_MAXN 48

/* ── shared scaffold (mirrors test-request-flush-batching) ───────────── */

static void bcp_rm_rf(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    system(cmd);
}

typedef struct {
    SlotcaskDb db;
    char base[PATH_MAX];
    int splits;
    int db_opened;
} Bcp;

static void bcp_ctl_reset(void) {
    shard_test_ctl_reset();
    atomic_store(&g_shard_test_kf_initial_slots, BCP_TIER);
}

static int bcp_open(Bcp *w, int splits) {
    memset(w, 0, sizeof(*w));
    w->splits = splits;
    atomic_store(&g_shard_test_kf_initial_slots, BCP_TIER);
    slotcask_init(64, 64);
    char b[] = "/tmp/shard-db-bulk-chain-XXXXXX";
    if (!mkdtemp(b)) return -1;
    snprintf(w->base, sizeof(w->base), "%s", b);
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", w->base);
    snprintf(k, sizeof(k), "%s/data/kf", w->base);
    if (mkdir(d, 0755) != 0 || mkdir(k, 0755) != 0) return -1;
    memset(&w->db, 0, sizeof(w->db));
    if (slotcask_open(&w->db, w->base, splits, 1, 64) != 0) return -1;
    w->db_opened = 1;
    /* slotcask_open copies g_db->bulk_commit_window, whose runner value is
       not fixed; pin it explicitly — any value >= the batch sizes keeps
       every chain member a single window. */
    w->db.bulk_commit_window = 4096;
    bcp_ctl_reset();
    return 0;
}

static void bcp_close(Bcp *w) {
    if (w->db_opened) slotcask_close(&w->db);
    bcp_rm_rf(w->base);
    slotcask_shutdown();
    shard_test_ctl_reset();
}

static int bcp_shard_of(Bcp *w, const char *key) {
    uint8_t h[16];
    compute_hash_raw(key, strlen(key), h);
    return compute_record_shard(h, w->splits);
}

/* ── window hooks (copied from rf_noop_prepare/apply/terminal) ───────── */

static int bcp_noop_prepare(SlotcaskBulkRec *recs, const size_t *active,
                            size_t nactive, void *ctx,
                            void **out_window_state) {
    (void)recs; (void)active; (void)nactive; (void)ctx;
    *out_window_state = NULL;
    return 0;
}
static int bcp_noop_apply(SlotcaskBulkRec *recs, const size_t *active,
                          size_t nactive, void *ctx, void *window_state) {
    (void)recs; (void)active; (void)nactive; (void)ctx;
    (void)window_state;
    return 0;
}
static void bcp_noop_terminal(void *ctx, void *window_state) {
    (void)ctx; (void)window_state;
}

static void bcp_fill_opts(SlotcaskBulkOpts *o) {
    memset(o, 0, sizeof(*o));
    o->has_indexed_fields = 1;
    o->prepare_window = bcp_noop_prepare;
    o->apply_window = bcp_noop_apply;
    o->commit_done = bcp_noop_terminal;
    o->release_window = bcp_noop_terminal;
    o->abort_window = bcp_noop_terminal;
}

/* ── batch + request drivers ─────────────────────────────────────────── */

typedef struct {
    char keys[BCP_MAXN][24];
    char vals[BCP_MAXN][24];
    SlotcaskBulkRec recs[BCP_MAXN];
    size_t n;
} BcpBatch;

/* Fill n records; every key hashes to `shard` (probed attempt suffix).
 * Keys are unique across batches via different `prefix`. Batch A uses val
 * "vA", batch B "vB". */
static void bcp_batch_fill(Bcp *w, BcpBatch *b, int prefix, size_t n,
                           int shard, const char *val) {
    for (size_t i = 0; i < n; i++) {
        int attempt = 0;
        for (;; attempt++) {
            snprintf(b->keys[i], sizeof(b->keys[i]), "bc-%d-%04zu-%d",
                     prefix, i, attempt);
            if (bcp_shard_of(w, b->keys[i]) == shard) break;
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
    Bcp *w;
    SlotcaskBulkShardInput in;
    int rc;
    int err;
} BcpReq;

static void bcp_req_init(BcpReq *r, Bcp *w, BcpBatch *b, int shard) {
    memset(r, 0, sizeof(*r));
    r->w = w;
    r->in.kf_shard_id = shard;
    r->in.recs = b->recs;
    r->in.nrecs = b->n;
    r->in.kind = SLOTCASK_BULK_INPUT_UPSERT;
    bcp_fill_opts(&r->in.opts.upsert);
    r->rc = 0;
}

static void *bcp_req_thread(void *arg) {
    BcpReq *r = arg;
    errno = 0;
    r->rc = slotcask_bulk_request_execute(&r->w->db, &r->in, 1);
    r->err = errno;
    return NULL;
}

/* kf capacity of one shard (0 on failure). Same shape as
 * test_kf_uncapped_growth.c::shard0_capacity, via the public
 * kf_path_for + kfcache_acquire/release. */
static size_t bcp_capacity(Bcp *w, int shard) {
    char p[PATH_MAX];
    kf_path_for(p, w->db.data_dir, shard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, p, BCP_TIER, 0) != 0) return 0;
    size_t cap = kh.capacity;
    kfcache_release(&kh);
    return cap;
}

/* rt_marker_scan copied from test-request-flush-batching. */
static int bcp_marker_scan(const char *base) {
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

static void bcp_wait_hits(int n) {
    for (int i = 0; i < 30000 && atomic_load(&g_shard_test_pause_hits) < n; i++)
        usleep(1000);
}

/* rf_record_visible renamed: exact length + bytes match. */
static int bcp_visible(Bcp *w, const char *key, const char *want) {
    void *v = NULL; size_t vl = 0;
    int rc = slotcask_get(&w->db, key, strlen(key), &v, &vl);
    if (rc != 0) return 0;
    int ok = vl == strlen(want) && memcmp(v, want, vl) == 0;
    free(v);
    return ok;
}

/* ── the scenario ────────────────────────────────────────────────────── */

/* Never reset or re-zero g_shard_test_pause_hits between the owner's
 * PRE_MERGE hit and the switch to GATE_WAIT: the joiner's hit must be the
 * 2nd, so shard_test_phase_pause's `hit != occurrence` path returns
 * without parking (scenario 13's barrier). */
static int bcp_scenario(int splits, int shard, size_t na, size_t nb,
                        int want_chains, size_t want_cap) {
    Bcp *w = calloc(1, sizeof(*w));
    BcpBatch *ba = calloc(1, sizeof(*ba));
    BcpBatch *bb = calloc(1, sizeof(*bb));
    BcpReq *ra = calloc(1, sizeof(*ra));
    BcpReq *rb = calloc(1, sizeof(*rb));
    ASSERT_NOT_NULL(w, "alloc");
    ASSERT_NOT_NULL(ba, "alloc");
    ASSERT_NOT_NULL(bb, "alloc");
    ASSERT_NOT_NULL(ra, "alloc");
    ASSERT_NOT_NULL(rb, "alloc");
    if (!w || !ba || !bb || !ra || !rb) {
        free(ra); free(rb); free(ba); free(bb); free(w);
        return t_ctx->failed > 0 ? 1 : 0;
    }
    pthread_t tha = 0, thb = 0;
    int a_started = 0, b_started = 0;

    if (bcp_open(w, splits) != 0) {
        ASSERT_TRUE(0, "bcp_open succeeds");
        bcp_close(w);
        free(ra); free(rb); free(ba); free(bb); free(w);
        return 1;
    }
    bcp_batch_fill(w, ba, 1, na, shard, "vA");
    bcp_batch_fill(w, bb, 2, nb, shard, "vB");
    bcp_ctl_reset();
    g_shard_test_pause_phase = SHARD_TEST_PHASE_PRE_MERGE;
    g_shard_test_pause_occurrence = 1;

    bcp_req_init(ra, w, ba, shard);
    if (pthread_create(&tha, NULL, bcp_req_thread, ra) != 0) {
        ASSERT_TRUE(0, "spawn A (committer)");
        atomic_store(&g_shard_test_pause_release, 1);
        bcp_close(w);
        free(ra); free(rb); free(ba); free(bb); free(w);
        return t_ctx->failed > 0 ? 1 : 0;
    }
    a_started = 1;
    bcp_wait_hits(1);
    ASSERT_TRUE(atomic_load(&g_shard_test_pause_hits) >= 1,
                "owner parked at the PRE_MERGE seam holding the gate");
    ASSERT_EQ_INT(bcp_capacity(w, shard), BCP_TIER,
                  "owner's stage left the table at 64 slots");

    g_shard_test_pause_phase = SHARD_TEST_PHASE_GATE_WAIT;
    g_shard_test_pause_occurrence = 1;
    bcp_req_init(rb, w, bb, shard);
    if (pthread_create(&thb, NULL, bcp_req_thread, rb) != 0) {
        ASSERT_TRUE(0, "spawn B (joiner)");
        atomic_store(&g_shard_test_pause_release, 1);
        pthread_join(tha, NULL);
        bcp_close(w);
        free(ra); free(rb); free(ba); free(bb); free(w);
        return t_ctx->failed > 0 ? 1 : 0;
    }
    b_started = 1;
    bcp_wait_hits(2);
    ASSERT_TRUE(atomic_load(&g_shard_test_pause_hits) >= 2,
                "joiner queued before the owner's take");
    atomic_store(&g_shard_test_pause_release, 1);
    pthread_join(tha, NULL);
    pthread_join(thb, NULL);

    ASSERT_EQ_INT(ra->rc, 0, "owner request ok");
    ASSERT_EQ_INT(rb->rc, 0, "second request ok");
    ASSERT_EQ_INT((int)atomic_load(&g_shard_test_bulk_chains), want_chains,
                  "chain count");
    size_t cap = bcp_capacity(w, shard);
    /* One-chain cases: the 128 comes from the admitted joiner's chain-aware
       pre-grow. The 2-chain admission-cap case: the joiner is refused, and
       the 128 comes from the second request's own stage. */
    ASSERT_EQ_INT((int)cap, (int)want_cap,
                  want_chains == 1
                      ? "kf table sized for the whole chain: 128 slots"
                      : "kf table sized for the second chain's own stage: 128 slots");
    ASSERT_TRUE((na + nb) * 4 < cap * 3,
                "75% load invariant holds after the chain");
    int all_visible = 1;
    for (size_t i = 0; i < ba->n; i++)
        if (!bcp_visible(w, ba->keys[i], ba->vals[i])) all_visible = 0;
    for (size_t i = 0; i < bb->n; i++)
        if (!bcp_visible(w, bb->keys[i], bb->vals[i])) all_visible = 0;
    ASSERT_TRUE(all_visible, "all records visible");
    ASSERT_EQ_INT(bcp_marker_scan(w->base), 0, "no markers retained");

    bcp_close(w);
    free(ra); free(rb); free(ba); free(bb); free(w);
    (void)a_started; (void)b_started;
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_bulk_chain_pregrow(void) {
    return bcp_scenario(8, 0, 30, 30, 1, 128);
}
static int test_bulk_chain_pregrow_shard300(void) {
    return bcp_scenario(512, 300, 30, 30, 1, 128);
}
static int test_bulk_chain_admission_cap(void) {
    return bcp_scenario(8, 0, 30, 40, 2, 128);
}

TEST_REGISTER("test-bulk-chain-pregrow", test_bulk_chain_pregrow)
TEST_REGISTER("test-bulk-chain-pregrow-shard300", test_bulk_chain_pregrow_shard300)
TEST_REGISTER("test-bulk-chain-admission-cap", test_bulk_chain_admission_cap)
