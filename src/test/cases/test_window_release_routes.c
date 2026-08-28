/* Regression: the window coordinator must fire exactly one of
   {commit_done, abort_window, release_window} per staged window, routing
   by on-disk truth (marker evidence), not by batch rc alone. Base-red:
   without the routing no hook ever fires, so every route assertion below
   fails and the staged allocation leaks (LSan fails the case under
   BUILD_MODE=asan as well). */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "fixtures.h"
#include "shard_test_ctl.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);

static void rt_cleanup_dir(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    system(cmd);
}

typedef struct {
    SlotcaskDb db;
    char base[PATH_MAX];
} RouteDb;

static int route_db_open(RouteDb *w) {
    slotcask_init(64, 64);
    char b[] = "/tmp/shard-db-route-test-XXXXXX";
    if (!mkdtemp(b)) return -1;
    snprintf(w->base, sizeof(w->base), "%s", b);
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", w->base);
    snprintf(k, sizeof(k), "%s/data/kf", w->base);
    if (mkdir(d, 0755) != 0 || mkdir(k, 0755) != 0) return -1;
    memset(&w->db, 0, sizeof(w->db));
    if (slotcask_open(&w->db, w->base, 8, 1, 64) != 0) return -1;
    w->db.bulk_commit_window = 0;
    shard_test_ctl_reset();
    return 0;
}

static void route_db_close(RouteDb *w) {
    slotcask_close(&w->db);
    rt_cleanup_dir(w->base);
    slotcask_shutdown();
    shard_test_ctl_reset();
}

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

typedef struct {
    int    prepares, prepare_fails;
    int    applies;
    int    commits, aborts, releases;
    int    fail_next;   /* arm: next prepare stages then fails (self-clean) */
    int    reject_all;  /* arm: prepare succeeds but rejects every record */
    void  *staged;      /* non-NULL between prepare and its route hook */
} RouteCounts;

static int rt_prepare(SlotcaskBulkRec *recs, const size_t *active,
                      size_t nactive, void *ctx) {
    RouteCounts *c = ctx;
    (void)recs; (void)active;
    if (c->fail_next) {
        /* Stage, then self-clean — the contract obliges a failed prepare
           to release what it staged before returning non-zero, and Route 4
           asserts staged==NULL afterward so the test actually proves it. */
        c->staged = malloc(32);
        free(c->staged);
        c->staged = NULL;
        c->prepare_fails++;
        return -1;
    }
    c->staged = malloc(32);           /* only a route hook may free this */
    if (!c->staged) return -1;
    c->prepares++;
    if (c->reject_all && nactive > 0)
        recs[active[0]].status = -2;  /* policy rejection by the hook */
    return 0;
}

static int rt_apply(SlotcaskBulkRec *recs, const size_t *active,
                    size_t nactive, void *ctx) {
    RouteCounts *c = ctx;
    (void)recs; (void)active; (void)nactive;
    c->applies++;
    return 0;
}

static void rt_commit(void *ctx) {
    RouteCounts *c = ctx;
    free(c->staged); c->staged = NULL;
    c->commits++;
}
static void rt_abort(void *ctx) {
    RouteCounts *c = ctx;
    free(c->staged); c->staged = NULL;
    c->aborts++;
}
static void rt_release(void *ctx) {
    RouteCounts *c = ctx;
    free(c->staged); c->staged = NULL;
    c->releases++;
}

/* Single-record (SlotcaskUpsertOpts) shapes for Route 6. */
static int rt_single_prepare_fail(const uint8_t *new_value, size_t new_vlen,
                                  uint32_t planned_kf_slot, void *ctx) {
    RouteCounts *c = ctx;
    (void)new_value; (void)new_vlen; (void)planned_kf_slot;
    c->staged = malloc(32);        /* staged, deliberately left behind:
                                      the adapter's abort_commit (Task 1g)
                                      is the cleanup route for this */
    if (!c->staged) return -1;
    c->prepares++;
    return -1;
}
static int rt_single_apply_noop(const uint8_t *new_value, size_t new_vlen,
                                uint32_t planned_kf_slot, void *ctx) {
    RouteCounts *c = ctx;
    (void)new_value; (void)new_vlen; (void)planned_kf_slot;
    c->applies++;
    return 0;
}

/* Drive one 1-record window through the bulk primitive with the recording
   hooks. Returns the public rc (bulk_finish_status folds shard rc to
   0 / -1, with errno==EINPROGRESS when any shard was pending). */
static int rt_drive_one(RouteDb *w, uint64_t key, RouteCounts *c, int shard) {
    char v[8] = "route";
    SlotcaskBulkRec rec;
    memset(&rec, 0, sizeof(rec));
    rec.key = &key; rec.klen = sizeof(key);
    rec.value = v;  rec.vlen = 5;
    SlotcaskBulkOpts o;
    memset(&o, 0, sizeof(o));
    o.has_indexed_fields = 1;
    o.prepare_window = rt_prepare;
    o.apply_window   = rt_apply;
    o.commit_done    = rt_commit;
    o.release_window = rt_release;
    o.abort_window   = rt_abort;
    o.bulk_hook_ctx  = c;
    return slotcask_bulk_upsert_in_kfshard(&w->db, shard, &rec, 1, &o);
}

static int test_window_release_routes(void) {
    RouteDb w;
    ASSERT_EQ_INT(route_db_open(&w), 0, "open route-test db");
    if (t_ctx->failed) return 1;

    uint64_t key = 4242;
    uint8_t h[16];
    compute_hash_raw((const char *)&key, sizeof(key), h);
    int shard = compute_record_shard(h, 8);

    /* Route 1 — success: exactly one commit_done. */
    RouteCounts c; memset(&c, 0, sizeof(c));
    ASSERT_EQ_INT(rt_drive_one(&w, key, &c, shard), 0, "success window rc");
    ASSERT_EQ_INT(c.prepares, 1, "success: one prepare");
    ASSERT_EQ_INT(c.commits, 1, "success: commit_done exactly once");
    ASSERT_EQ_INT(c.aborts, 0, "success: no abort");
    ASSERT_EQ_INT(c.releases, 0, "success: no release");
    ASSERT_TRUE(c.staged == NULL, "success: staged state released");
    ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "success: no marker left");

    /* Route 2 — pre-M publish failure: abort route, no marker on disk.
       shard_test_ctl_reset() first: the sync counters are process-global
       and cumulative — Route 1's window already counted an M barrier, so
       without a reset the armed occurrence would never match. */
    memset(&c, 0, sizeof(c));
    shard_test_ctl_reset();
    g_shard_test_fail_phase = SHARD_TEST_PHASE_M;
    g_shard_test_fail_occurrence = 1;
    ASSERT_EQ_INT(rt_drive_one(&w, key, &c, shard), -1, "pre-M failure rc");
    ASSERT_TRUE(errno != EINPROGRESS, "pre-M failure is not pending");
    g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
    ASSERT_EQ_INT(c.aborts, 1, "pre-M: abort_window fired");
    ASSERT_EQ_INT(c.commits, 0, "pre-M: no commit_done");
    ASSERT_EQ_INT(c.releases, 0, "pre-M: no release");
    ASSERT_TRUE(c.staged == NULL, "pre-M: staged released by abort");
    ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "pre-M: no marker file");

    /* Route 3 — post-M unresolved: a sticky K failure defeats the
       coordinator's own inline replay, so the outcome is genuinely
       pending. release_window fires, the marker stays, and a later clean
       write must gate-replay it and route commit_done normally. Reset
       first — same cumulative-counter reason as Route 2. */
    memset(&c, 0, sizeof(c));
    shard_test_ctl_reset();
    g_shard_test_fail_phase = SHARD_TEST_PHASE_K;
    g_shard_test_fail_occurrence = 1;
    g_shard_test_fail_sticky = 1;
    ASSERT_EQ_INT(rt_drive_one(&w, key, &c, shard), -1, "unresolved rc (-1 folded)");
    ASSERT_EQ_INT(errno, EINPROGRESS, "unresolved errno is EINPROGRESS");
    g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
    g_shard_test_fail_sticky = 0;
    ASSERT_EQ_INT(c.releases, 1, "unresolved: release_window fired");
    ASSERT_EQ_INT(c.commits, 0, "unresolved: no commit_done");
    ASSERT_EQ_INT(c.aborts, 0, "unresolved: no abort");
    ASSERT_TRUE(c.staged == NULL, "unresolved: staged released");
    ASSERT_EQ_INT(rt_marker_scan(w.base), 1, "unresolved: marker retained");
    memset(&c, 0, sizeof(c));
    ASSERT_EQ_INT(rt_drive_one(&w, key, &c, shard), 0,
                  "post-unresolved retry converges");
    ASSERT_EQ_INT(c.commits, 1, "retry: commit_done exactly once");
    ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "retry: marker cleared");

    /* Route 4 — failed prepare self-cleans; NO route hook fires. */
    memset(&c, 0, sizeof(c));
    c.fail_next = 1;
    ASSERT_EQ_INT(rt_drive_one(&w, key, &c, shard), -1, "failed prepare rc");
    ASSERT_EQ_INT(c.aborts + c.commits + c.releases, 0,
                  "failed prepare: no route hook");
    ASSERT_EQ_INT(c.prepare_fails, 1, "failed prepare: hook ran");
    ASSERT_TRUE(c.staged == NULL, "failed prepare: self-cleaned");

    /* Route 5 — all records policy-rejected: nactive==0, M skipped, no
       marker exists — abort route even though the batch rc is 0. */
    memset(&c, 0, sizeof(c));
    c.reject_all = 1;
    ASSERT_EQ_INT(rt_drive_one(&w, key, &c, shard), 0, "all-rejected rc");
    ASSERT_EQ_INT(c.aborts, 1, "all-rejected: abort route (never committed)");
    ASSERT_EQ_INT(c.commits, 0, "all-rejected: no commit_done");
    ASSERT_EQ_INT(c.releases, 0, "all-rejected: no release");
    ASSERT_TRUE(c.staged == NULL, "all-rejected: staged released");

    /* Route 6 — single-record adapter: a failing prepare_commit is cleaned
       by the ADAPTER (1g runs abort_commit on the failure path), so the
       self-clean contract holds even for hooks that stage and give up.
       rc -1, exactly one abort_commit, nothing leaked. */
    memset(&c, 0, sizeof(c));
    {
        uint64_t skey = 4242;
        char sv[8] = "route";
        SlotcaskUpsertOpts so;
        memset(&so, 0, sizeof(so));
        so.has_indexed_fields = 1;
        so.prepare_commit = rt_single_prepare_fail;
        so.apply_commit   = rt_single_apply_noop;
        so.abort_commit   = rt_abort;       /* recorder: increments aborts */
        so.pre_commit_ctx = &c;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, &skey, sizeof(skey),
                                                 sv, 5, &so, NULL), -1,
                      "single failed prepare rc");
    }
    ASSERT_EQ_INT(c.aborts, 1, "single: adapter ran abort_commit once");
    ASSERT_EQ_INT(c.commits + c.releases, 0, "single: no other route");
    ASSERT_TRUE(c.staged == NULL, "single: staged released");

    route_db_close(&w);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-window-release-routes", test_window_release_routes)
