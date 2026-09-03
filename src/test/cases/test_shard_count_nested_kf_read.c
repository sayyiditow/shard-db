/* Regression for the shard_count_worker nested kf-rdlock self-deadlock
 * (docs/plans/2026-08-27-shard-count-worker-nested-kf-read.md).
 *
 * Shape: the indexed-count two-pass worker pre-opens its shard's kf reader
 * for the pass-1 bitmap probe and used to carry it into the pass-2 batch
 * fetch. Every batch entry routes to the worker's OWN shard, so
 * slotcask_bulk_fetch_resolved's kf_reval_fetch_one re-acquired the SAME
 * kfcache entry on the same thread. The kfcache rwlocks are writer-
 * preferring NONRECURSIVE (shard_db_internal.h): with a mutation writer
 * queued behind the probe reader, the recursive reader blocks behind the
 * waiter while the waiter waits for the probe reader — a one-thread
 * self-cycle. Pre-fix that is a permanent hang: the count never answers
 * and the update never commits (the timed update join fails ~30s in).
 * Post-fix the worker releases the probe reader before pass-2, the update
 * commits, and the count returns the exact expected total.
 *
 * Determinism: the TEST_BUILD count-gap seam parks the worker after
 * pass-1 (probe reader still held); the test then starts the update,
 * which can only queue on the parked shard's kf wrlock, waits 500 ms so
 * the writer is certainly blocked inside pthread_rwlock_wrlock (that
 * state is sticky — it cannot clear until the worker releases), then
 * releases the park. Watchdog-safe by construction: client-side io
 * timeouts, timed joins, kill-daemon teardown; a wedged daemon never
 * touches the runner process. Red state is Linux/glibc-specific (the
 * non-glibc rwlock fallback tolerates reader recursion silently).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "fixtures.h"
#include "test_client.h"

#include <errno.h>
#include <ftw.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static const char *g_obj = "cntgap";

#define FIXTURE_ROWS 40
#define EXPECT_HITS 12
#define COUNT_IO_TIMEOUT_MS 25000

/* Index-drive leaf: score eq (btree). Bitmap post-filter leaf: stage eq
 * (explicit bitmap index). Record-fetch post-filter leaf: title contains
 * (unindexed). all_postfilters_are_bm == 0, so every bitmap-passing row
 * is fetched in pass-2 — which is where the nested acquire used to
 * happen. n_bm_postfilter > 0 is what pre-opens the probe reader. */
static const char *COUNT_CRITERIA =
    "{\"and\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"5\"},"
    "{\"field\":\"stage\",\"op\":\"eq\",\"value\":\"a\"},"
    "{\"field\":\"title\",\"op\":\"contains\",\"value\":\"hit\"}]}";

static int remove_tree_entry(const char *path, const struct stat *st,
                             int typeflag, struct FTW *ftwbuf) {
    (void)st;
    (void)typeflag;
    (void)ftwbuf;
    return remove(path);
}

static int remove_tree(const char *path) {
    if (!path || !path[0]) { errno = EINVAL; return -1; }
    if (nftw(path, remove_tree_entry, 32, FTW_DEPTH | FTW_PHYS) == 0)
        return 0;
    return errno == ENOENT ? 0 : -1;
}

/* Route EVERY row onto kf shard 0 so the single non-empty shard group
 * owns the shard whose kf reader/wrlock collide. */
static int build_fixture(TestEnv *env, int splits,
                         char *out_miss_key, size_t miss_key_sz) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[768], *resp = NULL;

    if (tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
                   &resp) != 0) { tc_close(tc); return -1; }
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":%d,\"streams\":2,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:64\",\"stage:varchar:8\"],"
        "\"indexes\":[\"score\",\"stage:bitmap\"]}", g_obj, splits);
    if (tc_request(tc, req, &resp) != 0 ||
        !SAFE_STRSTR(resp, "\"status\":\"created\"")) {
        free(resp); tc_close(tc); return -1;
    }
    free(resp); resp = NULL;

    int placed = 0, miss_picked = 0;
    out_miss_key[0] = '\0';
    for (int cand = 0; placed < FIXTURE_ROWS; cand++) {
        char k[32];
        snprintf(k, sizeof(k), "r%04d", cand);
        uint8_t h[16];
        compute_hash_raw(k, strlen(k), h);
        if (compute_record_shard(h, splits) != 0) continue;  /* shard 0 only */
        int is_hit = (placed < EXPECT_HITS);
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"%s\",\"value\":{\"score\":5,\"stage\":\"a\","
            "\"title\":\"%s%04d\"}}",
            g_obj, k, is_hit ? "hit" : "miss", placed);
        resp = NULL;
        if (tc_request(tc, req, &resp) != 0 ||
            !SAFE_STRSTR(resp, "\"status\":\"inserted\"")) {
            free(resp); tc_close(tc); return -1;
        }
        free(resp); resp = NULL;
        if (!is_hit && !miss_picked) {
            snprintf(out_miss_key, miss_key_sz, "%s", k);
            miss_picked = 1;
        }
        placed++;
    }
    tc_close(tc);
    return miss_picked ? 0 : -1;
}

static int run_exact_count(TestEnv *env, long *out) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[768];
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
             "\"criteria\":%s}", g_obj, COUNT_CRITERIA);
    char *resp = NULL;
    int crc = tc_request(tc, req, &resp);
    long n = (crc == 0 && resp) ? strtol(resp, NULL, 10) : -1;
    free(resp);
    tc_close(tc);
    *out = n;
    return (n >= 0) ? 0 : -1;
}

typedef struct {
    TestEnv  *env;
    pthread_t tid;
} CountArg;

/* Returns 0 iff the count round-tripped AND returned the exact total.
 * On the base tree this thread's daemon-side worker is wedged, so the
 * client times out and the rc is nonzero. */
static void *count_thread(void *p) {
    CountArg *a = (CountArg *)p;
    TestClientCfg cfg = { .port = a->env->port,
                          .io_timeout_ms = COUNT_IO_TIMEOUT_MS };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return (void *)(intptr_t)-2;
    char req[768];
    snprintf(req, sizeof(req),
             "{\"timeout_ms\":20000,\"mode\":\"count\",\"dir\":\"default\","
             "\"object\":\"%s\",\"criteria\":%s}", g_obj, COUNT_CRITERIA);
    char *resp = NULL;
    int crc = tc_request(tc, req, &resp);
    intptr_t rc = -1;
    if (crc == 0 && resp && !strstr(resp, "\"error\"")) {
        long n = strtol(resp, NULL, 10);
        rc = (n == EXPECT_HITS) ? 0 : -1;
    }
    free(resp);
    tc_close(tc);
    return (void *)rc;
}

typedef struct {
    TestEnv *env;
    char     key[32];
    int      rc;
    TuJoinSignal js;
    pthread_t tid;
} UpdArg;

static void *update_thread(void *p) {
    UpdArg *a = (UpdArg *)p;
    TestClientCfg cfg = { .port = a->env->port,
                          .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { a->rc = -1; tu_join_signal_mark_done(&a->js); return NULL; }
    char req[768], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"%s\","
             "\"key\":\"%s\",\"value\":{\"score\":5,\"stage\":\"a\","
             "\"title\":\"upd\"}}", g_obj, a->key);
    int crc = tc_request(tc, req, &resp);
    a->rc = (crc == 0 && resp && !strstr(resp, "\"error\"")) ? 0 : -1;
    free(resp);
    tc_close(tc);
    tu_join_signal_mark_done(&a->js);
    return NULL;
}

static int test_shard_count_nested_kf_read_run(void) {
    TestEnv env;
    memset(&env, 0, sizeof(env));
    const int splits = 8;
    ASSERT_EQ_INT(test_env_start(&env), 0, "daemon starts");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }

    char miss_key[32];
    ASSERT_EQ_INT(build_fixture(&env, splits, miss_key, sizeof(miss_key)), 0,
                  "build single-shard indexed fixture");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }

    /* Baseline with the seam unarmed: exact count, no park. */
    long base = -1;
    ASSERT_EQ_INT(run_exact_count(&env, &base), 0,
                  "baseline count round-trips");
    if (!t_ctx->failed)
        ASSERT_TRUE(base == EXPECT_HITS, "baseline count is exact");

    if (!t_ctx->failed) {
        ASSERT_EQ_INT(test_env_test_hook_install_kind(&env, 1), 0,
                      "arm count-gap hook");   /* kind 1 = count-worker
                                                pass-1 gap (test_control.c) */
        CountArg ca = { .env = &env };
        ASSERT_EQ_INT(pthread_create(&ca.tid, NULL, count_thread, &ca), 0,
                      "spawn parked count");
        int phase = -1;
        ASSERT_EQ_INT(test_env_test_hook_wait(&env, &phase), 0,
                      "count worker reached the pass-1 gap");
        ASSERT_EQ_INT(phase, 2,
                      "worker parked after pass-1 (probe reader held)");
        if (!t_ctx->failed) {
            UpdArg ua;
            memset(&ua, 0, sizeof(ua));
            ua.env = &env;
            snprintf(ua.key, sizeof(ua.key), "%s", miss_key);
            tu_join_signal_init(&ua.js);
            ASSERT_EQ_INT(pthread_create(&ua.tid, NULL, update_thread, &ua),
                          0, "spawn concurrent update");
            /* The worker holds the probe reader, so the update can only
             * be QUEUED on the kf wrlock. 500 ms makes "blocked inside
             * pthread_rwlock_wrlock" certain; that state is sticky until
             * the worker releases. */
            nanosleep(&(struct timespec){0, 500000000L}, NULL);

            test_env_test_hook_release(&env);

            /* Fixed build: the update acquires the wrlock the moment the
             * (now-released) worker drops the probe reader, commits, and
             * the count fetch completes. Base tree: worker wedged in the
             * nested acquire, update wedged behind it -> join times out. */
            int joined_upd = tu_timed_join(ua.tid, &ua.js, 30);
            ASSERT_EQ_INT(joined_upd, 0,
                          "update finishes after release (base tree: "
                          "wedged behind the recursive reader)");
            if (joined_upd == 0 && !t_ctx->failed) {
                ASSERT_EQ_INT(ua.rc, 0, "update committed");
                void *cres = NULL;
                pthread_join(ca.tid, &cres);
                ASSERT_EQ_INT((int)(intptr_t)cres, 0,
                              "count completed with the exact expected "
                              "total (base tree: client timeout)");
                long after = -1;
                ASSERT_EQ_INT(run_exact_count(&env, &after), 0,
                              "post-window count round-trips");
                if (!t_ctx->failed)
                    ASSERT_TRUE(after == EXPECT_HITS,
                                "totals converge after the window");
            } else {
                /* Wedged threads may still touch ua.js / sockets — leak
                 * them per the timed-join contract (daemon is killed
                 * below either way). */
                pthread_detach(ua.tid);
                pthread_detach(ca.tid);
            }
            if (joined_upd == 0) tu_join_signal_destroy(&ua.js);
        }
        test_env_test_hook_clear(&env);
    }

    test_env_kill(&env);
    char base_path[sizeof(env.db_root)];
    char *slash = strrchr(env.db_root, '/');
    if (slash && slash != env.db_root) {
        size_t base_len = (size_t)(slash - env.db_root);
        memcpy(base_path, env.db_root, base_len);
        base_path[base_len] = '\0';
        ASSERT_EQ_INT(remove_tree(base_path), 0,
                      "remove test fixture tree");
    }
    return t_ctx->failed ? 1 : 0;
}

TEST_REGISTER("test-shard-count-nested-kf-read",
              test_shard_count_nested_kf_read_run)
