/* Regression for the count / bulk-insert lock-order deadlock found by the
 * TSan gate (2026-11-13).
 *
 * Shape: bulk insert holds a kf shard's writer lock, then takes that
 * shard's bitmap writer locks (window preopen) — the documented order.
 * shard_count_worker with bitmap post-filters took the kf probe reader,
 * then the bitmap readers; before the pass-2 batch fetch it released the
 * probe reader but kept the bitmap readers, so the fetch re-took kf readers
 * while holding bitmap readers (bitmap -> kf). A bulk insert on the same
 * shard in that window waits on the bitmap writer lock while holding the
 * kf writer lock, and the count's fetch waits on the kf reader behind it:
 * both hang forever. Post-fix the worker drops its bitmap readers together
 * with the probe reader, so the bulk insert commits and the count answers.
 *
 * Determinism: INSTALL kind 4 parks the count worker right after the
 * probe reader is released (before the batch fetch). The test then starts
 * a bulk insert on the same shard and waits 500 ms so, on the base tree,
 * it is certainly blocked on the bitmap writer lock (sticky until the
 * worker drops its readers), then releases the park. Same precedent and
 * watchdog contract as test_shard_count_nested_kf_read.c: client io
 * timeouts, timed joins, kill-daemon teardown.
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

static const char *g_obj = "cntbmlo";

#define FIXTURE_ROWS 40
#define EXPECT_HITS 12
#define BULK_ROWS 8
#define COUNT_IO_TIMEOUT_MS 25000

/* Index-drive leaf score eq (btree); bitmap post-filter stage eq; record
 * post-filter title contains (unindexed) so bitmap-passing rows are fetched
 * in pass 2 — the window this test parks in. */
static const char *COUNT_CRITERIA =
    "{\"and\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"5\"},"
    "{\"field\":\"stage\",\"op\":\"eq\",\"value\":\"a\"},"
    "{\"field\":\"title\",\"op\":\"contains\",\"value\":\"hit\"}]}";

static int remove_tree_entry(const char *path, const struct stat *st,
                             int typeflag, struct FTW *ftwbuf) {
    (void)st; (void)typeflag; (void)ftwbuf;
    return remove(path);
}

static int remove_tree(const char *path) {
    if (!path || !path[0]) { errno = EINVAL; return -1; }
    if (nftw(path, remove_tree_entry, 32, FTW_DEPTH | FTW_PHYS) == 0)
        return 0;
    return errno == ENOENT ? 0 : -1;
}

/* Next key at or after *cand that routes to kf shard 0. */
static void next_shard0_key(int splits, int *cand, char *k, size_t ksz) {
    for (;; (*cand)++) {
        snprintf(k, ksz, "r%04d", *cand);
        uint8_t h[16];
        compute_hash_raw(k, strlen(k), h);
        if (compute_record_shard(h, splits) == 0) { (*cand)++; return; }
    }
}

/* Every row on kf shard 0: the count's only non-empty shard group is the
 * shard the bulk insert locks. *cand returns the next unused candidate. */
static int build_fixture(TestEnv *env, int splits, int *cand) {
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

    for (int placed = 0; placed < FIXTURE_ROWS; placed++) {
        char k[32];
        next_shard0_key(splits, cand, k, sizeof(k));
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"%s\",\"value\":{\"score\":5,\"stage\":\"a\","
            "\"title\":\"%s%04d\"}}",
            g_obj, k, placed < EXPECT_HITS ? "hit" : "miss", placed);
        if (tc_request(tc, req, &resp) != 0 ||
            !SAFE_STRSTR(resp, "\"status\":\"inserted\"")) {
            free(resp); tc_close(tc); return -1;
        }
        free(resp); resp = NULL;
    }
    tc_close(tc);
    return 0;
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
    long n = (crc == 0 && resp && !strstr(resp, "\"error\""))
                 ? strtol(resp, NULL, 10) : -1;
    free(resp);
    tc_close(tc);
    *out = n;
    return (n >= 0) ? 0 : -1;
}

typedef struct {
    TestEnv  *env;
    pthread_t tid;
} CountArg;

/* 0 iff the count round-tripped with the exact total (bulk rows are
 * "miss" titles and were not candidates when the worker parked). */
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
    char     body[2048];
    int      rc;
    TuJoinSignal js;
    pthread_t tid;
} BulkArg;

static void *bulk_thread(void *p) {
    BulkArg *a = (BulkArg *)p;
    TestClientCfg cfg = { .port = a->env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { a->rc = -1; tu_join_signal_mark_done(&a->js); return NULL; }
    char req[2304], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\","
             "\"records\":%s}", g_obj, a->body);
    int crc = tc_request(tc, req, &resp);
    char want[32];
    snprintf(want, sizeof(want), "\"inserted\":%d", BULK_ROWS);
    a->rc = (crc == 0 && resp && !strstr(resp, "\"error\"") &&
             strstr(resp, want)) ? 0 : -1;
    free(resp);
    tc_close(tc);
    tu_join_signal_mark_done(&a->js);
    return NULL;
}

static int test_count_bitmap_bulk_lock_order_run(void) {
    TestEnv env;
    memset(&env, 0, sizeof(env));
    const int splits = 8;
    ASSERT_EQ_INT(test_env_start(&env), 0, "daemon starts");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }

    int cand = 0;
    ASSERT_EQ_INT(build_fixture(&env, splits, &cand), 0,
                  "build single-shard bitmap fixture");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }

    long base = -1;
    ASSERT_EQ_INT(run_exact_count(&env, &base), 0, "baseline count round-trips");
    if (!t_ctx->failed)
        ASSERT_TRUE(base == EXPECT_HITS, "baseline count is exact");

    if (!t_ctx->failed) {
        BulkArg ba;
        memset(&ba, 0, sizeof(ba));
        ba.env = &env;
        size_t p = 0;
        SB_APPEND(ba.body, p, sizeof ba.body, "{");
        for (int i = 0; i < BULK_ROWS; i++) {
            char k[32];
            next_shard0_key(splits, &cand, k, sizeof(k));
            SB_APPEND(ba.body, p, sizeof ba.body,
                      "%s\"%s\":{\"score\":5,\"stage\":\"a\",\"title\":\"bulk%02d\"}",
                      i ? "," : "", k, i);
        }
        SB_APPEND(ba.body, p, sizeof ba.body, "}");

        ASSERT_EQ_INT(test_env_test_hook_install_kind(&env, 4), 0,
                      "arm late count-gap hook");  /* kind 4 = after the
                                                      probe reader release */
        CountArg ca = { .env = &env };
        ASSERT_EQ_INT(pthread_create(&ca.tid, NULL, count_thread, &ca), 0,
                      "spawn parked count");
        int phase = -1;
        ASSERT_EQ_INT(test_env_test_hook_wait(&env, &phase), 0,
                      "count worker reached the pre-fetch gap");
        ASSERT_EQ_INT(phase, 2, "worker parked before the batch fetch");
        if (!t_ctx->failed) {
            tu_join_signal_init(&ba.js);
            ASSERT_EQ_INT(pthread_create(&ba.tid, NULL, bulk_thread, &ba), 0,
                          "spawn concurrent bulk insert on the same shard");
            /* Base tree: the bulk insert takes the kf writer and queues on
             * the bitmap writer behind the parked worker's readers; that
             * state is sticky until the worker drops them. */
            nanosleep(&(struct timespec){0, 500000000L}, NULL);

            test_env_test_hook_release(&env);

            int joined = tu_timed_join(ba.tid, &ba.js, 30);
            ASSERT_EQ_INT(joined, 0,
                          "bulk insert finishes after release (base tree: "
                          "deadlocked with the count's batch fetch)");
            if (joined == 0 && !t_ctx->failed) {
                ASSERT_EQ_INT(ba.rc, 0, "bulk insert committed all rows");
                void *cres = NULL;
                pthread_join(ca.tid, &cres);
                ASSERT_EQ_INT((int)(intptr_t)cres, 0,
                              "count completed with the exact expected total");
                long after = -1;
                ASSERT_EQ_INT(run_exact_count(&env, &after), 0,
                              "post-window count round-trips");
                if (!t_ctx->failed)
                    ASSERT_TRUE(after == EXPECT_HITS,
                                "totals unchanged by the miss-only bulk rows");
            } else {
                /* Wedged threads may still touch ba.js / sockets: leak
                 * them per the timed-join contract (daemon killed below). */
                pthread_detach(ba.tid);
                pthread_detach(ca.tid);
            }
            if (joined == 0) tu_join_signal_destroy(&ba.js);
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
        ASSERT_EQ_INT(remove_tree(base_path), 0, "remove test fixture tree");
    }
    return t_ctx->failed ? 1 : 0;
}

TEST_REGISTER("test-count-bitmap-bulk-lock-order",
              test_count_bitmap_bulk_lock_order_run)
