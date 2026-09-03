/* Regression test for the bitmap-executor inline batch flush under held
 * kf+bitmap handles (docs/plans/2026-08-27-bitmap-inline-flush-hazard.md).
 *
 * Shape: a limit-bound streaming find whose primary leaf is a BITMAP index
 * routes to the legacy executor (btree_dispatch's IT_BITMAP branches). The
 * bitmap walk holds the shard's kf reader + bitmap handle across the emit,
 * and stream_find_cb's collector flushes the batch-fetch buffer INLINE when
 * it fills — slotcask_bulk_resolve_and_fetch then re-acquires the SAME
 * shard's kf reader on the same thread. kfcache locks are
 * PREFER_WRITER_NONRECURSIVE: with a window writer queued on kf(S) wr, the
 * recursive read acquire self-deadlocks, wedging the walker (which keeps
 * bitmap(S) rd) and the queued writer forever. The client timeout is the
 * only exit.
 *
 * Orchestration (deterministic, no lock-timing races): the TEST_BUILD
 * find-flush gate (test-control kind 2) parks the flusher with the handles
 * held and a full all-S batch; the mutation is then spawned so it QUEUES on
 * kf(S) wr; releasing the gate walks the flusher straight into the nested
 * acquire. On the fixed build (deferred collector) the same gate parks the
 * drain AFTER the handles drop, the mutation completes while the drainer is
 * parked (liveness proof), and the find finishes within its timeout.
 */
#define _GNU_SOURCE
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

#define SPLITS 8
#define BAIT_ROWS 14                 /* > streaming batch cap (limit 10) */
#define BAIT_SHARD_ROWS (BAIT_ROWS + 1)  /* bait + the parked key */
#define FILLER_ROWS 20
#define FIND_TIMEOUT_MS 8000
#define WRITER_SETTLE_S 3

static const char *g_obj = "bmflush";

static int count_eq(TestEnv *env, const char *crit, long *out) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[768], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
             "\"criteria\":%s}", g_obj, crit);
    int crc = tc_request(tc, req, &resp);
    long n = (crc == 0) ? tu_parse_count(resp) : -1;
    free(resp);
    tc_close(tc);
    *out = n;
    return (n >= 0) ? 0 : -1;
}

typedef struct {
    TestEnv *env;
    int rc;                 /* 0 = find completed without wire error */
    char req[768];
} FindArg;

static void *find_thread(void *p) {
    FindArg *fa = (FindArg *)p;
    TestClientCfg cfg = { .port = fa->env->port,
                          .io_timeout_ms = FIND_TIMEOUT_MS };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { fa->rc = -2; return NULL; }
    char *resp = NULL;
    int crc = tc_request(tc, fa->req, &resp);
    fa->rc = (crc == 0 && resp && !strstr(resp, "\"error\"")) ? 0 : -1;
    free(resp);
    tc_close(tc);
    return NULL;
}

typedef struct {
    TestEnv *env;
    int rc;                 /* wire result of the mutation */
    TuJoinSignal js;
    pthread_t tid;
    char key[32];
    char valjson[96];
} MutThreadArg;

static void *mutation_thread(void *p) {
    MutThreadArg *a = (MutThreadArg *)p;
    TestClientCfg cfg = { .port = a->env->port,
                          .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { a->rc = -1; tu_join_signal_mark_done(&a->js); return NULL; }
    char req[768], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"%s\","
             "\"key\":\"%s\",\"value\":%s}", g_obj, a->key, a->valjson);
    int crc = tc_request(tc, req, &resp);
    a->rc = (crc == 0 && resp && !strstr(resp, "\"error\"")) ? 0 : -1;
    free(resp);
    tc_close(tc);
    tu_join_signal_mark_done(&a->js);
    return NULL;
}

/* 15 keys (14 bait + parked) routed to ONE shard S, filler rows elsewhere.
 * Explicit `flag:bitmap` declaration — no reliance on the create-object
 * auto-default. */
static int build_fixture(TestEnv *env, char *parked_key, size_t parked_sz) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[768], *resp = NULL;

    if (tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
                   &resp) != 0) { tc_close(tc); return -1; }
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":%d,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"title:varchar:64\"],"
        "\"indexes\":[\"flag:bitmap\"]}", g_obj, SPLITS);
    if (tc_request(tc, req, &resp) != 0 ||
        !SAFE_STRSTR(resp, "\"status\":\"created\"")) {
        free(resp); tc_close(tc); return -1;
    }
    free(resp); resp = NULL;

    /* Bucket candidate keys by kf shard until one shard accumulates
     * BAIT_SHARD_ROWS keys — every eq "true" candidate then lives on that
     * single shard, so the flush batch provably contains S-routed hashes. */
    char keys[SPLITS][BAIT_SHARD_ROWS][32];
    int counts[SPLITS] = {0};
    int shard_s = -1;
    for (int cand = 0; shard_s < 0; cand++) {
        char k[32];
        snprintf(k, sizeof(k), "bk%05d", cand);
        uint8_t h[16];
        compute_hash_raw(k, strlen(k), h);
        int s = compute_record_shard(h, SPLITS);
        if (counts[s] >= BAIT_SHARD_ROWS) continue;   /* full already */
        snprintf(keys[s][counts[s]++], 32, "%s", k);
        if (counts[s] == BAIT_SHARD_ROWS) shard_s = s;
    }

    /* 14 bait rows: flag=true, distinct titles. */
    for (int i = 0; i < BAIT_ROWS; i++) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"%s\",\"value\":{\"flag\":true,\"title\":\"wb%02d\"}}",
            g_obj, keys[shard_s][i], i);
        if (tc_request(tc, req, &resp) != 0 ||
            !SAFE_STRSTR(resp, "\"status\":\"inserted\"")) {
            free(resp); tc_close(tc); return -1;
        }
        free(resp); resp = NULL;
    }

    /* Parked key: 15th S-routed row, flipped by the mutation mid-test. */
    snprintf(parked_key, parked_sz, "%s", keys[shard_s][BAIT_ROWS]);
    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"flag\":true,\"title\":\"parked\"}}",
        g_obj, parked_key);
    if (tc_request(tc, req, &resp) != 0 ||
        !SAFE_STRSTR(resp, "\"status\":\"inserted\"")) {
        free(resp); tc_close(tc); return -1;
    }
    free(resp); resp = NULL;

    /* Filler rows on other shards: flag=false — must never match. */
    int placed = 0;
    for (int cand = 0; placed < FILLER_ROWS; cand++) {
        char k[32];
        snprintf(k, sizeof(k), "fl%05d", cand);
        uint8_t h[16];
        compute_hash_raw(k, strlen(k), h);
        if (compute_record_shard(h, SPLITS) == shard_s) continue;
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"%s\",\"value\":{\"flag\":false,\"title\":\"f%02d\"}}",
            g_obj, k, placed);
        if (tc_request(tc, req, &resp) != 0 ||
            !SAFE_STRSTR(resp, "\"status\":\"inserted\"")) {
            free(resp); tc_close(tc); return -1;
        }
        free(resp); resp = NULL;
        placed++;
    }

    tc_close(tc);
    return 0;
}

static int test_bitmap_stream_find_flush_gate_run(void) {
    TestEnv env;
    memset(&env, 0, sizeof(env));
    ASSERT_EQ_INT(test_env_start(&env), 0, "daemon starts");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }

    char parked_key[32] = "";
    int built = build_fixture(&env, parked_key, sizeof(parked_key));
    ASSERT_EQ_INT(built, 0, "build bitmap streaming fixture");
    if (t_ctx->failed) { test_env_kill(&env); return 1; }

    long pre = -1;
    ASSERT_EQ_INT(count_eq(&env, "[{\"field\":\"flag\",\"op\":\"eq\","
                                 "\"value\":\"true\"}]", &pre), 0,
                  "pre-count round-trips");
    if (!t_ctx->failed)
        ASSERT_TRUE(pre == BAIT_SHARD_ROWS,
                    "pre-count exact (15 true rows, all one shard)");

    if (!t_ctx->failed) {
        ASSERT_EQ_INT(test_env_test_hook_install_kind(&env, 2), 0,
                      "arm find-flush gate");   /* kind 2 = batch-fetch park
                                                   (test_control.c) */

        /* The hazard find: bitmap-primary eq + never-matching unindexed
         * contains sibling forces the limit-bound STREAMING executor
         * (FP_PRIMARY_LEAF + post-filter tree); limit 10 < 15 same-shard
         * candidates forces the inline flush mid-walk on the legacy
         * collector. */
        FindArg fa = { .env = &env, .rc = -3,
                       .req = "{\"timeout_ms\":8000,\"mode\":\"find\","
                              "\"dir\":\"default\",\"object\":\"bmflush\","
                              "\"criteria\":{\"and\":["
                              "{\"field\":\"flag\",\"op\":\"eq\","
                              "\"value\":\"true\"},"
                              "{\"field\":\"title\",\"op\":\"contains\","
                              "\"value\":\"zzz-no-such-title\"}]},"
                              "\"limit\":10}" };
        pthread_t find_tid;
        ASSERT_EQ_INT(pthread_create(&find_tid, NULL, find_thread, &fa), 0,
                      "spawn hazard find");

        int phase = -1;
        ASSERT_EQ_INT(test_env_test_hook_wait(&env, &phase), 0,
                      "worker parked at the batch-fetch gate");
        ASSERT_EQ_INT(phase, 3, "gate phase = find batch-fetch park");

        if (!t_ctx->failed) {
            /* Queue the window writer on the walked shard while the flusher
             * sits parked holding kf(S)+bitmap(S) (legacy) / nothing (fixed).
             * The settle only needs to cover dispatch + marker fsync (ms
             * scale); the gate already pinned the hard ordering. */
            MutThreadArg ua = { .env = &env, .rc = -2,
                                .key = "", .valjson = "" };
            snprintf(ua.key, sizeof(ua.key), "%s", parked_key);
            snprintf(ua.valjson, sizeof(ua.valjson),
                     "{\"flag\":false,\"title\":\"parked\"}");
            tu_join_signal_init(&ua.js);
            ASSERT_EQ_INT(pthread_create(&ua.tid, NULL, mutation_thread,
                                         &ua), 0, "spawn mutation");
            sleep(WRITER_SETTLE_S);

            test_env_test_hook_release(&env);

            /* The find thread always terminates: it is bounded by its own
             * 8 s client timeout on the wedged base build, and by query
             * completion on the fixed build. */
            pthread_join(find_tid, NULL);
            ASSERT_EQ_INT(fa.rc, 0,
                          "streaming find completes within timeout "
                          "(base: wedged behind the recursive reader)");

            int jmut = tu_timed_join(ua.tid, &ua.js, 4);
            ASSERT_EQ_INT(jmut, 0,
                          "mutation completes after gate release "
                          "(base: writer queued behind the wedged walker)");
            if (jmut == 0 && !t_ctx->failed) {
                ASSERT_EQ_INT(ua.rc, 0, "mutation committed");

                /* Convergence: parked key flipped, everything else intact.
                 * The hazard find itself returns [] either way (the contains
                 * sibling never matches), so row correctness is asserted
                 * here, after the window. */
                long post = -1;
                ASSERT_EQ_INT(count_eq(&env, "[{\"field\":\"flag\","
                                             "\"op\":\"eq\","
                                             "\"value\":\"true\"}]",
                                       &post), 0,
                              "post-count round-trips");
                if (!t_ctx->failed)
                    ASSERT_TRUE(post == BAIT_ROWS,
                                "parked-key flip converged (14 true rows)");
            } else {
                /* Wedged thread may still touch ua.js / sockets — leak it
                 * per the timed-join contract (daemon is killed below). */
                pthread_detach(ua.tid);
            }
            if (jmut == 0) tu_join_signal_destroy(&ua.js);
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

TEST_REGISTER("test-bitmap-stream-find-flush-gate",
              test_bitmap_stream_find_flush_gate_run)
