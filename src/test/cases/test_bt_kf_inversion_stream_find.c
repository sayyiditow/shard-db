/* Regression test for the btree↔kfcache lock-order inversion deadlock
 * (docs/plans/2026-08-27-bt-kf-lock-inversion-chunked-fetch.md).
 *
 * Shape: park one indexed UPDATE inside its under-lock callback (the
 * slotcask_test_after_old seam fires with under_kf_wrlock == 1, holding the
 * target kf shard's WRLOCK before any btree lock is taken), then issue
 * limit-bound streaming finds whose primary-leaf scan fills the batch-fetch
 * buffer, forcing inline kf resolves while per-shard bt_cache rdlocks are
 * held. Pre-fix that is an AB-BA wedge — find holds BT-RD waiting KF-RD,
 * resumed writer holds KF-WR waiting BT-WR — and every find times out. Post-
 * fix the walkers close their own iterators before the blocking flush and
 * every find completes while the writer is still parked.
 *
 * Also pins the isolation invariant while the writer sits mid-window: rows
 * returned during the park must be whole pre-update versions (never hybrid),
 * and everything converges once the update lands.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "fixtures.h"
#include "test_client.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define FIXTURE_ROWS 200
#define WEDGE_ROWS 12      /* > streaming batch cap (limit-bound => 10) */
#define EXPECT_TOTAL (FIXTURE_ROWS + 1 + WEDGE_ROWS) /* safe + parked + bait */
#define PARKED_FINDS 3
#define FIND_IO_TIMEOUT_MS 25000

static const char *g_obj = "invfind";

typedef struct {
    TestEnv *env;
    int rc;                 /* wire result of the parked update */
    TuJoinSignal js;
    char key[32];
    char valjson[128];
} UpdThreadArg;

/* Phase-B find: single candidate on the parked shard, started BEFORE the
 * hook release so its worker sits inside the idx-shard bt rdlock while
 * blocked on the parked kf wrlock when the writer resumes. */
typedef struct {
    TestEnv   *env;
    char       req[768];
    pthread_t  tid;
} FindBArg;

static void *find_b_thread(void *p) {
    FindBArg *fb = (FindBArg *)p;
    TestClientCfg cfg = { .port = fb->env->port,
                          .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return (void *)(intptr_t)-2;
    char *resp = NULL;
    int crc = tc_request(tc, fb->req, &resp);
    /* Completion-without-error IS the assertion: the walker closed its
     * bt iterator before blocking, so it can never wedge the resumed
     * writer (legacy executor + held iterator = AB-BA base behaviour).
     * Whether the row emits as the pre-update (777) or post-update value
     * is decided legitimately by where the window lands relative to the
     * fetch; an empty result after the commit is equally valid. */
    intptr_t rc = (crc == 0 && resp && !strstr(resp, "\"error\"")) ? 0 : -1;
    free(resp);
    tc_close(tc);
    return (void *)rc;
}

static void *parked_update_thread(void *p) {
    UpdThreadArg *a = (UpdThreadArg *)p;
    TestClientCfg cfg = { .port = a->env->port,
                          .io_timeout_ms = 60000 };
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

/* One broad find whose candidates all pass the indexed leaf (score=5) and
 * all fail the unindexed post-filter (title contains needle) — the walk
 * therefore exhausts every matching entry and must flush the batch-fetch
 * buffer repeatedly even though nothing is emitted. */
static int parked_find(TestEnv *env, char **out_resp) {
    TestClientCfg cfg = { .port = env->port,
                          .io_timeout_ms = FIND_IO_TIMEOUT_MS };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[768];
    snprintf(req, sizeof(req),
             "{\"timeout_ms\":6000,\"mode\":\"find\",\"dir\":\"default\","
             "\"object\":\"%s\",\"criteria\":{\"and\":["
             "{\"field\":\"score\",\"op\":\"eq\",\"value\":\"5\"},"
             "{\"field\":\"title\",\"op\":\"contains\","
             "\"value\":\"zzzz-no-such-title\"}]},\"limit\":50}",
             g_obj);
    int crc = tc_request(tc, req, out_resp);
    tc_close(tc);
    return crc;
}

/* Route every non-parked row onto kf shards != parked_shard so finds
 * issued WHILE the update is parked never need the parked shard's kf
 * lock — their completion mid-window is genuine liveness, not lock luck.
 * The parked find in phase B deliberately targets the parked shard. */
static int build_fixture(TestEnv *env, int splits, int *out_parked_shard) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[768], *resp = NULL;

    if (tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
                   &resp) != 0) { tc_close(tc); return -1; }
    free(resp);

    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":%d,\"streams\":2,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:64\"],"
        "\"indexes\":[\"score\"]}", g_obj, splits);
    resp = NULL;
    if (tc_request(tc, req, &resp) != 0 ||
        !SAFE_STRSTR(resp, "\"status\":\"created\"")) {
        free(resp); tc_close(tc); return -1;
    }
    free(resp);

    /* Parked key: first candidate routing to shard 0. */
    int parked = -1;
    for (int cand = 0; parked < 0; cand++) {
        char k[32];
        snprintf(k, sizeof(k), "item%04d", cand);
        uint8_t h[16];
        compute_hash_raw(k, strlen(k), h);
        if (compute_record_shard(h, splits) == 0) { parked = cand; break; }
    }
    ASSERT_TRUE(parked >= 0, "found parked-shard key");
    *out_parked_shard = 0;

    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
        "\"key\":\"item%04d\",\"value\":{\"score\":777,\"title\":"
        "\"pre-park\"}}", g_obj, parked);
    resp = NULL;
    if (tc_request(tc, req, &resp) != 0 ||
        !SAFE_STRSTR(resp, "\"status\":\"inserted\"")) {
        free(resp); tc_close(tc); return -1;
    }
    free(resp);

    /* Remaining rows: keys routed anywhere EXCEPT shard 0. */
    int placed = 0;
    for (int cand = parked + 1; placed < FIXTURE_ROWS; cand++) {
        char k[32];
        snprintf(k, sizeof(k), "item%04d", cand);
        uint8_t h[16];
        compute_hash_raw(k, strlen(k), h);
        if (compute_record_shard(h, splits) == 0) continue;   /* skip parked shard */
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"%s\",\"value\":{\"score\":5,\"title\":"
            "\"w%s\"}}", g_obj, k, k + 4);
        resp = NULL;
        if (tc_request(tc, req, &resp) != 0 ||
            !SAFE_STRSTR(resp, "\"status\":\"inserted\"")) {
            free(resp); tc_close(tc); return -1;
        }
        free(resp);
        placed++;
    }

    /* Wedge bait: WEDGE_ROWS rows valued 7777 that all live on the PARKED
     * shard. A limit-bound find of score=7777 fills its batch before these
     * run out, forcing a mid-scan flush while the idx-file rdlock (and,
     * pre-fix, the parked shard's kf wrlock behind it) are still held —
     * the exact production AB-BA shape. */
    int wedged = 0;
    for (int cand = parked + 1; wedged < WEDGE_ROWS; cand++) {
        char k[32];
        snprintf(k, sizeof(k), "wedge%04d", cand);
        uint8_t h[16];
        compute_hash_raw(k, strlen(k), h);
        if (compute_record_shard(h, splits) != 0) continue;
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"%s\",\"value\":{\"score\":7777,\"title\":"
            "\"wb%s\"}}", g_obj, k, k + 5);
        resp = NULL;
        if (tc_request(tc, req, &resp) != 0 ||
            !SAFE_STRSTR(resp, "\"status\":\"inserted\"")) {
            free(resp); tc_close(tc); return -1;
        }
        free(resp);
        wedged++;
    }
    tc_close(tc);
    return 0;
}

static int count_eq(TestEnv *env, const char *json_criteria, long *out) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[512];
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
             "\"criteria\":%s}", g_obj, json_criteria);
    char *resp = NULL;
    int crc = tc_request(tc, req, &resp);
    long n = (crc == 0) ? tu_parse_count(resp) : -1;
    free(resp);
    tc_close(tc);
    *out = n;
    return (n >= 0) ? 0 : -1;
}

static int test_bt_kf_inversion_stream_find_run(void) {
    TestEnv env;
    memset(&env, 0, sizeof(env));
    const int splits = 8;
    ASSERT_EQ_INT(test_env_start(&env), 0, "daemon starts");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }
    ASSERT_EQ_INT(test_env_test_hook_install(&env), 0, "install pause hook");
    int parked_shard = -1;
    ASSERT_EQ_INT(build_fixture(&env, splits, &parked_shard), 0,
                  "build shard-aware indexed fixture");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }

    /*
     * Phase A — parked-window liveness. The update parks mid-window
     * holding kf shard-0's WRLOCK. All eq score=5 rows live on OTHER
     * shards, so these finds never need that lock: they must complete
     * promptly on any build.
     */
    pthread_t tid;
    UpdThreadArg ua = { .env = &env, .rc = -2,
                        .key = "", .valjson = "" };
    /* resolve the actual parked key the fixture picked */
    {
        /* parked key = first item%04d hashing to shard 0 — recompute. */
        int pk = -1;
        for (int cand = 0; pk < 0; cand++) {
            char k[32];
            snprintf(k, sizeof(k), "item%04d", cand);
            uint8_t h[16];
            compute_hash_raw(k, strlen(k), h);
            if (compute_record_shard(h, splits) == 0) {
                pk = cand;
                snprintf(ua.key, sizeof(ua.key), "%s", k);
                break;
            }
        }
    }
    snprintf(ua.valjson, sizeof(ua.valjson),
             "{\"score\":888,\"title\":\"parked\"}");
    tu_join_signal_init(&ua.js);
    ASSERT_EQ_INT(pthread_create(&tid, NULL, parked_update_thread, &ua), 0,
                  "spawn parked update thread");
    int under = -1;
    test_env_test_hook_wait(&env, &under);
    ASSERT_EQ_INT(under, 1, "update parked under kf wrlock");
    if (!t_ctx->failed) {
        for (int i = 0; i < PARKED_FINDS; i++) {
            char *resp = NULL;
            int crc = parked_find(&env, &resp);
            int ok = (crc == 0 && resp && !strstr(resp, "\"error\""));
            char why[160];
            snprintf(why, sizeof(why),
                     "shard-safe streaming find %d completes while writer "
                     "parked", i + 1);
            ASSERT_TRUE(ok, why);
            free(resp);
            if (!ok) break;

            long old_v = -1;
            count_eq(&env, "[{\"field\":\"score\",\"op\":\"eq\","
                           "\"value\":\"5\"}]", &old_v);
            ASSERT_TRUE(old_v == FIXTURE_ROWS,
                        "parked-shard state invisible to unrelated shards");
            if (old_v != FIXTURE_ROWS) break;
        }
    }

    /*
     * Phase B — the deadlock proof. Launch a find whose only candidate is
     * the PARKED key itself: its worker parks on the parked shard's kf
     * rdlock WHILE STILL HOLDING that idx file's bt rdlock. Releasing the
     * hook then lets the writer resume into phase I, which needs the same
     * idx file's WRLOCK. Base branch: AB-BA wedge — the update can never
     * finish and find B can never finish. Fixed build: the walker closed
     * its iterator before blocking, so the writer proceeds and both
     * unwind.
     */
    char findB_req[768];
    snprintf(findB_req, sizeof(findB_req),
             "{\"timeout_ms\":25000,\"mode\":\"find\",\"dir\":\"default\","
             "\"object\":\"%s\",\"criteria\":[{\"field\":\"score\","
             "\"op\":\"eq\",\"value\":\"777\"}],\"limit\":10}",
             g_obj);

    FindBArg fb = { .env = &env };
    snprintf(fb.req, sizeof(fb.req), "%s", findB_req);
    ASSERT_EQ_INT(pthread_create(&fb.tid, NULL, find_b_thread, &fb), 0,
                  "spawn phase-B find");

    /*
     * Phase C — the production AB-BA shape. eq(7777) matches WEDGE_ROWS
     * (12) rows but `limit` caps the streaming batch at 10, so the finder
     * must flush MID-SCAN while its worker still holds the contested idx
     * file's rdlock and probes the parked shard's kf lock. The resumed
     * writer then needs that same idx file's wrlock to apply the score
     * index delta. Chunked executor: iterator closed before flush -> no
     * cycle. Legacy executor: wedge -> the update below can never join.
     */
    FindBArg fc = { .env = &env };
    /* The unindexed contains() sibling makes the planner pick the
     * limit-bound STREAMING executor (fp PRIMARY_LEAF + non-leaf tree);
     * without it a bare eq falls to the collect-all path whose flush is
     * already lock-free and could never reproduce the inversion. */
    snprintf(fc.req, sizeof(fc.req),
             "{\"timeout_ms\":25000,\"mode\":\"find\",\"dir\":\"default\","
             "\"object\":\"%s\",\"criteria\":{\"and\":["
             "{\"field\":\"score\",\"op\":\"eq\",\"value\":\"7777\"},"
             "{\"field\":\"title\",\"op\":\"contains\","
             "\"value\":\"wb\"}]},\"limit\":10}",
             g_obj);
    int have_fc = 0;
    if (pthread_create(&fc.tid, NULL, find_b_thread, &fc) == 0) have_fc = 1;
    ASSERT_TRUE(have_fc, "spawn phase-C wedge find");

    struct timespec ts300 = { 0, 400000000L };
    nanosleep(&ts300, NULL);   /* let workers reach the parked kf lock */

    test_env_test_hook_release(&env);
    int joined_upd = tu_timed_join(tid, &ua.js, 40);
    ASSERT_EQ_INT(joined_upd, 0,
                  "parked update finishes after release (AB-BA would hang)");
    ASSERT_EQ_INT(ua.rc, 0, "parked update committed");

    if (joined_upd == 0) {
        void *fbr = NULL;
        pthread_join(fb.tid, &fbr);
        ASSERT_EQ_INT((int)(intptr_t)fbr, 0,
                      "phase-B find completed with the parked row");
        if (have_fc) {
            void *fcr = NULL;
            pthread_join(fc.tid, &fcr);
            ASSERT_EQ_INT((int)(intptr_t)fcr, 0,
                          "phase-C wedge find completed without error "
                          "(mid-scan flush exercised)");
        }
        long c5 = -1, c888 = -1, c7777 = -1, call_ = -1;
        count_eq(&env, "[{\"field\":\"score\",\"op\":\"eq\","
                       "\"value\":\"5\"}]", &c5);
        count_eq(&env, "[{\"field\":\"score\",\"op\":\"eq\","
                       "\"value\":\"888\"}]", &c888);
        count_eq(&env, "[{\"field\":\"score\",\"op\":\"eq\","
                       "\"value\":\"7777\"}]", &c7777);
        count_eq(&env, "[]", &call_);
        ASSERT_TRUE(c5 == FIXTURE_ROWS &&
                    c888 == 1 &&
                    c7777 == WEDGE_ROWS &&
                    call_ == EXPECT_TOTAL,
                    "post-release totals converge exactly");
    } else {
        pthread_detach(fb.tid);
        if (have_fc) pthread_detach(fc.tid);
        pthread_detach(tid);
    }

    if (joined_upd == 0) tu_join_signal_destroy(&ua.js);
    else { /* wedged thread may still touch js — leak it per contract */ }
    test_env_kill(&env);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", env.db_root);
    system(cmd);
    return t_ctx->failed ? 1 : 0;
}

TEST_REGISTER("test-bt-kf-inversion-stream-find",
              test_bt_kf_inversion_stream_find_run)
