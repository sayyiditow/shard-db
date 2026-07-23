/* test_slotcask_v2_crash.c — Phase-8 crash safety on v2 (slotcask).
 *
 * Mirrors test_crash_safety but creates a v2 object. Validates:
 *
 *   1. Every record acked before the SIGKILL survives the restart
 *      (committed records are durable — keyfile flip is the commit
 *      point, atomic 8B store, hits the page cache before ack).
 *   2. Post-crash count is in [BASELINE, BASELINE + CRASH_AT].
 *   3. Spot-checked baseline keys are readable verbatim.
 *   4. recover_streams runs on the post-crash slotcask_open: a fresh
 *      insert after the restart does NOT clobber a live record (this
 *      tests the Phase-5 fix to slotcask_open that always re-derives
 *      reserve_off from disk).
 *   5. A key past the attempted range is absent (no phantom records).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define BASELINE  1000
#define CRASH_AT   500

static int crash_run_cmd(const char *fmt, ...) {
    char cmd[2048];
    va_list ap; va_start(ap, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    return system(cmd);
}

static int crash_parse_count(const char *resp) {
    if (!resp) return -1;
    while (*resp == ' ' || *resp == '\n') resp++;
    if (*resp == '{') {
        const char *p = SAFE_STRSTR(resp, "\"count\":");
        return p ? atoi(p + 8) : -1;
    }
    return atoi(resp);
}

static int test_slotcask_v2_crash_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-v2crash-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    crash_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) {
        ASSERT_TRUE(0, "pick port");
        crash_run_cmd("rm -rf %s", base);
        return 1;
    }

    /* === First daemon: seed BASELINE committed records on a v2 object. === */
    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "first daemon spawn");
        crash_run_cmd("rm -rf %s", base);
        return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "first connect");
    if (!tc) { test_env_kill(&env); crash_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"crash2\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:32\"]}", &resp);
    free(resp); resp = NULL;

    char req[256];
    int baseline_inserted = 0;
    for (int i = 0; i < BASELINE; i++) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"crash2\","
            "\"key\":\"k%07d\",\"value\":{\"v\":\"val_%d\"}}", i, i);
        if (tc_request(tc, req, &resp) != 0) break;
        free(resp); resp = NULL;
        baseline_inserted++;
    }
    ASSERT_EQ_INT(baseline_inserted, BASELINE, "all baseline inserts succeeded");

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"crash2\"}", &resp);
    int count_pre = crash_parse_count(resp);
    free(resp); resp = NULL;
    ASSERT_EQ_INT(count_pre, BASELINE, "baseline count matches pre-crash");

    /* === Crash phase: send more inserts; SIGKILL halfway. === */
    int crash_phase_committed = 0;
    int kill_after = CRASH_AT / 2;
    for (int i = BASELINE; i < BASELINE + CRASH_AT; i++) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"crash2\","
            "\"key\":\"k%07d\",\"value\":{\"v\":\"val_%d\"}}", i, i);
        if (tc_request(tc, req, &resp) != 0) {
            free(resp); resp = NULL;
            break;
        }
        free(resp); resp = NULL;
        crash_phase_committed++;
        if (crash_phase_committed >= kill_after) {
            test_env_kill(&env);
            break;
        }
    }
    tc_close(tc); tc = NULL;

    /* === Restart at the same DB_ROOT + port. === */
    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn at same db_root");
        crash_run_cmd("rm -rf %s", base);
        return 1;
    }

    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after restart");
    if (!tc) { test_env_kill(&env2); crash_run_cmd("rm -rf %s", base); return 1; }

    /* Recount via the wire — slotcask_walk_live counts live entries. */
    tc_request(tc,
        "{\"mode\":\"recount\",\"dir\":\"default\",\"object\":\"crash2\"}", &resp);
    int count_after = crash_parse_count(resp);
    free(resp); resp = NULL;

    ASSERT_TRUE(count_after >= BASELINE,
                "post-crash count >= baseline (committed survives)");
    ASSERT_TRUE(count_after <= BASELINE + CRASH_AT,
                "post-crash count <= total attempted");
    ASSERT_TRUE(count_after >= BASELINE + crash_phase_committed,
                "every acked-before-crash record survives");

    /* Spot-check baseline keys (every 100th) — content survived intact. */
    int spot_check_failures = 0;
    for (int i = 0; i < BASELINE; i += 100) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"crash2\","
            "\"key\":\"k%07d\"}", i);
        if (tc_request(tc, req, &resp) != 0 || !resp) {
            spot_check_failures++;
        } else {
            char expected[64];
            snprintf(expected, sizeof(expected), "\"val_%d\"", i);
            if (!SAFE_STRSTR(resp, expected)) spot_check_failures++;
        }
        free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(spot_check_failures, 0,
                  "all baseline keys readable + intact post-crash");

    /* recover_streams correctness check: the post-restart slotcask_open
       must re-derive reserve_off from disk, otherwise the next insert
       would clobber the head record at offset 0. Insert a fresh key,
       then read back BOTH the new key and a baseline key from offset
       0's hash region — both must be intact. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"crash2\","
        "\"key\":\"post_recovery\",\"value\":{\"v\":\"fresh_after_crash\"}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"",
                    "post-crash insert succeeds");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"crash2\","
        "\"key\":\"post_recovery\"}", &resp);
    ASSERT_CONTAINS(resp, "\"v\":\"fresh_after_crash\"",
                    "post-crash insert read-back works");
    free(resp); resp = NULL;

    /* The key whose hash routes to slot 0 of stream 0's segment 0 is
       implementation-specific, but k0000000 is one of the earliest
       inserted records and must still be readable — proves the
       post-recovery insert didn't clobber early records. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"crash2\","
        "\"key\":\"k0000000\"}", &resp);
    ASSERT_CONTAINS(resp, "\"v\":\"val_0\"",
                    "k0000000 still intact after post-recovery insert "
                    "(reserve_off derived correctly)");
    free(resp); resp = NULL;

    /* Sanity: a key past CRASH_AT (never attempted) is absent. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"crash2\","
        "\"key\":\"k%07d\"}", BASELINE + CRASH_AT + 9999);
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && (SAFE_STRSTR(resp, "Not found") || SAFE_STRSTR(resp, "error")),
                "never-attempted key is absent (no phantom records)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env2);
    crash_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-v2-crash", test_slotcask_v2_crash_run)
