/* src/test/cases/test_crash_safety.c
 *
 * Crash safety on SIGKILL: spawn daemon, insert N committed records,
 * launch more inserts, SIGKILL the daemon mid-flight (no graceful
 * drain), restart against the same DB_ROOT, verify:
 *
 *   1. All committed (acked-to-client) records survive.
 *   2. The post-restart count is in [BASELINE, BASELINE+CRASH_AT].
 *   3. Spot-checked baseline keys are readable verbatim.
 *
 * Per CLAUDE.md crash-safety guarantees: each insert writes flag=0
 * then activates flag=1 in place. A crash between those leaves the
 * slot invisible (flag=0+key_len>0 → swept on next growth). Anything
 * past flag=1 is durable in the kernel page cache and survives the
 * SIGKILL (the daemon dies but the kernel doesn't drop the
 * file-backed mmap until the inode is closed).
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

#define BASELINE  1000   /* records inserted + acked before the crash phase */
#define CRASH_AT   500   /* additional inserts attempted before SIGKILL */

static int test_crash_safety_run(void) {
    /* 1. Persistent on-disk dir tree (caller-owned, survives restart). */
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-crash-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) {
        ASSERT_TRUE(0, "pick port");
        tu_run_cmd("rm -rf %s", base);
        return 1;
    }

    /* 2. First daemon: insert BASELINE clean records. */
    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "first daemon spawn");
        tu_run_cmd("rm -rf %s", base);
        return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "first connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"crash\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:32\"]}",
        &resp);
    free(resp); resp = NULL;

    char req[256];
    int baseline_inserted = 0;
    for (int i = 0; i < BASELINE; i++) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"crash\","
            "\"key\":\"k%07d\",\"value\":{\"v\":\"val_%d\"}}", i, i);
        if (tc_request(tc, req, &resp) != 0) break;
        free(resp); resp = NULL;
        baseline_inserted++;
    }
    ASSERT_EQ_INT(baseline_inserted, BASELINE, "all baseline inserts succeeded");

    /* Verify daemon's view of the count matches what we sent. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"crash\"}",
        &resp);
    int count_pre = tu_parse_count(resp);
    free(resp); resp = NULL;
    ASSERT_EQ_INT(count_pre, BASELINE, "baseline count matches pre-crash");

    /* 3. Crash phase: send more inserts; SIGKILL halfway through. */
    int crash_phase_committed = 0;
    int kill_after = CRASH_AT / 2;
    for (int i = BASELINE; i < BASELINE + CRASH_AT; i++) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"crash\","
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

    /* 4. Restart daemon at the same DB_ROOT + port. */
    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn at same db_root");
        tu_run_cmd("rm -rf %s", base);
        return 1;
    }

    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after restart");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    /* 5. Recount: must be in [BASELINE, BASELINE+CRASH_AT] and
          ≥ BASELINE+crash_phase_committed (every record acked before
          the kill must survive). */
    tc_request(tc,
        "{\"mode\":\"recount\",\"dir\":\"default\",\"object\":\"crash\"}",
        &resp);
    int count_after = tu_parse_count(resp);
    free(resp); resp = NULL;

    ASSERT_TRUE(count_after >= BASELINE,
                "post-crash count >= baseline (committed survives)");
    ASSERT_TRUE(count_after <= BASELINE + CRASH_AT,
                "post-crash count <= total attempted");
    ASSERT_TRUE(count_after >= BASELINE + crash_phase_committed,
                "every acked-before-crash record survives");

    /* 6. Spot-check: every baseline key (in 100-step strides) must
          be readable and intact. This is the strongest correctness
          assertion — count alone could be right while data is
          corrupted; this checks actual record content is recoverable. */
    int spot_check_failures = 0;
    for (int i = 0; i < BASELINE; i += 100) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"crash\","
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

    /* 7. Sanity: a key from past CRASH_AT (never attempted) must be a miss. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"crash\","
        "\"key\":\"k%07d\"}", BASELINE + CRASH_AT + 9999);
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && (SAFE_STRSTR(resp, "Not found") || SAFE_STRSTR(resp, "error")),
                "never-attempted key is absent (no phantom records)");
    free(resp); resp = NULL;

    /* 8. Clean teardown. */
    tc_close(tc);
    test_env_stop(&env2);   /* env2 wipes its own db_root via test_env_stop */
    /* test_env_stop's cleanup walked one level up from db_root, which
       is `base` — so the parent dir is gone. Belt-and-suspenders: */
    tu_run_cmd("rm -rf %s", base);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-crash-safety", test_crash_safety_run)
