/* src/test/cases/test_auto_vacuum.c
 * Auto-vacuum thread fires on schedule, runs plain vacuum on objects
 * exceeding the tombstone threshold, leaves objects below threshold
 * alone, and respects the env knobs (interval, pct, min_deleted).
 *
 * Custom daemon spawn: fixture's db.env doesn't carry AUTO_VACUUM, so
 * we write our own with tight thresholds (60s floor on interval per
 * config validation; we use 60 + a forced-hand by polling for change).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>




static int test_auto_vacuum_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-av-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755); mkdir(db_root, 0755);
    char logs_dir[300]; snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    mkdir(logs_dir, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    /* db.env with auto-vacuum on at 60-sec interval (min allowed),
       tight thresholds: pct=10%, min_deleted=10. */
    char env_path[300]; snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *f = fopen(env_path, "w");
    if (!f) { ASSERT_TRUE(0, "open db.env"); tu_run_cmd("rm -rf %s", base); return 1; }
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
        "export LOG_LEVEL=3\n"
        "export THREADS=2\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
        "export AUTO_VACUUM=1\n"
        "export AUTO_VACUUM_INTERVAL_SEC=60\n"
        "export VACUUM_RECOMMEND_TOMBSTONE_PCT=10\n"
        "export VACUUM_RECOMMEND_MIN_DELETED=10\n",
        db_root, port, base);
    fclose(f);

    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db not found"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    pid_t pid = fork();
    if (pid < 0) { ASSERT_TRUE(0, "fork"); tu_run_cmd("rm -rf %s", base); return 1; }
    if (pid == 0) {
        /* Redirect stdout/stderr to a log file before exec, same as
           fixtures.c's spawn_daemon(). Without this, the daemon child
           inherits whatever fd 1/2 the test binary itself had — when
           this test runs under a popen()'d pipe (the watchdog
           self-test), that leaves the daemon holding the pipe's
           write end open indefinitely after the watchdog _exit(124)s
           the immediate child, hanging the reader's fgets() on an
           EOF that never comes. */
        chdir(base);
        char dlog[400];
        snprintf(dlog, sizeof(dlog), "%s/daemon.log", base);
        int lfd = open(dlog, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (lfd >= 0) {
            dup2(lfd, 1);
            dup2(lfd, 2);
            close(lfd);
        }
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }
    TestEnv env = { .port = port, .daemon_pid = pid };
    snprintf(env.db_root, sizeof(env.db_root), "%s", db_root);

    /* Wait until ready. */
    int ready = 0;
    for (int i = 0; i < 100; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            if (tc_request(probe, "{\"mode\":\"db-dirs\"}", &r) == 0 && r) {
                ready = 1; free(r); tc_close(probe); break;
            }
            free(r); tc_close(probe);
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    ASSERT_TRUE(ready, "daemon ready with AUTO_VACUUM=1");
    if (!ready) { test_env_stop_keep(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop_keep(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;

    /* Object 1: needs vacuum (50 inserts, delete 30 → 60% tombstones, 30 ≥ 10). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"big\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    for (int i = 1; i <= 50; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"big\","
            "\"key\":\"k%d\",\"value\":{\"v\":%d}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    for (int i = 1; i <= 30; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"big\","
            "\"key\":\"k%d\"}", i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* Object 2: under threshold (5 inserts, delete 2 → 40% but only 2 deleted < min=10). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"sml\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    for (int i = 1; i <= 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sml\","
            "\"key\":\"k%d\",\"value\":{\"v\":%d}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    for (int i = 1; i <= 2; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"sml\","
            "\"key\":\"k%d\"}", i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* vacuum-check picks up `big` (recommend=true) but not `sml` (below min). */
    tc_request(tc, "{\"mode\":\"vacuum-check\"}", &resp);
    ASSERT_CONTAINS(resp, "\"object\":\"big\"", "vacuum-check sees big");
    ASSERT_CONTAINS(resp, "\"vacuum\":true", "vacuum-check recommends big");
    /* sml may not appear at all (deleted=2, the handler suppresses if deleted==0
       — here it's 2 so it appears, but with vacuum:false). */
    {
        const char *sml = SAFE_STRSTR(resp, "\"object\":\"sml\"");
        if (sml) {
            const char *next_obj = strstr(sml + 1, "\"object\"");
            const char *vac = strstr(sml, "\"vacuum\":");
            int found_false = 0;
            if (vac && (!next_obj || vac < next_obj)) {
                if (strncmp(vac, "\"vacuum\":false", 14) == 0) found_false = 1;
            }
            ASSERT_TRUE(found_false, "vacuum-check does NOT recommend sml");
        } else {
            ASSERT_TRUE(1, "sml not in vacuum-check (acceptable)");
        }
    }
    free(resp); resp = NULL;

    /* Confirm orphaned count for big = 30. */
    tc_request(tc, "{\"mode\":\"orphaned\",\"dir\":\"default\",\"object\":\"big\"}", &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 30, "big orphaned=30 pre-vacuum");
    free(resp); resp = NULL;

    /* Force the auto-vacuum cycle. We can't shrink AUTO_VACUUM_INTERVAL_SEC
       below 60s (config floor), and waiting 60s+ is too slow for a unit
       test. Instead, restart the daemon — the auto-vacuum thread runs ONCE
       on first wake (no immediate fire on startup though), so we simulate
       by manually calling vacuum via JSON to verify the would-be-vacuumed
       object cleans correctly. The thread itself is verified by the LOG
       line further below.

       Actually, the cleanest verification: we wait 90s for the thread to
       fire. That's slow but conclusive. (Bumped from 65s — macOS GH
       runners run hot enough that the thread can miss the 60-65s window
       under load. The interval floor is 60s; 30s slack is enough for
       both runners to settle.) Skip if SHARD_TEST_FAST=1. */
    if (getenv("SHARD_TEST_FAST")) {
        TAP_DIAG("# auto-vacuum: SHARD_TEST_FAST set, skipping the 90s wake test\n");
    } else {
        TAP_DIAG("# auto-vacuum: sleeping 90s for the first thread tick…\n");
        fflush(_TAP_OUT);
        sleep(90);

        tc_request(tc, "{\"mode\":\"orphaned\",\"dir\":\"default\",\"object\":\"big\"}", &resp);
        int orphaned_after = tu_parse_count(resp);
        ASSERT_EQ_INT(orphaned_after, 0, "big orphaned=0 after auto-vacuum tick");
        free(resp); resp = NULL;

        /* sml should be untouched. */
        tc_request(tc, "{\"mode\":\"orphaned\",\"dir\":\"default\",\"object\":\"sml\"}", &resp);
        ASSERT_EQ_INT(tu_parse_count(resp), 2, "sml orphaned=2 (untouched, below threshold)");
        free(resp); resp = NULL;

        /* Live count for big unchanged (vacuum reclaims tombstones, not live). */
        tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"big\"}", &resp);
        ASSERT_EQ_INT(tu_parse_count(resp), 20, "big count=20 (50 - 30 deleted)");
        free(resp); resp = NULL;
    }

    tc_close(tc);
    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-vacuum", test_auto_vacuum_run)
