#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int write_env(const char *env_dir, const char *db_root,
                     const char *warmup_line, int warmup_delay_ms) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/db.env", env_dir);
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f,
            "DB_ROOT=%s\nLOG_LEVEL=4\nTHREADS=2\nIO_THREADS=4\n"
            "FCACHE_MAX=4096\nWARMUP_TEST_DELAY_MS=%d\n",
            db_root, warmup_delay_ms);
    if (warmup_line) fprintf(f, "WARMUP=%s\n", warmup_line);
    return fclose(f);
}

static int run_harness(const char *env_dir, const char *db_root,
                       int hold_ms, int cycles, const char *callback_log,
                       int *timed_out) {
    char harness[PATH_MAX];
    if (!realpath("./build/bin/embedded_bg_harness", harness)) return -1;
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        char hold[32], count[32];
        snprintf(hold, sizeof(hold), "%d", hold_ms);
        snprintf(count, sizeof(count), "%d", cycles);
        execl(harness, harness, env_dir, db_root, hold, count,
              callback_log, (char *)NULL);
        _exit(127);
    }

    /* 30s budget matches the convention used elsewhere for daemon/fork
       tests (e.g. test_btcache_evict_race.c) — GitHub-hosted runners are
       2-4 vCPU and --jobs 4 test-runner contention can stretch a real
       open/insert/close cycle well past a couple hundred ms of headroom. */
    for (int waited = 0; waited < 30000; waited += 50) {
        int status = 0;
        pid_t got = waitpid(pid, &status, WNOHANG);
        if (got == pid)
            return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        struct timespec delay = { 0, 50 * 1000000L };
        nanosleep(&delay, NULL);
    }
    *timed_out = 1;
    kill(pid, SIGTERM);
    for (int i = 0; i < 20; i++) {
        int status;
        if (waitpid(pid, &status, WNOHANG) == pid) return -1;
        struct timespec delay = { 0, 25 * 1000000L };
        nanosleep(&delay, NULL);
    }
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
    return -1;
}

static int test_embedded_bg_threads_run(void) {
    char base[256], db_root[300], callback[320];
    snprintf(base, sizeof(base), "/tmp/shard-db-ebg-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    ASSERT_EQ_INT(write_env(base, db_root, "off", 0), 0,
                  "write embedded durability config");
    snprintf(callback, sizeof(callback), "%s/core.log", base);
    int timed_out = 0;
    ASSERT_EQ_INT(run_harness(base, db_root, 400, 2, callback, &timed_out), 0,
                  "embedded open-close-open harness exits cleanly");
    ASSERT_EQ_INT(timed_out, 0, "embedded lifecycle completes before timeout");

    /* A third process opens and reads the same record after cycle 2 closed,
       proving the second shutdown left reusable, readable state. */
    ASSERT_EQ_INT(write_env(base, db_root, "off", 0), 0,
                  "disable background work for post-close read");
    snprintf(callback, sizeof(callback), "%s/reopen.log", base);
    timed_out = 0;
    ASSERT_EQ_INT(run_harness(base, db_root, 0, 1, callback, &timed_out), 0,
                  "data remains readable after second close");

    /* No WARMUP= line: embedded mode must keep its resolved default off. */
    ASSERT_EQ_INT(write_env(base, db_root, NULL, 300), 0,
                  "write embedded config without WARMUP");
    snprintf(callback, sizeof(callback), "%s/warmup-default.log", base);
    timed_out = 0;
    ASSERT_EQ_INT(run_harness(base, db_root, 450, 1, callback, &timed_out), 0,
                  "embedded default-off warmup lifecycle exits");
    char *default_log = tu_read_file(callback);
    ASSERT_NOT_NULL(default_log, "read default warmup callback log");
    if (default_log) {
        ASSERT_TRUE(strstr(default_log, "WARMUP-TEST-DELAY") == NULL,
                    "implicit embedded warmup does not start");
        ASSERT_TRUE(strstr(default_log, "WARMUP done") == NULL,
                    "implicit embedded warmup has no completion event");
    }
    free(default_log);

    /* Explicit async is opt-in and remains joinable through close. */
    ASSERT_EQ_INT(write_env(base, db_root, "async", 300), 0,
                  "write explicit embedded async warmup config");
    snprintf(callback, sizeof(callback), "%s/warmup-async.log", base);
    timed_out = 0;
    ASSERT_EQ_INT(run_harness(base, db_root, 500, 1, callback, &timed_out), 0,
                  "explicit async warmup runs and joins");
    char *async_log = tu_read_file(callback);
    ASSERT_NOT_NULL(async_log, "read async warmup callback log");
    if (async_log) {
        ASSERT_CONTAINS(async_log, "WARMUP-TEST-DELAY",
                        "explicit async warmup reaches delayed task marker");
        ASSERT_CONTAINS(async_log, "WARMUP done",
                        "explicit async warmup completes before close returns");
    }
    free(async_log);

    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed ? 1 : 0;
}

TEST_REGISTER("test-embedded-bg-threads", test_embedded_bg_threads_run)
