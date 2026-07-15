/* src/test/cases/test_warmup_vacuum_race.c
 *
 * Regression test for a use-after-free race: the startup warmup thread
 * (WARMUP=async, the default) fans out one WarmupKfTask per kf shard onto
 * the I/O pool, each holding a bare SlotcaskDb* obtained from
 * slotcask_registry_get() with no lock. A concurrent `vacuum` with a
 * splits change (cmd_vacuum -> rebuild_object -> rebuild_object_v2 ->
 * slotcask_registry_invalidate) frees that SlotcaskDb while a queued or
 * in-flight warmup task still dereferences it (warmup_kf_task_fn reading
 * sdb->data_dir / sdb->slots_per_shard) -- a heap-use-after-free caught by
 * ASan on test-rebuild-recovery (see docs/plans/2026-07-15-auto-reshard-
 * shutdown-race.md, "the warmup thread is now in scope").
 *
 * Uses WARMUP_TEST_DELAY_MS (test-only, default 0/off in production) to
 * deterministically widen warmup_kf_task_fn's objlock-held window, then
 * fires a real `vacuum --splits` the instant a warmup task's "starting"
 * log line appears. Two assertions distinguish objlock-protected (fixed)
 * from unprotected (buggy) warmup:
 *   1. The vacuum response succeeds and the daemon stays alive/responsive
 *      afterward (the crash symptom itself, only reliably visible under
 *      ASan -- ASan CI is the primary way this regresses).
 *   2. Vacuum's elapsed time is at least ~(WARMUP_TEST_DELAY_MS - 300)ms:
 *      pre-fix, cmd_vacuum's objlock_wrlock() races the unprotected
 *      warmup task and returns almost instantly; post-fix, it blocks on
 *      pthread_rwlock_wrlock() until the warmup task's held rdlock (which
 *      spans the injected delay) releases. This timing assertion is the
 *      primary, robust proof -- it fails deterministically pre-fix and
 *      passes deterministically post-fix even on a non-ASan build where
 *      the UAF happens not to crash.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#define DELAY_MS 2000

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* Poll today's INFO log for a "WARMUP-TEST-DELAY: ... starting" line --
   proves a warmup kf task is currently inside its objlock-held critical
   section. Same log-routing note as test_auto_reshard_shutdown_race.c:
   LOG_INFO goes to -info.log. */
static int wait_for_warmup_delay_start(const char *db_root,
                                        const char *date_str, int timeout_s) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/logs/%s-info.log", db_root, date_str);
    for (int i = 0; i < timeout_s * 10; i++) {
        FILE *f = fopen(path, "r");
        if (f) {
            char line[1024];
            while (fgets(line, sizeof(line), f)) {
                if (strstr(line, "WARMUP-TEST-DELAY") && strstr(line, "starting")) {
                    fclose(f);
                    return 1;
                }
            }
            fclose(f);
        }
        usleep(100000);
    }
    return 0;
}

static int write_env(const char *env_path, const char *db_root, int port,
                      int with_delay) {
    FILE *ef = fopen(env_path, "w");
    if (!ef) return -1;
    fprintf(ef,
        "DB_ROOT=%s\nPORT=%d\nTIMEOUT=0\nTHREADS=2\nFCACHE_MAX=4096\nTLS_ENABLE=0\n",
        db_root, port);
    if (with_delay) fprintf(ef, "WARMUP_TEST_DELAY_MS=%d\n", DELAY_MS);
    fclose(ef);
    return 0;
}

static pid_t spawn_daemon(const char *base, const char *shard_db_abs) {
    pid_t pid = fork();
    if (pid == 0) {
        chdir(base);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }
    return pid;
}

static int wait_ready(int port, int tries) {
    for (int i = 0; i < tries; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            if (tc_request(probe, "{\"mode\":\"db-dirs\"}", &r) == 0 && r) {
                free(r); tc_close(probe); return 1;
            }
            free(r); tc_close(probe);
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    return 0;
}

static int test_warmup_vacuum_race_run(void) {
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    char date_str[16];
    strftime(date_str, sizeof(date_str), "%Y-%m-%d", &tmv);

    char base[] = "/tmp/shard-db-warmup-vacuum-race-XXXXXX";
    if (!mkdtemp(base)) { ASSERT_TRUE(0, "mkdtemp"); return 1; }

    int port = test_pick_port();
    char db_root[PATH_MAX];
    snprintf(db_root, sizeof(db_root), "%s/root", base);
    mkdir(db_root, 0755);

    char env_path[PATH_MAX];
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    ASSERT_TRUE(write_env(env_path, db_root, port, 0) == 0, "write initial db.env");

    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db not found");
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    /* Round 1: plain daemon, create the object with real kf shards on
       disk, then clean-shutdown so round 2's warmup thread has something
       to enumerate from a cold registry. */
    pid_t pid = spawn_daemon(base, shard_db_abs);
    ASSERT_TRUE(pid > 0, "fork daemon (round 1)");
    ASSERT_TRUE(wait_ready(port, 100), "daemon ready (round 1)");

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect (round 1)");
    if (!tc) {
        kill(pid, SIGKILL); int st; waitpid(pid, &st, 0);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"warmuprace\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"]}", &resp);
    free(resp); resp = NULL;

    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"warmuprace\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"v%d\"}}", i, i);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }
    tc_close(tc);

    kill(pid, SIGTERM);
    int st1 = 0;
    for (int i = 0; i < 100; i++) {
        if (waitpid(pid, &st1, WNOHANG) == pid) break;
        usleep(100000);
    }

    /* Round 2: restart the same db_root/port with the delay knob armed. */
    ASSERT_TRUE(write_env(env_path, db_root, port, 1) == 0, "write db.env with WARMUP_TEST_DELAY_MS");
    pid = spawn_daemon(base, shard_db_abs);
    ASSERT_TRUE(pid > 0, "fork daemon (round 2)");
    ASSERT_TRUE(wait_ready(port, 100), "daemon ready (round 2)");

    ASSERT_TRUE(wait_for_warmup_delay_start(db_root, date_str, 10),
        "WARMUP-TEST-DELAY starting line observed in -info.log");

    TestClient *vc = tc_connect(&cfg);
    ASSERT_NOT_NULL(vc, "connect for vacuum");
    if (!vc) {
        kill(pid, SIGKILL); int st; waitpid(pid, &st, 0);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    long t0 = now_ms();
    char *vresp = NULL;
    tc_request(vc,
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"warmuprace\","
        "\"splits\":16}", &vresp);
    long elapsed = now_ms() - t0;
    tc_close(vc);

    ASSERT_TRUE(vresp != NULL && !SAFE_STRSTR(vresp, "\"error\""),
        "vacuum/rebuild succeeds despite racing an in-flight warmup task");
    if (vresp) TAP_DIAG("# vacuum response: %s\n", vresp);
    free(vresp);

    ASSERT_TRUE(elapsed >= (DELAY_MS - 300),
        "vacuum's objlock_wrlock blocks on the warmup task's held rdlock (proves serialization, not a race)");
    TAP_DIAG("# elapsed vacuum time: %ldms (delay=%dms)\n", elapsed, DELAY_MS);

    /* Daemon must still be alive and responsive -- the ASan-only crash
       signal for the pre-fix case shows up as either a dead process here
       or an ASan abort captured separately by the sanitizer harness. */
    TestClient *pc2 = tc_connect(&cfg);
    ASSERT_NOT_NULL(pc2, "daemon still responsive after racing vacuum");
    if (pc2) {
        char *r = NULL;
        tc_request(pc2, "{\"mode\":\"db-dirs\"}", &r);
        ASSERT_TRUE(r != NULL, "db-dirs round-trip after racing vacuum");
        free(r);
        tc_close(pc2);
    }

    kill(pid, SIGTERM);
    int st2 = 0;
    int exited = 0;
    for (int i = 0; i < 100; i++) {
        if (waitpid(pid, &st2, WNOHANG) == pid) { exited = 1; break; }
        usleep(100000);
    }
    if (!exited) { kill(pid, SIGKILL); waitpid(pid, &st2, 0); }

    char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-warmup-vacuum-race", test_warmup_vacuum_race_run)
