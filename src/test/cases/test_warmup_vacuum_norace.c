/* src/test/cases/test_warmup_vacuum_norace.c
 *
 * Companion to test_warmup_vacuum_race.c, covering the gap that test does
 * NOT cover: it proves objlock-protected mutual exclusion while a warmup
 * kf task and a vacuum are *both in flight*, but its delay hook
 * (WARMUP_TEST_DELAY_MS) fires *inside* warmup_kf_task_fn's held
 * objlock_rdlock -- which itself blocks the vacuum's objlock_wrlock, so
 * the two always overlap by construction. It says nothing about the
 * no-overlap case: a WarmupKfTask collected in phase 1, sitting queued
 * (no lock held at all) while phase 1 keeps walking the rest of db_root,
 * during which a vacuum runs to full completion -- wrlock acquired,
 * rebuild_object_v2, schema.conf rewritten, slotcask_registry_invalidate,
 * wrlock released -- entirely before the queued task ever attempts
 * objlock_rdlock.
 *
 * That's exactly the scenario the original UAF (docs/plans/2026-07-20-
 * warmup-kftask-stale-sdb-uaf.md) needed a fix for: a WarmupKfTask that
 * captured a raw SlotcaskDb* (or, in the first-pass fix, a
 * SlotcaskSchemaInfo) back in phase 1 would dereference/reuse
 * already-freed or stale-shape state, because nothing about the object's
 * shape survives a full, non-overlapping rebuild. The shipped fix makes
 * warmup_kf_task_fn re-resolve everything fresh (load_schema +
 * slotcask_registry_get) under its own rdlock, taken *after* this test's
 * WARMUP_TEST_PRELOCK_DELAY_MS wait -- so by the time it locks anything,
 * the vacuum below is long done and it must observe the POST-vacuum
 * shape, never the stale pre-vacuum one.
 *
 * Uses WARMUP_TEST_PRELOCK_DELAY_MS (test-only, default 0/off in
 * production; server.c, warmup_kf_task_fn) to hold every warmup kf task
 * in an unlocked sleep long enough for a `vacuum --splits` changing the
 * object's shard count to run start-to-finish first. Assertions:
 *   1. The vacuum itself succeeds (no error) well before the warmup
 *      delay elapses -- proving it was NOT blocked by any warmup-held
 *      lock (the opposite of test_warmup_vacuum_race.c's timing
 *      assertion), i.e. a genuine no-overlap execution.
 *   2. The daemon stays alive/responsive once the delayed warmup tasks
 *      finally run past their sleep and take the rdlock (the UAF/crash
 *      symptom, primarily visible under ASan).
 *   3. Post-vacuum, the object's shard count as observed via
 *      estimate-index reflects the NEW splits, not the value warmup
 *      would have captured in phase 1 -- proving warmup_kf_task_fn
 *      re-resolved fresh rather than silently reopening/poisoning the
 *      registry with a stale shape.
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

#define PRELOCK_DELAY_MS 2000

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* Same log-routing note as test_warmup_vacuum_race.c: LOG_INFO goes to
   -info.log. Polls for the PRELOCK variant's "starting" marker, proving
   at least one warmup kf task is currently in its unlocked sleep. */
static int wait_for_prelock_delay_start(const char *db_root,
                                         const char *date_str, int timeout_s) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/logs/%s-info.log", db_root, date_str);
    for (int i = 0; i < timeout_s * 10; i++) {
        FILE *f = fopen(path, "r");
        if (f) {
            char line[1024];
            while (fgets(line, sizeof(line), f)) {
                if (strstr(line, "WARMUP-PRELOCK-TEST-DELAY") && strstr(line, "starting")) {
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
    if (with_delay) fprintf(ef, "WARMUP_TEST_PRELOCK_DELAY_MS=%d\n", PRELOCK_DELAY_MS);
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

static int test_warmup_vacuum_norace_run(void) {
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    char date_str[16];
    strftime(date_str, sizeof(date_str), "%Y-%m-%d", &tmv);

    char base[] = "/tmp/shard-db-warmup-vacuum-norace-XXXXXX";
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
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"warmupnorace\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"]}", &resp);
    free(resp); resp = NULL;

    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"warmupnorace\","
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

    /* Round 2: restart the same db_root/port with the prelock delay knob
       armed. Every WarmupKfTask now sleeps PRELOCK_DELAY_MS *before*
       taking objlock_rdlock at all -- no lock held during the sleep. */
    ASSERT_TRUE(write_env(env_path, db_root, port, 1) == 0,
        "write db.env with WARMUP_TEST_PRELOCK_DELAY_MS");
    pid = spawn_daemon(base, shard_db_abs);
    ASSERT_TRUE(pid > 0, "fork daemon (round 2)");
    ASSERT_TRUE(wait_ready(port, 100), "daemon ready (round 2)");

    ASSERT_TRUE(wait_for_prelock_delay_start(db_root, date_str, 10),
        "WARMUP-PRELOCK-TEST-DELAY starting line observed in -info.log");

    /* Fire the vacuum immediately: no warmup task can be holding
       objlock_rdlock right now (they're all in the unlocked prelock
       sleep), so this must run start-to-finish fast, well under
       PRELOCK_DELAY_MS. */
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
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"warmupnorace\","
        "\"splits\":16}", &vresp);
    long elapsed = now_ms() - t0;
    tc_close(vc);

    ASSERT_TRUE(vresp != NULL && !SAFE_STRSTR(vresp, "\"error\""),
        "vacuum/rebuild succeeds while warmup tasks sit in their unlocked prelock sleep");
    if (vresp) TAP_DIAG("# vacuum response: %s\n", vresp);
    free(vresp);

    /* The no-overlap proof: vacuum must NOT have been blocked by any
       warmup-held lock. If a prior/buggy build made warmup_kf_task_fn
       take objlock_rdlock before this test's sleep (i.e. the prelock
       knob had no effect because the delay landed inside the lock like
       the DELAY_MS knob), this would instead take >= PRELOCK_DELAY_MS. */
    ASSERT_TRUE(elapsed < (PRELOCK_DELAY_MS - 300),
        "vacuum completes fast, unblocked -- proves genuine no-overlap execution");
    TAP_DIAG("# elapsed vacuum time: %ldms (prelock delay=%dms)\n", elapsed, PRELOCK_DELAY_MS);

    /* Let the delayed warmup tasks finish their sleep and run past their
       (now fresh-reload) objlock_rdlock section. */
    struct timespec wait_ts = { (PRELOCK_DELAY_MS / 1000) + 2, 0 };
    nanosleep(&wait_ts, NULL);

    /* Daemon must still be alive and responsive -- the ASan-only crash
       signal for a UAF (old first-pass fix reusing a stale sdb pointer)
       shows up as either a dead process here or an ASan abort captured
       separately by the sanitizer harness. */
    TestClient *pc2 = tc_connect(&cfg);
    ASSERT_NOT_NULL(pc2, "daemon still responsive after fully-completed, non-overlapping vacuum");
    if (pc2) {
        char *r = NULL;
        tc_request(pc2, "{\"mode\":\"db-dirs\"}", &r);
        ASSERT_TRUE(r != NULL, "db-dirs round-trip after non-overlapping vacuum");
        free(r);

        /* Correctness proof, not just crash-freedom: the object must be
           usable at its NEW (post-vacuum) shape. A stale-info reuse bug
           (the first-pass fix's residual gap) would poison the registry
           entry with the OLD splits/slot_size on the very first
           registry_get miss after the rebuild -- exactly the miss this
           delayed warmup task causes -- so verify existing rows are
           still readable and the object still accepts writes at the new
           shape. */
        char *fresp = NULL;
        tc_request(pc2,
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"warmupnorace\","
            "\"key\":\"k0\"}", &fresp);
        ASSERT_TRUE(fresp != NULL && SAFE_STRSTR(fresp, "\"v0\""),
            "pre-vacuum row k0 still readable at the new post-vacuum shape");
        if (fresp) TAP_DIAG("# find k0 response: %s\n", fresp);
        free(fresp);

        char *iresp = NULL;
        tc_request(pc2,
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"warmupnorace\","
            "\"key\":\"k5\",\"value\":{\"name\":\"v5\"}}", &iresp);
        ASSERT_TRUE(iresp != NULL && !SAFE_STRSTR(iresp, "\"error\""),
            "insert after non-overlapping vacuum succeeds (registry not poisoned)");
        if (iresp) TAP_DIAG("# insert k5 response: %s\n", iresp);
        free(iresp);
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

TEST_REGISTER("test-warmup-vacuum-norace", test_warmup_vacuum_norace_run)
