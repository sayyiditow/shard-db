/* src/test/cases/test_nql_no_objlock_contention.c
 * Finding 4 regression: dispatch_nql_query used to take an unconditional
 * objlock_rdlock before its find/count/aggregate switch, even though every
 * NqlMode is a read and dispatch_json_query's own mode_is_write/
 * mode_is_schema gating already takes zero lock for the JSON equivalents
 * (find/count/aggregate are in neither list). That meant an NQL read could
 * block behind another connection's held schema wrlock on the same object
 * while the JSON-wire-protocol version of the identical read would not.
 *
 * SCHEMA_WRLOCK_TEST_DELAY_MS (test-only, 0/off in production) widens
 * dispatch_json_query's wrlock-held window deterministically: a `vacuum`
 * request sleeps for the configured duration immediately after acquiring
 * the object's wrlock, before doing any real work. A synchronous marker file
 * exists only while that delay is active. After observing the marker, the
 * test fires a concurrent NQL `find` on the same object and requires the
 * marker to still exist when the NQL response arrives.
 *
 *   - Pre-fix: dispatch_nql_query's own objlock_rdlock blocks until the
 *     held wrlock releases. The delay marker has necessarily been removed
 *     before the NQL response can arrive.
 *   - Post-fix: dispatch_nql_query takes no lock at all -- the NQL find
 *     returns well under DELAY_MS/2 while the marker is still present.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#define DELAY_MS 2000

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* The server creates this file synchronously after acquiring the wrlock and
   removes it when the injected delay ends. Checking its contents avoids
   accepting an unrelated or stale marker. */
static int wait_for_wrlock_delay_start(const char *marker_path, int timeout_s) {
    for (int i = 0; i < timeout_s * 10; i++) {
        FILE *f = fopen(marker_path, "r");
        if (f) {
            char line[256] = {0};
            int matched = fgets(line, sizeof line, f) &&
                          strstr(line, "mode=vacuum") &&
                          strstr(line, "object=lockrace");
            fclose(f);
            if (matched) return 1;
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
        "DB_ROOT=%s\nPORT=%d\nTIMEOUT=0\nTHREADS=4\nFCACHE_MAX=4096\nTLS_ENABLE=0\n",
        db_root, port);
    if (with_delay) fprintf(ef, "SCHEMA_WRLOCK_TEST_DELAY_MS=%d\n", DELAY_MS);
    fclose(ef);
    return 0;
}

static pid_t spawn_daemon(const char *base, const char *shard_db_abs) {
    pid_t pid = fork();
    if (pid == 0) {
        if (chdir(base) != 0) _exit(126);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }
    return pid;
}

/* Never signal a non-positive pid: kill(-1, ...) targets every process the
   test user may signal. Always reap the child, escalating only after the
   graceful-shutdown window expires. */
static void stop_daemon(pid_t pid) {
    if (pid <= 0) return;
    (void)kill(pid, SIGTERM);
    for (int i = 0; i < 100; i++) {
        int status = 0;
        pid_t w = waitpid(pid, &status, WNOHANG);
        if (w == pid || (w < 0 && errno == ECHILD)) return;
        usleep(100000);
    }
    (void)kill(pid, SIGKILL);
    int status = 0;
    while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
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

static int test_nql_no_objlock_contention_run(void) {
    char base[] = "/tmp/shard-db-nql-lock-race-XXXXXX";
    if (!mkdtemp(base)) { ASSERT_TRUE(0, "mkdtemp"); return 1; }

    int port = test_pick_port();
    char db_root[PATH_MAX];
    snprintf(db_root, sizeof(db_root), "%s/root", base);
    mkdir(db_root, 0755);

    char env_path[PATH_MAX];
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    ASSERT_TRUE(write_env(env_path, db_root, port, 0) == 0, "write initial db.env");

    char marker_path[PATH_MAX];
    snprintf(marker_path, sizeof marker_path,
             "%s/default/.schema-wrlock-test-delay-lockrace.active", db_root);

    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db not found");
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    /* Round 1: plain daemon, seed the object. */
    pid_t pid = spawn_daemon(base, shard_db_abs);
    ASSERT_TRUE(pid > 0, "fork daemon (round 1)");
    if (pid <= 0) {
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }
    int ready = wait_ready(port, 100);
    ASSERT_TRUE(ready, "daemon ready (round 1)");
    if (!ready) {
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect (round 1)");
    if (!tc) {
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"lockrace\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\",\"age:int\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"lockrace\","
                   "\"key\":\"k1\",\"value\":{\"name\":\"alice\",\"age\":30}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc);

    stop_daemon(pid);
    pid = -1;

    /* Round 2: restart with the delay knob armed. */
    ASSERT_TRUE(write_env(env_path, db_root, port, 1) == 0, "write db.env with SCHEMA_WRLOCK_TEST_DELAY_MS");
    pid = spawn_daemon(base, shard_db_abs);
    ASSERT_TRUE(pid > 0, "fork daemon (round 2)");
    if (pid <= 0) {
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }
    ready = wait_ready(port, 100);
    ASSERT_TRUE(ready, "daemon ready (round 2)");
    if (!ready) {
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    TestClient *vc = tc_connect(&cfg);
    ASSERT_NOT_NULL(vc, "connect for vacuum");
    if (!vc) {
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    /* Fire the wrlock-holding vacuum asynchronously: send the request and
       poll the synchronous active-marker rather than waiting for the
       response (which won't arrive until after DELAY_MS has elapsed). */
    int sent = tc_send(vc,
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"lockrace\"}") == 0;
    ASSERT_TRUE(sent, "vacuum request sent");
    if (!sent) {
        tc_close(vc);
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    int marker_seen = wait_for_wrlock_delay_start(marker_path, 10);
    ASSERT_TRUE(marker_seen,
        "synchronous schema-wrlock delay marker observed");
    if (!marker_seen) {
        tc_close(vc);
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    /* While the wrlock is held (vacuum is sleeping inside it), fire a
       concurrent NQL find on the SAME object over a second connection. */
    TestClient *nc = tc_connect(&cfg);
    ASSERT_NOT_NULL(nc, "connect for NQL find");
    if (nc) {
        long t0 = now_ms();
        char *nresp = NULL;
        int nql_rc = tc_request(nc, "find default lockrace", &nresp);
        long elapsed = now_ms() - t0;
        int marker_still_active = access(marker_path, F_OK) == 0;
        tc_close(nc);

        ASSERT_TRUE(nql_rc == 0, "NQL find round-trip succeeds");
        ASSERT_TRUE(nresp != NULL && !SAFE_STRSTR(nresp, "\"error\""),
            "NQL find succeeds while a schema wrlock is held on the same object");
        if (nresp) TAP_DIAG("# NQL find response: %s\n", nresp);
        free(nresp);

        ASSERT_TRUE(marker_still_active,
            "NQL response arrives before the held-wrlock delay ends");
        ASSERT_TRUE(elapsed < (DELAY_MS / 2),
            "NQL find does not block behind the held schema wrlock (no objlock taken)");
        TAP_DIAG("# NQL find elapsed: %ldms (wrlock hold=%dms)\n", elapsed, DELAY_MS);
    }

    /* Drain the vacuum response so the connection doesn't leak past the
       delay window. */
    char *vresp = NULL;
    tc_recv(vc, &vresp);
    free(vresp);
    tc_close(vc);

    stop_daemon(pid);

    char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-no-objlock-contention", test_nql_no_objlock_contention_run)
