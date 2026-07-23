/* src/test/cases/test_read_objlock_contention.c
 * Formerly "Finding 4": dispatch_nql_query's original unconditional
 * objlock_rdlock was removed on the theory that every NqlMode is a read and
 * dispatch_json_query's own mode_is_write/mode_is_schema gating already took
 * zero lock for the JSON equivalents (find/count/aggregate were in neither
 * list) -- so NQL should match. That "fix" was itself the bug: unlocked
 * reads on both wire protocols could race slotcask_registry_get() against a
 * concurrent rebuild/vacuum's slotcask_registry_invalidate() (which frees
 * the SlotcaskDb struct under objlock_wrlock, with zero refcounting) --
 * a genuine use-after-free, not a documented perf trade-off. See
 * docs/plans/2026-07-21-read-path-missing-objlock-uaf.md. Both
 * dispatch_json_query and dispatch_nql_query now take objlock_rdlock for
 * every read mode; this file was renamed twice (from
 * test_nql_no_objlock_contention.c through test_nql_objlock_contention.c to
 * here) and its assertions inverted, because the original name asserted the
 * exact opposite of correct behavior and covered only half the bug: it now
 * proves that BOTH a JSON `get` and an NQL `find`, fired concurrently,
 * correctly BLOCK behind a held schema wrlock (mutual exclusion, matching
 * the pre-"Finding 4" behavior this file used to guard against), rather
 * than proving either one doesn't.
 *
 * SCHEMA_WRLOCK_TEST_DELAY_MS (test-only, 0/off in production) widens
 * dispatch_json_query's wrlock-held window deterministically: a `vacuum`
 * request sleeps for the configured duration immediately after acquiring
 * the object's wrlock, before doing any real work. A synchronous marker file
 * exists only while that delay is active. After observing the marker, the
 * test sends a JSON `get` and an NQL `find` on the same object concurrently
 * (both requests in flight before either response is awaited, so both race
 * the held wrlock on separate worker threads) and requires both responses
 * to arrive only AFTER the marker (and thus the wrlock) is gone -- proving
 * both dispatchers' objlock_rdlock blocked until the writer released it,
 * instead of racing the concurrent free.
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

static int test_read_objlock_contention_run(void) {
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
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
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
       concurrent NQL find AND a concurrent JSON get on the SAME object,
       each over its own connection. Both requests are sent (tc_send) back
       to back, before either response is awaited, so both are genuinely
       in flight against the held wrlock at the same time -- sequential
       tc_request calls would let the first response drain the delay
       window before the second request is even sent, which would prove
       nothing about concurrent blocking. objlock_rdlock inside both
       dispatch_nql_query and dispatch_json_query must block until
       vacuum's objlock_wrlock releases -- proving neither read path can
       race the SlotcaskDb free that a real (non-test-delayed) rebuild
       would do at roughly this point. */
    TestClient *nc = tc_connect(&cfg);
    ASSERT_NOT_NULL(nc, "connect for NQL find");
    TestClient *gc = tc_connect(&cfg);
    ASSERT_NOT_NULL(gc, "connect for JSON get");

    long nql_t0 = 0, get_t0 = 0;
    int nql_sent = 0, get_sent = 0;
    if (nc) {
        nql_t0 = now_ms();
        nql_sent = tc_send(nc, "find default lockrace") == 0;
        ASSERT_TRUE(nql_sent, "NQL find request sent");
    }
    if (gc) {
        get_t0 = now_ms();
        get_sent = tc_send(gc,
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"lockrace\","
            "\"key\":\"k1\"}") == 0;
        ASSERT_TRUE(get_sent, "JSON get request sent");
    }

    if (nc && nql_sent) {
        char *nresp = NULL;
        int nql_rc = tc_recv(nc, &nresp);
        long elapsed = now_ms() - nql_t0;
        int marker_gone_by_response = access(marker_path, F_OK) != 0;
        tc_close(nc);

        ASSERT_TRUE(nql_rc == 0, "NQL find round-trip succeeds");
        ASSERT_TRUE(nresp != NULL && !SAFE_STRSTR(nresp, "\"error\""),
            "NQL find succeeds after the schema wrlock releases");
        if (nresp) TAP_DIAG("# NQL find response: %s\n", nresp);
        free(nresp);

        ASSERT_TRUE(marker_gone_by_response,
            "NQL response arrives only after the held-wrlock delay ends");
        ASSERT_TRUE(elapsed >= (DELAY_MS / 2),
            "NQL find blocks behind the held schema wrlock (objlock_rdlock taken)");
        TAP_DIAG("# NQL find elapsed: %ldms (wrlock hold=%dms)\n", elapsed, DELAY_MS);
    } else if (nc) {
        tc_close(nc);
    }

    if (gc && get_sent) {
        char *gresp = NULL;
        int get_rc = tc_recv(gc, &gresp);
        long elapsed = now_ms() - get_t0;
        int marker_gone_by_response = access(marker_path, F_OK) != 0;
        tc_close(gc);

        ASSERT_TRUE(get_rc == 0, "JSON get round-trip succeeds");
        ASSERT_TRUE(gresp != NULL && !SAFE_STRSTR(gresp, "\"error\""),
            "JSON get succeeds after the schema wrlock releases");
        ASSERT_TRUE(gresp != NULL && SAFE_STRSTR(gresp, "\"alice\"") &&
            SAFE_STRSTR(gresp, "30"),
            "JSON get returns the seeded row (name=alice, age=30)");
        if (gresp) TAP_DIAG("# JSON get response: %s\n", gresp);
        free(gresp);

        ASSERT_TRUE(marker_gone_by_response,
            "JSON get response arrives only after the held-wrlock delay ends");
        ASSERT_TRUE(elapsed >= (DELAY_MS / 2),
            "JSON get blocks behind the held schema wrlock (objlock_rdlock taken)");
        TAP_DIAG("# JSON get elapsed: %ldms (wrlock hold=%dms)\n", elapsed, DELAY_MS);
    } else if (gc) {
        tc_close(gc);
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

TEST_REGISTER("test-read-objlock-contention", test_read_objlock_contention_run)
