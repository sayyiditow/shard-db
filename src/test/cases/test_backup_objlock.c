/* src/test/cases/test_backup_objlock.c
 * Fenced backup: `backup` is a plain recursive logical copy of data/,
 * indexes/, metadata/ and files/ (plus fields.conf + tokens.conf + an
 * object.json manifest), so a
 * backup that runs while anything else is in flight on the object could
 * capture a half-finished commit window, a mid-resplit keyfile, a
 * `.bmfix-NNN` bitmap-repair marker or a half-written
 * metadata/sequences/<name> file. The fix adds `backup` to mode_is_schema,
 * so both dispatchers take the object's objlock_wrlock for it exactly like
 * vacuum/truncate: every other operation on that object -- reads included
 * -- blocks until the copy finishes, and nothing can be in flight inside
 * the copy.
 *
 * SCHEMA_WRLOCK_TEST_DELAY_MS (test-only, 0/off in production) widens
 * dispatch_json_query's wrlock-held window deterministically: a `backup`
 * request sleeps for the configured duration immediately after acquiring
 * the object's wrlock, before doing any real work. A synchronous marker file
 * exists only while that delay is active. After observing the marker, the
 * test sends a JSON `get` and a JSON `insert` on the same object concurrently
 * (both requests in flight before either response is awaited, so both race
 * the held wrlock on separate worker threads) and requires both responses
 * to arrive only AFTER the marker (and thus the wrlock) is gone -- proving
 * both blocked behind the backup's exclusive lock -- and then verifies on a
 * fresh connection that the raced insert actually committed after the lock
 * released.
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
                          strstr(line, "mode=backup") &&
                          strstr(line, "object=bkplock");
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

static int test_backup_fences_writers(void) {
    char base[] = "/tmp/shard-db-backup-fence-XXXXXX";
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
             "%s/default/.schema-wrlock-test-delay-bkplock.active", db_root);

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
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bkplock\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\",\"age:int\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bkplock\","
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
    ASSERT_NOT_NULL(vc, "connect for backup");
    if (!vc) {
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    /* Fire the wrlock-holding backup asynchronously: send the request and
       poll the synchronous active-marker rather than waiting for the
       response (which won't arrive until after DELAY_MS has elapsed). */
    int sent = tc_send(vc,
        "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"bkplock\"}") == 0;
    ASSERT_TRUE(sent, "backup request sent");
    if (!sent) {
        tc_close(vc);
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    int marker_seen = wait_for_wrlock_delay_start(marker_path, 10);
    ASSERT_TRUE(marker_seen,
        "synchronous backup wrlock delay marker observed");
    if (!marker_seen) {
        tc_close(vc);
        stop_daemon(pid);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    /* While the wrlock is held (backup is sleeping inside it), fire a
       concurrent JSON insert AND a concurrent JSON get on the SAME object,
       each over its own connection. Both requests are sent (tc_send) back
       to back, before either response is awaited, so both are genuinely
       in flight against the held wrlock at the same time -- sequential
       tc_request calls would let the first response drain the delay
       window before the second request is even sent, which would prove
       nothing about concurrent blocking. objlock_rdlock inside
       dispatch_json_query must block until backup's objlock_wrlock
       releases -- proving neither read nor write path can observe the
       object mid-copy, which is exactly what the exclusive lock fences. */
    TestClient *ic = tc_connect(&cfg);
    ASSERT_NOT_NULL(ic, "connect for JSON insert");
    TestClient *gc = tc_connect(&cfg);
    ASSERT_NOT_NULL(gc, "connect for JSON get");

    long ins_t0 = 0, get_t0 = 0;
    int ins_sent = 0, get_sent = 0;
    if (ic) {
        ins_t0 = now_ms();
        ins_sent = tc_send(ic,
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bkplock\","
            "\"key\":\"k2\",\"value\":{\"name\":\"bob\",\"age\":41}}") == 0;
        ASSERT_TRUE(ins_sent, "JSON insert request sent");
    }
    if (gc) {
        get_t0 = now_ms();
        get_sent = tc_send(gc,
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"bkplock\","
            "\"key\":\"k1\"}") == 0;
        ASSERT_TRUE(get_sent, "JSON get request sent");
    }

    if (ic && ins_sent) {
        char *iresp = NULL;
        int ins_rc = tc_recv(ic, &iresp);
        long elapsed = now_ms() - ins_t0;
        int marker_gone_by_response = access(marker_path, F_OK) != 0;
        tc_close(ic);

        ASSERT_TRUE(ins_rc == 0, "JSON insert round-trip succeeds");
        ASSERT_TRUE(iresp != NULL && !SAFE_STRSTR(iresp, "\"error\""),
            "JSON insert succeeds after the backup's wrlock releases");
        if (iresp) TAP_DIAG("# JSON insert response: %s\n", iresp);
        free(iresp);

        ASSERT_TRUE(marker_gone_by_response,
            "JSON insert response arrives only after the held-wrlock delay ends");
        ASSERT_TRUE(elapsed >= (DELAY_MS / 2),
            "JSON insert blocks behind the backup's exclusive lock");
        TAP_DIAG("# JSON insert elapsed: %ldms (wrlock hold=%dms)\n", elapsed, DELAY_MS);
    } else if (ic) {
        tc_close(ic);
    }

    if (gc && get_sent) {
        char *gresp = NULL;
        int get_rc = tc_recv(gc, &gresp);
        long elapsed = now_ms() - get_t0;
        int marker_gone_by_response = access(marker_path, F_OK) != 0;
        tc_close(gc);

        ASSERT_TRUE(get_rc == 0, "JSON get round-trip succeeds");
        ASSERT_TRUE(gresp != NULL && !SAFE_STRSTR(gresp, "\"error\""),
            "JSON get succeeds after the backup's wrlock releases");
        ASSERT_TRUE(gresp != NULL && SAFE_STRSTR(gresp, "\"alice\"") &&
            SAFE_STRSTR(gresp, "30"),
            "JSON get returns the seeded row (name=alice, age=30)");
        if (gresp) TAP_DIAG("# JSON get response: %s\n", gresp);
        free(gresp);

        ASSERT_TRUE(marker_gone_by_response,
            "JSON get response arrives only after the held-wrlock delay ends");
        ASSERT_TRUE(elapsed >= (DELAY_MS / 2),
            "JSON get blocks behind the held backup wrlock (objlock_rdlock taken)");
        TAP_DIAG("# JSON get elapsed: %ldms (wrlock hold=%dms)\n", elapsed, DELAY_MS);
    } else if (gc) {
        tc_close(gc);
    }

    /* Drain the backup response so the connection doesn't leak past the
       delay window. */
    char *vresp = NULL;
    tc_recv(vc, &vresp);
    ASSERT_TRUE(vresp != NULL && SAFE_STRSTR(vresp, "\"status\":\"backed_up\""),
        "backup completes");
    if (vresp) TAP_DIAG("# backup response: %s\n", vresp);
    free(vresp);
    tc_close(vc);

    /* The insert that raced the held wrlock must have committed once the
       lock released: a fresh connection reads k2. */
    TestClient *rc = tc_connect(&cfg);
    ASSERT_NOT_NULL(rc, "connect for post-backup get of k2");
    if (rc) {
        char *r2 = NULL;
        tc_request(rc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"bkplock\","
                       "\"key\":\"k2\"}", &r2);
        ASSERT_TRUE(r2 != NULL && SAFE_STRSTR(r2, "\"bob\""),
            "insert committed after the lock released");
        if (r2) TAP_DIAG("# post-backup get k2 response: %s\n", r2);
        free(r2);
        tc_close(rc);
    }

    stop_daemon(pid);

    char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-backup-fences-writers", test_backup_fences_writers)
