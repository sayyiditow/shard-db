/* src/test/cases/test_auto_reshard_shutdown_race.c
 *
 * Regression test for a use-after-free race: kfcache_shutdown() frees the
 * entire kfcache array and destroys every entry's rwlock while holding
 * only g_kfcache_lock. kfcache_invalidate_prefix() (reachable from an
 * in-flight auto-reshard via rebuild_object_v2 -> slotcask_registry_invalidate)
 * never takes that lock by design (see its comment in slotcask.c), so
 * before the fix, a reshard still running when shutdown reached
 * kfcache_shutdown() raced it with zero mutual exclusion.
 *
 * Uses KFCACHE_TEST_HOLD_MS (test-only, default 0/off in production) to
 * deterministically widen kfcache_invalidate_prefix()'s hold window, then
 * sends SIGTERM the instant the reshard's "starting" log line appears.
 * Two independent assertions distinguish joined (fixed) from detached
 * (buggy) shutdown:
 *   1. The daemon must not die by signal (the crash symptom itself).
 *   2. Shutdown must take at least ~HOLD_MS: pre-fix, cmd_server proceeds
 *      to kfcache_shutdown() immediately after SIGTERM regardless of the
 *      in-flight thread, so elapsed time is near-instant; post-fix,
 *      cmd_server blocks on pthread_join() until the held reshard call
 *      returns. This timing assertion is the primary, robust proof: it
 *      fails deterministically pre-fix and passes deterministically
 *      post-fix even on a run where the crash doesn't happen to manifest.
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

#define HOLD_MS 2000

static int fabricate_kf_total(const char *kf_path, uint64_t total) {
    int fd = open(kf_path, O_WRONLY);
    if (fd < 0) return -1;
    ssize_t w = pwrite(fd, &total, sizeof(total), 8);
    close(fd);
    return (w == (ssize_t)sizeof(total)) ? 0 : -1;
}

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* Poll the INFO log for the "AUTO-RESHARD ... starting" line.
   Note: LOG_WARN is level 2 which open_log_for_level() in config.c routes
   to the -info.log file (only level 1 / ERROR gets its own -error.log).
   The "starting" line goes to -info.log alongside INFO-level messages. */
static int wait_for_reshard_start(const char *db_root, const char *date_str,
                                    int timeout_s) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/logs/%s-info.log", db_root, date_str);
    for (int i = 0; i < timeout_s * 10; i++) {
        FILE *f = fopen(path, "r");
        if (f) {
            char line[1024];
            while (fgets(line, sizeof(line), f)) {
                if (strstr(line, "AUTO-RESHARD") && strstr(line, "starting")) {
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

static int test_auto_reshard_shutdown_race_run(void) {
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    int secs_left_in_hour = (59 - tmv.tm_min) * 60 + (60 - tmv.tm_sec);
    if (secs_left_in_hour < 90) {
        sleep(secs_left_in_hour + 5);
        now = time(NULL);
        localtime_r(&now, &tmv);
    }
    int target_hour = tmv.tm_hour;
    char date_str[16];
    strftime(date_str, sizeof(date_str), "%Y-%m-%d", &tmv);

    char base[] = "/tmp/shard-db-reshard-shutdown-XXXXXX";
    if (!mkdtemp(base)) { ASSERT_TRUE(0, "mkdtemp"); return 1; }

    int port = test_pick_port();
    char db_root[PATH_MAX];
    snprintf(db_root, sizeof(db_root), "%s/root", base);
    mkdir(db_root, 0755);

    /* Daemon reads ./db.env relative to its cwd (load_db_root(), main.c) --
       it does NOT take db_root as a CLI arg for `server`. Write db.env in
       `base` and chdir() the child into `base` before execl, matching
       test_auto_reshard.c's working pattern exactly. DB_ROOT inside
       db.env points at the separate `root` subdirectory. */
    char env_path[PATH_MAX];
    snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    FILE *ef = fopen(env_path, "w");
    ASSERT_NOT_NULL(ef, "open db.env for write");
    if (!ef) { char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c); return 1; }
    fprintf(ef,
        "DB_ROOT=%s\nPORT=%d\nTIMEOUT=0\nTHREADS=2\nFCACHE_MAX=4096\nTLS_ENABLE=0\n"
        "AUTO_RESHARD_ENABLE=1\nAUTO_RESHARD_HOUR=%d\n"
        "KFCACHE_TEST_HOLD_MS=%d\n",
        db_root, port, target_hour, HOLD_MS);
    fclose(ef);

    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db not found");
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    pid_t pid = fork();
    if (pid < 0) { ASSERT_TRUE(0, "fork"); char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c); return 1; }
    if (pid == 0) {
        chdir(base);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }

    /* Wait until ready (same poll pattern as test_auto_reshard.c). */
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
    ASSERT_TRUE(ready, "daemon ready with AUTO_RESHARD_ENABLE=1");
    if (!ready) {
        kill(pid, SIGKILL);
        int st; waitpid(pid, &st, 0);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) {
        kill(pid, SIGKILL);
        int st; waitpid(pid, &st, 0);
        char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
        return 1;
    }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"widgets\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"]}", &resp);
    free(resp); resp = NULL;

    /* Real insert: seeds a genuine "used" kfcache entry for this object's
       data_dir prefix. fabricate_kf_total() below writes straight to disk
       and never touches the in-memory kfcache -- without this insert, the
       invalidate loop in kfcache_invalidate_prefix() finds no matching
       entry and the injected hold never fires. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"widgets\","
                    "\"key\":\"w1\",\"value\":{\"name\":\"gear\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc);

    /* Fabricate a live count that forces reshard_target_for_count() to
       recommend growth, so auto_reshard_sweep_one acts on this object. */
    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path), "%s/default/widgets/data/kf/000.kf", db_root);
    ASSERT_TRUE(fabricate_kf_total(kf_path, 5000000ULL) == 0, "fabricate kf total");

    ASSERT_TRUE(wait_for_reshard_start(db_root, date_str, 90),
        "AUTO-RESHARD starting line observed in -info.log");

    long t0 = now_ms();
    kill(pid, SIGTERM);

    int status = 0;
    pid_t r = 0;
    for (int i = 0; i < 100; i++) { /* up to 10s */
        r = waitpid(pid, &status, WNOHANG);
        if (r == pid) break;
        usleep(100000);
    }
    if (r != pid) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
    }
    long elapsed = now_ms() - t0;

    ASSERT_TRUE(!WIFSIGNALED(status),
        "daemon does not die by signal when SIGTERM lands mid-reshard");
    if (WIFSIGNALED(status))
        TAP_DIAG("# daemon killed by signal %d\n", WTERMSIG(status));

    ASSERT_TRUE(elapsed >= (HOLD_MS - 300),
        "shutdown waits for the in-flight reshard (proves join, not detach)");
    TAP_DIAG("# elapsed shutdown time: %ldms (hold=%dms)\n", elapsed, HOLD_MS);

    char c[600]; snprintf(c, sizeof(c), "rm -rf %s", base); system(c);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-reshard-shutdown-race", test_auto_reshard_shutdown_race_run)
