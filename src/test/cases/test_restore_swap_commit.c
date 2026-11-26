/* src/test/cases/test_restore_swap_commit.c
 * Restore commit point, rollback and refusal (docs/plans/2026-11-26-fix-
 * restore-swap-and-kf-dirsync.md, T2):
 *
 *  - the commit is the durable SWAP removal, so a crash during the stage
 *    cleanup keeps the restored data (B1);
 *  - a failed commit directory sync rolls back instead of leaving the
 *    restored tree live with a SWAP marker (M1);
 *  - a rollback that cannot complete aborts the daemon and startup finishs
 *    it, and startup refuses a stage it cannot roll back;
 *  - an absent live subtree (files/) rolls back to an empty placeholder.
 *
 * Park sites come from the TEST_BUILD control channel (kind 5 = after the
 * renames, kind 6 = after the commit); the fault seams are the one-shot
 * SHARD_TEST_RF_* mask.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include "shard_test_ctl.h"
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define RS_OBJ_SPLITS 8
#define RS_OBJ_MAX_KEY 16

/* One request/response exchange built from a printf format. */
static char *rs_req(TestClient *tc, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
static char *rs_req(TestClient *tc, const char *fmt, ...) {
    char buf[2048];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    char *resp = NULL;
    if (tc_request(tc, buf, &resp) != 0) return NULL;
    return resp;
}

/* "<key>":"<value>" out of a JSON response; stops at the first '"'. */
static int rs_extract(const char *resp, const char *key, char *out, size_t sz) {
    if (!resp) return 0;
    char needle[64];
    snprintf(needle, sizeof(needle), "\"%s\":\"", key);
    const char *p = strstr(resp, needle);
    if (!p) return 0;
    p += strlen(needle);
    const char *q = strchr(p, '"');
    if (!q) return 0;
    size_t n = (size_t)(q - p);
    if (n + 1 > sz) n = sz - 1;
    memcpy(out, p, n);
    out[n] = '\0';
    return 1;
}

static int rs_backup(TestClient *tc, const char *obj, char *from, size_t sz) {
    char *r = rs_req(tc, "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"%s\"}", obj);
    int ok = rs_extract(r, "path", from, sz);
    free(r);
    if (!ok) return 0;
    char *sl = strrchr(from, '/');
    if (sl) memmove(from, sl + 1, strlen(sl + 1) + 1);
    return from[0] != '\0';
}

/* Count of `.restore-*` entries in obj_dir; the first one's full path in out. */
static int rs_find_stage(const char *obj_dir, char *out, size_t sz) {
    DIR *d = opendir(obj_dir);
    if (!d) return 0;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (strncmp(e->d_name, ".restore-", 9) != 0) continue;
        if (n == 0 && out && sz) snprintf(out, sz, "%s/%s", obj_dir, e->d_name);
        n++;
    }
    closedir(d);
    return n;
}

/* Poll waitpid(WNOHANG) every 50 ms. 1 when the daemon exited (status in
   *status, pid/fd reset), 0 when it is still running at the deadline. */
static int rs_wait_exit(TestEnv *env, int ms, int *status) {
    int waited = 0;
    if (status) *status = 0;
    while (waited < ms) {
        int st = 0;
        pid_t p = waitpid(env->daemon_pid, &st, WNOHANG);
        if (p == env->daemon_pid) {
            if (status) *status = st;
            env->daemon_pid = -1;
            if (env->test_control_fd >= 0) {
                close(env->test_control_fd);
                env->test_control_fd = -1;
            }
            return 1;
        }
        usleep(50 * 1000);
        waited += 50;
    }
    return 0;
}

static void rs_restore_req(char *buf, size_t sz, const char *obj, const char *from) {
    snprintf(buf, sz,
             "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"%s\","
             "\"from\":\"%s\",\"force\":true}", obj, from);
}

/* create-object, insert k1=alpha, backup, update k1=beta, insert k2=gamma. */
static int rs_setup(TestClient *tc, const char *obj, char *from, size_t fromsz) {
    char *r = rs_req(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    free(r);
    r = rs_req(tc,
               "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
               "\"splits\":%d,\"max_key\":%d,"
               "\"fields\":[\"name:varchar:16\"],\"indexes\":[\"name\"]}",
               obj, RS_OBJ_SPLITS, RS_OBJ_MAX_KEY);
    int created = r && strstr(r, "\"status\"") != NULL;
    free(r);
    if (!created) return 0;
    r = rs_req(tc,
               "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
               "\"key\":\"k1\",\"value\":{\"name\":\"alpha\"}}", obj);
    free(r);
    if (!rs_backup(tc, obj, from, fromsz)) return 0;
    r = rs_req(tc,
               "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"%s\","
               "\"key\":\"k1\",\"value\":{\"name\":\"beta\"}}", obj);
    free(r);
    r = rs_req(tc,
               "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
               "\"key\":\"k2\",\"value\":{\"name\":\"gamma\"}}", obj);
    free(r);
    return 1;
}

static void rs_check_prerestore(TestClient *tc, const char *obj) {
    char *r = rs_req(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"k1\"}", obj);
    ASSERT_CONTAINS(r, "\"name\":\"beta\"", "pre-restore: k1 is beta");
    free(r);
    r = rs_req(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"k2\"}", obj);
    ASSERT_CONTAINS(r, "true", "pre-restore: k2 exists");
    free(r);
    r = rs_req(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\"}", obj);
    ASSERT_EQ_INT(tu_parse_count(r), 2, "pre-restore: count is 2");
    free(r);
    r = rs_req(tc,
               "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
               "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"beta\"}]}", obj);
    ASSERT_EQ_INT(tu_parse_count(r), 1, "pre-restore: one record indexed as beta");
    free(r);
}

static void rs_check_restored(TestClient *tc, const char *obj) {
    char *r = rs_req(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"k1\"}", obj);
    ASSERT_CONTAINS(r, "\"name\":\"alpha\"", "restored: k1 is alpha");
    free(r);
    r = rs_req(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"k2\"}", obj);
    ASSERT_CONTAINS(r, "false", "restored: k2 is gone");
    free(r);
    r = rs_req(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\"}", obj);
    ASSERT_EQ_INT(tu_parse_count(r), 1, "restored: count is 1");
    free(r);
}

/* B1: the commit point is the durable SWAP removal, so a crash while the
   stage is being deleted keeps the restored data. */
static int test_restore_commit_before_cleanup(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rs_commit";
    char from[128] = "", obj_dir[PATH_MAX], stage[PATH_MAX] = "";
    if (!rs_setup(tc, obj, from, sizeof(from))) {
        ASSERT_TRUE(0, "rs_setup");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    snprintf(obj_dir, sizeof(obj_dir), "%s/default/%s", env.db_root, obj);

    ASSERT_EQ_INT(test_env_test_hook_install_kind(&env, 6), 0,
                  "install the committed park");
    char req[512];
    rs_restore_req(req, sizeof(req), obj, from);
    ASSERT_EQ_INT(tc_send(tc, req), 0, "send the restore");
    int phase = -1;
    ASSERT_EQ_INT(test_env_test_hook_wait(&env, &phase), 0, "park reached");
    ASSERT_EQ_INT(phase, 6, "parked after the commit");
    ASSERT_EQ_INT(rs_find_stage(obj_dir, stage, sizeof(stage)), 1, "one stage exists");
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/SWAP", stage);
    ASSERT_TRUE(access(p, F_OK) != 0, "the commit removed SWAP");
    snprintf(p, sizeof(p), "%s/old/data", stage);
    ASSERT_TRUE(access(p, F_OK) == 0, "the superseded original is parked in old/");

    /* Simulate a kill in the middle of the stage delete. */
    char cmd[PATH_MAX + 32];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/old/data/kf", stage);
    if (system(cmd) != 0) { /* best effort */ }
    test_env_kill(&env);
    tc_close(tc);

    if (test_env_start_at(&env, env.db_root, env.port) != 0) {
        ASSERT_TRUE(0, "restart after the cleanup crash");
        test_env_stop(&env); return 1;
    }
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect");
    if (!tc) { test_env_stop(&env); return 1; }
    rs_check_restored(tc, obj);
    ASSERT_EQ_INT(rs_find_stage(obj_dir, NULL, 0), 0, "no stage survives the restart");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* A crash after the renames but before the commit rolls back at restart. */
static int test_restore_crash_after_renames_rolls_back(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rs_crash";
    char from[128] = "", obj_dir[PATH_MAX], stage[PATH_MAX] = "";
    if (!rs_setup(tc, obj, from, sizeof(from))) {
        ASSERT_TRUE(0, "rs_setup");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    snprintf(obj_dir, sizeof(obj_dir), "%s/default/%s", env.db_root, obj);

    ASSERT_EQ_INT(test_env_test_hook_install_kind(&env, 5), 0,
                  "install the swapped park");
    char req[512];
    rs_restore_req(req, sizeof(req), obj, from);
    ASSERT_EQ_INT(tc_send(tc, req), 0, "send the restore");
    int phase = -1;
    ASSERT_EQ_INT(test_env_test_hook_wait(&env, &phase), 0, "park reached");
    ASSERT_EQ_INT(phase, 5, "parked after the renames");
    ASSERT_EQ_INT(rs_find_stage(obj_dir, stage, sizeof(stage)), 1, "one stage exists");
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/SWAP", stage);
    ASSERT_TRUE(access(p, F_OK) == 0, "SWAP is still there before the commit");
    test_env_kill(&env);
    tc_close(tc);

    if (test_env_start_at(&env, env.db_root, env.port) != 0) {
        ASSERT_TRUE(0, "restart after the crash");
        test_env_stop(&env); return 1;
    }
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect");
    if (!tc) { test_env_stop(&env); return 1; }
    rs_check_prerestore(tc, obj);
    ASSERT_EQ_INT(rs_find_stage(obj_dir, NULL, 0), 0, "the stage is gone");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* M1: a failed commit directory sync rolls back and says so. Then crafted
   leftovers a crash during rolled-back-stage cleanup can leave are removed
   at the next start. */
static int test_restore_commit_dirsync_failure_rolls_back(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rs_dirsync";
    char from[128] = "", obj_dir[PATH_MAX];
    if (!rs_setup(tc, obj, from, sizeof(from))) {
        ASSERT_TRUE(0, "rs_setup");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    snprintf(obj_dir, sizeof(obj_dir), "%s/default/%s", env.db_root, obj);

    ASSERT_EQ_INT(test_env_ctl_set_restore_fault(&env, SHARD_TEST_RF_COMMIT_DIRSYNC, 0), 0,
                  "arm the commit dirsync fault");
    char req[512], *resp = NULL;
    rs_restore_req(req, sizeof(req), obj, from);
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "restore exchange");
    ASSERT_CONTAINS(resp, "restore failed at directory sync", "the failure names the sync");
    ASSERT_CONTAINS(resp, "rolled back", "the reply says the restore rolled back");
    free(resp); resp = NULL;
    rs_check_prerestore(tc, obj);
    ASSERT_EQ_INT(rs_find_stage(obj_dir, NULL, 0), 0, "the rolled-back stage is gone");

    /* A clean retry restores. */
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "clean restore exchange");
    ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "the clean restore succeeds");
    free(resp); resp = NULL;
    rs_check_restored(tc, obj);

    /* Leftovers of a crash during the rolled-back stage cleanup: SWAP with
       no old/, and SWAP with an empty old/. */
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/.restore-crafted1", obj_dir); mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-crafted1/SWAP", obj_dir);
    FILE *f = fopen(p, "w");
    if (f) { fputs("x", f); fclose(f); }
    snprintf(p, sizeof(p), "%s/.restore-crafted2", obj_dir); mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-crafted2/old", obj_dir); mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-crafted2/SWAP", obj_dir);
    f = fopen(p, "w");
    if (f) { fputs("x", f); fclose(f); }

    tc_close(tc);
    test_env_stop_keep(&env);
    if (test_env_start_at(&env, env.db_root, env.port) != 0) {
        ASSERT_TRUE(0, "the daemon starts with the crafted leftovers");
        test_env_stop(&env); return 1;
    }
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect");
    if (!tc) { test_env_stop(&env); return 1; }
    rs_check_restored(tc, obj);
    ASSERT_EQ_INT(rs_find_stage(obj_dir, NULL, 0), 0, "the crafted stages are gone");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* The SWAP removal is not known durable: SWAP is re-created durably, then the
   restore rolls back. */
static int test_restore_unswap_sync_failure_rolls_back(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rs_unswap";
    char from[128] = "", obj_dir[PATH_MAX];
    if (!rs_setup(tc, obj, from, sizeof(from))) {
        ASSERT_TRUE(0, "rs_setup");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    snprintf(obj_dir, sizeof(obj_dir), "%s/default/%s", env.db_root, obj);

    ASSERT_EQ_INT(test_env_ctl_set_restore_fault(&env, SHARD_TEST_RF_UNSWAP_DIRSYNC, 0), 0,
                  "arm the swap-marker sync fault");
    char req[512], *resp = NULL;
    rs_restore_req(req, sizeof(req), obj, from);
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "restore exchange");
    ASSERT_CONTAINS(resp, "restore failed at swap marker sync", "the failure names the marker sync");
    ASSERT_CONTAINS(resp, "rolled back", "the reply says the restore rolled back");
    free(resp); resp = NULL;
    rs_check_prerestore(tc, obj);
    ASSERT_EQ_INT(rs_find_stage(obj_dir, NULL, 0), 0, "the rolled-back stage is gone");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* D2: an in-process rollback that cannot complete aborts the daemon; startup
   finishes the rollback. The second variant fails the SWAP re-create after
   the removal, so the abort leaves a committed (restored) object. */
static int test_restore_unrecoverable_rollback_aborts(void) {
    struct rlimit core = { 0, 0 };
    (void)setrlimit(RLIMIT_CORE, &core);   /* expected aborts write no core */
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rs_abort";
    char from[128] = "", obj_dir[PATH_MAX], req[512];
    if (!rs_setup(tc, obj, from, sizeof(from))) {
        ASSERT_TRUE(0, "rs_setup");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    snprintf(obj_dir, sizeof(obj_dir), "%s/default/%s", env.db_root, obj);
    rs_restore_req(req, sizeof(req), obj, from);

    /* Variant 1: commit dirsync fails and the rollback rename fails. */
    ASSERT_EQ_INT(test_env_ctl_set_restore_fault(
                      &env, SHARD_TEST_RF_COMMIT_DIRSYNC | SHARD_TEST_RF_ROLLBACK_RENAME, 0), 0,
                  "arm the unrecoverable rollback");
    ASSERT_EQ_INT(tc_send(tc, req), 0, "send the restore");
    int st = 0;
    int exited = rs_wait_exit(&env, 5000, &st);
    if (!exited) {
        tc_close(tc);
        test_env_stop(&env);
        ASSERT_TRUE(0, "the daemon aborts instead of leaving a half-swapped object");
        return 1;
    }
    ASSERT_TRUE(WIFSIGNALED(st) && WTERMSIG(st) == SIGABRT, "the daemon died with SIGABRT");
    tc_close(tc);

    if (test_env_start_at(&env, env.db_root, env.port) != 0) {
        ASSERT_TRUE(0, "restart finishes the interrupted rollback");
        test_env_stop(&env); return 1;
    }
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect");
    if (!tc) { test_env_stop(&env); return 1; }
    rs_check_prerestore(tc, obj);
    ASSERT_EQ_INT(rs_find_stage(obj_dir, NULL, 0), 0, "the stage is gone after recovery");

    /* Variant 2: the SWAP removal is not durable and its re-create fails. */
    ASSERT_EQ_INT(test_env_ctl_set_restore_fault(
                      &env, SHARD_TEST_RF_UNSWAP_DIRSYNC | SHARD_TEST_RF_SWAP_RECREATE, 0), 0,
                  "arm the unrecoverable marker sync");
    ASSERT_EQ_INT(tc_send(tc, req), 0, "send the second restore");
    st = 0;
    exited = rs_wait_exit(&env, 5000, &st);
    if (!exited) {
        tc_close(tc);
        test_env_stop(&env);
        ASSERT_TRUE(0, "the daemon aborts when the SWAP state is unknowable");
        return 1;
    }
    ASSERT_TRUE(WIFSIGNALED(st) && WTERMSIG(st) == SIGABRT, "the daemon died with SIGABRT");
    tc_close(tc);

    if (test_env_start_at(&env, env.db_root, env.port) != 0) {
        ASSERT_TRUE(0, "restart after the marker-sync abort");
        test_env_stop(&env); return 1;
    }
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect");
    if (!tc) { test_env_stop(&env); return 1; }
    rs_check_restored(tc, obj);
    ASSERT_EQ_INT(rs_find_stage(obj_dir, NULL, 0), 0, "the stage is gone after recovery");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* A forward-swap failure at metadata whose rollback cannot rename: abort and
   recover to the pre-restore object (indexes intact). The same swap failure
   alone rolls back in-process. */
static int test_restore_swap_failure_rollback_rename_fails(void) {
    struct rlimit core = { 0, 0 };
    (void)setrlimit(RLIMIT_CORE, &core);
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rs_swapfail";
    char from[128] = "", obj_dir[PATH_MAX], req[512];
    if (!rs_setup(tc, obj, from, sizeof(from))) {
        ASSERT_TRUE(0, "rs_setup");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    snprintf(obj_dir, sizeof(obj_dir), "%s/default/%s", env.db_root, obj);
    rs_restore_req(req, sizeof(req), obj, from);

    ASSERT_EQ_INT(test_env_ctl_set_restore_fault(
                      &env, SHARD_TEST_RF_SWAP_RENAME | SHARD_TEST_RF_ROLLBACK_RENAME, 2), 0,
                  "arm swap-of-metadata + rollback-rename faults");
    ASSERT_EQ_INT(tc_send(tc, req), 0, "send the restore");
    int st = 0;
    int exited = rs_wait_exit(&env, 5000, &st);
    if (!exited) {
        tc_close(tc);
        test_env_stop(&env);
        ASSERT_TRUE(0, "the daemon aborts instead of losing the original indexes");
        return 1;
    }
    ASSERT_TRUE(WIFSIGNALED(st) && WTERMSIG(st) == SIGABRT, "the daemon died with SIGABRT");
    tc_close(tc);

    if (test_env_start_at(&env, env.db_root, env.port) != 0) {
        ASSERT_TRUE(0, "restart finishes the interrupted rollback");
        test_env_stop(&env); return 1;
    }
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect");
    if (!tc) { test_env_stop(&env); return 1; }
    rs_check_prerestore(tc, obj);
    char idx[PATH_MAX];
    snprintf(idx, sizeof(idx), "%s/indexes", obj_dir);
    struct stat ist;
    ASSERT_TRUE(stat(idx, &ist) == 0 && S_ISDIR(ist.st_mode), "indexes is a directory again");
    char *r = rs_req(tc,
                     "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
                     "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"beta\"}]}", obj);
    ASSERT_EQ_INT(tu_parse_count(r), 1, "the indexed count is 1");
    free(r);

    /* Guard: the same forward-swap failure with a working rollback. */
    ASSERT_EQ_INT(test_env_ctl_set_restore_fault(&env, SHARD_TEST_RF_SWAP_RENAME, 2), 0,
                  "arm only the swap-of-metadata fault");
    char *resp = NULL;
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "restore exchange");
    ASSERT_CONTAINS(resp, "restore failed at swap of metadata", "the failure names metadata");
    ASSERT_CONTAINS(resp, "rolled back", "the reply says the restore rolled back");
    free(resp); resp = NULL;
    rs_check_prerestore(tc, obj);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* A live files/ that is absent at restore time rolls back to an empty
   placeholder, never to the restored copy. */
static int test_restore_absent_files_rolls_back(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rs_nofiles";
    char from[128] = "", obj_dir[PATH_MAX], stage[PATH_MAX] = "";
    char *r = rs_req(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    free(r);
    r = rs_req(tc,
               "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
               "\"splits\":%d,\"max_key\":%d,"
               "\"fields\":[\"name:varchar:16\"],\"indexes\":[\"name\"]}",
               obj, RS_OBJ_SPLITS, RS_OBJ_MAX_KEY);
    free(r);
    r = rs_req(tc,
               "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
               "\"key\":\"k1\",\"value\":{\"name\":\"alpha\"}}", obj);
    free(r);
    r = rs_req(tc,
               "{\"mode\":\"put-file\",\"dir\":\"default\",\"object\":\"%s\","
               "\"filename\":\"x.txt\",\"data\":\"YWxwaGE=\"}", obj);
    free(r);
    ASSERT_TRUE(rs_backup(tc, obj, from, sizeof(from)), "backup with files/x.txt");
    r = rs_req(tc,
               "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"%s\","
               "\"key\":\"k1\",\"value\":{\"name\":\"beta\"}}", obj);
    free(r);
    r = rs_req(tc,
               "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
               "\"key\":\"k2\",\"value\":{\"name\":\"gamma\"}}", obj);
    free(r);
    r = rs_req(tc,
               "{\"mode\":\"delete-file\",\"dir\":\"default\",\"object\":\"%s\","
               "\"filename\":\"x.txt\"}", obj);
    free(r);
    snprintf(obj_dir, sizeof(obj_dir), "%s/default/%s", env.db_root, obj);
    char files_dir[PATH_MAX];
    snprintf(files_dir, sizeof(files_dir), "%s/files", obj_dir);
    ASSERT_EQ_INT(rmdir(files_dir), 0, "rmdir the empty live files/");

    ASSERT_EQ_INT(test_env_test_hook_install_kind(&env, 5), 0, "install the swapped park");
    char req[512];
    rs_restore_req(req, sizeof(req), obj, from);
    ASSERT_EQ_INT(tc_send(tc, req), 0, "send the restore");
    int phase = -1;
    ASSERT_EQ_INT(test_env_test_hook_wait(&env, &phase), 0, "park reached");
    ASSERT_EQ_INT(phase, 5, "parked after the renames");
    ASSERT_EQ_INT(rs_find_stage(obj_dir, stage, sizeof(stage)), 1, "one stage exists");
    test_env_kill(&env);
    tc_close(tc);

    if (test_env_start_at(&env, env.db_root, env.port) != 0) {
        ASSERT_TRUE(0, "restart after the crash");
        test_env_stop(&env); return 1;
    }
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect");
    if (!tc) { test_env_stop(&env); return 1; }
    char xp[PATH_MAX];
    snprintf(xp, sizeof(xp), "%s/files/x.txt", obj_dir);
    ASSERT_TRUE(access(xp, F_OK) != 0,
                "the rolled-back files/ does not hold the restored file");
    rs_check_prerestore(tc, obj);
    ASSERT_EQ_INT(rs_find_stage(obj_dir, NULL, 0), 0, "the stage is gone");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Startup refuses a stage whose rollback cannot complete, and serves it once
   the obstacle is removed. */
static int test_restore_startup_refuses_unrecoverable_stage(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rs_refuse";
    char from[128] = "", obj_dir[PATH_MAX], stage[PATH_MAX] = "";
    if (!rs_setup(tc, obj, from, sizeof(from))) {
        ASSERT_TRUE(0, "rs_setup");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    snprintf(obj_dir, sizeof(obj_dir), "%s/default/%s", env.db_root, obj);

    ASSERT_EQ_INT(test_env_test_hook_install_kind(&env, 5), 0, "install the swapped park");
    char req[512];
    rs_restore_req(req, sizeof(req), obj, from);
    ASSERT_EQ_INT(tc_send(tc, req), 0, "send the restore");
    int phase = -1;
    ASSERT_EQ_INT(test_env_test_hook_wait(&env, &phase), 0, "park reached");
    ASSERT_EQ_INT(phase, 5, "parked after the renames");
    ASSERT_EQ_INT(rs_find_stage(obj_dir, stage, sizeof(stage)), 1, "one stage exists");
    test_env_kill(&env);
    tc_close(tc);

    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/rb", stage);
    FILE *f = fopen(p, "w");
    ASSERT_NOT_NULL(f, "create the blocking rb file");
    if (f) { fputs("not a dir", f); fclose(f); }

    ASSERT_TRUE(test_env_start_at(&env, env.db_root, env.port) != 0,
                "startup refuses an unrecoverable interrupted restore");
    test_env_kill(&env);
    snprintf(p, sizeof(p), "%s/SWAP", stage);
    ASSERT_TRUE(access(p, F_OK) == 0, "the refusing startup left SWAP in place");
    snprintf(p, sizeof(p), "%s/old/data", stage);
    ASSERT_TRUE(access(p, F_OK) == 0, "the refusing startup left old/data in place");

    snprintf(p, sizeof(p), "%s/rb", stage);
    ASSERT_EQ_INT(unlink(p), 0, "remove the blocking rb file");
    if (test_env_start_at(&env, env.db_root, env.port) != 0) {
        ASSERT_TRUE(0, "the daemon starts once the obstacle is gone");
        test_env_stop(&env); return 1;
    }
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect");
    if (!tc) { test_env_stop(&env); return 1; }
    rs_check_prerestore(tc, obj);
    ASSERT_EQ_INT(rs_find_stage(obj_dir, NULL, 0), 0, "the stage is gone");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* A stage the running daemon cannot roll back refuses the restore (I4
   exception) without touching the object; removing the stage lets the same
   restore run. */
static int test_restore_refuses_unrecoverable_leftover(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rs_manual";
    char from[128] = "", obj_dir[PATH_MAX];
    if (!rs_setup(tc, obj, from, sizeof(from))) {
        ASSERT_TRUE(0, "rs_setup");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    snprintf(obj_dir, sizeof(obj_dir), "%s/default/%s", env.db_root, obj);

    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/.restore-manual", obj_dir);             mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-manual/old", obj_dir);         mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-manual/old/metadata", obj_dir); mkdir(p, 0755);
    snprintf(p, sizeof(p), "%s/.restore-manual/old/metadata/marker.txt", obj_dir);
    FILE *f = fopen(p, "w");
    if (f) { fputs("marker", f); fclose(f); }
    snprintf(p, sizeof(p), "%s/.restore-manual/SWAP", obj_dir);
    f = fopen(p, "w");
    if (f) { fputs("x", f); fclose(f); }
    snprintf(p, sizeof(p), "%s/.restore-manual/rb", obj_dir);
    f = fopen(p, "w");
    if (f) { fputs("not a dir", f); fclose(f); }

    char req[512], *resp = NULL;
    rs_restore_req(req, sizeof(req), obj, from);
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "restore exchange");
    ASSERT_CONTAINS(resp, "an earlier interrupted restore", "the earlier stage is reported");
    free(resp); resp = NULL;
    snprintf(p, sizeof(p), "%s/.restore-manual/old/metadata/marker.txt", obj_dir);
    ASSERT_TRUE(access(p, F_OK) == 0, "the manual stage is untouched");
    rs_check_prerestore(tc, obj);

    char cmd[PATH_MAX + 32];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/.restore-manual", obj_dir);
    if (system(cmd) != 0) { /* best effort */ }
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "second restore exchange");
    ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "the restore succeeds once the stage is gone");
    free(resp); resp = NULL;
    rs_check_restored(tc, obj);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Count schema.conf lines that start with `prefix`; -1 when unreadable. */
static int rs_count_schema_lines(const char *db_root, const char *prefix) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/schema.conf", db_root);
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    char line[1024];
    int n = 0;
    size_t plen = strlen(prefix);
    while (fgets(line, sizeof(line), f))
        if (strncmp(line, prefix, plen) == 0) n++;
    fclose(f);
    return n;
}

static int rs_equal_files(const char *a, const char *b) {
    char *fa = tu_read_file(a), *fb = tu_read_file(b);
    int same = fa && fb && strcmp(fa, fb) == 0;
    free(fa); free(fb);
    return same;
}

/* M2: fields.conf is swapped with the data, so a failure before the commit
   leaves the live file byte-identical. */
static int test_restore_forced_fields_conf_not_touched_on_failure(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rsf_fields";
    char obj_dir[PATH_MAX], bak_fields[PATH_MAX], live_fields[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/default/%s", env.db_root, obj);
    snprintf(live_fields, sizeof(live_fields), "%s/fields.conf", obj_dir);

    char *r = rs_req(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    free(r);
    r = rs_req(tc,
               "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
               "\"splits\":%d,\"max_key\":%d,\"fields\":[\"name:varchar:16\"]}",
               obj, RS_OBJ_SPLITS, RS_OBJ_MAX_KEY);
    free(r);
    r = rs_req(tc,
               "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
               "\"key\":\"k1\",\"value\":{\"name\":\"alpha\"}}", obj);
    free(r);
    char from[128] = "";
    ASSERT_TRUE(rs_backup(tc, obj, from, sizeof(from)), "backup");
    snprintf(bak_fields, sizeof(bak_fields), "%s/backup/%s/fields.conf", obj_dir, from);

    r = rs_req(tc,
               "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"%s\","
               "\"fields\":[\"extra:int:default=7\"]}", obj);
    ASSERT_CONTAINS(r, "\"status\":\"rebuilt\"", "add-field rebuilt the object");
    free(r);
    char *f2 = tu_read_file(live_fields);
    ASSERT_NOT_NULL(f2, "live fields.conf is readable after add-field");
    ASSERT_TRUE(f2 && strstr(f2, "extra") != NULL, "the live fields.conf has the new field");

    char req[512], *resp = NULL;
    rs_restore_req(req, sizeof(req), obj, from);

    ASSERT_EQ_INT(test_env_ctl_set_restore_fault(&env, SHARD_TEST_RF_STAGE_COPY, 0), 0,
                  "arm the staging-copy fault");
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "restore exchange");
    ASSERT_CONTAINS(resp, "the live object was not changed", "staging failure says untouched");
    free(resp); resp = NULL;
    {
        char *now = tu_read_file(live_fields);
        ASSERT_TRUE(now && f2 && strcmp(now, f2) == 0,
                    "a staging failure leaves the live fields.conf untouched");
        free(now);
    }

    ASSERT_EQ_INT(test_env_ctl_set_restore_fault(&env, SHARD_TEST_RF_COMMIT_DIRSYNC, 0), 0,
                  "arm the commit dirsync fault");
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "second restore exchange");
    ASSERT_CONTAINS(resp, "rolled back", "commit failure rolled back");
    free(resp); resp = NULL;
    {
        char *now = tu_read_file(live_fields);
        ASSERT_TRUE(now && f2 && strcmp(now, f2) == 0,
                    "a rolled-back restore leaves the live fields.conf untouched");
        free(now);
    }
    free(f2);

    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "clean restore exchange");
    ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "the clean restore succeeds");
    free(resp); resp = NULL;
    ASSERT_TRUE(rs_equal_files(live_fields, bak_fields),
                "the restored fields.conf is the backup's");
    r = rs_req(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"k1\"}", obj);
    ASSERT_CONTAINS(r, "alpha", "the restored record is readable");
    ASSERT_TRUE(r && strstr(r, "extra") == NULL, "the dropped field is gone from reads");
    free(r);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* A live-absent fields.conf is installed after the commit; a failure before
   it leaves the file absent. */
static int test_restore_missing_fields_conf(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rsf_missing";
    char from[128] = "", obj_dir[PATH_MAX], live_fields[PATH_MAX], bak_fields[PATH_MAX];
    if (!rs_setup(tc, obj, from, sizeof(from))) {
        ASSERT_TRUE(0, "rs_setup");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    snprintf(obj_dir, sizeof(obj_dir), "%s/default/%s", env.db_root, obj);
    snprintf(live_fields, sizeof(live_fields), "%s/fields.conf", obj_dir);
    snprintf(bak_fields, sizeof(bak_fields), "%s/backup/%s/fields.conf", obj_dir, from);

    ASSERT_EQ_INT(unlink(live_fields), 0, "remove the live fields.conf");

    char req[512], *resp = NULL;
    rs_restore_req(req, sizeof(req), obj, from);
    ASSERT_EQ_INT(test_env_ctl_set_restore_fault(&env, SHARD_TEST_RF_STAGE_COPY, 0), 0,
                  "arm the staging-copy fault");
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "restore exchange");
    ASSERT_CONTAINS(resp, "the live object was not changed", "staging failure says untouched");
    free(resp); resp = NULL;
    ASSERT_TRUE(access(live_fields, F_OK) != 0,
                "a failed restore does not install fields.conf");

    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "clean restore exchange");
    ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "the clean restore succeeds");
    free(resp); resp = NULL;
    ASSERT_TRUE(access(live_fields, F_OK) == 0, "the clean restore installs fields.conf");
    ASSERT_TRUE(rs_equal_files(live_fields, bak_fields),
                "the installed fields.conf is the backup's");
    char *r = rs_req(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"k1\"}", obj);
    ASSERT_CONTAINS(r, "alpha", "the restored record is readable");
    free(r);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* A missing schema.conf line is re-created after the commit (guard: the
   pre-staging append already did this before the fix). */
static int test_restore_missing_schema_line(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rsf_line";
    char from[128] = "", prefix[64], req[512], *resp = NULL;
    if (!rs_setup(tc, obj, from, sizeof(from))) {
        ASSERT_TRUE(0, "rs_setup");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    snprintf(prefix, sizeof(prefix), "default:%s:", obj);
    char sed[PATH_MAX + 64];
    snprintf(sed, sizeof(sed), "sed -i '/^%s/d' %s/schema.conf", prefix, env.db_root);
    ASSERT_EQ_INT(tu_run_cmd("%s", sed), 0, "delete the schema.conf line");
    ASSERT_EQ_INT(rs_count_schema_lines(env.db_root, prefix), 0, "the line is gone");

    rs_restore_req(req, sizeof(req), obj, from);
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "restore exchange");
    ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "the restore succeeds");
    free(resp); resp = NULL;
    ASSERT_EQ_INT(rs_count_schema_lines(env.db_root, prefix), 1,
                  "exactly one schema.conf line after the restore");

    tc_close(tc);
    test_env_stop_keep(&env);
    if (test_env_start_at(&env, env.db_root, env.port) != 0) {
        ASSERT_TRUE(0, "the restarted daemon serves the object");
        test_env_stop(&env); return 1;
    }
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *r = rs_req(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"k1\"}", obj);
    ASSERT_CONTAINS(r, "alpha", "the restored record survives the restart");
    free(r);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* A failed post-commit schema-line install is reported and the data stays
   live; a re-run installs it. */
static int test_restore_schema_install_failure_reported(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rsf_install";
    char from[128] = "", prefix[64], req[512], *resp = NULL;
    if (!rs_setup(tc, obj, from, sizeof(from))) {
        ASSERT_TRUE(0, "rs_setup");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    snprintf(prefix, sizeof(prefix), "default:%s:", obj);
    char sed[PATH_MAX + 64];
    snprintf(sed, sizeof(sed), "sed -i '/^%s/d' %s/schema.conf", prefix, env.db_root);
    ASSERT_EQ_INT(tu_run_cmd("%s", sed), 0, "delete the schema.conf line");

    ASSERT_EQ_INT(test_env_ctl_set_restore_fault(&env, SHARD_TEST_RF_SCHEMA_INSTALL, 0), 0,
                  "arm the schema-install fault");
    rs_restore_req(req, sizeof(req), obj, from);
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "restore exchange");
    ASSERT_CONTAINS(resp, "restore committed but the schema.conf line could not be installed",
                    "the failed install is reported");
    free(resp); resp = NULL;
    ASSERT_EQ_INT(rs_count_schema_lines(env.db_root, prefix), 0, "no line was installed");

    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "clean restore exchange");
    ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "the re-run succeeds");
    free(resp); resp = NULL;
    ASSERT_EQ_INT(rs_count_schema_lines(env.db_root, prefix), 1,
                  "the re-run installs exactly one line");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* A manifest without valid storage_version/streams cannot recreate the line;
   the restore refuses before mutating anything (AUD-P3-6 on this path). */
static int test_restore_refuses_manifest_without_streams(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rsf_streams";
    char from[128] = "", prefix[64], req[512], *resp = NULL;
    if (!rs_setup(tc, obj, from, sizeof(from))) {
        ASSERT_TRUE(0, "rs_setup");
        tc_close(tc); test_env_stop(&env); return 1;
    }
    snprintf(prefix, sizeof(prefix), "default:%s:", obj);
    char sed[PATH_MAX + 128];
    snprintf(sed, sizeof(sed), "sed -i '/^%s/d' %s/schema.conf", prefix, env.db_root);
    ASSERT_EQ_INT(tu_run_cmd("%s", sed), 0, "delete the schema.conf line");
    snprintf(sed, sizeof(sed), "sed -i 's/\"streams\":[0-9]*,//' %s/default/%s/backup/%s/object.json",
             env.db_root, obj, from);
    ASSERT_EQ_INT(tu_run_cmd("%s", sed), 0, "strip streams from the manifest");

    rs_restore_req(req, sizeof(req), obj, from);
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "restore exchange");
    ASSERT_CONTAINS(resp, "lacks a valid", "the manifest is refused");
    free(resp); resp = NULL;
    ASSERT_EQ_INT(rs_count_schema_lines(env.db_root, prefix), 0,
                  "no schema.conf line is created");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* D8: the bitmap cache is dropped on restore, so an indexed bool count sees
   the restored records, not the pre-restore ones. */
static int test_restore_drops_bitmap_cache(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    ASSERT_TRUE(env.test_control_fd >= 0, "test-server daemon with a control channel");
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    const char *obj = "rsf_bitmap";
    char from[128] = "", req[512], *resp = NULL;
    char *r = rs_req(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    free(r);
    r = rs_req(tc,
               "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
               "\"splits\":%d,\"max_key\":%d,"
               "\"fields\":[\"flag:bool\",\"name:varchar:16\"],\"indexes\":[\"flag\"]}",
               obj, RS_OBJ_SPLITS, RS_OBJ_MAX_KEY);
    free(r);
    for (int i = 1; i <= 3; i++) {
        r = rs_req(tc,
                   "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
                   "\"key\":\"k%d\",\"value\":{\"flag\":true,\"name\":\"n%d\"}}",
                   obj, i, i);
        free(r);
    }
    ASSERT_TRUE(rs_backup(tc, obj, from, sizeof(from)), "backup");
    for (int i = 4; i <= 8; i++) {
        r = rs_req(tc,
                   "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
                   "\"key\":\"k%d\",\"value\":{\"flag\":true,\"name\":\"n%d\"}}",
                   obj, i, i);
        free(r);
    }
    r = rs_req(tc,
               "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
               "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]}", obj);
    ASSERT_EQ_INT(tu_parse_count(r), 8, "the indexed count warms the cache (8)");
    free(r);

    rs_restore_req(req, sizeof(req), obj, from);
    ASSERT_EQ_INT(tc_request(tc, req, &resp), 0, "restore exchange");
    ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "the restore succeeds");
    free(resp); resp = NULL;

    r = rs_req(tc,
               "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
               "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]}", obj);
    ASSERT_EQ_INT(tu_parse_count(r), 3, "the indexed count sees the restored records (3)");
    free(r);
    r = rs_req(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\"}", obj);
    ASSERT_EQ_INT(tu_parse_count(r), 3, "the plain count is 3");
    free(r);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-restore-commit-before-cleanup", test_restore_commit_before_cleanup)
TEST_REGISTER("test-restore-crash-after-renames-rolls-back", test_restore_crash_after_renames_rolls_back)
TEST_REGISTER("test-restore-commit-dirsync-failure-rolls-back", test_restore_commit_dirsync_failure_rolls_back)
TEST_REGISTER("test-restore-unswap-sync-failure-rolls-back", test_restore_unswap_sync_failure_rolls_back)
TEST_REGISTER("test-restore-unrecoverable-rollback-aborts", test_restore_unrecoverable_rollback_aborts)
TEST_REGISTER("test-restore-swap-failure-rollback-rename-fails", test_restore_swap_failure_rollback_rename_fails)
TEST_REGISTER("test-restore-absent-files-rolls-back", test_restore_absent_files_rolls_back)
TEST_REGISTER("test-restore-startup-refuses-unrecoverable-stage", test_restore_startup_refuses_unrecoverable_stage)
TEST_REGISTER("test-restore-refuses-unrecoverable-leftover", test_restore_refuses_unrecoverable_leftover)
TEST_REGISTER("test-restore-forced-fields-conf-not-touched-on-failure",
              test_restore_forced_fields_conf_not_touched_on_failure)
TEST_REGISTER("test-restore-missing-fields-conf", test_restore_missing_fields_conf)
TEST_REGISTER("test-restore-missing-schema-line", test_restore_missing_schema_line)
TEST_REGISTER("test-restore-schema-install-failure-reported",
              test_restore_schema_install_failure_reported)
TEST_REGISTER("test-restore-refuses-manifest-without-streams",
              test_restore_refuses_manifest_without_streams)
TEST_REGISTER("test-restore-drops-bitmap-cache", test_restore_drops_bitmap_cache)
