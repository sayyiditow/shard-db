/* src/test/cases/test_objlock.c
 * Port of tests/test-objlock.sh — per-object rwlock + crash-recovery
 * sweep of stale .new/.old artifacts on startup, plus MAX_KEY_CEILING.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
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

static void stop_no_wipe(TestEnv *env) {
    if (!env || env->daemon_pid <= 0) return;
    kill(env->daemon_pid, SIGTERM);
    for (int i = 0; i < 50; i++) {
        if (waitpid(env->daemon_pid, NULL, WNOHANG) == env->daemon_pid) {
            env->daemon_pid = -1; return;
        }
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    kill(env->daemon_pid, SIGKILL);
    waitpid(env->daemon_pid, NULL, 0);
    env->daemon_pid = -1;
}

static int run_cmd(const char *fmt, ...) {
    char cmd[2048];
    va_list ap; va_start(ap, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    return system(cmd);
}

static int path_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static int test_objlock_run(void) {
    /* Use caller-managed db_root so we can stop+restart against same state. */
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-objlock-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); run_cmd("rm -rf %s", base); return 1; }

    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "first daemon spawn");
        run_cmd("rm -rf %s", base);
        return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_kill(&env); run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"leads\","
        "\"splits\":16,\"max_key\":32,"
        "\"fields\":[\"name:varchar:32\",\"age:int\"],"
        "\"indexes\":[]}", &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"key\":\"a\",\"value\":{\"name\":\"alice\",\"age\":30}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"key\":\"b\",\"value\":{\"name\":\"bob\",\"age\":25}}", &resp); free(resp); resp = NULL;

    char path[400];
    snprintf(path, sizeof(path), "%s/default/leads/data", db_root);
    ASSERT_TRUE(path_exists(path), "leads/data exists after insert");

    /* Stop daemon (graceful, no wipe), inject stale crash-recovery artifacts. */
    tc_close(tc); tc = NULL;
    stop_no_wipe(&env);

    char obj[300];
    snprintf(obj, sizeof(obj), "%s/default/leads", db_root);

    run_cmd("mkdir -p %s/data.new/00", obj);
    run_cmd("echo 'stale shard' > %s/data.new/00/00.bin", obj);
    run_cmd("mkdir -p %s/indexes.new", obj);
    run_cmd("echo 'stale idx' > %s/indexes.new/stale.idx", obj);
    run_cmd("echo 'stale fields' > %s/fields.conf.new", obj);
    run_cmd("echo 'stale schema' > %s/schema.conf.new", obj);
    run_cmd("mkdir -p %s/data.old", obj);
    run_cmd("echo 'stale old' > %s/data.old/leftover", obj);

    snprintf(path, sizeof(path), "%s/data.new", obj);
    ASSERT_TRUE(path_exists(path), "injected data.new before recovery");
    snprintf(path, sizeof(path), "%s/indexes.new", obj);
    ASSERT_TRUE(path_exists(path), "injected indexes.new before recovery");
    snprintf(path, sizeof(path), "%s/fields.conf.new", obj);
    ASSERT_TRUE(path_exists(path), "injected fields.conf.new before recovery");
    snprintf(path, sizeof(path), "%s/schema.conf.new", obj);
    ASSERT_TRUE(path_exists(path), "injected schema.conf.new before recovery");
    snprintf(path, sizeof(path), "%s/data.old", obj);
    ASSERT_TRUE(path_exists(path), "injected data.old before recovery");

    /* Restart at same db_root — recovery should sweep. */
    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "respawn at same db_root");
        run_cmd("rm -rf %s", base);
        return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after restart");
    if (!tc) { test_env_kill(&env2); run_cmd("rm -rf %s", base); return 1; }

    snprintf(path, sizeof(path), "%s/data.new", obj);
    ASSERT_TRUE(!path_exists(path), "data.new removed after startup");
    snprintf(path, sizeof(path), "%s/indexes.new", obj);
    ASSERT_TRUE(!path_exists(path), "indexes.new removed after startup");
    snprintf(path, sizeof(path), "%s/fields.conf.new", obj);
    ASSERT_TRUE(!path_exists(path), "fields.conf.new removed after startup");
    snprintf(path, sizeof(path), "%s/schema.conf.new", obj);
    ASSERT_TRUE(!path_exists(path), "schema.conf.new removed after startup");
    snprintf(path, sizeof(path), "%s/data.old", obj);
    ASSERT_TRUE(!path_exists(path), "data.old removed after startup");
    snprintf(path, sizeof(path), "%s/data", obj);
    ASSERT_TRUE(path_exists(path), "data/ still present");

    /* Server still functional after recovery. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"leads\",\"key\":\"a\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"alice\"", "GET a returns alice");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"key\":\"c\",\"value\":{\"name\":\"carol\",\"age\":40}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"leads\",\"key\":\"c\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"carol\"", "INSERT+GET c");
    free(resp); resp = NULL;

    /* No regression: 200 sequential inserts complete. */
    char req[256];
    int ok = 1;
    for (int i = 1; i <= 200; i++) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"leads\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"x\",\"age\":1}}", i);
        if (tc_request(tc, req, &resp) != 0) { ok = 0; free(resp); resp = NULL; break; }
        free(resp); resp = NULL;
    }
    ASSERT_TRUE(ok, "200 sequential inserts complete");

    /* MAX_KEY_CEILING — max_key=2000 must be rejected, max_key=1024 accepted. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"keylim\","
        "\"splits\":16,\"max_key\":2000,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "create-object rejects max_key=2000");
    ASSERT_CONTAINS(resp, "exceeds ceiling", "rejection mentions ceiling");
    free(resp); resp = NULL;
    snprintf(path, sizeof(path), "%s/default/keylim", db_root);
    ASSERT_TRUE(!path_exists(path), "rejected object not created on disk");

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"keylim\","
        "\"splits\":16,\"max_key\":1024,\"fields\":[\"v:int\"],\"indexes\":[]}",
        &resp);
    ASSERT_TRUE(!resp || strstr(resp, "\"error\"") == NULL, "create-object accepts max_key=1024");
    free(resp); resp = NULL;

    tc_close(tc);
    stop_no_wipe(&env2);
    run_cmd("rm -rf %s", base);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-objlock", test_objlock_run)
