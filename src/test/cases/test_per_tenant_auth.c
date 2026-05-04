/* src/test/cases/test_per_tenant_auth.c
 * Port of tests/test-per-tenant-auth.sh — per-tenant token scoping.
 * Custom daemon spawn (DISABLE_LOCALHOST_TRUST=1, pre-seeded global token).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
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

static int run_cmd(const char *fmt, ...) {
    char cmd[2048];
    va_list ap; va_start(ap, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    return system(cmd);
}

static int file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static int file_size(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 ? (int)st.st_size : -1;
}

static int file_contains(const char *path, const char *needle) {
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    char buf[4096]; int found = 0;
    while (fgets(buf, sizeof(buf), f)) {
        if (strstr(buf, needle)) { found = 1; break; }
    }
    fclose(f);
    return found;
}

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

static int spawn_daemon(const char *base, const char *db_root, int port,
                        const char *shard_db_abs, const char *gtok,
                        TestEnv *env) {
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        chdir(base);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }
    env->port = port; env->daemon_pid = pid;
    snprintf(env->db_root, sizeof(env->db_root), "%s", db_root);

    for (int i = 0; i < 100; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            char preq[256]; snprintf(preq, sizeof(preq), "{\"mode\":\"db-dirs\",\"auth\":\"%s\"}", gtok);
            int ok = tc_request(probe, preq, &r) == 0 && r && strstr(r, "\"error\"") == NULL;
            free(r); tc_close(probe);
            if (ok) return 0;
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    return -1;
}

static int test_per_tenant_auth_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-pta-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    run_cmd("rm -rf %s", base);
    mkdir(base, 0755); mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); run_cmd("rm -rf %s", base); return 1; }

    /* dirs.conf: default + tenant_a + tenant_b. */
    char p[400];
    snprintf(p, sizeof(p), "%s/dirs.conf", db_root);
    FILE *f = fopen(p, "w");
    if (f) { fputs("default\ntenant_a\ntenant_b\n", f); fclose(f); }
    run_cmd("mkdir -p %s/tenant_a %s/tenant_b", db_root, db_root);
    run_cmd("touch %s/allowed_ips.conf", db_root);

    char gtok[64]; snprintf(gtok, sizeof(gtok), "sdb_global_admin_%d", (int)time(NULL));
    snprintf(p, sizeof(p), "%s/tokens.conf", db_root);
    f = fopen(p, "w"); if (f) { fprintf(f, "%s\n", gtok); fclose(f); }

    char env_path[300]; snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    f = fopen(env_path, "w");
    if (!f) { ASSERT_TRUE(0, "open db.env"); run_cmd("rm -rf %s", base); return 1; }
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
        "export LOG_LEVEL=2\n"
        "export THREADS=0\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
        "export DISABLE_LOCALHOST_TRUST=1\n",
        db_root, port, base);
    fclose(f);
    char logs_dir[400]; snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    mkdir(logs_dir, 0755);

    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db not found"); run_cmd("rm -rf %s", base); return 1;
    }

    TestEnv env = {0};
    if (spawn_daemon(base, db_root, port, shard_db_abs, gtok, &env) != 0) {
        ASSERT_TRUE(0, "spawn daemon"); run_cmd("rm -rf %s", base); return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { stop_no_wipe(&env); run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL; char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"tenant_a\",\"object\":\"users\","
        "\"auth\":\"%s\",\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\"]}", gtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"tenant_b\",\"object\":\"users\","
        "\"auth\":\"%s\",\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\"]}", gtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"tenant_a\",\"object\":\"users\","
        "\"key\":\"a1\",\"value\":{\"name\":\"Alice\"},\"auth\":\"%s\"}", gtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"tenant_b\",\"object\":\"users\","
        "\"key\":\"b1\",\"value\":{\"name\":\"Bob\"},\"auth\":\"%s\"}", gtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    /* Global token works in any dir. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\","
        "\"key\":\"a1\",\"auth\":\"%s\"}", gtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"Alice\"", "global reads tenant_a");
    free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"tenant_b\",\"object\":\"users\","
        "\"key\":\"b1\",\"auth\":\"%s\"}", gtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"Bob\"", "global reads tenant_b");
    free(resp); resp = NULL;
    snprintf(req, sizeof(req), "{\"mode\":\"stats\",\"auth\":\"%s\"}", gtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"uptime_ms\"", "global runs stats (admin)");
    free(resp); resp = NULL;
    snprintf(req, sizeof(req), "{\"mode\":\"db-dirs\",\"auth\":\"%s\"}", gtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "tenant_a", "global runs db-dirs (admin)");
    free(resp); resp = NULL;

    /* Missing/wrong token. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\",\"key\":\"a1\"}",
                   &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "no token rejected");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\","
        "\"key\":\"a1\",\"auth\":\"wrong\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "wrong token rejected");
    ASSERT_TRUE(resp && strstr(resp, "wrong") == NULL, "error does not leak token");
    free(resp); resp = NULL;

    /* add-token persists to tenant-local file. */
    char tatok[64]; snprintf(tatok, sizeof(tatok), "sdb_tenant_a_%d_v1", (int)time(NULL));
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"%s\","
        "\"dir\":\"tenant_a\"}", gtok, tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"token_added\"", "add-token (tenant_a) status");
    ASSERT_CONTAINS(resp, "\"scope\":\"tenant_a\"", "add-token echoes scope");
    free(resp); resp = NULL;

    char ta_path[400]; snprintf(ta_path, sizeof(ta_path), "%s/tenant_a/tokens.conf", db_root);
    ASSERT_TRUE(file_contains(ta_path, tatok), "tenant token saved in tenant_a/tokens.conf");
    char glb_path[400]; snprintf(glb_path, sizeof(glb_path), "%s/tokens.conf", db_root);
    ASSERT_TRUE(!file_contains(glb_path, tatok), "tenant token NOT in global tokens.conf");

    char tbtok[64]; snprintf(tbtok, sizeof(tbtok), "sdb_tenant_b_%d_v1", (int)time(NULL));
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"%s\","
        "\"dir\":\"tenant_b\"}", gtok, tbtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    /* Tenant token works in own dir. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\","
        "\"key\":\"a1\",\"auth\":\"%s\"}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"Alice\"", "tenant_a token reads tenant_a");
    free(resp); resp = NULL;

    /* Cross-tenant rejected. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"tenant_b\",\"object\":\"users\","
        "\"key\":\"b1\",\"auth\":\"%s\"}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "tenant_a token rejected on tenant_b");
    free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\","
        "\"key\":\"a1\",\"auth\":\"%s\"}", tbtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "tenant_b token rejected on tenant_a");
    free(resp); resp = NULL;

    /* Tenant token rejected on admin. */
    snprintf(req, sizeof(req), "{\"mode\":\"stats\",\"auth\":\"%s\"}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "tenant token rejected on stats");
    free(resp); resp = NULL;
    snprintf(req, sizeof(req), "{\"mode\":\"db-dirs\",\"auth\":\"%s\"}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "tenant token rejected on db-dirs");
    free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"tenant_a\",\"object\":\"x\","
        "\"auth\":\"%s\",\"splits\":16,\"max_key\":16,\"fields\":[\"n:int\"]}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "tenant token rejected on create-object");
    free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"new\"}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "tenant token rejected on add-token");
    free(resp); resp = NULL;

    /* list-tokens. */
    snprintf(req, sizeof(req), "{\"mode\":\"list-tokens\",\"auth\":\"%s\"}", gtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"scope\":\"global\"", "list-tokens shows global scope");
    ASSERT_CONTAINS(resp, "\"scope\":\"tenant_a\"", "list-tokens shows tenant_a scope");
    ASSERT_CONTAINS(resp, "\"scope\":\"tenant_b\"", "list-tokens shows tenant_b scope");
    ASSERT_TRUE(resp && strstr(resp, tatok) == NULL, "full token never printed");
    free(resp); resp = NULL;

    /* remove-token rewrites correct file. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"remove-token\",\"auth\":\"%s\",\"token\":\"%s\"}", gtok, tatok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    ASSERT_TRUE(!file_exists(ta_path) || file_size(ta_path) == 0,
                "tenant_a tokens.conf empty after removal");
    ASSERT_TRUE(file_contains(glb_path, gtok), "global tokens.conf still has admin token");

    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\","
        "\"key\":\"a1\",\"auth\":\"%s\"}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "revoked tenant token rejected");
    free(resp); resp = NULL;

    /* Persistence across restart. */
    tc_close(tc); tc = NULL;
    stop_no_wipe(&env);

    char rtok[64]; snprintf(rtok, sizeof(rtok), "sdb_restart_test_%d", (int)time(NULL));
    char tbpath[400]; snprintf(tbpath, sizeof(tbpath), "%s/tenant_b/tokens.conf", db_root);
    f = fopen(tbpath, "w"); if (f) { fprintf(f, "%s\n", rtok); fclose(f); }

    if (spawn_daemon(base, db_root, port, shard_db_abs, gtok, &env) != 0) {
        ASSERT_TRUE(0, "respawn daemon"); run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = port;
    tc = tc_connect(&cfg);
    if (tc) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"tenant_b\",\"object\":\"users\","
            "\"key\":\"b1\",\"auth\":\"%s\"}", rtok);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"name\":\"Bob\"", "disk-written tenant token loaded on start");
        free(resp); resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"users\","
            "\"key\":\"a1\",\"auth\":\"%s\"}", rtok);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"",
                        "disk-written tenant token scoped to its dir");
        free(resp); resp = NULL;
        tc_close(tc);
    }

    stop_no_wipe(&env);
    run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-per-tenant-auth", test_per_tenant_auth_run)
