/* src/test/cases/test_token_perms.c
 * Port of tests/test-token-perms.sh — per-object tokens + r/rw/rwx perms.
 * Requires DISABLE_LOCALHOST_TRUST so loopback doesn't bypass auth. The
 * fixture writes its own db.env, so we override it after creating the env
 * (but before spawning the daemon — done via a custom local helper).
 *
 * For the bare-line legacy test + TOKEN_CAP smoke, we stop the daemon and
 * mutate db.env / tokens.conf directly, then restart at the same db_root.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <fcntl.h>
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



static int file_contains(const char *path, const char *needle) {
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    char buf[4096];
    int found = 0;
    while (fgets(buf, sizeof(buf), f)) {
        if (strstr(buf, needle)) { found = 1; break; }
    }
    fclose(f);
    return found;
}

static int test_token_perms_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-tp-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    /* Pre-seed: dirs.conf with default + tp_acme + tp_beta.
       Pre-seed tokens.conf with a global admin token (must be readable BEFORE
       daemon starts so DISABLE_LOCALHOST_TRUST=1 doesn't lock us out). */
    char dirs_path[300]; snprintf(dirs_path, sizeof(dirs_path), "%s/dirs.conf", db_root);
    FILE *f = fopen(dirs_path, "w");
    if (f) { fputs("default\ntp_acme\ntp_beta\n", f); fclose(f); }
    tu_run_cmd("mkdir -p %s/tp_acme %s/tp_beta", db_root, db_root);
    tu_run_cmd("touch %s/allowed_ips.conf", db_root);

    char gtok[64]; snprintf(gtok, sizeof(gtok), "sdb_tp_admin_%d", (int)time(NULL));
    char gtok_path[300]; snprintf(gtok_path, sizeof(gtok_path), "%s/tokens.conf", db_root);
    f = fopen(gtok_path, "w");
    if (f) { fprintf(f, "%s\n", gtok); fclose(f); }

    /* Write db.env. We replicate fixture's content but add DISABLE_LOCALHOST_TRUST=1. */
    char env_path[300]; snprintf(env_path, sizeof(env_path), "%s/db.env", base);
    f = fopen(env_path, "w");
    if (!f) { ASSERT_TRUE(0, "open db.env"); tu_run_cmd("rm -rf %s", base); return 1; }
    fprintf(f,
        "export DB_ROOT=\"%s\"\n"
        "export PORT=%d\n"
        "export TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\n"
        "export LOG_LEVEL=2\n"
        "export THREADS=2\n"
        "export FCACHE_MAX=4096\n"
        "export TLS_ENABLE=0\n"
        "export DISABLE_LOCALHOST_TRUST=1\n",
        db_root, port, base);
    fclose(f);
    char logs_dir[400]; snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);
    mkdir(logs_dir, 0755);

    /* Spawn daemon with chdir to base (manual fork — fixture would overwrite env). */
    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db not found"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    pid_t pid = fork();
    if (pid < 0) { ASSERT_TRUE(0, "fork"); tu_run_cmd("rm -rf %s", base); return 1; }
    if (pid == 0) {
        chdir(base);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }
    TestEnv env = { .port = port, .daemon_pid = pid };
    snprintf(env.db_root, sizeof(env.db_root), "%s", db_root);

    /* Wait for daemon by polling auth'd db-dirs through a TCP probe. */
    int ready = 0;
    for (int i = 0; i < 100; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            char req[256];
            snprintf(req, sizeof(req),
                "{\"mode\":\"db-dirs\",\"auth\":\"%s\"}", gtok);
            if (tc_request(probe, req, &r) == 0 && r) {
                if (strstr(r, "\"error\"") == NULL) ready = 1;
            }
            free(r); tc_close(probe);
            if (ready) break;
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    if (!ready) {
        ASSERT_TRUE(0, "daemon ready");
        kill(pid, SIGKILL); waitpid(pid, NULL, 0);
        tu_run_cmd("rm -rf %s", base); return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop_keep(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL; char req[1024];
    /* Setup: 3 objects across 2 tenants. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"auth\":\"%s\",\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"status:varchar:16\",\"amount:int\"]}", gtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"tp_acme\",\"object\":\"users\","
        "\"auth\":\"%s\",\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\"]}", gtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"tp_beta\",\"object\":\"orders\","
        "\"auth\":\"%s\",\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"status:varchar:16\"]}", gtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"key\":\"o1\",\"value\":{\"status\":\"paid\",\"amount\":100},"
        "\"auth\":\"%s\"}", gtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"tp_acme\",\"object\":\"users\","
        "\"key\":\"u1\",\"value\":{\"name\":\"Alice\"},\"auth\":\"%s\"}", gtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"tp_beta\",\"object\":\"orders\","
        "\"key\":\"b1\",\"value\":{\"status\":\"paid\"},\"auth\":\"%s\"}", gtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    /* === perm=r === */
    char rtok[64]; snprintf(rtok, sizeof(rtok), "sdb_tp_read_%d", (int)time(NULL));
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"%s\","
        "\"dir\":\"tp_acme\",\"perm\":\"r\"}", gtok, rtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"key\":\"o1\",\"auth\":\"%s\"}", rtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"", "perm=r reads ok");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"key\":\"new\",\"value\":{\"status\":\"x\",\"amount\":1},\"auth\":\"%s\"}", rtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "perm=r insert rejected");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"delete\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"key\":\"o1\",\"auth\":\"%s\"}", rtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "perm=r delete rejected");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"find\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"criteria\":[],\"auth\":\"%s\"}", rtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"o1\"", "perm=r find ok");
    free(resp); resp = NULL;

    /* === perm=rw === */
    char rwtok[64]; snprintf(rwtok, sizeof(rwtok), "sdb_tp_rw_%d", (int)time(NULL));
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"%s\","
        "\"dir\":\"tp_acme\",\"perm\":\"rw\"}", gtok, rwtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"key\":\"o2\",\"value\":{\"status\":\"pending\",\"amount\":50},"
        "\"auth\":\"%s\"}", rwtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "perm=rw insert ok");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"delete\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"key\":\"o2\",\"auth\":\"%s\"}", rwtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"deleted\"", "perm=rw delete ok");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"tp_acme\",\"object\":\"newthing\","
        "\"auth\":\"%s\",\"splits\":16,\"max_key\":16,\"fields\":[\"n:int\"]}", rwtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "perm=rw create-object rejected");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"add-field\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"fields\":[\"note:varchar:32\"],\"auth\":\"%s\"}", rwtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "perm=rw add-field rejected");
    free(resp); resp = NULL;

    /* === tenant rwx === */
    char tatok[64]; snprintf(tatok, sizeof(tatok), "sdb_tp_tadmin_%d", (int)time(NULL));
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"%s\","
        "\"dir\":\"tp_acme\",\"perm\":\"rwx\"}", gtok, tatok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"tp_acme\",\"object\":\"widgets\","
        "\"auth\":\"%s\",\"splits\":16,\"max_key\":16,\"fields\":[\"n:int\"]}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "tenant-rwx create-object on own dir");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"add-field\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"fields\":[\"note:varchar:32\"],\"auth\":\"%s\"}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":", "tenant-rwx add-field on own dir");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req), "{\"mode\":\"stats\",\"auth\":\"%s\"}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "tenant-rwx stats rejected");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req), "{\"mode\":\"db-dirs\",\"auth\":\"%s\"}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "tenant-rwx db-dirs rejected");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"foo\","
        "\"dir\":\"tp_acme\"}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"unauthorized\"", "tenant-rwx cannot add-token");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"tp_beta\",\"object\":\"x\","
        "\"auth\":\"%s\",\"splits\":16,\"max_key\":16,\"fields\":[\"n:int\"]}", tatok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "tenant-rwx rejected on other dir");
    free(resp); resp = NULL;

    /* === object scope add-token === */
    char otok[64]; snprintf(otok, sizeof(otok), "sdb_tp_obj_%d", (int)time(NULL));
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"%s\","
        "\"dir\":\"tp_acme\",\"object\":\"orders\",\"perm\":\"rw\"}", gtok, otok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"token_added\"", "object-scoped add-token success");
    ASSERT_CONTAINS(resp, "\"scope\":\"tp_acme/orders\"", "scope shows dir/obj");
    free(resp); resp = NULL;

    char obj_tok_path[300];
    snprintf(obj_tok_path, sizeof(obj_tok_path), "%s/tp_acme/orders/tokens.conf", db_root);
    ASSERT_TRUE(file_contains(obj_tok_path, otok),
                "object-scoped token saved in <dir>/<obj>/tokens.conf");
    {
        char needle[80]; snprintf(needle, sizeof(needle), "%s:rw", otok);
        ASSERT_TRUE(file_contains(obj_tok_path, needle), "file line has :rw suffix");
    }

    /* === object-rw token: only works on (dir, object) === */
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"key\":\"o1\",\"auth\":\"%s\"}", otok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"", "object token reads target");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"key\":\"o3\",\"value\":{\"status\":\"x\",\"amount\":1},\"auth\":\"%s\"}", otok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "object token writes target (rw)");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"tp_acme\",\"object\":\"users\","
        "\"key\":\"u1\",\"auth\":\"%s\"}", otok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "object token rejected on sibling object");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"tp_beta\",\"object\":\"orders\","
        "\"key\":\"b1\",\"auth\":\"%s\"}", otok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"", "object token rejected on other tenant");
    free(resp); resp = NULL;

    /* === object-rwx === */
    char oxtok[64]; snprintf(oxtok, sizeof(oxtok), "sdb_tp_objx_%d", (int)time(NULL));
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"%s\","
        "\"dir\":\"tp_acme\",\"object\":\"orders\",\"perm\":\"rwx\"}", gtok, oxtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"add-index\",\"dir\":\"tp_acme\",\"object\":\"orders\","
        "\"field\":\"status\",\"auth\":\"%s\"}", oxtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":", "object-rwx add-index on target");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"tp_acme\",\"object\":\"something_new\","
        "\"auth\":\"%s\",\"splits\":16,\"max_key\":16,\"fields\":[\"n:int\"]}", oxtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"auth failed\"",
                    "object-rwx cannot create-object (tenant-scope)");
    free(resp); resp = NULL;

    /* === default perm on add-token is 'rw' === */
    char dtok[64]; snprintf(dtok, sizeof(dtok), "sdb_tp_default_%d", (int)time(NULL));
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"%s\","
        "\"dir\":\"tp_acme\"}", gtok, dtok);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    char tenant_tok_path[300];
    snprintf(tenant_tok_path, sizeof(tenant_tok_path), "%s/tp_acme/tokens.conf", db_root);
    {
        char needle[80]; snprintf(needle, sizeof(needle), "%s:rw", dtok);
        ASSERT_TRUE(file_contains(tenant_tok_path, needle), "default perm written as :rw");
    }

    /* === invalid perm rejected === */
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"bad1\","
        "\"dir\":\"tp_acme\",\"perm\":\"x\"}", gtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "invalid perm", "perm=x rejected");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"bad2\","
        "\"dir\":\"tp_acme\",\"perm\":\"rx\"}", gtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "invalid perm", "perm=rx rejected");
    free(resp); resp = NULL;

    /* === object scope requires dir === */
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"bad3\","
        "\"object\":\"orders\"}", gtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "object scope requires dir", "object-without-dir rejected");
    free(resp); resp = NULL;

    /* === object must exist === */
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"bad4\","
        "\"dir\":\"tp_acme\",\"object\":\"nonexistent\"}", gtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "object not found", "nonexistent object rejected");
    free(resp); resp = NULL;

    /* === list-tokens === */
    snprintf(req, sizeof(req), "{\"mode\":\"list-tokens\",\"auth\":\"%s\"}", gtok);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"scope\":\"global\"", "list shows global");
    ASSERT_CONTAINS(resp, "\"scope\":\"tp_acme\"", "list shows tenant");
    ASSERT_CONTAINS(resp, "\"scope\":\"tp_acme/orders\"", "list shows object");
    ASSERT_CONTAINS(resp, "\"perm\":\"r\"", "list shows perm r");
    ASSERT_CONTAINS(resp, "\"perm\":\"rw\"", "list shows perm rw");
    ASSERT_CONTAINS(resp, "\"perm\":\"rwx\"", "list shows perm rwx");
    free(resp); resp = NULL;

    /* === backward compat: bare-line token = rwx === */
    tc_close(tc); tc = NULL;
    test_env_stop_keep(&env);

    char ltok[64]; snprintf(ltok, sizeof(ltok), "sdb_tp_legacy_%d", (int)time(NULL));
    f = fopen(gtok_path, "w");
    if (f) {
        fprintf(f, "%s\n%s\n", ltok, gtok);   /* bare LEGACY first, then admin */
        fclose(f);
    }

    /* Restart manually. */
    pid = fork();
    if (pid < 0) { ASSERT_TRUE(0, "fork restart"); tu_run_cmd("rm -rf %s", base); return 1; }
    if (pid == 0) {
        chdir(base);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }
    env.daemon_pid = pid;
    /* Poll until ready. */
    ready = 0;
    for (int i = 0; i < 100; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            char preq[256]; snprintf(preq, sizeof(preq), "{\"mode\":\"db-dirs\",\"auth\":\"%s\"}", gtok);
            if (tc_request(probe, preq, &r) == 0 && r && strstr(r, "\"error\"") == NULL) ready = 1;
            free(r); tc_close(probe);
            if (ready) break;
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    ASSERT_TRUE(ready, "daemon ready after legacy-tok restart");

    cfg.port = port;
    tc = tc_connect(&cfg);
    if (tc) {
        snprintf(req, sizeof(req), "{\"mode\":\"stats\",\"auth\":\"%s\"}", ltok);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"uptime_ms\"", "bare-line token = admin (runs stats)");
        free(resp); resp = NULL;
    }

    /* === TOKEN_CAP smoke === */
    if (tc) { tc_close(tc); tc = NULL; }
    test_env_stop_keep(&env);

    f = fopen(env_path, "a");
    if (f) { fputs("export TOKEN_CAP=4096\n", f); fclose(f); }

    pid = fork();
    if (pid < 0) { ASSERT_TRUE(0, "fork TOKEN_CAP"); tu_run_cmd("rm -rf %s", base); return 1; }
    if (pid == 0) {
        chdir(base);
        execl(shard_db_abs, shard_db_abs, "server", (char *)NULL);
        _exit(127);
    }
    env.daemon_pid = pid;
    ready = 0;
    for (int i = 0; i < 100; i++) {
        TestClientCfg pc = { .port = port, .connect_timeout_ms = 200 };
        TestClient *probe = tc_connect(&pc);
        if (probe) {
            char *r = NULL;
            char preq[256]; snprintf(preq, sizeof(preq), "{\"mode\":\"db-dirs\",\"auth\":\"%s\"}", gtok);
            if (tc_request(probe, preq, &r) == 0 && r && strstr(r, "\"error\"") == NULL) ready = 1;
            free(r); tc_close(probe);
            if (ready) break;
        }
        struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    }
    ASSERT_TRUE(ready, "daemon ready with TOKEN_CAP=4096");
    cfg.port = port;
    tc = tc_connect(&cfg);
    if (tc) {
        snprintf(req, sizeof(req), "{\"mode\":\"stats\",\"auth\":\"%s\"}", gtok);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"uptime_ms\"", "server runs with TOKEN_CAP=4096");
        free(resp); resp = NULL;
        tc_close(tc);
    }

    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-token-perms", test_token_perms_run)
