/* src/test/audit/audit_repro_token_chain.c
 *
 * Audit repro for AUD-AUTH-2 (docs/plans/2026-11-25-audit-dispatch-auth.md).
 * The token store is an open-addressed hash table whose remove path clears
 * the slot without a tombstone, so removing one token breaks the probe chain
 * of every colliding token inserted after it: that token stops
 * authenticating, remove-token reports token_not_found, and it stays in
 * tokens.conf (and works again after a restart). Same pattern in the IP set.
 *
 * Regression case for docs/plans/2026-11-25-fix-auth-sets.md: removing token
 * A leaves colliding token B valid, B can then be revoked and stays revoked
 * on disk, tokens.conf is 0600, and a full table reports an error. Runs in
 * the default suite.
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static unsigned tc_h(const char *s) {
    unsigned h = 5381;
    while (*s) h = h * 33 + (unsigned char)*s++;
    return h;
}

static char *ta_req(int port, const char *line) {
    TestClientCfg cfg = { .port = port, .io_timeout_ms = 10000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return NULL;
    char *resp = NULL;
    if (tc_request(tc, line, &resp) != 0) resp = NULL;
    tc_close(tc);
    return resp;
}

static int audit_repro_token_chain_run(void) {
    char base[256], db_root[256];
    snprintf(base, sizeof(base), "/tmp/shard-db-ta-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);
    int port = test_pick_port();
    ASSERT_TRUE(port >= 0, "pick port");
    if (port < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    char gtok[64];
    snprintf(gtok, sizeof(gtok), "sdb_ta_admin_%d", (int)time(NULL));
    char p[320];
    snprintf(p, sizeof(p), "%s/dirs.conf", db_root);
    FILE *f = fopen(p, "w"); if (f) { fputs("default\n", f); fclose(f); }
    snprintf(p, sizeof(p), "%s/tokens.conf", db_root);
    f = fopen(p, "w"); if (f) { fprintf(f, "%s\n", gtok); fclose(f); }
    tu_run_cmd("touch %s/allowed_ips.conf; mkdir -p %s/logs", db_root, base);
    snprintf(p, sizeof(p), "%s/db.env", base);
    f = fopen(p, "w");
    if (!f) { ASSERT_TRUE(0, "db.env"); tu_run_cmd("rm -rf %s", base); return 1; }
    fprintf(f,
        "export DB_ROOT=\"%s\"\nexport PORT=%d\nexport TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\nexport LOG_LEVEL=2\nexport THREADS=2\n"
        "export FCACHE_MAX=4096\nexport TLS_ENABLE=0\n"
        "export DISABLE_LOCALHOST_TRUST=1\nexport TOKEN_CAP=64\n", db_root, port, base);
    fclose(f);

    char sdb[PATH_MAX];
    const char *rel = "./build/bin/shard-db";
    if (access(rel, X_OK) != 0) rel = "./shard-db";
    if (!realpath(rel, sdb)) { ASSERT_TRUE(0, "shard-db"); tu_run_cmd("rm -rf %s", base); return 1; }
    pid_t pid = fork();
    if (pid == 0) { if (chdir(base) != 0) _exit(126); execl(sdb, sdb, "server", (char *)NULL); _exit(127); }
    if (pid < 0) { ASSERT_TRUE(0, "fork"); tu_run_cmd("rm -rf %s", base); return 1; }

    char req[1024];
    snprintf(req, sizeof(req), "{\"mode\":\"db-dirs\",\"auth\":\"%s\"}", gtok);
    int ready = 0;
    for (int i = 0; i < 100 && !ready; i++) {
        char *r = ta_req(port, req);
        if (r && !strstr(r, "\"error\"")) ready = 1;
        free(r);
        if (!ready) { struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL); }
    }
    ASSERT_TRUE(ready, "daemon ready");
    if (ready) {
        /* Two tokens in the same bucket (djb2 % TOKEN_CAP), neither in the
           admin token's bucket. */
        char t1[32] = "", t2[32] = "";
        unsigned adm = tc_h(gtok) % 64;
        unsigned first = 0; int have = 0;
        for (int i = 0; i < 100000 && !t2[0]; i++) {
            char t[32]; snprintf(t, sizeof(t), "tok%d", i);
            unsigned b = tc_h(t) % 64;
            if (b == adm) continue;
            if (!have) { snprintf(t1, sizeof(t1), "%s", t); first = b; have = 1; }
            else if (b == first) snprintf(t2, sizeof(t2), "%s", t);
        }
        ASSERT_TRUE(t1[0] && t2[0], "found two colliding tokens");
        char *r;
        snprintf(req, sizeof(req), "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"%s\",\"perm\":\"rwx\"}", gtok, t1);
        free(ta_req(port, req));
        snprintf(req, sizeof(req), "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"%s\",\"perm\":\"rwx\"}", gtok, t2);
        free(ta_req(port, req));
        snprintf(req, sizeof(req), "{\"mode\":\"remove-token\",\"auth\":\"%s\",\"token\":\"%s\"}", gtok, t1);
        free(ta_req(port, req));
        snprintf(req, sizeof(req), "{\"mode\":\"db-dirs\",\"auth\":\"%s\"}", t2);
        r = ta_req(port, req);
        ASSERT_TRUE(r && !strstr(r, "\"error\""),
                    "colliding token still authenticates after another token was removed");
        free(r);
        snprintf(req, sizeof(req), "{\"mode\":\"remove-token\",\"auth\":\"%s\",\"token\":\"%s\"}", gtok, t2);
        r = ta_req(port, req);
        ASSERT_TRUE(r && strstr(r, "token_removed"),
                    "the colliding token can be revoked");
        free(r);

        /* The revoked token is gone from disk, and the file is 0600. */
        char tpath[512];
        snprintf(tpath, sizeof(tpath), "%s/tokens.conf", db_root);
        FILE *tf = fopen(tpath, "r");
        char content[4096] = "";
        if (tf) { size_t n = fread(content, 1, sizeof(content) - 1, tf); content[n] = '\0'; fclose(tf); }
        ASSERT_TRUE(tf && !strstr(content, t2) && !strstr(content, t1) && strstr(content, gtok),
                    "tokens.conf no longer lists the removed tokens");
        struct stat tst;
        ASSERT_TRUE(stat(tpath, &tst) == 0 && (tst.st_mode & 0777) == 0600,
                    "tokens.conf is mode 0600");

        /* A full table is an error, not a silent drop. */
        int ok = 0, full = 0;
        for (int i = 0; i < 80; i++) {
            snprintf(req, sizeof(req),
                "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"fill%d\",\"perm\":\"r\"}", gtok, i);
            r = ta_req(port, req);
            if (r && strstr(r, "token_added")) ok++;
            else if (r && strstr(r, "token store full")) full++;
            free(r);
        }
        ASSERT_TRUE(ok > 0 && ok <= 64 && full > 0, "a full token table reports an error");
    }
    kill(pid, SIGTERM);
    waitpid(pid, NULL, 0);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-token-store-chain", audit_repro_token_chain_run)
