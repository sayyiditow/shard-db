/* src/test/audit/audit_repro_idle_connections.c
 *
 * Audit repro for AUD-CONN-1 (docs/plans/2026-11-25-audit-connection-
 * lifecycle.md). Each accepted connection occupies one worker thread for its
 * whole lifetime, there is no socket or idle timeout, and accept needs no
 * authentication, so WORKERS idle connections (default: CPU count) stop the
 * server from answering anyone.
 *
 * Contract asserted: with WORKERS idle unauthenticated connections open, an
 * authenticated request is still answered within a few seconds. Expected
 * today: FAILS (the request times out). Passes once the Phase 4 fix lands
 * (idle/read timeouts or a connection cap that never starves workers).
 * Audit case: excluded from default run-all (--include-audit).
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

static char *ta_req(int port, const char *line) {
    TestClientCfg cfg = { .port = port, .io_timeout_ms = 10000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return NULL;
    char *resp = NULL;
    if (tc_request(tc, line, &resp) != 0) resp = NULL;
    tc_close(tc);
    return resp;
}

static int audit_repro_idle_connections_run(void) {
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
        "export DISABLE_LOCALHOST_TRUST=1\nexport WORKERS=4\n", db_root, port, base);
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
        TestClient *idle[4];
        TestClientCfg icfg = { .port = port, .connect_timeout_ms = 2000 };
        for (int i = 0; i < 4; i++) idle[i] = tc_connect(&icfg);   /* send nothing */
        { struct timespec ts = { 1, 0 }; nanosleep(&ts, NULL); }
        TestClientCfg cfg = { .port = port, .connect_timeout_ms = 2000, .io_timeout_ms = 3000 };
        TestClient *tc = tc_connect(&cfg);
        char *r = NULL;
        if (tc) { if (tc_request(tc, req, &r) != 0) r = NULL; tc_close(tc); }
        ASSERT_TRUE(r && strstr(r, "default"),
                    "an authenticated request is answered while 4 idle connections are open");
        free(r);
        for (int i = 0; i < 4; i++) if (idle[i]) tc_close(idle[i]);
    }
    kill(pid, SIGTERM);
    waitpid(pid, NULL, 0);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER_AUDIT("audit-repro-idle-connections", audit_repro_idle_connections_run)
