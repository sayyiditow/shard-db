/* src/test/audit/audit_repro_idle_connections.c
 *
 * Audit repro for AUD-CONN-1 (docs/plans/2026-11-25-audit-connection-
 * lifecycle.md). Each accepted connection occupies one worker thread for its
 * whole lifetime, there is no socket or idle timeout, and accept needs no
 * authentication, so WORKERS idle connections (default: CPU count) stop the
 * server from answering anyone.
 *
 * Regression case for docs/plans/2026-11-25-fix-conn-timeouts.md: with
 * IDLE_TIMEOUT_SEC=2, WORKERS idle unauthenticated connections are closed and
 * an authenticated request is then answered; a connection idle past the
 * timeout is dropped. Runs in the default suite.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static int audit_repro_idle_connections_run(void) {
    char base[] = "/tmp/shard-db-ta-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char db_root[256];
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    ASSERT_EQ_INT(mkdir(db_root, 0755), 0, "mkdir db root");
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
        "export DISABLE_LOCALHOST_TRUST=1\nexport WORKERS=4\nexport IDLE_TIMEOUT_SEC=2\n", db_root, port, base);
    fclose(f);

    TestEnv env = {0};
    ASSERT_EQ_INT(test_env_start_at(&env, db_root, port), 0, "start the daemon");
    if (env.daemon_pid <= 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    char req[1024];
    snprintf(req, sizeof(req), "{\"mode\":\"db-dirs\",\"auth\":\"%s\"}", gtok);
    {
        TestClient *idle[4];
        TestClientCfg icfg = { .port = port, .connect_timeout_ms = 2000 };
        for (int i = 0; i < 4; i++) idle[i] = tc_connect(&icfg);   /* send nothing */
        /* No settle sleep: accept order is FIFO, so the four idle
           connections reach the four workers first. */
        TestClientCfg cfg = { .port = port, .connect_timeout_ms = 2000, .io_timeout_ms = 6000 };
        TestClient *tc = tc_connect(&cfg);
        char *r = NULL;
        if (tc) { if (tc_request(tc, req, &r) != 0) r = NULL; tc_close(tc); }
        ASSERT_TRUE(r && strstr(r, "default"),
                    "an authenticated request is answered while 4 idle connections are open");
        free(r);
        for (int i = 0; i < 4; i++) if (idle[i]) tc_close(idle[i]);

        /* A connection idle past the timeout is closed by the server, not by
           the client deadline: the recv fails well inside the 6 s budget. */
        TestClient *late = tc_connect(&cfg);
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        char *r2 = NULL;
        int rc2 = late ? tc_recv(late, &r2) : -1;
        clock_gettime(CLOCK_MONOTONIC, &t1);
        long elapsed_ms = (t1.tv_sec - t0.tv_sec) * 1000L
                        + (t1.tv_nsec - t0.tv_nsec) / 1000000L;
        ASSERT_TRUE(rc2 != 0 || !r2, "a connection idle past IDLE_TIMEOUT_SEC is closed");
        ASSERT_TRUE(elapsed_ms < 5000,
                    "the server closed it, not the 6000 ms client timeout");
        free(r2);
        if (late) tc_close(late);
        /* ...and the server still serves new connections. */
        TestClient *fresh = tc_connect(&cfg);
        char *r3 = NULL;
        if (fresh) { if (tc_request(fresh, req, &r3) != 0) r3 = NULL; tc_close(fresh); }
        ASSERT_TRUE(r3 && strstr(r3, "default"), "a new connection is served afterwards");
        free(r3);
    }
    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-idle-connection-timeout", audit_repro_idle_connections_run)
