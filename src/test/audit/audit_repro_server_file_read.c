/* src/test/audit/audit_repro_server_file_read.c
 *
 * Audit repro for AUD-FILE-1 (docs/plans/2026-11-25-audit-caps-and-files.md).
 * bulk-insert, bulk-insert-delimited, bulk-delete, bulk-update(-delimited)
 * take a "file" parameter and put-file takes a "path": a path on the SERVER's
 * filesystem, opened as the daemon user, for any token with write permission
 * (an object-scoped "rw" token is enough). Ingesting the global tokens.conf
 * with delimiter ':' puts the admin token into a record key the low token can
 * read back with "keys".
 *
 * Contract asserted: a low-privilege token cannot make the server read an
 * arbitrary local file. Regression case (fixed; formerly failed). Passes once the Phase 4 fix
 * lands. Audit case: excluded from default run-all (--include-audit).
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

static char *ta_req(int port, const char *line) {
    TestClientCfg cfg = { .port = port, .io_timeout_ms = 10000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return NULL;
    char *resp = NULL;
    if (tc_request(tc, line, &resp) != 0) resp = NULL;
    tc_close(tc);
    return resp;
}

static int audit_repro_server_file_read_run(void) {
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
    f = fopen(p, "w"); if (f) { fprintf(f, "%s:rwx\n", gtok); fclose(f); }
    tu_run_cmd("touch %s/allowed_ips.conf; mkdir -p %s/logs", db_root, base);
    snprintf(p, sizeof(p), "%s/db.env", base);
    f = fopen(p, "w");
    if (!f) { ASSERT_TRUE(0, "db.env"); tu_run_cmd("rm -rf %s", base); return 1; }
    fprintf(f,
        "export DB_ROOT=\"%s\"\nexport PORT=%d\nexport TIMEOUT=0\n"
        "export LOG_DIR=\"%s/logs\"\nexport LOG_LEVEL=2\nexport THREADS=2\n"
        "export FCACHE_MAX=4096\nexport TLS_ENABLE=0\n"
        "export DISABLE_LOCALHOST_TRUST=1\n", db_root, port, base);
    fclose(f);

    TestEnv env = {0};
    ASSERT_EQ_INT(test_env_start_at(&env, db_root, port), 0, "start the daemon");
    if (env.daemon_pid <= 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    char req[1024];
    snprintf(req, sizeof(req), "{\"mode\":\"db-dirs\",\"auth\":\"%s\"}", gtok);
    {
        char *r;
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"t1\","
            "\"auth\":\"%s\",\"splits\":8,\"max_key\":64,\"fields\":[\"perm:varchar:16\"]}", gtok);
        free(ta_req(port, req));
        snprintf(req, sizeof(req),
            "{\"mode\":\"add-token\",\"auth\":\"%s\",\"token\":\"lowrw\","
            "\"dir\":\"default\",\"object\":\"t1\",\"perm\":\"rw\"}", gtok);
        free(ta_req(port, req));
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert-delimited\",\"auth\":\"lowrw\",\"dir\":\"default\","
            "\"object\":\"t1\",\"file\":\"%s/tokens.conf\",\"delimiter\":\":\"}", db_root);
        free(ta_req(port, req));
        r = ta_req(port,
            "{\"mode\":\"keys\",\"auth\":\"lowrw\",\"dir\":\"default\",\"object\":\"t1\"}");
        ASSERT_TRUE(r && !strstr(r, gtok),
                    "a low-privilege token must not read the server's tokens.conf through a file ingest");
        free(r);
    }
    test_env_stop_keep(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-server-file-paths-admin-only", audit_repro_server_file_read_run)
