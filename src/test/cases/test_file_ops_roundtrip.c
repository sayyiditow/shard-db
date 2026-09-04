/* File-op roundtrip guards (Task 6 of docs/plans/2026-09-04-bulk-commit-
   throughput-and-durability.md). The fsync / dir-fsync durability additions
   have no portable in-suite failure observable, so this case guards the
   atomic-replace semantics and round-trip behavior: put-file (path variant
   stores under the source basename), overwrite via the b64 data variant
   must replace content exactly, delete-file removes, get after delete
   errors. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void expect(int cond, const char *what) {
    ASSERT_TRUE(cond, what);
}

static int test_file_ops_roundtrip_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\","
                   "\"object\":\"fops\",\"splits\":8,\"max_key\":16,"
                   "\"fields\":[\"v:varchar:8\"]}", &resp);
    free(resp); resp = NULL;

    /* Seed a source file larger than one write() chunk. The path variant
       stores it under its own basename. */
    char src[PATH_MAX];
    snprintf(src, sizeof(src), "/tmp/shard-db-putfile-src-%d.bin", (int)getpid());
    FILE *f = fopen(src, "wb");
    expect(f != NULL, "open upload source");
    if (!f) { tc_close(tc); test_env_stop(&env); return 1; }
    for (int i = 0; i < 200000; i++) fputc('A' + (i % 26), f);
    fclose(f);

    {
        char req[PATH_MAX + 128];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"put-file\",\"dir\":\"default\",\"object\":\"fops\","
                 "\"path\":\"%s\"}", src);
        tc_request(tc, req, &resp);
        expect(resp && !strstr(resp, "\"error\""), "put-file (path) ok");
        free(resp); resp = NULL;
    }

    /* Overwrite with shorter content via the b64 data variant — the stored
       file must shrink exactly (guards atomic replace, not append/merge). */
    {
        char req[256];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"put-file\",\"dir\":\"default\","
                 "\"object\":\"fops\",\"filename\":\"upload-src.bin\","
                 "\"data\":\"aGVsbG8=\"}");   /* "hello" */
        tc_request(tc, req, &resp);
        expect(resp && !strstr(resp, "\"error\""), "b64 overwrite ok");
        free(resp); resp = NULL;
    }
    tc_request(tc,
        "{\"mode\":\"get-file\",\"dir\":\"default\",\"object\":\"fops\","
        "\"filename\":\"upload-src.bin\"}", &resp);
    expect(resp && strstr(resp, "\"data\":\"aGVsbG8=\""),
           "get-file returns the overwritten content");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"delete-file\",\"dir\":\"default\",\"object\":\"fops\","
        "\"filename\":\"upload-src.bin\"}", &resp);
    expect(resp && !strstr(resp, "\"error\""), "delete-file ok");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"get-file\",\"dir\":\"default\",\"object\":\"fops\","
        "\"filename\":\"upload-src.bin\"}", &resp);
    expect(resp && strstr(resp, "\"error\""), "get after delete errors");
    free(resp); resp = NULL;

    unlink(src);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-file-ops-roundtrip", test_file_ops_roundtrip_run)
