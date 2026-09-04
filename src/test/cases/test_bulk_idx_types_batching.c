/* Bitmap and trigram bulk windows (Task 2 of docs/plans/2026-09-04-bulk-
   commit-throughput-and-durability.md). The bitmap assertion is made on a
   BITMAP-ONLY object, before any trigram index exists: bitmap recording
   must happen inside apply_window (where the TLS collector is installed) —
   a prepare-time recording is a silent no-op (the review finding this
   guards: the earlier version recorded in prepare and the mixed bitmap+
   trigram object masked it, because trigram alone satisfied ops > 0). */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void expect(int cond, const char *what) {
    ASSERT_TRUE(cond, what);
}

static long json_int_after(const char *resp, const char *key) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":", key);
    const char *p = strstr(resp, pat);
    if (!p) return 0;
    return strtol(p + strlen(pat), NULL, 10);
}

static int test_bulk_idx_types_batching_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Phase 1: BITMAP-ONLY object. Index entries are strings — bare name =
       btree, name:trigram, name:bitmap (same mini-syntax as index.conf). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bix\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:varchar:8\"],"
        "\"indexes\":[\"flag:bitmap\"]}", &resp);
    expect(resp && !strstr(resp, "\"error\""), "create bitmap-only object ok");
    free(resp); resp = NULL;

    char *req = malloc(16384); size_t p = 0;
    p += (size_t)snprintf(req + p, 16384 - p,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bix\","
        "\"records\":[");
    for (int i = 0; i < 32; i++)
        p += (size_t)snprintf(req + p, 16384 - p,
            "%s{\"key\":\"B-%02d\",\"value\":{\"flag\":\"f%d\"}}",
            i ? "," : "", i, i % 2);
    snprintf(req + p, 16384 - p, "]}");
    tc_request(tc, req, &resp);
    expect(resp && !strstr(resp, "\"error\""), "bitmap bulk insert ok");
    free(resp); resp = NULL; free(req);

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bix\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"f1\"}],"
        "\"limit\":100}", &resp);
    expect(resp && strstr(resp, "B-"), "bitmap find hits");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"stats\"}", &resp);
    long ops = json_int_after(resp, "index_sync_ops_total");
    free(resp); resp = NULL;
    /* No other index type exists yet — this can only be > 0 if the bitmap
       window recorded and flushed its touches (RED on the prepare-time
       no-op bug; also RED on base's dead flush seam). */
    expect(ops > 0, "bitmap-only touches flushed (RED on prepare-time no-op)");

    /* Phase 2: TRIGRAM-ONLY object — window applies and the index resolves. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"tix\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"desc:varchar:128\"],"
        "\"indexes\":[\"desc:trigram\"]}", &resp);
    expect(resp && !strstr(resp, "\"error\""), "create trigram object ok");
    free(resp); resp = NULL;

    req = malloc(16384); p = 0;
    p += (size_t)snprintf(req + p, 16384 - p,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"tix\","
        "\"records\":[");
    for (int i = 0; i < 32; i++)
        p += (size_t)snprintf(req + p, 16384 - p,
            "%s{\"key\":\"T-%02d\",\"value\":{\"desc\":\"alpha beta gamma %d\"}}",
            i ? "," : "", i, i);
    snprintf(req + p, 16384 - p, "]}");
    tc_request(tc, req, &resp);
    expect(resp && !strstr(resp, "\"error\""), "trigram bulk insert ok");
    free(resp); resp = NULL; free(req);

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"tix\","
        "\"criteria\":[{\"field\":\"desc\",\"op\":\"contains\","
        "\"value\":\"beta\"}],\"limit\":100}", &resp);
    expect(resp && strstr(resp, "T-"), "trigram contains hits");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-bulk-idx-types-batching", test_bulk_idx_types_batching_run)
