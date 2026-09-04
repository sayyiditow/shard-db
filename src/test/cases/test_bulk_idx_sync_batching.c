/* Red on base: bulk insert/update/delete on an indexed object leave
   commit.index_sync_ops_total == 0 (per-record syncs bypass the flush
   seam). After Task 2 of docs/plans/2026-09-04-bulk-commit-throughput-
   and-durability.md the same workload records unique (field, shard)
   touches: ops > 0 and bounded by windows x fields x idx-shards, while
   index contents still converge (old values gone, new values found). */
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

/* Extract a JSON integer that follows `"key":` in resp. Returns 0 on miss. */
static long json_int_after(const char *resp, const char *key) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":", key);
    const char *p = strstr(resp, pat);
    if (!p) return 0;
    return strtol(p + strlen(pat), NULL, 10);
}

static int test_bulk_idx_sync_batching_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bat\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":["
        "\"status:varchar:16\","
        "\"note:varchar:64\""
        "],\"indexes\":[]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"bat\","
        "\"fields\":[\"status\"]}", &resp);
    free(resp); resp = NULL;

    /* 32 fresh inserts through the indexed bulk-insert window. */
    {
        char *req = malloc(8192); size_t p = 0;
        p += (size_t)snprintf(req + p, 8192 - p,
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bat\","
            "\"records\":[");
        for (int i = 0; i < 32; i++)
            p += (size_t)snprintf(req + p, 8192 - p,
                "%s{\"key\":\"K-%02d\",\"value\":{\"status\":\"S%d\",\"note\":\"n%d\"}}",
                i ? "," : "", i, i % 4, i);
        snprintf(req + p, 8192 - p, "]}");
        tc_request(tc, req, &resp);
        expect(resp && !strstr(resp, "\"error\""), "indexed bulk insert ok");
        free(resp); resp = NULL; free(req);
    }

    /* 32 updates that change the indexed field (per-record syncs today). */
    {
        char *req = malloc(8192); size_t p = 0;
        p += (size_t)snprintf(req + p, 8192 - p,
            "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"bat\","
            "\"records\":[");
        for (int i = 0; i < 32; i++)
            p += (size_t)snprintf(req + p, 8192 - p,
                "%s{\"key\":\"K-%02d\",\"value\":{\"status\":\"T%d\"}}",
                i ? "," : "", i, i % 3);
        snprintf(req + p, 8192 - p, "]}");
        tc_request(tc, req, &resp);
        expect(resp && !strstr(resp, "\"error\""), "indexed bulk update ok");
        free(resp); resp = NULL; free(req);
    }

    /* 16 deletes through the indexed bulk-delete window. */
    {
        char *req = malloc(2048); size_t p = 0;
        p += (size_t)snprintf(req + p, 2048 - p,
            "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"bat\","
            "\"keys\":[");
        for (int i = 0; i < 16; i++)
            p += (size_t)snprintf(req + p, 2048 - p, "%s\"K-%02d\"",
                                  i ? "," : "", i);
        snprintf(req + p, 2048 - p, "]}");
        tc_request(tc, req, &resp);
        expect(resp && !strstr(resp, "\"error\""), "indexed bulk delete ok");
        free(resp); resp = NULL; free(req);
    }

    /* Index contents converged: old values gone, new values found. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bat\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"S0\"}],"
        "\"limit\":100}", &resp);
    expect(resp && !strstr(resp, "\"error\""), "find old value ok");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bat\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"T1\"}],"
        "\"limit\":100}", &resp);
    expect(resp && strstr(resp, "K-"), "find new value hits");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"stats\"}", &resp);
    long windows = json_int_after(resp, "windows_total");
    long ops     = json_int_after(resp, "index_sync_ops_total");
    free(resp); resp = NULL;

    expect(windows > 0, "windows_total > 0 (Task 1 present)");
    expect(ops > 0, "index_sync_ops_total > 0 (RED on base)");
    /* Bound: <= windows x 1 field x index_splits_for(8)=2 shards per window
       (plus slack). Anything near the old per-record count (>= 48 syncs)
       fails this. */
    expect(ops <= windows * 2 + 8, "ops bounded by touched files, not records");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-bulk-idx-sync-batching", test_bulk_idx_sync_batching_run)
