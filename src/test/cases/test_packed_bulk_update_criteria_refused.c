/* Packed criteria bulk-update is a pre-existing product gap
 * (AUD-PACKED-BULKUPDATE): query_bulk.c::cmd_bulk_update refuses every packed
 * object before validation. This pins the refusal and proves it writes
 * nothing. A later feature plan flips this test.
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

static char *req(TestClient *tc, const char *json) {
    char *resp = NULL;
    tc_request(tc, json, &resp);
    return resp;
}

static int test_packed_bulk_update_criteria_refused_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = req(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    free(resp); resp = NULL;
    resp = req(tc,
        "{\"mode\":\"create-object\",\"declaration_layout\":\"packed\",\"dir\":\"default\",\"object\":\"pbu\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"status:varchar:16\",\"amount:int\"]}");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "packed object created");
    free(resp); resp = NULL;
    resp = req(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"pbu\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":10}},"
                     "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":20}}]}");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"inserted\":2") != NULL, "two records seeded");
    free(resp); resp = NULL;

    /* Snapshot state before the refused requests. */
    char *get_before = req(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"pbu\",\"keys\":[\"k1\",\"k2\"]}");
    char *count_before = req(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"pbu\"}");
    ASSERT_NOT_NULL(get_before, "get before");
    ASSERT_NOT_NULL(count_before, "count before");

    resp = req(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"pbu\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"value\":{\"status\":\"done\"}}");
    ASSERT_CONTAINS(resp,
                    "{\"error\":\"packed criteria updates await query integration\"}",
                    "criteria bulk-update refused");
    free(resp); resp = NULL;
    resp = req(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"pbu\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"value\":{\"status\":\"done\"},\"dry_run\":true}");
    ASSERT_CONTAINS(resp,
                    "{\"error\":\"packed criteria updates await query integration\"}",
                    "dry-run criteria bulk-update refused");
    free(resp); resp = NULL;

    char *get_after = req(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"pbu\",\"keys\":[\"k1\",\"k2\"]}");
    char *count_after = req(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"pbu\"}");
    ASSERT_NOT_NULL(get_after, "get after");
    ASSERT_NOT_NULL(count_after, "count after");
    ASSERT_EQ_STR(get_after, get_before, "records identical before/after");
    ASSERT_EQ_STR(count_after, count_before, "count identical before/after");

    free(get_before); free(get_after);
    free(count_before); free(count_after);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-packed-bulk-update-criteria-refused",
              test_packed_bulk_update_criteria_refused_run)
