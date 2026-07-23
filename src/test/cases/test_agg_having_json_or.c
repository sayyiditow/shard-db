/* src/test/cases/test_agg_having_json_or.c
 * Aggregate having OR support via the JSON protocol (having as {"or":[...]}).
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

static int test_agg_having_json_or_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"having_json_or_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\"],"
        "\"indexes\":[\"status\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"having_json_or_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":20}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"paid\",\"amount\":30}},"
        "{\"key\":\"k4\",\"value\":{\"status\":\"paid\",\"amount\":40}},"
        "{\"key\":\"k5\",\"value\":{\"status\":\"paid\",\"amount\":50}},"
        "{\"key\":\"k6\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k7\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k8\",\"value\":{\"status\":\"cancelled\",\"amount\":200}}"
        "]}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"having_json_or_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"s\"}],"
        "\"group_by\":[\"status\"],"
        "\"having\":{\"or\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"2\"},"
                            "{\"field\":\"s\",\"op\":\"gt\",\"value\":\"100\"}]}}",
        &resp);
    ASSERT_TRUE(resp != NULL, "got response");
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"", "JSON OR keeps paid (n=5>2)");
    ASSERT_CONTAINS(resp, "\"status\":\"cancelled\"", "JSON OR keeps cancelled (s=200>100)");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"status\":\"pending\"") == NULL,
                "JSON OR drops pending (neither branch matches)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-having-json-or", test_agg_having_json_or_run)
