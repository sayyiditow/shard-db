/* src/test/cases/test_agg_having_json_and.c
 * Regression: flat-array (implicit AND) having still works correctly
 * once JSON having is routed through the tree-aware evaluator.
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

static int test_agg_having_json_and_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"having_json_and_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\"],"
        "\"indexes\":[\"status\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"having_json_and_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":20}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"paid\",\"amount\":30}},"
        "{\"key\":\"k4\",\"value\":{\"status\":\"paid\",\"amount\":40}},"
        "{\"key\":\"k5\",\"value\":{\"status\":\"paid\",\"amount\":50}},"
        "{\"key\":\"k6\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k7\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k8\",\"value\":{\"status\":\"cancelled\",\"amount\":200}}"
        "]}", &resp); free(resp); resp = NULL;

    /* Flat array = implicit AND: n>1 and s>50.
       paid: n=5>1 true, s=150>50 true -> kept.
       pending: n=2>1 true, s=10>50 false -> dropped.
       cancelled: n=1>1 false -> dropped. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"having_json_and_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"s\"}],"
        "\"group_by\":[\"status\"],"
        "\"having\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"1\"},"
                    "{\"field\":\"s\",\"op\":\"gt\",\"value\":\"50\"}]}",
        &resp);
    ASSERT_TRUE(resp != NULL, "got response");
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"", "AND keeps paid (both conditions true)");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"status\":\"pending\"") == NULL,
                "AND drops pending (sum condition fails)");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"status\":\"cancelled\"") == NULL,
                "AND drops cancelled (count condition fails)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-having-json-and", test_agg_having_json_and_run)
