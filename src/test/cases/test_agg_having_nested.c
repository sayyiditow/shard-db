/* src/test/cases/test_agg_having_nested.c
 * Aggregate --having nested AND-within-OR via the NQL raw-text protocol.
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

static int test_agg_having_nested_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"having_nested_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\"],"
        "\"indexes\":[\"status\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    /* paid: count=5 sum=150 avg=30
       pending: count=1 sum=5 avg=5
       cancelled: count=3 sum=90 avg=30
       refunded: count=1 sum=1000 avg=1000 */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"having_nested_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":20}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"paid\",\"amount\":30}},"
        "{\"key\":\"k4\",\"value\":{\"status\":\"paid\",\"amount\":40}},"
        "{\"key\":\"k5\",\"value\":{\"status\":\"paid\",\"amount\":50}},"
        "{\"key\":\"k6\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k7\",\"value\":{\"status\":\"cancelled\",\"amount\":20}},"
        "{\"key\":\"k8\",\"value\":{\"status\":\"cancelled\",\"amount\":30}},"
        "{\"key\":\"k9\",\"value\":{\"status\":\"cancelled\",\"amount\":40}},"
        "{\"key\":\"k10\",\"value\":{\"status\":\"refunded\",\"amount\":1000}}"
        "]}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "aggregate default having_nested_t count(),sum(amount),avg(amount) --group-by status "
        "--having '(count gt 2 and sum_amount gt 100) or avg_amount lt 20'",
        &resp);
    ASSERT_TRUE(resp != NULL, "got response");
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"",
                     "nested: paid kept via (count>2 and sum>100)");
    ASSERT_CONTAINS(resp, "\"status\":\"pending\"",
                     "nested: pending kept via (avg<20)");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"status\":\"cancelled\"") == NULL,
                "nested: cancelled dropped (count>2 but sum not>100, avg not<20)");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"status\":\"refunded\"") == NULL,
                "nested: refunded dropped (count not>2, avg not<20)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-having-nested", test_agg_having_nested_run)
