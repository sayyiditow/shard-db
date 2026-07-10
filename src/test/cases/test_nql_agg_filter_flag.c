/* src/test/cases/test_nql_agg_filter_flag.c
 * Regression: NQL aggregate --filter flag must work for filters that
 * contain parentheses (IN, BETWEEN).  Positional filter heuristic must
 * still work for simple filters (backward compat).
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

static int test_nql_agg_filter_flag_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"agg_ff_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\"],"
        "\"indexes\":[\"status\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    /* 5 paid rows (amounts 10,20,30,40,50 = sum 150), 2 pending (5,5 = sum 10),
       1 cancelled (200). */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"agg_ff_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":20}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"paid\",\"amount\":30}},"
        "{\"key\":\"k4\",\"value\":{\"status\":\"paid\",\"amount\":40}},"
        "{\"key\":\"k5\",\"value\":{\"status\":\"paid\",\"amount\":50}},"
        "{\"key\":\"k6\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k7\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k8\",\"value\":{\"status\":\"cancelled\",\"amount\":200}}"
        "]}", &resp); free(resp); resp = NULL;

    /* 1. --filter with IN (parens) — must work, not misclassify */
    tc_request(tc,
        "aggregate default agg_ff_t --filter 'status in (paid, cancelled)' "
        "sum(amount) --group-by status",
        &resp);
    ASSERT_NOT_NULL(resp, "--filter with IN response not null");
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"", "--filter IN: paid present");
    ASSERT_CONTAINS(resp, "\"status\":\"cancelled\"", "--filter IN: cancelled present");
    ASSERT_TRUE(strstr(resp, "\"status\":\"pending\"") == NULL,
                "--filter IN: pending excluded");
    free(resp); resp = NULL;

    /* 2. Positional filter (backward compat) — simple filter still works */
    tc_request(tc,
        "aggregate default agg_ff_t 'status eq paid' "
        "sum(amount)",
        &resp);
    ASSERT_NOT_NULL(resp, "positional filter response not null");
    ASSERT_CONTAINS(resp, "\"sum_amount\":150", "positional filter: paid sum=150");
    free(resp); resp = NULL;

    /* 3. --filter overrides positional (when both present, --filter wins) */
    tc_request(tc,
        "aggregate default agg_ff_t 'status eq paid' "
        "--filter 'status eq pending' "
        "sum(amount)",
        &resp);
    ASSERT_NOT_NULL(resp, "--filter override response not null");
    ASSERT_CONTAINS(resp, "\"sum_amount\":10", "--filter override: pending sum=10");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-agg-filter-flag", test_nql_agg_filter_flag_run)
