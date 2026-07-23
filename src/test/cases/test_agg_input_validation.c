/* src/test/cases/test_agg_input_validation.c
 * Regression: unknown aggregate function must error (not silently become
 * count).  Invalid order_by alias must error (not sort by 0.0).
 * Uppercase "DESC" must work (not silently become ascending).
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

static int test_agg_input_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"agg_val_t\","
        "\"fields\":[\"tag:varchar:16\",\"score:int\"],"
        "\"indexes\":[\"tag\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"agg_val_t\",\"records\":{"
        "\"k1\":{\"tag\":\"a\",\"score\":10},"
        "\"k2\":{\"tag\":\"a\",\"score\":20},"
        "\"k3\":{\"tag\":\"b\",\"score\":50}"
        "}}", &resp); free(resp); resp = NULL;

    /* 1. Unknown agg function returns error, not count */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"agg_val_t\","
        "\"aggregates\":[{\"fn\":\"median\",\"field\":\"score\",\"alias\":\"m\"}],"
        "\"group_by\":[\"tag\"]}",
        &resp);
    ASSERT_NOT_NULL(resp, "unknown fn response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "unknown agg fn returns error");
    free(resp); resp = NULL;

    /* 2. Invalid order_by alias returns error */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"agg_val_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"tag\"],\"order_by\":\"nonexistent\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "invalid order_by response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "invalid order_by returns error");
    free(resp); resp = NULL;

    /* 3. Uppercase "DESC" works */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"agg_val_t\","
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"score\",\"alias\":\"s\"}],"
        "\"group_by\":[\"tag\"],\"order_by\":\"s\",\"order\":\"DESC\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "DESC uppercase response not null");
    /* b (score=50) should come first, a (score=30 total) second */
    const char *b_pos = SAFE_STRSTR(resp, "\"tag\":\"b\"");
    const char *a_pos = SAFE_STRSTR(resp, "\"tag\":\"a\"");
    ASSERT_NOT_NULL(b_pos, "DESC: b present");
    ASSERT_NOT_NULL(a_pos, "DESC: a present");
    ASSERT_TRUE(b_pos < a_pos, "DESC: b appears before a (descending)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-input-validation", test_agg_input_validation_run)
