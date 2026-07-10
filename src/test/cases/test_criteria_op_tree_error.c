/* src/test/cases/test_criteria_op_tree_error.c
 * Regression: criteria tree parser ({"or":[...]} / {"and":[...]}) must
 * also reject unknown operators and accept the "operator" alias.
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

static int test_criteria_op_tree_error_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"op_tree_t\","
        "\"fields\":[\"name:varchar:32\",\"score:int\"],"
        "\"indexes\":[\"score\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"op_tree_t\",\"records\":{"
        "\"k1\":{\"name\":\"low\",\"score\":10},"
        "\"k2\":{\"name\":\"high\",\"score\":90}"
        "}}", &resp); free(resp); resp = NULL;

    /* 1. OR tree with "operator" alias — must work */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"op_tree_t\","
        "\"criteria\":[{\"or\":["
        "{\"field\":\"score\",\"operator\":\"gt\",\"value\":50},"
        "{\"field\":\"name\",\"operator\":\"eq\",\"value\":\"low\"}"
        "]}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "OR tree with operator alias response not null");
    ASSERT_CONTAINS(resp, "\"name\":\"high\"", "OR tree: high matches score>50");
    ASSERT_CONTAINS(resp, "\"name\":\"low\"", "OR tree: low matches name eq");
    free(resp); resp = NULL;

    /* 2. OR tree with unrecognised op — must return error */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"op_tree_t\","
        "\"criteria\":[{\"or\":["
        "{\"field\":\"score\",\"op\":\"bogus\",\"value\":50}"
        "]}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "OR tree with bogus op response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "OR tree: bogus op returns error");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-criteria-op-tree-error", test_criteria_op_tree_error_run)
