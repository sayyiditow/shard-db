/* src/test/cases/test_criteria_operator_alias.c
 * Regression: "operator" must work as an alias for "op", and an
 * unrecognised operator string must return an error (not silently
 * degrade to equality).
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

static int test_criteria_operator_alias_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"op_alias_t\","
        "\"fields\":[\"name:varchar:32\",\"price:double\"],"
        "\"indexes\":[\"price\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"op_alias_t\",\"records\":{"
        "\"k1\":{\"name\":\"cheap\",\"price\":2.5},"
        "\"k2\":{\"name\":\"mid\",\"price\":7.0},"
        "\"k3\":{\"name\":\"expensive\",\"price\":15.0}"
        "}}", &resp); free(resp); resp = NULL;

    /* 1. "op" still works (baseline) */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"op_alias_t\","
        "\"criteria\":[{\"field\":\"price\",\"op\":\"gt\",\"value\":5}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "op:gt response not null");
    ASSERT_CONTAINS(resp, "\"name\":\"mid\"", "op:gt returns mid");
    ASSERT_CONTAINS(resp, "\"name\":\"expensive\"", "op:gt returns expensive");
    ASSERT_TRUE(strstr(resp, "\"name\":\"cheap\"") == NULL,
                "op:gt excludes cheap");
    free(resp); resp = NULL;

    /* 2. "operator" alias works identically */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"op_alias_t\","
        "\"criteria\":[{\"field\":\"price\",\"operator\":\"gt\",\"value\":5}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "operator:gt response not null");
    ASSERT_CONTAINS(resp, "\"name\":\"mid\"", "operator:gt returns mid");
    ASSERT_CONTAINS(resp, "\"name\":\"expensive\"", "operator:gt returns expensive");
    ASSERT_TRUE(strstr(resp, "\"name\":\"cheap\"") == NULL,
                "operator:gt excludes cheap");
    free(resp); resp = NULL;

    /* 3. Unrecognised operator returns error, not equality fallback */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"op_alias_t\","
        "\"criteria\":[{\"field\":\"price\",\"op\":\"bogus\",\"value\":5}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "bogus op response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "bogus op returns error");
    free(resp); resp = NULL;

    /* 4. Missing both "op" and "operator" returns error */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"op_alias_t\","
        "\"criteria\":[{\"field\":\"price\",\"value\":5}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "missing op response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "missing op returns error");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-criteria-operator-alias", test_criteria_operator_alias_run)
