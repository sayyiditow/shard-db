/* src/test/cases/test_criteria_between_missing_value2.c
 * Regression: "between"/"len_between" criteria missing "value2" must be
 * rejected with an error, not silently treated as an empty upper bound.
 * parse_one_criterion() validated "value" for every op that requires one
 * but never validated "value2" for the two range ops that need both
 * bounds (see docs/query-protocol/find.md — between requires value AND
 * value2).
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

static int test_criteria_between_missing_value2_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"btw_val_t\","
        "\"fields\":[\"age:int\",\"name:varchar:32\"],\"indexes\":[],\"splits\":8}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"btw_val_t\",\"records\":{"
        "\"k1\":{\"age\":30,\"name\":\"alice\"}}}",
        &resp); free(resp); resp = NULL;

    /* 1. "between" missing value2 must be rejected */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"btw_val_t\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"between\",\"value\":\"18\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "between missing value2 response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "between missing value2 rejected");
    free(resp); resp = NULL;

    /* 2. "len_between" missing value2 must be rejected */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"btw_val_t\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"len_between\",\"value\":\"3\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "len_between missing value2 response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "len_between missing value2 rejected");
    free(resp); resp = NULL;

    /* 3. "between" WITH both bounds must still work (no false-positive
       rejection) and must actually match */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"btw_val_t\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"between\",\"value\":\"18\",\"value2\":\"65\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "between with value2 response not null");
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "between with both bounds not rejected");
    ASSERT_CONTAINS(resp, "\"name\":\"alice\"", "between with both bounds matches alice");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-criteria-between-missing-value2", test_criteria_between_missing_value2_run)
