/* src/test/cases/test_criteria_field_value_validation.c
 * Regression: missing "field" must return error (not silent empty match).
 * Missing "value" for ops that require it must return error (not silent
 * empty-string match).  exists/not_exists must still work without "value".
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

static int test_criteria_field_value_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"fv_val_t\","
        "\"fields\":[\"name:varchar:32\",\"tag:varchar:16\"],"
        "\"indexes\":[\"tag\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"fv_val_t\",\"records\":{"
        "\"k1\":{\"name\":\"alpha\",\"tag\":\"red\"},"
        "\"k2\":{\"name\":\"beta\"}"
        "}}", &resp); free(resp); resp = NULL;

    /* 1. Missing "field" returns error */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fv_val_t\","
        "\"criteria\":[{\"op\":\"eq\",\"value\":\"x\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "missing field response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "missing field returns error");
    free(resp); resp = NULL;

    /* 2. Missing "value" for eq returns error */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fv_val_t\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "missing value for eq response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "missing value for eq returns error");
    free(resp); resp = NULL;

    /* 3. Missing "value" for gt returns error */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fv_val_t\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"gt\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "missing value for gt response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "missing value for gt returns error");
    free(resp); resp = NULL;

    /* 4. exists without "value" must work — tag field exists on k1, not k2 */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fv_val_t\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"exists\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "exists without value response not null");
    ASSERT_CONTAINS(resp, "\"name\":\"alpha\"", "exists: alpha has tag");
    ASSERT_TRUE(strstr(resp, "\"name\":\"beta\"") == NULL,
                "exists: beta has no tag");
    free(resp); resp = NULL;

    /* 5. not_exists without "value" must work */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fv_val_t\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"not_exists\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "not_exists without value response not null");
    ASSERT_CONTAINS(resp, "\"name\":\"beta\"", "not_exists: beta has no tag");
    ASSERT_TRUE(strstr(resp, "\"name\":\"alpha\"") == NULL,
                "not_exists: alpha has tag");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-criteria-field-value-validation", test_criteria_field_value_validation_run)
