/* src/test/cases/test_server_dispatch_validation.c
 * Regression: missing required fields must return error (not empty
 * response or worker hang).  Negative offset must return error.
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

static int test_server_dispatch_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"sdv_t\","
        "\"fields\":[\"name:varchar:32\"],\"indexes\":[],\"splits\":8}",
        &resp); free(resp); resp = NULL;

    /* 1. add-index missing field/fields returns error */
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"sdv_t\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "add-index missing field response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "add-index missing field returns error");
    free(resp); resp = NULL;

    /* 2. get-file-path missing filename returns error */
    tc_request(tc,
        "{\"mode\":\"get-file-path\",\"dir\":\"default\",\"object\":\"sdv_t\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "get-file-path missing filename response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "get-file-path missing filename returns error");
    free(resp); resp = NULL;

    /* 3. bulk-insert missing records/file returns error (not hang) */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"sdv_t\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "bulk-insert missing records response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "bulk-insert missing records returns error");
    free(resp); resp = NULL;

    /* 4. bulk-delete key-list missing keys/file returns error (not hang) */
    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"sdv_t\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "bulk-delete missing keys response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "bulk-delete missing keys returns error");
    free(resp); resp = NULL;

    /* 5. find with negative offset returns error */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"sdv_t\","
        "\"offset\":-5,\"limit\":10}",
        &resp);
    ASSERT_NOT_NULL(resp, "negative offset response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "negative offset returns error");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-server-dispatch-validation", test_server_dispatch_validation_run)
