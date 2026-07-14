/* src/test/cases/test_nql_input_validation.c
 * Regression: NQL unknown operator must error (not silently become equality).
 * Non-numeric --limit/--offset must error.  Empty order_by after colon
 * split must error.  Invalid order direction must error.
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

static int test_nql_input_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"nql_val_t\","
        "\"fields\":[\"name:varchar:32\",\"score:int\"],"
        "\"indexes\":[\"score\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"nql_val_t\",\"records\":{"
        "\"k1\":{\"name\":\"alice\",\"score\":10},"
        "\"k2\":{\"name\":\"bob\",\"score\":90}"
        "}}", &resp); free(resp); resp = NULL;

    /* 1. Unknown NQL operator returns error */
    tc_request(tc,
        "find default nql_val_t 'score foobar 50'",
        &resp);
    ASSERT_NOT_NULL(resp, "unknown NQL op response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "unknown NQL op returns error");
    free(resp); resp = NULL;

    /* 2. not + unknown op returns error (not equality) */
    tc_request(tc,
        "find default nql_val_t 'score not foo 50'",
        &resp);
    ASSERT_NOT_NULL(resp, "not+unknown op response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "not+unknown op returns error");
    free(resp); resp = NULL;

    /* 3. Non-numeric --limit returns error */
    tc_request(tc,
        "find default nql_val_t 'score gt 0' --limit abc",
        &resp);
    ASSERT_NOT_NULL(resp, "non-numeric limit response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "non-numeric limit returns error");
    free(resp); resp = NULL;

    /* 4. Negative --offset returns error */
    tc_request(tc,
        "find default nql_val_t 'score gt 0' --offset -5",
        &resp);
    ASSERT_NOT_NULL(resp, "negative offset response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "negative offset returns error");
    free(resp); resp = NULL;

    /* 5. --order-by :desc (leading colon) returns error */
    tc_request(tc,
        "find default nql_val_t 'score gt 0' --order-by :desc",
        &resp);
    ASSERT_NOT_NULL(resp, "leading colon response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "leading colon order-by returns error");
    free(resp); resp = NULL;

    /* 6. Invalid --order direction returns error */
    tc_request(tc,
        "find default nql_val_t 'score gt 0' --order sideways",
        &resp);
    ASSERT_NOT_NULL(resp, "invalid order direction response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "invalid order direction returns error");
    free(resp); resp = NULL;

    /* 7. Valid NQL still works (baseline) */
    tc_request(tc,
        "find default nql_val_t 'score gt 50'",
        &resp);
    ASSERT_NOT_NULL(resp, "valid NQL response not null");
    ASSERT_CONTAINS(resp, "\"name\":\"bob\"", "valid NQL returns bob");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"name\":\"alice\"") == NULL,
                "valid NQL excludes alice");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-input-validation", test_nql_input_validation_run)
