/* src/test/cases/test_criteria_in_then_invalid_leak.c
 * Regression: a criteria array containing a valid "in"/"not_in" leaf
 * followed by an invalid leaf must not leak the first leaf's in_values.
 * parse_criteria_json()'s array-form error path used to bare
 * free(criteria) instead of free_criteria(criteria, n), dropping the
 * already-parsed IN leaf's in_values array and its strdup'd strings.
 *
 * This is a leak, not a behavioral bug — the client-visible response
 * (an error, since the array is rejected as a whole) is identical
 * before and after the fix. Build with BUILD_MODE=asan to catch the
 * leak via LeakSanitizer; the TAP assertions below only guard that the
 * behavior (still an error) didn't regress.
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

static int test_criteria_in_then_invalid_leak_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"leak_t\","
        "\"fields\":[\"tag:varchar:16\"],\"indexes\":[\"tag\"],\"splits\":8}",
        &resp); free(resp); resp = NULL;

    /* Valid IN leaf (allocates in_values) followed by an invalid leaf
       (missing "op") in the SAME array. Repeated so a leak is visible in
       RSS growth even without a sanitizer, though ASAN/LSAN is the
       authoritative check (see execution rules). */
    for (int i = 0; i < 200; i++) {
        tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"leak_t\","
            "\"criteria\":[{\"field\":\"tag\",\"op\":\"in\",\"value\":\"a,b,c\"},"
            "{\"field\":\"bad\"}]}",
            &resp);
        ASSERT_NOT_NULL(resp, "response not null");
        ASSERT_CONTAINS(resp, "\"error\"", "invalid trailing criterion rejects whole array");
        free(resp); resp = NULL;
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-criteria-in-then-invalid-leak", test_criteria_in_then_invalid_leak_run)
