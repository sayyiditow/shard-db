/* src/test/cases/test_range_coalesce.c
 * The planner's same-field range coalescer should rewrite all four
 * lower×upper pairings on the same indexed field into a single BETWEEN
 * leaf with the right exclusivity flags. Verifies result correctness;
 * the perf win is observed in the bench, not the test suite.
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

static int parse_count(const char *resp) {
    if (!resp) return -1;
    while (*resp == ' ' || *resp == '\n') resp++;
    return atoi(resp);
}

static int do_count(TestClient *tc, const char *crit) {
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rc\","
        "\"criteria\":%s}", crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = parse_count(resp);
    free(resp);
    return n;
}

static int test_range_coalesce_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rc\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"n:int\"],\"indexes\":[\"n\"]}", &resp); free(resp); resp = NULL;

    /* Seed n = 1..10 so we can construct ranges with known answer counts. */
    for (int i = 1; i <= 10; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rc\","
            "\"key\":\"k%d\",\"value\":{\"n\":%d}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* gte 3 + lte 7 → [3,7] = {3,4,5,6,7} = 5 records. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"n\",\"op\":\"gte\",\"value\":\"3\"},"
        "{\"field\":\"n\",\"op\":\"lte\",\"value\":\"7\"}]"),
        5, "gte+lte → between inclusive");

    /* gt 3 + lte 7 → (3,7] = {4,5,6,7} = 4 records. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"3\"},"
        "{\"field\":\"n\",\"op\":\"lte\",\"value\":\"7\"}]"),
        4, "gt+lte → between min-exclusive");

    /* gte 3 + lt 7 → [3,7) = {3,4,5,6} = 4 records. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"n\",\"op\":\"gte\",\"value\":\"3\"},"
        "{\"field\":\"n\",\"op\":\"lt\",\"value\":\"7\"}]"),
        4, "gte+lt → between max-exclusive");

    /* gt 3 + lt 7 → (3,7) = {4,5,6} = 3 records. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"3\"},"
        "{\"field\":\"n\",\"op\":\"lt\",\"value\":\"7\"}]"),
        3, "gt+lt → between both-exclusive");

    /* Reverse-order pairings — order in the criteria array shouldn't matter. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"n\",\"op\":\"lt\",\"value\":\"7\"},"
        "{\"field\":\"n\",\"op\":\"gt\",\"value\":\"3\"}]"),
        3, "lt+gt (reversed) → between both-exclusive");

    /* Empty range — gt > lte. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"7\"},"
        "{\"field\":\"n\",\"op\":\"lte\",\"value\":\"3\"}]"),
        0, "gt 7 + lte 3 → empty range");

    /* Equal-bound exclusive pair → empty. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"5\"},"
        "{\"field\":\"n\",\"op\":\"lt\",\"value\":\"5\"}]"),
        0, "gt 5 + lt 5 → empty range (mutually exclusive)");

    /* Single-field unpaired (two GT) → no coalesce; planner falls through. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"3\"},"
        "{\"field\":\"n\",\"op\":\"gt\",\"value\":\"5\"}]"),
        5, "two GT same field → no coalesce, both filters apply, n>5 = {6..10}");

    /* Verify find returns correct keys for gt+lt (the most-exclusive case). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rc\","
        "\"criteria\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"3\"},"
        "{\"field\":\"n\",\"op\":\"lt\",\"value\":\"7\"}],"
        "\"order_by\":\"n\",\"order\":\"asc\"}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k4\"", "gt+lt finds k4");
    ASSERT_CONTAINS(resp, "\"key\":\"k5\"", "gt+lt finds k5");
    ASSERT_CONTAINS(resp, "\"key\":\"k6\"", "gt+lt finds k6");
    ASSERT_TRUE(resp && strstr(resp, "\"key\":\"k3\"") == NULL, "gt+lt excludes k3 (boundary)");
    ASSERT_TRUE(resp && strstr(resp, "\"key\":\"k7\"") == NULL, "gt+lt excludes k7 (boundary)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-range-coalesce", test_range_coalesce_run)
