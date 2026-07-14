/* src/test/cases/test_agg_having_no_group_regression.c
 * Regression guard: cmd_aggregate_do()'s fast-path shortcuts gate on
 * `no_having`, which used to be computed from having_json alone. Once
 * having flows through having_tree instead (for both the JSON and NQL
 * entry points), no_having must also check having_tree — otherwise a
 * real having filter is silently skipped whenever a no-group-by fast
 * path fires (count-only metadata path, algebraic sum/avg/min/max path,
 * neq shortcut, top-N streaming eligibility). This test uses the
 * plainest shape that triggers the count-only fast path (no criteria,
 * no group_by, single count() spec) and asserts the having filter still
 * drops the (only) bucket when its condition is false.
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

static int test_agg_having_no_group_regression_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"having_nogroup_t\","
        "\"fields\":[\"amount:int\"],\"indexes\":[],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"having_nogroup_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"amount\":20}},"
        "{\"key\":\"k3\",\"value\":{\"amount\":30}},"
        "{\"key\":\"k4\",\"value\":{\"amount\":40}},"
        "{\"key\":\"k5\",\"value\":{\"amount\":50}}"
        "]}", &resp); free(resp); resp = NULL;

    /* JSON path — impossible having (n>1000) must drop the single bucket
       to an empty array, NOT bypass to the raw count-only "{\"n\":5}" the
       count-only fast path would emit if no_having ignored having_tree. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"having_nogroup_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"having\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"1000\"}]}",
        &resp);
    ASSERT_TRUE(resp != NULL, "JSON: got response");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"n\":5") == NULL,
                "JSON: count-only fast path must not bypass having filter");
    ASSERT_CONTAINS(resp, "[]", "JSON: impossible having drops the only bucket");
    free(resp); resp = NULL;

    /* JSON control — satisfiable having (n>1) must keep the bucket. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"having_nogroup_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"having\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"1\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"n\":5", "JSON: satisfiable having keeps the bucket");
    free(resp); resp = NULL;

    /* NQL path — same impossible-having shape over the raw-text protocol. */
    tc_request(tc,
        "aggregate default having_nogroup_t count() --having 'count gt 1000'",
        &resp);
    ASSERT_TRUE(resp != NULL, "NQL: got response");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"count\":5") == NULL,
                "NQL: count-only fast path must not bypass having filter");
    ASSERT_CONTAINS(resp, "[]", "NQL: impossible having drops the only bucket");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-having-no-group-regression", test_agg_having_no_group_regression_run)
