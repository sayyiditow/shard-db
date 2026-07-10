/* src/test/cases/test_nql_agg_filter_override_leak.c
 * Regression: NQL aggregate combining a positional filter with --filter
 * must not leak the discarded positional CriteriaNode tree. nql.c's
 * --filter handler did `out->filter = nql_parse_filter(...)`
 * unconditionally, dropping whatever tree the positional-filter
 * heuristic had already built without freeing it.
 *
 * This is a leak, not a behavioral bug -- --filter already wins on main
 * (test_nql_agg_filter_flag_run's test 3 covers that). Build with
 * BUILD_MODE=asan to catch the leak via LeakSanitizer; the TAP
 * assertions below only guard that --filter still wins.
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

static int test_nql_agg_filter_override_leak_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"agg_leak_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\"],"
        "\"indexes\":[\"status\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"agg_leak_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"pending\",\"amount\":5}}"
        "]}", &resp); free(resp); resp = NULL;

    /* Positional filter + --filter in the same aggregate query, repeated —
       each iteration leaks one discarded CriteriaNode tree pre-fix. */
    for (int i = 0; i < 200; i++) {
        tc_request(tc,
            "aggregate default agg_leak_t 'status eq paid' "
            "--filter 'status eq pending' "
            "sum(amount)",
            &resp);
        ASSERT_NOT_NULL(resp, "response not null");
        ASSERT_CONTAINS(resp, "\"sum_amount\":10", "--filter override still wins");
        free(resp); resp = NULL;
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-agg-filter-override-leak", test_nql_agg_filter_override_leak_run)
