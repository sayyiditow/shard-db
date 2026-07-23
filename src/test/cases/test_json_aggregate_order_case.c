/* src/test/cases/test_json_aggregate_order_case.c
 * Characterization test for the JSON aggregate "order" field's
 * case-insensitive "desc" handling in dispatch_json_query(). Guards the
 * dead-code cleanup that drops the redundant
 * `strcmp(od,"desc")==0 || strcasecmp(od,"desc")==0` down to just the
 * strcasecmp half -- the strcmp half can never be true when the
 * strcasecmp half is false, so removing it must not change behavior.
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

static int test_json_aggregate_order_case_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ord_case_t\","
        "\"fields\":[\"grp:varchar:8\",\"amount:int\"],\"indexes\":[\"grp\"],\"splits\":8}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ord_case_t\",\"records\":{"
        "\"k1\":{\"grp\":\"a\",\"amount\":100},\"k2\":{\"grp\":\"b\",\"amount\":10}}}",
        &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"ord_case_t\","
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"total\"}],"
        "\"group_by\":[\"grp\"],\"order_by\":\"total\",\"order\":\"DESC\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "uppercase DESC response not null");
    if (resp) {
        const char *pa = SAFE_STRSTR(resp, "\"grp\":\"a\"");
        const char *pb = SAFE_STRSTR(resp, "\"grp\":\"b\"");
        ASSERT_TRUE(pa && pb && pa < pb, "order:DESC (uppercase) sorts descending by total");
    }
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-json-aggregate-order-case", test_json_aggregate_order_case_run)
