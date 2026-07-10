/* src/test/cases/test_nql_order_by_direction.c
 * Regression: NQL --order-by field:DIR must validate DIR the same way
 * the standalone --order flag does. Previously any suffix other than
 * the exact lowercase string "desc" was silently treated as ascending
 * (no error) -- so --order-by field:DESC (or a typo) silently sorted
 * the wrong way instead of honoring the direction or erroring.
 *
 * On the find path, cmd_find_do already handles "DESC" via
 * strcmp(order_dir, "desc") == 0 || strcmp(order_dir, "DESC") == 0,
 * so assertion 1 is a characterization/regression guard (passes before
 * and after the fix). Only assertion 2 (garbage direction rejected) is
 * the actual pre-fix-failing case.
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

static int test_nql_order_by_direction_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"nql_ord_t\","
        "\"fields\":[\"amount:int\"],\"indexes\":[\"amount\"],\"splits\":8}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"nql_ord_t\",\"records\":{"
        "\"k1\":{\"amount\":10},\"k2\":{\"amount\":20},\"k3\":{\"amount\":30}}}",
        &resp); free(resp); resp = NULL;

    /* 1. Uppercase direction must sort descending, not silently fall
       back to ascending. Already works on main via cmd_find_do's
       "DESC" special-case; this assertion guards against regression. */
    tc_request(tc, "find default nql_ord_t --order-by amount:DESC", &resp);
    ASSERT_NOT_NULL(resp, "DESC response not null");
    if (resp) {
        const char *p30 = strstr(resp, "\"amount\":30");
        const char *p10 = strstr(resp, "\"amount\":10");
        ASSERT_TRUE(p30 && p10 && p30 < p10, "amount:DESC sorts descending");
    }
    free(resp); resp = NULL;

    /* 2. Garbage direction must be rejected, not silently accepted as
       ascending. This is the actual pre-fix-failing assertion. */
    tc_request(tc, "find default nql_ord_t --order-by amount:sideways", &resp);
    ASSERT_NOT_NULL(resp, "garbage direction response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "garbage order direction rejected");
    free(resp); resp = NULL;

    /* 3. Lowercase 'asc' still works (backward compat). */
    tc_request(tc, "find default nql_ord_t --order-by amount:asc", &resp);
    ASSERT_NOT_NULL(resp, "asc response not null");
    if (resp) {
        const char *p30 = strstr(resp, "\"amount\":30");
        const char *p10 = strstr(resp, "\"amount\":10");
        ASSERT_TRUE(p30 && p10 && p10 < p30, "amount:asc sorts ascending");
    }
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-order-by-direction", test_nql_order_by_direction_run)
