/* src/test/cases/test_cas_tree_if.c
 *
 * Conditional-writes F3 — nested AND/OR trees in write `if` conditions,
 * flat-array compatibility, validation, bulk per-record trees.
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
#include <unistd.h>

static int test_cas_tree_if_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ti\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"status:varchar:12\",\"amount:int\","
        "\"region:varchar:8\",\"lo:long\",\"hi:long\"],"
        "\"indexes\":[\"status\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create ti");
    free(resp); resp = NULL;

    /* 1. Seed: k1 pending/3, k2 paid/9, k3 pending/50. */
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"ti\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"pending\",\"amount\":3,"
        "\"region\":\"eu\",\"lo\":10,\"hi\":20}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":9,"
        "\"region\":\"us\",\"lo\":10,\"hi\":20}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"pending\",\"amount\":50,"
        "\"region\":\"eu\",\"lo\":1,\"hi\":2}}]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "seed");
    free(resp); resp = NULL;

    /* 2. Nested OR/AND `if` — k1 matches (pending AND amount<10). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k1\",\"value\":{\"status\":\"done\"},\"if\":"
        "{\"or\":[{\"and\":[{\"field\":\"status\",\"op\":\"eq\","
        "\"value\":\"pending\"},{\"field\":\"amount\",\"op\":\"lt\","
        "\"value\":10}]},{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "nested OR update applies");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"ti\","
                   "\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"done\"", "new state visible");
    free(resp); resp = NULL;

    /* 3. Same tree against k3 (pending but amount 50): no branch matches. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k3\",\"value\":{\"status\":\"done\"},\"if\":"
        "{\"or\":[{\"and\":[{\"field\":\"status\",\"op\":\"eq\","
        "\"value\":\"pending\"},{\"field\":\"amount\",\"op\":\"lt\","
        "\"value\":10}]},{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "tree blocks k3");
    ASSERT_CONTAINS(resp, "\"current\":", "current still returned");
    free(resp); resp = NULL;

    /* 4. Flat array compat — unchanged semantics. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k2\",\"value\":{\"amount\":10},\"if\":"
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "flat array still works");
    free(resp); resp = NULL;

    /* 5. Unknown field → invalid if condition (validation, not a silent
          never-match). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k1\",\"value\":{\"amount\":4},\"if\":"
        "[{\"field\":\"nosuch\",\"op\":\"eq\",\"value\":1}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"invalid if condition\"",
                    "unknown field rejected");
    free(resp); resp = NULL;

    /* 6. Over-deep nesting (17 levels) → invalid if condition. */
    char deep[2048]; int off = snprintf(deep, sizeof(deep),
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k1\",\"value\":{\"amount\":4},\"if\":");
    for (int i = 0; i < 17; i++) off += snprintf(deep + off,
        sizeof(deep) - (size_t)off, "%s", "{\"and\":[");
    off += snprintf(deep + off, sizeof(deep) - (size_t)off,
        "{\"field\":\"amount\",\"op\":\"eq\",\"value\":4}");
    for (int i = 0; i < 17; i++) off += snprintf(deep + off,
        sizeof(deep) - (size_t)off, "%s", "]}");
    snprintf(deep + off, sizeof(deep) - (size_t)off, "}");
    tc_request(tc, deep, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"invalid if condition\"",
                    "depth cap inherited");
    free(resp); resp = NULL;

    /* 7. Delete with a nested tree. */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k3\",\"if\":{\"and\":[{\"field\":\"status\",\"op\":\"eq\","
        "\"value\":\"pending\"},{\"field\":\"region\",\"op\":\"eq\","
        "\"value\":\"eu\"}]}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"deleted\"", "nested delete applies");
    free(resp); resp = NULL;

    /* 8. Bulk per-record `if` with trees: k2 skipped, k1 updated. */
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\","
        "\"object\":\"ti\",\"records\":["
        "{\"key\":\"k2\",\"value\":{\"region\":\"ap\"},\"if\":{\"or\":"
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]}},"
        "{\"key\":\"k1\",\"value\":{\"region\":\"ap\"},\"if\":{\"or\":"
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"done\"}]}}]}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "bulk ok");
    ASSERT_CONTAINS(resp, "\"updated\":1", "one record updated");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "one record CAS-skipped");
    free(resp); resp = NULL;

    /* 9. eq_field works in a write condition. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k9\",\"value\":{\"status\":\"pending\",\"amount\":7,"
        "\"region\":\"eu\",\"lo\":7,\"hi\":7}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k9\",\"value\":{\"status\":\"cleared\"},\"if\":"
        "[{\"field\":\"lo\",\"op\":\"eq_field\",\"value\":\"hi\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "eq_field condition");
    free(resp); resp = NULL;

    /* 10. Typed comparison regression (rev-2 finding 7): a zero-padded
        integer literal now matches the typed value, same as find. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k9\",\"value\":{\"region\":\"xx\"},\"if\":"
        "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"07\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"",
                    "typed comparison parses literals like find");
    free(resp); resp = NULL;

    /* 11. Empty `if: []` keeps the historical vacuous pass. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k9\",\"value\":{\"region\":\"eu\"},\"if\":[   ]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "empty if passes vacuously");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-cas-tree-if", test_cas_tree_if_run)
