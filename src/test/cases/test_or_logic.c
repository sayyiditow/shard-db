/* src/test/cases/test_or_logic.c
 *
 * Port of tests/test-or-logic.sh — coverage for OR criteria across
 * find/count/aggregate/bulk planner shapes (A: AND only, B: AND+OR,
 * C: pure OR all-indexed → KeySet path, D: OR with non-indexed child
 * → full scan, plus hybrid AND+OR planning).
 */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Count occurrences of needle in haystack. */
static int substr_count(const char *hay, const char *needle) {
    if (!hay || !needle || !*needle) return 0;
    int n = 0;
    size_t nl = strlen(needle);
    for (const char *p = hay; (p = strstr(p, needle)); p += nl) n++;
    return n;
}

/* Send JSON request via tc, return heap-allocated response (caller frees).
   Returns NULL on failure. */
static char *req(TestClient *tc, const char *json) {
    char *resp = NULL;
    if (tc_request(tc, json, &resp) != 0) return NULL;
    return resp;
}

static int test_or_logic_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        ASSERT_TRUE(0, "daemon spawn");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    /* Register the "default" tenant dir at runtime. */
    char *resp = req(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    free(resp);

    /* === Schema + dataset setup (mirrors shell test exactly) ===
       status + region are indexed; amount + notes are NOT indexed. */
    resp = req(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"status:varchar:16\",\"amount:int\","
                    "\"region:varchar:16\",\"notes:varchar:64\"],"
        "\"indexes\":[\"status\",\"region\"]}");
    free(resp);

    static const char *records[] = {
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"or_orders\","
            "\"key\":\"o1\",\"value\":{\"status\":\"paid\",\"amount\":100,"
            "\"region\":\"us\",\"notes\":\"vip\"}}",
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"or_orders\","
            "\"key\":\"o2\",\"value\":{\"status\":\"paid\",\"amount\":50,"
            "\"region\":\"eu\",\"notes\":\"standard\"}}",
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"or_orders\","
            "\"key\":\"o3\",\"value\":{\"status\":\"pending\",\"amount\":200,"
            "\"region\":\"us\",\"notes\":\"vip\"}}",
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"or_orders\","
            "\"key\":\"o4\",\"value\":{\"status\":\"cancelled\",\"amount\":0,"
            "\"region\":\"asia\",\"notes\":\"refunded\"}}",
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"or_orders\","
            "\"key\":\"o5\",\"value\":{\"status\":\"refunded\",\"amount\":75,"
            "\"region\":\"eu\",\"notes\":\"duplicate\"}}",
    };
    for (size_t i = 0; i < sizeof(records)/sizeof(records[0]); i++) {
        resp = req(tc, records[i]); free(resp);
    }

    /* === PARSER: error cases === */
    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"or\":[]}]}");
    ASSERT_CONTAINS(resp, "empty or/and", "empty or rejected"); free(resp);

    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"and\":[]}]}");
    ASSERT_CONTAINS(resp, "empty or/and", "empty and rejected"); free(resp);

    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"field\":\"\"}]}");
    ASSERT_CONTAINS(resp, "missing field", "missing field rejected"); free(resp);

    /* === SHAPE A (AND only) — regression baseline === */
    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}");
    ASSERT_CONTAINS(resp, "2", "count status=paid"); free(resp);

    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                      "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}]}");
    ASSERT_CONTAINS(resp, "1", "count status=paid AND region=us"); free(resp);

    resp = req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                      "{\"field\":\"amount\",\"op\":\"gte\",\"value\":\"100\"}]}");
    ASSERT_CONTAINS(resp, "\"key\":\"o1\"", "find paid+amount>=100 includes o1");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"key\":\"o2\"") == NULL, "find paid+amount>=100 excludes o2");
    free(resp);

    /* === SHAPE B (AND + OR, indexed AND sibling drives primary) === */
    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                      "{\"or\":[{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"},"
                               "{\"field\":\"amount\",\"op\":\"lt\",\"value\":\"100\"}]}]}");
    ASSERT_CONTAINS(resp, "2", "Shape B: paid AND (region=us OR amount<100) = 2"); free(resp);

    resp = req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                      "{\"or\":[{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"},"
                               "{\"field\":\"amount\",\"op\":\"lt\",\"value\":\"100\"}]}]}");
    ASSERT_CONTAINS(resp, "\"key\":\"o1\"", "Shape B: includes o1 (paid+us)");
    ASSERT_CONTAINS(resp, "\"key\":\"o2\"", "Shape B: includes o2 (paid+amount=50)");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"key\":\"o3\"") == NULL, "Shape B: excludes o3 (pending)");
    free(resp);

    /* === SHAPE C (pure OR, all children indexed → KeySet path) === */
    /* o1 (paid,us), o2 (paid,eu), o3 (pending,us) → 3 */
    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                               "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}]}]}");
    ASSERT_CONTAINS(resp, "3", "Shape C: paid OR us = 3"); free(resp);

    resp = req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                               "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}]}]}");
    ASSERT_CONTAINS(resp, "\"key\":\"o1\"", "Shape C find: o1 present");
    ASSERT_CONTAINS(resp, "\"key\":\"o2\"", "Shape C find: o2 present");
    ASSERT_CONTAINS(resp, "\"key\":\"o3\"", "Shape C find: o3 present");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"key\":\"o4\"") == NULL, "Shape C find: o4 absent");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"key\":\"o5\"") == NULL, "Shape C find: o5 absent");
    free(resp);

    /* Three indexed OR children: o1,o2 (paid), o3 (pending), o4 (asia) → 4 */
    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                               "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"},"
                               "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"asia\"}]}]}");
    ASSERT_CONTAINS(resp, "4", "Shape C: 3-child OR = 4"); free(resp);

    /* === SHAPE D (OR with non-indexed child → full scan) === */
    /* o1,o2 (paid), o3 (amount=200) → 3 */
    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                               "{\"field\":\"amount\",\"op\":\"gte\",\"value\":\"200\"}]}]}");
    ASSERT_CONTAINS(resp, "3", "Shape D: paid OR amount>=200 = 3"); free(resp);

    /* notes is non-indexed; o1 (vip), o3 (vip), o4 (cancelled) */
    resp = req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"or\":[{\"field\":\"notes\",\"op\":\"eq\",\"value\":\"vip\"},"
                               "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"cancelled\"}]}]}");
    ASSERT_CONTAINS(resp, "\"key\":\"o1\"", "Shape D find: o1");
    ASSERT_CONTAINS(resp, "\"key\":\"o3\"", "Shape D find: o3");
    ASSERT_CONTAINS(resp, "\"key\":\"o4\"", "Shape D find: o4");
    free(resp);

    /* === HYBRID (non-indexed AND + fully-indexed OR → KeySet primary) ===
       OR-union = {o1,o2,o3}. Of those amount>60: o1(100), o3(200). Not o2(50). */
    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"field\":\"amount\",\"op\":\"gt\",\"value\":\"60\"},"
                      "{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                               "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}]}]}");
    ASSERT_CONTAINS(resp, "2", "Hybrid: amount>60 AND (paid OR us) = 2"); free(resp);

    /* === limit / order_by across OR ===
       Shape C with limit=2 should emit at most 2 rows. */
    resp = req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                               "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}]}],"
        "\"offset\":0,\"limit\":2}");
    int key_count = substr_count(resp, "\"key\":\"o");
    ASSERT_EQ_INT(key_count, 2, "Shape C find: limit=2 respected");
    free(resp);

    /* Ordered find with OR: paid: o1(100), o2(50); refunded: o5(75).
       Sort by amount asc: o2(50), o5(75), o1(100). First is o2. */
    resp = req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                               "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"refunded\"}]}],"
        "\"order_by\":\"amount\"}");
    ASSERT_CONTAINS(resp, "\"key\":\"o2\"", "ordered find with OR: first is o2");
    free(resp);

    /* === aggregate with OR ===
       Shape C: o1,o2,o3 matches — n=3, sum=100+50+200=350 */
    resp = req(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},"
                        "{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"total\"}],"
        "\"criteria\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                               "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}]}]}");
    ASSERT_CONTAINS(resp, "\"n\":3", "agg Shape C count");
    ASSERT_CONTAINS(resp, "\"total\":350", "agg Shape C sum");
    free(resp);

    /* Group by region: paid: o1(us),o2(eu); refunded: o5(eu). us=1, eu=2. */
    resp = req(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"region\"],"
        "\"criteria\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                               "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"refunded\"}]}]}");
    ASSERT_CONTAINS(resp, "\"region\":\"us\",\"n\":1", "agg group_by region us=1");
    ASSERT_CONTAINS(resp, "\"region\":\"eu\",\"n\":2", "agg group_by region eu=2");
    free(resp);

    /* === bulk-delete with OR (dry_run + actual) === */
    resp = req(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"cancelled\"},"
                               "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"refunded\"}]}],"
        "\"dry_run\":true}");
    ASSERT_CONTAINS(resp, "\"matched\":2", "bulk-delete dry_run matched=2");
    ASSERT_CONTAINS(resp, "\"deleted\":0", "bulk-delete dry_run deleted=0");
    ASSERT_CONTAINS(resp, "\"dry_run\":true", "bulk-delete dry_run flag");
    free(resp);

    /* count before actual delete = 5 */
    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\"}");
    ASSERT_CONTAINS(resp, "5", "count before actual delete = 5"); free(resp);

    resp = req(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"cancelled\"},"
                               "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"refunded\"}]}]}");
    ASSERT_CONTAINS(resp, "\"deleted\":2", "bulk-delete actual deleted=2"); free(resp);

    /* count after delete = 3 */
    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\"}");
    ASSERT_CONTAINS(resp, "3", "count after delete = 3"); free(resp);

    /* === bulk-update with OR condition ===
       amount<100: o2(50); region=us: o1, o3 → matched=3, updated=3 */
    resp = req(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"or\":[{\"field\":\"amount\",\"op\":\"lt\",\"value\":\"100\"},"
                               "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}]}],"
        "\"value\":{\"notes\":\"touched\"}}");
    ASSERT_CONTAINS(resp, "\"matched\":3", "bulk-update matched=3");
    ASSERT_CONTAINS(resp, "\"updated\":3", "bulk-update updated=3");
    free(resp);

    /* All three should have notes=touched */
    resp = req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"field\":\"notes\",\"op\":\"eq\",\"value\":\"touched\"}]}");
    ASSERT_CONTAINS(resp, "\"key\":\"o1\"", "o1 updated");
    ASSERT_CONTAINS(resp, "\"key\":\"o2\"", "o2 updated");
    ASSERT_CONTAINS(resp, "\"key\":\"o3\"", "o3 updated");
    free(resp);

    /* === Backward compatibility: flat array still means AND === */
    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}");
    ASSERT_CONTAINS(resp, "2", "flat-array single leaf"); free(resp);

    /* Simple-equality shorthand dict → AND */
    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":{\"status\":\"paid\",\"region\":\"us\"}}");
    ASSERT_CONTAINS(resp, "1", "simple-equality form still AND"); free(resp);

    /* === nested OR-in-OR ===
       After bulk-delete + bulk-update, remaining: o1(paid), o2(paid), o3(pending).
       Only paid = 2 (o1, o2). */
    resp = req(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"or_orders\","
        "\"criteria\":[{\"or\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\","
                                         "\"value\":\"paid\"}]}]}]}");
    ASSERT_CONTAINS(resp, "2", "nested-OR single leaf count=2"); free(resp);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-or-logic", test_or_logic_run)
