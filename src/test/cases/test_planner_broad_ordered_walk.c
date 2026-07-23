/* Broad-filter ordered finds must walk the order index (D3), not materialize-
 * and-sort (D2). Regression guard for the 30s `type in (...) ORDER BY score`
 * timeout: a bitmap candidate set is exact-but-huge (estimable && !saturated),
 * which the D2/D3 fork used to read as "small enough to sort". See
 * docs/plans/2026-06-04-planner-broad-ordered-walk.md.
 *
 * Strategy: use OP_EQUAL on a broad bitmap value with NO covering composite
 * so the planner falls through to the D2/D3 fork. OP_IN on bitmap is
 * separately tracked (issue C in the plan — it encodes the raw string). */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* Plan-path introspection hook (defined in query.c). Returns the order-overlay
 * kind as a string: "none" | "composite" | "sort" | "walk". */
extern const char *plan_filter_kind_for_test(
    const char *db_root, const char *object,
    const char *criteria_json, const char *order_by, int fetching,
    char *out_field, size_t fsz, char *out_order, size_t osz,
    int *out_total_cheap);

static const char *order_of(TestEnv *env, const char *obj,
                            const char *crit, const char *order_by) {
    static char order[32];
    char field[64] = {0}; int cheap = -1;
    order[0] = '\0';
    plan_filter_kind_for_test(env->db_root, obj, crit, order_by, 1,
                              field, sizeof(field), order, sizeof(order), &cheap);
    return order;
}

static int test_planner_broad_ordered_walk(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"p\"}", &resp); free(resp); resp=NULL;

    /* type: bitmap; score: single btree (drivable order_by). NO type+score or
     * other composite — OP_EQUAL on type must NOT have a covering composite,
     * falling through to the D2/D3 fork. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"p\",\"object\":\"bw\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"type:varchar:8\",\"score:int\"],"
        "\"indexes\":[\"type:bitmap\",\"score\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create bw"); free(resp); resp=NULL;

    /* 1000 rows, almost all type=story (broad, k=950 >> N/8=125),
     * a handful type=job (rare/selective). */
    for (int i = 0; i < 1000; i++) {
        char req[256];
        const char *t = (i % 200 == 0) ? "job" : "story";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"p\",\"object\":\"bw\",\"key\":\"k%04d\","
            "\"value\":{\"type\":\"%s\",\"score\":%d}}", i, t, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    /* BROAD: type=story covers ~all rows + ORDER BY score → must WALK. */
    ASSERT_EQ_STR(order_of(&env, "p/bw",
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"story\"}]", "score"),
        "walk",
        "broad type EQ + order score must walk the order index, not sort");

    /* Correctness: top-3 by score desc are the highest keys (score==i). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"p\",\"object\":\"bw\","
        "\"criteria\":[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"story\"}],"
        "\"order_by\":\"score\",\"order\":\"desc\",\"limit\":3}", &resp);
    ASSERT_CONTAINS(resp, "\"score\":999", "top score present");
    ASSERT_CONTAINS(resp, "\"score\":998", "2nd score present");
    ASSERT_CONTAINS(resp, "\"score\":997", "3rd score present");
    free(resp); resp=NULL;

    /* SELECTIVE: type=job is rare (5/1000, k=5 <= N/8=125) → fetch+sort, not
     * walk. Guards pick_sort_or_walk's selective branch. */
    ASSERT_EQ_STR(order_of(&env, "p/bw",
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"job\"}]", "score"),
        "sort",
        "selective type=job + order score must sort (small set), not walk");

    /* NON-DRIVABLE order_by: broad filter but the order_by field has no btree
     * to walk → must fall back to sort. Guards pick_sort_or_walk's third branch. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"p\",\"object\":\"nd\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"type:varchar:8\",\"rank:int\"],"
        "\"indexes\":[\"type:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create nd"); free(resp); resp=NULL;
    for (int i = 0; i < 400; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"p\",\"object\":\"nd\",\"key\":\"k%04d\","
            "\"value\":{\"type\":\"story\",\"rank\":%d}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
    ASSERT_EQ_STR(order_of(&env, "p/nd",
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"story\"}]", "rank"),
        "sort",
        "broad filter but non-drivable order_by must fall back to sort");

    tc_close(tc); test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-planner-broad-ordered-walk", test_planner_broad_ordered_walk)
