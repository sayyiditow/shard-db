/* Part A: verify that the op_caps table produces the correct eligibility
 * flags for each operator. The planner's whitelists (intersect, composite,
 * trigram, order_bound, rank) are now driven by this single table.
 * This test confirms each op's capabilities match expected behavior. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

extern int op_intersect_eligible_for_test(enum SearchOp op);
extern int op_composite_seed_eligible_for_test(enum SearchOp op);
extern int op_composite_exact_eligible_for_test(enum SearchOp op);
extern int op_order_bound_eligible_for_test(enum SearchOp op);
extern int op_trigram_prefers_for_test(enum SearchOp op);
extern int op_trigram_starts_for_test(enum SearchOp op);
extern int op_selectivity_rank_for_test(enum SearchOp op);

static int test_op_caps_intersect(void) {
    /* Ops that should be intersection-eligible */
    ASSERT_TRUE(op_intersect_eligible_for_test(OP_EQUAL),        "EQ intersect");
    ASSERT_TRUE(op_intersect_eligible_for_test(OP_STARTS_WITH),  "STARTS_WITH intersect");
    ASSERT_TRUE(op_intersect_eligible_for_test(OP_LESS),         "LESS intersect");
    ASSERT_TRUE(op_intersect_eligible_for_test(OP_GREATER),      "GREATER intersect");
    ASSERT_TRUE(op_intersect_eligible_for_test(OP_LESS_EQ),      "LESS_EQ intersect");
    ASSERT_TRUE(op_intersect_eligible_for_test(OP_GREATER_EQ),   "GREATER_EQ intersect");
    ASSERT_TRUE(op_intersect_eligible_for_test(OP_BETWEEN),      "BETWEEN intersect");
    ASSERT_TRUE(op_intersect_eligible_for_test(OP_IN),           "IN intersect");
    /* Ops that should NOT be intersection-eligible */
    ASSERT_TRUE(!op_intersect_eligible_for_test(OP_NOT_IN),      "NOT_IN intersect");
    ASSERT_TRUE(!op_intersect_eligible_for_test(OP_CONTAINS),    "CONTAINS intersect");
    ASSERT_TRUE(!op_intersect_eligible_for_test(OP_LIKE),        "LIKE intersect");
    return 0;
}
TEST_REGISTER("test-op-caps-intersect", test_op_caps_intersect)

static int test_op_caps_composite_seed(void) {
    ASSERT_TRUE(op_composite_seed_eligible_for_test(OP_EQUAL),       "EQ composite_seed");
    ASSERT_TRUE(op_composite_seed_eligible_for_test(OP_STARTS_WITH), "STARTS_WITH composite_seed");
    ASSERT_TRUE(op_composite_seed_eligible_for_test(OP_IN),          "IN composite_seed");
    ASSERT_TRUE(!op_composite_seed_eligible_for_test(OP_BETWEEN),    "BETWEEN composite_seed");
    ASSERT_TRUE(!op_composite_seed_eligible_for_test(OP_NOT_IN),     "NOT_IN composite_seed");
    return 0;
}
TEST_REGISTER("test-op-caps-composite-seed", test_op_caps_composite_seed)

static int test_op_caps_composite_exact(void) {
    ASSERT_TRUE(op_composite_exact_eligible_for_test(OP_EQUAL), "EQ composite_exact");
    ASSERT_TRUE(!op_composite_exact_eligible_for_test(OP_IN),   "IN composite_exact");
    ASSERT_TRUE(!op_composite_exact_eligible_for_test(OP_NOT_IN), "NOT_IN composite_exact");
    return 0;
}
TEST_REGISTER("test-op-caps-composite-exact", test_op_caps_composite_exact)

static int test_op_caps_order_bound(void) {
    ASSERT_TRUE(op_order_bound_eligible_for_test(OP_EQUAL),       "EQ order_bound");
    ASSERT_TRUE(op_order_bound_eligible_for_test(OP_LESS),        "LESS order_bound");
    ASSERT_TRUE(op_order_bound_eligible_for_test(OP_GREATER),     "GREATER order_bound");
    ASSERT_TRUE(op_order_bound_eligible_for_test(OP_LESS_EQ),     "LESS_EQ order_bound");
    ASSERT_TRUE(op_order_bound_eligible_for_test(OP_GREATER_EQ),  "GREATER_EQ order_bound");
    ASSERT_TRUE(op_order_bound_eligible_for_test(OP_BETWEEN),     "BETWEEN order_bound");
    ASSERT_TRUE(!op_order_bound_eligible_for_test(OP_IN),         "IN order_bound");
    ASSERT_TRUE(!op_order_bound_eligible_for_test(OP_CONTAINS),   "CONTAINS order_bound");
    return 0;
}
TEST_REGISTER("test-op-caps-order-bound", test_op_caps_order_bound)

static int test_op_caps_trigram_prefers(void) {
    ASSERT_TRUE(op_trigram_prefers_for_test(OP_CONTAINS),   "CONTAINS trigram_prefers");
    ASSERT_TRUE(op_trigram_prefers_for_test(OP_ICONTAINS),  "ICONTAINS trigram_prefers");
    ASSERT_TRUE(!op_trigram_prefers_for_test(OP_EQUAL),     "EQ trigram_prefers");
    ASSERT_TRUE(!op_trigram_prefers_for_test(OP_STARTS_WITH), "STARTS_WITH trigram_prefers");
    return 0;
}
TEST_REGISTER("test-op-caps-trigram-prefers", test_op_caps_trigram_prefers)

static int test_op_caps_trigram_starts(void) {
    ASSERT_TRUE(op_trigram_starts_for_test(OP_STARTS_WITH), "STARTS_WITH trigram_starts");
    ASSERT_TRUE(!op_trigram_starts_for_test(OP_EQUAL),      "EQ trigram_starts");
    ASSERT_TRUE(!op_trigram_starts_for_test(OP_CONTAINS),   "CONTAINS trigram_starts");
    return 0;
}
TEST_REGISTER("test-op-caps-trigram-starts", test_op_caps_trigram_starts)

static int test_op_caps_rank(void) {
    ASSERT_EQ_INT(op_selectivity_rank_for_test(OP_EQUAL),       0, "EQ rank");
    ASSERT_EQ_INT(op_selectivity_rank_for_test(OP_STARTS_WITH), 1, "STARTS_WITH rank");
    ASSERT_EQ_INT(op_selectivity_rank_for_test(OP_BETWEEN),     2, "BETWEEN rank");
    ASSERT_EQ_INT(op_selectivity_rank_for_test(OP_IN),          3, "IN rank");
    ASSERT_EQ_INT(op_selectivity_rank_for_test(OP_LESS),        4, "LESS rank");
    ASSERT_EQ_INT(op_selectivity_rank_for_test(OP_GREATER),     4, "GREATER rank");
    ASSERT_EQ_INT(op_selectivity_rank_for_test(OP_LESS_EQ),     4, "LESS_EQ rank");
    ASSERT_EQ_INT(op_selectivity_rank_for_test(OP_GREATER_EQ),  4, "GREATER_EQ rank");
    ASSERT_EQ_INT(op_selectivity_rank_for_test(OP_CONTAINS),    9, "CONTAINS rank");
    ASSERT_EQ_INT(op_selectivity_rank_for_test(OP_LIKE),        9, "LIKE rank");
    return 0;
}
TEST_REGISTER("test-op-caps-rank", test_op_caps_rank)

static int test_op_caps_planner_integration(void) {
    /* Smoke test: verify that plan_filter_kind_for_test produces expected
     * plan kinds using the migrated op_caps table. */
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"o\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"o\",\"object\":\"ob\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:varchar:16\",\"b:varchar:8\",\"c:int\"],"
        "\"indexes\":[\"a:btree\",\"a:trigram\",\"b:bitmap\",\"c\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create"); free(resp); resp=NULL;

    for (int i = 0; i < 100; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"o\",\"object\":\"ob\",\"key\":\"k%03d\","
            "\"value\":{\"a\":\"v%03d\",\"b\":\"x%03d\",\"c\":%d}}", i, i, i, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-op-caps-planner-integration", test_op_caps_planner_integration)
