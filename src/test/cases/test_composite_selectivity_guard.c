/* The composite must be SKIPPED when the seed prefix is broad and a selective
   sibling exists (type=story AND time>=T ORDER BY score), and USED when the
   seed is selective, when order_by carries the range, or when there's no
   selective sibling. Asserts the chosen order-mode via plan introspection. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

/* Plan introspection hook (defined in query.c). */
extern const char *plan_filter_kind_for_test(
    const char *db_root, const char *object,
    const char *criteria_json, const char *order_by, int fetching,
    char *out_field, size_t fsz, char *out_order, size_t osz,
    int *out_total_cheap);

static const char *order_of(TestEnv *env, const char *crit, const char *order_by) {
    static char order[32];
    char field[64] = {0}; int cheap = -1;
    order[0] = '\0';
    plan_filter_kind_for_test(env->db_root, "g/sg", crit, order_by, 1,
                              field, sizeof(field), order, sizeof(order), &cheap);
    return order;
}

static int test_selectivity_guard(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"g\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"g\",\"object\":\"sg\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"type:varchar:8\",\"status:varchar:8\",\"score:int\"],"
        "\"indexes\":[\"type:bitmap\",\"status:bitmap\",\"score\",\"type+score\",\"status+score\",\"type+status\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create sg"); free(resp); resp=NULL;

    /* Data distribution for selectivity guard:
       N = 1000 total. selectivity_budget(1000) = N/8 = 125.
       900 common records (> 125 → BROAD seed).
       100 rare records (≤ 125 → SELECTIVE sibling). */
    for (int i = 0; i < 900; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"g\",\"object\":\"sg\",\"key\":\"c%04d\","
            "\"value\":{\"type\":\"common\",\"status\":\"common\",\"score\":%d}}",
            i, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
    for (int i = 0; i < 100; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"g\",\"object\":\"sg\",\"key\":\"r%03d\","
            "\"value\":{\"type\":\"rare\",\"status\":\"rare\",\"score\":%d}}",
            i, 900 + i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    /* (a) BROAD seed (type=common, 900 > 125 budget) + SELECTIVE sibling
       (status=rare, 100 ≤ 125) + order by score
       -> SKIP composite (drive on selective status+score composite). */
    ASSERT_TRUE(strcmp(order_of(&env,
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"common\"},"
        " {\"field\":\"status\",\"op\":\"eq\",\"value\":\"rare\"}]",
        "score"), "composite") != 0,
        "broad common + selective rare + order score -> NOT composite");

    /* (b) SELECTIVE seed (type=rare, 100 ≤ 125) + any sibling + order by score
       -> USE composite (seed is already selective). */
    ASSERT_EQ_STR(order_of(&env,
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"rare\"},"
        " {\"field\":\"status\",\"op\":\"eq\",\"value\":\"rare\"}]",
        "score"), "composite",
        "selective type + status + order score -> composite");

    /* (c) BROAD seed (type=common), NO sibling, order by score -> USE composite. */
    ASSERT_EQ_STR(order_of(&env,
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"common\"}]",
        "score"), "composite",
        "broad common, no sibling, order score -> composite");

    /* (d) BROAD seed (type=common) + eq ON order_by field (status=rare) + order by status
       -> USE composite (order_by carries the eq, range-fold obr non-NULL). */
    ASSERT_EQ_STR(order_of(&env,
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"common\"},"
        " {\"field\":\"status\",\"op\":\"eq\",\"value\":\"rare\"}]",
        "status"), "composite",
        "broad common + eq on status + order BY status -> composite");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-composite-selectivity-guard", test_selectivity_guard)
