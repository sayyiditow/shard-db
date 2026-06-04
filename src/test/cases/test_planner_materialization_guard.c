/* Broad bitmap-AND must NOT materialize a prefilter keyset (gap D). With the
 * cap lowered, a broad `a=x AND b=y` intersection should be skipped (NULL →
 * walk), while a selective one still materializes. See
 * docs/plans/2026-06-04-planner-materialization-guard.md. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

extern int plan_keyset_materializes_for_test(const char *db_root, const char *object,
                                             const char *criteria_json);
extern void set_ordered_find_keyset_max_for_test(size_t v);

static int test_planner_materialization_guard(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"m\"}", &resp); free(resp); resp=NULL;
    /* score:int with btree so the cursor find (correctness) can order by it. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"m\",\"object\":\"ob\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:varchar:8\",\"b:varchar:8\",\"score:int\"],"
        "\"indexes\":[\"a:bitmap\",\"b:bitmap\",\"score\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create ob"); free(resp); resp=NULL;

    /* 300 rows: a=x for all; b=y for 290 (broad), b=z for 10 (selective). */
    for (int i = 0; i < 300; i++) {
        char req[256];
        const char *b = (i < 10) ? "z" : "y";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"m\",\"object\":\"ob\",\"key\":\"k%03d\","
            "\"value\":{\"a\":\"x\",\"b\":\"%s\",\"score\":%d}}", i, b, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    /* Lower the cap so 290 counts as "broad". */
    set_ordered_find_keyset_max_for_test(50);

    /* Broad: a=x (300) AND b=y (290) → intersection 290 > 50 → must SKIP. */
    ASSERT_EQ_INT(plan_keyset_materializes_for_test(env.db_root, "m/ob",
        "[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"x\"},{\"field\":\"b\",\"op\":\"eq\",\"value\":\"y\"}]"),
        0, "broad bitmap AND skips materialization (returns NULL)");

    /* Selective: a=x (300) AND b=z (10) → intersection 10 <= 50 → materializes. */
    ASSERT_EQ_INT(plan_keyset_materializes_for_test(env.db_root, "m/ob",
        "[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"x\"},{\"field\":\"b\",\"op\":\"eq\",\"value\":\"z\"}]"),
        1, "selective bitmap AND still materializes");

    /* Correctness: the selective AND still returns the right rows via a real count. */
    set_ordered_find_keyset_max_for_test(100000);  /* restore */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"m\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"x\"},"
        "{\"field\":\"b\",\"op\":\"eq\",\"value\":\"z\"}]}", &resp);
    ASSERT_CONTAINS(resp, "10", "selective AND count = 10");
    free(resp); resp=NULL;
    /* Broad AND find returns correct rows (walk path) with order_by on score btree. */
    set_ordered_find_keyset_max_for_test(50);
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"m\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"x\"},"
        "{\"field\":\"b\",\"op\":\"eq\",\"value\":\"y\"}],"
        "\"order_by\":\"score\",\"order\":\"asc\",\"limit\":3,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"rows\":", "broad AND find still returns rows (walk path)");
    free(resp); resp=NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-planner-materialization-guard", test_planner_materialization_guard)
