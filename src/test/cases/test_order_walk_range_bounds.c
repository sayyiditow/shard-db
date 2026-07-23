/* Order-by walk bounds: the walk must start/stop at the order_by-field range
 * criterion instead of scanning the whole index. Regression guard for the 30s
 * `title starts "Ask HN" AND time>=T ORDER BY time` (cursor) timeout. See
 * docs/plans/2026-06-04-order-walk-range-bounds.md. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

extern int order_walk_bounds_for_test(const char *db_root, const char *object,
                                       const char *criteria_json, const char *order_by,
                                       int *out_has_lo, int *out_has_hi);
extern long order_walk_scanned_for_test(void);
extern void order_walk_scanned_reset_for_test(void);

static int test_order_walk_bounds_helper(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"w\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"w\",\"object\":\"ob\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"cat:varchar:8\",\"t:long\"],"
        "\"indexes\":[\"cat\",\"t\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create ob"); free(resp); resp=NULL;

    int has_lo = -1, has_hi = -1;

    /* gte on order_by → lower bound only. */
    order_walk_bounds_for_test(env.db_root, "w/ob",
        "[{\"field\":\"t\",\"op\":\"gte\",\"value\":100}]", "t", &has_lo, &has_hi);
    ASSERT_EQ_INT(has_lo, 1, "gte sets lower bound");
    ASSERT_EQ_INT(has_hi, 0, "gte leaves upper unbounded");

    /* lte on order_by → upper bound only. */
    order_walk_bounds_for_test(env.db_root, "w/ob",
        "[{\"field\":\"t\",\"op\":\"lte\",\"value\":100}]", "t", &has_lo, &has_hi);
    ASSERT_EQ_INT(has_lo, 0, "lte leaves lower unbounded");
    ASSERT_EQ_INT(has_hi, 1, "lte sets upper bound");

    /* range on a DIFFERENT field → no bounds on order_by. */
    order_walk_bounds_for_test(env.db_root, "w/ob",
        "[{\"field\":\"cat\",\"op\":\"eq\",\"value\":\"x\"}]", "t", &has_lo, &has_hi);
    ASSERT_EQ_INT(has_lo, 0, "non-order_by leaf doesn't bound");
    ASSERT_EQ_INT(has_hi, 0, "non-order_by leaf doesn't bound");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-order-walk-bounds-helper", test_order_walk_bounds_helper)

static int test_order_walk_d3_bounded(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"ob\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"cat:varchar:8\",\"t:long\"],"
        "\"indexes\":[\"cat\",\"t\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create ob"); free(resp); resp=NULL;

    /* 1000 rows, all cat=x. t = i. Window t>=980 holds 20 rows (< limit 25). */
    for (int i = 0; i < 1000; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"ob\",\"key\":\"k%04d\","
            "\"value\":{\"cat\":\"x\",\"t\":%d}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    order_walk_scanned_reset_for_test();
    /* cat=x is broad/saturated → D3 order walk on t. Window t>=980 (20 rows). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"cat\",\"op\":\"eq\",\"value\":\"x\"},"
        " {\"field\":\"t\",\"op\":\"gte\",\"value\":980}],"
        "\"order_by\":\"t\",\"order\":\"desc\",\"limit\":25}", &resp);
    /* correctness: exactly the 20 in-window rows, top is t=999. */
    ASSERT_CONTAINS(resp, "\"t\":999", "top in-window row present");
    ASSERT_CONTAINS(resp, "\"t\":980", "boundary in-window row present");
    long scanned = order_walk_scanned_for_test();
    /* bounded: ~20 (window); unbounded would be ~1000. Generous ceiling. */
    ASSERT_TRUE(scanned < 200, "D3 walk bounded to the window, not full index");
    free(resp); resp=NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-order-walk-d3-bounded", test_order_walk_d3_bounded)

static int test_order_walk_cursor_bounded(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"c\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"c\",\"object\":\"ob\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"tag:varchar:8\",\"t:long\"],"
        "\"indexes\":[\"tag\",\"t\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create ob"); free(resp); resp=NULL;

    /* 1000 rows. Only 3 have tag=rare, all in the window t>=990 (sparse: <limit).
     * The other 997 are tag=common spread across t=0..996. */
    for (int i = 0; i < 1000; i++) {
        char req[256];
        const char *tag = (i >= 990 && i < 993) ? "rare" : "common";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"c\",\"object\":\"ob\",\"key\":\"k%04d\","
            "\"value\":{\"tag\":\"%s\",\"t\":%d}}", i, tag, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    order_walk_scanned_reset_for_test();
    /* cursor:null, sparse tag=rare + window t>=985, order by t. Matches=3<limit. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"c\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
        " {\"field\":\"t\",\"op\":\"gte\",\"value\":985}],"
        "\"order_by\":\"t\",\"order\":\"desc\",\"limit\":25,\"cursor\":null}", &resp);
    /* correctness: the 3 rare rows present; the {rows:...} wrapper is fine. */
    ASSERT_CONTAINS(resp, "\"t\":992", "rare row 992 present");
    ASSERT_CONTAINS(resp, "\"t\":990", "rare row 990 present");
    long scanned = order_walk_scanned_for_test();
    /* bounded: walk stops at t=985 (~15 entries); unbounded ran all ~1000. */
    ASSERT_TRUE(scanned < 200, "cursor walk bounded to the window, not full index");
    free(resp); resp=NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-order-walk-cursor-bounded", test_order_walk_cursor_bounded)

/* Highest-risk regression: paginating a WINDOWED query across multiple pages.
 * A bad far-bound could drop rows, repeat them, or run past the window. Walk
 * desc through a 20-row window in 3 pages and verify the sequence is correct,
 * boundary-inclusive at the window low, and terminates. */
static int test_order_walk_cursor_windowed_paging(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"pg\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"pg\",\"object\":\"ob\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"tag:varchar:8\",\"t:long\"],"
        "\"indexes\":[\"tag\",\"t\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create ob"); free(resp); resp=NULL;

    /* t = 0..29, all tag=common (non-sparse). Window t>=10 → 20 in-window rows. */
    for (int i = 0; i < 30; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"pg\",\"object\":\"ob\",\"key\":\"k%02d\","
            "\"value\":{\"tag\":\"common\",\"t\":%d}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    /* PAGE 1: cursor:null, window t>=10, desc, limit 8 → k29..k22. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"pg\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"t\",\"op\":\"gte\",\"value\":10}],"
        "\"order_by\":\"t\",\"order\":\"desc\",\"limit\":8,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k29\"", "page1 top = k29");
    ASSERT_CONTAINS(resp, "\"key\":\"k22\"", "page1 last = k22");
    free(resp); resp=NULL;

    /* PAGE 2: resume from k22 (t=22) → k21..k14. No repeat of k22 (no dupe). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"pg\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"t\",\"op\":\"gte\",\"value\":10}],"
        "\"order_by\":\"t\",\"order\":\"desc\",\"limit\":8,"
        "\"cursor\":{\"t\":\"22\",\"key\":\"k22\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k21\"", "page2 top = k21 (no gap after k22)");
    ASSERT_CONTAINS(resp, "\"key\":\"k14\"", "page2 last = k14");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"key\":\"k22\"") == NULL, "page2 does not repeat k22");
    free(resp); resp=NULL;

    /* PAGE 3: resume from k14 (t=14) → k13..k10, then cursor:null. Must STOP at
       the window low (gte 10 is inclusive → k10 present; k09 must NOT spill). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"pg\",\"object\":\"ob\","
        "\"criteria\":[{\"field\":\"t\",\"op\":\"gte\",\"value\":10}],"
        "\"order_by\":\"t\",\"order\":\"desc\",\"limit\":8,"
        "\"cursor\":{\"t\":\"14\",\"key\":\"k14\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k13\"", "page3 top = k13 (no gap after k14)");
    ASSERT_CONTAINS(resp, "\"key\":\"k10\"", "page3 includes inclusive window-low k10");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"key\":\"k14\"") == NULL, "page3 does not repeat k14");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"key\":\"k09\"") == NULL, "page3 does not spill below window (no k09)");
    ASSERT_CONTAINS(resp, "\"cursor\":null", "pagination terminates at window low");
    free(resp); resp=NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-order-walk-cursor-windowed-paging", test_order_walk_cursor_windowed_paging)
