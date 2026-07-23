/* Composite walk must fold an order_by range bound into the seek so it
   returns the correct window in the correct order — the comment-thread
   pagination shape: story_root=X AND time {>=|<=} T ORDER BY time {asc|desc}. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <string.h>
#include <stdlib.h>

static int test_range_fold_window(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"c\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"c\",\"object\":\"cm\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"story_root:long\",\"time:datetime\"],"
        "\"indexes\":[\"story_root\",\"time\",\"story_root+time\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create cm"); free(resp); resp=NULL;

    /* story_root=100 with comments on days 1..10; story_root=200 decoy. */
    for (int d = 1; d <= 10; d++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"c\",\"object\":\"cm\",\"key\":\"a%02d\","
            "\"value\":{\"story_root\":100,\"time\":\"2026-05-%02d 00:00:00\"}}", d, d);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"c\",\"object\":\"cm\",\"key\":\"z1\","
        "\"value\":{\"story_root\":200,\"time\":\"2026-05-05 00:00:00\"}}", &resp);
    free(resp); resp=NULL;

    /* Newer page: time>=day5 ASC limit 3 -> a05,a06,a07 in that order. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"c\",\"object\":\"cm\","
        "\"criteria\":[{\"field\":\"story_root\",\"op\":\"eq\",\"value\":100},"
        " {\"field\":\"time\",\"op\":\"gte\",\"value\":\"2026-05-05 00:00:00\"}],"
        "\"order_by\":\"time\",\"order\":\"asc\",\"limit\":3}", &resp);
    ASSERT_NOT_NULL(resp, "asc page");
    {
        const char *p5=SAFE_STRSTR(resp,"a05"), *p6=SAFE_STRSTR(resp,"a06"), *p7=SAFE_STRSTR(resp,"a07");
        ASSERT_TRUE(p5 && p6 && p7 && p5<p6 && p6<p7, "asc: a05,a06,a07 in order");
        ASSERT_TRUE(!SAFE_STRSTR(resp,"a04") && !SAFE_STRSTR(resp,"a08"), "asc: window excludes a04/a08");
        ASSERT_TRUE(!SAFE_STRSTR(resp,"z1"), "asc: decoy story_root excluded");
    }
    free(resp); resp=NULL;

    /* Older page: time<=day5 DESC limit 3 -> a05,a04,a03 in that order. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"c\",\"object\":\"cm\","
        "\"criteria\":[{\"field\":\"story_root\",\"op\":\"eq\",\"value\":100},"
        " {\"field\":\"time\",\"op\":\"lte\",\"value\":\"2026-05-05 00:00:00\"}],"
        "\"order_by\":\"time\",\"order\":\"desc\",\"limit\":3}", &resp);
    ASSERT_NOT_NULL(resp, "desc page");
    {
        const char *p5=SAFE_STRSTR(resp,"a05"), *p4=SAFE_STRSTR(resp,"a04"), *p3=SAFE_STRSTR(resp,"a03");
        ASSERT_TRUE(p5 && p4 && p3 && p5<p4 && p4<p3, "desc: a05,a04,a03 in order");
        ASSERT_TRUE(!SAFE_STRSTR(resp,"a06") && !SAFE_STRSTR(resp,"a02"), "desc: window excludes a06/a02");
    }
    free(resp); resp=NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-range-fold-window", test_range_fold_window)

/* Regression: seed with NO order_by range leaf still returns all of the
   prefix in order (whole-prefix walk unaffected). */
static int test_range_fold_no_bound(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"c2\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"c2\",\"object\":\"cm\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"story_root:long\",\"time:datetime\"],"
        "\"indexes\":[\"story_root\",\"time\",\"story_root+time\"]}", &resp);
    free(resp); resp=NULL;
    for (int d = 1; d <= 4; d++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"c2\",\"object\":\"cm\",\"key\":\"b%02d\","
            "\"value\":{\"story_root\":7,\"time\":\"2026-05-%02d 00:00:00\"}}", d, d);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"c2\",\"object\":\"cm\","
        "\"criteria\":[{\"field\":\"story_root\",\"op\":\"eq\",\"value\":7}],"
        "\"order_by\":\"time\",\"order\":\"asc\",\"limit\":50}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp,"b01") && SAFE_STRSTR(resp,"b04"), "no-bound: all rows returned");
    free(resp); resp=NULL;
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-range-fold-no-bound", test_range_fold_no_bound)
