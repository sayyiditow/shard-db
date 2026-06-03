/* Composite-prefix routing: a query of the shape
   `eq_field = V AND range_field >= T  ORDER BY range_field`
   must select FP_ORDER_COMPOSITE driven by eq_field when an
   `eq_field+range_field` composite exists — even though the
   selectivity seed would otherwise be the range_field btree. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <string.h>

/* Plan introspection hook (defined in query.c). */
extern const char *plan_filter_kind_for_test(
    const char *db_root, const char *object,
    const char *criteria_json, const char *order_by, int fetching,
    char *out_field, size_t fsz, char *out_order, size_t osz,
    int *out_total_cheap);

static int test_composite_prefix_selected(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d\"}", &resp);
    free(resp); resp = NULL;

    /* type: varchar bitmap (low cardinality); time: long btree;
       composite type+time. Mirrors hn/stories. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"st\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"type:varchar:16\",\"time:long\"],"
        "\"indexes\":[\"type:bitmap\",\"time\",\"type+time\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create st");
    free(resp); resp = NULL;

    /* A few rows so get_live_count > 0 (planner reads N). */
    for (int i = 0; i < 6; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"st\",\"key\":\"k%d\","
            "\"value\":{\"type\":\"%s\",\"time\":%d}}",
            i, (i % 3 == 0 ? "job" : "story"), i + 1);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    char field[64] = {0}, order[32] = {0}; int cheap = -1;
    const char *kind = plan_filter_kind_for_test(
        env.db_root, "d/st",
        "[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"job\"},"
        " {\"field\":\"time\",\"op\":\"gte\",\"value\":\"1\"}]",
        "time", 1 /* fetching */,
        field, sizeof(field), order, sizeof(order), &cheap);

    ASSERT_EQ_STR(order, "composite", "order mode is composite");
    ASSERT_EQ_STR(field, "type",      "composite seed is type (not time)");
    ASSERT_EQ_STR(kind,  "leaf",      "kind is PRIMARY_LEAF for composite drive");

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-composite-prefix-selected", test_composite_prefix_selected)

static int test_composite_prefix_results_correct(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d2\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d2\",\"object\":\"st\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"type:varchar:16\",\"time:long\"],"
        "\"indexes\":[\"type:bitmap\",\"time\",\"type+time\"]}", &resp);
    free(resp); resp = NULL;

    /* 12 rows: jobs at time=3,6,9; stories at other times. */
    for (int i = 1; i <= 12; i++) {
        char req[256];
        const char *t = (i == 3 || i == 6 || i == 9) ? "job" : "story";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d2\",\"object\":\"st\",\"key\":\"k%02d\","
            "\"value\":{\"type\":\"%s\",\"time\":%d}}",
            i, t, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* type=job ORDER BY time DESC → expect the 3 jobs newest-first: k09,k06,k03. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d2\",\"object\":\"st\","
        "\"criteria\":[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"job\"}],"
        "\"order_by\":\"time\",\"order\":\"desc\",\"limit\":25}", &resp);
    ASSERT_NOT_NULL(resp, "find returned");
    ASSERT_TRUE(strstr(resp, "k09") && strstr(resp, "k06") && strstr(resp, "k03"),
                "all 3 jobs returned");
    ASSERT_TRUE(!strstr(resp, "k01") && !strstr(resp, "k02"),
                "no stories leaked into job results");
    /* DESC order: k09 must appear before k06 before k03 in the payload. */
    {
        const char *p9 = strstr(resp, "k09");
        const char *p6 = strstr(resp, "k06");
        const char *p3 = strstr(resp, "k03");
        ASSERT_TRUE(p9 && p6 && p3 && p9 < p6 && p6 < p3,
                    "results are time-desc ordered");
    }
    free(resp); resp = NULL;

    /* Add the range sibling: type=job AND time>=5 → only k09,k06 (k03<5 excluded). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d2\",\"object\":\"st\","
        "\"criteria\":[{\"field\":\"type\",\"op\":\"eq\",\"value\":\"job\"},"
        " {\"field\":\"time\",\"op\":\"gte\",\"value\":\"5\"}],"
        "\"order_by\":\"time\",\"order\":\"desc\",\"limit\":25}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "k09") && strstr(resp, "k06"),
                "range: k09,k06 present");
    ASSERT_TRUE(resp && !strstr(resp, "k03"),
                "range: k03 (before cutoff) excluded by post-filter");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-composite-prefix-results-correct", test_composite_prefix_results_correct)

static int test_exact_composite_selected(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d3\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d3\",\"object\":\"pexact\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"by:varchar:32\",\"time:long\"],"
        "\"indexes\":[\"by\",\"time\",\"by+time\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create pexact"); free(resp); resp=NULL;
    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d3\",\"object\":\"pexact\",\"key\":\"k%d\","
            "\"value\":{\"by\":\"alice\",\"time\":%d}}", i, i+1);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
    char field[64]={0}, order[32]={0}; int cheap=-1;
    plan_filter_kind_for_test(env.db_root, "d3/pexact",
        "[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"alice\"},"
        " {\"field\":\"time\",\"op\":\"eq\",\"value\":\"2\"}]",
        NULL /* no order_by */, 1, field, sizeof(field), order, sizeof(order), &cheap);
    ASSERT_EQ_STR(order, "composite_exact", "by=X AND time=Y → exact composite");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-exact-composite-selected", test_exact_composite_selected)

static int test_exact_composite_results(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d4\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d4\",\"object\":\"pexactr\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"by:varchar:32\",\"time:long\"],"
        "\"indexes\":[\"by\",\"time\",\"by+time\"]}", &resp);
    free(resp); resp=NULL;
    /* alice@day2 is the target; decoys share one field but not both. */
    const char *rows[][3] = {
        {"k1","alice","2"},  /* match */
        {"k2","alice","3"},  /* same by, diff time */
        {"k3","bob",  "2"},  /* same time, diff by */
        {"k4","alice","2"},  /* match (dup by+time) */
    };
    for (int i=0;i<4;i++){ char req[256];
        snprintf(req,sizeof(req),
          "{\"mode\":\"insert\",\"dir\":\"d4\",\"object\":\"pexactr\",\"key\":\"%s\","
          "\"value\":{\"by\":\"%s\",\"time\":%s}}", rows[i][0],rows[i][1],rows[i][2]);
        tc_request(tc,req,&resp); free(resp); resp=NULL; }

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d4\",\"object\":\"pexactr\","
        "\"criteria\":[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"alice\"},"
        " {\"field\":\"time\",\"op\":\"eq\",\"value\":\"2\"}]}", &resp);
    ASSERT_NOT_NULL(resp, "find returned");
    ASSERT_TRUE(strstr(resp,"k1") && strstr(resp,"k4"), "both alice@day2 rows returned");
    ASSERT_TRUE(!strstr(resp,"k2") && !strstr(resp,"k3"), "decoys excluded");
    free(resp); resp=NULL;
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-exact-composite-results", test_exact_composite_results)

/* Without a by+time composite, by=X AND time=Y must NOT select composite_exact. */
static int test_exact_composite_no_composite(void) {
    TestEnv env; TestClient *tc; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d5\"}", &resp); free(resp); resp=NULL;
    /* Only single-field indexes — no by+time composite. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d5\",\"object\":\"pnc\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"by:varchar:32\",\"time:long\"],"
        "\"indexes\":[\"by\",\"time\"]}", &resp);
    free(resp); resp=NULL;
    for (int i = 0; i < 3; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d5\",\"object\":\"pnc\",\"key\":\"k%d\","
            "\"value\":{\"by\":\"alice\",\"time\":%d}}", i, i+1);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
    char order[32]={0}; int cheap=-1;
    plan_filter_kind_for_test(env.db_root, "d5/pnc",
        "[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"alice\"},"
        " {\"field\":\"time\",\"op\":\"eq\",\"value\":\"1\"}]",
        NULL, 1, NULL, 0, order, sizeof(order), &cheap);
    ASSERT_TRUE(strcmp(order, "composite_exact") != 0, "no composite → not composite_exact");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-exact-composite-no-composite", test_exact_composite_no_composite)
