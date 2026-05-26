#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "../../db/types.h"
#include "../test_client.h"
#include "../fixtures.h"
#include <stdlib.h>
#include <string.h>

/* The heap helpers are declared `static` in query.c but exposed to
 * tests via the TOPN_VIS macro when TEST_BUILD is set. */
extern void *topn_heap_new(int cap, int order_desc);
extern void  topn_heap_destroy(void *h);
extern int   topn_heap_size(void *h);
extern int   topn_heap_offer(void *h, double metric,
                              const char *gk, size_t gklen,
                              int64_t count, double sum,
                              double min, double max);
extern int   topn_heap_drain(void *h, double *metrics_out,
                              char **gkeys_out, size_t *gklens_out,
                              int64_t *counts_out, double *sums_out,
                              double *mins_out, double *maxs_out);

/* Forward decl of AggSpec — test's version must be compatible on
 * the first three fields (fn, field, alias) which are the only ones
 * eligible_for_topn_stream reads. */
typedef struct {
    int  fn;                /* enum AggFn from query.c */
    char field[256];
    char alias[256];
} TestAggSpec;

extern int eligible_for_topn_stream(
    const char *db_root, const char *object,
    const TestAggSpec *specs, int nspecs,
    const char *group_by_csv,
    const char *order_by_alias,
    int limit,
    const char *having);

static int test_topn_heap_basic_desc(void) {
    void *h = topn_heap_new(3, 1 /* desc */);
    ASSERT_TRUE(h != NULL, "heap created");
    ASSERT_EQ_INT(topn_heap_size(h), 0, "size starts 0");

    topn_heap_offer(h, 10, "a", 1, 10, 0, 0, 0);
    topn_heap_offer(h, 50, "b", 1, 50, 0, 0, 0);
    topn_heap_offer(h, 30, "c", 1, 30, 0, 0, 0);
    topn_heap_offer(h, 20, "d", 1, 20, 0, 0, 0);
    topn_heap_offer(h, 40, "e", 1, 40, 0, 0, 0);

    ASSERT_EQ_INT(topn_heap_size(h), 3, "size capped at 3");

    double metrics[3]; char *gks[3]; size_t gklens[3];
    int64_t counts[3]; double sums[3]; double mins[3]; double maxs[3];
    int n = topn_heap_drain(h, metrics, gks, gklens, counts, sums, mins, maxs);
    ASSERT_EQ_INT(n, 3, "drain returns 3");

    ASSERT_TRUE(metrics[0] == 50.0, "drain[0] = 50");
    ASSERT_TRUE(metrics[1] == 40.0, "drain[1] = 40");
    ASSERT_TRUE(metrics[2] == 30.0, "drain[2] = 30");

    for (int i = 0; i < n; i++) free(gks[i]);
    topn_heap_destroy(h);
    return 0;
}

static int test_topn_heap_basic_asc(void) {
    void *h = topn_heap_new(3, 0 /* asc */);
    topn_heap_offer(h, 10, "a", 1, 10, 0, 0, 0);
    topn_heap_offer(h, 50, "b", 1, 50, 0, 0, 0);
    topn_heap_offer(h, 30, "c", 1, 30, 0, 0, 0);
    topn_heap_offer(h, 20, "d", 1, 20, 0, 0, 0);
    topn_heap_offer(h, 40, "e", 1, 40, 0, 0, 0);

    double metrics[3]; char *gks[3]; size_t gklens[3];
    int64_t counts[3]; double sums[3]; double mins[3]; double maxs[3];
    int n = topn_heap_drain(h, metrics, gks, gklens, counts, sums, mins, maxs);
    ASSERT_EQ_INT(n, 3, "drain returns 3");

    ASSERT_TRUE(metrics[0] == 10.0, "drain[0] = 10");
    ASSERT_TRUE(metrics[1] == 20.0, "drain[1] = 20");
    ASSERT_TRUE(metrics[2] == 30.0, "drain[2] = 30");

    for (int i = 0; i < n; i++) free(gks[i]);
    topn_heap_destroy(h);
    return 0;
}

static int test_topn_heap_under_cap(void) {
    void *h = topn_heap_new(10, 1);
    topn_heap_offer(h, 10, "a", 1, 10, 0, 0, 0);
    topn_heap_offer(h, 50, "b", 1, 50, 0, 0, 0);
    topn_heap_offer(h, 30, "c", 1, 30, 0, 0, 0);

    ASSERT_EQ_INT(topn_heap_size(h), 3, "size = 3 (under cap)");

    double metrics[10]; char *gks[10]; size_t gklens[10];
    int64_t counts[10]; double sums[10]; double mins[10]; double maxs[10];
    int n = topn_heap_drain(h, metrics, gks, gklens, counts, sums, mins, maxs);
    ASSERT_EQ_INT(n, 3, "drain returns all 3");
    ASSERT_TRUE(metrics[0] == 50.0 && metrics[1] == 30.0 && metrics[2] == 10.0,
                "under-cap drain still sorted desc");

    for (int i = 0; i < n; i++) free(gks[i]);
    topn_heap_destroy(h);
    return 0;
}

TEST_REGISTER("test-topn-heap-basic-desc", test_topn_heap_basic_desc)
TEST_REGISTER("test-topn-heap-basic-asc", test_topn_heap_basic_asc)
TEST_REGISTER("test-topn-heap-under-cap", test_topn_heap_under_cap)

/* ========== Eligibility detector tests ========== */

static int test_topn_eligible_truth_table(void) {
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
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp);
    resp = NULL;

    /* Create object with fields + immediately-declared indexes. This creates
       index.conf with the index entries at object creation time, before any
       inserts trigger btree file allocation. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"topn_elig\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"name:varchar:32\",\"score:int\",\"score_unindexed:int\"],"
        "\"indexes\":[\"name\",\"score\"]}", &resp);
    free(resp);
    resp = NULL;

    /* Insert one record to trigger btree files. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"topn_elig\",\"key\":\"k1\",\"value\":{\"name\":\"a\",\"score\":1,\"score_unindexed\":0}}", &resp);
    free(resp); resp = NULL;

    /* AGG_COUNT enum value — from query.c enum AggFn { AGG_COUNT=0, ... } */
    const int AGG_COUNT_VAL = 0;
    const int AGG_SUM_VAL = 1;

    /* Verify db_root + object exist */
    ASSERT_NOT_NULL(env.db_root, "env.db_root is set");

    /* The shape checks: eligibility gating on input shape alone */

    /* The object path includes the dir: "default/topn_elig" */
    const char *obj_path = "default/topn_elig";

    /* Case A: minimal eligible shape - count() + single-field group_by + limit */
    TestAggSpec spec_a;
    spec_a.fn = AGG_COUNT_VAL;
    spec_a.field[0] = '\0';
    strcpy(spec_a.alias, "n");
    int r = eligible_for_topn_stream(env.db_root, obj_path,
                                      &spec_a, 1, "name", "n", 20, NULL);
    ASSERT_EQ_INT(r, 1, "Case A: count + indexed group_by + order_by agg alias + limit → ELIGIBLE");

    /* Case B: no limit → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "n", 0, NULL);
    ASSERT_EQ_INT(r, 0, "Case B: no limit (0) → NOT ELIGIBLE");

    /* Case C: order_by on group_by field instead of agg alias → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "name", 20, NULL);
    ASSERT_EQ_INT(r, 0, "Case C: order_by on group_by field → NOT ELIGIBLE");

    /* Case D: group_by on un-indexed field → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "score_unindexed", "n", 20, NULL);
    ASSERT_EQ_INT(r, 0, "Case D: un-indexed group_by → NOT ELIGIBLE");

    /* Case E: multi-field group_by → NOT ELIGIBLE in Phase 1 */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name,score", "n", 20, NULL);
    ASSERT_EQ_INT(r, 0, "Case E: multi-field group_by (Phase 1 limit) → NOT ELIGIBLE");

    /* Case F: sum on group_by field itself + order_by on sum alias → ELIGIBLE */
    TestAggSpec spec_f;
    spec_f.fn = AGG_SUM_VAL;
    strcpy(spec_f.field, "score");  /* sum on the group_by field itself */
    strcpy(spec_f.alias, "total");
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_f, 1, "score", "total", 10000, NULL);
    ASSERT_EQ_INT(r, 1, "Case F: sum on group_by field + limit=max → ELIGIBLE");

    /* Case G: limit > 10000 → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "n", 10001, NULL);
    ASSERT_EQ_INT(r, 0, "Case G: limit > 10000 → NOT ELIGIBLE");

    /* Case H: sum on different field → NOT ELIGIBLE in Phase 1 */
    TestAggSpec spec_h;
    spec_h.fn = AGG_SUM_VAL;
    strcpy(spec_h.field, "score_unindexed");  /* NOT the group_by field */
    strcpy(spec_h.alias, "total");
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_h, 1, "name", "total", 10000, NULL);
    ASSERT_EQ_INT(r, 0, "Case H: agg on non-group_by field (Phase 1 limit) → NOT ELIGIBLE");

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-topn-eligible-truth-table", test_topn_eligible_truth_table)

static int test_topn_stream_count_no_criteria(void) {
    TestEnv env;
    if (test_env_start(&env) != 0) {
        ASSERT_TRUE(0, "test_env_start failed");
        return 1;
    }

    char *resp = NULL;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* 100 unique names × variable counts; author N has count = N+1. Total = 5050. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"topn_e2e\","
        "\"splits\":8,\"max_key\":12,\"fields\":["
        "\"name:varchar:16\",\"score:int\"],"
        "\"indexes\":[\"name\",\"score\"]}", &resp);
    free(resp); resp = NULL;

    /* Build bulk body. */
    size_t body_cap = 1 << 20;
    char *body = malloc(body_cap);
    int p = 0;
    p += snprintf(body + p, body_cap - p, "{");
    int next_id = 0;
    for (int a = 0; a < 100; a++) {
        int n = a + 1;
        for (int j = 0; j < n; j++) {
            p += snprintf(body + p, body_cap - p,
                          "%s\"k%d\":{\"name\":\"author_%03d\",\"score\":%d}",
                          next_id == 0 ? "" : ",", next_id, a, j);
            next_id++;
        }
    }
    p += snprintf(body + p, body_cap - p, "}");

    size_t req_cap = body_cap + 1024;
    char *req = malloc(req_cap);
    snprintf(req, req_cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"topn_e2e\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp);
    free(req); free(body); free(resp); resp = NULL;

    /* Top 10 authors by count desc. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"topn_e2e\","
        "\"group_by\":[\"name\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"order_by\":\"n\",\"order\":\"desc\",\"limit\":10}", &resp);
    ASSERT_TRUE(resp != NULL, "got aggregate response");

    /* Top 10 should be author_099 (100), author_098 (99), ..., author_090 (91). */
    const char *p99 = strstr(resp, "\"author_099\"");
    const char *p98 = strstr(resp, "\"author_098\"");
    const char *p90 = strstr(resp, "\"author_090\"");
    ASSERT_TRUE(p99 != NULL, "author_099 present");
    ASSERT_TRUE(p98 != NULL, "author_098 present");
    ASSERT_TRUE(p90 != NULL, "author_090 present");
    ASSERT_TRUE(p99 != NULL && p98 != NULL && p99 < p98, "099 before 098");
    ASSERT_TRUE(p98 != NULL && p90 != NULL && p98 < p90, "098 before 090");

    free(resp); resp = NULL;
    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-topn-stream-count-no-criteria", test_topn_stream_count_no_criteria)
