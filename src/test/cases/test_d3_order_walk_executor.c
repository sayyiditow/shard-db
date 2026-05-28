/* test_d3_order_walk_executor.c
 *
 * Phase 1c step 4/6: D3 order-index walk executor.
 *
 * Verifies that find {score>50} order_by time [asc|desc] limit N
 * returns the correct N rows in sorted order when only a plain "time"
 * btree index exists (no composite) — exercising the new
 * find_via_order_index_walk() executor path.
 *
 * Object: cm_d3exec
 *   fields: score:int (indexed), time:long (indexed)
 *   NO composite index
 *   50 rows with score=100, time=i         (i=0..49)   → all match score>50
 *   30 rows with score=10,  time=i+1000    (i=0..29)   → none match score>50
 *
 * Planner assertions: plan_filter_kind_for_test must return kind=="leaf"
 * and order=="walk" before each find — confirms D3 executor is exercised.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── helpers ─────────────────────────────────────────────────── */

static char *do_req(TestClient *tc, const char *json) {
    char *resp = NULL;
    tc_request(tc, json, &resp);
    return resp ? resp : strdup("{\"error\":\"no response\"}");
}

static int has(const char *haystack, const char *needle) {
    return haystack && needle && strstr(haystack, needle) != NULL;
}

static int pos_of(const char *haystack, const char *needle) {
    if (!haystack || !needle) return -1;
    const char *p = strstr(haystack, needle);
    return p ? (int)(p - haystack) : -1;
}

/* ── planner assertion helper ────────────────────────────────── */

/* plan_filter_kind_for_test is exposed in TEST_BUILD.  We declare it
   here; the linker resolves it from query.c compiled with -DTEST_BUILD. */
extern const char *plan_filter_kind_for_test(
        const char *db_root, const char *object,
        const char *criteria_json, const char *order_by, int fetching,
        char *out_field, size_t fsz, char *out_order, size_t osz,
        int *out_total_cheap);

/* ── fixture setup ───────────────────────────────────────────── */

/* Create object cm_d3exec: score:int indexed, time:long indexed.
   NO composite index.  Insert:
     s0..s49  → score=100, time=i        (matches score>50)
     n0..n29  → score=10,  time=i+1000   (never matches score>50)
*/
static TestClient *d3_setup(TestEnv *env) {
    if (test_env_start(env) != 0) { ASSERT_TRUE(0, "daemon spawn"); return NULL; }
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(env); return NULL; }

    char *r = do_req(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}"); free(r);
    r = do_req(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cm_d3exec\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"score:int\",\"time:long\"],"
        "\"indexes\":[\"score\",\"time\"]}");
    free(r);

    /* Build bulk-insert: 50 score=100 rows + 30 score=10 rows. */
    char body[65536]; int p = 0;
    p += snprintf(body+p, sizeof(body)-p, "{");
    /* score=100 rows: s0..s49, time=0..49 */
    for (int i = 0; i < 50; i++) {
        p += snprintf(body+p, sizeof(body)-p,
            "%s\"s%d\":{\"score\":100,\"time\":%d}",
            i == 0 ? "" : ",", i, i);
    }
    /* score=10 rows: n0..n29, time=1000..1029 */
    for (int i = 0; i < 30; i++) {
        p += snprintf(body+p, sizeof(body)-p,
            ",\"n%d\":{\"score\":10,\"time\":%d}", i, 1000+i);
    }
    p += snprintf(body+p, sizeof(body)-p, "}");

    char req[65600];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"cm_d3exec\","
        "\"records\":%s}", body);
    r = do_req(tc, req); free(r);
    return tc;
}

/* ── test cases ──────────────────────────────────────────────── */

/* D3 ASC: find {score>50} order_by time asc limit 5
   Expected: s0(t=0), s1(t=1), s2(t=2), s3(t=3), s4(t=4) in that order.
   Planner must report kind="leaf" order="walk". */
static int test_d3_order_walk_asc(void) {
    TestEnv env = {0};
    TestClient *tc = d3_setup(&env);
    if (!tc) return 1;

    /* Planner assertion: must pick D3 (walk). */
    char out_order[32] = {0};
    char out_field[64] = {0};
    const char *kind = plan_filter_kind_for_test(
        env.db_root, "default/cm_d3exec",
        "[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}]",
        "time", 1,
        out_field, sizeof(out_field),
        out_order, sizeof(out_order),
        NULL);
    ASSERT_EQ_STR(kind,      "leaf", "D3 asc: planner kind=leaf");
    ASSERT_EQ_STR(out_order, "walk", "D3 asc: planner order=walk");

    char *r = do_req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cm_d3exec\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}],"
        "\"order_by\":\"time\",\"order\":\"asc\",\"limit\":5}");

    /* Must contain the 5 rows with the LOWEST time values that match score>50. */
    ASSERT_CONTAINS(r, "\"key\":\"s0\"", "D3 asc: s0 present");
    ASSERT_CONTAINS(r, "\"key\":\"s1\"", "D3 asc: s1 present");
    ASSERT_CONTAINS(r, "\"key\":\"s2\"", "D3 asc: s2 present");
    ASSERT_CONTAINS(r, "\"key\":\"s3\"", "D3 asc: s3 present");
    ASSERT_CONTAINS(r, "\"key\":\"s4\"", "D3 asc: s4 present");

    /* s5 is row 6 — beyond limit=5. */
    ASSERT_TRUE(!has(r, "\"key\":\"s5\""), "D3 asc: s5 absent (limit=5)");

    /* No score=10 (n-prefixed) rows. */
    ASSERT_TRUE(!has(r, "\"key\":\"n"),   "D3 asc: no n-rows");

    /* Order: s0 before s1, s1 before s2, etc. */
    ASSERT_TRUE(pos_of(r, "\"s0\"") < pos_of(r, "\"s1\""), "D3 asc: s0 < s1");
    ASSERT_TRUE(pos_of(r, "\"s1\"") < pos_of(r, "\"s2\""), "D3 asc: s1 < s2");
    ASSERT_TRUE(pos_of(r, "\"s2\"") < pos_of(r, "\"s3\""), "D3 asc: s2 < s3");
    ASSERT_TRUE(pos_of(r, "\"s3\"") < pos_of(r, "\"s4\""), "D3 asc: s3 < s4");

    ASSERT_TRUE(!has(r, "\"error\""), "D3 asc: no error");

    free(r);
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-d3-order-walk-asc", test_d3_order_walk_asc)

/* D3 DESC: find {score>50} order_by time desc limit 5
   Expected: s49(t=49), s48(t=48), s47(t=47), s46(t=46), s45(t=45).
   Planner must report kind="leaf" order="walk". */
static int test_d3_order_walk_desc(void) {
    TestEnv env = {0};
    TestClient *tc = d3_setup(&env);
    if (!tc) return 1;

    /* Planner assertion. */
    char out_order[32] = {0};
    char out_field[64] = {0};
    const char *kind = plan_filter_kind_for_test(
        env.db_root, "default/cm_d3exec",
        "[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}]",
        "time", 1,
        out_field, sizeof(out_field),
        out_order, sizeof(out_order),
        NULL);
    ASSERT_EQ_STR(kind,      "leaf", "D3 desc: planner kind=leaf");
    ASSERT_EQ_STR(out_order, "walk", "D3 desc: planner order=walk");

    char *r = do_req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cm_d3exec\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}],"
        "\"order_by\":\"time\",\"order\":\"desc\",\"limit\":5}");

    /* 5 rows with the LARGEST time among score>50 matches. */
    ASSERT_CONTAINS(r, "\"key\":\"s49\"", "D3 desc: s49 present");
    ASSERT_CONTAINS(r, "\"key\":\"s48\"", "D3 desc: s48 present");
    ASSERT_CONTAINS(r, "\"key\":\"s47\"", "D3 desc: s47 present");
    ASSERT_CONTAINS(r, "\"key\":\"s46\"", "D3 desc: s46 present");
    ASSERT_CONTAINS(r, "\"key\":\"s45\"", "D3 desc: s45 present");

    /* s44 is row 6 in desc — beyond limit. */
    ASSERT_TRUE(!has(r, "\"key\":\"s44\""), "D3 desc: s44 absent (limit=5)");

    /* No score=10 rows. */
    ASSERT_TRUE(!has(r, "\"key\":\"n"),    "D3 desc: no n-rows");

    /* Order: s49 before s48, s48 before s47, etc. */
    ASSERT_TRUE(pos_of(r, "\"s49\"") < pos_of(r, "\"s48\""), "D3 desc: s49 < s48");
    ASSERT_TRUE(pos_of(r, "\"s48\"") < pos_of(r, "\"s47\""), "D3 desc: s48 < s47");
    ASSERT_TRUE(pos_of(r, "\"s47\"") < pos_of(r, "\"s46\""), "D3 desc: s47 < s46");
    ASSERT_TRUE(pos_of(r, "\"s46\"") < pos_of(r, "\"s45\""), "D3 desc: s46 < s45");

    ASSERT_TRUE(!has(r, "\"error\""), "D3 desc: no error");

    free(r);
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-d3-order-walk-desc", test_d3_order_walk_desc)

/* D3 OFFSET: find {score>50} order_by time asc offset=5 limit=10
   Expected: s5(t=5)..s14(t=14) — s0..s4 skipped, s15 beyond limit.
   Planner must report kind="leaf" order="walk". */
static int test_d3_order_walk_offset(void) {
    TestEnv env = {0};
    TestClient *tc = d3_setup(&env);
    if (!tc) return 1;

    /* Planner assertion. */
    char out_order[32] = {0};
    char out_field[64] = {0};
    const char *kind = plan_filter_kind_for_test(
        env.db_root, "default/cm_d3exec",
        "[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}]",
        "time", 1,
        out_field, sizeof(out_field),
        out_order, sizeof(out_order),
        NULL);
    ASSERT_EQ_STR(kind,      "leaf", "D3 offset: planner kind=leaf");
    ASSERT_EQ_STR(out_order, "walk", "D3 offset: planner order=walk");

    char *r = do_req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cm_d3exec\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}],"
        "\"order_by\":\"time\",\"order\":\"asc\","
        "\"offset\":5,\"limit\":10}");

    /* Rows s5..s14 must be present. */
    ASSERT_CONTAINS(r, "\"key\":\"s5\"",  "D3 offset: s5 present");
    ASSERT_CONTAINS(r, "\"key\":\"s14\"", "D3 offset: s14 present");

    /* s0..s4 skipped by offset=5. */
    ASSERT_TRUE(!has(r, "\"key\":\"s0\""), "D3 offset: s0 absent (skipped)");
    ASSERT_TRUE(!has(r, "\"key\":\"s4\""), "D3 offset: s4 absent (skipped)");

    /* s15 beyond limit=10. */
    ASSERT_TRUE(!has(r, "\"key\":\"s15\""), "D3 offset: s15 absent (beyond limit)");

    /* No n-rows. */
    ASSERT_TRUE(!has(r, "\"key\":\"n"),    "D3 offset: no n-rows");

    ASSERT_TRUE(!has(r, "\"error\""), "D3 offset: no error");

    free(r);
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-d3-order-walk-offset", test_d3_order_walk_offset)
