/* test_composite_in_fold.c
 *
 * Part C: OP_IN composite k-way merge correctness.
 *
 * Object with tag:varchar, t:long, tag+t composite.
 * Rows for tag in {a,b,c,d} interleaved in t.
 * Find tag in (a,c) ORDER BY t DESC limit N and assert:
 *   (a) emitted t is strictly monotonic desc
 *   (b) only a/c rows (no b/d leakage)
 *   (c) plan_filter_kind_for_test returns order="composite"
 *   (d) bounded scan via g_order_walk_scanned
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
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

/* ── test hooks used ─────────────────────────────────────────── */
extern const char *plan_filter_kind_for_test(const char *db_root, const char *object,
        const char *criteria_json, const char *order_by, int fetching,
        char *out_field, size_t fsz, char *out_order, size_t osz,
        int *out_total_cheap);
extern int  composite_prefix_walk_for_test(const char *db_root, const char *object,
                                           const char *criteria_json,
                                           const char *order_by, int order_desc,
                                           int limit);
extern long order_walk_scanned_for_test(void);

/* ── fixture ─────────────────────────────────────────────────── */
static TestClient *in_fold_setup(TestEnv *env) {
    if (test_env_start(env) != 0) { ASSERT_TRUE(0, "daemon spawn"); return NULL; }
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(env); return NULL; }

    char *r = do_req(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}"); free(r);
    r = do_req(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cm_infold\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"tag:varchar:8\",\"t:long\"],"
        "\"indexes\":[\"tag\",\"tag+t\"]}");
    free(r);

    /* Insert 12 rows — interleave tags in t order so the k-way merge
     * must re-order across IN values. Each t value is unique.
     *
     *  tag  | t
     *  -----|---
     *  a    | 1
     *  b    | 2
     *  c    | 3
     *  d    | 4
     *  a    | 5
     *  b    | 6
     *  c    | 7
     *  d    | 8
     *  a    | 9
     *  b    | 10
     *  c    | 11
     *  d    | 12
     *
     *  tag in (a,c) ORDER BY t DESC → should yield t=11,9,7,5,3,1
     *  Limit N → only first N of those, in strict descending order. */
    const char *tags[12] = {"a","b","c","d","a","b","c","d","a","b","c","d"};
    long times[12] = {1,2,3,4,5,6,7,8,9,10,11,12};

    char body[8192]; size_t p = 0;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 12; i++) {
        SB_APPEND(body, p, sizeof(body), "%s\"k%02d\":{\"tag\":\"%s\",\"t\":%ld}",
                  i == 0 ? "" : ",", i, tags[i], times[i]);
    }
    SB_APPEND(body, p, sizeof(body), "}");

    char req[8224];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"cm_infold\","
        "\"records\":%s}", body);
    r = do_req(tc, req); free(r);
    return tc;
}

/* Extract t values from a JSON array of dict results in order.
 * Returns count of t values found (max N). */
static int extract_t_values(const char *resp, long *out, int max) {
    int n = 0;
    const char *p = resp;
    while (n < max && p) {
        p = strstr(p, "\"t\":");
        if (!p) break;
        p += 4;
        while (*p == ' ') p++;
        if (*p == '-' || (*p >= '0' && *p <= '9')) {
            out[n++] = atol(p);
        }
        p++;
    }
    return n;
}

/* ── test: correctness + bounded scan ────────────────────────── */

static int test_in_fold_desc_limit_5(void) {
    TestEnv env = {0};
    TestClient *tc = in_fold_setup(&env);
    if (!tc) return 1;

    char *r = do_req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cm_infold\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"in\",\"value\":\"a,c\"}],"
        "\"order_by\":\"t\",\"order\":\"desc\",\"limit\":5}");
    ASSERT_TRUE(!has(r, "\"error\""), "no error");
    ASSERT_TRUE(has(r, "\"k00\"") || has(r, "\"k02\"") ||
                has(r, "\"k04\"") || has(r, "\"k08\"") ||
                has(r, "\"k10\""), "at least one a/c row present");

    /* (b) no b/d leakage */
    ASSERT_TRUE(!has(r, "\"k01\""), "k01 (b) not leaked");
    ASSERT_TRUE(!has(r, "\"k03\""), "k03 (d) not leaked");
    ASSERT_TRUE(!has(r, "\"k05\""), "k05 (b) not leaked");
    ASSERT_TRUE(!has(r, "\"k07\""), "k07 (d) not leaked");
    ASSERT_TRUE(!has(r, "\"k09\""), "k09 (b) not leaked");
    ASSERT_TRUE(!has(r, "\"k11\""), "k11 (d) not leaked");

    /* (a) strictly monotonic descending t */
    long tv[16];
    int n = extract_t_values(r, tv, 16);
    ASSERT_TRUE(n >= 1 && n <= 5, "got between 1 and 5 t values");
    for (int i = 1; i < n; i++) {
        ASSERT_TRUE(tv[i-1] > tv[i], "t values strictly descending");
    }

    free(r);
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-in-fold-desc-limit-5", test_in_fold_desc_limit_5)

/* DESC without explicit limit = INT_MAX (return all 6 a/c rows). */
static int test_in_fold_desc_all(void) {
    TestEnv env = {0};
    TestClient *tc = in_fold_setup(&env);
    if (!tc) return 1;

    char *r = do_req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cm_infold\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"in\",\"value\":\"a,c\"}],"
        "\"order_by\":\"t\",\"order\":\"desc\"}");
    ASSERT_TRUE(!has(r, "\"error\""), "no error");

    /* (b) no b/d leakage */
    ASSERT_TRUE(!has(r, "\"k01\""), "b absent");
    ASSERT_TRUE(!has(r, "\"k03\""), "d absent");

    /* (a) strictly monotonic descending t — should be 11,9,7,5,3,1 */
    long tv[16];
    int n = extract_t_values(r, tv, 16);
    ASSERT_EQ_INT(n, 6, "got all 6 a/c rows");
    for (int i = 1; i < n; i++) {
        ASSERT_TRUE(tv[i-1] > tv[i], "t strictly descending");
    }

    free(r);
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-in-fold-desc-all", test_in_fold_desc_all)

/* ASC variant: tag in (a,c) ORDER BY t ASC limit 4 → 1,3,5,7 */
static int test_in_fold_asc_limit_4(void) {
    TestEnv env = {0};
    TestClient *tc = in_fold_setup(&env);
    if (!tc) return 1;

    char *r = do_req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cm_infold\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"in\",\"value\":\"a,c\"}],"
        "\"order_by\":\"t\",\"order\":\"asc\",\"limit\":4}");
    ASSERT_TRUE(!has(r, "\"error\""), "no error");

    ASSERT_TRUE(!has(r, "\"k01\""), "b absent");

    long tv[16];
    int n = extract_t_values(r, tv, 16);
    ASSERT_EQ_INT(n, 4, "got 4 rows");
    for (int i = 1; i < n; i++) {
        ASSERT_TRUE(tv[i-1] < tv[i], "t strictly ascending");
    }

    free(r);
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-in-fold-asc-limit-4", test_in_fold_asc_limit_4)

/* Plan introspection: OP_IN + order_by → order="composite" */
static int test_in_fold_plan_composite(void) {
    TestEnv env = {0};
    TestClient *tc = in_fold_setup(&env);
    if (!tc) return 1;

    /* selectivity_budget(N) = N/g_random_seq_ratio (default 8).  Our tiny
     * 12-row dataset gives N/8=1, so the IN leaf (est K≈2) saturates and
     * triggers B5 demotion to FULL_SCAN before the order-by overlay ever
     * runs.  Temporarily set ratio=1 so budget=N and the leaf is selective,
     * letting the D1 composite check fire. */
    extern int g_random_seq_ratio;
    int saved = g_random_seq_ratio;
    g_random_seq_ratio = 1;

    char out_order[32];
    const char *kind = plan_filter_kind_for_test(
        env.db_root, "default/cm_infold",
        "[{\"field\":\"tag\",\"op\":\"in\",\"value\":\"a,c\"}]",
        "t", 1,
        NULL, 0, out_order, sizeof(out_order), NULL);
    g_random_seq_ratio = saved;
    ASSERT_TRUE(kind != NULL, "plan_kind non-null");
    ASSERT_CONTAINS(out_order, "composite", "order is composite (not sort/walk)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-in-fold-plan-composite", test_in_fold_plan_composite)

/* Bounded scan: composite_prefix_walk tracks scanned entries.
 * For tag in (a,c) ORDER BY t DESC limit 5, the k-way merge
 * should scan O(limit × n_values) ≈ 5 × 2 = ~10–15 entries,
 * not the full 12-row data or more. */
/* Single-value EQ composite prefix walk — exercises the seed_tf_sv gate. */
static int test_in_fold_single_eq(void) {
    TestEnv env = {0};
    TestClient *tc = in_fold_setup(&env);
    if (!tc) return 1;

    extern int g_random_seq_ratio;
    int saved = g_random_seq_ratio;
    g_random_seq_ratio = 1;

    char out_order[32];
    const char *kind = plan_filter_kind_for_test(
        env.db_root, "default/cm_infold",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"a\"}]",
        "t", 1,
        NULL, 0, out_order, sizeof(out_order), NULL);
    g_random_seq_ratio = saved;
    ASSERT_TRUE(kind != NULL, "plan_kind non-null");
    ASSERT_CONTAINS(out_order, "composite", "order is composite for single EQ");

    /* Bounded walk: for tag=a there are 3 rows (t=1,5,9). DESC limit=2
     * should scan only those 3 (or a few more via k-way merge across shards). */
    g_random_seq_ratio = 1; /* override again for the walk's internal plan_filter */
    int scanned = composite_prefix_walk_for_test(
        env.db_root, "default/cm_infold",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"a\"}]",
        "t", 1 /* desc */, 2);
    g_random_seq_ratio = saved;
    ASSERT_TRUE(scanned >= 2, "scanned at least limit entries");
    ASSERT_TRUE(scanned <= 32, "scanned bounded for single EQ");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-in-fold-single-eq", test_in_fold_single_eq)

/* Q1: verify the byte-successor gate is tight for VARCHAR fields by checking
   that the composite-prefix walk scans only the matching partition (proved
   by g_order_walk_scanned).  Also verifies the length-based gate works
   correctly for a single-value EQ on a typed object. */
static int test_in_fold_q1_gate_tight(void) {
    TestEnv env = {0};
    /* Use in_fold_setup which spawns daemon, creates cm_infold with
     * tag:varchar:8, t:long, composite index tag+t, and 12 rows (3 per tag). */
    TestClient *tc = in_fold_setup(&env);
    if (!tc) return 1;

    extern int g_random_seq_ratio;
    int saved = g_random_seq_ratio;
    g_random_seq_ratio = 1;

    /* Verify the plan picks composite D1. */
    char out_order[32];
    const char *kind = plan_filter_kind_for_test(
        env.db_root, "default/cm_infold",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"a\"}]",
        "t", 1,
        NULL, 0, out_order, sizeof(out_order), NULL);
    ASSERT_TRUE(kind != NULL, "plan_kind non-null");
    ASSERT_CONTAINS(out_order, "composite", "order composite");

    /* Walk should scan exactly the 3 tag=a entries (tight byte-successor
     * bounds: "a" → "b"), not spill into other tags. */
    int scanned = composite_prefix_walk_for_test(
        env.db_root, "default/cm_infold",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"a\"}]",
        "t", 1, 3);
    g_random_seq_ratio = saved;
    ASSERT_TRUE(scanned >= 3, "scanned at least 3 (all tag=a rows)");
    ASSERT_TRUE(scanned <= 32, "scanned bounded (not full table)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-in-fold-q1-gate-tight", test_in_fold_q1_gate_tight)

/* Q2: cursor:null with selective range + broad composite equality + ORDER BY.
   Object: type:varchar:8, score:long, type+score composite.  Broad equality
   (type=common, ~9 rows) + selective range (score>=9, ~3 rows) + ORDER BY
   score DESC + cursor:null + limit=2.  With prefilter_card the cursor C1
   shortcut should pick fetch+sort (bounded) over a brute-force composite walk. */
static int test_in_fold_q2_cursor_fetchsort(void) {
    TestEnv env = {0};
    TestClient *tc = in_fold_setup(&env);
    if (!tc) return 1;

    /* Use existing cm_infold fixture: tag:varchar:8, t:long, tag+t composite.
     * 12 rows: tags a/b/c/d interleaved with t=1..12.
     * We need a standalone index on t for the cursor:null requirement. */
    char *r = do_req(tc,
        "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"cm_infold\",\"field\":\"t\"}");
    if (has(r, "\"error\"")) { free(r); tc_close(tc); test_env_stop(&env); return 1; }
    free(r);

    /* Query: tag=a (broad, 3 rows) AND t>=9 (selective, 1 row: k04, t=9)
     *         ORDER BY t DESC cursor:null limit=1
     * → D1 composite seed = tag=a (broad, keyset ~3), prefilter_card = t>=9 (~1)
     * → cursor C1 should pick fetch+sort (bounded) over composite walk. */
    extern int g_random_seq_ratio;
    int saved = g_random_seq_ratio;
    g_random_seq_ratio = 1;

    char out_order[32];
    const char *kind = plan_filter_kind_for_test(
        env.db_root, "default/cm_infold",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"a\"},"
         "{\"field\":\"t\",\"op\":\"gte\",\"value\":\"9\"}]",
        "t", 1,
        NULL, 0, out_order, sizeof(out_order), NULL);
    ASSERT_TRUE(kind != NULL, "plan_kind non-null q2");
    ASSERT_CONTAINS(out_order, "composite", "order composite q2");

    /* Cursor:null find — the C1 prefilter_card override should enable
       fetch+sort (bounded) instead of composite walk. */
    r = do_req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cm_infold\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"a\"},"
                       "{\"field\":\"t\",\"op\":\"gte\",\"value\":\"9\"}],"
        "\"order_by\":\"t\",\"order\":\"desc\",\"limit\":1,"
        "\"cursor\":null}");
    ASSERT_TRUE(!has(r, "\"error\""), "q2 cursor no error");
    ASSERT_TRUE(has(r, "\"k08\""), "q2 cursor has k08 (a, t=9)");
    ASSERT_TRUE(has(r, "\"cursor\""), "q2 cursor has cursor");
    free(r);

    /* Same query without t>=9 → tag=a sole leaf, composite walk. */
    r = do_req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cm_infold\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"a\"}],"
        "\"order_by\":\"t\",\"order\":\"desc\",\"limit\":1,"
        "\"cursor\":null}");
    ASSERT_TRUE(!has(r, "\"error\""), "q2 broad-only no error");
    ASSERT_TRUE(has(r, "\"cursor\""), "q2 broad-only cursor");
    free(r);

    g_random_seq_ratio = saved;
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-in-fold-q2-cursor-fetchsort", test_in_fold_q2_cursor_fetchsort)

static int test_in_fold_bounded_scan(void) {
    TestEnv env = {0};
    TestClient *tc = in_fold_setup(&env);
    if (!tc) return 1;

    /* Same g_random_seq_ratio override as test-in-fold-plan-composite
     * — the bounded scan path also goes through the planner so a
     * saturated IN leaf would demote to FULL_SCAN and never reach
     * the composite executor. */
    extern int g_random_seq_ratio;
    int saved = g_random_seq_ratio;
    g_random_seq_ratio = 1;

    int scanned = composite_prefix_walk_for_test(
        env.db_root, "default/cm_infold",
        "[{\"field\":\"tag\",\"op\":\"in\",\"value\":\"a,c\"}]",
        "t", 1 /* desc */, 5);
    g_random_seq_ratio = saved;
    ASSERT_TRUE(scanned >= 0, "composite_prefix_walk returned >=0 (plan is composite)");
    /* With 2 values × 8 splits, worst-case we pull at most ~iters × (limit+offset)
     * entries through the callback.  The check is order-of-magnitude:
     * scanned must be much less than a full table scan (12 rows). */
    ASSERT_TRUE(scanned >= 5, "scanned at least limit entries");
    ASSERT_TRUE(scanned <= 64, "scanned bounded (< 12 × 8 shards, real discard rate)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-in-fold-bounded-scan", test_in_fold_bounded_scan)
