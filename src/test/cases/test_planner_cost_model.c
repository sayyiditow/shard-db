#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "../../db/types.h"
#include "../test_client.h"
#include "../fixtures.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

extern int leaf_selective_for_test(const char *db_root, const char *object,
                                   const char *field, const char *value, size_t *out_k);

/* Shared fixture: object `cm` with a btree-indexed `tag` field;
 * 5 rows tag=rare, 200 rows tag=common (N=205, budget=205/8=25). */
static TestClient *cm_setup(TestEnv *env, const char *obj, const char *fields,
                            const char *indexes) {
    if (test_env_start(env) != 0) { ASSERT_TRUE(0, "spawn"); return NULL; }
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(env); return NULL; }
    char *resp=NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp=NULL;
    char co[1024];
    snprintf(co,sizeof(co),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":8,\"max_key\":12,\"fields\":[%s],\"indexes\":[%s]}",
        obj, fields, indexes);
    tc_request(tc, co, &resp); free(resp); resp=NULL;
    return tc;
}
static void cm_insert_tags(TestClient *tc, const char *obj) {
    char body[65536]; int p=0,k=0; char *resp=NULL;
    p+=snprintf(body+p,sizeof(body)-p,"{");
    for (int i=0;i<5;i++){p+=snprintf(body+p,sizeof(body)-p,"%s\"k%d\":{\"tag\":\"rare\"}",k==0?"":",",k);k++;}
    for (int i=0;i<200;i++){p+=snprintf(body+p,sizeof(body)-p,",\"k%d\":{\"tag\":\"common\"}",k);k++;}
    p+=snprintf(body+p,sizeof(body)-p,"}");
    char req[66560];
    snprintf(req,sizeof(req),"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":%s}",obj,body);
    tc_request(tc, req, &resp); free(resp);
}

static int test_cost_selectivity_primitive(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "cm", "\"tag:varchar:8\"", "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "cm");
    size_t kr=0, kc=0;
    int sr = leaf_selective_for_test(env.db_root, "default/cm", "tag", "rare", &kr);
    int sc = leaf_selective_for_test(env.db_root, "default/cm", "tag", "common", &kc);
    ASSERT_EQ_INT((int)kr, 5, "rare K=5");
    ASSERT_EQ_INT(sr, 1, "rare selective (5 <= budget 25)");
    ASSERT_EQ_INT(sc, 0, "common not selective (200 > budget 25)");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-cost-selectivity-primitive", test_cost_selectivity_primitive)

extern const char *plan_filter_kind_for_test(const char *db_root, const char *object,
        const char *criteria_json, const char *order_by, int fetching,
        char *out_field, size_t fsz, char *out_order, size_t osz,
        int *out_total_cheap);

/* A1: 1 selective btree eq → leaf, seeded on `tag`. */
static int test_planA1_selective_leaf(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "a1", "\"tag:varchar:8\"", "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "a1");
    char f[64]={0}, o[16]={0};
    const char *k = plan_filter_kind_for_test(env.db_root, "default/a1",
        "{\"tag\":\"rare\"}", NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(k, "leaf", "A1 selective eq → PRIMARY_LEAF");
    ASSERT_EQ_STR(f, "tag", "A1 seeds on tag");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-a1-selective-leaf", test_planA1_selective_leaf)

/* A2: 1 broad bitmap (active=true, 70/100) → bitmap smaller-side, never leaf. */
static int test_planA2_broad_bitmap(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "a2", "\"active:bool\"", "\"active:bitmap\"");
    if (!tc) return 1;
    char body[16384]; int p=0,k=0; char *resp=NULL;
    p+=snprintf(body+p,sizeof(body)-p,"{");
    for(int i=0;i<70;i++){p+=snprintf(body+p,sizeof(body)-p,"%s\"k%d\":{\"active\":true}",k==0?"":",",k);k++;}
    for(int i=0;i<30;i++){p+=snprintf(body+p,sizeof(body)-p,",\"k%d\":{\"active\":false}",k);k++;}
    p+=snprintf(body+p,sizeof(body)-p,"}");
    char req[17408];
    snprintf(req,sizeof(req),"{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"a2\",\"records\":%s}",body);
    tc_request(tc, req, &resp); free(resp);
    char f[64]={0}, o[16]={0};
    const char *kind = plan_filter_kind_for_test(env.db_root, "default/a2",
        "{\"active\":true}", NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kind, "bitmap", "A2 broad bitmap → BITMAP_SMALLER (not leaf)");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-a2-broad-bitmap", test_planA2_broad_bitmap)

/* A5: 1 non-indexed leaf → full scan. */
static int test_planA5_nonindexed_scan(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "a5", "\"tag:varchar:8\",\"note:varchar:16\"", "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "a5"); /* note absent → empty; only tag indexed */
    char f[64]={0}, o[16]={0};
    /* Use array-form criteria so parse_criteria_tree gets a proper contains leaf */
    const char *k = plan_filter_kind_for_test(env.db_root, "default/a5",
        "[{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(k, "scan", "A5 non-indexed → FULL_SCAN");
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-a5-nonindexed-scan", test_planA5_nonindexed_scan)

/* -----------------------------------------------------------------------
 * B-row tests: multi-leaf AND + fetch-vs-count dimension (Task 1b.3)
 * ----------------------------------------------------------------------- */

/* B1: two selective btree leaves (tag=rare AND tag2=rare).
 *   fetching=0 (count) → "intersect" (index-only, no record reads).
 *   fetching=1 (find)  → "leaf"      (fetch the most-selective, check
 *                                      the other on the fetched record). */
static int test_planB1_two_selective_btree(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "b1",
        "\"tag:varchar:8\",\"tag2:varchar:8\"",
        "\"tag\",\"tag2\"");
    if (!tc) return 1;
    /* Insert 5 rows matching both, 200 matching only tag=common/tag2=common */
    char body[65536]; int p=0,k=0; char *resp=NULL;
    p+=snprintf(body+p,sizeof(body)-p,"{");
    for(int i=0;i<5;i++){
        p+=snprintf(body+p,sizeof(body)-p,"%s\"k%d\":{\"tag\":\"rare\",\"tag2\":\"rare\"}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<200;i++){
        p+=snprintf(body+p,sizeof(body)-p,",\"k%d\":{\"tag\":\"common\",\"tag2\":\"common\"}",k); k++;
    }
    p+=snprintf(body+p,sizeof(body)-p,"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"b1\",\"records\":%s}",body);
    tc_request(tc,req,&resp); free(resp);

    char f[64]={0}, o[16]={0};
    /* count path (fetching=0): two selective indexed leaves → intersect */
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b1",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "intersect", "B1 count: two selective → intersect");

    /* find path (fetching=1): most-selective seeds PRIMARY_LEAF, other post-filters */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b1",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kf, "leaf", "B1 find: two selective → leaf (fetch+check)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-b1-two-selective-btree", test_planB1_two_selective_btree)

/* B2: selective btree + broad bitmap (tag=rare AND active=true).
 *   Both fetching=0 and fetching=1 → "leaf" seeded on btree field `tag`.
 *   The bitmap leaf is NOT the seed (bitmap deprioritized). */
static int test_planB2_selective_btree_broad_bitmap(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "b2",
        "\"tag:varchar:8\",\"active:bool\"",
        "\"tag\",\"active:bitmap\"");
    if (!tc) return 1;
    char body[65536]; int p=0,k=0; char *resp=NULL;
    p+=snprintf(body+p,sizeof(body)-p,"{");
    /* 5 rare+active=true, 200 common+active=true (broad bitmap: 205/205 ≫ budget) */
    for(int i=0;i<5;i++){
        p+=snprintf(body+p,sizeof(body)-p,"%s\"k%d\":{\"tag\":\"rare\",\"active\":true}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<200;i++){
        p+=snprintf(body+p,sizeof(body)-p,",\"k%d\":{\"tag\":\"common\",\"active\":true}",k); k++;
    }
    p+=snprintf(body+p,sizeof(body)-p,"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"b2\",\"records\":%s}",body);
    tc_request(tc,req,&resp); free(resp);

    char f[64]={0}, o[16]={0};
    /* fetching=1: selective btree seeds, bitmap post-filters */
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b2",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kf, "leaf", "B2 find: selective btree seeds, bitmap post-filters");
    ASSERT_EQ_STR(f, "tag", "B2 find: seed is tag (btree), not active (bitmap)");

    /* fetching=0: same — selective btree wins over broad bitmap */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b2",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "leaf", "B2 count: selective btree still seeds (not intersect: bitmap broad)");
    ASSERT_EQ_STR(f, "tag", "B2 count: seed is tag");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-b2-selective-btree-broad-bitmap", test_planB2_selective_btree_broad_bitmap)

/* B3: two broad bitmaps (active=true AND flagged=true) → "intersect"
 *   (pure-bitmap AND → popcount intersect), both fetching values. */
static int test_planB3_two_broad_bitmaps(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "b3",
        "\"active:bool\",\"flagged:bool\"",
        "\"active:bitmap\",\"flagged:bitmap\"");
    if (!tc) return 1;
    char body[65536]; int p=0,k=0; char *resp=NULL;
    p+=snprintf(body+p,sizeof(body)-p,"{");
    /* 150 rows: active=true (150/205 broad), flagged=true (150/205 broad) */
    for(int i=0;i<150;i++){
        p+=snprintf(body+p,sizeof(body)-p,"%s\"k%d\":{\"active\":true,\"flagged\":true}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<55;i++){
        p+=snprintf(body+p,sizeof(body)-p,",\"k%d\":{\"active\":false,\"flagged\":false}",k); k++;
    }
    p+=snprintf(body+p,sizeof(body)-p,"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"b3\",\"records\":%s}",body);
    tc_request(tc,req,&resp); free(resp);

    char f[64]={0}, o[16]={0};
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b3",
        "[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"},"
         "{\"field\":\"flagged\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kf, "intersect", "B3 find: pure-bitmap AND → intersect");

    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b3",
        "[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"},"
         "{\"field\":\"flagged\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "intersect", "B3 count: pure-bitmap AND → intersect");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-b3-two-broad-bitmaps", test_planB3_two_broad_bitmaps)

/* B4: selective btree + non-indexed leaf (tag=rare AND note contains "x").
 *   → "leaf" seeded on `tag` (fetch the 5 matching tag=rare, check note on record). */
static int test_planB4_selective_btree_nonindexed(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "b4",
        "\"tag:varchar:8\",\"note:varchar:16\"",
        "\"tag\"");   /* note is NOT indexed */
    if (!tc) return 1;
    cm_insert_tags(tc, "b4");   /* 5 rare + 200 common; note field empty */

    char f[64]={0}, o[16]={0};
    /* fetching=1: selective btree seeds; non-indexed note post-filters on record */
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b4",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kf, "leaf", "B4 find: selective btree seeds, non-indexed post-filters");
    ASSERT_EQ_STR(f, "tag", "B4 find: seed is tag");

    /* fetching=0: same — one selective + one non-indexed → leaf (can't intersect non-indexed) */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b4",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "leaf", "B4 count: selective btree + non-indexed → leaf");
    ASSERT_EQ_STR(f, "tag", "B4 count: seed is tag");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-b4-selective-btree-nonindexed", test_planB4_selective_btree_nonindexed)

/* B7: two non-indexed leaves → "scan".
 * Use fields with NO index so both leaves return pick_index_for_leaf=-1. */
static int test_planB7_all_nonindexed(void) {
    TestEnv env={0};
    /* bio and about: NO indexes at all (empty index list) */
    TestClient *tc = cm_setup(&env, "b7",
        "\"bio:varchar:16\",\"about:varchar:16\"",
        "");   /* intentionally no indexes */
    if (!tc) return 1;
    /* Insert a few rows so N>0 */
    char *resp=NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"b7\","
        "\"records\":{\"k0\":{\"bio\":\"hello\",\"about\":\"world\"},"
                      "\"k1\":{\"bio\":\"foo\",\"about\":\"bar\"}}}",
        &resp); free(resp);

    char f[64]={0}, o[16]={0};
    const char *k = plan_filter_kind_for_test(env.db_root,"default/b7",
        "[{\"field\":\"bio\",\"op\":\"contains\",\"value\":\"x\"},"
         "{\"field\":\"about\",\"op\":\"contains\",\"value\":\"y\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k, "scan", "B7: both non-indexed → full scan");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-b7-all-nonindexed", test_planB7_all_nonindexed)

/* A4: indexed trigram-contains whose estimate is saturated (rarest gram
 * count > budget) must stay PRIMARY_LEAF — NOT demote to FULL_SCAN.
 *
 * Setup: object `a4tg` with title:trigram; 50 rows containing "abc" (each
 * row value is "abcdef"), 155 rows without (value "xyz"). N=205, budget=25.
 * Pattern "abc" → one trigram "abc", count=50 > 25 → saturated.
 * pick_index_for_leaf returns IT_TRIGRAM (pattern ≥3 chars, trigram index
 * exists). card_est_leaf returns {estimable=1, saturated=1}.
 * Before fix: demotion branch fires → "scan". After fix: skipped → "leaf". */
static int test_planA4_saturated_trigram_stays_leaf(void) {
    TestEnv env={0};
    /* trigram index on title field */
    TestClient *tc = cm_setup(&env, "a4tg",
        "\"title:varchar:32\"",
        "\"title:trigram\"");
    if (!tc) return 1;

    /* Build bulk-insert: 50 rows with "abcdef" (contains trigram abc,bcd,cde,def)
     * and 155 rows with "xyZpqr" (no overlap). N=205, budget=205/8=25.
     * Rarest gram among abc/bcd/cde/def = 50 > 25 → saturated. */
    char body[65536]; int p=0, k=0; char *resp=NULL;
    p += snprintf(body+p, sizeof(body)-p, "{");
    for (int i=0; i<50; i++) {
        p += snprintf(body+p, sizeof(body)-p,
            "%s\"k%d\":{\"title\":\"abcdef\"}", k==0?"":",", k); k++;
    }
    for (int i=0; i<155; i++) {
        p += snprintf(body+p, sizeof(body)-p,
            ",\"k%d\":{\"title\":\"xyZpqr\"}", k); k++;
    }
    p += snprintf(body+p, sizeof(body)-p, "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"a4tg\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp);

    char f[64]={0}, o[16]={0};
    /* Pattern "abc" → IT_TRIGRAM, saturated. Must be "leaf", never "scan". */
    const char *k_str = plan_filter_kind_for_test(env.db_root, "default/a4tg",
        "[{\"field\":\"title\",\"op\":\"contains\",\"value\":\"abc\"}]",
        NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(k_str, "leaf",
        "A4: saturated trigram-contains stays PRIMARY_LEAF (never FULL_SCAN)");

    /* Also check fetching=0 (count path) — same result. */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root, "default/a4tg",
        "[{\"field\":\"title\",\"op\":\"contains\",\"value\":\"abc\"}]",
        NULL, 0, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(k_count, "leaf",
        "A4 count: saturated trigram-contains stays leaf");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-a4-saturated-trigram-stays-leaf",
              test_planA4_saturated_trigram_stays_leaf)

/* BCS: count (fetching=0), n_indexed=2, exactly ONE selective leaf.
 * tag=rare is selective (5 ≤ budget 25), tag2=common is broad (200 > 25).
 * Expected plan: PRIMARY_LEAF seeded on `tag` (the selective one).
 *
 * This locks the fall-through path in the multi-leaf block:
 *   !fetching && n_selective==1 → neither "intersect" branch fires;
 *   falls to single-seed block → prim_sel=true → FP_PRIMARY_LEAF. */
static int test_planBCS_count_one_selective_leaf(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "bcs",
        "\"tag:varchar:8\",\"tag2:varchar:8\"",
        "\"tag\",\"tag2\"");
    if (!tc) return 1;

    /* 5 rows tag=rare + tag2=common, 200 rows tag=common + tag2=common.
     * tag=rare: 5 ≤ 25 → selective. tag2=common: 205 > 25 → broad.
     * n_selective=1 (only tag=rare clears the bar). */
    char body[65536]; int p=0, k=0; char *resp=NULL;
    p += snprintf(body+p, sizeof(body)-p, "{");
    for (int i=0; i<5; i++) {
        p += snprintf(body+p, sizeof(body)-p,
            "%s\"k%d\":{\"tag\":\"rare\",\"tag2\":\"common\"}",
            k==0?"":",", k); k++;
    }
    for (int i=0; i<200; i++) {
        p += snprintf(body+p, sizeof(body)-p,
            ",\"k%d\":{\"tag\":\"common\",\"tag2\":\"common\"}", k); k++;
    }
    p += snprintf(body+p, sizeof(body)-p, "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bcs\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp);

    char f[64]={0}, o[16]={0};
    /* fetching=0, n_indexed=2, n_selective=1 → must fall through to leaf. */
    const char *kc = plan_filter_kind_for_test(env.db_root, "default/bcs",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"common\"}]",
        NULL, 0, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kc, "leaf",
        "BCS count: n_indexed=2, exactly 1 selective → PRIMARY_LEAF (not intersect)");
    ASSERT_EQ_STR(f, "tag",
        "BCS count: seed is the selective field `tag`");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-bcs-count-one-selective-leaf",
              test_planBCS_count_one_selective_leaf)

/* -----------------------------------------------------------------------
 * C-row tests: OR / hybrid (Task 1b.4)
 *
 * OR JSON shape confirmed from test_or_logic.c:
 *   pure OR root:    [{"or":[{leaf},{leaf}]}]
 *   AND + OR child:  [{leaf}, {"or":[{leaf},{leaf}]}]
 * Both produce a CNODE_AND root; the inner {"or":[...]} is a CNODE_OR child.
 * find_fully_indexed_or() handles the CNODE_AND-with-OR-child case.
 * ----------------------------------------------------------------------- */

/* C1: pure OR, every child a selective indexed eq (tag="rare" OR tag2="rare").
 * Both tag and tag2 are btree-indexed → find_fully_indexed_or returns the OR
 * node → FP_UNION. */
static int test_planC1_pure_or_all_indexed(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "c1",
        "\"tag:varchar:8\",\"tag2:varchar:8\"",
        "\"tag\",\"tag2\"");
    if (!tc) return 1;
    /* 5 rows tag=rare+tag2=rare, 200 tag=common+tag2=common.
     * Both selective fields indexed → pure OR with all indexed children. */
    char body[65536]; int p=0,k=0; char *resp=NULL;
    p+=snprintf(body+p,sizeof(body)-p,"{");
    for(int i=0;i<5;i++){
        p+=snprintf(body+p,sizeof(body)-p,"%s\"k%d\":{\"tag\":\"rare\",\"tag2\":\"rare\"}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<200;i++){
        p+=snprintf(body+p,sizeof(body)-p,",\"k%d\":{\"tag\":\"common\",\"tag2\":\"common\"}",k); k++;
    }
    p+=snprintf(body+p,sizeof(body)-p,"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"c1\",\"records\":%s}",body);
    tc_request(tc,req,&resp); free(resp);

    char f[64]={0}, o[16]={0};
    /* Pure OR array form: [{"or":[{tag=rare},{tag2=rare}]}] */
    const char *k_str = plan_filter_kind_for_test(env.db_root,"default/c1",
        "[{\"or\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
                  "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_str, "union", "C1: pure OR all-indexed → FP_UNION");

    /* Also test fetching=0 (count) */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root,"default/c1",
        "[{\"or\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
                  "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_count, "union", "C1 count: pure OR all-indexed → FP_UNION");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-c1-pure-or-all-indexed", test_planC1_pure_or_all_indexed)

/* C2: OR with a non-indexed child (tag="rare" OR note contains "x").
 * note is not indexed → find_fully_indexed_or returns NULL → FP_FULL_SCAN. */
static int test_planC2_or_with_nonindexed_child(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "c2",
        "\"tag:varchar:8\",\"note:varchar:16\"",
        "\"tag\"");   /* note is NOT indexed */
    if (!tc) return 1;
    cm_insert_tags(tc, "c2");   /* 5 rare + 200 common; note field absent */

    char f[64]={0}, o[16]={0};
    /* OR where one child (note contains "x") has no index */
    const char *k_str = plan_filter_kind_for_test(env.db_root,"default/c2",
        "[{\"or\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
                  "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_str, "scan", "C2: OR with non-indexed child → FULL_SCAN");

    /* Also count path */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root,"default/c2",
        "[{\"or\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
                  "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_count, "scan", "C2 count: OR with non-indexed child → FULL_SCAN");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-c2-or-with-nonindexed-child", test_planC2_or_with_nonindexed_child)

/* C3: AND of indexed leaf + OR sub-tree (tag="rare" AND (tag2="a" OR tag2="b")).
 * collect_and_leaves picks up the LEAF child (tag); the OR child is skipped.
 * most_selective_indexed seeds on tag → FP_PRIMARY_LEAF, field="tag".
 * The OR sub-tree becomes a per-record post-filter (has_subtree=1 internally). */
static int test_planC3_and_leaf_plus_or_subtree(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "c3",
        "\"tag:varchar:8\",\"tag2:varchar:8\"",
        "\"tag\",\"tag2\"");
    if (!tc) return 1;
    /* 5 rows tag=rare+tag2=a, 200 rows tag=common+tag2=b */
    char body[65536]; int p=0,k=0; char *resp=NULL;
    p+=snprintf(body+p,sizeof(body)-p,"{");
    for(int i=0;i<5;i++){
        p+=snprintf(body+p,sizeof(body)-p,"%s\"k%d\":{\"tag\":\"rare\",\"tag2\":\"a\"}",
            k==0?"":",",k); k++;
    }
    for(int i=0;i<200;i++){
        p+=snprintf(body+p,sizeof(body)-p,",\"k%d\":{\"tag\":\"common\",\"tag2\":\"b\"}",k); k++;
    }
    p+=snprintf(body+p,sizeof(body)-p,"}");
    char req[66560];
    snprintf(req,sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"c3\",\"records\":%s}",body);
    tc_request(tc,req,&resp); free(resp);

    char f[64]={0}, o[16]={0};
    /* AND + OR sub-tree: [{tag=rare}, {"or":[{tag2=a},{tag2=b}]}]
     * collect_and_leaves sees 1 LEAF (tag=rare), skips the OR child.
     * Seeds on tag → FP_PRIMARY_LEAF. */
    const char *k_str = plan_filter_kind_for_test(env.db_root,"default/c3",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"or\":[{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"a\"},"
                  "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"b\"}]}]",
        NULL, 1, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_str, "leaf", "C3: AND+OR sub-tree → PRIMARY_LEAF");
    ASSERT_EQ_STR(f, "tag", "C3: seed is the AND leaf `tag`, not the OR child");

    /* fetching=0 (count): same — one selective indexed AND-leaf seeds */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root,"default/c3",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"or\":[{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"a\"},"
                  "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"b\"}]}]",
        NULL, 0, f,sizeof(f), o,sizeof(o), NULL);
    ASSERT_EQ_STR(k_count, "leaf", "C3 count: AND+OR sub-tree → PRIMARY_LEAF");
    ASSERT_EQ_STR(f, "tag", "C3 count: seed is tag");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-c3-and-leaf-plus-or-subtree", test_planC3_and_leaf_plus_or_subtree)

/* -----------------------------------------------------------------------
 * D-row tests: order_by overlay (Task 1b.5)
 *
 * D1: composite (filter+order) index exists → FP_ORDER_COMPOSITE.
 * D2: no composite, seed K bounded → FP_ORDER_SORT.
 * D3: no composite, seed K broad (saturated) → FP_ORDER_INDEX_WALK.
 * total_cheap: materialized KeySet plans → 1; FULL_SCAN → 0.
 * ----------------------------------------------------------------------- */

/* D1: by=varchar:16 + time=long; indexes: "by" (btree) + "by+time" (composite btree).
 * criteria {"by":"alice"} (selective), order_by "time" → composite exists → COMPOSITE. */
static int test_planD1_composite_order(void) {
    TestEnv env={0};
    /* Two fields; composite index "by+time" stays btree. */
    TestClient *tc = cm_setup(&env, "d1",
        "\"by:varchar:16\",\"time:long\"",
        "\"by\",\"by+time\"");
    if (!tc) return 1;

    /* Insert 5 rows by="alice", 200 rows by="bob" (N=205, budget=25).
     * by="alice" is selective (5 ≤ 25). */
    char body[65536]; int p=0,k=0; char *resp=NULL;
    p += snprintf(body+p, sizeof(body)-p, "{");
    for (int i=0; i<5; i++) {
        p += snprintf(body+p, sizeof(body)-p,
            "%s\"k%d\":{\"by\":\"alice\",\"time\":%d}",
            k==0?"":",", k, k*100); k++;
    }
    for (int i=0; i<200; i++) {
        p += snprintf(body+p, sizeof(body)-p,
            ",\"k%d\":{\"by\":\"bob\",\"time\":%d}", k, k*100); k++;
    }
    p += snprintf(body+p, sizeof(body)-p, "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d1\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp);

    char f[64]={0}, o[32]={0};
    int tc_val = -1;
    const char *k_str = plan_filter_kind_for_test(env.db_root, "default/d1",
        "{\"by\":\"alice\"}", "time", 1,
        f, sizeof(f), o, sizeof(o), &tc_val);
    ASSERT_EQ_STR(k_str, "leaf", "D1: selective by= → PRIMARY_LEAF");
    ASSERT_EQ_STR(o, "composite", "D1: by+time composite → FP_ORDER_COMPOSITE");
    ASSERT_EQ_INT(tc_val, 1, "D1: total_cheap=1 (KeySet materialized)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-d1-composite-order", test_planD1_composite_order)

/* D2: by=varchar:16 + time=long; ONLY "by" indexed (NO composite).
 * criteria {"by":"alice"} (selective, K=5 ≤ budget), order_by "time" → SORT. */
static int test_planD2_sort_order(void) {
    TestEnv env={0};
    /* Only "by" indexed; no composite; time is not indexed. */
    TestClient *tc = cm_setup(&env, "d2",
        "\"by:varchar:16\",\"time:long\"",
        "\"by\"");
    if (!tc) return 1;

    /* 5 rows by="alice" (selective), 200 rows by="bob". */
    char body[65536]; int p=0,k=0; char *resp=NULL;
    p += snprintf(body+p, sizeof(body)-p, "{");
    for (int i=0; i<5; i++) {
        p += snprintf(body+p, sizeof(body)-p,
            "%s\"k%d\":{\"by\":\"alice\",\"time\":%d}",
            k==0?"":",", k, k*100); k++;
    }
    for (int i=0; i<200; i++) {
        p += snprintf(body+p, sizeof(body)-p,
            ",\"k%d\":{\"by\":\"bob\",\"time\":%d}", k, k*100); k++;
    }
    p += snprintf(body+p, sizeof(body)-p, "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d2\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp);

    char f[64]={0}, o[32]={0};
    int tc_val = -1;
    const char *k_str = plan_filter_kind_for_test(env.db_root, "default/d2",
        "{\"by\":\"alice\"}", "time", 1,
        f, sizeof(f), o, sizeof(o), &tc_val);
    ASSERT_EQ_STR(k_str, "leaf", "D2: selective by= → PRIMARY_LEAF");
    ASSERT_EQ_STR(o, "sort", "D2: bounded K=5, no composite → FP_ORDER_SORT");
    ASSERT_EQ_INT(tc_val, 1, "D2: total_cheap=1 (KeySet materialized)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-d2-sort-order", test_planD2_sort_order)

/* D3: by=varchar:16 + time=long; ONLY "by" indexed; value "bob" is BROAD
 * (200 rows > budget 25), order_by "time" → seed saturated → INDEX_WALK.
 * No composite exists. */
static int test_planD3_walk_order(void) {
    TestEnv env={0};
    /* Only "by" indexed; no composite. */
    TestClient *tc = cm_setup(&env, "d3",
        "\"by:varchar:16\",\"time:long\"",
        "\"by\"");
    if (!tc) return 1;

    /* 200 rows by="bob" (broad: 200 > budget 25), 5 rows by="alice".
     * Query on by="bob" → broad → seed saturated → FP_ORDER_INDEX_WALK. */
    char body[65536]; int p=0,k=0; char *resp=NULL;
    p += snprintf(body+p, sizeof(body)-p, "{");
    for (int i=0; i<200; i++) {
        p += snprintf(body+p, sizeof(body)-p,
            "%s\"k%d\":{\"by\":\"bob\",\"time\":%d}",
            k==0?"":",", k, k*100); k++;
    }
    for (int i=0; i<5; i++) {
        p += snprintf(body+p, sizeof(body)-p,
            ",\"k%d\":{\"by\":\"alice\",\"time\":%d}", k, k*100); k++;
    }
    p += snprintf(body+p, sizeof(body)-p, "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d3\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp);

    char f[64]={0}, o[32]={0};
    int tc_val = -1;
    /* by="bob" is broad: N=205, budget=25, K=200 > 25 → saturated.
     * pick_index_for_leaf=IT_BTREE (not bitmap). The demotion gate
     * (op_eligible_for_intersect) fires for eq, demoting to FULL_SCAN unless
     * the seed is a PRIMARY_LEAF at all. Wait — the single-seed block checks
     * !prim_sel && prim_it!=IT_BITMAP && est.saturated → FULL_SCAN.
     * But "bob" IS broad (saturated) and the single-seed block would scan.
     * For order overlay D3 we need the path where the plan IS a leaf but
     * the seed is broad. Use the multi-leaf path to force PRIMARY_LEAF:
     * criteria AND of [by="bob", time >= 0] where time is not indexed —
     * only by is indexed → n_indexed=1, prim=by, prim_sel=0, prim_it=IT_BTREE.
     * BUT the demotion guard fires → FULL_SCAN. No order overlay for FULL_SCAN.
     *
     * The spec says D3 fires "when candidates > budget" and the plan is
     * non-scan. To reach D3 cleanly: use a BROAD bitmap (never demoted to
     * FULL_SCAN) with order_by. A broad bitmap → FP_BITMAP_SMALLER. Its seed
     * K is the smaller-side complement count (estimable, may be saturated when
     * both sides are broad). For a pure D3-like "walk" path via btree, we need
     * n_selective=0 and all_bitmap=0, falling to intersect (which can then
     * have a broad seed for the overlay). OR: use the multi-leaf broad intersect
     * path (n_selective=0, n_indexed≥2) → FP_INTERSECT → order overlay runs
     * on source_leaves[0] which is broad → saturated → WALK.
     *
     * Build: two non-selective btree leaves, n_selective=0 → INTERSECT,
     * then overlay on source_leaves[0] (broad) → WALK. */

    /* Re-insert into a fresh object d3b with two indexed fields, both broad. */
    char *resp2=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"d3b\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"by:varchar:16\",\"cat:varchar:8\"],"
        "\"indexes\":[\"by\",\"cat\"]}",
        &resp2); free(resp2); resp2=NULL;

    /* 200 rows by="bob"/cat="x" → both broad (200 > budget 25). N=200. */
    char body2[65536]; int p2=0,k2=0;
    p2 += snprintf(body2+p2, sizeof(body2)-p2, "{");
    for (int i=0; i<200; i++) {
        p2 += snprintf(body2+p2, sizeof(body2)-p2,
            "%s\"r%d\":{\"by\":\"bob\",\"cat\":\"x\"}",
            k2==0?"":",", k2); k2++;
    }
    p2 += snprintf(body2+p2, sizeof(body2)-p2, "}");
    char req2[66560];
    snprintf(req2, sizeof(req2),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d3b\","
        "\"records\":%s}", body2);
    char *resp3=NULL;
    tc_request(tc, req2, &resp3); free(resp3);

    /* fetching=0 (count), both broad indexed → FP_INTERSECT (n_selective=0 path).
     * source_leaves[0] = "by"="bob" → broad → saturated → FP_ORDER_INDEX_WALK. */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o)); tc_val=-1;
    const char *k_d3b = plan_filter_kind_for_test(env.db_root, "default/d3b",
        "[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"bob\"},"
         "{\"field\":\"cat\",\"op\":\"eq\",\"value\":\"x\"}]",
        "by", 0,
        f, sizeof(f), o, sizeof(o), &tc_val);
    ASSERT_EQ_STR(k_d3b, "intersect", "D3: all-broad indexed → INTERSECT");
    ASSERT_EQ_STR(o, "walk", "D3: broad seed + order_by → FP_ORDER_INDEX_WALK");
    ASSERT_EQ_INT(tc_val, 1, "D3: total_cheap=1 (INTERSECT materializes KeySet)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-d3-walk-order", test_planD3_walk_order)

/* total_cheap: A1 (selective leaf, no order_by) → 1; A5 (scan) → 0. */
static int test_plan_total_cheap(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "tc_obj",
        "\"tag:varchar:8\",\"note:varchar:16\"",
        "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "tc_obj");

    char f[64]={0}, o[32]={0};
    int cheap = -1;

    /* A1: selective btree leaf, no order_by → PRIMARY_LEAF → total_cheap=1 */
    const char *k1 = plan_filter_kind_for_test(env.db_root, "default/tc_obj",
        "{\"tag\":\"rare\"}", NULL, 1,
        f, sizeof(f), o, sizeof(o), &cheap);
    ASSERT_EQ_STR(k1, "leaf", "total_cheap A1: kind=leaf");
    ASSERT_EQ_INT(cheap, 1, "total_cheap A1: total_cheap=1 (KeySet materialized)");

    /* A5: non-indexed → FULL_SCAN → total_cheap=0 */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o)); cheap=-1;
    const char *k5 = plan_filter_kind_for_test(env.db_root, "default/tc_obj",
        "[{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 1,
        f, sizeof(f), o, sizeof(o), &cheap);
    ASSERT_EQ_STR(k5, "scan", "total_cheap A5: kind=scan");
    ASSERT_EQ_INT(cheap, 0, "total_cheap A5: total_cheap=0 (no KeySet)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-total-cheap", test_plan_total_cheap)

/* D3 single-leaf: broad btree leaf + indexed order_by → demotion suppressed.
 *
 * Two scenarios:
 *  (a) order_by field IS btree-indexed → demotion must be suppressed so the
 *      seed stays PRIMARY_LEAF and the order overlay fires (saturated seed →
 *      FP_ORDER_INDEX_WALK / "walk").  Currently broken: B5 guard demotes to
 *      FULL_SCAN before the overlay runs.
 *  (b) order_by field is NOT indexed → demotion fires → FULL_SCAN (regression
 *      lock: this is correct behaviour that must stay true after the fix).
 *
 * Object layout:
 *   score:int  (btree-indexed) — criteria leaf: score > 50 (broad, gt eligible
 *                                 for intersect → B5 guard fires without fix)
 *   time:long  (btree-indexed) — order_by field for scenario (a)
 *   note:varchar:32 (NOT indexed) — order_by field for scenario (b)
 *
 * Data: 250 rows all with score=100 (> 50) → N=250, budget=250/8=31.
 *   K for "score>50" = 250 > 31 → saturated → demotion guard fires.
 */
static int test_planD3_single_leaf_indexed_order(void) {
    TestEnv env={0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    /* add-dir */
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* create object: score + time indexed, note NOT indexed */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"d3sl\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"score:int\",\"time:long\",\"note:varchar:32\"],"
        "\"indexes\":[\"score\",\"time\"]}",
        &resp); free(resp); resp = NULL;

    /* insert 250 rows: score=100, time=i, note="x" */
    char body[65536]; int bp = 0, ki = 0;
    bp += snprintf(body+bp, sizeof(body)-bp, "{");
    for (int i = 0; i < 250; i++) {
        bp += snprintf(body+bp, sizeof(body)-bp,
            "%s\"r%d\":{\"score\":\"100\",\"time\":\"%d\",\"note\":\"x\"}",
            ki == 0 ? "" : ",", ki, i);
        ki++;
    }
    bp += snprintf(body+bp, sizeof(body)-bp, "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"d3sl\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    char f[64]={0}, o[32]={0};
    int cheap = -1;

    /* (a) order_by="time" (indexed btree) — demotion must be suppressed.
     *   Expected: kind="leaf" (PRIMARY_LEAF, NOT demoted to scan),
     *             out_order="walk" (saturated seed → ORDER_INDEX_WALK). */
    const char *k_a = plan_filter_kind_for_test(env.db_root, "default/d3sl",
        "[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}]",
        "time", 1,
        f, sizeof(f), o, sizeof(o), &cheap);
    ASSERT_EQ_STR(k_a, "leaf",
        "D3 single-leaf: indexed order_by suppresses B5 demotion → PRIMARY_LEAF");
    ASSERT_EQ_STR(o, "walk",
        "D3 single-leaf: saturated seed + indexed order_by → ORDER_INDEX_WALK");

    /* (b) order_by="note" (NOT indexed) — demotion must still fire → FULL_SCAN.
     *   Regression lock: no indexed order_by means B5 should demote as before. */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o)); cheap = -1;
    const char *k_b = plan_filter_kind_for_test(env.db_root, "default/d3sl",
        "[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}]",
        "note", 1,
        f, sizeof(f), o, sizeof(o), &cheap);
    ASSERT_EQ_STR(k_b, "scan",
        "D3 single-leaf dual: unindexed order_by → B5 demotion still fires → FULL_SCAN");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-d3-single-leaf-indexed-order", test_planD3_single_leaf_indexed_order)

/* A3: starts_with on a trigram-only field (no btree) with prefix >= 3 chars.
 * Before fix: pick_index_for_leaf returns -1 for OP_STARTS_WITH when only a
 * trigram index exists → plan_filter falls through to FP_FULL_SCAN → "scan".
 * After fix: pick_index_for_leaf returns IT_TRIGRAM for prefix >= 3 chars →
 * plan_filter sees the leaf as indexed → FP_PRIMARY_LEAF → "leaf".
 *
 * Object cm_a3: title:varchar:64 indexed with trigram only (no btree).
 * 20 rows with title="Show HN: ...", 80 rows with unrelated titles.
 * Criteria: [{title starts_with "Show HN"}] (7 chars >= 3 → trigram eligible).
 * Expected: kind="leaf", seed_field="title". */
static int test_planA3_trigram_starts_with(void) {
    TestEnv env = {0};
    /* trigram-only index on title: no btree */
    TestClient *tc = cm_setup(&env, "cm_a3",
        "\"title:varchar:64\"",
        "\"title:trigram\"");
    if (!tc) return 1;

    /* Insert 20 rows title="Show HN: <i>" and 80 unrelated rows. */
    char body[65536]; int p = 0, k = 0; char *resp = NULL;
    p += snprintf(body + p, sizeof(body) - p, "{");
    for (int i = 0; i < 20; i++) {
        p += snprintf(body + p, sizeof(body) - p,
            "%s\"k%d\":{\"title\":\"Show HN: item %d\"}",
            k == 0 ? "" : ",", k, i);
        k++;
    }
    for (int i = 0; i < 80; i++) {
        p += snprintf(body + p, sizeof(body) - p,
            ",\"k%d\":{\"title\":\"Ask HN: something %d\"}", k, i);
        k++;
    }
    p += snprintf(body + p, sizeof(body) - p, "}");
    char req[66560];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"cm_a3\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    char f[64] = {0}, o[16] = {0};
    /* Prefix "Show HN" is 7 chars >= 3 → trigram eligible.
     * Planner must return "leaf" (not "scan") and seed on "title". */
    const char *kind = plan_filter_kind_for_test(env.db_root, "default/cm_a3",
        "[{\"field\":\"title\",\"op\":\"starts_with\",\"value\":\"Show HN\"}]",
        NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kind, "leaf",
        "A3: starts_with on trigram-only field (prefix>=3) → PRIMARY_LEAF, not FULL_SCAN");
    ASSERT_EQ_STR(f, "title",
        "A3: seed field is 'title'");

    /* Also verify fetching=0 (count path) — same result. */
    memset(f, 0, sizeof(f)); memset(o, 0, sizeof(o));
    const char *kind_count = plan_filter_kind_for_test(env.db_root, "default/cm_a3",
        "[{\"field\":\"title\",\"op\":\"starts_with\",\"value\":\"Show HN\"}]",
        NULL, 0, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kind_count, "leaf",
        "A3 count: starts_with on trigram-only field stays leaf on count path too");

    /* Short prefix (<3 chars) must still fall to scan — no trigram extractable. */
    memset(f, 0, sizeof(f)); memset(o, 0, sizeof(o));
    const char *kind_short = plan_filter_kind_for_test(env.db_root, "default/cm_a3",
        "[{\"field\":\"title\",\"op\":\"starts_with\",\"value\":\"Sh\"}]",
        NULL, 1, f, sizeof(f), o, sizeof(o), NULL);
    ASSERT_EQ_STR(kind_short, "scan",
        "A3 short: prefix <3 chars → no usable trigram → FULL_SCAN");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-a3-trigram-starts-with", test_planA3_trigram_starts_with)
