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
        char *out_field, size_t fsz, char *out_order, size_t osz);

/* A1: 1 selective btree eq → leaf, seeded on `tag`. */
static int test_planA1_selective_leaf(void) {
    TestEnv env={0};
    TestClient *tc = cm_setup(&env, "a1", "\"tag:varchar:8\"", "\"tag\"");
    if (!tc) return 1;
    cm_insert_tags(tc, "a1");
    char f[64]={0}, o[16]={0};
    const char *k = plan_filter_kind_for_test(env.db_root, "default/a1",
        "{\"tag\":\"rare\"}", NULL, 1, f, sizeof(f), o, sizeof(o));
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
        "{\"active\":true}", NULL, 1, f, sizeof(f), o, sizeof(o));
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
        NULL, 1, f, sizeof(f), o, sizeof(o));
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
        NULL, 0, f,sizeof(f), o,sizeof(o));
    ASSERT_EQ_STR(kc, "intersect", "B1 count: two selective → intersect");

    /* find path (fetching=1): most-selective seeds PRIMARY_LEAF, other post-filters */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kf = plan_filter_kind_for_test(env.db_root,"default/b1",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"tag2\",\"op\":\"eq\",\"value\":\"rare\"}]",
        NULL, 1, f,sizeof(f), o,sizeof(o));
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
        NULL, 1, f,sizeof(f), o,sizeof(o));
    ASSERT_EQ_STR(kf, "leaf", "B2 find: selective btree seeds, bitmap post-filters");
    ASSERT_EQ_STR(f, "tag", "B2 find: seed is tag (btree), not active (bitmap)");

    /* fetching=0: same — selective btree wins over broad bitmap */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b2",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o));
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
        NULL, 1, f,sizeof(f), o,sizeof(o));
    ASSERT_EQ_STR(kf, "intersect", "B3 find: pure-bitmap AND → intersect");

    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b3",
        "[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"},"
         "{\"field\":\"flagged\",\"op\":\"eq\",\"value\":\"true\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o));
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
        NULL, 1, f,sizeof(f), o,sizeof(o));
    ASSERT_EQ_STR(kf, "leaf", "B4 find: selective btree seeds, non-indexed post-filters");
    ASSERT_EQ_STR(f, "tag", "B4 find: seed is tag");

    /* fetching=0: same — one selective + one non-indexed → leaf (can't intersect non-indexed) */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *kc = plan_filter_kind_for_test(env.db_root,"default/b4",
        "[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"},"
         "{\"field\":\"note\",\"op\":\"contains\",\"value\":\"x\"}]",
        NULL, 0, f,sizeof(f), o,sizeof(o));
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
        NULL, 1, f,sizeof(f), o,sizeof(o));
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
        NULL, 1, f, sizeof(f), o, sizeof(o));
    ASSERT_EQ_STR(k_str, "leaf",
        "A4: saturated trigram-contains stays PRIMARY_LEAF (never FULL_SCAN)");

    /* Also check fetching=0 (count path) — same result. */
    memset(f,0,sizeof(f)); memset(o,0,sizeof(o));
    const char *k_count = plan_filter_kind_for_test(env.db_root, "default/a4tg",
        "[{\"field\":\"title\",\"op\":\"contains\",\"value\":\"abc\"}]",
        NULL, 0, f, sizeof(f), o, sizeof(o));
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
        NULL, 0, f, sizeof(f), o, sizeof(o));
    ASSERT_EQ_STR(kc, "leaf",
        "BCS count: n_indexed=2, exactly 1 selective → PRIMARY_LEAF (not intersect)");
    ASSERT_EQ_STR(f, "tag",
        "BCS count: seed is the selective field `tag`");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-plan-bcs-count-one-selective-leaf",
              test_planBCS_count_one_selective_leaf)
