/* src/test/cases/test_bm_intersect_count.c
 *
 * Verify bitmap word-level intersect popcount produces correct counts
 * for multi-field AND on bitmap-indexed fields. Exercises the
 * bm_popcount_intersect fast path added for all-bitmap eq/in COUNT.
 *
 * the tests declare `flag` bare, which promotes to bitmap, so the
 * intersect popcount fast path fires when both leaves are bitmap-
 * indexed and all ops are eq/in. */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Setup: spawn daemon, add-dir, create-object, return connected client. */
static TestClient *cm_setup(TestEnv *env, const char *obj,
                            const char *fields, const char *indexes) {
    if (test_env_start(env) != 0) { ASSERT_TRUE(0, "spawn"); return NULL; }
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(env); return NULL; }
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
               &resp); free(resp); resp = NULL;
    char co[1024];
    snprintf(co, sizeof(co),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":8,\"max_key\":12,\"fields\":[%s],\"indexes\":[%s]}",
        obj, fields, indexes);
    tc_request(tc, co, &resp); free(resp); resp = NULL;
    return tc;
}

/* Insert a single record. */
static void bm_insert(TestClient *tc, const char *obj,
                      const char *key, const char *flag, const char *region) {
    char *resp = NULL;
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"flag\":\"%s\",\"region\":\"%s\"}}",
        obj, key, flag, region);
    tc_request(tc, req, &resp);
    free(resp);
}

static int test_bm_intersect_eq(void) {
    TestEnv env = {0};
    /* bare bool `flag` promotes to bitmap; region:bitmap is explicit.
     * With both bitmap-indexed, the planner fires all_bitmap=1
     * and count eq+eq drops into bm_popcount_intersect. */
    TestClient *tc = cm_setup(&env, "bmix_eq",
        "\"flag:bool\",\"region:varchar:8\"",
        "\"flag\",\"region:bitmap\"");
    if (!tc) return 1;

    /* Insert 10 records with known distribution:
     *   4: flag=true,  region=alpha
     *   3: flag=true,  region=beta
     *   2: flag=false, region=alpha
     *   1: flag=false, region=beta
     */
    bm_insert(tc, "bmix_eq", "k1", "true", "alpha");
    bm_insert(tc, "bmix_eq", "k2", "true", "alpha");
    bm_insert(tc, "bmix_eq", "k3", "true", "alpha");
    bm_insert(tc, "bmix_eq", "k4", "true", "alpha");
    bm_insert(tc, "bmix_eq", "k5", "true", "beta");
    bm_insert(tc, "bmix_eq", "k6", "true", "beta");
    bm_insert(tc, "bmix_eq", "k7", "true", "beta");
    bm_insert(tc, "bmix_eq", "k8", "false", "alpha");
    bm_insert(tc, "bmix_eq", "k9", "false", "alpha");
    bm_insert(tc, "bmix_eq", "ka", "false", "beta");

    /* Diagnostic: verify records are present. */
    char *rd = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\","
                    "\"object\":\"bmix_eq\"}", &rd);
    ASSERT_CONTAINS(rd, "10", "diag: total count → 10");
    free(rd);

    /* Diagnostic: single-field bitmap count should work. */
    rd = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\","
                    "\"object\":\"bmix_eq\","
                    "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\","
                    "\"value\":\"true\"}]}", &rd);
    ASSERT_CONTAINS(rd, "7", "diag: single-field count flag=true → 7");
    free(rd);

    rd = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\","
                    "\"object\":\"bmix_eq\","
                    "\"criteria\":[{\"field\":\"region\",\"op\":\"eq\","
                    "\"value\":\"alpha\"}]}", &rd);
    ASSERT_CONTAINS(rd, "6", "diag: single-field count region=alpha → 6");
    free(rd);

    /* EQ+EQ: flag=true AND region=alpha → 4 */
    char *r1 = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\","
                    "\"object\":\"bmix_eq\","
                    "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\","
                    "\"value\":\"true\"},"
                    "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"alpha\"}]}",
               &r1);
    /* Debug: print raw response. */
    ASSERT_CONTAINS(r1, "4", "EQ+EQ: true & alpha → 4");
    free(r1);

    /* EQ+EQ: flag=false AND region=alpha → 2 */
    char *r2 = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\","
                    "\"object\":\"bmix_eq\","
                    "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\","
                    "\"value\":\"false\"},"
                    "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"alpha\"}]}",
               &r2);
    ASSERT_CONTAINS(r2, "2", "EQ+EQ: false & alpha → 2");
    free(r2);

    /* EQ+EQ: flag=false AND region=beta → 1 */
    char *r3 = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\","
                    "\"object\":\"bmix_eq\","
                    "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\","
                    "\"value\":\"false\"},"
                    "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"beta\"}]}",
               &r3);
    ASSERT_CONTAINS(r3, "1", "EQ+EQ: false & beta → 1");
    free(r3);

    /* Value not in dict: region=gamma → 0 */
    char *r4 = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\","
                    "\"object\":\"bmix_eq\","
                    "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\","
                    "\"value\":\"true\"},"
                    "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"gamma\"}]}",
               &r4);
    ASSERT_CONTAINS(r4, "0", "EQ+EQ dict miss → 0");
    free(r4);

    tc_close(tc); test_env_stop(&env);
    return 0;
}

static int test_bm_intersect_in(void) {
    TestEnv env = {0};
    TestClient *tc = cm_setup(&env, "bmix_in",
        "\"flag:bool\",\"region:varchar:8\"",
        "\"flag\",\"region:bitmap\"");
    if (!tc) return 1;

    /* Same distribution as EQ test */
    bm_insert(tc, "bmix_in", "k1", "true", "alpha");
    bm_insert(tc, "bmix_in", "k2", "true", "alpha");
    bm_insert(tc, "bmix_in", "k3", "true", "alpha");
    bm_insert(tc, "bmix_in", "k4", "true", "alpha");
    bm_insert(tc, "bmix_in", "k5", "true", "beta");
    bm_insert(tc, "bmix_in", "k6", "true", "beta");
    bm_insert(tc, "bmix_in", "k7", "true", "beta");
    bm_insert(tc, "bmix_in", "k8", "false", "alpha");
    bm_insert(tc, "bmix_in", "k9", "false", "alpha");
    bm_insert(tc, "bmix_in", "ka", "false", "beta");

    /* IN+EQ: flag=true AND region IN (alpha,beta) → all 7 true records = 7 */
    char *r1 = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\","
                    "\"object\":\"bmix_in\","
                    "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\","
                    "\"value\":\"true\"},"
                    "{\"field\":\"region\",\"op\":\"in\","
                    "\"value\":\"alpha,beta\"}]}", &r1);
    ASSERT_CONTAINS(r1, "7", "EQ+IN: true & (alpha|beta) → 7");
    free(r1);

    /* IN+IN: flag IN (false) AND region IN (alpha,beta)
     * → false+alpha(2)+false+beta(1)=3 */
    char *r2 = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\","
                    "\"object\":\"bmix_in\","
                    "\"criteria\":[{\"field\":\"flag\",\"op\":\"in\","
                    "\"value\":\"false\"},"
                    "{\"field\":\"region\",\"op\":\"in\","
                    "\"value\":\"alpha,beta\"}]}", &r2);
    ASSERT_CONTAINS(r2, "3", "IN+IN: false & (alpha|beta) → 3");
    free(r2);

    /* IN with value not in dict: flag=true AND region IN (alpha,gamma)
     * → only alpha matches → 4 */
    char *r3 = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\","
                    "\"object\":\"bmix_in\","
                    "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\","
                    "\"value\":\"true\"},"
                    "{\"field\":\"region\",\"op\":\"in\","
                    "\"value\":\"alpha,gamma\"}]}", &r3);
    ASSERT_CONTAINS(r3, "4", "EQ+IN with partial dict miss → 4");
    free(r3);

    tc_close(tc); test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-bm-intersect-eq", test_bm_intersect_eq)
TEST_REGISTER("test-bm-intersect-in", test_bm_intersect_in)
