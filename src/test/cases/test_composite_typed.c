#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "../../db/types.h"
#include "../test_client.h"
#include "../fixtures.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Shared helper — same pattern as test_planner_cost_model.c */
static TestClient *cm_setup(TestEnv *env, const char *obj,
                             const char *fields, const char *indexes) {
    if (test_env_start(env) != 0) { ASSERT_TRUE(0, "spawn"); return NULL; }
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(env); return NULL; }
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    char co[1024];
    snprintf(co, sizeof(co),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":8,\"max_key\":12,\"fields\":[%s],\"indexes\":[%s]}",
        obj, fields, indexes);
    tc_request(tc, co, &resp); free(resp); resp = NULL;
    return tc;
}

/* Test 1: int+long composite. Insert records, query with D1 prefix scan.
   Verify correct results and that D1 fires (fast response). */
static int test_composite_int_long(void) {
    TestEnv env = {0};
    TestClient *tc = cm_setup(&env, "cil",
        "\"score:int\",\"time:long\"",
        "\"score\",\"time\",\"score+time\"");
    if (!tc) return 1;

    /* Insert: score=100, time=1,2,3 and score=200, time=4,5 */
    for (int i = 0; i < 3; i++) {
        char req[256]; char *resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cil\","
            "\"key\":\"k%d\",\"value\":{\"score\":\"100\",\"time\":\"%d\"}}", i, i+1);
        tc_request(tc, req, &resp); free(resp);
    }
    for (int i = 0; i < 2; i++) {
        char req[256]; char *resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cil\","
            "\"key\":\"k%d\",\"value\":{\"score\":\"200\",\"time\":\"%d\"}}", i+3, i+4);
        tc_request(tc, req, &resp); free(resp);
    }

    /* D1 query: score=100 ORDER BY time ASC */
    char *r1 = NULL;
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cil\","
                    "\"criteria\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"100\"}],"
                    "\"order_by\":\"time\",\"order\":\"asc\",\"limit\":10}", &r1);
    /* Response should contain 3 entries with time=1,2,3 in order */
    ASSERT_CONTAINS(r1, "\"time\":1", "composite int+long: time=1 present");
    ASSERT_CONTAINS(r1, "\"time\":2", "composite int+long: time=2 present");
    ASSERT_CONTAINS(r1, "\"time\":3", "composite int+long: time=3 present");
    free(r1);

    /* COUNT with same criteria */
    char *r2 = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"cil\","
                    "\"criteria\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"100\"}]}", &r2);
    ASSERT_CONTAINS(r2, "3", "composite int+long COUNT: 3 matches");
    free(r2);

    tc_close(tc); test_env_stop(&env);
    return 0;
}

/* Test 2: varchar+int composite. Verify varchar prefix + D1 works. */
static int test_composite_varchar_int(void) {
    TestEnv env = {0};
    TestClient *tc = cm_setup(&env, "cvi",
        "\"name:varchar:16\",\"score:int\"",
        "\"name\",\"score\",\"name+score\"");
    if (!tc) return 1;

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cvi\",\"key\":\"a\",\"value\":{\"name\":\"alice\",\"score\":\"100\"}}", &resp); free(resp);
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cvi\",\"key\":\"b\",\"value\":{\"name\":\"alice\",\"score\":\"200\"}}", &resp); free(resp);
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cvi\",\"key\":\"c\",\"value\":{\"name\":\"bob\",\"score\":\"50\"}}", &resp); free(resp);

    /* D1: name=alice ORDER BY score ASC */
    char *r1 = NULL;
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cvi\","
                    "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"alice\"}],"
                    "\"order_by\":\"score\",\"order\":\"asc\",\"limit\":10}", &r1);
    ASSERT_CONTAINS(r1, "alice", "composite varchar+int: alice present");
    ASSERT_CONTAINS(r1, "100", "composite varchar+int: score=100 present");
    free(r1);

    tc_close(tc); test_env_stop(&env);
    return 0;
}

/* Test join: composite local key (int+long) joins to remote object
 * with the same composite index. Both sides use binary encoding. */
static int test_composite_join(void) {
    TestEnv env = {0};
    TestClient *tc = cm_setup(&env, "cj_driver",
        "\"score:int\",\"time:long\"",
        "\"score\",\"time\",\"score+time\"");
    if (!tc) return 1;

    /* Create remote object with same fields + composite */
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cj_remote\","
                    "\"splits\":8,\"max_key\":12,"
                    "\"fields\":[\"score:int\",\"time:long\",\"val:varchar:8\"],"
                    "\"indexes\":[\"score\",\"score+time\"]}", &resp); free(resp);

    /* Insert driver records */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cj_driver\","
                    "\"key\":\"d1\",\"value\":{\"score\":\"100\",\"time\":\"1\"}}", &resp); free(resp);
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cj_driver\","
                    "\"key\":\"d2\",\"value\":{\"score\":\"100\",\"time\":\"2\"}}", &resp); free(resp);

    /* Insert remote records */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cj_remote\","
                    "\"key\":\"r1\",\"value\":{\"score\":\"100\",\"time\":\"1\",\"val\":\"first\"}}", &resp); free(resp);
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cj_remote\","
                    "\"key\":\"r2\",\"value\":{\"score\":\"100\",\"time\":\"2\",\"val\":\"second\"}}", &resp); free(resp);
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cj_remote\","
                    "\"key\":\"r3\",\"value\":{\"score\":\"200\",\"time\":\"1\",\"val\":\"other\"}}", &resp); free(resp);

    /* Join: driver.score = remote.score AND driver.time = remote.time */
    char *r1 = NULL;
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cj_driver\","
                    "\"criteria\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"100\"}],"
                    "\"order_by\":\"time\",\"order\":\"asc\",\"limit\":10,"
                    "\"fields\":[\"time\"],"
                    "\"join\":[{\"as\":\"r\",\"object\":\"cj_remote\","
                    "\"local\":\"score+time\",\"remote\":\"score+time\","
                    "\"fields\":[\"val\"]}]}", &r1);
    ASSERT_CONTAINS(r1, "first", "composite join: val=first for score=100,time=1");
    ASSERT_CONTAINS(r1, "second", "composite join: val=second for score=100,time=2");
    free(r1);

    tc_close(tc); test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-composite-int-long", test_composite_int_long)
TEST_REGISTER("test-composite-varchar-int", test_composite_varchar_int)
TEST_REGISTER("test-composite-join", test_composite_join)
