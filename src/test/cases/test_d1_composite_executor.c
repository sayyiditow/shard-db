/* test_d1_composite_executor.c
 *
 * Phase 1c step 3/6: D1 composite-prefix sorted-scan executor.
 *
 * Verifies that find {by="alice"} order_by time [asc|desc] limit N
 * returns the correct N rows in the correct order when a composite
 * "by+time" btree index exists — exercising the new
 * find_via_composite_prefix() executor path.
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

/* Simple substring check: does `haystack` contain `needle`? */
static int has(const char *haystack, const char *needle) {
    return haystack && needle && strstr(haystack, needle) != NULL;
}

/* Return the position (index) at which `needle` first appears in
   `haystack`, or -1 if not found. Used to assert ordering. */
static int pos_of(const char *haystack, const char *needle) {
    if (!haystack || !needle) return -1;
    const char *p = strstr(haystack, needle);
    return p ? (int)(p - haystack) : -1;
}

/* ── fixture setup ───────────────────────────────────────────── */

/* Create object cm_d1exec: fields by:varchar:16, time:long.
   Indexes: "by" (btree) and composite "by+time" (btree).
   Insert 50 rows for by="alice" with time values 1000..49*1000 (step 1000),
   and 50 rows for by="bob" with varying time values.
   Keys: "a0".."a49" for alice, "b0".."b49" for bob. */
static TestClient *d1_setup(TestEnv *env) {
    if (test_env_start(env) != 0) { ASSERT_TRUE(0, "daemon spawn"); return NULL; }
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(env); return NULL; }

    char *r = do_req(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}"); free(r);
    r = do_req(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cm_d1exec\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"by:varchar:16\",\"time:long\"],"
        "\"indexes\":[\"by\",\"by+time\"]}");
    free(r);

    /* Build a bulk-insert JSON for alice (50 rows, time 1000..50000). */
    char body[131072]; size_t p = 0;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 50; i++) {
        long t = (long)(i+1) * 1000;   /* 1000, 2000, ..., 50000 */
        SB_APPEND(body, p, sizeof(body),
            "%s\"a%d\":{\"by\":\"alice\",\"time\":%ld}",
            i == 0 ? "" : ",", i, t);
    }
    /* Append 50 bob rows with shuffled times so they don't accidentally sort the same. */
    for (int i = 0; i < 50; i++) {
        long t = (long)(100 - i) * 1000;   /* 100000, 99000, ..., 51000 */
        SB_APPEND(body, p, sizeof(body),
            ",\"b%d\":{\"by\":\"bob\",\"time\":%ld}", i, t);
    }
    SB_APPEND(body, p, sizeof(body), "}");

    char req[131200];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"cm_d1exec\","
        "\"records\":%s}", body);
    r = do_req(tc, req); free(r);
    return tc;
}

/* ── test cases ──────────────────────────────────────────────── */

/* D1 ASC: find {by="alice"} order_by time asc limit 5
   Expected: the 5 rows with the LOWEST time values for alice,
   i.e. a0(t=1000), a1(t=2000), a2(t=3000), a3(t=4000), a4(t=5000),
   in that order and without any bob rows. */
static int test_d1_composite_exec_asc(void) {
    TestEnv env = {0};
    TestClient *tc = d1_setup(&env);
    if (!tc) return 1;

    char *r = do_req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cm_d1exec\","
        "\"criteria\":[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"alice\"}],"
        "\"order_by\":\"time\",\"order\":\"asc\",\"limit\":5}");

    /* Must contain the 5 alice rows with smallest times. */
    ASSERT_CONTAINS(r, "\"key\":\"a0\"", "D1 asc: a0 present");
    ASSERT_CONTAINS(r, "\"key\":\"a1\"", "D1 asc: a1 present");
    ASSERT_CONTAINS(r, "\"key\":\"a2\"", "D1 asc: a2 present");
    ASSERT_CONTAINS(r, "\"key\":\"a3\"", "D1 asc: a3 present");
    ASSERT_CONTAINS(r, "\"key\":\"a4\"", "D1 asc: a4 present");

    /* Must NOT contain a5 (it's row 6) or any bob row. */
    ASSERT_TRUE(!has(r, "\"key\":\"a5\""),  "D1 asc: a5 absent (limit=5)");
    ASSERT_TRUE(!has(r, "\"key\":\"b0\""),  "D1 asc: no bob rows");

    /* Order: a0 must appear before a1, a1 before a2, etc. */
    ASSERT_TRUE(pos_of(r, "\"a0\"") < pos_of(r, "\"a1\""), "D1 asc: a0 < a1 in output");
    ASSERT_TRUE(pos_of(r, "\"a1\"") < pos_of(r, "\"a2\""), "D1 asc: a1 < a2 in output");
    ASSERT_TRUE(pos_of(r, "\"a2\"") < pos_of(r, "\"a3\""), "D1 asc: a2 < a3 in output");
    ASSERT_TRUE(pos_of(r, "\"a3\"") < pos_of(r, "\"a4\""), "D1 asc: a3 < a4 in output");

    /* Must not contain error. */
    ASSERT_TRUE(!has(r, "\"error\""), "D1 asc: no error");

    free(r);
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-d1-composite-exec-asc", test_d1_composite_exec_asc)

/* D1 DESC: find {by="alice"} order_by time desc limit 5
   Expected: a49(t=50000), a48(t=49000), a47(t=48000), a46(t=47000), a45(t=46000)
   in descending order. */
static int test_d1_composite_exec_desc(void) {
    TestEnv env = {0};
    TestClient *tc = d1_setup(&env);
    if (!tc) return 1;

    char *r = do_req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cm_d1exec\","
        "\"criteria\":[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"alice\"}],"
        "\"order_by\":\"time\",\"order\":\"desc\",\"limit\":5}");

    /* Must contain the 5 alice rows with the LARGEST times. */
    ASSERT_CONTAINS(r, "\"key\":\"a49\"", "D1 desc: a49 present");
    ASSERT_CONTAINS(r, "\"key\":\"a48\"", "D1 desc: a48 present");
    ASSERT_CONTAINS(r, "\"key\":\"a47\"", "D1 desc: a47 present");
    ASSERT_CONTAINS(r, "\"key\":\"a46\"", "D1 desc: a46 present");
    ASSERT_CONTAINS(r, "\"key\":\"a45\"", "D1 desc: a45 present");

    /* Must NOT contain a44 (it's row 6 in desc) or any bob row. */
    ASSERT_TRUE(!has(r, "\"key\":\"a44\""), "D1 desc: a44 absent (limit=5)");
    ASSERT_TRUE(!has(r, "\"key\":\"b0\""),  "D1 desc: no bob rows");

    /* Order: a49 must appear before a48, a48 before a47, etc. */
    ASSERT_TRUE(pos_of(r, "\"a49\"") < pos_of(r, "\"a48\""), "D1 desc: a49 < a48");
    ASSERT_TRUE(pos_of(r, "\"a48\"") < pos_of(r, "\"a47\""), "D1 desc: a48 < a47");
    ASSERT_TRUE(pos_of(r, "\"a47\"") < pos_of(r, "\"a46\""), "D1 desc: a47 < a46");
    ASSERT_TRUE(pos_of(r, "\"a46\"") < pos_of(r, "\"a45\""), "D1 desc: a46 < a45");

    ASSERT_TRUE(!has(r, "\"error\""), "D1 desc: no error");

    free(r);
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-d1-composite-exec-desc", test_d1_composite_exec_desc)

/* D1 isolation: bob rows must never appear in an alice query,
   and alice rows must never appear in a bob query.
   Also verifies limit=10 with offset=5 on alice asc:
   expects a5..a14 (time 6000..15000). */
static int test_d1_composite_exec_offset(void) {
    TestEnv env = {0};
    TestClient *tc = d1_setup(&env);
    if (!tc) return 1;

    char *r = do_req(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cm_d1exec\","
        "\"criteria\":[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"alice\"}],"
        "\"order_by\":\"time\",\"order\":\"asc\","
        "\"offset\":5,\"limit\":10}");

    /* Rows a5..a14 (time 6000..15000) must all be present. */
    ASSERT_CONTAINS(r, "\"key\":\"a5\"",  "D1 offset: a5 present");
    ASSERT_CONTAINS(r, "\"key\":\"a14\"", "D1 offset: a14 present");

    /* a0..a4 skipped by offset=5. */
    ASSERT_TRUE(!has(r, "\"key\":\"a0\""), "D1 offset: a0 absent (skipped)");
    ASSERT_TRUE(!has(r, "\"key\":\"a4\""), "D1 offset: a4 absent (skipped)");

    /* a15 beyond limit. */
    ASSERT_TRUE(!has(r, "\"key\":\"a15\""), "D1 offset: a15 absent (beyond limit)");

    /* No bob rows. */
    ASSERT_TRUE(!has(r, "\"key\":\"b"),    "D1 offset: no bob rows");

    ASSERT_TRUE(!has(r, "\"error\""), "D1 offset: no error");

    free(r);
    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-d1-composite-exec-offset", test_d1_composite_exec_offset)
