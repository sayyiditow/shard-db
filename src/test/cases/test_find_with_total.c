/* src/test/cases/test_find_with_total.c
 * Phase 1d step 1/3 — server wiring + JSON shape.
 *
 * Verifies that:
 *   - find without "total" → bare array [...] (unchanged)
 *   - find with "total":true → {"rows":[...], "total":null}
 *   - find with both "total":true and "cursor":{} → error
 *   - aggregate without "total" → bare array / bare object (unchanged)
 *   - aggregate with "total":true → {"rows":[...], "total":null}
 *   - fetch with "total":true → {"rows":[...], "total":null}
 *
 * The "total" value is null for Phase 1d.1; real totals come in 1d.2/1d.3.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ helpers */

static TestClient *setup_obj(TestEnv *env, const char *obj,
                             const char *fields, const char *indexes) {
    if (test_env_start(env) != 0) { ASSERT_TRUE(0, "spawn"); return NULL; }
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(env); return NULL; }
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;
    char co[1024];
    snprintf(co, sizeof(co),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":8,\"max_key\":12,\"fields\":[%s],\"indexes\":[%s]}",
        obj, fields, indexes);
    tc_request(tc, co, &resp); free(resp); resp = NULL;
    return tc;
}

/* Insert n rows with tag="t<i>" and score=i into the given object. */
static void insert_rows(TestClient *tc, const char *obj, int n) {
    char body[65536]; int p = 0, k = 0; char *resp = NULL;
    p += snprintf(body + p, sizeof(body) - p, "{");
    for (int i = 0; i < n; i++) {
        p += snprintf(body + p, sizeof(body) - p,
                      "%s\"r%d\":{\"tag\":\"t%d\",\"score\":%d}",
                      k == 0 ? "" : ",", k, i, i * 10);
        k++;
    }
    p += snprintf(body + p, sizeof(body) - p, "}");
    char req[66560];
    snprintf(req, sizeof(req),
             "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
             "\"object\":\"%s\",\"records\":%s}", obj, body);
    tc_request(tc, req, &resp); free(resp);
}

/* ------------------------------------------------------------------ tests */

/* find without "total" flag → bare array [...]  (behaviour preserved) */
static int test_find_without_total(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "fwt1",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\"");
    if (!tc) return 1;
    insert_rows(tc, "fwt1", 5);

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fwt1\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"t0\"}]}",
        &resp);
    /* Must be a JSON array — starts with [ */
    ASSERT_NOT_NULL(resp, "find response not null");
    ASSERT_TRUE(resp[0] == '[', "no-total find → bare array starts with [");
    ASSERT_TRUE(strstr(resp, "\"rows\"") == NULL,
                "no-total find → no rows wrapper");
    ASSERT_TRUE(strstr(resp, "\"total\"") == NULL,
                "no-total find → no total field");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-find-without-total", test_find_without_total)

/* find with "total":true → {"rows":[...], "total":null} */
static int test_find_with_total_shape(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "fwt2",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\"");
    if (!tc) return 1;
    insert_rows(tc, "fwt2", 5);

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fwt2\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"t0\"}],"
        "\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "find+total response not null");
    /* Shape must be {"rows":[...], "total":null} */
    ASSERT_CONTAINS(resp, "\"rows\"", "find+total → rows key present");
    ASSERT_CONTAINS(resp, "\"total\":null", "find+total → total:null placeholder");
    /* The rows value must be an array starting with [ */
    const char *rows_pos = strstr(resp, "\"rows\":");
    ASSERT_NOT_NULL(rows_pos, "rows key found");
    const char *after_rows = rows_pos + strlen("\"rows\":");
    while (*after_rows == ' ') after_rows++;
    ASSERT_TRUE(*after_rows == '[', "find+total → rows value is array");
    /* Must contain our row */
    ASSERT_CONTAINS(resp, "\"key\":\"r0\"", "find+total → row r0 present");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-find-with-total-shape", test_find_with_total_shape)

/* find with "total":true and no criteria match → {"rows":[], "total":null} */
static int test_find_with_total_empty(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "fwt3",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\"");
    if (!tc) return 1;
    insert_rows(tc, "fwt3", 3);

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fwt3\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"missing\"}],"
        "\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "find+total empty response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "find+total empty → rows key");
    ASSERT_CONTAINS(resp, "\"total\":null", "find+total empty → total:null");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-find-with-total-empty", test_find_with_total_empty)

/* find with "total":true AND "cursor":{} → error (mutually exclusive) */
static int test_find_total_cursor_conflict(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "fwt4",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\",\"score\"");
    if (!tc) return 1;
    insert_rows(tc, "fwt4", 5);

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fwt4\","
        "\"criteria\":[],\"order_by\":\"score\","
        "\"total\":true,\"cursor\":{}}",
        &resp);
    ASSERT_NOT_NULL(resp, "total+cursor response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "total+cursor → error response");
    ASSERT_CONTAINS(resp, "mutually exclusive",
                    "total+cursor → mentions mutually exclusive");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-find-total-cursor-conflict", test_find_total_cursor_conflict)

/* find with "total":true and format=csv → error */
static int test_find_total_csv_conflict(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "fwt5",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\"");
    if (!tc) return 1;
    insert_rows(tc, "fwt5", 3);

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fwt5\","
        "\"criteria\":[],\"format\":\"csv\",\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "total+csv response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "total+csv → error response");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-find-total-csv-conflict", test_find_total_csv_conflict)

/* aggregate without "total" → bare array or bare object (unchanged) */
static int test_aggregate_without_total(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "awt1",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\"");
    if (!tc) return 1;
    insert_rows(tc, "awt1", 5);

    char *resp = NULL;
    /* No group_by → single-bucket response: bare object {...} */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"awt1\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "agg no-total response not null");
    ASSERT_TRUE(resp[0] == '{', "agg no-total no-group → bare object");
    ASSERT_TRUE(strstr(resp, "\"rows\"") == NULL,
                "agg no-total → no rows wrapper");
    free(resp); resp = NULL;

    /* group_by → bare array [...] */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"awt1\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"tag\"]}",
        &resp);
    ASSERT_NOT_NULL(resp, "agg no-total group resp not null");
    ASSERT_TRUE(resp[0] == '[', "agg no-total group → bare array");
    ASSERT_TRUE(strstr(resp, "\"rows\"") == NULL,
                "agg no-total group → no rows wrapper");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-aggregate-without-total", test_aggregate_without_total)

/* aggregate with "total":true and group_by → {"rows":[...], "total":null} */
static int test_aggregate_with_total_group(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "awt2",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\"");
    if (!tc) return 1;
    insert_rows(tc, "awt2", 5);

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"awt2\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"tag\"],\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "agg+total+group response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "agg+total group → rows key");
    ASSERT_CONTAINS(resp, "\"total\":null", "agg+total group → total:null");
    /* rows value should be an array */
    const char *rows_pos = strstr(resp, "\"rows\":");
    ASSERT_NOT_NULL(rows_pos, "rows key found in agg response");
    const char *after = rows_pos + strlen("\"rows\":");
    while (*after == ' ') after++;
    ASSERT_TRUE(*after == '[', "agg+total group → rows value is array");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-aggregate-with-total-group", test_aggregate_with_total_group)

/* aggregate with "total":true and no group_by → {"rows":{...}, "total":null} */
static int test_aggregate_with_total_nogroup(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "awt3",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\"");
    if (!tc) return 1;
    insert_rows(tc, "awt3", 5);

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"awt3\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "agg+total no-group response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "agg+total no-group → rows key");
    ASSERT_CONTAINS(resp, "\"total\":null", "agg+total no-group → total:null");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-aggregate-with-total-nogroup", test_aggregate_with_total_nogroup)

/* fetch with "total":true → {"rows":[...], "total":null} */
static int test_fetch_with_total(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "fcht1",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\"");
    if (!tc) return 1;
    insert_rows(tc, "fcht1", 5);

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"fetch\",\"dir\":\"default\",\"object\":\"fcht1\","
        "\"limit\":3,\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "fetch+total response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "fetch+total → rows key");
    ASSERT_CONTAINS(resp, "\"total\":null", "fetch+total → total:null");
    /* Must NOT contain "cursor" key when total mode is active */
    ASSERT_TRUE(strstr(resp, "\"cursor\"") == NULL,
                "fetch+total → no cursor in total-mode response");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-fetch-with-total", test_fetch_with_total)

/* fetch without "total" → {"results":[...]} (unchanged) */
static int test_fetch_without_total(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "fcht2",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\"");
    if (!tc) return 1;
    insert_rows(tc, "fcht2", 3);

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"fetch\",\"dir\":\"default\",\"object\":\"fcht2\","
        "\"limit\":3}",
        &resp);
    ASSERT_NOT_NULL(resp, "fetch no-total response not null");
    ASSERT_CONTAINS(resp, "\"results\"", "fetch no-total → results key (unchanged)");
    ASSERT_TRUE(strstr(resp, "\"rows\"") == NULL,
                "fetch no-total → no rows wrapper");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-fetch-without-total", test_fetch_without_total)
