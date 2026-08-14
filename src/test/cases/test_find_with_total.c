/* src/test/cases/test_find_with_total.c
 * Phase 1d steps 1/3 and 2/3 — server wiring + JSON shape + real totals.
 *
 * Phase 1d.1 tests:
 *   - find without "total" → bare array [...] (unchanged)
 *   - find with "total":true → {"rows":[...], "total":null}  (envelope shape)
 *   - find with both "total":true and "cursor":{} → error
 *   - aggregate without "total" → bare array / bare object (unchanged)
 *   - aggregate with "total":true → {"rows":[...], "total":N} (real value since 1d.3)
 *   - fetch with "total":true → {"rows":[...], "total":null}
 *
 * Phase 1d.2 tests (real totals):
 *   A1: FP_PRIMARY_LEAF + ORDER_NONE → total = idx_count_for_leaf(seed)
 *   A2: FP_BITMAP_SMALLER           → total = bm_popcount (bitmap count)
 *   B1: FP_INTERSECT                → total = |KeySet| after intersection
 *   C1: FP_UNION                    → total = |KeySet| after OR-union
 *   A5: FP_FULL_SCAN (no index)     → total = null
 *   D1: FP_ORDER_COMPOSITE          → total = null (spec)
 *   D3: FP_ORDER_INDEX_WALK         → total = null (spec)
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"   /* SB_APPEND — safe StringBuilder vs CodeQL snprintf-overflow flag */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ helpers */

/* Forward declarations for helpers defined in the 1d.2 section below. */
static int extract_total(const char *resp);
static int count_agg_rows(const char *resp);

static TestClient *setup_obj(TestEnv *env, const char *obj,
                             const char *fields, const char *indexes) {
    if (test_env_start(env) != 0) { ASSERT_TRUE(0, "spawn"); return NULL; }
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(env); return NULL; }
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
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
    char body[65536]; size_t p = 0; int k = 0; char *resp = NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < n; i++) {
        SB_APPEND(body, p, sizeof(body),
                  "%s\"r%d\":{\"tag\":\"t%d\",\"score\":%d}",
                  k == 0 ? "" : ",", k, i, i * 10);
        k++;
    }
    SB_APPEND(body, p, sizeof(body), "}");
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
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"rows\"") == NULL,
                "no-total find → no rows wrapper");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"total\"") == NULL,
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
    /* Shape must be {"rows":[...], "total":<value>}. After fp_compute_total
     * the value is the real match count (1 for tag=t0); pre-1d.2 it was
     * a null placeholder.  Either is shape-correct; just require the
     * "total" key be present. */
    ASSERT_CONTAINS(resp, "\"rows\"", "find+total → rows key present");
    ASSERT_CONTAINS(resp, "\"total\":", "find+total → total key present");
    /* The rows value must be an array starting with [ */
    const char *rows_pos = SAFE_STRSTR(resp, "\"rows\":");
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

/* find with "total":true and no criteria match → {"rows":[], "total":0}
   (indexed field: planner picks PRIMARY_LEAF, idx_count returns 0 = exact count) */
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
    /* tag is indexed → PRIMARY_LEAF → idx_count_for_leaf(missing) = 0, not null */
    ASSERT_CONTAINS(resp, "\"total\":0", "find+total empty → total:0 (indexed, zero matches)");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-find-with-total-empty", test_find_with_total_empty)

/* find with "total":true AND "cursor":{} → now allowed (mutual exclusion lifted) */
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
    ASSERT_TRUE(SAFE_STRSTR(resp, "mutually exclusive") == NULL,
                "total+cursor → no longer an error");
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
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"rows\"") == NULL,
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
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"rows\"") == NULL,
                "agg no-total group → no rows wrapper");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-aggregate-without-total", test_aggregate_without_total)

/* aggregate with "total":true and group_by → {"rows":[...], "total":N} */
/* insert_rows inserts 5 rows with tag="t0".."t4" (all distinct) → 5 groups. */
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
    /* Now returns real total = 5 distinct groups (Phase 1d.3). */
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, 5, "agg+total group → total = 5 distinct groups");
    /* rows value should be an array */
    const char *rows_pos = SAFE_STRSTR(resp, "\"rows\":");
    ASSERT_NOT_NULL(rows_pos, "rows key found in agg response");
    const char *after = rows_pos + strlen("\"rows\":");
    while (*after == ' ') after++;
    ASSERT_TRUE(*after == '[', "agg+total group → rows value is array");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-aggregate-with-total-group", test_aggregate_with_total_group)

/* aggregate with "total":true and no group_by → {"rows":{...}, "total":1} */
/* No group_by always produces exactly one scalar row → total = 1. */
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
    /* Now returns real total = 1 (single scalar row, Phase 1d.3). */
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, 1, "agg+total no-group → total = 1 (scalar row)");
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
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"cursor\"") == NULL,
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
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"rows\"") == NULL,
                "fetch no-total → no rows wrapper");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-fetch-without-total", test_fetch_without_total)

/* ================================================================
 * Phase 1d.2 tests: real total values per plan kind
 * ================================================================ */

/* Extract integer from "total":N in a JSON response.
   Returns the integer value, or -999 if "total":null, or -1 on error. */
static int extract_total(const char *resp) {
    if (!resp) return -1;
    const char *p = SAFE_STRSTR(resp, "\"total\":");
    if (!p) return -1;
    p += 8; /* skip "total": */
    while (*p == ' ') p++;
    if (strncmp(p, "null", 4) == 0) return -999;
    return atoi(p);
}

/* Count rows in the rows array — count occurrences of "key": to proxy row count. */
static int count_rows_in_resp(const char *resp) {
    if (!resp) return 0;
    const char *rows_start = SAFE_STRSTR(resp, "\"rows\":");
    if (!rows_start) {
        /* bare array */
        rows_start = resp;
    } else {
        rows_start += 7;
    }
    /* Count "key": occurrences in the rows section */
    int n = 0;
    const char *p = rows_start;
    while ((p = strstr(p, "\"key\":")) != NULL) { n++; p += 6; }
    return n;
}

/* ---- A1: FP_PRIMARY_LEAF + ORDER_NONE → real total ------- */
/* Insert 800 records: 5 with tag="rare", 795 with tag="common".
   selectivity_budget(800) = 800/8 = 100 > 5 → leaf selective → PRIMARY_LEAF.
   find {tag:"rare"} limit=3 total=true → rows.length=3, total=5. */
static int test_real_total_a1_primary_leaf(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "rt_a1",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\"");
    if (!tc) return 1;

    /* Bulk-insert in two passes to keep each request buffer small. */
    char body[32768]; size_t p = 0; char *resp = NULL;

    /* Pass 1: 5 "rare" records */
    p = 0;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 5; i++)
        SB_APPEND(body, p, sizeof(body),
            "%s\"rare%d\":{\"tag\":\"rare\",\"score\":%d}", i?",":"", i, i);
    SB_APPEND(body, p, sizeof(body), "}");
    {
        char req[33000];
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rt_a1\","
            "\"records\":%s}", body);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* Pass 2: 795 "common" records (split into 8 chunks of ~100) */
    for (int chunk = 0; chunk < 8; chunk++) {
        int start = chunk * 99, end = start + 99;
        if (chunk == 7) end = start + (795 - 7*99); /* last chunk */
        p = 0;
        SB_APPEND(body, p, sizeof(body), "{");
        for (int i = start; i < end; i++)
            SB_APPEND(body, p, sizeof(body),
                "%s\"cmn%d\":{\"tag\":\"common\",\"score\":%d}", i==start?"":",", i, i*10);
        SB_APPEND(body, p, sizeof(body), "}");
        char req[33000];
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rt_a1\","
            "\"records\":%s}", body);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* find with limit=3, total=true */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rt_a1\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"}],"
        "\"limit\":3,\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "A1 response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "A1 → rows key present");
    /* total must be the integer 5 (all matching records) */
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, 5, "A1 → total = 5 (all rare records)");
    /* rows array must have exactly 3 items (limit respected) */
    int nrows = count_rows_in_resp(resp);
    ASSERT_EQ_INT(nrows, 3, "A1 → rows length = 3 (limit)");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-real-total-a1-primary-leaf", test_real_total_a1_primary_leaf)

/* ---- A1b: ORDER_SORT still gets real total ---------------- */
/* Insert 800 records: 5 "rare" + 795 "common".  Same scale as A1 so the
   planner picks PRIMARY_LEAF.  ORDER_SORT (order_by score, no composite) must
   still compute total = 5 via a second index walk. */
static int test_real_total_a1_order_sort(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "rt_a1s",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\",\"score\"");
    if (!tc) return 1;

    char body[32768]; size_t p = 0; char *resp = NULL;

    /* Pass 1: 5 "rare" records */
    p = 0;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 5; i++)
        SB_APPEND(body, p, sizeof(body),
            "%s\"rare%d\":{\"tag\":\"rare\",\"score\":%d}", i?",":"", i, i*10);
    SB_APPEND(body, p, sizeof(body), "}");
    {
        char req[33000];
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rt_a1s\","
            "\"records\":%s}", body);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* Pass 2: 795 "common" records */
    for (int chunk = 0; chunk < 8; chunk++) {
        int start = chunk * 99, end = start + 99;
        if (chunk == 7) end = start + (795 - 7*99);
        p = 0;
        SB_APPEND(body, p, sizeof(body), "{");
        for (int i = start; i < end; i++)
            SB_APPEND(body, p, sizeof(body),
                "%s\"cmn%d\":{\"tag\":\"common\",\"score\":%d}", i==start?"":",", i, 100+i);
        SB_APPEND(body, p, sizeof(body), "}");
        char req[33000];
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rt_a1s\","
            "\"records\":%s}", body);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* order_by=score: no composite by+score, so ORDER_SORT path (buffer + qsort) */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rt_a1s\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare\"}],"
        "\"order_by\":\"score\",\"limit\":2,\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "A1 ORDER_SORT response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "A1s → rows key present");
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, 5, "A1 ORDER_SORT → total = 5 (sort doesn't affect count)");
    int nrows = count_rows_in_resp(resp);
    ASSERT_EQ_INT(nrows, 2, "A1 ORDER_SORT → rows length = 2 (limit)");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-real-total-a1-order-sort", test_real_total_a1_order_sort)

/* ---- A2: FP_BITMAP_SMALLER → total = bm_popcount --------- */
/* Insert 100 records: 70 active=true, 30 active=false.
   find {active:true} limit=3 total=true → rows=3, total=70. */
static int test_real_total_a2_bitmap(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "rt_a2",
        "\"active:bool\",\"score:int\"", "\"active:bitmap\"");
    if (!tc) return 1;

    char body[8192]; size_t p = 0; char *resp = NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 70; i++)
        SB_APPEND(body, p, sizeof(body),
            "%s\"t%d\":{\"active\":true,\"score\":%d}", i?",":"", i, i);
    for (int i = 0; i < 30; i++)
        SB_APPEND(body, p, sizeof(body),
            ",\"f%d\":{\"active\":false,\"score\":%d}", i, i);
    SB_APPEND(body, p, sizeof(body), "}");
    char req[8300];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rt_a2\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rt_a2\","
        "\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"limit\":3,\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "A2 response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "A2 → rows key present");
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, 70, "A2 → total = 70 (bitmap popcount)");
    int nrows = count_rows_in_resp(resp);
    ASSERT_EQ_INT(nrows, 3, "A2 → rows length = 3 (limit)");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-real-total-a2-bitmap", test_real_total_a2_bitmap)

/* ---- B1: FP_INTERSECT + small-primary → real total (post Phase 1d follow-up) ---
   After the Phase 1d follow-up that lets the daemon compute total for ALL
   want_total requests via fp_compute_total: when small_primary fires (the
   first leaf's KeySet is < INTERSECT_MIN_PRIMARY=10000 entries — the engine
   handed back the smallest leaf as a candidate set, not the actual
   intersection), the helper walks that candidate KeySet, fetches each record,
   verifies against the full criteria tree via parallel_indexed_count, and
   returns the true match count. Same work the explorer would do firing a
   separate count query; consolidated into this round-trip.

   Setup: 100 records, 60 with tag="x", 40 with tag="y".
   AND criteria: {tag:"x"} AND {score>=50}. 20 records have BOTH (tag=x and
   score in [50,69]). small_primary fires (60 tag="x" entries < 10000), the
   helper recheck-counts the 60 candidates against score>=50 → 20 matches. */
static int test_real_total_b1_intersect(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "rt_b1",
        "\"tag:varchar:4\",\"score:int\"", "\"tag\",\"score\"");
    if (!tc) return 1;

    char body[16384]; size_t p = 0; char *resp = NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    int first = 1;
    /* 20 records: tag="x", score=50..69 (match both leaves) */
    for (int i = 0; i < 20; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"xh%d\":{\"tag\":\"x\",\"score\":%d}", first?"":",", i, 50+i);
        first = 0;
    }
    /* 40 records: tag="x", score=0..39 (match tag but not score) */
    for (int i = 0; i < 40; i++)
        SB_APPEND(body, p, sizeof(body),
            ",\"xl%d\":{\"tag\":\"x\",\"score\":%d}", i, i);
    /* 40 records: tag="y" (match neither leaf for the intersect) */
    for (int i = 0; i < 40; i++)
        SB_APPEND(body, p, sizeof(body),
            ",\"y%d\":{\"tag\":\"y\",\"score\":%d}", i, i);
    SB_APPEND(body, p, sizeof(body), "}");
    char req[16500];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rt_b1\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rt_b1\","
        "\"criteria\":["
          "{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"x\"},"
          "{\"field\":\"score\",\"op\":\"gte\",\"value\":\"50\"}"
        "],\"limit\":5,\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "B1 response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "B1 → rows key present");
    /* small_primary fires (60 < INTERSECT_MIN_PRIMARY=10000): engine can't
       know exact intersection size → total must be null */
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, 20, "B1 → total = 20 (small-primary recheck: 20 of 60 tag=x match score>=50)");
    /* Rows must still be correct (post-filter verifies both criteria) */
    int nrows = count_rows_in_resp(resp);
    ASSERT_EQ_INT(nrows, 5, "B1 → rows length = 5 (limit)");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-real-total-b1-intersect", test_real_total_b1_intersect)

/* ---- C1: FP_UNION → total = |KeySet| after OR-union ------- */
/* Insert 50 records: 10 with tag="alpha", 10 with tag="beta",
   30 with tag="other". OR {tag:"alpha"} OR {tag:"beta"} → 20 matches.
   find limit=3 total=true → rows=3, total=20. */
static int test_real_total_c1_union(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "rt_c1",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\"");
    if (!tc) return 1;

    char body[8192]; size_t p = 0; char *resp = NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 10; i++)
        SB_APPEND(body, p, sizeof(body),
            "%s\"a%d\":{\"tag\":\"alpha\",\"score\":%d}", i?",":"", i, i);
    for (int i = 0; i < 10; i++)
        SB_APPEND(body, p, sizeof(body),
            ",\"b%d\":{\"tag\":\"beta\",\"score\":%d}", i, i);
    for (int i = 0; i < 30; i++)
        SB_APPEND(body, p, sizeof(body),
            ",\"o%d\":{\"tag\":\"other\",\"score\":%d}", i, i);
    SB_APPEND(body, p, sizeof(body), "}");
    char req[8300];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rt_c1\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rt_c1\","
        "\"criteria\":[{\"or\":["
          "{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"alpha\"},"
          "{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"beta\"}"
        "]}],\"limit\":3,\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "C1 response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "C1 → rows key present");
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, 20, "C1 → total = 20 (union keyset size)");
    int nrows = count_rows_in_resp(resp);
    ASSERT_EQ_INT(nrows, 3, "C1 → rows length = 3 (limit)");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-real-total-c1-union", test_real_total_c1_union)

/* ---- A5: FP_FULL_SCAN (non-indexed field) → total = null -- */
/* Insert 10 records, no index on "note".
   find {note:"hi"} total=true → total must be null. */
static int test_real_total_a5_full_scan(void) {
    TestEnv env = {0};
    /* score:int indexed, note:varchar:8 NOT indexed */
    TestClient *tc = setup_obj(&env, "rt_a5",
        "\"note:varchar:8\",\"score:int\"", "\"score\"");
    if (!tc) return 1;

    char body[2048]; size_t p = 0; char *resp = NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 10; i++)
        SB_APPEND(body, p, sizeof(body),
            "%s\"k%d\":{\"note\":\"hi\",\"score\":%d}", i?",":"", i, i);
    SB_APPEND(body, p, sizeof(body), "}");
    char req[2200];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rt_a5\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rt_a5\","
        "\"criteria\":[{\"field\":\"note\",\"op\":\"eq\",\"value\":\"hi\"}],"
        "\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "A5 response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "A5 → rows key present");
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, -999, "A5 → total = null (full scan, no cheap count)");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-real-total-a5-full-scan", test_real_total_a5_full_scan)

/* ---- D1: FP_ORDER_COMPOSITE → total = null (spec) --------- */
/* Setup mirrors test_d1_composite_executor: by:varchar + composite by+time.
   find {by:"alice"} order_by time limit=3 total=true → total must be null. */
static int test_real_total_d1_composite(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rt_d1\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"by:varchar:16\",\"time:long\"],"
        "\"indexes\":[\"by\",\"by+time\"]}",
        &resp); free(resp); resp = NULL;

    /* Insert 20 alice rows and 10 bob rows */
    char body[8192]; size_t p = 0;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 20; i++)
        SB_APPEND(body, p, sizeof(body),
            "%s\"a%d\":{\"by\":\"alice\",\"time\":%d}", i?",":"", i, i*100);
    for (int i = 0; i < 10; i++)
        SB_APPEND(body, p, sizeof(body),
            ",\"b%d\":{\"by\":\"bob\",\"time\":%d}", i, i*100);
    SB_APPEND(body, p, sizeof(body), "}");
    char req[8300];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rt_d1\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rt_d1\","
        "\"criteria\":[{\"field\":\"by\",\"op\":\"eq\",\"value\":\"alice\"}],"
        "\"order_by\":\"time\",\"limit\":3,\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "D1 response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "D1 → rows key present");
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, -999, "D1 → total = null (composite walk, per spec)");
    int nrows = count_rows_in_resp(resp);
    ASSERT_EQ_INT(nrows, 3, "D1 → rows length = 3 (limit respected)");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-real-total-d1-composite", test_real_total_d1_composite)

/* ---- D3: FP_ORDER_INDEX_WALK → real total (post Phase 1d follow-up) -------- */
/* Setup mirrors test_d3_order_walk_executor: score and time both indexed,
   no composite. Broad filter (score>50) + indexed order_by → D3.
   After the Phase 1d follow-up that lets the daemon compute total for
   ALL want_total requests (via fp_compute_total), D3 returns the real
   match count instead of null. find {score>50} order_by time limit=3
   total=true → total = 50 (the 50 score=100 rows that match score>50). */
static int test_real_total_d3_order_walk(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rt_d3\","
        "\"splits\":8,\"max_key\":12,"
        "\"fields\":[\"score:int\",\"time:long\"],"
        "\"indexes\":[\"score\",\"time\"]}",
        &resp); free(resp); resp = NULL;

    /* 50 score=100 rows (matches score>50) + 30 score=10 rows (doesn't match) */
    char body[8192]; size_t p = 0;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 50; i++)
        SB_APPEND(body, p, sizeof(body),
            "%s\"s%d\":{\"score\":100,\"time\":%d}", i?",":"", i, i);
    for (int i = 0; i < 30; i++)
        SB_APPEND(body, p, sizeof(body),
            ",\"n%d\":{\"score\":10,\"time\":%d}", i, 1000+i);
    SB_APPEND(body, p, sizeof(body), "}");
    char req[8300];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rt_d3\","
        "\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rt_d3\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"gt\",\"value\":\"50\"}],"
        "\"order_by\":\"time\",\"limit\":3,\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "D3 response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "D3 → rows key present");
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, 50, "D3 → total = 50 (50 score=100 rows match score>50)");
    int nrows = count_rows_in_resp(resp);
    ASSERT_EQ_INT(nrows, 3, "D3 → rows length = 3 (limit respected)");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-real-total-d3-order-walk", test_real_total_d3_order_walk)

/* UUID auto-keys must not affect exact ordered pagination totals. */
static int test_find_total_ordered_customers(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"customers\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:32\"],"
        "\"indexes\":[\"name\"],\"auto_key\":\"uuid\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"customers\",\"records\":["
        "{\"key\":\"11111111-2222-4333-8444-555566667777\",\"value\":{\"name\":\"Scap Alpha\"}},"
        "{\"key\":\"22222222-3333-4444-8555-666677778888\",\"value\":{\"name\":\"scap Beta\"}},"
        "{\"key\":\"33333333-4444-4555-8666-777788889999\",\"value\":{\"name\":\"Other\"}}]}",
        &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"customers\","
        "\"criteria\":[],\"order_by\":\"name\",\"order\":\"asc\",\"limit\":1,\"total\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"total\":3",
                    "ordered UUID find without criteria emits numeric total");
    ASSERT_EQ_INT(extract_total(resp), 3,
                  "ordered UUID find without criteria has exact total");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"customers\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"icontains\",\"value\":\"scap\"}],"
        "\"order_by\":\"name\",\"order\":\"asc\",\"limit\":1,\"total\":true}", &resp);
    ASSERT_EQ_INT(extract_total(resp), 2,
                  "ordered icontains UUID find has exact total");
    free(resp);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-find-total-ordered-customers", test_find_total_ordered_customers)

/* ================================================================
 * Phase 1d.3 tests — cmd_aggregate computes total group count
 * ================================================================ */

/* Count objects in a JSON array or object by counting "{" at depth 1. */
static int count_agg_rows(const char *resp) {
    if (!resp) return 0;
    /* Locate the rows value: either {"rows":[...]} or bare [...] / {single} */
    const char *start = SAFE_STRSTR(resp, "\"rows\":");
    if (start) {
        start += 7; /* skip "rows": */
        while (*start == ' ' || *start == '\t') start++;
    } else {
        start = resp;
    }
    if (*start == '{') return 1;   /* single-row no-group object */
    if (*start != '[') return 0;
    start++;
    int depth = 0, n = 0;
    for (const char *p = start; *p; p++) {
        if (*p == '{') { if (depth == 0) n++; depth++; }
        else if (*p == '}') { depth--; }
        else if (*p == ']' && depth == 0) break;
    }
    return n;
}

/* ---- aggregate group_by + total: 50 records, 5 distinct tags, limit=3 --- */
/* Inserts 50 records with tag cycling "g0".."g4" (10 each).
   aggregate count() group_by=tag limit=3 total=true
   → rows.length=3, total=5 (5 distinct groups, page limits to 3). */
static int test_agg_total_groupby(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "at_gb",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\",\"score\"");
    if (!tc) return 1;

    char body[4096]; size_t p = 0; char *resp = NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 50; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"r%d\":{\"tag\":\"g%d\",\"score\":%d}",
            i ? "," : "", i, i % 5, i * 2);
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[4200];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"at_gb\",\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"at_gb\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"cnt\"}],"
        "\"group_by\":[\"tag\"],\"limit\":3,\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "agg groupby → response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "agg groupby → rows key present");
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, 5, "agg groupby → total = 5 (5 distinct groups)");
    int nrows = count_agg_rows(resp);
    ASSERT_EQ_INT(nrows, 3, "agg groupby → rows length = 3 (limit)");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-agg-total-groupby", test_agg_total_groupby)

/* ---- aggregate no-group + total: 100 records, count() → total=1 --------- */
/* aggregate count() total=true on 100 records → rows.length=1, total=1
   (single scalar row — always 1 by definition). */
static int test_agg_total_nogroup(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "at_ng",
        "\"score:int\"", "\"score\"");
    if (!tc) return 1;

    char body[8192]; size_t p = 0; char *resp = NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 100; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"r%d\":{\"score\":%d}", i ? "," : "", i, i);
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[8300];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"at_ng\",\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"at_ng\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"cnt\"}],"
        "\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "agg no-group → response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "agg no-group → rows key present");
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, 1, "agg no-group → total = 1 (single scalar row)");
    int nrows = count_agg_rows(resp);
    ASSERT_EQ_INT(nrows, 1, "agg no-group → rows length = 1");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-agg-total-nogroup", test_agg_total_nogroup)

/* ---- aggregate streaming top-N + total: total=null (documented fallback) - */
/* eligible_for_topn_stream requires: single group_by, order_by an agg alias,
   limit > 0.  Insert 200 records with 20 distinct tags (10 each), then
   aggregate count() group_by=tag order_by=cnt desc limit=3 total=true.
   The streaming path doesn't materialise a full group table → total=null. */
static int test_agg_total_streaming_topn(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "at_sn",
        "\"tag:varchar:8\",\"score:int\"", "\"tag\",\"score\"");
    if (!tc) return 1;

    char body[16384]; size_t p = 0; char *resp = NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    for (int i = 0; i < 200; i++) {
        SB_APPEND(body, p, sizeof(body),
            "%s\"r%d\":{\"tag\":\"t%02d\",\"score\":%d}",
            i ? "," : "", i, i % 20, i);
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[16600];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"at_sn\",\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    /* This request shape is exactly what eligible_for_topn_stream requires. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"at_sn\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"cnt\"}],"
        "\"group_by\":[\"tag\"],\"order_by\":\"cnt\",\"order_desc\":true,"
        "\"limit\":3,\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "agg streaming topN → response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "agg streaming topN → rows key present");
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, -999, "agg streaming topN → total = null (documented fallback)");
    int nrows = count_agg_rows(resp);
    ASSERT_EQ_INT(nrows, 3, "agg streaming topN → rows length = 3 (limit)");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-agg-total-streaming-topn", test_agg_total_streaming_topn)

/* ---- aggregate group_by + criteria + total: filtered group count ---------- */
/* Insert 50 records: 5 authors × 10 records each, half with active=1.
   aggregate count() group_by=author criteria=[active=1] limit=2 total=true
   → total = 5 (all 5 authors appear in active records), rows = 2 (limit). */
static int test_agg_total_groupby_filtered(void) {
    TestEnv env = {0};
    TestClient *tc = setup_obj(&env, "at_gf",
        "\"author:varchar:8\",\"active:bool\"",
        "\"author\",\"active\"");
    if (!tc) return 1;

    char body[4096]; size_t p = 0; char *resp = NULL;
    SB_APPEND(body, p, sizeof(body), "{");
    /* 5 authors, each with 5 active + 5 inactive records (10 total) */
    int k = 0;
    for (int a = 0; a < 5; a++) {
        for (int j = 0; j < 10; j++) {
            SB_APPEND(body, p, sizeof(body),
                "%s\"r%d\":{\"author\":\"au%d\",\"active\":%s}",
                k ? "," : "", k, a, j < 5 ? "true" : "false");
            k++;
        }
    }
    SB_APPEND(body, p, sizeof(body), "}");
    char req[4200];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"at_gf\",\"records\":%s}", body);
    tc_request(tc, req, &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"at_gf\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"cnt\"}],"
        "\"group_by\":[\"author\"],"
        "\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"limit\":2,\"total\":true}",
        &resp);
    ASSERT_NOT_NULL(resp, "agg filtered groupby → response not null");
    ASSERT_CONTAINS(resp, "\"rows\"", "agg filtered groupby → rows key present");
    int total = extract_total(resp);
    ASSERT_EQ_INT(total, 5, "agg filtered groupby → total = 5 (5 authors with active records)");
    int nrows = count_agg_rows(resp);
    ASSERT_EQ_INT(nrows, 2, "agg filtered groupby → rows length = 2 (limit)");
    free(resp);

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-agg-total-groupby-filtered", test_agg_total_groupby_filtered)
