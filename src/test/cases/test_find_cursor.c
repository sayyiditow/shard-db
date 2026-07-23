/* src/test/cases/test_find_cursor.c
 * Port of tests/test-find-cursor.sh — keyset pagination via transparent
 * JSON cursor on indexed order_by.
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

/* Find the cursor.key value within a wrapped find response. Returns 1 if
   filled. Naive parser: locates `"cursor":{` then `"key":"...". */
static int extract_cursor_key(const char *resp, char *out, size_t out_sz) {
    if (!resp) return 0;
    const char *c = SAFE_STRSTR(resp, "\"cursor\":{");
    if (!c) return 0;
    const char *k = strstr(c, "\"key\":\"");
    if (!k) return 0;
    k += strlen("\"key\":\"");
    const char *e = strchr(k, '"');
    if (!e) return 0;
    size_t n = (size_t)(e - k);
    if (n + 1 > out_sz) n = out_sz - 1;
    memcpy(out, k, n); out[n] = '\0';
    return 1;
}

/* Count occurrences of `"key":"<prefix>` in a response (rough row count). */
static int count_keys_with_prefix(const char *resp, const char *prefix) {
    if (!resp) return 0;
    char needle[64];
    snprintf(needle, sizeof(needle), "\"key\":\"%s", prefix);
    int n = 0;
    const char *p = resp;
    while ((p = strstr(p, needle)) != NULL) { n++; p += strlen(needle); }
    return n;
}

static int test_find_cursor_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;

    /* 1. Basic ASC/DESC pagination — curs_int. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"n:int\",\"tag:varchar:16\"],\"indexes\":[\"n\"]}",
        &resp); free(resp); resp = NULL;
    for (int i = 1; i <= 10; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"curs_int\","
            "\"key\":\"k%d\",\"value\":{\"n\":%d,\"tag\":\"paid\"}}",
            i, i * 10);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* PAGE 1 SHAPES — cursor:null */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":3,"
        "\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"rows\":", "cursor:null → wrapped response");
    ASSERT_CONTAINS(resp, "\"key\":\"k1\"", "cursor:null → emits k1");
    ASSERT_CONTAINS(resp, "\"cursor\":{", "cursor:null → emits initial cursor");
    ASSERT_CONTAINS(resp, "\"n\":\"30\"", "cursor:null → cursor value at n=30");
    ASSERT_CONTAINS(resp, "\"key\":\"k3\"", "cursor:null → cursor.key is k3");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":3,"
        "\"cursor\":{}}", &resp);
    ASSERT_CONTAINS(resp, "\"rows\":", "cursor:{} also works as page 1");
    ASSERT_CONTAINS(resp, "\"key\":\"k1\"", "cursor:{} emits k1 at top");
    free(resp); resp = NULL;

    /* PAGE 2 — cursor from page 1. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":3,"
        "\"cursor\":{\"n\":\"30\",\"key\":\"k3\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k4\"", "page 2 starts at k4");
    ASSERT_CONTAINS(resp, "\"key\":\"k5\"", "page 2 includes k5");
    ASSERT_CONTAINS(resp, "\"key\":\"k6\"", "page 2 includes k6");
    /* k3 should not appear in rows[]. The cursor emits cursor.key=k6, so
       we're allowed to see k3 only inside "cursor":{...}. We need a more
       careful check — look in the rows array specifically. */
    {
        const char *rows = SAFE_STRSTR(resp, "\"rows\":");
        const char *cursor_obj = SAFE_STRSTR(resp, "\"cursor\":{");
        const char *rows_end = cursor_obj ? cursor_obj : resp + strlen(resp);
        char window[1024]; size_t n = (size_t)(rows_end - rows);
        if (n + 1 > sizeof(window)) n = sizeof(window) - 1;
        memcpy(window, rows, n); window[n] = '\0';
        ASSERT_TRUE(strstr(window, "\"key\":\"k3\"") == NULL,
                    "page 2 rows do NOT include k3 (excl)");
    }
    free(resp); resp = NULL;

    /* LAST PAGE — cursor:null. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":5,"
        "\"cursor\":{\"n\":\"60\",\"key\":\"k6\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k10\"", "last page includes k7..k10");
    ASSERT_CONTAINS(resp, "\"cursor\":null", "last page has cursor:null");
    free(resp); resp = NULL;

    /* DESC pagination. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"desc\",\"limit\":3,"
        "\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k10\"", "desc page 1 starts at k10");
    ASSERT_CONTAINS(resp, "\"key\":\"k8\"", "desc page 1 ends at k8 (cursor.key)");
    ASSERT_CONTAINS(resp, "\"n\":\"80\"", "desc page 1 cursor value n=80");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"desc\",\"limit\":3,"
        "\"cursor\":{\"n\":\"80\",\"key\":\"k8\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k7\"", "desc page 2 starts at k7");
    ASSERT_CONTAINS(resp, "\"key\":\"k6\"", "desc page 2 includes k6");
    /* Check k8 not in rows. */
    {
        const char *rows = SAFE_STRSTR(resp, "\"rows\":");
        const char *cursor_obj = SAFE_STRSTR(resp, "\"cursor\":");
        const char *rows_end = cursor_obj ? cursor_obj : resp + strlen(resp);
        char window[1024]; size_t n = (size_t)(rows_end - rows);
        if (n + 1 > sizeof(window)) n = sizeof(window) - 1;
        memcpy(window, rows, n); window[n] = '\0';
        ASSERT_TRUE(strstr(window, "\"key\":\"k8\"") == NULL,
                    "desc page 2 rows do NOT include k8");
    }
    free(resp); resp = NULL;

    /* No cursor → unwrapped array. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":3}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"rows\":") == NULL, "no cursor → no rows wrapper");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"cursor\":") == NULL, "no cursor → no cursor field");
    ASSERT_CONTAINS(resp, "\"key\":\"k1\"", "no cursor → still returns array");
    free(resp); resp = NULL;

    /* ERROR CASES. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"criteria\":[],\"order_by\":\"tag\",\"limit\":3,"
        "\"cursor\":{\"tag\":\"paid\",\"key\":\"k1\"}}", &resp);
    ASSERT_CONTAINS(resp, "field to be indexed", "reject non-indexed order_by");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"criteria\":[],\"order_by\":\"n\",\"limit\":3,\"cursor\":{\"n\":\"30\"}}",
        &resp);
    ASSERT_CONTAINS(resp, "missing 'key'", "reject cursor missing key");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"criteria\":[],\"limit\":3,\"cursor\":{\"n\":\"30\",\"key\":\"k3\"}}",
        &resp);
    ASSERT_CONTAINS(resp, "cursor requires order_by", "reject cursor without order_by");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"criteria\":[],\"order_by\":\"n\",\"limit\":3,"
        "\"cursor\":{\"key\":\"k3\"}}", &resp);
    ASSERT_CONTAINS(resp, "missing order_by field value",
                    "reject cursor missing value for order_by");
    free(resp); resp = NULL;

    /* 2. Tie-break — curs_tie. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"curs_tie\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"grp:int\"],\"indexes\":[\"grp\"]}", &resp); free(resp); resp = NULL;
    for (int i = 1; i <= 10; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"curs_tie\","
            "\"key\":\"t%d\",\"value\":{\"grp\":5}}", i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_tie\","
        "\"criteria\":[],\"order_by\":\"grp\",\"limit\":3,\"cursor\":null}",
        &resp);
    ASSERT_EQ_INT(count_keys_with_prefix(resp, "t"), 4 /* 3 rows + 1 cursor.key */,
                  "tie-break page 1: 3 rows + cursor.key");
    ASSERT_CONTAINS(resp, "\"cursor\":{", "tie-break page 1 has cursor");
    char cur_key[16] = {0};
    int got = extract_cursor_key(resp, cur_key, sizeof(cur_key));
    free(resp); resp = NULL;
    ASSERT_TRUE(got, "tie-break extracted cursor key");
    if (got) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_tie\","
            "\"criteria\":[],\"order_by\":\"grp\",\"limit\":3,"
            "\"cursor\":{\"grp\":\"5\",\"key\":\"%s\"}}", cur_key);
        tc_request(tc, req, &resp);
        /* Page 2: 3 rows + 1 cursor.key (or null cursor) — at least 3 rows. */
        ASSERT_TRUE(count_keys_with_prefix(resp, "t") >= 3,
                    "tie-break page 2 returns 3 rows");
        /* p1 cursor key must not appear in p2 rows. */
        const char *rows = SAFE_STRSTR(resp, "\"rows\":");
        const char *cursor_obj = SAFE_STRSTR(resp, "\"cursor\":");
        const char *rows_end = cursor_obj ? cursor_obj : resp + strlen(resp);
        char window[1024]; size_t n = (size_t)(rows_end - rows);
        if (n + 1 > sizeof(window)) n = sizeof(window) - 1;
        memcpy(window, rows, n); window[n] = '\0';
        char want[32]; snprintf(want, sizeof(want), "\"key\":\"%s\"", cur_key);
        ASSERT_TRUE(strstr(window, want) == NULL,
                    "tie-break page 2 rows do not include p1's last key");
        free(resp); resp = NULL;
    }

    /* 3. Cursor with criteria filter — curs_crit. flag=true on c2,c4,c6,c8,c10. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"curs_crit\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"n:int\",\"flag:bool\"],\"indexes\":[\"n\"]}",
        &resp); free(resp); resp = NULL;
    for (int i = 1; i <= 10; i++) {
        const char *fl = (i % 2 == 0) ? "true" : "false";
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"curs_crit\","
            "\"key\":\"c%d\",\"value\":{\"n\":%d,\"flag\":%s}}", i, i * 10, fl);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_crit\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"order_by\":\"n\",\"order\":\"asc\",\"limit\":2,\"cursor\":null}",
        &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"c2\"", "cursor+filter page 1 has c2");
    ASSERT_CONTAINS(resp, "\"key\":\"c4\"", "cursor+filter page 1 has c4");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"key\":\"c1\"") == NULL,
                "cursor+filter page 1 does NOT have c1");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"key\":\"c3\"") == NULL,
                "cursor+filter page 1 does NOT have c3");
    ASSERT_CONTAINS(resp, "\"cursor\":{", "cursor+filter page 1 emits cursor");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_crit\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"order_by\":\"n\",\"order\":\"asc\",\"limit\":2,"
        "\"cursor\":{\"n\":\"40\",\"key\":\"c4\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"c6\"", "cursor+filter page 2 has c6");
    ASSERT_CONTAINS(resp, "\"key\":\"c8\"", "cursor+filter page 2 has c8");
    /* c4 must not be in rows. */
    {
        const char *rows = SAFE_STRSTR(resp, "\"rows\":");
        const char *cursor_obj = SAFE_STRSTR(resp, "\"cursor\":");
        const char *rows_end = cursor_obj ? cursor_obj : resp + strlen(resp);
        char window[1024]; size_t n = (size_t)(rows_end - rows);
        if (n + 1 > sizeof(window)) n = sizeof(window) - 1;
        memcpy(window, rows, n); window[n] = '\0';
        ASSERT_TRUE(strstr(window, "\"key\":\"c4\"") == NULL,
                    "cursor+filter page 2 rows do NOT include c4");
    }
    free(resp); resp = NULL;

    /* 4. Cursor through deletes — k5 deleted, cursor at (n=50,key=k5). */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"curs_int\",\"key\":\"k5\"}",
                   &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"curs_int\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":5,"
        "\"cursor\":{\"n\":\"50\",\"key\":\"k5\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k6\"", "cursor past deleted k5 yields k6");
    /* k5 should not appear in rows[]. */
    {
        const char *rows = SAFE_STRSTR(resp, "\"rows\":");
        const char *cursor_obj = SAFE_STRSTR(resp, "\"cursor\":");
        const char *rows_end = cursor_obj ? cursor_obj : resp + strlen(resp);
        char window[1024]; size_t n = (size_t)(rows_end - rows);
        if (n + 1 > sizeof(window)) n = sizeof(window) - 1;
        memcpy(window, rows, n); window[n] = '\0';
        ASSERT_TRUE(strstr(window, "\"key\":\"k5\"") == NULL,
                    "cursor past deleted k5 does NOT emit k5");
    }
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-find-cursor", test_find_cursor_run)
