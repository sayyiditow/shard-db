/* src/test/cases/test_cursor_with_total.c
 * Verify that cursor + total:true works in a single find request,
 * returning {"rows":[...],"cursor":...,"total":N}.
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

/* Extract cursor JSON object as a string (the value of "cursor":{...} or "cursor":null).
   Returns 1 if a non-null cursor was found and copied into out. */
static int extract_cursor_json(const char *resp, char *out, size_t out_sz) {
    if (!resp) return 0;
    const char *p = SAFE_STRSTR(resp, "\"cursor\":{");
    if (!p) return 0;
    p += strlen("\"cursor\":");
    /* find matching closing brace */
    const char *start = p;
    int depth = 0; const char *c = p;
    while (*c) {
        if (*c == '{') depth++;
        else if (*c == '}') { depth--; if (depth == 0) { c++; break; } }
        c++;
    }
    size_t n = (size_t)(c - start);
    if (n + 1 > out_sz) return 0;
    memcpy(out, start, n); out[n] = '\0';
    return 1;
}

/* Extract integer value of "total":N from response. Returns -1 if absent. */
static int extract_total(const char *resp) {
    if (!resp) return -1;
    const char *p = SAFE_STRSTR(resp, "\"total\":");
    if (!p) return -1;
    p += strlen("\"total\":");
    if (*p == 'n') return -2; /* null */
    return atoi(p);
}

static int test_cursor_with_total_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"cwt\"}", &resp);
    free(resp); resp = NULL;

    /* Create object with int field n, indexed. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"cwt\",\"object\":\"items\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"n:int\"],"
        "\"indexes\":[\"n\"]}",
        &resp); free(resp); resp = NULL;

    /* Insert 20 records, n = 1..20. */
    for (int i = 1; i <= 20; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"cwt\",\"object\":\"items\","
            "\"key\":\"k%02d\",\"value\":{\"n\":%d}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* PAGE 1: cursor:null + total:true — should return rows, cursor, total:20 */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"cwt\",\"object\":\"items\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":5,"
        "\"cursor\":null,\"total\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"rows\":", "page1 uses rows wrapper");
    ASSERT_CONTAINS(resp, "\"cursor\":{", "page1 emits a non-null cursor");
    ASSERT_TRUE(extract_total(resp) == 20, "page1 total == 20");
    ASSERT_CONTAINS(resp, "\"key\":\"k01\"", "page1 contains k01");
    free(resp); resp = NULL;

    /* PAGE 1 again — verify cursor field ordering: cursor comes before total. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"cwt\",\"object\":\"items\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":5,"
        "\"cursor\":null,\"total\":true}",
        &resp);
    {
        const char *pc = SAFE_STRSTR(resp, "\"cursor\":");
        const char *pt = SAFE_STRSTR(resp, "\"total\":");
        ASSERT_TRUE(pc && pt && pc < pt, "cursor key appears before total key");
    }
    char cursor_json[256] = {0};
    int got_cursor = extract_cursor_json(resp, cursor_json, sizeof(cursor_json));
    ASSERT_TRUE(got_cursor, "extracted cursor from page1");
    free(resp); resp = NULL;

    /* PAGE 2: use cursor from page1 + total:true — total must still be 20. */
    if (got_cursor) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"cwt\",\"object\":\"items\","
            "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":5,"
            "\"cursor\":%s,\"total\":true}", cursor_json);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"rows\":", "page2 uses rows wrapper");
        ASSERT_TRUE(extract_total(resp) == 20, "page2 total == 20 (stable across pages)");
        ASSERT_CONTAINS(resp, "\"key\":\"k06\"", "page2 contains k06");
        free(resp); resp = NULL;
    }

    /* LAST PAGE: limit > total records — printed < limit so cursor must be null. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"cwt\",\"object\":\"items\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":25,"
        "\"cursor\":null,\"total\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"cursor\":null", "last page cursor is null");
    ASSERT_TRUE(extract_total(resp) == 20, "last page total == 20");
    free(resp); resp = NULL;

    /* CRITERIA + cursor + total: only records with n > 10 (10 records). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"cwt\",\"object\":\"items\","
        "\"criteria\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"10\"}],"
        "\"order_by\":\"n\",\"order\":\"asc\",\"limit\":5,"
        "\"cursor\":null,\"total\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"rows\":", "criteria page1 uses rows wrapper");
    {
        int t = extract_total(resp);
        ASSERT_TRUE(t == 10, "criteria total == 10 (n > 10)");
    }
    free(resp); resp = NULL;

    /* Verify the old mutual-exclusion error is GONE. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"cwt\",\"object\":\"items\","
        "\"criteria\":[],\"order_by\":\"n\",\"order\":\"asc\",\"limit\":3,"
        "\"cursor\":null,\"total\":true}",
        &resp);
    ASSERT_TRUE(SAFE_STRSTR(resp, "mutually exclusive") == NULL,
                "no mutually exclusive error when cursor+total combined");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-cursor-with-total", test_cursor_with_total_run)
