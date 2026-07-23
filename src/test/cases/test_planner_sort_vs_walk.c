#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

static int substr_count(const char *hay, const char *needle) {
    if (!hay || !needle || !*needle) return 0;
    int n = 0; size_t nl = strlen(needle);
    for (const char *p = hay; (p = strstr(p, needle)); p += nl) n++;
    return n;
}

extern const char *plan_filter_kind_for_test(const char *db_root, const char *object,
        const char *criteria_json, const char *order_by, int fetching,
        char *out_field, size_t fsz, char *out_order, size_t osz,
        int *out_total_cheap);

static int test_planner_sort_vs_walk(void) {
    TestEnv env = {0};
    TestClient *tc = NULL; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg); ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"sw\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"sw\",\"object\":\"swobj\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"cat:varchar:8\",\"t:long\"],"
        "\"indexes\":[\"cat\",\"t\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create"); free(resp); resp=NULL;

    for (int i = 0; i < 2000; i++) {
        char req[256];
        /* 5 rare rows (k≈5), 1995 common rows */
        const char *cat = (i < 5) ? "rare" : "common";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"sw\",\"object\":\"swobj\",\"key\":\"k%04d\","
            "\"value\":{\"cat\":\"%s\",\"t\":%d}}", i, cat, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    char order[64];

    /* Small set (k≈5) with limit 25 → prefer_fetch_sort wins → "sort" */
    plan_filter_kind_for_test(env.db_root, "sw/swobj",
        "[{\"field\":\"cat\",\"op\":\"eq\",\"value\":\"rare\"}]", "t", 1,
        NULL, 0, order, sizeof(order), NULL);
    ASSERT_EQ_STR(order, "sort", "rare seed + limit 25 → sort (not walk)");

    /* Broad set (k≈1995) with limit 25 → prefer_fetch_sort loses → "walk" */
    plan_filter_kind_for_test(env.db_root, "sw/swobj",
        "[{\"field\":\"cat\",\"op\":\"eq\",\"value\":\"common\"}]", "t", 1,
        NULL, 0, order, sizeof(order), NULL);
    ASSERT_EQ_STR(order, "walk", "common seed + limit 25 → walk (not sort)");

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-planner-sort-vs-walk", test_planner_sort_vs_walk)

/* C2 — cursor fetch+sort shortcut with pagination. Creates a small
   sparse-in-order set and fetches via cursor, verifying correct rows
   and cursor envelope across two pages. */
static int test_cursor_fetch_sort(void) {
    TestEnv env = {0};
    TestClient *tc = NULL; char *resp = NULL;
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    tc = tc_connect(&cfg); ASSERT_NOT_NULL(tc, "connect");

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"cf\"}", &resp); free(resp); resp=NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"cf\",\"object\":\"cfobj\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"kind:varchar:8\",\"score:long\"],"
        "\"indexes\":[\"kind\",\"score\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create"); free(resp); resp=NULL;

    /* 5 "rare" rows (k≈5), 1995 "common" rows — sparse in score. */
    for (int i = 0; i < 2000; i++) {
        char req[256];
        const char *kind = (i < 5) ? "rare" : "common";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"cf\",\"object\":\"cfobj\",\"key\":\"k%04d\","
            "\"value\":{\"kind\":\"%s\",\"score\":%d}}", i, kind, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }

    /* Page 1: limit=3 on rare set. Expect 3 rows, non-null cursor. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"cf\",\"object\":\"cfobj\","
        "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"rare\"}],"
        "\"order_by\":\"score\",\"order\":\"asc\",\"limit\":3,\"cursor\":null}",
        &resp);
    ASSERT_CONTAINS(resp, "\"kind\":\"rare\"", "page1 returns rare");
    ASSERT_CONTAINS(resp, "\"cursor\":{", "page1 has next cursor");
    ASSERT_EQ_INT(substr_count(resp, "\"kind\""), 3, "page1 has 3 rows");

    /* Extract cursor JSON for page 2. */
    const char *curp = SAFE_STRSTR(resp, "\"cursor\":");
    char *page2_cursor = NULL;
    if (curp) {
        const char *brace = strchr(curp, '{');
        if (brace) {
            int depth = 0; const char *end = brace;
            for (; *end; end++) { if (*end == '{') depth++; if (*end == '}') { depth--; if (depth == 0) break; } }
            if (depth == 0 && end > brace) {
                size_t clen = (size_t)(end - brace + 1);
                page2_cursor = malloc(clen + 1);
                memcpy(page2_cursor, brace, clen);
                page2_cursor[clen] = '\0';
            }
        }
    }
    ASSERT_TRUE(page2_cursor != NULL, "page1 cursor extracted");
    free(resp); resp = NULL;

    /* Page 2: resume from cursor. Expect 2 rows (remaining), null cursor. */
    char req2[1024];
    snprintf(req2, sizeof(req2),
        "{\"mode\":\"find\",\"dir\":\"cf\",\"object\":\"cfobj\","
        "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"rare\"}],"
        "\"order_by\":\"score\",\"order\":\"asc\",\"limit\":3,\"cursor\":%s}",
        page2_cursor);
    free(page2_cursor);
    tc_request(tc, req2, &resp);
    ASSERT_CONTAINS(resp, "\"kind\":\"rare\"", "page2 returns rare");
    ASSERT_CONTAINS(resp, "\"cursor\":null", "page2 cursor is null");
    ASSERT_EQ_INT(substr_count(resp, "\"kind\""), 2, "page2 has 2 remaining rows");
    free(resp); resp = NULL;

    tc_close(tc); test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-cursor-fetch-sort", test_cursor_fetch_sort)
