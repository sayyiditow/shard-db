/* src/test/cases/test_null_index.c
 *
 * BTRI index behavior with NULLs (null plan Task 6 item 8):
 *   - indexed long NULL: eq 0 misses it, not_exists finds it (scan path);
 *   - update null→5 makes eq 5 hit; 5→null makes eq 5 miss;
 *   - gt/gte (open-ended upper bounds) exclude the NULL row;
 *   - ordered walk returns values-then-NULL (ASC tail);
 *   - add-index over records containing NULLs → NULL entries exist
 *     (rebuild), ordered queries agree.
 */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static TestClient *ni_setup(TestEnv *env) {
    if (test_env_start(env) != 0) return NULL;
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(env); return NULL; }
    char *r = NULL;
    const char *seq[] = {
        "{\"mode\":\"add-dir\",\"dir\":\"ni\"}",
        "{\"mode\":\"create-object\",\"dir\":\"ni\",\"object\":\"t\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:long\",\"tag:varchar:8\"],\"indexes\":[\"v\"]}",
    };
    for (size_t i = 0; i < sizeof(seq) / sizeof(seq[0]); i++) {
        if (tc_request(tc, seq[i], &r) != 0 || strstr(r, "\"error\"")) {
            free(r); tc_close(tc); test_env_stop(env); return NULL;
        }
        free(r); r = NULL;
    }
    /* v: 10, NULL, 20, NULL */
    const char *rows[4] = {
        "{\"mode\":\"insert\",\"dir\":\"ni\",\"object\":\"t\",\"key\":\"a\",\"value\":{\"v\":\"10\",\"tag\":\"x\"}}",
        "{\"mode\":\"insert\",\"dir\":\"ni\",\"object\":\"t\",\"key\":\"b\",\"value\":{\"v\":null,\"tag\":\"y\"}}",
        "{\"mode\":\"insert\",\"dir\":\"ni\",\"object\":\"t\",\"key\":\"c\",\"value\":{\"v\":\"20\",\"tag\":\"z\"}}",
        "{\"mode\":\"insert\",\"dir\":\"ni\",\"object\":\"t\",\"key\":\"d\",\"value\":{\"tag\":\"w\"}}",
    };
    for (size_t i = 0; i < 4; i++) {
        if (tc_request(tc, rows[i], &r) != 0 || strstr(r, "\"error\"")) {
            free(r); tc_close(tc); test_env_stop(env); return NULL;
        }
        free(r); r = NULL;
    }
    return tc;
}

static int ni_count(TestClient *tc, const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"ni\",\"object\":\"t\",\"criteria\":%s}",
        crit);
    char *r = NULL;
    if (tc_request(tc, req, &r) != 0 || !r) return -1;
    int n = atoi(r);
    free(r);
    return n;
}

static int test_null_index_lifecycle(void) {
    TestEnv env;
    TestClient *tc = ni_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        ASSERT_EQ_INT(ni_count(tc, "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"0\"}]"),
                      0, "eq 0 misses NULLs (no fake zeros)");
        ASSERT_EQ_INT(ni_count(tc, "[{\"field\":\"v\",\"op\":\"not_exists\"}]"),
                      2, "not_exists finds both NULL rows (scan path)");
        ASSERT_EQ_INT(ni_count(tc, "[{\"field\":\"v\",\"op\":\"gt\",\"value\":\"0\"}]"),
                      2, "gt 0 excludes NULL rows (tier clamp)");
        ASSERT_EQ_INT(ni_count(tc, "[{\"field\":\"v\",\"op\":\"gte\",\"value\":\"10\"}]"),
                      2, "gte 10 = 10 and 20, not NULLs");
        char *r = NULL;
        /* NULL→5: eq 5 hits after update. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"update\",\"dir\":\"ni\",\"object\":\"t\",\"key\":\"b\","
            "\"value\":{\"v\":\"5\"}}", &r), 0, "update null→5");
        free(r); r = NULL;
        ASSERT_EQ_INT(ni_count(tc, "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"5\"}]"),
                      1, "eq 5 hits after null→5");
        /* 5→NULL: eq 5 misses again, not_exists back to 2. */
        free(r);
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"update\",\"dir\":\"ni\",\"object\":\"t\",\"key\":\"b\","
            "\"value\":{\"v\":null}}", &r), 0, "update 5→null");
        free(r); r = NULL;
        ASSERT_EQ_INT(ni_count(tc, "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"5\"}]"),
                      0, "eq 5 misses after 5→null");
        ASSERT_EQ_INT(ni_count(tc, "[{\"field\":\"v\",\"op\":\"not_exists\"}]"),
                      2, "not_exists back to 2");
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-index-lifecycle", test_null_index_lifecycle)

/* Ordered find places NULL rows at the ASC tail; add-index rebuild over
 * NULLs keeps counts consistent. */
static int test_null_index_order_rebuild(void) {
    TestEnv env;
    TestClient *tc = ni_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"ni\",\"object\":\"t\","
            "\"criteria\":[],\"order_by\":\"v\",\"order\":\"asc\",\"limit\":10}",
            &r), 0, "ordered find asc");
        ASSERT_NOT_NULL(r, "resp");
        /* Values first (a, c), then the NULL rows (b, d) at the tail. */
        const char *t4 = strstr(r, "\"key\":\"d\"");
        const char *t2 = strstr(r, "\"key\":\"b\"");
        const char *ta = strstr(r, "\"key\":\"a\"");
        ASSERT_NOT_NULL(ta && t2 && t4, "all 4 rows returned");
        if (ta && t2 && t4) {
            ASSERT_TRUE(ta < t2 && t2 < t4,
                        "NULL rows sort at the ASC tail (after values)");
        }
        free(r); r = NULL;

        /* Rebuild the index over NULLs (remove + add) — counts agree. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"remove-index\",\"dir\":\"ni\",\"object\":\"t\","
            "\"field\":\"v\"}", &r), 0, "remove-index");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"add-index\",\"dir\":\"ni\",\"object\":\"t\","
            "\"field\":\"v\"}", &r), 0, "add-index over NULLs");
        free(r); r = NULL;
        ASSERT_EQ_INT(ni_count(tc, "[{\"field\":\"v\",\"op\":\"not_exists\"}]"),
                      2, "not_exists = 2 after rebuild");
        ASSERT_EQ_INT(ni_count(tc, "[{\"field\":\"v\",\"op\":\"gt\",\"value\":\"0\"}]"),
                      2, "gt 0 = 2 after rebuild");
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-index-order-rebuild", test_null_index_order_rebuild)
