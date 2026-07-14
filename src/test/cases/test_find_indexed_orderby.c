/* src/test/cases/test_find_indexed_orderby.c
 * Indexed ordered-find fast path. When order_by is indexed and there
 * are no joins or excluded keys, find walks the btree in sort order
 * and skips the first `offset` matching entries instead of buffering
 * everything + qsorting + slicing. Verifies output matches the
 * pre-existing buffered path across:
 *   - empty criteria, ASC + DESC
 *   - non-empty indexed criteria
 *   - offset > 0 (skip count correctness)
 *   - rows / dict format
 *   - limit truncation
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Count occurrences of substring `s` in `resp`. */
static int count_substr(const char *resp, const char *s) {
    if (!resp || !s) return 0;
    int n = 0;
    const char *p = resp;
    size_t sl = strlen(s);
    while ((p = strstr(p, s)) != NULL) { n++; p += sl; }
    return n;
}

static int test_find_indexed_orderby_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ord_t\","
        "\"fields\":[\"age:int\",\"name:varchar:32\",\"active:bool\"],"
        "\"indexes\":[\"age\",\"active\"],\"splits\":16}",
        &resp); free(resp); resp = NULL;

    /* Seed 100 rows. age = i (1..100), unique. name = "u<i>". active toggles. */
    size_t cap = 100 * 96 + 256;
    char *payload = malloc(cap);
    if (!payload) { tc_close(tc); test_env_stop(&env); return 1; }
    size_t len = 0;
    SB_APPEND(payload, len, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ord_t\",\"records\":[");
    for (int i = 1; i <= 100; i++) {
        SB_APPEND(payload, len, cap,
            "%s{\"key\":\"k%d\",\"value\":{\"age\":%d,\"name\":\"u%d\",\"active\":%s}}",
            i == 1 ? "" : ",", i, i, i, (i % 2 == 0) ? "true" : "false");
    }
    SB_APPEND(payload, len, cap, "]}");
    tc_request(tc, payload, &resp); free(resp); resp = NULL;
    free(payload);

    /* ASC by age, no offset, limit 5 — first 5 = ages 1..5. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ord_t\","
        "\"criteria\":[],\"order_by\":\"age\",\"order\":\"asc\","
        "\"offset\":0,\"limit\":5,\"fields\":[\"age\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"age\":\"1\"", "ASC[0..5] contains age=1");
    ASSERT_CONTAINS(resp, "\"age\":\"5\"", "ASC[0..5] contains age=5");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\":\"6\"") == NULL, "ASC[0..5] excludes age=6");
    ASSERT_TRUE(count_substr(resp, "\"key\":\"k") == 5, "ASC[0..5] has 5 rows");
    free(resp); resp = NULL;

    /* ASC by age, offset 50, limit 5 — ages 51..55. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ord_t\","
        "\"criteria\":[],\"order_by\":\"age\",\"order\":\"asc\","
        "\"offset\":50,\"limit\":5,\"fields\":[\"age\"]}",
        &resp);
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\":\"50\"") == NULL, "ASC offset=50 excludes age=50");
    ASSERT_CONTAINS(resp, "\"age\":\"51\"", "ASC offset=50 includes age=51");
    ASSERT_CONTAINS(resp, "\"age\":\"55\"", "ASC offset=50 includes age=55");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\":\"56\"") == NULL, "ASC offset=50 excludes age=56");
    ASSERT_TRUE(count_substr(resp, "\"key\":\"k") == 5, "ASC offset=50 has 5 rows");
    free(resp); resp = NULL;

    /* DESC by age, offset 0, limit 5 — ages 100..96. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ord_t\","
        "\"criteria\":[],\"order_by\":\"age\",\"order\":\"desc\","
        "\"offset\":0,\"limit\":5,\"fields\":[\"age\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"age\":\"100\"", "DESC[0..5] includes age=100");
    ASSERT_CONTAINS(resp, "\"age\":\"96\"", "DESC[0..5] includes age=96");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\":\"95\"") == NULL, "DESC[0..5] excludes age=95");
    ASSERT_TRUE(count_substr(resp, "\"key\":\"k") == 5, "DESC[0..5] has 5 rows");
    free(resp); resp = NULL;

    /* DESC by age, offset 50, limit 5 — ages 50..46. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ord_t\","
        "\"criteria\":[],\"order_by\":\"age\",\"order\":\"desc\","
        "\"offset\":50,\"limit\":5,\"fields\":[\"age\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"age\":\"50\"", "DESC offset=50 includes age=50");
    ASSERT_CONTAINS(resp, "\"age\":\"46\"", "DESC offset=50 includes age=46");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\":\"51\"") == NULL, "DESC offset=50 excludes age=51");
    ASSERT_TRUE(count_substr(resp, "\"key\":\"k") == 5, "DESC offset=50 has 5 rows");
    free(resp); resp = NULL;

    /* ASC + criteria active=true, offset 0, limit 5 — first 5 evens = 2,4,6,8,10. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ord_t\","
        "\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"order_by\":\"age\",\"order\":\"asc\","
        "\"offset\":0,\"limit\":5,\"fields\":[\"age\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"age\":\"2\"", "ASC criteria includes age=2");
    ASSERT_CONTAINS(resp, "\"age\":\"10\"", "ASC criteria includes age=10");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\":\"1\"") == NULL, "ASC criteria excludes age=1");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\":\"3\"") == NULL, "ASC criteria excludes age=3");
    ASSERT_TRUE(count_substr(resp, "\"key\":\"k") == 5, "ASC criteria has 5 rows");
    free(resp); resp = NULL;

    /* ASC + criteria active=true, offset 25, limit 5 — last 5 evens = 92,94,96,98,100. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ord_t\","
        "\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":\"true\"}],"
        "\"order_by\":\"age\",\"order\":\"asc\","
        "\"offset\":45,\"limit\":5,\"fields\":[\"age\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"age\":\"92\"", "ASC criteria offset includes age=92");
    ASSERT_CONTAINS(resp, "\"age\":\"100\"", "ASC criteria offset includes age=100");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\":\"90\"") == NULL, "ASC criteria offset excludes age=90");
    ASSERT_TRUE(count_substr(resp, "\"key\":\"k") == 5, "ASC criteria offset has 5 rows");
    free(resp); resp = NULL;

    /* offset beyond match count → empty result. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ord_t\","
        "\"criteria\":[],\"order_by\":\"age\",\"order\":\"asc\","
        "\"offset\":200,\"limit\":5,\"fields\":[\"age\"]}",
        &resp);
    ASSERT_TRUE(count_substr(resp, "\"key\":\"k") == 0, "offset>count yields 0 rows");
    free(resp); resp = NULL;

    /* dict format with offset. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ord_t\","
        "\"criteria\":[],\"order_by\":\"age\",\"order\":\"asc\","
        "\"offset\":0,\"limit\":3,\"format\":\"dict\",\"fields\":[\"age\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "dict format includes k1");
    ASSERT_CONTAINS(resp, "\"k2\"", "dict format includes k2");
    ASSERT_CONTAINS(resp, "\"k3\"", "dict format includes k3");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k4\"") == NULL, "dict format excludes k4");
    free(resp); resp = NULL;

    /* Non-indexed order_by (name) must still work via buffered fallback. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ord_t\","
        "\"criteria\":[],\"order_by\":\"name\",\"order\":\"asc\","
        "\"offset\":0,\"limit\":3,\"fields\":[\"name\"]}",
        &resp);
    /* "u1" is the lex-min, then "u10", "u100" */
    ASSERT_CONTAINS(resp, "\"u1\"", "non-indexed asc includes u1");
    ASSERT_CONTAINS(resp, "\"u10\"", "non-indexed asc includes u10");
    ASSERT_CONTAINS(resp, "\"u100\"", "non-indexed asc includes u100");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-find-indexed-orderby", test_find_indexed_orderby_run)
