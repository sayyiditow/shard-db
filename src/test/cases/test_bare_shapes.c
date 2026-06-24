/* src/test/cases/test_bare_shapes.c
 * Port of tests/test-bare-shapes.sh — verifies 2026.05.1 read-mode
 * response shapes (bare values for get/exists/count/size/orphaned,
 * dict for get-multi).
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

/* Compare response to expected, trimming trailing newlines. */
static int eq_str(const char *resp, const char *expected) {
    if (!resp || !expected) return 0;
    size_t rl = strlen(resp);
    while (rl > 0 && (resp[rl - 1] == '\n' || resp[rl - 1] == ' ')) rl--;
    size_t el = strlen(expected);
    return rl == el && memcmp(resp, expected, rl) == 0;
}

static int test_bare_shapes_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"shape_t\","
        "\"fields\":[\"name:varchar:32\",\"age:int\"]}", &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"shape_t\","
                   "\"key\":\"k1\",\"value\":{\"name\":\"alice\",\"age\":30}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"shape_t\","
                   "\"key\":\"k2\",\"value\":{\"name\":\"bob\",\"age\":25}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"shape_t\","
                   "\"key\":\"k3\",\"value\":{\"name\":\"carol\",\"age\":40}}", &resp); free(resp); resp = NULL;

    /* get single → bare value dict */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"shape_t\",\"key\":\"k1\"}", &resp);
    ASSERT_TRUE(eq_str(resp, "{\"name\":\"alice\",\"age\":30}"), "get k1 bare value");
    free(resp); resp = NULL;

    /* get-multi → dict */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"shape_t\",\"keys\":[\"k1\",\"k2\"]}", &resp);
    ASSERT_TRUE(eq_str(resp,
        "{\"k1\":{\"name\":\"alice\",\"age\":30},\"k2\":{\"name\":\"bob\",\"age\":25}}"),
        "two-key dict shape");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"shape_t\",\"keys\":[\"k1\",\"missing\"]}", &resp);
    ASSERT_TRUE(eq_str(resp,
        "{\"k1\":{\"name\":\"alice\",\"age\":30},\"missing\":null}"),
        "missing key emits null");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"shape_t\",\"keys\":[]}", &resp);
    ASSERT_TRUE(eq_str(resp, "{}"), "empty keys array → {}");
    free(resp); resp = NULL;

    /* exists single → bare bool */
    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"shape_t\",\"key\":\"k1\"}", &resp);
    ASSERT_TRUE(eq_str(resp, "true"), "exists present → true");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"shape_t\",\"key\":\"nothere\"}", &resp);
    ASSERT_TRUE(eq_str(resp, "false"), "exists absent → false");
    free(resp); resp = NULL;

    /* count → bare number */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"shape_t\"}", &resp);
    ASSERT_TRUE(eq_str(resp, "3"), "count no-criteria");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"shape_t\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"30\"}]}", &resp);
    ASSERT_TRUE(eq_str(resp, "2"), "count with criteria");
    free(resp); resp = NULL;

    /* size → bare disk bytes (positive integer) */
    tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"shape_t\"}", &resp);
    ASSERT_TRUE(atoll(resp) > 0, "size > 0 (disk bytes)");
    free(resp); resp = NULL;

    /* delete + orphaned → bare deleted count */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"shape_t\",\"key\":\"k2\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"orphaned\",\"dir\":\"default\",\"object\":\"shape_t\"}", &resp);
    ASSERT_TRUE(eq_str(resp, "1"), "orphaned = 1 after delete");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"shape_t\"}", &resp);
    ASSERT_TRUE(atoll(resp) > 0, "size > 0 after delete");
    free(resp); resp = NULL;

    /* fetch format:dict — wraps with {"results":...,"cursor":...} */
    tc_request(tc,
        "{\"mode\":\"fetch\",\"dir\":\"default\",\"object\":\"shape_t\","
        "\"format\":\"dict\",\"limit\":10}", &resp);
    ASSERT_CONTAINS(resp, "\"results\":{", "fetch dict: results is dict");
    ASSERT_CONTAINS(resp, "\"cursor\":", "fetch dict: cursor present");
    free(resp); resp = NULL;

    /* find format:dict — bare {} */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"shape_t\","
        "\"criteria\":[],\"format\":\"dict\",\"limit\":10}", &resp);
    ASSERT_TRUE(resp && resp[0] == '{', "find dict header");
    ASSERT_CONTAINS(resp, "\"k3\":", "find dict contains k3");
    free(resp); resp = NULL;

    /* find format:dict + order_by */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"shape_t\","
        "\"criteria\":[],\"format\":\"dict\",\"order_by\":\"age\",\"limit\":10}", &resp);
    ASSERT_TRUE(resp && resp[0] == '{', "ordered dict opens with {");
    free(resp); resp = NULL;

    /* find format:dict on indexed paths */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"shape_idx\","
        "\"fields\":[\"status:varchar:16\",\"region:varchar:8\",\"amount:int\"],"
        "\"indexes\":[\"status\",\"region\",\"amount\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"shape_idx\","
                   "\"key\":\"ki1\",\"value\":{\"status\":\"paid\",\"region\":\"us\",\"amount\":100}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"shape_idx\","
                   "\"key\":\"ki2\",\"value\":{\"status\":\"paid\",\"region\":\"eu\",\"amount\":250}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"shape_idx\","
                   "\"key\":\"ki3\",\"value\":{\"status\":\"refunded\",\"region\":\"us\",\"amount\":75}}", &resp); free(resp); resp = NULL;

    /* PRIMARY_LEAF */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"shape_idx\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"format\":\"dict\"}", &resp);
    ASSERT_CONTAINS(resp, "\"ki1\":", "PRIMARY_LEAF dict has ki1");
    ASSERT_CONTAINS(resp, "\"ki2\":", "PRIMARY_LEAF dict has ki2");
    ASSERT_TRUE(strstr(resp, "\"ki3\"") == NULL, "PRIMARY_LEAF dict excludes refunded");
    free(resp); resp = NULL;

    /* PRIMARY_INTERSECT */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"shape_idx\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                       "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}],"
        "\"format\":\"dict\"}", &resp);
    ASSERT_CONTAINS(resp, "\"ki1\":", "PRIMARY_INTERSECT dict has ki1");
    ASSERT_TRUE(strstr(resp, "\"ki2\"") == NULL, "INTERSECT dict excludes ki2");
    free(resp); resp = NULL;

    /* PRIMARY_KEYSET */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"shape_idx\","
        "\"criteria\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"refunded\"},"
                              "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"eu\"}]}],"
        "\"format\":\"dict\"}", &resp);
    ASSERT_CONTAINS(resp, "\"ki2\":", "PRIMARY_KEYSET dict has ki2");
    ASSERT_CONTAINS(resp, "\"ki3\":", "PRIMARY_KEYSET dict has ki3");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"drop-object\",\"dir\":\"default\",\"object\":\"shape_idx\"}", &resp);
    free(resp); resp = NULL;

    /* find format:dict + join → REJECTED */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"shape_t\","
        "\"criteria\":[],\"format\":\"dict\","
        "\"join\":[{\"object\":\"x\",\"local\":\"name\",\"remote\":\"key\",\"as\":\"y\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "dict", "dict + join → error");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bare-shapes", test_bare_shapes_run)
