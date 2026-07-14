/* src/test/cases/test_unknown_field_validation.c
 * Locks the post-2026-05-25 contract that queries referencing an unknown
 * field in criteria / order_by / projection / group_by / aggregate-spec
 * RETURN A LOUD ERROR rather than silently producing zero / empty results.
 *
 * Pre-fix behaviour: a `category='books'` query against a DB without
 * `category` did a full 25M ordered walk, found 0 matches, returned `[]`
 * in 38 seconds. New behaviour: error in microseconds, no scan.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>

static int has_unknown_field_error(const char *resp) {
    return resp && SAFE_STRSTR(resp, "unknown field") != NULL;
}

static int test_unknown_field_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\",\"age:int\",\"city:varchar:32\"],"
        "\"indexes\":[\"name\",\"age\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ufv\",\"key\":\"k1\","
        "\"value\":{\"name\":\"alice\",\"age\":30,\"city\":\"london\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ufv\",\"key\":\"k2\","
        "\"value\":{\"name\":\"bob\",\"age\":25,\"city\":\"paris\"}}", &resp);
    free(resp); resp = NULL;

    /* ===== count =====
       Criterion on a field that doesn't exist must error. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"books\"}]}",
        &resp);
    ASSERT_TRUE(has_unknown_field_error(resp), "count: unknown criterion field → error");
    free(resp); resp = NULL;

    /* ===== find — criteria ===== */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"books\"}],"
        "\"limit\":10}", &resp);
    ASSERT_TRUE(has_unknown_field_error(resp), "find: unknown criterion field → error");
    free(resp); resp = NULL;

    /* ===== find — order_by ===== */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"alice\"}],"
        "\"order_by\":\"category\",\"order\":\"desc\",\"limit\":10}", &resp);
    ASSERT_TRUE(has_unknown_field_error(resp), "find: unknown order_by field → error");
    free(resp); resp = NULL;

    /* ===== find — projection ===== */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"alice\"}],"
        "\"limit\":10,\"fields\":[\"name\",\"category\"]}", &resp);
    ASSERT_TRUE(has_unknown_field_error(resp), "find: unknown projection field → error");
    free(resp); resp = NULL;

    /* ===== field-vs-field RHS ===== */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"eq_field\",\"value\":\"missing\"}]}",
        &resp);
    ASSERT_TRUE(has_unknown_field_error(resp), "count: eq_field unknown RHS → error");
    free(resp); resp = NULL;

    /* ===== aggregate — criteria ===== */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"books\"}],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}", &resp);
    ASSERT_TRUE(has_unknown_field_error(resp), "aggregate: unknown criterion field → error");
    free(resp); resp = NULL;

    /* ===== aggregate — group_by ===== */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"group_by\":[\"category\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}", &resp);
    ASSERT_TRUE(has_unknown_field_error(resp), "aggregate: unknown group_by → error");
    free(resp); resp = NULL;

    /* ===== aggregate — agg spec field ===== */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"missing_total\",\"alias\":\"s\"}]}",
        &resp);
    ASSERT_TRUE(has_unknown_field_error(resp), "aggregate: unknown sum() field → error");
    free(resp); resp = NULL;

    /* ===== aggregate — order_by referencing an aggregate alias is OK =====
       This is the regression test that caught my first overly-strict
       validation: order_by="n" must resolve to the count alias, not be
       rejected as an unknown field. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"group_by\":[\"age\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"order_by\":\"n\",\"order\":\"desc\",\"limit\":10}", &resp);
    ASSERT_TRUE(resp && !has_unknown_field_error(resp),
                "aggregate: order_by=alias 'n' is accepted (not unknown)");
    free(resp); resp = NULL;

    /* ===== aggregate — order_by referencing a group_by field is OK ===== */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"group_by\":[\"age\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"order_by\":\"age\",\"order\":\"desc\",\"limit\":10}", &resp);
    ASSERT_TRUE(resp && !has_unknown_field_error(resp),
                "aggregate: order_by=group_by field is accepted");
    free(resp); resp = NULL;

    /* ===== aggregate — order_by referencing nothing → error ===== */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"group_by\":[\"age\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"order_by\":\"category\",\"order\":\"desc\",\"limit\":10}", &resp);
    ASSERT_TRUE(has_unknown_field_error(resp),
                "aggregate: order_by=missing → error");
    free(resp); resp = NULL;

    /* ===== composite field: all subfields must exist =====
       'name+city' has both subfields in the schema → accepted. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"criteria\":[{\"field\":\"name+city\",\"op\":\"eq\",\"value\":\"alice|london\"}]}",
        &resp);
    ASSERT_TRUE(resp && !has_unknown_field_error(resp)
                && SAFE_STRSTR(resp, "unknown sub-field") == NULL,
                "count: composite 'name+city' (both subfields exist) → accepted");
    free(resp); resp = NULL;

    /* ===== composite with unknown subfield → error =====
       Composite fields only exist in the context of indexes; an unknown
       sub-name is the same bug class as a plain unknown field. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ufv\","
        "\"criteria\":[{\"field\":\"name+missing\",\"op\":\"eq\",\"value\":\"alice|x\"}]}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "unknown sub-field 'missing'") != NULL,
                "count: composite 'name+missing' → error pinpointing 'missing'");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-unknown-field-validation", test_unknown_field_validation_run)
