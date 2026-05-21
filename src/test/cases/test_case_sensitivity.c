/* src/test/cases/test_case_sensitivity.c
 * Port of tests/test-case-sensitivity.sh — case-sensitive defaults for
 * like/contains/starts/ends + case-insensitive ilike/icontains/istarts/iends.
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


static int run_count(TestClient *tc, const char *field, const char *op, const char *value) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"cit\","
        "\"criteria\":[{\"field\":\"%s\",\"op\":\"%s\",\"value\":\"%s\"}]}",
        field, op, value);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_case_sensitivity_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cit\","
        "\"fields\":[\"name:varchar:32\",\"tag:varchar:32\"],"
        "\"indexes\":[\"name\"],\"splits\":16}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"cit\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"name\":\"Alice\",\"tag\":\"VIP\"}},"
                     "{\"key\":\"k2\",\"value\":{\"name\":\"alice\",\"tag\":\"vip\"}},"
                     "{\"key\":\"k3\",\"value\":{\"name\":\"BOB\",\"tag\":\"vip\"}},"
                     "{\"key\":\"k4\",\"value\":{\"name\":\"bob\",\"tag\":\"VIP\"}},"
                     "{\"key\":\"k5\",\"value\":{\"name\":\"Carol\",\"tag\":\"std\"}}]}",
        &resp); free(resp); resp = NULL;

    /* contains (CS) */
    ASSERT_EQ_INT(run_count(tc, "name", "contains", "bob"), 1, "contains 'bob' (CS) → 1");
    ASSERT_EQ_INT(run_count(tc, "name", "contains", "BOB"), 1, "contains 'BOB' (CS) → 1");
    ASSERT_EQ_INT(run_count(tc, "name", "contains", "Bob"), 0, "contains 'Bob' (CS) → 0");

    /* icontains (CI) */
    ASSERT_EQ_INT(run_count(tc, "name", "icontains", "bob"), 2, "icontains 'bob' (CI) → 2");
    ASSERT_EQ_INT(run_count(tc, "name", "icontains", "BOB"), 2, "icontains 'BOB' (CI) → 2");
    ASSERT_EQ_INT(run_count(tc, "name", "icontains", "Bob"), 2, "icontains 'Bob' (CI) → 2");

    /* starts (CS) */
    ASSERT_EQ_INT(run_count(tc, "name", "starts", "ALI"), 0, "starts 'ALI' (CS) → 0");
    ASSERT_EQ_INT(run_count(tc, "name", "starts", "Ali"), 1, "starts 'Ali' (CS) → 1");
    ASSERT_EQ_INT(run_count(tc, "name", "starts", "ali"), 1, "starts 'ali' (CS) → 1");

    /* istarts (CI) */
    ASSERT_EQ_INT(run_count(tc, "name", "istarts", "ALI"), 2, "istarts 'ALI' (CI) → 2");
    ASSERT_EQ_INT(run_count(tc, "name", "istarts", "b"), 2, "istarts 'b' (CI) → 2");

    /* ends (CS) */
    ASSERT_EQ_INT(run_count(tc, "name", "ends", "OB"), 1, "ends 'OB' (CS) → 1");
    ASSERT_EQ_INT(run_count(tc, "name", "ends", "ob"), 1, "ends 'ob' (CS) → 1");
    ASSERT_EQ_INT(run_count(tc, "name", "ends", "Ob"), 0, "ends 'Ob' (CS) → 0");

    /* iends (CI) */
    ASSERT_EQ_INT(run_count(tc, "name", "iends", "OB"), 2, "iends 'OB' (CI) → 2");
    ASSERT_EQ_INT(run_count(tc, "name", "iends", "ce"), 2, "iends 'ce' (CI) → 2");

    /* like (CS) */
    ASSERT_EQ_INT(run_count(tc, "name", "like", "B%"), 1, "like 'B%' (CS) → 1");
    ASSERT_EQ_INT(run_count(tc, "name", "like", "A%"), 1, "like 'A%' (CS) → 1");
    ASSERT_EQ_INT(run_count(tc, "name", "like", "a%"), 1, "like 'a%' (CS) → 1");
    ASSERT_EQ_INT(run_count(tc, "name", "like", "%E"), 0, "like '%E' (CS) → 0");
    ASSERT_EQ_INT(run_count(tc, "name", "like", "%e"), 2, "like '%e' (CS) → 2");
    ASSERT_EQ_INT(run_count(tc, "name", "like", "BOB"), 1, "like 'BOB' no-% → 1");
    ASSERT_EQ_INT(run_count(tc, "name", "like", "bob"), 1, "like 'bob' no-% → 1");
    ASSERT_EQ_INT(run_count(tc, "name", "like", "Bob"), 0, "like 'Bob' no-% → 0");

    /* ilike (CI) */
    ASSERT_EQ_INT(run_count(tc, "name", "ilike", "B%"), 2, "ilike 'B%' (CI) → 2");
    ASSERT_EQ_INT(run_count(tc, "name", "ilike", "%LICE"), 2, "ilike '%LICE' (CI) → 2");
    ASSERT_EQ_INT(run_count(tc, "name", "ilike", "BoB"), 2, "ilike 'BoB' no-% (CI) → 2");
    ASSERT_EQ_INT(run_count(tc, "name", "ilike", "%c%"), 3, "ilike '%c%' (CI) → 3");

    /* Negatives */
    ASSERT_EQ_INT(run_count(tc, "name", "not_contains", "bob"), 4, "not_contains 'bob' → 4");
    ASSERT_EQ_INT(run_count(tc, "name", "not_icontains", "bob"), 3, "not_icontains 'bob' → 3");
    ASSERT_EQ_INT(run_count(tc, "name", "not_like", "B%"), 4, "not_like 'B%' → 4");
    ASSERT_EQ_INT(run_count(tc, "name", "not_ilike", "B%"), 3, "not_ilike 'B%' → 3");

    /* Non-indexed tag */
    ASSERT_EQ_INT(run_count(tc, "tag", "contains", "vip"), 2, "tag contains 'vip' (CS) → 2");
    ASSERT_EQ_INT(run_count(tc, "tag", "icontains", "vip"), 4, "tag icontains 'vip' (CI) → 4");
    ASSERT_EQ_INT(run_count(tc, "tag", "starts", "V"), 2, "tag starts 'V' (CS) → 2");
    ASSERT_EQ_INT(run_count(tc, "tag", "istarts", "v"), 4, "tag istarts 'v' (CI) → 4");

    /* find indexed CS */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cit\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"contains\",\"value\":\"bob\"}],"
        "\"fields\":[\"name\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"bob\"", "find contains 'bob' returns bob");
    ASSERT_TRUE(strstr(resp, "\"name\":\"BOB\"") == NULL, "find contains 'bob' rejects BOB");
    free(resp); resp = NULL;

    /* find ilike (CI) */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cit\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"ilike\",\"value\":\"%ob%\"}],"
        "\"fields\":[\"name\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"bob\"", "find ilike returns bob");
    ASSERT_CONTAINS(resp, "\"name\":\"BOB\"", "find ilike returns BOB");
    free(resp); resp = NULL;

    /* aggregate */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"cit\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"criteria\":[{\"field\":\"name\",\"op\":\"icontains\",\"value\":\"a\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"n\":3", "agg count icontains 'a' = 3");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-case-sensitivity", test_case_sensitivity_run)
