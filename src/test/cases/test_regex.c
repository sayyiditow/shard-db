/* src/test/cases/test_regex.c
 * Port of tests/test-regex.sh — POSIX extended regex via regex / not_regex.
 *
 * Note on backslash escaping: the daemon's JSON parser doesn't decode \\
 * back to a single backslash, so regex meta-chars that need escaping
 * (\., \+) are written with a single literal backslash in the JSON value.
 * In a C string literal that means `\\.` (which compiles to `\.`).
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


static int do_count(TestClient *tc, const char *criteria) {
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rxt\","
        "\"criteria\":%s}", criteria);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_regex_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rxt\","
        "\"fields\":[\"email:varchar:64\",\"phone:varchar:32\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rxt\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"email\":\"alice@example.com\",\"phone\":\"+15551234567\"}},"
                     "{\"key\":\"k2\",\"value\":{\"email\":\"bob@TEST.org\",\"phone\":\"5555555\"}},"
                     "{\"key\":\"k3\",\"value\":{\"email\":\"not-an-email\",\"phone\":\"+44 20 7946 0958\"}},"
                     "{\"key\":\"k4\",\"value\":{\"email\":\"carol@x.io\",\"phone\":\"abc\"}}]}",
        &resp);
    free(resp); resp = NULL;

    /* Substring (no anchors) */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"org\"}]"),
                  1, "regex 'org' → 1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"@example\"}]"),
                  1, "regex '@example' → 1");

    /* Anchors */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"^carol\"}]"),
                  1, "regex '^carol' → 1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"org$\"}]"),
                  1, "regex 'org$' → 1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"^.*\\.com$\"}]"),
                  1, "regex '^.*\\.com$' → 1");

    /* Character classes */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"[A-Z]\"}]"),
                  1, "regex '[A-Z]' → 1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"^[a-z]+@[a-z]+\\.[a-z]+$\"}]"),
                  2, "regex lowercase email → 2");

    /* Quantifiers */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"al+\"}]"),
                  1, "regex 'al+' → 1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"al{2,}\"}]"),
                  0, "regex 'al{2,}' → 0");

    /* Alternation */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"(alice|bob)\"}]"),
                  2, "regex '(alice|bob)' → 2");

    /* Phone */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"phone\",\"op\":\"regex\",\"value\":\"^\\+\"}]"),
                  2, "regex '^\\+' → 2");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"phone\",\"op\":\"regex\",\"value\":\"[0-9]{7,}\"}]"),
                  2, "regex '[0-9]{7,}' → 2");

    /* not_regex */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"phone\",\"op\":\"not_regex\",\"value\":\"^\\+\"}]"),
                  2, "not_regex '^\\+' → 2");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"not_regex\",\"value\":\"org\"}]"),
                  3, "not_regex 'org' → 3");

    /* Invalid regex degrades gracefully */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"^(.*\"}]"),
                  0, "bad regex → 0");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"email\",\"op\":\"not_regex\",\"value\":\"^(.*\"}]"),
                  4, "bad not_regex → 4 (negation of compile-fail = match all)");

    /* Combined */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"^[a-z]+@[a-z]+\\.[a-z]+$\"},"
         "{\"field\":\"phone\",\"op\":\"regex\",\"value\":\"^\\+\"}]"),
        1, "lowercase email AND +phone → 1 (k1)");

    /* Find returns right rows */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rxt\","
        "\"criteria\":[{\"field\":\"phone\",\"op\":\"regex\",\"value\":\"^\\+\"}],"
        "\"fields\":[\"email\",\"phone\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k1\"", "find regex includes k1");
    ASSERT_CONTAINS(resp, "\"key\":\"k3\"", "find regex includes k3");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"key\":\"k2\"") == NULL, "find regex excludes k2");
    free(resp); resp = NULL;

    /* Aggregate */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"rxt\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"criteria\":[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"@\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"n\":3", "agg count regex '@' = 3");
    free(resp); resp = NULL;

    /* bulk-update via regex */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"rxt\","
        "\"criteria\":[{\"field\":\"email\",\"op\":\"regex\",\"value\":\"^not-\"}],"
        "\"value\":{\"phone\":\"BLOCKED\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rxt\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"phone\":\"BLOCKED\"", "bulk-update via regex hit k3");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rxt\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"phone\":\"+15551234567\"", "bulk-update did not touch k1");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-regex", test_regex_run)
