/* src/test/cases/test_regex_anchor_prefilter.c
 * Verify the literal-anchor pre-filter on POSIX ERE regex doesn't
 * change query results — only speeds up the no-match case.
 *
 * The pre-filter is sound iff every regex query returns the same set
 * of records that plain regexec would return. We exercise patterns
 * where:
 *   - Anchors WILL be extracted (literal alternation, ^literal,
 *     bare literal). Pre-filter active.
 *   - Anchors WILL NOT be extracted (bracket classes, dot, +, *).
 *     Falls through to plain regexec.
 *   - Negated regex (NOT_REGEX): pre-filter inverts correctly.
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

static int extract_int(const char *resp) {
    if (!resp) return -1;
    return atoi(resp);
}

static int test_regex_anchor_prefilter_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    /* email indexed (regex on indexed varchar exercises btree-leaf path),
       bio non-indexed (regex on full record scan path). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rgx_t\","
        "\"fields\":[\"email:varchar:64\",\"bio:varchar:128\"],"
        "\"indexes\":[\"email\"],\"splits\":16}",
        &resp); free(resp); resp = NULL;

    /* Seed 100 rows. Spread emails across providers; bios with mixed content. */
    size_t cap = 100 * 192 + 256;
    char *payload = malloc(cap);
    if (!payload) { tc_close(tc); test_env_stop(&env); return 1; }
    size_t len = 0;
    SB_APPEND(payload, len, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"rgx_t\",\"records\":[");
    const char *providers[5] = {"@gmail.com", "@yahoo.com", "@outlook.com", "@example.org", "@example.net"};
    const char *bio_words[5] = {"engineer building stuff",
                                 "developer of services",
                                 "designer with portfolio",
                                 "zebra striped pattern",
                                 "manager and coordinator"};
    for (int i = 1; i <= 100; i++) {
        SB_APPEND(payload, len, cap,
            "%s{\"key\":\"k%d\",\"value\":{\"email\":\"u%d%s\",\"bio\":\"%s\"}}",
            i == 1 ? "" : ",", i, i, providers[i % 5], bio_words[i % 5]);
    }
    SB_APPEND(payload, len, cap, "]}");
    tc_request(tc, payload, &resp); free(resp); resp = NULL;
    free(payload);

    char buf[64];

    /* Pattern with extractable literal alternation. Anchors:
       ["@gmail", "@yahoo"]. Records 0,5,10... use @gmail (~20 rows),
       1,6,11... use @yahoo (~20 rows). Pattern technically matches
       @gmail.com or @yahoo.com → ~40 rows. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rgx_t\","
        "\"criteria\":[{\"field\":\"email\",\"op\":\"regex\","
                       "\"value\":\"@gmail|@yahoo\"}]}", &resp);
    int n_alt = extract_int(resp);
    snprintf(buf, sizeof(buf), "regex email '@gmail|@yahoo' = %d (expect 40)", n_alt);
    ASSERT_TRUE(n_alt == 40, buf);
    free(resp); resp = NULL;

    /* Pattern with bracket class — anchors extraction bails, plain regex
       runs on every record. Same answer should come back regardless. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rgx_t\","
        "\"criteria\":[{\"field\":\"email\",\"op\":\"regex\","
                       "\"value\":\"@[gy]ahoo|@gmail\"}]}", &resp);
    int n_class = extract_int(resp);
    /* @[gy]ahoo matches @gahoo or @yahoo. @gahoo doesn't exist; @yahoo
       matches our 20 yahoo rows. @gmail matches 20. Total 40. */
    snprintf(buf, sizeof(buf), "regex bracket class = %d (expect 40)", n_class);
    ASSERT_TRUE(n_class == 40, buf);
    free(resp); resp = NULL;

    /* not_regex on a literal anchor pattern. Pattern `engineer` matches
       ~20 bios (i%5==0); not_regex matches the other 80. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rgx_t\","
        "\"criteria\":[{\"field\":\"bio\",\"op\":\"not_regex\","
                       "\"value\":\"engineer\"}]}", &resp);
    int n_neg = extract_int(resp);
    snprintf(buf, sizeof(buf), "not_regex 'engineer' = %d (expect 80)", n_neg);
    ASSERT_TRUE(n_neg == 80, buf);
    free(resp); resp = NULL;

    /* Pattern `^z` — caret anchor + literal. Anchor extracted as "z",
       pre-filter checks haystack contains 'z'. False positive: bio with
       'z' but not at start (e.g. "blazing"). regexec resolves. None of
       our bios have 'z' anywhere except "zebra ..." which DOES start
       with z (~20 rows). */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rgx_t\","
        "\"criteria\":[{\"field\":\"bio\",\"op\":\"regex\","
                       "\"value\":\"^z\"}]}", &resp);
    int n_caret = extract_int(resp);
    snprintf(buf, sizeof(buf), "regex '^z' = %d (expect 20)", n_caret);
    ASSERT_TRUE(n_caret == 20, buf);
    free(resp); resp = NULL;

    /* Pattern with full-on metachar (.+) — anchors bail, plain regex
       runs. `.+@.+` matches any non-empty before+after @. All 100 rows. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"rgx_t\","
        "\"criteria\":[{\"field\":\"email\",\"op\":\"regex\","
                       "\"value\":\".+@.+\"}]}", &resp);
    int n_meta = extract_int(resp);
    snprintf(buf, sizeof(buf), "regex '.+@.+' = %d (expect 100)", n_meta);
    ASSERT_TRUE(n_meta == 100, buf);
    free(resp); resp = NULL;

    /* Find with regex — verify result set, not just count. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rgx_t\","
        "\"criteria\":[{\"field\":\"bio\",\"op\":\"regex\","
                       "\"value\":\"engineer|developer\"}],"
        "\"limit\":100,\"fields\":[\"email\"]}", &resp);
    /* Count occurrences of "key" markers — we expect 40 (engineer 20 + developer 20). */
    int marker_count = 0;
    const char *p2 = resp;
    while ((p2 = strstr(p2, "\"key\":\"k")) != NULL) { marker_count++; p2 += 8; }
    snprintf(buf, sizeof(buf), "find regex 'engineer|developer' = %d rows (expect 40)", marker_count);
    ASSERT_TRUE(marker_count == 40, buf);
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-regex-anchor-prefilter", test_regex_anchor_prefilter_run)
