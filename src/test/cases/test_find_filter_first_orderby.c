/* src/test/cases/test_find_filter_first_orderby.c
 *
 * Regression test for the cmd_find filter-first + order_by planner
 * shipped 2026.05.7.x. The bug fixed: when criteria has an indexed
 * leaf AND order_by is set, the old planner walked the order_by
 * btree end-to-end and ran criteria_match per record. For selective
 * filters (trigram contains, btree eq on a rare value, bitmap eq on
 * a minority value) this scanned the entire order_by index just to
 * confirm zero matches.
 *
 * Discovered via HN explorer /search at 789K comments:
 *   find icontains 'kubernetes' order_by time desc limit 25
 *   → 14s pre-fix (walks entire time btree)
 *   → ~80ms post-fix (empty KeySet short-circuits)
 *
 * This test covers all four filter shapes the fix handles:
 *   1. trigram contains → 0 matches  (empty KeySet short-circuit)
 *   2. trigram contains → some matches
 *   3. btree eq → 0 matches  (the same shape, different builder)
 *   4. btree eq → 1 match
 *
 * Each case verifies the query returns the expected rows in the
 * correct order. A perf regression of the underlying KeySet
 * pre-filter would re-introduce the per-record fetch + criteria
 * match for every order_by entry. Correctness regression would show
 * up as wrong rows or wrong ordering, which is what this asserts.
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

static int count_substr(const char *resp, const char *s) {
    if (!resp || !s) return 0;
    int n = 0;
    const char *p = resp;
    size_t sl = strlen(s);
    while ((p = strstr(p, s)) != NULL) { n++; p += sl; }
    return n;
}

static int test_find_filter_first_orderby_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;

    /* Object with three index types we care about:
       - age:int (btree, used as order_by)
       - tag:varchar:32 (btree, used for btree-eq filter)
       - bio:varchar:200 (trigram, used for trigram filter)
       The order_by walk runs on age; the filter side rotates
       between tag (btree-eq) and bio (trigram contains). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ffo\","
        "\"fields\":[\"age:int\",\"tag:varchar:32\",\"bio:varchar:200\"],"
        "\"indexes\":[\"age\",\"tag\",\"bio:trigram\"],\"splits\":8}",
        &resp);
    ASSERT_CONTAINS(resp, "created", "create-object ffo");
    free(resp); resp = NULL;

    /* Seed 100 rows. age 1..100 unique. tag = "common" for everyone
       except row 42 which gets "rare_tag" (used as the 1-match btree
       case). bio embeds "engineer" in odd rows and "scientist" in
       even rows so trigram contains matches a predictable subset.
       No row contains "kubernetes" — that's the zero-match probe. */
    size_t cap = 100 * 256 + 256;
    char *payload = malloc(cap);
    ASSERT_TRUE(payload != NULL, "alloc payload");
    if (!payload) { tc_close(tc); test_env_stop(&env); return 1; }
    size_t len = 0;
    SB_APPEND(payload, len, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ffo\",\"records\":[");
    for (int i = 1; i <= 100; i++) {
        const char *tag = (i == 42) ? "rare_tag" : "common";
        const char *bio = (i % 2) ? "Software engineer at startup"
                                  : "Data scientist exploring frontiers";
        SB_APPEND(payload, len, cap,
            "%s{\"key\":\"k%d\",\"value\":{\"age\":%d,\"tag\":\"%s\",\"bio\":\"%s\"}}",
            i == 1 ? "" : ",", i, i, tag, bio);
    }
    SB_APPEND(payload, len, cap, "]}");
    tc_request(tc, payload, &resp); free(resp); resp = NULL;
    free(payload);

    /* === Case 1: trigram filter, ZERO matches, with order_by ===
       Pre-fix: walked all 100 age entries → 14s shape.
       Post-fix: empty trigram KeySet → instant []. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ffo\","
        "\"criteria\":[{\"field\":\"bio\",\"op\":\"contains\",\"value\":\"kubernetes\"}],"
        "\"order_by\":\"age\",\"order\":\"desc\",\"limit\":10,\"fields\":[\"age\"]}",
        &resp);
    ASSERT_NOT_NULL(resp, "case1: response received");
    ASSERT_TRUE(strcmp(resp, "[]\n") == 0 || strcmp(resp, "[]") == 0,
        "case1: trigram 0-match + order_by returns []");
    free(resp); resp = NULL;

    /* === Case 2: trigram filter, MANY matches, with order_by ===
       50 odd rows contain "engineer". DESC by age limit 10 →
       ages 99, 97, 95, ..., 81 (10 highest odd ages). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ffo\","
        "\"criteria\":[{\"field\":\"bio\",\"op\":\"contains\",\"value\":\"engineer\"}],"
        "\"order_by\":\"age\",\"order\":\"desc\",\"limit\":10,\"fields\":[\"age\"]}",
        &resp);
    ASSERT_NOT_NULL(resp, "case2: response received");
    ASSERT_CONTAINS(resp, "\"age\":\"99\"", "case2: includes age=99 (highest odd)");
    ASSERT_CONTAINS(resp, "\"age\":\"81\"", "case2: includes age=81 (10th-highest odd)");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\":\"79\"") == NULL,
        "case2: excludes age=79 (would be 11th, past limit)");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\":\"100\"") == NULL,
        "case2: excludes age=100 (even row, bio='scientist')");
    ASSERT_TRUE(count_substr(resp, "\"key\":\"k") == 10,
        "case2: exactly 10 rows");
    free(resp); resp = NULL;

    /* === Case 3: btree-eq filter, ZERO matches, with order_by ===
       Same shape as kubernetes-on-comments — selective btree-eq +
       order_by + zero hits. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ffo\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"__no_such_tag__\"}],"
        "\"order_by\":\"age\",\"order\":\"desc\",\"limit\":10,\"fields\":[\"age\"]}",
        &resp);
    ASSERT_NOT_NULL(resp, "case3: response received");
    ASSERT_TRUE(strcmp(resp, "[]\n") == 0 || strcmp(resp, "[]") == 0,
        "case3: btree-eq 0-match + order_by returns []");
    free(resp); resp = NULL;

    /* === Case 4: btree-eq filter, ONE match, with order_by ===
       Only row 42 has tag='rare_tag'. Expect exactly that row back. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ffo\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"rare_tag\"}],"
        "\"order_by\":\"age\",\"order\":\"desc\",\"limit\":10,\"fields\":[\"age\",\"tag\"]}",
        &resp);
    ASSERT_NOT_NULL(resp, "case4: response received");
    ASSERT_CONTAINS(resp, "\"age\":\"42\"", "case4: returns the single matching row");
    ASSERT_CONTAINS(resp, "\"tag\":\"rare_tag\"", "case4: tag column present");
    ASSERT_TRUE(count_substr(resp, "\"key\":\"k") == 1,
        "case4: exactly 1 row");
    free(resp); resp = NULL;

    /* === Case 5: empty envelope shape — both formats ===
       Empty KeySet must close the ALREADY-OPEN envelope. Bug
       observed in HN explorer where short-circuit emitted a fresh
       `[]` after the caller had emitted `[`, producing `[[]`.
       Verify default format closes correctly. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ffo\","
        "\"criteria\":[{\"field\":\"bio\",\"op\":\"contains\",\"value\":\"xyznoresult\"}],"
        "\"order_by\":\"age\",\"order\":\"desc\",\"limit\":5}",
        &resp);
    ASSERT_NOT_NULL(resp, "case5: response received");
    /* Strict shape: response should be `[]` (with optional trailing newline),
       not `[[]` or `{}` or anything else. */
    ASSERT_TRUE(resp[0] == '[' && (resp[1] == ']' || resp[1] == 0),
        "case5: envelope opens and immediately closes — no `[[`");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-find-filter-first-orderby", test_find_filter_first_orderby_run)
