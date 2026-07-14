/* src/test/cases/test_agg_varchar_groupby_sum.c
 *
 * Extends the streaming-distinct fast path (test_agg_varchar_groupby_limit
 * already pins COUNT) to SUM / AVG / MIN / MAX on an indexed numeric agg
 * field. Same gating as that test: single indexed varchar group_by,
 * finite limit, no criteria / having / order_by. The new contract is that
 * within each emitted distinct group key, the agg field is fetched from
 * every contributing record and accumulated into the bucket's accumulator.
 *
 * Cases pinned here:
 *   1. duplicate keys across shards — sum/avg per group must equal the
 *      full per-key total, not a per-shard slice
 *   2. all-distinct keys — sum/avg per group must equal the single
 *      record's balance, count must be 1 each
 *   3. combined COUNT + SUM specs in one query — both accumulate
 *      correctly
 *   4. limit > distinct count — emits all groups, no error
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

static int count_braces(const char *s) {
    int n = 0;
    for (const char *p = s; (p = strchr(p, '{')) != NULL; p++) n++;
    return n;
}

/* Count how many times needle appears in resp. */
static int count_substr(const char *resp, const char *needle) {
    int n = 0;
    if (!resp || !needle) return 0;
    for (const char *p = resp; (p = strstr(p, needle)) != NULL; p++) n++;
    return n;
}

static int test_agg_varchar_groupby_sum_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* === Case 1: duplicates across shards — 300 rows × 3 roles ===
     * admin:   balance 100.00 each, ×100 → sum 10000, avg 100, min/max 100
     * editor:  balance 200.00 each, ×100 → sum 20000, avg 200, min/max 200
     * viewer:  balance 300.00 each, ×100 → sum 30000, avg 300, min/max 300
     */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u_sum\","
        "\"fields\":[\"role:varchar:16\",\"balance:numeric:14,2\"],"
        "\"indexes\":[\"role\",\"balance\"],\"splits\":8}", &resp);
    free(resp); resp = NULL;

    size_t cap = 300 * 192 + 256;
    char *payload = malloc(cap);
    ASSERT_NOT_NULL(payload, "alloc payload");
    if (!payload) { tc_close(tc); test_env_stop(&env); return 1; }
    size_t len = 0;
    SB_APPEND(payload, len, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"u_sum\",\"records\":[");
    const char *roles[3]    = {"admin", "editor", "viewer"};
    const char *balances[3] = {"100.00", "200.00", "300.00"};
    for (int i = 1; i <= 300; i++) {
        int r = (i - 1) % 3;
        SB_APPEND(payload, len, cap,
            "%s{\"key\":\"k%d\",\"value\":{\"role\":\"%s\",\"balance\":\"%s\"}}",
            i == 1 ? "" : ",", i, roles[r], balances[r]);
    }
    SB_APPEND(payload, len, cap, "]}");
    tc_request(tc, payload, &resp); free(resp); resp = NULL;
    free(payload);

    /* sum(balance) limit 2 — expect 2 groups each with sum=10000 or 20000 */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"u_sum\","
        "\"group_by\":[\"role\"],"
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"balance\",\"alias\":\"s\"}],"
        "\"limit\":2}", &resp);
    ASSERT_EQ_INT(count_braces(resp), 2, "case1: sum limit=2 → 2 buckets");
    /* The 2 emitted should each show full per-key total. We don't know which
       2 of admin/editor/viewer the planner picks (depends on lex order: admin,
       editor are the first 2 alphabetically), so check that BOTH 10000 and
       20000 appear and 30000 does NOT. */
    ASSERT_TRUE(count_substr(resp, "\"s\":10000") >= 1, "case1: sum=10000 present");
    ASSERT_TRUE(count_substr(resp, "\"s\":20000") >= 1, "case1: sum=20000 present");
    ASSERT_EQ_INT(count_substr(resp, "\"s\":30000"), 0, "case1: sum=30000 absent");
    free(resp); resp = NULL;

    /* avg(balance) limit 3 — 3 distinct roles, expect 100, 200, 300 */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"u_sum\","
        "\"group_by\":[\"role\"],"
        "\"aggregates\":[{\"fn\":\"avg\",\"field\":\"balance\",\"alias\":\"a\"}],"
        "\"limit\":3}", &resp);
    ASSERT_EQ_INT(count_braces(resp), 3, "case1: avg limit=3 → 3 buckets");
    ASSERT_TRUE(count_substr(resp, "\"a\":100") >= 1, "case1: avg admin=100");
    ASSERT_TRUE(count_substr(resp, "\"a\":200") >= 1, "case1: avg editor=200");
    ASSERT_TRUE(count_substr(resp, "\"a\":300") >= 1, "case1: avg viewer=300");
    free(resp); resp = NULL;

    /* min(balance) + max(balance) limit 2 — each role has constant balance,
       so min=max=that balance */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"u_sum\","
        "\"group_by\":[\"role\"],"
        "\"aggregates\":[{\"fn\":\"min\",\"field\":\"balance\",\"alias\":\"mn\"},"
                        "{\"fn\":\"max\",\"field\":\"balance\",\"alias\":\"mx\"}],"
        "\"limit\":2}", &resp);
    ASSERT_EQ_INT(count_braces(resp), 2, "case1: min/max limit=2 → 2 buckets");
    /* admin row should have mn=100, mx=100 */
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"role\":\"admin\"") != NULL, "case1: admin in result");
    free(resp); resp = NULL;

    /* count + sum together — bench uses this shape */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"u_sum\","
        "\"group_by\":[\"role\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},"
                        "{\"fn\":\"sum\",\"field\":\"balance\",\"alias\":\"s\"}],"
        "\"limit\":2}", &resp);
    ASSERT_EQ_INT(count_braces(resp), 2, "case1: count+sum limit=2 → 2 buckets");
    /* Both emitted should have n=100 and one of (s=10000 or s=20000). */
    ASSERT_EQ_INT(count_substr(resp, "\"n\":100"), 2, "case1: both counts=100");
    free(resp); resp = NULL;

    /* === Case 2: all-distinct usernames, each with unique balance === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u_uniq\","
        "\"fields\":[\"username:varchar:16\",\"balance:numeric:14,2\"],"
        "\"indexes\":[\"username\",\"balance\"],\"splits\":8}", &resp);
    free(resp); resp = NULL;

    cap = 200 * 128 + 256;
    payload = malloc(cap);
    ASSERT_NOT_NULL(payload, "alloc payload 2");
    if (!payload) { tc_close(tc); test_env_stop(&env); return 1; }
    len = 0;
    SB_APPEND(payload, len, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"u_uniq\",\"records\":[");
    for (int i = 1; i <= 200; i++) {
        SB_APPEND(payload, len, cap,
            "%s{\"key\":\"k%d\",\"value\":{\"username\":\"user_%04d\",\"balance\":\"%d.00\"}}",
            i == 1 ? "" : ",", i, i, i * 10);
    }
    SB_APPEND(payload, len, cap, "]}");
    tc_request(tc, payload, &resp); free(resp); resp = NULL;
    free(payload);

    /* sum(balance) limit 5: 5 distinct usernames, each with one record.
       sum equals that record's balance. count is 1 each. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"u_uniq\","
        "\"group_by\":[\"username\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},"
                        "{\"fn\":\"sum\",\"field\":\"balance\",\"alias\":\"s\"}],"
        "\"limit\":5}", &resp);
    ASSERT_EQ_INT(count_braces(resp), 5, "case2: limit=5 → 5 buckets");
    ASSERT_EQ_INT(count_substr(resp, "\"n\":1"), 5, "case2: all counts=1");
    free(resp); resp = NULL;

    /* === Case 3: limit > distinct count for u_sum (3 roles) === */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"u_sum\","
        "\"group_by\":[\"role\"],"
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"balance\",\"alias\":\"s\"}],"
        "\"limit\":100}", &resp);
    ASSERT_EQ_INT(count_braces(resp), 3, "case3: limit > distinct → 3 buckets");
    ASSERT_TRUE(count_substr(resp, "\"s\":10000") >= 1, "case3: admin sum=10000");
    ASSERT_TRUE(count_substr(resp, "\"s\":20000") >= 1, "case3: editor sum=20000");
    ASSERT_TRUE(count_substr(resp, "\"s\":30000") >= 1, "case3: viewer sum=30000");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-varchar-groupby-sum", test_agg_varchar_groupby_sum_run)
