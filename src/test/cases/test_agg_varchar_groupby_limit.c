/* src/test/cases/test_agg_varchar_groupby_limit.c
 *
 * High-cardinality varchar group_by + AGG_COUNT + limit should not have to
 * build a full N-bucket hash table just to truncate to `limit` rows.
 * When the group field is an indexed varchar and there's no criteria /
 * order_by / having, the planner can walk the field's btree leaves in
 * sorted order, emit (key, count) on key change, and stop after `limit`
 * distinct buckets have been emitted.
 *
 * This test pins the contract:
 *   - all-distinct varchar group_by + count: limit rows returned, each count=1
 *   - duplicates correctly summed into one bucket per distinct key
 *   - limit > distinct count: emit only distinct count, no error
 *
 * Correctness is the load-bearing thing here — perf is measured via
 * bench-queries. This test guards against future refactors regressing the
 * "first N distinct keys" semantic.
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

static int test_agg_varchar_groupby_limit_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* === Case 1: all-distinct usernames (high-card stress) === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u_distinct\","
        "\"fields\":[\"username:varchar:16\"],"
        "\"indexes\":[\"username\"],\"splits\":8}", &resp);
    free(resp); resp = NULL;

    /* 500 distinct usernames. Each appears once → count=1 per group. */
    size_t cap = 500 * 96 + 256;
    char *payload = malloc(cap);
    ASSERT_NOT_NULL(payload, "alloc payload");
    if (!payload) { tc_close(tc); test_env_stop(&env); return 1; }
    size_t len = 0;
    SB_APPEND(payload, len, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"u_distinct\",\"records\":[");
    for (int i = 1; i <= 500; i++) {
        SB_APPEND(payload, len, cap,
            "%s{\"key\":\"k%d\",\"value\":{\"username\":\"user_%04d\"}}",
            i == 1 ? "" : ",", i, i);
    }
    SB_APPEND(payload, len, cap, "]}");
    tc_request(tc, payload, &resp); free(resp); resp = NULL;
    free(payload);

    /* limit=10: expect exactly 10 buckets, each with n=1. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"u_distinct\","
        "\"group_by\":[\"username\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"limit\":10}", &resp);
    ASSERT_EQ_INT(count_braces(resp), 10, "case1: limit=10 → 10 buckets");
    /* Every count is 1 since usernames are distinct. The shape uses
       string "1" in JSON, look for ":1" tied to alias "n". */
    int n_ones = 0;
    for (const char *p = resp; (p = strstr(p, "\"n\":1")) != NULL; p++) n_ones++;
    ASSERT_EQ_INT(n_ones, 10, "case1: all 10 buckets have n=1");
    free(resp); resp = NULL;

    /* limit=10 with CSV format — same shape, 10 data rows + 1 header. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"u_distinct\","
        "\"group_by\":[\"username\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"limit\":10,\"format\":\"csv\"}", &resp);
    int rows = 0;
    for (const char *p = resp; (p = strchr(p, '\n')) != NULL; p++) rows++;
    ASSERT_EQ_INT(rows, 11, "case1 csv: 1 header + 10 data rows");
    free(resp); resp = NULL;

    /* === Case 2: duplicates — must correctly sum within a varchar bucket === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u_dup\","
        "\"fields\":[\"role:varchar:16\"],"
        "\"indexes\":[\"role\"],\"splits\":8}", &resp);
    free(resp); resp = NULL;

    /* 300 records across 3 roles: 100× admin, 100× editor, 100× viewer. */
    cap = 300 * 96 + 256;
    payload = malloc(cap);
    ASSERT_NOT_NULL(payload, "alloc payload 2");
    if (!payload) { tc_close(tc); test_env_stop(&env); return 1; }
    len = 0;
    SB_APPEND(payload, len, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"u_dup\",\"records\":[");
    const char *roles[3] = {"admin", "editor", "viewer"};
    for (int i = 1; i <= 300; i++) {
        SB_APPEND(payload, len, cap,
            "%s{\"key\":\"k%d\",\"value\":{\"role\":\"%s\"}}",
            i == 1 ? "" : ",", i, roles[(i - 1) % 3]);
    }
    SB_APPEND(payload, len, cap, "]}");
    tc_request(tc, payload, &resp); free(resp); resp = NULL;
    free(payload);

    /* limit=2: expect 2 buckets (some 2 of the 3 roles), each with count=100. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"u_dup\","
        "\"group_by\":[\"role\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"limit\":2}", &resp);
    ASSERT_EQ_INT(count_braces(resp), 2, "case2: limit=2 → 2 buckets");
    /* The 2 emitted buckets must each have n=100 (full count, not truncated). */
    int n_hundreds = 0;
    for (const char *p = resp; (p = strstr(p, "\"n\":100")) != NULL; p++) n_hundreds++;
    ASSERT_EQ_INT(n_hundreds, 2, "case2: each bucket count=100 (full count)");
    free(resp); resp = NULL;

    /* === Case 3: limit > distinct count — emit all distinct, no error === */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"u_dup\","
        "\"group_by\":[\"role\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"limit\":100}", &resp);
    ASSERT_EQ_INT(count_braces(resp), 3, "case3: limit > distinct → 3 buckets");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-varchar-groupby-limit", test_agg_varchar_groupby_limit_run)
