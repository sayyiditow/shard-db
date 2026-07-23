/* src/test/cases/test_agg_neq_shortcut.c
 * Port of tests/test-agg-neq-shortcut.sh — algebraic shortcut for
 * aggregate(NEQ X) on indexed field with COUNT/SUM/AVG specs:
 *   agg(neq X) = agg(*) - agg(eq X).
 * Verifies correctness vs the existing path, and that the shortcut
 * bails for MIN/MAX, group_by, having, and non-indexed fields.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"   /* SB_APPEND — safe StringBuilder vs CodeQL snprintf-overflow flag */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Extract `"<key>":<number>` (int or float) into out. Returns 1 if found. */
static int extract_field(const char *resp, const char *key, char *out, size_t out_sz) {
    if (!resp || !key) return 0;
    char needle[64]; snprintf(needle, sizeof(needle), "\"%s\":", key);
    const char *p = SAFE_STRSTR(resp, needle);
    if (!p) return 0;
    p += strlen(needle);
    size_t i = 0;
    if (*p == '-' && i + 1 < out_sz) out[i++] = *p++;
    while (i + 1 < out_sz && (*p == '.' || (*p >= '0' && *p <= '9'))) out[i++] = *p++;
    out[i] = '\0';
    return i > 0;
}

static int test_agg_neq_shortcut_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"fields\":[\"status:varchar:16\",\"region:varchar:8\",\"amount:int\"],"
        "\"indexes\":[\"status\"],\"splits\":16}", &resp); free(resp); resp = NULL;

    /* Seed 100 rows: paid iff i%5==0, region us iff odd, amount=i.
       SB_APPEND prevents the `len += snprintf(...)` overflow CodeQL
       flags — snprintf returns the would-have-written length, so on
       truncation len could advance past cap and underflow cap-len. */
    size_t cap = 100 * 96 + 256;
    char *payload = malloc(cap);
    if (!payload) { tc_close(tc); test_env_stop(&env); return 1; }
    size_t len = 0;
    SB_APPEND(payload, len, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"neq_t\",\"records\":[");
    for (int i = 1; i <= 100; i++) {
        const char *s = (i % 5 == 0) ? "paid" : "pending";
        const char *r = (i % 2 == 1) ? "us" : "eu";
        SB_APPEND(payload, len, cap,
            "%s{\"key\":\"k%d\",\"value\":{\"status\":\"%s\",\"region\":\"%s\",\"amount\":%d}}",
            (i == 1) ? "" : ",", i, s, r, i);
    }
    SB_APPEND(payload, len, cap, "]}");
    tc_request(tc, payload, &resp); free(resp); resp = NULL;
    free(payload);

    char buf[64];

    /* count(neq paid) — shortcut path. Expect 80. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"neq\",\"value\":\"paid\"}]}",
        &resp);
    ASSERT_TRUE(extract_field(resp, "n", buf, sizeof(buf)), "count(neq) returned n");
    ASSERT_TRUE(strcmp(buf, "80") == 0, "count(neq paid)=80");
    free(resp); resp = NULL;

    /* sum(amount) where neq paid — shortcut. Expect 4000. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"s\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"neq\",\"value\":\"paid\"}]}",
        &resp);
    ASSERT_TRUE(extract_field(resp, "s", buf, sizeof(buf)) && strcmp(buf, "4000") == 0,
                "sum(amount neq paid)=4000");
    free(resp); resp = NULL;

    /* avg(amount) where neq paid — shortcut. Expect 50 (or 50.0). */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"avg\",\"field\":\"amount\",\"alias\":\"a\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"neq\",\"value\":\"paid\"}]}",
        &resp);
    ASSERT_TRUE(extract_field(resp, "a", buf, sizeof(buf)), "avg returned");
    ASSERT_TRUE(strcmp(buf, "50") == 0 || strcmp(buf, "50.0") == 0 ||
                strcmp(buf, "50.000000") == 0, "avg=50");
    free(resp); resp = NULL;

    /* Combined COUNT+SUM+AVG — shortcut. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},"
                        "{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"s\"},"
                        "{\"fn\":\"avg\",\"field\":\"amount\",\"alias\":\"a\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"neq\",\"value\":\"paid\"}]}",
        &resp);
    ASSERT_TRUE(extract_field(resp, "n", buf, sizeof(buf)) && strcmp(buf, "80") == 0,
                "combined count=80");
    ASSERT_TRUE(extract_field(resp, "s", buf, sizeof(buf)) && strcmp(buf, "4000") == 0,
                "combined sum=4000");
    free(resp); resp = NULL;

    /* Correctness vs eq-path: total - eq_paid == neq_paid. */
    int total = 0, eq_paid = 0, neq_paid = 0;
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}", &resp);
    if (extract_field(resp, "n", buf, sizeof(buf))) total = atoi(buf);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}",
        &resp);
    if (extract_field(resp, "n", buf, sizeof(buf))) eq_paid = atoi(buf);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"neq\",\"value\":\"paid\"}]}",
        &resp);
    if (extract_field(resp, "n", buf, sizeof(buf))) neq_paid = atoi(buf);
    free(resp); resp = NULL;
    ASSERT_EQ_INT(total - eq_paid, neq_paid, "100 - eq_paid == neq_paid");

    /* Shortcut bails for MIN — must fall through. min(amount neq paid) = 1. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"min\",\"field\":\"amount\",\"alias\":\"mn\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"neq\",\"value\":\"paid\"}]}",
        &resp);
    ASSERT_TRUE(extract_field(resp, "mn", buf, sizeof(buf)) && strcmp(buf, "1") == 0,
                "min(amount neq paid)=1");
    free(resp); resp = NULL;

    /* Shortcut bails for MAX. max(amount neq paid) = 99. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"max\",\"field\":\"amount\",\"alias\":\"mx\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"neq\",\"value\":\"paid\"}]}",
        &resp);
    ASSERT_TRUE(extract_field(resp, "mx", buf, sizeof(buf)) && strcmp(buf, "99") == 0,
                "max(amount neq paid)=99");
    free(resp); resp = NULL;

    /* Mixed COUNT + MIN — bails for MIN, falls through. count=80, min=1. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},"
                        "{\"fn\":\"min\",\"field\":\"amount\",\"alias\":\"mn\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"neq\",\"value\":\"paid\"}]}",
        &resp);
    ASSERT_TRUE(extract_field(resp, "n", buf, sizeof(buf)) && strcmp(buf, "80") == 0,
                "mixed count=80");
    ASSERT_TRUE(extract_field(resp, "mn", buf, sizeof(buf)) && strcmp(buf, "1") == 0,
                "mixed min=1");
    free(resp); resp = NULL;

    /* Shortcut bails for non-indexed field. count(region neq us)=50. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"criteria\":[{\"field\":\"region\",\"op\":\"neq\",\"value\":\"us\"}]}",
        &resp);
    ASSERT_TRUE(extract_field(resp, "n", buf, sizeof(buf)) && strcmp(buf, "50") == 0,
                "count(region neq us)=50");
    free(resp); resp = NULL;

    /* Shortcut bails for group_by. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"region\"],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"neq\",\"value\":\"paid\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"region\":\"us\"", "group_by has us bucket");
    ASSERT_CONTAINS(resp, "\"region\":\"eu\"", "group_by has eu bucket");
    ASSERT_CONTAINS(resp, "\"n\":40", "group_by yields 40 per bucket");
    free(resp); resp = NULL;

    /* Shortcut bails for having. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"status\"],"
        "\"having\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"50\"}],"
        "\"criteria\":[]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"pending\"", "having keeps pending");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"status\":\"paid\"") == NULL, "having drops paid");
    free(resp); resp = NULL;

    /* Empty criteria — shortcut not engaged, returns whole-set. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}", &resp);
    ASSERT_TRUE(extract_field(resp, "n", buf, sizeof(buf)) && strcmp(buf, "100") == 0,
                "no-criteria count=100");
    free(resp); resp = NULL;

    /* CSV format on shortcut path. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},"
                        "{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"s\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"neq\",\"value\":\"paid\"}],"
        "\"format\":\"csv\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "n,s") != NULL, "CSV header");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "80,4000") != NULL, "CSV data");
    free(resp); resp = NULL;

    /* NEQ value matches everything. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"neq\",\"value\":\"this_value_doesnt_exist\"}]}",
        &resp);
    ASSERT_TRUE(extract_field(resp, "n", buf, sizeof(buf)) && strcmp(buf, "100") == 0,
                "count(neq nonexistent)=100");
    free(resp); resp = NULL;

    /* Overwrite k1, k2 to paid → neq_paid drops 80 → 78. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"region\":\"us\",\"amount\":1}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"region\":\"us\",\"amount\":2}}]}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"neq_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"neq\",\"value\":\"paid\"}]}",
        &resp);
    ASSERT_TRUE(extract_field(resp, "n", buf, sizeof(buf)) && strcmp(buf, "78") == 0,
                "count(neq paid) after overwrite = 78");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-neq-shortcut", test_agg_neq_shortcut_run)
