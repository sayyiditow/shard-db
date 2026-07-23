/* src/test/cases/test_and_intersection.c
 * Port of tests/test-and-intersection.sh — AND index intersection
 * (PRIMARY_INTERSECT planner path). Exercises count + find +
 * aggregate against pure AND trees over indexed leaves on
 * btree-rangeable ops. Negative cases verify fallback to
 * PRIMARY_LEAF / PRIMARY_KEYSET.
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



static int do_count(TestClient *tc, const char *crit) {
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ix_orders\","
        "\"criteria\":%s}", crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int count_keys(const char *resp) {
    if (!resp) return 0;
    int n = 0;
    const char *p = resp;
    while ((p = strstr(p, "\"key\":\"")) != NULL) { n++; p += 7; }
    return n;
}

static int test_and_intersection_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ix_orders\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"status:varchar:16\",\"amount:int\","
                    "\"region:varchar:16\",\"notes:varchar:64\"],"
        "\"indexes\":[\"status\",\"amount\",\"region\"]}", &resp); free(resp); resp = NULL;

    /* Build 200-record bulk-insert via single payload. SB_APPEND prevents
       the `off += snprintf(...)` overflow that CodeQL flags — snprintf's
       return value is the would-have-written length, so on truncation
       len advances past cap and subsequent writes underflow cap-len. */
    size_t cap = 200 * 256 + 256;
    char *payload = malloc(cap);
    if (!payload) { tc_close(tc); test_env_stop(&env); return 1; }
    size_t len = 0;
    SB_APPEND(payload, len, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ix_orders\",\"records\":[");
    for (int i = 1; i <= 200; i++) {
        const char *st = "paid"; int amt = 0; const char *region = "us";
        switch (i % 4) {
            case 0: st = "paid"; amt = 100 + i; region = "us"; break;
            case 1: st = "pending"; amt = 50 + i; region = "eu"; break;
            case 2: st = "paid"; amt = 200 + i; region = "eu"; break;
            case 3: st = "cancelled"; amt = 30 + i; region = "us"; break;
        }
        SB_APPEND(payload, len, cap,
            "%s{\"key\":\"k%d\",\"value\":{\"status\":\"%s\",\"amount\":%d,"
            "\"region\":\"%s\",\"notes\":\"order %d\"}}",
            (i == 1) ? "" : ",", i, st, amt, region, i);
    }
    SB_APPEND(payload, len, cap, "]}");
    tc_request(tc, payload, &resp); free(resp); resp = NULL;
    free(payload);

    /* COUNT 2-way EQ + EQ. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}]"),
        50, "count(paid AND us)");
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"eu\"}]"),
        50, "count(paid AND eu)");
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}]"),
        0, "count(pending AND us) — empty intersection");

    /* COUNT 2-way EQ + range. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"amount\",\"op\":\"gt\",\"value\":\"150\"}]"),
        88, "count(paid AND amount>150)");
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"amount\",\"op\":\"between\",\"value\":\"200\",\"value2\":\"250\"}]"),
        26, "count(paid AND amount between 200..250)");

    /* COUNT 3-way intersection. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"},"
        "{\"field\":\"amount\",\"op\":\"gt\",\"value\":\"150\"}]"),
        38, "count(paid AND us AND amount>150)");
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"eu\"},"
        "{\"field\":\"amount\",\"op\":\"lte\",\"value\":\"250\"}]"),
        13, "count(paid AND eu AND amount<=250)");

    /* COUNT EQ + IN. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"},"
        "{\"field\":\"status\",\"op\":\"in\",\"value\":\"paid,cancelled\"}]"),
        100, "count(us AND status IN (paid,cancelled))");

    /* COUNT EQ + STARTS_WITH. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"},"
        "{\"field\":\"status\",\"op\":\"starts\",\"value\":\"pa\"}]"),
        50, "count(us AND status starts 'pa')");

    /* FIND with limit applies to survivors. 50 paid+us; limit=10 → 10. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ix_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}],"
        "\"limit\":10}", &resp);
    ASSERT_EQ_INT(count_keys(resp), 10, "find limit=10 returns exactly 10");
    /* Sanity: every result has status=paid and region=us. */
    {
        int bad = 0;
        const char *p = resp ? resp : "";
        while ((p = strstr(p, "\"value\":")) != NULL) {
            const char *end = strchr(p, '}');
            if (!end) break;
            char chunk[256]; size_t n = (size_t)(end - p);
            if (n + 1 > sizeof(chunk)) n = sizeof(chunk) - 1;
            memcpy(chunk, p, n); chunk[n] = '\0';
            if (!strstr(chunk, "\"status\":\"paid\"") ||
                !strstr(chunk, "\"region\":\"us\"")) bad++;
            p = end + 1;
        }
        ASSERT_EQ_INT(bad, 0, "every find result matches both criteria");
    }
    free(resp); resp = NULL;

    /* offset 40, limit 50 → 10 remaining. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ix_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}],"
        "\"offset\":40,\"limit\":50}", &resp);
    ASSERT_EQ_INT(count_keys(resp), 10, "find offset=40 limit=50 returns 10");
    free(resp); resp = NULL;

    /* Empty intersection. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ix_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}],"
        "\"limit\":5}", &resp);
    /* Trim whitespace. */
    {
        const char *p = resp;
        while (p && (*p == ' ' || *p == '\n' || *p == '\t')) p++;
        ASSERT_TRUE(p && p[0] == '[' && p[1] == ']', "find empty intersection returns []");
    }
    free(resp); resp = NULL;

    /* FIND with field projection — only `amount` in value, not `status`. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"ix_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}],"
        "\"limit\":1,\"fields\":[\"amount\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"amount\":") != NULL, "projection has amount");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"status\":") == NULL, "projection excludes status");
    free(resp); resp = NULL;

    /* AGGREGATE sum + count: paid+us. sum=10100, count=50. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"ix_orders\","
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"total\"},"
                        "{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"total\":10100", "agg total=10100");
    ASSERT_CONTAINS(resp, "\"n\":50", "agg n=50");
    free(resp); resp = NULL;

    /* AGGREGATE min/max/avg. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"ix_orders\","
        "\"aggregates\":[{\"fn\":\"min\",\"field\":\"amount\",\"alias\":\"lo\"},"
                        "{\"fn\":\"max\",\"field\":\"amount\",\"alias\":\"hi\"},"
                        "{\"fn\":\"avg\",\"field\":\"amount\",\"alias\":\"avg\"}],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"lo\":104", "agg min=104");
    ASSERT_CONTAINS(resp, "\"hi\":300", "agg max=300");
    ASSERT_CONTAINS(resp, "\"avg\":202", "agg avg=202");
    free(resp); resp = NULL;

    /* AGGREGATE with group_by region: paid AND amount>150. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"ix_orders\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"region\"],"
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"amount\",\"op\":\"gt\",\"value\":\"150\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"region\":\"us\",\"n\":38", "group_by region us=38");
    ASSERT_CONTAINS(resp, "\"region\":\"eu\",\"n\":50", "group_by region eu=50");
    free(resp); resp = NULL;

    /* NEGATIVE: single leaf stays on PRIMARY_LEAF. */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]"),
                  50, "single leaf count(pending)");

    /* NEGATIVE: non-eligible op LIKE falls back. Just check we get a numeric. */
    {
        char *r = NULL;
        tc_request(tc,
            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"ix_orders\","
            "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
            "{\"field\":\"notes\",\"op\":\"like\",\"value\":\"%order 4%\"}]}",
            &r);
        const char *p = r;
        while (p && (*p == ' ' || *p == '\n' || *p == '\t')) p++;
        ASSERT_TRUE(p && *p >= '0' && *p <= '9',
                    "non-eligible LIKE falls back, returns numeric count");
        free(r);
    }

    /* NEGATIVE: mixed indexed + non-indexed. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"notes\",\"op\":\"eq\",\"value\":\"order 4\"}]"),
        1, "mixed indexed+non-indexed AND falls back");

    /* NEGATIVE: pure OR uses PRIMARY_KEYSET. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"},"
                  "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"cancelled\"}]}]"),
        100, "pure OR uses PRIMARY_KEYSET");

    /* EDGE: 4-way intersection. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"us\"},"
        "{\"field\":\"amount\",\"op\":\"gte\",\"value\":\"100\"},"
        "{\"field\":\"amount\",\"op\":\"lte\",\"value\":\"200\"}]"),
        25, "count 4-way intersection");

    /* EDGE: contradiction yields 0. */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"cancelled\"}]"),
        0, "contradiction yields 0");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-and-intersection", test_and_intersection_run)
