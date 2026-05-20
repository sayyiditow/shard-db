/* src/test/cases/test_binary_index.c
 * Port of tests/test-binary-index.sh — correctness of binary-native B+
 * tree keys (Path B): signed-int, numeric, date ranges across zero;
 * varchar eq/prefix; composite index regression; reindex three forms.
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


static int do_count(TestClient *tc, const char *obj, const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":%s}", obj, crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_binary_index_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;

    /* SIGNED INT RANGE — single + bulk write paths. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bi_int\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"n:int\"],\"indexes\":[\"n\"]}",
        &resp); free(resp); resp = NULL;

    long ints[] = { -2147483647L, -1000000L, -1L, 0L, 1L, 1000000L, 2147483647L };
    for (int i = 0; i < 7; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bi_int\","
            "\"key\":\"k_%ld\",\"value\":{\"n\":%ld}}", ints[i], ints[i]);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bi_int_bulk\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"n:int\"],\"indexes\":[\"n\"]}",
        &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bi_int_bulk\","
        "\"records\":["
        "{\"key\":\"b_min\",\"value\":{\"n\":-2147483647}},"
        "{\"key\":\"b_-1m\",\"value\":{\"n\":-1000000}},"
        "{\"key\":\"b_-1\",\"value\":{\"n\":-1}},"
        "{\"key\":\"b_0\",\"value\":{\"n\":0}},"
        "{\"key\":\"b_1\",\"value\":{\"n\":1}},"
        "{\"key\":\"b_1m\",\"value\":{\"n\":1000000}},"
        "{\"key\":\"b_max\",\"value\":{\"n\":2147483647}}]}",
        &resp); free(resp); resp = NULL;

    /* Bulk-path correctness. */
    ASSERT_EQ_INT(do_count(tc, "bi_int_bulk", "[{\"field\":\"n\",\"op\":\"eq\",\"value\":\"-1\"}]"),
                  1, "bulk-insert: eq -1 → 1");
    ASSERT_EQ_INT(do_count(tc, "bi_int_bulk", "[{\"field\":\"n\",\"op\":\"lt\",\"value\":\"0\"}]"),
                  3, "bulk-insert: lt 0 → 3 negatives");
    ASSERT_EQ_INT(do_count(tc, "bi_int_bulk", "[{\"field\":\"n\",\"op\":\"gte\",\"value\":\"0\"}]"),
                  4, "bulk-insert: gte 0 → 4 non-negatives");

    /* Single-path correctness — ranges across zero. */
    ASSERT_EQ_INT(do_count(tc, "bi_int", "[{\"field\":\"n\",\"op\":\"eq\",\"value\":\"-2147483647\"}]"),
                  1, "eq MIN_INT");
    ASSERT_EQ_INT(do_count(tc, "bi_int", "[{\"field\":\"n\",\"op\":\"eq\",\"value\":\"2147483647\"}]"),
                  1, "eq MAX_INT");
    ASSERT_EQ_INT(do_count(tc, "bi_int", "[{\"field\":\"n\",\"op\":\"lt\",\"value\":\"0\"}]"),
                  3, "lt 0 → 3 negatives");
    ASSERT_EQ_INT(do_count(tc, "bi_int", "[{\"field\":\"n\",\"op\":\"gte\",\"value\":\"0\"}]"),
                  4, "gte 0 → 4 non-negatives");
    ASSERT_EQ_INT(do_count(tc, "bi_int",
        "[{\"field\":\"n\",\"op\":\"between\",\"value\":\"-1000\",\"value2\":\"1000\"}]"),
                  3, "between -1000 and 1000 = 3");
    ASSERT_EQ_INT(do_count(tc, "bi_int", "[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"-2\"}]"),
                  5, "gt -2 → 5");

    /* NUMERIC RANGE. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bi_num\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"amt:numeric:10,2\"],\"indexes\":[\"amt\"]}",
        &resp); free(resp); resp = NULL;
    const char *nums[] = { "-999.99", "-0.01", "0", "0.01", "999.99" };
    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bi_num\","
            "\"key\":\"n_%d\",\"value\":{\"amt\":%s}}", i, nums[i]);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(do_count(tc, "bi_num", "[{\"field\":\"amt\",\"op\":\"lt\",\"value\":\"0\"}]"),
                  2, "numeric lt 0 → 2 negatives");
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"between\",\"value\":\"-1\",\"value2\":\"1\"}]"),
                  3, "numeric between -1 and 1 = 3");

    /* DATE RANGE. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bi_date\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"d:date\"],\"indexes\":[\"d\"]}",
        &resp); free(resp); resp = NULL;
    const char *dates[] = { "20200101", "20250601", "20300101" };
    for (int i = 0; i < 3; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bi_date\","
            "\"key\":\"d_%s\",\"value\":{\"d\":\"%s\"}}", dates[i], dates[i]);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(do_count(tc, "bi_date",
        "[{\"field\":\"d\",\"op\":\"between\",\"value\":\"20220101\",\"value2\":\"20270101\"}]"),
                  1, "date between 2022 and 2027 = 1");
    ASSERT_EQ_INT(do_count(tc, "bi_date", "[{\"field\":\"d\",\"op\":\"gte\",\"value\":\"20260101\"}]"),
                  1, "date gte 2026 = 1");

    /* VARCHAR EQ / PREFIX. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bi_varchar\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"s:varchar:32\"],\"indexes\":[\"s\"]}",
        &resp); free(resp); resp = NULL;
    const char *strs[] = { "alpha", "alpine", "beta", "gamma" };
    for (int i = 0; i < 4; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bi_varchar\","
            "\"key\":\"v_%s\",\"value\":{\"s\":\"%s\"}}", strs[i], strs[i]);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(do_count(tc, "bi_varchar", "[{\"field\":\"s\",\"op\":\"eq\",\"value\":\"alpha\"}]"),
                  1, "varchar eq alpha = 1");
    ASSERT_EQ_INT(do_count(tc, "bi_varchar", "[{\"field\":\"s\",\"op\":\"starts\",\"value\":\"alp\"}]"),
                  2, "varchar starts alp = 2");

    /* COMPOSITE INDEX — ASCII path regression. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bi_comp\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"status:varchar:16\",\"region:varchar:16\"],"
        "\"indexes\":[\"status+region\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bi_comp\","
                   "\"key\":\"c1\",\"value\":{\"status\":\"paid\",\"region\":\"us\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bi_comp\","
                   "\"key\":\"c2\",\"value\":{\"status\":\"paid\",\"region\":\"eu\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bi_comp\","
                   "\"key\":\"c3\",\"value\":{\"status\":\"pending\",\"region\":\"us\"}}", &resp); free(resp); resp = NULL;
    ASSERT_EQ_INT(do_count(tc, "bi_comp", "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"),
                  2, "composite: status=paid full-scan + matches 2");

    /* REINDEX three forms. */
    tc_request(tc, "{\"mode\":\"reindex\",\"dir\":\"default\",\"object\":\"bi_int\"}", &resp);
    ASSERT_CONTAINS(resp, "\"indexes\":1", "reindex single object");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"reindex\",\"dir\":\"default\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"reindexed\"", "reindex tenant reports success");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"reindex\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"reindexed\"", "reindex all reports success");
    free(resp); resp = NULL;

    /* Post-reindex spot check. */
    ASSERT_EQ_INT(do_count(tc, "bi_int", "[{\"field\":\"n\",\"op\":\"lt\",\"value\":\"0\"}]"),
                  3, "post-reindex: lt 0 still 3");
    ASSERT_EQ_INT(do_count(tc, "bi_varchar", "[{\"field\":\"s\",\"op\":\"starts\",\"value\":\"alp\"}]"),
                  2, "post-reindex: varchar prefix still 2");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-binary-index", test_binary_index_run)
