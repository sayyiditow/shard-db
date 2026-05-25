/* src/test/cases/test_length_ops.c
 * Port of tests/test-length-ops.sh — len_eq/neq/lt/gt/lte/gte/between
 * filters on varchar fields, both indexed (btree-walk) and non-indexed.
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


static int do_count(TestClient *tc, const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"lent\","
        "\"criteria\":%s}", crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_length_ops_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"lent\","
        "\"fields\":[\"name:varchar:64\",\"bio:varchar:200\"],"
        "\"indexes\":[\"name\"],\"splits\":16}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"lent\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"name\":\"a\",\"bio\":\"\"}},"
                     "{\"key\":\"k2\",\"value\":{\"name\":\"bob\",\"bio\":\"x\"}},"
                     "{\"key\":\"k3\",\"value\":{\"name\":\"alice\",\"bio\":\"vip\"}},"
                     "{\"key\":\"k4\",\"value\":{\"name\":\"carol\",\"bio\":\"vip\"}},"
                     "{\"key\":\"k5\",\"value\":{\"name\":\"elizabeth\",\"bio\":\"lengthier note\"}},"
                     "{\"key\":\"k6\",\"value\":{\"name\":\"longusername123\",\"bio\":\"...\"}}]}",
        &resp); free(resp); resp = NULL;

    /* Indexed name len_* */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"len_eq\",\"value\":\"5\"}]"), 2, "len_eq 5 → 2");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"len_eq\",\"value\":\"1\"}]"), 1, "len_eq 1 → 1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"len_eq\",\"value\":\"7\"}]"), 0, "len_eq 7 → 0");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"len_neq\",\"value\":\"5\"}]"), 4, "len_neq 5 → 4");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"len_lt\",\"value\":\"5\"}]"), 2, "len_lt 5 → 2");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"len_lte\",\"value\":\"5\"}]"), 4, "len_lte 5 → 4");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"len_gt\",\"value\":\"5\"}]"), 2, "len_gt 5 → 2");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"len_gte\",\"value\":\"9\"}]"), 2, "len_gte 9 → 2");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"len_between\",\"value\":\"3\",\"value2\":\"5\"}]"), 3, "len_between 3,5 → 3");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"len_between\",\"value\":\"1\",\"value2\":\"1\"}]"), 1, "len_between 1,1 → 1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"len_between\",\"value\":\"100\",\"value2\":\"200\"}]"), 0, "len_between 100,200 → 0");

    /* Find returns right rows */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"lent\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"len_eq\",\"value\":\"5\"}],"
        "\"fields\":[\"name\"],\"order_by\":\"name\",\"order\":\"asc\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"alice\"", "find len_eq 5 has alice");
    ASSERT_CONTAINS(resp, "\"name\":\"carol\"", "find len_eq 5 has carol");
    ASSERT_TRUE(strstr(resp, "\"name\":\"bob\"") == NULL, "find len_eq 5 didn't leak bob");
    free(resp); resp = NULL;

    /* Non-indexed bio len */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"bio\",\"op\":\"len_eq\",\"value\":\"0\"}]"), 1, "bio len_eq 0 → 1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"bio\",\"op\":\"len_lt\",\"value\":\"3\"}]"), 2, "bio len_lt 3 → 2");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"bio\",\"op\":\"len_eq\",\"value\":\"3\"}]"), 3, "bio len_eq 3 → 3");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"bio\",\"op\":\"len_gt\",\"value\":\"5\"}]"), 1, "bio len_gt 5 → 1");

    /* Aggregate */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"lent\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"criteria\":[{\"field\":\"name\",\"op\":\"len_gte\",\"value\":\"5\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"n\":4", "agg(count) name len_gte 5 → 4");
    free(resp); resp = NULL;

    /* Combined criteria */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"name\",\"op\":\"starts\",\"value\":\"a\"},"
         "{\"field\":\"name\",\"op\":\"len_lt\",\"value\":\"5\"}]"),
        1, "starts a AND len_lt 5 → 1");

    /* Non-existent field: post-2026-05-25 returns a loud error instead of
       a silent 0 (which used to mean "0 matches" but was indistinguishable
       from a genuine 0-result filter). */
    {
        char *resp = NULL;
        tc_request(tc,
            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"lent\","
            "\"criteria\":[{\"field\":\"missing_field\",\"op\":\"len_eq\",\"value\":\"5\"}]}",
            &resp);
        ASSERT_TRUE(resp && strstr(resp, "unknown field") != NULL,
                    "len_eq on unknown field → error");
        free(resp);
    }

    /* bulk-update with len criteria */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"lent\","
        "\"criteria\":[{\"field\":\"bio\",\"op\":\"len_eq\",\"value\":\"3\"}],"
        "\"value\":{\"bio\":\"VIP\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"lent\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"bio\":\"VIP\"", "k3 bio after bulk-update with len");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-length-ops", test_length_ops_run)
