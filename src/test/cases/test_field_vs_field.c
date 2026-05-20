/* src/test/cases/test_field_vs_field.c
 * Port of tests/test-field-vs-field.sh — eq_field/neq_field/lt_field/
 * gt_field/lte_field/gte_field. Compares LHS field against another
 * named field on the same record. Always full-scan (planner returns
 * leaf_is_indexed=0). Type-mismatched compare → silent zero.
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


static int do_count(TestClient *tc, const char *crit) {
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"fft\","
        "\"criteria\":%s}", crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_field_vs_field_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"fft\","
        "\"fields\":[\"received_at:int\",\"scheduled_at:int\","
                    "\"name:varchar:32\",\"alias:varchar:32\","
                    "\"amount:numeric:10,2\",\"budget:numeric:10,2\","
                    "\"day_a:date\",\"day_b:date\"],"
        "\"indexes\":[\"name\"]}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"fft\","
        "\"records\":["
        "{\"key\":\"k1\",\"value\":{\"received_at\":100,\"scheduled_at\":200,"
            "\"name\":\"alice\",\"alias\":\"alice\","
            "\"amount\":\"50.00\",\"budget\":\"100.00\","
            "\"day_a\":\"20260101\",\"day_b\":\"20260201\"}},"
        "{\"key\":\"k2\",\"value\":{\"received_at\":300,\"scheduled_at\":200,"
            "\"name\":\"bob\",\"alias\":\"BOB\","
            "\"amount\":\"100.00\",\"budget\":\"100.00\","
            "\"day_a\":\"20260301\",\"day_b\":\"20260301\"}},"
        "{\"key\":\"k3\",\"value\":{\"received_at\":150,\"scheduled_at\":150,"
            "\"name\":\"carol\",\"alias\":\"carol\","
            "\"amount\":\"75.00\",\"budget\":\"50.00\","
            "\"day_a\":\"20260601\",\"day_b\":\"20260101\"}}]}",
        &resp); free(resp); resp = NULL;

    /* int eq/neq/lt/gt/lte/gte_field on received_at vs scheduled_at:
       k1 100 vs 200 → lt
       k2 300 vs 200 → gt
       k3 150 vs 150 → eq */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"received_at\",\"op\":\"eq_field\",\"value\":\"scheduled_at\"}]"),
                  1, "received_at eq_field scheduled_at → 1 (k3)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"received_at\",\"op\":\"neq_field\",\"value\":\"scheduled_at\"}]"),
                  2, "received_at neq_field scheduled_at → 2 (k1,k2)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"received_at\",\"op\":\"lt_field\",\"value\":\"scheduled_at\"}]"),
                  1, "received_at lt_field scheduled_at → 1 (k1)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"received_at\",\"op\":\"gt_field\",\"value\":\"scheduled_at\"}]"),
                  1, "received_at gt_field scheduled_at → 1 (k2)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"received_at\",\"op\":\"lte_field\",\"value\":\"scheduled_at\"}]"),
                  2, "received_at lte_field scheduled_at → 2 (k1,k3)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"received_at\",\"op\":\"gte_field\",\"value\":\"scheduled_at\"}]"),
                  2, "received_at gte_field scheduled_at → 2 (k2,k3)");

    /* varchar eq_field is byte-exact (CS).
       k1: alice/alice → match. k2: bob/BOB → mismatch. k3: carol/carol → match. */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"eq_field\",\"value\":\"alias\"}]"),
                  2, "name eq_field alias (CS) → 2 (k1,k3)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"neq_field\",\"value\":\"alias\"}]"),
                  1, "name neq_field alias → 1 (k2)");

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fft\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"neq_field\",\"value\":\"alias\"}],"
        "\"fields\":[\"name\",\"alias\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k2\"", "find name neq_field alias returns k2");
    free(resp); resp = NULL;

    /* varchar lex compare via lt/gt_field.
       k1 alice/alice → eq. k2 bob/BOB → 0x62>0x42 so name>alias. k3 carol/carol → eq. */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"lt_field\",\"value\":\"alias\"}]"),
                  0, "name lt_field alias → 0");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"gt_field\",\"value\":\"alias\"}]"),
                  1, "name gt_field alias → 1 (k2 bob>BOB)");

    /* numeric (decimal fixed-point) field-vs-field.
       k1 50/100 → lt; k2 100/100 → eq; k3 75/50 → gt. */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"eq_field\",\"value\":\"budget\"}]"),
                  1, "amount eq_field budget → 1 (k2)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"lt_field\",\"value\":\"budget\"}]"),
                  1, "amount lt_field budget → 1 (k1)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"gt_field\",\"value\":\"budget\"}]"),
                  1, "amount gt_field budget → 1 (k3)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"gte_field\",\"value\":\"budget\"}]"),
                  2, "amount gte_field budget → 2 (k2,k3)");

    /* date field-vs-field.
       k1 0101/0201 → lt; k2 0301/0301 → eq; k3 0601/0101 → gt. */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"day_a\",\"op\":\"eq_field\",\"value\":\"day_b\"}]"),
                  1, "day_a eq_field day_b → 1 (k2)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"day_a\",\"op\":\"lt_field\",\"value\":\"day_b\"}]"),
                  1, "day_a lt_field day_b → 1 (k1)");

    /* Type mismatch silently matches nothing. */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"eq_field\",\"value\":\"received_at\"}]"),
                  0, "name eq_field received_at → 0 (varchar vs int)");
    /* Unknown RHS field. */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"eq_field\",\"value\":\"missing_field\"}]"),
                  0, "name eq_field missing_field → 0");

    /* Combined with regular ops via AND.
       received_at lt_field scheduled_at AND received_at gt 50 → k1 (100<200 and 100>50). */
    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"received_at\",\"op\":\"lt_field\",\"value\":\"scheduled_at\"},"
         "{\"field\":\"received_at\",\"op\":\"gt\",\"value\":\"50\"}]"),
        1, "lt_field AND gt 50 → 1 (k1)");

    /* aggregate respects field-vs-field. count=2, sum(received_at)=100+150=250. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"fft\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},"
                        "{\"fn\":\"sum\",\"field\":\"received_at\",\"alias\":\"s\"}],"
        "\"criteria\":[{\"field\":\"received_at\",\"op\":\"lte_field\",\"value\":\"scheduled_at\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"n\":2", "agg count where lte_field");
    ASSERT_CONTAINS(resp, "\"s\":250", "agg sum where lte_field = 250");
    free(resp); resp = NULL;

    /* bulk-update with field-vs-field criteria — only k3 (amount>budget). */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"fft\","
        "\"criteria\":[{\"field\":\"amount\",\"op\":\"gt_field\",\"value\":\"budget\"}],"
        "\"value\":{\"name\":\"OVERSPEND\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"fft\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"OVERSPEND\"", "k3 name updated to OVERSPEND");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"fft\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"alice\"", "k1 name unchanged");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-field-vs-field", test_field_vs_field_run)
