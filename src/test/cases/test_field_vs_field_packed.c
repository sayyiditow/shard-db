/* Packed copy of test_field_vs_field.c (class K original stays legacy until
 * T3). The original's criteria `bulk-update` block is dropped: packed objects
 * refuse criteria updates (AUD-PACKED-BULKUPDATE).
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
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"fftp\","
        "\"criteria\":%s}", crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_field_vs_field_packed_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"declaration_layout\":\"packed\",\"dir\":\"default\",\"object\":\"fftp\","
        "\"fields\":[\"received_at:int\",\"scheduled_at:int\","
                    "\"name:varchar:32\",\"alias:varchar:32\","
                    "\"amount:numeric:10,2\",\"budget:numeric:10,2\","
                    "\"day_a:date\",\"day_b:date\"],"
        "\"indexes\":[\"name\"]}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"fftp\","
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

    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"received_at\",\"op\":\"eq_field\",\"value\":\"scheduled_at\"}]"),
                  1, "received_at eq_field scheduled_at -> 1 (k3)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"received_at\",\"op\":\"neq_field\",\"value\":\"scheduled_at\"}]"),
                  2, "received_at neq_field scheduled_at -> 2 (k1,k2)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"received_at\",\"op\":\"lt_field\",\"value\":\"scheduled_at\"}]"),
                  1, "received_at lt_field scheduled_at -> 1 (k1)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"received_at\",\"op\":\"gt_field\",\"value\":\"scheduled_at\"}]"),
                  1, "received_at gt_field scheduled_at -> 1 (k2)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"received_at\",\"op\":\"lte_field\",\"value\":\"scheduled_at\"}]"),
                  2, "received_at lte_field scheduled_at -> 2 (k1,k3)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"received_at\",\"op\":\"gte_field\",\"value\":\"scheduled_at\"}]"),
                  2, "received_at gte_field scheduled_at -> 2 (k2,k3)");

    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"eq_field\",\"value\":\"alias\"}]"),
                  2, "name eq_field alias (CS) -> 2 (k1,k3)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"neq_field\",\"value\":\"alias\"}]"),
                  1, "name neq_field alias -> 1 (k2)");

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fftp\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"neq_field\",\"value\":\"alias\"}],"
        "\"fields\":[\"name\",\"alias\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k2\"", "find name neq_field alias returns k2");
    free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"lt_field\",\"value\":\"alias\"}]"),
                  0, "name lt_field alias -> 0");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"gt_field\",\"value\":\"alias\"}]"),
                  1, "name gt_field alias -> 1 (k2 bob>BOB)");

    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"eq_field\",\"value\":\"budget\"}]"),
                  1, "amount eq_field budget -> 1 (k2)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"lt_field\",\"value\":\"budget\"}]"),
                  1, "amount lt_field budget -> 1 (k1)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"gt_field\",\"value\":\"budget\"}]"),
                  1, "amount gt_field budget -> 1 (k3)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"gte_field\",\"value\":\"budget\"}]"),
                  2, "amount gte_field budget -> 2 (k2,k3)");

    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"day_a\",\"op\":\"eq_field\",\"value\":\"day_b\"}]"),
                  1, "day_a eq_field day_b -> 1 (k2)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"day_a\",\"op\":\"lt_field\",\"value\":\"day_b\"}]"),
                  1, "day_a lt_field day_b -> 1 (k1)");

    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"eq_field\",\"value\":\"received_at\"}]"),
                  0, "name eq_field received_at -> 0 (varchar vs int)");
    {
        char *r2 = NULL;
        tc_request(tc,
            "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"fftp\","
            "\"criteria\":[{\"field\":\"name\",\"op\":\"eq_field\",\"value\":\"missing_field\"}]}",
            &r2);
        ASSERT_TRUE(r2 && SAFE_STRSTR(r2, "unknown field") != NULL,
                    "name eq_field missing_field -> error");
        free(r2);
    }

    ASSERT_EQ_INT(do_count(tc,
        "[{\"field\":\"received_at\",\"op\":\"lt_field\",\"value\":\"scheduled_at\"},"
         "{\"field\":\"received_at\",\"op\":\"gt\",\"value\":\"50\"}]"),
        1, "lt_field AND gt 50 -> 1 (k1)");

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"fftp\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},"
                        "{\"fn\":\"sum\",\"field\":\"received_at\",\"alias\":\"s\"}],"
        "\"criteria\":[{\"field\":\"received_at\",\"op\":\"lte_field\",\"value\":\"scheduled_at\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"n\":2", "agg count where lte_field");
    ASSERT_CONTAINS(resp, "\"s\":250", "agg sum where lte_field = 250");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-field-vs-field-packed", test_field_vs_field_packed_run)
