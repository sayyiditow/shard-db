/* src/test/cases/test_bulk_upsert.c
 *
 * Port of tests/test-bulk-upsert.sh — bulk-insert acts as a true upsert:
 * overwriting an existing key updates the slot AND drops stale index
 * entries. Without that drop, idx_count_cb over-counts and AND-intersect
 * returns ghost candidates.
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

static int count_where(TestClient *tc, const char *criteria) {
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"upsert_t\","
        "\"criteria\":%s}", criteria);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_bulk_upsert_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"upsert_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\",\"note:varchar:32\"],"
        "\"indexes\":[\"status\",\"amount\"],\"splits\":16}", &resp);
    free(resp); resp = NULL;

    /* Seed 3 records. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"upsert_t\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":100,\"note\":\"v\"}},"
                     "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":200,\"note\":\"v\"}},"
                     "{\"key\":\"k3\",\"value\":{\"status\":\"pending\",\"amount\":50,\"note\":\"v\"}}]}",
        &resp);
    free(resp); resp = NULL;

    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"),
                  2, "count(status=paid)=2 after seed");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]"),
                  1, "count(status=pending)=1 after seed");

    /* Upsert: change indexed field — old idx entry must drop. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"upsert_t\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"refunded\",\"amount\":100,\"note\":\"v\"}}]}",
        &resp);
    free(resp); resp = NULL;

    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"),
                  1, "count(status=paid) drops 2->1");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"refunded\"}]"),
                  1, "count(status=refunded)=1");

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"upsert_t\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"fields\":[\"status\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k2\"", "find(paid) returns k2");
    ASSERT_TRUE(strstr(resp, "\"key\":\"k1\"") == NULL, "find(paid) does not return stale k1");
    free(resp); resp = NULL;

    /* No-op rewrite must not duplicate. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"upsert_t\","
        "\"records\":[{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":200,\"note\":\"v\"}}]}",
        &resp);
    free(resp); resp = NULL;

    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"),
                  1, "count(status=paid)=1 after no-op rewrite");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"200\"}]"),
                  1, "count(amount=200)=1 after no-op rewrite");

    /* Move k2 to (cancelled, 999) — both indexed fields drift. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"upsert_t\","
        "\"records\":[{\"key\":\"k2\",\"value\":{\"status\":\"cancelled\",\"amount\":999,\"note\":\"v\"}}]}",
        &resp);
    free(resp); resp = NULL;

    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"),
                  0, "count(paid)=0 after k2 moves");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"200\"}]"),
                  0, "count(amount=200)=0 after k2 moves");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"cancelled\"}]"),
                  1, "count(cancelled)=1");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"999\"}]"),
                  1, "count(amount=999)=1");

    /* if_not_exists skips. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"upsert_t\","
        "\"if_not_exists\":true,"
        "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":1,\"note\":\"v\"}}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"skipped\":1", "if_not_exists returns skipped:1");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"upsert_t\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"refunded\"", "k1 still refunded after CAS skip");
    free(resp); resp = NULL;

    /* AND-intersection (paid AND amount<100) must not include stale k4. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"upsert_t\","
        "\"records\":[{\"key\":\"k4\",\"value\":{\"status\":\"paid\",\"amount\":75,\"note\":\"v\"}}]}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"upsert_t\","
        "\"records\":[{\"key\":\"k4\",\"value\":{\"status\":\"refunded\",\"amount\":75,\"note\":\"v\"}}]}",
        &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                                  "{\"field\":\"amount\",\"op\":\"lt\",\"value\":\"100\"}]"),
                  0, "AND-intersect (paid AND amount<100)=0 (no stale k4)");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bulk-upsert", test_bulk_upsert_run)
