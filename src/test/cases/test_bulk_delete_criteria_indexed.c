/* test_bulk_delete_criteria_indexed.c — Test index-aware Phase 1 for bulk-delete-criteria.
 *
 * Exercises:
 *   - bulk-delete-criteria with indexed fields uses index to find matches
 *   - Deletes the correct number of records
 *   - Index entries are properly dropped post-delete
 *   - Subsequent finds and counts reflect the deletions
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
        "{\"mode\":\"count\",\"dir\":\"bdc_idx_dir\",\"object\":\"bdc_idx\","
        "\"criteria\":%s}", criteria);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int count_total(TestClient *tc) {
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"bdc_idx_dir\",\"object\":\"bdc_idx\"}", &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_bulk_delete_criteria_indexed_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"bdc_idx_dir\"}", &resp);
    free(resp); resp = NULL;

    /* Create object with splits=8, fields status:varchar:16, score:int */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"bdc_idx_dir\",\"object\":\"bdc_idx\","
        "\"splits\":8,\"max_key\":40,"
        "\"fields\":[\"status:varchar:16\",\"score:int\"],"
        "\"indexes\":[\"status\",\"score\"]}", &resp);
    free(resp); resp = NULL;

    /* Bulk-insert 500 records: 200 paid, 300 pending */
    char *bulk_insert = malloc(200000);
    int offset = 0;
    offset += snprintf(bulk_insert + offset, 200000 - offset,
        "{\"mode\":\"bulk-insert\",\"dir\":\"bdc_idx_dir\",\"object\":\"bdc_idx\","
        "\"records\":[");

    for (int i = 0; i < 200; i++) {
        if (i > 0) offset += snprintf(bulk_insert + offset, 200000 - offset, ",");
        offset += snprintf(bulk_insert + offset, 200000 - offset,
            "{\"key\":\"paid_%d\",\"value\":{\"status\":\"paid\",\"score\":%d}}",
            i, i % 100);
    }
    for (int i = 0; i < 300; i++) {
        offset += snprintf(bulk_insert + offset, 200000 - offset, ",");
        offset += snprintf(bulk_insert + offset, 200000 - offset,
            "{\"key\":\"pending_%d\",\"value\":{\"status\":\"pending\",\"score\":%d}}",
            i, i % 100);
    }
    offset += snprintf(bulk_insert + offset, 200000 - offset, "]}");

    tc_request(tc, bulk_insert, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":500", "bulk-insert: 500 inserted");
    free(resp); resp = NULL;
    free(bulk_insert);

    /* Verify initial state */
    ASSERT_EQ_INT(count_total(tc), 500, "total count = 500 after seed");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"),
                  200, "count(status=paid) = 200");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]"),
                  300, "count(status=pending) = 300");

    /* Test 3: Run bulk-delete-criteria with status=paid */
    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"bdc_idx_dir\",\"object\":\"bdc_idx\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"matched\":200", "bulk-delete-criteria: matched=200");
    ASSERT_CONTAINS(resp, "\"deleted\":200", "bulk-delete-criteria: deleted=200");
    ASSERT_CONTAINS(resp, "\"skipped\":0", "bulk-delete-criteria: skipped=0");
    free(resp); resp = NULL;

    /* Test 4: Verify paid records are gone */
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"),
                  0, "count(status=paid) = 0 after delete");

    /* Test 5: Verify pending records remain */
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]"),
                  300, "count(status=pending) = 300 after delete");

    /* Test 6: Run find by status=pending */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"bdc_idx_dir\",\"object\":\"bdc_idx\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}],\"limit\":500}", &resp);
    int found_count = 0;
    if (resp) {
        const char *p = resp;
        while ((p = strstr(p, "\"key\":\"")) != NULL) { found_count++; p += 7; }
    }
    ASSERT_EQ_INT(found_count, 300, "find(status=pending) returns 300 rows");
    free(resp); resp = NULL;

    /* Test 7: Verify total count = 300 */
    ASSERT_EQ_INT(count_total(tc), 300, "total count = 300 after delete");

    /* Test 8: Run bulk-delete-criteria with score >= 0 (all remaining) */
    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"bdc_idx_dir\",\"object\":\"bdc_idx\","
        "\"criteria\":[{\"field\":\"score\",\"op\":\"gte\",\"value\":\"0\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"deleted\":300", "bulk-delete-criteria: deleted=300 (all remaining)");
    free(resp); resp = NULL;

    /* Test 9: Verify total count = 0 */
    ASSERT_EQ_INT(count_total(tc), 0, "total count = 0 after final delete");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test_bulk_delete_criteria_indexed", test_bulk_delete_criteria_indexed_run)
