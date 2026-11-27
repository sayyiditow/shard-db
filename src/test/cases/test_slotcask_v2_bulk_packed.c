/* Packed copy of test_slotcask_v2_bulk.c (class K original stays legacy
 * until T3). The criteria bulk-update block is dropped; all other bulk
 * paths and index-consistency assertions are copied.
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
        "{\"mode\":\"count\",\"dir\":\"bulk2\",\"object\":\"ordersp\","
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
        "{\"mode\":\"count\",\"dir\":\"bulk2\",\"object\":\"ordersp\"}", &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_slotcask_v2_bulk_packed_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"bulk2\"}", &resp);
    free(resp); resp = NULL;

    /* v2 object */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"declaration_layout\":\"packed\",\"dir\":\"bulk2\",\"object\":\"ordersp\","
        "\"splits\":8,\"max_key\":40,"
        "\"fields\":[\"status:varchar:16\",\"amount:int\",\"region:varchar:16\","
                    "\"note:varchar:32\"],"
        "\"indexes\":[\"status\",\"amount\",\"region\"]}", &resp);
    free(resp); resp = NULL;

    /* ===== bulk-insert: 6 records ===== */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"bulk2\",\"object\":\"ordersp\","
        "\"records\":["
        "{\"key\":\"o1\",\"value\":{\"status\":\"paid\",\"amount\":100,\"region\":\"NA\",\"note\":\"a\"}},"
        "{\"key\":\"o2\",\"value\":{\"status\":\"paid\",\"amount\":200,\"region\":\"EU\",\"note\":\"b\"}},"
        "{\"key\":\"o3\",\"value\":{\"status\":\"pending\",\"amount\":50,\"region\":\"NA\",\"note\":\"c\"}},"
        "{\"key\":\"o4\",\"value\":{\"status\":\"paid\",\"amount\":300,\"region\":\"AS\",\"note\":\"d\"}},"
        "{\"key\":\"o5\",\"value\":{\"status\":\"refunded\",\"amount\":75,\"region\":\"EU\",\"note\":\"e\"}},"
        "{\"key\":\"o6\",\"value\":{\"status\":\"pending\",\"amount\":400,\"region\":\"NA\",\"note\":\"f\"}}"
        "]}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":6", "v2 bulk-insert: 6 inserted");
    free(resp); resp = NULL;

    ASSERT_EQ_INT(count_total(tc), 6, "count total = 6 after seed");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"),
                  3, "indexed count(status=paid) = 3");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"region\",\"op\":\"eq\",\"value\":\"NA\"}]"),
                  3, "indexed count(region=NA) = 3");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"amount\",\"op\":\"gte\",\"value\":\"200\"}]"),
                  3, "indexed count(amount>=200) = 3");

    /* ===== bulk-insert as upsert: rewrite o2 — status NA→AS, amount 200→999.
       Validates that v2 pre_commit hook drops stale btree entries on upsert. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"bulk2\",\"object\":\"ordersp\","
        "\"records\":[{\"key\":\"o2\",\"value\":{\"status\":\"refunded\",\"amount\":999,\"region\":\"AS\",\"note\":\"bX\"}}]}",
        &resp);
    free(resp); resp = NULL;

    ASSERT_EQ_INT(count_total(tc), 6, "count total still 6 after upsert");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"),
                  2, "count(status=paid) drops 3->2 (o2 reclassified)");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"region\",\"op\":\"eq\",\"value\":\"EU\"}]"),
                  1, "count(region=EU) drops 2->1 (o2 moved to AS)");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"200\"}]"),
                  0, "count(amount=200) drops 1->0 (no stale entry)");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"999\"}]"),
                  1, "count(amount=999) = 1");

    /* AND-intersect must not return stale candidates */
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
                                  "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"EU\"}]"),
                  0, "AND-intersect (paid AND EU) = 0 — no ghost o2");

    /* ===== if_not_exists skips existing key ===== */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"bulk2\",\"object\":\"ordersp\","
        "\"if_not_exists\":true,"
        "\"records\":[{\"key\":\"o1\",\"value\":{\"status\":\"X\",\"amount\":1,\"region\":\"X\",\"note\":\"x\"}},"
                     "{\"key\":\"o7\",\"value\":{\"status\":\"new\",\"amount\":10,\"region\":\"NA\",\"note\":\"g\"}}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"skipped\":1", "if_not_exists skipped existing o1");
    ASSERT_CONTAINS(resp, "\"inserted\":1", "if_not_exists inserted new o7");
    free(resp); resp = NULL;

    ASSERT_EQ_INT(count_total(tc), 7, "count total = 7 after if_not_exists insert");

    /* o1 untouched */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"bulk2\",\"object\":\"ordersp\",\"key\":\"o1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"", "o1 still paid after CAS skip");
    ASSERT_CONTAINS(resp, "\"region\":\"NA\"", "o1 still NA after CAS skip");
    free(resp); resp = NULL;

    /* ===== bulk-update-json: partial-field patch (note only on o3, o5) ===== */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"bulk2\",\"object\":\"ordersp\","
        "\"records\":[{\"key\":\"o3\",\"value\":{\"note\":\"c-patched\"}},"
                     "{\"key\":\"o5\",\"value\":{\"note\":\"e-patched\"}},"
                     "{\"key\":\"missing\",\"value\":{\"note\":\"never\"}}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"updated\":2", "bulk-update partial: 2 updated");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "bulk-update partial: 1 skipped (missing key)");
    free(resp); resp = NULL;

    /* note moved on o3, indexed fields unchanged → indexes still consistent */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"bulk2\",\"object\":\"ordersp\",\"key\":\"o3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"note\":\"c-patched\"", "o3 note patched");
    ASSERT_CONTAINS(resp, "\"status\":\"pending\"", "o3 status untouched");
    ASSERT_CONTAINS(resp, "\"amount\":50", "o3 amount untouched");
    free(resp); resp = NULL;

    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]"),
                  2, "pending count unchanged after note patch");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"50\"}]"),
                  1, "amount=50 count unchanged after note patch");

    /* ===== bulk-update-json: change indexed field (status) ===== */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"bulk2\",\"object\":\"ordersp\","
        "\"records\":[{\"key\":\"o3\",\"value\":{\"status\":\"paid\"}}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"updated\":1", "bulk-update indexed: 1 updated");
    free(resp); resp = NULL;

    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]"),
                  1, "pending count drops 2->1");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"),
                  3, "paid count rises 2->3 (o3 joined)");

    /* Criteria bulk-update dropped: packed objects refuse it
       (AUD-PACKED-BULKUPDATE). Later counts do not depend on it. */

    /* ===== bulk-delete key list ===== */
    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"bulk2\",\"object\":\"ordersp\","
        "\"keys\":[\"o5\",\"o7\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"deleted\":2", "bulk-delete key list: 2 deleted");
    free(resp); resp = NULL;

    ASSERT_EQ_INT(count_total(tc), 5, "count total = 5 after bulk-delete");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"refunded\"}]"),
                  1, "refunded count drops 2->1 (o5 gone)");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"new\"}]"),
                  0, "new count drops 1->0 (o7 gone)");

    /* ===== bulk-delete-criteria: drop all status=paid ===== */
    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"bulk2\",\"object\":\"ordersp\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"deleted\":3", "bulk-delete-criteria: 3 deleted (o1, o2, o3)");
    free(resp); resp = NULL;

    ASSERT_EQ_INT(count_total(tc), 2, "count total = 2 (o4, o6)");
    ASSERT_EQ_INT(count_where(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"),
                  0, "paid count = 0 after bulk-delete-criteria");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-v2-bulk-packed", test_slotcask_v2_bulk_packed_run)
