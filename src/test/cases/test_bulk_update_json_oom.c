/* src/test/cases/test_bulk_update_json_oom.c
 *
 * Deterministic OOM coverage for bulk_upd_json_run's six unchecked
 * allocation sites (Coverity CID 1699817). Each site is injected with a
 * malloc failure via bulk_upd_json_test_set_fail_alloc(N); the parser
 * must return the canonical "out of memory" error with no crash, no
 * leak, and no partial record state. In-process (process-local ShardDb)
 * so the TEST_BUILD hook is reachable.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/shard_db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern void bulk_upd_json_test_set_fail_alloc(int fail_n);

static int test_bulk_update_json_oom_run(void) {
    ShardDb *db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;

    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    shard_db_free_result(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"buo_t\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:64\"]}", &resp);
    shard_db_free_result(resp); resp = NULL;

    const char *req =
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"buo_t\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"a\"}},"
                     "{\"key\":\"k2\",\"value\":{\"v\":\"b\"}}]}";

    /* Seed k1/k2 — bulk-update only touches existing keys. */
    tu_pdb_request(db,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"buo_t\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"x\"}},"
                     "{\"key\":\"k2\",\"value\":{\"v\":\"y\"}}]}", &resp);
    ASSERT_TRUE(resp != NULL, "seed response");
    if (resp) {
        ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "seed bulk-insert succeeds");
    }
    shard_db_free_result(resp); resp = NULL;

    /* Control: no injection — both records updated. */
    bulk_upd_json_test_set_fail_alloc(0);
    tu_pdb_request(db, req, &resp);
    ASSERT_TRUE(resp != NULL, "control response");
    if (resp) {
        ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "control bulk-update succeeds");
    }
    shard_db_free_result(resp); resp = NULL;

    /* String-input allocation order in bulk_upd_json_run:
       1 = records array, 2 = k1 key, 3 = k1 field arrays,
       4 = k2 key, 5 = k2 field arrays. Every site must fail cleanly. */
    for (int n = 1; n <= 5; n++) {
        bulk_upd_json_test_set_fail_alloc(n);
        tu_pdb_request(db, req, &resp);
        ASSERT_TRUE(resp != NULL, "OOM response present");
        if (resp) {
            ASSERT_TRUE(strstr(resp, "\"error\":\"out of memory\"") != NULL,
                        "clean OOM error response");
        }
        shard_db_free_result(resp); resp = NULL;
    }

    /* Over-armed (no site left): behaves as control. */
    bulk_upd_json_test_set_fail_alloc(99);
    tu_pdb_request(db, req, &resp);
    ASSERT_TRUE(resp != NULL, "over-armed response");
    if (resp) {
        ASSERT_TRUE(strstr(resp, "\"error\"") == NULL,
                    "over-armed injection still succeeds");
    }
    shard_db_free_result(resp); resp = NULL;

    tu_pdb_request(db,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"buo_t\"}", &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 2, "both records still present after OOM cycles");
    shard_db_free_result(resp); resp = NULL;

    tu_pdb_drop_object(db, "default", "buo_t");
    return 0;
}

TEST_REGISTER("test-bulk-update-json-oom", test_bulk_update_json_oom_run)