/* src/test/cases/test_single_op_index_sync.c
 *
 * Single-op index durability shape: one indexed op issues exactly one
 * fdatasync per changed non-bitmap field (btree_sync_path counter), bitmap
 * fields keep exactly one bm_sync per changed bitmap field (bm_sync
 * counter), and the trigram/delete paths sync deterministically (base:
 * stack garbage decided them). Case 3 is the regression guard for the
 * bitmap-conditional sync flag.
 *
 * The bitmap field is a varchar with an explicit :bitmap index, NOT a bool:
 * bare JSON boolean literals are dropped by the partial-update merge (see
 * docs/plans/2026-08-17-bool-literal-merge-bug.md), which would make the
 * bitmap counts here nondeterministic. String literals merge correctly.
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

extern void btree_test_sync_reset(void);
extern int  btree_test_sync_count(void);
extern void bm_test_sync_reset(void);
extern int  bm_test_sync_count(void);

static int req_ok(ShardDb *db, const char *json, const char *what) {
    char *resp = NULL;
    int rc = tu_pdb_request(db, json, &resp);
    int ok = rc == 0 && resp && strstr(resp, "\"error\"") == NULL;
    if (!ok) fprintf(stderr, "req failed (%s): %s\n", what, resp ? resp : "(null)");
    shard_db_free_result(resp);
    return ok;
}

static int test_single_op_index_sync_run(void) {
    ShardDb *db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;

    ASSERT_TRUE(req_ok(db, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", "add-dir"),
                "add-dir");
    /* 2 btree + 1 trigram + 1 varchar bitmap field. */
    ASSERT_TRUE(req_ok(db,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ssox_t\","
        "\"splits\":8,\"max_key\":16,\"fields\":["
        "\"a:varchar:32\",\"b:varchar:32\",\"t:varchar:64\",\"status:varchar:8\"],"
        "\"indexes\":[\"a\",\"b\",\"t:trigram\",\"status:bitmap\"]}", "create"), "create-object");

    /* 1) Fresh insert touching all four fields → 3 btree/tg syncs
          (a, b via index_parallel's internal flush; t via the tg collector)
          + 1 bm_sync (status, via bitmap prepare/apply). On base the trigram
          leg is garbage-dependent. */
    btree_test_sync_reset(); bm_test_sync_reset();
    ASSERT_TRUE(req_ok(db,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ssox_t\","
        "\"key\":\"k1\",\"value\":{\"a\":\"alpha\",\"b\":\"beta\","
        "\"t\":\"trigram text one\",\"status\":\"A\"}}", "insert"), "insert");
    ASSERT_EQ_INT(btree_test_sync_count(), 3, "insert: 1 sync per non-bitmap field");
    ASSERT_EQ_INT(bm_test_sync_count(), 1, "insert: bitmap field syncs once");

    /* 2) Update changing one btree field → 1 btree sync, 0 bm. */
    btree_test_sync_reset(); bm_test_sync_reset();
    ASSERT_TRUE(req_ok(db,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ssox_t\","
        "\"key\":\"k1\",\"value\":{\"a\":\"alpha2\"}}", "update-one"), "update 1 field");
    ASSERT_EQ_INT(btree_test_sync_count(), 1, "update: only changed field syncs");
    ASSERT_EQ_INT(bm_test_sync_count(), 0, "update: no bitmap change, no bm sync");

    /* 3) REGRESSION GUARD (bitmap-conditional flag): update changing only
          the bitmap field → 0 btree syncs, exactly 1 bm_sync. A blanket
          sync_after=0 at the arg-fill site makes this 0/0 — silent bitmap
          durability loss the crash tests cannot catch. */
    btree_test_sync_reset(); bm_test_sync_reset();
    ASSERT_TRUE(req_ok(db,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ssox_t\","
        "\"key\":\"k1\",\"value\":{\"status\":\"B\"}}", "update-status"), "update status");
    ASSERT_EQ_INT(btree_test_sync_count(), 0, "status-only update: no btree syncs");
    ASSERT_EQ_INT(bm_test_sync_count(), 1, "status-only update: bitmap still syncs");

    /* 4) Update changing all four → 3 btree + 1 bm. */
    btree_test_sync_reset(); bm_test_sync_reset();
    ASSERT_TRUE(req_ok(db,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ssox_t\","
        "\"key\":\"k1\",\"value\":{\"a\":\"alpha3\",\"b\":\"beta3\","
        "\"t\":\"trigram text two\",\"status\":\"A\"}}", "update-all"), "update 4 fields");
    ASSERT_EQ_INT(btree_test_sync_count(), 3, "update: 3 changed non-bitmap sync");
    ASSERT_EQ_INT(bm_test_sync_count(), 1, "update: changed bitmap syncs once");

    /* 5) Delete → the OLD record had all four values → 3 btree/tg + 1 bm.
          Exercises v2_delete_apply_commit (the fourth hook flavor); on base
          its counts are garbage-dependent. */
    btree_test_sync_reset(); bm_test_sync_reset();
    ASSERT_TRUE(req_ok(db,
        "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"ssox_t\","
        "\"key\":\"k1\"}", "delete"), "delete");
    ASSERT_EQ_INT(btree_test_sync_count(), 3, "delete: removed entries sync");
    ASSERT_EQ_INT(bm_test_sync_count(), 1, "delete: cleared bitmap syncs");

    tu_pdb_drop_object(db, "default", "ssox_t");
    return 0;
}
TEST_REGISTER("test-single-op-index-sync", test_single_op_index_sync_run)
