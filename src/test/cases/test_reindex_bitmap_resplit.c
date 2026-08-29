/* Regression for bitmap rebuilds against auto-resized keyfile shards.
 *
 * A keyfile shard can double beyond slotcask_default_slots_for_splits().
 * The live write path grows its bitmap before setting a bit, but the
 * reindex path used the default capacity for every replacement bitmap.
 * Reindex then failed as soon as a live key resolved to a slot above that
 * default. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_assert.h"
#include "test_runner.h"
#include "fixtures.h"
#include "slotcask.h"
#include "types.h"
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static int test_reindex_bitmap_after_kf_resplit_run(void) {
    ShardDb *db = test_get_process_db();
    const char *root = test_get_process_db_root();
    if (!db || !root) {
        ASSERT_TRUE(0, "process-local db available");
        return 1;
    }

    const char *dir = "d";
    const char *object = "bitmap_resplit";
    tu_pdb_drop_object(db, dir, object);

    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\","
        "\"object\":\"bitmap_resplit\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"active:bool\"],"
        "\"indexes\":[\"active:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"",
                    "create bitmap-resplit fixture");
    free(resp); resp = NULL;

    char effective_root[PATH_MAX];
    snprintf(effective_root, sizeof(effective_root), "%s/%s", root, dir);
    Schema sch = load_schema(effective_root, object);
    ASSERT_EQ_INT(sch.splits, 8, "fixture uses eight keyfile shards");
    if (sch.splits != 8) {
        tu_pdb_drop_object(db, dir, object);
        return 1;
    }

    SlotcaskSchemaInfo info = {
        .splits = sch.splits,
        .slot_size = sch.slot_size,
        .streams = sch.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(effective_root, object, &info);
    ASSERT_NOT_NULL(sdb, "open fixture slotcask registry entry");
    if (!sdb) {
        tu_pdb_drop_object(db, dir, object);
        return 1;
    }

    size_t base_slots = slotcask_default_slots_for_splits(sch.splits);
    size_t grown_slots = base_slots * 2;
    char key[64];
    uint8_t hash[16];
    int found = 0;
    for (int i = 0; i < 200000; i++) {
        snprintf(key, sizeof(key), "high-slot-%d", i);
        compute_hash_raw(key, strlen(key), hash);
        if (compute_record_shard(hash, sch.splits) == 0 &&
            kf_slot_for(hash, grown_slots) >= base_slots) {
            found = 1;
            break;
        }
    }
    ASSERT_TRUE(found, "found key routed to shard 0 above default bitmap capacity");
    if (!found) {
        tu_pdb_drop_object(db, dir, object);
        return 1;
    }

    uint64_t fake_total = (uint64_t)((double)base_slots * 0.80);
    ASSERT_EQ_INT(slotcask_test_set_kf_total(sdb, 0, fake_total, 0), 0,
                  "synthetic load trips shard-0 auto-resplit");

    char insert[512];
    snprintf(insert, sizeof(insert),
             "{\"mode\":\"insert\",\"dir\":\"d\","
             "\"object\":\"bitmap_resplit\",\"key\":\"%s\","
             "\"value\":{\"active\":true}}", key);
    tu_pdb_request(db, insert, &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""),
                "insert after keyfile resplit succeeds");
    free(resp); resp = NULL;

    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path), "%s/%s/%s/data/kf/000.kf",
             root, dir, object);
    struct stat st;
    ASSERT_EQ_INT(stat(kf_path, &st), 0, "resplit keyfile exists");
    ASSERT_EQ_INT((long long)st.st_size,
                  (long long)(24 + grown_slots * sizeof(SlotcaskKfEntry)),
                  "shard-0 keyfile doubled beyond default capacity");

    tu_pdb_request(db,
        "{\"mode\":\"reindex\",\"dir\":\"d\","
        "\"object\":\"bitmap_resplit\"}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""),
                "reindex succeeds with auto-resized keyfile shard");
    free(resp);

    tu_pdb_drop_object(db, dir, object);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-reindex-bitmap-resplit",
              test_reindex_bitmap_after_kf_resplit_run)
