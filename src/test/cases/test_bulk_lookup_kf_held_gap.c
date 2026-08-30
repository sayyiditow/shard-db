/* Regression for the bulk-lookup phase gap (docs/plans/2026-08-28-
 * eliminate-tsan-supp.md Task B1).
 *
 * slotcask_bulk_lookup_in_kfshard used to release the kf reader after its
 * probe phase and only re-verify the segment record afterwards, so a
 * durability window could tombstone (T) and re-emit (P) the same slot in
 * the gap — plain-vs-plain byte races against the verify, and a violation
 * of the 2026-08-21 window contract ("Kf read handle stays live until the
 * segment record has been checked against its hash/key and copied").
 *
 * The fix holds the kf reader across both phases. This test parks the
 * lookup in the phase gap (TEST_BUILD hook), starts a delete — which must
 * BLOCK behind the held kf reader — then releases the park and requires
 * a coherent outcome. Red on the pre-fix tree: the delete completed while
 * the lookup was parked, and TSan reported the plain-vs-plain race.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_assert.h"
#include "test_runner.h"
#include "fixtures.h"
#include "slotcask.h"
#include "shard_test_ctl.h"
#include "types.h"
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

typedef struct GapLookupArg {
    SlotcaskDb *sdb;
    int shard;
    SlotcaskBulkRec rec;
} GapLookupArg;

static void *gap_lookup_main(void *arg) {
    GapLookupArg *a = arg;
    slotcask_bulk_lookup_in_kfshard(a->sdb, a->shard, &a->rec, 1);
    return NULL;
}

typedef struct GapDeleteArg {
    SlotcaskDb *sdb;
    const char *key;
    atomic_int done;
} GapDeleteArg;

static void *gap_delete_main(void *arg) {
    GapDeleteArg *a = arg;
    SlotcaskDeleteResult res;
    memset(&res, 0, sizeof(res));
    slotcask_delete_with_hooks(a->sdb, a->key, strlen(a->key), NULL, &res);
    atomic_store(&a->done, 1);
    return NULL;
}

static int test_bulk_lookup_kf_held_gap_run(void) {
    ShardDb *db = test_get_process_db();
    const char *root = test_get_process_db_root();
    if (!db || !root) {
        ASSERT_TRUE(0, "process-local db available");
        return 1;
    }

    const char *dir = "d";
    const char *object = "gapobj";
    const char *key = "gapkey";
    tu_pdb_drop_object(db, dir, object);

    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\","
        "\"object\":\"gapobj\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"value:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create gap fixture");
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"gapobj\","
        "\"key\":\"gapkey\",\"value\":{\"value\":7}}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""), "seed gap fixture");
    free(resp); resp = NULL;

    char effective_root[PATH_MAX];
    snprintf(effective_root, sizeof(effective_root), "%s/%s", root, dir);
    Schema sch = load_schema(effective_root, object);
    SlotcaskSchemaInfo info = {
        .splits = sch.splits,
        .slot_size = sch.slot_size,
        .streams = sch.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(effective_root,
                                                        object, &info);
    ASSERT_NOT_NULL(sdb, "registry open for gap fixture");
    if (!sdb) {
        tu_pdb_drop_object(db, dir, object);
        return 1;
    }

    uint8_t hash[16];
    compute_hash_raw(key, strlen(key), hash);
    int shard = compute_record_shard(hash, sch.splits);

    GapLookupArg larg = { .sdb = sdb, .shard = shard, .rec = {0} };
    larg.rec.key = key;
    larg.rec.klen = strlen(key);
    GapDeleteArg darg;
    memset(&darg, 0, sizeof(darg));
    darg.sdb = sdb;
    darg.key = key;
    atomic_init(&darg.done, 0);

    /* Arm the phase-gap park, start the lookup, wait for it to park. */
    atomic_store(&g_shard_test_bulk_lookup_gap, 1);
    atomic_store(&g_shard_test_bulk_lookup_gap_hit, 0);
    atomic_store(&g_shard_test_bulk_lookup_gap_release, 0);
    pthread_t lt, dt;
    ASSERT_TRUE(pthread_create(&lt, NULL, gap_lookup_main, &larg) == 0,
                "start parked lookup");
    for (int i = 0; i < 5000 && !atomic_load(&g_shard_test_bulk_lookup_gap_hit);
         i++)
        usleep(1000);
    ASSERT_TRUE(atomic_load(&g_shard_test_bulk_lookup_gap_hit) == 1,
                "lookup parked in the phase gap");

    /* The delete must queue behind the lookup's still-held kf reader. */
    ASSERT_TRUE(pthread_create(&dt, NULL, gap_delete_main, &darg) == 0,
                "start concurrent delete");
    int spins = 0;
    while (atomic_load(&darg.done) == 0 && spins < 500) {
        usleep(1000);
        spins++;
    }
    ASSERT_TRUE(atomic_load(&darg.done) == 0,
                "delete blocks behind the lookup's held kf reader "
                "(pre-fix tree completes it during the park)");

    atomic_store(&g_shard_test_bulk_lookup_gap_release, 1);
    pthread_join(lt, NULL);
    ASSERT_TRUE(larg.rec.status == 0, "lookup verified the live record");
    for (int i = 0; i < 5000 && atomic_load(&darg.done) == 0; i++)
        usleep(1000);
    ASSERT_TRUE(atomic_load(&darg.done) == 1,
                "delete completes after the lookup releases");
    pthread_join(dt, NULL);

    /* Key is gone: a second delete reports not_found. */
    SlotcaskDeleteResult res2;
    memset(&res2, 0, sizeof(res2));
    slotcask_delete_with_hooks(sdb, key, strlen(key), NULL, &res2);
    ASSERT_TRUE(res2.not_found == 1, "key removed exactly once");

    atomic_store(&g_shard_test_bulk_lookup_gap, 0);
    atomic_store(&g_shard_test_bulk_lookup_gap_hit, 0);
    atomic_store(&g_shard_test_bulk_lookup_gap_release, 0);
    tu_pdb_drop_object(db, dir, object);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bulk-lookup-kf-held-gap", test_bulk_lookup_kf_held_gap_run)
