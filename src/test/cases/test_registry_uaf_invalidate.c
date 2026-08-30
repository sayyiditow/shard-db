/* Regression for registry invalidation racing a borrowed SlotcaskDb handle.
 *
 * Before registry references, invalidate closes and frees a cached handle while
 * readers may still be touching it. This deliberately bypasses objlock: the
 * registry lifetime contract itself must make the borrowed handle safe. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_assert.h"
#include "test_runner.h"
#include "fixtures.h"
#include "slotcask.h"
#include "types.h"
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct RegistryUafReader {
    const char *root;
    const SlotcaskSchemaInfo *info;
    atomic_int *stop;
} RegistryUafReader;

static void *registry_uaf_reader_main(void *arg) {
    RegistryUafReader *ctx = arg;
    while (!atomic_load_explicit(ctx->stop, memory_order_acquire)) {
        SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(ctx->root, "uaf_obj", ctx->info);
        if (sdb) {
            volatile int shards = sdb->num_shards;
            volatile char data_dir[256];
            memcpy((void *)data_dir, sdb->data_dir, sizeof(data_dir));
            (void)shards;
        }
        usleep(200);
    }
    return NULL;
}

static int test_registry_uaf_invalidate_run(void) {
    ShardDb *db = test_get_process_db();
    const char *root = test_get_process_db_root();
    if (!db || !root) {
        ASSERT_TRUE(0, "process-local db available");
        return 1;
    }

    const char *dir = "d";
    const char *object = "uaf_obj";
    tu_pdb_drop_object(db, dir, object);

    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tu_pdb_request(db,
        "{\"mode\":\"create-object\",\"dir\":\"d\","
        "\"object\":\"uaf_obj\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"value:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"",
                    "create registry-uaf fixture");
    free(resp); resp = NULL;

    tu_pdb_request(db,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"uaf_obj\","
        "\"key\":\"one\",\"value\":{\"value\":1}}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""),
                "seed registry-uaf fixture");
    free(resp); resp = NULL;

    char effective_root[PATH_MAX];
    snprintf(effective_root, sizeof(effective_root), "%s/%s", root, dir);
    Schema sch = load_schema(effective_root, object);
    SlotcaskSchemaInfo info = {
        .splits = sch.splits,
        .slot_size = sch.slot_size,
        .streams = sch.streams,
    };

    atomic_int stop = 0;
    RegistryUafReader reader = {
        .root = effective_root,
        .info = &info,
        .stop = &stop,
    };
    pthread_t readers[4];
    int started = 0;
    for (; started < 4; started++) {
        if (pthread_create(&readers[started], NULL, registry_uaf_reader_main,
                           &reader) != 0) {
            ASSERT_TRUE(0, "start registry-uaf reader");
            break;
        }
    }

    for (int i = 0; i < 2000; i++) {
        slotcask_registry_invalidate(effective_root, object);
        usleep(500);
    }

    atomic_store_explicit(&stop, 1, memory_order_release);
    for (int i = 0; i < started; i++) pthread_join(readers[i], NULL);

    tu_pdb_drop_object(db, dir, object);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-registry-uaf-invalidate", test_registry_uaf_invalidate_run)
