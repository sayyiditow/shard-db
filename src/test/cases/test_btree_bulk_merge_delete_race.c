/* src/test/cases/test_btree_bulk_merge_delete_race.c
 *
 * Root-cause regression for the lost-update race between btree_bulk_merge's
 * extract-and-rebuild path and a concurrent btree_delete on the same B+ tree
 * file: btree_bulk_merge() takes a snapshot of the existing tree via
 * bt_extract_all() (a short-lived read acquire), then later republishes that
 * snapshot (merged with the new batch) via btree_bulk_build(). If a
 * btree_delete() for an entry that was present in the snapshot completes in
 * that window, its effect is silently overwritten when the snapshot is
 * rebuilt — the deleted entry resurrects.
 *
 * This does not rely on scheduling luck. It runs two controlled phases with
 * the same after-extract park. First, the production mutation gate remains
 * enabled: releasing the merge lets it rebuild and unlock before the delete
 * can run, so the deletion must persist. Second, a TEST_BUILD-only switch
 * bypasses that one delete gate. The test then waits unconditionally for the
 * delete to finish before it releases the merge, forcing the old
 * delete-before-rebuild ordering and proving that it resurrects the entry.
 * No timeout or retry loop decides either ordering.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "btree.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define SEED_COUNT 1200
#define DELETE_IDX 500

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    int             reached_hook;
    int             release;
    int             delete_done;
} RaceSync;

static void race_sync_init(RaceSync *s) {
    pthread_mutex_init(&s->lock, NULL);
    pthread_cond_init(&s->cond, NULL);
    s->reached_hook = 0;
    s->release = 0;
    s->delete_done = 0;
}

static void race_sync_destroy(RaceSync *s) {
    pthread_mutex_destroy(&s->lock);
    pthread_cond_destroy(&s->cond);
}

/* Runs on the merge thread, immediately after bt_extract_all() has copied
   and released the old tree. Announces "snapshot copied" and blocks until
   the test thread has driven the delete through to completion. */
static void after_extract_hook(void *ctx) {
    RaceSync *s = ctx;
    pthread_mutex_lock(&s->lock);
    s->reached_hook = 1;
    pthread_cond_broadcast(&s->cond);
    while (!s->release) pthread_cond_wait(&s->cond, &s->lock);
    pthread_mutex_unlock(&s->lock);
}

typedef struct {
    const char *path;
    BtEntry    *new_entries;
    size_t      new_count;
    int         rc;
    ShardDb    *db;
} MergeArgs;

static void *merge_thread_main(void *arg) {
    MergeArgs *a = arg;
    g_db = a->db;
    a->rc = btree_bulk_merge(a->path, a->new_entries, a->new_count);
    return NULL;
}

typedef struct {
    const char *path;
    const char *value;
    size_t      vlen;
    const uint8_t *hash;
    ShardDb    *db;
    RaceSync   *sync;
} DeleteArgs;

static void *delete_thread_main(void *arg) {
    DeleteArgs *a = arg;
    g_db = a->db;
    RaceSync *s = a->sync;

    /* Wait for the merge thread to have copied its snapshot and parked at
       the hook before deleting — this is what puts the delete inside the
       lost-update window. */
    pthread_mutex_lock(&s->lock);
    while (!s->reached_hook) pthread_cond_wait(&s->cond, &s->lock);
    pthread_mutex_unlock(&s->lock);

    btree_delete(a->path, a->value, a->vlen, a->hash);

    pthread_mutex_lock(&s->lock);
    s->delete_done = 1;
    pthread_cond_broadcast(&s->cond);
    pthread_mutex_unlock(&s->lock);
    return NULL;
}

static int g_search_count;
static int search_count_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    (void)v; (void)vl; (void)h; (void)ctx;
    g_search_count++;
    return 0;
}

static int search_count(const char *path, const char *value, size_t vlen) {
    g_search_count = 0;
    btree_search(path, value, vlen, search_count_cb, NULL);
    return g_search_count;
}

static void run_race_phase(const char *path, BtEntry *seed,
                           const char *del_value, size_t del_vlen,
                           const uint8_t del_hash[BT_HASH_SIZE],
                           BtEntry *merge_entry,
                           const char *merge_value, size_t merge_vlen,
                           int bypass_delete_gate, int expected_delete_count,
                           const char *phase_name) {
    ASSERT_EQ_INT(btree_bulk_build(path, seed, (size_t)SEED_COUNT), 0, phase_name);
    ASSERT_EQ_INT(search_count(path, del_value, del_vlen), 1, "delete target present before race");

    RaceSync sync;
    race_sync_init(&sync);
    btree_test_set_delete_gate_bypass(bypass_delete_gate);
    btree_test_set_after_extract_hook(after_extract_hook, &sync);

    MergeArgs margs = {
        .path = path, .new_entries = merge_entry, .new_count = 1,
        .rc = -2, .db = g_db
    };
    DeleteArgs dargs = {
        .path = path, .value = del_value, .vlen = del_vlen,
        .hash = del_hash, .db = g_db, .sync = &sync
    };

    pthread_t merge_tid, delete_tid;
    pthread_create(&merge_tid, NULL, merge_thread_main, &margs);
    pthread_create(&delete_tid, NULL, delete_thread_main, &dargs);

    pthread_mutex_lock(&sync.lock);
    while (!sync.reached_hook) pthread_cond_wait(&sync.cond, &sync.lock);
    if (bypass_delete_gate) {
        /* The explicit test-only bypass reproduces the pre-fix path. The
           merge cannot resume until this delete has completed, so this is a
           real delete-before-rebuild interleaving, not a timing guess. */
        while (!sync.delete_done) pthread_cond_wait(&sync.cond, &sync.lock);
    }
    sync.release = 1;
    pthread_cond_broadcast(&sync.cond);
    pthread_mutex_unlock(&sync.lock);

    pthread_join(delete_tid, NULL);
    pthread_join(merge_tid, NULL);

    btree_test_set_after_extract_hook(NULL, NULL);
    btree_test_set_delete_gate_bypass(0);
    race_sync_destroy(&sync);

    ASSERT_EQ_INT(margs.rc, 0, "btree_bulk_merge succeeded");
    ASSERT_EQ_INT(search_count(path, del_value, del_vlen), expected_delete_count,
        bypass_delete_gate ? "gate bypass reproduces resurrected entry"
                           : "mutation gate preserves completed delete");
    ASSERT_EQ_INT(search_count(path, merge_value, merge_vlen), 1,
        "newly merged entry is present exactly once");
}

static int test_btree_bulk_merge_delete_race_run(void) {
    char tmpl[] = "/tmp/shard-db-bulk-merge-race-XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd < 0) { ASSERT_TRUE(0, "mkstemp"); return 1; }
    close(fd);
    unlink(tmpl); /* mkstemp just reserves a unique name; btree owns the file */
    char path[sizeof(tmpl) + 4];
    snprintf(path, sizeof(path), "%s.idx", tmpl);
    unlink(path);

    setenv("SHARDKV_BULK_RATIO", "0", 1);

    /* Seed more than 1,000 entries so the extract-and-rebuild branch is doing
       real work, all routed through one local .idx file. */
    BtEntry *seed = malloc((size_t)SEED_COUNT * sizeof(BtEntry));
    for (int i = 0; i < SEED_COUNT; i++) {
        char *v = malloc(32);
        int vlen = snprintf(v, 32, "seed_%05d", i);
        seed[i].value = v;
        seed[i].vlen = (size_t)vlen;
        memset(seed[i].hash, 0, BT_HASH_SIZE);
        memcpy(seed[i].hash, &i, sizeof(int));
    }
    /* The delete target must be one of the just-seeded entries in the same
       .idx file so it is present in the snapshot each merge copies. */
    char del_value[32];
    size_t del_vlen = (size_t)snprintf(del_value, sizeof(del_value), "seed_%05d", DELETE_IDX);
    uint8_t del_hash[BT_HASH_SIZE];
    memset(del_hash, 0, BT_HASH_SIZE);
    memcpy(del_hash, &(int){DELETE_IDX}, sizeof(int));

    /* One-entry merge batch — new_count=1 keeps SHARDKV_BULK_RATIO=0's
       extract-and-rebuild branch cheap while still exercising the real
       merge-with-existing-tree path. */
    BtEntry merge_entry;
    char merge_value[32];
    size_t merge_vlen = (size_t)snprintf(merge_value, sizeof(merge_value), "zzz_new_entry");
    merge_entry.value = merge_value;
    merge_entry.vlen = merge_vlen;
    memset(merge_entry.hash, 0, BT_HASH_SIZE);
    merge_entry.hash[0] = 0xAB;

    run_race_phase(path, seed, del_value, del_vlen, del_hash,
                   &merge_entry, merge_value, merge_vlen,
                   0, 0, "seed bulk build for gated phase");
    run_race_phase(path, seed, del_value, del_vlen, del_hash,
                   &merge_entry, merge_value, merge_vlen,
                   1, 1, "seed bulk build for bypass phase");

    for (int i = 0; i < SEED_COUNT; i++) free((char *)seed[i].value);
    free(seed);
    unlink(path);
    unsetenv("SHARDKV_BULK_RATIO");
    bt_cache_shutdown();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-btree-bulk-merge-delete-race", test_btree_bulk_merge_delete_race_run)
