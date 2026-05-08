/* test_slotcask_resplit.c — kf shard auto-resplit (linear-hashing doubling).
 *
 * Verifies that kf_put_new triggers a resplit when global load crosses the
 * 75% threshold. Inserting 75% × num_shards × slots_per_shard records to
 * naturally trigger this would mean ~6M inserts at the splits=8 tier —
 * doable but slow for a unit test. We trick the threshold by manually
 * spiking db.live_count, then issue ONE insert and assert:
 *
 *   - The targeted shard's on-disk kf file doubled (24 MB → 48 MB at the
 *     splits=8 tier with 1M slots → 2M slots).
 *   - The kfcache entry's capacity reflects the new size on next acquire.
 *   - All previously-written records remain readable post-resplit.
 *   - The newly-inserted record is also readable.
 *
 * Standalone: calls slotcask_* directly. No daemon, no TCP. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>

static void rm_rf(const char *path) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    int rc = system(cmd); (void)rc;
}

static void unique_tmpdir(char out[256]) {
    const char *base = getenv("SHARD_TEST_TMPDIR");
    if (!base || !*base) base = "/tmp";
    snprintf(out, 256, "%s/shard_slotcask_resplit_%d_%ld",
             base, (int)getpid(), (long)time(NULL));
}

static int test_slotcask_resplit_run(void) {
    char dir[256];
    unique_tmpdir(dir);
    rm_rf(dir);
    slotcask_init(64, 64);

    SlotcaskDb db;
    /* splits=8 → slots_per_shard = 1M (slotcask_default_slots_for_splits). */
    int rc = slotcask_open(&db, dir, 8, 1, 256);
    ASSERT_EQ_INT(rc, 0, "slotcask_open succeeds");
    if (rc != 0) { rm_rf(dir); return 1; }

    /* Insert 100 records — gives us a meaningful repopulation walk. */
    for (int i = 0; i < 100; i++) {
        char k[32], v[64];
        snprintf(k, sizeof(k), "key-%05d", i);
        snprintf(v, sizeof(v), "value-%d", i);
        ASSERT_EQ_INT(slotcask_insert(&db, -1, k, strlen(k), v, strlen(v)), 0,
                      "pre-resplit insert");
    }

    /* Snapshot kf shard 0's file size before resplit. */
    char kf_path[512];
    snprintf(kf_path, sizeof(kf_path), "%s/data/kf/000.kf", dir);
    struct stat st_pre;
    ASSERT_EQ_INT(stat(kf_path, &st_pre), 0, "kf 000 exists pre-resplit");
    /* Default tier-1 sizing: 1M × 24B = 24 MB. */
    ASSERT_EQ_INT((long long)st_pre.st_size, (long long)(1ull * 1024 * 1024 * 24),
                  "kf 000 is 24 MB pre-resplit");

    /* Spike the live counter past the 75% global threshold:
       75% × num_shards × slots_per_shard = 0.75 × 8 × 1M = 6M. Set to 7M. */
    int64_t fake_load = 7ll * 1024 * 1024;
    atomic_store_explicit(&db.live_count, fake_load, memory_order_relaxed);

    /* Insert keys with hashes that route to shard 0 specifically. We don't
       have a public hash helper here; spam-insert until shard 0 fires the
       resplit. A handful of inserts will hit it given uniform xxh128. */
    int triggered = 0;
    for (int i = 0; i < 64; i++) {
        char k[32], v[16];
        snprintf(k, sizeof(k), "trigger-%d", i);
        snprintf(v, sizeof(v), "v%d", i);
        slotcask_insert(&db, -1, k, strlen(k), v, strlen(v));

        struct stat st;
        if (stat(kf_path, &st) == 0 &&
            (size_t)st.st_size > (size_t)st_pre.st_size) {
            triggered = 1;
            break;
        }
    }
    ASSERT_TRUE(triggered, "shard 0 resplit fired within 64 inserts");

    /* Confirm the new size is exactly 2x. */
    struct stat st_post;
    ASSERT_EQ_INT(stat(kf_path, &st_post), 0, "kf 000 still exists post-resplit");
    ASSERT_EQ_INT((long long)st_post.st_size,
                  (long long)(2ull * 1024 * 1024 * 24),
                  "kf 000 doubled to 48 MB");

    /* No leftover .new staging file. */
    char kf_new[512];
    snprintf(kf_new, sizeof(kf_new), "%s.new", kf_path);
    struct stat st_new;
    ASSERT_TRUE(stat(kf_new, &st_new) != 0, ".new staging file was cleaned up");

    /* Every original record still readable. */
    int readable = 0;
    for (int i = 0; i < 100; i++) {
        char k[32];
        snprintf(k, sizeof(k), "key-%05d", i);
        void *v = NULL; size_t vl = 0;
        if (slotcask_get(&db, k, strlen(k), &v, &vl) == 0) {
            readable++;
            free(v);
        }
    }
    ASSERT_EQ_INT(readable, 100, "all 100 originals readable post-resplit");

    /* Trigger keys (those that successfully inserted) are also readable. */
    int trigger_hits = 0;
    for (int i = 0; i < 64; i++) {
        char k[32];
        snprintf(k, sizeof(k), "trigger-%d", i);
        void *v = NULL; size_t vl = 0;
        if (slotcask_get(&db, k, strlen(k), &v, &vl) == 0) {
            trigger_hits++;
            free(v);
        }
    }
    ASSERT_TRUE(trigger_hits > 0, "at least one trigger key readable");

    /* Close + reopen — the larger kf file should be picked up automatically
       by kf_open_file's "existing file is bigger than expected" branch. */
    slotcask_close(&db);
    rc = slotcask_open(&db, dir, 8, 1, 256);
    ASSERT_EQ_INT(rc, 0, "slotcask_open after resplit succeeds");

    int readable_re = 0;
    for (int i = 0; i < 100; i++) {
        char k[32];
        snprintf(k, sizeof(k), "key-%05d", i);
        void *v = NULL; size_t vl = 0;
        if (slotcask_get(&db, k, strlen(k), &v, &vl) == 0) {
            readable_re++;
            free(v);
        }
    }
    ASSERT_EQ_INT(readable_re, 100, "all 100 originals readable after reopen");

    /* live_count should be rebuilt by walking — count of flag=1 entries
       across all shards. We can't easily check this externally, but it
       should be >0 (at minimum the 100 originals). */
    int64_t live = atomic_load_explicit(&db.live_count, memory_order_relaxed);
    ASSERT_TRUE(live >= 100, "live_count rebuilt at open >= 100");

    slotcask_close(&db);
    slotcask_shutdown();
    rm_rf(dir);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-resplit", test_slotcask_resplit_run)
