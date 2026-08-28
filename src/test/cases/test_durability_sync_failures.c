#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "bitmap.h"

#include <errno.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

static int test_durability_msync_injection_run(void) {
    char path[] = "/tmp/shard-db-durability-msync-XXXXXX";
    int fd = mkstemp(path);
    ASSERT_TRUE(fd >= 0, "create file-backed durability test mapping");
    if (fd < 0) return 1;

    size_t len = 4096;
    ASSERT_EQ_INT(ftruncate(fd, (off_t)len), 0,
                  "size durability test mapping");
    void *map = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_TRUE(map != MAP_FAILED, "map durability test file shared");
    if (map == MAP_FAILED) {
        close(fd);
        unlink(path);
        return 1;
    }
    ((unsigned char *)map)[0] = 0x5a;

    durability_test_msync_reset();
    durability_test_msync_fail_next(1, EIO);
    errno = 0;
    ASSERT_EQ_INT(durability_msync(map, len), -1,
                  "injected durability sync failure reaches caller");
    ASSERT_EQ_INT(errno, EIO, "injected durability sync preserves errno");

    int succeeded = -1;
    int failed = -1;
    durability_test_msync_counts(&succeeded, &failed);
    ASSERT_EQ_INT(succeeded, 0, "failed sync is not counted as successful");
    ASSERT_EQ_INT(failed, 1, "failed sync attempt is counted");

    ASSERT_EQ_INT(durability_msync(map, len), 0,
                  "sync succeeds after injected failure is consumed");
    durability_test_msync_counts(&succeeded, &failed);
    ASSERT_EQ_INT(succeeded, 1, "successful sync attempt is counted");
    ASSERT_EQ_INT(failed, 1, "prior failure count remains observable");

    durability_test_msync_reset();
    durability_test_msync_counts(&succeeded, &failed);
    ASSERT_EQ_INT(succeeded, 0, "reset clears successful sync count");
    ASSERT_EQ_INT(failed, 0, "reset clears failed sync count");

    munmap(map, len);
    close(fd);
    unlink(path);

    char cache_dir[] = "/tmp/shard-db-durability-dirty-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(cache_dir), "create dirty-tracking fixture");
    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path), "%s/000.kf", cache_dir);

    slotcask_shutdown();
    slotcask_init(16, 16);
    SlotcaskKfHandle kh;
    ASSERT_EQ_INT(kfcache_acquire(&kh, kf_path, 8, 1), 0,
                  "acquire cached keyfile writer");
    int dirty_slot = kh.slot;
    ASSERT_TRUE(dirty_slot >= 0, "keyfile writer receives tracked cache slot");
    kh.hdr->total = 1;
    kfcache_release(&kh);
    if (dirty_slot >= 0) {
        ASSERT_EQ_INT(atomic_load(&g_kfcache[dirty_slot].dirty), 1,
                      "releasing cached keyfile writer marks slot dirty");
        uint64_t first_dirty_ms =
            atomic_load(&g_kfcache[dirty_slot].dirty_since_ms);
        ASSERT_TRUE(first_dirty_ms > 0,
                    "first dirty transition records a timestamp");

        durability_test_msync_reset();
        durability_test_msync_fail_next(1, EIO);
        ASSERT_EQ_INT(durability_flush_dirty(&g_kfcache[dirty_slot].dirty,
                                             &g_kfcache[dirty_slot].dirty_since_ms,
                                             g_kfcache[dirty_slot].base,
                                             g_kfcache[dirty_slot].map_size),
                      -1, "injected keyfile sync failure is reported");
        ASSERT_EQ_INT(atomic_load(&g_kfcache[dirty_slot].dirty), 1,
                      "failed sweep restores keyfile dirty state");
        ASSERT_TRUE(atomic_load(&g_kfcache[dirty_slot].dirty_since_ms)
                        <= first_dirty_ms,
                    "failed sweep preserves earliest dirty timestamp");

        ASSERT_EQ_INT(durability_flush_dirty(&g_kfcache[dirty_slot].dirty,
                                             &g_kfcache[dirty_slot].dirty_since_ms,
                                             g_kfcache[dirty_slot].base,
                                             g_kfcache[dirty_slot].map_size),
                      1, "retrying keyfile sync succeeds");
        ASSERT_EQ_INT(atomic_load(&g_kfcache[dirty_slot].dirty), 0,
                      "successful retry clears keyfile dirty state");
    }

    char object_dir[PATH_MAX];
    snprintf(object_dir, sizeof(object_dir), "%s/object", cache_dir);
    SlotcaskDb sdb;
    ASSERT_EQ_INT(slotcask_open(&sdb, object_dir, 8, 1, 256), 0,
                  "open slotcask dirty-tracking fixture");
    ASSERT_EQ_INT(slotcask_insert_with_hooks(&sdb, 0, "key", 3, "value", 5,
                                              NULL, NULL), 0,
                  "insert mutates cached segment and keyfile mappings");
    int dirty_segments = 0;
    int dirty_seg_idx = -1;
    for (int i = 0; i < g_segcache_slots; i++) {
        if (atomic_load_explicit(&g_segcache[i].used, memory_order_acquire) &&
            atomic_load(&g_segcache[i].dirty)) {
            dirty_segments++;
            if (dirty_seg_idx < 0) dirty_seg_idx = i;
        }
    }
    ASSERT_TRUE(dirty_segments > 0,
                "slotcask insert marks its cached segment dirty");
    if (dirty_seg_idx >= 0) {
        ASSERT_EQ_INT(
            durability_flush_dirty(&g_segcache[dirty_seg_idx].dirty,
                                   &g_segcache[dirty_seg_idx].dirty_since_ms,
                                   g_segcache[dirty_seg_idx].map,
                                   g_segcache[dirty_seg_idx].map_size),
            1, "sweep synchronizes the dirty segment mapping");
    }
    slotcask_close(&sdb);

    char bt_path[PATH_MAX];
    snprintf(bt_path, sizeof(bt_path), "%s/value.idx", cache_dir);
    uint8_t hash[16] = {0};
    btree_insert(bt_path, "value", 5, hash);
    int dirty_btrees = 0;
    int dirty_bt_idx = -1;
    for (int i = 0; i < bt_cache_slots; i++) {
        if (atomic_load_explicit(&bt_cache[i].used, memory_order_acquire) &&
            atomic_load(&bt_cache[i].dirty)) {
            dirty_btrees++;
            if (dirty_bt_idx < 0) dirty_bt_idx = i;
        }
    }
    ASSERT_TRUE(dirty_btrees > 0, "btree writer release marks slot dirty");

    char bm_path[PATH_MAX];
    snprintf(bm_path, sizeof(bm_path), "%s/value.bm", cache_dir);
    BitmapShard *bm = bm_open(bm_path, 64, 1, 0, 0, 1);
    ASSERT_NOT_NULL(bm, "open cached bitmap writer");
    if (bm) {
        ASSERT_EQ_INT(bm_set(bm, (const uint8_t *)"value", 5, 3), 0,
                      "bitmap set mutates a cached mapping");
        bm_close(bm);
    }
    int dirty_bitmaps = 0;
    int dirty_bm_idx = -1;
    for (int i = 0; i < g_bm_cache_slots; i++) {
        if (atomic_load_explicit(&g_bm_cache[i].used, memory_order_acquire) &&
            atomic_load(&g_bm_cache[i].dirty)) {
            dirty_bitmaps++;
            if (dirty_bm_idx < 0) dirty_bm_idx = i;
        }
    }
    ASSERT_TRUE(dirty_bitmaps > 0, "bitmap writer close marks slot dirty");

    if (dirty_bt_idx >= 0) {
        ASSERT_EQ_INT(
            durability_flush_dirty(&bt_cache[dirty_bt_idx].dirty,
                                   &bt_cache[dirty_bt_idx].dirty_since_ms,
                                   bt_cache[dirty_bt_idx].map,
                                   bt_cache[dirty_bt_idx].map_size),
            1, "sweep synchronizes the dirty btree mapping");
    }
    if (dirty_bm_idx >= 0) {
        ASSERT_EQ_INT(
            durability_flush_dirty(&g_bm_cache[dirty_bm_idx].dirty,
                                   &g_bm_cache[dirty_bm_idx].dirty_since_ms,
                                   g_bm_cache[dirty_bm_idx].map,
                                   g_bm_cache[dirty_bm_idx].map_size),
            1, "sweep synchronizes the dirty bitmap mapping");
    }

    slotcask_shutdown();
    rmrf(cache_dir);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Forward-replay-only design (docs/plans/2026-08-21-main-durability-and-
   window.md): a transient sync fault hit after the marker is durable is
   never surfaced to the caller as a failure. The coordinator retries the
   window forward inline until it converges, so a one-shot injected fault
   (as opposed to a sticky/persistent one) always resolves to success and
   the record ends up durably holding its new value. */
static void assert_kf_sync_failure_recovers(SlotcaskDb *db, int rc,
                                            const char *key, size_t klen,
                                            const char *expected_val,
                                            size_t expected_vlen,
                                            const char *what) {
    ASSERT_EQ_INT(rc, 0, what);
    durability_test_msync_reset();

    void *val = NULL;
    size_t vlen = 0;
    ASSERT_EQ_INT(slotcask_get(db, key, klen, &val, &vlen), 0,
                  "record is readable after forward-replay recovery");
    if (val) {
        ASSERT_EQ_INT((int)vlen, (int)expected_vlen,
                      "recovered record has the new value's length");
        ASSERT_TRUE(vlen == expected_vlen &&
                    memcmp(val, expected_val, expected_vlen) == 0,
                    "recovered record holds the new value, not the old one");
        free(val);
    }
}

static int test_unindexed_kf_sync_failure_propagates(void) {
    char root[] = "/tmp/shard-db-unindexed-kf-sync-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(root), "create unindexed sync-failure fixture");
    if (!root[0]) return 1;

    char object_dir[PATH_MAX];
    snprintf(object_dir, sizeof(object_dir), "%s/object", root);
    slotcask_shutdown();
    slotcask_init(64, 64);
    SlotcaskDb db;
    ASSERT_EQ_INT(slotcask_open(&db, object_dir, 8, 1, 256), 0,
                  "open unindexed sync-failure fixture");

    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&db, 0, "fast-update", 11,
                                              "old", 3, NULL, NULL), 0,
                  "seed fast-path update");
    SlotcaskUpsertOpts slow_opts = { .check_needs_old = 1 };
    ASSERT_EQ_INT(slotcask_upsert_with_hooks(&db, 0, "slow-update", 11,
                                              "old", 3, &slow_opts, NULL), 0,
                  "seed slow-path update");

    durability_test_msync_reset();
    durability_test_msync_fail_on_call(2, EIO);
    assert_kf_sync_failure_recovers(&db,
        slotcask_upsert_with_hooks(&db, 0, "fast-insert", 11, "value", 5,
                                   NULL, NULL),
        "fast-insert", 11, "value", 5,
        "fast new-key upsert recovers from keyfile sync failure via replay");

    durability_test_msync_fail_on_call(2, EIO);
    assert_kf_sync_failure_recovers(&db,
        slotcask_upsert_with_hooks(&db, 0, "fast-update", 11, "new", 3,
                                   NULL, NULL),
        "fast-update", 11, "new", 3,
        "fast existing-key upsert recovers from keyfile sync failure via replay");

    durability_test_msync_fail_on_call(2, EIO);
    assert_kf_sync_failure_recovers(&db,
        slotcask_upsert_with_hooks(&db, 0, "slow-insert", 11, "value", 5,
                                   &slow_opts, NULL),
        "slow-insert", 11, "value", 5,
        "slow new-key upsert recovers from keyfile sync failure via replay");

    durability_test_msync_fail_on_call(2, EIO);
    assert_kf_sync_failure_recovers(&db,
        slotcask_upsert_with_hooks(&db, 0, "slow-update", 11, "new", 3,
                                   &slow_opts, NULL),
        "slow-update", 11, "new", 3,
        "slow existing-key upsert recovers from keyfile sync failure via replay");

    durability_test_msync_fail_on_call(2, EIO);
    assert_kf_sync_failure_recovers(&db,
        slotcask_insert_with_hooks(&db, 0, "insert-only", 11, "value", 5,
                                   NULL, NULL),
        "insert-only", 11, "value", 5,
        "insert-only path recovers from keyfile sync failure via replay");

    slotcask_close(&db);
    slotcask_shutdown();
    rmrf(root);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-durability-sync-failures",
              test_durability_msync_injection_run)
TEST_REGISTER("test-unindexed-kf-sync-failure",
              test_unindexed_kf_sync_failure_propagates)

/* ───── Plan 2026-08-21 Task 1 — BULK_COMMIT_WINDOW knob parsing ───── */

static int write_window_fixture(const char *dir, const char *window) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/db.env", dir);
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "DB_ROOT=/tmp/ignored\nBULK_COMMIT_WINDOW=%s\n", window);
    return fclose(f);
}

static int test_bulk_commit_window_config_run(void) {
    char fixture[] = "/tmp/shard-db-win-config-XXXXXX";
    char cwd[PATH_MAX], parsed_root[PATH_MAX];
    char *cwd_ok = getcwd(cwd, sizeof(cwd));
    ASSERT_NOT_NULL(cwd_ok, "capture cwd for window config test");
    if (!cwd_ok) return 1;
    char *fixture_ok = mkdtemp(fixture);
    ASSERT_NOT_NULL(fixture_ok, "create window config fixture");
    if (!fixture_ok) return 1;
    int saved_window = g_db->bulk_commit_window;
    chdir(fixture);

    struct { const char *in; int want; int ok; } cases[] = {
        { "16",     16,    1 },
        { "1024",   1024,  1 },
        { "16384",  16384, 1 },
        { "512  ",  512,   1 },
        { "15",     0,     0 },
        { "16385",  0,     0 },
        { "-5",     0,     0 },
        { "12x",    0,     0 },
        { "",       0,     0 },
        { "999999999999999999999999", 0, 0 },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        g_db->bulk_commit_window = 777;
        ASSERT_EQ_INT(write_window_fixture(fixture, cases[i].in), 0,
                      "write window fixture");
        int prc = load_db_root(parsed_root, sizeof(parsed_root));
        if (cases[i].ok) {
            ASSERT_EQ_INT(prc, 0, "valid window parses");
            ASSERT_EQ_INT(g_db->bulk_commit_window, cases[i].want,
                          "valid window value applied");
        } else {
            ASSERT_EQ_INT(g_db->bulk_commit_window, 777,
                          "invalid window preserves prior setting");
        }
    }

    g_db->bulk_commit_window = saved_window;
    chdir(cwd);
    rmrf(fixture);
    return t_ctx->failed ? 1 : 0;
}

TEST_REGISTER("test-bulk-commit-window-config",
              test_bulk_commit_window_config_run)
