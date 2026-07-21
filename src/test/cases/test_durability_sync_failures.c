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

static int write_config_fixture(const char *dir, const char *durability,
                                const char *warmup) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/db.env", dir);
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "DB_ROOT=/tmp/ignored\nDURABILITY_SYNC_MS=%s\n", durability);
    if (warmup) fprintf(f, "WARMUP=%s\n", warmup);
    return fclose(f);
}

static void test_durability_config_parser(void) {
    char fixture[] = "/tmp/shard-db-durability-config-XXXXXX";
    char cwd[PATH_MAX], saved_root[PATH_MAX], parsed_root[PATH_MAX];
    char *cwd_ok = getcwd(cwd, sizeof(cwd));
    ASSERT_NOT_NULL(cwd_ok, "capture cwd for config parser test");
    if (!cwd_ok) return;
    char *fixture_ok = mkdtemp(fixture);
    ASSERT_NOT_NULL(fixture_ok, "create durability config fixture");
    if (!fixture_ok) return;
    snprintf(saved_root, sizeof(saved_root), "%s", g_db_root);
    int saved_durability = g_durability_sync_ms;
    int saved_warmup_explicit = g_db->warmup_explicit;
    char saved_warmup[sizeof(g_warmup_mode)];
    snprintf(saved_warmup, sizeof(saved_warmup), "%s", g_warmup_mode);
    chdir(fixture);

    g_durability_sync_ms = 777;
    ASSERT_EQ_INT(write_config_fixture(fixture, "0", "off"), 0,
                  "write valid disabled durability config");
    ASSERT_EQ_INT(load_db_root(parsed_root, sizeof(parsed_root)), 0,
                  "parse valid disabled durability config");
    ASSERT_EQ_INT(g_durability_sync_ms, 0, "DURABILITY_SYNC_MS=0 disables sync");
    ASSERT_EQ_INT(g_db->warmup_explicit, 1, "valid WARMUP marks setting explicit");

    g_db->warmup_explicit = 0;
    g_durability_sync_ms = 777;
    ASSERT_EQ_INT(write_config_fixture(fixture, "50   ", "bogus"), 0,
                  "write floor durability config");
    ASSERT_EQ_INT(load_db_root(parsed_root, sizeof(parsed_root)), 0,
                  "parse durability floor with trailing whitespace");
    ASSERT_EQ_INT(g_durability_sync_ms, 50, "50ms durability floor is accepted");
    ASSERT_EQ_INT(g_db->warmup_explicit, 0,
                  "invalid WARMUP does not mark setting explicit");

    const char *invalid[] = { "-1", "1", "49", "12x", "", 
                              "999999999999999999999999" };
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        g_durability_sync_ms = 777;
        ASSERT_EQ_INT(write_config_fixture(fixture, invalid[i], NULL), 0,
                      "write invalid durability config");
        load_db_root(parsed_root, sizeof(parsed_root));
        ASSERT_EQ_INT(g_durability_sync_ms, 777,
                      "invalid durability value preserves prior setting");
    }

    snprintf(g_db_root, sizeof(g_db_root), "%s", saved_root);
    g_durability_sync_ms = saved_durability;
    g_db->warmup_explicit = saved_warmup_explicit;
    snprintf(g_warmup_mode, sizeof(g_warmup_mode), "%s", saved_warmup);
    chdir(cwd);
    rmrf(fixture);
}

static int test_durability_msync_injection_run(void) {
    test_durability_config_parser();

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
        DurabilitySyncStats stats = {0};
        durability_test_sync_one_pass(g_db, 1000, &stats);
        ASSERT_EQ_INT(stats.failed, 1,
                      "sweep reports injected keyfile sync failure");
        ASSERT_EQ_INT(stats.kf_synced, 0,
                      "failed keyfile sync is not reported as synced");
        ASSERT_EQ_INT(atomic_load(&g_kfcache[dirty_slot].dirty), 1,
                      "failed sweep restores keyfile dirty state");
        ASSERT_TRUE(atomic_load(&g_kfcache[dirty_slot].dirty_since_ms)
                        <= first_dirty_ms,
                    "failed sweep preserves earliest dirty timestamp");

        memset(&stats, 0, sizeof(stats));
        durability_test_sync_one_pass(g_db, 1000, &stats);
        ASSERT_EQ_INT(stats.failed, 0,
                      "retrying keyfile sync has no failure");
        ASSERT_EQ_INT(stats.kf_synced, 1,
                      "successful retry reports one keyfile sync");
        ASSERT_EQ_INT(atomic_load(&g_kfcache[dirty_slot].dirty), 0,
                      "successful retry clears keyfile dirty state");
    }

    char object_dir[PATH_MAX];
    snprintf(object_dir, sizeof(object_dir), "%s/object", cache_dir);
    SlotcaskDb sdb;
    ASSERT_EQ_INT(slotcask_open(&sdb, object_dir, 8, 1, 256), 0,
                  "open slotcask dirty-tracking fixture");
    ASSERT_EQ_INT(slotcask_insert(&sdb, 0, "key", 3, "value", 5), 0,
                  "insert mutates cached segment and keyfile mappings");
    int dirty_segments = 0;
    for (int i = 0; i < g_segcache_slots; i++) {
        if (__atomic_load_n(&g_segcache[i].used, __ATOMIC_ACQUIRE) &&
            atomic_load(&g_segcache[i].dirty)) {
            dirty_segments++;
        }
    }
    ASSERT_TRUE(dirty_segments > 0,
                "slotcask insert marks its cached segment dirty");
    DurabilitySyncStats segment_stats = {0};
    durability_test_sync_one_pass(g_db, 1000, &segment_stats);
    ASSERT_TRUE(segment_stats.seg_synced > 0,
                "sweep synchronizes the dirty segment mapping");
    slotcask_close(&sdb);

    char bt_path[PATH_MAX];
    snprintf(bt_path, sizeof(bt_path), "%s/value.idx", cache_dir);
    uint8_t hash[16] = {0};
    btree_insert(bt_path, "value", 5, hash);
    int dirty_btrees = 0;
    for (int i = 0; i < bt_cache_slots; i++) {
        if (__atomic_load_n(&bt_cache[i].used, __ATOMIC_ACQUIRE) &&
            atomic_load(&bt_cache[i].dirty)) {
            dirty_btrees++;
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
    for (int i = 0; i < g_bm_cache_slots; i++) {
        if (__atomic_load_n(&g_bm_cache[i].used, __ATOMIC_ACQUIRE) &&
            atomic_load(&g_bm_cache[i].dirty)) {
            dirty_bitmaps++;
        }
    }
    ASSERT_TRUE(dirty_bitmaps > 0, "bitmap writer close marks slot dirty");

    DurabilitySyncStats index_stats = {0};
    durability_test_sync_one_pass(g_db, 1000, &index_stats);
    ASSERT_TRUE(index_stats.bt_synced > 0,
                "sweep synchronizes the dirty btree mapping");
    ASSERT_TRUE(index_stats.bm_synced > 0,
                "sweep synchronizes the dirty bitmap mapping");

    slotcask_shutdown();
    rmrf(cache_dir);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-durability-sync-failures",
              test_durability_msync_injection_run)
