#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "bitmap.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    ShardDb *db;
    const char *path;
    _Atomic int done;
    int rc;
    int slot;
} KfWriterThreadCtx;

static void *blocked_kf_writer(void *arg) {
    KfWriterThreadCtx *ctx = (KfWriterThreadCtx *)arg;
    g_db = ctx->db;
    SlotcaskKfHandle h;
    ctx->rc = kfcache_acquire(&h, ctx->path, 8, 1);
    ctx->slot = ctx->rc == 0 ? h.slot : -1;
    if (ctx->rc == 0) kfcache_release(&h);
    atomic_store_explicit(&ctx->done, 1, memory_order_release);
    return NULL;
}

static int create_kf_reader_fixture(const char *path) {
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    size_t len = SLOTCASK_KF_HDR_SIZE + 8 * sizeof(SlotcaskKfEntry);
    int rc = ftruncate(fd, (off_t)len);
    if (rc == 0) {
        SlotcaskKfHeader hdr = {
            .magic = SLOTCASK_KF_MAGIC,
            .version = SLOTCASK_KF_VERSION,
        };
        if (pwrite(fd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) rc = -1;
    }
    close(fd);
    return rc;
}

static void run_bitmap_holder_child(const char *dir) {
    bm_cache_shutdown();
    bm_cache_init(16);

    char held_path[PATH_MAX];
    snprintf(held_path, sizeof(held_path), "%s/held.bm", dir);
    BitmapShard *seed = bm_open(held_path, 64, 1, 0, 0, 1);
    if (!seed) _exit(2);
    if (bm_set(seed, (const uint8_t *)"held", 4, 3) != 0) _exit(3);
    bm_close(seed);

    BitmapShard *held = bm_open(held_path, 64, 0, 0, 0, 0);
    if (!held) _exit(4);
    for (int i = 0; i < 16; i++) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/fill-%02d.bm", dir, i);
        BitmapShard *fill = bm_open(path, 64, 1, 0, 0, 1);
        if (!fill) _exit(5);
        if (bm_set(fill, (const uint8_t *)"fill", 4, (uint32_t)i) != 0)
            _exit(6);
        bm_close(fill);
    }
    if (bm_test(held, (const uint8_t *)"held", 4, 3) != 1) _exit(7);
    bm_close(held);
    _exit(0);
}

static int wait_child_bounded(pid_t pid, int *status, int timeout_ms) {
    int waited_ms = 0;
    while (waited_ms < timeout_ms) {
        pid_t rc = waitpid(pid, status, WNOHANG);
        if (rc == pid) return 1;
        if (rc < 0) return -1;
        usleep(10000);
        waited_ms += 10;
    }
    kill(pid, SIGKILL);
    waitpid(pid, status, 0);
    return 0;
}

static int find_kf_slot(const char *path) {
    for (int i = 0; i < g_kfcache_slots; i++) {
        if (__atomic_load_n(&g_kfcache[i].used, __ATOMIC_ACQUIRE) &&
            strcmp(g_kfcache[i].path, path) == 0) {
            return i;
        }
    }
    return -1;
}

static int open_release_kf(const char *path) {
    SlotcaskKfHandle h;
    if (kfcache_acquire(&h, path, 8, 1) != 0) return -1;
    kfcache_release(&h);
    return 0;
}

static int test_durability_cache_paths_run(void) {
    char dir[] = "/tmp/shard-db-durability-cache-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(dir), "create cache-path fixture");

    slotcask_shutdown();
    slotcask_init(16, 16);

    char paths[21][PATH_MAX];
    for (int i = 0; i < 16; i++) {
        snprintf(paths[i], sizeof(paths[i]), "%s/%02d.kf", dir, i);
        ASSERT_EQ_INT(open_release_kf(paths[i]), 0,
                      "populate tracked keyfile cache slot");
    }

    DurabilitySyncStats stats = {0};
    durability_test_sync_one_pass(g_db, 1000, &stats);
    int oldest_slot = find_kf_slot(paths[0]);
    ASSERT_TRUE(oldest_slot >= 0, "oldest keyfile entry is cached");
    if (oldest_slot < 0) {
        slotcask_shutdown();
        rmrf(dir);
        return 1;
    }
    durability_mark_dirty(&g_kfcache[oldest_slot].dirty,
                          &g_kfcache[oldest_slot].dirty_since_ms);

    durability_test_msync_reset();
    durability_test_msync_fail_next(1, EIO);
    snprintf(paths[16], sizeof(paths[16]), "%s/16.kf", dir);
    SlotcaskKfHandle first_replacement;
    ASSERT_EQ_INT(kfcache_acquire(&first_replacement, paths[16], 8, 1), 0,
                  "writer uses another victim after dirty sync failure");
    ASSERT_TRUE(first_replacement.slot >= 0,
                "replacement writer remains tracked after victim failure");
    int succeeded = -1;
    int failed = -1;
    durability_test_msync_counts(&succeeded, &failed);
    ASSERT_EQ_INT(failed, 1, "dirty eviction uses synchronous durability helper");
    ASSERT_TRUE(find_kf_slot(paths[0]) >= 0,
                "failed dirty eviction retains original cache entry");
    ASSERT_EQ_INT(atomic_load(&g_kfcache[oldest_slot].dirty), 1,
                  "failed dirty eviction restores dirty state");
    kfcache_release(&first_replacement);

    durability_test_msync_reset();
    snprintf(paths[17], sizeof(paths[17]), "%s/17.kf", dir);
    SlotcaskKfHandle second_replacement;
    ASSERT_EQ_INT(kfcache_acquire(&second_replacement, paths[17], 8, 1), 0,
                  "writer evicts dirty entry after successful sync");
    ASSERT_TRUE(second_replacement.slot >= 0,
                "successful dirty eviction installs tracked replacement");
    if (second_replacement.slot >= 0) {
        ASSERT_EQ_INT(atomic_load(&g_kfcache[second_replacement.slot].dirty), 0,
                      "reused cache slot begins clean before writer release");
        ASSERT_EQ_INT(atomic_load(
                          &g_kfcache[second_replacement.slot].dirty_since_ms),
                      0, "reused cache slot clears prior dirty timestamp");
    }
    durability_test_msync_counts(&succeeded, &failed);
    ASSERT_EQ_INT(succeeded, 1,
                  "successful dirty eviction completes blocking sync");
    ASSERT_EQ_INT(failed, 0, "successful dirty eviction has no sync failure");
    ASSERT_EQ_INT(find_kf_slot(paths[0]), -1,
                  "successfully synced dirty victim is replaced");
    kfcache_release(&second_replacement);

    int expected_failures = 0;
    for (int i = 0; i < g_kfcache_slots; i++) {
        if (__atomic_load_n(&g_kfcache[i].used, __ATOMIC_ACQUIRE)) {
            durability_mark_dirty(&g_kfcache[i].dirty,
                                  &g_kfcache[i].dirty_since_ms);
            expected_failures++;
        }
    }
    durability_test_msync_reset();
    durability_test_msync_fail_next(expected_failures, EIO);
    snprintf(paths[18], sizeof(paths[18]), "%s/18.kf", dir);
    SlotcaskKfHandle failed_replacement;
    errno = 0;
    ASSERT_EQ_INT(kfcache_acquire(&failed_replacement, paths[18], 8, 1), -1,
                  "writer returns after every dirty victim fails sync");
    ASSERT_EQ_INT(errno, EIO,
                  "all-victim sync failure preserves underlying EIO");
    ASSERT_TRUE(failed_replacement.slot < 0 && failed_replacement.hdr == NULL,
                "failed writer never receives an untracked mapping");
    durability_test_msync_counts(&succeeded, &failed);
    ASSERT_EQ_INT(failed, expected_failures,
                  "writer tries each distinct dirty victim before failing");

    durability_test_msync_reset();
    memset(&stats, 0, sizeof(stats));
    durability_test_sync_one_pass(g_db, 1000, &stats);

    int held_slots[64];
    int held_count = 0;
    for (int i = 0; i < g_kfcache_slots; i++) {
        if (__atomic_load_n(&g_kfcache[i].used, __ATOMIC_ACQUIRE)) {
            pthread_rwlock_rdlock(&g_kfcache[i].rwlock);
            held_slots[held_count++] = i;
        }
    }
    snprintf(paths[19], sizeof(paths[19]), "%s/19.kf", dir);
    KfWriterThreadCtx writer_ctx = {
        .db = g_db,
        .path = paths[19],
        .done = 0,
        .rc = -99,
        .slot = -1,
    };
    pthread_t writer_tid;
    ASSERT_EQ_INT(pthread_create(&writer_tid, NULL, blocked_kf_writer,
                                 &writer_ctx),
                  0, "start required-cache writer under full contention");
    usleep(100000);
    ASSERT_EQ_INT(atomic_load_explicit(&writer_ctx.done, memory_order_acquire),
                  0, "required-cache writer remains blocked while all slots are held");

    snprintf(paths[20], sizeof(paths[20]), "%s/20.kf", dir);
    ASSERT_EQ_INT(create_kf_reader_fixture(paths[20]), 0,
                  "create read-only uncached-fallback fixture");
    SlotcaskKfHandle reader;
    ASSERT_EQ_INT(kfcache_acquire(&reader, paths[20], 8, 0), 0,
                  "reader can acquire while every cache slot is held");
    ASSERT_EQ_INT(reader.slot, -1,
                  "reader retains uncached fallback under contention");
    kfcache_release(&reader);

    for (int i = 0; i < held_count; i++) {
        pthread_rwlock_unlock(&g_kfcache[held_slots[i]].rwlock);
    }
    ASSERT_EQ_INT(pthread_join(writer_tid, NULL), 0,
                  "join writer after releasing contended slots");
    ASSERT_EQ_INT(atomic_load_explicit(&writer_ctx.done, memory_order_acquire),
                  1, "required-cache writer completes after holder release");
    ASSERT_EQ_INT(writer_ctx.rc, 0,
                  "required-cache writer succeeds after contention clears");
    ASSERT_TRUE(writer_ctx.slot >= 0,
                "unblocked writer receives a tracked cache slot");

    char identity_seg[PATH_MAX];
    snprintf(identity_seg, sizeof(identity_seg), "%s/identity.dat", dir);
    SlotcaskSegHandle identity_seed;
    ASSERT_EQ_INT(segcache_acquire(&identity_seed, identity_seg, 1, 0, 1), 0,
                  "seed tracked segment for identity-race retry test");
    segcache_release(&identity_seed);
    segcache_test_force_identity_mismatches(5);
    SlotcaskSegHandle identity_retry;
    ASSERT_EQ_INT(segcache_acquire(&identity_retry, identity_seg, 0, 0, 1), 0,
                  "segment must-cache acquire survives five identity races");
    ASSERT_TRUE(identity_retry.slot >= 0,
                "identity-race retry eventually returns tracked segment slot");
    ASSERT_EQ_INT(segcache_test_identity_mismatches_remaining(), 0,
                  "segment acquire consumes every forced identity race");
    segcache_release(&identity_retry);

    pid_t bitmap_pid = fork();
    ASSERT_TRUE(bitmap_pid >= 0, "fork bitmap holder-vs-LRU regression");
    if (bitmap_pid == 0) run_bitmap_holder_child(dir);
    if (bitmap_pid > 0) {
        int bitmap_status = 0;
        int bitmap_finished = wait_child_bounded(bitmap_pid, &bitmap_status, 3000);
        ASSERT_EQ_INT(bitmap_finished, 1,
                      "bitmap LRU chooses another victim instead of blocking on holder");
        ASSERT_TRUE(WIFEXITED(bitmap_status) && WEXITSTATUS(bitmap_status) == 0,
                    "held bitmap remains mapped and readable through LRU pressure");
    }

    slotcask_shutdown();

    char disabled_kf[PATH_MAX];
    snprintf(disabled_kf, sizeof(disabled_kf), "%s/disabled.kf", dir);
    SlotcaskKfHandle disabled_kh;
    errno = 0;
    ASSERT_EQ_INT(kfcache_acquire(&disabled_kh, disabled_kf, 8, 1), -1,
                  "keyfile writer fails when required cache is disabled");
    ASSERT_EQ_INT(errno, ENODEV,
                  "disabled keyfile writer reports ENODEV");
    ASSERT_TRUE(access(disabled_kf, F_OK) != 0,
                "disabled keyfile writer does not create a mapping file");

    char disabled_seg[PATH_MAX];
    snprintf(disabled_seg, sizeof(disabled_seg), "%s/disabled.dat", dir);
    SlotcaskSegHandle disabled_sh;
    errno = 0;
    ASSERT_EQ_INT(segcache_acquire(&disabled_sh, disabled_seg, 1, 0, 1), -1,
                  "segment must-cache writer fails when cache is disabled");
    ASSERT_EQ_INT(errno, ENODEV,
                  "disabled segment must-cache writer reports ENODEV");
    ASSERT_TRUE(access(disabled_seg, F_OK) != 0,
                "disabled segment writer does not create a mapping file");

    bm_cache_shutdown();
    char disabled_bm[PATH_MAX];
    snprintf(disabled_bm, sizeof(disabled_bm), "%s/disabled.bm", dir);
    errno = 0;
    BitmapShard *disabled_bitmap =
        bm_open(disabled_bm, 64, 1, 0, 0, 1);
    ASSERT_TRUE(disabled_bitmap == NULL,
                "bitmap writer fails when required cache is disabled");
    ASSERT_EQ_INT(errno, ENODEV,
                  "disabled bitmap writer reports ENODEV");
    ASSERT_TRUE(access(disabled_bm, F_OK) != 0,
                "disabled bitmap writer does not create a mapping file");

    rmrf(dir);
    durability_test_msync_reset();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-durability-sync-cache-paths",
              test_durability_cache_paths_run)
