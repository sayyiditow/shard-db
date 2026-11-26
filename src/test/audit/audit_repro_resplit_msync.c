/* src/test/audit/audit_repro_resplit_msync.c
 *
 * Audit repro for AUD-KF-RESPLIT-SYNC (docs/plans/2026-11-25-audit-storage-
 * k-reentry.md, Phase 2 area 1 part 2). kfcache_resplit_locked ignores the
 * result of the Phase C msync(MS_SYNC) of the rebuilt keyfile and renames it
 * over the live one anyway. This case makes exactly that msync fail (EIO)
 * and asserts the contract every other sync in the engine follows: the
 * resplit is abandoned with the old keyfile still in place.
 *
 * Regression case for docs/plans/2026-11-25-fix-resplit-compact-sync.md (the
 * fix abandons the resplit). A second case fails the post-rename directory
 * fsync via the SHARD_TEST_BMFIX_KF_DIRSYNC seam and asserts no write is
 * acknowledged until the directory sync succeeds. Runs in the default suite.
 *
 * AUD-KF-DIRSYNC (docs/plans/2026-11-26-fix-restore-swap-and-kf-dirsync.md,
 * T4): the pending directory sync must survive a cache reacquire, an LRU
 * eviction and a prefix invalidation, so the block cannot be forgotten by a
 * drop that cannot refuse. Three cases below pin that (they do not use the
 * msync interposer, so they run under the sanitizers too).
 *
 * Mechanism: a test-binary msync() interposer (executable symbols win over
 * libc) that fails the first msync of a rebuilt keyfile. The resplit's
 * msync is identified by shape: MS_SYNC, length over two pages and not a
 * page multiple (per-slot syncs are page-aligned multiples). The interposer
 * itself is skipped under ASan/TSan, whose runtimes own msync.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "fixtures.h"
#include "shard_test_ctl.h"
#include "shard_db_internal.h"

#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>

extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);

static off_t rs_size(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 ? st.st_size : -1;
}

static int rs_upsert_shard(SlotcaskDb *db, int shard, uint64_t key) {
    char v[8] = "resplit";
    SlotcaskBulkRec rec;
    memset(&rec, 0, sizeof(rec));
    rec.key = &key; rec.klen = sizeof(key);
    rec.value = v;  rec.vlen = 7;
    SlotcaskBulkOpts o;
    memset(&o, 0, sizeof(o));
    return slotcask_bulk_upsert_in_kfshard(db, shard, &rec, 1, &o);
}

static int rs_upsert(SlotcaskDb *db, uint64_t key) {
    return rs_upsert_shard(db, 0, key);
}

#if !defined(__SANITIZE_ADDRESS__) && !defined(__SANITIZE_THREAD__)
static _Atomic int g_rs_arm;    /* fail the next big msync */
static _Atomic int g_rs_hits;   /* big msyncs failed */

int msync(void *addr, size_t len, int flags) {
    static int (*real)(void *, size_t, int);
    if (!real) real = (int (*)(void *, size_t, int))dlsym(RTLD_NEXT, "msync");
    if ((flags & MS_SYNC) && len > 8192 && (len % 4096) != 0 &&
        atomic_load(&g_rs_arm) &&
        atomic_exchange(&g_rs_arm, 0)) {
        atomic_fetch_add(&g_rs_hits, 1);
        errno = EIO;
        return -1;
    }
    return real(addr, len, flags);
}

static int audit_repro_resplit_msync_run(void) {
    SlotcaskDb db;
    char base[] = "/tmp/shard-db-rs-test-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", base);
    snprintf(k, sizeof(k), "%s/data/kf", base);
    ASSERT_TRUE(mkdir(d, 0755) == 0 && mkdir(k, 0755) == 0, "mkdir");
    slotcask_init(64, 64);
    shard_test_ctl_reset();
    atomic_store(&g_shard_test_kf_initial_slots, 256);
    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, base, 1, 1, 64), 0, "open db");
    if (t_ctx->failed) goto out;

    char kf[PATH_MAX];
    kf_path_for(kf, base, 0);
    atomic_store(&g_rs_hits, 0);
    atomic_store(&g_rs_arm, 1);
    off_t before = rs_size(kf);
    int inserted = 0;
    for (uint64_t key = 1; key <= 600 && atomic_load(&g_rs_hits) == 0; key++) {
        if (rs_upsert(&db, key) == 0) inserted++;
    }
    atomic_store(&g_rs_arm, 0);
    off_t after = rs_size(kf);

    ASSERT_EQ_INT(atomic_load(&g_rs_hits), 1,
                  "the rebuilt keyfile's msync was reached and failed once");
    char msg[200];
    snprintf(msg, sizeof(msg),
             "resplit abandoned after a failed keyfile msync (size before=%lld "
             "after=%lld, %d records inserted)",
             (long long)before, (long long)after, inserted);
    ASSERT_TRUE(after == before, msg);
out:
    slotcask_close(&db);
    {
        char cmd[PATH_MAX + 16];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
        if (system(cmd) != 0) { /* best effort */ }
    }
    slotcask_shutdown();
    shard_test_ctl_reset();
    return t_ctx->failed > 0 ? 1 : 0;
}
#else
static int audit_repro_resplit_msync_run(void) {
    /* Sanitizer runtimes own msync; this repro runs in the normal build. */
    return 0;
}
#endif

/* Rename done but directory fsync fails: the swap stays (it cannot be
   undone), and every write is refused until the directory sync is retried
   successfully. The refusal must survive a reacquire of the cache entry: a
   dropped flag would reopen the keyfile and acknowledge the write on a
   rename that may not survive power loss (AUD-KF-DIRSYNC). */
static int test_resplit_dirsync_gate(void) {
    slotcask_test_kf_dirsync_reset();
    SlotcaskDb db;
    char base[] = "/tmp/shard-db-rd-test-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", base);
    snprintf(k, sizeof(k), "%s/data/kf", base);
    ASSERT_TRUE(mkdir(d, 0755) == 0 && mkdir(k, 0755) == 0, "mkdir");
    slotcask_init(64, 64);
    shard_test_ctl_reset();
    atomic_store(&g_shard_test_kf_initial_slots, 256);
    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, base, 1, 1, 64), 0, "open db");
    if (t_ctx->failed) goto out;
    char kf[PATH_MAX];
    kf_path_for(kf, base, 0);
    off_t before = rs_size(kf);
    atomic_store(&g_shard_test_bmfix_fault_mode, SHARD_TEST_BMFIX_KF_DIRSYNC);
    atomic_store(&g_shard_test_bmfix_fault_count, 100000);   /* persistent: every directory sync fails */
    int refused = 0, ok_after = 0;
    uint64_t key = 1;
    for (; key <= 600; key++) {
        int rc = rs_upsert(&db, key);
        if (rs_size(kf) > before) {           /* the resplit happened */
            if (rc != 0) refused++;
            break;
        }
    }
    ASSERT_TRUE(rs_size(kf) > before, "the keyfile was resplit");

    /* Persistence across a cache reacquire: the identity recorded in Phase E
       matches the renamed inode, so no reopen clears the pending sync. */
    uint64_t opens = g_kf_open_file_call_count;
    int refused_after_reacquire = 0;
    for (int i = 0; i < 3; i++)
        if (rs_upsert(&db, 200000 + (uint64_t)i) != 0) refused_after_reacquire++;
    ASSERT_EQ_INT(refused_after_reacquire, 3,
                  "three further writes are refused while the dir sync fails");
    ASSERT_TRUE(g_kf_open_file_call_count == opens,
                "the refusal does not reopen the keyfile (Phase E identity)");

    atomic_store(&g_shard_test_bmfix_fault_mode, 0);
    for (int i = 0; i < 5; i++)
        if (rs_upsert(&db, 100000 + (uint64_t)i) == 0) ok_after++;
    ASSERT_TRUE(refused >= 1, "the write that follows an unsynced rename is not acknowledged");
    ASSERT_EQ_INT(ok_after, 5, "writes succeed once the directory sync works again");
out:
    slotcask_close(&db);
    {
        char cmd[PATH_MAX + 16];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
        if (system(cmd) != 0) { /* best effort */ }
    }
    slotcask_shutdown();
    shard_test_ctl_reset();
    slotcask_test_kf_dirsync_reset();
    return t_ctx->failed > 0 ? 1 : 0;
}

/* The pending directory sync survives LRU eviction of the keyfile's cache
   entry: a write to the evicted (then reacquired) shard is still refused. */
static int test_kf_dirsync_pending_survives_eviction(void) {
    slotcask_test_kf_dirsync_reset();
    SlotcaskDb db;
    char base[] = "/tmp/shard-db-rd-evict-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", base);
    snprintf(k, sizeof(k), "%s/data/kf", base);
    ASSERT_TRUE(mkdir(d, 0755) == 0 && mkdir(k, 0755) == 0, "mkdir");
    /* 128 cache slots, eviction from 64 residents. */
    slotcask_shutdown();
    slotcask_init(64, 64);
    shard_test_ctl_reset();
    atomic_store(&g_shard_test_kf_initial_slots, 256);
    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, base, 128, 1, 64), 0, "open db");
    if (t_ctx->failed) goto out;

    char kf0[PATH_MAX];
    kf_path_for(kf0, base, 0);
    off_t before = rs_size(kf0);
    atomic_store(&g_shard_test_bmfix_fault_mode, SHARD_TEST_BMFIX_KF_DIRSYNC);
    atomic_store(&g_shard_test_bmfix_fault_count, 100000);
    for (uint64_t key = 1; key <= 600; key++) {
        (void)rs_upsert(&db, key);
        if (rs_size(kf0) > before) break;     /* shard 0 was resplit */
    }
    ASSERT_TRUE(rs_size(kf0) > before, "shard 0 was resplit");

    /* Touch the other 127 shards until shard 0 is evicted. */
    int evicted = 0;
    for (int s = 1; s <= 127 && !evicted; s++) {
        (void)rs_upsert_shard(&db, s, 1000 + (uint64_t)s);
        evicted = !slotcask_test_kfcache_resident(kf0);
    }
    ASSERT_TRUE(evicted, "the keyfile's cache entry was evicted");

    ASSERT_TRUE(rs_upsert(&db, 300000) != 0,
                "the write after eviction is still refused");
    atomic_store(&g_shard_test_bmfix_fault_mode, 0);
    ASSERT_EQ_INT(rs_upsert(&db, 300001), 0,
                  "the write succeeds once the directory sync works again");
out:
    slotcask_close(&db);
    {
        char cmd[PATH_MAX + 16];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
        if (system(cmd) != 0) { /* best effort */ }
    }
    slotcask_shutdown();
    shard_test_ctl_reset();
    slotcask_test_kf_dirsync_reset();
    return t_ctx->failed > 0 ? 1 : 0;
}

/* The pending directory sync survives a prefix invalidation of the object's
   keyfile cache entries. */
static int test_kf_dirsync_pending_survives_invalidate(void) {
    slotcask_test_kf_dirsync_reset();
    SlotcaskDb db;
    char base[] = "/tmp/shard-db-rd-inval-XXXXXX";
    ASSERT_TRUE(mkdtemp(base) != NULL, "mkdtemp");
    if (t_ctx->failed) return 1;
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", base);
    snprintf(k, sizeof(k), "%s/data/kf", base);
    ASSERT_TRUE(mkdir(d, 0755) == 0 && mkdir(k, 0755) == 0, "mkdir");
    slotcask_init(64, 64);
    shard_test_ctl_reset();
    atomic_store(&g_shard_test_kf_initial_slots, 256);
    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, base, 1, 1, 64), 0, "open db");
    if (t_ctx->failed) goto out;

    char kf0[PATH_MAX];
    kf_path_for(kf0, base, 0);
    off_t before = rs_size(kf0);
    atomic_store(&g_shard_test_bmfix_fault_mode, SHARD_TEST_BMFIX_KF_DIRSYNC);
    atomic_store(&g_shard_test_bmfix_fault_count, 100000);
    for (uint64_t key = 1; key <= 600; key++) {
        (void)rs_upsert(&db, key);
        if (rs_size(kf0) > before) break;
    }
    ASSERT_TRUE(rs_size(kf0) > before, "the keyfile was resplit");

    char parent[PATH_MAX], object[PATH_MAX];
    snprintf(parent, sizeof(parent), "%s", base);
    char *slash = strrchr(parent, '/');
    ASSERT_NOT_NULL(slash, "split the temporary path");
    if (slash) {
        snprintf(object, sizeof(object), "%s", slash + 1);
        *slash = '\0';
        slotcask_registry_invalidate(parent, object);
    }
    ASSERT_TRUE(!slotcask_test_kfcache_resident(kf0),
                "the invalidation dropped the cache entry");

    ASSERT_TRUE(rs_upsert(&db, 300000) != 0,
                "the write after invalidation is still refused");
    atomic_store(&g_shard_test_bmfix_fault_mode, 0);
    ASSERT_EQ_INT(rs_upsert(&db, 300001), 0,
                  "the write succeeds once the directory sync works again");
out:
    slotcask_close(&db);
    {
        char cmd[PATH_MAX + 16];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
        if (system(cmd) != 0) { /* best effort */ }
    }
    slotcask_shutdown();
    shard_test_ctl_reset();
    slotcask_test_kf_dirsync_reset();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-resplit-msync-failure", audit_repro_resplit_msync_run)
TEST_REGISTER("test-resplit-dirsync-gate", test_resplit_dirsync_gate)
TEST_REGISTER("test-kf-dirsync-pending-survives-eviction",
              test_kf_dirsync_pending_survives_eviction)
TEST_REGISTER("test-kf-dirsync-pending-survives-invalidate",
              test_kf_dirsync_pending_survives_invalidate)
