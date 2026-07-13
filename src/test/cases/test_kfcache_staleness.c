#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include <string.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <unistd.h>

/* Regression test for the test-rebuild-recovery flakiness root cause:
   kfcache_acquire used to be a pure path-string-keyed cache with no check
   that a cache hit still refers to the file currently at that path.
   rebuild_object_v2 renames an object's data/ directory away and recreates
   a fresh one at the same path; if some other thread's
   kfcache_acquire(writer=1) call (e.g. the warmup subsystem's
   slotcask_open() fan-out) raced in and (re)installed a cache entry for a
   shard's kf path between slotcask_registry_invalidate() and the rename,
   that stale entry survived the rename and got served back to the
   rebuild's own reopen at the identical path — aliasing the OLD object's
   data into the freshly-rebuilt one and causing phantom duplicate-key
   rejections during the walk (test-rebuild-recovery's flaky
   ">=99 records survive rebuild" assertion).

   This test reproduces the aliasing directly: install a cache entry for a
   kf path, rename the file away (simulating the rebuild's rename), then
   acquire the same path again. Before the fix: returns the stale cached
   header (total=42, wrong — the bug). After the fix: kfcache_acquire
   detects the cached entry no longer matches the file at `path` and
   re-opens fresh (total=0, correct). */
static int test_kfcache_staleness_run(void) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-kfcache-staleness-%d", getpid());
    mkdir(tmpdir, 0755);

    slotcask_init(64, 64);

    char path[300];
    snprintf(path, sizeof(path), "%s/000.kf", tmpdir);
    char legacy[300];
    snprintf(legacy, sizeof(legacy), "%s/000.kf.legacy", tmpdir);

    SlotcaskKfHandle h1;
    int ret = kfcache_acquire(&h1, path, 8, 1);
    ASSERT_EQ_INT(ret, 0, "first acquire creates+caches kf file");
    h1.hdr->total = 42;
    kfcache_release(&h1);

    /* Simulate rebuild_object_v2's rename(data_dir, legacy_dir): the file
       backing the cached entry moves away. Nothing invalidates the cache
       here — this models a racing installer's entry surviving the
       rename, exactly as slotcask_open()'s per-shard kfcache_acquire
       fan-out can when it lands between slotcask_registry_invalidate and
       the rename. */
    ret = rename(path, legacy);
    ASSERT_EQ_INT(ret, 0, "rename kf file away, simulating rebuild's data->legacy rename");

    /* new_db's own reopen at the identical path, as rebuild_object_v2
       does right after the rename. Must NOT alias the old (renamed-away)
       file's contents — it must see a fresh, empty header. */
    SlotcaskKfHandle h2;
    ret = kfcache_acquire(&h2, path, 8, 1);
    ASSERT_EQ_INT(ret, 0, "second acquire after rename succeeds");
    ASSERT_EQ_INT((int)h2.hdr->total, 0,
        "second acquire must see a fresh file (total=0), not alias the renamed-away file's total=42");
    kfcache_release(&h2);

    slotcask_shutdown();
    system("rm -rf /tmp/shard-db-kfcache-staleness-*");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-kfcache-staleness", test_kfcache_staleness_run)
