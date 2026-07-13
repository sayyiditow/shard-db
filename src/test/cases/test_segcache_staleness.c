#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include <string.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <unistd.h>

/* Mirrors test_kfcache_staleness.c for segcache_acquire: segments are
   populated by the same class of rebuild-rename race as kf shards (see
   test_kfcache_staleness.c's comment for the full mechanism). A stale
   segcache hit during a rebuild walk is a *more* severe variant of the
   bug than the kf case — instead of merely rejecting a duplicate key, it
   would write freshly-inserted records into the wrong, about-to-be
   -orphaned segment file, silently losing them. */
static int test_segcache_staleness_run(void) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-segcache-staleness-%d", getpid());
    mkdir(tmpdir, 0755);

    slotcask_init(64, 64);

    char path[300];
    snprintf(path, sizeof(path), "%s/000000.dat", tmpdir);
    char legacy[300];
    snprintf(legacy, sizeof(legacy), "%s/000000.dat.legacy", tmpdir);

    SlotcaskSegHandle s1;
    int ret = segcache_acquire(&s1, path, 1, 1);
    ASSERT_EQ_INT(ret, 0, "first acquire creates+caches segment file");
    s1.map[0] = 0xAB;
    segcache_release(&s1);

    /* Simulate rebuild_object_v2's rename(data_dir, legacy_dir). */
    ret = rename(path, legacy);
    ASSERT_EQ_INT(ret, 0, "rename segment file away, simulating rebuild's rename");

    SlotcaskSegHandle s2;
    ret = segcache_acquire(&s2, path, 1, 1);
    ASSERT_EQ_INT(ret, 0, "second acquire after rename succeeds");
    ASSERT_EQ_INT((int)s2.map[0], 0,
        "second acquire must see a fresh segment (byte0=0), not alias the renamed-away file's byte0=0xAB");
    segcache_release(&s2);

    slotcask_shutdown();
    system("rm -rf /tmp/shard-db-segcache-staleness-*");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-segcache-staleness", test_segcache_staleness_run)
