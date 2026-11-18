/* Regression: free pool must drop slots consumed by compaction —
 * docs/plans/2026-11-17-kf-grave-frontier-fix.md, Task 9 (M3).
 *
 * At open the free pool is rebuilt from the recipient segment's kf
 * tombstones. varlen_compact_cb migrates a donor record into such a
 * tombstone slot but (base) never removes the slot from the pool, so a
 * later insert pops it and overwrites the migrated record.
 *
 * Stock varlen_compact_fixture_build, reopen, compact, 12 inserts: kA
 * and kkeyC must still read back and validate_live_refs must stay 0.
 * On base: kA/kkeyC are overwritten by pool-popped inserts and
 * validate reports 1.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include "varlen_compact_fixture.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define N_NEW_KEYS 12

static int test_slotcask_compact_pool_consumed_run(void) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-compact-pool-%d",
             (int)getpid());
    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", tmpdir);
    system(rmcmd);
    mkdir(tmpdir, 0755);

    slotcask_init(64, 64);
    slotcask_test_set_seg_max_bytes(VARLEN_FIXTURE_TEST_SEG_BYTES);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, tmpdir, 8, 1, 8192);
    ASSERT_EQ_INT(ret, 0, "slotcask_open");
    ret = varlen_compact_fixture_build(&db);
    ASSERT_EQ_INT(ret, 0, "build stock rotated fixture");
    slotcask_close(&db);

    /* Reopen: the free pool is rebuilt from kf tombstones — including
       the recipient slots the compaction is about to consume. */
    slotcask_test_set_seg_max_bytes(VARLEN_FIXTURE_TEST_SEG_BYTES);
    memset(&db, 0, sizeof(db));
    ret = slotcask_open(&db, tmpdir, 8, 1, 8192);
    ASSERT_EQ_INT(ret, 0, "reopen");

    /* A pending KFM2 marker means a flag=0 record may be live-pending
       (replayed later from that exact slot): compaction must not reuse
       flag=0 space, so it skips the object entirely. */
    char kfdir[300], mpath[400];
    snprintf(kfdir, sizeof(kfdir), "%s/data/kf", tmpdir);
    snprintf(mpath, sizeof(mpath), "%s/000_batch_1_0000000000000001_marker.dat",
             kfdir);
    {
        FILE *mf = fopen(mpath, "w");
        ASSERT_NOT_NULL(mf, "create placeholder pending marker");
        if (mf) fclose(mf);
    }
    int dropped = 0;
    ret = slotcask_compact_segs(&db, &dropped);
    ASSERT_EQ_INT(ret, 0, "compact_segs with pending marker completes");
    ASSERT_EQ_INT(dropped, 0, "pending marker: compaction skipped, no file dropped");
    ASSERT_EQ_INT(unlink(mpath), 0, "remove placeholder marker");

    ret = slotcask_compact_segs(&db, &dropped);
    ASSERT_EQ_INT(ret, 0, "compact_segs completes");
    ASSERT_TRUE(dropped > 0, "no pending marker: compaction drops the donor");

    char val[100];
    memset(val, 'n', sizeof(val));
    static char new_keys[N_NEW_KEYS][32];
    for (int i = 0; i < N_NEW_KEYS; i++) {
        snprintf(new_keys[i], 32, "pc-%02d-0123456789", i);
        ASSERT_EQ_INT(slotcask_insert_with_hooks(&db, 0, new_keys[i],
                                      strlen(new_keys[i]), val, sizeof(val),
                                      NULL, NULL), 0,
                      "post-compaction insert");
    }

    /* The migrated records must survive the post-compaction inserts. */
    void *val_out = NULL;
    size_t vlen_out = 0;
    ret = slotcask_get(&db, "kA", 2, &val_out, &vlen_out);
    ASSERT_EQ_INT(ret, 0, "kA survives post-compaction inserts");
    if (ret == 0) {
        ASSERT_EQ_INT((int)vlen_out, 1, "kA vlen intact");
        ASSERT_TRUE(memcmp(val_out, "v", 1) == 0, "kA value intact");
        free(val_out);
        val_out = NULL;
    }
    ret = slotcask_get(&db, "kkeyC", 5, &val_out, &vlen_out);
    ASSERT_EQ_INT(ret, 0, "kkeyC survives post-compaction inserts");
    if (ret == 0) {
        ASSERT_EQ_INT((int)vlen_out, 6, "kkeyC vlen intact");
        ASSERT_TRUE(memcmp(val_out, "cvalue", 6) == 0, "kkeyC value intact");
        free(val_out);
        val_out = NULL;
    }
    for (int i = 0; i < N_NEW_KEYS; i++) {
        ret = slotcask_get(&db, new_keys[i], strlen(new_keys[i]),
                           &val_out, &vlen_out);
        ASSERT_EQ_INT(ret, 0, "new key reads back");
        if (ret == 0) {
            ASSERT_TRUE(vlen_out == sizeof(val) &&
                        memcmp(val_out, val, sizeof(val)) == 0,
                        "new key value intact");
            free(val_out);
            val_out = NULL;
        }
    }

    uint64_t inv = 0;
    ASSERT_EQ_INT(slotcask_validate_live_refs(&db, &inv), 0,
                  "refs valid after post-compaction inserts");
    ASSERT_EQ_INT((int)inv, 0, "no invalid refs after post-compaction inserts");

    slotcask_close(&db);
    slotcask_test_set_seg_max_bytes(0);
    slotcask_shutdown();
    system(rmcmd);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-compact-pool-consumed",
              test_slotcask_compact_pool_consumed_run)
