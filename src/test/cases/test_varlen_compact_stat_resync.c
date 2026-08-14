#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include "varlen_compact_fixture.h"
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

static int test_varlen_compact_stat_resync_run(void) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-varlen-compact-stat-%d", (int)getpid());
    mkdir(tmpdir, 0755);

    slotcask_init(64, 64);
    slotcask_test_set_seg_max_bytes(VARLEN_FIXTURE_TEST_SEG_BYTES);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, tmpdir, 8, 1, 8192); /* single stream: deterministic file layout */
    ASSERT_EQ_INT(ret, 0, "slotcask_open");
    ret = varlen_compact_fixture_build(&db);
    ASSERT_EQ_INT(ret, 0, "build rotated donor/recipient fixture");
    int dropped = 0;
    ret = slotcask_compact_segs(&db, &dropped);
    ASSERT_EQ_INT(ret, 0, "compact_segs completes without an error");

    void *val_out;
    size_t vlen_out;
    ret = slotcask_get(&db, "kkeyC", 5, &val_out, &vlen_out);
    ASSERT_EQ_INT(ret, 0, "get C after compaction");
    if (ret == 0) {
        ASSERT_EQ_INT((int)vlen_out, 6, "C vlen intact");
        ASSERT_TRUE(memcmp(val_out, "cvalue", 6) == 0, "C value intact");
        free(val_out);
    }

    slotcask_close(&db);
    slotcask_test_set_seg_max_bytes(0);
    slotcask_shutdown();

    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", tmpdir);
    system(rmcmd);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-compact-stat-resync", test_varlen_compact_stat_resync_run)
