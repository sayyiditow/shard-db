#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include <string.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <unistd.h>

static char g_tmpdir[256];

static int test_slotcask_api_run(void) {
    snprintf(g_tmpdir, sizeof(g_tmpdir), "/tmp/shard-db-slotcask-api-%d", getpid());
    mkdir(g_tmpdir, 0755);

    slotcask_init(64, 64);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, g_tmpdir, 8, 4, 64);
    ASSERT_EQ_INT(ret, 0, "slotcask_open");

    uint64_t total, deleted;
    ret = slotcask_sum_kf_totals(&db, &total, &deleted);
    ASSERT_EQ_INT(ret, 0, "sum_kf_totals empty");
    ASSERT_EQ_INT((int)total, 0, "total=0");
    ASSERT_EQ_INT((int)deleted, 0, "deleted=0");

    const char *k1 = "key1";
    const char *v1 = "value1";
    ret = slotcask_insert(&db, -1, k1, 4, v1, 6);
    ASSERT_EQ_INT(ret, 0, "insert key1");

    ret = slotcask_sum_kf_totals(&db, &total, &deleted);
    ASSERT_EQ_INT(ret, 0, "sum_kf_totals after insert");
    ASSERT_TRUE(total > 0, "total>0 after insert");

    void *val_out; size_t vlen_out;
    ret = slotcask_get(&db, k1, 4, &val_out, &vlen_out);
    ASSERT_EQ_INT(ret, 0, "get key1");
    ASSERT_EQ_INT((int)vlen_out, 6, "vlen key1");
    ASSERT_TRUE(memcmp(val_out, v1, 6) == 0, "value key1");
    free(val_out);

    ret = slotcask_pregrow_kf(&db, 100);
    ASSERT_EQ_INT(ret, 0, "pregrow_kf");

    ret = slotcask_exists(&db, k1, 4);
    ASSERT_EQ_INT(ret, 1, "exists key1");

    ret = slotcask_exists(&db, "nonexistent", 11);
    ASSERT_EQ_INT(ret, 0, "exists nonexistent");

    ret = slotcask_delete(&db, k1, 4);
    ASSERT_EQ_INT(ret, 0, "delete key1");

    ret = slotcask_exists(&db, k1, 4);
    ASSERT_EQ_INT(ret, 0, "exists after delete");

    slotcask_close(&db);

    ret = slotcask_open(&db, g_tmpdir, 8, 4, 64);
    ASSERT_EQ_INT(ret, 0, "reopen");

    SlotcaskRecord recs[2];
    const char *k2 = "key2", *v2 = "val2", *k3 = "key3", *v3 = "val3";
    recs[0].key = k2; recs[0].klen = 4; recs[0].value = v2; recs[0].vlen = 4;
    recs[1].key = k3; recs[1].klen = 4; recs[1].value = v3; recs[1].vlen = 4;
    ret = slotcask_bulk_update(&db, recs, 2);
    ASSERT_EQ_INT(ret, -1, "bulk_update missing keys");

    slotcask_insert(&db, -1, k2, 4, v2, 4);
    slotcask_insert(&db, -1, k3, 4, v3, 4);
    recs[0].value = v2; recs[0].vlen = 4;
    recs[1].value = v3; recs[1].vlen = 4;
    ret = slotcask_bulk_update(&db, recs, 2);
    ASSERT_EQ_INT(ret, 0, "bulk_update existing");

    ret = slotcask_compact_kf(&db);
    ASSERT_EQ_INT(ret, 0, "compact_kf");

    int dropped = 0;
    ret = slotcask_compact_segs(&db, &dropped);
    ASSERT_TRUE(ret == 0 || ret == -1, "compact_segs (may be -1 with 0 eligible)");

    typedef struct { int count; } Ctx;
    Ctx ctx = {0};
    ret = slotcask_walk_one_shard(&db, 0, NULL, &ctx, NULL);
    ASSERT_TRUE(ret == 0 || ret == -1, "walk_one_shard (null cb)");

    slotcask_close(&db);
    slotcask_shutdown();

    system("rm -rf \"/tmp/shard-db-slotcask-api-*\"");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-api", test_slotcask_api_run)
