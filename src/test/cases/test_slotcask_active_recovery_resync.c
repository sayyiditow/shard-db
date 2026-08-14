#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static size_t rec_size(size_t klen, size_t vlen) {
    return (24u + klen + vlen + 7u) & ~(size_t)7u;
}

static int read_is(SlotcaskDb *db, const char *key,
                   const void *want, size_t want_len) {
    void *got = NULL;
    size_t got_len = 0;
    int rc = slotcask_get(db, key, strlen(key), &got, &got_len);
    int ok = rc == 0 && got_len == want_len &&
             memcmp(got, want, want_len) == 0;
    free(got);
    return ok;
}

static int test_slotcask_active_recovery_resync_run(void) {
    char dir[256];
    snprintf(dir, sizeof(dir), "/tmp/shard-db-active-recovery-%d", (int)getpid());
    char rmcmd[320];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", dir);
    system(rmcmd);
    mkdir(dir, 0755);

    char a[5000], b[4000], d[5000];
    memset(a, 'A', sizeof(a));
    memset(b, 'B', sizeof(b));
    memset(d, 'D', sizeof(d));

    slotcask_init(64, 64);
    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, dir, 8, 1, 8192), 0, "open fixture db");
    ASSERT_EQ_INT(slotcask_insert(&db, 0, "a", 1, a, sizeof(a)), 0,
                  "insert large donor A");
    ASSERT_EQ_INT(slotcask_insert(&db, 0, "c", 1, "C", 1), 0,
                  "insert live C after donor A");
    ASSERT_EQ_INT(slotcask_delete(&db, "a", 1), 0, "delete donor A");
    ASSERT_EQ_INT(slotcask_insert(&db, 0, "b", 1, b, sizeof(b)), 0,
                  "reuse A with smaller B and create an interior gap");

    const size_t c_end = rec_size(1, sizeof(a)) + rec_size(1, 1);
    ASSERT_TRUE(db.streams[0].reserve_off == c_end,
                "pre-restart append frontier is after C");
    slotcask_close(&db);

    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, dir, 8, 1, 8192), 0,
                  "reopen runs active-file recovery");
    ASSERT_TRUE(db.streams[0].reserve_off == c_end,
                "recovery restores append frontier after C, not at the gap");
    ASSERT_TRUE(read_is(&db, "c", "C", 1), "C survives the restart");

    ASSERT_EQ_INT(slotcask_insert(&db, 0, "d", 1, d, sizeof(d)), 0,
                  "post-restart append crosses the old gap");
    ASSERT_TRUE(read_is(&db, "c", "C", 1),
                "post-restart append does not overwrite C");
    ASSERT_TRUE(read_is(&db, "b", b, sizeof(b)), "B remains intact");
    ASSERT_TRUE(read_is(&db, "d", d, sizeof(d)), "D is readable");

    slotcask_close(&db);
    slotcask_shutdown();
    system(rmcmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-active-recovery-resync",
              test_slotcask_active_recovery_resync_run)
