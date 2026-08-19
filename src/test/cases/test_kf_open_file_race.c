#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "types.h"
#include "shard_db_internal.h"
#include "slotcask.h"
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define RACE_SLOTS_CAPACITY 8
#define RACE_HOLD_MS 50
#define RACE_POLL_TIMEOUT_MS 5000

typedef struct {
    ShardDb *db;
    const char *path;
    int rc;
} CreatorArgs;

static void *creator_thread_fn(void *arg) {
    CreatorArgs *a = arg;
    g_db = a->db;
    SlotcaskKfHandle h;
    a->rc = kfcache_acquire(&h, a->path, RACE_SLOTS_CAPACITY, 1);
    if (a->rc == 0) kfcache_release(&h);
    return NULL;
}

static int test_kf_open_file_race_run(void) {
    ShardDb *db = test_get_process_db();

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-kf-open-race-%d", getpid());
    mkdir(tmpdir, 0755);
    char path[300];
    snprintf(path, sizeof(path), "%s/000.kf", tmpdir);
    size_t want = SLOTCASK_KF_HDR_SIZE +
                  (size_t)RACE_SLOTS_CAPACITY * sizeof(SlotcaskKfEntry);

    db->kf_open_create_test_hold_ms = RACE_HOLD_MS;
    db->kf_open_file_call_count = 0;

    CreatorArgs cargs = { .db = db, .path = path, .rc = -2 };
    pthread_t creator;
    ASSERT_EQ_INT(pthread_create(&creator, NULL, creator_thread_fn, &cargs),
                  0, "spawn creator thread");

    int waited_ms = 0;
    struct stat st;
    while (waited_ms < RACE_POLL_TIMEOUT_MS) {
        if (stat(path, &st) == 0 && (size_t)st.st_size == want) break;
        struct timespec poll_ts = { 0, 1000000L };
        nanosleep(&poll_ts, NULL);
        waited_ms += 1;
    }
    ASSERT_TRUE(waited_ms < RACE_POLL_TIMEOUT_MS,
           "creator's ftruncate must land within timeout");

    SlotcaskKfHandle h2;
    int rc2 = kfcache_acquire(&h2, path, RACE_SLOTS_CAPACITY, 1);
    ASSERT_EQ_INT(rc2, 0,
        "racer must succeed once the creator's magic stamp lands, not fail "
        "on a still-initializing header");
    if (rc2 == 0) {
        ASSERT_EQ_INT((int)h2.hdr->total, 0,
            "racer must see a properly-initialized fresh header");
        kfcache_release(&h2);
    }

    pthread_join(creator, NULL);
    ASSERT_EQ_INT(cargs.rc, 0, "creator's own acquire must succeed");
    ASSERT_EQ_INT((int)db->kf_open_file_call_count, 1,
        "exactly one thread should have called kf_open_file() for this path");

    db->kf_open_create_test_hold_ms = 0;
    slotcask_shutdown();
    system("rm -rf /tmp/shard-db-kf-open-race-*");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-kf-open-file-race", test_kf_open_file_race_run)
