/* src/test/cases/test_durability_epoch_sync.c
 *
 * B3a path-keyed durability epochs: concurrent same-path syncs coalesce
 * into one raw op; sequential registrations never skip (a completed
 * sync is not reused by a later registration); a failed round
 * group-fails exactly its registrants with a failed round's errno, a
 * late registrant after a failed round fails promptly (monotone failed
 * high-water — a stale-high-water implementation retries forever
 * here); an injected failure group-fails and the path recovers on the
 * next round; a missing file is success for the FILE raw (bitmap-leg
 * rationale); the dir variant coalesces the same way.
 *
 * Concurrency shaping: durability_test_epoch_set_register_hold(N) makes
 * N registrants wait until all have registered before any claims, so
 * "one round covers all N" is deterministic (a raw start gate cannot
 * provide that — the first thread to claim wins before peers
 * register). Workers bind g_db themselves: it is __thread and spawned
 * threads start with NULL (same precedent as
 * test-bt-cache-writer-starvation).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

extern void btree_test_sync_reset(void);
extern int  btree_test_sync_count(void);

typedef enum { EPOCH_OP_BTREE, EPOCH_OP_FILE, EPOCH_OP_DIR } EpochOp;

typedef struct {
    ShardDb   *db;      /* thread-local g_db: bound at worker entry   */
    char       path[PATH_MAX];
    EpochOp    op;
    int        rc;
    int        err;
} EpochWorker;

static void *epoch_worker_fn(void *p) {
    EpochWorker *w = p;
    g_db = w->db;   /* __thread g_db: spawned threads start NULL */
    switch (w->op) {
    case EPOCH_OP_BTREE:
        w->rc = btree_sync_path(w->path);
        break;
    case EPOCH_OP_FILE:
        w->rc = durability_epoch_fdatasync_path_ex(w->path, NULL);
        break;
    case EPOCH_OP_DIR:
        w->rc = durability_epoch_fsync_dir(w->path);
        break;
    }
    w->err = w->rc != 0 ? errno : 0;
    return NULL;
}

static void run_workers(EpochWorker *w, int n) {
    pthread_t th[4];
    int spawned = 0;
    for (int i = 0; i < n; i++) {
        if (pthread_create(&th[i], NULL, epoch_worker_fn, &w[i]) != 0) {
            /* Release any armed register-hold first: fewer than n
               registrants would otherwise block forever on it, hanging
               the join (and the whole run-all worker) below. */
            durability_test_epoch_set_register_hold(0);
            durability_test_epoch_set_delay_ms(0);
            for (int j = 0; j < spawned; j++)
                pthread_join(th[j], NULL);
            ASSERT_EQ_INT(-1, 0, "spawn epoch worker");
            return;
        }
        spawned++;
    }
    for (int i = 0; i < n; i++)
        pthread_join(th[i], NULL);
}

/* Fill n workers with one path/op and run them as one held-open
   registration window (register-hold N + in-flight delay N ms): the
   first claimer's target covers all N registrations. */
static void run_one_round_workers(EpochWorker *w, int n, EpochOp op,
                                  const char *path, ShardDb *db) {
    for (int i = 0; i < n; i++) {
        w[i].db = db;
        snprintf(w[i].path, sizeof(w[i].path), "%s", path);
        w[i].op = op;
        w[i].rc = 0; w[i].err = 0;
    }
    durability_test_epoch_set_register_hold(n);
    durability_test_epoch_set_delay_ms(25);
    run_workers(w, n);
    durability_test_epoch_set_register_hold(0);
    durability_test_epoch_set_delay_ms(0);
}

static int test_durability_epoch_sync_run(void) {
    char cache_dir[] = "/tmp/shard-db-durepo-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(cache_dir), "create epoch fixture dir");
    if (!cache_dir[0]) return 1;
    slotcask_shutdown();
    slotcask_init(16, 16);   /* kfcache/segcache for btree_insert below */
    ShardDb *proc_db = g_db; /* bound into every spawned worker */

    char bt_path[PATH_MAX];
    snprintf(bt_path, sizeof(bt_path), "%s/value.idx", cache_dir);
    uint8_t hash[16] = {0};
    btree_insert(bt_path, "value", 5, hash);

    EpochWorker w[4];

    /* 1) Four concurrent same-path btree syncs → ONE raw op. */
    durability_test_epoch_reset();
    btree_test_sync_reset();
    run_one_round_workers(w, 4, EPOCH_OP_BTREE, bt_path, proc_db);
    for (int i = 0; i < 4; i++)
        ASSERT_EQ_INT(w[i].rc, 0, "coalesced same-path sync succeeds");
    ASSERT_EQ_INT(btree_test_sync_count(), 1,
                  "four concurrent same-path syncs perform one raw op");

    /* 2) No over-skipping: strictly sequential syncs each drive their
       own round (test_single_op_index_sync's exact counts depend on
       this). */
    durability_test_epoch_reset();
    btree_test_sync_reset();
    ASSERT_EQ_INT(btree_sync_path(bt_path), 0, "sequential sync 1");
    ASSERT_EQ_INT(btree_sync_path(bt_path), 0, "sequential sync 2");
    ASSERT_EQ_INT(btree_sync_path(bt_path), 0, "sequential sync 3");
    ASSERT_EQ_INT(btree_test_sync_count(), 3,
                  "sequential registrations each perform a raw op");

    /* 3) Group failure + late registrant. The raw op fails ENOTDIR: the
       path's parent is a regular FILE, so the btree writer's
       (mkdirp-ignored) open gets ENOTDIR and creates nothing. All four
       fail with that errno — today's independent syncs also fail 4/4,
       so this pins that grouping must not turn failures into successes.
       The trailing sequential call is the monotone high-water pin:
       bookkeeping that freezes the failed high-water at its first value
       (min-style) would retry this call forever. */
    durability_test_epoch_reset();
    char blocker_path[PATH_MAX];
    snprintf(blocker_path, sizeof(blocker_path), "%s/blocker", cache_dir);
    int bfd = open(blocker_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_TRUE(bfd >= 0, "create regular-file parent blocker");
    if (bfd >= 0) close(bfd);
    char blocked_path[PATH_MAX];
    snprintf(blocked_path, sizeof(blocked_path), "%s/blocker/missing.idx",
             cache_dir);
    run_one_round_workers(w, 4, EPOCH_OP_BTREE, blocked_path, proc_db);
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ_INT(w[i].rc, -1, "failed round group-fails");
        ASSERT_EQ_INT(w[i].err, ENOTDIR, "group failure keeps raw errno");
    }
    errno = 0;
    int rc_late = btree_sync_path(blocked_path);
    int err_late = errno;   /* capture before any assert side effects */
    ASSERT_EQ_INT(rc_late, -1,
                  "late registrant after a failed round fails promptly");
    ASSERT_EQ_INT(err_late, ENOTDIR, "late registrant sees the raw errno");

    /* 4) Dir variant coalesces the same way. */
    durability_test_epoch_reset();
    run_one_round_workers(w, 4, EPOCH_OP_DIR, cache_dir, proc_db);
    for (int i = 0; i < 4; i++)
        ASSERT_EQ_INT(w[i].rc, 0, "coalesced dir sync succeeds");
    ASSERT_EQ_INT(durability_test_epoch_dir_sync_count(), 1,
                  "four concurrent same-dir syncs perform one raw op");

    /* 5) Injected failure: one round fails EIO and group-fails all four
       with the injected errno; the next round succeeds and the failure
       does not stick. Raw-op count == 2 proves one round per
       registration window. */
    durability_test_epoch_reset();
    char plain_path[] = "/tmp/shard-db-durepo-file-XXXXXX";
    int pfd = mkstemp(plain_path);
    ASSERT_TRUE(pfd >= 0, "create plain-file fixture");
    if (pfd >= 0) close(pfd);
    durability_test_epoch_fail_next(1, EIO);
    run_one_round_workers(w, 4, EPOCH_OP_FILE, plain_path, proc_db);
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ_INT(w[i].rc, -1, "injected failure group-fails");
        ASSERT_EQ_INT(w[i].err, EIO, "injected errno reaches all four");
    }
    ASSERT_EQ_INT(durability_test_epoch_file_sync_count(), 1,
                  "failed registration window performed one raw op");
    run_one_round_workers(w, 4, EPOCH_OP_FILE, plain_path, proc_db);
    for (int i = 0; i < 4; i++)
        ASSERT_EQ_INT(w[i].rc, 0, "next round recovers");
    ASSERT_EQ_INT(durability_test_epoch_file_sync_count(), 2,
                  "recovery is one more round, not per-caller retries");
    unlink(plain_path);

    /* 6) Missing file (bitmap-leg rationale): the FILE raw converts
       ENOENT to success — nothing was ever written, so a covering
       round is vacuously true. This is also what keeps the weakened
       group-failure errno away from the bitmap call site's
       errno != ENOENT skip. */
    durability_test_epoch_reset();
    char absent_path[PATH_MAX];
    snprintf(absent_path, sizeof(absent_path), "%s/nope/absent.bm",
             cache_dir);
    int synced6 = -1;
    ASSERT_EQ_INT(durability_epoch_fdatasync_path_ex(absent_path,
                                                     &synced6), 0,
                  "missing file: nothing to sync is success");
    ASSERT_EQ_INT(synced6, 1, "missing-file round attempted the raw op");
    ASSERT_EQ_INT(durability_test_epoch_file_sync_count(), 1,
                  "missing-file sync counted as one attempt");

    errno = 0;
    synced6 = -1;
    int rc_null = durability_epoch_fdatasync_path_ex(NULL, &synced6);
    int err_null = errno;   /* capture before any assert side effects */
    ASSERT_EQ_INT(rc_null, -1,
                  "null path is rejected at the protocol entry");
    ASSERT_EQ_INT(err_null, EINVAL, "null path fails with EINVAL");
    ASSERT_EQ_INT(synced6, 0, "rejected call performed no raw op");
    ASSERT_EQ_INT(durability_test_epoch_file_sync_count(), 1,
                  "rejected null-path call is not counted");

    /* 7) Domain isolation: the SAME path under two different
       DurEpochDomains must never coalesce into one round — a BTREE
       registration may only be covered by the wrlock-holding btree raw,
       and vice versa. An implementation that drops `domain` from the
       registry key performs one raw op here instead of two. */
    durability_test_epoch_reset();
    btree_test_sync_reset();
    durability_test_epoch_set_register_hold(2);
    durability_test_epoch_set_delay_ms(25);
    w[0].db = proc_db;
    snprintf(w[0].path, sizeof(w[0].path), "%s", bt_path);
    w[0].op = EPOCH_OP_BTREE;
    w[0].rc = 0; w[0].err = 0;
    w[1].db = proc_db;
    snprintf(w[1].path, sizeof(w[1].path), "%s", bt_path);
    w[1].op = EPOCH_OP_FILE;
    w[1].rc = 0; w[1].err = 0;
    run_workers(w, 2);
    durability_test_epoch_set_register_hold(0);
    durability_test_epoch_set_delay_ms(0);
    ASSERT_EQ_INT(w[0].rc, 0, "btree-domain sync of shared path succeeds");
    ASSERT_EQ_INT(w[1].rc, 0, "file-domain sync of shared path succeeds");
    ASSERT_EQ_INT(btree_test_sync_count(), 1,
                  "btree domain performed its own raw op");
    ASSERT_EQ_INT(durability_test_epoch_file_sync_count(), 1,
                  "file domain performed its own raw op");

    slotcask_shutdown();
    rmrf(cache_dir);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-durability-epoch-sync", test_durability_epoch_sync_run)
