/* src/test/cases/test_bt_cache_writer_starvation.c
 *
 * Regression test for writer starvation on bt_cache's per-file rwlock
 * (src/db/btree.c, bt_cache_init: pthread_rwlock_init with NULL/default
 * attributes). glibc's default rwlock policy does not favor a blocked
 * writer over newly arriving readers: under continuous read pressure on
 * one .idx file, a writer parked in bt_acquire()'s pthread_rwlock_wrlock()
 * can be starved indefinitely while readers keep winning every subsequent
 * rdlock().
 *
 * Deterministic ordering: a "holder" thread takes a real rdlock and parks.
 * A writer then blocks in wrlock() behind it; the test observes the
 * TEST_BUILD-only pending counter to prove that fact. A late-reader thread
 * announces immediately before it calls rdlock(). While the holder remains
 * locked, the test requires that late reader NOT acquire: glibc's default
 * PTHREAD_RWLOCK_PREFER_READER_NP grants it ahead of the queued writer, so
 * the unfixed child exits with a distinct failure code. With
 * PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP, it remains blocked until the
 * holder releases and the queued writer completes. This tests the policy's
 * ordering contract directly; it does not rely on a probabilistic starvation
 * duration or reader throughput.
 *
 * The scenario runs in a forked child, and the parent has a real 30-second
 * bound (300 x 100ms polls), so any unexpected deadlock is contained.
 *
 * glibc/NPTL-only: PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP does not
 * exist on macOS or non-glibc Linux libcs, and rwlock_init_writer_preferring()
 * is a no-op there (falls back to today's default-attribute behavior). This
 * test's assertion only holds where that attribute is actually available, so
 * the whole scenario is compiled out elsewhere and reports a skip instead of
 * a false failure.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "btree.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

static int noop_count_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    (void)v; (void)vl; (void)h;
    int *n = ctx;
    (*n)++;
    return 0;
}

/* Bound waits in forked deadlock regressions on every supported platform. */
static int wait_child_bounded(pid_t pid, int *status, int secs) {
    for (int tick = 0; tick < secs * 10; tick++) {
        pid_t r = waitpid(pid, status, WNOHANG);
        if (r == pid) return 1;
        if (r < 0) return -1;
        usleep(100000);
    }
    kill(pid, SIGKILL);
    waitpid(pid, status, 0);
    return 0;
}

#if defined(__linux__) && defined(__GLIBC__)

#define BASE_COUNT 5000
#define WRITER_CHUNKS 5
#define CHUNK_SIZE 64
#define WRITER_PENDING_WAIT_SECS 5
#define HOLDER_ACQUIRE_WAIT_SECS 5
#define LATE_READER_BLOCK_CHECK_MS 250

static const char *g_vals[4] = {"paid", "pending", "refunded", "cancelled"};

static void make_hash(long id, uint8_t out[BT_HASH_SIZE]) {
    memset(out, 0, BT_HASH_SIZE);
    uint32_t mixed = (uint32_t)id * 2654435761u;
    memcpy(out, &mixed, sizeof(mixed));
    memcpy(out + 4, &mixed, sizeof(mixed));
    memcpy(out + 8, &id, sizeof(id) < 8 ? sizeof(id) : 8);
}

static int range_count(const char *path) {
    int n = 0;
    btree_range(path, "", 0,
                "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
                BT_HASH_SIZE, noop_count_cb, &n);
    return n;
}

typedef struct {
    const char *path;
    ShardDb    *db;
    atomic_int *chunks_done;
    atomic_int *writer_done;
} WriterArgs;

static void *writer_thread_main(void *arg) {
    WriterArgs *a = arg;
    g_db = a->db;
    for (int c = 0; c < WRITER_CHUNKS; c++) {
        BtEntry *entries = calloc(CHUNK_SIZE, sizeof(*entries));
        for (int i = 0; i < CHUNK_SIZE; i++) {
            long id = BASE_COUNT + (long)c * CHUNK_SIZE + i;
            entries[i].value = strdup(g_vals[id % 4]);
            entries[i].vlen = strlen(entries[i].value);
            make_hash(id, entries[i].hash);
        }
        btree_bulk_merge(a->path, entries, CHUNK_SIZE);
        for (int i = 0; i < CHUNK_SIZE; i++) free((char *)entries[i].value);
        free(entries);
        atomic_fetch_add(a->chunks_done, 1);
    }
    atomic_store(a->writer_done, 1);
    return NULL;
}

static int run_child(const char *base) {
    bt_cache_shutdown();
    bt_cache_init(16);

    char path[600];
    snprintf(path, sizeof(path), "%s/writer_starvation.idx", base);

    BtEntry *seed = calloc(BASE_COUNT, sizeof(*seed));
    for (int i = 0; i < BASE_COUNT; i++) {
        seed[i].value = strdup(g_vals[i % 4]);
        seed[i].vlen = strlen(seed[i].value);
        make_hash(i, seed[i].hash);
    }
    int seed_rc = btree_bulk_merge(path, seed, BASE_COUNT);
    for (int i = 0; i < BASE_COUNT; i++) free((char *)seed[i].value);
    free(seed);
    if (seed_rc != 0) _exit(2);

    ShardDb *db = g_db;
    atomic_int holder_attempted = 0, holder_acquired = 0, holder_release = 0;
    BtTestHoldRdlockArgs holder_args = {
        .path = path, .db = db, .attempted = &holder_attempted,
        .acquired = &holder_acquired, .release = &holder_release
    };
    pthread_t holder_tid;
    pthread_create(&holder_tid, NULL, btree_test_hold_rdlock, &holder_args);

    for (int waited = 0; !atomic_load(&holder_acquired); waited++) {
        if (waited >= HOLDER_ACQUIRE_WAIT_SECS * 10) _exit(3);
        usleep(100 * 1000);
    }

    atomic_int chunks_done = 0, writer_done = 0;
    WriterArgs wargs = { .path = path, .db = db, .chunks_done = &chunks_done,
                          .writer_done = &writer_done };
    pthread_t writer_tid;
    pthread_create(&writer_tid, NULL, writer_thread_main, &wargs);

    for (int waited = 0; btree_test_writer_pending_count() <= 0; waited++) {
        if (waited >= WRITER_PENDING_WAIT_SECS * 10) _exit(4);
        usleep(100 * 1000);
    }

    atomic_int late_attempted = 0, late_acquired = 0, late_release = 0;
    BtTestHoldRdlockArgs late_args = {
        .path = path, .db = db, .attempted = &late_attempted,
        .acquired = &late_acquired, .release = &late_release
    };
    pthread_t late_tid;
    pthread_create(&late_tid, NULL, btree_test_hold_rdlock, &late_args);

    /* Poll until the late reader is genuinely inside pthread_rwlock_rdlock()
       (not merely scheduled to call it) before starting the bounded
       observation window below. `late_attempted` alone isn't a safe sync
       point here: bt_acquire() does real work (cache-slot resolution)
       between setting it and actually calling rdlock(), and under
       scheduler contention that gap can widen enough that a fixed-delay
       window started from `late_attempted` could elapse before the reader
       ever reaches the blocking call — an unfixed build would then look
       like it passed, having never actually been tested. The
       reader-pending counter is incremented immediately before rdlock()
       and decremented immediately after it returns, so count > 0 proves
       the thread is blocked in the syscall right now. Also guard the
       degenerate case where the reader races through to acquisition
       before this loop ever samples a nonzero count (possible on a build
       that isn't writer-preferring, where the call barely blocks at all). */
    for (int waited = 0; btree_test_reader_pending_count() <= 0; waited++) {
        if (atomic_load(&late_acquired)) _exit(6);
        if (waited >= HOLDER_ACQUIRE_WAIT_SECS * 10) _exit(5);
        usleep(100 * 1000);
    }
    for (int waited = 0; waited < LATE_READER_BLOCK_CHECK_MS; waited++) {
        if (atomic_load(&late_acquired)) _exit(6);
        usleep(1000);
    }

    atomic_store(&holder_release, 1);
    pthread_join(holder_tid, NULL);

    /* The writer thread issues WRITER_CHUNKS separate wrlock acquisitions
       (one per btree_bulk_merge call), not one held lock for the whole
       batch. PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP only guarantees
       the late reader can't bypass the writer request it was already
       queued behind (proven above, before holder_release); it does not
       hold that reader back through the writer's later, separately-issued
       chunk requests. Once the writer's first chunk completes and releases,
       the already-queued late reader is legitimately next in line — release
       it the moment it acquires so it can't sit on the lock and starve the
       writer's remaining chunks (which would deadlock this test, not the
       production code: bt_acquire has no such test-only hold-open). */
    for (int waited = 0; !atomic_load(&late_acquired); waited++) {
        if (waited >= HOLDER_ACQUIRE_WAIT_SECS * 10) _exit(8);
        usleep(100 * 1000);
    }
    atomic_store(&late_release, 1);
    pthread_join(late_tid, NULL);

    for (int waited = 0; !atomic_load(&writer_done); waited++) {
        if (waited >= WRITER_PENDING_WAIT_SECS * 10) _exit(7);
        usleep(100 * 1000);
    }
    pthread_join(writer_tid, NULL);

    int got = range_count(path);
    int expected = BASE_COUNT + WRITER_CHUNKS * CHUNK_SIZE;
    if (got != expected || atomic_load(&chunks_done) != WRITER_CHUNKS) _exit(9);

    _exit(0);
}

static int test_bt_cache_writer_starvation_glibc_run(void) {
    char base[] = "/tmp/shard-db-bt-writer-starvation-XXXXXX";
    if (!mkdtemp(base)) { ASSERT_TRUE(0, "mkdtemp"); return 1; }

    pid_t pid = fork();
    if (pid < 0) {
        ASSERT_TRUE(0, "fork");
        char cmd[700]; snprintf(cmd, sizeof(cmd), "rm -rf %s", base); system(cmd);
        return 1;
    }
    if (pid == 0) run_child(base); /* never returns */

    int status = 0;
    int wait_rc = wait_child_bounded(pid, &status, 30);

    ASSERT_EQ_INT(wait_rc, 1, "child exits before the 30-second deadlock bound");
    if (WIFSIGNALED(status))
        TAP_DIAG("# child killed by signal %d (still blocked after 30s bound)\n", WTERMSIG(status));
    if (WIFEXITED(status) && WEXITSTATUS(status) != 0)
        TAP_DIAG("# child exited %d (2=seed failed, 3=holder never acquired, "
                  "4=writer never observed pending, 5=late reader never started, "
                  "6=late reader bypassed queued writer, 7=writer did not finish, "
                  "8=late reader did not resume, 9=post-run count/chunk mismatch)\n",
                  WEXITSTATUS(status));
    ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0,
        "late reader cannot bypass a queued writer on bt_cache's rwlock");

    char cmd[700]; snprintf(cmd, sizeof(cmd), "rm -rf %s", base); system(cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

#else /* !(defined(__linux__) && defined(__GLIBC__)) */

static int test_bt_cache_writer_starvation_glibc_run(void) {
    TAP_DIAG("# test-bt-cache-writer-starvation: requires glibc/NPTL's "
             "PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP, not available on "
             "this platform, skipping\n");
    return 0;
}

#endif /* defined(__linux__) && defined(__GLIBC__) */

/* ── Post-review regressions (2026-08-01 corrective plan, Task 1) ──
 *
 * The original atomic-publication implementation serialised every cache
 * acquire and every publish behind one process-global, writer-preferring
 * rwlock (the "publication gate"). That produced a genuine lock cycle:
 *
 *     reader holds cache entry A (long-lived handle, e.g. BtRangeIter)
 *       -> publisher holds the global gate and blocks on entry A's rwlock
 *       -> reader tries to acquire entry B and blocks on the global gate
 *
 * The replacement is a publication-generation protocol: publication never
 * holds a global gate and never blocks on a live target cache entry; a
 * successful rename advances a generation; cache entries record the
 * generation at which their open inode was validated, and an acquire that
 * finds a mismatched entry retires it non-blockingly or opens the current
 * path uncached. These regressions prove the invariant — publication
 * finishes, an acquire overlapping publication may complete on either
 * inode, and an acquire beginning after publication completes sees the new
 * inode — not a particular lock implementation. They run in bounded
 * children: the pre-fix code deadlocks the child, and the parent SIGKILLs
 * it after the 30-second bound.
 */

#define PUB_BASE_COUNT  100
#define PUB_CHUNK_SIZE  100

static void pub_make_hash(long id, uint8_t out[BT_HASH_SIZE]) {
    memset(out, 0, BT_HASH_SIZE);
    uint32_t mixed = (uint32_t)id * 2654435761u;
    memcpy(out, &mixed, sizeof(mixed));
    memcpy(out + 4, &mixed, sizeof(mixed));
    memcpy(out + 8, &id, sizeof(id) < 8 ? sizeof(id) : 8);
}

static void pub_seed_entries(BtEntry *entries, long base, int n) {
    for (int i = 0; i < n; i++) {
        long id = base + i;
        const char *v = (id % 4 == 0) ? "alpha" : (id % 4 == 1) ? "beta"
                                            : (id % 4 == 2) ? "gamma" : "delta";
        entries[i].value = strdup(v);
        entries[i].vlen = strlen(entries[i].value);
        pub_make_hash(id, entries[i].hash);
    }
}

static int pub_range_count(const char *path) {
    int n = 0;
    btree_range(path, "", 0,
                "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
                BT_HASH_SIZE, noop_count_cb, &n);
    return n;
}

static const char *g_pub_sentinel =
    "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";

/* Poll for presence (present=1) or absence (present=0) of the
   durability_test_pause marker for `phase` under `base`. */
static int pub_wait_marker(const char *base, const char *phase,
                           int present, int secs) {
    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker), "%s/.durability-test-%s.active",
             base, phase);
    for (int tick = 0; tick < secs * 10; tick++) {
        struct stat st;
        if ((stat(marker, &st) == 0) == present) return 0;
        usleep(100 * 1000);
    }
    return -1;
}

typedef struct {
    const char *path;
    ShardDb    *db;
    int         rc;
    atomic_int *done;
} PubMergeArgs;

static void *pub_merge_thread_main(void *arg) {
    PubMergeArgs *a = arg;
    g_db = a->db;
    BtEntry *entries = calloc(PUB_CHUNK_SIZE, sizeof(*entries));
    pub_seed_entries(entries, PUB_BASE_COUNT, PUB_CHUNK_SIZE);
    a->rc = btree_bulk_merge(a->path, entries, PUB_CHUNK_SIZE);
    for (int i = 0; i < PUB_CHUNK_SIZE; i++) free((char *)entries[i].value);
    free(entries);
    atomic_store(a->done, 1);
    return NULL;
}

/* Bounded multi-handle deadlock regression: retain A, pause publication
   of A, acquire/use B while A remains retained, then prove a post-
   publication A acquire sees the replacement contents. Pre-fix: the
   publisher holds the global gate and blocks on A's entry rwlock while
   the main thread blocks on the gate acquiring B — the child hangs and
   the parent kills it at the 30-second bound. */
static int run_bt_publish_deadlock_child(const char *base) {
    bt_cache_shutdown();
    bt_cache_init(16);

    char path_a[PATH_MAX], path_b[PATH_MAX];
    snprintf(path_a, sizeof(path_a), "%s/pub_a.idx", base);
    snprintf(path_b, sizeof(path_b), "%s/pub_b.idx", base);

    BtEntry *seed = calloc(PUB_BASE_COUNT, sizeof(*seed));
    pub_seed_entries(seed, 0, PUB_BASE_COUNT);
    if (btree_bulk_merge(path_a, seed, PUB_BASE_COUNT) != 0) {
        free(seed); _exit(2);
    }
    for (int i = 0; i < PUB_BASE_COUNT; i++) free((char *)seed[i].value);
    free(seed);

    BtEntry *seed_b = calloc(8, sizeof(*seed_b));
    pub_seed_entries(seed_b, 1000, 8);
    if (btree_bulk_merge(path_b, seed_b, 8) != 0) {
        for (int i = 0; i < 8; i++) free((char *)seed_b[i].value);
        free(seed_b); _exit(2);
    }
    for (int i = 0; i < 8; i++) free((char *)seed_b[i].value);
    free(seed_b);

    ShardDb *db = g_db;
    snprintf(db->durability_test_pause_phase,
             sizeof(db->durability_test_pause_phase), "%s",
             "bt-publish-before-rename");
    db->durability_test_pause_ms = 2000;

    /* Retain a real rdlock on A's cache entry for the whole publication. */
    BtRangeIter *it = btree_range_iter_open(path_a, "", 0, 0,
                                            g_pub_sentinel, BT_HASH_SIZE, 0, 0);
    if (!it) _exit(3);

    atomic_int pub_done = 0;
    PubMergeArgs margs = { .path = path_a, .db = db, .rc = -999,
                           .done = &pub_done };
    pthread_t pub_tid;
    pthread_create(&pub_tid, NULL, pub_merge_thread_main, &margs);

    int marker_seen = pub_wait_marker(base, "bt-publish-before-rename",
                                      1, 5) == 0;

    /* Must complete before releasing A. Pre-fix this blocks forever on the
       global publication gate. */
    int b_count = pub_range_count(path_b);
    if (b_count != 8) _exit(4);

    /* Release A's retained rdlock, then let publication finish. */
    btree_range_iter_close(it);

    if (marker_seen &&
        pub_wait_marker(base, "bt-publish-before-rename", 0, 5) != 0)
        _exit(5);

    for (int waited = 0; !atomic_load(&pub_done); waited++) {
        if (waited >= 100) _exit(6);
        usleep(100 * 1000);
    }
    pthread_join(pub_tid, NULL);
    if (margs.rc != 0) _exit(7);

    /* An acquire beginning after publication completes sees the new inode:
       the merged 200 entries, not the stale 100. */
    int a_count = pub_range_count(path_a);
    if (a_count != PUB_BASE_COUNT + PUB_CHUNK_SIZE) _exit(8);

    db->durability_test_pause_ms = 0;
    db->durability_test_pause_phase[0] = '\0';
    _exit(0);
}

/* Online-bulk-versus-reindex regression: retain two BtRangeIters on A,
   publish a rebuild of A, and require both operations to complete with
   every expected key still queryable afterwards. Pre-fix: the publisher
   blocks on A's entry rwlock (held by the iterators) inside its blocking
   cache invalidation and never completes. */
static int run_bt_bulk_reindex_child(const char *base) {
    bt_cache_shutdown();
    bt_cache_init(16);

    char path_a[PATH_MAX];
    snprintf(path_a, sizeof(path_a), "%s/reidx_a.idx", base);

    BtEntry *seed = calloc(PUB_BASE_COUNT, sizeof(*seed));
    pub_seed_entries(seed, 0, PUB_BASE_COUNT);
    if (btree_bulk_merge(path_a, seed, PUB_BASE_COUNT) != 0) {
        for (int i = 0; i < PUB_BASE_COUNT; i++) free((char *)seed[i].value);
        free(seed); _exit(2);
    }
    for (int i = 0; i < PUB_BASE_COUNT; i++) free((char *)seed[i].value);
    free(seed);

    BtRangeIter *it1 = btree_range_iter_open(path_a, "", 0, 0,
                                             g_pub_sentinel, BT_HASH_SIZE, 0, 0);
    BtRangeIter *it2 = btree_range_iter_open(path_a, "", 0, 0,
                                             g_pub_sentinel, BT_HASH_SIZE, 0, 0);
    if (!it1 || !it2) _exit(3);

    atomic_int pub_done = 0;
    PubMergeArgs margs = { .path = path_a, .db = g_db, .rc = -999,
                           .done = &pub_done };
    pthread_t pub_tid;
    pthread_create(&pub_tid, NULL, pub_merge_thread_main, &margs);

    for (int waited = 0; !atomic_load(&pub_done); waited++) {
        if (waited >= 100) _exit(9);
        usleep(100 * 1000);
    }
    pthread_join(pub_tid, NULL);
    if (margs.rc != 0) _exit(10);

    btree_range_iter_close(it1);
    btree_range_iter_close(it2);

    int a_count = pub_range_count(path_a);
    if (a_count != PUB_BASE_COUNT + PUB_CHUNK_SIZE) _exit(11);
    _exit(0);
}

static int test_bt_publish_generation_glibc_run(void) {
    char base[] = "/tmp/shard-db-bt-publish-XXXXXX";
    if (!mkdtemp(base)) { ASSERT_TRUE(0, "mkdtemp"); return 1; }

    pid_t pid = fork();
    if (pid < 0) {
        ASSERT_TRUE(0, "fork");
        char cmd[700]; snprintf(cmd, sizeof(cmd), "rm -rf %s", base); system(cmd);
        return 1;
    }
    if (pid == 0) run_bt_publish_deadlock_child(base); /* never returns */

    int status = 0;
    int wait_rc = wait_child_bounded(pid, &status, 30);
    ASSERT_EQ_INT(wait_rc, 1,
                  "bounded deadlock child exits before the 30-second bound");
    if (WIFEXITED(status) && WEXITSTATUS(status) != 0)
        TAP_DIAG("# deadlock child exited %d (2=seed failed, 3=iter open failed, "
                  "4=B unusable/never acquired, 5=marker never cleared, "
                  "6=publisher did not finish, 7=publisher failed, "
                  "8=post-publication A count wrong)\n",
                  WEXITSTATUS(status));
    ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0,
        "acquire of B completes while A is retained mid-publication");

    pid_t pid2 = fork();
    if (pid2 < 0) {
        ASSERT_TRUE(0, "fork");
        char cmd[700]; snprintf(cmd, sizeof(cmd), "rm -rf %s", base); system(cmd);
        return 1;
    }
    if (pid2 == 0) run_bt_bulk_reindex_child(base); /* never returns */

    int status2 = 0;
    int wait_rc2 = wait_child_bounded(pid2, &status2, 30);
    ASSERT_EQ_INT(wait_rc2, 1,
                  "bulk-vs-reindex child exits before the 30-second bound");
    if (WIFEXITED(status2) && WEXITSTATUS(status2) != 0)
        TAP_DIAG("# bulk-reindex child exited %d (2=seed failed, 3=iter open "
                  "failed, 9=publisher did not finish, 10=publisher failed, "
                  "11=post-reindex A count wrong)\n",
                  WEXITSTATUS(status2));
    ASSERT_TRUE(WIFEXITED(status2) && WEXITSTATUS(status2) == 0,
        "online bulk merge and retained range iterators complete together");

    char cmd[700]; snprintf(cmd, sizeof(cmd), "rm -rf %s", base); system(cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bt-cache-writer-starvation", test_bt_cache_writer_starvation_glibc_run)
TEST_REGISTER("test-bt-publish-generation", test_bt_publish_generation_glibc_run)
