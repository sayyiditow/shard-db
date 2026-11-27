/* src/db/test_control.c
 *
 * TEST_BUILD-only control channel for the deterministic
 * test-update-partial-concurrent seam. Compiled only into
 * shard-db-test-server; the production shard-db binary never links it.
 *
 * The test runner inherits one end of an anonymous AF_UNIX/SOCK_STREAM
 * socketpair across exec (`server --test-control-fd <fd>`) and drives the
 * daemon-side slotcask pause hook with fixed-size INSTALL/RELEASE/CLEAR
 * messages. The control thread installs/clears the hook and signals a
 * request thread parked inside it; the only time anything waits while a
 * kf-shard lock is held is the intentional callback pause itself.
 */
#ifdef TEST_BUILD

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

#include "slotcask.h"
#include "shard_test_ctl.h"
#include "test_control.h"
#include "btree.h"
/* query.c (TEST_BUILD): find-side per-leaf KeySet alloc-fault injection. */
void build_keyset_leaf_test_set_fail_alloc(int fail_n);
/* query_aggregate.c (TEST_BUILD): Pass-1 parallel-route observability. */
void igb_pass1_parallel_reset_for_test(void);
int igb_pass1_parallel_taken_for_test(void);

/* Message kinds — private duplicated constants; the runner (fixtures.c)
   defines the same layout independently. */
enum {
    TEST_HOOK_INSTALL = 1,
    TEST_HOOK_RELEASE = 2,
    TEST_HOOK_CLEAR   = 3,
    TEST_HOOK_ACK     = 4,
    TEST_HOOK_REACHED = 5,
    TEST_HOOK_SET_GLOBALS = 6,
    TEST_HOOK_GET_GLOBALS = 7,   /* reply ACK .a = the queried value */
};

/* INSTALL message phase selects which daemon-side hook to arm.
   0 = legacy slotcask after-old hook; 1 = count-worker pass-1 gap;
   2 = find-flush gate (batch-fetch park); 3 = guard check;
   4 = count-worker gap after the probe reader is released. Both count
   gaps report REACHED phase 2. */
#define TEST_HOOK_KIND_AFTER_OLD 0
#define TEST_HOOK_KIND_COUNT_GAP 1
#define TEST_HOOK_KIND_FIND_FLUSH_GATE 2
#define TEST_HOOK_KIND_GUARD_CHECK 3
#define TEST_HOOK_KIND_COUNT_GAP_LATE 4
/* TEST_HOOK_SET_GLOBALS phase selectors (runner-side wrappers in
   fixtures.c duplicate these by convention). */
#define TEST_HOOK_SET_GLOBALS_KF_SEAMS 0
#define TEST_HOOK_SET_GLOBALS_ESCAPE_FAIL 1
#define TEST_HOOK_SET_GLOBALS_BMFIX_FAULT 2
#define TEST_HOOK_SET_GLOBALS_BMLOCK_LOG 3
#define TEST_HOOK_SET_GLOBALS_BMKS_RACE 4
#define TEST_HOOK_SET_GLOBALS_FAIL_PHASE 5
#define TEST_HOOK_SET_GLOBALS_FAIL_STICKY 6
#define TEST_HOOK_SET_GLOBALS_BMFIX_BACKOFF 7
/* Audit campaign crash/restart fuzzer: arm the window-coordinator
 * phase-boundary pause (shard_test_phase_pause, ShardTestPhase) inside the
 * daemon so a biased-kill test can park the next matching window at the
 * payload-staging (P) barrier and SIGKILL there, leaving a deterministic
 * flag=0 grave. a = phase as (int64_t) so -1 disarms round-trips; b =
 * 1-based occurrence (which armed hit parks). The parked thread is never
 * released through this channel — the biased-kill caller kills the daemon
 * while parked (the 30 s park safety cap makes a missed kill self-heal). */
#define TEST_HOOK_SET_GLOBALS_PAUSE_PHASE 8
/* Arm the btree range-iterator open-failure seam: the NEXT
 * btree_range_iter_open on a path ending /<NNN>.idx for the given
 * 3-hex shard fails with EIO (one-shot; btree_test_fail_next_range_open_shard).
 * a = shard number as (int64_t); -1 disarms (clears the pending shot). */
#define TEST_HOOK_SET_GLOBALS_BT_RANGE_OPEN_FAIL 9
#define TEST_HOOK_SET_GLOBALS_KEYSET_LEAF_FAIL_ALLOC 10
#define TEST_HOOK_GET_PASS1_PARALLEL_TAKEN 11   /* GET_GLOBALS selector */
#define TEST_HOOK_SET_GLOBALS_PASS1_RESET 12    /* a=1 resets the counter */
/* Arm the shard-walker open-failure seam: every btree_search /
 * btree_range_ex open of a path ending /<NNN>.idx for the given 3-hex
 * shard fails with EIO until disarmed (btree_test_fail_walk_open_shard).
 * Persistent — unlike selector 9's one-shot — because one query may walk
 * the same numbered shard of several fields. a = shard number as
 * (int64_t); -1 disarms. */
#define TEST_HOOK_SET_GLOBALS_BT_WALK_OPEN_FAIL 13
/* Restore fault seams: a = mask (SHARD_TEST_RF_*), b = swap-rename index.
   b is stored first so the armed index is visible before the mask. */
#define TEST_HOOK_SET_GLOBALS_RESTORE_FAULT 14
/* Restore park sites (INSTALL .phase): SWAPPED = after the renames, before
   the commit fsyncs; COMMITTED = after the commit, before the stage delete. */
#define TEST_HOOK_KIND_RESTORE_PARK_SWAPPED 5
#define TEST_HOOK_KIND_RESTORE_PARK_COMMITTED 6

typedef struct {
    uint32_t kind;   /* INSTALL..REACHED; SET_GLOBALS=6 */
    int32_t  phase;  /* REACHED: 0=stale snapshot, 1=under kf wrlock; else 0 */
    uint64_t a;      /* SET_GLOBALS: initial slots (0 = default) */
    uint64_t b;      /* SET_GLOBALS: test-only max slots (0 = uncapped) */
} TestHookMessage;

typedef struct {
    int             fd;
    pthread_t       thread;
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    int             running;
    int             waiting_for_release;
    int             release;
} TestControl;

static TestControl g_test_control;

static int test_control_write_full(int fd, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    while (n > 0) {
        ssize_t w = write(fd, p, n);
        if (w > 0) {
            p += w;
            n -= (size_t)w;
            continue;
        }
        if (w < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

static int test_control_read_full(int fd, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    while (n > 0) {
        ssize_t r = read(fd, p, n);
        if (r > 0) {
            p += r;
            n -= (size_t)r;
            continue;
        }
        if (r < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

/* Runs on the request thread inside the pause hook: reports the call-site
   phase (under_kf_wrlock) and blocks until the control thread broadcasts a
   release. A failed REACHED write releases immediately. */
static void test_control_after_old(int under_kf_wrlock, void *ctx_ptr) {
    TestControl *c = ctx_ptr;
    TestHookMessage reached = { .kind = TEST_HOOK_REACHED,
                                .phase = under_kf_wrlock };
    pthread_mutex_lock(&c->lock);
    c->waiting_for_release = 1;
    c->release = 0;
    pthread_cond_broadcast(&c->cond);
    pthread_mutex_unlock(&c->lock);

    if (test_control_write_full(c->fd, &reached, sizeof(reached)) != 0) {
        pthread_mutex_lock(&c->lock);
        c->release = 1;
        pthread_cond_broadcast(&c->cond);
        c->waiting_for_release = 0;
        pthread_mutex_unlock(&c->lock);
        return;
    }

    pthread_mutex_lock(&c->lock);
    while (c->running && !c->release)
        pthread_cond_wait(&c->cond, &c->lock);
    c->waiting_for_release = 0;
    pthread_mutex_unlock(&c->lock);
}

/* Runs on the count worker thread inside the pass-1 gap seam (installed by
   INSTALL kind 1 via shard_db_test_set_count_gap_hook): reports REACHED
   with phase 2 and blocks until the control thread broadcasts a release
   (or the daemon stops). Same contract as test_control_after_old; the
   caller holds the pass-1 KF reader while parked — that is the point. */
static void test_control_count_gap_block(void *ctx_ptr) {
    TestControl *c = ctx_ptr;
    TestHookMessage reached = { .kind = TEST_HOOK_REACHED, .phase = 2 };
    pthread_mutex_lock(&c->lock);
    c->waiting_for_release = 1;
    c->release = 0;
    pthread_cond_broadcast(&c->cond);
    pthread_mutex_unlock(&c->lock);

    if (test_control_write_full(c->fd, &reached, sizeof(reached)) != 0) {
        pthread_mutex_lock(&c->lock);
        c->release = 1;
        pthread_cond_broadcast(&c->cond);
        c->waiting_for_release = 0;
        pthread_mutex_unlock(&c->lock);
        return;
    }

    pthread_mutex_lock(&c->lock);
    while (c->running && !c->release)
        pthread_cond_wait(&c->cond, &c->lock);
    c->waiting_for_release = 0;
    pthread_mutex_unlock(&c->lock);
}

/* Runs on the request thread right after a passing guard check
   (installed by INSTALL kind 3 via shard_db_test_set_guard_hook):
   reports REACHED with phase 4 and blocks until the control thread
   broadcasts a release (or the daemon stops). Same contract as
   test_control_count_gap_block; the caller holds shared objlock and
   every writer gate after a passing guard check — that is the point. */
static void test_control_guard_block(void *ctx_ptr) {
    TestControl *c = ctx_ptr;
    TestHookMessage reached = { .kind = TEST_HOOK_REACHED, .phase = 4 };
    pthread_mutex_lock(&c->lock);
    c->waiting_for_release = 1;
    c->release = 0;
    pthread_cond_broadcast(&c->cond);
    pthread_mutex_unlock(&c->lock);

    if (test_control_write_full(c->fd, &reached, sizeof(reached)) != 0) {
        pthread_mutex_lock(&c->lock);
        c->release = 1;
        pthread_cond_broadcast(&c->cond);
        c->waiting_for_release = 0;
        pthread_mutex_unlock(&c->lock);
        return;
    }

    pthread_mutex_lock(&c->lock);
    while (c->running && !c->release)
        pthread_cond_wait(&c->cond, &c->lock);
    c->waiting_for_release = 0;
    pthread_mutex_unlock(&c->lock);
}

/* Runs on the find worker thread at the batch-fetch gate (installed by
   INSTALL kind 2 via shard_db_test_set_find_flush_gate_hook): reports
   REACHED with phase 3 and blocks until the control thread broadcasts a
   release (or the daemon stops). Same contract as the other park
   functions; the caller is about to issue a blocking batch fetch — on
   the legacy collector with the walk's kf+bitmap handles still held,
   on the deferred collector with none held. That is the point. */
static void test_control_find_flush_gate_block(void *ctx_ptr) {
    TestControl *c = ctx_ptr;
    TestHookMessage reached = { .kind = TEST_HOOK_REACHED, .phase = 3 };
    pthread_mutex_lock(&c->lock);
    c->waiting_for_release = 1;
    c->release = 0;
    pthread_cond_broadcast(&c->cond);
    pthread_mutex_unlock(&c->lock);

    if (test_control_write_full(c->fd, &reached, sizeof(reached)) != 0) {
        pthread_mutex_lock(&c->lock);
        c->release = 1;
        pthread_cond_broadcast(&c->cond);
        c->waiting_for_release = 0;
        pthread_mutex_unlock(&c->lock);
        return;
    }

    pthread_mutex_lock(&c->lock);
    while (c->running && !c->release)
        pthread_cond_wait(&c->cond, &c->lock);
    c->waiting_for_release = 0;
    pthread_mutex_unlock(&c->lock);
}

/* Runs on the request thread inside a restore park site (installed by
   INSTALL kind 5/6 via shard_db_test_set_restore_park_hook): reports
   REACHED with the site's phase (5 = swapped, 6 = committed) and blocks
   until the control thread broadcasts a release (or the daemon stops).
   Same contract as test_control_guard_block. */
static void test_control_restore_park_block(void *ctx_ptr, int phase) {
    TestControl *c = ctx_ptr;
    TestHookMessage reached = { .kind = TEST_HOOK_REACHED, .phase = phase };
    pthread_mutex_lock(&c->lock);
    c->waiting_for_release = 1;
    c->release = 0;
    pthread_cond_broadcast(&c->cond);
    pthread_mutex_unlock(&c->lock);

    if (test_control_write_full(c->fd, &reached, sizeof(reached)) != 0) {
        pthread_mutex_lock(&c->lock);
        c->release = 1;
        pthread_cond_broadcast(&c->cond);
        c->waiting_for_release = 0;
        pthread_mutex_unlock(&c->lock);
        return;
    }

    pthread_mutex_lock(&c->lock);
    while (c->running && !c->release)
        pthread_cond_wait(&c->cond, &c->lock);
    c->waiting_for_release = 0;
    pthread_mutex_unlock(&c->lock);
}

static void test_control_restore_park_swapped(void *ctx_ptr) {
    test_control_restore_park_block(ctx_ptr, 5);
}

static void test_control_restore_park_committed(void *ctx_ptr) {
    test_control_restore_park_block(ctx_ptr, 6);
}

static void *test_control_thread_main(void *arg) {
    TestControl *c = arg;
    for (;;) {
        TestHookMessage msg;
        if (test_control_read_full(c->fd, &msg, sizeof(msg)) != 0)
            break; /* EOF or I/O error */

        switch (msg.kind) {
        case TEST_HOOK_INSTALL:
            if (msg.phase == TEST_HOOK_KIND_COUNT_GAP ||
                msg.phase == TEST_HOOK_KIND_COUNT_GAP_LATE) {
                /* the armed value is the park site (1 or 2) */
                atomic_store(&g_shard_test_count_gap,
                             msg.phase == TEST_HOOK_KIND_COUNT_GAP ? 1 : 2);
                atomic_store(&g_shard_test_count_gap_hit, 0);
                shard_db_test_set_count_gap_hook(test_control_count_gap_block,
                                                 c);
            } else if (msg.phase == TEST_HOOK_KIND_FIND_FLUSH_GATE) {
                atomic_store(&g_shard_test_find_flush_gate, 1);
                atomic_store(&g_shard_test_find_flush_gate_hit, 0);
                shard_db_test_set_find_flush_gate_hook(
                    test_control_find_flush_gate_block, c);
            } else if (msg.phase == TEST_HOOK_KIND_GUARD_CHECK) {
                shard_db_test_set_guard_hook(test_control_guard_block, c);
            } else if (msg.phase == TEST_HOOK_KIND_RESTORE_PARK_SWAPPED) {
                shard_db_test_set_restore_park_hook(
                    SHARD_TEST_RESTORE_PARK_SWAPPED,
                    test_control_restore_park_swapped, c);
            } else if (msg.phase == TEST_HOOK_KIND_RESTORE_PARK_COMMITTED) {
                shard_db_test_set_restore_park_hook(
                    SHARD_TEST_RESTORE_PARK_COMMITTED,
                    test_control_restore_park_committed, c);
            } else {
                slotcask_test_set_after_old_hook(test_control_after_old, c);
            }
            break;
        case TEST_HOOK_RELEASE:
            pthread_mutex_lock(&c->lock);
            c->release = 1;
            pthread_cond_broadcast(&c->cond);
            pthread_mutex_unlock(&c->lock);
            break;
        case TEST_HOOK_SET_GLOBALS:
            if (msg.phase == TEST_HOOK_SET_GLOBALS_ESCAPE_FAIL) {
                atomic_store(&g_shard_test_json_escape_fail, (int)msg.a);
            } else if (msg.phase == TEST_HOOK_SET_GLOBALS_BMFIX_FAULT) {
                atomic_store(&g_shard_test_bmfix_fault_count, 0);
                atomic_store(&g_shard_test_bmfix_fault_mode, (int)msg.a);
                atomic_store(&g_shard_test_bmfix_fault_count, (int)msg.b);
            } else if (msg.phase == TEST_HOOK_SET_GLOBALS_BMLOCK_LOG) {
                atomic_store(&g_shard_test_bmlock_log, (int)msg.a);
            } else if (msg.phase == TEST_HOOK_SET_GLOBALS_BMKS_RACE) {
                if (msg.a) {
                    atomic_store(&g_shard_test_bmks_gate, 0);
                    atomic_store(&g_shard_test_bmks_stage, (int)msg.a);
                } else {
                    atomic_store(&g_shard_test_bmks_stage, 0);
                    atomic_store(&g_shard_test_bmks_gate, 1);
                }
            } else if (msg.phase == TEST_HOOK_SET_GLOBALS_FAIL_PHASE) {
                /* a = window-coordinator fail phase ((long)(int64_t)a so
                   -1 round-trips through the uint64 field), b = 1-based
                   occurrence. Arm order matters: zero the per-phase
                   counters first (occurrence is counted from the arm,
                   matching the workers' __atomic_add_fetch), then
                   occurrence, then phase — a worker that sees the armed
                   phase must already see the occurrence. Disarm stores
                   phase first so workers stop failing before anything
                   else moves. */
                long fail_phase = (long)(int64_t)msg.a;
                if (fail_phase >= 0) {
                    for (int i = 0; i < SHARD_TEST_PHASE_COUNT; i++)
                        __atomic_store_n(&g_shard_test_sync_counts[i], 0,
                                         __ATOMIC_RELAXED);
                    atomic_store(&g_shard_test_fail_occurrence, (long)msg.b);
                    atomic_store(&g_shard_test_fail_phase, fail_phase);
                } else {
                    atomic_store(&g_shard_test_fail_phase, fail_phase);
                }
            } else if (msg.phase == TEST_HOOK_SET_GLOBALS_FAIL_STICKY) {
                atomic_store(&g_shard_test_fail_sticky, msg.a ? 1 : 0);
            } else if (msg.phase == TEST_HOOK_SET_GLOBALS_BMFIX_BACKOFF) {
                /* a = interval in ms ((int64_t) round-trip); <= 0 means the
                   production default. */
                atomic_store(&g_shard_test_bmfix_backoff_ms, (long)(int64_t)msg.a);
            } else if (msg.phase == TEST_HOOK_SET_GLOBALS_PAUSE_PHASE) {
                /* a = ShardTestPhase ((int64_t) round-trip; -1 disarms),
                   b = 1-based occurrence. Arm order matters, mirroring
                   FAIL_PHASE: a worker must see hits==0 (fresh counting)
                   and the occurrence before it sees the armed phase, so
                   the first matching window after this ACK is the one
                   that parks. Disarm stores the phase first so a parked
                   worker stops waiting for a release that will never
                   come. */
                int pause_phase = (int)(int64_t)msg.a;
                if (pause_phase >= 0) {
                    atomic_store(&g_shard_test_pause_hits, 0);
                    atomic_store(&g_shard_test_pause_release, 0);
                    atomic_store(&g_shard_test_pause_occurrence,
                                 (int)msg.b);
                    atomic_store(&g_shard_test_pause_phase, pause_phase);
                } else {
                    /* Disarm: store the phase first so no NEW hit parks,
                       then raise the release flag — the parked worker
                       polls g_shard_test_pause_release every 1 ms and
                       would otherwise sit out its full 30 s safety cap
                       waiting for a release that will never come. */
                    atomic_store(&g_shard_test_pause_phase, -1);
                    atomic_store(&g_shard_test_pause_release, 1);
                }
            } else if (msg.phase ==
                       TEST_HOOK_SET_GLOBALS_BT_RANGE_OPEN_FAIL) {
                /* a = index shard number ((int64_t) round-trip); -1
                   disarms a pending (unconsumed) failure shot. b = how
                   many matching opens to skip before firing (reaches a
                   later route's open of the same numbered shard). */
                btree_test_fail_nth_range_open_shard((int)(int64_t)msg.a,
                                                     (int)msg.b);
            } else if (msg.phase == TEST_HOOK_SET_GLOBALS_PASS1_RESET) {
                /* a = 1: zero the Pass-1 parallel-route counter. */
                if (msg.a)
                    igb_pass1_parallel_reset_for_test();
            } else if (msg.phase ==
                       TEST_HOOK_SET_GLOBALS_KEYSET_LEAF_FAIL_ALLOC) {
                /* a = countdown of per-leaf KeySet-build allocations to
                   fail with ENOMEM (atomic; 0 disarms). Deterministic
                   hard-failure trigger for the find-side keyset builds,
                   whose shard walks do not go through
                   btree_range_iter_open (selector 9 does not reach
                   them). */
                build_keyset_leaf_test_set_fail_alloc((int)(int64_t)msg.a);
            } else if (msg.phase ==
                       TEST_HOOK_SET_GLOBALS_BT_WALK_OPEN_FAIL) {
                /* a = index shard number; every btree_search /
                   btree_range_ex open of that shard's .idx fails with EIO
                   until disarmed (a = -1) — persistent, because a query
                   may walk the same numbered shard of several fields and
                   the test must not depend on which walk comes first. */
                btree_test_fail_walk_open_shard((int)(int64_t)msg.a);
            } else if (msg.phase == TEST_HOOK_SET_GLOBALS_KF_SEAMS) {
                atomic_store(&g_shard_test_kf_initial_slots, (size_t)msg.a);
                atomic_store(&g_shard_test_kf_max_slots, (size_t)msg.b);
            } else if (msg.phase == TEST_HOOK_SET_GLOBALS_RESTORE_FAULT) {
                atomic_store(&g_restore_test_swap_rename_at, (int)msg.b);
                atomic_store(&g_restore_test_fault_mask, (int)msg.a);
            }
            /* an unknown selector is ignored (still ACKed): it must never
               fall through into the kf seams */
            break;
        case TEST_HOOK_CLEAR:
            slotcask_test_set_after_old_hook(NULL, NULL);
            shard_db_test_set_count_gap_hook(NULL, NULL);
            shard_db_test_set_find_flush_gate_hook(NULL, NULL);
            shard_db_test_set_guard_hook(NULL, NULL);
            shard_db_test_set_restore_park_hook(0, NULL, NULL);
            atomic_store(&g_shard_test_count_gap, 0);
            atomic_store(&g_shard_test_find_flush_gate, 0);
            pthread_mutex_lock(&c->lock);
            c->release = 1;
            pthread_cond_broadcast(&c->cond);
            pthread_mutex_unlock(&c->lock);
            break;
        case TEST_HOOK_GET_GLOBALS:
            /* Reply ACK carries the queried value in .a. Only the Pass-1
               route counter today; extend with more selectors as needed. */
            {
                /* Unknown selectors reply UINT64_MAX so a client never
                   blocks waiting for an answer that will not come. */
                uint64_t val = UINT64_MAX;
                if (msg.phase == TEST_HOOK_GET_PASS1_PARALLEL_TAKEN)
                    val = (uint64_t)igb_pass1_parallel_taken_for_test();
                TestHookMessage rep2 = { .kind = TEST_HOOK_ACK,
                                         .phase = 0, .a = val };
                if (test_control_write_full(c->fd, &rep2, sizeof(rep2)) != 0)
                    return NULL;
                continue;   /* the shared ACK below must not double-reply */
            }
        }

        if (msg.kind == TEST_HOOK_INSTALL || msg.kind == TEST_HOOK_RELEASE ||
            msg.kind == TEST_HOOK_CLEAR || msg.kind == TEST_HOOK_SET_GLOBALS) {
            TestHookMessage ack = { .kind = TEST_HOOK_ACK, .phase = 0 };
            if (test_control_write_full(c->fd, &ack, sizeof(ack)) != 0) break;
        } else {
            break;
        }
    }
    return NULL;
}

int shard_db_test_control_start(int fd) {
    if (fd < 0) return 0; /* idempotent */
    memset(&g_test_control, 0, sizeof(g_test_control));
    g_test_control.fd = fd;
    pthread_mutex_init(&g_test_control.lock, NULL);
    pthread_cond_init(&g_test_control.cond, NULL);
    g_test_control.running = 1;
    if (pthread_create(&g_test_control.thread, NULL,
                       test_control_thread_main, &g_test_control) != 0) {
        g_test_control.running = 0;
        close(fd);
        pthread_mutex_destroy(&g_test_control.lock);
        pthread_cond_destroy(&g_test_control.cond);
        return -1;
    }
    return 0;
}

void shard_db_test_control_stop(void) {
    if (g_test_control.fd < 0 || !g_test_control.running) return;
    /* Release a parked callback and prevent any future fire, then wake the
       control thread's blocking read. */
    slotcask_test_set_after_old_hook(NULL, NULL);
    shard_db_test_set_count_gap_hook(NULL, NULL);
    shard_db_test_set_find_flush_gate_hook(NULL, NULL);
    shard_db_test_set_guard_hook(NULL, NULL);
    shard_db_test_set_restore_park_hook(0, NULL, NULL);
    pthread_mutex_lock(&g_test_control.lock);
    g_test_control.running = 0;
    g_test_control.release = 1;
    pthread_cond_broadcast(&g_test_control.cond);
    pthread_mutex_unlock(&g_test_control.lock);
    shutdown(g_test_control.fd, SHUT_RDWR);
    pthread_join(g_test_control.thread, NULL);
    close(g_test_control.fd);
    g_test_control.fd = -1;
    pthread_mutex_destroy(&g_test_control.lock);
    pthread_cond_destroy(&g_test_control.cond);
}

#endif /* TEST_BUILD */
