/* src/db/shard_test_ctl.h
 *
 * TEST_BUILD-only window-coordinator instrumentation for the per-Kf-window
 * durability regression suite (docs/plans/2026-08-21-main-durability-and-
 * window.md, Task 1). Production builds never include this header; the
 * definitions live in slotcask.c under the same guard and are inert unless
 * a test arms them. The coordinator barriers call shard_test_note_sync()
 * once per durable-sync barrier and shard_test_phase_pause() at each phase
 * boundary; both are compiled out entirely in non-TEST_BUILD builds via
 * the guard at the call sites.
 */
#ifndef SHARD_DB_SHARD_TEST_CTL_H
#define SHARD_DB_SHARD_TEST_CTL_H

#ifdef TEST_BUILD

#include <stdatomic.h>
#include <time.h>

typedef enum {
    SHARD_TEST_PHASE_P = 0,   /* payload staging barrier */
    SHARD_TEST_PHASE_M,       /* marker publication barrier */
    SHARD_TEST_PHASE_A,       /* activation barrier */
    SHARD_TEST_PHASE_I,       /* secondary-index barrier */
    SHARD_TEST_PHASE_K,       /* kf slot/header barrier */
    SHARD_TEST_PHASE_T,       /* old-payload tombstone barrier */
    SHARD_TEST_PHASE_C,       /* marker clear barrier */
    SHARD_TEST_PHASE_COUNT
} ShardTestPhase;

extern long g_shard_test_sync_counts[SHARD_TEST_PHASE_COUNT];
extern long g_shard_test_fail_phase;       /* -1 = disabled */
extern long g_shard_test_fail_occurrence;  /* 1-based */
extern int  g_shard_test_fail_postlink;    /* M: fail after link() (pending) */
extern int  g_shard_test_fail_sticky;      /* fail every hit >= occurrence,
                                             * not just the one exact match --
                                             * simulates a persistent fault
                                             * (e.g. disk full) that also
                                             * defeats the coordinator's own
                                             * inline forward-replay retry. */
extern int  g_shard_test_pause_phase;      /* -1 = disabled */
extern int  g_shard_test_pause_occurrence; /* 1-based */
extern _Atomic int g_shard_test_pause_hits;
extern _Atomic int g_shard_test_pause_release;

static inline void shard_test_ctl_reset(void) {
    for (int i = 0; i < SHARD_TEST_PHASE_COUNT; i++)
        g_shard_test_sync_counts[i] = 0;
    g_shard_test_fail_phase = -1;
    g_shard_test_fail_occurrence = 0;
    g_shard_test_fail_postlink = 0;
    g_shard_test_fail_sticky = 0;
    g_shard_test_pause_phase = -1;
    g_shard_test_pause_occurrence = 1;
    atomic_store(&g_shard_test_pause_hits, 0);
    atomic_store(&g_shard_test_pause_release, 0);
}

/* Barrier call: count the sync in `phase`; return 1 when this attempt is
 * the armed failure (the barrier must then fail and return -1). */
static inline int shard_test_note_sync(int phase) {
    if (phase < 0 || phase >= SHARD_TEST_PHASE_COUNT) return 0;
    /* concurrent Kf windows on different shards note-sync in parallel */
    long n = __atomic_add_fetch(&g_shard_test_sync_counts[phase], 1,
                                __ATOMIC_RELAXED);
    if (g_shard_test_fail_phase == (long)phase &&
        g_shard_test_fail_occurrence > 0) {
        if (n == g_shard_test_fail_occurrence)
            return 1;
        if (g_shard_test_fail_sticky &&
            g_shard_test_sync_counts[phase] > g_shard_test_fail_occurrence)
            return 1;
    }
    return 0;
}

/* Phase-boundary call: block the coordinator thread when armed, until the
 * test releases (or a 30 s safety cap expires). */
static inline void shard_test_phase_pause(int phase) {
    if (phase < 0 || phase >= SHARD_TEST_PHASE_COUNT) return;
    if (g_shard_test_pause_phase != phase) return;
    int hit = atomic_fetch_add(&g_shard_test_pause_hits, 1) + 1;
    if (hit != g_shard_test_pause_occurrence) return;
    for (int i = 0; i < 30000; i++) {
        if (atomic_load(&g_shard_test_pause_release)) return;
        struct timespec ts = { 0, 1000000L };
        nanosleep(&ts, NULL);
    }
}

#endif /* TEST_BUILD */
#endif /* SHARD_DB_SHARD_TEST_CTL_H */
