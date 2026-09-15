#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "shard_db_internal.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <unistd.h>

/* Regression test for the objlock table-exhaustion bug: on the old
   fixed-size table, once every bucket held a distinct key, get_lock()
   returned NULL and every objlock_rdlock/wrlock call silently proceeded
   WITHOUT taking any lock. This test creates far more than the old
   256-bucket ceiling worth of distinct (db_root, object) keys and then
   proves real mutual exclusion still holds on one of the keys well past
   that ceiling, and that an injected allocation failure is reported
   rather than swallowed. See docs/plans/2026-09-06-objlock-dynamic-directory.md
   (red evidence: docs/plans/2026-09-06-objlock-dynamic-directory.red-evidence.md).

   Mutual-exclusion proof design (deterministic, not timing-pass): while
   the reader holds the rdlock, a writer thread must NOT be able to
   acquire — polled for 500ms. A fail-open implementation enters within
   microseconds of the writer thread starting, so this catches it with
   no race in the pass direction; the only scheduling assumption is that
   a spawnable thread runs within 500ms (the same assumption
   test_objlock_unit.c's barriers make, with margin). The writer then
   MUST acquire after the reader releases — waited on unbounded. */

#define NUM_KEYS 2000
#define PAST_CEILING_INDEX 1500 /* > old OBJLOCK_BUCKETS (256) */

static _Atomic int g_growth_rd_ready;
static _Atomic int g_growth_rd_release;
static _Atomic int g_growth_reader_lock_failed;
static _Atomic int g_growth_writer_entered;

static void *growth_rd_wait_worker(void *arg) {
    const char *obj = (const char *)arg;
    if (objlock_rdlock("growth-root", obj) != 0) {
        atomic_store(&g_growth_reader_lock_failed, 1);
        atomic_store(&g_growth_rd_ready, 1); /* release main's wait */
        return NULL; /* never unlock a lock that was never taken */
    }
    atomic_store(&g_growth_rd_ready, 1);
    while (atomic_load(&g_growth_rd_release) == 0) usleep(1000);
    objlock_rdunlock("growth-root", obj);
    return NULL;
}

/* Runs objlock_wrlock off-thread; asserts stay on main (t_ctx is
   __thread). Acquiring is recorded, releasing is left to main. */
static void *growth_wr_worker(void *arg) {
    const char *obj = (const char *)arg;
    if (objlock_wrlock("growth-root", obj) == 0)
        atomic_store(&g_growth_writer_entered, 1);
    return NULL;
}

static void wait_for(atomic_int *flag, int expected, int max_polls) {
    for (int i = 0; i < max_polls && atomic_load(flag) != expected; i++)
        usleep(1000);
}

static int test_objlock_dynamic_growth_run(void) {
    objlock_init();

    char names[NUM_KEYS][32];
    for (int i = 0; i < NUM_KEYS; i++) {
        snprintf(names[i], sizeof(names[i]), "obj-%d", i);
        int rc = objlock_rdlock("growth-root", names[i]);
        ASSERT_EQ_INT(rc, 0, "rdlock succeeds well past the old 256-bucket ceiling");
        objlock_rdunlock("growth-root", names[i]);
    }

    /* Real mutual-exclusion proof on a key past the old ceiling. */
    const char *key = names[PAST_CEILING_INDEX];
    pthread_t reader, writer;
    atomic_store(&g_growth_rd_ready, 0);
    atomic_store(&g_growth_rd_release, 0);
    atomic_store(&g_growth_reader_lock_failed, 0);
    atomic_store(&g_growth_writer_entered, 0);

    pthread_create(&reader, NULL, growth_rd_wait_worker, (void *)key);
    wait_for(&g_growth_rd_ready, 1, 30000);
    ASSERT_EQ_INT(atomic_load(&g_growth_reader_lock_failed), 0,
                  "reader acquired its rdlock cleanly");

    /* While the reader holds, the writer must stay blocked. On the
       fail-open pre-fix path the writer's "lock" is a no-op and it
       enters within microseconds — this assertion is what catches that,
       deterministically. */
    pthread_create(&writer, NULL, growth_wr_worker, (void *)key);
    wait_for(&g_growth_writer_entered, 1, 500); /* poll up to 500ms */
    ASSERT_EQ_INT(atomic_load(&g_growth_writer_entered), 0,
                  "writer stayed blocked while the reader held the rdlock - a fail-open lock would enter immediately");

    /* Release the reader; the blocked writer must now acquire
       (unbounded wait -- no timing assumption in the pass direction). */
    atomic_store(&g_growth_rd_release, 1);
    pthread_join(reader, NULL);
    wait_for(&g_growth_writer_entered, 1, 30000);
    ASSERT_EQ_INT(atomic_load(&g_growth_writer_entered), 1,
                  "writer acquired after the reader released - real mutual exclusion past the old 256-object ceiling");
    objlock_wrunlock("growth-root", key);
    pthread_join(writer, NULL);

    /* Injected allocation failure must fail closed. */
    objlock_test_set_fail_alloc(1);
    int fail_rc = objlock_rdlock("growth-root", "never-seen-before-key");
    ASSERT_EQ_INT(fail_rc, -1, "injected allocation failure is reported, not swallowed");
    objlock_test_set_fail_alloc(0);
    /* No matching rdunlock: the lock was never taken. */

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-objlock-dynamic-growth", test_objlock_dynamic_growth_run)

/* Confirms objlock_test_set_fail_alloc fails exactly the Nth guarded
   allocation and that the directory is left in a valid, still-usable
   state afterward (a subsequent unrelated key succeeds). */
static int test_objlock_fail_alloc_recovery_run(void) {
    objlock_init();
    objlock_test_set_fail_alloc(1);
    int rc1 = objlock_rdlock("failtest-root", "will-fail");
    ASSERT_EQ_INT(rc1, -1, "injected failure reported");
    objlock_test_set_fail_alloc(0);
    int rc2 = objlock_rdlock("failtest-root", "will-succeed");
    ASSERT_EQ_INT(rc2, 0, "directory still usable after a prior failed resolve");
    objlock_rdunlock("failtest-root", "will-succeed");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-objlock-fail-alloc-recovery", test_objlock_fail_alloc_recovery_run)
