#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <pthread.h>
#include <time.h>

/* Regression for the production hang (docs/plans/2026-08-10-
 * parallel-pool-cross-pool-nesting-starvation.md): an IO-pool worker
 * calling into the CPU pool (parallel_for) — exactly what bulk-insert's
 * apply_window -> idx_build_field_worker does — was not recognised as a
 * nested call, so it queued and blocked like a genuine top-level caller
 * instead of running inline. Under concurrent load this collapses all
 * cross-pool parallelism onto the CPU pool's fixed worker count. */

#define CPU_POOL_SIZE 2
#define IO_POOL_SIZE  8
#define INNER_N       4
#define TASK_SLEEP_MS 50

static void *cpu_sleep_task(void *arg) {
    (void)arg;
    struct timespec ts = { .tv_sec = 0, .tv_nsec = TASK_SLEEP_MS * 1000000L };
    nanosleep(&ts, NULL);
    return NULL;
}

static void *io_nested_task(void *arg) {
    (void)arg;
    /* Cross-pool nested call: runs on an IO-pool worker thread, calls
       into the CPU pool. Mirrors v2_bulk_ins_apply_window's call into
       idx_build_field_worker in the real bulk-insert commit path. */
    parallel_for(cpu_sleep_task, NULL, INNER_N, 0);
    return NULL;
}

typedef struct {
    int done;
    pthread_mutex_t mu;
    pthread_cond_t cv;
} Completion;

static void *driver_thread(void *arg) {
    Completion *c = (Completion *)arg;
    parallel_for_io(io_nested_task, NULL, IO_POOL_SIZE, 0);
    pthread_mutex_lock(&c->mu);
    c->done = 1;
    pthread_cond_signal(&c->cv);
    pthread_mutex_unlock(&c->mu);
    return NULL;
}

static long cpn_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static int test_parallel_cross_pool_nesting_run(void) {
    parallel_pool_init(CPU_POOL_SIZE);
    parallel_io_pool_init(IO_POOL_SIZE);

    Completion c;
    c.done = 0;
    pthread_mutex_init(&c.mu, NULL);
    pthread_cond_init(&c.cv, NULL);

    long start = cpn_now_ms();
    pthread_t driver;
    ASSERT_EQ_INT(pthread_create(&driver, NULL, driver_thread, &c), 0, "spawn driver");

    /* Bound the wait: a genuine hang must fail this test in ~2s, not
       burn the harness's 180s per-case watchdog. */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 2;

    pthread_mutex_lock(&c.mu);
    int rc = 0;
    while (!c.done && rc == 0)
        rc = pthread_cond_timedwait(&c.cv, &c.mu, &deadline);
    int completed = c.done;
    pthread_mutex_unlock(&c.mu);

    long elapsed = cpn_now_ms() - start;

    ASSERT_TRUE(completed, "cross-pool nested parallel_for_io completed within 2s");

    if (completed) {
        pthread_join(driver, NULL);
        /* Fixed: 8 IO workers each run their own 4 sub-tasks inline, in
           parallel with each other -> ~4*50ms = 200ms wall time.
           Buggy: all 8*4=32 sub-tasks funnel through 2 CPU-pool workers
           -> ~32/2*50ms = 800ms+. Wide margin (500ms) to stay
           non-flaky under CI load while still failing hard on the bug. */
        ASSERT_TRUE(elapsed < 500, "cross-pool nesting preserves parallelism (<500ms)");
        parallel_io_pool_shutdown();
        parallel_pool_shutdown();
    } else {
        /* Don't call *_shutdown() here: it joins pool worker threads,
           which may themselves be stuck. Detach and leave the leaked
           threads/pools behind — the test has already failed via the
           assertion above, and this test always runs in its own worker
           process under run-all's default mode. */
        pthread_detach(driver);
    }

    pthread_mutex_destroy(&c.mu);
    pthread_cond_destroy(&c.cv);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-parallel-cross-pool-nesting", test_parallel_cross_pool_nesting_run)
