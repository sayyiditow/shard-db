/* Global compute-parallelism thread pool.
 *
 * Problem it solves: hot paths (bulk-insert Phase 2, parallel index build,
 * shard activation, scan_shards, ...) each used to spawn their own
 * pthread_create/join batch. Under N concurrent TCP callers, each spawning
 * P threads, the server ran N*P OS threads on 16 cores — 10x overcommit for
 * N=10,P=16 — and most of the wall time was OS scheduling delay, not work.
 *
 * Fix: a fixed-size worker pool sized by THREADS config (default = nproc).
 * All callers submit tasks to one shared queue; workers drain it. No
 * overcommit, no per-call thread creation cost.
 *
 * API:
 *   parallel_pool_init(n)      — called once at server startup
 *   parallel_pool_shutdown()   — called at server shutdown
 *   parallel_for(fn, args, n, stride)
 *       — submit n tasks (fn(args + i*stride) for i in [0,n)), block until all
 *         complete. Mirrors the existing "spawn + join" idiom at the call
 *         site, but cooperates with other concurrent callers via the pool.
 *
 * Nesting: parallel_for is structurally safe under nesting (a parallel_for
 * task can itself call parallel_for) AND under concurrent callers. While
 * the caller waits for its task group to finish, it doesn't passively
 * block — it pops tasks from the global queue and runs them itself
 * (work-stealing). Every blocked thread is therefore an active drain on
 * the queue, so the queue can never stall: there's always at least one
 * runner per pending task. This holds regardless of pool_size vs
 * outer-task-count, so callers don't need any guard like `n < pool_size`.
 *
 * The fallback when the queue is momentarily empty but our group still has
 * tasks in flight (running on other workers) is a 1ms timed cond_wait on
 * the group's cv, which naturally re-checks the queue on wake-up.
 */

#include "types.h"
#include <pthread.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    /* _Atomic so help-drain loops can read remaining lock-free between
       wait-checks; the mutex still gates the cond_broadcast on completion
       to prevent the classic check-then-wait lost-wakeup race. */
    _Atomic int remaining;
} PoolGroup;

typedef struct {
    void *(*fn)(void *);
    void *arg;
    PoolGroup *group;
} PoolTask;

/* Bounded queue. Capacity chosen to comfortably absorb N callers * P tasks
 * without blocking producers in the common case (256 shards * 16 callers). */
#define POOL_QUEUE_CAP 8192

static PoolTask  g_queue[POOL_QUEUE_CAP];
static int       g_q_head = 0;
static int       g_q_tail = 0;
static int       g_q_count = 0;
static pthread_mutex_t g_q_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_q_not_empty = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  g_q_not_full  = PTHREAD_COND_INITIALIZER;

static pthread_t *g_pool_threads = NULL;
static int        g_pool_nthreads = 0;

/* Submit-batch size for parallel_for (POOL_CHUNK in db.env). 0 = auto
   (= nproc). Defined here so the symbol travels with the rest of the
   pool machinery — test/bench binaries link parallel.c without config.c
   and the variable is then never written, leaving the auto-default. */
int g_pool_chunk = 0;

/* Per-thread flag: 1 while a pool worker is running a task, 0 otherwise.
   parallel_for uses this to decide between two wait strategies:
     - Top-level callers (TCP handler, CLI) → plain cond_wait. Pool
       workers grab and run tasks in parallel; the caller doesn't
       compete for them, which gives full pool parallelism on the
       common single-level case.
     - Nested callers (already inside a pool task) → help-drain. The
       caller pops tasks from the queue and runs them itself while
       waiting, so the pool can never starve when (concurrent_outer ×
       inner_count) >= pool_size. This is the deadlock-prevention path. */
static __thread int t_in_pool_task = 0;
/* Read lock-free at parallel_pool_init/shutdown entry, parallel_pool_size,
   and parallel_for; written under g_q_lock at parallel_pool_shutdown and
   lock-free at parallel_pool_init. _Atomic gives correct cross-thread
   visibility regardless of which writer-lock was held. (Was volatile —
   volatile is for memory-mapped IO, not thread synchronization.) */
static _Atomic int g_pool_running = 0;

/* Pop one task from the head of the global queue if non-empty. Returns 1
   on success (caller owns *out and must run it), 0 if queue is empty.
   Used by both pool_worker and the help-drain loop in parallel_for. */
static int try_pop_task(PoolTask *out) {
    pthread_mutex_lock(&g_q_lock);
    if (g_q_count == 0) {
        pthread_mutex_unlock(&g_q_lock);
        return 0;
    }
    *out = g_queue[g_q_head];
    g_q_head = (g_q_head + 1) % POOL_QUEUE_CAP;
    g_q_count--;
    pthread_cond_signal(&g_q_not_full);
    pthread_mutex_unlock(&g_q_lock);
    return 1;
}

/* Run a task, then atomically decrement its group's remaining counter.
   Broadcast the group's cv when remaining hits zero so the parallel_for
   waiter (and any help-draining sibling) wakes promptly. The mutex
   bracket around the broadcast prevents the lost-wakeup race against
   waiters that just read remaining > 0 and are about to cond_wait. */
static void run_task_finish(PoolTask t) {
    t.fn(t.arg);
    int prev = atomic_fetch_sub_explicit(&t.group->remaining, 1,
                                         memory_order_acq_rel);
    if (prev == 1) {
        pthread_mutex_lock(&t.group->mu);
        pthread_cond_broadcast(&t.group->cv);
        pthread_mutex_unlock(&t.group->mu);
    }
}

static void *pool_worker(void *arg) {
    (void)arg;
    while (1) {
        pthread_mutex_lock(&g_q_lock);
        while (g_q_count == 0 && g_pool_running)
            pthread_cond_wait(&g_q_not_empty, &g_q_lock);
        if (g_q_count == 0) { pthread_mutex_unlock(&g_q_lock); return NULL; }
        PoolTask t = g_queue[g_q_head];
        g_q_head = (g_q_head + 1) % POOL_QUEUE_CAP;
        g_q_count--;
        pthread_cond_signal(&g_q_not_full);
        pthread_mutex_unlock(&g_q_lock);

        t_in_pool_task = 1;
        run_task_finish(t);
        t_in_pool_task = 0;
    }
}

void parallel_pool_init(int nthreads) {
    if (g_pool_running) return;
    if (nthreads <= 0) nthreads = parallel_threads();
    if (nthreads < 2) nthreads = 2;
    g_pool_nthreads = nthreads;
    g_pool_threads = malloc((size_t)nthreads * sizeof(pthread_t));
    g_pool_running = 1;
    for (int i = 0; i < nthreads; i++)
        pthread_create(&g_pool_threads[i], NULL, pool_worker, NULL);
}

void parallel_pool_shutdown(void) {
    /* coverity[lock_evasion] coverity[missing_lock] intentional lock-free
       fast-skip — `_Atomic int` gives torn-read-free visibility; if a
       concurrent init flipped the flag to 1 between this check and the
       lock below, we'd just take the lock and do nothing. Idempotent
       shutdown is correct either way. */
    if (!g_pool_running) return;
    pthread_mutex_lock(&g_q_lock);
    g_pool_running = 0;
    pthread_cond_broadcast(&g_q_not_empty);
    pthread_mutex_unlock(&g_q_lock);
    for (int i = 0; i < g_pool_nthreads; i++)
        pthread_join(g_pool_threads[i], NULL);
    free(g_pool_threads);
    g_pool_threads = NULL;
    g_pool_nthreads = 0;
}

int parallel_pool_size(void) { return g_pool_running ? g_pool_nthreads : 0; }

static void enqueue_locked(PoolTask t) {
    while (g_q_count >= POOL_QUEUE_CAP)
        pthread_cond_wait(&g_q_not_full, &g_q_lock);
    g_queue[g_q_tail] = t;
    g_q_tail = (g_q_tail + 1) % POOL_QUEUE_CAP;
    g_q_count++;
    pthread_cond_signal(&g_q_not_empty);
}

void parallel_for(void *(*fn)(void *), void *args, int n, size_t stride) {
    if (n <= 0) return;
    if (!g_pool_running || n == 1) {
        /* Pool unavailable (e.g. CLI mode) or only one task — run inline. */
        for (int i = 0; i < n; i++) fn((char *)args + (size_t)i * stride);
        return;
    }

    PoolGroup group;
    pthread_mutex_init(&group.mu, NULL);
    pthread_cond_init(&group.cv, NULL);
    atomic_init(&group.remaining, n);

    /* Enqueue in small chunks, releasing the queue lock between chunks so
       concurrent callers' tasks interleave in the FIFO queue (rather than
       the pool draining caller-by-caller). Default = core count: chunk=1
       causes submitters and workers to fight over g_q_lock constantly;
       very large chunks re-serialize callers. Core-count is a decent
       middle ground (one chunk ≈ one worker-cycle). Configurable via
       POOL_CHUNK in db.env. */
    static int SUBMIT_CHUNK = 0;
    if (SUBMIT_CHUNK == 0) {
        if (g_pool_chunk > 0) {
            SUBMIT_CHUNK = g_pool_chunk;
        } else {
            long nproc = sysconf(_SC_NPROCESSORS_ONLN);
            SUBMIT_CHUNK = (nproc > 0) ? (int)nproc : 16;
        }
    }
    for (int i = 0; i < n; i += SUBMIT_CHUNK) {
        int end = i + SUBMIT_CHUNK;
        if (end > n) end = n;
        pthread_mutex_lock(&g_q_lock);
        for (int j = i; j < end; j++) {
            PoolTask t = { fn, (char *)args + (size_t)j * stride, &group };
            enqueue_locked(t);
        }
        pthread_mutex_unlock(&g_q_lock);
    }

    if (t_in_pool_task) {
        /* Nested call (we're already running inside a pool task). If we
           cond_wait passively, all pool workers could end up blocked on
           their own outer tasks waiting for inner tasks that no one is
           draining → deadlock. Help-drain instead: while our group has
           tasks remaining, pop and run any task from the global queue.
           When the queue is momentarily empty but our group still has
           tasks running on other workers, fall back to a 1ms timed
           cond_wait that re-checks both queue and group state on wake. */
        while (atomic_load_explicit(&group.remaining, memory_order_acquire) > 0) {
            PoolTask t;
            if (try_pop_task(&t)) {
                run_task_finish(t);
                continue;
            }
            pthread_mutex_lock(&group.mu);
            if (atomic_load_explicit(&group.remaining, memory_order_acquire) > 0) {
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_nsec += 1000000;  /* 1ms re-check cadence */
                if (ts.tv_nsec >= 1000000000) { ts.tv_sec++; ts.tv_nsec -= 1000000000; }
                pthread_cond_timedwait(&group.cv, &group.mu, &ts);
            }
            pthread_mutex_unlock(&group.mu);
        }
    } else {
        /* Top-level call (TCP handler / CLI thread). Plain cond_wait
           lets pool workers run the tasks in parallel without the
           caller competing for them — the common case where the pool
           is otherwise idle. */
        pthread_mutex_lock(&group.mu);
        while (atomic_load_explicit(&group.remaining, memory_order_acquire) > 0)
            pthread_cond_wait(&group.cv, &group.mu);
        pthread_mutex_unlock(&group.mu);
    }

    pthread_mutex_destroy(&group.mu);
    pthread_cond_destroy(&group.cv);
}
