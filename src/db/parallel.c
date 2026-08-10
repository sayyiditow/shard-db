/* Global compute-parallelism thread pool.
 *
 * Problem it solves: hot CPU paths (parallel index build, index-update
 * fan-out, OR-leaf planning, aggregate shard workers, ...) each used to
 * spawn their own
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
 * task can itself call parallel_for, or parallel_for_io, or vice versa)
 * AND under concurrent callers. A thread that is already executing as a
 * worker of either pool — CPU or IO — runs a nested call's tasks
 * serially, inline, on itself rather than submitting them to the shared
 * queue. This trades away fan-out parallelism for that nested call, but
 * guarantees forward progress no matter how many nested callers pile up
 * concurrently: none of them ever compete for the fixed-size worker
 * pool's own capacity, so the pool can never be starved by its own
 * nested callers. This holds for same-pool nesting and cross-pool
 * nesting alike (both check the same t_in_pool_worker flag), so callers
 * don't need any guard like `n < pool_size`.
 */

#include "types.h"
#include <pthread.h>
#include <sched.h>
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
    /* Number of workers currently inside the post-fn body of
       run_task_finish (the window that touches group->mu / group->cv).
       Bumped before fetch_sub on remaining, decremented after the
       broadcast unlock. Without this, the help-drain path can read
       remaining==0 via acquire (after fetch_sub release-publishes it),
       exit its wait loop, and proceed to pthread_mutex_destroy while
       the last worker is still between fetch_sub and the
       lock+broadcast+unlock at run_task_finish — TSan trips, and on
       hardware that destroys the mutex bytes (glibc does) the worker
       can deadlock or trash a reused slot. */
    _Atomic int finishing;
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

/* Compute thread-pool size knob (THREADS in db.env). 0 = auto (nproc).
   Same rationale as g_pool_chunk — kept here so parallel.c links
   standalone in test/bench builds. */
int g_max_threads = 0;

/* I/O thread pool — persistent workers, separate queue.
   Parallelises I/O-bound work (mmap page faults, segment file reads)
   that benefits from oversubscription. Sized independently from the
   CPU pool so long page-fault waits don't starve CPU-bound queries. */
static pthread_t *g_io_pool_threads = NULL;
static int        g_io_nthreads = 0;
static _Atomic int g_io_running = 0;

#define IO_QUEUE_CAP 8192
static PoolTask   g_io_queue[IO_QUEUE_CAP];
static int        g_io_q_head = 0, g_io_q_tail = 0, g_io_q_count = 0;
static pthread_mutex_t g_io_q_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_io_q_not_empty = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  g_io_q_not_full  = PTHREAD_COND_INITIALIZER;

int parallel_threads(void) {
    if (g_max_threads > 0) return g_max_threads;
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? (int)n : 4;
}

/* Per-thread flag: 1 while the calling thread is executing as a worker
   for EITHER pool (CPU or IO), 0 otherwise. Shared across both pools so
   a cross-pool nested call (an IO-pool worker calling parallel_for(), or
   a CPU-pool worker calling parallel_for_io()) is recognised as nested
   too, not just a call back into the caller's own pool — a thread-local
   scoped to just one pool previously let cross-pool nesting fall through
   to the top-level enqueue-and-block path, which starves under enough
   concurrent nested callers (see docs/plans/2026-08-10-parallel-pool-
   cross-pool-nesting-starvation.md). parallel_for/parallel_for_io use
   this flag to decide between two execution strategies:
     - Top-level callers (any thread not currently a worker of either
       pool) → enqueue and block on the pool's own dedicated workers,
       for full pool parallelism on the common single-level case.
     - Nested callers (already executing as a worker of either pool) →
       run the n sub-tasks serially, inline, on the calling thread. */
static __thread int t_in_pool_worker = 0;
/* Read lock-free at parallel_pool_init/shutdown entry, parallel_pool_size,
   and parallel_for; written under g_q_lock at parallel_pool_shutdown and
   lock-free at parallel_pool_init. _Atomic gives correct cross-thread
   visibility regardless of which writer-lock was held. (Was volatile —
   volatile is for memory-mapped IO, not thread synchronization.) */
static _Atomic int g_pool_running = 0;

/* Run a task, then atomically decrement its group's remaining counter.
   Broadcast the group's cv when remaining hits zero so the parallel_for
   waiter (and any help-draining sibling) wakes promptly. The mutex
   bracket around the broadcast prevents the lost-wakeup race against
   waiters that just read remaining > 0 and are about to cond_wait. */
static void run_task_finish(PoolTask t) {
    t.fn(t.arg);
    /* Mark ourselves as "in the post-fn body" before publishing the
       decrement so the caller's finishing-wait covers the entire window
       through the cond_broadcast unlock. Relaxed is fine here: the
       acq_rel fetch_sub below is the synchronisation point against the
       caller's remaining-read. */
    atomic_fetch_add_explicit(&t.group->finishing, 1, memory_order_relaxed);
    int prev = atomic_fetch_sub_explicit(&t.group->remaining, 1,
                                         memory_order_acq_rel);
    if (prev == 1) {
        pthread_mutex_lock(&t.group->mu);
        pthread_cond_broadcast(&t.group->cv);
        pthread_mutex_unlock(&t.group->mu);
    }
    /* Release-ordered so the caller's acquire-load of finishing==0
       happens-after this decrement (and the unlock above). */
    atomic_fetch_sub_explicit(&t.group->finishing, 1, memory_order_release);
}

static void *pool_worker(void *arg) {
    (void)arg;
    g_db = g_shard_db_instance;
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

        t_in_pool_worker = 1;
        run_task_finish(t);
        t_in_pool_worker = 0;
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
        db_thread_create(&g_pool_threads[i], pool_worker, NULL);
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
    if (!g_pool_running || n == 1 || t_in_pool_worker) {
        /* Pool unavailable (e.g. CLI mode), single task, or the calling
           thread is already a worker of either pool (same-pool or
           cross-pool nesting). Run inline rather than enqueue: enqueuing
           would let this call re-enter a shared data structure (e.g. a
           flush buffer) whose "done" signal only the current thread can
           issue — the same self-deadlock parallel_for_io's inline path
           guards against — and, for cross-pool nesting specifically,
           prevents the starvation this flag exists to close (see
           docs/plans/2026-08-10-parallel-pool-cross-pool-nesting-
           starvation.md). */
        for (int i = 0; i < n; i++) fn((char *)args + (size_t)i * stride);
        return;
    }

    PoolGroup group;
    pthread_mutex_init(&group.mu, NULL);
    pthread_cond_init(&group.cv, NULL);
    atomic_init(&group.remaining, n);
    atomic_init(&group.finishing, 0);

    /* Enqueue in small chunks, releasing the queue lock between chunks so
       concurrent callers' tasks interleave in the FIFO queue (rather than
       the pool draining caller-by-caller). Default = core count: chunk=1
       causes submitters and workers to fight over g_q_lock constantly;
       very large chunks re-serialize callers. Core-count is a decent
       middle ground (one chunk ≈ one worker-cycle). Configurable via
       POOL_CHUNK in db.env. */
    /* Init-once chunk size. _Atomic + relaxed because the value is
       deterministic from g_pool_chunk / nproc; concurrent first-callers
       compute the same value and the write race is idempotent — but
       plain int still trips TSan, so use atomic_load/store relaxed. */
    static _Atomic int SUBMIT_CHUNK = 0;
    int submit_chunk = atomic_load_explicit(&SUBMIT_CHUNK, memory_order_relaxed);
    if (submit_chunk == 0) {
        if (g_pool_chunk > 0) {
            submit_chunk = g_pool_chunk;
        } else {
            long nproc = sysconf(_SC_NPROCESSORS_ONLN);
            submit_chunk = (nproc > 0) ? (int)nproc : 16;
        }
        atomic_store_explicit(&SUBMIT_CHUNK, submit_chunk, memory_order_relaxed);
    }
    for (int i = 0; i < n; i += submit_chunk) {
        int end = i + submit_chunk;
        if (end > n) end = n;
        pthread_mutex_lock(&g_q_lock);
        for (int j = i; j < end; j++) {
            PoolTask t = { fn, (char *)args + (size_t)j * stride, &group };
            enqueue_locked(t);
        }
        pthread_mutex_unlock(&g_q_lock);
    }

    /* The entry guard above already returned via the inline path whenever
       the calling thread is a worker of either pool, so control only
       reaches here for a genuine top-level caller (never a nested one).
       Plain cond_wait lets pool workers run the tasks in parallel without
       the caller competing for them. */
    pthread_mutex_lock(&group.mu);
    while (atomic_load_explicit(&group.remaining, memory_order_acquire) > 0)
        pthread_cond_wait(&group.cv, &group.mu);
    pthread_mutex_unlock(&group.mu);

    /* All tasks complete (remaining==0) but the last worker may still
       be between fetch_sub and the broadcast unlock — wait for finishing
       to drain before destroying mu/cv. Spinning is fine: the window is
       a few instructions and bounded; sched_yield avoids burning a core
       under load. */
    while (atomic_load_explicit(&group.finishing, memory_order_acquire) > 0)
        sched_yield();

    pthread_mutex_destroy(&group.mu);
    pthread_cond_destroy(&group.cv);
}

/* IO pool worker — runs tasks from the IO queue. Same pattern as
   pool_worker; sets the shared t_in_pool_worker flag so parallel_for and
   parallel_for_io both recognise this thread as a pool worker for
   nesting purposes, regardless of which pool it belongs to. */
static void *io_pool_worker(void *arg) {
    (void)arg;
    g_db = g_shard_db_instance;
    while (1) {
        pthread_mutex_lock(&g_io_q_lock);
        while (g_io_q_count == 0 && g_io_running)
            pthread_cond_wait(&g_io_q_not_empty, &g_io_q_lock);
        if (g_io_q_count == 0) { pthread_mutex_unlock(&g_io_q_lock); return NULL; }
        PoolTask t = g_io_queue[g_io_q_head];
        g_io_q_head = (g_io_q_head + 1) % IO_QUEUE_CAP;
        g_io_q_count--;
        pthread_cond_signal(&g_io_q_not_full);
        pthread_mutex_unlock(&g_io_q_lock);
        t_in_pool_worker = 1;
        run_task_finish(t);
        t_in_pool_worker = 0;
    }
}

void parallel_io_pool_init(int nthreads) {
    if (g_io_running) return;
    if (nthreads <= 0) nthreads = parallel_threads() * 4;
    if (nthreads < 2) nthreads = 2;
    g_io_nthreads = nthreads;
    g_io_pool_threads = malloc((size_t)nthreads * sizeof(pthread_t));
    g_io_running = 1;
    for (int i = 0; i < nthreads; i++)
        db_thread_create(&g_io_pool_threads[i], io_pool_worker, NULL);
}

void parallel_io_pool_shutdown(void) {
    if (!g_io_running) return;
    pthread_mutex_lock(&g_io_q_lock);
    g_io_running = 0;
    pthread_cond_broadcast(&g_io_q_not_empty);
    pthread_mutex_unlock(&g_io_q_lock);
    for (int i = 0; i < g_io_nthreads; i++)
        pthread_join(g_io_pool_threads[i], NULL);
    free(g_io_pool_threads);
    g_io_pool_threads = NULL;
    g_io_nthreads = 0;
}

int parallel_io_pool_size(void) { return g_io_nthreads; }

void parallel_for_io(void *(*fn)(void *), void *args, int n, size_t stride) {
    if (n <= 0) return;
    if (!g_io_running || n == 1 || t_in_pool_worker) {
        /* Run inline when the calling thread is already a worker of
           either pool (same-pool or cross-pool nesting). Enqueuing
           instead would let this call re-enter a shared data structure
           (e.g. a BatchFetchBuf) whose flusher is this very thread,
           causing pthread_cond_wait to deadlock on a broadcast only the
           flusher thread can issue — and, for cross-pool nesting
           specifically, is exactly the starvation this flag exists to
           close (see docs/plans/2026-08-10-parallel-pool-cross-pool-
           nesting-starvation.md). */
        for (int i = 0; i < n; i++) fn((char *)args + (size_t)i * stride);
        return;
    }

    PoolGroup group;
    pthread_mutex_init(&group.mu, NULL);
    pthread_cond_init(&group.cv, NULL);
    atomic_init(&group.remaining, n);
    atomic_init(&group.finishing, 0);

    /* Enqueue in chunks to avoid holding g_io_q_lock for the full batch.
       Same rationale as parallel_for's chunked submission. */
    static _Atomic int IO_SUBMIT_CHUNK = 0;
    int chunk = atomic_load_explicit(&IO_SUBMIT_CHUNK, memory_order_relaxed);
    if (chunk == 0) {
        long nproc = sysconf(_SC_NPROCESSORS_ONLN);
        chunk = (nproc > 0) ? (int)nproc : 16;
        atomic_store_explicit(&IO_SUBMIT_CHUNK, chunk, memory_order_relaxed);
    }
    for (int i = 0; i < n; i += chunk) {
        int end = i + chunk;
        if (end > n) end = n;
        pthread_mutex_lock(&g_io_q_lock);
        for (int j = i; j < end; j++) {
            PoolTask t = { fn, (char *)args + (size_t)j * stride, &group };
            while (g_io_q_count >= IO_QUEUE_CAP)
                pthread_cond_wait(&g_io_q_not_full, &g_io_q_lock);
            g_io_queue[g_io_q_tail] = t;
            g_io_q_tail = (g_io_q_tail + 1) % IO_QUEUE_CAP;
            g_io_q_count++;
            pthread_cond_signal(&g_io_q_not_empty);
        }
        pthread_mutex_unlock(&g_io_q_lock);
    }

    /* The entry guard above already returned via the inline path whenever
       the calling thread is a worker of either pool, so control only
       reaches here for a genuine top-level caller. Plain cond_wait — the
       IO pool has its own workers to drain the queue. */
    pthread_mutex_lock(&group.mu);
    while (atomic_load_explicit(&group.remaining, memory_order_acquire) > 0)
        pthread_cond_wait(&group.cv, &group.mu);
    pthread_mutex_unlock(&group.mu);

    while (atomic_load_explicit(&group.finishing, memory_order_acquire) > 0)
        sched_yield();

    pthread_mutex_destroy(&group.mu);
    pthread_cond_destroy(&group.cv);
}
