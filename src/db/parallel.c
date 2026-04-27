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
 * Deadlock note: tasks submitted via parallel_for() must NOT themselves call
 * parallel_for() (could deadlock if the pool is already saturated by the
 * parent call's sibling tasks). Current call sites are all leaf workers —
 * safe. Worth a comment on any new task function added.
 */

#include "types.h"
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int remaining;
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
static volatile int g_pool_running = 0;

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

        t.fn(t.arg);

        pthread_mutex_lock(&t.group->mu);
        if (--t.group->remaining == 0)
            pthread_cond_broadcast(&t.group->cv);
        pthread_mutex_unlock(&t.group->mu);
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
    group.remaining = n;

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

    pthread_mutex_lock(&group.mu);
    while (group.remaining > 0)
        pthread_cond_wait(&group.cv, &group.mu);
    pthread_mutex_unlock(&group.mu);

    pthread_mutex_destroy(&group.mu);
    pthread_cond_destroy(&group.cv);
}
