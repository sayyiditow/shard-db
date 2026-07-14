/* src/test/test_runner.c */
#include "test_runner.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#ifdef TEST_BUILD
extern void test_reset_caches(void);
extern void test_init_process_db(void);  /* ensures g_db is set for in-process tests */
#endif

__thread TestCtx *t_ctx = NULL;

static TestCaseEntry *g_head = NULL;

void test_register(TestCaseEntry *entry) {
    /* Insert at end so registration order matches source order. */
    if (!g_head) { g_head = entry; return; }
    TestCaseEntry *p = g_head;
    while (p->next) p = p->next;
    p->next = entry;
}

int test_count(void) {
    int n = 0;
    for (TestCaseEntry *p = g_head; p; p = p->next) n++;
    return n;
}

const TestCaseEntry *test_first(void) { return g_head; }

static int run_case(const TestCaseEntry *tc) {
    TestCtx ctx = { .name = tc->name };
    t_ctx = &ctx;
    printf("# %s\n", tc->name);
#ifdef TEST_BUILD
    test_init_process_db();
#endif
    int rc = tc->fn();
    if (rc != 0 && ctx.failed == 0) ctx.failed = 1; /* fn signalled fail without assert */
    printf("# %s: %d passed, %d failed\n", tc->name, ctx.passed, ctx.failed);
    t_ctx = NULL;
    return ctx.failed;
}

int test_run_one(const char *name) {
    for (TestCaseEntry *p = g_head; p; p = p->next) {
        if (strcmp(p->name, name) == 0) return run_case(p);
    }
    fprintf(stderr, "no test named '%s'\n", name);
    return 1;
}

/* Some pure in-process unit tests (no TestEnv/TCP daemon of their own)
   are provably race-free by their own C11/POSIX happens-before design,
   but empirically flake when a *sibling* worker thread calls fork() to
   spawn a TestEnv daemon at the same moment: glibc's pthread_atfork
   handling briefly stalls unrelated threads in the same process while
   acquiring malloc arena locks around the syscall. Verified: 0/20
   failures running test-objlock-unit alone; nonzero whenever it runs
   concurrently with any TestEnv-based case, at every --jobs > 1 tried
   (2, 4). Never a --jobs 1 failure. Not a bug in the test or in
   objlock.c — this is OS-level scheduling perturbation from a sibling
   thread's fork(), which no amount of atomic-barrier synchronization in
   the test itself can compensate for. Fix: keep isolation-sensitive
   cases out of the pool entirely — run them single-threaded before any
   worker thread has a chance to fork. */
static int test_needs_isolation(const char *name) {
    return strcmp(name, "test-objlock-unit") == 0;
}

/* Builds a flat array of every registered case matching `filter` (NULL
   = all). Returns the count; *out receives a malloc'd array the caller
   must free. Both the sequential and parallel paths share this so
   filtering behavior is identical between them. */
static int collect_matching(const char *filter, TestCaseEntry ***out) {
    int cap = 16, n = 0;
    TestCaseEntry **arr = malloc((size_t)cap * sizeof(*arr));
    for (TestCaseEntry *p = g_head; p; p = p->next) {
        if (filter && !strstr(p->name, filter)) continue;
        if (n == cap) {
            cap *= 2;
            void *np = realloc(arr, (size_t)cap * sizeof(*arr));
            if (!np) { free(arr); *out = NULL; return 0; }
            arr = np;
        }
        arr[n++] = p;
    }
    *out = arr;
    return n;
}

/* Strictly sequential — byte-identical output to the pre-parallel
   implementation this replaces. This is the --jobs 1 fallback. */
static int run_all_sequential(TestCaseEntry **cases, int n) {
    int total_fail = 0, total_passed = 0;
    for (int i = 0; i < n; i++) {
        TestCaseEntry *p = cases[i];
        TestCtx ctx = { .name = p->name };
        t_ctx = &ctx;
        printf("# %s\n", p->name);
#ifdef TEST_BUILD
        test_init_process_db();
#endif
        int rc = p->fn();
        if (rc != 0 && ctx.failed == 0) ctx.failed = 1;
        total_fail += ctx.failed;
        total_passed += ctx.passed;
        printf("# %s: %d passed, %d failed\n", p->name, ctx.passed, ctx.failed);
        t_ctx = NULL;
#ifdef TEST_BUILD
        test_reset_caches();
#endif
    }
    printf("1..%d\n", n);
    printf("# total: %d passed, %d failed across %d cases\n",
           total_passed, total_fail, n);
    return total_fail;
}

/* ---- Parallel path (jobs > 1) ---- */

typedef struct {
    _Atomic(const char *) name;   /* current test name; NULL = worker idle */
    _Atomic long start_ms;
} WorkerSlot;

typedef struct {
    TestCaseEntry **cases;
    int n;
    _Atomic int next;             /* self-draining index, mirrors parallel.c's
                                     atomic fetch-add dispatch pattern */
    _Atomic int total_passed;
    _Atomic int total_failed;
    pthread_mutex_t print_lock;   /* serializes each completed test's buffered
                                     output flush to real stdout */
    WorkerSlot *slots;
    int nslots;
    _Atomic int done;             /* set 1 once all workers have joined, tells
                                     the watchdog to stop polling */
} ParallelCtx;

typedef struct {
    ParallelCtx *pc;
    int slot;
} WorkerArg;

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* Hard-aborts the whole process if any single test case runs longer
   than the watchdog limit. A stuck pthread (e.g. blocked in a syscall
   against a wedged daemon) cannot be safely cancelled mid-flight in C,
   so this mirrors the `timeout(1)` convention: kill the process, exit
   124, let CI/the human re-run and investigate which case hung. */
static void *watchdog_main(void *arg) {
    ParallelCtx *pc = arg;
    long limit_ms = 180000;
    const char *env = getenv("SHARD_TEST_WATCHDOG_SEC");
    if (env && atoi(env) > 0) limit_ms = (long)atoi(env) * 1000L;

    while (!atomic_load_explicit(&pc->done, memory_order_acquire)) {
        long now = now_ms();
        for (int i = 0; i < pc->nslots; i++) {
            const char *name = atomic_load_explicit(&pc->slots[i].name, memory_order_acquire);
            if (!name) continue;
            long start = atomic_load_explicit(&pc->slots[i].start_ms, memory_order_acquire);
            if (now - start > limit_ms) {
                fflush(stdout);
                fprintf(stderr,
                    "\n# WATCHDOG: test '%s' exceeded %lds on worker %d — aborting run-all\n",
                    name, limit_ms / 1000, i);
                fflush(stderr);
                _exit(124);
            }
        }
        struct timespec req = { 1, 0 };
        nanosleep(&req, NULL);
    }
    return NULL;
}

static void *worker_main(void *arg_) {
    WorkerArg *wa = arg_;
    ParallelCtx *pc = wa->pc;
    int slot = wa->slot;

    for (;;) {
        int idx = atomic_fetch_add_explicit(&pc->next, 1, memory_order_relaxed);
        if (idx >= pc->n) break;
        TestCaseEntry *tc = pc->cases[idx];

        char *buf = NULL;
        size_t bufsz = 0;
        FILE *mem = open_memstream(&buf, &bufsz);
        TestCtx ctx = { .name = tc->name, .out = mem };
        t_ctx = &ctx;

        long now = now_ms();
        atomic_store_explicit(&pc->slots[slot].start_ms, now, memory_order_release);
        atomic_store_explicit(&pc->slots[slot].name, tc->name, memory_order_release);

        fprintf(mem, "# %s\n", tc->name);
#ifdef TEST_BUILD
        test_init_process_db();
#endif
        int rc = tc->fn();
        if (rc != 0 && ctx.failed == 0) ctx.failed = 1;
        fprintf(mem, "# %s: %d passed, %d failed\n", tc->name, ctx.passed, ctx.failed);
        fflush(mem);

        atomic_store_explicit(&pc->slots[slot].name, NULL, memory_order_release);
        t_ctx = NULL;
#ifdef TEST_BUILD
        test_reset_caches();
#endif
        atomic_fetch_add_explicit(&pc->total_passed, ctx.passed, memory_order_relaxed);
        atomic_fetch_add_explicit(&pc->total_failed, ctx.failed, memory_order_relaxed);

        pthread_mutex_lock(&pc->print_lock);
        fwrite(buf, 1, bufsz, stdout);
        pthread_mutex_unlock(&pc->print_lock);

        fclose(mem);
        free(buf);
    }
    return NULL;
}

static int run_all_parallel(TestCaseEntry **cases, int n, int jobs) {
    if (jobs > n) jobs = n > 0 ? n : 1;

    printf("# run-all: %d worker(s)\n", jobs);
    fflush(stdout);

    ParallelCtx pc;
    pc.cases = cases;
    pc.n = n;
    atomic_init(&pc.next, 0);
    atomic_init(&pc.total_passed, 0);
    atomic_init(&pc.total_failed, 0);
    atomic_init(&pc.done, 0);
    pthread_mutex_init(&pc.print_lock, NULL);
    pc.nslots = jobs;
    pc.slots = calloc((size_t)jobs, sizeof(*pc.slots));
    for (int i = 0; i < jobs; i++) {
        atomic_init(&pc.slots[i].name, (const char *)NULL);
        atomic_init(&pc.slots[i].start_ms, 0L);
    }

    pthread_t wd;
    pthread_create(&wd, NULL, watchdog_main, &pc);

    pthread_t *threads = malloc((size_t)jobs * sizeof(pthread_t));
    WorkerArg *wargs = malloc((size_t)jobs * sizeof(WorkerArg));
    for (int i = 0; i < jobs; i++) {
        wargs[i].pc = &pc;
        wargs[i].slot = i;
        pthread_create(&threads[i], NULL, worker_main, &wargs[i]);
    }
    for (int i = 0; i < jobs; i++) pthread_join(threads[i], NULL);

    atomic_store_explicit(&pc.done, 1, memory_order_release);
    pthread_join(wd, NULL);

    int total_passed = atomic_load_explicit(&pc.total_passed, memory_order_relaxed);
    int total_failed = atomic_load_explicit(&pc.total_failed, memory_order_relaxed);
    printf("1..%d\n", n);
    printf("# total: %d passed, %d failed across %d cases\n",
           total_passed, total_failed, n);

    free(threads);
    free(wargs);
    free(pc.slots);
    pthread_mutex_destroy(&pc.print_lock);
    return total_failed;
}

int test_run_all(const char *filter, int jobs) {
    TestCaseEntry **cases = NULL;
    int n = collect_matching(filter, &cases);

    if (jobs <= 1) {
        int result = run_all_sequential(cases, n);
        free(cases);
        return result;
    }

    /* Partition out isolation-sensitive cases (see test_needs_isolation)
       and run them sequentially before any pool thread can fork(). */
    TestCaseEntry **iso = malloc((size_t)(n > 0 ? n : 1) * sizeof(*iso));
    TestCaseEntry **rest = malloc((size_t)(n > 0 ? n : 1) * sizeof(*rest));
    int niso = 0, nrest = 0;
    for (int i = 0; i < n; i++) {
        if (test_needs_isolation(cases[i]->name)) iso[niso++] = cases[i];
        else rest[nrest++] = cases[i];
    }

    int fail = 0;
    if (niso > 0) fail += run_all_sequential(iso, niso);
    if (nrest > 0) fail += run_all_parallel(rest, nrest, jobs);

    free(iso);
    free(rest);
    free(cases);
    return fail;
}
