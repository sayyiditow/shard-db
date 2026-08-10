# Fix: cross-pool nesting starvation in `parallel.c` (production hang)

## Background

A production deployment embedding shard-db as a library hung permanently
under concurrent indexed bulk-writes across multiple objects. Confirmed
via two live process inspections (`gdb -p <pid> -batch -ex "thread apply
all bt"`) taken roughly 25 hours apart on the same process: both captures
show an identical thread-state pattern — zero forward progress between
them, not merely slow progress. Deployment-identifying details (host,
process, embedding runtime, package versions) are intentionally omitted
below; the bug and its evidence are entirely within shard-db's own
`src/db/parallel.c` and reproduce independent of any of that.

Thread-state signature at the time of the hang (of ~57 total threads in
the process):
- **32 threads** stuck in `parallel_for()` on a plain, untimed
  `pthread_cond_wait`, reached from shard-db's indexed bulk-write commit
  path.
- **5 threads** stuck on `pthread_rwlock_wrlock` inside `btree_insert()`
  (via `btree_idx_insert()` ← `update_idx_fn()`).
- **1 thread** stuck on `pthread_mutex_lock`, same `btree_insert()` call
  chain.
- **2 threads** in `parallel_for_io()` inside `slotcask_pregrow_kf()`.
- Remaining threads: shard-db's own query-dispatch threads blocked on
  read locks (`kfcache_acquire_direct`/`btree_range_iter_open`) — i.e.
  ordinary GETs/finds stalled behind the writers above — plus assorted
  idle/background threads belonging to the embedding process, none of
  which were found to be holding anything shard-db was blocked on.

An earlier working theory (surfaced from a prior, separate investigation
pass) attributed the hang to lock contention inside the embedding
runtime's own garbage collector. That was checked directly against live
per-thread CPU-time data on the running process and did not hold up: the
runtime's GC helper threads were each using a modest, unremarkable share
of CPU with only a few hours of accumulated time over a ~25-hour process
lifetime, and the embedding runtime's own call-dispatch threads were
essentially idle for the entire run. That rules out the runtime's GC as
the primitive blocking shard-db's threads; the elevated GC activity
observed is better explained as a downstream symptom (client-side work
backing up behind the stalled native calls below), not the cause.

## Root cause

`src/db/parallel.c` implements two independent worker pools — a CPU pool
(`parallel_for`, sized `THREADS`/nproc) and an I/O pool (`parallel_for_io`,
sized `nproc×4`) — each with its own thread-local nesting-detection flag:

```c
static __thread int t_in_pool_task = 0;   /* set only by CPU-pool workers */
static __thread int t_in_io_task   = 0;   /* set only by IO-pool workers */
```

Both `parallel_for()` and `parallel_for_io()` open with:

```c
if (!g_pool_running || n == 1 || t_in_pool_task) {   /* parallel_for */
    for (int i = 0; i < n; i++) fn((char *)args + (size_t)i * stride);
    return;
}
```
```c
if (!g_io_running || n == 1 || t_in_io_task) {        /* parallel_for_io */
    for (int i = 0; i < n; i++) fn((char *)args + (size_t)i * stride);
    return;
}
```

When the nesting flag is set, the call runs its `n` sub-tasks **serially,
inline, on the calling thread** — it never touches the shared queue. This
is the *only* live nesting-safety mechanism in the file today. Both
functions also contain a second, later `if (t_in_pool_task)` /
`if (t_in_io_task)` block guarding a "help-drain" loop (pop from the
shared queue and run tasks while waiting for the group to finish) — but
that block is **dead code**: by the time control reaches it, the function
has already returned via the inline-serial path above whenever that same
flag is set. The file's doc comment (top of file) and the surrounding
inline comments describe the help-drain loop as the active nesting-safety
mechanism; it is not — it is unreachable. Same-pool nesting is protected
by the inline-serial path, not by draining.

**The bug:** neither guard checks the *other* pool's flag. shard-db's
indexed bulk-write commit path is intentionally split across both pools
by I/O-vs-CPU profile — the per-shard write step (mmap page faults on
segment/kf files) runs on the IO pool via `parallel_for_io()`; its
`apply_window` callback then merges newly written records into each
indexed field's B+ tree (CPU-bound: btree merge) on the CPU pool via
`parallel_for()` (`src/db/query_bulk.c`, in `v2_bulk_ins_apply_window` /
`idx_build_field_worker`, invoked from `bulk_insert_shard_worker_v2`).
A thread executing that inner call is an IO-pool worker:
`t_in_io_task=1`, `t_in_pool_task=0`. `parallel_for()`'s guard only
checks `t_in_pool_task` (false) — so it does **not** take the
inline-serial path. It falls through to the ordinary top-level path:
enqueue `n` tasks onto the CPU pool's shared queue, then block on a
plain `pthread_cond_wait` — indistinguishable from what a genuinely
unrelated top-level caller does.

Under enough concurrent indexed bulk-writes with enough combined
index/shard fan-out, every IO-pool worker (sized `nproc×4`) can reach
this call simultaneously. Each takes the same "fake top-level" path:
enqueue + block, contributing zero draining capacity to the CPU pool's
queue. That leaves only the CPU pool's fixed, much smaller `nproc`-sized
worker set to drain a queue now backed up by every IO-pool worker at
once — and in the observed snapshot, most of *those* workers are
themselves independently contending for the same per-shard btree index
files across the concurrent writes (`pthread_rwlock_wrlock`/
`pthread_mutex_lock` inside `btree_insert()`). That contention is
expected, ordinary behavior under concurrent indexed writes — the bug is
that the much larger IO-pool worker count that should be able to absorb
load during it is structurally locked out of helping, collapsing what
should be recoverable contention into a stall with no bound on how long
it can persist. Read paths then stall too, waiting on writers that can't
finish.

The same gap is symmetric: a CPU-pool worker calling `parallel_for_io()`
is equally unrecognized as nested (checks `t_in_io_task`, which such a
thread never sets). No call site currently reaching that direction has
been confirmed live, but the fix below closes both directions uniformly
since it isn't call-site-specific.

**Falsifiable prediction:** if this is the cause, then a minimal,
synthetic reproduction — a thread executing as an IO-pool worker calling
`parallel_for()` from outside the database entirely — will show the same
"enqueue + block" pattern and measurably worse throughput than genuine
parallel execution at identical scale, and unifying the nesting flag
across both pools will eliminate the gap. Task 1 builds exactly this
reproduction as the regression test.

## Design decisions (surfacing for review, not deciding silently)

**1. Unify the two flags.** Two ways to close the detection gap were
considered: (a) OR the two flags into each guard
(`t_in_pool_task || t_in_io_task`), keeping both thread-locals; or (b)
replace both thread-locals with a single `t_in_pool_worker`, set by both
`pool_worker` and `io_pool_worker`, checked by both guards. **(b) is
chosen** — same runtime fix, but removes the "flag belongs to the wrong
pool" bug class entirely rather than leaving two flags that must always
be kept in lockstep by every future guard added to this file.

**2. Remove the dead help-drain code**, rather than just renaming the
flag it references. Once the entry guard checks the unified flag, the
later `if (t_in_pool_worker)` branch in each function is unreachable by
construction (same as it already was, pre-fix, for same-pool nesting) —
keeping it around as inert dead code is a real maintainability hazard in
a concurrency-critical file (it is what led this investigation to
initially mischaracterize the bug as "help-drain doesn't recognize
cross-pool nesting" before closer reading found the branch can never
execute at all). Removing it also lets `try_pop_task`/`try_pop_io_task`
be deleted, since after removal they have no remaining callers. Task 2
below does this deletion as part of the same diff.

## Invariants / edge cases

- Same-pool nesting behavior (CPU task → `parallel_for`, IO task →
  `parallel_for_io`) must be byte-for-byte unchanged: the unified flag is
  set/cleared by exactly one pool-worker function per thread (a given OS
  thread is either a CPU-pool worker or an IO-pool worker, never both),
  so the guard condition each sees is equivalent to before.
- `n == 1` and `!g_pool_running`/`!g_io_running` short-circuits are
  untouched.
- Thread-locals are per-OS-thread; `pool_worker` and `io_pool_worker`
  threads are distinct OS threads, so both writing the same `__thread`
  variable name introduces no cross-thread aliasing.
- No signature change to `parallel_for`/`parallel_for_io`/
  `parallel_pool_init`/`parallel_io_pool_init`/`parallel_pool_shutdown`/
  `parallel_io_pool_shutdown`. Every call site across `index.c`,
  `query.c`, `query_aggregate.c`, `query_bulk.c`, `query_schema.c`,
  `storage.c` is unaffected by this diff's surface (only the internal
  nesting *detection*, not the calling convention, changes). No
  call-site audit is required for correctness; the fix is
  call-site-agnostic by construction.
- After deleting `try_pop_task`/`try_pop_io_task`, grep the repo for both
  names to confirm no other reference exists before removing them (Task
  2 includes this check explicitly).

## Task 1 — Regression test (write first, confirm it fails)

**Test-first step:** this is a new, minimal, synthetic reproduction —
not the real bulk-insert path — because the real path needs a large
dataset and hours to manifest as an observable hang; the defect is
purely structural in `parallel.c` and reproduces at unit-test scale and
speed once a thread from one pool is forced to nest into the other under
a worker-count squeeze.

Create `src/test/cases/test_parallel_cross_pool_nesting.c`:

```c
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <stdlib.h>
#include <string.h>
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

static long now_ms(void) {
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

    long start = now_ms();
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

    long elapsed = now_ms() - start;

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
```

Confirm the new file is picked up by the test build the same way
`src/test/cases/test_parallel.c` is (check `build.sh` / the test target's
source globbing) before assuming — if explicit registration is required
somewhere else, add it.

**Run it against the current, unfixed code and confirm it fails:**

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-parallel-cross-pool-nesting
```

Expected (pre-fix): the `elapsed < 500` assertion fails (observed elapsed
should land near 800–900ms). Paste the actual output. If instead the
`completed within 2s` assertion fails (i.e., the synthetic repro produces
a true hang rather than a bounded slowdown at this scale), that is still
a valid red result — paste it and proceed to Task 2 regardless; do not
adjust the test to force a particular assertion to be the one that fails.

If the test unexpectedly passes outright, stop — the reproduction doesn't
exercise the bug as hypothesized. Do not proceed to Task 2 until it's red
for the expected reason (throughput collapse from the enqueue+block path).

## Task 2 — Fix

All edits in `src/db/parallel.c`.

**Edit 2a** — file-level doc comment. Locate this exact block (top of
file):

```c
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
```

Replace with:

```c
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
```

**Edit 2b** — thread-local declarations, and removal of the now-to-be-dead
helper functions' *doc comments* referencing them (functions themselves
removed in Edit 2c below). Locate this exact block:

```c
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
/* Same flag for IO pool workers — used by parallel_for_io's help-drain
   to detect nested IO calls and drain the IO queue while waiting. */
static __thread int t_in_io_task = 0;
```

Replace with:

```c
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
```

**Edit 2c** — delete `try_pop_task` and `try_pop_io_task`. Both become
fully unused once Edits 2f/2g (below) remove their only call sites.
Before deleting, run:

```bash
grep -rn 'try_pop_task\|try_pop_io_task' src/
```

and confirm the only matches are the definitions themselves and the two
call sites being deleted in Edits 2f/2i. Locate this exact block:

```c
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

/* Pop one IO task from the head of the IO queue if non-empty. Same pattern
   as try_pop_task but operates on g_io_queue[] / g_io_q_lock. Used by
   io_pool_worker and the help-drain loop in parallel_for_io. */
static int try_pop_io_task(PoolTask *out) {
    pthread_mutex_lock(&g_io_q_lock);
    if (g_io_q_count == 0) {
        pthread_mutex_unlock(&g_io_q_lock);
        return 0;
    }
    *out = g_io_queue[g_io_q_head];
    g_io_q_head = (g_io_q_head + 1) % IO_QUEUE_CAP;
    g_io_q_count--;
    pthread_cond_signal(&g_io_q_not_full);
    pthread_mutex_unlock(&g_io_q_lock);
    return 1;
}

/* Run a task, then atomically decrement its group's remaining counter.
```

Replace with (deletes both functions, keeps the following comment that
starts `run_task_finish`):

```c
/* Run a task, then atomically decrement its group's remaining counter.
```

**Edit 2d** — `pool_worker`. Locate:

```c
        t_in_pool_task = 1;
        run_task_finish(t);
        t_in_pool_task = 0;
    }
}

void parallel_pool_init(int nthreads) {
```

Replace the two assignment lines with:

```c
        t_in_pool_worker = 1;
        run_task_finish(t);
        t_in_pool_worker = 0;
    }
}

void parallel_pool_init(int nthreads) {
```

**Edit 2e** — `parallel_for` entry guard. Locate:

```c
void parallel_for(void *(*fn)(void *), void *args, int n, size_t stride) {
    if (n <= 0) return;
    if (!g_pool_running || n == 1 || t_in_pool_task) {
        /* Pool unavailable (e.g. CLI mode), single task, or already inside a
           pool task. The last case prevents help-drain from picking up tasks
           that re-enter a shared data structure (e.g. a flush buffer) whose
           "done" signal only the current thread can issue — the same
           self-deadlock that t_in_io_task guards against in parallel_for_io. */
        for (int i = 0; i < n; i++) fn((char *)args + (size_t)i * stride);
        return;
    }
```

Replace with:

```c
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
```

**Edit 2f** — delete `parallel_for`'s dead help-drain branch. Locate this
exact block:

```c
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
            /* coverity[wait_not_in_locked_loop] outer while-loop at line
               above already re-checks `remaining` after wake, which is
               what Coverity's rule asks for. Looping cond_timedwait
               directly here would be wrong — we want a wake to fall back
               out so try_pop_task gets re-tried (the queue may have
               filled meanwhile), not to keep sleeping. */
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
```

Replace with:

```c
    /* The entry guard above already returned via the inline path whenever
       the calling thread is a worker of either pool, so control only
       reaches here for a genuine top-level caller (never a nested one).
       Plain cond_wait lets pool workers run the tasks in parallel without
       the caller competing for them. */
    pthread_mutex_lock(&group.mu);
    while (atomic_load_explicit(&group.remaining, memory_order_acquire) > 0)
        pthread_cond_wait(&group.cv, &group.mu);
    pthread_mutex_unlock(&group.mu);
```

**Edit 2g** — `io_pool_worker`'s header comment and body. Locate:

```c
/* IO pool worker — runs tasks from the IO queue. Same pattern as
   pool_worker but sets t_in_io_task instead of t_in_pool_task so
   parallel_for_io can detect nested calls and help-drain. */
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
        t_in_io_task = 1;
        run_task_finish(t);
        t_in_io_task = 0;
    }
}
```

Replace with:

```c
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
```

**Edit 2h** — `parallel_for_io` entry guard. Locate:

```c
void parallel_for_io(void *(*fn)(void *), void *args, int n, size_t stride) {
    if (n <= 0) return;
    if (!g_io_running || n == 1 || t_in_io_task) {
        /* Run inline when already inside an IO task. The help-drain loop
           would otherwise pop arbitrary IO tasks (e.g. btree shard walkers)
           that can re-enter a BatchFetchBuf whose flusher is this very
           thread, causing pthread_cond_wait to deadlock on a broadcast that
           only the flusher thread can issue. Inline execution avoids the
           re-entry entirely at the cost of serialising the nested fetch. */
        for (int i = 0; i < n; i++) fn((char *)args + (size_t)i * stride);
        return;
    }
```

Replace with:

```c
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
```

**Edit 2i** — delete `parallel_for_io`'s dead help-drain branch. Locate
this exact block:

```c
    if (t_in_io_task) {
        /* Nested IO call — help-drain the IO queue while waiting so the
           pool can never starve when (concurrent IO callers × inner tasks)
           >= IO pool size. Same pattern as parallel_for's help-drain. */
        while (atomic_load_explicit(&group.remaining, memory_order_acquire) > 0) {
            PoolTask t;
            if (try_pop_io_task(&t)) {
                run_task_finish(t);
                continue;
            }
            pthread_mutex_lock(&group.mu);
            if (atomic_load_explicit(&group.remaining, memory_order_acquire) > 0) {
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_nsec += 1000000;
                if (ts.tv_nsec >= 1000000000) { ts.tv_sec++; ts.tv_nsec -= 1000000000; }
                pthread_cond_timedwait(&group.cv, &group.mu, &ts);
            }
            pthread_mutex_unlock(&group.mu);
        }
    } else {
        /* Top-level or CPU-pool caller — plain cond_wait. IO pool has
           its own workers to drain the queue. */
        pthread_mutex_lock(&group.mu);
        while (atomic_load_explicit(&group.remaining, memory_order_acquire) > 0)
            pthread_cond_wait(&group.cv, &group.mu);
        pthread_mutex_unlock(&group.mu);
    }
```

Replace with:

```c
    /* The entry guard above already returned via the inline path whenever
       the calling thread is a worker of either pool, so control only
       reaches here for a genuine top-level caller. Plain cond_wait — the
       IO pool has its own workers to drain the queue. */
    pthread_mutex_lock(&group.mu);
    while (atomic_load_explicit(&group.remaining, memory_order_acquire) > 0)
        pthread_cond_wait(&group.cv, &group.mu);
    pthread_mutex_unlock(&group.mu);
```

After all of Task 2's edits, `grep -n 't_in_pool_task\|t_in_io_task\|try_pop_task\|try_pop_io_task' src/db/parallel.c` must return nothing.

## Task 3 — Confirm the regression test now passes

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-parallel-cross-pool-nesting
```

Paste the output. Expected: both assertions pass, elapsed comfortably
under 500ms (design target ~200ms).

## Task 4 — Full suite, fresh

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Paste the full pass/fail summary. Any pre-existing failure unrelated to
this change should be called out explicitly, not silently ignored — if
one appears, stop and ask before proceeding, don't paper over it.

## Task 5 — ASan + UBSan (required: this diff touches shared thread-pool state and locks)

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2
```

Paste the output. Any new finding must be root-caused and fixed now (if
simple) or written up as its own `docs/plans/<date>-<slug>.md` and, only
if deliberately deferred, added to `.tsan.supp`/equivalent with a named
suppression and full rationale — never silently ignored.

## Task 6 — TSan (required: this diff is a threading/synchronization fix)

```bash
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1
```

Paste the output. Same handling rule as Task 5 for any new finding. This
run is especially important here: it's the most direct way to confirm
the flag-unification and dead-code removal introduced no new race
relative to the pre-fix code, and that the removed branches really were
provably unreachable (a TSan-visible access pattern change would be a
strong signal the reachability analysis above was wrong).

## Documentation

No user-facing docs (`docs/`, `AGENTS.md`) describe `parallel.c`'s
internal nesting mechanism — it's purely an internal implementation
detail, not part of the wire/CLI protocol. The stale in-file comments
describing the mechanism are corrected as part of Task 2 itself, so no
separate documentation-sync task is needed.

## Out of scope for this diff

Nothing currently deferred — Task 2 includes the dead-code removal.

## Execution rules

- Branch off `main`: `fix/parallel-pool-cross-pool-nesting-starvation`.
- Do tasks in order; each task's command output gets pasted in full, not
  summarized, before moving to the next task.
- Per this repo's standing execution mode: leave all changes **uncommitted**
  on the branch when done — do not commit, push, or open a PR. The human
  and a reviewing agent review the raw `git diff` first.
- If a quoted anchor in Task 2 isn't found exactly as written, stop
  immediately: write `PLAN_NOTES.md` at the repo root describing the
  mismatch (which anchor, what the file actually contains there) and halt
  the entire run — do not guess, reinterpret, or continue to any other
  task, even an unrelated one. Resuming requires a human (or the planning
  model) to read `PLAN_NOTES.md` and hand back a patched or fresh plan.
- If you hit any decision this plan doesn't cover, stop and ask — do not
  improvise.
