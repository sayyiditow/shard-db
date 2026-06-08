# Plan: Replace sched_yield() spin in batch_buf_collect_hash with pthread_cond_wait

**Goal**: Fix the arm64 CI starvation hang in `test-and-intersection` by replacing the
`sched_yield()` busy-wait in `batch_buf_collect_hash` with a `pthread_cond_wait` that
blocks the calling thread until the flush completes.

**Root cause**: `batch_buf_collect_hash` is called by multiple IO pool threads concurrently
(one per btree shard, via `btree_dispatch` → `parallel_for_io` → `bm_shard_walk_worker`
→ `stream_find_cb`). When the buffer fills, one thread grabs the `flushing` flag and
calls `slotcask_bulk_resolve_and_fetch` (sequential, ~16 kfcache acquires). The remaining
7 IO pool threads hit the `sched_yield()` loop. On a 2-core arm64 CI runner with 200+
concurrent test daemons, those 7 spinning threads hold the 2 CPU cores, starving the
single flushing thread. The flush never completes; the request times out after 60 s.
16 failures × 60 s ≈ 10-minute CI gap exactly matches the observed timeline.

**Fix**: Add a `pthread_cond_t flush_done` condition variable to `BatchFetchBuf`. Waiting
threads call `pthread_cond_wait(&b->flush_done, &b->lock)` instead of spinning. The
flusher broadcasts `flush_done` after clearing `b->flushing`. No spin, no starvation.

---

## Execution rules

- Branch off `main`: `git checkout -b fix/batch-buf-spin-condvar`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all`
- Do tasks in order; do not skip.
- Locate every insertion site by the **quoted anchor text** given. If the anchor is not
  found exactly, stop and write `PLAN_NOTES.md` — do not guess or reinterpret.
- Never claim a step passed without showing the real build/test output.

---

## Task 1 — Add `flush_done` condvar to `BatchFetchBuf`

**File**: `src/db/query.c`

Locate the anchor (the full struct definition):
```
typedef struct BatchFetchBuf_ {
    uint8_t   (*pending)[16];
    size_t     pending_n;
    size_t     pending_cap;
    pthread_mutex_t lock;
    int             flushing;
    SlotcaskDb *sdb;
    int (*record_cb)(const uint8_t hash16[16],
                     const void *key, size_t klen,
                     const void *value, size_t vlen,
                     void *ctx);
    void       *record_ctx;
    volatile int stop;
} BatchFetchBuf;
```

Replace with:
```c
typedef struct BatchFetchBuf_ {
    uint8_t   (*pending)[16];
    size_t     pending_n;
    size_t     pending_cap;
    pthread_mutex_t lock;
    pthread_cond_t  flush_done;  /* signalled when flushing transitions 1→0 */
    int             flushing;
    SlotcaskDb *sdb;
    int (*record_cb)(const uint8_t hash16[16],
                     const void *key, size_t klen,
                     const void *value, size_t vlen,
                     void *ctx);
    void       *record_ctx;
    volatile int stop;
} BatchFetchBuf;
```

---

## Task 2 — Init `flush_done` in `batch_buf_init`

**File**: `src/db/query.c`

Locate the anchor (the mutex init line inside `batch_buf_init`):
```
    pthread_mutex_init(&b->lock, NULL);
    b->flushing = 0;
```

Replace with:
```c
    pthread_mutex_init(&b->lock, NULL);
    pthread_cond_init(&b->flush_done, NULL);
    b->flushing = 0;
```

---

## Task 3 — Destroy `flush_done` in `batch_buf_destroy`

**File**: `src/db/query.c`

Locate the anchor (the destroy sequence in `batch_buf_destroy`):
```
    batch_buf_flush(b);
    pthread_mutex_destroy(&b->lock);
    free(b->pending);
```

Replace with:
```c
    batch_buf_flush(b);
    pthread_cond_destroy(&b->flush_done);
    pthread_mutex_destroy(&b->lock);
    free(b->pending);
```

---

## Task 4 — Replace `sched_yield()` spin with `pthread_cond_wait`

**File**: `src/db/query.c`

Locate the anchor (the spin block inside `batch_buf_collect_hash`):
```
        if (b->flushing) {
            pthread_mutex_unlock(&b->lock);
            sched_yield();
            continue;
        }
        b->flushing = 1;
        pthread_mutex_unlock(&b->lock);

        batch_buf_flush_copy(b);

        pthread_mutex_lock(&b->lock);
        b->flushing = 0;
        pthread_mutex_unlock(&b->lock);
```

Replace with:
```c
        while (b->flushing) {
            /* Block until the flushing thread completes rather than
               spinning. On overloaded runners (arm64 CI, 2 cores, 200+
               concurrent test daemons) the sched_yield() loop starved the
               flush thread, causing 60 s timeouts in test-and-intersection. */
            pthread_cond_wait(&b->flush_done, &b->lock);
        }
        b->flushing = 1;
        pthread_mutex_unlock(&b->lock);

        batch_buf_flush_copy(b);

        pthread_mutex_lock(&b->lock);
        b->flushing = 0;
        pthread_cond_broadcast(&b->flush_done);
        pthread_mutex_unlock(&b->lock);
```

---

## Task 5 — Build and test

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` with the same N as before.
The test `test-and-intersection` must pass (it was the failing case on arm64).

---

## Invariants and edge cases

- `pthread_cond_wait` atomically releases `b->lock` and blocks. When the broadcast wakes
  the waiter, `b->lock` is re-acquired before `pthread_cond_wait` returns. The `while`
  loop (not `if`) re-checks `b->flushing` to handle spurious wakeups.
- `pthread_cond_broadcast` wakes ALL waiting threads at once. After waking, each thread
  re-enters the `while (b->flushing)` check. Only one will find `flushing == 0` and
  proceed to set `flushing = 1` — the others loop and wait again. This is correct: at
  most one thread flushes at a time.
- `batch_buf_flush_copy` is called with `b->lock` NOT held (same as before). It malloc's
  a copy of `pending`, resets `pending_n`, then calls `slotcask_bulk_resolve_and_fetch`
  outside the lock. This is unchanged.
- The `volatile int stop` early-exit path (`return -1`) does not interact with
  `flush_done` — it returns before reaching the flushing block.
- `batch_buf_destroy` calls `batch_buf_flush` (drains remaining pending entries) before
  destroying the condvar. By the time `pthread_cond_destroy` is called, all threads that
  could be waiting on `flush_done` have already returned from `batch_buf_collect_hash`
  (the parallel_for_io join has completed).
- `sched.h` is presumably already included (for `sched_yield`). Removing the
  `sched_yield()` call does not require removing the include — leave it in place.
