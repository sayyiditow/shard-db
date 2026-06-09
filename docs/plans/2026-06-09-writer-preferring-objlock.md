# Plan: Writer-preferring rwlocks in objlock.c

**Date:** 2026-06-09  
**Branch:** `feat/writer-preferring-objlock`  
**Goal:** Prevent writer starvation in objlock under sustained read load.

## Problem

`pthread_rwlock_t` defaults to reader-preferring on Linux glibc. Under a
continuous stream of `rdlock` holders (find/count/aggregate queries), a
wrlock waiter (vacuum, add-index, rebuild, schema mutation) can be
indefinitely deferred — a live-livelock where maintenance never completes.
`PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP` flips the preference:
once a writer is waiting, new readers queue behind it.

## Execution rules

- Branch off `main`: `git checkout -b feat/writer-preferring-objlock`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all`
- Never claim a step passed without showing the real output.
- If a quoted anchor is not found exactly, stop and write `PLAN_NOTES.md`.

---

## Task 1 — `src/db/objlock.c`: use writer-preferring attr on Linux

### Anchor (line to find before editing):

```c
            pthread_rwlock_init(&g_objlocks[slot].rwlock, NULL);
```

### Replacement:

```c
#ifdef PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP
            {
                pthread_rwlockattr_t attr;
                pthread_rwlockattr_init(&attr);
                pthread_rwlockattr_setkind_np(&attr,
                    PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
                pthread_rwlock_init(&g_objlocks[slot].rwlock, &attr);
                pthread_rwlockattr_destroy(&attr);
            }
#else
            pthread_rwlock_init(&g_objlocks[slot].rwlock, NULL);
#endif
```

### Why `PREFER_WRITER_NONRECURSIVE_NP` and not `PREFER_WRITER_NP`

The `_NONRECURSIVE` variant is required for correctness: the recursive
variant allows the same thread to acquire a wrlock it already holds, which
conflicts with our objlock usage pattern (a thread holding wrlock for
vacuum must not re-acquire it). The non-recursive variant enforces
exclusivity strictly and is the documented safe choice for this pattern.

### Why `#ifdef` not `#if defined(__linux__)`

`PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP` is defined by glibc when
`_GNU_SOURCE` is set, which is already required by the build. macOS
`pthread.h` simply won't define it so the `#else` branch silently applies —
no extra platform guard needed.

---

## Task 2 — Build and verify

```
SKIP_TESTS=1 ./build.sh
```

Expected: clean compile, no warnings about the new attr calls.

---

## Task 3 — Run full test suite

```
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` (same count as before, currently 4414).

Leave all changes **uncommitted**. Report the exact test output line.
