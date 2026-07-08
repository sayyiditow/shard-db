# Perf: move syscalls out of the btree-cache global lock on the miss path

## Nature of this plan

Perf refactor of `bt_acquire` / eviction in `src/db/btree.c`. The **hit** path
is already well designed (global `bt_cache_lock` held only for the probe, then
a per-entry rwlock with verify-and-retry). But on a **miss**, all of this runs
while holding the global mutex:

1. `bt_open_file()` — `open` + `fstat` + `mmap` + `madvise` (and for fresh
   writer files, `ftruncate` + header init),
2. the O(`bt_cache_slots`) LRU scan,
3. `bt_cache_drop_slot()` on the victim — `msync` + `munmap` + `close`.

A cold-cache indexed query fans out across up to 128 index shards in
parallel (`index_splits_for`), and every worker funnels through that one
mutex doing syscalls. This plan (a) defers the eviction victim's teardown to
after the lock is released, and (b) moves the `open`+`mmap` outside the lock
**for readers only**.

**Why readers only:** fresh-file creation (`writer=1`: `O_CREAT` +
`ftruncate` + `bt_init_file`) is race-free today *only because* the table
lock serializes `bt_open_file`. Two unserialized writers racing to create the
same index file could re-run `bt_init_file` and zero a tree the winner is
already inserting into. Readers never create or init files, and the parallel
fan-out that motivates this change is read-side. Writers keep the under-lock
open. Do not "improve" this by including writers.

**Lost-race disposition:** if, while a reader was opening the file unlocked,
another thread cached the same path, the reader serves its own fresh mapping
uncached (`slot = -1`; `bt_release` unmaps it). This is the same documented
tradeoff the code already accepts for the cache-full and retry-cap
fallbacks ("the duplicate MAP_SHARED is coherent"). No new hazard class is
introduced.

Behavior-preserving: no wire, on-disk, or result changes — only lock scope.
Bench validation (`bench-queries` cold start) is the **user's** step after
review; do not run benches to validate.

## Execution rules (read first)

- Branch off `main`: `git checkout -b perf/btcache-miss-path`.
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run-all`
  after **every** phase. Not green → STOP + `PLAN_NOTES.md`.
- Every edit locates its site by **quoted anchor text**; anchor not found
  exactly → STOP + `PLAN_NOTES.md`. All edits are in `src/db/btree.c`, which
  no other pending plan touches.
- Leave uncommitted; stop for review after each phase.

## Phase 0 — Baseline

`SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all` — record the
`# total:` line. Every phase must match it exactly.

## Phase 1 — deferred victim teardown

**1a.** Anchor on the comment block starting
`/* Tear down a cache slot. Caller holds bt_cache_lock and ensures no holder`
and the function `static void bt_cache_drop_slot(int slot) {` below it.
Replace the entire function (keep the comment block above it) with:

```c
/* Detach a slot's resources under bt_cache_lock without doing any syscalls.
   The caller disposes the returned fd/map AFTER releasing bt_cache_lock via
   bt_dispose_mapping(). Same no-rwlock-holder contract as before. */
static void bt_cache_evict_slot(int slot, int *out_fd, uint8_t **out_map,
                                size_t *out_sz) {
    BtCacheEntry *e = &bt_cache[slot];
    *out_fd = -1; *out_map = NULL; *out_sz = 0;
    if (!e->used) return;
    *out_fd = e->fd;
    *out_map = e->map;
    *out_sz = e->map_size;
    e->map = NULL;
    e->fd = -1;
    e->map_size = 0;
    e->used = 0;
    e->path[0] = '\0';
    bt_cache_count--;
}

/* Flush + unmap + close a detached mapping. Never call with bt_cache_lock
   held — keeping syscalls out of that lock is the point. */
static void bt_dispose_mapping(int fd, uint8_t *map, size_t map_size) {
    if (map && map_size > 0) msync(map, map_size, MS_ASYNC);
    if (map) munmap(map, map_size);
    if (fd >= 0) close(fd);
}

/* Synchronous wrapper for callers whose teardown is not hot (invalidate).
   NOTE: still does syscalls under bt_cache_lock at those call sites; they
   are admin-path only (remove-index). */
static void bt_cache_drop_slot(int slot) {
    int fd; uint8_t *map; size_t sz;
    bt_cache_evict_slot(slot, &fd, &map, &sz);
    bt_dispose_mapping(fd, map, sz);
}
```

**1b.** In `bt_acquire`, anchor on:

```c
    /* Evict LRU when over half-full or the probe couldn't find an empty slot. */
    if (slot < 0 || bt_cache_count >= bt_cache_slots / 2) {
```

and on the block's interior:

```c
        if (lru >= 0) {
            bt_cache_drop_slot(lru);
            slot = lru;
        }
```

Introduce victim locals immediately **before** the `/* Evict LRU ... */`
comment:

```c
    int vic_fd = -1; uint8_t *vic_map = NULL; size_t vic_sz = 0;
```

and change the interior to:

```c
        if (lru >= 0) {
            bt_cache_evict_slot(lru, &vic_fd, &vic_map, &vic_sz);
            slot = lru;
        }
```

**1c.** Both return paths after the eviction block must dispose the victim
**after** their `pthread_mutex_unlock(&bt_cache_lock);`:

- Uncached-full path — anchor:

```c
    if (slot < 0) {
        /* Cache truly full — serve uncached. */
        pthread_mutex_unlock(&bt_cache_lock);
```

  insert directly after the unlock line:

```c
        bt_dispose_mapping(vic_fd, vic_map, vic_sz);
```

- Install path — anchor:

```c
    if (writer) pthread_rwlock_wrlock(lock);
    else        pthread_rwlock_rdlock(lock);
    pthread_mutex_unlock(&bt_cache_lock);
```

  insert directly after that unlock line:

```c
    bt_dispose_mapping(vic_fd, vic_map, vic_sz);
```

(`bt_dispose_mapping` is a no-op when no victim was evicted — locals stay
`-1/NULL/0`.)

Build + `run-all`: must match Phase 0. Stop for review.

## Phase 2 — reader-only optimistic open

In `bt_acquire`, anchor on:

```c
    __atomic_add_fetch(&g_bt_cache_misses, 1, __ATOMIC_RELAXED);

    int fd;
    uint8_t *map;
    size_t sz;
    if (bt_open_file(path, writer, &fd, &map, &sz) < 0) {
        pthread_mutex_unlock(&bt_cache_lock);
        return -1;
    }
```

Replace that hunk with:

```c
    __atomic_add_fetch(&g_bt_cache_misses, 1, __ATOMIC_RELAXED);

    int fd;
    uint8_t *map;
    size_t sz;
    if (!writer) {
        /* Readers: open+mmap OUTSIDE the table lock so parallel cold-cache
           index fan-out doesn't serialize on syscalls. Safe for readers
           only — they never create/init the file. Writers stay under the
           lock: fresh-file creation (O_CREAT + ftruncate + bt_init_file)
           relies on the table lock for serialization. */
        pthread_mutex_unlock(&bt_cache_lock);
        if (bt_open_file(path, 0, &fd, &map, &sz) < 0) return -1;
        pthread_mutex_lock(&bt_cache_lock);
        int refound = 0;
        slot = bt_cache_probe(path, &refound);
        if (refound) {
            /* Lost the install race: another thread cached this path while
               we were opening. Serve our fresh mapping uncached — duplicate
               MAP_SHARED of the same file is coherent (same accepted
               tradeoff as the cache-full fallback below); bt_release will
               munmap+close it. */
            pthread_mutex_unlock(&bt_cache_lock);
            bt->slot = -1;
            bt->fd = fd;
            bt->map = map;
            bt->map_size = sz;
            return 0;
        }
    } else {
        if (bt_open_file(path, 1, &fd, &map, &sz) < 0) {
            pthread_mutex_unlock(&bt_cache_lock);
            return -1;
        }
    }
```

Invariants to preserve (verify, do not change):

- The reader re-probe result (`slot`, possibly `-1` when the table is full)
  flows into the existing eviction/install logic unchanged — Phase 1's code
  handles `slot < 0` and the install path identically for both branches.
- `mkdirp(dirname_of(path))` inside `bt_open_file` uses a static buffer; it
  is only reached on the `writer=1` branch, which remains lock-serialized.
- The retry-cap fall-through above this hunk (`if (++retries >= 4)`)
  re-acquires `bt_cache_lock` before breaking out of the hit loop; both
  branches of the new hunk are entered with the lock held, exactly as before.

Build + `run-all`: must match Phase 0. Stop for review.

## Phase 3 — race validation

1. `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh` (TSAN build — `BUILD_MODE` is an
   env var read via `BUILD_MODE="${BUILD_MODE:-release}"` in build.sh;
   build.sh takes no positional args, so `./build.sh tsan` silently builds
   a plain release binary and skips sanitizing entirely). `SKIP_TESTS=1`
   avoids running the full suite under TSAN's overhead before the targeted
   subset below. Then run the index/btree-heavy subset:
   `./build/bin/shard-db-test run-all --filter index` and
   `--filter btree` (and `--filter cursor` if registered). Any data-race
   report → STOP + `PLAN_NOTES.md` with the full TSAN output.
2. Rebuild release (`SKIP_TESTS=1 ./build.sh`) and `run-all` — must match
   Phase 0.

## Guardrails

- Do **not** extend the optimistic open to writers (see "Why readers only").
- Do **not** change the LRU policy or scan in this plan. The scan is
  memory-only; with the syscalls moved out it is microseconds under the
  lock. Sampled eviction is possible future work, gated on bench evidence.
- Do **not** touch the hit path's verify-and-retry loop or the rwlock
  handoff contract (`bt_release` is the matched unlock).
- `btree_cache_invalidate` and `bt_cache_shutdown` keep their current
  synchronous teardown semantics (via the `bt_cache_drop_slot` wrapper and
  the shutdown loop respectively).

## Definition of done

- No syscall (`open/fstat/mmap/madvise/msync/munmap/close/ftruncate`) is
  executed while `bt_cache_lock` is held on the **reader** miss path or in
  either branch's eviction teardown. (Writer `bt_open_file` under the lock
  is the one intentional exception — call it out in the branch message.)
- `run-all` matches Phase 0 at every phase boundary; TSAN subset clean.
- Branch message notes: reader-only scope, lost-race-serves-uncached
  disposition, and that cold-start bench validation (`bench-queries`) is
  pending user measurement.
- Leave uncommitted.
