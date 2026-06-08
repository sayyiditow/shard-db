# Plan: MADV_DONTNEED for segment reads during rebuild_object_v2

**Goal**: After `rebuild_object_v2` processes each segment's records during
`slotcask_walk_live`, call `madvise(MADV_DONTNEED)` on the segment's mmap'd region
before releasing it. This allows the kernel to reclaim those pages immediately instead
of keeping a full copy of the old segment store in the page cache.

**Why**: `rebuild_object_v2` (triggered by `vacuum --compact`, `vacuum --splits`,
`add-field`, `edit-field`) calls `slotcask_walk_live` which under the hood calls
`walk_one_shard_inner` for each kf shard. Pass 3 of that function acquires each
distinct segment exactly once via `segcache_acquire` (MAP_SHARED mmap), reads live
records from it, then releases it via `segcache_release`. No cache-bypass mechanism
exists today — every segment page gets cached. On a 100 GB object this displaces the
entire page cache.

`rebuild_object_v2` holds `objlock_wrlock` for the duration, so no concurrent readers
can be accessing the same object's data — `MADV_DONTNEED` on those segment pages is
safe to apply. The fix is to add a `dontneed` flag through the static call chain and
expose a new public entry point `slotcask_walk_live_dontneed` that rebuild uses instead
of `slotcask_walk_live`.

**Safety invariant**: `MADV_DONTNEED` is ONLY wired to the new `slotcask_walk_live_dontneed`.
The existing `slotcask_walk_live` and `slotcask_walk_one_shard` are unchanged and
continue to behave as before. Never pass `dontneed=1` from query scan paths.

---

## Execution rules

- Branch off `main`: `git checkout -b feat/odirect-rebuild-dontneed`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all`
- Do tasks in order; do not skip.
- Locate every insertion site by the **quoted anchor text** given. If the anchor is not
  found exactly, stop and write `PLAN_NOTES.md` — do not guess or reinterpret.
- Never claim a step passed without showing the real build/test output.

---

## Task 1 — Add `dontneed` parameter to `walk_one_shard_inner`

**File**: `src/db/slotcask.c`

Locate the forward declaration anchor:
```
static int walk_one_shard_inner(SlotcaskDb *db, int kf_shard_id,
                                 SlotcaskScanCb cb, void *ctx,
                                 int *stop_flag);
```

Replace with:
```c
static int walk_one_shard_inner(SlotcaskDb *db, int kf_shard_id,
                                 SlotcaskScanCb cb, void *ctx,
                                 int *stop_flag, int dontneed);
```

---

Locate the thin wrapper anchor:
```
static int walk_one_shard(SlotcaskDb *db, int kf_shard_id,
                          SlotcaskScanCb cb, void *ctx) {
    return walk_one_shard_inner(db, kf_shard_id, cb, ctx, NULL);
}
```

Replace with:
```c
static int walk_one_shard(SlotcaskDb *db, int kf_shard_id,
                          SlotcaskScanCb cb, void *ctx) {
    return walk_one_shard_inner(db, kf_shard_id, cb, ctx, NULL, 0);
}
```

---

Locate the actual definition anchor (the function body open line):
```
static int walk_one_shard_inner(SlotcaskDb *db, int kf_shard_id,
                                 SlotcaskScanCb cb, void *ctx,
                                 int *stop_flag) {
```

Replace with:
```c
static int walk_one_shard_inner(SlotcaskDb *db, int kf_shard_id,
                                 SlotcaskScanCb cb, void *ctx,
                                 int *stop_flag, int dontneed) {
```

---

## Task 2 — Apply `MADV_DONTNEED` before segment release in Pass 3

**File**: `src/db/slotcask.c`

Inside `walk_one_shard_inner`, Pass 3 holds one segment handle across runs of records
sharing `(sid, fid)`. The release happens in two places:

**Place A** — when switching to a new segment:
```
        if ((int)r->sid != held_sid || (int)r->fid != held_fid) {
            if (sh.slot >= 0 || sh.fd >= 0) segcache_release(&sh);
```

Replace with:
```c
        if ((int)r->sid != held_sid || (int)r->fid != held_fid) {
            if (sh.slot >= 0 || sh.fd >= 0) {
                if (dontneed) madvise(sh.map, sh.map_size, MADV_DONTNEED);
                segcache_release(&sh);
            }
```

**Place B** — after the loop ends (final segment release):
```
    if (sh.slot >= 0 || sh.fd >= 0) segcache_release(&sh);
```

Replace with:
```c
    if (sh.slot >= 0 || sh.fd >= 0) {
        if (dontneed) madvise(sh.map, sh.map_size, MADV_DONTNEED);
        segcache_release(&sh);
    }
```

Note: there are exactly two `segcache_release(&sh)` calls in Pass 3 — Places A and B
above. The allocation-failure fallback path (earlier in the function) uses a different
per-record acquire pattern with its own `segcache_release(&sh)` inside the for-loop;
that path does NOT get the `dontneed` treatment (it acquires/releases per-record, so
DONTNEED would be counterproductive there).

---

## Task 3 — Update `slotcask_walk_one_shard` to pass `dontneed`

**File**: `src/db/slotcask.c`

Locate:
```
int slotcask_walk_one_shard(SlotcaskDb *db, int kf_shard_id,
                             SlotcaskScanCb cb, void *ctx,
                             int *stop_flag) {
    if (!db || !cb || kf_shard_id < 0 || kf_shard_id >= db->num_shards)
        return -1;
    return walk_one_shard_inner(db, kf_shard_id, cb, ctx, stop_flag);
}
```

Replace with:
```c
int slotcask_walk_one_shard(SlotcaskDb *db, int kf_shard_id,
                             SlotcaskScanCb cb, void *ctx,
                             int *stop_flag) {
    if (!db || !cb || kf_shard_id < 0 || kf_shard_id >= db->num_shards)
        return -1;
    return walk_one_shard_inner(db, kf_shard_id, cb, ctx, stop_flag, 0);
}
```

---

## Task 4 — Add `slotcask_walk_live_dontneed` (new public function)

**File**: `src/db/slotcask.c`

Locate the end of `slotcask_walk_live`:
```
int slotcask_walk_live(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return -1;
    /* Sequential walk. Parallelism is the engine's job (it knows about
       thread-local output streams and other engine-side state); the
       storage primitive just exposes a per-shard walker. See
       slotcask_walk_one_shard for the per-shard entry point used by the
       engine's parallel scan_shards_v2. */
    int stop_flag = 0;
    for (int s = 0; s < db->num_shards; s++) {
        if (__atomic_load_n(&stop_flag, __ATOMIC_ACQUIRE)) break;
        WalkWorkerArg arg = {
            .db = db, .kf_shard_id = s,
            .cb = cb, .ctx = ctx,
            .stop_flag = &stop_flag,
        };
        walk_worker(&arg);
    }
    return 0;
}
```

Insert immediately after the closing `}` of `slotcask_walk_live`:
```c

/* Same as slotcask_walk_live but calls madvise(MADV_DONTNEED) on each
   segment's mmap region after the last record from that segment is
   emitted.  ONLY safe when the caller holds objlock_wrlock — no concurrent
   readers may be accessing the object's segment files.  Used by
   rebuild_object_v2 (vacuum --compact, --splits, add-field, edit-field). */
int slotcask_walk_live_dontneed(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return -1;
    int stop_flag = 0;
    for (int s = 0; s < db->num_shards; s++) {
        if (__atomic_load_n(&stop_flag, __ATOMIC_ACQUIRE)) break;
        walk_one_shard_inner(db, s, cb, ctx, &stop_flag, /*dontneed=*/1);
    }
    return 0;
}
```

---

## Task 5 — Declare `slotcask_walk_live_dontneed` in slotcask.h

**File**: `src/db/slotcask.h`

Locate:
```
int slotcask_walk_live(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx);
```

Insert immediately after it:
```c
int slotcask_walk_live_dontneed(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx);
```

---

## Task 6 — Use `slotcask_walk_live_dontneed` in `rebuild_object_v2`

**File**: `src/db/query.c`

Locate:
```
    slotcask_walk_live(&legacy_db, v2_rebuild_walk_cb, &walk_ctx);
```

Replace with:
```c
    slotcask_walk_live_dontneed(&legacy_db, v2_rebuild_walk_cb, &walk_ctx);
```

There is exactly one call to `slotcask_walk_live` that passes `v2_rebuild_walk_cb` —
that is the only change in query.c. Do not change any other `slotcask_walk_live` call
sites.

---

## Task 7 — Build and test

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` with the same N as before.

Pay attention to tests involving `vacuum`, `add-field`, `edit-field`, or any rebuild
path.

---

## Invariants and edge cases

- `madvise(MADV_DONTNEED)` on a MAP_SHARED region is safe: on Linux, for MAP_SHARED it
  marks pages as evictable under pressure but does not unmap them. Other processes
  accessing the same pages via their own mapping will still see valid data (no page
  fault). Since rebuild holds objlock_wrlock, no other thread holds a segcache handle
  on the same object's segments during the walk, so the madvise only affects pages that
  the walk itself brought in.
- The allocation-failure fallback path (per-record segcache_acquire in the early part of
  `walk_one_shard_inner`) is not changed. That path acquires and releases each segment
  handle one at a time, inside its own loop, and is not reached by rebuild in practice
  (requires malloc failure for the WalkRecRef array).
- Place A and Place B are the only two `segcache_release(&sh)` calls in Pass 3. Verify
  there are no others after any edits.
- The `madvise` call is placed before `segcache_release` so the map pointer is still
  valid. `segcache_release` may munmap the region if the cache evicts this entry.
