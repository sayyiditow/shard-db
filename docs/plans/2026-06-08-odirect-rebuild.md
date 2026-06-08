# Plan: O_DIRECT segment scan for rebuild_object_v2

**Goal**: Replace the mmap-based `slotcask_walk_live` call in `rebuild_object_v2` with a
true O_DIRECT segment scan (`slotcask_walk_live_o_direct`) so that rebuilding an object's
storage does not pull all its segment data into the page cache.

**Why**: `rebuild_object_v2` is called by `cmd_add_field`, `cmd_remove_field`,
`cmd_edit_field`, and `cmd_reindex`. It reads every live record once from the old DB.
The current path goes through `slotcask_walk_live` → `walk_one_shard_inner` → per-shard
`segcache_acquire` (MAP_SHARED mmap). All segment pages are faulted into cache and stay
there after the rebuild. On a 50 M-record object that can be tens of GBs of cache
eviction. O_DIRECT bypasses the page cache entirely.

**Approach**: Add `slotcask_walk_live_o_direct(db, cb, ctx)` to `slotcask.c` / `slotcask.h`.
It enumerates all `.dat` segment files under each stream directory (the same readdir
pattern as `scan_shards_v2_o_direct` in `query.c`) and calls `seg_scan_o_direct` for each,
using a thin `od_record_cb` → `SlotcaskScanCb` adapter. Sequential, not parallel —
`rebuild_object_v2` holds `objlock_wrlock` and the V2RebuildCtx is not thread-safe.

---

## Execution rules

- Branch off `main`: `git checkout -b feat/odirect-rebuild`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all`
- Do tasks in order; do not skip.
- Locate every insertion site by the **quoted anchor text** given. If the anchor is not
  found exactly, stop and write `PLAN_NOTES.md` — do not guess or reinterpret.
- Never claim a step passed without showing the real build/test output.

---

## Task 1 — Add `#include "io_direct.h"` to `slotcask.c`

**File**: `src/db/slotcask.c`

Locate the anchor (the last system include before the extern declaration):
```
#include <time.h>
#include <pthread.h>
```

Replace with:
```c
#include <time.h>
#include <pthread.h>
#include "io_direct.h"
```

If `#include "io_direct.h"` is already present (because another plan ran first), skip this
task and write `PLAN_NOTES.md: io_direct.h already included`.

---

## Task 2 — Add `WalkOdAdapter` struct and `walk_od_rec_cb` adapter

**File**: `src/db/slotcask.c`

Locate the anchor (the comment immediately before `slotcask_walk_live`):
```
int slotcask_walk_live(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return -1;
    /* Sequential walk. Parallelism is the engine's job (it knows about
       thread-local output streams and other engine-side state); the
       storage primitive just exposes a per-shard walker. See
       slotcask_walk_one_shard for the per-shard entry point used by the
       engine's parallel scan_shards_v2. */
```

Insert immediately BEFORE this anchor:
```c
/* Adapter: od_record_cb (raw slot bytes) → SlotcaskScanCb (parsed key/value).
   Used by slotcask_walk_live_o_direct for cache-bypassing full-DB walks. */
typedef struct {
    SlotcaskScanCb  cb;
    void           *ctx;
    int             stop;
} WalkOdAdapter;

static int walk_od_rec_cb(const uint8_t *rec, size_t vlen,
                           const uint8_t hash16[16], void *raw) {
    WalkOdAdapter *a = (WalkOdAdapter *)raw;
    if (a->stop) return 1;
    uint16_t klen;
    memcpy(&klen, rec + 16, 2);
    const uint8_t *key   = rec + 24;
    const uint8_t *value = rec + 24 + (size_t)klen;
    int rc = a->cb(hash16, key, klen, value, vlen, a->ctx);
    if (rc != 0) a->stop = 1;
    return rc;
}

```

---

## Task 3 — Add `slotcask_walk_live_o_direct`

**File**: `src/db/slotcask.c`

Locate the anchor (the `slotcask_walk_live` closing brace followed by the next comment):
```
    return 0;
}

/* Walk one kf shard's live entries into `cb`. Same semantics as
   slotcask_walk_live but scoped to a single shard so the engine can
   parallelise across shards
```

Insert immediately after the `}` (after `return 0;`) and before the next comment:
```c

/* Cache-bypassing full-DB walk via O_DIRECT segment scans.
   Enumerates every .dat file under <data_dir>/data/streams/<s>/ and
   calls seg_scan_o_direct on each. Segment pages never enter the page
   cache. Sequential (not parallel) — callers like rebuild_object_v2 hold
   objlock_wrlock and pass a non-thread-safe V2RebuildCtx. */
int slotcask_walk_live_o_direct(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return -1;
    WalkOdAdapter a = { .cb = cb, .ctx = ctx, .stop = 0 };
    for (int s = 0; s < db->num_streams && !a.stop; s++) {
        char stream_dir[PATH_MAX];
        snprintf(stream_dir, sizeof(stream_dir),
                 "%s/data/streams/%03d", db->data_dir, s);
        DIR *dh = opendir(stream_dir);
        if (!dh) continue;
        struct dirent *de;
        while (!a.stop && (de = readdir(dh)) != NULL) {
            size_t nlen = strlen(de->d_name);
            if (nlen < 4 || strcmp(de->d_name + nlen - 4, ".dat") != 0) continue;
            char seg_path[PATH_MAX];
            snprintf(seg_path, sizeof(seg_path), "%s/%s", stream_dir, de->d_name);
            seg_scan_o_direct(seg_path, db->slot_size, walk_od_rec_cb, &a);
        }
        closedir(dh);
    }
    return 0;
}

```

---

## Task 4 — Declare `slotcask_walk_live_o_direct` in `slotcask.h`

**File**: `src/db/slotcask.h`

Locate the anchor (the existing declaration of `slotcask_walk_live`):
```
int slotcask_walk_live(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx);
```

Replace with:
```c
int slotcask_walk_live(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx);

/* O_DIRECT variant: same contract as slotcask_walk_live but reads segment
   files with cache-bypassing I/O. Use for one-shot full-DB walks (rebuild,
   schema migration) where page-cache pollution is costly. Sequential. */
int slotcask_walk_live_o_direct(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx);
```

---

## Task 5 — Call `slotcask_walk_live_o_direct` in `rebuild_object_v2`

**File**: `src/db/query.c`

Locate the anchor (the `slotcask_walk_live` call inside `rebuild_object_v2`):
```
    slotcask_walk_live(&legacy_db, v2_rebuild_walk_cb, &walk_ctx);
```

Replace with:
```c
    slotcask_walk_live_o_direct(&legacy_db, v2_rebuild_walk_cb, &walk_ctx);
```

---

## Task 6 — Build and test

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` with the same N as before.
Pay attention to tests involving `add-field`, `remove-field`, `edit-field`, `vacuum`.

---

## Invariants and edge cases

- `seg_scan_o_direct` calls the callback only for live (flag==1) slots — tombstones and
  empty slots are skipped. This matches `walk_one_shard_inner`'s behaviour (only live KF
  entries are emitted).
- `walk_od_rec_cb` reads `klen` from `rec + 16` (uint16 LE), matching the segment slot
  layout (`[hash16][klen2B][flag1B][rsv1B][vlen4B][key][value]`).
- The O_DIRECT walk visits records in segment-file order (not KF-shard order). This is
  fine for `v2_rebuild_walk_cb` — it inserts each record into the new DB independently.
  Sequence-number backfill uses `atomic_store_explicit` so concurrent ordering doesn't
  matter even if we later parallelise.
- If `seg_scan_o_direct` encounters a filesystem that doesn't support O_DIRECT, it falls
  back to buffered I/O with `POSIX_FADV_DONTNEED` internally — the caller is unaffected.
- If `od_open` / `seg_scan_o_direct` fails for a segment (e.g. ENOENT on a file just
  unlocked by a concurrent vacuum), that file's records are silently skipped. This is the
  same as `walk_one_shard_inner`'s `if (segcache_acquire(...) != 0) continue`.
- `slotcask_walk_live_o_direct` is sequential — no `parallel_for_io`. Callers that need
  parallelism (engine query paths) already use `scan_shards_v2_o_direct`.
