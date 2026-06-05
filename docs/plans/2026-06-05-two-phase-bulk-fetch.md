# Plan: Two-Phase Bulk Fetch Consolidation

**Date:** 2026-06-05
**Status:** Draft
**Priority:** High (performance + technical debt)

## Problem

The codebase has 3 different fetch approaches doing the same thing, each wrong in its own way:

| Function | Data source | File opens per call | Problem |
|---|---|---|---|
| `shard_find_worker` + `search_shard_worker` | `data/NNN.bin` (old shard) | 1 per shard | Uses legacy linear-probe format, legacy file path |
| `read_record_ref` (in loops) | KF + segment (v2) | KF + segment per record | Worst: opens KF and segment **per record** — D2, keyset emit, process_batch |
| `slotcask_bulk_lookup_by_hash` / `slotcask_bulk_get_in_kfshard` | KF + segment (v2) | 1 KF + N segments, sequential | Segments read **sequentially** within each KF shard |

**Root causes:**

1. **Parallelism at wrong granularity.** Current code groups by KF shard then reads segment files sequentially within each shard. But a segment file can be referenced by multiple KF shards — each re-opens it. The I/O bottleneck is segment files, not KF shards.

2. **`parallel_for` for segment reads.** The bounded CPU pool (`parallel_for`) is used for KF shard workers that do segment I/O inside. This blocks the pool that other queries need. Segment reads are I/O-bound (mmap page faults on cold files) and should use `parallel_for_io` (dedicated pthreads, oversubscription-tolerant).

3. **KF probe done twice in bitmap paths.** `shard_count_worker`'s bitmap post-filter path already walks the KF (linear-probing to find the slot for `bm_test`), capturing `SlotcaskKfEntry *e` with `(sid, fid, off)`. Then it discards these and calls `slotcask_bulk_lookup_by_hash` which re-probes the same KF from scratch.

## New Two-Phase Model

Two clean entry points with two internal phases:

### Phase 1 — Resolve (CPU-bound, memory-only)

Input: flat array of hashes (or keys → compute hash)
Output: flat array of `SlotcaskResolvedRec` `(sid, fid, off, hash)` — the segment file and byte offset for each hash

Each hash maps to exactly one KF shard via `shard_for_hash(hash, num_shards)` — deterministic, O(1). No scanning needed. One pass to bucket all hashes into per-KF-shard groups.

**Dispatch: `parallel_for` (bounded CPU pool).** KF files are independent (different rwlocks). Work is memory-bound (read from mmap'd KF, linear-probe with 1-2 iterations avg). Fast: even 100K hashes takes ~1-2ms.

```
For each unique KF shard (parallel_for across shards):
  kfcache_acquire(kf_path, rdlock)
  for each hash in this shard:
    kf_lookup_no_verify(hash) → (sid, fid, off)
    append SlotcaskResolvedRec { hash, sid, fid, off } to resolved array
  kfcache_release
```

### Phase 2 — Fetch (I/O-bound, parallel across segment files)

Input: flat array of `SlotcaskResolvedRec`
Output: callback per live record

**Dispatch: `parallel_for_io` (dedicated pthreads).** Segment files are independent files. Oversubscription is beneficial for I/O wait (mmap page faults). One thread per unique segment file.

```
sort resolved array by (sid, fid)
group into contiguous runs of same (sid, fid)
for each run (parallel_for_io across runs):
  seg_path_for(path, sid, fid)
  segcache_acquire(path, rdlock)
  for each record in this run:
    rec = map + off
    if seg_rec_live_with_hash(rec, hash):  // verify hash matches
      klen = seg_rec_klen(rec)
      vlen = seg_rec_vlen(rec)
      cb(hash, rec+24, klen, rec+24+klen, vlen, ctx)
  segcache_release
```

### Two entry points

**Entry A: `slotcask_bulk_resolve_and_fetch`** — for callers that have hashes (D2, keyset emit, process_batch, cursor, count, agg, multi-get):
```
Phase 1 (resolve) + Phase 2 (fetch)
```

**Entry B: `slotcask_bulk_fetch_resolved`** — for callers that already know (sid, fid, off) (bitmap post-filter path):
```
Phase 2 (fetch) only — no re-probe
```

## New Types

```c
/* Resolved record location — output of phase 1 KF probe.
   24 bytes: 16B hash + 1B sid + 2B fid + 4B off + 1B padding. */
typedef struct __attribute__((packed)) {
    uint8_t  hash[16];
    uint8_t  sid;          /* stream id */
    uint16_t fid;          /* file id */
    uint32_t off;          /* byte offset in segment file */
} SlotcaskResolvedRec;
```

## New Functions

### In `slotcask.h` (declarations) + `slotcask.c` (implementations)

```c
// ===== Phase 1: Resolve =====

/* Resolve hashes to segment file + offset locations.
   Takes a flat array of hashes (any KF shards).
   Buckets by shard_for_hash internally, probes each KF shard sequentially.
   Returns malloc'd array of SlotcaskResolvedRec. *out_n = count of found records.
   Caller free()s the returned pointer. Returns NULL on error/not found. */
SlotcaskResolvedRec *slotcask_bulk_resolve_hashes(SlotcaskDb *db,
                                                   const uint8_t (*hashes)[16],
                                                   size_t n,
                                                   size_t *out_n);

// ===== Phase 2: Fetch =====

/* Fetch records from pre-resolved locations.
   Groups input by (sid, fid) and dispatches parallel_for_io across
   unique segment files. Each segment file is opened once via segcache.
   Records are verified via seg_rec_live_with_hash before callback.
   Callback signature matches existing slotcask_bulk_lookup_by_hash consumers.
   Returns 0 on success, -1 on error. */
int slotcask_bulk_fetch_resolved(SlotcaskDb *db,
                                  const SlotcaskResolvedRec *recs,
                                  size_t n,
                                  void *ctx,
                                  int (*cb)(const uint8_t *hash16,
                                            const uint8_t *key, uint16_t klen,
                                            const uint8_t *value, uint32_t vlen,
                                            void *ctx));

// ===== Combined =====

/* Resolve + fetch in one call (wraps both phases above).
   Same callback signature as slotcask_bulk_fetch_resolved.
   This is the primary entry point for callers with hashes. */
int slotcask_bulk_resolve_and_fetch(SlotcaskDb *db,
                                     const uint8_t (*hashes)[16],
                                     size_t n,
                                     void *ctx,
                                     int (*cb)(const uint8_t *hash16,
                                               const uint8_t *key, uint16_t klen,
                                               const uint8_t *value, uint32_t vlen,
                                               void *ctx));
```

### Internal helpers (static in slotcask.c)

```c
/* parallel_for_io worker for one segment file.
   Reads all records at their offsets, calls cb per live verified record. */
static void *seg_fetch_worker(void *arg);
```

## Implementation Details

### `slotcask_bulk_resolve_hashes` internals

```
1. Allocate resolved array with capacity n (worst-case all hashes found).
2. Determine number of KF shards for this database (db->num_shards).
3. Allocate per-shard hash-lists (or just iterate n twice: once to count/bucket, once to resolve).
   Simpler: single pass. For each hash i in 0..n-1:
    - sid_kf = shard_for_hash(hashes[i], db->num_shards)
    - track unique shards in a bitset or small array
    - store hash index -> shard mapping for second pass (or re-iterate)
   Or even simpler for first implementation:
    - Iterate all hashes, group by KF shard using a small array-of-arrays pattern:
      - Allocate `HashGroup { uint8_t (*hashes)[16]; int count; } groups[db->num_shards]`
      - Count first, allocate, then fill in second pass
    - Then for each non-empty shard:
      - kfcache_acquire
      - For each hash in this group: kf_lookup_no_verify → if found, append to resolved
      - kfcache_release
4. If resolved_n < n, optionally realloc to fit (or return as-is — caller won't read past out_n).
5. Return resolved array, set *out_n = resolved_n.
```

KF uniqueness tracking: a small `int seen[MAX_SHARDS]` or a dynamically-allocated list. `MAX_SPLITS = 4096`, so even a 4096-int array is 16KB — fine for stack or a small alloc.

### `slotcask_bulk_fetch_resolved` internals

```
1. If n == 0: return 0
2. Sort the resolved array by (sid, fid) — use qsort, 3-byte key comparison
3. Walk the sorted array, counting unique (sid, fid) pairs → nfiles
4. Allocate SegFetchArg[nfiles]
5. Walk again, filling SegFetchArg:
   - For each contiguous run of same (sid, fid):
     - Build seg_path
     - Store pointer to first record + count
6. If nfiles < 4: run sequentially (avoid thread overhead for trivial cases)
   - Else: parallel_for_io(seg_fetch_worker, args, nfiles, sizeof(SegFetchArg))
7. Free args, return 0
```

```c
typedef struct {
    char                    path[PATH_MAX];   /* segment file path */
    SlotcaskResolvedRec    *recs;             /* pointer into sorted array */
    size_t                  count;            /* number of records in this file */
    void                   *ctx;              /* user callback context */
    int                   (*cb)(const uint8_t *, const uint8_t *, uint16_t,
                                const uint8_t *, uint32_t, void *);
} SegFetchArg;
```

### `seg_fetch_worker` internals

```c
static void *seg_fetch_worker(void *arg) {
    SegFetchArg *fa = (SegFetchArg *)arg;
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, fa->path, 0, 0) != 0) return NULL;
    for (size_t i = 0; i < fa->count; i++) {
        const uint8_t *rec = h.map + fa->recs[i].off;
        // Verify hash matches at this location (handles KF hash collision)
        if (!seg_rec_live_with_hash(rec, fa->recs[i].hash)) continue;
        uint16_t klen = seg_rec_klen(rec);
        uint32_t vlen = seg_rec_vlen(rec);
        if (fa->cb(rec + 24, klen, rec + 24 + klen, vlen, fa->ctx) != 0)
            break;  /* callback signaled stop */
    }
    segcache_release(&h);
    return NULL;
}
```

### Callback signature

The existing callbacks used in the codebase (`cursor_fetch_cb`, `count_batch_cb`, etc.) all match `(hash16, key, klen, value, vlen, ctx)` — the same signature as the existing `slotcask_bulk_lookup_by_hash`. **No callback refactoring needed.** Callers just swap their `slotcask_bulk_lookup_by_hash` call for `slotcask_bulk_resolve_and_fetch` with the same callback.

For callers that currently use `read_record_ref` (D2, keyset emit), the callback wraps the existing inline logic (criteria_match_tree + extract sort key + heap push, or emit).

## Callers to Convert

### Group A — currently use `slotcask_bulk_lookup_by_hash` (straightforward swap)

These already have a flat hash array + callback. Replace call with `slotcask_bulk_resolve_and_fetch`.

#### A1: `cursor_fetch_worker` (query.c:17283)

```c
// Before:
uint8_t (*hashes)[16] = malloc(...);
for (int i = 0; i < ctx->entry_count; i++)
    memcpy(hashes[i], ctx->entries[i].hash, 16);
slotcask_bulk_lookup_by_hash(sdb, sid, hashes, (size_t)ctx->entry_count,
                              cursor_fetch_cb, ctx);
free(hashes);

// After:
slotcask_bulk_resolve_and_fetch(sdb, ctx->hashes, ctx->entry_count,
                                 ctx, cursor_fetch_cb);
// (hashes are already in ctx->entries[i].hash — extract once at worker start)
```

`cursor_fetch_cb` already has the right signature. Note: the caller (`shard_group_batch`) already groups `CursorFetchCtx` entries by KF shard. The current code re-computes the shard_id from the first hash. For the new resolve phase, we don't need pre-grouping — `resolve_hashes` handles cross-shard internally.

**Change detail:** The worker currently receives `ctx->entries` as `CollectedHash[]` pre-grouped by shard_id. With the new API, we can simplify: just extract all hashes into a flat array and call `resolve_and_fetch` which handles KF shard grouping internally.

#### A2: `shard_count_worker` — non-bitmap path (query.c:10590)

```c
// Before (lines 10590-10594, 10669-10686):
uint8_t (*fetch_hashes)[16] = calloc((size_t)n_need_fetch, sizeof(*fetch_hashes));
for (int ei = 0; ei < n_need_fetch; ei++)
    memcpy(fetch_hashes[ei], sc->entries[ei].hash, 16);
...
slotcask_bulk_lookup_by_hash(batch_sdb, shard_id, fetch_hashes,
                              (size_t)n_need_fetch, count_batch_cb, &cb_ctx);

// After:
if (n_need_fetch > 0) {
    slotcask_bulk_resolve_and_fetch(batch_sdb, fetch_hashes,
                                     (size_t)n_need_fetch, &cb_ctx, count_batch_cb);
}
free(fetch_hashes);
free(fetch_idx);
```

`count_batch_cb` already has the right signature. The `shard_id` variable and pre-grouping by shard is no longer needed — `resolve_hashes` handles it.

#### A3: `shard_agg_worker` (query.c:23360)

```c
// Before: slotcask_bulk_lookup_by_hash(sdb, sid, hashes, count, agg_scan_cb, ctx)
// After:  slotcask_bulk_resolve_and_fetch(sdb, hashes, count, ctx, agg_scan_cb)
```

### Group B — bitmap post-filter path (skip phase 1, biggest win)

#### B1: `shard_count_worker` — bitmap path (query.c:10605-10666)

Currently this path:
1. Linear-probes KF to find the slot for each hash
2. Tests bitmap membership at the KF slot
3. If bitmaps pass but non-bitmap post-filters exist, re-collects the hash
4. Calls `slotcask_bulk_lookup_by_hash` which **re-probes the same KF**

With the new model, during step 1 the code already has `SlotcaskKfEntry *e` (at line 10617) which contains `e->stream_id`, `e->file_id`, `e->offset`. **Capture these directly** instead of discarding them and re-probing later.

```c
// During KF probe (around line 10617):
SlotcaskKfEntry *e = &kf[slot];
if (memcmp(e->hash, sc->entries[ei].hash, 16) != 0) continue;
kf_slot = (uint32_t)slot;
kf_found = 1;
// NEW: capture resolved location
resolved_sid = e->stream_id;
resolved_fid = e->file_id;
resolved_off = e->offset;

// ... bitmap tests ...

// When record needs fetching (line 10662), instead of:
//   memcpy(fetch_hashes[n_need_fetch], sc->entries[ei].hash, 16);
// Build resolved array directly:
resolved[n_need_fetch] = (SlotcaskResolvedRec){
    .sid = resolved_sid, .fid = resolved_fid,
    .off = resolved_off
};
memcpy(resolved[n_need_fetch].hash, sc->entries[ei].hash, 16);
n_need_fetch++;
```

After the loop, call `slotcask_bulk_fetch_resolved` instead of `slotcask_bulk_lookup_by_hash`:

```c
if (n_need_fetch > 0) {
    slotcask_bulk_fetch_resolved(batch_sdb, resolved, n_need_fetch,
                                  &cb_ctx, count_batch_cb);
}
```

**This eliminates the KF re-probe entirely.** The segment file locations are already known from the first KF walk.

### Group C — currently use `slotcask_bulk_get_in_kfshard` (key-based)

#### C1: `multi_get_shard_worker` (storage.c:2370)

Currently calls `slotcask_bulk_get_in_kfshard(sdb, kf_shard_id, batch, count, vals, vlens)`. This function takes keys (not hashes), computes hashes internally, probes KF, reads segments.

Change: extract the pre-computed hashes from `MultiGetEntry.hash` (already computed during `parse_multi_key`), call `slotcask_bulk_resolve_and_fetch` with a callback that decodes the value and stores `result_json`.

```c
// Before:
slotcask_bulk_get_in_kfshard(sdb, kf_shard_id, batch, (size_t)sw->count, vals, vlens);
for (int ei = 0; ei < sw->count; ei++) {
    MultiGetEntry *e = &sw->entries[ei];
    if (batch[ei].status == 0 && vals[ei]) {
        char *decoded = typed_decode(sw->fs->ts, (const uint8_t *)vals[ei], (uint32_t)vlens[ei]);
        e->result_json = decoded ? decoded : strdup("null");
        free(vals[ei]);
    }
}

// After:
// Collect hashes from entries:
uint8_t (*hashes)[16] = ...;  // from entries[i].hash
typedef struct { SlotcaskDb *sdb; MultiGetShardWork *sw; int *statuses; } MultiGetCtx;
// Callback decodes value inline:
static int multi_get_fetch_cb(const uint8_t *hash16, const uint8_t *key, uint16_t klen,
                               const uint8_t *value, uint32_t vlen, void *ctx) {
    MultiGetCtx *mc = (MultiGetCtx *)ctx;
    // Find which entry this hash corresponds to
    // ...
    char *decoded = typed_decode(mc->sw->fs->ts, value, vlen);
    // Store result_json...
    return 0;
}
slotcask_bulk_resolve_and_fetch(sdb, hashes, count, &mc, multi_get_fetch_cb);
```

### Group D — currently use per-record `read_record_ref` (key set iteration)

#### D1: D2 fetch+sort — cursor (query.c:18201)

Currently calls `read_record_ref` per hash in the prefilter keyset:

```c
for (size_t i = 0; i < n_pre; i++) {
    RecordRef rr;
    if (read_record_ref(db_root, object, &sch, rows[i].hash, &rr) != 0) continue;
    if (tree && !criteria_match_tree(...)) { release_record_ref(&rr); continue; }
    // extract sort key, push to heap
    release_record_ref(&rr);
}
```

Change: collect all hashes from the prefilter keyset → `slotcask_bulk_resolve_and_fetch` with a callback that does criteria_match_tree + sort key extraction + heap push.

```c
// Collect all hashes from prefilter keyset:
size_t hash_count = keyset_size(cursor_prefilter_ks);
uint8_t (*hashes)[16] = malloc(hash_count * sizeof(*hashes));
keyset_iter(cursor_prefilter_ks, hash_collect_cb, hashes);  // or iterate manually

// Batch resolve + fetch with callback:
D2Ctx d2_ctx = { .heap = ..., .tree = tree, .sch = &sch, ... };
slotcask_bulk_resolve_and_fetch(sdb, hashes, hash_count, &d2_ctx, d2_fetch_cb);
free(hashes);

// Callback:
static int d2_fetch_cb(const uint8_t *hash16, const uint8_t *key, uint16_t klen,
                        const uint8_t *value, uint32_t vlen, void *ctx) {
    D2Ctx *c = (D2Ctx *)ctx;
    if (c->tree && !criteria_match_tree(value, c->tree, c->fs)) return 0;
    // extract sort key bytes
    typed_field_to_index_key(c->ts, value, c->order_field_idx, sort_key, &sklen);
    // push to heap (or full materialize)
    ...
    return 0;
}
```

**Key point:** the heap still limits memory to `offset + limit` entries. The callback fires per record, same as the loop — but fetch is now batched and parallelized.

#### D2: D2 fetch+sort — non-cursor (query.c:18717)

Same pattern as D1. The non-cursor D2 uses the same `read_record_ref` per-record loop. Replace with `slotcask_bulk_resolve_and_fetch` with a heap-push callback.

#### D3: Keyset emit (query.c:15772, 16036, 16132)

Three format-specific functions (`emit_keyset_default_fmt`, `emit_keyset_csv_fmt`, `emit_keyset_rows_fmt`). All iterate a keyset and call `read_record_ref` per hash.

Change: collect all keyset hashes → `slotcask_bulk_resolve_and_fetch` with format-specific callback.

```c
// Each emit function gets its own callback:
static int keyset_emit_default_cb(const uint8_t *hash16, const uint8_t *key,
                                   uint16_t klen, const uint8_t *value,
                                   uint32_t vlen, void *ctx) {
    // original inline logic: is_excluded check, criteria_match, emit
}

// Call site:
slotcask_bulk_resolve_and_fetch(sdb, hashes, hash_count, &emit_ctx, keyset_emit_default_cb);
```

### Group E — currently use `shard_find_worker` (old shard file path)

#### E1: `process_batch` (query.c:10820)

Currently:
1. Groups `CollectedHash` by shard_id into `ShardWorkCtx` workers
2. Calls `shard_find_worker` for each (sequential or `parallel_for`)
3. `shard_find_worker` opens `data/NNN.bin`, linear-probes slot headers

Change: replace the entire `process_batch` / `shard_find_worker` machinery with `slotcask_bulk_resolve_and_fetch`.

```c
// Before:
int nshard_groups = shard_group_batch(batch, batch_count, ...);
ShardWorkCtx *workers = calloc(nshard_groups, sizeof(ShardWorkCtx));
// fill workers...
if (batch_count < 1024 || nshard_groups <= 2) {
    for (...) shard_find_worker(&workers[g]);
} else {
    parallel_for(shard_find_worker, workers, nshard_groups, ...);
}
// iterate results...

// After:
// Extract flat hash array from batch:
uint8_t (*hashes)[16] = malloc(batch_count * sizeof(*hashes));
for (int i = 0; i < batch_count; i++)
    memcpy(hashes[i], batch[i].hash, 16);

// One call:
ProcessBatchCtx ctx = { ... };
slotcask_bulk_resolve_and_fetch(sdb, hashes, batch_count, &ctx, process_batch_cb);
free(hashes);

// Callback handles criteria_match_tree + joins + emit (same logic as shard_find_worker)
```

The callback wraps the match/join/emit logic from `shard_find_worker`. This is the largest single change (~120 lines of callback logic from the old worker).

**Delete after:** `shard_find_worker`, `ShardWorkCtx`, `shard_group_batch` (check if other callers remain).

#### E2: `search_shard_worker` (query.c:799)

Same pattern as E1 — old shard file path for CLI `search`. Replace with `slotcask_bulk_resolve_and_fetch`.

### Group F: Btree walk callbacks (deferred)

These receive hashes one-at-a-time from btree/composite/stream walks:
- `cursor_find_cb` (query.c:17057)
- `composite_prefix_cb` (query.c:11271)
- Stream callbacks (query.c:11095, 11828)
- Aggregate walk callback (query.c:17465)

Keep `read_record_ref` for these. The btree walk is bounded by the scan itself, not the per-record fetch. Future optimization: buffer N hashes, flush as batch via `resolve_and_fetch`, repeat.

### Group G: Single-record (keep)

- `cmd_get` (storage.c:987) — single get by key
- Join lookup (query.c:9238) — single join fetch
- CLI search single (query.c:22659)

These fetch one record. Keep `read_record_ref` — not worth batching.

## What to Delete

After all conversions:
- `shard_find_worker` — replaced by `resolve_and_fetch` + callback
- `search_shard_worker` — replaced
- `ShardWorkCtx`, `SearchShardWork` — unused
- `slotcask_bulk_lookup_by_hash` — replaced by `resolve_and_fetch`
- `slotcask_bulk_get_in_kfshard` — replaced by `resolve_and_fetch` with key-hash extraction
- Dead code after `return` in:
  - `read_record_ref` (lines 461-484)
  - `cmd_exists` (lines 6962-6989)
  - `cmd_delete` (lines 5140-5169)

**Keep:**
- `read_record_ref` — for single-record callers (get, join, cli)
- `slotcask_bulk_lookup_in_kfshard` — KF-only existence check, still used by `multi_exists_shard_worker`
- `slotcask_get` — for single `cmd_get`

## Execution Tasks (ordered)

### Task 1: New types + functions in slotcask

**Files:** `src/db/slotcask.h`, `src/db/slotcask.c`

1. Add `SlotcaskResolvedRec` type to `slotcask.h`
2. Implement `slotcask_bulk_resolve_hashes`:
   - Bucket hashes by KF shard (one pass)
   - For each KF shard: kfcache_acquire, probe all its hashes, capture (sid, fid, off), release
   - Return flat array
3. Implement `slotcask_bulk_fetch_resolved`:
   - Sort by (sid, fid), group runs, count unique files
   - Create `SegFetchArg[]` with per-file segments
   - If nfiles < 4: sequential (avoid thread overhead)
   - Else: `parallel_for_io` across segment files
4. Implement `slotcask_bulk_resolve_and_fetch` (wrapper calling 2 + 3)
5. Implement `seg_fetch_worker` (internal parallel_for_io worker)

**Verify:** Builds cleanly, no new warnings.

### Task 2: Convert shard_count_worker — bitmap path (B1)

**File:** `src/db/query.c`

1. In the KF probe loop (line 10612-10625), capture `e->stream_id`, `e->file_id`, `e->offset` into local vars
2. Replace `fetch_hashes[]` array with `SlotcaskResolvedRec resolved[]` array
3. After loop, call `slotcask_bulk_fetch_resolved` instead of `slotcask_bulk_lookup_by_hash`
4. Remove `shard_id` recomputation (line 10680-10681) — no longer needed

**Key benefit:** Eliminates KF re-probe for the bitmap path.

### Task 3: Convert shard_count_worker — non-bitmap path (A2)

**File:** `src/db/query.c`

1. Simplify `fetch_hashes` collection to directly call `slotcask_bulk_resolve_and_fetch`
2. Remove shard_id grouping — flat hash array
3. Same callback `count_batch_cb`

### Task 4: Convert shard_agg_worker (A4)

**File:** `src/db/query.c`

1. Replace `slotcask_bulk_lookup_by_hash` call with `slotcask_bulk_resolve_and_fetch`
2. Verify callback signature matches

### Task 5: Convert cursor_fetch_worker (A1)

**File:** `src/db/query.c`

1. Extract flat hash array from `ctx->entries`
2. Call `slotcask_bulk_resolve_and_fetch` with `cursor_fetch_cb`
3. Remove `shard_id` pre-computation

### Task 6: Convert multi_get_shard_worker (C1)

**File:** `src/db/storage.c`

1. Collect pre-computed hashes from entries
2. Add callback that decodes value + stores result_json
3. Remove `slotcask_bulk_get_in_kfshard` call

### Task 7: Convert D2 fetch+sort (D1, D2)

**File:** `src/db/query.c`

1. Collect all keyset hashes into flat array
2. Call `slotcask_bulk_resolve_and_fetch` with callback that does criteria_match_tree + sort key extraction + heap push
3. After batch fetch completes, emit from heap (existing code unchanged)

### Task 8: Convert keyset emit (D3)

**File:** `src/db/query.c`

1. For each of the 3 emit functions: collect hashes, call `resolve_and_fetch`
2. Wrap inline per-record logic (is_excluded, criteria_match, emit) into callback

### Task 9: Replace process_batch (E1)

**File:** `src/db/query.c`

1. Change `process_batch` to extract hashes from `batch[]` and call `resolve_and_fetch`
2. Create callback wrapping the match/join/emit logic from `shard_find_worker`
3. Delete `shard_find_worker`, `ShardWorkCtx`
4. Remove `shard_group_batch` if no other callers remain

### Task 10: Replace search_shard_worker (E2)

**File:** `src/db/query.c`

1. Change `parallel_fetch_and_print` to use `resolve_and_fetch`
2. Create callback for decode+emit
3. Delete `search_shard_worker`, `SearchShardWork`

### Task 11: Cleanup dead code

**File:** `src/db/query.c`

1. Remove dead code in `read_record_ref` (lines 461-484)
2. Remove dead code in `cmd_exists` (lines 6962-6989)
3. Remove dead code in cmd_delete (lines 5140-5169)

### Task 12: Remove old functions

**Files:** `src/db/slotcask.c`, `src/db/slotcask.h`

1. Delete `slotcask_bulk_lookup_by_hash` — no remaining callers
2. Delete `slotcask_bulk_get_in_kfshard` — no remaining callers
3. Remove declarations from header

### Task 13: Test + verify

1. Build: `SKIP_TESTS=1 ./build.sh`
2. Run full suite: `./build/bin/shard-db-test run-all` — all 4400+ tests pass
3. Benchmark: `./build/bin/shard-db-bench bench-queries` — verify no regression
4. Manual smoke test with 25M records, ordered find with 17k keyset — verify <100ms

## File Change Summary

| File | Additions | Deletions | Modifications |
|---|---|---|---|
| `src/db/slotcask.h` | 3 func decls + 1 type | 2 old func decls | — |
| `src/db/slotcask.c` | ~150 lines (resolve, fetch, seg_fetch_worker) | 2 old funcs (~200 lines) | — |
| `src/db/query.c` | Callbacks for D2/emit/batch/search | `shard_find_worker`, `search_shard_worker`, dead code | ~15 call sites |
| `src/db/storage.c` | Multi-get callback | `slotcask_bulk_get_in_kfshard` | `multi_get_shard_worker` |

## Risks

1. **Memory for resolved array.** 100K hashes → up to 100K resolved entries × 24 bytes = 2.4MB. Acceptable. For very large keysets (>1M), consider chunking or fallback.

2. **qsort over resolved entries.** 100K entries × 24 bytes, qsort with 3-byte key. ~1ms. Fine.

3. **`parallel_for_io` thread count.** 50-200 segment files → 50-200 pthreads. pthread_create is 10-30µs each, total ~1-5ms. Acceptable for I/O-bound work that takes 10s+ ms.

4. **Segcache stress.** More concurrent `segcache_acquire` calls. Segcache has per-entry rwlock; concurrent readers of different files don't contend. Same file from multiple KF shards is now one thread (grouped in phase 2).

5. **Callback refactoring for D2/keyset emit.** The inline per-record logic (criteria_match_tree, is_excluded, emit) needs to be extracted into callbacks. This is mechanical but must be correct — the callback receives different data layout (v2 slotcask format: `<key><value>` contiguous) vs what `read_record_ref` returned.

6. **Btree callbacks deferred.** If queries hitting the btree walk + per-record path remain slow after this change, the buffer+flush optimization is future work. For now, these are bounded by the walk, not the fetch — acceptable tradeoff.

## Performance Expectation

| Query | Before | After |
|---|---|---|
| Cursor C1, 17k keyset, ordered | ~16s (per-record) | ~50-100ms (batch resolve + parallel_for_io fetch) |
| Count, 17k keyset, bitmap filtered | ~5s (KF probe twice) | ~2s (KF probe once, skip re-probe) |
| D2 fetch+sort, 17k keyset | ~4.7s (per-record) | ~100-200ms (batch) |
| Multi-get, 10k keys | ~500ms (batched but sequential segments) | ~100-200ms (parallel_for_io across segments) |
| Process batch (non-cursor find), 17k keys | ~3s (old shard file) | ~100-200ms (v2 resolve+fetch) |
