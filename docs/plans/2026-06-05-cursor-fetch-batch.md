# Plan: Batched Fetch for Cursor C1 Path

**Date:** 2026-06-05  
**Status:** Draft  
**Priority:** High (performance)

## Problem

The cursor C1 fetch+sort path (lines 18028-18046 in query.c) performs **individual sequential lookups** for each hash in the keyset:

```c
for (size_t i = 0; i < n_pre; i++) {
    RecordRef rr;
    if (read_record_ref(db_root, object, &sch, sp_rows[i].hash, &rr) != 0) continue;
    // ... check criteria, extract sort key ...
    release_record_ref(&rr);
}
```

For 16k hashes, this results in:
- 16k individual `read_record_ref` calls
- Each call: kf cache acquire → linear probe → seg cache acquire → read → release both
- Random I/O pattern (each hash → different shard/segment)
- Cache thrashing (each lookup may evict previous entries)
- No batching or parallelism

**Observed performance:** 16.6 seconds for 16k records (1ms per lookup)

## Root Cause

The non-cursor find path already solves this correctly via `process_batch` (line 10755):
1. **Shard grouping** via `shard_group_batch` - groups hashes by shard_id
2. **Parallel execution** - for >= 1K entries, uses `parallel_for` with `shard_find_worker`
3. **Single mmap per shard** - `shard_find_worker` opens shard mmap ONCE, processes all hashes in that shard
4. **Sequential fallback** - for < 1K entries, still groups by shard (no thread overhead)

The cursor C1 path doesn't use this batching approach.

## Solution

Adapt the `process_batch` / `shard_find_worker` pattern for the C1 fetch+sort path:

### Phase 1: Convert KeySet to CollectedHash[]

Use existing `keyset_to_collected_hashes` (line 15512):
```c
CollectedHash *entries;
size_t entry_count;
if (keyset_to_collected_hashes(cursor_prefilter_ks, sch.splits, &entries, &entry_count) != 0) {
    // fallback to sequential
}
```

### Phase 2: Group by Shard

Use existing `shard_group_batch` (line 10764):
```c
int group_starts[1024], group_sizes[1024];
int nshard_groups = shard_group_batch(entries, entry_count, group_starts, group_sizes, 1024);
```

### Phase 3: Batched Fetch

Create a new worker function `cursor_fetch_worker` similar to `shard_find_worker` but:
- Uses v2 slotcask API (`slotcask_lookup_by_hash`) instead of v1 mmap
- Collects matching hashes + sort keys into a result array
- Does NOT emit JSON (that happens after sort)

```c
typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    CollectedHash *entries;
    int entry_count;
    CriteriaNode *tree;
    FieldSchema *fs;
    int order_field_idx;
    TypedSchema *ts;
    SmallPrefilterRow *results;  // output array
    int result_count;
    int result_cap;
    QueryDeadline *deadline;
} CursorFetchCtx;

static void *cursor_fetch_worker(void *arg) {
    CursorFetchCtx *ctx = (CursorFetchCtx *)arg;
    // Group entries by shard within this group
    // For each shard: open once, process all hashes, close
    // For each match: check criteria, extract sort key, append to results
}
```

### Phase 4: Parallel or Sequential Execution

```c
if (entry_count < 1024 || nshard_groups <= 2) {
    // Sequential (grouped by shard, no thread overhead)
    for (int g = 0; g < nshard_groups; g++) {
        CursorFetchCtx ctx = { ... };
        cursor_fetch_worker(&ctx);
        // Merge results into sp_rows
    }
} else {
    // Parallel across shard groups
    CursorFetchCtx *workers = calloc(nshard_groups, sizeof(CursorFetchCtx));
    // Initialize workers...
    parallel_for(cursor_fetch_worker, workers, nshard_groups, sizeof(CursorFetchCtx));
    // Merge results from all workers into sp_rows
    free(workers);
}
```

### Phase 5: Sort and Emit

After batched fetch, the existing sort + emit logic remains unchanged:
```c
qsort(sp_rows, n_kept, sizeof(SmallPrefilterRow), desc ? small_prefilter_cmp_desc : small_prefilter_cmp_asc);
// ... emit via cursor_find_cb ...
```

## Expected Performance

**Current:** 16k sequential lookups × ~1ms each = ~16 seconds

**With batching:**
- ~64 shard groups (for splits=4096)
- Parallel fetch across CPU cores
- Single mmap per shard (no cache thrashing)
- Expected: ~100-500ms (30-100× faster)

## Implementation Notes

1. **v2 slotcask API:** The cursor path uses v2 slotcask (`slotcask_lookup_by_hash`), not v1 mmap. The worker must use the v2 API.

2. **Result merging:** Multiple workers write to separate result arrays. Main thread merges into `sp_rows` after all workers complete.

3. **Memory management:** Each worker allocates its own result array. Main thread frees worker arrays after merging.

4. **Error handling:** If any worker fails (OOM, deadline), abort the entire batch and fall back to sequential or return error.

5. **Thread safety:** Workers read from shared `entries` array (read-only). Each worker writes to its own result array (no contention).

## Testing

1. **Correctness:** Run existing cursor tests to ensure results match sequential path
2. **Performance:** Benchmark with 16k keyset, measure fetch+filter time
3. **Edge cases:**
   - Empty keyset (n_pre=0)
   - Small keyset (n_pre < 1024, sequential path)
   - Large keyset (n_pre > 10k, parallel path)
   - Deadline timeout during fetch

## Files to Change

- `src/db/query.c`: Add `CursorFetchCtx` struct, `cursor_fetch_worker` function, replace sequential loop with batched fetch

## Risks

1. **Complexity:** Parallel fetch adds complexity. Must ensure thread safety and correct result merging.
2. **Memory:** Each worker allocates result array. For large keysets, this could use significant memory.
3. **Fallback:** If batched fetch fails, must fall back to sequential path gracefully.

## Rollback

If performance doesn't improve or correctness issues arise, revert to sequential path by removing the batched fetch code.
