# Audit: Reindex Builds Trigram Indexes Incompletely

**Date:** 2026-06-05  
**Auditor:** Workflow Leader  
**Status:** Root-cause identified — fix ready

## Executive Summary

**Root cause:** When reindex builds multiple indexes in one pass (n_fields > 1), the per-field memory budget is divided by n_fields, reducing the trigram buffer capacity (pairs_cap) below TG_MAX_DISTINCT (4096). Records with many distinct trigrams exceed this capacity and are **silently dropped** — no trigrams from those records are indexed.

**Symptom:** `count title icontains "the"` returns ~109 after reindex (should be ~millions). The add-index path works correctly because it builds one field at a time with a full budget.

**Timing tell:** add-index trigram = 53s vs full reindex (11 indexes) = 25s. The reindex is faster because it's skipping most of the trigram work!

---

## Code Path Analysis

### Reindex Path (build_indexes_streaming_multi)

1. **reindex_object** (index.c:3298) calls **build_indexes_streaming_multi** (index.c:3211)
2. **build_indexes_streaming_multi** calls **seg_seq_build_spills** (index.c:3220) with n_fields = total number of indexes
3. **seg_seq_build_spills** (index.c:3062) calculates per-field budget:

```c
// index.c:3085-3090
size_t budget = g_index_build_budget_bytes;  // default 64MB
if (budget < 64ULL * 1024 * 1024) budget = 64ULL * 1024 * 1024;
size_t per_worker_total = budget / (size_t)P / 2;  // P = worker count
if (per_worker_total < 8ULL * 1024 * 1024) per_worker_total = 8ULL * 1024 * 1024;
size_t per_field_budget = per_worker_total / (size_t)n_fields;  // ← DIVIDED BY n_fields
if (per_field_budget < 4ULL * 1024 * 1024) per_field_budget = 4ULL * 1024 * 1024;
```

4. **mf_worker_field_alloc** (index.c:2705) allocates buffers based on per_field_budget:

```c
// index.c:2717-2719
size_t cap = per_field_budget / (2 * sizeof(BtEntry) + est);
if (cap < 4096)    cap = 4096;      // ← MINIMUM CAP
if (cap > 2000000) cap = 2000000;
```

5. **seg_scan_worker** (index.c:2790) scans segments and calls **mf_append_field** (index.c:2868) for each record

6. **mf_append_field** (index.c:2626) handles trigrams:

```c
// index.c:2629-2651
if (d->type == STREAM_TRIGRAM) {
    // ... extract trigrams ...
    uint8_t tg[TG_MAX_DISTINCT][3];
    size_t n = tg_extract_distinct(vb + 2, al, tg, TG_MAX_DISTINCT);
    if (n == 0) return;
    
    // Flush if adding n trigrams would exceed capacity
    if (f->pair_count + n > f->pairs_cap || f->arena_used + n * 3 > f->arena_cap)
        mf_flush_field(f, splits, idx_n);
    
    // ← BUG: If n itself exceeds capacity, DROP ALL TRIGRAMS
    if (n > f->pairs_cap || n * 3 > f->arena_cap) return;
    
    // ... add trigrams to buffer ...
}
```

### Add-Index Path (build_trigram_pass)

1. **cmd_add_index** (index.c:1076) calls **build_trigram_pass** (index.c:1158)
2. **build_trigram_pass** (index.c:1816) calls **seg_seq_build_spills** (index.c:1856) with **n_fields = 1**
3. Same code path, but per_field_budget is **11× larger** (for 11 indexes)

---

## The Bug: Capacity Math

### Example: 11 indexes, 64MB budget, 8 workers

**Reindex (n_fields=11):**
```
per_worker_total = 64MB / 8 / 2 = 4MB
per_field_budget = 4MB / 11 = 372KB
cap = 372KB / (2 * 32 + 3) ≈ 5500
cap = max(5500, 4096) = 5500  // above minimum
```

But if budget is tighter or worker count is higher:
```
per_worker_total = 64MB / 16 / 2 = 2MB
per_field_budget = 2MB / 11 = 186KB
cap = 186KB / 67 ≈ 2800
cap = max(2800, 4096) = 4096  // hits minimum
```

**Add-index (n_fields=1):**
```
per_worker_total = 64MB / 8 / 2 = 4MB
per_field_budget = 4MB / 1 = 4MB
cap = 4MB / 67 ≈ 62,000
cap = max(62000, 4096) = 62000  // well above TG_MAX_DISTINCT
```

### The Failure Condition

A record with n distinct trigrams is dropped if:
```c
n > f->pairs_cap
```

With pairs_cap = 4096 (the minimum) and TG_MAX_DISTINCT = 4096, records with 4096 distinct trigrams hit the boundary. Records with long titles (e.g., 1000+ character varchar fields) can easily have 4000+ distinct trigrams.

**When dropped:** The check at line 2641 returns without indexing ANY trigrams from that record. This is silent — no error, no warning, no partial indexing.

---

## Why Add-Index Works

Add-index calls seg_seq_build_spills with n_fields=1, so:
- per_field_budget is 11× larger
- pairs_cap is 11× larger (e.g., 62,000 vs 5,500)
- No record can have more than 4096 trigrams (TG_MAX_DISTINCT limit)
- The check `n > f->pairs_cap` is never true
- All trigrams are indexed

---

## Verification

### Expected Behavior

After reindex with 11 indexes:
- pairs_cap ≈ 4096–5500 (depending on worker count)
- Records with >4096 distinct trigrams are dropped
- Only records with short titles (few trigrams) are indexed
- `count title icontains "the"` returns ~109 (only short titles with "the")

After add-index trigram:
- pairs_cap ≈ 62,000
- All records indexed (max 4096 trigrams per record)
- `count title icontains "the"` returns ~millions (correct)

### How to Verify

1. **Check pairs_cap during reindex:**
   ```c
   // Add debug logging to mf_worker_field_alloc (index.c:2720)
   fprintf(stderr, "mf_worker_field_alloc: type=%d, per_field_budget=%zu, pairs_cap=%zu\n",
           d->type, per_field_budget, cap);
   ```

2. **Count dropped records:**
   ```c
   // Add debug logging to mf_append_field (index.c:2641)
   if (n > f->pairs_cap || n * 3 > f->arena_cap) {
       fprintf(stderr, "DROPPED: n=%zu, pairs_cap=%zu, arena_cap=%zu\n",
               n, f->pairs_cap, f->arena_cap);
       return;
   }
   ```

3. **Compare trigram counts:**
   ```bash
   # After reindex
   ./shard-db count hn stories '{"title":{"icontains":"the"}}'
   # Expected: ~109 (buggy)
   
   # After remove-index + add-index
   ./shard-db remove-index hn stories title
   ./shard-db add-index hn stories title:trigram -f
   ./shard-db count hn stories '{"title":{"icontains":"the"}}'
   # Expected: ~millions (correct)
   ```

---

## Fix

### Option 1: Raise Minimum Cap (Simple)

Change the minimum cap at index.c:2718 from 4096 to 8192 or 16384:

```c
if (cap < 8192)    cap = 8192;   // was 4096
```

**Pros:** Simple, one-line change  
**Cons:** Increases memory usage for all builds; doesn't fix the root cause (records with >8192 trigrams still dropped)

### Option 2: Batch Large Trigram Sets (Correct)

Modify mf_append_field to handle records with more trigrams than pairs_cap by processing in batches:

```c
// index.c:2629-2651 (modified)
if (d->type == STREAM_TRIGRAM) {
    int tidx = d->field_indices[0];
    const TypedField *tf = &ts->fields[tidx];
    if (tf->type != FT_VARCHAR) return;
    const uint8_t *vb = value + tf->offset;
    uint16_t al = ((uint16_t)vb[0] << 8) | (uint16_t)vb[1];
    if (al == 0) return;
    uint8_t tg[TG_MAX_DISTINCT][3];
    size_t n = tg_extract_distinct(vb + 2, al, tg, TG_MAX_DISTINCT);
    if (n == 0) return;
    
    // Process trigrams in batches to handle n > pairs_cap
    size_t i = 0;
    while (i < n) {
        // Flush if buffer is full
        if (f->pair_count >= f->pairs_cap || f->arena_used + 3 > f->arena_cap) {
            mf_flush_field(f, splits, idx_n);
        }
        
        // Add as many trigrams as will fit
        size_t batch = 0;
        while (i + batch < n &&
               f->pair_count + batch + 1 <= f->pairs_cap &&
               f->arena_used + (batch + 1) * 3 <= f->arena_cap) {
            size_t off = f->arena_used;
            memcpy(f->arena + off, tg[i + batch], 3);
            f->arena_used += 3;
            f->pairs[f->pair_count + batch].value = (const char *)(uintptr_t)off;
            f->pairs[f->pair_count + batch].vlen  = 3;
            memcpy(f->pairs[f->pair_count + batch].hash, hash16, BT_HASH_SIZE);
            batch++;
        }
        f->pair_count += batch;
        i += batch;
    }
    return;
}
```

**Pros:** Correctly handles all records regardless of trigram count  
**Cons:** More complex, slightly slower (more flushes)

### Option 3: Dynamic Cap Based on Index Type (Balanced)

Set minimum cap based on index type:

```c
// index.c:2718 (modified)
size_t min_cap = (d->type == STREAM_TRIGRAM) ? (TG_MAX_DISTINCT + 1024) : 4096;
if (cap < min_cap) cap = min_cap;
```

**Pros:** Ensures trigram builds always have enough capacity; doesn't affect btree builds  
**Cons:** Increases memory for trigram builds; doesn't handle pathological cases (>5120 trigrams)

### Recommended Fix

**Option 2 (Batch Large Trigram Sets)** is the correct fix. It handles all cases and doesn't waste memory.

---

## Other Index Types

### Btree Indexes

The btree path (STREAM_BTREE) at index.c:2654-2686 has a similar check at line 2678:

```c
if (kl > f->arena_cap || 1 > f->pairs_cap) return;
```

This checks if a single key (kl bytes) exceeds arena_cap. Since:
- arena_cap is at least 65536 bytes (line 2722)
- btree keys are typically <1000 bytes
- pairs_cap is at least 4096

The check `1 > f->pairs_cap` is never true, and `kl > f->arena_cap` is very unlikely. **Btree indexes are not affected.**

### Bitmap Indexes

Bitmap indexes use a different path (mf_bitmap_spill_append) and don't have the same capacity constraints. **Bitmap indexes are not affected.**

---

## Test Plan

Add a test that:
1. Creates an object with a varchar field
2. Inserts records with long titles (1000+ characters, >4000 distinct trigrams)
3. Runs reindex
4. Asserts `count field icontains "common_trigram"` matches the add-index result

Example test case:
```c
// src/test/cases/test_reindex_trigram_completeness.c
TEST_REGISTER(reindex_trigram_completeness);

static void test_reindex_trigram_completeness(void) {
    // Setup: create object with varchar field
    // Insert 100 records with 1000-char titles (each has ~4000 distinct trigrams)
    // Run reindex
    // Count records with icontains "the" (should be ~100)
    // Remove trigram index, add it back
    // Count again (should still be ~100)
    // Assert counts match
}
```

---

## Summary

| Aspect | Reindex (n_fields=11) | Add-Index (n_fields=1) |
|--------|----------------------|------------------------|
| per_field_budget | ~372KB | ~4MB |
| pairs_cap | ~5500 (or 4096 min) | ~62,000 |
| Records with >4096 trigrams | **DROPPED** | Indexed |
| `count icontains "the"` | ~109 (buggy) | ~millions (correct) |
| Time | 25s (skips most work) | 53s (full build) |

**Root cause:** Per-field budget division reduces pairs_cap below TG_MAX_DISTINCT, causing records with many trigrams to be silently dropped.

**Fix:** Batch trigram processing in mf_append_field to handle n > pairs_cap.

---

**End of Audit**
