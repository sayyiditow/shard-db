# Plan: Cursor Sparsity Heuristic for Sparse Bitmap Prefilters

**Date:** 2026-06-05  
**Status:** Implementation in progress  
**Branch:** `fix/cursor-sparsity-heuristic`

## Problem

Production queries like `type=job ORDER BY score DESC limit 25 cursor:null` take 20+ seconds because:

1. The planner picks `type+score` composite (bitmap seed, K=17k)
2. `prefer_fetch_sort(17k, 5.6M, 0, 25)` returns false (17k² > 25 × 5.6M)
3. Walk path scans far through sparse job entries → 20s

The formula `K² < want × N` assumes uniform distribution of prefilter entries in the order-by index. For sparse bitmap seeds (e.g., jobs are rare in score order), this assumption fails — the walk scans far before finding matches.

## Root Cause Analysis

**The formula:** `K² < want × N`

This is the crossover where:
- Fetch+sort cost ≈ K (fetch all K candidates, filter, sort)
- Walk cost ≈ `want × N / K` (walk order-by index, assuming uniform distribution)

They cross at `K ≈ sqrt(want × N)`.

**Why it fails for sparse bitmaps:**

The walk cost formula assumes prefilter entries are uniformly distributed. But sparse bitmap seeds are often clustered (e.g., jobs have low scores, not spread across the score index). The actual walk cost is much higher: `want × N² / K²` (the sparsity factor is `N/K`).

**Revised crossover for sparse bitmaps:**
```
K = want × N² / K²
K³ = want × N²
```

## Solution

Replace the 5% heuristic with a mathematically correct formula for bitmap seeds:

**For bitmap seeds:** `K³ < want × N²`

This accounts for sparsity without arbitrary thresholds. The formula automatically adjusts based on N:
- N=5.6M: threshold ≈ 92k (K = (25 × 5.6M²)^(1/3))
- N=100M: threshold ≈ 630k (K = (25 × 100M²)^(1/3))

**For non-bitmap seeds:** Keep the original `K² < want × N` formula (uniform distribution assumption holds).

## Implementation

### Step 1: Update `prefer_fetch_sort` signature and logic

**File:** `src/db/query.c`  
**Location:** Line 13276

**Current (5% heuristic):**
```c
static int prefer_fetch_sort(size_t candidates, size_t N, int offset, int limit,
                             int is_bitmap_seed) {
    if (candidates == 0) return 1;
    size_t want = (size_t)((offset > 0 ? offset : 0) + (limit > 0 ? limit : 1));
    
    /* Sparsity override: when a bitmap seed is a small fraction of N,
       the uniform distribution assumption fails (prefilter entries are
       clustered, not spread). Fetch+sort beats walk even when the
       formula says otherwise. Threshold: K < 5% of N. */
    if (is_bitmap_seed && candidates < N / 20) {
        return 1;  // fetch+sort
    }
    
    /* Cost model: K² < want × N (crossover where fetch+sort beats walk).
       Assumes uniform distribution — valid for non-bitmap seeds and
       large bitmap seeds (> 5% of N). */
    return candidates * candidates < want * N;
}
```

**Replace with (K³ formula):**
```c
static int prefer_fetch_sort(size_t candidates, size_t N, int offset, int limit,
                             int is_bitmap_seed) {
    if (candidates == 0) return 1;
    size_t want = (size_t)((offset > 0 ? offset : 0) + (limit > 0 ? limit : 1));
    
    /* For bitmap seeds, the uniform distribution assumption often fails
       (sparse bitmaps are clustered, not spread). The walk cost scales as
       want × N² / K² instead of want × N / K. Crossover: K³ < want × N².
       Use __int128 to avoid overflow for large N (up to 4 billion). */
    if (is_bitmap_seed) {
        unsigned __int128 k3 = (unsigned __int128)candidates * candidates * candidates;
        unsigned __int128 want_n2 = (unsigned __int128)want * N * N;
        if (k3 < want_n2) return 1;
    }
    
    /* Cost model: K² < want × N (crossover where fetch+sort beats walk).
       Assumes uniform distribution — valid for non-bitmap seeds and
       large bitmap seeds where sparsity is less pronounced. */
    return candidates * candidates < want * N;
}
```

### Step 2: Update test to match K³ formula

**File:** `src/test/cases/test_cursor_sparse_prefetch.c`

**Current test (5% heuristic):**
- N=20000, K=800 (4% of N)
- K² = 640k > want×N = 500k → walk (formula)
- K < N/20 = 1000 → fetch+sort (5% override)

**New test (K³ formula):**
- N=20000, K=800
- K³ = 512M
- want×N² = 25 × 400M = 10B
- 512M < 10B → **fetch+sort** ✓

Update test comments to reflect K³ formula instead of 5% heuristic.

### Step 3: Verify all call sites

The signature changes are already in place from the 5% implementation:
- `prefer_fetch_sort` has `is_bitmap_seed` parameter ✓
- `pick_sort_or_walk` has `is_bitmap_seed` parameter ✓
- All call sites pass the bitmap flag ✓

No additional changes needed — only the formula inside `prefer_fetch_sort` changes.

## Acceptance Tests

1. **Sparse bitmap (K=800, N=20000):** K³ = 512M < want×N² = 10B → fetch+sort ✓
2. **Broad bitmap (K=19200, N=20000):** K³ = 7.1T > want×N² = 10B → walk ✓
3. **Production query (K=17k, N=5.6M):** K³ = 4.9T < want×N² = 780T → fetch+sort ✓
4. **Large sparse (K=100k, N=100M):** K³ = 1P < want×N² = 250P → fetch+sort ✓
5. **Very large (K=1M, N=100M):** K³ = 1E > want×N² = 250P → walk ✓

All existing tests must pass (no regression).

## Risks

1. **Overflow:** K³ for K=1M = 10¹⁸, fits in uint64. For K=10M = 10²¹, needs __int128. Using __int128 is safe.
2. **Performance:** __int128 arithmetic is slightly slower than uint64, but this is a one-time decision per query, not in the hot path.
3. **Correctness:** The K³ formula is derived from the sparsity cost model. If the model is wrong, the formula is wrong. Monitor production queries to validate.

## Future Work

- Add histograms to track actual value distribution and correlation between columns
- Use statistics to refine the sparsity factor (currently assumes worst-case N/K)
- Consider per-query hints (e.g., `prefer_fetch_sort: true` in JSON)

---

**End of Plan**
