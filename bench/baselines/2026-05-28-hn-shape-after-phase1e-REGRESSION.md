# HN-shape — after Phase 1e (REGRESSION discovered) — 2026-05-28

Captured via `bench/measure-shapes.sh` immediately after Phase 1e deploy
(src `2440c88`). **Two catastrophic regressions and pollution ratio
got worse instead of better.** Saved here as the empirical evidence
driving the regression hunt.

## Per-shape cost (cold = after drop_caches, warm = min of 3)

| Shape | After 1c (baseline) | **After 1e** | Δ warm | Severity |
|---|---:|---:|---:|---|
| `title starts "Show HN"` (count) | 19164ms | **30184ms** | +57% | regressed |
| **`by=aikah` + `order_by time desc` (profile find)** | **27ms** | **24137ms** | **+89000×** | **CATASTROPHIC** |
| `by=aikah` no order_by | 10ms | 11ms | flat | ok |
| `type=story` group_by author (aggregate) | 855ms | 820ms | −4% | ok |
| comment-tree `story_root` (find limit 500) | 6ms | 7ms | flat | ok |

## Pollution probe — got WORSE not better

| | After 1c | After 1e |
|---|---:|---:|
| agg warm | 938ms | 848ms |
| full scan | 20231ms | 13237ms |
| agg after scan | 5740ms | **6079ms** |
| **ratio (pollution impact)** | **6.1×** | **7.2×** ← higher |

## Hypotheses to investigate

1. **D1 (composite-prefix scan) regression — the most damning.** Warm ≈ cold (24s
   warm, 42s cold), so cache isn't helping at all on what should be a
   `limit=25` O(limit) walk. Signature screams "the composite walk is going
   through O_DIRECT" or "the composite walk no longer fires and something
   else is full-scanning." `find_via_composite_prefix` uses
   `btree_idx_walk_ordered`, not `btree_idx_range` or `scan_dispatch` — so
   in theory 1e.4 shouldn't touch it. But the empirical result says otherwise.
   Could also be 1d.2's small-prefilter shortcut or n_kept tracking inadvertently
   intercepting the composite path. Investigate cmd_find diff 0931482→2440c88
   for changes touching D1 / FP_ORDER_COMPOSITE routing.
2. **A3 trigram-prefix slower than full scan (30s vs 19s).** Either (a) the
   "sho" gram is too common (close to N total titles) so A3 walks ≈ full
   posting + verifies per record, exceeding the data-scan cost; OR (b) O_DIRECT
   on btree-leaf scans is genuinely slower than mmap at this scale (the 4MB
   double-buffer overhead isn't offsetting the cache-fill avoidance). The fact
   that pollution probe got WORSE suggests (b) — O_DIRECT isn't actually
   bypassing the cache the way intended.
3. **Pollution ratio worsening directly contradicts the O_DIRECT thesis.** If
   the O_DIRECT path were working as designed, the agg-after-scan should not
   pay the eviction penalty. The fact that it got SLIGHTLY worse means O_DIRECT
   isn't being applied correctly, OR mmap is still in the path somewhere
   (e.g., the slotcask layer's metadata reads).

## Action

Bisect to identify which commits regressed which shape, then revert or fix:
- Pull main locally at the commits 0931482 (post-1c, last known good D1), 5c69ecd
  (post-1d), 2440c88 (post-1e, current).
- Run a local micro-bench reproducing the D1 shape on a small composite
  fixture; compare timings across the three checkouts.
- If the regression is in 1d, the find-with-total integration broke the
  composite executor's hot path.
- If it's in 1e, the O_DIRECT migration touched D1 indirectly (perhaps via
  a shared helper) OR the O_DIRECT path itself isn't a perf win at this
  workload and needs a fallback heuristic.

Phase 1 of the planner rewrite is NOT shippable as-is. The 1c profile win
(27ms) must be restored before any further work.
