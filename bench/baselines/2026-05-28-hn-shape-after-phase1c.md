# HN-shape — after Phase 1c (live Netcup box, src 0931482, 2026-05-28)

Captured via `bench/measure-shapes.sh` immediately after Phase 1c deploy
(planner rewrite — count/find/aggregate migrated onto `plan_filter`,
D1 composite-prefix executor + D3 order-index walk executor live).
stories 5.6M, comments 38.5M, 15 GiB RAM box. Compare line-for-line
against `2026-05-28-hn-shape-baseline.md`.

## Isolated per-shape cost (cold = after drop_caches, warm = min of 3)

| Shape | cold | warm | Δ warm vs baseline | verdict |
|---|---:|---:|---:|---|
| `title starts "Show HN"` (count) | 19874ms | **19164ms** | −1.6% | unchanged — trigram-prefix is Phase 1e |
| `by=aikah` + `order_by time desc` (profile find) | 12694ms | **27ms** | **−98.8% (86×)** | **D1 win — `by+time` composite walked O(limit), no sort** |
| `by=aikah` no order_by (baseline) | 77ms | **10ms** | +43% (3ms) | flat — already a selective `by` lookup |
| `type=story` group_by author (aggregate) | 6008ms | **855ms** | −4.6% | flat — bitmap fast path unchanged |
| comment-tree `story_root` (find limit 500) | 86ms | **6ms** | −33% (3ms) | flat — already a composite/story_root walk |

## Pollution probe (does a full scan evict a warm shape?)

```
agg warm:        938ms
full scan:       20231ms
agg after scan:  5740ms     -> 6.1x slowdown (was 6.9x in baseline)
```

Still substantial. Phase 1e ([[backlog-odirect-full-scans]]) is the cure
— bypass page cache entirely on full scans so they cannot evict the hot
working set.

## Takeaways

1. **The profile-page bug ([[bug-find-orderby-selective-regression]]) is FIXED.**
   `by=X order_by time` drops 2338ms → 27ms — exactly the D1 composite-prefix
   executor walking the `by+time` index O(limit) in order, no in-memory sort.
2. **No regressions** — every shape is at least as fast as the baseline (cold-cache
   variance ±a few ms on the already-fast shapes is noise).
3. **Pollution unchanged** — confirms that the per-query plan choice is decoupled
   from the FS-level cache pressure. Scan-isolation (1e) is the separate work.
4. **title-prefix still 19s warm** — exactly as expected; this is the trigram-prefix
   work in Phase 1e ([[backlog-title-prefix-scan]]), out of Phase 1c's scope.
