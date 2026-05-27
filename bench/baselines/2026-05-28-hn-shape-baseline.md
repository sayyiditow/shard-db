# HN-shape baseline — 2026-05-28 (live Netcup box, src e1b375b)

Captured via `bench/measure-shapes.sh` (drops page cache between shapes →
isolated, un-confounded). stories 5.6M, comments 38.5M, 15 GiB RAM box.
This is the "before" the planner rewrite is measured against.

## Isolated per-shape cost (cold = after drop_caches, warm = min of 3)

| Shape | cold | warm | verdict |
|---|---|---|---|
| `title starts "Show HN"` (count) | 20866ms | **19484ms** | intrinsically slow — full scan (title trigram-only, no btree). THE root. |
| `by=aikah` + `order_by time desc` (profile find) | 17126ms | **2338ms** | real bug — order_by-time walk picks time index over selective `by` |
| `by=aikah` no order_by (baseline) | 31ms | **7ms** | fast — selective `by` lookup; adding order_by = 334× |
| `type=story` group_by author (aggregate) | 6239ms | **896ms** | bitmap-postfilter fix (PR #89) confirmed working |
| comment-tree `story_root` (find limit 500) | 369ms | **9ms** | fast (composite/story_root); was a pure pollution victim in the logs |

## Pollution probe (does a full scan evict a warm shape?)

```
agg warm:        937ms
full scan:       20538ms
agg after scan:  6432ms     -> 6.9x slowdown from one scan
```

## Takeaways (drives rewrite priority)

1. **title-prefix scan** is intrinsically slow (19.5s *warm*) AND is the pollution
   source (6.9x). Fixing it (trigram-prefix / btree on title) is doubly valuable.
2. **profile order_by** is a real planner bug (2.3s warm; 7ms without order_by) —
   the find+order_by-selective regression; fix = `by+time` composite-prefix scan.
3. `type=story` agg (896ms) and comment-tree (9ms) are FAST in isolation — their
   multi-second slow-log entries were cache-pollution collateral, not bugs.
