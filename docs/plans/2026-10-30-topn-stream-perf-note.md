# Note — restore streaming top-N speed after R3 (deferred)

**When:** after the type-correctness branch merges (same backlog slot as the
buffered-fetch slowness). Do not do this inside R3–R6.

## Why it is slow now
R3 decision D7: `agg_run_topn_stream` streams only when every aggregate spec is
`count(*)`. Any top-N with `sum/avg/min/max` or `count(field)` returns `-2` and
takes the hash-aggregate path (all groups materialised, then sorted). The
streaming heap (`topn_heap_*`, `double` metric) cannot hold exact values and
cannot count per-field contributions. Correct, but slower and more
`QUERY_BUFFER_MB` pressure on high-cardinality groups.

## Options (not measured; benches are the human's)
1. **Two-phase (preferred).** When the `order_by` metric is `count(*)`: stream
   to pick the top-N groups by count; then compute the remaining aggregates
   exactly (`agg_exact` states) only for those N groups in a second restricted
   pass. Cost O(N) exact work + one extra indexed read.
   - Must match the hash path's tie order at the Nth boundary exactly
     (deterministic tie-break on group label).
   - Tests: ties at the boundary, NULL group, packed numeric labels, workers
     1..N, asc/desc, offset+limit.
2. **Exact heap metric.** Make the heap metric a tagged `__int128`/`dec_t`
   value so `sum/avg/min/max` can order the heap. Changes the public
   `topn_heap_*` API and `test_agg_topn_stream.c`; largest surface.

## Acceptance
- Output byte-identical to the hash path for every case above (differential
  test: force-decline vs stream).
- Benchmark before/after run by the human; no correctness regression in
  `test-agg-topn-stream`, `test-agg-exact-*`, `test-agg-packed-*`.
- Normal full suite; ASan/UBSan + TSan gate per AGENTS.md (touches heap
  ownership and worker merges).
