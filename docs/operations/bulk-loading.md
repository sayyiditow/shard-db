# Bulk Loading

Two patterns for getting records into shard-db at scale:

1. **Pre-existing-indexes** — declare every index in `create-object`, then bulk-insert. Best for steady-state and small/medium loads.
2. **Load-then-index** — create the object with `"indexes":[]`, bulk-insert, then issue `add-index` per field after. Best for large one-time seeds and backfills.

The right choice depends on how many bulk-insert *calls* you'll make, not on total record count.

## The crossover rule

Let `R` = number of bulk-insert calls (chunks). The recommended chunk size is `~N / 200_000` rows per call, so for `N` total records, `R ≈ N / 200_000`:

| `R` (chunks of ~200 K) | `N` (total rows) | Recommended pattern |
|---|---|---|
| `R ≤ ~10` | up to ~2 M | **pre-existing-indexes parallel** wins |
| `R ≥ ~20` | ~4 M and up | **load-then-index** wins |
| in between | re-bench at your scale |

Why: every bulk-insert with pre-existing indexes triggers a per-(field, shard) merge cycle. Cumulative merge work scales `O(R²)` in chunk count. Beyond ~20 chunks the merges dominate, and one `add-index` from scratch is cheaper than R merges in.

At 25 M / 1 M chunks (`R = 25`) → solidly in the load-then-index zone. At 1 M / 200 K chunks (`R = 5`) → pre-existing wins.

## Pattern 1: pre-existing-indexes (steady-state)

For day-to-day inserts (the typical production workload) and small one-time loads up to ~2 M:

```bash
# Schema with indexes declared upfront
./shard-db query '{"mode":"create-object","dir":"app","object":"users",
  "splits":256,"max_key":32,
  "fields":["email:varchar:100","name:varchar:64","age:int",
            "active:bool","created_at:datetime"],
  "indexes":["email","age","active","created_at"]}'

# Insert in chunks of ~200 K each
for chunk in chunk_*.json; do
  ./shard-db bulk-insert app users "$chunk"
done
```

Each chunk pays the cost of updating all four indexes. At small `R` this is cheaper than rebuilding from scratch.

For maximum single-call throughput at this scale, use **parallel connections** (5 conns × 200 K per call). See `bench/bench_parallel.c` for the harness.

## Pattern 2: load-then-index (initial seed, large backfills)

For initial seeds at `N` ≥ 4 M and tenant-onboarding backfills:

```bash
# Step 1 — schema with NO indexes
./shard-db query '{"mode":"create-object","dir":"app","object":"users",
  "splits":256,"max_key":32,
  "fields":["email:varchar:100","name:varchar:64","age:int",
            "active:bool","created_at:datetime"],
  "indexes":[]}'

# Step 2 — bulk-insert ALL chunks. Index merge cost is now zero.
for chunk in chunk_*.json; do
  ./shard-db bulk-insert app users "$chunk"
done

# Step 3 — build all indexes in ONE pass. Passing "fields":[...] (plural)
# triggers cmd_add_indexes which scans storage once and accumulates
# entries for every index simultaneously. Per-field add-index would
# do N full scans over the same records — wasteful.
./shard-db query '{"mode":"add-index","dir":"app","object":"users",
  "fields":["email","age","active","created_at"]}'
```

Total wall time at 25 M / 1 M chunks / 12 indexes is roughly:

```
insert (no indexes) + add-indexes (single multi-field pass)
≈ 60–90 s + 60–120 s
≈ ~120–210 s   (vs ~5–7 min with pre-existing indexes at the same scale)
```

`add-index` (singular or plural) is durable when it returns — the indexes are fully built and queryable. If you crash mid-seed, the already-inserted data is intact; restart and resume the chunks that hadn't run.

**Don't** loop `add-index` per field as a substitute for `fields:[...]` — that runs N full scans (N × storage walk) instead of one. The plural form is the operational primitive; the singular form is a convenience for adding one new index to an already-populated table.

## Subsequent inserts as data grows

Once the seed is loaded, **never drop indexes for incremental inserts**. The daily delta has small `R` per batch and pre-existing-indexes wins for those. The drop/rebuild dance is only for initial-seed amplitude.

## Verifying the crossover on your hardware

The crossover thresholds are empirical and depend on:
- Disk class (NVMe vs SATA SSD vs HDD)
- Page cache size vs total dataset
- Number of indexes
- Field types (varchar with high cardinality is the heaviest)

Re-bench at your real scale before committing to a pattern. Two switches are wired into the bench harness:

```bash
# Pre-existing-indexes baseline (default)
SHARD_BENCH_COUNT=25000000 \
  ./build/bin/shard-db-bench run bench-queries

# Load-then-index — same data, different pattern, reports per-field
# add-index times so you can spot the heaviest fields.
SHARD_BENCH_COUNT=25000000 \
  SHARD_BENCH_DROP_INDEXES_FIRST=1 \
  ./build/bin/shard-db-bench run bench-queries
```

The harness prints both insert and add-index totals; compare the two runs at your scale.

For a finer-grained A/B at smaller `R` (1 M / invoice schema), `bench-parallel` already exercises both paths with five named test cases.

## Operational notes

- **Crash mid-`add-index`** — the index dir + index.conf are written atomically at the end; partial state is recoverable by re-running `add-index` for that field. There's no half-written index that would mislead the planner.
- **Reads during seed** — with `indexes:[]`, the planner picks `PRIMARY_NONE` for every criterion → full scan. Don't expose a partially-loaded table to production traffic until indexes are built.
- **Memory** — `add-index` respects `INDEX_BUILD_BUDGET_MB`. For very large fields (e.g. high-cardinality varchar at 25 M+) the build switches to the streaming external-merge pipeline automatically.
- **Auditing the choice** — `LOG_AUDIT(LOG_SUB_CONFIG, "ADD-INDEX ...")` lands in the audit log per field; tally that against your insert-time audit logs to confirm the seed sequence ran as planned.

## Further reading

- [docs/operations/benchmarks.md §5](benchmarks.md#5-invoice-multi-threaded--1m-records-64-fields-14-indexes) — full 1 M invoice numbers across pattern + connection-count permutations.
- [docs/query-protocol/bulk.md](../query-protocol/bulk.md) — wire-protocol reference for `bulk-insert`, chunk sizing, file vs inline payloads.
- [docs/concepts/indexes.md](../concepts/indexes.md) — why the per-(field, shard) merge cost scales `O(R²)`.
- [docs/reference/changelog.md](../reference/changelog.md) — historical perf notes; the original load-then-index measurements ship under the 2026.05.1 and 2026.05.4 entries.
