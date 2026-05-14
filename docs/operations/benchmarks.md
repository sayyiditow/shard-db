# Benchmarks

Five canonical workloads on **AMD Ryzen 7 7840U** (8C / 16T) · 32 GB · NVMe ext4 · Linux 6.19 · gcc 15.2 `-O2`. Each scenario is a C-level bench in `src/bench/` invoked via `./build/bin/shard-db-bench run <name>`. All numbers are from end-to-end runs with the server over TCP — **request parse, auth, encode, disk write, ACK** are all in the measurement. Nothing is bypassed.

> **Reads are strict request-response.** The single-record read benches (GET / EXISTS / UPDATE / DELETE) wait for each response before sending the next request. Real-world clients that pipeline requests on the wire (multiple in flight at once) will see meaningfully higher per-connection throughput. Treat the per-op latencies as honest single-request floors and the throughputs as the lower-bound for clients that don't pipeline.

## 1. K/V single-threaded — 10M records

`shard-db-bench run bench-kv` with `SHARD_BENCH_COUNT=10000000`, `SPLITS=128`.

Schema: **16-byte hex key, one `varchar(100)` value** — the same record shape used by LMDB / LevelDB / RocksDB `db_bench` so numbers compare directly. Unlike those embedded libraries, every request below crosses a TCP socket and goes through JSON/CSV parsing on the server.

| Operation | Throughput / Latency |
|---|---|
| Bulk insert (JSON, 10M in one request) | **3.46 M inserts/sec** (2.89 s) |
| Bulk insert (CSV, 10M in one request) | **4.64 M inserts/sec** (2.15 s) |
| **Bulk EXISTS (10K keys / request)** | **5.69 M ops/sec** (1.8 ms) |
| **Bulk DELETE (10K keys / request)** | **1.87 M ops/sec** (5.3 ms) |
| **Bulk GET (10K keys / request)** | **1.26 M ops/sec** (7.9 ms) |
| **Bulk UPDATE (10K keys / request)** | **822 k ops/sec** (12.2 ms) |
| GET ×10,000 (req-resp, 1 conn) | **33 k ops/sec** (mean 28µs / p50 28µs) |
| EXISTS ×10,000 hits (req-resp) | **33 k ops/sec** (mean 29µs / p50 29µs) |
| EXISTS ×10,000 all-miss (cold probe) | **35 k ops/sec** (mean 27µs / p50 27µs) |
| UPDATE ×10,000 (req-resp) | **30 k ops/sec** (mean 31µs / p50 31µs) |
| DELETE ×10,000 (req-resp) | **27 k ops/sec** (mean 35µs / p50 35µs) |
| Parallel GET (5 conns × 10k) | **151 k ops/sec** (331 ms) |
| Parallel UPDATE (5 conns × 10k) | **134 k ops/sec** (373 ms) |
| Disk footprint | 2.2 GB |

**Bulk multi-key paths are 30-170× faster than single-conn.** The single-conn req-resp ceiling is ~30 µs/op (dominated by TCP framing + JSON parse/encode). One bulk request handling 10K keys at a time hits 1.3M+/sec on every multi-key op — `bulk_lookup_in_kfshard` / `bulk_get_in_kfshard` / `bulk_upsert_in_kfshard` / `bulk_delete_in_kfshard` each acquire one kf wrlock per worker (vs per-record), batch seg I/O sorted by `(stream_id, file_id)`, and use parallel_for fan-out across kf shards. Use bulk wire shapes (`{"mode":"get","keys":[...]}`, `{"mode":"bulk-delete","keys":[...]}`) when you have multi-key workloads.

## 2. K/V multi-threaded — 10M records, scaling across connections

`shard-db-bench run bench-kv-parallel` with `SHARD_BENCH_TOTAL=10000000`, `SHARD_BENCH_CHUNK=2000000`, `SPLITS=128`.

Same schema, bulk insert fanned out across TCP connections. `SPLITS=128` is the sweet spot for 10M rows (≈78K records/shard — see the [splits sizing table](#splits-tuning)); going to 1024 at this scale *slows* the benchmark.

| Scenario | Time | Throughput |
|---|---|---|
| Single JSON, 10M | 2.80 s | **3.57 M/sec** |
| Single CSV, 10M | 2.04 s | **4.91 M/sec** |
| **Parallel JSON, 5 conns × 2M** | **1.37 s** | **7.29 M/sec** (2.04× single) |
| **Parallel CSV, 5 conns × 2M** | **1.19 s** | **8.42 M/sec** (1.71× single) ← fastest |

Shard load distribution (128 splits): avg 0.298 (kf), 78K records/shard, even distribution (records stddev <1 %).

**How to read these numbers.** On 16 B / 100 B records LMDB publishes ~1 M on-disk inserts/sec (embedded, no network). shard-db sustains **4.91 M/sec single-connection** (CSV) and scales to **8.42 M/sec at 5 connections × 2 M records each**, all over TCP with CSV parsing on the server. **Parallel keeps a 1.7–2.0× edge over single-conn** at this scale — multiple connections amortize per-call pipeline tail (parse, bucket, dispatch) against wider write fan-out. The slotcask v2 storage engine (2026.06+) improved parallel throughput by ~12 % over the prior layout while keeping single-conn rates similar. **Use single-connection for operational simplicity, parallel for headline throughput.**

<a id="shard-grow-pre-sizing"></a>
**Pre-grow (2026.05.x):** when `bulk-insert` receives a batch, the dispatcher reads each shard's current `slots_per_shard` and live record count, computes the smallest power-of-2 that holds (live + incoming), and grows each shard to that target once before workers start. Pre-grows run in parallel via the worker pool. The previous behaviour (worker grew its shard every time it overflowed during the insert) caused 9 incremental grows per shard at SPLITS=128 / 10M records, each rebucketing the existing data; now that's 1 pre-grow per shard with zero rebucket (the shards are empty when pre-grow fires for a fresh load).

## 3. Queries on 1M users

`shard-db-bench run bench-queries`.

13 typed fields (varchar, int, long, short, double, bool, byte, date, datetime, numeric, currency). 12 indexes (`username`, `email`, `age`, `active`, `birthday`, `user_id`, `rank`, `score`, `level`, `created_at`, `balance`, `hourly_rate`); `bio` left non-indexed to exercise full-scan paths. C-bench measurements; cache is hot from the same-process bulk insert.

**Insert** (1M records, 1 conn, JSON, 12 indexes built upfront): **0.24 M/sec** (4.12 s).

### COUNT — point lookups & ranges by field type

`eq` cost scales with **match cardinality** — the planner walks one btree
leaf entry per match. Point lookups (1 match) are sub-millisecond; large
match sets (100K+ records) pay btree-leaf-walk cost (~10-25 ns per entry,
parallel across idx shards). Cold-cache first reads add 1-3 ms one-time
disk-load tax per indexed field.

| op | matches | warm | cold (first query against this index) |
|---|---|---|---|
| `eq` point lookup (e.g. `user_id=N`) | 1 | **0.07-0.21 ms** | same |
| `eq` mid-cardinality (e.g. `age=30`, ~17K) | 10K-100K | 0.2-0.5 ms | 0.5-1.5 ms |
| `eq` low-cardinality (e.g. `active=false`, ~143K) | >100K | 0.5-2 ms | **2-4 ms** |
| `eq` non-indexed (full scan) | — | 46 ms | 50 ms |
| `neq` (negation shortcut: total − count(eq)) | inverted | 0.1-2 ms | 0.5-3 ms |
| Range single-bound (`gt` / `lt` / `gte` / `lte`) | varies | **0.4-9 ms** | 1-12 ms |
| Range coalesced (`gt + lt` on same field) | varies | **0.5-6 ms** (planner merges into one btree range) | 1-8 ms |
| `between` | varies | 1.6-4 ms | 2-6 ms |
| `in` / `not_in` (2-4 values) | small set | **0.08-1.0 ms** | 0.1-1.5 ms |

**Note**: There is no count shortcut for low-cardinality `eq` (e.g.
`active=false`) — every match increments a TLS-batched counter. For
fields where `eq` matches >100K records, the cost is the btree leaf
walk, not a metadata lookup.

### COUNT — string ops on indexed varchar

Substring/suffix ops on indexed varchar walk the btree leaves once (no record fetch — the value lives in the leaf), so they're 10–20× faster than the same op on a non-indexed field even though both visit O(N) entries.

| op | indexed varchar | non-indexed varchar (`bio`) |
|---|---|---|
| `starts` | 0.3–0.5 ms (prefix range scan) | **42–46 ms** (full record scan) |
| `ends` | 21 ms (full leaf scan, leaf-byte suffix match) | **42 ms** |
| `contains` | 17–23 ms (full leaf scan, leaf-byte memmem) | **52–54 ms** |
| `not_contains` | 17 ms | 58 ms |
| `like 'foo%'` | 1.1 ms (prefix range) | — |
| `like '%foo%'` | — | 57 ms |
| Case-insensitive variants (`istarts` / `icontains` / `iends` / `ilike`) | 12–29 ms | 51 ms |
| `regex` | 28–42 ms | 63–175 ms |
| `len_*` (length operators) | 12–18 ms | 49–64 ms |

The non-indexed full-scan column tops out at **~50–60 ms** because the v2 record walk now runs **parallel across all 64 kf shards** (one worker per shard via `parallel_for`), and within each shard refs are pre-sorted by `(stream_id, file_id)` so each segment is acquired **once per shard** instead of per record. That replaces ~1M `g_segcache_lock` acquires with ~320 (5–6 unique segments × 64 shards), turning what used to be 330 ms into 50 ms.

### COUNT — existence / OR / field-vs-field

| op | latency |
|---|---|
| `exists` typed non-varchar (always true → live_count shortcut) | **0.03–0.09 ms** |
| `exists` indexed varchar (btree size shortcut) | **9–11 ms** |
| `not_exists` indexed varchar (= live_count − count(exists)) | **11 ms** |
| `exists` non-indexed varchar | 67 ms (full scan) |
| `OR` 2 leaves (same field, eq) | 45 ms |
| `OR` 5 leaves | 105 ms (per-leaf btree walk + KeySet union, parallel across leaves but writes contend on shared KeySet) |
| `OR` cross-field (one selective + one ~50% match) | 170 ms (dominated by the 500K KeySet inserts) |
| `eq_field` / `neq_field` / `lt_field` / `gt_field` family | **33–44 ms** (always full scan — RHS is per-record) |

### FIND — limit-bound retrieval

Find-with-limit terminates as soon as the limit is reached; the parallel scan layer propagates an early-exit `stop_flag` so siblings bail without finishing their walks.

| query | latency |
|---|---|
| Indexed eq / range / between / IN — `limit 10` | **0.3–0.7 ms** |
| Indexed `starts` — `limit 10` | 0.7 ms |
| Indexed `contains` — `limit 10` | 10 ms (leaf scan, no early exit until 10 matches) |
| Indexed `regex` — `limit 10` | 28 ms |
| Indexed `not_regex` — `limit 10` | 0.5 ms (rare-mismatch shortcut) |
| Non-indexed `contains` / `starts` / `ends` on bio — `limit 10` | **27–34 ms** |
| Indexed AND across 2–3 fields | 14–75 ms (intersect cost grows with leaf cardinality) |
| Indexed + non-indexed AND (one selective indexed leaf) | 0.5 ms |

### AGGREGATE — single-fn standalone (no criteria)

Min/max are O(1) per shard (read the first/last leaf) — the btree being sorted by value gives them away for free. Sum/avg must visit every leaf (no algebraic shortcut), so they sit at 15–23 ms across types.

| op | latency |
|---|---|
| `count` (metadata shortcut) | **0.13 ms** |
| `min` / `max` on indexed (any type) | **0.04–0.31 ms** |
| `sum` / `avg` on indexed (any numeric type) | **15–23 ms** |

### AGGREGATE — with criteria (single-spec)

Walk-fetch-check fast path tries the agg btree first for any single-spec MIN/MAX with criteria — when criteria selectivity is >~0.05% it finds the answer in single-digit ms by walking the agg btree in MIN/MAX order, fetching records, and stopping at the first match per shard. Falls through to keyset-build for low-selectivity cases.

| query | latency |
|---|---|
| `min/max field where same_field op X` (same-field shortcut) | **0.08–0.51 ms** |
| `min/max field1 where field2 op X` (cross-field, walk-fetch-check) | **0.1–0.5 ms** |
| `min/max field1 where field2 AND field3 ...` (intersect path) | **0.15–0.51 ms** |
| `agg(neq)` on small-domain field (NEQ shortcut: total − eq) | 0.5 ms |
| `sum/avg field1 where field2 op X` (parallel agg-btree walk filtered by criteria KeySet) | **44–54 ms** |

### AGGREGATE — bundled & grouped

Indexed group_by walks the group field's btree once and looks up each entry's accumulator slot in a hash16→bucket map (built from the agg field's btree walk). For multi-spec sharing an agg field, that field's btree is walked once total. Multi-field group_by builds hash16→value maps for secondary group fields.

| query | latency |
|---|---|
| `count(*)` + `sum/avg/min/max balance` (no group, multi-spec) | **27 ms** |
| `group by active`, count + avg(balance) (low cardinality) | **123 ms** |
| `group by age` top 5 by count | **17 ms** (group cardinality 100, early-cap) |
| `group by age having n > 16k` | 17 ms |
| `group by active`, sum(balance) | 123 ms |
| `group by active`, min/max balance | 123 ms |
| `group by birthday`, count | 18 ms |
| `group by username`, count (varchar idx, ~1M unique buckets) | **360 ms** |
| `group by email`, sum(balance) (varchar idx, ~1M buckets) | **645 ms** |
| `group by F where indexed criterion` (criteria-filtered group_by) | 165–250 ms |
| `group by F where AND of indexed leaves` | **6.7 ms** (intersect-on-indexed seeds tiny KeySet) |
| `group by F1, F2` (multi-field) | 355–490 ms |

For high-cardinality varchar group_by, the bucket creation cost dominates — each unique value allocates an `AggBucket` from the per-query arena (50 ns/bucket) and inserts into a dynamic hash table that doubles to keep load factor ≤ 1. At 1M unique varchar buckets, that's ~50 ms of bucket setup alone, then ~50 ms per agg field btree walk.

### FIND — keyset cursor pagination

| query | latency |
|---|---|
| ASC / DESC by indexed field, `limit 100` | **0.18–0.38 ms** |
| Cursor continuation mid-range | 0.94 ms |
| `offset 50000 limit 100` (buffered pre-skip) | 2.4 ms |

The cursor path is O(log N) per page — staying sub-millisecond regardless of depth. **Use `cursor:null` continuation for deep pagination**, not `offset` (which collects the full prefix into the per-query buffer and aborts past `QUERY_BUFFER_MB`).

All 38 search operators use indexes when available. Full scans (non-indexed fields, `_field` ops where RHS is per-record) parallelize across kf shards with per-segment batched cache acquire — the v2 record walk hits ~50 ms / 1M records on this hardware.

## 3a. Queries on 25M users — scaling characteristics

`shard-db-bench run bench-queries` with `SHARD_BENCH_COUNT=25000000`. Same 13-field schema and 12 indexes as §3; the bench's auto-tuned tier picker selects `splits=128` (195K records/shard, fits the engine's 256K initial slots — no resplit during the 25M insert).

**Insert** (25M records, parallel, 12 indexes built upfront, disk-backed): **~120 k/sec** single-conn, **~430 k/sec** parallel × 5 conns (CSV form). 25M chunked in 1M batches.

These numbers assume **warm cache** (kernel page cache holds the relevant kf + segment pages). First-touch queries against an indexed field's btree pay a one-time disk-read cost (1-10 ms cold, sub-ms warm). The `cold` column shows the worst-case first-query latency after a cold daemon start; `warm` is what subsequent queries see.

### COUNT — selectivity scaling

| query | warm | cold (first-touch on this index) |
|---|---|---|
| `eq username` (point lookup, varchar idx) | **0.1 ms** | 1-2 ms |
| `eq active=false` (~3.5M matches, bool) | 15-30 ms | 200-400 ms |
| `eq age=30` (~417K matches, int) | 1-3 ms | 50-150 ms |
| `eq bio` (full scan, ~25M records) | 800-1500 ms | 6-9 s |
| `gt user_id>500000` (range, ~12.5M matches) | 200-400 ms | 1-2.5 s |
| `between age 30..40` | 30-50 ms | 300-500 ms |
| `contains bio 'DevOps'` (full scan + simd memmem) | 800-1000 ms | 1-3 s |
| `icontains bio 'devops'` (full scan + simd memcasemem) | 800-1000 ms | 1-3 s |
| `OR 5 leaves on age` | 2-3 s | 9-12 s |
| `OR cross-field` (age=30 OR active=false) | 4-5 s | 14-15 s |
| `eq_field age == rank` (always full scan) | 590-700 ms | 600-700 ms (CPU-bound) |

### AGGREGATE — single-fn standalone

The leaf-only walker + MADV_SEQUENTIAL combo shipped in 2026.05.4 brings cold sum/avg from multi-second to a few hundred ms — disc reads coalesce into 128 KB+ I/Os instead of 4 KB-per-page faults under the default `MADV_RANDOM`.

| query | warm | cold (2026.05.4) | cold (2026.05.3) |
|---|---|---|---|
| `count all` (metadata) | **0.6 ms** | 0.6 ms | 0.6 ms |
| `min/max` on indexed (int/long/etc.) | 0.1-1 ms | 0.2-1 ms | 0.5-200 ms |
| `sum age` (int) | 20-30 ms | **~180 ms** | 9-11 s |
| `sum user_id` (long) | similar | **~220 ms** | similar to int |
| `sum balance` (numeric) | 40-50 ms | **~210 ms** | 13-16 s |
| `avg score` (double) | 40-50 ms | **~40 ms** (warm path) | 0.5-2 s |

### AGGREGATE — bundled & grouped

The parallel Pass 1 + parallel merge wins are most visible at this scale. 2026.05.4 added two streaming-distinct fast paths gated on `single varchar group_by + finite limit + no criteria/having/order_by` — for queries that match the shape, the wins are dramatic:

| query | warm |
|---|---|
| `group by active` (count + avg) | 3-5 s |
| **`group by username, count limit 10` (varchar idx, high-card)** | **~3 ms cold** (was 4-5 s — streaming k-way merge, 2026.05.4) |
| **`group by email, sum(balance) limit 10` (varchar idx + indexed numeric agg)** | **~4 ms cold** (was 5-8 s — streaming k-way merge + per-emit lookup, 2026.05.4) |
| `group by username, count` (no limit, full enumeration) | 4-5 s |
| `group by email, sum(balance)` (no limit) | 5-8 s |
| `group by birthday, count` (low cardinality) | 90-110 ms |
| `group by F where AND-of-indexed` | 30-50 ms |
| `group by F1, F2` (multi-field, low cardinality) | 6-9 s |

### CURSOR pagination

| query | warm |
|---|---|
| ASC by indexed field, `limit 100` | 0.2-15 ms |
| **DESC by age** (currently walks full forward leaf chain to collect IDs) | **18-30 ms** warm, 1-10 s cold (architectural — `prev_leaf` pointer is on the backlog) |
| ASC + criteria, continuation | 1-3 ms |
| `offset 50000 limit 100` | 4-5 ms |

### Cold-cache caveat

Aggregate full-btree walks (sum/avg/max with no criteria) and OR-cross-field queries pay 5-15× cold-vs-warm penalty at 25M scale. First query against each indexed field reads ~750 MB of btree pages from disk; subsequent queries within the same daemon lifetime hit the OS page cache. **Production deployments with steady query patterns hit warm numbers**; a fresh daemon's first query against any large index pays the cold cost once.

For workloads that genuinely need fast cold-start (e.g. on-demand analytics) the kf header + segment data are mmap'd `MAP_SHARED` with `MADV_HUGEPAGE` — `posix_fadvise(POSIX_FADV_WILLNEED)` on startup would prefetch but isn't done today.

## 4. Invoice single-threaded — 1M records, 64 fields, 14 indexes

`shard-db-bench run bench-invoice`, `SPLITS=64`.

Realistic wide-object schema (~1.9 KB/record). Composite indexes include `irbmStatus+pdfSent`, `status+source`, `status+createdAt`, `status+invoiceDate`.

| Operation | Result |
|---|---|
| Bulk insert (no indexes) | **357 k/sec** (2.80 s) |
| Bulk insert (with 14 indexes) | **216 k/sec** (4.63 s) — 46% slowdown vs no-idx |
| Add 14 indexes post-insert | **2.70 s** (per-shard parallel build — 14 × splits/4 workers) |
| GET ×1000 (req-resp, 1 conn) | **29 k ops/sec** (mean 34µs / p50 32µs) |
| EXISTS ×1000 (req-resp) | **37 k ops/sec** (mean 27µs / p50 27µs) |
| Indexed eq `find` (any of 14 indexes, limit 10) | **0.8–1.6 ms** |
| Indexed `contains` via leaf scan | 0.8–10 ms |
| Indexed IN (2 values) | <1 ms |
| Composite index eq / starts | **0.5–0.9 ms** (composite index walk skips secondary post-filter) |
| Indexed `range` (wide gte+lte on `invoiceDate` / `createdAt`, limit 10) | **<1 ms warm** (was 64–65 ms in earlier docs — that was first-touch cold; the post-2026.05.1 batched seg-acquire makes warm-cache range cheap) |
| Indexed `OR` (two statuses) | **3.6 ms** (was 23 ms — kf-derived counts let the OR keyset size correctly without falling through to per-record scan) |
| Fetch page of 100 @ offset 5000 | <1 ms |
| Keys (first 100) | **<0.2 ms** — limit-bound calls (limit ≤ 1000) route through `slotcask_walk_one_shard_streaming`, which fires cb per record and bails immediately on stop_flag instead of completing the buffered Pass-1 ref-collect. Buffered path stays for unbounded scans (full-scan `count` etc.) where per-segment batched acquire still wins. |
| `count` full object | <1 ms |
| **Single DELETE ×1000 (with 14 indexes)** | **15 k/sec** (mean 67µs / p50 63µs) |
| **Bulk DELETE ×1000** | **143 k/sec** (7 ms) |
| VACUUM | **38 ms** (now actually rebuilds kf to drop tombstones — the previously-published 1 ms was the pre-2026.05.x reset_deleted_count path that lied about orphaned-count; the new vacuum is honest) |
| RECOUNT | **<1 ms** (kf-header sum — was 13 ms walking kf entries) |
| Disk footprint | 1.9 GB |

The earlier-published indexed-range figure (3 ms) was borrowed from §3's narrow `between (age 25-35)` query on the user schema; the C bench's invoice range queries cover wider date spans (one-month and two-week windows on `invoiceDate`/`createdAt`) so the 65–79 ms numbers are honest cost for matching tens of thousands of records and returning the first 10 ordered by index leaf order.

The delete speedups come from `bulk_del_shard_worker` and `single_delete` paths now going through the unified shard cache (`ucache_get_write` per shard). Pre-2026.05.1 they did per-call `open + flock + mmap MAP_SHARED + munmap`, paying full page-fault tax per request.

## 5. Invoice multi-threaded — 1M records, 64 fields, 14 indexes

`shard-db-bench run bench-parallel`, `SPLITS=64`, 5 connections × 200 k records each.

| Scenario | Time | Throughput |
|---|---|---|
| Single JSON, 1M, no indexes | 2.48 s | 400 k/sec |
| Single CSV, 1M, no indexes | 1.05 s | **950 k/sec** |
| **Parallel JSON, 5 conns × 200K, no indexes** | **0.65 s** | **1.54 M/sec** (3.9× single JSON) |
| Parallel JSON, 5 conns × 200K, pre-existing 14 indexes | 2.81 s | 360 k/sec |
| **Parallel CSV, 5 conns × 200K, no indexes** | **0.37 s** | **2.67 M/sec** (2.8× single CSV) ← fastest |
| **Parallel CSV, 5 conns × 200K, pre-existing 14 indexes** | **2.40 s** | **420 k/sec** |
| Add 14 indexes after bulk insert | ~2.7 s | (per-shard parallel build) |
| Disk footprint (with 14 indexes) | 1.6 GB |

**Parallel CSV no-idx hits 2.48 M/sec** — 2.7× single CSV's 903 k/sec, and roughly 5× the previously published bash-bench number for this scenario. Pre-grow contributes a ~2× win on every path; the parallel-vs-single benefit is on top of that and hasn't disappeared. **Use parallel for max throughput.**

For indexed loads at 1M / 5×200K, **parallel CSV with pre-existing indexes (2.30 s) beats parallel CSV no-idx + add-indexes (0.40 + 2.6 = 3.0 s) by 1.3×**, and beats single-conn load-then-index (1.11 + 2.69 = 3.80 s) by 1.65×. So pre-existing-indexes parallel wins at this scale. The R² merge-cost rule (see chunk-size tuning below) means this advantage shrinks as you push R higher: at 10M with the same 200K chunks (R=50), the merge cycles dominate and load-then-index can pull ahead. Re-bench when you change scale.

### Indexed bulk-insert chunk-size tuning

The per-shard btree layout (2026.05.1+) makes indexed bulk-insert sensitive to the *number* of bulk-insert REQUESTS, because each request triggers a sequential `bulk_merge` cycle per (field, shard). Cumulative extract work scales `O(R²)` where R is request count.

**Recommended at 1M+ records:** prefer **fewer, larger** bulk-insert calls. `requests ≈ N / 200_000` with `5 ≤ connections` remains a sensible floor: each request triggers one merge cycle per (field, shard), so packing more rows per request keeps the cycle count down. For non-indexed data loads, parallel always pays off (see §2); for indexed loads, parallel still helps, just by less because phase-4 dominates.

### Pattern comparison for indexed batch ingest (1M / 5×200K)

C-bench measurements:

| Pattern | 1M × 14 idx total time | Throughput |
|---|---|---|
| **Parallel CSV with pre-existing 14 idx (5 conns × 200K)** | **2.30 s** | **435 k/sec** ← winner at this scale |
| Parallel CSV no-idx → add-indexes | 0.40 + 2.62 = 3.02 s | 331 k/sec |
| Single CSV no-idx → add-indexes | 1.11 + 2.69 = 3.80 s | 263 k/sec |

The earlier "load-then-index always wins" guidance came from bash measurements that under-counted parallel throughput; with C bench, **parallel + pre-existing-indexes is the fastest path at 1M / 5×200K**. The R² merge-cost rule still applies — if you scale to 10M with the same 200K chunks (R=50), the merge cycles balloon and load-then-index reclaims the lead. As a rule of thumb: at the recommended `R ≈ N/200K` chunk count, pre-existing-indexes parallel wins for `R ≤ ~10`, and load-then-index wins for `R ≥ ~20`. Bench at your scale.

**Use load-then-index** when you can afford to drop indexes during the load (static schemas, batch-ingest pipelines), at very large scales where R is high, or when operational simplicity matters. **Use pre-existing-indexes parallel** for streaming workloads or moderate batch sizes (1M ish) where R stays small.

## Disk footprint

Per-shard btree layout adds ~25 % to indexed-object disk usage vs pre-2026.05.1 (1.3 → 1.6–1.7 GB on the invoice schema). Sources:

1. Each btree starts at `2 × bt_page_size = 8 KB`. With 14 indexes × 16 idx shards = 224 trees minimum, that's ~1.8 MB of header overhead before any data (vs 14 × 8 KB = 112 KB for the old single-tree layout).
2. Reduced prefix-compression effectiveness: each leaf page has 1/16 the entries to share prefixes with, so per-entry compression savings drop ~15–25 %.
3. Page-allocation rounding: each btree's pages are 4 KB; trailing slack accumulates across 16× more trees.

Real space cost on production datasets typically lands at **+20–30 %** vs the legacy layout.

## Notes

- **File-descriptor limit.** At `SPLITS ≥ 512`, `ucache_grow_shard` briefly holds 2 fds per shard during migration, so peak can hit ~8,256 fds at the default `FCACHE_MAX=4096`. The server auto-raises its soft limit to the hard limit at startup (no privilege needed); if the hard limit itself is too low (shells default to 1024 on many distros), the startup WARN tells you exactly what to put in `/etc/security/limits.conf` or as `LimitNOFILE=` in a systemd unit.

- **CSV vs JSON.** CSV bulk insert is faster because the CSV path parses directly against the mmap'd file via `(ptr, len)` spans with zero per-line memcpy, while the JSON path materializes a `JsonObj` per record.

### Splits — bench-derived numbers

Splits-sizing guidance lives in [tuning.md → Sizing `splits`](tuning.md#sizing-splits). The numbers below are the bench raw data that table is built from.

On the parallel K/V bench at 10M rows:

| `splits` | Records/shard | Wall time |
|----------|---------------|-----------|
| 64       | 156K          | 3.605s    |
| **128**  | **78K**       | **3.488s** (fastest) |
| 256      | 39K           | 3.986s    |
| 1024     | 9K            | 5.454s    |

Counter-intuitively, raising `splits` *beyond* the sweet spot slows things down even at 10 parallel connections — more shard files = more syscalls and mmap page faults per query, and shard-lock contention isn't the bottleneck at this scale. If you exceed ~1M records/shard you've saturated this design — split across multiple objects (or tenant dirs) rather than climbing past `MAX_SPLITS=4096`.

## Reproduce

```bash
./build.sh

# Each bench self-spawns its own daemon on a tmp DB_ROOT and tears down on exit.
# Scale via env vars (defaults match the published numbers in the tables above).

SHARD_BENCH_COUNT=10000000 ./build/bin/shard-db-bench run bench-kv             # §1
SHARD_BENCH_TOTAL=10000000 SHARD_BENCH_CHUNK=2000000 \
  ./build/bin/shard-db-bench run bench-kv-parallel                              # §2
./build/bin/shard-db-bench run bench-queries                                    # §3 (1M users)
./build/bin/shard-db-bench run bench-invoice                                    # §4 (1M invoice)
./build/bin/shard-db-bench run bench-parallel                                   # §5 (1M invoice parallel)

# Or run the full suite:
./build/bin/shard-db-bench run-all
```

Scale-override env vars: `SHARD_BENCH_COUNT` (single-conn benches), `SHARD_BENCH_TOTAL` + `SHARD_BENCH_CHUNK` (parallel benches), `SHARD_BENCH_USERS` + `SHARD_BENCH_ORDERS` (`bench-joins`). All sub-µs precision via `clock_gettime(CLOCK_MONOTONIC)`.
