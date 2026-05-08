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

13 typed fields (varchar, int, long, short, double, bool, byte, date, datetime, numeric, currency). Indexes on `username`, `email`, `age`, `active`, `birthday`. C-bench measurements; cache is hot from the same-process bulk insert that built the dataset.

| Operation class | Latency band |
|---|---|
| `count` metadata (no criteria) | **<1 ms** (O(1) counter file) |
| `count` indexed eq / between / in / lt / gt / lte / gte | **<1–6 ms** |
| `count` indexed `starts` | **<1 ms** |
| `count` indexed `exists` (full-set traversal) | **19 ms** |
| `count` indexed `contains` / `ends` / `ncontains` (leaf scan) | **7–8 ms** |
| `count` full-scan (non-indexed field) | **4–6 ms** (scan-path is lock-free; each shard runs concurrently) |
| `count` indexed + secondary filter | **22–64 ms** |
| `find` limit 10 — any indexed op | **<1 ms** |
| `find` limit 10 — full scan on non-indexed | **<1 ms** (Zone A probe + typed compare) |
| `find` indexed + secondary filter | **<1 ms** |
| `aggregate count` (metadata) | **<1 ms** |
| `aggregate` where indexed-eq | **10–24 ms** |
| `aggregate` where indexed range | **71–168 ms** |
| `aggregate` full-scan (sum/avg/min/max) | **390 ms** |
| `aggregate` group_by on full scan | **220–252 ms** |
| `aggregate` group_by + having | **296 ms** |
| `find` cursor page 1 (ASC/DESC, indexed `order_by`) | **<1 ms** |
| `find` cursor continuation (mid-range seek) | **<1 ms** |

All 38 search operators use indexes when available. Full scans stay fast because Zone A (24-byte metadata headers) remains resident in the page cache and typed binary records in Zone B are compared without JSON parsing.

**On deep offset-based pagination:** `offset:50000 limit:100 order_by:age` (no cursor) hits the buffer-sort path which collects the full prefix into the per-query memory buffer; on a 1M dataset with `QUERY_BUFFER_MB=500` this aborts with `query memory buffer exceeded` instead of returning a (very slow) page. Use `cursor:null` + continuation tokens for deep pagination — the cursor path is O(log N) per page regardless of depth and stays sub-millisecond.

## 4. Invoice single-threaded — 1M records, 64 fields, 14 indexes

`shard-db-bench run bench-invoice`, `SPLITS=64`.

Realistic wide-object schema (~1.9 KB/record). Composite indexes include `irbmStatus+pdfSent`, `status+source`, `status+createdAt`, `status+invoiceDate`.

| Operation | Result |
|---|---|
| Bulk insert (no indexes) | **357 k/sec** (2.80 s) |
| Bulk insert (with 14 indexes) | **230 k/sec** (4.35 s) — 36% slowdown vs no-idx |
| Add 14 indexes post-insert | **2.79 s** (per-shard parallel build — 14 × splits/4 workers) |
| GET ×1000 (req-resp, 1 conn) | **30 k ops/sec** (mean 33µs / p50 31µs / p99 101µs) |
| EXISTS ×1000 (req-resp) | **37 k ops/sec** (mean 26µs / p50 26µs / p99 43µs) |
| Indexed eq `find` (any of 14 indexes, limit 10) | **1–2 ms** |
| Indexed `contains` via leaf scan | 1–11 ms |
| Indexed IN (2 values) | 1 ms |
| Composite index eq / starts | 1–2 ms |
| Indexed `range` (wide gte+lte on `invoiceDate` / `createdAt`, limit 10) | **64–65 ms** (cold-cache; the bench's pre-range battery never touches these btrees, so they're cold when range fires. Once warm — see §3's age-range numbers — these drop to <1 ms) |
| Indexed `OR` (two statuses) | 23 ms |
| Fetch page of 100 @ offset 5000 | 1 ms |
| Keys (first 100) | <1 ms |
| `count` full object | <1 ms |
| **Single DELETE ×1000 (with 14 indexes)** | **10 k/sec** (mean 95µs / p50 91µs / p99 134µs) |
| **Bulk DELETE ×1000** | **111 k/sec** (9 ms) |
| VACUUM | 1 ms |
| Disk footprint | 1.6 GB |

The earlier-published indexed-range figure (3 ms) was borrowed from §3's narrow `between (age 25-35)` query on the user schema; the C bench's invoice range queries cover wider date spans (one-month and two-week windows on `invoiceDate`/`createdAt`) so the 65–79 ms numbers are honest cost for matching tens of thousands of records and returning the first 10 ordered by index leaf order.

The delete speedups come from `bulk_del_shard_worker` and `single_delete` paths now going through the unified shard cache (`ucache_get_write` per shard). Pre-2026.05.1 they did per-call `open + flock + mmap MAP_SHARED + munmap`, paying full page-fault tax per request.

## 5. Invoice multi-threaded — 1M records, 64 fields, 14 indexes

`shard-db-bench run bench-parallel`, `SPLITS=64`, 5 connections × 200 k records each.

| Scenario | Time | Throughput |
|---|---|---|
| Single JSON, 1M, no indexes | 2.67 s | 374 k/sec |
| Single CSV, 1M, no indexes | 1.11 s | **903 k/sec** |
| **Parallel JSON, 5 conns × 200K, no indexes** | **0.72 s** | **1.40 M/sec** (3.7× single JSON) |
| Parallel JSON, 5 conns × 200K, pre-existing 14 indexes | 2.61 s | 384 k/sec |
| **Parallel CSV, 5 conns × 200K, no indexes** | **0.40 s** | **2.48 M/sec** (2.7× single CSV) ← fastest |
| **Parallel CSV, 5 conns × 200K, pre-existing 14 indexes** | **2.30 s** | **435 k/sec** |
| Add 14 indexes after bulk insert | ~2.6 s | (per-shard parallel build) |
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
