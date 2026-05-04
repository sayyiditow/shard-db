# Benchmarks

Five canonical workloads on **AMD Ryzen 7 7840U** (8C / 16T) · 32 GB · NVMe ext4 · Linux 6.19 · gcc 15.2 `-O2`. Each scenario is a C-level bench in `src/bench/` invoked via `./build/bin/shard-db-bench run <name>`. All numbers are from end-to-end runs with the server over TCP — **request parse, auth, encode, disk write, ACK** are all in the measurement. Nothing is bypassed.

> **Reads are strict request-response.** The single-record read benches (GET / EXISTS / UPDATE / DELETE) wait for each response before sending the next request. Real-world clients that pipeline requests on the wire (multiple in flight at once) will see meaningfully higher per-connection throughput. Treat the per-op latencies as honest single-request floors and the throughputs as the lower-bound for clients that don't pipeline.

## 1. K/V single-threaded — 10M records

`shard-db-bench run bench-kv` with `SHARD_BENCH_COUNT=10000000`, `SPLITS=128`.

Schema: **16-byte hex key, one `varchar(100)` value** — the same record shape used by LMDB / LevelDB / RocksDB `db_bench` so numbers compare directly. Unlike those embedded libraries, every request below crosses a TCP socket and goes through JSON/CSV parsing on the server.

| Operation | Throughput / Latency |
|---|---|
| Bulk insert (JSON, 10M in one request) | **3.99 M inserts/sec** (2.51 s) |
| Bulk insert (CSV, 10M in one request) | **4.96 M inserts/sec** (2.02 s) |
| GET ×10,000 (req-resp, 1 conn) | **34.9 k ops/sec** (mean 28.2µs / p50 26.4µs / p99 51.1µs) |
| EXISTS ×10,000 hits (req-resp) | **36.4 k ops/sec** (mean 27.1µs / p50 25.8µs / p99 49.7µs) |
| EXISTS ×10,000 all-miss (cold probe) | **37.0 k ops/sec** (mean 26.8µs / p50 25.8µs / p99 43.6µs) |
| UPDATE ×10,000 (req-resp) | **32.5 k ops/sec** (mean 30.4µs / p50 28.2µs / p99 62.2µs) |
| DELETE ×10,000 (req-resp) | **17.9 k ops/sec** (mean 55.6µs / p50 51.4µs / p99 116.2µs) |
| Parallel GET (5 conns × 10k) | **165.9 k ops/sec** (301 ms) |
| Parallel UPDATE (5 conns × 10k) | **150.0 k ops/sec** (333 ms) |
| Disk footprint | 2.3 GB |

## 2. K/V multi-threaded — 10M records, scaling across connections

`shard-db-bench run bench-kv-parallel` with `SHARD_BENCH_TOTAL=10000000`, `SHARD_BENCH_CHUNK=2000000`, `SPLITS=128`.

Same schema, bulk insert fanned out across TCP connections. `SPLITS=128` is the sweet spot for 10M rows (≈78K records/shard — see the [splits sizing table](#splits-tuning)); going to 1024 at this scale *slows* the benchmark.

| Scenario | Time | Throughput |
|---|---|---|
| Single JSON, 10M | 2.41 s | **4.15 M/sec** |
| Single CSV, 10M | 1.87 s | **5.34 M/sec** |
| **Parallel JSON, 5 conns × 2M** | **1.47 s** | **6.79 M/sec** (1.64× single) |
| **Parallel CSV, 5 conns × 2M** | **1.32 s** | **7.55 M/sec** (1.41× single) ← fastest |

Shard load distribution (128 splits): avg 0.596, records stddev 1.6 %, 1 grow per shard (down from 9 pre-2026.05.x — see [pre-grow](#shard-grow-pre-sizing) below).

**How to read these numbers.** On 16 B / 100 B records LMDB publishes ~1 M on-disk inserts/sec (embedded, no network). shard-db sustains **5.34 M/sec single-connection** (CSV) and scales to **7.55 M/sec at 5 connections × 2 M records each**, all over TCP with CSV parsing on the server. **Parallel still wins for max throughput** — pre-grow (2026.05.x+) made bulk-insert ~2× faster on every path; multiple connections amortize their per-call pipeline tail (parse, bucket, dispatch, activate per request) against the wider write fan-out, so parallel keeps a ~1.4–1.6× edge over single-conn at this scale. **Use single-connection for operational simplicity, parallel for headline throughput.**

<a id="shard-grow-pre-sizing"></a>
**Pre-grow (2026.05.x):** when `bulk-insert` receives a batch, the dispatcher reads each shard's current `slots_per_shard` and live record count, computes the smallest power-of-2 that holds (live + incoming), and grows each shard to that target once before workers start. Pre-grows run in parallel via the worker pool. The previous behaviour (worker grew its shard every time it overflowed during the insert) caused 9 incremental grows per shard at SPLITS=128 / 10M records, each rebucketing the existing data; now that's 1 pre-grow per shard with zero rebucket (the shards are empty when pre-grow fires for a fresh load).

## 3. Queries on 1M users

`bench-queries.sh`.

13 typed fields (varchar, int, long, short, double, bool, byte, date, datetime, numeric, currency). Indexes on `username`, `email`, `age`, `active`, `birthday`.

| Operation class | Latency band |
|---|---|
| `count` metadata (no criteria) | **3 ms** (O(1) counter file) |
| `count` indexed eq / between / in / lt / gt / lte / gte | **3–12 ms** |
| `count` indexed `starts` / `exists` | **3–21 ms** |
| `count` indexed `contains` / `ends` / `ncontains` (leaf scan) | **41–44 ms** |
| `count` full-scan (non-indexed field) | **7–10 ms** (scan-path is lock-free; each shard runs concurrently) |
| `count` indexed + secondary filter | **16–48 ms** |
| `find` limit 10 — any indexed op | **2–4 ms** |
| `find` limit 10 — full scan on non-indexed | **2–3 ms** (Zone A probe + typed compare) |
| `find` indexed + secondary filter | **2–3 ms** |
| `aggregate count` (metadata) | **3 ms** |
| `aggregate` where indexed-eq | **12–21 ms** |
| `aggregate` where indexed range | **54–114 ms** |
| `aggregate` full-scan (sum/avg/min/max) | **419 ms** |
| `aggregate` group_by on full scan | **221–279 ms** |
| `aggregate` group_by + having | **326 ms** |
| `find` cursor page 1 (ASC/DESC, indexed `order_by`) | **2–4 ms** |
| `find` cursor continuation (mid-range seek) | **2–3 ms** |

All 17 search operators use indexes when available. Full scans stay fast because Zone A (24-byte metadata headers) remains resident in the page cache and typed binary records in Zone B are compared without JSON parsing.

## 4. Invoice single-threaded — 1M records, 64 fields, 14 indexes

`bench-invoice.sh 1000000 persistent`, `SPLITS=64`.

Realistic wide-object schema (~1.9 KB/record). Composite indexes include `irbmStatus+pdfSent`, `status+source`, `status+createdAt`, `status+invoiceDate`.

| Operation | Result |
|---|---|
| Bulk insert (no indexes) | **161 k/sec** (6.19 s) |
| Bulk insert (with 14 indexes) | **128 k/sec** (7.78 s) — 20 % index overhead |
| Add 14 indexes post-insert | **2.79 s** (per-shard parallel build — 14 × splits/4 workers) |
| GET ×1000 (pipelined) | **41 k ops/sec** (24 ms) |
| EXISTS ×1000 (pipelined) | **55 k ops/sec** (18 ms) |
| Indexed eq `find` (any of 14 indexes, limit 10) | **5 ms** |
| Indexed `contains` via leaf scan | 5–15 ms |
| Indexed IN (2 values) | 5 ms |
| Composite index eq / starts | 4–5 ms |
| Indexed `range` | 3 ms |
| Fetch page of 100 @ offset 5000 | 5 ms |
| Keys (first 100) | 4 ms |
| **Single DELETE ×1000 (with 14 indexes)** | **7.8 k/sec** (128 ms) — 2.7× faster vs pre-2026.05.1 |
| **Bulk DELETE ×1000** | **77 k/sec** (13 ms) — 16× faster vs pre-2026.05.1 |
| VACUUM | 6 ms |
| Disk footprint | 1.6 GB |

The delete speedups come from `bulk_del_shard_worker` and `single_delete` paths now going through the unified shard cache (`ucache_get_write` per shard). Pre-2026.05.1 they did per-call `open + flock + mmap MAP_SHARED + munmap`, paying full page-fault tax per request.

## 5. Invoice multi-threaded — 1M records, 64 fields, 14 indexes

`shard-db-bench run bench-parallel`, `SPLITS=64`. The numbers below come from the bash bench (`bench-parallel.sh`); they understate parallel throughput because the bash version forks a `shard-db query` subprocess per chunk (~10–30 ms per fork ×5 chunks). The C bench replaces the subprocess fan-out with pthreads — running it gives the more honest numbers, particularly for the parallel rows. Numbers in this section will be refreshed once `bench-parallel` C-bench output is captured at this scale.

| Scenario | Time | Throughput |
|---|---|---|
| Single JSON, 1M, no indexes | 5.92 s | **169 k/sec** |
| Single CSV, 1M, no indexes | 1.98 s | **505 k/sec** |
| Parallel JSON, 5 conns, no indexes | 3.06 s | 326 k/sec (bash; understated) |
| Parallel JSON, 5 conns, pre-existing 14 indexes | 5.27 s | **190 k/sec** (bash) |
| Parallel CSV, 5 conns, no indexes | 3.17 s | 316 k/sec (bash; understated) |
| Parallel CSV, 5 conns, pre-existing 14 indexes | 5.22 s | **191 k/sec** (bash) |
| Add 14 indexes after bulk insert | ~2.78 s | (per-shard parallel build) |
| Disk footprint (with 14 indexes) | 1.6 GB |

The K/V parallel C-bench results in §2 above show parallel beating single by ~1.4× at this hardware, so the real invoice parallel numbers are likely ~2× higher than the table shows for the no-index parallel rows. Indexed parallel is bottlenecked on phase-4 per-(field, shard) btree merges and benefits less from pthreads-vs-subprocess.

### Indexed bulk-insert chunk-size tuning

The per-shard btree layout (2026.05.1+) makes indexed bulk-insert sensitive to the *number* of bulk-insert REQUESTS, because each request triggers a sequential `bulk_merge` cycle per (field, shard). Cumulative extract work scales `O(R²)` where R is request count.

**Recommended at 1M+ records:** prefer **fewer, larger** bulk-insert calls. `requests ≈ N / 200_000` with `5 ≤ connections` remains a sensible floor: each request triggers one merge cycle per (field, shard), so packing more rows per request keeps the cycle count down. For non-indexed data loads, parallel always pays off (see §2); for indexed loads, parallel still helps, just by less because phase-4 dominates.

### Load-then-index for static schemas

For static-schema bulk loads at 1M+ records, the load-then-index pattern is competitive with pre-existing-indexes parallel and avoids the merge-into-existing penalty:

| Pattern | 1M × 14 idx total time | Throughput |
|---|---|---|
| Load CSV (single-conn) → add-indexes | 1.98 s + 2.78 s = 4.76 s | 210 k/sec |
| Pre-existing indexes (parallel CSV, 5 conns) | 5.22 s | 191 k/sec |

Load-then-index wins on this bash measurement; the C bench at parallel-conn invoice will tell us whether parallel-with-pre-existing-indexes catches up at that path's true throughput. Either way, **load-then-index is the simpler, safer recommendation for batch ingest** of static schemas because it avoids the per-(field, shard) merge cycle entirely. **Use pre-existing indexes for streaming workloads** where add-index isn't an option.

## Disk footprint

Per-shard btree layout adds ~25 % to indexed-object disk usage vs pre-2026.05.1 (1.3 → 1.6–1.7 GB on the invoice schema). Sources:

1. Each btree starts at `2 × bt_page_size = 8 KB`. With 14 indexes × 16 idx shards = 224 trees minimum, that's ~1.8 MB of header overhead before any data (vs 14 × 8 KB = 112 KB for the old single-tree layout).
2. Reduced prefix-compression effectiveness: each leaf page has 1/16 the entries to share prefixes with, so per-entry compression savings drop ~15–25 %.
3. Page-allocation rounding: each btree's pages are 4 KB; trailing slack accumulates across 16× more trees.

Real space cost on production datasets typically lands at **+20–30 %** vs the legacy layout.

## Notes

- **File-descriptor limit.** At `SPLITS ≥ 512`, `ucache_grow_shard` briefly holds 2 fds per shard during migration, so peak can hit ~8,256 fds at the default `FCACHE_MAX=4096`. The server auto-raises its soft limit to the hard limit at startup (no privilege needed); if the hard limit itself is too low (shells default to 1024 on many distros), the startup WARN tells you exactly what to put in `/etc/security/limits.conf` or as `LimitNOFILE=` in a systemd unit.

- **CSV vs JSON.** CSV bulk insert is faster because the CSV path parses directly against the mmap'd file via `(ptr, len)` spans with zero per-line memcpy, while the JSON path materializes a `JsonObj` per record.

### Splits tuning

Size `splits` to keep **records-per-shard in the 78K–200K sweet spot** (acceptable up to ~500K, degradation past ~1M). `create-object` defaults to `splits=16` when omitted — fine for test/demo loads, too low for anything above a few million rows. Pick from expected row count:

| Expected rows | Recommended `splits` | Records/shard at target |
|---------------|----------------------|-------------------------|
| < 1M          | 8–32                 | up to ~125K             |
| 1–10M         | 64                   | ~16K–156K               |
| 10–25M        | 128                  | ~78K–195K (optimal band) |
| 25–50M        | 256                  | ~98K–195K               |
| 50–100M       | 512                  | ~98K–195K               |
| 100–250M      | 512                  | ~200K–488K (acceptable) |
| 250–500M      | 1024                 | ~244K–488K              |
| 500M–1B       | 2048                 | ~244K–488K              |
| 1–4B          | 4096 (MAX_SPLITS)    | ~244K–976K (at limit)   |

Numbers are from the parallel K/V bench on 10M rows (128 splits fastest at 3.488s; 64 splits 3.605s; 256 splits 3.986s; 1024 splits 5.454s). Counter-intuitively, raising `splits` *beyond* the sweet spot slows things down even at 10 parallel connections — more shard files = more syscalls and mmap page faults per query, and shard-lock contention isn't the bottleneck at this scale. If you exceed ~1M records/shard you've saturated this design — split across multiple objects (or tenant dirs) rather than climbing past `MAX_SPLITS=4096`.

## Reproduce

```bash
./bench/bench-kv.sh 10000000                          # scenario 1 (default SPLITS=128)
./bench/bench-kv-parallel.sh 10000000 1000000 10      # scenario 2 (default SPLITS=128)
./bench/create-user-object.sh && \
  ./bench/insert-users.sh 1000000 && \
  ./bench/bench-queries.sh                            # scenario 3
./bench/bench-invoice.sh 1000000 persistent           # scenario 4 (default SPLITS=64)
./bench/bench-parallel.sh 1000000 100000 10           # scenario 5 (default SPLITS=64)
```

Scripts self-resolve to the repo root regardless of CWD and start/stop the server automatically. All scripts honour `SPLITS=N` to override their per-script default (128 for the K/V scripts, 64 for the invoice scripts — matched to the [splits sizing table](#splits-tuning) for each record count).
