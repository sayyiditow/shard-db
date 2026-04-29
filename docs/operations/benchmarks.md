# Benchmarks

Five canonical workloads on **AMD Ryzen 7 7840U** (8C / 16T) · 32 GB · NVMe ext4 · Linux 6.19 · gcc 15.2 `-O2`. Each scenario is a standalone script in `bench/`. All numbers are from end-to-end runs with the server over TCP — **request parse, auth, encode, disk write, ACK** are all in the measurement. Nothing is bypassed.

## 1. K/V single-threaded — 10M records

`bench-kv.sh 10000000`, `SPLITS=128`.

Schema: **16-byte hex key, one `varchar(100)` value** — the same record shape used by LMDB / LevelDB / RocksDB `db_bench` so numbers compare directly. Unlike those embedded libraries, every request below crosses a TCP socket and goes through JSON/CSV parsing on the server.

| Operation | Throughput / Latency |
|---|---|
| Bulk insert (JSON, 10M in one request) | **1.97 M inserts/sec** (5.08 s) |
| Bulk insert (CSV, 10M in one request) | **2.39 M inserts/sec** (4.19 s) |
| GET ×10,000 (pipelined, 1 conn) | **22.1 k ops/sec** (453 ms) |
| EXISTS ×10,000 hits (pipelined) | **22.8 k ops/sec** (439 ms) |
| EXISTS ×10,000 all-miss (cold probe) | **66.7 k ops/sec** (150 ms) |
| UPDATE ×10,000 (pipelined) | **17.8 k ops/sec** (561 ms) |
| DELETE ×10,000 (pipelined) | **9.05 k ops/sec** (1.11 s) |
| Parallel GET (5 conns × 10k) | **91.2 k ops/sec** (548 ms) |
| Parallel UPDATE (5 conns × 10k) | **62.9 k ops/sec** (795 ms) |
| Disk footprint | 2.3 GB |

## 2. K/V multi-threaded — 10M records, scaling across connections

`bench-kv-parallel.sh 10000000 1000000 10`, `SPLITS=128`.

Same schema, bulk insert fanned out across TCP connections. `SPLITS=128` is the sweet spot for 10M rows (≈78K records/shard — see the [splits sizing table](#splits-tuning)); going to 1024 at this scale *slows* the benchmark.

| Scenario | Time | Throughput |
|---|---|---|
| Single JSON, 10M | 4.86 s | **2.06 M/sec** |
| Single CSV, 10M | 4.27 s | **2.34 M/sec** |
| **Parallel JSON, 10 conns × 1M** | **3.73 s** | **2.68 M/sec** (1.30× single) |
| **Parallel CSV, 10 conns × 1M** | **3.67 s** | **2.72 M/sec** (1.16× single) |

Shard load distribution (128 splits): avg 0.596, records stddev 1.6 %, 9 grows per shard.

**How to read these numbers.** On 16 B / 100 B records LMDB publishes ~1 M on-disk inserts/sec (embedded, no network). shard-db sustains **2.34 M/sec single-connection** (CSV) and **2.72 M/sec** across 10 connections, over TCP with CSV parsing on the server. Single-connection → parallel ratio is now only 1.16–1.30× because the single-connection path is already fast enough that CPU-bound mmap writeback is the ceiling — adding connections mostly shortens the wall clock, not the cost per record.

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
| Bulk insert (no indexes) | **117 k/sec** (8.55 s) |
| Bulk insert (with 14 indexes) | **90 k/sec** (11.13 s) — 23 % index overhead |
| Add 14 indexes post-insert | **2.85 s** (per-shard parallel build — 14 × splits/4 workers) |
| GET ×1000 (pipelined) | **42 k ops/sec** (24 ms) |
| EXISTS ×1000 (pipelined) | **48 k ops/sec** (21 ms) |
| Indexed eq `find` (any of 14 indexes, limit 10) | **5 ms** |
| Indexed `contains` via leaf scan | 5–15 ms |
| Indexed IN (2 values) | 5 ms |
| Composite index eq / starts | 4–5 ms |
| Indexed `range` | 3 ms |
| Fetch page of 100 @ offset 5000 | 5 ms |
| Keys (first 100) | 4 ms |
| **Single DELETE ×1000 (with 14 indexes)** | **7.8 k/sec** (129 ms) — 2.7× faster vs pre-2026.05.1 |
| **Bulk DELETE ×1000** | **77 k/sec** (13 ms) — 16× faster vs pre-2026.05.1 |
| VACUUM | 8 ms |
| Disk footprint | 1.6 GB |

The delete speedups come from `bulk_del_shard_worker` and `single_delete` paths now going through the unified shard cache (`ucache_get_write` per shard). Pre-2026.05.1 they did per-call `open + flock + mmap MAP_SHARED + munmap`, paying full page-fault tax per request.

## 5. Invoice multi-threaded — 1M records, 64 fields, 14 indexes

`bench-parallel.sh 1000000 200000 5`, `SPLITS=64`.

Same schema, **5 connections × 200 k records each** — the sweet spot for indexed bulk inserts at this scale (see chunk-size tuning below).

| Scenario | Time | Throughput |
|---|---|---|
| Single JSON, 1M, no indexes | 8.20 s | **122 k/sec** |
| Single CSV, 1M, no indexes | 4.20 s | **238 k/sec** |
| **Parallel JSON, 5 conns, no indexes** | **3.91 s** | **256 k/sec** |
| Parallel JSON, 5 conns, pre-existing 14 indexes | 6.21 s | **161 k/sec** |
| **Parallel CSV, 5 conns, no indexes** | **3.62 s** | **276 k/sec** |
| Parallel CSV, 5 conns, pre-existing 14 indexes | 5.66 s | **177 k/sec** |
| Add 14 indexes after parallel bulk insert | ~2.85 s | (per-shard parallel build) |
| Disk footprint (with 14 indexes) | 1.7 GB |

### Indexed bulk-insert chunk-size tuning

The per-shard btree layout (2026.05.1+) makes indexed bulk-insert sensitive to the *number* of bulk-insert REQUESTS, because each request triggers a sequential `bulk_merge` cycle per (field, shard). Cumulative extract work scales `O(R²)` where R is request count. Measured on this 1M dataset:

| Shape | Requests | TEST 3 (JSON+idx) | TEST 5 (CSV+idx) |
|---|---|---|---|
| 10 conn × 100 k | 10 | 7.34 s | 7.04 s |
| **5 conn × 200 k** | **5** | **6.21 s** | **5.66 s** |
| 5 conn × 100 k (queued) | 10 | 8.35 s | 6.42 s |
| 1 × 1 M (single call) | 1 | 11.13 s (single-thread) | — |

**Bigger chunks = fewer merge cycles. 5 connections × 200 k each is the sweet spot for 1M records.** Connection count above 5 doesn't speed up phase-2 data writes meaningfully (16-core box doesn't saturate at 5 writers), and pushing chunk count up linearly hurts phase-4 merge cost. Recommended for indexed bulk-insert: **`requests ≈ N / 200_000` rounded down, with `5 ≤ connections ≤ requests`**.

### Load-then-index now wins for static schemas

With the per-shard layout, post-hoc add-indexes parallelises 14× wider (14 fields × splits/4 shards = 14 × 16 = 224 workers from a single-pass scan), making it 25–30 % faster than pre-2026.05.1. At 1M × 14 indexes: load CSV (3.62 s) + add-indexes (2.85 s) = **6.47 s** → 155 k/sec. Insert CSV with pre-existing indexes: 5.66 s → 177 k/sec. Pre-existing-indexes still wins on absolute throughput by ~14 %, but the gap shrunk; **load-then-index is preferred when feasible** because the merge-into-existing path scales worse with parallelism.

**Recommended:** load-then-index for static schemas at 1M+ records; pre-existing indexes for streaming workloads.

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
