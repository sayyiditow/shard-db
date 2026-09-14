# Benchmarks

Three canonical workloads on **AMD Ryzen 7 7840U** (8C / 16T) · 32 GB · NVMe ext4 · Linux 6.19 · gcc `-O2`, refreshed **2026-09-14** on the current engine (commit windows, marker V2, path-keyed durability epochs, B3b commit-chain merge). Each scenario is a C-level bench in `src/bench/` invoked via `./build/bin/shard-db-bench run <name>`. All numbers are from end-to-end runs with the server over TCP — **request parse, auth, encode, disk write, ACK** are all in the measurement. Nothing is bypassed.

> **Numbers removed in this refresh.** The earlier published tables for `bench-queries` (1M and 25M users), `bench-parallel` (indexed multi-conn), and the pre-2026.09 K/V headlines were measured against an engine without the current per-window durability model and no longer describe it; they have been removed rather than left to mislead. Re-run the bench before quoting numbers for those workloads. Qualitative query/planner guidance lives in [indexes.md](../concepts/indexes.md) and the [query-protocol](../query-protocol/overview.md) docs.

> **Reads are strict request-response.** The single-record read benches (GET / EXISTS / UPDATE / DELETE) wait for each response before sending the next request. Real-world clients that pipeline requests on the wire (multiple in flight at once) will see meaningfully higher per-connection throughput. Treat the per-op latencies as honest single-request floors and the throughputs as the lower-bound for clients that don't pipeline.

## 1. K/V single-threaded — 10M records

`SHARD_BENCH_COUNT=10000000 SHARD_BENCH_SPLITS=16 ./build/bin/shard-db-bench run bench-kv` (bench default is splits=128 — always pin `SHARD_BENCH_SPLITS`; see the [splits guidance](#splits-bench-derived-numbers)).

Schema: **16-byte hex key, one `varchar(100)` value** — the same record shape used by LMDB / LevelDB / RocksDB `db_bench` so numbers compare directly. Unlike those embedded libraries, every request below crosses a TCP socket and goes through JSON/CSV parsing on the server.

| Operation | Throughput / Latency |
|---|---|
| Bulk insert (JSON, 10M in one request) | **1.39 M inserts/sec** (7.17 s) |
| Bulk insert (CSV, 10M in one request) | **1.78 M inserts/sec** (5.63 s) |
| **Bulk EXISTS (10K keys / request)** | **5.03 M ops/sec** (2.0 ms) |
| **Bulk DELETE (10K keys / request)** | **63 k ops/sec** (158 ms) |
| **Bulk GET (10K keys / request)** | **1.68 M ops/sec** (6.0 ms) |
| **Bulk UPDATE (10K keys / request)** | **26 k ops/sec** (379 ms) |
| GET ×10,000 (req-resp, 1 conn) | **29 k ops/sec** (p50 30µs) |
| EXISTS ×10,000 hits (req-resp) | **30 k ops/sec** (p50 30µs) |
| EXISTS ×10,000 all-miss (cold probe) | **31 k ops/sec** (p50 28µs) |
| Parallel GET (5 conns × 10k) | **135 k ops/sec** (371 ms) |
| Durable single INSERT (full ACID commit) | p50 **19.5 ms** |
| Durable single UPDATE (full ACID commit) | p50 **19.5 ms** |
| Durable single DELETE (full ACID commit) | p50 **16.5 ms** |
| Disk footprint | 1.8 GB |

**Bulk multi-key paths are 60–250× faster than single-conn.** The single-conn req-resp ceiling is ~30 µs/op (dominated by TCP framing + JSON parse/encode). One bulk request handling 10K keys at a time hits up to 5M ops/sec on multi-key reads — `bulk_lookup_in_kfshard` / `bulk_get_in_kfshard` / `bulk_upsert_in_kfshard` / `bulk_delete_in_kfshard` each acquire one kf wrlock per worker (vs per-record), batch seg I/O sorted by `(stream_id, file_id)`, and use parallel fan-out across kf shards. Use bulk wire shapes (`{"mode":"get","keys":[...]}`, `{"mode":"bulk-delete","keys":[...]}`) when you have multi-key workloads.

The durable single-op row is the price of the full per-record commit protocol (segment write + marker publish + dir fsync + kf sync + clear): ~20 ms per record, end to end. Batched inserts amortize it per window instead of per record — that difference is the entire value of `bulk-insert`.

## 2. K/V multi-threaded — 10M records, scaling across connections

`SHARD_BENCH_TOTAL=10000000 SHARD_BENCH_CHUNK=2000000 SHARD_BENCH_SPLITS=16 ./build/bin/shard-db-bench run bench-kv-parallel`.

Same schema, bulk insert fanned out across TCP connections. `SPLITS=16` (≈ core/thread count on this host) is the measured optimum at 10M rows — 625K records/shard, even distribution, kf load 0.60.

| Scenario | Time | Throughput |
|---|---|---|
| Single JSON, 10M | 6.20 s | **1.61 M/sec** |
| Single CSV, 10M | 4.80 s | **2.09 M/sec** |
| **Parallel JSON, 5 conns × 2M** | **3.41 s** | **2.93 M/sec** (1.8× single JSON) |
| **Parallel CSV, 5 conns × 2M** | **3.15 s** | **3.17 M/sec** (1.52× single CSV) ← fastest |

**How to read these numbers.** On 16 B / 100 B records LMDB publishes ~1 M on-disk inserts/sec (embedded, no network, no per-window fsync). shard-db sustains **3.17 M/sec at 5 connections**, all over TCP with CSV parsing on the server and every commit-window durability barrier in the path. Parallel keeps a ~1.5–1.8× edge over single-conn at this scale. **Use single-connection for operational simplicity, parallel for headline throughput.**

The splits you choose dominate this workload — see [Splits — bench-derived numbers](#splits-bench-derived-numbers): at 10M rows, splits=64 (the old published recommendation) is the only sizing that **loses to a single connection**.

<a id="shard-grow-pre-sizing"></a>
**Pre-grow (2026.05.x):** when `bulk-insert` receives a batch, the dispatcher reads each shard's current `slots_per_shard` and live record count, computes the smallest power-of-2 that holds (live + incoming), and grows each shard to that target once before workers start. Pre-grows run in parallel via the worker pool. The previous behaviour (worker grew its shard every time it overflowed during the insert) caused 9 incremental grows per shard at 10M records, each rebucketing the existing data; now that's 1 pre-grow per shard with zero rebucket (the shards are empty when pre-grow fires for a fresh load).

## 3. Invoice — 1M records, 64 fields, 14 indexes

`shard-db-bench run bench-invoice` (splits=8 as measured; ~1.9 KB/record wide-object schema). Composite indexes include `irbmStatus+pdfSent`, `status+source`, `status+createdAt`, `status+invoiceDate`.

| Operation | Result |
|---|---|
| Bulk insert (no indexes) | **240 k/sec** (4.11 s) |
| Bulk insert (with 14 indexes) | **130 k/sec** (7.82 s) — ~46% slower vs no-idx |
| Add 14 indexes post-insert | **4.09 s** (per-shard parallel build) |
| GET ×1000 (req-resp, 1 conn) | **26 k ops/sec** (p50 35µs) |
| EXISTS ×1000 (req-resp) | **29 k ops/sec** (p50 30µs) |
| `eq supplierId` (~10K matches, limit 10) | **0.33 ms**/query |
| `contains number` (idx leaf scan, limit 10) | 23.9 ms |
| `contains batchNumber` (idx leaf scan, limit 10) | 15.6 ms |
| `IN` indexed (2 statuses, limit 10) | **0.39 ms** |
| `eq status + range amount` (AND, limit 10) | **0.50 ms** |
| `eq status + non-indexed currency` (AND, limit 10) | 1.54 s (currency not indexed — full scan after status filter) |
| Composite `status+invoiceDate starts` (limit 10) | **0.24 ms** |
| Composite `status+source eq` (limit 10) | **0.38 ms** |
| Composite `irbmStatus+pdfSent eq` (limit 10) | **0.17 ms** |
| `RANGE invoiceDate` (gte+lte, limit 10) | **0.45 ms** |
| `RANGE createdAt` (gte+lte, limit 10) | **0.43 ms** |
| `OR` two indexed statuses | **5.6 ms** |
| Fetch page of 100 @ offset 5000 | 1.3 ms (1.0 ms with projection) |
| Keys (first 100) | 35.9 ms |
| `count` full object | **0.18 ms** |
| Single DELETE ×100 (with 14 indexes) | p50 **26.5 ms** |
| Bulk DELETE ×1000 (drops idx entries) | 336 ms |
| VACUUM | 311 ms |
| RECOUNT | **0.12 ms** (kf-header sum) |
| Disk footprint | 1.4 GB |

The non-indexed-currency full scan (1.54 s) is the one deliberately slow query in the battery — it exists to show what an unindexed predicate costs on a wide 1M-row object; add the index or narrow on an indexed field.

## Splits — bench-derived numbers

Splits-sizing guidance lives in [tuning.md → Sizing `splits`](tuning.md#sizing-splits). The numbers below are the bench raw data that guidance is built from (all `bench-kv-parallel`, 5 conns, unindexed K/V, this host):

| rows | splits=8 | splits=16 | splits=64 |
|---|---|---|---|
| 1M — parallel CSV, M rows/s | 2.21 | — | 0.34 |
| 10M — single CSV, M rows/s | 1.92 | 2.09 | 2.70 |
| 10M — parallel CSV, M rows/s | 2.93 | **3.17** | 2.48 |

Parallel ingest peaks at **splits ≈ core/thread count** and falls off on both sides. Over-splitting is the expensive direction: 1M rows at splits=64 collapsed to 0.34 M rows/s, and at 10M rows splits=64 was the only sizing that lost to a single connection — gate parallelism above core count goes unused while every request still pays 4× the per-shard commit barriers. When the live count outgrows a band, widen (`vacuum --splits=N`, `AUTO_RESHARD`) rather than having pre-provisioned wide.

## Disk footprint

Per-shard btree layout adds ~25 % to indexed-object disk usage vs pre-2026.05.1 (1.3 → 1.6–1.7 GB on the invoice schema). Sources:

1. Each btree starts at `2 × bt_page_size = 8 KB`. With 14 indexes × 16 idx shards = 224 trees minimum, that's ~1.8 MB of header overhead before any data (vs 14 × 8 KB = 112 KB for the old single-tree layout).
2. Reduced prefix-compression effectiveness: each leaf page has 1/16 the entries to share prefixes with, so per-entry compression savings drop ~15–25 %.
3. Page-allocation rounding: each btree's pages are 4 KB; trailing slack accumulates across 16× more trees.

Real space cost on production datasets typically lands at **+20–30 %** vs the legacy layout.

## Notes

- **File-descriptor limit.** At `SPLITS ≥ 512`, `ucache_grow_shard` briefly holds 2 fds per shard during migration, so peak can hit ~8,256 fds at the default `FCACHE_MAX=4096`. The server auto-raises its soft limit to the hard limit at startup (no privilege needed); if the hard limit itself is too low (shells default to 1024 on many distros), the startup WARN tells you exactly what to put in `/etc/security/limits.conf` or as `LimitNOFILE=` in a systemd unit.

- **CSV vs JSON.** CSV bulk insert is faster because the CSV path parses directly against the mmap'd file via `(ptr, len)` spans with zero per-line memcpy, while the JSON path materializes a `JsonObj` per record.

## Reproduce

```bash
./build.sh

# Each bench self-spawns its own daemon on a tmp DB_ROOT and tears down on exit.

SHARD_BENCH_COUNT=10000000 SHARD_BENCH_SPLITS=16 \
  ./build/bin/shard-db-bench run bench-kv                              # §1
SHARD_BENCH_TOTAL=10000000 SHARD_BENCH_CHUNK=2000000 SHARD_BENCH_SPLITS=16 \
  ./build/bin/shard-db-bench run bench-kv-parallel                     # §2
./build/bin/shard-db-bench run bench-invoice                           # §3 (1M invoice, splits=8)

# Or run the full suite:
./build/bin/shard-db-bench run-all
```

Scale-override env vars: `SHARD_BENCH_COUNT` (single-conn benches; default 1M), `SHARD_BENCH_SPLITS` (all benches — `bench-kv` defaults to 128), `SHARD_BENCH_TOTAL` + `SHARD_BENCH_CHUNK` (parallel benches), `SHARD_BENCH_USERS` + `SHARD_BENCH_ORDERS` (`bench-joins`). All sub-µs precision via `clock_gettime(CLOCK_MONOTONIC)`. `bench-queries`, `bench-parallel`, and `bench-joins` remain registered and runnable; their result tables were removed from this page in the 2026-09-14 refresh (see the note at the top) — re-run them before quoting numbers.
