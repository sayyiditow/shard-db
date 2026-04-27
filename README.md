# shard-db

[![CI](https://github.com/sayyiditow/shard-db/actions/workflows/ci.yml/badge.svg)](https://github.com/sayyiditow/shard-db/actions/workflows/ci.yml)
[![Docs](https://github.com/sayyiditow/shard-db/actions/workflows/docs.yml/badge.svg)](https://github.com/sayyiditow/shard-db/actions/workflows/docs.yml)

A file-based sharded database written in C. Started as a key/value store; now a full small-scale DB with typed binary records, B+ tree indexes, joins, aggregates, CAS, and a multi-threaded TCP server. Single static binary, no external dependencies.

**Platform:** Linux x86_64 / ARM64 (uses `epoll`, `mmap`, POSIX pthreads). Not portable to macOS or Windows without source changes. License: MIT.

## Performance

Five canonical workloads on **AMD Ryzen 7 7840U** (8C / 16T) · 32 GB · NVMe ext4 · Linux 6.19 · gcc 15.2 `-O2`. Each scenario is a standalone script in `bench/`. All numbers are from end-to-end runs with the server over TCP — **request parse, auth, encode, disk write, ACK** are all in the measurement. Nothing is bypassed.

### 1. K/V single-threaded — 10M records (`bench-kv.sh 10000000`, `SPLITS=128`)

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

### 2. K/V multi-threaded — 10M records, scaling across connections (`bench-kv-parallel.sh 10000000 1000000 10`, `SPLITS=128`)

Same schema, bulk insert fanned out across TCP connections. `SPLITS=128` is the sweet spot for 10M rows (≈78K records/shard — see the [Splits tuning note](#notes)); going to 1024 at this scale *slows* the benchmark.

| Scenario | Time | Throughput |
|---|---|---|
| Single JSON, 10M | 4.86 s | **2.06 M/sec** |
| Single CSV, 10M | 4.27 s | **2.34 M/sec** |
| **Parallel JSON, 10 conns × 1M** | **3.73 s** | **2.68 M/sec** (1.30× single) |
| **Parallel CSV, 10 conns × 1M** | **3.67 s** | **2.72 M/sec** (1.16× single) |

Shard load distribution (128 splits): avg 0.596, records stddev 1.6 %, 9 grows per shard.

**How to read these numbers.** On 16 B / 100 B records LMDB publishes ~1 M on-disk inserts/sec (embedded, no network). shard-db sustains **2.34 M/sec single-connection** (CSV) and **2.72 M/sec** across 10 connections, over TCP with CSV parsing on the server. Single-connection → parallel ratio is now only 1.16–1.30× because the single-connection path is already fast enough that CPU-bound mmap writeback is the ceiling — adding connections mostly shortens the wall clock, not the cost per record.

### 3. Queries on 1M users (`bench-queries.sh`)

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

All 17 search operators (`eq`, `neq`, `lt`, `gt`, `lte`, `gte`, `between`, `in`, `not_in`, `like`, `not_like`, `contains`, `not_contains`, `starts`, `ends`, `exists`, `not_exists`) use indexes when available. Full scans stay fast because Zone A (24-byte metadata headers) remains resident in the page cache and typed binary records in Zone B are compared without JSON parsing.

### 4. Invoice single-threaded — 1M records, 64 fields, 14 indexes (`bench-invoice.sh 1000000 persistent`, `SPLITS=64`)

Realistic wide-object schema (~1.9 KB/record). Composite indexes include `irbmStatus+pdfSent`, `status+source`, `status+createdAt`, `status+invoiceDate`.

| Operation | Result |
|---|---|
| Bulk insert (no indexes) | **118.6 k/sec** (8.43 s) |
| Bulk insert (with 14 indexes) | **83.3 k/sec** (12.00 s) — 30 % index overhead |
| Add 14 indexes post-insert | **3.85 s** (single-pass, all 14 concurrent; scan-path is lock-free + binary index keys skip ASCII render) |
| GET ×1000 (pipelined) | **40 k ops/sec** (25 ms) |
| EXISTS ×1000 (pipelined) | **55.6 k ops/sec** (18 ms) |
| Indexed eq `find` (any of 14 indexes, limit 10) | **4 ms** |
| Indexed `contains` via leaf scan | 3–25 ms |
| Indexed IN (2 values) | 5 ms |
| Composite index eq / starts | 3–4 ms |
| Indexed `range` | 3–4 ms |
| Fetch page of 100 @ offset 5000 | 5 ms |
| Keys (first 100) | 3 ms |
| Single DELETE ×1000 (with 14 indexes) | **2.83 k/sec** (353 ms) |
| Bulk DELETE ×1000 | **4.74 k/sec** (211 ms) |
| VACUUM | 5 ms |
| Disk footprint | 1.3 GB |

### 5. Invoice multi-threaded — 1M records, 64 fields, 14 indexes (`bench-parallel.sh 1000000 100000 10`, `SPLITS=64`)

Same schema, 10 connections × 100 k records each.

| Scenario | Time | Throughput |
|---|---|---|
| Single JSON, 1M, no indexes | 7.97 s | **125 k/sec** |
| Single CSV, 1M, no indexes | 4.09 s | **245 k/sec** |
| **Parallel JSON, 10 conns, no indexes** | **3.66 s** | **273 k/sec** (2.18× single-JSON) |
| Parallel JSON, 10 conns, pre-existing 14 indexes | 5.57 s | **180 k/sec** |
| **Parallel CSV, 10 conns, no indexes** | **3.34 s** | **300 k/sec** (1.23× single-CSV) |
| Parallel CSV, 10 conns, pre-existing 14 indexes | 5.10 s | **196 k/sec** |
| Add 14 indexes after parallel bulk insert | ~3.7–3.9 s | (concurrent build across all 14) |
| Disk footprint (with 14 indexes) | 1.3 GB |

**Insert WITH indexes still beats load-then-index, margin is back.** At 1M × 14 indexes: load CSV (3.34 s) + add-indexes (3.72 s) = 7.06 s → 142 k/sec; insert CSV with pre-existing indexes: 5.10 s → **196 k/sec**. Post-hoc rebuild is fast (scan-path lock-free + binary index keys) so load-then-index stays viable for evolving schemas, but for static schemas pre-existing indexes win by ~40 %. **Recommended: create indexes up front when schema is stable; load-then-index when it isn't.**

### Notes

- **File-descriptor limit.** At `SPLITS ≥ 512`, `ucache_grow_shard` briefly holds 2 fds per shard during migration, so peak can hit ~8,256 fds at the default `FCACHE_MAX=4096`. The server auto-raises its soft limit to the hard limit at startup (no privilege needed); if the hard limit itself is too low (shells default to 1024 on many distros), the startup WARN tells you exactly what to put in `/etc/security/limits.conf` or as `LimitNOFILE=` in a systemd unit.
- **Splits tuning.** Size `splits` to keep **records-per-shard in the 78K–200K sweet spot** (acceptable up to ~500K, degradation past ~1M). `create-object` defaults to `splits=16` when omitted — fine for test/demo loads, too low for anything above a few million rows. Pick from expected row count:

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
- **CSV vs JSON.** CSV bulk insert is faster because the CSV path parses directly against the mmap'd file via `(ptr, len)` spans with zero per-line memcpy, while the JSON path materializes a `JsonObj` per record.

### Reproduce

```bash
./bench/bench-kv.sh 10000000                          # scenario 1 (default SPLITS=128)
./bench/bench-kv-parallel.sh 10000000 1000000 10      # scenario 2 (default SPLITS=128)
./bench/create-user-object.sh && \
  ./bench/insert-users.sh 1000000 && \
  ./bench/bench-queries.sh                            # scenario 3
./bench/bench-invoice.sh 1000000 persistent           # scenario 4 (default SPLITS=64)
./bench/bench-parallel.sh 1000000 100000 10           # scenario 5 (default SPLITS=64)
```

Scripts self-resolve to the repo root regardless of CWD and start/stop the server automatically. All scripts honour `SPLITS=N` to override their per-script default (128 for the K/V scripts, 64 for the invoice scripts — matched to the [splits sizing table](#notes) for each record count).

## Features

- **Typed fields** -- varchar (length-prefix, max 65535 bytes), int, long, short, double, bool, byte, date, datetime, numeric (fixed-point)
- **B+ tree indexes** -- single and composite (`field1+field2`), prefix-compressed leaves
- **Zone A/B shard layout** -- metadata (24B headers) separated from payloads for fast probing
- **Dynamic shard growth** -- each shard starts at 256 slots and doubles at 50% load (no slot cap — grows as data grows); up to **4096 shard *files* per object** (`MAX_SPLITS`, the filename format `NNN.bin`). Record capacity is bounded by disk, not by shard count.
- **Find / Count / Fetch** -- 17 search operators (eq, neq, lt/gt/lte/gte, between, in, not_in, like, not_like, contains, not_contains, starts, ends, exists, not_exists); all use indexes when available
- **AND / OR criteria** -- flat arrays are implicit AND (backward-compatible); `{"or":[...]}` and `{"and":[...]}` sub-nodes compose arbitrary trees; pure-OR queries with every child indexed use a lock-free KeySet index-union (sublinear, no shard scan)
- **Aggregations** -- count, sum, avg, min, max with `group_by`, `having`, `order_by`, `limit`; typed direct-to-double (no string round-trip)
- **Joins** -- inner and left joins with multi-join chaining, by primary key or any indexed field, composite local fields, tabular output
- **Conditional writes (CAS)** -- `if_not_exists`, `if:{...}` on insert/update/delete, `bulk-update`/`bulk-delete` by criteria with dry_run
- **Field defaults** -- `default=<literal>`, `auto_create`, `auto_update`, `seq()`, `uuid()`, `random(N)`
- **Schema mutations** -- add-field, remove-field (tombstone), rename-field, vacuum --compact, vacuum --splits (reshard)
- **Bulk operations** -- bulk-insert (JSON, CSV, delimited), bulk-delete
- **Response formats** -- JSON objects (default), tabular rows (`"format":"rows"`), or raw CSV/delimited text (`"format":"csv"` with optional `"delimiter":"|"`) on find/fetch/aggregate; joins always tabular
- **Multi-tenancy** -- `dir` parameter isolates tenants, validated against allowlist
- **Authentication** -- tokens scoped three tiers (global / per-tenant / per-object) × three permission levels (`r` / `rw` / `rwx`). Global IP allowlist for server-to-server bypass.
- **Statement timeout** -- `TIMEOUT` in db.env (seconds, 0 = off), enforced via cooperative cancellation in scan loops
- **Crash safety** -- atomic flag-flip writes, msync on shutdown, recovery sweeps stale `*.new`/`*.old` on startup
- **Async logging** -- ring buffer, date-rotated info/error files, slow query log (floor 100ms)
- **Observability** -- `stats` endpoint for connections, in-flight writes, cache hit rates, slow queries
- **No external dependencies** -- xxhash bundled as header-only; only libc + pthreads

## Build

```bash
./build.sh
```

Or manually:

```bash
gcc -O2 -o shard-db src/*.c -Isrc -lpthread
```

Output goes to `build/` with a default `db.env` and `schema.conf`.

## Configuration (db.env)

Place `db.env` in the working directory where you run shard-db.

| Variable | Default | Description |
|---|---|---|
| `DB_ROOT` | `./db` | Root directory for all data |
| `PORT` | `9199` | TCP server port |
| `TIMEOUT` | `0` | Query timeout seconds (0 = none) |
| `LOG_DIR` | `./logs` | Log directory |
| `LOG_LEVEL` | `3` | 0=off, 1=error, 2=warn, 3=info, 4=debug |
| `LOG_RETAIN_DAYS` | `7` | Auto-delete logs older than N days |
| `INDEX_PAGE_SIZE` | `4096` | B+ tree page size (1024-65536) |
| `THREADS` | `0` | Parallel scan threads (0 = auto nproc) |
| `WORKERS` | `0` | Worker thread pool (0 = auto, min 4) |
| `GLOBAL_LIMIT` | `100000` | Max records returned per query |
| `MAX_REQUEST_SIZE` | `33554432` | Max request size in bytes (32 MB) |
| `FCACHE_MAX` | `4096` | Shard mmap cache capacity |
| `BT_CACHE_MAX` | `256` | B+ tree index cache capacity |
| `QUERY_BUFFER_MB` | `500` | Per-query intermediate buffer cap in MB — protects against one bad query monopolising RAM |
| `DISABLE_LOCALHOST_TRUST` | `0` | Default: 127.0.0.1/::1 bypass auth (assumes trusted loopback proxy). Set to `1` for strict mode (tokens required even same-host) |
| `TOKEN_CAP` | `1024` | Open-addressed hash-table bucket count for the token store. Bump to 4096+ if you run thousands of tokens across scopes |
| `SLOW_QUERY_MS` | `500` | Slow query log threshold in ms |

## Quick Start

```bash
# 1. Build
./build.sh

# 2. Start the server
./shard-db start

# 3. Create an object (table)
./shard-db query '{
  "mode": "create-object",
  "dir": "default",
  "object": "users",
  "splits": 16,
  "max_key": 128,
  "fields": [
    "name:varchar:100",
    "email:varchar:200",
    "age:int",
    "balance:numeric:19,2",
    "active:bool",
    "created:datetime"
  ],
  "indexes": ["email", "name"]
}'

# 4. Insert a record
./shard-db insert default users user1 '{"name":"Alice","email":"alice@example.com","age":30,"balance":"1500.75","active":true}'

# 5. Get it back
./shard-db get default users user1

# 6. Find records
./shard-db find default users '[{"field":"age","op":"gt","value":"25"}]'

# 7. Stop
./shard-db stop
```

## Common Query Recipes

Real-world patterns, each stitched from the primitives documented under **JSON API Reference** below. Send any of these as the payload to `./shard-db query '<json>'` or over the TCP protocol.

### 1. Paginated filter with projection, sorted newest first

Return paid invoices from the last 30 days, newest first, 50 per page, only the fields a dashboard needs:

```json
{"mode":"find","dir":"acme","object":"invoices",
 "criteria":[
   {"field":"status","op":"eq","value":"paid"},
   {"field":"paid_at","op":"gte","value":"20260318000000"}
 ],
 "fields":["id","customer","total","paid_at"],
 "order_by":"paid_at","order":"desc",
 "offset":0,
 "limit":50}
```

For the next page, bump `offset` by `limit`. `find` buffers all matches then sorts, so deep pagination over huge result sets pays `O(matches)`; for raw full-scan pagination on an entire object (no filter), prefer `fetch` with keyset `cursor`.

### 2. Group-by aggregate with HAVING (revenue by product)

Top 10 products by revenue, excluding any product that sold fewer than 100 units:

```json
{"mode":"aggregate","dir":"acme","object":"orders",
 "criteria":[{"field":"status","op":"eq","value":"fulfilled"}],
 "group_by":["product_sku"],
 "aggregates":[
   {"fn":"count","alias":"units_sold"},
   {"fn":"sum","field":"line_total","alias":"revenue"},
   {"fn":"avg","field":"line_total","alias":"avg_ticket"}
 ],
 "having":[{"field":"units_sold","op":"gte","value":"100"}],
 "order_by":"revenue","order":"desc",
 "limit":10}
```

Output is tabular (`{"columns":[...], "rows":[[...]]}`) — drop-in for spreadsheets or charting libraries.

### 3. Multi-join: enrich orders with user email and product title

A left-join on products (emit nulls if the SKU is missing), inner-join on users (drop orders without a known user):

```json
{"mode":"find","dir":"acme","object":"orders",
 "criteria":[{"field":"status","op":"eq","value":"paid"}],
 "join":[
   {"object":"users","local":"user_id","remote":"key",
    "as":"user","type":"inner","fields":["email","name"]},
   {"object":"products","local":"product_sku","remote":"sku",
    "as":"product","type":"left","fields":["title","price"]}
 ],
 "limit":100}
```

`remote` is either `"key"` (primary-key lookup) or any **indexed** field on the joined object. Output columns: `orders.key`, `orders.{field}`, `user.{field}`, `product.{field}`.

### 4. Safe bulk update with dry-run first

Mark all `pending` orders older than 7 days as `expired`. Dry-run first to see the blast radius, then run for real with a `limit` guard:

```json
# Dry run — returns the would-be count without writing
{"mode":"bulk-update","dir":"acme","object":"orders",
 "criteria":[
   {"field":"status","op":"eq","value":"pending"},
   {"field":"created","op":"lt","value":"20260410000000"}
 ],
 "value":{"status":"expired"},
 "limit":10000,
 "dry_run":true}
```

```json
# Execute (drop dry_run, same criteria)
{"mode":"bulk-update","dir":"acme","object":"orders",
 "criteria":[
   {"field":"status","op":"eq","value":"pending"},
   {"field":"created","op":"lt","value":"20260410000000"}
 ],
 "value":{"status":"expired"},
 "limit":10000}
```

Use CAS (`"if":{"version":42}`) on single-record updates for lock-free concurrency control. Combine with `auto_update` on a `version:int` field to track revisions.

## CLI Commands

### Lifecycle

```bash
shard-db start              # Start server in background (reads PORT from db.env)
shard-db stop               # Graceful shutdown (waits for in-flight writes)
shard-db status             # Check if running, print pid and port
shard-db server             # Start in foreground (debug mode)
```

`start` and `server` take an `flock` on `$DB_ROOT/.shard-db.lock` before accepting connections. A second `start` on the same `DB_ROOT` — even from a different directory, different port, or copied binary — fails fast with a clear error. The kernel releases the lock automatically on normal exit or crash (SIGKILL, power loss), so no stale-lock cleanup is ever needed.

### CRUD

```bash
shard-db insert <dir> <object> <key> '<json_value>'
shard-db get <dir> <object> <key>
shard-db delete <dir> <object> <key>
shard-db exists <dir> <object> <key>
shard-db size <dir> <object>
```

### Query

```bash
shard-db find      <dir> <object> '<criteria_json>' [offset] [limit] [fields]
shard-db count     <dir> <object> [criteria_json]                   # omit criteria → O(1) metadata count
shard-db aggregate <dir> <object> '<aggregates_json>' [group_by_csv] [criteria_json] [having_json]
shard-db keys      <dir> <object> [offset] [limit]
shard-db fetch     <dir> <object> [offset] [limit] [fields]
```

### Bulk Operations

```bash
shard-db bulk-insert <dir> <object> [file]
shard-db bulk-delete <dir> <object> [file]
```

### Index Management

```bash
shard-db add-index <dir> <object> <field> [-f]    # -f = force rebuild
shard-db remove-index <dir> <object> <field>      # drop index (exact name match)
```

### File Storage

```bash
shard-db put-file    <dir> <object> <local-path> [--if-not-exists]  # upload (base64 over TCP)
shard-db get-file    <dir> <object> <filename> [<out-path>]         # download (base64 over TCP)
shard-db delete-file <dir> <object> <filename>                       # remove a stored file
```

Bytes-in-JSON for remote clients. Bounded by `MAX_REQUEST_SIZE` (default 32 MB ⇒ ~24 MB effective file). For same-host callers, the JSON API also exposes `{"mode":"put-file","path":"..."}` and `{"mode":"get-file-path",...}` as zero-copy fast paths.

### Maintenance

```bash
shard-db vacuum <dir> <object>
shard-db recount <dir> <object>
shard-db truncate <dir> <object>
shard-db backup <dir> <object>
```

### Diagnostics

```bash
shard-db stats                            # Global server stats (table format)
shard-db shard-stats [dir] [object]       # Per-shard load statistics
shard-db db-dirs                          # List allowed tenant directories
shard-db vacuum-check                     # Objects needing vacuum
```

### JSON Query Mode

Any operation can be sent as a JSON query:

```bash
shard-db query '{"mode":"get","dir":"default","object":"users","key":"user1"}'
shard-db query '{"mode":"find","dir":"default","object":"users","criteria":[{"field":"age","op":"gt","value":"25"}],"limit":10}'
```

## TCP Protocol

shard-db runs a TCP server. Send one JSON object per line, terminated by newline. Each response ends with a null byte (`\0`) followed by newline.

### Sending Requests from Terminal

```bash
# Using socat
echo '{"mode":"get","dir":"default","object":"users","key":"user1"}' | socat - TCP:localhost:9199

# Using nc/ncat
echo '{"mode":"insert","dir":"default","object":"users","key":"u1","value":{"name":"Bob"}}' | nc -q1 localhost 9199

# Pipeline multiple requests over one connection
printf '{"mode":"insert","dir":"default","object":"users","key":"u1","value":{"name":"Alice"}}\n{"mode":"insert","dir":"default","object":"users","key":"u2","value":{"name":"Bob"}}\n{"mode":"get","dir":"default","object":"users","key":"u1"}\n' | socat - TCP:localhost:9199
```

### Python

```python
import socket
import json

def query(request, host="localhost", port=9199):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.sendall((json.dumps(request) + "\n").encode())
    sock.shutdown(socket.SHUT_WR)
    data = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    sock.close()
    # Strip trailing null byte
    text = data.decode().strip().rstrip("\x00").strip()
    return json.loads(text) if text else None

# Create an object
query({
    "mode": "create-object",
    "dir": "default",
    "object": "products",
    "splits": 16,
    "max_key": 128,
    "fields": ["name:varchar:100", "price:numeric:10,2", "stock:int", "active:bool"]
})

# Insert
query({
    "mode": "insert",
    "dir": "default",
    "object": "products",
    "key": "prod-001",
    "value": {"name": "Widget", "price": "29.99", "stock": 150, "active": True}
})

# Insert only if key doesn't exist
query({
    "mode": "insert",
    "dir": "default",
    "object": "products",
    "key": "prod-001",
    "value": {"name": "Widget", "price": "29.99", "stock": 150, "active": True},
    "if_not_exists": True
})

# Update with compare-and-swap
query({
    "mode": "update",
    "dir": "default",
    "object": "products",
    "key": "prod-001",
    "value": {"stock": 149},
    "if": [{"field": "stock", "op": "eq", "value": "150"}]
})

# Get
result = query({"mode": "get", "dir": "default", "object": "products", "key": "prod-001"})
print(result)

# Get with field projection
result = query({
    "mode": "get",
    "dir": "default",
    "object": "products",
    "key": "prod-001",
    "fields": "name,price"
})

# Multi-get
results = query({
    "mode": "get",
    "dir": "default",
    "object": "products",
    "keys": ["prod-001", "prod-002", "prod-003"]
})

# Find with criteria
results = query({
    "mode": "find",
    "dir": "default",
    "object": "products",
    "criteria": [
        {"field": "price", "op": "lt", "value": "50.00"},
        {"field": "active", "op": "eq", "value": "true"}
    ],
    "offset": 0,
    "limit": 20
})

# Count matching records
result = query({
    "mode": "count",
    "dir": "default",
    "object": "products",
    "criteria": [{"field": "active", "op": "eq", "value": "true"}]
})

# Bulk insert
query({
    "mode": "bulk-insert",
    "dir": "default",
    "object": "products",
    "records": [
        {"id": "prod-100", "data": {"name": "Gadget A", "price": "9.99", "stock": 500}},
        {"id": "prod-101", "data": {"name": "Gadget B", "price": "19.99", "stock": 250}}
    ]
})

# Bulk update by criteria
query({
    "mode": "bulk-update",
    "dir": "default",
    "object": "products",
    "criteria": [{"field": "stock", "op": "eq", "value": "0"}],
    "value": {"active": False},
    "limit": 1000
})

# Bulk delete by criteria (dry run first)
query({
    "mode": "bulk-delete",
    "dir": "default",
    "object": "products",
    "criteria": [{"field": "active", "op": "eq", "value": "false"}],
    "dry_run": True
})

# Pipeline multiple requests over one connection
def pipeline(requests, host="localhost", port=9199):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    payload = "".join(json.dumps(r) + "\n" for r in requests)
    sock.sendall(payload.encode())
    sock.shutdown(socket.SHUT_WR)
    data = b""
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            break
        data += chunk
    sock.close()
    results = []
    for line in data.decode().split("\x00"):
        line = line.strip()
        if line:
            results.append(json.loads(line))
    return results

# Send 1000 inserts in one TCP connection
records = [
    {"mode": "insert", "dir": "default", "object": "products",
     "key": f"bulk-{i}", "value": {"name": f"Item {i}", "price": "1.00", "stock": i}}
    for i in range(1000)
]
results = pipeline(records)
```

### Java

```java
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class ShardDbClient {

    private final String host;
    private final int port;

    public ShardDbClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String query(String json) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            OutputStream out = socket.getOutputStream();
            out.write((json + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            socket.shutdownOutput();

            InputStream in = socket.getInputStream();
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] tmp = new byte[4096];
            int n;
            while ((n = in.read(tmp)) != -1) {
                buf.write(tmp, 0, n);
            }
            // Strip trailing null byte
            String result = buf.toString(StandardCharsets.UTF_8).trim();
            if (result.endsWith("\0")) {
                result = result.substring(0, result.length() - 1).trim();
            }
            return result;
        }
    }

    /** Pipeline multiple requests over one connection */
    public String[] pipeline(String[] requests) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            OutputStream out = socket.getOutputStream();
            StringBuilder sb = new StringBuilder();
            for (String req : requests) {
                sb.append(req).append("\n");
            }
            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            out.flush();
            socket.shutdownOutput();

            InputStream in = socket.getInputStream();
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] tmp = new byte[65536];
            int n;
            while ((n = in.read(tmp)) != -1) {
                buf.write(tmp, 0, n);
            }
            String raw = buf.toString(StandardCharsets.UTF_8);
            return raw.split("\0");
        }
    }

    public static void main(String[] args) throws IOException {
        ShardDbClient db = new ShardDbClient("localhost", 9199);

        // Create object
        db.query("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"orders\","
               + "\"splits\":16,\"max_key\":128,"
               + "\"fields\":[\"customer:varchar:100\",\"total:numeric:12,2\",\"status:varchar:20\",\"created:datetime\"],"
               + "\"indexes\":[\"customer\",\"status\"]}");

        // Insert
        String res = db.query("{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"orders\","
                            + "\"key\":\"ord-001\","
                            + "\"value\":{\"customer\":\"Alice\",\"total\":\"249.99\",\"status\":\"pending\"}}");
        System.out.println(res);

        // Conditional insert (if_not_exists)
        res = db.query("{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"orders\","
                      + "\"key\":\"ord-001\","
                      + "\"value\":{\"customer\":\"Alice\",\"total\":\"249.99\",\"status\":\"pending\"},"
                      + "\"if_not_exists\":true}");
        System.out.println(res);

        // Update with CAS
        res = db.query("{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"orders\","
                      + "\"key\":\"ord-001\","
                      + "\"value\":{\"status\":\"shipped\"},"
                      + "\"if\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]}");
        System.out.println(res);

        // Get
        res = db.query("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"orders\",\"key\":\"ord-001\"}");
        System.out.println(res);

        // Find
        res = db.query("{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\","
                      + "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"shipped\"}],"
                      + "\"limit\":50}");
        System.out.println(res);

        // Multi-get
        res = db.query("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"orders\","
                      + "\"keys\":[\"ord-001\",\"ord-002\",\"ord-003\"]}");
        System.out.println(res);

        // Bulk insert
        res = db.query("{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"orders\","
                      + "\"records\":["
                      + "{\"id\":\"ord-100\",\"data\":{\"customer\":\"Bob\",\"total\":\"50.00\",\"status\":\"pending\"}},"
                      + "{\"id\":\"ord-101\",\"data\":{\"customer\":\"Carol\",\"total\":\"75.00\",\"status\":\"pending\"}}"
                      + "]}");
        System.out.println(res);

        // Count
        res = db.query("{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"orders\","
                      + "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]}");
        System.out.println(res);

        // Pipeline
        String[] results = db.pipeline(new String[]{
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"orders\",\"key\":\"ord-001\"}",
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"orders\",\"key\":\"ord-100\"}",
            "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"orders\"}"
        });
        for (String r : results) {
            System.out.println(r.trim());
        }
    }
}
```

### JavaScript (Node.js)

```javascript
const net = require("net");

function query(request, host = "localhost", port = 9199) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    let data = "";

    socket.connect(port, host, () => {
      socket.write(JSON.stringify(request) + "\n");
      socket.end();
    });

    socket.on("data", (chunk) => (data += chunk.toString()));
    socket.on("end", () => {
      const text = data.replace(/\0/g, "").trim();
      try { resolve(JSON.parse(text)); }
      catch { resolve(text); }
    });
    socket.on("error", reject);
  });
}

function pipeline(requests, host = "localhost", port = 9199) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    let data = "";

    socket.connect(port, host, () => {
      const payload = requests.map((r) => JSON.stringify(r)).join("\n") + "\n";
      socket.write(payload);
      socket.end();
    });

    socket.on("data", (chunk) => (data += chunk.toString()));
    socket.on("end", () => {
      const results = data
        .split("\0")
        .map((s) => s.trim())
        .filter(Boolean)
        .map((s) => { try { return JSON.parse(s); } catch { return s; } });
      resolve(results);
    });
    socket.on("error", reject);
  });
}

(async () => {
  // Create object
  await query({
    mode: "create-object",
    dir: "default",
    object: "events",
    splits: 16,
    max_key: 128,
    fields: [
      "type:varchar:50",
      "source:varchar:100",
      "payload:varchar:2000",
      "timestamp:datetime",
      "severity:short",
    ],
    indexes: ["type", "source"],
  });

  // Insert
  let res = await query({
    mode: "insert",
    dir: "default",
    object: "events",
    key: "evt-001",
    value: { type: "login", source: "web", payload: '{"ip":"1.2.3.4"}', severity: 1 },
  });
  console.log(res);

  // Insert only if key doesn't exist
  res = await query({
    mode: "insert",
    dir: "default",
    object: "events",
    key: "evt-001",
    value: { type: "login", source: "web" },
    if_not_exists: true,
  });
  console.log(res);

  // Get
  res = await query({ mode: "get", dir: "default", object: "events", key: "evt-001" });
  console.log(res);

  // Get with projection
  res = await query({
    mode: "get",
    dir: "default",
    object: "events",
    key: "evt-001",
    fields: "type,source",
  });
  console.log(res);

  // Multi-get
  res = await query({
    mode: "get",
    dir: "default",
    object: "events",
    keys: ["evt-001", "evt-002"],
  });
  console.log(res);

  // Find
  res = await query({
    mode: "find",
    dir: "default",
    object: "events",
    criteria: [{ field: "type", op: "eq", value: "login" }],
    limit: 100,
  });
  console.log(res);

  // Count
  res = await query({
    mode: "count",
    dir: "default",
    object: "events",
    criteria: [{ field: "severity", op: "gte", value: "3" }],
  });
  console.log(res);

  // Bulk insert
  res = await query({
    mode: "bulk-insert",
    dir: "default",
    object: "events",
    records: [
      { id: "evt-100", data: { type: "error", source: "api", severity: 5 } },
      { id: "evt-101", data: { type: "warn", source: "worker", severity: 3 } },
    ],
  });
  console.log(res);

  // Bulk update by criteria
  res = await query({
    mode: "bulk-update",
    dir: "default",
    object: "events",
    criteria: [{ field: "type", op: "eq", value: "error" }],
    value: { severity: 10 },
    limit: 500,
  });
  console.log(res);

  // Bulk delete (dry run)
  res = await query({
    mode: "bulk-delete",
    dir: "default",
    object: "events",
    criteria: [{ field: "severity", op: "lt", value: "2" }],
    dry_run: true,
  });
  console.log(res);

  // Pipeline 1000 inserts
  const reqs = Array.from({ length: 1000 }, (_, i) => ({
    mode: "insert",
    dir: "default",
    object: "events",
    key: `batch-${i}`,
    value: { type: "batch", source: "script", severity: 1 },
  }));
  const results = await pipeline(reqs);
  console.log(`Pipelined ${results.length} responses`);
})();
```

## JSON API Reference

All requests are JSON objects with a `"mode"` field. Responses are JSON, one per request.

### Data Operations

#### insert

```json
{"mode":"insert","dir":"<dir>","object":"<obj>","key":"<key>","value":{...}}
```

Response: `{"status":"inserted","key":"<key>"}`

Optional fields:
- `"if_not_exists": true` -- fail if key already exists
- `"if": [<criteria>]` -- fail if existing record doesn't match criteria

On condition failure: `{"error":"condition_not_met","current":{...}}`

#### update

```json
{"mode":"update","dir":"<dir>","object":"<obj>","key":"<key>","value":{...}}
```

Response: `{"status":"updated","key":"<key>"}`

Optional: `"if": [<criteria>]` -- compare-and-swap condition

#### delete

```json
{"mode":"delete","dir":"<dir>","object":"<obj>","key":"<key>"}
```

Response: `{"status":"deleted","key":"<key>"}` or `{"status":"not_found","key":"<key>"}`

Optional: `"if": [<criteria>]` -- only delete if record matches

#### get

```json
{"mode":"get","dir":"<dir>","object":"<obj>","key":"<key>"}
{"mode":"get","dir":"<dir>","object":"<obj>","key":"<key>","fields":"name,email"}
{"mode":"get","dir":"<dir>","object":"<obj>","keys":["k1","k2","k3"]}
```

Response (single): `{"key":"<key>","value":{...}}`
Response (multi): `[{"key":"k1","value":{...}},{"key":"k2","value":{...}}]`

#### exists

```json
{"mode":"exists","dir":"<dir>","object":"<obj>","key":"<key>"}
{"mode":"exists","dir":"<dir>","object":"<obj>","keys":["k1","k2"]}
```

Response (single): `{"exists":true}`
Response (multi): `{"k1":true,"k2":false}`

#### not-exists

```json
{"mode":"not-exists","dir":"<dir>","object":"<obj>","keys":["k1","k2","k3"]}
```

Response: `["k3"]` (keys that don't exist)

#### size

```json
{"mode":"size","dir":"<dir>","object":"<obj>"}
```

Response: `{"count":1000}` — when the object has tombstoned (deleted) slots yet to be vacuumed, the response also includes `"orphaned":<N>`: `{"count":1000,"orphaned":42}`.

### Query Operations

#### find

```json
{"mode":"find","dir":"<dir>","object":"<obj>","criteria":[...],
 "offset":0,"limit":50,"fields":"name,email","excludedKeys":"bot1,bot2",
 "order_by":"<field>","order":"asc",
 "format":"rows",
 "join":[...]}
```

- **Default response**: `[{"key":"k1","value":{...}},...]`
- **`format:"rows"`**: `{"columns":["key","name","email"],"rows":[["k1","Alice","alice@x.com"],...]}`
- **With `join`**: always tabular regardless of `format` — see [Joins](#joins) below.

All parameters except `mode`, `dir`, `object`, and `criteria` are optional. For full-scan queries (no indexed criterion), the server uses a parallel shard scanner.

**Sorting (`order_by` + `order`):** when `order_by` is set, matches are buffered and sorted before the `offset`/`limit` slice is emitted — `O(matches * log matches)`. Numeric field types (int/long/short/double/numeric/date/datetime/bool/byte) sort numerically; varchar sorts lexicographically. `order` is `"asc"` (default) or `"desc"`. Not compatible with `join`. Keyset pagination for sorted finds (cursor-based) is a future feature; for now pair `order_by` with `offset`.

#### count

```json
{"mode":"count","dir":"<dir>","object":"<obj>","criteria":[...]}
```

Response: `{"count":150}`

#### keys

```json
{"mode":"keys","dir":"<dir>","object":"<obj>","offset":0,"limit":100}
```

Response: `["k1","k2","k3",...]`

#### fetch

```json
{"mode":"fetch","dir":"<dir>","object":"<obj>","offset":0,"limit":50,"fields":"name,email","cursor":"last_key","format":"rows"}
```

Response: `{"results":[{"key":"k1","value":{...}},...],"cursor":"<next>|null"}`. When `format:"rows"` is set, the shape is `{"columns":[...],"rows":[...],"cursor":"<next>|null"}`. Pass the returned `cursor` back in the next request to resume; `null` means no more pages. Use `cursor` for keyset pagination (more efficient than `offset` for deep pages).

### Bulk Operations

#### bulk-insert

```json
{"mode":"bulk-insert","dir":"<dir>","object":"<obj>","records":[{"id":"k1","data":{...}},{"id":"k2","data":{...}}]}
{"mode":"bulk-insert","dir":"<dir>","object":"<obj>","file":"/path/to/data.json"}
```

Response: `{"count":2}`. If any records were rejected, also includes `"errors":<N>` and `"error":"some_records_dropped"`.

#### bulk-insert-delimited

```json
{"mode":"bulk-insert-delimited","dir":"<dir>","object":"<obj>","file":"/path/to/data.csv","delimiter":"|"}
```

Every line is a record — there is no header row. The first column is the key; the remaining columns are field values in `fields.conf` order (skipping tombstoned fields). Default delimiter is `|`.

#### bulk-delete

By key list:
```json
{"mode":"bulk-delete","dir":"<dir>","object":"<obj>","keys":["k1","k2"]}
{"mode":"bulk-delete","dir":"<dir>","object":"<obj>","file":"/path/to/keys.json"}
```

By criteria:
```json
{"mode":"bulk-delete","dir":"<dir>","object":"<obj>","criteria":[...],"limit":1000,"dry_run":true}
```

Response (by key list): `{"deleted":<N>}`.
Response (by criteria): `{"matched":<M>,"deleted":<D>,"skipped":<S>}`, or `{"matched":<M>,"deleted":0,"skipped":0,"dry_run":true}` when `dry_run:true`.

#### bulk-update

```json
{"mode":"bulk-update","dir":"<dir>","object":"<obj>","criteria":[...],"value":{...},"limit":1000,"dry_run":true}
```

Response: `{"matched":<M>,"updated":<U>,"skipped":<S>}`, or `{"matched":<M>,"updated":0,"skipped":0,"dry_run":true}` when `dry_run:true`.

### Search Operators

Used in `criteria` arrays for find, count, bulk-update, bulk-delete, and `if` conditions on insert/update/delete.

| Operator | Description | Example |
|---|---|---|
| `eq` | Exact match | `{"field":"status","op":"eq","value":"active"}` |
| `neq` | Not equal | `{"field":"status","op":"neq","value":"deleted"}` |
| `gt` | Numeric > | `{"field":"age","op":"gt","value":"30"}` |
| `lt` | Numeric < | `{"field":"age","op":"lt","value":"65"}` |
| `gte` | Numeric >= | `{"field":"score","op":"gte","value":"100"}` |
| `lte` | Numeric <= | `{"field":"score","op":"lte","value":"999"}` |
| `like` | Wildcard `%` or `*` | `{"field":"name","op":"like","value":"Ali%"}` |
| `nlike` | Negated wildcard `%` or `*` | `{"field":"name","op":"nlike","value":"test*"}` |
| `contains` | Substring match | `{"field":"bio","op":"contains","value":"engineer"}` |
| `ncontains` | No substring | `{"field":"bio","op":"ncontains","value":"spam"}` |
| `starts` | Prefix match | `{"field":"email","op":"starts","value":"admin"}` |
| `ends` | Suffix match | `{"field":"email","op":"ends","value":".org"}` |
| `in` | Any of CSV values | `{"field":"status","op":"in","value":"active,pending"}` |
| `nin` | None of CSV values | `{"field":"role","op":"nin","value":"bot,spam"}` |
| `between` | Range inclusive | `{"field":"age","op":"between","value":"18","value2":"65"}` |
| `exists` | Field not empty | `{"field":"phone","op":"exists"}` |
| `nexists` | Field empty/null | `{"field":"deleted_at","op":"nexists"}` |

Multiple criteria in the same array are ANDed together. Indexed fields (B+ tree) are used as the primary filter automatically when available.

### Aggregations

#### aggregate

```json
{"mode":"aggregate","dir":"<dir>","object":"<obj>",
 "criteria":[...],
 "group_by":["status","currency"],
 "aggregates":[
   {"fn":"count","alias":"total"},
   {"fn":"sum","field":"amount","alias":"revenue"},
   {"fn":"avg","field":"amount","alias":"avg_amount"},
   {"fn":"min","field":"amount","alias":"smallest"},
   {"fn":"max","field":"amount","alias":"largest"}
 ],
 "having":[{"field":"total","op":"gt","value":"100"}],
 "order_by":"revenue",
 "order":"desc",
 "limit":20}
```

**Functions:** `count`, `sum`, `avg`, `min`, `max`

**Without group_by** (whole-table aggregation):

```json
{"mode":"aggregate","dir":"<dir>","object":"<obj>",
 "aggregates":[{"fn":"count","alias":"total"},{"fn":"sum","field":"amount","alias":"revenue"}]}
```

Response: `{"total":5000,"revenue":1250000}`

**With group_by:**

Response: `[{"status":"active","total":3000,"revenue":800000},{"status":"pending","total":2000,"revenue":450000}]`

All parameters except `mode`, `dir`, `object`, and `aggregates` are optional:
- `criteria` -- filter records before aggregating (WHERE)
- `group_by` -- group results by field(s)
- `having` -- filter groups after aggregation (uses same operators as criteria)
- `order_by` -- sort by any aggregate alias or group_by field
- `order` -- `"asc"` (default) or `"desc"`
- `limit` -- max groups returned (defaults to `GLOBAL_LIMIT`)

### Joins

Read-only relational joins on `find`. Inner and left types, by the remote object's primary key or any indexed field. Output is always tabular (columns + rows); the `format` parameter is ignored when `join` is present.

```json
{"mode":"find","dir":"<dir>","object":"orders",
 "criteria":[{"field":"status","op":"eq","value":"paid"}],
 "join":[
   {"object":"users","local":"user_id","remote":"key",
    "as":"user","type":"inner","fields":["email","name"]},
   {"object":"products","local":"product_sku","remote":"sku",
    "as":"product","type":"left","fields":["title","price"]}
 ],
 "limit":50}
```

Per-join options:
- `object` -- remote object (must be in same `dir`)
- `local` -- driver-side field (supports composite `"country+zip"`)
- `remote` -- either `"key"` (primary-key lookup, O(1) hash) or any indexed field (O(log n) btree)
- `as` -- column prefix in output (must differ from driver object name and other `as` values)
- `type` -- `"inner"` (default) or `"left"`
- `fields` -- remote fields to include (defaults to all non-tombstoned)

Response:
```json
{
  "columns": ["orders.key","orders.amount","orders.status","orders.user_id",
              "user.email","user.name","product.title","product.price"],
  "rows": [
    ["o1",99.5,"paid","u42","a@b.c","Ana","Widget",9.99],
    ["o2",39.0,"paid","u51",null,null,"Gadget",19.5]   // left-join no-match on user
  ]
}
```

**Limit behavior:**
- Inner-only (or no) joins: `limit` applied after join — records that inner-drop don't count toward the limit
- Left joins: `limit` applied during collection (every match emits)

**Validation:**
- `remote` must be `"key"` or an indexed field — unindexed fields rejected at parse time
- `as` must not collide with driver object name or another join's `as`

### Index Management

#### add-index

```json
{"mode":"add-index","dir":"<dir>","object":"<obj>","field":"email"}
{"mode":"add-index","dir":"<dir>","object":"<obj>","field":"email","force":true}
{"mode":"add-index","dir":"<dir>","object":"<obj>","fields":["email","status","city+country"]}
```

Composite indexes use `+` to join fields: `"city+country"` indexes the concatenation of both fields.

#### remove-index

```json
{"mode":"remove-index","dir":"<dir>","object":"<obj>","field":"email"}
{"mode":"remove-index","dir":"<dir>","object":"<obj>","fields":["email","city+country"]}
```

Drops the index by exact name match (pass composite names the same way you registered them, e.g. `"city+country"`). Unlinks the `.idx` file, removes the line from `index.conf`, and invalidates caches. Returns `{"status":"not_indexed",...}` (not an error) when the field wasn't indexed — safe to call idempotently. Queries against that field fall back to full-shard scan afterwards.

### Schema Management

#### create-object

```json
{"mode":"create-object","dir":"<dir>","object":"<obj>","splits":16,"max_key":128,
 "fields":["name:varchar:100","age:int","balance:numeric:12,2","active:bool"],
 "indexes":["name","age"]}
```

- `splits` -- initial number of shard files (default 4 = `MIN_SPLITS`, max 4096 = `MAX_SPLITS`)
- `max_key` -- maximum key length in bytes (default 64, hard ceiling 1024)
- `fields` -- array of field definitions (see Field Types below)
- `indexes` -- optional array of fields to index at creation

#### add-field

```json
{"mode":"add-field","dir":"<dir>","object":"<obj>","fields":["phone:varchar:20","verified:bool"]}
```

Triggers a full object rebuild to extend the binary layout.

#### remove-field

```json
{"mode":"remove-field","dir":"<dir>","object":"<obj>","fields":["legacy_field"]}
```

Tombstones the field (bytes reserved but invisible). Use `vacuum` with `compact:true` to physically reclaim space.

#### rename-field

```json
{"mode":"rename-field","dir":"<dir>","object":"<obj>","old":"email_addr","new":"email"}
```

Renames in fields.conf and all affected indexes (including composites).

### Field Types

| Type | Spec | On-disk bytes | Notes |
|---|---|---|---|
| `varchar` | `name:varchar:N` | N + 2 | `[uint16 BE length][content]`. **N must be 1..65535.** Content bytes above `length` are unused. |
| `int` | `age:int` | 4 | Signed 32-bit, big-endian |
| `long` | `id:long` | 8 | Signed 64-bit, big-endian |
| `short` | `flags:short` | 2 | Signed 16-bit, big-endian |
| `double` | `rate:double` | 8 | IEEE 754 |
| `bool` | `active:bool` | 1 | 0=false, 1=true |
| `byte` | `level:byte` | 1 | Unsigned 8-bit |
| `date` | `dob:date` | 4 | yyyyMMdd as int32 BE |
| `datetime` | `created:datetime` | 6 | yyyyMMdd (int32 BE) + HHmmss (uint16 BE packed) |
| `numeric` | `price:numeric:P,S` | 8 | Scaled int64 BE, S decimal places (fixed-point) |
| `currency` | `amount:currency` | 8 | Alias for `numeric:19,4` |

**Note on varchar**: `varchar` without `:N` is rejected, as are `varchar:0` and `varchar:65536+`. For larger blobs, store them in the object's `files/` directory and reference by filename.

### Field Defaults

Append default modifiers to field definitions:

| Modifier | Description | Example |
|---|---|---|
| `default=<value>` | Constant default | `status:varchar:20:default=pending` |
| `default=seq(name)` | Auto-increment sequence | `id:long:default=seq(user_id)` |
| `default=uuid()` | UUID v4 hex | `token:varchar:32:default=uuid()` |
| `default=random(N)` | N random hex bytes | `salt:varchar:16:default=random(8)` |
| `auto_create` | Server datetime on insert | `created:datetime:auto_create` |
| `auto_update` | Server datetime on insert+update | `modified:datetime:auto_update` |

Defaults are applied server-side when the field is absent from the request. Client-provided values always take precedence.

### Sequences

```json
{"mode":"sequence","dir":"<dir>","object":"<obj>","name":"order_id","action":"next"}
{"mode":"sequence","dir":"<dir>","object":"<obj>","name":"order_id","action":"next","batch":100}
{"mode":"sequence","dir":"<dir>","object":"<obj>","name":"order_id","action":"current"}
{"mode":"sequence","dir":"<dir>","object":"<obj>","name":"order_id","action":"reset"}
```

### Maintenance

#### vacuum

```json
{"mode":"vacuum","dir":"<dir>","object":"<obj>"}
{"mode":"vacuum","dir":"<dir>","object":"<obj>","compact":true}
{"mode":"vacuum","dir":"<dir>","object":"<obj>","compact":true,"splits":128}
```

- No flags: fast in-place tombstone reclaim
- `compact:true`: full rebuild, reclaims removed fields, recomputes shard layout
- `splits:N`: full rebuild with new shard count

#### recount

```json
{"mode":"recount","dir":"<dir>","object":"<obj>"}
```

Rescans all shards and updates the record count.

#### truncate

```json
{"mode":"truncate","dir":"<dir>","object":"<obj>"}
```

Deletes all records. Keeps the object definition and indexes.

#### backup

```json
{"mode":"backup","dir":"<dir>","object":"<obj>"}
```

Copies the object's data directory to a timestamped backup.

### File Storage

Files are stored under `$DB_ROOT/<dir>/<obj>/files/XX/XX/<filename>`, hash-bucketed by filename. Two variants for upload and download: a remote-safe **bytes-in-JSON** path (base64) and a zero-copy **server-local path** escape hatch.

#### put-file (upload bytes — remote-safe)

```json
{"mode":"put-file","dir":"<dir>","object":"<obj>",
 "filename":"invoice.pdf",
 "data":"<base64-encoded-bytes>",
 "if_not_exists":true}
```
Response: `{"status":"stored","filename":"...","bytes":N}` or `{"error":"file exists",...}` when `if_not_exists` is set and the file already exists.

Atomic write (`.tmp` + `fsync` + `rename`). Silent overwrite by default. Size is bounded by `MAX_REQUEST_SIZE` (default 32 MB ⇒ ~24 MB effective file after base64 inflation; raise the config to lift the cap).

CLI: `./shard-db put-file <dir> <obj> <local-path> [--if-not-exists]` — reads the file, base64-encodes, sends the JSON. Works from any host with TCP access to the server.

#### put-file (server-local path — zero-copy admin fast path)

```json
{"mode":"put-file","dir":"<dir>","object":"<obj>","path":"/srv/incoming/invoice.pdf"}
```

Server reads the path directly from its own filesystem. Only useful when the caller is on the same host as the server (or shares a filesystem). No base64 overhead.

#### get-file (download bytes — remote-safe)

```json
{"mode":"get-file","dir":"<dir>","object":"<obj>","filename":"invoice.pdf"}
```
Response: `{"status":"ok","filename":"...","bytes":N,"data":"<base64>"}` or `{"error":"file not found",...}`.

CLI: `./shard-db get-file <dir> <obj> <filename> [<out-path>]` — downloads and decodes. Writes raw bytes to `<out-path>` or stdout.

#### get-file-path (server path — zero-copy admin fast path)

```json
{"mode":"get-file-path","dir":"<dir>","object":"<obj>","filename":"invoice.pdf"}
```

Returns `{"path":"<db_root>/<obj>/files/XX/XX/<filename>"}` — the server-side path as a string. No bytes returned. Only useful for callers that can read the server's filesystem directly (same host, NFS, shared volume).

#### delete-file

```json
{"mode":"delete-file","dir":"<dir>","object":"<obj>","filename":"invoice.pdf"}
```
Response: `{"status":"deleted","filename":"..."}` on success, `{"error":"file not found","filename":"..."}` if the file is absent. Same filename rules as put-file / get-file.

CLI: `./shard-db delete-file <dir> <obj> <filename>`.

#### Filename rules

Filenames must be plain basenames — no `/`, no `\`, no `..`, no control characters, ≤ 255 bytes. Path-traversal attempts are rejected with `{"error":"invalid filename"}`.

### Diagnostics

#### stats

```json
{"mode":"stats"}
{"mode":"stats","format":"table"}
```

Returns server uptime, active threads, cache hit rates, and recent slow queries.

#### shard-stats

```json
{"mode":"shard-stats","dir":"<dir>","object":"<obj>"}
{"mode":"shard-stats","dir":"<dir>","object":"<obj>","format":"table"}
{"mode":"shard-stats","format":"table"}
```

Returns per-shard record counts and load factors. Without dir/object, shows all objects.

#### vacuum-check

```json
{"mode":"vacuum-check"}
```

Lists objects where deleted records >= 10% AND >= 1000 total.

#### db-dirs

```json
{"mode":"db-dirs"}
```

Returns the list of allowed tenant directories.

### Authentication

shard-db uses token-based authentication. Localhost (127.0.0.1, ::1) is always trusted. Remote clients must include `"auth":"<token>"` in every request.

```json
{"mode":"get","dir":"default","object":"users","key":"k1","auth":"my-secret-token"}
```

Manage tokens and IPs (requires trusted IP or valid token):

```json
{"mode":"add-token","token":"my-secret-token"}
{"mode":"remove-token","token":"my-secret-token"}
{"mode":"list-tokens"}

{"mode":"add-ip","ip":"192.168.1.10"}
{"mode":"remove-ip","ip":"192.168.1.10"}
{"mode":"list-ips"}
```

Token and IP lists are persisted to `$DB_ROOT/tokens.conf` and `$DB_ROOT/allowed_ips.conf`.

## TLS Encryption

Two supported paths — pick whichever fits your ops:

1. **Native TLS** (built in, OpenSSL-backed) — single binary, single port, one db.env knob. Zero proxy required.
2. **Reverse-proxy termination** (HAProxy / nginx / stunnel) — keep shard-db plaintext on loopback, terminate TLS in the proxy. Right choice when you already have a cert pipeline (cert-manager, Vault, ACME) and a proxy fleet.

Both modes are first-class. Native is faster to set up; proxy is cleaner if you already run nginx/HAProxy for other services.

### Option 1: Native TLS (recommended for greenfield)

Set five fields in `db.env`:

```bash
export TLS_ENABLE=1
export TLS_CERT="/etc/shard-db/cert.pem"   # server cert (chain ok)
export TLS_KEY="/etc/shard-db/key.pem"     # server private key
export TLS_CA="/etc/shard-db/ca.pem"       # client-side: CA bundle to verify the server (optional, falls back to OS trust store)
export TLS_SKIP_VERIFY=0                   # client-side dev escape hatch
```

Restart the daemon. `PORT` now speaks TLS 1.3 only — plaintext clients are rejected at handshake. The CLI auto-wraps every connection with TLS using the same db.env, so `./shard-db query '{"mode":"db-dirs"}'` just works.

**Generate a self-signed cert for dev / internal**:

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout /etc/shard-db/key.pem -out /etc/shard-db/cert.pem -days 365 \
    -subj "/CN=your-host" \
    -addext "subjectAltName=DNS:your-host,IP:10.0.0.5"
chmod 600 /etc/shard-db/key.pem
```

For production: Let's Encrypt or your internal CA.

**Verify the handshake**:

```bash
echo '{"mode":"ping"}' | openssl s_client -connect your-host:9199 -tls1_3 -quiet 2>/dev/null
```

**Notes**:
- TLS 1.3 only. TLS 1.2 ClientHello gets a protocol-version alert. Every modern client lib speaks 1.3 — OpenSSL ≥1.1.1, Java 8u261+, Go 1.12+, Python 3.7+.
- Cipher policy: TLS 1.3's mandatory AEAD set (AES-128-GCM, AES-256-GCM, ChaCha20-Poly1305). No knob.
- Client identity is still token-based (existing `tokens.conf`); native TLS does not enable mTLS.
- Cert hot-reload on SIGHUP is not implemented yet — restart the daemon to rotate.
- macOS build: `brew install openssl@3` (one-time); `build.sh` finds it via `brew --prefix`.

### Option 2: Reverse-proxy termination (HAProxy / nginx / stunnel)

Keep `TLS_ENABLE=0` (default). Put a TLS-terminating reverse proxy in front. **HAProxy** is the cleanest default: purpose-built for TCP+TLS, best sustained throughput of the common options, reloads certs without dropping connections, trivial config for a single TCP backend. nginx (`stream` module) is equivalent with slightly fuller features; stunnel is the minimal option for low-traffic / dev setups.

In all three cases, the **client code is identical** — just a standard TLS socket to the proxy's port.

**1. Generate a certificate:**

```bash
# Self-signed (dev / internal network)
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes -subj '/CN=shard-db'

# Production: use Let's Encrypt or your CA
```

HAProxy wants cert and key concatenated into one PEM:
```bash
cat cert.pem key.pem > shard-db.pem
chmod 600 shard-db.pem
```

**2. Install HAProxy:**

```bash
# Debian/Ubuntu
apt install haproxy

# Arch
pacman -S haproxy

# RHEL/CentOS
yum install haproxy
```

**3. `/etc/haproxy/haproxy.cfg`:**

```
global
    maxconn 20000
    tune.ssl.default-dh-param 2048

defaults
    mode tcp
    timeout connect 5s
    timeout client  30s
    timeout server  30s

frontend shard_db_tls
    bind *:9200 ssl crt /path/to/shard-db.pem alpn h2,http/1.1
    default_backend shard_db

backend shard_db
    server db1 127.0.0.1:9199 check
```

**4. Run:**

```bash
systemctl enable --now haproxy
```

Clients connect to port 9200 with TLS. HAProxy decrypts and forwards to shard-db on localhost:9199. **Bind shard-db to 127.0.0.1** so only the proxy can reach it directly.

**5. Verify:**

```bash
# Check HAProxy is listening on the TLS port
ss -tlnp | grep 9200

# Quick TLS handshake + ping (accepts self-signed for dev)
echo '{"mode":"ping"}' | openssl s_client -connect localhost:9200 -quiet 2>/dev/null
```

> **Alternatives:** nginx with the `stream` module (`ssl` + `proxy_pass 127.0.0.1:9199;`) is equally solid — pick it if you already run nginx. stunnel is fine for low-traffic or dev setups; its config is a 6-line `.conf` file with `accept=` and `connect=`.

**Client changes** -- use TLS sockets instead of plain TCP:

```python
# Python
import ssl, socket, json

def query_tls(request, host="your-server", port=9200):
    ctx = ssl.create_default_context()
    # For self-signed certs during dev:
    # ctx = ssl.create_default_context()
    # ctx.check_hostname = False
    # ctx.verify_mode = ssl.CERT_NONE
    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock = ctx.wrap_socket(raw, server_hostname=host)
    sock.connect((host, port))
    sock.sendall((json.dumps(request) + "\n").encode())
    sock.shutdown(socket.SHUT_WR)
    data = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk: break
        data += chunk
    sock.close()
    return json.loads(data.decode().strip().rstrip("\x00").strip())
```

```javascript
// Node.js
const tls = require("tls");
function query(request, host = "your-server", port = 9200) {
  return new Promise((resolve, reject) => {
    const socket = tls.connect(port, host, () => {
      socket.write(JSON.stringify(request) + "\n");
      socket.end();
    });
    let data = "";
    socket.on("data", (chunk) => (data += chunk.toString()));
    socket.on("end", () => resolve(JSON.parse(data.replace(/\0/g, "").trim())));
    socket.on("error", reject);
  });
}
```

```java
// Java
import javax.net.ssl.SSLSocketFactory;
SSLSocketFactory sf = (SSLSocketFactory) SSLSocketFactory.getDefault();
Socket socket = sf.createSocket("your-server", 9200);
// ... same read/write logic as plain TCP client above
```

## Storage Layout

```
$DB_ROOT/
  tokens.conf                      # API tokens
  allowed_ips.conf                  # Trusted IPs
  dirs.conf                        # Allowed tenant directories
  <dir>/
    <object>/
      fields.conf                  # Field definitions
      metadata/
        counts                     # "<live> <deleted>\n"
        sequences/                 # Per-sequence counter files
      data/
        NNN.bin                    # Shard files (e.g. 000.bin..0ff.bin for 256 splits; max 4096 shards)
      indexes/
        index.conf                 # List of indexed fields
        <field>.idx                # B+ tree index file
        <field1>+<field2>.idx      # Composite index
      files/                       # Stored files (put-file)
```

### Shard File Format

Each `.bin` shard file has three regions:

```
[ShardHeader: 32 bytes]
[Zone A: slots_per_shard * 24 bytes]   -- slot metadata (hash, flag, key_len, value_len)
[Zone B: slots_per_shard * slot_size]  -- payloads (key + value bytes)
```

**ShardHeader** (32 bytes): magic `SHKV`, version, slots_per_shard, record_count, reserved.

**Zone A** slot header (24 bytes): 16-byte xxh128 hash, 2-byte flag (0=empty, 1=active, 2=deleted), 2-byte key_len, 4-byte value_len.

**Zone B** payload: key bytes immediately followed by value bytes.

Probing reads Zone A only (24 bytes per slot), touching Zone B only on match. A full linear probe touches ~3 KB (fits in L2 cache).

Shards start at 256 slots and double when load exceeds 50%. Growth is atomic (build `.new` file, rehash, rename). Max shards per object is **4096** (enforced at create-object, vacuum --splits, and growth).

## Limits

| Thing | Limit |
|---|---|
| Shards per object | 4096 (= `MAX_SPLITS`, 3-hex-digit filename) |
| Fields per schema | 256 (= `MAX_FIELDS`) |
| Key length (`max_key`) | 1024 bytes (= `MAX_KEY_CEILING`, default 64) — every slot reserves `max_key` bytes so larger caps bloat `slot_size`; UUIDs fit in 36B |
| Varchar content | 65535 bytes (uint16 length prefix) |
| Aggregates per query | 32 |
| Record size | sum of typed-field sizes, computed automatically at `create-object` time (not user-settable) |

## Tests

All tests live in `tests/`. They start/stop the server themselves and self-resolve to the repo root, so `./tests/<name>.sh` works from any CWD.

```bash
./tests/test-objlock.sh                   # Schema mutation locking + key ceiling (18)
./tests/test-rename-field.sh              # rename-field correctness             (24)
./tests/test-remove-field.sh              # remove-field + vacuum --compact      (35)
./tests/test-vacuum-addfield.sh           # vacuum + add-field interaction       (50)
./tests/test-parallel-index-integrity.sh  # Concurrent bulk-insert integrity     (23)
./tests/test-joins.sh                     # Join support                         (17)
./tests/test-cli-shortcuts.sh             # count/aggregate CLI + delete-file    (28)
./tests/test-or-logic.sh                  # OR criteria, all four shapes         (43)
./tests/test-csv-export.sh                # CSV on find/fetch/aggregate/get/keys/exists (37)
./tests/test-per-tenant-auth.sh           # Per-tenant token scoping             (27)
./tests/test-token-perms.sh               # Per-object tokens + r/rw/rwx perms   (37)
# Total: 339 tests, all pass
```

## Benchmarks

All benchmarks live in `bench/`. Seed helpers (`create-user-object.sh`, `insert-users.sh`) live next to them.

```bash
./bench/bench-queries.sh                  # find/count/aggregate on 1M users
./bench/bench-joins.sh [count]            # join throughput
./bench/bench-kv.sh / bench/bench-kv-parallel.sh # bulk insert throughput
./bench/bench-invoice.sh / bench/bench-parallel.sh # 14-index invoice scenario
```

## License

MIT
