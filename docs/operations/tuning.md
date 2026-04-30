# Tuning

What to change in `db.env` and when. Pair with [`stats`](../query-protocol/diagnostics.md) — tune from observed cache hit rates, not guesses.

## The short version

```bash
# db.env
export THREADS=0            # auto = 4 × nproc, min 4
export WORKERS=0            # auto = max(CPU count, 4)
export FCACHE_MAX=4096      # raise if shard-mmap cache hit rate < 90%; allow-list {4096, 8192, 12288, 16384}
# BT_CACHE_MAX is no longer configurable — derived as FCACHE_MAX/4 (2026.05.1+)
export MAX_REQUEST_SIZE=33554432   # 32 MB; per-connection read buffer (memory planning!)
export QUERY_BUFFER_MB=500         # per-query intermediate buffer cap (collect-hash, KeySet, etc.)
export GLOBAL_LIMIT=100000         # soft result-set cap
export SLOW_QUERY_MS=500           # slow query threshold (floor 100 ms)
export TIMEOUT=0                   # statement timeout (seconds, 0=off; per-request `timeout_ms` overrides)
export INDEX_PAGE_SIZE=4096        # B+ tree page size (power-of-2, 1024–65536)
```

Only change what the data says to change.

## THREADS — scan parallelism

Number of worker threads spawned for parallel shard scans (`find`, `count`, `aggregate`, index builds).

- **Default 0** = online CPU count (`nproc`).
- Lower if the host has other workloads sharing CPU.
- Higher rarely helps — the work is CPU-bound on mmap reads; going above physical cores causes cache thrash.

When to care: large full scans. For indexed queries (which touch tiny candidate sets), `THREADS` barely matters.

## WORKERS — server thread pool

Number of threads servicing incoming connections.

- **Default 0** = `max(online CPUs, 4)`.
- Bump for heavy pipelining / high connection concurrency.
- Each worker adds memory (`MAX_REQUEST_SIZE` buffer × number of **concurrent connections**, not workers).

When to care: `stats` shows `active_threads` consistently at the cap. Usually not the bottleneck — threads are cheap; memory and disk are the limits.

## FCACHE_MAX — unified shard mmap cache (drives `BT_CACHE_MAX` too)

Capacity (in entries, not bytes) of the shared shard mmap cache (`ucache`). Every entry is one shard's mmap region. Since 2026.05.1, `BT_CACHE_MAX` is **derived** from this as `FCACHE_MAX / 4` and is no longer configurable on its own.

- **Default 4096** (so `bt_cache` capacity = 1024).
- Strict allow-list: `{4096, 8192, 12288, 16384}`. Invalid values fall back to default with a warning.
- Each object has `splits` shards in `ucache`, plus `splits/4` per-field idx files in `bt_cache` (per-shard btree layout).
- Raise if either `ucache.hits / (hits + misses) < 90%` (read-heavy) **or** `bt_cache.hits / (hits + misses) < 90%` (indexed-query heavy).
- Lower not possible — `4096` is the floor of the allow-list.

When to care:
- Many objects × many splits, with query latency higher than expected.
- Sum `objects × avg(splits)` for ucache sizing; `objects × avg(indexes) × avg(splits/4)` for bt_cache sizing.
- Bumping `FCACHE_MAX` from 4096 → 8192 doubles both caches. With 100 objects × 64 splits × 14 indexes, the per-shard layout creates 100 × 64 + 100 × 14 × 16 = 28 800 mmap entries — comfortably above 4096; bump to 16384 for full residency.

`BT_CACHE_MAX` set in db.env is **ignored** with a stderr warning.

## MAX_REQUEST_SIZE — per-request ceiling

Maximum bytes in one JSON request line. Oversized requests are drained and rejected with `{"error":"Request too large (max N bytes)"}`.

- **Default 32 MB** (`33554432`).
- Every **active connection** allocates a read buffer this size. 100 connections × 32 MB = 3.2 GB resident.
- Raise for:
  - Large bulk-inserts (`MAX_REQUEST_SIZE / avg_record_size` = max records per request).
  - Large [file uploads](../query-protocol/files.md) (base64 inflates 4/3, so 32 MB ≈ 24 MB effective file).
- Don't raise blindly — memory cost scales with concurrency.

### Sizing rule of thumb

```
planned peak connections × MAX_REQUEST_SIZE < 50% of total RAM
```

Leave room for the `ucache`, `bt_cache`, page cache, and working memory.

## GLOBAL_LIMIT — default result limit

Default `limit` applied when a query omits one (or sets `limit ≤ 0`).

- **Default 100 000**.
- Pure fallback — there is **no server-side clamp**. A per-query `"limit": 500000` returns 500K records even with `GLOBAL_LIMIT=100000`.
- Prevents runaway queries from accidentally materializing the whole object when a caller forgets `limit`.

If you need a hard ceiling that cannot be overridden, that's not what `GLOBAL_LIMIT` is — enforce it in the application layer or behind a reverse proxy. Pair the limit-default with `QUERY_BUFFER_MB` (a real cap on intermediate-buffer memory; the query aborts if exceeded) for memory protection.

## SLOW_QUERY_MS — slow query threshold

Queries exceeding this duration get logged to the in-memory slow-query ring (visible via `stats`) and to `slow-YYYY-MM-DD.log`.

- **Default 500 ms**. Minimum 100 ms (lower values ignored).
- `0` = disable.
- Raise if normal queries routinely cross the threshold and the log is noise.

## TIMEOUT — statement timeout

Seconds before a long-running query is cancelled cooperatively. Checked every 1024 iterations inside scan loops, so precision is coarse (tens of milliseconds, not microseconds).

- **Default 0** = disabled.
- Recommended: set to a protective upper bound (e.g., 30 seconds) to prevent stuck queries blocking worker threads.
- Per-request `"timeout_ms":N` overrides for `find` / `count` / `aggregate` / `bulk-delete` / `bulk-update`. Use a tight `timeout_ms` for interactive callers without lowering the global default.
- Applies to scans + bulk-update/delete; does not apply to single-record writes.

Response on timeout: `{"error":"query_timeout"}`.

## QUERY_BUFFER_MB — per-query memory cap

Upper bound on intermediate buffers any single query can hold — collect-hash arrays, KeySets (OR union, AND intersection), aggregate hash tables, ordered-find sort buffers, bulk-delete/update key lists.

- **Default 500 MB**.
- Checked at every collection site. Exceeding triggers `{"error":"query memory buffer exceeded; narrow criteria, add limit/offset, or stream via fetch+cursor"}` and the server keeps serving.
- Peak RAM per query ≈ `QUERY_BUFFER_MB × 1` (true RSS ~10–15% higher from malloc overhead).

When to care: heavy ad-hoc analytics that legitimately need bigger working sets. Multiply by expected concurrent heavy queries when sizing the host. Pair with whole-process containment (systemd `MemoryMax=`, cgroup `memory.max`, `ulimit -v`) as a backstop.

## INDEX_PAGE_SIZE — B+ tree page size

- **Default 4096 bytes** (matches typical page cache granularity).
- Valid range 1024–65536, must be power-of-2.
- Larger pages: fewer levels (faster descent), but more data read per page (wastes I/O on small scans).
- Smaller pages: more levels, more compact per-page.

Don't change without a specific reason and a benchmark to prove it. 4096 is a sweet spot on Linux x86_64.

## Disk layout

- **ext4 / xfs** — both tested, both fine.
- **SSD** — strongly recommended. shard-db is I/O-bound on scans once beyond page cache.
- **NVMe** — helps for bulk loads; marginal for steady-state queries.

Don't use network filesystems (NFS, CIFS) for `$DB_ROOT` unless you deeply trust their mmap semantics. Latency and coherence bugs compound.

## Sizing `splits`

`splits` is the number of shard files per object, fixed at `create-object` time and changeable only via `vacuum --splits=N` (re-hashes every record). The right value is driven by **records-per-shard**, not by load factor.

**Sweet spot: 78K–200K records/shard.** Acceptable to ~500K; past ~1M you're saturating this design. (Validated on the parallel K/V bench: 10M rows at 128 splits = 78K rec/shard completed in 3.488s. 64 splits: 3.605s. 256 splits: 3.986s. 1024 splits: 5.454s — too many shards = too many small files = more syscalls per query.)

| Expected rows | Recommended `splits` | Records/shard at cap |
|---------------|----------------------|----------------------|
| up to 1M      | 8                    | ~125K                |
| 1–4M          | 16                   | ~250K                |
| 4–10M         | 32                   | ~313K                |
| 10–25M        | 64                   | ~391K                |
| 25–60M        | 128                  | ~469K                |
| 60–125M       | 256                  | ~488K                |
| 125–250M      | 512                  | ~488K                |
| 250–500M      | 1024                 | ~488K                |
| 500M–1B       | 2048                 | ~488K                |
| 1B–2B+        | 4096 (MAX_SPLITS)    | ~488K and up         |

Defaults: `create-object` with no `splits` gives **16** (fine for objects up to ~4M rows). For sub-1M-row test objects on small servers (2–4 cores), set `splits=8` explicitly — halves memory overhead vs the default and still preserves indexed-read parallelism (8/4 = 2 index shards). For bigger loads, set it explicitly up front — or re-split later with `vacuum --splits=N`.

> **The daemon will tell you when to re-split.** Run `./shard-db shard-stats <dir> <object>` periodically. When `avg_rec_per_shard` crosses **500K**, the output emits `hint: records-per-shard approaching upper band (>500K) — consider vacuum --splits=N`. Past **1M** it escalates to `hint: re-split with vacuum --splits=N`. Past 1M and already at `MAX_SPLITS=4096`, the hint switches to `partition by object` — at that scale you want multiple B+ trees, not a larger one. Tier transitions in the table above land before the 500K nag, so following the table keeps you out of the warning zone.

### What else `shard-stats` flags

Beyond the records-per-shard nags above, `shard-stats` also surfaces shard-load skew:

- `max_records > 4 × min_records` → hint: `shard load is skewed — check key distribution`. Usually means non-random keys — prefix-heavy keys like `ord_0001`, `ord_0002` hash fine, but raw sequential integers cluster badly. Inspect the per-shard table in the output.

## When to run `vacuum`

- Tombstoned / active ratio > 10% and active > 1000 → `vacuum-check` flags it. Run `vacuum` (fast in-place reclaim).
- After `remove-field` → `vacuum --compact` to shrink `slot_size`.
- Shard skew (imbalanced load) or records-per-shard past the sweet spot → `vacuum --splits N` to reshard.

`vacuum` takes a write lock for the rebuild duration. Schedule during low-traffic windows on big objects.

## Benchmarks

Benchmark scripts are at the repo root:

- `./bench-kv.sh`, `./bench-kv-parallel.sh` — insert / get / update throughput.
- `./bench-queries.sh` — find / count / aggregate latency.
- `./bench-joins.sh [count]` — join throughput.
- `./bench-invoice.sh`, `./bench-parallel.sh` — 14-index realistic scenario.

Use them to validate tuning changes empirically, not theoretically.
