# Tuning

What to change in `db.env` and when. Pair with [`stats`](../query-protocol/diagnostics.md) — tune from observed cache hit rates, not guesses.

## The short version

```bash
# db.env
export THREADS=0            # auto = online CPU count
export WORKERS=0            # auto = max(CPU count, 4)
export FCACHE_MAX=4096      # raise if shard-mmap cache hit rate < 90%
export BT_CACHE_MAX=256     # raise if B+ tree cache hit rate < 90%
export MAX_REQUEST_SIZE=33554432   # 32 MB; per-connection read buffer (memory planning!)
export GLOBAL_LIMIT=100000         # soft result-set cap
export SLOW_QUERY_MS=500           # slow query threshold
export TIMEOUT=0                   # statement timeout (seconds, 0=off)
export INDEX_PAGE_SIZE=4096        # B+ tree page size (power-of-2)
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

## FCACHE_MAX — unified shard mmap cache

Capacity (in entries, not bytes) of the shared shard mmap cache (`ucache`). Every entry is one shard's mmap region.

- **Default 4096**.
- Each object has `splits` shards; cache needs at least the working set.
- Raise if `ucache.hits / (hits + misses) < 90%` for read-heavy workloads.
- Lower if memory is tight — LRU evicts on demand.

When to care: you have many objects × many splits, and query latency is higher than expected. Sum `objects × avg(splits)` across all dirs to size.

## BT_CACHE_MAX — B+ tree mmap cache

Capacity of the per-index B+ tree mmap cache. Separate from `ucache` because indexes have different access patterns.

- **Default 256**.
- Each entry is one `.idx` file's mmap.
- Raise if `bt_cache.hits / (hits + misses) < 90%` and you have many indexed queries.

When to care: you have many objects × many indexes. With 10 objects × 5 indexes each = 50 index files, the default 256 is fine. At 1000+ index files, raise.

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

## GLOBAL_LIMIT — soft result cap

Default ceiling for query `limit` when no explicit `limit` is provided, and a hard safety cap for aggregate results.

- **Default 100 000**.
- Prevents runaway queries from materializing millions of records.
- Per-query `limit` always wins when smaller.

When to raise: you legitimately need larger result sets (e.g., data exports). Consider paginating instead.

## SLOW_QUERY_MS — slow query threshold

Queries exceeding this duration get logged to the in-memory slow-query ring (visible via `stats`) and to `slow-YYYY-MM-DD.log`.

- **Default 500 ms**. Minimum 100 ms (lower values ignored).
- `0` = disable.
- Raise if normal queries routinely cross the threshold and the log is noise.

## TIMEOUT — statement timeout

Seconds before a long-running query is cancelled cooperatively. Checked every 1024 iterations inside scan loops, so precision is coarse (tens of milliseconds, not microseconds).

- **Default 0** = disabled.
- Recommended: set to a protective upper bound (e.g., 30 seconds) to prevent stuck queries blocking worker threads.
- Does not apply to writes — only to scans.

Response on timeout: `{"error":"Query deadline exceeded"}`.

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

Defaults: `create-object` with no `splits` gives **16** (fine for test objects and anything under ~2M rows). For bigger loads, set it explicitly up front — or re-split later with `vacuum --splits=N`.

Past 4B rows you've hit `MAX_SPLITS=4096`. Partition across multiple objects or tenant dirs rather than trying to climb further — at that scale you want multiple B+ trees, not one larger one.

### What `shard-stats` tells you

```bash
shard-db shard-stats <dir> <object>
```

Shows `avg_rec_per_shard` in the header. Compare it to the table above:

- `avg_rec_per_shard > 1,000,000` → hint: `re-split with vacuum --splits=N` (or already at MAX_SPLITS: partition by object).
- `avg_rec_per_shard > 500,000` with room to grow → hint: `consider vacuum --splits=N`.
- `max_records > 4 × min_records` → hint: `shard load is skewed — check key distribution` (usually means non-random keys — prefix-heavy keys like `ord_0001`, `ord_0002` hash fine, but if you're slotting raw sequential integers, look at `cmd_shard_stats`).

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
