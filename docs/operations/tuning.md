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
export MAX_CONCURRENT_QUERIES=0    # 0 = auto: max(4, min(nproc, 32))
export QUERY_BUFFER_MB=256         # per-query intermediate buffer cap (collect-hash, KeySet, etc.)
export INDEX_BUILD_BUDGET_MB=1024   # reindex/add-indexes peak memory per pass (default 1 GiB)
export GLOBAL_LIMIT=100000         # soft result-set cap
export SLOW_QUERY_MS=500           # slow query threshold (floor 100 ms)
export TIMEOUT=0                   # statement timeout (seconds, 0=off; per-request `timeout_ms` overrides)
export INDEX_PAGE_SIZE=4096        # B+ tree page size (power-of-2, 1024–65536)
```

Only change what the data says to change.

> **THREADS vs WORKERS at a glance:** they look symmetric but aren't. **THREADS** is the *compute* pool — the parallel-for that fans a single scan / index build across shards. **WORKERS** is the *I/O* pool — one thread per in-flight TCP request. A request arrives on a WORKER, and if it triggers a parallel scan, that scan farms shard-level work onto the THREADS pool. Sizing them is independent.

## Use explain before tuning

Before changing settings, add [`"explain":true`](../query-protocol/explain.md) to slow `find`/`count`/`aggregate` queries. The planner's chosen strategy (`leaf`, `scan`, `intersect`, `union`) and hint recommendations (`add_index`, `composite_index`, `add_trigram_index`) tell you exactly what to fix. Don't guess — inspect.

## THREADS — scan parallelism

Number of compute threads in the `parallel_for` pool used for parallel shard scans (`find`, `count`, `aggregate`), index builds, and bulk write phase 2.

- **Default 0** = `4 × online CPU count` (min 4). Yes, intentional oversubscription.
- Why 4×: scans take per-shard rwlocks. A thread-per-core pool stalls whenever a shard is briefly contended, leaving cores idle. 4× lets the kernel overlap waiters with runners. Measured ~18% faster on parallel bulk-insert vs 1× cores; no measurable downside on read-only scans (mmap reads are cheap to context-switch into).
- Lower if the host has other workloads sharing CPU — but going below `nproc` usually hurts.
- Going above 4× rarely helps — past that you're paying scheduler overhead for diminishing rwlock-overlap returns.

When to care: large full scans, parallel bulk-insert at >1M rows, indexed batch ingest. For indexed point queries (tiny candidate sets), `THREADS` barely matters.

## WORKERS — server thread pool

Number of threads in the TCP request pool. Each in-flight client request is dispatched onto one WORKER, which drives the request to completion (parsing, planning, optionally fanning out to the THREADS pool, writing the response).

- **Default 0** = `max(online CPUs, 4)`.
- The default is conservative and *not* connection-aware — at high connection concurrency you should bump this. Rule of thumb: `WORKERS ≥ p99 concurrent in-flight requests`. If clients pipeline requests on a single connection, the server still serializes them per-connection (one read at a time), so concurrent connections, not pipelined depth, drive sizing.
- Bumping WORKERS does NOT add per-worker memory unless the workers are actively servicing requests — the `MAX_REQUEST_SIZE` buffer is allocated per active connection, not per idle worker.

When to care: `stats` shows `active_threads` consistently at WORKERS cap → bump it. Usually not the bottleneck; the limits are typically memory (large requests) or disk (cold scans).

## FCACHE_MAX — v2 cache capacity (drives `BT_CACHE_MAX` too)

Capacity (in entries, not bytes) of the v2 kf and segment caches. Since 2026.05.1, `BT_CACHE_MAX` is **derived** from this as `FCACHE_MAX / 4` and is no longer configurable on its own.

- **Default 4096** (so `bt_cache` capacity = 1024).
- Strict allow-list: `{4096, 8192, 12288, 16384}`. Invalid values fall back to default with a warning.
- Each v2 object has `splits` kf shards in `kfcache` (plus its seg files in `segcache`); each indexed field has `index_splits_for(splits)` files in `bt_cache`.
- Raise if `bt_cache.hits / (hits + misses) < 90%` for indexed-query workloads.
- Lower not possible — `4096` is the floor of the allow-list.

When to care:
- Many objects × many splits, with query latency higher than expected.
- Sum `objects × avg(splits)` for kfcache sizing; `objects × avg(indexes) × avg(index_splits_for(splits))` for bt_cache sizing.
- Bumping `FCACHE_MAX` from 4096 → 8192 doubles both caches. With 100 objects × 64 splits × 14 indexes, the per-shard layout creates 100 × 64 + 100 × 14 × 8 = 17 600 mmap entries — bump to 16384 for full residency.

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

Leave room for the kf/segment caches, `bt_cache`, page cache, and working memory.

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

## MAX_CONCURRENT_QUERIES — bounded query fan-in

Hard cap on the number of queries in flight at once. Each query takes a slot at dispatch entry and releases on exit (any return path). When all slots are taken, additional requests get an immediate `{"error":"server at capacity","max_concurrent_queries":N}` so the client can retry without holding the TCP thread.

- **Default 0** = auto = `max(4, min(nproc, 32))`.
- Worst-case query-buffer RAM = `MAX_CONCURRENT_QUERIES × QUERY_BUFFER_MB`. Pick the pair so the product fits comfortably in your host's free RAM — leaving room for OS page cache (mmap'd `.bin`, `.idx`, `.bm`, `.tg` files) is what makes shard-db's working set bounded by hot-data size, not total dataset size.
- Slots are global (process-wide), not per-tenant. A noisy tenant can fill all slots; per-tenant fairness is a deferred follow-up.

| host RAM | recommended `MAX_CONCURRENT_QUERIES` | `QUERY_BUFFER_MB` | worst-case query RAM | OS cache headroom |
|---|---|---|---|---|
| 4 GB | `4` | `128` | 0.5 GB | 3 GB |
| 8 GB | `8` | `256` | 2 GB | 5.5 GB |
| **16 GB** (default target) | `16` | `256` | 4 GB | 11 GB |
| 32 GB | `24` | `256` or `512` | 6–12 GB | 18–25 GB |
| 64 GB+ | `32` (auto ceiling) | `512` or higher | 16+ GB | 45+ GB |

## QUERY_BUFFER_MB — per-query memory cap

Upper bound on intermediate buffers any single query can hold — collect-hash arrays, KeySets (OR union, AND intersection, trigram intersect), aggregate hash tables, ordered-find sort buffers, bulk-delete/update key lists.

- **Default 256 MB**.
- Checked at every collection site. Exceeding triggers `{"error":"query memory buffer exceeded; narrow criteria, add limit/offset, or stream via fetch+cursor"}` and the server keeps serving.
- Peak RAM per query ≈ `QUERY_BUFFER_MB × 1` (true RSS ~10–15% higher from malloc overhead).
- Worst-case TOTAL RAM for query buffers ≈ `MAX_CONCURRENT_QUERIES × QUERY_BUFFER_MB` — see the table above.
- Auto-tune: at server startup, if `QUERY_BUFFER_MB` is at the unchanged default (256), it auto-tunes upward to `min(25% of total RAM, 4 GB) / MAX_CONCURRENT_QUERIES` (floor 256 MB). This gives heavy ad-hoc analytics breathing room on big-RAM hosts with few slots, while keeping the multiplicative peak bounded.

When to care: heavy ad-hoc analytics that legitimately need bigger working sets. Pair with whole-process containment (systemd `MemoryMax=`, cgroup `memory.max`, `ulimit -v`) as a final backstop — `MAX_CONCURRENT_QUERIES × QUERY_BUFFER_MB` is the predicted ceiling, but the OS-level cap protects against unforeseen growth (mmap'd file pages, daemon overhead, etc.).

## INDEX_BUILD_BUDGET_MB — reindex peak memory cap

Caps peak memory of multi-field index builds (`./shard-db reindex`, `./shard-db add-index` with multiple fields). The plural `cmd_add_indexes` extracts every indexed field's keys during one parallel scan of storage; without a budget that's `O(nfields × records)` resident, which OOMs hosts on big-record + many-index schemas.

- **Default 1024 MB**.
- **Floor 64 MB**. Below this, even single-field passes can't fit reasonable working sets.
- No upper bound from the parser; cap yourself based on host RAM.

How the cap is honoured:

- The builder estimates per-field bytes from `get_live_count() × (BtEntry + partition copy + per-key malloc + glibc chunk overhead)`. Estimation knows each field's typed encoding (varchar 50 % fill, fixed types from the schema, composites by summing child ASCII widths).
- Fields are grouped into passes whose summed estimate fits the budget. Each pass keeps the existing parallel scan + parallel build machinery; only the *fields-per-pass* concurrency is bounded.
- A single field that alone exceeds the budget still runs alone (always-include-at-least-one rule) — the budget never blocks a reindex, just paces it.

When to raise:

- **Big-RAM dedicated hosts (>16 GB).** Bump to `4096` or `8192` to fit more fields per pass. **Diminishing returns:** the scan cost per record scales with `nfields_in_batch`, not the number of passes, so the saving is mostly the per-pass setup overhead (a few seconds). Validation on 25M users × 12 indexed fields: `1024 → 105 s`, `8192 → 92.7 s` (only ~13 % faster despite 8× the budget). Raise for memory headroom, not speed.

When to lower:

- **Small VPS shapes (2–4 GB total RAM).** Drop to `256` or `128` if you index varchar-heavy schemas where the default 1 GiB plus the operating daemon's working set squeezes the host. The reindex will run more passes but won't trigger swap.
- **Containers with hard memory caps** (`cgroup memory.max`). Set the budget to ~50 % of the cap so the daemon can keep serving reads with comfortable headroom during the rebuild.

Mid-rebuild crash safety is unchanged — each pass force-rebuilds its fields from scratch via `btree_idx_unlink_all`, so an interrupted reindex resumes cleanly on rerun.

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

## Sizing `splits` (v2 / slotcask)

`splits` is the number of **keyfile** shards per object, fixed at `create-object` time and changeable only via `vacuum --splits=N` (re-hashes every key). In v2 the value-bearing data lives in **append-only seg files** that grow unbounded — `splits` no longer caps record count, only how the per-object keyfile is partitioned for parallel lookup.

What `splits` actually controls:

- Number of kf shards (`<obj>/kf/NNN.kf`) — each a packed array of 24 B hash-table slots.
- Number of idx shards per indexed field (`<obj>/indexes/<field>/NNN.idx`) — capped by `index_splits_for(splits)` (the curve in `src/db/types.h`).
- Read fan-out width for full scans / aggregates (work is parallel-for'd per kf shard).

What `splits` does **not** control:

- Total data size. Seg files (`<obj>/seg/<stream>/NNNN.seg`) roll over at `SLOTCASK_SEG_MAX_BYTES` and accumulate without bound. A 100 GB object on `splits=8` is fine on disk; the only question is whether the kf shards stay snappy.

The right value is driven by **per-shard kf load** (live + tombstoned slots), not records-per-shard in the v1 sense. Each kf shard auto-resplits in-place when its slot fill crosses 75 %, doubling capacity up to `SLOTCASK_MAX_SLOTS_PER_SHARD = 16M` slots. So a single kf shard tops out at ~12.8M live entries (16M × 80 %) and ~384 MB on disk — beyond that the next insert refuses and the operator must `vacuum --splits=N` to widen the keyspace.

### Keyfile sizing — initial + ceiling per shard

Slot count per shard is tiered on `splits` (see `slotcask_default_slots_for_splits()` in `slotcask.h`):

| `splits` range | Initial slots/shard | Initial bytes/shard | Per-shard ceiling (live entries) | Per-shard ceiling (bytes) |
|----------------|--------------------:|--------------------:|---------------------------------:|--------------------------:|
| ≤ 16           | 1 048 576 (1M)      | 24 MB               | 12.8M                            | 384 MB                    |
| ≤ 128          | 262 144 (256K)      | 6 MB                | 12.8M                            | 384 MB                    |
| ≤ 1024         | 131 072 (128K)      | 3 MB                | 12.8M                            | 384 MB                    |
| ≤ 4096         | 65 536 (64K)        | 1.5 MB              | 12.8M                            | 384 MB                    |

(All tiers share the same `SLOTCASK_MAX_SLOTS_PER_SHARD = 16M` ceiling. Resplits stay in-place — each doubling rebuilds one shard, not the whole keyspace.)

### Recommended `splits` by record count

| Expected live records | Recommended `splits` | Live records / kf shard | Per-shard headroom |
|-----------------------|----------------------|------------------------:|--------------------|
| up to 1M              | 8                    | ~125K                   | ~100× before resplit ceiling |
| 1–10M                 | 16                   | 63K – 625K              | ~20× headroom |
| 10–50M                | 64                   | 156K – 781K             | ~16× headroom |
| 50–200M               | 256                  | 195K – 781K             | ~16× headroom |
| 200M–1B               | 1024                 | 195K – 977K             | ~13× headroom |
| 1B–5B                 | 2048                 | 488K – 2.4M             | ~5× headroom |
| 5B–10B                | 4096 (MAX_SPLITS)    | 1.2M – 2.4M             | ~5× headroom |
| 10B+                  | 4096, partition by object | n/a — partition the object | — |

Numbers are aimed at keeping each kf shard well below its per-shard ceiling so resplits stay cheap and concurrent inserts don't queue behind a wrlock-held doubling. The exact records/shard band is forgiving — kf lookup stays O(1) at any load below the resplit threshold.

Defaults: `create-object` with no `splits` gives **8** (fine for sub-10M objects — the ~80 % case). For 50M+ rows set `splits` explicitly per the table above; otherwise let the daemon nag you and `vacuum --splits=N` later, or turn on `AUTO_RESHARD_ENABLE=1` (see [configuration.md](../getting-started/configuration.md)) to have a nightly job do it for you automatically.

> **The daemon will tell you when to re-split.** Run `./shard-db shard-stats <dir> <object>` periodically. The hint is driven by the object's *total* live record count against the same sizing table above (the identical lookup `AUTO_RESHARD_ENABLE`'s nightly sweep uses, see [Configuration](../getting-started/configuration.md)) — it fires whenever that count recommends a bigger `splits` than the object currently has, regardless of how evenly load is spread across shards. Separately, if max/min shard skew exceeds 4× the output flags `shard load is skewed — check key distribution` — a distribution/key-hashing health check, not a sizing recommendation. At `MAX_SPLITS=4096` and still nagging (10B+ live records), partition the object instead.

### When kf size starts to matter

kf lookups are O(1) average (linear probing under ≤ 80 % load = ~1.25 probes), so a "big" kf isn't inherently slow. What does cost time:

- **Resplit duration.** A shard at the 16M-slot ceiling rebuilds in ~80–160 ms (single-threaded, holds the shard's wrlock). At smaller shard sizes it's milliseconds. The resplit blocks inserts to that one shard only; reads and inserts on every other shard continue.
- **Cold mmap.** Each kf shard is a separate mmap region in `kfcache` (sized from `FCACHE_MAX`). If your active object × shards count exceeds the cache, lookups fault in 4 KB pages from disk — ~170 slots per page, so still ~1 fault per cold lookup. Watch `kfcache.hits / (hits + misses)` in `stats`; raise `FCACHE_MAX` if < 90 %.
- **Concurrent inserts during resplit.** Resplit holds the kf shard's wrlock. If a hot shard resplits during a bulk-insert burst, writes to that shard pause. Mitigation: pick `splits` so the initial slot capacity comfortably covers your ingest rate before the first auto-resplit fires.

### What else `shard-stats` flags

Beyond the kf-fill nags above, `shard-stats` also surfaces shard-load skew:

- `max_records > 4 × min_records` → hint: `shard load is skewed — check key distribution`. Usually means non-random keys — prefix-heavy keys like `ord_0001`, `ord_0002` hash fine, but raw sequential integers cluster badly. Inspect the per-shard table in the output.

## When to run `vacuum`

- Tombstoned / live ratio > 10 % and live > 1000 → `vacuum-check` flags it. Run `vacuum` (Direction-C seg compaction — rewrites live records into fresh seg files, frees the old ones).
- After `remove-field` → `vacuum` (drops the field's bytes from every record on the rewrite).
- A single kf shard approaching the 16M-slot ceiling, or skewed shard load → `vacuum --splits=N` to widen the keyspace and rehash.

`vacuum` takes a write lock for the rebuild duration. Schedule during low-traffic windows on big objects.

## Benchmarks

Benchmarks run via the C bench harness:

```bash
./build/bin/shard-db-bench run bench-kv          # single-threaded K/V
./build/bin/shard-db-bench run bench-kv-parallel  # multi-connection scaling
./build/bin/shard-db-bench run bench-queries      # find / count / aggregate
./build/bin/shard-db-bench run bench-joins        # join throughput
./build/bin/shard-db-bench run bench-invoice      # 14-index realistic scenario
```

Override scale via env vars (`SHARD_BENCH_COUNT`, `SHARD_BENCH_TOTAL`, `SHARD_BENCH_CHUNK`, `SHARD_BENCH_USERS`, `SHARD_BENCH_ORDERS`). Full reference: [benchmarks.md](benchmarks.md).

Use them to validate tuning changes empirically, not theoretically.
