# Concurrency

shard-db is multi-threaded: a worker thread pool services TCP connections, scan paths parallelize across shards, and bulk-insert builds indexes in parallel. The locking model is fine-grained and avoids a global write lock.

## Lock hierarchy (bottom up)

| Scope | Lock type | Purpose |
|---|---|---|
| Per ucache entry (one shard mmap) | rwlock | Readers share; a writer takes exclusive. |
| Per object (logical) | rwlock ("objlock") | Normal ops take read; schema mutations take write. |
| Global maps (schemas, indexes, dirs) | mutex | Short-held, protects cache-lookup structures. |
| Process wide | atomic counters | `in_flight_writes`, `active_threads` — no locks, just atomics. |

## Per-ucache-entry rwlock

Every mmapped shard file has its own rwlock. This is the hot path for both reads and writes:

- **Reads** (`get`, `find`, `count`, `aggregate`, scans) take **shared** (read) on the shard they're touching. Multiple readers can scan the same shard simultaneously.
- **Writes** (`insert`, `update`, `delete`, index updates) take **exclusive** (write) on the shard they're modifying. A writer blocks readers only on that one shard.

Because records route by `hash[0..1] % splits`, an insert/update/delete touches exactly one shard. Other shards remain fully concurrent. Full scans parallelize across shards — one thread per shard group — and each thread locks only the shard it's reading.

## Per-object rwlock ("objlock")

Layered on top of the per-shard locks. Every JSON request gets classified:

- **Normal ops** (all CRUD, queries, bulk ops, index ops) → `objlock_rdlock()`. Many concurrent.
- **Schema mutations** (`add-field`, `remove-field`, `rename-field`, `vacuum --compact`, `vacuum --splits`, `truncate`) → `objlock_wrlock()`. Blocks everyone; held only for the duration of the rebuild.

This serializes schema rebuilds against everything else without holding a long-lived lock during normal traffic.

## Write drain on shutdown

`./shard-db stop` sets `server_running = 0` to refuse new connections and waits up to 30 seconds for the `in_flight_writes` atomic to reach zero. This guarantees that every write that entered the server before shutdown either committed or returned an error — no half-written records.

Reads are not drained; they're safe to abandon.

## Atomic write flag pattern

Each slot write is two steps:

1. Write the new record payload into Zone B with `flag=0`.
2. Atomically flip `flag` to `1` in Zone A.

If the process dies between 1 and 2, the slot stays invisible to readers (`flag=0` = empty). On next shard growth, it gets overwritten.

Updates replacing an existing slot payload use the same pattern: write-new-then-flip. Deletes flip `flag=1` → `flag=2` (tombstoned).

## Parallel scan workers

For any find/count/aggregate without an index, `scan_shards()` spawns `THREADS` parallel workers (default = number of online CPUs). Each worker:

1. Takes a shard group.
2. Opens each shard's ucache entry (read lock).
3. Walks Zone A linearly, loading Zone B only for candidates that match on metadata.

Aggregates accumulate into per-thread counters and fold at the end. No shared lock in the hot loop.

For indexed multi-criteria, `parallel_indexed_count` / `parallel_indexed_agg` walk the primary index's hits in parallel, with each worker filtering the remaining criteria against its slice.

## Parallel index build

`cmd_add_indexes` with multiple fields does a **single** shard scan and emits tuples to per-field sort buffers in parallel. Then builds all B+ trees in parallel (one pthread per index).

## Statement timeout

Set `TIMEOUT=<seconds>` in db.env. Every scan loop calls `query_deadline_tick()` every 1024 iterations — a coarse monotonic-clock check. When exceeded, the query returns `{"error":"Query deadline exceeded"}`. Precision is milliseconds-accurate for long scans, but the check granularity means a query finishes its current 1024-record chunk before actually stopping.

## Crash consistency

- **Writes** — atomic flag flip (see above). Crash mid-write = invisible slot.
- **Rebuilds** (vacuum, growth, schema mutations) — build `.new` file, rename atomically. Crash before rename = original intact; crash after = new file in place.
- **Startup sweep** — any leftover `.new` / `.old` files under any object are removed on startup before accepting connections.

## What's NOT transactional

shard-db is not a multi-statement ACID database. There are **no transactions across records**. CAS is per-record (`insert if_not_exists`, `update if:{...}`) and gives you optimistic concurrency control without locks.

If you need to update two records atomically, your options are:
- Write-ahead log your intent into a third record as a staging step.
- Use `bulk-update` with `criteria` — still per-record CAS, but batched.
- Combine `auto_update` with a `version:long:default=seq(...)` field and compare-and-swap on version.

## Connection scaling

Each connection runs on a worker thread (bounded by `WORKERS`, default = `max(ncpu, 4)`). The server uses `epoll` for accept and hands off ready sockets to the worker queue. Single connection is not a bottleneck — pipelining multiple JSON requests over one socket gets close to per-connection line rate.

Cache pressure: every connection allocates a `MAX_REQUEST_SIZE`-byte buffer. At the default 32 MB, 100 connections = 3.2 GB. Raise `MAX_REQUEST_SIZE` deliberately.
