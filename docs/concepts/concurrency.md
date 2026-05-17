# Concurrency

shard-db is multi-threaded: a worker thread pool services TCP connections, scan paths parallelize across shards, and bulk writes parallelize across streams. The locking model is fine-grained and avoids a global write lock.

## Lock hierarchy (bottom up)

| Scope | Lock type | Purpose |
|---|---|---|
| Per kfcache entry (one kf shard mmap) | rwlock | Readers share; a writer takes exclusive. Commits go through here. |
| Per segcache entry (one seg file mmap) | rwlock | Routine record writes take rdlock (each owns a unique offset); eviction/recovery takes wrlock. |
| Per bt_cache entry (one btree mmap) | rwlock | Same model, separate cache. One entry per per-shard idx file. |
| Per stream (one append lane) | mutex + try-lock pool | Tail reservation serialised per stream; free-pool consumers use try-lock. |
| Per object (logical) | rwlock ("objlock") | Normal ops take read; schema mutations take write. |
| Global maps (schemas, indexes, dirs, slotcask registry) | mutex | Short-held, protects cache-lookup structures. |
| Process wide | atomic counters | `in_flight_writes`, `active_threads`, `server_running` — no locks, just atomics. |

## Per-kfcache-entry rwlock — the commit lock

Every mmapped keyfile shard has its own rwlock. This is the hot path for both reads and writes:

- **Reads** (`get`, `find`, `count`, `aggregate`, scans) take **shared** (rdlock) on the kf shard they're touching. Multiple readers can probe the same shard simultaneously.
- **Writes** (`insert`, `update`, `delete`, kf auto-resplit) take **exclusive** (wrlock) on the shard they're modifying. A writer blocks readers only on that one shard.

Because records route by `hash[0..1] % splits`, every CRUD operation touches exactly one kf shard. Other shards remain fully concurrent. Full scans parallelize across shards — one task per shard — and each task locks only the shard it's reading.

The commit itself is a single `__atomic_store_n` on the slot's trailing 8 bytes (`flag + stream_id + file_id + offset`) while the wrlock is held. Other writers to *this* shard are blocked; readers on *other* shards continue unaffected.

## Per-segcache-entry rwlock — the value path

Segment writes use a different pattern. Each writer:

1. Reserves a `(stream_id, file_id, offset)` from the active segment's tail under the stream's mutex (a single contended mutex per stream, held for microseconds).
2. Takes **rdlock** on the segcache entry for the chosen segment.
3. Writes its record bytes at the reserved offset (no overlap with any other writer — each owns a unique region).
4. Releases rdlock.

The segcache **wrlock** is reserved for cache eviction (mmap teardown) and recovery scans. The rdlock-during-write pattern is safe because the kf commit is the publication step: until the atomic 8B store on the kf slot completes, no reader knows the segment bytes exist. After the store, readers acquire the same segcache rdlock to read them — but by then the bytes are fully written.

This means a typical concurrent-insert workload sees **two locks per write**, both shared: kf-shard wrlock (held for the slot store and counter bump, ~microseconds) and seg rdlock (held for the memcpy, ~microseconds). Different writers to different streams + different shards run truly in parallel.

## Per-bt_cache-entry rwlock (per-shard btree, 2026.05.1+)

Each indexed field is sharded into `index_splits_for(splits)` btree files (`<obj>/indexes/<field>/NNN.idx`). Every btree file has its own rwlock — same model as kfcache, separate cache (`BT_CACHE_MAX = FCACHE_MAX/4`, derived). Writes route by record hash to a single idx-shard; reads fan out across all shards in parallel via the `parallel_for` worker pool.

This was the central reason for the per-shard layout. Pre-2026.05.1, a single `<field>.idx` file meant `bulk_build` (which truncates and rewrites the whole file) raced against in-flight readers holding an mmap of intermediate state. Per-file rwlocks give writers and readers proper isolation, and the parallel fan-out turns indexed lookups into N-way concurrent btree probes for free.

## Per-stream mutex + free pool

Each of the (up-to-16) write streams has:

- **`rotation_lock`** — the mutex that serialises tail reservation in the active segment. Held only long enough to bump `reserve_off` by `(24 + klen + vlen)` bytes and check for segment rotation (≥ 128 MB → start a new seg file). Microseconds per write.
- **`pool_lock` + free_slots[]** — the snake-game pool of tombstoned slots ready for reuse. Consumers use `pthread_mutex_trylock`; only one consumer at a time. If the trylock fails, the writer falls through to tail reservation. The pool's been kept lock-light because contention here regresses sustained-delete workloads.

Two different writers hashing to the same stream contend on `rotation_lock` briefly. Two writers hashing to different streams don't share a stream-level lock at all — they only contend in kfcache, and only if their target shards happen to overlap (which is independent of stream choice — `shard_id = hash[0..1] % splits`, `stream_id = hash[15] % streams`).

## Per-object rwlock ("objlock")

Layered on top of the per-shard locks. Every JSON request gets classified:

- **Normal ops** (all CRUD, queries, bulk ops, index ops) → `objlock_rdlock()`. Many concurrent.
- **Schema mutations** (`add-field`, `remove-field`, `rename-field`, `vacuum --compact`, `vacuum --splits`, `truncate`) → `objlock_wrlock()`. Blocks everyone; held only for the duration of the rebuild.

This serializes schema rebuilds against everything else without holding a long-lived lock during normal traffic.

## Write drain on shutdown

`./shard-db stop` sets `server_running = 0` (atomic) to refuse new connections and waits up to 30 seconds for the `in_flight_writes` atomic to reach zero. This guarantees that every write that entered the server before shutdown either committed or returned an error — no half-written records.

Reads are not drained; they're safe to abandon mid-scan.

## Commit semantics (v2)

A v2 write is sequenced as:

1. Reserve a segment offset under the stream's `rotation_lock`.
2. Take seg rdlock; write `[24B header][key][value]` to the reserved offset; release seg rdlock.
3. Take kf wrlock for the target shard; linear-probe to the slot for this hash.
4. **Commit:** `__atomic_store_n` on the slot's trailing 8 bytes — transitions `(flag=0|2, _, _, _)` to `(flag=1, stream_id, file_id, offset)`. Acquire/release ordering pairs with the reader's `__atomic_load_n` on the same bytes.
5. Bump `header.total` (and `header.deleted--` if this was a tombstone reuse).
6. Release kf wrlock.

If the process dies between steps 2 and 4, the segment bytes are orphaned (no kf slot points at them) but no record is visible-but-torn. If it dies after step 4, the record is durable in the page cache.

Tombstone (`delete`) is the same sequence with `flag=2` and no segment write. Update is read-old-then-write-new (the old segment bytes become orphaned + reclaimable by vacuum or the snake-game pool).

## Parallel scan workers

For any `find` / `count` / `aggregate` without an index, the scan path spawns `THREADS` parallel workers (default `4 × nproc`). Each worker:

1. Takes a kf shard from the work queue.
2. Acquires the kf shard's rdlock; walks the slot array; for each `flag=1` slot, follows `(stream_id, file_id, offset)` into the segcache (rdlock).
3. Runs `match_typed()` against the segment payload — zero-malloc byte compares.

Aggregates accumulate into per-thread counters and fold at the end. No shared lock in the hot loop.

For indexed multi-criteria, the planner walks the primary index's hits in parallel via `parallel_for`, with each worker filtering remaining criteria against its slice. AND-intersect (`PRIMARY_INTERSECT`) builds candidate KeySets in parallel — one per indexed leaf — then intersects them lock-free via xxh128-keyed open addressing.

## Parallel index build

`cmd_add_indexes` with multiple fields does a **single** kf-and-seg scan and emits tuples to per-field sort buffers in parallel. Then runs **one worker per indexed field** (`idx_build_field_worker`) — each worker buckets entries by idx-shard locally and merges them sequentially. Replaces the pre-2026.05.1 dispatch shape (14 fields × N idx-shards = 100+ tasks) with a flat 14-task fan-out — fewer queue contention points, same total work.

The adaptive batcher introduced in 2026.05.3 groups fields into passes that fit `INDEX_BUILD_BUDGET_MB` — large-record schemas at 25M+ no longer OOM the host during reindex.

## Statement timeout

Set `TIMEOUT=<seconds>` in db.env for a global default, or pass `"timeout_ms":N` per request to override. Every scan loop calls `query_deadline_tick()` every 1024 iterations — coarse monotonic-clock check via `_Atomic int timed_out`. When exceeded, the query returns `{"error":"query_timeout"}`. Precision is millisecond-accurate for long scans; the check granularity means a query finishes its current 1024-record chunk before actually stopping.

## Per-query memory cap

`QUERY_BUFFER_MB` (default 500) caps the intermediate buffers any single query can hold. Checked at every collection site (ordered find buffer, aggregate hash tables, OR KeySet, AND-intersection KeySets, bulk-delete/update key list, btree hash collection). When exceeded → `{"error":"query memory buffer exceeded; narrow criteria, add limit/offset, or stream via fetch+cursor"}`. Prevents one bad query from monopolising RAM. Pair with whole-process containment (`MemoryMax=`, cgroup `memory.max`) as a backstop.

## Crash consistency

- **Writes** — atomic 8B store on the kf slot is the commit (see above). Crash before that = invisible record + orphaned segment bytes. Crash after = durable record.
- **kf resplits** — build `<NNN>.kf.new`, rename atomically. Crash before rename = original intact (staged file unlinked on startup). Crash after = new file in place.
- **Direction-C seg compaction** — each record's migration is a single `kf_repoint_at_slot` atomic store. Crash mid-compaction leaves a partially-drained donor seg, which the next vacuum picks up.
- **Schema mutations** (vacuum --compact, add-field, etc.) — build the new tree under `.new` paths, rename atomically. Same as v1.
- **Startup sweep** — any leftover `.new` files under any object are unlinked on startup before accepting connections.

## What's NOT transactional

shard-db is not a multi-statement ACID database. There are **no transactions across records**. CAS is per-record (`insert if_not_exists`, `update if:{...}`) and gives you optimistic concurrency control without locks.

If you need to update two records atomically, your options are:

- Write-ahead log your intent into a third record as a staging step.
- Use `bulk-update` with `criteria` — still per-record CAS, but batched.
- Combine `auto_update` with a `version:long:default=seq(...)` field and compare-and-swap on version.

## Connection scaling

Each connection runs on a worker thread (bounded by `WORKERS`, default = `max(nproc, 4)`). The accept loop in `src/db/server.c` uses `poll(2)` on both Linux and macOS — the listen socket is a single fd, so `epoll`'s selectivity has nothing to gain over the POSIX baseline `poll`, and one accept path runs everywhere. Ready connections are handed off to the worker queue. Single connection is not a bottleneck — pipelining multiple JSON requests over one socket gets close to per-connection line rate.

Cache pressure: every active connection allocates a `MAX_REQUEST_SIZE`-byte read buffer. At the default 32 MB, 100 concurrent connections = 3.2 GB. Raise `MAX_REQUEST_SIZE` deliberately.

## Sanitizer-clean as of 2026.05.4

The full 77-case suite runs cleanly under both AddressSanitizer and ThreadSanitizer (see the [2026.05.4 release notes](../release-notes/2026.05.4.md) for the bug list and per-site fixes). Notable patterns enforced:

- `_Atomic int` for every cross-thread stop flag (`g_log_running`, `server_running`, `active_threads`, `in_flight_writes`, `g_scan_stop`, `QueryDeadline.timed_out`).
- `localtime` → `localtime_r` everywhere (libc's non-reentrant `localtime` returned a shared static buffer, racing across concurrent log calls).
- `parallel_for`'s help-drain race fixed with an `_Atomic int finishing` counter — caller waits for both `remaining==0` AND `finishing==0` before destroying the pool group.

Two known patterns remain suppressed (documented inline in `.tsan.supp`): the `bt_acquire / segcache_acquire / kfcache_acquire` verify-retry lock-order false positive (release happens between acquires; TSan tracks cycles without modeling unlocks), and the `seg_record_emit` byte-level races where the byte-18 flag is the release-store/acquire-load synchronisation point for the full record (C11 guarantees coherency after observing `flag==1`; TSan tracks each byte independently).
