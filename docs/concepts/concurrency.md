# Concurrency

shard-db is multi-threaded: a worker thread pool services TCP connections, scan paths parallelize across shards, and bulk writes parallelize across streams. The locking model is fine-grained and avoids a global write lock.

## Lock hierarchy (bottom up)

| Scope | Lock type | Purpose |
|---|---|---|
| Per kfcache entry (one kf shard mmap) | rwlock | Readers share; a writer takes exclusive. Commits go through here. |
| Per bitmap-cache entry (one bitmap shard mmap) | rwlock | Readers share; bitmap mutation and rebuild take exclusive. When both kfcache and bitmap are needed, kfcache is acquired first. |
| Per segcache entry (one seg file mmap) | rwlock | Routine record writes take rdlock (each owns a unique offset); eviction/recovery takes wrlock. |
| Per bt_cache entry (one btree mmap) | rwlock | Same model, separate cache. One entry per per-shard idx file. |
| Per stream (one append lane) | mutex + try-lock pool | Tail reservation serialised per stream; free-pool consumers use try-lock. |
| Per object (logical) | rwlock ("objlock") | Normal ops take read; schema mutations take write. |
| Global maps (schemas, indexes, dirs, slotcask registry) | mutex | Short-held, protects cache-lookup structures. |
| Process wide | atomic counters | `in_flight_writes`, `active_threads`, `server_running` — no locks, just atomics. |

### Cross-cache rule: kfcache before bitmap cache

Bitmap bit positions are kf slot numbers, so any operation that resolves
bitmap bits through the corresponding kf shard may need both cache entries at
once. Those paths always acquire the kfcache entry first and the bitmap-cache
entry second. That acquisition order is mandatory; release operations do not
block and therefore do not add lock-order edges. CRUD already has this shape:
the kfcache write lock is the commit lock and bitmap maintenance runs from the
pre-commit hook while it is held. Query emission, KeySet construction, and
bitmap rebuild use the same acquisition order.

Opening the bitmap first is forbidden even for read/read nesting. A concurrent
CRUD writer can hold the kfcache wrlock while waiting for the bitmap wrlock; a
reader holding the bitmap rdlock while waiting for the kfcache rdlock would
complete an AB-BA deadlock.

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

### Indexed-read lock rule: never block on kfcache while holding a btree lock

The write path takes `kfcache wrlock → bt_cache wrlock` (mutation windows run
phase-I index apply under the held kf writer). Therefore **no btree-lock-holding
context may block on kfcache** — that inversion AB-BA-deadlocks against any
concurrent indexed write. Every reader executor complies with one of two
patterns:

1. **Try-then-release (ordered walks, cursor pagination):** callbacks that
   need the record call `read_record_ref_try` through their
   `BtOrderedWalkHandle`; on `EBUSY` they release every iterator, perform
   the normal blocking fetch, and the walk reopens past the delivered
   `(value,hash)`.
2. **Owner-thread chunking (streaming find, composite key, min/max walk,
   varlen group-by):** fan-out workers own private `BtRangeIter`s and drain
   a bounded hash batch, **close their own iterator**, do the blocking bulk
   fetch outside any bt lock, then reopen past the last delivered
   entry using inclusive bounds plus the raw-hash16 tiebreak skip. Worker
   memory stays bounded (per-worker cap derived from `QUERY_BUFFER_MB`),
   and no cross-thread iterator state exists.

Calling blocking record-fetch helpers directly from any bt-callback context
recreates the deadlock.

Related liveness rule: object-wide counter sums (`slotcask_sum_kf_totals`,
used by counts and find-total hints) read shard headers via lock-free
`pread` **by design** — a mutation window legitimately holds one kf wrlock
for its entire M→C span, and taking per-shard readers there would stall
every unrelated read for the whole window. Counter values are advisory
metadata; each shard contributes an internally-consistent pre- or
post-window pair.

## Writer preference on file-cache rwlocks (2026.07.x+)

`bt_cache`, `kfcache`, `segcache`, and the bitmap cache initialize their
per-file rwlocks writer-preferring on glibc/Linux
(`PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP`, via
`rwlock_init_writer_preferring()` in `shard_db_internal.h`). Default-attribute
`pthread_rwlock_t` on glibc/NPTL prefers readers indefinitely: a writer
blocked in `pthread_rwlock_wrlock()` on one of these per-file locks could be
starved by continuous concurrent reader traffic on that same file, since new
readers were always granted ahead of a waiting writer. The writer-preferring
attribute instead queues new readers behind an already-waiting writer, so a
writer is guaranteed to make progress once it starts waiting, at some cost
to peak reader throughput on a hot file under sustained write pressure.

On non-glibc platforms (macOS, other Linux libcs), `pthread_rwlockattr_setkind_np`
and the `_NONRECURSIVE_NP` attribute don't exist — there's no portable
equivalent to fall back to. These four caches keep the platform default
there (no writer-preference guarantee), same as before this change.

`objlock` (below) is unchanged everywhere, on every platform: it
deliberately keeps default-attribute (platform-default) rwlocks, because its
API permits a thread to hold a recursive read lock, which a nonrecursive
writer-preferring policy doesn't support safely — a recursive reader could
self-deadlock behind a queued writer.

## Per-stream mutex + free pool

Each of the (up-to-16) write streams has:

- **`rotation_lock`** — the mutex that serialises tail reservation in the active segment. Held only long enough to bump `reserve_off` by `(24 + klen + vlen)` bytes and check for segment rotation (≥ 128 MB → start a new seg file). Microseconds per write.
- **`pool_lock` + free_slots[]** — the snake-game pool of tombstoned slots ready for reuse. Consumers use `pthread_mutex_trylock`; only one consumer at a time. If the trylock fails, the writer falls through to tail reservation. The pool's been kept lock-light because contention here regresses sustained-delete workloads.

Two different writers hashing to the same stream contend on `rotation_lock` briefly. Two writers hashing to different streams don't share a stream-level lock at all — they only contend in kfcache, and only if their target shards happen to overlap (which is independent of stream choice — `shard_id = hash[0..1] % splits`, `stream_id = hash[15] % streams`).

## Per-object rwlock ("objlock")

Layered on top of the per-shard locks. Every JSON request gets classified:

- **Normal ops** (all CRUD, queries, bulk ops) → `objlock_rdlock()`. Many concurrent.
- **Schema mutations** (`add-field`, `remove-field`, `rename-field`, `vacuum --compact`, `vacuum --splits`, `truncate`, `add-index`, `remove-index`) → `objlock_wrlock()`. Blocks everyone; held only for the duration of the rebuild.

`add-index`/`remove-index` take `objlock_wrlock()` (not `objlock_rdlock()`, despite mutating rather than restructuring schema) because both `unlink()` and rebuild index files in place — the same pattern `vacuum`/`truncate` use. Without the exclusive lock, a concurrent insert's on-the-fly index maintenance can land on the same file mid-rebuild and either get silently discarded or corrupt the B-tree pages being written (fixed in 2026.07.1 — see [changelog](../reference/changelog.md#202671)).

This serializes schema rebuilds against everything else without holding a long-lived lock during normal traffic.

## Write drain on shutdown

`./shard-db stop` sets `server_running = 0` (atomic) to refuse new connections and waits up to 30 seconds for the `in_flight_writes` atomic to reach zero. This guarantees that every write that entered the server before shutdown either committed or returned an error — no half-written records.

Reads are not drained; they're safe to abandon mid-scan.

`AUTO_VACUUM`/`AUTO_RESHARD_ENABLE`'s background threads are joined (not detached) as part of this same shutdown sequence, before the live cache teardown (`slotcask_shutdown`/`kfcache_shutdown` and `bt_cache_shutdown`). If either thread is mid-sweep on an object when `stop` is issued, shutdown waits for that item to finish — unbounded in theory, but no worse in practice than the exclusive objlock that operation already holds against all other traffic on that object. This closes a use-after-free race: without the join, `kfcache_shutdown()` could free/destroy the kfcache array while a reshard/vacuum thread was still using it.

## Warmup thread vs. concurrent vacuum/rebuild

`warmup_thread` (`src/db/server.c`) walks the whole `db_root` tree at
startup and fans out one I/O-pool task per kf shard (`WarmupKfTask` /
`warmup_kf_task_fn`) to pre-fault pages into cache. Phase 1 (the tree walk
that collects tasks) and phase 2 (the pool actually running them) are not
atomic with each other — a task collected early in the walk can sit queued,
holding no lock at all, for as long as the rest of the walk takes, which is
easily long enough for a concurrent `vacuum --splits`/`--compact` to run to
full completion on that same object: take `objlock_wrlock()`, rewrite
`schema.conf`, call `slotcask_registry_invalidate` (which frees the old
`SlotcaskDb`), release the wrlock.

Because of that gap, `warmup_kf_task_fn` must never carry a `SlotcaskDb*`
(or the `SlotcaskSchemaInfo` used to obtain one) captured during phase 1
across into phase 2 — either can go stale mid-gap, and reusing stale info
on a subsequent registry miss would silently reopen the object at the
*wrong* shape (`rebuild_object_v2` invalidates the registry but never
repopulates it, so the first `registry_get` after a rebuild is the one that
determines the reopened shape). Instead, each task reloads the schema and
re-resolves the registry entry fresh, under its own `objlock_rdlock()`,
taken only at the point the task actually runs — not back in phase 1. That
lock is mutually exclusive with the vacuum's wrlock, so the reload always
observes either the fully-pre-rebuild or fully-post-rebuild schema, never a
torn one. This closed a real UAF (`docs/plans/2026-07-20-warmup-kftask-stale-sdb-uaf.md`):
the first-pass fix re-resolved the `SlotcaskDb*` but still reused a
phase-1-captured `SlotcaskSchemaInfo`, which is exactly as stale-able as the
pointer it replaced.

Two test-only delay knobs (`WARMUP_TEST_DELAY_MS`, inside the rdlock;
`WARMUP_TEST_PRELOCK_DELAY_MS`, before it's even taken) exist to
deterministically exercise both shapes of the race:
`test_warmup_vacuum_race.c` proves mutual exclusion while both are
in-flight (delay fires *inside* the rdlock, so the vacuum's wrlock request
necessarily overlaps in time); `test_warmup_vacuum_norace.c` proves the
no-overlap case — vacuum completes *entirely* before the delayed task ever
attempts the rdlock — still leaves the daemon crash-free and the object
correctly re-resolved to its new post-vacuum shape.

## Durability: window barriers and eviction-time flush

Durability is bounded synchronously by the per-window M/A/I/K/T/C marker protocol (see [storage-model.md](storage-model.md)), not by a periodic background sweep — a mutation's durability point is the fsync'd marker write, not a subsequent timed flush. There is no longer a mandatory background durability-sync thread; each cache entry's `dirty`/`dirty_since_ms` fields (`_Atomic`, on every kf/seg/bt/bm cache entry) are only consumed synchronously, at cache-eviction time.

`durability_flush_dirty()` (`src/db/durability.c`) is called inline whenever `kfcache_acquire`/`segcache_acquire`/the bt/bm cache pick an LRU victim to evict (`bitmap.c`, `btree.c`, `slotcask.c`): before a dirty mapping is dropped from the cache, it is `msync()`-ed so a crash after eviction can't lose it. A failed sync restores the entry's dirty state (and its earliest `dirty_since_ms`, via `durability_restore_earliest`) so the next eviction attempt retries rather than silently losing the write. `durability_mark_dirty()` sets these fields at each of its call sites (`bitmap.c`, `btree.c`, `slotcask.c`) purely to feed this eviction-time flush — there is no other consumer.

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

`QUERY_BUFFER_MB` (default 256) caps the intermediate buffers any single query can hold. Checked at every collection site (ordered find buffer, aggregate hash tables, OR KeySet, AND-intersection KeySets, bulk-delete/update key list, btree hash collection). When exceeded → `{"error":"query memory buffer exceeded; narrow criteria, add limit/offset, or stream via fetch+cursor"}`. Prevents one bad query from monopolising RAM. Pair with whole-process containment (`MemoryMax=`, cgroup `memory.max`) as a backstop.

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

The full 232-case suite runs cleanly under both AddressSanitizer and ThreadSanitizer (see the [2026.05.4 release notes](../release-notes/2026.05.4.md) for the bug list and per-site fixes). Notable patterns enforced:

- `_Atomic int` for every cross-thread stop flag (`g_log_running`, `server_running`, `active_threads`, `in_flight_writes`, `g_scan_stop`, `QueryDeadline.timed_out`).
- `localtime` → `localtime_r` everywhere (libc's non-reentrant `localtime` returned a shared static buffer, racing across concurrent log calls).
- `parallel_for`'s help-drain race fixed with an `_Atomic int finishing` counter — caller waits for both `remaining==0` AND `finishing==0` before destroying the pool group.

Two known patterns remain suppressed (documented inline in `.tsan.supp`): the `bt_acquire / segcache_acquire / kfcache_acquire` verify-retry lock-order false positive (release happens between acquires; TSan tracks cycles without modeling unlocks), and the `seg_record_emit` byte-level races where the byte-18 flag is the release-store/acquire-load synchronisation point for the full record (C11 guarantees coherency after observing `flag==1`; TSan tracks each byte independently).
