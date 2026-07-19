# Storage model

shard-db's storage engine is **slotcask** — a bitcask-style key/value layout where keys and values live in separate files. Slotcask has been the default for new objects since 2026.05.1 and is the only supported engine as of 2026.05.5; the legacy probe-into-slot engine (historically "v1") was removed in that release. Operators upgrading from a pre-2026.05.5 install with v1 objects on disk must first run 2026.05.4's `./migrate` to convert them.

For the full on-disk tree, see [Configuration → Storage layout](../getting-started/configuration.md#storage-layout). This page walks through what actually happens when you insert, read, or delete a record.

## Objects, shards, streams

- An **object** is shard-db's table. Every object has a typed schema (`fields.conf`), a per-object keyfile (sharded), per-object segment files (streamed), and optional indexes.
- A **keyfile shard** is one `.kf` file under `<object>/data/kf/`. `splits` is configured per-object and locked to a power of 2 in `{8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}` (`MIN_SPLITS`–`MAX_SPLITS`); default is 8 when `create-object` doesn't pass `splits`. The 3-hex-digit filename (`NNN.kf`) caps the count at 4096.
- A **stream** is one append-write lane for value records. Streams have their own subdirectory under `<object>/data/streams/NNN/` containing the active segment file and any rotated-but-not-yet-compacted predecessors. The number of streams is fixed at `create-object` time from the host's CPU count (`SLOTCASK_MAX_STREAMS = 16`).
- A **segment** is one `.dat` file under a stream's directory, named `NNNNNN.dat` (6-digit zero-padded file id). Segments are append-only and rotate at `SLOTCASK_SEG_MAX_BYTES = 128 MB`.
- A **slot** is one 24-byte entry inside a keyfile shard. Slots point at value records in segment files via `(stream_id, file_id, offset)`.

### How a record routes

Each record's 128-bit xxh128 hash determines two things:

```
shard_id  = hash[0..1]  % splits      # which kf shard owns the slot
stream_id = hash[15]    % streams     # which stream's seg file holds the value
```

Within the chosen shard, the **slot index** is `hash[2..5] % slots_per_shard`; collisions (same slot, different key) are resolved by **linear probing** within the shard — scan forward until an empty slot or the same key is found. Probe chains stay short because shards auto-resplit (double capacity in place) once load crosses 75 %.

The stream choice is independent of the shard. Different writes hashing to the same kf shard but different streams append to different seg files in parallel — no per-shard write serialisation on the value path. Only the kf slot store contends, and that's a single atomic 8-byte instruction.

## Keyfile (kf) format

Every `.kf` file is laid out as:

```
[SlotcaskKfHeader: 24 bytes]
[SlotcaskKfEntry × slots_per_shard]
```

### File header (24 bytes)

```
Offset  Size  Field        Notes
 0       4    magic         'SKF1' (0x31464B53 little-endian)
 4       4    version       SLOTCASK_KF_VERSION (currently 1)
 8       8    total         non-empty slots (live + tombstoned)
16       8    deleted       tombstoned slots
```

`total` is the resplit trigger — tombstones still create probe-chain pressure, so the shard doubles based on `total`, not `total − deleted`. `live = total − deleted` is computed when callers need it (no separate counter).

Updates to the header happen under the shard's wrlock (acquired via `kfcache_acquire(writer=1)`); no atomics required. This is the single source of truth for record counts in v2 — `size`, `orphaned`, `recount` all read it directly. `recount` is no longer needed post-crash because the header is durable in the page cache.

### Slot entry (24 bytes)

```
Offset  Size  Field        Notes
 0      16    hash          full xxh128 of the key
16       1    flag          0=empty, 1=live, 2=tombstoned
17       1    stream_id     which stream holds the value
18       2    file_id       segment file id within that stream
20       4    offset        byte offset of the value record within that segment
```

The trailing 8 bytes (`flag + stream_id + file_id + offset`) are 8-byte aligned, so **a write or update commits via a single `__atomic_store_n` on that uint64** (`kf_repoint`). After that store is visible to other threads, the new value is the record's authoritative location; the old slot/segment bytes are orphaned (reclaimed on vacuum or by the snake-game pool).

Lookups read the slot and follow `(stream_id, file_id, offset)` into the segment cache to get the value. No data lives in the keyfile — only the pointer to where the data is.

## Segment format

Segment files are append-only. Each record laid down is preceded by an in-segment header that mirrors the keyfile entry's stable fields (so recovery can scan a seg byte-for-byte without needing the kf):

```
Offset  Size  Field
 0      16    hash             xxh128 of the key
16       2    klen             uint16 — key length
18       1    flag             0=empty, 1=live, 2=tombstoned (mirrors kf)
19       1    reserved
20       4    vlen             uint32 — value length
24    klen    key bytes
24+klen vlen  value bytes (typed-binary record)
```

Layout total per record: `24 + klen + vlen` bytes.

**flag at byte 18** mirrors the keyfile flag. A `pwrite()` of one byte tombstones a record without rewriting the value. The kf flag is the *commit truth*; the seg flag is a redundant mirror used by full-segment recovery scans.

### How writes land

1. The writer claims a `(stream_id, file_id, offset)` from the active segment of the target stream — either by appending past the segment's reserved tail or by popping a tombstoned slot from the stream's free pool (snake-game reuse). One mutex per stream serialises tail reservation; pool pops use try-lock.
2. Writer copies `[header][key][value]` into the reserved region via the mmap'd segment.
3. Writer takes the shard's wrlock and finds the slot for this hash (linear probe).
4. **Commit:** writer issues a single atomic 8-byte store on the slot's trailing 8 bytes, transitioning `(flag=0|2, _, _, _)` to `(flag=1, stream_id, file_id, offset)`.
5. Writer increments `header.total` (and `header.deleted--` if reusing a tombstone).

The atomic 8B store is the durability boundary. Page-cache writeback eventually persists everything, but for readers, the moment the store is visible is the moment the record exists.

### Direction-C segment compaction

Vacuum (`vacuum`, no flags) sweeps each stream and pair-merges sparse non-active segments. For every non-active seg in a stream:

1. Stat the seg's live count (segs cache the live count in-memory; recomputed on open if unknown).
2. Pick the two sparsest non-active segs as a (donor, recipient) pair.
3. For every live record in the donor: migrate its bytes into a tombstoned hole in the recipient via `kf_repoint_at_slot`, atomically updating the keyfile slot to point at the new location.
4. When the donor is fully drained, unlink the file (`munmap` first, then `close + unlink + fsync` on the parent dir).

The active seg of each stream is never touched, so concurrent appends after vacuum returns are unaffected. This is the primary disk-reclaim path for delete-heavy workloads — the snake-game free pool reuses tombstone holes *within* the active seg, but only Direction-C compacts across segs.

## Dynamic shard growth — kf auto-resplit

When a kf shard's `header.total × 4 ≥ capacity × 3` (≥ 75 % fill), it doubles in place:

1. Build `<NNN>.kf.new` at twice the slot capacity (header + 2× slot array, ftruncate to size).
2. Stream every flag=1 slot from the old file into the new one, computing each slot's new index from `hash[2..5] % new_capacity`. Tombstoned slots are dropped — `new.total = live_copied`, `new.deleted = 0`. Linear probing handles collisions in the larger space.
3. Atomically rename `.new` over the original.
4. Update the kfcache entry to point at the new mmap.

The resplit holds the shard's wrlock; concurrent inserts to that one shard pause for the duration (~80–160 ms at 16M slots, milliseconds at smaller sizes). Reads and inserts on every other shard continue uninterrupted.

There is **no per-shard slot cap** in normal operation — the shard keeps doubling until the global `SLOTCASK_MAX_SLOTS_PER_SHARD = 16M` ceiling, at which point `kf_put_new` refuses further inserts and the operator must `vacuum --splits=N` to widen the keyspace. At 80 % load that's ~12.8M live records per shard; a `splits=4096` object can hold tens of billions of live records before any single shard hits the ceiling — well into "you should be partitioning" territory.

**What is capped:** `MAX_SPLITS = 4096` is the maximum number of kf shard *files* per object (`NNN.kf` is three hex digits). Beyond that, partition the object.

## Snake-game free-slot pool

Per stream, an in-memory free list holds `(file_id, offset)` of tombstoned slots ready for reuse. On insert, the writer first tries `pool_pop()` (try-lock; only one consumer at a time); if it gets a free slot, the new record overwrites the tombstoned bytes in place — no segment growth. If the pool is empty (or the try-lock fails), the writer falls through to the tail-append path.

This makes steady-state delete-then-insert workloads (e.g. a queue) free of fragmentation on the active seg. The pool is rebuilt on `slotcask_open` by scanning each segment's records for `flag=2` entries — done in parallel via `parallel_for_io`, one worker per stream.

The pool only covers the active seg's tombstones and any rotated-but-not-yet-compacted segs. Once Direction-C vacuum drains and unlinks a sparse seg, its tombstones are gone (the live records moved to a denser seg's holes).

## Typed binary records (segment payload)

The value bytes inside each segment record are **not JSON** — they're packed typed fields in the order declared in `fields.conf`. See [Concepts → Typed records](typed-records.md) for the full type list and on-disk sizes.

Because the layout is fixed by the schema, `match_typed()` (query.c) can compare a criterion against a record's payload **without parsing** — it knows at schema-load time where each field starts and how to interpret it. Zero allocations per record, direct byte compares.

## Crash safety

The slotcask engine is "commit on atomic 8B store" — every other byte may be in flight when the process dies:

- **A crash between writing the segment record and committing the kf slot** → the segment bytes are orphaned (no slot points at them). Disk space is wasted until the next vacuum, but no record is corrupted or visible-but-torn.
- **A crash between committing the kf slot and updating `header.total`** → the slot is live and readable; `header.total` is one behind. The next vacuum or `recount` reconciles via `slotcask_sum_kf_totals` (it walks each kf header). For v2, recount is rarely needed because the header is durable in the page cache and survives SIGKILL.
- **A crash mid-resplit** → the `.kf.new` staging file is unlinked on startup before any shard is touched. Original kf stays intact.
- **A crash mid-Direction-C compaction** → either the kf slot's atomic store landed (donor record is now in the recipient) or it didn't (still in the donor). The startup recovery scan reads each segment's flags and rebuilds the free pool; no inconsistency window.

On startup, the daemon's `validate_metadata` pass sweeps `.shard-db.lock` (refuses two daemons on one root), validates that every object in `schema.conf` has a `fields.conf` + `data/` tree, and unlinks stale `.new`/`.old` rebuild artifacts.

## Caches

Two LRU caches sit between the engine and disk:

- **kfcache** — path-keyed cache of mmap'd keyfile shards. Capacity from `FCACHE_MAX` (default 4096 entries). Each entry holds `(fd, mmap, header, slot_array, capacity, rwlock)`. Readers take rdlock; writers (inserts, updates, deletes, resplit) take wrlock. Per-entry locks mean a write to one shard blocks only that one shard — every other shard remains fully concurrent.
- **segcache** — path-keyed cache of mmap'd segment files. Same model; capacity `FCACHE_MAX`. Routine record writes take rdlock (each writer owns a unique reserved offset, so they don't conflict on bytes); eviction and recovery take wrlock.

Both caches LRU-evict once half-full. mmap regions are `MAP_SHARED` so writes hit the page cache directly and become visible to other readers as soon as the atomic store lands.

See [Concepts → Concurrency](concurrency.md) for the full locking model.

## Indexes

Indexes are stored separately from the kf and seg files: `<object>/indexes/<field>/<NNN>.idx` (B+ trees with prefix-compressed leaves). Each indexed field is split into `index_splits_for(splits)` files (non-linear curve in `src/db/types.h`) so reads fan out across all idx-shards via the parallel-for worker pool. When a query can use an index, the server reads the idx-leaf bytes (and only the matching slots' kf entries + seg payloads), never scanning all kf shards.

Index maintenance is hooked into the kf commit: every `slotcask_upsert_with_hooks` or `slotcask_delete_with_hooks` call fires a `pre_commit` callback that resolves the per-field value and writes to (new value) or deletes from (old value) every relevant idx shard. Indexes are always in sync — there's no separate "build" phase for online inserts. The standalone `add-index` path scans every kf shard once, parallelising the field extraction + bulk btree build.

Full detail: [Concepts → Indexes](indexes.md).

## Files (stored blobs, not records)

`<object>/files/<filename>` — uploaded via [put-file](../query-protocol/files.md). Flat layout — basename is the lookup key. Not reachable through queries; fetched directly by filename. (Pre-2026.05.2 stored at `<object>/files/<XX>/<XX>/<filename>` with xxh128 hash buckets; existing installs upgrade with the one-shot `./migrate` binary.)

## Legacy v1 layout (pre-slotcask, removed in 2026.05.5)

Before 2026.05.1 the engine was probe-into-slot: each shard file (`data/NNN.bin`) held a 32-byte header, a Zone A region of 24-byte slot metadata, and a Zone B region of variable-size record payloads — keys and values interleaved within the same file. Writes flipped a `flag` byte in Zone A from 0 to 1 to commit; tombstones flipped it to 2. There was no kf/seg split, no streams, and no atomic 8-byte commit — durability relied on the OS page cache writeback ordering.

This layout was removed in 2026.05.5. Operators on a pre-2026.05.5 install with v1 objects on disk must run 2026.05.4's `./migrate` to convert them to slotcask before upgrading; this binary refuses v1 at load with a clear error.
