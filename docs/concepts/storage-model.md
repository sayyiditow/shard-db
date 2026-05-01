# Storage model

A conceptual walkthrough of what lives on disk when you insert a record. For the full directory tree, see [Configuration → Storage layout](../getting-started/configuration.md#storage-layout).

## Objects, shards, slots

- An **object** is shard-db's table. Every object has a typed schema (`fields.conf`), one or more shard files, optional indexes, and a directory for stored files.
- A **shard** is one `.bin` file under `<object>/data/`. `splits` is configured per-object and locked to a power of 2 in `{8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}` (`MIN_SPLITS`–`MAX_SPLITS`); default is 8 when `create-object` doesn't pass `splits`. The 3-hex-digit filename (`NNN.bin`) caps the count at 4096.
- A **slot** is a single record position within a shard. Shards start at 256 slots each and grow dynamically.

### Record routing

Every record's 128-bit xxh128 hash dictates its slot:

```
shard_id = hash[0..1] % splits
slot     = hash[2..5] % slots_per_shard
```

Collisions (same slot, different key) are resolved by **linear probing** within the shard — scan forward until an empty slot or the same key is found. Because shards grow before 50% load, probe chains stay short.

## Shard file format

Each `.bin` shard file has three regions:

```
[ShardHeader: 32 bytes]
[Zone A: slots_per_shard × 24 bytes]   -- slot metadata
[Zone B: slots_per_shard × slot_size]  -- payloads (key + value)
```

### ShardHeader (32 bytes)

Magic `SHKV`, version, `slots_per_shard`, active record count, reserved bytes for future fields.

### Zone A — slot headers (24 bytes each)

Each slot's metadata:

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 16 | xxh128 hash | Full 128-bit hash of the key |
| 16 | 2 | flag | `0` = empty, `1` = active, `2` = tombstoned |
| 18 | 2 | key_len | Length of the key in Zone B |
| 20 | 4 | value_len | Length of the typed-binary value in Zone B |

Zone A is compact (24 bytes × slot count) and stays resident in page cache. Probing reads only Zone A — for a typical linear probe depth, that's ~3 KB of touched memory (fits in L2 cache).

### Zone B — payloads

For slot `i`, the payload sits at offset `sizeof(ShardHeader) + slots * 24 + i * slot_size`. Layout within the payload:

```
[key_len bytes: raw key][value_len bytes: typed-binary record]
```

Zone B is only read when a probe matches a hash in Zone A — so full-scan filters still walk quickly because most slot payloads are never touched until their hash confirms a candidate.

## Why Zone A/B separation

- **Probing locality**: scanning hashes in Zone A linearly is branchless-friendly and cache-warm.
- **Payload read only on match**: a full scan that rejects on metadata never touches Zone B.
- **Growth is cheap**: doubling `slots_per_shard` means expanding two parallel arrays — both fixed stride, no pointer chasing.

## Dynamic shard growth

When a shard's load exceeds 50%:

1. Build `.new` file with doubled `slots_per_shard`.
2. Rehash every active slot into the new file.
3. Rename `.new` → original (atomic).

Growth is transparent to concurrent readers (they keep reading the old mmap until they re-open). Writers block briefly during rename. **No slot cap** — shards grow as data grows.

**What is capped:** `MAX_SPLITS = 4096` is the maximum number of shard **files** per object (the `NNN.bin` name has three hex digits). That's a ceiling on shard count, not record count.

## Typed binary records (Zone B payload)

Records in Zone B are **not JSON** — they're packed typed fields in the order declared in `fields.conf`. See [Concepts → Typed records](typed-records.md) for the full type list and on-disk sizes.

Because the layout is fixed by the schema, `match_typed()` (query.c) can compare a criterion against a Zone B region **without parsing** — it knows at schema-load time where each field starts and how to interpret it. Zero allocations per record, direct byte compares.

## Crash safety

- Every write is `write flag=0` → activate by flipping `flag=1` in place. If the process dies between those two writes, the slot is invisible to readers and gets cleaned on next growth.
- Shard rebuilds (vacuum, add-field, growth) use a `.new` file + atomic `rename`. A crash mid-rebuild leaves the original intact.
- On startup, the server sweeps stale `.new` / `.old` artifacts across every object.

## Per-object metadata

```
<object>/metadata/
  counts              # "<live_records> <tombstoned>\n"
  sequences/          # one file per named sequence, counter persisted between restarts
```

`counts` is updated on every insert/delete for O(1) `size` queries. `recount` does a full scan and rewrites this file.

## mmap strategy

- **Reads**: `MAP_PRIVATE` — copy-on-write mapping, so a concurrent writer can't tear a read. Backed by the shared `ucache` (see below).
- **Writes**: `MAP_SHARED` via `ucache` entries, with a per-entry rwlock. Writes go through the cache so readers on the same fd see them immediately.

### `ucache` — unified shard mmap cache

`FCACHE_MAX` entries (default 4096). Each entry is one mmapped shard. Evicted LRU-style when full. Every read and write takes the entry's rwlock (shared for read, exclusive for write), so concurrent reads are lock-free to each other; only a writer blocks readers, and only on the specific shard.

See [Concepts → Concurrency](concurrency.md) for the full locking model.

## Indexes (separate from the shard file)

Indexes live in `<object>/indexes/<field>/<NNN>.idx` as B+ trees with prefix-compressed leaves — each indexed field is **sharded into `splits/4` files** (per-shard btree layout, 2026.05.1+) so reads fan out across all shards via the parallel-for pool. When a query can use an index, the server reads only the relevant idx-shards' leaves + the matching slots' headers, never scanning unmatched data shards. Full detail: [Concepts → Indexes](indexes.md).

## Files (stored blobs, not records)

`<object>/files/<filename>` — uploaded via [put-file](../query-protocol/files.md). Flat layout — basename is the lookup key. Not reachable through queries; fetched directly by filename. (Pre-2026.05.2 stored at `<object>/files/<XX>/<XX>/<filename>` with xxh128 hash buckets; existing installs upgrade with the one-shot `./migrate` binary.)
