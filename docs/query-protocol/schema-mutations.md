# Schema mutations

Evolve object schemas without downtime. All mutations are atomic at the object level — a per-object write-lock blocks normal ops briefly during the rebuild.

## create-object

Create a new typed object. See [Quick start](../getting-started/quickstart.md) for a full example.

```json
{
  "mode": "create-object",
  "dir": "<dir>",
  "object": "<obj>",
  "splits": 16,
  "max_key": 128,
  "fields": [
    "name:varchar:100",
    "age:int",
    "balance:numeric:12,2",
    "active:bool"
  ],
  "indexes": ["name","age"]
}
```

| Param | Required | Default | Meaning |
|---|---|---|---|
| `dir` | yes | — | Tenant dir. Auto-registered if new. |
| `object` | yes | — | Object name. |
| `splits` | no | `8` (`DEFAULT_SPLITS`) | Initial shard count. Must be a power of 2 in `[8, 4096]` (`MIN_SPLITS`–`MAX_SPLITS`). The default is tuned for sub-1M-row objects; pass `splits` explicitly for larger workloads. |
| `max_key` | no | `64` | Max key length in bytes. Hard ceiling 1024 (`MAX_KEY_CEILING`). |
| `fields` | yes | — | Array of typed field specs. See [Concepts → Typed records](../concepts/typed-records.md). |

`max_value` (the Zone B record size) is **always computed** as the sum of typed-field sizes — not user-configurable. Stored in `schema.conf` for persistence only.
| `indexes` | no | `[]` | Fields to index at creation. Single or composite (`"a+b"`). |

Response: `{"status":"created","object":"...","splits":N,"max_key":N,"value_size":N,"fields":N}`.

## add-field

Append new fields to an existing object.

```json
{
  "mode": "add-field",
  "dir": "<dir>",
  "object": "<obj>",
  "fields": ["phone:varchar:20","verified:bool:default=false"]
}
```

### What happens

1. Takes the object's **write lock** (`objlock_wrlock`).
2. Builds a new shard layout with the extra fields appended.
3. Re-encodes every record: existing field values preserved, new fields filled with their defaults (or empty).
4. Atomically swaps (`.new` → original rename).
5. Rebuilds indexes (none referencing the new field yet; existing indexes are preserved).
6. Releases the write lock.

### Notes

- Default values for new fields are applied at rebuild time. If a field has no default, its bytes are zeroed.
- Existing record count and hash routing are preserved.
- Full object rebuild — scales with object size. Not instantaneous on millions of records.

## rename-field

Metadata-only; no data rewrite.

```json
{
  "mode": "rename-field",
  "dir": "<dir>",
  "object": "<obj>",
  "old": "email_addr",
  "new": "email"
}
```

### What happens

1. Rewrites `fields.conf` with the new name.
2. Rewrites `indexes/index.conf` and renames `indexes/<old>/` → `indexes/<new>/` (per-shard directory rename — all `NNN.idx` files travel with the rename, no rebuild required).
3. For composite indexes, rewrites parts that reference the old name and renames the composite directory accordingly.
4. Invalidates caches.

### Constraints

- Both names must pass `valid_field_name()` (no `:`, `+`, `/`, spaces).
- `new` must not collide with an existing field.

Response: `{"status":"renamed","old":"...","new":"..."}`.

## remove-field

Tombstones one or more fields. Bytes stay reserved in Zone B until [`vacuum --compact`](#vacuum) runs.

```json
{
  "mode": "remove-field",
  "dir": "<dir>",
  "object": "<obj>",
  "fields": ["legacy_status","deprecated_flag"]
}
```

### What happens

1. Appends `:removed` to each field's line in `fields.conf`.
2. **Auto-drops** any index referencing a removed field (including composites).
3. Queries treating those fields return empty values; writes silently ignore them.

### Why tombstone vs delete

- **Cheap**: no data rewrite until vacuum.
- **Reversible (soft)**: re-adding the same field via `add-field` creates a new column; the old tombstoned bytes stay reserved until compact.

### Reclaiming the bytes

```json
{"mode":"vacuum","dir":"<dir>","object":"<obj>","compact":true}
```

Response from remove-field: `{"status":"removed","fields":N,"indexes_dropped":N}`.

## vacuum

Maintenance — reclaim deleted-record slots, drop tombstoned fields, or reshard.

### Flavors

| Call | What it does |
|---|---|
| `{"mode":"vacuum",...}` | Fast in-place tombstone reclaim. Rewrites slots with `flag=2` (deleted) back to `flag=0` (empty). No schema change. |
| `{"mode":"vacuum","compact":true}` | Full rebuild. Drops tombstoned fields, shrinks `slot_size`. Indexes preserved. |
| `{"mode":"vacuum","splits":N}` | Full rebuild with a new shard count. Re-hashes data; hash routing identity is preserved. **Triggers a full reindex** — see below. |
| `{"mode":"vacuum","compact":true,"splits":N}` | Both — compact schema and reshard in one pass. |

### Why `splits` triggers reindex (2026.05.1+)

Each indexed field is now sharded into `splits/4` btree files (`<obj>/indexes/<field>/<NNN>.idx`). Changing `splits` changes the per-field shard count, so the on-disk `NNN.idx` files for the old layout become unreachable orphans. `vacuum --splits` calls `reindex_object()` after the data rebuild, which:

1. Wipes every per-field idx directory (`indexes/<field>/`).
2. Rebuilds each indexed field at the new `splits/4` shard count.
3. Preserves the index list (`index.conf`) — same indexes, fresh layout.

Plain `vacuum --compact` (no `splits`) leaves indexes alone — the per-field shard count doesn't change.

### What triggers the need

- Many deletes → `vacuum-check` flags objects where tombstoned ≥10% and total ≥1000.
- Removed fields → `compact` to reclaim their bytes.
- Shard skew from growth → `splits:N` to even out load. See [`shard-stats`](diagnostics.md).

### Locks

All vacuum flavors take the object's write lock. Normal ops block for the duration of the rebuild.

Response: `{"status":"vacuumed","object":"...","compact":true|false,"splits":N}`.

## truncate

Delete all records; schema and indexes survive.

```json
{"mode":"truncate","dir":"<dir>","object":"<obj>"}
```

- Fast: zeroes out every shard's Zone A + Zone B metadata, resets counts to 0.
- Indexes are emptied.
- Field schema (including tombstones) stays intact.

Response: `{"status":"truncated","object":"..."}`.

## recount

Rescans every shard, counts live/tombstoned slots, and rewrites `metadata/counts`. Use when `size` returns numbers that look wrong (e.g., after a crash or manual disk edit).

```json
{"mode":"recount","dir":"<dir>","object":"<obj>"}
```

Response: `{"count":N,"orphaned":M}`.

## backup

Copies the object's `data/`, `indexes/`, `metadata/`, and `files/` directories into a timestamped snapshot under the same root.

```json
{"mode":"backup","dir":"<dir>","object":"<obj>"}
```

Response: `{"status":"backed_up","path":"<dir>/<obj>.backup-20260418T153012"}`.

Snapshot is a point-in-time copy — in-flight writes after the copy starts may or may not be included. For consistent production backups, pause writes first (or accept best-effort consistency).

## Lock model summary

| Mutation | Lock | Blocks |
|---|---|---|
| `add-field`, `remove-field`, `vacuum --compact`, `vacuum --splits` | `objlock_wrlock` | All other ops on this object (reads + writes). |
| `rename-field` | `objlock_wrlock` | Same. |
| `truncate` | `objlock_wrlock` | Same. |
| `backup`, `recount` | `objlock_rdlock` | Only schema mutations. |
| Normal CRUD / queries | `objlock_rdlock` | Only schema mutations. |

Held only for the rebuild duration. For multi-second rebuilds, clients see temporarily-blocked queries; consider running these in a maintenance window.

See [Concepts → Concurrency](../concepts/concurrency.md) for the full locking story.
