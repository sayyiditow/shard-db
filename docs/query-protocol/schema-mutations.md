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

`value_size` (the per-record payload size, stored in segment files) is **always computed** as the sum of typed-field sizes — not user-configurable. Returned in `create-object` and `describe-object` responses; recorded internally for slot-size accounting.
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

## edit-field

Edit one or more existing fields in place — same-type only. Used to grow/shrink a `varchar`, widen/narrow an integer family field, change a `numeric`'s scale, or widen `float → double`.

```json
{
  "mode": "edit-field",
  "dir": "<dir>",
  "object": "<obj>",
  "fields": [
    "name:varchar:200",
    "age:long",
    "balance:numeric:18,4"
  ]
}
```

CLI shortcut (single-field — JSON form covers batch):

```bash
./shard-db edit-field <dir> <obj> 'name:varchar:200'
```

### What changes are allowed

| Edit | Rule |
|---|---|
| `varchar:N → varchar:M` | Grow always allowed. Shrink refused **pre-flight** if any live record's content length exceeds `M`. |
| Integer family (`short ↔ int ↔ long`) | Widen always allowed (sign-extension preserves negatives). Narrow refused **pre-flight** if any live record's value falls outside the new type's `[-2^(N×8-1), 2^(N×8-1) − 1]` range. |
| `numeric:P,S1 → numeric:P,S2` | Scale-up multiplies the stored `int64` by `10^(S2−S1)`; refused pre-flight if any value would overflow `int64`. Scale-down divides and **truncates toward zero** (matches Postgres). |
| `float → double` | Always allowed; IEEE 754 widen, no validation needed. |

**Cross-type edits are hard-refused** with the hint: use `add-field <new> + remove-field <old> + bulk-update` and migrate the data explicitly.

### What happens

1. Takes the object's **write lock** (`objlock_wrlock`).
2. Refuses immediately if `storage_version != 2` (point to `./migrate`).
3. Parses every edit spec; refuses on unknown field name, tombstoned field, duplicate edit in the same request, invalid type, or cross-type change.
4. Builds a new `TypedSchema` by overlaying each edited field onto a clone of the old schema (positions unchanged, only `size` / `offset` / `numeric_scale` move).
5. **No-op fast path** — if no field's encoding actually changed (a varchar staying the same size with only `:default=...` modifier shift, say), skip the data rebuild and rewrite `fields.conf` only.
6. Otherwise **pre-flight scan**: walks every live record across all keyfile shards and verifies each edited field's value fits the new shape. First violation aborts with `{"error":"Pre-flight failed on field [<name>]: <reason>"}` — no data or schema change.
7. **Rebuild**: runs the same v2 rebuild path used by `add-field` / `vacuum --compact`, but with `transform_field_value()` re-encoding the edited fields per record. Atomic — the legacy data tree is preserved until the rebuild succeeds.
8. Rewrites `fields.conf` to lock in the new spec.
9. Wipes and rebuilds every index in `index.conf` — affected indexes have stale leaf bytes, and v1 of the feature rebuilds all of them for simplicity (acceptable: optimise to "only affected" later if it becomes a hot path).
10. Releases the write lock.

### Response

```json
{"status":"edited","fields":N,"rebuilt":true,"slot_size":N,"indexes_rebuilt":N}
```

No-op fast path returns `{"status":"edited","fields":N,"rebuilt":false}` — fields.conf updated but no data rebuild ran.

### Notes

- **v2 only**. v1 (legacy zone-A/B) is hard-refused with a pointer to `./migrate`. 2026.06 drops v1 entirely.
- **Default modifiers**: if the new spec includes a different `:default=...` / `:auto_create` / `:auto_update`, that affects future inserts only — existing records keep their stored values.
- **Indexed fields**: a varchar grow that doesn't shrink content, an integer widen, or any encoding-changing edit on an indexed field still rebuilds the index (the on-disk leaf bytes change). Queries on the indexed field continue to resolve to the same records post-edit.
- Full object rebuild — scales with object size, not slot count. Not instantaneous on millions of records. Holds the wrlock for the duration.

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

Tombstones one or more fields. Bytes stay reserved in every record's payload until [`vacuum --compact`](#vacuum) runs.

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
| `{"mode":"vacuum",...}` | **v1**: in-place tombstone reclaim — slots with `flag=2` are zeroed back to `flag=0`. **v2**: Direction-C seg compaction — sparse non-active seg files are pair-merged into denser ones via kf-repoint, then unlinked. Active seg of each stream is never touched. *Also*: if the host's CPU count has changed since `create-object` and `slotcask_streams_for_nproc()` no longer matches `schema.streams`, the call automatically promotes to a full rebuild that re-routes records into the new stream layout. Idempotent. |
| `{"mode":"vacuum","compact":true}` | Full rebuild. Drops tombstoned fields, shrinks `slot_size`. Indexes preserved. |
| `{"mode":"vacuum","splits":N}` | Full rebuild with a new shard count. Re-hashes data; hash routing identity is preserved. **Triggers a full reindex** — see below. Also folds in the streams-mismatch check on the same rebuild, so you never need a second call. |
| `{"mode":"vacuum","compact":true,"splits":N}` | Both — compact schema and reshard in one pass. |

### Why `splits` triggers reindex (2026.05.1+)

Each indexed field is sharded into `index_splits_for(splits)` btree files (`<obj>/indexes/<field>/<NNN>.idx`). Changing `splits` changes the per-field shard count, so the on-disk `NNN.idx` files for the old layout become unreachable orphans. `vacuum --splits` calls `reindex_object()` after the data rebuild, which:

1. Wipes every per-field idx directory (`indexes/<field>/`).
2. Rebuilds each indexed field at the new `index_splits_for(splits)` shard count.
3. Preserves the index list (`index.conf`) — same indexes, fresh layout.

Plain `vacuum --compact` (no `splits`) leaves indexes alone — the per-field shard count doesn't change.

### What triggers the need

- Many deletes → `vacuum-check` flags objects where tombstoned ≥10% and total ≥1000.
- Removed fields → `compact` to reclaim their bytes.
- Shard skew from growth → `splits:N` to even out load. See [`shard-stats`](diagnostics.md).
- **CPU upgrade or container resize** (v2) → the next default `vacuum` self-heals: it detects `slotcask_streams_for_nproc() ≠ schema.streams` and dispatches to the rebuild path, after which records route by the new stream count. The auto-vacuum thread picks this up automatically once any object also crosses the deletion-pct threshold; for an explicit fix, run `./shard-db vacuum <dir> <obj>`.

### Locks

All vacuum flavors take the object's write lock. Normal ops block for the duration of the rebuild.

Response (v2 light path): `{"status":"vacuumed","cleaned":<seg-files-dropped>}`.
Response (heavy path — `compact`, `splits`, or streams-mismatch): `{"status":"rebuilt","live":N,"splits":N,"streams":N,"slot_size":N,"compact":true|false,"indexes_rebuilt":N}`.

## truncate

Delete all records; schema and indexes survive.

```json
{"mode":"truncate","dir":"<dir>","object":"<obj>"}
```

- Fast: zeroes out every kf shard (slot array + header counters) and drops every seg file; resets total/deleted to 0. Schema and `fields.conf` stay intact.
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
