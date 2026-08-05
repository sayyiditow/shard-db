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
| `indexes` | no | `[]` | Fields to index at creation. Single or composite (`"a+b"`). |
| `auto_key` | no | (none) | Opt into server-generated keys at insert time. `"uuid"` → 16-byte UUIDv4 binary, rendered as 36-char dashed string on read (requires `max_key >= 16`). `"seq(<name>)"` → 8-byte int64 BE from a named sequence, rendered as decimal string on read (requires `max_key >= 8`; sequence is pre-initialised to 0 if absent, first `next` returns 1). Set once at create-object and immutable for the object's life — there is no `set-auto-key` mutation. See [Auto-generated keys](#auto-generated-keys) below. |

`value_size` (the per-record payload size, stored in segment files) is **always computed** as the sum of typed-field sizes — not user-configurable. Returned in `create-object` and `describe-object` responses; recorded internally for slot-size accounting.

Response: `{"status":"created","object":"...","splits":N,"max_key":N,"value_size":N,"fields":N}`.

### Auto-generated keys

Declare `"auto_key":"uuid"` or `"auto_key":"seq(<name>)"` at create-object to turn on server-generated keys.

```json
{
  "mode":"create-object","dir":"<d>","object":"users",
  "splits":16,"max_key":16,
  "fields":["name:varchar:64"],
  "auto_key":"uuid"
}
```

**Insert with the key omitted** → server generates per the object's mode, returns the rendered key:

```json
// request
{"mode":"insert","dir":"<d>","object":"users","value":{"name":"Alice"}}

// response
{"status":"inserted","key":"7a8c2f12-9d31-4abc-9c4a-1a2b3c4d5e6f"}
```

**Insert with a key provided** → upsert (exists → update, else → insert). The provided key must parse in the rendered form (36-char dashed UUID or decimal int).

```json
// request
{"mode":"insert","dir":"<d>","object":"orders","key":"42","value":{"amount":99}}

// response
{"status":"inserted","key":"42"}
```

CAS modifiers (`if_not_exists`, `if`) apply to provided-key inserts as usual. Omit-key + `if:{...}` is rejected at parse time (the predicate applies to a specific record; auto-gen doesn't compose).

**Storage shape** — keys are stored in their on-disk binary form: 16 bytes for `uuid`, 8 bytes BE int64 for `seq`. Wire I/O always renders as the canonical string form (UUID dashed, seq decimal). `get` / `delete` / `find` / `keys` / `fetch` all accept and emit the rendered form.

**bulk-insert** — per-record omit-key gets auto-generated; per-record provided-key upserts. The whole batch is refused up front if any provided key is malformed. Generated keys for the batch are allocated in one shot (single `/dev/urandom` read for UUID; single seq flock for seq) so per-record overhead stays low. **Per-record CAS** is enforced: omit-key records take the strict-insert path (collision → that single record is `condition_not_met` and counted in `skipped`, the other records still write) while provided-key records remain upsert. The check piggybacks on the existing kf-lookup pass — zero extra lookups, zero added latency for non-auto-key bulk-insert.

```json
// request
{"mode":"bulk-insert","dir":"<d>","object":"orders",
 "records":[{"value":{"amount":1}},
            {"key":"500","value":{"amount":500}},
            {"value":{"amount":2}}]}

// response — keys[] preserves input order
{"status":"bulk-inserted","count":3,"skipped":0,"keys":["4","500","5"]}
```

The dict form (`{"k1":{...},"k2":{...}}`) has keys baked into the wire shape — every entry is treated as provided-key.

**bulk-insert-delimited** (CSV / TSV / pipe / etc.) also supports auto-key: per row, an **empty first column** means "auto-generate" and a non-empty first column is parsed as a wire-form key (upsert). Response shape matches the JSON form (`{"status":"bulk-inserted","count":N,"skipped":M,"keys":[...]}` for auto-key objects). When using inline `data` in a JSON request, the standard JSON escapes (`\n`, `\r`, `\t`, `\"`, `\\`, `\uXXXX`) are decoded before parsing — so newline-separated records work as expected.

```json
{"mode":"bulk-insert-delimited","dir":"<d>","object":"orders",
 "delimiter":",",
 "data":",100\n42,42\n,200\n"}

// response (seq watermark was at 5):
{"status":"bulk-inserted","count":3,"skipped":0,"keys":["6","42","7"]}
```

**update / delete** require a key as today. `auto_key` only fires on insert; update with no key errors with the usual "Missing key" message.

**Constraints**:

- `uuid` mode → `max_key` must be at least 16.
- `seq(<name>)` mode → `max_key` must be at least 8.
- The sequence name must be valid (`valid_field_name()` rules — no `:`, `+`, `/`, spaces, parens). The sequence file lives at `<obj>/metadata/sequences/<name>` and is shared with any field that also uses `:default=seq(<name>)`.
- `auto_key` is persisted as the trailing token on the schema.conf line: `dir:object:splits:max_key:2:streams:auto_key=...`.
- **No retroactive enable** — auto_key can only be set at create-object. There is no schema mutation to add or change auto_key later. Revisit only if customers need it.
- **Seq collisions** — for `seq` mode, if you manually insert records with numeric keys at or above the current sequence value, the next auto-generated insert can collide. Single insert returns `{"error":"condition_not_met"}` for that record; bulk-insert (JSON + delimited) skips just the colliding record (`skipped:N` in the response) and inserts every other auto-gen normally — the manual record's data is never silently overwritten. UUID collisions are effectively impossible at any realistic scale.

## add-field

Append new fields to an existing object.

```json
{
  "mode": "add-field",
  "dir": "<dir>",
  "object": "<obj>",
  "fields": [
    "phone:varchar:20",
    "verified:bool:default=false",
    "rowid:long:default=seq(rowid)",
    "trace_id:varchar:36:default=uuid()",
    "nonce:varchar:18:default=random(8)"
  ]
}
```

### What happens

1. Takes the object's **write lock** (`objlock_wrlock`).
2. Builds a new shard layout with the extra fields appended.
3. **Backfill pass** — re-encodes every existing record: prior field values preserved, new fields stamped with their computed default (see table below).
4. Atomically swaps (`.new` → original rename).
5. Rebuilds indexes (none referencing the new field yet; existing indexes are preserved).
6. Releases the write lock.

### Computed defaults on backfill

When the new field's spec includes a default modifier, the rebuild walk applies it to every existing record:

| Modifier | Backfill behaviour |
|---|---|
| `:default=<literal>` | Stamped verbatim on every existing record. Goes through the same type-aware encoder used at insert time (int BE, varchar length prefix, numeric scaling, etc.). |
| `:default=seq(<name>)` | The server **reserves a contiguous range** `[start, start+live_count)` from the named sequence in one flock, then assigns the values sequentially as the walk progresses. After the rebuild, the next insert-time `seq(<name>)` call resumes from `start+live_count`. |
| `:default=uuid()` | Fresh UUIDv4 generated per record (`/dev/urandom`). For `varchar:36` fields the canonical 36-char dashed string is stored; for the native `uuid` type the raw 16 bytes are stored. |
| `:default=random(<N>)` | Fresh `N` random bytes per record (`/dev/urandom`), hex-encoded to `2N` characters. The request is **refused pre-flight** if `2N` exceeds the field's storage cap — no rebuild is started in that case. |
| `:auto_create` / `:auto_update` | **Inert during backfill.** These are insert/update-time generators — the original record's creation timestamp is unknown, so stamping `now()` on every row would lie about history. Existing records keep zero bytes for this field; future inserts/updates fire the generator as normal. |
| (no modifier) | Existing records' bytes for the new field are zero. Decoders render that as the type's "absent" form (empty string for varchar, `0` for int, etc.). |

### Notes

- Existing record count and hash routing are preserved.
- Full object rebuild — scales with object size. Not instantaneous on millions of records.
- Sequence allocation is exact at walk start: the live count is read from the kf-header summary on the legacy slotcask handle, which is the authoritative source of record counts in v2.

## edit-field

Edit one or more existing fields in place — same-type only. Used to grow/shrink a `varchar`, widen/narrow an integer family field, change a `numeric`'s scale, widen `float → double`, or append / rename / widen an `enum`.

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

For enum renames, set `"allow_rename": true` at the top level — without it, any change at an existing enum position is rejected (renames are easy to typo and would silently relabel every existing record):

```json
{
  "mode": "edit-field",
  "dir": "acme", "object": "items",
  "fields": ["color:enum(crimson,green,blue,yellow)"],
  "allow_rename": true
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
| `enum(a,b,c) → enum(a,b,c,d,…)` (append) | Always allowed. Existing records keep their byte index; new value gets the next index. No rebuild needed; only `fields.conf` is updated. |
| `enum(a,b,c) → enum(a,b,c,…257+ values)` (auto-widen 1B → 2B) | Allowed. Triggers a full record rebuild (zero-extends each record's byte index) and rewrites the bitmap with the wider encoding. |
| `enum(a,b,c) → enum(x,b,c)` (rename at position) | Requires `"allow_rename": true` in the request body. Existing records keep their byte index; the displayed value changes. Without the flag, refused. |
| `enum(a,b,c) → enum(a,b)` (remove) | **Always refused.** Records reference values by position; removing would corrupt every record using the dropped value. |
| `enum(a,b,c) → enum(c,a,b)` (reorder) | **Refused** (caught by the position-by-position diff — same shape as remove). |
| `enum(…) → enum(…)` narrow 2B → 1B | **Refused.** Records that hold an index ≥ 256 would lose data. |

**Other cross-type edits are hard-refused** with the hint: use `add-field <new> + remove-field <old> + bulk-update` and migrate the data explicitly.

### What happens

1. Takes the object's **write lock** (`objlock_wrlock`).
2. Refuses immediately if `storage_version != 2` (point to `./migrate`).
3. Parses every edit spec; refuses on unknown field name, tombstoned field, duplicate edit in the same request, invalid type, or cross-type change.
4. Builds a new `TypedSchema` by overlaying each edited field onto a clone of the old schema (positions unchanged, only `size` / `offset` / `numeric_scale` move).
5. **No-op fast path** — if no field's encoding actually changed (a varchar staying the same size with only `:default=...` modifier shift, say), skip the data rebuild and rewrite `fields.conf` only.
6. Otherwise **pre-flight scan**: walks every live record across all keyfile shards and verifies each edited field's value fits the new shape. First violation aborts with `{"error":"Pre-flight failed on field [<name>]: <reason>"}` — no data or schema change.
7. **Rebuild**: runs the same v2 rebuild path used by `add-field` / `vacuum --compact`, but with `transform_field_value()` re-encoding the edited fields per record. Atomic — the legacy data tree is preserved until the rebuild succeeds.
8. Rewrites `fields.conf` to lock in the new spec.
9. **Smart reindex**: walks `index.conf` and only rebuilds indexes whose referenced fields actually changed encoding. Indexes referencing untouched fields are skipped — the response carries `indexes_skipped:N` alongside `indexes_rebuilt:N` so operators can verify.
10. Releases the write lock.

### Response

```json
{"status":"edited","fields":N,"rebuilt":true,"slot_size":N,"indexes_rebuilt":R,"indexes_skipped":S}
```

No-op fast path returns `{"status":"edited","fields":N,"rebuilt":false}` — fields.conf updated but no data rebuild ran.

### dry_run

Pass `"dry_run":true` to run every validation step (cross-type refusal, FT_ENUM prefix check, per-record varchar overflow + integer narrowing pre-flight) without writing anything.

```json
{"status":"ok","dry_run":true,"fields":N,"would_rebuild":true}
```

`would_rebuild:false` means every edit is encoding-equivalent — the change would only touch `fields.conf` (e.g. carrying through a new default modifier on an unchanged type). Useful before running a same-type narrow on a large object.

### Notes

- **Default modifier carry-through**: when a new edit spec OMITS `:default=...` / `:auto_create` / `:auto_update`, the modifier from the OLD `fields.conf` line is preserved — `edit-field age:long` against an existing `age:int:default=42` keeps `:default=42`.
- **Changing a default**: include the new modifier on the edit spec, e.g. `edit-field age:int:default=99`. Supported forms: `:default=<literal>`, `:default=seq(<name>)`, `:default=uuid()`, `:default=random(N)`, `:auto_create`, `:auto_update`. The new modifier affects **future inserts only** — existing records keep their stored values. For a one-shot backfill of existing records, use `add-field <new-name>` with the modifier, then `bulk-update` and `remove-field <old-name>`.
- **Indexed fields**: a varchar grow that doesn't shrink content, an integer widen, or any encoding-changing edit on an indexed field rebuilds only that index (smart reindex). Queries on the indexed field continue to resolve to the same records post-edit.
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
| `{"mode":"vacuum",...}` | Direction-C seg compaction — sparse non-active seg files are pair-merged into denser ones via kf-repoint, then unlinked. Active seg of each stream is never touched. *Also*: if the host's CPU count has changed since `create-object` and `slotcask_streams_for_nproc()` no longer matches `schema.streams`, the call automatically promotes to a full rebuild that re-routes records into the new stream layout. Idempotent. |
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

A full rebuild (`vacuum` with `compact`, `splits`, or streams correction; field add/edit/remove rebuilds) validates every live kf reference before it copies anything. If any live kf entry points to a missing or unreadable segment record, the rebuild aborts with `{"error":"Rebuild aborted: N invalid live kf reference(s); original data restored"}` and leaves the pre-rebuild object intact. Operators should restore from backup or use the previous release's repair tool (2026.07.1 `rebuild-kf`) against a backup copy.

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
| `add-index`, `remove-index` | `objlock_wrlock` | Same — both unlink()+rebuild index files in place; needs exclusivity against concurrent on-the-fly index writes (2026.07.1). |
| `backup`, `recount` | `objlock_rdlock` | Only schema mutations. |
| Normal CRUD / queries | `objlock_rdlock` | Only schema mutations. |

Held only for the rebuild duration. For multi-second rebuilds, clients see temporarily-blocked queries; consider running these in a maintenance window.

See [Concepts → Concurrency](../concepts/concurrency.md) for the full locking story.
