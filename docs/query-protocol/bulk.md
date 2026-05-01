# Bulk operations

Single round-trip operations for inserting, updating, or deleting many records at once. All support `dry_run` (for update / delete) and respect CAS. Parallel index build on bulk-insert.

## bulk-insert

### Shape

`records` (or the file contents) accepts either of two shapes — pick whichever round-trips with the producer:

**Dict form** (round-trips with `get-multi`):

```json
{
  "mode": "bulk-insert",
  "dir": "<dir>",
  "object": "<obj>",
  "records": {
    "<key1>": {...},
    "<key2>": {...}
  }
}
```

**Array form** (one record per element):

```json
{
  "mode": "bulk-insert",
  "dir": "<dir>",
  "object": "<obj>",
  "records": [
    {"key":"<key>","value":{...}},
    {"key":"<key>","value":{...}}
  ]
}
```

Or from a file:

```json
{"mode":"bulk-insert","dir":"<dir>","object":"<obj>","file":"/path/to/data.json"}
```

File content is the same JSON (either shape — one big object or one big array, not NDJSON).

### Record shape (array form)

- `"key"` — the record key.
- `"value"` — an object whose fields match the typed schema.

> **Breaking change in 2026.05.1**: array-form record fields renamed from `"id"` / `"data"` to `"key"` / `"value"` for consistency with single-`insert` and the dict shape. Update existing payloads.

### Upsert semantics

`bulk-insert` is a **true upsert**: if a key already exists, its record is overwritten and any stale index entries pointing at the old value are dropped before the new entry is written. Pass `"if_not_exists": true` to make it idempotent (existing keys are skipped, response includes `"skipped":N`).

### Response

```json
{"inserted": 10000}
```

With `if_not_exists` and pre-existing keys:
```json
{"inserted": 9997, "skipped": 3}
```

If any records were rejected (type mismatch, missing required field, etc.):
```json
{"inserted": 9997, "skipped": 0, "errors": 3, "error": "some_records_dropped"}
```

### Performance

- **Single-request bulk loads** — ~117 k records/sec single-thread on the invoice schema (1 M records, 14 indexes, splits=64). Add-indexes-from-scratch ≈ 350 k records/sec equivalent. See the README performance tables for the full set.
- **Parallel index build** — one worker per indexed field, each streaming the per-shard merges sequentially within the worker (per-shard btree layout, 2026.05.1+). 14 fields × `splits/4` shards run as 14 dispatched tasks.
- **Indexed bulk-insert chunk-size guidance** — for parallel inserts into a pre-existing-indexed object, prefer **fewer, larger** bulk-insert calls. Each call triggers a sequential `bulk_merge` per (field, shard); cumulative work scales `O(R²)` in request count `R`. At 1 M records, **5 connections × 200 K each** is the sweet spot. Bigger chunks always win on the indexed path.

### Pattern: load + index in one go

Create the object with indexes listed, then bulk-insert. Indexes are built as part of the load:

```json
{"mode":"create-object","dir":"default","object":"users","splits":16,"max_key":128,
 "fields":["email:varchar:200","name:varchar:100","age:int"],
 "indexes":["email","age"]}
```

## bulk-insert-delimited

CSV-style flat files. Faster to generate than JSON for large exports.

```json
{
  "mode": "bulk-insert-delimited",
  "dir": "<dir>",
  "object": "<obj>",
  "file": "/path/to/data.csv",
  "delimiter": "|"
}
```

### Wire format

- Every line is a record. **No header row.**
- First column = key.
- Remaining columns = field values in `fields.conf` order (skipping tombstoned fields).
- Default delimiter: `|`. Pass any single character via `"delimiter"`.

Example file (delimiter `|`):
```
u1|Alice|alice@x.com|30|true
u2|Bob|b@x.com|22|true
u3|Carol|c@x.com|45|false
```

For an object with fields `name:varchar:100|email:varchar:200|age:int|active:bool`.

Use `|` (default) or `,` for CSV — any byte that isn't present in your data works.

### Value encoding

- **varchar** — raw bytes (no quoting).
- **int / long / short** — ASCII digits, optional `-` prefix.
- **bool** — `true` / `false`.
- **date** — `yyyyMMdd`.
- **datetime** — `yyyyMMddHHmmss`.
- **numeric** — decimal matching the scale.

No escaping. Pick a delimiter your data doesn't contain.

## bulk-delete

### By key list

```json
{
  "mode": "bulk-delete",
  "dir": "<dir>",
  "object": "<obj>",
  "keys": ["k1", "k2", "k3"]
}
```

Or from file:

```json
{"mode":"bulk-delete","dir":"<dir>","object":"<obj>","file":"/path/to/keys.json"}
```

File = JSON array of keys.

Response: `{"deleted": <N>}` (keys that weren't present are silently skipped).

### By criteria

```json
{
  "mode": "bulk-delete",
  "dir": "<dir>",
  "object": "<obj>",
  "criteria": [{"field":"status","op":"eq","value":"cancelled"}],
  "limit": 1000,
  "dry_run": true
}
```

Response (dry run):
```json
{"matched":437,"deleted":0,"skipped":0,"dry_run":true}
```

Response (actual):
```json
{"matched":437,"deleted":437,"skipped":0}
```

- `limit` caps the number of deletions (defensive — prevents runaway deletes).
- `skipped` counts records that matched the criteria but failed an `if` check (not shown above, but possible when combined).
- Always dry-run first for criteria-based deletes in production.

## bulk-update

`bulk-update` has three sub-shapes — dispatched by which field is set in the request.

### Criteria-driven mass update

```json
{
  "mode": "bulk-update",
  "dir": "<dir>",
  "object": "<obj>",
  "criteria": [{"field":"status","op":"eq","value":"pending"}],
  "value": {"status":"expired"},
  "limit": 10000,
  "dry_run": true
}
```

- Updates every record matching `criteria` by merging `value` into the existing record.
- Per-record CAS applies: `auto_update` fields bump, indexes stay in sync.
- `dry_run:true` returns the match count without writing.
- `limit` caps the update.

Response: `{"matched":2450, "updated":2450, "skipped":0}`.

### Per-key partial update — inline records

```json
{
  "mode": "bulk-update",
  "dir": "<dir>",
  "object": "<obj>",
  "records": {
    "<key1>": {"<field>":"<new value>"},
    "<key2>": {"<field>":"<new value>"}
  }
}
```

Or the array form:

```json
{
  "mode": "bulk-update",
  "dir": "<dir>",
  "object": "<obj>",
  "records": [
    {"key":"<key1>","value":{"<field>":"<new value>"}},
    {"key":"<key2>","value":{"<field>":"<new value>"}}
  ]
}
```

- Updates each named key by merging the inner object into the existing record. **Only fields present overwrite**; absent fields are kept as-is.
- Missing keys count toward `skipped`, not `updated`.
- Indexes stay in sync.

### Per-key partial update — from a file

```json
{"mode":"bulk-update","dir":"<dir>","object":"<obj>","file":"/path/to/patches.json"}
```

File contents are the same shape (dict or array). Same semantics as the inline form.

> **Breaking change in 2026.05.1**: the array form's record fields renamed from `"id"` / `"data"` to `"key"` / `"value"`. Match the single-`insert` and dict-shape conventions.

## Dry-run workflow

For `bulk-update` and `bulk-delete` on criteria, **always** run `dry_run:true` first in production:

```json
// 1. Dry run — check blast radius
{..., "dry_run":true}

// 2. Execute — same criteria, drop dry_run
{..., "limit":10000}
```

## Crash safety

- Each record commits atomically — per-slot flag flip, same as a single `insert`/`update`/`delete`. A mid-batch crash leaves the records written up to that point in place; nothing is half-written.
- The batch as a whole is **not** transactional — there's no all-or-nothing across records. `bulk-insert` dispatches records to per-shard workers in parallel; the per-shard btree merge runs once per `(field, shard)` at the end of the batch under the per-shard rwlock.
- For atomicity across multiple records, stage intent in a control record and reconcile in the app.

## Performance tips

- **Bulk-insert over many singles** — amortizes connection setup, schema lookup, and (critically) enables the parallel index build.
- **Create indexes before the bulk load** — so the load builds them inline. Adding indexes on a loaded object re-scans everything.
- **Delimited (`|`-separated) over JSON** for huge CSV ingest — parses ~10–20% faster.
- **Tune `MAX_REQUEST_SIZE`** if sending multi-million-record bulk loads in one request. 32 MB default holds ~300 k records at typical sizes.
- **Pipeline many moderate bulk-inserts** rather than one huge one — keeps each request under the size limit and lets the server progress-log in info logs.

## What bulk ops don't support

- **Per-record error recovery in JSON** — if record #50 fails type validation, records 1–49 are committed and the response's `errors` count goes up. The failed record's details are logged but not returned to the client today.
- **Heterogeneous objects in one call** — a bulk operation is scoped to one `(dir, object)` pair.
