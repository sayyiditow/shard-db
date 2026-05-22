# Index management

Create and drop btree, bitmap, and trigram indexes. For the conceptual model and type trade-offs, see [Concepts → Indexes](../concepts/indexes.md).

## add-index

### Single field

```json
{"mode":"add-index","dir":"<dir>","object":"<obj>","field":"email"}
```

Optional `"force":true` rebuilds even if the index already exists (useful after suspected corruption).

### Explicit type suffix

Suffix the field name to pick a non-default index type:

```json
{"mode":"add-index","dir":"<dir>","object":"<obj>","field":"status:bitmap"}
{"mode":"add-index","dir":"<dir>","object":"<obj>","field":"status:bitmap(32)"}
{"mode":"add-index","dir":"<dir>","object":"<obj>","field":"body:trigram"}
```

| spec | type | files written | use for |
|---|---|---|---|
| `field` | btree (or bitmap if bool/enum auto-default) | `<field>/<NNN>.idx` | default — every field type |
| `field:btree` | btree (explicit) | same | force btree on bool/enum (suppresses auto-bitmap) |
| `field:bitmap` | bitmap, cap=256 | `<field>/<NNN>.bm` (1:1 with data shards) | low-cardinality varchar, fast eq/in/neq |
| `field:bitmap(N)` | bitmap, cap=`N` | same | override default cap (`N ∈ [2, 65535]`) |
| `field:trigram` | trigram | `<field>/<NNN>.tg` (btree fan-out curve) | varchar substring search (`contains` / `i_contains`) |

A field may have multiple index types simultaneously (e.g. both `username` and `username:trigram`). The planner picks per-query.

### Multiple fields (parallel build)

```json
{"mode":"add-index","dir":"<dir>","object":"<obj>",
 "fields":["email","status","city+country"]}
```

Builds all listed indexes in a **single shard scan** — much faster than calling `add-index` once per field on large objects.

### Composite

Concatenate field names with `+`:

```json
{"mode":"add-index","dir":"<dir>","object":"<obj>","field":"country+zip"}
```

Stores the concatenation of `country` + `zip` as the index key. Accelerates queries filtering on `country` alone (leading prefix) or on `country AND zip`. Does **not** help queries filtering on `zip` alone.

### Behavior

- If the index already exists and `force:true` is not set: `{"status":"exists","field":"..."}`.
- Build pipeline by type:
    - **btree** — external-merge sort: per-kf-shard parallel walks spill sorted runs to temp files; per-output-shard k-way merge feeds a streaming `bulk_build`. Bounded per-worker memory (`INDEX_BUILD_BUDGET_MB`), tight prefix-compressed leaves. Files: `<obj>/indexes/<field>/<NNN>.idx` (`index_splits_for(splits)` shards).
    - **bitmap** — parallel kf-shard walks write `bm_set` directly into mmap'd `.bm` files. No accumulation (mmap is the durable store). Files: `<obj>/indexes/<field>/<NNN>.bm` (1:1 with data shards).
    - **trigram** — same external-merge pipeline as btree, but each record contributes one entry per distinct trigram. Files: `<obj>/indexes/<field>/<NNN>.tg` (BTRH btree format, `index_splits_for(splits)` shards).
- Updates `<obj>/indexes/index.conf`.
- Invalidates `g_idx_cache` for the object.

Response (single): `{"status":"indexed","field":"email","records":N,"duration_ms":T}`.
Response (multi): `{"status":"indexed","count":3,"records":N,"duration_ms":T}` (or `{"status":"ok",...}` if all fields were typed and no btree work happened).

## estimate-index

Projects the on-disk size and per-record trigram count for a hypothetical trigram index before committing to the build. Useful for capacity planning on large objects.

```json
{"mode":"estimate-index","dir":"<dir>","object":"<obj>","spec":"body:trigram"}
```

Samples up to 1024 live records, extracts distinct trigrams per record, returns aggregated stats:

```json
{"records":N,"sample_size":S,"avg_distinct_trigrams":A,"projected_entries":E,"projected_bytes":B}
```

Only `:trigram` specs are supported today (btree and bitmap sizes are derivable from `live × per_entry` directly). See [Concepts → Indexes](../concepts/indexes.md#trigram-cost-notes) for the cost model.

## remove-index

### Single field

```json
{"mode":"remove-index","dir":"<dir>","object":"<obj>","field":"email"}
```

### Multiple

```json
{"mode":"remove-index","dir":"<dir>","object":"<obj>","fields":["email","city+country"]}
```

### Behavior

- Looks up the matched line in `index.conf` to determine the index type, then unlinks the appropriate files:
    - **btree** → `<NNN>.idx` files + the `<field>/` directory
    - **bitmap** → `<NNN>.bm` files + bm_cache invalidation + the `<field>/` directory
    - **trigram** → `<NNN>.tg` files + btree_cache invalidation + the `<field>/` directory
- For fields with multiple index types, pass the explicit suffix to drop just one: `"field":"email:trigram"` drops only the trigram, leaving any btree intact.
- Rewrites `index.conf` without the removed entry.
- Invalidates `g_idx_cache` for the object.
- Safe on non-existent index: returns `{"status":"not_indexed","field":"..."}` — not an error. Idempotent.

Response (single): `{"status":"removed","field":"email"}` or `{"status":"not_indexed","field":"..."}`.
Response (multi): `{"status":"removed","count":N,"not_indexed":M}`.

### Post-removal behavior

Queries referencing the dropped field fall back to full-shard scan. Re-add the index if the workload is query-heavy on that field.

## Composite naming rules

- Fields joined with `+`: `"status+created"`, `"country+zip+city"`.
- Name must match **exactly** when removing — no spaces, no alternate orderings.
- Up to 16 fields per composite.
- Don't use `+` in regular field names.

## What `add-field` / `remove-field` / `vacuum --splits` do to indexes

- `remove-field` **automatically drops** any index referencing the removed field (including composites). You don't need to call `remove-index` separately.
- `add-field` doesn't create indexes for the new field. Call `add-index` if you want one.
- `rename-field` renames the `indexes/<field>/` directory (all `NNN.idx` files travel with the rename) and updates composite references.
- `vacuum --splits` triggers a **full reindex** because the per-field idx-shard count is `index_splits_for(splits)` — changing splits changes the layout. See [Schema mutations → vacuum](schema-mutations.md#vacuum).

## CLI shortcuts

```bash
./shard-db add-index <dir> <obj> <field> [-f]      # -f forces rebuild
./shard-db remove-index <dir> <obj> <field>
```

For batch adds/removes, use the JSON mode above.

## Inspection

```bash
cat $DB_ROOT/<dir>/<obj>/indexes/index.conf      # registered indexes (one per line, canonical form)
ls  $DB_ROOT/<dir>/<obj>/indexes/                # one directory per indexed field
ls  $DB_ROOT/<dir>/<obj>/indexes/<field>/        # per-shard files: NNN.idx (btree), NNN.bm (bitmap), NNN.tg (trigram)
```

`index.conf` lines are canonical specs: bare `field` for btree, `field:bitmap`, `field:bitmap(N)`, or `field:trigram`. A field with multiple index types appears on multiple lines. Use [`stats`](diagnostics.md) to see the B+ tree mmap cache hit rate (`bt_cache.hits / misses`) which covers both `.idx` and `.tg` files; bitmap cache stats appear under `bm_cache.*`.

Stale orphan files from a previous, higher `splits` value would survive `add-index` (it only writes `0..index_splits_for(splits)-1`); use `./shard-db reindex <dir> <obj>` to wipe and rebuild every per-field idx directory cleanly.

## When to force-rebuild

`force:true` on `add-index` drops and re-creates the index from a fresh shard scan. Reasons:

- You suspect `.idx` corruption (rare — the server refuses to read corrupt trees on open).
- You added/removed many records in ways that could have skewed leaf layout (the tree is self-rebalancing, but a one-shot rebuild yields optimal page layout).
- You're migrating / resharding and want a known-good state.

Force-rebuild has the same cost as initial build — O(N) scan, B+ tree bulk-load. Normal index maintenance is incremental (updated on every write) and doesn't need rebuilds.
