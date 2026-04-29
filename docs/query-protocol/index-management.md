# Index management

Create and drop B+ tree indexes. For the conceptual model, see [Concepts → Indexes](../concepts/indexes.md).

## add-index

### Single field

```json
{"mode":"add-index","dir":"<dir>","object":"<obj>","field":"email"}
```

Optional `"force":true` rebuilds even if the index already exists (useful after suspected corruption).

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
- Builds via a single shard scan that fans out to per-field workers — each indexed field becomes one task in the parallel-for pool. The worker buckets entries by idx-shard and merges them sequentially per shard (per-shard btree layout, 2026.05.1+).
- Creates `<obj>/indexes/<field>/<NNN>.idx` — `splits/4` files per indexed field.
- Updates `<obj>/indexes/index.conf`.
- Invalidates `g_idx_cache` for the object.

Response (single): `{"status":"indexed","field":"email"}`.
Response (multi): `{"status":"indexed","count":3}`.

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

- Unlinks every `<NNN>.idx` file under `indexes/<field>/` and removes the directory.
- Rewrites `index.conf` without the removed entry.
- Invalidates `g_idx_cache` and the B+ tree mmap cache for those files.
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
- `vacuum --splits` triggers a **full reindex** because the per-field idx-shard count is `splits/4` — changing splits changes the layout. See [Schema mutations → vacuum](schema-mutations.md#vacuum).

## CLI shortcuts

```bash
./shard-db add-index <dir> <obj> <field> [-f]      # -f forces rebuild
./shard-db remove-index <dir> <obj> <field>
```

For batch adds/removes, use the JSON mode above.

## Inspection

```bash
cat $DB_ROOT/<dir>/<obj>/indexes/index.conf      # registered indexes (one per line)
ls  $DB_ROOT/<dir>/<obj>/indexes/                # one directory per indexed field
ls  $DB_ROOT/<dir>/<obj>/indexes/<field>/        # per-shard NNN.idx files (splits/4 of them)
```

Index file format is binary (B+ tree with prefix-compressed leaves, page size = `INDEX_PAGE_SIZE`). Use [`stats`](diagnostics.md) to see the B+ tree mmap cache hit rate (`bt_cache.hits / misses`).

Stale orphan files from a previous, higher `splits` value would survive `add-index` (it only writes `0..splits/4-1`); use `./shard-db reindex <dir> <obj>` to wipe and rebuild every per-field idx directory cleanly.

## When to force-rebuild

`force:true` on `add-index` drops and re-creates the index from a fresh shard scan. Reasons:

- You suspect `.idx` corruption (rare — the server refuses to read corrupt trees on open).
- You added/removed many records in ways that could have skewed leaf layout (the tree is self-rebalancing, but a one-shot rebuild yields optimal page layout).
- You're migrating / resharding and want a known-good state.

Force-rebuild has the same cost as initial build — O(N) scan, B+ tree bulk-load. Normal index maintenance is incremental (updated on every write) and doesn't need rebuilds.
