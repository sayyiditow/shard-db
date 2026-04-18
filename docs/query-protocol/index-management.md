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
- Builds with parallel shard scans (one pthread per index field when multiple are requested).
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

- Unlinks the `.idx` file.
- Rewrites `index.conf` without the removed entry.
- Invalidates `g_idx_cache` and the B+ tree mmap cache for that file.
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

## What `add-field` / `remove-field` do to indexes

- `remove-field` **automatically drops** any index referencing the removed field (including composites). You don't need to call `remove-index` separately.
- `add-field` doesn't create indexes for the new field. Call `add-index` if you want one.
- `rename-field` renames the `.idx` file and updates composite references.

## CLI shortcuts

```bash
./shard-db add-index <dir> <obj> <field> [-f]      # -f forces rebuild
./shard-db remove-index <dir> <obj> <field>
```

For batch adds/removes, use the JSON mode above.

## Inspection

```bash
cat $DB_ROOT/<dir>/<obj>/indexes/index.conf
ls  $DB_ROOT/<dir>/<obj>/indexes/
```

Index file format is binary (B+ tree with prefix-compressed leaves). Use [`stats`](diagnostics.md) to see the B+ tree mmap cache hit rate.

## When to force-rebuild

`force:true` on `add-index` drops and re-creates the index from a fresh shard scan. Reasons:

- You suspect `.idx` corruption (rare — the server refuses to read corrupt trees on open).
- You added/removed many records in ways that could have skewed leaf layout (the tree is self-rebalancing, but a one-shot rebuild yields optimal page layout).
- You're migrating / resharding and want a known-good state.

Force-rebuild has the same cost as initial build — O(N) scan, B+ tree bulk-load. Normal index maintenance is incremental (updated on every write) and doesn't need rebuilds.
