# Indexes

shard-db indexes are **B+ trees with prefix-compressed leaves**, stored per-object under `<object>/indexes/<field>/<NNN>.idx` — each field's btree is split into `splits/4` shards (the **per-shard btree layout**, 2026.05.1+). Reads fan out across all shards in parallel via the unified worker pool; writes route by record hash to a single shard. Every one of the 38 search operators uses an index when one is available (with a few intentional full-scan exceptions noted in [find → Operators](../query-protocol/find.md#operators)).

## When to add an index

Add an index when:
- You run `find` / `count` / `aggregate` filtered by that field, and
- The object is big enough that a full scan is noticeably slow (tens of thousands of records and up), or
- You'll use the field as a `join` `remote` key.

Don't bother for tiny objects or fields with very low cardinality (`bool`, `active: true/false`) — the index scan overhead isn't worth it vs a 2–3 ms shard scan.

## Single and composite indexes

### Single-field

```json
{"mode":"add-index","dir":"acme","object":"invoices","field":"customer"}
```

Files created: `<obj>/indexes/customer/000.idx` … `<NNN>.idx` (`splits/4` shards). For `splits=64`, that's 16 idx-shard files; for `splits=128`, 32 files; etc.

### Composite

Concatenate fields with `+`:

```json
{"mode":"add-index","dir":"acme","object":"invoices","field":"status+created"}
```

Directory created: `<obj>/indexes/status+created/<NNN>.idx`. The composite name (with `+`) becomes the directory; per-shard files inside.

Composite indexes store the **concatenation** of the listed field values as the key. They accelerate queries that filter on the **leading prefix** of the composite — e.g., `status+created` helps `WHERE status=? AND created>?`, but not `WHERE created>?` alone.

As many fields as you need can be joined (up to 16 per index). Order matters: pick fields by cardinality (highest first) for best range selectivity.

## Batch add

```json
{"mode":"add-index","dir":"acme","object":"invoices",
 "fields":["customer","status","status+created","product_sku"]}
```

Builds all of them in parallel with a **single** shard scan. Significantly faster than calling `add-index` once per field on large objects.

## Removing an index

```json
{"mode":"remove-index","dir":"acme","object":"invoices","field":"customer"}
```

Or batch: `"fields":["customer","status+created"]`.

Safe to call on a non-existent index — returns `{"status":"not_indexed",...}` (not an error). Queries on the dropped field fall back to full-shard scan.

## How queries pick an index

Given a `criteria` array, the planner:

1. Finds any criterion where the field is indexed and the operator is selective enough (anything other than pure `exists`/`not_exists`).
2. Uses that index as the **primary filter** — the B+ tree yields candidate hashes.
3. Runs the remaining criteria against each candidate record using `match_typed()` (zero-malloc byte compares).

For **composite** indexes, the planner matches against the leading-prefix pattern: `status="paid"` uses `status+created`, but `created > X` alone does not.

For **multi-criterion** queries where several fields are indexed, the single most selective index wins (roughly: the one with the most-equal-style operators on the leading field). A future optimization could intersect multiple indexes; today it picks one.

## Cost

Indexed lookups on 1 M records stay in the 1–3 ms band across all 17 operators. That's mostly:

- B+ tree descent: O(log N) page loads, hitting the warm `BT_CACHE` after first use.
- Candidate count: usually small for equality/range filters.
- Per-candidate record fetch: one Zone B memcpy + typed decode.

Full scans without an index are surprisingly fast (2–3 ms on 1 M records) because Zone A (24 bytes × slot count) stays in page cache, and most records reject on metadata without touching Zone B. But full scans are O(N), so they get expensive as the object grows past a few million records.

## B+ tree file format

Page-based, fixed `INDEX_PAGE_SIZE` (default 4096 bytes). Leaves are **prefix-compressed** every K=16 entries:

- Every 16th entry stores the full key (an **anchor**).
- Entries between anchors store only the suffix that differs from the preceding anchor.
- Search is two-stage: binary search over anchors, then linear within the 16-entry block.

The effect: leaves pack ~2–3× more keys per page than uncompressed. Range scans touch fewer pages.

## Index maintenance

Indexes are **kept in sync automatically** on `insert`, `update`, `delete`, and bulk ops. Every write call resolves the per-field value and either writes to (new value) or deletes from (old value) the index.

### When to rebuild

The server doesn't rebuild indexes automatically after schema changes. Rebuild manually (`add-index ... force:true`) if you suspect corruption, though under normal operation this shouldn't be needed.

After `vacuum --splits` (resharding), indexes are preserved — hash routing is identity-preserving, so the same hash still points to the same records.

After `remove-field`, any index referencing the tombstoned field is **automatically dropped**. Re-add after the field comes back (or permanently, after a compact vacuum).

## Naming rules

- Index name = exact field name used in `add-index`.
- Composite name uses `+` as the separator: `"country+zip"`. Don't use `+` in regular field names — it's reserved.
- Names are case-sensitive and must match the field name exactly (including any renames).

## Inspecting indexes

- `<obj>/indexes/index.conf` — authoritative list of registered indexes.
- `<obj>/indexes/<field>/<NNN>.idx` — per-shard B+ tree files (one directory per indexed field, `splits/4` files inside).
- `stats` output includes B+ tree cache hit rate — low hit rate on a read-heavy workload suggests raising `FCACHE_MAX` (which derives `BT_CACHE_MAX = FCACHE_MAX/4` automatically as of 2026.05.1).

## Why per-shard?

Pre-2026.05.1 each field was one big `<field>.idx` file. The 2026.05.1 redesign sharded that into `splits/4` files because:

1. **Concurrency hazard.** A writer doing `bulk_build` truncates and rewrites the file; a reader holding a private mmap saw inconsistent intermediate state. Per-file `pthread_rwlock_t` (one per shard) gives readers and writers proper isolation.
2. **Read parallelism.** `find` / `count` / `aggregate` fan out across all shards via `parallel_for`; with 16 idx shards on a 16-core box, indexed lookups parallelise N-way for free.
3. **Smaller per-file working set.** A 100M-row index that was 3 GB single-file becomes ~12 MB per shard at `splits=4096`. Easier on the page cache.

The trade is more on-disk space (~20-25 % bloat from reduced prefix-compression effectiveness with smaller per-leaf working sets, plus 1.8 MB of empty-tree headers for the typical 14-index schema) and a structural cost on bulk-insert into pre-existing-indexed objects (each merge call hits 16 files instead of 1). For static schemas, **load-then-index** is now preferred over insert-with-pre-existing-indexes.
