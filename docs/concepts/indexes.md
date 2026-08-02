# Indexes

## Index build and maintenance

Rebuilds publish each B-tree, trigram, and bitmap shard atomically. A builder
first materialises and syncs a sibling temporary file, then replaces the live
path. A concurrent reader may complete on the old complete file, while a reader
that begins after publication completes sees the replacement. Cache visibility
uses publication generations plus file-identity validation rather than a global
publication gate. A failed scan, spill read, or temporary write leaves the
previous live shard intact; an empty rebuild publishes a valid empty shard
rather than removing the index.

If the post-rename parent-directory sync fails, the replacement is already
live but its crash durability is unconfirmed. The command returns a durability
warning and operators should repeat the maintenance operation after resolving
the underlying filesystem error. Atomicity is per shard: a later shard failure
can leave an index with a mix of old and new shards, so retry the operation.
Target-shard publication and `index.conf` metadata publication have separate
failure and durability-warning states. Reindex removes obsolete
legacy/high-shard files only after all requested replacement shards were
published. Crash-abandoned generated temporary siblings are removed during
startup before index work begins.

shard-db ships three index types:

- **B+ tree** (default for every typed field) — prefix-compressed leaves at `<object>/indexes/<field>/<NNN>.idx`. Each field's btree is split into `index_splits_for(splits)` shards — a non-linear fan-out curve (`8→2, 16→4, 32→4, 64→8, 128→16, 256→16, 512→32, 1024→64, 2048→64, 4096→128`) that caps idx file count at high split values without sacrificing read parallelism at moderate splits. Reads fan out across all idx-shards in parallel via the unified worker pool; writes route by record hash to a single shard. Every search operator uses the btree when available (default fallthrough is a full-leaf scan with per-entry criterion check — still cheaper than scanning the data files since leaves are smaller than records).
- **Bitmap** (auto-default for `bool` and `enum` fields since 2026.05.7; opt-in via `"field:bitmap"` or `"field:bitmap(N)"` for low-cardinality `varchar`) — one dense bit per slot, per-distinct-value, at `<object>/indexes/<field>/<NNN>.bm`. The bitmap shards 1:1 with data shards so bit `i` of shard `s` = "slot `i` in data shard `s` has this value". Default cap is 256 distinct values per (shard, field); override with `bitmap(N)` up to 65535. For `enum` fields the cap matches the field's declared domain (256 for 1-byte enums, 65535 for 2-byte enums declared with >256 values). The planner routes `eq` / `in` / `neq` / `not_in` through a popcount-style fast path and every other op through a per-shard dict-scan (≤ cap dict entries iterated, decoded, matched, then the matching value-bitmaps walked). Either way, queries route through the index file — never the data file.
- **Trigram** (opt-in via `"field:trigram"` on `varchar`; new in 2026.05.7) — substring index for `contains` and `i_contains` queries. Each record contributes one entry per distinct 3-byte lowercased trigram extracted from the field's value into `<object>/indexes/<field>/<NNN>.tg` (uses the same BTRH btree format as `.idx` so existing infrastructure handles caching, range scans, and reads). Queries extract trigrams from the pattern, look up posting lists per trigram, intersect rarest-first, then per-record verify the substring (kills false positives from order-insensitive trigram matching). When a field has both btree AND trigram, the planner auto-picks btree-leaf scan for short patterns (`< 6` chars — verify cost dominates trigram for non-selective common substrings) and trigram for longer patterns. Build uses external-merge sort: bounded per-worker memory + tight prefix-compressed leaves at any scale (25M, 100M, 1B records).

## When to add an index

Add an index when:
- You run `find` / `count` / `aggregate` filtered by that field, and
- The object is big enough that a full scan is noticeably slow (tens of thousands of records and up), or
- You'll use the field as a `join` `remote` key.

For `bool` and `enum` fields you usually don't need to make a decision — the auto-default puts them on bitmap. Bitmap pays off most when:
- Your queries `count` / `aggregate` on the field (popcount is O(slots / 64), no btree leaf walk per match).
- Selectivity is broad (10%+ of records match). At those selectivities a btree leaf walk emits one cb per match; bitmap returns in popcount time.

Bitmap's per-shard dict cap is 256 by default (override `bitmap(N)` up to 65535). When the cap is exceeded the wire layer surfaces an actionable error pointing at btree — the operator should switch the field's index type.

## Single and composite indexes

### Single-field

```json
{"mode":"add-index","dir":"acme","object":"invoices","field":"customer"}
```

Files created: `<obj>/indexes/customer/000.idx` … `<NNN>.idx` (`index_splits_for(splits)` shards). For `splits=64`, that's 8 idx-shard files; for `splits=128`, 16 files; for `splits=4096`, 128 files. See the curve table above.

### Explicit type

`add-index` / `create-object` accept an explicit type suffix when you want bitmap or trigram on a field that wouldn't auto-default to that type:

```json
{"mode":"add-index","dir":"acme","object":"users","field":"status:bitmap"}
{"mode":"add-index","dir":"acme","object":"users","field":"status:bitmap(32)"}
{"mode":"add-index","dir":"acme","object":"posts","field":"body:trigram"}
```

| spec | files created (per data shard) | use case |
|---|---|---|
| `field` (no suffix) | `<obj>/indexes/<field>/<NNN>.idx` (btree fan-out curve) | default — every field type |
| `field:btree` | same as above (explicit) | force btree on a bool/enum (suppresses auto-bitmap) |
| `field:bitmap` | `<obj>/indexes/<field>/<NNN>.bm` (1:1 with data shards) | low-cardinality varchar; `eq` / `in` / `neq` hot paths |
| `field:bitmap(N)` | same, with cap `N ∈ [2, 65535]` | override the default 256-value cap |
| `field:trigram` | `<obj>/indexes/<field>/<NNN>.tg` (btree fan-out curve, BTRH format) | substring search on varchar — `contains` / `i_contains` |

Auto-defaults: `bool` and `enum` fields → bitmap. Everything else → btree. To override an auto-default, declare the type explicitly (e.g., `active:btree` keeps a bool on btree). Multiple types on the same field are allowed and useful — e.g., `email` declared as both `email` (btree, for eq lookups) AND `email:trigram` (for substring search) — the planner picks per-query.

Trigram cost notes:
- Disk: ~5× the btree's footprint on the same field (one entry per (record × distinct trigram in value) vs btree's one per record), but still bounded — see `estimate-index` to project before committing.
- Build: external-merge sort (bounded memory at any scale). Per-shard merge runs in parallel up to pool size.
- Query: rarest-trigram-first intersect skips full posting walks when patterns are selective. For non-selective common short patterns where btree also exists on the field, the planner auto-picks btree-leaf scan instead (configurable threshold at 6 chars).

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

Given a `criteria` array, the planner picks one of five paths:

- **`PRIMARY_LEAF`** — single indexed AND leaf drives the candidate set; remaining criteria are post-filtered on the records via `match_typed()` (zero-malloc byte compares).
- **`PRIMARY_INTERSECT`** — pure AND of 2+ indexed leaves on rangeable operators (eq, lt, gt, lte, gte, between, in, starts_with). Walks each leaf's btree into a lock-free hash KeySet, intersects the sets, and skips per-record fetch entirely for `count` and AND-only `aggregate`. Capped at `MAX_INTERSECT_LEAVES = 8` indexed leaves — past that the planner falls back to `PRIMARY_LEAF`.
- **`PRIMARY_KEYSET`** — pure OR (every child indexed) unions candidate hashes into a KeySet; pure-OR `count` returns `|KeySet|` directly.
- **Hybrid AND+OR** — indexed AND leaf as primary, OR sub-tree as per-record post-filter.
- **`PRIMARY_NONE`** — no indexable leaf; parallel scan across every kf shard. Each worker probes its shard's slot array, follows live slots into the segcache, runs `match_typed()` on the payload.

For **composite** indexes, the planner matches against the leading-prefix pattern: `status="paid"` uses `status+created`, but `created > X` alone does not.

Selectivity rank for intersect ordering: `eq < starts_with < between < in < range`. Substring/suffix ops (`like`, `contains`, `ends`, `not_*`) and large-set ops (`neq`, `not_in`) cannot drive intersection.

## Cost

Indexed lookups on 1 M records stay in the 1–3 ms band across most of the 38 operators. That's mostly:

- B+ tree descent: O(log N) page loads, hitting the warm `bt_cache` after first use.
- Candidate count: usually small for equality/range filters.
- Per-candidate record fetch: one kf slot read + one seg payload memcpy + typed decode.

`len_*` operators (varchar length filters) answer entirely from btree-leaf metadata — no record fetch at all, since the leaf carries the field's encoded length. These are the fastest non-equality lookups in the system.

Full scans without an index parallelize across kf shards (`THREADS` workers); each worker walks its shard's slot array and only follows live slots into segments. Many records reject on the in-kf hash alone (e.g. eq queries with a hash-routed shard) without ever touching a segment. But full scans are O(N), so they get expensive as the object grows past a few million records.

## B+ tree file format

Page-based, fixed `INDEX_PAGE_SIZE` (default 4096 bytes). Magic `BTRH` (v4 format, 2026.05.5+ — entries sort by `(value, hash)`). Leaves are **prefix-compressed** every K=16 entries:

- Every 16th entry stores the full key (an **anchor**).
- Entries between anchors store only the suffix that differs from the preceding anchor.
- Search is two-stage: binary search over anchors, then linear within the 16-entry block.

The effect: leaves pack ~2–3× more keys per page than uncompressed. Range scans touch fewer pages.

V4 (`BTRH`) adds the `(value, hash)` total order. Within a duplicate-value cluster, hashes break ties so every entry has a unique sort position. Internal pages carry both the separator value and its hash, so descent for a known `(value, hash)` tuple routes directly to the unique leaf holding it — `btree_delete` is O(log N) again instead of falling back to a chain walk.

V3 (`BTRG`) added two header fields that make DESC iteration O(1)-step. V4 keeps them:

- `BtFileHeader.last_leaf_page` — pointer to the rightmost leaf. DESC iterators start here in O(1).
- `BtPageHeader.prev_leaf` — backward link maintained on every leaf split. DESC steps left one page at a time via `ph->prev_leaf` instead of indexing into a precomputed array.

Older formats (`BTRE`, `BTRF`, `BTRG`) are rejected at open with a clear error and require a reindex. Run 2026.05.5's `./migrate` once — it spawns the daemon, calls `./shard-db reindex`, and stops the daemon. Idempotent: re-running on an already-BTRH install just rebuilds btrees in their current format. From pre-2026.05.1 installs, install 2026.05.4 first and run that release's `./migrate` to convert v1 objects to slotcask, then upgrade to 2026.05.5 and re-run `./migrate`.

## Index maintenance

Indexes are **kept in sync automatically** on `insert`, `update`, `delete`, and bulk ops. The slotcask engine's `pre_commit` hook fires while the kf wrlock is held — it resolves the per-field value for both the new payload and (on update) the old payload, and writes to (new value) or deletes from (old value) every relevant idx shard before the kf atomic store commits the record. If any index update fails, the commit is aborted and the record is not visible to readers.

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
- `<obj>/indexes/<field>/<NNN>.idx` — per-shard B+ tree files (one directory per indexed field, `index_splits_for(splits)` files inside).
- `stats` output includes B+ tree cache hit rate (`bt_cache.hits / (hits + misses)`) — low hit rate on a read-heavy workload suggests raising `FCACHE_MAX` (which derives `BT_CACHE_MAX = FCACHE_MAX/4` automatically as of 2026.05.1).

## Why per-shard?

Pre-2026.05.1 each field was one big `<field>.idx` file. The 2026.05.1 redesign sharded that into per-`splits` slices because:

1. **Concurrency hazard.** A writer doing `bulk_build` truncates and rewrites the file; a reader holding a private mmap saw inconsistent intermediate state. Per-file `pthread_rwlock_t` (one per idx-shard) gives readers and writers proper isolation.
2. **Read parallelism.** `find` / `count` / `aggregate` fan out across all idx-shards via `parallel_for`; with 16 idx shards on a 16-core box, indexed lookups parallelise N-way for free.
3. **Smaller per-file working set.** A 100M-row index that was 3 GB single-file becomes manageable per shard. Easier on the page cache.

The trade is more on-disk space (~20-25 % bloat from reduced prefix-compression effectiveness with smaller per-leaf working sets, plus ~100KB of empty-tree headers per index at higher fan-outs) and a structural cost on bulk-insert into pre-existing-indexed objects. For static schemas at large scale (R = total records / 200K ≥ ~20 parallel chunks), **load-then-index** is preferred; for smaller-scale or steady-state workloads, parallel-insert-with-pre-existing-indexes wins. The 2026.05.2 pre-grow optimisation made every path ~2× faster, but the crossover rule still applies.
