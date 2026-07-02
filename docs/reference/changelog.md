# Changelog

This is the maintained per-release summary. The root [`CHANGELOG.md`](https://github.com/sayyiditow/shard-db/blob/main/CHANGELOG.md) is a pointer here; rich per-version notes (motivation, migration, code references) live in [`docs/release-notes/`](../release-notes/).

Versions follow `yyyy.mm.N` — year-month, with `N` as the counter within that month.

## 2026.07.1

Compact VARCHAR storage, Natural Query Language (NQL), kf-corruption recovery, and an `add-index`/`remove-index` locking fix. Covers everything merged since 2026.06.3. Full notes: [docs/release-notes/2026.07.1.md](../release-notes/2026.07.1.md). Wire-compatible with 2026.06.3.

### Storage: Compact VARCHAR (run `./migrate`)

- **VARIABLE-format segments** — shard-db now stores typed records as exactly `24 + key_len + value_len` bytes in segment files, with no zero-padding to a fixed slot size. For typical varchar workloads where most field values are short, this can reduce segment file sizes by 50–90%.

- **Trailing-zero trim on every write** — `typed_encode_trim_len()` walks the typed field layout from the end and computes the shortest prefix that fully encodes all non-zero fields. Zero fields at the end are omitted; the decoder fills them back as zero on read. Safe for all field types: zero = empty string (varchar), 0 (int/long/short), 0.0 (double), false (bool), epoch (date/datetime). Happens automatically on every INSERT / UPDATE / bulk-insert after upgrading.

- **`compact` command** — rewrites all existing segment files for an object, applying the trim to every record. Needed to reclaim space from records written by older binary versions.

  ```bash
  # CLI (server must be stopped)
  ./shard-db compact default stories

  # JSON API (server running, requires rwx permission)
  ./shard-db query '{"mode":"compact","dir":"default","object":"stories"}'
  ```

- **`./migrate` runs compact automatically** — Phase 2 of the migrate tool iterates all objects and calls compact on each one. Run once after upgrading:

  ```bash
  ./shard-db stop
  ./migrate          # converts format + compacts all objects
  ./shard-db start
  ```

  Idempotent — safe to re-run if interrupted.

### Features: NQL wire protocol

- **NQL wire protocol** — the server now accepts NQL on the same TCP port as JSON, detected by the first character of the request line: `{` = JSON, `f`/`c`/`a` = NQL (`find`/`count`/`aggregate`). Response framing (`\0\n` terminator, pipelining) is identical.

- **NQL filter grammar** — a recursive-descent parser (`nql.c`) produces the same `CriteriaNode *` tree the JSON engine already uses. Full grammar:
  - `and-expr` / `or-expr` with correct AND > OR precedence; parentheses for grouping.
  - `BETWEEN value AND value` (the disambiguating `and` is consumed by `between`, not the logical combinator).
  - `IN (v1, v2, ...)` / `NOT IN [v1, v2, ...]` with either bracket style.
  - All 38 operators, including symbolic aliases (`=`, `!=`, `>`, `<`, `>=`, `<=`) and keyword aliases (`gt`, `lt`, `gte`, `lte`, `eq`, `neq`).
  - Existence checks: `field exists` / `field not_exists`.
  - String/regex ops: `contains`, `starts`, `ends`, `like`, `regex`, case-insensitive `i`-variants.
  - Length ops: `len_eq`, `len_gt`, `len_lt`, `len_between`, `len_gte`, `len_lte`.
  - Field-vs-field: `eq_field`, `neq_field`, `lt_field`, etc.
  - Values: single-quoted strings, numbers, booleans, bare words.

- **NQL command parser** — dispatches `find`, `count`, `aggregate` with flags:
  - `find`: `--limit N`, `--offset N`, `--fields f1,f2`, `--order-by field[:dir]`, `--format json|rows|csv|dict`, `--auth token`
  - `aggregate`: `--group-by f1,f2`, `--having filter`, `--order-by alias[:dir]`, `--limit N`
  - Auth token via `--auth` for raw-TCP access from non-trusted IPs; CLI uses trusted-localhost.

- **CLI `find` command** — new `./shard-db find <dir> <obj> [filter] [flags]` shortcut. Previously `find` was JSON-only via `./shard-db query '{...}'`.
  - `./shard-db count` and `./shard-db aggregate` retain their existing positional JSON API and gain NQL auto-detection: if the criteria argument contains `(` (aggregate spec) or lacks `[`/`{`, it routes through NQL.

- **NQL reference docs** — full grammar, operator table, normalization rules, TCP usage, and limitations at [`docs/query-protocol/nql.md`](../query-protocol/nql.md).

### Examples

```bash
./shard-db find default users 'age > 25 and status = active' --limit 20
./shard-db find default users 'status in (active,pending)' --fields name,email --format csv
./shard-db count default orders 'total between 100 and 999 and paid = true'
./shard-db aggregate default orders 'status = active' sum(amount),count() \
  --group-by region --having 'count > 10' --order-by sum_amount:desc

# TCP wire (server auto-detects)
echo 'find default users "age > 25" --limit 5' | nc localhost 9199
```

### Internal

- **Compact VARCHAR internals**: `typed_encode_trim_len(ts, buf, full_len)` walks the field table in reverse to find the last non-zero boundary; `schema_trim_fn` is the `SlotcaskTrimFn` adapter. `slotcask_compact(db, trim_fn, ctx)` replays every live record through the trim callback and rewrites segments atomically. `slotcask_compact_kf` rebuilds keyfile pointers after segments are rewritten.
- New `src/db/nql.c` + `src/db/nql.h` — lexer, filter parser, aggregate spec parser, command tokenizer. No engine changes; NQL builds the same `CriteriaNode *` the JSON criteria path does.
- `cmd_count_tree`, `cmd_find_tree`, `cmd_aggregate_tree` — new public wrappers that accept a pre-built `CriteriaNode *` directly, bypassing JSON criteria parsing. `cmd_count_do`, `cmd_find_do`, `cmd_aggregate_do` extracted as static helpers (callers own tree/spec memory; helpers borrow and do not free).
- 9 new unit tests in `src/test/cases/test_nql.c` (nql-simple-filter, nql-find-command, nql-count-command, nql-aggregate-command, nql-and-or, nql-between, nql-in, nql-parse-errors, nql-flags).

### kf corruption recovery

- **compact-kf guard** — `vacuum`/compact's segment-merge path no longer deletes a donor segment file when a record's kf update couldn't be confirmed during migration; previously this left dangling kf pointers to files that no longer existed.
- **`rebuild-kf` command** — repairs already-corrupted kf entries by rescanning all segment files for an object and rebuilding kf slots from what's actually on disk. Idempotent. `./migrate` runs it automatically (phase 2/3, before compact); embedded (npm) clients auto-run it once per `db_root` on first use, also exposed as `shardDb.rebuildKf(dir, object)`. See [schema-mutations.md#rebuild-kf](../query-protocol/schema-mutations.md#rebuild-kf).
- **`rebuild_object_v2` (`vacuum --splits`) recovery** — restores `data/` from `.rebuild_legacy_root` on walk failure instead of stranding the object half-migrated; a single corrupt record during the walk is skipped (logged) instead of aborting the whole rebuild.

### add-index / remove-index locking fix

- **Exclusive locking** — `add-index`/`remove-index` now take `objlock_wrlock` instead of the shared `objlock_rdlock`, closing a race where a concurrent insert's on-the-fly index update could land on the same index file mid-rebuild and either get silently discarded or corrupt the B-tree pages being written. Same lock class `vacuum`/`truncate` already use for the identical unlink-and-rebuild pattern. See [Concepts → Concurrency](../concepts/concurrency.md#per-object-rwlock-objlock).

### Fixes

- **`reindex_seg_cb` segfault on compacted objects** — fixed an out-of-bounds read when `add-index`/`reindex` ran against records shorter than `ts->total_size` (i.e. anything written after the compact trim above).
- **`compact` dispatch double-dir path bug**.
- **`recover_one_stream` realloc leak on OOM** during crash-recovery directory scanning.
- **`export-schema`** — removed the unused, unvalidated `version` field from its output.
- **npm packaging** — `nql.c` added to `binding.gyp` (was missing, breaking NQL in embedded builds); npm package version corrected `1.1.2` → `1.0.7`.

### Upgrade

```bash
./shard-db stop
./migrate          # phase 1: convert fixed→variable; phase 2: rebuild-kf + compact
./shard-db start
```

Migration required to reclaim disk space and repair any kf corruption from earlier versions. See [Upgrade notes](../release-notes/2026.07.1.md#upgrade-notes) for the force-rebuild-suspect-indexes step.

## 2026.06.3

Explain query mode, lock-free warm reads, and a reindex spill-file collision fix. No `./migrate` required; wire-compatible with 2026.06.2.

Full notes: [`docs/release-notes/2026.06.3.md`](../release-notes/2026.06.3.md).

### Features

- **Explain mode** — `"explain":true` on any `find`, `count`, or `aggregate` request returns the query plan without executing it. Response includes plan type (`leaf`/`scan`/`intersect`/`union`), source leaves with index type and cardinality estimates, postfilter leaves, order strategy (`index_walk`/`composite`/`sort`/…), and optimization hints (`add_index`, `composite_index`, `add_trigram_index`, `add_bitmap_index`). CLI: `./shard-db explain <dir> <obj> '<criteria>'`. TUI: `e` key in query preview opens formatted explain output.

### Performance

- **Lock-free warm reads** — generation counters on the global cache table let warm reads (cache hits) skip the table lock entirely. Only cold misses and writers take the lock. Reduces contention under high-concurrency, warm-cache workloads.

### Fixes

- **Reindex spill collision** — btree and trigram builders on the same field previously shared one spill directory, causing mutual overwrites during multi-index reindex. Each builder now uses a private `<obj>/tmp/spill/<field>/<type>/` directory. Recommended upgrade if your objects have trigram indexes.

## 2026.06.2

Embedded mode, native Node.js/Bun npm package, and async Promise-based query API. No `./migrate` required; wire-compatible with 2026.06.1.

Full notes: [`docs/release-notes/2026.06.2.md`](../release-notes/2026.06.2.md).

### Features

- **Embedded mode** — link shard-db as a library (`libshard-db.a`) into any C process. `ShardDb *db = shard_db_open(path); shard_db_query(db, json); shard_db_close(db)`. No daemon, TCP, or OpenSSL. Thread-safe; full engine in-process.
- **npm package** — `npm install shard-db`. Native N-API addon for Node.js ≥ 18 and Bun ≥ 1. Prebuilts for linux-x64, linux-arm64, darwin-arm64. `db.query()` returns a `Promise<string>`; `QueryBody` discriminated union covers all modes with TypeScript autocomplete.
- **Log handler callback** — register a C function to receive structured log events from the embedded engine (errors, warnings, slow queries, audit events).

### Fixes

- **C1 cursor tiebreaker** — `small_prefilter_cmp_asc` includes `hash16` as a secondary key; prevents duplicate/skipped results when multiple records share the same order-field value across cursor pages.
- **JSON boolean flags** — `true` (JSON boolean) is now accepted wherever a boolean flag is expected; previously only `1` was accepted.
- **`total` for PRIMARY_LEAF + post-filter** — queries with one indexed leaf and a post-filter sibling now correctly report `total` via the hint KeySet path.
- **JSON-escape varchar field values** — varchar content is now JSON-escaped in all read responses (find/fetch/aggregate/file). Unescaped control characters or quotes in stored strings no longer break response parsing.
- **`g_io_threads` NULL-deref guard + trigram leaf compilation for `find:total`** — fixes a startup crash when the IO thread pool is not initialized, and correctly compiles trigram criteria for `find` queries with `"total":true`.

## 2026.06.1

Query planner overhaul, O_DIRECT I/O series, batch-fetch consolidation, and cursor+total pagination. No `./migrate` required; wire-compatible with 2026.05.8.

### Performance

- **O_DIRECT series** — cache-bypassing pread in full-scan queries (find, count, aggregate), parallel aggregation fan-out, reindex worker streams, startup recovery (`recover_streams`), compact donor scan, and KF shard walks. Single-shot 32 MB fast path (`DB_ODIRECT_BUF_MB`) with adaptive mincore fallback (opens buffered when ≥80% pages resident).
- **Batch-fetch consolidation** — two-phase bulk fetch, IO thread pool migration (`IO_THREADS`, default `4×nproc`), cursor pagination C1 path batch, streaming wfc_worker MIN/MAX batch-fetch. Separates I/O threads from CPU pool to avoid starvation on slow-disk workloads.
- **Bitmap parallel popcount** — `GROUP BY <bitmap_field>` with aggregates (avg/sum/min/max) uses parallel bitmap popcount; slot-level bitmap intersect for criteria.
- **k-way min-heap merge** — cursor pagination (`fetch+sort DESC`) now uses a k-way min-heap across per-shard btree iterators instead of linear scan, enabling stable O(log N)-per-step pagination.

### Features

- **Cursor+total in single request** — `"total":true` alongside `"cursor":{}` in find request returns both the page results and the full match count in one round-trip. Avoids the two-query pattern (cursor + separate count).
- **Composite index improvements** — typed binary encoding, reindex `--composites-only` flag, eq+ORDER BY routing (D1/D2/D3 decision paths).
- **Bitmap IGB+hbm** — `GROUP BY <bitmap_field>, avg/sum/min/max(...)` materializes only the selected aggregates, skipping irrelevant fields via itemized-group-by (IGB) + histogram metadata (hbm).
- **Reindex flags** — new `--composites-only` for rebuilding only composite indexes without full-shard rescan.

### Query planner

Complete rewrite of the criteria-tree planner with decision-table executors, cardinality estimator, and composite-index routing:

- **Decision-table executors (D1/D2/D3)** — indexed composite (field1+field2) queries now route to decision paths optimized for the operator pattern (eq+eq, eq+range, etc.).
- **Broad-filter ordered finds** — `find` with `order_by` on non-indexed field now walks the order-by index to filter early, materializing only matched records rather than buffering all matches before sorting.
- **Range-folding for pagination** — offset + limit translate to btree range bounds when the order-by field is indexed, avoiding the full-buffer sort.
- **Materialization guards** — cancel eager record materialization when the KeySet is much larger than remaining page depth, falling back to lightweight scans.
- **Selectivity guards** — skip expensive multi-index intersection or materialization when a single leaf's cardinality dominates, picking the fastest path.

### Fixes

- **Writer-preferring rwlock** — replaces reader-writer-fairness model to prevent writer starvation on high-read workloads. Writers now take priority once they arrive, so index builds and schema mutations proceed without unbounded delay.
- **Condvar replacing sched_yield()** — batch_buf_collect_hash no longer spins; coordination waits on a condition variable with bounded wake-up latency.
- **Cross-tenant index cache key scoping** — index cache keys now include tenant+object scope to prevent cross-tenant entry collision.
- **Binary-key btree correctness** — btree callbacks now receive correctly-encoded binary keys for composite-index pagination.
- **Help-drain deadlock prevention** — parallel_for help-drain path no longer deadlocks when nested work arrives while finalizing.
- **Bulk_delete null memcpy guard** — bulk_delete no longer memcpy's from NULL when a record has no indexed fields.

### Configuration

New environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `IO_THREADS` | `4×nproc` | Separate I/O thread pool size (distinct from CPU-bound `THREADS` pool). |
| `DB_ODIRECT_BUF_MB` | 32 | O_DIRECT buffer size per worker in MB. Peak RAM ~ `2×DB_ODIRECT_BUF_MB×IO_THREADS`. |
| `WARMUP` | `async` | Startup cache warmth: `async` (detached), `sync` (block), `off` (skip). |
| `AUTO_VACUUM` | 0 | Enable background vacuum thread (0/1). |
| `AUTO_VACUUM_INTERVAL_SEC` | 3600 | Auto-vacuum poll interval in seconds (floor 60). |
| `VACUUM_RECOMMEND_TOMBSTONE_PCT` | 10 | Tombstone ratio threshold for vacuum recommendation. |
| `VACUUM_RECOMMEND_MIN_DELETED` | 1000 | Minimum deleted count before vacuum is recommended. |
| `RANDOM_SEQ_COST_RATIO` | 8 | Planner cost ratio for random vs sequential I/O. Higher prefers full-scan. |

All new variables are optional; defaults apply when unset. Existing configurations continue to work unchanged.

## 2026.05.8

Correctness + perf release. Headline: **4800× speedup** on selective-filter + order_by queries when the matching record's order-key lands late in the sort walk. **Logging framework reshape** (typed `LOG_*` macros, new `<date>-audit.log`, structured `subsystem` field — operators with log-parsing regexes need to update). **Unknown-field queries error loudly** instead of silently returning empty results. No `./migrate` required, wire-compatible with 2026.05.7.x.

Full notes: [`docs/release-notes/2026.05.8.md`](../release-notes/2026.05.8.md). Highlights:

- **`find criteria + order_by`** with selective prefilter no longer walks the full order_by btree. New small-prefilter shortcut (K ≤ 1000 → fetch + in-memory sort) plus unified `pick_index_for_leaf` at plan time. Worst case `eq username order_by age desc limit 10`: **1737 ms → 0.36 ms**.
- **Unknown-field validation** at the entry of `cmd_count`/`cmd_find`/`cmd_aggregate`. Composite-field sub-names validated too. Returns `{"error":"unknown field 'X' in criteria"}` in microseconds instead of `[]` after a 38-second scan.
- **`cmd_drop_object`** 17-33 ms during warmup (was 3-20 s) — fixed `g_reg_lock`-held-across-`slotcask_open` contention.
- **`cmd_bulk_insert` SIGBUS on page-aligned payloads** fixed; server path no longer routes through memfd+mmap.
- **Warmup drives slotcask registry + kfcache via cache APIs** so first user query is genuinely warm.
- **Bitmap cardinality probe** before materializing ordered-find prefilter — skips ~37 s of doomed materialization on broad bitmap criteria.
- **`pick_index_for_leaf` unifies index dispatch** at plan time, no runtime cross-index cascade. Closes silent-empty bugs on sub-3-char trigram and bitmap-with-tiered-KeySet.
- **Logging framework**: typed `LOG_INFO/WARN/ERROR/DEBUG/AUDIT`, new `<date>-audit.log`, structured subsystem field. Log line shape changed — see operator notes.
- **`KF_RESPLIT`** demoted from raw stderr to `LOG_INFO(LOG_SUB_SLOTCASK,...)`.
- **`docs/operations/bulk-loading.md`** new cookbook with the R-crossover rule (R = N/200K, load-then-index for R ≥ 20). New `SHARD_BENCH_DROP_INDEXES_FIRST=1` knob in `bench-queries` to A/B the seed pattern.
- **Bulk-insert/delete index-drift safety net**: `LOG_ERROR("index.conf drift...")` if it ever fires. Audit confirmed unreachable today; future-proofed.
- **`bench-cache-pollution`** reproducer added. 25M scale: noise band (working set fits in RAM). Re-trigger at full-HN scale.

### Operator migration notes

- Log-parsing scripts that match `\[ERROR\]` etc. need to switch to `^[\d-]+ [\d:]+ ERROR \[`. New shape: `2026-05-25 14:33:21 ERROR [subsystem] body`.
- New `<date>-audit.log` file in `LOG_DIR`. Add to log shipping if you forward shard-db logs externally. Bypasses `LOG_LEVEL`.
- If seeding > 4 M records on a fresh object: drop indexes → bulk-insert → multi-field `add-index` (plural) is dramatically faster than inserting into pre-existing indexes. See [`docs/operations/bulk-loading.md`](../operations/bulk-loading.md). Don't loop singular `add-index` per field — that's N full scans.

## 2026.05.7.1

Patch release on the 2026.05.7 line. Headline is a **critical correctness fix in `cmd_find`** that returned `[]` non-deterministically on indexed queries served by a busy daemon. Bundled with **edit-field polish**, **add-field computed-defaults backfill**, and **startup warmup**.

Wire format and on-disk format unchanged. No `./migrate` required.

### Bug fixes

- **`cmd_find` stale `g_out` on reused pool workers (#64).** `stream_find_cb` and `cursor_find_cb` wrote to the thread-local `g_out` left in the worker by a previous request, so OUT() reached a closed fd and the current client got `[]` even though records matched. Non-deterministic — fresh workers worked; reused workers silently dropped output. Forced `g_out = sc->parent_out` unconditionally at every callback entry. `CursorFindCtx` gains a `parent_out` field. Surfaced via the HN explorer at 789K rows.
- **`FT_TIMESTAMP` missing from `compile_one` switch (#64).** Every typed kind had a case except FT_TIMESTAMP (added in 2026.05.6). `cc->i1` stayed at the memset-zero default → `time eq X` matched 0 rows, `time gte X` matched ALL rows (filter silently ignored), AND-intersect with a timestamp sibling collapsed to 0. `count` was unaffected because it goes through `encode_criterion_value` (config.c) which DID handle FT_TIMESTAMP. Anything that runs `criteria_match_tree` (find, aggregate post-filter, AND-intersect) was wrong. Fixed by adding `case FT_TIMESTAMP:` next to `case FT_LONG:` (same int64 BE encoding) in both the scalar and IN-list switches.
- **`add-field` slot-write latent bug (#67).** When the new field happened to fit within 8-byte slot-size padding, `slot_changed` was 0 and the rebuild walk took the verbatim re-insert path that wrote only the OLD value length — the new field bytes were silently never written for existing records. Now `slot_changed = (size changed) || (n_added > 0)`. Existing `add-field` benches in the slot-padding regime measured "add-field that silently skipped writing the new field"; re-bench.
- **macOS build fix for warmup (#69).** `posix_fadvise` is Linux-only; gated behind `#ifdef __linux__`. The explicit 4 KB `read(2)` after the fadvise is what actually pages the file in, so the macOS path keeps the synchronous priming intact and just skips the async read-ahead hint.

### Features

- **`edit-field` polish (#65).** Default-modifier carry-through: omitting `:default=…` / `:auto_create` / `:auto_update` preserves the modifier from the old `fields.conf` line. Smart reindex: only rebuilds indexes whose referenced fields actually changed encoding (response carries `indexes_rebuilt` + `indexes_skipped`). `dry_run` flag: validation + pre-flight scan with no writes, returns `{"status":"ok","dry_run":true,"would_rebuild":bool}`.
- **`add-field` computed-defaults backfill (#67).** Parser now accepts `:default=<literal>` / `:default=seq(<name>)` / `:default=uuid()` / `:default=random(N)` on both `add-field` and `edit-field` (was rejecting most spec forms outright). Backfill during rebuild: literal stamped on every existing record; seq pre-reserves a range and assigns sequentially; uuid generates UUIDv4 per record; random reads `getrandom` bytes per record. `:auto_create` / `:auto_update` are inert during backfill (they only fire at insert/update). Also closes the documented "no way to *change* a default via edit-field" gap from #65 — same parser fix applies to both code paths.
- **Startup warmup (#68).** New env knob `WARMUP=async|sync|off` (default `async`). On startup, a detached thread walks every `(dir, object)` and primes the OS page cache for every `.kf` + `.idx` + `.bm` + `.tg` file. Stream segment files (`.dat`) are deliberately NOT warmed — they can be GBs each and warming them all would evict the smaller, hotter index pages. Restart-while-explorer-running goes from ~15 s cold-render to instant.

### Tests

- `test_find_timestamp_criteria` (new, 31 assertions) — covers FT_TIMESTAMP scalar + IN + AND-intersect + count parity.
- `test_edit_field_polish` (new, 26 assertions) — default carryover, auto_create / auto_update carryover, smart reindex, dry_run.
- `test_add_field_computed_defaults` (new, 34 assertions) — DK_LITERAL / DK_SEQ / DK_UUID / DK_RANDOM backfill + edit-field default change + random-too-big rejection.
- `test_auto_key_multi` (new, 29 assertions) — regression coverage for `parse_multi_key` on `auto_key=uuid|seq` objects (#66).

Full suite: **3603 / 3603 across 89 cases.**

## 2026.05.7

Feature release bundling **three new index types**, a **bounded query concurrency cap**, a **filter-first planner for `find + order_by`** (194× wins on zero-match selective queries surfaced by real-app testing), and a **CPD-driven dedup sweep**.

### Index types

- **Bitmap index** (PR #56). Default for `bool` and `enum` fields; opt-in via `field:bitmap` / `field:bitmap(N)` for low-cardinality varchar. Dense one-bit-per-slot layout at `<obj>/indexes/<field>/<NNN>.bm`, 1:1 with data shards. Popcount fast paths for `eq` / `in` / `neq` / `not_in`; dict-scan for every other op. Default cap 256 distinct values per (shard, field), override up to 65535.
- **Enum field type** (PR #57). New typed field `field:enum(a,b,c,...)`. Width-adaptive storage (1 byte for ≤256 values, 2 bytes for >256). Auto-promote to bitmap index. Strict create-object default — unknown values rejected at insert (PR #58).
- **Trigram index** (PR #60). New opt-in `field:trigram` on varchar for substring search (`contains` / `i_contains`). Extracts distinct 3-byte lowercased trigrams per record into `<obj>/indexes/<field>/<NNN>.tg` (BTRH btree format — shares cache + tooling with `.idx`). Queries intersect per-trigram posting lists rarest-first, then per-record verify the substring. **Planner heuristic**: when a field has both btree AND trigram, short patterns (< 6 chars) use btree-leaf scan (faster on common substrings), long patterns use trigram (faster on selective queries). Field-only trigram (no btree) uses trigram regardless of pattern length.
- **Bounded-memory index build pipeline.** External-merge sort with per-worker spill files + k-way merge feeds streaming `bt_stream_build_*` into the final btree. Per-output-shard memory stays at a few MB regardless of input size — verified at 25M records (was OOMing pre-fix). Same pipeline powers btree and trigram builds; bitmap builds write directly into mmap'd `.bm` files (no accumulation needed).

### Bounded query concurrency

- **New `MAX_CONCURRENT_QUERIES` knob** (PR #61). Hard cap on simultaneously-running queries via sem_trywait semaphore at dispatch entry; cleanup-attribute release on any exit path. Default `0` = auto = `max(4, min(nproc, 32))`. Exceeded → immediate `{"error":"server at capacity","max_concurrent_queries":N}` so clients retry without holding the TCP thread.
- **Worst-case query-buffer RAM is now predictable**: `MAX_CONCURRENT_QUERIES × QUERY_BUFFER_MB`. Pair with cgroup / systemd `MemoryMax` as the final OS-level guard.
- **`QUERY_BUFFER_MB` default lowered 500 → 256.** Auto-tune still kicks in but now divides the process-wide query budget (min 25% RAM, 4 GB ceiling) by the resolved slot count instead of letting a single query grab all of it.

### Filter-first planner for `find + order_by` (PR #63)

- New `build_keyset_from_plan` dispatcher wraps all existing KeySet builders (trigram, bitmap, btree-leaf, AND-intersect, OR-union) behind one entry point. `cursor_find_cb` gains a `prefilter_ks` field — skips fetch + criteria_match for entries not in the KeySet.
- Applies to both the cursor pagination path and the non-cursor ordered-walk fast path. Threshold `ORDERED_FIND_KEYSET_MAX = 100K`; broader filters fall back to legacy walk-ordered.
- Closes the regression where `find icontains 'kubernetes' order_by time desc limit 25` over 789K comments took 14 seconds (planner walked the entire `time` btree to confirm 0 matches). Now **73 ms warm** — **194× faster**. Discovered via the HN explorer showcase exercising real query shapes that the bench-queries suite didn't compose.
- Empty-KeySet short-circuit emit-fix: previously emitted `[]` after the caller already emitted `[`, producing malformed `[[]`. Fixed to emit envelope-close only.

### Code quality

- **CPD-driven dedup sweep** (PR #59, PR #62). Consolidated benchmark utilities (`bench_du_bytes` / `bench_fmt_bytes`), `storage.c` bucket-dispatch (3 callers → 1 helper), `query.c` aggregate min/max via KeySet (2 callers → 1 helper). 130 LOC removed, no behavior change.
- **CodeQL #94 snprintf safety fix** (PR #61). Test bulk-insert builder no longer uses the `off += snprintf(buf+off, cap-off, ...)` antipattern that can drive `off` past `cap` on truncation.

### Migration

Wire-format and on-disk format unchanged from 2026.05.5 / 2026.05.6. **No `./migrate` required.** Existing objects continue to work; opt into the new index types via `add-index` on the fields where it makes sense.

Test coverage: **85 cases / 3483 assertions, 0 failures.**

Full notes: [release-notes/2026.05.7.md](../release-notes/2026.05.7.md).

## 2026.05.6

Hotfix release for two latent JSON-escape bugs in the typed-record
path plus a new `timestamp` field type. HN comment text caught
both escape bugs on the first real load while building the public
showcase; the `timestamp` type was a community draft from
2026-04-30 that paired cleanly with the same release.

- **Decode side (escape).** `decode_field_to_buf` and
  `buf_field_value` no longer emit raw varchar bytes inside JSON
  quotes — both route through a new `json_escape_into()` helper
  that does RFC 8259-compliant escaping. Pre-fix, stored
  newlines / quotes / backslashes corrupted the response stream
  mid-object.
- **Encode side (unescape).** `typed_encode` and
  `typed_encode_defaults` now route FT_VARCHAR string values
  through the pre-existing `json_unescape_string()` helper, so
  wire-form escapes (`\"`, `\n`, `\\`, `\uXXXX`…) become their
  intended byte sequences in storage. Pre-fix, `"a\"b"` on the
  wire was stored as the four literal bytes `a\"b`. CSV path is
  untouched (raw bytes are correct for CSV).
- **Outer-buffer sizing.** `typed_decode` and
  `typed_decode_stream` switched both the per-field decode buffer
  AND the outer record-JSON buffer from flat heuristics to
  per-field-type sizing. Pre-fix the outer buffer (`nfields *
  300`) silently truncated mid-value on records with multi-KB
  varchar content (e.g. HN comments); `SB_APPEND` fails closed
  rather than loud, so corruption was invisible from the wire.
- **New: `timestamp` field type.** 8 bytes, signed int64 BE,
  semantic Unix epoch milliseconds. Storage / comparison / index
  key identical to `FT_LONG`; the type adds `:auto_create` and
  `:auto_update` generators that emit
  `clock_gettime(CLOCK_REALTIME)` in ms. Distinct from
  `datetime` (calendar-packed, can't represent pre-1970 / post-9999)
  and `long` (no time-source defaults).
- Regression tests: `test-json-escape` (19 assertions) and
  `test-timestamp` (14 assertions).

Wire format unchanged. On-disk schema unchanged. No `./migrate`.
Existing varchar records that contain JSON metacharacters remain
on disk in their pre-fix shape; re-ingest if you need clean data.

Test coverage: **81 cases / 3091 assertions, 0 failures.**

Full notes: [release-notes/2026.05.6.md](../release-notes/2026.05.6.md).

## 2026.05.5

Breaking-cleanup release. Legacy v1 (probe-into-slot) storage engine
removed entirely; slotcask is the only supported layout. B+ tree
on-disk sort order also changes to `(value, hash)` (magic `'BTRH'`),
closing a silent-no-op bug in `btree_delete` on duplicate-value
clusters and restoring O(log N) deletes. `./migrate` rebuilds btrees
on first start after upgrade — idempotent, reindex-only, no data-shard
work.

- **v1 storage engine removed.** Every Zone A / Zone B code path,
  the `SHARD_ALLOW_V1_CREATE` test opt-in, the v1 text counts file,
  and the `addr_from_hash` / `compute_addr` helpers are gone. `Schema`
  + `SlotcaskSchemaInfo` lose their `storage_version` fields. The
  `storage_version` slot in `schema.conf` is preserved on disk for
  forward compatibility, and the daemon refuses any value other than
  `2` at load with an error pointing operators at the 2026.05.4
  migrate path.
- **B+ tree (value, hash) sort.** Indexed btrees now order entries by
  `(value, hash)` lexicographically. Internal pages carry the
  separator's hash alongside its value so descent routes directly to
  the unique leaf holding the target tuple. Replaces the pre-2026.05.5
  fallback that walked the entire leaf chain whenever the cluster
  spanned multiple leaves — and silently no-op'd when the entry sat
  on a leaf the walk had already crossed. Magic rolls `'BTRG'` →
  `'BTRH'`; existing btrees must be rebuilt via `./migrate`.
- **`./migrate` reinstated** as the BTRH reindex orchestrator. Reads
  `db.env`, starts the daemon, runs `./shard-db reindex`, stops the
  daemon. Idempotent — running on an already-BTRH install just
  rewrites btrees in their current format. Operators upgrading from
  a pre-2026.05.5 install with v1 objects must still install 2026.05.4
  first and run that release's `./migrate` to convert v1 → slotcask,
  then upgrade.
- **edit-field shipped** — same-type schema mutations
  (`varchar:N` width change, `numeric:P,S` precision/scale within
  ranges, `default=` updates). Cross-type transforms remain refused.
  See [Schema mutations](../query-protocol/schema-mutations.md).
- **auto-key shipped** — `"auto_key":"uuid"` or
  `"auto_key":"seq(<name>)"` at `create-object` makes the server
  generate keys on inserts that omit the `key` field. Provided keys
  go through upsert as before; CAS modifiers respected.
- **Slotcask CRUD write paths optimised.** Four wins land together
  in PR #46 — measured on 10M-record real-disk workloads:
  - Single `slotcask_update` / `slotcask_delete` collapse their
    redundant double kf-wrlock cycle (test-only entry points; the
    production paths already had the single-cycle pattern).
  - Bulk-upsert Phase 5 tombstones now sort by `(sid, fid)` and walk
    in runs under one segcache rdlock per file, mirroring
    bulk-delete's Phase 3 batching.
  - Every CRUD pre_commit (`v2_update`, `v2_delete`,
    `v2_bulk_upd_pre_commit_bulk`) dispatches per-indexed-field
    delete+write via `parallel_for + update_idx_fn` (promoted from
    storage.c-static to a shared index.c helper).
  - Index-key extraction in pre_commits uses a no-alloc arena
    (`build_index_key_from_record_into`) instead of `2 × nidx`
    mallocs per record. Jumbo varchar indexes (>4096 B) fall back
    to malloc.

  Bench deltas vs main HEAD on real disk: BULK INSERT JSON +33%,
  CSV +45%; single UPDATE/DELETE p50 ~2.6× faster; bulk
  UPDATE/DELETE 3–20× faster; parallel UPDATE 2.7× faster. Tombstone
  contract unchanged; no semantics change.
- Test coverage: 79 cases / 3058 assertions including a new
  `test-btree-value-hash-sort` covering insert/delete/bulk-merge/split
  invariants for the new sort order. ~12 000 lines of v1 dispatch +
  fallback code retired.

Full notes: [release-notes/2026.05.5.md](../release-notes/2026.05.5.md).

## 2026.05.4

Query performance, concurrency-safety, **and macOS support**. No protocol changes, no schema changes, no migration step — drop in the new binaries and restart.

### macOS (Apple Silicon) now supported

Linux-isms swapped for portable equivalents:

- `epoll_create1`/`epoll_wait` → `poll()` in the server accept loop (`src/db/server.c`). Single listen fd; no `epoll` selectivity to lose.
- `memfd_create` + `/proc/self/fd/N` in `cmd_bulk_insert_string` guarded by `#ifdef __linux__`; macOS uses the existing `/tmp` fallback (same code path that already covered `memfd_create` failure on Linux).
- `<linux/limits.h>` → `<limits.h>` + `<sys/param.h>` for `PATH_MAX` in 3 files.
- `-lncursesw` → `-lncurses` on Darwin in `build.sh` (macOS's bundled ncurses is built with wide-char baked in, no `-w` suffix).
- CI: new `macos-latest` runner added to `.github/workflows/ci.yml` matrix; full 77-case suite runs on Apple Silicon too. Artifact `shard-db-macos-arm64.tar.gz` joins the existing `linux-x86_64` + `linux-arm64` tarballs.

Pre-existing Darwin work (`#ifdef __APPLE__` for `funopen` vs `fopencookie` in TLS, `mremap` fall-back to munmap+mmap in btree, `sync_file_range` already gated to Linux in storage) all stayed as-is and works.

### Query performance — five fast-path landings

### Query performance — five fast-path landings

Headline cold-bench movements at 25M users (post `sync && drop_caches`):

| Query shape | Before | After | Speedup |
|---|---|---|---|
| `sum X` single-spec, indexed numeric (int/long/short/numeric/date) | ~2 s each | ~200 ms each | **~10×** |
| `group by username, count limit 10` (high-card varchar idx) | 5.6 s | **3.6 ms** | **~1570×** |
| `group by email, sum(balance) limit 10` (varchar idx + indexed numeric agg) | 7.5 s | **4.1 ms** | **~1800×** |
| First cold full-scan `count starts bio 'Software'` (non-idx varchar) | 1.3 s | ~800 ms | ~1.6× |
| `agg WHERE active=false (count+avg)` | 2.7 s | 1.1 s | ~2.5× |

Implementation: five PRs (#33-#36 plus the perf branch in this release):

- **Leaf-only walker for single-spec SUM/AVG.** New `btree_walk_all_values` in btree.c — a tight forward leaf-chain walk that bypasses `BtRangeIter`'s per-entry overhead (no hash memcpy, no bound check, no yield-buffer copy). Per-entry CPU drops from ~145 ns to ~50 ns. Routed from `agg_single_shard_worker` for SUM/AVG; MIN/MAX keep the iter path (they short-circuit on the first leaf entry anyway).

- **`MADV_SEQUENTIAL` on btree leaf walks.** The per-btree mmap is set to `MADV_RANDOM` at acquire time (correct for point lookups; suppresses readahead). Sum/avg full walks under that hint page-fault 4 KB at a time. `MADV_SEQUENTIAL` for the walk duration coalesces into 128 KB+ readahead I/Os; restored to `MADV_RANDOM` at all exit paths so concurrent point lookups keep their no-wasted-readahead behaviour. `POSIX_FADV_WILLNEED` was tried first and didn't move cold sums — async prefetch races the walk's faults.

- **`MADV_SEQUENTIAL` on slotcask kf during full-shard walks.** Same pattern applied to the kf walk that drives every PRIMARY_NONE full-scan query (`find/count/aggregate` with non-indexed criteria, regex bio, field-vs-field). The matching extension to seg files was tried and reverted — the shared-segment files plus `MADV_SEQUENTIAL`'s "free after use" semantic caused cross-query cache eviction that regressed unrelated queries.

- **Streaming k-way merge for varchar `group_by` + count + limit.** The IGB fast path builds a full N-bucket hash table even when `limit=10`. New path walks each idx_shard's btree leaves via `BtRangeIter`, runs a k-way merge to dedup the same varchar across shards (idx_shards are hash16-routed, so the same value can land in multiple shards), emits `(key, count)` directly into `ctx.ht`, stops at `limit`. Gates strictly on single varchar group_by + COUNT-only + finite limit + no criteria / having / order_by.

- **Streaming-distinct extended to SUM/AVG/MIN/MAX.** Same k-way merge, plus a per-emit `VSStaged` slot that aggregates SUM/AVG/MIN/MAX values via `slotcask_lookup_by_hash` of each contributing record's hash16. Gates on indexed non-varchar agg field; per-run cap (16384 records) aborts cleanly to IGB on low-cardinality data so ctx.ht stays untouched.

### Concurrency audit — TSan + ASan clean (full suite)

Triggered by a TSan flake on PR #35's CI. We did a systematic pass under both sanitizers across all 76 test cases.

**Real bugs fixed:**

- **`parallel_for` help-drain race.** Caller's nested-path acquire-read of `remaining` could see 0 and proceed to `pthread_mutex_destroy` while the last worker was still between `fetch_sub` and the post-broadcast unlock. Real crash potential under nested parallel calls. Added `_Atomic int finishing` to `PoolGroup` — workers bump before fetch_sub, decrement after the broadcast; caller waits for both `remaining==0` AND `finishing==0` before destroying. `sched_yield` spin; window is microseconds.

- **`objlock.c` fast-path probe race.** The lockless probe of `(used, name)` raced with the slow-path install. In theory this could allow two threads to install entries for the same object in different slots — and then a concurrent vacuum/rebuild + insert might end up using different rwlocks for the same object → data corruption. Made `used` `_Atomic` with release-store after `strncpy`; acquire-load in fast path guarantees `strcmp` sees a coherent name snapshot.

- **Memory leak in parallel aggregate paths.** `parallel_agg_scan_shards_v2`, `parallel_indexed_agg`, and `parallel_agg_scan_shards` called `agg_ctx_merge` per worker but never freed the per-worker arenas. ASan flagged 12 MB+ leak per `group by X count+avg(Y)` query at 25M. Added `agg_ctx_free_local` after each merge.

**Stop flags and stats counters made atomic** (benign data races, now silent under TSan): `g_log_running`, `server_running`, `active_threads`, `in_flight_writes`, `g_scan_stop`, `QueryDeadline.timed_out`, `CountCtx.dl_counter`, `JoinBtHit.found`, `parallel_for`'s `SUBMIT_CHUNK` init-once. Two test-only races also fixed.

**`localtime` → `localtime_r`** in config.c and query.c — libc's non-reentrant `localtime` returned a shared static buffer, racing across concurrent log calls. Logged-timestamp-only impact, but cleared the warning.

**Suppressions for known-correct-under-C11-but-TSan-blind patterns.** Two `.tsan.supp` entries: the `bt_acquire / segcache_acquire / kfcache_acquire` verify-retry lock-order false positive (release happens between acquires; TSan tracks cycles without modeling unlocks), and the `seg_record_emit / seg_rec_*` byte-level races where the byte-18 flag is the release-store/acquire-load synchronisation point for the full record (C11 guarantees coherency after observing `flag==1`; TSan tracks each byte independently). Both documented inline in `.tsan.supp`. The `slotcask_registry_invalidate` use-after-free is suppressed and tracked as a backlog item (needs SlotcaskDb refcounting; in practice the invalidate only fires from drop-object during quiet periods).

**ASan suppression** (`.lsan.supp`): match_criterion's thread-local `regex_t` cache leaks ~8 KB total at process exit (one per pool worker). Future fix is `pthread_key_create` destructor.

### Multi-field int group_by — silent bucket collisions fixed

PR #32 shipped an integer-hash fast path for `group_by` on int/long/short/numeric/date fields. A multi-field full-scan path had three compounding bugs:

- `typed_field_to_raw` emitted 0 bytes when `v==0` for INT/LONG/SHORT/NUMERIC/DATE → zero values silently collapsed in the raw key, so e.g. `(a=0,b=5)` and `(a=5,b=0)` hashed identically.
- 16-byte inline raw_key cap silently dropped trailing fields when ngroups summed to >16 B → `(1,1,1)` and `(1,1,2)` could collide on 3-long groupings.
- `agg_ctx_clone_shared` didn't propagate `use_int_keys` to parallel workers, so the integer fast path was effectively unused under parallel scan.
- `agg_scan_cb`'s stack `gbuf` wasn't NUL-initialised, so prior-record bytes leaked into the next record's group_vals on the string fallback path when `typed_field_to_buf_raw` returned 0 for v=0.

Fix: `typed_field_to_raw` now always emits the full natural byte width; `AGG_INT_KEY_CAP` bumped to 32 bytes; `cmd_aggregate` gates `use_int_keys` on total width fitting the cap; `agg_ctx_clone_shared` propagates the flag; `gbuf[i][0]` NUL-init'd before each decode. Pinned in `test_agg_int_groupby_multi`.

### FT_TIME parser — strict validation

The `time` field encode + criterion parse both blindly indexed `val[0..7]` without validating that positions 2/5 were `:`, that the digit positions held digits, or that hh/mm/ss were in range. Garbage input silently encoded to incorrect seconds-of-day. Now both paths require all eight chars to be well-formed `HH:MM:SS`; malformed encodes 0, matching the FT_UUID convention. Pinned in `test_config_encode`.

### `-march=native` is now opt-in via `BUILD_MARCH`

Previous release builds hard-coded `-march=native`, tying any CI-shipped binary to the build host's microarchitecture. Default release is now `-O2 -flto=auto` (portable across x86-64 + ARM64). Self-built deployments opt in:

```bash
./build.sh                          # portable (default)
BUILD_MARCH=native ./build.sh       # self-built, full local codegen
BUILD_MARCH=x86-64-v3 ./build.sh    # portable-but-modern (BMI2 / AVX2)
```

### Branch protection

Classic branch protection enabled on `main` (mirror of the existing ruleset). Closes the Scorecard Branch-Protection finding without changing the existing PR-required workflow (admin bypass preserved for solo-OSS dev ergonomics).

## 2026.05.3

### Reindex memory safety — adaptive batching + pre-sized pairs + exact-key malloc

`./shard-db reindex` on big-record schemas could OOM the host. On a 25M-row, 12-indexed-field schema, peak RSS hit ~25 GB and froze a 29 GB / 0-swap desktop into kernel direct-reclaim spin. Three compounding causes — all fixed:

1. **Scan callbacks malloc'd `f->size` per record per field** — a `varchar:100` reserved 100 B for a 12-character email. Peek the varchar length prefix and malloc exact size in both `index_scan_cb` and `multi_index_scan_cb`. On varchar-heavy schemas this alone trims gigabytes of fragmentation at 25M scale.
2. **Pair arrays doubled exponentially from 4096.** Pre-size from `get_live_count()` with a 4096 floor and 1 Gi BtEntry cap; the doubling path stays as a fallback for concurrent inserts that push past the estimate. Eliminates the 2× transient peak from realloc holding old + new buffers.
3. **`cmd_add_indexes` held every field's `pairs[]` + `parted_per_field[]` alive simultaneously** for the fused single-scan optimisation — peak = `O(nfields × records)`. New adaptive batching estimates per-field bytes from `live_count × (BtEntry + partition copy + per-key malloc + glibc chunk overhead)` and groups fields into passes that fit `INDEX_BUILD_BUDGET_MB` (default **1024 MB**, floor 64). Each pass keeps the existing parallel scan + parallel build; only fields-per-pass concurrency is bounded. An oversized single field still runs alone via always-include-at-least-one.

Estimator knows each field's typed encoding (varchar 50 % fill, fixed types from the schema, composites by summing child ASCII widths — `status+invoiceDate` is ~18 B per key, not the previous flat 64 B).

Validation on 25M × 12 fields:
- Before: ~25 GB peak, host froze, hard reset required.
- After: ~2 GB peak per pass, reindex completes in **105 s**.

Tuning rule: budget is a **memory cap, not a speed knob**. At this scale, `1024 → 105 s` and `8192 → 92.7 s` — only ~13 % saved by 8× the budget because the per-record callback work scales with `nfields × records` regardless of batching, and qsort + `btree_bulk_build` dominate. Raise for memory headroom (and to keep peak under cgroup limits), not throughput.

`./migrate`'s phase-2 reindex picks this up automatically — operators upgrading from 2026.05.2 do nothing extra; large-object reindex during upgrade is now bounded and won't freeze the host.

### B+ tree v3 — O(1)-step DESC iteration via `prev_leaf` + `last_leaf_page`

File magic bumped: `BT_MAGIC = 0x42545247` ("BTRG"). Two header changes:

- `BtFileHeader.last_leaf_page` — rightmost leaf in the chain. DESC iterator starts here in O(1).
- `BtPageHeader.prev_leaf` — backward link. DESC iterator steps left via `ph->prev_leaf` instead of indexing into a precomputed array.

Old DESC walks did `root → leftmost leaf → forward-walk the entire leaf chain into a `desc_leaves[]` array → iterate right-to-left`. That malloc'd a buffer proportional to leaf count for the iterator's lifetime and added forward-walk latency to cursor start. V3 replaces the array with a 1-slot cursor.

Split-path maintenance: on `bt_split_leaf`, the new right half's `prev_leaf` is the old page; the old next leaf's `prev_leaf` becomes the new id; if the old leaf was the rightmost, `last_leaf_page` advances to the new id. `leaf_rebuild` preserves `prev_leaf`.

Older formats are rejected at open with a clear error: **V1 (string-keyed, `BTRE`)** and **V2 (binary keys, no `prev_leaf`, `BTRF`)** require a reindex. `./migrate` phase 2 handles this automatically — operators upgrading from 2026.05.2 see no extra steps; existing v2 `.idx` files are wiped and rebuilt as v3 during migrate's reindex.

### Performance — parallel single-spec aggregate fast path

`cmd_aggregate`'s sum/avg/min/max fast path (single indexed numeric field, no criteria, no group_by) used to walk the field's idx shards sequentially. On 25M records × 16 idx shards that serialised 16 cold leaf scans for a 9-15 s total. New `agg_single_shard_worker` + `AggSingleArg` fan out per-shard accumulation via `parallel_for_io`; cold reads overlap on the I/O queue, then a single reducer merges. Sequential calloc-OOM fallback preserves correctness on tight hosts.

Bench wins on 25M users (single-conn): `sum age` ~9 s → ~190 ms range; `sum balance` similar. MIN/MAX already short-circuited after one leaf entry — they pick up the parallelism but the savings are smaller in absolute terms.

### Fixed — pure-OR count missed single-child AND wrapper

`keyset_count_from_or` detected "pure OR" (no AND siblings, no per-record re-match needed) with:

```
tree == or_node || tree->kind == CNODE_OR
```

The common shape `criteria: [{"or":[...]}]` parses as `CNODE_AND { n_children=1, children[0] = or_node }`, which the guard classified as hybrid. Pure-OR counts ran a per-record re-match they didn't need. Added the third case — single-child AND wrapping the OR returns `|KeySet|` directly.

### Fixed — bench harness clobbered the operator's `db.env`

`test_env_start_at` (used by every C bench's persistent mode) wrote a fresh minimal 8-line `db.env` at `<base>/db.env` on every run. With `SHARD_BENCH_DB_ROOT=./db` (the documented way to run `bench-queries` against operator data) base resolves to `.`, so the bench silently nuked the repo-root `db.env` — losing `INDEX_BUILD_BUDGET_MB`, `QUERY_BUFFER_MB`, `AUTO_VACUUM_*`, and everything else the operator had configured. Fix: skip the write when `<base>/db.env` already exists, and parse `PORT=` out of the existing file so the bench client connects where the daemon actually binds.

### Tooling — `bench-queries` persistent-mode header reports actual on-disk size

Header used to print `COUNT` (the env target) regardless of skip-insert state, so a persistent run against a 25M dataset showed `— 1000000 users`. Now queries `size` and substitutes the live count; tags the line `persistent` and drops the chunk= field (no inserts happen).

### New env knob — `INDEX_BUILD_BUDGET_MB`

Peak per-pass memory budget for `cmd_add_indexes` / `reindex` / `./migrate` phase 2. Default `1024` (1 GiB), floor 64. See [Tuning → INDEX_BUILD_BUDGET_MB](../operations/tuning.md) and [Configuration](../getting-started/configuration.md).

### kf shard auto-resplit — unbounded inserts, no per-shard cap

Each kf shard now grows in place by doubling whenever its load crosses 75 %. No global ceiling; shards keep doubling indefinitely. If a single shard ever becomes operationally unwieldy, `shard-stats` surfaces it and the operator reshards via `vacuum --splits=N`.

Mechanics:
- **24-byte header** prefixes every kf file: `[magic 'SKF1'][version][total uint64][deleted uint64]`. `total` counts non-empty slots (live + tombstoned) — the resplit trigger metric, since tombstones still create lookup probe-chain pressure. `deleted` counts tombstones; live = total − deleted, computed only when callers need it. Updates happen under the kf wrlock, no atomics.
- **Streaming resplit:** walk old kf in order, write each flag=1 entry directly into `kf.new`'s mmap'd region via linear-probe at the new capacity. Zero malloc — memory cost stays flat regardless of shard size. Tombstones are dropped during resplit (`new.total = live_copied`, `new.deleted = 0`), so resplit also reclaims tombstone space.
- **Crash safety:** `kf.new` is staged-then-renamed; old kf stays live until the atomic rename. `slotcask_open` unlinks leftover `kf.new` files at startup — idempotent recovery.
- **Trigger:** `header.total * 4 >= capacity * 3` checked once per `kf_put_new`. One mmap load on the hot path.

Closes the gap where the engine had a hard insert cap based on `splits × slots_per_shard`. The cap is gone; doubling is bounded only by disk space.

### v2 default vacuum — Direction-C seg compaction + streams-mismatch self-heal

Default `{"mode":"vacuum"}` (no flags) on a v2 object used to be a no-op besides resetting the `deleted` counter. It now does two things:

1. **Direction-C seg compaction.** Per stream, every non-active seg file is stat'd for live count. The sparsest are pair-merged into denser ones — donor's live records are migrated into recipient's tombstone holes via `kf_repoint_at_slot`, then the donor file is unlinked (segcache wrlock drains in-flight readers, `msync + munmap + close + unlink + fsync(parent)`). The active seg of each stream is never touched, so concurrent appends after vacuum return are unaffected. Reclaims disk for delete-heavy / no-write workloads where the snake-game pool can't reuse tombstones inline.
2. **Streams-mismatch self-heal.** If `slotcask_streams_for_nproc()` no longer matches `schema.streams` (CPU upgrade, container resize, hand-edited schema), the call promotes to a full rebuild that re-routes records into the new stream layout. `vacuum --splits=N` folds in the same check on the same rebuild.

`./shard-db vacuum <dir> <obj>` and the auto-vacuum thread both pick this up. Response shape: `{"status":"vacuumed","cleaned":<files-dropped>}` for the light path; `{"status":"rebuilt", ...,"streams":N,...}` for the heavy path.

## 2026.05.2 — 2026-05-05

### Performance — aggregate fast paths (sum/avg/min/max + NEQ + EXISTS)

Single-spec aggregates without group_by/having/criteria on an indexed non-varchar field now walk btree leaves directly. Encoded leaf bytes decode straight to a double via the inverse of `encode_field_for_index`, so no record fetch and no slot-header probe per row. The full record-decode scan path is reserved for multi-spec aggregates (`{"sum","avg","min","max"}` together) and grouped aggregates.

Bench wins on a 1M-record `users` object (single-conn, default schema):

- AGGREGATE single-fn standalone (30 rows: count + sum/avg per type + min/max per type): **8.8s → 202ms total** (~43× faster).
- `min/max` per-type: 200-400ms each → **0.04-0.35ms** each (record-fetch elimination dominates).
- `sum/avg` per type: 250-380ms each → **13-22ms** each (still scans every leaf, but no record decode).

Two related shortcuts:

- **NEQ aggregate count-only path** — `agg(count where field neq X) = live_count − count(field eq X)`. Previously the planner ran a full `scan_shards` to compute `count(*)`; now uses metadata `live_count`. **156ms → 0.42ms** at 1M (~370× faster). Works for both `{...}` and `[{...}]` criteria forms — the array form (parsed as `CNODE_AND` with one child) was previously missing from the eligibility check.
- **EXISTS / NOT_EXISTS shortcut** — for non-varchar typed fields every record carries the field, so `count(EXISTS field) = live_count` and `count(NOT_EXISTS field) = 0` by definition. No scan. **22ms → 0.05ms** for the 12 typed-field rows in `bench-queries`. Varchar EXISTS / NOT_EXISTS now route to `PRIMARY_NONE` (parallel `scan_shards` 64-way) instead of the contended single-counter btree walk; ~22ms → ~3ms.

Code: `src/db/query.c` (`decode_index_key_to_double`, the `Fast path: single-spec SUM / AVG / MIN / MAX` block in `cmd_aggregate`, the `count_only` branch in the NEQ shortcut, the existence shortcut at the top of `cmd_count`, `leaf_is_indexed` change to bail EXISTS/NOT_EXISTS).

### Performance — regex on indexed varchar

`regex` and `not_regex` on indexed varchar fields no longer fall to a full record scan. The planner allows them through `leaf_is_indexed` for varchar and the callback runs `regexec` against the literal leaf bytes. A thread-local `(pattern → regex_t)` cache in `match_criterion` ensures `regcomp` fires once per thread per distinct pattern, not once per leaf entry — without it, enabling the indexed path would have regressed on workloads hitting `collect_hash_cb` / `idx_count_cb` millions of times. Non-varchar indexed fields stay on the full-scan path because their leaves carry encoded sortable bytes (top-bit-flipped ints, etc.) that regex would match against garbage.

### Performance — query planner cleanups

- **Range coalesce on same-field bounds** — `gt/lt/gte/lte` pairs on the same field collapse to one `BETWEEN` with `min_exclusive`/`max_exclusive` flags. All four pairings (`gt+lt`, `gt+lte`, `gte+lt`, `gte+lte`) hit the indexed range path; previously only `gte+lte` got the win and the other three ran two separate range walks. Bench: paired-range rows 4-5ms → 2-3ms.
- **OR limit pushdown** — pure-OR `find` paths now stop building the union once `offset+limit` candidates are reached. Big rematch step skipped entirely when the limit is small.
- **KeySet capacity floor on intersect** — capacity is now `max(leaf_capacity_hint, live_count)`. Previously a heavily-compressed btree's `leaf_capacity_hint` could under-size the KeySet and the table would saturate under bulk inserts. The 3-way `active+age+score` intersect on bench-queries went from **74s → 91ms** (~800×) once the capacity stopped capping early.
- **Index fan-out curve** — `index_splits_for(splits)` is now a non-linear table (`8→2, 16→4, 32→4, 64→8, 128→16, 256→16, 512→32, 1024→64, 2048→64, 4096→128`) instead of `splits/4`. Caps idx fan-out at high split counts so a 4096-shard object doesn't open 1024 idx files for every search.

### Fixed — `count(varchar field)` over-counted empty strings

`agg_scan_cb`'s `AGG_COUNT` branch incremented for every matched record without checking the field's value. Typed records always carry every field, but a varchar field can have empty content (`elen == 0`); `count(varchar_field)` should match `OP_EXISTS`-on-varchar semantics and skip empties. Fixed plus three call-site fixes: `spec_tfs[i]` now resolves for `AGG_COUNT` specs (was skipped), the metadata fast path bails when count's field is varchar, and the NEQ count-only shortcut bails on `count(varchar field)` since `idx_count_cb` can't apply the elen filter. Test: `test-count-varchar-field` (7 assertions, including grouped + criteria-narrowed forms).

### Fixed — `./shard-db start` reported success but daemon didn't listen

The startup metadata validator added in 2026.05.2 ran *after* `fork()` so its stderr went to `/dev/null` and the parent had already printed `shard-db started (pid N)`. Operators saw "started" then immediate "stopped" with the only diagnostic in `error.log`. Two fixes: validation moved before fork so any future fatal error reaches the user's terminal, and the `dirs.conf` consistency rule softened from fatal to a warning. Stale schema entries can't cause silent mis-routing — the auth/route layer already rejects unknown tenants — so refusing startup blocked operators on any DB that had outlived a removed test tenant.

### Tooling — bench harness uses unified table view

All eight benches (`bench-queries`, `bench-invoice`, `bench-joins`, `bench-kv`, `bench-kv-parallel`, `bench-parallel`, `bench-grow`, `bench-incremental`) now produce sectioned tables with relative bar charts and min/p50/max/total footers via `src/bench/bench_table.c`. New `bench_table_record(label, us, ok, extra)` lets pre-computed timings (bulk-insert throughput, pipelined latency batches, parallel-worker fan-out) share the same section as single-shot `tc_request` rows; `extra` is an optional trailing column for throughput-style metadata (`0.39 M rows/s`, `p50=31µs  31 k op/s`).

### Tooling — `bench-queries` covers every operator × every applicable type

222 rows across 21 sections. Every operator class touches every applicable field type so per-type pathology surfaces in one run: `eq` / `neq` / range / `in`/`not_in` / `exists` / string ops (CS + CI) / `len_*` / regex / field-vs-field / OR widths / aggregate single-fn + with-criteria + bundled / cursor by 7 indexed types. Insert path uses 10M-record chunks so 1M / 10M / 100M scales all run with bounded peak memory.

### Performance — bulk-insert pre-grow

Bulk-insert no longer grows shards incrementally during the write phase. The dispatcher computes each shard's target slot count from the incoming batch (`next_pow2(live + incoming)`) and grows each shard once, in parallel, before workers start. The previous behaviour rebucketed existing data on every doubling — eliminated.

Same-shape benchmark wins on AMD Ryzen 7 7840U (C-bench measurements):

- K/V CSV bulk insert (10M, single conn, SPLITS=128): 2.39 → **5.34 M/sec** (2.23×)
- K/V CSV bulk insert (10M, 5 conns × 2M): 2.72 → **7.55 M/sec** (2.78×)
- Invoice CSV bulk insert (1M, single conn, no idx, SPLITS=64): 238 → **505 k/sec** (2.12×, bash measurement; C-bench likely higher)
- Invoice load-then-index (1M, CSV + add 14 idx): 6.47 s → **4.76 s**

**Tuning rule:** the pre-2026.05.x guidance — *use multiple connections (`R ≈ N/200K`, `5 ≤ conns`) for max throughput* — still applies. Pre-grow makes every path ~2× faster; parallel inserts continue to scale ~1.4–1.6× over single-conn at this hardware. Earlier docs in this branch briefly claimed "single now beats parallel"; that was a bash-bench artifact (the bash parallel test forks `shard-db query` subprocesses per chunk, costing 10–30 ms each ×5 chunks). The C bench (`shard-db-bench run bench-kv-parallel`) confirms parallel still wins.

Operational guidance:

- For max throughput: parallel connections with chunks of ~2 M records.
- For simplicity: single connection — it's ~1.4× behind the parallel peak, so the trade is real but small.
- For indexed batch loads at 1M+ records: **load-then-index** is competitive and avoids the per-(field, shard) merge cycle that scales `O(R²)` with request count.
- For streaming with pre-existing indexes: parallel + small `R = N / 200K` chunks remains the right pattern.

Read paths, single-record writes, deletes, vacuum, recount, query/count/aggregate are all unchanged — no regressions.

Code: `src/db/storage.c` (`ucache_grow_to`, `ucache_peek_slots`), `src/db/query.c` (`pre_grow_shards_for_bulk_insert`), bench harness at `src/bench/bench_grow.c`. The delimited-format bulk-insert path now emits the same `BULK-INSERT … grows=N grow_total=Tms` log line as the JSON path at `LOG_LEVEL>=3`.

A C test/bench framework also landed in this work (`build/bin/shard-db-test`, `build/bin/shard-db-bench`) replacing bash benches with sub-µs-precision C measurements. All future perf claims should come from these.

## 2026.05.1 — 2026-05-02 (reissued)

Originally released 2026-04-30 as the per-shard btree release. The tag was deleted and rebuilt 2026-05-02 with the response-shape overhaul + `./migrate` upgrade binary bundled in. **Replace your build from the prior 2026.05.1 download — read responses changed shape.**

### Breaking — read response shapes

Read modes now return bare values where possible. Update your client.

| Mode | Before | After |
|---|---|---|
| `get` (single) | `{"key":"u1","value":{...}}` | `{...}` (bare value dict) |
| `get` (multi) | `[{"key":"u1","value":{...}},...]` | `{"u1":{...},"missing":null,...}` (dict; missing → null; empty → `{}`) |
| `exists` (single) | `{"exists":true}` | `true` |
| `count` | `{"count":42}` | `42` |
| `size` | `{"count":N}` (+ optional `orphaned`) | bare integer (live count only) |
| `orphaned` (NEW) | — | bare integer (tombstoned slot count, O(1)) |

Errors continue to come back as `{"error":"..."}` so clients can branch on JSON type to disambiguate. Multi-key `exists`, `keys`, `aggregate`, all writes, all admin/file/auth/stats modes are unchanged.

### Added — `find` / `fetch` `format:"dict"`

`format:"dict"` returns `{"k1":{...},"k2":{...}}` — O(1) lookup by primary key on the client side, round-trips with `bulk-insert`'s dict shape. Works on every find path including indexed planner branches (PRIMARY_LEAF, PRIMARY_INTERSECT, PRIMARY_KEYSET) and cursor pagination (envelope becomes `{"results":{...},"cursor":...}`). Rejected with `join` (joins force tabular). With `order_by`, dict iteration order is parser-dependent — use the default array or `format:"rows"` if strict iteration order matters.

### Added — `format:"csv"` works with `join`

Joined queries can now emit raw CSV instead of the default `{"columns":[...],"rows":[...]}` JSON envelope. Same column-naming convention (`<driver>.key`, `<driver>.<field>`, `<as>.<field>`); left-join no-match → empty cell. Custom delimiter via `delimiter:"|"`. Dict format is still rejected with joins (joined rows have no single primary key to dict-key on).

### Added — `bulk-update` accepts dict shape

Both `records:` (inline) and `file:` payloads now accept either:

- `{"k1":{...},"k2":{...}}` — round-trips with `get-multi`
- `[{"id":"k1","data":{...}}, ...]` — existing array form

Same as `bulk-insert` already worked.

### Added — `./migrate` binary

Per-release one-shot upgrade runner. Runs every required migration for the release with the daemon stopped, then exits. For 2026.05.1 it does:

1. **migrate-files** — lift pre-2026.05.2 `<obj>/files/<XX>/<XX>/<filename>` hash buckets to flat `<obj>/files/<filename>` layout (filesystem-only, holds the same `.shard-db.lock` flock as the daemon).
2. **reindex** — spawn `./shard-db start`, run `./shard-db reindex`, stop the daemon. Rebuilds every B+ tree under the per-shard layout shipped in 2026.05.1.

Idempotent — re-running after a successful pass is a no-op. Linked into `build/bin/migrate` alongside `shard-db` and `shard-cli`.

### Removed

- `./shard-db migrate-files` CLI subcommand → moved to `./migrate`. Running it now redirects with a pointer to the new binary.
- `{"mode":"migrate-files"}` JSON dispatch removed from the daemon.
- `cmd_migrate_files()` (and its helpers) removed from query.c so the dead code doesn't ship with future releases.

### Changed

- **Bulk array-form record fields renamed** — `bulk-insert` and `bulk-update`'s array form (`records:[...]` and file payloads) now expect `"key"` / `"value"` instead of `"id"` / `"data"`. Aligns with `insert` / `update` single-record requests and the new `get-multi` dict shape. The dict form (preferred) is unaffected. **Update existing payloads** — old field names are no longer accepted (the parser silently treats records without the new names as malformed and counts them as `skipped`).
- **`bulk-insert` / `bulk-insert-delimited` clean-path response field renamed** — `{"count":N}` → `{"inserted":N}`. The CAS path always used `"inserted"`; this aligns the no-skips path so the field name is consistent across all three response shapes (clean, with skips, with errors).
- **`bulk-insert-delimited` default delimiter is now `,`** (was `|`). Aligns with `bulk-update-delimited` and CSV format on `find`/`fetch`. Pass `delimiter:"|"` explicitly if you need pipes.
- Documented that `bulk-insert` accepts both dict and array shapes (the parser already supported both — the doc was incomplete).

### Upgrade procedure

```bash
./shard-db stop
# replace build/bin/ contents with the new release artifacts
./migrate                        # one-shot; idempotent
./shard-db start
```

### Original 2026.05.1 — per-shard btree release

### Changed

- **Indexes are now per-shard.** Each indexed field stores its B+ tree as `splits/4` files under `<obj>/indexes/<field>/<NNN>.idx`. Reads fan out across all shards in parallel via the worker pool; writes route by record hash to a single shard. Per-file `pthread_rwlock_t` gives readers and writers proper isolation (the pre-2026.05.1 single-file layout had a race window where `bulk_build`'s truncate could be observed by an in-flight reader's mmap).
- **`BT_CACHE_MAX` is no longer configurable** — derived as `FCACHE_MAX / 4`. Setting it in db.env emits a stderr warning and is ignored. `FCACHE_MAX` accepts a strict allow-list of `{4096, 8192, 12288, 16384}`.
- **`vacuum --splits` triggers a full reindex** because the per-field shard count depends on `splits`. The data rebuild is followed by `reindex_object()`, which wipes and rebuilds every per-field idx directory at the new shard count.
- **`bulk-insert` is a true upsert** — overwriting an existing key drops its stale index entries before writing the new value. Pass `if_not_exists:true` to keep the old idempotent behaviour.

### Performance

- Bulk loads ~117 k records/sec single-thread on the 14-index invoice schema (1 M records, splits=64). Add-indexes-from-scratch ≈ 350 k records/sec equivalent.
- For parallel inserts into a pre-existing-indexed object, prefer **fewer, larger** `bulk-insert` calls. Each call triggers a sequential `bulk_merge` per (field, shard); cumulative work scales O(R²) in request count. Sweet spot at 1 M records is **5 connections × 200 K records each**.

### Trade

- Disk footprint up ~25 % (smaller per-leaf working sets reduce prefix-compression effectiveness; ~1.8 MB of empty-tree headers for a typical 14-index schema).
- Insert-with-pre-existing-indexes hits N×16 file ops per merge call instead of N×1. **Load-then-index** is now the recommended pattern for static schemas.

### Documentation

- New [`shard-cli`](../cli/shard-cli.md) page — full reference for the ncurses TUI binary built alongside `shard-db`.
- All docs updated for the per-shard layout, 38 search operators, native TLS, per-tenant + per-object tokens, AND index intersection, cursor pagination.

## 2026.05 — 2026-04-29

Major feature drop.

### Added

- **38 search operators** — original 17 plus length operators (`len_eq/neq/lt/gt/lte/gte/between` on varchar, answered from btree leaf vlen with no record fetch), case-insensitive variants (`ilike`, `icontains`, `istarts`, `iends`, `not_ilike`, `not_icontains`), field-vs-field on the same record (`eq_field`, `neq_field`, `lt_field`, `gt_field`, `lte_field`, `gte_field`), and POSIX extended regex (`regex`, `not_regex`, compiled once at criteria-compile time).
- **Native TLS 1.3** via OpenSSL — opt in with `TLS_ENABLE=1` in db.env. Single-port (TLS-only when enabled). Reverse-proxy termination remains supported as the alternative.
- **Per-tenant and per-object tokens** with `r` / `rw` / `rwx` permissions. Tokens live in `<dir>/tokens.conf` or `<dir>/<obj>/tokens.conf`. Token management is always server-admin scope.
- **Cursor pagination on `find`** — keyset cursor on any indexed `order_by` field. O(limit) per page regardless of depth. Pass `cursor:null` to opt in; `cursor:null` in the response signals last page.
- **AND index intersection** — `PRIMARY_INTERSECT` planner branch for pure AND of 2+ indexed leaves on rangeable operators. Walks each leaf's btree into a `KeySet`, intersects the sets, and skips per-record fetch entirely for `count`. Big speedups when intersection is much smaller than any single leaf.
- **OR criteria** in `find` / `count` / `aggregate` / `bulk-update` / `bulk-delete`. Five planner paths, including pure-indexed-OR via lock-free `KeySet` union (no record fetch for count).
- **CSV / delimited export** on `find`, `fetch`, `aggregate`, `get` (multi-key), `keys`, `exists` (multi-key) via `format:"csv"` (+ optional `delimiter`). RFC 4180-style quoting.
- **Per-request `timeout_ms`** override for `find` / `count` / `aggregate` / `bulk-delete` / `bulk-update`.
- **Per-query memory cap** via `QUERY_BUFFER_MB` (default 500) at every collection site.
- **`shard-cli`** — separate ncurses TUI binary built alongside `shard-db`. Top-level menus: Server, Browse, Query, Schema, Maintenance, Auth, Stats. See [CLI → shard-cli](../cli/shard-cli.md).
- **`stats-prom`** — Prometheus text-format exposition of the same counters as `stats`.
- **`list-objects`** + **`describe-object`** — schema/catalog discovery used by shard-cli; useful for any tooling.
- **`list-files`** — paginated, alphabetical inventory of stored files for an object, with optional `prefix`.
- **`add-dir` / `remove-dir`** — runtime tenant-directory management; `remove-dir` defaults to refusing non-empty trees.
- **`delete-file`** — JSON mode + CLI shortcut.
- **Bulk update by JSON list** — `{"mode":"bulk-update","records":[{"id":"k","data":{...}}]}` for per-key partial updates (alternative to the criteria form).
- **`bulk-insert-delimited`** — CSV-style flat file loader, parses directly against the page cache with no per-line memcpy.
- **Aggregate NEQ algebraic shortcut** — `count(neq X)` rewrites to `count(*) - count(eq X)`.
- **Single-instance guard** — `flock` on `$DB_ROOT/.shard-db.lock` prevents two daemons from sharing a data root.

### Changed

- Server can now `mkdirp(db_root)` on first start — no need to pre-create the data root.
- Build directory ships `bin/db.env.example` (won't overwrite an existing `db.env`).
- Removed `start.sh` / `stop.sh` / `status.sh` wrapper scripts (the binary's lifecycle commands are sufficient).

## 2026.04.3 — 2026-04-18

### Added
- `remove-index` JSON mode + CLI — drop an index by exact name without touching data. Safe on non-existent names (idempotent).
- `put-file` **base64-in-JSON** variant — remote-safe uploads that don't require shared filesystem access. Atomic `.tmp`+`fsync`+`rename`.
- `put-file` **`if_not_exists`** — CAS on file uploads, same semantics as insert CAS.
- `get-file` JSON mode + CLI — stream files back to any remote client, base64 over the wire.
- Filename sanitizer — rejects `/`, `\`, `..`, control chars, empty or oversized names.

### Changed
- `./shard-db put-file <dir> <obj> <path>` CLI routes through the new TCP base64 path by default, working from any host with TCP access. The old server-local path remains accessible via explicit JSON (`{"mode":"put-file","path":"..."}`).

### Fixed
- Oversized-request error path no longer hangs the client. The "Request too large" handler previously emitted a format string with an embedded NUL, truncating the response terminator; clients would wait forever for `\0`.

### Documentation
- `/docs` tree introduced with MkDocs Material. GitHub Pages deployment wired up.

## 2026.04.2 — 2026-04-18

### Added
- `order_by` + `order` on `find` — sort matches before pagination (numeric for numeric types, lexicographic for varchar). Not compatible with `join`.
- `*` wildcard on `LIKE` — in addition to `%`, accepts `*` as the glob character for ergonomic match patterns.

### Changed
- `MAX_FIELDS` bumped from 64 to **256** per schema.

## 2026.04.2 (patch, same day)

### Fixed
- Fresh-install Quick Start: pidfile was written before the logs directory existed; tenant wasn't auto-registered in `dirs.conf` on first-use. Both fixed.
- Legacy stdio fast-path returned SEGV on missing objects instead of a clean error; drained `in_flight_writes` on early-return.
- Several README vs code mismatches caught during pre-release validation.

## 2026.04.1 — 2026-04-17

Initial v1 release.

Core storage, query engine, indexes, CAS, schema mutations, multi-tenancy, auth, async logging, stats, 167 tests across 6 scripts. See the repo `CHANGELOG.md` for the full v1 feature inventory.

## Versioning

Releases follow `yyyy.mm.N` — year-month plus a counter within that month. There is no separate "v1" / "v2" track; new features ship in the next monthly release. Anything not yet shipped lives as an open issue on the [GitHub repo](https://github.com/sayyiditow/shard-db/issues), not a roadmap doc.
