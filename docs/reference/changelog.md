# Changelog

For the full history see [`CHANGELOG.md`](https://github.com/sayyiditow/shard-db/blob/main/CHANGELOG.md) at the repo root. This page summarizes shipped releases and notes on what's in flight.

Versions follow `yyyy.mm.N` — year-month, with `N` as the counter within that month.

## Unreleased — slotcask-engine branch

### kf shard auto-resplit — unbounded inserts without operator intervention

When a kf shard's global load crosses 75 %, the next insert into that shard doubles the kf file in place via linear-hashing rehash:

- Snapshot every flag=1 entry, stage a fresh kf.new at 2× size, repopulate by linear-probing at the new capacity, atomic rename + fsync(parent dir), tear down the old fd/map under the entry wrlock.
- Trigger is one relaxed atomic load on `SlotcaskDb.live_count` per insert — sub-ns on the hot path. Resplit fires once per doubling, amortised over hundreds of thousands of inserts.
- Hard ceiling: `SLOTCASK_KF_MAX_SLOTS_PER_SHARD = 16M` slots per shard. At 16M × 24B × 4096 shards that's 1.5 TB of kf — past any realistic dataset. Beyond the cap, operator reshards via `vacuum --splits=N`.
- Crash safety: kf.new is staged-then-renamed; if the rebuild crashes mid-write the old kf is still the live file. `slotcask_open`'s kf-walk loop now also unlinks any leftover `kf.new` at startup. Idempotent.

Closes the gap where the engine had a hard insert cap based on `splits × slots_per_shard` (e.g. 8M at splits=8, 32M at splits=128) — inserts past that cap used to fail silently. They now grow the kf instead.

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
