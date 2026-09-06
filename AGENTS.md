# AGENTS.md

User-facing docs live under `docs/`.

@CORE-PROCESS.md

## Standing exceptions for this repo

- **Execution mode:** leave work **uncommitted** after plan execution — the reviewing agent + human MUST review the raw `git diff` first; nothing is committed until that review pass is done.
- **Build/test commands for plans:** build with `SKIP_TESTS=1 ./build.sh`; test with `./build/bin/shard-db-test run[-all]`.
- **Dynamic-safety tooling for this repo (CORE-PROCESS.md's "Definition of done" gate):** ASan+UBSan and TSan, via `BUILD_MODE`. Any diff touching locks, shared/cached state (kfcache, segcache, bitmap cache, btree cache), object lifetimes, or background threads must be run locally under both before being called done — not deferred to CI. This workspace supports normal default all-core parallelism for both sanitizers; do not add `--jobs` unless a newly demonstrated harness limitation requires it. Run each full suite three consecutive times:
  - `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`, then three fresh runs of `./build/bin/shard-db-test run-all` (no env options needed — see below).
  - `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`, then three fresh runs of `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all` (no suppressions file — `.tsan.supp` was eliminated 2026-08-30; every prior finding is fixed or restructured).
  - **Strictness is baked into the asan build** (`-fno-sanitize-recover=undefined`): UBSan runtime errors abort at the point of error, ASan halts on the first memory error, and LSan fails the exit code on leaks — a bare run-all is the gate. **No `halt_on_error=0` anywhere in gate invocations**: findings must fail the run, not print and continue (a collect-all gate masks leaks/runtime errors behind tolerated child exit codes; the 2026-08-27 ASan gate passed 3× that way while `slotcask_bulk_fetch_resolved` leaked on every call). If a finding aborts a daemon mid-suite, that is the gate working; fix it, then rerun. Reruns after each fix surface subsequent findings. TSan needs no equivalent flag — its default already exits nonzero when any report fired (suppressed findings excluded). Debugging aid only: `UBSAN_OPTIONS=print_stacktrace=1` adds a call stack to UBSan's one-line report.
  - New findings get root-caused and either fixed now (if simple — see CORE-PROCESS.md's "never paper over issues") or written up as a `docs/plans/<date>-<slug>.md` and, only if deliberately deferred, added to `.tsan.supp` with a named-function suppression and a full rationale paragraph — never a blanket suppression, never "not a real bug" without justification.
  - CI (`.github/workflows/sanitizers.yml`, `.github/workflows/tsan.yml`) runs both against the full suite as a backstop, not a substitute for the local run above.


## Overview

shard-db is a high-performance database in C. Single static binary, single process, no external dependencies. Typed binary records, B+ tree indexes, joins, aggregates, CAS, multi-threaded TCP server with optional native TLS 1.3. Linux x86_64 / ARM64 + macOS (Apple Silicon, 2026.05.4+).

## Build & test

```bash
./build.sh                                        # builds + runs the C test suite at the end (set SKIP_TESTS=1 to skip)
./build/bin/shard-db-test run-all                 # all registered C tests; transport varies by case
./build/bin/shard-db-test run-all --filter <substr>   # narrow the run
./build/bin/shard-db-test run <name>              # single case (e.g. test-or-logic)
./build/bin/shard-db-test list                    # list registered cases
```

C test cases live under `src/test/cases/test_*.c`. Each links via `TEST_REGISTER` static-init; names mirror the case file. `run-all` uses separate worker processes by default, while `--jobs 1` runs cases sequentially in one process. Cases that exercise the server or wire response start an isolated daemon on a free port and tmpdir. Setup-only and unit cases may instead reuse the process-local `ShardDb` initialized by the runner; those cases must remove any objects they create so the sequential path stays isolated. `SKIP_TESTS=1 ./build.sh` builds without running the suite.

Bench cases live in `src/bench/bench_*.c`, run via `./build/bin/shard-db-bench`. **The user runs benches**; do not run them to validate perf.

## Source layout

### Daemon (`src/db/`)

- `types.h` — shared types, externs, function declarations
- `util.c` — JSON helpers, `b64_encode/decode`, `valid_filename`
- `config.c` — db.env, schema/index/dirs caches, typed-field encode/decode (`encode_field_len`)
- `storage.c` — xxh128 hashing, GET/INSERT/DELETE, CAS helpers, slotcask registry access, `build_idx_path`, `compute_addr`
- `index.c` — per-shard B+ tree wrappers (`btree_idx_*`), parallel indexing, `reindex_clean_legacy`
- `query.c` — criteria matching, planner core, find/count orchestration
- `query_aggregate.c` — aggregate operations, group-by, having, top-N, hash tables
- `query_join.c` — join planning, resolution, lookup, and result emission
- `query_plan.c` — compiled criteria, match_typed*, criteria tree parser, planner (plan_filter), cmd_explain
- `query_maint.c` — maintenance ops (vacuum, backup, recount, reindex, etc.)
- `query_schema.c` — schema mutations (create/drop object, edit/add/remove fields/indexes)
- `query_bulk.c` — bulk insert/delete/update
- `query_find.c` — scan helpers, fetch, keys, exists, CSV output, file ops
- `query_internal.h` — shared cross-TU prototypes, types, and enums
- `server.c` — multi-threaded TCP server (poll-based accept + thread pool), JSON dispatch, auth, stats, optional TLS
- `tls.c / tls.h` — OpenSSL wrapper; `tls_fopen()` wraps `SSL *` as a stdio `FILE *` via fopencookie/funopen so existing OUT() / fgets() call sites stay untouched
- `btree.c / btree.h` — B+ tree (page-based, prefix-compressed leaves, mmap'd, `BtRangeIter`, unified `bt_acquire/bt_release`)
- `objlock.c` — per-object rwlock (normal ops share; vacuum/rebuild exclusive)
- `keyset.c` — lock-free open-addressed hash table of 16-byte xxh128 keys (OR-union + AND-intersect candidates)
- `main.c` — CLI entry point

### shard-cli (`src/cli/`)

Separate ncurses TUI binary; links no daemon source. Speaks the same TCP+TLS wire; reads `HOST`/`PORT`/`TLS_*`/`TOKEN` from env (source `db.env` first).

`cli.h` (decls), `conn.c` (self-contained TCP+TLS client), `widgets.c` (menu/picker/form/modals/status), `views.c` (panels + tiny JSON parser + `describe_object()` cache + criteria builder), `main.c` (entry, env load, top-level dispatch).

Top-level menus: Server / Browse / Query / Schema / Maintenance / Auth / Stats. Builds to `./shard-cli` and `build/bin/shard-cli`.

## Configuration files

- `db.env` — `DB_ROOT`, `PORT`, `TIMEOUT`, `LOG_*`, `THREADS`, `WORKERS`, `GLOBAL_LIMIT`, `MAX_CONCURRENT_QUERIES`, `MAX_REQUEST_SIZE`, `FCACHE_MAX`, `BT_CACHE_MAX = FCACHE_MAX/4`, `QUERY_BUFFER_MB`, `INDEX_BUILD_BUDGET_MB`, `DISABLE_LOCALHOST_TRUST`, `TOKEN_CAP`, `SLOW_QUERY_MS`, TLS knobs (`TLS_ENABLE`, `TLS_CERT`, `TLS_KEY`, `TLS_CA`, `TLS_SKIP_VERIFY`, `TLS_SERVER_NAME`). Full reference: [docs/getting-started/configuration.md](docs/getting-started/configuration.md).
- `$DB_ROOT/tokens.conf` — global tokens. Line format `token[:perm]`, `perm ∈ {r, rw, rwx}`, no suffix = `rwx`.
- `$DB_ROOT/<dir>/tokens.conf` — per-tenant tokens (same format).
- `$DB_ROOT/<dir>/<obj>/tokens.conf` — per-object tokens (same format).
- `$DB_ROOT/allowed_ips.conf` — global trusted IPs (skip token check).
- `$DB_ROOT/dirs.conf` — allowed tenant directories.
- `$DB_ROOT/schema.conf` — `dir:object:splits:max_key:2:streams[:auto_key=<mode>]`. The literal `2` is the engine-version slot, kept in the on-disk format for forward compatibility; this binary refuses any other value at load (legacy v1 objects must be upgraded via 2026.05.4's `./migrate` first). `auto_key=uuid` → 16-byte UUIDv4 keys; `auto_key=seq(<name>)` → 8-byte int64 BE keys from the named sequence. `max_value` is derived from `fields.conf` (sum of typed-field byte lengths) and `slot_size = 24 + max_key + max_value` rounded up to 8, floor 32.
- `$DB_ROOT/<dir>/<obj>/fields.conf` — `name:type[:size|P,S][:default=...]`.

## Storage model (high-level)

Two on-disk layers per object (under `<db_root>/<dir>/<object>/`):

- **Keyfile shards** (`data/kf/NNN.kf`, 3 hex digits, max `MAX_SPLITS=4096`): open-addressed hash table mapping each key's xxh128 hash to its segment location. Each entry is a 24-byte slot header (16B hash, 2B key_len, 1B flag, 1B stream_id, 4B file_id) + key bytes. `flag`: 0=empty, 1=live, 2=tombstone. Shard routed by `hash[0..1] % splits`.
- **Segment files** (`data/streams/NNN/NNNNNN.dat`): append-only rotating files, one stream per the object's `streams` schema field. Each segment record is 24B header (16B hash, 2B key_len, 1B flag, 1B reserved, 4B vlen) + key bytes + value bytes, padded to `slot_size`. This is what the sequential reindex scan reads via `seg_scan_o_direct`.
- **Addressing**: kf shard = `hash[0..1] % splits`; slot = `hash[2..5] % slots_per_shard`, linear probing; segment stream = `hash[15] % num_streams` (hash-routed so a key's insert/update history stays in one stream, maximising free-pool slot reuse).
- **Dynamic growth**: 50% kf load → double `slots_per_shard`. `MAX_SPLITS=4096` caps shard *files*, not slots.
- **I/O**: kf writes use slotcask's kfcache (mmap MAP_SHARED); segment writes append-only; reindex reads segments via O_DIRECT double-buffered scan.
- **Crash safety**: write flag=0 → activate batch flag=1; recovery sweeps stale `*.new`/`*.old` on startup.
- **Indexed-write crash safety**: indexed mutations persist a durable commit-intent marker (KFM2 **V2** batch marker — 16-byte header + one 32-byte `KfMarkerSlot` per record, exact-size validated, `1 ≤ count ≤ 16384`; replay re-derives record bytes from the named segment records) before secondary-index apply. Bulk commands run as deferred requests (two-epoch waves, per-kf-shard writer admission gates held request-wide — see `docs/concepts/concurrency.md`); single writes keep per-window immediate durability. Recovery is forward-replay-only: a crash at any point after the marker is durable causes startup to forward-replay it and apply the window to completion, never roll it back. Marker identity is the exact on-disk path (`MarkerRef`, nonce-bearing names); corrupt, unparseable, or pre-V2 marker evidence fails closed.
- **Concurrency**: per-kf-shard rwlock; per-object rwlock for schema mutations; per-btree-file rwlock (`BT_CACHE_MAX`). Ordered-index callbacks must use `read_record_ref_try` via `BtOrderedWalkHandle` before falling back to a blocking record fetch, so they never wait on kfcache while holding bt_cache rdlocks. Every cursor that delivers a row continuously records its own resume point (not only the cursor that triggers release); a failed reopen clears the buffered head and retires that cursor for the remainder of the walk so it cannot reappear out of order after a later release.
- **Index layout**: each indexed field shards into `index_splits_for(splits)` btree files at `<obj>/indexes/<field>/<NNN>.idx`. Writes route by hash16 to one shard; reads fan out across all shards in parallel; cursor pagination uses k-way streaming merge across `BtRangeIter`s. Routing: `idx_shard_for_hash(hash16, splits)`. The `index_splits_for` curve caps idx fan-out at high split counts: `8→2, 16→4, 32→4, 64→8, 128→16, 256→16, 512→32, 1024→64, 2048→64, 4096→128` (see `src/db/types.h` for the rationale).
- **Record counts (v2)**: per-shard kf header (24 B at byte 0 of every kf shard) carries `total` (live + tombstoned) and `deleted`. `live = total − deleted`. Updated atomically inside `slotcask_put` / `slotcask_delete` under the kf wrlock — single source of truth, durable to mmap writeback (and so survives SIGKILL/OOM/crash without a flush window). `get_live_count` / `get_deleted_count` sum the kf headers via `slotcask_sum_kf_totals` (`pread` per shard, ~1ms cold even at splits=4096). The legacy `metadata/counts` text file is no longer written for v2; `recount` is unnecessary post-crash. v1 falls back to the text file.

Deep dives: [docs/concepts/storage-model.md](docs/concepts/storage-model.md), [docs/concepts/indexes.md](docs/concepts/indexes.md), [docs/concepts/concurrency.md](docs/concepts/concurrency.md).

## Typed binary record format

Driven by fields.conf. One slot = sum of field sizes (fixed).

| Type | Encoding |
|---|---|
| `varchar:N` | `[uint16 BE length][content]` → on-disk N+2 bytes. Max content **65535**. |
| `int / long / short` | 4 / 8 / 2 bytes BE signed |
| `double` | 8 bytes IEEE 754 |
| `bool / byte` | 1 byte |
| `date` | 4 bytes BE int32 (`yyyyMMdd`) |
| `datetime` | 6 bytes (BE int32 yyyyMMdd + BE uint16 packed HHmmss) |
| `datetimems` | 8 bytes (BE int32 yyyyMMdd + BE uint32 ms-of-day) |
| `ipv4` | 4 bytes, network byte order |
| `ipv6` | 16 bytes, network byte order |
| `numeric:P,S` | 8 bytes BE int64 × 10^S |

Field defaults (in fields.conf): `:default=<literal>`, `:auto_create`, `:auto_update`, `:default=seq(<name>)`, `:default=uuid()`, `:default=random(N)`. Reference: [docs/concepts/typed-records.md](docs/concepts/typed-records.md).

## Indexes (high-level)

- B+ tree, prefix-compressed leaves (anchors every K=16, two-stage bsearch).
- Per-shard layout — every indexed field is `index_splits_for(splits)` btree files; `splits` ∈ powers of 2 in [8, 4096]. The `index_splits_for()` mapping is non-linear (caps fan-out at high splits — see types.h for the table) and not separately configurable.
- Wrapper API (`index.c`, declared in `types.h`): `btree_idx_insert/delete` (single shard), `btree_idx_search/range/range_ex` (fan out, callbacks fire in arbitrary inter-shard order), `btree_idx_walk_ordered` (k-way streaming merge for cursor), `btree_idx_unlink_all/exists`.
- Composite indexes: literal `field1+field2` becomes the on-disk directory name.
- All **38 search operators** use index when available — eq family, range, between, in/not_in, like/not_like, contains/not_contains, starts/ends, exists/not_exists, len_* (varchar length, btree-leaf-only no record fetch), case-insensitive i-variants, eq_field family (full-scan only — RHS is per-record), regex/not_regex (POSIX, full-scan only).
- Case-sensitivity: `eq, neq, like, not_like, contains, not_contains, starts, ends` are byte-exact. The i-variants are ASCII tolower.

Deep dive: [docs/concepts/indexes.md](docs/concepts/indexes.md).

## CLI commands

```bash
# Lifecycle
./shard-db start | stop | status | server                       # server = foreground (debug)

# CRUD
./shard-db insert <dir> <obj> <key> <val>
./shard-db get | exists | delete <dir> <obj> <key>

# Query
./shard-db find <dir> <obj> '<criteria>' [off] [lim] [fields]
./shard-db count <dir> <obj> [criteria_json]                    # empty criteria = O(1) metadata
./shard-db aggregate <dir> <obj> <aggregates_json> [group_by_csv] [criteria_json] [having_json]
./shard-db keys | fetch <dir> <obj> [off] [lim] [fields]

# Bulk (JSON dict or array of {key,value} — see docs/query-protocol/bulk.md)
./shard-db bulk-insert <dir> <obj> [file]                       # acts as upsert
./shard-db bulk-delete <dir> <obj> [file]

# Files
./shard-db put-file <dir> <obj> <local-path> [--if-not-exists]
./shard-db get-file <dir> <obj> <filename> [<out-path>]
./shard-db delete-file <dir> <obj> <filename>
./shard-db list-files <dir> <obj> [pattern] [offset] [limit] [--match=<prefix|suffix|contains|glob>]

# Maintenance
./shard-db size <dir> <obj>                                     # disk bytes used by the object (all data + index files); same as du -sb
./shard-db orphaned <dir> <obj>                                 # deleted record count (O(1) metadata)
./shard-db recount | truncate | vacuum | backup <dir> <obj>
./shard-db add-index <dir> <obj> <field> [-f]                   # field or field1+field2
./shard-db remove-index <dir> <obj> <field>
./shard-db edit-field <dir> <obj> <name:type[:param]>           # same-type only, v2 only; JSON form covers batch
./shard-db reindex [dir] [obj]                                  # no args = all tenants

# Diagnostics
./shard-db version                                                # print compiled version + minimum supported source version
./shard-db stats | stats-prom | db-dirs | vacuum-check
./shard-db shard-stats <dir> <obj>

# JSON-only (advanced)
./shard-db query '{"mode":"create-object","dir":"...","object":"...","splits":N,"max_key":N,"fields":[...],"indexes":[...]}'
./shard-db query '{"mode":"create-object","dir":"...","object":"...","splits":N,"max_key":16,"fields":[...],"auto_key":"uuid"}'      # server-generated UUID keys
./shard-db query '{"mode":"create-object","dir":"...","object":"...","splits":N,"max_key":8,"fields":[...],"auto_key":"seq(my_seq)"}'  # server-generated seq keys
./shard-db query '{"mode":"list-objects","dir":"<dir>"}'
./shard-db query '{"mode":"describe-object","dir":"<dir>","object":"<obj>"}'
```

Operators upgrading from a pre-2026.05.5 install that still has v1
(legacy probe-into-slot) objects must first install 2026.05.4 and run
that release's `./migrate` to convert v1 → v2; this version refuses
any v1 object at load.

2026.05.5 also rolls B+ tree magic `'BTRG'` → `'BTRH'` for the
`(value, hash)` sort order. 2026.08.2 performs strict compatibility gating:
an empty root initializes directly, while a populated root must contain
2026.08.1 clean-open evidence. Startup does not migrate data or rebuild
indexes; use `./shard-db reindex` explicitly when required. The standalone
`./migrate`, `migrate-varlen`, and storage migration JSON modes are removed.
The minimum supported source release is 2026.08.1.

**Bulk-insert at scale**: pre-grow (2026.05.x) makes bulk-insert ~2× faster on every path. Parallel still wins for max throughput — C-bench shows CSV K/V at 5.34 M/sec single vs **7.55 M/sec at 5 conns × 2M** (1.41× single). The "single beats parallel" claim that briefly appeared in earlier docs was a bash-bench artifact (shell forked `$BIN query` subprocesses per chunk ×5; each fork costs 10–30 ms). With C pthreads, the original `R ≈ N/200K, 5 ≤ conns` rule still holds.

**Indexed batch ingest (1M scale, 5×200k chunks, C-bench)**: parallel-with-pre-existing-14-idx wins at **2.30 s / 435 k/sec**, beating parallel-no-idx + add-indexes (3.02 s / 331 k/sec) and single-no-idx + add-indexes (3.80 s / 263 k/sec). The earlier "load-then-index always wins" framing came from bash and is reversed in C bench at this scale. **Crossover rule of thumb:** at `R ≈ N/200K`, pre-existing-indexes parallel wins for `R ≤ ~10`, load-then-index wins for `R ≥ ~20` (per-(field, shard) merge cost scales `O(R²)`). Re-bench at your scale; both patterns are valid.

**Bench harness:** `./build/bin/shard-db-bench` (C, sub-µs precision via `clock_gettime(CLOCK_MONOTONIC)`). Eight registered benches: `bench-kv`, `bench-kv-parallel`, `bench-grow`, `bench-invoice`, `bench-parallel`, `bench-queries`, `bench-joins`, `bench-incremental`. Override scale via env vars (`SHARD_BENCH_COUNT` / `SHARD_BENCH_TOTAL` / `SHARD_BENCH_CHUNK` / `SHARD_BENCH_USERS` / `SHARD_BENCH_ORDERS`). Bash bench scripts under `bench/` are deprecated — delete after C-bench is the trusted source.

## JSON query protocol

All advanced queries: `./shard-db query '<json>'`. Wire format: newline-delimited JSON over TCP, response framed by `\0\n`. Full reference: [docs/query-protocol/overview.md](docs/query-protocol/overview.md).

### Modes

| Mode | Reference |
|---|---|
| `find`, `fetch` (criteria + offset/limit/fields/format/order_by/cursor) | [find.md](docs/query-protocol/find.md) |
| `count` | [count.md](docs/query-protocol/count.md) |
| `aggregate` (count/sum/avg/min/max + group_by + having + order_by) | [aggregate.md](docs/query-protocol/aggregate.md) |
| `find` + `join` (inner/left, by primary key or indexed field; tabular only) | [joins.md](docs/query-protocol/joins.md) |
| `bulk-insert / bulk-delete / bulk-update` | [bulk.md](docs/query-protocol/bulk.md) |
| `insert / update / delete` with `if` / `if_not_exists` (CAS) | [cas.md](docs/query-protocol/cas.md) |
| `put-file / get-file / delete-file / list-files / get-file-path` | [files.md](docs/query-protocol/files.md) |
| `add-field / edit-field / remove-field / rename-field / vacuum / add-index / remove-index` | [schema-mutations.md](docs/query-protocol/schema-mutations.md) |
| `add-token / remove-token / list-tokens / add-ip / remove-ip / list-ips / stats / shard-stats / vacuum-check / list-objects / describe-object` | [diagnostics.md](docs/query-protocol/diagnostics.md) |
| `create-object / drop-object` | [overview.md](docs/query-protocol/overview.md) |

Cookbook: [docs/query-protocol/recipes.md](docs/query-protocol/recipes.md).

### Read response shapes (2026.05.1 — bare values)

| Mode | Response |
|---|---|
| `get` (single) | bare value dict (no `{key,value}` wrapper) |
| `get` (multi) | `{"k1":{...},"k2":{...},"missing":null}` |
| `exists` (single) | bare `true`/`false` |
| `exists` (multi) | `{"k1":true,"k2":false}` |
| `count`, `orphaned` | bare integer (record count) |
| `size` | bare integer (disk bytes, same accounting as `du -sb`) |
| `find`, `fetch` | array (default), or `format ∈ {rows, csv, dict}` |

Errors always: `{"error":"..."}` — clients branch on JSON type to disambiguate.

### Per-request knobs (any query)

- `"timeout_ms":N` — per-request override of global `TIMEOUT` (thread-local; doesn't leak across requests). 0/absent = global. Applies to find/count/aggregate/bulk-delete/bulk-update.
- `"format":"csv"` — raw CSV text (not JSON-wrapped) on find/fetch/aggregate/get-multi/keys/exists-multi. Optional `delimiter` (single char, default `,`, accepts `\t` literal). RFC 4180 minus multiline. `csv + join` → tabular CSV (`<driver>.<field>` and `<as>.<field>` columns).
- `"format":"dict"` — `{key:{...}}` on find/fetch. Rejects join.
- `"cursor":null` (or `{}`) — opt into keyset cursor on find. Requires indexed `order_by`. Rejects `format:"csv"` and `join`. See [find.md](docs/query-protocol/find.md) for cursor protocol.

### Auth (scope × permission)

Token's location determines scope: `tokens.conf` at `$DB_ROOT` = global; at `$DB_ROOT/<dir>` = tenant; at `$DB_ROOT/<dir>/<obj>` = object. Suffix determines perm: `r`/`rw`/`rwx` (no suffix = `rwx`, backward-compat). Trusted IP (global only) bypasses tokens. Localhost trusted by default; `DISABLE_LOCALHOST_TRUST=1` for strict mode. Token storage: open-addressed hash table sized `TOKEN_CAP` (default 1024 buckets). Token management is always server-admin. Full admin-scope-per-command table: [docs/concepts/multi-tenancy.md](docs/concepts/multi-tenancy.md).

### Planner (find/count/aggregate criteria tree)

`criteria` is implicit AND. OR via `{"or":[...]}`, explicit AND via `{"and":[...]}`. `MAX_CRITERIA_DEPTH = 16`. `MAX_INTERSECT_LEAVES = 8`. The planner picks one of:

- `PRIMARY_LEAF` — single indexed AND leaf drives, post-filter siblings via `criteria_match_tree`.
- `PRIMARY_INTERSECT` — pure AND of 2+ indexed leaves on rangeable ops (eq, lt, gt, lte, gte, between, in, starts_with). Walks each leaf into a KeySet, intersects, **skips per-record fetch entirely for count**.
- `PRIMARY_KEYSET` — pure OR (every child indexed) → union into KeySet; pure-OR count returns `|KeySet|` directly.
- AND + OR hybrid → indexed AND leaf as primary, OR sub-tree as per-record post-filter.
- `PRIMARY_NONE` — full parallel shard scan (`scan_shards`).

Selectivity rank for intersect ordering: `eq < starts_with < between < in < range`. Cardinality estimation from index stats is future work. Substring/suffix ops (like, contains, ends, not_*) and large-set ops (neq, not_in) cannot drive intersection.

### Native TLS

Optional, off by default. `TLS_ENABLE=1` in db.env makes `PORT` TLS-only (single-port model). TLS 1.3 only. Client identity = tokens, not mTLS. SNI/verify defaults to `localhost`; override via `TLS_SERVER_NAME`. Cert hot-reload is not implemented (rotation = restart). Server refuses to start if cert/key missing/unreadable/mismatched. Reverse-proxy termination remains supported. Full config: [docs/getting-started/configuration.md](docs/getting-started/configuration.md).

### Single-instance guard

`cmd_server` takes `flock(LOCK_EX | LOCK_NB)` on `$DB_ROOT/.shard-db.lock` before daemonizing. Second `start` on the same DB_ROOT fails fast (different port / config / binary doesn't matter). Kernel releases on crash too — no manual cleanup. Lock file contains the PID for `lsof`/`cat`.

### Per-query memory cap

`QUERY_BUFFER_MB` (default 256) bounds intermediate buffers any single query can hold. 9 collection sites checked (ordered find buffer, aggregate buckets, bulk-delete/update key list, OR KeySet, `CollectCtx.entries`, `ShardWorkCtx.results`, per-worker agg hash tables, list-files names buffer, bitmap deferred candidate batches). Exceeded → query aborts with `{"error":"query memory buffer exceeded; ..."}` (the bitmap deferred collector instead stops collecting and returns the partial result, deadline-style); server keeps serving.

`MAX_CONCURRENT_QUERIES` (default 0 = auto = `max(4, min(nproc, 32))`) hard-caps simultaneously-running queries. Implemented via a sem_trywait semaphore in `dispatch_json_query` — every request takes a slot at entry and releases via `__attribute__((cleanup))` on any return path. Exceeded → immediate `{"error":"server at capacity"}`, client retries. Worst-case query-buffer RAM = `MAX_CONCURRENT_QUERIES × QUERY_BUFFER_MB` (predictable peak for sizing). Pair with whole-process containment (systemd MemoryMax, cgroup, ulimit -v) as a final backstop. See [docs/reference/limits.md](docs/reference/limits.md).

## Limits / constants

- `MAX_SPLITS = 4096` — max kf shards per object (3 hex digits in `data/kf/NNN.kf`).
- `DEFAULT_SPLITS = 8`, `MIN_SPLITS = 8` — `create-object` default. Sweet spot 78K–200K records/shard. Sizing table: [docs/operations/tuning.md](docs/operations/tuning.md).
- `MAX_KEY_CEILING = 1024` — hard ceiling on per-object `max_key`.
- `varchar` max content = **65535 bytes** (uint16 length prefix).
- `MAX_FIELDS = 256` (bumped from 64 in 2026.04.2).
- **Auto-key** (2026.05.5+): per-object opt-in flag at `create-object` (`"auto_key":"uuid"` or `"auto_key":"seq(<name>)"`). Server generates the key on insert when the client omits it; provided keys go through upsert (CAS modifiers respected). Stored as 16 bytes UUID binary or 8 bytes int64 BE; rendered as 36-char dashed / decimal string on every read. `update`/`delete` always require keys. Not retroactive — no schema mutation to add later. See [docs/query-protocol/schema-mutations.md → Auto-generated keys](docs/query-protocol/schema-mutations.md#auto-generated-keys).
- `MAX_AGG_SPECS = 32`.
- `MAX_CRITERIA_DEPTH = 16`, `MAX_INTERSECT_LEAVES = 8`.

Full reference: [docs/reference/limits.md](docs/reference/limits.md), [docs/reference/error-codes.md](docs/reference/error-codes.md), [docs/reference/changelog.md](docs/reference/changelog.md).
