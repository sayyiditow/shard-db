# CLAUDE.md

Guidance for Claude Code when working in this repository. User-facing docs live under `docs/`; this file is a fast index for me, not for users.

## Overview

shard-db is a high-performance file-based database in C. Single static binary, single process, no external dependencies. Typed binary records, B+ tree indexes, joins, aggregates, CAS, multi-threaded TCP server with optional native TLS 1.3. Linux x86_64 / ARM64; macOS port planned for 2026.05.2.

## Build & test

```bash
./build.sh                       # builds shard-db (daemon, ~324K), shard-cli (TUI, ~60K), migrate (one-shot upgrades) → build/bin/
./tests/<name>.sh                # one test; each starts/stops its own server, portable CWD
```

Test scripts (33 total, ~970 assertions; names are self-descriptive — `ls tests/` to enumerate):

```
test-objlock.sh                  test-cli-shortcuts.sh        test-bulk-cas.sh
test-rename-field.sh             test-or-logic.sh             test-schema-export.sh
test-remove-field.sh             test-csv-export.sh           test-stats-prom.sh
test-vacuum-addfield.sh          test-per-tenant-auth.sh      test-bulk-upsert.sh
test-parallel-index-integrity.sh test-token-perms.sh          test-bulk-update-json.sh
test-joins.sh                    test-request-timeout.sh      test-agg-neq-shortcut.sh
test-bulk-update-delimited.sh    test-binary-index.sh         test-length-ops.sh
test-find-cursor.sh              test-tls.sh                  test-case-sensitivity.sh
test-and-intersection.sh         test-describe.sh             test-list-files.sh
test-tenant-mgmt.sh              test-migrate-binary.sh       test-bare-shapes.sh
test-field-vs-field.sh           test-regex.sh                test-stress-no-hang.sh
```

Bench scripts live in `bench/`. **The user runs benches**; do not run them to validate perf.

## Source layout

### Daemon (`src/db/`)

- `types.h` — shared types, externs, function declarations
- `util.c` — JSON helpers, `b64_encode/decode`, `valid_filename`
- `config.c` — db.env, schema/index/dirs caches, typed-field encode/decode (`encode_field_len`)
- `storage.c` — xxh128 hashing, mmap, GET/INSERT/DELETE, CAS helpers, ucache, `build_idx_path`, `compute_addr`
- `index.c` — per-shard B+ tree wrappers (`btree_idx_*`), parallel indexing, `reindex_clean_legacy`
- `query.c` — find, count, aggregate, joins, bulk ops, planner (`choose_primary_source`), maintenance
- `server.c` — multi-threaded TCP server (epoll), JSON dispatch, auth, stats, optional TLS
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

- `db.env` — `DB_ROOT`, `PORT`, `TIMEOUT`, `LOG_*`, `THREADS`, `WORKERS`, `GLOBAL_LIMIT`, `MAX_REQUEST_SIZE`, `FCACHE_MAX`, `BT_CACHE_MAX = FCACHE_MAX/4`, `QUERY_BUFFER_MB`, `DISABLE_LOCALHOST_TRUST`, `TOKEN_CAP`, `SLOW_QUERY_MS`, TLS knobs (`TLS_ENABLE`, `TLS_CERT`, `TLS_KEY`, `TLS_CA`, `TLS_SKIP_VERIFY`, `TLS_SERVER_NAME`). Full reference: [docs/getting-started/configuration.md](docs/getting-started/configuration.md).
- `$DB_ROOT/tokens.conf` — global tokens. Line format `token[:perm]`, `perm ∈ {r, rw, rwx}`, no suffix = `rwx`.
- `$DB_ROOT/<dir>/tokens.conf` — per-tenant tokens (same format).
- `$DB_ROOT/<dir>/<obj>/tokens.conf` — per-object tokens (same format).
- `$DB_ROOT/allowed_ips.conf` — global trusted IPs (skip token check).
- `$DB_ROOT/dirs.conf` — allowed tenant directories.
- `$DB_ROOT/schema.conf` — `dir:object:splits:max_key:max_value:prealloc_mb`.
- `$DB_ROOT/<dir>/<obj>/fields.conf` — `name:type[:size|P,S][:default=...]`.

## Storage model (high-level)

- **Shard files**: `data/NNN.bin` (3 hex digits, max 4096 = `MAX_SPLITS`).
- **Slot header** (24B): 16B xxh128 hash, 2B flag, 2B key_len, 4B value_len.
- **Layout**: `[ShardHeader 32B][Zone A: slots × 24B headers][Zone B: slots × slot_size payloads]`.
- **Addressing**: `shard = hash[0..1] % splits`, `slot = hash[2..5] % slots_per_shard`, linear probing.
- **Dynamic growth**: 50% load → double `slots_per_shard`. `MAX_SPLITS=4096` caps shard *files*, not slots.
- **I/O**: mmap throughout — MAP_SHARED for writes (via ucache), MAP_PRIVATE for reads.
- **Crash safety**: write flag=0 → activate batch flag=1; recovery sweeps stale `*.new`/`*.old` on startup.
- **Concurrency**: per-ucache-entry rwlock; per-object rwlock for schema mutations; per-btree-file rwlock (`BT_CACHE_MAX`).
- **Index layout**: each indexed field shards into `splits/4` btree files at `<obj>/indexes/<field>/<NNN>.idx`. Writes route by hash16 to one shard; reads fan out across all shards in parallel; cursor pagination uses k-way streaming merge across `BtRangeIter`s. Routing: `idx_shard_for_hash(hash16, splits)`, `idx_shard_for_data_shard(s) = s/4`.

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
| `numeric:P,S` | 8 bytes BE int64 × 10^S |

Field defaults (in fields.conf): `:default=<literal>`, `:auto_create`, `:auto_update`, `:default=seq(<name>)`, `:default=uuid()`, `:default=random(N)`. Reference: [docs/concepts/typed-records.md](docs/concepts/typed-records.md).

## Indexes (high-level)

- B+ tree, prefix-compressed leaves (anchors every K=16, two-stage bsearch).
- Per-shard layout — every indexed field is `splits/4` btree files; `splits` ∈ powers of 2 in [8, 4096]; `index_splits_for(splits) = splits/4` is derived (no separate config).
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
./shard-db size | orphaned <dir> <obj>                          # bare integers (O(1) metadata)
./shard-db recount | truncate | vacuum | backup <dir> <obj>
./shard-db add-index <dir> <obj> <field> [-f]                   # field or field1+field2
./shard-db remove-index <dir> <obj> <field>
./shard-db reindex [dir] [obj]                                  # no args = all tenants

# Diagnostics
./shard-db stats | stats-prom | db-dirs | vacuum-check
./shard-db shard-stats <dir> <obj>

# JSON-only (advanced)
./shard-db query '{"mode":"create-object","dir":"...","object":"...","splits":N,"max_key":N,"fields":[...],"indexes":[...]}'
./shard-db query '{"mode":"list-objects","dir":"<dir>"}'
./shard-db query '{"mode":"describe-object","dir":"<dir>","object":"<obj>"}'

# Per-release one-shot upgrade — separate binary at build/bin/migrate. Run with daemon stopped; manages start/stop itself.
./migrate
```

**Bulk-insert at scale**: prefer FEWER, LARGER calls. Each request triggers a sequential `bulk_merge` per (field, shard); cumulative work scales O(R²) where R = request count. Sweet spot at 1M records ≈ 5 conns × 200K records.

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
| `add-field / remove-field / rename-field / vacuum / add-index / remove-index` | [schema-mutations.md](docs/query-protocol/schema-mutations.md) |
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
| `count`, `size`, `orphaned` | bare integer |
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

`QUERY_BUFFER_MB` (default 500) bounds intermediate buffers any single query can hold. 8 collection sites checked (ordered find buffer, aggregate buckets, bulk-delete/update key list, OR KeySet, `CollectCtx.entries`, `ShardWorkCtx.results`, per-worker agg hash tables, list-files names buffer). Exceeded → query aborts with `{"error":"query memory buffer exceeded; ..."}`; server keeps serving. Pair with whole-process containment (systemd MemoryMax, cgroup, ulimit -v). See [docs/reference/limits.md](docs/reference/limits.md).

## Limits / constants

- `MAX_SPLITS = 4096` — max shards per object (3 hex digits in `NNN.bin`).
- `DEFAULT_SPLITS = 8`, `MIN_SPLITS = 8` — `create-object` default. Sweet spot 78K–200K records/shard. Sizing table: [docs/operations/tuning.md](docs/operations/tuning.md).
- `MAX_KEY_CEILING = 1024` — hard ceiling on per-object `max_key`.
- `varchar` max content = **65535 bytes** (uint16 length prefix).
- `MAX_FIELDS = 256` (bumped from 64 in 2026.04.2).
- `MAX_AGG_SPECS = 32`.
- `MAX_CRITERIA_DEPTH = 16`, `MAX_INTERSECT_LEAVES = 8`.

Full reference: [docs/reference/limits.md](docs/reference/limits.md), [docs/reference/error-codes.md](docs/reference/error-codes.md), [docs/reference/changelog.md](docs/reference/changelog.md).
