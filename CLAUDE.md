# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

shard-db is a file-based database in C with a key/value foundation plus full query features (find, count, aggregate, joins, CAS). Inspired by chronicle-db. xxh128 hashing, mmap for reads and writes, typed binary records via fields.conf, linear probing, per-shard rwlock, multi-threaded TCP server, async logging, 17 search operators.

## Build & Test

```bash
./build.sh                          # compile + populate build/bin/db.env + build/db/schema.conf
# or: gcc -O2 -o shard-db src/*.c -Isrc -lpthread

# Tests — all in tests/ folder, start/stop server automatically, portable CWD
./tests/test-objlock.sh                   # Schema mutation locking + key ceiling (18)
./tests/test-rename-field.sh              # rename-field correctness              (24)
./tests/test-remove-field.sh              # remove-field + vacuum --compact       (35)
./tests/test-vacuum-addfield.sh           # vacuum + add-field                    (50)
./tests/test-parallel-index-integrity.sh  # Concurrent bulk-insert integrity      (23)
./tests/test-joins.sh                     # Join support                          (17)
./tests/test-cli-shortcuts.sh             # count/aggregate CLI + delete-file     (28)
./tests/test-or-logic.sh                  # OR criteria, all four shapes          (43)
./tests/test-csv-export.sh                # CSV on find/fetch/aggregate/get/keys/exists (37)
./tests/test-per-tenant-auth.sh           # Per-tenant token scoping              (27)
./tests/test-token-perms.sh               # Per-object tokens + r/rw/rwx perms    (37)
./tests/test-request-timeout.sh           # Per-request timeout_ms                (10)
./tests/test-bulk-update-delimited.sh     # CSV per-key partial update            (34)
./tests/test-binary-index.sh              # Binary-native B+ tree keys + reindex  (18)
# Total: 401 tests

# Benchmarks — all in bench/ folder
./bench/bench-queries.sh                  # find/count/aggregate on 1M users
./bench/bench-joins.sh [count]            # join throughput
./bench/bench-kv.sh / bench-kv-parallel.sh # bulk insert throughput
./bench/bench-invoice.sh / bench-parallel.sh # 14-index invoice scenario
```

## Architecture

### Source files (src/)

- **types.h** — Shared types, externs, function declarations
- **util.c** — Utilities, JSON helpers
- **config.c** — db.env, schema/index/dirs caches, typed-field encode/decode
- **storage.c** — Hashing, mmap, GET/INSERT/DELETE, CAS helpers, ucache
- **index.c** — B+ tree indexing, parallel indexing
- **query.c** — Find, count, aggregate, joins, bulk ops, maintenance
- **server.c** — Multi-threaded TCP server (epoll), JSON dispatch, auth (token + IP allowlist), stats. **Plaintext TCP only — no native TLS**; terminate TLS at a reverse proxy (nginx `stream` module recommended; HAProxy or stunnel also work).
- **main.c** — CLI entry point
- **btree.h / btree.c** — B+ tree index (page-based, prefix-compressed leaves, mmap'd)
- **objlock.c** — Per-object rwlock (normal ops share; vacuum/rebuild exclusive)

### Configuration

- **db.env** — Config: `DB_ROOT`, `PORT`, `TIMEOUT` (seconds, 0=off), `LOG_DIR`, `LOG_LEVEL`, `LOG_RETAIN_DAYS`, `INDEX_PAGE_SIZE`, `THREADS`, `WORKERS`, `GLOBAL_LIMIT`, `MAX_REQUEST_SIZE`, `FCACHE_MAX`, `BT_CACHE_MAX`, `QUERY_BUFFER_MB` (per-query intermediate buffer cap, default 500), `DISABLE_LOCALHOST_TRUST` (strict mode, default 0), `TOKEN_CAP` (token table bucket count, default 1024), `SLOW_QUERY_MS` (floor 100ms, 0=off)
- **$DB_ROOT/tokens.conf** — Global / admin API tokens (one per line). Line format: `token[:perm]` where `perm ∈ {r, rw, rwx}`; no suffix = `rwx` (admin, backward-compatible with pre-perm files).
- **$DB_ROOT/\<dir\>/tokens.conf** — Per-tenant tokens. Same line format. Scope covers any object within `<dir>`.
- **$DB_ROOT/\<dir\>/\<object\>/tokens.conf** — Per-object tokens. Same line format. Scope covers only that one object.
- **$DB_ROOT/allowed_ips.conf** — Trusted IPs (skip token check entirely; global only — no per-tenant IP lists)
- **$DB_ROOT/dirs.conf** — Allowed tenant directories
- **$DB_ROOT/schema.conf** — Per-object: `dir:object:splits:max_key:max_value:prealloc_mb`
- **$DB_ROOT/\<dir\>/\<object\>/fields.conf** — Typed field schema, one per line: `name:type[:size|P,S][:default=...]`

### Storage layout

- **Shard files**: `data/NNN.bin` (3 hex digits, supports 4096 shards max = `MAX_SPLITS`)
- **Slot header** (24 bytes): 16-byte xxh128 hash, 2-byte flag, 2-byte key_len, 4-byte value_len
- **Slot file layout**: `[ShardHeader: 32B][Zone A: slots × 24B headers][Zone B: slots × slot_size payloads]`
- **Addressing**: `shard = hash[0..1] % splits`, `slot = hash[2..5] % slots_per_shard`, linear probing
- **Dynamic shard growth**: 50% load → double `slots_per_shard` (no slot cap, grows as data grows). `MAX_SPLITS=4096` is the cap on the *number of shard files* per object (3 hex digits in `NNN.bin`), not on slots.
- **All I/O via mmap**: MAP_SHARED for writes (via ucache), MAP_PRIVATE for reads
- **Crash safety**: write flag=0 → activate batch flag=1; recovery sweeps stale `*.new`/`*.old` on startup
- **Concurrency**: per-ucache-entry rwlock (shared for read, exclusive for write); per-object rwlock for schema mutations
- **Multi-tenancy**: `dir` parameter in every data query, validated against dirs.conf
- **Logging**: Async ring buffer, separate info/error files by date, auto-retention

### Typed binary record format

Records are stored in a fixed-slot typed binary format driven by fields.conf.

- **varchar:N** — `[uint16 BE length][content]` → on-disk = N+2 bytes. Max content **65535 bytes**.
- **int/long/short** — 4/8/2 bytes big-endian signed
- **double** — 8 bytes IEEE 754
- **bool/byte** — 1 byte
- **date** — 4 bytes big-endian int32 (`yyyyMMdd`)
- **datetime** — 6 bytes (`yyyyMMdd` BE int32 + `HHmmss` packed BE uint16)
- **numeric:P,S** — 8 bytes big-endian int64 × 10^S (decimal fixed-point)

### Field defaults (in fields.conf)

- `:default=<literal>` — constant on INSERT
- `:auto_create` — server datetime on INSERT
- `:auto_update` — server datetime on INSERT and every UPDATE
- `:default=seq(<name>)` — call cmd_sequence, next value on INSERT
- `:default=uuid()` — UUID v4 on INSERT
- `:default=random(N)` — N random bytes hex on INSERT

### Indexes

- **B+ tree** with prefix-compressed leaves (anchors every K=16 entries, two-stage bsearch)
- Single field: `indexes:["name"]`
- Composite: `indexes:["country+zip"]` (concatenated field values)
- **All 17 search operators** use index when available: eq, neq, lt, gt, lte, gte, between, in, not_in, like, not_like, contains, not_contains, starts, ends, exists, not_exists

## Commands

```bash
# Lifecycle
./shard-db start                                  # Background TCP server
./shard-db stop                                   # Graceful shutdown (drains in-flight writes)
./shard-db status                                 # Running check
./shard-db server                                 # Foreground (debug)

# CRUD
./shard-db insert <dir> <obj> <key> <val>
./shard-db get <dir> <obj> <key>
./shard-db delete <dir> <obj> <key>
./shard-db exists <dir> <obj> <key>

# Query (CLI — for simple ad-hoc; full query shape via JSON)
./shard-db find <dir> <obj> '<criteria>' [off] [lim] [fields]
./shard-db count <dir> <obj> [criteria_json]      # empty criteria = O(1) metadata count
./shard-db aggregate <dir> <obj> <aggregates_json> [group_by_csv] [criteria_json] [having_json]
./shard-db keys <dir> <obj> [off] [lim]
./shard-db fetch <dir> <obj> [off] [lim] [fields]

# Bulk
./shard-db bulk-insert <dir> <obj> [file]         # JSON: [{"id":"k","data":{...}},...]
./shard-db bulk-delete <dir> <obj> [file]

# Files (base64-in-JSON over TCP — remote-safe)
./shard-db put-file <dir> <obj> <local-path> [--if-not-exists]
./shard-db get-file <dir> <obj> <filename> [<out-path>]
./shard-db delete-file <dir> <obj> <filename>

# Maintenance
./shard-db size|recount|truncate|vacuum|backup <dir> <obj>
./shard-db add-index <dir> <obj> <field> [-f]     # field or field1+field2
./shard-db remove-index <dir> <obj> <field>       # drop index (exact name match)
./shard-db reindex [dir] [obj]                    # rebuild indexes; no args = all tenants
./shard-db query '{"mode":"create-object","dir":"...","object":"...","splits":N,"max_key":N,"fields":[...],"indexes":[...]}'

# Diagnostics
./shard-db stats                                  # Global (connections, in-flight, cache hit, slow log)
./shard-db shard-stats <dir> <obj>                # Per-shard load table
./shard-db db-dirs                                # List allowed tenant dirs
./shard-db vacuum-check                           # Objects needing vacuum
```

## JSON query protocol

All advanced queries go through `./shard-db query '<json>'`.

### Find / Count / Fetch

```json
{"mode":"find","dir":"t","object":"o",
 "criteria":[{"field":"age","op":"gt","value":"30"}],
 "offset":0,"limit":100,
 "fields":["id","name"],
 "format":"rows"}           // optional: "rows" = tabular {"columns":[...],"rows":[[...]]}
```

### Auth: scope + permissions

Tokens have two independent dimensions: **scope** (what they can touch) and **permission** (what they can do).

**Scope** is determined by which tokens.conf file the token lives in:

| Scope | File | Covers |
|---|---|---|
| global | `$DB_ROOT/tokens.conf` | any `dir`, any object |
| tenant | `$DB_ROOT/<dir>/tokens.conf` | any object within `<dir>` |
| object | `$DB_ROOT/<dir>/<obj>/tokens.conf` | only `<dir>/<obj>` |

**Permission** is the suffix on the token line:

| Perm | Reads | Writes | Admin |
|---|---|---|---|
| `r` | ✓ | ✗ | ✗ |
| `rw` | ✓ | ✓ | ✗ |
| `rwx` | ✓ | ✓ | scope-dependent |
| _(no suffix)_ | same as `rwx` | backward-compat for pre-2026.05 tokens.conf | |

Admin commands themselves have scope. A token needs `rwx` AND scope at least as broad as the command's admin scope:

| Command | Admin scope | Who can run it |
|---|---|---|
| `stats`, `db-dirs`, `vacuum-check`, `shard-stats`, `add-ip`/`remove-ip`/`list-ips`, `add-token`/`remove-token`/`list-tokens` | server | global `rwx` or trusted IP only |
| `create-object` | tenant | global-`rwx` or tenant-`rwx` on that dir |
| `truncate`, `vacuum`, `backup`, `recount`, `add-field`, `remove-field`, `rename-field`, `add-index`, `remove-index` | object | any `rwx` whose scope covers that object |

**Precedence on each request**:
1. Trusted IP (global `allowed_ips.conf`) → bypass.
2. Token match with sufficient scope + perm → allow.
3. Otherwise `{"error":"auth failed"}`.

Token management (`add-token`/`remove-token`/`list-tokens`) is **always** server-admin regardless of what scope/perm the token being managed has. Tenant admins and object admins cannot issue new tokens — the platform operator issues all credentials.

Localhost (127.0.0.1/::1) is trusted by default (typical deployment: loopback-connecting proxy). Set `DISABLE_LOCALHOST_TRUST=1` to require tokens from same-host callers too (strict mode).

`add-token` accepts optional `"dir"`, `"object"`, and `"perm"` fields. Default perm is `rw` (principle of least privilege — admins opt into `rwx` explicitly). Invalid perms (`x`, `rx`, etc.) are rejected. `object` scope requires `dir`.

`list-tokens` returns `{"token":"fingerprint","scope":"global"|"<dir>"|"<dir>/<obj>","perm":"r|rw|rwx"}` per entry. Full tokens are never echoed.

Token storage: open-addressed hash table sized by `TOKEN_CAP` (default 1024 buckets). Lookup O(1), lock-free reads. Bump `TOKEN_CAP` in db.env if you expect more than ~700 tokens in total across all scopes.

### Single-instance guard

`cmd_server` takes `flock(LOCK_EX | LOCK_NB)` on `$DB_ROOT/.shard-db.lock` before daemonizing. A second `./shard-db start` on the same `DB_ROOT` — even with a different port, a different config file, or a copied binary — fails fast with a clear error. The kernel holds the lock for the server's lifetime and releases it automatically on normal exit *or crash* (SIGKILL, power loss), so there's nothing to clean up manually. The lock file contains the running daemon's PID for `lsof`/`cat` visibility.

### Per-request statement timeout

Any query can carry a `"timeout_ms":N` field to override the global `TIMEOUT` for that single request. Applies to `find`, `count`, `aggregate`, `bulk-delete`, `bulk-update`. Value is a hard cap in milliseconds; when exceeded, the query aborts with `{"error":"query_timeout"}` and the server keeps serving.

- `"timeout_ms":0` or absent → falls back to the global `TIMEOUT` (unchanged behaviour).
- Thread-local override — a tight timeout on one request doesn't leak to the next request on the same worker thread.
- Zero perf cost: the existing `QueryDeadline` mechanism reads the override from a thread-local at construction time, no extra cost per iteration.

Use it to give specific callers tighter deadlines (e.g., `"timeout_ms":200` for an interactive dashboard) without reconfiguring the server.

### Per-query memory cap

`QUERY_BUFFER_MB` (default 500) bounds the intermediate buffers any single query can hold. Checked at 7 collection sites: ordered find buffer, aggregate buckets (shared atomic across parallel workers), bulk-delete/update key list, OR KeySet, `CollectCtx.entries` (btree hash collection), `ShardWorkCtx.results` (downstream of CollectCtx), per-worker aggregate hash tables (via the shared atomic). When exceeded the query aborts with `{"error":"query memory buffer exceeded; narrow criteria, add limit/offset, or stream via fetch+cursor"}` and the server keeps serving.

Rough sizing: peak RAM per query ≈ `QUERY_BUFFER_MB × 1` (true RSS ~10-15% higher due to malloc chunk overhead not accounted for). Multiply by expected concurrent heavy queries when sizing the host. Pair with whole-process containment (systemd `MemoryMax=`, cgroup `memory.max`, container limit, or `ulimit -v`) as a backstop.

### CSV / delimited export

`"format":"csv"` on `find`, `fetch`, `aggregate`, `get` (multi-key), `keys`, and `exists` (multi-key) emits **raw CSV text** (not JSON-wrapped). Optional `delimiter` field picks a single-char separator — defaults to `,`, accepts `\t` literal for tab.

```json
{"mode":"find","dir":"t","object":"o","criteria":[],"format":"csv","delimiter":"|"}
```

Body:
```
key|status|amount|note
o1|paid|100|vip
o2|paid|50|"a,comma,here"
```

- First row = column names.
- Newlines (`\n`/`\r`) inside values → replaced with a space so one physical line = one logical row.
- Values containing the delimiter or `"` are wrapped in `"` with internal `"` doubled (RFC 4180 minus multiline).
- NULL fields → empty cell.
- Errors still come as JSON — content type is unified only on success.
- `csv + join` is rejected (`format=csv is not supported with join`).

### OR criteria

`criteria` is implicit AND. Introduce an OR branch with `{"or":[...]}` (or explicit sub-AND via `{"and":[...]}`). Supported in find / count / aggregate `criteria` and `having`, and in bulk-delete / bulk-update.

```json
"criteria":[
  {"field":"status","op":"eq","value":"paid"},
  {"or":[
    {"field":"region","op":"eq","value":"us"},
    {"field":"total","op":"gte","value":"1000"}
  ]}
]
```

Planner picks one of four paths automatically:
- **Pure AND** → primary-indexed-leaf scan (today's path).
- **AND + OR, indexed AND sibling** → indexed leaf drives candidates, OR sub-tree evaluated per record via tree match.
- **Pure OR, every child indexed** → per-child B+ tree lookups unioned into a concurrent KeySet (xxh128 hashes, lock-free CAS inserts); pure-OR count returns `|KeySet|` directly without fetching records.
- **OR with non-indexed child** → full parallel shard scan, tree match per record.

Hybrid (non-indexed AND + fully-indexed OR) uses KeySet as primary-candidate source and applies the AND siblings as a post-filter. Max nesting depth is `MAX_CRITERIA_DEPTH = 16`. Empty `or:[]` / `and:[]` → `{"error":"empty or/and"}`.

### Aggregation

```json
{"mode":"aggregate","dir":"t","object":"o",
 "criteria":[...],
 "group_by":["status"],
 "aggregates":[{"fn":"count","alias":"n"},
               {"fn":"sum","field":"amount","alias":"total"},
               {"fn":"avg","field":"score","alias":"avg_score"},
               {"fn":"min","field":"rank","alias":"best"},
               {"fn":"max","field":"rank","alias":"worst"}],
 "having":[{"field":"n","op":"gte","value":"10"}],
 "order_by":"total","order_desc":true,"limit":5}
```

### Joins (read-only, tabular output only)

```json
{"mode":"find","dir":"t","object":"orders",
 "criteria":[{"field":"status","op":"eq","value":"paid"}],
 "join":[
   {"object":"users","local":"user_id","remote":"key",
    "as":"user","type":"inner","fields":["email","name"]},
   {"object":"products","local":"product_sku","remote":"sku",
    "as":"product","type":"left","fields":["title"]}
 ],
 "limit":50}
```

Output is always tabular when `join` is present. Columns: `{driver}.key`, `{driver}.{field}`, `{as}.{field}`. Left-join no-match emits nulls for that join's columns. `remote` must be either `"key"` (primary-key lookup) or an indexed field.

### CAS (conditional writes)

- `{"mode":"insert", ..., "if_not_exists":true}` — idempotent insert, returns error if key exists
- `{"mode":"update", ..., "if":{"status":"pending"}}` — update only if condition matches
- `{"mode":"delete", ..., "if":{"version":42}}` — delete only if condition matches
- `{"mode":"bulk-update", "criteria":[...], "value":{...}, "limit":N, "dry_run":true}` — conditional mass update
- `{"mode":"bulk-delete", "criteria":[...], "limit":N, "dry_run":true}` — mass delete by criteria

### File storage

Files live at `$DB_ROOT/<dir>/<obj>/files/XX/XX/<filename>`, hash-bucketed by filename. Filenames are validated (no `/`, `\`, `..`, control chars, ≤255 bytes).

Remote-safe (base64 in JSON):
- `{"mode":"put-file","dir":"...","object":"...","filename":"...","data":"<b64>","if_not_exists":true}` — atomic `.tmp`+`fsync`+`rename`. `if_not_exists` is optional CAS.
- `{"mode":"get-file","dir":"...","object":"...","filename":"..."}` — returns `{"status":"ok","bytes":N,"data":"<b64>"}`.
- `{"mode":"delete-file","dir":"...","object":"...","filename":"..."}` — returns `{"status":"deleted","filename":"..."}` or `{"error":"file not found","filename":"..."}`.

Server-local zero-copy (same-host callers only — admin fast path):
- `{"mode":"put-file","dir":"...","object":"...","path":"/srv/file.pdf"}` — server reads the path directly.
- `{"mode":"get-file-path","dir":"...","object":"...","filename":"..."}` — returns `{"path":"..."}` as a string; no bytes on the wire.

Size ceiling = `MAX_REQUEST_SIZE` (default 32 MB ⇒ ~24 MB effective after base64 inflation). Raise `MAX_REQUEST_SIZE` in db.env to lift it; every connection allocates a read buffer this size.

### Schema mutations

- `{"mode":"rename-field","old":"X","new":"Y"}` — metadata-only, preserves data
- `{"mode":"remove-field","fields":["a","b"]}` — tombstone (space reclaimed on vacuum --compact)
- `{"mode":"add-field","fields":["age:int","email:varchar:40"]}` — append fields, triggers rebuild
- `{"mode":"vacuum","compact":true}` — drop tombstoned fields, shrink slot_size
- `{"mode":"vacuum","splits":N}` — reshard (indexes survive; hash routing preserved)

## Key internals

- `CriteriaNode` (types.h) + `parse_criteria_tree` / `criteria_match_tree` / `compile_criteria_tree` (query.c): AND/OR tree form of criteria. Leaves hold a pre-compiled `CompiledCriterion` so the hot path stays zero-malloc-per-record
- `KeySet` (keyset.c): lock-free open-addressed hash table of 16-byte xxh128 keys; lives on the OR index-union fast path
- `choose_primary_source` (query.c): planner that picks `PRIMARY_LEAF` / `PRIMARY_KEYSET` / `PRIMARY_NONE` based on indexability of the tree's leaves
- `match_typed()` / `CompiledCriterion` (query.c): typed-binary criterion matching — zero malloc per record, direct byte compares
- `scan_shards()`: parallel mmap-based shard scanner (one thread per shard group)
- `index_parallel()`: spawns pthread per index field during bulk insert
- `shard_find_worker` / `shard_count_worker` / `shard_agg_worker`: per-shard parallel workers for indexed find/count/agg
- `parallel_indexed_count` / `parallel_indexed_agg`: orchestrators for indexed multi-criteria
- `QueryDeadline` + `query_deadline_tick()`: statement-timeout enforcement (coarse clock, every 1024 iterations)
- `idx_count_cb`: single-criterion inline count (no record fetch, O(1) per btree hit)
- `btree_insert/search/range/bulk_build/bulk_merge`: B+ tree ops with prefix-compressed leaves
- `ucache`: unified shard mmap cache (FCACHE_MAX entries, per-entry rwlock, LRU eviction)
- `typed_encode/decode/typed_get_field_str`: typed binary encode/decode with length-prefix varchar
- `encode_field_len` (config.c): length-based encoder used by the CSV bulk-insert path to parse directly against an mmap'd page cache with (ptr, len) spans — no per-line memcpy. `encode_field()` is a thin `encode_field_len(f, val, strlen(val), out)` wrapper for null-terminated callers (JSON bulk-insert, typed_encode, cmd_update, etc.)
- `b64_encode/decode` (util.c): RFC 4648 base64, whitespace-tolerant on decode; used by `cmd_put_file_b64`/`cmd_get_file_b64`
- `valid_filename` (util.c): basename sanitizer (no `/`, `\`, `..`, control chars, ≤255 bytes) — enforced on every remote upload/download
- `cmd_put_file_tcp`/`cmd_get_file_tcp` (server.c): client-side helpers invoked by CLI; `query_collect` accumulates a full response buffer before parsing
- `compute_addr` / `addr_from_hash`: xxh128 hash → shard_id/slot
- `build_shard_filename(dir, shard_id)`: canonical `NNN.bin` formatter (3 hex, MAX_SPLITS=4096)
- `g_schema_cache` / `g_idx_cache` / `g_typed_cache`: in-memory caches for config files
- `is_valid_dir()`: tenant directory whitelist enforcement

## Limits / constants

- `MAX_SPLITS = 4096` — max shards per object (3 hex digits in `NNN.bin`)
- `MAX_KEY_CEILING = 1024` — hard ceiling on per-object `max_key`; uint16 `SlotHeader.key_len` allows 65535 but every slot reserves `max_key` bytes, so large caps bloat `slot_size`. Keys are stored raw in Zone B, length lives in Zone A header (no in-payload prefix).
- `varchar` max content = **65535 bytes** (uint16 length prefix)
- `MAX_FIELDS = 256` fields per schema (bumped from 64 in 2026.04.2)
- `MAX_AGG_SPECS = 32` aggregates per query
