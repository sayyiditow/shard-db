# Limits

Hard caps and practical bounds. Everything here is enforced at compile time unless noted.

## Hard-coded constants (`src/db/types.h`)

| Constant | Value | Meaning |
|---|---|---|
| `MIN_SPLITS` | 8 | Minimum kf-shards per object at `create-object`. At the floor, `index_splits_for(8) = 2` per-field idx shards — preserves k-way-merge parallelism on indexed reads while giving small-server (2–4 core) deployments a tighter sizing option for sub-1M-row objects. |
| `DEFAULT_SPLITS` | 8 | Used by `create-object` when `splits` is omitted or 0. Tuned for the common sub-1M-row case; specify `splits` explicitly for larger workloads. |
| `MAX_SPLITS` | 4096 | Maximum shards per object. Filename is `NNN.kf` (3 hex digits), so this is structural — not a policy knob. |
| Allowed `splits` set | `{8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}` | `splits` must be a power of 2 in this set. Other values are rejected at `create-object` and `vacuum --splits`. |
| `MAX_KEY_CEILING` | 1024 bytes | Hard upper limit on per-object `max_key`. Keys are stored raw inside each segment record (length-prefixed by `klen` in the 24B seg header). |
| `MAX_FIELDS` | 256 | Max fields per typed schema (bumped from 64 in 2026.04.2). |
| `MAX_LINE` | 65 536 bytes | Internal buffer size for queries and intermediate strings. |
| `MAX_AGG_SPECS` | 32 | Max aggregate specs in one `aggregate` query. |
| `MAX_CRITERIA_DEPTH` | 16 | Maximum nesting depth of AND/OR criteria trees. |
| `MAX_INTERSECT_LEAVES` | 8 | Maximum number of indexed AND-leaves the planner will intersect via `PRIMARY_INTERSECT`. Trees with more eligible leaves fall back to `PRIMARY_LEAF`. |
| `SLOTCASK_MAX_SLOTS_PER_SHARD` | 16 777 216 (16M) | Per-kf-shard slot ceiling. Resplits stop here; further inserts to a full shard refuse and require `vacuum --splits=N` to widen the keyspace. |
| `SLOTCASK_MAX_STREAMS` | 16 | Maximum streams per object (segment-write lanes). Actual count picked from nproc at `create-object` time. |
| `SLOTCASK_SEG_MAX_BYTES` | 128 MB | Segment file rotation point. |
| kf resplit trigger | 75 % (inline) / 50 % (pre-grow) | Shard doubles in place when `header.total × 4 ≥ capacity × 3` (75 %, inline path). Bulk-insert pre-grow triggers at 50 % (`count × 2 ≥ slots`). |
| `SLOW_QUERY_RING` | 64 | Recent-slow-queries ring buffer size (memory-resident). |
| `DIRS_BUCKETS` | 2048 | Hash bucket count for the in-memory `dirs.conf` cache. |

Changing these requires recompiling. Most don't need to change.

## Record-level limits

| Thing | Limit | Notes |
|---|---|---|
| Key length | `max_key` bytes (per-object, max 1024) | Set at `create-object`. Keys longer than this error on insert. |
| varchar content | 65 535 bytes | uint16 length prefix. |
| Total record payload | derived | Sum of typed-field sizes (`value_size` in `describe-object`). Not user-configurable. Stored in segment files; the 24-byte seg header + `klen` bytes of key precede it. |
| Per-kf-shard live records (default tier) | 12.8M | Resplit ceiling × 80 % load. A `splits=4096` object can hold tens of billions of live records before any single shard hits the ceiling. |
| Fields per object | 256 | `MAX_FIELDS`. Includes tombstoned fields until compact. |

## Query-level limits

| Thing | Limit | Notes |
|---|---|---|
| `limit` per query | `GLOBAL_LIMIT` (default 100 000) | Default applied when `limit` is omitted or ≤ 0. Per-query `limit` overrides without clamp — there is no server-side ceiling. |
| Aggregates per query | 32 | `MAX_AGG_SPECS`. |
| `criteria` length | bounded by `MAX_REQUEST_SIZE` | No hard count cap, just the request-size ceiling. |
| AND/OR criteria depth | 16 | `MAX_CRITERIA_DEPTH`. Empty `or:[]` / `and:[]` rejected. |
| AND-intersection leaves | 8 | `MAX_INTERSECT_LEAVES` — beyond this the planner picks single-leaf primary. |
| `join` chain length | no hard cap | Each join adds a per-record lookup; keep it short. |
| Composite index arity | 16 fields | Hard-coded in `IndexScanCtx`. |
| Per-query intermediate buffers | `QUERY_BUFFER_MB` MB (default 256) | Enforced at every collection site (collect-hash, KeySet, aggregate buckets, sort buffers, trigram intersect). Exceeded → `query memory buffer exceeded` error and the server keeps serving. |
| Concurrent queries in-flight | `MAX_CONCURRENT_QUERIES` (default auto = `max(4, min(nproc, 32))`) | Hard cap on simultaneously-running queries. Exceeded → immediate `{"error":"server at capacity"}` response so the client can retry without holding the TCP thread. `MAX_CONCURRENT_QUERIES × QUERY_BUFFER_MB` is the predictable peak for query-buffer RAM. |
| Per-request statement timeout | global `TIMEOUT` (db.env, seconds) or per-request `timeout_ms` (override) | Disabled when both are 0. |
| Request size (one JSON line) | `MAX_REQUEST_SIZE` (default 32 MB) | Configurable per install. |
| Response size | no cap | Bounded by `QUERY_BUFFER_MB` (intermediate buffers) + the caller-supplied `limit`. |

## File storage limits

| Thing | Limit | Notes |
|---|---|---|
| Filename | 255 bytes, plain basename | No `/`, `\`, `..`, control chars. |
| File size (base64 variant) | ~24 MB at default config | Bounded by `MAX_REQUEST_SIZE / 1.33`. Raise `MAX_REQUEST_SIZE` to lift. |
| File size (server-local variant) | unbounded | Limited only by disk. |
| `list-files` walk cost | O(file count) | Single `opendir(<obj>/files)` + `readdir` loop; comfortable up to several million files on ext4/XFS. |

## Concurrency limits

| Thing | Default | Notes |
|---|---|---|
| `THREADS` (parallel-pool workers) | 0 → `4 × nproc`, min 4 | Drives every parallel hot path: shard scans, parallel index builds, indexed find/count/aggregate fan-out, bulk-insert phase 2. |
| `WORKERS` (server thread pool) | 0 → `nproc`, min 4 | Accepts connections + dispatches request handlers. |
| `POOL_CHUNK` (parallel_for chunk) | 0 → `nproc` | Submission chunk size; rarely needs tuning. |
| `FCACHE_MAX` (`kfcache` and `segcache`, entries per cache) | 4096 | Strict allow-list `{4096, 8192, 12288, 16384}`. |
| `BT_CACHE_MAX` (btree mmap cache) | derived | `FCACHE_MAX / 4` since 2026.05.1; not separately configurable. |
| In-flight writes drain timeout | 30 s | `./shard-db stop` gives writes this long to finish. |
| Per-connection read buffer | `MAX_REQUEST_SIZE` | Allocated on first request, persists for connection lifetime. |
| Single-instance guard | flock on `$DB_ROOT/.shard-db.lock` | Two daemons cannot share a `DB_ROOT`, even with different ports. |

## Storage limits

| Thing | Limit | Notes |
|---|---|---|
| kf shards per object | 4096 (`MAX_SPLITS`) | Filename format (`NNN.kf`). Typical sweet spot is 78K–200K records/kf-shard at create time — see [Tuning → Sizing splits (v2 / slotcask)](../operations/tuning.md#sizing-splits-v2--slotcask). |
| Per-field idx-shards | `index_splits_for(splits)` | Non-linear curve in `src/db/types.h`: `8→2, 16→4, 32→4, 64→8, 128→16, 256→16, 512→32, 1024→64, 2048→64, 4096→128`. Reads fan out across all idx-shards in parallel; writes route by record hash to one. |
| Streams per object | `min(nproc, 16)` | Append-write lanes. Fixed at `create-object` time, immutable for the object's life unless `vacuum` self-heals a streams-mismatch. |
| Segment files per stream | filesystem-limited | Rotate at 128 MB. Direction-C vacuum pair-merges sparse non-active segs to reclaim space. |
| Objects per tenant (`<dir>`) | filesystem-limited | No internal cap. |
| Tenants (`dirs.conf`) | filesystem-limited | Validated via O(1) hash. |
| Indexes per object | no hard cap | Each is a directory of `index_splits_for(splits)` B+ tree files. The caches cap *hot* mappings independently, not on-disk count: `kfcache` and `segcache` each use `FCACHE_MAX`; `bt_cache` and `bm_cache` each use `FCACHE_MAX / 4`. |
| Tenant name length | 64 bytes | Validated by `add-dir` (rejects `/`, `\`, `..`, control chars). |
| `edit-field` scope | same-type only | Allowed transforms: varchar grow/shrink, integer family widen/narrow (`short ↔ int ↔ long`), `numeric` scale change, `float → double` widen. Pre-flight refuses any record that wouldn't fit the new shape. Cross-type changes refused with a hint to use `add-field + remove-field + bulk-update`. Holds `objlock_wrlock` for the rebuild duration. See [schema-mutations → edit-field](../query-protocol/schema-mutations.md#edit-field). |
| `auto_key` mode | `uuid` requires `max_key>=16`; `seq(<name>)` requires `max_key>=8` | Per-object opt-in at create-object; immutable thereafter. UUID stored as 16-byte binary, seq as 8-byte int64 BE. Rendered as dashed UUID / decimal on every read. See [schema-mutations → Auto-generated keys](../query-protocol/schema-mutations.md#auto-generated-keys). |

## Auth limits

| Thing | Default | Notes |
|---|---|---|
| `TOKEN_CAP` (token table buckets) | 1024 | Open-addressed hash. Bump to 4096+ at thousands of tokens across scopes. |
| Token scopes | global / per-tenant / per-object | Determined by which `tokens.conf` file the line lives in. |
| Token permissions | `r` / `rw` / `rwx` | Suffix on the token line. No suffix = `rwx` (backward-compat). |

## Not supported

- **Transactions across records.** Use [CAS](../query-protocol/cas.md) per record (including bulk forms).
- **Cross-dir joins.** Same-tenant only.
- **Right / full outer joins.** Left and inner only.
- **Nested aggregates, window functions.** Post-process in the app.
- **Streaming binary file uploads.** Files ride inside JSON as base64 today; size capped by `MAX_REQUEST_SIZE`. A length-prefixed binary body is on the backlog for very-large uploads.
- **Native replication.** Use block-level (DRBD) or filesystem-level (ZFS send/recv).
- **Platforms other than Linux + macOS.** Linux x86_64 / ARM64 and macOS Apple Silicon are first-class. Windows requires Docker/WSL2.

## What's already shipped

Native TLS 1.3, OR criteria, AND index intersection, cursor pagination, per-tenant + per-object tokens with `r`/`rw`/`rwx`, `delete-file` / `list-files`, `stats-prom`, `MAX_FIELDS` bumped to 256, per-shard btree layout, `bulk-insert` true upsert + chunk-size guidance, per-request `timeout_ms`, `QUERY_BUFFER_MB`, **slotcask v2 engine** (keyfile + append-only segments, snake-game pool, kf auto-resplit, Direction-C compaction), **macOS Apple Silicon support**, **b+ tree v3** with O(1)-step DESC iteration, query-perf fast paths (leaf-only walker, streaming k-way merge for varchar group_by) — all landed in 2026.05+. See the [changelog](changelog.md) for the full list.
