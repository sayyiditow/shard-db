# Limits

Hard caps and practical bounds. Everything here is enforced at compile time unless noted.

## Hard-coded constants (`src/db/types.h`)

| Constant | Value | Meaning |
|---|---|---|
| `MIN_SPLITS` | 8 | Minimum shards per object at `create-object`. At the floor, `splits/4 = 2` per-field idx shards — preserves k-way-merge parallelism on indexed reads while giving small-server (2–4 core) deployments a tighter sizing option for sub-1M-row objects. |
| `DEFAULT_SPLITS` | 16 | Used by `create-object` when `splits` is omitted or 0. |
| `MAX_SPLITS` | 4096 | Maximum shards per object. Filename is `NNN.bin` (3 hex digits), so this is structural — not a policy knob. |
| Allowed `splits` set | `{8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}` | `splits` must be a power of 2 in this set. Other values are rejected at `create-object` and `vacuum --splits`. |
| `MAX_KEY_CEILING` | 1024 bytes | Hard upper limit on per-object `max_key`. Keys are stored raw in Zone B, so every slot reserves `max_key` bytes — larger caps bloat `slot_size`. |
| `MAX_FIELDS` | 256 | Max fields per typed schema (bumped from 64 in 2026.04.2). |
| `MAX_LINE` | 65 536 bytes | Internal buffer size for queries and intermediate strings. |
| `MAX_AGG_SPECS` | 32 | Max aggregate specs in one `aggregate` query. |
| `MAX_CRITERIA_DEPTH` | 16 | Maximum nesting depth of AND/OR criteria trees. |
| `MAX_INTERSECT_LEAVES` | 8 | Maximum number of indexed AND-leaves the planner will intersect via `PRIMARY_INTERSECT`. Trees with more eligible leaves fall back to `PRIMARY_LEAF`. |
| `INITIAL_SLOTS` | 256 | Starting `slots_per_shard` for new shards. |
| `GROW_LOAD_NUM` / `GROW_LOAD_DEN` | 1 / 2 | Growth threshold: 50% load factor. |
| `SLOW_QUERY_RING` | 64 | Recent-slow-queries ring buffer size (memory-resident). |
| `DIRS_BUCKETS` | 2048 | Hash bucket count for the in-memory `dirs.conf` cache. |

Changing these requires recompiling. Most don't need to change.

## Record-level limits

| Thing | Limit | Notes |
|---|---|---|
| Key length | `max_key` bytes (per-object, max 1024) | Set at `create-object`. Keys longer than this error on insert. |
| varchar content | 65 535 bytes | uint16 length prefix. |
| Total record size (Zone B) | `max_value` bytes | Always computed as the sum of typed-field sizes at `create-object`; not user-configurable. Stored in `schema.conf` for persistence. |
| Fields per object | 256 | `MAX_FIELDS`. Includes tombstoned fields until compact. |

## Query-level limits

| Thing | Limit | Notes |
|---|---|---|
| `limit` per query | `GLOBAL_LIMIT` (default 100 000) | Soft cap. Per-query `limit` overrides. |
| Aggregates per query | 32 | `MAX_AGG_SPECS`. |
| `criteria` length | bounded by `MAX_REQUEST_SIZE` | No hard count cap, just the request-size ceiling. |
| AND/OR criteria depth | 16 | `MAX_CRITERIA_DEPTH`. Empty `or:[]` / `and:[]` rejected. |
| AND-intersection leaves | 8 | `MAX_INTERSECT_LEAVES` — beyond this the planner picks single-leaf primary. |
| `join` chain length | no hard cap | Each join adds a per-record lookup; keep it short. |
| Composite index arity | 16 fields | Hard-coded in `IndexScanCtx`. |
| Per-query intermediate buffers | `QUERY_BUFFER_MB` MB (default 500) | Enforced at every collection site (collect-hash, KeySet, aggregate buckets, sort buffers). Exceeded → `query memory buffer exceeded` error and the server keeps serving. |
| Per-request statement timeout | global `TIMEOUT` (db.env, seconds) or per-request `timeout_ms` (override) | Disabled when both are 0. |
| Request size (one JSON line) | `MAX_REQUEST_SIZE` (default 32 MB) | Configurable per install. |
| Response size | no cap | Bounded by memory + `GLOBAL_LIMIT`. |

## File storage limits

| Thing | Limit | Notes |
|---|---|---|
| Filename | 255 bytes, plain basename | No `/`, `\`, `..`, control chars. |
| File size (base64 variant) | ~24 MB at default config | Bounded by `MAX_REQUEST_SIZE / 1.33`. Raise `MAX_REQUEST_SIZE` to lift. |
| File size (server-local variant) | unbounded | Limited only by disk. |
| `list-files` walk cost | O(file count) | Walks the `XX/XX` bucket tree; comfortable up to ~1M files. |

## Concurrency limits

| Thing | Default | Notes |
|---|---|---|
| `THREADS` (parallel-pool workers) | 0 → `4 × nproc`, min 4 | Drives every parallel hot path: shard scans, parallel index builds, indexed find/count/aggregate fan-out, bulk-insert phase 2. |
| `WORKERS` (server thread pool) | 0 → `nproc`, min 4 | Accepts connections + dispatches request handlers. |
| `POOL_CHUNK` (parallel_for chunk) | 0 → `nproc` | Submission chunk size; rarely needs tuning. |
| `FCACHE_MAX` (shard mmap cache) | 4096 | Strict allow-list `{4096, 8192, 12288, 16384}`. |
| `BT_CACHE_MAX` (btree mmap cache) | derived | `FCACHE_MAX / 4` since 2026.05.1; not separately configurable. |
| In-flight writes drain timeout | 30 s | `./shard-db stop` gives writes this long to finish. |
| Per-connection read buffer | `MAX_REQUEST_SIZE` | Allocated on first request, persists for connection lifetime. |
| Single-instance guard | flock on `$DB_ROOT/.shard-db.lock` | Two daemons cannot share a `DB_ROOT`, even with different ports. |

## Storage limits

| Thing | Limit | Notes |
|---|---|---|
| Shards per object | 4096 (`MAX_SPLITS`) | Filename format. Typical sweet spot is 78K–200K records/shard — see [Tuning → Sizing splits](../operations/tuning.md#sizing-splits). |
| Per-field idx-shards | `splits / 4` | 2026.05.1+. Reads fan out across all shards in parallel; writes route by record hash to one. |
| Objects per tenant (`<dir>`) | filesystem-limited | No internal cap. |
| Tenants (`dirs.conf`) | filesystem-limited | Validated via O(1) hash. |
| Indexes per object | no hard cap | Each is a directory of `splits/4` B+ tree files. Both caches (`ucache` + `bt_cache`) cap *hot* mappings, not on-disk count. |
| Tenant name length | 64 bytes | Validated by `add-dir` (rejects `/`, `\`, `..`, control chars). |

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
- **Platforms other than Linux.** Uses `epoll`; the macOS port is on the near-term backlog (2026.05.2). Containers cover the cross-platform case meanwhile.

## What's already shipped

Native TLS 1.3, OR criteria, AND index intersection, cursor pagination, per-tenant + per-object tokens with `r`/`rw`/`rwx`, `delete-file` / `list-files`, `stats-prom`, `MAX_FIELDS` bumped to 256, per-shard btree layout, `bulk-insert` true upsert + chunk-size guidance, per-request `timeout_ms`, `QUERY_BUFFER_MB` — all landed in 2026.05+. See the [changelog](changelog.md) for the full list.
