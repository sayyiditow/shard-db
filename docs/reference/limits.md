# Limits

Hard caps and practical bounds. Everything here is enforced at compile time unless noted.

## Hard-coded constants (`src/db/types.h`)

| Constant | Value | Meaning |
|---|---|---|
| `MIN_SPLITS` | 4 | Minimum shards per object at `create-object`. |
| `MAX_SPLITS` | 4096 | Maximum shards per object. Filename is `NNN.bin` (3 hex digits), so this is structural — not a policy knob. |
| `MAX_KEY_CEILING` | 1024 bytes | Hard upper limit on per-object `max_key`. Keys are stored raw in Zone B, so every slot reserves `max_key` bytes — larger caps bloat `slot_size`. |
| `MAX_FIELDS` | 256 | Max fields per typed schema (bumped from 64 in 2026.04.2). |
| `MAX_LINE` | 65536 bytes | Internal buffer size for queries and intermediate strings. |
| `MAX_AGG_SPECS` | 32 | Max aggregate specs in one `aggregate` query. |
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
| `criteria` length | no hard cap | Practical limit is `MAX_REQUEST_SIZE`. |
| `join` chain length | no hard cap | Each join adds a per-record lookup; keep it short. |
| Composite index arity | 16 fields | Hard-coded in `IndexScanCtx`. |
| Request size (one JSON line) | `MAX_REQUEST_SIZE` (default 32 MB) | Configurable per install. |
| Response size | no cap | Bounded by memory + `GLOBAL_LIMIT`. |

## File storage limits

| Thing | Limit | Notes |
|---|---|---|
| Filename | 255 bytes, plain basename | No `/`, `\`, `..`, control chars. |
| File size (base64 variant) | ~24 MB at default config | Bounded by `MAX_REQUEST_SIZE / 1.33`. Raise `MAX_REQUEST_SIZE` to lift. |
| File size (server-local variant) | unbounded | Limited only by disk. |

## Concurrency limits

| Thing | Default | Notes |
|---|---|---|
| `THREADS` | 0 (= online CPUs) | Parallel scan workers. |
| `WORKERS` | 0 (= max(CPU, 4)) | TCP connection worker pool. |
| In-flight writes drain timeout | 30 s | `./shard-db stop` gives writes this long to finish. |
| Per-connection read buffer | `MAX_REQUEST_SIZE` | Allocated on first request, persists for connection lifetime. |

## Storage limits

| Thing | Limit | Notes |
|---|---|---|
| Shards per object | 4096 | Filename format. |
| Objects per tenant (`<dir>`) | filesystem-limited | No internal cap. |
| Tenants (`dirs.conf`) | filesystem-limited | Validated via O(1) hash. |
| Indexes per object | no hard cap | Each is one B+ tree file. `BT_CACHE_MAX` caps hot-cache entries, not count. |

## Not supported

- **Transactions across records.** Use [CAS](../query-protocol/cas.md) per record.
- **Cross-dir joins.** Same-tenant only.
- **OR in criteria.** AND-only combining; emulate with multi-query + dedupe or `in` sets.
- **Right / full outer joins.** Left and inner only.
- **Nested aggregates, window functions.** Post-process in the app.
- **Native replication.** Use block-level (DRBD) or filesystem-level (ZFS send/recv).
- **TLS (native).** Terminate at a reverse proxy — see [Deployment](../operations/deployment.md).
- **Platforms other than Linux.** Uses `epoll`; not portable without code changes.

## Roadmap

Several of these ("unlimited fields", native TLS, native replication) are on the [v2 roadmap](../v2/roadmap.md).
