# Changelog

All notable changes to shard-db are documented in this file. Versions follow the `yyyy.mm.N` scheme (year-month, with N as the release counter within that month).

## [2026.04.3] — 2026-04-18

### Added
- `remove-index` JSON mode + CLI — drop an index by exact name without touching data. Batch variant accepts `"fields":[...]`. Safe on non-existent names (returns `{"status":"not_indexed",...}` — idempotent, not an error).
- `put-file` **base64-in-JSON** variant — remote-safe uploads that don't require shared-filesystem access. Atomic write via `.tmp`+`fsync`+`rename`. Inherits the existing `MAX_REQUEST_SIZE` cap (default 32 MB ⇒ ~24 MB effective file size).
- `put-file` **`if_not_exists`** flag — CAS on file uploads; refuses overwrite when set.
- `get-file` JSON mode + CLI — stream files back to remote clients as base64. Pairs with the existing `get-file-path` (server-local fast path, unchanged).
- `valid_filename()` — strict basename validator: rejects `/`, `\`, `..`, control chars, empty names, >255 bytes. Enforced on every remote upload/download.
- `b64_encode`/`b64_decode` in util.c — RFC 4648 standard alphabet, whitespace-tolerant on decode, invalid-char strict.
- `/docs` site with MkDocs Material — every CLI command, every JSON mode, quick start, concepts, operations. Deploys to GitHub Pages via the new `.github/workflows/docs.yml` workflow.

### Changed
- CLI `./shard-db put-file <dir> <obj> <path>` now routes through the new TCP base64 path by default. Works from any host with TCP access to the server. The old server-local path mode is still accessible via explicit JSON (`{"mode":"put-file","path":"..."}`) for admin fast-path use cases.

### Fixed
- Oversized-request error path no longer hangs clients. The "Request too large" handler had an embedded NUL in its format string, truncating the response before the `\0\n` command separator — clients would wait forever for the terminator. Same bug fixed on the "Server shutting down" branch.
- README/docs drift on `create-object` defaults: `splits` actually defaults to `MIN_SPLITS=4` (not 64) and max is `MAX_SPLITS=4096` (not 256).

## [2026.04.2] — 2026-04-18

### Added
- `order_by` + `order` parameters on `find` — sort matches before pagination. Numeric field types sort numerically; varchar lexicographically. Not compatible with `join`.
- `*` wildcard on `LIKE` — in addition to `%`, accepts `*` as the glob character.

### Changed
- `MAX_FIELDS` bumped from 64 to **256** per schema.

### Fixed (same-day patch)
- Fresh-install Quick Start: pidfile was written before the logs directory existed; tenant wasn't auto-registered in `dirs.conf` on first use. Both fixed.
- Legacy stdio fast-path returned SEGV on missing objects instead of a clean error; drained `in_flight_writes` on early-return.
- Several README-vs-code mismatches caught during pre-release validation.

## [2026.04.1] — 2026-04-17

Initial v1 release.

### Core storage
- File-based KV with xxh128 hashing, mmap-backed reads/writes, linear probing
- Typed binary record format driven by `fields.conf` (varchar with uint16 length prefix up to 65535 bytes, int/long/short, double, bool/byte, date, datetime, numeric with P,S fixed-point)
- Per-slot 24-byte header (hash + flag + key_len + value_len) in Zone A; raw key+value payload in Zone B
- Dynamic shard growth (50% load factor doubles `slots_per_shard`, capped at `MAX_SPLITS=4096`)
- Crash recovery on startup: stale `.new` / `.old` rebuild artifacts are swept

### Query
- 17 search operators: eq, neq, lt, gt, lte, gte, between, in, not_in, like, not_like, contains, not_contains, starts, ends, exists, not_exists
- Find / count / aggregate (sum, avg, min, max, count) with group_by + having
- Read-only joins (inner / left) with tabular output
- Bulk insert / delete / update (with criteria and dry-run)
- Full-scan and indexed query paths; parallel per-shard workers for indexed queries
- Statement timeout enforcement (`SLOW_QUERY_MS`, coarse tick every 1024 iterations)

### Indexes
- B+ tree with prefix-compressed leaves (anchor every K=16 entries, two-stage bsearch)
- Single-field and composite (`field1+field2`) indexes
- All 17 search operators use indexes when available
- Parallel index build during bulk insert (one pthread per field)

### CAS / conditional writes
- `if_not_exists` on insert (idempotent)
- `if:{...}` conditional update and delete
- Bulk update/delete with `criteria` + `limit` + `dry_run`

### Schema mutations
- `rename-field` — metadata-only, preserves data
- `remove-field` — tombstone (space reclaimed with `vacuum --compact`)
- `add-field` — append new fields, triggers rebuild
- `vacuum --compact` — drop tombstoned fields, shrink slot_size
- `vacuum --splits N` — reshard (indexes + hash routing preserved)
- Per-object rwlock: normal ops share, schema mutations take exclusive

### Server
- Multi-threaded TCP server, epoll-based accept loop, JSON dispatch
- Token-based auth (`tokens.conf`) + IP allowlist (`allowed_ips.conf`)
- Multi-tenancy via `dir` parameter, validated against `dirs.conf`
- Async ring-buffer logging, info/error split, date-rotated, auto-retention
- Stats (`./shard-db stats`) and per-shard load table (`./shard-db shard-stats`)

### Field defaults
- `default=<literal>` — constant on INSERT
- `auto_create` — server datetime on INSERT
- `auto_update` — server datetime on INSERT and every UPDATE
- `default=seq(<name>)` — sequence values
- `default=uuid()` — UUID v4
- `default=random(N)` — N random hex bytes

### Limits
- `MAX_SPLITS = 4096` (max shards per object)
- `MAX_KEY_CEILING = 1024` (hard cap on per-object `max_key`; default 64)
- `MAX_FIELDS = 64` (per schema)
- `MAX_AGG_SPECS = 32` (per query)
- `varchar` content: 65535 bytes

### Tests
- 167 tests across 6 scripts: objlock (18), rename-field (24), remove-field (35), vacuum+add-field (50), parallel-index integrity (23), joins (17)

### Known limitations
- **Linux-only**: server uses `epoll`; no macOS or Windows support
- **Plaintext TCP**: no native TLS. Protect with IP allowlist + tokens; terminate TLS at a reverse proxy — **HAProxy recommended** (best TCP+TLS throughput), nginx `stream` or stunnel also work. See README → TLS Encryption.
- **No replication**: single-node only; use DRBD / block-level replication for HA
