# Changelog

All notable changes to shard-db are documented in this file. Versions follow the `yyyy.mm.N` scheme (year-month, with N as the release counter within that month).

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
