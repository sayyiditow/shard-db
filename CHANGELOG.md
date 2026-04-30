# Changelog

All notable changes to shard-db are documented in this file. Versions follow the `yyyy.mm.N` scheme (year-month, with N as the release counter within that month).

## [2026.05.1] — 2026-05-01

Big release. Per-shard btree layout, native TLS 1.3, per-tenant + per-object auth, OR criteria, AND-intersection planner, find-cursor pagination, TUI client, full Coverity Scan sweep (79 → 0 defects). Headline: parallel CSV bulk-insert at **2.96 M records/sec** (5 conns × 2M/chunk on a Ryzen 7 7840U, beating the previous 2.76 M baseline).

### Added
- **Per-shard btree layout** — every indexed field now sharded into `splits/4` btree files at `<obj>/indexes/<field>/<NNN>.idx`. Writes route by xxh128 hash to one shard; reads fan out across all shards in parallel; cursor pagination uses streaming k-way merge for global ordering. Per-file rwlock + persistent MAP_SHARED (mirrors ucache). `BT_CACHE_MAX = FCACHE_MAX/4` derived; not separately configurable.
- **Find-cursor pagination** — keyset-based deep pagination on indexed `order_by`. Pass `cursor:null` to opt in (page 1), hand back the returned cursor verbatim for page N+1. ASC + DESC supported. O(limit) per page regardless of depth (vs offset's O(matches)).
- **Native TLS 1.3** — single-port, in-process, OpenSSL-based. Off by default; `TLS_ENABLE=1 + TLS_CERT/KEY` enables. TLS 1.3-only (mandatory AEAD ciphers); reverse-proxy termination still supported. Hostname verification via `SSL_set1_host`.
- **Per-tenant + per-object tokens** with `r` / `rw` / `rwx` permissions. Three scope levels (`$DB_ROOT/tokens.conf` global, `$DB_ROOT/<dir>/tokens.conf` tenant, `$DB_ROOT/<dir>/<obj>/tokens.conf` object). Trusted IPs, localhost, and admin tokens still bypass / cover everything as before.
- **OR criteria** — `{"or":[...]}` and `{"and":[...]}` in `criteria` and `having`. Five execution paths (pure AND single-leaf, pure AND multi-leaf intersect, AND+OR hybrid, pure OR fully-indexed union, OR with non-indexed child) chosen automatically by the planner. Lock-free `KeySet` for OR-union and AND-intersection candidate sets.
- **AND index intersection** (`PRIMARY_INTERSECT`) — when `criteria` is a pure AND of 2+ indexed leaves on rangeable operators (eq/lt/gt/lte/gte/between/in/starts_with), the planner intersects candidate sets via `KeySet` and skips per-record fetch for `count`. Order-of-magnitude wins when the intersection is much smaller than any single leaf.
- **Aggregate NEQ algebraic shortcut** — `count(neq X)` rewritten internally as `count(*) - count(eq X)`, turning a near-everything scan into two cheap counts.
- **`shard-cli`** — separate ncurses TUI client (~60K stripped, no daemon code linked). Connects over TCP+TLS using existing CLI auth/env. Top-level menus for Server / Browse / Query / Schema / Maintenance / Auth / Stats. Builds alongside `shard-db`.
- **Per-request statement timeout** — `"timeout_ms":N` on `find` / `count` / `aggregate` / `bulk-delete` / `bulk-update`. Overrides global `TIMEOUT` for that single request. Aborts with `{"error":"query_timeout"}` and the server keeps serving.
- **`QUERY_BUFFER_MB` cap** (default 500) — bounds intermediate buffers any single query can hold (collect-hash, KeySet, aggregate buckets, sort buffers). Exceeded → `{"error":"query memory buffer exceeded; …"}` and the server keeps serving. Pair with `MemoryMax=` / cgroups for whole-process containment.
- **CSV / delimited export** — `format:"csv"` on `find` / `fetch` / `aggregate` / `get` (multi-key) / `keys` / `exists` (multi-key). Optional `delimiter` field; defaults to `,`, accepts `\t`. RFC 4180 quoting. CSV not supported with `join`.
- **Bulk-update JSON form** — `{"mode":"bulk-update","records":[{"id":"k","data":{...}}, ...]}` for per-key partial updates. Only fields in `data` overwrite; absent fields are kept. Distinct from criteria-based mass update (still supported).
- **`delete-file` + `list-files`** — file-storage management modes. `list-files` walks the bucket tree with optional prefix filter and pagination; `delete-file` is idempotent on missing files (returns clear error).
- **`stats-prom`** — Prometheus text-format exposition of all `stats` counters. Wire to a Grafana dashboard; the old JSON `stats` endpoint is unchanged.
- **15 new search operators** (now 38 total): length ops on varchar (`len_eq` / `len_neq` / `len_lt` / `len_gt` / `len_lte` / `len_gte` / `len_between`); case-insensitive ASCII variants of like/contains/starts/ends (`ilike` / `icontains` / `istarts` / `iends` and their not-forms); field-vs-field comparators (`eq_field` / `neq_field` / `lt_field` / `gt_field` / `lte_field` / `gte_field`); POSIX extended regex (`regex` / `not_regex`).
- **Token management** — `add-token` / `remove-token` / `list-tokens` (scope + perm). `list-tokens` returns fingerprints, never raw tokens.
- **Tenant management** — `add-dir` / `remove-dir`. Cross-process safe via flock on `dirs.conf`.
- **Schema export / import** — `export-schema` / `import-schema` to snapshot / restore the schema layout (objects, fields, indexes, splits) without touching data.
- **`describe-object` + `list-objects`** — discoverability JSON modes used by `shard-cli` to populate menus.
- **Single-instance guard** — `flock(LOCK_EX|LOCK_NB)` on `$DB_ROOT/.shard-db.lock`. Prevents two daemons sharing a `DB_ROOT` even with different ports.
- **ARM64 release matrix** — linux-arm64 alongside linux-amd64 in CI.
- **Signed releases** — cosign keyless OIDC signing + SLSA Level 3 provenance attestations on every tagged release.

### Changed
- **`MIN_SPLITS` 16 → 8 + `DEFAULT_SPLITS` 16 → 8** — tuned for the ~70% case (sub-1M-row objects on small servers). Index parallelism preserved (8/4 = 2 idx shards still gives k-way merge fan-out). Larger workloads should pass `splits` explicitly per the tightened sizing-tier table in `docs/operations/tuning.md`.
- **Sizing tiers tightened** — each tier transition lands before the 500K records/shard `shard-stats` nag. `8→1M`, `16→1–4M`, `32→4–10M`, `64→10–25M`, `128→25–60M`, then ~doubling rows-per-tier through `4096→1B+`.
- **Bulk-insert is now true upsert** — overwriting an existing key drops stale index entries before writing, so re-bulk-inserting the same dataset doesn't double-index. Tuning rule: indexed bulk-insert prefers FEWER LARGER calls — sweet spot at 1M records is ~5 conns × 200K records/request.
- **`shard-stats` hint thresholds** documented and surfaced in `docs/operations/tuning.md`. Auto-suggests `vacuum --splits=N` at 500K records/shard, escalates at 1M, suggests `partition by object` past `MAX_SPLITS`.

### Fixed
- **TOCTOU in `cprf`** (recursive backup copy) — `lstat`-then-`opendir` pattern was symlink-swap vulnerable. Now opens once via `O_NOFOLLOW`, `fstat` + `fdopendir` operate on the same fd. Closes Coverity TOCTOU.
- **btree cache evict-during-rwlock-wait race** — between dropping `bt_cache_lock` and acquiring the per-entry rwlock, another thread could `bt_cache_drop_slot` to evict and reuse the slot for a different path; we'd lock the right rwlock but read the wrong file's `fd`/`map`. Verify-and-retry under the rwlock + bounded retries; cache-miss path takes rwlock before unlocking the table mutex.
- **OOM-path NULL deref crashes** in 5 bulk paths (insert / insert-string / update-delim / update-json / vacuum / shard-stats / recount). The `x = NULL; break;` realloc-failure pattern left the count > 0, then dereferenced NULL in phase 2. Reset count alongside pointer; nested cleanup where records own heap allocations.
- **Lock-evasion / data races** on lock-free fast-skip flags — `UCacheEntry.used` / `.slots_per_shard`, `OrderedCollectCtx.budget_exceeded`, `BulkCriteriaCtx.budget_exceeded` / `.count`, `AdvSearchCtx.count` / `.printed`, `parallel.c::g_pool_running` (was `volatile int`, which is not a synchronization primitive in C). All now `_Atomic` with the lock-free read pattern documented at each site.
- **typed-cache second-lookup race** in `load_typed_schema` — post-populate lookup was lock-free; could observe partial state of another thread's populate. Now snapshots the return pointer while still under `g_typed_lock`, second lookup eliminated.
- **`db-dirs` + `vacuum-check` handlers** — iterated `g_dirs_used` / `g_dirs` lock-free. Now snapshot under `g_dirs_lock` before iterating (mirrors `objlock.c`).
- **`nthreads * 64` INTEGER_OVERFLOW** in `wq_init` — typo'd `WORKERS=999999999` in `db.env` could wrap signed int. `nthreads` now clamped to `[4, 1024]`.
- **`fdopen(out_fd, ...)` with `out_fd = dup(cfd) = -1`** — guarded with ternary check.
- **Counts file open-failure silent-fail** in `update_counts` / `set_count` / `reset_deleted_count`. Now log + bail on `open()` failure.
- **`mkdirp` / `mkdir` / `rename` / `fstat` / `fscanf` unchecked returns** — full sweep across server / config / storage / query / util.
- **`load_index_fields` consumer-side null-termination** — belt-and-suspenders `idx_fields[i][255] = '\0'` after every `load_index_fields` call (10 sites). Source already terminates within 256 bytes; this closes the STRING_NULL chain at every consumer.
- **`MIN_SPLITS = 4` doc drift** — the actual code-floor was 16 throughout 2026.04.x; docs incorrectly claimed 4 in some places. Now correctly documented at the new floor of 8.
- **`GLOBAL_LIMIT` documentation correctness** — it's a fallback default applied when `limit` is omitted, NOT a hard server-side clamp. Per-query `"limit": 500000` returns 500K records even with `GLOBAL_LIMIT=100000`. Use `QUERY_BUFFER_MB` for the actual memory protection. Corrected across `tuning.md`, `limits.md`, `configuration.md`, `aggregate.md`.
- **`read_file()` hardened** — `fseek` / `ftell` / `fread` returns checked, `malloc` NULL guard, negative `ftell` rejected before cast to size_t.
- **`SO_REUSEADDR` / `TCP_NODELAY`** — best-effort socket options, explicit `(void)` cast to silence CHECKED_RETURN.
- **REVERSE_INULL** in `cmd_bulk_insert` stdin path — initial `json = malloc(cap)` had no NULL check; line dereferenced before the post-loop check caught it. Initial-malloc NULL guard added; redundant outer check removed.
- **9 RESOURCE_LEAK CIDs** in OOM-cleanup paths — `arena`, `idx_pairs[]`, `idx_pair_counts/caps`, `json` / `data` buffers were missing from the free chain.
- **`first_empty` dead code** in `ucache_probe` + `bt_cache_probe` — vestigial tombstone-aware probing variable that was never used (these tables clear `used` outright on delete).
- **Build artefact hygiene** — `./build.sh` now purges `build/db`, `build/logs`, `build/bin/db.env` at the start of each build so re-runs always emit a clean tree. Deploy message updated to point at `build/bin/` (not `build/`).

### Performance
- Length-scan `count` **3–4× faster** (length ops answer from btree leaf entry's `vlen`, no record fetch).
- Single + bulk deletes **2.7–16× faster** depending on shape (per-shard btree layout reduces lock contention; bulk-delete batches index work).
- `add-indexes` ~**30% faster** (per-shard parallel build: 14 fields × 16 idx-shards = 224 workers from a single-pass scan).
- Parallel CSV bulk-insert (5 conns × 2M each, 10M total): **2.96 M/sec** on Ryzen 7 7840U / NVMe ext4. Single-conn CSV stays in 2.04–2.22 M/sec band (was 2.39 in 2026.04.x; the per-shard layout adds ~7% overhead at single-conn but parallel beats the prior baseline).
- Disk footprint: **+25%** with 14 indexes (1.3 → 1.6–1.7 GB on the invoice schema) — per-shard btree layout (each btree starts at `2 × bt_page_size = 8 KB`; 14 × 16 = 224 trees × 8 KB headers ≈ 1.8 MB before any data, vs 14 × 8 KB = 112 KB for the old single-tree layout).

### Security / hardening
- **Coverity Scan**: 79 → 0 outstanding defects across 16 categories (RESOURCE_LEAK, FORWARD_NULL, LOCK_EVASION / MISSING_LOCK, ATOMICITY, OVERRUN, STRING_NULL, REVERSE_INULL, CHECKED_RETURN, NEGATIVE_RETURNS, INTEGER_OVERFLOW, TOCTOU, DEADCODE, UNINIT). Real bugs fixed; the rest are documented false-positives with inline reasoning.
- **OpenSSF Best Practices** badge.
- **Pinned GitHub Actions by commit SHA** across all workflows (Scorecard alerts).
- **Hash-pinned every transitive pip dep** with `--require-hashes` (Scorecard #61).
- **CodeQL workflow** added with read-all permissions.
- **PII shape stripped** from bench data.

### Notes on the 2026.05.x line
- **macOS port** lands as `2026.05.2` (3 Linux-isms — `epoll` → `kqueue`, `memfd_create`, `posix_fallocate` — plus CI matrix expansion).
- **Embedded mode** (single-process library + Node.js N-API binding + npm publish) is on the roadmap for 2026.06; multi-process semantics handled by the same `.shard-db.lock` flock guard the daemon already uses.

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
