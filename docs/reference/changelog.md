# Changelog

For the full history see [`CHANGELOG.md`](https://github.com/sayyiditow/shard-db/blob/main/CHANGELOG.md) at the repo root. This page summarizes shipped releases and notes on what's in flight.

Versions follow `yyyy.mm.N` — year-month, with `N` as the counter within that month.

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
