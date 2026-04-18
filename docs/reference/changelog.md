# Changelog

For the full history see [`CHANGELOG.md`](https://github.com/sayyiditow/shard-db/blob/main/CHANGELOG.md) at the repo root. This page summarizes shipped releases and notes on what's in flight.

Versions follow `yyyy.mm.N` — year-month, with `N` as the counter within that month.

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

## Where to see what's next

[Roadmap (v2)](../v2/roadmap.md) — native TLS, replication, paid tier features, streaming-binary file protocol for very-large uploads.
