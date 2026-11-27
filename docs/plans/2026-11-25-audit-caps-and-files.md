# Audit — Phase 2 area 4: caps, plus server-local file parameters (2026-11-25)

Read-only audit; symbols in `src/db/server.c`, `config.c`, `query_bulk.c`.

## AUD-FILE-1 — server-local file paths are open to any write token (CRITICAL, confirmed)

Six wire modes take a path on the **server's** filesystem and open it as the
daemon user, with no confinement and no scope check beyond ordinary data
`rw`: `bulk-insert` (`file`), `bulk-insert-delimited` (`file`), `bulk-delete`
(`file`), `bulk-update` (`file`), `bulk-update-delimited` (`file`) and
`put-file` (`path`, documented as "server-local"). They are the CLI's
local-file arguments exposed unchanged to the network.

Confirmed live: an object-scoped `rw` token created by the admin ran
`bulk-insert-delimited` with `file=<DB_ROOT>/tokens.conf` and `delimiter=":"`;
`keys` on its own object then returned the **global admin token**, which
`list-tokens` accepted (global rwx). Likewise `file=/etc/passwd` was ingested
into the token's object (44 rows) and read back with `find`. `bulk-insert`
with a JSON file ingests any readable JSON file; `put-file path=` followed by
`get-file` returns any readable file's bytes.

- Impact: arbitrary file read as the daemon user for any token with write
  permission on one object, including `tokens.conf` (global takeover),
  `db.env` (TLS key paths), other tenants' JSON exports. Violates the tenant
  isolation and A3/A4 boundaries.
- Repro: `audit-repro-server-file-read` (`--include-audit`), red.
- Fix options (design decision, needs approval):
  1. Refuse `file`/`path` unless the caller is a trusted IP or a global rwx
     token (admin); tokens use inline `records`/`data` instead.
  2. Confine to a configured import directory (`IMPORT_DIR`, realpath-checked,
     no symlink escapes) for everyone but trusted callers.
  3. Remove the wire parameters; keep the file forms CLI-only.
  Recommendation: option 1 (smallest, matches the existing trust model), with
  option 2 as an optional follow-up for operators who need file import over
  the network. Breaking for token clients that used server-local files.

## Status: fix applied (uncommitted)

Option 1 approved 2026-11-25. `server_paths_allowed` (trusted IP or global rwx
token) guards all six branches in `dispatch_json_query`; repro
`audit-repro-server-file-read` passes. Remaining: confinement (`IMPORT_DIR`)
is an optional follow-up; the CLI file forms are unaffected (trusted).

## AUD-CAP-1 — MAX_CONCURRENT_QUERIES does not cover NQL or the text path (major, proof trace)

`slot_try_acquire`/`slot_cleanup` are used only at the top of
`dispatch_json_query`. `dispatch_nql_query` (find, count, aggregate: the
heaviest read shapes) and the legacy text path never take a slot, so the
documented bound "peak query-buffer RAM = MAX_CONCURRENT_QUERIES x
QUERY_BUFFER_MB" does not hold: the only limit is the worker count (default
CPU count, up to 1024), each worker able to hold a `QUERY_BUFFER_MB` buffer
per collection site. An authenticated read-only client can therefore
exceed the intended memory ceiling by sending NQL instead of JSON.
Fix: take the slot in `server_process_fast` for every grammar (or in each
dispatcher). Test: N concurrent NQL aggregates must see "server at capacity"
beyond the cap.

## Other cap observations

- `QUERY_BUFFER_MB` is checked at ~130 sites (`g_query_buffer_max_bytes`:
  query.c 55, query_aggregate.c 26, typed_row.c 10, plan 8, storage 8, ...).
  Each collection site is verified in the Phase 3 slice of its mode; the
  inventory above is the cross-reference list. `query_bulk.c` has a single
  cap check (the key list), so the bulk parsers rely on `MAX_REQUEST_SIZE`
  and the unchecked allocations in AUD-BULK-OOM.
- Slot is taken before authentication, so unauthenticated requests briefly
  consume slots; each releases on return (cleanup attribute covers every
  return path in `dispatch_json_query`).
- `slot_init`: if `sem_init` fails (macOS: unnamed semaphores are
  unsupported) the cap is silently disabled with a stderr line; known gap
  (macOS), the daemon then runs uncapped.
- `request-size`: `MAX_REQUEST_SIZE` bounds a single line (oversize drain
  covered in the connection findings); fd/thread exhaustion at `accept` not
  yet exercised.
