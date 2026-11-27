# Audit — Phase 2 area 2: dispatch and auth (2026-11-25, in progress)

Read-only audit; no product-source edits. Cites symbols in `src/db/server.c`.

## AUD-AUTH-1 — legacy `\x1F` text protocol has no authentication (CRITICAL)

`worker_thread` hands every request line to `server_process_fast`. A line
that starts with `{` goes to `dispatch_json_query` (authenticates), a line
starting with `f`/`c`/`a` without `\x1F` goes to `dispatch_nql_query`
(authenticates as `find`), and **every other line is parsed as the legacy
`\x1F`-delimited protocol and executed with no token check and no trusted-IP
check**. A comment in the `restore` branch states it: "The text path has no
authentication". `DISABLE_LOCALHOST_TRUST`, `tokens.conf`, tenant and object
scopes, `allowed_ips.conf` and `mode_admin_level` are never consulted.

Reachable commands include `get`, `exists`, `insert`, `delete`, `find`,
`fetch`, `keys`, `bulk-insert`, `bulk-delete`, `put-file`, `get-file-path`,
`size`, `orphaned`, `vacuum`, `recount`, `truncate`, `backup`,
`restore [--force]`, `add-index`, `remove-index`, `sequence`.

Live check (strict mode, one admin token, `DISABLE_LOCALHOST_TRUST=1`):
JSON `stats` without a token -> `auth failed`; text `insert`, `get` and
`truncate` without a token -> succeeded, and the record was gone afterwards.
Repro: `audit-repro-text-protocol-auth` (`--include-audit`), red on the
current tree (2 of 4 assertions fail: unauthenticated read returns the
record, unauthenticated truncate destroys it).

- Violates the threat model's first invariant ("unauthenticated peer reaches
  *no* data", Trust boundary 1) and the documented token model.
- Impact: any peer that can reach `PORT` can read, modify, destroy or
  overwrite (`restore --force`) any object of any tenant. Data loss by an
  unauthenticated peer. Severity: critical; DL-first rule applies.
- Not covered by any existing test (the token tests use the JSON grammar).
- Fix options (a design decision, so the plan needs your approval):
  1. Remove the legacy text grammar (breaking for any client using it).
  2. Keep it only for trusted IPs: `if (!is_ip_trusted(client_ip))` refuse,
     which keeps localhost-trust semantics and closes remote access; still
     unauthenticated for every local process.
  3. Add a token field to the text grammar and run `is_authorized` with the
     command name as the mode.
  Recommendation: option 2 now (smallest, closes the hole), option 1 or 3 as
  a follow-up once usage of the text grammar is known. Breaking-change
  flag: yes, for remote clients using the text grammar.

## Status: fix applied (uncommitted)

Option 2 approved by the human 2026-11-25. `server_process_fast` now refuses
the legacy text grammar unless `is_ip_trusted(client_ip)`; the repro
`audit-repro-text-protocol-auth` passes (4/4), the full suite passes (714
cases). Not yet done: sanitizer gate, blind review of the diff, commit.
Follow-up: decide whether to remove the grammar or add a token field.

## Matrix review so far (no other defect found yet)

- `is_authorized`: scope match (global / tenant / object) then
  `mode_admin_level` (NONE / OBJECT / TENANT / SERVER) and
  `mode_is_data_write`; the early admin gate for token/ip/dir management
  uses `is_token_valid`, which requires a global, rwx token (verified; covered
  by `test_token_perms` and `test_per_tenant_auth`).
- Case handling: `mode_admin_level` and the JSON dispatch chain use
  `strcmp`; only the lock-class helpers use `strcasecmp`. A differently cased
  mode is unknown to dispatch, not an escalation.
- NQL path (`dispatch_nql_query`) authenticates as `find` (read-only modes
  only) but, unlike the JSON and text paths, never calls `is_valid_dir` on
  `cmd.dir`. A traversal-shaped dir cannot match a scoped token and
  `load_schema` finds no schema line for it, so no read occurs; it is a
  consistency gap (minor, AUD-NQL-DIR).

## AUD-AUTH-2 — token and IP stores break their probe chains on removal (major, confirmed)

`token_set_remove` and `ip_set_remove` clear the slot without a tombstone or
backshift in an open-addressed table. Every colliding entry inserted after
the removed one becomes unreachable, because lookups stop at the first unused
slot. Observed live (TOKEN_CAP=64, two colliding tokens): after
`remove-token` of the first, the second returns `auth failed`; `remove-token`
of the second answers `token_not_found`, yet it stays in `tokens.conf` (the
next save rewrites it from the table) and authenticates again after a
restart. So a token cannot be revoked at runtime once its chain is broken, and
access flaps between runtime and restart. IP trust has the same defect
(`remove-ip` of an entry whose chain was broken reports `ip_not_found`).

- Repro: `audit-repro-token-chain` (`--include-audit`), red (2 of 5
  assertions fail).
- Fix (Phase 4): tombstones or backshift deletion in both tables; rebuild on
  load; same for the tenant `dirs` set if it shares the pattern (verify).
- Related minor: `token_set_add_full` silently drops a token when the table
  is full, while `add-token` still answers `token_added` and writes the file;
  a later restart loads it into a fresh table (so it reappears) or drops it
  on overflow.

## AUD-AUTH-3 — auth config files are rewritten in place (minor-major)

`save_tokens_conf_full` and `save_allowed_ips_conf` use `fopen(path,"w")`
(truncate then write): no temp file, no fsync, no atomic rename, no mode (so
umask, typically 0644) and the open/write errors are ignored. Effects: a
crash during a save leaves an empty or partial `tokens.conf` (lock-out of
every token-based client); `add-token` reports success when the write failed;
token secrets are world-readable on a multi-user host (the backup copy of an
object's `tokens.conf` is forced to 0600, the live file is not). Local-file
access is outside the threat model (A5), so the permission part is a
hardening item. Fix: write to a 0600 temp file, fsync, rename, fsync the
directory, and fail the mode on any error.

## Other checks (no defect)

- `token_compare` is length-then-bytes without early exit; tokens are looked
  up by hash bucket first. Acceptable under the threat model.
- `list-tokens` shows `first4...last4` for tokens longer than 10 characters:
  for an 11-character token that is 8 of 11 characters. Global admin only;
  minor (AUD-AUTH-4: use a fixed fingerprint such as a hash prefix).
- Peer address is IPv4 only (`AF_INET` socket, `inet_ntop(AF_INET)`); the
  server binds `INADDR_ANY`. Consistent; IPv6 clients cannot connect.
- Path inputs: `is_valid_dir` (membership in the dirs set), `is_valid_object`
  (no separators, `:`, control chars, leading dot), `valid_filename` (no
  separators, control chars, `.`/`..`), `from` of restore (no `/` or `..`).
  `valid_filename` allows a leading dot (e.g. `.hidden` inside `files/`);
  harmless but the `.sdprobe-` backup skip shows dot-files are reachable.

## Still to do in this area

Per-mode scope x perm table checked against `server.c` for all modes;
the per-mode scope x perm table for every mode (done at the `is_authorized`
level; per-mode behavior is checked again in each Phase 3 slice).
