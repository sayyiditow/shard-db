# Fix plan 3 — token and IP sets must delete correctly; auth files must be written atomically (AUD-AUTH-2, AUD-AUTH-3)

## Brief
**Problem and root cause.** `token_set_remove` and `ip_set_remove` clear a slot of an open-addressed table without a
tombstone, so lookups stop at the cleared slot and colliding entries inserted later become unreachable
(`remove-token` of such a token says `token_not_found`; it persists in `tokens.conf` and revives after restart).
`save_tokens_conf_full` and `save_allowed_ips_conf` use `fopen("w")` (truncate in place, no fsync, no rename, umask
mode, errors ignored); `token_set_add_full` silently drops a token when the table is full.

**Logical flow.**
1. Deletion uses backshift (Knuth algorithm R) for both tables: after clearing slot i, walk forward and move any entry
   whose home bucket makes it unreachable into the hole. Same for the tenant `dirs` set if it shares the pattern
   (verify in `dirs_remove`; fix identically or file separately).
2. `token_set_add_full` returns an int; `add-token` reports `{"error":"token store full"}` instead of success.
3. Saves write `<file>.tmp` (mode 0600), fsync, rename, fsync the directory; `add-token`/`remove-token`/`add-ip`/
   `remove-ip` return an error and roll the in-memory change back (or report) when the save fails.

**Decisions.**
| Decision | Alternatives | Why | Tag |
|---|---|---|---|
| Backshift deletion | tombstone flags | no extra state, keeps probe length short | DEFAULT |
| Roll back memory on save failure | keep memory, report error | runtime and file must agree | DEFAULT |
| 0600 for `tokens.conf`/`allowed_ips.conf` | leave umask | secrets | DEFAULT (an operator running as another user must adjust ownership) |

**Blast radius.** server.c token/IP code; `restore_merge_object_tokens` (uses `token_set_add_full`); startup load;
JSON gains error results on full/save failure. File mode change on rewrite.

**Tests.** `audit-repro-token-chain` goes green; table-fill case (cap 64: add 64, then 65th errors); removal of the
first/middle/last of a collision chain keeps the others valid, in both tables, plus random add/remove model test
against a reference set; save failure (read-only dir via faultlib open EACCES) returns an error and leaves the old
file; file mode is 0600; restart round trip.

**Out of scope.** Token hashing at rest; constant-time lookup; `list-tokens` fingerprint (AUD-AUTH-4, minor batch).

**Review log.** none yet.

## Plan (executor)
Branch `fix/auth-sets`. 1. Red tests above (token-chain already red). 2. Backshift helper `open_addr_delete(...)`
shared by token and IP tables (symbols: `token_set_remove`, `ip_set_remove`, arrays `g_token_set*`, `g_ip_set*`).
3. `token_set_add_full` int result and callers (`load_one_tokens_file`, `add-token`, `restore_merge_object_tokens`).
4. Atomic save helper `write_file_atomic(path, mode, writer_cb)` used by both save functions; propagate errors.
5. Check `dirs_remove` in config.c for the same defect; fix or record. 6. Docs and changelog; full suite; revert-prove;
uncommitted.
