# Fix plan 2 — backup and restore must report copy failures and not wipe first (AUD-BACKUP-SILENT)

## Brief
**Problem and root cause.** `cprf` returns void and `copy_file` results are discarded, so `cmd_backup` reports
`backed_up` after ENOSPC/EIO/unreadable files, and nothing is fsynced. `cmd_restore` runs `rmrf` on the live
subtrees, then `cprf` (errors ignored), then reports `restored`: a failed copy leaves a partial object reported as
restored. Backup directory names have 1-second resolution (two backups in a second merge).

**Logical flow.**
1. `cprf` and `copy_file` return `int`; every call site propagates; `cprf` returns -1 on the first error (open,
   mkdir, copy), after attempting nothing further in that subtree.
2. `copy_file` fsyncs the destination file; `cprf` fsyncs each created directory; `cmd_backup` fsyncs `object.json`
   and the backup directory; only then prints `backed_up`. Failure prints `{"error":"backup failed: <step>: <errno>"}`
   and removes the partial backup directory.
3. `cmd_restore` copies into a staging directory (`<obj>/.restore-<pid>-<ts>`) first; on success it renames the live
   subtrees to `.old` names, renames staged ones in, fsyncs the parent, then deletes `.old`. Any failure before the
   rename leaves the live object untouched and returns an error. Startup sweeps stale `.restore-*`/`*.old` dirs.
4. Backup directory name gains a uniqueness suffix when the timestamp directory exists.

**Decisions.**
| Decision | Alternatives | Why | Tag |
|---|---|---|---|
| Stage-then-swap restore | keep wipe-then-copy but report errors | crash mid-restore must not leave a half object | NEEDS YOU (more code and disk space: restore needs room for a second copy) |
| fsync on backup | none | a "backed_up" that can vanish on power loss is not a backup | DEFAULT |
| Unique name suffix `_N` | keep 1 s resolution | merging two backups is silent corruption | DEFAULT |

**Blast radius.** `cprf`/`copy_file` (static in query_maint.c); `cmd_backup`, `cmd_restore`; the daemon startup sweep;
JSON responses gain an error shape on failure (previously always success); disk usage during restore doubles.
Breaking: restore needs free space for a second copy.

**Tests.** ENOSPC/EIO via `audit/faultlib.so` on backup (error reported, no partial dir) and on restore (live object
unchanged, error reported); crash between staged copy and swap (kill at a pause point) leaves a restorable state;
two backups in one second get distinct directories; existing backup/restore cases stay green.

**Out of scope.** Object tokens merge on restore; files/ semantics (unchanged).

**Review log.** none yet.

## Plan (executor)
Branch `fix/backup-restore-errors`. Tasks, each test-first:
1. Failing tests: faultlib ENOSPC backup (expects error), faultlib restore (expects live object intact), same-second
   backups (expects 2 dirs). Red proof pasted.
2. `query_maint.c`: change `copy_file`, `copy_file_sparse` callers and `cprf` to int; fsync file/dir; propagate.
3. `cmd_backup`: collect errors, fsync, cleanup on failure, unique dir name; emit error JSON.
4. `cmd_restore`: stage, verify, swap, fsync; remove staging on failure; startup sweep of `.restore-*` in the
   per-object recovery sweep (embedded.c `bmfix_recover_all` neighbor or object open path) — name the exact function
   when implementing; if no suitable startup hook exists, stop and write `PLAN_NOTES.md`.
5. Docs: `docs/query-protocol/schema-mutations.md` (backup/restore errors, space note), changelog.
6. Full suite; revert-prove each new test; leave uncommitted.
