# Phase 2 summary and fix queue (2026-11-25)

State of every finding raised in Phase 2 (storage, dispatch/auth, connection
lifecycle, caps, locks, planner). "Decision" is what the human must approve
before a fix is written (CORE-PROCESS: no execution without an approved plan).

| ID | Severity | State | Repro | Proposed fix | Decision |
|---|---|---|---|---|---|
| AUD-AUTH-1 text protocol unauthenticated | critical | **fixed, uncommitted** (trusted addresses only) | `audit-repro-text-protocol-auth` green | done; follow-up: remove grammar or add token field | none now |
| AUD-FILE-1 server-local file paths | critical | **fixed, uncommitted** (trusted IP or global rwx token) | `audit-repro-server-file-read` green | done; optional `IMPORT_DIR` confinement | none now |
| AUD-VAC-KF-MARKER light vacuum with pending markers | DL (conditional on a prior I/O fault) | red | `audit-repro-vacuum-marker` | `slotcask_compact_kf` skips while markers pending (+ replay verifies slot key) | approve plan |
| AUD-BACKUP-SILENT backup/restore swallow errors, restore wipes first | major | proof trace | none yet (faultlib ENOSPC) | propagate errors, fsync, stage-then-swap restore | approve plan |
| AUD-COMPACT-ORDER donor unlinked before kf repoint durable | suspected major (power loss) | proof trace | needs ordering oracle | msync kf shards before `compact_drop_seg_file` | approve plan, then repro |
| AUD-KF-RESPLIT-SYNC resplit ignores keyfile msync/dir fsync | suspected major | red (error ignored) | `audit-repro-resplit-msync` | abandon resplit on sync failure | approve plan; second reviewer on severity |
| AUD-AUTH-2 token/IP set removal breaks probe chain | major | red | `audit-repro-token-chain` | tombstones or backshift deletion | approve plan |
| AUD-CONN-1 idle connections starve workers | major | red | `audit-repro-idle-connections` | read/idle timeout (new db.env key), handshake timeout | approve plan (new config key, breaking for idle clients) |
| AUD-CAP-1 slot cap skips NQL/text | major | proof trace | to add | take slot in `server_process_fast` for every grammar | approve plan |
| AUD-AUTH-3 auth files written in place | minor-major | proof trace | - | temp + fsync + rename, 0600 | with AUTH-2 |
| AUD-BULK-DEL-K delete K re-entry | minor | traced, not DL | - | treat already-tombstoned slot as done | with bulk slice |
| AUD-BULK-DEFER-CONT | minor | traced correct, untested | - | add duplicate-key test | with bulk slice |
| AUD-REPLAY-BOUNDS | minor | proof trace | - | bounds-check replay flag helpers | with VAC-KF-MARKER |
| AUD-AUTH-4, AUD-CONN-2, AUD-NQL-DIR | minor | proof trace | - | small hardenings | batch |
| AUD-BULK-OOM, AUD-PLN-2/3, AUD-REDUCE-1 | minor/refactor | recorded | - | Phase 4 batches | later |

Suggested fix order (DL and security first): VAC-KF-MARKER, BACKUP-SILENT,
AUTH-2 + AUTH-3, CONN-1, CAP-1, then COMPACT-ORDER and RESPLIT-SYNC once
their repros are confirmed by a second reviewer, then the minors.

Gates still owed before any push: ASan+UBSan x3 and TSan x3 (the diff touches
`server.c` request dispatch only so far, no locks; the gate is still run once
before the push per the repo rule), a blind review of the branch diff, and
the human's raw-diff review.

## Execution status (2026-11-25, later)

Plans 1-5 approved and executed in one working tree (no per-plan branches;
nothing committed), full suite 722 cases / 0 failed:

| Plan | Result | Regression case |
|---|---|---|
| 1 vacuum vs pending markers | kf compaction skips while markers pending (`kf_skipped`), update replay verifies the slot key, replay helpers bounds-checked | `test-vacuum-kf-pending-markers` (the defense-in-depth check was also shown to fail closed with the guard disabled) |
| 2 backup/restore | errors propagated, fsync, unique dirs, stage-then-swap restore with startup rollback | `test-backup-restore-failures`, `test-restore-swap-rollback` |
| 3 auth sets | tombstone deletion, atomic 0600 saves, error on full table / save failure | `test-token-store-chain` |
| 4 connection timeouts | `IDLE_TIMEOUT_SEC`, `HANDSHAKE_TIMEOUT_SEC`, fail-closed peer address | `test-idle-connection-timeout` |
| 5 query slot | slot for NQL and text grammars | `test-query-slot-all-grammars` (red-proven by disabling the fix) |

Also promoted to regular cases: `test-text-protocol-auth`,
`test-server-file-paths-admin-only`. Not done: per-fix blind review, the
ASan/TSan gate (once before push), revert-prove pastes for plans 2-4 (their
asserts exercise outputs that cannot occur on the old code), the IP-set
tombstone path has no dedicated wire test (the token table does).
