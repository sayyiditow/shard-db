# Phase 3B.4 Revision 2 — execution evidence ledger

Executor: GLM-5.3-Flash (ZCode agent), 2026-10-26. All raw logs, extracted
driver/checks/probe sources and probe binaries live outside the repository in
`/tmp/p3b4-exec-jbsu6lgg/` (bootstrap output folder) plus the checks scratch
dirs `/tmp/p3b4-check-p0b7p3t1/` (collector RED),
`/tmp/p3b4-check-8oyimzx2/` (GREEN checks) and
`/tmp/p3b4-check-29huozoz/` (final checks); they are referenced, not copied.
The final directory reference was added during raw-diff review after checking
the actual final-checks.log. Nothing committed, staged or pushed.

## Identity and hashes

- Start and end: `a97f9f21caa0afcd29ae54ec665abf79466783ec`, branch
  `fix/phase1-safe-scalar-conversions` — matched at start, unchanged at end
  (no commits made).
- Approved plan `docs/plans/2026-10-26-phase3b4-packed-bulk-insert-plan.md`
  SHA256 `c06e2b748316ea25b45bdf5c969b5f9bc26a18ef64d53dcfaed1786d841acb6b`
  — matched at start and finish.
- Protected rig `docs/plans/2026-10-19-durability-certification-rig.md` SHA256
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` — matched
  at start and finish; contents never read, edited, moved, staged or executed
  (the driver reads its bytes only into SHA256).
- Start tree contained exactly the expected planning artifacts: the pre-existing
  five-line Phase 3 index modification
  (`docs/plans/2026-10-23-phase3-packed-records-plan.md`, preserved untouched),
  the untracked protected rig, the plan and the review. No other changes.
- Stage-1 `--check-only` preflight ran at Task 0 on this real checkout
  (`stage1-check-only.log`): all code-block checksums, pinned HEAD, ancestor,
  protected hash, committed parent sources and checkpoint-0 baseline verified
  with no source writes. Each applied stage re-verified its anchors and both
  boundary checkpoints through the plan's own driver before writing.
- Final checkpoint-3 table re-verified independently after execution: all 10
  entries byte-identical (`MATCH` for build.sh, both new typed_bulk_insert
  files, typed_row.h/.c, query_bulk.c, the test case and all three concept
  docs; new files exist, checkpoint-0-absent files existed then as nulls).

## Environment

- gcc (GCC) 16.2.1 20260810; Linux 7.2.10-arch1-1 x86_64; 16 cores.
- Binary: shard-db 2026.10.2 (required source release 2026.09.2, enforced).
- Every shell command prefixed `rtk proxy` (unfiltered); normal build
  `rtk proxy env SKIP_TESTS=1 ./build.sh`; tests via `build/bin/shard-db-test`.

## Commands and transcripts (raw logs in /tmp/p3b4-exec-jbsu6lgg/)

| Gate | Command (from plan Appendix A) | Exit | Result | Log |
|---|---|---|---|---|
| Task 0 preflight | `driver.py --stage 1 --check-only` | 0 | 4 files verified, no writes | stage1-check-only.log |
| Stage-1 apply | `driver.py --stage 1` | 0 | 4 files (2 new sources, test case, build.sh ×6 anchors) | stage1-apply.log |
| RED build | `env SKIP_TESTS=1 ./build.sh` | 0 | **zero warnings** | red-build.log |
| RED focused | `run test-typed-bulk-insert` | 33 | **102 passed / 33 failed** | red-focused.log |
| RED collector | `checks.py --mode red-collect` | 0 | probe exit 1: `pointer=(nil) requested=0 (expected NULL/2)` | red-collect.log |
| Stage-2 apply | `driver.py --stage 2` | 0 | 4 files (full module, typed_row.h/.c, query_bulk.c ×4 anchors) | stage2-apply.log |
| GREEN build | `env SKIP_TESTS=1 ./build.sh` | 0 | **zero warnings** | green-build.log |
| GREEN focused | `run test-typed-bulk-insert` | 0 | **178 passed / 0 failed** | green-focused.log |
| GREEN checks | `checks.py --mode green` | 0 | collector `requested=2`; 90 OOM injections; max-schema | green-checks.log |
| Stage-3 apply | `driver.py --stage 3` | 0 | 3 doc files via unique anchors (source untouched) | stage3-apply.log |
| Full suite | `run-all` | 0 | **19643 passed / 0 failed across 522 cases** | full.log |
| Final checks | `checks.py --mode green` (rerun) | 0 | same probe results as green-checks.log | final-checks.log |
| Diff hygiene | `rtk git diff --check` | 0 | clean | (inline) |

RED ran in exactly the plan's order (stub + regression before any
implementation), captured before stage 2 was applied.

## RED failure classes (all 33 named, from red-focused.log)

No physical rows: `dict physical value`, `upsert physical value`, `empty
positional physical row`, `binary-delimited physical row`, `decoded key
stored`, `reopened exact bare value`, `small packed maximum-schema row`,
`key-only OLD still readable`, `explicit empty row present`, `maximum varchar
physical value`. No fresh counts/default/AC/NULL outcomes: `two fresh
insertions`, `updates not counted as fresh`, `strict new committed`, `existing
is condition skip`, `good row commits`, `bad constructor counted error`, `not
condition skip`, `delimited inserted`. No successful positional replacement:
`positional CRLF and ignored surplus`, `length-delimited embedded NUL`,
`key-only lines skipped`, `key-only lines never insert`. No actual-size
insertion: `large schema small batch`, `maximum varchar bulk insert`, `actual
maximum payload`; plus their seed/setup assertions. Rejection assertions passed
incidentally on the stub, as the plan predicts; they are proven by the
implementation GREEN plus the injection probe, not by the stub run.

## Probe results (green-checks.log and final-checks.log, exit 0)

- Collector GREEN: `collect failure: pointer=(nil) requested=2 (expected
  NULL/2)` — the requested count now survives calloc failure; both callers can
  mark prepared inputs failed. RED run had printed requested=0 with probe exit 1.
- Allocation injections, one per selected allocation ordinal
  (typed_bulk_insert, typed_row, packed_record, decnum compiled without LTO
  and **without TEST_BUILD**, linked against the normal embedded library):
  - JSON layout: 26 replacement + 20 fresh failures pass — exact OLD or
    absence, live/deleted counts, segment position, tracked live=0.
  - Positional layout: 25 replacement + 19 fresh failures pass — same
    invariants.
  - Total **90** allocation failures, matching the plan (26/20/25/19).
- Maximum schema: actual value **2088** bytes; largest selected allocation
  **3072** (< the probe's 8 MiB guard), live=0 — no schema-maximum buffer.
  Benign `RESHARD-HINT` INFO lines from the splits=1 probe fixture appear in
  the log; probe assertions are unaffected (exit 0).

## Inventory (census)

- Working tree changes are exactly the owned set: modified `build.sh`,
  `src/db/query_bulk.c`, `src/db/typed_row.h/.c`,
  `docs/concepts/typed-row-adapter.md`, `docs/concepts/owning-write-hooks.md`;
  new `src/db/typed_bulk_insert.h/.c`,
  `src/test/cases/test_typed_bulk_insert.c`, `docs/concepts/packed-bulk-insert.md`.
  All byte-identical to Appendix B / checkpoint-3 hashes. Planning artifacts
  preserved untouched.
- Packed routes appear only in the two planned positions
  (`src/db/query_bulk.c:1364` JSON, `:2116` delimited), each guarded by
  `ts->packed.nfields`; both runners reach the same
  `typed_bulk_insert_command`.
- `typed_schema_bind_packed` has no production caller outside
  `typed_row.c` itself and private tests — no loader/create-object path binds
  packed layouts; public packed storage remains inactive.
- The bulk module never touches `new_from_old_alloc` /
  `slotcask_upsert_with_hooks`: bulk NEW buffers are owned by the adapter's
  rows and freed after the synchronous request, never by the single-record
  owning hook.
- No TODO/FIXME/debug markers introduced (the only `XXXXXX` hits are mkdtemp
  template strings; `snprintf` hits are field-name formatting).
- Build adds `src/db/typed_bulk_insert.c` to all five module lists (daemon,
  embedded static library, test-server daemon, test runner, benchmark binary)
  and registers the test case; benchmark is built, never run.
- No sanitizer or benchmark run in this child, per the plan; branch sanitizer
  gates remain the end-of-branch, immediately-before-push obligation.

## Appendix B conformance

All new functions/callbacks and command routes are byte-identical to Appendix
B: `typed_bulk_insert_command` (the only exported symbol of the new module),
`typed_json_validate` and `typed_insert_inputs_alloc` declarations in
typed_row.h, `prepared_insert_finish` (static, shared finalizer used by
`typed_insert_alloc` and `typed_insert_inputs_alloc`), `bulk_value` /
`BulkCell` (static value callback), and the two query_bulk routes. Verified by
the driver's block-hash and checkpoint checks plus the independent final
hash re-comparison — no assembly deviations.

## Deviations

None. Every gate matched the plan's stated numbers exactly: zero-warning
builds, RED 102/33 (exit 33), collector RED pointer=(nil)/requested=0
(probe exit 1), GREEN 178/0, collector requested=2, all 90 injections
preserving state, full suite 19643/0 across 522 cases (exit 0), clean
`git diff --check`, all checkpoint hashes. Local sockets were available; no
test-mode change was needed. Work is left unstaged and uncommitted for the two
mandatory raw-diff reviews (plan-aware and plan-blind).
