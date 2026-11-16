# Phase 3A execution evidence — 2026-10-25

Concise actual-output record of executing
`docs/plans/2026-10-25-phase3a-packed-codec-plan.md`
(SHA256 `c35bab3dec21c560c71130aa69f36ab4d88c08d29d2569d13d84f785f90d9a9f`).
This is the executor's record, distinct from the plan's planning-time evidence.

## Identity

- Branch `fix/phase1-safe-scalar-conversions`, HEAD
  `8c0bdf6259d2dc33e540e28e82b9f8010750047d` (verified before execution).
- Plan checksum verified before execution; protected plan
  `docs/plans/2026-10-19-durability-certification-rig.md` checked by hash only
  (never read/edited/moved/staged): start and end SHA256 both
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` — unchanged.
- Pre-existing working-tree state preserved: modified
  `docs/plans/2026-10-19-type-correctness-recovery-plan.md`; untracked
  `docs/plans/2026-10-19-durability-certification-rig.md`,
  `2026-10-23-phase3-packed-records-plan{,-review}.md`,
  `2026-10-25-phase3-repair-review.md`, `2026-10-25-phase3a-packed-codec-plan.md`,
  `docs/plans/archive/`. No planning file was modified by execution.

## Changed-file inventory (execution)

- `build.sh` — four exact replacements (daemon gcc line, LIB_SRCS,
  shard-db-test-server line, shard-db-test list + two new case entries).
- New `src/db/packed_record.h` (SHA256
  `181abc577b8baa6a51206816cc33fd19e2ac347f3570a2016a04315880a6468a`),
  new `src/db/packed_record.c` (stub
  `79ead8b01246f2707c817d7630a6ee7c8a9b192f7ab49be81f008575ac73f8c9`;
  Task 2 full replacement `8f883cf3944b2eedb9b51a4e74b0b4c7643a3f316083cc521b5db1825696ab0d`).
- New `src/test/cases/test_packed_record_codec.c` (SHA256
  `c51ba08f9e2acdfb931c9ff3ff38d0b15caf4c0d4b4039e854158d9cc46f181a`),
  new `src/test/cases/test_packed_record_storage.c` (SHA256
  `fd65c281b06a60cdb8a2e7004897decaff2d14c2c937fc98808d955601bc3d73`).
- `src/db/types.h`, `src/db/seg_scan_varlen.h`, `src/db/slotcask.c`,
  `src/db/slotcask.h` — the twelve Task 2b exact replacements, each anchor
  matched exactly once (Edit-tool exact-unique match).
- New `docs/concepts/packed-record-codec.md` (Task 3a body verbatim).

`git diff --check` clean. config.c, storage.c, query*.c, index.c, embedded.c,
decnum.c, type_desc.c and all existing tests: unchanged (verified via
`git diff --name-only`). Packed storage remains inactive: no schema activation,
no public flag, no migration.

## Task 0

- `rtk git rev-parse HEAD` → `8c0bdf6259d2dc33e540e28e82b9f8010750047d`;
  branch `fix/phase1-safe-scalar-conversions`; tracked production clean apart
  from the pre-existing recovery-plan edit.
- Socket preflight: `local TCP socket preflight: OK` (first attempt, same
  environment used for all daemon tests).

## Task 1 — RED (stubs + tests against unchanged transport)

- Stub build `rtk proxy env SKIP_TESTS=1 ./build.sh`: exit 0, zero
  compiler warnings.
- `rtk proxy ./build/bin/shard-db-test run test-packed-record-codec`:
  exit 61 — `16 passed, 61 failed`. Matches the plan's measured codec RED
  exactly; failures are stub `DEC_ERR_INTERNAL` (status 10) missing-feature
  failures.
- `rtk proxy ./build/bin/shard-db-test run test-packed-record-storage`:
  exit 38 — `11 passed, 38 failed`. Matches the plan's forecast stub-stage
  count. Named mechanisms confirmed in the log:
  - scanner rejects packed+nullmap: `known generation combinations only` —
    expected 1 got 0 (flag 3);
  - emitter masks 3→1: `packed bit survives physical emitter` —
    expected 3 got 1;
  - invalid generations accepted and fixture mutated: `invalid generation
    insert rejected` — expected -1 got 0, plus the direct/pipeline batch
    rejection assertions (expected -1 got 0 / expected 22 got 0).
  Physical regressions ran before codec prerequisites as required (the
  codec-prerequisite assertions fail later with status 10).
- Optional final full RED not run (plan marks it "may"); the mandatory full
  suite ran after the complete change (below). Raw RED/GREEN logs are in
  `/tmp/p3a-exec/`, outside version control.

## Task 2 — GREEN

- Complete-candidate build `rtk proxy env SKIP_TESTS=1 ./build.sh`:
  exit 0, zero compiler warnings.
- `run test-packed-record-codec`: exit 0 — `268 passed, 0 failed` (expected 268/0).
- `run test-packed-record-storage`: exit 0 — `103 passed, 0 failed` (expected 103/0).
- Full suite `rtk proxy ./build/bin/shard-db-test run-all`: exit 0 —
  `1..518`, `# total: 18816 passed, 0 failed across 518 cases`. All
  pre-existing assertions unchanged.

## Task 3 — independent checks (scratch `/tmp/p3a-check`, not in version control)

- `cc -O1 -Wall -Wextra -Werror -shared -fPIC ...` libpacked.so: clean.
- `python3 /tmp/p3a-check/numeric_oracle.py /tmp/p3a-check/libpacked.so`:
  `3000 passed, 0 failed; independent Decimal assignment/round/render/cap
  oracle, seed20261025` (expected 3000/0).
- `oom_probe` build clean; run:
  `encode=3 compose=3 view=3 allocations; every injected failure preserves
  output contracts` (expected encode=3 compose=3 view=3).

## Discrepancies

All quoted anchors matched exactly once; no unexpected build warnings,
test failures or uncovered decisions were reported; no PLAN_NOTES.md required.

The subsequent raw-diff review found one comment-only deviation: seg_record_emit
retained the old NULLMAP-only comment instead of the plan's replacement.
The reviewer aligned it with the approved block. No executable tokens changed.
After that correction, all nine code/build/test files and the internal codec
document match the approved plan byte for byte. The optional
final full-RED run (plan: "Executor may run") was not executed — RED evidence
is the two focused stub runs above; the mandatory full suite ran green after
the complete change.

## Hand-back

Everything uncommitted and unstaged for plan-aware and independent blind
raw-diff review. No branch creation, commits, pushes, PRs or merges. No
benchmarks or sanitizers were run (branch-series sanitizer gates remain
immediately before any eventual push). 3B/3C not started; packed storage
inactive.
