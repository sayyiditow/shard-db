# Phase 2 Task 2 warning amendment

Date: 2026-10-22. Scope: repair the plan after `PLAN_NOTES-2.md`; do not resume
implementation in the halted checkout.

- Input HEAD: `f5b80b4c58e49cfd8ac8b74ed9c1366f7b3a3130`.
- Previously approved plan: `58378f603a5d9d75c393f52fb32f5ea48f932c6bff9541591af31a5b30f5e570`.
- Amended plan SHA256: `8f508ff99b80bf4e885e048a4e16b219b1067bf669c533b267688bf62081ff88`.
- Protected certification plan (hash only):
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.

## Diagnosis and decision

The executor's halt is valid. Task 2 introduced three static definitions before
their first consumers. GCC 16.2.1's complete `-O2 -flto=auto` build also warned
about converting a possibly negative signed coefficient count to an allocation
size. A standalone object compile did not establish whole-program cleanliness.

Keep the zero-warning requirement. Move `DEC_ROUND_POWERS` into Task 5 and
`dec_limb_digits`/`dec_coeff_exp_raw` into Task 7. Guard each parse/unpack digit
allocation with `1 <= count <= DEC_MAX_COEFF_DIGITS`, then convert to `size_t`.
This makes the existing bound explicit at the allocation site. No warning
suppression, diagnostic downgrade, numeric-limit change, API change, packed
format change, or test-assertion change is introduced.

## Exact resume scope

The plan's new **Resume-only Task 2 warning repair** is authoritative for the
current halt. After human approval, apply only its three complete helper
removals and its exact `dec_parse` allocation replacement. Preserve both halt
notes, header, test, build wiring, and completed Task 1 evidence. Rebuild and
repeat Task 2's focused test; require zero warnings and the existing 557/181
transitional result. Then continue Tasks 3–8 in order. Task 5/7 introduce the
moved definitions when needed; unpack receives its guard with Task 7.
Fresh executions use the amended task blocks and skip the resume-only section.
The earlier first-halt resumption instruction is explicitly historical.

## Verification performed by the reviewing agent

All code changes below were confined to an isolated checkout under
`/tmp/p2-warning-repair-6jsl8a2n/checkout`, created from `git archive HEAD` with
the halted header, unit case and five build edits copied in. The Task 2 repair
was applied from the amendment's exact anchors, each matching once. Tasks 3–7
were assembled from the amended plan blocks. This is planning verification,
not executor progress in the real checkout.

- Ordinary GCC 16.2.1/LTO object compilation of each stage 2–7: zero warnings.
- **Complete Task 2 build**, `BUILD_MODE=release SKIP_TESTS=1 ./build.sh`:
  exit 0, zero warnings; actual runner: **557 passed, 181 failed**, exit 181
  (expected unfinished-core RED).
- **Complete final-core build**, same normal command: exit 0, zero warnings;
  actual runner: **888 passed, 0 failed**, exit 0.
- Plan-extracted independent Python-decimal oracle: **426 passed, 0 failed**.
- Plan-extracted ordinary allocation-failure probe: both parse allocation
  failures, both unpack allocation failures, and addition overflow leave
  canonical zero; exit 0.
- Full final-core regression suite: **18404 passed, 0 failed across 516 cases**,
  exit 0. Isolated test daemons ran with local loopback sockets enabled.
- Independent standards and spec reviews: no actionable findings.

Scratch logs, sources, probes and manifests remain outside version control.
No sanitizers or benchmarks were run. The real halted source/build/test files
and both halt notes retain their pre-review hashes. No staging, commits,
pushes, merges, Phase 3 work or protected-plan content access occurred.
The executing agent must still capture its own amended Task 2 and Tasks 3–8
results before handing back the uncommitted raw diff.
