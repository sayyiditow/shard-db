# Phase 3B.5 independent plan review — revision 2

Reviewed plan: [packed keyed bulk partial updates](2026-10-26-phase3b5-packed-bulk-update-plan.md).
Final plan SHA256: `a49a1d0b02444ab0ac8e600d8c3f3bc68ece196960f229c91550fc026538871b`.
Fixed checkpoint: `03617a898d5dff291c539b3ab33522054fd36cfb` on
`fix/phase1-safe-scalar-conversions`.
Planner/reviewer: GPT 6.1 Sol. Intended executor: GLM 5.3.

## Result

Ready for user approval and execution. Three findings were corrected in the
plan and scratch proposal. Independent source re-review and execution-readiness
re-review found no remaining actionable issues. This reviews a proposed change;
production code is unchanged and the executed raw diff still requires review.

## Findings and resolutions

1. **P1 — repeated partial-update keys lose patches.** Dictionary aliases
   (including escaped keys) and repeated positional keys bypassed the array-only
   duplicate screen. Each row could compose against the same OLD, discarding a
   disjoint patch while reporting success. All update forms now screen storage
   keys before execution. Insert duplicate behavior stays unchanged. Regressions
   assert rejection, unchanged OLD/counts and no segment reservation.
2. **P2 — shard error hides committed later rows.** A first-window pre-marker
   failure can coexist with later committed windows. Folding a shard rc=-1 over
   every row misreported completed rows as errors. Update counts now use terminal
   row statuses and retain a separate partial-request failure warning. Pending
   durability still overrides counts. A one-shot marker-link fault independently
   proves the first OLD survives and the second NEW persists after reopen.
3. **P2 — missing CORE-PROCESS temporary-revert proof.** Initial RED-first remains
   mandatory. A separate scratch-only proof now reverses/reapplies frozen query
   and review-fix hunks, verifies hashes, and reruns the actual probes. Checkout
   sources and checked-out binaries are never reverted. This closes the process
   conflict without replacing initial RED with a later reproduction.

## Measured normal scratch validation

| Gate | Result |
|---|---|
| Revised stage1 build | exit0, no compiler warnings |
| Initial focused RED against stubs | 200 passed / 35 failed, exit35 |
| Legacy/criteria RED on unchanged source | 0/5, four isolated SIGSEGV reproductions |
| Duplicate regression before fixes | 227 passed / 8 failed |
| Partial-window reporting before fix | 4 failed response gates; reopen proves OLD/NEW |
| Fixed build | exit0, no compiler warnings |
| Focused GREEN | 235 passed / 0 failed |
| Normal full suite | 19,878 passed / 0 failed across523 cases, exit0 |
| Selected allocation failures | 112: dict26 + missing8 + array26 + positional25 + file27 |
| Maximum-schema update | actual2091 bytes; largest selected allocation3072; live0 |
| Outcome/duplicate probe | 19 gates pass, exit0 |
| Scratch query revert/reapply | 0/5 with expected signals, then5/0 |
| Scratch review-fix revert/reapply | expected10 failed gates, then all19 pass |
| Manifest/execution review | 43 blocks, 42 hashes; unique anchors/checkpoints0–3 |
| Root stage1 driver preflight | 4 files verified, no writes |

The independent source reviewer first inspected proposed code without the plan
or expected counts. The separate execution reviewer inspected all complete
blocks, scripts, source anchors, hashes and process rules. Their final reviews
were read-only. The planner executed the normal scratch validation above.

## Evidence and boundaries

Scratch source: `/tmp/p3b5-plan-xizlzd9a`; fresh stage1 archive:
`/tmp/p3b5-r2-stage1-8zjsd2gn`. Raw logs remain outside version control:
`/tmp/p3b5-r2-stage1-build.log`, `/tmp/p3b5-r2-stage1-red.log`,
`/tmp/p3b5-r2-legacy-red.log`, `/tmp/p3b5-review-red.log`,
`/tmp/p3b5-r2-green-build.log`, `/tmp/p3b5-r2-green.log`,
`/tmp/p3b5-r2-full.log`, `/tmp/p3b5-r2-checks.log`,
`/tmp/p3b5-r2-proof.log`. Runnable probes are frozen in the plan; temporary
artifacts need not survive for execution.

No production edits/builds, Git writes, metadata activation, migrations,
benchmarks or sanitizers. The protected certification document was accessed
by hash only; its SHA256 remains
`fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
Public packed schemas remain inactive. Criteria/CAS/index support and remaining
read/delete/metadata/migration work retain their later roadmap boundaries.
