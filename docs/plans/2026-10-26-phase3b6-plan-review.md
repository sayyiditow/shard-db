# Phase 3B.6 independent plan review — revision 2

Plan: [whole-record JSON readers](2026-10-26-phase3b6-whole-record-readers-plan.md).
Final SHA256: `770505911d06179f19276b952ae7f94ada52c0e06df910ae7a3034d27f20fed9`.
Base HEAD: `35e49ab8cd7a7efe7350562e34eaeac62ea4afae` on
`fix/phase1-safe-scalar-conversions`.
Planner/reviewer: GPT 6.1 Sol. Intended executor: GLM 5.3.

## Standards and independent blind source review

No production correctness defect or hard standards violation found in the
proposed decoder/GET implementation. The independent reviewer inspected source
differences and actual callers without reading the plan or expected counts.
It checked schema dispatch, generation rejection, allocation ownership, legacy
rendering, FILE failures and GET error/newline handling. Its independent narrow
runs passed the original focused167 checks and original102-gate source probe.

The planner also identified a fixture safety issue: after failed mkdtemp the
new regression continued with an invalid path. Revision 2 now frees owned
fixture input/schema and returns before storage initialization or recursive
cleanup. The blind reviewer independently confirmed this correction.

Probe hardening additionally requires all four successful allocation sweeps to
be nonempty and compares complete packed JSON, including GET newline, before
using its verified body length for newline-failure injection. The independent
source re-review found no new findings or vacuous assertions. Final hardened
runtime results below were run by the planner.

## Plan/spec and execution-readiness review

Two findings were corrected; independent re-review found none remaining:

- **P2: memory disclosure.** The original proposal understated legacy GET
  memory as final output plus NUL. The unchanged legacy allocator instead
  reserves est=4+sum(name length+need+4), with need5 for NULL, 6*n+3 for varchar,
  6*n+4 for enum and512 for other scalar fields. A heap field workspace can
  coexist during rendering. The revised plan/docs distinguish that conservative
  capacity and workspace from exact packed output allocation. The FILE API
  comment no longer promises exact sizing. No unplanned allocator rewrite.
- **P3: protected hash timing.** The original start/end-only wording conflicted
  with the driver's stage revalidation. Revision 2 explicitly permits repeated
  hash-only verification and adds a terminal hash command to Appendix A.

All23 blocks /22 manifest hashes, committed parent hashes, unique anchors and
checkpoints0–3 reproduce independently in memory. Bootstrap/driver/checks Python
syntax passes. Nine owned final files are frozen; source equals tested scratch.
The plan preserves the inactive public format and lists the unconverted
projection/field-string/query/index/maintenance callers explicitly.

## Final normal scratch validation

| Gate | Result |
|---|---|
| Baseline normal build | exit0, no warnings |
| Final focused RED | 124 passed /38 failed, exit38 |
| Final proposed build | exit0, no warnings |
| Final focused GREEN | 167 passed /0 failed |
| Fresh full suite | 20,045 passed /0 failed across524 cases, exit0 |
| Selected allocation injections | 22: legacy direct3/GET3, packed direct8/GET8 |
| Hardened probe | all106 gates pass, live0 |
| Maximum-schema row | actual2085; largest selected allocation2966 |
| Baseline probe | exactly16 expected failed gates, exit1 |
| Scratch revert/reapply proof | baseline16 defect gates, then all106 pass |
| Root stage1 --check-only | two files verified; no writes |

Raw logs stay outside version control: `/tmp/p3b6-r2-stage1-{build,red}.log`,
`/tmp/p3b6-r2-{build,green,full}.log`, `/tmp/p3b6-r2-probe-{red,green}.log`,
`/tmp/p3b6-r2-proof.log`. Proposed source:
`/tmp/p3b6-plan-a4g9aeu_`; baseline replay:
`/tmp/p3b6-stage1-8v0jvemc`. The complete runnable checks are frozen in the plan,
so executor reproduction does not depend on keeping these temporary artifacts.

These tests verify adapters, real packed physical bytes/reopen, and the actual
source-extracted GET body with local dependency stubs. They do not establish
public metadata binding, network integration or registry concurrency acceptance.
Existing registered tests exercise the public legacy behavior in the full suite.

## Hand-back boundary

Review fixes are complete; ready for user approval/executor handoff. No production implementation,
build, staging, commit, push, sanitizer, benchmark, migration or public activation
occurred during this review. Production HEAD remains35e49ab. The protected
certification plan was verified by hash only and remains unchanged at
`fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
Executed raw-diff review is still mandatory after implementation.
