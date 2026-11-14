# Phase 3 repair — planning review and validation

2026-10-25. Baseline8c0bdf6259d2dc33e540e28e82b9f8010750047d,
branch fix/phase1-safe-scalar-conversions. Planning only; no production edits,
staging, commits, public activation, migration, benchmarks or sanitizers.

## Disposition

The rejected one-shot plan is superseded by the user-approved inactive3A/3B/3C
split. The complete [3A candidate](2026-10-25-phase3a-packed-codec-plan.md)
is ready for human review. Approval/execution has not happened.
The [Phase3 index](2026-10-23-phase3-packed-records-plan.md) maps all17 original
findings and the arithmetic correction to candidate fixes or mandatory later
work. It does not describe3B/3C issues as closed.

## Independent reviews

Standards/executability: complete blocks, real harness APIs, exact build lists,
RED-before-implementation order and inactive boundary checked. A wrong prose
status name was corrected to DEC_ERR_INTERNAL. Root replay independently
verified all16 replacements match exactly once and nine resulting code/build/
test files are byte-identical to the isolated tested candidate.

Blind code/spec: no remaining codec findings. Ordinary independent probe
covered9332 successful numeric round trips and3418 clean assignment failures.
Review found invalid generation masking and missing free-pool splitting tests;
both were repaired and re-reviewed.

Final caller audit additionally found that value_compute legitimately rewrites
NEW metadata after initial prevalidation. A regression reproduced96passed/7failed
against the otherwise-green candidate. A guard now rejects invalid computed
flags before fallback staging; independent review confirmed the failure cleanup
has no published marker or staged hook state at that point.

## Isolated verification

Scratch archive: /tmp/phase3-repair-8mj00p4h/checkout from exact HEAD.
Source blocks extracted/replayed literally; shared production sources untouched.

- Stub codec RED:16/61.
- Pre-callback physical stub RED:11/32. Final stub physical count11/38 remains
  an explicitly unmeasured forecast, not a gate or evidence claim.
- Callback-specific RED against otherwise-green candidate:96/7.
- Final normal build:exit0, zero compiler warnings.
- Final focused codec:268/0.
- Final focused physical storage:103/0, including4MiB all-byte checks,
  exact disk vlen/compact offsets, reopen/strict walk, huge-schema tiny rows,
  donor-head and split-leftover reuse, invalid entry/computed generations.
- Final full normal suite:18816passed/0failed across518cases, exit0.
- Independent Python Decimal oracle:3000/0, seed20261025.
- Deterministic OOM probe:encode/compose/view each3allocation sites; every
  injected failure preserved output contracts.

Raw logs and probes remain in /tmp. A sandbox attempt could not create sockets
(EPERM) and produced7272/337; that environment failure is not counted as code
verification. The final full run had local socket access. An accidentally
overlapping scratch build was interrupted; its log is excluded from clean-build
evidence. These limitations are disclosed in the child plan.

## Scope and next boundary

3A supplies no public application-level numeric capability.3B must integrate
every actual-size consumer/adapter;3C must prove authoritative metadata,
publication durability, downgrade and pending-replay safety. Required Phase4–6
semantics and explicit activation approval remain gates.

Protected certification file contents were never read. Start/end hash:
fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.
The original rejected draft is archived byte-identically under plans/archive;
its historical code must not be executed.

## Fresh blind pass before executor handoff

A new review round used two fresh reviewers blind to the plan/history
(codec and transport), plus a separate executability reviewer. No actionable
3A code defect or execution blocker was reported. Independent ordinary codec
probes passed 10,000 mixed-schema and 19,462 constrained numeric roundtrips.

All 16 quoted replacements and all five complete source-block hashes were
rechecked; the nine final source/build/test files are byte-identical to the
previously full-suite-validated candidate. No redundant full suite was run:
this pass changed only prose formatting and an explicit socket preflight.
The documented socket preflight passed using the normal permitted environment.

The handoff pins plan SHA256
c35bab3dec21c560c71130aa69f36ab4d88c08d29d2569d13d84f785f90d9a9f.
Physical tests do not directly prove pending replay, actual compaction or
concurrent packed writes; these remain explicit later integration gates.
No production edits, staging, commits, sanitizers or benchmarks.
