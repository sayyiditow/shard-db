# Phase 3B.1 — blind review and Revision 2 fixes

2026-10-25. Review target: proposed adapter patch extracted from the Phase 3B.1
plan, relative to b9cea93fd7e314eeb7505675e7d2212da850e184. No implementation
commit exists. Production sources were not modified.

A fresh reviewer received raw extracted code and repository standards, without
the originating plan or previous reviews. A second fresh reviewer checked the
written contract and execution scope separately. The planning agent also
probed legacy wrapper boundaries. All probes stayed under /tmp; no sanitizers,
benchmarks, staging, commits or pushes occurred. The certification plan was
never read or edited.

## Standards / blind correctness

- **P2 — Packed scalar errno:** fixed scalar conversion could fail with errno=0
  despite the adapter header's error promise. A shared private row_encode_fixed
  boundary now resets stale errno, translates validation failures to EINVAL,
  and preserves ENOMEM. Both packed setters and legacy update conversion use it.
- **P3 — Enum TYPE suffix:** the binder delegated to a legacy parser that ignores
  text following the final closing parenthesis. It now rejects trailing text
  before parsing, without mutating the schema.

Re-review: both reproductions pass; no further concrete memory-safety, ownership,
span, output-budget or atomicity defect found within reviewed preconditions.
No actionable heuristic code smell was reported.

## Specification / contract

- **P2 — TYPE-only binding:** enum modifiers/junk were accepted despite the
  TYPE-only input contract. The suffix guard and schema-preservation regressions
  resolve this finding (overlaps the blind review's enum finding).
- **P2 — Legacy failure contract:** malformed OLD returned -1 with no errno or
  message. The new wrapper reports EILSEQ; invalid supplied spans and scalar
  values report EINVAL; owned outputs remain NULL/0 on failure.

Re-review: both reproductions pass, including expanded stale-errno cases. No
additional confirmed decimal, retained-default, actual-size or legacy carry
defect was found. Production consumers remain deliberately outside this child.

## Additional planning-agent reproductions

- **P2 — NULL JSON:** legacy delegation dereferenced NULL. The adapter now rejects
  it with EINVAL before calling the old encoders, for both defaults modes. The
  proposed regression isolates any recurrence in a child process so a crash
  cannot terminate the rest of the case.
- **P2 — Enum parser allocation status:** the legacy void parser has no status
  result; allocation failure was translated to EINVAL. The binder now clears
  and captures errno around that parser, retains ENOMEM across cleanup, and
  preserves schema bytes. A scratch probe fails all three enum parse allocations.

## Test-first evidence and disposition

Thirty-three regression assertions were added before fixes. Against the previous
candidate, the complete case was **201 passed / 13 failed, exit 13**, zero build
warnings. Failures reproduced the reported errno, enum and isolated NULL-crash
mechanisms. The parent-only fork assertion avoids duplicate child TAP output;
its parent assertion and expected result are unchanged.

Revision 2 updates the complete core/test/view blocks and their checksums in
[the child plan](2026-10-25-phase3b1-typed-row-adapter-plan.md). The scratch probe
also asserts ENOMEM for every injected allocation failure. Current staged
RED/GREEN, full-suite and allocation results are recorded in that plan's
Appendix G; historical Revision 1 results are explicitly nonbinding.

All findings above are resolved in the proposed patch and independently
rechecked. The final exact-block replay is complete: 214/0 focused, 19030/0 full suite
across 519 cases, zero warnings, and all 56 allocation-failure injections pass. This report does not approve production activation or complete Phase 3B.

Standards: 2 findings resolved (worst: P2 scalar errno). Specification: 2
findings resolved (worst: P2 TYPE/failure contracts). Additional planning-agent
NULL-input and enum-allocation reproductions are resolved as recorded above.
