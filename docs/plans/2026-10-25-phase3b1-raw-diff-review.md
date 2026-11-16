# Phase 3B.1 Revision 2 — executed raw-diff review

2026-10-25. Base/HEAD b9cea93fd7e314eeb7505675e7d2212da850e184 on
fix/phase1-safe-scalar-conversions. Implementation remains unstaged/uncommitted.
No source changes, rebuild, full-suite rerun, sanitizers, benchmarks or Git
writes were performed during this review. A fresh blind code reviewer and
a separate plan-aware compliance reviewer inspected the actual worktree,
including untracked source files. Protected certification content was never
read; its hash remains exact.

## Standards / blind correctness

### P2 — Unknown enum members silently become the first member

Location: src/db/typed_row.c:224, typed_prepared_set's fixed-field branch.
It calls row_encode_fixed, which delegates to encode_json_field_checked and
encode_field_checked. Those lower-level helpers do not enforce enum membership.
The legacy insert JSON collector performs that validation separately. The new
packed collector never performs it.

A fresh reviewer compiled actual typed_row.c separately without LTO, linked
against the current built archive, and reproduced:

```text
legacy encode unknown: -2
bind: 0
case={"f0":"bogus"} rc=0 errno=0 result={"f0":"red"}
case={"f0":"bogus","f0":"blue"} rc=0 errno=0 result={"f0":"blue"}
case={"f0":true} rc=0 errno=0 result={"f0":"red"}
```

Schema is enum(red,blue). This loses the supplied value and lets an invalid
earlier duplicate escape validation. The current plan/header require checked
fixed-scalar semantics and validation of every supplied occurrence. Existing
public insert behavior already rejects the nonempty unknown literal, so this
is an adapter compatibility gap that belongs here, not a new enum-policy
project to defer until Phase 7. Packed storage remains inactive, so public
objects do not yet route through this defective adapter.

Required fix: validate decoded nonempty enum text using enum_value_index before
encoding/replacing the cell, with a useful field error and EINVAL. Preserve
existing empty/null policy rather than silently expanding scope. A rejection
must preserve prepared cell, owner, seen marker and byte count.

Required regressions: unknown enum input, invalid earlier duplicate followed
by a valid member, decoded escaped valid member, direct setter failure
atomicity/default eligibility, and the affected update/default adapter paths.
The planner must amend the plan with exact hunks and RED-first instructions;
then execute the approved fix and rerun focused, normal full-suite and relevant
allocation probes before re-review.

Probe source/executable remain outside version control:
/tmp/p3b1_raw_blind_probe.c and /tmp/p3b1_raw_blind_probe. The primary reviewer
reran the reproduction and confirmed the output. No additional actionable
bounds, ownership, output-budget or hard standards issue was identified.

## Specification / literal execution compliance

No execution deviation found. Approved plan SHA256 remains
6c5c757ae34e50d0bb2cb6ab0e5da6422da62e01bab3be7ad4783bcfc1cae725.
All 37 block IDs are unique; all six immutable block hashes match. Reconstructing
the 13 unique anchored changes from the pinned base produces byte-identical
build.sh, config.c and types.h. Final adapter/header/test and both concept docs
also match exactly. Eight implementation/doc files were checked, including
untracked files. All five build lists and test registration are present.

No loader/create-object caller binds packed schemas; storage/query/index/bulk/
rebuild sources are unchanged. Protected hash:
fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.

Executor logs under /tmp/shard-p3b1-exec-psUgfw support the evidence ledger:
zero build warnings/errors; RED 13/23, RED 84/34, focused GREEN 214/0, full suite
19030/0 across 519 cases and 56 allocation injections. Full-suite log has zero
not-ok/EPERM events. Primary review reran the focused case: 214/0, exit 0.
These passing tests do not cover the enum counterexamples above.

## Original disposition (superseded by Revision 3 below)

Standards/correctness: 1 unresolved P2 finding (enum coercion). Literal plan
compliance: 0 findings. The executor followed the approved plan; the missing
validation is a plan/design gap. Not ready for cleanup/commit until the fix
and its regression evidence have been reviewed. This review changes only this
report, preserving implementation and prior evidence for inspection.


## Revision 3 fix and final disposition — 2026-10-25

The user authorized the planner to amend and execute the small fix. Appendix H
now contains exact regression and implementation hunks, applied RED-first.
The original P2 finding is resolved: decoded nonempty enum members are checked
before fixed-field allocation/replacement; invalid input reports the field and
EINVAL, preserving cell ownership, contents, seen marker and byte count.
Empty-string and NULL contracts remain intact. All original assertions remain.

Measured results: RED 232/24 (exit 24), GREEN 256/0 (exit 0), warning-free builds,
normal full suite 19072/0 across 519 cases (exit 0), allocation probe 56/56
(exit 0, no tracked leaks). Complete evidence is in the typed-row evidence
ledger; raw logs remain under /tmp/p3b1-enum-fix/.

Independent blind re-review inspected actual code/tests without reading the
plan and recompiled its original three probes against current typed_row.c;
each rejects with EINVAL and NULL output. No remaining actionable finding.
Separate plan-aware re-review found no execution deviation or scope drift;
exact R2-plus-H correspondence and all frozen checksums were verified.

Final disposition: correctness/standards 0 unresolved findings; specification
0 unresolved findings. Ready for cleanup/raw-diff approval and commit when
explicitly requested. Packed binding remains declaration/definition only,
protected hash remains exact, all work remains unstaged/uncommitted, and no
sanitizers, benchmarks or Git writes occurred.
