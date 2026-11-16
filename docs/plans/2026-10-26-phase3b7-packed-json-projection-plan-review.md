# Phase 3B.7 — blind plan review and repair

Base: decbee6a18fc950bbf18e2f4ce6ad68956cad554 on
fix/phase1-safe-scalar-conversions. Production implementation remains absent.
Planner/reviewer: GPT 6.1 Sol. Intended executor: GLM 5.3, per user assignment.

## Standards

Fresh independent review checked the frozen driver, tests, code, source anchors,
manifest hashes, build wiring and allocator/FILE probe boundaries without the
planning conversation. No actionable standards/executability finding. A second
pass over Revision 2 independently replayed all 29 blocks, unique anchors and
checkpoints and found no additional issue.

The probe runs the actual extracted command body and actual selected modules.
Read/schema/registry dependencies are mocked; FILE operations remain real.
Mocked raw-record allocation and libc FILE internals are outside the allocation
sweep, as explicitly documented. No end-to-end network or concurrency claim.

## Spec

**SPEC-01 (P2), resolved — fixed logical width was not validated.**
The existing packed_schema_ok compared directory width with TypedField.size,
without checking the logical type's decoder width. A one-byte bound bool field
whose logical type became FT_LONG could pass the view guard and reach an
eight-byte scalar read. Enum carrier/count/table inconsistencies also escaped
the guard. This requires inconsistent in-memory schema metadata; no claim is
made that valid public binder output normally produces it.

Revision 2 adds packed_fixed_width and the shared guard check. Widths match
config.c parse_field_type; unknown fixed logical types reject, and enum width
must agree with count with a present table. Aggregate type_desc.int_width is
not used as a storage-width table. The shared guard protects prepared rows,
field text and full-record decoding as well as the new projections. Valid bound
schemas, legacy branches and all storage formats stay unchanged.

The complete code and 48 new assertions are frozen in the amended plan, with
an additional command probe for an inconsistent unselected fixed field. RED
uses sufficient backing allocation while retaining the exact short record
length, so the regression demonstrates the rejected span without requiring a
read beyond its C allocation. Restored valid metadata is asserted readable.

## Verification

- Guard-only focused RED on complete Revision 1 implementation: 175 passed,
  43 failed; the corresponding command probe gives 121 passing/1 failing gate.
- Repaired focused case: 218 passed, 0 failed, zero-warning normal build.
- Revised probe: 122 passing gates, zero failures; all 31 allocation injections
  retain cleanup/error contracts. Reversal/reapply reproduces 22 passing/20
  failing gates at checkpoint1, then 122/0 at checkpoint2.
- All nine final source/document hashes match the plan. Root driver stage1
  --check-only verifies four files without writes; all 29 block hashes and
  Python scripts are checked. Editorial RED exit references are pinned to176.
- Fresh complete-plan RED: 40 passed/176 failed, exit176; fresh RED probe22/20.
- Fresh normal full suite: 20263 passed, 0 failed across525 cases, exit0.
- No open findings after the independent repair review and runtime verification.

Raw logs/probes/builds stay under /tmp/p3b7-r2-* and outside version control.
No sanitizer, benchmark, production source edit, staging, commit, push, PR,
metadata binding, migration or certification execution occurred in this review.
The protected file is accessed only by hash and remains unchanged.

## Final checkpoint

Reviewed Revision 2 plan SHA256:
1f45c320abe0e890ae7011bdbc93646bbe8e00733be1cecaf92e310fc72c24ae.
Protected hash rechecked at finish:
fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135.
All 29 frozen blocks/Python scripts/unique anchors/checkpoints verified.
Root source HEAD remains decbee6; only the child plan, this review and Phase3
work-index planning edits are pending. Next: executor follows amended Tasks0–3
and stops unstaged/uncommitted for executed raw-diff reviews.
