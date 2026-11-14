# Phase 1 fourth revision review

Reviewed 2026-10-21. No production edits, branches or commits. Protected certification plan SHA256 remains `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.

The four R3 corrections are present: complete-span scalar index parsing, uncapped leading-zero integer parsing, corrected accessor/function dispositions, and red-first execution order. The document's conversion-core and new regression blocks appear verbatim in the scratch sources. The supplied logs do report red 62 passed/52 failed, green 114 passed/0 failed, and full suite 17,486 passed/0 failed across 513 cases. Those results are real evidence for the subset actually applied, but approval remains blocked by two aggregate-path gaps.

## R4-01 — P1: the integrated scratch patch omits every query_aggregate.c change

Files: `/tmp/p1-int/src/db/query_aggregate.c`, `/tmp/p1-patch.py`; child-plan Task 4 and its integration evidence claims.

Compared all corresponding source files between the production checkout and `/tmp/p1-int`. query_aggregate.c is byte-for-byte identical to baseline. The patch script does not edit it. Consequently, the two planned unsigned-magnitude numeric renderer replacements and all four aggregate scale guards (two textual and two double-conversion consumers) were never compiled or executed in the claimed complete integrated patch.

Compiled a focused harness including the scratch tree's actual query_aggregate.c and linking its util.o. Actual behavior:

```text
raw_numeric=--9223372036854775808
raw_double=1.23457
```

The numeric case uses INT64_MIN at scale 0, multiplier 1. A separate invocation with value 1 and multiplier 0 terminated by signal 8 (SIGFPE; rtk exit 136). Thus the scratch binary retains precisely the aggregate defects Task 4 is intended to fix. The new 114-assertion case does not exercise these helpers and cannot detect their omission.

Required correction:

- Apply every planned query_aggregate.c hunk in the scratch integration script, with an explicit expected file/hunk manifest so a missing translation unit cannot be declared complete merely because all implemented substitutions matched.
- Add aggregate-path regression coverage for numeric scale-0 INT64_MIN rendering through both record and index text helpers, plus unsupported-legacy-scale fallback through all four aggregate consumers. A focused direct-helper test is appropriate for the legacy multiplier state that new schema validation prevents creating through supported declarations.
- Correct the evidence ledger to distinguish the earlier partial integration from the completed rerun. Rebuild and capture focused green and the normal full suite after adding the omitted hunks; preserve end-of-series sanitizer timing.

The Task 4 prose also calls the two division anchors textually identical, although one source occurrence includes `return AGG_VALUE_PRESENT;` on the same line and the other does not. Use the actual full anchors within their correctly named enclosing functions when adding the missing integration steps.

## R4-02 — P2: the plan's round-trip floating-output contract omits aggregate text renderers

Files: child-plan Task 6; src/db/query_aggregate.c `typed_field_to_buf_raw` FT_DOUBLE/FT_FLOAT (baseline lines 1096/1101), `decode_idx_to_buf` FT_DOUBLE/FT_FLOAT (1432/1442).

Task 6 updates only config.c's decode_field_to_buf. Both aggregate text helpers still use `%g`, even if the plan is applied literally and completely. They participate in public aggregate/group-by output. The probe above shows typed_field_to_buf_raw rendering the binary64 value 1.2345678901234567 as 1.23457, contradicting the stated 17-digit double/9-digit float output contract and leaving different response paths with different precision.

Required correction: add complete anchored replacements for both helpers' float and double formatters using the same 9/17-digit policy, and add aggregate/group-label regressions for both index-backed and record-backed paths. Include this file in the patch manifest and rerun the normal suite after the complete change. If the intended Phase 1 contract is narrower, that would need an explicit contract change; the current parent/child documentation promises round-trip floating output generally.

## Review evidence

Focused harness and logs: `/tmp/p1-r4-review-bdlrg3vy/` (probe.c, probe, normal.log, zero.log). The harness includes the actual scratch query_aggregate.c; only the thread-local global is provided by the harness. The compile succeeded after correcting the harness invocation to pass the helper's existing fifth keep_zero argument. No scratch implementation was changed by this review.

Read the existing supplied red/green/full-suite logs; did not rerun an unchanged full suite, since the source comparison and focused failures already establish the missing coverage and patch omission.

Next handoff: close these two aggregate gaps, provide a complete changed-file/hunk manifest and matching scratch diff, then return the focused and normal full-suite results for final review. Do not execute in the production tree, commit, or touch the certification plan as part of these review corrections.
