# Type-correctness plan revision and independent review

Date: 2026-10-20. Source HEAD:
`1dd1888fda49ce295717590d1f65415963445270`.
Planning files remain uncommitted; this is not an implementation review.

## Requested corrections completed

- Replaced the all-child-plans-upfront gate with cross-cutting contract review
  followed by just-in-time child plans. Each production phase still requires
  a complete anchored code/test plan, human approval and raw-diff review.
- Corrected the proposed 25/26-coefficient-digit decimal field contribution
  to 37 bytes: descriptor 8, payload header 16, packed digits 13. Smaller
  coefficients use their actual lengths; shared record overhead is separate.
- Corrected the new raw varchar payload ceiling to 65535 bytes, excluding
  its new eight-byte descriptor and excluding the old two-byte prefix.
- Added independently computed maximum-schema value/slot sizing and aligned
  the acceptance matrix and executing-agent handoff with those equations.
- Wrote a copy-ready handoff scoped to Phase 0 investigation, contract proof
  and Phase 1 child-plan authoring, with no production edits authorized yet.

## Blind review method and result

A fresh reviewer received the repository paths and review assignment without
the author's conversation history, change rationale or expected findings.
The reviewer read repository rules and the parent, baseline, acceptance and
handoff documents, and spot-checked relevant current source.

**Result: no actionable P1/P2/P3 findings reported in that pass.** The reviewer
independently confirmed the 37-byte field calculation, 73744-byte decimal
payload maximum, and the following value/aligned-slot pairs:

| Schema at 256 fields | Maximum value | Maximum slot, max_key=1024 |
| --- | ---: | ---: |
| Bare numeric fields | 18880545 | 18881600 |
| Bare varchar fields | 16779041 | 16780096 |

Source spot checks supported legacy numeric parsing through double, ignored
precision, rejected bare varchar, float-widening byte-order mismatch,
whole-field trailing trimming, schema export's precision-18 reconstruction,
128-MiB segment limit and 544-byte B+ tree value-key cap. The test client's
response buffer grows dynamically.

This result means the planning package is consistent within the inspected
scope. It does **not** prove the proposed layout, migration ordering, key codec
or complete route behavior. Those remain explicit Phase 0 evidence obligations.
The reviewer did not execute builds, probes or the acceptance fixture during
the blind pass and did not open the protected certification document.

## Additional author-side source check and correction

After that pass, checking the pinned PostgreSQL source exposed a remaining
contradiction in the average contract. It said to adopt PostgreSQL's algorithm
but described a 16383-place calculation cap. PostgreSQL 18's numeric division
selection instead clamps calculation scale to 0..1000; the larger 16383 limit
applies to stored fractional scale. Corrected the parent and handoff to make
this distinction explicit, including possible rounding of very small averages
and the need for independent golden cases before implementation.

Primary evidence:

- [PostgreSQL REL_18_0 numeric.c](https://github.com/postgres/postgres/blob/REL_18_0/src/backend/utils/adt/numeric.c):
  numeric_avg, numeric_div_opt_error and select_div_scale.
- [PostgreSQL REL_18_0 numeric.h](https://github.com/postgres/postgres/blob/REL_18_0/src/include/utils/numeric.h):
  calculation-scale and significant-digit constants.

The correction preserves the requested storage range. Python-decimal
validation remains mandatory; a live PostgreSQL server remains optional.
This source check is distinct from the original blind pass.

The same independent reviewer then performed a separate follow-up check of
the changed paragraphs against the pinned PostgreSQL sources. They confirmed
the calculation/storage distinction, rounding policy, sizing and execution
boundaries, and reported **no new actionable findings**. This follow-up did
not run PostgreSQL or the generated decimal oracle; those remain Phase 0 work.

## Local verification

- Independent arithmetic assertions passed for field payloads, descriptors,
  maximum-schema nullmap/value sizes and aligned slot sizes.
- Markdown fence pairing and relative links passed checks.
- The complete acceptance C block was extracted to a temporary file and
  passed `cc -std=gnu11 -Wall -Wextra -Werror -fsyntax-only` against current
  test headers. This is syntax checking, not a passing daemon test.
- No production code, registered test, build configuration or dependency was
  changed. Production build/full-suite/sanitizer results are not claimed.
- The protected certification plan's SHA-256 is unchanged:

  ```text
  fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135
  ```

## What the executing agent receives

Use [the handoff](2026-10-20-type-correctness-executor-handoff.md). Its first
assignment is Phase 0, followed by a complete Phase 1 child plan and a stop
for human approval under CORE-PROCESS.md. It does not authorize implementing
all phases, committing/pushing, or running the separate certification rig.
