# Phase 2 — fresh independent blind review

Date: 2026-10-23. Baseline HEAD: `f5b80b4c58e49cfd8ac8b74ed9c1366f7b3a3130`.
Disposition: **one remaining P2 finding; defer cleanup/commit until corrected**.
The previous average-result and NULL-parse findings remain closed.

## Independence and scope

A newly spawned reviewer received no conversation history and did not read
plans, earlier reviews, evidence or halt notes. It reviewed the raw build diff,
public decimal header, implementation and tests against repository standards.
No project edits, git writes, sanitizers, benchmarks or full-suite runs were
performed. The primary independently reproduced the finding afterward.

## BLIND-01 [P2] Bound completed values returned by public rounding

Location: `src/db/decnum.c:591–595`, normalization and unconditional success
return from exported `dec_round_scale`; public contract in `src/db/decnum.h`.

The completed-average API now checks its rounded result, but the exported
rounding API itself can return success outside the supported domain. Two
fresh ordinary-build reproductions against valid parsed values:

1. Parse 131072 nines followed by `.5`, then round directly to scale 0.
   Parse and rounding return `DEC_OK`; the result has **131073 integer
   digits**. Packing succeeds, but unpacking returns `DEC_ERR_RANGE` (3).
2. Parse `1`, then round directly to scale 16384. Rounding and packing return
   `DEC_OK`; unpacking returns `DEC_ERR_PAYLOAD` (7) because display scale
   exceeds `DEC_MAX_DSCALE`.

The first case uses an ordinary scale and valid input, independent of any
argument about unsupported scale inputs. The second exposes the missing
public scale restriction as well. The header explicitly permits temporary
negative display scales for this primitive, but does not document arbitrary
positive over-limit outputs. The module claims checked digit limits.

Correct the public rounding boundary to reject unsupported final values and
leave canonical zero on failure, and specify its scale domain explicitly.
An internal unchecked rounding helper may be needed: constrained assignment
must preserve its existing post-round precision-overflow classification.
The primary confirmed that the first input assigned to numeric(1,0) currently
returns `DEC_ERR_PRECISION` (4), with cleared output; a new generic range error
must not accidentally replace that established contract. Preserve documented
negative-scale rounding behavior as well.

Required regression coverage: direct positive/negative rounding carry at the
integer ceiling, its valid neighbor, display scale at and above the ceiling,
canonical output on failure, unchanged constrained precision errors, and
negative-scale behavior. Capture regression RED before changing production.

## Checks and limits

- Current focused suite: **909 passed, 0 failed** (reviewer rerun).
- Fresh differential scratch checks: **12000 arithmetic/rounding cases** and
  **12000 precision/serialization cases** matched Python Decimal. These did
  not replace the separately reproduced ceiling tests.
- Primary reproduction artifacts: `/tmp/p2-blind-round-boundary-7oj6vauq/`.
- Allocation failures inspected, not fault-injected during this blind pass.
  Portability inspected, not executed on other architectures/OSes.
- All four source/build/test hashes stayed unchanged during the review.
- Protected plan checksum remained
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`;
  the primary verified hash only and the blind reviewer read no plan files.

This is a review result, not authorization to modify code or commit. Amend
and approve the correction before implementation; preserve the earlier
review/evidence documents as historical records.
