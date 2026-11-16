# Phase 2 completed implementation — raw-diff review

Date: 2026-10-23. Baseline: `f5b80b4c58e49cfd8ac8b74ed9c1366f7b3a3130`.
Reviewed the actual unstaged `build.sh` diff and new `decnum.h`, `decnum.c`,
`test_decnum_core.c`, and execution evidence. Scope excludes the protected
certification plan; only its checksum was verified.

Disposition: **two issues require correction before commit**. Both exist in
the approved plan itself; the executor followed that plan. No implementation
or plan code was changed during this review.

## Standards and independent blind review

The blind reviewer inspected the raw code, header contract and repository
rules without reading the Phase 2 plan or its evidence.

### RAW-01 [P2] Reject final average overflow after rounding

Location: `src/db/decnum.c:777–780`, after `dec_round_scale` in `dec_div_avg`.

A valid input containing 131072 integer nines, a decimal point and 1001
fractional nines, divided by count 1, selects display scale 1000. Rounding
carries into a 131073rd integer digit. The function returns success without
rechecking the bare integer limit. Packing also returns success, but the
module rejects that same payload on decode.

Ordinary scratch reproduction against the actual module:

```
parse_status=0
average_status=0
integer_digits=131073
display_scale=1000
pack_status=0
unpack_status=3  # DEC_ERR_RANGE
```

Required correction: validate the final rounded average against the supported
bare limits, return `DEC_ERR_RANGE`, and clear the output to canonical zero.
Add a regression that fails on the current implementation, checking both the
error and output invariants, with a neighboring valid boundary. The low-level
rounding API intentionally allows temporary negative display scales; do not
silently change that separate contract as part of this fix.

### RAW-02 [P2] Clear reused parse output before rejecting NULL text

Location: `src/db/decnum.c:173–174`, initial argument validation in `dec_parse`.

The header promises canonical zero on every output-producing failure except
alias rejection. Parse `42` into an initialized output, then call
`dec_parse(NULL, 0, &out, NULL, 0)`. The NULL-input check returns before the
existing output is released/reset:

```
status=8  # DEC_ERR_INVALID
sign=1
ndigits=1
first_limb=42
allocation_retained=true
```

Required correction: reject a NULL output pointer first, release/reset a valid
initialized output, then reject NULL text. Add a reused-output regression
checking canonical mathematical zero, scale and released storage; preserve
existing NULL-output validation. Retaining the old allocation here is a
contract violation, not evidence of an unconditional memory leak.

No additional actionable documented-standards breach or baseline code-smell
finding was identified. The blind reviewer also checked 15000 randomized
averages and 25000 randomized precision assignments against Python Decimal;
those ordinary-value checks passed and do not cover the failing maximum.

## Spec review

The separate plan-aware reviewer confirmed both defects independently:

- **SPEC-01 [P2], same defect as RAW-01:** the frozen bare-numeric contract is
  131072 integer digits and 16383 fractional digits. A successful average
  returning 131073 integer digits violates it and cannot round-trip through
  the frozen packed representation. The missing check is present in the
  approved plan too.
- **SPEC-02 [P2], same defect as RAW-02:** the lifecycle contract says that on
  every failure the output is canonical zero, except aliases. NULL text with
  a valid reused output violates it. The plan contains the same guard order.

Otherwise the header and complete unit case match the plan's fenced blocks;
production functions and the five build edits match the amended plan.
The two reported execution deviations (illustrative RED failure text and
one blank-line residue) do not affect behavior.

## Verification and boundaries

- Current focused case: **888 passed, 0 failed** (independently rerun by the
  spec reviewer).
- Existing independent-oracle case/output files rechecked: **426/0**.
- Executor's recorded full suite: **18404 passed, 0 failed across 516 cases**;
  this review did not rerun the full suite or a full build.
- Both findings reproduced independently by the reviewers and by the primary
  agent against an ordinary scratch shared-library build of the actual code.
  Primary reproduction artifacts: `/tmp/p2-raw-review-hh186e7e/`.
- Approved plan SHA256:
  `8f508ff99b80bf4e885e048a4e16b219b1067bf669c533b267688bf62081ff88`.
- Protected plan SHA256, hash-only:
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
- No source/build/test edits, sanitizer runs, benchmarks, git writes, or
  protected-plan content access. Raw scratch probes/logs remain outside git.

Standards/blind axis: **2 P2 findings**, worst RAW-01. Spec axis: **2 P2
findings**, worst SPEC-01. These are the same two defects, not four distinct
issues. Amend the plan, approve the fixes, capture regression RED before the
fixes, then rerun focused/oracle/OOM/full-suite checks and raw-diff review.
