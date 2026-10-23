# Phase 2 child-plan review — revision required

Date: 2026-10-22. Reviewed plan:
`2026-10-22-phase2-decimal-core-plan.md` (3020 lines), SHA256
`32f232f2ba79f333b57af098a5429cf8a79a246f335ba4c59b5d3874d818487f`.
Base/HEAD: `f5b80b4c58e49cfd8ac8b74ed9c1366f7b3a3130` on
`fix/phase1-safe-scalar-conversions`. Tracked production diff is empty.

**Verdict: do not approve execution yet.** The broad design and phase boundary
match the agreed roadmap, but extracted-code checks reproduced correctness
failures and a literal execution failure. Fix the plan and its regression/oracle
coverage before implementation. The two independent review axes below overlap
where the same implementation defect violates both correctness standards and
a frozen requirement; their severity assessments are retained separately.

## Standards

Independent reviewer findings (four, worst P2):

### STD-01 — P2: zero operands corrupt fractional addition/subtraction

Plan lines 1756–1757 and 1877–1910. `dec_cmp_mag` compares weight before
recognizing zero. Canonical zero has weight 0 and is wrongly larger than a
fraction with negative weight. The signed subtraction path underflows and
assigns sign ZERO to nonzero limbs. Unsanitized extracted-code reproduction:

```text
0 + 0.5: DEC_OK, sign ZERO, two limbs, text 9999.5
0 - 0.5: DEC_OK, sign ZERO, two limbs, text 9999.5
```

Handle zero operands explicitly before magnitude comparison. Regress both
operand orders, both signs, addition/subtraction, scaled zeros and repeated
accumulation starting at zero. The prose after Task 4 currently asserts this
path is correct; correct that explanation too.

### STD-02 — P2: exported rounding breaks canonical comparison/hash invariants

Lines 1946–1999. `dec_round_scale` returns after truncation without normalizing.
Rounding 0.1 (also 0.49) to scale 0 leaves sign POS, weight -1, ndigits 0.
It renders 0 but compares greater than canonical zero and hashes differently.
Trailing zero limbs can likewise survive rounding. Normalize successful
results and test the exported function directly; wrappers that normalize
later do not establish the exported API invariant.

### STD-03 — P2: inconsistent output lifecycle and failure guarantees

Lines 1833, 1879, 2017, 2127 and 2275 reset allocated outputs with
`dec_init(out)`, losing owned storage on reused outputs. Parse explicitly
supports reuse, but other output APIs have no consistent fresh-output-only
precondition. Moreover lines 184–185 promise canonical-zero partial outputs,
while precision overflow returns DEC_ERR_PRECISION with 100.0 still in out
for 99.95 assigned to numeric(3,1).

Specify and implement a single initialized/reused-output and failure contract.
Test reuse through zero/cancellation/precision/average/unpack paths, validation
failures and allocation failures. Respect alias checks before releasing input
storage. Do not simply document contradictory behavior as safe.

### STD-04 — P2: literal Task 2 leaves duplicate dec_int_digits

Lines 1410–1415 instruct replacing only normalize, parse, text_len and to_text.
The text block at 1673 also defines dec_int_digits, whose stub at 564 remains;
Task 3 explicitly assumes the production definition already landed.
Following the replacement list literally produces:

```text
error: redefinition of ‘dec_int_digits’
note: previous definition of ‘dec_int_digits’
```

Explicitly include the fifth stub replacement, with an unambiguous anchor.
Validate the actual task-by-task edits, not only an assembled final module.
No material Fowler heuristic finding was reported.

## Spec

Independent reviewer findings (five), plus root oracle/hash findings (two).
Total seven, worst P1:

### SPEC-01 — P1: zero breaks required exact addition/subtraction

Same mechanism and evidence as STD-01. The frozen exact add/subtract
requirement is violated by ordinary valid inputs. This is a blocker before
execution approval, regardless of the existing test assertion count.

### SPEC-02 — P2: packed decoding bypasses frozen validation

Lines 2302 and 2307–2321. The integer-limit check only runs for m>0. A canonical
131073-digit coefficient with m=0 decodes with DEC_OK and integer_digits=131073;
131074 coefficient digits with m=-1 also yield 131073 integer digits and succeed.
The general bound is nd+m<=131072, without the positive-exponent guard.

The decoder also never checks the unused low nibble when nd is odd. A one-digit
payload whose final byte is 0x1F is accepted as 1, despite the frozen zero-pad
requirement. Validate all payload fields before constructing a dec_t; add
negative tests for all exponent signs and odd pad nibbles, plus exact valid
boundaries. Include canonical zero header validation in that audit.

### SPEC-03 — P2: public round violates signed-zero/equal-hash contracts

Same direct-operation reproduction as STD-02. Every publicly returned value
must meet the canonical invariants used by comparison, hashing and packing.
Tests limited to apply_precision/div_avg wrappers miss this requirement.

### SPEC-04 — P2: output/error promises disagree with implementation

Same reuse and precision-overflow evidence as STD-03. The handoff requires
explicit ownership/error contracts; implement and test the chosen contract
rather than leaving different operations with incompatible behavior.

### SPEC-05 — P2: Task 8 requires explicitly prohibited sanitizers

Execution rule at 292–293 forbids sanitizer builds in this phase. Nevertheless
2910–2924 compile/run the oracle under ASan+UBSan and 3014–3015 make sanitizer
cleanliness a completion condition. The handoff prohibited sanitizers, not
merely the final full-branch sanitizer gate.

Remove sanitizer flags and required sanitizer outcomes from Task 8, summary
and milestone; use ordinary focused validation here. Preserve the disclosed
planning-time run as historical evidence, including that it was outside the
handoff instructions. Do not erase the evidence or count it as the final gate.
Final ASan+UBSan/TSan timing remains unchanged.

### SPEC-06 — P2: the oracle rejects correct fractional cancellation payloads

Lines 2802–2816 retain the Decimal zero exponent when add/sub cancels. For
1.23 + (-1.23), Decimal produces exponent -2; the oracle clears the coefficient
but leaves m=-2. The canonical zero payload written by C correctly has m=0.
Adding this case to the supplied scratch harness produces:

```text
FAIL id=0 op=add: payload hex mismatch
0 passed, 1 failed of 1
actual: 0.00, canonical zero payload exponent 0, display scale 2
```

Canonicalize the expected zero coefficient exponent independently of display
scale. Add cancellation/identity cases to the generated and committed tests,
including fractional operands. Fix the oracle according to the contract, not
by altering correct C serialization to match a mistaken expectation.

### SPEC-07 — P3: hash rationale and algorithm labeling need correction

Lines 264–267, 1784, 2469 and 2997–2999. Rejecting xxh128 because it necessarily
pulls in storage.c is factually wrong: the existing vendored xxhash.h can be
used standalone with XXH_INLINE_ALL. A scratch cc -Isrc/db compile using
XXH3_128bits passed without storage.c or a new runtime dependency. FNV remains
a possible design choice, but justify that actual tradeoff honestly.

The chosen offset 1469598103934665603 also differs from the documented FNV-1a
64 offset 14695981039346656037. C and oracle repeat the same nonstandard value,
so their agreement does not validate the FNV label. Use the intended standard
constant and independent known vectors, or explicitly specify a custom-seeded
hash. Reference: https://datatracker.ietf.org/doc/html/draft-eastlake-fnv-08
(section 5). Equal-value hashing alone is not evidence of standard FNV output.

## Alignment with the discussion and remaining decisions

- The full bare range, constrained P/S ranges (including S>P/negative S),
  separate display scale, preserved trailing zeros, PG18 average-scale policy,
  frozen packed payload and core-only scope match the discussion in intent.
- All four build anchors were independently found exactly once at f5b80b4.
  No premature consumer wiring is proposed.
- **Small-value performance evaluation is missing.** The later conversation
  asked the planner to evaluate checked 64/128-bit common-value paths while
  keeping the frozen storage format. This plan uses heap-backed base-10000
  limbs throughout; __int128 division remainder is not that evaluation.
  Add a concrete comparison and recommendation, or explicitly justify deferral.
  Implementation of fast paths was not mandated; correctness tests do not
  prove the new core faster. Do not run benchmarks on the user's behalf.
- Decision 1: uniform rejection of zero text beyond the display-scale limit is
  consistent with the agreed bounded display scale; keep it in the revised plan.
- Decision 2: max operand display scale for addition is consistent with the
  selected semantics; no need to reopen it.
- Decision 3: the seven-significant-digit exponent policy is an explicit new
  lexical rule and can be approved with the revised plan, provided the exact
  cap/leading-zero behavior is tested and not advertised as complete PG input
  compatibility. It does not reduce the agreed stored numeric range.
- Decision 4: settle the hash using corrected rationale/constant above; do not
  silently swap hash algorithms during implementation.
- Decision 5: keep scratch oracle/probe/log artifacts out of version control,
  as already instructed. Complete reproducible sources in the plan are fine.
  This is not a reason to ask the human again about committing debug artifacts.
- Decision 6: linking the unused module into daemon/lib/test now is consistent
  with planned build verification and introduces no new runtime dependency.

Future Phase 6 consideration (not a blocking Phase 2 contract defect): storage
range and aggregate intermediate range need separate explicit policies.
Adding two maximum stored integers overflows this core's add limit although
their average fits; later cancellation can similarly depend on partitioning.
The parent explicitly permits propagated overflow/resource errors and has not
frozen wider accumulator limits. Decide/test this in the aggregate child plan
(and expose any necessary core extension) rather than claiming unlimited
intermediate arithmetic. Up to 20 extra decimal digits would cover uint64
row-count growth without changing the packed storage format.

## Verification performed for this review

Two independent read-only reviewers examined the actual plan. Each confirmed
key arithmetic/payload defects in its own ordinary scratch build. The root
independently extracted the supplied C, probe and Python script into
`/tmp/p2-root-review/`, with no production changes or test registration.
The final-module assembly deliberately replaced all stubs, including
int_digits; a separate literal Task 2 build exposed STD-04.

```text
ordinary cc build of extracted core and supplied probe: exit 0
supplied oracle: wrote 411 cases; 411 passed, 0 failed of 411
extra identity cases: 0+0.5 / 0-0.5 fail with 9999.5
extra cancellation case: 1.23 + -1.23 exposes oracle payload mismatch
odd padding 0x1f: DEC_OK (should reject)
131073 integer digits at m=0: DEC_OK (should reject)
131073 integer digits at m=-1: DEC_OK (should reject)
round(0.1,0): DEC_OK, text 0, cmp_zero 1, hash_equal false
precision overflow: DEC_ERR_PRECISION, output still 100.0
literal Task 2: compile exit 1, duplicate dec_int_digits
```

No sanitizers, benchmarks, production build/full suite, git writes or protected
plan content reads were performed. Only this review document was added by the
root. Protected SHA256 still matches
`fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.

Next: revise the plan/code/tests/oracle for all findings, add the performance
assessment, replay literal task steps in scratch with ordinary compilation,
then return the revised plan for review. Do not implement or commit Phase 2 yet.

Standards: 4 findings, worst P2. Spec: 7 findings, worst P1 (with overlap).
