# Phase 2 Revision 2 re-review — two incomplete fixes remain

Reviewed 2026-10-22 against HEAD
`f5b80b4c58e49cfd8ac8b74ed9c1366f7b3a3130`. Revised plan SHA256:
`18e847235c9f5ea6ce5fd8d074f248f0fd4c603c80c907f6ef1898a644bf00ae`.

**Verdict: revise before execution approval.** Zero-operand arithmetic, direct
round normalization, the duplicate-definition instruction, zero/pad decoding,
precision-overflow cleanup, successful output reuse, oracle cancellation and
hash rationale/constant are corrected in the revised text. The ordinary oracle
reproduces 426 passed/0 failed. Two previously reported defects remain, plus
one unmeasured performance assertion. The test intended to establish one of
the fixes is a false positive.

## Standards

Fresh independent C/lifecycle review: **2 findings, worst P2**.

### STD-R2-01 — P2: parse/unpack allocation failure still returns noncanonical zero

Plan lines 1877–1885 and 1905–1906 set `out->sign`/`dscale` before the limb
allocation. `dec_from_digits` returns failed `dec_reserve` directly; parse
(2074–2077) and unpack (2782–2785) propagate it without resetting the output.
This remains an incomplete fix of STD-03/SPEC-04.

A fresh ordinary scratch build with allocation call 2 forced to fail gives:

```text
parse("1.25") -> DEC_ERR_NOMEM
sign=POS, weight=0, ndigits=0, dscale=2
cmp(out, canonical_zero)=1; hash_equal=false
```

This is not the canonical zero promised by lines 487–491. Reset the output on
all such helper failures. Add deterministic allocation-failure coverage for
both allocating stages of parse and unpack, checking output state as well as
status. Audit all output-producing APIs' error returns, not only the successful
reuse paths. Scope the contract precisely: valid initialized nonaliased outputs;
alias rejection must preserve the input, and low-level reserve/lifecycle
operations need their own explicit promises.

### STD-R2-02 — P2: addition range failure retains its rejected result

Line 2348 returns DEC_ERR_RANGE without clearing output. Fresh ordinary
reproduction:

```text
parse(131072 nines) + parse("1") -> DEC_ERR_RANGE
out.sign=POS, out.weight=32768, out.ndigits=1
out integer digits=131073
```

Clear/reset output before returning the error. Extend the existing overflow
regression to assert canonical state (sign, weight, limbs, comparison/hash),
not just the return code. This is the other incomplete STD-03 failure path.

STD-01, STD-02 and STD-04 are resolved. Successful reuse no longer discards the
old allocation. No new material style/process or Fowler heuristic finding.

## Spec

Fresh independent requirements review: **3 findings, worst P2**. The lifecycle
finding overlaps the standards axis; totals are not unique-issue counts.

### SPEC-R2-01 — P2: negative-exponent payload bound is still bypassed; new test is wrong

Line 2761 now uses `m >= 0`, so the integer-bound check still excludes all
negative exponents. The required bound is `nd + m <= 131072` for every exponent
sign. A correctly constructed payload with:

```text
nd = 131074
m = -1 (bytes FF FF FF FF)
dscale = 1
coefficient = 131074 nonzero decimal digits
```

returns DEC_OK and has **131073 integer digits**. Root and independent reviewer
both reproduced it in fresh ordinary scratch builds. SPEC-02 is only partially
resolved; m=0, odd pad and zero exponent checks are now correct.

The new unit test at 1636 writes only `pl[11] = 0xFF` into a zeroed header.
Its exponent bytes are **00 00 00 FF**, meaning **+255**, not -1. It also leaves
display scale 0. Consequently it passes by exercising the positive-exponent
overflow branch; it does not establish negative-exponent validation.

Remove the sign guard from the integer bound. Correct the fixture with all four
signed exponent bytes and dscale=1. Assert the decoded header fields in the
fixture setup so a mislabeled test cannot silently test another branch. Include
a valid boundary with m=-1 as well as the one-over invalid case. Re-run the
exact true-negative-exponent reproduction, not a reconstructed variant that
changes its exponent or display scale.

### SPEC-R2-02 — P2: canonical-zero-on-failure contract remains incomplete

Same allocation-failure and add-overflow evidence as STD-R2-01/02. The new
lifecycle contract explicitly promises canonical output on failure, and the
reported claim that NOMEM paths are fixed is not supported by the actual code.
Both paths require fixes plus failure-state assertions/injection. Successful
reuse, precision overflow, average zero-count and corrupted-input rejection
are useful coverage but do not exercise these paths.

### SPEC-R2-03 — P3: remove the unmeasured parse speed claim

Line 321 states `~2× on the smallest inputs` without measurements. The discussion
required measurement before claiming an improvement, and benchmarks remain
human-run. Remove the number or clearly identify it as an unmeasured hypothesis.
The fast-path evaluation and recommendation to defer are otherwise within the
requested scope. The packed format can also be emitted directly from a small
coefficient; limb conversion is a current implementation choice, not an inherent
requirement of the frozen packed bytes. Avoid absolute claims that rule out
that option without analysis.

SPEC-01, SPEC-03, SPEC-05, SPEC-06 and SPEC-07 are resolved in the revised
arithmetic/oracle/commands. No further sanitizer run was made. The earlier
overstep remains disclosed. Scratch tooling stays out of version control, and
linking the module in the three existing build lists remains the chosen scope;
those are settled directions rather than new approval questions.

## Verification and limits

The root freshly extracted the revised module/header/probe/oracle into
`/tmp/p2-r2-root-review/`. Builds used ordinary `cc -O0/-O1`, without sanitizers.
A separate ordinary object build substituted allocator wrappers to force the
second allocation failure. No production build or registered test was run;
the reported 863-assertion suite was not independently rerun by the root.

```text
supplied oracle: 426 passed, 0 failed of 426
0 + 0.5: DEC_OK, 0.5
0 - 0.5: DEC_OK, -0.5
round(0.1,0): cmp_zero=0, hash_equal=true
true m=-1 oversized payload: DEC_OK, integer_digits=131073 (wrong)
maximum-integer + 1: DEC_ERR_RANGE, nonzero output retained (wrong)
injected allocation2 failure: status=6, sign=1, ndigits=0, dscale=2,
    cmp_zero=1, hash_equal=0 (wrong)
```

The production tracked diff remains empty. The protected certification plan
was checked by hash only and remains
`fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
No production changes, git writes, benchmarks, sanitizers or protected content
reads. Only this review document was added by the root. Raw scratch evidence
stays local and is not staged/committed.

Next: amend those exact paths and tests, remove/qualify the performance claim,
repeat ordinary scratch validation and literal task replay, then return the
plan for review. Do not implement or commit Phase 2 yet.

Standards: 2 findings, worst P2. Spec: 3 findings, worst P2 (with overlap).
