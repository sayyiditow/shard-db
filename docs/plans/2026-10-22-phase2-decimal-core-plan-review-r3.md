# Phase 2 Revision 3 re-review — ready for human approval

Reviewed 2026-10-22 against HEAD
`f5b80b4c58e49cfd8ac8b74ed9c1366f7b3a3130` on
`fix/phase1-safe-scalar-conversions`.

Submitted plan SHA256:
`f7a3e05a150070dbacc1fed29f4a8ea6f8ffa916d77dabd84f33584ed7ed5883`.

The root corrected one command-only defect during review; the final plan SHA256
is **`092f9bed3215a7a6bef7acc1c19afcf01cf53b3f87d1fef44518a0f0a227a425`**.
This is the revision to approve and hand to the executor. Production C, test
code and build.sh remain untouched. No implementation approval is inferred.

## Standards

The fresh independent C/lifecycle pass confirmed the remaining functional
findings are resolved:

- Both dec_from_digits allocation branches now reset failed outputs.
- Addition overflow frees/resets the rejected value.
- Prior zero-identity arithmetic, direct rounding normalization, successful
  output reuse and literal task replacement fixes remain in place.

**One P2 execution-command finding was found and resolved during this review.**
The scratch OOM probe includes decnum.c in its own translation unit, but the
Task 8 command also separately compiled src/db/decnum.c. Following the submitted
command produced duplicate definitions for every public API. The root removed
only that redundant argument:

```sh
rtk proxy cc -O1 -Isrc/db /tmp/p2-oracle/decnum_oom_probe.c \
  -o /tmp/p2-oracle/decnum_oom_probe
rtk proxy /tmp/p2-oracle/decnum_oom_probe
```

A fresh ordinary scratch build with this correction passes all supplied
allocation-failure cases and the addition-overflow check. An inverse-edit hash
check proves this one argument removal is the only change to the submitted
plan. No proposed C implementation or assertion was altered.

Standards: **1 finding, P2, resolved; 0 open**.

## Spec

The fresh independent requirements pass found **no actionable approval blocker**.
The unconditional packed integer bound now applies to every exponent sign.
The negative-exponent fixture encodes all four exponent bytes correctly, has
display scale 1, and covers both the valid boundary and the next invalid digit.
The root independently reproduced:

```text
nd=131073, m=-1, dscale=1: DEC_OK, integer_digits=131072
nd=131074, m=-1, dscale=1: DEC_ERR_RANGE, canonical zero output
```

Both allocation stages of parse/unpack now leave canonical zero on NOMEM.
Maximum-integer addition overflow also leaves canonical zero. The narrowed
lifecycle/alias/reserve contracts match the revised implementation. Performance
expectations are labeled as hypotheses; storage layout and supported numeric
range remain frozen. Deferring additional small-value fast paths remains an
explicit design recommendation, not an unmeasured speed claim.

The prior oracle, FNV constant/rationale and sanitizer-instruction corrections
remain in place. Scratch tooling remains outside version control. Wider
aggregate intermediates remain a specifically recorded Phase 6 decision.

Spec: **0 findings**.

## Actual verification and limits

The root and independent reviewers used fresh ordinary scratch extractions,
not the prior compiled artifacts. Root directory: `/tmp/p2-r3-root-review/`.

```text
extracted core and supplied probe: ordinary cc compilation succeeds
supplied Decimal oracle: 426 passed, 0 failed of 426
parse allocation 1 failure: NOMEM, sign=0, nd=0, w=0, cmp0=0, hash_eq=1
parse allocation 2 failure: NOMEM, sign=0, nd=0, w=0, cmp0=0, hash_eq=1
unpack allocation 1 failure: NOMEM, sign=0, nd=0, w=0, cmp0=0, hash_eq=1
unpack allocation 2 failure: NOMEM, sign=0, nd=0, w=0, cmp0=0, hash_eq=1
add overflow: RANGE, sign=0, nd=0, cmp0=0
true negative-exponent valid and invalid boundaries: expected statuses/state
all four build anchors: exactly one match each
tracked production diff: empty
```

The planner's 888-assertion unit and literal-task replay results are reported
planning evidence; the root did not independently rerun that full case/replay.
The ordinary oracle/fault/boundary checks above are independently executed.
No production build/full suite, sanitizers, benchmarks or git writes occurred.
No protected-plan content was read. Its SHA256 remains
`fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
Raw scratch artifacts stay local, untracked and uncommitted.

## Next boundary

The corrected plan is ready for explicit human approval. Once approved, the
executor should follow its Tasks 1–8 on the existing branch: capture the real
stub-first red, execute literal edits, run focused tests, ordinary oracle/OOM
probes and the normal full suite, then leave the implementation uncommitted
for plan-aware and independent blind raw-diff review. Stop on anchor mismatch
or an uncovered decision. No sanitizer run, benchmark, push, PR, migration or
certification work is authorized by this review.

Standards: 1 P2 found and resolved, 0 open. Spec: 0 findings. No open blockers.
