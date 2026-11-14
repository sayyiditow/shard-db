# Phase 2 executor handoff — exact decimal core

Phase 1 is the reviewed scalar-safety foundation. Its final local commit is
identified in the handing agent's response; verify that commit is an ancestor
of your checkout and record the exact HEAD before planning. Continue on
`fix/phase1-safe-scalar-conversions`, preserving its commits and any unrelated
working-tree files. Do not start from the old main checkout or discard Phase 1.
The human has selected local phase commits on the existing integration branch;
this supersedes the parent plan's generic fresh-from-default branch wording
for subsequent phases of this series.

Read AGENTS.md, CORE-PROCESS.md, CONTRIBUTING.md, the parent recovery plan,
its acceptance and baseline companions, the Phase 0 evidence ledger, and the
Phase 1 amendments/final raw-diff review. Historical scratch patches are
reference evidence, not the current implementation or replacement patches.

## Immediate assignment: plan Phase 2, then stop for approval

1. Record branch/HEAD/worktree identity and hash the protected certification
   plan without reading its contents. It must remain exactly SHA256
   `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
   Never edit, move, stage, run, incorporate or delete that protected file.
2. Re-inventory the actual decimal consumers after Phase 1. Identify the
   seam for a self-contained C decimal core and every consumer of changed
   declarations. Existing checked int64 numeric conversion is a temporary
   safety boundary; do not mistake it for the new arbitrary-width core.
3. Produce a complete anchored Phase 2 child plan under `docs/plans/`.
   Include all API declarations, complete function/struct/code/test hunks,
   exact unique source anchors, ownership/error contracts, bounded memory
   arithmetic, consumer inventory and normal build/test commands. No new
   external runtime dependency, process-global mutable scratch or speculative
   integration into later storage/query phases.
4. Define exact signed coefficient/scale semantics and a separate display
   scale. Include parse, normalize, compare, scale-independent mathematical
   hash, precision/scale enforcement and ties-away rounding, add/subtract,
   division by uint64 row count for average, exact text output and packed
   serialization. Preserve the frozen record-format contracts; explain the
   interface between the internal representation and the packed payload.
   Include checked sizing and error handling without double/float arithmetic.
5. Cover the frozen contracts explicitly: bare numeric supports 131072
   integer and 16383 fractional digits; constrained P=1..1000 and
   S=-1000..1000, including S>P and negative S; numeric(P) has scale 0;
   currency is numeric(19,4). Signed zero canonicalizes mathematically;
   equal values hash equally despite display-scale differences. Preserve
   meaningful trailing zeros; exponent input normalizes its display scale
   (1.0e2 renders 100). Negative declared scale renders at display scale 0.
   Do not reduce bare numeric to 1000 significant digits.
6. Use Phase 0's pinned PostgreSQL 18 division-scale algorithm, including
   normalized base-10000 weights, leading groups, the quotient-weight
   decrement and the final calculation-scale clamp 0..1000. Stored
   fractional scale still supports 16383. Independently validate with
   Python Decimal using explicit precision, rounding and exponent context.
   Do not use the new implementation as its own numerical oracle. A live
   PostgreSQL capture is optional; never use a user's finance database.
7. Plan meaningful test-first proof: the original positive/negative
   1234567890123456.1234567890 and adjacent last-place values; 2^53/int64
   boundaries; signed zero/trailing zeros/exponents/carries/ties;
   numeric(3,1), numeric(3,5), negative scales, overflow after rounding,
   currency; equal-value hash/sort; cancellation/partitioned sums; repeating
   and terminating averages, tiny results, operand display scales >1000
   and quotient-weight boundaries. Exercise the entire 147455-digit value,
   one beyond each limit, malformed/embedded-NUL input and enormous exponent
   text rejected without exponent-proportional allocation or iteration.
   Specify expected failing reasons on the Phase 1 base, without breaking
   its existing scalar assertions or inventing a public storage route before
   that integration exists.
8. Deliver the child plan and phase ledger for review; STOP. This handoff
   authorizes planning only. Parent-plan approval and Phase 1's commit do
   not authorize Phase 2 production edits. If evidence disproves a frozen
   contract, identify it for revision rather than silently changing policy.

## After the human approves that child plan

- Execute its tasks in order: stage the regression/reproduction on unchanged
  production code, capture the expected red, apply the approved core, capture
  green. A new-module harness may supply the planned red proof where the
  public route intentionally belongs to later phases.
- If an anchor does not match exactly, write PLAN_NOTES.md and halt the entire
  execution. Do not guess or continue unrelated tasks. An uncovered design
  decision also returns to the human/planner.
- Use `rtk` for shell commands. Build with
  `rtk proxy env SKIP_TESTS=1 ./build.sh`; run targeted cases with
  `rtk proxy ./build/bin/shard-db-test run <case>`, then the normal full suite
  with `rtk proxy ./build/bin/shard-db-test run-all`. Preserve actual commands,
  outputs, statuses and source identities. No benchmarks or assertion weakening.
- Leave the implementation uncommitted for plan-aware and independent blind
  raw-diff review. The human authorizes each local phase commit separately.
  Do not push, open a PR or merge under this handoff.
- Keep sanitizers at the final all-fixes branch gate immediately before
  push/PR: ASan+UBSan full suite three times; TSan full suite three times
  with --jobs 2 and AGENTS.md's exact options. These have not run yet.
- Phase 2 builds the core. Phase 3 integrates packed numeric/varchar records
  (bare varchar ceiling 4 MiB); Phase 4 wires writes/defaults/schema; Phase 5
  exact criteria/indexes/joins/cursors; Phase 6 exact aggregates/output;
  Phase 7 remaining scalar grammars; Phase 8 migration/schema conversion;
  Phase 9 finance acceptance/docs/final review. Each has its own child-plan
  approval boundary. Both numeric blockers require subsequent integration;
  neither Phase 1 nor the standalone Phase 2 core alone completes the roadmap.

The separate certification rig remains scheduled only after this work lands.
