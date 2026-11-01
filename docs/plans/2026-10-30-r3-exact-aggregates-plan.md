# R3 — exact aggregates, grouping/having, parallel merge, analytical fast paths and output

## Status

**Proposed plan for review — no implementation changes have been applied to the repo.**
Written 2026-10-30 against `fix/type-correctness-recovery` at
`ae9cb6be5cd75a62c9b1896cf915081b4a8a83f4` ("feat: integrate exact typed
predicates and indexes"; clean tree apart from local handoff/plan scratch; 15
commits ahead of local `main`; not pushed). It supersedes the interrupted
recovery draft; every anchor below was re-derived and machine-verified against
this checkout (each hunk was applied to a copy of the current source with a
strict exactly-one-match assertion, and the assembled result compiles — see
"Planning-time validation" below).

Parent contracts: [recovery plan](2026-10-19-type-correctness-recovery-plan.md)
(Phase 6, frozen average/result-scale policy, consolidation amendment),
[Phase 3 index](2026-10-23-phase3-packed-records-plan.md),
[executed R2](2026-10-28-r2-exact-query-index-plan.md) (R3 handoff section).
This is the single R3 delivery plan; it is not a plan chain.

Out of scope and protected: buffered-fetch optimisation (separate later design);
R4 scalar grammars; R5 metadata / migration / recovery; R6 activation,
client/doc acceptance and final gates;
`docs/plans/2026-10-19-durability-certification-rig.md` (not read, not
touched). Public packed binding stays **inactive**: all packed coverage uses
private physical fixtures. No benchmarks, no ASan/UBSan/TSan — the human owns
those. No locks, lock order, shard scheduling, writer reservations or snapshot
semantics change.

### Planning-time validation (evidence, not acceptance)

The revised plan was extracted and replayed against HEAD in the disposable
`/tmp/r3-plan-review-check` overlay on 2026-10-30. Only this plan document was
edited in the repo; production sources and branch state were unchanged.

- All **130 integration hunks** replay with exactly one match for every
  start/end anchor. Extraction uses this document's code fences, including
  the repaired decoder signature and closing brace. Closing Markdown fences
  are on their own lines; the full anchor replay still passes.
- The assembled `query_aggregate.c` (6,789 lines), `agg_exact.c` and extended
  `decimal_order.c` compile in production and `TEST_BUILD` modes: six objects,
  zero warnings under `-std=gnu11 -O2 -Wall -Wextra -Werror` with the repo's
  existing `-Wno-unused-parameter` allowance. All four extracted test files
  pass syntax checks with those flags and real repo headers.
- The decoder/core standalone harness reports `passed=58133 failed=0`.
  Regenerating oracle vectors matches the embedded header byte-for-byte.
  The npm snippet passes `node --check`; real npm/native acceptance was not
  run. Task 7 now provides complete, exact-anchor patches for every doc edit.
- The disposable binary linking real daemon/test infrastructure to the
  extracted implementation and **nine proposed registrations** reports:

  ```text
  test-decimal-order-decode: 781 passed, 0 failed
  test-agg-exact-core: 57352 passed, 0 failed
  test-agg-exact-routes: 85 passed, 0 failed
  test-agg-exact-limits: 85 passed, 0 failed
  test-agg-exact-output-safety: 23 passed, 0 failed
  test-agg-packed-routes-bounded: 81 passed, 0 failed
  test-agg-packed-routes-bare: 81 passed, 0 failed
  test-agg-packed-routes-extended: 720216 passed, 0 failed
  test-agg-packed-output-safety: 674 passed, 0 failed
  total: 779378 passed, 0 failed across 9 cases
  ```

  Command: `rtk proxy /tmp/r3-plan-review-check/route-check run-all --jobs 1`.
  Full output: `/tmp/r3-plan-review-check/final-nine-cases.log`.
- The **four original second-review reproductions** were rerun against a
  fresh extracted copy and all pass: count(field)/NULL avg, packed count-top-N
  labels, finite sum overflow (formerly exit 134), and retained decimal output
  under a 1MiB cap. The last now returns the single 103-byte budget error
  instead of a 3,280,491-byte success. Logs are in
  `/tmp/r3-blind2-d0v8wk6q/test-blind-*.log`.
- The existing top-N source's **11 registrations** pass with a disposable
  TEST_BUILD daemon compiled from the extracted implementation, not the repo
  binary: `70 passed, 0 failed across 11 cases`. Command:
  `rtk proxy /tmp/r3-plan-review-check/route-check-with-topn run-all --filter topn --jobs 1`.
  Output: `/tmp/r3-plan-review-check/existing-topn-cases.log`.

These are targeted planning checks of extracted code, **not** a full repo
suite pass, implementation acceptance, sanitizer results, or a replacement
for the executor's per-defect reversal evidence. Tasks 0–7 retain those gates.

Review history (two blind-review rounds; all findings repaired in the code
and tests below) is intentionally not repeated here — the raw diff and the
D1–D8 decisions are the review surface.

## 1. What is wrong (diagnosed; mechanisms per the R2 handoff)

| # | Defect | Observed on base | Mechanism (root cause) |
|---|---|---|---|
| A1 | **Packed numeric indexed min/max** (R2 handoff #1) | values 0,1,2,NULL ⇒ `min ≈ -904322805175995.6`, `max ≈ -904322805175995.5` | `decode_index_key_to_double` reads the first 8 payload bytes of the R2 ordered decimal key as a flipped big-endian int64. The new key has no 8-byte width and no inverse existed. |
| A2 | **Packed numeric indexed group labels** (R2 handoff #2) | values 0,1,2,NULL ⇒ one unlabeled `{"c":4}` bucket | Group Pass-1/serial IGB and secondary-map builders label through `decode_idx_to_buf`, whose `FT_NUMERIC` case needs `plen >= 8` and returns 0 otherwise; every label stays `""` and all keys fold into one bucket (NULL included, because the run compare keys on the label). |
| A3 | `long`/`timestamp`/legacy `numeric` sums & min/max cross `double` | `sum(l)` of two rows `9007199254740993` ⇒ `18014398509481984` (exact `…986`); `max(l)` ⇒ `9007199254740992` | `AggAccum`, `AggSingleArg`, `awc`, bundled, VS-staged, top-N heap, `WfcArg` all store `double`; `typed_field_to_double`/`decode_index_key_to_double` widen int64 to double (53-bit). |
| A4 | Decimal-tail and average rendering | legacy `numeric:19,4` `0.1000+0.2000` ⇒ `0.3` (declared scale lost); the `test_aggregate_type_matrix.c` timestamp-avg golden `1490833334666.666748` is a `%.6f` double artifact | `fmt_double` = `%.6f` then trim; averages are `sum/count` in double, not the frozen `dec_div_avg` policy. |
| A5 | Having / order_by on aggregate results compare through `double` text | adjacent values `9007199254740992`/`…993` indistinguishable | `agg_bucket_value` returns `double`; `agg_result_matches` formats `%.6f` and re-parses via `match_criterion`; `agg_sort_cmp` compares doubles; numeric group labels sort with `strcmp` (`"10" < "9"`). |
| A6 | **`group_by` on a `timestamp` field collapses to one `""` bucket** | 3 distinct timestamps ⇒ one bucket | Neither `typed_field_to_buf_raw` nor `decode_idx_to_buf` has an `FT_TIMESTAMP` case; the label stays `""` and the string key is the empty label. |
| A7 | **Long varchar group keys silently merge** | two distinct 650-byte values ⇒ one group `"c":2` | `gbuf[MAX_FIELDS][512]` truncates labels to 511 bytes; `agg_find_or_create`'s `char key[4096]` `break`s (truncating the key) when a tagged component would not fit. |
| A8 | **avg of zero contributing values is `0` on indexed shortcuts, `null` on scan** | indexed `ts eq 3000` ⇒ `{"s":0}`; unindexed control ⇒ `{"s":null}` | Leaf-only, bundled, criteria-bundled and `emit_min_max_via_keyset` shortcuts compute `counts>0 ? sum/count : 0.0`; only the shared final emitters emit `null`. |
| A9 | Packed **record-fed** aggregates read legacy offsets | not publicly visible (packed inactive) but every record-fed accumulator goes through `agg_field_ptr`/`typed_record_view`, which cannot interpret a packed record (the helper hands back the zero field ⇒ silent zeros) | R2 repaired criteria matching with `TypedRowView`; aggregates were left on the legacy view. |
| A10 | Errors vs partial totals | allocation/budget failure inside merge `continue`s past the bucket; `calloc`-fail sites return empty successes; emitters stream bytes before all values are proven renderable | No uniform error channel for exact-state allocation, non-finite floats, iterator/storage failure or output rendering. |

## 2. Decisions that need the human (defaults are what this plan implements)

CORE-PROCESS.md: do not decide architecture silently. Each default below is
implemented by this plan; changing one before approval is a localized edit.

| ID | Decision | Default in this plan | Alternative / cost |
|---|---|---|---|
| D1 | The index key of a **bare** packed numeric carries no display scale (`decimal_order_into` ignores `dscale`). Index-only **value** shortcuts would print a different scale than a scan. | Index-only value shortcuts are gated off for bare packed numeric (`agg_index_exact()==0`): they fall through to the record-fed path (exact, deterministic, parent Phase 6-sanctioned fast-path decline). Bounded `numeric(P,S)`/`currency` (display scale constant `max(S,0)`), integer classes and floats keep every shortcut. Presence/ordering walks remain valid. | Store scale in the key = B+ tree/key-format change (R5/R6 territory). |
| D2 | Numeric **group label** text and equality. | Group identity is the mathematical value (`1.0` and `1.00` are one group). Label = declared scale for bounded packed numeric; **canonical shortest scale** for bare packed numeric — identical on scan and index routes (tested across worker layouts, merge orders and both routes). Legacy numeric labels keep the historical `typed_field_to_buf_raw` form. | Keep first-seen scale (nondeterministic across worker layouts — rejected). |
| D3 | Empty-input outputs, per format (explicit freeze). | **JSON every route**: `sum`/`min`/`max` with zero contributing values print `0`; `avg` prints `null` — now on every route (A8). **CSV keeps the historical cell**: an empty avg renders `0` (as today), as do empty sum/min/max. The JSON/CSV asymmetry is intentional wire compatibility, asserted per route in the tests. | Emit `null` for sum/min/max empties = public wire change; R6 acceptance decision. |
| D4 | Result lexemes (wire shape stays a JSON number; no global string protocol). | Integer-class `sum` = exact integer lexeme (may exceed int64, never `.0`); `avg` of integer/decimal = `dec_div_avg` lexeme at the PostgreSQL-18 scale; legacy `numeric:P,S` and packed numeric `sum`/`min`/`max` keep **declared/display scale** (`0.3000`, not `0.3`); floats use shortest round-trip `%.15g…%.17g` (T07). | Trimmed zeros = silent scale loss (rejected by the parent contract). Existing expectations that encode a `%.6f` double artifact are updated only per Task 5's protocol. |
| D5 | Numeric-typed `group_by` fields in `order_by` / `having`. | Numeric-class group fields (int/long/short/byte/timestamp/numeric/float/double) order and compare **numerically/exactly**; every other group field (bool, enum, uuid, ip, time, calendar, varchar) compares as text. Today numeric labels sort with `strcmp` and `bool` labels go through `atof`. | Keep lexicographic = documented-bug preservation. |
| D6 | Group value longer than the label/key buffers (A7) or containing NUL. | **Explicit error** `{"error":"group_by value exceeds aggregate key limit (511 bytes) or contains NUL"}` — never a truncated key. **This is a new compatibility restriction requiring explicit approval**: queries that today silently merge such groups (wrong answers) will fail with this error instead. Full-length heap keys across IGB/bitmap workers remain possible later but are a different delivery. | Silent truncation (today's corruption) is not an option; heap-sized keys are a large separate change. |
| D7 | Streaming top-N (`agg_run_topn_stream`) keeps its `double` metric heap whose public test API (`topn_heap_*`) is externally linked by `test_agg_topn_stream.c`. | Keep the heap API unchanged. The streaming executor **declines (`-2`, existing hash-path fallback)** unless every spec is `count(*)` (no field); count(field), floating, integer and decimal value aggregates use the hash path (exact contribution counts and errors). Packed numeric group labels still use the exact decoder on count(*) streams. | Rework heap to 128-bit/decimal metrics + update the test (more surface). |
| D8 | **Text min/max truncation at 1,023 bytes** (flagged by the handoff review). | Fixed in this plan, not deferred: the record-fed text offer keeps exact bytes (`agg_span_text` feeds `agg_text_offer`, which arena-copies on replace); varchar declines the bounded indexed text shortcut and uses the record-fed route even with an index present. Calendar scalar conversions retain bounded scratch. No supported length is silently narrowed; legacy and packed fixtures assert full-length text min/max. | Deferring would leave a silent-wrongness class the handoff explicitly rules out. |

## 3. Frozen R3 contracts

1. **No intermediate double for exact values.** Integer-class (`int, short,
   byte, bool, long, timestamp`) and legacy `numeric:P,S` accumulate in checked
   `__int128` (legacy numeric keeps its raw scaled integer; scale is
   metadata). Packed numeric (bare, bounded, currency) accumulates in `dec_t`
   via the committed core (`dec_add`, `dec_cmp`, `dec_div_avg`). `float`/`double`
   stay `double`.
2. **Overflow.** `__int128` additions are `__builtin_add_overflow`-checked;
   `dec_add` range failures and allocation failures propagate as errors. A sum
   is never silently clamped, wrapped or returned partially. For finite
   floats, a sticky sum-overflow bit propagates through merges; only a
   requested SUM/AVG reports it. COUNT/MIN/MAX never fail because an unused
   sum overflows. Non-finite input remains an immediate error.
3. **Average.** `avg` of integer-class, legacy numeric and packed numeric =
   `dec_div_avg(sum, count)` (PostgreSQL 18 `select_div_scale`: base-10000
   weights and leading limbs, 16 significant digits, both operands' display
   scales as floors, clamp 0..1000, ties away from zero). `avg` of floats =
   `sum / count` in double. No contributing value ⇒ JSON `null` (D3).
4. **min/max.** Compare mathematically (`dec_cmp`, integer compare, double
   compare). Equal-value tie ⇒ keep the larger display scale (commutative, so
   identical for every worker layout, merge order and route).
5. **NULL / zero / empty.** NULL (bitmap) never contributes. Numeric zero is a
   value. Text unset markers keep the existing presence doctrine. NULL forms
   its own group; `1.0` and `1.00` form one group (D2).
6. **Failure ≠ success.** Allocation failure, `QUERY_BUFFER_MB` rejection,
   deadline, malformed packed record, iterator/storage failure, non-finite
   float, over-long group value and decimal range failure each produce one
   explicit `{"error":…}` response and non-zero return, from **every** route,
   before any success-shaped byte is written.
7. **Output.** One cell renderer feeds JSON object, JSON rows (`total`), CSV
   and scalar shortcuts, so the same value prints the same lexeme on every
   route (D3's CSV asymmetry excepted and tested). Number lexemes are
   preserved end to end (no atof/strtod/`%.6f` on exact values).
8. **Concurrency unchanged.** All per-worker states are private `AggCtx`
   clones as today; the shared `QUERY_BUFFER_MB` atomic stays the only shared
   counter. Exact decimal states are per-bucket heap storage released through
   the same ownership path as the buckets themselves.

## 4. Consumer inventory and remaining-consumer closure census

Searched at the base commit (planning greps recorded in the hand-back; the
executor re-runs them in Task 6). "Owner" = who must make the seam safe.

| Symbol / seam | Where used | Today | Owner / disposition |
|---|---|---|---|
| `AggAccum` (`sum,min,max` doubles) | `query_aggregate.c` only | double | **R3** Tasks 2/4 (exact `AggNum`) |
| `AggResult`, `agg_bucket_value`, `agg_result_matches`, `agg_having_*`, `agg_sort_cmp` | `query_aggregate.c` only | double via `%.6f` | **R3** Task 4.7/4.8 |
| `typed_field_to_double` | aggregate callbacks only (verified: no other TU) | legacy-offset, double | **R3** deleted |
| `decode_index_key_to_double` | aggregate walks only (verified: no other TU) | double | **R3** deleted |
| `decode_idx_to_buf` (non-static, `query_internal.h`) | aggregate label sites; **non-aggregate**: `query.c:460` (`index_payload_matches`), `query.c:3086` (bitmap dict match), `query_join.c` (comment only) | no packed-numeric/timestamp case | aggregate sites **R3** (`agg_index_label`). `query.c:460/3086` are R2-closed candidate/match surfaces; `FT_TIMESTAMP` is added to the shared function itself so any caller is correct |
| `typed_field_to_buf_raw` (non-static) | aggregate labels only | no `FT_TIMESTAMP` case | **R3** adds the case |
| `agg_field_ptr` / `typed_record_view` | aggregate record callbacks only | legacy only | **R3** replaced by `TypedRowView` |
| `fmt_double` | aggregate emitters only | `%.6f` | **R3** deleted |
| `topn_heap_*`, `eligible_for_topn_stream`, `agg_run_topn_stream` | `query_aggregate.c`, `test_agg_topn_stream.c` (externs) | double metric | API **unchanged** (D7); executor streams only count(*) specs; other aggregates use the exact path |
| `cmd_aggregate`, `cmd_aggregate_tree`, `cmd_aggregate_do` | `types.h` decls, `server.c:677/2195` dispatch, NQL | | public entry signatures unchanged; `cmd_aggregate_do` (static) gains two trailing seam params |
| match_criterion (query.c legacy string matcher) | untyped legacy objects; **no aggregate caller after Task 4.7** | atof-based | not aggregates; legacy-only, R2-executed predicate surface — closed, no change |
| `query_plan.c` `atof/strtof` in compile_criteria | FT_DOUBLE/FT_FLOAT predicates only | double for float class | float-class by design — closed |
| JSON/CSV aggregate output consumers | `docs/query-protocol/aggregate.md`, `nql.md`, `npm/index.d.ts`, `npm/README.md`, `src/cli/views.c` | | docs + npm note in Task 7; shard-cli renders server text verbatim (no `atof`/`strtod` over `src/cli` — verified; Task 7 re-runs the grep) |
| Remaining `double` in the aggregate TU after Task 4 | — | — | only: top-N heap (D7), float-class helpers, `QueryDeadline`/timing — Task 6 pastes the grep |

Census closure rule (R3 exit): after Task 6,
`grep -n "fmt_double\|decode_index_key_to_double\|typed_field_to_double\|agg_field_ptr\|1e308" src/db/query_aggregate.c`
prints nothing (validated on the assembled overlay), and the full `double`
grep lists only the categories above. Every seam has an owner or a verified
safe disposition; none is deferred silently.

## 5. Execution rules (embedded, per CORE-PROCESS.md)

- Branch: stay on `fix/type-correctness-recovery`. **Do not commit, push,
  merge or create branches.** Leave all work uncommitted for raw-diff review
  (AGENTS.md standing exception). Do not stage `PLAN_NOTES.md`, raw logs,
  scratch snapshots or object files. HANDOFF.md is local scratch — never stage
  it.
- Tasks run in order. Build: `rtk proxy env SKIP_TESTS=1 ./build.sh`
  (zero warnings). Tests: `rtk proxy ./build/bin/shard-db-test run[-all]`.
  Prefix every shell command with `rtk`.
- If a quoted anchor is not found **exactly**, write `PLAN_NOTES.md` describing
  the mismatch and halt the entire run. If you hit a decision the plan does not
  cover, stop and ask.
- **Test-first staging.** Tasks 1–2 are independently buildable and carry
  their own RED/GREEN. Tasks 4.x hunks are one integrated change to a single
  translation unit: **apply all of 4.1–4.9 in order, then build** — interim
  builds are expected to fail (the old double struct users exist until the
  last hunk lands) and are NOT semantic evidence. The legacy semantic RED is
  Task 3's legacy route tests against the
  Task-2 build, with the packed file unregistered. Packed semantic RED uses
  the named reversals after the Task-4 schema seam exists; a missing-symbol
  link failure is never counted as behavioral evidence.
- **Per-defect reversal proof** (parent protocol): after GREEN, for each
  defect below, temporarily restore only that defect's old behaviour in the
  working tree, re-run the named test and paste the failing output, then
  restore the fix and paste the passing output. Do not use `git stash` for
  this (it would also revert prerequisite hunks of the same TU); edit the one
  branch/line the reversal names:
  - A1/A2: in `agg_index_label_f`, force `is_num && ns->cls == AGC_DEC` down
    the legacy `decode_idx_to_buf` path (delete the AGC_DEC branch) —
    `test-agg-packed-routes-bounded` group-label and scalar assertions fail.
  - A3 precision: in `agg_num_offer_int`, temporarily insert
    `v = (agg_i128)(double)v;` before the checked addition. Run
    `test-agg-exact-routes`: the two `9007199254740993` offers lose digits and
    its exact sum/min/max assertions fail. Restore the line and rerun GREEN.
    Separately remove the checked-add guard to prove the core overflow vector;
    overflow reversal is not evidence for the original precision defect.
  - A4/D4: in `agg_num_spec_for`, force legacy `numeric` `scale = 0` —
    `test-agg-exact-routes` `sn`/`mnn`/`mxn` literals fail.
  - A6: delete the `FT_TIMESTAMP` case in `decode_idx_to_buf` — the
    timestamp-group assertion fails on the indexed object only.
  - A7/D6 composite limit: restore `break` for the key-overflow branch in
    `agg_find_or_create` — `ar_combined_key` in `test-agg-exact-limits` fails.
    Its nine 500-byte components each pass the component limit.
  - A7/D6 component limit: temporarily clamp `n` to `cap - 1` instead of
    returning E2BIG in `agg_row_label`'s varchar limit branch — the unindexed
    `ar_longkey` assertion fails. Restore before the next reversal.
  - D8: in `agg_span_text`, temporarily cap varchar `*len` at 1023 before
    returning — `ar_full_text` and the extended packed text assertion fail.
  - Finite-float COUNT/MIN/MAX: restore immediate ERANGE on overflow in both
    the floating offer and merge — `ae_limits` and `ar_large_float` fail.
    Restore; disabling the SUM/AVG sticky-error check must instead fail their
    required out-of-range assertions.
  - Criteria-bundled count(field): restore the `AGG_COUNT` outer-loop skip
    and remove the final `!specs[i].field[0]` guard — `ar_matrix`'s count-only
    and sibling count assertions fail on indexed objects.
  - Output-allocation cleanup: temporarily remove `if (!c) return` from
    `agg_cells_dispose` (the outer owning-array cleanup); keep the dedicated
    cells fault hook enabled by the test. `test-agg-exact-limits` fails by
    signal instead of its required OOM response. Restore and rerun GREEN.
  - Top-N cleanup: after heap drain, temporarily add a second
    `topn_heap_destroy(heap);` in the new `if (rerr)` render-error block.
    `test-agg-exact-output-safety`'s forced stream array OOM fails by signal;
    restore the single-owner cleanup and prove one OOM response.
  - Escaped text atomicity: temporarily replace the injected-failure branch
    in `agg_cell_text` with `c->escaped = strdup(""); return 0;`.
    `test-agg-exact-output-safety`'s text and both group-label OOM assertions
    fail because a success response replaces the required single error.
    Restore before the next case.
  - Render budget: temporarily set `need = 40;` immediately before
    `agg_cell_reserve(c, need)` in `agg_cell_from_num`. The 1MiB rejection
    assertion in `test-agg-packed-output-safety` fails. Restore; its 8MiB
    exact-success check must still validate all 200 untruncated decimals.
  - Top-N classification/presence: temporarily re-enable the old
    count(field)/floating-spec stream eligibility, then restore the
    float-only `gb_numeric` assignment one at a time. The output-safety
    registrations must respectively fail NULL-contribution and exact packed
    label assertions. Restore both changes and rerun GREEN.
  - A8/D3: in `agg_cell_from_num`, temporarily replace the empty-AVG branch
    `if (fn == AGG_AVG) { c->is_null = 1; return 0; }` with:

    ```c
    if (fn == AGG_AVG) {
        if (agg_cell_reserve(c, 2)) return -1;
        c->lex = strdup("0");
        if (!c->lex) { errno = ENOMEM; return -1; }
        return 0;
    }

```

    `ar_matrix`'s `{"a":null}` assertion fails (CSV `0` remains compatible).
    Restore and rerun GREEN.
  - A10: in `agg_ctx_merge`, `continue` on merge failure without recording
    `dst->agg_error` — the fault-loop assertions fail.
  - A9: in `agg_row_open`, ignore `typed_row_open` failure for packed schemas
    (return 0 with `r->ok = 1` cleared) — packed-route assertions fail.
- Do not run benchmarks, ASan/UBSan or TSan. Do not read, edit, move, stage or
  execute the protected certification plan.
- New tests use private temp roots (`mkdtemp`) and restore `g_db_root` on every
  return; request-layer tests use uniquely named objects and
  `tu_pdb_drop_object`; none depends on test order. Tests that touch
  `query_buffer_max_bytes` or the alloc-fault hook save and restore them with
  an `__attribute__((cleanup))` guard (the budget is `g_db->query_buffer_max_bytes`
  — a macro, not an assignable global).

## 6. Tasks

### Task 0 — Preflight, baseline, oracle (no production change)

1. `rtk proxy git rev-parse HEAD` must print
   `ae9cb6be5cd75a62c9b1896cf915081b4a8a83f4`; `rtk proxy git status --short`
   must list no tracked modification.
2. `rtk proxy env SKIP_TESTS=1 ./build.sh` — zero warnings;
   `rtk proxy ./build/bin/shard-db-test run-all` — record
   `total: 21719 passed, 0 failed across 537 cases` (R2 baseline). Any other
   number: stop and report.
3. Spot-check the first anchor of every 4.x hunk before starting Task 4.
4. Save §7.2's `agg_oracle.py` outside the repo and run
   `rtk proxy python3 -I agg_oracle.py`; its output must equal the §7.3
   goldens (planning run recorded:
   `two_2p53p1_longs sum= 18014398509481986 avg= 9007199254740993.0000 …`,
   `ts_third sum= 4472500004000 avg= 1490833334666.6667 …`,
   `dec_tail sum= 0.3000 avg= 0.15000000000000000000 …`,
   `cancel sum= 1 avg= 0.20000000000000000000 …`,
   `bare_mixed_scale sum= 3.50 avg= 1.16666666666666666667 …`,
   `small_avg sum= 0.0000003 avg= 0.000000150000000000000000 …`,
   `ties_away sum= 3 avg= 1.5000000000000000 …`).

### Task 1 — `decimal_order_decode` (inverse of the R2 ordered decimal key)

**Mechanism (A1/A2).** R2 added `decimal_order_into` but no inverse, so every
consumer that needs the *value* from an index key reuses the legacy 8-byte
decoders. This task adds the single decoder; Tasks 2/4 consume it.

**Test first** — create `src/test/cases/test_decimal_order_decode.c` (complete
file below) and register it in `build.sh`: add
`    src/test/cases/test_decimal_order_decode.c \` immediately after the quoted
line `    src/test/cases/test_decnum_core.c \` (single occurrence, test list
only). RED: `rtk proxy env SKIP_TESTS=1 ./build.sh` fails to link
(`decimal_order_decode`, `decimal_min_dscale` undefined) — paste the linker
error.

[Task 1 production code]

GREEN: build clean; `rtk proxy ./build/bin/shard-db-test run test-decimal-order-decode`
passes. Reversal proof: change `out->digits[i] = (uint16_t)(raw - 1);` to
`raw`, re-run, paste the round-trip failures, restore.

### Task 2 — `agg_exact` module (exact states, rendering, comparison)

**Mechanism.** One pure module owns every exact-arithmetic rule so the nine
accumulation sites cannot drift: classification, `AggNum` (sum/min/max),
merge, result extraction (PostgreSQL `avg`), rendering, comparison, ordered-key
decoding and the QUERY_BUFFER accounting for decimal storage.

**Test first** — create `src/test/cases/test_agg_exact_core.c` and
`src/test/cases/agg_exact_vectors.h` (complete files below; the header is the
committed output of §7.2's generator at seed 20261030 and was reviewed against
§7.3's literals). Register the test in `build.sh` immediately after the
`    src/test/cases/test_decimal_order_decode.c \` line added in Task 1. RED =
missing header/`agg_exact.h`. Add the module to build.sh by replacing every
occurrence of `src/db/decimal_order.c` in the **five** source lists with
`src/db/decimal_order.c src/db/agg_exact.c`
(`rtk proxy grep -c "src/db/decimal_order.c" build.sh` = 5 before and
`rtk proxy grep -c "src/db/agg_exact.c" build.sh` = 5 after).

GREEN: `rtk proxy ./build/bin/shard-db-test run test-agg-exact-core` passes
(planning evidence: the revised module + test pass standalone 58,133/0). Reversal
proof: revert the `dec_minmax` tie comparison (`>` → `<`) and the
`__builtin_add_overflow` guard one at a time; the corresponding vectors fail;
restore.

[Task 2 production code]

### Task 3 — Route tests authored; RED captured against the Task-2 build (no production change)

Create both complete route test files below, but initially register **only**
`src/test/cases/test_agg_exact_routes.c` in `build.sh`, immediately after the
unique anchor `    src/test/cases/test_indexed_zero_exists.c \`:

```text
    src/test/cases/test_agg_exact_routes.c \
```

Build with Tasks 1–2 in place and production otherwise base. Run
`rtk proxy ./build/bin/shard-db-test run test-agg-exact-routes` and
`rtk proxy ./build/bin/shard-db-test run test-agg-exact-limits`; paste the
semantic RED output. The route test's integration-only allocation hook is
weakly declared and guarded, so this build links without Task 4.

Keep `test_agg_packed_routes.c` **unregistered** during this build. All cases
share one executable: registering it now would prevent even the legacy RED
from linking because `cmd_aggregate_with_schema_for_test` does not exist yet.
A link failure is not packed semantic regression evidence.

After all Task 4 hunks land, register the packed file immediately after the
unique `    src/test/cases/test_agg_exact_routes.c \` anchor:

```text
    src/test/cases/test_agg_packed_routes.c \
```

Build once, run every route registration, then perform the named packed
per-defect reversals in §5 to obtain semantic RED/GREEN with the seam present.
The packed file is authored test-first; its semantic reversal proof happens
after integration. Do not claim its tests ran on the Task-2 binary.

[Task 3 test code]

### Task 4 — The integration (one TU; apply 4.1–4.9 in order, then build)

All hunks target `src/db/query_aggregate.c` unless stated. Every anchor below
was machine-verified to occur exactly once in the base file in the stated
relation. Apply in the listed order (later hunks never depend on text a
previous hunk consumed); the **first** build happens after 4.9.

[Task 4 production code]

GREEN (all of 4.1–4.9 landed):

1. Register the packed file as Task 3 specifies, then run
   `rtk proxy env SKIP_TESTS=1 ./build.sh` — zero warnings.
   `rtk proxy nm ./build/bin/shard-db-test` must include a defined
   `agg_test_fail_cells_alloc` and `agg_test_fail_escape_alloc` symbols;
   either missing definition is a failure, not a skip
   of the guarded Task-3-compatible OOM regression.
2. `rtk proxy ./build/bin/shard-db-test run test-agg-exact-routes` — passes
   (RED→GREEN per route); same for `test-agg-exact-limits`,
   `test-agg-packed-routes-bounded`, `test-agg-packed-routes-bare`,
   `test-agg-packed-routes-extended`, `test-decimal-order-decode`,
   `test-agg-exact-core`, `test-agg-exact-output-safety`, and
   `test-agg-packed-output-safety`.
3. Reversal proofs per §5, one defect at a time, outputs pasted.
4. `rtk proxy ./build/bin/shard-db-test run-all` — full suite; classify every
   failing existing assertion per Task 5's E/R/N protocol before editing anything.

### Task 5 — Existing-expectation changes (allowed only per this protocol)

Run the full suite after Task 4. Every failing existing assertion must be
classified; only class **E** may be edited, each with the oracle value, and
the edit list is pasted into the hand-back:

- **E — expectation encodes the old `%.6f` double artifact.** Known:
  `src/test/cases/test_aggregate_type_matrix.c` expects
  `"a":1490833334666.666748` (timestamp avg; the six-value dataset's oracle
  avg is `745416668333.33333333` — computed by Appendix B / §7.2's oracle, matching the
  `dec_div_avg` policy). Any other assertion expecting a trimmed legacy
  numeric sum (`12.5` for `numeric:P,2`) changes to the declared-scale form
  (`12.50`) — oracle-checked.
- **R — regression** (any other change in value, shape, or ordering of rows
  with equal sort keys excepted): stop, root-cause, fix the production code.
  Never edit the test.
- **N — new explicit error** (over-long group value, invalid having literal):
  only where the old test relied on silent truncation — report to the human
  instead of editing.

`test-agg-topn-stream` keeps passing unchanged (heap API untouched, D7); a
streaming case that now declines to the hash path must produce identical rows.

### Task 6 — Remaining-consumer closure and output-failure audit

1. `rtk proxy grep -n "fmt_double\|decode_index_key_to_double\|typed_field_to_double\|agg_field_ptr\|1e308" src/db/query_aggregate.c`
   must print nothing.
2. `rtk proxy grep -n "double" src/db/query_aggregate.c` — paste output; every
   hit must be (a) top-N heap/`TopNWalkCtx` (D7), (b) `agg_render_double`
   callers, (c) `QueryDeadline`/timing. Anything else halts the run.
3. `rtk proxy grep -n "atof\|strtod\|%.6f" src/db/query_aggregate.c src/cli`
   — no aggregate-path hit remains; record any `src/cli` hit with an owner in
   the census table.
4. Route matrix (§9): paste the table with pass counts; every failure row
   (alloc, budget, over-long key, packed fault) must be green.
5. Dead code: the build is warning-free; delete any helper the compiler flags.

### Task 7 — Docs, npm and acceptance hand-back

1. **`docs/query-protocol/aggregate.md`** — replace these two complete rows
   (one exact occurrence):

```text
| `sum` | yes | Supports `int`, `long`, `short`, `float`, `double`, `numeric`, `bool`, `byte`, and `timestamp`; result is a JSON number. |
| `avg` | yes | Same supported types as `sum`; returns `sum / count` as a JSON number. |
```

   with:

```text
| `sum` | yes | Supports `int`, `long`, `short`, `float`, `double`, `numeric`, `bool`, `byte`, and `timestamp`; integer/decimal results are exact JSON numbers — integer sums stay exact beyond 2⁵³ and decimals keep declared/display scale; float/double retain binary floating-point semantics. |
| `avg` | yes | Same supported types as `sum`; integer/decimal averages use the PostgreSQL 18 exact result-scale policy (16 significant digits, ties away from zero) and render as an exact JSON number; float averages stay binary floating. No contributing value renders `null` (JSON) on every route, or the CSV cell `0`. |
```

   Insert the following complete section immediately before the exact
   unique heading quoted below, including its literal backticks:

```text
## `having`
```

   Inserted section:

```markdown
### Numeric exactness

- Integer-class and decimal aggregates are computed exactly (no binary
  floating point); a `sum` of `long` values may exceed 2⁵³ and still prints
  every digit. Clients must keep the number lexeme intact (a `JSON.parse`
  loses digits above 2⁵³ — see the npm notes).
- Decimal results keep scale: bounded `numeric(P,S)` results render at the
  declared scale (`0.3000`, not `0.3`); bare numeric groups use canonical
  shortest labels (`1.0` and `1.00` are one group labelled `1`).
- `group_by` labels, `order_by` and `having` on numeric fields are exact and
  numeric (not lexicographic); `timestamp` groups render epoch-millis labels.
- A group value longer than 511 bytes (or containing NUL) is an explicit
  error, never a truncated key:
  `{"error":"group_by value exceeds aggregate key limit (511 bytes) or contains NUL"}`.
- An invalid numeric literal in `having` is a validation error:
  `{"error":"invalid numeric value 'abc' for having on 's'"}`.
- `QUERY_BUFFER_MB` covers retained number strings and escaped aggregate
  text/group values. Insufficient memory returns a query-buffer error before
  any success response; reduce groups/limit or increase the configured cap.
```

2. **`npm/README.md`** — insert this complete block immediately before
   the unique quoted anchor:

```text
Ordinary fetch/multi-get/keys/existence CSV cells escape literal backslash as `\\` and control bytes as `\u00xx`.
```

```markdown
Aggregate responses also preserve exact number lexemes: summing two `long`
values of `9007199254740993` returns `18014398509481986`. Keep this response
as text or read it with a lossless JSON parser; `JSON.parse` converts number
tokens to JavaScript `Number` and can lose digits. `ShardDb#query()` returns
the raw response string; the package does not parse aggregate results.

```

   **`npm/index.d.ts`** — insert this complete comment fragment immediately
   before the unique quoted anchor:

```text
   * Multiple concurrent queries are safe — shard-db's worker pool handles parallelism.
```

```typescript
   * Aggregate integer/decimal results also require preserved number lexemes;
   * see "Exact decimal and CSV values" in the README.
```

3. `npm/test/basic.js` — the suite already exercises `aggregate` (section 17
   asserts `agg.total === 60` via `JSON.parse`, exact for small integers).
   Add one precise **raw lexeme** assertion immediately after the
   `agg.total === 60` assertion (anchor: the line
   `` assert(agg && agg.total === 60, 'aggregate sum returns correct total') ``):

```javascript
  // 17b. raw JSON carries integer lexemes that JS Number cannot represent.
  const exactObj = 'r3_exact_aggregate'
  try {
    const created = await db.query({ mode: 'create-object', dir: 'test', object: exactObj,
      splits: 8, max_key: 16, fields: ['score:long'] })
    assert(!created.includes('"error"'), 'exact aggregate fixture created')
    const inserted = await db.query(`{"mode":"bulk-insert","dir":"test","object":"${exactObj}","records":[{"key":"b1","value":{"score":9007199254740993}},{"key":"b2","value":{"score":9007199254740993}}]}`)
    assert(!inserted.includes('"error"'), 'exact integer rows inserted')
    const rawAgg = await db.query({ mode: 'aggregate', dir: 'test', object: exactObj,
      aggregates: [{ fn: 'sum', field: 'score', alias: 'total' }] })
    assert(rawAgg.trim() === '{"total":18014398509481986}',
      'aggregate sum keeps the exact lexeme above 2^53 (no JSON.parse)')
  } finally {
    await db.query({ mode: 'drop-object', dir: 'test', object: exactObj })
  }
```

   (`query()` accepts a raw JSON string: the template above contains literal
   decimal digits, never a JS Number conversion. This isolated `long` object
   contains exactly two rows and is dropped in `finally`; the earlier `scores`
   fixture and its small-integer assertion are independent.)

4. **`docs/reference/changelog.md`** — insert this complete paragraph after
   the unique heading and its following blank line (anchor below includes
   that blank line):

```markdown
## Unreleased

```

```markdown
Aggregates now preserve exact integer and decimal arithmetic through scans,
indexes, parallel merging, grouping, `having`, ordering, and JSON/CSV
output. Integer sums retain every digit beyond int64 and 2⁵³; decimal
results retain declared/display scale, and integer/decimal averages follow
the PostgreSQL 18 result-scale policy with ties rounded away from zero.
Clients that depend on the previous trimmed decimal output must preserve
the new number lexemes. Timestamp group labels use epoch milliseconds,
and numeric group fields compare numerically in `order_by` and `having`.
An average with no contributing values returns JSON `null` on every route;
CSV retains its historical `0` cell. Oversized or NUL-containing group keys
and invalid numeric `having` literals now return explicit errors. Packed
decimal indexed aggregates agree with scans, varchar extrema retain their
full values, and finite floating-point `count`/`min`/`max` remain available
when an unused sum would overflow. Query memory limits cover retained
rendered number strings and escaped text; allocation failures return an
error before any success response is emitted.

```

5. Census: append §4's table with final status to the hand-back.
6. Full suite fresh: `rtk proxy env SKIP_TESTS=1 ./build.sh` (zero warnings)
   then `rtk proxy ./build/bin/shard-db-test run-all`; paste the final
   `total:` line. Also run and paste individually: `test-decimal-order-decode`,
   `test-agg-exact-core`, `test-agg-exact-routes`, `test-agg-exact-limits`,
   `test-agg-packed-routes-bounded`, `test-agg-packed-routes-bare`,
   `test-agg-packed-routes-extended`, `test-agg-topn-stream`,
   `test-aggregate-type-matrix`, `test-alloc-error-propagation`.
7. `rtk proxy git status --short` — only the files in §10 are modified/added;
   run `rtk proxy git diff --check`. Leave everything **uncommitted**. No
   benchmarks, no sanitizers (the human owns them). No locks, lock
   order or shared counters change, but the diff **does** add heap-owned
   per-bucket decimal states that are created, merged and released across
   parallel workers (object lifetimes + background threads). The AGENTS.md
   dynamic-safety gate therefore applies: state this in the hand-back, and
   before push (once, after all fixes, never between fix rounds) the human runs
   `BUILD_MODE=asan` and `BUILD_MODE=tsan` builds with three fresh
   `run-all` runs each (TSan with `--jobs 2`, `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"`).
   The executor does not run them.

## 7.3 Literal goldens (all reproduced by Appendix B / §7.2's oracle at planning time)

```text
2 × 9007199254740993          sum 18014398509481986   avg 9007199254740993.0000
1490833334666,…667,…667       sum 4472500004000       avg 1490833334666.6667
0.1000 + 0.2000 (scale 4)     sum 0.3000              avg 0.15000000000000000000   min 0.1000 max 0.2000
±9223372036854775807 ×2, 1    sum 1                   avg 0.20000000000000000000
bare 1.0, 2.00, 0.5           sum 3.50                avg 1.16666666666666666667   min 0.5  max 2.00
bare 1, 1.00, 1.0             sum 3.00                avg 1.00000000000000000000   min 1.00 max 1.00
0.0000001, 0.0000002          sum 0.0000003           avg 0.000000150000000000000000
12345678901234567890.1234567890, ±0.0000000001
                              sum 12345678901234567890.1234567890
                              avg 4115226300411522630.0411522630
route dataset (rows k1..k6):  l sum 18014398509481987  avg 3602879701896397.4000
                              n sum 0.3000  avg 0.07500000000000000000
                              ts sum 4472500010000  avg 745416668333.33333333
                              group a: sum(l) 18014398509481986  sum(n) 0.3000
                              group b: sum(l) 1  sum(n) 0.0000
packed {0,1,2} bounded(19,4)  sum 3.0000 avg 1.00000000000000000000 min 0.0000 max 2.0000
packed {0,1,2} bare           sum 3      avg 1.00000000000000000000 min 0     max 2
packed tie 1, 1.00, 1.0       sum 3.00   avg 1.00000000000000000000 min 1.00  max 1.00  one group "1"
packed large+bounds           sum 12345678901234567890.1234
                              avg 2469135780246913578.0247
                              min -999999999999999.9999  max 12345678901234567890.1234
packed 2×500000000000000.0000 sum 1000000000000000.0000  avg 500000000000000.0000
int {3,2,1,0}                 sum 6 avg 1.5000000000000000
partition 120k rows f0=i%3    sum(f1) 120000  avg(f0) 1.00000000000000000000
                              groups 0/1/2 × 40000 each
```

## 9. Route matrix (acceptance evidence; paste with pass counts)

| Route | Representative request | Covered by |
|---|---|---|
| count(*) metadata | `count` only | existing `test-aggregate-type-matrix` |
| leaf-only / bundled walk (no criteria) | `sum/avg/min/max` on indexed fields | `r3l_i`, `r3l_i64`, packed `pi`,`p16` |
| record scan / hash aggregation | unindexed object | `r3l_s`, packed `ps` |
| same-field btree extreme | `max(l)` with `l lte …` | `ar_matrix` |
| walk-fetch-check | `max(l)` with `ts lt …` | `ar_matrix` |
| keyset extreme (single leaf) | `min/max` with one indexed criterion | `ar_matrix` (`ts lt 1490833334668`) |
| keyset extreme (intersect, 2+ leaves) | AND of two indexed leaves | `ar_matrix` (`g eq b AND ts lt 1490833334668`) |
| criteria-bundled index walk | multi-spec with indexed criteria | `ar_matrix` (`ts gte 0`) |
| parallel indexed shard worker + merge | `sum` with criteria | `ar_matrix` |
| IGB (indexed group-by) incl. pass 2 | `group_by g` / `ts` with sums | `r3l_i`, packed `pi` |
| IGB serial path | small cardinality at low split count | `r3l_s` group cases |
| IGB bitmap dictionary | group by a bitmap-indexed numeric | `test-agg-packed-routes-extended` (e) covers the numeric-dict label route via IGB; add the `b:bitmap` object below |
| varchar stream (VS) | `group_by g`, `limit` ≤ 100000, no order | `ar_limits_run` limit variant + existing `test-agg-varchar-groupby-*` |
| partition merge (`part_merge_worker`) | ≥100k rows, ≥4 idx shards | `test-agg-packed-routes-extended` (e) |
| top-N streaming | indexed group + count(*) metric; other specs decline | existing `test-agg-topn-stream`, both output-safety registrations |
| having (tree and flat) | adjacent values > 2^53 | `ar_adjacent` |
| order_by numeric group / aggregate alias | `ts`, `s` | `ar_matrix`, `ar_adjacent` |
| outputs: JSON object, rows+total, CSV | scalar and grouped | `ar_matrix` |
| count(field) scan parity | NULLs excluded; IGB/VS/stream row counts decline count(field) | `ar_matrix`, `test-agg-exact-output-safety` |
| streaming packed group labels and fixed-width NULL tier | count(*) with order_by/limit | `test-agg-packed-output-safety` |
| output budget, OOM and escaping | full long decimals; one error before output | both output-safety registrations |
| failures: alloc, budget, over-long key, packed fault | | `ar_limits_run`, core, packed (d)(f) |

Bitmap-dictionary route case to add to `ar_limits_run` (before the budget
block):

```c
    /* Bitmap-dictionary group route: labels stay exact for packed/legacy
       numeric and zero values are kept (the old `<= 0 → continue` dropped
       zero-valued groups). */
    r = ar_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3bm\","
               "\"splits\":8,\"max_key\":16,\"fields\":[\"n:numeric:19,4\"],"
               "\"indexes\":[\"n:bitmap\"]}");
    shard_db_free_result(r);
    r = ar_req("{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"r3bm\",\"records\":["
               "{\"key\":\"z\",\"value\":{\"n\":0}},{\"key\":\"o\",\"value\":{\"n\":1}}]}");
    shard_db_free_result(r);
    ar_expect("r3bm", ",\"group_by\":[\"n\"]", "{\"fn\":\"count\",\"alias\":\"c\"}",
              "[{\"n\":\"0\",\"c\":1},{\"n\":\"1\",\"c\":1}]", "bitmap dict keeps zero group");
    tu_pdb_drop_object(ar_db, "default", "r3bm");
```

## 10. Files expected in the diff

Added: `src/db/agg_exact.h`, `src/db/agg_exact.c`,
`src/test/cases/test_decimal_order_decode.c`, `test_agg_exact_core.c`,
`test_agg_exact_routes.c`, `test_agg_packed_routes.c`,
`src/test/cases/agg_exact_vectors.h`.
Modified: `src/db/decimal_order.c`, `src/db/decimal_order.h`,
`src/db/query_aggregate.c`, `build.sh`,
`docs/query-protocol/aggregate.md`, `npm/index.d.ts`, `npm/README.md`,
`docs/reference/changelog.md`, and
`src/test/cases/test_aggregate_type_matrix.c` (class E only).
Nothing else. Not staged/committed: `PLAN_NOTES.md`, `HANDOFF.md`, logs,
scratch scripts, object files.

## 11. Risks and review focus (for the plan-blind reviewer)

1. **Ownership of heap-backed states** (`agg_ctx_release_nums`, partition
   splice nulling, merge release, VS ownership move, skipped-merge cleanup) —
   the double-free/leak surface; the budget counter returning to zero is
   asserted in the core test and in the packed route teardown.
2. **`TypedRowView` strictness for packed records** — a malformed packed
   record is now an error (previously a silent zero); legacy records keep the
   "all-missing" degrade.
3. **Index-key semantics** — `agg_num_offer_key` assumes the R2 tier/payload
   layout; bare numeric is gated (D1); bounded display scale is the schema's
   constant. Any future key-format change must update `decimal_order_decode`
   and its test together.
4. **Behaviour changes to call out to the human** (all in CHANGELOG): declared
   scale preserved in numeric results; `avg` of no values is `null` on all
   JSON routes (CSV keeps `0`); numeric group fields order numerically;
   over-long group value is an explicit error (D6 — new restriction);
   invalid `having` literal is an error; float aggregates print shortest
   round-trip; timestamp group-by now works.
5. **Out of scope, unchanged**: the `agg_hash` strlen-on-tagged-key partition
   skew (performance only), the varchar NUL policy (R4), buffered-fetch
   performance (deferred), public packed activation (R6), bare-numeric key
   scale (R5).

## 11b. Deferred follow-up (not R3)

D7 makes streaming top-N decline to the hash path unless every spec is
`count(*)`. Restoring speed is a post-type-correctness optimisation; see
`docs/plans/2026-10-30-topn-stream-perf-note.md`.

## 12. Approval requested

Approve or amend decisions **D1–D8** (§2) and name the executor. On approval
the executor runs Tasks 0–7 in order on the current branch, leaving all
changes uncommitted for the raw-diff review; sanitizer and benchmark runs
remain the human's.

## Appendix A — build.sh registration anchors (verified at HEAD)

- Test list: after the line `    src/test/cases/test_decnum_core.c \` (line
  354) insert the decoder/core test lines; after
  `    src/test/cases/test_indexed_zero_exists.c \` (line 199), Task 3 first
  inserts only the legacy route-test line; the packed line is added after
  Task 4 supplies its schema seam. Each inserted line ends with ` \` and the final entry of
  each list keeps its existing terminator.
- Source lists: `src/db/decimal_order.c` occurs **five** times (lines 111,
  118, 161, 170, 543); replace each occurrence with
  `src/db/decimal_order.c src/db/agg_exact.c`.

## Appendix B — vectors-header provenance

The committed `agg_exact_vectors.h` below was generated at seed 20261030 and
its `AGG_VECS` entries pass the core test under every partition/merge order.
The §7.3 literals are hand-frozen from the oracle and take precedence over the
generator on any disagreement (the generator is fixed, never the literal).

### Task 1 production code

**`src/db/decimal_order.h` — replace the whole file with:**

```c
#ifndef SHARD_DECIMAL_ORDER_H
#define SHARD_DECIMAL_ORDER_H
#include "decnum.h"
/* Untiered canonical value key; status 0/-1, length reset on failure.
   NULL output is a validating measurement request. */
int decimal_order_into(const dec_t *, uint8_t *, size_t, size_t *);
/* Inverse of decimal_order_into for one untiered payload (tier byte already
   removed). `out` must be an initialized/freed dec_t; on success it holds the
   exact value with the MINIMAL lossless display scale, on failure it is the
   canonical zero and errno is EILSEQ (malformed/non-canonical payload,
   outside the finite digit limits) or ENOMEM. Zero decodes from the single
   byte 01. Never trusts the length: every limb, the weight, the terminator
   and the digit-limit domain are validated. */
int decimal_order_decode(const uint8_t *in, size_t n, dec_t *out);
/* Smallest display scale that renders d losslessly (0 for zero and for
   integers); independent of d->dscale. */
int32_t decimal_min_dscale(const dec_t *d);
#endif
```

**`src/db/decimal_order.c` — append after the final `}` of `decimal_order_into` (the file currently ends `*len = need;\n  return 0;\n}`):**

```c
int32_t decimal_min_dscale(const dec_t *d) {
  if (!d || !d->ndigits || !d->digits)
    return 0;
  uint16_t last = d->digits[d->ndigits - 1];
  int tz = 0;
  while (last && last % 10 == 0) {
    tz++;
    last /= 10;
  }
  int64_t exponent = ((int64_t)d->weight - d->ndigits + 1) * 4 + tz;
  return exponent < 0 ? (int32_t)-exponent : 0;
}

int decimal_order_decode(const uint8_t *in, size_t n, dec_t *out) {
  if (!out) {
    errno = EINVAL;
    return -1;
  }
  dec_free(out);
  if (!in || n == 0)
    return invalid();
  if (n == 1) {
    if (in[0] != 1)
      return invalid();
    return 0;
  }
  if (n < 7 || ((n - 7) & 1))
    return invalid();
  size_t nd = (n - 7) / 2;
  if (nd > DEC_MAX_LIMBS || (in[0] != 0 && in[0] != 2))
    return invalid();
  int neg = in[0] == 0;
  uint8_t x = neg ? 255 : 0; /* undo the negative-key byte inversion */
  uint32_t w = ((uint32_t)(in[1] ^ x) << 24) | ((uint32_t)(in[2] ^ x) << 16) |
               ((uint32_t)(in[3] ^ x) << 8) | (uint32_t)(in[4] ^ x);
  int32_t weight = (int32_t)(w ^ UINT32_C(0x80000000));
  if ((uint8_t)(in[n - 2] ^ x) != 0 || (uint8_t)(in[n - 1] ^ x) != 0)
    return invalid();
  if (dec_reserve(out, (uint16_t)nd) != DEC_OK) {
    errno = ENOMEM;
    return -1;
  }
  for (size_t i = 0; i < nd; i++) {
    uint16_t raw = (uint16_t)(((uint16_t)(in[5 + 2 * i] ^ x) << 8) |
                              (uint16_t)(in[6 + 2 * i] ^ x));
    if (raw < 1 || raw > 10000) {
      dec_free(out);
      return invalid();
    }
    out->digits[i] = (uint16_t)(raw - 1);
  }
  if (!out->digits[0] || !out->digits[nd - 1]) {
    dec_free(out);
    return invalid();
  }
  out->ndigits = (uint16_t)nd;
  out->weight = weight;
  out->sign = neg ? DEC_SIGN_NEG : DEC_SIGN_POS;
  /* same finite domain as the encoder / dec_unpack */
  uint16_t first = out->digits[0];
  int fd = first >= 1000 ? 4 : first >= 100 ? 3 : first >= 10 ? 2 : 1;
  int32_t md = decimal_min_dscale(out);
  if ((int64_t)weight * 4 + fd > DEC_MAX_INT_DIGITS ||
      md > DEC_MAX_FRAC_DIGITS) {
    dec_free(out);
    return invalid();
  }
  out->dscale = md;
  return 0;
}
```

### Task 2 production code

**New `src/db/agg_exact.h` (complete file):**

```c
/* src/db/agg_exact.h
 *
 * Exact aggregate states. Integer-class fields and legacy numeric accumulate
 * in checked 128-bit integers, packed numeric in the decnum core, float/double
 * stay binary floating. Nothing here touches I/O, locks or schema loading;
 * every function is thread-confined to the AggEnv/AggNum it is handed. */
#ifndef AGG_EXACT_H
#define AGG_EXACT_H
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include "decnum.h"
#include "types.h"
#include "typed_row.h"

typedef __int128 agg_i128;
enum AggFn { AGG_COUNT, AGG_SUM, AGG_AVG, AGG_MIN, AGG_MAX };
typedef enum { AGC_NONE = 0, AGC_INT, AGC_DEC, AGC_FLT } AggClass;

typedef struct {
    AggClass cls;
    int      scale;    /* AGC_INT: decimal places of the stored integer (legacy numeric S, else 0) */
    int      dscale;   /* AGC_DEC: constant display scale of a bounded column (max(S,0)); bare: 0 */
    int      bare;     /* AGC_DEC: bare numeric - display scale is per value */
    size_t   index;    /* schema field index */
    enum FieldType type;
} AggNumSpec;

typedef struct {
    _Atomic size_t *bytes;   /* shared QUERY_BUFFER_MB counter; NULL = unmetered */
    size_t          limit;
    dec_t           val;     /* decoded-input scratch (one per ctx/worker) */
} AggEnv;

typedef struct {
    uint8_t has;             /* min/max hold a value */
    uint8_t float_sum_overflow; /* sticky; only SUM/AVG consume this error */
    union {
        struct { agg_i128 sum, min, max; } i;
        struct { dec_t    sum, min, max; } d;
        struct { double   sum, min, max; } f;
    } u;
} AggNum;                    /* all-zero is the valid empty state */

typedef struct AggValue {
    AggClass cls;
    int      present;        /* 0: no contributing value */
    agg_i128 i;  int scale;  /* AGC_INT */
    const dec_t *dv;         /* AGC_DEC: borrowed from the AggNum or &own */
    dec_t    own;            /* storage for computed decimals (avg) */
    double   f;              /* AGC_FLT */
} AggValue;                  /* never copy by value once dv may be &own */

/* 1 when the field can be sum/avg/min/max-ed (fills *out), else 0. */
int  agg_num_spec_for(const TypedSchema *ts, size_t idx, AggNumSpec *out);
/* 0 for bare packed numeric: its ordered index key lacks the display scale, so
   index-only VALUE shortcuts must not be used (decision D1). */
int  agg_index_exact(const AggNumSpec *sp);

void agg_env_init(AggEnv *e, _Atomic size_t *bytes, size_t limit);
void agg_env_free(AggEnv *e);
void agg_num_release(AggNum *n, AggClass cls, AggEnv *e);

/* Return 1 value accumulated, 0 absent (NULL / incomplete / foreign tier),
   -1 error (errno: ENOMEM, ENOBUFS = budget, ERANGE = overflow/non-finite,
   EILSEQ = malformed). The caller owns the per-spec count. */
int  agg_num_offer_row(const AggNumSpec *sp, const TypedRowView *row,
                       AggNum *n, AggEnv *e);
int  agg_num_offer_key(const AggNumSpec *sp, const uint8_t *key, size_t klen,
                       AggNum *n, AggEnv *e);
/* Direct offers (also the unit-test surface): same contract, 0 ok / -1 errno.
   The caller owns the per-spec contributing-value count. A finite floating
   sum overflow is sticky state, checked by agg_num_value for SUM/AVG only;
   MIN/MAX and presence counts remain usable. Non-finite input is -1. */
int  agg_num_offer_int(AggNum *n, agg_i128 v);
int  agg_num_offer_double(AggNum *n, double v);
int  agg_num_offer_decimal(AggNum *n, AggEnv *e, const dec_t *v);
/* Decode a floating-class ordered key (top-N streaming only). */
int  agg_key_double(const AggNumSpec *sp, const uint8_t *key, size_t klen,
                    double *out);
int  agg_num_merge(AggNum *dst, const AggNum *src, AggClass cls, AggEnv *e);

void agg_value_init(AggValue *v);
void agg_value_release(AggValue *v);
void agg_value_from_count(AggValue *v, int64_t count);
/* Result of one spec over `count` contributing values. 0 ok / -1 errno. */
int  agg_num_value(const AggNum *n, const AggNumSpec *sp, enum AggFn fn,
                   int64_t count, AggValue *out);
/* Parse a canonical group label (INT/DEC via dec_parse, FLT via strtod). */
int  agg_value_from_label(const AggNumSpec *sp, const char *label, AggValue *out);
/* malloc'd number lexeme for a PRESENT value; NULL with errno on failure. */
char *agg_value_render(const AggValue *v);
char *agg_render_double(double v);
/* Same-spec ordering, allocation-free (sort comparator). */
int  agg_value_cmp(const AggValue *a, const AggValue *b);
/* Compare to a numeric literal. 0 ok (*cmp = -1/0/1) / -1 errno (EINVAL when
   the literal is not a finite number). */
int  agg_value_cmp_text(const AggValue *a, const char *lit, int *cmp);
/* Label text of a decimal: packed cell or ordered key payload. D2 scale rule.
   Returns length (>=0) or -1 (errno; ENAMETOOLONG when buf is too small). */
int  agg_dec_label(const AggNumSpec *sp, const uint8_t *p, size_t plen,
                   int ordered, char *buf, size_t cap, AggEnv *e);

#ifdef TEST_BUILD
void agg_exact_test_set_fail_alloc(int fail_n);  /* Nth guarded allocation fails; 0 = never */
#endif
#endif
```

**New `src/db/agg_exact.c` (complete file):**

```c
#include "agg_exact.h"
#include "decimal_order.h"
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef TEST_BUILD
static _Atomic int g_agg_fail_alloc;
void agg_exact_test_set_fail_alloc(int n) { atomic_store(&g_agg_fail_alloc, n); }
static int fault(void) {
    int n = atomic_load(&g_agg_fail_alloc);
    while (n > 0) {
        if (atomic_compare_exchange_weak(&g_agg_fail_alloc, &n, n - 1))
            return n == 1;
    }
    return 0;
}
#else
static int fault(void) { return 0; }
#endif

static const dec_t ZERO_DEC = {0};

static int fail(int e) { errno = e; return -1; }
static int dec_errno(dec_status st) {
    switch (st) {
    case DEC_OK: return 0;
    case DEC_ERR_NOMEM: return ENOMEM;
    case DEC_ERR_RANGE: case DEC_ERR_PRECISION: return ERANGE;
    default: return EILSEQ;
    }
}

int agg_num_spec_for(const TypedSchema *ts, size_t idx, AggNumSpec *out) {
    memset(out, 0, sizeof *out);
    if (!ts || idx >= (size_t)ts->nfields) return 0;
    const TypedField *f = &ts->fields[idx];
    out->index = idx;
    out->type = f->type;
    switch (f->type) {
    case FT_LONG: case FT_INT: case FT_SHORT: case FT_BYTE: case FT_BOOL:
    case FT_TIMESTAMP:
        out->cls = AGC_INT;
        return 1;
    case FT_DOUBLE: case FT_FLOAT:
        out->cls = AGC_FLT;
        return 1;
    case FT_NUMERIC:
        if (ts->packed.nfields) {
            const PkrField *p = &ts->packed.fields[idx];
            out->cls = AGC_DEC;
            out->bare = p->bare;
            out->dscale = (!p->bare && p->scale > 0) ? p->scale : 0;
            return 1;
        }
        out->cls = AGC_INT;
        out->scale = f->numeric_scale_mult > 0 ? f->numeric_scale : 0;
        return 1;
    default:
        return 0;
    }
}

int agg_index_exact(const AggNumSpec *sp) {
    return !(sp->cls == AGC_DEC && sp->bare);
}

void agg_env_init(AggEnv *e, _Atomic size_t *bytes, size_t limit) {
    memset(e, 0, sizeof *e);
    e->bytes = bytes;
    e->limit = limit;
}
void agg_env_free(AggEnv *e) { dec_free(&e->val); }

static int charge(AggEnv *e, size_t add) {
    if (!e->bytes || !add) return 0;
    size_t prev = atomic_fetch_add_explicit(e->bytes, add, memory_order_relaxed);
    if (prev + add > e->limit) {
        atomic_fetch_sub_explicit(e->bytes, add, memory_order_relaxed);
        return fail(ENOBUFS);
    }
    return 0;
}
static void uncharge(AggEnv *e, size_t n) {
    if (e->bytes && n) atomic_fetch_sub_explicit(e->bytes, n, memory_order_relaxed);
}
static size_t dec_bytes(const dec_t *d) { return (size_t)d->cap * sizeof(uint16_t); }

/* Move `fresh` into `slot`, charging only the storage growth; consumes fresh. */
static int slot_put(AggEnv *e, dec_t *slot, dec_t *fresh) {
    size_t nb = dec_bytes(fresh), ob = dec_bytes(slot);
    if (nb > ob && charge(e, nb - ob)) { dec_free(fresh); return -1; }
    if (nb < ob) uncharge(e, ob - nb);
    dec_free(slot);
    *slot = *fresh;
    dec_init(fresh);
    return 0;
}
static int slot_copy(AggEnv *e, dec_t *slot, const dec_t *src) {
    dec_t fresh; dec_init(&fresh);
    if (fault()) return fail(ENOMEM);
    dec_status st = dec_add(src, &ZERO_DEC, &fresh);   /* exact copy, keeps dscale */
    if (st != DEC_OK) { dec_free(&fresh); return fail(dec_errno(st)); }
    return slot_put(e, slot, &fresh);
}
static int slot_add(AggEnv *e, dec_t *slot, const dec_t *v) {
    dec_t fresh; dec_init(&fresh);
    if (fault()) return fail(ENOMEM);
    dec_status st = dec_add(slot, v, &fresh);
    if (st != DEC_OK) { dec_free(&fresh); return fail(dec_errno(st)); }
    return slot_put(e, slot, &fresh);
}

void agg_num_release(AggNum *n, AggClass cls, AggEnv *e) {
    if (cls == AGC_DEC) {
        uncharge(e, dec_bytes(&n->u.d.sum) + dec_bytes(&n->u.d.min) +
                    dec_bytes(&n->u.d.max));
        dec_free(&n->u.d.sum); dec_free(&n->u.d.min); dec_free(&n->u.d.max);
    }
    memset(n, 0, sizeof *n);
}

int agg_num_offer_int(AggNum *n, agg_i128 v) {
    agg_i128 s;
    if (__builtin_add_overflow(n->u.i.sum, v, &s)) return fail(ERANGE);
    n->u.i.sum = s;
    if (!n->has) { n->u.i.min = n->u.i.max = v; n->has = 1; }
    else { if (v < n->u.i.min) n->u.i.min = v; if (v > n->u.i.max) n->u.i.max = v; }
    return 0;
}
int agg_num_offer_double(AggNum *n, double v) {
    if (!isfinite(v)) return fail(ERANGE);
    if (!n->float_sum_overflow) {
        double s = n->u.f.sum + v;
        if (!isfinite(s)) n->float_sum_overflow = 1;
        else n->u.f.sum = s;
    }
    if (!n->has) { n->u.f.min = n->u.f.max = v; n->has = 1; }
    else { if (v < n->u.f.min) n->u.f.min = v; if (v > n->u.f.max) n->u.f.max = v; }
    return 0;
}
/* min/max with the commutative tie rule: equal value => larger display scale. */
static int dec_minmax(AggNum *n, AggEnv *e, const dec_t *vmin, const dec_t *vmax) {
    if (!n->has) {
        if (slot_copy(e, &n->u.d.min, vmin) || slot_copy(e, &n->u.d.max, vmax)) return -1;
        n->has = 1;
        return 0;
    }
    int c = dec_cmp(vmin, &n->u.d.min);
    if ((c < 0 || (c == 0 && vmin->dscale > n->u.d.min.dscale)) &&
        slot_copy(e, &n->u.d.min, vmin)) return -1;
    c = dec_cmp(vmax, &n->u.d.max);
    if ((c > 0 || (c == 0 && vmax->dscale > n->u.d.max.dscale)) &&
        slot_copy(e, &n->u.d.max, vmax)) return -1;
    return 0;
}
int agg_num_offer_decimal(AggNum *n, AggEnv *e, const dec_t *v) {
    if (slot_add(e, &n->u.d.sum, v)) return -1;
    return dec_minmax(n, e, v, v);
}

/* Big-endian signed reader (record bytes) and ordered-key readers (sign bit
   flipped / IEEE total-order transform). */
static agg_i128 rd_i(const uint8_t *p, int w) {
    uint64_t u = 0;
    for (int i = 0; i < w; i++) u = (u << 8) | p[i];
    return (agg_i128)((int64_t)(u << (64 - 8 * w)) >> (64 - 8 * w));
}
static agg_i128 key_i(const uint8_t *p, int w) {
    uint64_t u = 0;
    for (int i = 0; i < w; i++) u = (u << 8) | p[i];
    u ^= (uint64_t)1 << (8 * w - 1);
    return (agg_i128)((int64_t)(u << (64 - 8 * w)) >> (64 - 8 * w));
}
static double key_d(const uint8_t *p, int w) {
    if (w == 8) {
        uint64_t b = 0;
        for (int i = 0; i < 8; i++) b = (b << 8) | p[i];
        b = (b & ((uint64_t)1 << 63)) ? b ^ ((uint64_t)1 << 63) : ~b;
        double v; memcpy(&v, &b, 8); return v;
    }
    uint32_t b = 0;
    for (int i = 0; i < 4; i++) b = (b << 8) | p[i];
    b = (b & 0x80000000u) ? b ^ 0x80000000u : ~b;
    float f; memcpy(&f, &b, 4); return (double)f;
}

int agg_num_offer_row(const AggNumSpec *sp, const TypedRowView *row,
                      AggNum *n, AggEnv *e) {
    TypedRowSpan span;
    if (typed_row_span(row, sp->index, &span)) return -1;
    if (span.is_null || !span.complete) return 0;
    const uint8_t *p = span.data;
    switch (sp->cls) {
    case AGC_DEC: {
        dec_status st = dec_unpack(span.data, span.len, &e->val);
        if (st != DEC_OK) return fail(dec_errno(st));
        return agg_num_offer_decimal(n, e, &e->val) ? -1 : 1;
    }
    case AGC_FLT: {
        double v;
        if (sp->type == FT_FLOAT) { float f; memcpy(&f, p, 4); v = (double)f; }
        else memcpy(&v, p, 8);
        return agg_num_offer_double(n, v) ? -1 : 1;
    }
    case AGC_INT: {
        agg_i128 v;
        switch (sp->type) {
        case FT_LONG: case FT_TIMESTAMP: case FT_NUMERIC: v = rd_i(p, 8); break;
        case FT_INT:   v = rd_i(p, 4); break;
        case FT_SHORT: v = rd_i(p, 2); break;
        case FT_BOOL:  v = p[0] ? 1 : 0; break;
        case FT_BYTE:  v = p[0]; break;
        default: return 0;
        }
        return agg_num_offer_int(n, v) ? -1 : 1;
    }
    default: return 0;
    }
}

static int key_payload(const AggNumSpec *sp, const uint8_t *k, size_t kl,
                       const uint8_t **p, size_t *pl) {
    (void)sp;
    if (!k || kl == 0 || k[0] != 0x00) return 0;     /* NULL tier / foreign tier */
    *p = k + 1; *pl = kl - 1;
    return 1;
}

int agg_key_double(const AggNumSpec *sp, const uint8_t *key, size_t klen,
                   double *out) {
    const uint8_t *p; size_t pl;
    if (sp->cls != AGC_FLT || !key_payload(sp, key, klen, &p, &pl)) return 0;
    int w = sp->type == FT_FLOAT ? 4 : 8;
    if (pl < (size_t)w) return 0;
    *out = key_d(p, w);
    return isfinite(*out) ? 1 : fail(ERANGE);
}

int agg_num_offer_key(const AggNumSpec *sp, const uint8_t *key, size_t klen,
                      AggNum *n, AggEnv *e) {
    const uint8_t *p; size_t pl;
    if (!key_payload(sp, key, klen, &p, &pl)) return 0;
    switch (sp->cls) {
    case AGC_FLT: {
        double v; int r = agg_key_double(sp, key, klen, &v);
        return r == 1 ? (agg_num_offer_double(n, v) ? -1 : 1) : r;
    }
    case AGC_INT: {
        int w;
        switch (sp->type) {
        case FT_LONG: case FT_TIMESTAMP: case FT_NUMERIC: w = 8; break;
        case FT_INT: w = 4; break;
        case FT_SHORT: w = 2; break;
        case FT_BOOL: case FT_BYTE:
            if (pl < 1) return 0;
            return agg_num_offer_int(n, sp->type == FT_BOOL ? (p[0] ? 1 : 0) : p[0]) ? -1 : 1;
        default: return 0;
        }
        if (pl < (size_t)w) return 0;
        return agg_num_offer_int(n, key_i(p, w)) ? -1 : 1;
    }
    case AGC_DEC: {
        if (!agg_index_exact(sp)) return fail(ENOTSUP);   /* caller must gate (D1) */
        if (decimal_order_decode(p, pl, &e->val)) return -1;
        if (e->val.dscale < sp->dscale) e->val.dscale = sp->dscale;
        return agg_num_offer_decimal(n, e, &e->val) ? -1 : 1;
    }
    default: return 0;
    }
}

int agg_num_merge(AggNum *dst, const AggNum *src, AggClass cls, AggEnv *e) {
    switch (cls) {
    case AGC_INT: {
        agg_i128 s;
        if (__builtin_add_overflow(dst->u.i.sum, src->u.i.sum, &s)) return fail(ERANGE);
        dst->u.i.sum = s;
        if (src->has) {
            if (!dst->has) { dst->u.i.min = src->u.i.min; dst->u.i.max = src->u.i.max; dst->has = 1; }
            else {
                if (src->u.i.min < dst->u.i.min) dst->u.i.min = src->u.i.min;
                if (src->u.i.max > dst->u.i.max) dst->u.i.max = src->u.i.max;
            }
        }
        return 0;
    }
    case AGC_FLT: {
        if (src->float_sum_overflow) dst->float_sum_overflow = 1;
        if (!dst->float_sum_overflow) {
            double s = dst->u.f.sum + src->u.f.sum;
            if (!isfinite(s)) dst->float_sum_overflow = 1;
            else dst->u.f.sum = s;
        }
        if (src->has) {
            if (!dst->has) { dst->u.f.min = src->u.f.min; dst->u.f.max = src->u.f.max; dst->has = 1; }
            else {
                if (src->u.f.min < dst->u.f.min) dst->u.f.min = src->u.f.min;
                if (src->u.f.max > dst->u.f.max) dst->u.f.max = src->u.f.max;
            }
        }
        return 0;
    }
    case AGC_DEC:
        if (slot_add(e, &dst->u.d.sum, &src->u.d.sum)) return -1;
        return src->has ? dec_minmax(dst, e, &src->u.d.min, &src->u.d.max) : 0;
    default: return 0;
    }
}

void agg_value_init(AggValue *v) { memset(v, 0, sizeof *v); }
void agg_value_release(AggValue *v) { dec_free(&v->own); memset(v, 0, sizeof *v); }
void agg_value_from_count(AggValue *v, int64_t c) {
    agg_value_init(v);
    v->cls = AGC_INT; v->present = 1; v->i = c;
}

/* ---- rendering ---- */
static char *render_i128(agg_i128 v, int scale) {
    char rev[48]; size_t nd = 0;
    int neg = v < 0;
    unsigned __int128 m = neg ? (unsigned __int128)0 - (unsigned __int128)v
                              : (unsigned __int128)v;
    do { rev[nd++] = (char)('0' + (int)(m % 10)); m /= 10; } while (m);
    while (nd < (size_t)scale + 1) rev[nd++] = '0';
    char *out = malloc(nd + 4);
    if (!out || fault()) { free(out); errno = ENOMEM; return NULL; }
    size_t o = 0, intd = nd - (size_t)scale;
    if (neg) out[o++] = '-';
    for (size_t i = 0; i < intd; i++) out[o++] = rev[nd - 1 - i];
    if (scale) {
        out[o++] = '.';
        for (size_t i = 0; i < (size_t)scale; i++) out[o++] = rev[scale - 1 - i];
    }
    out[o] = 0;
    return out;
}
char *agg_render_double(double v) {
    if (!isfinite(v)) { errno = ERANGE; return NULL; }
    char buf[40];
    for (int prec = 15; prec <= 17; prec++) {
        snprintf(buf, sizeof buf, "%.*g", prec, v);
        if (strtod(buf, NULL) == v || prec == 17) break;
    }
    char *o = fault() ? NULL : strdup(buf);
    if (!o) errno = ENOMEM;
    return o;
}
char *agg_value_render(const AggValue *v) {
    switch (v->cls) {
    case AGC_INT: return render_i128(v->i, v->scale);
    case AGC_FLT: return agg_render_double(v->f);
    case AGC_DEC: {
        size_t n = dec_text_len(v->dv), got = 0;
        char *o = fault() ? NULL : malloc(n);
        if (!o) { errno = ENOMEM; return NULL; }
        dec_status st = dec_to_text(v->dv, o, n, &got);
        if (st != DEC_OK) { free(o); errno = dec_errno(st); return NULL; }
        return o;
    }
    default: errno = EINVAL; return NULL;
    }
}

static int int_to_dec(agg_i128 v, int scale, dec_t *out) {
    char *t = render_i128(v, scale);
    if (!t) return -1;
    dec_status st = dec_parse(t, strlen(t), out, NULL, 0);
    free(t);
    return st == DEC_OK ? 0 : fail(dec_errno(st));
}

int agg_num_value(const AggNum *n, const AggNumSpec *sp, enum AggFn fn,
                  int64_t count, AggValue *out) {
    agg_value_init(out);
    out->cls = sp->cls;
    out->present = count > 0;
    if (sp->cls == AGC_FLT && n->float_sum_overflow &&
        (fn == AGG_SUM || fn == AGG_AVG)) return fail(ERANGE);
    if (fn == AGG_AVG) {
        if (sp->cls != AGC_FLT) out->cls = AGC_DEC;   /* integer averages are decimals */
        if (count <= 0) return 0;
        if (sp->cls == AGC_FLT) { out->f = n->u.f.sum / (double)count;
            return isfinite(out->f) ? 0 : fail(ERANGE); }
        dec_t sum; dec_init(&sum);
        const dec_t *sp_dec = &n->u.d.sum;
        if (sp->cls == AGC_INT) {
            if (int_to_dec(n->u.i.sum, sp->scale, &sum)) return -1;
            sp_dec = &sum;
        }
        dec_status st = dec_div_avg(sp_dec, (uint64_t)count, &out->own);
        dec_free(&sum);
        if (st != DEC_OK) return fail(dec_errno(st));
        out->cls = AGC_DEC; out->dv = &out->own;
        return 0;
    }
    switch (sp->cls) {
    case AGC_INT:
        out->scale = sp->scale;
        out->i = fn == AGG_SUM ? n->u.i.sum : fn == AGG_MIN ? n->u.i.min : n->u.i.max;
        break;
    case AGC_FLT:
        out->f = fn == AGG_SUM ? n->u.f.sum : fn == AGG_MIN ? n->u.f.min : n->u.f.max;
        break;
    case AGC_DEC:
        out->dv = fn == AGG_SUM ? &n->u.d.sum : fn == AGG_MIN ? &n->u.d.min : &n->u.d.max;
        break;
    default: break;
    }
    return 0;
}

int agg_value_from_label(const AggNumSpec *sp, const char *label, AggValue *out) {
    agg_value_init(out);
    out->cls = sp->cls == AGC_FLT ? AGC_FLT : AGC_DEC;
    out->present = label[0] != '\0';
    if (!out->present) return 0;
    if (sp->cls == AGC_FLT) {
        char *end; out->f = strtod(label, &end);
        return (*end || !isfinite(out->f)) ? fail(EILSEQ) : 0;
    }
    dec_status st = dec_parse(label, strlen(label), &out->own, NULL, 0);
    if (st != DEC_OK) return fail(dec_errno(st));
    out->dv = &out->own;
    return 0;
}

int agg_value_cmp(const AggValue *a, const AggValue *b) {
    if (a->cls == AGC_FLT) return a->f < b->f ? -1 : a->f > b->f;
    if (a->cls == AGC_INT && b->cls == AGC_INT && a->scale == b->scale)
        return a->i < b->i ? -1 : a->i > b->i;
    return dec_cmp(a->dv, b->dv);   /* callers materialise INT as DEC when scales differ */
}

int agg_value_cmp_text(const AggValue *a, const char *lit, int *cmp) {
    if (a->cls == AGC_FLT) {
        char *end; double d = strtod(lit, &end);
        if (end == lit || *end || !isfinite(d)) return fail(EINVAL);
        *cmp = a->f < d ? -1 : a->f > d;
        return 0;
    }
    dec_t l; dec_init(&l);
    dec_status st = dec_parse(lit, strlen(lit), &l, NULL, 0);
    if (st != DEC_OK) { dec_free(&l); return fail(st == DEC_ERR_NOMEM ? ENOMEM : EINVAL); }
    dec_t tmp; dec_init(&tmp);
    const dec_t *lhs = a->dv;
    if (a->cls == AGC_INT) {
        if (int_to_dec(a->i, a->scale, &tmp)) { dec_free(&l); return -1; }
        lhs = &tmp;
    }
    *cmp = dec_cmp(lhs, &l);
    dec_free(&tmp); dec_free(&l);
    return 0;
}

int agg_dec_label(const AggNumSpec *sp, const uint8_t *p, size_t plen,
                  int ordered, char *buf, size_t cap, AggEnv *e) {
    if (sp->cls != AGC_DEC) return fail(EINVAL);
    if (ordered) { if (decimal_order_decode(p, plen, &e->val)) return -1; }
    else {
        dec_status st = dec_unpack(p, plen, &e->val);
        if (st != DEC_OK) return fail(dec_errno(st));
    }
    int32_t md = decimal_min_dscale(&e->val);
    e->val.dscale = sp->bare ? md : (md > sp->dscale ? md : sp->dscale);
    size_t need = dec_text_len(&e->val), got = 0;
    if (need > cap) return fail(ENAMETOOLONG);
    if (dec_to_text(&e->val, buf, cap, &got) != DEC_OK) return fail(EILSEQ);
    return (int)got;   /* dec_to_text reports the length without the NUL */
}
```

#### 4.1 — file head, types, top-N streaming, keyset-emit error channel

#### 4.1.1 includes
Anchor (exactly one occurrence in the file):
```text
#include "query_internal.h"
#include <math.h>
```
Replace with:
```c
#include "query_internal.h"
#include "agg_exact.h"
#include "decimal_order.h"
#include "typed_row.h"
#include <math.h>
#include <sys/stat.h>
```
#### 4.1.2 enum AggFn
Anchor (exactly one occurrence in the file):
```text
enum AggFn { AGG_COUNT, AGG_SUM, AGG_AVG, AGG_MIN, AGG_MAX };

typedef struct {
    enum AggFn fn;
```
Replace with:
```c
typedef struct {
    enum AggFn fn;
```
#### 4.1.3 AggAccum
Anchor (exactly one occurrence in the file):
```text
typedef struct {
    double sum;
    double min;
    double max;
    int64_t count;
    char *text_min;
    size_t text_min_len;
    char *text_max;
    size_t text_max_len;
    int text_present;
} AggAccum;
```
Replace with:
```c
typedef struct {
    int64_t count;          /* COUNT result / contributing-value count */
    AggNum  num;            /* sum/min/max (agg_exact) */
    char *text_min;
    size_t text_min_len;
    char *text_max;
    size_t text_max_len;
    int text_present;
} AggAccum;
```
#### 4.1.4 AggResult
Anchor (exactly one occurrence in the file):
```text
typedef struct {
    AggResultKind kind;
    int present;
    double number;
    const char *text;
    size_t text_len;
} AggResult;
```
Replace with:
```c
typedef struct {
    AggResultKind kind;
    int present;
    AggValue val;           /* AGG_RESULT_NUMBER */
    const char *text;       /* AGG_RESULT_TEXT */
    size_t text_len;
} AggResult;
```
#### 4.1.5 agg_index_exact_field
Anchor (exactly one occurrence in the file):
```text
static int agg_type_is_text(enum FieldType t) {
    return t == FT_VARCHAR || t == FT_DATE || t == FT_DATETIME || t == FT_DATETIMEMS;
}
```
Replace with:
```c
static int agg_type_is_text(enum FieldType t) {
    return t == FT_VARCHAR || t == FT_DATE || t == FT_DATETIME || t == FT_DATETIMEMS;
}

/* 1 when this field's index key carries its full exact value for value
   shortcuts (D1): false only for bare packed numeric, whose ordered key has
   no display scale. Callers fall through to the record-fed route. */
static int agg_index_exact_field(const TypedSchema *ts, const TypedField *tf) {
    AggNumSpec ns;
    return agg_num_spec_for(ts, (size_t)(tf - ts->fields), &ns) &&
           agg_index_exact(&ns);
}
```
#### 4.1.6 agg_index_label_f
Anchor (exactly one occurrence); insert immediately after it:
```text
static const char AGG_VALUE_TIER_CEIL[1] = { 0x01 };
```
Inserted text:
```c

/* Output cells own every allocation needed after the first success byte. */
typedef struct {
    char *lex, *escaped;
    int is_null, is_text;
    _Atomic size_t *budget;
    size_t limit, charged;
} AggCell;
static int agg_report_error(int);
static void agg_cells_bind(AggCell *, size_t, _Atomic size_t *, size_t);
static AggCell *agg_cells_alloc(size_t, _Atomic size_t *, size_t);
static void agg_cells_free(AggCell *, int);
static void agg_cells_dispose(AggCell *, size_t);
static int agg_cell_count(AggCell *, int64_t);
static int agg_cell_text(AggCell *, const char *, size_t, int);
static void agg_cell_emit_json(const AggCell *);

/* Group label from an index payload (tier byte already stripped; B+ tree leaf
   payloads and bitmap dictionary values share the decimal_order form for
   packed numeric). Packed numeric decodes with the D2 scale rule; everything
   else keeps the legacy renderer. Returns length >= 0, -1 on error (errno;
   E2BIG when the label would not fit - never truncated). `base` supplies the
   QUERY_BUFFER accounting (NULL = unmetered). */
static int agg_index_label_f(const TypedField *f, const AggNumSpec *ns,
                             int is_num, const AggEnv *base, const uint8_t *p,
                             size_t plen, char *buf, size_t cap) {
    buf[0] = '\0';
    if (is_num && ns->cls == AGC_DEC) {
        AggEnv env;
        agg_env_init(&env, base ? base->bytes : NULL, base ? base->limit : 0);
        int n = agg_dec_label(ns, p, plen, 1, buf, cap, &env);
        int e = errno;
        agg_env_free(&env);
        errno = e == ENAMETOOLONG ? E2BIG : e;
        return n;
    }
    if (f->type == FT_VARCHAR) {
        if (plen >= cap || (plen && memchr(p, 0, plen))) { errno = E2BIG; return -1; }
        if (plen) memcpy(buf, p, plen);
        buf[plen] = '\0';
        return (int)plen;
    }
    int n = decode_idx_to_buf(f, p, plen, buf, cap, 0);
    return n < 0 ? -1 : n;
}
```
#### 4.1.7 topn fwd decls
Anchor (exactly one occurrence in the file):
```text
/* Forward declarations for helpers defined later in this file that the
 * streaming top-N executor needs. */
static AggValueStatus decode_index_key_to_double(const TypedField *f,
                                        const uint8_t *key, size_t klen,
                                        double *out);
static void fmt_double(char *buf, size_t sz, double v);

```
Replace with:
```c
```
#### 4.1.8 TopNWalkCtx
Anchor (exactly one occurrence in the file):
```text
    const TypedField *gb_tf;   /* for future sum/min/max on the group field */
} TopNWalkCtx;
```
Replace with:
```c
    const TypedField *gb_tf;   /* for future sum/min/max on the group field */
    AggNumSpec gb_spec;        /* exact classification of the group field */
    int gb_numeric;            /* exact numeric group-label classification */
} TopNWalkCtx;
```
#### 4.1.9 topn numeric decode
Anchor (exactly one occurrence in the file):
```text
    if (c->gb_tf && c->gb_tf->type != FT_VARCHAR) {
        double v;
        if (decode_index_key_to_double(c->gb_tf,
                                        (const uint8_t *)enc_val, enc_val_len,
                                        &v) == AGG_VALUE_PRESENT) {
```
Replace with:
```c
    if (c->gb_numeric && c->gb_spec.cls == AGC_FLT) {
        double v;
        if (agg_key_double(&c->gb_spec, (const uint8_t *)enc_val, enc_val_len,
                           &v) == 1) {
```
#### 4.1.10 topn decline
Anchor (exactly one occurrence in the file):
```text
    if (order_spec_idx < 0) return -1;

    void *heap = topn_heap_new(limit, order_desc);
```
Replace with:
```c
    if (order_spec_idx < 0) return -1;

    /* The stream has only one row-count state, not per-field present counts.
       All value/presence-dependent specs use the exact hash executor (D7). */
    for (int i = 0; i < nspecs; i++)
        if (specs[i].fn != AGG_COUNT || specs[i].field[0]) return -2;

    void *heap = topn_heap_new(limit, order_desc);
```

#### 4.1.11 topn ctx setup
Anchor (exactly one occurrence in the file):
```text
    ctx.gb_tf          = resolve_idx_field(fs->ts, group_by_field);
```
Replace with:
```c
    ctx.gb_tf = resolve_idx_field(fs->ts, group_by_field);
    ctx.gb_numeric = ctx.gb_tf && ctx.gb_tf->type != FT_BOOL &&
        agg_num_spec_for(fs->ts, (size_t)(ctx.gb_tf - fs->ts->fields), &ctx.gb_spec);
```

#### 4.1.12 topn emit
Start anchor (exactly one occurrence):
```text
    /* Determine if the group_by field's btree stores raw bytes (varchar)
     * or encoded numeric bytes. For varchar, gkeys[i] is the raw string.
     * For numeric types, decode via decode_index_key_to_double + format. */
    int gb_is_varchar = ctx.gb_tf && ctx.gb_tf->type == FT_VARCHAR;
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
    OUT(want_total ? "],\"total\":null}\n" : "]\n");
```
Keep the end anchor text; replace everything from the start anchor's first byte up to (not including) the end anchor with:
```c
    /* heap was destroyed immediately after drain. Only drained arrays and
       rendered cells remain owned here; never destroy heap a second time. */
    size_t live = ((size_t)n_out + 1) *
        (4 * sizeof(double) + sizeof(int64_t) + sizeof(char *) + sizeof(size_t));
    for (int i = 0; i < n; i++) live += gklens[i] + 1;
    _Atomic size_t render_bytes = live;
    AggCell *cellv = agg_cells_alloc((size_t)n * (size_t)nspecs,
                                     &render_bytes, g_query_buffer_max_bytes);
    int rerr = cellv ? 0 : (errno ? errno : ENOMEM);
    AggCell *labels = rerr ? NULL : agg_cells_alloc((size_t)n,
                                     &render_bytes, g_query_buffer_max_bytes);
    if (!rerr && !labels) rerr = errno ? errno : ENOMEM;
    for (int i = 0; i < n && !rerr; i++) {
        const uint8_t *raw = (const uint8_t *)gkeys[i];
        size_t raw_len = gklens[i];
        /* Fixed-width NULL keys may have padding after their tier byte. */
        if (raw_len && raw[0] == 0x01) {
            if (agg_cell_text(&labels[i], NULL, 0, 0)) rerr = errno;
        } else if (!raw_len || raw[0] != 0x00 || !ctx.gb_tf) {
            rerr = EILSEQ;
        } else {
            char label[512];
            int len = agg_index_label_f(ctx.gb_tf, &ctx.gb_spec, ctx.gb_numeric,
                                        NULL, raw + 1, raw_len - 1,
                                        label, sizeof label);
            if (len < 0) rerr = errno ? errno : EILSEQ;
            else if (agg_cell_text(&labels[i], label, (size_t)len, 1)) rerr = errno;
        }
        for (int j = 0; j < nspecs && !rerr; j++)
            if (agg_cell_count(&cellv[(size_t)i * nspecs + j], counts[i]))
                rerr = errno ? errno : ENOMEM;
    }
    if (rerr) {
        agg_cells_dispose(labels, (size_t)n);
        agg_cells_dispose(cellv, (size_t)n * nspecs);
        for (int i = 0; i < n; i++) free(gkeys[i]);
        free(metrics); free(gkeys); free(gklens);
        free(counts); free(sums); free(mins); free(maxs);
        agg_report_error(rerr);
        return -3; /* one complete error response already emitted */
    }
    OUT(want_total ? "{\"rows\":[" : "[");
    for (int i = 0; i < n; i++) {
        if (i) OUT(",");
        OUT("{\"%s\":", group_by_field);
        agg_cell_emit_json(&labels[i]);
        for (int j = 0; j < nspecs; j++) {
            OUT(",\"%s\":", specs[j].alias);
            agg_cell_emit_json(&cellv[(size_t)i * nspecs + j]);
        }
        OUT("}");
        free(gkeys[i]);
    }
    agg_cells_dispose(labels, (size_t)n);
    agg_cells_dispose(cellv, (size_t)n * nspecs);
```

#### 4.1.13 keyset_emit_agg_cb
Anchor (exactly one occurrence in the file):
```text
    uint8_t stk[2048];
    uint8_t *block = (klen + vlen + 1 < sizeof(stk))
        ? stk : malloc(klen + vlen);
    if (block) {
        memcpy(block, key, klen);
        memcpy(block + klen, value, vlen);
        pthread_mutex_lock(&ctx->lock);
        agg_scan_cb(&hdr, block, ctx->agg_ctx);
        pthread_mutex_unlock(&ctx->lock);
        if (block != stk) free(block);
    }
    return 0;
}
```
Replace with:
```c
    uint8_t stk[2048];
    uint8_t *block = (klen + vlen + 1 < sizeof(stk))
        ? stk : malloc(klen + vlen);
    if (block) {
        memcpy(block, key, klen);
        memcpy(block + klen, value, vlen);
        pthread_mutex_lock(&ctx->lock);
        agg_scan_cb(&hdr, block, ctx->agg_ctx);
        pthread_mutex_unlock(&ctx->lock);
        if (block != stk) free(block);
    } else {
        agg_note_error(ctx->agg_ctx, ENOMEM);   /* never a silent skip */
        return 1;
    }
    return 0;
}
```
#### 4.1.14 keyset_emit_agg fallback
Anchor (exactly one occurrence in the file):
```text
            uint8_t stk[2048];
            uint8_t *block = (rr.klen + rr.vlen + 1 < sizeof(stk))
                ? stk : malloc(rr.klen + rr.vlen);
            if (block) {
                memcpy(block, rr.key, rr.klen);
                memcpy(block + rr.klen, rr.val, rr.vlen);
                agg_scan_cb(&hdr, block, agg_ctx);
                if (block != stk) free(block);
            }
            release_record_ref(&rr);
```
Replace with:
```c
            uint8_t stk[2048];
            uint8_t *block = (rr.klen + rr.vlen + 1 < sizeof(stk))
                ? stk : malloc(rr.klen + rr.vlen);
            if (block) {
                memcpy(block, rr.key, rr.klen);
                memcpy(block + rr.klen, rr.val, rr.vlen);
                agg_scan_cb(&hdr, block, agg_ctx);
                if (block != stk) free(block);
            } else {
                agg_note_error(agg_ctx, ENOMEM);
                release_record_ref(&rr);
                break;
            }
            release_record_ref(&rr);
```
#### 4.1.15 keyset_emit_agg sdb
Anchor (exactly one occurrence in the file):
```text
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &sinfo);
    if (sdb) {
        KeysetEmitAggCtx cb_ctx;
        memset(&cb_ctx, 0, sizeof(cb_ctx));
        cb_ctx.agg_ctx = agg_ctx;
        cb_ctx.dl = dl;
        pthread_mutex_init(&cb_ctx.lock, NULL);
        slotcask_bulk_resolve_and_fetch(sdb, hashes, n, &cb_ctx, keyset_emit_agg_cb);
        pthread_mutex_destroy(&cb_ctx.lock);
    }
    free(hashes);
```
Replace with:
```c
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &sinfo);
    if (sdb) {
        KeysetEmitAggCtx cb_ctx;
        memset(&cb_ctx, 0, sizeof(cb_ctx));
        cb_ctx.agg_ctx = agg_ctx;
        cb_ctx.dl = dl;
        pthread_mutex_init(&cb_ctx.lock, NULL);
        slotcask_bulk_resolve_and_fetch(sdb, hashes, n, &cb_ctx, keyset_emit_agg_cb);
        pthread_mutex_destroy(&cb_ctx.lock);
    } else {
        agg_note_error(agg_ctx, EIO);   /* registry miss: records unverifiable */
    }
    free(hashes);
```
#### 4.1.16 note_error fwd
Anchor (exactly one occurrence in the file):
```text
/* Forward decl — defined below. */
static int agg_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *raw_ctx);
```
Replace with:
```c
/* Forward decls — defined below. */
static int agg_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *raw_ctx);
static void agg_note_error(void*,int);
```

#### 4.1.17 topn reported error
Anchor (exactly one occurrence in the file):
```text
            if (rc == 0) {
                return 0;
            }
            if (rc == -1) {
```
Replace with:
```c
            if (rc == 0) {
                return 0;
            }
            if (rc == -3) return -1; /* error response was already written */
            if (rc == -1) {
```

#### 4.2 — record helpers, per-type timestamp labels, walk-fetch-check, scan callback feed

#### 4.2.1 delete agg_field_ptr
Start anchor (exactly one occurrence):
```text
/* Shared field-span resolver for the record-fed aggregate callbacks:
   returns the field bytes pointer and sets *avail to how many field
   bytes the record actually carries. A truncated field degrades to
   *avail < field size, so every converter's available<size check
   classifies it MISSING — never a fabricated zero. */
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
        ? fbase + tf->offset : g_zero_field_65537;
}
```
Delete everything from the start anchor's first byte through the end anchor's last byte inclusive and insert:
```text
(nothing — pure deletion)
```
#### 4.2.2 AggCtx tail
Anchor (exactly one occurrence in the file):
```text
    /* Integer group key optimization: 1 if all group fields are integer types */
    int use_int_keys;
} AggCtx;
static void agg_note_predicate_error(void *ctx,int error){((AggCtx*)ctx)->predicate_error=error;}
```
Replace with:
```c
    /* Integer group key optimization: 1 if all group fields are integer types */
    int use_int_keys;
    /* Exact aggregate state (agg_exact): scratch + QUERY_BUFFER accounting,
       the first exact-state/render/key failure (errno), and the numeric
       classification of every aggregate and group field. */
    AggEnv     env;
    int        agg_error;
    AggNumSpec spec_nums[MAX_AGG_SPECS];
    uint8_t    spec_is_num[MAX_AGG_SPECS];
    AggNumSpec group_nums[MAX_FIELDS];
    uint8_t    group_is_num[MAX_FIELDS];   /* numeric-class group field (D5; bool is text) */
} AggCtx;
static void agg_note_predicate_error(void *ctx,int error){((AggCtx*)ctx)->predicate_error=error;}
static void agg_note_error(void *ctx,int error){((AggCtx*)ctx)->agg_error=error;}

/* Ctx-level wrapper: label of group component `gi` from an index payload. */
static int agg_index_label(const AggCtx *ctx, int gi, const uint8_t *p,
                           size_t plen, char *buf, size_t cap) {
    return agg_index_label_f(ctx->group_tfs[gi], &ctx->group_nums[gi],
                             ctx->group_is_num[gi], &ctx->env, p, plen, buf, cap);
}

/* ---- typed-row access for the record-fed routes ---------------------- */

/* One validated view per record (legacy, nullmap or packed). A legacy view
   failure degrades exactly as before — every field reads as missing — but a
   packed schema never degrades silently: the failure is recorded. */
typedef struct { TypedRowView v; int ok; } AggRow;

static int typed_field_int_width(int ft);
static int typed_field_to_raw(const TypedField *f, const uint8_t *p,
                              uint8_t *buf, size_t bufsz);
static int agg_row_label(AggCtx *ctx, const AggRow *r, int gi, char *buf,
                         size_t cap, uint8_t *is_null);
static int agg_span_text(const AggCtx *ctx, const TypedField *f,
                         const TypedRowSpan *s, char *buf, size_t cap,
                         const char **text, size_t *len);

static int agg_row_open(AggCtx *ctx, const uint8_t *raw, size_t len,
                        uint8_t hdr2, AggRow *r) {
    memset(r, 0, sizeof *r);
    if (typed_row_open(ctx->fs->ts, raw, len, hdr2, &r->v) == 0) {
        r->ok = 1;
        return 0;
    }
    if (ctx->fs->ts && ctx->fs->ts->packed.nfields) {
        ctx->agg_error = errno ? errno : EILSEQ;
        return -1;
    }
    return 0;
}

/* 1 when the field span is readable (it may still be NULL / incomplete). */
static int agg_row_span(const AggRow *r, size_t idx, TypedRowSpan *s) {
    memset(s, 0, sizeof *s);
    return r->ok && typed_row_span(&r->v, idx, s) == 0;
}

static int agg_row_unset(const AggCtx *ctx, const TypedField *f,
                         const TypedRowSpan *s) {
    if (f->type == FT_VARCHAR && ctx->fs->ts->packed.nfields) return s->len == 0;
    if (!s->complete) return 1;
    return agg_field_is_unset(f, s->data);
}

/* Group label of component `gi`: length >= 0 (buf NUL-terminated), -1 with
   ctx->agg_error set. *is_null reports a NULL component. Over-long values and
   embedded NULs are an explicit error (E2BIG, decision D6), never a truncated
   key. Packed numeric uses the D2 scale rule; every other field keeps the
   historical typed_field_to_buf_raw rendering. */
static int agg_row_label(AggCtx *ctx, const AggRow *r, int gi, char *buf,
                         size_t cap, uint8_t *is_null) {
    buf[0] = '\0';
    *is_null = 0;
    const TypedField *f = ctx->group_tfs[gi];
    TypedRowSpan s;
    if (!agg_row_span(r, (size_t)ctx->group_idx[gi], &s)) return 0;
    if (s.is_null) { *is_null = 1; return 0; }
    if (f->type == FT_VARCHAR) {
        const uint8_t *c; size_t n;
        if (ctx->fs->ts->packed.nfields) { c = s.data; n = s.len; }
        else {
            if (!s.complete) return 0;
            int len = varchar_eff_len(s.data, f->size);
            c = s.data + 2;
            n = len > 0 ? (size_t)len : 0;
        }
        if (n >= cap || (n && memchr(c, 0, n))) { ctx->agg_error = E2BIG; return -1; }
        if (n) memcpy(buf, c, n);
        buf[n] = '\0';
        return (int)n;
    }
    if (!s.complete) return 0;
    if (ctx->group_is_num[gi] && ctx->group_nums[gi].cls == AGC_DEC) {
        int n = agg_dec_label(&ctx->group_nums[gi], s.data, s.len, 0, buf, cap,
                              &ctx->env);
        if (n < 0) { ctx->agg_error = errno == ENAMETOOLONG ? E2BIG : errno; return -1; }
        return n;
    }
    typed_field_to_buf_raw(f, s.data, buf, cap, 1);
    return (int)strlen(buf);
}

/* Raw fixed-width key for the all-integer group fast path (same bytes as
   before: tag byte + natural width, NULL = zeroed width). */
static int agg_row_raw_key(const AggCtx *ctx, const AggRow *r,
                           const uint8_t *gnulls, uint8_t *raw_key) {
    int kp = 0;
    for (int i = 0; i < ctx->ngroups && kp < AGG_INT_KEY_CAP; i++) {
        const TypedField *gtf = ctx->group_tfs[i];
        if (!gtf) continue;
        if (kp + 1 >= AGG_INT_KEY_CAP) break;
        raw_key[kp++] = gnulls[i] ? 1 : 0;
        int w = typed_field_int_width(gtf->type);
        if (gnulls[i]) {
            if (w <= 0 || kp + w > AGG_INT_KEY_CAP) break;
            memset(raw_key + kp, 0, (size_t)w);
            kp += w;
            continue;
        }
        TypedRowSpan s;
        if (!agg_row_span(r, (size_t)ctx->group_idx[i], &s) || !s.complete) break;
        int len = typed_field_to_raw(gtf, s.data, raw_key + kp,
                                     (size_t)(AGG_INT_KEY_CAP - kp));
        if (len > 0) kp += len;
    }
    return kp;
}
```
#### 4.2.3 tfbr timestamp
Anchor (exactly one occurrence in the file):
```text
    case FT_BYTE:
        return snprintf(buf, bufsz, "%u", p[0]);
    case FT_NUMERIC: {
        int64_t v = ld_be_i64(p);
        if (v == 0) { buf[0] = '0'; buf[1] = '\0'; return 1; }
```
Replace with:
```c
    case FT_BYTE:
        return snprintf(buf, bufsz, "%u", p[0]);
    case FT_TIMESTAMP: {
        int64_t v = ld_be_i64(p);
        if (!keep_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%lld", (long long)v);
    }
    case FT_NUMERIC: {
        int64_t v = ld_be_i64(p);
        if (v == 0) { buf[0] = '0'; buf[1] = '\0'; return 1; }
```
#### 4.2.4 ditb timestamp
Anchor (exactly one occurrence in the file):
```text
        if (skip_zero && secs == 0 && p[0]==0 && p[1]==0 && p[2]==0) return 0;
        int hh = secs / 3600, mm = (secs % 3600) / 60, ss = secs % 60;
        return snprintf(buf, bufsz, "%02d:%02d:%02d", hh, mm, ss);
    }
    case FT_UUID: {
```
Replace with:
```c
        if (skip_zero && secs == 0 && p[0]==0 && p[1]==0 && p[2]==0) return 0;
        int hh = secs / 3600, mm = (secs % 3600) / 60, ss = secs % 60;
        return snprintf(buf, bufsz, "%02d:%02d:%02d", hh, mm, ss);
    }
    case FT_TIMESTAMP: {
        if (plen < 8) return 0;
        uint64_t u = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                     ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                     ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                     ((uint64_t)p[6] << 8)  |  (uint64_t)p[7];
        int64_t v = (int64_t)(u ^ (1ULL << 63));
        if (skip_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%lld", (long long)v);
    }
    case FT_UUID: {
```
#### 4.2.5 WfcBatchCtx
Anchor (exactly one occurrence in the file):
```text
typedef struct {
    CriteriaNode     *tree;
    FieldSchema      *fs;
    const TypedField *agg_tf;
    int               agg_tf_idx;  /* schema index of agg_tf — bitmap NULL test */
    int               desc;     /* 0 = MIN (ASC walk), 1 = MAX (DESC) */
    double           *best;
    int              *found;
    pthread_mutex_t   mu;
    int predicate_error;
} WfcBatchCtx;
```
Replace with:
```c
typedef struct {
    CriteriaNode     *tree;
    FieldSchema      *fs;
    const TypedField *agg_tf;
    int               agg_tf_idx;  /* schema index of agg_tf — bitmap NULL test */
    int               desc;     /* 0 = MIN (ASC walk), 1 = MAX (DESC) */
    const AggNumSpec *spec;
    AggNum           *num;
    AggEnv           *env;
    int              *found;
    pthread_mutex_t   mu;
    int predicate_error;
    int err;
} WfcBatchCtx;
```
#### 4.2.6 WfcArg
Anchor (exactly one occurrence in the file):
```text
    QueryDeadline    *deadline;
    int               budget;     /* max walks per shard before bailout */

    /* Per-shard outputs */
    double  best;
    int     found;
    int     budget_exceeded;
    int     dl_counter;
    int predicate_error;
} WfcArg;
```
Replace with:
```c
    QueryDeadline    *deadline;
    int               budget;     /* max walks per shard before bailout */

    /* Per-shard outputs */
    const AggNumSpec *spec;
    AggNum  num;
    AggEnv  env;
    int     found;
    int     budget_exceeded;
    int     dl_counter;
    int predicate_error;
} WfcArg;
```
#### 4.2.7 wfc_batch_cb body
Anchor (exactly one occurrence in the file):
```text
    /* NULL skips min/max (§2.5) — the bitmap bit decides on nullmap
       records; legacy records keep the decode-time absence contract. */
    if (hdr2 & SREC_FLAG_NULLMAP) {
        const uint8_t *val = (const uint8_t *)value;
        const uint8_t *nb = typed_bitmap_of(val, hdr2);
        if (nb && typed_field_null(nb, val[0], bc->agg_tf_idx)) return 0;
    }
    size_t agg_avail;
    const uint8_t *agg_p = agg_field_ptr(value, vlen, bc->agg_tf, &agg_avail, hdr2, &(int){0});
    double v;
    if (typed_field_to_double(bc->agg_tf, agg_p, agg_avail, &v) != AGG_VALUE_PRESENT)
        return 0;
    pthread_mutex_lock(&bc->mu);
    if (!*bc->found ||
        (!bc->desc && v < *bc->best) ||
        ( bc->desc && v > *bc->best)) {
        *bc->best  = v;
        *bc->found = 1;
    }
    pthread_mutex_unlock(&bc->mu);
    return 0;
```
Replace with:
```c
    TypedRowView view;
    if (typed_row_open(bc->fs->ts, (const uint8_t *)value, vlen, hdr2, &view)) {
        pthread_mutex_lock(&bc->mu);
        bc->predicate_error = errno ? errno : EILSEQ;
        pthread_mutex_unlock(&bc->mu);
        return 1;
    }
    pthread_mutex_lock(&bc->mu);
    int r = agg_num_offer_row(bc->spec, &view, bc->num, bc->env);
    if (r < 0) bc->err = errno ? errno : EILSEQ;
    else if (r == 1) *bc->found = 1;
    pthread_mutex_unlock(&bc->mu);
    return r < 0;
```
#### 4.2.8 wfc_worker bc init
Anchor (exactly one occurrence in the file):
```text
    WfcBatchCtx bc = {
        .tree   = w->tree,
        .fs     = w->fs,
        .agg_tf = w->agg_tf,
        .agg_tf_idx = (int)(w->agg_tf - w->fs->ts->fields),
        .desc   = w->desc,
        .best   = &w->best,
        .found  = &w->found,
        .mu     = PTHREAD_MUTEX_INITIALIZER,
    };
```
Replace with:
```c
    agg_env_init(&w->env, NULL, 0);
    memset(&w->num, 0, sizeof w->num);
    WfcBatchCtx bc = {
        .tree   = w->tree,
        .fs     = w->fs,
        .agg_tf = w->agg_tf,
        .agg_tf_idx = (int)(w->agg_tf - w->fs->ts->fields),
        .desc   = w->desc,
        .spec   = w->spec,
        .num    = &w->num,
        .env    = &w->env,
        .found  = &w->found,
        .mu     = PTHREAD_MUTEX_INITIALIZER,
    };
```
#### 4.2.9 wfc presence test
Anchor (exactly one occurrence in the file):
```text
            double v;
            if (decode_index_key_to_double(w->agg_tf,
                                            (const uint8_t *)val,
                                            vlen, &v) != AGG_VALUE_PRESENT) continue;

            last_len = vlen > BT_MAX_VAL_LEN ? BT_MAX_VAL_LEN : vlen;
```
Replace with:
```c
            /* Presence of a non-NULL tier entry; the value comes from the
               fetched record in wfc_batch_cb. */
            if (vlen == 0 || (uint8_t)val[0] != 0x00) continue;

            last_len = vlen > BT_MAX_VAL_LEN ? BT_MAX_VAL_LEN : vlen;
```
#### 4.2.10 wfc_worker err propagate
Anchor (exactly one occurrence in the file):
```text
        if (bn > 0) {
            /* No bt lock is held here — blocking kf fetch is safe. */
            flush_wfc_batch(sdb, w, batch, bn, &bc);
            bn = 0;
        }
        if (w->predicate_error || w->found || w->budget_exceeded || exhausted || dl_stop) break;
```
Replace with:
```c
        if (bn > 0) {
            /* No bt lock is held here — blocking kf fetch is safe. */
            flush_wfc_batch(sdb, w, batch, bn, &bc);
            bn = 0;
        }
        if (bc.err) w->predicate_error = bc.err;
        if (w->predicate_error || w->found || w->budget_exceeded || exhausted || dl_stop) break;
```
#### 4.2.11 tfd fwd delete
Anchor (exactly one occurrence in the file):
```text
/* Forward declaration for wfc_batch_cb */
static AggValueStatus typed_field_to_double(const TypedField *f, const uint8_t *p,
                                            size_t available, double *out);

```
Replace with:
```c
```
#### 4.2.12 tfd def delete
Start anchor (exactly one occurrence):
```text
/* Extract a typed field as a double for numeric aggregate operations. */
static AggValueStatus typed_field_to_double(const TypedField *f, const uint8_t *p,
                                            size_t available, double *out) {
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
/* Text projection for text min/max. The per-type unset-marker doctrine is
   generation-independent (matching count(field)): an empty varchar, an
   all-zero calendar, and midnight are NOT present textual values, so they
   never become min/max offers — on legacy and nullmap records alike. The
   caller's bitmap NULL check runs before this function and skips NULLs. */
```
Keep the end anchor text; replace everything from the start anchor's first byte up to (not including) the end anchor with:
```text
(nothing — pure deletion)
```
#### 4.2.13 agg_span_text
Anchor (exactly one occurrence); insert immediately after it:
```text
    if (n < 0) return AGG_VALUE_MISSING;
    if ((size_t)n >= bufsz) n = (int)bufsz - 1;
    buf[n] = '\0';
    if (out_len) *out_len = (size_t)n;
    return AGG_VALUE_PRESENT;
}
```
Inserted text:
```c

/* Borrow full varchar bytes until agg_text_offer copies them into its arena.
   Only bounded calendar representations use the supplied scratch buffer. */
static int agg_span_text(const AggCtx *ctx, const TypedField *f,
                         const TypedRowSpan *s, char *buf, size_t cap,
                         const char **text, size_t *len) {
    if (f->type == FT_VARCHAR) {
        if (ctx->fs->ts->packed.nfields) {
            *text = (const char *)s->data;
            *len = s->len;
        } else {
            if (!s->complete) return 0;
            int n = varchar_eff_len(s->data, f->size);
            *text = (const char *)s->data + 2;
            *len = n > 0 ? (size_t)n : 0;
        }
        return *len > 0;
    }
    if (!s->complete) return 0;
    *text = buf;
    return typed_field_to_text(f, s->data, s->len, buf, cap, len, 0) ==
           AGG_VALUE_PRESENT;
}
```
#### 4.2.14 accums init delete
Anchor (exactly one occurrence in the file):
```text
    memset(b->accums, 0, (size_t)ctx->nspecs * sizeof(AggAccum));
    for (int i = 0; i < ctx->nspecs; i++) {
        b->accums[i].min = 1e308;
        b->accums[i].max = -1e308;
    }
    b->next = ctx->ht[h];
```
Replace with:
```c
    memset(b->accums, 0, (size_t)ctx->nspecs * sizeof(AggAccum));
    b->next = ctx->ht[h];
```
#### 4.2.15 key truncation E2BIG
Anchor (exactly one occurrence in the file):
```text
        uint32_t bl = is_null ? 0 : (uint32_t)strlen(vals[i]);
        if (kp + 5 + (int)bl >= (int)sizeof(key) - 1) break;
```
Replace with:
```c
        uint32_t bl = is_null ? 0 : (uint32_t)strlen(vals[i]);
        if (kp + 5 + (int)bl >= (int)sizeof(key) - 1) {
            ctx->agg_error = E2BIG;   /* never truncate a group key (D6) */
            return NULL;
        }
```

#### 4.3 — exact walker, cells, deletions, sort state, same-field, merge, VS, keyset emitter

#### 4.3.1 walker+cells
Anchor (exactly one occurrence); insert immediately after it:
```text
    a->text_present = 1;
}
```
Inserted text:
```c

/* One error response for every route (no success-shaped partial output). */
static int agg_report_error(int err) {
    switch (err) {
    case ENOBUFS:  OUT(QUERY_BUFFER_ERR); break;
    case ENOMEM:   OUT("{\"error\":\"out of memory\"}\n"); break;
    case ERANGE:   OUT("{\"error\":\"aggregate result out of range\"}\n"); break;
    case E2BIG:    OUT("{\"error\":\"group_by value exceeds aggregate key limit (511 bytes) or contains NUL\"}\n"); break;
    case ETIMEDOUT: OUT("{\"error\":\"query_timeout\"}\n"); break;
    case EIO:      OUT("{\"error\":\"aggregate storage access failed\"}\n"); break;
    default:       OUT("{\"error\":\"invalid typed record\"}\n"); break;
    }
    errno = err ? err : EILSEQ;
    return -1;
}

typedef struct {
    const char *db_root, *object, *field;
    int shard_id;
    const AggNumSpec *spec;
    const KeySet *ks;               /* optional membership filter */
    int first_only, desc;           /* min/max: stop at first PRESENT entry per shard */
    const char *lo; size_t lo_len; int lo_excl;
    const char *hi; size_t hi_len; int hi_excl;
    QueryDeadline *dl; int dl_counter;
    _Atomic size_t *budget; size_t limit;
    AggNum num; int64_t count; int err; int timed_out;   /* outputs */
} AggWalkArg;

static void *agg_walk_worker(void *raw) {
    AggWalkArg *a = (AggWalkArg *)raw;
    AggEnv env;
    agg_env_init(&env, a->budget, a->limit);
    char path[PATH_MAX];
    build_idx_path(path, sizeof(path), a->db_root, a->object, a->field, a->shard_id);
    /* Untouched index shards are created lazily; ENOENT is an empty shard.
       An existing shard whose iterator cannot open still fails closed. */
    struct stat st;
    if (stat(path, &st) < 0) {
        if (errno != ENOENT) a->err = EIO;
        agg_env_free(&env);
        return NULL;
    }
    BtRangeIter *it = btree_range_iter_open(
        path, a->lo ? a->lo : "", a->lo ? a->lo_len : 0, a->lo ? a->lo_excl : 0,
        a->hi ? a->hi : AGG_VALUE_TIER_CEIL, a->hi ? a->hi_len : 1,
        a->hi ? a->hi_excl : 1, a->desc);
    if (!it) {
        /* The shard exists: an open failure must not become an empty total. */
        LOG_ERROR(LOG_SUB_QUERY, "agg_walk_worker: btree_range_iter_open %s failed", path);
        a->err = EIO;
        agg_env_free(&env);
        return NULL;
    }
    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        if (query_deadline_tick(a->dl, &a->dl_counter)) { a->timed_out = 1; break; }
        if (a->ks && !keyset_contains(a->ks, hash16)) continue;
        int r = agg_num_offer_key(a->spec, (const uint8_t *)val, vlen, &a->num, &env);
        if (r < 0) { a->err = errno ? errno : EILSEQ; break; }
        if (r == 0) continue;
        a->count++;
        if (a->first_only) break;
    }
    btree_range_iter_close(it);
    agg_env_free(&env);
    return NULL;
}

/* Walk every idx shard of `proto->field` in parallel and merge into *out /
   *count. 0 ok, -1 error (errno), 1 deadline. */
static int agg_walk_field(const AggWalkArg *proto, int n_idx, AggNum *out,
                          int64_t *count) {
    AggWalkArg *args = calloc((size_t)n_idx, sizeof *args);
    if (!args) { errno = ENOMEM; return -1; }
    for (int s = 0; s < n_idx; s++) { args[s] = *proto; args[s].shard_id = s; }
    parallel_for_io(agg_walk_worker, args, n_idx, sizeof *args);
    AggEnv env;
    agg_env_init(&env, proto->budget, proto->limit);
    int rc = 0, err = 0, timed = 0;
    for (int s = 0; s < n_idx; s++) {
        if (args[s].timed_out) timed = 1;
        if (args[s].err && !err) err = args[s].err;
    }
    for (int s = 0; s < n_idx; s++) {
        if (!err && agg_num_merge(out, &args[s].num, proto->spec->cls, &env))
            err = errno ? errno : EILSEQ;
        *count += args[s].count;
        agg_num_release(&args[s].num, proto->spec->cls, &env);
    }
    agg_env_free(&env);
    free(args);
    if (err) { errno = err; rc = -1; }
    else if (timed) rc = 1;
    return rc;
}

/* Output accounting covers arrays, full decimal lexemes and escaped text.
   Integer/float cells reserve a conservative bounded allocation; decimals
   reserve dec_text_len exactly before allocating the potentially huge text. */
#ifdef TEST_BUILD
static _Thread_local int agg_fail_cells_alloc, agg_fail_escape_alloc;
void agg_test_fail_cells_alloc(int enabled) { agg_fail_cells_alloc = enabled; }
void agg_test_fail_escape_alloc(int n) { agg_fail_escape_alloc = n; }
#endif
static int agg_render_charge(_Atomic size_t *bytes, size_t limit, size_t n) {
    if (!bytes || !n) return 0;
    size_t old = atomic_load_explicit(bytes, memory_order_relaxed);
    do {
        if (n > limit || old > limit - n) { errno = ENOBUFS; return -1; }
    } while (!atomic_compare_exchange_weak_explicit(bytes, &old, old + n,
                 memory_order_relaxed, memory_order_relaxed));
    return 0;
}
static void agg_render_uncharge(_Atomic size_t *bytes, size_t n) {
    if (bytes && n) atomic_fetch_sub_explicit(bytes, n, memory_order_relaxed);
}
static void agg_cells_bind(AggCell *c, size_t n, _Atomic size_t *bytes, size_t limit) {
    for (size_t i = 0; i < n; i++) { c[i].budget = bytes; c[i].limit = limit; }
}
static AggCell *agg_cells_alloc(size_t n, _Atomic size_t *bytes, size_t limit) {
    size_t count = n ? n : 1;
    if (count > SIZE_MAX / sizeof(AggCell)) { errno = ENOBUFS; return NULL; }
    size_t need = count * sizeof(AggCell);
    if (agg_render_charge(bytes, limit, need)) return NULL;
#ifdef TEST_BUILD
    if (agg_fail_cells_alloc) {
        agg_render_uncharge(bytes, need); errno = ENOMEM; return NULL;
    }
#endif
    AggCell *c = calloc(count, sizeof *c);
    if (!c) { agg_render_uncharge(bytes, need); errno = ENOMEM; return NULL; }
    agg_cells_bind(c, count, bytes, limit);
    return c;
}
static int agg_cell_reserve(AggCell *c, size_t n) {
    if (agg_render_charge(c->budget, c->limit, n)) return -1;
    c->charged += n;
    return 0;
}
static void agg_cells_free(AggCell *c, int n) {
    if (!c) return;
    for (int i = 0; i < n; i++) {
        free(c[i].lex); free(c[i].escaped);
        agg_render_uncharge(c[i].budget, c[i].charged);
        c[i].lex = c[i].escaped = NULL; c[i].charged = 0;
    }
}
static void agg_cells_dispose(AggCell *c, size_t n) {
    if (!c) return;
    _Atomic size_t *bytes = c[0].budget;
    agg_cells_free(c, (int)n);
    agg_render_uncharge(bytes, (n ? n : 1) * sizeof(AggCell));
    free(c);
}
static int agg_cell_count(AggCell *c, int64_t n) {
    char tmp[32];
    int len = snprintf(tmp, sizeof tmp, "%lld", (long long)n);
    if (agg_cell_reserve(c, (size_t)len + 1)) return -1;
    c->lex = strdup(tmp);
    if (!c->lex) { errno = ENOMEM; return -1; }
    return 0;
}
static int agg_cell_text(AggCell *c, const char *text, size_t len, int present) {
    c->is_text = 1; c->is_null = !present;
    if (!present) return 0;
    if (agg_cell_reserve(c, len + 1)) return -1;
    c->lex = malloc(len + 1);
    if (!c->lex) { errno = ENOMEM; return -1; }
    memcpy(c->lex, text, len); c->lex[len] = 0;
    size_t need = 1;
    for (size_t i = 0; i < len; i++) {
        unsigned char ch = (unsigned char)text[i];
        size_t add = (ch == '"' || ch == '\\' || ch == '\b' || ch == '\f' ||
                      ch == '\n' || ch == '\r' || ch == '\t') ? 2 : ch < 0x20 ? 6 : 1;
        if (need > SIZE_MAX - add) { errno = ENOBUFS; return -1; }
        need += add;
    }
    if (agg_cell_reserve(c, need)) return -1;
#ifdef TEST_BUILD
    if (agg_fail_escape_alloc > 0 && --agg_fail_escape_alloc == 0) {
        errno = ENOMEM; return -1;
    }
#endif
    c->escaped = malloc(need);
    if (!c->escaped) { errno = ENOMEM; return -1; }
    int n = json_escape_into(c->escaped, need - 1, text, len);
    if (n < 0) { errno = EILSEQ; return -1; }
    c->escaped[n] = 0;
    return 0;
}
static int agg_cell_from_num(const AggNumSpec *sp, const AggNum *num,
                             enum AggFn fn, int64_t count, AggCell *c) {
    if (count <= 0) {
        if (fn == AGG_AVG) { c->is_null = 1; return 0; }
        if (agg_cell_reserve(c, 2)) return -1;
        c->lex = strdup("0");
        if (!c->lex) { errno = ENOMEM; return -1; }
        return 0;
    }
    AggValue v;
    if (agg_num_value(num, sp, fn, count, &v)) return -1;
    size_t need = v.cls == AGC_DEC ? dec_text_len(v.dv)
                                 : 128 + (v.scale > 0 ? (size_t)v.scale : 0);
    if (agg_cell_reserve(c, need)) { agg_value_release(&v); return -1; }
    c->lex = agg_value_render(&v);
    int e = errno; agg_value_release(&v);
    if (!c->lex) { errno = e ? e : ENOMEM; return -1; }
    return 0;
}
static void agg_cell_emit_json(const AggCell *c) {
    if (c->is_null) OUT("null");
    else if (c->is_text) OUT("\"%s\"", c->escaped);
    else OUT("%s", c->lex ? c->lex : "0");
}
static void agg_cell_emit_csv(const AggCell *c, char delim) {
    csv_emit_cell(c->is_null ? (c->is_text ? "" : "0") : (c->lex ? c->lex : ""), delim);
}
static void agg_emit_delim(char d) { char s[2] = { d, '\0' }; OUT("%s", s); }
/* Scalar (no group_by) response: JSON object, rows+total wrapper, or CSV
   header+row. All values are already rendered, so nothing can fail here. */
static void agg_emit_scalar_cells(const AggSpec *specs, int nspecs,
                                  const AggCell *cells, char csv_delim,
                                  int want_total) {
    if (csv_delim) {
        for (int i = 0; i < nspecs; i++) {
            if (i > 0) agg_emit_delim(csv_delim);
            csv_emit_cell(specs[i].alias, csv_delim);
        }
        OUT("\n");
        for (int i = 0; i < nspecs; i++) {
            if (i > 0) agg_emit_delim(csv_delim);
            agg_cell_emit_csv(&cells[i], csv_delim);
        }
        OUT("\n");
        return;
    }
    OUT(want_total ? "{\"rows\":{" : "{");
    for (int i = 0; i < nspecs; i++) {
        if (i > 0) OUT(",");
        OUT("\"%s\":", specs[i].alias);
        agg_cell_emit_json(&cells[i]);
    }
    OUT(want_total ? "},\"total\":1}\n" : "}\n");
}
```
#### 4.3.2 delete awc
Start anchor (exactly one occurrence):
```text
typedef struct {
    const char       *db_root;
    const char       *object;
    const char       *fld;
    int               shard_id;
    const TypedField *tf;
    KeySet           *crit_ks;
    const int        *sibs;       /* spec indices sharing this field */
    int               nsibs;
    QueryDeadline    *deadline;
    int               dl_counter;
    /* outputs (one slot per agg spec — only sibs[] entries are touched) */
    int64_t          *counts;     /* MAX_AGG_SPECS-wide */
    double           *sums;
    double           *mins;
    double           *maxs;
    int              *present;
} AwcShardArg;
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
        for (int k = 0; k < a->nsibs; k++) {
            int idx = a->sibs[k];
            a->counts[idx]++;
            a->sums[idx] += v;
            if (v < a->mins[idx]) a->mins[idx] = v;
            if (v > a->maxs[idx]) a->maxs[idx] = v;
            a->present[idx] = 1;
        }
    }
    btree_range_iter_close(it);
    return NULL;
}
```
Delete everything from the start anchor's first byte through the end anchor's last byte inclusive and insert:
```text
(nothing — pure deletion)
```
#### 4.3.3 sort rewrite
Anchor (exactly one occurrence in the file):
```text
/* Sort context for qsort */
static AggSpec *g_sort_specs;
static int g_sort_nspecs;
static char g_sort_field[256];
static int g_sort_desc;
static int g_sort_ngroups;
static char (*g_sort_group_fields)[256];
static const AggCtx *g_sort_ctx;
```
Replace with:
```c
/* Exact sort keys are materialised once per bucket (so the comparator never
   allocates) and the permutation is sorted through pointers (AggValue may
   borrow its own storage and must not be moved by qsort). */
typedef struct {
    AggBucket *b;
    AggResult  r;        /* aggregate alias, or numeric group label */
    int        use_label;/* text group field: compare group_vals with strcmp */
} AggSortRow;
static int g_sort_desc;
```
#### 4.3.4 agg_free
Anchor (exactly one occurrence in the file):
```text
static void agg_free(AggCtx *ctx) {
    /* Arena owns every per-bucket allocation — one bulk free vs
       O(buckets × 5) malloc-companion frees in the pre-arena path. */
    agg_arena_free(&ctx->arena);
```
Replace with:
```c
static void agg_free(AggCtx *ctx) {
    /* Exact decimal states are heap-backed (decnum buffers) — release them
       before the arena disappears; then the per-query env scratch. */
    agg_ctx_release_nums(ctx);
    agg_env_free(&ctx->env);
    /* Arena owns every per-bucket allocation — one bulk free vs
       O(buckets × 5) malloc-companion frees in the pre-arena path. */
    agg_arena_free(&ctx->arena);
```
#### 4.3.5 delete fmt_double
Anchor (exactly one occurrence in the file):
```text
/* Format a double, removing trailing zeros */
static void fmt_double(char *buf, size_t sz, double v) {
    snprintf(buf, sz, "%.6f", v);
    /* Trim trailing zeros after decimal point */
    char *dot = strchr(buf, '.');
    if (dot) {
        char *end = buf + strlen(buf) - 1;
        while (end > dot && *end == '0') *end-- = '\0';
        if (end == dot) *end = '\0';
    }
}
```
Replace with:
```c
```
#### 4.3.6 C6a same-field signature
Anchor (exactly one occurrence in the file):
```text
static int agg_minmax_same_field_btree(
        const char *db_root, const char *object, const char *field,
        const char *alias, const TypedField *agg_tf, int splits,
        int is_max, const SearchCriterion *crit,
        const char *format, const char *delimiter, int want_total) {
    const TypedSchema *literal_schema=load_typed_schema(db_root,object);
    if(!search_criterion_indexable(load_typed_schema(db_root,object),crit))return 0;
```
Replace with:
```c
static int agg_minmax_same_field_btree(
        const char *db_root, const char *object, const char *field,
        const char *alias, const TypedField *agg_tf, const TypedSchema *ts,
        int splits,
        int is_max, const SearchCriterion *crit,
        const char *format, const char *delimiter, int want_total) {
    const TypedSchema *literal_schema = ts;
    if (!search_criterion_indexable(ts, crit)) return 0;
```
#### 4.3.7 C6b same-field tail
Anchor (exactly one occurrence in the file):
```text
    int n_idx = index_splits_for(splits);
    int desc = is_max ? 1 : 0;
    double best = 0.0;
    int have = 0;
    for (int s = 0; s < n_idx; s++) {
        char idx_path[PATH_MAX];
        build_idx_path(idx_path, sizeof(idx_path), db_root, object, field, s);
        BtRangeIter *it = btree_range_iter_open(
            idx_path, min_v, min_l, min_x,
            max_v, max_l, max_x, desc);
        if (!it) continue;
        const char *val; size_t vlen; const uint8_t *hash16;
        while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
            double v;
            if (decode_index_key_to_double(agg_tf,
                                            (const uint8_t *)val, vlen, &v) == AGG_VALUE_PRESENT) {
                if (!have)        { best = v; have = 1; }
                else if (desc)    { if (v > best) best = v; }
                else              { if (v < best) best = v; }
                break;
            }
        }
        btree_range_iter_close(it);
    }

    char vbuf[64];
    fmt_double(vbuf, sizeof(vbuf), have ? best : 0.0);
    char csv_delim_local = (format && strcmp(format, "csv") == 0)
                             ? parse_csv_delim(delimiter) : 0;
    if (csv_delim_local) {
        csv_emit_cell(alias, csv_delim_local);
        OUT("\n");
        csv_emit_cell(vbuf, csv_delim_local);
        OUT("\n");
    } else if (want_total) {
        OUT("{\"rows\":{\"%s\":%s},\"total\":1}\n", alias, vbuf);
    } else {
        OUT("{\"%s\":%s}\n", alias, vbuf);
    }
    return 1;
}
```
Replace with:
```c
    /* Checked literals are untiered payloads; iterator bounds include tier 0.
       Leave the default tier-1 ceiling untouched. */
    if (min_v == (const char *)buf1 || max_v == (const char *)buf1) {
        memmove(buf1 + 1, buf1, len1); buf1[0] = 0x00;
        if (min_v == (const char *)buf1) min_l++;
        if (max_v == (const char *)buf1) max_l++;
    }
    if (max_v == (const char *)buf2) {
        memmove(buf2 + 1, buf2, len2); buf2[0] = 0x00; max_l++;
    }
    if (crit->op == OP_EQUAL || crit->op == OP_LESS_EQ) max_x = 0;
    AggNumSpec ns;
    if (!agg_num_spec_for(ts, (size_t)(agg_tf - ts->fields), &ns) ||
        !agg_index_exact(&ns))
        return 0;                     /* record-fed route decides (D1) */
    QueryDeadline wdl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    _Atomic size_t wbytes = 0;
    atomic_init(&wbytes, 0);
    AggWalkArg proto = { .db_root = db_root, .object = object, .field = field,
        .spec = &ns, .dl = &wdl, .budget = &wbytes,
        .limit = g_query_buffer_max_bytes, .first_only = 1, .desc = is_max,
        .lo = min_v, .lo_len = min_l, .lo_excl = min_x,
        .hi = max_v, .hi_len = max_l, .hi_excl = max_x };
    AggNum num; memset(&num, 0, sizeof num);
    int64_t cnt = 0;
    int rc = agg_walk_field(&proto, index_splits_for(splits), &num, &cnt);
    AggCell cell; memset(&cell, 0, sizeof cell);
    agg_cells_bind(&cell, 1, &wbytes, g_query_buffer_max_bytes);
    AggSpec one = { .fn = is_max ? AGG_MAX : AGG_MIN };
    snprintf(one.alias, sizeof one.alias, "%s", alias);
    int err = rc < 0 ? (errno ? errno : EILSEQ) : (rc > 0 || wdl.timed_out) ? ETIMEDOUT : 0;
    if (!err && agg_cell_from_num(&ns, &num, one.fn, cnt, &cell)) err = errno;
    AggEnv env; agg_env_init(&env, &wbytes, g_query_buffer_max_bytes);
    agg_num_release(&num, ns.cls, &env);
    agg_env_free(&env);
    if (err) { agg_cells_free(&cell, 1); return agg_report_error(err); }
    char csv_delim_local = (format && strcmp(format, "csv") == 0)
                             ? parse_csv_delim(delimiter) : 0;
    agg_emit_scalar_cells(&one, 1, &cell, csv_delim_local, want_total);
    agg_cells_free(&cell, 1);
    return 1;
}
```
#### 4.3.8 C7a run_plan sdb miss
Anchor (exactly one occurrence in the file):
```text
        SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
        if (sdb) parallel_agg_scan_shards_o_direct(ctx, sdb);
    }
```
Replace with:
```c
        SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
        if (sdb) parallel_agg_scan_shards_o_direct(ctx, sdb);
        else ctx->agg_error = EIO;   /* no scan possible: error, not empty */
    }
```
#### 4.3.9 C7b run_plan error
Anchor (exactly one occurrence in the file):
```text
    if (ctx->budget_exceeded || ctx->predicate_error) return -1;
    return 0;
```
Replace with:
```c
    if (ctx->budget_exceeded || ctx->predicate_error || ctx->agg_error) return -1;
    return 0;
```
#### 4.3.10 C8a secmap struct
Anchor (exactly one occurrence in the file):
```text
    HashStrMap       local_map;      /* per-worker output */
    int              cap_hint;       /* hsm_init capacity hint */
    int              arena_hint;     /* hsm_init arena hint */
    int              aborted;
    int              dl_counter;
    QueryDeadline   *dl;
} SecMapBuildWorker;
```
Replace with:
```c
    HashStrMap       local_map;      /* per-worker output */
    int              cap_hint;       /* hsm_init capacity hint */
    int              arena_hint;     /* hsm_init arena hint */
    int              aborted;
    int              dl_counter;
    QueryDeadline   *dl;
    const AggCtx    *actx;           /* owning ctx for exact labels (D2) */
    int              gi;             /* group component index + 1 */
    int              err;            /* first label failure (errno) */
} SecMapBuildWorker;
```
#### 4.3.11 C8b secmap label
Anchor (exactly one occurrence in the file):
```text
        char centry[5 + 512];
        int is_null = ((uint8_t)val[0] != 0x00);
        uint32_t bl = 0;
        if (!is_null) {
            char dbuf[512];
            int dlen = decode_idx_to_buf(w->gtf_s, (const uint8_t *)val + 1,
                                          vlen - 1, dbuf, sizeof(dbuf), 0);
            if (dlen < 0) continue;
            bl = (uint32_t)dlen;
            memcpy(centry + 5, dbuf, (size_t)dlen);
        }
```
Replace with:
```c
        char centry[5 + 512];
        int is_null = ((uint8_t)val[0] != 0x00);
        uint32_t bl = 0;
        if (!is_null) {
            char dbuf[512];
            int dlen = agg_index_label(w->actx, w->gi, (const uint8_t *)val + 1,
                                        vlen - 1, dbuf, sizeof(dbuf));
            if (dlen < 0) { w->err = errno ? errno : EILSEQ; w->aborted = 1; break; }
            bl = (uint32_t)dlen;
            memcpy(centry + 5, dbuf, (size_t)dlen);
        }
```
#### 4.3.12 igb pass1 label
Anchor (exactly one occurrence in the file):
```text
                gbufs[0][0] = '\0';
                int n = decode_idx_to_buf(w->gtf, prim,
                                           prim_len, gbufs[0], sizeof(gbufs[0]), 0);
                if (n < 0) continue;
                gvals[0] = gbufs[0];
```
Replace with:
```c
                gbufs[0][0] = '\0';
                int n = agg_index_label(&w->local, 0, prim, prim_len,
                                        gbufs[0], sizeof(gbufs[0]));
                if (n < 0) { w->local.agg_error = errno ? errno : EILSEQ; w->aborted = 1; break; }
                gvals[0] = gbufs[0];
```
#### 4.3.13 part merge fold
Anchor (exactly one occurrence in the file):
```text
            for (int k = 0; k < p->local.nspecs; k++) {
                AggAccum *sa = &src->accums[k], *da = &mb->accums[k];
                da->sum += sa->sum;
                da->count += sa->count;
                if (sa->count > 0) {
                    if (sa->min < da->min) da->min = sa->min;
                    if (sa->max > da->max) da->max = sa->max;
                }
                if (sa->text_present) {
                    if (sa->text_min)
                        agg_text_offer(&p->local, da, AGG_MIN,
                                       sa->text_min, sa->text_min_len);
                    if (sa->text_max)
                        agg_text_offer(&p->local, da, AGG_MAX,
                                       sa->text_max, sa->text_max_len);
                }
            }
```
Replace with:
```c
            for (int k = 0; k < p->local.nspecs; k++) {
                AggAccum *sa = &src->accums[k], *da = &mb->accums[k];
                if (p->local.spec_is_num[k] &&
                    agg_num_merge(&da->num, &sa->num, p->local.spec_nums[k].cls,
                                  &p->local.env)) {
                    p->local.agg_error = errno ? errno : EILSEQ;
                    p->aborted = 1;
                    return NULL;
                }
                da->count += sa->count;
                if (p->local.spec_is_num[k])
                    agg_num_release(&sa->num, p->local.spec_nums[k].cls,
                                    &p->local.env);
                if (sa->text_present) {
                    if (sa->text_min)
                        agg_text_offer(&p->local, da, AGG_MIN,
                                       sa->text_min, sa->text_min_len);
                    if (sa->text_max)
                        agg_text_offer(&p->local, da, AGG_MAX,
                                       sa->text_max, sa->text_max_len);
                }
            }
```
#### 4.3.14 delete single
Start anchor (exactly one occurrence):
```text
/* Per-shard accumulator + worker for the single-spec SUM/AVG/MIN/MAX
   fast path inside cmd_aggregate. Each shard's btree leaf walk is
   independent, so parallel_for_io fans them out — at 25M records the
   sequential walk took 9-15 s cold (idx shards × ~30 MB sequential
   reads serialised); parallel cuts that close to single-shard cost. */
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
/* Per-emit staging slot for the varchar-streaming fast path. Holds
```
Keep the end anchor text; replace everything from the start anchor's first byte up to (not including) the end anchor with:
```text
(nothing — pure deletion)
```
#### 4.3.15 C12a VSStaged
Anchor (exactly one occurrence in the file):
```text
typedef struct {
    char    key[BT_MAX_VAL_LEN + 1];
    size_t  klen;
    int64_t total_count;                  /* AGG_COUNT total for this key */
    double  spec_sum[MAX_AGG_SPECS];      /* AGG_SUM / AGG_AVG running sum */
    double  spec_min[MAX_AGG_SPECS];      /* AGG_MIN running min */
    double  spec_max[MAX_AGG_SPECS];      /* AGG_MAX running max */
    int64_t spec_count[MAX_AGG_SPECS];    /* live-value count per spec */
} VSStaged;
```
Replace with:
```c
typedef struct {
    char    key[BT_MAX_VAL_LEN + 1];
    size_t  klen;
    int64_t total_count;
} VSStaged;
```
#### 4.3.16 C12b VSLookupCtx
Anchor (exactly one occurrence in the file):
```text
typedef struct {
    VSStaged              *staged;         /* base of staging array */
    const AggSpec         *specs;
    const TypedField     **spec_tfs;
    const int             *spec_tidx;      /* schema index per spec (NULL-bit test) */
    int                    nspecs;
    const VSPendPair      *pairs;          /* sorted by hash16 */
    size_t                 n;
    /* Partition workers deliver concurrently; staged accums are shared
     * per slot set, so attribution writes serialize through this mu. */
    pthread_mutex_t        mu;
} VSLookupCtx;
```
Replace with:
```c
typedef struct {
    VSStaged              *staged;         /* base of staging array */
    const AggSpec         *specs;
    const TypedField     **spec_tfs;
    const int             *spec_tidx;      /* schema index per spec (NULL-bit test) */
    int                    nspecs;
    const TypedSchema     *ts;             /* for typed_row_open */
    AggNum                *nums;           /* [slot * nspecs + i] exact states */
    int64_t               *counts;         /* contributing values per slot/spec */
    const AggNumSpec      *spec_nums;      /* per-spec exact classification */
    AggEnv                *env;            /* owning ctx env (QUERY_BUFFER accounting) */
    int                    err;            /* first offer failure (errno) */
    const VSPendPair      *pairs;          /* sorted by hash16 */
    size_t                 n;
    /* Partition workers deliver concurrently; staged accums are shared
     * per slot set, so attribution writes serialize through this mu. */
    pthread_mutex_t        mu;
} VSLookupCtx;
```
#### 4.3.17 C12c vs_lookup_cb body
Anchor (exactly one occurrence in the file):
```text
    pthread_mutex_lock(&c->mu);
    VSStaged *cur = &c->staged[c->pairs[p].slot];
    const uint8_t *rec = (const uint8_t *)value;
    /* Bitmap view for the NULL-bit test — resolved once per record. */
    const uint8_t *vbase = NULL, *vbm = NULL;
    int vbm_bytes = 0; size_t vregion = 0;
    int view_ok = typed_record_view(rec, vlen, hdr2, &vbase, &vbm,
                                    &vbm_bytes, &vregion) == 0;
    (void)vbase; (void)vregion;
    for (int i = 0; i < c->nspecs; i++) {
        enum AggFn fn = c->specs[i].fn;
        if (fn == AGG_COUNT) continue;
        const TypedField *tf = c->spec_tfs[i];
        if (!tf) continue;
        /* NULL spec value skips the accumulator (§2.5: sum/avg/min/max
           skip NULLs; avg's denominator counts non-NULL only). */
        if (c->spec_tidx && c->spec_tidx[i] >= 0 && view_ok &&
            typed_field_null(vbm, vbm_bytes, c->spec_tidx[i])) continue;
        size_t avail;
        const uint8_t *fp = agg_field_ptr(rec, vlen, tf, &avail, hdr2, &(int){0});
        double v;
        if (typed_field_to_double(tf, fp, avail, &v) != AGG_VALUE_PRESENT) continue;
        switch (fn) {
        case AGG_SUM:
        case AGG_AVG:
            cur->spec_sum[i] += v;
            cur->spec_count[i]++;
            break;
        case AGG_MIN:
            if (cur->spec_count[i] == 0 || v < cur->spec_min[i])
                cur->spec_min[i] = v;
            cur->spec_count[i]++;
            break;
        case AGG_MAX:
            if (cur->spec_count[i] == 0 || v > cur->spec_max[i])
                cur->spec_max[i] = v;
            cur->spec_count[i]++;
            break;
        default: break;
        }
    }
```
Replace with:
```c
    pthread_mutex_lock(&c->mu);
    const uint8_t *rec = (const uint8_t *)value;
    TypedRowView view;
    int vok = typed_row_open(c->ts, rec, vlen, hdr2, &view) == 0;
    for (int i = 0; i < c->nspecs; i++) {
        enum AggFn fn = c->specs[i].fn;
        if (fn == AGG_COUNT || !c->spec_tfs[i] || !vok) continue;
        /* NULL spec value skips the accumulator (§2.5: sum/avg/min/max
           skip NULLs; avg's denominator counts non-NULL only). */
        if (c->spec_tidx && c->spec_tidx[i] >= 0) {
            TypedRowSpan sp;
            if (typed_row_span(&view, (size_t)c->spec_tidx[i], &sp) == 0 &&
                sp.is_null) continue;
        }
        size_t k = (size_t)c->pairs[p].slot * (size_t)c->nspecs + (size_t)i;
        int r = agg_num_offer_row(&c->spec_nums[i], &view, &c->nums[k], c->env);
        if (r < 0) { c->err = errno ? errno : EILSEQ; break; }
        if (r == 1) c->counts[k]++;
    }
```
#### 4.3.18 C13a keyset sig
Anchor (exactly one occurrence in the file):
```text
static void emit_min_max_via_keyset(const char *db_root, const char *object,
                                    const Schema *sch,
                                    const AggSpec *spec,
                                    const TypedField *agg_tf,
                                    KeySet *ks,
                                    const char *format,
                                    const char *delimiter,
                                    int want_total) {
```
Replace with:
```c
static int emit_min_max_via_keyset(const char *db_root, const char *object,
                                    const Schema *sch,
                                    const AggSpec *spec,
                                    const TypedField *agg_tf,
                                    const TypedSchema *ts,
                                    KeySet *ks,
                                    const char *format,
                                    const char *delimiter,
                                    int want_total) {
```
#### 4.3.19 C13b keyset body
Anchor (exactly one occurrence in the file):
```text
    int n_idx = index_splits_for(sch->splits);
    int desc  = (spec->fn == AGG_MAX) ? 1 : 0;
    double best = 0.0;
    int have = 0;
    for (int s = 0; s < n_idx; s++) {
        char idx_path[PATH_MAX];
        build_idx_path(idx_path, sizeof(idx_path), db_root,
                       object, spec->field, s);
        BtRangeIter *it = btree_range_iter_open(
            idx_path, "", 0, 0,
            AGG_VALUE_TIER_CEIL, 1, 1, desc);
        if (!it) continue;
        const char *val; size_t vlen; const uint8_t *hash16;
        while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
            if (!keyset_contains(ks, hash16)) continue;
            double v;
            if (decode_index_key_to_double(agg_tf,
                                           (const uint8_t *)val,
                                           vlen, &v) == AGG_VALUE_PRESENT) {
                if (!have) { best = v; have = 1; }
                else if (desc) { if (v > best) best = v; }
                else           { if (v < best) best = v; }
                break;
                    /* per-shard min/max found */
            }
        }
        btree_range_iter_close(it);
    }
    keyset_free(ks);

    char vbuf[64];
    fmt_double(vbuf, sizeof(vbuf), have ? best : 0.0);
    char csv_delim_local = (format && strcmp(format, "csv") == 0)
                             ? parse_csv_delim(delimiter) : 0;
    if (csv_delim_local) {
        csv_emit_cell(spec->alias, csv_delim_local);
        OUT("\n");
        csv_emit_cell(vbuf, csv_delim_local);
        OUT("\n");
    } else if (want_total) {
        OUT("{\"rows\":{\"%s\":%s},\"total\":1}\n", spec->alias, vbuf);
    } else {
        OUT("{\"%s\":%s}\n", spec->alias, vbuf);
    }
}
```
Replace with:
```c
    AggNumSpec ns;
    if (!agg_num_spec_for(ts, (size_t)(agg_tf - ts->fields), &ns) ||
        !agg_index_exact(&ns)) {
        keyset_free(ks);
        return 0;                     /* record-fed route decides (D1) */
    }
    QueryDeadline wdl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    _Atomic size_t wbytes = 0;
    atomic_init(&wbytes, 0);
    AggWalkArg proto = { .db_root = db_root, .object = object,
        .field = spec->field, .spec = &ns, .ks = ks, .dl = &wdl,
        .budget = &wbytes, .limit = g_query_buffer_max_bytes,
        .first_only = 1, .desc = spec->fn == AGG_MAX };
    AggNum num; memset(&num, 0, sizeof num);
    int64_t cnt = 0;
    int rc = agg_walk_field(&proto, index_splits_for(sch->splits), &num, &cnt);
    keyset_free(ks);
    AggCell cell; memset(&cell, 0, sizeof cell);
    agg_cells_bind(&cell, 1, &wbytes, g_query_buffer_max_bytes);
    int err = rc < 0 ? (errno ? errno : EILSEQ) : (rc > 0 || wdl.timed_out) ? ETIMEDOUT : 0;
    if (!err && agg_cell_from_num(&ns, &num, spec->fn, cnt, &cell)) err = errno;
    AggEnv env; agg_env_init(&env, &wbytes, g_query_buffer_max_bytes);
    agg_num_release(&num, ns.cls, &env);
    agg_env_free(&env);
    if (err) { agg_cells_free(&cell, 1); return agg_report_error(err); }
    char csv_delim_local = (format && strcmp(format, "csv") == 0)
                             ? parse_csv_delim(delimiter) : 0;
    agg_emit_scalar_cells(spec, 1, &cell, csv_delim_local, want_total);
    agg_cells_free(&cell, 1);
    return 1;
}
```

#### 4.3.20 schema-aware candidate dispatch
Anchor (exactly one occurrence in the file):
```text
            if(btree_dispatch_checked(db_root, object, pc->field, sch->splits,
                           pc,
                           resolve_idx_field(ctx->fs->ts, pc->field),
                           collect_hash_cb, &cc)<0){ctx->predicate_error=errno?errno:EILSEQ;}
```
Replace with:
```c
            if (btree_dispatch_schema_checked(ctx->fs->ts, db_root, object,
                    pc->field, sch->splits, pc,
                    resolve_idx_field(ctx->fs->ts, pc->field),
                    collect_hash_cb, &cc) < 0)
                ctx->predicate_error = errno ? errno : EILSEQ;
```

#### 4.4 — scan callback, clone/merge ownership, silent-drop closures, teardown

#### 4.4.1 agg_scan_cb
Start anchor (exactly one occurrence):
```text
static int agg_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *raw_ctx) {
    AggCtx *ctx = (AggCtx *)raw_ctx;
    if (ctx->budget_exceeded) return 1;
    if (query_deadline_tick(ctx->deadline, &ctx->dl_counter)) return 1;
    const uint8_t *raw = block + hdr->key_len;
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
static int parse_agg_specs(const char *json, AggSpec **out) {
```
Delete everything from the start anchor's first byte through the end anchor's last byte inclusive and insert:
```c
static int agg_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *raw_ctx) {
    AggCtx *ctx = (AggCtx *)raw_ctx;
    if (ctx->budget_exceeded || ctx->agg_error) return 1;
    if (query_deadline_tick(ctx->deadline, &ctx->dl_counter)) return 1;
    const uint8_t *raw = block + hdr->key_len;
    const uint8_t hdr2 = (uint8_t)(hdr->flag >> 8);

    int matched = 0;
    if (criteria_match_tree_checked(raw, hdr->value_len, ctx->tree, ctx->fs, hdr2, &matched)) {
        ctx->predicate_error = errno ? errno : EILSEQ;
        return 1;
    }
    if (!matched) return 0;

    AggRow row;
    if (agg_row_open(ctx, raw, hdr->value_len, hdr2, &row)) return 1;

    /* Group components into stack buffers (no malloc). gbuf[i][0] is NUL up
       front so an empty/absent component never leaks a prior record's bytes. */
    char gbuf[MAX_FIELDS][512];
    char *gvals[MAX_FIELDS];
    uint8_t gnulls[MAX_FIELDS] = {0};
    for (int i = 0; i < ctx->ngroups; i++) {
        gbuf[i][0] = '\0';
        if (ctx->group_tfs[i]) {
            if (agg_row_label(ctx, &row, i, gbuf[i], sizeof gbuf[i], &gnulls[i]) < 0)
                return 1;
        } else if (ctx->fs->ts && ctx->fs->ts->packed.nfields) {
            size_t tl = 0;
            errno = 0;
            char *s = typed_packed_named_text(ctx->fs->ts, raw, hdr->value_len,
                                              hdr2, ctx->group_fields[i],
                                              sizeof gbuf[i], &tl);
            if (!s && errno) { ctx->agg_error = errno == ENOBUFS ? E2BIG : errno; return 1; }
            if (s) { memcpy(gbuf[i], s, tl); gbuf[i][tl] = '\0'; free(s); }
        } else {
            char *s = decode_field((const char *)raw, hdr->value_len,
                                   ctx->group_fields[i], ctx->fs, hdr2);
            if (s) {
                size_t sl = strlen(s);
                if (sl >= sizeof gbuf[i]) { free(s); ctx->agg_error = E2BIG; return 1; }
                memcpy(gbuf[i], s, sl); gbuf[i][sl] = '\0';
                free(s);
            }
        }
        gvals[i] = gbuf[i];
    }

    uint8_t raw_key[AGG_INT_KEY_CAP];
    int raw_key_len = 0;
    if (ctx->use_int_keys) raw_key_len = agg_row_raw_key(ctx, &row, gnulls, raw_key);

    AggBucket *bkt;
    if (ctx->ngroups > 0) {
        bkt = agg_find_or_create(ctx, gvals, ctx->ngroups, raw_key, raw_key_len, gnulls);
    } else {
        char *empty = "";
        bkt = agg_find_or_create(ctx, &empty, 1, NULL, 0, NULL);
    }
    if (!bkt) return 1;

    for (int i = 0; i < ctx->nspecs; i++) {
        AggAccum *a = &bkt->accums[i];
        const TypedField *stf = ctx->spec_tfs[i];
        if (ctx->specs[i].fn == AGG_COUNT) {
            /* count(*) counts every record; count(field) skips NULL and, for
               the presence-doctrine types, the encoded unset marker. */
            if (ctx->specs[i].field[0] && stf) {
                TypedRowSpan s;
                int have = agg_row_span(&row, (size_t)(stf - ctx->fs->ts->fields), &s);
                if (have && s.is_null) continue;
                if (agg_count_inspects_presence(stf->type) &&
                    (!have || agg_row_unset(ctx, stf, &s)))
                    continue;
            }
            a->count++;
            continue;
        }
        if (!stf) continue;
        size_t sidx = (size_t)(stf - ctx->fs->ts->fields);
        if ((ctx->specs[i].fn == AGG_MIN || ctx->specs[i].fn == AGG_MAX) &&
            agg_type_is_text(stf->type)) {
            TypedRowSpan s;
            char scratch[64]; const char *text = NULL; size_t text_len = 0;
            if (agg_row_span(&row, sidx, &s) && !s.is_null &&
                agg_span_text(ctx, stf, &s, scratch, sizeof scratch, &text, &text_len))
                agg_text_offer(ctx, a, ctx->specs[i].fn, text, text_len);
            continue;
        }
        if (!ctx->spec_is_num[i] || !row.ok) continue;
        int r = agg_num_offer_row(&ctx->spec_nums[i], &row.v, &a->num, &ctx->env);
        if (r < 0) { ctx->agg_error = errno ? errno : EILSEQ; return 1; }
        if (r == 1) a->count++;
    }
    return 0;
}

static int parse_agg_specs(const char *json, AggSpec **out) {
```
#### 4.4.2 clone_shared
Anchor (exactly one occurrence in the file):
```text
    dst->deadline = src->deadline;
    dst->use_int_keys = src->use_int_keys;
    /* dl_counter stays 0 (per-worker local) */
```
Replace with:
```c
    dst->deadline = src->deadline;
    dst->use_int_keys = src->use_int_keys;
    memcpy(dst->spec_nums, src->spec_nums, sizeof(src->spec_nums));
    memcpy(dst->spec_is_num, src->spec_is_num, sizeof(src->spec_is_num));
    memcpy(dst->group_nums, src->group_nums, sizeof(src->group_nums));
    memcpy(dst->group_is_num, src->group_is_num, sizeof(src->group_is_num));
    memcpy(dst->group_idx, src->group_idx, sizeof(src->group_idx));
    agg_env_init(&dst->env, src->shared_buffer_bytes, src->env.limit);
    /* dl_counter stays 0 (per-worker local) */
```
#### 4.4.3 merge
Anchor (exactly one occurrence in the file):
```text
/* Merge src's hash table into dst. Buckets in src live in src->arena
   so we don't free them individually — agg_find_or_create copies the
   group_vals strings into dst->arena, then the caller (or
   agg_ctx_free_local on src) frees src->arena en masse. */
static void agg_ctx_merge(AggCtx *dst, AggCtx *src) {
    if(src->predicate_error)dst->predicate_error=src->predicate_error;
```
Replace with:
```c
/* Release every bucket's exact states (heap-backed for packed numeric).
   Idempotent: released states are the zero state. Callers must not walk a
   chain whose buckets were spliced elsewhere (see the partition splice). */
static void agg_ctx_release_nums(AggCtx *ctx) {
    if (!ctx->ht) return;
    for (size_t i = 0; i < ctx->ht_cap; i++)
        for (AggBucket *b = ctx->ht[i]; b; b = b->next)
            for (int k = 0; k < ctx->nspecs; k++)
                if (ctx->spec_is_num[k])
                    agg_num_release(&b->accums[k].num, ctx->spec_nums[k].cls,
                                    &ctx->env);
}

/* Merge src's hash table into dst. Buckets in src live in src->arena
   so we don't free them individually — agg_find_or_create copies the
   group_vals strings into dst->arena, then the caller (or
   agg_ctx_free_local on src) frees src->arena en masse. */
static void agg_ctx_merge(AggCtx *dst, AggCtx *src) {
    if(src->predicate_error)dst->predicate_error=src->predicate_error;
    if (src->agg_error && !dst->agg_error) dst->agg_error = src->agg_error;
```
#### 4.4.4 merge fold
Anchor (exactly one occurrence in the file):
```text
            for (int k = 0; k < src->nspecs; k++) {
                AggAccum *sa = &b->accums[k], *da = &dbkt->accums[k];
                da->sum += sa->sum;
                da->count += sa->count;
                if (sa->count > 0) {
                    if (sa->min < da->min) da->min = sa->min;
                    if (sa->max > da->max) da->max = sa->max;
                }
                if (sa->text_present) {
                    if (sa->text_min)
                        agg_text_offer(dst, da, AGG_MIN, sa->text_min, sa->text_min_len);
                    if (sa->text_max)
                        agg_text_offer(dst, da, AGG_MAX, sa->text_max, sa->text_max_len);
                }
            }
        }
        src->ht[i] = NULL;
```
Replace with:
```c
            for (int k = 0; k < src->nspecs; k++) {
                AggAccum *sa = &b->accums[k], *da = &dbkt->accums[k];
                if (src->spec_is_num[k] &&
                    agg_num_merge(&da->num, &sa->num, src->spec_nums[k].cls, &dst->env)) {
                    if (!dst->agg_error) dst->agg_error = errno ? errno : EILSEQ;
                }
                da->count += sa->count;
                if (src->spec_is_num[k])
                    agg_num_release(&sa->num, src->spec_nums[k].cls, &src->env);
                if (sa->text_present) {
                    if (sa->text_min)
                        agg_text_offer(dst, da, AGG_MIN, sa->text_min, sa->text_min_len);
                    if (sa->text_max)
                        agg_text_offer(dst, da, AGG_MAX, sa->text_max, sa->text_max_len);
                }
            }
        }
        src->ht[i] = NULL;
```
#### 4.4.5 D5a o_direct args
Anchor (exactly one occurrence in the file):
```text
    AggKfShardArg *args = calloc((size_t)sdb->num_shards, sizeof(*args));
    if (!args) return;
```
Replace with:
```c
    AggKfShardArg *args = calloc((size_t)sdb->num_shards, sizeof(*args));
    if (!args) { main_ctx->agg_error = ENOMEM; return; }
```
#### 4.4.6 D5b batch_cb ENOMEM
Anchor (exactly one occurrence in the file):
```text
    uint8_t stk[2048];
    uint8_t *block = (klen + vlen + 1 < sizeof(stk))
        ? stk : malloc(klen + vlen);
    if (block) {
        memcpy(block, key, klen);
        memcpy(block + klen, value, vlen);
        pthread_mutex_lock(&sa->lock);
        agg_scan_cb(&hdr, block, &sa->local);
        pthread_mutex_unlock(&sa->lock);
        if (block != stk) free(block);
    }
    return 0;
}
```
Replace with:
```c
    uint8_t stk[2048];
    uint8_t *block = (klen + vlen + 1 < sizeof(stk))
        ? stk : malloc(klen + vlen);
    if (block) {
        memcpy(block, key, klen);
        memcpy(block + klen, value, vlen);
        pthread_mutex_lock(&sa->lock);
        agg_scan_cb(&hdr, block, &sa->local);
        pthread_mutex_unlock(&sa->lock);
        if (block != stk) free(block);
    } else {
        sa->local.agg_error = ENOMEM;   /* never a silent skip */
        return 1;
    }
    return 0;
}
```
#### 4.4.7 D5c shard worker sdb
Anchor (exactly one occurrence in the file):
```text
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(sa->db_root, sa->object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "shard_agg_worker: slotcask_registry_get failed for %s/%s", sa->db_root, sa->object);
        return NULL;
    }

    /* Extract hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)sa->entry_count * sizeof(*hashes));
    if (!hashes) {
        LOG_ERROR(LOG_SUB_QUERY, "shard_agg_worker: malloc %d hashes failed", sa->entry_count);
        return NULL;
    }
```
Replace with:
```c
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(sa->db_root, sa->object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "shard_agg_worker: slotcask_registry_get failed for %s/%s", sa->db_root, sa->object);
        sa->local.agg_error = EIO;
        return NULL;
    }

    /* Extract hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)sa->entry_count * sizeof(*hashes));
    if (!hashes) {
        LOG_ERROR(LOG_SUB_QUERY, "shard_agg_worker: malloc %d hashes failed", sa->entry_count);
        sa->local.agg_error = ENOMEM;
        return NULL;
    }
```
#### 4.4.8 D5d indexed workers calloc
Anchor (exactly one occurrence in the file):
```text
    ShardAggCtx *workers = calloc(nshard_groups, sizeof(ShardAggCtx));
    for (int g = 0; g < nshard_groups; g++) {
```
Replace with:
```c
    ShardAggCtx *workers = calloc(nshard_groups, sizeof(ShardAggCtx));
    if (!workers) { main_ctx->agg_error = ENOMEM; return; }
    for (int g = 0; g < nshard_groups; g++) {
```
#### 4.4.9 free_local
Anchor (exactly one occurrence in the file):
```text
static void agg_ctx_free_local(AggCtx *ctx) {
    /* Same arena teardown as agg_free, but without freeing the shared
       resources (specs, mutex) — those are owned by the cloning origin. */
    agg_arena_free(&ctx->arena);
```
Replace with:
```c
static void agg_ctx_free_local(AggCtx *ctx) {
    /* Same arena teardown as agg_free, but without freeing the shared
       resources (specs, mutex) — those are owned by the cloning origin. */
    agg_ctx_release_nums(ctx);
    agg_env_free(&ctx->env);
    agg_arena_free(&ctx->arena);
```

#### 4.5 — leaf/bundled unification, IGB/VS eligibility and labels, pass-2 ring, igb_skip

#### 4.5.1 leaf block
Start anchor (exactly one occurrence):
```text
    /* Fast path: single-spec SUM / AVG / MIN / MAX on an indexed non-varchar
       field with no criteria / group_by / having. Walk btree leaves
       directly across the field's idx shards, decode encoded leaf bytes
       via decode_index_key_to_double (no record fetch, no slot probe),
       accumulate per-shard, merge.

       MIN  → first leaf entry per shard (ASC iter), take global min.
       MAX  → last  leaf entry per shard (DESC iter), take global max.
       SUM  → walk every entry, sum decoded values.
       AVG  → SUM with running count, divide at end.

       Skipped for varchar — typed_field_to_double atof()'s string content,
       so sum/avg/min/max on names is degenerate by design and a leaf-byte
       decode wouldn't change the output. Skipped for composite ("a+b")
       since those aren't a single typed scalar. Saves 200-400ms per
       query at 1M records vs the full record-decode scan. */
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
    /* Fast path: bundled multi-spec aggregate, no criteria, no group, no
       having. Every spec must be COUNT (count(*) or count(non-varchar))
       or SUM/AVG/MIN/MAX on an indexed non-varchar non-composite field.
```
Delete everything from the start anchor's first byte through the end anchor's last byte inclusive and insert:
```c
    /* Fast path: single-spec text min/max on an indexed calendar field.
       Varchar declines this bounded text decoder and uses full record spans,
       including with an index present (D8). */
    if (!tree && no_group && no_having && nspecs == 1 &&
        (specs[0].fn == AGG_MIN || specs[0].fn == AGG_MAX) &&
        fs.ts && specs[0].field[0] && !strchr(specs[0].field, '+')) {
        int fi = typed_field_index(fs.ts, specs[0].field);
        if (fi >= 0 &&
            agg_type_is_text(fs.ts->fields[fi].type) &&
            fs.ts->fields[fi].type != FT_VARCHAR &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
            int tr = agg_minmax_text_indexed(db_root, object, &sch, &specs[0],
                                    &fs.ts->fields[fi], format, delimiter,
                                    want_total);
            return tr < 0 ? -1 : 0;
        }
    }

    /* Fast path: no criteria, no group, no having. Every spec is count(*),
       count(field) on an indexed numeric-class field, or sum/avg/min/max on
       an indexed numeric-class field whose index key carries its full value
       (D1). One exact walk per distinct field; count(*) reads metadata. */
    if (!tree && no_group && no_having && fs.ts) {
        int eligible = 1, has_walk = 0;
        for (int i = 0; i < nspecs && eligible; i++) {
            AggSpec *sp = &specs[i];
            if (sp->fn == AGG_COUNT && !sp->field[0]) continue;
            AggNumSpec ns;
            int fi = sp->field[0] && !strchr(sp->field, '+')
                         ? typed_field_index(fs.ts, sp->field) : -1;
            if (fi < 0 || !agg_num_spec_for(fs.ts, (size_t)fi, &ns) ||
                !agg_index_exact(&ns) ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits))
                eligible = 0;
            else has_walk = 1;
        }
        if (eligible && has_walk) {
            QueryDeadline wdl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
            _Atomic size_t wbytes = 0;
            atomic_init(&wbytes, 0);
            AggCell cells[MAX_AGG_SPECS];
            memset(cells, 0, sizeof cells);
            agg_cells_bind(cells, nspecs, &wbytes, g_query_buffer_max_bytes);
            int done[MAX_AGG_SPECS] = {0}, err = 0;
            int n_idx = index_splits_for(sch.splits);
            for (int i = 0; i < nspecs && !err; i++) {
                if (done[i] || !specs[i].field[0]) continue;
                int fi = typed_field_index(fs.ts, specs[i].field);
                AggNumSpec ns;
                agg_num_spec_for(fs.ts, (size_t)fi, &ns);
                int sibs[MAX_AGG_SPECS], nsibs = 0, nmin = 0, nmax = 0, nfull = 0;
                for (int j = i; j < nspecs; j++) {
                    if (done[j] || strcmp(specs[j].field, specs[i].field) != 0) continue;
                    sibs[nsibs++] = j; done[j] = 1;
                    if (specs[j].fn == AGG_MIN) nmin++;
                    else if (specs[j].fn == AGG_MAX) nmax++;
                    else nfull++;
                }
                AggWalkArg proto = { .db_root = db_root, .object = object,
                    .field = specs[i].field, .spec = &ns, .dl = &wdl,
                    .budget = &wbytes, .limit = g_query_buffer_max_bytes };
                if (!nfull && (nmin == 0 || nmax == 0)) {   /* pure min or pure max */
                    proto.first_only = 1;
                    proto.desc = nmax > 0;
                }
                AggNum num; memset(&num, 0, sizeof num);
                int64_t cnt = 0;
                AggEnv env; agg_env_init(&env, &wbytes, g_query_buffer_max_bytes);
                int rc = agg_walk_field(&proto, n_idx, &num, &cnt);
                if (rc < 0) err = errno ? errno : EILSEQ;
                else if (rc > 0 || wdl.timed_out) err = ETIMEDOUT;
                for (int k = 0; k < nsibs && !err; k++) {
                    int si = sibs[k];
                    if (specs[si].fn == AGG_COUNT) {
                        if (agg_cell_count(&cells[si], cnt)) err = errno;
                    } else if (agg_cell_from_num(&ns, &num, specs[si].fn, cnt, &cells[si])) {
                        err = errno;
                    }
                }
                agg_num_release(&num, ns.cls, &env);
                agg_env_free(&env);
            }
            for (int i = 0; i < nspecs && !err; i++)
                if (specs[i].fn == AGG_COUNT && !specs[i].field[0] &&
                    agg_cell_count(&cells[i], (int64_t)get_live_count(db_root, object)))
                    err = errno;
            if (err) {
                agg_cells_free(cells, nspecs);
                return agg_report_error(err);
            }
            agg_emit_scalar_cells(specs, nspecs, cells, csv_delim, want_total);
            agg_cells_free(cells, nspecs);
            return 0;
        }
    }

    /* REMOVED-BUNDLED-MARKER */
```
#### 4.5.2 delete bundled tail
Start anchor (exactly one occurrence):
```text
    /* REMOVED-BUNDLED-MARKER */
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
    /* Streaming top-N dispatch: eligible_for_topn_stream + agg_run_topn_stream.
```
Keep the end anchor text; replace everything from the start anchor's first byte up to (not including) the end anchor with:
```text
(nothing — pure deletion)
```
#### 4.5.3 igb eligibility
Anchor (exactly one occurrence in the file):
```text
            if (!ctx.spec_tfs[i] ||
                agg_type_is_text(ctx.spec_tfs[i]->type) ||
                strchr(sp->field, '+') ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
                igb_eligible = 0; break;
            }
```
Replace with:
```c
            if (!ctx.spec_tfs[i] || !ctx.spec_is_num[i] ||
                !agg_index_exact(&ctx.spec_nums[i]) ||
                agg_type_is_text(ctx.spec_tfs[i]->type) ||
                strchr(sp->field, '+') ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
                igb_eligible = 0; break;
            }
```
#### 4.5.4 E4a bitmap count-only site
Anchor (exactly one occurrence in the file):
```text
                    char display[512];
                    if (decode_idx_to_buf(gtf_bm, bm_vals[i], bm_vlens[i],
                                          display, sizeof(display), 0) <= 0)
                        continue;
                    char *kvp[1] = { display };
                    AggBucket *b = agg_find_or_create(&ctx, kvp, 1, NULL, 0, NULL);
                    if (!b) break;
                    for (int j = 0; j < ctx.nspecs; j++) {
                        if (ctx.specs[j].fn == AGG_COUNT)
                            b->accums[j].count = (unsigned long)total;
                    }
```
Replace with:
```c
                    char display[512];
                    int dlen = agg_index_label(&ctx, 0, bm_vals[i], bm_vlens[i],
                                               display, sizeof(display));
                    if (dlen < 0) { ctx.agg_error = errno ? errno : EILSEQ; goto igb_skip; }
                    char *kvp[1] = { display };
                    AggBucket *b = agg_find_or_create(&ctx, kvp, 1, NULL, 0, NULL);
                    if (!b) break;
                    for (int j = 0; j < ctx.nspecs; j++) {
                        if (ctx.specs[j].fn == AGG_COUNT)
                            b->accums[j].count = (unsigned long)total;
                    }
```
#### 4.5.5 E4b bitmap slot site
Anchor (exactly one occurrence in the file):
```text
                        for (int i = 0; i < bm_n; i++) {
                            if (counts[i] == 0) continue;
                            char display[512];
                            if (decode_idx_to_buf(gtf_bm, bm_vals[i], bm_vlens[i],
                                                  display, sizeof(display), 0) <= 0)
                                continue;
```
Replace with:
```c
                        for (int i = 0; i < bm_n; i++) {
                            if (counts[i] == 0) continue;
                            char display[512];
                            if (agg_index_label(&ctx, 0, bm_vals[i], bm_vlens[i],
                                                display, sizeof(display)) < 0) {
                                ctx.agg_error = errno ? errno : EILSEQ;
                                goto igb_skip;
                            }
```
#### 4.5.6 E4c bitmap hbm site
Anchor (exactly one occurrence in the file):
```text
                for (int v = 0; v < bm_n_h && !bm_aborted; v++) {
                    /* Decode display string for bucket key. */
                    char display_h[512];
                    if (decode_idx_to_buf(gtf_bm_h,
                                          bm_vals_h[v], bm_vlens_h[v],
                                          display_h, sizeof(display_h), 0) <= 0)
                        continue;
```
Replace with:
```c
                for (int v = 0; v < bm_n_h && !bm_aborted; v++) {
                    /* Decode display string for bucket key (D2 exact label). */
                    char display_h[512];
                    if (agg_index_label(&ctx, 0, bm_vals_h[v], bm_vlens_h[v],
                                        display_h, sizeof(display_h)) < 0) {
                        ctx.agg_error = errno ? errno : EILSEQ;
                        goto igb_skip;
                    }
```
#### 4.5.7 E5a secmap assign
Anchor (exactly one occurrence in the file):
```text
                    sw[s].arena_hint = per_s_arena;
                    sw[s].dl         = &dl;
                }
```
Replace with:
```c
                    sw[s].arena_hint = per_s_arena;
                    sw[s].dl         = &dl;
                    sw[s].actx       = &ctx;
                    sw[s].gi         = g + 1;
                }
```
#### 4.5.8 E5b secmap err check
Anchor (exactly one occurrence in the file):
```text
                parallel_for_io(sec_map_build_worker, sw, n_idx_s,
                                sizeof(SecMapBuildWorker));
                /* Serial merge: walk each per-worker local_map and insert
```
Replace with:
```c
                parallel_for_io(sec_map_build_worker, sw, n_idx_s,
                                sizeof(SecMapBuildWorker));
                for (int s = 0; s < n_idx_s; s++)
                    if (sw[s].err && !ctx.agg_error) ctx.agg_error = sw[s].err;
                /* Serial merge: walk each per-worker local_map and insert
```
#### 4.5.9 serial igb label
Anchor (exactly one occurrence in the file):
```text
                            gbufs[0][0] = '\0';
                            int n = decode_idx_to_buf(gtf, prim,
                                                      prim_len, gbufs[0], sizeof(gbufs[0]), 0);
                            if (n < 0) continue;
                            gvals[0] = gbufs[0];
```
Replace with:
```c
                            gbufs[0][0] = '\0';
                            int n = agg_index_label(&ctx, 0, prim, prim_len,
                                                    gbufs[0], sizeof(gbufs[0]));
                            if (n < 0) { ctx.agg_error = errno ? errno : EILSEQ; aborted = 1; break; }
                            gvals[0] = gbufs[0];
```
#### 4.5.10 pass2 ring
Anchor (exactly one occurrence in the file):
```text
                #define PFB 16
                struct { uint8_t hash[16]; double v; } ring[PFB];
                for (int s = 0; s < n_idx_a && !aborted; s++) {
                    char idx_path[PATH_MAX];
                    build_idx_path(idx_path, sizeof(idx_path), db_root,
                                   object, fld, s);
                    BtRangeIter *it = btree_range_iter_open(
                        idx_path, "", 0, 0, AGG_VALUE_TIER_CEIL, 1, 1, 0);
                    if (!it) continue;
                    const char *val; size_t vlen; const uint8_t *hash16;
                    int filled = 0;
                    while (1) {
                        /* Refill the ring (copies hash16 — iter pointer is
                           invalidated on next iter_next call). */
                        while (filled < PFB) {
                            if (btree_range_iter_next(it, &val, &vlen, &hash16) != 1) break;
                            if (query_deadline_tick(&dl, &dl_counter)) { aborted = 1; break;
                    }
                            double v;
                            if (decode_index_key_to_double(atf,
                                                           (const uint8_t *)val,
                                                           vlen, &v) != AGG_VALUE_PRESENT) continue;
                            memcpy(ring[filled].hash, hash16, 16);
                            ring[filled].v = v;
                            __builtin_prefetch(&hbk.entries[hbk_index(&hbk, hash16)], 0, 0);
                            filled++;
                        }
                        if (aborted) break;
                        if (filled == 0) break;

                        /* Drain ring (cache-warm thanks to prefetch). */
                        for (int r = 0; r < filled; r++) {
                            struct AggBucket *bkt = hbk_get(&hbk, ring[r].hash);
                            if (!bkt) continue;
                            AggBucket *ab = (AggBucket *)bkt;
                            double v = ring[r].v;
                            for (int k = 0; k < nsibs; k++) {
                                AggAccum *a = &ab->accums[sibs[k]];
                                a->count++;
                                a->sum += v;
                                if (v < a->min) a->min = v;
                                if (v > a->max) a->max = v;
                            }
                        }
                        filled = 0;
                    }
                    btree_range_iter_close(it);
                }
                #undef PFB
```
Replace with:
```c
                #define PFB 16
                struct { uint8_t hash[16]; uint16_t klen; uint8_t key[560]; } ring[PFB];
                for (int s = 0; s < n_idx_a && !aborted; s++) {
                    char idx_path[PATH_MAX];
                    build_idx_path(idx_path, sizeof(idx_path), db_root,
                                   object, fld, s);
                    BtRangeIter *it = btree_range_iter_open(
                        idx_path, "", 0, 0, AGG_VALUE_TIER_CEIL, 1, 1, 0);
                    if (!it) continue;
                    const char *val; size_t vlen; const uint8_t *hash16;
                    int filled = 0;
                    while (1) {
                        /* Refill the ring (copies hash16 + key — iter pointers
                           are invalidated on the next iter_next call). */
                        while (filled < PFB) {
                            if (btree_range_iter_next(it, &val, &vlen, &hash16) != 1) break;
                            if (query_deadline_tick(&dl, &dl_counter)) { aborted = 1; break;
                    }
                            if (vlen == 0 || vlen > sizeof ring[0].key) continue;
                            memcpy(ring[filled].hash, hash16, 16);
                            memcpy(ring[filled].key, val, vlen);
                            ring[filled].klen = (uint16_t)vlen;
                            __builtin_prefetch(&hbk.entries[hbk_index(&hbk, hash16)], 0, 0);
                            filled++;
                        }
                        if (aborted) break;
                        if (filled == 0) break;

                        /* Drain ring (cache-warm thanks to prefetch). */
                        for (int r = 0; r < filled; r++) {
                            struct AggBucket *bkt = hbk_get(&hbk, ring[r].hash);
                            if (!bkt) continue;
                            AggBucket *ab = (AggBucket *)bkt;
                            for (int k = 0; k < nsibs; k++) {
                                AggAccum *a = &ab->accums[sibs[k]];
                                int rr = agg_num_offer_key(&ctx.spec_nums[sibs[k]],
                                                           ring[r].key, ring[r].klen,
                                                           &a->num, &ctx.env);
                                if (rr < 0) { ctx.agg_error = errno ? errno : EILSEQ; aborted = 1; break; }
                                if (rr == 1) a->count++;
                            }
                        }
                        filled = 0;
                    }
                    btree_range_iter_close(it);
                }
                #undef PFB
```
#### 4.5.11 igb_skip
Anchor (exactly one occurrence in the file):
```text
igb_skip:
    if (igb_done) {
```
Replace with:
```c
igb_skip:
    if (ctx.agg_error) {
        int e = ctx.agg_error;
        agg_free(&ctx);
        return agg_report_error(e);
    }
    if (igb_done) {
```
#### 4.5.12 vs eligibility
Anchor (exactly one occurrence in the file):
```text
            if (!ctx.spec_tfs[i] || ctx.spec_tfs[i]->type == FT_VARCHAR ||
                strchr(sp->field, '+') ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
                vs_eligible = 0; break;
            }
```
Replace with:
```c
            if (!ctx.spec_tfs[i] || !ctx.spec_is_num[i] ||
                ctx.spec_nums[i].cls == AGC_DEC ||
                ctx.spec_tfs[i]->type == FT_VARCHAR ||
                strchr(sp->field, '+') ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
                vs_eligible = 0; break;
            }
```
#### 4.5.13 E10a vs alloc
Anchor (exactly one occurrence in the file):
```text
        VSStaged *staged = calloc((size_t)limit, sizeof(VSStaged));
        if (!iters || !cur_keys || !cur_klens || !has_cur || !cur_hash16 ||
            !staged) {
            free(iters); free(cur_keys); free(cur_klens);
            free(has_cur); free(cur_hash16); free(staged);
            goto vs_skip;
        }
```
Replace with:
```c
        VSStaged *staged = calloc((size_t)limit, sizeof(VSStaged));
        AggNum *vs_nums = (!vs_need_lookup || limit <= 0) ? NULL
            : calloc((size_t)limit * (size_t)nspecs, sizeof(AggNum));
        int64_t *vs_counts = (!vs_need_lookup || limit <= 0) ? NULL
            : calloc((size_t)limit * (size_t)nspecs, sizeof(int64_t));
        if (!iters || !cur_keys || !cur_klens || !has_cur || !cur_hash16 ||
            !staged || (vs_need_lookup && (!vs_nums || !vs_counts))) {
            free(iters); free(cur_keys); free(cur_klens);
            free(has_cur); free(cur_hash16); free(staged);
            free(vs_nums); free(vs_counts);
            goto vs_skip;
        }
```
#### 4.5.14 E10b vs lc site 1
Anchor (exactly one occurrence in the file):
```text
            if (vs_need_lookup && pend_n >= VS_FLUSH_CAP) {
                VSLookupCtx lc = {
                    .staged   = staged,
                    .specs    = specs,
                    .spec_tfs = ctx.spec_tfs,
                    .spec_tidx = vs_spec_tidx,
                    .nspecs   = nspecs,
                };
                pthread_mutex_init(&lc.mu, NULL);
                vs_flush_pending(vs_sdb, &lc, pend, pend_n,
                                 iters, has_cur, cur_keys, cur_klens,
                                 cur_hash16, n_idx_g, db_root, object,
                                 ctx.group_fields[0], 1 /* reopen */);
                pthread_mutex_destroy(&lc.mu);
                pend_n = 0;
            }
```
Replace with:
```c
            if (vs_need_lookup && pend_n >= VS_FLUSH_CAP) {
                VSLookupCtx lc = {
                    .staged   = staged,
                    .specs    = specs,
                    .spec_tfs = ctx.spec_tfs,
                    .spec_tidx = vs_spec_tidx,
                    .nspecs   = nspecs,
                    .ts       = fs.ts,
                    .nums     = vs_nums,
                    .counts   = vs_counts,
                    .spec_nums = ctx.spec_nums,
                    .env      = &ctx.env,
                };
                pthread_mutex_init(&lc.mu, NULL);
                vs_flush_pending(vs_sdb, &lc, pend, pend_n,
                                 iters, has_cur, cur_keys, cur_klens,
                                 cur_hash16, n_idx_g, db_root, object,
                                 ctx.group_fields[0], 1 /* reopen */);
                pthread_mutex_destroy(&lc.mu);
                pend_n = 0;
                if (lc.err && !ctx.agg_error) { ctx.agg_error = lc.err; aborted = 1; }
            }
```
#### 4.5.15 E10c vs lc site 2
Anchor (exactly one occurrence in the file):
```text
        if (!aborted && vs_need_lookup && pend_n > 0) {
            VSLookupCtx lc = {
                .staged   = staged,
                .specs    = specs,
                .spec_tfs = ctx.spec_tfs,
                .spec_tidx = vs_spec_tidx,
                .nspecs   = nspecs,
            };
            pthread_mutex_init(&lc.mu, NULL);
            vs_flush_pending(vs_sdb, &lc, pend, pend_n,
                             iters, has_cur, cur_keys, cur_klens,
                             cur_hash16, n_idx_g, db_root, object,
                             ctx.group_fields[0], 0 /* no reopen */);
            pthread_mutex_destroy(&lc.mu);
            pend_n = 0;
        }
```
Replace with:
```c
        if (!aborted && vs_need_lookup && pend_n > 0) {
            VSLookupCtx lc = {
                .staged   = staged,
                .specs    = specs,
                .spec_tfs = ctx.spec_tfs,
                .spec_tidx = vs_spec_tidx,
                .nspecs   = nspecs,
                .ts       = fs.ts,
                .nums     = vs_nums,
                .counts   = vs_counts,
                .spec_nums = ctx.spec_nums,
                .env      = &ctx.env,
            };
            pthread_mutex_init(&lc.mu, NULL);
            vs_flush_pending(vs_sdb, &lc, pend, pend_n,
                             iters, has_cur, cur_keys, cur_klens,
                             cur_hash16, n_idx_g, db_root, object,
                             ctx.group_fields[0], 0 /* no reopen */);
            pthread_mutex_destroy(&lc.mu);
            pend_n = 0;
            if (lc.err && !ctx.agg_error) { ctx.agg_error = lc.err; aborted = 1; }
        }
```
#### 4.5.16 E10d vs staged init delete
Anchor (exactly one occurrence in the file):
```text
            VSStaged *cur = &staged[emitted];
            memcpy(cur->key, min_key, min_klen);
            cur->key[min_klen] = '\0';
            cur->klen = min_klen;
            cur->total_count = total_count;
            for (int i = 0; i < nspecs; i++) {
                cur->spec_sum[i] = 0.0;
                cur->spec_min[i] = 0.0;
                cur->spec_max[i] = 0.0;
                cur->spec_count[i] = 0;
            }

            emitted++;
```
Replace with:
```c
            VSStaged *cur = &staged[emitted];
            memcpy(cur->key, min_key, min_klen);
            cur->key[min_klen] = '\0';
            cur->klen = min_klen;
            cur->total_count = total_count;

            emitted++;
```
#### 4.5.17 E10e vs commit
Anchor (exactly one occurrence in the file):
```text
            for (int j = 0; j < nspecs; j++) {
                switch (specs[j].fn) {
                case AGG_COUNT:
                    b->accums[j].count = staged[i].total_count;
                    break;
                case AGG_SUM:
                case AGG_AVG:
                    b->accums[j].sum   = staged[i].spec_sum[j];
                    b->accums[j].count = staged[i].spec_count[j];
                    break;
                case AGG_MIN:
                    b->accums[j].min   = staged[i].spec_min[j];
                    b->accums[j].count = staged[i].spec_count[j];
                    break;
                case AGG_MAX:
                    b->accums[j].max   = staged[i].spec_max[j];
                    b->accums[j].count = staged[i].spec_count[j];
                    break;
                }
            }
        }
        free(staged);
```
Replace with:
```c
            for (int j = 0; j < nspecs; j++) {
                if (specs[j].fn == AGG_COUNT) {
                    b->accums[j].count = staged[i].total_count;
                    continue;
                }
                size_t k = (size_t)i * (size_t)nspecs + (size_t)j;
                b->accums[j].num   = vs_nums[k];   /* ownership moves */
                b->accums[j].count = vs_counts[k];
            }
        }
        free(vs_counts);
        free(vs_nums);
        free(staged);
```
#### 4.5.18 E10f vs abort free
Anchor (exactly one occurrence in the file):
```text
        if (aborted) {
            /* Low-cardinality data — let IGB handle it. ctx.ht hasn't
               been touched yet because we staged everything. */
            free(staged);
            goto vs_skip;
        }
```
Replace with:
```c
        if (aborted) {
            /* Low-cardinality data — let IGB handle it. ctx.ht hasn't
               been touched yet because we staged everything. Released
               exact states were never committed into ctx buckets. */
            for (size_t k = 0; k < (size_t)limit * (size_t)nspecs; k++)
                if (vs_nums) agg_num_release(&vs_nums[k], ctx.spec_nums[k % nspecs].cls, &ctx.env);
            free(vs_counts); free(vs_nums); free(staged);
            goto vs_skip;
        }
```

#### 4.5.19 count field requires record presence in igb
Anchor (exactly one occurrence in the file):
```text
                if (sp->field[0] && ctx.spec_tfs[i] &&
                    agg_count_inspects_presence(ctx.spec_tfs[i]->type)) {
                    igb_eligible = 0; break;
                     /* presence-doctrine count: per-record marker check */
                }
```
Replace with:
```c
                if (sp->field[0]) {
                    /* Numeric fields can be NULL too: grouping-key counts
                       cannot stand in for count(field)'s present counts. */
                    igb_eligible = 0; break;
                }
```

#### 4.5.20 count field requires record presence in vs
Anchor (exactly one occurrence in the file):
```text
                if (sp->field[0] && strcmp(sp->field, ctx.group_fields[0]) != 0) {
                    vs_eligible = 0; break;
                }
```
Replace with:
```c
                if (sp->field[0]) {
                    /* Even the group field can be NULL or an unset marker. */
                    vs_eligible = 0; break;
                }
```

#### 4.6 — walk-fetch dispatch, guards, same-field/keyset call sites

#### 4.6.1 F1a wfc merge
Anchor (exactly one occurrence in the file):
```text
                int any_indefinite = 0;
                int predicate_error=0;
                double best = 0.0;
                int have = 0;
                for (int s = 0; s < n_idx; s++) {
                    if (wargs[s].budget_exceeded) any_indefinite = 1;
                    if(wargs[s].predicate_error)predicate_error=wargs[s].predicate_error;
                    if (wargs[s].found) {
                        if (!have) { best = wargs[s].best; have = 1; }
                        else if (desc) { if (wargs[s].best > best) best = wargs[s].best; }
                        else           { if (wargs[s].best < best) best = wargs[s].best; }
                    }
                }
                free(wargs);
                if(predicate_error){OUT("{\"error\":\"invalid typed record\"}\n");errno=predicate_error;return -1;}
                if (dl.timed_out) {
                    LOG_WARN(LOG_SUB_QUERY, "cmd_aggregate_do: query deadline exceeded (single-shard fast path)");
                    OUT("{\"error\":\"query_timeout\"}\n");
                    return -1;
                }
                if (!any_indefinite) {
                    char vbuf[64];
                    fmt_double(vbuf, sizeof(vbuf), have ? best : 0.0);
                    char csv_delim_local = (format && strcmp(format, "csv") == 0)
                                             ? parse_csv_delim(delimiter) : 0;
                    if (csv_delim_local) {
                        csv_emit_cell(specs[0].alias, csv_delim_local);
                        OUT("\n");
                        csv_emit_cell(vbuf, csv_delim_local);
                        OUT("\n");
                    } else if (want_total) {
                        OUT("{\"rows\":{\"%s\":%s},\"total\":1}\n", specs[0].alias, vbuf);
                    } else {
                        OUT("{\"%s\":%s}\n", specs[0].alias, vbuf);
                    }
                    return 0;
                }
                /* else: fall through to keyset/intersect paths below */
```
Replace with:
```c
                int any_indefinite = 0;
                int predicate_error=0;
                int found = 0;
                AggNum total; memset(&total, 0, sizeof total);
                AggEnv wenv; agg_env_init(&wenv, NULL, 0);
                for (int s = 0; s < n_idx; s++) {
                    if (wargs[s].budget_exceeded) any_indefinite = 1;
                    if(wargs[s].predicate_error)predicate_error=wargs[s].predicate_error;
                    if (!predicate_error &&
                        agg_num_merge(&total, &wargs[s].num, wfc_ns.cls, &wenv))
                        predicate_error = errno ? errno : EILSEQ;
                    agg_num_release(&wargs[s].num, wfc_ns.cls, &wenv);
                    if (wargs[s].found) found = 1;
                }
                agg_env_free(&wenv);
                free(wargs);
                if(predicate_error){int e=predicate_error;agg_num_release(&total,wfc_ns.cls,&wenv);return agg_report_error(e);}
                if (dl.timed_out) {
                    LOG_WARN(LOG_SUB_QUERY, "cmd_aggregate_do: query deadline exceeded (single-shard fast path)");
                    agg_num_release(&total, wfc_ns.cls, &wenv);
                    OUT("{\"error\":\"query_timeout\"}\n");
                    return -1;
                }
                if (!any_indefinite) {
                    AggCell cell; memset(&cell, 0, sizeof cell);
                    agg_cells_bind(&cell, 1, ctx.shared_buffer_bytes, g_query_buffer_max_bytes);
                    int err = 0;
                    if (agg_cell_from_num(&wfc_ns, &total, specs[0].fn, found ? 1 : 0, &cell))
                        err = errno ? errno : EILSEQ;
                    agg_num_release(&total, wfc_ns.cls, &wenv);
                    if (err) { agg_cells_free(&cell, 1); return agg_report_error(err); }
                    char csv_delim_local = (format && strcmp(format, "csv") == 0)
                                             ? parse_csv_delim(delimiter) : 0;
                    agg_emit_scalar_cells(&specs[0], 1, &cell, csv_delim_local, want_total);
                    agg_cells_free(&cell, 1);
                    return 0;
                }
                agg_num_release(&total, wfc_ns.cls, &wenv);
                /* else: fall through to keyset/intersect paths below */
```
#### 4.6.2 F2a same-field guard
Anchor (exactly one occurrence in the file):
```text
        if (sf_leaf_node && sf_agg_fi >= 0 &&
            !agg_type_is_text(fs.ts->fields[sf_agg_fi].type) &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
```
Replace with:
```c
        if (sf_leaf_node && sf_agg_fi >= 0 &&
            !agg_type_is_text(fs.ts->fields[sf_agg_fi].type) &&
            agg_index_exact_field(fs.ts, &fs.ts->fields[sf_agg_fi]) &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
```
#### 4.6.3 F2b wfc guard
Anchor (exactly one occurrence in the file):
```text
        if (agg_fi >= 0 &&
            !agg_type_is_text(fs.ts->fields[agg_fi].type) &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
            const TypedField *agg_tf = &fs.ts->fields[agg_fi];
            int n_idx = index_splits_for(sch.splits);
            int desc = (specs[0].fn == AGG_MAX) ? 1 : 0;
            WfcArg *wargs = calloc((size_t)n_idx, sizeof(WfcArg));
```
Replace with:
```c
        if (agg_fi >= 0 &&
            !agg_type_is_text(fs.ts->fields[agg_fi].type) &&
            agg_index_exact_field(fs.ts, &fs.ts->fields[agg_fi]) &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
            const TypedField *agg_tf = &fs.ts->fields[agg_fi];
            AggNumSpec wfc_ns;
            agg_num_spec_for(fs.ts, (size_t)agg_fi, &wfc_ns);
            int n_idx = index_splits_for(sch.splits);
            int desc = (specs[0].fn == AGG_MAX) ? 1 : 0;
            WfcArg *wargs = calloc((size_t)n_idx, sizeof(WfcArg));
```
#### 4.6.4 F3a call site 0
Anchor (exactly one occurrence in the file):
```text
            int minmax_status0=agg_minmax_same_field_btree(
                    db_root, object, specs[0].field, specs[0].alias,
                    agg_tf, sch.splits,
                    specs[0].fn == AGG_MAX, crit, format, delimiter, want_total);
```
Replace with:
```c
            int minmax_status0=agg_minmax_same_field_btree(
                    db_root, object, specs[0].field, specs[0].alias,
                    agg_tf, fs.ts, sch.splits,
                    specs[0].fn == AGG_MAX, crit, format, delimiter, want_total);
```
#### 4.6.5 F3b call site 1
Anchor (exactly one occurrence in the file):
```text
            int minmax_status1=agg_minmax_same_field_btree(
                    db_root, object, specs[0].field, specs[0].alias,
                    agg_tf, sch.splits,
                    specs[0].fn == AGG_MAX, crit, format, delimiter, want_total);
```
Replace with:
```c
            int minmax_status1=agg_minmax_same_field_btree(
                    db_root, object, specs[0].field, specs[0].alias,
                    agg_tf, fs.ts, sch.splits,
                    specs[0].fn == AGG_MAX, crit, format, delimiter, want_total);
```
#### 4.6.6 F4a keyset guard
Anchor (exactly one occurrence in the file):
```text
        if (crit_leaf_node && agg_fi >= 0 &&
            !agg_type_is_text(fs.ts->fields[agg_fi].type) &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits) &&
            leaf_is_indexed(&crit_leaf_node->leaf, db_root, object, NULL, 0)) {
```
Replace with:
```c
        if (crit_leaf_node && agg_fi >= 0 &&
            !agg_type_is_text(fs.ts->fields[agg_fi].type) &&
            agg_index_exact_field(fs.ts, &fs.ts->fields[agg_fi]) &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits) &&
            leaf_is_indexed(&crit_leaf_node->leaf, db_root, object, NULL, 0)) {
```
#### 4.6.7 F4b keyset call site
Anchor (exactly one occurrence in the file):
```text
                if (!atomic_load_explicit(&sk.full, memory_order_relaxed) &&
                    !dl.timed_out && keyset_size(ks) > 0) {
                    emit_min_max_via_keyset(db_root, object, &sch,
                                            &specs[0], agg_tf, ks,
                                            format, delimiter, want_total);
                    return 0;
                }
                keyset_free(ks);
```
Replace with:
```c
                if (!atomic_load_explicit(&sk.full, memory_order_relaxed) &&
                    !dl.timed_out && keyset_size(ks) > 0) {
                    int ks_rc = emit_min_max_via_keyset(db_root, object, &sch,
                                            &specs[0], agg_tf, fs.ts, ks,
                                            format, delimiter, want_total);
                    if (ks_rc == 0) return 0;       /* answered */
                    return ks_rc;                    /* -1: error already emitted */
                }
                keyset_free(ks);
```
#### 4.6.8 F5a intersect guard
Anchor (exactly one occurrence in the file):
```text
        if (agg_fi >= 0 &&
            !agg_type_is_text(fs.ts->fields[agg_fi].type) &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
            const TypedField *agg_tf = &fs.ts->fields[agg_fi];
            SearchCriterion *isect_leaves[MAX_INTERSECT_LEAVES];
```
Replace with:
```c
        if (agg_fi >= 0 &&
            !agg_type_is_text(fs.ts->fields[agg_fi].type) &&
            agg_index_exact_field(fs.ts, &fs.ts->fields[agg_fi]) &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
            const TypedField *agg_tf = &fs.ts->fields[agg_fi];
            SearchCriterion *isect_leaves[MAX_INTERSECT_LEAVES];
```
#### 4.6.9 F5b intersect call site
Anchor (exactly one occurrence in the file):
```text
                if (ks && !small_primary && !dl.timed_out &&
                    keyset_size(ks) > 0) {
                    emit_min_max_via_keyset(db_root, object, &sch,
                                            &specs[0], agg_tf, ks,
                                            format, delimiter, want_total);
                    return 0;
                }
                if (ks) keyset_free(ks);
```
Replace with:
```c
                if (ks && !small_primary && !dl.timed_out &&
                    keyset_size(ks) > 0) {
                    int ks_rc = emit_min_max_via_keyset(db_root, object, &sch,
                                            &specs[0], agg_tf, fs.ts, ks,
                                            format, delimiter, want_total);
                    if (ks_rc == 0) return 0;       /* answered */
                    return ks_rc;                    /* -1: error already emitted */
                }
                if (ks) keyset_free(ks);
```
#### 4.6.10 warg spec
Anchor (exactly one occurrence in the file):
```text
                    wargs[s].deadline  = &dl;
                    wargs[s].budget    = 10000;
                }
```
Replace with:
```c
                    wargs[s].deadline  = &dl;
                    wargs[s].budget    = 10000;
                    wargs[s].spec      = &wfc_ns;
                }
```

#### 4.7 — criteria-bundled route, having rewrite, sort/emitter call sites

#### 4.7.1 G1a aw eligibility
Anchor (exactly one occurrence in the file):
```text
            if (sp->fn == AGG_COUNT) {
                if (sp->field[0]) {
                    if (strchr(sp->field, '+')) { aw_eligible = 0; break;
                    }
                    int fi = typed_field_index(fs.ts, sp->field);
                    /* Presence-doctrine counts need per-record marker
                       checks — bail to the general scan path. */
                    if (fi >= 0 && agg_count_inspects_presence(fs.ts->fields[fi].type)) {
                        aw_eligible = 0; break;
                    }
                }
                aw_min_max_only = 0;
                continue;
            }
```
Replace with:
```c
            if (sp->fn == AGG_COUNT) {
                if (sp->field[0]) {
                    if (strchr(sp->field, '+')) { aw_eligible = 0; break;
                    }
                    int fi = typed_field_index(fs.ts, sp->field);
                    /* Presence-doctrine counts need per-record marker
                       checks — bail to the general scan path. */
                    if (fi >= 0 && agg_count_inspects_presence(fs.ts->fields[fi].type)) {
                        aw_eligible = 0; break;
                    }
                    /* count(field) on this route = walked PRESENT entries
                       (scan parity: NULLs excluded), so the field must be
                       walked here too. */
                    if (fi < 0 || !agg_index_exact_field(fs.ts, &fs.ts->fields[fi]) ||
                        !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
                        aw_eligible = 0; break;
                    }
                }
                aw_min_max_only = 0;
                continue;
            }
```
#### 4.7.2 G1b aw numeric eligibility
Anchor (exactly one occurrence in the file):
```text
            int fi = typed_field_index(fs.ts, sp->field);
            if (fi < 0 || agg_type_is_text(fs.ts->fields[fi].type) ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
                aw_eligible = 0; break;
            }
            if (sp->fn != AGG_MIN && sp->fn != AGG_MAX) aw_min_max_only = 0;
```
Replace with:
```c
            int fi = typed_field_index(fs.ts, sp->field);
            AggNumSpec aw_ns;
            if (fi < 0 || !agg_num_spec_for(fs.ts, (size_t)fi, &aw_ns) ||
                !agg_index_exact(&aw_ns) ||
                agg_type_is_text(fs.ts->fields[fi].type) ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
                aw_eligible = 0; break;
            }
            if (sp->fn != AGG_MIN && sp->fn != AGG_MAX) aw_min_max_only = 0;
```
#### 4.7.3 G1c aw block
Start anchor (exactly one occurrence):
```text
            if (!aw_indef && crit_ks && !dl.timed_out) {
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
                keyset_free(crit_ks);
                return 0;
            }
            if (crit_ks) keyset_free(crit_ks);
```
Delete everything from the start anchor's first byte through the end anchor's last byte inclusive and insert:
```c
            if (!aw_indef && crit_ks && !dl.timed_out) {
                long match_count = (long)keyset_size(crit_ks);
                AggCell cells[MAX_AGG_SPECS];
                memset(cells, 0, sizeof cells);
                agg_cells_bind(cells, nspecs, ctx.shared_buffer_bytes, g_query_buffer_max_bytes);
                int done[MAX_AGG_SPECS] = {0}, err = 0;
                int n_idx = index_splits_for(sch.splits);
                for (int i = 0; i < nspecs && !err; i++) {
                    if (done[i] || !specs[i].field[0]) continue;
                    int fi = typed_field_index(fs.ts, specs[i].field);
                    if (fi < 0) continue;
                    AggNumSpec ns;
                    agg_num_spec_for(fs.ts, (size_t)fi, &ns);
                    int sibs[MAX_AGG_SPECS], nsibs = 0;
                    for (int j = i; j < nspecs; j++) {
                        if (done[j] || strcmp(specs[j].field, specs[i].field) != 0) continue;
                        sibs[nsibs++] = j; done[j] = 1;
                    }
                    AggWalkArg proto = { .db_root = db_root, .object = object,
                        .field = specs[i].field, .spec = &ns, .ks = crit_ks,
                        .dl = &dl, .budget = ctx.shared_buffer_bytes,
                        .limit = g_query_buffer_max_bytes };
                    AggNum num; memset(&num, 0, sizeof num);
                    int64_t cnt = 0;
                    int rc = agg_walk_field(&proto, n_idx, &num, &cnt);
                    if (rc < 0) err = errno ? errno : EILSEQ;
                    else if (rc > 0 || dl.timed_out) err = ETIMEDOUT;
                    for (int k = 0; k < nsibs && !err; k++) {
                        int si = sibs[k];
                        if (specs[si].fn == AGG_COUNT) {
                            /* count(field) = walked PRESENT entries within the
                               criteria set — scan parity (NULLs excluded). */
                            if (agg_cell_count(&cells[si], cnt)) err = errno;
                        } else if (agg_cell_from_num(&ns, &num, specs[si].fn, cnt, &cells[si])) {
                            err = errno;
                        }
                    }
                    agg_num_release(&num, ns.cls, &ctx.env);
                }
                for (int i = 0; i < nspecs && !err; i++)
                    if (specs[i].fn == AGG_COUNT && !specs[i].field[0] &&
                        agg_cell_count(&cells[i], match_count))
                        err = errno;   /* count(*) = |KeySet| */
                if (err) {
                    agg_cells_free(cells, nspecs);
                    keyset_free(crit_ks);
                    return agg_report_error(err);
                }
                agg_emit_scalar_cells(specs, nspecs, cells, csv_delim, want_total);
                agg_cells_free(cells, nspecs);
                keyset_free(crit_ks);
                return 0;
            }
            if (crit_ks) keyset_free(crit_ks);
```
#### 4.7.4 having group
Start anchor (exactly one occurrence):
```text
static AggResult agg_bucket_value(const AggCtx *ctx, AggBucket *bkt,
                                  const char *name) {
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
/* Collect all buckets into a flat array */
```
Keep the end anchor text; replace everything from the start anchor's first byte up to (not including) the end anchor with:
```c
static const dec_t AGG_ZERO_DEC = {0};

/* Zero value for a numeric result with no contributors (sorting keeps the
   historical "absent compares as 0"). */
static void agg_value_zero_fill(AggValue *v) {
    v->present = 1;
    if (v->cls == AGC_DEC && !v->dv) v->dv = &AGG_ZERO_DEC;
}

static void agg_result_release(AggResult *r) { agg_value_release(&r->val); }

/* 0 ok / -1 errno. The result may borrow the bucket's decimals: it must not
   outlive the bucket and must be released with agg_result_release. */
static int agg_bucket_value(const AggCtx *ctx, AggBucket *bkt, const char *name,
                            AggResult *out) {
    memset(out, 0, sizeof *out);
    out->kind = AGG_RESULT_NUMBER;
    agg_value_init(&out->val);
    for (int g = 0; g < ctx->ngroups; g++) {
        if (strcmp(ctx->group_fields[g], name) != 0) continue;
        const char *v = bkt->group_vals[g];
        if (!ctx->group_is_num[g]) {
            out->kind = AGG_RESULT_TEXT;
            out->present = v[0] != '\0';
            out->text = v;
            out->text_len = strlen(v);
            return 0;
        }
        return agg_value_from_label(&ctx->group_nums[g], v, &out->val) ? -1
               : ((out->present = out->val.present), 0);
    }
    for (int i = 0; i < ctx->nspecs; i++) {
        if (strcmp(ctx->specs[i].alias, name) != 0) continue;
        const AggSpec *spec = &ctx->specs[i];
        AggAccum *a = &bkt->accums[i];
        if (spec->fn == AGG_COUNT) {
            agg_value_from_count(&out->val, a->count);
            out->present = 1;
            return 0;
        }
        if (ctx->spec_tfs[i] && agg_type_is_text(ctx->spec_tfs[i]->type)) {
            out->kind = AGG_RESULT_TEXT;
            out->present = a->text_present;
            out->text = spec->fn == AGG_MIN ? a->text_min : a->text_max;
            out->text_len = spec->fn == AGG_MIN ? a->text_min_len : a->text_max_len;
            return 0;
        }
        if (!ctx->spec_is_num[i]) return 0;
        if (agg_num_value(&a->num, &ctx->spec_nums[i], spec->fn, a->count, &out->val))
            return -1;
        out->present = spec->fn == AGG_SUM ? 1 : a->count > 0;
        if (!out->val.present) out->val.cls = ctx->spec_nums[i].cls;
        return 0;
    }
    return 0;
}

static int agg_text_matches(const AggResult *r, const SearchCriterion *c) {
    if (!r->present) return c->op == OP_NOT_EXISTS;
    const char *v = r->text; size_t vl = r->text_len;
    size_t ql = strlen(search_criterion_value(c));
    int cmp = agg_text_cmp(v, vl, search_criterion_value(c), ql);
    switch (c->op) {
    case OP_EQUAL: return cmp == 0;
    case OP_NOT_EQUAL: return cmp != 0;
    case OP_LESS: return cmp < 0;
    case OP_GREATER: return cmp > 0;
    case OP_LESS_EQ: return cmp <= 0;
    case OP_GREATER_EQ: return cmp >= 0;
    case OP_BETWEEN: {
        int lo = agg_text_cmp(v, vl, search_criterion_value(c), strlen(search_criterion_value(c)));
        int hi = agg_text_cmp(v, vl, search_criterion_value2(c), strlen(search_criterion_value2(c)));
        return (c->min_exclusive ? lo > 0 : lo >= 0) &&
               (c->max_exclusive ? hi < 0 : hi <= 0);
    }
    case OP_IN:
        for (int i = 0; i < c->in_count; i++)
            if (agg_text_cmp(v, vl, c->in_values[i], strlen(c->in_values[i])) == 0) return 1;
        return 0;
    case OP_NOT_IN:
        for (int i = 0; i < c->in_count; i++)
            if (agg_text_cmp(v, vl, c->in_values[i], strlen(c->in_values[i])) == 0) return 0;
        return 1;
    case OP_EXISTS: return 1;
    default: return 0;
    }
}

/* 1 match / 0 no match / -1 error (errno). Numeric comparisons are exact:
   decimals by dec_cmp, integers as 128-bit, floats as doubles. */
static int agg_result_matches(const AggResult *r, const SearchCriterion *c) {
    if (r->kind == AGG_RESULT_TEXT) return agg_text_matches(r, c);
    if (!r->present) return c->op == OP_NOT_EXISTS;
    int cmp = 0;
    switch (c->op) {
    case OP_EXISTS: return 1;
    case OP_NOT_EXISTS: return 0;
    case OP_EQUAL: case OP_NOT_EQUAL: case OP_LESS: case OP_GREATER:
    case OP_LESS_EQ: case OP_GREATER_EQ:
        if (agg_value_cmp_text(&r->val, search_criterion_value(c), &cmp)) return -1;
        switch (c->op) {
        case OP_EQUAL: return cmp == 0;
        case OP_NOT_EQUAL: return cmp != 0;
        case OP_LESS: return cmp < 0;
        case OP_GREATER: return cmp > 0;
        case OP_LESS_EQ: return cmp <= 0;
        default: return cmp >= 0;
        }
    case OP_BETWEEN: {
        int lo, hi;
        if (agg_value_cmp_text(&r->val, search_criterion_value(c), &lo) ||
            agg_value_cmp_text(&r->val, search_criterion_value2(c), &hi))
            return -1;
        return (c->min_exclusive ? lo > 0 : lo >= 0) &&
               (c->max_exclusive ? hi < 0 : hi <= 0);
    }
    case OP_IN: case OP_NOT_IN: {
        int found = 0;
        for (int i = 0; i < c->in_count && !found; i++) {
            if (agg_value_cmp_text(&r->val, c->in_values[i], &cmp)) return -1;
            found = cmp == 0;
        }
        return c->op == OP_IN ? found : !found;
    }
    default: return 0;
    }
}

/* One leaf against one bucket: 1/0, or -1 with errno set. */
static int agg_having_leaf_match(const AggCtx *ctx, AggBucket *bkt,
                                 const SearchCriterion *leaf) {
    AggResult val;
    if (agg_bucket_value(ctx, bkt, leaf->field, &val)) return -1;
    int m = agg_result_matches(&val, leaf);
    int e = errno;
    agg_result_release(&val);
    if (m < 0) errno = e;
    return m;
}

/* Check having criteria against a bucket */
static int agg_having_match(const AggCtx *ctx, AggBucket *bkt,
                            SearchCriterion *having, int nhaving) {
    for (int i = 0; i < nhaving; i++) {
        int m = agg_having_leaf_match(ctx, bkt, &having[i]);
        if (m <= 0) return m;
    }
    return 1;
}

/* Tree-aware having evaluation — supports OR, nested AND/OR.
   Mirrors criteria_match_tree() but evaluates against aggregate bucket
   values instead of raw record bytes. */
static int agg_having_match_tree(const AggCtx *ctx, AggBucket *bkt,
                                 const CriteriaNode *n) {
    if (!n) return 1;
    switch (n->kind) {
    case CNODE_LEAF: return agg_having_leaf_match(ctx, bkt, &n->leaf);
    case CNODE_AND:
        for (int i = 0; i < n->n_children; i++) {
            int m = agg_having_match_tree(ctx, bkt, n->children[i]);
            if (m <= 0) return m;
        }
        return 1;
    case CNODE_OR:
        for (int i = 0; i < n->n_children; i++) {
            int m = agg_having_match_tree(ctx, bkt, n->children[i]);
            if (m != 0) return m;
        }
        return 0;
    }
    return 0;
}

```
#### 4.7.5 having kind
Anchor (exactly one occurrence in the file):
```text
            if (ctx->group_tfs[g]) {
                kind = agg_type_is_text(ctx->group_tfs[g]->type) ? 1 : 0;
                tname = agg_field_type_name(ctx->group_tfs[g]->type);
            }
```
Replace with:
```c
            if (ctx->group_tfs[g]) {
                kind = ctx->group_is_num[g] ? 0 : 1;
                tname = agg_field_type_name(ctx->group_tfs[g]->type);
            }
```
#### 4.7.6 having literal check
Anchor (exactly one occurrence in the file):
```text
    switch (c->op) {
    case OP_EQUAL: case OP_NOT_EQUAL:
    case OP_LESS: case OP_GREATER: case OP_LESS_EQ: case OP_GREATER_EQ:
    case OP_BETWEEN: case OP_IN: case OP_NOT_IN:
    case OP_EXISTS: case OP_NOT_EXISTS:
        return 0;
    default:
        snprintf(err, errsz,
                 "aggregate having unsupported for field type %s", tname);
        return -1;
    }
}
```
Replace with:
```c
    if (kind == 0 && c->op != OP_EXISTS && c->op != OP_NOT_EXISTS) {
        int n = (c->op == OP_IN || c->op == OP_NOT_IN) ? c->in_count
                : (c->op == OP_BETWEEN ? 2 : 1);
        for (int i = 0; i < n; i++) {
            const char *lit = (c->op == OP_IN || c->op == OP_NOT_IN)
                                  ? c->in_values[i]
                                  : (i == 0 ? search_criterion_value(c)
                                            : search_criterion_value2(c));
            dec_t d; dec_init(&d);
            dec_status st = dec_parse(lit, strlen(lit), &d, NULL, 0);
            dec_free(&d);
            if (st != DEC_OK) {
                snprintf(err, errsz, "invalid numeric value '%.64s' for having on '%s'",
                         lit, c->field);
                return -1;
            }
        }
    }
    switch (c->op) {
    case OP_EQUAL: case OP_NOT_EQUAL:
    case OP_LESS: case OP_GREATER: case OP_LESS_EQ: case OP_GREATER_EQ:
    case OP_BETWEEN: case OP_IN: case OP_NOT_IN:
    case OP_EXISTS: case OP_NOT_EXISTS:
        return 0;
    default:
        snprintf(err, errsz,
                 "aggregate having unsupported for field type %s", tname);
        return -1;
    }
}
```
#### 4.7.7 having tree call
Anchor (exactly one occurrence in the file):
```text
    if (having_tree) {
        int dst = 0;
        for (int i = 0; i < nbuckets; i++) {
            if (agg_having_match_tree(&ctx, buckets[i], having_tree))
                buckets[dst++] = buckets[i];
        }
        nbuckets = dst;
```
Replace with:
```c
    if (having_tree) {
        int dst = 0;
        for (int i = 0; i < nbuckets; i++) {
            int m = agg_having_match_tree(&ctx, buckets[i], having_tree);
            if (m < 0) { int e = errno; free(buckets); agg_free(&ctx); return agg_report_error(e); }
            if (m) buckets[dst++] = buckets[i];
        }
        nbuckets = dst;
```
#### 4.7.8 having flat call
Anchor (exactly one occurrence in the file):
```text
        if (nhaving > 0) {
            int dst = 0;
            for (int i = 0; i < nbuckets; i++) {
                if (agg_having_match(&ctx, buckets[i], having, nhaving))
                    buckets[dst++] = buckets[i];
            }
            nbuckets = dst;
        }
```
Replace with:
```c
        if (nhaving > 0) {
            int dst = 0;
            for (int i = 0; i < nbuckets; i++) {
                int m = agg_having_match(&ctx, buckets[i], having, nhaving);
                if (m < 0) {
                    int e = errno;
                    free(buckets); free_criteria(having, nhaving); agg_free(&ctx);
                    return agg_report_error(e);
                }
                if (m) buckets[dst++] = buckets[i];
            }
            nbuckets = dst;
        }
```
#### 4.7.9 sort call
Anchor (exactly one occurrence in the file):
```text
        g_sort_specs = specs;
        g_sort_nspecs = nspecs;
        strncpy(g_sort_field, order_by, 255);
        g_sort_desc = order_desc;
        g_sort_ngroups = ctx.ngroups;
        g_sort_group_fields = ctx.group_fields;
        g_sort_ctx = &ctx;
        qsort(buckets, nbuckets, sizeof(AggBucket *), agg_sort_cmp);
        /* The comparator's context points into this frame — drop it as
           soon as the sort is done so no later use can see stale state. */
        g_sort_ctx = NULL;
```
Replace with:
```c
        if (agg_sort_buckets(&ctx, buckets, nbuckets, order_by, order_desc)) {
            int e = errno;
            free(buckets); free_criteria(having, nhaving); agg_free(&ctx);
            return agg_report_error(e);
        }
```

#### 4.8 — sort function, final emitters, error branches, schema seam

#### 4.8.1 sort fn
Start anchor (exactly one occurrence):
```text
static int agg_sort_cmp(const void *a, const void *b) {
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
static void agg_free(AggCtx *ctx) {
```
Keep the end anchor text; replace everything from the start anchor's first byte up to (not including) the end anchor with:
```c
static int agg_sort_row_cmp(const void *pa, const void *pb) {
    const AggSortRow *x = *(const AggSortRow *const *)pa;
    const AggSortRow *y = *(const AggSortRow *const *)pb;
    int cmp;
    if (x->use_label) {
        cmp = strcmp(x->r.text, y->r.text);
    } else if (x->r.kind == AGG_RESULT_TEXT || y->r.kind == AGG_RESULT_TEXT) {
        cmp = agg_text_cmp(x->r.present ? x->r.text : "", x->r.present ? x->r.text_len : 0,
                           y->r.present ? y->r.text : "", y->r.present ? y->r.text_len : 0);
    } else {
        cmp = agg_value_cmp(&x->r.val, &y->r.val);
    }
    return g_sort_desc ? -cmp : cmp;
}

static int agg_sort_buckets(const AggCtx *ctx, AggBucket **buckets, int n,
                            const char *order_by, int order_desc) {
    if (n < 2) return 0;
    AggSortRow *rows = calloc((size_t)n, sizeof *rows);
    AggSortRow **ptrs = malloc((size_t)n * sizeof *ptrs);
    if (!rows || !ptrs) { free(rows); free(ptrs); errno = ENOMEM; return -1; }
    int rc = 0;
    for (int i = 0; i < n && !rc; i++) {
        rows[i].b = buckets[i];
        ptrs[i] = &rows[i];
        int is_group = -1;
        for (int g = 0; g < ctx->ngroups; g++)
            if (strcmp(ctx->group_fields[g], order_by) == 0) { is_group = g; break; }
        if (is_group >= 0 && !ctx->group_is_num[is_group]) {
            rows[i].use_label = 1;
            rows[i].r.kind = AGG_RESULT_TEXT;
            rows[i].r.text = buckets[i]->group_vals[is_group];
            continue;
        }
        if (agg_bucket_value(ctx, buckets[i], order_by, &rows[i].r)) { rc = -1; break; }
        if (rows[i].r.kind == AGG_RESULT_NUMBER && !rows[i].r.val.present)
            agg_value_zero_fill(&rows[i].r.val);
    }
    if (!rc) {
        g_sort_desc = order_desc;
        qsort(ptrs, (size_t)n, sizeof *ptrs, agg_sort_row_cmp);
        for (int i = 0; i < n; i++) buckets[i] = ptrs[i]->b;
    }
    int e = errno;
    for (int i = 0; i < n; i++) agg_result_release(&rows[i].r);
    free(rows); free(ptrs);
    if (rc) errno = e;
    return rc;
}

```
#### 4.8.2 cells render
Anchor (exactly one occurrence in the file):
```text
    /* Output — CSV path short-circuits before JSON emit. */
```
Replace with:
```c
    size_t ncell = (size_t)nbuckets * (size_t)nspecs;
    size_t nlabel = (size_t)nbuckets * (size_t)ctx.ngroups;
    AggCell *cells = agg_cells_alloc(ncell, ctx.shared_buffer_bytes, g_query_buffer_max_bytes);
    int output_error = cells ? 0 : (errno ? errno : ENOMEM);
    AggCell *labels = output_error ? NULL : agg_cells_alloc(nlabel,
                         ctx.shared_buffer_bytes, g_query_buffer_max_bytes);
    if (!output_error && !labels) output_error = errno ? errno : ENOMEM;
    for (int bi = 0; bi < nbuckets && !output_error; bi++) {
        AggBucket *bucket = buckets[bi];
        for (int g = 0; g < ctx.ngroups && !output_error; g++) {
            int present = !(bucket->group_nulls && bucket->group_nulls[g]);
            const char *text = bucket->group_vals[g];
            if (agg_cell_text(&labels[(size_t)bi * ctx.ngroups + g],
                              text, present ? strlen(text) : 0, present))
                output_error = errno ? errno : ENOMEM;
        }
        for (int i = 0; i < nspecs && !output_error; i++) {
            AggAccum *acc = &bucket->accums[i];
            AggCell *cell = &cells[(size_t)bi * nspecs + i];
            if (specs[i].fn == AGG_COUNT) {
                if (agg_cell_count(cell, acc->count)) output_error = errno;
            } else if (ctx.spec_is_num[i]) {
                if (agg_cell_from_num(&ctx.spec_nums[i], &acc->num, specs[i].fn,
                                      acc->count, cell)) output_error = errno;
            } else if (ctx.spec_tfs[i] && agg_type_is_text(ctx.spec_tfs[i]->type) &&
                       (specs[i].fn == AGG_MIN || specs[i].fn == AGG_MAX)) {
                const char *text = specs[i].fn == AGG_MIN ? acc->text_min : acc->text_max;
                size_t len = specs[i].fn == AGG_MIN ? acc->text_min_len : acc->text_max_len;
                if (agg_cell_text(cell, text, acc->text_present ? len : 0, acc->text_present))
                    output_error = errno;
            }
        }
    }
    if (output_error) {
        agg_cells_dispose(labels, nlabel); agg_cells_dispose(cells, ncell);
        free(buckets); free_criteria(having, nhaving); agg_free(&ctx);
        return agg_report_error(output_error);
    }

    /* Output — CSV path short-circuits before JSON emit. */
```

#### 4.8.3 csv emit
Anchor (exactly one occurrence in the file):
```text
            for (int i = 0; i < nspecs; i++) {
                if (ctx.ngroups > 0 || i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                AggAccum *a = &b->accums[i];
                char vbuf[64];
                if (agg_csv_minmax_text(&specs[i], a, ctx.spec_tfs[i], csv_delim))
                    continue;
                switch (specs[i].fn) {
                    case AGG_COUNT: snprintf(vbuf, sizeof(vbuf), "%ld", a->count); break;
                    case AGG_SUM:   fmt_double(vbuf, sizeof(vbuf), a->sum); break;
                    case AGG_AVG:   fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->sum / a->count : 0.0); break;
                    case AGG_MIN:   fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->min : 0.0); break;
                    case AGG_MAX:   fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->max : 0.0); break;
                    default:        vbuf[0] = '\0'; break;
                }
                csv_emit_cell(vbuf, csv_delim);
            }
```
Replace with:
```c
            for (int i = 0; i < nspecs; i++) {
                if (ctx.ngroups > 0 || i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                agg_cell_emit_csv(&cells[(size_t)bi * nspecs + i], csv_delim);
            }
```

#### 4.8.4 nogroup emit
Anchor (exactly one occurrence in the file):
```text
        for (int i = 0; i < nspecs; i++) {
            AggAccum *a = &b->accums[i];
            if (i > 0) OUT(",");
            char vbuf[64];
            switch (specs[i].fn) {
                case AGG_COUNT:
                    OUT("\"%s\":%ld", specs[i].alias, a->count);
                    break;
                case AGG_SUM:
                    fmt_double(vbuf, sizeof(vbuf), a->sum);
                    OUT("\"%s\":%s", specs[i].alias, vbuf);
                    break;
                case AGG_AVG:
                    /* §2.5: avg with zero contributing values (empty table
                       or all-NULL group) renders null, not 0. */
                    if (a->count > 0) {
                        fmt_double(vbuf, sizeof(vbuf), a->sum / a->count);
                        OUT("\"%s\":%s", specs[i].alias, vbuf);
                    } else {
                        OUT("\"%s\":null", specs[i].alias);
                    }
                    break;
                case AGG_MIN:
                    if (agg_json_minmax_text(&specs[i], a, ctx.spec_tfs[i])) break;
                    fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->min : 0.0);
                    OUT("\"%s\":%s", specs[i].alias, vbuf);
                    break;
                case AGG_MAX:
                    if (agg_json_minmax_text(&specs[i], a, ctx.spec_tfs[i])) break;
                    fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->max : 0.0);
                    OUT("\"%s\":%s", specs[i].alias, vbuf);
                    break;
            }
        }
```
Replace with:
```c
        for (int i = 0; i < nspecs; i++) {
            if (i > 0) OUT(",");
            OUT("\"%s\":", specs[i].alias);
            agg_cell_emit_json(&cells[i]);
        }
```

#### 4.8.5 grouped emit
Anchor (exactly one occurrence in the file):
```text
            for (int i = 0; i < nspecs; i++) {
                AggAccum *a = &b->accums[i];
                if (!first) OUT(",");
                char vbuf[64];
                if (agg_json_minmax_text(&specs[i], a, ctx.spec_tfs[i])) {
                    first = 0;
                    continue;
                }
                switch (specs[i].fn) {
                    case AGG_COUNT:
                        OUT("\"%s\":%ld", specs[i].alias, a->count);
                        break;
                    case AGG_SUM:
                        fmt_double(vbuf, sizeof(vbuf), a->sum);
                        OUT("\"%s\":%s", specs[i].alias, vbuf);
                        break;
                    case AGG_AVG:
                        /* §2.5: all-NULL (or no contributing) values → null. */
                        if (a->count > 0) {
                            fmt_double(vbuf, sizeof(vbuf), a->sum / a->count);
                            OUT("\"%s\":%s", specs[i].alias, vbuf);
                        } else {
                            OUT("\"%s\":null", specs[i].alias);
                        }
                        break;
                    case AGG_MIN:
                        fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->min : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf);
                        break;
                    case AGG_MAX:
                        fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->max : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf);
                        break;
                }
                first = 0;
            }
```
Replace with:
```c
            for (int i = 0; i < nspecs; i++) {
                if (!first) OUT(",");
                OUT("\"%s\":", specs[i].alias);
                agg_cell_emit_json(&cells[(size_t)bi * nspecs + i]);
                first = 0;
            }
```

#### 4.8.6 free cells nogroup
Anchor (exactly one occurrence in the file):
```text
        OUT(want_total ? "},\"total\":1}\n" : "}\n");
    } else {
        /* Group_by: array of objects */
```
Replace with:
```c
        OUT(want_total ? "},\"total\":1}\n" : "}\n");
    } else {
        /* Group_by: array of objects */
```

#### 4.8.7 free cells grouped
Anchor (exactly one occurrence in the file):
```text
        if (want_total) OUT("],\"total\":%d}\n", agg_total_groups);
        else OUT("]\n");
    }

    free(buckets);
```
Replace with:
```c
        if (want_total) OUT("],\"total\":%d}\n", agg_total_groups);
        else OUT("]\n");
    }
    agg_cells_dispose(labels, nlabel);
    agg_cells_dispose(cells, ncell);

    free(buckets);
```

#### 4.8.8 H8a error branch
Anchor (exactly one occurrence in the file):
```text
        } else if(ctx.predicate_error){
            OUT("{\"error\":\"invalid typed record\"}\n");
        } else if (ctx.budget_exceeded) {
```
Replace with:
```c
        } else if (ctx.agg_error) {
            agg_report_error(ctx.agg_error);
        } else if(ctx.predicate_error){
            OUT("{\"error\":\"invalid typed record\"}\n");
        } else if (ctx.budget_exceeded) {
```
#### 4.8.9 H8b late error
Anchor (exactly one occurrence in the file):
```text
    if(ctx.predicate_error){int error=ctx.predicate_error;agg_free(&ctx);OUT("{\"error\":\"invalid typed record\"}\n");errno=error;return -1;}
```
Replace with:
```c
    if (ctx.agg_error) { int e = ctx.agg_error; agg_free(&ctx); return agg_report_error(e); }
    if(ctx.predicate_error){int error=ctx.predicate_error;agg_free(&ctx);OUT("{\"error\":\"invalid typed record\"}\n");errno=error;return -1;}
```
#### 4.8.10 H9a signature
Anchor (exactly one occurrence in the file):
```text
                            const char *format, const char *delimiter, int want_total) {
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;

    Schema sch = load_schema(db_root, object);
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
```
Replace with:
```c
                            const char *format, const char *delimiter, int want_total,
                            const Schema *sch_in, const FieldSchema *fs_in) {
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;

    Schema sch;
    FieldSchema fs;
    if (sch_in && fs_in) { sch = *sch_in; fs = *fs_in; }   /* test seam: private schema */
    else {
        sch = load_schema(db_root, object);
        init_field_schema(&fs, db_root, object);
    }
```
#### 4.8.11 env init
Anchor (exactly one occurrence in the file):
```text
    for (int i = 0; i < ctx.nspecs && i < MAX_AGG_SPECS; i++) {
        ctx.spec_tfs[i] = NULL;
        /* Resolve TypedField for COUNT specs too — agg_scan_cb's
           count(field) presence-doctrine check needs ctx.spec_tfs to
           know the field's type. Composite ("a+b") still resolves NULL. */
        if (ctx.specs[i].field[0] && fs.ts && !strchr(ctx.specs[i].field, '+')) {
            int idx = typed_field_index(fs.ts, ctx.specs[i].field);
            if (idx >= 0) ctx.spec_tfs[i] = &fs.ts->fields[idx];
        }
    }
```
Replace with:
```c
    for (int i = 0; i < ctx.nspecs && i < MAX_AGG_SPECS; i++) {
        ctx.spec_tfs[i] = NULL;
        /* Resolve TypedField for COUNT specs too — agg_scan_cb's
           count(field) presence-doctrine check needs ctx.spec_tfs to
           know the field's type. Composite ("a+b") still resolves NULL. */
        if (ctx.specs[i].field[0] && fs.ts && !strchr(ctx.specs[i].field, '+')) {
            int idx = typed_field_index(fs.ts, ctx.specs[i].field);
            if (idx >= 0) ctx.spec_tfs[i] = &fs.ts->fields[idx];
        }
    }

    agg_env_init(&ctx.env, &agg_budget_bytes, g_query_buffer_max_bytes);
    for (int i = 0; i < ctx.nspecs && i < MAX_AGG_SPECS; i++)
        if (ctx.spec_tfs[i] && ctx.specs[i].fn != AGG_COUNT)
            ctx.spec_is_num[i] = (uint8_t)agg_num_spec_for(
                fs.ts, (size_t)(ctx.spec_tfs[i] - fs.ts->fields), &ctx.spec_nums[i]);
    for (int i = 0; i < ctx.ngroups; i++)
        if (ctx.group_tfs[i])
            ctx.group_is_num[i] = (uint8_t)(agg_num_spec_for(
                fs.ts, (size_t)ctx.group_idx[i], &ctx.group_nums[i]) &&
                ctx.group_tfs[i]->type != FT_BOOL);
    /* Packed decimals are variable-width values. Legacy numeric's 8-byte
       raw grouping shortcut would collide or disagree with index keys. */
    if (fs.ts && fs.ts->packed.nfields)
        for (int i = 0; i < ctx.ngroups; i++)
            if (ctx.group_is_num[i] && ctx.group_nums[i].cls == AGC_DEC)
                ctx.use_int_keys = 0;
```
#### 4.8.12 H11a cmd_aggregate call
Anchor (exactly one occurrence in the file):
```text
    int r = cmd_aggregate_do(db_root, object, tree, specs, nspecs,
                            group_by_json, NULL,
                            having_tree,
                            order_by, order_desc, limit,
                            format, delimiter, want_total);
```
Replace with:
```c
    int r = cmd_aggregate_do(db_root, object, tree, specs, nspecs,
                            group_by_json, NULL,
                            having_tree,
                            order_by, order_desc, limit,
                            format, delimiter, want_total, NULL, NULL);
```
#### 4.8.13 H11b cmd_aggregate_tree call
Anchor (exactly one occurrence in the file):
```text
    int r = cmd_aggregate_do(db_root, object, criteria_tree, specs, naggs,
                            group_by_buf,
                            NULL,                  /* having_json — not used */
                            having_tree,           /* passed directly */
                            order_by, order_desc, limit,
                            format, delimiter, want_total);
```
Replace with:
```c
    int r = cmd_aggregate_do(db_root, object, criteria_tree, specs, naggs,
                            group_by_buf,
                            NULL,                  /* having_json — not used */
                            having_tree,           /* passed directly */
                            order_by, order_desc, limit,
                            format, delimiter, want_total, NULL, NULL);
```
#### 4.8.14 H11c seam fn
Anchor (exactly one occurrence **after 4.8.12 has been applied** — it quotes
4.8.12's replacement text, so it has zero matches in the unmodified base);
insert immediately after it:
```text
                            order_by, order_desc, limit,
                            format, delimiter, want_total, NULL, NULL);
    free(specs);
    return r;
}
```
Inserted text:
```c

#ifdef TEST_BUILD
/* Aggregate over a caller-supplied (e.g. private packed) schema; the rest of
   the pipeline is byte-for-byte the production path. */
int cmd_aggregate_with_schema_for_test(const char *db_root, const char *object,
        const Schema *schema, const FieldSchema *fields,
        const char *criteria_json, const char *group_by_json,
        const char *aggregates_json, const char *having_json,
        const char *order_by, int order_desc, int limit,
        const char *format, const char *delimiter, int want_total) {
    if (!schema || !fields || !fields->ts) { errno = EINVAL; return -1; }
    AggSpec *specs = NULL;
    int nspecs = parse_agg_specs(aggregates_json, &specs);
    if (nspecs <= 0) { free(specs); errno = EINVAL; return -1; }
    const char *perr = NULL;
    CriteriaNode *tree = criteria_json && criteria_json[0]
        ? parse_criteria_tree(criteria_json, &perr) : NULL;
    CriteriaNode *having = having_json && having_json[0]
        ? parse_criteria_tree(having_json, &perr) : NULL;
    int rc = perr ? -1 : cmd_aggregate_do(db_root, object, tree, specs, nspecs,
                         group_by_json, NULL, having, order_by, order_desc,
                         limit, format, delimiter, want_total, schema, fields);
    free_criteria_tree(having);
    free_criteria_tree(tree);
    free(specs);
    return rc;
}
#endif
```

#### 4.8.15 grouped labels prepared
Anchor (exactly one occurrence in the file):
```text
                if (b->group_nulls && b->group_nulls[g]) {
                    OUT("\"%s\":null", ctx.group_fields[g]);
                } else {
                    char *gv = json_escape_const(b->group_vals[g]);
                    OUT("\"%s\":\"%s\"", ctx.group_fields[g], gv ? gv : "");
                    free(gv);
                }
```
Replace with:
```c
                OUT("\"%s\":", ctx.group_fields[g]);
                agg_cell_emit_json(&labels[(size_t)bi * ctx.ngroups + g]);
```
#### 4.8.16 csv labels prepared
Anchor (exactly one occurrence in the file):
```text
                csv_emit_cell(b->group_vals[g], csv_delim);
```
Replace with:
```c
                agg_cell_emit_csv(&labels[(size_t)bi * ctx.ngroups + g], csv_delim);
```

#### 4.8.17 csv output cleanup
Anchor (exactly one occurrence in the file):
```text
            OUT("\n");
        }

        free(buckets);
        free_criteria(having, nhaving);
```
Replace with:
```c
            OUT("\n");
        }

        agg_cells_dispose(labels, nlabel);
        agg_cells_dispose(cells, ncell);
        free(buckets);
        free_criteria(having, nhaving);
```

#### 4.8.18 csv unused bucket removal
Anchor (exactly one occurrence in the file):
```text
        for (int bi = 0; bi < nbuckets; bi++) {
            AggBucket *b = buckets[bi];
            for (int g = 0; g < ctx.ngroups; g++) {
```
Replace with:
```c
        for (int bi = 0; bi < nbuckets; bi++) {
            for (int g = 0; g < ctx.ngroups; g++) {
```

#### 4.8.19 scalar unused bucket removal
Anchor (exactly one occurrence in the file):
```text
        /* No group_by: single object */
        AggBucket *b = buckets[0];
```
Replace with:
```c
        /* No group_by: single object */
```

#### 4.8.20 grouped unused bucket removal
Anchor (exactly one occurrence in the file):
```text
        for (int bi = 0; bi < nbuckets; bi++) {
            AggBucket *b = buckets[bi];
            if (bi > 0) OUT(",");
```
Replace with:
```c
        for (int bi = 0; bi < nbuckets; bi++) {
            if (bi > 0) OUT(",");
```

#### 4.8.21 delete fallible text emitters
Start anchor (exactly one occurrence):
```text
static int agg_csv_minmax_text(const AggSpec *spec, const AggAccum *a,
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
static int agg_minmax_text_indexed(const char *db_root, const char *object,
```
Keep the end anchor text; replace everything from the start anchor's first byte up to (not including) the end anchor with:
```text
(nothing — pure deletion)
```

#### 4.8.22 indexed calendar text prepared
Start anchor (exactly one occurrence):
```text
static int agg_minmax_text_indexed(const char *db_root, const char *object,
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
static int agg_minmax_same_field_btree(
```
Keep the end anchor text; replace everything from the start anchor's first byte up to (not including) the end anchor with:
```c
static int agg_minmax_text_indexed(const char *db_root, const char *object,
                                   const Schema *sch, const AggSpec *spec,
                                   const TypedField *tf, const char *format,
                                   const char *delimiter, int want_total) {
    int desc = spec->fn == AGG_MAX, have = 0, error = 0;
    char best[1024]; size_t best_len = 0;
    for (int sh = 0; sh < index_splits_for(sch->splits) && !error; sh++) {
        char path[PATH_MAX]; struct stat st;
        build_idx_path(path, sizeof path, db_root, object, spec->field, sh);
        if (stat(path, &st) < 0) { if (errno != ENOENT) error = EIO; continue; }
        BtRangeIter *it = btree_range_iter_open(path, "", 0, 0,
                                               AGG_VALUE_TIER_CEIL, 1, 1, desc);
        if (!it) { error = EIO; break; }
        const char *value; size_t vlen; const uint8_t *hash;
        while (btree_range_iter_next(it, &value, &vlen, &hash) == 1) {
            char text[1024]; size_t len = 0;
            if (vlen >= sizeof text) { error = E2BIG; break; }
            if (decode_index_key_to_text(tf, (const uint8_t *)value, vlen,
                                         text, &len) != AGG_VALUE_PRESENT) continue;
            if (!have || (desc ? agg_text_cmp(text, len, best, best_len) > 0
                              : agg_text_cmp(text, len, best, best_len) < 0)) {
                memcpy(best, text, len + 1); best_len = len; have = 1;
            }
            break;
        }
        btree_range_iter_close(it);
    }
    _Atomic size_t bytes = 0;
    AggCell cell = {0}; agg_cells_bind(&cell, 1, &bytes, g_query_buffer_max_bytes);
    if (!error && agg_cell_text(&cell, best, have ? best_len : 0, have)) error = errno;
    if (error) { agg_cells_free(&cell, 1); return agg_report_error(error); }
    char delim = format && !strcmp(format, "csv") ? parse_csv_delim(delimiter) : 0;
    agg_emit_scalar_cells(spec, 1, &cell, delim, want_total);
    agg_cells_free(&cell, 1);
    return 1;
}

```

#### 4.9 — dead-code removal and stale-comment cleanup

#### 4.9.1 delete decode_index_key_to_double
Start anchor (exactly one occurrence):
```text
/* Decode a btree index leaf entry's encoded bytes back to the original
```
End anchor (exactly one occurrence in the file, after the start anchor):
```text
/* Decode an indexed varchar/calendar key into the canonical textual value. */
```
Keep the end anchor text; replace everything from the start anchor's first byte up to (not including) the end anchor with:
```text
(nothing — pure deletion)
```
#### 4.9.2 I2a drop gtf_bm
Anchor (exactly one occurrence in the file):
```text
            const TypedField *gtf_bm = ctx.group_tfs[0];
            uint8_t  bm_vals[256][1024];
```
Replace with:
```c
            uint8_t  bm_vals[256][1024];
```
#### 4.9.3 I2b drop gtf_bm_h
Anchor (exactly one occurrence in the file):
```text
            const TypedField *gtf_bm_h = ctx.group_tfs[0];
            int      bm_dl_h = 0;
```
Replace with:
```c
            int      bm_dl_h = 0;
```
#### 4.9.4 I2c drop atf
Anchor (exactly one occurrence in the file):
```text
                const TypedField *atf = ctx.spec_tfs[i];
                /* Collect every spec sharing this field. */
```
Replace with:
```c
                /* Collect every spec sharing this field. */
```
#### 4.9.5 I3a comment topn
Anchor (exactly one occurrence in the file):
```text
       the numeric emit's decode_index_key_to_double re-strips internally
       (MISSING for the NULL key — accumulators then skip it). */
```
Replace with:
```c
       the numeric emit's exact label helper re-strips internally
       (the NULL key renders null). */
```
#### 4.9.6 I3b comment csv text
Anchor (exactly one occurrence in the file):
```text
   output (textual spec) so the caller skips the numeric path, 0 when
   the spec is numeric and must render through fmt_double. */
```
Replace with:
```c
   output (textual spec) so the caller skips the numeric path, 0 when
   the spec is numeric and must render through the exact cell renderer. */
```
#### 4.9.7 I3c comment keyset
Anchor (exactly one occurrence in the file):
```text
       fetches; the agg value comes straight from the leaf bytes via
       decode_index_key_to_double. ~10× faster than agg_run_plan's path
```
Replace with:
```c
       fetches; the agg value comes straight from the leaf bytes via
       the exact key decoder. ~10× faster than agg_run_plan's path
```
#### 4.9.8 I3d comment vs
Anchor (exactly one occurrence in the file):
```text
        - SUM/AVG/MIN/MAX on an indexed non-varchar non-composite field
          (record fetch by hash16 + typed_field_to_double). */
```
Replace with:
```c
        - SUM/AVG/MIN/MAX on an indexed non-varchar non-composite field
          (record fetch by hash16 + exact row offer). */
```

### Task 3 test code (authored before any production edit)


**New `src/test/cases/test_decimal_order_decode.c` (complete file; registered in Task 1/2):**

```c
/* src/test/cases/test_decimal_order_decode.c
 *
 * decimal_order_decode is the exact inverse of decimal_order_into over the
 * canonical value domain: sign, weight and limbs round-trip, the display
 * scale becomes the minimal lossless one, and every malformed payload is
 * rejected without a partial result. Expectations are literal strings; the
 * oracle for ordering equivalence is the byte order of the encoder itself
 * (verified against dec_cmp on 40x40 pairs). */
#include "test_runner.h"
#include "test_assert.h"
#include "../db/decimal_order.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>

static int dod_enc(const char *text, uint8_t *buf, size_t cap, size_t *len) {
    dec_t d; dec_init(&d);
    if (dec_parse(text, strlen(text), &d, NULL, 0) != DEC_OK) { dec_free(&d); return -1; }
    int rc = decimal_order_into(&d, buf, cap, len);
    dec_free(&d);
    return rc;
}

static void dod_roundtrip(const char *in, const char *expect_minimal) {
    uint8_t buf[600]; size_t n = 0;
    ASSERT_EQ_INT(dod_enc(in, buf, sizeof buf, &n), 0, "encode");
    dec_t out; dec_init(&out);
    ASSERT_EQ_INT(decimal_order_decode(buf, n, &out), 0, "decode");
    char text[300]; size_t tl = 0;
    ASSERT_EQ_INT(dec_to_text(&out, text, sizeof text, &tl), DEC_OK, "render");
    ASSERT_EQ_STR(text, expect_minimal, in);
    dec_t again; dec_init(&again);
    ASSERT_TRUE(dec_parse(in, strlen(in), &again, NULL, 0) == DEC_OK &&
                dec_cmp(&again, &out) == 0, "mathematical value preserved");
    ASSERT_EQ_INT(decimal_min_dscale(&out), out.dscale, "dscale is minimal");
    dec_free(&out); dec_free(&again);
}

static int dod_run(void) {
    static const struct { const char *in, *minimal; } v[] = {
        {"0", "0"}, {"0.000", "0"}, {"-0.0", "0"}, {"1", "1"}, {"-1", "-1"},
        {"1.2300", "1.23"}, {"100", "100"}, {"1e2", "100"}, {"0.0001", "0.0001"},
        {"-0.00010", "-0.0001"}, {"9999", "9999"}, {"10000", "10000"},
        {"12345678901234567890.0000000001", "12345678901234567890.0000000001"},
        {"-9007199254740993", "-9007199254740993"},
        {"0.000000000000000000000000000001", "0.000000000000000000000000000001"},
    };
    for (size_t i = 0; i < sizeof v / sizeof *v; i++)
        dod_roundtrip(v[i].in, v[i].minimal);

    /* order equivalence: byte order of keys == dec_cmp order */
    static const char *pool[] = {"-100","-10.5","-1","-0.5","-0.0001","0","0.0001",
        "0.5","1","1.00001","9.9999","10","10.0001","99999999","100000000"};
    for (size_t a = 0; a < sizeof pool / sizeof *pool; a++)
      for (size_t b = 0; b < sizeof pool / sizeof *pool; b++) {
        uint8_t x[600], y[600]; size_t xn, yn;
        ASSERT_EQ_INT(dod_enc(pool[a], x, sizeof x, &xn), 0, "enc a");
        ASSERT_EQ_INT(dod_enc(pool[b], y, sizeof y, &yn), 0, "enc b");
        dec_t da, db; dec_init(&da); dec_init(&db);
        dec_parse(pool[a], strlen(pool[a]), &da, NULL, 0);
        dec_parse(pool[b], strlen(pool[b]), &db, NULL, 0);
        int want = dec_cmp(&da, &db);
        size_t m = xn < yn ? xn : yn;
        int got = memcmp(x, y, m); if (!got) got = xn < yn ? -1 : xn > yn;
        ASSERT_TRUE((want < 0) == (got < 0) && (want > 0) == (got > 0),
                    "key byte order equals numeric order");
        dec_free(&da); dec_free(&db);
      }

    /* malformed payloads are rejected, output stays canonical zero */
    uint8_t good[600]; size_t gn = 0;
    ASSERT_EQ_INT(dod_enc("12.5", good, sizeof good, &gn), 0, "base");
    struct { const char *why; size_t len; int byte; uint8_t val; } bad[] = {
        {"empty", 0, -1, 0}, {"short", 3, -1, 0}, {"odd length", gn - 1, -1, 0},
        {"bad tag", gn, 0, 7}, {"zero tag with body", gn, 0, 1},
        {"nonzero terminator", gn, (int)gn - 1, 9}, {"limb 0 sentinel", gn, 5, 0},
    };
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++) {
        uint8_t t[600]; memcpy(t, good, gn);
        if (bad[i].byte >= 0) t[bad[i].byte] = bad[i].val;
        if (!strcmp(bad[i].why, "limb 0 sentinel")) { t[5] = 0; t[6] = 0; }
        dec_t out; dec_init(&out); errno = 0;
        ASSERT_EQ_INT(decimal_order_decode(t, bad[i].len, &out), -1, bad[i].why);
        ASSERT_TRUE(errno == EILSEQ && out.sign == DEC_SIGN_ZERO && !out.ndigits,
                    "rejected with EILSEQ and zero result");
        dec_free(&out);
    }
    ASSERT_EQ_INT(decimal_order_decode(good, gn, NULL), -1, "null out rejected");
    return 0;
}
TEST_REGISTER("test-decimal-order-decode", dod_run)
```

**New `src/test/cases/test_agg_exact_core.c` (complete file; registered in Task 1/2):**

```c
/* Exact aggregate states against the independent oracle: random + literal
   vectors under every partition/merge order, overflow, non-finite, budget,
   allocation faults, comparison, rendering and ordered-key decoding. */
#include "test_runner.h"
#include "test_assert.h"
#include "../db/agg_exact.h"
#include "../db/decimal_order.h"
#include "agg_exact_vectors.h"
#include <errno.h>
#include <float.h>
#include <stdlib.h>
#include <string.h>

static agg_i128 ae_raw(const char *t) {           /* "-12.50" -> -1250 */
    agg_i128 v = 0; int neg = 0;
    for (const char *p = t; *p; p++) {
        if (*p == '-') neg = 1;
        else if (*p != '.') v = v * 10 + (*p - '0');
    }
    return neg ? -v : v;
}

static void ae_render(const AggNum *n, const AggNumSpec *sp, enum AggFn fn,
                      int64_t cnt, char *out, size_t cap) {
    AggValue v;
    ASSERT_EQ_INT(agg_num_value(n, sp, fn, cnt, &v), 0, "value");
    char *t = agg_value_render(&v);
    ASSERT_NOT_NULL(t, "render");
    snprintf(out, cap, "%s", t ? t : "");
    free(t);
    agg_value_release(&v);
}

static int ae_vectors(void) {
    size_t nv = sizeof AGG_VECS / sizeof *AGG_VECS;
    static const int parts_list[] = { 1, 2, 3, 5 };
    for (size_t vi = 0; vi < nv; vi++) {
        const typeof(AGG_VECS[0]) *V = &AGG_VECS[vi];
        AggNumSpec sp = { .cls = V->cls == 'I' ? AGC_INT : AGC_DEC,
                          .scale = V->cls == 'I' ? V->scale : 0 };
        for (size_t pi = 0; pi < sizeof parts_list / sizeof *parts_list; pi++) {
            int parts = parts_list[pi];
            for (int reverse = 0; reverse < 2; reverse++) {
                _Atomic size_t bytes = 0;
                AggEnv env; agg_env_init(&env, &bytes, (size_t)1 << 30);
                AggNum num[5]; int64_t cnt[5];
                memset(num, 0, sizeof num); memset(cnt, 0, sizeof cnt);
                for (int i = 0; i < V->n; i++) {
                    int p = i % parts;
                    if (V->cls == 'I') {
                        ASSERT_EQ_INT(agg_num_offer_int(&num[p], ae_raw(V->v[i])), 0, "offer int");
                    } else {
                        dec_t d; dec_init(&d);
                        ASSERT_EQ_INT(dec_parse(V->v[i], strlen(V->v[i]), &d, NULL, 0), DEC_OK, "parse");
                        ASSERT_EQ_INT(agg_num_offer_decimal(&num[p], &env, &d), 0, "offer dec");
                        dec_free(&d);
                    }
                    cnt[p]++;
                }
                AggNum total; int64_t tc = 0; memset(&total, 0, sizeof total);
                for (int k = 0; k < parts; k++) {
                    int p = reverse ? parts - 1 - k : k;
                    ASSERT_EQ_INT(agg_num_merge(&total, &num[p], sp.cls, &env), 0, "merge");
                    tc += cnt[p];
                    agg_num_release(&num[p], sp.cls, &env);
                }
                char got[512];
                ae_render(&total, &sp, AGG_SUM, tc, got, sizeof got);
                ASSERT_EQ_STR(got, V->sum, "sum");
                ae_render(&total, &sp, AGG_AVG, tc, got, sizeof got);
                ASSERT_EQ_STR(got, V->avg, "avg");
                ae_render(&total, &sp, AGG_MIN, tc, got, sizeof got);
                ASSERT_EQ_STR(got, V->mn, "min");
                ae_render(&total, &sp, AGG_MAX, tc, got, sizeof got);
                ASSERT_EQ_STR(got, V->mx, "max");
                agg_num_release(&total, sp.cls, &env);
                ASSERT_EQ_INT((long)atomic_load(&bytes), 0, "budget returns to zero (no leak)");
                agg_env_free(&env);
            }
        }
    }
    return 0;
}

static int ae_limits(void) {
    AggNum n; memset(&n, 0, sizeof n);
    agg_i128 big = ((agg_i128)1 << 126);
    ASSERT_EQ_INT(agg_num_offer_int(&n, big), 0, "2^126 fits");
    errno = 0;
    ASSERT_EQ_INT(agg_num_offer_int(&n, big), -1, "overflow is an error");
    ASSERT_EQ_INT(errno, ERANGE, "ERANGE");

    AggNum f; memset(&f, 0, sizeof f);
    ASSERT_EQ_INT(agg_num_offer_double(&f, DBL_MAX), 0, "finite");
    ASSERT_EQ_INT(agg_num_offer_double(&f, DBL_MAX), 0, "finite input: defer unused sum overflow");
    AggNumSpec fsp = { .cls = AGC_FLT, .type = FT_DOUBLE };
    AggValue fv;
    ASSERT_EQ_INT(agg_num_value(&f, &fsp, AGG_MIN, 2, &fv), 0, "min survives sum overflow");
    ASSERT_TRUE(fv.f == DBL_MAX, "exact finite min"); agg_value_release(&fv);
    ASSERT_EQ_INT(agg_num_value(&f, &fsp, AGG_MAX, 2, &fv), 0, "max survives sum overflow");
    ASSERT_TRUE(fv.f == DBL_MAX, "exact finite max"); agg_value_release(&fv);
    errno = 0;
    ASSERT_EQ_INT(agg_num_value(&f, &fsp, AGG_SUM, 2, &fv), -1, "requested sum rejects overflow");
    ASSERT_EQ_INT(errno, ERANGE, "ERANGE sum"); agg_value_release(&fv);
    errno = 0;
    ASSERT_EQ_INT(agg_num_value(&f, &fsp, AGG_AVG, 2, &fv), -1, "requested avg rejects overflow");
    ASSERT_EQ_INT(errno, ERANGE, "ERANGE avg"); agg_value_release(&fv);
    AggNum fa = {0}, fb = {0};
    ASSERT_EQ_INT(agg_num_offer_double(&fa, DBL_MAX), 0, "merge left");
    ASSERT_EQ_INT(agg_num_offer_double(&fb, DBL_MAX), 0, "merge right");
    ASSERT_EQ_INT(agg_num_merge(&fa, &fb, AGC_FLT, NULL), 0, "merge defers sum overflow");
    ASSERT_EQ_INT(agg_num_value(&fa, &fsp, AGG_MIN, 2, &fv), 0, "merged min survives");
    ASSERT_TRUE(fv.f == DBL_MAX, "merged min"); agg_value_release(&fv);
    errno = 0;
    ASSERT_EQ_INT(agg_num_value(&fa, &fsp, AGG_SUM, 2, &fv), -1, "merged requested sum fails");
    ASSERT_EQ_INT(errno, ERANGE, "merged ERANGE"); agg_value_release(&fv);

    /* budget rejection leaves the counter untouched and the state valid */
    _Atomic size_t bytes = 0;
    AggEnv env; agg_env_init(&env, &bytes, 8);
    AggNum d; memset(&d, 0, sizeof d);
    dec_t v; dec_init(&v);
    ASSERT_EQ_INT(dec_parse("123456789012345678901234567890", 30, &v, NULL, 0), DEC_OK, "parse");
    errno = 0;
    ASSERT_EQ_INT(agg_num_offer_decimal(&d, &env, &v), -1, "budget rejects");
    ASSERT_EQ_INT(errno, ENOBUFS, "ENOBUFS");
    ASSERT_EQ_INT((long)atomic_load(&bytes), 0, "no charge leaked");
    agg_num_release(&d, AGC_DEC, &env);
    agg_env_free(&env);

    /* allocation-fault injection: Nth guarded allocation fails, state intact */
    agg_env_init(&env, NULL, 0);
    memset(&d, 0, sizeof d);
    agg_exact_test_set_fail_alloc(1);
    errno = 0;
    ASSERT_EQ_INT(agg_num_offer_decimal(&d, &env, &v), -1, "alloc fault");
    ASSERT_EQ_INT(errno, ENOMEM, "ENOMEM");
    agg_exact_test_set_fail_alloc(0);
    ASSERT_EQ_INT(agg_num_offer_decimal(&d, &env, &v), 0, "recovers after fault");
    agg_num_release(&d, AGC_DEC, &env);
    agg_env_free(&env);
    dec_free(&v);
    return 0;
}

static int ae_compare_render(void) {
    /* exact comparison to numeric literals; scale-insensitive for decimals */
    AggValue a; agg_value_init(&a);
    a.cls = AGC_INT; a.present = 1; a.i = 1250; a.scale = 2;          /* 12.50 */
    int c = 9;
    ASSERT_EQ_INT(agg_value_cmp_text(&a, "12.5", &c), 0, "cmp ok");
    ASSERT_EQ_INT(c, 0, "12.50 == 12.5");
    ASSERT_EQ_INT(agg_value_cmp_text(&a, "12.51", &c), 0, "cmp ok");
    ASSERT_EQ_INT(c, -1, "12.50 < 12.51");
    errno = 0;
    ASSERT_EQ_INT(agg_value_cmp_text(&a, "abc", &c), -1, "non-number rejected");
    ASSERT_EQ_INT(errno, EINVAL, "EINVAL");
    /* adjacent values above 2^53 stay distinct */
    agg_value_init(&a);
    a.cls = AGC_INT; a.present = 1; a.i = 9007199254740993LL;
    ASSERT_EQ_INT(agg_value_cmp_text(&a, "9007199254740992", &c), 0, "cmp ok");
    ASSERT_EQ_INT(c, 1, "…993 > …992 exactly");
    /* shortest round-trip floats */
    char *t = agg_render_double(0.1);
    ASSERT_EQ_STR(t, "0.1", "0.1"); free(t);
    t = agg_render_double(0.30000000000000004);
    ASSERT_EQ_STR(t, "0.30000000000000004", "17 digits when needed"); free(t);
    errno = 0;
    ASSERT_TRUE(agg_render_double(1.0 / 0.0) == NULL && errno == ERANGE, "inf is an error");
    return 0;
}

static int ae_keys_labels(void) {
    /* ordered decimal key: tier 0 + decimal_order payload */
    AggNumSpec bounded = { .cls = AGC_DEC, .dscale = 4, .type = FT_NUMERIC };
    AggNumSpec bare = { .cls = AGC_DEC, .bare = 1, .type = FT_NUMERIC };
    uint8_t key[600]; size_t pl = 0;
    dec_t d; dec_init(&d);
    ASSERT_EQ_INT(dec_parse("2", 1, &d, NULL, 0), DEC_OK, "parse");
    ASSERT_EQ_INT(decimal_order_into(&d, key + 1, sizeof key - 1, &pl), 0, "encode");
    key[0] = 0x00;
    AggEnv env; agg_env_init(&env, NULL, 0);
    AggNum n; memset(&n, 0, sizeof n);
    errno = 0;
    ASSERT_EQ_INT(agg_num_offer_key(&bare, key, pl + 1, &n, &env), -1, "bare key value use is gated");
    ASSERT_EQ_INT(errno, ENOTSUP, "ENOTSUP");
    ASSERT_EQ_INT(agg_num_offer_key(&bounded, key, pl + 1, &n, &env), 1, "bounded decodes");
    char got[64];
    ae_render(&n, &bounded, AGG_MAX, 1, got, sizeof got);
    ASSERT_EQ_STR(got, "2.0000", "bounded key renders at the declared scale");
    key[0] = 0x01;
    ASSERT_EQ_INT(agg_num_offer_key(&bounded, key, pl + 1, &n, &env), 0, "NULL tier skipped");
    agg_num_release(&n, AGC_DEC, &env);

    char label[64];
    int len = agg_dec_label(&bounded, key + 1, pl, 1, label, sizeof label, &env);
    ASSERT_EQ_INT(len, 6, "length"); ASSERT_EQ_STR(label, "2.0000", "bounded label");
    len = agg_dec_label(&bare, key + 1, pl, 1, label, sizeof label, &env);
    ASSERT_EQ_INT(len, 1, "length"); ASSERT_EQ_STR(label, "2", "bare label is canonical");
    ASSERT_EQ_INT((int)strlen(label), len, "returned length equals strlen");
    errno = 0;
    ASSERT_EQ_INT(agg_dec_label(&bounded, key + 1, pl, 1, label, 3, &env), -1, "small buffer");
    ASSERT_EQ_INT(errno, ENAMETOOLONG, "ENAMETOOLONG");
    agg_env_free(&env);
    dec_free(&d);
    return 0;
}

static int ae_run(void) {
    return ae_vectors() | ae_limits() | ae_compare_render() | ae_keys_labels();
}
TEST_REGISTER("test-agg-exact-core", ae_run)
```

**New `src/test/cases/agg_exact_vectors.h` (committed; generated once by the oracle script in §7.2 and reviewed against §7.3's literals — the committed copy below was generated with seed 20261030 and passes the core test):**

```c
/* GENERATED by gen_agg_vectors.py (seed 20261030) from the independent
   Python-decimal oracle. Do not edit by hand. */
static const struct { char cls; int scale, n; const char *v[12];
                      const char *sum, *avg, *mn, *mx; } AGG_VECS[] = {
    {'I', 0, 2, {"9007199254740993", "9007199254740993"}, "18014398509481986", "9007199254740993.0000", "9007199254740993", "9007199254740993"},
    {'I', 0, 5, {"9223372036854775807", "9223372036854775807", "-9223372036854775807", "-9223372036854775807", "1"}, "1", "0.20000000000000000000", "-9223372036854775807", "9223372036854775807"},
    {'I', 0, 3, {"1490833334666", "1490833334667", "1490833334667"}, "4472500004000", "1490833334666.6667", "1490833334666", "1490833334667"},
    {'I', 4, 2, {"0.1000", "0.2000"}, "0.3000", "0.15000000000000000000", "0.1000", "0.2000"},
    {'I', 4, 2, {"1.5000", "-1.5000"}, "0.0000", "0.00000000000000000000", "-1.5000", "1.5000"},
    {'I', 0, 4, {"-0", "0", "-1", "1"}, "0", "0.00000000000000000000", "-1", "1"},
    {'D', 0, 3, {"1.0", "2.00", "0.5"}, "3.50", "1.16666666666666666667", "0.5", "2.00"},
    {'D', 0, 3, {"1", "1.00", "1.0"}, "3.00", "1.00000000000000000000", "1.00", "1.00"},
    {'D', 4, 3, {"0.0000", "1.0000", "2.0000"}, "3.0000", "1.00000000000000000000", "0.0000", "2.0000"},
    {'D', 0, 3, {"12345678901234567890.1234567890", "0.0000000001", "-0.0000000001"}, "12345678901234567890.1234567890", "4115226300411522630.0411522630", "-0.0000000001", "12345678901234567890.1234567890"},
    {'D', 0, 2, {"0.0000001", "0.0000002"}, "0.0000003", "0.000000150000000000000000", "0.0000001", "0.0000002"},
    {'D', 0, 3, {"9007199254740993.000000000000000001", "9007199254740993.000000000000000001", "9007199254740993.000000000000000001"}, "27021597764222979.000000000000000003", "9007199254740993.000000000000000001", "9007199254740993.000000000000000001", "9007199254740993.000000000000000001"},
    {'D', 0, 3, {"99999999999999999999999999999999999999.9999", "0.0001", "-99999999999999999999999999999999999999.9999"}, "0.0001", "0.000033333333333333333333", "-99999999999999999999999999999999999999.9999", "99999999999999999999999999999999999999.9999"},
    {'I', 0, 9, {"174908", "312101021810872283", "-6946243309511538064", "-134864", "-663624", "7767943978366928238", "131786", "-3000983528416298378", "2090382826100693998"}, "223200988350166283", "24800109816685143", "-6946243309511538064", "7767943978366928238"},
    {'I', 4, 11, {"900719925474.0990", "-60.7009", "51.9062", "52.1165", "-78.8823", "-6244001584156.3110", "23.5039", "900719925474.0994", "900719925474.0994", "-121733811040940.1322", "-342338890932200.6713"}, "-467614543780886.8733", "-42510413070989.7158", "-342338890932200.6713", "900719925474.0994"},
    {'I', 2, 10, {"85652738855030448.29", "7090925615805285.73", "-2597.95", "90071992547409.89", "62048007381769421.16", "-399.68", "90071992547409.91", "39815824833642191.43", "74856331389651147.18", "90071992547409.92"}, "269734044053537725.88", "26973404405353772.59", "-2597.95", "85652738855030448.29"},
    {'I', 0, 3, {"-3884324665016144922", "4714242134778204083", "9007199254740992"}, "838924669016800153", "279641556338933384", "-3884324665016144922", "4714242134778204083"},
    {'I', 2, 5, {"9930.55", "90071992547409.89", "90071992547409.94", "9724.02", "-3256.11"}, "180143985111218.29", "36028797022243.6580", "-3256.11", "90071992547409.94"},
    {'I', 4, 1, {"77368602010227.6932"}, "77368602010227.6932", "77368602010227.6932", "77368602010227.6932", "77368602010227.6932"},
    {'I', 0, 4, {"-485305", "9007199254740994", "881993440713587944", "-20696"}, "891000639967822937", "222750159991955734", "-485305", "881993440713587944"},
    {'I', 0, 10, {"6827400223469626646", "9007199254740989", "-894147", "9007199254740994", "6419508280697386139", "5912581114413658932", "-323034815240757859", "-2498649835387919161", "9007199254740990", "9007199254740993"}, "16373833764970064516", "1637383376497006452", "-2498649835387919161", "6827400223469626646"},
    {'I', 0, 6, {"3878860337320467715", "762030", "-5555313685704206847", "-8327791778435805578", "3346934365187690323", "4288136152210621878"}, "-2369174609420470479", "-394862434903411747", "-8327791778435805578", "4288136152210621878"},
    {'I', 0, 12, {"9007199254740991", "5960214767356022365", "9007199254740992", "9007199254740991", "-764562", "693337", "9007199254740991", "2755287056164595504", "-6283234494048740062", "9007199254740991", "9007199254740989", "9007199254740990"}, "2495317724254993517", "207943143687916126", "-6283234494048740062", "5960214767356022365"},
    {'I', 2, 1, {"49772284506986873.72"}, "49772284506986873.72", "49772284506986873.72", "49772284506986873.72", "49772284506986873.72"},
    {'I', 0, 1, {"-952603"}, "-952603", "-952603.000000000000", "-952603", "-952603"},
    {'I', 0, 8, {"622047", "9007199254740990", "9007199254740990", "9007199254740995", "-4870404004948488999", "-3257233498348983601", "36402", "7573586751984108577"}, "-527029153548482599", "-65878644193560325", "-4870404004948488999", "7573586751984108577"},
    {'I', 0, 2, {"-2720667764005636967", "9007199254740990"}, "-2711660564750895977", "-1355830282375447989", "-2720667764005636967", "9007199254740990"},
    {'I', 4, 5, {"213893042986438.4257", "195684355762811.6673", "900719925474.0993", "38.3906", "-724106397533879.3681"}, "-313628278859116.7852", "-62725655771823.3570", "-724106397533879.3681", "213893042986438.4257"},
    {'I', 0, 6, {"5799851322741189843", "944516", "16306", "111977", "-3186201447922382221", "9007199254740993"}, "2622657074074621414", "437109512345770236", "-3186201447922382221", "5799851322741189843"},
    {'I', 0, 11, {"9007199254740989", "9007199254740995", "9007199254740990", "8591953587288234056", "-4108064380564542099", "9007199254740990", "9007199254740991", "-532306", "-8529235620022654524", "-3903645528690385647", "9007199254740993"}, "-7894948746461434572", "-717722613314675870", "-8529235620022654524", "8591953587288234056"},
    {'I', 4, 10, {"900719925474.0992", "-305159105742218.8563", "-378844509182153.1608", "617029728963442.9994", "900719925474.0989", "-490992605516379.6877", "693411128503089.2931", "42.8442", "-70.6226", "900719925474.0992"}, "138146796802175.1066", "13814679680217.5107", "-490992605516379.6877", "693411128503089.2931"},
    {'I', 0, 7, {"9007199254740994", "9007199254740992", "9007199254740995", "-780284", "8821405228509539190", "7480809853127918673", "200203"}, "16329236679401100763", "2332748097057300109", "-780284", "8821405228509539190"},
    {'I', 2, 3, {"-3453.73", "8855.13", "73017564082980420.69"}, "73017564082985822.09", "24339188027661940.70", "-3453.73", "73017564082980420.69"},
    {'I', 0, 9, {"9007199254740992", "72038040909342395", "-6010699109600930663", "9007199254740991", "9007199254740994", "-495035", "-6580113988886420953", "9007199254740991", "-855544"}, "-12482746260560395832", "-1386971806728932870", "-6580113988886420953", "72038040909342395"},
    {'I', 4, 10, {"63.2353", "-68.8503", "-91.3592", "-167209052546758.6132", "-48.3571", "900719925474.0991", "900719925474.0990", "597183229737494.5728", "19.4575", "59.5345"}, "431775617041617.8184", "43177561704161.7818", "-167209052546758.6132", "597183229737494.5728"},
    {'I', 0, 7, {"80583", "-617327", "2753158913615439359", "8144091516582582260", "9007199254740994", "9007199254740995", "9007199254740994"}, "10924272027961707858", "1560610289708815408", "-617327", "8144091516582582260"},
    {'I', 2, 2, {"38699357545388757.82", "53867210732212861.04"}, "92566568277601618.86", "46283284138800809.43", "38699357545388757.82", "53867210732212861.04"},
    {'I', 0, 8, {"-472776", "-5068139004848441907", "9007199254740994", "131391", "-242388", "9007199254740992", "-248802", "-685566"}, "-5050124606340478062", "-631265575792559758", "-5068139004848441907", "9007199254740994"},
    {'I', 0, 4, {"9007199254740992", "9007199254740993", "9007199254740992", "-2206910433643924143"}, "-2179888835879701166", "-544972208969925292", "-2206910433643924143", "9007199254740993"},
    {'I', 4, 4, {"900719925474.0994", "-35.0236", "900719925474.0989", "2.3617"}, "1801439850915.5364", "450359962728.88410000", "-35.0236", "900719925474.0994"},
    {'I', 0, 11, {"9007199254740993", "9007199254740994", "9007199254740992", "7134317722755381500", "-6329324759262842796", "5024490211309595046", "5393982618380472030", "9007199254740990", "1022716563222498247", "9007199254740995", "3606639540564213090"}, "15897857893243022081", "1445259808476638371", "-6329324759262842796", "7134317722755381500"},
    {'I', 0, 9, {"714549", "9007199254740995", "-1655669675659660560", "-92920", "485245", "55587", "-4394711824295269699", "9007199254740989", "-2478006347681808667"}, "-8510373449126094481", "-945597049902899387", "-4394711824295269699", "9007199254740995"},
    {'I', 4, 12, {"900719925474.0994", "-67.0478", "900719925474.0992", "451214563803072.4567", "111739641363149.5975", "611983018584800.9031", "900719925474.0991", "547167852243461.8488", "67.6280", "203343394339580.7563", "879936820358186.1907", "99.5141"}, "2808087450468774.1451", "234007287539064.5121", "-67.0478", "879936820358186.1907"},
    {'I', 4, 7, {"-45.3369", "-3.5215", "740075353389324.2956", "900719925474.0993", "11.6042", "-62.9761", "-55.1946"}, "740976073314642.9700", "105853724759234.7100", "-62.9761", "740075353389324.2956"},
    {'I', 2, 11, {"66963639295554139.44", "-5909.75", "90071992547409.89", "90071992547409.95", "90071992547409.89", "7369.65", "90071992547409.92", "-45175055767793403.46", "90071992547409.95", "90071992547409.89", "90071992547409.93"}, "22419087475594065.30", "2038098861417642.3000", "-45175055767793403.46", "66963639295554139.44"},
    {'I', 0, 9, {"-831257", "-668092", "-246785", "562961", "9007199254740995", "-279563", "9007199254740990", "9007199254740989", "-8109154581219926043"}, "-8082132983457165805", "-898014775939685089", "-8109154581219926043", "9007199254740995"},
    {'I', 0, 6, {"9007199254740994", "9007199254740992", "8428580581377777928", "9007199254740994", "9007199254740994", "-722968"}, "8464609378396018934", "1410768229732669822", "-722968", "8428580581377777928"},
    {'I', 0, 8, {"703916", "1184815192875048773", "-46643", "4739355277456953345", "-165960309278472509", "-7817196882391149429", "121786", "8914370179571248394"}, "6855383458234407633", "856922932279300954", "-7817196882391149429", "8914370179571248394"},
    {'I', 2, 7, {"-9080.46", "90071992547409.91", "8240394947212729.66", "-52146581055238404.54", "-38203081408176520.49", "90071992547409.95", "74698862967331645.62"}, "-7230260563784810.35", "-1032894366254972.9071", "-52146581055238404.54", "74698862967331645.62"},
    {'I', 0, 6, {"9007199254740990", "-665979", "9007199254740993", "-8828826226102396861", "9007199254740994", "9007199254740990"}, "-8792797429084098873", "-1465466238180683146", "-8828826226102396861", "9007199254740994"},
    {'I', 0, 9, {"9007199254740995", "240575", "224211", "2599554178284931721", "9007199254740993", "-784077", "690270", "6165445424332384210", "2332003883671134481"}, "11115017884798303379", "1235001987199811487", "-784077", "6165445424332384210"},
    {'I', 2, 3, {"90071992547409.89", "5817.18", "90071992547409.89"}, "180143985100636.96", "60047995033545.6533", "5817.18", "90071992547409.89"},
    {'I', 0, 2, {"452027", "9007199254740992"}, "9007199255193019", "4503599627596509.5000", "452027", "9007199254740992"},
    {'I', 4, 4, {"900719925474.0995", "-659017532402572.4226", "-84.3096", "-37.0557"}, "-658116812477219.6884", "-164529203119304.9221", "-659017532402572.4226", "900719925474.0995"},
    {'I', 2, 4, {"-68037141869411045.90", "49062009040078087.42", "90071992547409.92", "90071992547409.89"}, "-18794988844238138.67", "-4698747211059534.6675", "-68037141869411045.90", "49062009040078087.42"},
    {'I', 2, 3, {"10444065630900909.23", "7456.15", "90071992547409.92"}, "10534137623455775.30", "3511379207818591.7667", "7456.15", "10444065630900909.23"},
    {'I', 0, 6, {"9007199254740990", "-126846", "930226426326989224", "9007199254740995", "380714", "1753434243029889434"}, "2701675067866614511", "450279177977769085", "-126846", "1753434243029889434"},
    {'I', 2, 5, {"90071992547409.89", "90071992547409.92", "2187.35", "-882.80", "90071992547409.92"}, "270215977643534.28", "54043195528706.8560", "-882.80", "90071992547409.92"},
    {'I', 4, 4, {"-639741789005216.7996", "-83.8074", "900719925474.0990", "-781574399994481.8710"}, "-1420415469074308.3790", "-355103867268577.0948", "-781574399994481.8710", "900719925474.0990"},
    {'I', 2, 8, {"90071992547409.95", "-7526.97", "-4649.81", "-7850.41", "-5007.65", "-77273359104886480.21", "90071992547409.92", "90071992547409.95"}, "-77003143127269285.23", "-9625392890908660.6538", "-77273359104886480.21", "90071992547409.95"},
    {'I', 0, 7, {"526828", "521190", "-3866375324733326531", "9007199254740991", "6738328472031723600", "9007199254740993", "-882015"}, "2889967545808045056", "412852506544006437", "-3866375324733326531", "6738328472031723600"},
    {'I', 4, 7, {"900719925474.0992", "94.2829", "624826398723558.3399", "-174992559451035.4951", "28.3342", "78.3374", "900719925474.0991"}, "451635279123671.9976", "64519325589095.9997", "-174992559451035.4951", "624826398723558.3399"},
    {'I', 2, 1, {"90071992547409.90"}, "90071992547409.90", "90071992547409.9000", "90071992547409.90", "90071992547409.90"},
    {'I', 0, 12, {"-3005994356805430800", "9007199254740992", "-346684", "9007199254740992", "9007199254740995", "9007199254740989", "-190131", "-600360", "9007199254740989", "9007199254740994", "9007199254740989", "-372783"}, "-2942943962023753818", "-245245330168646152", "-3005994356805430800", "9007199254740995"},
    {'I', 0, 9, {"-7118635964680551001", "1989357152921263084", "9007199254740991", "9007199254740991", "530591", "-3571971931126402871", "2202611504708102405", "-512140", "-451984"}, "-6480624839668539934", "-720069426629837770", "-7118635964680551001", "2202611504708102405"},
    {'I', 2, 3, {"64705943780569104.23", "90071992547409.90", "4924.35"}, "64796015773121438.48", "21598671924373812.83", "4924.35", "64705943780569104.23"},
    {'I', 0, 1, {"-422733"}, "-422733", "-422733.000000000000", "-422733", "-422733"},
    {'I', 2, 8, {"90071992547409.95", "-3462.71", "-34586128658592868.21", "82746764342385458.30", "2167.03", "90071992547409.89", "90071992547409.89", "2913640981162583.53"}, "51344492642596107.67", "6418061580324513.4588", "-34586128658592868.21", "82746764342385458.30"},
    {'I', 0, 11, {"9081608387296939734", "350164", "9007199254740995", "-7581581078885621770", "904381", "-6264783674671804953", "1276819211050728271", "-699184", "9007199254740990", "6941921714906542128", "3899746657163682891"}, "7371745615370503647", "670158692306409422", "-7581581078885621770", "9081608387296939734"},
    {'I', 0, 8, {"-2983373455302789306", "2058841948907499747", "7162305925892765762", "-7077537126322526397", "9007199254740994", "9007199254740989", "3413075030451023375", "-7427229868355983535"}, "-4835903146220528371", "-604487893277566046", "-7427229868355983535", "7162305925892765762"},
    {'I', 0, 10, {"-2422228541041578083", "451220844982839912", "9007199254740989", "-921430", "57044", "9007199254740993", "9007199254740991", "7545890624506551825", "789304", "9007199254740993"}, "5610911725466702538", "561091172546670254", "-2422228541041578083", "7545890624506551825"},
    {'I', 0, 9, {"7741029760062895232", "-5883743092675624660", "-1697988611523039090", "814207921302291549", "5230476696323012660", "-385624946322071878", "4150435099845712976", "564398", "-991405"}, "9968792827012749782", "1107643647445861087", "-5883743092675624660", "7741029760062895232"},
    {'I', 0, 12, {"9007199254740992", "-3389622305730134594", "2513640008722255781", "9007199254740993", "451399", "9007199254740991", "9007199254740993", "959401", "-7913387356816284284", "69555", "9007199254740992", "-803673"}, "-8744333657549781454", "-728694471462481788", "-7913387356816284284", "2513640008722255781"},
    {'I', 4, 5, {"788957470921653.6533", "-253148193686156.1850", "-70.3775", "40.4793", "900719925474.0990"}, "536709997160941.6691", "107341999432188.3338", "-253148193686156.1850", "788957470921653.6533"},
    {'I', 0, 3, {"-313515", "8237700668369825360", "9007199254740994"}, "8246707867624252839", "2748902622541417613", "-313515", "8237700668369825360"},
    {'I', 0, 7, {"-409901", "-26003", "286956", "9007199254740990", "9007199254740995", "9007199254740990", "461851"}, "27021597764535878", "3860228252076554.0000", "-409901", "9007199254740995"},
    {'I', 0, 5, {"-843843", "9007199254740990", "133341", "-54699", "111500445365724090"}, "120507644619699879", "24101528923939976", "-843843", "111500445365724090"},
    {'I', 2, 1, {"90071992547409.95"}, "90071992547409.95", "90071992547409.9500", "90071992547409.95", "90071992547409.95"},
    {'I', 0, 3, {"9007199254740989", "-7904837032054266206", "9007199254740992"}, "-7886822633544784225", "-2628940877848261408", "-7904837032054266206", "9007199254740992"},
    {'I', 0, 6, {"9007199254740989", "975652", "9007199254740991", "9007199254740995", "9007199254740991", "351576"}, "36028797020291194", "6004799503381865.6667", "351576", "9007199254740995"},
    {'I', 0, 2, {"4669071837086503407", "2244544318605307903"}, "6913616155691811310", "3456808077845905655", "2244544318605307903", "4669071837086503407"},
    {'I', 2, 10, {"-51133653092657878.19", "90071992547409.93", "-7955.71", "37223221651791067.61", "-9329.94", "2967.77", "90071992547409.94", "5312.79", "7645.83", "-42272540855637130.26"}, "-56002828311410480.23", "-5600282831141048.0230", "-51133653092657878.19", "37223221651791067.61"},
    {'I', 0, 12, {"9007199254740994", "-603666", "-762433", "9007199254740989", "9007199254740992", "-7693773645459906933", "-4191067054784186629", "9007199254740989", "-164670", "9007199254740989", "364564", "5648235550594565111"}, "-6191569153376989703", "-515964096114749142", "-7693773645459906933", "5648235550594565111"},
    {'I', 4, 10, {"900719925474.0989", "870998439544858.8548", "900719925474.0989", "900719925474.0992", "31.6217", "44.7009", "900719925474.0993", "900719925474.0989", "-63.0503", "643651547155168.0344"}, "1519153586327410.6567", "151915358632741.0657", "-63.0503", "870998439544858.8548"},
    {'I', 0, 11, {"9007199254740989", "-2745216400793759837", "5803630521848405526", "9007199254740991", "416852", "9007199254740993", "-1279145378157503115", "-285659", "-7628245713699406671", "9007199254740990", "-1202684109595078004"}, "-7015632283378246945", "-637784753034386086", "-7628245713699406671", "5803630521848405526"},
    {'I', 4, 4, {"847635656598181.5014", "900719925474.0994", "26.3802", "98.8106"}, "848536376523780.7916", "212134094130945.1979", "26.3802", "847635656598181.5014"},
    {'I', 0, 7, {"654574477362990931", "-3112372930172421625", "6376625910218179397", "-657540232844966349", "2910379169776955873", "2968922131031938393", "9007199254740991"}, "9149595724627417611", "1307085103518202516", "-3112372930172421625", "6376625910218179397"},
    {'I', 0, 11, {"-688321", "805090", "578187", "9007199254740989", "2631146637610025126", "328155", "9007199254740989", "-734035", "6529165715292013340", "9007199254740991", "-6630705269142741682"}, "2556628681523808829", "232420789229437166", "-6630705269142741682", "6529165715292013340"},
    {'I', 0, 6, {"7422091490391861980", "9007199254740992", "9007199254740989", "-6148505811701545367", "4636619561171130548", "9007199254740990"}, "5937226837625670132", "989537806270945022", "-6148505811701545367", "7422091490391861980"},
    {'I', 2, 5, {"58862850975151277.09", "-42200022633649668.93", "-75842319173853521.67", "90071992547409.95", "41890218766723232.23"}, "-17199200073081271.33", "-3439840014616254.2660", "-75842319173853521.67", "58862850975151277.09"},
    {'I', 2, 12, {"90071992547409.91", "-8945.36", "-8845.08", "90071992547409.93", "8988.69", "66681398708438548.28", "45835216823355998.84", "90071992547409.92", "25391802900685494.37", "90071992547409.89", "90071992547409.89", "90071992547409.89"}, "138448850387755699.17", "11537404198979641.60", "-8945.36", "66681398708438548.28"},
    {'I', 0, 7, {"-351760", "-1914473908988253368", "9007199254740989", "165224", "4721760575442837322", "-9028590911322428953", "-4758711313433720605"}, "-10971008359047011151", "-1567286908435287307", "-9028590911322428953", "4721760575442837322"},
    {'I', 2, 11, {"-15751599321072034.54", "-66135916821642970.55", "-30850145537237905.58", "90071992547409.89", "90071992547409.94", "90071992547409.94", "90071992547409.90", "-8763.02", "248.27", "-9541.10", "90071992547409.92"}, "-112287301717233916.93", "-10207936519748537.9027", "-66135916821642970.55", "90071992547409.94"},
    {'I', 2, 9, {"-29259327870720780.64", "45134381402620502.58", "9644.15", "-15257655660400645.58", "-58012009079451636.91", "90071992547409.93", "90071992547409.95", "-6508.37", "-1877.33"}, "-57214467222856482.22", "-6357163024761831.3578", "-58012009079451636.91", "45134381402620502.58"},
    {'I', 0, 8, {"-37578", "1930110511078198409", "9007199254740993", "-404883", "-239496", "625161", "-220236", "441325"}, "1939117710333103695", "242389713791637962", "-404883", "1930110511078198409"},
    {'I', 0, 5, {"-5729649304737024340", "-5716697749633501799", "9007199254740992", "9007199254740992", "7621777313209367126"}, "-3806555342651677029", "-761311068530335406", "-5729649304737024340", "7621777313209367126"},
    {'I', 2, 3, {"-7531.04", "7416.55", "61092424496194160.59"}, "61092424496194046.10", "20364141498731348.70", "-7531.04", "61092424496194160.59"},
    {'I', 0, 2, {"-655644", "9007199254740995"}, "9007199254085351", "4503599627042675.5000", "-655644", "9007199254740995"},
    {'I', 2, 8, {"-2775.87", "90071992547409.93", "18555270121874339.76", "-26201140071918182.06", "-14331801427019668.25", "90071992547409.90", "19381039412520626.52", "90071992547409.89"}, "-2326415986903430.18", "-290801998362928.7725", "-26201140071918182.06", "19381039412520626.52"},
    {'I', 4, 4, {"528898135525019.1705", "-691578401574641.6503", "900719925474.0993", "900719925474.0989"}, "-160878826198674.2816", "-40219706549668.5704", "-691578401574641.6503", "528898135525019.1705"},
    {'I', 2, 7, {"-76042971232476025.70", "90071992547409.93", "90071992547409.92", "-171.57", "19802654702513203.02", "-7884181942276268.96", "90071992547409.92"}, "-63854282494597033.44", "-9122040356371004.7771", "-76042971232476025.70", "19802654702513203.02"},
    {'I', 0, 3, {"9007199254740990", "9173116767048765581", "9007199254740994"}, "9191131165558247565", "3063710388519415855", "9007199254740990", "9173116767048765581"},
    {'I', 2, 2, {"90071992547409.89", "-74273866831558827.48"}, "-74183794839011417.59", "-37091897419505708.80", "-74273866831558827.48", "90071992547409.89"},
    {'I', 4, 11, {"289610602206264.2534", "-879838749658044.6257", "-6.9979", "-73484194737713.0716", "-78.4722", "900719925474.0994", "64.2932", "25.1844", "900719925474.0989", "-48.2182", "917816360789984.4715"}, "255905458451395.0152", "23264132586490.4559", "-879838749658044.6257", "917816360789984.4715"},
    {'I', 0, 11, {"-658969", "6615656377510248276", "9007199254740992", "253861", "9007199254740989", "9007199254740994", "21079", "9007199254740992", "9007199254740993", "9007199254740992", "367774"}, "6669699573038677973", "606336324821697998", "-658969", "6615656377510248276"},
    {'I', 0, 5, {"9007199254740995", "9007199254740991", "6475881875092305444", "9007199254740991", "-751901"}, "6502903472855776520", "1300580694571155304", "-751901", "6475881875092305444"},
    {'I', 2, 12, {"11819642372663415.34", "90071992547409.94", "90071992547409.93", "-5412.94", "-5374.12", "2780.09", "-839.53", "-5025.99", "90071992547409.93", "-88727878310096406.16", "90071992547409.92", "-6280.03"}, "-76547947967263503.62", "-6378995663938625.3017", "-88727878310096406.16", "11819642372663415.34"},
    {'I', 4, 10, {"900719925474.0992", "-70.7048", "-857825792853426.0092", "-74.2640", "-98.3760", "900719925474.0994", "793802344155485.2965", "900719925474.0993", "900719925474.0989", "900719925474.0993"}, "-59519849070813.5614", "-5951984907081.3561", "-857825792853426.0092", "793802344155485.2965"},
    {'I', 0, 7, {"-8005596601048999825", "-1261590372951947670", "-612051", "3487600141817147601", "167760", "-8830061776923749178", "595267"}, "-14609648609107398096", "-2087092658443914014", "-8830061776923749178", "3487600141817147601"},
    {'I', 2, 3, {"90071992547409.92", "90071992547409.91", "90071992547409.90"}, "270215977642229.73", "90071992547409.9100", "90071992547409.90", "90071992547409.92"},
    {'I', 0, 11, {"9007199254740994", "9007199254740993", "6951896952400473402", "9007199254740991", "9007199254740991", "-352733", "9007199254740993", "9007199254740990", "-75516", "-859542", "-7844465451718159821"}, "-838525303790528258", "-76229573071866205", "-7844465451718159821", "6951896952400473402"},
    {'I', 0, 12, {"5129748763577647601", "7009617490375292476", "469667", "-5603784471387156832", "25621", "9007199254740995", "1833538210626436512", "559352", "9007199254740995", "591617", "4312036132049752441", "-517834"}, "12699170523752582611", "1058264210312715218", "-5603784471387156832", "7009617490375292476"},
    {'I', 4, 1, {"-17.0510"}, "-17.0510", "-17.0510000000000000", "-17.0510", "-17.0510"},
    {'I', 0, 12, {"9007199254740989", "-113519", "9007199254740995", "9007199254740994", "-426275029398311602", "8669430603233041772", "9007199254740993", "9007199254740994", "-760390", "-5111580026837220932", "9007199254740993", "213494"}, "3185618742525294781", "265468228543774565", "-5111580026837220932", "8669430603233041772"},
    {'I', 2, 11, {"90071992547409.93", "90071992547409.94", "21331710047639642.13", "90071992547409.94", "90071992547409.89", "90071992547409.95", "-79058451441493242.63", "4291383807381144.78", "-9391.10", "90071992547409.93", "-23273622581550116.47"}, "-76168548212747503.71", "-6924413473886136.7009", "-79058451441493242.63", "21331710047639642.13"},
    {'I', 0, 1, {"-171723"}, "-171723", "-171723.000000000000", "-171723", "-171723"},
    {'I', 2, 3, {"45306203872054516.26", "-5838.44", "-6873.34"}, "45306203872041804.48", "15102067957347268.16", "-6873.34", "45306203872054516.26"},
    {'I', 0, 6, {"4098631275155861709", "967424", "312092", "4857016986598482506", "-3169924143219453494", "813076"}, "5785724118536983313", "964287353089497219", "-3169924143219453494", "4857016986598482506"},
    {'I', 2, 12, {"8395.78", "6823.96", "-14247214404861981.05", "6203.06", "91629061561178045.66", "-13107676927578580.94", "5849.61", "-7078.25", "90071992547409.89", "-53136707931677497.89", "69270338867027956.72", "9903.16"}, "80497873156665449.71", "6708156096388787.4758", "-53136707931677497.89", "91629061561178045.66"},
    {'I', 4, 3, {"232297869856363.2840", "-386758323882291.2643", "-93.0715"}, "-154460454026021.0518", "-51486818008673.6839", "-386758323882291.2643", "232297869856363.2840"},
    {'I', 0, 1, {"132197"}, "132197", "132197.000000000000", "132197", "132197"},
    {'I', 4, 7, {"77.8549", "813653462960867.7920", "52406119051734.3562", "-276754025221454.2047", "-89.5940", "900719925474.0991", "708645889198717.7563"}, "1298852165915328.0598", "185550309416475.4371", "-276754025221454.2047", "813653462960867.7920"},
    {'I', 2, 12, {"-9447.44", "90071992547409.90", "90071992547409.93", "-50101862401743961.61", "50577100394477005.85", "90071992547409.89", "75261990875985175.23", "-9037.73", "90071992547409.93", "90071992547409.94", "-3028.29", "-7651.30"}, "76187588831426104.30", "6348965735952175.3583", "-50101862401743961.61", "75261990875985175.23"},
    {'I', 0, 12, {"7091864165447443690", "-365226", "-247648", "-2224590653224986161", "9007199254740991", "-3322313648867866462", "-2483671008770711849", "9007199254740992", "9007199254740995", "-2320799233113433866", "-5690045991878586663", "-7328592670556784262"}, "-16251127443201315469", "-1354260620266776289", "-7328592670556784262", "7091864165447443690"},
    {'I', 0, 7, {"773474", "-7472513338181862815", "9007199254740991", "-4373012198485759877", "6568221602290957821", "551648114246638943", "7340783403822626287"}, "2624134782948114824", "374876397564016403", "-7472513338181862815", "7340783403822626287"},
    {'I', 2, 8, {"90071992547409.92", "90071992547409.95", "90071992547409.91", "69152096656602857.93", "90071992547409.91", "-12901020194884402.28", "9501.37", "90071992547409.92"}, "56701436424465006.63", "7087679553058125.8288", "-12901020194884402.28", "69152096656602857.93"},
    {'I', 2, 5, {"90071992547409.92", "-42697602861121600.10", "6492.79", "90071992547409.90", "7828.88"}, "-42517458876012458.61", "-8503491775202491.7220", "-42697602861121600.10", "90071992547409.92"},
    {'I', 4, 9, {"392260276975128.5956", "320635787511007.6341", "-9575302413022.6181", "900719925474.0991", "-91.3887", "900719925474.0989", "900719925474.0991", "-89.9676", "848011515002787.1060"}, "1554034436852141.6584", "172670492983571.2954", "-9575302413022.6181", "848011515002787.1060"},
    {'I', 0, 11, {"9007199254740993", "3659209737135780389", "2608456280587808597", "-5540925422182354489", "9007199254740993", "22917", "5455948274031162542", "9007199254740990", "238937", "563141", "-4561241176466010353"}, "1648469290871434657", "149860844624675878", "-5540925422182354489", "5455948274031162542"},
    {'I', 0, 12, {"911872", "7595164489049414751", "9007199254740992", "9007199254740991", "9007199254740989", "9007199254740993", "-8709073015985485332", "-644141", "-713935", "9007199254740993", "-419565", "-6424076245902423846"}, "-7492948776565655238", "-624412398047137937", "-8709073015985485332", "7595164489049414751"},
    {'I', 2, 2, {"90071992547409.93", "72270551534846911.56"}, "72360623527394321.49", "36180311763697160.75", "90071992547409.93", "72270551534846911.56"},
    {'I', 2, 6, {"-43970061950169552.29", "90071992547409.91", "90071992547409.91", "-412.70", "-62006273338337903.49", "90071992547409.90"}, "-105706119310865638.76", "-17617686551810939.79", "-62006273338337903.49", "90071992547409.91"},
    {'I', 4, 6, {"-57820727161389.8295", "677694315400567.8705", "900719925474.0994", "900719925474.0990", "689853108689877.7620", "900719925474.0990"}, "1312428856705478.1004", "218738142784246.3501", "-57820727161389.8295", "689853108689877.7620"},
    {'D', 0, 10, {"-6485949161800871726736.57401699076054650414950878", "-809271624.18284537", "51367933862523652438626776305454.76", "934028045855205836.9979709", "592606266470082556347552388742613488.605501", "-470156596046078858.68078003880145109160631", "569474.101644562676014791", "3744420023673428728723140.50209677617395", "641602418178368847.12005851863", "-4349535732795179517821717913154250971.075714044399215141154059990237"}, "-3756878098387496502641741402599541948.426084686481247945909878770237", "-375687809838749650264174140259954194.842608468648124794590987877024", "-4349535732795179517821717913154250971.075714044399215141154059990237", "592606266470082556347552388742613488.605501"},
    {'D', 4, 4, {"-6345979194785377179.3877", "-46337.3077", "1.0149", "-285276.3073"}, "-6345979194785708791.9878", "-1586494798696427197.9970", "-6345979194785377179.3877", "1.0149"},
    {'D', 0, 7, {"-22413770959545103391.3782971989175518979698", "43557787137053221472118855.168646390745098981993139912", "-397478173453227280089802803727674.498986", "-822995312549074412417467432767.14318581628754071", "94473541897780668118.67093235072880696", "6339312265289404149682191659117.823427532184118946916", "-9343273101353611643780168339050222132.8843555659"}, "-9343665063166554271533696717422039874.241818307447067719060660088", "-1334809294738079181647670959631719982.034545472492438245580094298", "-9343273101353611643780168339050222132.8843555659", "6339312265289404149682191659117.823427532184118946916"},
    {'D', 4, 3, {"-759551488838460028079455467945573.8434", "13854176339435.9491", "-4193.9365"}, "-759551488838460028065601291610331.8308", "-253183829612820009355200430536777.2769", "-759551488838460028079455467945573.8434", "13854176339435.9491"},
    {'D', 0, 9, {"-6338343.35", "-1.9830629240958", "16956789548914.601095565475079195347697", "81.600239407", "-83104179588632167723.3767392568049536605", "53929.1", "6402430834814393430016380919744481.74341473", "-221298557813434347332728425.46749157150281604", "25351185021154511471621854706.477873910467000983869277250"}, "6402455964700773666930873359967619.345329860538510478716974250", "711383996077863740770097039996402.149481095615390053190774917", "-221298557813434347332728425.46749157150281604", "6402430834814393430016380919744481.74341473"},
    {'D', 0, 10, {"897546886.4800202494504796457248116627", "22197467854264556.28437026554261904564741382", "62524628528425251411858964682.1906261", "32453607025190836744225325.62737018285070887", "392304875701295859057126065737.010", "34820392455547436530946739172.03616557144", "67290081981419117091.0551819946217228", "26176884041289144769078409428246149.324", "405542448563600228645507.0512", "21713179018.74649714681762150832232762"}, "26177373724045046823659318216994125.8054315107231518696945531027", "2617737372404504682365931821699412.5805431510723151869694553103", "897546886.4800202494504796457248116627", "26176884041289144769078409428246149.324"},
    {'D', 0, 3, {"-7506303967450325250708713614288024522.43981", "-8137345989378.906021713952010979396862939704", "-999735769925324325746487421590756.785058633035"}, "-7507303703220250575034468239055604658.130890346987010979396862939704", "-2502434567740083525011489413018534886.043630115662336993132287646568", "-7506303967450325250708713614288024522.43981", "-8137345989378.906021713952010979396862939704"},
    {'D', 4, 5, {"-108785516031291944938783287925547.6765", "-86095086062056.0275", "47160400.2344", "2351535118353998789945808891107611.4312", "7403224664351238628.9041"}, "2242749602322714248145594915519036.8657", "448549920464542849629118983103807.3731", "-108785516031291944938783287925547.6765", "2351535118353998789945808891107611.4312"},
    {'D', 0, 1, {"84340577300.1862123675748238379"}, "84340577300.1862123675748238379", "84340577300.1862123675748238379", "84340577300.1862123675748238379", "84340577300.1862123675748238379"},
    {'D', 4, 11, {"446000296419573504136898113065134351.5998", "-41481125398.2446", "1.6888", "4529967365183841.7673", "87655.9338", "-311037966714672853821003397434599138.9350", "63043119022380316297036.8716", "247387737666028399.3563", "51073697853072183290562.7901", "-35947741044201113077482.5555", "51.2806"}, "134962329704978819643643630567219881.5532", "12269302700452619967603966415201807.4139", "-311037966714672853821003397434599138.9350", "446000296419573504136898113065134351.5998"},
    {'D', 0, 3, {"-5648109447908.7156624527234740853", "389331512494486169370149407407313826.5", "-795756498241404674167959431319354965.93453322020887158075"}, "-406424985746918504797815672021489048.15019567293234566605", "-135474995248972834932605224007163016.05006522431078188868", "-795756498241404674167959431319354965.93453322020887158075", "389331512494486169370149407407313826.5"},
    {'D', 0, 1, {"-247329945495156035668827434316467477.8021766"}, "-247329945495156035668827434316467477.8021766", "-247329945495156035668827434316467477.8021766", "-247329945495156035668827434316467477.8021766", "-247329945495156035668827434316467477.8021766"},
    {'D', 0, 8, {"-3253958216317548276508.7", "95025814582.66657493822361622721794566365", "8084831694117619038784611253334911432.80466596672072969072223441154", "450019652077189.057945723406412772", "-379002985339266276174.83236682815533578597464304251", "-97995893747454442708860.539446572804373103171569159117", "4419216656.290401468062555629459366", "12736021858772983616445366390461031.83592415648055"}, "8097567715976290393546557627565219348.583698851934155430253333873563", "1012195964497036299193319703445652418.572962356491769428781666734195", "-97995893747454442708860.539446572804373103171569159117", "8084831694117619038784611253334911432.80466596672072969072223441154"},
    {'D', 0, 5, {"-9823.994578791371790", "812026.35233200898306015417820", "-731638596214609287254.55574418999136164105970089", "64680.8185216737751436378177588", "-858214144402203106161378679051152.057371168915"}, "-858214144402934744757593287471523.43684046751994784906374209", "-171642828880586948951518657494304.68736809350398956981274842", "-858214144402203106161378679051152.057371168915", "812026.35233200898306015417820"},
    {'D', 0, 10, {"-2383203.720702002729444413175562345", "-3222796", "855934529865.946", "353137301521975055089660390.302342577578600565063", "7.72151753112812945187", "-1929012298550.614209578846992773071777602", "-6931381040230277777571167174762.85622006584748121", "74.762480762968838896450962433", "2911303306489317470951636.0506654736044759659867177", "801358489155508890311474.964008773116446860621721"}, "-6931024190266960158762799625863.444116529027426656254938814", "-693102419026696015876279962586.344411652902742665625493881", "-6931381040230277777571167174762.85622006584748121", "353137301521975055089660390.302342577578600565063"},
    {'D', 0, 2, {"731096099775915968281939691.84750478", "-514813229203083.20079361904822196106389163555"}, "731096099775401155052736608.64671116095177803893610836445", "365548049887700577526368304.32335558047588901946805418223", "-514813229203083.20079361904822196106389163555", "731096099775915968281939691.84750478"},
    {'D', 4, 2, {"82278882403004004426790729933186032.3185", "7436053941234974314.3330"}, "82278882403004011862844671168160346.6515", "41139441201502005931422335584080173.3258", "7436053941234974314.3330", "82278882403004004426790729933186032.3185"},
    {'D', 0, 2, {"318388432.95416152717572401467", "7949566063362660366881671304273029256"}, "7949566063362660366881671304591417688.95416152717572401467", "3974783031681330183440835652295708844.47708076358786200734", "318388432.95416152717572401467", "7949566063362660366881671304273029256"},
    {'D', 4, 9, {"2050886276671834356603971330224674316.9769", "872217311575376498.8209", "-1239301024688131578510892589494026.5365", "24169800344522354351887299179.2362", "-55734845240112374935348.5658", "432873030909849239778101139398135269.3201", "-536.5862", "9208349859.3490", "-965107366059303618467822378043.1764"}, "2482519065619374016049274669507027168.8382", "275835451735486001783252741056336352.0931", "-1239301024688131578510892589494026.5365", "2050886276671834356603971330224674316.9769"},
    {'D', 0, 4, {"-203065.18203079", "90165913.8183105623454", "1.6361064599", "-69.308"}, "89962780.9643862322454", "22490695.2410965580614", "-203065.18203079", "90165913.8183105623454"},
    {'D', 4, 4, {"-1435206026476328548739208856802.3742", "-6268386525241195830.4987", "-7916886881471565494827253525985813091.0785", "7753842850802.0306"}, "-7916888316677591977424181036593014921.9208", "-1979222079169397994356045259148253730.4802", "-7916886881471565494827253525985813091.0785", "7753842850802.0306"},
    {'D', 4, 3, {"427042127637003020.3687", "358320069682240746924848708490264117.5682", "2658909240641284.7862"}, "358320069682240747354549745367908422.7231", "119440023227413582451516581789302807.5744", "2658909240641284.7862", "358320069682240746924848708490264117.5682"},
    {'D', 0, 2, {"18439210822396372746.318", "9809974483.5127082043449888915067474072"}, "18439210832206347229.8307082043449888915067474072", "9219605416103173614.9153541021724944457533737036", "9809974483.5127082043449888915067474072", "18439210822396372746.318"},
    {'D', 0, 6, {"-39073310126781264054641593.9930046", "7580173442490144494945747479.2820", "-2470255855541518393.665", "870703449315761", "-17192215313303718748187719.9247390890795", "-8130687938871083289.3"}, "7523907906449986421179632243.3992563109205", "1253984651074997736863272040.5665427184868", "-39073310126781264054641593.9930046", "7580173442490144494945747479.2820"},
    {'D', 0, 8, {"792269910.32931876455986071749537788239", "68800177073604203202463611647840509.1429761281", "4831725140852149354559316175.238059266", "35068582872854086898174993017.1835864036322285951857968576", "-6923.918257028", "7262098658246654328764889195300.202521", "-94901896213706.3469270226", "73.317379799912081044622659196694"}, "68807479072570463562933728167394355.148657311604170357303833936684", "8600934884071307945366716020924294.393582163950521294662979242086", "-94901896213706.3469270226", "68800177073604203202463611647840509.1429761281"},
    {'D', 4, 2, {"56864878338.8945", "-135.3058"}, "56864878203.5887", "28432439101.79435000", "-135.3058", "56864878338.8945"},
    {'D', 0, 1, {"-11968527217.21661"}, "-11968527217.21661", "-11968527217.21661000", "-11968527217.21661", "-11968527217.21661"},
    {'D', 0, 7, {"-371481972101652600617.22429545890739104", "-968429861100949409604933403494.82", "-69111959263881008987396.56628689602563", "4398.719777517418678", "49044.87516532449516909112540", "93777070975504506672972007721.546552621723160383410", "532536912640646880479546.102425011954827853148957929"}, "-874652327071973498267742450797.366661879341185712315642071", "-124950332438853356895391778685.338094554191597958902234582", "-968429861100949409604933403494.82", "93777070975504506672972007721.546552621723160383410"},
    {'D', 0, 2, {"85.1533058749026393871", "-29024517182171559.0164871672389928154940"}, "-29024517182171473.8631812923363534283940", "-14512258591085736.9315906461681767141970", "-29024517182171559.0164871672389928154940", "85.1533058749026393871"},
    {'D', 4, 8, {"1418687.7972", "995346829824640700.2257", "484699905037772.3572", "5638651025903697916.2386", "-9446780938059057610303040.0186", "-166285860084702168.7009", "6208178000956200.2571", "5190500082793.7909"}, "-9446774463648993559171138.0528", "-1180846807956124194896392.2566", "-9446780938059057610303040.0186", "5638651025903697916.2386"},
    {'D', 4, 1, {"-38110618759862621.9378"}, "-38110618759862621.9378", "-38110618759862621.9378", "-38110618759862621.9378", "-38110618759862621.9378"},
    {'D', 0, 3, {"-27258571327010050087001452948303192", "-3288004631958577427661.48408566535479", "2516947601810499486892529221773.548726719"}, "-27256054379411527592146518996509079.93535894635479", "-9085351459803842530715506332169693.31178631545160", "-27258571327010050087001452948303192", "2516947601810499486892529221773.548726719"},
    {'D', 0, 10, {"546160966037859962605.7796", "85366.01472425936590793", "-66241717286096.907777753629781734189394731", "-140072751795002360820178357156352.5355212073994112277", "-216486509219096241341523427698.12", "-5322.953632470680927207303179167", "81874987170220.0580564593", "7751677077.7840664606068552787", "-408309179339409788356457386735600770.976310353211712370", "-715519155315390.47618950984571370"}, "-408449468577713463653252746889896362.332984115494783030492573898", "-40844946857771346365325274688989636.233298411549478303049257390", "-408309179339409788356457386735600770.976310353211712370", "546160966037859962605.7796"},
    {'D', 4, 5, {"-8697362737669425851962426393479.1999", "12102.1050", "-22091557757032920562720640011.4564", "63775908085150594770933996.4695", "84414948344829883741585238451.1243"}, "-8634975571173543738188790848940.9575", "-1726995114234708747637758169788.1915", "-8697362737669425851962426393479.1999", "84414948344829883741585238451.1243"},
    {'D', 4, 9, {"-29060965.3201", "805321.3921", "12876545928928552108506355911361.8230", "5285018553891793183.8283", "687365952399307448535124376805.5812", "93404858230329.1715", "-285626.7826", "5635671660295400587724.1350", "-79175867160.2987"}, "13563911886968816329216426490973.5297", "1507101320774312925468491832330.3922", "-79175867160.2987", "12876545928928552108506355911361.8230"},
    {'D', 4, 4, {"6257209390598000579564574078566016.5415", "6075484693085666084103361721234.6134", "67924450512594865195746224.3070", "4255663812732334106599441421191.0294"}, "6267540607028269092350142077454666.4913", "1566885151757067273087535519363666.6228", "67924450512594865195746224.3070", "6257209390598000579564574078566016.5415"},
    {'D', 0, 12, {"-9654263.22456768840618", "8620.2101162402525563749112921", "96646971.778773513420343384386385", "-220429297236.78379929492", "-20986945140596642304857114228088931538.805587902819265446", "-3791906908627542365132192821.374258478249", "7693479284453161581905133036616962135.1641873135751", "27528601365833553492789064869.76293447760769837705624", "-7608039682745489141.5323628504523679", "425529084109052171939015143578435142.8", "-1.92954270753129118060", "9.16981087435613797917772"}, "-12867936748297734101414994823324447254.7642965031662684110683629", "-1072328062358144508451249568610370604.5636913752638557009223636", "-20986945140596642304857114228088931538.805587902819265446", "7693479284453161581905133036616962135.1641873135751"},
    {'D', 0, 10, {"619625245957768352556156432080979.997077003", "4501218073243.483176191435", "21135.28004613489036876706318", "687869862772.986535", "-2120990730.495316907389467021481", "14612329609095206265598443.09729", "22650108045844.634523602282142569726524", "652979696689253583180497822", "-39386999834662793300.78053179341299", "8522475987533044.4779"}, "619625913549755272455424277929254.680699230805054315308704", "61962591354975527245542427792925.468069923080505431530870", "-39386999834662793300.78053179341299", "619625245957768352556156432080979.997077003"},
    {'D', 0, 4, {"-178033.64", "0.866462736896976525203", "1878684888992.4", "18614.19905110724930926769"}, "1878684729573.825513844146285792893", "469671182393.456378461036571448223", "-178033.64", "1878684888992.4"},
    {'D', 4, 6, {"69093432187539314808506056978.8596", "-5237954311088891296755027644050798646.8217", "698968106415519480064527738945.3466", "-45542956733065862016.8655", "6154055.7756", "7358304319318619480491.1103"}, "-5237953543027345380934870185457230192.5951", "-872992257171224230155811697576205032.0992", "-5237954311088891296755027644050798646.8217", "698968106415519480064527738945.3466"},
    {'D', 0, 9, {"2933083976274261.735", "-40275501843051097491627706685.11", "35571896931293435735", "-459698320729521260347343590791281157.55229", "-767684972658.3172566817026", "-6790974664981925455919636709897044.31455178354894892599708105219", "5354164886829900028201997951.5411", "-7582428048246966758131625.2445182266572", "-237333.880639032894335"}, "-466489330323422534497878410100518556.14315572480308392599708105219", "-51832147813713614944208712233390950.68257285831145376955523122802", "-459698320729521260347343590791281157.55229", "5354164886829900028201997951.5411"},
    {'D', 0, 2, {"7135048721273420705.453293", "8260692340.94902990803195500"}, "7135048729534113046.40232290803195500", "3567524364767056523.20116145401597750", "8260692340.94902990803195500", "7135048721273420705.453293"},
    {'D', 4, 3, {"464584.9610", "58347852464409.2322", "81078144280740716876554857974064.6283"}, "81078144280740716934902710903058.8215", "27026048093580238978300903634352.9405", "464584.9610", "81078144280740716876554857974064.6283"},
    {'D', 0, 7, {"-112878133163162604001297159148326.925", "8623417816871548030.96057405932182862074", "6463.1480297473773809", "36559708367064067624223.0604870701", "95953426038983472182378311963839808086.69090863526", "-375.088804977114590365653777", "2174689016439181627225461948.174939179540670105513260890"}, "95953313163025034604545277174845300050.021133714485289260599483890", "13707616166146433514935039596406471435.717304816355041322942783413", "-112878133163162604001297159148326.925", "95953426038983472182378311963839808086.69090863526"},
    {'D', 4, 8, {"3737359027983119890787996840055.9213", "98.3266", "737799536295819319.8268", "534211818652777673677912.3214", "9361004322974716.0534", "511627398716168606679166008482.4027", "1043931502.8423", "-6.9281"}, "4248986960911854310786499252080.7664", "531123370113981788848312406510.0958", "-6.9281", "3737359027983119890787996840055.9213"},
    {'D', 0, 10, {"-815455178731058785.557456730763707318979", "56.76", "-1.94543514358", "-5567601624143827302656541447962322443.072184436604624", "7034157603436043.081767861477328504227427367184", "131399073847595.20643648", "5707582.803342882591630", "-86927689806288045236792914444.0847749079351053", "-73810160908.822750806", "-4412639321.251365174984319945818049378"}, "-5567601711071517109752876385026104626.882419975798798060569622010816", "-556760171107151710975287638502610462.688241997579879806056962201082", "-5567601624143827302656541447962322443.072184436604624", "7034157603436043.081767861477328504227427367184"},
    {'D', 0, 1, {"-461955494885039609992772855464247.8"}, "-461955494885039609992772855464247.8", "-461955494885039609992772855464247.8", "-461955494885039609992772855464247.8", "-461955494885039609992772855464247.8"},
    {'D', 4, 7, {"855243948872023.3611", "73600002599370946916879587126092677.3538", "-82511261160998047.0704", "-704195905464076525770.3827", "-945816055654367508898690591936633.7648", "92915969133503911635838013031610794.8295", "520939066887517.9046"}, "165570155677219786766778367344002562.2311", "23652879382459969538111195334857508.8902", "-945816055654367508898690591936633.7648", "92915969133503911635838013031610794.8295"},
    {'D', 0, 8, {"111546706808.2154976", "50664312844053762830974474675.390279", "1541614783697990410661002385.1296944164", "-74128556697347935832457562323494339.98472542585639526", "53588546624268352130645.554569616270853", "48500304852176052548127398524204.62938149762261160301141", "-44662410895196891423412390507525068.51334705889744242183603", "1507639185342674142801112482.24517174"}, "-118742413574072249562270061097068207.33347861446037307882462", "-14842801696759031195283757637133525.91668482680754663485308", "-74128556697347935832457562323494339.98472542585639526", "48500304852176052548127398524204.62938149762261160301141"},
    {'D', 0, 1, {"-1.24937451"}, "-1.24937451", "-1.24937451000000000000", "-1.24937451", "-1.24937451"},
    {'D', 4, 1, {"-429765003841480325065038140.2536"}, "-429765003841480325065038140.2536", "-429765003841480325065038140.2536", "-429765003841480325065038140.2536", "-429765003841480325065038140.2536"},
    {'D', 4, 8, {"535135165940771.9692", "59343488475.4717", "45267769934465096959051215923.9366", "-499526319236830289.9057", "0.5700", "40179132604528633590384150783139.7469", "0.2947", "31372061886396373985763986211953254953.6013"}, "31372102110796748448363682430427852975.6847", "3921512763849593556045460303803481621.9606", "-499526319236830289.9057", "31372061886396373985763986211953254953.6013"},
    {'D', 0, 2, {"61777649907.017314889389", "56207977294699303029213.3263505146077974899187"}, "56207977294761080679120.3436654039967974899187", "28103988647380540339560.1718327019983987449594", "61777649907.017314889389", "56207977294699303029213.3263505146077974899187"},
    {'D', 4, 7, {"1.7748", "-898216973030399632671662.2014", "6182211389532814931590723309.3955", "2816294938072894793637185325273846791.8249", "7061326513986200780123.7419", "530532066822323344911406612120703.6317", "-13026821855254369197903867141046473.8261"}, "2803798654465782981670491382903752794.3413", "400542664923683283095784483271964684.9059", "-13026821855254369197903867141046473.8261", "2816294938072894793637185325273846791.8249"},
    {'D', 4, 6, {"-44699.0592", "902670965328905963584066910430957689.7202", "-61710761444.0345", "-1092413121419915450958856.2470", "-2515361063.8300", "98994636654285400605.4793"}, "902670965327813649457283585039232232.0288", "150445160887968941576213930839872038.6715", "-1092413121419915450958856.2470", "902670965328905963584066910430957689.7202"},
    {'D', 0, 4, {"-20235070.9759013", "-43031609252161403389.5583238774234682147527964", "50497.3884829379482", "7660345591234912.1135001584"}, "-43023948906590353051.0322420810752682147527964", "-10755987226647588262.7580605202688170536881991", "-43031609252161403389.5583238774234682147527964", "7660345591234912.1135001584"},
    {'D', 0, 12, {"-149062744545910534339692721038.7353861228", "-8783192028054181074392598197453647277.757357854", "6290112747997611054055.49901329839852359020542", "29600815661618421922820294395128423.2458595274032", "-9353456349934031635093983.265250369059671", "-1708573113525.798753273797796906426127", "3951072.1733352", "-310.8", "36.9543493441936920", "-85.91593", "-1708057523697400258.34855", "-179655969669083269335.7"}, "-8753591361464654545981527178125112228.448670249662052316220707", "-729465946788721212165127264843759352.370722520805171026351726", "-8783192028054181074392598197453647277.757357854", "29600815661618421922820294395128423.2458595274032"},
    {'D', 0, 11, {"59328678360561969.28281230541", "-1", "739169206.211380904331380", "-33.783128951453796010", "0.9095453440040", "37993662966906864440767283537887.1107563963651335", "-57330924615684332171972.8993062", "96872306625496173772079255.635681996882506396938", "96719899950755142292367190293.1493926251867103131096", "28.75855792701407", "-83537.3577321145871814214669"}, "38090479681833379792228190283096.0179602331528227785807", "3462770880166670890202562753008.7289054757411657071437", "-57330924615684332171972.8993062", "37993662966906864440767283537887.1107563963651335"},
    {'D', 4, 8, {"-238247517874.9214", "12919477272110655946686046.1049", "-618034608432690893494422.0703", "17.2238", "-4.0172", "421746043908.6394", "-89665572074066948232855244.9914", "-66127499938051.0572"}, "-77364129410454927181075625.0894", "-9670516176306865897634453.1362", "-89665572074066948232855244.9914", "12919477272110655946686046.1049"},
    {'D', 4, 5, {"-280194001263649823610656860420243399.6478", "-50275351775585102190034026359772747298.3153", "691768116605556981.2450", "800318753820986.1768", "54768729991075713575260640765704.3021"}, "-50555491008118760937238539524192847026.2392", "-10111098201623752187447707904838569405.2478", "-50275351775585102190034026359772747298.3153", "54768729991075713575260640765704.3021"},
    {'D', 0, 3, {"-1632953325272811.8539479", "-256582029432082304680737464602790.1003", "106628390693975681554998996.55280316844358083552"}, "-256581922803691612338009234876605.40144473155641916448", "-85527307601230537446003078292201.80048157718547305483", "-256582029432082304680737464602790.1003", "106628390693975681554998996.55280316844358083552"},
    {'D', 0, 8, {"94729361365527297056.17160623362306455374551", "543352240.9147739865745385259", "7493604438483.4288114450635365095451392619", "-6152962319.5983834622183484959131", "0.7532", "1825956257054326.508396213006298874587545411", "7970779111395451680.7586727", "-8427299942661062371663673015.856111355365638601911199"}, "-8427299839959088450489041546.9190342393165486340461043271", "-1053412479994886056311130193.3648792799145685792557630409", "-8427299942661062371663673015.856111355365638601911199", "94729361365527297056.17160623362306455374551"},
    {'D', 0, 1, {"2216230391803643.640366661480563261792736"}, "2216230391803643.640366661480563261792736", "2216230391803643.640366661480563261792736", "2216230391803643.640366661480563261792736", "2216230391803643.640366661480563261792736"},
    {'D', 4, 12, {"10.8424", "653.4008", "700867330340608395382533.0188", "-19.2461", "4176173802158788109222.7583", "93921757254892803378412.1821", "-49628086716.3399", "176738664437968252.5418", "536771444813908384479.9663", "-71006318985347036158751407694.2483", "8751985090879388527.8423", "35444879108672435726146.1131"}, "-71005484029506361306731156191.1684", "-5917123669125530108894263015.9307", "-71006318985347036158751407694.2483", "700867330340608395382533.0188"},
    {'D', 0, 2, {"532362918.84789176867300180540230461514", "-843626119.90662484628341838807319"}, "-311263201.05873307761041658267088538486", "-155631600.52936653880520829133544269243", "-843626119.90662484628341838807319", "532362918.84789176867300180540230461514"},
    {'D', 0, 9, {"9809500607", "-8217487087867100568956093577980.520452", "82550813064007300.278707516119198989485", "-71974288757856487617040729.19189", "0.8256020602740367800", "-453879026630383073158273870014539043.6218598881384640", "5.9604398186655085", "-938029608.9527659197255794179280005181", "-785740081731678542"}, "-453887244189445229719888573521357990.2222184128052991484430005181", "-50431916021049469968876508169039776.6913576014228110164936667242", "-453879026630383073158273870014539043.6218598881384640", "82550813064007300.278707516119198989485"},
    {'D', 0, 1, {"-91825957912676582015563125975320.03"}, "-91825957912676582015563125975320.03", "-91825957912676582015563125975320.03", "-91825957912676582015563125975320.03", "-91825957912676582015563125975320.03"},
    {'D', 4, 3, {"-6450023.1373", "228538407977748.3053", "41748497.2694"}, "228538443276222.4374", "76179481092074.1458", "-6450023.1373", "228538407977748.3053"},
    {'D', 0, 9, {"100589374998303533003705186249164729.9085455", "7839983553258805852.537331", "74767282759655703858757842524865971710.2054606654072984379", "11554368367225818510", "20504210712303.068636685213065075188971019615", "1034306051942631194782513875.4723610673453512", "46662716807352082934648293283284.081616202169356972470870883", "4424529857173.15551922071", "-472472894344232449380298989936.327134"}, "74867918325932226470949041023117137502.102336340845071685559841902615", "8318657591770247385661004558124126389.122481815649452409506649100291", "-472472894344232449380298989936.327134", "74767282759655703858757842524865971710.2054606654072984379"},
    {'D', 0, 12, {"341288373847612198024174281225747501.369148028923", "9685.05871998261609276563443455825", "-8522965078630410857125972003.9117395033731", "926011225067688522542794282236.6338", "1.364985", "-432030355364029872991610.072723972660347761592231025", "9964444939074614695787387567.54408883871846831644579304", "3128902493541593463560962147625042417.6881413093136009403", "28249009139063748271477166554778760.83755793223777061", "-4383348603.521969355", "56135788864.8324522139176913319047142855", "-12892661.6779950839663456337636"}, "3498440803980542307633975998727832156.14446539072683056892911085875", "291536733665045192302831333227319346.34537211589390254741075923823", "-8522965078630410857125972003.9117395033731", "3128902493541593463560962147625042417.6881413093136009403"},
    {'D', 0, 8, {"-151607180403589298.3720904845133534883483", "68789094716.657608348865383002", "9619042520198994204.1514333882874995986", "3450817476141645501260757517512302.22116", "-8201790695093.718", "-808384257582595920014753850009.846244196829360230807", "7885.579912734367906814631522726", "-6994381736162044334362398005014136.81"}, "-3544372644277971961594448447539430.136220209821924303923777274", "-443046580534746495199306055942428.767027526227740537990472159", "-6994381736162044334362398005014136.81", "3450817476141645501260757517512302.22116"},
    {'D', 4, 6, {"-94241434515619.4199", "534279782297624316219271402.0767", "0.7984", "654.2364", "54519574402052228164392737113.3343", "-2515304.6347"}, "55053854184349758239174978246.3912", "9175642364058293039862496374.3985", "-94241434515619.4199", "54519574402052228164392737113.3343"},
    {'D', 4, 5, {"79039473553.9293", "-4200341262682183.7323", "-10516618317.2323", "45.4809", "556812174850.0615"}, "-4199715927652051.4929", "-839943185530410.2986", "-4200341262682183.7323", "556812174850.0615"},
    {'D', 0, 2, {"2072357.0", "-5273840511599806210809086569386757016.9025982920265185427855112358"}, "-5273840511599806210809086569384684659.9025982920265185427855112358", "-2636920255799903105404543284692342329.9512991460132592713927556179", "-5273840511599806210809086569386757016.9025982920265185427855112358", "2072357.0"},
    {'D', 4, 6, {"9981749818622445391349016199.2350", "7894456488421234354.8796", "-7646003.6649", "97564140581292400.1700", "-4120294029912613.9385", "-1.1534"}, "9981749826610345726313984335.5278", "1663624971101724287718997389.2546", "-4120294029912613.9385", "9981749818622445391349016199.2350"},
    {'D', 0, 12, {"1.10", "3292.769533384822168176604", "-464.01417960", "80848535074274997184613328540.66867151468445684", "56625318882891363979063055298.5827", "5.94648568686302", "356.114127125", "-5504.6759", "-9208584378916465317499885.06262330397453033", "419700778682637124546134258634871156", "-176064105855875089068306477654.5480742444844", "-1295372429"}, "419700740083176641458489887391902713.880740562910714686604", "34975061673598053454874157282658559.490061713575892890550", "-176064105855875089068306477654.5480742444844", "419700778682637124546134258634871156"},
    {'D', 0, 12, {"87731914305468746148889458102.621064603751013949643562681585", "850.1239", "21315300416357619998703712026706833.0585702976784436", "-3032.314705080955443780777386", "-9160056080831231364314493464952003.915680887750358021034865250134", "-46392442765605138520875765688262506.64617958", "-134053620754.8172131558", "66703526140011239456605658.1", "14249868256485534545693076583490128.20260896077355068198885695500", "380937365972106004564.667759835962171466473595327", "65931191066638848212.584516", "-8834480564185335460300433996054.795763"}, "-19996076855539122198939477939720003.131122006340622103706236286549", "-1666339737961593516578289828310000.260926833861718508642186357212", "-46392442765605138520875765688262506.64617958", "21315300416357619998703712026706833.0585702976784436"},
    {'D', 4, 1, {"5101457174943579429299529500496.0831"}, "5101457174943579429299529500496.0831", "5101457174943579429299529500496.0831", "5101457174943579429299529500496.0831", "5101457174943579429299529500496.0831"},
    {'D', 0, 6, {"-8827487521774496385842.99679456131029406", "-5.7475678914884939", "64937794817486929004378368.8688993214505304469", "-6633879987112364434847839691184999.657276971422791082748035943906", "-1322111069644256997486026422491281.46469368181810283701640", "178507018810551727304776713438158927.087"}, "170551027818724073202408001832475166.089566215410848567135564056094", "28425171303120678867068000305412527.681594369235141427855927342682", "-6633879987112364434847839691184999.657276971422791082748035943906", "178507018810551727304776713438158927.087"},
    {'D', 4, 7, {"2778761404586175234126973705.2214", "-75311881875327679231864047692425.2774", "-6092249937503282203805.1839", "2370625527628256065877479755326.4337", "5880824439066.5436", "-26883435379872.4514", "31077141505861177525882344314.5942"}, "-72907400450881225771732451763690.1198", "-10415342921554460824533207394812.8743", "-75311881875327679231864047692425.2774", "2370625527628256065877479755326.4337"},
    {'D', 0, 7, {"866016370128.963996", "-25545278854109720399927753.38219558868003609318056", "503.59649", "-338617313312.639974089010", "681527127668746729829963775978950948", "259295426.6356689779067285198", "994478412347796802628895.45334876472126070"}, "681527127644195929388202380040004836.62733406493795312661944", "97361018234885132769743197148572119.51819058070542187523135", "-25545278854109720399927753.38219558868003609318056", "681527127668746729829963775978950948"},
    {'D', 4, 2, {"919941979998290.1275", "-8552272171271.5199"}, "911389707827018.6076", "455694853913509.3038", "-8552272171271.5199", "919941979998290.1275"},
    {'D', 0, 8, {"-0.41735901683967071373603000", "2516876853365673.98750045623808485898621", "-973278035666515819986450761.847862760037", "95081393.7647018", "80917933619.07576793851754059406", "9.23815394350203779686", "7307073356.5692920289454248001151683723", "42487923962904.1757565710"}, "-973278035663956366889033805.4540490386735826637146516277", "-121659754457994545861129225.6817561298341978329643314535", "-973278035666515819986450761.847862760037", "2516876853365673.98750045623808485898621"},
    {'D', 4, 10, {"88653464067103666352.9228", "987068759.2397", "4359502374091777232621935132070526.6327", "900.7565", "-25.7501", "429777677643092.4135", "5823388059744771817574708.5275", "1.7126", "-785751906740523104337731224.1816", "357809963261860638.6063"}, "4359501594163347563547411642153730.8799", "435950159416334756354741164215373.0880", "-785751906740523104337731224.1816", "4359502374091777232621935132070526.6327"},
    {'D', 4, 9, {"7388277951636743654181498707.0643", "1.4650", "-55344392389285357334246903547708572.2951", "4320750.3015", "423.1752", "-3006822.4538", "-9715106328306.8884", "684269910668188301.7340", "-7402615914850353358895969271423509.8685"}, "-62747000915857758372139023074459027.7658", "-6971888990650862041348780341606558.6406", "-55344392389285357334246903547708572.2951", "7388277951636743654181498707.0643"},
    {'D', 4, 1, {"-5824577.2639"}, "-5824577.2639", "-5824577.263900000000", "-5824577.2639", "-5824577.2639"},
    {'D', 0, 11, {"59.71475847212922990905", "49498297733571.36459744325989046356256678", "-87306678276686517862530987004249881.340499152843427476658693", "-137064791342.9901", "111220023793115295587060982595080.3716", "-152095098.7773293307", "-9676145123119265982905381926502222.77943523669451519", "918286999243063750764837055.704190", "84136864218361510217162436973.8563019327425406603", "2958039944", "-781700498320063761.6740142445922784011387227"}, "-96871518320861451726926474302059622.54993011669856003488484892", "-8806501665532859247902406754732692.95908455606350545771680445", "-87306678276686517862530987004249881.340499152843427476658693", "111220023793115295587060982595080.3716"},
    {'D', 0, 2, {"197098.746586847838930358652576541", "-439676950959236610778853251.3659"}, "-439676950959236610778656152.619313152161069641347423459", "-219838475479618305389328076.309656576080534820673711730", "-439676950959236610778853251.3659", "197098.746586847838930358652576541"},
    {'D', 0, 6, {"79963116242871265433088848201044.98658434188052649825877775687", "-2152855.72476806105685", "269493438427202234449447.1261881680210", "553032878159217426166542334958668.6569792124", "20610736022691780792262.4573576894728762836285355", "8113002149.2679639"}, "632995994692192866049533309250716.77030525071755278188731325687", "105499332448698811008255551541786.12838420845292546364788554281", "-2152855.72476806105685", "553032878159217426166542334958668.6569792124"},
    {'D', 0, 2, {"637988197860249971979611121201458725.761", "6033662908623342017.056099524329811974"}, "637988197860249978013274029824800742.817099524329811974", "318994098930124989006637014912400371.408549762164905987", "6033662908623342017.056099524329811974", "637988197860249971979611121201458725.761"},
    {'D', 0, 8, {"982569499406116999943098808545.61949", "-95837714425661141090989394423994052028.899610819", "50128620801457299.62519820614103889428060", "-612793686.52389122", "-899761547254739.45", "-2919240.61167066029109722", "-5695452387.3341556122373420003805", "20136876995719768201379341208724907"}, "-95817576566095921916621786286743481330.57464010538740032609990", "-11977197070761990239577723285842935166.32183001317342504076249", "-95837714425661141090989394423994052028.899610819", "20136876995719768201379341208724907"},
    {'D', 0, 3, {"-2374076365841.5790056259353679291502456", "-913427698.24148502669251325742687", "-878837636035713825395523.115717954752842799567027"}, "-878837636038088815189062.9362086073807239861441426", "-292945878679362938396354.3120695357935746620480475", "-878837636035713825395523.115717954752842799567027", "-913427698.24148502669251325742687"},
    {'D', 4, 9, {"91077152919320282827825978.5837", "-689797390793664762.7065", "270708288382298825651594400663156660.7296", "95961166296459.8389", "-97796360700212.5992", "1.7118", "-70176874612077548.3620", "94191304491063778011324325650638212188.6021", "8473388.2455"}, "94462012779537153989135264233537522154.0439", "10495779197726350443237251581504169128.2271", "-689797390793664762.7065", "94191304491063778011324325650638212188.6021"},
    {'D', 0, 11, {"-9223735631633655460378003863559019561.5709018294592436992960469", "-32894468284694190804350190617.91475016771521", "126083657692423067.891676482493009762189", "9847032827272368604.5313318232925", "-96784600071914889.1139053051165416045270", "95823523524242453154095657040060", "66981361566533.632955", "87602426007.86769", "-314179792187750777210458756298978.13", "467322786.72063292540158", "92242121393632009905201.6863113976"}, "-9223954020796695001398458444674371784.3989596735039055416340469", "-838541274617881363763496222243124707.6726326975912641401485497", "-9223735631633655460378003863559019561.5709018294592436992960469", "95823523524242453154095657040060"},
    {'D', 0, 1, {"8789179904962277126290779267283398.44448872185689871"}, "8789179904962277126290779267283398.44448872185689871", "8789179904962277126290779267283398.44448872185689871", "8789179904962277126290779267283398.44448872185689871", "8789179904962277126290779267283398.44448872185689871"},
    {'D', 4, 9, {"710886711445.4343", "-1475.6550", "-844879529.5987", "-219025170166.2993", "168740420664497620134894667924658304.2401", "-91773554436658.7267", "9417953774964926995856131836517.9901", "630486795278108605104716498417264.7922", "98.8667"}, "169380325413550693666903958017135801.0437", "18820036157061188185211550890792866.7826", "-91773554436658.7267", "168740420664497620134894667924658304.2401"},
    {'D', 0, 12, {"443028083749465522029485627678927813.270103031526657912", "188380234144829908379.566", "954505518528089049798938.808064376653", "10.498605453", "-6715749063363.747245", "-568928.1217414888716430348", "56008981584114918608287865731701.77904373", "-91784356937567078414566.482252459784557633051617", "-79498360065158142853960846.993383449", "-19247.50710858", "88264370287040184418013.469033741294", "-6433375654259793323545.2867119419393"}, "443084092652496017419386504133434359.252407412878157244148383", "36923674387708001451615542011119529.937700617739846437012365", "-79498360065158142853960846.993383449", "443028083749465522029485627678927813.270103031526657912"},
    {'D', 0, 5, {"-10528961055951222811.96539819482191534", "708556388.460484321743743881100", "-304.0198115763353277585800", "-10863027834.687607598648454728571", "-92661057283674.84766225078958"}, "-10529053727162978237.0599952988515339460510", "-2105810745432595647.4119990597703067892102", "-10528961055951222811.96539819482191534", "708556388.460484321743743881100"},
    {'D', 0, 7, {"-846494301138958864562569813", "-705.9565059", "312510717172895338960872801255405442.3747", "2681217992196532180811271847886193.8063", "-55172361475880328775979482300.64040148723524134", "6325787259091.897668381716308", "-722825947.181"}, "315191879146236094122402757625671961.30076099448106666", "45027411306605156303200393946524565.90010871349729524", "-55172361475880328775979482300.64040148723524134", "312510717172895338960872801255405442.3747"},
    {'D', 0, 10, {"-1527068770082171356585091967541989865.30438445", "-203543001925058389545.04797324330", "626422186549086739250007142979511785.995240", "-73.35092", "-473829.9231623892966", "-14649879.56227975553", "560955730488718800997532982795395585.4677806304023019096", "970.93417938921071888", "-3711179599512909038641.000025418939806", "6798288532964412351410589677037637.2544318211086337189270"}, "-332892564511405318708742690072595854.5371134163447514914730", "-33289256451140531870874269007259585.4537113416344751491473", "-1527068770082171356585091967541989865.30438445", "626422186549086739250007142979511785.995240"},
    {'D', 0, 1, {"599116157691.53972602929878178209"}, "599116157691.53972602929878178209", "599116157691.53972602929878178209", "599116157691.53972602929878178209", "599116157691.53972602929878178209"},
    {'D', 0, 1, {"2362079655903888248996900473466242.905921158921978518635"}, "2362079655903888248996900473466242.905921158921978518635", "2362079655903888248996900473466242.905921158921978518635", "2362079655903888248996900473466242.905921158921978518635", "2362079655903888248996900473466242.905921158921978518635"},
    {'D', 4, 11, {"2280945455.3812", "82458320853059073484758406075.9265", "-86912462521594005694666856344515213718.7076", "-55887704263917728485200329672056002099.7581", "29213549873806305563660222511144303.0583", "-7925132602407.5349", "-628147010671804879951625722528.7163", "2.6373", "577588275408516067953653.3726", "884436047665.4450", "999984666917160689183911223376322.7798"}, "-142769953796659122944084746872051667276.1162", "-12979086708787192994916795170186515206.9197", "-86912462521594005694666856344515213718.7076", "29213549873806305563660222511144303.0583"},
    {'D', 4, 10, {"112.7729", "-58.3387", "-878285106407313.7909", "-251906333708879120674526494379944787.1043", "87995498744214941052148901.3464", "5907920934880346288647852308664.9130", "22245698076120104836884593921.8870", "251542108637563409.3828", "0.5752", "-11437894401716847.0357"}, "-251900403454250665224692139461453995.3923", "-25190040345425066522469213946145399.5392", "-251906333708879120674526494379944787.1043", "5907920934880346288647852308664.9130"},
    {'D', 0, 11, {"636264744274342.43018468783693359096882752964", "8263275916820.167179237771429989", "3235.508809572498041388447", "-12603578325373453486896662463.5", "-409780814328928661807086892641.352", "-771754034322.7319667759738423784310069106", "-344776897106631101034258315.211368553284131595453603085369", "2163.77558734784665309487320287169", "2102345570254018544454448724210487.86149796917103805", "11231992.197104868492043997118413921", "-980849233567543018434610698257.8782022902"}, "2100941991850899593333375373093041.266826064158166136522834326361", "190994726531899963030306852099367.387893278559833285138439484215", "-980849233567543018434610698257.8782022902", "2102345570254018544454448724210487.86149796917103805"},
    {'D', 0, 5, {"-306605208074429515404618.5223", "2705250127606806621845406117010713532.135929796593733880288084073760", "-538069415058087694494875964759.354209446412", "303748388.75847", "613703763.940085480132"}, "2705249589537084958549637193536796306.957975830313733880288084073760", "541049917907416991709927438707359261.391595166062746776057616814752", "-538069415058087694494875964759.354209446412", "2705250127606806621845406117010713532.135929796593733880288084073760"},
    {'D', 4, 1, {"-158648434336784.8856"}, "-158648434336784.8856", "-158648434336784.8856", "-158648434336784.8856", "-158648434336784.8856"},
    {'D', 0, 10, {"-10.7464347946278719035", "20617486214991565913.944387706104447587700187542737", "4552.430280308300999788181", "49296955134092770693291476322896.630888358156678398026", "58130382473614686937148372035241742.112095412988229627598535185", "44190250957510.071533119934", "181812726771258687004.5145137328742917033823817929", "61180101441540279498476.1339506", "31146924693310337.486339458", "29065213828191568781600554498.6097482091075723141371"}, "58179708494023990462256086586142921.187302110838347515525204520637", "5817970849402399046225608658614292.118730211083834751552520452064", "-10.7464347946278719035", "58130382473614686937148372035241742.112095412988229627598535185"},
    {'D', 4, 4, {"-69663079215814.1475", "3427293341914131592657159991342.6408", "42292273.3079", "0.1091"}, "3427293341914131522994123067801.9103", "856823335478532880748530766950.4776", "-69663079215814.1475", "3427293341914131592657159991342.6408"},
    {'D', 0, 4, {"3432385276299257862584603253724432192.712", "-2861738136200229237046237682.4919143872790", "833.366263328854550308", "54.4630434461217814"}, "3432385273437519726384374016678195398.049392387697331708", "858096318359379931596093504169548849.512348096924332927", "-2861738136200229237046237682.4919143872790", "3432385276299257862584603253724432192.712"},
    {'D', 0, 10, {"476474776634606336941214527093161.277375215083810", "5812.63960079848740109421", "2116978.2889552049", "924699", "6395665629688923599576557.69404353649444614", "-10.7508474370678637366997254912", "535486511695837303508.262005398170386", "-205382324506804.521703221096689836713582071", "-60.4021527675", "-3.03286"}, "476474783030807452936451642513838.4544167274714896607966924378", "47647478303080745293645164251383.8454416727471489660796692438", "-205382324506804.521703221096689836713582071", "476474776634606336941214527093161.277375215083810"},
    {'D', 0, 2, {"530768573599812.587370318608968936", "309273122840340812937673613966515776.363009215567158"}, "309273122840340812938204382540115588.950379534176126936", "154636561420170406469102191270057794.475189767088063468", "530768573599812.587370318608968936", "309273122840340812937673613966515776.363009215567158"},
    {'D', 0, 2, {"28488998751482394.251", "1.119"}, "28488998751482395.370", "14244499375741197.6850", "1.119", "28488998751482394.251"},
    {'D', 0, 5, {"-88592983069777091039.07109353686141287121324", "60186154759794697003130.7088622039502143243823873", "-46269954881711387115891.6605983063753", "-6571.7284014783514841387528687877", "275.252221709101658829"}, "13827606895013532789903.5009905914636761434162785123", "2765521379002706557980.7001981182927352286832557025", "-46269954881711387115891.6605983063753", "60186154759794697003130.7088622039502143243823873"},
    {'D', 4, 6, {"89745937730553.0761", "57667451341126002800556089688417.1020", "0.3115", "54577386380666979578858553685044169.9345", "583.0005", "700.8522"}, "54635053832008105581748855712464424.2768", "9105842305334684263624809285410737.3795", "0.3115", "54577386380666979578858553685044169.9345"},
    {'D', 4, 4, {"-6057404891391518978.4318", "-1184822719361535126.6056", "983.9070", "-62458674979691330638411337491550.6795"}, "-62458674979698572866022090544671.8099", "-15614668744924643216505522636167.9525", "-62458674979691330638411337491550.6795", "983.9070"},
    {'D', 4, 5, {"1050433160044401441631352231088.5191", "-191191117268262754312348875637.2187", "7596312493264862264591146135880338905.9923", "87.1265", "-493507771578063.7109"}, "7596313352506905040729339947112116380.7083", "1519262670501381008145867989422423276.1417", "-191191117268262754312348875637.2187", "7596312493264862264591146135880338905.9923"},
    {'D', 4, 8, {"830388784415203509227875654.6833", "718080642.7738", "-3103138448552305761123933306746859.0169", "7216643499876619859351634290284.1436", "-788976569880249003542877751.4015", "9144819.0950", "-740944813790.3999", "53141914466751409224105642375620.5530"}, "-3042779849173463197086710562671379.5696", "-380347481146682899635838820333922.4462", "-3103138448552305761123933306746859.0169", "53141914466751409224105642375620.5530"},
    {'D', 0, 1, {"4063699817394818594114120180274270044.614527659990147946583256331"}, "4063699817394818594114120180274270044.614527659990147946583256331", "4063699817394818594114120180274270044.614527659990147946583256331", "4063699817394818594114120180274270044.614527659990147946583256331", "4063699817394818594114120180274270044.614527659990147946583256331"},
    {'D', 4, 11, {"-0.2915", "-80121732915592173832324048.7947", "-5477741228900879064.0477", "-47798928543710366438.2496", "-1.5372", "324744.6972", "-136782656441699524546.7760", "875746534925142184093681632417106117.1952", "20230139931.6122", "-3162521710368651447603861729.6060", "-43950292078390597030525886.0175"}, "875746531638548258671721219870089078.1844", "79613321058049841697429201806371734.3804", "-3162521710368651447603861729.6060", "875746534925142184093681632417106117.1952"},
    {'D', 0, 6, {"27980581452917981081269246275780445.849548480409833455103430263", "-40765349.5", "42507506020486127752349973349861313.02174261073913352436137623", "2904", "24.0125495348244846428776801265", "-70.05135"}, "70488087473404108833619219584879267.3324906259734516223424866195", "11748014578900684805603203264146544.5554151043289086037237477699", "-40765349.5", "42507506020486127752349973349861313.02174261073913352436137623"},
    {'D', 4, 11, {"-50566200164363325482648104922549524.3411", "-95732819966618345007.0522", "94957265907777.2857", "-9056801.7863", "-28395388120255262173366536.6998", "2492.3927", "74499413669698.0117", "106338435716994077105592382529.5642", "-38208994977611374054019562975068219736.8587", "32323591428497471527994029090612038.7048", "753643411703236940453323834903091622.0493"}, "-37473594068333962734379721882525871448.7297", "-3406690369848542066761792898411442858.9754", "-38208994977611374054019562975068219736.8587", "753643411703236940453323834903091622.0493"},
    {'D', 4, 10, {"-48494994990207469.2595", "2021808452785299475095356313832.3293", "7073455865.8550", "-5375911548462361498191404048301.4964", "-76365196740948259622.1170", "529323556177113898312943207472981953.7780", "637984891422738097935211.3860", "94.7247", "91890154554.8633", "-268536388870.0092"}, "529320202074656129728650944011937250.0542", "52932020207465612972865094401193725.0054", "-5375911548462361498191404048301.4964", "529323556177113898312943207472981953.7780"},
    {'D', 0, 1, {"-73046016309741728921582804641634499.035742730221641853"}, "-73046016309741728921582804641634499.035742730221641853", "-73046016309741728921582804641634499.035742730221641853", "-73046016309741728921582804641634499.035742730221641853", "-73046016309741728921582804641634499.035742730221641853"},
    {'D', 4, 6, {"-7.5495", "12586.3652", "437201941090.9673", "32110680501385850.6627", "87772675172440639.9349", "-7314.6344"}, "119883792875772845.7462", "19980632145962140.9577", "-7314.6344", "87772675172440639.9349"},
    {'D', 4, 12, {"11322515330398113021730928751653206.6160", "-560047.7387", "-1376717726781052198475.4452", "6933.1646", "-2985.8998", "20797640466896697281482.4553", "-30461156912799525674481.7880", "208732615034570061163236940819.9613", "0.5037", "93294619349896700767889.5165", "79192001059539264436319956.8086", "436146382771600.6848"}, "11322724142287403036944715627305898.8391", "943560345190616919745392968942158.2366", "-30461156912799525674481.7880", "11322515330398113021730928751653206.6160"},
    {'D', 0, 2, {"8.70363219", "34951084155400964.3009677579937743615"}, "34951084155400973.0045999479937743615", "17475542077700486.5022999739968871808", "8.70363219", "34951084155400964.3009677579937743615"},
    {'D', 0, 11, {"986575026737545790069.891", "-82602298866979241885331.305449230688367117085", "-4048311656674.5487", "1843142610098883245779506.73496510512837199909990", "-79826684790055655468091251659593936978.470203708934", "9948275719348107490908553604299.682216369122884875963955342", "22792953003031843200465329785.87508043961367957089180237", "2681682479855938096914796837485033.6025", "383958475763459.56684105657752", "-46154103053231293966.84", "-375918307.1054886727656585"}, "-79823993136505365698122467255630939102.917238641945569171129342288", "-7256726648773215063465678841420994463.901567149267779015557212935", "-79826684790055655468091251659593936978.470203708934", "2681682479855938096914796837485033.6025"},
    {'D', 0, 9, {"127492632696085738472.714357075792", "-75792365113683214.55222482921978055", "168932903.77846785261549692452086", "362431396756043699790565923724699627.6902", "-43592583993.979106113814304257891246", "1251483026835386398995196343139793.541055717650683926062457120631", "8854791612697644186710008496278521.0123972392615272", "99732273733740207807850231.952743752440782229254540641", "-25977904528683354945931.851279870232364154216253386"}, "372537671495283153622322940565426410.306610824494041317730358375631", "41393074610587017069146993396158490.034067869388226813081150930626", "-25977904528683354945931.851279870232364154216253386", "362431396756043699790565923724699627.6902"},
    {'D', 0, 12, {"-809168211978712729809857675522.153546689238118780", "8586795033614864.87562844790545", "-41415525373400968970154719.5602019718985", "-246095254233814996651693246.91558165581311962566559639", "79412011619057.03644754910", "-59621828936728453642185001347033.0983493074805996777695413", "-143862292170777113.71605016370668377126929", "877.2313061402654869877446626", "-569212715870627234529494550600627255.89750782", "-567902223747156650961730865997861849.98", "-722175702613839935215448650311.76395064887187920", "613454063666921608026464586112749.62091259108"}, "-1136562639014479064631918213067439504.32089352865796406695976509", "-94713553251206588719326517755619958.69340779405483033891331376", "-569212715870627234529494550600627255.89750782", "613454063666921608026464586112749.62091259108"},
    {'D', 0, 5, {"350038490087187420569380323287.68765049", "-6420805769616365321383643920776260.2684835753521357443299", "-74568663", "-8695.470", "46.7758"}, "-6420455731126278133963074615030284.2750330853521357443299", "-1284091146225255626792614923006056.8550066170704271488660", "-6420805769616365321383643920776260.2684835753521357443299", "350038490087187420569380323287.68765049"},
    {'D', 4, 6, {"-187498394.4838", "-51356.5991", "8896633965592545811135062.0420", "-59168429439977482.4410", "88248435534416224959391694651298691.6969", "2.7849"}, "88248435543312858865815810834906522.9999", "14708072590552143144302635139151087.1667", "-59168429439977482.4410", "88248435534416224959391694651298691.6969"},
    {'D', 0, 9, {"1.54710541206856094896348061852", "-518135456357580624887385467.822281986874885885158805241668", "62925544449846729551872018871444214409.22874331433056183315", "8094408708095819650776190.99094245878", "46830928655704.061724", "-336292242420589855280295287.98326333850805104", "-3.5184135973086139799", "4779815214231093505.851517893358328960908182235928", "165.68869657"}, "62925544449000396266581806256087059218.044770725845900837962857612780", "6991727161000044029620200695120784357.560530080649544537551428623642", "-518135456357580624887385467.822281986874885885158805241668", "62925544449846729551872018871444214409.22874331433056183315"},
    {'D', 0, 12, {"-248306077916443326158542186138.26364117986593255846562", "-66276494103.50", "679858489429108264055511134049.666969849559936414140831435554", "-7001020986102.91530057", "-389456288389461570003313405.1404715076174182151", "-76982679225750934681.35586584788535114", "-903957014046771161986460839170967760.694750077692", "-26836.002963943", "1721537928076882.9413", "42908353.878506", "-326533753548784827.1876525805300368", "-8268000585110946429260477018039464.142290546881618468734944327503"}, "-912224583468926961447743497849614032.716160403912420768159732891949", "-76018715289077246787311958154134502.726346700326035064013311074329", "-903957014046771161986460839170967760.694750077692", "679858489429108264055511134049.666969849559936414140831435554"},
    {'D', 4, 6, {"-471.6127", "-542001966405.6038", "6815088161903321612715109695.2445", "3962.1592", "-84251342026256241768239561967.4471", "43549795267385849429978635.0324"}, "-77392704069085534848096436552.2275", "-12898784011514255808016072758.7046", "-84251342026256241768239561967.4471", "6815088161903321612715109695.2445"},
    {'D', 0, 7, {"-5754875902125790431002471767.138040926117911088", "-75195202057957477264.571221661255661899017597116", "-9467481204651896.328", "2743137510610896961946.4541898255615378", "-74542241293345596138205551", "99808701911387120511.31402190405089057847579440121", "42901.3893141045"}, "-5829415375677593044018681119.87973675326114460854180271479", "-832773625096799006288383017.12567667903730637264882895926", "-5754875902125790431002471767.138040926117911088", "2743137510610896961946.4541898255615378"},
    {'D', 4, 6, {"-73776264244.4637", "-5876495540263762501998199811.3679", "89572673657477130.5261", "694785252538246364858829389543609686.7105", "-16221180759825652692938448471.6758", "-76.2229"}, "694785230440570064858986794488174213.5063", "115797538406761677476497799081362368.9177", "-16221180759825652692938448471.6758", "694785252538246364858829389543609686.7105"},
    {'D', 0, 8, {"57944401523642719953739.653192760878215175372251278660", "8028031596636955920.49", "-679940949073.271831383142224945302959050899", "7067103035185909959085438151.2575334105211952716499784", "291871303914771", "-573854074085140339285286666977890", "96854398382187467476946336.77927055785815", "-45003997056463790.5461198211792084762"}, "-573846910069754386345426441181834.637954475063872974480729372239", "-71730863758719298293178305147729.329744309382984121810091171530", "-573854074085140339285286666977890", "7067103035185909959085438151.2575334105211952716499784"},
    {'D', 0, 2, {"-23137123800.8061075104820147403009865", "-233869941400.31376352917112079012698"}, "-257007065201.1198710396531355304279665", "-128503532600.5599355198265677652139833", "-233869941400.31376352917112079012698", "-23137123800.8061075104820147403009865"},
    {'D', 0, 7, {"-22847537933096437475.72178427265303647", "-422473911637373793235488552744.038253874", "-60095200401600753959300767790237.6", "631979747972826863817565.4512", "275225609233247.57913590822", "-373408.49664668096905222480", "-19886264211.56496186014489245209299653614"}, "-60517673681281227042436766367264.39131077954698114689299653614", "-8645381954468746720348109481037.77018725422099730669899950516", "-60095200401600753959300767790237.6", "631979747972826863817565.4512"},
    {'D', 4, 9, {"-29077728332124483035872058962.3461", "-425358305507352275.2179", "6977592468482184441850732624.6114", "-5411154.8997", "55563734760501005594.0792", "60755169743.0407", "912941628562171890.6184", "580619871930.5404", "971852792807033.6309"}, "-22100135807590008016303063575.9427", "-2455570645287778668478118175.1047", "-29077728332124483035872058962.3461", "6977592468482184441850732624.6114"},
    {'D', 0, 10, {"-1564315.499578243096629641422351", "9294134.721261085302246054570", "35612789333.86986473927981699494835161667", "51077034984.4352880510460", "-490358293973.772152093256893", "89810928233538473102539607170309369.60262890", "7354828752274133777806151230638.83", "-509958810.137", "-7448853712610438023637846712498.15655203357319054", "-9.0342035709049644191275733"}, "89810834208578136798293371304128854.85955683479638544896842731667", "8981083420857813679829337130412885.48595568347963854489684273167", "-7448853712610438023637846712498.15655203357319054", "89810928233538473102539607170309369.60262890"},
    {'D', 0, 12, {"303861461484894516025028803758806605.2413255199773995177", "-30578857150552622399456515457447849227.2140816979755001", "-5399113643173563952885654570909868829.93530539458168593", "-508820546398584521392882443172894969.9620789986495264389797", "964418166961208.32", "697305817170350954815885429474799.63752006035581732182645982505", "-65703740118368447218012570198033854080.917083206545334633392836451773", "2662826564403421669147098483077.228651860679", "32089723363718375104633171714004.967286279", "192335560291.92", "-933752354169737993447545580907657.4", "-4429213.147966799"}, "-101886871690995394840963487135188803991.261732376739830262846076626723", "-8490572640916282903413623927932400332.605144364728319188570506385560", "-65703740118368447218012570198033854080.917083206545334633392836451773", "303861461484894516025028803758806605.2413255199773995177"},
    {'D', 4, 4, {"62873244.2046", "1.9697", "38275280662215607710399010.5054", "812405109781.9663"}, "38275280662216420178382038.6460", "9568820165554105044595509.6615", "1.9697", "38275280662215607710399010.5054"},
    {'D', 0, 4, {"13110283362.35076849922257934589835959", "-23219.936836683335488512923070620", "-14.636499425979857", "-65052055472289287.4811693446"}, "-65052042362029159.703736954692766167024711030", "-16263010590507289.925934238673191541756177758", "-65052055472289287.4811693446", "13110283362.35076849922257934589835959"},
    {'D', 0, 1, {"-42491412750342886914875343383031856056.086227422113507205714"}, "-42491412750342886914875343383031856056.086227422113507205714", "-42491412750342886914875343383031856056.086227422113507205714", "-42491412750342886914875343383031856056.086227422113507205714", "-42491412750342886914875343383031856056.086227422113507205714"},
    {'D', 0, 5, {"219860.02331942", "40756582070178587.2733", "-4754138381562480006723.56662389033959374061726988", "-81434901863.7811", "77862979451464428925194888912.6565535200485194227063381752"}, "77862974697366803863350378772.6054490497089256820890682952", "15572594939473360772670075754.5210898099417851364178136590", "-4754138381562480006723.56662389033959374061726988", "77862979451464428925194888912.6565535200485194227063381752"},
    {'D', 0, 8, {"877593665936732087.60823", "-4563526441916654270996.99820460662969058353514691", "804618411055904040887.48387722967514601148989", "-972704.56180104991", "842446405866513306794663284922780954.609866008417183973925168", "-8177667722113.6820719950971034543388899964", "-330814.399224388506714483229", "-5161903646573418047817750307904644066.981885869786337029"}, "-4319457240706908499053532395464386766.9212146718375155646879789064", "-539932155088363562381691549433048345.8651518339796894455859973633", "-5161903646573418047817750307904644066.981885869786337029", "842446405866513306794663284922780954.609866008417183973925168"},
    {'D', 0, 4, {"-44473753854964797154.30221325952243050151409290208", "64.95501958249445", "-3911072871726576.63319674899203603061526", "354542081531817869558394284.86679000734828588"}, "354542037054152941721870618.88639958132826934787064709792", "88635509263538235430467654.72159989533206733696766177448", "-44473753854964797154.30221325952243050151409290208", "354542081531817869558394284.86679000734828588"},
    {'D', 0, 3, {"-20513470483940817441446691317199052044.9522134875729", "-6429056807589796265231294025300329.8712278547433218312239769", "709155838598439603626725171932.45041438287440531"}, "-20519898831592568639272318984499180442.3730269594418165212239769", "-6839966277197522879757439661499726814.1243423198139388404079923", "-20513470483940817441446691317199052044.9522134875729", "709155838598439603626725171932.45041438287440531"},
};
```

**New `src/test/cases/test_agg_exact_routes.c` (complete file):**

```c
/* src/test/cases/test_agg_exact_routes.c
 *
 * R3 legacy-object route matrix over the request layer. Three identical
 * objects (all-indexed splits 8, unindexed, all-indexed splits 64) prove
 * route/layout parity for exact sums above 2^53, declared-scale decimals,
 * timestamp groups, empty-input avg, and the CSV renderer. The limits
 * registration covers having on adjacent values, the D6 key-limit error,
 * bounded-memory rejection and the allocation-fault loop.
 */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/shard_db.h"
#include "../db/agg_exact.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static ShardDb *ar_db;
static char *ar_req(const char *json) {
    char *r = NULL;
    tu_pdb_request(ar_db, json, &r);
    return r;
}
static void ar_chomp(char *s) {
    size_t n = s ? strlen(s) : 0;
    while (n && (s[n - 1] == '\n' || s[n - 1] == ' ')) s[--n] = 0;
}
/* Exact response comparison (trailing newline ignored). */
static void ar_expect(const char *obj, const char *extra, const char *aggs,
                      const char *expect, const char *msg) {
    char req[2048];
    snprintf(req, sizeof req,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
        "\"aggregates\":[%s]%s}", obj, aggs, extra);
    char *r = ar_req(req);
    ASSERT_NOT_NULL(r, msg);
    if (!r) return;
    ar_chomp(r);
    ASSERT_EQ_STR(r, expect, msg);
    shard_db_free_result(r);
}

static void ar_make(const char *obj, int splits, int indexed) {
    char req[2048];
    snprintf(req, sizeof req,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":%d,\"max_key\":16,\"fields\":[\"l:long\",\"n:numeric:19,4\","
        "\"ts:timestamp\",\"d:double\",\"g:varchar:16\"]%s}", obj, splits,
        indexed ? ",\"indexes\":[\"l\",\"n\",\"ts\",\"d\",\"g\"]" : "");
    char *r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "object created");
    shard_db_free_result(r);
    snprintf(req, sizeof req,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"g\":\"a\",\"l\":9007199254740993,\"n\":\"0.1000\",\"ts\":1490833334666,\"d\":0.5}},"
        "{\"key\":\"k2\",\"value\":{\"g\":\"a\",\"l\":9007199254740993,\"n\":\"0.2000\",\"ts\":1490833334667,\"d\":0.25}},"
        "{\"key\":\"k3\",\"value\":{\"g\":\"b\",\"l\":9223372036854775807,\"n\":\"1.5000\",\"ts\":1490833334667,\"d\":1.0}},"
        "{\"key\":\"k4\",\"value\":{\"g\":\"b\",\"l\":-9223372036854775807,\"n\":\"-1.5000\",\"ts\":1000,\"d\":2.0}},"
        "{\"key\":\"k5\",\"value\":{\"g\":\"b\",\"l\":1,\"n\":null,\"ts\":2000,\"d\":null}},"
        "{\"key\":\"k6\",\"value\":{\"g\":\"c\",\"l\":null,\"n\":null,\"ts\":3000,\"d\":null}}]}", obj);
    r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "rows inserted");
    shard_db_free_result(r);
}

#define AR_S(fn, f, a) "{\"fn\":\"" fn "\",\"field\":\"" f "\",\"alias\":\"" a "\"}"
static void ar_matrix(const char *obj) {
    /* A3/A4: whole-object scalars (leaf-only bundle on indexed objects,
       hash scan otherwise). Oracle goldens: see docs/plans R3 §goldens. */
    ar_expect(obj, "",
        AR_S("sum","l","s") "," AR_S("avg","l","a") "," AR_S("min","l","mn") ","
        AR_S("max","l","mx") "," AR_S("sum","n","sn") "," AR_S("avg","n","an") ","
        AR_S("min","n","mnn") "," AR_S("max","n","mxn") "," AR_S("avg","ts","at") ","
        AR_S("sum","d","sd") "," AR_S("avg","d","ad"),
        "{\"s\":18014398509481987,\"a\":3602879701896397.4000,"
        "\"mn\":-9223372036854775807,\"mx\":9223372036854775807,"
        "\"sn\":0.3000,\"an\":0.07500000000000000000,\"mnn\":-1.5000,\"mxn\":1.5000,"
        "\"at\":745416668333.33333333,\"sd\":3.75,\"ad\":0.9375}", obj);
    /* group_by text key, grouped sums (exact, scaled, empty-set shapes) */
    ar_expect(obj, ",\"group_by\":[\"g\"],\"order_by\":\"g\"",
        "{\"fn\":\"count\",\"alias\":\"c\"}," AR_S("sum","l","s") "," AR_S("sum","n","sn"),
        "[{\"g\":\"a\",\"c\":2,\"s\":18014398509481986,\"sn\":0.3000},"
        "{\"g\":\"b\",\"c\":3,\"s\":1,\"sn\":0.0000},"
        "{\"g\":\"c\",\"c\":1,\"s\":0,\"sn\":0}]", obj);
    /* A6/D5: timestamp group labels and numeric ordering */
    ar_expect(obj, ",\"group_by\":[\"ts\"],\"order_by\":\"ts\"",
        "{\"fn\":\"count\",\"alias\":\"c\"}",
        "[{\"ts\":\"1000\",\"c\":1},{\"ts\":\"2000\",\"c\":1},{\"ts\":\"3000\",\"c\":1},"
        "{\"ts\":\"1490833334666\",\"c\":1},{\"ts\":\"1490833334667\",\"c\":2}]", obj);
    /* A8: avg of no contributing value is null on every JSON route */
    ar_expect(obj, ",\"criteria\":{\"field\":\"ts\",\"op\":\"eq\",\"value\":3000}",
        AR_S("avg","l","a"), "{\"a\":null}", obj);
    /* single-spec extremes: same-field, keyset and walk-fetch-check routes */
    ar_expect(obj, ",\"criteria\":{\"field\":\"l\",\"op\":\"lte\",\"value\":\"9007199254740993\"}",
        AR_S("max","l","m"), "{\"m\":9007199254740993}", obj);
    ar_expect(obj, ",\"criteria\":{\"field\":\"ts\",\"op\":\"lt\",\"value\":1490833334668}",
        AR_S("max","l","m"), "{\"m\":9223372036854775807}", obj);
    ar_expect(obj, ",\"criteria\":{\"field\":\"ts\",\"op\":\"gte\",\"value\":0}",
        AR_S("sum","l","s") "," AR_S("sum","n","sn"),
        "{\"s\":18014398509481987,\"sn\":0.3000}", obj);
    /* intersect keyset extreme: AND of two indexed leaves */
    ar_expect(obj, ",\"criteria\":{\"and\":[{\"field\":\"g\",\"op\":\"eq\",\"value\":\"b\"},"
                   "{\"field\":\"ts\",\"op\":\"lt\",\"value\":1490833334668}]}",
        AR_S("min","l","mn") "," AR_S("max","n","mx"),
        "{\"mn\":-9223372036854775807,\"mx\":1.5000}", obj);
    /* CSV shares the renderer (D3: empty avg is the CSV cell "0") */
    ar_expect(obj, ",\"format\":\"csv\"", AR_S("sum","l","s") "," AR_S("sum","n","sn"),
        "s,sn\n18014398509481987,0.3000", obj);
    ar_expect(obj, ",\"format\":\"csv\",\"criteria\":{\"field\":\"ts\",\"op\":\"eq\",\"value\":3000}",
        AR_S("avg","l","a"), "a\n0", obj);
    /* rows+total wrapper */
    ar_expect(obj, ",\"total\":true", AR_S("sum","l","s"),
        "{\"rows\":{\"s\":18014398509481987},\"total\":1}", obj);
    /* count(field) scan parity on the criteria-bundled route: NULLs excluded */
    ar_expect(obj, ",\"criteria\":{\"field\":\"ts\",\"op\":\"gte\",\"value\":0}",
        "{\"fn\":\"count\",\"field\":\"l\",\"alias\":\"c\"},"
        "{\"fn\":\"count\",\"field\":\"n\",\"alias\":\"cn\"},"
        "{\"fn\":\"count\",\"alias\":\"ct\"}",
        "{\"c\":5,\"cn\":4,\"ct\":6}", obj);
    /* Same field has SUM + COUNT; do not overwrite its walked count. */
    ar_expect(obj, ",\"criteria\":{\"field\":\"ts\",\"op\":\"gte\",\"value\":0}",
        AR_S("sum","l","s") "," AR_S("count","l","c") ","
        AR_S("count","n","cn") ",{\"fn\":\"count\",\"alias\":\"ct\"}",
        "{\"s\":18014398509481987,\"c\":5,\"cn\":4,\"ct\":6}", obj);
}

static int ar_run(void) {
    ar_db = test_get_process_db();
    ASSERT_NOT_NULL(ar_db, "process db");
    if (!ar_db) return 1;
    char *r = ar_req("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);
    ar_make("r3l_i", 8, 1);
    ar_make("r3l_s", 8, 0);
    ar_make("r3l_i64", 64, 1);
    ar_matrix("r3l_i");
    ar_matrix("r3l_s");
    ar_matrix("r3l_i64");
    tu_pdb_drop_object(ar_db, "default", "r3l_i");
    tu_pdb_drop_object(ar_db, "default", "r3l_s");
    tu_pdb_drop_object(ar_db, "default", "r3l_i64");
    return 0;
}
TEST_REGISTER("test-agg-exact-routes", ar_run)

/* Having / order_by on adjacent values above 2^53, long group keys, budget
   and allocation-fault behaviour. */
static void ar_adjacent(const char *obj, int indexed) {
    char req[1024];
    snprintf(req, sizeof req,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\",\"splits\":8,"
        "\"max_key\":16,\"fields\":[\"l:long\",\"w:long\"]%s}", obj,
        indexed ? ",\"indexes\":[\"l\",\"w\"]" : "");
    char *r = ar_req(req); shard_db_free_result(r);
    snprintf(req, sizeof req,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":["
        "{\"key\":\"a\",\"value\":{\"l\":9007199254740992,\"w\":1}},"
        "{\"key\":\"b\",\"value\":{\"l\":9007199254740993,\"w\":2}}]}", obj);
    r = ar_req(req); shard_db_free_result(r);
    ar_expect(obj, ",\"group_by\":[\"w\"],\"order_by\":\"w\","
        "\"having\":{\"field\":\"s\",\"op\":\"eq\",\"value\":\"9007199254740993\"}",
        AR_S("sum","l","s"), "[{\"w\":\"2\",\"s\":9007199254740993}]", obj);
    ar_expect(obj, ",\"group_by\":[\"w\"],\"order_by\":\"s\",\"order\":\"desc\"",
        AR_S("sum","l","s"),
        "[{\"w\":\"2\",\"s\":9007199254740993},{\"w\":\"1\",\"s\":9007199254740992}]", obj);
    ar_expect(obj, ",\"group_by\":[\"l\"],\"order_by\":\"l\",\"order\":\"desc\","
        "\"having\":{\"field\":\"l\",\"op\":\"gt\",\"value\":\"9007199254740992\"}",
        "{\"fn\":\"count\",\"alias\":\"c\"}", "[{\"l\":\"9007199254740993\",\"c\":1}]", obj);
    ar_expect(obj, ",\"group_by\":[\"w\"],\"having\":{\"field\":\"s\",\"op\":\"gt\",\"value\":\"abc\"}",
        AR_S("sum","l","s"),
        "{\"error\":\"invalid numeric value 'abc' for having on 's'\"}", obj);
    tu_pdb_drop_object(ar_db, "default", obj);
}

static void ar_longkey(const char *obj, int indexed) {
    char req[4096], a[700], b[700];
    size_t len = indexed ? 520 : 650; /* indexed payload stays below BT_MAX_VAL_LEN */
    memset(a, 'a', len); a[len] = 0;
    memcpy(b, a, len + 1); b[len - 1] = 'b';
    snprintf(req, sizeof req,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\",\"splits\":8,"
        "\"max_key\":16,\"fields\":[\"v:varchar:2000\"]%s}", obj,
        indexed ? ",\"indexes\":[\"v\"]" : "");
    char *r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "long-key object created"); shard_db_free_result(r);
    snprintf(req, sizeof req,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":["
        "{\"key\":\"p\",\"value\":{\"v\":\"%s\"}},{\"key\":\"q\",\"value\":{\"v\":\"%s\"}}]}",
        obj, a, b);
    r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "long-key rows inserted"); shard_db_free_result(r);
    ar_expect(obj, ",\"group_by\":[\"v\"]", "{\"fn\":\"count\",\"alias\":\"c\"}",
        "{\"error\":\"group_by value exceeds aggregate key limit (511 bytes) or contains NUL\"}", obj);
    tu_pdb_drop_object(ar_db, "default", obj);
}


/* Nine valid 500-byte components overflow only the composite 4096-byte key. */
static void ar_combined_key(void) {
    char req[12000], value[501], groups[256]; size_t o = 0, g = 0;
    memset(value, 'x', 500); value[500] = 0;
    o += (size_t)snprintf(req + o, sizeof req - o,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3combo\","
        "\"splits\":8,\"max_key\":16,\"fields\":[");
    for (int i = 0; i < 9; i++)
        o += (size_t)snprintf(req + o, sizeof req - o, "%s\"v%d:varchar:500\"", i ? "," : "", i);
    snprintf(req + o, sizeof req - o, "]}");
    char *r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "combined-key fixture created"); shard_db_free_result(r);
    o = (size_t)snprintf(req, sizeof req,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"r3combo\",\"key\":\"k\",\"value\":{");
    for (int i = 0; i < 9; i++)
        o += (size_t)snprintf(req + o, sizeof req - o, "%s\"v%d\":\"%s\"", i ? "," : "", i, value);
    snprintf(req + o, sizeof req - o, "}}");
    r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "combined-key row inserted"); shard_db_free_result(r);
    g += (size_t)snprintf(groups + g, sizeof groups - g, ",\"group_by\":[");
    for (int i = 0; i < 9; i++)
        g += (size_t)snprintf(groups + g, sizeof groups - g, "%s\"v%d\"", i ? "," : "", i);
    snprintf(groups + g, sizeof groups - g, "]");
    ar_expect("r3combo", groups, "{\"fn\":\"count\",\"alias\":\"c\"}",
        "{\"error\":\"group_by value exceeds aggregate key limit (511 bytes) or contains NUL\"}",
        "combined key rejects instead of truncating");
    tu_pdb_drop_object(ar_db, "default", "r3combo");
}

static void ar_large_float(const char *obj, int indexed) {
    char req[1024];
    snprintf(req, sizeof req,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":64,\"max_key\":16,\"fields\":[\"d:double\"]%s}",
        obj, indexed ? ",\"indexes\":[\"d\"]" : "");
    char *r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "float fixture created"); shard_db_free_result(r);
    snprintf(req, sizeof req,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":["
        "{\"key\":\"a\",\"value\":{\"d\":1e308}},{\"key\":\"b\",\"value\":{\"d\":1e308}}]}", obj);
    r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "finite float rows inserted"); shard_db_free_result(r);
    ar_expect(obj, "", AR_S("count","d","c"), "{\"c\":2}", "finite float count");
    ar_expect(obj, "", AR_S("min","d","mn") "," AR_S("max","d","mx"),
        "{\"mn\":1e+308,\"mx\":1e+308}", "finite extrema survive unused sum overflow");
    ar_expect(obj, "", AR_S("sum","d","s"),
        "{\"error\":\"aggregate result out of range\"}", "requested overflowing sum errors");
    ar_expect(obj, "", AR_S("avg","d","a"),
        "{\"error\":\"aggregate result out of range\"}", "requested overflowing avg errors");
    tu_pdb_drop_object(ar_db, "default", obj);
}

static void ar_full_text(void) {
    char a[1101], b[1101], req[4096], expected[2300];
    memset(a, 'a', 1100); a[1100] = 0;
    memcpy(b, a, sizeof b); b[1050] = 'b';
    char *r = ar_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3text\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:2000\"]}");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "text fixture created"); shard_db_free_result(r);
    snprintf(req, sizeof req,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"r3text\",\"records\":["
        "{\"key\":\"a\",\"value\":{\"v\":\"%s\"}},{\"key\":\"b\",\"value\":{\"v\":\"%s\"}}]}", a, b);
    r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "full text rows inserted"); shard_db_free_result(r);
    snprintf(expected, sizeof expected, "{\"mn\":\"%s\",\"mx\":\"%s\"}", a, b);
    ar_expect("r3text", "", AR_S("min","v","mn") "," AR_S("max","v","mx"), expected,
        "full text extrema differ beyond byte 1023");
    tu_pdb_drop_object(ar_db, "default", "r3text");
}

/* Weak only during Task-3 legacy RED; Task-4 verification requires this hook. */
extern void agg_test_fail_cells_alloc(int) __attribute__((weak));
extern void agg_test_fail_escape_alloc(int) __attribute__((weak));
static void ar_fault_restore(int *unused) {
    (void)unused;
    agg_exact_test_set_fail_alloc(0);
    if (agg_test_fail_cells_alloc) agg_test_fail_cells_alloc(0);
    if (agg_test_fail_escape_alloc) agg_test_fail_escape_alloc(0);
}

typedef struct { ShardDb *db; size_t cap; } ArBudgetSave;
static void ar_budget_restore(ArBudgetSave *saved) {
    saved->db->query_buffer_max_bytes = saved->cap;
}

static int ar_limits_run(void) {
    int fault_guard __attribute__((cleanup(ar_fault_restore))) = 0;
    ar_db = test_get_process_db();
    ASSERT_NOT_NULL(ar_db, "process db");
    if (!ar_db) return 1;
    char *r = ar_req("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);
    ar_adjacent("r3adj_i", 1);
    ar_adjacent("r3adj_s", 0);
    ar_longkey("r3key_i", 1);
    ar_longkey("r3key_s", 0);
    ar_combined_key();
    ar_large_float("r3float_i", 1);
    ar_large_float("r3float_s", 0);
    ar_full_text();

    /* Bounded memory: 3000 distinct groups under a tiny per-request cap.
       The cap is the process db's own field (g_query_buffer_max_bytes is a
       macro over g_db; shard_db_query binds g_db = db for every request). */
    ArBudgetSave saved_budget __attribute__((cleanup(ar_budget_restore))) = {
        ar_db, ar_db->query_buffer_max_bytes
    };
    r = ar_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3many\","
               "\"splits\":8,\"max_key\":16,\"fields\":[\"l:long\"]}");
    shard_db_free_result(r);
    char *big = malloc(400000); size_t o = 0;
    o += (size_t)snprintf(big + o, 400000 - o,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"r3many\",\"records\":[");
    for (int i = 0; i < 3000; i++)
        o += (size_t)snprintf(big + o, 400000 - o, "%s{\"key\":\"k%d\",\"value\":{\"l\":%d}}",
                              i ? "," : "", i, i);
    snprintf(big + o, 400000 - o, "]}");
    r = ar_req(big); shard_db_free_result(r); free(big);
    ar_db->query_buffer_max_bytes = 8192;
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3many\","
               "\"group_by\":[\"l\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}],\"limit\":5000}");
    ASSERT_TRUE(r && strstr(r, "query memory buffer exceeded") && !strstr(r, "\"c\""),
                "bounded-memory rejection is an error, not a partial success");
    shard_db_free_result(r);
    ar_db->query_buffer_max_bytes = saved_budget.cap;
    tu_pdb_drop_object(ar_db, "default", "r3many");

    /* Allocation faults on the exact render path: error-or-correct, never
       a success-shaped partial response. */
    ar_make("r3flt", 8, 0);
    if (agg_test_fail_cells_alloc) {
        agg_test_fail_cells_alloc(1);
        ar_expect("r3flt", ",\"group_by\":[\"g\"]", "{\"fn\":\"count\",\"alias\":\"c\"}",
            "{\"error\":\"out of memory\"}", "cell allocation fails before output");
        agg_test_fail_cells_alloc(0);
        ar_expect("r3flt", ",\"group_by\":[\"g\"],\"order_by\":\"g\"", "{\"fn\":\"count\",\"alias\":\"c\"}",
            "[{\"g\":\"a\",\"c\":2},{\"g\":\"b\",\"c\":3},{\"g\":\"c\",\"c\":1}]",
            "cell allocation recovers with exact complete response");
    }
    int saw_error = 0, saw_ok = 0;
    for (int n = 1; n <= 12; n++) {
        agg_exact_test_set_fail_alloc(n);
        r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3flt\","
                   "\"aggregates\":[" AR_S("sum","n","sn") "," AR_S("avg","n","an") "]}");
        agg_exact_test_set_fail_alloc(0);
        ASSERT_NOT_NULL(r, "response");
        if (!r) break;
        ar_chomp(r);
        if (strstr(r, "\"error\"")) {
            saw_error = 1;
            ASSERT_TRUE(!strstr(r, "\"sn\""), "error response carries no partial value");
        } else {
            saw_ok = 1;
            ASSERT_EQ_STR(r, "{\"sn\":0.3000,\"an\":0.07500000000000000000}", "success is exact");
        }
        shard_db_free_result(r);
    }
    ASSERT_TRUE(saw_error && saw_ok, "fault loop observed both an error and the exact result");
    tu_pdb_drop_object(ar_db, "default", "r3flt");
    return 0;
}
TEST_REGISTER("test-agg-exact-limits", ar_limits_run)

/* Second blind review: streaming/fallback parity and atomic escaped output. */
static int ar_output_safety(void) {
    int fault_guard __attribute__((cleanup(ar_fault_restore))) = 0;
    ar_db = test_get_process_db(); if (!ar_db) return 1;
    char *r = ar_req("{\"mode\":\"add-dir\",\"dir\":\"default\"}"); shard_db_free_result(r);
    ar_make("r3outputs", 8, 1);
    /* count(field) must decline the one-count streaming state. */
    ar_expect("r3outputs", ",\"group_by\":[\"g\"],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":10",
        AR_S("count","l","c"),
        "[{\"g\":\"b\",\"c\":3},{\"g\":\"a\",\"c\":2},{\"g\":\"c\",\"c\":0}]", "topN nullable count parity");
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3outputs\","
        "\"group_by\":[\"d\"],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":10,\"aggregates\":["
        "{\"fn\":\"count\",\"alias\":\"c\"}," AR_S("avg","d","a") "]}");
    ASSERT_TRUE(r && strstr(r, "{\"d\":null,\"c\":2,\"a\":null}"), "all-NULL avg remains JSON null");
    shard_db_free_result(r);

    r = ar_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3outfloat\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"d:double\"],\"indexes\":[\"d\"]}");
    ASSERT_TRUE(r && !strstr(r,"\"error\""),"overflow object"); shard_db_free_result(r);
    r = ar_req("{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"r3outfloat\",\"records\":["
        "{\"key\":\"a\",\"value\":{\"d\":1e308}},{\"key\":\"b\",\"value\":{\"d\":1e308}}]}");
    ASSERT_TRUE(r && !strstr(r,"\"error\""),"overflow rows"); shard_db_free_result(r);
    ar_expect("r3outfloat", ",\"group_by\":[\"d\"],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":10",
        "{\"fn\":\"count\",\"alias\":\"c\"}," AR_S("sum","d","s"),
        "{\"error\":\"aggregate result out of range\"}","overflow is one error, never heap corruption");
    tu_pdb_drop_object(ar_db,"default","r3outfloat");

    if (agg_test_fail_cells_alloc) {
        agg_test_fail_cells_alloc(1);
        ar_expect("r3outputs", ",\"group_by\":[\"g\"],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":10",
            "{\"fn\":\"count\",\"alias\":\"c\"}","{\"error\":\"out of memory\"}","stream render array OOM frees drained data once");
        agg_test_fail_cells_alloc(0);
    }
    if (agg_test_fail_escape_alloc) {
        for (int stream = 0; stream < 2; stream++) {
            agg_test_fail_escape_alloc(1);
            ar_expect("r3outputs", stream
                ? ",\"group_by\":[\"g\"],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":10"
                : ",\"group_by\":[\"g\"]",
                "{\"fn\":\"count\",\"alias\":\"c\"}","{\"error\":\"out of memory\"}","group escaping OOM precedes any success output");
            agg_test_fail_escape_alloc(0);
        }
    }
    ar_expect("r3outputs", ",\"group_by\":[\"g\"],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":10",
        "{\"fn\":\"count\",\"alias\":\"c\"}",
        "[{\"g\":\"b\",\"c\":3},{\"g\":\"a\",\"c\":2},{\"g\":\"c\",\"c\":1}]","stream succeeds after faults");
    tu_pdb_drop_object(ar_db,"default","r3outputs");

    r = ar_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3outtext\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:2000\"]}");
    ASSERT_TRUE(r && !strstr(r,"\"error\""),"text object"); shard_db_free_result(r);
    r = ar_req("{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"r3outtext\","
        "\"key\":\"a\",\"value\":{\"v\":\"hello\"}}");
    ASSERT_TRUE(r && !strstr(r,"\"error\""),"text row"); shard_db_free_result(r);
    if (agg_test_fail_escape_alloc) {
        agg_test_fail_escape_alloc(1);
        ar_expect("r3outtext","",AR_S("min","v","mn"),"{\"error\":\"out of memory\"}","text extrema escaping fails atomically");
        agg_test_fail_escape_alloc(0);
    }
    ar_expect("r3outtext","",AR_S("min","v","mn"),"{\"mn\":\"hello\"}","full text succeeds after escaping fault");
    tu_pdb_drop_object(ar_db,"default","r3outtext");
    return 0;
}
TEST_REGISTER("test-agg-exact-output-safety", ar_output_safety)

```

**New `src/test/cases/test_agg_packed_routes.c` (complete file):**

```c
/* src/test/cases/test_agg_packed_routes.c
 *
 * R3 private physical packed fixture (public packed binding stays inactive).
 * Hand-built TypedSchema bound with typed_schema_bind_packed; records written
 * through typed_encode_alloc + slotcask_upsert_with_hooks(PKR_FLAGS); index
 * keys inserted with typed_index_record_key_into + btree_idx_insert;
 * aggregates executed through the TEST_BUILD schema seam.
 *
 * Registrations: test-agg-packed-routes-bounded, test-agg-packed-routes-bare,
 * test-agg-packed-routes-extended (bare tie scale-equivalence, large bounded
 * decimals + cancellation, scaled-unit overflow past 2^63, allocation faults,
 * the >=100k-row partition merge, and the bounded-memory rejection).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/typed_row.h"
#include "../db/typed_index.h"
#include "../db/packed_record.h"
#include "../db/query_internal.h"
#include "../db/agg_exact.h"
#include <errno.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

extern int cmd_aggregate_with_schema_for_test(const char *, const char *,
    const Schema *, const FieldSchema *, const char *, const char *,
    const char *, const char *, const char *, int, int, const char *,
    const char *, int);

static int apr_rm(const char *p, const struct stat *s, int k, struct FTW *f) {
    (void)s; (void)k; (void)f; return remove(p);
}
static void apr_restore_root(char (*saved)[PATH_MAX]) { memcpy(g_db_root, *saved, PATH_MAX); }

static char *apr_run(const char *root, const char *obj, const Schema *sch,
                     const FieldSchema *fs, const char *group, const char *aggs,
                     const char *criteria) {
    char *out = NULL; size_t len = 0;
    FILE *m = open_memstream(&out, &len);
    FILE *saved = g_out; g_out = m;
    cmd_aggregate_with_schema_for_test(root, obj, sch, fs, criteria, group, aggs,
                                       NULL, NULL, 0, 0, NULL, NULL, 0);
    g_out = saved; fclose(m);
    if (out) { size_t n = strlen(out); while (n && out[n - 1] == '\n') out[--n] = 0; }
    return out;
}

typedef struct { const char *key, *f0, *f1; } AprRow;

static void apr_object(const char *root, const char *obj, int splits, int indexed,
                       TypedSchema *ts, const char *f0_decl, const AprRow *rows,
                       int nrows, Schema *sch_out, SlotcaskDb **db_out) {
    char path[512];
    snprintf(path, sizeof path, "%s/%s", root, obj);
    ASSERT_EQ_INT(mkdir(path, 0700), 0, "obj dir");
    snprintf(path, sizeof path, "%s/schema.conf", root);
    FILE *f = fopen(path, "a");                      /* one line per object */
    fprintf(f, ":%s:%d:32:2:1\n", obj, splits); fclose(f);
    snprintf(path, sizeof path, "%s/%s/fields.conf", root, obj);
    f = fopen(path, "w"); fprintf(f, "f0:%s\nf1:int\n", f0_decl); fclose(f);
    *sch_out = load_schema(root, obj);
    ASSERT_EQ_INT(sch_out->splits, splits, "schema loads");
    SlotcaskSchemaInfo info = { .splits = sch_out->splits, .slot_size = sch_out->slot_size,
                                .streams = sch_out->streams };
    *db_out = slotcask_registry_get(root, obj, &info);
    ASSERT_NOT_NULL(*db_out, "slotcask open");
    if (indexed) {
        snprintf(path, sizeof path, "%s/%s/indexes", root, obj); mkdir(path, 0700);
        snprintf(path, sizeof path, "%s/%s/indexes/f0", root, obj); mkdir(path, 0700);
        snprintf(path, sizeof path, "%s/%s/indexes/f1", root, obj); mkdir(path, 0700);
        snprintf(path, sizeof path, "%s/%s/indexes/index.conf", root, obj);
        f = fopen(path, "w"); fputs("f0\nf1\n", f); fclose(f);
        invalidate_idx_cache(root, obj);
    }
    for (int i = 0; i < nrows; i++) {
        char json[4096];
        snprintf(json, sizeof json, "{\"f0\":%s,\"f1\":%s}", rows[i].f0, rows[i].f1);
        uint8_t *value = NULL; size_t n = 0;
        ASSERT_EQ_INT(typed_encode_alloc(ts, json, 0, NULL, NULL, &value, &n, NULL, 0), 0, "encode");
        SlotcaskUpsertOpts opts = { .record_hdr2 = PKR_FLAGS };
        SlotcaskUpsertResult res;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(*db_out, 0, rows[i].key, strlen(rows[i].key),
                                                 value, n, &opts, &res), 0, "store");
        if (indexed) {
            uint8_t hash[16];
            compute_hash_raw(rows[i].key, strlen(rows[i].key), hash);
            for (int fi = 0; fi < 2; fi++) {
                uint8_t enc[544]; size_t el = 0;
                int have = typed_index_record_key_into(ts, value, n, PKR_FLAGS,
                                                       fi ? "f1" : "f0", enc, sizeof enc, &el);
                ASSERT_TRUE(have == 1, "key encodes");
                ASSERT_EQ_INT(btree_idx_insert(root, obj, fi ? "f1" : "f0", sch_out->splits,
                                               (const char *)enc, el, hash), 0, "index insert");
            }
        }
        free(value);
    }
}

static TypedSchema *apr_schema(const char *f0_decl) {
    const char *types[2] = { f0_decl, "int" };
    TypedSchema *ts = calloc(1, sizeof *ts);
    ASSERT_NOT_NULL(ts, "private schema");
    if (!ts) return NULL;
    ts->typed = 1; ts->nfields = 2;
    if (!strncmp(f0_decl, "varchar:", 8)) parse_field_type(f0_decl, &ts->fields[0]);
    else ts->fields[0].type = FT_NUMERIC;
    parse_field_type("int", &ts->fields[1]);
    snprintf(ts->fields[0].name, 256, "f0"); ts->fields[0].name_len = 2;
    snprintf(ts->fields[1].name, 256, "f1"); ts->fields[1].name_len = 2;
    ASSERT_EQ_INT(typed_schema_bind_packed(ts, types, 2, NULL, 0), 0, "bind");
    return ts;
}

static void apr_teardown_schema(TypedSchema *ts) {
    for (int i = 0; i < ts->nfields; i++) free_enum_values(&ts->fields[i]);
    free(ts);
}

/* ---- core matrix: 3 layouts x (scalars, packed-int scalars, group labels,
   criteria sum). Oracle-checked literals: 0,1,2,NULL dataset. ---- */
static const AprRow APR_ROWS[] = {
    { "zero", "0", "3" }, { "one", "1", "2" }, { "two", "2", "1" },
    { "null", "null", "0" },
};

static int apr_case(int bare) {
    char saved_root[PATH_MAX] __attribute__((cleanup(apr_restore_root)));
    memcpy(saved_root, g_db_root, PATH_MAX);
    const char *f0_type = bare ? "numeric" : "numeric:19,4";
    TypedSchema *ts = apr_schema(f0_type);
    if (!ts) return 1;

    char root[] = "/tmp/shard-agg-packed-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(root), "root");
    snprintf(g_db_root, PATH_MAX, "%s", root);
    struct { const char *name; int splits, indexed; } layouts[] = {
        { "pi", 8, 1 }, { "ps", 8, 0 }, { "p16", 16, 1 },
    };
    for (int li = 0; li < 3; li++) {
        Schema sch; SlotcaskDb *db = NULL;
        apr_object(root, layouts[li].name, layouts[li].splits, layouts[li].indexed, ts,
                   f0_type, APR_ROWS, 4, &sch, &db);
        FieldSchema fs = { .ts = ts };
        const char *obj = layouts[li].name;
        char *o;
        /* A1: min/max/sum/avg of the packed numeric (index shortcut when
           bounded, record-fed route when bare — D1 gate). */
        o = apr_run(root, obj, &sch, &fs, NULL,
            "[{\"fn\":\"min\",\"field\":\"f0\",\"alias\":\"mn\"},{\"fn\":\"max\",\"field\":\"f0\",\"alias\":\"mx\"},"
            "{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"},"
            "{\"fn\":\"count\",\"field\":\"f0\",\"alias\":\"c\"}]", NULL);
        ASSERT_EQ_STR(o, bare
            ? "{\"mn\":0,\"mx\":2,\"s\":3,\"a\":1.00000000000000000000,\"c\":3}"
            : "{\"mn\":0.0000,\"mx\":2.0000,\"s\":3.0000,\"a\":1.00000000000000000000,\"c\":3}",
            "packed numeric scalars");
        free(o);
        /* A9: packed fixed int through the record-fed accumulator */
        o = apr_run(root, obj, &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f1\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"f1\",\"alias\":\"a\"}]", NULL);
        ASSERT_EQ_STR(o, "{\"s\":6,\"a\":1.5000000000000000}", "packed int scalars");
        free(o);
        /* A2: group labels (value and NULL groups), indexed and scan parity */
        o = apr_run(root, obj, &sch, &fs, "[\"f0\"]",
            "[{\"fn\":\"count\",\"alias\":\"c\"}]", NULL);
        const char *l0 = bare ? "{\"f0\":\"0\",\"c\":1}" : "{\"f0\":\"0.0000\",\"c\":1}";
        const char *l1 = bare ? "{\"f0\":\"1\",\"c\":1}" : "{\"f0\":\"1.0000\",\"c\":1}";
        const char *l2 = bare ? "{\"f0\":\"2\",\"c\":1}" : "{\"f0\":\"2.0000\",\"c\":1}";
        ASSERT_TRUE(o && strstr(o, l0) && strstr(o, l1) && strstr(o, l2) &&
                    strstr(o, "{\"f0\":null,\"c\":1}") && !strstr(o, "\"f0\":\"\""),
                    "four labelled groups (value + NULL), none unlabeled");
        free(o);
        /* criteria-bearing aggregate over the packed rows (scan or candidate) */
        o = apr_run(root, obj, &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"}]",
            "{\"field\":\"f1\",\"op\":\"gte\",\"value\":2}");
        ASSERT_EQ_STR(o, bare ? "{\"s\":1}" : "{\"s\":1.0000}", "criteria sum");
        free(o);
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, obj);
        invalidate_idx_cache(root, obj);
    }
    apr_teardown_schema(ts);
    memcpy(g_db_root, saved_root, PATH_MAX);
    ASSERT_EQ_INT(nftw(root, apr_rm, 16, FTW_DEPTH | FTW_PHYS), 0, "fixture removed");
    return 0;
}
static int apr_bounded(void) { return apr_case(0); }
static int apr_bare(void) { return apr_case(1); }
TEST_REGISTER("test-agg-packed-routes-bounded", apr_bounded)
TEST_REGISTER("test-agg-packed-routes-bare", apr_bare)

/* ---- extended: (a) bare tie scale-equivalence; (b) large bounded decimals
   + cancellation; (c) scaled-unit overflow past 2^63; (d) allocation faults;
   (e) >=100k-row partition merge; (f) bounded-memory rejection. ---- */

typedef struct { ShardDb *db; size_t cap; } AprBudgetSave;
static void apr_budget_restore(AprBudgetSave *saved) {
    saved->db->query_buffer_max_bytes = saved->cap;
}

static void apr_fault_restore(int *unused) {
    (void)unused; agg_exact_test_set_fail_alloc(0);
}

static int apr_extended(void) {
    int fault_guard __attribute__((cleanup(apr_fault_restore))) = 0;
    char saved_root[PATH_MAX] __attribute__((cleanup(apr_restore_root)));
    memcpy(saved_root, g_db_root, PATH_MAX);
    char root[] = "/tmp/shard-agg-packed-ext-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(root), "root");
    snprintf(g_db_root, PATH_MAX, "%s", root);
    Schema sch; SlotcaskDb *db = NULL; FieldSchema fs; char *o;

    /* (a) bare tie: rows 1, 1.00, 1.0 — one canonical group, larger-scale
       min/max ties, sum at the largest operand scale (oracle goldens). */
    {
        static const AprRow rows[] = {
            { "t1", "1", "1" }, { "t2", "1.00", "1" }, { "t3", "1.0", "1" },
        };
        TypedSchema *ts = apr_schema("numeric");
        ASSERT_NOT_NULL(ts, "tie schema"); if (!ts) return 1;
        apr_object(root, "tie", 8, 1, ts, "numeric", rows, 3, &sch, &db);
        fs.ts = ts;
        o = apr_run(root, "tie", &sch, &fs, NULL,
            "[{\"fn\":\"min\",\"field\":\"f0\",\"alias\":\"mn\"},{\"fn\":\"max\",\"field\":\"f0\",\"alias\":\"mx\"},"
            "{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"},"
            "{\"fn\":\"count\",\"field\":\"f0\",\"alias\":\"c\"}]", NULL);
        ASSERT_EQ_STR(o, "{\"mn\":1.00,\"mx\":1.00,\"s\":3.00,"
                         "\"a\":1.00000000000000000000,\"c\":3}", "tie scalars");
        free(o);
        o = apr_run(root, "tie", &sch, &fs, "[\"f0\"]",
            "[{\"fn\":\"count\",\"alias\":\"c\"}]", NULL);
        ASSERT_EQ_STR(o, "[{\"f0\":\"1\",\"c\":3}]", "one canonical bare group");
        free(o);
        /* scale-equivalent keys on the index route form one group too */
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "tie");
        invalidate_idx_cache(root, "tie");
        apr_teardown_schema(ts);
    }

    /* D8: full packed varchar spans, differing after byte 1023. */
    {
        char a[1101], b[1101], ja[1103], jb[1103], expected[2300];
        memset(a, 'a', 1100); a[1100] = 0;
        memcpy(b, a, sizeof b); b[1050] = 'b';
        snprintf(ja, sizeof ja, "\"%s\"", a);
        snprintf(jb, sizeof jb, "\"%s\"", b);
        AprRow rows[] = { {"a", ja, "1"}, {"b", jb, "1"} };
        TypedSchema *ts = apr_schema("varchar:2000");
        ASSERT_NOT_NULL(ts, "packed text schema"); if (!ts) return 1;
        apr_object(root, "text", 8, 0, ts, "varchar:2000", rows, 2, &sch, &db);
        fs.ts = ts;
        snprintf(expected, sizeof expected, "{\"mn\":\"%s\",\"mx\":\"%s\"}", a, b);
        o = apr_run(root, "text", &sch, &fs, NULL,
            "[{\"fn\":\"min\",\"field\":\"f0\",\"alias\":\"mn\"},"
            "{\"fn\":\"max\",\"field\":\"f0\",\"alias\":\"mx\"}]", NULL);
        ASSERT_EQ_STR(o, expected, "full packed text extrema"); free(o);
        slotcask_registry_put(db); slotcask_registry_invalidate(root, "text");
        invalidate_idx_cache(root, "text"); apr_teardown_schema(ts);
    }

    /* (b)+(c) bounded numeric:38,4 — large values, cancellation tail, and a
       sum crossing 2^63 scaled units (checked __int128, no double). */
    {
        static const AprRow rows[] = {
            { "g1", "12345678901234567890.1234", "1" },
            { "g2", "0.0001", "1" }, { "g3", "-0.0001", "1" },
            { "g4", "999999999999999.9999", "1" },
            { "g5", "-999999999999999.9999", "1" },
            { "h1", "500000000000000.0000", "2" },
            { "h2", "500000000000000.0000", "2" },
        };
        TypedSchema *ts = apr_schema("numeric:38,4");
        ASSERT_NOT_NULL(ts, "large schema"); if (!ts) return 1;
        apr_object(root, "large", 8, 1, ts, "numeric:38,4", rows, 7, &sch, &db);
        fs.ts = ts;
        o = apr_run(root, "large", &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"},"
            "{\"fn\":\"min\",\"field\":\"f0\",\"alias\":\"mn\"},{\"fn\":\"max\",\"field\":\"f0\",\"alias\":\"mx\"}]",
            "{\"field\":\"f1\",\"op\":\"eq\",\"value\":1}");
        ASSERT_EQ_STR(o, "{\"s\":12345678901234567890.1234,"
                         "\"a\":2469135780246913578.0247,"
                         "\"mn\":-999999999999999.9999,"
                         "\"mx\":12345678901234567890.1234}", "large + cancellation");
        free(o);
        o = apr_run(root, "large", &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"}]",
            "{\"field\":\"f1\",\"op\":\"eq\",\"value\":2}");
        ASSERT_EQ_STR(o, "{\"s\":1000000000000000.0000,\"a\":500000000000000.0000}",
                      "scaled-unit sum past 2^63");
        free(o);
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "large");
        invalidate_idx_cache(root, "large");
        apr_teardown_schema(ts);
    }

    /* (d) allocation faults on the packed render path: error or exact. */
    {
        TypedSchema *ts = apr_schema("numeric:19,4");
        ASSERT_NOT_NULL(ts, "fault schema"); if (!ts) return 1;
        apr_object(root, "flt", 8, 0, ts, "numeric:19,4", APR_ROWS, 4, &sch, &db);
        fs.ts = ts;
        int saw_error = 0, saw_ok = 0;
        for (int n = 1; n <= 32; n++) {
            agg_exact_test_set_fail_alloc(n);
            o = apr_run(root, "flt", &sch, &fs, NULL,
                "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},"
                "{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"}]", NULL);
            agg_exact_test_set_fail_alloc(0);
            ASSERT_NOT_NULL(o, "fault response");
            if (!o) break;
            if (strstr(o, "\"error\"")) {
                saw_error = 1;
                ASSERT_TRUE(!strstr(o, "\"s\""), "error response carries no partial value");
            } else {
                saw_ok = 1;
                ASSERT_EQ_STR(o, "{\"s\":3.0000,\"a\":1.00000000000000000000}", "exact after faults");
            }
            free(o);
        }
        /* Parallel scan/merge allocations vary with worker layout. The
           sweep proves errors are explicit; a disabled-fault call proves
           exact success without guessing how many allocations it takes. */
        agg_exact_test_set_fail_alloc(0);
        o = apr_run(root, "flt", &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"},"
            "{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"}]", NULL);
        ASSERT_EQ_STR(o, "{\"s\":3.0000,\"a\":1.00000000000000000000}", "unfaulted exact success");
        if (o && !strstr(o, "\"error\"")) saw_ok = 1;
        free(o);
        ASSERT_TRUE(saw_error && saw_ok, "fault injection errors and unfaulted exact success");
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "flt");
        invalidate_idx_cache(root, "flt");
        apr_teardown_schema(ts);
    }

    /* (f) bounded memory: tiny cap rejects the bucket allocation explicitly. */
    {
        TypedSchema *ts = apr_schema("numeric:19,4");
        ASSERT_NOT_NULL(ts, "budget schema"); if (!ts) return 1;
        apr_object(root, "bud", 8, 1, ts, "numeric:19,4", APR_ROWS, 4, &sch, &db);
        fs.ts = ts;
        ShardDb *pdb = test_get_process_db();
        AprBudgetSave saved_budget __attribute__((cleanup(apr_budget_restore))) = {
            pdb, pdb->query_buffer_max_bytes
        };
        pdb->query_buffer_max_bytes = 1024;
        o = apr_run(root, "bud", &sch, &fs, "[\"f0\"]",
            "[{\"fn\":\"count\",\"alias\":\"c\"},{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"}]", NULL);
        pdb->query_buffer_max_bytes = saved_budget.cap;
        ASSERT_NOT_NULL(o, "budget response");
        ASSERT_TRUE(o && strstr(o, "query memory buffer exceeded") && !strstr(o, "\"c\""),
                    "budget rejection is an error, not a partial success");
        free(o);
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "bud");
        invalidate_idx_cache(root, "bud");
        apr_teardown_schema(ts);
    }

    /* (e) partition merge: >=100k rows at splits 64 (8 idx shards) forces the
       parallel IGB scatter + per-partition merge. Groups must be identical to
       the serial layouts and sum exactly. */
    {
        static const AprRow few[] = { { "seed", "null", "null" } };
        TypedSchema *ts = apr_schema("numeric");
        ASSERT_NOT_NULL(ts, "partition schema"); if (!ts) return 1;
        apr_object(root, "part", 64, 1, ts, "numeric", few, 1, &sch, &db);
        fs.ts = ts;
        for (int i = 0; i < 120000; i++) {
            char json[64], key[16];
            snprintf(json, sizeof json, "{\"f0\":%d,\"f1\":1}", i % 3);
            snprintf(key, sizeof key, "p%d", i);
            uint8_t *value = NULL; size_t n = 0;
            ASSERT_EQ_INT(typed_encode_alloc(ts, json, 0, NULL, NULL, &value, &n, NULL, 0), 0, "bulk encode");
            SlotcaskUpsertOpts opts = { .record_hdr2 = PKR_FLAGS };
            SlotcaskUpsertResult res;
            ASSERT_EQ_INT(slotcask_upsert_with_hooks(db, 0, key, strlen(key),
                                                     value, n, &opts, &res), 0, "bulk store");
            uint8_t hash[16];
            compute_hash_raw(key, strlen(key), hash);
            for (int fi = 0; fi < 2; fi++) {
                uint8_t enc[544]; size_t el = 0;
                ASSERT_EQ_INT(typed_index_record_key_into(ts, value, n, PKR_FLAGS,
                                                          fi ? "f1" : "f0", enc,
                                                          sizeof enc, &el), 1, "bulk key");
                ASSERT_EQ_INT(btree_idx_insert(root, "part", fi ? "f1" : "f0",
                                               sch.splits, (const char *)enc, el,
                                               hash), 0, "bulk index");
            }
            free(value);
        }
        o = apr_run(root, "part", &sch, &fs, "[\"f0\"]",
            "[{\"fn\":\"count\",\"alias\":\"c\"},{\"fn\":\"sum\",\"field\":\"f1\",\"alias\":\"s\"}]",
            NULL);
        ASSERT_NOT_NULL(o, "partition response");
        ASSERT_TRUE(o && strstr(o, "{\"f0\":\"0\",\"c\":40000,\"s\":40000}") &&
                    strstr(o, "{\"f0\":\"1\",\"c\":40000,\"s\":40000}") &&
                    strstr(o, "{\"f0\":\"2\",\"c\":40000,\"s\":40000}") &&
                    strstr(o, "{\"f0\":null,\"c\":1,\"s\":0}"),
                    "partition merge folds every worker exactly including NULL seed");
        free(o);
        o = apr_run(root, "part", &sch, &fs, NULL,
            "[{\"fn\":\"sum\",\"field\":\"f1\",\"alias\":\"s\"},{\"fn\":\"avg\",\"field\":\"f0\",\"alias\":\"a\"}]",
            NULL);
        ASSERT_EQ_STR(o, "{\"s\":120000,\"a\":1.00000000000000000000}", "partition scalar totals");
        free(o);
        slotcask_registry_put(db);
        slotcask_registry_invalidate(root, "part");
        invalidate_idx_cache(root, "part");
        apr_teardown_schema(ts);
    }

    memcpy(g_db_root, saved_root, PATH_MAX);
    ASSERT_EQ_INT(nftw(root, apr_rm, 16, FTW_DEPTH | FTW_PHYS), 0, "extended fixture removed");
    return 0;
}
TEST_REGISTER("test-agg-packed-routes-extended", apr_extended)

static char *apr_ordered(const char *root, const char *obj, const Schema *sch,
                         const FieldSchema *fs, const char *group) {
    char *out = NULL; size_t len = 0;
    FILE *m = open_memstream(&out, &len), *saved = g_out; g_out = m;
    cmd_aggregate_with_schema_for_test(root,obj,sch,fs,NULL,group,
        "[{\"fn\":\"count\",\"alias\":\"c\"}]",NULL,"c",1,10,NULL,NULL,0);
    g_out = saved; fclose(m); return out;
}
static int apr_output_safety(void) {
    char saved_root[PATH_MAX] __attribute__((cleanup(apr_restore_root)));
    memcpy(saved_root,g_db_root,PATH_MAX);
    char root[]="/tmp/shard-agg-output-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(root),"root"); snprintf(g_db_root,PATH_MAX,"%s",root);
    static const AprRow rows[] = { {"a","0","3"},{"b","1","2"},{"c","2","1"},{"n","null","null"} };
    for (int bare = 0; bare < 2; bare++) {
        const char *decl = bare ? "numeric" : "numeric:19,4", *obj = bare ? "bare" : "bounded";
        TypedSchema *ts=apr_schema(decl); Schema sch; SlotcaskDb *db=NULL;
        apr_object(root,obj,8,1,ts,decl,rows,4,&sch,&db); FieldSchema fs={.ts=ts};
        char *out=apr_ordered(root,obj,&sch,&fs,"[\"f0\"]");
        ASSERT_TRUE(out && strstr(out,bare ? "\"f0\":\"0\"" : "\"f0\":\"0.0000\"") &&
            strstr(out,bare ? "\"f0\":\"1\"" : "\"f0\":\"1.0000\"") &&
            strstr(out,bare ? "\"f0\":\"2\"" : "\"f0\":\"2.0000\"") &&
            strstr(out,"\"f0\":null") && !strstr(out,"\"f0\":\"\""),"packed count(*) topN labels exact"); free(out);
        out=apr_ordered(root,obj,&sch,&fs,"[\"f1\"]");
        ASSERT_TRUE(out && strstr(out,"{\"f1\":null,\"c\":1}"),"fixed-width NULL tier is a NULL group"); free(out);
        slotcask_registry_put(db);slotcask_registry_invalidate(root,obj);invalidate_idx_cache(root,obj);apr_teardown_schema(ts);
    }
    TypedSchema *ts=apr_schema("numeric"); Schema sch; SlotcaskDb *db=NULL;
    apr_object(root,"budget",8,0,ts,"numeric",NULL,0,&sch,&db); FieldSchema fs={.ts=ts};
    char *json=malloc(17000), *literal=malloc(16400);
    ASSERT_NOT_NULL(json,"json buffer"); ASSERT_NOT_NULL(literal,"literal buffer");
    if (!json || !literal) { free(json);free(literal);return 1; }
    literal[0]='1';literal[1]='.';memset(literal+2,'0',16383);literal[16385]=0;
    for (int i=0;i<200;i++) {
        char key[32];snprintf(key,sizeof key,"k%d",i);
        snprintf(json,17000,"{\"f0\":%s,\"f1\":%d}",literal,i);
        uint8_t *value=NULL;size_t len=0;
        ASSERT_EQ_INT(typed_encode_alloc(ts,json,0,NULL,NULL,&value,&len,NULL,0),0,"large-scale encode");
        SlotcaskUpsertOpts opts={.record_hdr2=PKR_FLAGS};SlotcaskUpsertResult res;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(db,0,key,strlen(key),value,len,&opts,&res),0,"large-scale row");free(value);
    }
    free(json);
    ShardDb *pdb=test_get_process_db();
    AprBudgetSave saved __attribute__((cleanup(apr_budget_restore)))={pdb,pdb->query_buffer_max_bytes};
    pdb->query_buffer_max_bytes=1024*1024;
    char *out=apr_run(root,"budget",&sch,&fs,"[\"f1\"]",
        "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"}]",NULL);
    ASSERT_EQ_STR(out,"{\"error\":\"query memory buffer exceeded; narrow criteria, add limit/offset, or stream via fetch+cursor\"}",
        "3MB retained lexemes reject a 1MiB budget before output");free(out);
    pdb->query_buffer_max_bytes=8*1024*1024;
    out=apr_run(root,"budget",&sch,&fs,"[\"f1\"]",
        "[{\"fn\":\"sum\",\"field\":\"f0\",\"alias\":\"s\"}]",NULL);
    ASSERT_NOT_NULL(out,"large rendered success");
    int seen=0;const char *pos=out;
    while (pos && (pos=strstr(pos,"\"s\":"))) {
        pos+=4;
        ASSERT_TRUE(!strncmp(pos,literal,strlen(literal)) && pos[strlen(literal)]=='}',"every retained decimal keeps full display scale");
        seen++;pos+=strlen(literal);
    }
    ASSERT_EQ_INT(seen,200,"all 200 exact groups survive with sufficient budget");
    free(out);free(literal);pdb->query_buffer_max_bytes=saved.cap;
    slotcask_registry_put(db);slotcask_registry_invalidate(root,"budget");invalidate_idx_cache(root,"budget");apr_teardown_schema(ts);
    memcpy(g_db_root,saved_root,PATH_MAX);
    ASSERT_EQ_INT(nftw(root,apr_rm,16,FTW_DEPTH|FTW_PHYS),0,"output fixtures removed");
    return 0;
}
TEST_REGISTER("test-agg-packed-output-safety",apr_output_safety)

```

## 7.2 Oracle and vector generator (run outside the repo, never committed)

**`agg_oracle.py` (complete):**

```python
#!/usr/bin/env python3
"""Independent exact oracle for R3 aggregates (PostgreSQL 18 avg policy).

Uses only Python's decimal module with a context wide enough that every
operation below is exact; shares no code with src/db/decnum.c.
"""
import sys
from decimal import Decimal, localcontext, ROUND_HALF_UP

PREC = 600


def limb_info(d):
    """(weight, first limb) of |d| in PostgreSQL base-10000 form."""
    if d == 0:
        return 0, 0
    a = abs(d)
    weight = a.adjusted() // 4
    with localcontext() as c:
        c.prec = PREC
        first = int(a / (Decimal(10000) ** weight))
    return weight, first


def dscale_of(d):
    e = d.as_tuple().exponent
    return max(0, -e)


def select_div_scale(sum_, count):
    w1, f1 = limb_info(sum_)
    w2, f2 = limb_info(Decimal(count))
    qweight = w1 - w2
    if f1 <= f2:
        qweight -= 1
    rscale = 16 - qweight * 4
    rscale = max(rscale, dscale_of(sum_), 0)
    return min(rscale, 1000)


def exact_sum(vals):
    with localcontext() as c:
        c.prec = PREC
        s = Decimal(0)
        for v in vals:
            s += v
        return s


def avg_text(vals):
    """vals: list of Decimal (display scale carried by the exponent)."""
    n = len(vals)
    if n == 0:
        return None
    s = exact_sum(vals)
    # sum keeps the largest operand display scale
    ds = max(dscale_of(v) for v in vals)
    with localcontext() as c:
        c.prec = PREC
        s = s.quantize(Decimal(1).scaleb(-ds)) if ds else s
    rscale = select_div_scale(s, n)
    with localcontext() as c:
        c.prec = PREC
        c.rounding = ROUND_HALF_UP  # ties away from zero on the magnitude
        q = (s / Decimal(n)).quantize(Decimal(1).scaleb(-rscale))
    return format(q, "f")


def sum_text(vals, min_scale=0):
    s = exact_sum(vals)
    ds = max([dscale_of(v) for v in vals] + [min_scale])
    with localcontext() as c:
        c.prec = PREC
        return format(s.quantize(Decimal(1).scaleb(-ds)), "f")


def minmax_text(vals, fn):
    best = None
    for v in vals:
        if best is None:
            best = v
            continue
        if (fn == "min" and v < best) or (fn == "max" and v > best):
            best = v
        elif v == best and dscale_of(v) > dscale_of(best):
            best = v  # tie: larger display scale (deterministic across layouts)
    return None if best is None else format(best, "f")


if __name__ == "__main__":
    cases = {
        "two_2p53p1_longs": [Decimal("9007199254740993")] * 2,
        "ts_third": [Decimal(1490833334666), Decimal(1490833334667), Decimal(1490833334667)],
        "dec_tail": [Decimal("0.1000"), Decimal("0.2000")],
        "cancel": [Decimal("9223372036854775807"), Decimal("9223372036854775807"),
                   Decimal("-9223372036854775807"), Decimal("-9223372036854775807"),
                   Decimal("1")],
        "bare_mixed_scale": [Decimal("1.0"), Decimal("2.00"), Decimal("0.5")],
        "small_avg": [Decimal("0.0000001"), Decimal("0.0000002")],
        "ties_away": [Decimal("1"), Decimal("2")],
    }
    for k, v in cases.items():
        print(k, "sum=", sum_text(v), "avg=", avg_text(v),
              "min=", minmax_text(v, "min"), "max=", minmax_text(v, "max"))
```

**`gen_agg_vectors.py` (complete):**

```python
import random, sys
from decimal import Decimal as D, getcontext
import agg_oracle as o
getcontext().prec = 600     # every construction/quantize below is exact

rnd = random.Random(20261030)
cases = []          # (cls, scale, [text values])

def add(cls, scale, vals): cases.append((cls, scale, vals))

# hand-written (the plan's literal goldens)
add('I', 0, ["9007199254740993", "9007199254740993"])
add('I', 0, ["9223372036854775807", "9223372036854775807",
             "-9223372036854775807", "-9223372036854775807", "1"])
add('I', 0, ["1490833334666", "1490833334667", "1490833334667"])
add('I', 4, ["0.1000", "0.2000"])
add('I', 4, ["1.5000", "-1.5000"])
add('I', 0, ["-0", "0", "-1", "1"])
add('D', 0, ["1.0", "2.00", "0.5"])
add('D', 0, ["1", "1.00", "1.0"])
add('D', 4, ["0.0000", "1.0000", "2.0000"])
add('D', 0, ["12345678901234567890.1234567890", "0.0000000001", "-0.0000000001"])
add('D', 0, ["0.0000001", "0.0000002"])
add('D', 0, ["9007199254740993.000000000000000001"] * 3)
add('D', 0, ["99999999999999999999999999999999999999.9999",
             "0.0001", "-99999999999999999999999999999999999999.9999"])

def rint(): return rnd.choice([rnd.randint(-10**6, 10**6),
                               rnd.randint(-2**63 + 1, 2**63 - 1),
                               2**53 + rnd.randint(-3, 3)])
for _ in range(120):
    n = rnd.randint(1, 12)
    scale = rnd.choice([0, 0, 2, 4])
    vals = []
    for _ in range(n):
        r = rint()
        s = str(abs(r)).rjust(scale + 1, '0')
        t = (s[:-scale] + '.' + s[-scale:]) if scale else s
        vals.append(('-' if r < 0 and r != 0 else '') + t)
    add('I', scale, vals)
for _ in range(150):
    n = rnd.randint(1, 12)
    bounded = rnd.random() < 0.4
    sc = 4 if bounded else 0
    vals = []
    for _ in range(n):
        ip = rnd.randint(0, 10**rnd.randint(0, 38))
        fs = sc if bounded else rnd.randint(0, 30)
        fp = ''.join(rnd.choice('0123456789') for _ in range(fs))
        t = str(ip) + ('.' + fp if fs else '')
        if rnd.random() < 0.4 and (ip or fp.strip('0')): t = '-' + t
        vals.append(t)
    add('D', sc, vals)

def emit(cls, scale, vals):
    dv = [D(v) for v in vals]
    if cls == 'I':
        # integer-class results are rendered at the column scale
        q = D(1).scaleb(-scale)
        sm = format(o.exact_sum(dv).quantize(q), 'f') if scale else str(int(o.exact_sum(dv)))
        mn = format(min(dv).quantize(q), 'f') if scale else str(int(min(dv)))
        mx = format(max(dv).quantize(q), 'f') if scale else str(int(max(dv)))
        ints = [(str(int(v.scaleb(scale))) if scale else str(int(v))) for v in dv]
        av = o.avg_text([D(x).scaleb(-scale).quantize(D(1).scaleb(-scale)) if scale else D(x) for x in ints])
        shown = vals
    else:
        sm, av = o.sum_text(dv, scale), o.avg_text(dv)
        mn, mx = o.minmax_text(dv, 'min'), o.minmax_text(dv, 'max')
        shown = vals
    q = lambda s: '"' + s + '"'
    return ('    {\'%s\', %d, %d, {%s}, %s, %s, %s, %s},' %
            (cls, scale, len(shown), ', '.join(q(v) for v in shown),
             q(sm), q(av), q(mn), q(mx)))

print('/* GENERATED by gen_agg_vectors.py (seed 20261030) from the independent')
print('   Python-decimal oracle. Do not edit by hand. */')
print('static const struct { char cls; int scale, n; const char *v[12];')
print('                      const char *sum, *avg, *mn, *mx; } AGG_VECS[] = {')
for c in cases: print(emit(*c))
print('};')
```
