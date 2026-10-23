# Phase 2 child plan — exact decimal core and independent numerical oracle

Status: **implemented and raw-diff reviewed, including both post-review fix
rounds (2026-10-23).** The exact decimal core passes 929 focused checks;
the full suite records 18445 passed, 0 failed across 516 cases. The final
independent blind and spec re-reviews report no actionable findings. See
[the final re-review](2026-10-23-phase2-rounding-rereview.md) and
[execution evidence](2026-10-22-phase2-core-evidence.md).

This document preserves the historical approved task sequence, halt/resume
instructions and amendments; those checkpoint instructions are no longer
current and must not be replayed on the completed tree. The final executed
revision had SHA256
`fb58c61f8295d50aa82fafb5c0127205a9c317e01e8638cb8a68ac1d646ff49a`.
Only this status preamble was updated during commit preparation; the code
blocks and historical evidence remain unchanged. Phase 3 needs its own child
plan and approval.

**Post-review fix round 2 (2026-10-23), after the fresh blind review of the
completed implementation:** one further P2 in the plan's public
`dec_round_scale` — the same class as RAW-01 but on the direct rounding
boundary. (a) Rounding a maximum-sized value (131072 integer nines plus a
fractional nine) at scale 0 carries into a 131073rd integer digit and
returns success; the value packs but the module's own decoder rejects it.
(b) `dec_round_scale(v, 16384)` succeeds with display scale 16384, outside
the frozen 16383 display-scale domain; it packs and fails decoding. The fix
bounds `|scale| <= DEC_MAX_DSCALE` (`DEC_ERR_RANGE`, output left as the
canonical zero, matching the module's tested validation-failure behavior)
and re-checks the integer limit after the rounding carry (`DEC_ERR_RANGE`,
canonical zero). Constrained assignment is preserved:
`dec_apply_precision` translates that range failure to
`DEC_ERR_PRECISION` — with `maxdigits <= 2000` for every declared (P,S),
a rounded value past the bare integer limit is always a precision
overflow — so no existing constrained-assignment status changes, and all
negative-scale behavior (including documented negative display scales that
callers clamp) is unchanged. The header contract is updated accordingly.
Regressions for both boundaries plus the precision-preservation guard rail
are added in **Post-review fix round 2 — public rounding boundary**, captured
RED before the production edits, then re-verified through the focused case,
the ordinary oracle, the allocation-failure probe and the full suite.
Approved by the human 2026-10-23 with execution authorized in the same
instruction.

**Post-review fix round (2026-10-23), after the completed-implementation
raw-diff review [2026-10-23-phase2-raw-diff-review.md](2026-10-23-phase2-raw-diff-review.md):**
the review confirmed two P2 defects that exist in this plan's own approved
code; the executor followed the plan correctly and the 888 passing checks
missed both boundaries. RAW-01/SPEC-01 — `dec_div_avg` returned a rounded
average of 131073 integer digits (rounding a maximum-sized value carries
into a 131073rd digit at selected scale 1000) with a success status and a
packable payload the module's own decoder rejects; the fix validates the
final rounded average against the bare integer limit, returns
`DEC_ERR_RANGE`, and leaves the canonical zero. RAW-02/SPEC-02 — `dec_parse`
checked `!val` before releasing a reused output, so NULL text returned
`DEC_ERR_INVALID` while retaining the previous value and its allocation,
violating the header's canonical-zero failure contract; the fix rejects a
NULL output pointer first, then releases the reused output, then rejects
NULL text. The low-level `dec_round_scale` contract (temporary negative
display scales allowed) is intentionally unchanged. Both regressions are
added to the unit case in **Post-review fix round — regressions and
production fixes** (the "never edited by later tasks" wording for Task 1c
is superseded by exactly these two reviewed insertion blocks), captured RED
against the unfixed module before either production fix is applied, then
re-verified through the focused case, the ordinary oracle, the
allocation-failure probe and the full suite. Approved by the human
2026-10-23 with execution authorized in the same instruction.

**Task 2 warning amendment (2026-10-22), after PLAN_NOTES-2.md:**
This is the current halted checkpoint and supersedes the first-halt resume
instructions below. The first repair, Task 1e RED and baseline run, and Task 2
were already executed. Preserve them and both halt records. After approval,
apply only the three exact unused-helper deletions and parse allocation
replacement in **Resume-only Task 2 warning repair**, then rebuild/re-run
Task 2 verification before proceeding to Task 3. Do not reapply Tasks 1a–1e,
Task 1d edits, or all of Task 2; do not implement Task 7 early.

The zero-warning requirement is retained without warning suppressions.
Helpers are introduced only at their first consumers (Task 5 or Task 7),
and parse/unpack allocations explicitly check their signed coefficient count
against 1..DEC_MAX_COEFF_DIGITS immediately before casting/allocating. This
makes the existing bound local to GCC's whole-program range analysis.
No valid numeric value, API, on-disk format or test assertion changes.
Full integrated scratch evidence is in
`2026-10-22-phase2-warning-amendment.md`.

**Build-wiring amendment (2026-10-22), after the Task 1e halt:**
`PLAN_NOTES.md` correctly identifies an omitted consumer. The earlier edit 3
links `shard-db-test-server`, not the `shard-db-test` runner. Retain that edit
(the test daemon mirrors the production daemon), label it accurately, and add
Task 1d edit 5 for the runner using its unique target/entry-point anchor.
The inventory now includes four decimal-module source lists plus the case
registration, five edits total. The bench executable remains unchanged: it
has no decimal caller in Phase 2, and no benchmark is authorized.

Earlier clean standalone C replays and unique-anchor checks did not establish
that the complete build.sh wiring links. The actual integrated build failure
in PLAN_NOTES.md is authoritative; the repair's full-build/RED evidence is
recorded separately in `2026-10-22-phase2-build-wiring-amendment.md`.

**First-halt resumption boundary (historical; superseded by the Task 2
warning amendment above for PLAN_NOTES-2.md):** this describes only the
earlier PLAN_NOTES.md checkpoint. At the reported halt, Tasks 1a–1c and Task 1d edits 1–4
already exist. After approval, verify that state against the plan and apply
**only edit 5** plus the resume-only unused-helper removal below; do not
reapply the first four edits or overwrite the stubs.
The existing test file must match the current Task 1c content plus that
unused helper before its removal. Then restart Task 1e, capture the actual
RED, and continue Tasks 2–8 in order.
Preserve PLAN_NOTES.md as the halt record. If the actual state differs from
that checkpoint, halt for the planner rather than guessing. Fresh execution
from the clean input revision still applies all five edits.

**Revision 3 (2026-10-22), responding to
[the Revision 2 re-review](2026-10-22-phase2-decimal-core-plan-review-r2.md):**
STD-R2-01/SPEC-R2-02 — `dec_from_digits` resets its output to the canonical
zero when its limb allocation fails (both branches), closing the
parse/unpack NOMEM leak the forced-failure reproduction exposed; the header
contract states that helper-internal failures reset the output, and scopes
alias rejection (inputs preserved) and the low-level primitives (dec_reserve
never releases storage and leaves the value unchanged). A deterministic
allocation-failure probe (scratch, malloc/realloc substitution, no
production seam — the Phase 1 decision) now proves both allocating stages of
parse and unpack leave the canonical zero. STD-R2-02 — the add range failure
releases the rejected result; the overflow regression asserts sign, weight,
limb count, comparison and hash against the canonical zero. SPEC-R2-01 — the
packed integer bound `nd + m ≤ 131072` now applies to every exponent sign
(the `m ≥ 0` guard is gone), and the m = −1 fixture was corrected: all four
exponent bytes, display scale 1, and encoded-header setup assertions so a
mislabeled case cannot silently test another branch, plus a valid m = −1
boundary at exactly 131072 integer digits. SPEC-R2-03 — the fast-path
evaluation claims no speedup figures (all unmeasured hypotheses; benchmarks
are human-run) and notes the packed format could be emitted directly from a
small coefficient, making limb conversion an implementation choice rather
than an inherent requirement.

**Revision 2 (2026-10-22), responding to
[the Phase 2 plan review](2026-10-22-phase2-decimal-core-plan-review.md):**
STD-01/SPEC-01 — zero operands are now handled by explicit identity returns
before any magnitude logic, with regressions for both operand orders, both
signs, both operations, scaled zeros and repeated accumulation; the Task 4
prose that claimed the old path was correct is rewritten. STD-02/SPEC-03 —
`dec_round_scale` normalizes successful results (rounding to zero yields the
canonical zero; trailing zero limbs are stripped) and the unit case exercises
the exported function directly. STD-03/SPEC-04 — one output contract, stated
in `decnum.h` and implemented everywhere: outputs accept initialized or
reused values, never alias inputs, and every failure leaves the canonical
zero (precision overflow included), with reuse/failure tests through add,
precision, average and unpack. STD-04 — Task 2 now enumerates all five stub
replacements, Tasks 3/4/7 state which stubs each block defines and which
stubs to delete, and the literal task sequence was replayed in scratch with
ordinary compilation (Task 1 RED 162/422 through Task 7 863/0, every build
clean). SPEC-02 — the packed decoder enforces `nd + m ≤ 131072` for every
exponent sign, rejects nonzero odd-count pad nibbles, and validates the
canonical zero header (m must be 0), with hand-built negative payloads and
the exact 131072-digit boundary tested. SPEC-05 — Task 8 and every execution
rule now use ordinary compilation only; the earlier sanitizer scratch run is
disclosed as an instruction overstep and is neither repeated nor required.
SPEC-06 — the oracle canonicalizes the zero payload exponent independently of
display scale, and identity/cancellation cases (fractional operands
included) were added to both the oracle's analytic set and the committed
unit case. SPEC-07 — the hash uses the standard FNV-1a 64 offset basis
14695981039346656037 pinned by known vectors, and the xxh128 rationale was
corrected (it is viable standalone; FNV-1a wins on zero vendored code, not
on quality). The requested small-value 64/128-bit fast-path evaluation is
added with a defer recommendation, and the Phase 6 aggregate-intermediate
range policy is recorded as that phase's explicit decision.

**Planning-time verification (2026-10-22, revised after review):** every code
block in this plan was extracted verbatim into a scratch tree and executed
before each hand-back. The first pass ran the unit case under ASan+UBSan+LSan
— this **exceeded the handoff instruction**, which prohibited sanitizer runs
outright; the disclosure is retained as historical evidence, that run is not
the branch gate, and no sanitizer run is required or expected by this revised
plan. The review round's replay used ordinary `cc` only, following the literal
task order (stub RED through Task 7 green) to validate each task's edit list.
Findings fixed across both passes: two production defects (the add place-span
formula; divisor magnitude scaling and remainder seeding in average
division), the zero-operand add/sub corruption, non-canonical zeros from the
exported rounding, the output-lifecycle contract (reuse leaks; precision
overflow leaving a value in `out`), the packed-decoder integer-limit bypass
and missing pad-nibble/canonical-zero checks, a wrong FNV-1a offset basis,
several wrong hand-written test expectations, and six oracle-script defects
including the cancellation-payload exponent. The production tree is
untouched and Task 1's stub-first RED capture is still mandatory.

## Input identity

- Branch: `fix/phase1-safe-scalar-conversions` (continued — do **not** branch
  fresh; the handoff supersedes the parent's fresh-branch wording for this
  series).
- Base commit: `f5b80b4c58e49cfd8ac8b74ed9c1366f7b3a3130` — verified as HEAD
  at planning time, on top of baseline `1dd1888`. All anchors below were
  quoted from this exact tree.
- Worktree at planning start: only untracked planning files
  (`2026-10-19-durability-certification-rig.md`,
  `2026-10-22-phase1-index-text-oom-probe.c`,
  `2026-10-22-phase1-review-fixes-evidence/`,
  `2026-10-22-phase1-scratch/`); no tracked-source modifications.
- Protected certification plan verified **without reading its contents**:
  `sha256sum docs/plans/2026-10-19-durability-certification-rig.md` →
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`
  (matches the frozen value). The file is never edited, moved, staged,
  executed, or deleted. Re-verify the checksum at evidence time (Task 8).

## Goal and scope boundary

Phase 2 delivers one self-contained, bounded-memory exact decimal module
(`src/db/decnum.h` / `src/db/decnum.c`) plus its unit regression case and an
independent Python `decimal` oracle validation. It gives later phases a
common exact interpretation of a finance value: parse, normalize, compare,
scale-independent hash, precision/scale enforcement with ties-away rounding,
add/subtract, divide-by-row-count for averages under the frozen PostgreSQL 18
policy, exact text rendering, and packed serialization.

**Explicitly out of scope (later phases, per the parent plan and handoff):**
any consumer wiring — storage encoders, write/schema surfaces, index keys,
aggregates, migration, criteria, CLI/npm, user-facing docs. No new external
runtime dependency (the shipped binary gains zero dependencies; the oracle is
dev-time Python, like the repo's existing fuzz corpora tooling). No binary
floating-point conversion anywhere in the module. No benchmarks. No
intermediate sanitizer runs (the branch-series ASan+UBSan 3x / TSan 3x `--jobs 2`
gates stay at the end of all fixes, immediately before push/PR). Work is left
**uncommitted** for raw-diff review per this repo's standing execution mode.

The Phase 1 checked int64 numeric boundary (`conv_numeric_text` and its
consumers) is untouched by this plan. It remains the temporary safety boundary
until Phase 4 replaces the stored representation.

## Frozen contracts honored (verification mapping)

| Contract (parent Task 0.2 / handoff) | Where enforced in this plan |
| --- | --- |
| Bare numeric: 131,072 integer digits, 16,383 fractional digits | `dec_parse` limit checks (`DEC_MAX_INT_DIGITS`, `DEC_MAX_DSCALE`); Task 3 tests exercise the full 147,455-digit value and one past each limit |
| No 1000-significant-digit reduction for bare numeric | coefficient domain is the full 147,455 digits; Task 3 max-value round trip proves it |
| Constrained `numeric(P,S)`: P 1..1000, S −1000..1000, S>P and negative S allowed | `dec_apply_precision` parameter validation + PG18 `apply_typmod` port; Task 5 tests cover `numeric(3,1)`, `numeric(3,5)`, negative scales, currency |
| `numeric(P)` has scale 0; currency = `numeric(19,4)` | declaration mapping happens in later schema phases; the core's precision op is exercised with (P, 0) and (19, 4) in Task 5 tests |
| Round to declared S ties away, then enforce precision; overflow **after rounding** is an error | `dec_round_scale` (ties away on the magnitude) then the PG18 ddigits walk; Task 5 tests (`0.009995` in `numeric(3,5)`, `999994` in `numeric(4,-2)`, `99.95` in `numeric(3,1)`) |
| Signed zero canonicalizes mathematically; equal values hash equally across display scales | canonical `DEC_SIGN_ZERO`; `dec_hash` reads sign/weight/limbs only; Task 3/6 tests |
| Preserve meaningful trailing zeros; exponent normalizes display scale (`1.0e2` renders `100`) | display scale kept separately from the canonical coefficient; Task 3 tests (`1.2300`, `0.00100`, `1.0e2`, `100e-5`) |
| Negative declared S renders at display scale 0 | `dec_apply_precision` clamps `dscale` to ≥ 0 after rounding (PG18 `apply_typmod` "don't allow negative dscale"); Task 5 test |
| Average/result scale = PostgreSQL 18 `select_div_scale`: normalized base-10000 weights, leading groups, quotient-weight decrement, 16 significant digits, both display scales as floors, final clamp 0..1000 — distinct from the stored 16383 fractional limit | `dec_select_div_scale` is a line-by-line port of `REL_18_0/src/backend/utils/adt/numeric.c:10135`; Task 6 tests reproduce the Phase 0 golden cases |
| Division rounds half-away-from-zero on the magnitude | `dec_round_scale` is a port of PG18 `round_var` (round_powers `{0,1000,100,10}`, carry into a new leading limb); quotient gets one guard limb so the rounding decision is exact (PG18 `div_var` framing) |
| Packed payload layout (Phase 0 candidate): 16-byte header + packed digits; 73,744-byte ceiling is a validation ceiling, not per-record reservation | `dec_pack`/`dec_unpack` with full field validation; Task 7 tests |
| Exponent abuse rejected without exponent-proportional allocation or iteration | single linear scan; leading-zero-stripped exponent run > 7 digits → `DEC_ERR_EXPONENT` with O(1) memory; limit checks are pure integer arithmetic before any allocation; Task 3 tests feed a 10^6-digit exponent |
| `INT64_MIN`-style magnitudes never negated in signed arithmetic | sign-magnitude limb representation — no signed-integer negation exists in the module; Task 4 asserts `−2^63 − 1` is exact |

**Contract deviations found: none.** Two clarifications pinned during planning
(recorded for the human in *Open decisions*): zero-valued input whose
fractional digit span exceeds 16383 is rejected by the same uniform display
scale limit (D1), and the exponent digit-run policy (D3). Neither contradicts
a frozen contract; both are inside the frozen contracts' silence and are
flagged rather than chosen silently.

One evidence note: the Phase 0 ledger pins `select_div_scale` at
`REL_18_STABLE numeric.c:10173`; this planning pass downloaded tag
`REL_18_0` (the parent's pinned tag) where the same function sits at line
10135. The algorithm text is identical for `select_div_scale`, `div_var`,
`round_var`, `strip_var`, and `numeric_avg` (avg = `numeric_div(sum, N)`,
confirmed at `REL_18_0` `numeric.c:6247-6283`). The plan cites `REL_18_0`.

## Consumer inventory

### Phase 2's direct consumers (the only files this plan touches)

| Consumer | Change | Anchor |
| --- | --- | --- |
| `build.sh` daemon link line | add `src/db/decnum.c` | unique `src/db/main.c src/db/config.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto` |
| `build.sh` `LIB_SRCS` (embedded static lib) | add `src/db/decnum.c` | unique `src/db/query_find.c src/db/server.c src/db/config.c src/db/type_desc.c \` (trailing backslash disambiguates from the link line) |
| `build.sh` test-daemon (`shard-db-test-server`) link line | add `src/db/decnum.c` | unique `src/db/config.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c` |
| `build.sh` test-runner (`shard-db-test`) link list | add `src/db/decnum.c` | unique two-line target/entry-point anchor quoted in Task 1d edit 5 |
| `build.sh` C test-case list | register the new case once | unique `    src/test/cases/test_scalar_csv_update.c \` |
| `src/db/decnum.h`, `src/db/decnum.c` | new module (this plan supplies complete file contents) | new files |
| `src/test/cases/test_decnum_core.c` | new unit case (complete contents below) | new file |

The module compiles under the daemon's exact `WARN_CFLAGS`
(`-Wall -Wextra -Wno-format-truncation -Wno-unused-parameter
-Wno-address-of-packed-member -Wno-unused-result`) with zero warnings, and
under `-flto` + `--gc-sections` the not-yet-referenced API is dropped from the
shipped binary — linking it now proves it builds under the daemon's flags
without adding runtime surface. `unsigned __int128` has existing repo
precedent (`src/db/query_plan.c:3176-3179`).

### Future-phase consumer registry (verified in this checkout; NOT touched by Phase 2)

Every site that later phases must migrate onto this core, re-verified
post-Phase-1 in the committed tree at `f5b80b4`. Phase 2 changes none of them.

| Area | Sites (file:symbol) |
| --- | --- |
| Schema representation | `src/db/types.h:135` (`FT_NUMERIC` "8 bytes int64 × 10^S" comment), `types.h:191-192` (`numeric_scale`, `numeric_scale_mult`); `src/db/type_desc.c:17` (`[FT_NUMERIC] = { "numeric", 8 }`) |
| Schema parsing | `src/db/config.c` `parse_field_type` numeric branch (P parsed and dropped, `atoi(comma+1)` for S, `numeric_scale_mult` guard outside 0..18) |
| Write encoders | `src/db/config.c` `conv_numeric_text` (config.c:2046), `conv_numeric` (config.c:2186), `encode_field_checked` FT_NUMERIC dispatch (config.c:2229-2230) |
| Index text encoder | `src/db/config.c` `encode_field_text_for_index` FT_NUMERIC case (config.c:2697 — `conv_numeric_text` then top-bit-flip int64 key), Phase 1's allocation-free rename |
| Criteria | `src/db/query_plan.c:83` `parse_numeric_i64` (zero fallback on invalid), `query_plan.c:359-361` (scalar bounds), `query_plan.c:508-511` (IN lists), `query_plan.c:863-865` (`cmp_typed_field_pair` int64 compare), `query_plan.c:985-988` (`match_typed` FT_NUMERIC → `cmp_op_i64`) |
| Cursor/index key emit | `src/db/query.c:5706` (`encode_criterion_value` FT_LONG/TIMESTAMP/NUMERIC top-bit-flip) |
| Record transform | `src/db/query_find.c:742-800` `transform_field_value` FT_NUMERIC rescale (multiply/truncate int64), `query_find.c:862-866` `field_needs_transform` |
| Aggregate decode/render | `src/db/query_aggregate.c:1107-1120` (leaf decode → double), `:1308-1330` (`decode_idx_to_buf` scaled render), `:1517-1540` (record render), `:2068-2074` `typed_field_to_double` (double division by `numeric_scale_mult`), `:2676` type name, `:2705` numeric-agg eligibility |
| Output projection | `src/db/query_find.c:434-448` `projected_field_is_numeric`; `json_projected_value` numeric pass-through |
| Computed updates | `src/db/storage.c:1596-1599` ($incr/$copy type eligibility), `:1651-1653` ($copy same-type/scale guard), `:1782-1827` (inline checked scaled-int64 $incr), `:1852` overflow message |
| Schema round trip | `src/db/query_schema.c:96-115` `field_value_fits_new` scale-up check, `query_schema.c:1900-1901` (`describe-object` emits `"scale":%d` only) |
| Test/build registration | `build.sh`: production daemon, embedded LIB_SRCS, test daemon, test runner, and case registration; five quoted replacements in Task 1d |

Known external consumers (dependent finance application, raw-JSON clients) are
out of workspace; this phase changes no wire or on-disk format, so nothing
external observes Phase 2.

## Design

### Representation

```c
typedef struct {
    int32_t  weight;   /* base-10000 exponent of digits[0]; 0 for zero */
    int32_t  dscale;   /* display scale (rendered fractional digits), 0..16383 */
    dec_sign sign;     /* DEC_SIGN_ZERO / DEC_SIGN_POS / DEC_SIGN_NEG */
    uint16_t ndigits;  /* valid limbs; 0 for zero */
    uint16_t cap;      /* allocated limbs */
    uint16_t *digits;  /* base-10000 limbs, most significant first, 0..9999 */
} dec_t;
```

- Value = ±Σ `digits[i]` × 10000^(weight − i). This is PostgreSQL's NumericVar
  shape, which makes the pinned `select_div_scale` port literal.
- The **mathematical value is canonical**: no leading or trailing zero limbs,
  one canonical zero (`DEC_SIGN_ZERO`, weight 0, ndigits 0). Every producing
  operation ends with `dec_normalize`, so equal values always carry identical
  (sign, weight, limbs) — which is exactly what `dec_cmp` and `dec_hash` read.
  Display scale is metadata carried beside the value: `1.00`, `1.0`, `1`
  compare and hash equal but render their own scales.
- Digit-level trailing zeros are *not* kept in the limbs (they are display
  metadata in `dscale`); the packed payload re-derives its mathematically
  canonical digit string from the limbs (see `dec_pack`), matching the Phase 0
  spike's "1.2300 → coefficient 123 × 10⁻², display scale 4".
- **Checked memory sizing:** coefficient limbs ≤ `DEC_MAX_LIMBS` = 36,864
  (147,455 digits); the allocation ceiling is `DEC_MAX_LIMBS_ALLOC` =
  `DEC_MAX_LIMBS + 2` for transient carry limbs during add and rounding-carry
  prepend. Every growth goes through `dec_reserve` with a `uint16` doubling
  guard (`ncap > UINT16_MAX/2` clamp) and an OOM status. Largest single
  buffers: limb array 73,732 bytes; text buffer bound `DEC_TEXT_MAX` = 147,459
  bytes; payload bound `DEC_PAYLOAD_MAX` = 73,744 bytes; division state is one
  `unsigned __int128` remainder (< 10²⁴, divisor < 10²⁰) plus the output limb
  array. No stack arrays scale with value size anywhere (test helpers heap
  allocate their buffers).
- Ownership/error contract: every fallible API returns `dec_status`; the only
  allocation-failure status is `DEC_ERR_NOMEM`, and partial outputs are left
  as canonical zeros. `err/errsz` message buffers follow the repo's
  `fmt_conv_err` pattern (`dec_parse`, `dec_apply_precision`) but the module
  formats them itself — it includes nothing but libc headers, so later phases
  can consume it from any translation unit. Composite operations require
  `out` to be distinct from their inputs (`DEC_ERR_INVALID` otherwise) — the
  same non-aliasing contract PostgreSQL's NumericVar functions document.

### Arithmetic policies (all fixed, none left to the executor)

- **Parse** grammar: `[+-]? ( digits+ ( "." digits* )? | "." digits+ )
  ( [eE] [+-]? digits+ )?` — Phase-1-parity outer strip of one
  space/tab/CR/LF run, whole-input consumption, embedded NUL rejected.
  `5.` and `.5` are valid (Phase 1 and PostgreSQL both accept them).
- **Limits before allocation**: from run lengths and the exponent only —
  `int_digits = max(1, int_len + E − leading_zeros)` must be ≤ 131072;
  `dscale = max(0, frac_len − E)` must be ≤ 16383; both checks are pure
  integer arithmetic, so oversized input is rejected with no allocation at
  all. The canonical digit buffer is allocated only after the limits pass,
  so it is bounded by 147,455 bytes regardless of input size.
- **Exponent policy**: single linear scan; strip leading zeros of the digit
  run, then more than 7 significant digits → `DEC_ERR_EXPONENT`. A
  10^6-digit exponent is rejected after scanning its (already-consumed) run
  with zero allocations and no value-proportional work; syntax is still
  validated over the full run. A zero value accepts any in-syntax exponent
  (`0e999999` → zero, display scale 0).
- **Add/sub**: align to the coarser operand's weight, limb-wise add with
  carry or magnitude-subtract with borrow (larger magnitude wins the sign),
  result display scale = max(operand display scales) (PostgreSQL `add_var`
  policy), normalize, then enforce `dec_int_digits ≤ 131072` →
  `DEC_ERR_RANGE`. Cancellation yields the canonical zero.
- **Round** (`dec_round_scale`): PG18 `round_var` port —
  `di = (weight+1)·4 + scale`; di < 0 → canonical zero with
  `dscale = scale`; else truncate to `ceil(di/4)` limbs and round
  half-away-from-zero within the dropped position (`DEC_ROUND_POWERS =
  {0,1000,100,10}`, `extra ≥ pow10/2`), propagating carry down to a
  possible new leading limb (weight++). Operates on the magnitude, so it is
  ties-away for both signs.
- **Precision** (`dec_apply_precision`): PG18 `apply_typmod` port — round at
  S, clamp display scale to ≥ 0, then `maxdigits = P − S` and the ddigits
  walk `(weight+1)·4` minus leading zero limbs (−4) and high-order zero
  digits of the first nonzero limb (−3/−2/−1); still > maxdigits →
  `DEC_ERR_PRECISION`. A true zero always passes (ndigits 0 loop).
- **Average division** (`dec_div_avg`): divisor built canonically from the
  `uint64` count; `rscale = select_div_scale(sum, N)`; `res_weight =
  sum.weight − N.weight + 1`, `res_ndigits = max(1, res_weight + 1 +
  ceil(rscale/4)) + 1` guard limb; schoolbook division feeding one dividend
  limb at a time into an `unsigned __int128` remainder (`rem < divisor <
  10²⁰`, so `rem·10⁴ + limb < 2^81`; trial quotient `rem/divisor < 10⁴`
  always — exactness argument: with `rem < divisor`,
  `(rem·10⁴ + 9999)/divisor ≤ 10⁴ − 1`); assemble, then
  `dec_round_scale(out, rscale)` + normalize. Zero sum → canonical zero with
  `dscale = rscale` (PG `div_var` zero path). `count == 0` →
  `DEC_ERR_DIV_ZERO`.
- **select_div_scale port** (exact, from `REL_18_0` numeric.c:10135):
  defaults `weight=0, firstdigit=0` when an operand is zero; leading zero
  limbs decrement the effective weight (`weight − i` at the first nonzero
  limb); `qweight = weight1 − weight2`, decremented when
  `firstdigit1 ≤ firstdigit2` ("can't be sure, assume var1 is less");
  `rscale = clamp(16 − 4·qweight, max(dscale1, dscale2, 0), 1000)` — the
  display-scale floors are the **raw** dscales (the leading-group adjustment
  affects only the weight/firstdigit estimate), exactly as pinned in the
  Phase 0 ledger.
- **Text** (`dec_to_text`): integer limbs rendered most-significant-first
  (top limb trimmed of leading zeros, lower limbs `%04u`), `0` for a zero
  integer part, then exactly `dscale` fractional digits walked digit-by-digit
  from limbs (`limb_at_place`, zeros where the coefficient is exhausted) —
  `1.0e2` → `100`, `1.2300` → `1.2300`, `0.00100` → `0.00100`,
  `0.0e-3` → `0.0000`. `dec_text_len` gives the exact buffer requirement;
  short buffers get `DEC_ERR_TRUNCATED`, never silent truncation.
- **Pack/unpack** (frozen Phase 0 layout): 16-byte header (`version`,
  `sign`, reserved 0, BE digit count, BE int32 coefficient exponent,
  BE display scale) + ceil(digits/2) nibble-packed digits, high nibble
  first, odd-count low pad nibble 0. Decode validates version, sign,
  reserved, digit count ≤ 147455, exact byte length, every nibble ≤ 9, the
  pad nibble, digit-string canonicity (no leading/trailing zero digit;
  `count == 0 ⇔ sign == ZERO`), the bare digit limits
  (`−m ≤ 16383`, `C + m ≤ 131072`), `dscale ≤ 16383`, and
  `dscale ≥ max(0, −m)` so the payload can always be rendered losslessly.
  Structural failures → `DEC_ERR_PAYLOAD`; limit failures → `DEC_ERR_RANGE`.
- **Hash**: FNV-1a 64 (standard offset basis 14695981039346656037, prime
  1099511628211) over `[sign][weight int32 BE][ndigits uint16 BE]
  [limb uint16 BE]...` — mathematical value only, verified against the
  published empty-input vector and a computed canonical-zero vector in the
  unit case. xxh128 was considered and is viable standalone
  (`XXH_INLINE_ALL` with a vendored xxhash.h needs no storage.c linkage);
  FNV-1a was chosen because it needs zero vendored code for a non-crypto
  group-key use, accepting its weaker avalanche and collision resistance.
  Swapping to xxh128 later is an isolated change to `dec_hash` plus the
  oracle''s mirror.

### Small-value 64/128-bit fast-path evaluation (deferred, with rationale)

The question raised in review: should common small values take checked
int64/`unsigned __int128` paths instead of heap-backed base-10000 limbs,
while keeping the frozen storage format and canonical invariants?

Candidate shape: a union in `dec_t` — an inline `{int64 coefficient, int32
scale}` form for values with at most 18–19 significant digits, limb storage
otherwise. Per operation:

- **parse**: inputs within 18 digits and no exponent could accumulate into
  `uint64` directly, skipping the limb walk. This is an unmeasured
  hypothesis — no benchmark has been run (benchmarks are human-run), and no
  speedup figure is claimed.
- **cmp**: equal-scale int64 pairs compare in one instruction; differing
  scales need a checked ×10^k alignment first. Filters dominated by
  same-scale comparisons would benefit; mixed-scale ones partially. Also
  unmeasured.
- **add**: checked `__int128` sum, then normalize — plausibly the
  interesting case for same-scale accumulation, but equally unmeasured.
- **hash**: no fast path is possible without canonicalizing first. Equal
  values at different scales (`1` vs `1.0`) must hash equally, so an inline
  form must strip the factor-10 gcd and then feed the *same* byte encoding
  as the slow path — for ≤ 5 limbs the saving is marginal.
- **pack**: the frozen payload is defined over the canonical digit string.
  The current implementation derives that string from limbs; a small-value
  path could also emit the payload directly from the inline coefficient.
  Limb conversion is a current implementation choice, not an inherent
  requirement of the packed bytes — whichever route a fast path takes
  needs analysis, not assumption.

What it costs: a second representation branch in every API, duplicated
canonicalization logic (or conversion at every text/pack/hash boundary), a
doubled test matrix, and a wider `dec_t`.

**Recommendation: defer.** Phase 2 has no consumer, so there is no measured
hot path to justify the complexity; correctness-first infrastructure is the
goal. The API and failure contracts above are representation-neutral, so a
checked int64 path can be added inside `decnum.c` later — in Phase 6, where
aggregate accumulation volume is real and the user (not the agent) runs the
benches — without changing stored bytes, the packed format, or any
signature. The `unsigned __int128` division remainder is not this
evaluation; it is the exactness mechanism for the one division the contract
requires.

### Boundary for later phases (aggregate intermediate range)

This core enforces the bare digit limits on every stored value and on each
add/subtract result. Phase 6's aggregates need an explicit intermediate
policy decided in that phase's child plan: summing many near-maximum values
can overflow this core's add limit even when the final average fits, and
cancellation makes overflow partition-dependent. The parent plan permits
propagated overflow/resource errors and has not frozen wider accumulators;
up to ~20 extra decimal digits of intermediate headroom would cover uint64
row-count growth without touching the packed storage format. Phase 6 must
choose, document, and test that policy (including any core extension such as
an explicit wide-accumulator API) rather than claiming unlimited
intermediate arithmetic here.

## Embedded execution rules for this child plan

1. Continue on `fix/phase1-safe-scalar-conversions` at `f5b80b4` or later
   (human may have added commits; anchors were verified at `f5b80b4`).
   Preserve all untracked planning files.
2. Never edit, move, stage, execute or delete
   `docs/plans/2026-10-19-durability-certification-rig.md`; verify its
   SHA-256 (above) at start and at Task 8.
3. Do tasks **in order**. Each task's tests already exist and fail (Task 1
   stages the whole case first); a production task turns its group green.
   Never weaken, skip, or delete an assertion. If a test cannot pass
   honestly, stop and report.
4. If any quoted anchor is not found exactly, write `PLAN_NOTES.md`
   describing the mismatch and **halt the entire execution run** — do not
   guess, reinterpret, or continue any task. If a decision is not covered by
   this plan, stop and ask; do not improvise.
5. Commands (rtk, exact forms):
   - build: `rtk proxy env SKIP_TESTS=1 ./build.sh`
   - focused: `rtk proxy ./build/bin/shard-db-test run test-decnum-core`
   - full suite: `rtk proxy ./build/bin/shard-db-test run-all`
   - probe (Task 8): the exact `cc`/run commands in Task 8.
   Copy actual outputs into the phase evidence document. Never rerun-until-
   green; a failing-then-passing test is a confirmed bug until root-caused.
   Ordinary compilation everywhere: this phase runs no sanitizer build and no
   benchmark (the branch-series ASan+UBSan 3x / TSan 3x `--jobs 2` gates
   remain at the end of all fixes, exactly as AGENTS.md schedules them).
6. Keep work **uncommitted**. No push/PR/merge. No benchmarks. No sanitizer
   builds anywhere in this phase — not as a gate, not as a scratch smoke test
   (branch-series gate comes later, after all fixes; the planning round's
   sanitizer scratch run was an instruction overstep and is not repeated).
7. Scratch artifacts (probe binary, oracle cases/outputs, logs) stay under a
   single disposable directory (`/tmp/p2-oracle/`) and out of `git status`;
   only the files named in this plan enter the working tree.

---

## Task 1 — Stage the regression case, module skeleton, and build wiring; capture RED

**Test first.** The new module is the deliverable, so the red proof is a
new-module harness (explicitly allowed by the handoff: "A new-module harness
may supply the planned red proof where the public route intentionally belongs
to later phases"). The test case below is the **final** test content — it is
written once here and never edited by later tasks; each production task
replaces stub bodies until the whole case is green.

### Task 1a — `src/db/decnum.h` (complete new file)

```c
/* src/db/decnum.h
 *
 * Exact decimal core: sign + base-10000 coefficient + weight, with a
 * separate display scale. No binary floating point anywhere; every
 * operation is checked (allocation, digit limits, declared precision)
 * and reports failures through dec_status. Self-contained by design —
 * this module includes only libc headers so storage, query and
 * aggregate code can consume it without pulling in unrelated internals.
 *
 * Representation: value = ±digits × 10000^weight, digits[0] the most
 * significant limb. The mathematical value is canonical (no leading or
 * trailing zero limbs, one canonical zero); equal values therefore
 * carry identical sign/weight/limbs, which is what dec_cmp and
 * dec_hash read. dscale ("display scale") is metadata: the number of
 * fractional digits to render, preserved from the input so 1.2300 and
 * 1.23 are the same value with different display scales.
 */
#ifndef DECNUM_H
#define DECNUM_H

#include <stdint.h>
#include <stddef.h>

#define DEC_LIMB_DIGITS     4            /* decimal digits per limb */
#define DEC_LIMB_BASE       10000
#define DEC_MAX_INT_DIGITS  131072       /* bare numeric integer-digit limit */
#define DEC_MAX_FRAC_DIGITS 16383        /* bare numeric fractional-digit limit */
#define DEC_MAX_COEFF_DIGITS (DEC_MAX_INT_DIGITS + DEC_MAX_FRAC_DIGITS)
#define DEC_MAX_LIMBS       ((DEC_MAX_COEFF_DIGITS + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS)
/* Transient ceiling for aligned intermediates (carry limb, rounding
   carry). A contract-valid value never exceeds DEC_MAX_LIMBS; the two
   extra limbs only exist between assembly and normalization. */
#define DEC_MAX_LIMBS_ALLOC (DEC_MAX_LIMBS + 2)
#define DEC_MAX_DSCALE      16383
#define DEC_MAX_PRECISION   1000
#define DEC_MAX_SCALE       1000
#define DEC_MIN_SCALE       (-1000)
/* PostgreSQL 18 calculation constants (numeric.h): the average/division
   result-scale clamp is distinct from the stored 16383-digit limit. */
#define DEC_MIN_SIG_DIGITS  16
#define DEC_AVG_MAX_RSCALE  1000

#define DEC_PAYLOAD_VERSION 1
#define DEC_PAYLOAD_HEADER  16
#define DEC_PAYLOAD_MAX     (DEC_PAYLOAD_HEADER + (DEC_MAX_COEFF_DIGITS + 1) / 2)
#define DEC_TEXT_MAX        (2 + DEC_MAX_INT_DIGITS + 1 + DEC_MAX_FRAC_DIGITS + 1)

typedef enum {
    DEC_SIGN_ZERO = 0,
    DEC_SIGN_POS  = 1,
    DEC_SIGN_NEG  = 2
} dec_sign;

typedef enum {
    DEC_OK = 0,
    DEC_ERR_SYNTAX,     /* malformed decimal text (incl. embedded NUL) */
    DEC_ERR_EXPONENT,   /* exponent text outside the supported domain */
    DEC_ERR_RANGE,      /* value outside the finite bare-numeric digit limits */
    DEC_ERR_PRECISION,  /* constrained assignment overflows precision after rounding */
    DEC_ERR_DIV_ZERO,   /* average over zero rows */
    DEC_ERR_NOMEM,      /* allocation failure */
    DEC_ERR_PAYLOAD,    /* packed payload structurally invalid */
    DEC_ERR_INVALID,    /* NULL argument, aliasing violation, P/S out of domain */
    DEC_ERR_TRUNCATED,  /* text/payload output buffer too small */
    DEC_ERR_INTERNAL    /* invariant violation; never returned for valid input */
} dec_status;

typedef struct {
    int32_t  weight;
    int32_t  dscale;
    dec_sign sign;
    uint16_t ndigits;
    uint16_t cap;
    uint16_t *digits;
} dec_t;

/* Lifecycle. dec_init leaves a canonical zero with no buffer.
   dec_free accepts an initialized or already-freed value.
   Output contract for every function below that writes a dec_t: `out`
   must be a valid initialized (or previously freed) value and must not
   alias any input — alias rejection leaves every input untouched; on
   every failure return `out` is left as the canonical zero, on success
   it holds the result. This contract covers failure paths inside
   helpers too (a failed limb allocation inside parse/unpack/arithmetic
   resets the output). The low-level primitives make their own narrower
   promises: dec_reserve never releases storage and leaves the value
   unchanged on failure. */
void dec_init(dec_t *d);
void dec_free(dec_t *d);
/* Grow the limb buffer. DEC_ERR_NOMEM on allocation failure,
   DEC_ERR_RANGE past DEC_MAX_LIMBS_ALLOC. On failure the value is
   unchanged; storage is never released here. */
dec_status dec_reserve(dec_t *d, uint16_t limbs);
/* Canonicalize in place: strip leading/trailing zero limbs; an empty
   limb list becomes the canonical zero. dscale is kept untouched. */
void dec_normalize(dec_t *d);

/* Parse the full finite decimal grammar (output contract above):
     [+-]? ( digits+ ( "." digits* )? | "." digits+ ) ( [eE] [+-]? digits+ )?
   One outer ASCII space/tab/CR/LF run is stripped (literal parity with
   the scalar conversion boundary). The whole input must be consumed;
   an embedded NUL is a syntax error. The display scale is the
   fractional digit count after the exponent is applied, never
   negative (1.0e2 renders 100); trailing zeros stay display-only
   (1.2300 keeps display scale 4). Scanning is a single linear pass
   with no exponent-proportional allocation: an exponent run with more
   than 7 significant digits (leading zeros ignored) fails with
   DEC_ERR_EXPONENT. `out` may be a reused dec_t; any prior buffer is
   released. */
dec_status dec_parse(const char *val, size_t vlen, dec_t *out,
                     char *err, size_t errsz);

/* Exact text at display scale: integer digits, then exactly dscale
   fractional digits. dec_text_len is the exact byte count including
   the NUL; a short buffer fails with DEC_ERR_TRUNCATED. */
size_t dec_text_len(const dec_t *d);
dec_status dec_to_text(const dec_t *d, char *buf, size_t bufsz,
                       size_t *out_len);

/* Full mathematical order; display scale is ignored (1.0 == 1.00). */
int dec_cmp(const dec_t *a, const dec_t *b);

/* FNV-1a 64 over the canonical mathematical form only (sign, weight,
   limbs) — equal values hash equally regardless of display scale. */
uint64_t dec_hash(const dec_t *d);

/* Digits left of the decimal point: 0 for zero, else >= 1 (values
   below 1 count the leading "0"). */
int32_t dec_int_digits(const dec_t *d);

/* out = a + b / a - b, exact. Result display scale is the larger
   operand's. DEC_ERR_RANGE when the result leaves the bare digit
   limits; DEC_ERR_NOMEM on allocation failure. out must differ from a
   and b. */
dec_status dec_add(const dec_t *a, const dec_t *b, dec_t *out);
dec_status dec_sub(const dec_t *a, const dec_t *b, dec_t *out);

/* Round the magnitude to `scale` fractional digits, ties away from
   zero (PostgreSQL 18 round_var semantics, including carry into a new
   leading limb). scale < 0 rounds left of the point. Sets dscale to
   scale (callers clamp negative display scale where the contract
   requires it — see dec_apply_precision). The result is canonical:
   rounding to zero yields the canonical zero, never a signed or
   weighted zero, and no trailing zero limbs survive. In-place: on
   DEC_ERR_NOMEM `d` is left as the canonical zero. */
dec_status dec_round_scale(dec_t *d, int32_t scale);

/* Constrained assignment (PostgreSQL 18 apply_typmod): round to
   `scale` (ties away), clamp display scale to >= 0, then enforce
   precision: the rounded value's digit count left of the point must be
   <= precision - scale. DEC_ERR_PRECISION names overflow after
   rounding. P must be 1..1000 and S -1000..1000 (DEC_ERR_INVALID
   otherwise). out must differ from in. */
dec_status dec_apply_precision(const dec_t *in, int32_t precision,
                               int32_t scale, dec_t *out,
                               char *err, size_t errsz);

/* sum / count under the frozen PostgreSQL 18 average policy:
   select_div_scale picks the result scale from normalized base-10000
   weights and leading groups (16 significant digits, both operands'
   display scales as floors, final clamp 0..1000 — distinct from the
   stored 16383-digit fractional limit); the quotient is computed to
   that scale with one guard limb and rounded half-away-from-zero on
   the magnitude. A zero sum yields zero at the selected scale.
   count == 0 is DEC_ERR_DIV_ZERO. out must differ from sum. */
dec_status dec_div_avg(const dec_t *sum, uint64_t count, dec_t *out);

/* Packed payload (frozen Phase 0 layout), DEC_PAYLOAD_HEADER bytes of
   header plus ceil(significant digits / 2) packed digit bytes:
     [0]  uint8  version (DEC_PAYLOAD_VERSION)
     [1]  uint8  sign (dec_sign)
     [2]  uint16 BE reserved = 0
     [4]  uint32 BE significant digit count (0 for zero, else 1..147455)
     [8]  int32  BE coefficient exponent m (value = ±digits × 10^m)
     [12] uint32 BE display scale
     [16] ceil(digits/2) bytes, two decimal digits per byte, high
          nibble first; an odd count pads the final low nibble with 0
   Decode validates every field: version, sign/reserved/consistency,
   nibble values, pad nibble, exact length, digit-string canonicity
   (no leading/trailing zero digit), the bare numeric digit limits and
   a display scale that can render the value losslessly. */
size_t dec_payload_len(const dec_t *d);
dec_status dec_pack(const dec_t *d, uint8_t *out, size_t cap,
                    size_t *out_len);
dec_status dec_unpack(const uint8_t *buf, size_t len, dec_t *out);

#endif /* DECNUM_H */
```

### Task 1b — `src/db/decnum.c` (complete STUB file; later tasks replace bodies)

```c
/* src/db/decnum.c
 *
 * Exact decimal core. See decnum.h for the representation contract.
 * STUB SKELETON: lifecycle is real (the test harness needs it); every
 * mathematical operation reports failure without producing a value.
 * Production bodies land task-by-task in the approved plan.
 */
#include "decnum.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

void dec_init(dec_t *d) {
    d->weight = 0;
    d->dscale = 0;
    d->sign = DEC_SIGN_ZERO;
    d->ndigits = 0;
    d->cap = 0;
    d->digits = NULL;
}

void dec_free(dec_t *d) {
    free(d->digits);
    dec_init(d);
}

dec_status dec_reserve(dec_t *d, uint16_t limbs) {
    if (limbs <= d->cap) return DEC_OK;
    if (limbs > DEC_MAX_LIMBS_ALLOC) return DEC_ERR_RANGE;
    uint16_t ncap = d->cap ? d->cap : 16;
    while (ncap < limbs) {
        if (ncap > UINT16_MAX / 2) { ncap = limbs; break; }
        ncap = (uint16_t)(ncap * 2);
    }
    uint16_t *grown = realloc(d->digits, (size_t)ncap * sizeof(uint16_t));
    if (!grown) return DEC_ERR_NOMEM;
    d->digits = grown;
    d->cap = ncap;
    return DEC_OK;
}

void dec_normalize(dec_t *d) {
    (void)d;
}

dec_status dec_parse(const char *val, size_t vlen, dec_t *out,
                     char *err, size_t errsz) {
    (void)val; (void)vlen; (void)out;
    if (err && errsz > 0) err[0] = '\0';
    return DEC_ERR_SYNTAX;
}

size_t dec_text_len(const dec_t *d) {
    (void)d;
    return 2;
}

dec_status dec_to_text(const dec_t *d, char *buf, size_t bufsz,
                       size_t *out_len) {
    (void)d; (void)buf; (void)bufsz; (void)out_len;
    return DEC_ERR_INTERNAL;
}

int dec_cmp(const dec_t *a, const dec_t *b) {
    (void)a; (void)b;
    return 0;
}

uint64_t dec_hash(const dec_t *d) {
    (void)d;
    return 0;
}

int32_t dec_int_digits(const dec_t *d) {
    (void)d;
    return 0;
}

dec_status dec_add(const dec_t *a, const dec_t *b, dec_t *out) {
    (void)a; (void)b; (void)out;
    return DEC_ERR_INTERNAL;
}

dec_status dec_sub(const dec_t *a, const dec_t *b, dec_t *out) {
    (void)a; (void)b; (void)out;
    return DEC_ERR_INTERNAL;
}

dec_status dec_round_scale(dec_t *d, int32_t scale) {
    (void)d; (void)scale;
    return DEC_ERR_INTERNAL;
}

dec_status dec_apply_precision(const dec_t *in, int32_t precision,
                               int32_t scale, dec_t *out,
                               char *err, size_t errsz) {
    (void)in; (void)precision; (void)scale; (void)out;
    if (err && errsz > 0) err[0] = '\0';
    return DEC_ERR_INTERNAL;
}

dec_status dec_div_avg(const dec_t *sum, uint64_t count, dec_t *out) {
    (void)sum; (void)count; (void)out;
    return DEC_ERR_INTERNAL;
}

size_t dec_payload_len(const dec_t *d) {
    (void)d;
    return 0;
}

dec_status dec_pack(const dec_t *d, uint8_t *out, size_t cap,
                    size_t *out_len) {
    (void)d; (void)out; (void)cap; (void)out_len;
    return DEC_ERR_INTERNAL;
}

dec_status dec_unpack(const uint8_t *buf, size_t len, dec_t *out) {
    (void)buf; (void)len; (void)out;
    return DEC_ERR_PAYLOAD;
}
```

### Task 1c — `src/test/cases/test_decnum_core.c` (complete new file — final content)

```c
/* Exact decimal core unit coverage: parsing, normalization, comparison,
   hashing, precision/rounding, addition, average division, text and
   packed serialization. Unit case — no daemon, no fixtures. */
#include "test_runner.h"
#include "test_assert.h"
#include "decnum.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Parse text, compare status, render at display scale, compare text
   and display scale. */
static void expect_parse(const char *text, dec_status want_status,
                         const char *want_text, int32_t want_dscale,
                         const char *label) {
    dec_t d;
    dec_init(&d);
    char err[192] = {0};
    dec_status st = dec_parse(text, strlen(text), &d, err, sizeof(err));
    ASSERT_EQ_INT(st, want_status, label);
    if (st == DEC_OK) {
        char *buf = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(buf, label);
        if (buf) {
            size_t n = 0;
            dec_status rt = dec_to_text(&d, buf, DEC_TEXT_MAX, &n);
            ASSERT_EQ_INT(rt, DEC_OK, label);
            if (want_text) ASSERT_EQ_STR(buf, want_text, label);
            ASSERT_EQ_INT((long long)d.dscale, (long long)want_dscale, label);
            ASSERT_EQ_INT((long long)n + 1, (long long)dec_text_len(&d), label);
            free(buf);
        }
    } else if (want_text) {
        ASSERT_EQ_STR(err, want_text, label);
    }
    dec_free(&d);
}

static void expect_cmp_text(const char *a_text, const char *b_text,
                            int want, const char *label) {
    dec_t a, b;
    dec_init(&a); dec_init(&b);
    char err[64];
    ASSERT_EQ_INT(dec_parse(a_text, strlen(a_text), &a, err, sizeof(err)),
                  DEC_OK, label);
    ASSERT_EQ_INT(dec_parse(b_text, strlen(b_text), &b, err, sizeof(err)),
                  DEC_OK, label);
    int c = dec_cmp(&a, &b);
    int norm = c < 0 ? -1 : (c > 0 ? 1 : 0);
    ASSERT_EQ_INT(norm, want, label);
    dec_free(&a); dec_free(&b);
}

static void expect_hash_equal(const char *const *texts, size_t n,
                              const char *label) {
    dec_t d;
    dec_init(&d);
    uint64_t h0 = 0;
    char err[64];
    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ_INT(dec_parse(texts[i], strlen(texts[i]), &d,
                                err, sizeof(err)), DEC_OK, label);
        uint64_t h = dec_hash(&d);
        if (i == 0) h0 = h;
        ASSERT_TRUE(h == h0, label);
    }
    dec_free(&d);
}

static void expect_add_text(const char *a_text, const char *b_text,
                            dec_status want_status, const char *want_text,
                            const char *label) {
    dec_t a, b, r;
    dec_init(&a); dec_init(&b); dec_init(&r);
    char err[64];
    ASSERT_EQ_INT(dec_parse(a_text, strlen(a_text), &a, err, sizeof(err)),
                  DEC_OK, label);
    ASSERT_EQ_INT(dec_parse(b_text, strlen(b_text), &b, err, sizeof(err)),
                  DEC_OK, label);
    dec_status st = dec_add(&a, &b, &r);
    ASSERT_EQ_INT(st, want_status, label);
    if (st == DEC_OK && want_text) {
        char *buf = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(buf, label);
        if (buf) {
            size_t n = 0;
            dec_status rt = dec_to_text(&r, buf, DEC_TEXT_MAX, &n);
            ASSERT_EQ_INT(rt, DEC_OK, label);
            if (rt == DEC_OK) ASSERT_EQ_STR(buf, want_text, label);
            free(buf);
        }
    }
    dec_free(&a); dec_free(&b); dec_free(&r);
}

static void expect_sub_text(const char *a_text, const char *b_text,
                            const char *want_text, const char *label) {
    dec_t a, b, r;
    dec_init(&a); dec_init(&b); dec_init(&r);
    char err[64];
    ASSERT_EQ_INT(dec_parse(a_text, strlen(a_text), &a, err, sizeof(err)),
                  DEC_OK, label);
    ASSERT_EQ_INT(dec_parse(b_text, strlen(b_text), &b, err, sizeof(err)),
                  DEC_OK, label);
    dec_status st = dec_sub(&a, &b, &r);
    ASSERT_EQ_INT(st, DEC_OK, label);
    if (st == DEC_OK && want_text) {
        char *buf = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(buf, label);
        if (buf) {
            size_t n = 0;
            dec_status rt = dec_to_text(&r, buf, DEC_TEXT_MAX, &n);
            ASSERT_EQ_INT(rt, DEC_OK, label);
            if (rt == DEC_OK) ASSERT_EQ_STR(buf, want_text, label);
            free(buf);
        }
    }
    dec_free(&a); dec_free(&b); dec_free(&r);
}

static void expect_prec_text(const char *text, int32_t precision, int32_t scale,
                             dec_status want_status, const char *want_text,
                             const char *label) {
    dec_t in, out;
    dec_init(&in); dec_init(&out);
    char err[192] = {0};
    ASSERT_EQ_INT(dec_parse(text, strlen(text), &in, err, sizeof(err)),
                  DEC_OK, label);
    dec_status st = dec_apply_precision(&in, precision, scale, &out,
                                        err, sizeof(err));
    ASSERT_EQ_INT(st, want_status, label);
    if (st == DEC_OK && want_text) {
        char *buf = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(buf, label);
        if (buf) {
            size_t n = 0;
            dec_status rt = dec_to_text(&out, buf, DEC_TEXT_MAX, &n);
            ASSERT_EQ_INT(rt, DEC_OK, label);
            if (rt == DEC_OK) ASSERT_EQ_STR(buf, want_text, label);
            free(buf);
        }
    }
    dec_free(&in); dec_free(&out);
}

static void expect_avg_text(const char *sum_text, uint64_t count,
                            dec_status want_status, const char *want_text,
                            const char *label) {
    dec_t sum, out;
    dec_init(&sum); dec_init(&out);
    char err[64];
    if (sum_text) {
        ASSERT_EQ_INT(dec_parse(sum_text, strlen(sum_text), &sum,
                                err, sizeof(err)), DEC_OK, label);
    }
    dec_status st = dec_div_avg(&sum, count, &out);
    ASSERT_EQ_INT(st, want_status, label);
    if (st == DEC_OK && want_text) {
        char *buf = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(buf, label);
        if (buf) {
            size_t n = 0;
            dec_status rt = dec_to_text(&out, buf, DEC_TEXT_MAX, &n);
            ASSERT_EQ_INT(rt, DEC_OK, label);
            if (rt == DEC_OK) ASSERT_EQ_STR(buf, want_text, label);
            free(buf);
        }
    }
    dec_free(&sum); dec_free(&out);
}

/* Parse, pack, unpack; compare mathematical value, display scale and
   rendered text against the original. */
static void expect_pack_roundtrip(const char *text, const char *label) {
    dec_t d, u;
    dec_init(&d); dec_init(&u);
    char err[64];
    ASSERT_EQ_INT(dec_parse(text, strlen(text), &d, err, sizeof(err)),
                  DEC_OK, label);
    size_t plen = dec_payload_len(&d);
    ASSERT_TRUE(plen >= DEC_PAYLOAD_HEADER && plen <= DEC_PAYLOAD_MAX, label);
    uint8_t *payload = malloc(plen);
    ASSERT_NOT_NULL(payload, label);
    if (payload) {
        size_t out_len = 0;
        ASSERT_EQ_INT(dec_pack(&d, payload, plen, &out_len), DEC_OK, label);
        ASSERT_EQ_INT((long long)out_len, (long long)plen, label);
        ASSERT_EQ_INT(dec_unpack(payload, out_len, &u), DEC_OK, label);
        ASSERT_EQ_INT(dec_cmp(&d, &u), 0, label);
        ASSERT_EQ_INT((long long)d.dscale, (long long)u.dscale, label);
        char *t1 = malloc(DEC_TEXT_MAX), *t2 = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(t1, label); ASSERT_NOT_NULL(t2, label);
        if (t1 && t2) {
            size_t n1 = 0, n2 = 0;
            dec_status r1 = dec_to_text(&d, t1, DEC_TEXT_MAX, &n1);
            dec_status r2 = dec_to_text(&u, t2, DEC_TEXT_MAX, &n2);
            ASSERT_EQ_INT(r1, DEC_OK, label);
            ASSERT_EQ_INT(r2, DEC_OK, label);
            /* never print unrendered buffers: RED-phase stub output
               must stay valid TAP text */
            if (r1 == DEC_OK && r2 == DEC_OK) ASSERT_EQ_STR(t2, t1, label);
        }
        free(t1); free(t2);
        free(payload);
    }
    dec_free(&d); dec_free(&u);
}

/* Flip one byte of a valid payload and require DEC_ERR_PAYLOAD (or
   DEC_ERR_RANGE for fields whose corruption can only exceed limits). */
static void expect_pack_corrupt(const char *text, size_t off, uint8_t xor_mask,
                                dec_status want_status, const char *label) {
    dec_t d, u;
    dec_init(&d); dec_init(&u);
    char err[64];
    ASSERT_EQ_INT(dec_parse(text, strlen(text), &d, err, sizeof(err)),
                  DEC_OK, label);
    size_t plen = dec_payload_len(&d);
    uint8_t *payload = malloc(plen + 1);
    ASSERT_NOT_NULL(payload, label);
    if (payload) {
        size_t out_len = 0;
        ASSERT_EQ_INT(dec_pack(&d, payload, plen, &out_len), DEC_OK, label);
        payload[off] = (uint8_t)(payload[off] ^ xor_mask);
        dec_status st = dec_unpack(payload, out_len, &u);
        ASSERT_EQ_INT(st, want_status, label);
        free(payload);
    }
    dec_free(&d); dec_free(&u);
}

static char *build_max_value(size_t *len_out) {
    size_t len = (size_t)DEC_MAX_INT_DIGITS + 1 + (size_t)DEC_MAX_FRAC_DIGITS;
    char *s = malloc(len + 1);
    if (!s) return NULL;
    memset(s, '9', (size_t)DEC_MAX_INT_DIGITS);
    s[DEC_MAX_INT_DIGITS] = '.';
    memset(s + DEC_MAX_INT_DIGITS + 1, '1', (size_t)DEC_MAX_FRAC_DIGITS);
    s[len] = '\0';
    *len_out = len;
    return s;
}

static int test_decnum_core_run(void) {
    /* ---- group 1: parse + display-scale rendering ---- */
    expect_parse("1234567890123456.1234567890", DEC_OK,
                 "1234567890123456.1234567890", 10,
                 "original numeric(26,10) value parses exactly");
    expect_parse("-1234567890123456.1234567890", DEC_OK,
                 "-1234567890123456.1234567890", 10,
                 "original negative value parses exactly");
    expect_parse("1234567890123456.1234567891", DEC_OK,
                 "1234567890123456.1234567891", 10,
                 "adjacent last-place value +1");
    expect_parse("1234567890123456.1234567889", DEC_OK,
                 "1234567890123456.1234567889", 10,
                 "adjacent last-place value -1");
    expect_parse("0", DEC_OK, "0", 0, "bare zero");
    expect_parse("-0", DEC_OK, "0", 0, "signed zero renders unsigned");
    expect_parse("-0.00", DEC_OK, "0.00", 2, "signed zero keeps display scale");
    expect_parse("0.0e-3", DEC_OK, "0.0000", 4, "zero exponent negative scale");
    expect_parse("1.2300", DEC_OK, "1.2300", 4, "trailing zeros preserved");
    expect_parse("0.00100", DEC_OK, "0.00100", 5, "leading+trailing frac zeros");
    expect_parse("1.0e2", DEC_OK, "100", 0, "exponent normalizes display scale");
    expect_parse("100e-5", DEC_OK, "0.00100", 5, "negative exponent scale");
    expect_parse("0e5", DEC_OK, "0", 0, "zero with positive exponent");
    expect_parse("0.00e5", DEC_OK, "0", 0, "zero exponent swallows frac zeros");
    expect_parse("9007199254740993", DEC_OK, "9007199254740993", 0,
                 "2^53+1 exact (loses low bit in double)");
    expect_parse("-9007199254740993", DEC_OK, "-9007199254740993", 0,
                 "negative 2^53+1 exact");
    expect_parse("0.30000000000000004", DEC_OK, "0.30000000000000004", 17,
                 "classic double-error value exact");
    expect_parse("9223372036854775808", DEC_OK, "9223372036854775808", 0,
                 "2^63 exact (one past INT64_MAX)");
    expect_parse("18446744073709551615", DEC_OK, "18446744073709551615", 0,
                 "2^64-1 exact");
    expect_parse("  12.50  ", DEC_OK, "12.50", 2, "outer whitespace stripped");
    expect_parse("+7.25", DEC_OK, "7.25", 2, "explicit plus sign");
    expect_parse(".5", DEC_OK, "0.5", 1, "leading-dot fraction");
    expect_parse("5.", DEC_OK, "5", 0, "trailing dot");
    expect_parse("1e00005", DEC_OK, "100000", 0, "leading-zero exponent digits");
    expect_parse("1e131071", DEC_OK, NULL, 0,
                 "10^131071 inside the integer limit");
    expect_parse("1e-16383", DEC_OK, NULL, 16383,
                 "10^-16383 inside the fractional limit");
    expect_parse("99999e131067", DEC_OK, NULL, 0,
                 "131072 integer digits via exponent");

    /* ---- group 2: grammar rejection ---- */
    expect_parse("", DEC_ERR_SYNTAX, NULL, 0, "empty input rejected");
    expect_parse("   ", DEC_ERR_SYNTAX, NULL, 0, "whitespace-only rejected");
    expect_parse("+", DEC_ERR_SYNTAX, NULL, 0, "bare sign rejected");
    expect_parse("-", DEC_ERR_SYNTAX, NULL, 0, "bare minus rejected");
    expect_parse(".", DEC_ERR_SYNTAX, NULL, 0, "bare dot rejected");
    expect_parse("+.e5", DEC_ERR_SYNTAX, NULL, 0, "sign dot exponent rejected");
    expect_parse("e5", DEC_ERR_SYNTAX, NULL, 0, "bare exponent rejected");
    expect_parse("1e", DEC_ERR_SYNTAX, NULL, 0, "dangling e rejected");
    expect_parse("1e+", DEC_ERR_SYNTAX, NULL, 0, "dangling e+ rejected");
    expect_parse("1e-", DEC_ERR_SYNTAX, NULL, 0, "dangling e- rejected");
    expect_parse("1e5.5", DEC_ERR_SYNTAX, NULL, 0, "fractional exponent rejected");
    expect_parse("1e5e5", DEC_ERR_SYNTAX, NULL, 0, "double exponent rejected");
    expect_parse("1.2.3", DEC_ERR_SYNTAX, NULL, 0, "second dot rejected");
    expect_parse("abc", DEC_ERR_SYNTAX, NULL, 0, "letters rejected");
    expect_parse("--1", DEC_ERR_SYNTAX, NULL, 0, "double sign rejected");
    expect_parse("1 2", DEC_ERR_SYNTAX, NULL, 0, "embedded space rejected");
    expect_parse("1,2", DEC_ERR_SYNTAX, NULL, 0, "comma rejected");
    expect_parse("1\t2", DEC_ERR_SYNTAX, NULL, 0, "embedded tab rejected");
    expect_parse("12x", DEC_ERR_SYNTAX, NULL, 0, "trailing junk rejected");

    /* ---- group 3: finite digit limits, one past each ---- */
    expect_parse("1.5e-16383", DEC_ERR_RANGE, NULL, 0,
                 "16384 fractional digits via exponent rejected");
    {
        size_t len = 0;
        char *mx = build_max_value(&len);
        ASSERT_NOT_NULL(mx, "max value builder");
        if (mx) {
            dec_t d;
            dec_init(&d);
            char err[64];
            ASSERT_EQ_INT(dec_parse(mx, len, &d, err, sizeof(err)), DEC_OK,
                          "full 147455-digit value parses");
            ASSERT_EQ_INT((long long)d.dscale, DEC_MAX_FRAC_DIGITS,
                          "max value display scale");
            ASSERT_EQ_INT((long long)dec_int_digits(&d), DEC_MAX_INT_DIGITS,
                          "max value integer digits");
            char *buf = malloc(DEC_TEXT_MAX);
            ASSERT_NOT_NULL(buf, "max value render buffer");
            if (buf) {
                size_t n = 0;
                ASSERT_EQ_INT(dec_to_text(&d, buf, DEC_TEXT_MAX, &n), DEC_OK,
                              "max value renders");
                ASSERT_TRUE(n == len && memcmp(buf, mx, len) == 0,
                            "max value renders byte-identical");
                free(buf);
            }
            dec_free(&d);
            free(mx);
        }
    }
    {
        /* one more integer digit than the limit, direct and via exponent */
        size_t n_int = (size_t)DEC_MAX_INT_DIGITS + 1;
        char *s = malloc(n_int + 2);
        ASSERT_NOT_NULL(s, "over-limit builder");
        if (s) {
            memset(s, '9', n_int);
            s[n_int] = '\0';
            expect_parse(s, DEC_ERR_RANGE, NULL, 0,
                         "131073 integer digits rejected");
            /* 131072 nines followed by e1 = 131073 digits */
            s[n_int - 1] = 'e';
            s[n_int] = '1';
            s[n_int + 1] = '\0';
            expect_parse(s, DEC_ERR_RANGE, NULL, 0,
                         "131073 digits via exponent rejected");
            free(s);
        }
    }
    {
        size_t n = (size_t)DEC_MAX_FRAC_DIGITS + 1;
        char *s = malloc(n + 3);
        ASSERT_NOT_NULL(s, "frac over-limit builder");
        if (s) {
            s[0] = '0'; s[1] = '.';
            memset(s + 2, '1', n);
            s[n + 2] = '\0';
            expect_parse(s, DEC_ERR_RANGE, NULL, 0,
                         "16384 fractional digits rejected");
            free(s);
        }
    }

    /* ---- group 4: exponent abuse ---- */
    {
        size_t big = 1000000;
        char *s = malloc(big + 4);
        ASSERT_NOT_NULL(s, "exponent abuse builder");
        if (s) {
            s[0] = '1'; s[1] = 'e';
            memset(s + 2, '9', big);
            s[big + 2] = '\0';
            expect_parse(s, DEC_ERR_EXPONENT, NULL, 0,
                         "million-digit exponent rejected");
            free(s);
        }
        char *z = malloc(64);
        ASSERT_NOT_NULL(z, "zero exponent builder");
        if (z) {
            snprintf(z, 64, "1e0000000000000000000005");
            expect_parse(z, DEC_OK, "100000", 0,
                         "leading-zero exponent run accepted");
            free(z);
        }
    }

    /* ---- group 5: comparison order ---- */
    expect_cmp_text("1", "2", -1, "1 < 2");
    expect_cmp_text("2", "10", -1, "2 < 10 (numeric, not lexicographic)");
    expect_cmp_text("-2", "-10", 1, "-2 > -10");
    expect_cmp_text("1", "1.0000000000000000000000001", -1,
                    "deeper fraction is larger");
    expect_cmp_text("-1", "-1.0000000000000000000000001", 1,
                    "negative deeper fraction is smaller");
    expect_cmp_text("1.0", "1.00", 0, "display scale ignored");
    expect_cmp_text("0", "-0.000", 0, "zero equals signed zero");
    expect_cmp_text("0", "0.00001", -1, "zero < tiny");
    expect_cmp_text("-0.00001", "0.00001", -1, "tiny negative < tiny positive");
    expect_cmp_text("1e3", "999.9999999999", 1, "exponent form compares");
    expect_cmp_text("1234567890123456.1234567890",
                    "1234567890123456.1234567891", -1,
                    "original value adjacent ordering");
    expect_cmp_text("-1234567890123456.1234567890",
                    "-1234567890123456.1234567891", 1,
                    "original negative adjacent ordering");

    /* ---- group 6: scale-independent hashing ---- */
    {
        static const char *const eq1[] = {
            "1", "1.0", "1.00", "1e0", "10e-1", "01", "0001.000", "+1"
        };
        expect_hash_equal(eq1, sizeof(eq1) / sizeof(eq1[0]),
                          "equal ones hash equally");
        static const char *const eqz[] = {
            "0", "-0", "0.0", "0.000", "0e5", "-0.00e2"
        };
        expect_hash_equal(eqz, sizeof(eqz) / sizeof(eqz[0]),
                          "zeros hash equally across spellings");
        static const char *const eqo[] = {
            "1234567890123456.1234567890",
            "12345678901234561234567890e-10",
            "0.12345678901234561234567890e16"
        };
        expect_hash_equal(eqo, sizeof(eqo) / sizeof(eqo[0]),
                          "original value hashes equal across spellings");
    }

    /* ---- group 7: addition / subtraction ---- */
    expect_add_text("9223372036854775807", "1", DEC_OK,
                    "9223372036854775808", "INT64_MAX + 1 exact");
    expect_sub_text("-9223372036854775808", "1", "-9223372036854775809",
                    "INT64_MIN - 1 exact (no signed negation)");
    expect_add_text("9007199254740992", "1", DEC_OK,
                    "9007199254740993", "2^53 + 1 exact");
    expect_add_text("1.0000000000000001", "-1.0000000000000000", DEC_OK,
                    "0.0000000000000001", "near-cancellation keeps low digits");
    expect_add_text("1.5", "-1.5", DEC_OK, "0.0",
                    "exact cancellation keeps display scale");
    expect_add_text("0.9999", "0.0001", DEC_OK, "1.0000",
                    "carry keeps display scale");
    expect_add_text("1.2300", "1.2", DEC_OK, "2.4300",
                    "sum display scale is the larger operand's");
    expect_add_text("-1.25", "1.25", DEC_OK, "0.00",
                    "sign-symmetric cancellation keeps scale");
    expect_add_text("1e131071", "1e131071", DEC_OK, NULL,
                    "sum inside the integer limit");
    expect_add_text("999999999999999999999999999999", "1", DEC_OK,
                    "1000000000000000000000000000000", "carry across 30 digits");
    {
        /* 131072 nines plus one overflows the bare integer limit */
        size_t n_int = (size_t)DEC_MAX_INT_DIGITS;
        char *s = malloc(n_int + 1);
        dec_t a, b, r;
        dec_init(&a); dec_init(&b); dec_init(&r);
        ASSERT_NOT_NULL(s, "overflow sum builder");
        if (s) {
            memset(s, '9', n_int);
            s[n_int] = '\0';
            char err[64];
            ASSERT_EQ_INT(dec_parse(s, n_int, &a, err, sizeof(err)), DEC_OK,
                          "max integer parses");
            ASSERT_EQ_INT(dec_parse("1", 1, &b, err, sizeof(err)), DEC_OK,
                          "one parses");
            dec_t z;
            dec_init(&z);
            ASSERT_EQ_INT(dec_add(&a, &b, &r), DEC_ERR_RANGE,
                          "sum past 131072 integer digits is a range error");
            ASSERT_EQ_INT((int)r.sign, (int)DEC_SIGN_ZERO,
                          "range failure leaves the canonical zero sign");
            ASSERT_EQ_INT((long long)r.weight, 0,
                          "range failure leaves weight 0");
            ASSERT_EQ_INT((long long)r.ndigits, 0,
                          "range failure leaves no limbs");
            ASSERT_EQ_INT(dec_cmp(&r, &z), 0,
                          "range failure output compares as zero");
            ASSERT_TRUE(dec_hash(&r) == dec_hash(&z),
                          "range failure output hashes as zero");
            dec_free(&z);
        }
        free(s);
        dec_free(&a); dec_free(&b); dec_free(&r);
    }
    expect_add_text("0", "0.5", DEC_OK, "0.5", "zero + fraction");
    expect_add_text("0", "-0.5", DEC_OK, "-0.5", "zero + negative fraction");
    expect_sub_text("0", "0.5", "-0.5", "zero - fraction");
    expect_sub_text("0", "-0.5", "0.5", "zero - negative fraction");
    expect_add_text("0.5", "0", DEC_OK, "0.5", "fraction + zero");
    expect_sub_text("0.5", "0", "0.5", "fraction - zero");
    expect_add_text("-0.5", "0", DEC_OK, "-0.5", "negative fraction + zero");
    expect_sub_text("-0.5", "0", "-0.5", "negative fraction - zero");
    expect_add_text("0", "0", DEC_OK, "0", "zero + zero");
    expect_sub_text("0", "0", "0", "zero - zero");
    expect_add_text("0.00", "0.5", DEC_OK, "0.50",
                    "scaled zero keeps the larger display scale");
    expect_add_text("0.5", "0.00", DEC_OK, "0.50",
                    "scaled zero second operand keeps display scale");
    {
        /* repeated accumulation starting at zero stays exact */
        dec_t acc, step, r;
        dec_init(&acc); dec_init(&step); dec_init(&r);
        char err[64];
        ASSERT_EQ_INT(dec_parse("0", 1, &acc, err, sizeof(err)), DEC_OK,
                      "accumulator starts at zero");
        for (int i = 0; i < 7; i++) {
            ASSERT_EQ_INT(dec_parse("0.0000000000000001",
                                    strlen("0.0000000000000001"), &step,
                                    err, sizeof(err)), DEC_OK, "step parses");
            ASSERT_EQ_INT(dec_add(&acc, &step, &r), DEC_OK, "accumulate");
            dec_free(&acc);
            acc = r;
            dec_init(&r);
        }
        char *buf = malloc(DEC_TEXT_MAX);
        ASSERT_NOT_NULL(buf, "accumulator render");
        if (buf) {
            size_t n = 0;
            ASSERT_EQ_INT(dec_to_text(&acc, buf, DEC_TEXT_MAX, &n), DEC_OK,
                          "accumulator renders");
            ASSERT_EQ_STR(buf, "0.0000000000000007",
                          "seven steps sum exactly");
            free(buf);
        }
        dec_free(&acc); dec_free(&step); dec_free(&r);
    }

    {
        /* associativity holds exactly across scales */
        dec_t a, b, c, ab, abc, bc, abc2;
        dec_init(&a); dec_init(&b); dec_init(&c);
        dec_init(&ab); dec_init(&abc); dec_init(&bc); dec_init(&abc2);
        char err[64];
        ASSERT_EQ_INT(dec_parse("1.000000000000000001", 19, &a, err, sizeof(err)), DEC_OK, "assoc a");
        ASSERT_EQ_INT(dec_parse("-2345678901234.5678", 19, &b, err, sizeof(err)), DEC_OK, "assoc b");
        ASSERT_EQ_INT(dec_parse("777.123456789000000000", 21, &c, err, sizeof(err)), DEC_OK, "assoc c");
        ASSERT_EQ_INT(dec_add(&a, &b, &ab), DEC_OK, "assoc ab");
        ASSERT_EQ_INT(dec_add(&ab, &c, &abc), DEC_OK, "assoc (a+b)+c");
        ASSERT_EQ_INT(dec_add(&b, &c, &bc), DEC_OK, "assoc bc");
        ASSERT_EQ_INT(dec_add(&a, &bc, &abc2), DEC_OK, "assoc a+(b+c)");
        ASSERT_EQ_INT(dec_cmp(&abc, &abc2), 0, "exact associativity");
        dec_free(&a); dec_free(&b); dec_free(&c);
        dec_free(&ab); dec_free(&abc); dec_free(&bc); dec_free(&abc2);
    }

    /* ---- group 7b: direct rounding meets canonical invariants ---- */
    {
        struct { const char *in; int32_t scale; const char *out; } rt[] = {
            { "0.1", 0, "0" }, { "0.49", 0, "0" },
            { "0.5", 0, "1" }, { "0.6", 0, "1" },
            { "-0.1", 0, "0" }, { "-0.5", 0, "-1" },
            { "9999.9999", 2, "10000.00" }, { "10000.4", 0, "10000" },
            { "2.5", 0, "3" },
        };
        dec_t v, want;
        for (size_t k = 0; k < sizeof(rt) / sizeof(rt[0]); k++) {
            dec_init(&v); dec_init(&want);
            char err[64];
            ASSERT_EQ_INT(dec_parse(rt[k].in, strlen(rt[k].in), &v,
                                    err, sizeof(err)), DEC_OK, "round seed");
            ASSERT_EQ_INT(dec_round_scale(&v, rt[k].scale), DEC_OK,
                          "direct round succeeds");
            char *buf = malloc(DEC_TEXT_MAX);
            ASSERT_NOT_NULL(buf, "round render");
            if (buf) {
                size_t n = 0;
                dec_status rr = dec_to_text(&v, buf, DEC_TEXT_MAX, &n);
                ASSERT_EQ_INT(rr, DEC_OK, "round result renders");
                if (rr == DEC_OK)
                    ASSERT_EQ_STR(buf, rt[k].out, "direct round text");
                free(buf);
            }
            ASSERT_EQ_INT(dec_parse(rt[k].out, strlen(rt[k].out), &want,
                                    err, sizeof(err)), DEC_OK, "round expect");
            ASSERT_EQ_INT(dec_cmp(&v, &want), 0, "round result compares");
            ASSERT_TRUE(dec_hash(&v) == dec_hash(&want),
                        "round result hashes like its value");
            ASSERT_TRUE(v.ndigits != 0 || v.sign == DEC_SIGN_ZERO,
                        "rounded zero is the canonical zero");
            dec_free(&v); dec_free(&want);
        }
    }

    /* ---- group 7c: output lifecycle (reuse and failure) ---- */
    {
        dec_t out, zero;
        dec_init(&out); dec_init(&zero);
        char err[64];
        /* reused output through success and cancellation */
        ASSERT_EQ_INT(dec_parse("1.5", 3, &out, err, sizeof(err)), DEC_OK,
                      "lifecycle seed");
        dec_t a, b;
        dec_init(&a); dec_init(&b);
        ASSERT_EQ_INT(dec_parse("2.25", 4, &a, err, sizeof(err)), DEC_OK, "a");
        ASSERT_EQ_INT(dec_parse("0.75", 4, &b, err, sizeof(err)), DEC_OK, "b");
        ASSERT_EQ_INT(dec_add(&a, &b, &out), DEC_OK, "reuse out on success");
        ASSERT_EQ_INT(dec_cmp(&out, &zero) != 0 ? 1 : 0, 1,
                      "reused out holds the sum");
        ASSERT_EQ_INT(dec_parse("-3", 2, &a, err, sizeof(err)), DEC_OK, "-3");
        ASSERT_EQ_INT(dec_add(&a, &b, &out), DEC_OK, "reuse out on success 2");
        ASSERT_EQ_INT(dec_parse("-0.75", 5, &a, err, sizeof(err)), DEC_OK,
                      "-0.75");
        ASSERT_EQ_INT(dec_add(&a, &b, &out), DEC_OK, "reuse out on cancel");
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0, "cancellation is canonical zero");
        ASSERT_TRUE(out.sign == DEC_SIGN_ZERO, "cancellation sign is ZERO");
        /* precision overflow leaves the canonical zero in out */
        ASSERT_EQ_INT(dec_parse("99.95", 5, &a, err, sizeof(err)), DEC_OK,
                      "overflow operand");
        ASSERT_EQ_INT(dec_apply_precision(&a, 3, 1, &out, err, sizeof(err)),
                      DEC_ERR_PRECISION, "precision overflow rejects");
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0,
                      "precision failure leaves canonical zero");
        /* validation failure leaves canonical zero */
        ASSERT_EQ_INT(dec_apply_precision(&a, 0, 0, &out, NULL, 0),
                      DEC_ERR_INVALID, "bad precision rejects");
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0,
                      "validation failure leaves canonical zero");
        /* average zero-count failure leaves canonical zero */
        ASSERT_EQ_INT(dec_parse("42.5", 4, &a, err, sizeof(err)), DEC_OK, "42.5");
        ASSERT_EQ_INT(dec_div_avg(&a, 7, &out), DEC_OK, "avg success reuses out");
        ASSERT_EQ_INT(dec_div_avg(&a, 0, &out), DEC_ERR_DIV_ZERO,
                      "zero count rejects");
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0,
                      "average failure leaves canonical zero");
        /* unpack failure after success leaves canonical zero */
        uint8_t payload[DEC_PAYLOAD_HEADER + 2] = {0};
        size_t plen = 0;
        ASSERT_EQ_INT(dec_parse("1.23", 4, &a, err, sizeof(err)), DEC_OK, "1.23");
        ASSERT_EQ_INT(dec_pack(&a, payload, sizeof(payload), &plen), DEC_OK,
                      "pack for lifecycle");
        ASSERT_EQ_INT(dec_unpack(payload, plen, &out), DEC_OK,
                      "unpack success reuses out");
        payload[16] ^= 0xA0;
        ASSERT_EQ_INT(dec_unpack(payload, plen, &out), DEC_ERR_PAYLOAD,
                      "unpack failure rejects");
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0,
                      "unpack failure leaves canonical zero");
        dec_free(&out); dec_free(&zero); dec_free(&a); dec_free(&b);
    }

    /* ---- group 7d: FNV-1a 64 known vector ---- */
    {
        dec_t z;
        dec_init(&z);
        /* FNV-1a 64 offset 0xcbf29ce484222325, seven zero bytes
           (sign, weight, ndigits of the canonical zero) */
        ASSERT_TRUE(dec_hash(&z) == UINT64_C(8614007388540201639),
                    "canonical zero hash matches the FNV-1a known vector");
        dec_free(&z);
    }

    /* ---- group 8: precision / scale enforcement, ties away ---- */
    expect_prec_text("9.94", 3, 1, DEC_OK, "9.9", "numeric(3,1) rounds down");
    expect_prec_text("9.96", 3, 1, DEC_OK, "10.0",
                     "numeric(3,1) carries into a new integer digit");
    expect_prec_text("99.95", 3, 1, DEC_ERR_PRECISION, NULL,
                     "numeric(3,1) overflows only after rounding");
    expect_prec_text("0.00999", 3, 5, DEC_OK, "0.00999",
                     "numeric(3,5) maximum value fits");
    expect_prec_text("-0.00999", 3, 5, DEC_OK, "-0.00999",
                     "numeric(3,5) negative maximum fits");
    expect_prec_text("0.01", 3, 5, DEC_ERR_PRECISION, NULL,
                     "numeric(3,5) rejects one unit above maximum");
    expect_prec_text("0.009995", 3, 5, DEC_ERR_PRECISION, NULL,
                     "numeric(3,5) overflows after rounding");
    expect_prec_text("999949", 4, -2, DEC_OK, "999900",
                     "negative scale rounds left of the point");
    expect_prec_text("999994", 4, -2, DEC_ERR_PRECISION, NULL,
                     "negative scale overflow after rounding");
    expect_prec_text("123456", 4, -2, DEC_OK, "123500",
                     "negative scale rounds to hundreds");
    expect_prec_text("2.5", 2, 0, DEC_OK, "3", "ties away: 2.5 -> 3");
    expect_prec_text("-2.5", 2, 0, DEC_OK, "-3", "ties away: -2.5 -> -3");
    expect_prec_text("1.005", 3, 2, DEC_OK, "1.01", "ties away at scale 2");
    expect_prec_text("0.5", 1, 0, DEC_OK, "1", "ties away: 0.5 -> 1");
    expect_prec_text("12345678901234.5678", 19, 4, DEC_OK,
                     "12345678901234.5678", "currency-shape value fits");
    expect_prec_text("999999999999999.9999", 19, 4, DEC_OK,
                     "999999999999999.9999", "numeric(19,4) maximum fits");
    expect_prec_text("9999999999999999.9999", 19, 4, DEC_ERR_PRECISION, NULL,
                     "numeric(19,4) one integer digit too many");
    expect_prec_text("0", 1, 0, DEC_OK, "0", "zero fits any precision");
    expect_prec_text("0.000", 3, 5, DEC_OK, "0.00000",
                     "zero renders at declared scale");
    expect_prec_text("1234567890123456.1234567890", 26, 10, DEC_OK,
                     "1234567890123456.1234567890",
                     "original value fits numeric(26,10)");
    {
        /* P/S domain validation */
        dec_t in, out;
        dec_init(&in); dec_init(&out);
        char err[64];
        ASSERT_EQ_INT(dec_parse("1", 1, &in, err, sizeof(err)), DEC_OK,
                      "precision domain seed");
        ASSERT_EQ_INT(dec_apply_precision(&in, 0, 0, &out, NULL, 0),
                      DEC_ERR_INVALID, "P=0 rejected");
        ASSERT_EQ_INT(dec_apply_precision(&in, 1001, 0, &out, NULL, 0),
                      DEC_ERR_INVALID, "P=1001 rejected");
        ASSERT_EQ_INT(dec_apply_precision(&in, 10, -1001, &out, NULL, 0),
                      DEC_ERR_INVALID, "S=-1001 rejected");
        ASSERT_EQ_INT(dec_apply_precision(&in, 10, 1001, &out, NULL, 0),
                      DEC_ERR_INVALID, "S=1001 rejected");
        dec_free(&in); dec_free(&out);
    }

    /* ---- group 9: average division (PostgreSQL 18 policy) ---- */
    expect_avg_text("3", 2, DEC_OK, "1.5000000000000000",
                    "avg(1,2): sum 3 count 2, 16 places");
    expect_avg_text("1", 2, DEC_OK, "0.50000000000000000000",
                    "1/2: firstdigit decrement gives 20 places");
    expect_avg_text("10", 3, DEC_OK, "3.3333333333333333",
                    "10/3: 16 places");
    expect_avg_text("2", 3, DEC_OK, "0.66666666666666666667",
                    "2/3: repeating, firstdigit decrement gives 20 places");
    expect_avg_text("1", 3, DEC_OK, "0.33333333333333333333",
                    "1/3: repeating, 20 places");
    expect_avg_text("5", 5, DEC_OK, "1.00000000000000000000",
                    "5/5: equal firstdigits decrement qweight (20 places)");
    expect_avg_text("0", 2, DEC_OK, "0.00000000000000000000",
                    "zero dividend renders zeros at selected scale");
    expect_avg_text("13", 3, DEC_OK, "4.3333333333333333",
                    "partitioned sum 13 over 3");
    expect_avg_text("1", 10000000, DEC_OK, "0.000000100000000000000000",
                    "tiny average: qweight from 10^7 divisor (24 places)");
    expect_avg_text("1", 100000000, DEC_OK, "0.0000000100000000000000000000",
                    "tiny average below 0.000001 (28 places)");
    expect_avg_text("10000", 9999, DEC_OK, "1.0001000100010001",
                    "quotient-weight boundary: firstdigit 1 vs 9999 gives 16");
    expect_avg_text("99999", 10000, DEC_OK, "9.9999000000000000",
                    "quotient-weight boundary: firstdigit 9 vs 1");
    expect_avg_text("1234567890123456.1234567890", 1, DEC_OK,
                    "1234567890123456.1234567890",
                    "count 1 returns the sum exactly");
    expect_avg_text("0", 1, DEC_OK, "0.00000000000000000000",
                    "zero over one row at 20 places (firstdigit decrement)");
    {
        dec_t sum, out;
        dec_init(&sum); dec_init(&out);
        ASSERT_EQ_INT(dec_div_avg(&sum, 0, &out), DEC_ERR_DIV_ZERO,
                      "count 0 rejected");
        dec_free(&sum); dec_free(&out);
    }
    {
        /* operand display scale > 1000: the calculation clamp wins */
        size_t frac = 1500;
        char *s = malloc(frac + 4);
        dec_t sum, out;
        dec_init(&sum); dec_init(&out);
        ASSERT_NOT_NULL(s, "big-dscale sum builder");
        if (s) {
            s[0] = '1'; s[1] = '.';
            memset(s + 2, '0', frac);
            s[frac + 2] = '\0';
            char err[64];
            ASSERT_EQ_INT(dec_parse(s, frac + 2, &sum, err, sizeof(err)),
                          DEC_OK, "display scale 1500 parses");
            ASSERT_EQ_INT(dec_div_avg(&sum, 1, &out), DEC_OK,
                          "average over big-dscale sum succeeds");
            ASSERT_EQ_INT((long long)out.dscale, DEC_AVG_MAX_RSCALE,
                          "average result clamped to 1000 fractional digits");
        }
        free(s);
        dec_free(&sum); dec_free(&out);
    }

    /* ---- group 10: packed serialization ---- */
    expect_pack_roundtrip("1234567890123456.1234567890",
                          "original value packs/unpacks");
    expect_pack_roundtrip("-1234567890123456.1234567890",
                          "original negative packs/unpacks");
    expect_pack_roundtrip("0", "zero packs/unpacks");
    expect_pack_roundtrip("-0.00", "signed zero keeps display scale in payload");
    expect_pack_roundtrip("1.2300", "trailing-zero display scale survives");
    expect_pack_roundtrip("1.0e2", "exponent-normalized value survives");
    expect_pack_roundtrip("0.00100", "small display-scale value survives");
    {
        size_t len = 0;
        char *mx = build_max_value(&len);
        dec_t d;
        dec_init(&d);
        ASSERT_NOT_NULL(mx, "pack max builder");
        if (mx) {
            char err[64];
            ASSERT_EQ_INT(dec_parse(mx, len, &d, err, sizeof(err)), DEC_OK,
                          "max value parse for pack");
            size_t plen = dec_payload_len(&d);
            ASSERT_EQ_INT((long long)plen,
                          (long long)(DEC_PAYLOAD_HEADER +
                                      (DEC_MAX_COEFF_DIGITS + 1) / 2),
                          "max payload is exactly 73744 bytes");
            expect_pack_roundtrip(mx, "147455-digit value packs/unpacks");
            free(mx);
        }
        dec_free(&d);
    }
    {
        /* payload byte layout spot check: 1.23 -> 123 x 10^-2, dscale 2 */
        dec_t d;
        dec_init(&d);
        char err[64];
        ASSERT_EQ_INT(dec_parse("1.23", 4, &d, err, sizeof(err)), DEC_OK,
                      "payload layout seed");
        size_t plen = dec_payload_len(&d);
        ASSERT_EQ_INT((long long)plen, 16 + 2, "1.23 payload length");
        uint8_t p[32] = {0};
        size_t out_len = 0;
        ASSERT_EQ_INT(dec_pack(&d, p, sizeof(p), &out_len), DEC_OK,
                      "1.23 packs");
        ASSERT_EQ_INT(p[0], DEC_PAYLOAD_VERSION, "payload version byte");
        ASSERT_EQ_INT(p[1], DEC_SIGN_POS, "payload sign byte");
        ASSERT_EQ_INT(p[2], 0, "reserved high byte");
        ASSERT_EQ_INT(p[3], 0, "reserved low byte");
        ASSERT_EQ_INT(p[4], 0, "digit count byte 0");
        ASSERT_EQ_INT(p[5], 0, "digit count byte 1");
        ASSERT_EQ_INT(p[6], 0, "digit count byte 2");
        ASSERT_EQ_INT(p[7], 3, "digit count byte 3 (123 digits)");
        ASSERT_EQ_INT(p[8], 0xFF, "exponent -2 high byte");
        ASSERT_EQ_INT(p[11], 0xFE, "exponent -2 low byte (123 x 10^-2)");
        ASSERT_EQ_INT(p[15], 2, "display scale byte");
        ASSERT_EQ_INT(p[16], 0x12, "digits high nibble 1");
        ASSERT_EQ_INT(p[17], 0x30, "digits low nibble pair 2,3");
        dec_free(&d);
    }
    expect_pack_corrupt("1.23", 1, 0x01, DEC_ERR_PAYLOAD,
                        "corrupt sign byte rejected");
    expect_pack_corrupt("1.23", 3, 0x01, DEC_ERR_PAYLOAD,
                        "corrupt reserved byte rejected");
    expect_pack_corrupt("1.23", 16, 0xA0, DEC_ERR_PAYLOAD,
                        "corrupt digit nibble rejected");
    expect_pack_corrupt("1.23", 0, 0x01, DEC_ERR_PAYLOAD,
                        "corrupt version rejected");
    {
        /* SPEC-02 regressions: limits for every exponent sign, odd
           pad nibble, canonical zero header. */
        dec_t d;
        dec_init(&d);
        /* hand-built payload: nd digits of 9 with exponent m */
        struct hand_payload {
            uint8_t *bytes;
            size_t len;
        };
        size_t len = 0;
        char *mx = build_max_value(&len);
        ASSERT_NOT_NULL(mx, "unpack limit builder");
        if (mx) {
            char err2[64];
            ASSERT_EQ_INT(dec_parse(mx, len, &d, err2, sizeof(err2)), DEC_OK,
                          "max value for unpack limits");
            size_t plen = dec_payload_len(&d);
            uint8_t *pl = malloc(plen + 2);
            ASSERT_NOT_NULL(pl, "unpack limit payload");
            if (pl) {
                size_t ol = 0;
                ASSERT_EQ_INT(dec_pack(&d, pl, plen, &ol), DEC_OK, "pack max");
                /* valid boundary: m = -16383 decodes inside the limits */
                dec_t u;
                dec_init(&u);
                ASSERT_EQ_INT(dec_unpack(pl, ol, &u), DEC_OK,
                              "147455 digits at m=-16383 decode");
                dec_free(&u);
                free(pl);
            }
            free(mx);
        }
        /* 131073 integer digits at m = 0 exceed the integer limit */
        {
            long long nd = 131073;
            size_t plen = DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2);
            uint8_t *pl = malloc(plen);
            ASSERT_NOT_NULL(pl, "m0 payload");
            if (pl) {
                memset(pl, 0, plen);
                pl[0] = DEC_PAYLOAD_VERSION;
                pl[1] = DEC_SIGN_POS;
                pl[7] = (uint8_t)(nd & 0xFF);
                pl[6] = (uint8_t)((nd >> 8) & 0xFF);
                pl[5] = (uint8_t)((nd >> 16) & 0xFF);
                for (size_t k = 0; k < (size_t)((nd + 1) / 2) - 1; k++)
                    pl[DEC_PAYLOAD_HEADER + k] = 0x99;
                pl[plen - 1] = 0x90;   /* odd count: zero pad nibble */
                ASSERT_EQ_INT(pl[5], 0x02, "m0 fixture count byte 1");
                ASSERT_EQ_INT(pl[7], 0x01, "m0 fixture count byte 3");
                ASSERT_EQ_INT(pl[11], 0x00, "m0 fixture exponent byte 3");
                dec_t u;
                dec_init(&u);
                ASSERT_EQ_INT(dec_unpack(pl, plen, &u), DEC_ERR_RANGE,
                              "131073 integer digits at m=0 rejected");
                ASSERT_EQ_INT((int)u.sign, (int)DEC_SIGN_ZERO,
                              "m=0 rejection leaves the canonical zero");
                dec_free(&u);
                free(pl);
            }
        }
        /* 131074 coefficient digits at m = -1 still exceed the limit
           (131074 - 1 = 131073 integer digits). The fixture asserts the
           exact encoded header before decoding so a mislabeled case
           cannot silently exercise another branch. */
        {
            long long nd = 131074;
            size_t plen = DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2);
            uint8_t *pl = malloc(plen);
            ASSERT_NOT_NULL(pl, "mneg payload");
            if (pl) {
                memset(pl, 0, plen);
                pl[0] = DEC_PAYLOAD_VERSION;
                pl[1] = DEC_SIGN_POS;
                pl[7] = (uint8_t)(nd & 0xFF);
                pl[6] = (uint8_t)((nd >> 8) & 0xFF);
                pl[5] = (uint8_t)((nd >> 16) & 0xFF);
                pl[8] = 0xFF; pl[9] = 0xFF; pl[10] = 0xFF; pl[11] = 0xFF;
                pl[15] = 0x01;   /* dscale 1 satisfies the lossless rule */
                for (size_t k = 0; k < (size_t)((nd + 1) / 2); k++)
                    pl[DEC_PAYLOAD_HEADER + k] = 0x99;
                /* fixture setup: assert the encoded header fields */
                ASSERT_EQ_INT(pl[4], 0, "m-1 fixture count byte 0");
                ASSERT_EQ_INT(pl[5], 0x02, "m-1 fixture count byte 1");
                ASSERT_EQ_INT(pl[6], 0x00, "m-1 fixture count byte 2");
                ASSERT_EQ_INT(pl[7], 0x02, "m-1 fixture count byte 3");
                ASSERT_EQ_INT(pl[8], 0xFF, "m-1 fixture exponent byte 0");
                ASSERT_EQ_INT(pl[9], 0xFF, "m-1 fixture exponent byte 1");
                ASSERT_EQ_INT(pl[10], 0xFF, "m-1 fixture exponent byte 2");
                ASSERT_EQ_INT(pl[11], 0xFF, "m-1 fixture exponent byte 3");
                ASSERT_EQ_INT(pl[15], 0x01, "m-1 fixture display scale");
                dec_t u;
                dec_init(&u);
                ASSERT_EQ_INT(dec_unpack(pl, plen, &u), DEC_ERR_RANGE,
                              "131073 integer digits at m=-1 rejected");
                ASSERT_EQ_INT((int)u.sign, (int)DEC_SIGN_ZERO,
                              "m=-1 rejection leaves the canonical zero");
                dec_free(&u);
                free(pl);
            }
        }
        /* valid boundary: 131073 digits at m = -1 is exactly 131072
           integer digits and decodes */
        {
            long long nd = 131073;
            size_t plen = DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2);
            uint8_t *pl = malloc(plen);
            ASSERT_NOT_NULL(pl, "mneg boundary payload");
            if (pl) {
                memset(pl, 0, plen);
                pl[0] = DEC_PAYLOAD_VERSION;
                pl[1] = DEC_SIGN_POS;
                pl[7] = (uint8_t)(nd & 0xFF);
                pl[6] = (uint8_t)((nd >> 8) & 0xFF);
                pl[5] = (uint8_t)((nd >> 16) & 0xFF);
                pl[8] = 0xFF; pl[9] = 0xFF; pl[10] = 0xFF; pl[11] = 0xFF;
                pl[15] = 0x01;   /* dscale 1 >= -m */
                for (size_t k = 0; k < (size_t)((nd + 1) / 2) - 1; k++)
                    pl[DEC_PAYLOAD_HEADER + k] = 0x99;
                pl[plen - 1] = 0x90;   /* odd count: zero pad nibble */
                ASSERT_EQ_INT(pl[5], 0x02, "boundary fixture count byte 1");
                ASSERT_EQ_INT(pl[7], 0x01, "boundary fixture count byte 3");
                ASSERT_EQ_INT(pl[11], 0xFF, "boundary fixture exponent byte 3");
                dec_t u;
                dec_init(&u);
                ASSERT_EQ_INT(dec_unpack(pl, plen, &u), DEC_OK,
                              "131072 integer digits at m=-1 decode");
                ASSERT_EQ_INT((long long)dec_int_digits(&u),
                              DEC_MAX_INT_DIGITS,
                              "boundary value has exactly 131072 integer digits");
                dec_free(&u);
                free(pl);
            }
        }
        /* exact boundary: 131070 digits at m = +2 fit */
        {
            long long nd = 131070;
            size_t plen = DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2);
            uint8_t *pl = malloc(plen);
            ASSERT_NOT_NULL(pl, "boundary payload");
            if (pl) {
                memset(pl, 0, plen);
                pl[0] = DEC_PAYLOAD_VERSION;
                pl[1] = DEC_SIGN_POS;
                pl[7] = (uint8_t)(nd & 0xFF);
                pl[6] = (uint8_t)((nd >> 8) & 0xFF);
                pl[5] = (uint8_t)((nd >> 16) & 0xFF);
                pl[11] = 0x02;   /* m = +2 */
                pl[15] = 0x02;   /* dscale 0 would fail the lossless rule */
                for (size_t k = 0; k < (size_t)((nd + 1) / 2); k++)
                    pl[DEC_PAYLOAD_HEADER + k] = 0x99;
                dec_t u;
                dec_init(&u);
                ASSERT_EQ_INT(dec_unpack(pl, plen, &u), DEC_OK,
                              "131072 integer digits at m=2 decode");
                ASSERT_EQ_INT((long long)dec_int_digits(&u), DEC_MAX_INT_DIGITS,
                              "boundary value has exactly 131072 integer digits");
                dec_free(&u);
                free(pl);
            }
        }
        /* odd count pad nibble must be zero */
        {
            char err2[64];
            ASSERT_EQ_INT(dec_parse("1", 1, &d, err2, sizeof(err2)), DEC_OK,
                          "one-digit seed");
            uint8_t one[DEC_PAYLOAD_HEADER + 1];
            size_t ol = 0;
            ASSERT_EQ_INT(dec_pack(&d, one, sizeof(one), &ol), DEC_OK,
                          "one-digit packs");
            ASSERT_EQ_INT((int)one[16], 0x10, "pad nibble is zero on pack");
            one[16] = 0x1F;   /* same digit, nonzero pad */
            dec_t u;
            dec_init(&u);
            ASSERT_EQ_INT(dec_unpack(one, ol, &u), DEC_ERR_PAYLOAD,
                          "nonzero pad nibble rejected");
            dec_free(&u);
        }
        /* canonical zero header: nonzero exponent rejected */
        {
            uint8_t zero_pl[DEC_PAYLOAD_HEADER];
            memset(zero_pl, 0, sizeof(zero_pl));
            zero_pl[0] = DEC_PAYLOAD_VERSION;
            zero_pl[1] = DEC_SIGN_ZERO;
            zero_pl[15] = 2;   /* dscale 2 */
            dec_t u;
            dec_init(&u);
            ASSERT_EQ_INT(dec_unpack(zero_pl, sizeof(zero_pl), &u), DEC_OK,
                          "canonical zero payload decodes");
            ASSERT_EQ_INT((long long)u.dscale, 2, "zero payload keeps dscale");
            zero_pl[11] = 0xFF;   /* m = -1 */
            ASSERT_EQ_INT(dec_unpack(zero_pl, sizeof(zero_pl), &u),
                          DEC_ERR_PAYLOAD,
                          "zero payload with nonzero exponent rejected");
            dec_free(&u);
        }
        dec_free(&d);
    }
    {
        dec_t d, u;
        dec_init(&d); dec_init(&u);
        char err[64];
        ASSERT_EQ_INT(dec_parse("1.23", 4, &d, err, sizeof(err)), DEC_OK,
                      "truncation seed");
        uint8_t p[32];
        size_t out_len = 0;
        ASSERT_EQ_INT(dec_pack(&d, p, sizeof(p), &out_len), DEC_OK,
                      "truncation pack");
        ASSERT_EQ_INT(dec_unpack(p, out_len - 1, &u), DEC_ERR_PAYLOAD,
                      "short payload rejected");
        ASSERT_EQ_INT(dec_unpack(p, out_len + 1, &u), DEC_ERR_PAYLOAD,
                      "long payload rejected");
        dec_free(&d); dec_free(&u);
    }

    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-decnum-core", test_decnum_core_run)
```

### Resume-only Task 1c cleanup — remove the unused status-name helper

The integrated scratch build exposed `-Wunused-function` for an unused helper
in the originally approved test file. The final Task 1c file above no longer
contains it. **Fresh execution does not perform this deletion.** At the
reported halted checkpoint only, remove exactly this complete block from
`src/test/cases/test_decnum_core.c` (quoted unique anchor, replacement is
empty). Keep every assertion and test body unchanged:

```c
static const char *dec_status_name(dec_status s) {
    switch (s) {
    case DEC_OK:            return "ok";
    case DEC_ERR_SYNTAX:    return "syntax";
    case DEC_ERR_EXPONENT:  return "exponent";
    case DEC_ERR_RANGE:     return "range";
    case DEC_ERR_PRECISION: return "precision";
    case DEC_ERR_DIV_ZERO:  return "div-zero";
    case DEC_ERR_NOMEM:     return "nomem";
    case DEC_ERR_PAYLOAD:   return "payload";
    case DEC_ERR_INVALID:   return "invalid";
    case DEC_ERR_TRUNCATED: return "truncated";
    default:                return "internal";
    }
}
```

After this deletion, the test file must be byte-identical to the final Task 1c
code block. Header and stub source must remain byte-identical to Tasks 1a/1b.
No other production or test behavior change is part of this amendment.

### Task 1d — build wiring (five anchored edits)

In `build.sh`, apply these exact replacements:

1. Daemon link line — replace the unique text

```
src/db/main.c src/db/config.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
```

with

```
src/db/main.c src/db/config.c src/db/decnum.c src/db/type_desc.c src/db/nql.c src/db/embedded.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
```

2. `LIB_SRCS` — replace the unique line

```make
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/type_desc.c \
```

with

```make
          src/db/io_direct.c src/db/query.c src/db/query_aggregate.c src/db/query_join.c src/db/query_plan.c src/db/query_maint.c src/db/query_schema.c src/db/query_bulk.c src/db/query_find.c src/db/server.c src/db/config.c src/db/decnum.c src/db/type_desc.c \
```

3. Test-daemon (`shard-db-test-server`) link line — retain this edit and replace the unique text

```
src/db/config.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c -Isrc/db
```

with

```
src/db/config.c src/db/decnum.c src/db/type_desc.c src/db/nql.c src/db/embedded.c src/db/test_control.c -Isrc/db
```

4. Test-case registration — replace the unique line

```make
    src/test/cases/test_scalar_csv_update.c \
```

with

```make
    src/test/cases/test_scalar_csv_update.c \
    src/test/cases/test_decnum_core.c \
```

5. Test-runner (`shard-db-test`) source list — replace this unique two-line
anchor. The target and entry point disambiguate it from both the test daemon
and benchmark lists; do not use the repeated config.c/type_desc.c tail.

```make
gcc $MODE_CFLAGS -DTEST_BUILD -o shard-db-test \
    src/test/shard-db-test.c \
```

with

```make
gcc $MODE_CFLAGS -DTEST_BUILD -o shard-db-test \
    src/test/shard-db-test.c \
    src/db/decnum.c \
```

There are four module source lists: production daemon, embedded library, test
daemon, and test runner. The registration is the fifth edit. Do not add the
module to the benchmark target in this phase: it has no decimal caller. The
normal build still compiles that target; this is not authorization to run it.

### Task 1e — RED capture

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-decnum-core
```

Expected red (record the actual counts and paste them into the evidence
document): the build succeeds with zero warnings; essentially every
functional assertion in groups 1–10 fails because the stub rejects every
parse (`DEC_ERR_SYNTAX`) and every operation returns `DEC_ERR_INTERNAL` —
for example `expect_parse("1234567890123456.1234567890", DEC_OK, ...)` fails
as "expected 0 got 1" (status enum), the corruption-group status checks fail
as "expected 6 got 10", and the only `ok` lines are incidental (assertions
that assert a *failure* status the stub also produces, e.g. empty-input
rejection, plus buffer-null guards). If any group-2 malformed-input assertion
passes, that is the stub accidentally matching — note it in the evidence, do
not adjust the test.

Also confirm the rest of the suite is unaffected:

```sh
rtk proxy ./build/bin/shard-db-test run-all
```

Expected: the pre-existing 515 cases stay green (`# total: 17516 passed,
0 failed across 515 cases` plus the new case's red counts, `1..516`).

---

## Task 2 — Lifecycle internals, parse, normalization, exact text (green: groups 1–4)

Replace **five** complete stub definitions in `src/db/decnum.c` —
`dec_normalize`, `dec_parse`, `dec_int_digits`, `dec_text_len`, and
`dec_to_text` — with the production bodies below, and insert the file-local
helpers immediately after the existing `dec_reserve` definition. Anchors:
each stub body is unique in the file (match from the function's signature
through its closing brace). The first production block below replaces the
`dec_normalize` stub; the parse block replaces the `dec_parse` stub; the
final block replaces the `dec_int_digits`, `dec_text_len`, AND `dec_to_text`
stubs — all three, or the module will not compile (duplicate definitions).

```c
/* Same ASCII set as the scalar conversion boundary. */
static int dec_is_space(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

static const uint16_t DEC_POW10[4] = { 1, 10, 100, 1000 };

/* Limb holding the given base-10000 place (0 when outside the value). */
static uint16_t dec_limb_at_place(const dec_t *d, int32_t place) {
    if (place > d->weight) return 0;
    int64_t idx = (int64_t)d->weight - (int64_t)place;
    if (idx >= (int64_t)d->ndigits) return 0;
    return d->digits[idx];
}


/* Digits left of the decimal point, upper-bounded and then adjusted for
   leading zero digits of the top limb — PostgreSQL 18 apply_typmod's
   ddigits. Can be <= 0 for values below 1. */
static int32_t dec_ddigits(const dec_t *d) {
    int32_t dd = (d->weight + 1) * DEC_LIMB_DIGITS;
    if (d->ndigits > 0) {
        uint16_t top = d->digits[0];
        if (top < 10) dd -= 3;
        else if (top < 100) dd -= 2;
        else if (top < 1000) dd -= 1;
    }
    return dd;
}


/* Build limbs for value = ±digits × 10^m; digits is canonical (no
   leading/trailing zero digit, ndig >= 1) and the caller has already
   enforced the integer/fraction digit limits, so the limb count is
   bounded by DEC_MAX_LIMBS. */
static dec_status dec_from_digits(dec_t *out, dec_sign sign,
                                  const char *dg, size_t ndig,
                                  long long m, int32_t dscale) {
    dec_init(out);
    out->dscale = dscale;
    out->sign = sign;

    if (m >= 0) {
        long long ivlen = (long long)ndig + m;
        size_t nlimbs = (size_t)((ivlen + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS);
        dec_status st = dec_reserve(out, (uint16_t)nlimbs);
        if (st != DEC_OK) {
            dec_free(out);   /* failure leaves the canonical zero */
            return st;
        }
        long long lead_pad = (long long)nlimbs * DEC_LIMB_DIGITS - ivlen;
        for (size_t L = 0; L < nlimbs; L++) {
            uint16_t v = 0;
            for (int q = 0; q < DEC_LIMB_DIGITS; q++) {
                long long p = (long long)L * DEC_LIMB_DIGITS + q;
                long long k = p - lead_pad;
                int d = (k >= 0 && k < (long long)ndig) ? dg[k] - '0' : 0;
                v = (uint16_t)(v * 10 + d);
            }
            out->digits[L] = v;
        }
        out->weight = (int32_t)nlimbs - 1;
        out->ndigits = (uint16_t)nlimbs;
    } else {
        long long f = -m;                    /* fractional digits */
        long long ip = (long long)ndig + m;  /* integer digits (may be <= 0) */
        size_t nli = ip > 0 ? (size_t)((ip + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS) : 0;
        size_t nlf = (size_t)((f + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS);
        size_t nlimbs = nli + nlf;
        dec_status st = dec_reserve(out, (uint16_t)nlimbs);
        if (st != DEC_OK) {
            dec_free(out);   /* failure leaves the canonical zero */
            return st;
        }
        if (ip > 0) {
            size_t lead_pad = (size_t)nli * DEC_LIMB_DIGITS - (size_t)ip;
            for (size_t L = 0; L < nli; L++) {
                uint16_t v = 0;
                for (int q = 0; q < DEC_LIMB_DIGITS; q++) {
                    long long p = (long long)(L * DEC_LIMB_DIGITS + q) -
                                  (long long)lead_pad;
                    int d = (p >= 0 && p < (long long)ndig) ? dg[p] - '0' : 0;
                    v = (uint16_t)(v * 10 + d);
                }
                out->digits[L] = v;
            }
        }
        for (size_t L = 0; L < nlf; L++) {
            uint16_t v = 0;
            for (int q = 0; q < DEC_LIMB_DIGITS; q++) {
                long long j = (long long)L * DEC_LIMB_DIGITS + q;
                long long idx = ip + j;   /* index into dg; <0 or >=ndig pads 0 */
                int d = (idx >= 0 && idx < (long long)ndig) ? dg[idx] - '0' : 0;
                v = (uint16_t)(v * 10 + d);
            }
            out->digits[nli + L] = v;
        }
        out->weight = ip > 0 ? (int32_t)nli - 1 : -1;
        out->ndigits = (uint16_t)nlimbs;
    }
    dec_normalize(out);
    return DEC_OK;
}
```

Replacement for the complete stub `dec_normalize`:

```c
void dec_normalize(dec_t *d) {
    size_t lead = 0;
    while (lead < d->ndigits && d->digits[lead] == 0) lead++;
    if (lead > 0) {
        memmove(d->digits, d->digits + lead,
                (size_t)(d->ndigits - lead) * sizeof(uint16_t));
        d->ndigits = (uint16_t)(d->ndigits - lead);
        d->weight -= (int32_t)lead;
    }
    while (d->ndigits > 0 && d->digits[d->ndigits - 1] == 0)
        d->ndigits--;
    if (d->ndigits == 0) {
        d->sign = DEC_SIGN_ZERO;
        d->weight = 0;
    }
}
```

Replacement for the complete stub `dec_parse`:

```c
/* Digit at combined "[int].[frac]" run position p (skips the '.'). */
static char dec_run_digit(const char *val, size_t int_start, size_t int_len,
                          size_t p) {
    return p < int_len ? val[int_start + p]
                       : val[int_start + int_len + 1 + (p - int_len)];
}

dec_status dec_parse(const char *val, size_t vlen, dec_t *out,
                     char *err, size_t errsz) {
    if (err && errsz > 0) err[0] = '\0';
    if (!val || !out) return DEC_ERR_INVALID;
    dec_free(out);   /* release any prior buffer; leaves the canonical zero */
    if (vlen > 0 && memchr(val, 0, vlen) != NULL) {
        if (err && errsz > 0) snprintf(err, errsz, "embedded NUL byte");
        return DEC_ERR_SYNTAX;
    }
    while (vlen > 0 && dec_is_space(val[0])) { val++; vlen--; }
    while (vlen > 0 && dec_is_space(val[vlen - 1])) vlen--;

    size_t i = 0;
    int neg = 0;
    if (vlen > 0 && (val[0] == '+' || val[0] == '-')) {
        neg = (val[0] == '-');
        i = 1;
    }
    size_t int_start = i;
    while (i < vlen && val[i] >= '0' && val[i] <= '9') i++;
    size_t int_len = i - int_start;
    size_t frac_len = 0;
    if (i < vlen && val[i] == '.') {
        i++;
        size_t fs = i;
        while (i < vlen && val[i] >= '0' && val[i] <= '9') i++;
        frac_len = i - fs;
    }

    long long e_val = 0;
    if (i < vlen && (val[i] == 'e' || val[i] == 'E')) {
        i++;
        int e_neg = 0;
        if (i < vlen && (val[i] == '+' || val[i] == '-')) {
            e_neg = (val[i] == '-');
            i++;
        }
        size_t es = i;
        while (i < vlen && val[i] >= '0' && val[i] <= '9') i++;
        size_t el = i - es;
        if (el == 0) {
            if (err && errsz > 0) snprintf(err, errsz, "expected a decimal number");
            return DEC_ERR_SYNTAX;
        }
        /* Bounded-memory exponent policy: strip leading zeros, then cap
           the significant run. Scanning stays linear in the input with
           zero allocations and no value-proportional work. */
        size_t k = 0;
        while (k < el && val[es + k] == '0') k++;
        if (el - k > 7) {
            if (err && errsz > 0)
                snprintf(err, errsz, "exponent out of supported range");
            return DEC_ERR_EXPONENT;
        }
        for (; k < el; k++) e_val = e_val * 10 + (val[es + k] - '0');
        if (e_neg) e_val = -e_val;
    }

    if (i != vlen || (int_len + frac_len) == 0) {
        if (err && errsz > 0) snprintf(err, errsz, "expected a decimal number");
        return DEC_ERR_SYNTAX;
    }

    long long total = (long long)(int_len + frac_len);
    long long E = e_val;
    long long dscale = (long long)frac_len - E;
    if (dscale < 0) dscale = 0;
    if (dscale > DEC_MAX_DSCALE) {
        if (err && errsz > 0)
            snprintf(err, errsz, "more than %d fractional digits",
                     DEC_MAX_FRAC_DIGITS);
        return DEC_ERR_RANGE;
    }

    long long lz = 0;
    while (lz < total &&
           dec_run_digit(val, int_start, int_len, (size_t)lz) == '0')
        lz++;
    if (lz == total) {
        /* Exact zero: canonical sign and limbs; display scale keeps the
           input's fractional span (already bounded). */
        out->sign = DEC_SIGN_ZERO;
        out->dscale = (int32_t)dscale;
        return DEC_OK;
    }
    long long int_digits = (long long)int_len + E - lz;
    if (int_digits < 1) int_digits = 1;
    if (int_digits > DEC_MAX_INT_DIGITS) {
        if (err && errsz > 0)
            snprintf(err, errsz, "more than %d integer digits",
                     DEC_MAX_INT_DIGITS);
        return DEC_ERR_RANGE;
    }
    long long last = total - 1;
    while (last > lz &&
           dec_run_digit(val, int_start, int_len, (size_t)last) == '0')
        last--;
    long long ndig = last - lz + 1;
    long long trail = total - 1 - last;
    long long m = E - (long long)frac_len + trail;

    if (ndig < 1 || ndig > DEC_MAX_COEFF_DIGITS) return DEC_ERR_RANGE;
    size_t digit_bytes = (size_t)ndig;
    char *dg = malloc(digit_bytes);
    if (!dg) return DEC_ERR_NOMEM;
    for (long long p = 0; p < ndig; p++)
        dg[p] = dec_run_digit(val, int_start, int_len, (size_t)(lz + p));
    dec_status st = dec_from_digits(out, neg ? DEC_SIGN_NEG : DEC_SIGN_POS,
                                    dg, (size_t)ndig, m, (int32_t)dscale);
    free(dg);
    return st;
}
```

Replacement for the complete stub `dec_text_len` and `dec_to_text`:

```c
int32_t dec_int_digits(const dec_t *d) {
    if (d->ndigits == 0) return 0;
    int32_t dd = dec_ddigits(d);
    return dd < 1 ? 1 : dd;
}

size_t dec_text_len(const dec_t *d) {
    size_t n = 1;   /* NUL */
    if (d->ndigits == 0) {
        n += 1;
    } else {
        if (d->sign == DEC_SIGN_NEG) n += 1;
        n += (size_t)dec_int_digits(d);
    }
    if (d->dscale > 0) n += 1 + (size_t)d->dscale;
    return n;
}

dec_status dec_to_text(const dec_t *d, char *buf, size_t bufsz,
                       size_t *out_len) {
    if (!d || !buf || !out_len) return DEC_ERR_INVALID;
    size_t need = dec_text_len(d);
    if (bufsz < need) return DEC_ERR_TRUNCATED;
    char *p = buf;
    if (d->ndigits == 0) {
        *p++ = '0';
    } else {
        if (d->sign == DEC_SIGN_NEG) *p++ = '-';
        if (d->weight >= 0) {
            for (int32_t pl = d->weight; pl >= 0; pl--) {
                uint16_t v = dec_limb_at_place(d, pl);
                if (pl == d->weight) {
                    /* top limb: trim leading zeros */
                    char tmp[8];
                    int n = snprintf(tmp, sizeof(tmp), "%u", (unsigned)v);
                    memcpy(p, tmp, (size_t)n);
                    p += n;
                } else {
                    char tmp[8];
                    snprintf(tmp, sizeof(tmp), "%04u", (unsigned)v);
                    memcpy(p, tmp, 4);
                    p += 4;
                }
            }
        } else {
            *p++ = '0';
        }
    }
    if (d->dscale > 0) {
        *p++ = '.';
        for (int32_t j = 1; j <= d->dscale; j++) {
            int32_t q = -((j + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS);
            int32_t idx = 3 + 4 * q + j;
            uint16_t v = dec_limb_at_place(d, q);
            int digit = (v / DEC_POW10[3 - idx]) % 10;
            *p++ = (char)('0' + digit);
        }
    }
    *p = '\0';
    *out_len = need - 1;
    return DEC_OK;
}
```

Verify:

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-decnum-core
```

Expected: groups 1–4 fully green (parse/render/limits/exponent). Groups 5–10
stay red — record the actual counts (zero warnings). The remaining stub
groups must not pass accidentally: `dec_cmp` still returns 0, so only
equal-comparison assertions (e.g. "display scale ignored") are incidentally
ok — note them in the evidence.

### Resume-only Task 2 warning repair

This subsection applies **only** at the checkpoint in PLAN_NOTES-2.md, after
human approval of this amendment. Fresh execution uses the complete amended
Task 2 blocks above and skips this subsection. Preserve the existing header,
unit test, build.sh and both halt records; all earlier executed steps remain.

Remove each following complete block exactly once from the current
`src/db/decnum.c` (each is a unique quoted anchor; replacement is empty):

```c
/* PostgreSQL 18 round_var: per-position powers for within-limb rounding. */
static const uint16_t DEC_ROUND_POWERS[4] = { 0, 1000, 100, 10 };
```

```c
/* Decimal digits carried by one limb (canonical values only). */
static int32_t dec_limb_digits(uint16_t v) {
    return v >= 1000 ? 4 : v >= 100 ? 3 : v >= 10 ? 2 : 1;
}
```

```c
/* Base-10 exponent of the coefficient's final limb digit. */
static long long dec_coeff_exp_raw(const dec_t *d) {
    return (long long)(d->weight - (int32_t)d->ndigits + 1) * DEC_LIMB_DIGITS;
}
```

Then replace this unique allocation block in the current dec_parse:

```c
    char *dg = malloc((size_t)ndig);
    if (!dg) return DEC_ERR_NOMEM;
```

with:

```c
    if (ndig < 1 || ndig > DEC_MAX_COEFF_DIGITS) return DEC_ERR_RANGE;
    size_t digit_bytes = (size_t)ndig;
    char *dg = malloc(digit_bytes);
    if (!dg) return DEC_ERR_NOMEM;
```

No other current source edit is authorized by this resume patch. In particular,
the two moved helper functions and rounding constant are absent until Tasks 7
and 5 respectively; the guarded unpack allocation is applied with Task 7.
After these edits, compare the current Task 2 functions/helpers to the amended
Task 2 blocks, then repeat its normal build and focused run. Require zero
warnings and the same 557 passed/181 failed transitional result before Task 3.
Retain actual counts/statuses; do not rerun completed Task 1 or weaken tests.
If the checkpoint/anchors differ, halt rather than guessing.

## Task 3 — Comparison, hash, integer-digit count (green: groups 5–6)

Replace the complete stub `dec_cmp` with the block below **and delete the
complete stub `dec_hash`** — this one block defines `dec_cmp_mag`, `dec_cmp`,
`dec_hash_byte`, and `dec_hash`; inserting it at the `dec_cmp` stub and
leaving the `dec_hash` stub in place would define `dec_hash` twice. (The
production `dec_int_digits` already landed with Task 2's text block.):

```c
static int dec_cmp_mag(const dec_t *a, const dec_t *b) {
    if (a->weight != b->weight) return a->weight > b->weight ? 1 : -1;
    size_t n = a->ndigits > b->ndigits ? a->ndigits : b->ndigits;
    for (size_t k = 0; k < n; k++) {
        int la = k < a->ndigits ? a->digits[k] : 0;
        int lb = k < b->ndigits ? b->digits[k] : 0;
        if (la != lb) return la > lb ? 1 : -1;
    }
    return 0;
}

int dec_cmp(const dec_t *a, const dec_t *b) {
    if (a->sign == DEC_SIGN_ZERO)
        return b->sign == DEC_SIGN_ZERO ? 0 :
               (b->sign == DEC_SIGN_POS ? -1 : 1);
    if (b->sign == DEC_SIGN_ZERO)
        return a->sign == DEC_SIGN_POS ? 1 : -1;
    if (a->sign != b->sign) return a->sign == DEC_SIGN_POS ? 1 : -1;
    int mag = dec_cmp_mag(a, b);
    return a->sign == DEC_SIGN_POS ? mag : -mag;
}

static uint64_t dec_hash_byte(uint64_t h, uint8_t b) {
    h ^= b;
    return h * 1099511628211ULL;   /* FNV-1a 64 */
}

uint64_t dec_hash(const dec_t *d) {
    uint64_t h = 14695981039346656037ULL;   /* standard FNV-1a 64 offset */
    h = dec_hash_byte(h, (uint8_t)d->sign);
    uint32_t w = (uint32_t)d->weight;
    h = dec_hash_byte(h, (uint8_t)(w >> 24));
    h = dec_hash_byte(h, (uint8_t)(w >> 16));
    h = dec_hash_byte(h, (uint8_t)(w >> 8));
    h = dec_hash_byte(h, (uint8_t)w);
    h = dec_hash_byte(h, (uint8_t)(d->ndigits >> 8));
    h = dec_hash_byte(h, (uint8_t)d->ndigits);
    for (size_t k = 0; k < d->ndigits; k++) {
        h = dec_hash_byte(h, (uint8_t)(d->digits[k] >> 8));
        h = dec_hash_byte(h, (uint8_t)d->digits[k]);
    }
    return h;
}
```

Same verify commands. Expected: groups 5–6 green; 7–10 red. (`dec_cmp`
correctness makes group 7's cancellation/associativity still red via the
`dec_add` stub status.)

## Task 4 — Addition and subtraction (green: group 7)

Insert the block below at the `dec_add` stub (anchor: the complete stub
`dec_status dec_add(const dec_t *a, const dec_t *b, dec_t *out) {` through
its closing brace) **and delete the complete stub `dec_sub`** — the block
defines `dec_add_signed`, `dec_add`, and `dec_sub`; replacing both stubs
with it would define `dec_sub` twice:

```c
/* Shared aligned add/subtract. Magnitudes are limb arrays with a
   separate sign, so no signed-integer negation ever happens (an
   INT64_MIN-shaped magnitude is never negated in two's complement).
   Alignment runs over base-10000 places from each operand's most
   significant place (weight) down to its lowest (weight - ndigits +
   1); limbs accumulate least-significant-first (carry/borrow need
   that direction) and the result is reversed in place into the
   most-significant-first layout. */
static dec_status dec_add_signed(const dec_t *a, const dec_t *b,
                                 int b_negate, dec_t *out) {
    if (!out) return DEC_ERR_INVALID;
    if (out == a || out == b) return DEC_ERR_INVALID;   /* never alias */
    dec_free(out);   /* reused outputs release storage; failures leave zero */
    if (!a || !b) return DEC_ERR_INVALID;
    dec_sign sa = a->sign;
    dec_sign sb = b->sign;
    if (b_negate) {
        if (sb == DEC_SIGN_POS) sb = DEC_SIGN_NEG;
        else if (sb == DEC_SIGN_NEG) sb = DEC_SIGN_POS;
    }
    int32_t maxds = a->dscale > b->dscale ? a->dscale : b->dscale;

    if (sa == DEC_SIGN_ZERO && sb == DEC_SIGN_ZERO) {
        out->dscale = maxds;
        return DEC_OK;
    }
    /* Zero operands: dec_cmp_mag is a magnitude compare and must never
       see a zero (canonical zero's weight 0 would beat a negative
       weight). The result is the other operand, sign applied. */
    if (sa == DEC_SIGN_ZERO || sb == DEC_SIGN_ZERO) {
        const dec_t *nz = sa == DEC_SIGN_ZERO ? b : a;
        if (nz->ndigits > 0) {
            dec_status st = dec_reserve(out, nz->ndigits);
            if (st != DEC_OK) return st;
            memcpy(out->digits, nz->digits,
                   (size_t)nz->ndigits * sizeof(uint16_t));
            out->ndigits = nz->ndigits;
            out->weight = nz->weight;
            out->sign = sa == DEC_SIGN_ZERO ? sb : sa;
        } else {
            out->sign = DEC_SIGN_ZERO;   /* both effective operands zero */
        }
        out->dscale = maxds;
        return DEC_OK;
    }
    if (sa == sb) {
        /* Magnitudes add. Zero operands contribute nothing; at least
           one is nonzero here, so the result carries that sign. */
        int32_t hi_place = INT32_MIN, lo_place = INT32_MAX;
        if (a->ndigits) {
            if (a->weight > hi_place) hi_place = a->weight;
            if (a->weight - (int32_t)a->ndigits + 1 < lo_place)
                lo_place = a->weight - (int32_t)a->ndigits + 1;
        }
        if (b->ndigits) {
            if (b->weight > hi_place) hi_place = b->weight;
            if (b->weight - (int32_t)b->ndigits + 1 < lo_place)
                lo_place = b->weight - (int32_t)b->ndigits + 1;
        }
        int32_t nlimbs = hi_place - lo_place + 1;
        dec_status st = dec_reserve(out, (uint16_t)(nlimbs + 1));
        if (st != DEC_OK) return st;
        int32_t carry = 0;
        for (int32_t k = 0; k < nlimbs; k++) {
            int32_t place = lo_place + k;
            int32_t s = (int32_t)dec_limb_at_place(a, place) +
                        (int32_t)dec_limb_at_place(b, place) + carry;
            out->digits[k] = (uint16_t)(s % DEC_LIMB_BASE);
            carry = s / DEC_LIMB_BASE;
        }
        int32_t n = nlimbs;
        if (carry) { out->digits[n] = (uint16_t)carry; n++; }
        /* digits[k] holds place lo_place + k (least significant
           first); reverse into the most-significant-first layout. */
        for (int32_t lo = 0, hi = n - 1; lo < hi; lo++, hi--) {
            uint16_t t = out->digits[lo];
            out->digits[lo] = out->digits[hi];
            out->digits[hi] = t;
        }
        out->ndigits = (uint16_t)n;
        out->weight = lo_place + n - 1;
        out->sign = sa;
    } else {
        /* Opposite signs: subtract the smaller magnitude from the
           larger; equal magnitudes cancel to the canonical zero. */
        int mag = dec_cmp_mag(a, b);
        if (mag == 0) {
            out->dscale = maxds;   /* out is already the canonical zero */
            return DEC_OK;
        }
        const dec_t *big = mag > 0 ? a : b;
        const dec_t *small = mag > 0 ? b : a;
        int32_t lo_place =
            small->weight - (int32_t)small->ndigits + 1;
        if (big->weight - (int32_t)big->ndigits + 1 < lo_place)
            lo_place = big->weight - (int32_t)big->ndigits + 1;
        int32_t nlimbs = big->weight - lo_place + 1;
        dec_status st = dec_reserve(out, (uint16_t)nlimbs);
        if (st != DEC_OK) return st;
        int32_t borrow = 0;
        for (int32_t k = 0; k < nlimbs; k++) {
            int32_t place = lo_place + k;
            int32_t d = (int32_t)dec_limb_at_place(big, place) -
                        (int32_t)dec_limb_at_place(small, place) - borrow;
            if (d < 0) { d += DEC_LIMB_BASE; borrow = 1; }
            else borrow = 0;
            out->digits[k] = (uint16_t)d;
        }
        /* reverse into the most-significant-first layout */
        for (int32_t lo = 0, hi = nlimbs - 1; lo < hi; lo++, hi--) {
            uint16_t t = out->digits[lo];
            out->digits[lo] = out->digits[hi];
            out->digits[hi] = t;
        }
        out->ndigits = (uint16_t)nlimbs;
        out->weight = big->weight;
        out->sign = mag > 0 ? sa : sb;
    }
    out->dscale = maxds;
    dec_normalize(out);
    if (dec_int_digits(out) > DEC_MAX_INT_DIGITS) {
        dec_free(out);   /* failure leaves the canonical zero */
        return DEC_ERR_RANGE;
    }
    return DEC_OK;
}

dec_status dec_add(const dec_t *a, const dec_t *b, dec_t *out) {
    return dec_add_signed(a, b, 0, out);
}

dec_status dec_sub(const dec_t *a, const dec_t *b, dec_t *out) {
    return dec_add_signed(a, b, 1, out);
}
```

Zero operands never reach `dec_cmp_mag`: the branch is a pure magnitude
compare, and canonical zero's weight 0 would wrongly beat any operand with a
negative weight (0 + 0.5 would underflow into garbage). The zero-operand
block returns the other operand with the applied sign before any magnitude
logic, covering addition and subtraction in both operand orders and every
sign; scaled zeros keep the larger display scale through `maxds`. The
`nlimbs + 1` reservation exists only for the same-sign carry limb; a carry
cannot escape the borrow branch because the larger magnitude bounds the
subtraction.

Same verify commands. Expected: group 7 green; groups 8–10 red.

## Task 5 — Rounding and constrained precision (green: group 8)

Replace the complete stub `dec_round_scale` and stub `dec_apply_precision`
with the respective blocks below. The first block introduces
`DEC_ROUND_POWERS` immediately before the rounding implementation, at its
first consumer; it was deliberately omitted from Task 2 to keep the
intermediate builds warning-clean:

```c
/* PostgreSQL 18 round_var: per-position powers for within-limb rounding. */
static const uint16_t DEC_ROUND_POWERS[4] = { 0, 1000, 100, 10 };

/* PostgreSQL 18 round_var, ported to this limb layout: truncate to the
   kept decimal position and round the dropped remainder half away from
   zero, propagating carry into a possible new leading limb. */
dec_status dec_round_scale(dec_t *d, int32_t rscale) {
    if (!d) return DEC_ERR_INVALID;
    d->dscale = rscale;
    int64_t di = (int64_t)(d->weight + 1) * DEC_LIMB_DIGITS + rscale;
    if (di < 0) {
        d->ndigits = 0;
        d->weight = 0;
        d->sign = DEC_SIGN_ZERO;
        return DEC_OK;
    }
    int32_t keep = (int32_t)((di + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS);
    int32_t di4 = (int32_t)(di % DEC_LIMB_DIGITS);
    if (keep < (int32_t)d->ndigits ||
        (keep == (int32_t)d->ndigits && di4 > 0)) {
        int carry = 0;
        int32_t prop = keep;   /* first limb to receive carry */
        if (di4 == 0) {
            carry = d->digits[keep] >= DEC_LIMB_BASE / 2;
        } else {
            uint16_t pow10 = DEC_ROUND_POWERS[di4];
            int extra = d->digits[keep - 1] % pow10;
            d->digits[keep - 1] = (uint16_t)(d->digits[keep - 1] - extra);
            if (extra >= pow10 / 2) {
                int t = pow10 + d->digits[keep - 1];
                if (t >= DEC_LIMB_BASE) { t -= DEC_LIMB_BASE; carry = 1; }
                d->digits[keep - 1] = (uint16_t)t;
            }
            prop = keep - 1;
        }
        d->ndigits = (uint16_t)keep;
        while (carry) {
            if (prop == 0) {
                /* carry out of the top limb: prepend a new leading limb */
                dec_status st = dec_reserve(d, (uint16_t)(d->ndigits + 1));
                if (st != DEC_OK) {
                    dec_free(d);   /* failure leaves the canonical zero */
                    return st;
                }
                memmove(d->digits + 1, d->digits,
                        (size_t)d->ndigits * sizeof(uint16_t));
                d->digits[0] = 1;
                d->ndigits++;
                d->weight++;
                break;
            }
            prop--;
            int s = d->digits[prop] + carry;
            if (s >= DEC_LIMB_BASE) {
                d->digits[prop] = (uint16_t)(s - DEC_LIMB_BASE);
                carry = 1;
            } else {
                d->digits[prop] = (uint16_t)s;
                carry = 0;
            }
        }
    }
    /* Every publicly returned value meets the canonical invariants
       comparison, hashing and packing rely on: rounding can truncate
       to zero (kept limbs gone) or leave trailing zero limbs. */
    dec_normalize(d);
    return DEC_OK;
}
```

```c
dec_status dec_apply_precision(const dec_t *in, int32_t precision,
                               int32_t scale, dec_t *out,
                               char *err, size_t errsz) {
    if (err && errsz > 0) err[0] = '\0';
    if (!out) return DEC_ERR_INVALID;
    if (out == in) return DEC_ERR_INVALID;   /* never release aliased input */
    dec_free(out);   /* reused outputs release storage; failures leave zero */
    if (!in) return DEC_ERR_INVALID;
    if (precision < 1 || precision > DEC_MAX_PRECISION ||
        scale < DEC_MIN_SCALE || scale > DEC_MAX_SCALE) {
        if (err && errsz > 0)
            snprintf(err, errsz,
                     "precision/scale outside 1..1000 / -1000..1000");
        return DEC_ERR_INVALID;
    }
    /* Clone into out (rounding mutates in place). */
    if (in->ndigits > 0) {
        dec_status st = dec_reserve(out, in->ndigits);
        if (st != DEC_OK) return st;
        memcpy(out->digits, in->digits,
               (size_t)in->ndigits * sizeof(uint16_t));
        out->ndigits = in->ndigits;
        out->weight = in->weight;
        out->sign = in->sign;
    }
    out->dscale = in->dscale;
    dec_status st = dec_round_scale(out, scale);
    if (st != DEC_OK) {
        dec_free(out);
        return st;
    }
    if (out->dscale < 0) out->dscale = 0;   /* never a negative display scale */
    dec_normalize(out);
    /* PostgreSQL 18 apply_typmod precision check, after rounding. */
    int32_t maxdigits = precision - scale;
    int32_t dd = (out->weight + 1) * DEC_LIMB_DIGITS;
    if (dd > maxdigits) {
        for (size_t k = 0; k < out->ndigits; k++) {
            uint16_t dig = out->digits[k];
            if (dig) {
                if (dig < 10) dd -= 3;
                else if (dig < 100) dd -= 2;
                else if (dig < 1000) dd -= 1;
                if (dd > maxdigits) {
                    if (err && errsz > 0)
                        snprintf(err, errsz,
                                 "numeric field overflow: precision %d scale %d "
                                 "holds an absolute value below 10^%d",
                                 precision, scale, maxdigits);
                    dec_free(out);
                    return DEC_ERR_PRECISION;
                }
                break;
            }
            dd -= DEC_LIMB_DIGITS;
        }
    }
    return DEC_OK;
}

/* PostgreSQL 18 select_div_scale (REL_18_0 numeric.c:10135), ported.
   Normalized base-10000 weight and first digit per operand (defaults 0
   for zero), quotient-weight decrement when the first digits order
   ambiguously, 16 significant digits, raw display scales as floors,
   final clamp 0..DEC_AVG_MAX_RSCALE — distinct from the stored
   16383-digit fractional limit. */
```

Same verify commands. Expected: group 8 green; groups 9–10 red.

## Task 6 — Average division (green: group 9)

Insert `dec_select_div_scale` immediately before the stub `dec_div_avg`
(anchor: the complete stub `dec_status dec_div_avg(...)` block), then replace
that complete stub with the production body:

```c
static int32_t dec_select_div_scale(const dec_t *v1, const dec_t *v2) {
    int32_t weight1 = 0, weight2 = 0;
    uint16_t firstdigit1 = 0, firstdigit2 = 0;
    for (size_t i = 0; i < v1->ndigits; i++) {
        firstdigit1 = v1->digits[i];
        if (firstdigit1 != 0) {
            weight1 = v1->weight - (int32_t)i;
            break;
        }
    }
    for (size_t i = 0; i < v2->ndigits; i++) {
        firstdigit2 = v2->digits[i];
        if (firstdigit2 != 0) {
            weight2 = v2->weight - (int32_t)i;
            break;
        }
    }
    int32_t qweight = weight1 - weight2;
    if (firstdigit1 <= firstdigit2) qweight--;
    int32_t rscale = DEC_MIN_SIG_DIGITS - qweight * DEC_LIMB_DIGITS;
    if (rscale < v1->dscale) rscale = v1->dscale;
    if (rscale < v2->dscale) rscale = v2->dscale;
    if (rscale < 0) rscale = 0;
    if (rscale > DEC_AVG_MAX_RSCALE) rscale = DEC_AVG_MAX_RSCALE;
    return rscale;
}

dec_status dec_div_avg(const dec_t *sum, uint64_t count, dec_t *out) {
    if (!out) return DEC_ERR_INVALID;
    if (out == sum) return DEC_ERR_INVALID;   /* never alias */
    dec_free(out);   /* reused outputs release storage; failures leave zero */
    if (!sum) return DEC_ERR_INVALID;
    if (count == 0) return DEC_ERR_DIV_ZERO;

    /* Canonical divisor from the row count (<= 20 digits). */
    char dg[24];
    int nd = 0;
    uint64_t t = count;
    while (t > 0) {
        dg[nd++] = (char)('0' + (int)(t % 10));
        t /= 10;
    }
    for (int k = 0; k < nd / 2; k++) {
        char c = dg[k];
        dg[k] = dg[nd - 1 - k];
        dg[nd - 1 - k] = c;
    }
    dec_t dv;
    dec_status st = dec_from_digits(&dv, DEC_SIGN_POS, dg, (size_t)nd, 0, 0);
    if (st != DEC_OK) return st;

    int32_t rscale = dec_select_div_scale(sum, &dv);

    if (sum->ndigits == 0) {
        dec_free(&dv);
        out->dscale = rscale;   /* out is already the canonical zero */
        return DEC_OK;
    }

    /* PostgreSQL 18 div_var framing: one guard limb beyond the target
       scale makes the final half-away rounding decision exact. */
    int32_t res_weight = sum->weight - dv.weight + 1;
    int32_t res_ndigits =
        res_weight + 1 + (rscale + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS;
    if (res_ndigits < 1) res_ndigits = 1;
    res_ndigits += 1;
    if (res_ndigits > DEC_MAX_LIMBS_ALLOC) {
        dec_free(&dv);
        return DEC_ERR_RANGE;
    }
    st = dec_reserve(out, (uint16_t)res_ndigits);
    if (st != DEC_OK) {
        dec_free(&dv);
        return st;
    }

    /* Exact schoolbook division, one dividend limb at a time. The
       remainder stays below the divisor (< 10000^5 < 2^67), so
       rem*10^4 + limb stays far inside unsigned __int128, and the
       trial quotient (rem/divisor) is always a valid base-10000 limb
       (rem < divisor ⇒ quotient digit <= 10^4 - 1). */
    unsigned __int128 dvmag = 0;
    for (size_t k = 0; k < dv.ndigits; k++)
        dvmag = dvmag * DEC_LIMB_BASE + dv.digits[k];
    /* Scale the limb integer by the exponent of the divisor's lowest
       limb. Normalization strips trailing zero limbs, so that exponent
       (weight - ndigits + 1) is >= 0 for a count, and the product is
       the count itself (< 2^64). */
    int32_t shift = dv.weight - (int32_t)dv.ndigits + 1;
    for (int32_t k = 0; k < shift; k++)
        dvmag *= DEC_LIMB_BASE;

    /* Seed the remainder with the dividend limbs above the first
       quotient place. When the divisor's weight >= 1 the quotient's
       top place sits below the dividend's top, and those high limbs
       must enter the division. The seed is < divisor: it is at most
       10000^(w2-1) while the divisor is >= 10000^(w2-1)*1000, so every
       quotient digit stays a valid base-10000 limb. */
    unsigned __int128 rem = 0;
    for (int32_t p = sum->weight; p > res_weight; p--)
        rem = rem * DEC_LIMB_BASE + dec_limb_at_place(sum, p);
    for (int32_t k = 0; k < res_ndigits; k++) {
        int32_t place = res_weight - k;
        rem = rem * DEC_LIMB_BASE + dec_limb_at_place(sum, place);
        out->digits[k] = (uint16_t)(rem / dvmag);
        rem %= dvmag;
    }
    out->ndigits = (uint16_t)res_ndigits;
    out->weight = res_weight;
    out->sign = sum->sign == DEC_SIGN_NEG ? DEC_SIGN_NEG : DEC_SIGN_POS;
    out->dscale = rscale;
    dec_free(&dv);

    st = dec_round_scale(out, rscale);
    if (st != DEC_OK) return st;
    dec_normalize(out);
    return DEC_OK;
}
```

Same verify commands. Expected: group 9 green; group 10 red.

## Task 7 — Packed serialization (green: group 10)

Insert the block below at the `dec_payload_len` stub (anchor: the complete
stub `size_t dec_payload_len(const dec_t *d) {` through its closing brace)
**and delete the complete stubs `dec_pack` and `dec_unpack`** — the block
defines the two coefficient helpers, three payload helpers and
`dec_payload_len`, `dec_pack`, `dec_unpack`; replacing each stub with it
would multiply-define them. The new helpers use `dec_limb_at_place`,
`DEC_POW10` and `dec_from_digits` from Task 2. `dec_limb_digits` and
`dec_coeff_exp_raw` are introduced here, at their first consumers, so
intermediate stages have no unused definitions:

```c
/* Decimal digits carried by one limb (canonical values only). */
static int32_t dec_limb_digits(uint16_t v) {
    return v >= 1000 ? 4 : v >= 100 ? 3 : v >= 10 ? 2 : 1;
}

/* Base-10 exponent of the coefficient's final limb digit. */
static long long dec_coeff_exp_raw(const dec_t *d) {
    return (long long)(d->weight - (int32_t)d->ndigits + 1) * DEC_LIMB_DIGITS;
}

/* Trailing zero digits of the last limb (canonical values only). */
static int32_t dec_last_limb_trailing_zeros(uint16_t v) {
    if (v % 10 != 0) return 0;
    if (v % 100 != 0) return 1;
    if (v % 1000 != 0) return 2;
    return 3;
}

/* Mathematically canonical digit count: strip the last limb's trailing
   zero digits (including its fraction-padding zeros) from the span. */
static long long dec_coeff_digits(const dec_t *d) {
    long long span =
        (long long)(d->ndigits - 1) * DEC_LIMB_DIGITS +
        dec_limb_digits(d->digits[0]);
    return span - dec_last_limb_trailing_zeros(d->digits[d->ndigits - 1]);
}

/* Digit at decimal exponent e of the coefficient (0 = units digit). */
static int dec_coeff_digit(const dec_t *d, long long e) {
    int32_t q = (int32_t)(e / DEC_LIMB_DIGITS) -
                (e % DEC_LIMB_DIGITS < 0 ? 1 : 0);   /* floor division */
    int32_t idx = 3 + 4 * q - (int32_t)e;
    uint16_t v = dec_limb_at_place(d, q);
    return (v / DEC_POW10[3 - idx]) % 10;
}

size_t dec_payload_len(const dec_t *d) {
    long long nd = d->ndigits == 0 ? 0 : dec_coeff_digits(d);
    return DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2);
}

dec_status dec_pack(const dec_t *d, uint8_t *out, size_t cap,
                    size_t *out_len) {
    if (!d || !out || !out_len) return DEC_ERR_INVALID;
    long long nd = d->ndigits == 0 ? 0 : dec_coeff_digits(d);
    size_t need = DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2);
    if (cap < need) return DEC_ERR_TRUNCATED;
    memset(out, 0, need);
    out[0] = DEC_PAYLOAD_VERSION;
    out[1] = (uint8_t)d->sign;
    /* [2..3] reserved stay 0 */
    uint32_t be32 = (uint32_t)nd;
    out[4] = (uint8_t)(be32 >> 24);
    out[5] = (uint8_t)(be32 >> 16);
    out[6] = (uint8_t)(be32 >> 8);
    out[7] = (uint8_t)be32;
    int64_t m = d->ndigits == 0 ? 0
                                : dec_coeff_exp_raw(d) +
                                  dec_last_limb_trailing_zeros(
                                      d->digits[d->ndigits - 1]);
    uint32_t mbe = (uint32_t)m;
    out[8] = (uint8_t)(mbe >> 24);
    out[9] = (uint8_t)(mbe >> 16);
    out[10] = (uint8_t)(mbe >> 8);
    out[11] = (uint8_t)mbe;
    uint32_t dsbe = (uint32_t)d->dscale;
    out[12] = (uint8_t)(dsbe >> 24);
    out[13] = (uint8_t)(dsbe >> 16);
    out[14] = (uint8_t)(dsbe >> 8);
    out[15] = (uint8_t)dsbe;
    for (long long k = 0; k < nd; k++) {
        int digit = dec_coeff_digit(d, m + (nd - 1 - k));   /* most significant first */
        uint8_t nib = (uint8_t)digit;
        if (k % 2 == 0) out[DEC_PAYLOAD_HEADER + k / 2] |= (uint8_t)(nib << 4);
        else out[DEC_PAYLOAD_HEADER + k / 2] |= nib;
    }
    *out_len = need;
    return DEC_OK;
}

dec_status dec_unpack(const uint8_t *buf, size_t len, dec_t *out) {
    if (!out) return DEC_ERR_INVALID;
    dec_free(out);   /* reused outputs release storage; failures leave zero */
    if (!buf) return DEC_ERR_INVALID;
    if (len < DEC_PAYLOAD_HEADER) return DEC_ERR_PAYLOAD;
    if (buf[0] != DEC_PAYLOAD_VERSION) return DEC_ERR_PAYLOAD;
    dec_sign sign = (dec_sign)buf[1];
    if (sign > DEC_SIGN_NEG) return DEC_ERR_PAYLOAD;
    if (buf[2] != 0 || buf[3] != 0) return DEC_ERR_PAYLOAD;
    long long nd = (long long)((uint32_t)buf[4] << 24 |
                               (uint32_t)buf[5] << 16 |
                               (uint32_t)buf[6] << 8 |
                               (uint32_t)buf[7]);
    long long m = (long long)(int32_t)((uint32_t)buf[8] << 24 |
                                       (uint32_t)buf[9] << 16 |
                                       (uint32_t)buf[10] << 8 |
                                       (uint32_t)buf[11]);
    long long dscale = (long long)((uint32_t)buf[12] << 24 |
                                   (uint32_t)buf[13] << 16 |
                                   (uint32_t)buf[14] << 8 |
                                   (uint32_t)buf[15]);
    if (nd > DEC_MAX_COEFF_DIGITS) return DEC_ERR_RANGE;
    if ((sign == DEC_SIGN_ZERO) != (nd == 0)) return DEC_ERR_PAYLOAD;
    if (len != DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2))
        return DEC_ERR_PAYLOAD;
    if (dscale < 0 || dscale > DEC_MAX_DSCALE) return DEC_ERR_PAYLOAD;
    if (nd == 0) {
        /* canonical zero carries no exponent */
        if (m != 0) return DEC_ERR_PAYLOAD;
        out->dscale = (int32_t)dscale;
        return DEC_OK;
    }
    /* Integer digits are the digit count shifted by the exponent; the
       bound holds for every exponent sign (m + nd <= 0 is a value
       below 1 and always fits). */
    if (m + nd > DEC_MAX_INT_DIGITS) return DEC_ERR_RANGE;
    if (m < 0 && -m > DEC_MAX_FRAC_DIGITS) return DEC_ERR_RANGE;
    if (dscale < (m < 0 ? -m : 0)) return DEC_ERR_PAYLOAD;

    if (nd < 1 || nd > DEC_MAX_COEFF_DIGITS) return DEC_ERR_RANGE;
    size_t digit_bytes = (size_t)nd;
    char *dg = malloc(digit_bytes);
    if (!dg) return DEC_ERR_NOMEM;
    for (long long k = 0; k < nd; k++) {
        uint8_t byte = buf[DEC_PAYLOAD_HEADER + k / 2];
        int nib = (k % 2 == 0) ? (byte >> 4) : (byte & 0x0F);
        if (nib > 9) { free(dg); return DEC_ERR_PAYLOAD; }
        dg[k] = (char)('0' + nib);
    }
    if (dg[0] == '0' || dg[nd - 1] == '0') {
        free(dg);
        return DEC_ERR_PAYLOAD;   /* digit string must be canonical */
    }
    if (nd % 2 == 1 &&
        (buf[DEC_PAYLOAD_HEADER + nd / 2] & 0x0F) != 0) {
        free(dg);
        return DEC_ERR_PAYLOAD;   /* unused low nibble must be zero pad */
    }
    dec_status st = dec_from_digits(out, sign, dg, (size_t)nd, m,
                                    (int32_t)dscale);
    free(dg);
    return st;
}
```

Same verify commands. Expected: **all groups green** — the full case passes.

## Task 8 — Full suite, independent oracle, evidence, checksum

### 8a — Full suite

```sh
rtk proxy env SKIP_TESTS=1 ./build.sh
rtk proxy ./build/bin/shard-db-test run test-decnum-core
rtk proxy ./build/bin/shard-db-test run-all
```

Expected: zero compiler warnings; `# total: 17516 + <new passed>, 0 failed
across 516 cases`, exit 0. Copy the actual lines into the evidence document.
No sanitizer run anywhere in this phase (branch-series gate later, unchanged):
the probe, the unit case, and every replay in this plan compile with ordinary
`cc` only.

### 8b — Independent numerical oracle (mandatory)

**Normative statement:** the C implementation is never its own oracle. The
oracle is CPython 3's `decimal` module under an explicit context, driven by
the scratch harness below. Both harness files are complete here; the executor
creates them under `/tmp/p2-oracle/`, runs the chain, and pastes the outputs
into the evidence document. They are scratch tooling (like Phase 0's
`avg_scale_validate.py` and Phase 1's index-text-oom probe) and stay out of
`git status` — the committed regression net is `test_decnum_core.c` alone.
The probe builds with ordinary `cc -O1` only.

**Probe — `/tmp/p2-oracle/decnum_probe.c`** (complete file). It links the
real module and prints one pipe-separated result line per input case:

```c
/* Scratch Phase 2 oracle probe. Reads cases from argv[1]
   (id|op|a|b|count|P|S, "-" for absent fields), executes the real
   decnum module, writes results to argv[2] as
   id|status|text|dscale|hash|plen|payload_hex_or_dash.
   Not part of the committed suite. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include "decnum.h"

static void emit(FILE *f, long long id, dec_status st, const dec_t *d) {
    char *buf = malloc(DEC_TEXT_MAX);
    if (!buf) { fprintf(f, "%lld|%d|||||\n", id, (int)st); return; }
    size_t n = 0;
    const char *text = "";
    if (d && dec_to_text(d, buf, DEC_TEXT_MAX, &n) == DEC_OK) text = buf;
    size_t plen = d ? dec_payload_len(d) : 0;
    char *hex = NULL;
    if (d && plen <= 4096) {
        uint8_t *p = malloc(plen);
        size_t ol = 0;
        if (p && dec_pack(d, p, plen, &ol) == DEC_OK) {
            hex = malloc(plen * 2 + 1);
            if (hex) {
                for (size_t k = 0; k < ol; k++)
                    snprintf(hex + k * 2, 3, "%02x", p[k]);
            }
        }
        free(p);
    }
    fprintf(f, "%lld|%d|%s|%d|%" PRIu64 "|%zu|%s\n",
            id, (int)st, text, d ? (int)d->dscale : 0,
            d ? dec_hash(d) : 0, plen, hex ? hex : "-");
    free(hex);
    free(buf);
}

int main(int argc, char **argv) {
    if (argc != 3) { fprintf(stderr, "usage: probe IN OUT\n"); return 2; }
    FILE *in = fopen(argv[1], "r");
    FILE *out = fopen(argv[2], "w");
    if (!in || !out) { perror("open"); return 2; }
    char line[4096];
    long long id = 0;
    while (fgets(line, sizeof(line), in)) {
        id = atoll(strtok(line, "|"));
        const char *op = strtok(NULL, "|");
        const char *a = strtok(NULL, "|");
        const char *b = strtok(NULL, "|");
        const char *cs = strtok(NULL, "|");
        const char *ps = strtok(NULL, "|");
        const char *ss = strtok(NULL, "|");
        dec_t x, y, r;
        dec_init(&x); dec_init(&y); dec_init(&r);
        char err[64];
        dec_status st = DEC_ERR_INVALID;
        if (strcmp(op, "parse") == 0) {
            st = dec_parse(a, strlen(a), &r, err, sizeof(err));
        } else if (strcmp(op, "add") == 0 || strcmp(op, "sub") == 0) {
            if (dec_parse(a, strlen(a), &x, err, sizeof(err)) == DEC_OK &&
                dec_parse(b, strlen(b), &y, err, sizeof(err)) == DEC_OK)
                st = strcmp(op, "add") == 0 ? dec_add(&x, &y, &r)
                                            : dec_sub(&x, &y, &r);
        } else if (strcmp(op, "prec") == 0) {
            if (dec_parse(a, strlen(a), &x, err, sizeof(err)) == DEC_OK)
                st = dec_apply_precision(&x, (int32_t)atol(ps),
                                         (int32_t)atol(ss), &r,
                                         err, sizeof(err));
        } else if (strcmp(op, "divavg") == 0) {
            if (dec_parse(a, strlen(a), &x, err, sizeof(err)) == DEC_OK)
                st = dec_div_avg(&x, (uint64_t)strtoull(cs, NULL, 10), &r);
        }
        emit(out, id, st, st == DEC_OK ? &r : NULL);
        dec_free(&x); dec_free(&y); dec_free(&r);
    }
    fclose(in);
    fclose(out);
    return 0;
}
```

**Oracle — `/tmp/p2-oracle/decnum_oracle.py`** (complete file). It (a)
generates the case file (mandated analytic cases + seeded random cases,
fixed seed), (b) independently computes every expected value with
`decimal` under an explicit context (own grammar/canonicalization, own
select_div_scale port, own FNV/payload construction — never the C module),
and (c) diffs the probe output:

```python
#!/usr/bin/env python3
"""Phase 2 independent numerical oracle for the exact decimal core.

    decnum_oracle.py gen CASES            # write the case file
    decnum_oracle.py check CASES ACTUAL   # validate probe output

Values are computed with decimal.Decimal under an explicit context
(ROUND_HALF_UP = ties away from zero). Nothing here imports or trusts
the C implementation.
"""
import random
import sys
from decimal import Decimal, getcontext, ROUND_HALF_UP

# CPython 3.11+ caps int->str conversion; wide values exceed the default.
sys.set_int_max_str_digits(200000)

# coefficient digits + guard: far above any quotient we compute
getcontext().prec = 400000

SEED = 20261022
MAX_INT_DIGITS = 131072
MAX_FRAC_DIGITS = 16383
MAX_COEFF_DIGITS = MAX_INT_DIGITS + MAX_FRAC_DIGITS
FNV_OFFSET = 14695981039346656037  # standard FNV-1a 64 offset basis
FNV_PRIME = 1099511628211
MASK64 = (1 << 64) - 1

STATUS = {"ok": 0, "syntax": 1, "exponent": 2, "range": 3, "precision": 4,
          "divzero": 5, "nomem": 6, "payload": 7, "invalid": 8,
          "truncated": 9, "internal": 10}


def fnv1a(data):
    h = FNV_OFFSET
    for b in data:
        h = ((h ^ b) * FNV_PRIME) & MASK64
    return h


def dec_hash(sign, weight, limbs):
    data = bytes([sign]) + (weight & 0xFFFFFFFF).to_bytes(4, "big") \
        + len(limbs).to_bytes(2, "big")
    for limb in limbs:
        data += limb.to_bytes(2, "big")
    return fnv1a(data)


def digits_of(text):
    """Independent parser: returns (sign, coeff_digits, m, dscale, status).

    value = ±coeff × 10^m with coeff canonical (no lead/trail zeros);
    status mirrors the C contract: syntax / exponent / range.
    """
    s = text.strip(" \t\r\n")
    if "\x00" in text:
        return None, None, None, None, "syntax"
    i = 0
    neg = False
    if s and s[0] in "+-":
        neg = s[0] == "-"
        i = 1
    j = i
    while j < len(s) and s[j].isdigit() and s[j].isascii():
        j += 1
    int_len = j - i
    frac_len = 0
    if j < len(s) and s[j] == ".":
        j += 1
        k = j
        while k < len(s) and s[k].isdigit() and s[k].isascii():
            k += 1
        frac_len = k - j
        j = k
    e = 0
    if j < len(s) and s[j] in "eE":
        j += 1
        eneg = False
        if j < len(s) and s[j] in "+-":
            eneg = s[j] == "-"
            j += 1
        k = j
        while k < len(s) and s[k].isdigit() and s[k].isascii():
            k += 1
        run = s[j:k]
        if not run:
            return None, None, None, None, "syntax"
        stripped = run.lstrip("0")
        if len(stripped) > 7:
            return None, None, None, None, "exponent"
        e = int(stripped) if stripped else 0
        if eneg:
            e = -e
        j = k
    if j != len(s) or int_len + frac_len == 0:
        return None, None, None, None, "syntax"
    E = e
    dscale = max(0, frac_len - E)
    if dscale > MAX_FRAC_DIGITS:
        return None, None, None, None, "range"
    digits = s[i:i + int_len] + s[i + int_len + 1:i + int_len + 1 + frac_len]
    total = int_len + frac_len
    p = 0
    while p < total and digits[p] == "0":
        p += 1
    if p == total:
        return (1 if neg else 0), "", E - frac_len, dscale, "ok"  # zero
    int_digits = int_len + E - p
    if int_digits < 1:
        int_digits = 1
    if int_digits > MAX_INT_DIGITS:
        return None, None, None, None, "range"
    last = total - 1
    while last > p and digits[last] == "0":
        last -= 1
    coeff = digits[p:last + 1]
    trail = total - 1 - last
    m = E - frac_len + trail
    return (2 if neg else 0), coeff, m, dscale, "ok"


def coeff_to_decimal(sign, coeff, m):
    if not coeff:
        return Decimal(0)
    d = Decimal((1 if sign == 2 else 0, tuple(int(c) for c in coeff), m))
    return d


def to_limbs(coeff, m):
    """Canonical base-10000 limbs + weight exactly as the contract defines."""
    v = coeff_to_decimal(0, coeff, m)  # positive
    t = v.as_tuple()
    ds = list(t.digits)
    exp = t.exponent
    # integer digit string of the positive value
    if exp >= 0:
        iv = "".join(str(x) for x in ds) + "0" * exp
        frac = ""
    else:
        f = -exp
        if len(ds) > f:
            iv = "".join(str(x) for x in ds[:len(ds) - f])
            frac = "".join(str(x) for x in ds[len(ds) - f:])
        else:
            iv = "0"
            frac = "0" * (f - len(ds)) + "".join(str(x) for x in ds)
    nli = (len(iv) + 3) // 4 if iv != "0" else 0
    nlf = (len(frac) + 3) // 4
    ivp = iv.rjust(nli * 4, "0") if nli else ""
    frp = frac.ljust(nlf * 4, "0") if nlf else ""
    limbs = [int(ivp[a * 4:a * 4 + 4] or "0") for a in range(nli)] + \
            [int(frp[a * 4:a * 4 + 4]) for a in range(nlf)]
    weight = nli - 1 if nli else -1
    while limbs and limbs[0] == 0:
        limbs.pop(0)
        weight -= 1
    while limbs and limbs[-1] == 0:
        limbs.pop()
    if not limbs:
        weight = 0
    return weight, limbs


def render(sign, coeff, m, dscale):
    v = coeff_to_decimal(sign, coeff, m)
    if v == 0:
        return "0." + "0" * dscale if dscale else "0"
    iv = int(abs(v).scaleb(dscale))
    s = str(iv).rjust(dscale + 1, "0")
    if dscale:
        s = s[:-dscale] + "." + s[-dscale:]
    return ("-" if sign == 2 else "") + s


def payload_bytes(sign, coeff, m, dscale):
    nd = len(coeff)
    out = bytearray(16)
    out[0] = 1
    out[1] = sign
    out[4:8] = nd.to_bytes(4, "big")
    out[8:12] = (m & 0xFFFFFFFF).to_bytes(4, "big")
    out[12:16] = dscale.to_bytes(4, "big")
    packed = bytearray((nd + 1) // 2)
    for k, c in enumerate(coeff):
        nib = int(c)
        if k % 2 == 0:
            packed[k // 2] |= nib << 4
        else:
            packed[k // 2] |= nib
    return bytes(out) + bytes(packed)


def pg_weight_first(v):
    """Positive nonzero Decimal -> (base-10000 weight, first group)."""
    t = v.as_tuple()
    a = v.adjusted()
    w = a // 4
    n = a - 4 * w + 1
    first = 0
    for i in range(n):
        # digits[] is most-significant-first: the digit at exponent e
        # sits at index exp + len - 1 - e; implied zeros fall outside.
        e = a - i
        k = t.exponent + len(t.digits) - 1 - e
        d = t.digits[k] if 0 <= k < len(t.digits) else 0
        first = first * 10 + d
    return w, first


def select_div_scale(v1, dscale1, v2, dscale2):
    w1, fd1 = pg_weight_first(abs(v1)) if v1 != 0 else (0, 0)
    w2, fd2 = pg_weight_first(abs(v2)) if v2 != 0 else (0, 0)
    qw = w1 - w2
    if fd1 <= fd2:
        qw -= 1
    rs = 16 - 4 * qw
    rs = max(rs, dscale1, dscale2, 0)
    return min(rs, 1000)


def round_prec(check):
    """apply_typmod port: False when the rounded value overflows P-S."""
    v, P, S = check
    if v == 0:
        return True
    maxdigits = P - S
    return (v.adjusted() + 1) <= maxdigits


def gen_case(op, a, b=None, count=None, P=None, S=None):
    fields = [op, a, b if b is not None else "-",
              str(count) if count is not None else "-",
              str(P) if P is not None else "-", str(S) if S is not None else "-"]
    return "|".join(fields)


def gen():
    cases = []
    # mandated analytic cases
    analytic = [
        ("parse", "1234567890123456.1234567890"), ("parse", "-1234567890123456.1234567890"),
        ("parse", "1234567890123456.1234567891"), ("parse", "1234567890123456.1234567889"),
        ("parse", "1.2300"), ("parse", "1.0e2"), ("parse", "0.00100"),
        ("parse", "-0"), ("parse", "-0.00"), ("parse", "0e5"),
        ("parse", "9007199254740993"), ("parse", "18446744073709551615"),
        ("parse", "9223372036854775808"), ("parse", ".5"), ("parse", "5."),
        ("parse", "1e131071"), ("parse", "1e-16383"), ("parse", "99999e131067"),
        ("parse", "1e131072"), ("parse", "1e-16384"),
        ("parse", ""), ("parse", "1e"), ("parse", "1..2"), ("parse", "abc"),
        ("parse", "1e99999999"), ("add", "9223372036854775807", "1"),
        ("add", "-9223372036854775808", "-1"),
        ("add", "1.0000000000000001", "-1"),
        ("add", "1e131071", "1e131071"),
        ("prec", "99.95", None, None, 3, 1),
        ("prec", "0.00999", None, None, 3, 5),
        ("prec", "0.01", None, None, 3, 5),
        ("prec", "0.009995", None, None, 3, 5),
        ("prec", "999949", None, None, 4, -2),
        ("prec", "999994", None, None, 4, -2),
        ("prec", "2.5", None, None, 2, 0), ("prec", "-2.5", None, None, 2, 0),
        ("prec", "1234567890123456.1234567890", None, None, 26, 10),
        ("divavg", "3", None, 2), ("divavg", "1", None, 2),
        ("divavg", "10", None, 3), ("divavg", "2", None, 3),
        ("divavg", "1", None, 3), ("divavg", "5", None, 5),
        ("divavg", "0", None, 2), ("divavg", "13", None, 3),
        ("divavg", "1", None, 10000000), ("divavg", "1", None, 100000000),
        ("divavg", "10000", None, 9999), ("divavg", "99999", None, 10000),
        ("divavg", "1234567890123456.1234567890", None, 1),
        ("add", "0", "0.5"), ("sub", "0", "0.5"),
        ("add", "0.5", "0"), ("sub", "0.5", "0"),
        ("add", "0", "-0.5"), ("sub", "-0.5", "0"),
        ("add", "0.00", "0.5"), ("add", "0.5", "0.00"),
        ("add", "0", "0"), ("sub", "0", "0"),
        ("add", "1.23", "-1.23"), ("sub", "1.23", "1.23"),
        ("add", "-1.23", "1.2300"),
        ("add", "0.1", "-0.1"), ("sub", "0.1", "0.1"),
    ]
    numbered = list(analytic)
    rng = random.Random(SEED)
    for _ in range(120):
        digits = "".join(rng.choice("0123456789") for _ in range(rng.randint(1, 40)))
        frac = "".join(rng.choice("0123456789") for _ in range(rng.randint(0, 30)))
        text = digits + ("." + frac if frac else "")
        if rng.random() < 0.3:
            text += "e" + str(rng.randint(-4000, 4000))
        if rng.random() < 0.4:
            text = "-" + text
        numbered.append(("parse", text))
    for _ in range(80):
        mk = lambda: "".join(rng.choice("0123456789") for _ in range(rng.randint(1, 25)))
        a = mk() + "." + mk()
        b = mk() + "." + mk()
        if rng.random() < 0.5:
            a = "-" + a
        if rng.random() < 0.5:
            b = "-" + b
        numbered.append((rng.choice(["add", "sub"]), a, b))
    for _ in range(80):
        v = "".join(rng.choice("0123456789") for _ in range(rng.randint(1, 20)))
        numbered.append(("prec", v, None, None,
                         rng.randint(1, 1000), rng.randint(-1000, 1000)))
    for _ in range(80):
        v = "".join(rng.choice("0123456789") for _ in range(rng.randint(1, 30)))
        if rng.random() < 0.5:
            v += "." + "".join(rng.choice("0123456789") for _ in range(rng.randint(1, 12)))
        if rng.random() < 0.5:
            v = "-" + v
        count = rng.choice([1, 2, 3, 7, 9999, 10**6, 10**12, 2**63 - 1, 2**64 - 1,
                            rng.randint(1, 10**6)])
        numbered.append(("divavg", v, None, count))
    for i, c in enumerate(numbered):
        cases.append(str(i) + "|" + gen_case(*c))
    with open(sys.argv[2], "w") as f:
        f.write("\n".join(cases) + "\n")
    print(f"wrote {len(cases)} cases (seed {SEED})")


def check(case_path, actual_path):
    with open(case_path) as f:
        case_lines = [l.rstrip("\n") for l in f if l.strip()]
    with open(actual_path) as f:
        actual_lines = [l.rstrip("\n") for l in f if l.strip()]
    assert len(case_lines) == len(actual_lines), "line count mismatch"
    failed = 0
    ctx = getcontext()
    for cl, al in zip(case_lines, actual_lines):
        cid, op, a, b, cs, ps, ss = (cl.split("|") + ["-"] * 7)[:7]
        fields = al.split("|")
        got = {"id": int(fields[0]), "status": int(fields[1])}
        got["text"] = fields[2]
        got["dscale"] = int(fields[3]) if fields[3] else 0
        got["hash"] = int(fields[4]) if fields[4] else 0
        got["plen"] = int(fields[5]) if fields[5] else 0
        got["hex"] = fields[6] if len(fields) > 6 else "-"
        want_status = "ok"

        def fail(why):
            nonlocal failed
            failed += 1
            print(f"FAIL id={cid} op={op}: {why}")

        if op == "parse":
            sign, coeff, m, dscale, want_status = digits_of(a)
            if want_status != "ok":
                if got["status"] != STATUS[want_status]:
                    fail(f"status {got['status']} != {STATUS[want_status]}")
                continue
            exp_text = render(sign, coeff, m, dscale)
            sign_eff = (1 if sign == 0 else sign) if coeff else 0
            m_eff = m if coeff else 0
            w, limbs = to_limbs(coeff, m) if coeff else (0, [])
            exp_hash = dec_hash(sign_eff, w, limbs) if coeff else dec_hash(0, 0, [])
            exp_plen = 16 + (len(coeff) + 1) // 2
            exp_hex = payload_bytes(sign_eff, coeff, m_eff, dscale).hex() \
                if exp_plen <= 4096 else None
        elif op in ("add", "sub"):
            sa, ca, ma, da, s1 = digits_of(a)
            sb, cb, mb, db, s2 = digits_of(b)
            if s1 != "ok" or s2 != "ok":
                fail("case generation bug: non-ok operand")
                continue
            va = coeff_to_decimal(sa, ca, ma)
            vb = coeff_to_decimal(sb, cb, mb)
            v = va + vb if op == "add" else va - vb
            dscale = max(da, db)
            sign = 0 if v == 0 else (2 if v < 0 else 1)
            tv = v.normalize() if v != 0 else v
            coeff = "".join(str(x) for x in tv.as_tuple().digits)
            m = tv.as_tuple().exponent
            if v == 0:
                coeff = ""
                m = 0   # canonical zero payload carries no exponent
            elif coeff == "0":
                coeff = ""
            exp_text = render(sign, coeff, m, dscale)
            w, limbs = to_limbs(coeff, m) if coeff else (0, [])
            sign_hash = (1 if sign == 0 else sign) if coeff else 0
            exp_hash = dec_hash(sign_hash, w, limbs) if coeff else dec_hash(0, 0, [])
            want_status = "ok"
            if v != 0 and (v.adjusted() + 1) > MAX_INT_DIGITS:
                want_status = "range"
            exp_plen = 16 + (len(coeff) + 1) // 2
            exp_hex = payload_bytes(sign, coeff, m, dscale).hex() \
                if exp_plen <= 4096 else None
        elif op == "prec":
            sign, coeff, m, dscale, s1 = digits_of(a)
            P, S = int(ps), int(ss)
            if not (1 <= P <= 1000 and -1000 <= S <= 1000):
                want_status = "invalid"
                exp_text = None
            else:
                v = coeff_to_decimal(sign, coeff, m)
                q = v.quantize(Decimal(1).scaleb(-S), rounding=ROUND_HALF_UP)
                if not round_prec((q, P, S)):
                    want_status = "precision"
                    exp_text = None
                else:
                    dscale = max(S, 0)
                    t = q.as_tuple()
                    coeff = "".join(str(x) for x in t.digits)
                    m = t.exponent
                    while coeff.endswith("0") and len(coeff) > 1:
                        coeff = coeff[:-1]
                        m += 1
                    if q == 0:
                        sign, coeff, m = 0, "", 0
                    exp_text = render(sign, coeff, m, dscale)
            if exp_text is None:
                if got["status"] != STATUS[want_status]:
                    fail(f"status {got['status']} != {STATUS[want_status]}")
                continue
            w, limbs = to_limbs(coeff, m) if coeff else (0, [])
            sign_hash = (1 if sign == 0 else sign) if coeff else 0
            sign_pay = sign_hash
            exp_hash = dec_hash(sign_hash, w, limbs) if coeff else dec_hash(0, 0, [])
            exp_plen = 16 + (len(coeff) + 1) // 2
            exp_hex = payload_bytes(sign_pay, coeff, m, dscale).hex() \
                if exp_plen <= 4096 else None
        elif op == "divavg":
            sign, coeff, m, dscale, s1 = digits_of(a)
            count = int(cs)
            if count == 0:
                if got["status"] != STATUS["divzero"]:
                    fail("count 0 not div-zero")
                continue
            v = coeff_to_decimal(sign, coeff, m)
            n = Decimal(count)
            rs = select_div_scale(v, dscale, n, 0)
            q = (v / n).quantize(Decimal(1).scaleb(-rs), rounding=ROUND_HALF_UP)
            sign = 0 if q == 0 else (2 if q < 0 else 1)
            t = q.as_tuple()
            coeff = "".join(str(x) for x in t.digits)
            m = t.exponent
            while coeff.endswith("0") and len(coeff) > 1:
                coeff = coeff[:-1]
                m += 1
            if q == 0:
                sign, coeff, m = 0, "", 0
            exp_text = render(sign, coeff, m, rs)
            sign = (1 if sign == 0 else sign) if coeff else 0
            w, limbs = to_limbs(coeff, m) if coeff else (0, [])
            exp_hash = dec_hash(sign, w, limbs) if coeff else dec_hash(0, 0, [])
            exp_plen = 16 + (len(coeff) + 1) // 2
            exp_hex = payload_bytes(sign, coeff, m, rs).hex() \
                if exp_plen <= 4096 else None
        else:
            fail("unknown op")
            continue

        if got["status"] != STATUS["ok"]:
            fail(f"status {got['status']} != 0")
        elif got["text"] != exp_text:
            fail(f"text {got['text']!r} != {exp_text!r}")
        elif got["dscale"] != (dscale if op != "divavg" else rs):
            fail(f"dscale {got['dscale']}")
        elif got["hash"] != exp_hash:
            fail(f"hash {got['hash']} != {exp_hash}")
        elif got["plen"] != exp_plen:
            fail(f"plen {got['plen']} != {exp_plen}")
        elif exp_hex is not None and got["hex"] != exp_hex:
            fail(f"payload hex mismatch")
    total = len(case_lines)
    print(f"{total - failed} passed, {failed} failed of {total}")
    return 1 if failed else 0


if __name__ == "__main__":
    if sys.argv[1] == "gen":
        gen()
    elif sys.argv[1] == "check":
        sys.exit(check(sys.argv[2], sys.argv[3]))
    else:
        print("usage: gen CASES | check CASES ACTUAL", file=sys.stderr)
        sys.exit(2)
```

**Exact oracle commands** (ordinary compilation only — this phase runs no
sanitizers; see the execution rules):

```sh
mkdir -p /tmp/p2-oracle
# (create the two files above from this plan)
rtk proxy cc -O1 -Isrc/db /tmp/p2-oracle/decnum_probe.c src/db/decnum.c \
  -o /tmp/p2-oracle/decnum_probe
python3 /tmp/p2-oracle/decnum_oracle.py gen /tmp/p2-oracle/cases.txt
rtk proxy /tmp/p2-oracle/decnum_probe /tmp/p2-oracle/cases.txt /tmp/p2-oracle/actual.txt
python3 /tmp/p2-oracle/decnum_oracle.py check /tmp/p2-oracle/cases.txt /tmp/p2-oracle/actual.txt
```

Expected: the probe exits 0, and the oracle prints `426 passed, 0 failed of
426` (66 analytic + 360 seeded-random cases, seed 20261022 — the generator
prints the exact count). Any mismatch is a defect to root-cause and fix —
the oracle is normative for value semantics, never adjusted to match the
implementation. (The oracle deliberately reimplements the grammar/limit
arithmetic and the scale-selection port instead of calling anything from the
module; the value math itself is pure `decimal`.) If a genuine oracle bug is
found, fix the oracle, rerun the whole chain, and record the correction in
the evidence like Phase 0 did for its five corrected expectations.

**Allocation-failure coverage (deterministic, ordinary build):** the module
has no production allocation seam (the Phase 1 decision), so the committed
suite cannot inject NOMEM. The scratch probe below substitutes
`malloc`/`realloc`, includes the real `decnum.c`, and fails the Nth
allocation to prove the output contract on both allocating stages of parse
and unpack, plus the plain add-overflow state:

```c
/* Scratch Phase 2 allocation-failure probe. Substitutes malloc/realloc,
   includes the real module, and fails the Nth allocation to prove the
   output contract: every failure leaves the canonical zero. Not part of
   the committed suite (no production allocation seam). Ordinary build:
   cc -O1 -Isrc/db decnum_oom_probe.c -o decnum_oom_probe
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "decnum.h"

static long oom_countdown = -1;   /* -1 = never fail; N = fail allocation N */
static long oom_seen = 0;
static void *oom_malloc(size_t n) {
    oom_seen++;
    if (oom_countdown > 0 && oom_seen == oom_countdown) return NULL;
    return malloc(n);
}
static void *oom_realloc(void *p, size_t n) {
    oom_seen++;
    if (oom_countdown > 0 && oom_seen == oom_countdown) return NULL;
    return realloc(p, n);
}
#define malloc oom_malloc
#define realloc oom_realloc
#include "decnum.c"
#undef malloc
#undef realloc

static dec_t z;

static int check_oom(const char *label, dec_status st, const dec_t *d) {
    int ok = st == DEC_ERR_NOMEM && d->sign == DEC_SIGN_ZERO &&
             d->weight == 0 && d->ndigits == 0 &&
             dec_cmp(d, &z) == 0 && dec_hash(d) == dec_hash(&z);
    printf("%s: status=%d sign=%d nd=%u w=%d cmp0=%d hash_eq=%d %s\n",
           label, (int)st, (int)d->sign, d->ndigits, d->weight,
           dec_cmp(d, &z), dec_hash(d) == dec_hash(&z), ok ? "ok" : "FAIL");
    return ok;
}

int main(void) {
    dec_init(&z);
    int failed = 0;
    const char *frac = "1.25";
    const char *good = "1";

    /* count the allocations parse needs (dg buffer + limb reserve) */
    dec_t d;
    dec_init(&d);
    oom_seen = 0;
    dec_status st = dec_parse(frac, strlen(frac), &d, NULL, 0);
    long parse_allocs = oom_seen;
    printf("parse(\"1.25\") ok=%d uses %ld allocations\n", (int)st, parse_allocs);
    failed |= st != DEC_OK || parse_allocs != 2;

    /* fail each allocation stage of parse; assert status AND state */
    for (long n = 1; n <= parse_allocs; n++) {
        dec_free(&d);
        oom_seen = 0;
        oom_countdown = n;
        st = dec_parse(frac, strlen(frac), &d, NULL, 0);
        oom_countdown = -1;
        failed |= !check_oom("parse fail", st, &d);
    }

    /* unpack: same two stages (digit buffer + limb reserve) */
    uint8_t payload[DEC_PAYLOAD_HEADER + 2] = {0};
    size_t plen = 0;
    st = dec_parse(good, strlen(good), &d, NULL, 0);
    st = dec_pack(&d, payload, sizeof(payload), &plen);
    for (long n = 1; n <= 2; n++) {
        dec_free(&d);
        oom_seen = 0;
        oom_countdown = n;
        st = dec_unpack(payload, plen, &d);
        oom_countdown = -1;
        failed |= !check_oom("unpack fail", st, &d);
    }

    /* add overflow leaves the canonical zero (plain path, no OOM) */
    char *mx = malloc((size_t)DEC_MAX_INT_DIGITS + 1);
    memset(mx, '9', (size_t)DEC_MAX_INT_DIGITS);
    mx[DEC_MAX_INT_DIGITS] = '\0';
    dec_t a, b, r;
    dec_init(&a); dec_init(&b); dec_init(&r);
    dec_parse(mx, (size_t)DEC_MAX_INT_DIGITS, &a, NULL, 0);
    dec_parse("1", 1, &b, NULL, 0);
    st = dec_add(&a, &b, &r);
    int ok = st == DEC_ERR_RANGE && r.sign == DEC_SIGN_ZERO &&
             r.ndigits == 0 && dec_cmp(&r, &z) == 0;
    printf("add overflow: status=%d sign=%d nd=%u cmp0=%d %s\n",
           (int)st, (int)r.sign, r.ndigits, dec_cmp(&r, &z), ok ? "ok" : "FAIL");
    failed |= !ok;

    free(mx);
    dec_free(&a); dec_free(&b); dec_free(&r); dec_free(&d);
    printf("%s\n", failed ? "FAILURES PRESENT" : "all allocation-failure states canonical");
    return failed ? 1 : 0;
}
```

```sh
rtk proxy cc -O1 -Isrc/db /tmp/p2-oracle/decnum_oom_probe.c \
  -o /tmp/p2-oracle/decnum_oom_probe
rtk proxy /tmp/p2-oracle/decnum_oom_probe
```

Expected: `parse("1.25")` reports exactly 2 allocations; failing either one
yields `DEC_ERR_NOMEM` with `sign=0, nd=0, w=0, cmp0=0, hash_eq=1`; the add
overflow line reports `status=3` with the same canonical state; exit 0.

Optionally (not a gate, per the Phase 0 decision): a disposable PostgreSQL 18
instance may capture a comparison table for the average cases. Never connect
to a user's finance database.

### 8c — Evidence document and checksum

Create `docs/plans/2026-10-22-phase2-core-evidence.md` recording: input
revision, the Task 1 red transcript (actual failed counts + example reasons),
per-task green transcripts, the final full-suite line, the probe/oracle
summary lines, any oracle corrections, and the re-verified protected-plan
checksum. Keep raw logs in `/tmp/p2-oracle/`; paste the load-bearing lines
only.

```sh
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md
rtk git status --short
rtk git diff --stat
```

Expected checksum: `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
Expected `git status`: only the new tracked-file candidates (`src/db/decnum.h`,
`src/db/decnum.c`, `src/test/cases/test_decnum_core.c`, `build.sh`,
the evidence document) plus the pre-existing untracked planning files. **Stop
here: leave everything uncommitted for plan-aware and blind raw-diff review;
the human authorizes any local Phase 2 commit separately.**

## Red/green summary

| Stage | Command | Expected |
| --- | --- | --- |
| Task 1 RED | `rtk proxy ./build/bin/shard-db-test run test-decnum-core` | stub rejects all parses / all ops fail; roughly three quarters of assertions fail with status mismatches (planning replay: 185 passed, 424 failed — actual counts pasted) |
| Task 1 baseline | `rtk proxy ./build/bin/shard-db-test run-all` | 515 pre-existing cases stay green (`17516 passed, 0 failed`) |
| Tasks 2–7 | build + focused run | each build compiles and the task's groups turn green; later groups stay red (planning replay: Task 2 557/181, Task 3 540/198 — real comparisons replace the always-equal stub, Task 4 647/154, Task 5 754/92, Task 6 815/73, Task 7 888/0) |
| Task 8 GREEN | full suite + probe + oracle | 516 cases all green; oracle `0 failed of 426` |

## No new dependencies

- Runtime: none. `decnum.c` uses libc only (`stdlib`, `string`, `stdio`).
- Build/test: none (the module joins existing build lists).
- Dev-time validation: CPython 3 `decimal` (stdlib), run manually from
  `/tmp/p2-oracle/`; the committed suite never invokes Python. No supply-chain
  or licensing impact; nothing to vendor.

## Open decisions for the human (flagged, not silently chosen)

1. **Zero with an over-limit fractional span is rejected.** `dec_parse`
   applies the 16383-fractional-digit limit uniformly, so
   `0.` + 16384 zeros is `DEC_ERR_RANGE` even though the value is zero.
   PostgreSQL's stored dscale ceiling is 16383 and our packed display-scale
   field follows the same bound; accepting giant zero-fraction text would
   need a special case that later storage phases would have to re-implement.
   Alternative: special-case zero to clamp its display scale. Recommended:
   keep the uniform rejection (simple, matches the stored-scale limit).
2. **Sum display scale = max of operand display scales** (PostgreSQL
   `add_var`). The alternative — min, or value-canonical scale — would lose
   trailing zeros across accumulation. No action needed; recorded as policy.
3. **Exponent digit-run policy:** strip leading zeros, then > 7 significant
   digits → `DEC_ERR_EXPONENT`. This intentionally differs from Phase 1's
   legacy `conv_numeric_text` (which rejects any run longer than 6 digits
   before stripping zeros); the legacy route keeps its own behavior until
   Phase 4. PostgreSQL accepts long leading-zero runs, which the new policy
   matches.
4. **Hash choice (settled):** FNV-1a 64 with the standard offset basis
   14695981039346656037 and prime 1099511628211, over the canonical form,
   pinned by known-vector tests in the unit case. xxh128 is a viable
   alternative via a standalone vendored xxhash.h (XXH_INLINE_ALL) — the
   earlier claim that it would pull in storage.c was wrong; FNV-1a wins on
   zero vendored code, not on quality. No algorithm swap during
   implementation. Phase 5 index keys remain an order-preserving encoding
   and will not use this hash.
5. **Oracle tooling location:** the probe and Python script are scratch
   (complete sources in this plan, executed from `/tmp/p2-oracle/`, excluded
   from the commit per the repo's "raw probes out of version control"
   practice). Alternative: commit them under `src/test/oracle/` for
   re-runnability. Recommended: keep scratch now; if the human wants them
   versioned, that is a two-file addition at commit time.
6. **decnum.c linked into the daemon/lib/test builds now** while unused:
   proves the daemon's exact warning flags, keeps later phases' wiring
   one-line, and `-flto --gc-sections` drops it from the shipped binary.
   Alternative: defer all build-list edits to Phase 3. Recommended: link now.

## Milestone check

Phase 2 is complete when: the case is green in the full suite, the oracle
differential passes with ordinary compilation, the evidence document exists
with real transcripts, the protected checksum still matches, and nothing is
committed. This does **not** unblock the finance milestone — packed storage
(Phase 3), write/schema wiring (Phase 4), exact criteria/indexes (Phase 5),
exact aggregates (Phase 6), and the remaining phases each require their own
approved child plan on top of this core.

## Post-review fix round — regressions and production fixes (2026-10-23)

Authoritative for the post-raw-diff-review fix round only
([review](2026-10-23-phase2-raw-diff-review.md), findings RAW-01/RAW-02 =
SPEC-01/SPEC-02). Execution order is mandatory: **(1)** apply the two test
insertion blocks below to `src/test/cases/test_decnum_core.c` and capture
the real RED (the 12 new failure-contract assertions must fail against the
unfixed module while the existing 888 and the 8 immediately-true neighbor
assertions pass); **(2)** apply the two production replacements; **(3)**
re-run the focused case (expect all green), the ordinary oracle (426/0 —
no generated case reaches the new overflow), the allocation-failure probe
(exit 0 — parse's allocation count is unchanged), and the full suite. Never
weaken or reorder the new assertions. All anchors below are unique in the
current tree; a mismatch halts per the standing rule.

### Regression 1 — RAW-01, rounded-average overflow (group 9)

Insert after the count-0 rejection block (anchor: the complete block below,
exactly once):

```c
    {
        dec_t sum, out;
        dec_init(&sum); dec_init(&out);
        ASSERT_EQ_INT(dec_div_avg(&sum, 0, &out), DEC_ERR_DIV_ZERO,
                      "count 0 rejected");
        dec_free(&sum); dec_free(&out);
    }
```

Insertion (replacement = anchor + this block):

```c
    {
        /* RAW-01: rounding the quotient can carry into a 131073rd
           integer digit; the final average must respect the bare
           integer limit and leave the canonical zero on failure. */
        size_t n_int = (size_t)DEC_MAX_INT_DIGITS;
        size_t n_frac = 1001;
        char *s = malloc(n_int + 1 + n_frac + 1);
        dec_t sum, out, z;
        dec_init(&sum); dec_init(&out); dec_init(&z);
        ASSERT_NOT_NULL(s, "avg overflow builder");
        if (s) {
            memset(s, '9', n_int);
            s[n_int] = '.';
            memset(s + n_int + 1, '9', n_frac);
            s[n_int + 1 + n_frac] = '\0';
            char err[64];
            ASSERT_EQ_INT(dec_parse(s, n_int + 1 + n_frac, &sum, err,
                                    sizeof(err)), DEC_OK,
                          "avg overflow sum parses");
            ASSERT_EQ_INT(dec_div_avg(&sum, 1, &out), DEC_ERR_RANGE,
                          "rounded average past 131072 integer digits is a "
                          "range error");
            ASSERT_EQ_INT((int)out.sign, (int)DEC_SIGN_ZERO,
                          "average overflow leaves the canonical zero sign");
            ASSERT_EQ_INT((long long)out.weight, 0,
                          "average overflow leaves weight 0");
            ASSERT_EQ_INT((long long)out.ndigits, 0,
                          "average overflow leaves no limbs");
            ASSERT_EQ_INT(dec_cmp(&out, &z), 0,
                          "average overflow output compares as zero");
            ASSERT_TRUE(dec_hash(&out) == dec_hash(&z),
                        "average overflow output hashes as zero");
            free(s);
        }
        dec_free(&sum); dec_free(&out); dec_free(&z);
    }
    {
        /* neighboring valid boundary: 1000 fractional nines need no
           round-up and stay exactly at the integer limit */
        size_t n_int = (size_t)DEC_MAX_INT_DIGITS;
        size_t n_frac = 1000;
        char *s = malloc(n_int + 1 + n_frac + 1);
        dec_t sum, out;
        dec_init(&sum); dec_init(&out);
        ASSERT_NOT_NULL(s, "avg boundary builder");
        if (s) {
            memset(s, '9', n_int);
            s[n_int] = '.';
            memset(s + n_int + 1, '9', n_frac);
            s[n_int + 1 + n_frac] = '\0';
            char err[64];
            ASSERT_EQ_INT(dec_parse(s, n_int + 1 + n_frac, &sum, err,
                                    sizeof(err)), DEC_OK,
                          "avg boundary sum parses");
            ASSERT_EQ_INT(dec_div_avg(&sum, 1, &out), DEC_OK,
                          "rounded average at the valid boundary succeeds");
            ASSERT_EQ_INT((long long)dec_int_digits(&out),
                          DEC_MAX_INT_DIGITS,
                          "boundary average keeps exactly 131072 integer "
                          "digits");
            free(s);
        }
        dec_free(&sum); dec_free(&out);
    }
```

### Regression 2 — RAW-02, NULL text into a reused output (group 7c)

Insert before the block's final free line (anchor: the complete text below,
exactly once):

```c
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0,
                      "unpack failure leaves canonical zero");
        dec_free(&out); dec_free(&zero); dec_free(&a); dec_free(&b);
```

Insertion (replacement = the first two anchor lines + this block + the
final free line):

```c
        /* RAW-02: NULL text into a reused output must release the prior
           buffer and leave the canonical zero, not retain the value.
           The seed carries nonzero weight and display scale so every
           contract assertion below genuinely fails on an unfixed
           module (a "42" seed would make the weight and scale checks
           vacuously pass against the stale value). */
        ASSERT_EQ_INT(dec_parse("42000.50", 8, &out, err, sizeof(err)), DEC_OK,
                      "reused output seed for NULL text");
        ASSERT_EQ_INT(dec_parse(NULL, 0, &out, NULL, 0), DEC_ERR_INVALID,
                      "NULL text rejected");
        ASSERT_EQ_INT((int)out.sign, (int)DEC_SIGN_ZERO,
                      "NULL text failure leaves the canonical zero sign");
        ASSERT_EQ_INT((long long)out.weight, 0,
                      "NULL text failure leaves weight 0");
        ASSERT_EQ_INT((long long)out.ndigits, 0,
                      "NULL text failure leaves no limbs");
        ASSERT_TRUE(out.digits == NULL,
                    "NULL text failure releases the prior buffer");
        ASSERT_EQ_INT((long long)out.dscale, 0,
                      "NULL text failure clears display scale");
        ASSERT_EQ_INT(dec_cmp(&out, &zero), 0,
                      "NULL text failure output compares as zero");
        /* NULL output pointer stays rejected without dereference */
        ASSERT_EQ_INT(dec_parse("42", 2, NULL, NULL, 0), DEC_ERR_INVALID,
                      "NULL output pointer rejected");
```

### Production fix 1 — `dec_div_avg` final range check

Replace the complete text below at the end of `dec_div_avg` (exactly once):

```c
    st = dec_round_scale(out, rscale);
    if (st != DEC_OK) return st;
    dec_normalize(out);
    return DEC_OK;
}
```

with:

```c
    st = dec_round_scale(out, rscale);
    if (st != DEC_OK) return st;
    dec_normalize(out);
    /* RAW-01: rounding can carry the quotient past the bare integer
       limit (a maximum-sized sum rounded up at the selected scale);
       re-check after normalization so a success status never escapes
       with a value the packed decoder would reject. */
    if (dec_int_digits(out) > DEC_MAX_INT_DIGITS) {
        dec_free(out);   /* failure leaves the canonical zero */
        return DEC_ERR_RANGE;
    }
    return DEC_OK;
}
```

### Production fix 2 — `dec_parse` guard order

Replace the complete text below at the head of `dec_parse` (exactly once):

```c
    if (!val || !out) return DEC_ERR_INVALID;
    dec_free(out);   /* release any prior buffer; leaves the canonical zero */
```

with:

```c
    if (!out) return DEC_ERR_INVALID;
    dec_free(out);   /* release any prior buffer; leaves the canonical zero */
    if (!val) return DEC_ERR_INVALID;
```

(The preceding `if (err && errsz > 0) err[0] = '\0';` line is untouched.
NULL output stays rejected before any dereference; a valid reused output is
released before the NULL-text rejection, so every output-producing failure
leaves the canonical zero as the header promises.)

### Expected results

- RED round (test insertions only): 897 passed, 12 failed — the six
  RAW-01 failure-contract assertions and six RAW-02 failure-contract
  assertions fail; all 888 existing assertions and the 9 immediately-true
  neighbor/validation assertions pass (21 new assertions total). Record
  the actual counts.
- Correction made during execution, before any production fix: the first
  RED capture used the seed literal `"42"`, whose weight 0 and display
  scale 0 made the RAW-02 weight and scale assertions pass vacuously
  against the stale value (actual interim capture: 899 passed, 10
  failed). The seed was corrected to `"42000.50"` (weight 1, dscale 2) so
  all six RAW-02 assertions are genuinely RED, matching this section's
  stated bar; the interim capture is retained in the evidence document.
- After both production fixes: `909 passed, 0 failed` (exit 0), oracle
  `426 passed, 0 failed`, OOM probe exit 0 (still exactly 2 parse
  allocations), full suite `18425 passed, 0 failed across 516 cases`,
  exit 0. Paste actuals into the evidence document.

## Post-review fix round 2 — public rounding boundary (2026-10-23)

Authoritative for the second raw-diff-review round only (fresh blind
finding in public `dec_round_scale`; the RAW-01/RAW-02 fixes are confirmed
correct and unchanged). Execution order is mandatory: **(1)** apply the
test insertion below and capture the real RED (the seven new
failure-contract assertions must fail against the unfixed module while all
existing assertions and the boundary/validation guard-rail assertions
pass); **(2)** apply the three production replacements plus the header
contract update; **(3)** re-run the focused case (all green), the ordinary
oracle (426/0), the allocation-failure probe (exit 0) and the full suite.
All anchors below are unique in the current tree; a mismatch halts per the
standing rule.

### Regression — public rounding boundary (after group 7b)

Insert after the direct-rounding block (anchor: the complete text below,
exactly once):

```c
            dec_free(&v); dec_free(&want);
        }
    }

    /* ---- group 7c: output lifecycle (reuse and failure) ---- */
```

Insertion (replacement = anchor with the three new blocks between the
group-7b close and the group-7c comment):

```c
            dec_free(&v); dec_free(&want);
        }
    }
    {
        /* fix round 2: rounding at scale 0 can carry into a 131073rd
           integer digit; the public boundary must fail with the
           canonical zero. */
        size_t n_int = (size_t)DEC_MAX_INT_DIGITS;
        char *s = malloc(n_int + 3);
        dec_t v, z;
        dec_init(&v); dec_init(&z);
        ASSERT_NOT_NULL(s, "round overflow builder");
        if (s) {
            memset(s, '9', n_int);
            s[n_int] = '.';
            s[n_int + 1] = '9';
            s[n_int + 2] = '\0';
            char err[64];
            ASSERT_EQ_INT(dec_parse(s, n_int + 2, &v, err, sizeof(err)),
                          DEC_OK, "round overflow seed parses");
            ASSERT_EQ_INT((long long)dec_int_digits(&v), DEC_MAX_INT_DIGITS,
                          "round overflow seed at the integer limit");
            ASSERT_EQ_INT(dec_round_scale(&v, 0), DEC_ERR_RANGE,
                          "rounding into a 131073rd integer digit is a "
                          "range error");
            ASSERT_EQ_INT((int)v.sign, (int)DEC_SIGN_ZERO,
                          "round range failure leaves the canonical zero "
                          "sign");
            ASSERT_EQ_INT((long long)v.ndigits, 0,
                          "round range failure leaves no limbs");
            ASSERT_EQ_INT(dec_cmp(&v, &z), 0,
                          "round range failure output compares as zero");
            free(s);
        }
        dec_free(&v); dec_free(&z);
    }
    {
        /* display scale is bounded by the frozen 16383 fractional-digit
           domain on both sides of zero */
        dec_t v;
        dec_init(&v);
        char err[64];
        ASSERT_EQ_INT(dec_parse("1", 1, &v, err, sizeof(err)), DEC_OK,
                      "dscale domain seed");
        ASSERT_EQ_INT(dec_round_scale(&v, DEC_MAX_DSCALE + 1), DEC_ERR_RANGE,
                      "scale 16384 rejected");
        ASSERT_EQ_INT((int)v.sign, (int)DEC_SIGN_ZERO,
                      "scale domain failure leaves the canonical zero sign");
        ASSERT_EQ_INT(dec_parse("1", 1, &v, err, sizeof(err)), DEC_OK,
                      "positive boundary seed");
        ASSERT_EQ_INT(dec_round_scale(&v, DEC_MAX_DSCALE), DEC_OK,
                      "scale 16383 accepted");
        ASSERT_EQ_INT((long long)v.dscale, DEC_MAX_DSCALE,
                      "scale 16383 kept as display scale");
        ASSERT_EQ_INT(dec_parse("1", 1, &v, err, sizeof(err)), DEC_OK,
                      "negative scale seed");
        ASSERT_EQ_INT(dec_round_scale(&v, -DEC_MAX_DSCALE), DEC_OK,
                      "scale -16383 accepted");
        ASSERT_EQ_INT(dec_parse("1", 1, &v, err, sizeof(err)), DEC_OK,
                      "negative domain seed");
        ASSERT_EQ_INT(dec_round_scale(&v, -DEC_MAX_DSCALE - 1), DEC_ERR_RANGE,
                      "scale -16384 rejected");
        dec_free(&v);
    }
    {
        /* constrained assignment keeps reporting precision overflow
           even when rounding carries past the bare integer limit */
        size_t n_int = (size_t)DEC_MAX_INT_DIGITS;
        char *s = malloc(n_int + 3);
        dec_t in, out;
        dec_init(&in); dec_init(&out);
        ASSERT_NOT_NULL(s, "prec boundary builder");
        if (s) {
            memset(s, '9', n_int);
            s[n_int] = '.';
            s[n_int + 1] = '9';
            s[n_int + 2] = '\0';
            char err[192] = {0};
            ASSERT_EQ_INT(dec_parse(s, n_int + 2, &in, err, sizeof(err)),
                          DEC_OK, "prec boundary seed parses");
            ASSERT_EQ_INT(dec_apply_precision(&in, 1000, 0, &out, err,
                                              sizeof(err)),
                          DEC_ERR_PRECISION,
                          "constrained overflow stays a precision error");
            free(s);
        }
        dec_free(&in); dec_free(&out);
    }

    /* ---- group 7c: output lifecycle (reuse and failure) ---- */
```

### Production edit 1 — `dec_round_scale` entry domain check

Replace (exactly once):

```c
dec_status dec_round_scale(dec_t *d, int32_t rscale) {
    if (!d) return DEC_ERR_INVALID;
    d->dscale = rscale;
```

with:

```c
dec_status dec_round_scale(dec_t *d, int32_t rscale) {
    if (!d) return DEC_ERR_INVALID;
    /* The display scale is bounded by the frozen 16383 fractional-digit
       domain; a wider scale would pack but never decode. */
    if (rscale > DEC_MAX_DSCALE || rscale < -DEC_MAX_DSCALE) {
        dec_free(d);   /* failure leaves the canonical zero */
        return DEC_ERR_RANGE;
    }
    d->dscale = rscale;
```

### Production edit 2 — `dec_round_scale` post-carry limit check

Replace (exactly once):

```c
    /* Every publicly returned value meets the canonical invariants
       comparison, hashing and packing rely on: rounding can truncate
       to zero (kept limbs gone) or leave trailing zero limbs. */
    dec_normalize(d);
    return DEC_OK;
}
```

with:

```c
    /* Every publicly returned value meets the canonical invariants
       comparison, hashing and packing rely on: rounding can truncate
       to zero (kept limbs gone) or leave trailing zero limbs. */
    dec_normalize(d);
    /* The carry into a new leading limb can push a limit-respecting
       value one digit past the bare integer limit (a maximum-sized
       value rounded up at scale 0); re-check so a success status never
       escapes with a value the packed decoder would reject. */
    if (dec_int_digits(d) > DEC_MAX_INT_DIGITS) {
        dec_free(d);   /* failure leaves the canonical zero */
        return DEC_ERR_RANGE;
    }
    return DEC_OK;
}
```

### Production edit 3 — `dec_apply_precision` preserves the precision error

Replace (exactly once):

```c
    out->dscale = in->dscale;
    dec_status st = dec_round_scale(out, scale);
    if (st != DEC_OK) {
        dec_free(out);
        return st;
    }
```

with:

```c
    out->dscale = in->dscale;
    dec_status st = dec_round_scale(out, scale);
    if (st != DEC_OK) {
        dec_free(out);
        if (st == DEC_ERR_RANGE) {
            /* Rounding carried past the bare integer limit; with
               maxdigits <= 2000 for every declared (P,S) that is also
               a precision overflow, so constrained assignment keeps
               reporting DEC_ERR_PRECISION. */
            if (err && errsz > 0)
                snprintf(err, errsz,
                         "numeric field overflow: precision %d scale %d "
                         "holds an absolute value below 10^%d",
                         precision, scale, precision - scale);
            return DEC_ERR_PRECISION;
        }
        return st;
    }
```

(The validated `P`/`S` domain guarantees `scale` is inside
`dec_round_scale`'s new bounds, so the translation only ever sees a genuine
post-rounding value overflow. Execution correction, before the fix was
first built green: the message originally referenced `maxdigits`, which is
declared only at the later precision check — the first build failed with
`'maxdigits' undeclared`, and the message now computes the identical
`precision - scale` expression inline.)

### Production edit 4 — header contract update

Replace the complete `dec_round_scale` comment block in `src/db/decnum.h`
(exactly once):

```c
/* Round the magnitude to `scale` fractional digits, ties away from
   zero (PostgreSQL 18 round_var semantics, including carry into a new
   leading limb). scale < 0 rounds left of the point. Sets dscale to
   scale (callers clamp negative display scale where the contract
   requires it — see dec_apply_precision). The result is canonical:
   rounding to zero yields the canonical zero, never a signed or
   weighted zero, and no trailing zero limbs survive. In-place: on
   DEC_ERR_NOMEM `d` is left as the canonical zero. */
dec_status dec_round_scale(dec_t *d, int32_t scale);
```

with:

```c
/* Round the magnitude to `scale` fractional digits, ties away from
   zero (PostgreSQL 18 round_var semantics, including carry into a new
   leading limb). scale < 0 rounds left of the point. |scale| must be
   <= DEC_MAX_DSCALE — DEC_ERR_RANGE otherwise, since the packed
   display-scale field cannot represent a wider scale — and
   DEC_ERR_RANGE when the carry produces more than DEC_MAX_INT_DIGITS
   integer digits (a maximum-sized value rounded up can cross the bare
   limit). Sets dscale to scale (callers clamp negative display scale
   where the contract requires it — see dec_apply_precision; the P/S
   domain validation keeps every constrained assignment inside these
   bounds, and a range failure there is reported as DEC_ERR_PRECISION).
   The result is canonical: rounding to zero yields the canonical zero,
   never a signed or weighted zero, and no trailing zero limbs survive.
   In-place: on any failure (DEC_ERR_NOMEM or DEC_ERR_RANGE) `d` is
   left as the canonical zero. */
dec_status dec_round_scale(dec_t *d, int32_t scale);
```

### Expected results

- RED round (test insertion only): 922 passed, 7 failed — the four
  round-overflow failure-contract assertions and the three scale-domain
  rejection assertions fail against the unfixed module; all 909 existing
  assertions and the 13 boundary/validation guard-rail assertions pass
  (20 new assertions total). Record the actual counts.
- After the production edits: `929 passed, 0 failed` (exit 0), oracle
  `426 passed, 0 failed`, OOM probe exit 0, full suite
  `18445 passed, 0 failed across 516 cases`, exit 0. Paste actuals into
  the evidence document.
