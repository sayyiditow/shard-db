# Phase 2 decimal core — execution evidence

Date: 2026-10-23 (execution) on branch `fix/phase1-safe-scalar-conversions`,
HEAD `f5b80b4c58e49cfd8ac8b74ed9c1366f7b3a3130` throughout (never moved).
Plan: `docs/plans/2026-10-22-phase2-decimal-core-plan.md`, executed through
three approved revisions, each checksum-verified before the work it
authorized:

| Revision | SHA256 | Scope |
| --- | --- | --- |
| Original approved | `092f9bed3215a7a6bef7acc1c19afcf01cf53b3f87d1fef44518a0f0a227a425` | Tasks 1–8; halted at Task 1e (build-wiring gap) → `PLAN_NOTES.md` |
| Build-wiring amendment | `58378f603a5d9d75c393f52fb32f5ea48f932c6bff9541591af31a5b30f5e570` | Edit 5 (runner list) + resume-only Task 1c helper deletion; halted at Task 2 verify (warning mismatch) → `PLAN_NOTES-2.md` |
| Warning amendment | `8f508ff99b80bf4e885e048a4e16b219b1067bf669c533b267688bf62081ff88` | Resume-only Task 2 warning repair (three helper removals + guarded parse allocation); moved definitions to Tasks 5/7; guarded unpack allocation with Task 7 |

Protected certification plan verified by hash only at every checkpoint —
contents never read:

```
fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135  docs/plans/2026-10-19-durability-certification-rig.md
```

Both halt records (`PLAN_NOTES.md`, `PLAN_NOTES-2.md`) are preserved verbatim;
raw logs and scratch artifacts live in `/tmp/p2-oracle/` (outside version
control). All edits were applied through byte-exact, count-checked anchors
(extracted verbatim from the plan's fenced blocks; every anchor matched
exactly once).

## Task 1e — stub-first RED (captured 2026-10-22 under the wiring amendment)

Build: `rtk proxy env SKIP_TESTS=1 ./build.sh` → exit 0, zero warnings.

Focused RED (`rtk proxy ./build/bin/shard-db-test run test-decnum-core`):

```
# test-decnum-core: 185 passed, 424 failed
exit code 168
```

Example failure reasons (stub rejects every parse with `DEC_ERR_SYNTAX`;
every operation returns `DEC_ERR_INTERNAL`):

```
not ok 1 - original numeric(26,10) value parses exactly
#   expected 0 got 1
not ok 2 - original numeric(26,10) value parses exactly
#   expected '1234567890123456.1234567890' got ''
```

206 of the failures are status mismatches of the form `expected 0 got 1`.
Note: the plan's illustrative corruption-group reason "expected 6 got 10"
does not appear; the corruption assertions instead fail one step earlier at
the pack call (`expected 0 got 10`), because the stub `dec_pack` returns
`DEC_ERR_INTERNAL` before any payload exists. The binding expectations —
185/424, exit 168 — matched the warning-amendment's integrated verification
exactly.

Incidentally passing (185 total): all 19 group-2 grammar rejections (the
stub's `DEC_ERR_SYNTAX` is the expected status, e.g. "ok 52 - empty input
rejected") plus buffer-null guards; test unadjusted, per the plan's
instruction.

Baseline: `rtk proxy ./build/bin/shard-db-test run-all` → exit 1 (expected,
new case red):

```
1..516
# total: 17701 passed, 424 failed across 516 cases
```

All 515 pre-existing cases individually report `0 failed`
(17516 + 185 incidental = 17701).

## Warning repair (resume-only Task 2 section, warning amendment)

Applied at the PLAN_NOTES-2 checkpoint: removed `DEC_ROUND_POWERS`,
`dec_limb_digits`, `dec_coeff_exp_raw` from the Task 2 helpers (each a
unique quoted anchor, replacement empty) and replaced the parse allocation
with the guarded form:

```c
    if (ndig < 1 || ndig > DEC_MAX_COEFF_DIGITS) return DEC_ERR_RANGE;
    size_t digit_bytes = (size_t)ndig;
    char *dg = malloc(digit_bytes);
    if (!dg) return DEC_ERR_NOMEM;
```

Comparison against the amended Task 2 blocks: `dec_normalize`, the parse
block and the text block are byte-identical; the helpers region differs from
the fresh-execution block by one blank line (the deletion's empty
replacement leaves the single preceding blank where the fresh block retains
both blanks around the removed helper). Functions, bodies and order are
identical; no other difference.

Task 2 re-verify: build exit 0, **zero warnings**; focused run
**557 passed, 181 failed** (exit 181 — expected transitional RED).

## Tasks 3–7 — per-task green transcripts

Every task: `rtk proxy env SKIP_TESTS=1 ./build.sh` (exit 0, zero warnings —
verified by grep over each build log) then the focused run. All counts match
the plan's replay column:

| Task | Groups turned green | Result |
| --- | --- | --- |
| 3 — comparison/hash | 5–6 | `540 passed, 198 failed` |
| 4 — add/sub | 7 | `647 passed, 154 failed` |
| 5 — rounding/precision (introduces `DEC_ROUND_POWERS` at first use) | 8 | `754 passed, 92 failed` |
| 6 — average division | 9 | `815 passed, 73 failed` |
| 7 — packed serialization (introduces `dec_limb_digits`/`dec_coeff_exp_raw`, guarded unpack allocation) | 10 — all | `888 passed, 0 failed` (exit 0) |

## Post-review fix round (2026-10-23) — RAW-01 / RAW-02

The raw-diff review (`2026-10-23-phase2-raw-diff-review.md`, 2 P2 findings,
both plan-origin) was fixed via the plan's **Post-review fix round**
amendment (Revision 4 note; human approved amend+execute in one
instruction). Execution order followed the amendment: test regressions
first (RED against the unfixed module), then the two production fixes, then
full re-verification.

Regressions added (21 new assertions: 12 failure-contract, 9
neighbor/validation):

- RAW-01 (group 9): `131072` nines + `1001` fractional nines ÷ count 1
  must be `DEC_ERR_RANGE` with the canonical zero (sign/weight/limbs/cmp/
  hash), plus the neighboring valid boundary (`1000` fractional nines →
  `DEC_OK`, exactly `131072` integer digits).
- RAW-02 (group 7c): NULL text into a reused output → `DEC_ERR_INVALID`
  with canonical zero (sign/weight/limbs/dscale/cmp) and released storage
  (`digits == NULL`), seed `"42000.50"`; NULL output pointer stays
  rejected without dereference.

RED captures (both before any production fix; builds zero-warning):

1. Interim capture, seed `"42"`: `899 passed, 10 failed` — exposed that
   the seed's weight 0 / dscale 0 made the RAW-02 weight and scale
   assertions pass vacuously against the stale value.
2. Correction (documented in the amendment): seed changed to `"42000.50"`
   (weight 1, dscale 2). Final RED capture: **`897 passed, 12 failed`**
   (exit 12) — all six RAW-01 and all six RAW-02 failure-contract
   assertions fail; the 888 existing and 9 neighbor/validation assertions
   pass.

Production fixes (each a unique-anchor replacement, applied after RED):

- `dec_div_avg`: after `dec_round_scale` + `dec_normalize`, re-check
  `dec_int_digits(out) > DEC_MAX_INT_DIGITS` → `dec_free(out)` +
  `DEC_ERR_RANGE` (mirrors the existing add-overflow check).
- `dec_parse`: guard order — NULL output pointer rejected first, reused
  output released via `dec_free`, then NULL text rejected.

Re-verification against the fixed module (all ordinary builds, zero
warnings):

```
rtk proxy ./build/bin/shard-db-test run test-decnum-core
# test-decnum-core: 909 passed, 0 failed          (exit 0)
python3 decnum_oracle.py check cases.txt actual.txt
426 passed, 0 failed of 426                        (probe + check exit 0)
rtk proxy decnum_oom_probe
parse("1.25") ok=0 uses 2 allocations; all allocation-failure states
canonical; add overflow status=3 canonical         (exit 0)
rtk proxy ./build/bin/shard-db-test run-all
1..516
# total: 18425 passed, 0 failed across 516 cases   (exit 0)
```

No oracle corrections were needed (no generated case reaches the new
overflow); the probe's allocation count is unchanged.

## Post-review fix round 2 (2026-10-23) — public rounding boundary

The fresh blind review found one further P2 in the plan's public
`dec_round_scale` (RAW-01/RAW-02 confirmed correct, unchanged): (a) rounding
a maximum-sized value at scale 0 carries into a 131073rd integer digit with
success; (b) scale 16384 is accepted, outside the frozen 16383
display-scale domain — both pack and fail decode. Fixed via the plan's
**Post-review fix round 2** amendment (Revision 5 note; human approved
amend+execute).

Regression-first (20 new assertions; builds zero-warning). RED against the
unfixed module: **`922 passed, 7 failed`** (exit 7) — the four
round-overflow failure-contract assertions (`131072` nines + `.9` at scale
0 → `DEC_ERR_RANGE` + canonical zero), the two positive scale-domain
assertions (`16384` rejected), and `scale -16384 rejected`; the guard rails
pass on the unfixed module as designed (scale `±16383` accepted, constrained
overflow stays `DEC_ERR_PRECISION`).

Production edits (unique-anchor replacements; header contract updated to
document `|scale| <= DEC_MAX_DSCALE`, the post-carry integer-limit check,
and the canonical-zero-on-any-failure guarantee):

- `dec_round_scale` entry: domain check → `DEC_ERR_RANGE`, canonical zero.
- `dec_round_scale` tail: after `dec_normalize`, integer-limit re-check →
  `DEC_ERR_RANGE`, canonical zero.
- `dec_apply_precision`: a `DEC_ERR_RANGE` from rounding is translated to
  `DEC_ERR_PRECISION` (with `maxdigits <= 2000` for every declared (P,S)
  it is always a precision overflow), preserving every constrained-
  assignment status; negative-scale behavior unchanged.

Execution stumbles, both before any green claim and fully documented:

1. The amendment's first translation block referenced `maxdigits`, which is
   declared only at the later precision check — the first build failed
   (`'maxdigits' undeclared`, `decnum.c:650`). The plan text and code now
   compute the identical `precision - scale` inline.
2. Removing that broken block via empty replacement also consumed its
   leading `out->dscale = in->dscale;` line, leaving `dec_apply_precision`
   without the dscale assignment and the round call (one fresh build ran
   against that state: `910 passed, 19 failed`). The complete intended
   block was then restored in one anchored edit; the final region was
   diff-verified against the amended plan text.

Re-verification against the fixed module (fresh ordinary builds, zero
warnings):

```
rtk proxy ./build/bin/shard-db-test run test-decnum-core
# test-decnum-core: 929 passed, 0 failed          (exit 0)
python3 decnum_oracle.py check cases.txt actual.txt
426 passed, 0 failed of 426                        (probe + check exit 0)
rtk proxy decnum_oom_probe
all allocation-failure states canonical            (exit 0; 2 parse allocs)
rtk proxy ./build/bin/shard-db-test run-all
1..516
# total: 18445 passed, 0 failed across 516 cases   (exit 0)
```

## Task 8 — final gates

8a — full suite (fresh build, zero warnings):

```
rtk proxy ./build/bin/shard-db-test run test-decnum-core
# test-decnum-core: 888 passed, 0 failed          (exit 0)
rtk proxy ./build/bin/shard-db-test run-all
1..516
# total: 18404 passed, 0 failed across 516 cases  (exit 0)
```

8b — independent Python-decimal oracle (ordinary `cc -O1` probe; seed
20261022; 66 analytic + 360 seeded-random cases):

```
wrote 426 cases (seed 20261022)
426 passed, 0 failed of 426            (probe exit 0, check exit 0)
```

No oracle corrections were needed.

Deterministic allocation-failure probe (ordinary `cc -O1`, compiled without
a separate `src/db/decnum.c` argument — the probe includes the module):

```
parse("1.25") ok=0 uses 2 allocations          (ok=0 is st == DEC_OK)
parse fail: status=6 sign=0 nd=0 w=0 cmp0=0 hash_eq=1 ok
parse fail: status=6 sign=0 nd=0 w=0 cmp0=0 hash_eq=1 ok
unpack fail: status=6 sign=0 nd=0 w=0 cmp0=0 hash_eq=1 ok
unpack fail: status=6 sign=0 nd=0 w=0 cmp0=0 hash_eq=1 ok
add overflow: status=3 sign=0 nd=0 cmp0=0 ok
all allocation-failure states canonical        (exit 0)
```

Both allocating stages of parse and unpack leave the canonical zero on
NOMEM; the add range failure leaves the canonical zero.

## Task 8c — checksums and tree state

```
fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135  docs/plans/2026-10-19-durability-certification-rig.md
```

`git status --short`: modified `build.sh` only; untracked new files
`src/db/decnum.h`, `src/db/decnum.c`, `src/test/cases/test_decnum_core.c`,
this evidence document, plus the pre-existing planning/amendment/halt files.
`git diff --stat`: `build.sh | 8 +++++---  (5 insertions, 3 deletions)` —
the five plan build-list edits and the case registration, nothing else.

## Deviations and notes

1. Two halted checkpoints (build wiring; Task 2 warnings), each resolved by
   a human-approved amendment; records preserved unchanged.
2. One blank-line difference in the Task 2 helpers region vs the
   fresh-execution amended block (deletion-anchor semantics; content
   identical) — documented above, no functional effect.
3. Task 1e corruption-group failure text differs from the plan's
   illustrative example (actual: `expected 0 got 10` at the pack call);
   binding counts matched exactly.
4. Fix round: the RAW-02 regression seed was corrected during execution
   (`"42"` → `"42000.50"`) after the interim RED capture showed two
   vacuously-passing assertions; both captures and the amendment
   correction are recorded above. The amendment's first-draft post-fix
   expectations (908 focused / 18424 suite) miscounted the new assertion
   total by one; actuals (909 / 18425) govern.
5. No sanitizer builds, no benchmarks, no commits/stage/push/PR/merge at any
   point. No protected-plan content read (hash-only verification).
