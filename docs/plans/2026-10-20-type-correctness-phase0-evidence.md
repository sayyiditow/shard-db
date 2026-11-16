# Phase 0 evidence and decision ledger — type-correctness recovery

Status: **planning evidence only.** Nothing in this workstream has been
implemented; every "failure" below is the *current* behavior of an unfixed
binary, and the full suite that passed is the *old* suite. The next agent
must not read any green output here as evidence of the type-correctness
fixes. Companion: [Phase 1 child plan](2026-10-20-phase1-safe-scalar-conversions-plan.md)
(awaiting approval, not executed).

## Identity

- Source revision: `1dd1888fda49ce295717590d1f65415963445270` (branch `main`).
- Working tree: production sources clean; untracked files are the planning
  documents only (including the two pre-existing
  `2026-10-20-type-correctness-{executor-handoff.md,plan-review.mdf}` —
  preserved untouched).
- Protected plan `docs/plans/2026-10-19-durability-certification-rig.md`
  SHA-256 verified **at start and at finalization**:
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
- No production source, test registration, build list, or storage format
  was changed. Scratch sources and binaries live in
  `/tmp/shard-db-phase0-xA2RyY/` (disposable; key outputs are pasted here).

## 1. Baseline build and full suite (fresh, normal mode)

```
rtk proxy env SKIP_TESTS=1 ./build.sh     → rc=0
rtk proxy ./build/bin/shard-db-test run-all →
    1..511
    # total: 17254 passed, 0 failed across 511 cases
    suite rc=0
```

Baseline is fully green. No unexpected build or test failures to carry
forward. Full logs: `/tmp/shard-db-phase0-xA2RyY/build_normal.log`,
`suite_normal.log`.

The Phase 0 planning-time probes (baseline plan: numeric-repro, types-audit,
widen-repro, varchar-validator) were reused without rerun: the assignment's
revision-match condition holds (same commit `1dd1888`, no source diff), and
every claim they established was additionally re-proven below through
public routes.

## 2. Consumer inventory (Task 0.1, verified)

Discovery commands run (outputs archived in the scratch dir as
`inv_encoders.txt` (69 matches), `inv_numeric.txt` (172), `inv_conversions.txt`
(156), `inv_readers.txt` (100), `inv_capacity.txt` (209)). The parent plan's
anchor table was re-verified symbol-by-symbol; the Phase 1 caller
classification (checked vs shim vs deferred) is in the child plan's
inventory section. Notable confirmed facts:

- `encode_field_len` numeric path: `atof` → `(int64_t)(dv * mult + 0.5)`
  (config.c:2002-2010) — double conversion + unchecked cast.
- `parse_field_type` numeric branch (config.c:1516-1522): P is parsed and
  **dropped**; S via `atoi`; no range check; `10^scale` loop overflows
  silently for S > 18 (config.c:1577-1584).
- `validate_field_type_clean` (query_schema.c:1022): `numeric:` accepted
  with any suffix (prefix match only), no S validation.
- `typed_encode` has **no production callers** (tests only); production
  inserts go through `typed_encode_defaults`, which already returns
  `-2 + err_buf` and is handled as `{"error":"%s"}` in `cmd_insert_v2`
  (storage.c:1048-1063) — the exact propagation pattern Phase 1 reuses.
- `update_compose_new_from_old` call sites: storage.c:1921,
  query_bulk.c:3407, 4212, 4829.
- `encode_field_for_index` numeric encoding is exact (stored int64 + top
  -bit flip, config.c:2330-2338) — indexed/scan parity holds once writes
  are validated.
- Aggregate/float `%g` render sites (query_aggregate.c:1096/1101, 1432/1442)
  serve joins and group labels only (callers enumerated) — Phase 6 scope
  per the parent; Phase 1 covers the config.c decode sites.
- Bulk insert rejects overlength varchar per-row today (`errors++;`,
  query_bulk.c:2385-2392); JSON bulk-upd uses per-record `-1` skips.

## 3. Public-route failure evidence (isolated daemon/client fixtures)

Scratch probe (`route_probe.c`) linked the repo's own
`src/test/fixtures.c` + `test_client.c`, spawned an isolated daemon
(test-server binary, generated db.env, picked port), and spoke the JSON
wire. Full transcript: `/tmp/shard-db-phase0-xA2RyY/route_probe.out`.
Final line: **13 baseline bugs confirmed, 0 unexpected** (after fixing the
probe's own edit-field request shape; first run also captured below).

Confirmed on public routes (expected baseline failures — each was printed
verbatim in the transcript):

| Route probe | Observed response |
| --- | --- |
| insert `numeric:26,10` = `1234567890123456.1234567890` (indexed) | `{"status":"inserted"}` then get → `{"n":-922337203.6854775808,…}` |
| short `32768` | `"a":-32768` |
| int `2147483648` | `"b":-2147483648` |
| int `"123abc"` | `"b":123` |
| long `9223372036854775808` | `"c":9223372036854775807` |
| timestamp `9223372036854775808` | `"t":9223372036854775807` |
| byte `256` / `-1` | `"d":0` / `"d":255` |
| double `1.2345678901234567` | `"f":1.23457` |
| float `1.2345678` | `"f2":1.23457` |
| `edit-field … "fields":["x:double"]` after float 3.25 | rebuild ok, get → `{"x":5.34788e-318}` |
| create `numeric` (bare) | rejected with error (expected, correct) |
| create `varchar` (bare) | rejected with error (expected, correct) |
| over-length varchar update | rejected; old record intact (`"v":"abc","n":7`), count 1 — preservation already works |

Additional recorded behaviors (verbatim in transcript):

- Declared P is informational: `numeric:30,5` accepted, `1.5` → `"n":1.50000`.
- Oversized indexed varchar write (600 B into `varchar:1000` + index):
  insert **fails** with `{"error":"upsert failed"}` — no truncation, record
  not written.
- Oversized **unindexed** value inserts and reads fine; `add-index` over
  the oversized live value fails cleanly: `{"error":"index build failed
  for v: …; index metadata was not changed; one or more shards may already
  have been published"}` — old index metadata preserved, record intact.
  (The message's partial-publication wording deserves precision in the
  Phase 5 child plan.)

## 4. Task 0.3 layout/sizing spike

Standalone scratch program (`layout_spike.c`), no repo sources, 59 checks,
**0 failures** (`layout_spike.out`). Verified, among all checks:

- Field cost formula `8 + 16 + ceil(canonical_coefficient_digits/2)`:
  25-digit → **37**, 26-digit → **37**; the reported 17-digit
  numeric(26,10) value → 33. Varchar = `8 + content`; raw ceiling
  **65535** (not the legacy prefix-inclusive 65537).
- nullmap(256 fields) = 33. Maxima via checked arithmetic:
  all-bare-numeric value **18880545** B, all-bare-varchar **16779041** B —
  both equal to the handoff's numbers.
- Slots at max_key=1024 (+24 B record header, engine's align-8):
  **18881600** and **16780096** — both equal to the handoff's numbers.
  (An earlier review note claiming an align-8/align-32 discrepancy at
  16780072 was this reviewer's addition error; the spike disproved it.
  Both numbers are align-8 facts.)
- Both maxima < `SLOTCASK_SEG_MAX_BYTES` = 128 MiB (slotcask.h:43) and
  < 2^32 (uint32 offsets/vlen safe).
- Full-range bare decimal (131072 '9' + '.' + 16383 '1'): parses,
  canonicalizes to 147455 digits, payload exactly 73744 B, byte-exact
  round trip.
- Compact mixed record (numeric + two varchars + int, small values):
  55 bytes total; NULL vs empty-varchar vs numeric-zero produce three
  distinct, validated byte patterns.
- Descriptor validation rejects: out-of-record offsets, offsets into the
  directory, invalid nibbles, reserved≠0, sign>2, over-ceiling lengths.
- Exponent normalization `1.0e2` → renders `100` (display scale 0);
  `1.2300` keeps display scale 4 (`123` × 10⁻²).
- Free-pool buckets are coarse capacity ranges
  (`slotcask_bucket_for`, slotcask.c:3916-3923): an 80-byte record is
  bucket 0 regardless of a 18.8 MB schema slot_size; pops are
  fit-checked with leftover splitting — small records stay tight at the
  new ceilings (static evidence; the real pool is exercised by the suite).

Two spike defects found and fixed during the run (decimal digit-copy bug
including the '.' separator; a wrong corruption-probe byte index) — the
spike is evidence of the layout math, not production code.

Ceiling revision (2026-10-20, during human review of this ledger): the bare
varchar ceiling moved from 65,535 to **4 MiB (4,194,304 content bytes)** —
the packed layout's uint32 descriptor makes the ceiling enforcement policy
rather than format, so raising it again later would not be a format change.
The spike was re-run at the new ceiling: **59/59 checks pass**
(`layout_spike_4mib.out` in the scratch dir). Verified numbers: varchar
field max `8 + 4,194,304` bytes; all-bare-varchar maximum value
**1,073,743,905** B; slot at max_key=1024 **1,073,744,960** B — inside
int32/uint32, but exceeding the 128 MiB segment maximum as a *bound* only:
every actual record is individually seg-max-checked
(`append_reserve_single_varlen`), a single maxed-varchar record is
4,195,393 B and fits, and roughly 31 maxed varchar fields fit per record
before the existing segment-capacity write error applies. The numeric
maxima (18,880,545 / 18,881,600) are unchanged.

## 5. Contract freeze (Task 0.2) — additions pinned this run

- **Division/average result scale (PostgreSQL 18, pinned from source):**
  `REL_18_STABLE/src/backend/utils/adt/numeric.c:10173 select_div_scale` —
  base-10000 weights (`DEC_DIGITS=4`): `qweight = weight1 − weight2`,
  decremented when `firstdigit1 ≤ firstdigit2`;
  `rscale = clamp(16 − 4·qweight, max(dscale1, dscale2, 0), 1000)`
  (`NUMERIC_MIN_SIG_DIGITS=16`, `NUMERIC_MIN_DISPLAY_SCALE=0`,
  `NUMERIC_MAX_DISPLAY_SCALE=NUMERIC_MAX_PRECISION=1000`, numeric.h:43-53).
  `numeric_avg` = `numeric_div(sum, N)` (numeric.c:6285-6315), so averages
  use exactly this selection; `div_var` rounds the last digit
  half-away-from-zero on the magnitude. This **confirms the handoff's
  distinction**: stored fractional scale may reach 16383, while the
  calculation scale clamp is 0..1000.
- Validated with Python `decimal` under explicit context
  (`avg_scale_validate.py`, 22 checks, **ALL PASS**): 1/2 → 20 places;
  10/3 → 16; 5/5 → 20 (qweight-- rule); zero dividend → 20 places of
  zeros; dscale dominance; ±4000-exponent clamps; ties-away-from-zero at
  an exact 5; avg(1,2) = `1.5000000000000000`. Five initial check
  *expectations* were wrong (not the algorithm) and were corrected — the
  corrections are visible in the scratch script's history.
- No live PostgreSQL was used (per decision: source-derived pinning;
  live capture remains an optional evidence upgrade).
- Scalar grammar, finite limits, layout/version/downgrade and
  migration/replay/publication ordering: as frozen in the parent plan's
  Task 0.2/Phase 3/Phase 8 — no revisions required by this run; the
  parent's sizing numbers were corrected to the spike-verified values
  (see §6).

## 6. Corrections applied to the parent plan (2026-10-20)

In the parent's Task 0.3: the decimal field-cost now states the exact
formula with the 25/26-digit → **37** bound (33 remains the 17-digit
example), bare varchar's ceiling is **65535** (was mis-stated 65537), and
the spike-verified capacity/slot numbers are recorded. An index-feasibility
requirement for Phase 0/5 is now explicit (see §7).

## 7. Index feasibility (pre-freeze evidence)

- Budget: `BT_MAX_VAL_LEN = 544` = tier byte(s) + 512 (btree.h:24);
  BTRI keys are `[tier][payload]` with tier 0x00 value / 0x01 NULL
  (index.c:158-165).
- **Corrected (2026-10-20, reviewer finding): tier bytes are per
  COMPONENT, not per key.** `build_index_key_from_record_into`
  (index.c:1386-1444) emits `[tier][payload]` for every composite
  component (`out[cp] = is_null ? 0x01 : 0x00; cp += 1 + payload`),
  zero-pads NULL fixed-width components to the component width, and
  collapses NULL varchars to the tier byte alone (payload 0). The key
  budget is therefore `Σ(1 + payloadᵢ) ≤ 544`, not `1 + Σpayloadᵢ`; the
  first revision's "one tier byte per key" and its conclusion that
  "composites of numeric(1000)+anything exceed 544" were both wrong.
- Corrected feasibility examples for a candidate order-preserving
  decimal codec (`3 + ceil(digits/2)`): numeric(1000) single-field
  `1 + 503 = 504` ✓; numeric(1000)+numeric(26,10) = `504 + 17 = 521` ✓
  **fits** (previously reported as exceeding); numeric(1000)+
  numeric(1000) = 1008 ✗; numeric(1000)+varchar(40) = 545 ✗ by one byte.
  Per-value non-indexable thresholds are unchanged: bare numerics above
  1080 canonical digits (`1 + 3 + ceil(d/2) ≤ 544`) and varchar content
  above 543 bytes. Scans remain fully usable; columns are never rejected
  for their theoretical maximum. Phase 5 publishes the final codec's
  exact table.
- Current-engine behavior (captured, §3): oversized indexed writes fail
  loudly; index builds over oversized live values fail with metadata
  preserved; no truncation anywhere. Phase 1 changes none of this.

## 8. Decisions and open items

- Decisions taken (human-confirmed 2026-10-20): source-derived PG pinning
  (no live-PG gate); fixed-width descriptors accepted with pinned
  overhead; bare varchar ceiling raised to 4 MiB (4,194,304), replacing
  the 65,535 default, folded into the parent Task 0.2/0.3/Phase 3, the
  issue register (T18) and the acceptance fixture/matrix.
- Deferred (not decided here): shortest-round-trip float rendering as a
  possible later protocol revision (parent pinned fixed 9/17); per-field
  error messages in bulk per-row tallies (bulk contract keeps counts;
  messages are LOG-only) — consistent with the documented bulk semantics.
- Phase 1 child plan delivered with this ledger; it is **not approved and
  not executed**. Sanitizer gates remain post-fixes per AGENTS.md; the
  durability certification rig remains later work, untouched.
- Scratch dir `/tmp/shard-db-phase0-xA2RyY/` holds all raw transcripts
  cited above; it is disposable and everything load-bearing is pasted
  here.

## 9. Addendum (2026-10-20, integrated Phase 1 scratch verification)

The original scratch directory was removed by external /tmp cleanup; it
was recreated at the same path and now holds the Phase 1 fourth-revision
integration artifacts instead (`p1_red_run.log`, `p1_green_run2.log`,
`p1_green_suite2.log`, `p1_red_build.log`, `p1_green_build*.log`). All
previously cited outputs are pasted in the sections above, so nothing
evidentiary was lost. The integrated verification itself: a pristine
`git archive HEAD` copy at `/tmp/p1-int`, the regression case added
alone (RED: 52 failed / 62 passed against unchanged production code),
then the partial Phase 1 patch applied by
`/tmp/p1-patch.py` (every implemented anchor matched exactly once) and rebuilt.
This was initially reported as complete; R4 subsequently verified that
`query_aggregate.c` was omitted entirely. The results below therefore
validate that partial integration, not every planned hunk
(GREEN: 114/114 on the new case; full scratch suite 17,486 passed /
0 failed across 513 cases after the deliberate
`test_computed_update.c` §13 contract update described in the Phase 1
plan). This is scratch evidence only — no production tree was modified.


## 10. R4 aggregate closure (2026-10-22)

The fifth-revision plan and scratch patch now include every aggregate
hunk omitted from the fourth revision. The previous addendum's green
counts are historical evidence for a partial patch, not a complete one.
The reviewed production checkout still has no tracked source edits.

Actual outputs after staging the additional aggregate regressions first:

```text
public red: 125 passed, 6 failed
helpers red: 0 passed, 10 failed (two SIGFPE cases)
public green: 131 passed, 0 failed
helpers green: 10 passed, 0 failed
# total: 17520 passed, 0 failed across 513 cases
full-suite exit code: 0
Manifest verified: 14 expected code files, 8 aggregate hunks
```

The aggregate delta's red capture used the previously reviewed partial
Phase 1 scratch tree with unchanged baseline aggregate code. The earlier
whole-Phase-1 red-first capture remains separate evidence. Both layers
are explicitly distinguished in the [closure and handoff](2026-10-22-phase1-scratch/closure.md).
That folder preserves the targeted logs, full-suite summary/hash, complete
tested code diff and manifest independently of temporary-directory cleanup.
The full raw suite log is `/tmp/p1-r5-full-suite.log`; its SHA256 is
`500c38b2dde638badafb5d692bd196a56965ad205d6fbc66ec3f299264e48aee`. The code was built with
`rtk proxy env SKIP_TESTS=1 ./build.sh` (exit 0, no compiler warnings),
and tested with the prescribed targeted run and `run-all`, all in
`/tmp/p1-int`. No production build, benchmark, commit or certification
execution took place; sanitizer gates remain at the documented end of
the implementation branch series.
