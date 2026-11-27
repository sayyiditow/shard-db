# fix/audit-igb-bitmap-256-cap — AUD-QRY-5

Campaign: `docs/plans/2026-11-19-audit-campaign.md` Phase 4 (DL queue,
first of the post-triad branches). Finding: `AUD-QRY-5` in
`docs/plans/2026-11-19-audit-index.md` (DL-class: silent wrong groups in
the aggregate group_by mode). Branch name:
`fix/audit-igb-bitmap-256-cap`. Branch base: the campaign harness working
tree (`test/audit-oracle-harnesses`), stacked after the
`fix/audit-igb-bitmap-group-fallback` (AUD-QRY-1) and
`fix/audit-agg-subtree-cover-gate` (AUD-QRY-2) changes if they are still
uncommitted, or `main` if they have landed. Sequence: after the triad
(QRY-1 → QRY-2 → QRY-3).

## Brief (approval surface)

### Problem and root cause

`aggregate` with `group_by` over a bitmap-indexed field holding **more
than 256 distinct values** silently returns at most 256 groups — values
beyond the first 256 collected are dropped with no error, on every shape
of the IGB bitmap fast path — while `count`/`find` over the same object
are correct and a no-index twin agrees. Root cause (code-read verified,
2026-11-19): `bm_collect_uniq_cb` silently stops collecting new distinct
values at the context cap (`if (*c->n >= c->cap) return 0;`,
query_aggregate.c:4727), and **both** of its call sites pass cap 256 with
256-row stack arrays:

- the count-only bitmap branch: `uint8_t bm_vals[256][1024]` +
  `BmDictCollectCtx dc = { ..., .cap = 256 }` (query_aggregate.c:6034,
  6052-6053);
- the Phase 1b hbm branch: `uint8_t bm_vals_h[256][1024]` (query_aggregate.c:6210).

Nothing else bounds this usefully: `bitmap(N)` is legal at create-object
for N ∈ [2, 65535] (`BM_HARD_CEILING`, query_schema.c:1529-1532 +
bitmap.h:83; `parse_index_spec` itself only rejects N ≤ 0,
config.c:1230-1239), and the on-disk dict honors `max_values`
(bitmap.c:1146 refuses insert #max_values+1; bare `bitmap` defaults to
256 = `BM_DEFAULT_MAX_VALUES`, bitmap.h:82) — so an object with
`v:bitmap(300)` legitimately holds 300 distinct values in the bitmap
dict on disk; only the group_by collection truncates. The three served
shapes all lose groups 257..N:

1. no criteria → dict popcount per collected value (query_aggregate.c:6059-6084);
2. criteria all-bitmap-eq, `can_slot` → slot-level walk per collected
   value (6090-6180);
3. no criteria + hbm specs (sum/avg) → Phase 1b (6210+).

The slot-level walk's own `unsigned long counts[256]` (6118) is indexed
by collected `bm_n` and is NOT independently defective — once collection
is fixed, all shapes are.

Found by the AUD-QRY-1 round-3 blind review as a sibling defect in the
same bitmap IGB block (recorded in the QRY-1 plan's out-of-scope and in
the index).

### Logical flow

1. The collection callback gains an overflow signal: when a **new
   distinct** value (not dedup-equal to any stored one) arrives while
   `*n >= cap`, set `ctx->overflow = 1` instead of silently returning.
2. After the collection loop (single site — it runs once, before the
   `!tree` / `can_slot` split at query_aggregate.c:6056-6057), overflow
   routes the query out of the bitmap fast path (`goto igb_skip` with
   `igb_done` untouched) → the existing per-record scan executor produces
   exact groups. Same handling for the Phase 1b branch's collection.
3. ≤256-distinct-value objects never set the flag and keep the fast path
   unchanged.

### Decisions made

| decision | alternatives | why | tag |
|---|---|---|---|
| Overflow-detect at collection → abandon the bitmap fast path → per-record scan executor | (b) fail-closed `agg_error`; (c) grow the collection dynamically up to `max_values` | (a/b) (this option) mirrors the APPROVED QRY-1 pattern — the scan executor is the campaign's differential oracle and keeps a legitimate query answerable correctly (slower); (b) returns an error for a servable query — worse than a slower correct answer; (c) is the real perf fix but needs its own memory-budget and oracle work (up to `max_values` × 1024 B collection state) — recorded as a follow-up, not smuggled into a DL fix | DEFAULT |
| One overflow check after the shared collection (covers shapes 1+2) plus one after the Phase 1b collection | per-shape checks | the collection runs once at the top of the block, before the `!tree`/`can_slot` split; a single guard after it covers both shapes without touching the split logic | DEFAULT |
| Tests avoid values longer than 1024 bytes | include them | >1024 B values hit AUD-QRY-6's truncated-copy corruption (dedup against a truncated copy misfires, inflating distinct counts) — that defect is a separate branch; mixing them here would attribute its wrong answers to this fix | DEFAULT |

### Blast radius

- Consumers: `cmd_aggregate_do`'s bitmap IGB block only. Queries affected:
  group_by over a bitmap field with >256 distinct values — today silently
  wrong, after the fix correct (record-scan executor).
- **Perf caveat (stated plainly):** at large row counts the record scan is
  expensive — the count-only bitmap branch exists precisely to "avoid
  12.5M btree leaf entries at 25M" (its own comment), and a full record
  scan costs more than that walk. This branch chooses correct-but-slower
  over silently-wrong (DL wins over perf, per the campaign's priority
  order); the dynamic-growth follow-up restores the fast path for large
  objects. Flag the cliff in the PR body so operators with
  bitmap(N>256) group fields at scale re-bench after landing.
- `bench-queries` does not use bitmap(N>256) group fields (verify at
  execution; if one does, paste before/after numbers).
- No wire/protocol/on-disk change. No reader/writer paths outside the
  block are touched.

### Test strategy

- New product regression case `test-igb-bitmap-dict-cap` (Task 2):
  bitmap(300) object vs no-index oracle, 300 distinct values; asserts
  full label-set + per-label count equality on the no-criteria,
  hbm-spec, and can_slot shapes, plus a ≤256-distinct control that must
  stay on the fast path (correctness assertion — guards over-fixing).
- Campaign planner differential spot-check: the diff's pd_bitmap shape
  uses 5 categories (under cap) — no delta expected; record the DIVSEC
  line as the unchanged baseline (Task 5).
- Full suite `run-all --jobs 8` + sanitizer gate once each (executor
  routing change, no locks/lifetimes touched → single pass per AGENTS).

### Out of scope

- AUD-QRY-6 (the `min(vlen,1024)`-copy/full-`vlen`-record OOB read in the
  SAME callback): separate branch, planned after this one; its truncated
  copies make the dedup comparison unsound for >1024 B values, which is
  why this branch's tests avoid long values. If QRY-6 lands first, its
  fix must not regress this branch's overflow behavior (both touch
  `bm_collect_uniq_cb` — coordinate, one concern per branch).
- Dynamic-growth collection up to `max_values` (perf follow-up, own plan
  if wanted).
- AUD-QRY-1 / -2 / -3 (separate branches, ahead in the queue).

### Review log

- Round 1 (blind reviewer, 2026-11-20): **0 blockers, 0 majors**; 5
  minors, all folded in — `t:long` pinned in the fixture field list;
  the overflow latch clarified as IN-LOOP OR-accumulate (`dc` is
  declared inside the per-shard loop); "bitmap(N) accepts any N ≥ 1"
  corrected to the object-level legal range [2, 65535]
  (query_schema.c:1529-1532, `BM_HARD_CEILING`) with the bare-`bitmap`
  default (256, `BM_DEFAULT_MAX_VALUES`) noted; assertion 4 marked
  correctness-only (fast-path retention asserted by code-read, PR-body
  note); index row cross-reference updated to Task 0. Reviewer also
  verified independently: exactly two callback call sites; all three
  shapes truncate; `bm_iter_values` walks the full dict so the latch is
  accurate and cannot false-positive (dedup precedes the cap check);
  `igb_skip` fall-through reaches the exact scan executor for both
  criteria and no-criteria shapes with no double accounting; can_slot
  eligibility of assertion 3; Phase 1b routing of assertion 2; gate
  compliance (routing-only diff → single sanitizer pass).
- Round 2 (blind re-review, 2026-11-20): **0 blockers, 0 majors, 4
  minors, all folded in** — index-row range text tasked (Task 4.7);
  `int bm_over = 0;` / `bm_over_h` declarations named; Task 0's
  aggregates JSON pinned; stash-stacking note added to Task 4.1 (the
  reds are unaffected by lifted QRY-1/2 hunks — the assertion shapes
  never reach QRY-1's guard). Reviewer re-verified every anchor fresh
  (cap 4727; call sites 6034/6052-6054 and 6210/6226-6228; igb_skip
  7006 → agg_run_plan 7022; range [2,65535] at query_schema.c:1529-1533,
  BM_HARD_CEILING 65535 / default 256 bitmap.h:82-83; Phase 1b gate
  6208 verbatim; RED math for all three shapes). **CONVERGED — ready
  for approval.**

---

## Plan (executor-facing)

### Ground rules

Branch base per the header (harness working tree stacked after the
committed-state caveat; the base MUST contain `src/test/audit/` for the
Task 5 differential spot-check, else halt per PLAN_NOTES). Work
uncommitted (repo standing exception) until the human reviews the raw
diff. Revert-prove steps use path-limited stashes only
(`git stash push -- <fix files>`), never a bare `git stash`. Build
`SKIP_TESTS=1 ./build.sh`; suite `./build/bin/shard-db-test run-all
--jobs 8`; single case `./build/bin/shard-db-test run <name>`.
`PLAN_NOTES.md` + halt on any mismatch between this plan and the code
(symbol moved ≠ halt; record and continue only when the intended site is
unambiguous). One heavy job at a time; `/tmp/shard-db-*` removed after
every run.

### Task 0 — capture the red evidence artifact (base build)

Before writing the test, capture the live wire repro on the base build:
`docs/plans/audit-artifacts/AUD-QRY-5-repro.sh` (start one daemon via
`./build/bin/shard-db` + `db.env` on a throwaway root; create
`cap_demo` with fields `v:varchar:16`,`u:int` and indexes
`["v:bitmap(300)","u"]`; bulk-insert 900 rows `k%04d` with
`v="v%03d" (i%300)`, `u=i%13`; then `aggregate group_by=v` with
`"aggregates":[{"fn":"count","alias":"n"}]` and NO criteria, plus a
bare `count` on the same object) → `.out` shows the group list capped at
256 labels while count reports 900. Paste both into
`docs/plans/audit-artifacts/`. This is the finding's repro (the index
row cites it).

### Task 1 — identify the branch each test shape exercises (code-read, cheap)

Record in the branch notes which IGB branch serves each of the three
test shapes on the fixture object (expected: shape 1 → count-only
popcount branch; shape 2 → Phase 1b hbm branch — `igb_needs_hbm` is set
at query_aggregate.c:5982 for a non-count numeric spec on a btree-indexed
field, and the Phase 1b gate is `igb_group_uses_bitmap && igb_needs_hbm &&
!tree` at 6208; shape 3 → `can_slot` slot-level branch). Halt per
PLAN_NOTES if a shape turns out unreachable on the fixture.

### Task 2 — regression test (test-first; expect RED on base)

New file `src/test/cases/test_igb_bitmap_dict_cap.c`, registered
`TEST_REGISTER("test-igb-bitmap-dict-cap", ...)`, wired into `build.sh`
next to the aggregate cases (anchor: the
`src/test/cases/test_aggregate_type_matrix.c \` line). Fixture/daemon
helpers copied from `test_aggregate_type_matrix.c`
(`test_env_start(&env)` / `test_env_stop`); per-label count/sum
extraction helpers likewise. Objects, identical rows (`i` in
`[0,900)`, `k%04d`, `v = "v%03d" % (i%300)`, `u = i%13`, `t = i`):

- `cap_plain` — no indexes (oracle);
- `cap_bm` — `indexes: ["v:bitmap(300)","w:bitmap(4)","t","u"]`, fields
  `v:varchar:16`, `w:varchar:8` (`"w_0"`/`"w_1"`, `i%2`), `t:long`
  (indexed but never queried), `u:int`;
- `cap_small` — `indexes: ["v:bitmap(300)"]` but only 100 distinct `v`
  values in its rows (over-fixing control).

Assertions (1-4 extract the per-label count vector and compare against
`cap_plain`'s — never raw-string compare; aggregate responses without
`order_by` emit in hash-collection order):

1. No-criteria `group_by=v`, count-only spec → full 300-label set
   equality + per-label counts. RED today: `cap_bm` returns ≤256 labels.
2. No-criteria `group_by=v`, specs `[{count},{sum u}]` (Phase 1b shape)
   → label set + per-label count AND sum equality. RED today.
3. `criteria: [{"field":"w","op":"eq","value":"w_1"}]`, `group_by=v`
   (can_slot shape — all criteria sources bitmap eq, no postfilter) →
   label set + counts equality. RED today (only the first 256 labels
   collected).
4. `cap_small` no-criteria group_by → correct today AND post-fix.
   NOTE: this assertion is correctness-only — it cannot distinguish
   "fast path retained" from "unconditionally routed to scan" (a
   hypothetical over-fix passes it too). State in the PR body that
   fast-path retention for ≤256-distinct objects is asserted by
   code-read (the latch fires only on genuine overflow), not by this
   test.
5. `count` (no criteria) on `cap_bm` == 900 — control proving the
   defect is group_by-specific.

Test-first proof: build on the branch base, run the case — assertions
1-3 RED (missing labels), 4-5 GREEN. Paste.

### Task 3 — the fix

In `src/db/query_aggregate.c`:

1. `BmDictCollectCtx` (query_aggregate.c:4714-4719) gains
   `int overflow;`. In `bm_collect_uniq_cb`, replace the silent
   `if (*c->n >= c->cap) return 0;` with:
   ```c
   if (*c->n >= c->cap) {
       /* AUD-QRY-5: a new distinct value beyond the collection cap —
          the bitmap group_by fast path cannot serve this object's
          group set. Callers route to the record-scan executor. */
       c->overflow = 1;
       return 0;
   }
   ```
2. Count-only/`can_slot` shared block: declare `int bm_over = 0;`
   before the collection loop (6038); latch IN-LOOP — `BmDictCollectCtx
   dc` is declared inside the per-shard loop (6052-6053), so immediately
   after each `bm_iter_values(bms, bm_collect_uniq_cb, &dc)` call
   (6054) OR-accumulate a block-local flag: `bm_over |= dc.overflow;`.
   Then after the loop, next to the existing
   `if (bm_n == 0) { igb_done = 1; goto igb_skip; }` (6057), insert:
   ```c
   if (bm_over) {
       /* AUD-QRY-5: >256 distinct group values — the popcount and
          slot-level shapes below would silently drop the rest (the
          collection caps at bm_vals' 256 rows). The record-scan
          executor is exact for every shape. */
       goto igb_skip;
   }
   ```
3. Phase 1b hbm branch: same shape — declare `int bm_over_h = 0;`
   before its collection loop (~6214), OR-accumulate right after its
   `bm_iter_values` call (query_aggregate.c:~6228, ctx declared in-loop
   at 6226-6227), same `goto igb_skip` before any bucket is emitted.
4. `igb_skip` reachability: verify (code-read, Task 1 record) that
   reaching `igb_skip` with `igb_done == 0` and no criteria falls through
   to the record-scan executor and produces exact groups — this is the
   same contract the APPROVED QRY-1 fix relies on. If the no-criteria
   path instead needs `igb_done` left at 1 to avoid double accounting,
   halt per PLAN_NOTES with the evidence.
5. Update the stale comments: the "Avoids 12.5M btree leaf entries at
   25M" block comment (query_aggregate.c:6026-6032) and
   `bm_collect_uniq_cb`'s header comment must state the cap behavior and
   the scan fallback.

Do NOT touch `bm_popcount_one_value`, the `counts[256]` slot arrays, or
the `can_slot` eligibility computation.

### Task 4 — prove and gate

1. Revert-prove-paste: `git stash push -- src/db/query_aggregate.c`
   (never a bare stash; NOTE in the prove output if this lifted
   still-uncommitted QRY-1/QRY-2 hunks in the same file — the reds are
   unaffected: this plan's assertion shapes never reach QRY-1's guard,
   verified by the round-2 review) → rebuild → Task 2 reds →
   `git stash pop` → rebuild → greens. Paste both outputs.
2. Targeted: `./build/bin/shard-db-test run test-igb-bitmap-dict-cap`.
3. Full suite: `run-all --jobs 8`, fresh, all green (AUD-CI-1 load
   flakes aside — re-run a fired case solo per the index note).
4. Differential spot-check: `SHARD_AUDIT_DIFF_ROWS=5000
   SHARD_AUDIT_DIFF_CRITERIA=200 ./build/bin/shard-db-test run-all
   --include-audit --filter audit-planner-diff` — no new divergences vs
   the QRY-2-branch baseline (the diff's bitmap twin is under cap);
   record the DIVSEC line.
5. Sanitizer gate once each: `BUILD_MODE=asan` → `run-all --jobs 6`;
   `BUILD_MODE=tsan` → `run-all --jobs 3`. Rebuild normal afterwards.
6. Check whether `docs/reference/changelog.md` expects a per-fix entry;
   add one if the repo's convention is per-fix.
7. Index: AUD-QRY-5 row → `fix-branch` status with this branch name;
   paste the Task 0 artifact reference into the row AND correct the
   row's bitmap(N) range parenthetical to the legal [2, 65535] object
   range (round-2 minor — the row still carries the parse-level
   "any N ≥ 1" phrasing).

### Embedded execution rules

Tasks in order; both revert outputs pasted; no unrelated changes;
`PLAN_NOTES.md` + halt on mismatch (including the Task 1 routing
verification and the Task 3.4 `igb_skip` contract check).
