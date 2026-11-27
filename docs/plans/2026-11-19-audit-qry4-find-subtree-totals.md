# fix/audit-find-subtree-totals — AUD-QRY-4

Campaign: `docs/plans/2026-11-19-audit-campaign.md` Phase 4 (post-triad
queue). Finding: `AUD-QRY-4` (widened) in
`docs/plans/2026-11-19-audit-index.md` (major: find drops OR sub-trees
from rows and/or `total` on routes that trust the plan's candidate set
without a full-tree re-match — and several total sites emit wrong
numbers where they should decline). Branch name:
`fix/audit-find-subtree-totals`. Branch base: AFTER
`fix/audit-agg-subtree-cover-gate` (AUD-QRY-2) has landed — the fix
rides that branch's `FilterPlan.has_subtree` field (query_internal.h);
base = harness working tree stacked after QRY-2, or `main` if landed.
Sequence: last of the current plan queue (after QRY-5/-6/-7; QRY-2 must
be in the base).

## Brief (approval surface)

### Problem and root cause

Find has sub-tree blindness AUD-QRY-2 fixed on the aggregate side,
spread over several gates; two of them also emit WRONG totals where the
code elsewhere commits to exact-or-null. All anchors re-verified in the
round-1 review (2026-11-20):

1. **Intersect rows + total (large primaries)** —
   `keyset_find_from_intersect` (query.c:6434-6470):
   `need_rematch = small_primary || (fp->n_postfilter > 0)` (6452) and
   `if (out_total && !small_primary && fp->n_postfilter == 0)
   *out_total = keyset_size(ks)` (6460-6461). A
   `[indexed-leaf, indexed-leaf, OR…]` tree yields FP_INTERSECT with
   `n_postfilter == 0`; when the SMALLEST leaf's candidate count ≥
   INTERSECT_MIN_PRIMARY = 10000 (query.c:5319; `small_primary` is set
   per-smallest-leaf, 5557/5595) the OR drops from rows AND total.
2. **The null contract is defeated by the caller** — after
   `keyset_find_from_intersect` returns, cmd_find forces
   `find_total_null = 0` UNCONDITIONALLY (query.c:11113); same after
   the union route (11220). The executor's "leave out_total untouched
   → null" convention (6454-6459) never survives the call: a declined
   total keeps `find_total`'s 0 init (10361) and the response emits a
   WRONG `"total":0` — today, for every small-primary/postfilter
   intersect find with `total:true` (a pre-existing instance of this
   audit's class), and it would still be wrong after a site-1-only fix.
3. **Union total on the btree route** — `keyset_find_from_or`
   unconditionally reports `*out_total = keyset_size(ks)` (query.c:6419,
   reached at 11212 whenever `find_plan_reads_bitmap` is false — i.e.
   the btree FP_UNION route). For a pure OR root the union is exact;
   for the planner's nL==0 AND-of-ORs shape the union covers only the
   FIRST fully-indexed OR conjunct (AUD-QRY-2's Task 2 sets
   `has_subtree = 1` on that plan) — and that partial union is a
   SUPERSET of the true matches (every true match satisfies the first
   conjunct; query_plan.c:2547-2561, 3602-3623), so the reported total
   OVER-counts (round-2 verified: reports 160 where the true count is
   106; the audit index records the same direction). Rows are
   exact there — the prepared-union route at 9455-9480 rematch-guards
   and validates its totals, and QRY-2's flag makes
   `rematch_unnecessary` false on 9455-9459, so ROWS re-match; only
   this 6419 total is blind.
4. **Streaming-leaf total gate** — query.c:11100-11103
   (`want_total && fp.kind != FP_INTERSECT && fp.n_postfilter == 0`):
   sub-tree-blind for FP_PRIMARY_LEAF / FP_BITMAP_SMALLER shapes
   (FP_UNION fails the 11058-11061 eligibility and never reaches it).
   When it declines, the 11284-11293 `fp_compute_total` fallback
   produces an exact validated count (9162-9232) — the fix falls into
   an existing exact path.
5. **Non-streaming leaf total** — query.c:11189-11196: total =
   `idx_count_for_leaf(seed)` with NO `n_postfilter`/`has_subtree`
   guard (found by the round-1 review; `cmd_count`'s use of the same
   helper IS shape-guarded at 7129-7133, find's is not) — blind for
   no-limit `[leaf, OR]` finds and even postfilter-bearing leaf finds.
6. **Exact today, verified NA**: the leaf rematch gate
   (`rematch_unnecessary`, 6404-6408) fires only on pure-OR /
   single-child-AND shapes — exactly the shapes where `has_subtree ==
   0` (QRY-2's Task 2 semantics); the prepared-union route (9455-9480);
   cursor/ordered/bitmap routes (cursor rows full-tree matched,
   query_find.c:178; cursor totals via `fp_compute_total` 9861-9862);
   `total_cheap` (set query_plan.c:3877-3878, consumed only by explain
   display, query.c:6655-6656).

Found by the AUD-QRY-2 plan's blind review (round 1, M2) + the QRY-4
round-1 review (sites 3/5 corrected, site 6 verified NA, the 11113/11220
defeat).

### Logical flow

1. **Total-decline contract** (REWORKED round 2 — the original
   SIZE_MAX-slot sentinel collided with the pipeline's EXISTING
   SIZE_MAX hard-error convention; see the review log): decline is
   signaled via `find_total_null` ALONE, never through the slot
   value. `find_total` keeps its 0 init (10361 unchanged) and the
   hard-error check at 11272 (unconditional
   `if (find_total == SIZE_MAX) … return -1`, sitting OUTSIDE the
   want_total guard at 11275) keeps its meaning — but its input is
   closed at the source: the three `idx_count_for_leaf` sites that
   feed `find_total` (11101/11189/11194) handle the SIZE_MAX hard
   error explicitly (error flag, not a sentinel value), so a hard
   error can no longer leak into the slot and 11272 becomes
   unreachable-but-retained defense. The callers at 11113 / 11220
   replace the unconditional `find_total_null = 0` with the decline
   decision (caller-visible via `fp.has_subtree` / `fp.n_postfilter`,
   the same terms the fix adds below): declined → the tail upgrades
   the decline to a validated EXACT total via the 11284-11293
   `fp_compute_total` fallback wherever the plan kind is computable
   (null only remains for FULL_SCAN/refusal/timeout shapes — decline
   is intermediate, never a wrong number). The intersect executor
   exposes its third decline condition to the caller: `small_primary`
   is a LOCAL inside `keyset_find_from_intersect` (query.c:6442, set
   at 5557/5595), not a FilterPlan field — the fix plumbs a one-int
   decline out-channel (`int *out_declined`) through the single 11107
   call site (or carries `small_primary` on FilterPlan alongside
   QRY-2's `has_subtree` — Task 0 picks the smaller diff; the
   out-channel is the DEFAULT, it does not touch the shared struct).
   The caller's decision then covers all three conditions:
   `find_total_null = (small_primary || fp.n_postfilter > 0 ||
   fp.has_subtree)`, inside the existing `want_total && !dl.timed_out`
   guard. The exact-write sites clear the null flag as they do today.
   This also FIXES the pre-existing small-primary/postfilter
   `"total":0` emissions (same class, same branch — one concern:
   find totals that trust the plan). Task 0 verifies the exact
   flag/plumbing shape at execution.
2. Site 1: `need_rematch` gains `|| fp->has_subtree` (6452); the total
   assignment gains `&& !fp->has_subtree` (6460).
3. Site 3: the union total at 6419 declines when `fp->has_subtree`
   (partial union) — leave untouched otherwise (pure OR stays exact);
   the caller-side sentinel handles the null signaling.
4. Site 4: the 11100 gate gains `&& !fp.has_subtree` → falls to the
   existing exact `fp_compute_total` fallback (11284-11293).
5. Site 5: the 11189-11196 assignment declines when
   `fp.has_subtree || fp.n_postfilter > 0` → same exact fallback.
6. Sub-tree-free plans keep today's fast routes untouched.

### Decisions made

| decision | alternatives | why | tag |
|---|---|---|---|
| Decline-via-null-flag contract (find_total_null alone; find_total keeps its 0 init; the 11272 SIZE_MAX hard-error check keeps its meaning with its input closed at the source sites), fixing the 11113/11220 defeats AND the pre-existing wrong `"total":0` | (a) SIZE_MAX-slot sentinel — REJECTED round 2: collides with the pipeline's existing SIZE_MAX hard-error convention (fp_compute_total 9173/9214/9221/9223 → checked 11288; idx_count_for_leaf 7356/7396/7409/7429 → checked by the UNCONDITIONAL 11272 outside the want_total guard 11275): with a SIZE_MAX init every tail-reaching find with an untouched slot (FULL_SCAN finds, want_total-gated streaming/leafish finds, timed-out routes, the trigram path that skips the total block via 11168) aborts with an unterminated envelope, and the 11284-11293 exact fallback becomes unreachable for exactly the targeted routes; (b) thread a separate out_null flag through every executor; (c) recompute totals by full scan always | (this option) preserves the hard-error pipeline untouched, matches the existing exact-or-null convention (6454-6459, `prepared_find_recover`), repairs the already-wrong total:0 emissions in the same concern; (b) duplicates what the null flag expresses; (c) regresses every large find | DEFAULT |
| Site 5's guard includes `n_postfilter > 0` (not just has_subtree) | has_subtree only | the 11189 assignment is n_postfilter-blind too (round-1 finding; cmd_count guards, find does not) — same wrong-total class, same one-line fix | DEFAULT |
| Scope = the six sites in Task 0's table; `rematch_unnecessary` (6404-6408) classified NA (provably exact; has_subtree is 0 on every shape it fires on) | re-verify at execution | the round-1 review proved NA; Task 0 re-states it so the executor does not add a pointless term | DEFAULT |
| Task 0 enumerates ALL find-route total/decide sites with the WIDENED grep (`need_rematch\|rematch_unnecessary\|out_total\|total_cheap\|find_total\|idx_count_for_leaf\|total_null`) and classifies each before any edit | fix only the tabled sites | the differential never generates two-leaves+OR, so the suite may not catch a missed site; the widened grep is what found site 5 | DEFAULT |

### Blast radius

- Consumers: `cmd_find`'s intersect/union/leaf total routes + the two
  rematch gates (all in query.c).
- Rows: sites 1 and 9439 change rows (large intersect + OR — today
  silently wrong, after the fix exact via rematch; 9439 is the
  PREPARED-intersect twin: OR dropped from rows while totals stay
  exact — an inconsistent rows-wrong/total-right pairing).
- Totals: several shapes that emitted WRONG `"total":0` (declined
  intersect finds today; the union partial class; sub-tree leaf finds)
  now emit `"total":null` or an exact number — never a wrong number
  (flag in PR body: `total:null` may appear where a wrong 0 appeared).
- Perf honesty: site 4's fix routes C1-shape totals through
  `fp_compute_total`, which materializes the candidate KeySet and
  fetch-validates every candidate (9191-9199, 9226) — materially
  heavier than the leaf count walk, but the same cost class as today's
  small-primary intersect fallback; conventionally acceptable.
- No wire/protocol/on-disk change. Aggregate/count untouched (QRY-2).

### Test strategy

- New product regression case `test-find-subtree-totals` (Task 2):
  fixture-scale reds for sites 3/4 (measured shapes), a ≥24k-row
  large-intersect case for site 1 (rows AND total vs oracle; the
  smallest leaf must reach ≥ 10000 — the threshold is PER-SMALLEST-LEAF,
  not combined), the pre-existing wrong-total:0 pin for declined
  intersect finds, controls for the fast routes.
- Oracle totals: read from the oracle twin's COUNT-mode responses
  (its find totals come back `total:null` on the FULL_SCAN branch —
  never compare against those).
- Differential spot-check: the find section must stay 0-divergence on
  the generated shapes (the two-leaves+OR generator gap is a recorded
  harness gap, out of scope).
- Full suite + sanitizer gate once each (predicate terms + sentinel
  plumbing; no locks/lifetimes).

### Out of scope

- Native OR sub-tree planning in find executors (perf follow-up).
- The planner's nL==0 first-conjunct-only union itself (AUD-QRY-2's
  flag + this branch's total decline handle the correctness side;
  actually planning it = follow-up).
- Adding the two-leaves+OR shape to the planner-differential generator
  (audit-harness branch).
- Aggregate/count totals (QRY-2's scope).

### Review log

- Round 1 (blind reviewer, 2026-11-20): **2 blockers, 2 majors, 6
  minors, all folded in — the plan was rewritten.** B1: site 3's
  mechanism and fix target were wrong (the 9455-9480 prepared-union
  route is ALREADY exact for has_subtree plans — QRY-2's flag makes
  its rematch gate fire and its totals validate; the real blind total
  is `keyset_find_from_or`'s unconditional `*out_total = keyset_size`
  at query.c:6419 on the btree FP_UNION route, reached at 11212 when
  `find_plan_reads_bitmap` is false); the tabled 9455 refusal was a
  no-op that regressed an exact fast route. B2: "total stays null"
  was unimplementable as tabled — cmd_find forces
  `find_total_null = 0` at 11113/11220, so declined totals emitted a
  WRONG `"total":0` (a pre-existing wrong-total class the review
  surfaced: every small-primary/postfilter intersect find with
  total:true today) — fixed: the sentinel contract in Logical flow 1.
  M1: Task 0's grep missed the blind sites (11189-11196
  `idx_count_for_leaf` with no guard — new site 5; 11113/11220) —
  widened grep terms. M2: assertion-3 fixture algebra wrong (threshold
  is per-smallest-leaf at 5557/5595, not combined; 12000 rows keeps
  `small_primary=1`; `explain` cannot detect small_primary —
  control dropped) — ≥24k rows specified. Minors: small-intersect
  control scoped to rows (its totals are the pre-existing total:0
  class — now pinned by the new contract case); oracle totals sourced
  from count-mode (FULL_SCAN find totals are null); the "160 vs 106"
  measurement re-attributed to site 3 (6419), site 4 scoped to
  leaf shapes; 6404-6408 pre-classified NA; site-4 perf honesty;
  ft_big bulk-insert chunking note. Reviewer independently verified:
  INTERSECT_MIN_PRIMARY=10000 (5319), small_primary per-smallest-leaf
  (5557/5595), the exact fp_compute_total fallback (9162-9232,
  11284-11293), `fp` reachable at the prepared sites (9403-9410),
  total_cheap display-only (6655-6656), cursor/bitmap routes exact.
- Round 2 (blind reviewer, 2026-11-20): **1 blocker, 1 major, 5
  minors — the sentinel contract was reworked; round-3 confirmation
  of the rework scheduled.** B1: the SIZE_MAX-slot sentinel collided
  with the pipeline's EXISTING SIZE_MAX hard-error convention —
  query.c:11272 is an UNCONDITIONAL
  `if (find_total == SIZE_MAX) … return -1` outside the want_total
  guard (11275), and SIZE_MAX is the error return of fp_compute_total
  (9173/9214/9221/9223) and idx_count_for_leaf (7356/7396/7409/7429);
  with a sentinel init EVERY tail-reaching find with an untouched
  slot (FULL_SCAN finds, want_total-gated streaming/leafish finds,
  timed-out intersect/union routes, the trigram path skipping the
  total block at 11168) would abort with an unterminated envelope,
  and the 11284-11293 exact fallback would be unreachable for exactly
  the targeted routes. Reworked: decline rides `find_total_null`
  ALONE; find_total keeps its 0 init; the hard-error case 11272
  catches today is handled at its source (explicit error flag at
  11101/11189/11194); 11272 retained as unreachable defense; Task 0's
  table now lists 11272, the tail consumers (11281-11293) and the
  prepared writes (11011/11054). M1: the table missed the bitmap twin
  of site 1 — query.c:9439 (`prepared_find_prepare`'s intersect
  need_rematch has no has_subtree term): a `[bitmap-leaf, btree-leaf,
  OR…]` tree with both leaves ≥10000 and no limit window drops the OR
  from ROWS on PREP_INTERSECT while totals stay exact (9475-9489) —
  an inconsistent rows-wrong/total-right pairing; added to the table
  with the same `|| fp->has_subtree` term (fp IS a parameter there,
  9403-9410). Minors: assertion 1's red re-attributed to site 4 (the
  streaming gate — limit:1 routes C1 through 11058-11061 → 11087;
  site 5 fires only for limit-0 / >1000-row leaf finds); site 3's
  direction corrected (the partial union OVER-counts: superset 160
  reported vs 106 true, matching the audit index); assertion 3 now
  pins no-limit explicitly and pins `u = key index` so the red's
  algebra (intersection rows outside the OR band) is checkable up
  front; fixture section states ft_plain loads identical datasets and
  that every OR child must be an indexed field (u would yield
  FP_FULL_SCAN). Reviewer re-verified fresh: the whole site table,
  planner mechanics (n_postfilter from collect_and_leaves only;
  selectivity_budget saturation route for assertion 3), the 11272
  collision mechanics, the 9439 route, and the B1 remedy's
  compatibility with fp_compute_total's out_null contract
  (9167/9205/9399).
- Round 3 (same reviewer, focused re-read of the rework, 2026-11-20):
  **REVISE — 1 blocker + 1 major + 2 minors, all folded.** B1r3: the
  reworked caller-side decline decision ("the caller-visible terms
  fp.has_subtree / fp.n_postfilter") cannot observe the intersect
  executor's THIRD decline condition — `small_primary` is a function
  local (query.c:6442, set at 5557/5595), not a FilterPlan member
  (query_internal.h:171-192 verified) — so assertion 4's exact shape
  (`[v eq cat_1, w eq w_1]` on 400 rows: both leaves <10000 →
  small_primary=1; both non-selective vs budget N/8=50 →
  FP_INTERSECT all-in-source, n_postfilter=0) evaluates FALSE →
  `find_total_null=0` → emits the WRONG `"total":0`, re-creating the
  pre-existing class; fixed: one-int decline out-channel
  (`int *out_declined`) through the single 11107 call site (DEFAULT;
  the reviewer explicitly distinguished this from the rejected
  blanket out_null-flag alternative) or `small_primary` on
  FilterPlan — Task 0 picks; the 11113 row now tables all three
  conditions inside the surviving `want_total && !dl.timed_out`
  guard. M1r3: the 9439 fix shipped with ZERO test coverage (all
  fixtures were all-btree, so find_plan_reads_bitmap never fired and
  the prepared route was never reached) — fixed: `b:varchar:16` +
  `b:bitmap` on ft_big (explicitly NOT w-as-bitmap, which would flip
  assertion 3's shape to the prepared route and destroy its red) +
  assertion 3b (no-limit rows-vs-oracle on the
  `[bitmap-leaf, btree-leaf, OR…]` shape); the stale blast-radius
  sentence ("only site 1 changes rows") corrected. Minors: Logical
  flow 1 now states decline is INTERMEDIATE (the 11284-11293 tail
  normally upgrades it to a validated exact total; null remains only
  for FULL_SCAN/refusal/timeout); the 11113/11220 rows pin the
  surviving `want_total && !dl.timed_out` guard wrapper (mid-timeout
  must keep emitting null). Reviewer confirmed the B1 abort mechanics
  CLOSED (0 init + null-alone + source-side hard-error handling +
  11272 unreachable-retained + the table mechanically complete + all
  five round-2 minors verifiably folded).
- Round 4 (same reviewer, final fold check, 2026-11-20): **CONVERGED —
  0 blockers, 0 majors, 1 advisory minor, folded.** The advisory: the
  one-int `out_declined` out-channel must be caller-initialized to 0
  at the call site (`int declined = 0;` passed as `&declined`) so the
  executor's early-return paths (6447) can never feed the decision an
  uninitialized read — added to the 11113 row's plumbing text.
  Reviewer verified all round-3 folds closed (B1r3 decision formula +
  out-channel + guard wrapper; M1r3 fixture code-validity — bare
  `b:bitmap` accepted by parse_index_spec, config.c ~1227 — and
  assertion 3b's route re-derived end-to-end through the code:
  both leaves 12000 > budget 3000 → FP_INTERSECT all-in-source at
  query_plan.c:3813-3822 → find_plan_reads_bitmap true → PREP_INTERSECT,
  stream=0 → base need_rematch=0 drops the OR from rows at 11001-11002
  → RED; post-fix rematch → GREEN), confirmed `keyset_find_from_intersect`
  is single-caller (11107) so the out-channel is exactly the 2-line
  plumbing claimed, and marked the plan ready for execution once its
  base (AUD-QRY-2's `FilterPlan.has_subtree`) lands.

---

## Plan (executor-facing)

### Ground rules

Branch base per the header — it MUST contain `FilterPlan.has_subtree`
(AUD-QRY-2's field); if absent, halt per PLAN_NOTES. Base must contain
`src/test/audit/` for the Task 4 differential spot-check. Work
uncommitted; path-limited stashes only; build/test per AGENTS;
`PLAN_NOTES.md` + halt on mismatch; one heavy job at a time;
`/tmp/shard-db-*` removed after every run.

### Task 0 — enumerate and classify the gates (code-read, cheap; halt-on-mismatch)

`grep -n "need_rematch\|rematch_unnecessary\|out_total\|total_cheap\|find_total\|idx_count_for_leaf\|total_null" src/db/query.c`
— for EVERY find-route site that decides candidates are final (no
per-record tree re-match) or a candidate count is a valid total,
record: site (file:line), condition, sub-tree-blind today (yes/no),
planned change. Expected table (verify, extend if a new site appears):

| site | condition | blind? | change |
|---|---|---|---|
| query.c:6452 `keyset_find_from_intersect` need_rematch | `small_primary \|\| n_postfilter > 0` | yes | `\|\| fp->has_subtree` |
| query.c:6460 total assignment | `!small_primary && n_postfilter == 0` | yes | `&& !fp->has_subtree` (else decline — the caller's null flag carries it, Logical flow 1) |
| query.c:11113 / 11220 caller null-signaling | unconditional `find_total_null = 0` | defeats every decline | `find_total_null = (small_primary \|\| fp.n_postfilter > 0 \|\| fp.has_subtree)` — all THREE intersect decline conditions (round-3 B1r3: `small_primary` is an executor local at 6442; plumb a one-int `out_declined` through the single 11107 call site, or carry it on FilterPlan — Task 0 picks; round-4 advisory: the caller initializes the out-int to 0 at the call site — the executor's early-return paths at 6447 must never feed the decision an uninitialized read), inside the existing `want_total && !dl.timed_out` guard (round-3 m2: the guard wrapper survives the rewrite; the timed-out path keeps emitting null via the untouched flag + skipped 11284 fallback); the union route's 11220 decision is caller-complete as-is |
| query.c:10361 `find_total` init | 0 | — | UNCHANGED (round-2 B1: decline never rides the slot value — SIZE_MAX is the pipeline's hard-error convention) |
| query.c:11272 unconditional SIZE_MAX hard-error check | `find_total == SIZE_MAX → return -1` (OUTSIDE the want_total guard 11275) | NA today (catches idx_count_for_leaf hard-error leaks from 11101/11189/11194) | keep as defense; close its input at the source (next row) — with the null-alone contract the sentinel-as-declined state never exists, so it must never abort an untouched-slot find |
| query.c:11101 / 11189 / 11194 `idx_count_for_leaf` sites feeding find_total | return SIZE_MAX on hard error (7356/7396/7409/7429) | the leak 11272 exists to catch | handle SIZE_MAX explicitly at the source with an error flag (round-2 B1 remedy) — never write it into the slot |
| query.c:11281-11293 tail consumers + fp_compute_total exact fallback | declined → validated exact total (9162-9232) | — | verify it consumes the null-flag contract correctly (its out_null contract 9167/9205/9399 is compatible — round-2 verified) |
| query.c:9439 `prepared_find_prepare` intersect need_rematch | `stream \|\| sp \|\| fp->n_postfilter > 0` (fp IS a parameter, 9403-9410) | yes — bitmap twin of site 1: for a `[bitmap-leaf, btree-leaf, OR…]` tree with both leaves ≥10000 and no limit window, PREP_INTERSECT → `keyset_emit_find(..., need_rematch ? tree : NULL)` at 11001-11002 drops the OR from ROWS while totals stay exact via 9475-9489 (inconsistent rows-wrong/total-right) | `\|\| fp->has_subtree` (round-2 M1) |
| query.c:11011 / 11054 prepared total writes | validated totals (9475-9489) | NA (exact) | none (verify) |
| query.c:6419 `keyset_find_from_or` total | unconditional `*out_total = keyset_size(ks)` | yes for has_subtree plans (partial nL==0 union — OVER-counts: the union is a superset of true matches, 160 reported vs 106 true) | decline when `fp->has_subtree` (fp reachable — parameter of the route's caller; verify the exact plumbing) |
| query.c:11100-11103 streaming-leaf total gate | `want_total && kind != FP_INTERSECT && n_postfilter == 0` | yes (leaf shapes only) | `&& !fp.has_subtree` (exact fallback 11284-11293). NOTE (round-2 m1): the C1-shape `limit:1` red routes HERE (streaming gate 11058-11061 → 11087); site 5 fires only for limit-0 / >1000-row leaf finds |
| query.c:11189-11196 non-streaming leaf total | `idx_count_for_leaf(seed)`, no guard | yes (n_postfilter AND has_subtree) | decline when `fp.has_subtree \|\| fp.n_postfilter > 0` → exact fallback (and SIZE_MAX hard error handled at source, B1) |
| query.c:9455-9480 prepared union | rematch-gated + validated totals | NA (exact for has_subtree plans) | none |
| query.c:6404-6408 leaf rematch | fires only where has_subtree == 0 | NA (provably exact) | none |
| query.c:6655 `total_cheap` | explain display only | NA (verify no other consumer) | none |

### Task 1 — regression test (test-first; expect RED on base)

New file `src/test/cases/test_find_subtree_totals.c`, registered
`TEST_REGISTER("test-find-subtree-totals", ...)`, wired into `build.sh`
next to the find cases (anchor: the `test_find_with_total.c \` line —
copy its fixture/request helpers; note its 64KB request-chunking
pattern for big loads). Objects: `ft_plain` (no indexes, ORACLE — read
expected totals from its COUNT-mode responses, never its find totals:
FULL_SCAN find totals are `null`), `ft_ix` (`v,t,w` btrees, `u:int`
unindexed; fields `v:varchar:48` 5 cats, `w:varchar:16` 3 cats,
`t:long`, `u:int`; 400 rows), `ft_big` (same fields plus
`b:varchar:16`; ~24000 rows; `v` = 2 values, `w` = 2 values; indexes
`v,t,w` btrees + **`b:bitmap`** — the round-3 M1r3 element that makes
the prepared intersect route (9437-9439) reachable; do NOT make `w`
bitmap, that would flip assertion 3's shape to the prepared route and
destroy its site-1 red; `b` = 2 values `b_0`/`b_1` alternating, 12000
rows each ≥ the 10000 threshold; chunk the bulk-inserts per the
helper's pattern). `ft_plain` loads the IDENTICAL dataset as its twin
per shape (the oracle claim requires it — state it in the case
comment). Every OR child in assertions and controls must be an INDEXED
field (`u` is unindexed — an OR containing it yields FP_FULL_SCAN,
find_fully_indexed_or NULL at query_plan.c:3593-3599, changing what
the control pins).

Assertions (compare against `ft_plain`'s count-mode numbers):

1. C1 shape, fixture scale (400 rows on ft_ix): `find`
   `[t between 50..149, OR[v eq cat_1, u eq 7]]`,
   `limit:1,total:true` → total == oracle count. RED today via site 4 —
   the streaming gate (limit:1 routes the C1 shape through the
   streaming executor, 11058-11061 → 11087, and the total gate at
   11100-11103 is sub-tree-blind; site 5 fires only for limit-0 /
   >1000-row leaf finds; the QRY-2 review measured 100 vs 26 on the
   equivalent shape).
2. AND-of-ORs, fixture scale: `[{or[v=cat_1,v=cat_2]},
   {or[w=w_0,w=w_2]}]`, `total:true`, no limit → total == oracle
   count AND rows == oracle rows. RED today via site 3 (6419 partial
   union total; measured 106 vs 160 class).
3. Large intersect + OR (site 1): on `ft_big`, criteria
   `[v eq <val≥10000 rows>, w eq <val≥10000 rows>, OR[u gte 12000,
   u lt 1000]]` — BOTH eq leaves' candidate counts must exceed 10000
   (the threshold is per-smallest-leaf) so `small_primary == 0` and
   the OR drops from rows AND total; ~24000 rows at 2 values per
   field satisfies this. Assert rows AND `total:true` vs oracle — NO
   limit (a `limit > 0` window routes the shape to the streaming
   executor, 11058, and silences the red). RED
   today. (Verify the red is the intersect route by the DROP ITSELF:
   rows must exceed the OR-filtered count on base — no explain-based
   small_primary assertion; explain cannot show it.) Pin `u = key
   index` (the reviewer's distribution pin): the red requires
   intersection rows OUTSIDE the OR band [1000, 12000) — u values in
   [0, 24000) put ~23 000 there, so the algebra is checkable up front
   and a degenerate all-in-band u cannot silently kill the red.
3b. Prepared intersect + OR (site 9439, round-3 M1r3): on `ft_big`,
    criteria `[b eq b_0, v eq <val≥10000 rows>, OR[u gte 12000,
    u lt 1000]]`, NO limit, no total assertion — ROWS == oracle rows.
    RED on base via the bitmap-leaf+btree-leaf prepared route
    (find_plan_reads_bitmap → PREP_INTERSECT at 9437-9439 → OR dropped
    from rows while the emitted total stays exact — the
    rows-wrong/total-right pairing). Verify the red is THIS route by
    the drop itself (rows exceed the OR-filtered count on base); the
    `b:bitmap` index is what makes the shape reach the prepared route
    at all (all-btree shapes never do — that is why this assertion
    exists alongside assertion 3).
4. Declined-total contract (the sentinel fix): small intersect find —
   `ft_ix`, `[v eq cat_1, w eq w_1]` under the threshold,
   `total:true` → post-fix `total` is EITHER the exact oracle count OR
   `null` — never a wrong number; on base record what it emits
   (expected `"total":0` — the pre-existing wrong class; if it emits
   the exact number today, record that and assert exactness
   post-fix instead; halt per PLAN_NOTES only if the shape cannot be
   pinned).
5. Controls (GREEN before and after): pure-OR root find with total on
   `ft_ix` (exact union, unchanged); leaf-only find with total (no OR
   — exact, unchanged); small intersect + OR ROWS equal the oracle
   (rematch fires at fixture scale today — rows-only control; do not
   assert its total against the oracle, that is assertion 4's class).

Test-first proof: 1-3 + 3b RED for the expected reasons (totals/rows vs
oracle count-mode numbers), 4's base state recorded, 5 GREEN. Paste.

### Task 2 — the fix

Apply Task 0's table in `src/db/query.c`: the two rematch/total terms
at 6452/6460; the null-alone decline contract (the two caller sites
11113/11220 — 10361's init stays 0, B1); the 6419 union-total decline;
the 11100 gate term; the 11189-11196 decline; the 9439 prepared term
(M1); the 11272/11101/11189/11194 hard-error reconciliation (explicit
error flag at the source sites — 11272 retained as defense). Each site
gets a one-line `/* AUD-QRY-4: ... */` comment. No planner changes
(the flag exists from QRY-2); no aggregate/count changes;
`rematch_unnecessary` (6404) and the prepared-union route stay
untouched.

### Task 3 — prove and gate

1. Revert-prove-paste: `git stash push -- src/db/query.c` → rebuild →
   Task 1 reds → `git stash pop` → rebuild → greens. Paste.
2. Targeted case + full suite `run-all --jobs 8` fresh (note ft_big's
   added wall time alongside QRY-2's 40k case).
3. Sanitizer gate once each (asan `--jobs 6`, tsan `--jobs 3`); rebuild
   normal.
4. Differential spot-check — find section stays 0-divergence on the
   generated shapes; record DIVSEC.
5. Changelog convention check + entry if per-fix.
6. Index: AUD-QRY-4 → `fix-branch` with this branch name + Task 0's
   final table summarized in the row.

### Embedded execution rules

Tasks in order; both revert outputs pasted; no unrelated changes;
`PLAN_NOTES.md` + halt on mismatch (Task 0 table surprises, Task 1
shape-3 threshold failures especially).
