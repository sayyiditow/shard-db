# fix/audit-agg-subtree-cover-gate — AUD-QRY-2

Campaign: `docs/plans/2026-11-19-audit-campaign.md` Phase 4 (second DL
branch). Finding: `AUD-QRY-2` in `docs/plans/2026-11-19-audit-index.md`
(DL: silent over-count in aggregate scalar and group_by). Branch name:
`fix/audit-agg-subtree-cover-gate`. Branch base: see Ground rules (single rule). Sequence:
lands after `fix/audit-igb-bitmap-group-fallback` (AUD-QRY-1) so the two
fixes' tests don't fight over the same bitmap group_by shapes.

## Brief (approval surface)

### Problem and root cause

`aggregate` (scalar **and** group_by) over-counts filtered queries: criteria of the
form `[indexed-leaf, OR[…]]` aggregate over the leaf's full candidate set —
the OR sub-tree is silently dropped (repro: `count` = 7, `find` = 7 rows,
`aggregate count` = 26 on the same data; the 26 is the whole leaf set).
Root cause: the planner records OR/nested-AND sub-trees as `has_subtree`
but then **discards the flag** (`(void)has_subtree;` in `plan_filter`,
query_plan.c — the `FilterPlan` struct has no field for it). What the plan
does carry — source leaves and `postfilter_leaves` — is verified where
those two lists are consumed (postfilter-leaf criteria are correct today:
the unindexed-leaf control proves the record-recheck fallback), but a
sub-tree is in NEITHER list, so no executor can recheck it — including
the fast paths below. (Verified 2026-11-20 by code read and wire runs:
`agg_run_plan`'s primary-leaf and FP_UNION branches DO re-check the full
tree per record, because `ctx->tree` stays set and `agg_scan_cb` evaluates
it per record; an earlier amendment claiming otherwise was wrong. The
post-QRY-1 residual orprobe divergence, 14 vs 20, comes from the IGB
`can_slot` bitmap slot-walk, fixed by this plan's Task 2/3 gates.) The no-record-fetch
KeySet fast paths treat the plan's source set as the full predicate:

- `agg_criteria_fully_covered` (query_plan.c) — the gate that admits trees
  into the aggregate KeySet fast paths — returns "covered" when
  `fp.kind != FP_FULL_SCAN && fp.n_postfilter == 0`. An OR sub-tree is
  neither a source nor a postfilter, so sub-tree-bearing trees pass the gate
  with a whole unverified predicate.
- Consumers of that gate: the scalar `aw` fast path (count(*) = |KeySet|,
  sum/min/max via `agg_walk_field` over the KeySet — no record fetch) and
  the IGB group-by path (`crit_ks` filters the group-field walk — no record
  fetch).
- The IGB bitmap slot-level branch's `can_slot` condition checks
  `source_is_bitmap && n_postfilter == 0 && all-eq` — sub-tree-blind as
  well, so bitmap-eq leaf + OR-subtree criteria over-count groups.
- The TopN streaming prefilter (`eligible_for_topn_stream` + the
  `topn_fp` keyset builders) has the same shape (count-only specs, no
  record fetch) and needs the same guard.

`count` is correct today (its intersect executor re-checks or accounts
for partial coverage). `find` carries the SAME sub-tree hole at large
intersect primaries: `keyset_find_from_intersect` restores the tree only
when `small_primary || n_postfilter > 0`, so a two-indexed-leaf intersect
with an OR beside it drops the OR from rows and total once the candidate
keyset exceeds the small-primary threshold. That find-side hole is
recorded as its own campaign finding (AUD-QRY-4) and is OUT OF SCOPE here
(one concern per branch). This plan must not cite find as a correctness
precedent.

### Logical flow

1. `plan_filter` computes `has_subtree` (AND root with any non-LEAF child)
   and, after this change, stores it in `FilterPlan`.
2. `agg_criteria_fully_covered` additionally requires `!fp.has_subtree` —
   trees with sub-trees are no longer "fully covered" and stop being
   admitted to the scalar `aw` and IGB KeySet fast paths, which fall back to
   the record-scan executors (exact, slower).
3. The IGB `can_slot` condition additionally requires `!igb_crit_fp.has_subtree`
   — sub-tree-bearing bitmap-eq criteria route out of the slot-level walk.
4. The TopN prefilter builders refuse `has_subtree` (and postfilter)
   plans via their existing `-2` fallback.
5. `keyset_agg_from_intersect`'s record-phase restore condition gains the
   sub-tree flag (relocation blocker).
6. A second silent leaf-loss route is closed by the same flag: an AND root
   with more than `MAX_INTERSECT_LEAVES` (8) LEAF children loses leaves 9+
   in `collect_and_leaves` (in neither source nor postfilter list, so
   `n_postfilter == 0`); `plan_filter` sets `has_subtree` (documented as
   "criteria not fully represented by the plan") for that shape too.

### Decisions made

| decision | alternatives | why | tag |
|---|---|---|---|
| Carry `has_subtree` in `FilterPlan`, refuse at the no-fetch gates, and extend the intersect record-phase restore condition | (b) plan the OR sub-tree itself (flattened leaf extraction + keyset union/intersect); (c) re-verify per record inside the fast paths (fetch after all) | (a) is one field + one predicate term per gate and falls back to executors that are already the differential oracle; (b) is a planner feature (real perf win for common OR shapes) but new planning logic needs its own oracle coverage first; (c) negates the reason the fast paths exist (no record fetch) | DEFAULT |
| Guard predicate = `fp.has_subtree \|\| fp.n_postfilter > 0` at TopN (which checks plan fields directly, not `agg_criteria_fully_covered`) | rely on fully_covered there | TopN eligibility does not route through `agg_criteria_fully_covered` today; its keyset builders consume `topn_fp` directly | DEFAULT |
| `keyset_agg_from_intersect`: extend the restore condition; leave `agg_run_plan`'s primary-leaf and FP_UNION branches unchanged (verify-only) | gate those branches too | they re-check the full tree per record (`ctx->tree` set at entry, cloned by `agg_ctx_clone_shared`, evaluated in `agg_scan_cb`); gating them would force a full kf-shard scan for every `[selective indexed leaf, OR]` aggregate that is correct and fast today. Only FP_INTERSECT nulls the tree, which Task 3 fixes | DEFAULT |
| Fix only the aggregate family; `count`/`find` untouched | audit them too | both re-check per record today; 866-divergence differential shows zero count divergence (find has its own holes, recorded as AUD-QRY-4) | DEFAULT |

### Blast radius

- `FilterPlan` grows one `int` field (memset-initialized; no ABI surface —
  internal struct, `query_internal.h`).
- Consumers of `agg_criteria_fully_covered`: the scalar `aw` gate and the
  IGB gate (both fall back to correct paths). Queries affected: aggregate
  scalar/group_by with OR sub-trees beside indexed leaves — today silently
  wrong, after the fix correct (slower).
- `can_slot` + TopN: same query classes.
- `agg_run_plan` primary-leaf and FP_UNION branches: unchanged (they
  re-check the tree per record).
- Perf note for the Brief: OR-beside-leaf aggregates lose the no-fetch
  shortcut. `bench-queries` aggregate benches don't use OR-beside-leaf
  criteria (verify at execution; paste before/after if one does).
- No wire/protocol/on-disk change.

### Test strategy

- New product regression case `test_agg_or_subtree_full_tree` (Task 1):
  scalar OR case, all-indexed OR case (pure `aw` drop), group_by OR case,
  bitmap slot-level OR case, TopN OR case, the orprobe shape on the
  bitmap twin (case 12 — post-QRY-1 residual, red via the IGB `can_slot`
  path, green with Task 2 alone), the >8-leaf AND (case 13) — each against a same-data no-index oracle object, plus
  a postfilter-leaf control (correct today because the unindexed leaf forces the record-recheck executor; it proves fallback exactness, not fast-path retention), and the in-process fast-path-retention assertions (case 15).
- The campaign planner differential remains the wide net; after this branch
  the planner-diff aggregate/group_by sections must be at zero OR-attributed
  divergences.
- Full suite + sanitizer gate as AGENTS prescribes (the diff touches no
  locks/lifetimes → single pass each; full suite mandatory).

### Out of scope

- Planning OR sub-trees themselves (perf follow-up, own plan).
- AUD-QRY-1 / AUD-QRY-3 (separate branches).
- `having`-clause and cursor interactions: IGB results feed `having`, so
  `having` over the affected group_by shapes IS fixed as a side effect of
  the same gate; no `having`-specific code changes here. Cursor paths are
  not affected (same-tree evaluation, verified by existing cases).
- Find-side holes recorded as AUD-QRY-4 (widened): (1) the large-intersect
  `keyset_find_from_intersect` restore condition; (2) the streaming-leaf
  `find_total` in `cmd_find` (`want_total && fp.kind != FP_INTERSECT &&
  fp.n_postfilter == 0` is sub-tree-blind; measured: `limit:1,total:true`
  returns total 100 vs true 26 on the C1 shape, 160 vs 106 on
  AND-of-ORs); (3) the UNION `total` (|KeySet|); (4) every `total_cheap`
  consumer. The new `FilterPlan.has_subtree` field is the intended gate
  for all of them, to be consumed in the QRY-4 branch.
- AUD-QRY-8 (new, DL-class, separate plan): when a composite index fully
  covers the eq leaves (`find_exact_covering_composite` → `FP_PRIMARY_LEAF`
  with `n_source > 1`, `n_postfilter = 0`, `FP_ORDER_COMPOSITE_EXACT`),
  `count` returns 0 and `aggregate` returns `[]` while `find` is correct
  (repro: fields a,b,c; indexes `b` and `a+b`; criteria `[a eq 1, b eq
  2]`; find 20 rows, count 0). No aggregate/count code handles
  `COMPOSITE_EXACT`: `aw`, IGB, TopN and `agg_run_plan` all walk
  `source_leaves[0]`, a field with no standalone index, and
  `agg_criteria_fully_covered` calls it covered. This plan's gate does not
  cover it.

### Review log

- Round 1 (blind reviewer, 2026-11-19): 1 blocker — `[indexed-leaf,
  indexed-leaf, OR…]` trees yield FP_INTERSECT with `n_postfilter == 0`,
  so the planned gate refusal relocated the drop into
  `keyset_agg_from_intersect` (restores the tree only on
  `small_primary || n_postfilter > 0`); fixed: restore condition extended
  with the sub-tree flag + new Task 1 case 6. 2 majors — AND-of-two-ORs
  (nL==0) unions only the first OR conjunct and stayed has_subtree=0
  (fixed: flag set for AND-with-OR-children + case 7); the Brief's
  "find is correct" claim was false at large intersect primaries
  (corrected; find-side hole recorded as AUD-QRY-4, out of scope here).
  5 minors fixed: can_slot reworded as defense-in-depth, goto-site
  control-flow note corrected, TopN guard extends the existing check with
  matched errno shape, TopN test red-conditions (count alias + limit <
  categories), postfilter-control wording.
- Round 2 (blind re-review): 0 blockers, 0 majors.
- Round 3 (independent blind review, 2026-11-19): 0 blockers, 3 majors, 5
  minors, all folded in. M1: the nL==0 flag fired on the common array-form
  `[{"or":[...]}]` (single OR child, union exact), disabling fast paths for
  it (fixed: flag only when the AND has a child beyond the returned OR;
  control added). M2: the Task 3 restore-condition change had no failing
  test since `small_primary` always fires at fixture scale (fixed: large
  all-bitmap-AND case 9, red with Task 3's restore reverted; case 6
  group_by=v on the bitmap object is already red today). M3: find `total`
  holes recorded under widened AUD-QRY-4. New sibling finding AUD-QRY-8
  (composite-exact count/aggregate wrong). Reviewer ran the fixture:
  cases 1,2,3,4,5,7 red today (100 vs 26, 100 vs 47, 80 vs 53, 160 vs
  106). Minors: branch base unified, fixture/helper names given, run
  command given, having wording, no-op initializer check dropped.
- Round 6 (independent blind, 2026-11-20): 0 blockers, 2 majors, 8
  minors, all folded: case 11 could not fail (added in-process case 15
  asserting `has_subtree`/`fully_covered`); the revert-prove procedure
  would have reverted QRY-1 too (patch-based procedure, manual one-term
  revert for case 9). Minors: complete RED list, TopN variants moved to
  `agg_btree`, case 5 tie handling, case 9/14 fixture formulas, >8 rule
  placement, Brief wording contradictions, over-count wording.
- AMENDMENT (2026-11-20) REVERTED after review round 5: the amendment
  gated `agg_run_plan`'s primary-leaf and FP_UNION branches on the false
  premise that they do not re-check the tree (they do; verified in code
  and by wire runs). Round 5 (independent blind, 2026-11-20): 1 blocker
  (that false premise: Task 4 would force a full scan on correct fast
  queries; the orprobe red actually comes from IGB `can_slot`), 1 major (an
  AND with more than 8 LEAF children silently loses leaves 9+, same bug
  class; folded into the `has_subtree` rule, case 13), minors (case 11 used
  an unindexed OR so it scanned; case 9 fixture unspecified; Task 4 comment
  rewrite would make comments false). All folded; Task 4 is verify-only
  again.

---

## Plan (executor-facing)

### Ground rules

Branch base: the campaign harness working tree (`test/audit-oracle-harnesses`)
stacked after `fix/audit-igb-bitmap-group-fallback`'s changes (this plan's
bitmap twin tests build on QRY-1's scan fallback), or `main` if both have
landed there by execution time. This is the single branch-base rule; the
header line defers to it. Revert-prove steps use path-limited stashes
only (`git stash push -- <fix files>`), never a bare `git stash`: the
base may carry the uncommitted harness tree.
Work uncommitted until human review. Build/test commands per AGENTS.
`PLAN_NOTES.md` + halt on mismatch.

### Background: the five sites, precisely

All in src/db unless named. The planner side:

- `plan_filter` (query_plan.c): computes local `has_subtree`
  (AND root with a non-LEAF child), currently ends with `(void)has_subtree;`.
  `FilterPlan` (query_internal.h) has no field for it.
- `agg_criteria_fully_covered` (query_plan.c): returns
  `fp.kind != FP_FULL_SCAN && fp.n_postfilter == 0`.

The aggregate consumers:

- Scalar `aw` fast path (query_aggregate.c, gate
  `agg_criteria_fully_covered(...)` above the `aw_eligible` loop): builds
  `crit_ks` from plan sources only; `count(*) = |KeySet|`;
  `agg_walk_field` accumulates from index leaf bytes for KeySet members. No
  record fetch → sub-tree dropped. RED EVIDENCE:
  `docs/plans/audit-artifacts/AUD-QRY-2-repro.out` (o_btree/o_bitmap
  aggregate count 26 vs plain 7, same criteria), plus planner-diff
  aggregate-section divergences (102 + 182 across full-budget seeds, all
  OR-bearing).
- IGB group-by (query_aggregate.c, gate `agg_criteria_fully_covered` at the
  `igb_eligible` computation; `crit_ks` filters the group-field walk): same
  drop, wrong group counts. RED: planner-diff `pd_plain vs pd_btree`
  group_by divergences (51 + 91, all OR-bearing) — note these shapes have a
  btree group field, so they are NOT QRY-1's empty-bucket class.
- IGB bitmap slot-level branch: `can_slot` requires
  `igb_crit_fp.source_is_bitmap && n_postfilter == 0` and all-eq sources —
  has_subtree-blind; a bitmap-eq leaf beside an OR sub-tree slot-walks with
  the sub-tree dropped.
- TopN streaming (`eligible_for_topn_stream` + the `topn_fp` keyset
  builders right after): count-only specs, no record fetch; sub-tree (and
  postfilter) blind.
- `agg_run_plan`'s FP_PRIMARY_LEAF / FP_BITMAP_SMALLER branch and its
  FP_UNION branch ARE sub-tree-safe (verified, review round 5):
  `agg_run_plan` sets `ctx->tree = tree` at entry, `parallel_indexed_agg`
  clones it with `agg_ctx_clone_shared`, and `agg_scan_cb` calls
  `criteria_match_tree_checked` on every record; `keyset_agg_from_or` →
  `keyset_emit_agg` also reaches `agg_scan_cb` with the tree intact (see the
  comments at query_aggregate.c:64-68 and above `agg_criteria_fully_covered`).
  The post-QRY-1 residual (orprobe 14 vs 20) is the IGB `can_slot` slot walk:
  source `w eq` (bitmap), `n_postfilter == 0`, all sources eq, `can_slot`
  never looks at the sub-tree.
- `keyset_agg_from_intersect` is NOT sub-tree-safe: it restores the
  original tree for the record phase only when `small_primary ||
  fp->n_postfilter > 0`; a full intersect with `has_subtree` restores
  nothing, `agg_scan_cb` then runs with a NULL tree node (matches
  everything) and the OR is dropped. This is the relocation hazard for
  this fix (review round 1, blocker) — Task 3 extends the restore
  condition.

### Task 1 — regression test (test-first; expect RED on audit-base)

New file `src/test/cases/test_agg_or_subtree_full_tree.c`, registered `TEST_REGISTER("test-agg-or-subtree-full-tree", ...)` and wired into `build.sh` next to the aggregate cases; copy the fixture/daemon and response-parsing helpers from `src/test/cases/test_agg_topn_stream.c` / `test_aggregate_type_matrix.c` (`test_env_start(&env)`; bitmap index syntax `"v:bitmap"` in create-object; TopN request = `group_by`, `order_by:"<alias>"`, `order`, `limit`). Fixture: daemon;
objects `agg_plain` (no indexes) and `agg_btree` (`v,t,w` btrees), fields
`v:varchar:48` (5 cats), `u:int` (i%13), `t:long` (=i), `w:varchar:16`
(i%3), 400 rows. Assertions, each comparing `agg_btree` against
`agg_plain`:

1. Scalar OR, mixed-indexed: `criteria:
   [{"field":"t","op":"between","value":50,"value2":149},
   {"or":[{"field":"v","op":"eq","value":"cat_1"},
   {"field":"u","op":"eq","value":7}]}]`, aggregate count — today 100 on
   `agg_btree` (full leaf set) vs oracle (true intersections); the numbers
   differ per fixture — assert equality with the oracle, not a constant.
2. Scalar OR, all-indexed: same OR over `v` and `w`
   (`{"or":[{"field":"v","op":"eq","value":"cat_1"},{"field":"w","op":"eq",
   "value":"w_2"}]}`) beside the t-range — the RED comes from the
   FP_PRIMARY_LEAF(t) seed dropping everything beside its leaf; the gate
   never inspects OR children either way, so this shape passes
   `agg_criteria_fully_covered` with the sub-tree unverified.
3. group_by OR: group_by=v with the mixed-indexed OR criteria — per-label
   count vector equality vs oracle.
4. Bitmap slot-level OR (needs a bitmap twin): add `agg_bm`
   (`v:bitmap`,`w:bitmap`,`t`) with identical rows; criteria
   `[{"field":"v","op":"eq","value":"cat_1"},{"or":[{"field":"w","op":"eq",
   "value":"w_1"},{"field":"w","op":"eq","value":"w_2"}]}]`, group_by=v —
   today slot-walks with the OR dropped.
5. TopN OR (on `agg_btree`; TopN needs a btree on the group field — on
   `agg_bm` the TopN variants never reach TopN, they go IGB + sort): aggregate
   with `order_by` set to a **count alias** (TopN eligibility requires the
   order spec to be count) and `limit` strictly less than the number of
   distinct `v` categories; the tie at the boundary makes labels order-
   dependent, so compare the sorted count vector (and labels only above the
   tie boundary) — compare the emitted
   counts, not just group membership (membership alone can pass with
   wrong counts).
6. Two indexed AND-leaves + OR (the intersect relocation shape):
   `[{"field":"v","op":"eq","value":"cat_1"},{"field":"w","op":"eq",
   "value":"w_1"},{"or":[{"field":"u","op":"eq","value":5},
   {"field":"u","op":"eq","value":9}]}]`, scalar count on `agg_btree` —
   `plan_filter` yields FP_INTERSECT with `n_postfilter == 0` and
   `has_subtree` set. At this fixture scale the smallest leaf keyset is
   under INTERSECT_MIN_PRIMARY (10000), so `small_primary` fires and the
   tree IS restored — assertion GREEN today; the drop is real only above
   the threshold, or under a Task-2-only build (gate refuses, restore not
   yet extended). The flag-based fix is size-independent; this case pins
   the exact post-fix behavior at fixture scale (and documents why it
   must not be expected red here).
7. AND-of-two-ORs scalar count:
   `[{"or":[{"field":"v","op":"eq","value":"cat_1"},{"field":"v","op":"eq",
   "value":"cat_2"}]},{"or":[{"field":"w","op":"eq","value":"w_0"},
   {"field":"w","op":"eq","value":"w_2"}]}]` — the nL==0 union covers
   only the FIRST OR conjunct today; must equal the oracle after the
   Task 2 nL==0 flag.
9. Large-scale restore-condition case (pins Task 3; the only case that
   fails without it): ~40k rows, object with `v` and `w` as 2-value
   bitmaps (all-bitmap AND → `FP_INTERSECT` regardless of selectivity,
   each leaf matching ≥ 10000 rows so `small_primary == 0`) plus an OR
   over `u`; aggregate (fetching=0) count compared with the no-index
   oracle. Proof of necessity: RED with Task 2's gate applied but Task 3's
   `|| fp->has_subtree` restore reverted; GREEN with both. Mark the case
   slow if the suite has such a convention.
10. IGB intersect shape: on `agg_bm` (bitmaps on v,w + btree t) at fixture
   scale, case-6 criteria with `group_by:"v"` (and a TopN variant,
   `order_by:"<count alias>"`, `limit` < categories) — RED today (reviewer
   measured 27 vs 4); must match the oracle after the fix.
11. Array-form control: `[{"or":[{"field":"v","op":"eq","value":"cat_1"},
   {"field":"w","op":"eq","value":"w_1"}]}]` — an ALL-INDEXED OR (so
   explain reports `"plan":"union"`, not a scan; an OR containing the
   unindexed `u` would scan and could not catch a wrongly-firing Task-2
   `n_children > 1` rule). Assert the plan via explain is union AND the
   aggregate equals the oracle.
12. Orprobe shape on the bitmap twin (post-QRY-1 residual; evidence
    `docs/plans/audit-artifacts/AUD-QRY-1-residual-orprobe.{sh,out}`, 14 vs
    20): on `agg_bm`, criteria `[{"field":"w","op":"eq","value":"w_0"},
    {"or":[{"field":"v","op":"eq","value":"cat_2"},{"field":"u","op":"between",
    "value":2,"value2":9}]}]`, group_by=v count — full per-label vector vs
    `agg_plain`, plus scalar count() cross-check. RED on the base (audit-base
    + QRY-1) because the IGB `can_slot` slot walk ignores the OR; GREEN
    with Task 2 alone (`fully_covered` refusal). Its RED/GREEN boundary is
    Task 2, not any `agg_run_plan` change.
13. Over-8-leaves AND (`MAX_INTERSECT_LEAVES`): on `agg_bm`, criteria
    `[v eq cat_1 x4 (4 identical leaves), w eq w_1 x4, u eq 7]` (9 LEAF
    children) — RED today (group_by v returns 27 vs plain 2, count
    correct = 2; TopN variant, if wanted, on `agg_btree` where TopN is
    reachable); GREEN after the Task 2 `has_subtree` extension.
14. Case 9 fixture (previously unspecified): object `agg_big`, 40,000 rows
    via bulk-insert, keys `k%06d`, `v:varchar:16` (`a`/`b` by i%2,
    `v:bitmap`), `w:varchar:16` (`w:bitmap`, assigned so each leaf and the
    leaf intersection stay >= 10000 rows: `w` = `w_` + `(i/2)%2`, so each
    leaf = 20000 and v AND w = 10000), `u:int` (i%13, btree). Values are
    formatted `a`/`b` and `w_0`/`w_1`. Case 9's criteria: `[v eq a, w eq
    w_0, {or:[u eq 3, u eq 7]}]` (2 bitmap leaves → serial intersect path,
    where `small_primary` checks `leaves[0]`; each leaf and the
    intersection >= 10000 = INTERSECT_MIN_PRIMARY). Oracle twin
    `agg_big_plain` with no indexes. (This fixture is the setup for case 9
    above; read them together.)
16. (Controls, originally numbered 8; ordering note: cases are referenced by
    number elsewhere, so numbers are kept) Controls (must pass before AND after): postfilter-leaf scalar
   (`[t-range, u-gte]` — correct today BECAUSE the unindexed leaf forces
   the record-recheck executor; the control proves fallback exactness,
   not fast-path retention); pure-OR root (UNION, exact); fully-indexed
   AND without OR (intersect, exact).

15. Fast-path-retention assertions (in-process; `agg_criteria_fully_covered`
    is non-static in query_plan.c and `test_agg_topn_stream.c` already calls
    daemon internals — copy that pattern; explain cannot show `has_subtree`):
    `plan_filter(...).has_subtree == 0` / `fully_covered == 1` for
    `[{"or":[v eq, w eq]}]` (array form), `{"or":[...]}` root, and `[t-range]`;
    `has_subtree == 1` / `fully_covered == 0` for `[t-range, OR]`, AND-of-ORs
    and the 9-leaf shape. This is the only test that fails if the
    nL==0 rule wrongly fires (case 11 only guards result correctness).

Test-first proof: on the branch base (audit-base + QRY-1 branch state),
assertions 1-5, 7, 9, 10, 12, 13 and 15's `==1` expectations RED for the
expected reason (fast-path value ≠ oracle / field absent), assertions 6, 8
and 11 green. Paste. (Measured on the pre-fix tree by the reviewer: cases
1-5, 7, 9, 10, 12, 13 red; 6, 8, 11 green.)

### Task 2 — planner: carry the flag

- `FilterPlan` (query_internal.h): add `int has_subtree;` (memset-zeroed
  construction everywhere; designated/partial initializers also zero unnamed
  members, so no per-site check is needed).
- `plan_filter`: replace `(void)has_subtree;` with
  `fp.has_subtree = has_subtree;`. There are five `goto order_overlay`
  sites: the two UNION jumps (CNODE_OR root and the nL==0 AND-of-ORs
  block) happen BEFORE the has_subtree computation and leave it memset-0;
  the three intersect/bitmap jumps happen after it. UNION-leaves-0 is
  correct for a pure CNODE_OR root (the union is exact), but the nL==0
  block's union is PARTIAL when `tree->kind == CNODE_AND` (an AND whose
  children are all OR nodes: `find_fully_indexed_or` unions only the
  FIRST fully-indexed OR child — the planner's own KNOWN-LIMITATION
  comment admits the rest are not narrowed). So in the nL==0 block, set
  `fp.has_subtree = 1` only when `tree->kind == CNODE_AND` AND the AND has
  a child other than the single OR the union returned (e.g.
  `tree->n_children > 1`) — the common array form `[{"or":[...]}]`
  normalizes to a CNODE_AND with a single OR child, whose union is exact
  and must keep the scalar/IGB/TopN fast paths:
  AND-of-ORs then refuses the no-fetch fast paths, which cannot recheck
  what the union did not narrow. The CNODE_OR-root union stays
  has_subtree=0 (exact).
- In `plan_filter`, INSIDE the `has_subtree` computation loop and before
  `fp.has_subtree = has_subtree;` (a later placement at `order_overlay`
  would miss the composite-exact and FULL_SCAN returns), also set
  `has_subtree = 1` when a CNODE_AND root has
  more than `MAX_INTERSECT_LEAVES` LEAF children (`collect_and_leaves`
  silently drops leaves 9+ into neither the source nor the postfilter
  list). Update the field's comment to "criteria not fully represented by
  the plan".
- `agg_criteria_fully_covered`: return
  `fp.kind != FP_FULL_SCAN && fp.n_postfilter == 0 && !fp.has_subtree;`
  and fix the comment above it ("n_postfilter == 0" → "n_postfilter == 0
  and no OR/nested-AND sub-tree").

### Task 3 — aggregate gates

- `keyset_agg_from_intersect`: extend the tree-restore condition to
  include the sub-tree flag — today `small_primary || fp->n_postfilter > 0`;
  after this task also `|| fp->has_subtree`. Without this, refusing the
  no-fetch fast paths (Task 2) merely relocates the drop into this
  executor for `[indexed-leaf, indexed-leaf, OR…]` trees above the
  small-primary threshold (review round 1 blocker).
- IGB `can_slot`: extend the condition with `&& !igb_crit_fp.has_subtree`.
  NOTE: after the fully-covered gate this is defense-in-depth only — keep
  it because `can_slot` reads the plan directly and must not silently
  relax if the gates ever change.
- TopN: the existing postfilter refusal after `topn_fp` is computed
  (`topn_fp.n_postfilter > 0` → destroy heap, return -2) — extend it with
  `|| topn_fp.has_subtree`, matching that site's exact cleanup and errno
  shape (no `errno = 0` there; the caller ignores errno on -2). Do not
  add a parallel second check.

### Task 4 — verification (no code change)

Confirm by code read, and record the result in the branch notes, that
`agg_run_plan` keeps `ctx->tree` intact for the FP_PRIMARY_LEAF /
FP_BITMAP_SMALLER and FP_UNION branches and that `agg_scan_cb` evaluates it
per record (set at entry; `agg_ctx_clone_shared` copies it; no nulling
except in `keyset_agg_from_intersect`). The round-5 reviewer re-verified
this with wire runs through `agg_run_plan` (primary-leaf on bitmap w, case 1
with an unindexed sum field, case 7 with an unindexed sum field — all
correct). If your read contradicts it, `PLAN_NOTES.md` and halt. Do NOT add
a gate to these branches: it would replace a correct candidate drive with a
full kf-shard scan.

### Task 5 — prove and gate

1. Revert-prove-paste per CORE-PROCESS. The QRY-1 fix is uncommitted in the
   SAME file as this fix (`src/db/query_aggregate.c`), so `git stash push --
   <file>` would revert QRY-1 too. Instead: BEFORE Task 2, snapshot the
   base (`git diff -- src/db/query_aggregate.c src/db/query_plan.c
   src/db/query_internal.h > /tmp/qry2-base.patch`, plus a `cp` of each file
   to a scratch dir). After the fix, save the QRY-2 delta as a patch
   (`git diff` against the snapshot), prove red with `git apply -R
   <qry2.patch>` → rebuild → run → `git apply <qry2.patch>` → rebuild →
   green; paste both. For case 9, additionally revert by hand the single
   `|| fp->has_subtree` term in `keyset_agg_from_intersect`'s restore
   condition (a stash/patch cannot select one hunk), rebuild, confirm case 9
   alone goes red, restore.
2. Targeted case + full suite `run-all --jobs 8`.
3. Campaign differential spot-check (`SHARD_AUDIT_DIFF_ROWS=5000
   SHARD_AUDIT_DIFF_CRITERIA=200 ./build/bin/shard-db-test run-all
   --include-audit --filter audit-planner-diff`; DIVSEC = the per-section
   divergence counter line the harness prints; attribute by the per-criterion
   `DIVERGENCE` shape pairs):
   aggregate-section OR-attributed divergences and btree-pair group_by
   OR-attributed divergences must be zero; bitmap-pair group_by
   divergences must be zero after combined with the QRY-1 branch. Record
   DIVSEC.
4. Sanitizer gate once each (asan `--jobs 6`, tsan `--jobs 3`); rebuild
   normal.
5. Index: AUD-QRY-2 → `fix-branch`, note the root-cause refinement
   (agg_criteria_fully_covered + missing FilterPlan field; the
   slot-level/TopN holes) in the row.

### Embedded execution rules

As QRY-1's plan (tasks in order, both revert outputs pasted, no unrelated
changes, PLAN_NOTES on mismatch).
