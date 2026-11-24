# fix/audit-composite-exact-agg — AUD-QRY-8

Campaign: `docs/plans/2026-11-19-audit-campaign.md` Phase 4 (DL queue,
second of the post-triad branches). Finding: `AUD-QRY-8` in
`docs/plans/2026-11-19-audit-index.md` (DL-class: `count` returns 0 and
`aggregate` returns 0/`[]` for criteria the planner served with the
composite-exact plan, while `find` is correct). Branch name:
`fix/audit-composite-exact-agg`. Branch base: the campaign harness
working tree (`test/audit-oracle-harnesses`), stacked after the uncommitted
QRY-1/QRY-2 changes if still uncommitted, or `main` if they have landed.
Sequence: after the triad (and independent of QRY-5; if both are in
flight, land QRY-5 first — they touch different files).

## Brief (approval surface)

### Problem and root cause

plan_filter Phase B (`find_exact_covering_composite`,
query_plan.c:3457-3472; call site query_plan.c:3731-3748) plans criteria
whose eq leaves are fully covered by a composite index (`a+b` for
`[a eq x, b eq y]`) as `FP_PRIMARY_LEAF` + `FP_ORDER_COMPOSITE_EXACT`,
with `fp.source_leaves` = the covered eq leaves **in composite order** —
fields that may have NO standalone index (the composite directory is the
only index). Only `find` serves this plan (D1 composite-exact branch,
query.c:10433-10460 → `find_via_composite_key` rebuilds the exact
composite key; `fp_compute_total` refuses it → `total:null`,
query.c:9193). Every other consumer is COMPOSITE_EXACT-blind (grep:
zero hits in query_aggregate.c; the count primary-leaf branch has no
order check):

- **count** (`cmd_count_with_tree`, branch condition at query.c:7124):
  `is_single_leaf` is
  false (≥2 leaves), so the else-branch dispatches
  `btree_dispatch_checked(db_root, object, source_leaves[0]->field, ...)`
  on field `a` (7186-7187) → no standalone index → **success with zero
  entries** (OP_EQUAL → `btree_idx_search` → per-shard `bt_acquire`
  failure returns silently, index.c:262-268/btree.c:2320) →
  `parallel_indexed_count` over zero entries → prints `0`.
- **aggregate** (scalar `aw`, IGB, TopN, and `agg_run_plan`'s OWN
  plan_filter call at query_aggregate.c:4018): no COMPOSITE_EXACT
  handling anywhere. `agg_criteria_fully_covered` passes the plan
  (`kind != FP_FULL_SCAN && n_postfilter == 0`), the `aw` gate builds
  `crit_ks` from `source_leaves[0]` only (5451) — with a standalone
  index on `a`: `count(*) = |{a=x}|`, the `b` half silently ignored;
  without: `build_keyset_from_leaf_ck` refuses (CBF_NONE,
  query.c:5211-5212) → falls through (5569) → `agg_run_plan` dispatches
  `source_leaves[0]` again (4023-4063) → `parallel_indexed_agg` opens
  `indexes/a/` → nothing → `0`/`[]`. The IGB (6340-6356) and TopN
  (974-1042) gates share the blindness, and `agg_run_plan` (4018,
  4023-4063) is the terminal consumer of every aggregate fallback
  route.
- **bulk-delete**: safe — `build_keyset_from_leaf_ck` refuses
  the unindexed leaf → NULL → `bulk_delete_phase1_indexed`
  (query.c:9087-9142) falls back to
  the full re-checking scan (`!ks → return 0`, 9101-9104); and even
  with a built keyset `bulk_criteria_indexed_cb` re-verifies the full
  tree (9041-9045). Verified in Task 1 anyway (destructive op, assert
  explicitly).

Reviewer-measured red evidence (AUD-QRY-2 round-3 blind review): fields
a,b,c; indexes `b` and `a+b`; criteria `[a eq 1, b eq 2]` → find 20 rows
correct, `count` → `0`, scalar `aggregate count(*)` → `0` (the group_by
shape → `[]`).

### Logical flow

1. `cmd_count_with_tree`: the primary-leaf branch condition additionally
   excludes `fp.order == FP_ORDER_COMPOSITE_EXACT` → the tree falls to
   the existing `FP_FULL_SCAN` else-branch (full record scan with
   per-record tree re-check → exact).
2. EVERY `plan_filter` call site in query_aggregate.c —
   `cmd_aggregate_do`'s scalar fp, IGB `igb_crit_fp`, TopN `topn_fp`,
   AND `agg_run_plan`'s own `agg_run_fp` call (query_aggregate.c:4018,
   a separate function and the terminal consumer of every aggregate
   fallback route) — normalizes: composite-exact → `FP_FULL_SCAN`, with
   a comment citing AUD-QRY-8. Every downstream consumer (the
   `agg_criteria_fully_covered` gates, `can_slot`, the TopN keyset
   builders, `agg_run_plan`) already routes `FP_FULL_SCAN` to the
   record-scan executor — the campaign's differential oracle. Task 0
   enumerates all sites before any edit.
3. `agg_criteria_fully_covered` gains the explicit term
   `&& fp.order != FP_ORDER_COMPOSITE_EXACT` (defense-in-depth, mirroring
   the APPROVED QRY-2 `can_slot` precedent: a gate that reads the plan
   directly must not silently relax if callers change).
4. `find` untouched (D1 branch stays; `fp_compute_total`'s refusal stays
   — `total:null` there is the documented can't-compute-cheaply
   behavior, and remains acceptable after this fix).
5. `plan_filter` itself untouched (the composite-exact plan is the
   planner's truth and `cmd_explain` prints it; find depends on it).

### Decisions made

| decision | alternatives | why | tag |
|---|---|---|---|
| Refuse-first: normalize composite-exact to `FP_FULL_SCAN` at aggregate's plan sites + guard count's primary-leaf branch; fall back to tree-rechecking executors | (b) teach the aggregate/count executors to drive the composite exact key (build the exact key per group / use the composite for candidates); (c) intersect the covered leaves normally | (a) (this option) is the audit-safe minimal fix: one predicate + one-line normalizations, routes to executors that are already the oracle, zero new planning surface; (b) is a real perf feature (composite-exact aggregates could be O(matched keys)) but needs its own oracle coverage and planning work; (c) duplicates the exact-key logic Phase B deliberately owns | DEFAULT |
| Normalize at the aggregate call sites, not inside `plan_filter` | plan_filter refuses the plan entirely | find consumes this plan correctly and explain must print the planner's truth; refusing at the source would regress find's fastest path | DEFAULT |
| Bulk-delete: assert-only (no code change) unless Task 1 proves it unsafe | fix it preemptively | code-read says safe-by-accident (CBF_NONE NULL → scan fallback); a destructive-op assertion belongs in the suite regardless; if the assertion is RED on base, extend the normalization to `bulk_delete_phase1_indexed`'s plan in the SAME branch (still one concern — composite-exact consumers) and record it in PLAN_NOTES | DEFAULT |

### Blast radius

- Consumers: `cmd_count_with_tree`, `cmd_aggregate_do` + `agg_run_plan`
  (+ TopN/IGB plan sites), `agg_criteria_fully_covered`. Queries
  affected: composite-exact-planned count/aggregate — today silently
  wrong, after the fix correct (record scan).
- **Perf (stated honestly, two classes):** (i) composite-ONLY objects
  (covered field has no standalone index): today the count/aggregate
  routes return wrong answers via a broken/empty walk — after the fix
  they scan, so a real slowdown buys correctness. (ii) composite +
  standalone-indexed objects (the `ce_mix` class): COUNT is correct
  today (per-record re-check over real candidates, query.c:1216) and
  becomes a full scan — a genuine perf regression on a correct path,
  accepted here for the audit-safe uniform rule; the leaf-executor
  AGGREGATES on the same class are WRONG today (the `aw` fast path
  reads `count(*) = |KeySet|` with no re-check, query_aggregate.c:5554-5557)
  and become correct scans. The composite-exact aggregate executor is
  the perf follow-up that restores the fast path.
  `bench-queries` does not use composite-exact aggregate/count shapes
  (verify at execution; if one does, paste before/after).
- find: NO change (its D1 path, totals and cursor behavior untouched).
- No wire/protocol/on-disk change.

### Test strategy

- New product regression case `test-composite-exact-count-agg` (Task 1):
  three objects (no-index oracle; composite-only twin
  `["b","a+b"]`; composite+standalone twin `["a","b","a+b"]`), asserting
  count/aggregate/group_by/TopN equality against the oracle and find
  controls, plus the bulk-delete destructive-op assertion on a fourth
  twin.
- Campaign differential spot-check: the diff's composite shape is
  all-scan today (AUD-PLN-3 — its `u+v` object has no standalone
  indexes, so `prim < 0` → FULL_SCAN and Phase B never fires) → no
  differential delta expected; record the DIVSEC line as the unchanged
  baseline (Task 4).
- Full suite `run-all --jobs 8` + sanitizer gate once each (executor
  routing, no locks/lifetimes → single pass per AGENTS).

### Out of scope

- Teaching the composite-exact plan to aggregate/count executors (perf
  follow-up, own plan if wanted).
- The `total:null` on composite-exact find (documented behavior;
  changing it = giving `fp_compute_total` a composite path — perf
  follow-up territory).
- AUD-QRY-2's `has_subtree` work (landed separately; orthogonal —
  composite-exact has no sub-trees).
- AUD-PLN-3 (composite-only objects never plan composite paths at all —
  planner-quality gap, separate).

### Review log

- Round 2 (blind re-review, 2026-11-20): **0 blockers, 0 majors, 4
  minors, all folded in** — blast-radius class (ii) split honestly
  (ce_mix COUNT correct-today → scan regression; leaf-executor
  aggregates WRONG-today via the no-recheck `|KeySet|` read at
  5554-5557 → correctness fix); assertion 5's tie situation documented
  (16-way tie at count 2 under `(i*i)%97`; equality rests on identical
  post-fix scan executors, not distinct counts); scalar-site variable
  named `aw_fp` (5444); anchors corrected (7124; `igb_skip` label at
  7006, 7022 is the igb_done→agg_run_plan dispatch). Reviewer
  re-verified all four plan_filter sites (970/4018/5444/6011), the
  post-fix placements (before the FULL_SCAN checks at 6015/974/5450),
  the IGB NULL-crit_ks decline gate (~6406), bulk-delete
  double-safety, and that adding the standalone `c` index changes no
  assertion's red/green. **CONVERGED — ready for approval.**
- Round 1 (blind reviewer, 2026-11-20): **0 blockers, 2 majors, 6
  minors, all folded in.** M1: Task 4's revert stash list omitted
  `src/db/query_aggregate.c` — with the aggregate-site normalizations
  left in place the promised reds cannot reproduce (fixed: all three
  fix files stashed + PLAN_NOTES note about stacked uncommitted
  QRY-1/2 hunks). M2: assertions 4-5 could not exercise the IGB/TopN
  normalization sites because the group field `c` had no standalone
  index (`eligible_for_topn_stream` requires a btree group index,
  query_aggregate.c:673; IGB eligibility 5942-5951) — fixed: standalone
  `c` btree added to ce_comp/ce_mix/ce_del (reviewer verified safe:
  Phase B's fill and count's `pc` stay keyed to composite order).
  Minors: anchors corrected (branch condition 7124, dispatch 7186-7187,
  bulk_delete span 9087-9142); `agg_run_plan`'s own plan_filter call
  (4018) named as the fourth mandatory site; blast-radius perf split
  into the two honest classes (composite-ONLY = wrong-today,
  composite+standalone = correct-today-but-regressed); assertion 5's
  uniform-tie hazard removed via the skewed `c = (i*i)%97` mapping;
  scalar count(*) → `0` wording. Reviewer independently verified: the
  count=0 route end-to-end (btree_dispatch_checked succeeds with zero
  entries via silent bt_acquire failure), bulk-delete double-safety
  (CBF_NONE fallback AND full-tree re-verification in
  bulk_criteria_indexed_cb), row algebra (50 / 100), TopN request
  precedent, and gate compliance.

---

## Plan (executor-facing)

### Ground rules

Branch base per the header; the base MUST contain `src/test/audit/` for
the Task 4 differential spot-check, else halt per PLAN_NOTES. Work
uncommitted (repo standing exception). Path-limited stashes only
(`git stash push -- <fix files>`), never a bare `git stash`. Build
`SKIP_TESTS=1 ./build.sh`; suite `run-all --jobs 8`; `PLAN_NOTES.md` +
halt on mismatch. One heavy job at a time; `/tmp/shard-db-*` removed
after every run.

### Task 0 — enumerate the consumers (code-read, cheap; halt-on-mismatch)

1. `grep -n "plan_filter" src/db/query_aggregate.c` — list EVERY call
   site (expected FOUR: `cmd_aggregate_do`'s scalar fp, the IGB
   `igb_crit_fp`, the TopN `topn_fp`, and `agg_run_plan`'s
   `agg_run_fp` at query_aggregate.c:4018 — the last is in a DIFFERENT
   function and is the terminal consumer of every aggregate fallback
   route: aw fall-through 5569, igb_skip 7022, TopN -2 974-977/1070-1074;
   it MUST get the normalization or every aggregate assertion stays red
   post-fix). Any further site gets the same treatment — record it.
2. `grep -rn "FP_ORDER_COMPOSITE_EXACT" src/db/` — confirm the full
   consumer list matches the Brief (find D1 query.c:10433-10457 +
   `fp_compute_total` 9193 + both explain printers query_plan.c:2747 /
   query.c:6650; nothing in query_aggregate.c). Any NEW consumer found
   gets a one-line note in the branch notes (fix or verified-NA with
   reason).
3. Confirm `cmd_count_with_tree`'s FP_PRIMARY_LEAF branch condition at
   query.c:7124 (function starts 7094) matches the Brief's quote, the
   dispatch at 7186-7187, and `bulk_delete_phase1_indexed`'s span
   9087-9142.

### Task 1 — regression test (test-first; expect RED on base)

New file `src/test/cases/test_composite_exact_count_agg.c`, registered
`TEST_REGISTER("test-composite-exact-count-agg", ...)`, wired into
`build.sh` next to the aggregate cases. Fixture helpers copied from
`test_aggregate_type_matrix.c`. Schema: fields `a:int`, `b:int`,
`c:int`; 300 rows `k%04d`, `a = i%3`, `b = i%2`, `c = (i*i)%97` (so
`[a eq 1, b eq 0]` matches exactly 50 rows, and the matched rows' `c`
distribution is SKEWED — `c = i%5` would give uniform ties among the
i≡4 (mod 6) rows and make the TopN ordering ambiguous; the case still
asserts against the oracle twin, not constants). Objects:

- `ce_plain` — no indexes (oracle);
- `ce_comp` — `indexes: ["b","a+b","c"]` (the reviewer repro shape
  plus a standalone btree on the GROUP field `c`: without it the IGB
  and TopN fast paths decline — `eligible_for_topn_stream` requires a
  btree on the group field, query_aggregate.c:673; IGB requires a
  btree/bitmap group index, 5942-5951 — and assertions 4-5 would not
  exercise the sites this fix normalizes. Verified safe: Phase B's
  composite fill (query_plan.c:3743-3747) and count's `pc` stay keyed
  to composite order, i.e. leaf `a`; adding `c` changes neither);
- `ce_mix` — `indexes: ["a","b","a+b","c"]` (aggregate-wrong-only
  shape);
- `ce_del` — same schema/indexes as `ce_comp` (bulk-delete twin).

Assertions (1-5 compare against `ce_plain` responses — per-label
extraction for group_by, never raw-string compare):

1. `count` `[a eq 1, b eq 0]` on `ce_comp` == oracle. RED today (0).
2. `aggregate count(*)` same criteria on `ce_comp` == oracle. RED today
   (`0`).
3. Same on `ce_mix` == oracle. RED today (100 = all `a=1`, `b`
   ignored — proves the standalone-index variant is broken too).
4. `group_by=c` same criteria on `ce_comp` == oracle vector. RED today.
5. TopN shape: `group_by=c`, `order_by:"<count alias>"`,
   `limit:3`, same criteria on `ce_comp` — emitted labels AND counts
   equal the oracle's top-3 under the same request. RED today.
   NOTE: the `(i*i)%97` mapping over the matched rows (i ≡ 4 mod 6)
   still yields tied top counts (16 values at count 2) — the assertion
   holds because post-fix BOTH sides run the identical scan executor
   over identical kf layouts, so group-table iteration and tie
   resolution match; label equality rests on that determinism, not on
   distinct counts (verified by the round-2 review).
6. `find` same criteria on `ce_comp` == oracle rows (key-set +
   per-record field equality). GREEN today (D1 path) — control.
7. `explain` find same criteria on `ce_comp` contains
   `"composite_exact"`. GREEN today — control pinning that the fix must
   NOT disturb find's plan.
8. `bulk-delete` `[a eq 1, b eq 0]` on `ce_del` → response count ==
   50; post-delete `count` on `ce_del` == oracle's post-delete count
   (computed from the case's own model). Expected GREEN today
   (safe-by-accident); if RED, stop — record in PLAN_NOTES and apply
   the Decision-3 extension.
9. Single-leaf control: `count` `[b eq 1]` on `ce_comp` == oracle
   (the normal leaf path must stay fast and correct).

Test-first proof: build on the branch base, run the case — 1-5 RED for
the expected reasons (0 vs oracle; 100 vs 50 on ce_mix), 6-9 GREEN.
Paste.

### Task 2 — the fix (count)

`cmd_count_with_tree` (branch condition at query.c:7124): change the
primary-leaf branch
condition to exclude the composite-exact plan, e.g.

```c
if ((fp.kind == FP_PRIMARY_LEAF || fp.kind == FP_BITMAP_SMALLER) &&
    !(fp.kind == FP_PRIMARY_LEAF &&
      fp.order == FP_ORDER_COMPOSITE_EXACT)) {
```

with the comment: `/* AUD-QRY-8: the composite-exact plan's source
leaves are composite-covered fields with no standalone index — the leaf
dispatch below would collect nothing and print 0. Fall to the scan
executor, which re-checks the full tree per record. (find serves this
plan via find_via_composite_key; count has no composite-exact
executor.) */`

### Task 3 — the fix (aggregate)

At EACH plan_filter call site in `src/db/query_aggregate.c` (Task 0
list: `cmd_aggregate_do`'s scalar fp, `igb_crit_fp`, `topn_fp`, AND
`agg_run_plan`'s `agg_run_fp` at 4018), immediately after the call:

```c
if (fp.order == FP_ORDER_COMPOSITE_EXACT)
    fp.kind = FP_FULL_SCAN;   /* AUD-QRY-8: no aggregate consumer can
    serve the composite-exact plan (its source leaves may have no
    standalone index); the record-scan executor re-checks the full
    tree. find consumes this plan via find_via_composite_key — the
    planner output itself is unchanged. */
```

(variable names per site: `aw_fp` / `igb_crit_fp` / `topn_fp` /
`agg_run_fp`). Then
`agg_criteria_fully_covered` (query_plan.c:~4032) gains
`&& fp.order != FP_ORDER_COMPOSITE_EXACT` with a one-line AUD-QRY-8
comment (defense-in-depth). Update the Phase-B comment in `plan_filter`
(query_plan.c:3728-3730) to note that only `find` consumes the plan it
returns.

### Task 4 — prove and gate

1. Revert-prove-paste: `git stash push -- src/db/query.c
   src/db/query_plan.c src/db/query_aggregate.c` (ALL THREE fix files —
   stashing only query.c/query_plan.c would leave the aggregate-site
   normalizations in place and the reds would not reproduce) → rebuild
   → Task 1 reds (1-5) → `git stash pop` → rebuild → greens. If the
   aggregate normalization and the count guard are proven separately,
   two stash rounds are acceptable — paste each. (Never a bare stash.
   NOTE for PLAN_NOTES: if QRY-1/QRY-2 are still uncommitted on the
   base, whole-file stashes also lift those branches' hunks — the
   red/green direction is unaffected but the reverted state is
   base-minus-those-fixes; record that in the prove notes.)
2. Targeted: `./build/bin/shard-db-test run
   test-composite-exact-count-agg`.
3. Full suite `run-all --jobs 8`, fresh, all green (AUD-CI-1 flakes
   aside — re-run a fired case solo).
4. Differential spot-check (`SHARD_AUDIT_DIFF_ROWS=5000
   SHARD_AUDIT_DIFF_CRITERIA=200 ... --filter audit-planner-diff`) —
   no delta vs the post-QRY-2 baseline (composite twin is all-scan);
   record DIVSEC.
5. Sanitizer gate once each (asan `--jobs 6`, tsan `--jobs 3`); rebuild
   normal afterwards.
6. Changelog convention check + entry if per-fix.
7. Index: AUD-QRY-8 row → `fix-branch` with this branch name; note the
   Task 0 consumer enumeration result (any new consumer found).

### Embedded execution rules

Tasks in order; both revert outputs pasted; no unrelated changes;
`PLAN_NOTES.md` + halt on mismatch (including Task 0's enumeration and
the Task 1 assertion-8 red surprise).
