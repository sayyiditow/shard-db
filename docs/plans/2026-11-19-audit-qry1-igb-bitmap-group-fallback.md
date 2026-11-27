# fix/audit-igb-bitmap-group-fallback — AUD-QRY-1

Campaign: `docs/plans/2026-11-19-audit-campaign.md` Phase 4 (first DL branch).
Finding: `AUD-QRY-1` in `docs/plans/2026-11-19-audit-index.md` (DL: silent
wrong results in the aggregate group_by mode). Branch name:
`fix/audit-igb-bitmap-group-fallback` — branch base per the Ground rules
below (the campaign harness working tree; `main` once the harness branch
has landed there).

## Brief (approval surface)

### Problem and root cause

`aggregate` with `group_by` over a field indexed **bitmap-only** returns
silently empty groups (`[]`) whenever the criteria are present and not
"slot-eligible" (i.e. the criteria plan is not all-bitmap-eq with no
postfilter) — while `count` and `find` over the same criteria are correct on
the same object, and a plain twin agrees. Root cause: the indexed group-by
(IGB) fast path admits bitmap-only group fields
(`query_aggregate.c`, IGB eligibility block: `!has_btree_g &&
field_has_index_type(..., IT_BITMAP)` → eligible, `igb_group_uses_bitmap = 1`);
its count-only bitmap branch handles exactly two shapes (no criteria;
bitmap-eq `can_slot`), and everything else "falls through to btree/scan path" —
but the fall-through walks the group field's **btree**
(`igb_pass1_worker` opens `build_idx_path(..., gfield, shard_id)` and
`btree_range_iter_open` fails on the missing files), which does not exist on a
bitmap-only field → zero Pass-1 entries → zero buckets → `[]` with no error.

### Logical flow

1. Request: `aggregate` + `group_by=v` + criteria whose plan seeds from a
   btree leaf, an OR sub-tree, or carries a postfilter — on an object where
   `v` has a bitmap index and no btree.
2. IGB eligibility passes (bitmap-only accepted).
3. Criteria plan is not slot-eligible → the bitmap branch falls through.
4. Today: the fall-through walks `indexes/v/NNN.idx` (absent) → empty groups.
5. After the fix: the same fall-through is recognized as "bitmap-only group
   field that this path cannot serve" and routes to the existing per-record
   scan executor, which is the differential oracle (always correct).

### Decisions made

| decision | alternatives | why | tag |
|---|---|---|---|
| Non-slot-eligible + bitmap-only group field routes to the per-record scan path (`goto igb_skip`) | (b) KeySet-filtered bitmap dict walk (bucket per dict value, slot hash ∈ crit_ks); (c) build a btree index on the group field on the fly | (a) is the minimal correct fix: the scan path is the differential oracle, exists, and is exercised by every other shape; (b) is a worthwhile perf follow-up but adds a new bucket-walk shape that itself needs oracle coverage first; (c) is a schema-level change | DEFAULT |
| Fix point = one guard before the btree Pass-1 block (`if (igb_group_uses_bitmap) goto igb_skip;`) rather than edits inside each branch | per-branch checks | by the time control reaches the btree Pass-1 block, every bitmap-only shape that *can* be served (no-criteria popcount, no-criteria hbm via Phase 1b, slot-eligible count-only) has already finished *for the shapes it serves* (note: those served branches have their own sibling defects, AUD-QRY-5/-6 below, out of scope here); anything bitmap-only still reaching this line is unserviceable | DEFAULT |
| No behavior change for btree-indexed or unindexed group fields | — | unindexed group fields already fail IGB eligibility; btree group fields walk a real btree | DEFAULT |

### Blast radius

- Consumers: `cmd_aggregate_do` only (the IGB block and its `igb_skip` label
  are local to that function). The guard fires only when the group field is
  bitmap-only AND criteria are present AND the plan was not slot-eligible —
  the set of queries that today return silently-empty groups.
- No wire/protocol change: the same response shape, now with correct groups.
- Perf: affected queries get slower (record scan instead of an index walk
  that returned garbage). Bench impact confined to the bitmap-group-by +
  criteria shape; `bench-queries` aggregate benches do not use bitmap-only
  group fields (verify at execution; if one does, paste before/after).
- No on-disk format change.

### Test strategy

- New product regression case `test_igb_bitmap_group_fallback` (see
  Task 1): four assertions — btree-seeded criteria, sum-spec variant,
  slot-eligible control, no-criteria control — each compared against a
  same-data no-index oracle object. (OR-subtree criteria are deliberately
  NOT here: that shape's wrong answers belong to AUD-QRY-2's branch.)
- The campaign planner differential (`audit-planner-diff`, bitmap twin) is the
  wide net: its group_by section must drop to zero divergences attributable to
  the empty-group signature after this fix (AUD-QRY-2 divergences remain until
  that branch lands).
- Full suite `run-all --jobs 8` (mandatory); sanitizer gate once (executor
  routing change — no locks/lifetimes touched → single pass per AGENTS).

### Out of scope

- The KeySet-filtered bitmap walk (perf follow-up; its own plan if wanted).
- AUD-QRY-2 (OR sub-trees) and AUD-QRY-3 (counter underflow) — separate
  branches, one concern each. Tests here must not assert on criteria shapes
  those branches change (OR-bearing group_by results change again when
  QRY-2 lands; the QRY-1 test avoids OR criteria except as a
  not-slot-eligible driver where the oracle comparison is what's asserted —
  actually keep the OR-subtree variant OUT of this branch's test entirely;
  the unindexed-sibling variant covers the same routing).

- AUD-QRY-5 (DL class, sibling, separate branch): the bitmap IGB branches
  (`bm_collect_uniq_cb`, `bm_vals[256]`) silently drop groups when a bitmap
  field has more than 256 distinct values (`field:bitmap(N)` allows up to
  65535), on the no-criteria popcount, `can_slot` and Phase 1b paths.
- AUD-QRY-6 (memory safety, sibling, separate branch): the same callback
  copies `min(vlen,1024)` bytes but records the full `vlen`;
  `bm_popcount_one_value` / `bm_walk` / `agg_index_label` then read `vlen`
  bytes from a 1024-byte row (OOB read, wrong labels) for bitmap values up
  to 0xffff bytes.
- AUD-QRY-7 (minor/robustness, sibling): `igb_pass1_worker` treats any
  `btree_range_iter_open` failure as an empty shard (log only, no
  `aborted`/`agg_error`), so an I/O failure on a genuine btree group field
  also drops that shard's groups silently. Fail-closed finding.
  These three are to be written into the audit index and planned after
  QRY-1/2/3; they do not change this plan.

### Review log

- Round 1 (blind reviewer, 2026-11-19): 1 blocker — branch base
  contradicted the mandatory differential check (base must contain the
  audit tooling; fixed: base = the harness working tree, conditional on
  main); 1 major — assertion 2's `gb_bm` fixture lacked the `u` btree, so
  the sum-spec case would not be red (fixed: `["v:bitmap","t","u"]`);
  minors — guard placement ambiguity (fixed: single placement, before the
  KeySet build), case-name mismatch (unified), Brief assertion list
  mismatch (OR variant removed), differential-attribution phrasing
  (shape-pair tally added), category strings pinned, sanitizer wording,
  build.sh anchor.
- Round 2 (blind re-review, 2026-11-19): 0 blockers, 0 majors; 4 minors
  fixed (stale Brief branch sentence, divergence count misquote
  148+144=292, per-label extraction for the sum assertion instead of raw
  compare, expected-delta phrasing for the differential spot-check).
  CONVERGED — ready for approval.
- Round 3 (independent blind review, 2026-11-19): 0 blockers, 2 majors, 6
  minors, all folded in: M1 bare `git stash` would remove the uncommitted
  harness (fixed: path-limited stash); M2 sibling defects in the same
  bitmap block recorded as AUD-QRY-5/-6/-7 and Decision 2 qualified;
  minors: case-name, fixture helper, response parsing, stale comments,
  divergence-number wording, miss-prediction halt rule, changelog check.

---

## Plan (executor-facing)

### Ground rules

Branch base: the campaign harness working tree (`test/audit-oracle-harnesses`,
the only tree that contains `src/test/audit/` and the `--include-audit`
tooling this plan's verification tasks use) — create the fix branch from it.
If the harness branch has been committed to `main` by execution time, branch
off `main` instead; either way the base MUST contain the audit tooling, or
Task 3's differential spot-check is unexecutable (halt per PLAN_NOTES
instead of improvising). Work uncommitted (repo standing exception) until
the human reviews the raw diff. Build `SKIP_TESTS=1 ./build.sh`; suite
`./build/bin/shard-db-test run-all --jobs 8`; single case
`./build/bin/shard-db-test run <name>`. If any assumption below doesn't
match the code, write `PLAN_NOTES.md` and halt. `/tmp/shard-db-*` removed
after every run.

### Background: the defect path, precisely

In `cmd_aggregate_do` (src/db/query_aggregate.c), the IGB fast path:

1. Eligibility accepts a group field whose only index is a bitmap
   (`has_btree_g == 0 && field_has_index_type(..., IT_BITMAP)`),
   setting `igb_group_uses_bitmap = 1`.
2. With `tree` present, the criteria plan runs (`igb_crit_fp`); only
   `FP_FULL_SCAN` disqualifies.
3. The count-only bitmap branch serves two shapes: no criteria (dict
   popcounts) and `can_slot` (plan sources all bitmap **eq**, no
   postfilter). Everything else falls through — the code comment says
   "fall through to btree/scan path".
4. The `igb_group_uses_bitmap && igb_needs_hbm && !tree` (Phase 1b) branch
   also requires no criteria.
5. Control then reaches the btree IGB block: criteria KeySet built from the
   plan sources, then Pass 1 walks the group field's **btree** —
   `igb_pass1_worker` opens `build_idx_path(..., group_fields[0],
   shard_id)` and `btree_range_iter_open` returns NULL (bitmap-only field →
   no `.idx` files) → the worker returns early → zero Pass-1 entries →
   zero buckets → the aggregate emits `[]`.

Red evidence on `audit-base` (f19b17d), already captured:
`docs/plans/audit-artifacts/AUD-QRY-1-repro.out` — 4-shape differential:
bitmap twin returns `[]` for t-range and OR criteria where the plain twin
returns the correct groups, while count and find agree everywhere. Harness
signature: planner-diff group_by section, `pd_plain vs pd_bitmap` (292 divergences in total across the two full-budget seeds, 148 + 144,
`AUD-QRY-1`-attributed).

### Task 1 — regression test (test-first; expect RED on audit-base)

New file `src/test/cases/test_igb_bitmap_group_fallback.c`, registered with
`TEST_REGISTER` (a normal suite case — this is a fixed-behavior regression,
not an audit fuzz case). Wire it into `build.sh`'s shard-db-test link list next to the aggregate
cases (anchor: the `src/test/cases/test_aggregate_type_matrix.c \` line —
insert before or after it).

Fixture (in-case): daemon via `test_env_start(&env)` / `test_env_stop` (not `test_env_start_at`, the crash-test helper that makes the caller own root, port and cleanup); case registered as `TEST_REGISTER("test-igb-bitmap-group-fallback", ...)` (hyphenated, matching existing cases); parse per-label count/sum with the helper used in `src/test/cases/test_aggregate_type_matrix.c`; two objects with
identical fields `v:varchar:48` (values exactly `cat_0`..`cat_3`,
`i % 4`), `t:long` (=i), `u:int` (`i % 13`), 400 rows (`k00000`..) via
bulk-insert:
- `gb_plain` — no indexes (oracle);
- `gb_bm` — bitmap-only GROUP field plus btrees for the criteria/agg
  leaves: `indexes: ["v:bitmap","t","u"]`. (`u` MUST be btree-indexed:
  the IGB sum-spec eligibility requires the sum field's btree to exist —
  without it the query routes to the record-scan executor and returns
  correct groups today, so assertion 2 would not be red. `v` stays
  bitmap-only — that is the defect's precondition.)

Assertions (each: group-by count-per-label vector extracted from the
response, compared between `gb_plain` and `gb_bm`):

1. `criteria: [{"field":"t","op":"between","value":50,"value2":149}]`
   (btree-seeded, selective — today: `gb_bm` returns `[]`, oracle returns
   groups) — the core red.
2. Same criteria with `aggregates: [{"fn":"count","alias":"n"},{"fn":"sum",
   "field":"u","alias":"s"}]` (sum spec on an indexed numeric field —
   exercises the `igb_needs_hbm` + criteria shape) — extract the
   per-label count/sum values and compare (aggregate responses without
   `order_by` emit in hash-collection order — never raw-string compare
   the two objects' responses).
3. Slot-eligible control: `criteria: [{"field":"v","op":"eq",
   "value":"cat_1"}]` — must stay correct (it already is; guards against
   over-fixing).
4. No-criteria control: group_by with no criteria — must stay correct
   (popcount path).

Test-first proof: build on `audit-base` state (this branch's base), run the
case — assertions 1 and 2 must FAIL with `gb_bm` returning `[]` (expected
reason: empty bucket set from the missing-btree walk); assertions 3 and 4
must pass. Paste that output into the branch notes (revert-prove-paste:
after the fix, revert ONLY the fix file with `git stash push -- src/db/query_aggregate.c`, re-run, confirm the same red, then `git stash pop`. Never a bare `git stash`: the base is the uncommitted harness tree and a bare stash would remove the harness, the build.sh wiring and the runner changes).

### Task 2 — the fix

In `cmd_aggregate_do`, at the top of the btree IGB block — immediately
after the declarations of `gtf` and `n_idx_g` and BEFORE the criteria
KeySet build and the `hbk` init (anchor: the declarations directly under
the Phase 1b block's closing brace). At this point `crit_ks == NULL` and
`hbk_ready == 0`, so the frees in the snippet are no-ops kept only for
robustness against future reordering. Do NOT place the guard after the
KeySet build. Insert:

```c
if (igb_group_uses_bitmap) {
    /* Bitmap-only group field: every servable shape (no-criteria
       popcount, no-criteria hbm, slot-eligible count-only) has been
       handled above. Criteria remain and the plan was not slot-eligible
       — the btree Pass-1 walk below would read indexes/<field>/NNN.idx,
       which does not exist for a bitmap-only field, and silently emit
       zero groups (AUD-QRY-1). Route to the per-record scan path. */
    if (crit_ks) keyset_free(crit_ks);
    if (hbk_ready) hbk_free(&hbk);
    goto igb_skip;
}
```

Notes for the executor:

- The snippet's `crit_ks` free is a no-op at this placement (never built
  yet); keep it as defensive cleanup or drop it — either is correct, state
  which in the commit body.
- `igb_skip` frees nothing itself — every existing reach point frees its
  own locals first; with the guard before the KeySet build there is
  nothing new to free.
- `sec_maps` is always NULL when the guard fires (multi-field bitmap-only
  primaries are disqualified at the `igb_eligible` computation), so no
  secondary-map cleanup is needed.
- Do not touch the Phase 1b / can_slot branches.

### Task 3 — prove and gate

1. Revert-prove-paste: with the test from Task 1 present, `git stash push --
   src/db/query_aggregate.c` (the fix only; never a bare `git stash`) → rebuild → run the case → expect exactly the Task-1 red; `git stash
   pop` → rebuild → re-run → all four assertions green. Paste both outputs.
2. Targeted: `./build/bin/shard-db-test run test-igb-bitmap-group-fallback`.
3. Full suite: `run-all --jobs 8` — fresh, all green (AUD-CI-1 load flakes
   aside; if one fires, re-run that case solo per the index note).
4. Campaign differential spot-check: `SHARD_AUDIT_DIFF_ROWS=5000
   SHARD_AUDIT_DIFF_CRITERIA=200 ./build/bin/shard-db-test run-all
   --include-audit --filter audit-planner-diff` — after this fix the
   `pd_plain vs pd_bitmap` group_by signature (empty groups) must be gone
   for non-OR criteria; OR-bearing group_by divergences on the
   pd_plain-vs-pd_btree pair remain (AUD-QRY-2, next branch). Expected
   end state: the bitmap pair drops to zero for ALL criteria (OR-bearing
   bitmap-twin group_by also routes to scan post-fix and matches
   pd_plain); only the btree pair keeps OR divergences. Attribute
   divergences from the per-criterion `DIVERGENCE` log lines' shape pairs
   (the DIVSEC counter alone cannot separate pairs). Record the DIVSEC
   line plus the shape-pair tally. If the bitmap pair does NOT drop to zero,
   do not proceed: record the residual signature in `PLAN_NOTES.md` and halt.
5. Sanitizer gate: `BUILD_MODE=asan` → `run-all --jobs 6` once;
   `BUILD_MODE=tsan` → `run-all --jobs 3` once. The change is
   executor routing plus cleanup-path frees — if the final diff ends up
   touching kf/lifetime handling beyond the goto (it should not), run the
   three-consecutive-passes gate per AGENTS instead. Rebuild normal
   afterwards.
6. Update the stale code comments in the same diff: the IGB eligibility comment ("bitmap-only supports single field, count-only, no criteria", near the `igb_group_uses_bitmap` setup) and the "fall through to btree/scan path" comment after the count-only bitmap branch must describe the new guard. Check whether `docs/reference/changelog.md` expects an entry for this fix (documentation-sync rule) and add one if the repo's convention is per-fix entries. The branch's raw `git diff` also contains the uncommitted harness tree; human review should use `git diff -- src/db/query_aggregate.c src/test/cases/test_igb_bitmap_group_fallback.c build.sh`.
7. Update `docs/plans/2026-11-19-audit-index.md`: AUD-QRY-1 row →
   `fix-branch` status with this branch name; paste the DIVSEC delta into
   the row's disposition note.

### Embedded execution rules

Tasks in order; test-first per CORE-PROCESS with both revert outputs pasted;
no unrelated changes; `PLAN_NOTES.md` + halt on any mismatch between this
plan and the code (symbol moved ≠ halt; record and continue only when the
intended site is unambiguous).
