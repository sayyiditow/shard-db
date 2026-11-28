# Audit campaign — master findings index

Campaign: `docs/plans/2026-11-19-audit-campaign.md` (v3, approved 2026-11-19).
Threat model: `docs/plans/2026-11-19-audit-threat-model.md` (provisional draft,
awaiting approval).

**`audit-base` = `f19b17d61e01d4828331aef696cd08bdd97b2210`** (= merge commit
`f19b17d`, tag `2026.11.2`). Confirmed against `main` HEAD at Phase 0,
2026-11-19. Recorded here per the plan; **no git tag created** (human runs all
git writes).

Severity: **DL** (data loss/corruption — plan Priority; fixed first, never
deferred) > **blocker** > **major** > **minor**. Every blocker/major/DL carries
a repro or a written proof trace; unreproduced items are `suspected` and do not
block closure. Auth/path/parsing severity is judged against the threat model.

Status values: `open` / `suspected` / `fix-branch` / `deferred` / `wontfix`.

## Findings

| id | area | severity | checklist items hit | repro | status | fix-branch | disposition note |
|---|---|---|---|---|---|---|---|
| AUD-CI-1 | ci / test-infra | major (test-only) | n/a | CI run 43944867415 log (2026-11-16, commit 163b36a): `test-rebuild-validation` exceeded the 1200 s watchdog → `run-all` aborted, exit 124, ASan job failed. Handover evidence: `test-rebuild-txn-*` "rebuild reaches the (deterministic/post-index) pause" — fixed 20 s `wait_for_path` over ~200 fsyncs before the marker (~1.2 s local tmpfs; far slower on CI disks, macOS full-fsync, ASan/TSan). Pre-dates 2026.11.2 (also failed at 163b36a). Disproven: not the SEEK_DATA probe, not port reuse, not reproducible locally at `--jobs 8/12`. **Local add (2026-11-19, `--jobs 8` suite after tool wiring): `test-bulk-stage-ungrown-backoff` hit the same family — bounded-wait retry timed out (`got -1`) under all-core load, passes solo (61/61), cascading count failures only.** Same Phase-4 branch. | yes (CI log + local suite log) | open | fix/audit-rebuild-pause-waits (Phase-4 first branch) | Fix: scale the deadline for load/sanitizer/macOS; print daemon log tail on *every* pause miss (today only the splits case does). Affected so far: `test_rebuild_txn_recovery.c`, `test_rebuild_recovery.c`, `test_rebuild_validation.c` (watchdog hit), `test_bulk_stage_ungrown.c` (back-off wait). Root cause and disproven alternatives per handover §Seed findings. |
| AUD-PLAT-1 | platform / caps | major | Phase 2.4 (caps), checklist 12 (platform) | Expected: `config: sem_init failed: Function not implemented - slot cap disabled` in macOS CI logs (config.c) → `MAX_CONCURRENT_QUERIES` silently off on macOS (no unnamed semaphores). Verify on a macOS CI log / locally in Phase 1 before it counts as major. | trace (log line; verification pending) | suspected | — | Handover seed. macOS-only; consequence: documented per-query concurrency cap silently disabled → memory-containment math (`MAX_CONCURRENT_QUERIES × QUERY_BUFFER_MB`) invalid on macOS. |
| AUD-PLAT-2 | platform / io | minor | checklist 5 (resource lifetimes) | Observed via `strace` during planning: multiple threads can each create an 8 MiB `.sdprobe-*` file concurrently before the probe cache is populated. Benign cost; record only. | trace (strace) | open | — | Handover seed. No action unless a slice shows it delays startup or exhausts /tmp. |
| AUD-HARN-1 | harness quirk (phase1-incidental) | note (suspected) | n/a | Records committed via raw `slotcask_insert_with_hooks` + `slotcask_close` in a test process are **not visible to a subsequently started daemon** on the same root (probe readback: 0/3 readable at the first restart; repeated). No product write path mixes a raw-slotcask writer with a daemon on one root; `slotcask_validate_live_refs` is unaffected. Recorded so no future harness assumes cross-process visibility of out-of-band slotcask writes. | repro: `audit-crash-restart` probe telemetry (since removed from the harness) | suspected | — | Needs a repro through PRODUCT paths before it can count above note severity; likely documented-behavior territory (embedded/open lifecycle), Phase 2.1 can confirm. |
| AUD-GATE-1 | gate evidence: 163b36a daemon SIGBUS at open | evidence (base-only; expected on the known-bug parent) | checklist 6 (crash safety) | On `163b36a` (kf grave-frontier bug parent), the crash/restart fuzzer's kill pattern produces roots whose open crashes the **base daemon** with SIGBUS (BUS_ADRERR — access beyond a mapped file extent): 7 daemon cores recorded, stack symbolized to `slotcask_pool_rebuild_worker` ← `parallel_for_io` (parallel.c:390) ← `slotcask_open` (slotcask.c:5089) ← `slotcask_registry_get` (slotcask.c:10222/10168). Evidence: `docs/plans/audit-artifacts/gateA-163b36a-sigbus-core366691.txt`, `gateA-163b36a-sigbus-run.log`. The FIXED build (f19b17d) ran the same harness/seed ranges (250+ seeds) with zero daemon crashes. **Supplementary evidence only** (human decision 2026-11-19): the accepted gate is the crafted deterministic case `audit-craft-grave-frontier` (see the gate section below). | yes (cores + symbolized stacks) | closed (superseded as gate; kept as supplementary) | — | Gate evidence, not a new product defect: base is the known-broken parent. Open question for the Phase-2 storage slice: is `slotcask_pool_rebuild_worker` one of the scans the f3c6ee5 fix widened, or a **sibling still reachable on main** (fixed build never crashed, but the trigger shape may not have been hit there)? |
| AUD-QRY-1 | query / aggregate group_by × bitmap | **DL** (silent wrong results in a query mode) | checklist 2 (query modes), 6 | `group_by` over a field indexed **bitmap-only** with criteria whose plan is not all-bitmap-eq **silently returns zero groups**. Root cause: the IGB (indexed group-by) fast path accepts bitmap-only group fields (query_aggregate.c:5943–5955, `igb_group_uses_bitmap=1`); with criteria, the count-only bitmap branch requires `can_slot` = all sources bitmap **eq** + no postfilter (query_aggregate.c:6086–6109), and everything else "falls through to btree/scan path" (6181) — which walks the group field's **btree**, which does not exist on a bitmap-only field → zero buckets → `[]`. Repro: 4 shapes (plain/btree/bitmap/composite), 100 rows, group_by=v where v is bitmap-only: t-between criteria → plain `[5 groups]`, bitmap `[]` while count=26 and find rows agree on every shape. `docs/plans/audit-artifacts/AUD-QRY-1-repro.sh` + `.out`. Harness signature: planner-diff group_by section, pd_plain vs pd_bitmap (full-budget seeds 1–2: 148+292 divergences, all this class). | yes (script + output) | **fix-branch in progress — RULING 2026-11-20: accepted as done** (closes its own class; the residual is the QRY-2 class). Sanitizer gates: run ONCE, immediately before the push, not now. Index row stays NOT-fixed until QRY-2 lands and the planner-diff bitmap pair reaches zero. | `fix/audit-igb-bitmap-group-fallback` — plan: `docs/plans/2026-11-19-audit-qry1-igb-bitmap-group-fallback.md` (review rounds 1–3 done; **APPROVED to execute 2026-11-19, first in order**) | Fix: single guard — bitmap-only group field + non-slot-eligible criteria routes to the per-record scan path before the btree Pass-1 walk. **Execution state 2026-11-20**: test written+wired, clean red proven (75/24, artifact), fix applied, targeted case green (99/0), revert-prove-paste done (stash red → pop green, artifact), full suite green (691 cases / 2,416,227 assertions / 0 failed), stale comments + changelog entry (unreleased 2026.11.3 heading, no version bump — human's release cut) done. **HALT**: the Task 3.4 differential expectation ("bitmap pair drops to zero for ALL criteria") is falsified — the pd_plain-vs-pd_bitmap pair retains **14 OR-bearing group_by divergences** (DIVSEC count=0 find=0 agg=44 group_by=36 order_by=0; bitmap pair 148+292 → 14, i.e. QRY-1's own class IS gone; residual root-caused with a wire repro, `docs/plans/audit-artifacts/AUD-QRY-1-residual-orprobe.{sh,out}`: plain n=14 vs bitmap n=20 while count()=14 on BOTH — **mechanism per review round 5/6: the IGB `can_slot` bitmap slot-walk** (the OR sub-tree is invisible to `n_postfilter` — it is neither source nor postfilter — so `source_is_bitmap && n_postfilter == 0 && all-eq` fires and the slot walk serves the unfiltered leaf counts). An earlier attribution to `agg_run_plan`'s primary-leaf branch was WRONG — that branch re-checks the full tree per record, verified in code and by wire runs in review round 5). This is the AUD-QRY-2 class, fixed by the QRY-2 branch's Task 2/3 gates (`fully_covered` refusal + the `can_slot` has_subtree term) — see the AUD-QRY-2 row. Sanitizer gates NOT run (plan-halted); run them before push if the human rules the branch done-as-is. KeySet-filtered bitmap walk noted as perf follow-up. Era: present at audit-base f19b17d. |
| AUD-QRY-2 | query / aggregate scalar + group_by vs OR sub-trees | **DL** (silent over-count in a query mode) | checklist 2 (query modes), 6 | `cmd_aggregate`'s KeySet fast path drops OR sub-trees: criteria `[indexed-leaf, OR[…]]` aggregate over the **leaf's full key set**. The `aw_eligible` branch builds `crit_ks = build_keyset_from_leaf_ck(...)` from the plan's source leaves only and sets `count(*) = \|KeySet\|` (query_aggregate.c:5443–5530); the planner never even carries the has_subtree flag out of plan_filter (`(void)has_subtree;`, query_plan.c:3638). count and find recheck the whole tree per record and are correct; the group_by IGB path shares the pattern (its `crit_ks` is also built from source leaves only → btree-shape group_by over-counts too). Repro: `[t between 50,75, {or:[v=cat_1, u=7]}]` → count=7, find=7 rows on ALL shapes; aggregate count = 7 plain but **26** btree AND bitmap (all 26 t-range records; OR ignored). AND-only control correct (5/5/5). `docs/plans/audit-artifacts/AUD-QRY-2-repro.sh` + `.out`. Harness signature: aggregate section (+ btree-pair group_by), criteria containing `"or"` (full-budget seeds 1–2: 102+51 and 182+91 divergences, 100% OR-bearing). | yes (script + output) | **fix-branch — EXECUTION COMPLETE 2026-11-20, all gates green, uncommitted pending human raw-diff review** (queue position 1) | `fix/audit-agg-subtree-cover-gate` — plan: `docs/plans/2026-11-19-audit-qry2-agg-subtree-cover-gate.md` (review rounds 1–3 done; **APPROVED to execute 2026-11-19, after QRY-1**) | Refined root cause (review round 1): the planner discards `has_subtree` (`(void)has_subtree;`), and `agg_criteria_fully_covered` (query_plan.c) passes `n_postfilter == 0` trees whose OR sub-tree is invisible to every no-fetch KeySet path (scalar `aw`, IGB, IGB slot-level `can_slot`, TopN prefilter); the intersect relocation hazard (`keyset_agg_from_intersect` restores the tree only on `small_primary || n_postfilter > 0`) is covered by the plan's Task 3. Fix: carry `has_subtree` on FilterPlan, complete the cover gate, extend the intersect restore condition. AND-of-two-ORs (nL==0 first-OR-only union) covered by the same flag. **Find carries the same intersect hole — see AUD-QRY-4 (out of this branch's scope).** **QRY-1-residual attribution history (2026-11-20):** the halt earlier blamed `agg_run_plan`'s primary-leaf branch; review round 5 (blind) REFUTED that — `agg_run_plan` re-checks the full tree per record (`ctx->tree` set at entry, cloned by `agg_ctx_clone_shared`, evaluated per record in `agg_scan_cb`; re-verified with wire runs), and the real route of the orprobe red (14 vs 20) is the **IGB `can_slot` bitmap slot-walk**: the OR sub-tree is neither source nor postfilter so `n_postfilter == 0`, and `can_slot` never inspects the sub-tree. An interim amendment gating `agg_run_plan` was REVERTED (it would have forced full scans on correct fast queries). Task 4 is verify-only again. **RULING 2026-11-20: execute the round-6-folded plan** (0B/2M/8m all folded: in-process case 15, patch-based revert-prove because QRY-1 is uncommitted in the same file, manual one-term revert for case 9, the >8-leaf rule inside the `has_subtree` loop — see AUD-QRY-10 — TopN variants on `agg_btree`). **EXECUTED 2026-11-20, every gate green:** Task 1 red on base exactly as the plan predicted (17 failures: cases 1,2,3,4,5,7,9,10,12,13 — `AUD-QRY-2-red-base.log`; case 15's file red via the expected `'FilterPlan' has no member named 'has_subtree'` compile — `AUD-QRY-2-red-case15-compile.log`); patch-based revert-prove (17-red → 189+20 green, `AUD-QRY-2-revert-prove.log`); case 9's one-term revert failed ONLY case 9; full suite `--jobs 8` 693 cases / 2,416,436 assertions / 0 failed; **differential DIVSEC all-zero** (count=0 find=0 agg=0 group_by=0 order_by=0 — zero pd_X-vs-pd_Y lines: the QRY-1 bitmap-pair residual AND the btree-pair divergences are gone, `AUD-QRY-2-diff-divsec.log`); ASan+UBSan `--jobs 6` and TSan `--jobs 3` both 693/0; Task 4 verified (agg_run_plan re-checks per record — code read + the green sub-tree cases that route through it post-fix). Fix surface: `FilterPlan.has_subtree` (query_internal.h), plan_filter's three rules + `agg_criteria_fully_covered` term (query_plan.c), intersect-restore + can_slot + TopN terms (query_aggregate.c); no agg_run_plan change (Task 4 verify-only). Era: predates audit-base. |
| AUD-PLN-2 | query / planner criteria compile | minor | checklist 2 | An explicit `{"and":[l1,l2]}` node as the **sole list element** (`[{"and":[…]}]`) does not flatten: the implicit-AND root has one non-leaf child, `collect_and_leaves` sees nL=0, `find_fully_indexed_or` finds no OR child → FP_FULL_SCAN even when every leaf is indexed. Results stay correct (the scan executor matches the whole tree) — a planner-quality gap, not DL. Discovered while tuning the differential's criteria generator (intersect tally stayed 0 until the generator switched to two list elements); also explains why `{and:…}`-wrapped criteria never hit the IGB/aw fast paths in early smoke runs. Proof trace: explain of `[{"and":[t-between,t-between]}]` on a btree shape → `{"plan":"scan","source":[]}` while the same AND as two list elements → `{"plan":"intersect"}`; count identical either way (16). | trace (transcript, this session) | open | — | Record only; candidate for a Phase-4 planner-quality branch if the human wants it. |
| AUD-PLN-3 | query / planner composite indexes | minor | checklist 2 | Composite-only objects (e.g. indexes `["u+v"]`, no single-field indexes) **never plan any composite path**: `most_selective_indexed` returns -1 (no singly-indexed leaf) and `plan_filter` returns FP_FULL_SCAN at query_plan.c:3726 **before** Phase B's exact-covering composite check (query_plan.c:3752) and the D1 composite-prefix overlay — both require `prim ≥ 0`. u-eq, [u-eq,v-eq] (the exact-covering case), and u-eq+order_by all scan on a composite-only object; the `u+v` index directory exists on disk and is maintained by writes. Results correct via scan (not DL); composite index reads silently cost full scans. Proof trace: create cp with `["u+v"]`, 5000 rows, `indexes/` contains `u+v`; explain u-eq / [u-eq,v-eq] / [u-eq,t-range] / u-eq order_by=t → all `{"plan":"scan","source":[]}`. | trace (transcript, this session) | open | — | Fix direction (if wanted): let exact-covering composite Phase B run before the prim<0 early return. Planner-quality branch candidate alongside AUD-PLN-2. |
| AUD-HARN-2 | harness quirk (phase1-incidental) | note | n/a | The W1 kf-tier seam (`test_env_ctl_set_kf_seams`, control channel) reaches only a **running** daemon; a restart re-reads sizing from db.env. A load done under `initial=64` then restarted WITHOUT the seam reopens the root under the default tier and bitmap reads degrade to ~0 (sizing mismatch, TEST_BUILD only; `slotcask_default_slots_for_splits` consumers include bitmap sizing). The persistence knob is `KF_TEST_INITIAL_SLOTS=<n>` in db.env (config.c:625, TEST_BUILD-only, not a documented production setting). Both era-gate overlays and the resplit gate case persist the seam this way. | repro: audit-gate-resplit-bitmap first restart attempt (254→fail, fixed by the env line) | open (harness note) | — | No product action: production tiers never vary per process. Recorded so future crash tests persist era seams across restarts. |
| AUD-QRY-3 | storage / count metadata vs fault recovery | **DL-class** (count query returns 0 for a fully intact object) | checklist 5 (resource lifetimes), 6 (crash safety), 2 (count mode) | A faulted mutation window that is rejected **after** kf slot reservation writes a tombstone with **no** counter bookkeeping (slotcask.c ~3281: "No header total/deleted bookkeeping"), but a later insert reusing that tombstone slot decrements `hdr->deleted--` (slotcask.c:3268, `plan->reused_tomb`) → **underflow to 0xFFFFFFFFFFFFFFFF when deleted==0**. `slotcask_sum_kf_totals` (slotcask.c:5282) sums raw across shards and the object-level clamp (`total > deleted`) flips false → **`count` returns 0 for the whole object** while every record is intact and correctly readable (per-key verification passes; describe works). Repro: deterministic — `audit-fault-inject` seed 3 (3/3 reproductions), 24 ops, fdatasync fault hit=1 EMFILE, SIGKILL, clean restart: 13 acked ops (incl. an acked bulk), 8/8 model-live keys correct, count→0. Kept-root evidence: shard 002 header `deleted=0xffffffffffffffff` (total=1), sum total=7 vs deleted=2^64−1 → clamp → 0; recovery WARN "1 staged, not-activated record(s)" corroborates the torn window. `docs/plans/audit-artifacts/AUD-QRY-3-evidence.txt`. Blast radius: every `get_live_count` consumer — client count, find-total hints, auto-reshard sweep sizing, vacuum decisions — reads 0 until a rebuild fixes the header. | yes (kept-root hexdump + deterministic seed) | **fix-branch — EXECUTION COMPLETE 2026-11-20, all gates green, uncommitted pending human raw-diff review** | `fix/audit-kf-deleted-underflow` — plan: `docs/plans/2026-11-19-audit-qry3-kf-counter-underflow.md` (review rounds 1–3 done — round 3 widened the root cause: ordinary bitmap-cap rejection after reservation also creates inert tombstones, so existing roots may already be wrapped; **APPROVED 2026-11-19 with the repair-policy DEFAULT**: warn at kf open when `deleted > total`, no auto-repair in this branch, plus a queued follow-up plan making `recount` rebuild the kf header from the slots; contains the full counter-reader impact inventory) | Fix (review-refined): symmetric bookkeeping at `kf_write_tombstone_at_slot` (`total++ && deleted++`, only on the non-flag==2 → flag==2 transition so KFM2 replay re-entry cannot double-count) **plus** `plan->kf_header_changed = 1` in the abandoned-plan branch — without that flag the header does not flush and the booking is lost in a power cut while the tombstone bytes persist. **Executed 2026-11-20, every gate green:** the bitmap-cap wrap REPRODUCED on base first (the human's verify-first directive: 5 shards wrapped, count→0, orphaned −1 — `AUD-QRY-3-red-base.log`); case `test-kf-deleted-underflow` 203/0 green post-fix (includes the crafted-legacy-tombstone reuse guard case B); revert-prove red→green (`AUD-QRY-3-revert-prove.log`); fault-harness seed 3 clean, 15-seed matrix 13/15 = the documented AUD-HARN-3 phantom baseline with ZERO count violations (`AUD-QRY-3-fault-matrix.log`); full suite `--jobs 8` 694/0; **sanitizer gate ×3 consecutive each** (AGENTS shared-state rule): ASan 694/0 ×3, TSan 694/0 ×3 (`AUD-QRY-3-sanitizer-gates.log`). Task 3's open-time WARN implemented per the DEFAULT (warn-once per KfCacheEntry via an atomic bit; vacuum's kf-compact/resplit remain the heals). Reader inventory unchanged (see the plan's table): no deletion path acts on the corrupt value; skip/refuse directions as inventoried. |
| AUD-QRY-4 | query / find intersect vs OR sub-trees (WIDENED 2026-11-19: all find `total` sites) | major (**fixed on branch 2026-11-22** — red-proofed: 100 vs 20, 160 vs 106, 6000 vs 3250 rows+total, "total":0 vs 27) | checklist 2 (find mode) | The find-side twin of AUD-QRY-2. **Site 1**: `keyset_find_from_intersect` (query.c) restores the original tree for per-record re-match only when `small_primary \|\| fp->n_postfilter > 0`, and reports `*out_total = keyset_size(ks)` — a criteria tree `[indexed-leaf, indexed-leaf, OR[…]]` whose intersect candidate set exceeds INTERSECT_MIN_PRIMARY = 10000 (and with no small `limit` window routing to streaming) drops the OR from both rows and total. **Sites 2–4 (widened per the AUD-QRY-2 round-3 review)**: (2) the streaming-leaf `want_total` condition in `cmd_find` (`want_total && fp.kind != FP_INTERSECT && fp.n_postfilter == 0` is sub-tree-blind; reviewer-measured on the QRY-2 fixture shapes: `limit:1,total:true` returns total **100 vs true 26** on the C1 shape, **160 vs 106** on AND-of-ORs); (3) the UNION `total` (= \|KeySet\|, exact for a pure OR root but partial for the nL==0 AND-of-ORs union — same hole AUD-QRY-2's Task 2 nL==0 flag closes on the aggregate side); (4) every `total_cheap` consumer that trusts the plan without re-checking the tree. The campaign differential never generates two-leaves+OR, which is why 866 divergences showed zero find-section hits. NOT covered by the AUD-QRY-2 branch (one concern per branch). | RED-proofed on base 2026-11-22 (`AUD-QRY-4-revert-prove.log`): case1 streaming-gate total 100 vs 20; case2 partial-union total 160 vs 106; case3 wide-AND (>8 leaves) rows+total 6000 vs 3250 (the AUD-QRY-10 class, find side); case4 declined-total "total":0 vs 27; pure-OR / leaf-only / assertion-3b controls green on base too | **fix-branch — EXECUTION COMPLETE 2026-11-22, all gates green, uncommitted pending human raw-diff review** | own branch after AUD-QRY-2 lands (**2026-11-20 ruling: queue position after QRY-8/QRY-5** — earliest allowed is once QRY-2 lands; the `FilterPlan.has_subtree` field the QRY-2 branch adds makes every site's fix a one-predicate-term change; plan CONVERGED rounds 1–4) | Verify empirically first (10k-row fixture, two broad indexed leaves + OR, compare find rows/total vs scan oracle; the limit:1 total sites are reprovable at fixture scale); if confirmed, fix all four sites in one branch riding QRY-2's flag. **EXECUTED 2026-11-22, every gate green — see the queue entry (item 5) for the full record.** |
| AUD-QRY-5 | query / aggregate group_by × bitmap distinct-value cap | **DL-class** (silent wrong groups in a query mode) | checklist 2 (query modes) | `bm_collect_uniq_cb` silently stops collecting at the 256-slot cap (`if (*c->n >= c->cap) return 0;`, query_aggregate.c:4727) and **both** collection call sites pass cap 256 with 256-row stack arrays — the count-only bitmap branch (`bm_vals[256][1024]` + `.cap = 256`, query_aggregate.c:6034/6053) and the Phase 1b hbm branch (`bm_vals_h[256][1024]`, query_aggregate.c:6210). A bitmap field declared `bitmap(N)` with N ≥ 257 (`bitmap(N)` is legal for N ∈ [2, 65535] — query_schema.c:1529-1532, `BM_HARD_CEILING`; `parse_index_spec` itself only rejects N ≤ 0, config.c:1230-1239) that holds ≥ 257 distinct values makes group_by return at most 256 groups: values beyond the first 256 collected get no bucket on the no-criteria popcount path, no slot walk on the `can_slot` path, no hbm bucket on Phase 1b — silently, with no error. The slot-level walk itself is unaffected (`counts[256]` is indexed by collected `bm_n`). Found by the AUD-QRY-1 round-3 review (sibling defect in the same bitmap IGB block, recorded in the QRY-1 plan's out-of-scope). | yes (live wire repro: `docs/plans/audit-artifacts/AUD-QRY-5-repro.{sh,out}` — cap_demo bitmap(300)+u, 900 rows/300 distinct: count 900, group_by 256 labels) | **fix-branch — EXECUTION COMPLETE 2026-11-20, all gates green, uncommitted pending human raw-diff review** | `fix/audit-igb-bitmap-256-cap` (plan CONVERGED, rounds 1–2; **APPROVED to execute 2026-11-20: after QRY-1/QRY-2/QRY-3, SECOND of the DL pair — QRY-8 first, then this**) | Fix: overflow-detect at collection (`BmDictCollectCtx.overflow` latched on a NEW distinct value at cap — dedup precedes the cap check, no false positives) → both bitmap IGB branches route to the per-record scan executor; ≤256-distinct objects keep the fast path. **Executed 2026-11-20, every gate green:** Task 1 branch-routing verified (popcount / Phase 1b / can_slot all reachable on cap_bm); Task 2 red exactly as planned — case1 popcount 300-vs-256, case2 Phase 1b 300-vs-256, case3 can_slot oracle-150-odd-labels vs twin-132-wrong (`AUD-QRY-5-red-base.log`; two earlier drafts had test-only bugs, noted in PLAN_NOTES); case4 ≤256-distinct control + case5 count==900 green; revert-prove via the plan's sanctioned path-limited stash (lifts stacked uncommitted QRY-1/2/8 hunks; reds unaffected — verified identical, `AUD-QRY-5-revert-prove.log`); full suite `--jobs 8` 696/0; differential DIVSEC all-zero (`AUD-QRY-5-diff-divsec.log`); ASan 696/0 + TSan 696/0 once each. |
| AUD-QRY-6 | query / aggregate group_by × bitmap long values | major (**fixed on branch 2026-11-22** — memory-safety + silent wrong groups; ASan-red proofed) | checklist 2, 5 (resource lifetimes) | `bm_collect_uniq_cb` copies `min(vlen, 1024)` bytes into the 1024-byte stack rows but records the **full** `vlen` (query_aggregate.c:4728-4730). Every later consumer reads `vlen` bytes from the truncated row: `bm_popcount_one_value` (6062-6064), `bm_walk`/`bm_test` (6155-6156), `agg_index_label` (6071, 6166, 6264) — a stack OOB read of up to 64511 bytes past the row (bitmap dict values may be up to 0xffff bytes: bitmap.c:1121/1152 refuse only > 0xffff), plus a guaranteed mismatch (a truncated copy never equals its dict value), so any value longer than 1024 bytes never counts → its groups vanish even below the 256 cap. ASan-detectable. Found by the AUD-QRY-1 round-3 review. | RED-proofed on base 2026-11-22: functional (`AUD-QRY-6-revert-prove.log` — case2 lv_bm OK silent under-report vs oracle ERR; case3 lv_only OK silent empty) + memory-safety (`AUD-QRY-6-asan-base.log` — stack-buffer-overflow READ of size 1999 in `bm_collect_uniq_cb`'s dedup memcmp at query_aggregate.c:4751 via bm_iter_values ← the popcount-branch collection at 6111; the lv_wide fixture's shard-placement engineering is REQUIRED for detectability — see PLAN_NOTES) | **fix-branch — EXECUTION COMPLETE 2026-11-22, all gates green, uncommitted pending human raw-diff review** | planned after QRY-5 (same callback + arrays; may share the branch) — **2026-11-20 ruling: queue position after QRY-4**; plan CONVERGED rounds 1–2. **EXECUTED 2026-11-22** — the plan's DEFAULT decision (refuse collection for vlen > row width, latch QRY-5's signal, scan executor → documented E2BIG parity). Also corrected by execution: `agg_index_label` is NOT an OOB consumer (E2BIG short-circuit at 729) — consumers are the dedup memcmp + the dict-lookup paths, exactly as the folded plan's Brief lists. |
| AUD-QRY-7 | query / aggregate group_by btree open failure treated as empty shard | major (**fixed on branch 2026-11-22** — fail-closed violation; RED-proofed) | checklist 2, 5 | `igb_pass1_worker` treats ANY `btree_range_iter_open` failure as an empty shard: LOG_ERROR + `return NULL` with no `w->aborted` / no `ctx.agg_error` (query_aggregate.c:4251-4256) — in contrast with the hbk_init failure directly above it (4242-4246 sets `aborted = 1`) and the bitmap branches' ENOENT-vs-hard-error split (6045-6051, 6135-6142, both fail closed with `agg_error`). An I/O error or corruption while opening a genuine btree group field's index silently drops that shard's groups from aggregate results. Found by the AUD-QRY-1 round-3 review. | trace (code-read above; repro in the QRY-7 plan: fault/open-failure injection on a btree group field) | **fix-branch — EXECUTION COMPLETE 2026-11-22; review round 2026-11-23: the regression test now faults the open through the test-control range-iterator open-failure seam (root-safe, no chmod, no restart) and adds the PARALLEL Pass-1 route (100k rows / 8 idx shards); base RED re-proven on BOTH routes — case1 (serial) + case5 (parallel), 4 failed assertions; fix green 111/0. All gates green, uncommitted pending human raw-diff review** (`AUD-QRY-7-revert-prove.log`: base RED = case1's two assertions — a silently-short OK group list; post-fix the faulted group_by fails closed with the pinned EIO text; baseline + controls green; the hook-based rewrite's revert-prove is recorded in PLAN_NOTES) | planned after QRY-5/6 — **2026-11-20 ruling: queue position after QRY-4** (with QRY-6); plan CONVERGED rounds 1–2 | Fix: distinguish ENOENT (no index files for the shard — verify whether v2 objects always create all `index_splits_for` files at index build, which would make even ENOENT unexpected) from hard errors and set `aborted`/`agg_error` fail-closed, mirroring the bitmap branches. |
| AUD-QRY-8 | query / count + aggregate vs the composite-exact plan | **DL-class** (count returns 0, aggregate 0/[], find correct) | checklist 2 (query modes) | plan_filter Phase B (`find_exact_covering_composite`, query_plan.c:3731-3748) plans `[a-eq, b-eq]` over a covering composite index `a+b` as `FP_PRIMARY_LEAF` + `FP_ORDER_COMPOSITE_EXACT`, with `source_leaves` = the covered eq leaves — fields that may have **no standalone index** (the repro's `a`). Only `find` handles this plan (D1 composite-exact branch, query.c:10433 → `find_via_composite_key` rebuilds the exact composite key; `fp_compute_total` refuses it → total:null, query.c:9193). Every other consumer is COMPOSITE_EXACT-blind: `cmd_count_with_tree`'s primary-leaf else-branch dispatches on `source_leaves[0]->field` = `a` (query.c:7186-7187) → `btree_dispatch_checked` on the nonexistent standalone index succeeds with zero entries (per-shard `bt_acquire` failure returns silently, btree.c:2320) → `parallel_indexed_count` over zero entries → prints `0`; the aggregate family has zero COMPOSITE_EXACT handling (grep: none in query_aggregate.c) — the `aw` gate (`agg_criteria_fully_covered`: kind != FULL_SCAN && n_postfilter == 0) builds `crit_ks` from `source_leaves[0]` only (5451; with a standalone index on `a`: `count(*) = \|{a=x}\|`, the `b` half ignored — query_aggregate.c:5554-5557 has NO per-record re-check) and without it `build_keyset_from_leaf_ck` refuses (CBF_NONE, query.c:5211-5212) → fall-through 5569 → `agg_run_plan`'s own plan (4018) dispatches `source_leaves[0]` again (4023-4063) → `[]`/0; IGB (6340-6356) and TopN (974-1042) share the blindness. Reviewer-measured (AUD-QRY-2 round-3): fields a,b,c; indexes `b` and `a+b`; criteria `[a eq 1, b eq 2]` → find 20 rows correct, count `0`, scalar aggregate count(*) `0` (group_by → `[]`). Bulk-delete safe (CBF_NONE fallback AND full-tree re-verification in `bulk_criteria_indexed_cb`, query.c:9041-9045). Discovered as AUD-QRY-2's round-3 sibling; NOT covered by the QRY-2 branch (COMPOSITE_EXACT has no sub-tree). | yes (reviewer-measured fixture; the QRY-8 plan re-proves wire-level and pins the count route) | **fix-branch — EXECUTION COMPLETE 2026-11-20; review round 2026-11-23 (3 reviewers, 0 blockers): re-planned AT THE PLANNER per review — Phase B now serves the composite-exact plan only to record-fetching callers, the count guard and all four aggregate normalizations are removed, count/aggregate keep index-driven plans (leaf/intersect; scan only when no source leaf has its own index); `explain` perf-shape control added (case 10). All gates green on the revised tree, uncommitted pending human raw-diff review** | `fix/audit-composite-exact-agg` (plan CONVERGED, rounds 1–2; **APPROVED to execute 2026-11-20: after QRY-1/QRY-2/QRY-3, FIRST of the DL pair**) | Fix: refuse-first — guard count's primary-leaf branch (7124) + normalize composite-exact → FP_FULL_SCAN at all FOUR plan_filter sites in query_aggregate.c (970/4018/5444/6011) + defense-in-depth term in `agg_criteria_fully_covered`; find untouched. **Executed 2026-11-20, every gate green:** Task 0 enumerated the 4 aggregate sites (post-QRY-2 lines 974/4024/5450/6021) and confirmed the consumer list matches the Brief (no new consumer); Task 1 red exactly as planned — cases 1-5 RED (0 vs 50; 100 vs 50 on ce_mix; phantom vectors/TopN), 6-9 GREEN including bulk-delete (safe-by-accident CONFIRMED on this base — Decision 3's extension NOT needed; `AUD-QRY-8-red-base.log`); patch-based revert-prove red→green (`AUD-QRY-8-revert-prove.log`); full suite `--jobs 8` 695/0; differential DIVSEC all-zero, no delta (`AUD-QRY-8-diff-divsec.log`); ASan 695/0 + TSan 695/0 once each. |
| AUD-QRY-9 | query / aggregate IGB Pass-2 + secondary-map btree opens fail open (+ the limited/streamed k-way merge opens, added by the round-2 review) | major (**fixed on branch 2026-11-23** — fail-closed violation, same class as AUD-QRY-7) | checklist 2, 5 | Two more fail-open btree opens inside `cmd_aggregate_do`'s IGB block (discovered by the AUD-QRY-7 round-1 review, recorded not fixed there — one concern per branch): (1) Pass-2's agg-field walk treats ANY `btree_range_iter_open` failure as an empty shard (`if (!it) continue;`, query_aggregate.c:6954-6956) — silently wrong sum/avg/min/max for that shard; (2) `sec_map_build_worker` does the same (query_aggregate.c:4172-4177), and its silent miss converts to `multi_skip` record drops at 4337/6882 (consumed via `sw[s].err`/`sw[s].aborted` at 6482/6489). Both contrast with the block's own fail-closed bitmap branches (6045-6051, 6135-6142). Fix mirrors the AUD-QRY-7 template (stat-first ENOENT-empty, EIO fail-closed). | trace (code-read; repro via the test-control range-iterator open-failure seam with a skip count targeting each route's own open — `test_env_ctl_set_bt_range_open_fail_nth`, root-safe) | **fix-branch — EXECUTION COMPLETE 2026-11-23, all three sites fail closed, uncommitted pending human raw-diff review** (`test-igb-agg-open-fail-routes`: RED on base — silently-wrong sums / silently-short group lists on all three routes; GREEN post-fix 171/0) | executed 2026-11-23 alongside the QRY-7/QRY-8 review round (one case, three routes) | Fix: same stat-first + EIO fail-closed treatment at the Pass-2 agg-field walk, `sec_map_build_worker`, and BOTH limited/streamed k-way merge opens (initial + flush-reopen), one case covering all three routes. Fix: same stat-first + EIO fail-closed treatment at both sites, one branch. |
| AUD-QRY-12 | query / composite-exact find returns zero rows for csv, rows and join when the composite's first covered part has no standalone index | **DL-class border — fixed on branch 2026-11-23** (wrong empty results in three find shapes; default format correct) | checklist 2 (query modes) | plan_filter Phase B fills `source_leaves` in COMPOSITE order; the csv/rows/join shapes fall through the D1 composite-key executor (it is gated to default format) to the generic leaf dispatch, which drives on `source_leaves[0]` — with indexes `["b","a+b"]` and criteria `[a eq, b eq]` that is the phantom `a` leaf (no standalone index): zero candidates → header-only CSV, `[]` rows, empty join. Repro: `test-composite-exact-formats` (indexes b, a+b, c; 300 rows; default 50, csv 0, rows 0, join 0 on base). | RED-proven on base 2026-11-23 (case1/2/3 diags: csv bytes=10, join keys=0); GREEN post-fix 28/0 | **fix-branch — EXECUTION COMPLETE 2026-11-23, uncommitted pending human raw-diff review** | `fix/audit-composite-exact-agg` (rides the QRY-8 branch's Phase-B surface) | Fix: Phase B puts a covered leaf that has its own standalone index FIRST (D1's key rebuild matches leaves by FIELD NAME — order-agnostic, so the swap is safe); if no covered leaf is standalone-indexed, the exact plan is skipped and the normal ranking runs (prim >= 0 guaranteed upstream). |
| AUD-QRY-13 | slotcask / abort_pre_marker holes | **DL (confirmed)** — acknowledged inserts unreadable (`exists` false, count still counts them) | checklist 5, 6 (crash safety) | A deferred window with `nactive==0` aborted before the abandoned-tombstone pass, stranding flag==0 reservations; later windows of the same request had been placed beyond them (res_map), so after the request they sat behind a hole (`kf_probe_next` stops at flag==0). Reachable from ordinary input (bitmap distinct-value cap rejecting a whole window). | `test-abort-pre-marker-holes`, `test-unpublished-window-holes`, `test-publishing-window-abandoned-sync` (each red on the previous code) | **EXECUTED 2026-11-24 (extended after blind review round 6: unpublished-window exits + pre-marker durable pass; uncommitted)** | `docs/plans/2026-11-24-audit-qry13-abort-pre-marker-holes.md` | Fix: the abort branch now runs the shared `bulk_tombstone_abandoned_locked` pass and syncs it before the request can succeed. Pre-existing records are unaffected (only same-request later windows). Keyfiles already holding such holes: `vacuum`. |
| AUD-QRY-14 | btree / never-created index shard read as a hard failure | **regression inside the round-4 M3 change — fixed on branch 2026-11-24, never committed** (121 suite cases) | checklist 2, 6 | Index shards are created lazily by the first write that routes a record to them, so a shard nothing routed to has NO file (open → ENOENT). The round-4 M3 change made three walkers report ANY failed btree acquire as an error: `btree_search` / `btree_range_ex` (whose fan-out deliberately visits every shard — every indexed eq / range / IN / between / neq on a small object failed), `chunk_walk_worker` and `wfc_worker` (NULL from `btree_range_iter_open` on the absent shard became "index storage error" / "aggregate storage access failed"). The other fail-closed consumers already used a stat-first guard; these three had none. Repro: `/tmp` probe with 2 records → only `indexes/addr/001.idx` exists; `count addr eq …` → `{"error":"invalid typed record"}`. Pinned by `test-idx-absent-shard` Part A (one record → exactly one shard file per index; the shard number is read off the scratch tree, never assumed). | yes (red on the M3-as-first-written tree: Part A 15 failures; green post-fix) | **fixed on branch 2026-11-24, uncommitted** | rides the round-4 fix branch (`test/audit-oracle-harnesses`) | Fix: the rule lives once in btree.c — `bt_acquire_shard_reader` (ENOENT = absent = empty; anything else = -1), used by the three walkers and by `btree_range_iter_open` (an absent shard opens as an EXHAUSTED iterator; NULL now always means a failure of an existing file, so no consumer needs a stat-first guard). `bt_open_file` sets `errno = EINVAL` at its two non-syscall rejection sites (zero-size, foreign format) so a stale ENOENT cannot pass a real failure off as "absent". |
| AUD-QRY-15 | query / join by indexed remote field | **major — fixed on branch 2026-11-24** (silent wrong results under an index storage failure) | checklist 2, 6 | `resolve_join_record` (query_join.c) ignored `btree_idx_search`'s return: when a shard of the remote field's index cannot be opened the lookup reads "no match" — an inner join silently drops the driver row, a left join emits a false `null` — 200 OK, well-formed. Repro (walker seam `test_env_ctl_set_bt_walk_open_fail` on the shard holding the remote record; the driver sits on a NON-indexed field so the lookup is the only walk of that shard): baseline `rows:[["k0001",1,1,1,"t1"]]`; armed inner → `rows:[]`, armed left → `["k0001",1,1,1,null]`. `test-idx-absent-shard` Part C shapes "scan driver, join by field" and "scan driver, left join by field". | yes (red on the pre-fix build: 2 shapes OTHER; green post-fix) | **fixed on branch 2026-11-24, uncommitted** | rides the round-4 fix branch | Fix: propagate the walker failure (`!= 0` → `return -1`, errno kept) at both lookup sites — the single-field hit and both passes of `join_candidate_components`; all three callers already fail closed on `-1`. Same class as the round-4 "ignored find_via_composite_key return" item but NOT in that list — surfaced by the route matrix; split out if preferred. |
| AUD-QRY-16 | query / find envelope close strings at four self-closing exits | **minor — fixed on branch 2026-11-24** (malformed close on a budget or collect error in rows / joined finds) | checklist 2 | Four exits that already closed the envelope on `main` closed it wrongly for `format:"rows"` and joined finds: with `total:true` they emitted `],"total":null}}` (one `}` too many), without it `]` (the object never closed) — the `rc == -2` and OR-union budget exits, and the ordered-fallback and small-prefilter collect errors. Not reproduced on the wire (budget and collect faults on a rows/join shape); code-read against the normal tail's close (`],"total":N}` / `]}`), which the round-4 M2 code had copied for the intersect exit. | trace (code-read) | **fixed on branch 2026-11-24, uncommitted** | rides the round-4 fix branch | Fix: one shared close (`find_close_envelope_on_error`, query.c) mirroring the normal tail with `total:null`, used by those four exits and the two round-4 exits. **Tried and REVERTED the same session:** a wider sweep that also closed the envelope at the 21 bare `return -1` exits of `cmd_find_do_impl`. It was wrong — `docs/query-protocol/find.md` keeps a failure after the response has started unterminated on purpose ("Bitmap-source errors…", "Unordered indexed pages"; pinned by `test-kef-trigram-starts-error`, which went red on the sweep and caught it). Whether pre-row failures on the other routes should close is a design ruling — AUD-QRY-22. **Also made contract-exact:** the round-4 M2 intersect and M3 composite-exact exits closed unconditionally, including after rows were emitted; they now close only while nothing but the prefix is on the wire (`t_find_rows_emitted`). Repro of the mid-stream half, RED before / GREEN after: `test-kef-intersect-midstream-error` (fetch failure on the second batch: closed page with 1,024 rows plus an appended error object → unterminated) and `test-idx-absent-shard` Part D (composite-exact reopen failure after rows: closed page with 4,924–9,139 rows plus error → unterminated). |
| AUD-QRY-17 | query / dispatch default branch (O_DIRECT full-leaf scan) | major (code-proven; silent short results under an index read failure) | checklist 2, 6 | `btree_od_shard_worker` (query.c) ignores `btree_leaf_scan_o_direct`'s return, whose contract is `0` success, `+1` early stop, `-errno` I/O error (io_direct.h): a shard that fails to stat, open or read is silently skipped for every operator the dispatch's `default:` branch serves (contains, ends, like-substring, regex, len_*, not_in, not_like, exists/not_exists …). Its OOM fallback ignores `btree_idx_range`'s result too. Same class as the round-4 M3 walkers, not in that list. No fault seam reaches `btree_leaf_scan_o_direct`, so this is a code proof, not a repro. | trace (code-read) | **open — needs a seam before it can be red-proven** (round-5 candidate) | — | Fix direction: the worker records the failure (ENOENT = empty shard), `btree_idx_full_leaf_scan_o_direct` returns it, the `default:` leg does `errno = …; return -1` like the other 13 legs. Needs a TEST_BUILD seam on the O_DIRECT scanner. |
| AUD-QRY-18 | btree / k-way ordered walk (keyset-cursor find, ordered walks) | major (code-proven; silent short pages under an index open failure) | checklist 2, 6 | `btree_walk_ordered_ranges` (btree.c): `cursors[s].iter = btree_range_iter_open(…); if (!cursors[s].iter) { resume[s].done = 1; continue; }` — a shard whose open fails is retired as if exhausted, so its entries vanish from every page of a keyset-cursor find (and from the composite-prefix ordered walk, query.c `btree_idx_walk_ordered` callers) with 200 OK. Before 2026-11-24 NULL also meant "absent shard"; since the AUD-QRY-14 fix an absent shard opens as an exhausted iterator, so NULL is now always a real failure that this branch swallows. The round-4 iter-open seam (`test_env_ctl_set_bt_range_open_fail`) reaches it directly. | trace (code-read; seam available) | **open** (round-5 candidate) | — | Fix direction: plumb an error out of `btree_walk_ordered_ranges` and `btree_idx_walk_ordered` (both void today) to their callers and fail the page closed. |
| AUD-QRY-19 | btree / `btree_walk_all_values` | minor (latent — no product caller) | checklist 2 | Returns `0` ("success") when the file cannot be opened (`if (bt_acquire(…) != 0) return 0;`) although its contract is "cb's last return value". Only tests call it today (`test_o_direct_scan.c`); any aggregate that adopts the "3× faster" walker inherits a silent-empty shard. | trace (code-read) | open | — | Fix direction: the shard-reader helper (absent = empty, any other failure = -1) once it gains a product caller. |
| AUD-QRY-20 | query / count and aggregate error text | minor (diagnostics) | checklist 2 | Every hard failure of the count/aggregate KeySet stages renders as `{"error":"invalid typed record"}` regardless of errno (`count == SIZE_MAX` → that string; the multi-leaf count collect renders `invalid index criterion`), so an index storage failure is reported as a data error. Pre-existing text; the round-4 M3 change made the storage route reachable. Repro: `test-idx-absent-shard` Part B — eight count shapes answer `invalid typed record` and one `invalid index criterion`, all for one injected EIO. | yes (matrix diags) | open | — | Fix direction: render by errno, as the find side now does (`find failed: <strerror>`). |
| AUD-QRY-21 | btree / first creation of an index shard file | minor (design question for the human) | checklist 5, 6 | The incremental writer creates a shard file in place: `open(O_CREAT)` → `fstat` → `ftruncate` → `mmap` → `bt_init_file` (`bt_open_file`). A concurrent reader (readers open outside `bt_cache_lock`) can observe the zero-size or not-yet-initialised file, and a crash inside that window leaves it at rest. Readers reject both (`EINVAL` since 2026-11-24; errno was stale before), so every fail-closed consumer reports a storage error — in the window (the first insert into a shard while a query fans out over it) and, after a torn creation, until the next write to that shard or a reindex heals it. `main` read both as an empty shard. Not reproduced; the window is two syscalls wide and only on first creation. Bulk and reindex builds are unaffected (they publish by rename). | trace (code-read) | **open — needs a ruling** | — | Options: (a) accept and heal zero-size `.idx` files in the startup sweep alongside `*.new`/`*.old`; (b) read zero-size/uninitialised as empty in `bt_acquire_shard_reader`; (c) make creation atomic. |
| AUD-QRY-22 | query / wire shape of a find that fails BEFORE any row is emitted | minor (design question for the human; documented behaviour, not a defect) | checklist 2 | `cmd_find_do_impl` writes the envelope prefix, then runs the route. When the route fails before the first row — a KeySet build, candidate collect, join lookup or iterator open that hits a storage error — the documented contract leaves the response unterminated (find.md: a failure after the response has started is unterminated), and that is what 17 exits do. Three exceptions close the bare envelope and append an error object: the two budget exits (pre-existing on `main`) and, since round 4, the AND-intersect build failure and the composite-exact walk. So the same failure class answers three ways depending on the route. Repro of the unterminated pre-row cases: `test-idx-absent-shard` Part C under the walker seam — rows-format finds, both join shapes and the default-format OR find return a bare prefix (`{"columns":[…],"rows":[` / `[`), classified there as fail-closed per the contract. A client that only parses JSON cannot tell those apart from a mid-stream cut; a client that reads the prefix cannot tell an error from a slow page. | yes (Part C diags) | **open — needs a ruling** | `docs/plans/2026-11-24-audit-qry22-find-pre-row-failure-shape.md` (PROPOSED) | Options: (a) keep as documented (no code change; the plan closes as wontfix); (b) extend the round-4 rule to every route — close + error object while nothing but the prefix is on the wire, unterminated after — which supersedes the find.md sentence for pre-row failures and needs an exact rows-emitted signal per executor (the sweep of 2026-11-24 was built without one and reverted). |
| AUD-QRY-11 | slotcask / kf window-replay re-entry double-books the insert commit | major (live over-count; not DL — records intact, count over-reports until the next header rebuild) | checklist 2, 5 | `bulk_apply_and_sync_kf_locked` is contractually re-runnable (K-phase msync failure → coordinator retry / forward replay), and `kf_commit_planned_slot` books unconditionally: a re-run re-commits every active insert against its already-flag==1 slot — one phantom `total++` per record; a reused-tombstone commit takes `deleted--` twice. The abandoned-tombstone write is guarded (prior_flag); the active commit is not. | trace (code-read of the K retry ladder + commit bookkeeping; repro rides the existing K-phase sync-failure seam) | **plan written 2026-11-23 at the round-2 review — NOT implemented** | `docs/plans/2026-11-23-audit-qry10-kf-commit-reentry.md` (file named per the review directive; AUD-QRY-10 taken) | Fix: idempotent commit — skip the counter update when the slot already holds flag==1 with this hash; same observed-state guard for the DELETE branch if `kf_tombstone_entry_locked` books unconditionally; red test `test_kf_window_replay_double_booking` via `test_env_ctl_set_fail_phase(SHARD_TEST_PHASE_K)`. |
| AUD-QRY-10 | query / planner AND with >8 LEAF children silently loses leaves 9+ | **DL-class** (silent over-count in a query mode) | checklist 2 (query modes) | `collect_and_leaves` caps at `MAX_INTERSECT_LEAVES` = 8: an AND root with MORE than 8 LEAF children keeps leaves 1–8 as intersect sources and silently drops leaves 9+ into NEITHER the source nor the postfilter list — `n_postfilter == 0`, the same invisibility as an OR sub-tree — so every no-fetch KeySet fast path counts the 8-leaf intersection while ignoring leaf 9+. Measured (QRY-2 plan case 13, fixture 400 rows): criteria `[v eq cat_1 ×4, w eq w_1 ×4, u eq 7]` (9 LEAF children; true count = 2, i=46/241) → group_by v on the bitmap twin returns the 8-leaf intersection (27) while count() is correct (its executor re-checks). Found by the QRY-2 plan review round 5 (major: same bug class as the OR sub-tree drop). NOTE: the human directive called this finding "AUD-QRY-9" — that ID was already taken (row above), so the next free ID is used. | reviewer-measured case 13 numbers (group_by 27 vs 2; count 2 correct) | **fix-branch — FIXED in the QRY-2 branch, executed 2026-11-20** (all gates green; uncommitted pending human review): Task 2 sets `has_subtree = 1` INSIDE the `has_subtree` computation loop — before `fp.has_subtree = has_subtree;` so the composite-exact and FULL_SCAN early returns carry it too — when an AND root has more than MAX_INTERSECT_LEAVES LEAF children; the field comment becomes "criteria not fully represented by the plan". Regression: QRY-2 plan case 13 (`test-agg-or-subtree-full-tree`, red-on-base → green post-fix) + the 9-leaf assertion in `test-agg-subtree-flag-fields`. | Fix: the QRY-2 `has_subtree` gate covers it (refuses every no-fetch fast path for the shape → record-scan executors, which match the whole tree). |
| AUD-RES-1 | nql / resource lifetime | major | checklist 5 (resource lifetimes), Phase-1 step 3 fuzzing | A single legacy-path request routed to the NQL dispatcher leaks **152 bytes per request** with no bound: `:pl\nfind pt plt 'indext in[ sed erert f"` (39 bytes) → `dispatch_nql_query` → `nql_parse_command` → `nql_parse_filter` → `read_list` (nql.c:184 malloc + nql.c:199 strdup) — the filter parse fails mid-list and the partially built list is never freed (`nql_free_command` unreachable on that error path). Wire-reachable pre-auth-escalation (any tenant line), unbounded growth → memory-exhaustion DoS. Found by the new `fuzz_frame` harness (request-framing fuzzer, real fast-path dispatcher) under ASan+LSan; deterministic single-request repro. Artifacts: `AUD-RES-1-leak-input.bin` (39 bytes), `AUD-RES-1-leak-report.txt` (full LSan stack: 128B direct + 24B indirect in read_list). | yes (input + LSan report; replay `./fuzz/build/fuzz_frame AUD-RES-1-leak-input.bin`) | open | fix/audit-nql-parse-leak (Phase 4; small: free the partial list on the parse-error path) | Reached through `fuzz_frame` incidentally — the NQL dispatcher is behind the legacy fast path the framing harness executes for real. |
| AUD-HARN-3 | harness gap (fault oracle) | note (harness, phase1) | n/a | The fault harness's dead-key check flags **unacknowledged-but-applied** writes as "resurrection/phantom" violations — but the oracle contract (and the per-shard window atomicity design, "atomicity scope is one Kf shard, by design") explicitly allows an unacked write to be **fully applied** (forward replay lands marker-durable windows regardless of the client's error response). The crash/restart fuzzer handles this with pending/observed-value resolution; the fault harness v1 lacks it, so fault runs on window-era code overcount violations (334b78d fix-side 15/60; current code 2/15 — seeds 5/7/13 phantoms are values from unacked ops that landed). Harness fix (Phase-4 tool branch): adopt observed state for unacked ops at restart (pending mechanism), and model bulk per-shard (or flag only mixed-within-shard). The genuine product signal buried under these was AUD-QRY-3. | yes (seed logs: `AUD-HARN-3-seed5-phantom.log`) | open (harness) | same tool branch as AUD-CI-1 work or standalone | Do NOT count AUD-HARN-3 "violations" as product findings; they are contract-legal states misclassified by the harness. |

(Coverity/CodeQL history seeds land in Phase 1 step 2 — needs the
human-exported Coverity CSV; see Open items.)

## CI-stability baseline (Phase 0 step 4)

Recorded 2026-11-19 via `gh run list --branch main -L 20` + run logs.

| date | commit | workflow | result | note |
|---|---|---|---|---|
| 2026-11-18 17:01Z | f19b17d (**audit-base**) | CI, Sanitizers (ASan+UBSan), TSan, Codecov | **in_progress >24 h** | Started 2026-11-18T17:01Z, still `in_progress` 2026-11-19 — stale or wedged; audit-base's own CI is **not yet green-confirmed**. Flagged to the human (check/rerun; agent takes no CI write action). |
| 2026-11-18 17:01Z | f19b17d | CodeQL, cppcheck, scan-build, libFuzzer, Scorecard, Deploy docs | success | — |
| 2026-11-16 19:58Z | 163b36a (parent of the kf grave-frontier fix) | Sanitizers (ASan+UBSan) | **failure** | `test-rebuild-validation` exceeded the 1200 s watchdog; `run-all` aborted (exit 124); no sanitizer findings in the log. → AUD-CI-1 family evidence. Run 43944867415. |
| 2026-11-16 19:58Z | 163b36a | CI, TSan, CodeQL, cppcheck, scan-build, libFuzzer, Codecov, Scorecard | success | — |

Notes:
- The CI ASan workflow runs with an `SHARD_TEST_EXCLUDE` list of load-sensitive
  cases (incl. `test-rebuild-txn-crash-after-walk`, several auto-reshard/vacuum
  cases) — CI is *weaker* than the local gate; the campaign's local runs
  (`--jobs 8`, ASan `--jobs 6`, TSan `--jobs 3`) stay the source of truth.
- Known flake AUD-CI-1 is the only red family observed in this window; fix
  assigned to the Phase-4 branch above.

## Mode inventory (Phase 0 step 0 — authoritative, mechanically derived)

Derived from `src/db/server.c` @ audit-base: the JSON dispatch chain in
`dispatch_json_query` (server.c:749; strcmp branches ~783–2292), the admin
classification (`mode_admin_level`, `mode_is_data_write`, `mode_is_write`,
`mode_is_schema`), the legacy fast path (`server_process_fast`, server.c:2433,
strcasecmp chain 2590–2645), NQL (`dispatch_nql_query`, server.c:641; `nql.c`),
and CLI dispatch (`src/db/main.c`). Slice numbers refer to the plan's Phase 3
table.

### JSON wire modes (`{"mode":...}`, `dispatch_json_query`)

| mode | admin level | write class | slice | notes |
|---|---|---|---|---|
| add-token | SERVER (early gate) | admin | 12 | early gate bypasses full scope×perm check (server.c:783); `--dir/--object` targeting |
| remove-token | SERVER (early gate) | admin | 12 | as above |
| add-ip | SERVER (early gate) | admin | 12 | as above |
| remove-ip | SERVER (early gate) | admin | 12 | as above |
| list-tokens | SERVER (early gate) | read | 12 | token-secret exposure check |
| list-ips | SERVER (early gate) | read | 12 | as above |
| add-dir | SERVER (early gate) | admin | 13 (audit with 12) | handover: slice 13; auth matrix in slice 12 |
| remove-dir | SERVER (early gate) | admin | 13 (audit with 12) | as above; destructive (removes tenant dir) |
| db-dirs | SERVER | read | 12 | |
| stats | SERVER | read | 12 | `format:table`/prom variants |
| stats-prom | SERVER | read | 12 | |
| vacuum-check | SERVER | read | 12 | |
| shard-stats (no object) | SERVER | read | 12 | |
| reindex (no args) | SERVER | schema/write | 10 (global form: 13-adjacent) | admin-server; per-object form is slice 10 |
| reindex (dir/obj) | SERVER | schema/write | 10 | strict durable rebuild; bmfix clearing |
| list-objects | SERVER | read | 12 | |
| create-object | TENANT | schema | 11 | |
| drop-object | OBJECT | schema/destroy | 11 | |
| truncate | OBJECT | schema/destroy | 10 | |
| vacuum | OBJECT | schema/rewrite | 10 | |
| backup | OBJECT | schema/read | 10 | takes exclusive object lock (mode_is_schema comment) |
| restore | OBJECT | schema/rewrite | 13 (audit with 10) | handover: slice 13; self-locks (fast-path comment server.c:2559); DL-critical |
| describe-object | NONE | read | 12 | schema/fields/indexes report; not in `mode_admin_level` lists → any reader |
| recount | OBJECT | schema | 10 | v2: O(1) from kf headers |
| add-field | OBJECT | schema | 13 (audit with 11) | handover: slice 13; `edit-field` batch JSON form covers add in slice 11 |
| edit-field | OBJECT | schema | 11 | same-type only (v2) |
| remove-field | OBJECT | schema/destroy | 11 | |
| rename-field | OBJECT | schema | 11 | |
| add-index | OBJECT | schema/rebuild | 11 | unlink+rebuild under wrlock (mode_is_schema comment) |
| remove-index | OBJECT | schema/destroy | 11 | |
| insert | NONE | data-write | 1 | guard_write block (server.c:1644); auto-key interplay |
| update | NONE | data-write | 1 | **JSON-wire only — no CLI command, no fast-path branch** |
| delete | NONE | data-write | 1 | |
| get (single + multi) | NONE | read | 13 (audit with 6) | handover: slice 13; multi-key dict form in slice 6 |
| exists | NONE | read | 6 | single bare bool / multi dict |
| not-exists | NONE | read | 13 (audit with 6) | handover: slice 13 |
| size | NONE | read | 10 (du -sb accounting) | |
| orphaned | NONE | read | 10 | |
| estimate-index | NONE | read | 13 | handover: slice 13; trigram size projection |
| count | NONE | read | 4 | planner shortcuts (INTERSECT/KEYSET) |
| find | NONE | read | 4 | formats rows/csv/dict, order_by, cursor, join (slice 8 sub-slice) |
| keys | NONE | read | 6 | |
| fetch | NONE | read | 6 | typed_read_command CSV route |
| bulk-insert | NONE | data-write | 2 | stages, growth gates, KFM2 markers |
| bulk-insert-delimited | NONE | data-write | 13 (audit with 2) | handover: slice 13 |
| bulk-delete | NONE | data-write | 3 | |
| bulk-update | NONE | data-write | 3 | |
| bulk-update-delimited | NONE | data-write | 13 (audit with 3) | handover: slice 13 |
| put-file | NONE | data-write | 9 | `--if-not-exists` |
| get-file | NONE | read | 9 | |
| get-file-path | NONE | read | 9 | |
| delete-file | NONE | data-write | 9 | |
| list-files | NONE | read | 9 | pattern/match modes |
| aggregate | NONE | read | 7 | group_by, having, top-N |
| sequence | NONE | data-write | 13 | handover: slice 13; `cmd_sequence` (query_maint.c:245): current/reset/next/next-batch; file+flock store |
| shard-stats (with object) | NONE | read | 10 (or 12) | object form |
| *(fallback)* | — | — | 12 | `{"error":"Unknown mode: %s"}` (server.c:2292) |

### Legacy `\x1F`-delimited fast path (`server_process_fast`, server.c:2433)

Third wire grammar: non-`{` lines containing `\x1F` (or any non-NQL text), args
split on `\x1F`, `MAX_ARGS=8`, dir/object validated then `fields.conf` stat'd.
**Not enumerated in the plan text — discovered during this inventory** (see
PLAN NOTES below). Own lock classification (`fast_restore` self-lock special
case, `fast_wr` = mode_is_schema). Commands (strcasecmp chain 2590–2645; all
single-key/arg forms):

| cmd | class | slice |
|---|---|---|
| get, insert, delete, exists, keys, fetch, find, size, orphaned | data / read | 13 surface audit; per-mode semantics live in slices 1/4/6/10 |
| backup, restore, vacuum, recount, truncate, add-index, remove-index | schema/destroy | 13 surface audit; semantics in slices 10/11 |
| bulk-insert, bulk-delete | data-write | 13 surface audit; semantics in slices 2/3 |
| put-file, get-file-path | data-write / read | 13 surface audit; semantics in slice 9 |
| sequence | data-write | 13 |
| *(fallback)* | — | `Error: Unknown command: %s` |

No `update`, `count`, `aggregate`, `list-files`, `delete-file`, `get-file` on
this path. Reachable by any authenticated client and by every fall-through CLI
command (main.c tail → `cmd_query`, server.c:4240).

### NQL text mini-language (`dispatch_nql_query`, server.c:641; `nql.c`)

Lines starting `f`/`c`/`a` without `\x1F` → `nql_parse_command` →
find/count/aggregate with NQL filter grammar (`nql_parse_filter`) and its own
auth path. CLI `explain` builds NQL with `--explain` (main.c:233) and sends it
as a raw line. → **slice 13** (grammar → slice 4/7 semantics; `cmd_explain`
(query_plan.c:2694) is the planner surface used by the Phase-1 differential).

### CLI-only surfaces (`src/db/main.c`; client-side, speak JSON or `\x1F`)

| command | mechanism | slice |
|---|---|---|
| version, query (JSON passthrough), start/server/stop/status | local / lifecycle | 12/13; single-instance guard in slice 2-adjacent storage review |
| explain | NQL w/ `--explain` | 13 (with 4/7) |
| export-schema, import-schema | client-side wrappers (`cmd_export_schema`/`cmd_import_schema`, server.c:4613/4694) over describe-object/create-object | 11 |
| drop / drop-object, list-objects, describe-object/describe, add/remove token, add/remove ip, list tokens/ips, add-dir/remove-dir, edit-field, remove-field, rename-field, find, count, aggregate, put-file, get-file, delete-file, list-files, estimate-index, shard-stats, reindex (incl. `--composites-only`), stats, stats-prom, db-dirs, vacuum-check | JSON builders → `cmd_query_json` | respective mode slices (shape conformance: CLI wrapper vs wire response) |
| insert, get, exists, delete, keys, fetch, size, orphaned, recount, truncate, vacuum, backup, add-index, remove-index, bulk-insert, bulk-delete, sequence | **fall through to `cmd_query` → legacy `\x1F` wire** | 13 (surface) + respective slices |
| *(no `update`)* | — | note: `update` is JSON-wire-only |

### Non-dispatch surfaces

| surface | location | slice |
|---|---|---|
| Embedded API (`shard_db_open`, thread-local `g_db`) | embedded.c | covered inside mode slices per plan; lifecycle init in Phase 2.1 |
| Background auto-vacuum thread | server.c:2822/3347 (`auto_vacuum_sweep_one`, `sweep_all_objects`) | 13 |
| Nightly auto-reshard thread | server.c:3390+ (`auto_reshard_sweep_one`; runs heavy `--splits` under object wrlock) | 13 |
| Server lifecycle: poll accept + thread pool, work queue, graceful shutdown drain (`in_flight_writes`, worker-fd shutdown()) | server.c ~2296–2430, 3400+ | Phase 2.3 (connection lifecycle) |
| Startup: version gate/eligibility, KFM2 replay, bmfix recovery (`bmfix_recover_all`), marker recovery sweep (`g_marker_recovery_ran`) | storage/slotcask/durability | Phase 2.1 (storage & durability) |
| Sequences file store + auto-key generation (uuid/seq) | query_maint.c:245, config.c:987–1013, insert path | 13 (+ slice 1 interplay) |
| Config parsing: db.env, schema.conf (incl. `auto_key=` tail scan), fields.conf, tokens.conf ×3 levels, allowed_ips.conf, dirs.conf | config.c, server.c auth block | Phase 2.2 (dispatch & auth) + 13 |
| Packed CSV read routes (`packed_csv_fetch`/`packed_csv_get_multi` via `typed_read_command.c`) | packed_csv_command.c, typed_read_command.c | 13 (grammar); semantics in slices 6 |
| Trigram index paths | trigram.c + callers (index.c, query.c, query_plan.c, query_maint.c, slotcask.c) | 13 (structure); `contains`/`like` semantics in slice 4 |
| Typed bulk insert engine | typed_bulk_insert.c | 2 |
| TLS wrapper | tls.c/tls.h, tls_stub.c | Phase 2.3 |
| TEST_BUILD-only control: `--test-control-fd` (main.c:131), `shard_test_ctl.h` pause/fail seams + named fault seams (handover list) | test_control.c/h, seam macros | audit tooling surface (Phase 1 fault injection); not production |
| shard-cli (`src/cli/`) | separate binary | **out of scope** (deferred, approved) |
| bench harness (`src/bench/`, `build/bin/shard-db-bench`) | — | out of scope (targets may be added in Phase 1; human runs them) |

## Open items needing the human

**Deferred (2026-11-24, human decision): ordered-walk reopen failure retires a shard silently.**
`btree_walk_ordered_ranges` fails closed when a range's FIRST open fails, but a
reopen after a release (cursor / `order_by` / composite-prefix walks drop their
iterator locks around blocking record fetches) that fails is, by documented
design and a pinned test (`test-range-failed-reopen-retirement`), retired for
the rest of the walk while the surviving ranges continue — so that shard's
remaining rows are silently missing from a response that looks complete. Only
reachable on a mid-query index-file open fault (I/O error, corruption); no
stored data is affected. Decision: defer to the per-mode audit (cursor /
order_by slice) — recommended direction is to fail closed (error, or an
unterminated response once rows are out) and update the pinned test.


1. **Threat-model approval** (above; provisional draft usable for Phase 1).
2. ~~Coverity~~ **nothing to do** — the human triaged every Coverity issue
   (0 open); recorded 2026-11-19, Coverity skipped entirely. Analyzer inputs
   = CodeQL alerts + cppcheck/scan-build CI logs only.
3. **Bug-pair picks** for the planner-differential and fault-injection
   known-bug gates — candidate fix commits (from `git log --grep`,
   2026-11-19):
   - **Planner differential** (cross-shape divergence candidates):
     `e69170b` fix: remap bitmap slots across keyfile resplit;
     `088987a` fix: rebuild transactions record indexes_may_change from begin;
     `334b78d` feat: durability windows + bt↔kf lock-order inversion fix
     (introduced test-bt-kf-inversion-stream-find).
   - **Fault injection** (crash/durability candidates):
     `f3c6ee5` fix: kf grave-run frontier recovery (already the crash
     fuzzer's gate — pick a second pair);
     `18678ff` fix: scope startup bitmap repair to marked shards;
     `6c65490` feat(storage): make indexed writes atomic across crash
     boundaries;
     `334b78d` (as above — usable for either gate).
   The human picks one pair per gate before those gates run.
4. ~~audit-base CI status~~ — resolved per human (2026-11-19): the runs are
   still executing, not wedged; no CI action.
5. ~~Crash/restart known-bug gate decision~~ — **RESOLVED (human option (b),
   2026-11-19); gate ACCEPTED**: the crafted deterministic case
   `audit-craft-grave-frontier` (carries the fix regression's incident
   geometry self-contained — the regression case does not exist at
   163b36a) is **RED on 163b36a and GREEN on 2026.11.2, deterministically**:
   163b36a = 7 identical failures across two runs (s1 head-grave frontier
   parks at 0; D,E,F appends crush live C; refs go dangling; s2
   interior-run frontier mis-park), exit 7 both runs; 2026.11.2 = 62/62
   assertions, both runs. The kill-only modes remain the general fuzzer
   (SHARD_AUDIT_MODE unset); the SIGBUS + 83/150 load-run evidence stays as
   AUD-GATE-1 supplementary material.
   Remaining gate work (human decisions 2–3, 2026-11-19 session — see the
   gate outcomes below):
   - ~~**Planner differential** pair e69170b vs parent~~ — **GATE ACCEPTED
     (2026-11-19)**. New case `audit-gate-resplit-bitmap`
     (`src/test/audit/audit_gate_resplit_bitmap.c`): twins plain vs
     `v:bitmap`, W1 kf seam (`test_env_ctl_set_kf_seams(64,0)` +
     `KF_TEST_INITIAL_SLOTS=64` persisted in db.env for the restart leg,
     see AUD-HARN-2) forces in-place resplits under a 2000-row load;
     per-cat v-eq counts, full key sets, bulk-delete tombstone tracking,
     and a clean restart are all compared cross-shape, with an explain
     assertion (`"index":"bitmap"`) proving the bitmap path is driven.
     Uses only count/find/explain — no aggregate/group_by — so the
     audit-base findings AUD-QRY-1/2 cannot pollute it.
     **RED on e69170b^**: 12 deterministic failures, all per-cat bitmap
     undercounts after the resplit (500 → 153/181/170/153 post-resplit;
     stable after deletes and across a restart) — stale bitmap bits skip
     every moved record, exactly the fixed defect. kf file 2560→12312 B
     proves the resplit fired.
     **GREEN on e69170b** (254/254 ×2) **and on audit-base f19b17d**
     (254/254 ×3).
     Evidence: `docs/plans/audit-artifacts/gateE69170b-parent-red.log`;
     era overlay `audit/scripts/overlay-gate-e69170b.sh` (additive edits
     to the era's own test-infra files — this era drifts ~448 lines from
     main, so nothing is copied wholesale); scratch worktrees
     `../shard-db-gate-e69170b` (parent, faa8340) and
     `../shard-db-gate-e69170b-fix` (fix) left in place until the human
     accepts this gate.
   - ~~**Fault injection** pair~~ — **FINAL VERDICT (2026-11-19): NOT
     REPRODUCIBLE as a red-parent/green-fix gate — recorded, no gate.**
     6c65490 vs parent was already recorded not reproducible (no control
     channel); per the human's fallback the pair became **334b78d vs
     parent** in the human-approved **interposer-only** variant (seam
     scenarios compiled out via `-DSHARD_AUDIT_NO_SEAMS`; they stay on
     current code). Harness gained `SHARD_AUDIT_FAULT_SEAMS=0` (runtime
     switch), kind-op pinning (sticky-fsync → fsync/fdatasync,
     short-write → write/pwrite — the other draws were vacuous), per-op
     tracing + kept-root triage on violation. Matrix **60 seeds × 24
     ops**: **334b78d^ (parent) 60/60 clean**; **334b78d (fix) 45/60 —
     15 "violations", all the AUD-HARN-3 phantom class** (unacked-but-
     applied writes, contract-legal → harness artifact, not a product
     regression). Why no red-on-parent (recorded per the human's
     "record why" instruction): (a) the parent's durability weakness
     (timer-synced writes, pre-KFM2) is unobservable through SIGKILL —
     dirty page cache survives process death and the interposer cannot
     evict it; (b) every interposer I/O fault at this era makes the
     write path **fail closed** (op errors, nothing acked, oracle
     accepts errors by contract); (c) the bug classes 334b78d actually
     fixed (marker replay, bt↔kf inversion deadlock) need machine-crash
     semantics or timed concurrency — outside this harness's reach.
     **The matrix was still productive on current code**: re-running the
     same seeds on audit-base surfaced **AUD-QRY-3** (deterministic
     seed 3: kf deleted-counter underflow → object count reads 0 with
     all records intact — DL-class, Phase-4 fix branch) plus AUD-HARN-3
     (the harness contract gap itself). Evidence:
     `fault-gate-334b78d-parent-60x24.stdout`,
     `fault-gate-334b78d-fix-60x24.stdout` in audit-artifacts. Scratch
     worktrees `../shard-db-gate-334b78d`, `../shard-db-gate-334b78d-fix`
     left in place until the human accepts this (negative) gate outcome.
6. ~~**Planner-differential decision-4 work**~~ — **DONE (2026-11-19)**.
   Tuning applied to `audit_planner_diff.c`: bitmap shape →
   `["v:bitmap","w:bitmap","t"]`, btree shape → `["v","t","w"]`, new field
   w (3 cats), v widened to 5 cats (rows/5 > N/8 budget → robustly broad),
   generator flattened to implicit-AND list elements (see AUD-PLN-2 for
   the nested-AND trap), AND pairs = [t-range,t-range] (B1),
   [v-eq,w-eq] (B3/B6), [u-eq,v-eq] (composite-exact), OR[u,u] for the
   composite union; explain every 2nd criterion alternating find/count
   mode. **Full budget 500 criteria × 4 shapes, seeds 1+2, 5000 rows —
   all five plan paths exercised** (explain window = 250/seed):

   | PLANPATH | plain | btree | bitmap | composite |
   |---|---|---|---|---|
   | scan | 250 / 250 | 57 / 69 | 45 / 56 | 250 / 250 |
   | leaf | 0 / 0 | 111 / 111 | 40 / 43 | 0 / 0 |
   | bitmap | 0 / 0 | 0 / 0 | 66 / 61 | 0 / 0 |
   | intersect | 0 / 0 | 42 / 25 | 59 / 45 | 0 / 0 |
   | union | 0 / 0 | 40 / 45 | 40 / 45 | 0 / 0 |
   (seed 1 / seed 2; composite tallies are all-scan — see AUD-PLN-3.)

   **Verdict: zero unattributed divergence.** 301 + 565 divergences, all
   in the aggregate/group_by sections, 100% matching the two new finding
   signatures: AUD-QRY-1 (bitmap-pair group_by, 148+292) and AUD-QRY-2
   (OR-bearing criteria: aggregate 102+182, btree-pair group_by 51+91).
   count/find/order_by sections: **0 divergences across both seeds**.
   The v1 harness's "0 divergences" was real for its criteria
   distribution — the coverage raise (decision 4's stated condition)
   is exactly what surfaced AUD-QRY-1/2. Log:
   `docs/plans/audit-artifacts/audit-planner-diff-fullbudget-seed1-2.log`.

## DL triad — Phase-4 plan write-ups (2026-11-19, human directive)

Plans written per CORE-PROCESS (Brief + Plan, tagged decisions, test-first
tasks, blind review rounds) — **awaiting human approval before any
execution**:

1. `docs/plans/2026-11-19-audit-qry1-igb-bitmap-group-fallback.md` —
   `fix/audit-igb-bitmap-group-fallback` (AUD-QRY-1).
2. `docs/plans/2026-11-19-audit-qry2-agg-subtree-cover-gate.md` —
   `fix/audit-agg-subtree-cover-gate` (AUD-QRY-2 + the
   `keyset_agg_from_intersect` relocation blocker; also the nL==0
   AND-of-ORs hole). Review surfaced AUD-QRY-4 (find twin) — out of
   scope, own branch.
3. `docs/plans/2026-11-19-audit-qry3-kf-counter-underflow.md` —
   `fix/audit-kf-deleted-underflow` (AUD-QRY-3; includes the
   counter-reader impact inventory the human asked for and the
   NEEDS YOU repair-policy decision).

Review state per plan: **CONVERGED + APPROVED (human, 2026-11-19)**. Round 1
findings fixed (QRY-1: branch-base blocker + fixture major; QRY-2:
intersect-relocation blocker + nL==0 + find-claim majors; QRY-3:
durability blocker + replay-re-entry major), round 2 blind re-reviews all
returned **0 blockers, 0 majors**, and a third independent blind round
revised all three in place (QRY-1: path-limited stash only, pinned
case/fixture names; QRY-2: nL==0 flag only when the AND has a child beyond
the single OR, new large-scale case 9 proving the restore condition, cases
10/11 added; QRY-3: root cause widened to ordinary bitmap-cap rejection —
reuse guard added as Task 2c, test switched to the deterministic bitmap(4)
wire repro with no LD_PRELOAD and no harness dependency, reviewer-measured
red numbers pasted into the review log). Execution order QRY-1 → QRY-2 →
QRY-3 (QRY-3 must build and pass on `main` alone; verify the bitmap-cap
repro actually produces the wrap on base before coding).

Phase-4 plan queue (2026-11-19 human directive, after the triad) — all
five plans WRITTEN and blind-reviewed. **Execution order ruled
2026-11-20**: QRY-2 (amended plan awaiting the human's blind review)
→ QRY-3 → QRY-8 → QRY-5 → QRY-4 → QRY-6 → QRY-7. QRY-1: accepted as
done 2026-11-20 (sanitizer gates once, immediately before its push;
index row stays not-fixed until QRY-2 lands and the planner-diff
bitmap pair reaches zero):

0. `docs/plans/2026-11-19-audit-qry2-agg-subtree-cover-gate.md` —
   `fix/audit-agg-subtree-cover-gate` (AUD-QRY-2, DL-class).
   **AMENDED 2026-11-20** (human ruling after the QRY-1 halt): scope
   extended to `agg_run_plan`'s candidate-driven branches — the
   primary-leaf drive (`agg_prim`, 4023-4063) AND the FP_UNION branch
   (4077-4086; `keyset_agg_from_or` verified no-recheck at 178-194) —
   both refuse `has_subtree`-bearing plans → per-record FULL_SCAN; the
   four false "leaf branch re-checks per record" claims corrected; the
   orprobe repro (14 vs 20) added as Task 1 case 12; Task 4 rewritten
   from verify-only-with-halt to the UNCONDITIONAL fix. **Awaiting the
   human's blind review of the amendment before execution.**
1. `docs/plans/2026-11-19-audit-qry5-igb-bitmap-256-cap.md` —
   `fix/audit-igb-bitmap-256-cap` (AUD-QRY-5, DL-class). **CONVERGED**
   (round 1: 0B/0M/5 minors; round 2: 0B/0M/4 minors — all folded).
   **APPROVED to execute 2026-11-20** (after QRY-3; SECOND of the DL
   pair).
2. `docs/plans/2026-11-19-audit-qry8-composite-exact-agg.md` —
   `fix/audit-composite-exact-agg` (AUD-QRY-8, DL-class). **CONVERGED**
   (round 1: 0B/2M/6 minors — stash list + group-field index fixed;
   round 2: 0B/0M/4 minors — all folded). **APPROVED to execute
   2026-11-20** (after QRY-3; FIRST of the DL pair).
3. `docs/plans/2026-11-19-audit-qry6-igb-bitmap-vlen-cap.md` —
   `fix/audit-igb-bitmap-vlen-cap` (AUD-QRY-6). **CONVERGED** (round 1:
   rewritten — 1 blocker: post-fix outcome is the documented E2BIG
   parity, not "exact groups" — aggregate.md:149-151; 1 major:
   latch-before-bm_n==0-early-out ordering + long-values-only case.
   Round 2: 0B/1M/2m — the ASan red was unproducible from the
   functional fixtures (read end ~6 kB vs the 262,144-byte
   `bm_vals[256][1024]` array; ASan fires only crossing the ARRAY
   end) — separate ASan-only `lv_wide` fixture (bitmap(300), 255
   distinct short + 2 distinct long) + detectability boundary written
   into Task 0; all folded). **2026-11-20 ruling: queue position after
   QRY-4** (with QRY-7).
   **EXECUTED 2026-11-22, every gate green, uncommitted pending
   human raw-diff review** (`fix/audit-igb-bitmap-vlen-cap`): Task 1
   case `test-igb-bitmap-long-value` RED on base exactly as planned
   (case2 lv_bm OK-silent-under-report vs oracle E2BIG; case3 lv_only
   OK-silent-empty; oracle + short-value controls green —
   `AUD-QRY-6-revert-prove.log`); Task 0 ASan red captured at the
   plan's exact target site (`AUD-QRY-6-asan-base.log`:
   stack-buffer-overflow READ in bm_collect_uniq_cb's dedup memcmp,
   query_aggregate.c:4751, via bm_iter_values ← the popcount-branch
   collection at 6111). **Task 0 route finding (recorded, no halt):**
   the plan's detectability arithmetic assumed the long value collects
   at row 255 of the shared array; with uncontrolled key routing the
   per-shard dict walks interleave so longs collect at row
   ~Σ(earlier shards' distinct shorts) — the read end stays inside the
   262,144-byte array and QRY-5's cap latch turns the shape into a
   silent ERR (no report). Root-caused by parsing the on-disk .bm
   dicts (dict entries persist through delete: long-phase rejected
   probe keys had polluted shards 0/4/6 with 2000-byte dict entries).
   Fixture adjusted per the plan's contingency (never the assertions):
   lv_wide probe-inserts candidate keys with a 4-byte value, keeps
   shard-7 hits via shard-stats records deltas, deletes rejects, then
   UPDATEs the 257 kept keys in place — 255 distinct short labels
   first, then W0/W1 (1999-byte actual length, NUL-terminated VLEN
   buffer) — so the last-walked shard's dict ends
   [s000..s254, W0, W1]: W0 stores (truncated) at row 255 and W1's
   dedup memcmp reads 255·1024+1999 = 263,119 > 262,144 → ASan fires.
   Task 2 fix per the plan's DEFAULT: the callback refuses
   `vlen > sizeof(*c->vals)` BEFORE the dedup loop (the loop is the
   OOB site), latching QRY-5's signal; both bitmap IGB blocks' latch
   guards already sit before the bm_n == 0 early-outs (QRY-5's
   placement satisfied the round-1 major ordering — verified, only
   comments extended). Case green 6307/0 (error parity on every
   shape); revert-prove RED 4 exact → GREEN 6307/0; full suite
   `--jobs 8` 698 cases / 2,435,334 assertions / 0 failed; **DIVSEC
   all-zero** (200 criteria × 4 shapes); ASan gate = the fix's
   memory-safety proof (case clean under ASan, no report, no leaks) +
   full `--jobs 6`; TSan `--jobs 3`; changelog entry added.
4. `docs/plans/2026-11-19-audit-qry7-igb-pass1-open-fail.md` —
   `fix/audit-igb-pass1-open-fail` (AUD-QRY-7). **CONVERGED** (round 1:
   rewritten — 2 blockers: the SERIAL Pass-1 fallback ~6799-6801 is the
   route objects <100k take — joined to the fix; deleted-file fault is
   legitimate ENOENT per the lazy-creation truth — fault recast as
   chmod-000 + stat-first template per agg_walk_worker 2374-2392 + EIO
   normalization; 3 majors. Round 2: 0B/1M/3m — the
   serial-route propagation was pinned against a nonexistent `err`
   local (agg_walk_worker uses its `a->err` FIELD; the IGB block has
   none) and the goto-igb_skip hedge leaked hbk/crit_ks/sec_maps by
   skipping the 6906-6914 cleanup — fixed to the 6854-precedent form
   `ctx.agg_error = EIO; aborted = 1; break;`; all folded).
   **2026-11-20 ruling: queue position after QRY-4** (with QRY-6).
   **EXECUTED 2026-11-22, every gate green, uncommitted pending
   human raw-diff review** (`fix/audit-igb-pass1-open-fail`): Task 0
   pins re-verified at current lines (worker open site ~4273, worker
   calloc zero-init ~6676, post-join ~6698, serial loop ~6909 with the
   fail-open `if (!it) continue;`, the agg_walk_worker stat-first
   template ~2381, the in-loop precedent form `ctx.agg_error = ...;
   aborted = 1; break;`, and the restart APIs test_env_stop_keep +
   test_env_start_at per test_durability_ordering.c:446-453). Task 1
   case `test-igb-pass1-open-fail` RED on base exactly as planned
   (case1: faulted pf_ix returns a silently-short OK group list, no
   error object; baseline sanity + pf_plain/count/restore controls
   green — one test draft bug fixed first: the case2 control had
   reused the faulted comparison helper and was rewritten pf_plain-
   only). Task 2 fix per the plan: stat-first in BOTH routes (ENOENT =
   empty shard; existing-file open failure fails closed), worker
   gains `open_errno` (EIO) + `aborted`, post-join consumes it
   first-non-zero-wins into ctx.agg_error (sec_map worker pattern),
   serial route uses the 6854-precedent break form. Case green 57/0
   (pinned text "aggregate storage access failed"); revert-prove
   RED 2 exact → GREEN 57/0; full suite `--jobs 8` 699 cases /
   2,435,391 assertions / 0 failed; **DIVSEC all-zero**; ASan + TSan
   once each (break-based propagation through the existing cleanup —
   no locks/shared state); changelog entry added (behavior-change
   flag included).
5. `docs/plans/2026-11-19-audit-qry4-find-subtree-totals.md` —
   `fix/audit-find-subtree-totals` (AUD-QRY-4 widened). Round 1:
   rewritten (2 blockers: site 3's real blind total is
   keyset_find_from_or's unconditional out_total at query.c:6419, the
   prepared-union route is exact; the null contract was defeated at
   11113/11220; 2 majors). Round 2: 1 blocker + 1 major + 5 minors,
   all folded — B1 reworked the sentinel contract (decline rides
   `find_total_null` ALONE; init stays 0; the SIZE_MAX hard-error
   handled at its source sites; 11272 retained as defense) and M1
   added query.c:9439 (prepared-intersect need_rematch, the bitmap
   twin of site 1). Round 3 (focused re-read of the rework): 1
   blocker + 1 major + 2 minors, all folded — B1r3: the caller-side
   decline decision could not see `small_primary` (an executor local
   at 6442, not a FilterPlan field); fixed with a one-int
   `out_declined` out-channel through the single 11107 call site
   (DEFAULT) or a FilterPlan field, all three conditions tabled inside
   the surviving `want_total && !dl.timed_out` guard; M1r3: the 9439
   fix ships TESTED now (b:bitmap on ft_big + assertion 3b, no-limit
   rows-vs-oracle on the prepared shape); minors: decline-is-
   intermediate clause, guard-wrapper pin. Round 4 (final fold
   check): **CONVERGED** — 0B/0M/1 advisory (caller initializes the
   out-int to 0; folded); all round-3 folds verified closed, the
   fixture element code-valid, assertion 3b's route re-derived
   end-to-end; ready for execution once its base (AUD-QRY-2's
   `FilterPlan.has_subtree`) lands. **2026-11-20 ruling: queue
   position after QRY-8/QRY-5** (earliest allowed once QRY-2 lands).
   **EXECUTED 2026-11-22, every gate green, uncommitted pending
   human raw-diff review** (`fix/audit-find-subtree-totals`): Task 0
   sites re-verified at current lines; Task 1 red on base exactly the
   reviewer's numbers (100 vs 20 streaming gate, 160 vs 106 partial
   union, 6000 vs 3250 wide-AND rows+total, "total":0 vs 27 declined
   class; controls green — `/tmp/qry4-red2.log` shapes, recorded in
   `AUD-QRY-4-revert-prove.log`); Task 2 fix applied in one pass
   (out_declined out-channel + caller decision inside the surviving
   `want_total && !dl.timed_out` guard; 9447 need_rematch
   `|| fp->has_subtree` as defense-in-depth; streaming + leaf-total
   gates exclude has_subtree/postfilter plans; idx_count_for_leaf
   SIZE_MAX → find_total_error handled at both source sites, tail
   check extended; union caller `find_total_null = (fp.has_subtree)`);
   case green 9888/0; revert-prove (sanctioned whole-file query.c
   stash — lift-note: also lifts QRY-8's count-only guard, find reds
   unaffected) RED 5/5 exact → GREEN 9888/0; full suite `--jobs 8`
   697 cases / 2,429,027 assertions / 0 failed; **differential DIVSEC
   all-zero** (count=0 find=0 agg=0 group_by=0 order_by=0, 200
   criteria × 4 shapes, `AUD-QRY-4-diff-divsec.log`). **Case-3b route
   finding (recorded, no halt):** assertion 3b (prepared-intersect
   shape) was unexpectedly GREEN on base — route investigation showed
   bitmap-eq leaves are intersect-ineligible (they become postfilters,
   so n_postfilter > 0 already forces the rematch), making the
   prepared-intersect blindness UNREACHABLE for plain finds; the 9447
   term ships anyway as defense-in-depth (per the plan). ASan+UBSan
   and TSan run this same day (find-orchestration diff, no
   locks/atomics/shared state → once each); changelog entry added.

New finding from the review chain: **AUD-QRY-9** (row above) — the
IGB block's Pass-2 and sec_map fail-open btree opens, planned after
AUD-QRY-7.

## Phase 1 step 5 — coverage baseline (2026-11-19; collection mechanism root-caused, rerun IN PROGRESS)

- **Root cause of the garbage baselines found (supersedes the "parallel
  clobber" hypothesis — that was at most a secondary effect).** Test
  processes chdir into the tmpfs scratch root (`src/test/fixtures.c:548/766`),
  and every `--coverage` object path baked at compile time is RELATIVE
  (build.sh compiles `-o build/obj/x.o`), so each dumping process writes
  its .gcda against its own cwd — inside `/tmp/shard-db-*` — and the
  mandatory `rm -rf /tmp/shard-db-*` deletes them. The suite was green the
  whole time; only the counters were thrown away. The single-invocation
  binaries (daemon, test-server, test, bench — build.sh lines 111/160/168/545)
  additionally carry their gcno/gcda under CWD-relative `<binary>-<tu>.*`
  names, so the old finisher's `build/obj/*.gcda` glob never even saw them.
  Reference profile proving the suite model is sound: CI's codecov job
  (run 44284273009, same `run-all --jobs 1`) reports query_aggregate 72.0%,
  slotcask 84.9%, query.c 70.8%, query_plan 80.7%, total 78.0% lines.
- **Fix (tool-only, this branch), in three parts** (each found by an
  intermediate wrong artifact — recorded so the next coverage run
  doesn't rediscover them):
  1. **GCOV_PREFIX collection** — the serial suite + oracle runs
     execute with `GCOV_PREFIX="$PWD/build/cov-gcda"
     GCOV_PREFIX_STRIP=0`, relocating every dump into one cwd-proof
     tree (495 gcda after the full suite + oracles).
  2. **gcov pairing = staging, not paths** — gcov does NOT read the
     explicitly-passed .gcda path: it re-derives the data file from
     the gcno's recorded object name inside the -o directory
     ("cannot open data file, assuming not executed" when absent;
     "stamp mismatch" when a STALE gcda from an earlier build sits
     there — which is exactly what the first two "real-looking" runs
     actually read). `coverage-baseline-finish.sh` now stages each
     prefix-tree gcda next to its matching gcno (repo root /
     build/obj), runs per-binary-prefix groups (longest-prefix
     assignment; per-prefix staging lists — a bare `shard-db-` glob
     matches every binary), gcovs each group in isolation (per-prefix
     .gcov files avoid cross-variant overwrites), then removes the
     staged copies.
  3. **Known collection limit (recorded, not fixable at test-infra
     level): wire daemons contribute ~nothing** — test teardown
     kills them (SIGKILL/SIGTERM-_exit), so libgcov never dumps; the
     `[shard-db]` variant rows are near-zero by construction
     (slotcask 47.75% = startup/shutdown paths only). The suite's
     coverage carrier is the in-process test binary (`[shard-db-test]`)
     — the same model CI's codecov job has.
- Serial suite under the collection env: **690/690 cases,
  2,416,127 assertions, 0 failed** (~31 min wall — the "expect hours"
  estimate was wrong). Final baseline
  (`docs/plans/audit-artifacts/coverage-baseline.txt`, regenerated
  2026-11-20): per-binary-variant rows, worst-first, + never-executed
  functions — headline `[shard-db-test]`: query_aggregate **69.41%**
  lines, query_plan 70.23%, query.c 59.12%, slotcask 79.50%, btree
  85.37% (CI's codecov union reference: 72.0 / 80.7 / 70.8 / 84.9 /
  89.1 — consistent modulo per-variant vs union accounting and the
  extra oracle cases CI lacks). **21,371 never-executed functions on
  the real artifact** (the preliminary 4,113 was clobbered-data).
  Step-8 input ready. Then rebuild normal (`SKIP_TESTS=1 ./build.sh`).
- Step-8 note (unchanged): `bench_stats.c`/`bench_table.c` (no
  TEST_REGISTER) remain confirmed candidates; the real never-executed
  list is in the regenerated artifact.

## Phase 1 steps 7 + 9 — profiling prep + soak prep (2026-11-19, scripts authored; the human runs them)

- `audit/scripts/perf-bench.sh <bench-name>` — perf record (999 Hz, -g)
  around one registered bench + flat `perf report --stdio` into a
  timestamped perf-report-*.txt. Requires perf; hot paths per mode get
  recorded into this index as the human pastes numbers.
- `audit/scripts/soak.sh <seconds>` — mixed-workload soak on a throwaway
  root: 4 request classes per round (insert/update/bulk-insert/delete),
  3 readers (count/find/aggregate group_by), vacuum every 25 rounds,
  reindex every 200, clean restart every 500, request failures captured
  to soak-req-errors.log. TSAN variant: build tsan, run with reduced
  concurrency per AGENTS. Runs overnight by the human; results recorded
  in this index (Phase 5 readiness evidence).

## Phase 1 step 6 — bench inventory (2026-11-19)

Registered benches (12, mechanically derived from TEST_REGISTER in
src/bench/): bench-bitmap-vs-btree, bench-btree, bench-cache-pollution,
bench-grow, bench-incremental, bench-invoice, bench-joins, bench-kv,
bench-kv-parallel, bench-parallel, bench-queries, bench-trigram-sizing.
(AgentS.md's "eight registered benches" list is stale — docs note for the
human; not edited.)

Dead bench sources (step-8 candidates, listed not deleted):
`src/bench/bench_stats.c`, `src/bench/bench_table.c` — no TEST_REGISTER,
not runnable. Risk: none at runtime (never registered); confirm no
`#include` pulls them into other TUs before removal.

Modes WITHOUT a bench (work items; new targets get built in step 6, run by
the human): aggregate (group_by/having/top-N — bench-queries covers find +
add-index only), count (criteria + O(1)), CAS family (if/if_not_exists/
if_match/$incr/$copy/return_record), bulk-update, cursor pagination,
vacuum/vacuum-check, backup/restore, reindex, recount/orphaned/truncate/
size, schema mutations (create-object/drop-object/edit-field/rename-field),
NQL text path, TLS throughput, delimited bulk, estimate-index, sequence/
auto-key. Priority for new targets: aggregate-group_by (AUD-QRY-1/2 land
in its executors — perf proof needed after those fixes), vacuum, CAS,
bulk-update, cursor.

## Phase 1 step 3 — fuzzing status (2026-11-19, in progress)

New harnesses built, validated, and wired (`fuzz/build.sh`,
`fuzz/README.md`, `.github/workflows/fuzz.yml`, seed corpora under
`fuzz/corpora/<target>/`):

| harness | target | smoke result |
|---|---|---|
| `fuzz_filename` | valid_filename / is_valid_object / dir_name_ok + traversal invariant | 2.6M execs/30s, no findings |
| `fuzz_cursor` | parse_cursor_object (via `#include "query.c"`) | 2.1M execs/30s, no findings |
| `fuzz_frame` | request-framing worker loop + real legacy dispatcher, end-to-end over a socketpair | 4.3-4.7k execs/s at 150-240s; **found AUD-RES-1** (NQL parse leak, deterministic) |

Symbol identification (step-3 first sub-step) recorded in `fuzz/README.md`
(framing loop = inline in static `worker_thread`; re-parse sites =
describe-export / list-snapshot / get-file base64 — those three need a
connected two-role harness and are the remaining step-3 item, next
session). Harness-authoring notes: fuzz_criteria/fuzz_filename were
silently unbuildable on this workstation pre-campaign (`-latomic`
unresolvable — Arch keeps libatomic in the gcc toolchain dir; build.sh now
locates it), and fuzz_frame required per-thread `g_db` binding + a
per-input `max_request_size` re-pin (dispatched requests reload config and
reset the knob mid-connection-set).

## Phase 1 step 4 — oracle harness status (2026-11-19)

Branch `test/audit-oracle-harnesses` (uncommitted; raw-diff review pending).
All audit cases excluded from default `run-all` (`TEST_REGISTER_AUDIT` +
`--include-audit`); bench caller updated mechanically; CI workflows untouched.

| harness | location | smoke result | notes |
|---|---|---|---|
| crash/restart model fuzzer | `src/test/audit/audit_crash_restart.c` (+ `audit_common.h`) | fixed build: 2/2 seeds green; 163b36a: base-only failures + SIGBUS (AUD-GATE-1) | uniform + biased (parked-at-P, chained same-stream) kills; per-replica models; observed-value pending resolution; in-process `validate_live_refs` oracle |
| planner differential | `src/test/audit/audit_planner_diff.c` | **tuned + full budget done 2026-11-19**: 500 criteria × 4 shapes × seeds 1–2, all 5 PLANPATH paths exercised; every divergence attributed to AUD-QRY-1/2 (see decision-4 outcome above) | twin shapes: plain / btree(v,t,w) / bitmap(v:bitmap,w:bitmap,t) / composite(u+v); count/find-set/aggregate-scalar/group_by/order_by=t equality; per-section DIVSEC tally + PLANPATH tally; explain alternates find/count mode every 2nd criterion; criteria generator flattened to implicit-AND elements (AUD-PLN-2) |
| fault injection | `src/test/audit/audit_fault_inject.c` + `audit/faultlib.c` | era matrices (60 seeds × 24 ops, interposer-only): 334b78d^ 60/60 clean; 334b78d fix 45/60 (15 × AUD-HARN-3 phantom artifact); **current code 13/15 on seeds 1–15 — 2 real: AUD-QRY-3 (seed 3, deterministic count-zero) + 1 AUD-HARN-3 phantom** → gate recorded not-reproducible (open item 5); harness remains the general fuzzer | interposer: write/pwrite/fsync/fdatasync/open/mmap faults (nth/sticky/errno/short-write; sticky-fsync and short-write kinds pin their op — the other draws were vacuous); engine seams: fail-phase sticky, bmfix-fault (compiled out via `-DSHARD_AUDIT_NO_SEAMS` / runtime `SHARD_AUDIT_FAULT_SEAMS=0`); per-op trace + kept-root triage dump on violation |
| resplit-bitmap gate case | `src/test/audit/audit_gate_resplit_bitmap.c` | **ACCEPTED planner gate**: RED e69170b^ (12 failures) / GREEN e69170b + f19b17d (254/254 ×2 and ×3) | twins plain/v:bitmap; W1 seam forces resplit ( kf 2560→12312 B asserted); per-cat count + keyset + tombstone + restart equality with explain bitmap-usage proof; count/find only (immune to AUD-QRY-1/2) |
| gate tooling | `audit/scripts/overlay-gate-163.sh` + `run-gate-163.sh` (163b36a); **new:** `overlay-gate-e69170b.sh`, `overlay-gate-334b78d.sh` (additive era edits — these eras drift 448/865 lines from main, nothing copied wholesale); scratch worktrees `../shard-db-gate-163`, `../shard-db-gate-e69170b`, `../shard-db-gate-e69170b-fix`, `../shard-db-gate-334b78d`, `../shard-db-gate-334b78d-fix` | overlays verified by build + run on each side | gate invocation batches seeds (crash-contained); worktrees left in place until the human accepts the respective gates |

Harness gaps (recorded, deliberately deferred): planner differential does not
yet exercise cursor/join variants (v1 note in file header); faultlib has no
malloc hook (dlsym bootstrap — engine OOM seams cover allocation failure);
full-budget runner scripts (2000-seed crash budget, 500-criteria differential,
seam × 20 fault budget, soak) land with Phase-1 steps 5–9.

Default-suite confirmation after tool wiring (`run-all --jobs 8`, 2026-11-19):
**690 cases, 2,416,111 assertions passed, 3 failed** — all three in
`test-bulk-stage-ungrown-backoff` ("retry after the back-off succeeds within
bounded wait" → `got -1`, two cascading count checks), i.e. a bounded-wait
timeout under all-core load; the case passes solo (61/61) and neither it nor
its inputs are touched by the tooling branch (`git diff main` touches only
test-infra files). Recorded as AUD-CI-1 family evidence (fixed bounded waits
vs load); flagged for the same Phase-4 fix branch.

## Phase 1 steps 1-2 — lint + static analyzers (2026-11-25)

- `BUILD_MODE=lint` added to `build.sh` (audit-only, never shipped).
  584 unique warnings; dispositioned by class in
  `docs/plans/audit-artifacts/lint-triage.md` (sign-conversion/conversion/
  shadow/unused = benign-by-design; maybe-uninitialized = false positive).
- gcc `-fanalyzer` + clang-tidy on util/config/server (+ clang-analyzer on
  nql/schema_wire/query_bulk). Two unchecked mallocs fixed in place
  (config.c typed field decode, server.c worker table).
- **AUD-BULK-OOM (minor, not DL; bulk-insert slice):** `query_bulk.c`
  `arena_new`/`calloc(records)`/`arena_alloc` results are used unchecked in
  both bulk-insert parsers (1678, 2444 and the arena_alloc sites): an
  allocation failure crashes the daemon instead of returning an error.
  No test for allocation-failure paths exists (also listed under the
  existing no-alloc-failure-test gap).
- **AUD-BULK-DEL-K (minor, Phase 2 area 1; traced 2026-11-25, see 2026-11-25-audit-storage-k-reentry.md; found by the Opus blind review of
  AUD-QRY-11):** `bulk_apply_and_sync_kf_locked`'s delete branch is not
  idempotent on K re-entry: `kf_tombstone_entry_locked` returns -1 when the
  key's slot is already tombstoned, so a delete window whose first K
  tombstoned and then failed msync fails again on the inline replay
  (rc -2 / durability degraded; the marker gate finishes it). Counters stay
  correct. Fix: treat "slot already flag==2 at the expected old slot" as
  done on re-entry; add a delete variant of the K-failure test.
- **AUD-BULK-DEFER-CONT (minor, Phase 2 area 1; traced correct, untested, see 2026-11-25-audit-storage-k-reentry.md):** `slotcask_bulk_finalize_shard`
  continues to later windows after one window failed finalize twice;
  correctness rests on the gate replay's lookup-based recovery. No
  counter corruption found; no test covers it.
- **AUD-KF-RESPLIT-SYNC (suspected major, Phase 4; Phase 2 area 1 part 2,
  2026-11-25):** `kfcache_resplit_locked` ignores the Phase C `msync` result
  and a failed post-rename kf-dir fsync (the latter only guards bitmaps), so
  a resplit can publish an unsynced keyfile. Details and proposed fix/test in
  `2026-11-25-audit-storage-k-reentry.md`. Repro `audit-repro-resplit-msync`
  is red on the current tree (engine proceeds after the injected msync EIO);
  a second reviewer must still confirm severity before it counts as major.
- **AUD-VAC-KF-MARKER (DL, confirmed; Phase 2 area 1 part 3, 2026-11-25):**
  light `vacuum` (manual and nightly) skips segment compaction while KFM2
  markers are pending but still compacts the keyfiles, moving slots that the
  retained update marker names; the later replay repoints the wrong slot and
  tombstones the real record. Repro `audit-repro-vacuum-marker` red: updated
  key unreadable, checked walk EUCLEAN. Fix plan pending your approval.
  Sibling unverified: `rebuild_object_v2` with markers pending.
- **AUD-AUTH-1 (CRITICAL, confirmed; Phase 2 area 2, 2026-11-25):** the legacy
  `\x1F` text protocol on the main port is executed with no token and no
  IP check (get/insert/delete/truncate/vacuum/backup/restore --force ...).
  Live-confirmed in strict mode; repro `audit-repro-text-protocol-auth` red.
  See `2026-11-25-audit-dispatch-auth.md`. Fixed 2026-11-25 (option 2, trusted IPs only), uncommitted, awaiting blind review.
- **AUD-AUTH-2 (major, confirmed):** token and IP hash sets remove without a
  tombstone, breaking probe chains; colliding tokens stop authenticating,
  cannot be revoked at runtime, and revive after restart. Repro
  `audit-repro-token-chain` red.
- **AUD-AUTH-3 (minor-major):** `tokens.conf` / `allowed_ips.conf` saved in
  place, no fsync/atomic rename/0600, errors ignored.
- **AUD-AUTH-4 (minor):** `list-tokens` fingerprint leaks 8 chars of a token.
- **AUD-CONN-1 (major, confirmed; Phase 2 area 3):** no socket/idle timeout
  and one worker per connection for its lifetime: WORKERS idle
  unauthenticated connections make the server unresponsive. Repro
  `audit-repro-idle-connections` red. See
  `2026-11-25-audit-connection-lifecycle.md`.
- **AUD-CONN-2 (minor):** `client_ip` defaults to 127.0.0.1 if `getpeername`
  fails (fails open while localhost trust is on).
- **AUD-FILE-1 (CRITICAL, confirmed; Phase 2 area 4):** `file`/`path` params of
  bulk-insert/-delimited/-delete/-update/-update-delimited and put-file read
  arbitrary server files for any write token; an object-scoped rw token read
  the global admin token via tokens.conf. Repro `audit-repro-server-file-read`
  red. See `2026-11-25-audit-caps-and-files.md`. Fixed 2026-11-25 (option 1: trusted IP or global rwx token), uncommitted.
- **AUD-CAP-1 (major, proof trace):** the query slot cap covers only the JSON
  dispatcher; NQL and the text path are uncapped.
- **Phase 2 areas 5-6 (2026-11-25):** lock order and planner reviewed against
  Phase 1 evidence; no new defect; see `2026-11-25-audit-locks-and-planner.md`.
- **Phase 2 summary and fix queue (2026-11-25):** `2026-11-25-audit-phase2-summary.md`.
  **Phase 3 walkthrough plan:** `2026-11-25-audit-phase3-walkthrough-plan.md`.
- **AUD-NQL-DIR (minor):** `dispatch_nql_query` does not call `is_valid_dir`.
- **AUD-COMPACT-ORDER (suspected major, power-loss only; Phase 2 area 1
  part 4):** segment compaction unlinks the donor file before the kf repoint
  is durable. See the findings doc; needs an ordering-oracle repro.
- **AUD-BACKUP-SILENT (major; proof trace):** `cmd_backup`/`cmd_restore`
  swallow every copy error (`cprf` is void), never fsync, and restore wipes
  the live subtrees before copying; both report success regardless.
- **AUD-REPLAY-BOUNDS (minor hardening):** replay segment-flag helpers index
  `map[offset+18]` without a bounds check. See the findings doc.
- AUD-QRY-11 test gap: only the legacy window path is covered, not the
  deferred `bulk_finalize_one_kf_window` retry.
- Needs-look rows for the dispatch/auth slice: 60x `cert-err34-c`
  (atoi/strtol without error checks) in config.c/util.c; nql.c `ferr`
  branch warnings.

## Phase 1 step 3 — export-path fuzz (2026-11-25)

`fuzz_export` added (build.sh, README, fuzz.yml, seed corpus
`fuzz/corpora/export/`): the export / get-file response re-parse steps
(walk_string_array, walk_object_array, field_obj_to_spec, get-file
`"data"` + b64_decode). 60 s smoke: 2.29M execs, no findings. The
`query_collect` socket round-trip itself is not fuzzed (the responses
come from the daemon's own server). Step 3 is now complete except the
plateau runs, which are the human's/CI's wall time.

## Phase 1 step 8 — code-reduction candidates (2026-11-25)

Artifact: `docs/plans/audit-artifacts/code-reduction-candidates.md`.
**AUD-REDUCE-1 (refactor, Phase 4 lane):** 31 caller-less functions
(~560 lines; recovery-adjacent ones held), duplication hot spots in
`query_bulk.c` and `slotcask.c` (hold until oracle-covered), size hotspots
listed. No code deleted.

## Phase 1 leftovers executed (2026-11-25)

- **AUD-QRY-11 (major, not DL): EXECUTED.** `kf_commit_planned_slot` is now
  idempotent for an already-committed slot (same hash + location). New case
  `test-kf-window-replay-double-booking` (in-process, legacy window path, one
  K failure): red on base (total 2 vs 1; reuse live 2 vs 1), green fixed.
  Full suite 714/0. Sanitizer gate: covered by the single pre-push run.
- **AUD-RES-1 (major): EXECUTED.** `read_list` frees the partial IN-list on
  every error path. New case `test-nql-filter-parse-error-leak`
  (mallinfo2 growth; also an LSan signal under the asan gate): red on base
  (704,000 bytes over 4000 parses), green fixed (0). Replaying
  `AUD-RES-1-leak-input.bin` through `fuzz_frame` is the original repro.
- AGENTS.md bench note updated (12 registered benches).
- **Phase 3 slice 1 findings:** FIXED on `fix/single-write-audit-slice1` (uncommitted,
  plan `2026-11-28-audit-slice1-single-write-fixes.md`): AUD-P3-1 (double reply on
  guarded bad auto-key), AUD-P3-2 (empty key accepted, breaks keys/fetch), AUD-P3-3
  (oversize key: empty reply), AUD-P3-7 (unknown/removed field names ignored),
  AUD-P3-9 (single-write `dry_run` contradicted the docs and under-validated),
  AUD-P3-10 (delete error escaping and `return_record` drops). OPEN: AUD-P3-4 (find
  double-escapes keys; slices 8/10), AUD-P3-8 (pending-durability reply unreachable;
  needs a fault-injection repro and its own plan). Known unrelated bug noted:
  `src/cli/main.c:419` builds the CLI single insert with `"data"` instead of `"value"`.
  Detail: `2026-11-25-audit-single-writes.md`.
- **Pending findings from the legacy-removal plan reviews (for the schema slice, 5):**
  AUD-P3-5 (silent-damage, pre-existing): `cmd_drop_object` removes the object
  tree before stripping its schema.conf line, and swallows a failed rewrite;
  a crash or failed rewrite leaves a stale line that the next `create-object`
  of the same name keeps, so the new object runs with the old splits/streams/
  max_key. AUD-P3-6 (pre-existing): `cmd_restore` can write a schema line the
  startup parser refuses (`bak_streams` 0 from a hand-made `object.json`).
  **Fixed for the restore path 2026-11-26** (`2026-11-26-fix-restore-swap-and-kf-dirsync.md`):
  the line is installed only after the commit, and a manifest without a valid
  `storage_version`/`streams` is refused when the line must be recreated. The
  drop-object half (AUD-P3-5) remains open.
- **Pre-merge blind review of the audit branch (2026-11-26), production diff:**
  confirmed by reading `query_maint.c` unless marked unverified.
  - AUD-RESTORE-B1 (blocker, silent-damage; mine, commit 8bbc821): restore success path
    does `rmrf(stage)` with the SWAP marker still inside; a kill mid-delete leaves SWAP
    plus a half-deleted `old/`, and `maint_restore_swap_recover` then deletes the live
    restored tree and renames the half-deleted pre-restore tree into place. Fix:
    `unlink(SWAP)` + `fsync_dir(stage)` before `rmrf(stage)`; recovery deletes
    marker-less staging without rollback. **Fixed 2026-11-26** (the commit point is the
    durable SWAP removal; stage cleanup is best-effort).
  - AUD-RESTORE-M1 (major, silent-damage): on `fsync_dir(obj_dir)` failure the restored
    object is live but reported failed, marker kept; acknowledged writes are then dropped
    by the next startup/restore rollback. Fix: retry the dir fsync, or roll back now.
    **Fixed 2026-11-26** (a failed commit sync rolls back and replies "rolled back").
  - AUD-RESTORE-M2 (major, silent-damage): `ensure_schema_conf_line` and
    `copy_file(fields.conf)` run before staging (return value ignored), so a staging
    failure says "the live object was not changed" while schema/fields were rewritten.
    Fix: apply both after the swap, include in rollback, check the return.
    **Fixed 2026-11-26** (`fields.conf` is swapped with the data; the schema line and a
    live-absent `fields.conf` are installed only after the commit, checked).
  - AUD-KF-DIRSYNC (major, low probability; renamed from -EVICT; mine, 351c086): the
    `dir_unsynced` flag lived in the kfcache entry, and Phase E never updated
    `file_dev`/`file_ino`, so the next acquire dropped and reopened the entry and
    reinstalled it clean; eviction and prefix invalidation lost it the same way, and a
    later write was acknowledged without the directory fsync. **Fixed 2026-11-26** (a
    process-wide pending table keyed by keyfile path + Phase E records the new inode;
    sticky overflow syncs every keyfile's directory).
  - AUD-RESTORE-BMCACHE (major, silent-damage; found while planning the restore fix):
    restore dropped the btree cache but not the bitmap cache, and the bitmap cache has no
    inode check, so a post-restore counted query could serve pre-restore bits.
    **Fixed 2026-11-26** (`restore_invalidate_all` also calls `bm_cache_invalidate`).
  - AUD-BT-REOPEN (major, pre-existing; unverified): `btree_walk_ordered_ranges`
    retires a cursor silently when a reopen fails after a release; the walk returns 0
    with a short page.
  - Minors (unverified): zeroed header read as absent shard (btree.c, io_direct.c);
    card_est ignores walk errors; ENOBUFS maps to "index storage error"; abandoned-
    reservation tombstone zeroes the location of a reused tombstone; resplit vs
    pending markers; per-chunk full-prefix msync; compact_sync_all_kf per donor; OOM
    leaks in bulk-insert parse; add-token memory vs disk on failed save; token
    tombstones never reclaim; error strings missing from error-codes.md.
  Test-code review: no blockers/majors; minors listed in the conversation (assertions
  that cannot fail, root-vacuous pass, cwd-relative binary, nested audit run, 3.6 MB of
  artifacts). **Addressed 2026-11-26** (T5 of the restore/kf-dirsync plan).
  - Notes for the restore/kf-dirsync fix (2026-11-26): an object whose schema.conf line
    is missing is not visited by startup recovery (startup walks schema.conf), so its
    stage is only handled by `cmd_restore`'s own recovery call — the object is not
    servable without its line anyway. The keyfile pending-directory-sync table is never
    pruned for dropped objects (bounded at 64 entries + sticky overflow; perf only, out
    of scope).
- **AUD-RESTORE-SPLITS (pre-existing, silent-damage; for the maintenance slice):** `cmd_restore`
  with `force` and a backup whose splits/max_key differ copies the backup's keyfile shards under
  the unchanged live schema line, silently breaking shard routing; refusing is not enough because
  auto-reshard widens splits nightly (default on), so older backups would become unrestorable;
  needs its own design (restore + reshard/reindex, or restore into a new object).

## Finding found during the legacy-removal T0.5 halt (2026-11-27)

- **AUD-PACKED-BULKUPDATE (major, pre-existing, product gap):** `cmd_bulk_update`
  (`query_bulk.c`) refuses every packed object with `{"error":"packed criteria updates
  await query integration"}`, so `bulk-update` with `criteria` (including `dry_run`) does not
  work on the default and, after the gate, only layout, while `docs/query-protocol/bulk.md`
  documents it as supported. Per-key (array) bulk-update is separate. Packed computed
  updates (`$incr` on numeric renders `136.00`, different error texts) also differ from the
  legacy route. Needs a feature plan; the legacy-removal refactor pins the refusal with a test
  and does not fix it. Add to the slice-3 (bulk-update) walkthrough.

- **AUD-PACKED-GAPS (found at legacy-removal T0.5, 2026-11-27; packed behaviours that differ
  from the legacy route, none fixed by the refactor; pinned or recorded in the `-packed` test
  copies; add to the slice-3 bulk walkthrough):**
  - per-key bulk `if` refuses with `packed bulk conditions await integration`;
  - per-record computed bulk update returns `invalid packed update`; computed-update error
    text collapses source/policy detail to `malformed or incompatible computed value`;
  - bulk-insert-delimited drops a row whose middle cell is empty (legacy zero-filled it);
  - per-key bulk-update reports a rejected NULL patch in `errors[]`, not `skipped`, and a
    malformed input rejects the whole request (`invalid packed bulk input`) instead of
    skipping the bad record;
  - the aggregate limiter picks a different group subset than legacy; sum/avg render
    exact-decimal (`10000.00`, 16-decimal avg);
  - no packed equivalent exists for the legacy composite-key bounds, arena-OOM and per-key
    OOM fault-injection tests (`test-composite-varchar-bound`, `test-bulk-insert-arena-oom`,
    `test-bulk-update-json-oom`), nor for the criteria-timeout route (`test-request-timeout`);
  - `test_scalar_conversion_safety` k7: the packed "scale out of range" rejection is no longer
    exercised (input changed to hit `numeric field overflow`).

## CI flakiness (noted 2026-11-28, after legacy-removal merge)

- **AUD-CI-FLAKES (minor, pre-existing, also on `main`):** macOS `test-rebuild-txn-*` cases
  miss their pause point intermittently, and TSan `test-rebuild-recovery` hangs
  intermittently. Neither is caused by the legacy-removal refactor (T0.5 converted those
  tests to packed without changing their pause/recovery logic). Root-cause or time-box and
  escalate before the next release; do not rerun until green.
