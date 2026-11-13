# Plan G — running reservation count (F9), docs reconciliation (F10), startup `.bm.remap` sweep pins (F11)

## Brief

**Primary goal:** Close the last three P3 rows of `docs/plans/2026-11-06-bitmap-resplit-followups.md` (F9, F10, F11) with tests that are *provably* load-bearing, and leave the user-facing docs true after Plans A–F.

**Problem and root cause**
- **F9.** `kf_plan_window_insert_slot` (`src/db/slotcask.c`) asks `req_res_empty(res_map, res_cap)` — a linear scan of the whole `ReqSlotRes` map — to decide whether the request holds planned-but-uncommitted slots on the shard (the "never resplit mid-request" gate). The answer is already known: every successful reservation goes through one `req_res_insert` call site, and the chain-seed step is the only other writer. A running count in `ReqShard` answers the same question in O(1). The 2026-11-06 draft was written before Plans B and E and misses the chain-seed writer (`req_shard_seed_chain_reservations` replaces `res_map` wholesale), which a naive "increment at the insert site" count would let drift. **Honest value note:** after Plan B's stage pre-grow, the scan is unreachable on the deferred-request path in production (pre-grow keeps projected load < 75 %, so the gate's load condition is false at plan time) and O(1) on the legacy path (`res_map == NULL`). F9 is therefore a hygiene/robustness change, not a measurable speed-up (derived from the code; benches are out of bounds). See decision 1.
- **F10.** Re-checked against the current tree: AGENTS.md's growth clause is already conditional (Plan B); `concurrency.md` already states the bitmap-handle rule (Plan D: cached-only; only an uninitialized cache — test fixtures — hands out uncached reader handles; the former "uncached limit" no longer exists) and the 5 s back-off / `BULK_STAGE_UNGROWN` behaviour; the changelog Unreleased section carries Plans A–E. **Remaining stragglers:** `docs/concepts/storage-model.md` and `docs/reference/limits.md` still say "inserts never refuse for capacity" unconditionally, and `docs/query-protocol/bulk.md` does not tell clients that a bulk stage can fail closed and must be retried.
- **F11.** Plan E reshaped `bmfix_recover_all` (`src/db/embedded.c`). Re-derived: the sweep `slotcask_bmfix_sweep` still runs for every object with no pending marker (including flagged ones); objects *with* markers get their `.bm.remap` temporaries removed by `slotcask_bmfix_clear` (called after a successful repair or by a full reindex). The only path that skips both is "cannot read the disabled flag" (object stays disabled, repair not attempted — harmless disk use until reindex). **No production change is needed.** Neither branch is pinned by any test (existing cases only assert *no* leftovers after in-process faults). Add two pinning cases.

**Logical flow**
1. Scaffold on the unmodified production logic: `ReqShard.res_count` (declared, unmaintained), TEST_BUILD counters, a TEST_BUILD verifier that compares the count with a full scan at every gate evaluation, chain seed and shard release. Write the three F9 cases and the two F11 cases. F9 cases are RED on this scaffold (count never maintained); F11 cases are GREEN-by-design.
2. Production switch: maintain the count at its four write sites; the gate reads it; delete `req_res_empty`.
3. Docs (F10), same plan.
4. Proofs: three hand-reverted F9 mutations, two F11 mutations; full suite.

**Decisions made**

| Decision | Alternatives considered | Why | Tag |
|---|---|---|---|
| **F9 scope.** Implement the running count as you asked, with a drift detector. | (B) Close F9 as obsolete: after Plan B the scan is unreachable in production, so the change buys nothing measurable and adds state that can drift. Drop Phases 1–2 F9 work, keep F10/F11. | You asked for F9; the TEST_BUILD verifier makes drift a test failure, not a latent bug. Option B is the lower-risk choice if you value "no new state" over finishing the ledger row. | **NEEDS YOU** |
| **G-restore (carried from the Plan E review) is NOT in this plan.** `cmd_restore` (`query_maint.c`) arms `g_bmfix_any_disabled` only when the restored `indexes/` carries `.bmfix-disabled`; a backup carrying `.bmfix-NNN` markers but no flag is served with bitmap planning ON until the next restart repairs it. Still present at HEAD. | Fold into this plan; or its own plan before the branch gate. | You scoped Plan G to F9–F11. It is a pre-existing correctness gap that needs its own design (write flag + arm gate vs. repair inside restore), regression test and docs. Recommend a dedicated plan before the sanitizer gate. | **NEEDS YOU** |
| `res_count` is assigned `src_n + dst_n` in the chain seed (both values are already computed there by `req_res_count`), not recomputed. | Re-scan after the copy | `req_res_copy` fails on a duplicate slot before the assignment, so on success the sum is exact. | DEFAULT |
| Drift detector is TEST_BUILD-only and compiled out of production. It scans only at window starts (`nreserved == 0`), at chain seed, and at shard release (O(cap) × windows — negligible even for large suite cases). | Verify on every plan call (quadratic) | Cheap, deterministic, and fires in the same three places the count can go wrong. | DEFAULT |
| The gate and the verifier share one variable (`res_clear = (res_count == 0)`), so a mutation of the gate decision is caught, not only a drifting count. | Separate expressions | Without sharing, mutating the gate to `1` would pass the verifier. | DEFAULT |
| F9 tests are in-process (`slotcask_bulk_request_execute`), reusing the `test_bulk_chain_pregrow.c` choreography scaffold, with `bulk_commit_window = 16` so one request spans several windows, and a chain case on shard 300 of 512 (index ≥ 256). | Wire-level daemon test with a log grep (stale draft) | The draft's `REQ-RES-SCAN` log-absence assertion would pass on its own setup after the fix. Counters give a non-vacuous positive signal. | DEFAULT |
| F11: no production change; two cases appended to `test_bitmap_resplit.c` (own daemons, 512 splits, shard 300/511, planted temporaries, a name-suffix negative control, inode snapshot proving only the temporaries were touched). | A unit test of `slotcask_bmfix_sweep` | The startup wiring (`bmfix_recover_all`) is what Plan E changed; only a restart exercises it. | DEFAULT |
| F10 edits: `storage-model.md`, `limits.md`, `bulk.md`, plus a one-clause fix to the Unreleased changelog sentence that still says inserts never refuse for capacity (exact strings below). No new changelog entry (no behaviour change; F9 is internal). `concurrency.md`/AGENTS.md verified true, not edited. | Add a changelog paragraph | CORE-PROCESS requires docs in the same plan, and nothing user-visible changed. | DEFAULT |
| The stale draft `2026-11-06-res-count-docs-remap-sweep.md` is superseded by this file and left in place. | Delete it | Deleting is your call (untracked; no git write needed). | DEFAULT |

**Blast radius:** `slotcask.c` only for production logic — `ReqShard` (file-private struct), `kf_plan_window_insert_slot` (static, one caller), `req_res_empty` (static, one caller, deleted), `req_shard_seed_chain_reservations` (static, one caller), `slotcask_bulk_shard_release` (static), `slotcask_bulk_stage_shard`. TEST_BUILD-only: `shard_test_ctl.h` (3 counters + reset). `build.sh` (1 new test). Tests: 1 new file, 2 cases appended to `test_bitmap_resplit.c`. Docs: 4 files. No wire, CLI, config, or on-disk change; non-TEST_BUILD binaries compile the same behaviour (the gate answer is identical by invariant I1).

**Test strategy**
- F9: 3 in-process cases (chain on shard 0/8; chain on shard 300/512; single multi-window request on shard 300/512) assert the verifier ran (non-vacuous), saw non-empty maps (cross-window and seeded), and found **zero** count/gate drift. Red on the scaffold (count unmaintained); three hand-reverted mutations prove each write site and the gate are load-bearing.
- F11: two restart cases (n == 0 branch: sweep; n > 0 branch: repair clears) with planted temporaries, a control that must survive, and an oracle check. Green on base by design; mutations s1/s2 prove each branch.
- F10: grep evidence before/after; no test.

**Out of scope:** G-restore (decision 2); Plan H/I work; benchmarks; sanitizers (the branch gate runs once after Plan I); committing; editing the follow-ups ledger; the "flag unreadable" sweep skip (harmless, documented here only).

**Review log:** see the end of this file.

---

## Executor Plan

Planner: Claude Sonnet 5.5. Executor: GLM 5.3 Flash (per the human). Derived against local HEAD `6b68518` (Plans A–F committed) on `fix/type-correctness-recovery`. Proceed only after the human has approved the Brief; if the human chose option B on decision 1, skip every F9 step (Phase 0 items 3–4, Phase 1 F9 parts, Phase 2, mutations m1–m3, the F9 totals) and do F10/F11 only.

### Execution contract

- Work on the existing branch; never create/switch branches, never touch `main`. First confirm `git rev-parse --short HEAD` = `6b68518` (or a descendant the human names) and a clean **tracked** tree. Keep every untracked plan file. Leave all work **uncommitted**. No git writes, push, PR, benchmarks, or sanitizer runs.
- Follow `AGENTS.md` and `CORE-PROCESS.md`; phases in order, test-first.
- Plan mismatch (a symbol or behaviour described here doesn't exist or behaves differently): write `PLAN_NOTES-plang.md` with the evidence and halt. If a symbol merely moved and the new place is unambiguous, record it and proceed. A decision this plan doesn't cover → halt and ask. Never weaken, skip or loosen a test. Never rerun an unexplained failure until it goes green.
- Commands: every shell invocation starts with `rtk`. Build: `rtk proxy env SKIP_TESTS=1 ./build.sh`. Single case: `rtk ./build/bin/shard-db-test run <name>`. Full suite: `rtk ./build/bin/shard-db-test run-all --jobs 4`. Capture the real exit status (no `| tail` hiding it). After each test invocation, once its processes stop, `rm -rf /tmp/shard-db-*`.
- Evidence goes in the new file `docs/plans/2026-11-11-plan-g-execution-evidence.md` (style of `2026-11-11-plan-e-execution-evidence.md`).
- `ASSERT_*` does not stop at the first failure: judge every RED case by its **full** failing-assertion set; it must equal the "expected on base" set below. A failing assertion outside it, or a missing one → halt and report.
- **No git write for the revert proofs:** save `rtk proxy git diff` of the tracked files plus copies of the untracked files to the session scratchpad before reverting by hand; restore by hand; verify with `rtk proxy git diff | diff - <scratch>/plan-g-full.patch` (empty).

### Invariants for `ReqShard.res_count` (F9) — the executor must preserve and the tests pin

Let `live(rs)` = number of buckets `i < rs->res_cap` with `rs->res_map[i].used != 0`.

- **I1 (definition).** `rs->res_map != NULL ⇒ rs->res_count == live(rs)`; `rs->res_map == NULL ⇒ rs->res_count == 0`. Therefore `res_count == 0 ⇔ req_res_empty(...)`: the gate's answer is unchanged.
- **I2 (exhaustive write sites of `res_map`; grep-verified in Phase 0).** (a) `calloc` of the `ReqShard` array in the request-begin function (`req->shards = calloc(...)`): count 0, map NULL. (b) `slotcask_bulk_stage_shard`: allocates `res_map` (or leaves it NULL with `res_cap = 0` on `SIZE_MAX`/calloc failure) → count set to 0. (c) the planning insert site in `bulk_plan_window_locked`: `req_res_insert` returns 1 only when a bucket is newly claimed (0 for a NULL map and for a double insert) → count increments **only on 1**. (d) `req_shard_seed_chain_reservations`: replaces `dst->res_map`/`res_cap`; every early return happens **before** the replacement, so the count is assigned together with the map: `dst->res_count = src_n + dst_n` (no duplicates possible: `req_res_copy` fails first). (e) `slotcask_bulk_shard_release`: frees the map → count set to 0. **No path removes or compacts reservations** (entries are never un-used until the free), so the count never decrements.
- **I3 (crash/reset paths).** The state is in-memory only. A stage that fails before the allocation leaves count 0 / map NULL (calloc'd). An allocation failure leaves map NULL / `res_cap` 0: `req_res_insert` is a no-op, so the count stays 0 and agrees with I1. Seed failure leaves the old map and old count untouched. A `ReqShard` is single-use per request (`rs->used` → `EALREADY`), so it is never re-staged after release; resetting the count in release keeps "map NULL ⇒ count 0" for any late reader. A process crash discards everything.
- **I4 (concurrency).** Every access to `res_map`/`res_count` is already under the kf shard's writer gate: stage (pipeline holds it), planning (publish under the gate), seed and release (run by the gate holder in `bulk_commit_chain_run`; joiners' `ReqShard`s are touched only by the committer while the joiner is blocked — the same assumption `res_map` already relies on). `res_count` is a plain `size_t`, never read outside the gate. The TEST_BUILD counters are `_Atomic`.

### Phase 0 — discovery and audit (evidence only, no code)

Record each item; any mismatch → halt.

1. `git rev-parse --short HEAD`, `git status --short` (tracked clean), `git branch --show-current`.
2. Anchor counts, each in `src/db/slotcask.c` unless noted (`rtk proxy grep -cF`):
   ```bash
   'ReqSlotRes *res_map; size_t res_cap;     /* cross-window planned slots */'   # 1
   'static int req_res_empty(const ReqSlotRes *map, size_t cap) {'                 # 1
   'static int req_res_copy(ReqSlotRes *dst, size_t dst_cap,'                      # 1
   'nreserved == 0 && req_res_empty(res_map, res_cap)) {'                          # 1
   'rs->res_cap = req_res_pow2(n * 2);'                                            # 1
   'req_res_insert(rs->res_map, rs->res_cap, &s->kf_plan);'                        # 1
   '    dst->res_cap = cap;'                                                       # 1
   '    free(rs->res_map);'                                                        # 1
   '    rs->res_map = NULL;'                                                       # 2 (release + stage SIZE_MAX branch)
   '_Atomic long g_shard_test_bulk_chains;'                                         # 1
   'req->shards = calloc((size_t)db->num_shards, sizeof(*req->shards));'          # 1
   # src/db/shard_test_ctl.h:
   'extern _Atomic long g_shard_test_bulk_chains;'                                  # 1
   'atomic_store(&g_shard_test_bulk_chains, 0);'                                    # 1
   # build.sh:
   'src/test/cases/test_bulk_chain_pregrow.c \'                                     # 1
   # src/db/embedded.c:
   '(void)slotcask_bmfix_sweep(data_dir);'                                          # 1
   # src/db/slotcask.c:
   '    if (bmfix_unlink_remaps(idx) != 0) rc = -1;'                                # 1
   ```
3. (F9) Write-site audit — paste and compare with I2: `rtk proxy grep -n 'res_map\|res_cap\|req_res_insert\|req_res_empty\|req_res_copy\|req_res_count\|kf_plan_window_insert_slot' src/db/slotcask.c`. Expected: `req_res_insert` has exactly one caller (planning); `req_res_empty` exactly one (the gate); `kf_plan_window_insert_slot` exactly one caller; `req_res_copy`/`req_res_count` callers only inside `req_shard_seed_chain_reservations`; no other statement assigns `->res_map` or `->res_cap` besides the stage block, the seed, and the release `rs->res_map = NULL`. Anything else → halt.
4. (F9) The planning gate is evaluated once per window at its first plan: `rtk proxy grep -n 'size_t nreserved = 0;' src/db/slotcask.c` must show it inside `bulk_plan_window_locked` (per-window local). Record.
5. (F11) Layout audit — in a scratch run of the Phase 1 F11 setup record `ls <root>/br/t/indexes` and `ls <root>/br/t/indexes/flag | head`, confirming field directories `flag`, `kind`, `n` and 3-hex shard names (`12c` = 300, `1ff` = 511). A different layout → halt.
6. (F11) Repair temp naming: `rtk proxy grep -n 'rebuild-XXXXXX' src/db/index.c` (the durable rebuild temp is `.rebuild-*`, not `<NNN>.bm.remap`, so the repair cannot clobber a planted remap; record).
7. (F10) Evidence greps, paste outputs:
   ```bash
   rtk proxy grep -n 'never refuse for capacity' AGENTS.md docs/concepts/storage-model.md docs/reference/limits.md
   rtk proxy grep -n 'BULK_STAGE_UNGROWN' docs/concepts/concurrency.md docs/reference/changelog.md docs/query-protocol/bulk.md
   rtk proxy grep -n 'uncached reader handles' docs/concepts/concurrency.md
   ```
   Expected: `AGENTS.md` carries `except when a shard's growth is temporarily impossible` (its line also contains the phrase and is correct); `storage-model.md:137` and `limits.md:19` carry the unconditional phrase; `bulk.md` has no `BULK_STAGE_UNGROWN`; `concurrency.md` has the "Only an uninitialized cache (test fixtures) hands out uncached reader handles" sentence. Different state → halt.
8. (F10) Confirm the client-visible shape before documenting it: in `src/db/query_bulk.c` read the emit sites that print `some_records_dropped` (grep it) and confirm records with `status < 0` count toward `errors`. If they do not → halt (the `bulk.md` text below would be wrong). Also read the `auto_key_mode != AK_NONE` branch just above each emit site (`query_bulk.c`, non-delimited and delimited paths) and confirm it prints `{"status":"bulk-inserted","count":…,"skipped":…,"keys":[…]}` with no `errors` member — the `bulk.md` text documents both shapes; a different shape → halt. Then read `bulk_insert_response` in `src/db/typed_bulk_insert.c` (packed-layout path) and confirm it prints `errors` + `some_records_dropped` when `errors != 0`, appends `keys` for `auto_key` schemas, and that a failed group counts its records as errors (grep `inputs[g].rc`); a different behaviour → halt.

### Phase 1 — scaffold and tests (all RED/GREEN states recorded)

#### 1A. Production scaffold (no behaviour change)

`src/db/shard_test_ctl.h`: after `extern _Atomic long g_shard_test_bulk_chains;` add
```c
/* F9 drift detector (slotcask.c, TEST_BUILD): how often the reservation count
   was cross-checked against a full scan, how many of those saw a non-empty
   map, and how many disagreed (count != scan, or the gate's `clear` != scan
   emptiness). Production builds do not have these. */
extern _Atomic long g_shard_test_res_checks;
extern _Atomic long g_shard_test_res_checks_nonempty;
extern _Atomic long g_shard_test_res_mismatch;
```
and in the reset function after `atomic_store(&g_shard_test_bulk_chains, 0);` add three `atomic_store(..., 0);` lines for them.

`src/db/slotcask.c`:
1. After `_Atomic long g_shard_test_bulk_chains;` (inside the existing `#ifdef TEST_BUILD`) add the three definitions.
2. `ReqShard`: replace `ReqSlotRes *res_map; size_t res_cap;     /* cross-window planned slots */` with the same line followed by
   ```c
       size_t res_count;                        /* F9: running number of used buckets in res_map (I1) */
   ```
3. After the end of `req_res_copy` (before the comment block that starts `/* Non-mutating probe for a brand-new kf entry.`) add:
   ```c
   #ifdef TEST_BUILD
   /* F9: cross-check the running count (and the gate's emptiness answer)
      against a full scan. Called at window starts, after a chain seed and at
      shard release — the only places the count can be observed wrong. */
   static void req_res_verify(const ReqSlotRes *map, size_t cap, size_t count,
                              int clear) {
       size_t scanned = req_res_count(map, cap);
       atomic_fetch_add(&g_shard_test_res_checks, 1);
       if (scanned) atomic_fetch_add(&g_shard_test_res_checks_nonempty, 1);
       if (scanned != count || clear != (scanned == 0))
           atomic_fetch_add(&g_shard_test_res_mismatch, 1);
   }
   #endif
   ```
4. `kf_plan_window_insert_slot`: add the parameter `size_t res_count,` after `const ReqSlotRes *res_map, size_t res_cap,`. In `kf_plan_window_insert_slot` only (`(void)db;` occurs 5 times in the file), immediately after its `(void)db;` add
   ```c
       const int res_clear = (res_count == 0);   /* running |res_map| (F9) */
       (void)res_clear;
   #ifdef TEST_BUILD
       if (res_map && nreserved == 0)
           req_res_verify(res_map, res_cap, res_count, res_clear);
   #endif
   ```
5. The single caller (inside `bulk_plan_window_locked`): next to `size_t res_cap = 0;` add `size_t res_count = 0;`; next to `res_cap = rs->res_cap;` add `res_count = rs->res_count;`; pass `res_count,` after `res_map, res_cap,` in the call at the `kf_plan_window_insert_slot(` site (the string `res_map, res_cap,` occurs twice in the file; the other hit is the `req_res_probe(res_map, res_cap, slot)` call inside the function). Paste the edited block.
6. `req_shard_seed_chain_reservations`: after `dst->res_cap = cap;` add
   ```c
   #ifdef TEST_BUILD
       req_res_verify(dst->res_map, dst->res_cap, dst->res_count, dst->res_count == 0);
   #endif
   ```
   (the scaffold does **not** assign the count; Phase 2 does).
7. `slotcask_bulk_shard_release`: immediately before `free(rs->res_map);` add
   ```c
   #ifdef TEST_BUILD
       if (rs->res_map)
           req_res_verify(rs->res_map, rs->res_cap, rs->res_count, rs->res_count == 0);
   #endif
   ```
Build: no new warnings (the `(void)res_clear;` keeps the non-TEST_BUILD binary quiet). Paste the build tail. Existing tests are unaffected (the counters are never read by them).

#### 1B. F9 test file

Create `src/test/cases/test_bulk_res_count.c` and register it in `build.sh` after the `test_bulk_chain_pregrow.c \` line (`    src/test/cases/test_bulk_res_count.c \`).

Build it from `test_bulk_chain_pregrow.c`: copy the scaffold sections verbatim — `#include`s, `extern compute_hash_raw`, `bcp_rm_rf`, `Bcp`, `bcp_ctl_reset`, `bcp_open`, `bcp_close`, `bcp_shard_of`, the three `bcp_noop_*`, `bcp_fill_opts`, `BcpBatch`, `bcp_batch_fill`, `BcpReq`, `bcp_req_init`, `bcp_req_thread`, `bcp_marker_scan`, `bcp_wait_hits`, `bcp_visible` — renaming `bcp_`→`brc_`, `Bcp`→`Brc`, `BCP_`→`BRC_` (`sed -i 's/bcp_/brc_/g; s/Bcp/Brc/g; s/BCP_/BRC_/g'`), **omit** `bcp_capacity` and the old scenario/test functions, and make exactly these two changes: (1) `brc_open(Brc *w, int splits, int window)` sets `w->db.bulk_commit_window = window;` instead of `4096` (update the comment: "window < batch size makes every request span several windows"); (2) the temp-dir template becomes `/tmp/shard-db-res-count-XXXXXX`. Replace the file header comment with:
```c
/* F9: the bulk planning gate reads ReqShard.res_count (a running count of
 * cross-window reservations) instead of scanning the reservation map.
 * Invariant I1: res_count == number of used buckets (0 when the map is NULL).
 * TEST_BUILD verifies it against a full scan at every window start, chain seed
 * and shard release (g_shard_test_res_checks / _nonempty / _mismatch).
 * bulk_commit_window = 16 makes each request span several windows, so later
 * windows plan while earlier windows' reservations are live; the chain cases
 * (shard 0 of 8 and shard 300 of 512) also exercise the seed, where a later
 * member's map is replaced by owner + own reservations. Runs in-process, own
 * tmpdir. */
```
Then add (the new code):
```c
#define BRC_WINDOW 16

/* Same choreography as test-bulk-chain-pregrow: owner parks at PRE_MERGE,
 * joiner queues, release -> one 2-member chain. Never re-zero
 * g_shard_test_pause_hits between the two waits. */
static int brc_chain(int splits, int shard, size_t na, size_t nb) {
    Brc *w = calloc(1, sizeof(*w));
    BrcBatch *ba = calloc(1, sizeof(*ba));
    BrcBatch *bb = calloc(1, sizeof(*bb));
    BrcReq *ra = calloc(1, sizeof(*ra));
    BrcReq *rb = calloc(1, sizeof(*rb));
    ASSERT_NOT_NULL(w, "alloc");
    ASSERT_NOT_NULL(ba, "alloc");
    ASSERT_NOT_NULL(bb, "alloc");
    ASSERT_NOT_NULL(ra, "alloc");
    ASSERT_NOT_NULL(rb, "alloc");
    if (!w || !ba || !bb || !ra || !rb) {
        free(ra); free(rb); free(ba); free(bb); free(w);
        return 1;
    }
    pthread_t tha = 0, thb = 0;
    if (brc_open(w, splits, BRC_WINDOW) != 0) {
        ASSERT_TRUE(0, "brc_open succeeds");
        brc_close(w);
        free(ra); free(rb); free(ba); free(bb); free(w);
        return 1;
    }
    brc_batch_fill(w, ba, 1, na, shard, "vA");
    brc_batch_fill(w, bb, 2, nb, shard, "vB");
    brc_ctl_reset();   /* resets the pause seams AND the F9 counters */
    g_shard_test_pause_phase = SHARD_TEST_PHASE_PRE_MERGE;
    g_shard_test_pause_occurrence = 1;
    brc_req_init(ra, w, ba, shard);
    if (pthread_create(&tha, NULL, brc_req_thread, ra) != 0) {
        ASSERT_TRUE(0, "spawn A (committer)");
        atomic_store(&g_shard_test_pause_release, 1);
        brc_close(w);
        free(ra); free(rb); free(ba); free(bb); free(w);
        return 1;
    }
    brc_wait_hits(1);
    ASSERT_TRUE(atomic_load(&g_shard_test_pause_hits) >= 1,
                "owner parked at the PRE_MERGE seam holding the gate");
    g_shard_test_pause_phase = SHARD_TEST_PHASE_GATE_WAIT;
    g_shard_test_pause_occurrence = 1;
    brc_req_init(rb, w, bb, shard);
    if (pthread_create(&thb, NULL, brc_req_thread, rb) != 0) {
        ASSERT_TRUE(0, "spawn B (joiner)");
        atomic_store(&g_shard_test_pause_release, 1);
        pthread_join(tha, NULL);
        brc_close(w);
        free(ra); free(rb); free(ba); free(bb); free(w);
        return 1;
    }
    brc_wait_hits(2);
    ASSERT_TRUE(atomic_load(&g_shard_test_pause_hits) >= 2,
                "joiner queued before the owner's take");
    atomic_store(&g_shard_test_pause_release, 1);
    pthread_join(tha, NULL);
    pthread_join(thb, NULL);

    ASSERT_EQ_INT(ra->rc, 0, "owner request ok");
    ASSERT_EQ_INT(rb->rc, 0, "second request ok");
    ASSERT_EQ_INT((int)atomic_load(&g_shard_test_bulk_chains), 1,
                  "one chain merged both requests (the seed path ran)");
    /* Derived on this tree: 4 window-start checks (owner w1 empty, owner w2,
       joiner w1 seeded, joiner w2) + 1 seed + 2 releases = 7 checks, 6 of them
       non-empty. Bounds are deliberately looser; the actuals go in the
       evidence file. An unseeded joiner would leave only 2 non-empty
       window-start checks, so >= 3 also proves the seed. */
    ASSERT_TRUE(atomic_load(&g_shard_test_res_checks) >= 4,
                "verifier ran at every window start (non-vacuous)");
    ASSERT_TRUE(atomic_load(&g_shard_test_res_checks_nonempty) >= 3,
                "verifier saw non-empty maps (cross-window and seeded)");
    ASSERT_EQ_INT((int)atomic_load(&g_shard_test_res_mismatch), 0,
                  "res_count tracks the reservation map (no drift)");
    int all_visible = 1;
    for (size_t i = 0; i < ba->n; i++)
        if (!brc_visible(w, ba->keys[i], ba->vals[i])) all_visible = 0;
    for (size_t i = 0; i < bb->n; i++)
        if (!brc_visible(w, bb->keys[i], bb->vals[i])) all_visible = 0;
    ASSERT_TRUE(all_visible, "all records visible");
    ASSERT_EQ_INT(brc_marker_scan(w->base), 0, "no markers retained");
    brc_close(w);
    free(ra); free(rb); free(ba); free(bb); free(w);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* One request, three windows (16 + 16 + 8), no chain partner. */
static int brc_single(int splits, int shard, size_t n) {
    Brc *w = calloc(1, sizeof(*w));
    BrcBatch *ba = calloc(1, sizeof(*ba));
    BrcReq *ra = calloc(1, sizeof(*ra));
    ASSERT_NOT_NULL(w, "alloc");
    ASSERT_NOT_NULL(ba, "alloc");
    ASSERT_NOT_NULL(ra, "alloc");
    if (!w || !ba || !ra) { free(ra); free(ba); free(w); return 1; }
    if (brc_open(w, splits, BRC_WINDOW) != 0) {
        ASSERT_TRUE(0, "brc_open succeeds");
        brc_close(w);
        free(ra); free(ba); free(w);
        return 1;
    }
    brc_batch_fill(w, ba, 1, n, shard, "vS");
    brc_ctl_reset();
    brc_req_init(ra, w, ba, shard);
    brc_req_thread(ra);   /* synchronous: no pause seam armed */
    ASSERT_EQ_INT(ra->rc, 0, "request ok");
    /* Derived: 3 window-start checks (empty, 16, 32 reserved) + 1 release. */
    ASSERT_TRUE(atomic_load(&g_shard_test_res_checks) >= 3,
                "verifier ran at every window start (non-vacuous)");
    ASSERT_TRUE(atomic_load(&g_shard_test_res_checks_nonempty) >= 2,
                "verifier saw non-empty maps (cross-window)");
    ASSERT_EQ_INT((int)atomic_load(&g_shard_test_res_mismatch), 0,
                  "res_count tracks the reservation map (no drift)");
    int all_visible = 1;
    for (size_t i = 0; i < ba->n; i++)
        if (!brc_visible(w, ba->keys[i], ba->vals[i])) all_visible = 0;
    ASSERT_TRUE(all_visible, "all records visible");
    ASSERT_EQ_INT(brc_marker_scan(w->base), 0, "no markers retained");
    brc_close(w);
    free(ra); free(ba); free(w);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_bulk_res_count_chain(void)         { return brc_chain(8, 0, 30, 30); }
static int test_bulk_res_count_chain_shard300(void){ return brc_chain(512, 300, 30, 30); }
static int test_bulk_res_count_single_windows(void){ return brc_single(512, 300, 40); }

TEST_REGISTER("test-bulk-res-count-chain", test_bulk_res_count_chain)
TEST_REGISTER("test-bulk-res-count-chain-shard300", test_bulk_res_count_chain_shard300)
TEST_REGISTER("test-bulk-res-count-single-windows", test_bulk_res_count_single_windows)
```
Mechanical construction: `sed -n '1,190p'` of `test_bulk_chain_pregrow.c` is the scaffold up to (not including) `bcp_capacity` (it contains the `BCP_TIER`/`BCP_MAXN` defines at lines 46–47, `bcp_batch_fill`, `BcpReq`, `bcp_req_thread`); `sed -n '204,233p'` is the `/* rt_marker_scan …` comment through `bcp_visible` (`bcp_marker_scan`, `bcp_wait_hits`, `bcp_visible`; the `bcp_capacity` doc comment + function occupy 191–203 and are omitted). Confirm both ranges with `grep -n` before use: line 189 must close `bcp_req_thread`, 190 be blank, 191 start the `bcp_capacity` comment, 204 be the `/* rt_marker_scan …` comment and 233 the closing `}` of `bcp_visible`; any mismatch → halt. The `1,190` range includes the old 26-line header comment (replace it with the new one below) and the 3-line comment above `w->db.bulk_commit_window = 4096;` (delete it, see the `brc_open` edit). Concatenate those two ranges, apply the `sed` rename, make the two stated edits, then append the new code below. Keep every `#include` (`brc_marker_scan` needs `<dirent.h>`). The `brc_open` comment above `w->db.bulk_commit_window = ...;` (the one mentioning 4096) is replaced by the single line `/* window < batch size makes every request span several windows. */`. Notes: `BRC_MAXN` stays 48 (≥ 40). `brc_ctl_reset()` calls `shard_test_ctl_reset()` (which resets the new counters once Phase 1A is applied) and re-pins the tier to 64. `brc_batch_fill` keys are shard-pinned by `compute_record_shard` as in the source file.

Build, then run the three cases on the scaffold. **Expected on base (scaffold):** each case fails **exactly one** assertion, `res_count tracks the reservation map (no drift)` (the count stays 0 while reservations exist); every other assertion passes. Record the actual `res_checks` / `res_checks_nonempty` values: on the first run of each case add one temporary `fprintf(stderr, "res checks=%ld nonempty=%ld mismatch=%ld\n", atomic_load(&g_shard_test_res_checks), atomic_load(&g_shard_test_res_checks_nonempty), atomic_load(&g_shard_test_res_mismatch));` just before the counter assertions, paste its output, then **remove the line** before Phase 2. Expected actuals: chain 7/6, single 4/3 (mismatch > 0 on the scaffold); different actuals that still satisfy the lower bounds are recorded, below the bounds → halt. A different failing set → halt and report.

#### 1C. F11 cases (appended to `src/test/cases/test_bitmap_resplit.c`)

Caveat: the sweep case's "touched only the temporaries" check assumes a plain no-marker restart rewrites no other index file (shown by the shard-300 case only for a restart with a repair). If that one assertion goes red, investigate before anything else and record what rewrote the file — do not drop the assertion.

Both cases copy the setup of `test_bmfix_repair_shard_300` (calloc `Br`, row flags, `test_env_start`, `test_env_ctl_set_kf_seams(&b->env, 64, 0)`, `br_connect`, `add-dir`, `create-object` with `"splits":512`, the three fields/indexes, two `br_bulk` chunks of 250, `br_check(b, "after seed")`, then capture `port`/`root`, `tc_close`, `test_env_stop_keep`). Add the two functions before the `TEST_REGISTER` block and register `test-bmfix-remap-sweep` and `test-bmfix-remap-clear-with-marker` after `test-bmfix-repair-shard-300`. No existing assertion or function is modified.

**Case `test-bmfix-remap-sweep`** (no marker → the `n == 0` sweep branch). After `test_env_stop_keep`:
```c
    char rmA[600], rmB[600], rmC[600], keepf[600];
    snprintf(rmA, sizeof(rmA),   "%s/br/t/indexes/flag/12c.bm.remap", root);   /* bitmap field, shard 300 */
    snprintf(rmB, sizeof(rmB),   "%s/br/t/indexes/kind/1ff.bm.remap", root);   /* bitmap field, shard 511 */
    snprintf(rmC, sizeof(rmC),   "%s/br/t/indexes/n/12c.bm.remap", root);      /* btree field dir: the sweep visits every field dir */
    snprintf(keepf, sizeof(keepf), "%s/br/t/indexes/flag/12c.bm.remap.keep", root);   /* suffix control: must survive */
    ASSERT_EQ_INT(tu_run_cmd(": > '%s' && : > '%s' && : > '%s' && : > '%s'", rmA, rmB, rmC, keepf), 0, "plant temporaries");
    ASSERT_TRUE(tu_file_exists(rmA) && tu_file_exists(rmB) && tu_file_exists(rmC) && tu_file_exists(keepf),
                "temporaries and control exist before the restart");
    ASSERT_EQ_INT(tu_run_cmd("test -z \"$(find %s/br/t/indexes -maxdepth 1 -name '.bmfix-*')\"", root), 0,
                "no repair marker (this is the no-marker branch)");
```
(`br_repair_marker_exists` is only used after the restart.) Messages for the remaining asserts, in order: `startup OK`, `reconnect`, the three `startup swept …` strings above, `non-matching name survives the sweep`, `the sweep touched only the temporaries`. Then `BrInoSnap before; br_inode_snapshot(&before, root);`, `br_seam_once(root);`, restart exactly like the shard-300 case (`br_stderr_capture`/`test_env_start_at`/`br_stderr_restore`), `ASSERT_TRUE(started, "startup OK")`, reconnect, then assert:
- `!tu_file_exists(rmA)`, `!tu_file_exists(rmB)`, `!tu_file_exists(rmC)` — messages `startup swept the flag/12c .bm.remap`, `... kind/1ff ...`, `... n/12c ...`;
- `tu_file_exists(keepf)` — `non-matching name survives the sweep`;
- `br_check(b, "after sweep restart")` (oracle count/find per bitmap value);
- `BrInoSnap after; br_inode_snapshot(&after, root);` and for every `before.e[i].rel` other than the three planted `.bm.remap` rels: `br_inode_same(&before, &after, rel)` — message `the sweep touched only the temporaries`.

**Case `test-bmfix-remap-clear-with-marker`** (marker → repair → `slotcask_bmfix_clear`). Same setup; after `test_env_stop_keep` plant exactly like `test_bmfix_repair_shard_300`: `truncate -s 33` on `flag/12c.bm` and `: >` the marker `.bmfix-12c`; plus `flag/12c.bm.remap` and `kind/12c.bm.remap`. Snapshot after planting, `br_seam_once`, restart as above. Assert: startup OK; `!br_repair_marker_exists(b)` (marker gone); both remaps gone (`the repair path cleared the .bm.remap temporaries` ×2); `flag/12c.bm` inode **changed** (`the repair actually ran` — proves this is the n > 0 branch, not the sweep); `br_check(b, "after repair restart")`.

Run both on the unmodified production logic. **Expected: both GREEN by design** (they pin existing behaviour; their red proof is mutations s1/s2 in Phase 4). If either is red → investigate before anything else (halt if the cause is a planned-behaviour mismatch).

### Phase 2 — F9 production switch

1. `slotcask_bulk_stage_shard`: replace `rs->res_cap = req_res_pow2(n * 2);` with the same line followed by `rs->res_count = 0;` (before the `if (rs->res_cap != SIZE_MAX)` block).
2. Insert site: replace
   ```c
                    req_res_insert(rs->res_map, rs->res_cap, &s->kf_plan);
   ```
   with
   ```c
                    if (req_res_insert(rs->res_map, rs->res_cap, &s->kf_plan))
                        rs->res_count++;
   ```
3. `req_shard_seed_chain_reservations`: after `dst->res_cap = cap;` and **before** the TEST_BUILD verify block add `dst->res_count = src_n + dst_n;`.
4. `slotcask_bulk_shard_release` **only** (not the stage's `SIZE_MAX` else-branch, which also contains `rs->res_map = NULL;`): directly after the `free(rs->res_map);` / `    rs->res_map = NULL;` pair add `    rs->res_count = 0;`. Afterwards `rtk proxy grep -n 'rs->res_count = 0;' src/db/slotcask.c` must show exactly 2 hits (stage, release). These two resets are I1 hygiene and are not load-bearing for any test; do **not** invent a mutation for them.
5. Gate: in `kf_plan_window_insert_slot` delete the line `(void)res_clear;` and replace `nreserved == 0 && req_res_empty(res_map, res_cap)) {` with `nreserved == 0 && res_clear) {`. Replace the comment block above the gate's `if` so it reads (keep Plan B's sentences, add the count):
   ```c
        /* Never resplit while this window or request holds planned-but-
           uncommitted slots: they are numbered for the current table and a
           resplit would commit them into the wrong places. res_clear is
           `res_count == 0`, the running |res_map| maintained by
           req_res_insert and the chain seed (F9) — the map is not scanned.
           After a successful stage pre-grow this trigger is unreachable while
           reservations exist (the stage fails closed on under-coverage —
           BULK_STAGE_UNGROWN), so this resplit only ever runs for the first,
           reservation-free plan. The kf_max_slots check keeps the TEST_BUILD
           tier seam coherent: the stage loop and this planner path respect
           the same cap (production kf_max_slots() is SIZE_MAX, so this is a
           no-op there). */
   ```
6. Delete `req_res_empty` and its two-line comment block (`/* 1 if no slot is reserved. Stops at the first reservation, ...`) together.
7. Build (no new warnings; `rtk proxy grep -c req_res_empty src/db/slotcask.c` must print 0). Run the three F9 cases: GREEN. Run `test-bulk-chain-pregrow`, `-shard300`, `test-bulk-chain-admission-cap`, `test-bitmap-resplit-bulk`, `test-bitmap-resplit-chain`, `test-request-flush-batching`, `test-bulk-stage-ungrown-seam`: all GREEN. Paste.

### Phase 3 — F10 docs (exact edits; each anchor must occur exactly once — else halt)

1. `docs/concepts/storage-model.md`: replace
   `and inserts never refuse for capacity. At 80 % load`
   with
   `and capacity is never the reason an insert is refused. The one transient exception: when a shard's growth is temporarily impossible (a failed bitmap remap puts that shard's growth on a 5 s back-off, or a resplit hits an I/O error), a bulk-insert shard stage that needs the growth fails closed — every record of that stage errors, nothing from it commits (\`BULK_STAGE_UNGROWN\` in the error log) — and the client retries (see [concurrency.md](concurrency.md)). At 80 % load`
2a. `docs/reference/changelog.md` (`## Unreleased`, the "Per-kf-shard kf growth is **uncapped**" paragraph; the anchor spans a line break): replace
   `with no\nopt-in required. The nightly auto-reshard sweep`
   with
   `with no\nopt-in required (the one transient exception is a bulk-insert shard stage that fails closed while that shard's growth is temporarily impossible — see "Bulk insert stages fail closed when growth is impossible" above). The nightly auto-reshard sweep`
   (verify with `rtk proxy grep -c 'opt-in required' docs/reference/changelog.md` = 1 first).
2. `docs/reference/limits.md` (the "Per-kf-shard slot growth" row): replace
   `for the life of the object — inserts never refuse for capacity. When the sizing table`
   with
   `for the life of the object — there is no capacity ceiling. The only transient refusal is a bulk-insert shard stage that fails closed while that shard's growth is temporarily impossible (\`BULK_STAGE_UNGROWN\`; retry). When the sizing table`
3. `docs/query-protocol/bulk.md`: after the fenced block that shows `{"inserted": 9997, "skipped": 0, "errors": 3, "error": "some_records_dropped"}` (the paragraph "If any records were rejected …") and before `### Performance`, insert:
   ```
   A bulk-insert shard stage whose keyfile table cannot be grown (a transient condition — a failed bitmap remap puts that shard's growth on a 5 s back-off, or a resplit hit an I/O error) fails closed: nothing from that stage is committed, other shards' stages are unaffected, and the daemon logs `BULK_STAGE_UNGROWN`. On packed-layout objects (the default) the response carries `errors` and `"error":"some_records_dropped"` for every record of that shard's stage (with `auto_key`, the `keys` array is still appended, in input order, so the failed records can be resent under their keys); bulk-insert is an upsert, so re-sending the request or just the failed records is harmless. Legacy-layout objects without `auto_key` report the same `errors` shape; legacy-layout `auto_key` objects answer `{"status":"bulk-inserted","count":N,...}` without an `errors` member — compare `count` with the number of records sent and retry only the missing ones (re-sending a record whose key was omitted generates a new key).
   ```
4. Re-run the Phase 0 item 7 greps and paste: `never refuse for capacity` must now appear only in `AGENTS.md` (already conditional); the the changelog sentence wraps across lines (`never refuse for` / `capacity` near changelog.md:252), so the single-line grep will not show it — it is fixed by edit 2a; `BULK_STAGE_UNGROWN` appears in `bulk.md`. `rtk git diff --stat -- docs AGENTS.md` shows exactly the four docs (`storage-model.md`, `limits.md`, `bulk.md`, `changelog.md`). `AGENTS.md`, `concurrency.md`, `error-codes.md` are verified-true and **not** edited; `changelog.md` gets only the 2a clause.

### Phase 4 — proofs

Save the scratch copies first (see the execution contract).

1. **F9 mutations (apply one at a time; rebuild; run the three F9 cases; record; hand-revert; rebuild; GREEN):**
   - **m1** — restore the insert site to the bare statement `req_res_insert(rs->res_map, rs->res_cap, &s->kf_plan);` (no increment): all three F9 cases RED with exactly `{res_count tracks the reservation map (no drift)}` each.
   - **m2** — delete `dst->res_count = src_n + dst_n;` in the seed: the two chain cases RED with exactly `{res_count tracks the reservation map (no drift)}`; `test-bulk-res-count-single-windows` GREEN.
   - **m3** — change `const int res_clear = (res_count == 0);` to `const int res_clear = 1;`: all three RED with exactly `{res_count tracks the reservation map (no drift)}` (the verifier sees `clear` disagree with a non-empty map).
   A different set → halt (the derivation or choreography is wrong).
2. **F11 mutations:**
   - **s1** — in `src/db/embedded.c` delete the line `(void)slotcask_bmfix_sweep(data_dir);   /* best effort: only disk space */` (leave the braces): `test-bmfix-remap-sweep` RED with exactly the three `startup swept …` assertions; `test-bmfix-remap-clear-with-marker` GREEN.
   - **s2** — in `slotcask_bmfix_clear` delete `if (bmfix_unlink_remaps(idx) != 0) rc = -1;`: `test-bmfix-remap-clear-with-marker` RED with exactly the two `the repair path cleared …` assertions; `test-bmfix-remap-sweep` GREEN.
3. **Restore check:** after the last revert, `rtk proxy git diff | diff - <scratch>/plan-g-full.patch` is empty (the snapshot is `rtk proxy git diff` taken after Phase 3, before the first mutation; the plain `rtk git diff` hook output is condensed and must not be used for it), and all five new cases are GREEN again. Paste.

### Phase 5 — full suite and hand-back

1. Fresh build; `rtk ./build/bin/shard-db-test run-all --jobs 4`; record totals = baseline + **5 new cases** (3 F9 + 2 F11), **0 failed**; `rm -rf /tmp/shard-db-*`. A failure is root-caused, never rerun until green.
2. Definition of done: no new warnings; no TODO/FIXME/debug output (the Phase 1B counter `fprintf` is removed); no mutation left; `git status --short` shows only this plan's files (`build.sh`, `src/db/{slotcask.c,shard_test_ctl.h}`, `src/test/cases/test_bitmap_resplit.c`, new `src/test/cases/test_bulk_res_count.c`, the four docs) plus the untracked plan/evidence files; `src/db/embedded.c` is **unmodified**.
3. Hand back (evidence file + chat): `git diff --stat`, Phase 0 audits, the Phase 1 base evidence with the exact failing sets and actual counter values, Phase 2/3 greens and doc diff, Phase 4 mutation sets, Phase 5 totals. State plainly: work is **uncommitted**; the sanitizer gate runs once after Plan I (F9 touches shared per-request state under the shard gate and falls under it); F9 is a hygiene change, not a measured speed-up; G-restore is not addressed.

---

## Blind review rounds

- **Round 1** (fresh reviewer, plan + repo only): 0 blockers, 2 majors, 5 minors. Majors: (1) the `bulk.md` text claimed `errors` for every object, but `auto_key` objects print `{"status":"bulk-inserted","count":…,"keys":[…]}` with no `errors` and a blind resend would mint new keys → text now documents both shapes and Phase 0 item 8 makes the executor check the auto-key branch; (2) the release-reset anchor `rs->res_map = NULL;` matched twice → now pinned to `slotcask_bulk_shard_release`, with a 2-hit grep and a note that the two resets are not load-bearing. Minors fixed: explicit `sed -n` extraction ranges for the new test file, find-based marker check, caveat on the sweep case's inode assertion, changelog-wrap grep note. The reviewer independently confirmed the write-site list, I1–I4, the 7/6 and 4/3 counter derivations, all failing-assertion sets for the scaffold and m1–m3, and every Phase 0 anchor.
- **Round 2** (new reviewer): 0 blockers, 2 majors, 5 minors. Majors: (1) the `bulk.md` text was still wrong for packed-layout (default) objects, which answer via `typed_bulk_insert.c::bulk_insert_response` with `errors` + `some_records_dropped` and, for `auto_key`, an appended `keys` array → text rewritten per layout, Phase 0 item 8 now also reads `bulk_insert_response`; (2) the `sed -n` ranges for the new test file were off (`208,234` started mid-function, `1,193` caught a dangling comment) → corrected to `1,190` and `204,233` with a verify-before-use check. Minors fixed: function-qualified `(void)db;` and `res_map, res_cap,` anchors, exact m1 resulting line, `rtk proxy git diff` for the snapshot/restore check, exact replacement comment for `brc_open`. The reviewer independently re-confirmed the write-site list, counter bounds and mutation failing sets.
- **Round 3** (new reviewer): **0 blockers, 0 majors**, 7 minors. All code claims re-verified independently (write sites, I1–I4, counter derivations, failing sets for scaffold/m1–m3, F11 branch isolation by s1/s2, bulk-insert response shapes on packed, legacy and auto-key paths, sed ranges). Minors fixed: stale scaffold comment on `res_clear`, wrong explanation of the duplicate `res_map, res_cap,` anchor, sed-boundary note, mandatory temporary counter print (then removed) so the evidence actuals are real, spelled-out F11 assertion/messages, Unreleased changelog sentence (new edit 2a), provenance wording. Converged: zero blockers and zero majors after 3 rounds (cap 4). Open items: none beyond the two NEEDS YOU rows.
