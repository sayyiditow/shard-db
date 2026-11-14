# Plan E — shard-scoped startup bitmap repair with a safe escape (F5 + F7a)

## Brief

**Primary goal:** A pending bitmap-remap marker must never stop the daemon from starting. Startup repairs only the named shards' bitmaps. If that repair fails, the daemon still serves the object correctly with bitmaps switched off until `reindex` repairs it.

**Problem and root cause:** An interrupted keyfile rebuild leaves `<obj>/indexes/.bmfix-NNN`, which names one kf shard whose bitmaps may still use old slot numbers. At startup, `bmfix_recover_all` (`src/db/embedded.c`) handles any such marker by running `reindex_object_durable_checked_ex`. That is a full rebuild of every btree, trigram and bitmap index on every shard. Any failure, including the marker cleanup, returns −1 and refuses startup. The same failure then repeats on every later start, and `reindex` can't be run because it needs a running daemon. The cost grows with the size of the object, not with how much is damaged, and when it fails there is no way out.

**Logical flow**
1. Startup lists each object's markers. A name that parses as a hex shard id below the object's split count selects that shard. Any other `.bmfix-*` name selects every shard.
2. Under the object's write lock, rebuild only the selected shards' bitmaps for every bitmap field. This uses the existing temp-file + `bm_publish_replace` path, and a durability failure counts as a failure.
3. If the rebuild succeeds, clear the markers and any old escape flag. Then the object is fully healed.
4. If it fails and the object has no pending KFM2 commit marker, durably write `<obj>/indexes/.bmfix-disabled`, log an ERROR naming `reindex`, keep the markers and keep starting. If the flag can't be written, or KFM2 markers are pending for that object, refuse startup as today.
5. While the flag exists, the read planner never chooses a bitmap for that object. Reads fall back to btree or a full scan, so they stay correct. Up front, with a clear error, the object refuses every record mutation (insert/update/delete/bulk-*), and every schema or maintenance mode in `mode_is_schema` (add-field, edit-field, rename-field, remove-field, add-index, remove-index, vacuum, truncate). Background auto-vacuum and auto-reshard skip it. `put-file`, `delete-file`, `sequence`, `backup`, `restore`, `reindex` and `drop-object` stay allowed.
6. A full (non-composites-only) `reindex` of a flagged or marked object runs the strict durable rebuild. Only if that succeeds does it clear the markers and then the flag. A later start retries the repair first, and success there also clears both. A flag with no markers left is removed at startup.

| Decision | Alternatives considered | Why | Tag |
|---|---|---|---|
| Escape = start with bitmap planning disabled for the object; `reindex` repairs and re-enables | Keep refusing startup; drop the object's bitmap index entries | Human decided this on 2026-11-06; reconfirm with this Brief | NEEDS YOU (decided 2026-11-06) |
| While flagged, refuse up front (before any segment, marker or kf change) every record mutation (insert/update/delete/bulk-*), and every schema or maintenance mode in `mode_is_schema` (add-field, edit-field, rename-field, remove-field, add-index, remove-index, vacuum, truncate). Background sweeps skip it. | Only fail writes that touch the damaged file (draft behaviour); allow writes and skip bitmap maintenance | A write that fails inside index apply, after its KFM2 marker is durable, leaves a pending window. The next start would replay that window into the damaged bitmap and refuse, which recreates the trap. Up-front refusal is the only form that is always safe. It is broader than "bitmap-indexed writes fail": updates that touch only non-bitmap fields are refused too. | NEEDS YOU |
| Repair fails **and** the object has pending KFM2 markers on an unclean start → refuse startup (current behaviour, clearer log) | Replay skips bitmap apply for flagged objects, and a flagged repair always rebuilds all shards | Rare (it needs both an interrupted remap and an interrupted indexed window, plus a repair failure). Skipping bitmap apply inside replay changes forward-replay code and needs its own proof. Recommend refusing, documented as a residual case. | NEEDS YOU |
| A disabled flag that can't be read or removed at startup → keep serving with the object flagged (WARN), not refuse | Refuse startup | Refusing would recreate the trap; a stuck flag only makes the object read-only with bitmaps off, which is always correct. Only an unreadable marker directory, an unloadable schema for a marked object, a failed flag write, or a failed repair with pending KFM2 markers still refuse. | DEFAULT |
| An unparseable or out-of-range marker name → rebuild every shard's bitmaps (still bitmaps only) | Fail closed (refuse startup) | Failing closed on a stray file would recreate the trap this plan removes. Rebuilding everything is still correct. | DEFAULT |
| Flag present but no markers at startup → remove the flag | Leave it until `reindex` | That state arises when `reindex` crashed between clearing the markers and removing the flag, or when an earlier startup repair succeeded but the flag removal failed. Writes were refused the whole time, so the bitmaps are complete. | DEFAULT |
| Flag check = a process-wide sticky atomic, set when any object is flagged, then one `access()` only when it is set | Always `access()`; in-memory per-object table | No cost on the hot path in the normal case. The flag can appear in exactly two places: startup repair (single-threaded, before serving) and `restore` of a backup that carried it (under the object wrlock). Both set the atomic before the flag can be observed. | DEFAULT |
| `build_bitmap_pass` gains an optional per-shard selection mask (NULL = all) | A separate one-shard builder; one call per shard | Reuses the parallel walk and publish path. Selected shards still build in parallel. | DEFAULT |
| New cases go in `src/test/cases/test_bitmap_resplit.c`, reusing its `br_*` helpers | New test file | Its helpers (oracle, restart, marker checks) already cover this scenario. No `build.sh` change is needed. | DEFAULT |

**Blast radius:** `embedded.c::bmfix_recover_all`; `slotcask.c/.h` (`slotcask_bmfix_pending` is replaced by `slotcask_bmfix_marked_shards`, and the flag helpers are added; the only caller is `bmfix_recover_all`); `index.c` (`build_bitmap_pass` signature and its one caller `cmd_add_index`, new repair entry, `cmd_reindex`); `query.c::field_has_index_type` and `query.c::field_index_type` (the read gates for every bitmap planner and dispatcher, listed in P0); `server.c` (JSON and text dispatch gates, `auto_reshard_sweep_one`, `auto_vacuum_sweep_one`); test seams (`shard_test_ctl.h`, `slotcask.c` definitions, `test_control.c`, `fixtures.c/.h`). New on-disk artifact `.bmfix-disabled` (empty file). An older binary treats it as a `.bmfix-` marker: it runs one full strict reindex and deletes the file. That is benign, because the rebuild is complete. New error response for refused mutations on a flagged object. No wire or CLI shape, config key or record/index format changes. Behaviour changes:
- An object that used to block startup now serves read-only.
- A full `reindex` of an object with serving-time `.bmfix` markers (left after a logged commit failure) now uses the strict durable rebuild and clears those markers. Today only startup clears them; the comments in `slotcask.c` and AGENTS.md are updated to match.
- `restore` replaces the object's flag/marker state with the backup's.

**Test strategy**
- Scope: a corrupted shard-0 bitmap plus a planted `.bmfix-000` → startup succeeds. Only shard 0's `.bm` files get new inodes; other shards' `.bm` files and the btree `.idx` files keep theirs; counts are exact.
- Malformed marker name → every shard's `.bm` is rebuilt, no btree is rebuilt, the marker is cleared and startup succeeds.
- Escape: an injected repair failure → startup succeeds. The flag is written, markers are kept, the ERROR log line appears and reads are exact with no bitmap plan, and a btree field still plans through its btree. JSON and text-path mutations and vacuum are refused with no state change. A `composites_only` reindex keeps the flag. A full wire `reindex` clears both, after which writes work and bitmap plans return.
- Heal at startup: flagged state plus a clean restart → flag and markers are cleared and writes work.
- Flag write fails (the flag path is a directory) → startup refuses.
- Flag without markers → removed at startup.
- A full `reindex` of an unflagged object with a serving-time `.bmfix` marker clears that marker.
- F7a: an unclean start with both a KFM2 marker and a `.bmfix` marker → both resolved and counts exact. This pins resolution and exactness, not the repair-before-replay order, because a rebuild after replay would also be correct. Same, but with the repair failing → startup refuses with the documented log.
- Each bug-fix test is red on the base and green after the fix, with a revert proof. Coverage-only tests carry a mutation proof.

**Residual cases (documented, not fixed):** an object with `fields.conf` but absent from schema.conf isn't walked at startup, so its flag doesn't arm the sticky atomic. Once any flag has appeared in a process, every bitmap-type check and every mutation dispatch on every object costs one `access()` until restart.

**Out of scope:** keeping writes available while flagged; replaying KFM2 windows past a damaged bitmap; showing the flag in `describe-object`/`stats`; F4 (immediate repair of skipped corrupt bitmaps); gating `backup`/`restore` (restore takes the backup's flag/marker state and arms the sticky atomic, P3.6); benchmarks; the branch sanitizer gate (runs once after Plan I).

**Approval:** Human approved this Brief on 2026-11-11, with all three NEEDS YOU rows as recommended (escape design reconfirmed; up-front refusal scope; refuse startup when repair fails with pending KFM2). They accepted the round-4 escalation (two wording majors fixed, not re-reviewed). Execution is authorized.

**Review log:** The Nov 7 draft `2026-11-06-bmfix-startup-repair-scope-and-escape.md` is superseded by this file: it was written before Plans A–D, collides with D's test-hook slot 4, leaks an fd, fails closed on stray marker names and left pending-window replay unhandled. Blind review rounds: see bottom of file.

---

## Executor Plan

Planner: Claude Opus 5.5. Executor: whoever the human designates; don't infer attribution. Derived against local commit `a73ce13` (Plans A–D present) on `fix/type-correctness-recovery`. The executor may proceed only after the human has approved the Brief and both NEEDS YOU rows. If the human picked the alternative on either row, this Plan doesn't cover it: halt.

### Execution contract

- Work on the existing `fix/type-correctness-recovery` branch. Don't create or switch branches and never touch `main`. First confirm HEAD = `a73ce13` (or a descendant the human names) and a clean tracked tree. Keep every untracked plan or evidence file. Leave all work **uncommitted** for raw-diff review. No git writes, push, PR, benchmarks or sanitizer runs.
- Follow `AGENTS.md` and `CORE-PROCESS.md`. Do the phases in order, test-first.
- Plan mismatch (a symbol or behaviour described here doesn't exist or behaves differently): write `PLAN_NOTES-bmfixesc.md` with the evidence and halt. If a symbol has only moved and its new place is unambiguous, record that and proceed. A design decision this plan doesn't cover means halt and ask, not improvise. Never weaken, skip or loosen a test. Never rerun an unexplained failure until it goes green.
- Commands: every shell invocation starts with `rtk`. Build with `rtk proxy env SKIP_TESTS=1 ./build.sh`. Single case: `rtk ./build/bin/shard-db-test run <name>`. Full suite: `rtk ./build/bin/shard-db-test run-all --jobs 4`. Capture the real exit status (no `| tail` hiding it). After each test invocation, once its processes have stopped, run `rm -rf /tmp/shard-db-*`.
- Record all evidence (commands, output and exit status) in the new file `docs/plans/2026-11-11-plan-e-execution-evidence.md`.
- Starts expected to succeed use `test_env_start_at` with the existing `br_*` timeouts (120 s client I/O). During the base (RED) runs, a `test_env_start_at` that is expected to fail on base is bounded by the fixture's own readiness timeout.
- **Before every daemon start** in the new cases (restart, `test_env_start_at` or raw spawn), make sure db.env contains `KF_TEST_INITIAL_SLOTS=64`. Otherwise startup extends the kf files and changes the slot layout. `br_restart_kill` (and any helper that already calls it) does this. Before each direct `test_env_start_at` or `br_spawn_expect_exit`, call `br_persist_seam(root)` exactly once for that start, unless the db.env already has the line from an earlier start of the same root (check with `grep -q`).

- Expected-failure starts use a local raw-spawn helper (see P1), not `test_env_start_at`, so "exited with failure" is distinguished from "timed out". Never call `br_connect` after an expected-failure start.
- Every log line a test greps is emitted at **LOG_WARN or LOG_ERROR** (the fixture's db.env uses `LOG_LEVEL=2`, so INFO is dropped).
- Startup lines go synchronously to `<base>/daemon.log`; raw spawns write to the helper's own log file, under `<base>`. Use `br_log_has`, which greps all of `<base>`.
- Lines logged while serving (e.g. `cmd_reindex`'s) go through the async log thread. Assert them with the polling `br_fault_stage_log`, never with a single grep.

### Phase 0 — discovery and consumer audit (evidence only, no code)

Record each in the evidence file; any mismatch with the claim → halt.

1. `slotcask_bmfix_pending` has exactly one caller (`bmfix_recover_all`). `bmfix_recover_all` is called twice (clean and unclean branches of `shard_db_recover_before_stamp`), both before the KFM2 sweep. `build_bitmap_pass` has exactly one caller (`cmd_add_index`) plus its forward declaration. `slotcask_bmfix_clear` today unlinks every `.bmfix-` prefixed name (so it would delete `.bmfix-disabled`).
2. **Read-gate audit.** List every production path that opens a bitmap for **reading**: callers of `bm_open`/`bm_open_many_ro`, the bitmap candidate builders, popcount/count helpers, group-by bitmap paths, and every caller of `field_index_type` (known: `btree_dispatch_schema_checked`'s bitmap branch and the stream-find btree check). Each must reach the bitmap only through `field_has_index_type(..., IT_BITMAP)`, `pick_index_for_leaf_schema`/`pick_index_for_leaf` (gated transitively, since they call `field_has_index_type`) or `field_index_type` (gated directly in P3.3). Allowed exceptions: write-maintenance callers (unreachable while flagged, P3.4) and rebuilders (`build_bitmap_pass`, `reindex_object_checked_impl`, `query_find.c::rebuild_bitmap_indexes`, `query_schema.c::create_bm_worker`). Any other path → halt.
3. **Mutation-entry audit.** List every way a record mutation or a kf rebuild can start while serving: JSON `dispatch_json_query` (`mode_is_write`, `mode_is_schema`), the text fast path (`fast_wr`/`fast_rd`), deferred bulk requests (confirm they go through `dispatch_json_query` after its object lock), `shard_db_query` (confirm it calls `dispatch_json_query`), `auto_vacuum_sweep_one`, `auto_reshard_sweep_one`, any `reshard` mode, and every caller of `kfcache_resplit_locked`/`slotcask_compact_kf`/`rebuild_object_v2` that can run outside those entries. Confirm `reindex`, `drop-object` and `restore` aren't in either classifier. Any entry P3.4 doesn't cover → halt.
4. `INDEX_TEST_FAIL_NEXT_BUILD` is parsed from db.env only under `TEST_BUILD`, is one-shot, and with the BTRI stamp present nothing at startup consumes it before `bmfix_recover_all`.
5. `TEST_HOOK_SET_GLOBALS` selectors 0–4 are taken (4 = `BMKS_RACE`). The handler's final `else` applies the kf seams, so new selectors must be `else if` branches placed before it. `g_shard_test_fail_phase`, `_occurrence` and `_sticky` are plain globals, and `g_shard_test_sync_counts[]` is never reset by an arm.
6. Whether index.conf can hold one field declared both as bitmap and as btree (via `create-object` `indexes` or `add-index`, e.g. `kind:bitmap` + `kind:btree`) with both built. If yes, P1's escape case uses it (`kind` gets both). If no (expected, since `idx_should_auto_bitmap` re-promotes enums), record the evidence and drop the dual-declaration assertion. P3.3's `field_index_type` gate is still implemented, and the escape case instead asserts that `n gte 0` explain still names the btree and its count is exact while flagged.
7. The exact substring that identifies a bitmap plan in the response to `{"mode":"count",...,"criteria":[...],"explain":true}` (emitted by `cmd_explain` / the plan printer in `query_plan.c`, e.g. `"bitmap"`). Record a sample response from a scratch daemon.
8. How `test_null_migration.c::fork_raw_daemon`/`spawn_daemon_raw` spawn and reap a daemon (argv, env, db.env location, log location).
9. Docs describing bmfix startup behaviour or startup refusal: `rtk grep -rn 'bmfix\|interrupted bitmap remap\|startup refused' AGENTS.md docs/`.

### Phase 1 — seam first, tests, base evidence

**1a. Seam (test infrastructure only, before any test).**
- Make `g_shard_test_fail_phase`, `g_shard_test_fail_occurrence` and `g_shard_test_fail_sticky` `_Atomic` in `shard_test_ctl.h` and in their definitions in `slotcask.c`.
- In `shard_test_note_sync`, load each once into a local and use the local `n` from the counter increment; don't re-read `g_shard_test_sync_counts[phase]`. Existing plain assignments stay valid C11.
- In `test_control.c` add `TEST_HOOK_SET_GLOBALS_FAIL_PHASE 5` (`a` = phase as `(long)(int64_t)msg.a`, so −1 round-trips; `b` = occurrence). When arming (phase ≥ 0), the handler first resets the counters (below), then stores occurrence, then phase. When disarming (phase −1), it stores phase first. The reset sets `g_shard_test_sync_counts[]` to 0 for all phases with `__atomic_store_n(&..., 0, __ATOMIC_RELAXED)` (matching the workers' `__atomic_add_fetch`), so occurrence is counted from the arm.
- Add `TEST_HOOK_SET_GLOBALS_FAIL_STICKY 6` (`a` = 0/1).
- Both are `else if` branches before the final `else`, inside the existing `TEST_BUILD` guard.
- Fixture wrapper `int test_env_ctl_set_fail_phase(TestEnv *env, int phase, int occurrence, int sticky)` in `fixtures.c/.h` next to `test_env_ctl_set_bmlock_log`. Phase −1 disarms; sticky is sent first when arming and cleared after disarming.
- Build; run `test-window-release-routes`, `test-request-flush-batching`, `test-durability-ordering` and `test-marker-v2` (plus every other case found by `rtk grep -ln 'g_shard_test_fail_\(phase\|occurrence\|sticky\)' src/test/cases/`, recorded in P0.5); they must pass.

**1b. Seam probe (replaces any guess about the bulk K path).** Run the probe as the first half of the F7a case (up to the kill), then the case; don't create a separate test file.
- Run a scratch daemon test, or the F7a case's first half, to record evidence: arming phase K occ 1 sticky and then sending one single-row bulk-insert returns a response within the client timeout (record it). Exactly one `data/kf/*_marker.dat` then exists, disarm succeeds, and `test_env_kill` + restart replays it.
- If the request hangs or no marker remains, halt.

**1c. Tests** in `src/test/cases/test_bitmap_resplit.c`, each registered with `TEST_REGISTER`.
- Reuse `br_open`, `br_bulk`, `br_fault_stage_bulk` (sends rows without updating the oracle), `br_check`/`br_check_tc`, `br_marker_exists`, `br_remap_leftovers`, `br_restart_kill`, `br_persist_seam`, `br_log_has`.
- New static helpers:
  - `br_inode_snapshot`/`br_inode_same`: after `test_env_stop_keep` and before any planted change, `stat()` every `indexes/flag/*.bm`, `indexes/kind/*.bm` and every file in `indexes/n/`, keyed by path. Record `(st_ino, st_ctim)`. "Unchanged" means both are equal; "changed" means either differs. ctime guards against inode reuse on non-tmpfs filesystems.
  - `br_spawn_expect_exit`: a local copy of the P0.8 raw-spawn pattern. It forks the daemon against the stopped root with a 60 s budget and returns its exit status, or −1 on timeout after SIGKILL+reap. The test asserts exit (not timeout) and a nonzero status.
  - `br_flag_exists`: `.bmfix-disabled`.
  - `br_repair_marker_exists`: a `.bmfix-*` entry **other than** `.bmfix-disabled`, implemented with `find ... -name '.bmfix-*' ! -name '.bmfix-disabled'`. Every "marker" assertion in the new cases uses this helper, never `br_marker_exists`, which also matches the flag.
- Oracle rows stay within `BR_N` (3000).
- Each case owns its daemon and tmpdir and stops it on every path (`br_close`).
- Text fast-path request: send through the same `TestClient` the exact line format the text dispatcher parses (P0 records it from `server.c`; fields separated by `\x1F`): `insert`, `br/t` per its dir/object convention, key `k02002`, value `{"flag":true,"kind":"a","n":2002}`.

Seeded object: `br_open(b, 0)` (splits 8; bitmaps `flag`, `kind`; btree `n`). The dual declaration from P0.6 is only asserted if P0.6 shows that `create-object` with `"indexes":["flag:bitmap","kind:bitmap","kind:btree","n"]` builds and reloads both. In that case use a local `br_open` variant for the two escape cases only; otherwise use `br_open` unchanged. Bulk `k00000..k01499` in 250-row chunks, then `br_check`.

Refusal error, exactly, after trimming the trailing newline: `{"error":"object writes disabled: bitmap repair failed at startup; run reindex"}`. A missing key's `get` returns whatever base returns for a missing single key; P0 records that response, and the test asserts it. Text fast path: `Error: object writes disabled: bitmap repair failed at startup; run reindex`.

| Case | Setup (stop with `test_env_stop_keep`) | Assertions | Base result, failing assertion |
|---|---|---|---|
| `test-bmfix-repair-shard-scoped` | seeded; snapshot; `truncate -s 33 indexes/flag/000.bm`; empty `indexes/.bmfix-000`; persist seam; restart | startup OK; marker gone; `br_check` exact; `flag/000.bm` and `kind/000.bm` inodes changed; all other `.bm` and all `n/` inodes unchanged; log has `bitmap rebuild of 1 shard` | RED (full set listed below) |
| `test-bmfix-repair-bad-marker-name` | seeded; snapshot; empty `.bmfix-zz`; restart | startup OK; marker gone; `br_check` exact; every `.bm` inode changed; every `n/` inode unchanged | RED: "`n/` unchanged" |
| `test-bmfix-escape-disabled` | seeded; stop; `truncate -s 33 indexes/flag/000.bm`; plant empty `.bmfix-000`; append `export INDEX_TEST_FAIL_NEXT_BUILD=5` to db.env (same mechanism as `br_persist_seam`); restart with `test_env_start_at` | startup OK; flag exists; marker present; log has `bitmap planning DISABLED`; `br_check` exact; explain of `flag eq true` lacks the P0.7 bitmap substring (and, per P0.6, `kind eq b` explain shows btree and its count is exact). Each returns exactly the refusal error: JSON `insert` of `k02000`, `update` of `k00001` setting `n`, `delete` of `k00002`, `bulk-insert` of `k02001`, `vacuum`. Text fast-path insert of `k02002` returns the text refusal. Then `count` [] = 1500, `get k02000`/`k02002` missing, `k00001` keeps its old `n`, `k00002` present. `n gte 0` count is exact and its explain names btree. Wire `reindex` with `composites_only:true` → flag and marker still present. Full wire `reindex` of `br/t` → success; flag and marker gone; log has `bitmap planning re-enabled`; insert `k02000` succeeds (oracle updated); `br_check` exact; explain has the bitmap substring | RED: "startup OK" (base refuses) |
| `test-bmfix-escape-heals-at-startup` | the escape-disabled setup and its assertions through "`br_check` exact" (no mutations, no reindex); stop; delete the knob line from db.env (`sed -i`); restart | startup OK; flag and marker gone; log has `bitmap planning re-enabled`; insert works; `br_check` exact | RED: first "startup OK" |
| `test-bmfix-escape-flag-write-fails` | seeded; corrupt + marker + knob; `mkdir indexes/.bmfix-disabled`; `br_spawn_expect_exit` | daemon exited (not timeout), status nonzero; log has `could not write the disabled flag` | RED: log assertion (base logs `reindex failed`) |
| `test-bmfix-flag-without-markers` | seeded; snapshot; empty `.bmfix-disabled`, no markers; restart | startup OK; flag gone; log has `stale disabled flag removed`; every `.bm` and `n/` inode unchanged; insert works | RED: "inodes unchanged" (base treats the flag as a marker and reindexes) |
| `test-bmfix-reindex-clears-serving-markers` | seeded; arm `SHARD_TEST_BMFIX_COMMIT_SYNC` count 100000; `br_bulk(1500,1750)`; disarm; assert `br_repair_marker_exists` | full wire `reindex` succeeds; `br_repair_marker_exists` false; `br_check` exact | RED: "marker false" (base reindex leaves markers) |
| `test-bmfix-unclean-with-kfm2` (F7a) | seeded; arm `SHARD_TEST_BMFIX_COMMIT_SYNC` count 100000; `br_bulk(1500,1750)`; disarm; assert `br_repair_marker_exists`; arm fail phase K occ 1 sticky; `br_fault_stage_bulk(1750,1751)`; disarm (phase −1); set `rows[1750].live = 1`; `test_env_kill`. The fault-stage call is `br_fault_stage_bulk(b, 1750, 1751, &inserted, &errors)`; record its return value and counters as evidence only. | after kill both `*_marker.dat` and `.bmfix-*` exist; restart (`test_env_start_at`); startup OK; neither marker kind remains; `br_check` (1751 live rows) exact; `count` [] = 1751 | GREEN by design; mutation proof P4.3(a) |
| `test-bmfix-unclean-repair-fails-with-kfm2` | as F7a through the kill; append the knob; `br_spawn_expect_exit` | exited, nonzero; log has `with pending commit markers; startup refused`; no `.bmfix-disabled` | RED: log assertion (base logs `reindex failed`) |

Base evidence: build and run each case. `ASSERT_*` does not stop at the first failure, so judge each RED case by its full set of failing assertions. That set must be exactly the "expected on base" set below; a failing assertion outside it → halt. F7a must be green; red there is a real finding → halt and report.

Expected base-failing assertion sets (everything else in the case must pass on base):
- shard-scoped: "other `.bm` unchanged", "`n/` unchanged", "log has `bitmap rebuild of 1 shard`".
- bad-marker-name: "`n/` unchanged".
- escape-disabled: "startup OK" only. Structure the case so that a failed start runs `br_close` and returns without asserting anything further.
- escape-heals-at-startup: "startup OK" of the first start (same early-exit rule).
- escape-flag-write-fails: "log has `could not write the disabled flag`".
- flag-without-markers: "log has `stale disabled flag removed`", "inodes unchanged".
- reindex-clears-serving-markers: "marker false".
- unclean-repair-fails-with-kfm2: "log has `with pending commit markers; startup refused`".

### Phase 2 — shard-scoped repair

1. `index.c::build_bitmap_pass` gains a last parameter `const uint8_t *only_shards` (NULL = every shard; else `sch->splits` bytes, nonzero = rebuild).
   - Allocate `BmShardWalkArg` only for selected shards, packed densely, so `parallel_for` runs only those walks. The failure-cleanup and publish loops iterate the packed array.
   - With no shard selected, return OK without touching disk.
   - The banner reports the selected count (`%d shards` → selected count, plus ` (partial)` when a mask is given).
   - Update the forward declaration; `cmd_add_index` passes NULL.
2. `index.c`: new `int bitmap_rebuild_shards_durable(const char *eff_root, const char *object, const uint8_t *only_shards, int *out_errno)`, declared in `types.h` next to `reindex_object_durable_checked_ex`.
   - The caller holds `objlock_wrlock`.
   - Under `TEST_BUILD`, first consume `g_index_test_fail_next_build` exactly like `reindex_object_checked_impl` (positive → zero it, fail with that errno, before any work).
   - Load `Schema`/`TypedSchema`; `splits <= 0`, `streams <= 0` or no typed schema → fail with EINVAL.
   - Resolve the bitmap field set from `index.conf` with exactly `reindex_object_checked_impl`'s rules (`parse_index_spec`, `idx_should_auto_bitmap`, the 2-byte enum `max_values = 65535`, skip composites). Factor that per-line resolution into one static helper used by both, without changing `reindex_object_checked_impl`'s behaviour. A missing `index.conf` (ENOENT) means no bitmap fields.
   - For each field, `build_bitmap_pass(..., force=0, only_shards)`. Any status other than `INDEX_BUILD_OK` (including `INDEX_BUILD_DURABILITY_UNCONFIRMED`) → set `*out_errno` (build errno, else EIO), log ERROR `BMFIX-RECOVER <root>/<obj>: bitmap rebuild failed for field F (status S, errno E)`, return −1. No bitmap fields → 0.
3. `slotcask.c/.h`: replace `slotcask_bmfix_pending` with `int slotcask_bmfix_marked_shards(const char *data_dir, int splits, uint8_t *out_mask)`.
   - Returns the count of marker entries (0 = none) or −1 on `opendir`/`readdir` error (ENOENT of `indexes/` → 0). `out_mask == NULL` (with any `splits`) only counts.
   - The name `.bmfix-disabled` is not a marker.
   - A suffix of 1–8 characters, all in `[0-9a-f]` (no sign, whitespace or `0x`), whose value is in `[0, splits)`, sets that byte; any other `.bmfix-*` name sets all bytes.
   - `slotcask_bmfix_clear` must also skip `.bmfix-disabled`.
   - Add `int slotcask_bmfix_disabled_exists(const char *data_dir)` (1/0/−1).
   - Add `int slotcask_bmfix_disabled_write(const char *data_dir)`: `open(O_WRONLY|O_CREAT|O_TRUNC)`, fsync, close, fsync `indexes/`; 0, or −1 with errno preserved; every fd closed on every path.
   - Add `int slotcask_bmfix_disabled_remove(const char *data_dir, int *removed)`: unlink + fsync `indexes/`; ENOENT → 0 with `*removed = 0`; `*removed = 1` when a file was unlinked; `removed` may be NULL.

### Phase 3 — recovery rework and gates

1. `embedded.c::bmfix_recover_all` (rewrite its header comment to match). For each schema entry, in this exact order:
   1. `n = slotcask_bmfix_marked_shards(data_dir, 0, NULL)` (count only, no lock). If `n < 0`, refuse startup with an ERROR.
   2. `f = slotcask_bmfix_disabled_exists(data_dir)`. If `f < 0`, treat this object as flagged: log WARN `BMFIX-RECOVER d/o: cannot read disabled flag (errno=E); treating object as disabled; repair not attempted`, set `ended_flagged`, and **continue with the next object**. Repair is not attempted, which is safe: writes are refused and reads avoid bitmaps.
   3. If `n == 0`, run the best-effort `.remap` sweep (as today). If `f == 1`, take `objlock_wrlock`, call `slotcask_bmfix_disabled_remove` and unlock. A failed `objlock_wrlock` here or in step 4 refuses startup with an ERROR, as today.
      - Removed: WARN `BMFIX-RECOVER d/o: stale disabled flag removed (no markers)`.
      - Removal failed: WARN `BMFIX-RECOVER d/o: could not remove stale disabled flag (errno=E); object stays disabled until reindex` and set `ended_flagged`.
      - Continue to the next object.
   4. `n > 0`:
      - `splits = load_schema(eff_root, obj).splits`. If `splits <= 0`, refuse startup with an ERROR naming the object.
      - Allocate the mask with `calloc(splits, 1)` and run `slotcask_bmfix_marked_shards(data_dir, splits, mask)`. A `< 0` result refuses. Free the mask on every path.
      - Take `objlock_wrlock` and hold it through step 5, unlocking before every continue or return.
      - Log WARN `BMFIX-RECOVER d/o: interrupted bitmap remap; bitmap rebuild of K shard(s)` (K = selected count).
      - Call `bitmap_rebuild_shards_durable`. On success, call `slotcask_bmfix_clear`. On a failure of either, save `errno` (or the rebuild's `out_errno`) immediately into a local `E` before any other call, and use `E` in every later log line. On success of that, call `slotcask_bmfix_disabled_remove`.
      - Everything succeeded: if a flag was removed, log WARN `BMFIX-RECOVER d/o: repair succeeded; bitmap planning re-enabled`.
      - Only the **remove** failed (markers are already gone): log WARN `... repair succeeded but disabled flag could not be removed (errno=E); object stays disabled until reindex`, set `ended_flagged` and continue. Writes stay refused and the bitmaps are complete, so this state is safe.
   5. A **rebuild** or **clear** failure leads to the escape decision:
      - `object_has_pending_markers(data_dir) != 0` (−1 counts as pending): log ERROR `BMFIX-RECOVER d/o: repair failed (errno=E) with pending commit markers; startup refused` and refuse.
      - Otherwise call `slotcask_bmfix_disabled_write`. If it fails, log ERROR `BMFIX-RECOVER d/o: could not write the disabled flag (errno=E); startup refused` and refuse.
      - Otherwise log ERROR `BMFIX-RECOVER d/o: bitmap planning DISABLED (repair failed, errno=E); reads fall back, writes refused; run reindex`, set `ended_flagged` and continue.
   6. After the loop, if any object has `ended_flagged`, call `atomic_store(&g_bmfix_any_disabled, 1)`. That is an `_Atomic int`, defined in `slotcask.c` and declared in `slotcask.h`, and it is never cleared within a process.
2. `slotcask.c`: `int slotcask_bmfix_object_disabled(const char *eff_root, const char *object)` returns 0 at once when `g_bmfix_any_disabled == 0`. Otherwise it returns the `disabled_exists` result for `<eff_root>/<object>`, with −1 treated as 1. It saves and restores `errno` around its `access()`, so it never changes errno for callers that use `errno ? errno : EIO` or `errno = 0; ...; if (errno)` conventions.
3. `query.c` read gates, `IT_BITMAP` only:
   - `field_has_index_type`: a matching bitmap entry doesn't count when `slotcask_bmfix_object_disabled(db_root, object)`.
   - `field_index_type`: a matching bitmap entry is skipped (keep scanning) when disabled; if no other entry matches, return the existing default (`IT_BTREE`).
   - No other index types change.
4. Mutation gates (every P0.3 entry), each **after** the object lock is held and **before** any mutation work:
   - **JSON `dispatch_json_query`**: immediately after the generic `took_wrlock`/`took_rdlock` block and before the CAS guard block. Gate when `(mode_is_schema(mode) || mode_is_write(mode))`, mode not in {`put-file`, `delete-file`, `sequence`}, and `slotcask_bmfix_object_disabled(db_root, object)`. Emit the refusal error, unlock exactly the lock taken (`objlock_wrunlock`/`objlock_rdunlock`), free `guard_json`, `mode`, `dir` and `object` like the adjacent early returns, and return. Release the guard reservation/registry pin if either is already held at that point (P0 confirms they are not).
   - **Text fast path**: after its `fast_wr`/`fast_rd` lock, gate when `fast_wr` or the command is one of `insert`, `delete`, `bulk-insert`, `bulk-delete`. `sequence`, `put-file`, `get-file-path` and `restore` are allowed. If P0.3 finds another text-path record mutation, halt. Print the text refusal, unlock the taken lock and `goto timing` (the existing exit).
   - **`auto_vacuum_sweep_one` / `auto_reshard_sweep_one`** (code-review only, recorded as such at hand-back; their timers make a deterministic test disproportionate): check the flag before the `AUTO-RESHARD ... starting` WARN / the equivalent vacuum log; that first check returns silently. Check again right after they take the object wrlock (that is, after their recommend/threshold early returns, so the WARN fires only when the object would really be processed), skip a flagged object. Unlock and log WARN `auto-vacuum: skipping d/o (bitmap repair pending)` / `auto-reshard: skipping d/o (bitmap repair pending)`.
5. `index.c::cmd_reindex`, per object, still under its wrlock (move `objlock_wrunlock` after this step):
   - Let `pending = slotcask_bmfix_marked_shards(data_dir, 0, NULL) != 0 || slotcask_bmfix_disabled_exists(data_dir) != 0`. A −1 from either counts as pending. No mask is needed: reindex rebuilds everything.
   - If `pending && !composites_only`: rebuild with `reindex_object_durable_checked_ex` instead of `reindex_object_checked`. On rc 0: `slotcask_bmfix_clear`, then `slotcask_bmfix_disabled_remove`. Log WARN `REINDEX d/o: bitmap repair markers cleared`, plus `; bitmap planning re-enabled` only when `removed == 1`. The data directory is `<eff_root>/<obj>`, built by the executor with the same `snprintf` pattern `bmfix_recover_all` uses. Count as rebuilt or skipped as today. Any failure (rebuild, clear, remove) → `objects_failed++`, WARN log with errno, markers/flag left as they are.
   - If `pending && composites_only`: rebuild as today, touch neither, log WARN `REINDEX d/o: composites-only reindex leaves bitmap repair pending`.
   - Otherwise unchanged. The response shape is unchanged.
6. `query_maint.c::cmd_restore`: after the restored `indexes/` is in place and before the object wrlock is released, if `slotcask_bmfix_disabled_exists(<object dir>)` is nonzero, `atomic_store(&g_bmfix_any_disabled, 1)` and log WARN `RESTORE d/o: restored object has bitmap planning disabled; run reindex`. No new test (it needs a backup taken while flagged). Record in the evidence file that this path is covered by code review only, and say so at hand-back.

### Phase 4 — proofs

1. Build; all nine new cases green; existing `test-bitmap-resplit-*` cases green.
2. Revert proof:
   - Right after the P1 base evidence and before any production code, save seam + tests as `git diff > <scratch>/plan-e-seam.patch`.
   - After P3: `git diff > <scratch>/plan-e-full.patch`; `git checkout -- src/db src/test`; `git apply <scratch>/plan-e-seam.patch` (seam + tests only); rebuild; run the RED cases and confirm their failing sets match P1.
   - Then `git checkout -- src/db src/test` and `git apply <scratch>/plan-e-full.patch`; rebuild; rerun green.
   - Any `git apply` failure → halt. `<scratch>` is outside the repo; the patches are deleted at hand-back.
3. Mutation proofs. Apply one at a time, rebuild, run the named case, record the output, then revert and rebuild:
   - (a) delete the unclean-branch `bmfix_recover_all` call → `test-bmfix-unclean-with-kfm2` red;
   - (b) make the flag-write failure path continue instead of refusing → `test-bmfix-escape-flag-write-fails` red;
   - (c) skip the `object_has_pending_markers` check → `test-bmfix-unclean-repair-fails-with-kfm2` red;
   - (d) make an invalid marker name return −1 → `test-bmfix-repair-bad-marker-name` red;
   - (e) remove the JSON gate → `test-bmfix-escape-disabled` red at the JSON refusal; (e′) remove the text gate → red at the text refusal;
   - (f) remove the `composites_only` guard in `cmd_reindex` → `test-bmfix-escape-disabled` red at "flag still present";
   - (g) remove the `field_has_index_type` gate → `test-bmfix-escape-disabled` red at the explain assertion.
   - The `field_index_type` gate is defense in depth: its callers are reached only after the gated planner chose btree. Record it as code-review only; if P0.6 found the dual declaration possible, add (h): remove that gate → red at the dual-declaration assertion.
4. Docs, same diff:
   - `AGENTS.md` "Bitmap slot coherence" final sentence (shard-scoped repair, escape flag, write refusals, residual refuse-on-pending-KFM2 case);
   - every P0.9 doc hit;
   - `docs/reference/error-codes.md` (the refusal error);
   - `docs/reference/changelog.md` `## Unreleased` (including the old-binary behaviour for `.bmfix-disabled`, that a full `reindex` now clears repair markers, and that `restore` takes the backup's flag/marker state);
   - the `slotcask.c` comments that say only startup clears markers.
   Map each doc claim to its evidence.

### Phase 5 — full suite and hand-back

1. Fresh build; `rtk ./build/bin/shard-db-test run-all --jobs 4`; record the totals: baseline + 9 cases, **0 failed**; clean up. A failure gets root-caused, never rerun until green.
2. Definition of done: no new warnings, no TODO/FIXME/debug output, no mutation left. `git status --short` = this plan's files + preserved untracked docs.
3. Hand back (in the evidence file and in chat): `git diff --stat`, P0 audits, P1 seam probe and base evidence, P4 revert and mutation proofs, P5 totals. State plainly that the work is uncommitted and the sanitizer gate is deferred to the branch gate after Plan I. Request a blind raw-diff review (it touches startup recovery, lock-holding dispatch and an on-disk artifact).

---

## Blind review rounds

- **Round 1** (fresh reviewer, plan + repo only): 1 blocker, 7 majors, 9 minors. Fixes:
  - B1: `cmd_reindex` cleared repair state after a composites-only or non-durable rebuild → durable rebuild required, composites-only leaves state, test + mutation (f).
  - M1: flag-without-markers was green on base → inode assertion.
  - M2: two "green" cases were red on log text → reclassified.
  - M3: `field_index_type` bypassed the gate → gated + audited.
  - M4: seam probe needs the daemon hook → seam first, probe in P1b, counter reset.
  - M5: F7a rows exceeded `BR_N` and `kfm2-a` was outside the oracle → rows 1500–1750 via `br_fault_stage_bulk`.
  - M6: log levels → WARN/ERROR mandated.
  - M7: expected-failure start → raw-spawn helper.
  - Minors fixed: single `build_bitmap_pass` caller, `splits` source, selector placement/casts/TSan re-read, gate frees, text-path test, explain substring via P0.7, consistent flag-removal policy under lock, `auto_vacuum_sweep_one`, restore documented.
- **Round 2** (new reviewer): 0 blockers, 5 majors, 11 minors. Fixes:
  - M1: `restore` can bring the flag in while serving → `cmd_restore` arms the atomic; Brief corrected.
  - M2: RED cases fail several assertions → full expected sets listed.
  - M3: `br_marker_exists` matches the flag → `br_repair_marker_exists`.
  - M4: async serving-time logs → `br_fault_stage_log` polling.
  - M5: seam before every start.
  - Minors fixed: NULL-mask count mode, schema loaded only when markers exist, relaxed atomic counter reset, reindex marker-clearing flagged as a behaviour change, sweep WARN placement, deadline rule fixed, call shapes pinned, `exists` −1 handled, F7a claim narrowed, P0.6 fallback assertion, concrete revert-proof patches.
- **Round 3** (new reviewer): 0 blockers, 1 major, 17 minors. The major: flag read/remove failures refused startup, an untagged trap; now they continue flagged (new DEFAULT row). Minors fixed: errno preserved, strict hex grammar, one explicit P3.1 sequence with lock scope, F7a helper, table/list aligned, seam rule, P0.6 variant specified, store order, `test-marker-v2` smoke, newline/missing-key shapes, closed text-gate set, auto-sweep code-review-only plus pre-log check, new test for reindex clearing serving markers, residual-cost/orphan notes, probe folded into F7a.
- **Round 4** (new reviewer; the CORE-PROCESS cap): 0 blockers, 2 majors, 10 minors. Both majors were wording/precision, not design:
  - M1: "skip to step 6" could be read as leaving the per-object loop → now "continue with the next object".
  - M2: the Brief understated the gate → the full `mode_is_schema` list is now in the Brief, decision row and docs.
  - Minors fixed: `remove` signature, `field_index_type` gate marked defense in depth, errno captured early, lock-failure policy, inode+ctime snapshot after stop, explicit setups, arm order and grep, reindex log wording, tags/order/consistency, "transitively".
  - **Not re-reviewed by a fifth round (cap reached).** Escalated to the human per CORE-PROCESS 1c: the human may approve on the Brief, or ask for one more blind round on these round-4 edits.
