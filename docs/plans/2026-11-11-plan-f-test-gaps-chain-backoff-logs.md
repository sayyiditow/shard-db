# Plan F — deterministic chain-merge coverage, back-off seam, scoped log assertions (F7c + F7d + F7e)

## Brief

**Primary goal:** Close the three remaining weak-test rows of the bitmap-resplit follow-ups (F7c, F7d, F7e) so that each one is *provably* load-bearing: a test that turns red when the code it guards is broken, with no timing bets and no assertion that can pass on its own setup.

**Problem and root cause**
- **F7c.** `test-bitmap-resplit-chain` races four clients and never forces a commit-chain merge, so it can pass without a merge having happened and does not turn red when the joiner's `chain_pending` pre-grow term is removed. The 2026-11-07 draft proposed new daemon control-channel hooks, but (a) an in-process deterministic merge choreography already exists (`g_shard_test_pause_*` at the `PRE_MERGE` / `GATE_WAIT` seams, used by `test-request-flush-batching` scenario 13), and (b) the draft's red leg is unreachable: chain admission is now capacity-capped (`max_chain_records` in `slotcask_bulk_shard_pipeline` / `bulk_gate_waiter_take_matching`), so a joiner that would overflow the table is simply not admitted. What `chain_pending` actually protects is the 75% load invariant of the chain's table, which is directly observable as the shard's kf capacity.
- **F7d.** `test-bitmap-resplit-fault-prepare` and `-fault-rename` call `sleep(6)` to out-wait a hard-coded 5 s growth back-off (`kfcache_resplit_locked`, two `+ 5000ULL` sites). The sleep is redundant (the retry loop that follows polls for up to 20 s) and the tests never assert that a back-off exists or how long it is.
- **F7e.** `br_log_has` greps the whole test base directory. Log routing is binary: while the async log writer runs, lines go only to `<base>/logs/*`; before it starts (startup recovery) lines go only to stderr, which the tests capture into `<base>/daemon-restart.log` (fd-2 capture) or `<base>/refuse.log` (raw spawn). A whole-base grep can therefore pass on `db.env`, on an earlier run's log, or on the wrong phase's output. The corrupt-bitmap case also asserts an async serving-time line with a single un-polled grep (latent flake).
- **Carried item (Plan E review).** The one-shot `INDEX_TEST_FAIL_NEXT_BUILD` knob is consumed by both `reindex_object_checked_impl` and `bitmap_rebuild_shards_durable`; whichever runs first eats it.

**Logical flow**
1. Seams first (test infrastructure only): `g_shard_test_bmfix_backoff_ms` + control selector 7 + fixture wrapper; a separate one-shot repair knob (variable, setter, db.env parse) that nothing consumes yet; a scoped log helper API with a *whole-base shim* body.
2. Write the new/edited tests against the seams, run them on that base state, record the exact failing sets (RED where the production piece is missing; GREEN-by-design for coverage-only tests).
3. Production switches: `kfcache_resplit_locked` reads the back-off through `kf_bmfix_backoff_ms()` (TEST_BUILD seam, production constant 5000); `bitmap_rebuild_shards_durable` consumes the repair knob instead of the shared one; the four Plan E cases that arm a repair failure switch to the new db.env knob.
4. Replace the log-helper shim with the real scoped implementation; migrate all call sites.
5. Revert proof, three mutation proofs for the coverage-only chain tests, full suite.

**Decisions made**

| Decision | Alternatives considered | Why | Tag |
|---|---|---|---|
| F7c is an **in-process slotcask** test (new file, three cases: shard 0 of 8, shard 300 of 512, admission-cap serialization) using the existing pause seams. The observable is the shard's kf capacity (64 at the owner's park, 128 after the chain) plus `g_shard_test_bulk_chains`. | (B) The draft's wire-level design: two new control-channel hooks (kinds 4/5, REACHED phases 5/6) so the merge can be forced through the daemon with real bitmaps. | In-process reuses proven seams, needs no daemon plumbing, uses exact shard-pinned keys (no hash-spread statistics), and has a unique red signal for each mutation. **Residual if you choose A:** a joiner-triggered resplit with already-committed bitmap bits is still covered only by the racy wire case `test-bitmap-resplit-chain` (oracle smoke, header comment corrected to say so). B would close that residual at the cost of ~80 lines of daemon test plumbing and a new latch design that needs its own review. | **NEEDS YOU** |
| Split the shared fail-next knob: new db.env `BMFIX_REPAIR_TEST_FAIL_NEXT=<errno>` consumed only by `bitmap_rebuild_shards_durable`; `INDEX_TEST_FAIL_NEXT_BUILD` stays for `reindex_object_checked_impl` (BTRI migration + wire reindex). Four Plan E cases switch knob name. | Leave shared; document the consumption order | You asked for a decision. Cheap, TEST_BUILD-only, and a new case (`test-bmfix-test-knobs-independent`) pins independence both ways; the shared knob made the escape cases depend on "no other build runs first", which a future migration+marker case would break silently. Override if you prefer zero churn in the committed Plan E tests. | DEFAULT |
| F7d seam is **control-channel only** (selector 7, `g_shard_test_bmfix_backoff_ms`); no db.env knob | db.env knob as in the stale draft | The tests arm it on the running daemon before the fault; a db.env knob adds a second parse path for no use. Production value stays 5000 and the `retry in 5s` log text is unchanged (it is inaccurate under the seam; acceptable, TEST_BUILD only). | DEFAULT |
| F7d proof is **two-sided and sleep-free**: seam = 2500 ms; immediately after disarm one retry must still be rejected; the successful retry must land in [2250, 4000) ms after the failure. The 100 ms poll in `br_fault_stage_retry` remains (a poll, not a sleep). | Shorten to 300 ms and `sleep(1)` (stale draft); attempt-count bound | Wall-clock bounds are one-sided safe: the back-off cannot expire early (CLOCK_MONOTONIC) and the unseamed 5 s default cannot land under 4000. Attempt-count bounds depend on per-attempt latency. | DEFAULT |
| F7e uses **three explicit scopes** (`BR_LOG_SERVING` = `<base>/logs`, polled; `BR_LOG_RESTART` = `<base>/daemon-restart.log`; `BR_LOG_REFUSE` = `<base>/refuse.log`), `grep -F`. The corrupt-bitmap `KF_BMFIX_SKIP` assertion moves to the polled serving scope. A dedicated case pins isolation with planted tokens (positive control + 9 negative cross-checks). | One `<base>/logs`-only helper (stale draft) | The draft's single scope would break every startup-line assertion from Plan E (those lines never reach `logs/`). | DEFAULT |
| New file `test_bulk_chain_pregrow.c` (+ `build.sh`) for F7c | Add scenarios to `test_request_flush_batching.c` | That file's helpers are hard-wired to 8 splits and its scenarios share one db (shard-0 totals carry between scenarios, breaking exact capacity math). | DEFAULT |

**Blast radius:** test plumbing only plus two TEST_BUILD-guarded production-file edits. `slotcask.c` (back-off helper; both call sites; seam definition), `shard_test_ctl.h`, `test_control.c` (selector 7), `fixtures.c/.h` (wrapper), `index.c` + `types.h` + `config.c` (repair knob), `build.sh`, `test_bitmap_resplit.c`, one new test file. Production (non-TEST_BUILD) binaries compile the same behaviour: back-off 5000 ms, the new setter is a no-op stub. No wire, CLI, config-key, on-disk or user-doc change. `INDEX_TEST_FAIL_NEXT_BUILD` consumers: `test_null_migration.c` (unchanged, BTRI path) and four Plan E cases (renamed). The follow-ups ledger row F7 is closed by this plan; it is not edited (Plans A–E left that doc alone).

**Test strategy**
- F7c: owner parks at `PRE_MERGE` with the table still at 64 slots; joiner proven queued; release; exactly one chain; the joiner's stage grew the table to 128 *because of* `chain_pending`; 75% load invariant; all records visible; no markers. Same on shard 300 of a 512-shard db (ids ≥ 256). Third case: a joiner too big for the remaining capacity is not admitted (two chains, still capacity 128).
- F7d: two existing cases; held-back-off rejection right after disarm; success within the seam interval.
- F7e: isolation case over three scopes; 12 existing call sites re-scoped.
- Knob split: one case, both directions.
- Every RED case lists its exact base-failing assertion set; coverage-only cases carry mutation proofs.

**Out of scope:** the wire-level bitmap-bearing chain-growth test (see decision 1); `test_bulk_stage_ungrown_backoff` (already polls, no sleep bet; could adopt the seam later to save ~5 s); `test_bitmap_cache_lock_errors.c` log handling; F1–F6, F8–F11; benchmarks; the branch sanitizer gate (runs once after Plan I); editing the follow-ups ledger; committing anything.

**Approval:** Human approved this Brief on 2026-11-11, with the NEEDS YOU row as recommended (in-process F7c test; the wire-level alternative is out of scope). Review converged in round 4 (0 blockers, 0 majors). Execution is authorized.

**Review log:** see the end of this file. The 2026-11-06 draft `2026-11-06-bitmap-test-gaps-chain-backoff-logs.md` is superseded by this file (written before Plans A–E; its hook kinds 4/5 and phase numbering collide with later work, its F7c red leg is unreachable behind the admission cap, its F7e scope breaks startup-line assertions, and its F7d consumes SET_GLOBALS selector 6, now taken).

---

## Executor Plan

Planner: Claude Sonnet 5.5. Executor: GLM 5.3 Flash (per the human). Derived against local commit `18678ff` (Plans A–E present) on `fix/type-correctness-recovery`. Proceed only after the human has approved the Brief. If the human chose alternative (B) on decision 1, this Plan does not cover it: halt.

### Execution contract

- Work on the existing `fix/type-correctness-recovery` branch; never create/switch branches, never touch `main`. First confirm `git rev-parse --short HEAD` = `18678ff` (or a descendant the human names) and a clean **tracked** tree. Keep every untracked plan/evidence file. Leave all work **uncommitted** for raw-diff review. No git writes, push, PR, benchmarks, or sanitizer runs.
- Follow `AGENTS.md` and `CORE-PROCESS.md`. Do the phases in order, test-first.
- Plan mismatch (a symbol or behaviour described here doesn't exist or behaves differently): write `PLAN_NOTES-planf.md` with the evidence and halt. If a symbol merely moved and its new place is unambiguous, record that and proceed. A decision this plan doesn't cover means halt and ask. Never weaken, skip or loosen a test. Never rerun an unexplained failure until it goes green.
- Commands: every shell invocation starts with `rtk`. Build: `rtk proxy env SKIP_TESTS=1 ./build.sh`. Single case: `rtk ./build/bin/shard-db-test run <name>`. Full suite: `rtk ./build/bin/shard-db-test run-all --jobs 4`. Capture the real exit status (no `| tail` hiding it). After each test invocation, once its processes have stopped, `rm -rf /tmp/shard-db-*`.
- Record all evidence (commands, output, exit status) in the new file `docs/plans/2026-11-11-plan-f-execution-evidence.md`, in the style of `docs/plans/2026-11-11-plan-e-execution-evidence.md`.
- `ASSERT_*` does not stop at the first failure: judge every RED case by its **full** set of failing assertions. That set must equal the "expected on base" set in this plan; a failing assertion outside it, or a missing one → halt and report.
- Daemon-backed cases in `test_bitmap_resplit.c`: before every direct start call `br_seam_once(root)` (db.env must carry `KF_TEST_INITIAL_SLOTS=64`). Every case owns its daemon and tmpdir and stops it on every path.

### Phase 0 — discovery and audit (evidence only, no code)

Record each item in the evidence file; any mismatch → halt.

1. `git rev-parse --short HEAD`, `git status --short` (tracked clean), `git branch --show-current`.
2. Anchor counts (each must match):
   ```bash
   rtk proxy grep -cF 'e->bmfix_retry_ms = kf_now_us() / 1000ULL + 5000ULL;' src/db/slotcask.c        # 2
   rtk proxy grep -cF 'static inline uint64_t kf_now_us(void) {' src/db/slotcask.c                    # 1
   rtk proxy grep -cF '_Atomic int g_shard_test_bmfix_fault_count;' src/db/slotcask.c                 # 1
   rtk proxy grep -cF 'extern _Atomic int g_shard_test_bmfix_fault_count;' src/db/shard_test_ctl.h    # 1
   rtk proxy grep -cF 'atomic_store(&g_shard_test_bmfix_fault_count, 0);' src/db/shard_test_ctl.h     # 1
   rtk proxy grep -cF '#define TEST_HOOK_SET_GLOBALS_FAIL_STICKY 6' src/db/test_control.c             # 1
   rtk proxy grep -cF '} else if (msg.phase == TEST_HOOK_SET_GLOBALS_FAIL_STICKY) {' src/db/test_control.c   # 1
   rtk proxy grep -cF 'int test_env_ctl_set_json_escape_fail(TestEnv *env, int fail) {' src/test/fixtures.c  # 1
   rtk proxy grep -cF 'int test_env_ctl_set_json_escape_fail(TestEnv *env, int fail);' src/test/fixtures.h   # 1
   rtk proxy grep -cF 'void index_test_fail_next_build(int en);' src/db/types.h                       # 1
   rtk proxy grep -cF 'index_test_fail_next_build(atoi(p + 27));' src/db/config.c                     # 1
   rtk proxy grep -cF 'src/test/cases/test_bulk_stage_ungrown.c \' build.sh                           # 1
   rtk proxy grep -cF 'sleep(6);' src/test/cases/test_bitmap_resplit.c                                # 2
   rtk proxy grep -cF 'static int br_log_has(Br *b, const char *what) {' src/test/cases/test_bitmap_resplit.c  # 1
   rtk proxy grep -cF 'static int br_fault_stage_log(Br *b, const char *what) {' src/test/cases/test_bitmap_resplit.c  # 1
   ```
3. **Seam region.** `slotcask.c` lines ~44–80: the `g_shard_test_*` definitions sit inside a `#ifdef TEST_BUILD` region (record the matching `#endif`). `kf_now_us` is `CLOCK_MONOTONIC` and is defined *before* `kfcache_resplit_locked`; `kf_max_slots` is defined *after* it (so the new helper goes right after `kf_now_us`).
4. **Back-off semantics.** Quote the gate at the top of `kfcache_resplit_locked` (`new_cap > kh->capacity && e->bmfix_retry_ms > t_start / 1000ULL && e->bmfix_retry_gen == gen` → `EAGAIN`) and both setter sites (prepare failure, rename failure; both growth-only).
5. **Chain facts.** Quote: `max_chain_records` computation after the owner's stage in `slotcask_bulk_shard_pipeline`; the two admission clauses in `bulk_gate_waiter_take_matching` (`admitted_records <= max_chain_records` and `w->in->nrecs <= max_chain_records - admitted_records`); `bulk_input_chain_admissible`; the joiner's `chain_pending` argument and `uint64_t projected = kh.hdr->total + chain_pending + (uint64_t)n;`; `atomic_fetch_add(&g_shard_test_bulk_chains, 1)`; that `SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_PRE_MERGE)` is after the owner's stage and before `take_matching`, and `SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_GATE_WAIT)` in `writer_gate_lock` is after the caller's queue push and before the blocking lock. Record that the pause counter's `hit != occurrence` path returns immediately (so B "hits" GATE_WAIT without parking, as scenario 13 relies on).
6. **In-process db facts.** `slotcask_open(db, dir, num_shards, num_streams, slot_size)` sets `slots_per_shard` from `slotcask_default_slots_for_splits`, which honours `g_shard_test_kf_initial_slots`; `SLOTCASK_MAX_SHARDS` = 4096 (512 is legal); the runner's process-local `g_db` exists (ENODEV otherwise — `test_request_flush_batching.c` opens in-process the same way); `shard_test_ctl_reset()` zeroes the kf-tier seam (so tests must re-store it after any reset); `kf_path_for` and `kfcache_acquire/release` are public (`slotcask.h`), as `test_kf_uncapped_growth.c::shard0_capacity` uses.
7. **Log routing.** Quote `log_msg_sub` (`config.c`): while `g_log_running` the entry goes to the ring → `<LOG_DIR>/<date>-error.log` (ERROR) or `<date>-info.log` (WARN/INFO/DEBUG); otherwise to a handler or `stderr`. Record the call order showing `shard_db_recover_before_stamp` / `bmfix_recover_all` run before `log_init` in daemon start (`rtk grep -n 'log_init\|shard_db_recover_before_stamp' src/db/*.c`). Fixture db.env sets `LOG_DIR=<base>/logs` and `LOG_LEVEL=2` (`fixtures.c`); `<base>/daemon.log` holds the first-start stdout/stderr; `br_stderr_capture` writes `<base>/daemon-restart.log` (O_TRUNC per capture); `br_spawn_expect_exit` writes `<base>/refuse.log` (O_TRUNC).
8. **Call-site inventory** (must be exactly these 12; record `grep -n 'br_log_has\|br_fault_stage_log' src/test/cases/test_bitmap_resplit.c` output): serving scope — `BULK_STAGE_UNGROWN` and `KF_BMFIX_PREPARE_FAILED` in `test_bitmap_resplit_fault_prepare` (2) and `BULK_STAGE_UNGROWN` in `..._fault_rename` (1), `KF_BMFIX_SKIP` in `..._corrupt_bitmap` (1, currently un-polled), `bitmap planning re-enabled` (polled) in `test_bmfix_escape_disabled` (1); restart scope — `bitmap rebuild of 1 shard` (`test_bmfix_repair_shard_scoped`), `bitmap planning DISABLED` (`test_bmfix_escape_disabled`, `test_bmfix_escape_heals_at_startup`), `bitmap planning re-enabled` (heals case), `stale disabled flag removed` (`test_bmfix_flag_without_markers`) (5); refuse scope — `could not write the disabled flag` (`..._flag_write_fails`), `with pending commit markers; startup refused` (`..._unclean_repair_fails_with_kfm2`) (2). For each restart-scope site confirm an fd-2 capture (`br_stderr_capture`/`br_stderr_restore`) wraps the start that produced it; for each refuse-scope site confirm `br_spawn_expect_exit` produced it. Any discrepancy → halt.
9. **Knob consumers.** `g_index_test_fail_next_build` is read/zeroed at exactly two sites (`reindex_object_checked_impl`, `bitmap_rebuild_shards_durable`); `index_test_fail_next_build` is declared in `types.h` and has a no-op stub outside TEST_BUILD (record how `types.h` guards the declaration); `test_null_migration.c` arms it via `mig_write_env`. The Plan E knob uses: `br_escape_start` (`br_env_append(root, "export INDEX_TEST_FAIL_NEXT_BUILD=5")`, shared by `test_bmfix_escape_disabled` and `test_bmfix_escape_heals_at_startup`), `test_bmfix_escape_flag_write_fails`, `test_bmfix_unclean_repair_fails_with_kfm2`, and one `br_env_remove_line(root, "INDEX_TEST_FAIL_NEXT_BUILD")` in the heals case: 4 `INDEX_TEST_FAIL_NEXT_BUILD` text sites in `test_bitmap_resplit.c` in total. `rtk grep -rn INDEX_TEST_FAIL_NEXT_BUILD src docs AGENTS.md` outside `docs/plans/`: only the files above.
10. **Control channel.** `TEST_HOOK_SET_GLOBALS` selectors 0–6 are taken; the handler's final `else` applies the kf seams, so selector 7 must be an `else if` placed **before** it; the handler ACKs every SET_GLOBALS; `TestHookMessage.a/.b` are `uint64_t`.
11. **Reindex wire shape.** A failing build in `cmd_reindex` returns `{"error":"reindex failed",...}`; success returns `{"status":"reindexed",...}` (quote `index.c` `cmd_reindex` tail).
12. `test_env_start`'s fixture db.env contains the line `export LOG_LEVEL=2` (quote it) and no engine code logs the string `LOG_LEVEL=2` (`rtk grep -n 'LOG_LEVEL=2' src/db`: config parse only).

### Phase 1 — seams first (infrastructure only, before any test)

**1a. Back-off seam (inert).**
- `slotcask.c`, inside the TEST_BUILD seam block, after `_Atomic int g_shard_test_bmfix_fault_count;` add `_Atomic long g_shard_test_bmfix_backoff_ms;`.
- `shard_test_ctl.h`: after `extern _Atomic int g_shard_test_bmfix_fault_count;` add `extern _Atomic long g_shard_test_bmfix_backoff_ms;` with a comment (growth back-off interval in ms; 0 = production 5000; set via control selector 7); in `shard_test_ctl_reset` after `atomic_store(&g_shard_test_bmfix_fault_count, 0);` add `atomic_store(&g_shard_test_bmfix_backoff_ms, 0);`.
- `test_control.c`: add `#define TEST_HOOK_SET_GLOBALS_BMFIX_BACKOFF 7` after the `...FAIL_STICKY 6` define; add, immediately after the `FAIL_STICKY` branch and before the final `else`:
  ```c
              } else if (msg.phase == TEST_HOOK_SET_GLOBALS_BMFIX_BACKOFF) {
                  /* a = interval in ms ((int64_t) round-trip); <= 0 means the
                     production default. */
                  atomic_store(&g_shard_test_bmfix_backoff_ms, (long)(int64_t)msg.a);
  ```
- `fixtures.h` / `fixtures.c`: `int test_env_ctl_set_bmfix_backoff(TestEnv *env, long ms);` declared after `test_env_ctl_set_fail_phase`; defined before `test_env_ctl_set_json_escape_fail` as `return ctl_set_globals(env, 7, (uint64_t)(int64_t)ms, 0);` (comment: phase 7 = `TEST_HOOK_SET_GLOBALS_BMFIX_BACKOFF`).
- Nothing consumes the variable yet.

**1b. Repair-knob seam (inert).**
- `index.c`, directly after the `index_test_fail_next_build` `#ifdef TEST_BUILD … #else … #endif` block, add the same shape for `g_bmfix_repair_test_fail_next` (`static _Atomic int`) and `void index_test_bmfix_repair_fail_next(int en)` (TEST_BUILD: store `en > 0 ? en : 0` with release; else: `(void)en;`). Comment: one-shot, consumed only by `bitmap_rebuild_shards_durable`.
- `types.h`: declare it next to `index_test_fail_next_build` with the same guard treatment P0.9 recorded.
- `config.c`, in the TEST_BUILD env chain right after the `INDEX_TEST_FAIL_NEXT_BUILD=` branch: `} else if (strncmp(p, "BMFIX_REPAIR_TEST_FAIL_NEXT=", 28) == 0) { index_test_bmfix_repair_fail_next(atoi(p + 28)); ` (28 = `strlen("BMFIX_REPAIR_TEST_FAIL_NEXT=")`, verify with `printf %s 'BMFIX_REPAIR_TEST_FAIL_NEXT=' | wc -c`). Comment like the neighbour's.
- `bitmap_rebuild_shards_durable` is **not** switched yet.

**1c. Scoped log API with a whole-base shim (test code).** In `test_bitmap_resplit.c`:
- Add `#include <time.h>`.
- Add after `br_remap_leftovers`:
  ```c
  typedef enum { BR_LOG_SERVING, BR_LOG_RESTART, BR_LOG_REFUSE } BrLogScope;
  static const char *const br_log_scope_rel[] = { "logs", "daemon-restart.log", "refuse.log" };
  /* (the shim body references it via `(void)br_log_scope_rel;` to avoid -Wunused-const-variable until Phase 3) */
  /* Single-shot grep of ONE log source (F7e). SERVING = <base>/logs (async
     writer; only lines logged while the daemon serves), RESTART = the fd-2
     capture of the latest direct start (startup recovery lines are written
     synchronously to stderr before the log writer exists), REFUSE = the raw
     spawn's stdout/stderr. grep -F: the needle is data, not a regex. */
  static int br_log_grep(Br *b, BrLogScope sc, const char *what) {
      (void)sc; (void)br_log_scope_rel; (void)br_now_ms;   /* SHIM (removed in Phase 3): whole-base grep, the old behaviour */
      return tu_run_cmd("grep -rq '%s' \"$(dirname '%s')\" 2>/dev/null", what, b->env.db_root) == 0;
  }
  /* Positive assertion helper: serving-time lines drain through the async
     writer, so that scope polls (bounded 5 s); startup scopes are synchronous. */
  static int br_log_has(Br *b, BrLogScope sc, const char *what) {
      int tries = sc == BR_LOG_SERVING ? 500 : 1;
      for (int i = 0; i < tries; i++) {
          if (br_log_grep(b, sc, what)) return 1;
          if (i + 1 < tries) usleep(10000);
      }
      return 0;
  }
  ```
  Delete the old `br_log_has` and `br_fault_stage_log`; migrate the 12 P0.8 call sites (scopes: serving = the 4 `br_fault_stage_log` sites (482, 483, 522 and the escape-disabled `bitmap planning re-enabled`) + `KF_BMFIX_SKIP`; restart = `bitmap rebuild of 1 shard`, both `bitmap planning DISABLED`, the heals-case `bitmap planning re-enabled`, `stale disabled flag removed`; refuse = `could not write the disabled flag`, `with pending commit markers; startup refused`; `br_fault_stage_log(b, X)` always maps to `BR_LOG_SERVING`) to `br_log_has(b, <scope>, "…")` (`br_fault_stage_log(b, X)` → `br_log_has(b, BR_LOG_SERVING, X)`). The shim keeps every existing case's behaviour identical, so the existing cases still pass.
- Add `static long br_now_ms(void)` (CLOCK_MONOTONIC → ms) **above** the log helpers (the shim body references it; the `(void)br_now_ms;` in the shim avoids an unused-static warning until T2 uses it).
- Fix the comment on `br_stderr_capture` that says "br_log_has (grep -r over <base>)" to name the RESTART scope.

Build. Run the smoke set and require pass (exit 0, paste): `test-bitmap-resplit-fault-prepare`, `-fault-rename`, `-corrupt-bitmap`, `test-bmfix-escape-disabled`, `test-bmfix-escape-heals-at-startup`, `test-bmfix-flag-without-markers`, `test-bmfix-repair-shard-scoped`, `test-bmfix-escape-flag-write-fails`, `test-bmfix-unclean-repair-fails-with-kfm2`, `test-request-flush-batching`.

### Phase 1b — tests and base evidence (continues Phase 1)

**T1 — `src/test/cases/test_bulk_chain_pregrow.c` (new; register in `build.sh` right after `src/test/cases/test_bulk_stage_ungrown.c \`).** Three cases, one shared scenario function. Header comment: F7c; what is pinned and why (see Brief); runs in-process, own tmpdir.

Scaffold (mirror the shapes in `test_request_flush_batching.c`; do not modify that file):
- Includes: `test_runner.h`, `test_assert.h`, `types.h`, `slotcask.h`, `shard_test_ctl.h`, `<dirent.h> <errno.h> <limits.h> <pthread.h> <stdatomic.h> <stdio.h> <stdlib.h> <string.h> <unistd.h>`; `extern void compute_hash_raw(const char *key, size_t key_len, uint8_t hash_out[16]);`.
- `#define BCP_TIER 64`, `#define BCP_MAXN 48`. `typedef struct { SlotcaskDb db; char base[PATH_MAX]; int splits; } Bcp;`.
- `bcp_ctl_reset()`: `shard_test_ctl_reset(); atomic_store(&g_shard_test_kf_initial_slots, BCP_TIER);`.
- `bcp_open(Bcp *w, int splits)`: `memset`; set the tier seam; `slotcask_init(64, 64)`; `mkdtemp("/tmp/shard-db-bulk-chain-XXXXXX")`; create `<base>/data` and `<base>/data/kf`; `slotcask_open(&w->db, w->base, splits, 1, 64)`; after `slotcask_open` set `w->db.bulk_commit_window = 4096` explicitly (`slotcask_open` copies `g_db->bulk_commit_window`, whose runner value is not fixed; any value ≥ 64 keeps each member one window); `bcp_ctl_reset()`. `bcp_open` returns -1 on any failure; the scenario then closes whatever opened and returns 1. `bcp_close`: `slotcask_close`, `rm -rf` the base (same shape as `rf_cleanup_dir`), `slotcask_shutdown()`, `shard_test_ctl_reset()`.
- `bcp_shard_of(w, key)`: `compute_hash_raw` then `compute_record_shard(h, w->splits)`.
- `BcpBatch { char keys[BCP_MAXN][24]; char vals[BCP_MAXN][24]; SlotcaskBulkRec recs[BCP_MAXN]; size_t n; }` and `bcp_batch_fill(w, b, prefix, n, shard, val)` (batch A uses val `"vA"`, batch B `"vB"`): key `"bc-%d-%04zu-%d"` (prefix, i, probe attempt) probed until `bcp_shard_of == shard`; value `"%s-%04zu"`; `recs[i]` = key/klen/value/vlen with everything else zero. (Keys unique across the two batches via different `prefix`.)
- `bcp_fill_opts` = `rf_fill_opts(o, NULL)` copied verbatim with local no-op hooks copied from `rf_noop_prepare`, `rf_noop_apply`, `rf_noop_terminal` (`test_request_flush_batching.c` lines ~154–168; rename `bcp_noop_*`) (`has_indexed_fields = 1`, `prepare_window/apply_window/commit_done/release_window/abort_window` no-ops).
- `BcpReq { Bcp *w; SlotcaskBulkShardInput in; int rc; int err; }`, `bcp_req_init(r, w, batch, shard)` (`in.kf_shard_id = shard; in.recs = batch->recs; in.nrecs = batch->n; in.kind = SLOTCASK_BULK_INPUT_UPSERT; bcp_fill_opts(&in.opts.upsert); in.rc = 0`), `bcp_req_thread` (`r->rc = slotcask_bulk_request_execute(&r->w->db, &r->in, 1); r->err = errno;`).
- `bcp_capacity(w, shard)`: `kf_path_for(p, w->db.data_dir, shard)`; `kfcache_acquire(&kh, p, BCP_TIER, 0)`; return `kh.capacity` (0 on failure); `kfcache_release`.
- `bcp_marker_scan(base)` = `rt_marker_scan` copied from `test_request_flush_batching.c` (count of `*_marker.dat` in `<base>/data/kf`).
- `bcp_wait_hits(n)`: poll `g_shard_test_pause_hits >= n` up to 30000 × 1 ms.
- `bcp_visible(w, key, want)` = `rf_record_visible` (`test_request_flush_batching.c` lines ~334–341) renamed: `slotcask_get(&w->db, key, strlen(key), &v, &vl)`, exact length+bytes match, free `v`.
- `Bcp` also carries `int db_opened` (set after a successful `slotcask_open`); `bcp_close` calls `slotcask_close` only when it is set.

Scenario `bcp_scenario(int splits, int shard, size_t na, size_t nb, int want_chains, size_t want_cap)` (returns `t_ctx->failed > 0 ? 1 : 0`; all allocations on the heap; every path joins both threads and closes the db):
1. `bcp_open`; fill batch A (prefix 1, `na`, shard) and B (prefix 2, `nb`, shard); `bcp_ctl_reset()`; `g_shard_test_pause_phase = SHARD_TEST_PHASE_PRE_MERGE; g_shard_test_pause_occurrence = 1;`.
2. Start thread A. `bcp_wait_hits(1)`. Assert: `pause_hits >= 1` ("owner parked at the PRE_MERGE seam holding the gate"); `bcp_capacity(shard) == 64` ("owner's stage left the table at 64 slots").
3. `g_shard_test_pause_phase = SHARD_TEST_PHASE_GATE_WAIT; g_shard_test_pause_occurrence = 1;` Start thread B. `bcp_wait_hits(2)`. Assert `pause_hits >= 2` ("joiner queued before the owner's take"). Always then `atomic_store(&g_shard_test_pause_release, 1)`; join A, B.
4. Assert: `ra.rc == 0` ("owner request ok"), `rb.rc == 0` ("second request ok"); `(int)atomic_load(&g_shard_test_bulk_chains) == want_chains` ("chain count"); `bcp_capacity(shard) == want_cap` ("kf table sized for the whole chain: 128 slots"); `(na + nb) * 4 < capacity * 3` ("75% load invariant holds after the chain"); every key of both batches visible with its value ("all records visible"); `bcp_marker_scan(base) == 0` ("no markers retained").
5. `bcp_close`.

Cases (register all three): `test-bulk-chain-pregrow` = `bcp_scenario(8, 0, 30, 30, 1, 128)`; `test-bulk-chain-pregrow-shard300` = `bcp_scenario(512, 300, 30, 30, 1, 128)`; `test-bulk-chain-admission-cap` = `bcp_scenario(8, 0, 30, 40, 2, 128)`.

Never reset or re-zero `g_shard_test_pause_hits` between the owner's PRE_MERGE hit and the switch to GATE_WAIT (the joiner's hit must be the 2nd, so it returns without parking). Why these numbers (put in the file comment): tier 64, total 0. Owner 30: `30*4=120 < 192` → no growth, `max_chain_records = 64`. Joiner 30 ≤ 64−30 → admitted; its stage projects `0+30+30=60` → `240 ≥ 192` → doubles to 128 (`240 < 384`). Without `chain_pending` it projects 30 and the chain commits 60 records into 64 slots (93.75% load). A joiner of 40 > 64−30 is not admitted: two chains; the second request's own stage projects `30+40=70` → 128.

**T2 — F7d edits in `test_bitmap_resplit_fault_prepare` and `test_bitmap_resplit_fault_rename`** (apply to both; anchors need only be unique inside each function):
- Add `#define BR_BACKOFF_MS 2500` near the other defines.
- Immediately before the `ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, SHARD_TEST_BMFIX_PREPARE|RENAME, 1000), 0, "arm: …");` line add `ASSERT_EQ_INT(test_env_ctl_set_bmfix_backoff(&b->env, BR_BACKOFF_MS), 0, "shorten the growth back-off (F7d seam)");`.
- **Bounded faulted request.** Take `long t_req0 = br_now_ms();` immediately before the first (fault-armed) `br_fault_stage_bulk` call and, right after `t_fail` is taken, add `ASSERT_TRUE(t_fail - t_req0 < 1000, "the faulted request left most of the back-off window");` (its latency counts against the window; the back-off starts inside it).
- **Reorder (flake control).** Keep the first `br_fault_stage_bulk(b, 220, 732, &inserted, &errors)` assertion ("fault-blocked growth rejects the new stages") and the two assertions that follow it (`inserted == 0` "no record committed from rejected stages", `errors == 512` "every submitted record was rejected") **exactly where they are, before `t_fail`** (they must keep checking the fault-armed request). Directly after those two assertions add `long t_fail = br_now_ms();`, then **move the `ASSERT_EQ_INT(test_env_ctl_set_bmfix_fault(&b->env, 0, 0), 0, "disarm");` line up to directly follow it**, delete the `sleep(6);` line and its comment, and insert right after the disarm:
  ```c
      long t_disarm = br_now_ms();
      ASSERT_TRUE(t_disarm - t_req0 < BR_BACKOFF_MS - 500,
                  "test is still inside the back-off window at the held-back-off check");
      ASSERT_EQ_INT(br_fault_stage_bulk(b, 220, 732, &inserted, &errors), 1,
                    "back-off still holds growth after the fault is disarmed");
      ASSERT_EQ_INT(inserted, 0, "back-off rejection committed nothing");
      ASSERT_EQ_INT(errors, 512, "back-off rejected every submitted record");
  ```
  The remaining existing assertions after those two (`br_count`, `br_check`, log polls, marker/temporary checks) stay, now **after** the held request; none depends on the fault being armed and the log lines persist. Only the disarm RPC and one request run between `t_fail` and the held check. `t_fail` is taken after the daemon replied, so it is never earlier than the back-off start of the shard that failed last; the `-250` slack in the lower bound covers that gap.
- After the existing `ASSERT_EQ_INT(br_fault_stage_retry(b, 220, 732), 0, "retry every rejected key succeeds");` add:
  ```c
      long waited = br_now_ms() - t_fail;
      ASSERT_TRUE(waited >= BR_BACKOFF_MS - 250, "growth stayed held for the seam interval");
      ASSERT_TRUE(waited < BR_BACKOFF_MS + 1500, "retry succeeded within the shortened back-off window");
  ```
- No `sleep(` remains in either function (`grep -c 'sleep(' ` over the two function bodies = 0; `usleep` polls elsewhere are untouched).

**T3 — `test-bmfix-test-knobs-independent` (new, in `test_bitmap_resplit.c`, `TEST_REGISTER`ed with the others).** Reuses `br_open(b,0)`, `br_seed_1500`, `br_env_append`, `br_seam_once`, `br_stderr_capture/restore`, `br_flag_exists`, `br_repair_marker_exists`, `br_check`, `br_q`. Allocate `Br` on the heap; every path `br_close`s.
- **Plumbing (all parts).** Save `port`/`root` from `b->env` before each stop; `tc_close(b->tc)` then `test_env_stop_keep(&b->env)` before every stop; after every started daemon `ASSERT_EQ_INT(br_connect(b), 0, "reconnect")` (if it fails, `br_close` and return) before any query.
- **Part A (build knob does not hit the repair path).** Seed 1500; `tc_close` + `test_env_stop_keep`; `truncate -s 33 indexes/flag/000.bm`; plant empty `indexes/.bmfix-000`; `br_env_append(root, "export INDEX_TEST_FAIL_NEXT_BUILD=5")`; `br_seam_once`; start with fd-2 capture. If the start fails, `br_close` and return (no further assertions). Assert, in this order:
  - A1 "startup OK"; A2 `!br_flag_exists(b)` "repair ran and succeeded: no disabled flag"; A3 `!br_repair_marker_exists(b)` "marker cleared by the repair"; A4 `br_log_has(b, BR_LOG_RESTART, "bitmap rebuild of 1 shard")` "repair ran" ; `br_check(b, "after repair")` (exact counts);
  - A5: `reindex {"mode":"reindex","dir":"br","object":"t"}` response contains `"error":"reindex failed"` ("the build knob was still armed: the repair did not consume it");
  - A6: the same request again contains `"status":"reindexed"` ("one-shot: consumed by the reindex").
- **Part B (repair knob does not hit reindex; it does hit the repair).** Stop the daemon (`test_env_stop_keep`); `sed -i` out the `INDEX_TEST_FAIL_NEXT_BUILD` line, append `export BMFIX_REPAIR_TEST_FAIL_NEXT=5`; restart with capture (no markers now). B1 "startup OK"; B2 full wire `reindex` contains `"status":"reindexed"` ("reindex ignores the repair knob"). Stop; truncate `flag/000.bm`, plant `.bmfix-000`; `br_seam_once`; restart with capture. B3 "startup OK"; B4 `br_flag_exists(b)` ("repair honours the repair knob: escape flag written"); B5 `br_log_has(b, BR_LOG_RESTART, "bitmap planning DISABLED")`.
- Expected on base (Phase 1 state: the repair path still consumes the shared knob; the new knob is parsed but inert): failing set = {A2, A3, A5, B4, B5}. (A2/A3: the shared knob made the repair fail, so the flag was written and the marker kept; A5: the knob was consumed so the reindex succeeds; B4/B5: the repair knob is ignored so the repair succeeds.) Everything else passes. **Assertion strings**: use the quoted text verbatim so the set can be compared.

**T4 — `test-bitmap-resplit-log-scope` (new, in `test_bitmap_resplit.c`).** `br_open(b, 0)` (a real fixture daemon, so `<base>/logs` and `db.env` exist). Plant, via `tu_run_cmd` with `$(dirname '%s')`: `<base>/logs/zz-canary.log` containing `CANARY-SERVING`; `<base>/daemon-restart.log` containing `CANARY-RESTART`; `<base>/refuse.log` containing `CANARY-REFUSE`. Assertions (descriptions verbatim):
- Positive controls (3, raw `br_log_grep`): "serving scope finds its own token", "restart scope finds its own token", "refuse scope finds its own token"; plus `br_log_has(b, BR_LOG_SERVING, "CANARY-SERVING")` "polled serving scope finds its token".
- Positive control for the db.env token: `tu_run_cmd("grep -q 'LOG_LEVEL=2' \"$(dirname '%s')/db.env\"", root) == 0` "db.env carries the LOG_LEVEL=2 token" (5th positive; passes on base).
- Negative cross-checks (9, each `!br_log_grep(...)`), tokens X ∈ {the other two scopes' tokens, `LOG_LEVEL=2` which exists only in `db.env`}: "serving scope must not see CANARY-RESTART", "…CANARY-REFUSE", "…LOG_LEVEL=2"; "restart scope must not see CANARY-SERVING", "…CANARY-REFUSE", "…LOG_LEVEL=2"; "refuse scope must not see CANARY-SERVING", "…CANARY-RESTART", "…LOG_LEVEL=2".
- Expected on base (whole-base shim): the 5 positive assertions pass; all 9 negative assertions fail.

**Base evidence.** Build; run T1×3, the two F7d cases, T3, T4. Expected:
- `test-bulk-chain-pregrow`, `-shard300`, `-admission-cap`: **GREEN** (coverage-only; they exercise already-correct production code; proof is in Phase 4 mutations).
- `test-bitmap-resplit-fault-prepare` and `-fault-rename`: **RED**, each failing set = {"retry succeeded within the shortened back-off window"}.
- `test-bmfix-test-knobs-independent`: **RED**, set = {A2, A3, A5, B4, B5} as above.
- `test-bitmap-resplit-log-scope`: **RED**, set = the 9 negatives.
- Paste each case's full output and exit status; a deviation from these sets → halt.
Record `git diff --stat` and `git status --short` here (the new test file is untracked).

### Phase 2 — production switches

1. `slotcask.c`: right after `kf_now_us` (before `kfcache_resplit_locked`) add:
   ```c
   /* F7d: growth back-off interval. Production 5000 ms; TEST_BUILD tests set
      g_shard_test_bmfix_backoff_ms (control selector 7) so fault cases never
      sleep against a hard-coded constant. */
   static uint64_t kf_bmfix_backoff_ms(void) {
   #ifdef TEST_BUILD
       long ms = atomic_load(&g_shard_test_bmfix_backoff_ms);
       if (ms > 0) return (uint64_t)ms;
   #endif
       return 5000ULL;
   }
   ```
   Replace **both** `e->bmfix_retry_ms = kf_now_us() / 1000ULL + 5000ULL;` with `e->bmfix_retry_ms = kf_now_us() / 1000ULL + kf_bmfix_backoff_ms();`. Do not change the `retry in 5s` log text.
2. `index.c::bitmap_rebuild_shards_durable`: switch its TEST_BUILD consumption block from `g_index_test_fail_next_build` to `g_bmfix_repair_test_fail_next` (log text unchanged). `reindex_object_checked_impl` is untouched.
3. `test_bitmap_resplit.c`: in the four P0.9 text sites replace `INDEX_TEST_FAIL_NEXT_BUILD` with `BMFIX_REPAIR_TEST_FAIL_NEXT` (3 `br_env_append` lines, 1 `br_env_remove_line` pattern). **T3 Part A keeps the old name** (it is the point of that case); P0.9's count of old-name sites outside T3 must drop to 0.
4. Add a one-line comment at `bitmap_rebuild_shards_durable`'s consume block naming `BMFIX_REPAIR_TEST_FAIL_NEXT` (the two consume blocks in `index.c` are identical except for the log text; the one to switch is the block whose log text is `BMFIX-RECOVER %s/%s: test seam failing bitmap rebuild`).
5. Correct the comment block in `test_bitmap_resplit.c` that begins "Concurrent bulk-inserts into one object: requests queued on a shard's writer gate merge into one commit chain" (above `BrChainArg`/`br_chain_main`) so it no longer claims to force a chain merge: say it is an oracle smoke test under concurrent bulk load, and that the deterministic merge/pre-grow proof is `test-bulk-chain-pregrow` (comment-only; done before the Phase 4 patch snapshot).
6. Build (no new warnings); run: the two F7d cases, T3, the four escape/refuse cases (`test-bmfix-escape-disabled`, `-escape-heals-at-startup`, `-escape-flag-write-fails`, `-unclean-repair-fails-with-kfm2`), `test-null-migration-c-refuses` (`test_null_migration.c`, arms `INDEX_TEST_FAIL_NEXT_BUILD`), all GREEN. Paste.

### Phase 3 — real log scoping

1. Replace the shim body of `br_log_grep` with
   ```c
   return tu_run_cmd("grep -rFq -- '%s' \"$(dirname '%s')/%s\" 2>/dev/null",
                     what, b->env.db_root, br_log_scope_rel[sc]) == 0;
   ```
   and at the top `if (strchr(what, '\'')) return 0;` (a needle with a single quote would break the shell; no current caller has one).
2. Run T4 (now GREEN), then every case that uses `br_log_has` (the 12 sites' cases + T3). All GREEN. If a restart-scope assertion fails, the capture does not cover that start: investigate (do not widen the scope); record.

### Phase 4 — proofs

1. **Revert proof (F7d + knob split).** Create `<scratch>/plan-f-full.patch` (`<scratch>` = the session scratchpad directory, outside the repo) with `git diff` (tracked files) and copy the untracked `src/test/cases/test_bulk_chain_pregrow.c` to `<scratch>`. Revert by hand **exactly** the three production switches (the build then warns `-Wunused-function` for `kf_bmfix_backoff_ms`: expected in this intermediate state only) and nothing else: in `slotcask.c` the two `kf_bmfix_backoff_ms()` call sites back to `5000ULL`; in `index.c::bitmap_rebuild_shards_durable` the consume block back to `g_index_test_fail_next_build`. Rebuild; run the two F7d cases, T3 and the four escape/refuse cases. Expected: F7d cases RED, each with the single P1 failing assertion; T3 RED with {A2, A3, A5, B4, B5}; the four escape/refuse cases RED (record each failing set; each must include its first knob-dependent assertion: `test-bmfix-escape-disabled` and `-escape-heals-at-startup`: "disabled flag written"; `-escape-flag-write-fails`: "flag-write failure logged"; `-unclean-repair-fails-with-kfm2`: "pending-KFM2 refusal logged"; cascades beyond those are expected and need not be pre-enumerated). Before reverting, verify the saved patch is non-empty and `git apply -R --check` succeeds on the current tree. The two spawn-expect-exit cases take up to 60 s each to fail in this state; allow for it. Then restore by re-applying exactly the three hand edits from Phase 2 (no `git checkout`/`git apply`: the contract forbids git writes and a checkout would discard the whole uncommitted implementation), rebuild, rerun all GREEN, and confirm `git diff | diff - <scratch>/plan-f-full.patch` is empty. A non-empty diff → halt. Delete the scratch files at hand-back.
2. **Mutation proofs for T1 (apply one at a time; rebuild; run the three T1 cases; record output; revert; rebuild; green):**
   - **m1** — in the joiner's pre-grow in `slotcask_bulk_stage_shard`, change `uint64_t projected = kh.hdr->total + chain_pending + (uint64_t)n;` to `… = kh.hdr->total + (uint64_t)n;`. Expected: `test-bulk-chain-pregrow` and `-shard300` RED with exactly {"kf table sized for the whole chain: 128 slots", "75% load invariant holds after the chain"}; `-admission-cap` GREEN.
   - **m2** — make `bulk_input_chain_admissible` return 0 unconditionally. Expected: `test-bulk-chain-pregrow` and `-shard300` RED with exactly {"chain count"}; `-admission-cap` GREEN.
   - **m3** — delete the `w->in->nrecs <= max_chain_records - admitted_records &&` clause in `bulk_gate_waiter_take_matching`. Expected: `-admission-cap` RED with exactly {"chain count"}; the other two GREEN.
   If a mutation's failing set differs, halt and report (the calibration math is wrong or the choreography is not deterministic).
3. **Mutation m4 (knob independence).** In `reindex_object_checked_impl`, also consume `g_bmfix_repair_test_fail_next` (copy the consume block with the new variable). Expected: T3 RED with {B2}. Revert.
4. **Docs.** No user-facing doc, changelog or AGENTS.md change (test-only seams; production back-off unchanged). Record `git diff --stat -- docs AGENTS.md` = empty (plan/evidence files are untracked). The comment correction of `test_bitmap_resplit_chain` is done in Phase 2 (step 5), before the patch snapshot.

### Phase 5 — full suite and hand-back

1. Fresh build; `rtk ./build/bin/shard-db-test run-all --jobs 4`; record totals = baseline + **5 new cases** (3 in `test_bulk_chain_pregrow.c`, `test-bmfix-test-knobs-independent`, `test-bitmap-resplit-log-scope`), **0 failed**; `rm -rf /tmp/shard-db-*`. A failure is root-caused, never rerun until green.
2. Definition of done: no new warnings; no TODO/FIXME/debug output; no mutation left (each m1–m4 reverted — verify with `git diff | diff - <scratch>/plan-f-full.patch` (empty)); `git status --short` = this plan's files (`build.sh`, `src/db/{config.c,index.c,slotcask.c,shard_test_ctl.h,test_control.c,types.h}`, `src/test/{fixtures.c,fixtures.h}`, `src/test/cases/test_bitmap_resplit.c`, new `src/test/cases/test_bulk_chain_pregrow.c`) plus preserved untracked docs. No `sleep(` left in the two F7d functions.
3. Hand back (evidence file + chat): `git diff --stat`, P0 audits, P1 base evidence with the exact failing sets, P2/P3 greens, P4 revert and mutation proofs, P5 totals. State plainly: work is **uncommitted**; the sanitizer gate is deferred to the branch gate after Plan I; the follow-ups ledger row F7 is closed by this work but was not edited; the wire-level bitmap-bearing chain-growth residual (decision 1). Request a blind raw-diff review (it adds a TEST_BUILD seam read inside `kfcache_resplit_locked`, a control-channel selector, and a knob split).

---

## Blind review rounds

- **Round 1** (fresh reviewer, plan + repo only): 0 blockers, 1 major, 8 minors. Major: T2's held-back-off check ran after many slow assertions, so under `--jobs 4` the 2500 ms window could close → disarm and held request now run immediately after `t_fail`, slow assertions moved after. Minors fixed: unused-const warning in the shim state, wrong case name for the old-knob regression, explicit `bulk_commit_window`, scaffold gaps (values, no-op hooks, failure path), positive control for the `LOG_LEVEL=2` token, comment anchor for the chain-test header, patch pre-check and 60 s timeouts in the revert proof, pause-counter note. All plan claims about anchors, call sites, log routing, knob lengths, selector placement and the F7c arithmetic/failing sets were independently traced and confirmed.
- **Round 2** (new reviewer): 0 blockers, 2 majors, 5 minors. Majors: (1) the T2 reorder put `t_fail`/disarm before the existing `inserted == 0` / `errors == 512` assertions, silently retargeting them to the held request → `t_fail` now follows those two assertions, which stay before it; (2) the revert proof used `git checkout`/`git apply`, a git write that could discard the implementation → restore now re-applies the three hand edits and diffs against the saved patch. Minors fixed: `br_now_ms` unused-static in the 1c build, upper bound widened to `BR_BACKOFF_MS + 1500` (4000; the unseamed 5 s default still cannot land under it), log-file naming (`-error.log` / `-info.log`), `bcp_visible` source lines, `db_opened` flag, `rtk proxy` prefix on the P0 greps. Accepted: T1's park/queued assertions have no mutation proof (they are choreography preconditions; a missing park fails after the 30 s pause cap).
- **Round 3** (new reviewer): 0 blockers, 1 major, 9 minors. Major: the Phase 4.4 comment edit came after the Phase 4.1 patch snapshot, so the Phase 5.2 "diff equals snapshot" check could never pass → comment edit moved to Phase 2 step 5. Minors fixed: T3 reconnect/restart plumbing, the non-existent comment in Phase 2 (now an added one-line comment with the identifying log text), unique anchors for the two identical consume blocks, the expected `-Wunused-function` in the revert state, the vacuous MUTATION grep (dropped), a bound on the faulted request's latency (`t_fail - t_req0 < 1000`), inlined 12-site scope mapping. Accepted: T1 stays prose (execution contract already says halt on ambiguity); park/queued preconditions have no mutation proof.
- **Round 4** (new reviewer; the CORE-PROCESS cap): **0 blockers, 0 majors**, 5 minors. All code claims, call-site scopes, the F7c arithmetic, m1–m4, T3/T4/F7d failing sets, the no-git-write revert proof and the Phase 5.2 diff check were independently traced and confirmed. Minors fixed: the held-back-off guard now bounds `t_disarm - t_req0` (so the request is guaranteed inside every shard's window), the 4-vs-3 `br_fault_stage_log` wording, `br_now_ms` ordering/shim snippet, phase heading. Noted for the branch sanitizer gate: the 1000 ms faulted-request bound and the +1500 ms success headroom are sized for the normal `--jobs 4` suite and may need widening (e.g. `BR_BACKOFF_MS` 4000 with scaled bounds) under TSan `--jobs 2`; the executor records any such failure rather than loosening silently.
