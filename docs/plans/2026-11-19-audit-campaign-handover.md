# Handover: audit campaign executor

Date: 2026-11-19. Read this first, then `docs/plans/2026-11-19-audit-campaign.md`
(the approved plan, v3). Nothing in the campaign has been executed yet.

## Your mission

Execute the approved audit campaign. **Binding exit criterion (human directive):
no critical data-loss (DL) bug remains in any query mode.** Perf, docs and polish
are secondary and may be deferred; DL findings may not. The plan's "Priority"
section defines DL and the data-loss-first ordering — follow it.

## State of the repo (as of handover)

- Release 2026.11.2 is on `main` (merge commit `f19b17d`, PR #391; tags
  `2026.11.2` and `npm-v1.5.0` pushed by the human). `audit-base` = `f19b17d`
  (record the SHA in the index; do **not** create a git tag).
- The plan file and this handover are **uncommitted**. The human runs all git
  writes (commit/push/PR/merge/tags) — CORE-PROCESS forbids a standing
  permission. At the end of each session, say which doc-only findings files are
  ready for the human to commit on an `audit/findings` branch.
- Work on a feature branch for any code/tool additions (`chore|test/audit-*`);
  never on `main`.

## Approved decisions (all NEEDS-YOU items accepted at defaults)

- Fuzz done = coverage plateau (1 h smoke, then until no new edges for 2 h; cap
  8 h/target); zero untriaged crashes.
- Oracle/soak budgets: crash/restart fuzzer 2,000 seeds × ≤50 ops (normal),
  300 under ASan; planner differential 500 criteria × 3 data shapes; fault
  injection every seam × 20 seeds; soak 8 h normal + 4 h TSan (human-run).
- Findings: human commits doc-only batches; agent never commits.
- shard-cli: deferred.

## Where to start

**Phase 0** (1 session): mode inventory from the `server.c` dispatch table
(every `strcmp(mode, …)` branch + CLI-only + non-dispatch surfaces), threat model
(`docs/plans/2026-11-19-audit-threat-model.md`, provisional draft allowed while
the human approves), the findings index (`docs/plans/2026-11-19-audit-index.md`),
record `audit-base`, CI-stability baseline, seed known findings.

Then **Phase 1**, data-loss-first: build the crash/restart model fuzzer and
its known-bug gate **before** the lint/analyzer triage.

## Repo facts verified during planning (do not re-derive)

- Existing infra to extend, not recreate: `fuzz/` (`build.sh`, `fuzz_json`,
  `fuzz_b64`, `fuzz_criteria`, `stubs.c`, `README.md`, `corpora/`),
  `.github/workflows/fuzz.yml` (60 s/harness), `BUILD_MODE=coverage` in
  `build.sh`, CI analyzers (CodeQL, Coverity weekly (all triaged, 0 open), cppcheck, scan-build,
  Codecov, sanitizers, TSan).
- Installed: clang, clang-tidy, cppcheck, perf, gcov, llvm-cov. Not installed:
  valgrind (not required).
- Test runner (`src/test/test_runner.c`) has only substring `--filter` and
  exact-name `--exclude`; test sources are listed by hand in `build.sh`. The
  plan's `TEST_REGISTER_AUDIT` + `--include-audit` mechanism is new work and
  must keep default `run-all` and all CI/sanitizer workflows unchanged.
- Fault seams that already exist: `g_shard_test_fail_phase/occurrence/sticky`,
  `BT_TEST_PUBLISH_FAIL_STAGE`, `SCHEMA_CONF_TEST_FSYNC_FAIL_CALL`,
  `INDEX_TEST_FAIL_NEXT_BUILD`, `BMFIX_REPAIR_TEST_FAIL_NEXT`,
  `INDEXED_ABORT_FAIL_AFTER`; pause hooks `REBUILD_TEST_PAUSE_PHASE/_MS`,
  `DURABILITY_TEST_PAUSE_*`, `SHARD_TEST_PHASE_*` (`shard_test_ctl.h`). The
  test daemon is dynamically linked (so `LD_PRELOAD` interposition works).
- `slotcask_validate_live_refs` is in-process only; the model for opening a
  root in-process after a kill is `src/test/cases/test_slotcask_grave_run_frontier.c`.
- There is no wire/planner knob to force a `PRIMARY_*` plan; `explain` reports
  the plan only. The plan's twin-object method avoids engine edits.
- Known-bug gate pair: the kf grave-frontier bug reproduces on `163b36a`
  (parent of fix `f3c6ee5`) and is fixed on `2026.11.2`. It needs a flag-0
  grave run longer than the slot_size resync window — uniform random kills may
  never produce it, hence the plan's biased mode (repeated kills at the
  staged-write pause on one stream).
- Dispatch modes not in the original slice table: `add-dir`/`remove-dir`,
  `bulk-insert-delimited`/`bulk-update-delimited`, `estimate-index`, `restore`,
  `sequence`, `not-exists`, single `get`, `add-field`; also NQL (`nql.c`),
  `packed_csv_command.c`, `trigram.c`, background auto-vacuum / auto-reshard
  sweeps (all in slice 13).

## Seed findings for the index

- **AUD-CI-1 (flake)**: `test-rebuild-txn-*` in `test_rebuild_txn_recovery.c`
  fail with "rebuild reaches the (deterministic/post-index) pause". Root cause:
  fixed 20 s `wait_for_path` while the paused daemon does ~200 fsyncs before the
  marker (197 measured under strace; ~1.2 s locally, far slower on CI disks,
  macOS full-fsync, ASan/TSan). Pre-dates 2026.11.2 (main failed the same way at
  `163b36a`). Disproven alternatives: not the SEEK_DATA probe (forced-untrusted
  verdict made no timing difference), not port reuse (ports are reserved), not
  reproducible locally on tmpfs or ext4 at `--jobs 8/12`. Fix (Phase-4 branch):
  scale the deadline for load/sanitizer/macOS and print the daemon log tail on
  every pause miss (today only the splits case does). Check
  `test_rebuild_recovery.c`/`test_rebuild_validation.c` for the same pattern.
- **macOS caps**: `config: sem_init failed: Function not implemented - slot cap
  disabled` in macOS CI logs (config.c) → `MAX_CONCURRENT_QUERIES` silently off
  on macOS (no unnamed semaphores). Verify and record.
- Multiple threads can each create an 8 MiB `.sdprobe-*` file concurrently
  before the probe cache is populated (observed in `strace`); benign cost but
  note it.

## Process rules you must follow

- CORE-PROCESS.md and AGENTS.md apply. Audits edit **no product source**;
  Phase 1/4 may add test/tool code only; fixes are Phase-4 branches, one
  concern each, test-first with revert-prove-paste.
- Finding quality bar: every major/blocker/DL has a repro or a written proof
  trace; blockers/DL get a second reviewer. Unreproduced = `suspected`.
- Machine hygiene: `/tmp` is tmpfs. Normal suite `./build/bin/shard-db-test
  run-all --jobs 8`; ASan `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` then
  `--jobs 6`; TSan `BUILD_MODE=tsan …` then `TSAN_OPTIONS="second_deadlock_stack=1:
  print_stacktrace=1" … --jobs 3`. Sanitizer gates once per branch at the end
  (3× when locks/atomics/shared state/lifetimes/threads are touched). Remove
  `/tmp/shard-db-*` after every run. `BUILD_MODE=asan/tsan` overwrites
  `build/bin` — rebuild normal with `SKIP_TESTS=1 ./build.sh` afterward. One
  heavy job at a time.
- The human runs all benches/profiles and the overnight soak.
- Any design decision → its own plan; plan-assumption mismatch → `PLAN_NOTES.md`
  and halt. Do not improvise.
- Do not run a second blind-review loop on the campaign plan unless the human
  asks; one Opus review (4 blockers, 6 majors) was run and folded in (v3).

## Open items needing the human during the campaign

- Approve the threat model (Phase 0) — provisional draft may proceed meanwhile.
- (Coverity: nothing to do — the human has triaged all issues, 0 open.)
- Pick the known-bug fix-commit pairs for the planner-differential and
  fault-injection gates (executor lists candidates via `git log --grep`).
- Run benches/profiles and the soak when the scripts are ready.
