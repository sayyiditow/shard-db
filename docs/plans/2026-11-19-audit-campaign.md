# Audit campaign: every query mode to production grade

Date: 2026-11-19. Status: **v3 — approved by the human 2026-11-19 (all NEEDS YOU decisions accepted at their defaults); not yet executed.**
Baseline: release 2026.11.2 (tag `2026.11.2` = commit f19b17d, npm `npm-v1.5.0`).
`audit-base` is a **SHA recorded in the index** (default f19b17d), not a created
git tag — tag creation is a git write the human runs.

---

## Brief (approval surface)

### Priority (human directive, 2026-11-19)

**The campaign's first and binding exit criterion: no critical data-loss bug
remains in any query mode.** Everything else (perf, docs conformance, polish)
is secondary and may be deferred; this may not. Consequences:

- New severity class **DL (data loss/corruption)**, above blocker: silent loss
  or overwrite of an acknowledged write; resurrection of a deleted record;
  a record readable but wrong (clobbered/torn/dangling ref); index/bitmap
  disagreeing with records such that a query silently omits data; a mutation
  (vacuum, truncate, edit-field, reindex, backup/restore, drop-object,
  bulk-*) that destroys or loses data on success *or* on failure/crash;
  unrecoverable state after crash. Every DL needs a repro and is fixed before
  any non-DL work; a DL cannot be deferred or wontfixed.
- **Every mode that writes or destroys data** (insert/update/delete, all bulk,
  files, vacuum, truncate, backup/restore, reindex, add/edit/remove/rename
  field and index, create/drop object, sequence, auto-key) must be covered by
  the crash/restart model fuzzer with an acknowledged-write oracle, or have the
  gap recorded as an open DL-risk row that blocks closure.
- Phase order is data-loss-first: Phase 1 oracle tests and the Phase 2 storage
  slice run before analyzers/lint triage; Phase 3 slices run write/destroy
  modes first (1, 2, 3, 10, 11, 9, 13-write) and read-only modes last.
- Phase 5's readiness table leads with a **data-integrity** column; "production
  ready" is not claimed for any mode whose cell is not green.

### Goal

Make every shard-db query mode production-grade — correct, secure, efficient,
crash-safe, and consistent with its documentation — verified by a systematic
audit, so the project can credibly be advertised for production use.

The campaign is **tool-first, read-second**: automated tooling and
oracle-based tests run first and produce ranked findings; code reading is then
aimed at what the tools cannot reach. Fixes land afterwards as separately
reviewed branches. Success is measurable:

- Every slice has a findings doc (`docs/plans/<date>-audit-<slice>.md`) with
  **zero untriaged** blocker/major findings.
- **Zero open DL findings in every mode** (the binding criterion above).
- Every blocker/major finding has a **repro** (failing test or script) or a
  written proof trace; blockers get a second reviewer's confirmation.
- Fuzzing: every target has plateaued (see decision table) with zero untriaged
  findings; corpora checked in at `fuzz/corpora/`.
- Static-analysis output (new runs **and** the existing CI/Coverity/CodeQL
  history) fully dispositioned: fix now / deferred-with-plan /
  wontfix-with-rationale.
- The oracle-based tests (crash/restart model fuzzer, planner differential,
  fault injection) run clean for the budget in the decision table, with every
  divergence triaged.
- Every write mode has crash-recovery drill coverage, or a documented gap with
  a specified drill landing in a Phase-4 test branch.
- Code-reduction candidates (dead code, duplication) are identified, indexed
  and each dispositioned (remove / consolidate / keep-with-reason); removals
  that land carry before/after line counts and are protected by the oracle
  tests.
- Every perf finding closed with a before/after bench number (human-run) or
  explicitly accepted as-is.
- Every mode's protocol docs verified against actual behavior (response shapes,
  error codes, limits); artifact: a docs-conformance table in each findings doc
  (doc claim → observed behavior → pass/fail).
- Final deliverable: a per-mode production-readiness table, including
  operational columns (backup/restore, upgrade, observability, disk-full), for
  adoption material.

### Why now

The engine is feature-rich but has never had a mode-by-mode assurance pass.
The recent production bugs (kf grave-frontier clobbering live records, bitmap
remap coherence, KFM2 replay, rebuild-txn flakes) are **state-space and
crash-ordering** bugs in the storage engine, not input-validation slips — so
the campaign leads with tooling that explores state space (crash/restart model
fuzzing, planner differential testing, fault injection) and gives the storage
and durability layer its own audit slice. Hand-rolled trust-boundary parsers
(`json_parse_object`, the criteria-tree parser, the cursor decoder) and a
~88k-line daemon (`slotcask.c` 12.6k, `query.c` 11.5k, `query_aggregate.c` 7.4k,
`query_bulk.c` 6.5k) are the rest of the risk profile.

### Approach (logical flow)

0. **Setup** (Phase 0): threat model, index doc, `audit-base`, CI-stability
   baseline.
1. **Tooling, no code reading** (Phase 1): static analyzers, extended fuzzing,
   oracle-based test harnesses, coverage, bench/profile inventory. Output:
   triaged findings lists and gap maps. Fixes: none. *Phase 1 is tool-driven
   but not read-free: writing the harnesses and triaging tool output require
   reading the engine. That reading is incidental, not the audit — any defect
   noticed goes into the index as `suspected` with source `phase1-incidental`
   (repro still required before it counts as major/blocker).*
2. **Storage & shared layers, reading guided by Phase-1 output** (Phase 2):
   storage/durability engine, dispatch/auth, connection lifecycle, caps, lock
   order, criteria compiler/planner.
3. **Per-mode slices, risk-ordered** (Phase 3): writes → reads → files →
   maintenance → schema → diagnostics, same checklist each slice.
4. **Fix branches** (Phase 4): security/correctness first, perf second,
   refactors last behind characterization tests. One concern per branch.
5. **Closure** (Phase 5): re-run gates, coverage delta, readiness table.

### What "read-only" means here

Audit *slices* (Phases 2–3) edit no product source. Phase 1 *adds* test/tool
code (fuzz harnesses, oracle tests, build modes, scripts) — additive, never
modifying engine behavior. Neither phase fixes bugs; fixes are Phase 4 branches.

### Decisions made

| Decision | Choice | Alternatives considered | Why | Tag |
|---|---|---|---|---|
| Fuzzer | libFuzzer in-process, **extending existing `fuzz/`** (`build.sh`, `fuzz_json/_b64/_criteria`, `stubs.c`, `corpora/`, CI `fuzz.yml`) | New `src/fuzz/` + `BUILD_MODE=fuzz`; AFL++; socket-level fuzzing | Infra exists and runs in CI; orders of magnitude more execs/sec than sockets; AFL++ fallback if libFuzzer fights a target | DEFAULT |
| Corpus location | `fuzz/corpora/` in-repo (already the case) | Local-only | Already settled in the repo; enables CI fuzzing | DEFAULT |
| Fuzz "done" criterion | **Coverage plateau**: 1 h smoke, then run until no new edges for 2 h (cap 8 h/target), zero untriaged crashes; then hand continuous fuzzing to CI/OSS-Fuzz | 24 h wall per target; ad hoc | Wall-time budgets never terminate meaningfully and 6×24 h monopolizes the only workstation | **APPROVED (default)** |
| New build modes | Only `lint` is new: gcc (as `build.sh` uses), `WARN_CFLAGS` minus its `-Wno-*` entries plus `-Wall -Wextra -Wconversion -Wshadow`, defaults to `SKIP_TESTS=1`; `coverage` already exists | Permanent CFLAGS change; per-TU pragmas | `-Wconversion` on deliberate BE narrowing is noise; must not slow everyone's builds | DEFAULT |
| Audit snapshot | `audit-base` = SHA of release `2026.11.2` (f19b17d) or a human-named later `main` commit, recorded in the index (no tag created by the agent); slices read it | Audit on moving main | Findings cite stable symbols while fix branches land independently | DEFAULT |
| Fix timing | Fix branches start once Phase 1 and the Phase 2 storage and dispatch/auth findings land, in parallel with remaining slices | Strictly after all auditing | Shared/storage fixes unblock the highest-risk modes sooner; sanitizer gate is per-branch either way | DEFAULT |
| Findings committing | Everything stays uncommitted until the human reviews it; **the human runs every commit** (CORE-PROCESS: no standing git permission). Each session ends by stating which doc-only findings files are ready to commit on an `audit/findings` branch | Agent commits findings per session | Weeks of uncommitted notes is a loss risk, mitigated by the human committing doc-only batches often; a standing agent-commit permission is forbidden by CORE-PROCESS | **APPROVED (default)** |
| Planner-path forcing (differential test) | **Twin-object data shapes**: identical data in objects with no indexes (full-scan oracle), btree-only, bitmap, composite; compare results; `explain` confirms which path ran and a path counts as covered only then | `TEST_BUILD`-only force-plan hook in `shard_test_ctl.h` (an engine edit) | No engine change; the hook is the fallback if a `PRIMARY_*` path cannot be reached by data shape — then its own NEEDS-YOU decision | DEFAULT |
| Fault injection mechanism | `LD_PRELOAD` interposer (`audit/faultlib.c`) for `write/pwrite/fsync/fdatasync/open/mmap/malloc`, armed by env (nth-call or probability); reuse existing phase seams (`g_shard_test_fail_phase/occurrence/sticky`, `BT_TEST_PUBLISH_FAIL_STAGE`, `SCHEMA_CONF_TEST_FSYNC_FAIL_CALL`, `INDEX_TEST_FAIL_NEXT_BUILD`, `BMFIX_REPAIR_TEST_FAIL_NEXT`, `INDEXED_ABORT_FAIL_AFTER`). A fault mode with no seam is logged as a **gap**, not improvised | New in-engine hooks | Test daemon is dynamically linked, so interposition needs no engine edits | DEFAULT |
| Audit-case isolation | New `TEST_REGISTER_AUDIT` macro + `--include-audit` flag in `test_runner.c` (skipped by default `run-all`); sources added to the `shard-db-test` line in `build.sh`; CI/sanitizer workflows unchanged | Name-prefix filter (does not exist); separate binary | Runner has only substring `--filter` and exact-name `--exclude`; this keeps default behavior and CI untouched | DEFAULT |
| Reaching static parse functions in fuzz harnesses | Harness `#include`s the `.c` (as `fuzz_criteria` does for `query.c`); framing via `fmemopen`/stubbed `FILE*` | Un-`static` the functions (engine edit) | Zero engine change; un-static would be a NEEDS-YOU decision | DEFAULT |
| Oracle-test and soak budgets | Crash/restart fuzzer: 2,000 seeds × ≤50 ops (normal build), 300 seeds under ASan; planner differential: 500 generated criteria × 3 data shapes; fault injection: every seam × 20 seeds; soak: 8 h normal + 4 h TSan (human-run); "pass" = zero divergences, every divergence triaged | Undefined / until bored | Makes "clean for the budget" decidable; counts are tunable, the fixed budget is the point | **APPROVED (default)** |
| Coverity input | **None needed**: the human has triaged all Coverity issues (0 open as of 2026-11-19). Record that in the index. CodeQL via `gh api repos/sayyiditow/shard-db/code-scanning/alerts`; cppcheck/scan-build via `gh run view --log` | Export/triage Coverity again | Already dispositioned by the human | DEFAULT |
| Toolchain | **Already installed**: clang, clang-tidy, cppcheck, perf, gcov, llvm-cov. Not installed: valgrind (not required — ASan/TSan/UBSan are the gate) | Build tools from source | Nothing to install | DEFAULT |
| shard-cli slice | Deferred (optional slice after Phase 5) | In-scope now; permanently out | Separate binary, same wire; the daemon is the production surface first | **APPROVED (default)** |
| Oracle-test harness location | New `src/test/audit/` driven by the existing `shard-db-test` runner (cases registered via `TEST_REGISTER_AUDIT`, see the isolation row) | Separate binary; shell scripts | Reuses fixtures (`test_env_start_at`, kill/restart) and the sanitizer builds | DEFAULT |

### Blast radius

The audit itself changes no engine behavior. Phase-1 tooling adds a `lint`
build mode, new fuzz harnesses, and `audit-*` test cases/bench targets
(additive; no existing build/test path changes — confirm by running the normal
suite once after Phase 1). Phase-4 fix branches each carry their own plan-level
blast-radius analysis; wire/on-disk format changes are not expected from audit
findings, but any that surface become their own plans flagged as breaking.

### Test strategy

- Audit slices run existing suites/cases read-only (targeted case runs, crash
  drills that already exist); they never weaken or skip tests.
- Phase 1 oracle tests are validated by a **known-bug check**: each harness must
  detect a reintroduced historical bug (the kf grave-frontier clobber is the
  first: the harness must fail on commit `163b36a`, pass on `2026.11.2`).
- Phase 4: every correctness fix test-first — regression test fails on
  `audit-base`, passes after, revert-prove-paste per CORE-PROCESS.
- Perf fixes: human-run benches, before/after numbers pasted into the finding.
- Crash drills missing per a slice's findings land as new test cases in
  Phase-4 branches.
- Sanitizer gate per AGENTS.md standing rule, once per branch, at the end.

### Out of scope

- shard-cli internals (unless the human pulls it into scope — decision above).
- Bench harness internals (targets get *added*, the harness is not reworked).
- Docs rewrites beyond conformance fixes found by slices.
- `embedded.c` as a separate slice (covered inside the relevant mode slices; it
  shares `query.c` paths).
- Any redesign: findings that need a design decision become their own plans for
  approval — never improvised inside the campaign. Code reduction is limited to
  removing dead code and consolidating duplication behind tests; it is not a
  rewrite or an architecture change.
- Fixing the load-sensitive `test-rebuild-txn-*` pause waits is a Phase 0
  *item* (stabilize CI) but its fix is a normal fix branch, not campaign work.

### Review log

- 2026-11-14: approach proposed by the human; reviewed in-session — shape kept
  (cross-cutting first, then slices), additions folded in: in-process fuzzing
  with done-criteria, connection-lifecycle slice item, completed checklist,
  docs-conformance lens, crash drills, `-Wconversion` as opt-in build mode,
  fixed audit snapshot, bench-gap inventory, shared severity scale + index doc.
- 2026-11-19 (v1): campaign doc written per human request.
- 2026-11-19 (v2): lens review against the repo. Folded in: oracle-based testing
  track (crash/restart model fuzzer, planner differential, fault injection);
  storage & durability slice (Phase 2); reuse of existing `fuzz/`, coverage
  mode and CI analyzers instead of recreating them; stale references (branch
  name, tool installs) corrected; plateau-based fuzz criterion; CI-stability
  baseline; threat model; finding quality bar (repro required); committed
  doc-only findings; extended checklist (concurrency, key/typed-field edges,
  resource exhaustion, platform differences, upgrade/backup); macOS
  `sem_init` finding seeded. **Formal blind review (Opus, §1c) run once after
  these changes — see the next entry.**
- 2026-11-19 (v3): Opus blind review round 1 (4 blockers, 6 majors, minors):
  folded in a no-engine-change planner differential (twin objects + `explain`),
  an `LD_PRELOAD` fault-injection interposer, audit-case isolation
  (`TEST_REGISTER_AUDIT`), a fully specified crash/restart oracle (in-process
  validation, biased kill points, known-bug gate budget and worktree method),
  mechanical mode inventory + slice 13, named fuzz-target symbols and static
  reach method, human-run commits and SHA-not-tag `audit-base`, re-estimated
  Phase 1 with grouped lint triage, explicit oracle/soak budgets (NEEDS YOU),
  human-exported Coverity input, honest "incidental reading" statement for
  Phase 1, and minor fixes (stale step refs, lint spec, AUD-CI-1 cases, `gh`
  commands, bench-gap re-derivation).
- 2026-11-19: added the code-reduction track at the human's request (Phase 1
  candidate listing, Phase 4 refactor lane gated on oracle coverage, closure
  reporting). It does not change the data-loss-first priority.
- 2026-11-19: human approved the plan with all four NEEDS-YOU decisions at their
  defaults (plateau fuzz criterion, fixed oracle/soak budgets, human-run commits
  of doc-only findings, shard-cli deferred). Next: Phase 0.

---

## Plan (executor-facing)

### Ground rules

- **Timebox**: the sessions listed per slice in its table row (a "1–3"
  slice may take up to 3; treat estimates as a floor). A hard stop ends with a written
  "what I didn't get to" section in that slice's findings doc.
- **No product-source edits in audits.** Allowed: running builds (sanitizer,
  lint, coverage), suites and targeted cases; adding test/tool code in Phase 1
  and Phase 4 only.
- **Severity scale**: DL (data loss/corruption — see Priority; always fixed
  first, never deferred), then CORE-PROCESS blocker (wrong or unsafe; must
  fix), major (likely rework or defect; should fix), minor (polish).
  Severity of auth/path/parsing findings is judged against the threat model.
- **Finding quality bar**: every blocker/major carries a repro (failing test,
  script, or fuzz input) or a written proof trace citing symbols; blockers are
  confirmed by a second reviewer before entering Phase 4. Unreproduced claims
  are recorded as "suspected" and never block closure criteria until
  reproduced or disproved.
- **Fixed snapshot**: all slices cite `audit-base` symbols (no line numbers);
  fix branches branch off `main` as it moves.
- **Execution mode**: code/tool diffs uncommitted until the human reviews the
  raw diff (repo standing exception); doc-only findings per the decision table.
- **Benches/profiles**: the human runs every bench/profile; the campaign only
  builds targets and scripts and records numbers the human supplies.
- **Machine hygiene**: `/tmp` is tmpfs; run the normal suite as
  `run-all --jobs 8`, ASan `--jobs 6`, TSan `--jobs 3`; remove
  `/tmp/shard-db-*` after every run; fuzzers and the crash/restart fuzzer are
  CPU-heavy — run one heavy job at a time.
- **Escalation**: anything needing a design decision stops and becomes its own
  plan; plan-assumption mismatches → `PLAN_NOTES.md` + halt, per CORE-PROCESS.

### Phase 0 — Campaign setup (1 session)

0. **Mode inventory**: build the authoritative mode list mechanically from the
   dispatch table in `server.c` (every `strcmp(mode, …)` branch), plus CLI-only
   commands and non-dispatch surfaces (NQL `nql.c`, `packed_csv_command.c`,
   `trigram.c`, background auto-vacuum and nightly auto-reshard sweeps,
   sequences/auto-key generation, db.env/config parsing). Assign every entry to
   a slice or explicitly scope it out in the index. Known modes not in the
   original slice table (now slice 13): `add-dir`/`remove-dir`,
   `bulk-insert-delimited`/`bulk-update-delimited`, `estimate-index`, `restore`,
   `sequence`, `not-exists`, single `get`, `add-field`.
1. Write `docs/plans/2026-11-19-audit-threat-model.md` (one page; a provisional
   draft may be used for Phase 1 while the human approves): adversaries
   (unauthenticated network peer; authenticated tenant; tenant-to-tenant;
   local user with file access), assets, trust boundaries (wire, TLS,
   `tokens.conf` scopes, `allowed_ips.conf`, localhost trust, file paths), and
   the definition of blocker/major for each boundary. Human approves.
2. Create the master findings index `docs/plans/2026-11-19-audit-index.md`: one
   row per finding — id (`AUD-<area>-<n>`), area, severity, checklist items
   hit, repro (path or "trace"), status (open / suspected / fix-branch /
   deferred / wontfix), fix-branch, disposition note.
3. Confirm `audit-base` (default f19b17d) and record the exact SHA in the index
   (no tag is created by the agent).
4. **CI-stability baseline**: record the known flake (`test-rebuild-txn-*`
   "rebuild reaches the pause", root cause: fixed 20 s wait over ~200 fsyncs)
   as `AUD-CI-1` (cases in `test_rebuild_txn_recovery.c`; check
   `test_rebuild_recovery.c`/`test_rebuild_validation.c` for the same wait
   pattern) with its fix assigned to a Phase-4 branch; list other red/flaky jobs
   via `gh run list --branch main -L 20 --json conclusion,name,workflowName`.
5. Seed the index with already-known findings: macOS
   `sem_init failed … slot cap disabled` (config.c; → `MAX_CONCURRENT_QUERIES`
   silently off on macOS); anything surfaced by the CodeQL history
   (Phase 1 step 2).

### Phase 1 — Tool-driven baseline (~8–10 agent sessions; fuzz plateaus run as background wall time outside session counts)

All output goes to the index; nothing is fixed. Harness/tool code is additive
and left uncommitted for raw-diff review.

1. **`BUILD_MODE=lint`** in `build.sh` (flags per the decision table). Triage
   **grouped by warning class × TU**: sample at least 5 or 10% of each group
   (whichever is larger), record one disposition rule per group (benign-by-design
   / fix / needs-look), and index only fix and needs-look rows individually.
   `-Wconversion` noise on big-endian narrowing is dispositioned as a group,
   not silenced.
2. **Static analyzers, existing + new**: (a) pull and triage the existing
   findings — CodeQL alerts and cppcheck/scan-build CI logs via `gh` (Coverity:
   all triaged by the human, 0 open; just note it in the index); (b) run `clang-tidy` (checks
   `bugprone-*,clang-analyzer-*,concurrency-*,cert-*`, using `compile_flags.txt`
   or a generated compile database) and `gcc -fanalyzer`, timeboxed to one
   session, on parser and dispatch TUs first (`util.c`, `server.c`,
   `config.c`), then the rest. Raw output goes to
   `docs/plans/audit-artifacts/`, triaged rows to the index. List-building only.
3. **Fuzzing, extending `fuzz/`**: keep `fuzz_json`, `fuzz_b64`,
   `fuzz_criteria`; add harnesses (each added to `fuzz/build.sh`,
   `fuzz/README.md` and `.github/workflows/fuzz.yml`) for: `parse_cursor_object`
   (static in `query.c`; reached by `#include`); `valid_filename` (`config.c`)
   and the dir/object/filename path-join helpers; the request-framing and
   `MAX_REQUEST_SIZE` mid-frame logic in `server.c` (via `fmemopen`/stubbed
   `FILE*`, since `fuzz/build.sh` deliberately does not link `server.c`/TLS);
   the response re-parse sites in `server.c`. First sub-step: the executor
   identifies the exact framing and re-parse symbols and lists them in
   `fuzz/README.md`. Seed corpora from test fixtures and doc examples into
   `fuzz/corpora/<target>/`. Done per the plateau criterion; hangs triaged as
   timeouts; reproducers recorded.
4. **Oracle-based test harnesses** (under `src/test/audit/`, cases named
   `audit-*`, excluded from default `run-all`):
   - **Crash/restart model fuzzer** (needs the `TEST_BUILD` daemon for the
     `SHARD_TEST_PHASE_*` pause/fail hooks): seeded RNG generates write
     sequences (insert/update/delete/bulk ops/CAS/vacuum/reindex/add-index)
     against an in-memory model; SIGKILL at randomized points *and* at pause
     hooks, with a **biased mode** that repeatedly kills at the staged-write
     pause on one stream to build long flag-0 grave runs (uniform kill points
     will not produce them). After each kill: open the root **in-process** (as
     `test_slotcask_grave_run_frontier.c` does) and require (a)
     `slotcask_validate_live_refs` = 0 invalid refs; restart the daemon and
     require (b) results equal across the twin-object shapes over the wire, and
     (c) every acknowledged write present and no unacknowledged write
     half-visible, versus the model. Seeds and failing op-logs are recorded for
     replay.
   - **Planner differential**: for generated criteria over generated data, load
     identical data into twin objects (no indexes / btree-only / bitmap fields /
     composite) and assert identical `find`/`count`/`aggregate` results, with
     and without `order_by`/cursor/join. `explain` confirms which `PRIMARY_*`
     path each run took; paths never reached by data shape are listed and become
     the force-hook decision.
   - **Fault injection**: ENOSPC, EIO, short writes/reads, EMFILE and alloc
     failure via the `LD_PRELOAD` interposer plus the named existing seams; a
     mode with no seam is logged as a gap. Require fail-closed behavior and
     recoverability, never silent corruption.
   - **Known-bug check (gate for each harness)**: build the harness against
     `163b36a` in a scratch `git worktree` with the harness overlaid (first
     confirm the fixtures/seams it needs exist there; if not, record the
     mismatch in `PLAN_NOTES.md` and halt). The crash/restart fuzzer must
     reproduce the kf grave-frontier clobber on `163b36a` (biased mode, ≤500
     seeds) and pass on `2026.11.2`; not rediscovered within budget → escalate
     to the human. The planner differential and fault-injection harnesses each
     get one named seeded regression from the bug history: the executor lists
     candidate fix commits (`git log --grep` on the bitmap-remap, KFM2-replay
     and index-publish fixes) and the human picks the pair before the gate runs.
5. **Coverage**: existing `BUILD_MODE=coverage` (gcov data); full suite
   (`run-all --jobs 8`), then the Phase-1 oracle runs; `gcov` summaries (per-TU
   line/branch %, plus never-executed functions) saved to
   `docs/plans/audit-artifacts/coverage-baseline.txt` so every slice starts from
   known blind spots.
6. **Bench inventory**: re-derive from `src/bench/` (`bench_queries.c` already
   contains aggregate benches; do not assume an old gap list). Modes without a
   bench become work items; new targets are built here, **run by the human**.
7. **Profiling prep**: `perf record` + flamegraph scripts per bench target in
   `audit/scripts/` (not the deprecated `bench/`); the human runs them; hot
   paths recorded per mode in the index.
8. **Code-reduction candidates** (list-building only; nothing is deleted
   here): (a) never-executed functions from the coverage baseline (step 5),
   cross-checked with `cppcheck --enable=unusedFunction` and `-Wunused*` from
   the lint build and a symbol-reference check (`grep`/`nm` for functions with
   no callers, including TEST_BUILD-only seams, which are kept); (b) a
   duplication scan over `src/db` (a clone detector such as `jscpd` or
   `cppcheck`'s duplicate-branch checks; the executor names the tool used and
   its threshold in the artifact); (c) large-function and file-size hotspots
   (`slotcask.c`, `query.c`, `query_aggregate.c`, `query_bulk.c`). Each
   candidate becomes an index row with kind `refactor`, estimated lines
   removable, and risk (hot path? crash-recovery path? documented behavior?).
   Candidates on recovery/durability/data-integrity paths are marked
   **hold until oracle-covered**.
9. **Soak prep**: a long-running mixed-workload script in `audit/scripts/`
   (writers, readers, vacuum, reindex, restarts) for the human to run overnight
   under normal and TSan builds, per the decision-table budget; results
   recorded in the index.

**Phase 1 exit**: normal full suite re-run once (confirms no behavior change;
CI/sanitizer behavior is unchanged because audit cases are skipped unless
`--include-audit`),
index populated, gap maps written, the one-page "Phase 1 summary" lists what
the tools found and what they cannot reach (the input to Phase 2/3 reading).

### Phase 2 — Storage and shared layers (4–5 sessions; reading guided by Phase 1)

One findings doc per area: `docs/plans/<date>-audit-<area>.md`.

1. **Storage & durability engine** (2 sessions): `slotcask.c`, `storage.c`,
   `durability.c`, `btree.c`, `bitmap.c`, `keyset.c`, kfcache. Cover: kf growth
   and resplit; bitmap slot coherence (bmfix markers, disabled-object path);
   KFM2 V2 markers and forward replay; recovery frontier logic; segment
   rotation and free pool; vacuum/compaction; backup/restore (sparse copy,
   round-trip); startup version gate and migrations (`.btri-migrated`,
   eligibility gate, rollback barrier); enumeration of crash points per
   multi-step mutation, mapped to existing drills or specified new ones.
2. **Dispatch & auth**: scope×perm enforcement matrix for *every* mode
   (diagnostics included); token parsing edge cases; `allowed_ips.conf`
   bypass; localhost-trust paths incl. `DISABLE_LOCALHOST_TRUST`; path
   traversal across dir/obj/filename and `valid_filename`.
3. **Connection lifecycle**: wire framing, partial frames, `MAX_REQUEST_SIZE`
   mid-frame behavior, idle timeouts, slow clients vs the poll-accept +
   thread-pool path, TLS handshake/teardown failure paths.
4. **Caps**: all `QUERY_BUFFER_MB` collection sites (10 documented + any
   missed), `MAX_CONCURRENT_QUERIES` semaphore acquire/release on every return
   path and its platform behavior (macOS `sem_init` unsupported), request-size
   cap, fd/thread exhaustion.
5. **Lock order**: documented order (objlock → per-kf-shard rwlocks → bt_cache
   → kfcache → bitmap canonical order; `BtOrderedWalkHandle`
   no-block-under-rdlock rule; cursor resume-point rules) verified against
   every multi-lock holder.
6. **Criteria compiler & planner**: depth/leaf limits, selectivity rank,
   keyset union/intersect, count shortcuts, fallback correctness for every
   planner decision path (cross-checked against the Phase-1 differential
   results).

### Phase 3 — Mode slices, risk-ordered (~15–18 sessions; floor)

| # | Slice | Main files | Sessions |
|---|---|---|---|
| 1 | insert / update / delete + CAS (`if`/`if_not_exists`/`if_match`/`if_no_match`, `$incr`/`$copy`, `return_record`) | `storage.c`, `server.c` | 1–2 |
| 2 | bulk-insert (stages, growth gates, `BULK_STAGE_UNGROWN`, KFM2 markers) | `query_bulk.c`, `slotcask.c` | 2–3 |
| 3 | bulk-delete / bulk-update | `query_bulk.c` | 1–2 |
| 4 | find (criteria, order_by, formats, offset/limit) + count | `query.c`, `query_find.c` | 2–3 |
| 5 | cursor pagination (k-way merge, resume points) | `index.c`, `btree.c` | 1–2 |
| 6 | fetch / keys / exists / get-multi | `query_find.c` | 1 |
| 7 | aggregate (group_by, having, top-N, hash tables) | `query_aggregate.c` | 2–3 |
| 8 | join | `query_join.c` | 1–2 |
| 9 | files (put/get/delete/list/get-file-path) | `query_find.c`, `server.c` | 1 |
| 10 | maintenance (vacuum, backup, reindex, recount, truncate, size, orphaned, vacuum-check) | `query_maint.c`, `index.c` | 2 |
| 11 | schema mutations (create/drop object, add/edit/remove/rename field, add/remove index, auto-key) | `query_schema.c`, `config.c` | 1–2 |
| 12 | diagnostics + token/IP management | `server.c` | 1 |
| 13 | remaining modes from the Phase-0 inventory (`add-dir`/`remove-dir`, delimited bulk, `estimate-index`, `restore`, `sequence`, `not-exists`, NQL, trigram, background sweeps, config parsing) | `server.c`, `nql.c`, `packed_csv_command.c`, `trigram.c`, `config.c` | 2 |

**Slice order is data-loss-first**: run slices 1, 2, 3, 10, 11, 9, then the
write/destroy modes of 13, then the read-only slices (4–8, 12). Slice 4–8
sessions are shortened first if time runs short; slices 1–3, 9–11 are not.

**Every slice runs the same checklist** (template baked into the findings doc):

0. **Data-loss pass first**: for every way this mode can write, move or
   destroy data — success path, each failure path, and a kill at each step —
   state what must be true afterward and show it holds (cite the window
   logic, the drill, or the oracle seed that covers it); anything unshown is a
   DL-risk row.
1. Input validation & bounds (multi-key, empty, max-size, malformed; binary,
   UTF-8 and boundary-length keys).
2. Integer overflow / size arithmetic (slots, offsets, lengths, rotations).
3. Auth: scope×perm enforced correctly for this mode (incl. diagnostics).
4. Response-shape conformance vs docs — clients branch on JSON type; shape
   drift is a correctness bug, not polish.
5. Resource lifetimes: fds, mmaps, buffers on every return path.
6. Crash safety: read the window logic; run existing drills where they exist
   (flag=0 staged states, KFM2 replay, bmfix recovery); confirm coverage by the
   Phase-1 crash/restart fuzzer; specify missing SIGKILL drills for Phase 4
   (mid-bulk, mid-KFM2-apply, mid-vacuum/backup).
7. Lock order held; no blocking waits under held locks; behavior under
   concurrent mixed workload (reader/writer/vacuum/reindex) — check against
   TSan results and the soak.
8. Memory-cap collection sites touched; `timeout_ms` applicability.
9. Allocation in hot loops.
10. Error-path side effects: what already persisted when the failure returns;
    behavior on ENOSPC/EIO/EMFILE (per the fault-injection results).
11. Typed-field edges where the mode touches typed data: numeric precision,
    date/time boundaries, NULL semantics.
12. Platform differences (Linux vs macOS: `fsync`/`F_FULLFSYNC`, semaphores,
    `SEEK_DATA`).
13. Docs-vs-behavior: the mode's protocol doc + error-codes + limits refs.
14. Timebox respected; "not covered" section written.

Per slice: cross-reference Phase-1 artifacts (lint/analyzer rows, coverage
gaps, fuzz/oracle findings for touched code) *before* reading, read the code
paths against `audit-base`, run the checklist, write the findings doc, update
the index.

### Phase 4 — Fix branches (start once Phase 1 and the Phase 2 storage and dispatch/auth findings land; parallel with remaining slices)

- **Order**: security/correctness → perf → refactors. One concern per branch,
  named `fix|perf|refactor/audit-<slug>`, branched off current `main`.
- **Correctness**: test-first per CORE-PROCESS (regression fails on
  `audit-base`, passes after; revert-prove-paste). Crash-drill specs from
  slices land here as test cases. `AUD-CI-1` (rebuild-pause waits) is the
  first branch: deadline scaled for load/sanitizer/macOS and the daemon log tail
  printed on every pause miss.
- **Perf**: before/after numbers from human-run benches pasted into the
  finding; no perf change merges on assertion alone.
- **Refactors / code reduction**: lowest priority, strictly after all DL and
  blocker work on the touched area. One concern per branch
  (`refactor/audit-<slug>`). Preconditions: characterization tests exist and
  the relevant Phase-1 oracles (crash/restart, planner differential) are
  trusted (passed their known-bug gate) and green on `audit-base`; the branch
  must keep them green. Each branch reports lines removed/changed (`git diff
  --stat`), and a bench number if it touches a hot path (human-run). Never
  mixed with behavior changes; removals of anything on a recovery, durability
  or data-integrity path need the human's explicit go.
- **Sanitizer gate** (AGENTS.md standing rule, once per branch after ALL
  fixes, immediately before push): `BUILD_MODE=asan` → `run-all --jobs 6`;
  `BUILD_MODE=tsan` → `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
  run-all --jobs 3`. Diff touching locks/atomics/shared-cached
  state/lifetimes/threads → three consecutive passes of each; otherwise one. No
  `halt_on_error=0`. Normal full suite (`--jobs 8`) mandatory for every change;
  `/tmp/shard-db-*` removed after every run. Gate findings are fixed or planned
  — never suppressed without a named-function rationale.
- Index updated per finding as branches land.

### Phase 5 — Closure & readiness report (1–2 sessions)

Re-run the full suite + both sanitizer suites on final `main`; report net
lines removed by the code-reduction track and the remaining indexed candidates; re-run fuzzers
on the grown corpora to plateau; re-run the oracle harnesses at the full
budget; human-run soak; coverage delta vs Phase 1; confirm every index row is
closed, deferred-with-plan, or wontfix-with-rationale. Produce the **per-mode
production-readiness table** (mode × correctness / security / perf /
docs-conformance / crash-safety, plus operational columns: backup/restore,
upgrade, observability, disk-full), each cell with an evidence link — the
artifact for adoption material.

### Session protocol (every slice session)

1. Re-read the slice's protocol docs and prior findings.
2. Cross-reference Phase-1 artifacts for the touched files.
3. Read the code paths against `audit-base`; run the 14-point checklist.
4. Write the findings doc (repro or proof trace per major/blocker); update the
   index; write the "not covered" section.

### Estimates

Phase 0: 1 session (blocked on threat-model approval; provisional draft
allowed). Phase 1: ~8–10 plus background fuzz wall time. Phase 2: 4–5. Phase 3:
~17–20 (floor). Phase 5: 1–2. Total ~32–38 audit sessions plus open-ended fix sessions driven
by findings. The index is the single source of truth for what is open.
