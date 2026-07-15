# Test Transport Audit: TCP+subprocess vs. direct C calls — Plan B

**This is Phase 1 (audit only).** This plan produces a findings report
and stops at a human/planning checkpoint — it makes **no code changes**.
Any confirmed correctness gap gets fixed by a **separate**, later plan
(written after the human reviews and approves specific findings), because
the exact files/cases/mutations touched by a fix cannot be named until
this audit has run — naming them now would mean designing fixes ad hoc
mid-execution, which conflicts with this repo's literal-plan execution
model (CORE-PROCESS requires every edit site to be anchored and every
fix's mutation-proof sequence to be spelled out in advance; neither is
possible before Task 1's findings exist).

**Goal**: audit every **registered test case** (`TEST_REGISTER` entry —
not file, see Background) under `src/test/cases/*.c` (171 files / 282
cases as of 2026-07-16, re-derive both counts fresh at execution time —
see Task 1 Step 0) for whether it uses the *correct* transport for what
it claims to verify — **not** a speed-driven conversion pass. A test that
spins up a real daemon over TCP when it's only exercising a pure function
is wasted setup cost but not a correctness bug; a test that uses a direct
in-process C call when the thing under test is genuinely protocol-,
auth-, concurrency-, or crash-boundary-sensitive is a **false sense of
coverage** — it can pass while the real (TCP-facing, multi-threaded,
multi-process) behavior is broken. This plan only *finds* the second
class (the fix is Phase 2, a separate plan). The first class (TCP-tests
that could be direct calls) is reported for visibility but **not**
converted — this is an explicit, user-set boundary on scope (see
CLAUDE.md's recorded instruction: "audit for correctness only,
conservative, not a blanket speed conversion").

## Background (from prior investigation; corrected 2026-07-16 — the
original counts below were already stale the day this plan was drafted,
and drifted further from same-day concurrent PR work. This repo has
active concurrent work landing test files daily, so **do not trust these
counts verbatim at execution time either** — Task 1 Step 0 re-derives
them from source before auditing.)

`src/test/cases/*.c` = **171 files** (was 167 when first drafted).

Five coarse file-level buckets are useful as a starting inventory — the
original draft missed a fixture variant (`test_env_start_at`) entirely,
plus 3 hand-rolled-fork files that landed the same day as the draft, and
initially misclassified the runner meta-test:

- **129** call `test_env_start`/`test_env_start_ex` (the standard fixture:
  spawns a real `shard-db` daemon subprocess on a free port, talks to it
  over the real TCP+JSON wire protocol via `tc_connect`/`tc_request`).
- **6** call `test_env_start_at` instead — a sibling fixture in
  `src/test/fixtures.c` (`int test_env_start_at(TestEnv *env, const char
  *db_root, int port)`) that spawns the same real daemon subprocess over
  the same real TCP wire, just letting the caller pin `db_root`/`port`
  (used by tests that restart/reopen a daemon against a fixed location,
  e.g. crash-recovery tests). **Treat `test_env_start_at` exactly like
  `test_env_start` for this rubric** — it is real TCP+subprocess
  transport, not a direct-call shortcut, even though it doesn't match
  the literal name. Files: `test_coverity_disk_corruption_bitmap.c`,
  `test_coverity_disk_corruption_btree.c`,
  `test_coverity_disk_corruption_segments.c`, `test_crash_safety.c`,
  `test_objlock.c`, `test_slotcask_v2_crash.c`. Five of these predate
  this plan by weeks to months — the original "re-verified this session"
  claim was inaccurate even at authoring time, not just a later-drift
  problem.
- **9** exercise a real daemon over TCP via a **hand-rolled**
  `fork()`+`execl()` spawn instead of either fixture (verified via grep
  for `fork()`/`test_env_start` across all 171 files): `test_per_tenant_auth.c`,
  `test_auto_reshard.c`, `test_auto_reshard_shutdown_race.c`,
  `test_auto_vacuum.c`, `test_btcache_evict_race.c`, `test_tls.c`,
  `test_startup_validator.c`, `test_token_perms.c`,
  `test_warmup_vacuum_race.c`. (Original draft listed only the first 6 of
  these; `test_auto_reshard_shutdown_race.c`, `test_btcache_evict_race.c`,
  and `test_warmup_vacuum_race.c` landed the same day as this plan via
  concurrent bugfix PRs #248/#249 and were missed.) Each has a documented
  reason in its own file header for not using a shared fixture
  (typically: needs a custom `db.env` with specific knobs the fixture's
  default env doesn't set, e.g. `AUTO_VACUUM=1`, TLS cert paths, or a
  startup-time validation flag).
- **26** are files where the subject under test is called directly, with
  no daemon/TCP transport (originally counted as 27 in the first draft,
  which miscounted `test_runner_parallel.c` into this bucket — see the
  correction below, it's actually a fifth kind). **Correction: "no
  subprocess of any kind" (an earlier draft's phrasing) was false** — 9
  of these 26 files call `system()` or `popen()` for cleanup
  (`rm -rf` of their tmpdir), OpenSSL key/cert generation, or a `find`
  helper: `test_kfcache_staleness.c`, `test_o_direct_scan.c`,
  `test_slotcask_basic.c`, `test_slotcask_cas.c`,
  `test_segcache_staleness.c`, `test_slotcask_resplit.c`,
  `test_slotcask_api.c`, `test_tls_unit.c` (OpenSSL), and
  `test_variable_length.c`. None of these spawn `shard-db` or talk TCP —
  the subprocess is incidental tooling, not the transport under test —
  but "no subprocess" was inaccurate. These 26 files contain **36**
  registered `TEST_REGISTER` cases (not 26 — most files register more
  than one case), and — see the correction below — this is very likely
  an undercount of the true direct-call case total, since mixed files
  (below) hide more:
  `test_btree.c`, `test_btree_value_hash_sort.c`,
  `test_btree_inplace_leaf.c`, `test_kfcache_staleness.c`,
  `test_o_direct_scan.c`, `test_coverity_encode_criterion_overflow.c`,
  `test_coverity_group_by_overflow.c`,
  `test_coverity_join_buf_overflow.c`,
  `test_coverity_seg_scan_varlen_overflow.c`, `test_slotcask_basic.c`,
  `test_config_encode.c`, `test_index_splits_curve.c`,
  `test_keyset.c`, `test_nql.c`, `test_reshard_target.c`,
  `test_objlock_unit.c`, `test_parallel.c`,
  `test_slotcask_cas.c`, `test_simd.c`, `test_segcache_staleness.c`,
  `test_slotcask_resplit.c`, `test_slow_query_log.c`,
  `test_slotcask_api.c`, `test_tls_unit.c`, `test_util.c`,
  `test_variable_length.c`.
- **1** (`test_runner_parallel.c`) is a **meta/CLI-subprocess test** — a
  fifth kind, distinct from all four buckets above. See the correction
  below and Task 1 Step 0.5.

129 + 6 + 9 + 26 + 1 = 171 (file-level count — see below: file-level
totals are a rough starting point, not the audit unit, and are known to
mis-bucket cases within mixed files).

**Corrections found on review of this plan's second draft:**

- **The four buckets above are file-level and mutually exclusive by
  construction (first-match `elif`), which hides hybrid files.** 5 of the
  11 files that call `test_env_start_at` **also** call
  `test_env_start`/`test_env_start_ex` for other cases in the same file
  — they land in the 129-bucket above by `elif` priority, silently
  hiding their `test_env_start_at` cases: `test_bitmap_index.c`,
  `test_enum.c`, `test_rebuild_kf.c`, `test_rebuild_recovery.c`,
  `test_vacuum_streams_mismatch.c`. This is harmless for the *file*-level
  bucket counts above (both fixtures are real TCP+subprocess transport,
  treated identically by the rubric), but it means a file-level verdict
  cannot be trusted for these 5 files — Task 1 audits at the
  **registered-case** level specifically to avoid this blind spot (see
  Task 1 Step 0.5).
- **The repository has 282 registered `TEST_REGISTER` entries across
  these 171 files** (up to 22 in a single file, `test_find_with_total.c`)
  — file-level classification can conceal cases with different transport
  or intent bundled into one file. `src/test/cases/*.c` file count is a
  coarse proxy for audit scope, not the actual audit unit; the actual
  audit unit is the registered case (see Task 1 Step 0.5).
- **File-level transport inheritance is unsafe even for files that only
  call one of `test_env_start`/`test_env_start_ex`/`test_env_start_at`
  (i.e., files that look "clean" under the hybrid check above).** Verified
  examples include `test_agg_topn_stream.c` (3 pure direct heap cases plus
  cases whose setup/assertion paths use the daemon), `test_nql_joins.c`
  (6 direct parser cases and 4 TCP cases), and
  `test_planner_op_capability.c` (7 direct capability-table cases and 1
  TCP integration case). There is a second distinction within a single
  case: **setup transport can differ from assertion/subject transport**.
  For example, every `test_planner_cost_model.c` case uses the daemon via
  `cm_setup()` to create on-disk state, while most assertions call planner
  test helpers directly; its accurate description is "TCP setup + direct
  subject call," not simply TCP or direct. Conversely, all 22
  `test_find_with_total.c` cases reach the daemon (20 through
  `setup_obj()`, 2 through an explicit fixture start) and assert responses
  obtained through `tc_request`; an earlier line-range heuristic wrongly
  labeled most of them direct because it did not follow `setup_obj()` and
  omitted `tc_request` from its signal set. Do not use any heuristic-derived
  estimate as a case-count floor. The file-level counts above (129 / 6 / 9
  / 26 / 1) are only a rough starting inventory. **Task 1 Step 0.5 checks
  every one of the 282 registered cases and records setup transport
  separately from assertion/subject transport — no file is exempt.**
- **`test_runner_parallel.c`'s cases all spawn `./build/bin/shard-db-test`
  itself** as a subprocess via `popen()`, to test the runner's own
  `--jobs N` parallelism (interleaving, pass/fail totals, watchdog
  hard-abort) — not shard-db's daemon or query logic. It doesn't fit
  categories 1-10 (all about the shard-db-server boundary) or the
  direct-in-process-C-call bucket (it's real subprocess spawning), hence
  its own bucket above. Excluded from the transport rubric entirely; see
  Task 1 Step 0.5.

## Global Constraints

- No new dependencies.
- Build: `SKIP_TESTS=1 ./build.sh`. Test:
  `./build/bin/shard-db-test run-all` /
  `./build/bin/shard-db-test run <name>`.
- **This repo's standing execution mode**: leave work uncommitted after
  execution (Sonnet reviews the raw diff); execution runs on a fresh
  branch off `main` by a non-Claude model — do not spawn a Claude
  subagent to execute.
- **Diff baseline**: before starting, capture `git status --short`. This
  approved plan document may already be an uncommitted input handed to the
  executor; also capture
  `git diff -- docs/plans/2026-07-15-tcp-vs-direct-call-test-audit.md`, then
  do not edit or revert it. At handoff, the only change added by execution
  relative to those captured baselines must be the findings doc.
- **Scope discipline (explicit, from the user)**: this is a correctness
  audit, not a performance-motivated rewrite. Do not convert a
  TCP-based test to a direct-call test (or vice versa) solely to make
  the suite faster. Only touch a test when Task 1's audit finds it is
  using the wrong transport for what it actually verifies — i.e., a real
  correctness/coverage gap, not a speed opportunity.
- If a quoted anchor is not found exactly, stop and write
  `PLAN_NOTES.md` — do not guess.
- If a judgment call arises that this plan doesn't clearly resolve and
  isn't a per-case classification call (e.g., a scope question, a build
  command that doesn't work as documented, an ambiguity about which
  files are in-scope), stop and ask — do not improvise.
- **Carve-out**: per-case transport/verdict classification uncertainty
  ("is this specific case's assertion actually protocol-sensitive?") is
  explicitly **not** a stop-and-ask trigger — that would halt the audit
  on essentially every ambiguous case, and ambiguity here is expected at
  scale (282 cases). Record it as `NEEDS_REVIEW` in the findings table
  (see Task 1 Step 4) with your reasoning and move on. The human resolves
  `NEEDS_REVIEW` rows at the Task 2 checkpoint, not during execution.

## Task 1: build and apply a classification rubric (audit, no code changes)

**Files:** none modified — this task produces a report as a new file,
`docs/plans/2026-07-15-tcp-vs-direct-call-test-audit-findings.md`.

**Audit unit is the registered `TEST_REGISTER` case, not the file** (see
Background) — a file-level verdict can bundle cases with different
transport or intent. For each case, distinguish transport used only to
prepare state from transport actually traversed by the assertion/subject
under test; a case may legitimately have a compound classification such
as `TCP setup + direct assertion`.

**Methodology**: `grep -c ASSERT` / assertion-message greps alone are
**not** sufficient to establish transport intent — an assertion's text
doesn't say why the test needed a real daemon. For each case, read: the
registered entry function's full body, any setup/helper functions it
calls, the file header comment (usually states intent), and all
assertion forms used (not just `ASSERT_CONTAINS`/`ASSERT_EQ` — inspect all
`ASSERT_*` and local assertion helpers). Skimming is fine for cases that are
obviously in one bucket (e.g. a pure B+-tree-math case with no daemon
symbols in sight); slow down for anything ambiguous and use
`NEEDS_REVIEW` rather than guessing (see Global Constraints carve-out).

The question for each case is not "is it fast?" but: **does
what this test asserts actually depend on going through the real
daemon boundary?** A test needs TCP+subprocess (either via a shared
fixture — `test_env_start`/`test_env_start_ex`/`test_env_start_at` — or a
justified hand-rolled spawn) when it verifies any of:

1. **Wire protocol framing/parsing** — JSON request/response shape,
   `\0\n` framing, CSV/dict format output, error-JSON shape as seen by a
   real client.
2. **Auth/token/IP-trust enforcement** — token scope resolution
   (global/tenant/object), permission suffix (`r`/`rw`/`rwx`), trusted-IP
   bypass — these are enforced in `server.c`'s connection-handling path,
   not reachable via a direct C call into `query_*.c`.
3. **TLS handshake/certificate behavior.**
4. **Multi-connection / multi-thread concurrency as seen by the
   server's thread pool** — e.g. two real client connections racing,
   `MAX_CONCURRENT_QUERIES` semaphore behavior, per-object rwlock
   contention between concurrent *requests* (as opposed to concurrent
   in-process function calls, which a direct-call test can already
   exercise with its own pthreads).
5. **Process-level behavior** — daemon startup validation, crash-safety
   recovery on restart, single-instance flock guard, auto-vacuum/
   auto-reshard background threads, slow-query logging as observed
   through the server's request path.
6. **Server-level config wiring** — env var → running-daemon-behavior
   (e.g. `QUERY_BUFFER_MB`, `TIMEOUT`, `THREADS`) where the thing under
   test is "does the daemon actually apply this knob," not "does the
   underlying function behave correctly given a parameter."

A test does **not** need TCP+subprocess — a direct in-process C call
(linking the relevant `src/db/*.c` TUs directly, calling e.g.
`cmd_find`/`btree_insert`/`slotcask_put` etc.) is the correct and
sufficient transport — when it verifies:

7. Pure algorithmic/data-structure correctness (B+ tree operations,
   criteria matching, encode/decode, hashing, SIMD paths, keyset
   union/intersect, index-splits curve math).
8. Coverity-flagged-overflow regression tests (buffer bounds on a
   specific function's inputs).
9. A `.conf` file/config-parsing helper's behavior given crafted input.
10. In-process concurrency that doesn't depend on the server's thread
    pool or connection handling (e.g. `test_objlock_unit.c`,
    `test_parallel.c`'s own pthread usage against `parallel_for`).

- [ ] **Step 0 (re-derive the live file-level classification — do not
  trust the static lists above verbatim)**: this repo has active
  concurrent work landing test files daily, and the Background lists
  above were already stale once. Before auditing, regenerate the four
  buckets from source:
  ```bash
  for f in src/test/cases/*.c; do
    if grep -qE 'test_env_start(_ex)?\(' "$f"; then echo "env_start: $f";
    elif grep -q 'test_env_start_at(' "$f"; then echo "env_start_at: $f";
    elif grep -q 'fork()' "$f"; then echo "fork_only: $f";
    else echo "direct: $f"; fi
  done
  ```
  This script is a coarse **first pass** only — its `elif` priority
  hides hybrid files (see Background). Reconcile the fresh output
  against the Background lists; if it differs (new/removed/renamed
  files), note the delta (with file names) at the top of the findings
  doc from Step 4.
- [ ] **Step 0.5 (build the real audit unit: registered-case inventory —
  setup and assertion transport determined per case for EVERY file, no
  file exempted)**:
  Step 0's file-level bucket is **not** a valid substitute for this step
  for any file, including files not among the verified examples in
  Background — those examples are illustrative, not exhaustive. A file
  only using one
  fixture-call *name* throughout can still mix direct-call and TCP cases,
  or use TCP only for setup before directly invoking the subject under
  test. Verified examples and the previously-corrected heuristic false
  positives are listed in Background; none is a license to assume the
  remaining files are uniform.
  1. Enumerate every registered case fresh:
     `grep -rn 'TEST_REGISTER' src/test/cases/*.c` (282 at review time).
     Build a table: `case name | file | entry function name | setup
     transport | assertion/subject transport`.
  2. **For every one of the 282 cases** (not a subset), determine
     transport by locating that specific entry function's definition and
     reading its body and following local helper calls until both setup
     and assertion paths are resolved; do not stop at an arbitrary single
     level of indirection. As a discovery aid, grep the entry-function
     range and followed helpers for `test_env_start(_ex)?\(|
     test_env_start_at\(|tc_connect\(|tc_request\(|fork\(\)|popen\(|
     system\(`, plus direct calls to the named subject under test. Record
     all transports that apply rather than forcing a case into one
     mutually-exclusive bucket. A
     starting mechanical aid (verify its output, don't trust it blindly —
     it has known false-negative risk for helper-indirected calls, per
     the `test_runner_parallel.c` discrepancy found when this was
     prototyped during plan review):
     ```python
     import re, glob
     TRANSPORT_RE = re.compile(
         r'test_env_start_at\(|test_env_start(_ex)?\(|'
         r'tc_connect\(|tc_request\(|fork\(\)|popen\('
     )
     REG_RE = re.compile(r'TEST_REGISTER\(\s*"([^"]+)"\s*,\s*(\w+)\s*\)')
     for f in sorted(glob.glob("src/test/cases/*.c")):
         lines = open(f, encoding="utf-8", errors="replace").readlines()
         regs = [(m.group(1), m.group(2), i) for i, l in enumerate(lines)
                 for m in [REG_RE.search(l)] if m]
         fn_def = {}
         for _, fn, _ in regs:
             pat = re.compile(r'\b' + re.escape(fn) + r'\s*\(')
             for i, l in enumerate(lines):
                 if pat.search(l) and ('static' in l or l.strip().startswith(fn)):
                     fn_def[fn] = i; break
         ordered = sorted(fn_def.items(), key=lambda kv: kv[1])
         for idx, (fn, start) in enumerate(ordered):
             end = ordered[idx+1][1] if idx+1 < len(ordered) else len(lines)
             body = "".join(lines[start:end])
             print(f, fn, bool(TRANSPORT_RE.search(body)))
     ```
     This boolean only says that the bounded entry-function range contains
     one known signal; it neither follows helpers nor distinguishes setup
     from the assertion path. Only accept a case's classification after a
     manual read has resolved both paths. If they differ, record a compound
     value such as `TCP setup + direct assertion`; if either path remains
     unclear, mark `NEEDS_REVIEW`.
  3. Carve out **meta/CLI-subprocess tests** — cases that spawn the
     `shard-db-test` binary itself (or any test-harness tooling) via
     `popen()`/`fork()`+`exec()`, rather than the `shard-db` daemon.
     `test_runner_parallel.c` is the confirmed instance (`grep -l
     'popen(' src/test/cases/*.c` to check for others at execution
     time — `test_auto_vacuum.c` and `test_slotcask_basic.c` also match
     but their `popen()` calls are for `find`/CLI daemon-log helpers,
     not for re-invoking the test runner; confirm this hasn't changed).
     These cases are **excluded from categories 1-10** (the rubric is
     about the shard-db server boundary; a meta-test's correctness
     question is "does the runner's `--jobs N` produce the right
     results," which categories 1-10 don't speak to). Mark their
     verdict `N/A (meta-test)` in the findings table, not `OK`/gap/
     `NEEDS_REVIEW`.
  4. Once every case has been individually resolved, you may **report**
     a file as a single row/summary in the findings doc if (and only if)
     every one of its cases turned out to share the same verdict and the
     same setup/assertion transport pair —
     that's a presentation shortcut for Step 4, not a license to skip
     step 2's per-case check for that file. A grouped row must enumerate
     every included case name and entry function so the findings artifact
     still proves complete coverage.
- [ ] **Step 1**: for each case whose setup or assertion path uses a real
  daemon (`test_env_start`/`test_env_start_ex`/`test_env_start_at` or a
  hand-rolled spawn), apply the Methodology above and check whether the
  *stated assertion* actually traverses or depends on criteria 1-6. Cases
  recorded as `TCP setup + direct assertion` belong here for the question
  "is TCP required by the assertion?" and also in Step 2 for the reverse
  correctness check; Steps 1 and 2 intentionally overlap. Flag any case
  whose assertions are **entirely** about categories 7-10 (pure logic)
  despite going through a live daemon as
  `TCP NOT REQUIRED BY STATED ASSERTIONS` — this is **not** a
  recommendation to remove the daemon setup; a TCP-based test can still
  provide legitimate end-to-end wiring coverage beyond what its explicit
  assertions state. Report it, don't imply removal, and don't act on it
  per the scope-discipline constraint above.
- [ ] **Step 2**: for every case whose assertion/subject transport is a
  direct call — including the 36 cases in the 26 originally-identified
  files and every compound `TCP setup + direct assertion` case found by
  Step 0.5 — check the reverse: does any
  assertion actually
  depend on wire framing, auth, TLS, real multi-connection concurrency,
  or process-level behavior (categories 1-6) despite the assertion path
  not going through a daemon? **This is the correctness-relevant
  direction** — a direct-call
  test claiming to verify e.g. "concurrent writes from two clients don't
  corrupt X" but actually only spinning up in-process pthreads sharing
  one `ShardDb *` handle would NOT be exercising the real
  per-connection/thread-pool path, and could pass while a genuine
  server-level race exists.
- [ ] **Step 3**: for the 9 hand-rolled-fork files (`test_per_tenant_auth.c`,
  `test_auto_reshard.c`, `test_auto_reshard_shutdown_race.c`,
  `test_auto_vacuum.c`, `test_btcache_evict_race.c`, `test_tls.c`,
  `test_startup_validator.c`, `test_token_perms.c`,
  `test_warmup_vacuum_race.c`), confirm each one's documented reason for
  bypassing the shared fixtures is still accurate against the current
  fixtures' capabilities — but note the shared fixtures are themselves
  limited: `test_env_start_ex` unconditionally writes `TLS_ENABLE=0`
  into the `db.env` it creates (`src/test/fixtures.c` ~line 240).
  `test_env_start_at` only writes `TLS_ENABLE=0` when *it* creates
  `db.env` (`O_CREAT|O_EXCL` succeeds, ~line 359); if `db.env` already
  exists at that path it preserves the existing config as-is instead.
  Either way, neither fixture is currently usable for TLS testing in
  practice — `wait_daemon_ready`'s readiness probe (~line 114) speaks
  plaintext TCP, so even a preserved TLS-enabled `db.env` would fail
  the readiness check before a test could use it. A hand-rolled spawn
  testing TLS, a non-default server knob, or a specific restart/recovery
  sequence may remain justified. This is a maintainability observation,
  report only — do not propose conversions here (that's Phase 2's job if
  approved).
- [ ] **Step 4**: write findings to
  `docs/plans/2026-07-15-tcp-vs-direct-call-test-audit-findings.md` as a
  table: `case name | file | entry function | setup transport |
  assertion/subject transport | what it asserts | correct assertion
  transport per the rubric | verdict (OK /
  TCP NOT REQUIRED BY STATED ASSERTIONS / CORRECTNESS GAP / NEEDS_REVIEW
  / N/A (meta-test)) | proposed action (report-only — no fixes in this
  plan)`. Every "CORRECTNESS GAP" row must cite the specific
  assertion(s) that depend on a category-1-6 behavior the current
  transport can't actually exercise. Every `NEEDS_REVIEW` row must state
  why the call couldn't be made confidently. Open the doc with the Step
  0/0.5 reconciliation note (file and case counts actually audited, any
  delta from this plan's Background section, and the resolved hybrid-file
  and meta-test list).
- [ ] **Step 5 (report the non-actionable findings alongside the
  actionable ones — same doc, same step, not a separate task)**: for
  every case flagged `TCP NOT REQUIRED BY STATED ASSERTIONS` (**not** a
  claim the daemon setup is useless — just that it's not required by
  what's currently asserted; a TCP-based test can still provide
  legitimate end-to-end wiring coverage beyond its explicit assertions),
  the Step-3 fixture-modernization observations, and every
  `NEEDS_REVIEW` row: these are already in the Step 4 table by
  construction — additionally collect them into a short "optional
  follow-up, out of this plan's scope" summary list at the end of the
  findings doc, per the user's conservative-audit framing. Do not act on
  any of them without separate explicit sign-off — a future, separate
  task can pick these up if wanted.

## Task 2: human/planning checkpoint — STOP, no fixes in this plan

**Files:** none. This task is a handoff, not an implementation step.

- [ ] Present the findings doc from Task 1 Step 4 to the human (and/or
  the planning model) for review. Call out, specifically: every
  CORRECTNESS GAP row, every NEEDS_REVIEW row, and the hybrid-file /
  meta-test resolutions from Step 0.5.
- [ ] **Execution stops here.** Do not write, propose, or scaffold any
  fix. If Task 1 found zero CORRECTNESS GAP rows (NEEDS_REVIEW rows may
  still exist), say so explicitly — a clean audit is a valid, complete
  outcome for this plan, and there is nothing to hand off.
- [ ] If the human approves specific CORRECTNESS GAP (and/or resolved
  NEEDS_REVIEW) rows for fixing, that work is a **separate, new plan
  document** (e.g. `docs/plans/YYYY-MM-DD-tcp-audit-fixes.md`), written
  fresh against the approved rows — not an amendment to this plan, and
  not started by this plan's executor. That follow-up plan must, at
  minimum:
  - **Default to preserving the existing direct-call unit test and
    adding a new TCP-integration case** for the server-boundary gap,
    rather than converting or deleting the direct test. Conversion risks
    losing precise low-level coverage the direct test already provides
    even after the TCP case exists; only propose removing/replacing the
    original if the plan explicitly justifies why it adds no residual
    value once the new case exists.
  - **Not assume `test_env_start`/`test_env_start_ex` is always the
    right fixture for the new case.** `test_env_start_ex` unconditionally
    writes `TLS_ENABLE=0`; `test_env_start_at` only writes it when it's
    the one creating `db.env` (preserves an existing one otherwise) —
    but its plaintext-only readiness probe makes it unusable for TLS
    either way in practice (`src/test/fixtures.c` ~lines 114, 240, 359).
    Both fixtures only expose a `QUERY_BUFFER_MB` override otherwise — a
    gap involving TLS, daemon restart/recovery sequencing, or a
    non-default server knob needs `test_env_start_at` (for non-TLS knobs)
    or a justified new hand-rolled spawn (for TLS), and the plan must say
    which and why.
  - **Spell out the exact mutation-proof sequence per gap**, e.g.:
    1. Production code + existing direct test: passes.
    2. Inject a representative server-boundary defect: existing direct
       test still passes (proves the gap).
    3. Add the new TCP regression case, defect still present: new case
       fails.
    4. Remove the defect: new case passes.
    5. Confirm no trace of the injected defect remains in the diff.

    The plan must name the *specific* mutation for each gap up front
    (which line, what wrong behavior) — the choice of mutation is
    itself design work and belongs in that plan, decided against the
    real gap, not improvised generically here.
  - Use the exact test commands from this repo's CLAUDE.md
    (`SKIP_TESTS=1 ./build.sh` to build;
    `./build/bin/shard-db-test run-all` to test, `--jobs 1` for a
    sequential baseline run and the default parallel run as a second
    pass — not a vague "and parallel").

## Definition of done

- [ ] `docs/plans/2026-07-15-tcp-vs-direct-call-test-audit-findings.md`
  exists and covers every registered `TEST_REGISTER` case (not just
  file) under `src/test/cases/*.c`, as counted fresh by Task 1 Steps 0
  and 0.5 at execution time (171 files / 282 cases as of 2026-07-16;
  this plan's earlier drafts claimed 167 files and no case-level count —
  re-derive both, don't hardcode).
- [ ] Every case has separate setup-transport and assertion/subject-
  transport classifications; compound cases such as `TCP setup + direct
  assertion` were evaluated by both Task 1 Steps 1 and 2. The 5 known
  fixture-variant hybrid files and all mixed-case files are classified
  per-case, never inherited from a file-level bucket.
- [ ] `test_runner_parallel.c` (and any other meta/CLI-subprocess test
  Step 0.5 finds) is marked `N/A (meta-test)`, not forced into the
  transport rubric.
- [ ] No CORRECTNESS GAP row was fixed in this plan — Task 2 is a
  checkpoint/handoff, not an implementation task. Any approved fix lives
  in a separate follow-up plan.
- [ ] No test was converted or modified for speed alone, or at all — this
  plan makes zero code changes. Relative to the captured `git status` and
  plan-diff baselines, the only added change is the new findings doc; the
  executor did not edit or revert this approved plan or any other
  pre-existing worktree change.
- [ ] `SKIP_TESTS=1 ./build.sh` still builds clean (nothing should have
  changed under `src/`), confirming the audit stayed read-only.
