# Test Transport Audit: TCP+subprocess vs. direct C calls — Plan B

**Goal**: audit `src/test/cases/*.c` (167 files) for whether each test
uses the *correct* transport for what it claims to verify — **not** a
speed-driven conversion pass. A test that spins up a real daemon over TCP
when it's only exercising a pure function is wasted setup cost but not a
correctness bug; a test that uses a direct in-process C call when the
thing under test is genuinely protocol-, auth-, concurrency-, or
crash-boundary-sensitive is a **false sense of coverage** — it can pass
while the real (TCP-facing, multi-threaded, multi-process) behavior is
broken. This plan finds and fixes the second class only. The first class
(TCP-tests that could be direct calls) is reported for visibility but
**not** converted unless a genuine correctness gap is found alongside it
— this is an explicit, user-set boundary on scope (see CLAUDE.md's
recorded instruction: "audit for correctness only, conservative, not a
blanket speed conversion").

## Background (from prior investigation, re-verified this session)

`src/test/cases/*.c` = 167 files.
- 134 call `test_env_start`/`test_env_start_ex` (the standard fixture:
  spawns a real `shard-db` daemon subprocess on a free port, talks to it
  over the real TCP+JSON wire protocol via `tc_connect`/`tc_request`).
- 33 do not call `test_env_start`. Of those:
  - 6 still exercise a real daemon over TCP, but via a **hand-rolled**
    `fork()`+`execl()` spawn instead of the shared fixture (verified via
    grep for `fork()`/`tc_connect`/`TestClient` across the 33):
    `test_per_tenant_auth.c`, `test_auto_reshard.c`,
    `test_auto_vacuum.c`, `test_tls.c`, `test_startup_validator.c`,
    `test_token_perms.c`. Each has a documented reason in its own file
    header for not using the shared fixture (typically: needs a custom
    `db.env` with specific knobs the fixture's default env doesn't set,
    e.g. `AUTO_VACUUM=1`, TLS cert paths, or a startup-time validation
    flag).
  - 27 are true direct-in-process-C-call unit tests, no daemon, no
    fork, no TCP: `test_btree.c`, `test_btree_value_hash_sort.c`,
    `test_btree_inplace_leaf.c`, `test_kfcache_staleness.c`,
    `test_o_direct_scan.c`, `test_coverity_encode_criterion_overflow.c`,
    `test_coverity_group_by_overflow.c`,
    `test_coverity_join_buf_overflow.c`,
    `test_coverity_seg_scan_varlen_overflow.c`, `test_slotcask_basic.c`,
    `test_config_encode.c`, `test_index_splits_curve.c`,
    `test_keyset.c`, `test_nql.c`, `test_reshard_target.c`,
    `test_objlock_unit.c`, `test_runner_parallel.c`, `test_parallel.c`,
    `test_slotcask_cas.c`, `test_simd.c`, `test_segcache_staleness.c`,
    `test_slotcask_resplit.c`, `test_slow_query_log.c`,
    `test_slotcask_api.c`, `test_tls_unit.c`, `test_util.c`,
    `test_variable_length.c`.

## Global Constraints

- No new dependencies.
- Build: `SKIP_TESTS=1 ./build.sh`. Test:
  `./build/bin/shard-db-test run-all` /
  `./build/bin/shard-db-test run <name>`.
- **This repo's standing execution mode**: leave work uncommitted after
  execution (Sonnet reviews the raw diff); execution runs on a fresh
  branch off `main` by a non-Claude model — do not spawn a Claude
  subagent to execute.
- **Scope discipline (explicit, from the user)**: this is a correctness
  audit, not a performance-motivated rewrite. Do not convert a
  TCP-based test to a direct-call test (or vice versa) solely to make
  the suite faster. Only touch a test when Task 1's audit finds it is
  using the wrong transport for what it actually verifies — i.e., a real
  correctness/coverage gap, not a speed opportunity.
- If a quoted anchor is not found exactly, stop and write
  `PLAN_NOTES.md` — do not guess.
- If a judgment call arises that this plan doesn't clearly resolve
  (e.g., "is this test's assertion actually protocol-sensitive?"), stop
  and ask — do not improvise a reclassification.

## Task 1: build and apply a classification rubric (audit, no code changes)

**Files:** none modified — this task produces a report as a new file,
`docs/plans/2026-07-15-tcp-vs-direct-call-test-audit-findings.md`.

The question for each of the 167 files is not "is it fast?" but: **does
what this test asserts actually depend on going through the real
daemon boundary?** A test needs TCP+subprocess (either via the shared
`test_env_start` fixture or a justified hand-rolled spawn) when it
verifies any of:

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

- [ ] **Step 1**: for each of the 134 `test_env_start`-based files, skim
  what it actually asserts (`grep -c ASSERT` + a read of assertion
  messages is enough — no need to read every full file top-to-bottom;
  use the Explore agent or targeted greps for `ASSERT_CONTAINS\|
  ASSERT_EQ` messages per file to work through this efficiently) and
  check it against criteria 1-6. Flag any file whose assertions are
  **entirely** about categories 7-10 (pure logic) despite going through
  a live daemon — these are the "wasted setup, not a bug" class,
  reported but not touched per the scope-discipline constraint above.
- [ ] **Step 2**: for each of the 27 direct-call files, check the
  reverse — does any assertion actually depend on wire framing, auth,
  TLS, real multi-connection concurrency, or process-level behavior
  (categories 1-6) despite not going through a daemon? **This is the
  correctness-relevant direction** — a direct-call test claiming to
  verify e.g. "concurrent writes from two clients don't corrupt X" but
  actually only spinning up in-process pthreads sharing one `ShardDb *`
  handle would NOT be exercising the real per-connection/thread-pool
  path, and could pass while a genuine server-level race exists.
- [ ] **Step 3**: for the 6 hand-rolled-fork files, confirm each one's
  documented reason for bypassing the shared fixture is still accurate
  against the current fixture's capabilities (e.g., can
  `test_env_start_ex` now accept custom env vars that it couldn't when
  these were written? If so, that's a maintainability finding — using
  the shared fixture where possible reduces duplicated spawn/readiness-
  poll logic — but only convert if Task 1's correctness lens, not speed,
  motivates it; note it as an optional low-priority cleanup, not a
  required fix.)
- [ ] **Step 4**: write findings to
  `docs/plans/2026-07-15-tcp-vs-direct-call-test-audit-findings.md` as a
  table: `file | current transport | what it asserts | correct
  transport per the rubric | verdict (OK / wasted-setup-only /
  CORRECTNESS GAP) | proposed action`. Every "CORRECTNESS GAP" row must
  cite the specific assertion(s) that depend on a category-1-6 behavior
  the current transport can't actually exercise.

## Task 2: fix confirmed correctness gaps only

**Files:** whichever specific files Task 1 flags as CORRECTNESS GAP —
cannot be enumerated in advance since Task 1 hasn't run yet.

- [ ] **Step 1**: for each CORRECTNESS GAP row from Task 1's findings
  table, write a **failing** assertion first if possible (i.e., confirm
  the current direct-call version of the test would NOT catch a real
  bug in the category-1-6 behavior it claims to cover — e.g., temporarily
  introduce the bug it should catch, in a throwaway local build, and
  show the existing test still passes) before converting the test to go
  through `test_env_start`/TCP. This is the TDD proof that the gap is
  real, not hypothetical.
- [ ] **Step 2**: convert that specific test to use the shared
  `test_env_start` fixture + `tc_request`, following the pattern in an
  existing TCP-based test that covers similar ground (cite the specific
  file used as a template).
- [ ] **Step 3**: re-run with the previously-introduced bug present —
  confirm the converted test now fails. Revert the bug, confirm it
  passes. Paste both outputs (the "revert → confirm-fail → reapply →
  confirm-pass" evidence CORE-PROCESS requires for every fix).
- [ ] **Step 4**: full suite sanity (`./build/bin/shard-db-test run-all
  --jobs 1` and parallel). Paste output.

If Task 1 finds **zero** correctness gaps, Task 2 is a no-op — say so
explicitly, do not invent a conversion to have something to show. A
clean audit with no findings is a valid, complete outcome for this plan.

## Task 3: report wasted-setup-only findings (no code changes)

For files flagged "wasted-setup-only" (TCP fixture used for pure-logic
assertions) or the Task-1-Step-3 fixture-modernization candidates: list
them in the findings doc as optional follow-up cleanup, explicitly
labeled as **out of this plan's scope** per the user's conservative-
audit framing. Do not act on them without separate explicit sign-off —
a future, separate "test suite speed" task can pick these up if wanted.

## Definition of done

- [ ] `docs/plans/2026-07-15-tcp-vs-direct-call-test-audit-findings.md`
  exists and covers all 167 files.
- [ ] Every CORRECTNESS GAP finding (if any) has a fix with proven
  revert/confirm-fail/reapply/confirm-pass evidence.
- [ ] No test was converted for speed alone.
- [ ] Full suite green, both `--jobs 1` and parallel.
