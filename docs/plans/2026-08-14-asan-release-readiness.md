# ASan release-readiness

## Goal

Make the ASan/UBSan build clean under concurrent test execution before any
release, feature work, or TSan remediation. This plan deliberately does not
alter GitHub Actions or TSan: each deserves a separate, evidence-based plan.

Amendment (approved 2026-08-14): the all-CPU ASan run is clean of sanitizer
findings but `test-auto-reshard-throttle` exceeds the runner's 180-second
watchdog when it contends with other cases. Preserve its real 1,050,000-record
datasets and schedule it as an explicit exclusive case: drain ordinary cases,
run it without competing test daemons, then resume ordinary parallel work.
The fixture cannot use a synthetic threshold count because rebuild deliberately
fails closed when copied live records do not match the source KF-header count.

## Evidence and feedback loop

The following is a short, red-capable reproduction on the current `main`:

```sh
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run test-bitmap-index
```

It reaches the post-restart phase, prints repeated
`slotcask_registry: open failed .../bulkcapovf`, then fails the persistence
assertion with the pre-restart survivor count versus zero after restart. The
full ASan run also reported LeakSanitizer allocations rooted at
`slotcask_open()` through `slotcask_registry_get()` from request and warmup
paths. The restart error's actual errno/recovery reason is not yet exposed;
therefore no repair mechanism is assumed in this plan.

## Invariants

- An acknowledged bulk-insert record is either readable after clean restart or
  the mutation returned an error; an `exists` error must never be interpreted
  as `false` by a persistence test.
- Partial bitmap-cap rejection may reject only affected records; it must not
  make the object impossible to reopen or discard accepted peers.
- Every successful registry open is released at daemon shutdown, after all
  background users have stopped.
- No sanitizer finding is suppressed, ignored, retried away, or converted into
  a skipped test.
- The final ASan verification uses concurrent `run-all` (no `--jobs 1`), not
  merely the focused case.
- The auto-reshard fixtures retain their real 1,050,000-record datasets. A
  fabricated count cannot replace those rows because reshard validates its
  copied live-record count against the source KF-header count and fails closed
  on a mismatch.

## Task 1 — expose and capture the actual restart-open failure

Test first. In `src/test/cases/test_bitmap_index.c`, locate the quoted anchor
`"bulk-window survivors unchanged across restart"`. Before counting the
post-restart result as existing/non-existing, add an assertion that the
response is non-null and does not contain `"error"`. The assertion message
must name `bulkcapovf restart exists response` and print the response through
the existing TAP diagnostic mechanism on failure. Run the focused ASan command
above and prove this new assertion is red before any production change.

In `src/db/slotcask.c`, locate the quoted anchor
`fprintf(stderr, "slotcask_registry: open failed for %s/%s\\n",`. Preserve
`errno` immediately after `slotcask_open()` returns, restore it before return,
and include `strerror(saved_errno)` in the existing error message. Do not
replace the low-level reason with a generic error. Re-run the focused test and
record the first concrete failing syscall/recovery validation path.

Decision gate: if the captured reason does not identify a single failing
operation and its inputs, stop execution and add a narrowly scoped diagnostic
probe tagged `[DEBUG-asan-restart]`; remove it once the cause is established.
Do not guess a repair or continue to Task 2 without a confirmed cause.

## Task 2 — repair the confirmed restart/persistence mechanism

This task starts only after Task 1 identifies the specific mechanism. Make
the smallest repair at its ownership boundary, preserving the invariants
above. Before the repair, extend the existing `bulkcapovf` scenario to assert:

1. each post-restart `exists` response is a valid boolean response rather
   than an error;
2. every pre-restart accepted key remains readable after restart; and
3. rejected keys remain absent.

The extended assertion must fail with the base revision, pass with the repair,
and be left as the regression test. If the cause is crash-marker evidence,
test both intent-only and intent-plus-abort recovery states. If it is segment
recovery, test the precise active stream/file transition which failed—do not
broaden recovery acceptance for corrupt records.

After the repair, temporarily revert only the repair hunk, run the focused
test, and paste the expected red result; re-apply the exact hunk and paste the
green result.

## Task 3 — eliminate ASan registry/warmup leaks

Use the full-ASan leak stacks to identify each allocation family from
`slotcask_open()` (`streams`, `kf_slot_refs`, `seg_slot_refs`, and
`seg_slot_caps`) and prove which shutdown path bypasses `slotcask_close()`.
Locate the quoted anchors `void bg_threads_stop(ShardDb *db)` in
`src/db/server.c` and `void slotcask_shutdown(void)` in `src/db/slotcask.c`.
Ensure all warmup/background threads have been joined before registry teardown,
and ensure each registered database is closed exactly once.

Add a focused daemon lifecycle regression test using the existing warmup test
helpers: start with warmup enabled, force at least one registry open, perform
a clean stop, and assert clean exit. It must be run under ASan with leak
detection enabled and fail before the ownership fix. Do not use a LeakSanitizer
suppression or disable `detect_leaks`.

## Task 4 — make the throttle deadline sanitizer-appropriate without weakening it

Test first. The baseline all-CPU ASan command below is red solely at
`test-auto-reshard-throttle`: both objects fail to reach 16 splits inside its
40-second deadline. Preserve that output.

In `src/test/cases/test_auto_reshard.c`, locate the quoted anchors
`#define GROWN_BASE_COUNT 1050000`, `#define RESHARD_WAIT_SCALE 6`, and
`/* Two objects, both under-split (splits=8, seeded with GROWN_BASE_COUNT`.

Change the sanitizer conditional so ASan and TSan both use
`RESHARD_WAIT_SCALE 6`; keep uninstrumented builds at one. The wait grows,
but both required 8→16 transitions and both completion-log assertions stay
mandatory. Do not alter the real-record fixture or fabricate a KF count: a
source/header count mismatch is deliberately rejected by rebuild recovery.

Run the throttle case under ASan at default worker count. It must prove both
objects reach 16 splits and that at least two `splits done` log lines appear.
Then run the uninstrumented throttle case to ensure its normal deadline and
semantics remain unchanged.

## Task 5 — schedule heavyweight cases exclusively

Test first. In `src/test/cases/test_runner_parallel.c`, add a small registered
fixture pair consisting of a parallel-safe ordinary case and an exclusive case
that records its start/end in the existing test-runner capture output. Assert
that the exclusive case never overlaps an ordinary case and that ordinary cases
resume after it completes when the runner uses `--jobs 2`.

In `src/test/test_runner.h`, locate the quoted anchor `TEST_REGISTER(` and add
an explicit exclusive registration form alongside it. In `src/test/test_runner.c`,
locate the quoted anchors `typedef struct {` defining the registered test case
and `static int run_all_parallel(`. Add an `exclusive` property and change the
scheduler so it:

1. does not start an exclusive case while any child is active;
2. starts no ordinary case while an exclusive child is active; and
3. preserves the existing per-child watchdog and result collection behavior.

Register only `test-auto-reshard-throttle` using the exclusive form. Do not
special-case its name in the runner. Prove the new runner regression is red on
the base scheduler, green after the scheduler change, and re-run the existing
`test-runner-parallel` case.

## Task 6 — concurrent ASan release gate

Run, fresh from the repaired tree:

```sh
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all
```

Run that concurrent command three independent times. Each run must report zero
failed assertions, no ASan/UBSan/LeakSanitizer output, and no watchdog abort.
Then run the same build's focused `test-bitmap-index` and the new lifecycle
test once more. Do not run benchmarks.

If any failure shifts between runs, treat it as unresolved: retain the failed
artifact, make a short red-capable reproduction, and add it to this plan before
declaring ASan complete. Leave all changes uncommitted for raw-diff review.

## Execution rules

- Work on a fresh `fix/asan-release-readiness` branch from `main`.
- Do tasks in order. If any quoted anchor is absent, write `PLAN_NOTES.md` and
  halt the entire execution run.
- No GitHub workflow, TSan suppression, or unrelated feature change is in
  scope for this plan. The approved ASan wait scale and explicit exclusive
  scheduling of `test-auto-reshard-throttle` are the sole harness changes in
  scope.
- After raw-diff review and only after human approval, the next plan is TSan
  remediation; it starts from the ASan-clean revision.
