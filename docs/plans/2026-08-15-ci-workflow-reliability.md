# CI workflow reliability — implementation plan

## Evidence and root causes

GitHub Actions run `31842651577` (2026-08-14) establishes two independent
CI failures:

1. **macOS ARM64 compile failure.** Apple Clang rejects four references to
   Linux-specific `EUCLEAN` in `src/db/slotcask.c`; Darwin has no such errno
   constant. This happens before the test suite starts.
2. **Linux ARM64 test failure.** The serial suite completed in roughly
   14 minutes but `test-auto-reshard` failed its 1,050,000-record
   preservation assertion and its already-correctly-sized-object assertion.
   This is a functional failure, not a workflow timeout. Its specific
   mechanism is not yet known, so no speculative workflow-only fix may mask it.

The other apparent failures are mostly cancellations. Each merge triggers a
new `push` suite on `main` while an earlier workflow is still running;
`concurrency.group: ${{ github.workflow }}-${{ github.ref }}` cancels the
older run of that workflow. Each workflow job has its own hosted runner;
they do not share one machine. TSan has repeatedly been cancelled after more
than two hours, not demonstrated to have reached its `timeout-minutes: 150`
limit.

## Decision required before execution

The expensive workflows currently run for both `pull_request` and `push` to
`main`. Choose one policy before changing their triggers:

- **PR-only (recommended):** CI, Coverage, ASan, and TSan validate PR heads;
  main receives no duplicate full suite after merge. This eliminates the
  cancellation churn, but a merge queue/branch-protection policy must ensure
  the checked PR SHA is what merges.
- **PR + main:** retain post-merge validation and accept that newer commits
  cancel stale in-progress runs; optimize only individual job runtime.

## Global execution rules

- Start from current `main` on a fresh `fix/ci-workflow-reliability` branch.
- Leave all changes uncommitted for raw-diff review.
- If a quoted anchor below is absent, write `PLAN_NOTES.md` and stop.
- Build with `SKIP_TESTS=1 ./build.sh`; never run benchmarks.
- Do not change `--jobs` from 1 to 2 until the auto-reshard regression is
  reproduced and explained. Coverage must remain `--jobs 1` because gcov
  output is deliberately sequential.

## Task 1 — make the macOS portability failure red locally

The regression command is a preprocessor compile check, which is fast and
directly exercises the failure:

```sh
clang -fsyntax-only -Isrc/db src/db/slotcask.c
```

It must fail before the change with an undeclared `EUCLEAN` diagnostic (or,
if the host is not Darwin, the executor records that macOS GitHub Actions is
the only available red-capable environment and uses its failed log as the
reproduction evidence).

In `src/db/slotcask.c`, find the include block ending with the project's
headers and insert this exact compatibility definition immediately after it:

```c
/* Linux exposes EUCLEAN for detected on-disk corruption; Darwin does not.
   EIO is the portable errno for an unreadable/corrupt storage object. */
#ifndef EUCLEAN
#define EUCLEAN EIO
#endif
```

Then run `SKIP_TESTS=1 ./build.sh`. A macOS CI run must compile the matrix
leg before this task is considered passed.

## Task 2 — diagnose and lock down `test-auto-reshard` before changing CI width

Root cause is unknown. First add no production or workflow change. Run the
actual target repeatedly on an ARM64 Linux GitHub runner (draft branch run)
and preserve the response bodies that the two failing assertions consume.
The diagnostic must report:

1. parsed `count` response and `wctx.ok` at the count assertion;
2. full `describe-object sized` response;
3. AUTO-RESHARD log lines for `grown`, `sized`, and `huge`.

The temporary diagnostics must be uniquely tagged and removed before any
final diff. The red-capable command is:

```sh
./build/bin/shard-db-test run test-auto-reshard
```

After identifying one specific mechanism, write a regression test that fails
on the base branch and proves that mechanism. The plan for the resulting
production/test fix must include a revert-to-red, reapply-to-green proof;
do not substitute `--jobs 2`, longer waits, or skipped assertions for that
proof.

## Task 3 — workflow trigger policy and resource measurement

Apply the user-approved policy to the exact trigger anchors in
`.github/workflows/ci.yml`, `.github/workflows/codecov.yml`,
`.github/workflows/sanitizers.yml`, and `.github/workflows/tsan.yml`:

```yaml
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
```

If **PR-only** is selected, replace each block with:

```yaml
on:
  pull_request:
    branches: [main]
```

If **PR + main** is selected, leave it unchanged and document that
cancellations are expected obsolete-run cancellation, not test failures.

Add a short runner diagnostic step before each test step to report available
CPU and memory, then remove it only after capturing one green run:

```yaml
      - name: Report runner capacity
        run: |
          nproc || sysctl -n hw.ncpu
          free -h || vm_stat
```

Use its output plus job step timestamps to set timeouts from measured
runtime, with at least 50% headroom. Do not lower a timeout merely to make
cancelled work appear quicker.

## Task 4 — conditional `--jobs 2` experiment

Only after Tasks 1–2 are green, make a separate, reviewable CI experiment:

```yaml
      - name: Run full C test suite
        run: ./build/bin/shard-db-test run-all --jobs 2
```

This replaces only the same `run-all --jobs 1` command in the regular CI
matrix. It must not modify Coverage or TSan in the experiment. Compare at
least three ARM64 and x86_64 runs with the previous serial baseline; accept
the change only if all runs pass and test-step wall time improves. Otherwise
restore `--jobs 1`.

## Verification

1. `SKIP_TESTS=1 ./build.sh` passes locally.
2. The auto-reshard regression test passes after its root-cause fix, and is
   shown failing when that fix is temporarily reverted.
3. Full default suite: `./build/bin/shard-db-test run-all --jobs 1`.
4. Workflow YAML is reviewed as a raw diff; no commits are created.
5. On the branch, inspect CI, Coverage, ASan, and TSan logs with
   `gh run view <id> --log-failed`; report completed durations separately
   from cancellations and queues.
