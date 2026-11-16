# Phase 2 build-wiring amendment and halted-checkpoint handoff

Date: 2026-10-22. Base HEAD
`f5b80b4c58e49cfd8ac8b74ed9c1366f7b3a3130`, branch
`fix/phase1-safe-scalar-conversions`.

The executor correctly halted at Task 1e. The root read PLAN_NOTES.md,
confirmed its diagnosis, and amended the plan. The root's prior review
verified the four quoted anchors but missed a separate test-runner source
list. Earlier standalone C replays were not integrated build.sh evidence.
They must not be cited as proof that the original build wiring worked.

## Patched plan and exact scope

Current plan: `2026-10-22-phase2-decimal-core-plan.md`, SHA256:

```text
58378f603a5d9d75c393f52fb32f5ea48f932c6bff9541591af31a5b30f5e570
```

It supersedes approved SHA `092f9bed...27a425` only as a proposed amendment;
resumption still requires the human's approval. No source implementation
has been resumed by the root.

- Keep Task 1d edits 1–4. Edit 3 is now correctly labeled the TEST_BUILD
  daemon (`shard-db-test-server`), and remains part of the plan.
- Add edit 5 to the actual test runner's source list. Its quoted two-line
  anchor names `-o shard-db-test` and `src/test/shard-db-test.c`, so it cannot
  match the daemon or the repeated benchmark tail. The replacement adds
  `src/db/decnum.c` exactly once.
- The inventory now names four module source lists plus case registration:
  production daemon, embedded library, test daemon, test runner; five edits.
- The benchmark source list is unchanged: it has no decimal caller in this
  phase. The normal build compiles it, but no benchmark is run.
- The first repaired integrated build also exposed an unused static helper,
  dec_status_name, in the new unit case. Remove that helper only. The current
  Task 1c file omits it, and a separate resume-only section quotes the entire
  unique helper for deletion from the already-created test file. All 192
  assertion invocations and test bodies remain unchanged.

No decimal-core implementation, API, numerical policy, packed format or test
expectation changed in this amendment. No assertions were weakened.

## Actual integrated verification

The root created an isolated fresh `git archive HEAD` checkout at
`/tmp/p2-wiring-repair-9n_vh81y/`, without the untracked protected plan. The
three existing Task 1 files were initially verified byte-identical to their
approved fenced blocks. All five literal build replacements were applied to
the clean archived build.sh, with every anchor matching exactly once. The
result after edits 1–4 was byte-identical to the executor's halted build.sh.
The unused helper was then removed from the scratch test only, making it
byte-identical to amended Task 1c. The real complete build.sh and real test
runner were executed; this was not a stub test harness or syntax-only check.

First integration attempt: build exit 0, but an unused-helper warning; real
RED 185/424, exit 168. The scratch supervisor had initially assumed RED would
exit 1, which was too strict: expected RED means a nonzero test status plus
the expected failures, not a particular value. That diagnostic-driver
assumption was corrected; no test assertion or runner behavior changed.

Final actual commands/output (cwd is the isolated archive):

```text
rtk proxy env BUILD_MODE=release SKIP_TESTS=1 ./build.sh
exit_code: 0
elapsed_seconds: 70.94
compiler warning/error diagnostics: none
Built: build/bin/

rtk proxy ./build/bin/shard-db-test run test-decnum-core
# test-decnum-core: 185 passed, 424 failed
exit_code: 168
elapsed_seconds: 0.02
```

The RED is the intended missing-core behavior of the approved stub, rather
than a link failure. No mathematical implementation from Tasks 2–7 was applied
for this validation. No full suite was run in this diagnostic repair; the
executor must still run Task 1e's baseline checks and the later normal suite.
No sanitizer or benchmark was run.

Two independent reviewers checked the target inventory, anchor uniqueness,
checkpoint instructions and unused-helper-only deletion; no open findings.

## State preserved

The root changed only the plan and this amendment report. Hashes of the
executor's build.sh, decnum.h, stub decnum.c, new unit case and PLAN_NOTES.md
match their pre-repair checkpoint hashes. No workspace source/test/build
edit, git staging, commit, push, PR, merge or later-phase execution occurred.
Keep PLAN_NOTES.md unchanged as the original halt evidence.

The protected certification plan was verified by hash only and remains:
`fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.
Raw build/test logs and JSON identities stay in the local scratch directory,
outside version control.

## Copy-ready approval/resumption message

Approve the amended Phase 2 plan at SHA256
`58378f603a5d9d75c393f52fb32f5ea48f932c6bff9541591af31a5b30f5e570`
and resume from the checkpoint recorded in PLAN_NOTES.md.

1. Read this amendment and the updated plan; verify HEAD, plan hash, protected
   hash and the existing partial implementation against the declared checkpoint.
2. Preserve Tasks 1a–1c and Task 1d edits 1–4. Apply only the new Task 1d
   edit 5 and the precisely quoted resume-only unused-helper deletion. Do not
   reapply earlier edits, overwrite the stubs or alter assertions. Verify all
   three Task 1 files now match their amended complete code blocks.
3. Restart Task 1e: perform the normal build and real focused RED capture, then
   its baseline checks. Record actual statuses/counts (RED may exit 168).
4. Continue Tasks 2–8 literally, including the ordinary oracle/OOM probes and
   normal full suite. Keep the entire implementation uncommitted/unstaged for
   plan-aware and independent blind raw-diff review.
5. No sanitizers, benchmarks, debug/log commits, push/PR/merge, certification
   work or later phases. Preserve the protected file and the halt record.
   Halt again if the checkpoint or any anchor differs; do not improvise.
