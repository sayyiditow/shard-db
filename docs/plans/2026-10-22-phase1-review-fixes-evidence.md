# Phase 1 implementation-review fixes — evidence and next steps

Date: 2026-10-22. Branch: `fix/phase1-safe-scalar-conversions`.
HEAD remains `1dd1888fda49ce295717590d1f65415963445270`.
All implementation and planning work is uncommitted. No git staging,
commits, pushes, PR creation, benchmarks or sanitizer runs were performed
by this fix pass. No later phase was implemented.

## Findings resolved

- **I-01:** Removed the scalar text-index allocation and its zero-on-OOM
  fallback. The renamed internal `encode_field_text_for_index` consumes
  existing complete C strings for libc-parsed scalar kinds. All nine
  direct consumers and the indirect criterion wrapper were audited;
  arbitrary varchar/numeric spans retain their own length semantics.
  Scalar termination assertions enforce the documented internal contract.
  The public embedded header never exported this internal converter.
- **I-02:** Delimited updates now pass `cell_len` to the checked encoder.
  A malformed scalar containing NUL and a suffix is rejected as a whole;
  old data/index entries survive, other valid rows progress, and varchar
  keeps bytes after embedded NUL.
- **I-03:** Retained the new-write finite-only policy checks and restored
  legacy-source coverage using stopped-daemon fixtures. The test discovers
  the actual stream, validates/patches the existing record, verifies
  NaN/Infinity after reopening the same root, and asserts named $copy
  rejection plus destination preservation.
- **I-04:** Quoted eligible JSON fields decode once before checked
  conversion. The shared extraction walk carries original quote metadata;
  single/bulk inserts and updates, index extraction and decoded-NUL
  rejection now agree. Defaults and CSV remain literal inputs.

The complete anchored amendments are linked from the child-plan header:
`2026-10-22-phase1-index-oom-amendment.md`,
`2026-10-22-phase1-csv-amendment.md`,
`2026-10-22-phase1-legacy-copy-amendment.md`, and
`2026-10-22-phase1-scalar-routes-amendment.md`.
Those amendments override the historical child snippets for the corrected
functions. The evidence subfolder preserves logs and source identities.

## Actual checks

Normal build command: `rtk proxy env SKIP_TESTS=1 ./build.sh`.
The build log completed its final Built/Deploy output with no compiler
warning/error diagnostics. The source snapshot was frozen before the
build; all 22 recorded code/doc file hashes stayed unchanged through the
following targeted and full-suite runs.

```text
test-scalar-conversion-safety: 131 passed, 0 failed
test-scalar-routes-checked: 78 passed, 0 failed
test-scalar-csv-update: 30 passed, 0 failed
test-computed-update: 75 passed, 0 failed
test-computed-update-legacy-nonfinite: 19 passed, 0 failed
index text allocation-fault probe: 9 passed, 0 failed; zero allocation attempts
aggregate direct helpers: 10 passed, 0 failed
# total: 17516 passed, 0 failed across 515 cases
full-suite exit code: 0; elapsed 238.53 seconds
```

Targeted commands were `rtk proxy ./build/bin/shard-db-test run <case>`;
the full command was `rtk proxy ./build/bin/shard-db-test run-all`. Local
daemon/socket permissions were enabled for the fixture runs. The scalar
allocation probe's exact compile/run command is in its amendment. The
aggregate helper used `2026-10-22-phase1-scratch/run-probe.py` against this
actual checkout, not a scratch implementation. `git diff --check` passed.
Old encoder/helper names are absent from src/db; each new C test file is
registered once. Ignored graphify output was neither altered nor counted.

The first JSON red capture was 31 passed/14 failed. Its escaped-write
failures are genuine; three new count checks also had response-newline/
expected-count mistakes, which were corrected to full integer parsing and
mathematically correct counts before implementation. An additional 33
unit assertions were added after that red capture; do not claim the entire
final 78-assertion case was captured red. CSV red was 22 passed/8 failed.
The earlier focused I-01 red showed 42 becoming0 on allocation failure;
its new probe proves this allocation no longer exists.

The first legacy fixture run (7 passed/8 failed) was a fixture setup
failure, not a guard regression: create-object auto-selects stream count,
so a hard-coded stream000 was wrong. The corrected fixture discovers the
actual stream and explicitly verifies the loaded non-finite source before
copy. Its 19-assertion run is green. No failing guard was fixed here;
missing coverage was restored. All misleading first-run outputs remain
preserved rather than being silently discarded or described as valid red.

Full raw logs: `/tmp/p1-review-fixes-evidence/`. Full-suite log SHA256:
`bac1c3c9ac3087e86eb76b244956cba96fd5ed8dbf80f7e4fb42edb4d1d75fef`.
Persistent targeted logs/summary/identities:
[2026-10-22-phase1-review-fixes-evidence](2026-10-22-phase1-review-fixes-evidence/).

## Review status

The implementation agents completed source changes and source freeze.
The root performed the completed runtime verification and a plan-aware
raw-diff/contract check. The independent code-quality agent and the
verification worker hit an account usage limit; the root completed the
verification above, but **the independent post-fix review did not finish**.
No independent approval or ready-to-merge status is claimed. The anti-pattern
checks above were completed by the root; a separate agent pass was likewise
unavailable. This must remain visible in the next handoff.

Sanitizers stay at the documented end-of-completed-branch-series gate:
ASan+UBSan three full runs; TSan three full runs with `--jobs 2`. They were
not discharged by these normal runs, nor deferred to CI in lieu of local
execution. The protected certification plan remains separate and untouched;
its SHA256 still is
`fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135`.

## Recommended next steps and git cadence

1. Finish independent raw-diff review of Phase 1, including these fixes.
   Address any findings and run the normal full suite after a fix round.
2. After human review and explicit authorization of that git operation,
   make a local Phase 1 commit containing implementation, tests and current
   relevant documentation. Leave the protected certification plan out.
3. Write/review the Phase 2 child plan (exact decimal core and independent
   oracle) against the reviewed Phase 1 state, then execute only after its
   own approval. Continue the same plan → tests → implementation → review
   cycle, with a local commit at each reviewed phase boundary when authorized.
4. Keep one integration fix branch for the type-correctness work. Push/open
   the final PR only after the coherent roadmap milestone, integration/
   migration/application acceptance and final local sanitizer gates are
   complete, and the user explicitly authorizes those git operations.
   This gives phase-sized commits inside one integrated branch/PR.
5. The durability certification plan runs only after this work lands.

Commit checkpoints are a recommendation, not permission to commit now.
Phase 1 prevents unsafe conversions; the finance application remains blocked
until exact storage, writes, queries/indexes, calculations, migration and
application acceptance are green together. Do not equate this Phase 1 result
with the full numeric milestone.


## Independent post-fix review follow-up

The fresh independent raw-diff and specification reviews have now completed;
the earlier worker-limit note above is historical. See
[the raw-diff review](2026-10-22-phase1-raw-diff-review.md). Functional findings
were zero on both passes. One P3 standards cleanup remains: remove task/phase/
review identifiers from newly added source comments. No source was edited
during the review, so the recorded green source identity is unchanged.


## Comment cleanup follow-up

The remaining P3 comment-style finding was resolved after the human requested
cleanup and a local commit. All five edited source files retain exactly the
same non-comment tokens. Earlier source hashes identify the pre-cleanup test
snapshot and should not be mistaken for the final comment-cleaned file hashes.
Final verification is in `2026-10-22-phase1-cleanup-and-commit.md`. Raw logs,
debug probes, manifests and scratch files stay local, excluded from the commit.
