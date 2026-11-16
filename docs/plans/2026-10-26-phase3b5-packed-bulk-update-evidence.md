# Phase 3B.5 — packed keyed bulk partial updates: execution evidence

Curated actual transcript of Revision 2 execution. Raw unfiltered logs, probe
sources, scratch objects and binaries live only under `/tmp` (paths below);
nothing raw is committed or staged. All commands were run with the `rtk proxy`
prefix exactly as Appendix A orders them. No deviations: the run completed
with zero mismatches, so no `PLAN_NOTES.md` exists.

## Identity

| Item | Value |
|---|---|
| Start HEAD | `03617a898d5dff291c539b3ab33522054fd36cfb` (verified = required base) |
| End HEAD | `03617a898d5dff291c539b3ab33522054fd36cfb` (no git writes, nothing staged) |
| Branch | `fix/phase1-safe-scalar-conversions` (start = end) |
| Approved plan | `docs/plans/2026-10-26-phase3b5-packed-bulk-update-plan.md` |
| Plan SHA256 | `a49a1d0b02444ab0ac8e600d8c3f3bc68ece196960f229c91550fc026538871b` |
| Protected rig | `docs/plans/2026-10-19-durability-certification-rig.md` |
| Protected SHA256 | `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` (bytes hashed only; contents never read, never copied) |
| Executor / compiler | GLM 5.3; gcc (GCC) 16.2.1 20260810, Linux x86_64 |
| Bootstrap dir | `/tmp/p3b5-exec-drth1by6` (blocks extracted verbatim; no hand transcription) |

Pre-existing planning artifacts inventoried and preserved untouched:
`docs/plans/2026-10-23-phase3-packed-records-plan.md` (modified before this
run), `docs/plans/2026-10-26-phase3b5-plan-review.md`, and the protected rig
file. Nothing added to the index at any point.

## Commands executed (Appendix A order)

```
python3 /tmp/p3b5-run-bootstrap.py                       # executes the plan's bootstrap block verbatim -> /tmp/p3b5-exec-drth1by6
driver.py --stage 1 --check-only                         # "stage 1 preflight verified: 4 files; no writes"
driver.py --stage 1                                      # "stage 1 verified: 4 files"
SKIP_TESTS=1 ./build.sh                                  # exit 0, zero warnings (log: /tmp/p3b5-exec-stage1-build.log)
shard-db-test run test-typed-bulk-update                 # exit 35 — RED
checks.py --mode red                                     # exit 0 (probe exit 1 = expected RED)
driver.py --stage 2                                      # "stage 2 verified: 2 files"
SKIP_TESTS=1 ./build.sh                                  # exit 0, zero warnings
shard-db-test run test-typed-bulk-update                 # exit 0 — GREEN
shard-db-test run test-typed-bulk-insert                 # exit 0
checks.py --mode green                                   # exit 0, "all Phase 3B.5 checks passed"
checks.py --mode regression-proof                        # exit 0, "checkout untouched"
driver.py --stage 3                                      # "stage 3 verified: 3 files"
shard-db-test run-all                                    # exit 0
checks.py --mode green                                   # exit 0, repeat, "all Phase 3B.5 checks passed"
rtk git diff --check                                     # exit 0, clean
```

Key raw logs: `/tmp/p3b5-exec-{bootstrap,stage1-check,stage1-apply,stage1-build,
red-test,checks-red,stage2-apply,stage2-build,green-test,insert-test,
checks-green-1,checks-regproof,stage3-apply,full-suite,checks-green-2}.log`.

## RED (mandatory, before implementation)

Focused `test-typed-bulk-update` against ENOTSUP stubs with query_bulk
untouched: **200 passed / 35 failed, exit 35**. All 35 failures are the
intended missing behaviors (partial dict/array outcomes, matched/updated/
skipped/error distinction, explicit NULL, embedded NUL, positional blanks,
file/inline source, max-schema and 4 MiB grow/shrink, concurrent disjoint
patches). Incidental stub-rejection passes were not counted as evidence.

Checks `--mode red` (compiles the actual `src/db/query_bulk.c` with
scratch-only malloc/calloc substitution, no production seam):
`legacy/criteria: 0 passed, 5 failed` — cases 0–3 (three request-copy loops +
heap cell copy) exited with **signal 11**, case 4 (packed criteria refusal)
exited 1. No crash outside the deliberately isolated reproductions.

## GREEN (stage 2)

- `test-typed-bulk-update`: **235 passed / 0 failed, exit 0**.
- `test-typed-bulk-insert` (untouched prior regression): **178 passed / 0
  failed, exit 0**.
- Checks `--mode green`:
  - legacy/criteria **5 passed / 0 failed**;
  - allocation-failure probe: **112** isolated failures (dict existing 26 +
    missing 8, array existing 26, positional existing 25, file existing 27),
    each preserving exact OLD/absence, kf totals, segment position, live=0;
  - maximum-schema partial update **actual 2091**, largest selected
    allocation **3072** (< 8 MiB guard), live=0 — matches the plan's
    predicted numbers;
  - standalone partial-outcome/duplicate probe: **19/19 gates PASS**
    (`gate failures=0`) — one pre-marker link failure in the first window,
    `rc=1`, `matched:2 updated:1 skipped:1`, keyed error for `a` only,
    partial-outcome warning, independent reopen proves `a`=OLD / `b`=NEW,
    totals preserved; dictionary (escaped `\u0061`) and delimited duplicate
    requests rejected with exact OLD/counts/position preserved.

## Scratch-only temporary-revert/reapply proof (CORE-PROCESS)

`checks.py --mode regression-proof`, all in `/tmp/p3b5-check-ixk68ulu`;
checked-out sources and build products never modified:

- Reversed the seven frozen query hunks in a scratch copy; hash verified
  byte-identical to the committed baseline (`e3d8b998…`), compiled and run:
  `legacy/criteria: 0 passed, 5 failed` with four signal-11 children.
  Reapplied: hash verified equal to checkpoint 2 (`c619ab1d…`), run:
  **5 passed / 0 failed**.
- Reversed the four exact review-fix hunks in a scratch copy of the bulk
  module, linked its object ahead of the static library: the partial probe
  failed with exactly the **10 expected defect gates** (false
  `updated:0 skipped:2`, row `b` wrongly errored, missing partial-outcome
  warning; both duplicate-screening defect classes in dict and delimited
  forms). Reapplied: hash verified equal to checkpoint 2 (`90ab9519…`),
  recompiled: **19/19 gates pass, failures=0**.

## Stage 3 and final verification

- `driver.py --stage 3`: `stage 3 verified: 3 files` (new
  `docs/concepts/packed-bulk-update.md`; link/wording updates in
  `packed-bulk-insert.md` and `typed-row-adapter.md`).
- Fresh full suite after stage 3: **19878 passed / 0 failed across 523
  cases, exit 0** (`test-typed-bulk-insert` 178/0 inside the suite; builds
  and runs include the new `test-typed-bulk-update` case, 235/0).
- Checks `--mode green` repeated: all pass (same 112 / 2091 / 3072 / 19-gate
  numbers), `rtk git diff --check` clean.
- Both builds after stage 1 and stage 2 produced zero warnings.

### Final owned-file hashes (all equal plan checkpoint 3)

| File | SHA256 |
|---|---|
| `build.sh` | `149b27a34602008666a0b1e9d44ee02dff52ab7bd19172a4d5a898370a12b999` |
| `docs/concepts/packed-bulk-insert.md` | `a647f6caaa3bdcb93776283a2e520eadd005c178e1f826ffda3766bd89c40c28` |
| `docs/concepts/packed-bulk-update.md` | `a3ad8b1d8b5d65e066b1e763b45122964eb6766ab5cd5ba230c9301f36e268e8` |
| `docs/concepts/typed-row-adapter.md` | `0a84766a6041f615e76e69403975e002eda99aeb0a22bed34aec74f67bdadf3a` |
| `src/db/query_bulk.c` | `c619ab1d8439f9468b80e7bd12fbc6f3d578b29436cef45d47a568db91f07908` |
| `src/db/typed_bulk_insert.c` | `90ab9519209f1b80459f804ae36221faf26a26a478d92a809fccb1cb0fa8833b` |
| `src/db/typed_bulk_insert.h` | `865f3b5667448e4341445169c5c34086718865b5179b873d9a1e801ad4258e5d` |
| `src/test/cases/test_typed_bulk_update.c` | `a9480ccbaf4e32bea9d231f58bccf05381ac7e867742c90610eaea532b1195b0` |

## Source caller census (verified at hand-back)

- `typed_schema_bind_packed`: production call sites — none (definition in
  `src/db/typed_row.c`, declaration in `typed_row.h` only). Public loaders
  and `create-object` still never bind packed schemas; no public packed
  binding/default-schema switch appeared in the diff.
- `typed_bulk_update_command`: declared `typed_bulk_insert.h:17`, defined
  `typed_bulk_insert.c:332`; only query-layer caller is
  `src/db/query_bulk.c:4378` (early packed delimited route). The source
  wrapper also calls it at `typed_bulk_insert.c:343` (inline) and `:359`
  (file), and the private tests call it directly.
- `typed_bulk_update_source`: declared `typed_bulk_insert.h:20`, defined
  `typed_bulk_insert.c:339`; only query-layer caller is
  `src/db/query_bulk.c:5124` (early
  packed branch of the JSON update route, before legacy field/criteria
  setup; shared by cmd_bulk_update_json/_string file+inline forms).
- `typed_bulk_insert_command`: both existing `query_bulk.c` callers
  unchanged; insert response/behavior untouched (178/0 regression).
- Legacy guard hunks: three request-copy loops and the heap-cell `memcpy`
  now NULL-checked (`query-edit-4/5/6/7`), criteria entry guarded before
  parse/compile/scan (`query-edit-3`).

## Deviations

None. Every anchor matched exactly, the driver verified all stage preflights
and post-write checkpoints, and every predicted number (200/35, 0/5 with four
SIGSEGV, 235/0, 178/0, 112, 2091/3072, 19 gates, 10 defect gates, 19878/0
across 523) reproduced.

## Hand-back state

Work is stopped **uncommitted and unstaged** for the plan-aware and
independent blind raw-diff reviews. No sanitizers, benchmarks, staging,
commits, pushes, metadata activation or later-phase work was performed.
Branch-series sanitizer gates remain deferred to the authorized pre-push
window per AGENTS.md.

## Raw-diff review clarification

The reviewer clarified the caller census above to distinguish query-layer
callers from the source wrapper and private tests. This is evidence wording
only; executed code and all eight approved checkpoint hashes are unchanged.
