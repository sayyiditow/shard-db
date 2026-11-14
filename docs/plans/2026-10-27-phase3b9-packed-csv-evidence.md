# Phase 3B.9 execution evidence — 2026-10-27

Executor: GLM 5.3, executing Revision 2 of
[2026-10-27-phase3b9-packed-csv-rendering-plan.md](2026-10-27-phase3b9-packed-csv-rendering-plan.md)
(Planner/reviewer: GPT 6.1 Sol). All commands were run exactly as Appendix A
specifies, via `rtk` / `rtk proxy`, in the repository root. Raw logs, probe
sources, extracted tools and binaries stay under `/tmp/p3b9-exec-r924yji5/`
(`P3B9_WORK`) and `/tmp/p3b9-check-*/` scratch dirs — never version control.

## Identity

| Item | Value |
|---|---|
| Branch | `fix/phase1-safe-scalar-conversions` |
| Base HEAD (start and end) | `994e2eb43497721e34cd4aecf0159a00e42c226a` |
| Approved plan SHA256 | `a71db6d07f2e4de71263c1a5f57b53c8e75c53ea5bc18b1c174822fcf09cfa89` |
| Protected rig SHA256 | `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` (hash-only access; never read, copied, edited, moved or staged) |
| Staged diff at start | empty (`git diff --cached --name-only` → no output) |
| Pre-existing artifacts | `M docs/plans/2026-10-23-phase3-packed-records-plan.md` (parent work index, modified before this run), untracked protected rig + 3B.9 plan + 3B.9 review |
| Socket preflight | local TCP bind/connect OK (port 52241) before any suite work |
| Bootstrap | `P3B9_WORK=/tmp/p3b9-exec-r924yji5`; bootstrap asserted the block inventory and verified all 27 manifest block hashes (28 extracted blocks including MANIFEST) before emitting DRIVER/CHECKS |

Documentation discovery (read-only agent + direct reads) covered the work
index, parent recovery plan, 3B.8 plan/evidence/reviews, concepts
(typed-row-adapter, packed-named-projection, packed-json-projection,
typed-records) and query-protocol (overview, find, joins). It confirmed every
Task 0 claim: legacy CSV normalizes CR/LF to spaces (`find.md` line 103,
`csv_emit_cell` at `src/db/query_find.c:1727`), the wire frame terminates at
raw NUL (`overview.md` lines 45–51), named display semantics include '+'
composites and exact raw decimal text, and the new APIs
(`typed_csv_cell_text`, `typed_packed_csv_header`, `typed_packed_csv_row`,
`pkr_schema_validate`) have zero production callers. Cited symbol locations
matched the plan's table (`query_find.c` 1707/1748/1771, `query_join.c`
492/522, codec `schema_ok` at `src/db/packed_record.c:116`, adapter
`packed_schema_ok` in `src/db/typed_row.c`, and `pkr_schema_validate` absent
from `src/` at the pre-execution checkpoint). No contradictions; no `PLAN_NOTES.md` needed.

## Task 1 — RED (captured before any renderer implementation)

| Command | Exit | Key raw output |
|---|---|---|
| `driver.py --stage 1 --check-only` | 0 | `stage 1 preflight verified: 6 files; no writes` |
| `driver.py --stage 1` | 0 | `stage 1 verified: 6 files` (postimage hashes asserted by driver) |
| `env SKIP_TESTS=1 ./build.sh` | 0 | full build completed; `grep -ci warning` → 0 |
| `shard-db-test run test-packed-csv` | **112** | `# test-packed-csv: 32 passed, 112 failed` |
| `checks.py --mode red` | 0 | probe compiled from real sources; `semantics mode=0/1/2 FAIL`, `semantics failed=3` at the probe's expected internal exit 1 (handled by the red wrapper; setup/build failure never accepted) |

RED failure mechanism is semantic, not setup/link: on link stubs every
rendering assertion sees the stub's `NULL`/ENOTSUP — e.g. `not ok 1 - CSV cell
exact / expected '' got '(null)'` and `expected 'k,false0\n' got '(null)'` —
while setup assertions pass incidentally, exactly as the plan predicts. All
RED areas from the plan are covered by the committed case (exact bytes, NUL
vs literal `\u0000`, delimiters/quotes/control bytes, NULL/empty, exact and
one-short budgets, header/row projections, duplicates, composites, limits,
schema/generation/corruption failures, maximum numeric digits, 4 MiB all-NUL
varchar; all 256 input byte values).

## Task 2 — GREEN, oracle, allocation-failure and reversal proofs

| Command | Exit | Key raw output |
|---|---|---|
| `driver.py --stage 2` | 0 | `stage 2 verified: 2 files` |
| `env SKIP_TESTS=1 ./build.sh` | 0 | full build completed; 0 warning lines |
| `shard-db-test run test-packed-csv` | 0 | `# test-packed-csv: 144 passed, 0 failed` |
| `checks.py --mode green` | 0 | gates below |
| `checks.py --mode regression-proof` | 0 | proof below |

Green gates (first green run; identical on the post-docs re-run):

```
semantics failed=0
repair header metadata PASS / repair default identity 0 PASS / repair default identity 1 PASS
repair failures=0
mode=0 injections=1 / mode=1 injections=2 / mode=2 injections=12
OOM injections=15 live=0
maximum actual=2085 largest=9 live=0 PASS
Python CSV/byte oracle: 3035 passed, 0 failed; seed=20261027
```

All 15 allocation injections (cell 1, header 2, row 12) return NULL/ENOMEM
with authoritative length zero and zero live buffers; the maximum-schema
selected row reports actual 2085 / largest single allocation 9 / live 0 —
actual-size, not schema-bound. The independent Python oracle recomputes outer
CSV quoting plus byte-decoder round-trip over 607 analytic/seeded byte samples
× 5 delimiters = 3035 cases.

Regression-proof proof chain (scratch copies only; checkout untouched):

1. Reverse the frozen renderer and codec validator back to link stubs — the
   resulting byte strings hash-match the stage 1 checkpoints exactly
   (`src/db/typed_row.c` = `2628d1f3…`, `src/db/packed_record.c` =
   `d2af06f9…`); the reversed probe reports `semantics failed=3` (exit 1).
2. Compile `CSV_REVIEW_BASELINE` (the exact pre-review renderer, never a
   driver stage) against the current codec entry point — it fails the exact
   repair gates: `repair failures=3` (malformed-header metadata gate + two
   default-identity gates), proving the Revision 2 repairs against
   functioning old code, not merely stubs.
3. Restore the frozen renderer — restored text equals the checked-out source,
   `restored.count(CSV_FULL)==1`, and the full green gate set passes again:
   `scratch reversal/reapply hashes verified; checkout untouched`.

## Task 3 — docs and fresh full suite

| Command | Exit | Key raw output |
|---|---|---|
| `driver.py --stage 3` | 0 | `stage 3 verified: 3 files` |
| `shard-db-test run-all` (fresh, after all edits) | 0 | `1..527` / `# total: 20603 passed, 0 failed across 527 cases` |
| `checks.py --mode green` (re-run) | 0 | all gates identical, incl. oracle `3035 passed, 0 failed; seed=20261027` |
| `git diff --check` | 0 | no output (no whitespace/conflict markers) |
| `sha256sum docs/plans/2026-10-19-durability-certification-rig.md` | 0 | `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` |
| nine-final-hash check vs MANIFEST checkpoint 3 | 0 | all nine `MATCH`; `ALL NINE FINAL FILE HASHES VERIFIED` |

The forecast numbers were met exactly: focused RED 32/112 (exit 112), focused
GREEN 144/0 (exit 0), full suite 20603/0 across 527 cases (exit 0). Both full
builds are warning-free. No TODO/FIXME/debug leftovers in the touched sources.

## Limits and non-claims

- The packed storage path stays inactive: loaders/create-object never bind
  packed schemas; the new CSV APIs have no production caller; legacy CSV/JSON
  behavior is unchanged (boundary hashes for query_find.c, query_join.c,
  query.c, storage.c, query_internal.h, types.h, config.c, index.c,
  query_plan.c, query_aggregate.c, decnum.c are asserted by both driver and
  checks on every invocation).
- No public route acceptance, FILE atomicity, platform coverage or
  performance claim follows from these helper tests. The OOM probe
  instruments exactly `typed_row.c`, `packed_record.c`, `decnum.c`; the
  maximum-allocation metric excludes pre-existing schema/input/record fixture
  storage. Budgets bound output, not total peak; no QUERY_BUFFER_MB
  integration is claimed.
- Ordinary gcc modes only (-O1 in probes, the build's normal flags). No
  sanitizers and no benchmarks in this run; the branch-final ASan+UBSan/TSan
  gates remain immediately before push/PR per AGENTS.md.
- No Git writes: nothing staged, nothing committed, nothing pushed.

## Final inventory

Nine files belong to this child (all hash-matched to checkpoint 3):

- `src/db/packed_record.h` (+3 lines: `pkr_schema_validate` declaration)
- `src/db/packed_record.c` (+4 lines: pure `schema_ok` wrapper)
- `src/db/typed_row.h` (+20 lines: checked CSV API declarations)
- `src/db/typed_row.c` (+163 lines: stubs→full renderers; final state is full)
- `src/test/cases/test_packed_csv.c` (new, 193 lines)
- `build.sh` (test registration line)
- `docs/concepts/packed-csv-rendering.md` (new, 68 lines)
- `docs/concepts/typed-row-adapter.md` (+4 lines link paragraph)
- `docs/concepts/packed-named-projection.md` (+4 lines link paragraph)

Planning artifacts preserved alongside: the approved plan, the plan review,
the parent work index (pre-existing modification, untouched by this run), and
the protected hash-only rig. Work is stopped unstaged and uncommitted for
plan-aware and independent blind raw-diff review.
