# Phase 3B.7 — checked packed JSON cells and projected GET: execution evidence

Curated actual transcript of Revision 2 execution (Tasks 0–3). Raw unfiltered
logs, probe sources, scratch objects and binaries live only under `/tmp` (paths
below); nothing raw is committed or staged. All commands were run with the
`rtk proxy` prefix exactly as Appendix A orders them. No deviations: the run
completed with zero mismatches, so no `PLAN_NOTES.md` exists.

## Identity

| Item | Value |
|---|---|
| Start HEAD | `decbee6a18fc950bbf18e2f4ce6ad68956cad554` (verified = required base; ancestor/HEAD checks passed inside every driver stage) |
| End HEAD | `decbee6a18fc950bbf18e2f4ce6ad68956cad554` (no git writes, nothing staged — `git diff --cached` empty) |
| Branch | `fix/phase1-safe-scalar-conversions` (start = end) |
| Approved plan | `docs/plans/2026-10-26-phase3b7-packed-json-projection-plan.md` |
| Plan SHA256 | `1f45c320abe0e890ae7011bdbc93646bbe8e00733be1cecaf92e310fc72c24ae` |
| Protected rig | `docs/plans/2026-10-19-durability-certification-rig.md` |
| Protected SHA256 | `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` — verified at start, revalidated by every driver stage preflight, and explicitly re-hashed at the end (bytes hashed only; contents never read, never copied). The separate checks.py probes do not hash this file. |
| Executor / compiler | GLM 5.3; gcc (GCC) 16.2.1 20260810, Linux x86_64 |
| Bootstrap dir | `/tmp/p3b7-exec-m00vfm35` (29 blocks extracted verbatim by the plan's bootstrap block; extracted bootstrap itself hashed `0dd7b108…f437a` = manifest block hash; `driver.py` = `b7fca19b…f6eb`, `checks.py` = `0d7e8453…6153`, both equal their manifest hashes; `projection-probe` = `eb8332f8…b191`) |

Pre-existing planning artifacts inventoried and preserved untouched:
`docs/plans/2026-10-23-phase3-packed-records-plan.md` (modified before this
run), `docs/plans/2026-10-26-phase3b7-packed-json-projection-plan-review.md`,
the plan itself, and the protected rig file. Nothing added to the index at any
point. Task 0 read AGENTS.md/CORE-PROCESS.md, the Phase 3 work index, the
parent recovery plan, the reviewed 3B.6 plan/evidence/raw-diff/plan reviews,
typed-records.md, typed-row-adapter.md, whole-record-readers.md,
packed-record-codec.md and query-protocol/overview.md/find.md; the protected
rig's contents were never opened.

## Commands executed (Appendix A order)

```
python3 /tmp/p3b7-logs/bootstrap.py                     # executes the plan's bootstrap block verbatim -> /tmp/p3b7-exec-m00vfm35
driver.py --stage 1 --check-only                        # "stage 1 preflight verified: 4 files; no writes"
driver.py --stage 1                                     # "stage 1 verified: 4 files"
SKIP_TESTS=1 ./build.sh                                 # exit 0, zero warnings (log: /tmp/p3b7-logs/stage1-build.log)
shard-db-test run test-packed-json-projection           # exit 176 — RED (log: /tmp/p3b7-logs/stage1-red.log)
checks.py --mode red                                    # wrapper exit 0 (probe exit 1 = expected RED; log: /tmp/p3b7-logs/probe-red.log)
driver.py --stage 2                                     # "stage 2 verified: 3 files"
SKIP_TESTS=1 ./build.sh                                 # exit 0, zero warnings (log: /tmp/p3b7-logs/stage2-build.log)
shard-db-test run test-packed-json-projection           # exit 0 — GREEN (log: /tmp/p3b7-logs/stage2-green.log)
checks.py --mode green                                  # exit 0, 122 PASS / 0 FAIL (log: /tmp/p3b7-logs/probe-green.log)
checks.py --mode regression-proof                       # exit 0, "scratch revert/reapply proof passed; checkout source and binaries untouched" (log: /tmp/p3b7-logs/proof.log)
driver.py --stage 3                                     # "stage 3 verified: 3 files"
shard-db-test run-all                                   # exit 0 (log: /tmp/p3b7-logs/full.log)
checks.py --mode green                                  # exit 0, repeat, 122 PASS / 0 FAIL (log: /tmp/p3b7-logs/probe-green-final.log)
rtk git diff --check                                    # exit 0, clean
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md  # exact expected hash again
```

Stage 2 was applied only after the RED exit 176 and the 22/20 red probe were
confirmed. Raw logs: `/tmp/p3b7-logs/{bootstrap,stage1-check,stage1-apply,
stage1-build,stage1-red,probe-red,stage2-apply,stage2-build,stage2-green,
probe-green,proof,stage3-apply,full,probe-green-final,diff-check,
protected-final}.log` plus `/tmp/p3b7-logs/bootstrap.py` (extraction helper
that reproduces the plan's bootstrap block byte-exactly, hash-verified against
the manifest before execution).

## RED (mandatory, before implementation)

Focused `test-packed-json-projection` against unchanged projection/GET code
(with the stage-1 ENOTSUP link stubs and the still-unfixed shared guard):
**40 passed / 176 failed, exit 176**. The 176 failures carry 128 assertion
signatures in the intended missing-behavior classes: expected projected
cells/objects absent because the stubs return NULL/ENOTSUP (46×
`out&&!strcmp(out,expected)`, 1× full-decimal digit survival), guard/argument
validation not yet implemented (29× expected EINVAL, 17×
`typed_row_open` fixed logical-width/enum metadata mismatch rejection, 14×
expected EILSEQ corruption/generation refusal, 1× expected ENOTSUP composite
refusal), budget enforcement absent (18× expected ENOMEM one-short rejections,
2× expected E2BIG selector limits). The legacy numeric/bool/composite/unknown
controls (`p_legacy_controls`) passed on both sides and are controls, not
evidence of implemented packed behavior; incidental stub-input passes are not
counted as implemented validation.

Checks `--mode red` compiled the actual `typed_row.c`/`query_find.c`/
`packed_record.c`/`decnum.c` with scratch-only `p7_*` malloc/calloc/realloc/
free substitutions and the source-extracted `cmd_get_fields` body with local
schema/registry/read mocks (scratch `/tmp/p3b7-check-2u6zjiqg`; no production
seam): probe exit 1, exactly **22 passing / 20 failing gates** — the planned
defect classes: packed cell/rows/named cells missing with empty allocation
sweeps (proving the new paths are absent, not OOM safety), projected GET
wrong output with no sweep, composite/generation-mismatch/unselected-corrupt/
fixed-schema-mismatch/overlong requests not refusing, partial-write/newline/
readonly FILE failures not detected, max-schema and unselected-4MiB bounds
absent. Setup failures would exit 2; none occurred.

## GREEN (stage 2)

- `test-packed-json-projection`: **218 passed / 0 failed, exit 0**.
- Checks `--mode green` (scratch `/tmp/p3b7-check-ep6s6moe`): **122 PASS /
  0 FAIL**, `probe failures=0`. Allocation injections **31** — direct cell 6,
  named `json_projected_field` 6, rows `json_typed_row_cell` 6, unknown-name
  4, projected command 9 — every injected failure returned NULL/ENOMEM or one
  explicit command error, freed all tracked buffers (`live==0`) and released
  the mocked registry reference exactly once. The command sweep asserted the
  exact error body `{"error":"out of memory"}` with no success prefix at every
  one of its 9 injection points. Output-failure fixtures: short/partial
  accepted write, failing final newline, read-only FILE — each returned
  failure, released the registry ref once, and never appended a replacement
  error after an accepted I/O prefix. Generation mismatch reported exactly
  `{"error":"corrupt record"}`; composite reported exactly
  `{"error":"packed composite projections await integration"}`; overlong
  selection reported exactly `{"error":"projection exceeds limit"}`.
- Maximum-schema projection: row **actual 2085**, largest selected allocation
  **30**, `live=0` despite the >1 GiB declared ceiling. Unselected 4 MiB
  varchar: row **actual 4194339**, largest selected allocation **15**,
  `live=0` — no declared-schema or whole-JSON-row allocation. The fetched raw
  record is still allocated by the real (mocked) reader; the probe excludes
  that plain malloc from selected-allocation accounting. Decimal validation
  workspaces remain bounded and can allocate for unselected numerics.

## Scratch-only revert/reapply proof (CORE-PROCESS)

`checks.py --mode regression-proof`, all in `/tmp/p3b7-check-miom3tbf`;
checked-out sources and build products never modified:

- Copied the fixed `typed_row.c`/`query_find.c`/`storage.c` (hash-verified
  equal to checkpoint 2 first), reversed the exact stage-2 hunks in scratch,
  verified byte-identical checkpoint-1 hashes (for `query_find.c`/
  `storage.c` the committed baseline; for `typed_row.c` checkpoint 1
  including the deliberate stage-1 link stubs), compiled and ran: **22 PASS /
  20 FAIL**, reproducing exactly the original 20 defect gates. Reapplied the
  hunks, verified hash equality with checkpoint 2, recompiled: **122 PASS /
  0 FAIL**.

## Stage 3 and final verification

- `driver.py --stage 3`: `stage 3 verified: 3 files` (new
  `docs/concepts/packed-json-projection.md`; link corrections in
  `typed-row-adapter.md` and `whole-record-readers.md`).
- Fresh full suite after docs: **20263 passed / 0 failed across 525 cases,
  exit 0** (includes the new 218/0 case).
- Checks `--mode green` repeated on the final tree (scratch
  `/tmp/p3b7-check-t_exakl0`): **122 PASS / 0 FAIL**, same 6+6+6+4+9=31
  injections and 2085/30 and 4194339/15 `live=0` numbers.
- `rtk git diff --check`: exit 0, clean. Diff artifact scan: no TODO/FIXME/
  debug print/probe/cache additions (the single `snprintf(err,cap,…)` grep hit
  is pre-existing `row_error()` context, not an added line).
- Both builds (after stage 1 and stage 2) produced zero compiler warnings
  (`grep -ci warning` = 0 in both build logs).

### Final owned-file hashes (all nine equal plan checkpoint 3)

| File | SHA256 |
|---|---|
| `build.sh` | `238623e52b87932e2302d75eb6a50e6b7e3ab15f89837b477db62f491a1e807e` |
| `docs/concepts/packed-json-projection.md` | `8de72ed51afb9425e813591e0b649501ff470656dc70fbe704a786be313b5a62` |
| `docs/concepts/typed-row-adapter.md` | `736ba37fda8ac618815989e0eb452d859422885e8c147f89b56d36938ee45a6a` |
| `docs/concepts/whole-record-readers.md` | `dcba70251ac6366042ccf6075f5342dbfda999243d22f667a31adb27127f00f0` |
| `src/db/query_find.c` | `8916f6d3e43bd733e5e0d81f13ea91fe12840e67fb4daf563bdbfd6c7546e1bc` |
| `src/db/storage.c` | `05edc75ef31d6e53e214a5a389e1ce64cb26280df491842d32808155035397c7` |
| `src/db/typed_row.c` | `71d5e928af46a46fe304bf8f34b87831d0f465c758971699a98bf06c154411b1` |
| `src/db/typed_row.h` | `c74a59fd1c9b6d7fb3158d94d013ce5fcf5c3109c648e3be6c47299f51b5a602` |
| `src/test/cases/test_packed_json_projection.c` | `7b490aa0fa10e00a96362819f40e78b9deb0853afadab78e3b4d11aca17300fe` |

Diff scope: the seven modified owned files above plus two new files
(`docs/concepts/packed-json-projection.md`,
`src/test/cases/test_packed_json_projection.c`). The only other working-tree
changes are the pre-existing modification to
`docs/plans/2026-10-23-phase3-packed-records-plan.md` and the pre-existing
untracked planning/review files, all present before this run.

## Binder census (verified at hand-back)

- `typed_schema_bind_packed`: production references are only its definition
  (`src/db/typed_row.c`) and declaration (`src/db/typed_row.h`) — no loader
  or `create-object` caller exists; 10 references are test-fixture usage.
  No default-format, schema-version, migration or activation edit appeared;
  packed storage remains publicly inactive.
- The packed branches in `json_projected_field`/`json_typed_row_cell` and
  `cmd_get_fields` trigger only on `ts->packed.nfields` (an explicitly bound
  schema); every legacy branch is byte-identical to baseline, and shared
  adapters/GET pass `SIZE_MAX`, preserving the uncapped output policy.

## Deviations

None. Every anchor matched exactly; the driver verified all stage preflights
and post-write checkpoints; all 29 manifest block hashes matched; and every
predicted number (40/176 exit 176, 22/20 probe, 218/0, 122/0, 31 injections,
2085/30 and 4194339/15 live0, reverse 22/20 then reapply 122/0, 20263/0
across 525) reproduced.

## Hand-back state

Work is stopped **uncommitted and unstaged** for the plan-aware and
independent blind raw-diff reviews. No sanitizers, benchmarks, staging,
commits, pushes, PRs, public packed activation, later-phase work, or
protected-rig implementation was performed. Branch-series sanitizer gates
(ASan+UBSan and TSan, three runs each) remain deferred to the authorized
pre-push window per AGENTS.md once all branch fixes are done. Remaining scope
is unchanged from the plan's "Following work": raw-field/CSV/composite
contracts, caller error propagation, packed-safe scan/projection envelopes,
remaining read/index/delete/maintenance consumers, 3C metadata/publication,
Phases 4–9, then the protected certification rig.
