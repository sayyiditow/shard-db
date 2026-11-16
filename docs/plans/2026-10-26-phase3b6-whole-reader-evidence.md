# Phase 3B.6 — schema-aware whole-record JSON readers: execution evidence

Curated actual transcript of Revision 2 execution. Raw unfiltered logs, probe
sources, scratch objects and binaries live only under `/tmp` (paths below);
nothing raw is committed or staged. All commands were run with the `rtk proxy`
prefix exactly as Appendix A orders them. No deviations: the run completed
with zero mismatches, so no `PLAN_NOTES.md` exists.

## Identity

| Item | Value |
|---|---|
| Start HEAD | `35e49ab8cd7a7efe7350562e34eaeac62ea4afae` (verified = required base; ancestor check passed inside the driver) |
| End HEAD | `35e49ab8cd7a7efe7350562e34eaeac62ea4afae` (no git writes, nothing staged) |
| Branch | `fix/phase1-safe-scalar-conversions` (start = end) |
| Approved plan | `docs/plans/2026-10-26-phase3b6-whole-record-readers-plan.md` |
| Plan SHA256 | `770505911d06179f19276b952ae7f94ada52c0e06df910ae7a3034d27f20fed9` |
| Protected rig | `docs/plans/2026-10-19-durability-certification-rig.md` |
| Protected SHA256 | `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` — verified at start, revalidated by every driver stage preflight, and explicitly re-hashed at the end (bytes hashed only; contents never read, never copied) |
| Executor / compiler | GLM 5.3; gcc (GCC) 16.2.1 20260810, Linux x86_64 |
| Bootstrap dir | `/tmp/p3b6-exec-bhs5wtrc` (23 blocks extracted verbatim by the plan's bootstrap block; `driver.py` = `49af0525…`, `checks.py` = `3cb81bf0…`, both equal their manifest hashes) |

Pre-existing planning artifacts inventoried and preserved untouched:
`docs/plans/2026-10-23-phase3-packed-records-plan.md` (modified before this
run), `docs/plans/2026-10-26-phase3b6-plan-review.md`, the plan itself, and
the protected rig file. Nothing added to the index at any point.

## Commands executed (Appendix A order)

```
python3 /tmp/p3b6-bootstrap.py                          # executes the plan's bootstrap block verbatim -> /tmp/p3b6-exec-bhs5wtrc
driver.py --stage 1 --check-only                        # "stage 1 preflight verified: 2 files; no writes"
driver.py --stage 1                                     # "stage 1 verified: 2 files"
SKIP_TESTS=1 ./build.sh                                 # exit 0, zero warnings (log: /tmp/p3b6-exec-stage1-build.log)
shard-db-test run test-whole-record-readers             # exit 38 — RED (log: /tmp/p3b6-exec-stage1-red.log)
checks.py --mode red                                    # wrapper exit 0 (probe exit 1 = expected RED; log: /tmp/p3b6-exec-probe-red.log)
driver.py --stage 2                                     # "stage 2 verified: 4 files"
SKIP_TESTS=1 ./build.sh                                 # exit 0, zero warnings (log: /tmp/p3b6-exec-stage2-build.log)
shard-db-test run test-whole-record-readers             # exit 0 — GREEN (log: /tmp/p3b6-exec-stage2-green.log)
checks.py --mode green                                  # exit 0, all 106 gates PASS (log: /tmp/p3b6-exec-probe-green.log)
checks.py --mode regression-proof                       # exit 0, "scratch revert/reapply proof passed; checkout source and binaries untouched" (log: /tmp/p3b6-exec-proof.log)
driver.py --stage 3                                     # "stage 3 verified: 3 files"
shard-db-test run-all                                   # exit 0 (log: /tmp/p3b6-exec-full.log)
checks.py --mode green                                  # exit 0, repeat, all 106 gates PASS (log: /tmp/p3b6-exec-probe-green-final.log)
rtk git diff --check                                    # exit 0, clean
rtk proxy sha256sum docs/plans/2026-10-19-durability-certification-rig.md  # exact expected hash again
```

Stage 2 was applied only after the RED exit 38 and the 16-gate red probe were
confirmed. Raw logs: `/tmp/p3b6-exec-{stage1-apply,stage1-build,stage1-red,
probe-red,stage2-apply,stage2-build,stage2-green,probe-green,proof,stage3-apply,
full,probe-green-final}.log` plus `/tmp/p3b6-bootstrap.py`.

## RED (mandatory, before implementation)

Focused `test-whole-record-readers` against unchanged readers: **124 passed /
38 failed, exit 38**. Failure signatures are the intended missing behaviors:
valid packed-generation records are rejected by the legacy-only allocating
and FILE decoders (including `expected 0 got -1` for a valid packed stream).
Conversely, a packed schema with legacy generation flags is accepted and its
directory bytes are interpreted as fixed scalar fields, with errno remaining
zero (`expected 84 got 0` and `expected 'PREFIX' got
'PREFIX{"f0":76561193668182016,…}'`). NULL input is misclassified as
corruption (`expected 22 got 84` = EINVAL vs EILSEQ). The legacy stream loop
also omits the long enum and accepts malformed legacy field metadata while
emitting success-shaped JSON. Invalid-flag/descriptor rejection assertions that
already pass on baseline are controls, not evidence of implemented packed reads. Baseline rejection passes were not
counted as evidence of implemented packed behavior.

Checks `--mode red` compiles the actual `config.c`/`typed_row.c`/
`packed_record.c`/`decnum.c` with scratch-only `p6_*` malloc/calloc/realloc/
free substitutions and the source-extracted `cmd_get` body with local
read/schema/registry mocks (scratch `/tmp/p3b6-check-mqd1hbaw`; no production
seam): probe exit 1, **probe failures=16**, exactly the four planned classes —
legacy omission/OOM (6), packed-read (6), GET output/mismatch (3),
maximum-schema (1). Setup failures exit 2; none occurred.

## GREEN (stage 2)

- `test-whole-record-readers`: **167 passed / 0 failed, exit 0**.
- Checks `--mode green` (scratch `/tmp/p3b6-check-5etxwl7s`): **all 106 gates
  PASS, 0 FAIL**, `probe failures=0`. Allocation injections **22** —
  legacy direct 3, legacy GET 3, packed direct 8, packed GET 8 — each
  signalled (rc −1 / GET rc 1, errno ENOMEM) with no partial or false-success
  output; `selected buffers released` (live=0) and `registry reference
  released once` on every mocked GET path. Every successful path asserted its
  allocation sweep nonempty and normal packed GET matched its complete JSON
  body including newline before the newline-failure injection. Maximum-schema
  row **actual 2085**, largest selected allocation **2966** (< 4096 despite
  the >1 GiB declared ceiling), live=0 — matches the plan's predicted numbers.
- Output-failure fixtures: read-only FILE (`ferror` set, error returned),
  short/partial accepted write, and newline failure — no replacement error
  object appended after an accepted I/O prefix in any of them; generation
  mismatch reports exactly `{"error":"corrupt record"}`.

## Scratch-only revert/reapply proof (CORE-PROCESS)

`checks.py --mode regression-proof`, all in `/tmp/p3b6-check-xv5bu6vh`;
checked-out sources and build products never modified:

- Copied fixed `config.c` and `storage.c` (hash-verified equal to checkpoint 2
  first), reversed the exact stage-2 hunks in scratch, verified byte-identical
  committed baseline hashes, compiled and ran: **probe failures=16** (same
  baseline defect gates). Reapplied the hunks, verified hash equality with
  checkpoint 2, recompiled: **all 106 gates pass, probe failures=0**.

## Stage 3 and final verification

- `driver.py --stage 3`: `stage 3 verified: 3 files` (new
  `docs/concepts/whole-record-readers.md`; clarification hunks in
  `typed-row-adapter.md` and `typed-records.md`).
- Fresh full suite after docs: **20045 passed / 0 failed across 524 cases,
  exit 0** (includes the new 167/0 case).
- Checks `--mode green` repeated on the final tree: **106 PASS / 0 FAIL**,
  same 3+3+8+8 injections and 2085/2966/live=0 numbers.
- `rtk git diff --check`: exit 0, clean. Diff artifact scan: no TODO/FIXME/
  debug/print/probe/cache additions.
- Both builds (after stage 1 and stage 2) produced zero warnings.

### Final owned-file hashes (all nine equal plan checkpoint 3)

| File | SHA256 |
|---|---|
| `build.sh` | `488af11a8be07f866004d55e86f962526a02af11adb2015b507e18afec1cc1f0` |
| `docs/concepts/typed-records.md` | `1d4670538b63e5df02f425974c874653993a492b448777f5824b68966b48a6d5` |
| `docs/concepts/typed-row-adapter.md` | `4020adc7a8295014702e6288994d9d643be86e4faa2f089d2a35b14556b03f21` |
| `docs/concepts/whole-record-readers.md` | `62ecd2bc4c448d43ad1dfde5afac6170c00ddfab6f6d97f4d1d364b02fa34f22` |
| `src/db/config.c` | `6d17d8f4605a3d3c9fa497656eb24da64e9c4a2e650424a1bd1aa44663438f6a` |
| `src/db/storage.c` | `3abc4fb41ea4796184f15d7bf704b727bbe9f59d849c4b372a01d18d6bd287ab` |
| `src/db/typed_row.h` | `389b9d9b13e4c96f620c2ce851a29f1985e79781c6df0726604ec4ebd6131da7` |
| `src/db/types.h` | `3814b2897e40065c321cbfd78c9c0ee504ca9d2510e6cfd3e62f979257512361` |
| `src/test/cases/test_whole_record_readers.c` | `34ead3ccfcfb4a8f1b1c63ca733c35c22518d4173e8ee682765d626bc6d892ff` |

Diff scope: the seven modified owned files above plus two new files
(`docs/concepts/whole-record-readers.md`, `src/test/cases/test_whole_record_readers.c`).
The only other working-tree change is the pre-existing modification to
`docs/plans/2026-10-23-phase3-packed-records-plan.md`, present before this run.

## Source caller census (verified at hand-back)

- `typed_schema_bind_packed`: production call sites — none (definition in
  `src/db/typed_row.c`, declaration in `typed_row.h` only). Public loaders
  and `create-object` still never bind packed schemas; no public packed
  binding/default-format switch appeared in the diff. Packed storage remains
  publicly inactive.
- `typed_decode`: existing callers (`query_find.c` decode_value/v2_fetch_cb;
  `storage.c` single_write_decode, update dry response, delete response,
  multi_get_fetch_cb) unchanged — they gain checked packed dispatch only
  through the shared entry, per the approved bridge.
- `typed_decode_stream`: sole production caller remains `storage.c:cmd_get`.
- `typed_record_view` and `typed_get_field_str` untouched: raw-offset and
  strlen-based field routes keep their legacy contracts.

## Deviations

None. Every anchor matched exactly; the driver verified all stage preflights
and post-write checkpoints; all 22 manifest block hashes matched; and every
predicted number (124/38 exit 38, 16 red gates, 167/0, 106 gates, 22
injections, 2085/2966/live0, 16-then-106 proof, 20045/0 across 524) reproduced.

## Hand-back state

Work is stopped **uncommitted and unstaged** for the plan-aware and
independent blind raw-diff reviews. No sanitizers, benchmarks, staging,
commits, pushes, PRs, public packed activation, projection/field work, or
protected-rig implementation was performed. Branch-series sanitizer gates
remain deferred to the authorized pre-push window per AGENTS.md. Remaining
scope is unchanged from the plan's "Following work": length-aware
field/projection/rows/CSV child, remaining read/index/delete/matching
consumers, 3C metadata/publication, Phases 4–9, then the protected
certification rig.

## Raw-diff review clarification

The reviewer corrected two evidence-only statements: nine owned files comprise
seven modifications plus two new files, and the RED narrative now distinguishes
rejection of valid packed records from acceptance of a packed schema with legacy
flags. Invalid flag/descriptor controls are not presented as failing baseline
cases. Actual logs, test counts and every approved implementation hash remain
unchanged. The pre-existing parent-plan edit is outside the nine-file inventory.
