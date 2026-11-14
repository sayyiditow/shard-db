# Phase 3B.10 execution evidence — 2026-10-27

Executor: GLM 5.3, executing the approved
[packed CSV commands plan](2026-10-27-phase3b10-packed-csv-commands-plan.md)
(Planner/reviewer: GPT 6.1 Sol). All commands were run exactly as Appendix A
specifies, via `rtk` / `rtk proxy`, in the repository root. Raw logs, the
published driver/checks/probe tools, probe sources and binaries stay under
`/tmp/p3b10-exec-5e2u3zev/` (`P3B10_WORK`, with `logs/` holding one raw log
per Appendix A command) and the four `/tmp/p3b10-check-*` scratch dirs —
never version control.

## Identity

| Item | Value |
|---|---|
| Branch | `fix/phase1-safe-scalar-conversions` (unchanged start→end) |
| Base HEAD (start and end) | `174f7d8e99bee4a619a23aed3dfa07a3cbb05bcf` |
| Approved plan SHA256 | `374b66160925e9c3bb19635452e408980e652ec671be9c6a0e645a99f6a9b343` (verified before bootstrap) |
| Protected rig SHA256 | `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` — verified at start and re-hashed at end; contents never read, copied, edited, moved, staged or executed |
| Staged diff at start and end | empty (`git diff --cached` no output) |
| Pre-existing artifacts at start | `M docs/plans/2026-10-23-phase3-packed-records-plan.md` (work index), untracked protected rig + 3B.10 plan + 3B.10 review — exactly the permitted set |
| Socket preflight | local TCP bind/connect/accept OK (port 54251) before any suite work |
| Bootstrap | `P3B10_WORK=/tmp/p3b10-exec-5e2u3zev`, exit 0; the BOOTSTRAP block asserted the block inventory and verified all 34 manifest block hashes (35 extracted blocks including MANIFEST) before emitting `driver.py` / `checks.py` / `probe_tools.py` |
| Final tree | unstaged and uncommitted; see "Working tree at hand-back" |

Documentation discovery (Task 0) read the work index, 3B.9
plan/evidence/plan-review, `concepts/packed-csv-rendering.md`,
`typed-row-adapter.md`, `concurrency.md` (writer-reservation acquisition,
borrowing and objlock sections), `storage-model.md` and query-protocol
overview/find/joins, plus `slotcask.h` reservation/walk/lookup contracts and
the existing `test_owning_write_hooks.c` fixture pattern. Every Task 0 table
claim matched the sources: strict walk returns −1 on unreadable/invalid
references and requires shared lock + all writer gates; the reservation API is
zero-init, same-thread, non-nested with partial-acquisition release; legacy
`cmd_fetch_v2`/`cmd_get_multi` match the described frontends. The
consolidation amendment that appeared mid-run in two planning-side files is
documented in [executed review](2026-10-27-phase3b10-raw-diff-review.md#spec--execution-conformance); it touches no
manifest, boundary or protected file and required no execution decision.

## Task 1 — stage 1, genuine RED captured before implementation

| Command | Exit | Key raw output |
|---|---|---|
| `driver.py --stage 1 --check-only` | 0 | `stage 1 preflight verified: 4 files; no writes` |
| `driver.py --stage 1` | 0 | `stage 1 verified: 4 files` (postimage hashes asserted by driver) |
| `env SKIP_TESTS=1 ./build.sh` | 0 | full build; `grep -ci warning` → 0 |
| `shard-db-test run test-packed-csv-commands` | **39** | `# test-packed-csv-commands: 178 passed, 39 failed` |
| `checks.py --mode red` | 0 | compiled probe from real sources + stubs; `probe gates=6 failures=2` (expected internal exit 1 handled by wrapper) |

RED is semantic, not setup/link. On the ENOTSUP stubs every success-path
assertion fails on status (`expected 0 got -1` for empty fetch, multi-get CSV,
fetch CSV, projections, cursors, UUID/sequence routes, maximum-schema fetch)
and every error-emission assertion fails because the stub writes no JSON
(`one error before any CSV prefix`: assertion `text&&!strncmp(text,"{\"error\":",9)`
sees empty output). Setup, cleanup and reservation-hygiene assertions pass
incidentally. The two probe dispatch gates — `actual fetch reaches checked
backend` and `actual multi-get reaches checked backend` — FAIL because the
real extracted frontends return −1 without CSV; the four ancillary gates
(registry cleanup ×2, writer gates released ×2) PASS, exactly the predicted
6 gates / 2 failures. RED allocation tracking is disabled by design (stubs
cannot warm storage caches); no baseline leak claim is made.

## Task 2 — full backends, packed-only dispatch, GREEN and proofs

| Command | Exit | Key raw output |
|---|---|---|
| `driver.py --stage 2` | 0 | `stage 2 verified: 3 files` |
| `env SKIP_TESTS=1 ./build.sh` | 0 | `grep -ci warning` → 0 |
| `shard-db-test run test-packed-csv-commands` | 0 | `# test-packed-csv-commands: 217 passed, 0 failed` |
| `checks.py --mode green` | 0 | `probe gates=388 failures=0`; `mode=0 allocation injections=24`; `mode=1 allocation injections=47`; `maximum largest=24 live=0`; `final selected live=0` |
| `checks.py --mode regression-proof` | 0 | scratch reversal of the three source files to exact stage-1 hashes → `probe gates=6 failures=2` (same two dispatch-output FAILs); reapply → exact stage-2 hashes → `probe gates=388 failures=0`; final line: `all three reversed/restored source hashes exact; checkout sources and binaries untouched` |

All 388 green gates PASS, 0 FAIL: 2× `actual command dispatch exact`, 78×
`all writer gates released`, 70× `construction failure explicit` (rc −1 with
errno ENOMEM), 2× `deadline before success prefix` (ETIMEDOUT), 1× `maximum
schema bounded actual allocations` (largest selected allocation 24 bytes,
live 0), 79× `no selected live buffers`, 71× `one JSON error no CSV prefix`
(every allocation injection — fetch 24 + multi-get 47 — produces one explicit
NULL-free `{"error":...}` with errno ENOMEM, never a CSV prefix), 79× `one
scoped registry cleanup`, 2× `registry failure before success prefix` (EIO),
2× `storage failure before success prefix` (EIO). Forced storage/registry/
deadline failures run through delegating scratch wrappers (`p10_walk`,
`p10_lookup`, `p10_clock`) — no production seam. The probe's metadata/registry
lookups are mocked; its frontends are the actual source-extracted command
bodies doing real backend/storage work against a one-shard physical fixture.

## Task 3 — docs, fresh full suite, exact hand-back

| Command | Exit | Key raw output |
|---|---|---|
| `driver.py --stage 3` | 0 | `stage 3 verified: 4 files` (concept doc new; two concept updates; `slotcask.h` guard-lookup caller comment) |
| `shard-db-test run-all` (fresh, post-docs) | 0 | `1..528` / `# total: 20820 passed, 0 failed across 528 cases` |
| `checks.py --mode green` (repeat) | 0 | `probe gates=388 failures=0`; injections 24+47; `maximum largest=24 live=0`; `final selected live=0` |
| `git diff --check` | 0 | no output (clean) |
| `sha256sum docs/plans/2026-10-19-durability-certification-rig.md` | 0 | `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` — unchanged |

Final checkpoint verification (after the full suite): all **10/10** approved
checkpoint-3 hashes MATCH and all **20/20** unchanged-boundary hashes MATCH.

## Contract bounds exercised

Exact large decimals with scale/trailing zeros (`1234567890123456.1234567890`),
NUL rendered `\u0000` versus literal `\\u0000`, embedded quote/comma/newline
escaping, composite projection (`f2,f1+f0`) with custom delimiter `|`, strict
decimal cursor plus offset overflow rejection (`9223372036854775807` + 1),
negative/junk cursor, `total` + CSV rejection, tiny budget rejection
(`buffer_budget=1` and `64`), successful projected output with a 100-byte
retained-response budget at a 256-field schema, generation mismatch
(`SREC_FLAG_NULLMAP` row), non-JSON/non-array/non-string keys, trailing text,
binary keys with NUL (`{'x',0,'z'}` stored and echoed with length), UUID
storage/display plus fully escaped UUID text under the text bound, sequence
keys (8-byte BE, `-1` display), malformed sequence/UUID rejection, missing /
duplicate / empty key sets, short-write and buffered-flush failure with no
replacement error appended to an accepted CSV prefix, reservation released
before I/O (`writer.held==0`) and on every failure path, and maximum-schema
actual-size response.

## Reference/gate cleanup

Every command call asserts `slotcask_write_reservation_held()==0` on return
(focused test) and the probe asserts `all writer gates released` and `one
scoped registry cleanup` on all 79 invocation points, `no selected live
buffers` in every armed invocation and `final selected live=0` at exit. No
reservation, registry reference or owned response line leaks on any success,
storage, registry, deadline, allocation or I/O path.

## Limitations

Packed storage stays publicly inactive: the new dispatch branches trigger only
for CSV format plus an explicitly bound packed schema; loaders/create-object
still never bind packed; legacy CSV and all non-CSV branches are byte-identical
(`docs/concepts/packed-csv-commands.md` states the contract). The probe mocks
metadata/registry loading, so no public loader, network or end-to-end
activation acceptance is claimed. `buffer_budget` caps owned retained response
lines only — borrowed input, cache mappings, fixed frames, reservation
bookkeeping and transient renderer/decoder workspaces are excluded, so this is
not the full request-memory policy. Reservation acquisition and stdio writes
block; no cancellation guarantee is claimed while waiting. Fixture is a
single-shard local physical store; no cross-platform, throughput, sanitizer or
benchmark claim — per AGENTS.md the branch-final ASan+UBSan/TSan gates remain
immediately before any authorized push/PR.

## Ten final checkpoint hashes

| File | SHA256 |
|---|---|
| `src/db/packed_csv_command.h` | `d81b0e468ca0b1db9ba1ee15cb7d42e15ac63ab9b5ea2180921bf1298178924d` |
| `src/db/packed_csv_command.c` | `b4967cfa2e0cc1805832c358d62964bf5f51c7a4e6ce859e60e8e02c1d486ceb` |
| `src/test/cases/test_packed_csv_commands.c` | `99006ee654f77b5e41f44feae7489574b2d163321d71462d90fdbd1804898c1d` |
| `build.sh` | `f6355ec998a85821e70da3897af243903627caa90071c101d6c194f43c3a9623` |
| `src/db/query_find.c` | `912588f74c20223c288aacffed15d8995e117d3db0a7398699073ac235436159` |
| `src/db/storage.c` | `dd0e0a9e72c9e4bcd38eda3ba1b9240ec6165d07a5c949f16af2f824087ec6cd` |
| `docs/concepts/packed-csv-commands.md` | `10c52548b686f3d6f3b9e45162e8226bc893943f7d5a540e7a07eb2de81787a4` |
| `src/db/slotcask.h` | `59a76eb30281627e2dd50b889f3327905c7b00462e5f747105bd8f8f5b59d24c` |
| `docs/concepts/packed-csv-rendering.md` | `9f21d52090ea98652df6bd9a550ebf6a79ba62b411e437ca1593e673a3d086f0` |
| `docs/concepts/typed-row-adapter.md` | `2ab98d158cc7408d85e48ddf2cf5485098dd43eefe072cba272d96aeaf0ca24f` |

## Working tree at hand-back

Unstaged, uncommitted, on `fix/phase1-safe-scalar-conversions` at
`174f7d8e99bee4a619a23aed3dfa07a3cbb05bcf`, staged diff empty, ready for
plan-aware and fresh blind raw-diff review. Executor write set (this child):
modified `build.sh`, `src/db/query_find.c`, `src/db/storage.c`,
`src/db/slotcask.h`, `docs/concepts/packed-csv-rendering.md`,
`docs/concepts/typed-row-adapter.md`; new `src/db/packed_csv_command.h`,
`src/db/packed_csv_command.c`, `src/test/cases/test_packed_csv_commands.c`,
`docs/concepts/packed-csv-commands.md`, and this evidence file. Preserved
planning-side artifacts (not executor-written):
`docs/plans/2026-10-23-phase3-packed-records-plan.md` and
`docs/plans/2026-10-19-type-correctness-recovery-plan.md` (planner
consolidation edits, see [executed review](2026-10-27-phase3b10-raw-diff-review.md#spec--execution-conformance)), the
hash-only protected rig, the 3B.10 plan and its review.

## Raw-diff review correction

The reviewer corrected one evidence-only attribution: the 256-field projected
fetch with buffer_budget=100 is a successful actual-size response test, not a
budget-rejection test. Rejection cases use budgets 1 and 64. Production code,
tests, plan/manifest hashes and recorded gate results are unchanged.

## Commit cleanup

Removed one surplus blank line at the end of the executable-plan document to
satisfy staged diff whitespace checks. The execution/review-time approved SHA
remains historical evidence (`374b66160925e9c3bb19635452e408980e652ec671be9c6a0e645a99f6a9b343`);
the cleaned document SHA256 is `7e9bedd8f17d7ccbe83996ce0f19908021bbcd82307066086798ba8f36f9db11`.
All fenced blocks, manifest hashes, production postimages and recorded results
are unchanged. No execution used the cleaned serialization. Temporary notes
were removed after preserving their provenance in the curated review/evidence.
