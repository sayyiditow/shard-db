# Phase 3B.8 — packed named display text and composite JSON: execution evidence

Curated actual transcript of Revision 2 execution (Tasks 0–3). Raw unfiltered
logs, probe sources, scratch objects and binaries live only under `/tmp` (paths
below); nothing raw is committed or staged. All seventeen Appendix A commands
were run in the published order with the `rtk proxy` prefix exactly as written.
The run completed with zero mismatches, so no `PLAN_NOTES.md` exists.

## Identity

| Item | Value |
|---|---|
| Start HEAD | `cfc4970c2e0fcfa4e4a649f5cb7e347a147a04f7` (verified = required reviewed 3B.7 base; also enforced inside every driver stage) |
| End HEAD | `cfc4970c2e0fcfa4e4a649f5cb7e347a147a04f7` (no git writes; `git diff --cached` empty) |
| Branch | `fix/phase1-safe-scalar-conversions` (start = end) |
| Approved plan | `docs/plans/2026-10-26-phase3b8-packed-named-projection-plan.md` |
| Plan SHA256 | `6085fe6c14b1e58905b812f5db96aab22fff088fb2f84512064dfbaad1bd0551` |
| Protected rig | `docs/plans/2026-10-19-durability-certification-rig.md` |
| Protected SHA256 | `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` — verified by hash at start, revalidated by each of the four driver-stage preflights (the driver hashes it; the checks.py probes do not), and explicitly re-hashed at the end. Contents were never read, copied, staged or executed. |
| Executor / compiler | GLM 5.3; gcc 16.2.1 20260810 as reported by `gcc --version` on this workstation, Linux x86_64 |
| Bootstrap dir | `/tmp/p3b8-exec-_ld9j8ca` (31 blocks extracted verbatim by the plan's frozen bootstrap, executed via `rtk proxy python3` from the repo root; the bootstrap block itself was extracted with the driver's regex and hash-verified `5dbe194b…4709` against the manifest before execution; extracted `driver.py` = `8380092…40ca5`, `checks.py` = `daa6075…f6cc4`, both equal to their manifest block hashes) |

Pre-existing planning artifacts inventoried and preserved untouched:
`docs/plans/2026-10-23-phase3-packed-records-plan.md` (modified before this
run), `docs/plans/2026-10-26-phase3b8-packed-named-projection-plan.md`,
`docs/plans/2026-10-27-phase3b8-plan-review.md`, and the protected rig file.
Nothing was added to the index at any point. Task 0 read AGENTS.md and
CORE-PROCESS.md, this plan, the Phase 3 work index, the parent recovery plan
header, the reviewed Phase3B.7 plan/evidence/raw-diff review,
typed-records.md, typed-row-adapter.md, packed-json-projection.md,
whole-record-readers.md and query-protocol/overview.md/find.md/joins.md. The
protected rig's contents were never opened.

## Commands executed (Appendix A order)

```
driver.py --stage 1 --check-only   # "stage 1 preflight verified: 5 files; no writes"
driver.py --stage 1                # "stage 1 verified: 5 files"
env SKIP_TESTS=1 ./build.sh        # exit 0, zero warnings (log: /tmp/p3b8-logs/stage1-build.log)
run test-packed-named-projection   # exit 158 — RED (log: /tmp/p3b8-logs/red-named.log)
run test-packed-json-projection    # exit 2 — RED (log: /tmp/p3b8-logs/red-projection.log)
checks.py --mode red               # wrapper exit 0; expected probe exit 1 internally handled (log: /tmp/p3b8-logs/probe-red.log)
driver.py --stage 2                # "stage 2 verified: 3 files"
env SKIP_TESTS=1 ./build.sh        # exit 0, zero warnings (log: /tmp/p3b8-logs/stage2-build.log)
run test-packed-named-projection   # exit 0 — GREEN (log: /tmp/p3b8-logs/green-named.log)
run test-packed-json-projection    # exit 0 — GREEN (log: /tmp/p3b8-logs/green-projection.log)
checks.py --mode green             # exit 0, 258 PASS / 0 FAIL (log: /tmp/p3b8-logs/probe-green.log)
checks.py --mode regression-proof  # exit 0 (log: /tmp/p3b8-logs/proof.log)
driver.py --stage 3                # "stage 3 verified: 3 files"
shard-db-test run-all              # exit 0 (log: /tmp/p3b8-logs/full.log)
checks.py --mode green             # exit 0, repeat, 258 PASS / 0 FAIL (log: /tmp/p3b8-logs/probe-green-final.log)
git diff --check                   # exit 0, clean
sha256sum protected rig            # exact expected hash again
```

(Each line above ran as the corresponding published Appendix A command with
`rtk proxy`, `--plan docs/plans/2026-10-26-phase3b8-packed-named-projection-plan.md`
and the frozen scripts from `/tmp/p3b8-exec-_ld9j8ca`.) Stage 2 was applied
only after the new RED exit 158, the prior-projection RED exit 2 and the 21/17
red probe were confirmed. All raw logs: `/tmp/p3b8-logs/{stage1-build,
red-named,red-projection,probe-red,stage2-build,green-named,green-projection,
probe-green,proof,full,probe-green-final}.log` plus `bootstrap.py` and its
hash-verifying extractor `extract_bootstrap.py` — all outside version control.

## RED (mandatory, before implementation)

Focused `test-packed-named-projection` against stubbed named APIs and unchanged
production code: **38 passed / 158 failed, exit 158**. The 158 failures carry
the intended missing-behavior classes (90 printed assertion signatures,
dominant ones counted): exact raw bytes/length and exact raw-budget checks
(16× `out&&used==length&&!memcmp(out,expected,length)&&!out[length]`, 16× the
same without the terminator clause), one-short budget rejections (17×
`!out&&errno==ENOMEM&&used==0`, 16× `!out&&errno==ENOMEM`), absence distinct
from error (7× `!out&&errno==0&&used==0`), generation/corruption and NULL-argument
rejections (`EILSEQ`×9, `EINVAL`×4, `E2BIG`×3 signatures), the full
two-component decimal digit check and the maximum binary composite check
(one each). No ENOTSUP appears in the new case's failure set — its stubs fail
by returning NULL/absence, exactly the unimplemented contract.

Focused `test-packed-json-projection` (prior case, with only the two
deliberately advanced assertions updated to exact-value form): **216 passed /
2 failed, exit 2**. The only failures are `not ok 88 - packed composite named
helper exact` and `not ok 89 - mixed composite request exact` — the two
replacements of the former temporary ENOTSUP refusals, proved RED before the
renderer existed. No other assertion changed.

Checks `--mode red` compiled the actual `typed_row.c`/`query_find.c`/
`packed_record.c`/`decnum.c` with scratch-only `p8_*` allocator substitutions
and the source-extracted `cmd_get_fields` body with local schema/registry/read
mocks (scratch `/tmp/p3b8-check-tfc0yn7e`; no production seam): probe exit 1,
internally handled, exactly **21 passing / 17 failing gates**. The 17 failing
gates are the planned defect classes: composite raw/JSON/shared cells absent
with empty allocation sweeps, projected GET wrong output, empty-composite/
component-limit/unselected-corruption/command bounds not enforced, backend
int-count bound absent (`largest escape input=0`), and maximum-schema sizing
absent. Setup failures would exit 2; none occurred.

## GREEN (stage 2)

- `test-packed-named-projection`: **196 passed / 0 failed, exit 0**.
- `test-packed-json-projection`: **218 passed / 0 failed, exit 0** (both
  advanced composite assertions now pass with the exact escaped output).
- Checks `--mode green` (scratch `/tmp/p3b8-check-_5s3_hpl`): **258 PASS /
  0 FAIL**, `probe failures=0`. Allocation injections **77** — raw numeric
  composite 12, direct JSON 13, named binary JSON 9, raw binary composite 8,
  projected command 35 — every injected failure returned NULL/ENOMEM with
  `text_len` reset where applicable or one command error with no success
  prefix, freed all tracked buffers (`live==0`) and released the mocked
  registry reference exactly once. FILE fixtures covered short/partial
  accepted writes and a failing newline, each returning failure with no
  replacement error after the accepted I/O prefix. Empty composite renders
  `null`; over-component requests report the projection limit; unselected
  corruption still rejects.
- Delegated-backend bound: the probe's escaping wrapper delegates to the real
  `json_escape_into` while recording input spans. Green probe reports
  `largest escape input=4096` with the full **147476**-byte quoted output of
  **24579** raw NUL bytes intact (`3×8193` components, every `\u0000` byte
  verified) — the count-safety invariant through actual calls. Stage-1 RED
  reports `largest escape input=0` because its named stubs never reach the
  escape backend; that result does not demonstrate the old writer's call size.
  The separate planning-time old-writer probe in Appendix D of the approved
  plan reports 257/1 with `largest escape input=24579`, demonstrating the
  unbounded single call. The 2 GiB example is established by exact arithmetic,
  never allocated.
- Maximum-schema projection: row **actual 2085**, largest selected allocation
  **36**, `live=0`, despite the >1 GiB declared schema ceiling. The fetched
  raw record is still allocated by the real (mocked) reader and excluded from
  selected-allocation accounting; decimal validation workspaces remain
  separately bounded.

## Scratch-only revert/reapply proof (CORE-PROCESS)

`checks.py --mode regression-proof`, all in `/tmp/p3b8-check-4qowy5rx`;
checked-out sources and build products never modified:

- Copied the fixed `typed_row.c`/`query_find.c`/`storage.c` (hash-verified
  against checkpoint 2 / committed parent first), reversed the exact stage-2
  hunks in scratch, verified byte-identical checkpoint-1 hashes (for
  `query_find.c`/`storage.c` the committed baseline; for `typed_row.c`
  checkpoint 1 including the deliberate stage-1 link stubs), recompiled and
  ran: **21 PASS / 17 FAIL**, reproducing exactly the original defect gates.
  Reapplied the hunks, verified hash equality with checkpoint 2 and the
  unchanged committed `storage.c` hash, recompiled: **258 PASS / 0 FAIL**.

## Stage 3 and final verification

- `driver.py --stage 3`: `stage 3 verified: 3 files` (new
  `docs/concepts/packed-named-projection.md`; contract/link corrections in
  `docs/concepts/packed-json-projection.md` and `typed-row-adapter.md`).
- Fresh full suite after docs: **20459 passed / 0 failed across 526 cases,
  exit 0** (adds the new 196/0 case to the committed 3B.7 suite).
- Checks `--mode green` repeated on the final tree (scratch
  `/tmp/p3b8-check-xiizbr_1`): **258 PASS / 0 FAIL**, same 12+13+9+8+35=77
  injections, `largest escape input=4096`, maximum actual 2085 / largest 36 /
  `live=0`.
- `rtk git diff --check`: exit 0, clean. Added-line artifact scan over the
  whole diff (`TODO|FIXME|XXX|fprintf|printf|puts|syslog`): zero hits — no
  debug/TODO/log/cache/probe additions.
- Both builds (after stage 1 and stage 2) produced zero compiler warnings
  (`grep -ci warning` = 0 in both build logs).

### Final owned-file hashes (all nine equal plan checkpoint 3)

| File | SHA256 |
|---|---|
| `build.sh` | `cefe04d25ba0b9fb2df3a37daab53a98c271081b61d2f1a53203dd84a67a50c1` |
| `docs/concepts/packed-json-projection.md` | `d4ebc9b21baf3aff076ec79404de710768b152efa83288af6539ce8b27b5421a` |
| `docs/concepts/packed-named-projection.md` | `54f78f9996ae57cc49ed041a5cef8347b84c43ddd2bf254e0cc6d06630093284` |
| `docs/concepts/typed-row-adapter.md` | `78cbe03002f468649a89bb3b08baeff9dd6b94c9ce15a839cc76491350652eae` |
| `src/db/query_find.c` | `0cbfaae19cee7502fe90a4612a8a0cbf7ecc5d51dbf9e49568bef0c5d3c55b9a` |
| `src/db/typed_row.c` | `001ef5983f43c1df8b1b53fff32e1b7311cd6fae0c550a578b5361356d758267` |
| `src/db/typed_row.h` | `0b56077eaf8f14f70a17460eac915d2e9eeb2a7ffcde409eaedf0b64b6c3129a` |
| `src/test/cases/test_packed_json_projection.c` | `fb517576276f5b952b8bf6147d59eff7070789983156bcc7350edb8993894e16` |
| `src/test/cases/test_packed_named_projection.c` | `24a57573b0b88a432cef3dd3fe8ff224262e7c67122b5e91142d4831e03ee71c` |

`src/db/storage.c` remains byte-identical to its committed parent hash
(`05edc75e…35397c7`), verified inside the regression-proof checkpoint
comparison. Diff scope: the seven modified owned files above plus three new
files (`docs/concepts/packed-named-projection.md`,
`src/test/cases/test_packed_named_projection.c`, and this evidence file). The
only other working-tree changes are the pre-existing modification to
`docs/plans/2026-10-23-phase3-packed-records-plan.md` and the pre-existing
untracked planning/review files, all present before this run.

## Binder census (verified at hand-back)

- `typed_schema_bind_packed`: production references are only its definition
  (`src/db/typed_row.c:47`) and declaration (`src/db/typed_row.h:63`) — no
  loader or `create-object` caller exists; eight test-case files use it as
  fixtures. No default-format, schema-version, migration or activation edit
  appeared; packed storage remains publicly inactive.
- `json_projected_field`'s packed branch now delegates to
  `typed_packed_named_json`; `cmd_get_fields` is untouched. No CSV, criteria,
  sort, group, join or cursor consumer gained packed dispatch. No new
  production TU, test-global, runtime flag or loader seam was added (stage 1's
  single `build.sh` line registers the new test case only).

## Deviations

None. Every anchor matched exactly; the driver verified all stage preflights
and post-write checkpoints; all 30 manifest block hashes matched; and every
required number reproduced: RED 38/158 exit 158 and 216/2 exit 2, red probe
21/17, GREEN 196/0 and 218/0, green probe 258/0 with 77 injections and
`largest escape input=4096`, reversal 21/17 then reapply 258/0, full suite
20459/0 across 526 cases, zero warnings on both builds.

## Hand-back state

Work is stopped **uncommitted and unstaged** for the plan-aware and
independent blind raw-diff reviews. No sanitizers, benchmarks, staging,
commits, pushes, PRs, public packed activation, later-phase work, or
protected-rig implementation was performed; the branch-final ASan+UBSan and
TSan gates remain deferred to the authorized pre-push window per AGENTS.md.
Remaining scope is unchanged from the plan's "Following work": CSV
representation/error propagation and enclosing read/query emitter contracts
without activating raw matching/sort/group/cursor consumers, the binary/NUL
CSV decision in that child, remaining read/delete/index/maintenance inventory
before 3B closure, Phase 3C metadata/publication, Phases 4–9, then the
protected certification rig.
