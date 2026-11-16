# Phase 3B.1 execution evidence (2026-10-25)

Executor: GLM 5.3, per [the Revision 2 plan](2026-10-25-phase3b1-typed-row-adapter-plan.md)
(SHA256 `6c5c757ae34e50d0bb2cb6ab0e5da6422da62e01bab3be7ad4783bcfc1cae725`).
Every planning-reference result matched; **no deviations occurred** and no
assertion was changed. Work is left uncommitted/unstaged for raw-diff review.

## Checkpoint and protection

| Check | Actual | Exit |
|---|---|---|
| `rtk git rev-parse HEAD` | `b9cea93fd7e314eeb7505675e7d2212da850e184` | 0 |
| `rtk git branch --show-current` | `fix/phase1-safe-scalar-conversions` | 0 |
| Pre-existing planning diffs | `2026-10-23-phase3-packed-records-plan.md` (M) + three untracked planning docs preserved untouched | — |
| Protected `2026-10-19-durability-certification-rig.md` SHA256 (hash-only, never read) | `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` — exact at start and end | 0 |

## Mechanical extraction

Appendix A bootstrap extracted the driver to `/tmp/shard-p3b1-exec-psUgfw/apply.py`
(exit 0). All six Appendix E immutable blocks were extracted and hashed before
any stage ran; every checksum matched (`row-header 4d19ffa9…`, `row-tests
26727e7b…`, `row-stub 1a6a4d87…`, `row-views 0c803fcf…`, `row-core 4aa90389…`,
`probe-c 02c7a615…`). Stages 1–4 were applied by the extracted driver only;
each anchored old block matched exactly once (the driver halts otherwise) and
stage 2/3 verified byte-identical header/tests inputs.

## Task gates (actual commands and results)

| Task / command | Result | Exit |
|---|---|---|
| Stage 1 | 13 anchored edits + `typed_row.h`, stub `typed_row.c`, `test_typed_row_adapter.c` created | 0 |
| Task 1 `rtk proxy env SKIP_TESTS=1 ./build.sh` | built all targets incl. bench, **0 warnings** | 0 |
| Task 1 `rtk proxy ./build/bin/shard-db-test run test-typed-row-adapter` | **RED 13 passed / 23 failed** — unsupported-stub failures only | 23 |
| Stage 2 | stub replaced by complete row-views module; header/tests unchanged | 0 |
| Task 2 build (same command) | **0 warnings** | 0 |
| Task 2 focused run (same command) | **RED 84 passed / 34 failed** — preparation/render stubs remain | 34 |
| Stage 3 | views replaced by complete row-core module | 0 |
| Task 3 build (same command) | **0 warnings** | 0 |
| Task 3 focused run (same command) | **GREEN 214 passed / 0 failed** | 0 |
| Task 3 `rtk proxy ./build/bin/shard-db-test run-all` | **19030 passed / 0 failed across 519 cases** | 0 |

The full suite ran once against the frozen final code, first attempt, in the
host's socket-capable execution mode; the log contains zero `not ok` lines and
zero EPERM events, so no fallback invocation was needed. No functional failure
was rerun into success; RED outputs were preserved unfiltered.

## Task 4 probe

Probe extracted with the driver (exit 0). `typed_row.c` compiled separately
without LTO (`gcc -std=gnu11 -O1 -g -fno-lto -Wall -Wextra -Werror`, exit 0),
linked with the normal archive and `--wrap` for malloc/calloc/realloc/free/
generate_default (exit 0), then executed:

```text
OOM case 0: 21 allocation failures propagated, no tracked leaks
OOM case 1: 11 allocation failures propagated, no tracked leaks
OOM case 2: 8 allocation failures propagated, no tracked leaks
OOM case 3: 13 allocation failures propagated, no tracked leaks
binder: 3 allocation failures preserve schema and ENOMEM
4 MiB varchar with 8-byte budget: 0 output allocations
sequence/random: 2 calls for 2 defaults, unchanged by repeated defaults/measure/compose
sizeof(TypedSchema)=154656 sizeof(PkrSchema)=7184 sizeof(TypedPrepared)=8464
```

Exit 0: all **56 injected allocation failures** (53 route + 3 enum-binder)
propagated with correct errno/cleared owned outputs and no tracked leaks.
The probe's `json_unescape_string: malloc(n) failed` stderr lines are the
expected injected-failure output of the existing JSON helper. Stage 4 then
created `docs/concepts/typed-row-adapter.md` and updated the codec intro
(both doc-only; no code/test change followed, so no re-verification was due).

## Final inventory and hand-back state

`rtk git diff --check` clean. Tracked diffs: `build.sh`, `src/db/types.h`,
`src/db/config.c` (include + `static` removed from four otherwise
byte-identical helpers), `docs/concepts/packed-record-codec.md`, plus the
pre-existing planning-document modification. New files: `src/db/typed_row.h`,
`src/db/typed_row.c`, `src/test/cases/test_typed_row_adapter.c`,
`docs/concepts/typed-row-adapter.md`, this evidence file.
`rtk rg -n 'typed_schema_bind_packed' src/db` shows declaration
(`typed_row.h:62`) and definition (`typed_row.c:29`) only — no loader or
create-object caller. No storage/query/index/bulk/rebuild file differs.
Packed storage remains inactive; no sanitizers, benchmarks, staging, commits,
pushes or PRs were run.

## Revision 3 — authorized post-review enum fix

The user asked GPT-6.1 to amend the plan and execute this small fix.
GLM 5.3 remains the original Revision 2 executor; GPT-6.1 planned/executed
this fix round. The original Revision 2 evidence and checksum above remain
historical, unchanged.

Root cause: fixed-field encoding assumes the caller already checked enum
membership. The packed setter now validates decoded nonempty enum spans
with enum_value_index before allocation or cell replacement. Its error
identifies the field and returns EINVAL through existing cleanup. Empty-string
member-zero and explicit-NULL behavior remain unchanged. No new export,
format, production caller or activation was added.

Appendix H was written before source edits. Regressions were added first,
then the warning-free RED build and focused run were captured before the guard.
All 214 prior assertions remain; 42 new assertions cover unknown JSON values,
invalid early duplicates, raw booleans, escaped valid members, empty/NULL,
cell/owner/seen/byte-count preservation, defaults and updates.

| Gate | Actual result | Exit |
|---|---|---|
| RED build: rtk proxy env SKIP_TESTS=1 ./build.sh | zero warnings/errors | 0 |
| RED: rtk proxy ./build/bin/shard-db-test run test-typed-row-adapter | 232 passed / 24 failed | 24 |
| GREEN build: same build command | zero warnings/errors | 0 |
| GREEN: same focused command | 256 passed / 0 failed | 0 |
| Normal full suite: rtk proxy ./build/bin/shard-db-test run-all | 19072 passed / 0 failed across 519 cases | 0 |
| Separate-module allocation probe | all 56 injections pass, no tracked leaks | 0 |

Actual focused summaries:
```text
# test-typed-row-adapter: 232 passed, 24 failed
# test-typed-row-adapter: 256 passed, 0 failed
```

The probe was extracted unchanged from probe-c. Commands:
```sh
rtk proxy gcc -std=gnu11 -O1 -g -fno-lto -Wall -Wextra -Werror -Isrc/db -c src/db/typed_row.c -o /tmp/p3b1-enum-fix/module.o
rtk proxy gcc -std=gnu11 -O1 -g -Wall -Wextra -Werror -Isrc/db /tmp/p3b1-enum-fix/probe.c /tmp/p3b1-enum-fix/module.o build/bin/libshard-db.a -Wl,--wrap=malloc,--wrap=calloc,--wrap=realloc,--wrap=free,--wrap=generate_default -pthread -lssl -lcrypto -lm -o /tmp/p3b1-enum-fix/probe
rtk proxy /tmp/p3b1-enum-fix/probe
```
Output: encode 21, update 11, decode 8, transfer 13, binder 3 failures injected;
all propagated correctly. The 4 MiB varchar/8-byte-budget check allocates no
output, and two sequence/random defaults still generate exactly twice.
Struct sizes remain 154656 / 7184 / 8464 bytes.

Independent blind re-review recompiled the original three reproductions:
each now rejects with EINVAL and NULL output. It found no remaining actionable
issue. A separate plan-aware reviewer found H1/H2 exact, fresh/resume order
coherent, and no execution deviation or scope drift. Automated comparison
confirms both actual source/test files equal the frozen R2 blocks with only
their Appendix H edit; all six original block checksums remain valid.
No stage-1/2 replay was claimed for this explicitly post-execution fix.

Scratch commands, probe and raw logs remain outside version control under
/tmp/p3b1-enum-fix/. No sanitizer or benchmark was run; no staging, commit,
push or PR occurred. Packed binding still has declaration/definition only.
The protected certification file is checked by hash only.

Actual full-suite summary (fresh socket-capable run, exit 0):
```text
1..519
# total: 19072 passed, 0 failed across 519 cases
```
Final protected hash matches the required value exactly. Contents never read.
Current Revision 3 plan SHA256: efc2ca747f544bd47612668ea6b4717505a71c302f5144dd95c60b6eaaa55b1b.
Both independent re-review dispositions are clear; ready for cleanup/raw-diff
approval and an explicitly authorized commit. No known finding remains open
in this fix round.

## Cleanup for the user-authorized commit

Planner/reviewer GPT-6.1; original executor GLM 5.3; approved enum fix executed
by GPT-6.1. Both models are credited. Cleanup updates planning status only;
implementation and tests are unchanged from the verified/re-reviewed tree.
Commit candidates are the adapter, build wiring, regression case, concept docs,
roadmap index, executable plan, concise evidence and review records. Raw logs,
scratch probes/binaries, build outputs, caches and the protected certification
plan are excluded. Branch-series sanitizer gates remain immediately before
push, after all roadmap fixes; no push is authorized in this cleanup.
