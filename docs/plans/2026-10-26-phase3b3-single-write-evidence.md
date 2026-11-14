# Phase 3B.3 Revision 2 — execution evidence ledger

Executor: GLM-5.3-Flash (ZCode agent), 2026-10-26. All raw logs, probe objects and
binaries live outside the repository in `/tmp/shard-p3b3-exec-V2CTZJ/` and are
referenced, not copied. Nothing committed, staged or pushed.

## Identity and hashes

- Base: `82e5d4053431b0b163a621712772d20727d4ff28`, branch
  `fix/phase1-safe-scalar-conversions` — matched at start; work left uncommitted on it.
- Plan `docs/plans/2026-10-26-phase3b3-single-write-integration-plan.md` SHA256
  `45d902bc50a532513ec39659d9771d53ab589ab09dc8d6bbe7caeb77df90fda7` — matched.
- Protected rig `docs/plans/2026-10-19-durability-certification-rig.md` SHA256
  `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` — matched at
  start and finish; contents never read.
- Start tree contained exactly the expected planning artifacts (this plan, the
  Phase 3 index link modification, the untracked protected rig). No other changes.
- All stage-0/1/2/3 checkpoint hashes validated by the plan's own driver before each
  write; final checkpoint-3 table re-verified independently after stage 3: all 12
  entries match.

## Commands and transcripts (all prefixed `rtk`)

| Gate | Command (from plan) | Exit | Result |
|---|---|---|---|
| Stage-1 apply | `apply.py <plan> 1` | 0 | 5 files via unique anchors |
| RED build | `SKIP_TESTS=1 ./build.sh` | 0 | zero warnings (`red-build.log`) |
| RED focused | `run test-single-write-integration` | 32 | **87 passed / 32 failed** (`red.log`) |
| RED decoder | compile+link+run `render-red` | 1 | **0 passed / 2 failed** (`render-red.log`) |
| Stage-2 apply | `apply.py <plan> 2` | 0 | 5 files via unique anchors |
| GREEN build | `SKIP_TESTS=1 ./build.sh` | 0 | zero warnings (`green-build.log`) |
| GREEN focused | `run test-single-write-integration` | 0 | **240 passed / 0 failed** (`green.log`) |
| Hook regression | `run test-bt-kf-inversion-stream-find` | 0 | **20 passed / 0 failed** (`hook-regression.log`) |
| Full suite | `run-all` | 0 | **19465 passed / 0 failed across 521 cases** (`full.log`) |
| Probe build | 3× `-fno-lto` object compiles + LTO static-lib link | 0 | (`probe`, objects in tmpdir) |
| Probe run | `./probe` | 0 | see below (`probe.log`) |
| Stage-3 apply | `apply.py <plan> 3` | 0 | 3 doc files via unique anchors |
| Diff hygiene | `git diff --check` | 0 | clean |

## RED failure mechanisms (named, inspected — not just exit codes)

- Focused RED failures include the required classes: transport `explicit fresh
  owning callback` / `fresh constructor once` / `flag alone rejects` / `invalid
  option errno`, and successful-write assertions `single actual-size insert`,
  `response exact numeric`, `bare exponent normalized`, `max schema tiny
  single insert/update`, `NOT NULL varchar auto_create`, `full 4 MiB single
  insert`, `first/second concurrent partial update`.
- Decoder RED (unchanged config.c): allocation ordinal 1 → child **signal 11**
  (unchecked outer malloc dereference), reported by the parent as a failure;
  allocation ordinal 2 → child **exit 2**, incomplete/error result from the
  512-byte varchar fallback truncation (`json_escape_into overflow ... buflen=512`
  logged). Parent exit 1 with 0 passed / 2 failed. Long-enum assertions
  (`long enum is not silently omitted`, `complete legacy JSON`) also RED.

## Probe results (`probe.log`, exit 0)

- Decoder OOM GREEN: `legacy decoder failure checks: 2 passed, 0 failed`;
  both ordinals return NULL/ENOMEM with tracked live=0.
- Write injections, one per selected allocation ordinal:
  - Legacy layout: 3 fresh + 3 replacement + 3 partial-update failures pass —
    exact OLD or absence, live/deleted counts, segment position, tracked live=0.
  - Packed layout: 10 fresh + 9 replacement + 5 partial-update failures pass —
    same invariants.
  - Total 33 write allocation failures, matching Appendix C.
- Maximum schema: all three modes (fresh/replace/update) largest selected
  allocation **2968 bytes**, tracked live=0 — no allocation approached the
  8 MiB ceiling; no schema-maximum scratch.

## Inventory (census)

- `new_from_old_alloc_on_insert=1` is set only by `cmd_insert_v2_impl`
  (`src/db/storage.c:1207`) and the focused transport case
  (`src/test/cases/test_single_write_integration.c:145`); remaining hits are the
  header declaration/doc and the slotcask.c validator/consumer checks.
- `typed_schema_bind_packed` appears in no loader/create-object source
  (`query_schema.c`/`config.c`/`main.c`); packed layouts stay publicly inactive.
- `cmd_update_v2_impl` wires `.new_from_old_alloc = v2_update_owned` and no longer
  assigns `.new_from_old`; `v2_update_new_from_old` survives only as the legacy
  bounded producer called from `v2_update_owned` (storage.c:1974).
- Packed index/CAS guard rails in both `_impl` functions run before defaults
  encoding, trim wiring, CAS parse and any write.
- No TODO/FIXME/debug markers introduced (single `XXXXXX` hit is the mkdtemp
  template string). No sanitizer or benchmark runs, per plan rule 7.

## Deviations

None. Every gate matched Appendix C's Revision 2 numbers exactly. Note for
reviewers: `probe.log` interleaves benign `Error: Object [obj] not found` stderr
lines from the pre-existing schema-lookup helper (`config.c:914`) against the
metadata-less fixture root; probe assertions are unaffected (exit 0). Stage 3 ran
after the suite/probe strictly in the plan's Task 3 order and touched only the
three `docs/concepts/*.md` files, so no source rerun was required.
