# R1 execution evidence — 2026-10-28

This is the historical revision-3 execution ledger. A subsequent user-approved
[fetch-skip amendment](2026-10-28-r1-fetch-skip-amendment.md) restores keyfile-only
pagination skipping. Its later source changes intentionally supersede the
affected revision-3 postimages; do not replay the frozen driver over that work.
See the [amendment evidence](2026-10-28-r1-fetch-skip-evidence.md) for current
verification.

Executor: GLM 5.3, executing revision 3 of the approved
[R1 consolidated ordinary integration plan](2026-10-27-r1-write-read-integration-plan.md)
(Planner/reviewer: GPT 6.1 Sol). All commands were run exactly as Appendix A
specifies, via `rtk` / `rtk proxy`, from the repository root. Raw logs,
extracted driver/checks/probe tools, probe sources and binaries stay under
`/tmp/r1-exec-u1hmwncs/` and the scratch replay tree
`/tmp/r1-exec-replay-5nb34vqp/` — never version control.

## Identity and preflight (Task 0)

| Item | Value |
|---|---|
| Branch | `fix/phase1-safe-scalar-conversions` (unchanged start→end) |
| Base HEAD (start and end) | `0ef1a0ba3b33ec94c947e85091f866b39a1ee583` |
| Executed plan SHA256 | `7ad0e3958f9f22f5a293d2e99964bde0a15ea17b7b8a1a2c0f6a595db50936d2` (revision 3, incl. reviewed export-callback and corrupt-keyfile corrections) |
| Protected rig SHA256 | `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` — verified at start and re-hashed at end; contents never read, copied, edited, moved, staged or executed |
| Staged diff at start and end | empty (porcelain shows only ` M` / `??` entries) |
| Pre-existing artifacts at start and end | `M docs/plans/2026-10-19-type-correctness-recovery-plan.md`, `M docs/plans/2026-10-23-phase3-packed-records-plan.md`; untracked protected rig, R1 plan, `2026-10-28-r1-blind-review.md` — exactly the permitted set, preserved |
| Production source at start | clean apart from the planning artifacts above |
| Extraction | Appendix A bootstrap → `/tmp/r1-exec-u1hmwncs`; `driver.py` `940e76e7257711614f1edf4f8e765d5dc126c7a6657cc46ce44a007536427a9f` and `checks.py` `8ef94cdb84a82c2b866a9147fe9d158142248a93d7176f39ea03deed860d0f86` both match the published expected values (`export_probe.py` extracted alongside, `acd7494e7c9d65abb879f0d56fb65a1c328324ab3e9cf2dfc32cb40edfbe68e7`; plan publishes no expectation for it) |
| Stage 1 check-only | `stage 1 preflight: 10 files, no writes` |
| Manifest identity | base/branch/protected match; 17 boundaries; checkpoints 0→3; stage edits 10/24/8 files |
| Socket preflight | `rtk proxy ./build/bin/shard-db-test run test-or-logic` → 44 passed, 0 failed, exit 0 — isolated daemon starts; no EPERM, no socket-capable workaround needed |

References read before execution: AGENTS.md, CORE-PROCESS.md, the parent
recovery/consolidation plan, the Phase 3 work index, the revision 3 blind
review, and the R1 plan itself. The protected rig was accessed only via
SHA256 (by the driver's manifest check and explicit `sha256sum`).

## Task 1 — stage 1, genuine RED on the frozen stubs

| Command | Exit | Key raw output |
|---|---|---|
| `driver.py --stage 1` | 0 | `stage 1 applied: 10 files; exact postimages verified` |
| `env SKIP_TESTS=1 ./build.sh` | 0 | full build; zero warnings (`grep -ci warning` → 0) |
| `shard-db-test run test-r1-routes` | **36** | `# test-r1-routes: 69 passed, 36 failed` |

RED matches the frozen expectation exactly (69/36, exit 36). Failure classes
are the planned ones: stubbed ordinary reads/escaping/projection, exact
computed addition, default consumption/size, describe identity, packed import
refusal, auto-key bulk. Log: `/tmp/r1-exec-u1hmwncs/red-focused.log`.

## Task 2 — stage 2, GREEN and fresh full suite

| Command | Exit | Key raw output |
|---|---|---|
| `driver.py --stage 2` | 0 | `stage 2 applied: 24 files; exact postimages verified` |
| `env SKIP_TESTS=1 ./build.sh` | 0 | full build; zero warnings |
| `shard-db-test run test-r1-routes` | 0 | `# test-r1-routes: 109 passed, 0 failed` |
| `shard-db-test run-all` | 0 | `1..529` / `# total: 20931 passed, 0 failed across 529 cases` |

GREEN and the full suite match the frozen expectations exactly (109/0 exit 0;
20931/0 across 529 cases exit 0). The suite ran to completion on the fresh
warning-free build; no binary was rebuilt or copied while it was live.
Logs: `green-focused.log`, `green-full-suite.log`.

## Task 3 — stage 3, frozen checks, reversal replay, census

| Command | Exit | Key raw output |
|---|---|---|
| `driver.py --stage 3` | 0 | `stage 3 applied: 8 files; exact postimages verified` |
| `driver.py --stage 3 --verify` | 0 | `checkpoint 3 verified` |
| `checks.py --output /tmp/r1-exec-u1hmwncs/checks` | 0 | summary below |
| `git diff --check` | 0 | no output (no whitespace errors) |

Checks highlights (full log: `checks.log`):

- Concurrency boundary: `concurrency prefix unchanged; checked readers add no
  locks/gates; CSV has no reservation; public binder inactive` — slotcask.c
  prefix (475,195 bytes) hash-verified, no writer-gate/reservation/lock
  primitives in appended code, 3B.10's all-shard CSV reservation gone,
  `typed_schema_bind_packed(` still exactly 2 references.
- Allocation-failure admission: 126 deterministic selected-allocation failures,
  all `live=0` — fetch 25 + multi-get 47 + computed 26 + declaration 2 +
  packed CSV 15 + single GET 11. Corrected quota peaks: defaults rejection
  peak=2 under budget 65536 (baseline module peaked 120002); computed OLD
  rejection peak=0 under budget 4096 (baseline peaked 218337).
- Scheduling: all three multi-key modes run 2 shard jobs / 2 bulk calls for
  two touched shards (baseline module: 0 jobs / 0 calls).
- `query_collect` framing: exact-capacity payload parses with bounded
  terminator (baseline probe: SIGSEGV, signal 11).
- Computed updates: every computed occurrence validated against immutable OLD;
  duplicate-invalid rejected; OOM preserves ENOMEM at ordinal 5.
- Corruption probe (flag 3): final binary — single/exists/get/revalidation all
  `rc=-1 errno=117 (EUCLEAN)` plus both tombstone controls, 6/6 PASS.
  Pre-correction scratch suffix — exactly three bulk checks FAIL (exists/get/
  revalidation), single + tombstone controls still pass, `failures=3`.
- Export transport: final binary 18/18 PASS (error envelopes, missing/mistyped
  objects, malformed arrays, capacity, empty-tenant success controls); scratch
  pre-correction daemon — exactly six RED scenarios, empty-tenant success
  controls pass, no fake-server artifacts.
- TUI declaration transport: byte escaping, UTF-8, surrogate pairs, six
  malformed arrays pass. CSV byte decoder: 2000 seeded round trips, all 256
  bytes, NUL/literal distinction pass (`node npm/test/csv-bytes.js`).

### Regression reversal in a fresh scratch tree

`git archive 0ef1a0ba3b33ec94c947e85091f866b39a1ee583` → fresh
`/tmp/r1-exec-replay-5nb34vqp/` (tracked files only; no protected/untracked
file copied). Driver `--scratch` (bounded to `/tmp`) applied stage 1
(`10 files; exact postimages verified`), build clean (0 warnings), focused run
**69 passed, 36 failed, exit 36** — genuine RED, same test file and
expectations as production. Stages 2 and 3 applied (`24` / `8 files; exact
postimages verified`), rebuild clean (0 warnings), focused run **109 passed,
0 failed, exit 0**, checks script exit 0 with identical summary lines. No
production checkout file was ever temporarily reverted.

### Owned-file census

- Working tree: 27 stage-owned tracked files modified, 8 stage-owned new
  files (`schema_wire.{c,h}`, `typed_read_command.{c,h}`,
  `test_r1_routes.c`, `npm/csv-bytes.js`, `npm/test/csv-bytes.js`,
  `docs/concepts/ordinary-type-integration.md`), plus the 2 pre-existing
  planning-doc modifications and 3 pre-existing untracked planning artifacts.
- Scratch-final vs production: zero content differences among tracked source,
  test, docs and build-script files; the only diffs are gitignored build
  artifacts (`build/`, root binaries) and the two planning docs whose
  production copies carry the pre-existing uncommitted planning edits that
  the archive intentionally excludes. Every owned postimage matched the
  driver's measured checkpoints in both trees.
- Everything unstaged and uncommitted; no branch create/reset, commit, push,
  merge, R2 work, packed activation, benchmark, or sanitizer run occurred.
  The branch-final ASan+UBSan and TSan (×3, `--jobs 2`) gates remain
  scheduled immediately before any authorized push/PR per AGENTS.md.

## Deviations

None. Every anchor matched exactly once, every postimage checksum verified on
first application in both trees, and all RED/GREEN/full-suite/checks outputs
matched the plan's frozen values on their first run. No PLAN_NOTES.md halt
condition was hit.
