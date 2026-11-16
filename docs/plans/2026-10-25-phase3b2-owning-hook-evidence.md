# Phase 3B.2 owning write hook — execution evidence

Actual executor run of `docs/plans/2026-10-25-phase3b2-owning-write-hook-plan.md`
(SHA256 `c57de89e09d213694d16818877fc895b7259037263df1e5fe6c528ee6413f7af`),
Tasks 0–3 only, on 2026-10-25. Executor model: GLM-5.3-Flash
(`account:zai-individual-coding-plan/GLM-5.3-Flash`). All commands run via
`rtk`/`rtk proxy` per the plan. Raw logs, driver, probe source and binaries
kept outside version control under `/tmp/p3b2-exec-iQXQZb`. Nothing staged,
committed, pushed, or merged; no sanitizers or benchmarks run.

## Identity / checkpoint

| Check | Actual | Exit |
|---|---|---|
| `git rev-parse HEAD` | `47f5b0ab35f956355d9f9db63056906f610eb797` | 0 |
| `git branch --show-current` | `fix/phase1-safe-scalar-conversions` | 0 |
| Protected rig SHA256 (start) | `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` | 0 |
| Plan SHA256 | `c57de89e09d213694d16818877fc895b7259037263df1e5fe6c528ee6413f7af` | 0 |

Pre-existing tree matched the plan's allowance: only the Phase 3 roadmap index
(tracked, modified), this plan, the blind review, and the protected untracked
rig — no implementation diff. Daemon socket capability established before the
full suite (TCP + unix bind on 127.0.0.1 succeeded in the default capable
mode; no EPERM restriction encountered). Task 0 pinned-source regions read and
confirmed: adapter ctx/value_compute/pre_commit wrappers, both hook wrappers,
`slotcask_record_size_varlen`, `slotcask_seg_max_bytes` (TEST_BUILD override),
old constructor + opts layout in slotcask.h, `typed_update_alloc` owning
update, `test_packed_record_storage` physical fixtures, storage.c single
production `new_from_old` assignment (`v2_update_new_from_old`), embedded
process-context init/close, cas.md contracts, adapter doc anchor unique.

## Task 1 — stage 1 + runtime RED

| Step | Actual | Exit |
|---|---|---|
| Driver extraction from plan block | written to `/tmp/p3b2-exec-iQXQZb/apply.py` | 0 |
| `apply.py … 1` | `stage 1 : 3 files applied via unique anchors` (slotcask.h e1+e2, build.sh e3, new test file) | 0 |
| `SKIP_TESTS=1 ./build.sh` | built; **zero warnings** | 0 |
| `shard-db-test run test-owning-write-hooks` | **99 passed, 42 failed** | 42 |

RED mechanism verified against the plan's expectations (not just the counts):
owning hook ignored — `constructor exactly once` / `rejecting constructor once`
fail with zero callback calls; NEW bytes lost — `stored actual result size`,
`owned result render`, `reopened actual bytes`; wrong error behavior —
`owned constructor rejection is hard`, `constructor failure errno`; wrong trim
behavior — `owned bytes bypass legacy trimmer`. Existing legacy-callback
checks pass in the same run. No compiler failure was involved: the header-only
optional fields compile against the unchanged storage implementation.

## Task 2 — stage 2 + GREEN + full suite

| Step | Actual | Exit |
|---|---|---|
| `apply.py … 2` | `stage 2 : 1 files applied via unique anchors` (slotcask.c e4–e7) | 0 |
| `SKIP_TESTS=1 ./build.sh` | built; **zero warnings** | 0 |
| `shard-db-test run test-owning-write-hooks` | **153 passed, 0 failed** | 0 |
| `shard-db-test run-all` | `1..520` / `# total: 19225 passed, 0 failed across 520 cases` | 0 |

RED/GREEN/full all match the planning reference exactly (99/42 → 153/0 →
19225/0 across 520).

## Task 3 — ownership/OOM/limit probe + docs

| Step | Actual | Exit |
|---|---|---|
| Probe block extraction | `/tmp/p3b2-exec-iQXQZb/probe.c` (114 lines) | 0 |
| `gcc -c src/db/slotcask.c` `-fno-lto -Wall -Wextra -Werror` | clean | 0 |
| `gcc probe.c + slotcask-probe.o + libshard-db.a --wrap=malloc/calloc/realloc/free` | clean | 0 |
| `probe` | all 8 cases pass, `1GB schema bound caused no allocation over 8MiB` | 0 |
| `apply.py … 3` | `stage 3 : 2 files applied via unique anchors` (new owning-write-hooks.md, typed-row-adapter.md link) | 0 |
| `git diff --check` | clean | 0 |
| Protected rig SHA256 (end) | `fbd645417d905741ab87b4651bb61f90c873f67d6be810335a9e3803e4147135` (unchanged) | 0 |

Probe transcript:

~~~text
case 0: calls=1 owned-frees=1 hooks=1 max-allocation=49152, OLD/result intact
case 1: calls=1 owned-frees=1 hooks=0 max-allocation=2085, OLD/result intact
case 2: calls=1 owned-frees=0 hooks=0 max-allocation=2085, OLD/result intact
case 3: calls=1 owned-frees=0 hooks=0 max-allocation=2085, OLD/result intact
case 4: calls=1 owned-frees=1 hooks=0 max-allocation=2085, OLD/result intact
case 5: calls=1 owned-frees=1 hooks=1 max-allocation=2085, OLD/result intact
case 6: calls=1 owned-frees=1 hooks=0 max-allocation=2085, OLD/result intact
case 7: calls=1 owned-frees=1 hooks=0 max-allocation=2085, OLD/result intact
8 ownership/OOM/limit cases pass; 1GB schema bound caused no allocation over 8MiB
~~~

Exactly one constructor call per case, one free per allocated owning result,
zero frees when no result exists (injected malloc failure / NULL success),
live owned buffer inside pre_commit (hooks=1 with observed bytes, cases 0 and
5 precommit-refusal), OLD/generation preserved every case, and no operation
allocation over the probe's 8 MiB cap under the ~1 GiB schema.

## Final census and checkpoint 3 re-verification

`new_from_old_alloc` appears only in `src/db/slotcask.h`,
`src/db/slotcask.c`, and `src/test/cases/test_owning_write_hooks.c` — no
storage.c/query_bulk.c/loader/SDK production assignment; existing
`new_from_old` behavior and its single storage.c call site untouched.

Working-tree changes produced by this run (all unstaged/uncommitted):

- `build.sh` (test registration) — `28681cc82cb5c9e858b333708fd69637f4b5fc5198e25c5eff76af6972df95b3`
- `src/db/slotcask.h` — `2cf265d1fa9629a89e08670af2d6401bd3206cfc2b22fb91fb03f43683f128c6`
- `src/db/slotcask.c` — `6836a15715f4ac46808d4acb718f30b14dadde8943242825dbb18270a236fbd0`
- `src/test/cases/test_owning_write_hooks.c` (new) — `8b156410510b5e2bc3f585091519410ed7083ae402f0bc003dfec020a0822201`
- `docs/concepts/owning-write-hooks.md` (new) — `d0c71eef5de383c0fe6bf1e591645b770401dd75bb513e8189a61d4e975678c4`
- `docs/concepts/typed-row-adapter.md` — `fb551e31a3b59cda085db556556e4da7fc6cdc32d72c8527e07e3ab04a15cd64`

All six match the plan's checkpoint 3 byte-for-byte (re-hashed independently
after the final stage). Pre-existing planning artifacts not touched by this
run: `docs/plans/2026-10-23-phase3-packed-records-plan.md` (modified before
handoff), the blind-review file, and the protected rig (hash-verified only,
never read/edited/moved/staged/executed).

Pending branch-series obligations intentionally not claimed here: sanitizer
gates (after all fixes, per AGENTS.md), raw-diff review, and the remaining
3B/3C children listed in the plan.

## Review and user-authorized commit cleanup — 2026-10-26

Independent executed raw-diff review found zero unresolved findings; see
2026-10-26-phase3b2-raw-diff-review.md. Primary reviewer reran the focused case:
153 passed, 0 failed, exit 0. No code/test edits followed the verified full run.
The human authorized cleanup/commit and confirmed GPT-6.1 as planner and
GLM 5.3 as executor for commit credits. The runtime model identifier recorded
at execution above remains unchanged.

Cleanup synchronizes completion status and links only. All six implementation/
test/concept files still match approved checkpoint 3 byte-for-byte; all 23
embedded executable blocks and hashes remain unchanged. Raw logs, probes,
binaries, build outputs, caches and the protected certification plan are not
commit candidates. No push is authorized; branch-series sanitizers remain
scheduled after all fixes, immediately before push.
