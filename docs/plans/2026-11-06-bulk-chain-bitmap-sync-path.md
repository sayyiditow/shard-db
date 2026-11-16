# Plan: bulk chain commit syncs the wrong bitmap file path (`%03d` vs `%03x`)

Date: 2026-11-06. Planner: Claude Sonnet 5.5. Executor: whichever model the human designates (do not assume).
Found during the blind review of `docs/plans/2026-11-05-bitmap-stale-after-resplit-fix.md`. Independent of that plan; either can land first.

## Execution rules (embedded — follow literally)

- Branch: work directly on `fix/type-correctness-recovery` (single long-lived branch; no sub-branch; never `main`). **Leave all work uncommitted**
  (AGENTS.md). Git write operations only on explicit human request.
- Do tasks in order. Paste real command output for every build/test step.
- If a quoted anchor is not found **exactly once**, write `PLAN_NOTES-bmsync.md` describing the mismatch and **halt the entire run
  immediately** — no guessing, no continuing to later tasks. Resuming requires the human or planning model to hand back a patched plan.
- If you hit a decision the plan does not cover, stop and ask. Never weaken a test to make a failure disappear.
- Build: `SKIP_TESTS=1 ./build.sh`. Tests: `./build/bin/shard-db-test run <name>`, full: `./build/bin/shard-db-test run-all --jobs 4`.
  `rm -rf /tmp/shard-db-*` after every run. Do not run benches.
- Sanitizers: this diff touches no locks, caches, lifetimes or threads → no sanitizer gate needed for this plan alone. If it lands on the same
  branch as the resplit-fix plan, that plan's single Task 9 gate covers it.

## Root cause

Bitmap files are named `<root>/<obj>/indexes/<field>/<NNN>.bm` with `NNN` = the **hex** shard id (`%03x`), built by `bm_build_path`
(`src/db/bitmap.c`) and used by every creator and reader. `req_bitmap_file_path` in `src/db/slotcask.c` (used only by the bulk chain commit
flush) formats the same path with `%03d` (**decimal**). The two agree for shard ids 0–9 and diverge from id 10 on (shard 10 lives in `00a.bm`;
the flush syncs `010.bm`, which is shard 16's file, or a name that does not exist at all).

Consequences:
- The flush calls `fdatasync_path(bpath)`; the raw op converts `ENOENT` to success ("missing file = nothing ever written"), so the missing
  file is silently treated as synced. The coalesced bitmap durability sync is skipped for every shard id ≥ 10 (any object with `splits ≥ 11`).
- For ids whose decimal spelling is also a valid hex name of another shard (e.g. id 16 → `016.bm`, which is shard 22's file), the wrong
  shard's file is synced (harmless but useless).
- Dirty bitmap pages still reach disk through the bm cache's own msync/close paths, so this is a **durability-window** bug (crash after the
  bulk commit returns but before writeback may lose bitmap bits for those shards; startup KFM2 replay re-applies indexed windows, which bounds
  the damage), not an immediate wrong-result bug. Do not claim more than that.
- The non-chain path is correct: `bitmap_sync_shard_path` (`src/db/index.c`) builds the path via `bm_build_path`.

Misleading comment to correct: "mirrors index.c's bm_build_path" is false for the format.

## Call sites (searched: `grep -n req_bitmap_file_path src`)

Definition and exactly one caller, both in `src/db/slotcask.c` (the chain flush loop `if (all[i].type != IT_BITMAP) continue;`).
No other consumers, no signature change, no format change, no external API.

## Invariants / edge cases

- Path produced for any shard id must equal `bm_build_path`'s for the same inputs (single source of truth).
- Shard ids ≥ 4096 do not exist (`MAX_SPLITS`); `%03x` handles up to `fff`.
- Missing file must still be non-fatal (existing `ENOENT` semantics unchanged).
- `PATH_MAX`/1024 buffer unchanged.

## Task 0 — preconditions

- [ ] 0.1 `git status --short`; expect only untracked docs/plans files. Confirm `git branch --show-current` is `fix/type-correctness-recovery`.
- [ ] 0.2 Confirm each anchor below occurs exactly once (`grep -c`); any mismatch → `PLAN_NOTES-bmsync.md`, halt:
  `static void req_bitmap_file_path(char *out, size_t outlen,` ; `snprintf(out, outlen, "%s/%s/indexes/%s/%03d.bm",` ;
  `bm_build_path(char *out, size_t outlen,` in `src/db/bitmap.c`; `TEST_REGISTER("test-durability-epoch-sync", test_durability_epoch_sync_run)`
  (only to confirm the test dir naming); in `build.sh` the line `    src/test/cases/test_predicate_equivalence.c \`.
- [ ] 0.3 Confirm `src/db/slotcask.c` includes `bitmap.h` (grep `#include "bitmap.h"`); if not, add it in Task 2 next to the other quoted includes.

## Task 1 — failing test first

The helper is static, so the unit test needs a visible seam: Task 2 makes it a non-static delegate declared in `src/db/slotcask.h`. To
get a genuinely red run on the base, Task 1 adds the test **and** the declaration only; the definition on the base still uses `%03d`.

- [ ] 1.1 `src/db/slotcask.h`: after the line `int  slotcask_compact_kf(SlotcaskDb *db);` add
```c

/* Path of the bitmap file for (object, field, kf shard) as used by the bulk chain commit flush.
   Must equal bm_build_path() for the same inputs. Exposed for tests. */
void req_bitmap_file_path(char *out, size_t outlen,
                          const char *db_root, const char *object,
                          const char *field, int shard_idx);
```
- [ ] 1.2 `src/db/slotcask.c`: replace `static void req_bitmap_file_path(char *out, size_t outlen,` with `void req_bitmap_file_path(char *out, size_t outlen,`
  (keep the body unchanged for now so the test is red on the base logic).
- [ ] 1.3 Create `src/test/cases/test_bulk_chain_bitmap_path.c`:
```c
/* Red on base: the bulk chain commit flush builds bitmap paths with %03d (decimal) while bm_build_path
   uses %03x (hex), so shard ids >= 10 sync a file that does not exist (ENOENT is treated as success).
   See docs/plans/2026-11-06-bulk-chain-bitmap-sync-path.md. */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "bitmap.h"

#include <stdio.h>
#include <string.h>

static int test_bulk_chain_bitmap_path_run(void) {
    char a[1024], b[1024];
    int mismatches = 0;
    for (int shard = 0; shard < 4096; shard++) {
        req_bitmap_file_path(a, sizeof(a), "/r", "obj", "fld", shard);
        bm_build_path(b, sizeof(b), "/r", "obj", "fld", shard);
        if (strcmp(a, b) != 0) {
            if (mismatches < 3)
                fprintf(stderr, "shard %d: chain=%s canonical=%s\n", shard, a, b);
            mismatches++;
        }
    }
    ASSERT_EQ_INT(mismatches, 0, "chain flush bitmap path equals bm_build_path for every shard id");

    req_bitmap_file_path(a, sizeof(a), "/r", "obj", "fld", 10);
    ASSERT_TRUE(strstr(a, "/00a.bm") != NULL, "shard 10 is spelled 00a.bm");
    req_bitmap_file_path(a, sizeof(a), "/r", "obj", "fld", 4095);
    ASSERT_TRUE(strstr(a, "/fff.bm") != NULL, "shard 4095 is spelled fff.bm");
    return 0;
}

TEST_REGISTER("test-bulk-chain-bitmap-path", test_bulk_chain_bitmap_path_run)
```
- [ ] 1.4 `build.sh`: after the line `    src/test/cases/test_predicate_equivalence.c \` add `    src/test/cases/test_bulk_chain_bitmap_path.c \`
  (if the resplit-fix plan already added `test_bitmap_resplit.c` after that same line, add this one after that instead; one line either way).
- [ ] 1.5 Build and run **red** (paste): `SKIP_TESTS=1 ./build.sh 2>&1 | tail -5; ./build/bin/shard-db-test run test-bulk-chain-bitmap-path; rm -rf /tmp/shard-db-*`
  Expected: fails with 4086 mismatches (4096 − 10 ids that agree) and the stderr lines show e.g. `shard 10: chain=…/010.bm canonical=…/00a.bm` (the `chain=` side is `010.bm`).
  Different failure reason → stop and ask.

## Task 2 — the fix (one source of truth)

- [ ] 2.1 `src/db/slotcask.c`: replace the whole block
```c
/* Bitmap file path for the chain commit flush (mirrors index.c's
   bm_build_path: <root>/<obj>/indexes/<field>/<NNN>.bm). */
void req_bitmap_file_path(char *out, size_t outlen,
                                 const char *db_root, const char *object,
                                 const char *field, int shard_idx) {
    snprintf(out, outlen, "%s/%s/indexes/%s/%03d.bm",
             db_root, object, field, shard_idx);
}
```
  with
```c
/* Bitmap file path for the chain commit flush. Delegates to bm_build_path so the
   shard spelling (%03x, hex) can never drift from the creators/readers. */
void req_bitmap_file_path(char *out, size_t outlen,
                          const char *db_root, const char *object,
                          const char *field, int shard_idx) {
    bm_build_path(out, outlen, db_root, object, field, shard_idx);
}
```
  (The pre-edit text is what Task 1.2 leaves behind; quote it exactly as it stands then.)
- [ ] 2.2 Green (paste): `SKIP_TESTS=1 ./build.sh 2>&1 | tail -5; ./build/bin/shard-db-test run test-bulk-chain-bitmap-path; rm -rf /tmp/shard-db-*`
  Expected: pass, no new warnings.
- [ ] 2.3 Revert proof (paste both): temporarily restore the `%03d` body, rebuild, confirm the test fails with the Task 1.5 reason, restore
  the delegate, rebuild, confirm green again.

## Task 3 — docs

- [ ] 3.1 `docs/reference/changelog.md`: under the Unreleased heading add one bullet: bulk commit flush synced bitmap files by a decimal
  shard spelling, skipping the coalesced durability sync for shard ids ≥ 10 (objects with splits ≥ 11); crash-window only, KFM2 replay re-applies
  indexed windows; no format change. (Quote the Unreleased heading anchor exactly once before editing.)
- [ ] 3.2 No concept-doc change (no documented behavior changes).

## Task 4 — verification

- [ ] 4.1 Related suites (paste summaries): `./build/bin/shard-db-test run-all --filter bulk`, `--filter durability`, `--filter bitmap`;
  `rm -rf /tmp/shard-db-*` after each.
- [ ] 4.2 Full suite: `./build/bin/shard-db-test run-all --jobs 4` (paste the totals line); `rm -rf /tmp/shard-db-*`. All pass.
- [ ] 4.3 `git diff --stat`: expect only `slotcask.c`, `slotcask.h`, `build.sh`, the new test, the changelog (+ this plan untracked).

## Out of scope / follow-up

- The chain flush's metric `g_commit_index_sync_ops_total` still increments when the file is absent (ENOENT → success). Making the counter
  count only real syncs (via the `out_synced` argument of `durability_epoch_fdatasync_path_ex`) is a separate observability change; not done here.
- No end-to-end crash test (it would need a kill between commit return and writeback); the unit test pins the actual defect (path spelling).

## Task 5 — hand-back

- [ ] 5.1 Report: base-red output, green output, revert proof, full-suite totals, `git diff --stat`. Leave uncommitted. Request a blind review of
  the raw diff (touches a durability path, tiny but not exempt).
