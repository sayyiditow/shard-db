# Fix plan 7: restore commit point, rollback, fields.conf, keyfile dir-sync flag (2026-11-26)

Status: APPROVED by the human 2026-11-26 (v3.2; v3.3 execution correction
applied 2026-11-26 — see the Review log) (rewrite after two blind rounds on v1/v2; v3 rounds 1–2 folded; review loop converged), ready
for approval. Source: pre-merge blind review of the audit
branch (AUD-RESTORE-B1/M1/M2, AUD-KF-DIRSYNC, index
`2026-11-19-audit-index.md`). Every defect is in code that exists only on this
unmerged branch (stage-then-swap restore, 8bbc821; dir-sync gate, 351c086). No
source edited yet.

## Brief

**Problem and root cause.**
(1) **B1** `cmd_restore` (`query_maint.c`) ends with `rmrf(stage)` while
`stage/SWAP` still exists; a kill mid-delete leaves SWAP next to a half-deleted
`old/`, and `maint_restore_swap_recover` then deletes the live (restored) tree
and renames the half-deleted original in: both copies lost. There is no commit
point. (2) **M1** a failed final `fsync_dir(obj_dir)` replies "failed" but
leaves the restored tree live, SWAP kept, lock released; later writes are
acknowledged and then dropped by the next rollback. (3) **M2**
`ensure_schema_conf_line` and an unchecked `copy_file(fields.conf)` run before
staging, so a staging failure lies ("live object was not changed") and no
rollback restores `fields.conf`. Rollbacks also delete before they rename,
ignore every error, and leave a live-absent subtree (`files/`) restored;
startup ignores recovery failure. (4) **KF-DIRSYNC** the "rename not yet
durable" flag lives in the kfcache entry and is lost on the next acquire
(Phase E never updates `file_dev/file_ino`, so the entry is discarded and
reinstalled clean), on eviction and on prefix invalidation.

**Logical flow (restore).** lock → finish/refuse leftovers (recovery now
returns an error) → all refusals (no mutation) → stage copy (+ staged
`fields.conf`) → create placeholder dirs for absent live subtrees → durable
SWAP → renames (live→`old/`, staged→live) → fsync obj/old/new dirs → **commit
= unlink SWAP + fsync stage** → install missing `fields.conf`/schema line →
invalidate caches (now incl. bitmap cache) → delete stage → reply. Any failure
after SWAP: state-driven rollback that renames before it deletes; if the
rollback itself fails the daemon aborts and startup finishes it; startup
refuses to serve if it cannot.
**Logical flow (kf).** pending directory syncs move out of the cache entry
into a small process-wide table keyed by keyfile path (survives every drop);
Phase E records the new inode.

**Decisions.**

| # | Decision | Alternatives | Why | Tag |
|---|---|---|---|---|
| D1 | Full scope; executor DeepSeek (exact model name/version to be given by the human), run separately from this session; Claude plans and reviews the diff, and does not execute (changed 2026-11-26 from Claude because of usage limits); unreferenced raw audit logs deleted | Claude executes | human decision per CORE-PROCESS step 3 | DECIDED |
| D2 | **Fail-stop (`abort()`) when an in-process rollback cannot complete**; restart recovery finishes it; startup refuses if it cannot | hold the object wrlock forever (hangs every client and shutdown); in-memory quarantine (no single gate covers every mode; `.bmfix-disabled` proven insufficient) | after a failed fsync/rename the state is unknowable; crash-and-recover is the only path that never serves a half-swapped object. Cost: an embedded host process dies too | **DECIDED 2026-11-26: abort is accepted** (recommend fail-stop) |
| D3 | Commit point = durable SWAP removal, after fsync of obj dir + `old/` + `new/` | one fsync only | POSIX rename durability needs both directories | DEFAULT |
| D4 | State-driven rollback: "the original is at `old/<name>` if present, else live"; placeholders make it total; restored copies go to `rb/` by rename | per-step journal | no new on-disk format, idempotent, crash-safe at every step | DEFAULT |
| D5 | `fields.conf` swapped when both exist; installed after commit (rename from stage) when live is missing; schema line appended after commit, checked + fsynced; refuse before mutation if a missing line cannot be valid (storage_version≠2 or streams∉[1,16], closes AUD-P3-6 on this path) | keep pre-staging installs | M2; keeps the pre-schema-migration rollback (backup.md) working | DEFAULT |
| D6 | Post-commit install failure → error reply naming the file, return 1 (no token merge), data stays restored; re-run with force | roll back | commit already durable; re-run is idempotent | DEFAULT |
| D7 | KF pending table outside the entry (64 slots, sticky overflow = sync dir on every kf sync) | gate each drop (v2 draft) | prefix-invalidate and DISCARD cannot refuse; EVICT refusal pins entries and hot-loops | DEFAULT |
| D8 | Restore also drops bitmap-cache entries (found while planning: no inode check without a publish-generation change) | leave | stale bitmap reads after restore | DEFAULT |
| D9 | `force` with differing splits/max_key/streams unchanged (AUD-RESTORE-SPLITS) | refuse | auto-reshard makes older backups unrestorable | DEFAULT |
| D10 | include-audit check: in-process argv-parse + selection count instead of a nested run (moves the parser into `test_runner.c`) | nested `list` | `list` takes no flags | DEFAULT |
| D11 | Sanitizer gate not run here: once for the whole branch before push (3× ASan, 3× TSan: kfcache state + new mutex) | run now | AGENTS.md rule | DEFAULT |

**Blast radius.** `cmd_restore` replies: one changed string (directory sync
now ends "rolled back, the live object was not changed"), new strings (see
T6), a daemon abort on an unrecoverable rollback, a refusal while an
unrecoverable leftover exists. `maint_restore_swap_recover` `void`→`int`
(callers: `embedded.c` startup, `cmd_restore`, `test_backup_restore_errors.c`);
startup may now refuse. `server.c` unchanged (JSON branch already skips the
token merge on 1; text branch shares `cmd_restore`). `KfCacheEntry.dir_unsynced`
removed (internal). Staging dir name `.restore-<pid>` → `.restore-<pid>-<n>`,
new `rb/` inside it (unreleased format). No on-disk data, config or CLI change.

**Test strategy.** Crash at P2/P3 park points + restart (B1, rollback);
injected dir-sync / rename / marker faults (M1, fail-stop); crafted leftover
stages (startup and restore refusal); fields.conf/schema-line faults (M2);
bitmap count after restore (D8); kf flag survives reacquire / eviction /
invalidate (KF). Every regression test red on base (+seams), green after.

**Out of scope.** AUD-RESTORE-SPLITS, AUD-BT-REOPEN, other production minors
in the index, schema.conf concurrent-rewrite interplay, pruning of pending-table
entries for dropped objects (perf only), legacy-removal refactor, Phase 3.

**Review log.** v1/v2: 2 blind rounds; findings folded here: drop-object
deletes `backup/`; `.bmfix-disabled` cannot fence a half-swap; constant stage
name + `rfail` `rmrf`; void recovery ignored at startup; rollback
deletes-before-rename; unknown durability after a failed `fsync_dir(stage)`;
live-absent subtrees; bitmap re-arm only on success; cached "missing line";
unchecked schema writes; seams via the control channel; eviction-test sizing;
four `force` uses; text protocol. v3: design changed (D4, D7, D8).
**v3 round 1 (blind, full):** 1 silent-damage blocker, 2 clarity majors, 5
minors, all fixed in v3.1: (B) a crash while deleting a rolled-back stage
could leave SWAP with no `old/`, and the rollback's `fsync_dir(old)` (ENOENT)
then made startup refuse a valid root → the rollback is a no-op when `old/`
is absent, `rb/` is synced only if used, and every post-rollback delete is
preceded by an "uncommit" (unlink SWAP + fsync stage); rows 4/11 corrected,
row 11a added, tests added (T1, T2). (M) the T5 artifact check matched this
plan itself → scoped grep with named allowed citers. (M) T4 sanitizer-block
move left a duplicate stub and an out-of-scope `rs_size` → spelled out, plus
compile checks in both sanitizer modes. Minors: I4 on the `cmd_restore`
recovery path (fail-stop when its rollback failed after progress),
`rs_wait_exit` arity/handle reset, core dumps off in abort tests, fsync-error
note at A1, eviction halt note kept. **Round 2 (delta):** 0 blockers, 0
silent-damage majors; clarity items fixed in v3.2: grep option order in T5,
`rs_upsert` moved with `rs_size` in T4, I4 wording + row 13, recovery
preserves the first failure's errno, T2 makes the registry invalidation
unconditional, a stray `.restore-*` file is skipped (not a startup refusal;
test added), `<sys/resource.h>`, placeholder note in docs. **Review loop
converged.**
**Execution correction (2026-11-26, v3.3):** T2's
`test-restore-startup-refuses-unrecoverable-stage` is a guard after T1, not
red on base: T1's startup-refusal contract (`restore_recover_impl` returns
-1; `embedded.c` returns -1 on `< 0`) already makes it green at the T2
red-proof point. Evidence and the halt record are in `PLAN_NOTES.md`. No
behaviour change; the test itself is unchanged.

---

## Plan

### Execution rules

- Branch `test/audit-oracle-harnesses` (no new branch; human decision). Leave
  all work uncommitted; never `git add/commit/push`. Delete files with plain
  `rm`.
- Build: `SKIP_TESTS=1 ./build.sh 2>&1 | tee /tmp/claude-build.log`, then
  `grep -E " error| warning" /tmp/claude-build.log` must print nothing new
  (never judge a build by `tail`). Single case:
  `./build/bin/shard-db-test run <name>`. Full suite:
  `./build/bin/shard-db-test run-all --jobs 8` (watch `free -h`; drop to 4 on
  OOM). After every run: `rm -rf /tmp/shard-db-*`.
- Tasks in order T0…T6. In T1–T5 every test of the task is written and run
  **before** the task's production change; paste the red output, then the
  green output after the change. A test that passes on base is marked
  "guard" below; if a test marked red passes on base, record it in
  `PLAN_NOTES.md` and halt (except D8's test: record and continue).
- If the code does not match a statement in this plan (symbol missing,
  different behaviour), write `PLAN_NOTES.md` describing the mismatch and halt.
  A symbol that merely moved: proceed and note it. A decision this plan does
  not cover: stop and ask.
- New daemon tests use the TEST_BUILD daemon (`shard-db-test-server`, picked by
  the fixtures) and assert `env.test_control_fd >= 0`. Restarts:
  `test_env_kill` (crash) or `test_env_stop_keep` (clean), then
  `test_env_start_at(&env, env.db_root, env.port)`; cleanup `test_env_stop`.
- Never count `fsync_dir` calls by order (recovery and `cprf` call it too);
  every seam below is a named site.

### Invariants (restore)

- **A1 (assumption)** directory operations become durable in issue order
  (journaled filesystems); the explicit fsyncs bound which prefix is durable.
  Note: on Linux a failed fsync consumes the error, so a later successful
  `fsync_dir` of the same directory (e.g. the SWAP re-create in commit step
  2) does not prove the earlier metadata reached disk; on journaled
  filesystems directory metadata is committed in order with the journal, so
  this is accepted.
- **I1** SWAP is durable before the first rename and is removed only after
  every rename is durable (fsync of obj dir, `old/`, `new/`).
- **I2** every name in the swap set exists live when SWAP becomes durable
  (placeholders for absent dirs; `fields.conf` is in the set only when it
  exists live). Hence: for any name, the original tree is at `old/<name>` if
  that exists, otherwise at the live path.
- **I3** no step deletes an original before commit: rollback moves the live
  (restored) entry to `rb/<name>` by rename, then `old/<name>` back; `old/` is
  deleted only with a stage that has no SWAP, i.e. after commit or after a
  durable rollback that emptied it.
- **I4** the object lock is never released while a stage with a durable SWAP
  and a non-empty `old/` exists **and the live tree may differ from the
  original**: the call commits, rolls back durably, or aborts. Exception, on
  `cmd_restore`'s own recovery call: a rollback that fails after moving
  something in this call calls `restore_fail_stop`; one that fails with no
  progress leaves the live tree as recovery found it (possibly part-rolled-back
  by an earlier process that died), and refusing + unlocking is accepted
  because a stage with names in `old/` can survive to `cmd_restore` only on an
  object startup did not visit (no schema.conf line, so not servable) or a
  hand-made one (table row 13).
- **I6** no stage that still holds SWAP is deleted: every post-rollback
  delete first "uncommits" (unlink SWAP, `fsync_dir(stage)`); if that fails
  the stage is kept (it then holds SWAP with no names in `old/`, which the
  rollback treats as a no-op). The rollback never fsyncs a directory that may
  be absent (`old/` is checked first, `rb/` only if used).
- **I5** recovery returns -1 only after it stopped at a failed step, leaving
  the stage (SWAP, `old/`, `rb/`) in place; startup and `cmd_restore` refuse
  on -1.

### Crash-interleaving table (state after a crash/abort at each point, and what recovery does)

Stage `S = <obj>/.restore-<pid>-<n>`; "R" = `restore_rollback_swap`.

| # | Crash point | Disk state recovery sees | Recovery | Result |
|---|---|---|---|---|
| 1 | before `mkdir(S)` | no stage | — | pre-restore |
| 2 | during staging copy / syncs | S, no SWAP, live untouched | `rmrf(S)` | pre-restore |
| 3 | during placeholder mkdirs | as 2 + empty live dirs | `rmrf(S)` | pre-restore + inert empty dirs |
| 4 | SWAP written, maybe not durable; or crash inside the pre-SWAP failure exit (uncommit, then `rmrf(S)`) | S ± SWAP, no `old/<name>`; `old/` may be empty or absent | SWAP: R (returns 0 when `old/` is absent or holds no names) → uncommit → `rmrf(S)`; no SWAP: `rmrf(S)` | pre-restore |
| 5 | mid-renames (any prefix) | SWAP; per name: (a) original live, (b) original in `old/`, live absent, (c) original in `old/`, restored live | R: (c) live→`rb/`, `old`→live; (b) `old`→live; (a) nothing | pre-restore |
| 6 | after renames, during commit fsyncs (P2) | as 5(c) for all names | R | pre-restore |
| 7 | after `unlink(SWAP)`, before/at `fsync_dir(S)` | renames durable; SWAP present or absent | present: R → pre-restore; absent: `rmrf(S)` → restored | either is consistent; no reply was sent |
| 7b | failed `fsync_dir(S)`, SWAP recreated (or recreate fails → abort) | SWAP present or absent | as 7 | consistent; no "success" reply |
| 8 | after commit, during `fields.conf`/schema-line install | no SWAP | `rmrf(S)` | restored; a missing file/line may still be missing → re-run restore force (as before the restore, the object lacked it) |
| 9 | during final `rmrf(S)` (P3 = just before) — **B1** | no SWAP, partial `old/` | `rmrf(S)` | restored (fixed) |
| 10 | in-process R mid-way (after any failure past SWAP) | SWAP durable (never removed, or recreated durably in 7b) | R continues from any prefix | pre-restore |
| 11 | R complete, during the uncommit (after `unlink(SWAP)`, before/at `fsync_dir(S)`) | SWAP present or absent; `old/` present with no names; `rb/` holds restored copies | SWAP: R no-op (0) → uncommit → `rmrf(S)`; none: `rmrf(S)` | pre-restore |
| 11a | uncommit done, during `rmrf(S)` | no SWAP (durable), any subset of `old/`, `new/`, `rb/` | `rmrf(S)` | pre-restore |
| 13 | an earlier R (other process) moved live→`rb/<name>` and died; `cmd_restore`'s recovery then fails with no progress (only possible for an object startup did not visit, or a hand-made stage) | SWAP, `old/<name>` present, live `<name>` absent | refuse + unlock (I4 exception) | object unchanged by this call; not servable without its schema line; next start or restore retries |
| 11b | uncommit failed (stage kept) | SWAP, `old/` without names | as 11 at the next start or restore | pre-restore |
| 12 | R fails in-process → `abort()` | as 10 | startup R; if it fails → startup refuses, S untouched | pre-restore, or operator action (docs) |

Re-walk for I6 (round 1): the only `rmrf` of a stage that can still hold
SWAP is preceded by the uncommit in every path (pre-SWAP exit, row 4;
`restore_abandon`, rows 10–11; recovery after R = 0, rows 5–6 and 11); the
success path deletes only after the commit removed SWAP (rows 8–9); the
directories R fsyncs are the object dir, `old/` (checked to exist first) and
`rb/` (only after a rename into it in the same call), so no row can make R
fail on a vanished directory.

A stage of an object whose schema.conf line is missing is not visited by
startup (startup walks schema.conf); `cmd_restore`'s own recovery call handles
it, and the object is unusable without its line anyway (index note, T6).

---

### T0 Seams and test helpers on the base tree (no behaviour change)

1. `src/db/shard_test_ctl.h` (inside `#ifdef TEST_BUILD`), add:
   ```c
   /* Restore fault seams (query_maint.c); one-shot per bit. */
   enum {
       SHARD_TEST_RF_COMMIT_DIRSYNC   = 1,   /* commit: dir syncs after the renames */
       SHARD_TEST_RF_UNSWAP_DIRSYNC   = 2,   /* commit: fsync_dir(stage) after unlink(SWAP) */
       SHARD_TEST_RF_SWAP_RECREATE    = 4,   /* re-creating SWAP after 2 failed */
       SHARD_TEST_RF_ROLLBACK_RENAME  = 8,   /* rollback: rename(old/<name>, live) */
       SHARD_TEST_RF_ROLLBACK_DIRSYNC = 16,  /* rollback: closing dir syncs */
       SHARD_TEST_RF_SWAP_RENAME      = 32,  /* forward swap: rename(new/<name>, live) at index */
       SHARD_TEST_RF_STAGE_COPY       = 64,  /* staging copy fails before any copy */
       SHARD_TEST_RF_SCHEMA_INSTALL   = 128  /* schema.conf line install fails */
   };
   enum { SHARD_TEST_RESTORE_PARK_SWAPPED = 1, SHARD_TEST_RESTORE_PARK_COMMITTED = 2 };
   extern _Atomic int g_restore_test_fault_mask;
   extern _Atomic int g_restore_test_swap_rename_at;   /* 0-based swap-name index */
   void shard_db_test_set_restore_park_hook(int site, shard_db_test_gate_fn fn, void *ctx);
   ```
   (`shard_db_test_gate_fn` is declared further down in the same header: put
   the prototype after that typedef.) In `shard_test_ctl_reset` add
   `atomic_store(&g_restore_test_fault_mask, 0); atomic_store(&g_restore_test_swap_rename_at, 0);`.
2. `src/db/query_maint.c`: add `#include "shard_test_ctl.h"` under
   `#ifdef TEST_BUILD`, and this block above `copy_file_sparse`:
   ```c
   #ifdef TEST_BUILD
   _Atomic int g_restore_test_fault_mask;
   _Atomic int g_restore_test_swap_rename_at;
   static int restore_test_fault(int bit) {
       int m = atomic_load(&g_restore_test_fault_mask);
       while (m & bit)
           if (atomic_compare_exchange_weak(&g_restore_test_fault_mask, &m, m & ~bit)) {
               errno = EIO; return 1;
           }
       return 0;
   }
   static int restore_test_swap_rename_fault(size_t i) {
       if ((int)i != atomic_load(&g_restore_test_swap_rename_at)) return 0;
       return restore_test_fault(SHARD_TEST_RF_SWAP_RENAME);
   }
   static pthread_mutex_t g_restore_park_lock = PTHREAD_MUTEX_INITIALIZER;
   static int g_restore_park_site;
   static shard_db_test_gate_fn g_restore_park_fn;
   static void *g_restore_park_ctx;
   void shard_db_test_set_restore_park_hook(int site, shard_db_test_gate_fn fn, void *ctx) {
       pthread_mutex_lock(&g_restore_park_lock);
       g_restore_park_site = fn ? site : 0; g_restore_park_fn = fn; g_restore_park_ctx = ctx;
       pthread_mutex_unlock(&g_restore_park_lock);
   }
   static void restore_test_park(int site) {   /* one-shot, mirrors shard_db_test_guard_park */
       shard_db_test_gate_fn fn = NULL; void *ctx = NULL;
       pthread_mutex_lock(&g_restore_park_lock);
       if (g_restore_park_fn && g_restore_park_site == site) {
           fn = g_restore_park_fn; ctx = g_restore_park_ctx;
           g_restore_park_fn = NULL; g_restore_park_ctx = NULL; g_restore_park_site = 0;
       }
       pthread_mutex_unlock(&g_restore_park_lock);
       if (fn) fn(ctx);
   }
   #else
   #define restore_test_fault(bit) 0
   #define restore_test_swap_rename_fault(i) 0
   #define restore_test_park(site) ((void)0)
   #endif
   ```
3. Instrument the **base** `cmd_restore` / `maint_restore_swap_recover` (these
   sites move in T1–T3; each task re-places them at the named new site):
   - RF_STAGE_COPY: immediately before the staging loop
     `for (size_t i = 0; !rfail && i < nsubs; i++)`:
     `if (!rfail && restore_test_fault(SHARD_TEST_RF_STAGE_COPY)) { rfail = "staging copy"; rerrno = EIO; }`.
   - RF_SWAP_RENAME: in the forward swap loop, the condition
     `stat(fresh, &st) == 0 && rename(fresh, live) != 0` becomes
     `stat(fresh, &st) == 0 && (restore_test_swap_rename_fault(swapped) || rename(fresh, live) != 0)`.
   - RF_ROLLBACK_RENAME: in the "Roll back every completed swap, newest first"
     loop and in `maint_restore_swap_recover`, `(void)rename(aside, live);`
     becomes `if (!restore_test_fault(SHARD_TEST_RF_ROLLBACK_RENAME)) (void)rename(aside, live);`
     (not the inner "put this one back" rename).
   - RF_COMMIT_DIRSYNC: the `if (fsync_dir(obj_dir) != 0) {` right after the
     `if (rfail) { ... }` block becomes
     `if (restore_test_fault(SHARD_TEST_RF_COMMIT_DIRSYNC) || fsync_dir(obj_dir) != 0) {`.
   - RF_SCHEMA_INSTALL: `if (ensure_schema_conf_line(...) != 0)` becomes
     `if (restore_test_fault(SHARD_TEST_RF_SCHEMA_INSTALL) || ensure_schema_conf_line(...) != 0)`.
   - Park P2: `restore_test_park(SHARD_TEST_RESTORE_PARK_SWAPPED);` between
     the `if (rfail) {...}` block and that `fsync_dir(obj_dir)` check.
   - Park P3: `restore_test_park(SHARD_TEST_RESTORE_PARK_COMMITTED);`
     immediately before the success path's `rmrf(stage);`.
4. `maint_restore_swap_recover` returns `int` (prototype in `types.h`): 1 when
   it processed at least one stage with SWAP, else 0 (no -1 yet). `embedded.c`
   `shard_db_recover_before_stamp`: `(void)maint_restore_swap_recover(obj_dir);`;
   `cmd_restore`: `(void)maint_restore_swap_recover(obj_dir);`.
5. `src/db/test_control.c`: `#define TEST_HOOK_SET_GLOBALS_RESTORE_FAULT 14`
   (a = mask, b = swap-rename index; store `b` first, then the mask);
   `#define TEST_HOOK_KIND_RESTORE_PARK_SWAPPED 5`,
   `#define TEST_HOOK_KIND_RESTORE_PARK_COMMITTED 6`. Add a park function per
   kind with the exact body of `test_control_guard_block` but REACHED `.phase`
   = 5 / 6 (a shared static helper taking the phase is fine). In the INSTALL
   switch, before the final `else`, map kind 5 → `shard_db_test_set_restore_park_hook(SHARD_TEST_RESTORE_PARK_SWAPPED, <fn5>, c)`
   and kind 6 → `..._COMMITTED, <fn6>`. `TEST_HOOK_CLEAR` and
   `shard_db_test_control_stop` also call
   `shard_db_test_set_restore_park_hook(0, NULL, NULL)`.
6. `src/test/fixtures.c/.h`: `int test_env_ctl_set_restore_fault(TestEnv *env, int mask, int swap_index);`
   = `ctl_set_globals(env, 14, (uint64_t)mask, (uint64_t)swap_index)`.
7. `src/db/slotcask.c/.h` (TEST_BUILD, next to the other `slotcask_test_*`
   declarations): `int slotcask_test_kfcache_resident(const char *path)` (0
   when `!g_db || !g_kfcache`; else lock `g_kfcache_lock`, `kfcache_probe`,
   unlock, return found) and `void slotcask_test_kf_dirsync_reset(void)` (empty
   body now; filled in T4).
8. Build, full suite green (paste the summary line): T0 changes no behaviour.

### T1 Recovery contract and the rollback helper

Tests first (in-process, `src/test/cases/test_backup_restore_errors.c`; arm
seams by storing `g_restore_test_fault_mask` directly, disarm on every path):
- `test-restore-swap-rollback` (existing): assert the first
  `maint_restore_swap_recover(base)` returns 1 and the unmarked one returns 0.
  Guard.
- `test-restore-recover-rename-failure-keeps-stage`: fake object dir with
  `data/new.txt` ("restored"), `.restore-77/SWAP`, `.restore-77/old/data/orig.txt`;
  arm RF_ROLLBACK_RENAME; recover returns -1; `.restore-77/SWAP` and
  `.restore-77/old/data/orig.txt` exist. Disarm; recover returns 1;
  `data/orig.txt` exists, `data/new.txt` and `.restore-77` do not. Red on base
  (base deletes live, skips the rename, deletes the stage: `orig.txt` gone).
- `test-restore-recover-mixed-swap-states`: SWAP stage with: `old/data/o` +
  live `data/r` (state c); `old/indexes/o` + live `indexes` absent +
  `new/indexes/r` (state b); live `metadata/o` + `new/metadata/r`, no
  `old/metadata` (state a); `old/fields.conf` ("orig") + live `fields.conf`
  ("restored"). After recover (returns 1): `data/o`, `indexes/o`, `metadata/o`
  present, no `r` files live, `fields.conf` reads "orig", stage gone. Red on
  base (`fields.conf` not rolled back).
- `test-restore-recover-refuses-non-dir-rb`: SWAP stage with `old/data/o`,
  live `data/r`, and a regular file `.restore-79/rb`; recover returns -1;
  `old/data/o` and live `data/r` unchanged; stage intact. Red on base (base
  returns 1 and moves the data).
- `test-restore-recover-swap-without-old` (round-1 blocker): (i) fake object
  dir with live `data/o` and `.restore-80/SWAP` only (no `old/`, no `rb/`);
  (ii) `.restore-81/SWAP` with an empty `old/` and no `rb/`. Recover returns
  0 or 1 (assert `>= 0`), both stages are gone, live `data/o` unchanged.
  Guard on base (base just deletes them); red against the v3 design.
- `test-restore-recover-skips-stray-file`: fake object dir with live `data/o`
  and a regular file `.restore-stray`; recover returns 0, the file still
  exists, `data/o` unchanged. Red on base for "file still exists" (base
  unlinks it) — intended change; not a regression proof.

Change (`query_maint.c`), replacing the base recovery and inline rollbacks:
```c
#define RESTORE_STAGE_PREFIX ".restore-"
/* Every name a swap may move, in swap order. Rollback/recovery act only on
   names that have <stage>/old/<name> (I2). */
static const char *const RESTORE_SWAP_NAMES[] =
    { "data", "indexes", "metadata", "files", "fields.conf" };
#define RESTORE_NSWAP (sizeof(RESTORE_SWAP_NAMES) / sizeof(RESTORE_SWAP_NAMES[0]))

/* State-driven rollback (I2/I3/I6): for each name with old/<name>, move the
   live entry (a restored copy) to rb/<name>, then old/<name> back. Idempotent
   from any crash prefix. 0 = rolled back durably (or nothing to roll back);
   -1 = stopped (errno set). *progress is set to 1 when this call renamed
   anything (caller may pass NULL). */
static int restore_rollback_swap(const char *obj_dir, const char *stage, int *progress) {
    char rb[PATH_MAX], old[PATH_MAX];
    int moved_trash = 0;
    if (progress) *progress = 0;
    if (snprintf(rb, sizeof(rb), "%s/rb", stage) >= (int)sizeof(rb) ||
        snprintf(old, sizeof(old), "%s/old", stage) >= (int)sizeof(old)) {
        errno = ENAMETOOLONG; return -1;
    }
    struct stat st;
    /* No old/ at all: nothing was ever moved aside, or a cleanup already
       emptied it (table rows 4, 11): nothing to roll back, no syncs. */
    if (lstat(old, &st) != 0) return errno == ENOENT ? 0 : -1;
    if (mkdir(rb, 0755) != 0) {
        if (errno != EEXIST) return -1;
        if (lstat(rb, &st) != 0) return -1;
        if (!S_ISDIR(st.st_mode)) { errno = ENOTDIR; return -1; }
    }
    for (size_t k = RESTORE_NSWAP; k-- > 0; ) {
        const char *name = RESTORE_SWAP_NAMES[k];
        char aside[PATH_MAX], live[PATH_MAX], trash[PATH_MAX];
        if (snprintf(aside, sizeof(aside), "%s/%s", old, name) >= (int)sizeof(aside) ||
            snprintf(live, sizeof(live), "%s/%s", obj_dir, name) >= (int)sizeof(live) ||
            snprintf(trash, sizeof(trash), "%s/%s", rb, name) >= (int)sizeof(trash)) {
            errno = ENAMETOOLONG; return -1;
        }
        if (lstat(aside, &st) != 0) { if (errno == ENOENT) continue; return -1; }
        if (lstat(live, &st) == 0) {
            if (lstat(trash, &st) == 0) rmrf(trash);      /* only ever a restored copy */
            else if (errno != ENOENT) return -1;
            if (rename(live, trash) != 0) return -1;
            moved_trash = 1;
            if (progress) *progress = 1;
        } else if (errno != ENOENT) {
            return -1;
        }
        if (restore_test_fault(SHARD_TEST_RF_ROLLBACK_RENAME) || rename(aside, live) != 0)
            return -1;
        if (progress) *progress = 1;
    }
    if (restore_test_fault(SHARD_TEST_RF_ROLLBACK_DIRSYNC) || fsync_dir(obj_dir) != 0 ||
        fsync_dir(old) != 0 || (moved_trash && fsync_dir(rb) != 0))
        return -1;
    return 0;
}

/* I6: make "no SWAP" durable before deleting a rolled-back (or never-swapped)
   stage, so a crash mid-delete cannot leave SWAP behind a vanished old/.
   On failure the stage is kept: it holds SWAP and no names in old/, which
   the next rollback treats as a no-op. */
static void restore_uncommit_and_remove(const char *stage) {
    char mark[PATH_MAX];
    if (snprintf(mark, sizeof(mark), "%s/SWAP", stage) >= (int)sizeof(mark)) return;
    if (unlink(mark) != 0 && errno != ENOENT) {
        LOG_WARN(LOG_SUB_CONFIG, "RESTORE %s: cannot remove SWAP (%s); staging kept", stage, strerror(errno));
        return;
    }
    if (fsync_dir(stage) != 0) {
        LOG_WARN(LOG_SUB_CONFIG, "RESTORE %s: cannot sync the SWAP removal (%s); staging kept", stage, strerror(errno));
        return;
    }
    rmrf(stage);
}

/* 1 = at least one interrupted swap rolled back; 0 = nothing to roll back;
   -1 = a SWAP stage could not be rolled back (kept for the next attempt and
   the operator, I5). Stages without SWAP are deleted (before SWAP: nothing
   moved; after commit: old/ is the superseded tree). */
static int restore_recover_impl(const char *obj_dir, int *out_progress);
int maint_restore_swap_recover(const char *obj_dir) { return restore_recover_impl(obj_dir, NULL); }
```
`*out_progress` (when non-NULL) is set to 1 only when a stage's
`restore_rollback_swap` **failed after reporting progress** (the live tree may
then be part-rolled-back, I4); a stage that rolled back fully does not set it.
Body of `restore_recover_impl` (every call to `restore_rollback_swap` passes a
local progress int): `opendir(obj_dir)`; ENOENT/ENOTDIR → 0;
other failure → LOG_ERROR, -1. Collect every entry name starting with
`RESTORE_STAGE_PREFIX` into a heap array first (realloc; OOM → -1), `closedir`,
then for each: first `lstat(<stage>)`; if it is not a directory (a stray file
named `.restore-*`), `LOG_WARN` and skip it — neither fail nor delete; `lstat`
error ENOENT → skip; other error → failed. Then `lstat(<stage>/SWAP)`; present → `restore_rollback_swap`; on -1
`LOG_ERROR(LOG_SUB_CONFIG, "RESTORE-RECOVER %s: interrupted restore %s could not be rolled back: %s; staging kept (see docs/operations/backup.md)", ...)`,
mark failed, **do not** `rmrf`, continue; on 0 LOG_WARN as today, mark rolled
back, `restore_uncommit_and_remove(stage)` (never a bare `rmrf` of a SWAP
stage, I6). SWAP `lstat` error other than ENOENT → failed, keep. No
SWAP → `rmrf(stage)`. If anything was removed, `(void)fsync_dir(obj_dir)`.
Save the errno of the first failing stage (before logging) and restore it
right before returning -1, so `cmd_restore` step 1 reports the real cause.
Return `failed ? -1 : rolled_back`.

Callers: `embedded.c` `shard_db_recover_before_stamp`: on `< 0`
`LOG_ERROR(... "RESTORE-RECOVER %s/%s: startup refused", ...)`, also
`fprintf(stderr, ...)`, `free(rents)`, `return -1`. `cmd_restore` (T2).
Green: the four in-process tests.

### T2 Restore commit point, rollback paths, fail-stop, leftovers

Tests first: new file `src/test/cases/test_restore_swap_commit.c` (add it to
the explicit case list in `build.sh` next to `test_backup_restore_errors.c`).
Shared static helpers in the file: `rs_req(tc, fmt, ...)` (returns the reply),
`rs_backup(tc, obj, from_out)` (backup, extract the basename of `"path"`),
`rs_find_stage(obj_dir, out)` (count of `.restore-*` entries, first name),
`rs_wait_exit(TestEnv *env, int ms, int *status)` (polls
`waitpid(env->daemon_pid, status, WNOHANG)` every 50 ms; on exit sets
`env->daemon_pid = -1`, closes `env->test_control_fd` and sets it to -1, returns
1; returns 0 if still running at the deadline), `rs_setup(tc, obj)`:
create-object `{"splits":8,"max_key":16,"fields":["name:varchar:16"],"indexes":["name"]}`,
insert k1 `alpha`, backup → `from`, then `update` k1 → `beta`, insert k2
`gamma`. "Pre-restore state" = k1 `beta`, `exists` k2 `true`, count 2,
`count` with criteria `name eq beta` = 1; "restored state" = k1 `alpha`, k2
`false`, count 1. The two abort cases (`...-aborts`, `...-rollback-rename-fails`)
first call `setrlimit(RLIMIT_CORE, &(struct rlimit){0, 0})` (`#include <sys/resource.h>`; inherited by the
spawned daemon; it only lowers a limit of the test process) so an expected
`abort()` writes no core file. Every restore below is `{"mode":"restore",...,"from":<from>,"force":true}`.

- `test-restore-commit-before-cleanup` (B1): install kind 6; `tc_send` the
  restore; `test_env_test_hook_wait` → phase 6; assert one stage, no
  `<stage>/SWAP`, `<stage>/old/data` exists; `rm -rf <stage>/old/data/kf`
  (simulates the half-done delete); `test_env_kill`; restart; restored state,
  no stage. Red on base (SWAP present; recovery installs the gutted tree).
- `test-restore-crash-after-renames-rolls-back`: kind 5, kill at P2, restart →
  pre-restore state, no stage. Guard.
- `test-restore-commit-dirsync-failure-rolls-back` (M1): arm
  RF_COMMIT_DIRSYNC; reply contains `restore failed at directory sync` and
  `rolled back`; pre-restore state; no stage; a second (clean) restore →
  restored state. Red on base. Round-1 variant in the same case, after the
  clean restore: craft the leftovers a crash during rolled-back-stage cleanup
  can leave — `<obj>/.restore-crafted1/SWAP` alone, and
  `<obj>/.restore-crafted2/SWAP` + empty `old/` (no `rb/`) —
  `test_env_stop_keep`, restart: the daemon starts, restored state, no
  `.restore-*` left. Guard on base; red against the v3 design.
- `test-restore-unswap-sync-failure-rolls-back`: arm RF_UNSWAP_DIRSYNC; reply
  contains `restore failed at swap marker sync` and `rolled back`; pre-restore
  state; no stage. Red on base (base succeeds).
- `test-restore-unrecoverable-rollback-aborts`: arm
  RF_COMMIT_DIRSYNC|RF_ROLLBACK_RENAME; the restore gets no `"status"` reply;
  `int st; rs_wait_exit(&env, 5000, &st)` = 1 with `WIFSIGNALED(st) && WTERMSIG(st) == SIGABRT`;
  restart → pre-restore state,
  no stage. Second variant in the same case after that: arm
  RF_UNSWAP_DIRSYNC|RF_SWAP_RECREATE → abort; the unlink of SWAP survives a
  process abort (page cache), so after restart the restored state, no stage.
  Red on base
  (no abort; `rs_wait_exit` returns 0 — then `test_env_stop` and fail).
- `test-restore-swap-failure-rollback-rename-fails`: arm RF_SWAP_RENAME at
  index 2 (`metadata`) | RF_ROLLBACK_RENAME → abort; restart → pre-restore
  state, `<obj>/indexes` is a directory, the indexed count is 1. Red on base
  (no abort; base's rollback loop deletes live `indexes/` and loses the
  original with the stage).
  Plus a guard sub-case: RF_SWAP_RENAME at index 2 alone → reply contains
  `restore failed at swap of metadata` and `rolled back`; pre-restore state.
- `test-restore-absent-files-rolls-back`: `put-file x.txt`, backup, then
  `delete-file x.txt`, `rmdir <obj>/files` (empty; created by create-object);
  kind 5, kill at P2, restart → `<obj>/files/x.txt` absent. Red on base.
- `test-restore-startup-refuses-unrecoverable-stage`: kind 5, kill at P2;
  create a regular file `<stage>/rb`; `test_env_start_at` must fail (then
  `test_env_kill` to reap); `<stage>/SWAP` and `<stage>/old/data` still exist;
  remove `<stage>/rb`; restart succeeds → pre-restore state. **Guard after
  T1** (execution correction 2026-11-26: T1's startup-refusal contract —
  `restore_recover_impl` returns -1 and `embedded.c` returns -1 on `< 0` —
  already makes this green at the T2 red-proof point; its revert-proof is
  T1's `embedded.c` change, red on the pre-T1 tree).
- `test-restore-refuses-unrecoverable-leftover`: daemon running; craft
  `<obj>/.restore-manual/SWAP`, `.restore-manual/old/metadata/marker.txt`,
  regular file `.restore-manual/rb`; restore → reply contains
  `an earlier interrupted restore`; `marker.txt` still there; pre-restore
  state; `rm -rf .restore-manual`; restore → restored state. Red on base.

Change: rewrite the body of `cmd_restore` from the lock onward to this order.
Refusal checks keep their text and the four existing `force` uses
(non-empty data/indexes/metadata; non-empty `files/`; splits/max_key
mismatch; `fields.conf` differs). T3 changes the `fields.conf`/schema part;
in T2 keep the base pre-staging `ensure_schema_conf_line`/`copy_file` calls
where they are.
1. `objlock_wrlock`; then `int prog = 0; int rec = restore_recover_impl(obj_dir, &prog);`
   `< 0 && prog` → `restore_fail_stop(obj_dir, "recovery of an earlier interrupted restore", errno, errno)`
   (the live tree may be part-rolled-back, I4);
   `< 0` → unlock, `{"error":"restore refused: an earlier interrupted restore of this object could not be rolled back; see the error log"}`, return 1;
   `> 0` → `restore_invalidate_all(db_root, object)`. T2 creates this static
   function by moving the existing invalidation block (idx cache, schema
   caches, the `indexes/` btree walk) into it unchanged, plus
   `slotcask_registry_invalidate(db_root, object)` made **unconditional** (the
   base guard `have_meta && bak_sv == 2` uses `cmd_restore` locals; this part
   of T3's change is pulled forward); T3 adds the bitmap and counts calls. Remove the later `maint_restore_swap_recover`
   call.
2. Refusals (unchanged), then cache invalidation (existing position).
3. Stage name: file-scope `static _Atomic unsigned g_restore_stage_seq;`,
   `snprintf(stage, ..., "%s/%s%d-%u", obj_dir, RESTORE_STAGE_PREFIX, (int)getpid(), atomic_fetch_add(&g_restore_stage_seq, 1) + 1)`.
   `int stage_made = mkdir(stage, 0755) == 0;` — on failure reply
   `restore failed at staging directory: %s; the live object was not changed`
   **without** `rmrf`. Then `new/`, `old/`.
4. Staging copy (RF_STAGE_COPY seam stays first), `fsync_dir(new)`,
   `fsync_dir(stage)`. Swap set `names[]`: `data`, `indexes`, `metadata`,
   `files` (if `bak_files`), plus `fields.conf` from T3.
5. Placeholders: for each directory name of the set that `lstat` says is
   absent live (ENOENT), `mkdir` it and remember it (bit mask); other `lstat`
   errors fail. If any were made: `fsync_dir(obj_dir)`. Failure →
   `restore failed at directory preparation: %s; the live object was not changed`.
6. SWAP: `open(O_WRONLY|O_CREAT|O_TRUNC, 0644)`, `fsync`, `close` (checked),
   `fsync_dir(stage)`; failure → `restore failed at swap marker: %s; the live object was not changed`.
   Every pre-SWAP failure (3–6) goes through one exit: `rmdir` the
   placeholders made, then, if `stage_made`, `restore_uncommit_and_remove(stage)`
   (unlink SWAP, `fsync_dir(stage)`, then `rmrf`; a kept stage is harmless,
   row 4), unlock, reply. (A crash there leaves only inert empty dirs and at
   most a SWAP stage with no names in `old/`, rows 3–4.)
7. Forward swap: for `i` over `names`: `rename(live, old/<name>)` (any
   failure, ENOENT included, fails: I2 guarantees existence);
   `lstat(new/<name>)`: present → `restore_test_swap_rename_fault(i) || rename(new/<name>, live)`;
   ENOENT → nothing (backup lacks it); other error fails. Then, whatever the
   outcome: `if (slotcask_bmfix_disabled_exists(obj_dir) != 0) atomic_store(&g_bmfix_any_disabled, 1);`.
   Failure → capture `errno` first, then `char step[64]; snprintf(step, sizeof(step), "swap of %s", names[i]);`
   and `return restore_abandon(db_root, object, obj_dir, stage, step, err);`
   (after the bitmap re-arm line).
8. `restore_test_park(SHARD_TEST_RESTORE_PARK_SWAPPED);` then the commit:
   ```c
   if (restore_test_fault(SHARD_TEST_RF_COMMIT_DIRSYNC) || fsync_dir(obj_dir) != 0 ||
       fsync_dir(stage_old) != 0 || fsync_dir(stage_new) != 0)
       return restore_abandon(db_root, object, obj_dir, stage, "directory sync", errno);
   if (unlink(swap_mark) != 0 && errno != ENOENT)
       return restore_abandon(db_root, object, obj_dir, stage, "swap marker removal", errno);
   if (restore_test_fault(SHARD_TEST_RF_UNSWAP_DIRSYNC) || fsync_dir(stage) != 0) {
       int e = errno;
       /* Removal not known durable either way: put SWAP back durably before
          rolling back, or a crash mid-rollback would let recovery delete old/
          as a committed leftover (table row 7b). */
       int ok = 0;
       if (!restore_test_fault(SHARD_TEST_RF_SWAP_RECREATE)) {
           int mfd = open(swap_mark, O_WRONLY | O_CREAT | O_TRUNC, 0644);
           ok = mfd >= 0 && fsync(mfd) == 0;
           if (mfd >= 0 && close(mfd) != 0) ok = 0;
           if (ok && fsync_dir(stage) != 0) ok = 0;
       }
       if (!ok) restore_fail_stop(obj_dir, "swap marker sync", e, errno);
       return restore_abandon(db_root, object, obj_dir, stage, "swap marker sync", e);
   }
   /* COMMITTED */
   ```
9. Post-commit (T3 adds installs here), `restore_invalidate_all`, the bitmap
   LOG_WARN (moved from the old success tail, condition unchanged),
   `restore_test_park(SHARD_TEST_RESTORE_PARK_COMMITTED);`, `rmrf(stage)`,
   `(void)fsync_dir(obj_dir)`, unlock, reply `restored` (unchanged text).
   Stage removal failures are harmless (no SWAP).

Helpers (static, `query_maint.c`):
```c
/* After a durable SWAP: roll back, clean up, reply. Never returns on a failed
   rollback (D2). */
static int restore_abandon(const char *db_root, const char *object, const char *obj_dir,
                           const char *stage, const char *step, int err) {
    if (restore_rollback_swap(obj_dir, stage, NULL) != 0)
        restore_fail_stop(obj_dir, step, err, errno);
    restore_uncommit_and_remove(stage);   /* I6: never rmrf a stage holding SWAP */
    (void)fsync_dir(obj_dir);
    restore_invalidate_all(db_root, object);
    objlock_wrunlock(db_root, object);
    OUT("{\"error\":\"restore failed at %s: %s; rolled back, the live object was not changed\"}\n",
        step, strerror(err ? err : EIO));
    return 1;
}
```
`static void restore_fail_stop(const char *obj_dir, const char *step, int err, int rb_err) __attribute__((noreturn));`:
build one message `RESTORE %s: %s failed (%s) and the rollback could not be completed (%s); aborting, startup recovery finishes the rollback`;
`LOG_ERROR(LOG_SUB_CONFIG, "%s", msg)`; because logging is asynchronous and the
daemon's stderr is `/dev/null`, also write it synchronously: if
`g_log_dir[0]`, `FILE *lf = open_log_for_level(1)` (declare
`FILE *open_log_for_level(int level);` in `types.h` next to `g_log_dir`),
`fprintf` + `fclose`; `fprintf(stderr, ...)`, `fflush(stderr)`; `abort()`. Do
not call `log_shutdown` (joins a thread that may not exist when embedded).
Green: all T2 tests; `test-backup-restore-failures`, `test-restore`,
`test-backup-files`, `test-restore-tokens` still pass.

### T3 fields.conf, schema line, caches

Tests first (same new file; object per case):
- `test-restore-forced-fields-conf-not-touched-on-failure`: create (`name:varchar:16`),
  insert k1 `alpha`, backup, `add-field` `["extra:int:default=7"]` (expect
  `"status":"rebuilt"`); save live `fields.conf` bytes F2 (`tu_read_file`).
  Arm RF_STAGE_COPY: reply contains `the live object was not changed`; live
  `fields.conf` == F2. Arm RF_COMMIT_DIRSYNC: reply `rolled back`; == F2.
  Clean restore: `fields.conf` == `<backup>/fields.conf` bytes; get k1 contains
  `alpha` and not `extra`. Red on base.
- `test-restore-missing-fields-conf`: unlink live `fields.conf`; arm
  RF_STAGE_COPY → failure; `fields.conf` absent. Clean restore → present, ==
  backup bytes; get k1 `alpha`. Red on base (first part).
- `test-restore-missing-schema-line`: `sed -i '/^default:<obj>:/d' <db_root>/schema.conf`;
  restore → restored; exactly one line starting `default:<obj>:`;
  `test_env_stop_keep` + restart; get k1 `alpha`. Guard.
- `test-restore-schema-install-failure-reported`: delete the line; arm
  RF_SCHEMA_INSTALL → reply contains `restore committed but the schema.conf line could not be installed`;
  no line; clean restore → `restored`, one line. Red on base.
- `test-restore-refuses-manifest-without-streams`: delete the line;
  `sed -i 's/"streams":[0-9]*,//' <backup>/object.json`; restore → reply
  contains `lacks a valid`; no line for the object. Red on base (base appends
  a `:2:0:` line that startup refuses).
- `test-restore-drops-bitmap-cache` (D8): create `{"fields":["flag:bool","name:varchar:16"],"indexes":["flag"]}`,
  k1..k3 `flag:true`, backup, k4..k8 `flag:true`; count with
  `[{"field":"flag","op":"eq","value":"true"}]` = 8 (warms the cache);
  restore; same count = 3; plain count = 3. Expected red on base; if it passes
  on base, note it in `PLAN_NOTES.md` and continue (guard).

Changes (`query_maint.c`):
- `restore_invalidate_all(const char *db_root, const char *object)`: the
  existing `invalidate_idx_cache`, `invalidate_schema_caches`, the
  `indexes/<field>/` walk (moved verbatim) calling **both**
  `btree_cache_invalidate(sp)` and `bm_cache_invalidate(sp)` per entry
  (`#include "bitmap.h"`), `counts_invalidate(db_root, object)`, and
  `slotcask_registry_invalidate(db_root, object)` unconditionally (drop the
  `bak_sv == 2` condition). Called: after recovery rolled back (T2 step 1),
  at the existing pre-staging position, in `restore_abandon`, after commit.
- Factor the line scan of `ensure_schema_conf_line` into
  `static int schema_conf_scan(FILE *f, const char *prefix, int pfxlen)` (1
  found, 0 not, -1 `ferror`). New
  `static int schema_conf_line_present(const char *db_root, const char *object)`:
  `fopen(conf, "r")` (ENOENT → 0), `flock(LOCK_SH)`, scan, unlock, `fclose`.
- `ensure_schema_conf_line`: check `flock`; scan; when missing: `fseek` END,
  `fprintf` (< 0 fails), `fflush`, `fsync(fileno(f))`; unlock; `fclose`
  checked; after an append `fsync_dir(g_db_root)`; any failure → LOG_ERROR,
  errno preserved, -1.
- In `cmd_restore`, after the existing refusals and before the cache
  invalidation: `int line_present = schema_conf_line_present(db_root, object);`
  `< 0` → `{"error":"cannot read schema.conf: %s; restore refused"}`;
  `== 0 && (bak_sv != 2 || bak_streams < 1 || bak_streams > SLOTCASK_MAX_STREAMS)` →
  `{"error":"backup manifest (object.json) lacks a valid storage_version/streams; cannot recreate the schema.conf line; restore refused"}`.
  Remove the pre-staging `ensure_schema_conf_line` call and the stale
  "wipe + cprf" comments.
- `fields.conf`: keep the open-then-fstat of the backup copy and the `!force`
  byte compare/refusal. Decide `fields_mode`: backup copy not a regular file →
  NONE; live `lstat` ENOENT → INSTALL; live regular → SWAP; live other →
  `{"error":"live fields.conf is not a regular file; restore refused"}`.
  Remove the pre-staging `copy_file(fields_src, fields_dst, ...)`. INSTALL and
  SWAP: in staging, `copy_file(fields_src, <stage>/new/fields.conf, sa.st_mode)`
  checked (`rfail = "fields.conf"`). SWAP: append `fields.conf` as the last
  name of the swap set (its index is `nswap-1`; no placeholder).
- Post-commit, before `restore_invalidate_all`:
  ```c
  const char *install_fail = NULL; int ierr = 0;
  if (fields_mode == FIELDS_INSTALL &&
      (rename(stage_new_fields, fields_dst) != 0 || fsync_dir(obj_dir) != 0)) {
      install_fail = "fields.conf"; ierr = errno;
  }
  if ((restore_test_fault(SHARD_TEST_RF_SCHEMA_INSTALL) ||
       ensure_schema_conf_line(db_root, object, bak_splits, bak_max_key,
                               bak_sv, bak_streams, bak_packed) != 0) && !install_fail) {
      install_fail = "the schema.conf line"; ierr = errno;
  }
  ```
  After unlock: if `install_fail`,
  `{"error":"restore committed but %s could not be installed: %s; the backup data is live; re-run restore with force=true"}`
  and return 1 (the JSON branch then skips the token merge; text protocol
  prints the same).
- Update the `cmd_restore` header comment to describe stage → swap → commit.
Green: T3 tests plus all restore cases.

### T4 kfcache: the pending directory sync is not lost

Tests first (`src/test/audit/audit_repro_resplit_msync.c`). File layout
after the change: the real `test_resplit_dirsync_gate` and the two new cases
live **outside** the `#if !defined(__SANITIZE_ADDRESS__) && !defined(__SANITIZE_THREAD__)`
block (they do not use the msync interposer); move `rs_size` and `rs_upsert`
above that `#if` (both halves use them; the gate test calls `rs_upsert`); add `rs_upsert_shard(db, shard, key)` (copy of
`rs_upsert` with a shard argument) outside the block; delete the `#else`
stub `static int test_resplit_dirsync_gate(void) { return 0; }` and keep only
the `audit_repro_resplit_msync_run` stub there. `#include "shard_db_internal.h"`;
the dirsync cases call `slotcask_test_kf_dirsync_reset()` at start and end:
- `test-resplit-dirsync-gate` (diagnostic extension): after the resplit loop,
  before disarming: `uint64_t opens = g_kf_open_file_call_count;` three more
  upserts to shard 0 are each refused (`!= 0`), and
  `g_kf_open_file_call_count == opens` (no reopen: Phase E identity). Red on
  base (the next acquire reopens and the flag is lost).
- `test-kf-dirsync-pending-survives-eviction`: `slotcask_shutdown();
  slotcask_init(64, 64);` (kfcache_init: 128 slots, eviction from 64
  residents), `slotcask_open(&db, base, 128, 1, 64)`, initial slots 256,
  arm KF_DIRSYNC count 100000, resplit shard 0 as in the gate test; then for
  `s = 1..127` upsert one key into shard `s` until
  `!slotcask_test_kfcache_resident(kf0)`; assert evicted (if it is never
  evicted, the test's sizing assumption is wrong: write `PLAN_NOTES.md` and
  halt — do not enlarge the loop or weaken the assertion);
  an upsert to shard 0 is refused; disarm; next upsert to shard 0 succeeds.
  End with `slotcask_close`, `rm -rf`, `slotcask_shutdown()`,
  `shard_test_ctl_reset()` (the runner re-inits caches via `test_reset_caches`).
  Red on base.
- `test-kf-dirsync-pending-survives-invalidate`: as the gate test; after the
  resplit, `slotcask_registry_invalidate(<parent of base>, <basename of base>)`;
  assert not resident; upsert to shard 0 refused; disarm; succeeds. Red on base.

Changes (`slotcask.c`, `shard_db_internal.h`):
- Remove `KfCacheEntry.dir_unsynced` and its three uses (install store in
  `kfcache_acquire_ex`, the gate in `kfcache_sync_slots_locked_impl`, the
  store in Phase D).
- After the `kf_dirsync_fault` definition (`#endif`), add:
  ```c
  /* Keyfile renames whose directory fsync failed (AUD-KF-DIRSYNC). Kept outside
     the cache entry so no drop, reopen or invalidation forgets them. */
  #define KF_DIRSYNC_PENDING_CAP 64
  typedef struct { char path[PATH_MAX]; uint64_t gen; int used; } KfDirsyncPending;
  static KfDirsyncPending g_kf_dirsync_pending[KF_DIRSYNC_PENDING_CAP];
  static pthread_mutex_t g_kf_dirsync_lock = PTHREAD_MUTEX_INITIALIZER;
  static _Atomic int g_kf_dirsync_npending;
  static _Atomic int g_kf_dirsync_overflow;   /* sticky: every kf sync also syncs its dir */
  static uint64_t g_kf_dirsync_gen;           /* under g_kf_dirsync_lock */

  static void kf_dirsync_pending_add(const char *kf_path) {
      pthread_mutex_lock(&g_kf_dirsync_lock);
      int free_i = -1;
      for (int i = 0; i < KF_DIRSYNC_PENDING_CAP; i++) {
          KfDirsyncPending *p = &g_kf_dirsync_pending[i];
          if (p->used && strcmp(p->path, kf_path) == 0) {
              p->gen = ++g_kf_dirsync_gen;
              pthread_mutex_unlock(&g_kf_dirsync_lock);
              return;
          }
          if (!p->used && free_i < 0) free_i = i;
      }
      if (free_i >= 0) {
          KfDirsyncPending *p = &g_kf_dirsync_pending[free_i];
          snprintf(p->path, sizeof(p->path), "%s", kf_path);
          p->gen = ++g_kf_dirsync_gen;
          p->used = 1;
          atomic_fetch_add(&g_kf_dirsync_npending, 1);
      } else if (!atomic_exchange(&g_kf_dirsync_overflow, 1)) {
          LOG_ERROR(LOG_SUB_SLOTCASK, "KF_DIRSYNC_PENDING_FULL: every keyfile sync now also "
                    "syncs its directory until restart");
      }
      pthread_mutex_unlock(&g_kf_dirsync_lock);
  }

  /* 0 = nothing pending for kf_path, or the retried directory fsync succeeded;
     -1 = still not known durable (errno set): the caller must not acknowledge. */
  static int kf_dirsync_pending_settle(const char *kf_path) {
      int overflow = atomic_load(&g_kf_dirsync_overflow);
      if (atomic_load(&g_kf_dirsync_npending) == 0 && !overflow) return 0;
      int idx = -1; uint64_t gen = 0;
      pthread_mutex_lock(&g_kf_dirsync_lock);
      for (int i = 0; i < KF_DIRSYNC_PENDING_CAP; i++)
          if (g_kf_dirsync_pending[i].used && strcmp(g_kf_dirsync_pending[i].path, kf_path) == 0) {
              idx = i; gen = g_kf_dirsync_pending[i].gen; break;
          }
      pthread_mutex_unlock(&g_kf_dirsync_lock);
      if (idx < 0 && !overflow) return 0;
      char dir[PATH_MAX];
      snprintf(dir, sizeof(dir), "%s", kf_path);
      char *sl = strrchr(dir, '/');
      if (!sl) { errno = EINVAL; return -1; }
      *sl = '\0';
      if (fsync_dir(dir) != 0) { if (!errno) errno = EIO; return -1; }
      if (kf_dirsync_fault()) { errno = EIO; return -1; }
      if (idx >= 0) {
          pthread_mutex_lock(&g_kf_dirsync_lock);
          KfDirsyncPending *p = &g_kf_dirsync_pending[idx];
          if (p->used && p->gen == gen && strcmp(p->path, kf_path) == 0) {
              p->used = 0; p->path[0] = '\0';
              atomic_fetch_sub(&g_kf_dirsync_npending, 1);
          }
          pthread_mutex_unlock(&g_kf_dirsync_lock);
      }
      return 0;
  }
  ```
  The `gen` check stops a sync that started before a newer failed rename of
  the same path from clearing it.
- `kfcache_sync_slots_locked_impl`: replace the `dir_unsynced` block with
  `if (h->slot >= 0 && kf_dirsync_pending_settle(g_kfcache[h->slot].path) != 0) return -1;`
  at the same position (before the `nslots == 0 && !header_changed` return;
  the writer lock keeps `path` stable).
- Phase D of `kfcache_resplit_locked`: `if (!kf_dir_synced) kf_dirsync_pending_add(e->path);`.
- Phase E, after `e->capacity = new_cap;`:
  ```c
  struct stat nst;
  if (fstat(reopen_fd, &nst) == 0) { e->file_dev = nst.st_dev; e->file_ino = nst.st_ino; }
  else { e->file_dev = 0; e->file_ino = 0; }   /* one benign reopen on the next acquire */
  ```
- `slotcask_test_kf_dirsync_reset`: lock, `memset` the table, zero
  `g_kf_dirsync_npending` and `g_kf_dirsync_overflow`, unlock.
Invariants: coverage unchanged (same single acknowledgement gate as base);
while any entry is pending every kf sync takes `g_kf_dirsync_lock` briefly
(a lookup); entries of dropped objects are never pruned (perf only, out of
scope); a persistent dir-sync fault costs one failing `fsync_dir` per write
to that keyfile, under its writer lock, never under `g_kfcache_lock`. Phase D's
`kf_bmfix_fault(KF_DIRSYNC)` and `kf_dirsync_fault()` share one count: tests
arm a persistent count.
Green: the three cases, `test-resplit-msync-failure`, the bitmap-resplit and
kf cases (`run-all --filter kf`, `--filter resplit`, `--filter bmfix`).
Compile-only check of the sanitizer layout of this file:
`BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` and `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`
(grep each log for " error"/" warning"; no sanitizer suite run, D11), then
rebuild the normal mode with `SKIP_TESTS=1 ./build.sh` before continuing.

### T5 Test-review minors and artifacts (no production code)

- `test_bulk_insert_arena_oom.c`: the count assertion becomes
  `ASSERT_TRUE(resp && resp[0] == '0' && (resp[1] == '\n' || resp[1] == '\0'), ...)`;
  `tu_pdb_drop_object(db, "default", "bao_t")` before the final return and on
  the buffer-failure return.
- `test_kf_deleted_underflow.c`: the three `atol(resp)` assertions also
  require `isdigit((unsigned char)resp[0])` (`#include <ctype.h>`).
- `test_audit_oracle_strictness.c`: log path `"%s/../oracle-selftest.log"`
  with `env.db_root` (removed by `test_env_stop`); drop the
  `/tmp/shard-db-audit-logs` mkdir; on `fopen` failure `tc_close`,
  `test_env_stop`, return 1; `fclose(logf)` before every return; final return
  `t_ctx->failed > 0 ? 1 : 0`.
- `test_backup_restore_errors.c` `test-backup-restore-failures`: run the
  backup/restore parts for every user; wrap only the two `chmod` blocks in
  `if (geteuid() != 0)` with `TAP_DIAG("# running as root: chmod-based copy-failure checks skipped\n")`
  otherwise. Same-second check made deterministic: before the backups,
  `mkdir -p <obj>/backup` and pre-create empty dirs named
  `strftime("%Y%m%d_%H%M%S")` for `now .. now+5` (`localtime_r`); each backup
  path's basename must be `<one of those>_<n>`, and the two differ; `rmdir` the
  pre-created dirs afterwards.
- `src/test/audit/audit_repro_{idle_connections,text_protocol_auth,token_chain,server_file_read}.c`:
  `base` via `mkdtemp("/tmp/shard-db-ta-XXXXXX")`; keep writing the custom
  `db.env` into `base` (PORT = picked port), then
  `TestEnv env = {0}; test_env_start_at(&env, db_root, port)` (it keeps an
  existing `db.env`, resolves the binary like every other case, and waits for
  readiness); delete the hand-rolled `realpath`/`fork`/`execl` and the
  readiness loop; every exit after a successful start goes through one label
  doing `test_env_stop_keep(&env); tu_run_cmd("rm -rf %s", base);`. Idle test:
  delete the 1 s sleep (accept order is FIFO, so the idle connections reach
  workers first); replace the 3 s sleep by `tc_recv(late, &r2)` on a client
  with `io_timeout_ms = 6000`, timed with `CLOCK_MONOTONIC`: assert it failed
  and returned in under 5000 ms (closed by the server, not by the client
  timeout).
- `test_runner_include_audit.c` (D10): move the `run-all` argv loop out of
  `src/test/shard-db-test.c` `main` into
  `int test_parse_run_all_args(int argc, char **argv, int first, const char **filter, const char **exclude, int *jobs, int *include_audit)`
  in `test_runner.c` (same semantics and stderr message; returns 0 or 2), plus
  `int test_count_selected(const char *filter, const char *exclude, int include_audit)`
  (wraps `collect_matching`, frees the array); declare both in
  `test_runner.h`; `main` calls the parser. The case drops `popen` and asserts
  in-process: `--filter audit-planner-diff --include-audit` → rc 0, flag 1,
  filter set; `--include-audit --jobs 3` → flag 1, jobs 3; `--jobs --include-audit`
  → flag 1, jobs 0; `--include_audit` → rc 2; `test_count_selected("audit-planner-diff", NULL, 0) == 0`
  and `(..., 1) == 1` (build each argv from `char` arrays, not string
  literals, so the build stays warning-free). Revert proof: temporarily make the parser stop at
  `i + 1 < argc` and paste the failing first assertion.
- Artifacts: `rm` exactly these (each referenced by no `docs/plans/*.md`,
  checked by basename and by stem for `{sh,out}` forms):
  `docs/plans/audit-artifacts/{AUD-QRY-1-red-base.log,AUD-QRY-1-revert-prove.log,AUD-QRY-2-red-base-full.log,AUD-QRY-3-red-base-full.log,AUD-QRY-4-sanitizer-gates.log,AUD-QRY-5-red-base-full.log,AUD-QRY-6-sanitizer-gates.log,AUD-QRY-7-sanitizer-gates.log,gateA-run1-ok-count.txt,clang-tidy-util-config.log,gcc-fanalyzer-util-config-server.log,lint-raw.log,cppcheck-unusedFunction.txt}`.
  Re-run the check first, for each file's basename and its stem (name
  without extension):
  `grep -rlF --exclude=2026-11-26-fix-restore-swap-and-kf-dirsync.md --exclude=PLAN_NOTES.md -- "<name>" docs/ *.md`
  (options before `--`). Expected hits: `AUD-QRY-1-red-base` only in
  `AUD-QRY-1-revert-prove.log`; clang-tidy, gcc-fanalyzer and lint-raw only
  in `lint-triage.md`; cppcheck only in `code-reduction-candidates.md`; all
  others none.
  Allowed hits (do not halt): the file itself;
  `docs/plans/audit-artifacts/lint-triage.md` and
  `docs/plans/audit-artifacts/code-reduction-candidates.md` (for the last
  four files); and the two deleted AUD-QRY-1 logs citing each other. Any other
  hit: halt and note. (`PLAN_NOTES.md` is git-excluded scratch and is
  ignored; it is the only citer of the three `*-sanitizer-gates.log`.) In
  `lint-triage.md` and `code-reduction-candidates.md` append "(raw log removed
  2026-11-26; regenerate with the command above)" at each citation. Keep
  everything else, including all of `audit/scripts/` (each is cited by the
  audit index). List the deletions in `PLAN_NOTES.md`. No test is deleted.

### T6 Docs, index, verification

- `docs/operations/backup.md`: Restore section: how it works (stage copy needs
  free space about the backup's size; SWAP; commit; crash before commit rolls
  back at the next start, after commit keeps the restored data); `fields.conf`
  is swapped with the data; a missing `fields.conf`/schema line is installed
  right after the commit (and the "committed but … could not be installed"
  error with its remedy); refusal of a manifest without valid
  storage_version/streams when the line must be recreated; a warning that
  `force` over a backup with different splits keeps the live schema line
  (AUD-RESTORE-SPLITS, do not); new "Interrupted restores" subsection: abort
  on an unrecoverable rollback, startup refusal, manual recovery (inside
  `<obj>/.restore-*/`: `old/<name>` = pre-restore copy, `new/` and `rb/` =
  restored copies; for each `old/<name>` move the live `<name>` aside, move
  `old/<name>` back, delete the staging directory, start). Note that a
  rolled-back restore into an object missing a subtree (typically `files/`)
  leaves that subtree as an empty live directory (inert placeholder). Replace "Known
  windows (files/)" by one line keeping the still-true caveat (a backup whose
  `files/` was pruned but manifest kept leaves live `files/` absent after a
  forced restore). Fix the wrong error text (`refusing to overwrite non-empty
  live object…` → `<sub>/ is not empty (use force=true to overwrite)`).
- `docs/reference/error-codes.md`: a Restore table with every restore error
  string in `cmd_restore` after this plan (new and existing).
- `docs/reference/changelog.md`, 2026.11.3: restore commits atomically
  (cleanup crash keeps the restored data), rolls back on a failed directory
  sync, aborts and recovers when a rollback cannot complete, startup refuses
  an unrecoverable interrupted restore, `fields.conf` and schema-line writes
  happen only after the commit, bitmap cache dropped on restore; keyfile
  directory-sync block now survives reopen, eviction and invalidation.
- `docs/plans/2026-11-19-audit-index.md`: mark AUD-RESTORE-B1/M1/M2,
  AUD-KF-DIRSYNC (rename from -EVICT) fixed by this plan; AUD-P3-6 fixed for
  the restore path; add AUD-RESTORE-BMCACHE (D8, fixed); notes: missing schema
  line ⇒ startup recovery never visits that object's stage; pending table
  never pruned for dropped objects.
- Full suite fresh, paste the summary line; `rm -rf /tmp/shard-db-*`;
  `git status --short` and `git diff --stat` pasted; hand back uncommitted.
  Remind the human that the branch's sanitizer gate (D11) is still owed.
