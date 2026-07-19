# Fix: transactional `rebuild_object_v2` crash recovery (Finding 11)

Source finding: `docs/plans/2026-07-16-storage-durability-and-recovery-findings.md`,
Finding 11.

This revision supersedes the original marker-free proposal. That proposal
treated existence of `.rebuild_legacy_root/data` as proof that the rebuild
walk had not completed. That invariant was unsafe because `rmrf()` recursively
deletes the directory: a crash during cleanup can leave the path present but
partially deleted. Restoring it would destroy the complete rebuilt `data/` and
replace it with an incomplete legacy tree.

Execution mode for this repo (per `CLAUDE.md`): execute on a fresh branch off
`main`, leave all changes uncommitted for review, build with
`SKIP_TESTS=1 ./build.sh`, and test with
`./build/bin/shard-db-test run[-all]`.

If a quoted anchor does not match exactly, write `PLAN_NOTES.md` and halt the
entire run. Do not reinterpret a stale anchor. If implementation exposes a
state not covered by the recovery table below, stop and ask.

## Root cause

`rebuild_object_v2` currently performs a multi-file transaction without an
on-disk transaction state:

1. Delete any stale `data.legacy/`.
2. Rename `data/` to `data.legacy/`.
3. Rename `data.legacy/` to `.rebuild_legacy_root/data/`.
4. Create a fresh `data/` and walk the legacy copy into it.
5. Rewrite `fields.conf` and/or the object's `schema.conf` entry.
6. Recursively delete `.rebuild_legacy_root`.
7. Rebuild affected indexes.

A process crash between steps 2 and 6 leaves the only complete copy under a
path startup recovery ignores. A subsequent `slotcask_open()` creates a new
empty `data/`, and a later rebuild can delete the surviving legacy copy.

The old plan correctly identified that failure, but its proposed fix had four
additional correctness gaps:

- Recursive `rmrf()` cannot be a commit marker.
- Data encoded with `new_sch` is not committed until `fields.conf`,
  `schema.conf`, and affected indexes agree with it.
- `edit-field` rewrites `fields.conf` after `rebuild_object_v2` returns, so a
  transaction contained only inside the old function boundary cannot cover it.
- Recovery logged ambiguous/failed restores and then allowed the daemon or
  embedded handle to serve the object anyway.

## Locking model (confirmed call-site inventory)

The per-object write lock protects a live rebuild from concurrent writers in
the same process:

- JSON and fast-protocol schema modes take `objlock_wrlock` in `server.c`.
- Embedded requests use the same JSON dispatcher.
- Auto-reshard explicitly takes `objlock_wrlock`.
- `cmd_add_fields` and `cmd_edit_fields` document that their caller holds it.

One exception exists today: `auto_vacuum_sweep_one()` calls `cmd_vacuum()`
without a lock. Although described as a light vacuum, `cmd_vacuum()` upgrades
to `rebuild_object()` when the CPU-derived stream count differs from the
stored schema. Task 5 closes that hole by taking the per-object write lock
around every auto-vacuum call.

The per-object lock is process-local and does not protect startup recovery
from a second process. Today `shard_db_open_internal()` runs recovery before
the daemon acquires `$DB_ROOT/.shard-db.lock`, and embedded mode never acquires
that flock. Task 4 makes DB-root ownership a prerequisite for recovery in both
modes.

Reads intentionally do not take `objlock` in this codebase. This plan does not
change that established live-read model; the lock requirement here is to
prevent mutations while the old snapshot is walked.

## Scope of the guarantee

This plan guarantees recovery from process interruption: `SIGKILL`, OOM kill,
or a process crash. It does **not** claim completed writes survive sudden power
loss. Page-cache writeback and bounded `MS_SYNC`/`fsync` behavior are owned by
`docs/plans/2026-07-17-durability-sync-and-embedded-bg-threads.md` (Finding 2).
That plan must land separately before advertising a power-loss durability
guarantee.

## Transaction design

Introduce three internal per-object directories:

```text
.rebuild_txn.preparing/    backup/manifest construction only; live data untouched

.rebuild_txn.active/       transaction is uncommitted; recovery rolls back
  data/                    complete pre-rebuild data tree
  fields.conf.rollback     complete pre-rebuild fields file
  meta                     version + old splits/streams + index-dirty flag

.rebuild_txn.done/         commit point passed; recovery only deletes this
```

The commit point is the same-filesystem atomic rename:

```c
rename(".rebuild_txn.active", ".rebuild_txn.done")
```

Recursive deletion happens only after that rename. Therefore a crash during
cleanup leaves `.rebuild_txn.done`, which is always garbage; recovery never
restores from it.

The transaction module is deliberately deep. Rebuild callers learn only this
interface; backup layout, idempotent rollback, legacy compatibility, and
cleanup stay inside `objlock.c`:

```c
typedef struct RebuildTxn RebuildTxn;

RebuildTxn *rebuild_txn_begin(const char *db_root, const char *object,
                              int old_splits, int old_streams,
                              int indexes_may_change);
const char *rebuild_txn_legacy_root(const RebuildTxn *txn);
int rebuild_txn_commit(RebuildTxn *txn);
int rebuild_txn_abort(RebuildTxn *txn);
void rebuild_txn_cleanup_committed(RebuildTxn *txn);
void rebuild_txn_free(RebuildTxn *txn);

/* Returns 0 only when every object is safe to serve. */
int rebuild_recovery(const char *db_root);
```

`rebuild_txn_begin()` performs all preconditions before moving live data:

1. Refuse to start if `.rebuild_txn.active` exists; startup recovery must
   resolve it first.
2. Delete stale `.rebuild_txn.preparing` and `.rebuild_txn.done`, then verify
   both are absent. Neither can ever be authoritative.
3. Create `.rebuild_txn.preparing`.
4. Copy the current `fields.conf` to `fields.conf.rollback` without removing
   the source.
5. Write `meta` through a temp file and atomically rename it into place.
   Format:

   ```text
   version=1
   old_splits=8
   old_streams=4
   indexes_may_change=1
   ```

6. Atomically rename `.rebuild_txn.preparing` to `.rebuild_txn.active`. An
   active transaction therefore always has a complete rollback file and a
   valid manifest.
7. Rename `data/` directly to `.rebuild_txn.active/data/`. The new path is
   already in the shape `slotcask_open(legacy_root, ...)` requires, so the
   unsafe intermediate `data.legacy/` rename is removed from new rebuilds.

`rebuild_txn_abort()` is idempotent and leaves `.active` intact until all
rollback steps succeed:

1. Invalidate the object's slotcask/cache entries.
2. Delete the new `data/`; verify it is actually absent.
3. If `.active/data` exists, rename it back to `data/`. If it is absent but
   `data/` exists, a prior interrupted rollback already completed this step.
4. Atomically restore `fields.conf` from the retained rollback copy.
5. Restore old splits/streams with
   `update_schema_conf_splits_streams()` and require success.
6. If `indexes_may_change=1`, run `reindex_object_checked()` against the
   restored old data/schema. Indexes are derived state; rebuilding them makes
   rollback safe even if the crash interrupted a new-layout reindex.
7. Rename `.active` to `.done`, then recursively delete `.done`.

If any step fails, return nonzero and preserve the transaction directory for
the next startup or manual intervention. Never fabricate success.

`rebuild_txn_commit()` performs **only** the atomic `.active` -> `.done`
rename. Failure to rename is a failed commit and the caller must invoke
`rebuild_txn_abort()`. `rebuild_txn_cleanup_committed()` recursively removes
`.done` afterward. Keeping these separate provides a deterministic
`after-commit` crash point and ensures an interrupted cleanup can never be
mistaken for an active rollback source.

## Recovery state table

Recovery runs before validation, cache warmup, background threads, or request
workers.

| On-disk state | Action |
|---|---|
| `.rebuild_txn.preparing` only | Delete it; live data and metadata have not moved |
| `.rebuild_txn.active` with valid `meta` | Idempotently roll back data + fields + schema + affected indexes |
| `.rebuild_txn.done` | Delete it; never restore from it |
| more than one of `.preparing`, `.active`, `.done` | Refuse startup and preserve all transaction paths |
| active transaction missing/invalid metadata | Refuse startup and preserve it |
| old `data.legacy` exists and `data/` does not | Rename it back to `data/` |
| old `.rebuild_legacy_root/data` exists and `data/` does not | Rename it back to `data/` |
| old staging copy and `data/` both exist | Refuse startup; old code gives no safe way to distinguish an incomplete new walk from an interrupted legacy cleanup |
| both old staging paths exist | Refuse startup and preserve both |
| empty old `.rebuild_legacy_root` plus normal `data/` | Delete the empty shell |
| no transaction/staging paths | No-op |

Legacy ambiguous states deliberately become an actionable startup failure.
Choosing either tree automatically can destroy the only complete copy.

## Metadata commit seam

Move edit-field's metadata write and selective reindex inside the rebuild
transaction with one finalization interface:

```c
typedef struct {
    int (*apply_metadata)(void *ctx);
    int (*rebuild_indexes)(void *ctx, int *out_rebuilt);
    void *ctx;
    int indexes_may_change;
} RebuildFinalizeOps;

int rebuild_object_v2(const char *db_root, const char *object,
                      const Schema *old_sch, const TypedSchema *old_ts,
                      const Schema *new_sch, TypedSchema *new_ts,
                      int *new_to_old, int slot_changed,
                      int splits_changed, int drop_tombstoned,
                      char added_lines[][256], int n_added,
                      const RebuildFinalizeOps *finalize);
```

- `rebuild_object()` passes `NULL`; `splits_changed` continues to select the
  internal full-index rebuild and sets the transaction's index-dirty flag.
- `cmd_edit_fields()` supplies adapters around
  `rewrite_fields_conf_for_edit()` and `selective_reindex_dirty()`, plus
  `indexes_may_change = n_dirty > 0`.
- `rebuild_object_v2()` calls `apply_metadata` after its own add/compact
  `fields.conf` rewrite and schema update. It invalidates schema/index caches,
  then calls `rebuild_indexes`, and only then reaches `rebuild_txn_commit()`.
- Remove the post-return edit-field rewrite. A callback error aborts the whole
  transaction and restores the old layout.
- Check, rather than ignore, the return from
  `update_schema_conf_splits_streams()`.
- Complete all required reindexing before commit. Any failure aborts. If an
  abort follows partial reindexing, `indexes_may_change` causes rollback to
  regenerate old indexes.
- The existing `reindex_object()` does not expose build failure: it ignores
  `build_indexes_streaming_multi()`'s return and reports the requested count.
  Add `reindex_object_checked(..., int *out_count)` in `index.c`/`types.h` and
  use it for transactional forward reindex and rollback. It returns nonzero
  on allocation, schema-load, or index-build failure. Keep `reindex_object()`
  as a compatibility wrapper for existing non-transactional callers.
- Change `selective_reindex_dirty()` from `void` to `int` and propagate the
  return from `cmd_add_indexes()` so edit-field cannot commit after a failed
  selective rebuild.

## Test-only deterministic pause seam

Add two instance fields, default-disabled and parsed from the per-test
`db.env`:

```c
char rebuild_test_pause_phase[32]; /* test-only; empty = disabled */
int  rebuild_test_pause_ms;        /* test-only; 0 = disabled */
```

Accepted phases are `after-stage`, `after-walk`, `after-metadata`, and
`after-commit`. At a matching phase, `rebuild_object_v2()` creates
`<obj>/.rebuild-test-<phase>.active`, sleeps in 100 ms slices for the configured
duration, then removes the marker. Tests wait for the marker and `SIGKILL` the
daemon; no timing guess is required. This is the same explicit test-only style
as `SCHEMA_WRLOCK_TEST_DELAY_MS`. After killing the paused daemon, the test
must unlink only this known test marker before restart; transaction and legacy
artifacts remain untouched for recovery.

## Tasks

### Task 1 — write the regression tests first

Add `src/test/cases/test_rebuild_txn_recovery.c` and register it immediately
after `test_rebuild_recovery.c` in `build.sh`. Also add
`src/test/embedded_lock_harness.c`, a tiny separate process that calls
`shard_db_open(argv[1])`, returns 0 only when the open is correctly refused,
and closes/returns failure if it unexpectedly acquires the live daemon's DB
root. Build it against `build/bin/libshard-db.a`; do not call public
`shard_db_open()` inside the test-runner process, whose process-local database
globals are already initialized by the runner.

The new file must include `<errno.h>` directly and must not include unused
`<ftw.h>`. Use `TestEnv`, unique per-test `db.env` files, and existing
`test_env_start_at()` lifecycle helpers. It must register these cases:

```c
TEST_REGISTER("test-rebuild-legacy-stage1-safe-restore", test_legacy_stage1)
TEST_REGISTER("test-rebuild-legacy-stage2-no-data-safe-restore", test_legacy_stage2_no_data)
TEST_REGISTER("test-rebuild-legacy-ambiguous-refuses-start", test_legacy_ambiguous)
TEST_REGISTER("test-rebuild-txn-crash-after-stage", test_txn_crash_after_stage)
TEST_REGISTER("test-rebuild-txn-crash-after-walk", test_txn_crash_after_walk)
TEST_REGISTER("test-rebuild-txn-edit-field-after-metadata", test_edit_crash_after_metadata)
TEST_REGISTER("test-rebuild-txn-done-cleanup-never-restores", test_done_cleanup)
TEST_REGISTER("test-rebuild-txn-recovery-idempotent", test_recovery_idempotent)
```

Required assertions:

- Both unambiguous old layouts preserve all inserted records.
- Old `.rebuild_legacy_root/data` plus a live `data/` makes daemon startup
  fail, and both directories remain byte-for-byte present.
- Killing at `after-stage` and `after-walk` rolls back to the old record count,
  old fields, and old splits on restart.
- Killing edit-field at `after-metadata` restores both the old bytes and old
  `fields.conf`; decoding the record returns the original value.
- A hand-built `.rebuild_txn.done` containing a deliberately incomplete old
  tree is deleted while the complete live `data/` remains authoritative.
- Killing recovery after its data rename (construct the corresponding
  `.active/data`-absent state directly) converges on the next startup.
- No test removes an ambiguous artifact merely to make startup succeed.

Build and run these tests against unmodified code first. They must fail because
the transaction paths/pause hooks are not implemented and legacy recovery is
absent. Paste the actual failures.

### Task 2 — implement the deep rebuild transaction module

**Files:** `src/db/types.h`, `src/db/objlock.c`, `src/db/index.c`.

Add the opaque interface exactly as shown under “Transaction design.” Keep
`struct RebuildTxn`, path construction, metadata parsing, atomic file copy,
legacy compatibility, rollback, commit, and cleanup private to `objlock.c`.

Requirements:

- Use `lstat()` for artifact classification; do not follow a staging symlink.
- Reject non-directory transaction roots and non-regular metadata/rollback
  files.
- Never call `rmrf()` on `.active`.
- Construct rollback files only under `.preparing`; atomically rename that
  directory to `.active` before moving `data/`.
- After every `rmrf()` of live `data/`, verify `lstat(data_dir)` returns
  `ENOENT` before attempting restore.
- Use temp-file + `rename()` for `fields.conf` restoration and retain the
  rollback source until `.active` becomes `.done`.
- Parse `meta` strictly: version exactly 1, valid splits, positive streams,
  each required key exactly once, no unknown or truncated line.
- Bound every `snprintf()` and reject truncation.
- `rebuild_recovery()` returns nonzero if any object is ambiguous or any
  restore step fails.
- Add and use `reindex_object_checked()` as specified under “Metadata commit
  seam”; rollback must not mark itself complete if old-layout reindex fails.

### Task 3 — make the entire rebuild one transaction

**Files:** `src/db/query_find.c`, `src/db/query_schema.c`,
`src/db/query_internal.h`, `src/db/shard_db_internal.h`, `src/db/config.c`.

In `rebuild_object_v2` replace the current staging block beginning with:

```c
    /* Clean any stale data.legacy from a prior crashed rebuild. */
    rmrf(legacy_dir);
```

and ending after the second legacy rename with a call to
`rebuild_txn_begin()`. Do not retain `data.legacy` in the new path.

Use `rebuild_txn_legacy_root(txn)` when opening `legacy_db`. Route every error
after `begin()` through one cleanup label that closes any opened handles,
invalidates caches, calls `rebuild_txn_abort()`, frees the transaction, and
returns an error. Do not return directly from inside an active transaction.

Apply metadata in this order while `.active` still exists:

1. Internal add-field/tombstone-compaction `fields.conf` rewrite.
2. Checked `schema.conf` splits/streams update.
3. Optional edit-field metadata adapter.
4. Schema/index cache invalidation.
5. Internal full reindex for splits changes or the optional edit-field
   selective-index adapter.
6. Registry/cache invalidation again so neither new-data nor legacy-data mmap
   entries remain live across backup cleanup.
7. `rebuild_txn_commit()` (atomic rename only).
8. `after-commit` test pause.
9. `rebuild_txn_cleanup_committed()`.
10. Success response.

Move edit-field's `rewrite_fields_conf_for_edit()` into its adapter and remove
the old post-return block beginning with:

```c
    /* Rebuild succeeded — rewrite fields.conf to lock in the new spec. */
```

Add the test pause helper and invoke it at the four documented transaction
phases. The `after-commit` pause must happen after `.active` has atomically
become `.done` but before recursive cleanup.

Add the two test-only fields to `ShardDb`, initialize them to disabled values,
add guarded `g_rebuild_test_*` macros alongside the existing schema-lock test
macro, and parse `REBUILD_TEST_PAUSE_PHASE` / `REBUILD_TEST_PAUSE_MS` from the
per-instance `db.env`. Reject negative milliseconds and unknown phases rather
than silently accepting a misspelled test hook.

### Task 4 — make startup recovery exclusive and fail closed

**Files:** `src/db/shard_db_internal.h`, `src/db/embedded.c`, `src/db/server.c`,
`src/db/types.h`, `src/db/objlock.c`.

Add a DB-root lock descriptor to `ShardDb`:

```c
int db_root_lock_fd; /* held for embedded instance lifetime; -1 when unused */
```

Explicitly initialize it to `-1` in `db_defaults_set()`; `calloc`'s zero is a
valid descriptor and must not be interpreted as “no lock.”

Add shared helpers:

```c
int db_root_lock_acquire(const char *db_root, int *out_fd);
void db_root_lock_release(int *fd);
```

Move `rebuild_recovery()` out of `shard_db_open_internal()`. Locate this exact
anchor in `embedded.c`:

```c
    load_dirs();
    load_tokens_conf(db->db_root);
    load_allowed_ips_conf(db->db_root);
    objlock_init();
    rebuild_recovery(db->db_root);

    return db;
```

Replace it with:

```c
    load_dirs();
    load_tokens_conf(db->db_root);
    load_allowed_ips_conf(db->db_root);
    objlock_init();

    return db;
```

`shard_db_open_internal()` remains responsible for one initialization pass,
but no longer mutates recovery artifacts before DB-root ownership is known.

Delete the redundant post-fork initialization/recovery pass in `cmd_server`.
Locate this exact anchor in `server.c`:

```c
    parallel_io_pool_init(io_pool_sz);
    /* load_dirs() already called pre-fork (see validate_metadata block). */
    load_tokens_conf(db_root);
    load_allowed_ips_conf(db_root);
    objlock_init();
    rebuild_recovery(db_root);

    int nthreads = g_workers > 0 ? g_workers : (int)sysconf(_SC_NPROCESSORS_ONLN);
```

Replace it with:

```c
    parallel_io_pool_init(io_pool_sz);

    int nthreads = g_workers > 0 ? g_workers : (int)sysconf(_SC_NPROCESSORS_ONLN);
```

Do not leave a second recovery call after daemonization or stderr redirection.
For daemon mode, the single checked recovery call follows this order:

1. Create DB_ROOT.
2. Acquire `.shard-db.lock`.
3. Initialize `ShardDb`.
4. Run recovery and require success.
5. Run `validate_metadata()`.
6. Only then daemonize/bind/listen/start workers.

For embedded mode, acquire the same flock before
`shard_db_open_internal()`, store it in `db_root_lock_fd`, run recovery before
startup migration/pools, and release it in every error path and in
`shard_db_close()`.

Change the declaration from `void` to `int`. Remove both old unchecked call
sites described above, then add exactly two checked call sites: one pre-fork
in `cmd_server()` after DB-root lock acquisition and instance initialization,
and one in `shard_db_open()` after DB-root lock acquisition and instance
initialization but before startup migration. Any ambiguous or failed recovery
must produce a clear stderr/log diagnostic and abort startup; it must never
return a usable embedded handle or start a request worker.

Add a regression case that execs `embedded_lock_harness` on a DB root already
locked by a daemon and asserts the child observes `shard_db_open() == NULL`
without changing transaction artifacts.

### Task 5 — close the auto-vacuum lock hole

**File:** `src/db/server.c`.

Replace this exact body block in `auto_vacuum_sweep_one`:

```c
    uint64_t obj_t0 = now_ms();
    cmd_vacuum(eff, obj_name, 0, 0);
    LOG_INFO(LOG_SUB_VACUUM, "AUTO-VACUUM done %s/%s in %lums",
            dir_name, obj_name, (unsigned long)(now_ms() - obj_t0));
    ctx->vacuumed++;
```

with:

```c
    uint64_t obj_t0 = now_ms();
    objlock_wrlock(eff, obj_name);
    int vacuum_rc = cmd_vacuum(eff, obj_name, 0, 0);
    objlock_wrunlock(eff, obj_name);
    LOG_INFO(LOG_SUB_VACUUM, "AUTO-VACUUM done %s/%s rc=%d in %lums",
            dir_name, obj_name, vacuum_rc,
            (unsigned long)(now_ms() - obj_t0));
    if (vacuum_rc == 0) ctx->vacuumed++;
```

Add a deterministic test using `after-stage`: force a stream-count mismatch,
let auto-vacuum enter the pause, issue a concurrent insert, and assert the
insert remains blocked until the rebuild releases the write lock. Afterward,
assert the inserted record and every preexisting record are present.

### Task 6 — prove red, then green

Before implementation:

```text
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all --filter rebuild-txn
./build/bin/shard-db-test run test-rebuild-legacy-stage1-safe-restore
```

Paste the expected failures. After Tasks 2-5:

```text
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all --filter rebuild
./build/bin/shard-db-test run-all --filter auto-vacuum
./build/bin/shard-db-test run-all --filter embedded
./build/bin/shard-db-test run-all
```

Because this change touches lock ownership and cross-process lifecycle, also
run the repository's existing ThreadSanitizer build against the affected
tests:

```text
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all --filter rebuild
./build/bin/shard-db-test run-all --filter auto-vacuum
```

Do not suppress or waive a sanitizer finding.

### Task 7 — documentation disposition

Update Finding 11 in
`docs/plans/2026-07-16-storage-durability-and-recovery-findings.md` to point at
the completed implementation and recorded test output. Correct its guarantee
from “power loss” to process interruption, with Finding 2 owning bounded
power-loss durability.

## Execution notes — 2026-07-19

Red was reproduced against an isolated `HEAD` tree containing only the new
public-interface regression test:

```text
./build/bin/shard-db-test run test-rebuild-legacy-stage1-safe-restore
not ok 5 - all records restored before requests are served
#   expected 50 got 0
not ok 7 - recovery consumes data.legacy
# test-rebuild-legacy-stage1-safe-restore: 5 passed, 2 failed
```

Green release validation:

```text
SKIP_TESTS=1 ./build.sh
Built: build/bin/

./build/bin/shard-db-test run-all --filter rebuild
# total: 384 passed, 0 failed across 11 cases

./build/bin/shard-db-test run-all --filter auto-vacuum
# total: 17 passed, 0 failed across 1 cases

./build/bin/shard-db-test run-all --filter embedded
# total: 5 passed, 0 failed across 1 cases

./build/bin/shard-db-test run-all
# total: 10428 passed, 0 failed across 300 cases
```

ThreadSanitizer initially exposed two pre-existing cache lock-order
inversions reached by these stronger rebuild tests. Cache eviction/install
now consistently avoids waiting for an entry lock while holding its table
mutex, and `rebuild-kf` releases its segment entry before taking a key-file
entry. After those fixes, the required sanitizer validation was clean:

```text
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
Built: build/bin/

./build/bin/shard-db-test run-all --filter rebuild
# total: 384 passed, 0 failed across 11 cases

./build/bin/shard-db-test run-all --filter auto-vacuum
# total: 17 passed, 0 failed across 1 cases
```

No sanitizer report was suppressed or waived.

## Definition of done

- [x] New rebuilds never create `data.legacy` or use recursive deletion as a
      transaction signal.
- [x] `.active` always means rollback; `.done` always means cleanup-only.
- [x] Data, fields metadata, schema metadata, and affected indexes cross the
      commit point together.
- [x] Edit-field metadata is inside the transaction.
- [x] Every active-transaction error converges through rollback.
- [x] Ambiguous legacy state fails startup without deleting either copy.
- [x] Recovery failure prevents daemon and embedded request service.
- [x] Recovery runs only while the process owns the DB-root flock.
- [x] Auto-vacuum holds the per-object write lock even when it upgrades to a
      streams-mismatch rebuild.
- [x] Crash-phase and repeated-recovery tests pass.
- [x] Existing `test-rebuild-recovery`, vacuum, add/edit-field, auto-reshard,
      and auto-vacuum tests pass.
- [x] Full suite and applicable sanitizer run are green.
- [x] No compiler warnings, debug artifacts, or unrelated changes.
- [x] Actual red/green/full-suite outputs are pasted into the execution notes.
