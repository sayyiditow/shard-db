# Follow-ups from review of fix/rebuild-txn-recovery

Three items surfaced during code review of the executed
`docs/plans/2026-07-18-rebuild-legacy-crash-recovery.md` plan (branch
`fix/rebuild-txn-recovery`, currently uncommitted). None are correctness
bugs in the shipped diff — they close a test-coverage gap and two small
nits so the branch can be pushed and merged with a clean bill of health.

Branch: continue on the existing `fix/rebuild-txn-recovery` branch (already
checked out, changes uncommitted). Do not create a new branch.

Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run-all`
(or `run <name>` / `run-all --filter <substr>` to target the affected
cases).

## Task 1 (major): cover the splits-changing rollback of schema.conf under a crash pause

**Root cause of the gap**: `rebuild_txn_abort()`
(`src/db/objlock.c`, `rebuild_txn_abort`) unconditionally reverts
`schema.conf`'s splits/streams via `update_schema_conf_splits_streams(...,
txn->old_splits, txn->old_streams)`. The forward write of the *new*
splits/streams happens in `rebuild_object_v2()`
(`src/db/query_find.c`) between the `after-walk` pause (line 1045) and the
`after-metadata` pause (line 1100):

```c
    if ((splits_changed || streams_changed) &&
        update_schema_conf_splits_streams(
            db_root, object, splits_changed ? new_sch->splits : 0,
```

The only existing test that pauses at `after-metadata` is
`test_edit_crash_after_metadata` in
`src/test/cases/test_rebuild_txn_recovery.c`, which drives
`cmd_edit_fields` via `trigger_edit_rebuild()` — a path that never changes
splits/streams. So the forward write shown above never executes in that
test, and nothing exercises "crash after schema.conf was rewritten to the
*new* splits value, then roll back to the *old* value." A bug in that
revert (wrong object, wrong direction, off-by-one on the value) would go
undetected by the current suite.

**Regression test first**: add a new test that reuses the existing
`test_txn_crash_phase(phase, object)` helper
(`src/test/cases/test_rebuild_txn_recovery.c`, already used by
`test_txn_crash_after_stage`/`test_txn_crash_after_walk`) with phase
`"after-metadata"` on a splits-changing object — `trigger_splits_rebuild()`
already issues `{"mode":"vacuum",...,"splits":16}` against an object created
at `splits:8` by `create_object_with_records()`, so this exact combination
does not exist yet only because no test asks for `after-metadata` on a
splits object.

Locate this exact anchor in `src/test/cases/test_rebuild_txn_recovery.c`:

```c
static int test_txn_crash_after_walk(void) {
    return test_txn_crash_phase("after-walk", "txnwalk");
}
```

Insert immediately after it:

```c

static int test_txn_crash_after_metadata_splits(void) {
    return test_txn_crash_phase("after-metadata", "txnmetasplits");
}
```

Then strengthen `test_txn_crash_phase` itself so all three phases
(`after-stage`, `after-walk`, `after-metadata`) verify schema.conf was left
at (or restored to) the pre-rebuild splits value — not just record count and
`.rebuild_txn.active` absence. Add a small reader helper. Locate this exact
anchor (top of the file, after the existing includes/helpers):

```c
static int append_pause_config(const char *db_root, const char *phase) {
```

Insert immediately before it:

```c
static int read_schema_splits(const char *db_root, const char *object) {
    char conf_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/schema.conf", db_root);
    FILE *f = fopen(conf_path, "r");
    if (!f) return -1;
    char prefix[300];
    snprintf(prefix, sizeof(prefix), "default:%s:", object);
    size_t plen = strlen(prefix);
    char line[512];
    int splits = -1;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, prefix, plen) == 0) {
            sscanf(line + plen, "%d", &splits);
            break;
        }
    }
    fclose(f);
    return splits;
}

```

Then locate this exact anchor inside `test_txn_crash_phase`:

```c
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart rolls back active transaction");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_count(&env, object), 60,
                      "rollback restores all pre-rebuild records");
        char active[PATH_MAX];
        snprintf(active, sizeof(active), "%s/default/%s/.rebuild_txn.active",
                 saved_db_root, object);
        ASSERT_TRUE(access(active, F_OK) != 0,
                    "successful recovery consumes active transaction");
        test_env_stop(&env);
    }
```

Replace with:

```c
    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "restart rolls back active transaction");
    if (env.daemon_pid > 0) {
        ASSERT_EQ_INT(request_count(&env, object), 60,
                      "rollback restores all pre-rebuild records");
        char active[PATH_MAX];
        snprintf(active, sizeof(active), "%s/default/%s/.rebuild_txn.active",
                 saved_db_root, object);
        ASSERT_TRUE(access(active, F_OK) != 0,
                    "successful recovery consumes active transaction");
        ASSERT_EQ_INT(read_schema_splits(saved_db_root, object), 8,
                      "schema.conf splits reverted to pre-rebuild value");
        test_env_stop(&env);
    }
```

Finally, register the new case. Locate this exact anchor near the bottom of
the file:

```c
TEST_REGISTER("test-rebuild-txn-crash-after-walk", test_txn_crash_after_walk)
```

Insert immediately after it:

```c
TEST_REGISTER("test-rebuild-txn-crash-after-metadata-splits", test_txn_crash_after_metadata_splits)
```

And register the new test file's binary entry — `build.sh` already lists
`test_rebuild_txn_recovery.c`, so no `build.sh` change is needed for this
task.

**Verify**: run
`./build/bin/shard-db-test run-all --filter rebuild-txn` and confirm the
new case passes and the two existing crash-phase cases still pass with the
strengthened assertion.

## Task 2 (minor, test): stale assertion in test_rebuild_recovery.c no longer tests anything

`src/test/cases/test_rebuild_recovery.c` still asserts the **old** legacy
staging path is gone post-recovery, but `rebuild_object_v2` no longer ever
creates that path (it uses `.rebuild_txn.active/data` now), so the assertion
is vacuously true regardless of whether the new transaction scheme's own
cleanup works.

Locate this exact anchor:

```c
    /* Bug 1 fix: no .rebuild_legacy_root left behind after successful walk. */
    char legacy_path[PATH_MAX];
    snprintf(legacy_path, sizeof(legacy_path),
             "%s/default/rebuildrecov/.rebuild_legacy_root", env.db_root);
    ASSERT_TRUE(access(legacy_path, F_OK) != 0, "no .rebuild_legacy_root left behind");
```

Replace with:

```c
    /* No rebuild-transaction artifacts left behind after successful walk
       (replaces the old data.legacy/.rebuild_legacy_root staging scheme). */
    char txn_active[PATH_MAX], txn_done[PATH_MAX], txn_preparing[PATH_MAX];
    snprintf(txn_active, sizeof(txn_active),
             "%s/default/rebuildrecov/.rebuild_txn.active", env.db_root);
    snprintf(txn_done, sizeof(txn_done),
             "%s/default/rebuildrecov/.rebuild_txn.done", env.db_root);
    snprintf(txn_preparing, sizeof(txn_preparing),
             "%s/default/rebuildrecov/.rebuild_txn.preparing", env.db_root);
    ASSERT_TRUE(access(txn_active, F_OK) != 0, "no .rebuild_txn.active left behind");
    ASSERT_TRUE(access(txn_done, F_OK) != 0, "no .rebuild_txn.done left behind");
    ASSERT_TRUE(access(txn_preparing, F_OK) != 0, "no .rebuild_txn.preparing left behind");
```

**Verify**: `./build/bin/shard-db-test run test-rebuild-recovery` still
passes.

## Task 3 (minor, logging): restore the actionable DB-root-lock failure message

Before this branch, `cmd_server()` printed a specific, actionable message
naming the lock file and telling the operator how to stop the running
instance. The new shared `db_root_lock_acquire()` helper
(`src/db/objlock.c`) replaced it with a generic one that dropped the
"stop it first" hint.

Old message (still in `main`, `src/db/server.c`):

```c
"(lock held on %s). Stop it first with './shard-db stop'.\n",
```

Locate this exact anchor in `src/db/objlock.c`:

```c
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        fprintf(stderr,
                "shard-db: DB root %s is already open by another process\n",
                db_root);
        close(fd);
        return -1;
    }
```

Replace with:

```c
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        fprintf(stderr,
                "shard-db: DB root %s is already open by another process "
                "(lock held on %s). Stop it first with './shard-db stop'.\n",
                db_root, lockpath);
        close(fd);
        return -1;
    }
```

**Verify**: no dedicated test exists for this message (it wasn't tested
before either); confirm by manual smoke test — start a daemon, attempt a
second `./shard-db start` against the same `DB_ROOT`, and check stderr
contains the restored hint. Do not add a new test for stderr text; this is
a message-content nit, not a behavior change.

## Execution notes — 2026-07-19

The new metadata-splits case was first built with only
`rebuild_txn_abort()`'s schema restore temporarily replaced by a no-op. It
failed on the intended assertion while data rollback still succeeded:

```text
./build/bin/shard-db-test run test-rebuild-txn-crash-after-metadata-splits
ok 7 - rollback restores all pre-rebuild records
ok 8 - successful recovery consumes active transaction
not ok 9 - schema.conf splits reverted to pre-rebuild value
#   expected 8 got 16
# test-rebuild-txn-crash-after-metadata-splits: 8 passed, 1 failed
```

After restoring `update_schema_conf_splits_streams()`, all three crash-phase
cases passed the strengthened schema assertion:

```text
./build/bin/shard-db-test run-all --filter rebuild-txn
# total: 56 passed, 0 failed across 7 cases
```

The replacement transaction-artifact assertions also passed:

```text
./build/bin/shard-db-test run test-rebuild-recovery
# test-rebuild-recovery: 110 passed, 0 failed
```

The manual second-daemon smoke test printed the restored operator hint:

```text
shard-db: DB root <tmp-db-root> is already open by another process
(lock held on <tmp-db-root>/.shard-db.lock).
Stop it first with './shard-db stop'.
```

Final release validation:

```text
SKIP_TESTS=1 ./build.sh
Built: build/bin/

./build/bin/shard-db-test run-all
# total: 10441 passed, 0 failed across 301 cases
```

The temporary mutation was removed. Task 1 and Task 2 changed tests only;
the only production-code follow-up is Task 3's message string in
`src/db/objlock.c`. `src/db/query_find.c` was not changed by this follow-up.

## Definition of done

- [x] Task 1's new test fails if you temporarily revert
      `rebuild_txn_abort()`'s `update_schema_conf_splits_streams` call to a
      no-op, then passes once restored — paste both outputs.
- [x] `./build/bin/shard-db-test run-all` — full suite green, paste the
      final tally.
- [x] `git diff main -- src/db/objlock.c src/db/query_find.c` shows no
      change beyond Task 3's message string (Task 1/2 are test-only).
- [x] Leave the branch uncommitted for review, per this repo's standing
      execution-mode exception.
