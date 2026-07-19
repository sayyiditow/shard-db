# Fix: CodeQL TOCTOU alerts (#184-191)

Source: GitHub code-scanning alerts #184-#191 on `main`, rule
`cpp/toctou-race-condition` (CWE-367), across three files:

- `src/test/cases/test_vacuum_recount_validation.c` — alerts #184-#187
  (pre-existing, from commit `4a7bbbf`).
- `src/db/objlock.c` — alerts #188, #189 (new, from the just-merged
  `fix/rebuild-txn-recovery` branch, PR #255 / commit `342e268`).
- `src/test/cases/test_rebuild_txn_recovery.c` — alerts #190, #191 (new,
  same branch).

Execution mode for this repo (per `CLAUDE.md`): planner/executor for this
task is Claude, executing directly in this session on a fresh branch off
`main`. Leave all changes uncommitted for review (human reviews the raw
`git diff` before anything is committed). Build with
`SKIP_TESTS=1 ./build.sh`; test with `./build/bin/shard-db-test run[-all]`.
The `fix/rebuild-txn-recovery` branch has already merged to `main`
(commit `14cd218`, confirmed via `git log --oneline -- <file>` showing no
pending work on either target file beyond that merge) — no further wait
needed.

If a quoted anchor does not match exactly, write `PLAN_NOTES.md` and halt
the entire run. Do not reinterpret a stale anchor.

## Root cause

CodeQL's `cpp/toctou-race-condition` query flags any `stat()`/`lstat()`/
`access()` check on a path followed by a later operation on the *same
filename string* rather than a held file descriptor — the classic
"resolve, check, resolve-again, act" gap where the underlying filesystem
entry could change between the two path resolutions. Three distinct
instances of this pattern exist across two files (test code) and one
instance in production code, each needing a slightly different fix:

### 1. `test_vacuum_recount_validation.c` (alerts #184-187) — test only

Two test functions each `stat()` a kf shard file by path to capture its
original permission bits, then later `chmod()` the *same path string* to
revoke permissions (fault injection), then later still `chmod()` the path
string again to restore the original bits:

- `test_vacuum_recount_object_not_open_run`: `stat(kf_path, &kf_st)` at
  line 211 is followed by `chmod(kf_path, 0)` at line 227 (alert #186) and
  by `chmod(kf_path, original_mode)` at line 268 (alert #187).
- `test_recount_kf_header_read_failure_run`: `stat(kf_path, &kf_st)` at
  line 341 is followed by `chmod(kf_path, 0)` at line 350 (alert #184) and
  by `chmod(kf_path, original_mode)` at line 363 (alert #185).

**Fix:** pin the fd once via `open()`, then `fstat()`/`fchmod()` on that
fd for every subsequent check/mutation, per CodeQL's own stated
remediation.

**Exploitability:** CodeQL classifies all four `"test"`. `kf_path` lives
under a per-test `TestEnv` tmpdir created and torn down within a single
test run; nothing else touches that path between check and use, and
`run-all`'s parallel workers each get a uniquely-named db_root. Not
exploitable. Fixed anyway because it's the correct idiom, it's small and
mechanical, and it clears 4 open "High" alerts without a dismissal entry.

### 2. `src/db/objlock.c` (alerts #188, #189) — production code

`copy_regular_file_atomic()` (called from `rebuild_txn_begin` to snapshot
`fields.conf` into the transaction dir, and from the commit path to
restore it) does `lstat(src, &st)` at line 222-223 to reject symlinks and
capture mode bits, then `open(src, O_RDONLY)` at line 224 on the same path
string (alert #188).

`parse_meta()` (called during startup recovery to read a rebuild
transaction's `meta` file) does `lstat(path, &st)` at line 279-281 to
reject symlinks/oversized files, then `fopen(path, "r")` at line 282 on
the same path string (alert #189).

**Fix:** open first (with `O_NOFOLLOW` to preserve the original
symlink-rejection behavior — an `lstat` that finds a symlink fails
`S_ISREG`; `open(..., O_NOFOLLOW)` on a symlink fails with `ELOOP`, same
net effect), then `fstat()` the returned fd for the mode/size checks that
used to run against the separately-resolved `lstat` result.

**Exploitability:** both paths (`txn->fields_path`, `txn->fields_rollback`,
`txn->active/meta`) are daemon-internal paths under `DB_ROOT`, constructed
by the daemon itself from the object's own directory — not attacker-
controlled network input. An attacker able to race a symlink swap here
would already need local filesystem write access to `DB_ROOT`, at which
point far larger issues exist. Still fixed to CodeQL's idiom: it's
production code so the fd-pinning is worth doing for real defense-in-depth
(a local co-tenant process with stray write access to `DB_ROOT` is a more
plausible threat than in the test-tmpdir cases above), and this diff gets
the full review pass in step 4 of `CORE-PROCESS.md` (concurrency +
security boundary) rather than the lighter test-only treatment.

### 3. `test_rebuild_txn_recovery.c` (alerts #190, #191) — test only

Two test functions each poll `wait_for_path()` (line 115-121, itself just
a loop of `access(path, F_OK)`) until a daemon-written pause marker file
appears, then **re-check** `access(marker, F_OK) == 0` immediately before
`unlink(marker)` — a redundant second resolution of the same path alert
#190 at line 313 (`test_txn_crash_phase`, shared by three registered
tests) and alert #191 at line 415 (`test_edit_crash_after_metadata`).

**Fix:** there's no fd-based equivalent for "does this path exist, then
unlink it" (unlink is inherently path-based), but the second `access()`
call is provably redundant here: `wait_for_path()`'s return value already
tells us whether the marker was observed to exist (it returns 0 only after
an `access()` call inside its own loop succeeded). Reusing that
already-known result instead of re-resolving the path a second time
removes the check-then-act gap entirely rather than papering over it.

**Exploitability:** CodeQL classifies both `"test"`. Same per-test tmpdir
reasoning as section 1. Not exploitable; fixed because it's a genuine
redundant check (not just an idiom swap) and clears 2 more open alerts.

## Scope of the guarantee

This is a hardening/refactor of existing crash-injection and fault-
injection code, not a behavior bug fix. There is no reproducible runtime
failure to demonstrate red→green on. The regression protection here is:
every affected test must keep passing with byte-identical assertions
before and after, and no other test may regress (an fd leak or a missed
`close`/restore would show up as a failure in a *later* test that reuses
the same object/dir/fd table, since `run-all` reuses worker processes
across cases in non-`--jobs 1` mode). CodeQL itself cannot be run locally
(no `codeql` CLI in this environment; the repo's only CodeQL entry point
is the GitHub Actions workflow on push/PR to `main`), so alert closure can
only be confirmed after this branch's PR triggers that workflow — call
this out explicitly in the PR description rather than claiming local proof
of it.

Because section 2 touches production code (`objlock.c`), that part of the
diff gets a full `CORE-PROCESS.md` step-4 review pass (correctness,
security, resource handling, concurrency, crash safety) — not just the
"test-only, low risk" reasoning that covers sections 1 and 3.

## Tasks

### Task 1 — confirm current behavior before touching anything

Build and run all six affected tests plus the surrounding rebuild-txn
suite, to record a baseline pass:

```text
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-vacuum-recount-object-not-open
./build/bin/shard-db-test run test-recount-kf-header-read-failure
./build/bin/shard-db-test run-all --filter rebuild-txn
```

Paste all outputs (expected: all pass, matching existing behavior this
plan must not change).

### Task 2 — fix `test_vacuum_recount_object_not_open_run`

**File:** `src/test/cases/test_vacuum_recount_validation.c`.

Add `#include <fcntl.h>` immediately after the existing include block:

```c
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
```

becomes:

```c
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
```

Replace this exact block (the `stat`-based permission capture plus the
first `chmod`):

```c
    struct stat kf_st;
    int stat_rc = stat(kf_path, &kf_st);
    ASSERT_EQ_INT(stat_rc, 0, "stat kf shard before open-failure injection");
    if (stat_rc != 0) {
        tc_close(tc);
        test_env_stop(&env);
        return 1;
    }
    mode_t original_mode = kf_st.st_mode & 07777;

    tc_close(tc);
    tc = NULL;
    test_env_stop_keep(&env);

    int chmod_changed = 0;
    int restarted = 0;
    TestClient *tc2 = NULL;
    int chmod_rc = chmod(kf_path, 0);
    ASSERT_EQ_INT(chmod_rc, 0, "revoke kf shard permissions");
    if (chmod_rc != 0) goto cleanup;
    chmod_changed = 1;
```

with:

```c
    int kf_fd = open(kf_path, O_RDONLY);
    ASSERT_TRUE(kf_fd >= 0, "open kf shard fd before open-failure injection");
    if (kf_fd < 0) {
        tc_close(tc);
        test_env_stop(&env);
        return 1;
    }
    struct stat kf_st;
    int fstat_rc = fstat(kf_fd, &kf_st);
    ASSERT_EQ_INT(fstat_rc, 0, "fstat kf shard fd before open-failure injection");
    if (fstat_rc != 0) {
        close(kf_fd);
        tc_close(tc);
        test_env_stop(&env);
        return 1;
    }
    mode_t original_mode = kf_st.st_mode & 07777;

    tc_close(tc);
    tc = NULL;
    test_env_stop_keep(&env);

    int chmod_changed = 0;
    int restarted = 0;
    TestClient *tc2 = NULL;
    int chmod_rc = fchmod(kf_fd, 0);
    ASSERT_EQ_INT(chmod_rc, 0, "revoke kf shard permissions");
    if (chmod_rc != 0) goto cleanup;
    chmod_changed = 1;
```

Note: `kf_fd` stays open across `test_env_stop_keep`/`test_env_start_at`.
This is safe — the fd belongs to the test process, not the daemon child
process; stopping/restarting the daemon does not unlink or recreate the kf
shard file, so the same open fd continues to refer to the same inode
throughout. Do not close and reopen it around the restart.

Replace this exact cleanup block:

```c
cleanup:
    free(resp);
    if (tc2) tc_close(tc2);
    if (chmod_changed) {
        ASSERT_EQ_INT(chmod(kf_path, original_mode), 0,
                      "restore kf shard permissions");
    }
    if (restarted) {
        test_env_stop(&env);
    } else if (test_env_start_at(&env, saved_db_root, saved_port) == 0) {
        test_env_stop(&env);
    }
    return 0;
```

with:

```c
cleanup:
    free(resp);
    if (tc2) tc_close(tc2);
    if (chmod_changed) {
        ASSERT_EQ_INT(fchmod(kf_fd, original_mode), 0,
                      "restore kf shard permissions");
    }
    close(kf_fd);
    if (restarted) {
        test_env_stop(&env);
    } else if (test_env_start_at(&env, saved_db_root, saved_port) == 0) {
        test_env_stop(&env);
    }
    return 0;
```

`close(kf_fd)` is unconditional in `cleanup` because every path that
reaches the `cleanup:` label runs after `kf_fd` was successfully opened
(the two failure paths that precede the open succeeding return directly,
without going through `cleanup`).

### Task 3 — fix `test_recount_kf_header_read_failure_run`

**File:** `src/test/cases/test_vacuum_recount_validation.c`.

Replace this exact block:

```c
    struct stat kf_st;
    int stat_rc = stat(kf_path, &kf_st);
    ASSERT_EQ_INT(stat_rc, 0, "stat kf shard before recount read failure");
    if (stat_rc != 0) {
        tc_close(tc);
        test_env_stop(&env);
        return 1;
    }
    mode_t original_mode = kf_st.st_mode & 07777;

    int chmod_rc = chmod(kf_path, 0);
    ASSERT_EQ_INT(chmod_rc, 0, "revoke kf shard permissions while cached");
```

with:

```c
    int kf_fd = open(kf_path, O_RDONLY);
    ASSERT_TRUE(kf_fd >= 0, "open kf shard fd before recount read failure");
    if (kf_fd < 0) {
        tc_close(tc);
        test_env_stop(&env);
        return 1;
    }
    struct stat kf_st;
    int fstat_rc = fstat(kf_fd, &kf_st);
    ASSERT_EQ_INT(fstat_rc, 0, "fstat kf shard before recount read failure");
    if (fstat_rc != 0) {
        close(kf_fd);
        tc_close(tc);
        test_env_stop(&env);
        return 1;
    }
    mode_t original_mode = kf_st.st_mode & 07777;

    int chmod_rc = fchmod(kf_fd, 0);
    ASSERT_EQ_INT(chmod_rc, 0, "revoke kf shard permissions while cached");
```

Replace this exact block (the restore inside the `if (chmod_rc == 0)`
branch):

```c
        free(resp); resp = NULL;
        ASSERT_EQ_INT(chmod(kf_path, original_mode), 0,
                      "restore cached object's kf shard permissions");
    }

    free(resp);
    tc_close(tc);
    test_env_stop(&env);
    return 0;
```

with:

```c
        free(resp); resp = NULL;
        ASSERT_EQ_INT(fchmod(kf_fd, original_mode), 0,
                      "restore cached object's kf shard permissions");
    }

    close(kf_fd);
    free(resp);
    tc_close(tc);
    test_env_stop(&env);
    return 0;
```

### Task 4 — fix `copy_regular_file_atomic` in `src/db/objlock.c`

**File:** `src/db/objlock.c`. No new includes needed (`fcntl.h`,
`sys/stat.h`, `unistd.h` already come in via `types.h`).

Replace this exact block:

```c
static int copy_regular_file_atomic(const char *src, const char *dst,
                                    const char *tmp) {
    struct stat st;
    if (lstat(src, &st) != 0 || !S_ISREG(st.st_mode)) return -1;
    int in = open(src, O_RDONLY);
    if (in < 0) return -1;
    int out = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, st.st_mode & 0777);
    if (out < 0) { close(in); return -1; }
```

with:

```c
static int copy_regular_file_atomic(const char *src, const char *dst,
                                    const char *tmp) {
    int in = open(src, O_RDONLY | O_NOFOLLOW);
    if (in < 0) return -1;
    struct stat st;
    if (fstat(in, &st) != 0 || !S_ISREG(st.st_mode)) { close(in); return -1; }
    int out = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, st.st_mode & 0777);
    if (out < 0) { close(in); return -1; }
```

`O_NOFOLLOW` preserves the original symlink-rejection semantics: the old
`lstat()` would see a symlink's own mode (not `S_ISREG`) and reject it;
`open(..., O_NOFOLLOW)` on a symlink fails with `ELOOP`, so `in < 0` and
the function returns `-1` the same as before. The rest of the function
(read/write loop, close/rename/unlink-on-failure) is unchanged — `st` is
still in scope and still used for `st.st_mode & 0777` below.

### Task 5 — fix `parse_meta` in `src/db/objlock.c`

**File:** `src/db/objlock.c`.

Replace this exact block:

```c
static int parse_meta(RebuildTxn *txn) {
    char path[PATH_MAX];
    if (path_join2(path, sizeof(path), txn->active, "meta") != 0) return -1;
    struct stat st;
    if (lstat(path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size <= 0 ||
        st.st_size > 1024) return -1;
    FILE *f = fopen(path, "r");
    if (!f) return -1;
```

with:

```c
static int parse_meta(RebuildTxn *txn) {
    char path[PATH_MAX];
    if (path_join2(path, sizeof(path), txn->active, "meta") != 0) return -1;
    int fd = open(path, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size <= 0 ||
        st.st_size > 1024) {
        close(fd);
        return -1;
    }
    FILE *f = fdopen(fd, "r");
    if (!f) {
        close(fd);
        return -1;
    }
```

The rest of the function (the `fgets` loop and the single `fclose(f)`
further down) is unchanged — `fclose(f)` closes the underlying `fd` too
since `fdopen()` took ownership of it, so no separate `close(fd)` is
needed on that path.

### Task 6 — fix `test_txn_crash_phase` in `test_rebuild_txn_recovery.c`

**File:** `src/test/cases/test_rebuild_txn_recovery.c`. Shared helper used
by `test-rebuild-txn-crash-after-stage`,
`test-rebuild-txn-crash-after-walk`, and
`test-rebuild-txn-crash-after-metadata-splits`.

Replace this exact block:

```c
    ASSERT_EQ_INT(marker_rc, 0,
                  "rebuild reaches deterministic pause");
    if (access(marker, F_OK) == 0) {
        test_env_kill(&env);
        unlink(marker);
    }
    if (request_pid > 0) waitpid(request_pid, NULL, 0);
```

with:

```c
    ASSERT_EQ_INT(marker_rc, 0,
                  "rebuild reaches deterministic pause");
    if (marker_rc == 0) {
        test_env_kill(&env);
        unlink(marker);
    }
    if (request_pid > 0) waitpid(request_pid, NULL, 0);
```

`marker_rc` (set a few lines above by
`int marker_rc = wait_for_path(marker, 5000);`) already tells us whether
the marker was observed to exist — `wait_for_path()` returns 0 only after
an internal `access(path, F_OK) == 0` succeeded. Reusing it removes the
redundant second path resolution instead of re-checking with a fresh
`access()` call immediately before `unlink()`.

### Task 7 — fix `test_edit_crash_after_metadata` in `test_rebuild_txn_recovery.c`

**File:** `src/test/cases/test_rebuild_txn_recovery.c`. Registered as
`test-rebuild-txn-edit-field-after-metadata`.

Replace this exact block:

```c
    ASSERT_EQ_INT(wait_for_path(marker, 5000), 0,
                  "edit rebuild reaches metadata pause");
    if (access(marker, F_OK) == 0) {
        test_env_kill(&env);
        unlink(marker);
    }
    if (request_pid > 0) waitpid(request_pid, NULL, 0);
```

with:

```c
    int marker_rc = wait_for_path(marker, 5000);
    ASSERT_EQ_INT(marker_rc, 0,
                  "edit rebuild reaches metadata pause");
    if (marker_rc == 0) {
        test_env_kill(&env);
        unlink(marker);
    }
    if (request_pid > 0) waitpid(request_pid, NULL, 0);
```

Same fix as Task 6, applied to the sibling function that previously called
`wait_for_path()` inline inside the `ASSERT_EQ_INT` rather than storing its
result first (matching the style `test_txn_crash_phase` already uses).

### Task 8 — rebuild and rerun, compare to baseline

```text
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-vacuum-recount-object-not-open
./build/bin/shard-db-test run test-recount-kf-header-read-failure
./build/bin/shard-db-test run-all --filter rebuild-txn
./build/bin/shard-db-test run-all
```

Paste all outputs. The six targeted tests must pass with the same
assertions as the Task 1 baseline (no new failures, no skipped
assertions). `run-all` must show no new failures anywhere else in the
suite (an fd leak from a missed `close` on some path this plan didn't
anticipate would most plausibly surface as an `EMFILE`/resource failure
much later in the run — if `run-all` shows anything new, stop and ask
rather than rerunning until green).

Because this touches file-descriptor lifetime in both test code and
production code, also run:

```text
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all --filter vacuum-recount
./build/bin/shard-db-test run-all --filter rebuild-txn
```

Do not suppress or waive a sanitizer finding.

### Task 9 — open the PR and let CodeQL confirm closure

Push the branch and open the PR as normal (human runs this step per repo
git-safety rules). In the PR description, note explicitly that alert
closure (#184-#191) cannot be verified locally and must be confirmed from
the PR's own CodeQL run once CI completes — check
`https://github.com/sayyiditow/shard-db/security/code-scanning` (or the
PR's checks tab) after CI finishes, rather than assuming closure from the
diff alone. Also note that the `objlock.c` portion of this diff (Tasks 4-5)
touches production code and should get the full `CORE-PROCESS.md` step-4
review pass (a reviewer blind to this plan), not just the lighter
test-only treatment given to the other tasks.

## Definition of done

- [ ] All four `chmod(path, ...)` calls flagged by alerts #186/#187/#184/#185
      are replaced by `fchmod(fd, ...)` on a single fd opened once per
      test, per function, before the first permission mutation.
- [ ] The `lstat(path,...)` + `open(path,...)` pair (alert #188) and the
      `lstat(path,...)` + `fopen(path,...)` pair (alert #189) in
      `objlock.c` are replaced by `open(..., O_NOFOLLOW)` + `fstat(fd,...)`
      on the same fd, preserving the original symlink-rejection behavior.
- [ ] The redundant `access(marker, F_OK)` re-checks (alerts #190/#191) in
      `test_rebuild_txn_recovery.c` are replaced by reusing the stored
      `wait_for_path()` result.
- [ ] No `stat`/`lstat`/`access` remains on a path that is later mutated or
      opened by that same path string, anywhere touched by this plan.
- [ ] Every fd opened is closed on every exit path (verified by reading
      each function's full control flow, not just the primary path).
- [ ] The six targeted tests pass with assertions unchanged in substance
      from the Task 1 baseline.
- [ ] Full suite (`run-all`) shows no new failures relative to `main`.
- [ ] Applicable ThreadSanitizer runs are green.
- [ ] No compiler warnings, debug artifacts, or unrelated changes — this
      diff touches only: the two functions + include list in
      `test_vacuum_recount_validation.c`; `copy_regular_file_atomic` and
      `parse_meta` in `objlock.c`; the two marker-check blocks in
      `test_rebuild_txn_recovery.c`.
- [ ] PR description flags that CodeQL alert closure must be confirmed
      from the PR's own CI run, not asserted from the diff, and flags the
      `objlock.c` portion for a full (non-plan-aware) review pass.
- [ ] Actual before/after test outputs are pasted into the execution
      notes.
