# 2026.08.2: VARIABLE-only segments and permanent migration retirement

Planning document only. Do not execute this plan, change the release tags, or
publish artifacts until the current 2026.08.1 production validation is
complete.

## Execution protocol

Execute this plan only after explicit human approval, from a fresh
`refactor/variable-only-segments` branch created from the default branch.
Execute tasks in order and leave the complete change uncommitted for raw-diff
review, per this repository's standing exception.

Every production task starts with its named failing test or compile/API audit.
Capture and paste the expected red output before implementing that task, then
capture and paste the green output afterward. Do not weaken, remove, skip, or
repeatedly rerun a failing test to obtain green. A failure that passes on rerun
remains a bug until root-caused or explicitly escalated.

Quoted anchors are mandatory. If an anchor is not found exactly, write
`PLAN_NOTES.md` with the missing anchor and nearby current source, then halt
the entire execution run. Do not reinterpret the edit or continue to another
task. Resumption requires the human or planning model to patch or replace this
plan. If implementation reaches a decision not covered here, stop and ask.

All shell commands must be prefixed with `rtk`; in a command chain, prefix
each segment. Build with `rtk env SKIP_TESTS=1 ./build.sh` and run tests with
`rtk ./build/bin/shard-db-test ...`. Do not run benchmarks; the user owns
benchmark execution.

This plan is for **2026.08.2**, immediately after 2026.08.1. It supersedes
the earlier assumption that 2026.08.2 would perform an automatic
FIXED-to-VARIABLE conversion. The last release that performs that conversion
is 2026.08.1. The 2026.08.2 binary must contain no storage migration code,
startup migration, `.format` checker, migration CLI, migration JSON mode, or
npm migration API.

## Release contract

The upgrade boundary is deliberately strict:

1. Operators must stop writes and open the root once with 2026.08.1.
2. That 2026.08.1 startup sweep must finish successfully and the process must
   be shut down cleanly.
3. Only then may the binary be replaced with 2026.08.2.

The 2026.08.1 sweep is therefore the final conversion window. The operator
must not create or restore objects after that sweep and before the 2026.08.2
cutover. The upgrade documentation must make this quiesce/sweep/clean-stop
sequence explicit; a version marker alone cannot detect a FIXED object created
after the last 2026.08.1 sweep.

2026.08.2 remains strict in both daemon and embedded paths. Keep a small
`.version` compatibility marker, but rename the code and messages around it
from migration to compatibility/version gating:

| Root state | 2026.08.2 result |
|---|---|
| Filesystem-empty root with no marker | Allow, initialize, and stamp `2026.08.2`. |
| Non-empty root with marker exactly `2026.08.1` | Allow without format scanning or conversion; run ordinary crash recovery, then atomically stamp `2026.08.2`. |
| Non-empty root with marker `2026.08.2` | Allow with no-op compatibility check. |
| Non-empty root with no marker | Refuse: the root cannot prove it passed through 2026.08.1. |
| Non-empty root with a version older than `2026.08.1` | Refuse and instruct the operator to open it with 2026.08.1 first. |
| Root with a version newer than `2026.08.2` | Refuse as a downgrade. |
| Non-empty root with malformed/unreadable version evidence | Refuse without mutating the root. |

Implement the exact prior-release requirement rather than treating a minimum
version as informational. `SHARD_DB_VERSION` becomes `2026.08.2`, the
required source becomes exactly `2026.08.1`, and the compatibility decision is
enforced. Do not use the compatibility check to run reindex, rewrite segment
files, repair objects, or otherwise migrate data.

The two invalid-evidence rows are intentionally one public refusal class.
Missing `.version` on a non-empty root and malformed/unreadable `.version`
both return `SHARD_DB_VERSION_INVALID` and print the same conservative message:
`shard-db: non-empty DB_ROOT lacks valid 2026.08.1/2026.08.2 compatibility
evidence`. Tests distinguish the fixtures and prove both are non-mutating, but
must not expect separate error codes or diagnostics. Too-old and downgrade
evidence retain their distinct decisions/messages.

The `.format` marker is no longer part of the 2026.08.2 storage contract.
Remove its reader, writer, and checker. Any stale marker left by an earlier
release is inert and must not be interpreted, deleted, or used to decide how
to open a segment. 2026.08.2 opens all segments through the VARIABLE path by
construction. This is safe only because the documented 2026.08.1 handoff is a
required operational precondition.

“Filesystem-empty” is literal. After acquiring the DB-root lock, enumerate
the root and ignore only `.` / `..` and `.shard-db.lock`, which is required to
serialize this decision. Any other entry — including an empty `schema.conf`,
configuration/auth files, an orphan object directory, `.version`, `.format`,
a clean marker, logs, or an unknown file — makes the root non-empty. The lock
file is the sole permitted mutation before an incompatible root is rejected;
no instance initialization, cleanup sweep, recovery, log creation,
clean-marker consumption, version stamp, or object mutation may precede the
read-only compatibility decision.

The compatibility seam is split into a read-only check and a later atomic
stamp. Daemon and embedded startup use this exact order:

```text
mkdir root if absent → acquire root lock → read-only version check →
initialize process state → perform ordinary crash recovery → validate all
schema and object metadata → atomically stamp when required → start worker
pools / accept requests
```

If initialization, recovery, schema enumeration, or metadata validation fails,
do not stamp. A crash before stamping leaves `2026.08.1` and is safe to retry
with 2026.08.2; a successful stamp is the commit point after which 2026.08.1
downgrade is unsupported. The compatible-open byte-comparison test uses a clean
fixture with no pending recovery evidence. It permits only `.version`
advancement and consumption of the `.shard-db.clean` lifecycle flag; ordinary
recovery may change files when recovery evidence exists.

### Segment file-id policy

`048000.dat` is not a VARIABLE-format requirement. It is the temporary
collision-avoidance range used by the 2026.08.1 FIXED-to-VARIABLE converter
while old low-numbered source files still exist. A VARIABLE-only object may
and should start its first stream file at the ordinary `000000.dat` name;
file IDs do not encode the segment format.

The 2026.08.2 binary must therefore allocate fresh VARIABLE segment files
from `000000.dat` onward. Existing roots converted by 2026.08.1 may retain
`048000.dat` and later IDs; 2026.08.2 opens and serves those files normally
without treating the number as a format marker or renumbering them during
ordinary startup.

There is no second in-place full-repack engine in 2026.08.2. Delete
`slotcask_compact()` and both of its command surfaces. The sole supported full
repack is `vacuum` with `"compact":true`, which already uses the
`rebuild_object()` transaction: it stages the old data as rollback evidence,
builds a fresh canonical data directory, commits through the rebuild marker,
and is resolved by startup recovery if interrupted. A successful heavy vacuum
therefore creates a fresh segment generation beginning at `000000.dat` and
removes the old `048000+` generation when transaction cleanup completes.

Retain `slotcask_compact_segs()` for lightweight sparse-segment merging and
`slotcask_compact_kf()` for keyfile tombstone cleanup. These are the existing
no-flags vacuum path, not a substitute full repacker. Preserve their current
crash/resynchronization protocol while collapsing their FIXED/VARIABLE
dispatch to the remaining VARIABLE implementation.

Keep the O_DIRECT full-scan architecture unchanged. Full-query scans continue
to enumerate segment files and read them sequentially in parallel; do not
replace them with KF-driven random record fetches and do not add a per-record
KF lookup to the hot scan callback. The retained storage invariant is that
normal mutations and lightweight compaction tombstone a displaced segment
record, so exactly one segment copy for a key remains marked live. The removed
`slotcask_compact()` must not be replaced by another in-place copier that can
leave two live copies.

Every parsed numeric segment filename must fit the KF's `uint16_t` file-id
field. `065535.dat` is valid; `065536.dat`, `999999.dat`, parse overflow, and
any `.dat` name outside the exact six-digit grammar are corruption and
must fail open/recovery rather than be skipped or influence active-file
selection. Unrelated non-segment files retain their existing disposition.

## Scope boundary

“Fixed” in this plan means the old **fixed-size segment record format** only.
Do not remove or rename unrelated concepts:

- typed record payloads still have schema-defined field widths;
- wire response `format` values such as `rows`, `csv`, and `dict` remain;
- the on-disk schema engine-version field `2` and the historical pre-
  2026.05.5 v1 migration documentation remain accurate;
- `reindex`, `vacuum`, and schema export/import remain maintenance or
  schema-transfer operations, not upgrade migrations; the standalone
  `compact` command and JSON mode are removed in favor of transactional
  `vacuum` with `"compact":true`.

Use this distinction in the implementation audit so a broad search for
`format` or `migration` does not remove protocol formatting, typed-record
documentation, or historical compatibility notes.

## Task 1 — replace startup migration with strict compatibility gating

### 1a. Red tests

Before production edits, replace the tests registered by the exact build
anchors `src/test/cases/test_startup_auto_migration.c \` and
`src/test/cases/test_startup_format_sweep.c \` with
`test_version_compat.c` and `test_version_startup_paths.c`. The first test
bit-tests every compatibility-table row plus these filesystem cases: truly
empty; lock-file-only; empty `schema.conf`; config-only; orphan object data;
unreadable root; unreadable/malformed marker. The second launches both daemon
and embedded opens. Refusal snapshots ignore only `.shard-db.lock` and permit
no other change. The shared pre-pool seam on a clean 2026.08.1 root changes
only `.version` and the clean lifecycle flag. A full successful daemon start
may additionally create its documented PID/log runtime artifacts, but hashes
of schema, object metadata, KF, index, and segment files remain identical;
recovery or metadata-validation failure does not stamp; success stamps only
after validation and before pools start. Include clean-marked 2026.08.1 roots
with malformed `schema.conf` and unreadable object metadata; daemon and
embedded must reject them while `.version` remains
2026.08.1. Segment-file-ID rejection is added test-first with the shared
validator extension in Task 2c, after the strict parser exists. Run both new
cases and capture their expected compile/link failure against the old API.

### 1b. Version API and startup order

Replace the complete block anchored by
`#define SHARD_DB_VERSION "2026.08.1"` in `src/db/version.h` with:

```c
#define SHARD_DB_VERSION "2026.08.2"
#define SHARD_DB_REQUIRED_SOURCE_VERSION "2026.08.1"

#define SHARD_DB_VERSION_FILE_OK       0
#define SHARD_DB_VERSION_FILE_MISSING  1
#define SHARD_DB_VERSION_FILE_ERROR   (-1)

enum ShardDbVersionStampResult {
    SHARD_DB_VERSION_STAMP_OK = 0,
    SHARD_DB_VERSION_STAMP_FAILED = -1,
    SHARD_DB_VERSION_STAMP_UNCERTAIN = -2
};

enum ShardDbVersionDecision {
    SHARD_DB_VERSION_NOOP = 0,
    SHARD_DB_VERSION_STAMP = 1,
    SHARD_DB_VERSION_TOO_OLD = -3,
    SHARD_DB_VERSION_DOWNGRADE = -2,
    SHARD_DB_VERSION_INVALID = -4
};
```

Replace the declarations anchored by
`/* Version file helpers (embedded.c) */` in `src/db/types.h` with this
complete public seam:

```c
/* Read-only compatibility check. Caller holds the DB-root lock. */
int shard_db_version_check(const char *db_root,
                           char *out_disk_version, size_t out_sz);
/* Atomic compatibility commit, called only after recovery and validation. */
int shard_db_version_stamp(const char *db_root);
int shard_db_version_file_read(const char *db_root, char *out, size_t out_sz);
int shard_db_version_file_write(const char *db_root, const char *version);
```

In `src/db/util.c`, replace the complete function anchored by
`int shard_db_version_decide(`. Its resulting decision table is exhaustive:
filesystem-empty plus missing marker is `STAMP`; non-empty plus missing,
malformed, unreadable, or any older version other than exactly 2026.08.1 is a
refusal; exact 2026.08.1 is `STAMP`; exact 2026.08.2 is `NOOP`; newer is
`DOWNGRADE`. Remove the obsolete `minimum_version` and `has_migration`
parameters from its declaration and every test caller.

The complete replacement function is:

```c
int shard_db_version_decide(const char *disk_version, int version_present,
                            int db_empty, const char *current_version,
                            const char *required_source_version) {
    if (!current_version || !required_source_version ||
        !shard_db_version_is_valid(current_version) ||
        !shard_db_version_is_valid(required_source_version))
        return SHARD_DB_VERSION_INVALID;
    if (db_empty)
        return version_present ? SHARD_DB_VERSION_INVALID
                               : SHARD_DB_VERSION_STAMP;
    if (!version_present || !disk_version ||
        !shard_db_version_is_valid(disk_version))
        return SHARD_DB_VERSION_INVALID;

    int current_cmp = shard_db_version_compare(disk_version, current_version);
    if (current_cmp > 0) return SHARD_DB_VERSION_DOWNGRADE;
    if (current_cmp == 0) return SHARD_DB_VERSION_NOOP;
    if (strcmp(disk_version, required_source_version) == 0)
        return SHARD_DB_VERSION_STAMP;
    return SHARD_DB_VERSION_TOO_OLD;
}
```

Replace its declaration at the exact anchor
`int shard_db_version_decide(const char *disk_version, int version_present,`
in `src/db/types.h` with:

```c
int shard_db_version_decide(const char *disk_version, int version_present,
                            int db_empty, const char *current_version,
                            const char *required_source_version);
```

In `src/db/embedded.c`, delete the complete functions anchored by
`static int run_startup_migration(`, `static int db_root_is_empty(`,
`static int run_startup_format_sweep(`, and
`int shard_db_startup_migrate(`. At the former seam, add complete helpers that
(1) enumerate the root using the literal emptiness rule above, (2) perform
only marker read/validation and return the enum decision, and (3) call the
existing atomic writer only from `shard_db_version_stamp`.

The new read-only helper bodies are (`shard_db_version_stamp` is defined once,
separately below with the atomic writer):

```c
static int db_root_is_filesystem_empty(const char *db_root) {
    DIR *dir = opendir(db_root);
    if (!dir) return -1;
    int empty = 1;
    int read_errno = 0;
    errno = 0;
    for (struct dirent *de = readdir(dir); de; de = readdir(dir)) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0 ||
            strcmp(de->d_name, ".shard-db.lock") == 0)
            continue;
        empty = 0;
        break;
    }
    if (empty) read_errno = errno;
    int close_rc = closedir(dir);
    if (read_errno != 0 || close_rc != 0) return -1;
    return empty;
}

int shard_db_version_check(const char *db_root,
                           char *out_disk_version, size_t out_sz) {
    char disk_version[64] = {0};
    int empty = db_root_is_filesystem_empty(db_root);
    if (empty < 0) return SHARD_DB_VERSION_INVALID;
    int read_rc = shard_db_version_file_read(db_root, disk_version,
                                              sizeof(disk_version));
    int present = read_rc == SHARD_DB_VERSION_FILE_OK;
    if (read_rc == SHARD_DB_VERSION_FILE_ERROR)
        return SHARD_DB_VERSION_INVALID;
    if (present && out_disk_version && out_sz > 0)
        snprintf(out_disk_version, out_sz, "%s", disk_version);
    return shard_db_version_decide(present ? disk_version : NULL, present,
                                   empty, SHARD_DB_VERSION,
                                   SHARD_DB_REQUIRED_SOURCE_VERSION);
}
```

At the exact function anchor
`int shard_db_version_file_write(const char *db_root, const char *version) {`
in `src/db/embedded.c`, replace the complete function with the following.
The return value deliberately distinguishes a known pre-rename failure from a
post-rename durability failure:

```c
int shard_db_version_file_write(const char *db_root, const char *version) {
    if (!db_root || !version || !version[0])
        return SHARD_DB_VERSION_STAMP_FAILED;
    char path[PATH_MAX];
    char tmp[PATH_MAX];
    snprintf(path, sizeof(path), "%s/.version", db_root);
    snprintf(tmp, sizeof(tmp), "%s/.version.tmp.XXXXXX", db_root);
    int fd = mkstemp(tmp);
    if (fd < 0) return SHARD_DB_VERSION_STAMP_FAILED;

    size_t len = strlen(version);
    const char *p = version;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) {
            (void)close(fd);
            (void)unlink(tmp);
            return SHARD_DB_VERSION_STAMP_FAILED;
        }
        p += n;
        len -= (size_t)n;
    }
    if (write(fd, "\n", 1) != 1 || fsync(fd) != 0) {
        (void)close(fd);
        (void)unlink(tmp);
        return SHARD_DB_VERSION_STAMP_FAILED;
    }
    if (close(fd) != 0) {
        (void)unlink(tmp);
        return SHARD_DB_VERSION_STAMP_FAILED;
    }
    if (rename(tmp, path) != 0) {
        (void)unlink(tmp);
        return SHARD_DB_VERSION_STAMP_FAILED;
    }

    /* Rename is the logical commit. From here onward failure means the new
       name may or may not be durable across a crash, so callers must refuse
       this startup and re-read the marker under the lock on the next one. */
    int dfd = open(db_root, O_RDONLY | O_DIRECTORY);
    if (dfd < 0) return SHARD_DB_VERSION_STAMP_UNCERTAIN;
    int sync_rc = fsync(dfd);
    int close_rc = close(dfd);  /* always close, even if fsync failed */
    if (sync_rc != 0 || close_rc != 0)
        return SHARD_DB_VERSION_STAMP_UNCERTAIN;
    return SHARD_DB_VERSION_STAMP_OK;
}
```

`shard_db_version_stamp` reports the exact failure class and returns it
unchanged; do not collapse it to `0/-1`:

```c
int shard_db_version_stamp(const char *db_root) {
    int rc = shard_db_version_file_write(db_root, SHARD_DB_VERSION);
    if (rc == SHARD_DB_VERSION_STAMP_UNCERTAIN) {
        fprintf(stderr,
                "shard-db: stamp commit uncertain; retry startup to verify "
                ".version\n");
    } else if (rc == SHARD_DB_VERSION_STAMP_FAILED) {
        fprintf(stderr,
                "shard-db: failed to stamp compatible database version\n");
    }
    return rc;
}
```

Tests inject temp write, file sync, pre-rename close, rename, directory open,
directory sync, and post-rename directory-close failures. A failure before
rename returns `SHARD_DB_VERSION_STAMP_FAILED` and leaves the old marker. A
failure after a successful rename returns
`SHARD_DB_VERSION_STAMP_UNCERTAIN`. Both startup callers treat any result
other than `SHARD_DB_VERSION_STAMP_OK` as failure. The shared stamp helper
prints the exact message shown in its code; the daemon releases its root lock
and returns `1`, while embedded open releases its root lock, tears down the
not-yet-started instance, resets `g_instance_open`, and returns `NULL`.
Neither caller may start pools after either result. The next locked startup
resolves an uncertain result only by rerunning `shard_db_version_check` and
rereading `.version`.

Do not insert code immediately after the opening line of a lock-failure `if`.
In `shard_db_open()`, replace the complete hunk from `mkdirp(db_root);`
through and including the closing brace of
`if (db_root_lock_acquire(db_root, &lock_fd) != 0) { ... }`. The resulting
hunk creates the root, acquires the lock, returns on lock failure, then — after
that closing brace — calls `shard_db_version_check` before the exact next
anchor `ShardDb *db = shard_db_open_internal(db_root);`. Store whether the
decision is `STAMP`; every negative decision releases the lock, resets
`g_instance_open`, and returns without initialization.

In `cmd_server()`, replace the complete hunk from `mkdirp(db_root);` through
the closing brace of its `db_root_lock_acquire` failure branch. Call the same
read-only check after the branch and before the exact anchor
`g_shard_db_instance = shard_db_open_internal(db_root);`. Every refusal prints
the matching compatibility error, releases the lock, and returns before log,
cache, cleanup, recovery, clean-marker, PID, daemon, pool, or socket mutation.

### 1c. Shared crash recovery before stamping

The daemon currently performs rebuild recovery plus clean-marker consumption
and durability-marker replay, while embedded open performs only rebuild
recovery. Replace both copies with shared helpers in `embedded.c`, declared at
the `/* Version file helpers (embedded.c) */` seam in `types.h`:

```c
int shard_db_recover_before_stamp(const char *db_root,
                                  int *out_markers_replayed);
int shard_db_validate_before_stamp(const char *db_root);
int shard_db_mark_clean_if_safe(const char *db_root);
```

`shard_db_recover_before_stamp` performs, in order:

1. validate `out_markers_replayed`, set `*out_markers_replayed = 0`, then call
   `rebuild_recovery(db_root)`;
2. read whether `.shard-db.clean` exists;
3. remove and directory-sync that flag so the running process is dirty;
4. if the previous shutdown was not clean, parse every `schema.conf` object,
   take its object write lock, and call `marker_recovery_sweep_object`;
5. fail if schema enumeration, marker replay, close, or directory sync fails.

At the exact `int clean_flag_write(` and `int clean_flag_remove(` anchors in
`slotcask.c`, replace their bodies so `write`, file `fsync`, `close`, `unlink`,
and the existing checked `fsync_dir(data_dir)` all propagate failure. Do not
retain the current best-effort directory sync. The compatibility stamp must
not follow a clean-marker mutation whose durability could not be established.

A missing `schema.conf` is an empty object set, not an error. Malformed active
schema lines and unreadable metadata fail closed. This helper contains the
complete code moved from the daemon marker-recovery block; `server.c` no
longer owns a private copy.

The daemon must preserve the existing observable recovery statistic. At the
exact current assignment anchor
`g_marker_recovery_ran = (markers_replayed > 0) ? 1 : 0;`, remove that
assignment together with the private recovery block. At the shared-helper call
site in `cmd_server`, use the out-parameter and restore the assignment with
this complete hunk:

```c
int markers_replayed = 0;
if (shard_db_recover_before_stamp(db_root, &markers_replayed) != 0) {
    fprintf(stderr,
            "shard-db: refusing to start: crash recovery failed; "
            "manual investigation required\n");
    db_root_lock_release(&lock_fd);
    return 1;
}
g_marker_recovery_ran = markers_replayed > 0 ? 1 : 0;
```

The embedded caller passes a local counter (not `NULL`) so the helper's output
path is exercised, but it has no public stats consumer and discards the value
after successful recovery. Keep the five existing
`test_durability_ordering.c` assertions for `marker_recovery_ran`; add daemon
clean/unclean startup coverage proving the value remains `0` when nothing was
replayed and becomes `1` when at least one marker was replayed.

Do not merely move the daemon's current `validate_metadata`: replace it behind
the shared `shard_db_validate_before_stamp` seam. The current implementation
silently skips malformed schema lines, tolerates unreadable schema files,
omits `ferror`/`fclose` failures, and does not validate KF/segment metadata.
The replacement contract is fail-closed and identical for daemon and embedded:

1. missing `schema.conf` is allowed only when `open` fails with `ENOENT`;
2. any other open/read/close error fails;
3. every nonblank, noncomment line must parse completely through the canonical
   schema parser, with valid dir/object names, v2 engine slot, splits, streams,
   and no trailing garbage;
4. every active schema object must have readable `fields.conf` and its expected
   data/KF/stream directory shape; every on-disk object containing `data/` must
   have exactly one matching schema entry;
5. every referenced tenant must be allowed by `dirs.conf`; the former warning
   becomes a pre-stamp failure because silently ignoring an object is not a
   successful 2026.08.1 handoff;
6. allocation, directory enumeration, `ferror`, and `fclose` failures fail;
7. Task 2c extends the same helper by calling the exported
   `slotcask_validate_segment_files` seam described there. `embedded.c` does
   not call `slotcask.c`'s static filename/state helpers directly and does not
   duplicate their grammar.

Validation is read-only. It must not call an object-open path that creates KF
or segment files. A clean lifecycle flag is not permission to skip validation.
Both daemon and embedded call recovery and then validation after
`shard_db_open_internal` and before stamping. If either helper fails, neither
path stamps nor starts pools. If the saved decision is `STAMP`, atomically
write 2026.08.2 only after both helpers succeed; only then start pools/listen.
Remove the later daemon-only `validate_metadata` call so validation runs
exactly once at the shared pre-stamp seam.

Move `any_markers_pending` from its exact definition anchor in `server.c` into
the shared helper seam and use it from `shard_db_mark_clean_if_safe`. Missing
`schema.conf` means no pending markers; unreadable/malformed metadata prevents
the clean flag. Replace the daemon shutdown block anchored by
`/* Mark shutdown as clean, but only if no object still has a retained` with a
call to this helper. In `shard_db_close`, call the same helper immediately
after background threads and both parallel pools are joined and
`counts_flush_all()` completes, but before cache/slotcask shutdown. Move the
daemon call to the equivalent point after its request workers, background
threads, parallel pools, and counts flush have drained. No thread capable of
creating a durability marker may remain when either path writes the clean
flag. A clean embedded close therefore writes the same lifecycle evidence as
daemon close.

The resulting daemon shutdown order is exactly:

```text
close listener → join request workers → drain in-flight writes →
stop/join background threads → shutdown I/O pool → shutdown CPU pool →
counts_flush_all → shard_db_mark_clean_if_safe → remove PID file →
shutdown caches/slotcask/schema/TLS/logging → destroy instance
```

The embedded order is exactly:

```text
stop/join background threads → shutdown I/O pool → shutdown CPU pool →
counts_flush_all → shard_db_mark_clean_if_safe →
shutdown caches/slotcask/schema → destroy instance
```

`shard_db_mark_clean_if_safe` returns failure if marker enumeration or the
durable clean-flag write fails. Shutdown logs/returns that failure where its
interface permits but leaves the clean flag absent; it never moves the call
earlier to make shutdown appear successful. `clean_flag_write` must unlink and
directory-sync any partially created flag before returning failure.

Update startup tests so a clean 2026.08.1 fixture may change exactly the two
control artifacts required by startup: `.version` is atomically advanced and
`.shard-db.clean` is consumed. Record, KF, index, segment, schema, and object
metadata bytes must remain unchanged. Add daemon and embedded fault cases for
rebuild recovery, marker replay, clean-marker removal, and version stamping;
none may start pools, and no failure before the stamp call may advance
`.version`. Add daemon and embedded cases in which recovery succeeds but
metadata validation fails; both must leave the source marker at 2026.08.1.

### 1d. Version metadata consumers

Removing `SHARD_DB_MIN_VERSION` must update every consumer before compiling:

- `build.sh` anchors `BUILD_MIN_VERSION=$(sed` and `MIN_MSG=` become
  `BUILD_REQUIRED_SOURCE_VERSION` and enforced exact-source wording;
- `src/db/main.c` replaces the complete `if (SHARD_DB_MIN_VERSION[0])` version
  output branch with the single enforced message below;
- `src/test/cases/test_version_compare.c` replaces assertions about an
  informational minimum with assertions for the exact enforced source;
- all `SHARD_DB_ENFORCE_MIN_VERSION`, `SHARD_DB_HAS_STARTUP_MIGRATION`, and
  `SHARD_DB_VERSION_RUN_MIGRATION` consumers are deleted.

The resulting CLI contract is:

```text
shard-db 2026.08.2 (required source release: 2026.08.1; enforced)
```

The complete `main.c` replacement branch is:

```c
if (strcmp(cmd, "version") == 0 || strcmp(cmd, "--version") == 0) {
    printf("shard-db %s (required source release: %s; enforced)\n",
           SHARD_DB_VERSION, SHARD_DB_REQUIRED_SOURCE_VERSION);
    return 0;
}
```

The complete `build.sh` replacement from `BUILD_VERSION=$(sed` through the
upgrade `echo` is:

```bash
BUILD_VERSION=$(sed -n 's/^#define SHARD_DB_VERSION "\(.*\)"/\1/p' src/db/version.h)
BUILD_REQUIRED_SOURCE_VERSION=$(sed -n 's/^#define SHARD_DB_REQUIRED_SOURCE_VERSION "\(.*\)"/\1/p' src/db/version.h)
echo "Upgrades: this build is shard-db $BUILD_VERSION. A filesystem-empty DB_ROOT initializes directly; every non-empty root must have been cleanly opened by exactly shard-db $BUILD_REQUIRED_SOURCE_VERSION before this binary starts. Run './shard-db version' to inspect a binary without starting it."
```

The build footer says an empty root may initialize directly; every non-empty
root must already carry exactly 2026.08.1 or 2026.08.2 evidence. It must not
say the requirement is informational or that startup self-migrates.

Write the compatibility tests before changing the implementation. Replace
the migration-shaped seam with a name and API that describe what it actually
does. Remove migration terminology from the public helper,
comments, logs, and errors.

Refactor `src/db/version.h`, `src/db/util.c`, `src/db/types.h`,
`src/db/embedded.c`, and the daemon startup path in `src/db/server.c`:

- remove `SHARD_DB_HAS_STARTUP_MIGRATION`;
- remove `SHARD_DB_VERSION_RUN_MIGRATION` and any migration-only decision
  branches;
- make missing `.version` on a non-empty root a hard failure;
- enforce the exact 2026.08.1 source requirement;
- retain only atomic marker read/write and the no-op/stamp compatibility
  decisions;
- run the same gate before starting embedded or daemon worker pools;
- change “startup migration failed” and “auto-migrate” messages to
  compatibility/preparation failures;
- ensure a rejected root is not stamped or otherwise modified, apart from the
  acquired/released `.shard-db.lock` artifact.

Delete `run_startup_migration`, `run_startup_format_sweep`, and their call
sites from the 2026.08.2 binary. There must be no startup index rebuild and no
startup segment conversion in this release. `./shard-db reindex` remains an
explicit maintenance command and must not be described as an upgrade path.

Add tests covering every row in the compatibility table, in both the shared
helper and the embedded/daemon-facing path. Include a test
that a compatible clean 2026.08.1 root changes only its version marker and
consumes its clean-lifecycle flag; record/index/segment bytes and object
metadata must not be rewritten.

Task 1 red/green commands, run in this order and paste both outputs:

```bash
rtk env SKIP_TESTS=1 ./build.sh
rtk ./build/bin/shard-db-test run test-version-compat
rtk ./build/bin/shard-db-test run test-version-startup-paths
```

## Task 2 — make the segment engine VARIABLE-only

### 2a. Red tests and complete consumer inventory

Before production edits, update `test_variable_length.c` so a new object must
have no `.format`, must create `000000.dat`, and must survive rotation,
recovery, and reopen through only the VARIABLE parser. Repurpose
`test_o_direct_scan.c` to feed a VARIABLE fixture through the final
`seg_scan_o_direct` API; it fails against the old fixed-stride implementation.
Add a compile audit that includes `slotcask.h`/`io_direct.h` and fails while any fixed enum,
format field, fixed scanner declaration, or migration declaration remains.
In the same red phase, add native negative tests proving the base still accepts
`migrate-varlen`, `migrate-files`, server `mode:"migrate"`, standalone CLI
`compact`, and server `mode:"compact"`; their final expectation is the normal
unknown-command/unknown-mode rejection with no storage mutation.

The executor must disposition every item in this inventory; “any other
caller” is not an instruction to discover scope later. The two retained
VARIABLE helpers are renamed only after their same-signature FIXED siblings
have been deleted, so no intermediate or final translation unit contains two
definitions with the same name:

- `src/db/slotcask.h`: format constants; `SlotcaskDb.format`;
  `SlotcaskSchemaInfo.format`; fixed-pool comments; migration declaration.
- `src/db/slotcask.c`: `pool_push_free`, `pool_try_pop_n`,
  `append_reserve_n`, `build_record_buf`, fixed `seg_write_record`,
  fixed `compact_one_stream`, the dead-code retention shim for
  `build_record_buf`, fixed branches in free-slot recovery/read/write/bulk/
  resplit paths, `.format` helpers/open initialization, the converter, and the
  standalone `slotcask_compact` family. Retain `seg_write_record_varlen`
  and `compact_one_stream_varlen` under their unsuffixed final names for normal
  writes and lightweight vacuum.
- `src/db/io_direct.c` and `io_direct.h`: delete all of
  `seg_scan_o_direct`, `seg_scan_o_direct_match`, and the orphan
  `seg_scan_o_direct_values` declaration; retain and rename
  `seg_scan_o_direct_varlen` to `seg_scan_o_direct` only after all callers use
  its `(path, max_slot_size, callback, context)` contract.
- `src/db/index.c`: remove `ReindexSegWork.format`, the fixed klen guard, and
  scanner dispatch; always use the resynchronizing VARIABLE scanner.
- `src/db/query_find.c`: remove both worker `format` fields, the basic scan
  dispatch, and the fixed-only inline-match fast path; use VARIABLE callbacks.
- `src/db/query_aggregate.c`: remove its worker `format` field and dispatch.
- `src/db/storage.c`: wire `trim_fn` without a format guard at all three
  registry acquisition sites.
- `src/db/main.c` and `src/db/server.c`: remove the standalone `compact`
  command/mode, migration command/mode callers, and their format guards and
  obsolete errors; retain the `vacuum` command and its `"compact":true`
  rebuild option.
- every production `slotcask_open` consumer: registry open in `slotcask.c`,
  rebuild old/new handles in `query_find.c`, and object creation in
  `query_schema.c`; all now receive a VARIABLE-only handle, never initialize
  or inspect format state, and preserve `slot_size` solely as the
  schema-derived maximum record size. The offline compact consumer is deleted.
- `src/db/types.h`, `embedded.c`, `util.c`, and `version.h`: remove the old
  startup migration declarations, decisions, and feature flag under Task 1.
- `npm/index.js` and `npm/index.d.ts`: remove the migration method and query
  union under Task 3.
- `build.sh` and every test/fixture named in Task 4: update registration and
  construction assumptions before deleting symbols.

At the anchor `typedef struct SlotcaskDb {` in `slotcask.h`, the complete
resulting handle is:

```c
typedef struct SlotcaskDb {
    char data_dir[PATH_MAX];
    int num_shards;
    int num_streams;
    int slot_size;          /* schema-derived maximum on-disk record size */
    size_t slots_per_shard;
    SlotcaskStream *streams;
    SlotRef *kf_slot_refs;
    SlotRef **seg_slot_refs;
    int *seg_slot_caps;
    SlotcaskTrimFn trim_fn;
    void *trim_ctx;
} SlotcaskDb;
```

At `typedef struct {` immediately below the comment beginning `Keyed by
(effective_root, object)`, the complete resulting schema info is:

```c
typedef struct {
    int splits;
    int slot_size;          /* schema-derived maximum on-disk record size */
    int streams;
} SlotcaskSchemaInfo;
```

At the `seg_scan_o_direct` declarations in `io_direct.h`, replace the full
fixed/VARIABLE/match/value declaration group with the one remaining API:

```c
int seg_scan_o_direct(const char *seg_path, size_t max_slot_size,
                      od_record_cb cb, void *ctx);
```

Search-only matches in `config.c`, `parallel.c`, `query.c`, `query_plan.c`,
`seg_scan_varlen.h`, `shard_db_internal.h`, and `test_control.c` must be
classified in the audit output as unrelated fixed-width/protocol terminology
or updated if they actually describe the removed segment format. Do not edit
them merely to make a broad word search empty.

Apply the colliding-helper deletions/renames in this explicit order:

1. At the exact anchor
   `static int seg_write_record(const SlotcaskDb *db, uint8_t stream_id,`,
   delete that complete FIXED function. Then, at the exact anchor
   `static int seg_write_record_varlen(const SlotcaskDb *db, uint8_t stream_id,`,
   rename only the identifier to `seg_write_record`; retain the complete
   VARIABLE body and update all retained VARIABLE call sites to the
   unsuffixed name.
2. At the exact anchor
   `static int compact_one_stream(SlotcaskDb *db, int stream_id) {`, delete
   that complete FIXED function. Do not rename the VARIABLE sibling yet.
   Task 2d replaces the complete function anchored by
   `static int compact_one_stream_varlen(SlotcaskDb *db, int stream_id) {`
   with its fail-closed final `compact_one_stream` body after the old symbol is
   gone.

After each pair, the compile audit must show exactly one `seg_write_record`
and, after Task 2d, exactly one `compact_one_stream`; neither suffixed symbol
may remain.

### 2b. Remove the redundant standalone full repacker

Before deleting production code, add red negative-surface tests and a source
audit. The base binary still accepts `./shard-db compact <dir> <object>` and
JSON `{"mode":"compact"}`; the final binary must reject both through the
normal unknown-command/unknown-mode contract. Do not add an alias or fallback
that silently routes either removed surface. Add a positive test proving
`{"mode":"vacuum","compact":true}` still succeeds through
`rebuild_object()`.

Delete the complete standalone full-repack interface and implementation:

- the `slotcask_compact` declaration and its repack comment in `slotcask.h`;
- `CmpSegMap`, `CmpStreamMaps`, `CmpStreamArg`, `compact_stream_worker`, and
  `slotcask_compact` in `slotcask.c`;
- `COMPACT_STREAM_BASE` and the converter's `MIGRATE_STREAM_BASE` /
  `MIGRATE_STREAM_STRIDE` constants and assertions once the converter is
  deleted in the same production task;
- the offline `compact` branch and usage text in `src/db/main.c`;
- the JSON `mode:"compact"` handler and the exact mode classification in
  `mode_is_schema()` in `src/db/server.c`;
- tests or documentation whose only subject is that standalone repacker.

Do not delete `SlotcaskTrimFn`, `SlotcaskDb.trim_fn`, or
`SlotcaskDb.trim_ctx`: normal VARIABLE writes still use them. Do not delete
`slotcask_compact_segs` or `slotcask_compact_kf`: the no-flags lightweight
vacuum still uses them. Retain the heavy vacuum transaction shape, but fix its
two existing correctness gaps below; the transaction is not a safe replacement
for the deleted repacker until both are fixed.

First, `compact:true` must force a rebuild even when splits, streams, slot size,
and field layout are unchanged. At the exact quoted anchor
`/* Nothing to do — caller probably called rebuild without flags */`, replace
that comment and complete following `if` hunk in `rebuild_object()` with:

```c
/* Explicit compact is itself work: it must rebuild the data generation even
   when no schema, split, or stream setting changes. */
if (!drop_tombstoned && !splits_changed && !slot_changed &&
    !streams_changed && n_added == 0) {
    OUT("{\"status\":\"noop\",\"reason\":\"no change requested\"}\n");
    return 0;
}
```

Second, rebuild copy failure is fatal. At the exact anchor
`typedef struct {` immediately preceding `SlotcaskDb        *new_db;`, remove
`skipped` from the complete `V2RebuildCtx` result. At the exact function anchor
`int v2_rebuild_walk_cb(`, in both insert branches replace log-and-continue
insert failure with this fail-closed result (the transformed branch frees
`buf` before this block, as it does today):

```c
if (rc != 0) {
    LOG_ERROR(LOG_SUB_CONFIG,
              "rebuild_v2: insert failed at copied record %llu (klen=%zu)",
              (unsigned long long)(ctx->live_count + 1u), klen);
    ctx->error = 1;
    return 1;
}
ctx->live_count++;
return 0;
```

The verbatim branch uses the same block with `rc` assigned from
`slotcask_insert`; do not retain a separate `skipped` path. Change
`V2RebuildCtx.live_count` to `uint64_t`.

The current pre-flight count is not just the single declaration: it is wrapped
in `if (slot_changed)`. At the exact anchor comment
`/* Pre-flight an existing live count so DK_SEQ ranges can be reserved`,
replace that complete comment plus the `long long pf_live = 0;` declaration
and the entire following `if (slot_changed) { ... }` hunk with:

```c
/* Read the authoritative source count for every rebuild. It is both the copy
   completeness invariant and the DK_SEQ reservation size. */
uint64_t source_total = 0, source_deleted = 0;
if (slotcask_sum_kf_totals(&legacy_db, &source_total, &source_deleted) != 0 ||
    source_deleted > source_total || source_total - source_deleted > INT64_MAX) {
    failure = "Failed to read a valid source live count";
    goto txn_fail;
}
uint64_t expected_live = source_total - source_deleted;
long long pf_live = (long long)expected_live;
```

The only remaining narrower consumer is the existing
`seq_next_val_batch(..., int n)` API. Do not cast `pf_live` silently. In the
`DK_SEQ` branch, before calling it, fail the still-uncommitted rebuild when the
range exceeds `INT_MAX`, then make the checked cast:

```c
if (expected_live > INT_MAX) {
    failure = "Sequence backfill exceeds the supported batch range";
    goto txn_fail;
}
long long start = seq_next_val_batch(db_root, object, nf->default_val,
                                     (int)expected_live);
```

This limit applies only to adding a sequence-default field. A multi-billion-row
resplit, heavy vacuum, or other rebuild without a DK_SEQ backfill remains
supported.

After the walk and before metadata staging or commit, replace the current
`skipped`/`walk_err` handling with this complete gate:

```c
if (walk_ctx.error || walk_ctx.live_count != expected_live) {
    snprintf(failure_buf, sizeof(failure_buf),
             "Rebuild copy incomplete: copied=%llu expected=%llu",
             (unsigned long long)walk_ctx.live_count,
             (unsigned long long)expected_live);
    failure = failure_buf;
    goto txn_fail;
}
```

No path may log a skipped record and proceed to `rebuild_txn_commit`.

There is no fallible legacy count update after the transaction commit. The KF
headers are already the source of truth and `set_count` is a compatibility
no-op, so widen that no-op instead of inventing post-commit abort semantics.
At the exact declaration anchor
`void set_count(const char *db_root, const char *object, int count);` in
`types.h`, replace it with:

```c
void set_count(const char *db_root, const char *object, uint64_t count);
```

At the exact definition anchor
`void set_count(const char *db_root, const char *object, int count) {` in
`storage.c`, replace the complete function with:

```c
void set_count(const char *db_root, const char *object, uint64_t count) {
    (void)db_root;
    (void)object;
    (void)count;
}
```

The complete call-site inventory is `query_find.c` after successful rebuild
and `query_maint.c` truncate with literal zero; both compile unchanged against
the widened signature. After the walk, keep
`uint64_t live_count = walk_ctx.live_count;`; do not introduce `int skipped`,
`int walk_err`, or any `int` copy. The completeness gate uses `walk_ctx.error`
directly before metadata staging. At the exact post-commit anchor
`reset_deleted_count(db_root, object);`, replace from that line through the
successful `return 0;` with:

```c
reset_deleted_count(db_root, object);
set_count(db_root, object, live_count);

LOG_AUDIT(LOG_SUB_CONFIG,
          "REBUILD-V2 %s/%s: live=%llu, splits=%d→%d, streams=%d→%d, "
          "slot_size=%d→%d, compact=%d, idx_rebuilt=%d",
          db_root, object, (unsigned long long)live_count,
          old_sch->splits, new_sch->splits,
          old_sch->streams, new_sch->streams,
          old_sch->slot_size, new_sch->slot_size,
          drop_tombstoned, idx_rebuilt);
OUT("{\"status\":\"rebuilt\",\"live\":%llu,\"splits\":%d,"
    "\"streams\":%d,\"slot_size\":%d,\"compact\":%s,"
    "\"indexes_rebuilt\":%d}\n",
    (unsigned long long)live_count,
    new_sch->splits, new_sch->streams, new_sch->slot_size,
    drop_tombstoned ? "true" : "false", idx_rebuilt);
return 0;
```

Thus all failure-producing checks occur before `rebuild_txn_commit`; the
post-commit tail contains no narrowing and no operation whose failure could
require an impossible transaction abort.

Add a TEST_BUILD-only one-shot rebuild-insert failure hook at the
`v2_rebuild_walk_cb` seam. The setter resets to disabled after firing and each
test resets it during teardown. Inject failure in both the verbatim and
transformed branches. Each case must return an error, restore the original
generation, preserve its complete key/value/count set, leave no authoritative
new generation, and succeed on a later retry after the hook is disabled.

Add this declaration at the existing TEST_BUILD declarations in `types.h`:

```c
void rebuild_test_fail_insert_after(int successful_inserts);
```

Add this complete implementation beside the existing rebuild test pause state
in `query_find.c`, and call `rebuild_test_should_fail_insert()` immediately
before each of the two rebuild `slotcask_insert` calls:

```c
#ifdef TEST_BUILD
static _Atomic int g_rebuild_fail_insert_after = -1;

void rebuild_test_fail_insert_after(int successful_inserts) {
    atomic_store_explicit(&g_rebuild_fail_insert_after,
                          successful_inserts, memory_order_release);
}

static int rebuild_test_should_fail_insert(void) {
    int current = atomic_load_explicit(&g_rebuild_fail_insert_after,
                                       memory_order_acquire);
    while (current >= 0) {
        int next = current == 0 ? -1 : current - 1;
        if (atomic_compare_exchange_weak_explicit(
                &g_rebuild_fail_insert_after, &current, next,
                memory_order_acq_rel, memory_order_acquire))
            return current == 0;
    }
    return 0;
}
#else
static int rebuild_test_should_fail_insert(void) { return 0; }
#endif
```

Each insert branch assigns `rc = -1` when this helper fires; otherwise it calls
`slotcask_insert`. Tests call `rebuild_test_fail_insert_after(-1)` during
teardown even though the hook self-disables after firing.

Add a heavy-vacuum regression fixture whose authoritative live segment files
begin at `048000.dat`. Run `vacuum` with `compact:true`, reopen the object,
verify every key/value, use unindexed criteria that force `FP_FULL_SCAN` and
verify O_DIRECT find/count observes every record exactly once, verify the new
generation begins at `000000.dat`, and verify no old `048000+` segment remains
after successful rebuild cleanup. Run the
existing rebuild crash tests against this high-ID fixture so every injected
interruption resolves to either the complete old generation or the complete
new generation, never an in-place partially repointed KF. The no-schema-change
variant is mandatory: it proves `compact:true` does not return `noop` and that
the resulting generation begins at `000000.dat`.

The final source audit must find no `slotcask_compact`, `CmpSegMap`,
`CmpStreamMaps`, `CmpStreamArg`, `compact_stream_worker`, standalone compact
CLI branch, or standalone compact JSON handler. Matches for
`slotcask_compact_segs`, `slotcask_compact_kf`, and the `vacuum` request's
Boolean `compact` option are required retained behavior, not audit failures.

Because removing `SlotcaskDb.format`, the format enum, and marker helpers
would otherwise leave the converter uncompilable, remove the C conversion
implementation and every native caller in this same production task:
`slotcask_migrate_to_varlen`, `migrate-varlen`, `migrate-files`, server
`mode:"migrate"`, `migrate-storage-version`, their declarations, and their
help/error text. These native migration command/mode red tests are part of
Task 2a and must already have been captured. Task 3 then finishes the
independent npm and shard-cli surface cleanup and runs the final cross-tree
audit; it does not postpone a native caller needed for Task 2 to compile.

### 2c. Do not create file zero when opening an existing stream

Add a red test to `test_variable_length.c` which constructs a valid VARIABLE
root whose only live segment is `048000.dat`, snapshots the object tree, opens
and closes it, and proves no `000000.dat` or other segment is created. A
second case proves a genuinely empty new stream still materializes
`000000.dat`. Add boundary cases proving `065535.dat` is accepted while
`065536.dat`, `999999.dat`, an overlong numeric name, and malformed `.dat`
segment names fail open without creating or selecting another active file.
Run each invalid case through daemon and embedded startup from a compatible
2026.08.1 marker; the Task 2c extension to
`shard_db_validate_before_stamp` must reject it and preserve `.version` at
2026.08.1 before object-opening code can create or repair files.

In `slotcask_open`, delete the complete block anchored by
`/* Eagerly create file_000 in each stream so the first append doesn't` from
its comment through its loop. Keep stream-directory initialization and
allocate `seg_slot_refs`. Before `recover_streams`, run a strict enumeration
for each stream and retain whether that stream is empty. Invalid enumeration
is fatal before recovery can narrow an ID to `uint16_t` or choose it as the
active file. Then call `recover_streams`; only afterward create file zero for
the streams previously proven empty.

Add this complete result type and helper at the stream-recovery seam:

```c
enum StreamSegmentState {
    STREAM_SEGMENTS_ERROR = -1,
    STREAM_SEGMENTS_INVALID = -2,
    STREAM_SEGMENTS_EMPTY = 0,
    STREAM_SEGMENTS_PRESENT = 1
};

static int data_file_id_from_name(const char *name);

static int stream_segment_state(const char *stream_dir) {
    DIR *dir = opendir(stream_dir);
    if (!dir) return STREAM_SEGMENTS_ERROR;
    int has = 0;
    int saved_errno = 0;
    errno = 0;
    for (struct dirent *de = readdir(dir); de; de = readdir(dir)) {
        int parsed = data_file_id_from_name(de->d_name);
        if (parsed == -1) continue;
        if (parsed == -2) {
            closedir(dir);
            return STREAM_SEGMENTS_INVALID;
        }
        has = 1;
    }
    saved_errno = errno;
    int close_rc = closedir(dir);
    if (saved_errno != 0 || close_rc != 0) return STREAM_SEGMENTS_ERROR;
    return has ? STREAM_SEGMENTS_PRESENT : STREAM_SEGMENTS_EMPTY;
}
```

Replace `data_file_id_from_name` with the same grammar instead of `sscanf`.
It returns `-1` for unrelated names and `-2` for a `.dat` name that is
malformed or out of range; `recover_one_stream` must treat `-2` as fatal:

```c
static int data_file_id_from_name(const char *name) {
    size_t len = strlen(name);
    if (len < 4 || strcmp(name + len - 4, ".dat") != 0) return -1;
    if (len != 10) return -2;
    uint32_t id = 0;
    for (int i = 0; i < 6; i++) {
        if (name[i] < '0' || name[i] > '9') return -2;
        id = id * 10u + (uint32_t)(name[i] - '0');
    }
    return id <= UINT16_MAX ? (int)id : -2;
}
```

Keep both grammar helpers private to `slotcask.c`. Instead of making
`embedded.c` reach static implementation details or duplicating the filename
grammar, export one read-only validation seam. At the exact declaration anchor
`int  slotcask_open(SlotcaskDb *db, const char *data_dir,` in `slotcask.h`,
insert this declaration immediately before it:

```c
/* Read-only validation of every stream directory and segment filename.
   Does not create, recover, select, truncate, or open a segment for write. */
int slotcask_validate_segment_files(const char *data_dir, int num_streams);
```

In `slotcask.c`, immediately after the complete new
`data_file_id_from_name` function above, insert this implementation. Because
it lives in the same translation unit it can call `stream_segment_state` and
therefore shares that helper's exact six-digit/`uint16_t` grammar:

```c
int slotcask_validate_segment_files(const char *data_dir, int num_streams) {
    if (!data_dir || num_streams <= 0 || num_streams > SLOTCASK_MAX_STREAMS) {
        errno = EINVAL;
        return -1;
    }
    for (int stream_id = 0; stream_id < num_streams; stream_id++) {
        char stream_dir[PATH_MAX];
        stream_dir_for(stream_dir, data_dir, stream_id);
        int state = stream_segment_state(stream_dir);
        if (state == STREAM_SEGMENTS_INVALID) {
            errno = EUCLEAN;
            return -1;
        }
        if (state == STREAM_SEGMENTS_ERROR) return -1;
    }
    return 0;
}
```

At the complete replacement of the current daemon-only function anchored by
`static int validate_metadata(const char *db_root) {` in `server.c` by
`shard_db_validate_before_stamp` in `embedded.c`, the per-object portion must
call this seam only after canonical schema parsing has supplied `streams` and
the read-only directory-shape checks have supplied `object_dir`:

```c
if (slotcask_validate_segment_files(object_dir, schema.streams) != 0) {
    failed = 1;
    break;
}
```

This call is part of the validator's existing fail path: it propagates through
`ferror`/`fclose` cleanup, returns failure, and prevents the compatibility
stamp in both daemon and embedded startup. The Task 2c daemon/embedded invalid
filename cases prove this happens before `slotcask_open` can create or recover
anything.

Immediately after allocating `seg_slot_refs`/`seg_slot_caps`, replace the
current unconditional recovery hunk with this complete integration:

```c
int *segment_states = calloc((size_t)num_streams, sizeof(int));
if (!segment_states) { free(open_args); goto fail; }
for (int i = 0; i < num_streams; i++) {
    segment_states[i] = stream_segment_state(db->streams[i].stream_dir);
    if (segment_states[i] < 0) {
        errno = segment_states[i] == STREAM_SEGMENTS_INVALID ? EUCLEAN : errno;
        free(segment_states);
        free(open_args);
        goto fail;
    }
}

(void)dirty_marker_exists;
if (recover_streams(db) != 0) {
    free(segment_states);
    free(open_args);
    goto fail;
}

for (int i = 0; i < num_streams; i++) {
    if (segment_states[i] != STREAM_SEGMENTS_EMPTY) continue;
    char path[PATH_MAX];
    seg_path_for(path, data_dir, i, 0);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 1, 1, 0) != 0) {
        free(segment_states);
        free(open_args);
        goto fail;
    }
    if (h.slot >= 0) {
        SlotRef *ref = seg_ref_for(db, i, 0);
        if (ref) {
            ref->slot = h.slot;
            ref->gen = atomic_load_explicit(&g_segcache[h.slot].gen,
                                             memory_order_acquire);
        }
    }
    segcache_release(&h);
}
free(segment_states);

/* Preserve the existing dirty-lifecycle behavior. This marker is written
   only after strict enumeration, recovery, and any empty-stream file-zero
   creation have all succeeded. Its current best-effort failure semantics are
   unchanged by this task. */
if (touch_dirty_marker(db) != 0) {
    /* Non-fatal — recovery will simply re-walk on the next open. */
}
```

Only a saved `STREAM_SEGMENTS_EMPTY` state therefore creates `000000.dat`.
Enumeration, recovery, or creation failure is fatal to open.
Existing low or historical high-ID streams are recovered and never gain file
zero merely by opening. The shown `touch_dirty_marker(db)` block is retained
exactly once after file-zero creation; delete its old copy from the replaced
unconditional recovery hunk. Update `data_file_id_from_name` and every recovery
enumerator to share the same exact grammar and bounds rather than silently
accepting or narrowing a larger integer after this precheck.

Accepting `065535.dat` is safe only while it has remaining capacity. At the
exact anchor `static int append_reserve_single_varlen(`, replace the complete
VARIABLE reservation helper with this ceiling-checked version; it
must not mutate `active_file_id`, `reserve_off`, output parameters, or create a
file when rotation would exceed the KF's `uint16_t` file-id field:

```c
static int append_reserve_single_varlen(SlotcaskDb *db, SlotcaskStream *p,
                                         size_t rec_size,
                                         uint32_t *file_id_out,
                                         uint32_t *offset_out) {
    (void)db;
    if (!file_id_out || !offset_out || rec_size > SLOTCASK_SEG_MAX_BYTES)
        return -1;

    pthread_mutex_lock(&p->rotation_lock);
    if (p->reserve_off > SLOTCASK_SEG_MAX_BYTES - rec_size) {
        if (p->active_file_id >= UINT16_MAX) {
            pthread_mutex_unlock(&p->rotation_lock);
            errno = EFBIG;
            return -1;
        }
        p->active_file_id++;
        p->reserve_off = 0;
    }
    *file_id_out = p->active_file_id;
    *offset_out = (uint32_t)p->reserve_off;
    p->reserve_off += rec_size;
    pthread_mutex_unlock(&p->rotation_lock);
    return 0;
}
```

Add a regression case with a completely full `065535.dat`: the next write
must fail with no `065536.dat`, no KF insertion/repoint, no count change, and
no mutation of the prior record. A non-full `065535.dat` remains writable
until its existing capacity is exhausted.

Remove the complete fixed-size segment implementation rather than leaving a
dead format enum or a defensive marker check:

- remove `SLOTCASK_FORMAT_FIXED`, the obsolete format state field and
  variable/fixed dispatch state where no longer needed;
- remove `.format` marker path/read/write helpers and marker creation;
- remove the fixed `seg_scan_o_direct` implementation, retaining the
  resynchronizing VARIABLE scanner from the preceding plan;
- collapse fixed/variable branches in `slotcask.c`, `io_direct.c`, `index.c`,
  `query_find.c`, `query_aggregate.c`, and `storage.c` to the VARIABLE path;
  `main.c` and `server.c` lose the standalone compact surfaces entirely under
  Task 2b rather than retaining format guards around them;
- remove fixed-only pool compatibility helpers and verify their call sites
  before deleting them;
- make normal VARIABLE append/rotation numbering start at file ID `0` for
  fresh objects (`000000.dat`); remove any assumption that `048000.dat` is
  needed for VARIABLE data;
- retain `slot_size` only where it still has a real VARIABLE/free-pool or
  schema purpose; remove fixed-only sizing assumptions;
- make writes, reads, recovery, resplit, lightweight compaction, and segment
  scans call the VARIABLE implementation directly;
- update comments and names so the remaining code no longer implies that two
  segment formats coexist.

### 2d. Make retained lightweight vacuum failures observable

The root cause is layered success-on-error behavior: the current per-stream
helper converts allocation/stat/migration/drop failures into zero dropped
files, `slotcask_compact_segs` therefore returns success, KF compaction skips
failed shards, and `cmd_vacuum` discards both public return codes before
printing `"vacuumed"`.

Keep `compact_one_stream_varlen`'s non-negative return as its dropped-file
count, but return `-1` for `opendir`, allocation, segment-stat,
`compact_migrate_records_varlen`, `kf_failed != 0`, and
`compact_drop_seg_file` failures. A migration failure or nonzero `kf_failed`
must preserve the donor and stop that vacuum call; earlier successfully
completed pair moves remain valid through their existing crash-safe protocol.
At the exact anchor `static int compact_one_stream_varlen(`, replace the
complete function and rename it to `compact_one_stream` with this result:

```c
static int compact_one_stream(SlotcaskDb *db, int stream_id) {
    char dir[PATH_MAX];
    stream_dir_for(dir, db->data_dir, stream_id);

    SlotcaskStream *p = &db->streams[stream_id];
    pthread_mutex_lock(&p->rotation_lock);
    uint32_t active = p->active_file_id;
    pthread_mutex_unlock(&p->rotation_lock);

    DIR *dh = opendir(dir);
    if (!dh) return -1;

    SegStat *files = NULL;
    size_t nfiles = 0, fcap = 0;
    int read_errno = 0;
    for (;;) {
        /* Set errno immediately before readdir, not before an iteration that
           may also call realloc or other errno-setting functions. */
        errno = 0;
        struct dirent *de = readdir(dh);
        if (!de) {
            read_errno = errno;
            break;
        }
        int parsed = data_file_id_from_name(de->d_name);
        if (parsed == -1) continue;
        if (parsed < 0) { free(files); closedir(dh); return -1; }
        uint32_t fid = (uint32_t)parsed;
        if (fid == active) continue;
        if (nfiles == fcap) {
            size_t nc = fcap ? fcap * 2 : 16;
            SegStat *next = realloc(files, nc * sizeof(*files));
            if (!next) { free(files); closedir(dh); return -1; }
            files = next;
            fcap = nc;
        }
        files[nfiles++] = (SegStat){
            .stream_id = stream_id,
            .file_id = fid,
            .live_count = 0,
            .total_slots = 0
        };
    }
    int close_rc = closedir(dh);
    if (read_errno != 0 || close_rc != 0) { free(files); return -1; }
    if (nfiles == 0) { free(files); return 0; }

    for (size_t k = 0; k < nfiles; k++) {
        if (seg_stat_one_varlen(db, stream_id, files[k].file_id,
                                &files[k].live_count,
                                &files[k].total_slots) != 0) {
            free(files);
            return -1;
        }
    }
    qsort(files, nfiles, sizeof(*files), seg_stat_cmp_live_asc);

    int dropped = 0;
    size_t i = 0, j = nfiles - 1;
    while (i < j) {
        if (files[i].total_slots == 0) { i++; continue; }
        if (files[i].live_count == 0) {
            if (compact_drop_seg_file(db, stream_id, files[i].file_id) != 0) {
                free(files);
                return -1;
            }
            dropped++;
            i++;
            continue;
        }
        uint32_t recipient_free =
            files[j].total_slots - files[j].live_count;
        if (recipient_free >= files[i].live_count) {
            uint32_t kf_failed = 0;
            if (compact_migrate_records_varlen(db, stream_id,
                                                files[i].file_id,
                                                files[j].file_id,
                                                &kf_failed) != 0 ||
                kf_failed != 0 ||
                compact_drop_seg_file(db, stream_id,
                                      files[i].file_id) != 0) {
                free(files);
                return -1;
            }
            dropped++;
            files[j].live_count += files[i].live_count;
            i++;
        } else {
            if (j == i + 1) break;
            j--;
        }
    }
    free(files);
    return dropped;
}
```

After renaming that helper to its final unsuffixed name, at the exact anchor
`int slotcask_compact_segs(` replace the complete public segment-compaction
function with:

```c
int slotcask_compact_segs(SlotcaskDb *db, int *out_dropped) {
    if (!db) return -1;
    int total = 0;
    for (int s = 0; s < db->num_streams; s++) {
        int rc = compact_one_stream(db, s);
        if (rc < 0) return -1;
        total += rc;
    }
    if (out_dropped) *out_dropped = total;
    return 0;
}
```

At the exact anchor `int slotcask_compact_kf(`, replace the complete
KF-compaction function with:

```c
int slotcask_compact_kf(SlotcaskDb *db) {
    if (!db || db->num_shards <= 0) return -1;
    for (int s = 0; s < db->num_shards; s++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0)
            return -1;
        int rc = 0;
        if (kh.hdr && kh.hdr->deleted > 0)
            rc = kfcache_resplit_locked(&kh, kh.capacity);
        kfcache_release(&kh);
        if (rc != 0) return -1;
    }
    return 0;
}
```

In `cmd_vacuum`'s light path, replace the hunk anchored by
`int dropped = 0;` through its unconditional `return 0;` with:

```c
int dropped = 0;
if (slotcask_compact_segs(sdb, &dropped) != 0) {
    OUT("{\"error\":\"segment compaction failed\"}\n");
    return 1;
}
if (slotcask_compact_kf(sdb) != 0) {
    OUT("{\"error\":\"keyfile compaction failed\"}\n");
    return 1;
}
reset_deleted_count(db_root, object);  /* v1 only; deleted in VARIABLE-only cleanup if now dead */
OUT("{\"status\":\"vacuumed\",\"cleaned\":%d}\n", dropped);
return 0;
```

Add deterministic failures for segment stat/allocation, donor migration,
donor deletion, KF acquire, and KF resplit. Each command must return an error,
must not print `"vacuumed"`, and must leave every live key readable. A later
fault-free vacuum must succeed. Capture each red result against the current
success-on-error behavior before changing production code.

Audit all `format` fields and branches, not just the enum references. In
particular inspect worker argument structs and the `db->format`/`sdb->format`
callers. After this task, a source-only search must find no fixed segment
implementation, no `.format` marker support, and no `SLOTCASK_FORMAT_FIXED`.

Register the new cases with these exact runner names and use this Task 2
red/green command sequence:

```bash
rtk env SKIP_TESTS=1 ./build.sh
rtk ./build/bin/shard-db-test run test-variable-length
rtk ./build/bin/shard-db-test run test-o-direct-scan-bit-equality
rtk ./build/bin/shard-db-test run test-removed-storage-surfaces
rtk ./build/bin/shard-db-test run test-heavy-vacuum-transaction
rtk ./build/bin/shard-db-test run test-segment-file-id-bounds
rtk ./build/bin/shard-db-test run test-light-vacuum-errors
rtk ./build/bin/shard-db-test run-all --filter rebuild-txn
```

## Task 3 — finish removal of every migration surface

The native CLI/JSON red tests are written and run in Task 2 before its native
surface deletions. At the start of this task, add npm declaration audits which
are red while `migrate()` and either removed query mode remain declared. Their
green contract proves `npm/index.js` and `npm/index.d.ts` contain no `migrate`
method, `{ mode: 'migrate' }`, or `{ mode: 'compact' }` union member. The npm
package has no TypeScript compiler/type-test harness, so this plan does not
invent one or add a dependency. Existing npm native build and smoke tests must
remain green; no compatibility fallback performs conversion.

Use this exact audit before and after the removal; base must print the known
API declarations, final must print no matches (ripgrep exit 1 is the expected
no-match result, not a test failure):

```bash
rtk rg -n "migrate\s*\(|mode:\s*'(migrate|compact)'|mode:\s*\"(migrate|compact)\"" npm/index.js npm/index.d.ts
```

Task 2 deletes the native conversion implementation and native callers as one
compile-safe unit. In this task, verify those deletions and remove the
remaining independent surfaces:

- verify `slotcask_migrate_to_varlen`, converter-local `SrcMap`/`DestMap`,
  migration ranges, server `mode:"migrate"`, `migrate-storage-version`,
  `migrate-varlen`, `migrate-files`, declarations, and native help/error text
  were removed in Task 2 and do not reintroduce them;
- remove `migrate(dir, object)` from `npm/index.js` and its declaration and
  migration query-body union entry in `npm/index.d.ts`;
- remove the standalone `{ mode: 'compact'; dir; object }` union member from
  `npm/index.d.ts`; retain the optional `compact?: boolean` property on the
  `{ mode: 'vacuum' }` member;
- in `src/cli/main.c`, rename the exact schema-transfer-only symbols
  `migrate_export`, `migrate_import`, and `menu_migrate` to
  `schema_transfer_export`, `schema_transfer_import`, and
  `menu_schema_transfer`; rename both visible `"Migrate"` menu labels to
  `"Schema Transfer"` and replace “migrate import” prose with
  “Schema Transfer → Import”; behavior remains export/import only;
- leave schema `export-schema`/`import-schema` available, but describe them as
  schema transfer rather than storage migration;
- leave historical references to the pre-2026.05.5 v1 conversion in place
  only where they explain an old release boundary, and label them as a
  historical prerequisite that 2026.08.2 does not provide.

Do not replace removed migration commands with a fake migration implementation
or an automatic fallback. An unknown/removed command or a concise “removed;
upgrade through 2026.08.1” compatibility error is acceptable, but the binary
must not contain a conversion path.

Task 3 red/green commands:

```bash
rtk rg -n "migrate\s*\(|mode:\s*'(migrate|compact)'|mode:\s*\"(migrate|compact)\"" npm/index.js npm/index.d.ts
rtk npm --prefix npm run build
rtk npm --prefix npm test
rtk env SKIP_TESTS=1 ./build.sh
rtk ./build/bin/shard-db-test run test-removed-storage-surfaces
```

## Task 4 — tests and fixtures

Tasks 1–3 already create, replace, and register their tests. Do not replace
those files a second time in this task. This is a consolidation pass over the
remaining fixtures and direct low-level callers only.

Before fixture edits, the compile audit must identify every remaining test
reference to a deleted format enum, `.format`, converter, fixed scanner, or
standalone full compactor. Update these existing lightweight-compaction cases
to construct VARIABLE fixtures directly while preserving their original
resync/crash assertions:

- `test_varlen_compact_recipient_resync.c`;
- `test_varlen_compact_stat_resync.c`;
- `test_varlen_compact_donor_resync.c`;
- `test_varlen_compact_crash_mid_migration.c` (rename only if its registered
  name also changes atomically in `build.sh`);
- `test_varlen_compact_donor_preserved_on_desync.c`.

Update stale fixture/comment assumptions in `test_dispatch_leak_paths.c`,
`src/test/fixtures.c`, `test_coverity_disk_corruption_segments.c`,
`test_coverity_seg_scan_varlen_overflow.c`, and
`test_varlen_scan_resync_odirect.c`. Audit every direct `slotcask_open()` test
caller in `test_durability_sync_failures.c`, `test_durability_ordering.c`,
`test_slotcask_resplit.c`, `test_slotcask_basic.c`, `test_slotcask_api.c`,
`test_kfcache_staleness.c`, `test_registry_single_flight.c`,
`test_slotcask_cas.c`, and all `test_varlen_compact_*.c` cases. Creation is
VARIABLE without an explicit converter; any fixed-stride fixture is rewritten
or deleted if fixed behavior was its only purpose.

Run this exact consolidation gate:

```bash
rtk env SKIP_TESTS=1 ./build.sh
rtk ./build/bin/shard-db-test run-all --filter varlen
rtk ./build/bin/shard-db-test run test-dispatch-leak-paths
rtk ./build/bin/shard-db-test run-all --filter coverity
rtk ./build/bin/shard-db-test run-all --filter durability
rtk ./build/bin/shard-db-test run test-slotcask-resplit
rtk ./build/bin/shard-db-test run test-slotcask-basic
rtk ./build/bin/shard-db-test run test-slotcask-api
```

Run the deleted-symbol audit after these fixtures are green. Do not register
the discarded A/B/C, batch-publication, or standalone-full-compactor tests.

## Task 5 — documentation and operator contract

Before editing prose, capture a red documentation audit listing every
present-tense claim that startup migrates/reindexes, every current
`migrate-varlen`/JSON/npm migration instruction, every claim that new segments
are FIXED, and every claim that filename ranges identify format. After the
edits, rerun the same audit and classify each remaining match as historical
release documentation or an error. Run the repository's documentation link/
format checks if present; do not invent a new documentation tool.

Update all current-release documentation, not only the storage-model page:

- `AGENTS.md`: storage model, version/upgrade behavior, and release guidance;
- `README.md`: startup instructions and current release compatibility text;
- `docs/concepts/storage-model.md`: VARIABLE-only segment layout and removal
  of present-tense FIXED descriptions; preserve typed-record and historical
  v1 sections where they are still useful;
- `docs/concepts/indexes.md`: remove the present-tense claim that "startup
  migration is automatic... rebuilds btrees in-process if the disk version
  is older" — 2026.08.2 performs no startup index rebuild per Task 1;
  operators use `./shard-db reindex` explicitly. Preserve the `BTRE`/`BTRF`/
  `BTRG` history as historical context;
- `docs/getting-started/install.md` and
  `docs/operations/deployment.md`: the exact final-2026.08.1 sweep procedure,
  strict refusal cases, clean-stop requirement, and no rollback by simply
  running an older binary after the marker reaches 2026.08.2;
- document that low-numbered files are valid for new VARIABLE objects,
  `048000+` files are valid legacy output from the 2026.08.1 conversion, and
  neither filename range is a format marker. Do not instruct operators to
  rename or delete high-numbered files during the 2026.08.2 upgrade;
- `docs/getting-started/configuration.md`: current version behavior if its
  migration wording is still current, while preserving the historical v1
  schema prerequisite;
- `docs/cli/index.md`: remove `migrate-varlen`/`migrate` claims and explain
  that standalone `compact` was removed; document `vacuum compact:true` as
  the supported transactional full rebuild and no-flags `vacuum` as
  lightweight cleanup. The removed `compact` and `./migrate` rows also
  currently claim startup migration performs an automatic index rebuild on
  upgrade — that claim goes away with Task 1's removal of
  `run_startup_migration` and must be corrected too;
- `docs/cli/shard-cli.md`: rename the `Migrate` row to `Schema Transfer` and
  describe export/import as schema-only bootstrap with no data or storage
  conversion;
- `docs/query-protocol/files.md` and
  `docs/query-protocol/schema-mutations.md`: remove claims that startup
  migration converts or rebuilds data in 2026.08.2;
- `docs/reference/changelog.md`, root `CHANGELOG.md`, and a new
  `docs/release-notes/2026.08.2.md`: document the breaking storage boundary,
  strict source version, no migration surfaces, standalone compact CLI/JSON
  removal, transactional heavy-vacuum replacement, and npm API removal. Do
  not rewrite historical release entries that accurately describe when the
  old command existed; mark current examples obsolete and add the 2026.08.2
  removal note instead. Correct any present-tense claim that
  `slotcask_compact()` itself is atomic: only the retained
  `rebuild_object()` heavy-vacuum transaction provides the supported
  all-or-nothing full-repack contract;
- `npm/README.md` and any npm API examples: document the removed `migrate()`
  method and the embedded upgrade precondition.

The upgrade runbook must say:

```text
backup → stop writes → run 2026.08.1 once → wait for its startup sweep →
cleanly stop 2026.08.1 → replace with 2026.08.2 → start and verify
```

It must also say that a failed 2026.08.1 sweep blocks the upgrade, and that a
2026.08.2 marker prevents a downgrade to 2026.08.1 without restoring a backup
or using a compatible newer binary. `reindex`, both retained `vacuum` paths,
and schema export/import must be explained separately from storage migration.
The standalone compact command/mode is removed rather than presented as an
upgrade tool. The
handoff checklist must verify that converted objects do not contain both
legacy low-numbered segment files and converted `048000+` files; it must not
assume that the `.format` marker alone proves cleanup completed.
This checklist is an operator-owned 2026.08.1 prerequisite, not a verifier or
fallback shipped in 2026.08.2. Once the operator installs 2026.08.2, the new
binary trusts that prerequisite and never interprets FIXED bytes or `.format`.

Task 5 red/green documentation audits:

```bash
rtk rg -n -i "startup migrat|auto-migrat|migrate-varlen|mode.?[:=].?migrate|shard-db compact|mode.?[:=].?compact|fixed[- ](segment|record|slot)|048000.*format|format.*048000" AGENTS.md README.md CHANGELOG.md docs npm/README.md
rtk rg -n "vacuum.*compact|Schema Transfer|2026\.08\.1|2026\.08\.2" AGENTS.md README.md CHANGELOG.md docs npm/README.md
```

Every remaining first-audit match must be pasted and classified as historical
release context or corrected. The second audit must show the replacement
operator contract; it is not by itself proof that obsolete text is absent.

## Task 6 — release metadata and npm surface

Update release metadata only as part of execution of this plan:

- bump `src/db/version.h` to `2026.08.2`, with exact enforced source
  `2026.08.1`;
- bump `npm/package.json` and both npm lockfile package-version entries to
  the next npm release version above whatever is current at execution time
  (check `npm/package.json` and `rtk git tag -l 'npm-v*'` — do not assume a
  stale number; as of this writing npm is at 1.0.16, so this would be
  1.0.17, but re-verify before bumping), and remove the npm migration API;
- add the 2026.08.2 release note before invoking the release workflow;
- verify the normal binary and embedded/npm artifacts report the matching
  source release and that the workflow's release-note body path resolves;
- document the breaking upgrade requirement in the npm release notes/readme.

The normal binary and npm artifact must be tested independently because both
call the compatibility gate, while only the npm package exposes the embedded
JavaScript API surface.

Task 6 red/green commands:

```bash
rtk env SKIP_TESTS=1 ./build.sh
rtk ./shard-db version
rtk npm --prefix npm run build
rtk npm --prefix npm test
rtk rg -n '"version"' npm/package.json npm/package-lock.json
rtk rg -n "2026\.08\.2|2026\.08\.1" src/db/version.h docs/release-notes/2026.08.2.md
```

## Task 7 — verification gates (execution phase only)

This change touches object opening, storage scanning, free-pool state, and
startup/shared state. Per the repository's standing safety requirement, run
the normal suite plus both sanitizer gates; these commands are listed for
execution later and must not be run as part of this planning turn:

```bash
rtk env SKIP_TESTS=1 ./build.sh
rtk ./build/bin/shard-db-test run-all

rtk env BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
rtk env ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2

rtk env BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
rtk env TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1
```

Also verify:

- a 2026.08.1-created-and-swept root opens in 2026.08.2 in both daemon and
  embedded mode;
- a fresh 2026.08.2 VARIABLE object creates `000000.dat` and continues
  correctly through rotation;
- standalone CLI `compact`, JSON `mode:"compact"`, `slotcask_compact`, and all
  private full-repack helpers/range constants are absent; neither removed
  command silently aliases another operation;
- no-flags lightweight vacuum still exercises `slotcask_compact_segs` and
  `slotcask_compact_kf` through their VARIABLE-only implementations;
- `vacuum compact:true` still uses the rebuild transaction, and every existing
  rebuild interruption resolves to one complete authoritative generation;
  it forces a rebuild even without schema changes, and an injected copy/insert
  failure aborts and restores the complete source generation rather than
  committing a reduced record count;
- an already-converted 2026.08.1 root whose live files begin at `048000.dat`
  opens and serves normally without renumbering during startup; after a
  successful heavy vacuum it reopens with a fresh `000000+` generation, no
  `048000+` source generation, all keys/values intact, and exact-once O_DIRECT
  find/count results;
- O_DIRECT full scans retain their sequential segment-file fan-out and are not
  replaced with KF-driven random fetches or per-record KF validation;
- `065535.dat` opens, while `065536.dat`, `999999.dat`, overlong, and malformed
  `.dat` segment names fail before active-file selection or file-zero creation;
  a non-full `065535.dat` remains writable, while rotation from a full one
  fails without creating `065536.dat`, narrowing the KF file ID, or changing
  counts/data;
- lightweight segment/KF vacuum failures return an error, never print
  `"vacuumed"`, preserve all live keys, and succeed on a later fault-free run;
- fresh-root initialization works;
- only a root containing no entry other than `.shard-db.lock` qualifies as
  fresh; empty-schema, config-only, orphan-data, and unknown-file roots do not;
- every incompatible `.version` case fails before worker pools and without
  changing data;
- a compatible clean open changes only the version marker and consumes the
  clean-lifecycle flag;
- the normal binary has no fixed scanner, fixed format state, `.format`
  checker, conversion function, migration CLI/mode, or migration npm API;
- protocol response `format` and schema export/import still work;
- npm native build, package smoke tests, and the embedded startup path pass;
  npm declarations contain neither removed `migrate` nor standalone `compact`
  query modes while retaining `vacuum.compact`;
- daemon and embedded startup share rebuild/marker recovery and clean-shutdown
  evidence; recovery failure never stamps or starts pools;
- release artifacts are built only after the raw diff and documentation have
  been reviewed, consistent with this repository's uncommitted-until-reviewed
  rule.
- the build emits no new warnings; the diff contains no work-created
  `TODO`/`FIXME`, debug print, commented-out implementation, unrelated edit,
  or undeclared dependency; paste the clean audits with the test outputs.

Suggested source audit patterns (scope them to implementation files so
historical documentation can remain accurate):

```bash
rtk rg -n "SLOTCASK_FORMAT_|seg_scan_o_direct_(varlen|match|values)" src/db src/test npm
rtk rg -n "slotcask_migrate_to_varlen|migrate-varlen|mode.*migrate|migrate-storage-version|migrate_export|migrate_import|menu_migrate|MIGRATE_STREAM_BASE|MIGRATE_STREAM_STRIDE|COMPACT_STREAM_BASE" src npm
rtk rg -n 'slotcask_compact([^_a-zA-Z0-9]|$)|CmpSegMap|CmpStreamMaps|CmpStreamArg|compact_stream_worker|strcmp\((cmd|mode), "compact"\)' src npm
rtk rg -n "startup_migrat|run_startup_format_sweep|VERSION_RUN_MIGRATION|format_file_path|read_format_marker|write_format_marker|/\.format|\"\.format\"" src/db src/test npm
rtk rg -n "SHARD_DB_MIN_VERSION|SHARD_DB_ENFORCE_MIN_VERSION|SHARD_DB_HAS_STARTUP_MIGRATION|SHARD_DB_VERSION_RUN_MIGRATION" src build.sh npm
rtk rg -n -i "fixed[- ](size|slot|format)|fixed-format|fixed stride" src/db src/test npm
```

The expected result is no storage-format migration implementation, standalone
full-repack implementation, or removed public surface in the 2026.08.2
binary. The standalone-compact regex intentionally excludes retained
`slotcask_compact_segs`, `slotcask_compact_kf`, and the vacuum request's
Boolean option; the exact deleted `slotcask_compact` function and standalone
command/mode must be absent. Every fixed-terminology match must be
pasted and classified as unrelated fixed-width typed/KF state or corrected;
no old fixed segment implementation may remain. Historical prose must be
clearly scoped to older releases.
