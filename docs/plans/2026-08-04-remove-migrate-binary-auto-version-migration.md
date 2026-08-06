# Remove the standalone `./migrate` binary; auto version-gated migration on daemon start

> **Execution clarification (2026-08-06):** The upcoming release assumes
> varlen conversion and compaction were completed by earlier releases. Its
> only startup migration is a full reindex. `SHARD_DB_MIN_VERSION` is
> `2026.07.3`, but the floor is informational and not enforced in this
> release because earlier releases never wrote `.version`; unversioned roots run the reindex and
> receive the new marker. The older varlen/compact code blocks below are
> historical design alternatives and are not part of this release's batch.

## Motivation

Today, upgrading a shard-db install requires the operator to know which
manual steps to run (`./migrate`, and before that possibly an even older
release's `./migrate`), because migration logic lives in a separate
one-shot binary (`src/migrate/main.c`) that the operator must remember to
invoke. Embedded mode already does this automatically — `shard_db_open()`
calls a private `run_startup_migration()` before starting thread pools —
but the TCP daemon (`cmd_server()`) has no equivalent; it just starts.

This plan makes the daemon binary self-migrating, the same way embedded
mode already is: on start, compare a new `$DB_ROOT/.version` file against
the binary's own compiled-in version; migrate automatically if the disk
is behind, refuse to start with an explicit message if the disk is ahead
(downgrade — unsupported), and skip entirely (fast no-op) if they match.
The standalone `./migrate` binary and its `src/migrate/` source are
deleted; both entry points (daemon and embedded) call one shared function.

## Grounding: what exists today

- **No compile-time version constant exists anywhere in the C codebase.**
  Confirmed via grep across `src/db/*.{c,h}` and `build.sh` — versions
  only appear in doc comments/changelog. This plan introduces one.
- **Three separate, pre-existing, lazy version/format gates already exist
  and are UNCHANGED by this plan:**
  1. Schema/engine version (v1 vs v2) — `load_schema()` in `config.c`
     (~line 918), refuses v1 objects, tells the operator to install
     2026.05.4 and run *that release's own* `./migrate` (a different,
     historical binary that still ships with 2026.05.4 — unaffected by
     deleting *this* release's copy).
  2. B+ tree magic (BTRG→BTRH) — `bt_open_file()` in `btree.c` (~line
     647), rejects non-current magic. Its message currently says "run
     ./migrate to reindex" — **stale**, since `src/migrate/main.c` never
     had a reindex phase (see finding below). Fixed as part of this plan
     (message text only — see Task 5).
  3. Segment format (FIXED vs VARIABLE) — the per-object `.format`
     marker in `slotcask.c`. Explicitly out of scope; user has deferred
     its removal to a later release.
- **Finding — doc/behavior mismatch in the current `./migrate` tool.**
  `build.sh`'s comment above the migrate build step and `AGENTS.md` both
  describe `./migrate` as running `./shard-db reindex` to rebuild B+
  trees into the BTRH format. The checked-in `src/migrate/main.c` source
  has no reindex or rebuild-kf phase; it runs `migrate-varlen` and then
  `compact` for every schema object. This plan's replacement must match
  the source that is actually present, not either stale description.
  The standalone `./shard-db reindex` CLI/JSON command itself is untouched
  by this plan — it still exists, unchanged, for on-demand operator use.
  What *is* new here: `run_startup_migration()` gains its own reindex
  phase for 2026.08.1 (Task 3), calling `reindex_object_checked()` — the
  same in-process primitive the CLI command's `cmd_reindex()` loop calls
  per object — directly, not by shelling out to the CLI command. See
  Design decision 4 for the motivating bug and the safety argument for
  calling it from the pre-fork startup window.
- **Embedded mode's existing `run_startup_migration()`** (`embedded.c`,
  static, around the comment `/* Run startup migrations for every
  registered object:`) already does varlen migration, fatal on failure,
  but is missing the CLI tool's `compact` phase. The CLI's `compact`
  subcommand in `main.c` is offered standalone but never auto-run by
  embedded mode today. `slotcask_compact()` fully rewrites
  every stream file (not a cheap no-op like the format check) — confirmed
  by reading `slotcask_compact()`
  in `slotcask.c` (~line 10358), which allocates per-stream maps and
  rewrites every live record. It must not run unconditionally on every
  start; it must be gated behind an actual version bump.
- **Both migration phases have real in-process primitives, already used by
  `main.c`'s CLI subcommands and available to `embedded.c`:**
  `slotcask_open()` + `slotcask_migrate_to_varlen(&sdb)` and
  `slotcask_open()` + `slotcask_compact(&sdb, schema_trim_fn, (void*)ts)`
  (requires `sdb.format == SLOTCASK_FORMAT_VARIABLE`, i.e. must run
  after a successful varlen migration — `ts` from
  `load_typed_schema(eff_root, obj)`). All declared in `types.h` /
  `slotcask.h`, confirmed callable outside `main.c`. A third primitive,
  `reindex_object_checked()` (declared in `types.h`, defined in
  `index.c`), is added to this release's batch as well — see Design
  decision 4 for why and for its safety argument in the pre-fork startup
  window.
- **`embedded.c` links into both the daemon binary and `libshard-db.a`**
  (`build.sh` line ~109 lists `embedded.c` among the daemon's own
  sources; `LIB_SRCS` for the static library includes it too) — so a
  function defined in `embedded.c` and declared in `types.h` is already
  reachable from `server.c` with no relinking changes.
- **`cmd_server()` insertion point** (`server.c`, function starts ~3608):
  lock acquire → `shard_db_open_internal` → rlimit → `rebuild_recovery`
  → marker recovery sweep → **[new version-gate + migration call goes
  here]** → pre-fork validation (`load_dirs` + `validate_metadata`) →
  fork/daemonize → PID write → log init → TLS → socket bind/listen.
  The insertion point is deliberately *before* pre-fork validation and
  *before* the fork, so migration output (like embedded mode's own
  `fprintf(stderr, ...)` progress lines) still reaches the operator's
  terminal, and a migration failure aborts cleanly before any listener
  binds — same rationale already documented for why pre-fork validation
  itself runs where it does.
- **Style precedent for a durable marker file**: `clean_flag_write` /
  `_exists` / `_remove` in `slotcask.c` (~line 2892) — `open` +
  `fsync(fd)` + `fsync` of the containing directory. `.version` reuses
  this pattern but stores content (the version string), not a
  zero-byte marker.

## Design decisions (flagging for approval — not silently picked)

1. **The `.version` gate is a small deep startup-version module shared by
   both entry points.** It decides whether startup is a no-op, should run
   this release's migration, should only stamp the version, or must refuse
   to start. Matching versions skip the whole per-object scan. A disk
   version newer than the binary is always refused.
2. **Missing `.version` is not automatically `0.0.0`.** A completely empty
   database means `$DB_ROOT/schema.conf` is absent or contains only blank
   and comment lines. Any other schema content, including malformed metadata,
   makes the root non-empty and cannot use the bootstrap bypass. An empty
   database is a bootstrap case: it bypasses any minimum-version floor,
   performs no data migration, writes the current version, and starts.
   A non-empty database with no `.version` is an unversioned legacy root.
   Earlier releases never wrote `.version`, so this release runs its reindex
   and then stamps the version; the minimum floor is informational only.
3. **Minimum source version is explicit release metadata.**
   `SHARD_DB_MIN_VERSION` is `2026.07.3` for this release. It is reported by
   the version command, but is informational and not enforced in this
   release because earlier releases did not write `.version`.
4. **Migration is optional per release.** The startup wrapper invokes the
   current release's migration implementation only when the disk version is
   older and that implementation exists. A release with no migration step
   simply stamps the current version after its version checks. This release's
   only migration phase is a full reindex of every materialised object.

   Reindex is new for 2026.08.1. This release fixes a bug where
   JSON-escaped varchar values could build mismatched index keys (see
   `docs/reference/changelog.md`, Unreleased: "JSON-escaped varchar values
   are decoded consistently before storage, indexing, and criteria
   comparison"), so an index shard written by any pre-2026.08.1 binary may
   already contain wrong keys on disk today. `reindex_object_checked`
   re-derives every index shard for an object straight from source-of-truth
   record data and publishes it atomically, which repairs that regardless
   of when or how the bad key was originally written — it's not specific
   to the escaping bug, it's a general "trust the records, not the old
   index files" repair. It is sequenced after `slotcask_close(&sdb)` in the
   per-object loop: it opens its own handle via the registry-based
   `slotcask_registry_get` (the same path the live `reindex` query mode
   and `cmd_add_index` use), which is distinct from this loop's direct
   `slotcask_open`/`slotcask_close`, and the two must not both be open on
   the same object at once. It has no dependency on the daemon's thread
   pools or listener — `reindex_object_checked` /
   `build_indexes_streaming_multi` spawn no threads of their own (no
   `pthread_create` anywhere in `index.c`) — only on `objlock_*` and the
   bt/bm/slotcask caches, all already initialized earlier in
   `shard_db_open_internal()` before `run_startup_migration()` is ever
   called. It runs unconditionally per object (not gated on this run's
   varlen outcome): by the point it runs, `sdb.format` is guaranteed
   `SLOTCASK_FORMAT_VARIABLE` for every object reached, because a failed
   varlen migration already `continue`s past that object earlier in the
   loop. Objects with no `indexes/index.conf` are a no-op (0 rebuilt),
   handled inside `reindex_object_checked` itself.

   Reindex is fatal to the batch on failure because migration must
   complete before `.version` is written. An interrupted run (crash,
   `kill -9`, disk full) simply leaves `.version` stale, so the next start
   redoes the whole batch from scratch; reindex fully rebuilds and
   atomically republishes each index, so retrying is safe.
5. **Both entry points call one shared function.** `shard_db_open()`
   (embedded) currently calls `run_startup_migration()` unconditionally
   on every open. This plan changes that call to the new version-gated
   `shard_db_startup_migrate()`, which is a **behavior change to embedded
   mode** beyond the literal daemon-only ask: embedded mode gains the
   downgrade/too-old refusal it didn't have, gains the missing `compact` and
   `reindex` phases, and gets faster (skips the full per-object scan once versions match)
   instead of always scanning. This is the cleanest way to avoid two
   copies of the same logic and matches "just like how embedded mode
   works" as the shared reference model. Flagging explicitly since it's
   a real behavior change to an existing, working path.
6. **Refusal messages distinguish downgrade from unsupported/unknown data.**
   A newer `.version` reports the recorded version and the running binary.
   A non-empty database below `SHARD_DB_MIN_VERSION`, including an unknown
   pre-versioning database when a floor is configured, reports the minimum
   required release and tells the operator to install that release or
   newer. The empty-database path never emits either refusal.

## Task 1 — `SHARD_DB_VERSION` constant + CalVer compare helper

**Test first.** Add to a new test case file
`src/test/cases/test_version_compare.c`:

```c
#include "test_assert.h"
#include "test_runner.h"
#include "types.h"
/* fixtures.h is required by the Task 6 version-subcommand test in this
   same file (tu_capture_cmd is declared there); pull it in up front so
   the file links from the first build rather than failing at Task 6. */
#include "fixtures.h"

static int test_version_compare_orders_calver_numerically(void) {
    /* Numeric, not lexical: "2026.09.5" < "2026.10.1" though "9" months
       into a 2-digit month means the month field is zero-padded. */
    ASSERT_TRUE(shard_db_version_compare("2026.09.5", "2026.10.1") < 0,
                "month compares numerically, not lexically");
    ASSERT_TRUE(shard_db_version_compare("2026.07.3", "2026.08.1") < 0,
                "older month is less");
    ASSERT_TRUE(shard_db_version_compare("2026.08.1", "2026.07.3") > 0,
                "newer month is greater");
    ASSERT_EQ_INT(shard_db_version_compare("2026.08.1", "2026.08.1"), 0,
                  "equal versions compare equal");
    ASSERT_TRUE(shard_db_version_compare("2027.01.1", "2026.12.9") > 0,
                "year dominates month/counter");
    ASSERT_TRUE(shard_db_version_compare("0.0.0", "2026.08.1") < 0,
                "the missing-.version sentinel sorts before any real release");
    ASSERT_TRUE(shard_db_version_compare("garbage", "2026.08.1") < 0,
                "malformed string sorts as older than any well-formed version");
    ASSERT_TRUE(shard_db_version_compare("2026.08.1junk", "2026.08.1") < 0,
                "trailing garbage is malformed, not equal");
    ASSERT_TRUE(shard_db_version_compare("2026.13.1", "2026.08.1") < 0,
                "out-of-range month is malformed");
    ASSERT_TRUE(shard_db_version_compare("026.08.1", "2026.08.1") < 0,
                "non-CalVer year width is malformed");
    /* Regression: the canonical zero-padded CalVer ("2026.08.1",
       SHARD_DB_VERSION, repo release-naming convention) must be VALID —
       the startup gate round-trips .version through shard_db_is_valid,
       and a parser that only accepted unpadded months would make every
       running install refuse to start on its own .version file. */
    ASSERT_TRUE(shard_db_version_is_valid("2026.08.1"),
                "zero-padded month is valid (the canonical on-disk form)");
    ASSERT_TRUE(shard_db_version_is_valid("2026.10.1"),
                "two-digit month is valid");
    ASSERT_TRUE(shard_db_version_is_valid("2026.12.9"),
                "unpadded counter is valid");
    ASSERT_TRUE(!shard_db_version_is_valid("2026.8.1"),
                "unpadded month is malformed");
    return 0;
}

TEST_REGISTER("version-compare", test_version_compare_orders_calver_numerically)
```

**Wire it into the build.** `src/test/cases/test_*.c` is a hand-maintained
list in `build.sh`'s `shard-db-test` gcc command (~line 167 onward), not a
glob — a new case file that isn't listed there never compiles into the test
binary and `run <name>` reports "unknown test" even after the code exists.
Add `src/test/cases/test_version_compare.c \` to that list now, alongside
the existing entries.

This must fail to compile/link (no `shard_db_version_compare` exists yet)
before the next step.

**Then add the constant and helper.**

New file `src/db/version.h`:

```c
#ifndef SHARD_DB_VERSION_H
#define SHARD_DB_VERSION_H

/* Compiled-in release metadata, CalVer yyyy.mm.N — see
   docs/reference/changelog.md for the scheme. Bump these in the same
   commit as every new docs/release-notes/<ver>.md + changelog.md entry.
   An empty minimum means this release accepts unknown pre-versioning
   databases and lets its idempotent migration path establish .version. */
#define SHARD_DB_VERSION "2026.08.1"
#define SHARD_DB_MIN_VERSION "2026.07.3"
#define SHARD_DB_ENFORCE_MIN_VERSION 0
#define SHARD_DB_HAS_STARTUP_MIGRATION 1

#define SHARD_DB_VERSION_FILE_OK      0
#define SHARD_DB_VERSION_FILE_MISSING 1
#define SHARD_DB_VERSION_FILE_ERROR  (-1)

enum ShardDbVersionDecision {
    SHARD_DB_VERSION_NOOP = 0,
    SHARD_DB_VERSION_RUN_MIGRATION = 1,
    SHARD_DB_VERSION_STAMP_ONLY = 2,
    SHARD_DB_VERSION_TOO_OLD = -3,
    SHARD_DB_VERSION_DOWNGRADE = -2,
    SHARD_DB_VERSION_INVALID = -4
};

#endif
```

In `src/db/util.c`, add (near the other small parsing helpers):

```c
/* Parse exactly yyyy.mm.N and compare numerically. A malformed string is
   never equal to a valid version and sorts older than a valid version.
   The canonical on-disk form is the repo's zero-padded CalVer — year
   %04d, month %02d, counter %d (e.g. "2026.08.1", matching
   SHARD_DB_VERSION and the docs/release-notes/ directory naming) — so
   the canonical round-trip below pads the month. Unpadded months
   ("2026.8.1") are malformed: .version is only ever written by the
   binary itself, which always emits the canonical form. */
static int parse_calver(const char *s, int out[3]) {
    char extra;
    if (!s || sscanf(s, "%d.%d.%d%c", &out[0], &out[1], &out[2], &extra) != 3)
        return 0;
    if (out[0] < 1000 || out[0] > 9999 ||
        out[1] < 1 || out[1] > 12 || out[2] < 0)
        return 0;
    char canonical[32];
    snprintf(canonical, sizeof(canonical), "%04d.%02d.%d",
             out[0], out[1], out[2]);
    if (strcmp(canonical, s) != 0) return 0;
    return 1;
}

int shard_db_version_compare(const char *a, const char *b) {
    int av[3], bv[3];
    int a_ok = parse_calver(a, av);
    int b_ok = parse_calver(b, bv);
    if (!a_ok && !b_ok) return 0;
    if (!a_ok) return -1;
    if (!b_ok) return 1;
    for (int i = 0; i < 3; i++) {
        if (av[i] != bv[i]) return av[i] < bv[i] ? -1 : 1;
    }
    return 0;
}

int shard_db_version_is_valid(const char *version) {
    int parsed[3];
    return parse_calver(version, parsed);
}
```

In `src/db/types.h`, add near the other `util.c` prototypes (locate the
block that already declares `b64_encode`/`valid_filename` and add
alongside):

```c
int shard_db_version_compare(const char *a, const char *b);
int shard_db_version_is_valid(const char *version);
int shard_db_version_decide(const char *disk_version, int version_present,
                            int db_empty, const char *current_version,
                            const char *minimum_version, int has_migration);
```

Also add `#include "version.h"` near the top of `types.h` (alongside the
existing local includes) so `SHARD_DB_VERSION` is visible everywhere
`types.h` already is.

**Verify**: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run version-compare` passes.

## Task 2 — `.version` file read/write helpers

**Test first.** Add to the same `test_version_compare.c` file (rename
test binary conceptually to cover both; keep one file, it's small):

```c
#include <unistd.h>
#include <sys/stat.h>

static int test_version_file_roundtrip(void) {
    char tmpdir[] = "/tmp/shard-db-version-test-XXXXXX";
    ASSERT_TRUE(mkdtemp(tmpdir) != NULL, "made tmpdir");

    char buf[64];
    ASSERT_EQ_INT(shard_db_version_file_read(tmpdir, buf, sizeof(buf)),
                  SHARD_DB_VERSION_FILE_MISSING,
                  "missing .version file is distinguishable");

    ASSERT_EQ_INT(shard_db_version_file_write(tmpdir, "2026.08.1"), 0,
                  "write succeeds");
    ASSERT_EQ_INT(shard_db_version_file_read(tmpdir, buf, sizeof(buf)), 0,
                  "read succeeds after write");
    ASSERT_EQ_STR(buf, "2026.08.1", "round-tripped content matches");

    char path[300];
    snprintf(path, sizeof(path), "%s/.version", tmpdir);
    unlink(path);
    ASSERT_EQ_INT(rmdir(tmpdir), 0, "cleaned up tmpdir");
    return 0;
}

TEST_REGISTER("version-file-roundtrip", test_version_file_roundtrip)
```

This must fail to link (no such functions yet) before the next step.

**Then add the helpers.** In `src/db/embedded.c`, add just above
`run_startup_migration` (which Task 3 will extend):

```c
/* $DB_ROOT/.version — durable record of which shard-db release last
   completed startup migration against this db_root. Returns
   SHARD_DB_VERSION_FILE_OK and fills out on success,
   SHARD_DB_VERSION_FILE_MISSING when the path does not exist, or
   SHARD_DB_VERSION_FILE_ERROR for an unreadable, oversized, or empty file.
   Missing and unreadable are intentionally distinct: a non-empty database
   must not silently mutate when its version evidence cannot be read. */
int shard_db_version_file_read(const char *db_root, char *out, size_t out_sz) {
    if (!db_root || !out || out_sz < 2) return SHARD_DB_VERSION_FILE_ERROR;
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/.version", db_root);
    FILE *f = fopen(path, "r");
    if (!f) return errno == ENOENT ? SHARD_DB_VERSION_FILE_MISSING
                                   : SHARD_DB_VERSION_FILE_ERROR;
    if (!fgets(out, (int)out_sz, f)) {
        fclose(f);
        return SHARD_DB_VERSION_FILE_ERROR;
    }
    if (!strchr(out, '\n') && !feof(f)) {
        fclose(f);
        return SHARD_DB_VERSION_FILE_ERROR;
    }
    fclose(f);
    out[strcspn(out, "\r\n")] = '\0';
    return out[0] ? SHARD_DB_VERSION_FILE_OK : SHARD_DB_VERSION_FILE_ERROR;
}

int shard_db_version_file_write(const char *db_root, const char *version) {
    if (!db_root || !version || !version[0]) return -1;
    char path[PATH_MAX];
    char tmp[PATH_MAX];
    snprintf(path, sizeof(path), "%s/.version", db_root);
    snprintf(tmp, sizeof(tmp), "%s/.version.tmp.XXXXXX", db_root);
    int fd = mkstemp(tmp);
    if (fd < 0) return -1;
    size_t len = strlen(version);
    const char *p = version;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) { close(fd); unlink(tmp); return -1; }
        p += n; len -= (size_t)n;
    }
    if (write(fd, "\n", 1) != 1 || fsync(fd) != 0) {
        close(fd); unlink(tmp); return -1;
    }
    if (close(fd) != 0 || rename(tmp, path) != 0) {
        unlink(tmp);
        return -1;
    }
    int dfd = open(db_root, O_RDONLY | O_DIRECTORY);
    if (dfd < 0 || fsync(dfd) != 0 || close(dfd) != 0) return -1;
    return 0;
}
```

Declare both in `types.h` near the other `embedded.c`-adjacent
declarations — `shard_db_open()` itself is declared in `shard_db.h`, not
`types.h`; the right spot is next to the existing `embedded.c` prototypes
already in `types.h`. Locate:

```c
int rebuild_recovery(const char *db_root);
int db_root_lock_acquire(const char *db_root, int *out_fd);
void db_root_lock_release(int *fd);
```

and add alongside:

```c
int shard_db_version_file_read(const char *db_root, char *out, size_t out_sz);
int shard_db_version_file_write(const char *db_root, const char *version);
```

**Verify**: `./build/bin/shard-db-test run version-file-roundtrip` passes.

## Task 3 — extend `run_startup_migration`, add the version gate

**Test first.** New case `src/test/cases/test_startup_auto_migration.c`
(uses the existing `TestEnv` fixture — same stop/mutate/restart pattern
already used by `test_durability_corrupt_marker_policy` in
`test_durability_ordering.c`):

```c
#include "test_assert.h"
#include "test_runner.h"
#include "fixtures.h"
#include "test_client.h"
#include "types.h"
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>
#include <dirent.h>

/* Fresh db_root: first start writes .version with the compiled-in
   SHARD_DB_VERSION; second start (versions now match) is a fast no-op —
   .version content is unchanged. */
static int test_startup_migration_bootstraps_version_file(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    int saved_port = env.port;
    test_env_stop_keep(&env);

    char vpath[PATH_MAX];
    snprintf(vpath, sizeof(vpath), "%s/.version", saved_db_root);
    char buf[64];
    FILE *f = fopen(vpath, "r");
    ASSERT_TRUE(f != NULL, ".version file exists after first start");
    if (f) { ASSERT_TRUE(fgets(buf, sizeof(buf), f) != NULL, "read .version"); fclose(f); }
    buf[strcspn(buf, "\n")] = '\0';
    ASSERT_EQ_STR(buf, SHARD_DB_VERSION, ".version matches the compiled-in version");

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "second start with matching .version succeeds");
    test_env_stop(&env);
    return 0;
}

/* A .version file claiming a newer release than this binary must refuse
   to start with a clear message, and must not touch any data. */
static int test_startup_refuses_downgrade(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    int saved_port = env.port;
    /* Stop before any direct filesystem mutation — same ordering as
       test_durability_corrupt_marker_policy, so nothing races the live
       daemon's own view of schema.conf. */
    test_env_stop_keep(&env);

    char tenant[PATH_MAX];
    snprintf(tenant, sizeof(tenant), "%s/tenant", saved_db_root);
    ASSERT_EQ_INT(mkdir(tenant, 0755), 0, "created tenant metadata");
    char schema[PATH_MAX];
    snprintf(schema, sizeof(schema), "%s/schema.conf", saved_db_root);
    FILE *sf = fopen(schema, "w");
    ASSERT_TRUE(sf != NULL, "created nonempty schema metadata");
    if (sf) { fputs("tenant:obj:8:16:2:1\n", sf); fclose(sf); }

    char vpath[PATH_MAX];
    snprintf(vpath, sizeof(vpath), "%s/.version", saved_db_root);
    FILE *f = fopen(vpath, "w");
    ASSERT_TRUE(f != NULL, "opened .version for overwrite");
    if (f) { fprintf(f, "9999.12.1\n"); fclose(f); }

    int rc = test_env_start_at(&env, saved_db_root, saved_port);
    ASSERT_TRUE(rc != 0, "daemon refuses to start against a newer .version");
    if (rc == 0) test_env_stop(&env);
    return 0;
}

/* Reindex is folded into this release's version-triggered migration batch
   (see Design decision 4). Simulate "wrong/stale on-disk index data"
   directly by deleting the indexed field's shard files while the daemon
   is stopped — index.conf (the field-list descriptor) is untouched, only
   the .idx shard content is gone — then confirm a version-triggered
   restart rebuilds them. A plain matching-version restart (see
   test_startup_migration_bootstraps_version_file) must not touch
   indexes at all; reindex only runs inside the version-gated batch. */
static int test_startup_migration_reindexes_stale_indexes(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    int saved_port = env.port;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_TRUE(tc != NULL, "connected to daemon");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"ridx\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:64\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: ridx");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"t\",\"object\":\"ridx\","
        "\"fields\":[\"name\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"ridx\","
        "\"key\":\"k1\",\"value\":{\"name\":\"alice\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc);

    test_env_stop_keep(&env);

    char idx_dir[PATH_MAX];
    snprintf(idx_dir, sizeof(idx_dir), "%s/t/ridx/indexes/name", saved_db_root);
    DIR *d = opendir(idx_dir);
    ASSERT_TRUE(d != NULL, "index shard directory exists before corruption");
    int deleted = 0;
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            char fpath[PATH_MAX];
            snprintf(fpath, sizeof(fpath), "%s/%s", idx_dir, e->d_name);
            if (unlink(fpath) == 0) deleted++;
        }
        closedir(d);
    }
    ASSERT_TRUE(deleted > 0,
                "deleted existing index shard file(s) to simulate stale data");

    char vpath[PATH_MAX];
    snprintf(vpath, sizeof(vpath), "%s/.version", saved_db_root);
    FILE *f = fopen(vpath, "w");
    ASSERT_TRUE(f != NULL, "opened .version for downgrade-stamp");
    if (f) { fprintf(f, "2026.07.1\n"); fclose(f); }

    ASSERT_EQ_INT(test_env_start_at(&env, saved_db_root, saved_port), 0,
                  "version-triggered restart succeeds");

    d = opendir(idx_dir);
    int has_file = 0;
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            has_file = 1;
            break;
        }
        closedir(d);
    }
    ASSERT_TRUE(has_file, "index shard rebuilt by version-triggered reindex");

    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("startup-migration-bootstrap", test_startup_migration_bootstraps_version_file)
TEST_REGISTER("startup-migration-refuses-downgrade", test_startup_refuses_downgrade)
TEST_REGISTER("startup-migration-reindexes-stale-indexes",
              test_startup_migration_reindexes_stale_indexes)
```

**Wire it into the build**, same reason as Task 1: add
`src/test/cases/test_startup_auto_migration.c \` to `build.sh`'s
`shard-db-test` source list.

Add these decision-level assertions to the same test file so the empty-root
exception and future minimum-version behavior are tested without mutating
compile-time release metadata:

```c
static int test_empty_root_bypasses_minimum_and_downgrade(void) {
    ASSERT_EQ_INT(shard_db_version_decide("9999.12.1", 1, 1,
                                          "2026.08.1", "2026.08.1", 1),
                  SHARD_DB_VERSION_STAMP_ONLY,
                  "empty root bootstraps even with newer evidence");
    return 0;
}

static int test_missing_nonempty_version_refuses_when_floor_exists(void) {
    ASSERT_EQ_INT(shard_db_version_decide(NULL, 0, 0,
                                          "2026.08.1", "2026.08.1", 1),
                  SHARD_DB_VERSION_TOO_OLD,
                  "unknown nonempty source is refused with a floor");
    return 0;
}

static int test_older_version_without_migration_only_stamps(void) {
    ASSERT_EQ_INT(shard_db_version_decide("2026.07.1", 1, 0,
                                          "2026.08.1", "", 0),
                  SHARD_DB_VERSION_STAMP_ONLY,
                  "older supported version without migration stamps only");
    return 0;
}

TEST_REGISTER("startup-migration-empty-version-exception",
              test_empty_root_bypasses_minimum_and_downgrade)
TEST_REGISTER("startup-migration-minimum-version",
              test_missing_nonempty_version_refuses_when_floor_exists)
TEST_REGISTER("startup-migration-no-step",
              test_older_version_without_migration_only_stamps)
```

This must fail before the implementation because the decision helper,
empty-root probe, and startup wrapper do not yet exist.

**Then extend `run_startup_migration`.** In `src/db/embedded.c`, locate the
current exact block below. The previous plan referred to a rebuild-kf phase
that is not present in this checkout; do not search for or invent that stale
anchor:

```c
        slotcask_close(&sdb);
    }
    fclose(f);

    return failed ? -1 : 0;
}
```

Replace the current tail with this current-release batch (varlen, then
compact, then reindex — see Design decision 4 for why reindex is included
for 2026.08.1 and why it is sequenced after `slotcask_close`). All three
phases are inside the version-triggered batch and never run on a
matching-version startup:

```c
        /* Compact is required for this release's migration batch; a failure
           must prevent .version from being stamped as complete. */
        if (sdb.format == SLOTCASK_FORMAT_VARIABLE) {
            TypedSchema *ts = load_typed_schema(eff_root, obj);
            if (ts) {
                fprintf(stderr, "[shard-db] compacting %s/%s...\n", dir, obj);
                if (slotcask_compact(&sdb, schema_trim_fn, (void *)ts) != 0)
                    failed = 1;
                else
                    fprintf(stderr, "[shard-db] compacted %s/%s\n", dir, obj);
            }
        }

        slotcask_close(&sdb);

        /* Reindex is required for this release's migration batch: 2026.08.1
           fixes a bug where JSON-escaped varchar values could build
           mismatched index keys, so an index shard written by an older
           binary may already be wrong on disk. reindex_object_checked
           re-derives every index shard from source-of-truth record data
           and publishes it atomically; it no-ops (0 rebuilt) for objects
           with no indexes/index.conf. Runs unconditionally here — by this
           point sdb.format is guaranteed VARIABLE for every object
           reached, since a failed varlen migration above already
           `continue`s past that object. Uses the registry-based
           slotcask_registry_get internally (via
           build_indexes_streaming_multi), distinct from this loop's
           direct slotcask_open/slotcask_close above — hence sequenced
           strictly after slotcask_close so the two are never open on the
           same object at once. */
        objlock_wrlock(eff_root, obj);
        int reindex_count = 0;
        int reindex_rc = reindex_object_checked(eff_root, obj, 0,
                                                &reindex_count);
        objlock_wrunlock(eff_root, obj);
        if (reindex_rc != 0) {
            fprintf(stderr, "[shard-db] reindex failed for %s/%s\n", dir, obj);
            failed = 1;
        } else if (reindex_count > 0) {
            fprintf(stderr, "[shard-db] reindexed %s/%s (%d indexes)\n",
                    dir, obj, reindex_count);
        }
    }
    fclose(f);

    return failed ? -1 : 0;
}

/* Empty means no active object records in schema.conf. Configuration-only
 * roots are allowed to bootstrap; schema-only roots are non-empty. */
static int db_root_is_empty(const char *db_root) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/schema.conf", db_root);
    FILE *f = fopen(path, "r");
    if (!f) return errno == ENOENT ? 1 : 0;

    char line[4096];
    int has_object = 0;
    while (fgets(line, sizeof(line), f)) {
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || *p == '\n' || *p == '\0') continue;
        /* Any non-comment schema content makes this a non-empty root.
           Malformed metadata must not obtain the empty-root bypass. */
        has_object = 1;
        break;
    }
    fclose(f);
    return has_object ? 0 : 1;
}

/* Shared startup seam used by cmd_server() and shard_db_open(). */
int shard_db_startup_migrate(const char *db_root,
                             char *out_disk_version, size_t out_sz) {
    char disk_version[64] = {0};
    int read_rc = shard_db_version_file_read(db_root, disk_version,
                                              sizeof(disk_version));
    int empty = db_root_is_empty(db_root);
    int present = (read_rc == SHARD_DB_VERSION_FILE_OK);

    if (read_rc == SHARD_DB_VERSION_FILE_ERROR && !empty) return -4;
    int decision = shard_db_version_decide(
        present ? disk_version : NULL, present, empty,
        SHARD_DB_VERSION, SHARD_DB_MIN_VERSION,
        SHARD_DB_HAS_STARTUP_MIGRATION);
    if (decision == SHARD_DB_VERSION_DOWNGRADE && out_disk_version)
        snprintf(out_disk_version, out_sz, "%s", disk_version);
    if (decision < 0) return decision;
    if (decision == SHARD_DB_VERSION_NOOP) return 0;
#if SHARD_DB_HAS_STARTUP_MIGRATION
    if (decision == SHARD_DB_VERSION_RUN_MIGRATION &&
        run_startup_migration(db_root) != 0)
        return -1;
#endif
    return shard_db_version_file_write(db_root, SHARD_DB_VERSION) == 0 ? 0 : -1;
}
```

`shard_db_startup_migrate` calls `shard_db_version_decide()`, which is
**not** defined in `embedded.c` — add it to `src/db/util.c` instead,
directly below the `parse_calver`/`shard_db_version_compare`/
`shard_db_version_is_valid` helpers from Task 1 (it's pure decision logic,
same home as the rest of the version-comparison code; the filesystem probe
`db_root_is_empty()` and the `shard_db_startup_migrate()` wrapper above stay
in `embedded.c` since they touch `db_root` and call `run_startup_migration`):

```c
int shard_db_version_decide(const char *disk_version, int version_present,
                            int db_empty, const char *current_version,
                            const char *minimum_version, int has_migration) {
    if (db_empty) return SHARD_DB_VERSION_STAMP_ONLY;
    if (!version_present)
        return (minimum_version && minimum_version[0])
             ? SHARD_DB_VERSION_TOO_OLD
             : (has_migration ? SHARD_DB_VERSION_RUN_MIGRATION
                              : SHARD_DB_VERSION_STAMP_ONLY);
    if (!disk_version || !shard_db_version_is_valid(disk_version))
        return SHARD_DB_VERSION_INVALID;
    if (minimum_version && minimum_version[0] &&
        (!shard_db_version_is_valid(minimum_version) ||
         shard_db_version_compare(disk_version, minimum_version) < 0))
        return SHARD_DB_VERSION_TOO_OLD;
    int cmp = shard_db_version_compare(disk_version, current_version);
    if (cmp > 0) return SHARD_DB_VERSION_DOWNGRADE;
    if (cmp == 0) return SHARD_DB_VERSION_NOOP;
    return has_migration ? SHARD_DB_VERSION_RUN_MIGRATION
                         : SHARD_DB_VERSION_STAMP_ONLY;
}
```

`run_startup_migration` itself stays `static` (it's now a private helper
of the new public wrapper, not called from anywhere else); only the new
`shard_db_startup_migrate` needs a `types.h` declaration:

```c
int shard_db_startup_migrate(const char *db_root, char *out_disk_version, size_t out_sz);
```
(add next to the `shard_db_version_file_*` declarations from Task 2)

**Update the one existing call site.** In `shard_db_open()`, locate:

```c
    /* Auto-migrate any FIXED-format objects before thread pools start.
       Migration is offline at this point — no registry entries open. */
    if (run_startup_migration(db_root) != 0) {
        fprintf(stderr, "shard_db_open: startup migration failed\n");
        g_shard_db_instance = NULL;
        g_db = NULL;
        /* Thread pools have not started yet. */
        db_cleanup_before_pools(db);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }
```

Replace with:

```c
    /* Auto-migrate any FIXED-format objects before thread pools start.
       Migration is offline at this point — no registry entries open.
       Version-gated: no-op once $DB_ROOT/.version matches this binary. */
    char disk_version[64] = {0};
    int mrc = shard_db_startup_migrate(db_root, disk_version, sizeof(disk_version));
    if (mrc == SHARD_DB_VERSION_DOWNGRADE) {
        fprintf(stderr,
                "shard_db_open: refusing to open: database version %s is newer "
                "than this binary (%s); install shard-db %s or newer.\n",
                disk_version, SHARD_DB_VERSION, disk_version);
        g_shard_db_instance = NULL;
        g_db = NULL;
        db_cleanup_before_pools(db);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }
    if (mrc == SHARD_DB_VERSION_TOO_OLD) {
        fprintf(stderr,
                "shard_db_open: refusing to open: this database requires "
                "shard-db %s or newer.\n", SHARD_DB_MIN_VERSION);
        g_shard_db_instance = NULL;
        g_db = NULL;
        db_cleanup_before_pools(db);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }
    if (mrc == SHARD_DB_VERSION_INVALID) {
        fprintf(stderr,
                "shard_db_open: refusing to open: %s/.version has invalid "
                "version evidence for a non-empty database.\n", db_root);
        g_shard_db_instance = NULL;
        g_db = NULL;
        db_cleanup_before_pools(db);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }
    if (mrc != 0) {
        fprintf(stderr, "shard_db_open: startup migration failed\n");
        g_shard_db_instance = NULL;
        g_db = NULL;
        /* Thread pools have not started yet. */
        db_cleanup_before_pools(db);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }
```

**Verify**: rebuild; `startup-migration-bootstrap`,
`startup-migration-refuses-downgrade`, and
`startup-migration-reindexes-stale-indexes` still won't fully pass yet
since `cmd_server` (Task 4) doesn't call the new function —
`test_env_start*` in this repo's harness spawns the real daemon via
`cmd_server`, not embedded mode, so these three tests only go green after
Task 4. That's expected and fine — Task 3's own build must compile
cleanly and the `version-compare` / `version-file-roundtrip` unit tests
from Tasks 1–2 must stay green.

## Task 4 — wire `cmd_server()` to the same gate

Locate this exact anchor in `src/db/server.c` (the closing of the
marker-recovery-sweep `if (!was_clean) { ... }` block, immediately
followed by the pre-fork validation comment):

```c
        }
    }

    /* Pre-fork validation: dirs.conf + schema.conf consistency must be
```

Insert the version gate + migration call between the two, so it reads:

```c
        }
    }

    /* Auto-migrate: compare $DB_ROOT/.version against this binary's
       compiled-in SHARD_DB_VERSION and migrate in-process if supported.
       Empty roots bootstrap; minimum-version, invalid-evidence, and
       downgrade refusals happen before fork/listen. */
    {
        char disk_version[64] = {0};
        int mrc = shard_db_startup_migrate(db_root, disk_version, sizeof(disk_version));
        if (mrc == SHARD_DB_VERSION_DOWNGRADE) {
            fprintf(stderr,
                    "shard-db: refusing to start: database version %s is newer "
                    "than this binary (%s); install shard-db %s or newer.\n",
                    disk_version, SHARD_DB_VERSION, disk_version);
            db_root_lock_release(&lock_fd);
            return 1;
        }
        if (mrc == SHARD_DB_VERSION_TOO_OLD) {
            fprintf(stderr,
                    "shard-db: refusing to start: this database requires "
                    "shard-db %s or newer.\n", SHARD_DB_MIN_VERSION);
            db_root_lock_release(&lock_fd);
            return 1;
        }
        if (mrc == SHARD_DB_VERSION_INVALID) {
            fprintf(stderr,
                    "shard-db: refusing to start: %s/.version has invalid "
                    "version evidence for a non-empty database.\n", db_root);
            db_root_lock_release(&lock_fd);
            return 1;
        }
        if (mrc != 0) {
            fprintf(stderr, "shard-db: refusing to start: startup migration failed\n");
            db_root_lock_release(&lock_fd);
            return 1;
        }
    }

    /* Pre-fork validation: dirs.conf + schema.conf consistency must be
```

**Verify**: `./build/bin/shard-db-test run startup-migration-bootstrap`,
`run startup-migration-refuses-downgrade`, and
`run startup-migration-reindexes-stale-indexes` all pass now (Task 3's
tests, exercised through the real daemon path this time).

## Task 5 — fix the stale BTRH rejection message

In `src/db/btree.c`, locate:

```c
        /* Reject any non-current btree format. V1 was string-keyed; V2
           lacked prev_leaf + last_leaf_page; V3 ('BTRG') had value-only
           leaf sort and value-only internal-page separators. The current
           format ('BTRH', 2026.05.5+) adds (value, hash) leaf sort and
           hash-bearing internal-page separators — same on-disk leaf byte
           layout but the comparator + descent semantics differ. Operators
           upgrade by running ./migrate which reindexes every object's
           btrees with the current format. */
```
and
```c
            fprintf(stderr,
                "btree: rejecting %s format at %s — run ./migrate to reindex\n",
                which, path);
            LOG_ERROR(LOG_SUB_BTREE, "bt_open_file %s: rejecting %s format — run ./migrate to reindex", path, which);
```

Replace with (the comment is corrected to describe what the tool
actually did even before its removal — reindex was always a separate
step; and the message now points at the real standalone command that
still exists post-removal):

```c
        /* Reject any non-current btree format. V1 was string-keyed; V2
           lacked prev_leaf + last_leaf_page; V3 ('BTRG') had value-only
           leaf sort and value-only internal-page separators. The current
           format ('BTRH', 2026.05.5+) adds (value, hash) leaf sort and
           hash-bearing internal-page separators — same on-disk leaf byte
           layout but the comparator + descent semantics differ. Operators
           upgrade by running `./shard-db reindex`, which rebuilds every
           object's btrees in the current format. */
```
and
```c
            fprintf(stderr,
                "btree: rejecting %s format at %s — run `./shard-db reindex`\n",
                which, path);
            LOG_ERROR(LOG_SUB_BTREE, "bt_open_file %s: rejecting %s format — run `./shard-db reindex`", path, which);
```

No test needed for a message-text-only change.

**Same stale-reference fix in the header.** `src/db/btree.h` carries the
header twin of the comment fixed above — `BT_MAGIC`'s doc block ends by
telling the operator to run the binary this plan deletes. Locate:

```c
/* File magic. "BTRH" (current, 2026.05.5+) = (value, hash) leaf sort +
   internal-page separators include hash, so single-key delete lands
   directly on the target leaf in O(log N) instead of walking the
   value-cluster chain. Previous "BTRG" (v3) added last_leaf_page +
   prev_leaf for O(1)-step DESC iteration; the v2 "BTRF" and v1 "BTRE"
   magics predate that. All non-current magics are rejected on open —
   run ./migrate to rebuild every btree. */
#define BT_MAGIC_V1  0x42545245u  /* legacy: string-keyed */
```

and change only the final clause to point at the standalone command that
still exists post-removal, mirroring the `btree.c` edit above:

```c
   magics predate that. All non-current magics are rejected on open —
   run `./shard-db reindex` to rebuild every btree. */
#define BT_MAGIC_V1  0x42545245u  /* legacy: string-keyed */
```

(Note for 2026.08.1 operators specifically: upgrading to this release
triggers a one-time automatic full reindex as part of its `.version`-gated
startup migration batch — see Task 8's changelog entry — so no manual
`./shard-db reindex` is needed for that particular upgrade. The header
keeps the generic `./shard-db reindex` pointer for on-demand use on
later releases.)

## Task 6 — `./shard-db version` subcommand

This plan introduces `SHARD_DB_VERSION`/`SHARD_DB_MIN_VERSION` as real
compiled-in constants for the first time (Task 1). Give operators a way to
read them from a built binary at runtime, not just from the one-time
post-build echo in Task 7 — useful for confirming what's actually deployed,
or what minimum source version a fresh binary requires before an upgrade.

**Test first.** Add to `src/test/cases/test_version_compare.c` (same file
as Tasks 1–2; it already covers version-related surface):

```c
static int test_version_subcommand_prints_compiled_version(void) {
    char *out = tu_capture_cmd("./build/bin/shard-db version 2>&1");
    ASSERT_TRUE(out != NULL, "captured version subcommand output");
    if (out) {
        ASSERT_TRUE(strstr(out, SHARD_DB_VERSION) != NULL,
                    "output contains the compiled-in version string");
        free(out);
    }
    return 0;
}

TEST_REGISTER("version-subcommand", test_version_subcommand_prints_compiled_version)
```

This must fail (no `version` subcommand exists yet — `tu_capture_cmd` gets
the usage-error output instead) before the next step.

**Then add the subcommand.** In `src/db/main.c`, locate:

```c
    const char *cmd = argv[1];

    /* JSON query mode */
    if (strcmp(cmd, "query") == 0) {
```

Insert between the two, so it reads:

```c
    const char *cmd = argv[1];

    if (strcmp(cmd, "version") == 0 || strcmp(cmd, "--version") == 0) {
        if (SHARD_DB_MIN_VERSION[0])
            printf("shard-db %s (minimum supported source version: %s)\n",
                   SHARD_DB_VERSION, SHARD_DB_MIN_VERSION);
        else
            printf("shard-db %s (no minimum source version floor)\n",
                   SHARD_DB_VERSION);
        return 0;
    }

    /* JSON query mode */
    if (strcmp(cmd, "query") == 0) {
```

Add one usage-help line under the existing `Lifecycle:` block near the top
of `main()` (locate `fprintf(stderr, "  server                               Start foreground (debug)\n");` and add directly after it):

```c
        fprintf(stderr, "  version                              Print compiled version + minimum supported source version\n");
```

`version`/`--version` intentionally does not call `load_db_root()` —
it must work from a bare, unconfigured checkout with no `db.env` present,
same as an operator's first action after unpacking a new release tarball.

**Verify**: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run version-subcommand` passes; `./build/bin/shard-db version` prints `shard-db 2026.08.1 (no minimum source version floor)`.

## Task 7 — delete `src/migrate/` and its build.sh wiring

Delete `src/migrate/main.c` and the now-empty `src/migrate/` directory.

In `build.sh`, locate:

```bash
# migrate — one-shot upgrade orchestrator. Spawns ./shard-db start, runs
# ./shard-db reindex (idempotent — rewrites btrees in the (value, hash)
# sort 2026.05.5 expects), then stops the daemon. Standalone binary; no
# daemon source linkage.
gcc $MODE_CFLAGS -o migrate src/migrate/main.c
[ "$DO_STRIP" = 1 ] && strip migrate

mkdir -p build/bin
```

Replace with:

```bash
mkdir -p build/bin
```

Locate:

```bash
cp shard-db shard-cli shard-db-test shard-db-test-server shard-db-bench migrate build/bin/
```

Replace with:

```bash
cp shard-db shard-cli shard-db-test shard-db-test-server shard-db-bench build/bin/
```

Locate:

```bash
echo "Upgrades: replace build/bin/ contents (shard-db + shard-cli + migrate), then run ./migrate once to rebuild B+ trees into 2026.05.5's (value, hash)-sorted layout. If still on v1 (pre-2026.05.5), first upgrade to 2026.05.4 and run that release's ./migrate."
```

Replace with (extracts the version strings from `src/db/version.h` at build
time via `sed` rather than hardcoding them in the shell script a second
place — the whole point of this plan is that a version string only ever
needs to be edited in one place per release; `sed -n 's/.../\1/p'` is
portable to both GNU sed on Linux and BSD sed on macOS, unlike `grep -P`):

```bash
BUILD_VERSION=$(sed -n 's/^#define SHARD_DB_VERSION "\(.*\)"/\1/p' src/db/version.h)
BUILD_MIN_VERSION=$(sed -n 's/^#define SHARD_DB_MIN_VERSION "\(.*\)"/\1/p' src/db/version.h)
if [ -n "$BUILD_MIN_VERSION" ]; then
    MIN_MSG="requires on-disk data from shard-db $BUILD_MIN_VERSION or newer (older/unversioned data is refused)"
else
    MIN_MSG="accepts data from any prior release (no minimum floor set by this release)"
fi
echo "Upgrades: replace build/bin/ contents (shard-db + shard-cli), then run ./shard-db start — this build is shard-db $BUILD_VERSION; it self-migrates \$DB_ROOT/.version automatically on start and $MIN_MSG. Run './shard-db version' any time to check a binary's version without starting it. Legacy v1 objects (pre-2026.05.5) still require the historical 2026.05.4 ./migrate upgrade path before this binary can open them."
```
(The `echo` above mirrors the operator-facing claim the Task 8 docs wording
must align with; keep the two in sync.)

**Update the one remaining source reference to the deleted binary.**
`main.c` still redirects the `migrate-files` subcommand to the standalone
binary this task removes, which would leave an operator-facing error message
pointing at a nonexistent file. Deleting the binary is what makes the message
stale, so this fix belongs in this task. Locate:

```c
    /* migrate-files moved to the standalone ./migrate binary in 2026.05.1.
       Redirect before the server check so the message lands whether the
       daemon is running or not. */
    if (strcmp(cmd, "migrate-files") == 0) {
        fprintf(stderr,
            "shard-db: 'migrate-files' moved to ./migrate in 2026.05.1.\n"
            "          Stop the daemon and run ./migrate instead.\n");
        return 1;
    }
```

Replace with (the migration it performed — lifting pre-2026.05.2
`<obj>/files/<XX>/<XX>/` hash buckets to flat layout — is exactly the
category automatic startup migration handles now, and the checked-in
migrate binary never actually implemented it; see the finding in
"Grounding"):

```c
    /* migrate-files was a one-shot 2026.05.1 upgrade step (lift
       pre-2026.05.2 <obj>/files/<XX>/<XX>/ hashed buckets to flat layout);
       the standalone ./migrate binary is removed as of 2026.08.1 and
       startup migration is automatic. Keep returning nonzero so scripts
       that predate the removal fail loudly instead of misrecognizing the
       command as a no-op. */
    if (strcmp(cmd, "migrate-files") == 0) {
        fprintf(stderr,
            "shard-db: 'migrate-files' was the 2026.05.1 one-shot file-layout\n"
            "          migration. Since 2026.08.1 migration is automatic:\n"
            "          start this binary and it self-migrates via $DB_ROOT/.version.\n");
        return 1;
    }
```

Grep `build.sh` once more after editing. Remove references to the deleted
orchestrator binary; retain only supported `shard-db` subcommands such as
`migrate-varlen` if they remain intentionally exposed.

**Verify**: no build command compiles or copies `migrate`; any remaining
`migrate-varlen` or `compact` text refers to a regular `shard-db` subcommand,
not the deleted orchestrator binary. Run `SKIP_TESTS=1 ./build.sh` and
confirm the printed "Upgrades:" line shows the real `SHARD_DB_VERSION`
value from `src/db/version.h`, not a placeholder.

## Task 8 — docs sync

Files below were found via repository search for `./migrate` and need
updating to describe the new automatic mechanism
instead of a manual `./migrate` step. **Leave `docs/release-notes/*.md`
and `docs/plans/*.md` untouched** — those are historical records of what
past releases actually shipped/discussed and must not be rewritten.

**Test first.** Record the current references before editing:

```bash
rg -n "(^|[[:space:]`])\./migrate([[:space:]`]|$)|build/bin/.*migrate" \
  docs AGENTS.md build.sh README.md
```

Update (remove/replace `./migrate` instructions with "start the binary;
it self-migrates via `$DB_ROOT/.version`, refusing to start on a
downgrade"):
- `docs/getting-started/install.md`
- `docs/getting-started/configuration.md`
- `docs/operations/deployment.md`
- `docs/query-protocol/schema-mutations.md`
- `docs/query-protocol/files.md`
- `docs/concepts/storage-model.md`
- `docs/concepts/indexes.md`
- `docs/cli/index.md`
- `AGENTS.md` (the "Operators upgrading..." paragraph near the CLI
  commands section, and the "2026.05.5 also rolls B+ tree magic..."
  paragraph — update both to reflect automatic startup migration.
  `./shard-db reindex` remains available standalone for on-demand
  operator use, but note that upgrading to 2026.08.1 specifically also
  triggers a one-time automatic full reindex as part of that release's
  `.version`-gated startup migration batch — see the changelog entry
  below — so operators don't need to run `./shard-db reindex` by hand
  after that particular upgrade.)
- `README.md` — three `./migrate` references, found via the grep above:
  1. Quick start build comment (~line 79):
     `./build.sh                      # builds shard-db (daemon) + shard-cli (TUI) + migrate (one-shot upgrades)`
     → `./build.sh                      # builds shard-db (daemon) + shard-cli (TUI)`
  2. Upgrade snippet (~lines 127-129):
     ```
     # replace build/bin/ contents with the new release (shard-db, shard-cli, migrate)
     ./migrate                       # runs every required migration for the new release; idempotent
     ```
     → replace with a comment + `./shard-db start` line reflecting automatic
     `.version`-gated migration (mirror the wording landed in Task 7's
     build.sh echo — same operator-facing claim, two places).
  3. The 2026.05.1-reissue historical note (~line 133) describing what
     that specific past release's `./migrate` did — this is a historical
     record of a shipped release, same category as `docs/release-notes/`;
     leave its content describing what 2026.05.1 actually required, but
     rephrase only the forward-looking clause so it doesn't imply
     `./migrate` still exists today (e.g. "the historical `./migrate`
     binary, since replaced by automatic startup migration").

  Also update the **ACID transactions** row of the comparison table
  (~line 49): change the cell from a bare `No` to `Per-record`, and add one
  sentence below the table (or a table footnote) citing
  `docs/concepts/concurrency.md`: single-key writes are atomic (single 8-byte
  commit store), durable (crash-safe + startup recovery sweep), and isolated
  (per-object rwlock) — i.e. real ACID properties at single-record/per-object
  granularity — but there is no multi-statement or cross-object transaction
  scope. Do not change the cell to a bare `Yes`; that overclaims
  multi-statement transaction support this release does not have.

  Optionally mention the new `./shard-db version` subcommand (Task 6)
  wherever the CLI command list is documented (`docs/cli/index.md`,
  and `AGENTS.md`'s CLI commands section under "Diagnostics").

Add a changelog entry under `## Unreleased` in
`docs/reference/changelog.md` describing: new `$DB_ROOT/.version` file,
automatic startup migration on both the daemon and embedded paths,
removal of the standalone `./migrate` binary, the new startup refusal on
a downgrade, and the new `./shard-db version` subcommand. Note that this
release's migration batch also runs a one-time full reindex of every
object automatically, and cross-reference the existing "JSON-escaped
varchar values are decoded consistently..." fix entry already in this
`## Unreleased` section as the motivating bug — that fix could leave
stale/mismatched index keys on disk from before it landed, and this
release's forced reindex repairs them without requiring the operator to
run `./shard-db reindex` by hand.

This task is documentation-only; re-run the repository search after editing
to confirm no stale reference to the deleted binary remains outside
`docs/release-notes/` and `docs/plans/`.

## Edge cases / invariants

- **First start under this release, empty `$DB_ROOT` (no schema.conf
  yet, e.g. brand-new install)**: `db_root_is_empty()` returns true before
  version comparison. No object migration — including reindex — or
  minimum/downgrade check runs; `.version` is atomically stamped with
  `SHARD_DB_VERSION`, and startup continues. Any future release can repeat
  this bootstrap behavior.
- **Empty root with stale, corrupt, or newer `.version`**: it is still an
  empty-root bootstrap and may replace the marker, because there is no data
  whose format could be downgraded.
- **Non-empty root without `.version`**: earlier releases never wrote the
  marker, so run this release's reindex and then stamp. The documented
  `2026.07.3` minimum is informational and not enforced in this release.
- **Older version with no migration step**: after minimum-version and
  validity checks, stamp the current version and start; do not scan objects.
- **Malformed `.version` on non-empty data**: refuse closed. Do not treat it
  as `0.0.0` and do not run migration.
- **Interrupted migration (crash mid-batch)**: `.version` is only
  written *after* the whole reindex returns success, so a crash during
  reindex leaves `.version` unwritten (or still at the old value) — next
  start retries the whole batch, including objects it already finished
  before the crash. Reindex is idempotent.
- **Downgrade or minimum-version refusal must not touch data.**
  `shard_db_startup_migrate` returns before calling `run_startup_migration`
  when the decision is downgrade, too old, or invalid evidence; no
  `slotcask_open` calls happen on those paths.
- **Concurrent daemon + embedded-mode access to the same `$DB_ROOT`** is
  already prevented by the pre-existing `db_root_lock_acquire` /
  single-instance guard, both of which run *before* the new migration
  call in both `cmd_server` and `shard_db_open` — unchanged ordering, so
  no new race is introduced by adding the version check alongside them.
- **`.version` write is temp-file + fsync + rename + directory-fsync.**
  A crash exposes the previous complete marker or the new complete marker,
  never an intentionally accepted partial version. A failed read on a
  non-empty root refuses closed.

## Build / test

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all --filter version
./build/bin/shard-db-test run-all --filter startup-migration
./build/bin/shard-db-test run-all
```

This diff touches daemon startup, `embedded.c`'s shared open path, and a
new on-disk marker file read under `db_root_lock_acquire` — per this
repo's standing AGENTS.md exception, run both sanitizer suites locally
against at least the affected cases before calling this done:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --filter "version|startup-migration|durability" --jobs 2

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --filter "version|startup-migration|durability" --jobs 1
```

## Execution rules

- Branch off `main`: `feat/auto-version-migration`.
- Do tasks in order (1 → 8); each task's test-first step must fail for
  the stated reason before its implementation step, and pass after.
- Build: `SKIP_TESTS=1 ./build.sh`. Test:
  `./build/bin/shard-db-test run[-all]`.
- Per this repo's standing execution-mode exception, leave all work
  **uncommitted** when done — the reviewing agent + human review the raw
  `git diff` before anything is committed.
- If a quoted anchor isn't found exactly as written, stop immediately —
  write `PLAN_NOTES.md` describing the exact mismatch (what was searched
  for, what's actually at that location) and halt the entire run, even
  for unrelated later tasks. Do not guess or reinterpret. Resuming
  requires a human or the planning model to read `PLAN_NOTES.md` and
  hand back a patched or fresh plan.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise, including but not limited to: any additional `./migrate`
  reference this plan's grep missed, any other call site of
  `run_startup_migration` beyond the one in `shard_db_open` this plan
  found, or any test in the existing suite that starts breaking because
  it depended on the old unconditional-scan embedded-mode behavior.
