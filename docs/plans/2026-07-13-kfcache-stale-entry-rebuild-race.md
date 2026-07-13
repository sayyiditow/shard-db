# kfcache/segcache stale-entry-on-rename fix — Implementation Plan

**Goal:** Fix the confirmed root cause of `test-rebuild-recovery`'s flaky
`>=99 records survive rebuild` assertion: `kfcache_acquire`/`segcache_acquire`
are pure path-string-keyed caches with no check that a cache hit still
refers to the file currently at that path. `rebuild_object_v2` renames an
object's `data/` directory away and recreates a fresh one at the same path;
if some other thread's `kfcache_acquire(writer=1)` call (most commonly the
async warmup subsystem, via `warmup_object_open()` → `slotcask_registry_get()`
→ `slotcask_open()`'s per-shard `parallel_for_io` fan-out) races in and
(re)installs a cache entry for a shard's kf/seg path between
`slotcask_registry_invalidate()` and the `rename()`, that entry survives the
rename untouched and gets served back as a cache **hit** to the rebuild's
own reopen at the identical path — aliasing the old object's data into the
freshly-rebuilt object. `kf_put_new` then finds genuine (verified)
duplicate keys during the rebuild walk and silently skips them, producing
`count < 100`.

**Architecture:** Add file-identity tracking (`st_dev`/`st_ino`, captured
via the `fstat()` already done at file-open time) to `KfCacheEntry` and
`SegCacheEntry`. On every cache hit in `kfcache_acquire`/`segcache_acquire`,
re-`stat()` the path and compare against the identity recorded at install
time; on mismatch (or the path no longer existing), evict the stale entry
and fall through to a real, fresh open instead of trusting the path string
alone. This is a **self-healing, general fix** — it closes the bug for
*any* racing caller (not just warmup), and for *any* rename-away-and-recreate
scenario, not just this one call site in `rebuild_object_v2`.

**Tech Stack:** C (POSIX `stat`/`fstat`, pthread mutex/rwlock). No new
dependencies.

## Global Constraints

- No new third-party dependencies.
- Build: `SKIP_TESTS=1 ./build.sh`. Full suite: `./build/bin/shard-db-test run-all`.
- This repo's execution mode (per `CLAUDE.md`): work stays **uncommitted**
  after execution — do not run `git add`/`git commit`/`git push` at any
  step below. The plan ends with the working tree containing the diff,
  ready for the human's/Sonnet's review.
- Branch off `main` before starting (fresh branch, name your own, e.g.
  `fix/kfcache-stale-entry-rebuild-race` — do not commit to `main`).
- If a quoted anchor below isn't found **exactly** in the file, stop and
  write `PLAN_NOTES.md` in the repo root describing the mismatch — do not
  guess or reinterpret the surrounding code.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.
- Do every task in order. Each task's "Run tests" step must actually be
  run and its real output pasted back — never claim a step passed without
  showing the command output.

---

## Background reading (do this before Task 1)

Read `src/db/slotcask.c` in full for the following functions so you
understand the existing locking discipline before touching it:
`kfcache_probe`, `kfcache_drop_slot`, `kfcache_invalidate_prefix`,
`kf_open_file`, `kfcache_acquire`, `segcache_probe`, `segcache_drop_slot`,
`seg_open_file`, `segcache_acquire`.

Key invariants already in the codebase (do not change these):
- `kfcache_drop_slot`/`segcache_drop_slot` must be called **while the
  caller holds `g_kfcache_lock`/`g_segcache_lock` (the mutex)**, and must
  **not** be called while the caller holds the per-entry `rwlock` — the
  file header comment above `kfcache_invalidate_prefix` explains the
  lock-ordering hazard this avoids (holding the per-entry rwlock while
  reaching for the table mutex can deadlock against the install path,
  which acquires them in the opposite order).
- `kfcache_acquire`/`segcache_acquire` return with the per-slot `rwlock`
  **held** on every success path (`writer=1` → wrlock, `writer=0` →
  rdlock); the caller releases via `kfcache_release`/`segcache_release`.
  Every new return path added below must preserve this contract exactly.

---

## Task 1: Add file-identity fields to the cache entry structs

**Files:**
- Modify: `src/db/shard_db_internal.h`

**Interfaces:**
- Produces: `KfCacheEntry.file_dev` (`dev_t`), `KfCacheEntry.file_ino`
  (`ino_t`), `SegCacheEntry.file_dev` (`dev_t`), `SegCacheEntry.file_ino`
  (`ino_t`) — consumed by Task 2 (populated at install) and Task 3/5
  (compared on hit).

- [ ] **Step 1: Edit the struct definitions**

Find this exact block **in `src/db/shard_db_internal.h`** (the struct
definitions carry `/* slotcask.c — kfcache */` / `/* slotcask.c —
segcache */` breadcrumb comments noting which `.c` file consumes them,
but the structs themselves are defined in this header — do not look for
them in `slotcask.c`):

```c
/* slotcask.c — kfcache */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *base;
    size_t   map_size;
    size_t   capacity;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
    _Atomic uint64_t gen;   /* incremented on eviction; SlotRef validation */
} KfCacheEntry;

/* slotcask.c — segcache */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
    _Atomic uint64_t gen;   /* incremented on eviction; SlotRef validation */
} SegCacheEntry;
```

Replace it with:

```c
/* slotcask.c — kfcache */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *base;
    size_t   map_size;
    size_t   capacity;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
    _Atomic uint64_t gen;   /* incremented on eviction; SlotRef validation */
    dev_t    file_dev;      /* identity of the file open at install time — */
    ino_t    file_ino;      /* detects rename-away-and-recreate at `path` */
} KfCacheEntry;

/* slotcask.c — segcache */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
    _Atomic uint64_t gen;   /* incremented on eviction; SlotRef validation */
    dev_t    file_dev;      /* identity of the file open at install time — */
    ino_t    file_ino;      /* detects rename-away-and-recreate at `path` */
} SegCacheEntry;
```

(`dev_t`/`ino_t` are already available — `<sys/stat.h>` is included near
the top of `types.h`, which includes this header at its end.)

- [ ] **Step 2: Build to confirm it still compiles (fields unused so far)**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: clean build, no errors (unused-field warnings are not expected
since both fields get written in Task 2).

---

## Task 2: Capture file identity in `kf_open_file` / `seg_open_file`

**Files:**
- Modify: `src/db/slotcask.c`

**Interfaces:**
- Produces: `kf_open_file(..., dev_t *out_dev, ino_t *out_ino)` and
  `seg_open_file(..., dev_t *out_dev, ino_t *out_ino)` — two new trailing
  out-params on each, populated from the `fstat()` each function already
  performs. Consumed by Task 3 (kfcache_acquire) and Task 5 (segcache_acquire).

- [ ] **Step 1: Update `kf_open_file`'s signature and body**

Find this exact block:

```c
static int kf_open_file(const char *path, size_t slots_capacity, int writer,
                        int *out_fd, uint8_t **out_base, size_t *out_size) {
```

Replace with:

```c
static int kf_open_file(const char *path, size_t slots_capacity, int writer,
                        int *out_fd, uint8_t **out_base, size_t *out_size,
                        dev_t *out_dev, ino_t *out_ino) {
```

Then find this exact block (the tail of the same function):

```c
    *out_fd = fd;
    *out_base = (uint8_t *)m;
    *out_size = want;
    return 0;
}

/* Populate handle's hdr/map/capacity from the cache entry's stored base. */
```

Replace with:

```c
    *out_fd = fd;
    *out_base = (uint8_t *)m;
    *out_size = want;
    *out_dev = st.st_dev;
    *out_ino = st.st_ino;
    return 0;
}

/* Populate handle's hdr/map/capacity from the cache entry's stored base. */
```

(`st` is the `struct stat` already populated by the `fstat(fd, &st)` call
earlier in this same function — no new syscall.)

- [ ] **Step 2: Update `seg_open_file`'s signature and body**

Find this exact block:

```c
static int seg_open_file(const char *path, int create,
                         int *out_fd, uint8_t **out_map, size_t *out_size) {
```

Replace with:

```c
static int seg_open_file(const char *path, int create,
                         int *out_fd, uint8_t **out_map, size_t *out_size,
                         dev_t *out_dev, ino_t *out_ino) {
```

Then find this exact block (the tail of the same function):

```c
    *out_fd = fd;
    *out_map = (uint8_t *)m;
    *out_size = SLOTCASK_SEG_MAX_BYTES;
    return 0;
}

int segcache_acquire(SlotcaskSegHandle *h, const char *path,
```

Replace with:

```c
    *out_fd = fd;
    *out_map = (uint8_t *)m;
    *out_size = SLOTCASK_SEG_MAX_BYTES;
    *out_dev = st.st_dev;
    *out_ino = st.st_ino;
    return 0;
}

int segcache_acquire(SlotcaskSegHandle *h, const char *path,
```

- [ ] **Step 3: Build — this will fail** (call sites not updated yet)

Run: `SKIP_TESTS=1 ./build.sh`
Expected: FAIL — compiler errors about `kf_open_file`/`seg_open_file`
being called with too few arguments, at their 3 call sites each. This is
expected; Task 3 and Task 5 fix the call sites. Do not attempt to work
around this — proceed directly to Task 3.

---

## Task 3: Fix `kfcache_acquire`'s call sites and add staleness validation

**Files:**
- Modify: `src/db/slotcask.c`

**Interfaces:**
- Consumes: `kf_open_file` from Task 2, `KfCacheEntry.file_dev/file_ino`
  from Task 1.
- Produces: `kfcache_acquire` now evicts and re-opens on a stale hit
  instead of aliasing. Behavior on a **valid** hit (`stat()` matches) is
  unchanged. Consumed by Task 4 (regression test).

Locate `kfcache_acquire` in full (`int kfcache_acquire(SlotcaskKfHandle *h,
const char *path, size_t slots_capacity, int writer) { ... }`). It has
four `kf_open_file`/`kf_handle_from_uncached` call sites and two
path-string cache-hit checks ("hit1" in the retry loop, "hit2" in the
lost-the-install-race branch). Replace the **entire function body**
(everything between the opening `{` and the matching closing `}`) with
the version below — this single replacement covers all four call-site
fixes and both staleness checks at once, since they interleave with the
existing control flow.

- [ ] **Step 1: Replace the full `kfcache_acquire` function**

Find the function starting at:

```c
int kfcache_acquire(SlotcaskKfHandle *h, const char *path,
                    size_t slots_capacity, int writer) {
```

and ending at its matching closing brace, immediately followed by:

```c
void kfcache_release(SlotcaskKfHandle *h) {
```

Replace the whole function with:

```c
int kfcache_acquire(SlotcaskKfHandle *h, const char *path,
                    size_t slots_capacity, int writer) {
    h->slot = -1;
    h->writer = writer;
    h->fd = -1;
    h->hdr = NULL;
    h->map = NULL;
    h->map_size = 0;
    h->capacity = 0;

    if (!g_kfcache) {
        /* Cache not initialised — direct mmap, no locking. */
        int fd; uint8_t *base; size_t sz; dev_t dev; ino_t ino;
        if (kf_open_file(path, slots_capacity, writer, &fd, &base, &sz, &dev, &ino) < 0) return -1;
        kf_handle_from_uncached(h, fd, base, sz);
        return 0;
    }

    /* Verify-and-retry on cache hit (mirrors bt_acquire). */
    int retries = 0;
    int found = 0, slot = -1;
    pthread_mutex_lock(&g_kfcache_lock);
    while (1) {
        slot = kfcache_probe(path, &found);
        if (!found) break;
        g_kfcache[slot].last_access =
            __atomic_add_fetch(&g_kfcache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &g_kfcache[slot].rwlock;
        pthread_mutex_unlock(&g_kfcache_lock);
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);

        /* coverity[atomicity] CID 1693850: `slot` came from the prior
           locked section, but we re-verify identity below
           (e->used && strcmp(e->path, path) == 0). On mismatch we
           drop the rwlock and retry — the verify is the consistency
           barrier the analyzer can't see. */
        KfCacheEntry *e = &g_kfcache[slot];
        if (e->used && strcmp(e->path, path) == 0) {
            struct stat pst;
            if (stat(path, &pst) == 0 &&
                pst.st_dev == e->file_dev && pst.st_ino == e->file_ino) {
                h->slot = slot;
                kf_handle_from_entry(h, e);
                /* coverity[missing_unlock] intentional: returning with the
                   per-slot rwlock held; caller releases via kfcache_release. */
                return 0;
            }
            /* Cached entry no longer matches the file currently at
               `path` — e.g. rebuild_object_v2 renamed data/ away and
               recreated it after this entry was installed by a racing
               kfcache_acquire(writer=1) (commonly warmup's
               slotcask_open() fan-out). Evict it so the retry below (or
               the miss path) re-opens the real current file instead of
               aliasing stale pre-rebuild data. */
            pthread_rwlock_unlock(lock);
            pthread_mutex_lock(&g_kfcache_lock);
            if (g_kfcache[slot].used && strcmp(g_kfcache[slot].path, path) == 0) {
                kfcache_drop_slot(slot);
            }
            if (++retries >= 4) break;
            continue;
        }
        pthread_rwlock_unlock(lock);
        if (++retries >= 4) {
            /* slot/found get re-set by the kfcache_probe call below
               in the install path (Coverity CID 1693833). */
            pthread_mutex_lock(&g_kfcache_lock);
            break;
        }
        pthread_mutex_lock(&g_kfcache_lock);
    }

    /* Miss path: open + install. Drop table lock during open since it can
       block on disk. */
    pthread_mutex_unlock(&g_kfcache_lock);
    int fd; uint8_t *base; size_t sz; dev_t dev; ino_t ino;
    if (kf_open_file(path, slots_capacity, writer, &fd, &base, &sz, &dev, &ino) < 0) return -1;
    pthread_mutex_lock(&g_kfcache_lock);

    /* Re-probe — another thread may have installed it while we were opening. */
    slot = kfcache_probe(path, &found);
    if (found) {
        g_kfcache[slot].last_access =
            __atomic_add_fetch(&g_kfcache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &g_kfcache[slot].rwlock;
        pthread_mutex_unlock(&g_kfcache_lock);
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);
        KfCacheEntry *e = &g_kfcache[slot];
        int matched = e->used && strcmp(e->path, path) == 0;
        if (matched) {
            struct stat pst;
            if (stat(path, &pst) == 0 &&
                pst.st_dev == e->file_dev && pst.st_ino == e->file_ino) {
                /* Genuinely valid — lost the install race. Discard our
                   own open; use the cached entry. */
                munmap(base, sz);
                close(fd);
                h->slot = slot;
                kf_handle_from_entry(h, e);
                /* coverity[missing_unlock] intentional: returning with the
                   per-slot rwlock held; caller releases via kfcache_release. */
                return 0;
            }
        }
        pthread_rwlock_unlock(lock);
        if (matched) {
            /* The racing installer's entry is itself stale (same
               rebuild-rename race, one level deeper). Evict it so it
               can't alias stale data for the next caller. */
            pthread_mutex_lock(&g_kfcache_lock);
            if (g_kfcache[slot].used && strcmp(g_kfcache[slot].path, path) == 0) {
                kfcache_drop_slot(slot);
            }
            pthread_mutex_unlock(&g_kfcache_lock);
        }
        /* Slot was evicted under us, or was just evicted above for
           staleness. Our own fd/base/sz (opened moments ago) are
           current — serve them uncached this once instead of opening
           a third time. */
        kf_handle_from_uncached(h, fd, base, sz);
        return 0;
    }

    /* Evict LRU if half-full or no empty slot. */
    if (slot < 0 || g_kfcache_count >= g_kfcache_slots / 2) {
        int lru = -1;
        uint64_t oldest = UINT64_MAX;
        for (int i = 0; i < g_kfcache_slots; i++) {
            if (g_kfcache[i].used && g_kfcache[i].last_access < oldest) {
                oldest = g_kfcache[i].last_access;
                lru = i;
            }
        }
        if (lru >= 0) { kfcache_drop_slot(lru); slot = lru; }
    }

    if (slot < 0) {
        /* Cache truly full — serve uncached. */
        pthread_mutex_unlock(&g_kfcache_lock);
        kf_handle_from_uncached(h, fd, base, sz);
        return 0;
    }

    KfCacheEntry *e = &g_kfcache[slot];
    strncpy(e->path, path, PATH_MAX - 1);
    e->path[PATH_MAX - 1] = '\0';
    e->fd = fd;
    e->base = base;
    e->map_size = sz;
    e->capacity = (sz - SLOTCASK_KF_HDR_SIZE) / sizeof(SlotcaskKfEntry);
    e->used = 1;
    e->last_access = __atomic_add_fetch(&g_kfcache_clock, 1, __ATOMIC_RELAXED);
    e->file_dev = dev;
    e->file_ino = ino;
    g_kfcache_count++;

    /* Take rwlock before releasing table mutex (closes evict-after-install race). */
    pthread_rwlock_t *lock = &e->rwlock;
    if (writer) pthread_rwlock_wrlock(lock);
    else        pthread_rwlock_rdlock(lock);
    pthread_mutex_unlock(&g_kfcache_lock);

    h->slot = slot;
    kf_handle_from_entry(h, e);
    return 0;
}
```

- [ ] **Step 2: Build**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: still FAILS — `seg_open_file`'s 3 call sites (in
`segcache_acquire`) are not fixed yet. That's Task 5. `kf_open_file`'s
call sites should now compile clean; if the build reports errors inside
`kfcache_acquire` specifically, stop and write `PLAN_NOTES.md` — do not
guess a fix.

---

## Task 4: Regression test for the kfcache staleness fix (write and confirm it fails first)

**Files:**
- Create: `src/test/cases/test_kfcache_staleness.c`
- Modify: `build.sh`

**Interfaces:**
- Consumes: `slotcask_init`, `kfcache_acquire`, `kfcache_release`,
  `slotcask_shutdown` (all from `slotcask.h`, already public).

This test reproduces the aliasing bug **directly and deterministically,
without any threads**: install a kfcache entry, rename the underlying
file away (simulating `rebuild_object_v2`'s `rename(data_dir,
legacy_dir)`), then acquire the same path again. Before the Task 3 fix,
this returns the stale cached header. After the fix, it detects the
identity mismatch and opens the real (fresh) file.

- [ ] **Step 1: Write the test file**

Create `src/test/cases/test_kfcache_staleness.c`:

```c
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include <string.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <unistd.h>

/* Regression test for the test-rebuild-recovery flakiness root cause:
   kfcache_acquire used to be a pure path-string-keyed cache with no check
   that a cache hit still refers to the file currently at that path.
   rebuild_object_v2 renames an object's data/ directory away and recreates
   a fresh one at the same path; if some other thread's
   kfcache_acquire(writer=1) call (e.g. the warmup subsystem's
   slotcask_open() fan-out) raced in and (re)installed a cache entry for a
   shard's kf path between slotcask_registry_invalidate() and the rename,
   that stale entry survived the rename and got served back to the
   rebuild's own reopen at the identical path — aliasing the OLD object's
   data into the freshly-rebuilt one and causing phantom duplicate-key
   rejections during the walk (test-rebuild-recovery's flaky
   ">=99 records survive rebuild" assertion).

   This test reproduces the aliasing directly: install a cache entry for a
   kf path, rename the file away (simulating the rebuild's rename), then
   acquire the same path again. Before the fix: returns the stale cached
   header (total=42, wrong — the bug). After the fix: kfcache_acquire
   detects the cached entry no longer matches the file at `path` and
   re-opens fresh (total=0, correct). */
static int test_kfcache_staleness_run(void) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-kfcache-staleness-%d", getpid());
    mkdir(tmpdir, 0755);

    slotcask_init(64, 64);

    char path[300];
    snprintf(path, sizeof(path), "%s/000.kf", tmpdir);
    char legacy[300];
    snprintf(legacy, sizeof(legacy), "%s/000.kf.legacy", tmpdir);

    SlotcaskKfHandle h1;
    int ret = kfcache_acquire(&h1, path, 8, 1);
    ASSERT_EQ_INT(ret, 0, "first acquire creates+caches kf file");
    h1.hdr->total = 42;
    kfcache_release(&h1);

    /* Simulate rebuild_object_v2's rename(data_dir, legacy_dir): the file
       backing the cached entry moves away. Nothing invalidates the cache
       here — this models a racing installer's entry surviving the
       rename, exactly as slotcask_open()'s per-shard kfcache_acquire
       fan-out can when it lands between slotcask_registry_invalidate and
       the rename. */
    ret = rename(path, legacy);
    ASSERT_EQ_INT(ret, 0, "rename kf file away, simulating rebuild's data->legacy rename");

    /* new_db's own reopen at the identical path, as rebuild_object_v2
       does right after the rename. Must NOT alias the old (renamed-away)
       file's contents — it must see a fresh, empty header. */
    SlotcaskKfHandle h2;
    ret = kfcache_acquire(&h2, path, 8, 1);
    ASSERT_EQ_INT(ret, 0, "second acquire after rename succeeds");
    ASSERT_EQ_INT((int)h2.hdr->total, 0,
        "second acquire must see a fresh file (total=0), not alias the renamed-away file's total=42");
    kfcache_release(&h2);

    slotcask_shutdown();
    system("rm -rf /tmp/shard-db-kfcache-staleness-*");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-kfcache-staleness", test_kfcache_staleness_run)
```

- [ ] **Step 2: Register the test file in `build.sh`**

Find this exact line in `build.sh`:

```
    src/test/cases/test_slotcask_api.c \
```

Replace with:

```
    src/test/cases/test_kfcache_staleness.c \
    src/test/cases/test_slotcask_api.c \
```

- [ ] **Step 3: Build and confirm the new test passes**

Since Task 3 already applied the fix, this test should pass now. Run:

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-kfcache-staleness
```

Expected: PASS (all assertions, including `total=0`, succeed).

- [ ] **Step 4: Verify the test actually catches the bug (temporarily revert Task 3, confirm failure, then re-apply)**

This step proves the regression test is real, not a tautology. Temporarily
revert Task 3's fix in a scratch copy, run the test, confirm it FAILS with
the `total=0` assertion reporting `total=42` instead, then restore Task 3's
fix.

```bash
git stash push -- src/db/slotcask.c
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-kfcache-staleness
```

Expected: FAIL — `second acquire must see a fresh file (total=0)...`
reports actual value 42.

```bash
git stash pop
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-kfcache-staleness
```

Expected: PASS again (Task 3's fix restored).

If `git stash` isn't usable in your environment (e.g. no git available to
the execution sandbox), instead: comment out the two `struct stat pst; if
(stat(...) ...)` staleness-check blocks added in Task 3 (temporarily
falling straight through to `return 0` on any path match, matching
pre-fix behavior), rebuild, run the test, confirm the FAIL, then uncomment
and rebuild again to confirm PASS.

---

## Task 5: Fix `segcache_acquire`'s call sites and add the same staleness validation

**Files:**
- Modify: `src/db/slotcask.c`

**Interfaces:**
- Consumes: `seg_open_file` from Task 2, `SegCacheEntry.file_dev/file_ino`
  from Task 1.
- Produces: `segcache_acquire` now evicts and re-opens on a stale hit,
  mirroring Task 3's fix for `kfcache_acquire`. Same bug class:
  `slotcask_registry_invalidate` also calls `segcache_invalidate_prefix`,
  and `rebuild_object_v2`'s reopen (`slotcask_open(&new_db, obj_dir,
  ...)`) also populates per-stream segment cache entries the same way the
  kf shards were populated — leaving `segcache_acquire` unfixed would
  leave the identical race in place, just one layer down (a stale
  segcache hit during the rebuild walk would write records into the
  wrong, about-to-be-orphaned segment file instead of merely rejecting a
  duplicate key — a *more* severe silent-data-loss variant of the same
  bug, not a milder one).

Locate `segcache_acquire` in full (`int segcache_acquire(SlotcaskSegHandle
*h, const char *path, int create, int writer) { ... }`). Replace the
entire function body the same way Task 3 did for `kfcache_acquire`.

- [ ] **Step 1: Replace the full `segcache_acquire` function**

Find the function starting at:

```c
int segcache_acquire(SlotcaskSegHandle *h, const char *path,
                     int create, int writer) {
```

and ending at its matching closing brace, immediately followed by:

```c
void segcache_release(SlotcaskSegHandle *h) {
```

Replace the whole function with:

```c
int segcache_acquire(SlotcaskSegHandle *h, const char *path,
                     int create, int writer) {
    h->slot = -1;
    h->writer = writer;
    h->fd = -1;
    h->map = NULL;
    h->map_size = 0;

    if (!g_segcache) {
        dev_t dev; ino_t ino;
        if (seg_open_file(path, create, &h->fd, &h->map, &h->map_size, &dev, &ino) < 0) return -1;
        return 0;
    }

    int retries = 0;
    int found = 0, slot = -1;
    pthread_mutex_lock(&g_segcache_lock);
    while (1) {
        slot = segcache_probe(path, &found);
        if (!found) break;
        g_segcache[slot].last_access =
            __atomic_add_fetch(&g_segcache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &g_segcache[slot].rwlock;
        pthread_mutex_unlock(&g_segcache_lock);
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);

        /* coverity[atomicity] CID 1693837: `slot` came from the prior
           locked section, but we re-verify identity below
           (e->used && strcmp(e->path, path) == 0). On mismatch we
           drop the rwlock and retry — the verify is the consistency
           barrier the analyzer can't see. */
        SegCacheEntry *e = &g_segcache[slot];
        if (e->used && strcmp(e->path, path) == 0) {
            struct stat pst;
            if (stat(path, &pst) == 0 &&
                pst.st_dev == e->file_dev && pst.st_ino == e->file_ino) {
                h->slot = slot;
                h->fd = e->fd;
                h->map = e->map;
                h->map_size = e->map_size;
                /* coverity[missing_unlock] intentional: returning with the
                   per-slot rwlock held; caller releases via segcache_release. */
                return 0;
            }
            /* Cached entry no longer matches the file currently at
               `path` — same rebuild-rename staleness class as
               kfcache_acquire. Evict it so the retry below (or the miss
               path) re-opens the real current file. */
            pthread_rwlock_unlock(lock);
            pthread_mutex_lock(&g_segcache_lock);
            if (g_segcache[slot].used && strcmp(g_segcache[slot].path, path) == 0) {
                segcache_drop_slot(slot);
            }
            if (++retries >= 4) break;
            continue;
        }
        pthread_rwlock_unlock(lock);
        if (++retries >= 4) {
            /* slot/found get re-set by the segcache_probe call below
               in the install path (Coverity CID 1693845). */
            pthread_mutex_lock(&g_segcache_lock);
            break;
        }
        pthread_mutex_lock(&g_segcache_lock);
    }

    pthread_mutex_unlock(&g_segcache_lock);
    int fd; uint8_t *map; size_t sz; dev_t dev; ino_t ino;
    if (seg_open_file(path, create, &fd, &map, &sz, &dev, &ino) < 0) return -1;
    pthread_mutex_lock(&g_segcache_lock);

    slot = segcache_probe(path, &found);
    if (found) {
        g_segcache[slot].last_access =
            __atomic_add_fetch(&g_segcache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &g_segcache[slot].rwlock;
        pthread_mutex_unlock(&g_segcache_lock);
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);
        SegCacheEntry *e = &g_segcache[slot];
        int matched = e->used && strcmp(e->path, path) == 0;
        if (matched) {
            struct stat pst;
            if (stat(path, &pst) == 0 &&
                pst.st_dev == e->file_dev && pst.st_ino == e->file_ino) {
                munmap(map, sz);
                close(fd);
                h->slot = slot;
                h->fd = e->fd;
                h->map = e->map;
                h->map_size = e->map_size;
                /* coverity[missing_unlock] intentional: returning with the
                   per-slot rwlock held; caller releases via segcache_release. */
                return 0;
            }
        }
        pthread_rwlock_unlock(lock);
        if (matched) {
            pthread_mutex_lock(&g_segcache_lock);
            if (g_segcache[slot].used && strcmp(g_segcache[slot].path, path) == 0) {
                segcache_drop_slot(slot);
            }
            pthread_mutex_unlock(&g_segcache_lock);
        }
        h->slot = -1;
        h->fd = fd;
        h->map = map;
        h->map_size = sz;
        return 0;
    }

    if (slot < 0 || g_segcache_count >= g_segcache_slots / 2) {
        int lru = -1;
        uint64_t oldest = UINT64_MAX;
        for (int i = 0; i < g_segcache_slots; i++) {
            if (g_segcache[i].used && g_segcache[i].last_access < oldest) {
                oldest = g_segcache[i].last_access;
                lru = i;
            }
        }
        if (lru >= 0) { segcache_drop_slot(lru); slot = lru; }
    }

    if (slot < 0) {
        pthread_mutex_unlock(&g_segcache_lock);
        h->slot = -1;
        h->fd = fd;
        h->map = map;
        h->map_size = sz;
        return 0;
    }

    SegCacheEntry *e = &g_segcache[slot];
    strncpy(e->path, path, PATH_MAX - 1);
    e->path[PATH_MAX - 1] = '\0';
    e->fd = fd;
    e->map = map;
    e->map_size = sz;
    e->used = 1;
    e->last_access = __atomic_add_fetch(&g_segcache_clock, 1, __ATOMIC_RELAXED);
    e->file_dev = dev;
    e->file_ino = ino;
    g_segcache_count++;

    pthread_rwlock_t *lock = &e->rwlock;
    if (writer) pthread_rwlock_wrlock(lock);
    else        pthread_rwlock_rdlock(lock);
    pthread_mutex_unlock(&g_segcache_lock);

    h->slot = slot;
    h->fd = fd;
    h->map = map;
    h->map_size = sz;
    return 0;
}
```

- [ ] **Step 2: Build**

Run: `SKIP_TESTS=1 ./build.sh`
Expected: PASS (clean build, no errors). If it fails, stop and write
`PLAN_NOTES.md` — do not guess a fix.

---

## Task 6: Regression test for the segcache staleness fix

**Files:**
- Create: `src/test/cases/test_segcache_staleness.c`
- Modify: `build.sh`

**Interfaces:**
- Consumes: `slotcask_init`, `segcache_acquire`, `segcache_release`,
  `slotcask_shutdown` (all from `slotcask.h`).

Same technique as Task 4, mirrored for segments: write a marker byte at
offset 0 of the mmap, rename the file away, reacquire at the same path,
confirm the new acquire sees a fresh (zeroed) file rather than the old
marker byte.

- [ ] **Step 1: Write the test file**

Create `src/test/cases/test_segcache_staleness.c`:

```c
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include <string.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <unistd.h>

/* Mirrors test_kfcache_staleness.c for segcache_acquire: segments are
   populated by the same class of rebuild-rename race as kf shards (see
   test_kfcache_staleness.c's comment for the full mechanism). A stale
   segcache hit during a rebuild walk is a *more* severe variant of the
   bug than the kf case — instead of merely rejecting a duplicate key, it
   would write freshly-inserted records into the wrong, about-to-be
   -orphaned segment file, silently losing them. */
static int test_segcache_staleness_run(void) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-segcache-staleness-%d", getpid());
    mkdir(tmpdir, 0755);

    slotcask_init(64, 64);

    char path[300];
    snprintf(path, sizeof(path), "%s/000000.dat", tmpdir);
    char legacy[300];
    snprintf(legacy, sizeof(legacy), "%s/000000.dat.legacy", tmpdir);

    SlotcaskSegHandle s1;
    int ret = segcache_acquire(&s1, path, 1, 1);
    ASSERT_EQ_INT(ret, 0, "first acquire creates+caches segment file");
    s1.map[0] = 0xAB;
    segcache_release(&s1);

    /* Simulate rebuild_object_v2's rename(data_dir, legacy_dir). */
    ret = rename(path, legacy);
    ASSERT_EQ_INT(ret, 0, "rename segment file away, simulating rebuild's rename");

    SlotcaskSegHandle s2;
    ret = segcache_acquire(&s2, path, 1, 1);
    ASSERT_EQ_INT(ret, 0, "second acquire after rename succeeds");
    ASSERT_EQ_INT((int)s2.map[0], 0,
        "second acquire must see a fresh segment (byte0=0), not alias the renamed-away file's byte0=0xAB");
    segcache_release(&s2);

    slotcask_shutdown();
    system("rm -rf /tmp/shard-db-segcache-staleness-*");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-segcache-staleness", test_segcache_staleness_run)
```

- [ ] **Step 2: Register the test file in `build.sh`**

Find this exact line in `build.sh` (added by Task 4, Step 2):

```
    src/test/cases/test_kfcache_staleness.c \
```

Replace with:

```
    src/test/cases/test_kfcache_staleness.c \
    src/test/cases/test_segcache_staleness.c \
```

- [ ] **Step 3: Build and confirm the new test passes**

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-segcache-staleness
```

Expected: PASS.

- [ ] **Step 4: Verify the test actually catches the bug**

Same approach as Task 4 Step 4: temporarily revert the two staleness
`stat()` checks added in Task 5 (or `git stash` the whole file and
manually reapply Task 3's kfcache fix without Task 5's segcache fix),
rebuild, confirm `test-segcache-staleness` FAILS with byte0 reported as
`0xAB` (171) instead of 0, then restore the fix and confirm PASS again.

---

## Task 7: Full suite regression check, including the original flaky test

**Files:** none (verification only).

- [ ] **Step 1: Run the full test suite**

```
./build/bin/shard-db-test run-all
```

Expected: PASS, same pass count as `main` plus the 2 new tests
(`test-kfcache-staleness`, `test-segcache-staleness`). No regressions.

- [ ] **Step 2: Run `test-rebuild-recovery` repeatedly to confirm the flakiness is gone**

This is the actual end-to-end confirmation — the whole reason for this
fix. Run it enough times to be confident given the original ~16-40%
observed failure rate:

```bash
for i in $(seq 1 30); do
  ./build/bin/shard-db-test run test-rebuild-recovery || echo "FAILED on run $i"
done
```

Expected: all 30 runs PASS (no `FAILED on run N` lines). If any run
fails, do not weaken the assertion or mark it skip/xfail — stop and
report the failure with the run's output; this would mean the fix is
incomplete, and the investigation needs to continue rather than declaring
victory prematurely.

- [ ] **Step 3: Report final state**

Leave the working tree **uncommitted** (per this repo's standing
execution mode). Report: full suite pass/fail counts, the 30-run
`test-rebuild-recovery` loop's result, and a summary of the diff
(`git diff --stat`) for the human/Sonnet review pass. Do not run `git
add`, `git commit`, `git push`, or open a PR — those are explicitly out
of scope for this execution step.

---

## Notes / explicitly out of scope

- This plan does **not** touch `rebuild_object_v2`, `warmup_object_open`,
  or any caller of `kfcache_acquire`/`segcache_acquire` — the fix is
  entirely inside the cache layer itself, which is why it self-heals
  regardless of which caller races in. Do not add locking or sequencing
  changes to `query_find.c` or `server.c` as part of this plan.
- Do not touch `kfcache_acquire_direct`/`segcache_acquire_direct` (the
  `SlotRef` fast-path functions) — they fall through to the just-fixed
  `kfcache_acquire`/`segcache_acquire` on any `gen` mismatch (including
  the eviction this fix now performs on a stale hit), so they inherit the
  fix automatically without needing their own `stat()` call on every
  warm hit. Adding a `stat()` call to the hot per-record fast path would
  be a real performance regression and is not needed for correctness —
  do not add one.
- All diagnostic instrumentation used during the original investigation
  (`DEBUG_KFCACHE`/`DEBUG_REBUILD`/`DEBUG_WARMUP` fprintf blocks, the
  `usleep()` race-widening calls, and the `test_env_stop` forensic-keep
  edit) has already been reverted on `main` before this plan was written.
  Do not reintroduce any of it.
