# Plan: pointer-indirected growable objlock directory, immortal entries, fail-closed lock API

## Status

**READY FOR HUMAN APPROVAL.** Not yet executed — no code changed, no tests
run. Anchors below are quoted text, verified 2026-09-06 against the tree at
branch `perf/request-level-commit-batching` (HEAD `bc1dede`). Line numbers
are navigation aids only and will drift — grep the quoted text.

Do not execute until the human approves this plan explicitly. This plan is
scoped to exactly three things, per explicit instruction: (a) a
pointer-indirected growable directory replacing the fixed 256-bucket
`g_objlocks` table, (b) immortal entries (no eviction, ever), (c) fixing the
fail-open NULL-return bug so callers cannot proceed unprotected. It does
**not** touch `db_root_lock_acquire`, `g_dirs`/`DIRS_BUCKETS`, or any file
cache (BtCache/BmCache/KfCache/SegCache) — those are separate tables with
separate lifecycles, out of scope.

A second agent is concurrently executing
`docs/plans/2026-09-05-request-level-commit-batching.md` on
`perf/request-level-commit-batching`. This plan branches off `main`
instead — execution starts only after that batching work finishes and
merges, on a fresh branch off `main`, not a continuation of the batching
branch. This plan touches `src/db/objlock.c`,
`src/db/shard_db_internal.h` (objlock-only region), `src/db/types.h`
(objlock-only prototypes), `src/db/embedded.c` (shutdown wiring only), the
12 objlock call sites enumerated below, `docs/concepts/concurrency.md`, and
adds two new test files. It does not touch commit/durability/marker code,
slotcask bulk-window logic, or query_bulk.c beyond reading its
alloc-failure-injection idiom as a pattern to mirror — no expected file
overlap with the concurrent batching work. If a diff conflict appears
anyway when this plan is executed, treat it as a merge concern to resolve
at execution time, not a reason to change this plan's design.

## Root cause

`g_objlocks` (`src/db/shard_db_internal.h`) is a fixed `ObjLockEntry
objlocks[OBJLOCK_BUCKETS]` array, `OBJLOCK_BUCKETS = 256`, open-addressed
with linear probing on `obj_str_hash(key) % OBJLOCK_BUCKETS`. `get_lock()`
in `src/db/objlock.c` probes for a free-or-matching slot; when the full
256-entry table has no free slot for a new `(db_root, object)` key, it
falls through to:

```c
    LOG_ERROR(LOG_SUB_SERVER, "objlock get_lock: table full (%d buckets), object '%s' will run WITHOUT rwlock protection",
               OBJLOCK_BUCKETS, key);
    return NULL;
```

Every one of the four public entry points then treats `NULL` as
"proceed without the lock":

```c
void objlock_rdlock(const char *db_root, const char *object) {
    pthread_rwlock_t *l = get_lock(db_root, object);
    if (l) pthread_rwlock_rdlock(l);
}
```

(`objlock_wrlock`/`objlock_rdunlock`/`objlock_wrunlock` mirror this.) This
is a **fail-open** design: exhausting the table silently disables the
mutual-exclusion guarantee for every subsequent operation on any object
whose key hashes into a full table, reintroducing the exact
concurrent-rebuild-vs-reader use-after-free the lock exists to prevent
(`slotcask_registry_get()`'s raw `SlotcaskDb*` racing
`slotcash_registry_invalidate()`). With ~576 B per entry the table itself
is not a leak and not large, but at real-world tenant/object counts (a
handful of objects × many tenants) 256 buckets is reachable, and there is
no eviction or growth — the table is a hard, silent ceiling.

The fix is two independent changes, both required for correctness:

1. **Make the table dynamically growable** so the ceiling is removed in
   practice (thousands of live objects supported, not 256).
2. **Make the failure path fail closed**: an allocation failure growing
   the directory must be reported to the caller, who must abort the
   operation rather than proceed unprotected.

## Fix shape

Replace the fixed array with a pointer-indirected open-addressed
directory: `ObjLockEntry **objlock_dir`, doubled at 50% load factor
(mirrors the documented "50% kf load → double slots_per_shard" pattern in
AGENTS.md). The entries themselves (`ObjLockEntry`, containing the
`pthread_rwlock_t`) are individually heap-allocated and **never moved or
freed** once created — only the directory of pointers is reallocated on
growth, copying pointer values (not entry contents) into a larger, rehashed
array. Because entry addresses are stable for the life of the process, no
in-flight lock holder can ever be invalidated by a resize, and no
eviction/pinning/refcounting machinery is needed at all: an entry is either
absent (never requested) or permanently present once first requested.

This removes the lock-free fast-path probe (`_Atomic int used` +
lock-free scan) from the old design: with entries individually allocated
and the directory itself resizable, a lock-free probe of the directory
array is not safe against a concurrent resize (the array being read might
be swapped from under the reader). The new design takes the table mutex
(`g_objlock_table_lock`) for every lookup — a plain `pthread_mutex_lock`
around an array-index dereference and pointer-chase is cheap compared to
the actual `pthread_rwlock_{rd,wr}lock` call that follows, so this is not
a meaningful regression; it trades a lock-free read fast path (unsafe
under growth) for a short, uncontended mutex hold (safe under growth).

`objlock_rdlock`/`objlock_wrlock` become `int`-returning: `0` on success,
`-1` if directory growth or entry allocation failed (OOM). Every call site
must check the return and abort its operation on `-1` rather than proceed.
`objlock_rdunlock`/`objlock_wrunlock` stay `void`: unlock only ever
resolves an existing entry (a lock that was successfully acquired earlier
is guaranteed to still be in the directory, since entries are immortal),
so it cannot allocate and cannot practically fail; a `NULL` resolve there
indicates a caller bug (mismatched lock/unlock), which is logged, not
propagated as a new error path.

## Call-site / consumer audit (verified 2026-09-06; executor re-runs the greps before editing)

All production call sites of `objlock_rdlock`/`objlock_wrlock` (the two
functions changing signature). `objlock_rdunlock`/`objlock_wrunlock` keep
their current `void` signature and current call sites are unaffected.

1. `src/db/embedded.c`, inside `shard_db_recover_before_stamp`:
   ```c
   objlock_wrlock(eff_root, entries[i].object);
   ...
   objlock_wrunlock(eff_root, entries[i].object);
   ```
   (loop over recovered markers during startup recovery, before the server
   accepts connections).
2. `src/db/query_maint.c`, `cmd_restore` — first statement of the
   lock-protected section:
   ```c
   objlock_wrlock(db_root, object);
   ```
3. `src/db/server.c`, `dispatch_nql_query`:
   ```c
   objlock_rdlock(db_root, cmd.obj);
   ```
   guarding an NQL_COUNT/FIND/AGGREGATE switch, unlocked via
   `objlock_rdunlock(db_root, cmd.obj);` at the function's return path.
4. `src/db/server.c`, `dispatch_json_query`, drop-object branch:
   ```c
   objlock_wrlock(drop_eff_root, object);
   ```
5. `src/db/server.c`, `dispatch_json_query`, describe-object branch:
   ```c
   objlock_rdlock(db_root, object);
   ```
6. `src/db/server.c`, `dispatch_json_query`, generic dispatch path:
   ```c
   int took_wrlock = mode_is_schema(mode);
   int took_rdlock = !took_wrlock;
   if (took_wrlock) objlock_wrlock(db_root, object);
   else if (took_rdlock) objlock_rdlock(db_root, object);
   ```
   (the largest call site — ~600 lines of dispatch logic between this lock
   and its two unlock points).
7. `src/db/server.c`, `server_process_fast` (legacy tab-separated
   protocol):
   ```c
   int fast_wr = mode_is_schema(cmd);
   int fast_rd = !fast_wr;
   if (fast_wr) objlock_wrlock(eff_root, object);
   else if (fast_rd) objlock_rdlock(eff_root, object);
   ```
8. `src/db/server.c`, `warmup_kf_task_fn` (background warmup thread):
   ```c
   objlock_rdlock(t->eff, t->obj);
   ```
9. `src/db/server.c`, `warmup_thread` (background directory-scan warmup):
   ```c
   objlock_rdlock(dir_path, de->d_name);
   ```
10. `src/db/server.c`, `auto_vacuum_sweep_one`:
    ```c
    objlock_wrlock(eff, obj_name);
    ```
11. `src/db/server.c`, `auto_reshard_sweep_one`:
    ```c
    objlock_wrlock(eff, obj_name);
    ```
12. `src/db/index.c`, `cmd_reindex`:
    ```c
    objlock_wrlock(eff_root, obj);
    ```

Each fix follows the enclosing function's existing error-handling idiom
(early `return`/`continue`/`goto`, or an error-JSON reply) — see each
call site's task below for the exact patch.

## Embedded execution rules

- Branch: always off `main` — e.g. `fix/objlock-dynamic-directory`. Never
  continue on `perf/request-level-commit-batching`; that branch is the
  concurrent agent's, and this work starts only after it finishes.
- Execution mode (this repo's standing exception): leave the diff
  **uncommitted** — human + reviewing agent review the raw `git diff`
  before anything is committed.
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test
  run-all` (narrow during iteration with `--filter objlock`).
- Dynamic-safety gate (this diff touches locks and shared state — the
  gate applies, not deferred to CI): run `BUILD_MODE=asan SKIP_TESTS=1
  ./build.sh` then three fresh consecutive `./build/bin/shard-db-test
  run-all`; then `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh` then three
  fresh consecutive `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
  ./build/bin/shard-db-test run-all`. No `halt_on_error=0`, no
  suppressions file. All 3×2 = 6 runs must be clean before this is done.
- If a quoted anchor in this plan is not found exactly in the tree at
  execution time: write `PLAN_NOTES.md` describing the mismatch and halt
  the entire run immediately — do not guess or reinterpret. Resuming
  requires the human (or the planning model) to read `PLAN_NOTES.md` and
  hand back a patched or fresh plan.
- If execution hits a decision this plan doesn't cover: stop and ask.

## Task 1 — regression test proving the current bug (red on base)

Test-first: write `src/test/cases/test_objlock_dynamic_growth.c` against
the **current** (pre-fix) `objlock.c` first, confirm it fails for the
expected reason, then proceed to Tasks 2-6 (the fix), then confirm it
passes.

This test cannot literally instantiate the old fixed-256 table from
outside `objlock.c` (no test hook exists to shrink the cap), so the red
run is proven differently: temporarily build with `OBJLOCK_BUCKETS`
(or, post-fix, `OBJLOCK_INITIAL_CAP`) set to a tiny value via a one-line
local edit — e.g. temporarily change the constant to `4` — rebuild, run
this test, and confirm the mutual-exclusion assertion fails (or the
process logs "table full ... WITHOUT rwlock protection" from the old
code, confirming the fail-open path is live); then revert the temporary
constant edit before continuing. Paste both the failing-run output and
the reverted diff as evidence.

```c
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <unistd.h>

/* Regression test for the objlock table-exhaustion bug: on the old
   fixed-size table, once every bucket held a distinct key, get_lock()
   returned NULL and every objlock_rdlock/wrlock call silently proceeded
   WITHOUT taking any lock. This test creates far more than the old
   256-bucket ceiling worth of distinct (db_root, object) keys and then
   proves real mutual exclusion still holds on one of the keys well past
   that ceiling, and that an injected allocation failure is reported
   rather than swallowed. See docs/plans/2026-09-06-objlock-dynamic-directory.md. */

#define NUM_KEYS 2000
#define PAST_CEILING_INDEX 1500 /* > old OBJLOCK_BUCKETS (256) */

static _Atomic int g_growth_rd_ready;
static _Atomic int g_growth_rd_release;
static _Atomic int g_growth_observed_rd_held;

static void *growth_rd_wait_worker(void *arg) {
    const char *obj = (const char *)arg;
    objlock_rdlock("growth-root", obj);
    atomic_fetch_add(&g_growth_rd_ready, 1);
    while (atomic_load(&g_growth_rd_release) == 0) usleep(1000);
    objlock_rdunlock("growth-root", obj);
    return NULL;
}

static void *growth_releaser(void *arg) {
    (void)arg;
    atomic_store(&g_growth_observed_rd_held, 1);
    atomic_store(&g_growth_rd_release, 1);
    return NULL;
}

static int test_objlock_dynamic_growth_run(void) {
    objlock_init();

    char names[NUM_KEYS][32];
    for (int i = 0; i < NUM_KEYS; i++) {
        snprintf(names[i], sizeof(names[i]), "obj-%d", i);
        int rc = objlock_rdlock("growth-root", names[i]);
        ASSERT_EQ_INT(rc, 0, "rdlock succeeds well past the old 256-bucket ceiling");
        objlock_rdunlock("growth-root", names[i]);
    }

    /* Real mutual-exclusion proof on a key past the old ceiling: a reader
       parks inside its rdlock; main's wrlock must block until the reader
       (via the releaser thread) lets go. On the fail-open path this key's
       lock was skipped entirely, so objlock_wrlock here would return
       immediately without g_growth_observed_rd_held having been set yet
       — this assertion is what catches that. */
    const char *key = names[PAST_CEILING_INDEX];
    pthread_t reader, releaser;
    atomic_store(&g_growth_rd_ready, 0);
    atomic_store(&g_growth_rd_release, 0);
    atomic_store(&g_growth_observed_rd_held, 0);
    pthread_create(&reader, NULL, growth_rd_wait_worker, (void *)key);
    while (atomic_load(&g_growth_rd_ready) < 1) usleep(1000);
    pthread_create(&releaser, NULL, growth_releaser, NULL);
    int wrc = objlock_wrlock("growth-root", key);
    ASSERT_EQ_INT(wrc, 0, "wrlock succeeds");
    ASSERT_EQ_INT(atomic_load(&g_growth_observed_rd_held), 1,
                  "wrlock blocked until the reader released - real mutual exclusion past the old 256-object ceiling");
    objlock_wrunlock("growth-root", key);
    pthread_join(reader, NULL);
    pthread_join(releaser, NULL);

    /* Injected allocation failure must fail closed. */
    objlock_test_set_fail_alloc(1);
    int fail_rc = objlock_rdlock("growth-root", "never-seen-before-key");
    ASSERT_EQ_INT(fail_rc, -1, "injected allocation failure is reported, not swallowed");
    objlock_test_set_fail_alloc(0);
    /* No matching rdunlock: the lock was never taken. */

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-objlock-dynamic-growth", test_objlock_dynamic_growth_run)
```

This is a unit-style case (reuses the runner's process-local `ShardDb`,
per AGENTS.md; creates only synthetic lock names under `growth-root:*`, no
real db objects, so there is nothing to clean up). Mirrors
`test_objlock_unit.c`'s atomic-barrier idiom (no sleep-based timing
assumptions beyond the `usleep(1000)` poll loops already used there).

## Task 2 — `shard_db_internal.h`: struct and macro changes

Anchor (current):

```c
/* objlock.c */
#define OBJLOCK_BUCKETS 256
typedef struct {
    char name[512];
    pthread_rwlock_t rwlock;
    _Atomic int used;
} ObjLockEntry;
```

Replace with:

```c
/* objlock.c */
#define OBJLOCK_INITIAL_CAP 1024
typedef struct {
    char name[512];
    pthread_rwlock_t rwlock;
} ObjLockEntry;
```

Anchor (current, inside `ShardDb`):

```c
    ObjLockEntry objlocks[OBJLOCK_BUCKETS];
    pthread_mutex_t objlock_table_lock;
```

Replace with:

```c
    ObjLockEntry **objlock_dir;
    uint32_t objlock_dir_cap;
    uint32_t objlock_dir_count;
    pthread_mutex_t objlock_table_lock;
```

Anchor (current macros):

```c
#define g_objlocks (g_db->objlocks)
#define g_objlock_table_lock (g_db->objlock_table_lock)
```

Replace with:

```c
#define g_objlock_dir (g_db->objlock_dir)
#define g_objlock_dir_cap (g_db->objlock_dir_cap)
#define g_objlock_dir_count (g_db->objlock_dir_count)
#define g_objlock_table_lock (g_db->objlock_table_lock)
```

## Task 3 — `types.h`: prototype changes

Anchor (current):

```c
void objlock_init(void);
void objlock_rdlock(const char *db_root, const char *object);
void objlock_rdunlock(const char *db_root, const char *object);
void objlock_wrlock(const char *db_root, const char *object);
void objlock_wrunlock(const char *db_root, const char *object);
```

Replace with:

```c
void objlock_init(void);
void objlock_shutdown(void);
int objlock_rdlock(const char *db_root, const char *object);   /* 0 ok, -1 alloc failure */
void objlock_rdunlock(const char *db_root, const char *object);
int objlock_wrlock(const char *db_root, const char *object);   /* 0 ok, -1 alloc failure */
void objlock_wrunlock(const char *db_root, const char *object);
void objlock_test_set_fail_alloc(int fail_n); /* test-only: fail the Nth guarded allocation */
```

## Task 4 — `objlock.c`: core rewrite

Replace the entirety of the table/`get_lock`/public-API region (from the
`OBJLOCK_BUCKETS`-era comment through the four public functions) with:

```c
static int g_objlock_test_fail_alloc;

void objlock_test_set_fail_alloc(int fail_n) {
    g_objlock_test_fail_alloc = fail_n;
}

static int objlock_should_fail_alloc(void) {
    if (g_objlock_test_fail_alloc > 0 && --g_objlock_test_fail_alloc == 0) return 1;
    return 0;
}

/* Directory growth: doubles at 50% load, mirroring the kf-shard
   "50% load -> double slots_per_shard" pattern (see AGENTS.md). Entries
   are never moved or freed by a grow -- only the array of pointers is
   reallocated and rehashed; existing ObjLockEntry addresses (and any
   rwlock currently held by a caller) stay valid. Caller holds
   g_objlock_table_lock. Returns 0 on success, -1 on allocation failure
   (directory left unchanged). */
static int objlock_dir_grow_locked(void) {
    if (g_objlock_dir_cap > UINT32_MAX / 2) { errno = EOVERFLOW; return -1; }
    uint32_t new_cap = g_objlock_dir_cap ? g_objlock_dir_cap * 2 : OBJLOCK_INITIAL_CAP;
    if (objlock_should_fail_alloc()) return -1;
    ObjLockEntry **new_dir = calloc(new_cap, sizeof(*new_dir));
    if (!new_dir) return -1;
    for (uint32_t i = 0; i < g_objlock_dir_cap; i++) {
        ObjLockEntry *e = g_objlock_dir[i];
        if (!e) continue;
        uint32_t idx = obj_str_hash(e->name) % new_cap;
        while (new_dir[idx]) idx = (idx + 1) % new_cap;
        new_dir[idx] = e;
    }
    free(g_objlock_dir);
    g_objlock_dir = new_dir;
    g_objlock_dir_cap = new_cap;
    return 0;
}

/* Find-or-create the entry for `key`. Caller holds g_objlock_table_lock.
   Grows the directory first if load would exceed 50%. Returns NULL on
   allocation failure (grow or entry alloc) -- directory left in a valid,
   usable (if unchanged) state. */
static ObjLockEntry *objlock_resolve_locked(const char *key) {
    if (!g_objlock_dir) return NULL; /* objlock_init() never called or failed */
    if ((uint64_t)(g_objlock_dir_count + 1) * 2 > g_objlock_dir_cap) {
        if (objlock_dir_grow_locked() != 0) return NULL;
    }
    uint32_t idx = obj_str_hash(key) % g_objlock_dir_cap;
    uint32_t start = idx;
    do {
        ObjLockEntry *e = g_objlock_dir[idx];
        if (!e) break;
        if (strcmp(e->name, key) == 0) return e;
        idx = (idx + 1) % g_objlock_dir_cap;
    } while (idx != start);

    if (objlock_should_fail_alloc()) return NULL;
    ObjLockEntry *e = calloc(1, sizeof(*e));
    if (!e) return NULL;
    snprintf(e->name, sizeof(e->name), "%s", key);
    pthread_rwlock_init(&e->rwlock, NULL);
    g_objlock_dir[idx] = e;
    g_objlock_dir_count++;
    return e;
}

/* Lookup only -- never allocates, never grows. Used by the unlock path,
   which must resolve an entry that was already successfully locked
   earlier (and so is guaranteed present, since entries are immortal). */
static ObjLockEntry *objlock_lookup_locked(const char *key) {
    if (!g_objlock_dir || g_objlock_dir_cap == 0) return NULL;
    uint32_t idx = obj_str_hash(key) % g_objlock_dir_cap;
    uint32_t start = idx;
    do {
        ObjLockEntry *e = g_objlock_dir[idx];
        if (!e) return NULL;
        if (strcmp(e->name, key) == 0) return e;
        idx = (idx + 1) % g_objlock_dir_cap;
    } while (idx != start);
    return NULL;
}

void objlock_init(void) {
    if (!g_db && g_shard_db_instance) g_db = g_shard_db_instance;
    if (g_objlock_dir) return; /* idempotent, mirrors bt_cache_init's guard --
                                   tests call objlock_init() directly against
                                   the runner's shared process-local instance */
    g_objlock_dir = calloc(OBJLOCK_INITIAL_CAP, sizeof(*g_objlock_dir));
    if (!g_objlock_dir) {
        fprintf(stderr, "shard-db: objlock_init: out of memory allocating initial directory\n");
        abort();
    }
    g_objlock_dir_cap = OBJLOCK_INITIAL_CAP;
    g_objlock_dir_count = 0;
}

/* Entries are immortal for the life of the process/instance -- freed only
   here, at shutdown. No eviction, no refcounting, no stale-name races. */
void objlock_shutdown(void) {
    pthread_mutex_lock(&g_objlock_table_lock);
    if (g_objlock_dir) {
        for (uint32_t i = 0; i < g_objlock_dir_cap; i++) {
            if (g_objlock_dir[i]) {
                pthread_rwlock_destroy(&g_objlock_dir[i]->rwlock);
                free(g_objlock_dir[i]);
            }
        }
        free(g_objlock_dir);
        g_objlock_dir = NULL;
        g_objlock_dir_cap = 0;
        g_objlock_dir_count = 0;
    }
    pthread_mutex_unlock(&g_objlock_table_lock);
}

int objlock_rdlock(const char *db_root, const char *object) {
    if (!g_db && g_shard_db_instance) g_db = g_shard_db_instance;
    char key[512];
    snprintf(key, sizeof(key), "%s:%s", db_root, object);
    pthread_mutex_lock(&g_objlock_table_lock);
    ObjLockEntry *e = objlock_resolve_locked(key);
    pthread_mutex_unlock(&g_objlock_table_lock);
    if (!e) {
        LOG_ERROR(LOG_SUB_SERVER, "objlock_rdlock: allocation failure resolving '%s'", key);
        return -1;
    }
    pthread_rwlock_rdlock(&e->rwlock);
    return 0;
}

void objlock_rdunlock(const char *db_root, const char *object) {
    char key[512];
    snprintf(key, sizeof(key), "%s:%s", db_root, object);
    pthread_mutex_lock(&g_objlock_table_lock);
    ObjLockEntry *e = objlock_lookup_locked(key);
    pthread_mutex_unlock(&g_objlock_table_lock);
    if (!e) {
        LOG_ERROR(LOG_SUB_SERVER, "objlock_rdunlock: no entry for '%s' (mismatched lock/unlock?)", key);
        return;
    }
    pthread_rwlock_unlock(&e->rwlock);
}

int objlock_wrlock(const char *db_root, const char *object) {
    if (!g_db && g_shard_db_instance) g_db = g_shard_db_instance;
    char key[512];
    snprintf(key, sizeof(key), "%s:%s", db_root, object);
    pthread_mutex_lock(&g_objlock_table_lock);
    ObjLockEntry *e = objlock_resolve_locked(key);
    pthread_mutex_unlock(&g_objlock_table_lock);
    if (!e) {
        LOG_ERROR(LOG_SUB_SERVER, "objlock_wrlock: allocation failure resolving '%s'", key);
        return -1;
    }
    pthread_rwlock_wrlock(&e->rwlock);
    return 0;
}

void objlock_wrunlock(const char *db_root, const char *object) {
    char key[512];
    snprintf(key, sizeof(key), "%s:%s", db_root, object);
    pthread_mutex_lock(&g_objlock_table_lock);
    ObjLockEntry *e = objlock_lookup_locked(key);
    pthread_mutex_unlock(&g_objlock_table_lock);
    if (!e) {
        LOG_ERROR(LOG_SUB_SERVER, "objlock_wrunlock: no entry for '%s' (mismatched lock/unlock?)", key);
        return;
    }
    pthread_rwlock_unlock(&e->rwlock);
}
```

Notes for the executor:
- `obj_str_hash` is the existing hash helper already used by the current
  `get_lock()` — reuse it unchanged.
- The rehash in `objlock_dir_grow_locked` hashes `e->name` (the stored
  `"db_root:object"` key), not a re-derivation from separate fields —
  matches how entries are looked up.
- `errno`/`EOVERFLOW` requires `#include <errno.h>` if not already
  included in `objlock.c`; check before adding.

## Task 5 — `embedded.c`: init/shutdown wiring

`objlock_init()`'s call site (inside the main init sequence, after
`load_allowed_ips_conf`) is unchanged — same call, same position; the
function is now idempotent internally (Task 4) so no caller-side change
is needed there.

Anchor 1, inside `db_cleanup_before_pools` (current):

```c
    bt_cache_shutdown();
    bm_cache_shutdown();
    slotcask_shutdown();
```

Replace with:

```c
    bt_cache_shutdown();
    bm_cache_shutdown();
    objlock_shutdown();
    slotcask_shutdown();
```

Anchor 2, inside `shard_db_close` (current):

```c
    bt_cache_shutdown();
    bm_cache_shutdown();
    slotcask_shutdown();
    schema_caches_shutdown();
```

Replace with:

```c
    bt_cache_shutdown();
    bm_cache_shutdown();
    objlock_shutdown();
    slotcask_shutdown();
    schema_caches_shutdown();
```

Both sites place `objlock_shutdown()` before `db_mutexes_destroy()` (which
destroys `g_objlock_table_lock` itself) — verify at execution time that
neither anchor's surrounding function reorders `db_mutexes_destroy()`
ahead of this point; if it does, treat as an anchor mismatch per Embedded
execution rules (write `PLAN_NOTES.md`, halt).

## Task 6 — call-site fixes (all 12, fail-closed)

Each site: on `-1`, abort the enclosing operation using that function's
existing error idiom, without taking further action on the object. Do
**not** call the corresponding unlock function when the lock call itself
failed (nothing was acquired).

1. `embedded.c`, `shard_db_recover_before_stamp` — this loop already
   checks a `rc` after the wrlock/wrunlock pair and does
   `if (rc != 0) { free(entries); return -1; }`; extend the same
   short-circuit to the lock call itself:
   ```c
   if (objlock_wrlock(eff_root, entries[i].object) != 0) {
       LOG_ERROR(LOG_SUB_RECOVERY, "recover: objlock_wrlock failed for '%s'", entries[i].object);
       free(entries);
       return -1;
   }
   ... existing recovery body ...
   objlock_wrunlock(eff_root, entries[i].object);
   ```

2. `query_maint.c`, `cmd_restore`:
   ```c
   if (objlock_wrlock(db_root, object) != 0) {
       return err_json("objlock_wrlock failed for restore");
   }
   ```
   (use whatever this function's existing error-JSON helper is named —
   confirm exact helper name at execution time via the function's other
   error returns; do not guess a name not present in the file.)

3. `server.c`, `dispatch_nql_query`:
   ```c
   if (objlock_rdlock(db_root, cmd.obj) != 0) {
       /* existing error-reply path for this function, e.g. */
       nql_reply_error(...);
       return;
   }
   ```
   Confirm the function's actual error-reply mechanism at execution time
   (it is `void`-returning; find its existing early-return-on-error
   pattern and mirror it verbatim).

4. `server.c`, `dispatch_json_query`, drop-object branch — mirror the
   branch's existing JSON-error-reply idiom on failure, skip the drop, no
   `objlock_wrunlock` call.

5. `server.c`, `dispatch_json_query`, describe-object branch — same
   pattern as #4.

6. `server.c`, `dispatch_json_query`, generic dispatch path:
   ```c
   int took_wrlock = mode_is_schema(mode);
   int took_rdlock = !took_wrlock;
   if (took_wrlock) {
       if (objlock_wrlock(db_root, object) != 0) { /* error-reply, return */ }
   } else if (took_rdlock) {
       if (objlock_rdlock(db_root, object) != 0) { /* error-reply, return */ }
   }
   ```
   Set `took_wrlock`/`took_rdlock` to 0 on the failing branch before
   returning if any code between the lock call and the function's two
   unlock sites could otherwise be reached — verify at execution time
   whether an early `return` from directly after this block already
   prevents both later unlock sites from executing (if `return` exits the
   whole function, no further guard is needed; if there is any
   `goto`/fallthrough back into the unlock region, the flags must be
   cleared explicitly).

7. `server.c`, `server_process_fast` — this function has an existing
   `goto timing;` idiom for early error returns; use it:
   ```c
   int fast_wr = mode_is_schema(cmd);
   int fast_rd = !fast_wr;
   if (fast_wr) {
       if (objlock_wrlock(eff_root, object) != 0) {
           OUT("Error: lock unavailable\n");
           fast_wr = 0; /* prevent the function's later objlock_wrunlock */
           goto timing;
       }
   } else if (fast_rd) {
       if (objlock_rdlock(eff_root, object) != 0) {
           OUT("Error: lock unavailable\n");
           fast_rd = 0;
           goto timing;
       }
   }
   ```

8. `server.c`, `warmup_kf_task_fn` — background best-effort warmup thread;
   on failure, skip this object's warmup and continue rather than
   crashing the thread:
   ```c
   if (objlock_rdlock(t->eff, t->obj) != 0) {
       LOG_ERROR(LOG_SUB_SERVER, "warmup: objlock_rdlock failed for '%s'", t->obj);
       return NULL; /* or the function's existing early-exit idiom */
   }
   ```

9. `server.c`, `warmup_thread` — same pattern as #8, inside the
   directory-scan loop:
   ```c
   if (objlock_rdlock(dir_path, de->d_name) != 0) {
       LOG_ERROR(LOG_SUB_SERVER, "warmup: objlock_rdlock failed for '%s'", de->d_name);
       continue;
   }
   ```

10. `server.c`, `auto_vacuum_sweep_one`:
    ```c
    if (objlock_wrlock(eff, obj_name) != 0) {
        LOG_ERROR(LOG_SUB_SERVER, "auto-vacuum: objlock_wrlock failed for '%s'", obj_name);
        return;
    }
    ```

11. `server.c`, `auto_reshard_sweep_one` — same pattern as #10.

12. `index.c`, `cmd_reindex`:
    ```c
    if (objlock_wrlock(eff_root, obj) != 0) {
        LOG_ERROR(LOG_SUB_SERVER, "reindex: objlock_wrlock failed for '%s'", obj);
        objects_failed++;
        continue; /* or this loop's existing skip-object idiom */
    }
    ```

Executor note: sites 2-7 reference "existing error-reply/idiom" rather
than fully dictating the patch, because the exact helper names
(`err_json`, `nql_reply_error`, etc.) must be confirmed against the live
file at execution time rather than guessed here — grep each function for
its other early-return-on-error branches and mirror them exactly. If a
site's existing error machinery doesn't obviously fit (e.g. no error-reply
helper exists on that path), stop and ask rather than improvising a new
one.

## Task 7 — OOM-injection unit test

Add to `test_objlock_dynamic_growth.c` (already included as the tail of
Task 1's test body above) or as a second `TEST_REGISTER` in the same file:
confirms `objlock_test_set_fail_alloc` fails exactly the Nth guarded
allocation and that the directory is left in a valid, still-usable state
afterward (a subsequent unrelated key still succeeds):

```c
static int test_objlock_fail_alloc_recovery_run(void) {
    objlock_init();
    objlock_test_set_fail_alloc(1);
    int rc1 = objlock_rdlock("failtest-root", "will-fail");
    ASSERT_EQ_INT(rc1, -1, "injected failure reported");
    objlock_test_set_fail_alloc(0);
    int rc2 = objlock_rdlock("failtest-root", "will-succeed");
    ASSERT_EQ_INT(rc2, 0, "directory still usable after a prior failed resolve");
    objlock_rdunlock("failtest-root", "will-succeed");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-objlock-fail-alloc-recovery", test_objlock_fail_alloc_recovery_run)
```

## Task 8 — docs sync

`docs/concepts/concurrency.md`'s existing "Per-object rwlock ('objlock')"
section describes the old fixed-256-bucket table and `void`-returning
lock calls. Update it in the same diff (not deferred) to describe:
- the pointer-indirected growable directory (initial cap 256, doubles at
  50% load, entries never move or free until process shutdown);
- immortal entries (no eviction) as the reason no stale-name/pin/refcount
  machinery is needed;
- the new `int`-returning `objlock_rdlock`/`objlock_wrlock` contract
  (0 success, -1 allocation failure — caller must abort the operation) and
  that `objlock_rdunlock`/`objlock_wrunlock` remain `void` (lookup-only,
  cannot fail in practice).

Executor: read the current section in full before editing and quote its
existing text as the anchor, rather than guessing its wording from this
plan.

## Acceptance criteria

- All 12 call sites compile against the new `int`-returning signatures;
  no call site ignores the return value.
- `test-objlock-unit` (existing) still passes unmodified.
- `test-objlock-dynamic-growth` and `test-objlock-fail-alloc-recovery`
  (new) pass.
- Full suite green: `SKIP_TESTS=1 ./build.sh` then
  `./build/bin/shard-db-test run-all`.
- ASan+UBSan and TSan gates: 3 consecutive clean `run-all` runs each, per
  Embedded execution rules.
- `docs/concepts/concurrency.md` updated in the same diff.
- No unrelated changes to `db_root_lock_acquire`, `g_dirs`, or any file
  cache.

## Edge cases & invariants

- **Directory growth never touches an existing entry's memory** — only
  the array of pointers is reallocated; any `pthread_rwlock_t` currently
  held by a caller stays at a stable address across a concurrent grow
  triggered by a different thread's insert.
- **Rehash on grow must find every live entry exactly once** — verified
  by Task 1's 2000-key test, which forces 2 grows (1024→2048→4096 as
  `count` crosses each 50%-of-cap threshold — initial cap is 1024, chosen
  so realistic object counts, per the human's estimate, never grow the
  directory in practice) and then proves the 1500th-inserted key's lock
  still round-trips correctly.
- **`objlock_resolve_locked` returning NULL leaves the directory
  unchanged** — a failed grow returns before mutating `g_objlock_dir`; a
  failed entry `calloc` leaves the newly-grown (or ungrown) directory
  fully valid for the next call.
- **Unlock on a key that was never locked** (caller bug, not a new
  scenario introduced by this change) logs and returns; this is
  unchanged behavior from today's `get_lock()`-returns-NULL-on-miss case
  translated to the new lookup path — not a new failure mode.
- **`objlock_init()` is idempotent** — required because
  `test_objlock_unit.c` calls it directly against the runner's shared
  process-local `ShardDb` (per AGENTS.md's reuse-the-runner's-instance
  convention); a second call must not leak or reallocate an
  already-initialized directory. Mirrors `bt_cache_init`'s existing
  re-init guard.
- **Entries created by test code under synthetic `db_root` values (e.g.
  `"growth-root"`) are harmless and require no cleanup** — they are not
  real db objects, just a name string + rwlock; immortality means they
  persist for the rest of the test process, same as `test_objlock_unit.c`
  already does today.
