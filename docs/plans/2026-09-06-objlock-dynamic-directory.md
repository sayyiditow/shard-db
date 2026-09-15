# Plan: pointer-indirected growable objlock directory, immortal entries, fail-closed lock API

## Status

**READY FOR HUMAN APPROVAL.** Not yet executed — no code changed, no tests
run. Anchors below are quoted text, originally verified 2026-09-06 against
branch `perf/request-level-commit-batching` (HEAD `bc1dede`) and
re-verified 2026-09-15 against `main` (HEAD `676d7a5`, post B3a/B3b
merges): all 12 call sites, the struct/macro/prototype anchors, both
shutdown anchors, and the test-harness idioms match. Line numbers are
navigation aids only and will drift — grep the quoted text.

Revised 2026-09-15 after a blind review pass. Fixes folded in: Task 1's
red-run procedure rewritten (the full test cannot compile on base);
deterministic red evidence specified; verified error idioms for sites
1-5 and 7; Task 5's shutdown-ordering halt-check resolved; Task 8's
initial-cap contradiction and premise corrected.

Revised again 2026-09-15 after a second review pass. Fixes folded in:
Task 5 now wires `objlock_shutdown()` into BOTH teardown roots —
`shard_db_destroy_after_storage()` (shared by `shard_db_close` and the
two direct server.c calls at server.c:3728/3831) as well as
`db_cleanup_before_pools` — so the daemon stop path cannot leak the
heap entries (LSan gate); Task 1 registers the new test file in
build.sh's hand-enumerated test-source list (otherwise the new cases
never compile and the acceptance criteria are silently ineffective);
the teardown quiescence contract is specified explicitly (Task 4,
Task 8, edge cases).

Do not execute until the human approves this plan explicitly. This plan is
scoped to exactly three things, per explicit instruction: (a) a
pointer-indirected growable directory replacing the fixed 256-bucket
`g_objlocks` table, (b) immortal entries (no eviction, ever), (c) fixing the
fail-open NULL-return bug so callers cannot proceed unprotected. It does
**not** touch `db_root_lock_acquire`, `g_dirs`/`DIRS_BUCKETS`, or any file
cache (BtCache/BmCache/KfCache/SegCache) — those are separate tables with
separate lifecycles, out of scope.

The request-level commit batching work
(`docs/plans/2026-09-05-request-level-commit-batching.md`) has landed
through B3a/B3b (merged to `main` as of 2026-09-15); the auto-widen-kf
ceiling plan (2026-09-14) is future work and not part of this effort.
This plan branches off `main` on a fresh branch — never a continuation
of the batching branch. If any further batching stage lands before
execution starts, re-run the audit greps below before editing. This
plan touches `src/db/objlock.c`,
`src/db/shard_db_internal.h` (objlock-only region), `src/db/types.h`
(objlock-only prototypes), `src/db/embedded.c` (shutdown wiring only),
`build.sh` (one line in the test-source list), the 12 objlock call
sites enumerated below, `docs/concepts/concurrency.md`, and adds one
new test file registering two cases. It does not touch commit/durability/marker code,
slotcask bulk-window logic, or query_bulk.c beyond reading its
alloc-failure-injection idiom as a pattern to mirror — no expected file
overlap with any further batching stages. If a diff conflict appears
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

## Call-site / consumer audit (verified 2026-09-06, re-verified 2026-09-15 against `main` @ `676d7a5`; executor still re-runs the greps before editing)

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

The full test body below uses the post-fix API (int-returning lock
calls, `objlock_test_set_fail_alloc`), so it **cannot compile against
the pre-fix tree**. The red proof is therefore two-step:

**Step 1 (red, on base).** Write a temporary base-compatible variant of
the test — identical scenario, but statement-form lock calls only (no
`int rc = ...` capture) and with the injected-allocation-failure section
removed. It cannot instantiate the old fixed-256 table from outside
`objlock.c` (no test hook exists to shrink the cap), so temporarily
change `OBJLOCK_BUCKETS` in `shard_db_internal.h` to `4` via a one-line
local edit, rebuild, and run the variant. **Primary red evidence is the
old code's log line** — `objlock get_lock: table full (%d buckets),
object '%s' will run WITHOUT rwlock protection` — which is the fail-open
path firing and is deterministic. Do **not** rely on the
mutual-exclusion assertion as the red detector: on the fail-open build
the wrlock returns immediately instead of blocking, so the releaser
thread may not have run yet when main asserts and the assert can pass
spuriously. Then revert the temporary constant edit. Paste the
failing-run output (showing the log line) and the reverted diff as
evidence accompanying the uncommitted diff for review.

**Step 2 (green, with the fix).** Proceed to Tasks 2-6, then write the
full test below — now compilable against the new API — and confirm it
passes against the real (default-capacity) build. Post-fix, the
mutual-exclusion assertion *is* deterministic (the wrlock can only
return after the reader released, and the releaser recorded its
observation before flipping the release flag), so it carries weight in
the final test even though it cannot carry the red run.

**Build registration (required — a second review finding).** `build.sh`
hand-enumerates the test sources in the `shard-db-test` gcc list (the
comment above it reads "Future test cases under src/test/cases/ get
listed here"); a new case file that is not listed never compiles or
registers, and `run-all` would silently skip it. Anchor (current,
build.sh, `src/test/cases/test_objlock_unit.c \` line inside the
`shard-db-test` link):

```make
    src/test/cases/test_objlock_unit.c \
```

Insert immediately before it:

```make
    src/test/cases/test_objlock_dynamic_growth.c \
```

One source file registers both cases (`test-objlock-dynamic-growth`,
`test-objlock-fail-alloc-recovery` — see Task 7). Verify with
`./build/bin/shard-db-test list` showing both names.

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
       (via the releaser thread) lets go — so by the time wrlock returns,
       the releaser has provably run and observed==1 is deterministic
       post-fix. (On the fail-open pre-fix path this assert is racy and
       may pass spuriously; the red run keys on the table-full log line
       instead — see Task 1 Step 1.) */
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
    if (idx == start) return NULL; /* table full -- unreachable: the 50%
                                      pre-grow above guarantees a free
                                      slot; reaching here means the
                                      invariant broke, fail closed */

    if (objlock_should_fail_alloc()) return NULL;
    ObjLockEntry *e = calloc(1, sizeof(*e));
    if (!e) return NULL;
    snprintf(e->name, sizeof(e->name), "%s", key);
    /* Default-attribute (reader-preferring) rwlock: objlock's API
       permits recursive read locks, which
       PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP does not support
       safely (see docs/plans/2026-07-29-cache-rwlock-writer-preference.md),
       so this stays default-attr even where the four file caches switch
       to writer-preferring. (Carried over verbatim from the pre-rewrite
       get_lock() -- do not drop in the rewrite.) */
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
   here, at teardown. No eviction, no refcounting, no stale-name races.

   Quiescence contract (the same requirement bt_cache_shutdown /
   slotcask_shutdown already carry: they free state an in-flight
   operation may be using too): no thread may still hold an objlock
   when this runs. The daemon stop path satisfies this by joining the
   request-worker pool (server.c:3803), draining in-flight writes, and
   stopping background threads before any teardown; embedded callers
   must ensure every shard_db_query() has returned before calling
   shard_db_close(). */
void objlock_shutdown(void) {
    /* Early-out BEFORE the mutex: dir == NULL means never inited or
       already torn down, and a prior teardown chain may already have
       destroyed the mutex via db_mutexes_destroy(). */
    if (!g_objlock_dir) return;
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

Anchor 2, inside `shard_db_destroy_after_storage` (current):

```c
    free(db->token_set_used);

    db_mutexes_destroy();
```

Replace with:

```c
    free(db->token_set_used);

    objlock_shutdown();

    db_mutexes_destroy();
```

Why both: this codebase has two independent teardown chains, and
`objlock_shutdown()` must sit in both or the daemon leaks every
heap-allocated entry (LSan gate fails). Verified 2026-09-15 on `main`:

- `db_cleanup_before_pools` (embedded.c:203) is the init-failure chain
  (error paths at embedded.c:753, 767, 791) and frees `db` itself — it
  never routes through `shard_db_destroy_after_storage`.
- `shard_db_destroy_after_storage` (embedded.c:886) is the shared final
  teardown with three callers: `shard_db_close` (embedded.c:919), the
  server's startup-failure stop (server.c:3728), and the daemon stop
  path (server.c:3831). Wiring it here covers all three; `shard_db_close`
  itself needs no edit.

Both insertion points run `objlock_shutdown()` before
`db_mutexes_destroy()` destroys `g_objlock_table_lock` — in
`db_cleanup_before_pools` the destroy is a few lines after the anchor
(embedded.c:216); in `shard_db_destroy_after_storage` it is immediately
after. objlock_shutdown's pre-lock early-out (Task 4) keeps a second
call on an already-torn-down chain from touching the destroyed mutex.

## Task 6 — call-site fixes (all 12, fail-closed)

Each site: on `-1`, abort the enclosing operation using that function's
existing error idiom, without taking further action on the object. Do
**not** call the corresponding unlock function when the lock call itself
failed (nothing was acquired).

1. `embedded.c`, `shard_db_recover_before_stamp` — this loop already
   checks a `rc` after the wrlock/wrunlock pair and does
   `if (rc != 0) { free(entries); return -1; }`; extend the same
   short-circuit to the lock call itself (note: `LOG_SUB_RECOVERY` does
   not exist in the tree — the recovery sweep itself logs under
   `LOG_SUB_SLOTCASK`, so use that):
   ```c
   if (objlock_wrlock(eff_root, entries[i].object) != 0) {
       LOG_ERROR(LOG_SUB_SLOTCASK, "recover: objlock_wrlock failed for '%s'", entries[i].object);
       free(entries);
       return -1;
   }
   ... existing recovery body ...
   objlock_wrunlock(eff_root, entries[i].object);
   ```

2. `query_maint.c`, `cmd_restore` (verified 2026-09-15: `int`-returning;
   its existing error idiom is `OUT` an error line + `return 1`, as in
   the "backup not found" branch — there is no `err_json` helper):
   ```c
   if (objlock_wrlock(db_root, object) != 0) {
       OUT("{\"error\":\"objlock_wrlock failed for restore\"}\n");
       return 1;
   }
   ```

3. `server.c`, `dispatch_nql_query` (verified 2026-09-15: `static void`;
   its existing early-return-on-error pattern is the "invalid object
   name" branch near the top of the function — `OUT` an error line,
   `nql_free_command(&cmd)`, `return`):
   ```c
   if (objlock_rdlock(db_root, cmd.obj) != 0) {
       OUT("{\"error\":\"object lock unavailable\"}\n");
       nql_free_command(&cmd);
       return;
   }
   ```

4. `server.c`, `dispatch_json_query`, drop-object branch — mirror the
   branch's existing JSON-error-reply idiom on failure, skip the drop, no
   `objlock_wrunlock` call. The error return must still free the
   branch's owned heap strings — `free(ie_s); free(mode); free(dir);
   free(object);` — exactly as the branch's normal tail does; skipping
   them fails the LSan gate.

5. `server.c`, `dispatch_json_query`, describe-object branch — same
   pattern as #4, freeing `mode`, `dir`, and `object` before the error
   return.

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
   `goto timing;` idiom for early error returns; use it. Verified
   2026-09-15: the unlock block (`if (fast_wr) objlock_wrunlock(...)`)
   sits before the `timing:` label (server.c:2424), so the jump already
   bypasses both the dispatch body and the unlock, and
   `fast_wr`/`fast_rd` are not read after the label — no flag-clearing
   is needed:
   ```c
   int fast_wr = mode_is_schema(cmd);
   int fast_rd = !fast_wr;
   if (fast_wr) {
       if (objlock_wrlock(eff_root, object) != 0) {
           OUT("Error: lock unavailable\n");
           goto timing;
       }
   } else if (fast_rd) {
       if (objlock_rdlock(eff_root, object) != 0) {
           OUT("Error: lock unavailable\n");
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

Executor note: sites 1, 2, 3, and 7 now quote idioms verified against
`main` on 2026-09-15; sites 4-6 still say "existing error-reply idiom"
because those branches' reply text should mirror their neighbors — grep
each function for its other early-return-on-error branches and mirror
them exactly, including their tail `free()` calls (the LSan gate fails
on a missed free). If a site's existing error machinery doesn't
obviously fit (e.g. no error-reply helper exists on that path), stop and
ask rather than improvising a new one.

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

`docs/concepts/concurrency.md`'s "Per-object rwlock ('objlock')" section
describes lock *policy* (which ops take rd vs wr) — it does **not**
document the table's implementation, and no stale fixed-256 or void-API
text exists anywhere in `docs/` or `README.md` (verified 2026-09-15;
`limits.md`'s only objlock mention, the edit-field row, stays true).
The update is therefore additive: append to that section, in the same
diff (not deferred):
- the pointer-indirected growable directory (initial cap **1024**,
  doubles at 50% load, entries never move or free until process
  shutdown);
- immortal entries (no eviction) as the reason no stale-name/pin/refcount
  machinery is needed;
- the new `int`-returning `objlock_rdlock`/`objlock_wrlock` contract
  (0 success, -1 allocation failure — caller must abort the operation) and
  that `objlock_rdunlock`/`objlock_wrunlock` remain `void` (lookup-only,
  cannot fail in practice);
- the teardown contract: entries are heap-allocated and freed only at
  shutdown, under the same no-in-flight-operations quiescence the cache
  shutdowns (`bt_cache_shutdown`/`slotcask_shutdown`) already require —
  the daemon stop path joins the request-worker pool, drains in-flight
  writes, and stops background threads before teardown; embedded
  callers must ensure every `shard_db_query()` has returned before
  `shard_db_close()`.

**Preserve verbatim** the existing paragraph (~concurrency.md:126)
stating objlock deliberately keeps default-attribute (platform-default)
rwlocks because its API permits recursive read locks — the rewrite keeps
NULL-attr init, so it stays true. Likewise keep the add-index/
remove-index rationale paragraph unchanged.

Out of scope for this diff but required at release: a changelog entry
(`docs/reference/changelog.md` "Unreleased" + `docs/release-notes/`) and
the `SHARD_DB_VERSION` bump in `src/db/version.h` — handled by the
release checklist, not this plan.

## Acceptance criteria

- Task 1 red evidence captured and included with the diff for review:
  the shrunken-base run's `table full ... WITHOUT rwlock protection` log
  line, plus the reverted `OBJLOCK_BUCKETS` constant diff.
- All 12 call sites compile against the new `int`-returning signatures;
  no call site ignores the return value.
- `test-objlock-unit` (existing) still passes unmodified.
- `test-objlock-dynamic-growth` and `test-objlock-fail-alloc-recovery`
  (new) pass, and both names appear in `./build/bin/shard-db-test list`
  (build.sh's test-source list includes the new file — a missing
  registration would make the two criteria above silently vacuous).
- Full suite green: `SKIP_TESTS=1 ./build.sh` then
  `./build/bin/shard-db-test run-all`.
- ASan+UBSan and TSan gates: 3 consecutive clean `run-all` runs each, per
  Embedded execution rules.
- `docs/concepts/concurrency.md` updated in the same diff (additive;
  default-attribute-rwlock paragraph preserved verbatim).
- No unrelated changes to `db_root_lock_acquire`, `g_dirs`, or any file
  cache.
- Human follow-up before release (not an executor task; this repo's rule
  is that the user runs benches): a bench spot-check (`bench-kv` /
  `bench-kv-parallel`). The rewrite trades the old lock-free fast-path
  probe for a short global-mutex hold on every request's lock/unlock —
  sound (the hold is tiny vs. the rwlock op that follows), but it lands
  immediately after the B3a/B3b perf work, so confirm no throughput
  regression before cutting the release.

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
- **Every teardown chain frees the directory and entries** —
  `objlock_shutdown()` lives in both teardown roots:
  `db_cleanup_before_pools` (the init-failure chain) and
  `shard_db_destroy_after_storage` (shared by `shard_db_close`, the
  server's startup-failure stop at server.c:3728, and the daemon stop
  path at server.c:3831). Missing either root leaks every entry and
  fails the LSan gate. Entry destruction requires the quiescence
  contract documented on `objlock_shutdown` (Task 4): the daemon path
  joins request workers before teardown; embedded callers must have no
  in-flight `shard_db_query()` when they call `shard_db_close()`. An
  in-flight query holding an objlock at close is the same class of
  misuse as holding a kfcache/btcache entry across close — not a new
  requirement this change introduces, but now stated explicitly.
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

## Execution addendum — second review round (2026-09-15)

Findings from the post-execution review, all fixed in the same
uncommitted diff:

1. **Legacy fast-path restore self-deadlock (pre-existing, High).**
   `server_process_fast` classified `restore` as a plain read
   (`restore` is absent from `mode_is_schema`) and rdlock'd it, then
   called `cmd_restore`, which takes the object's wrlock internally — a
   read-to-write upgrade on the same rwlock that blocks indefinitely.
   The JSON restore branch never had this problem (it dispatches before
   the generic take and takes no outer lock). Fixed by excluding
   `restore` from the fast path's lock classification: restore
   self-locks inside `cmd_restore`, identically to the JSON branch.
   Note for the future: `cmd_reindex` also self-locks, but neither
   dispatch path pre-locks it (the JSON `reindex` mode returns before
   the generic take; the fast path does not dispatch reindex).

2. **Regression test hardened (High).** The original mutual-exclusion
   assert could pass spuriously against a fail-open implementation (the
   releaser thread raced the writer's return). Replaced with a
   deterministic design: while the reader holds the rdlock, a writer
   thread must NOT acquire for 500ms (fail-open enters within
   microseconds — no pass-direction timing assumption), then MUST
   acquire after the reader releases (unbounded wait). The reader
   worker's lock return value is now captured via an atomic and asserted
   on main (workers cannot use t_ctx); on lock failure the worker
   returns immediately without the matching unlock, so an unexpected
   allocation failure cannot produce an unmatched unlock.

3. **Red evidence made durable.** The prior evidence lived in /tmp and
   was lost to a host reboot. Re-captured in an isolated worktree at
   the base commit (3996 fail-open log lines, all four probe calls on
   `obj-1500` unprotected) and recorded in
   [2026-09-06-objlock-dynamic-directory.red-evidence.md](2026-09-06-objlock-dynamic-directory.red-evidence.md).

4. **Judgement calls.** `objlock_test_set_fail_alloc` moved from the
   installed `types.h` to `shard_db_internal.h`, where every other test
   knob lives. The duplicated acquire/release bodies in the four public
   objlock functions are factored into `objlock_entry_acquire` /
   `objlock_entry_release`. `objlock_init`'s abort-on-OOM is documented
   as deliberate: init-time OOM is an instance-creation failure (the
   instance cannot serve without its lock directory), distinct from the
   runtime fail-closed contract — peer init paths (e.g. kfcache_init)
   don't check their calloc at all.

Gate status after these fixes: default build + full `run-all` green
(re-run below); the ASan/UBSan and TSan gates are to be run by the
human reviewer on this final diff (TSan previously passed 3× at
`--jobs 2` with `SHARD_TEST_WATCHDOG_SEC=600` on the pre-fix diff;
full parallelism crashed the host).
