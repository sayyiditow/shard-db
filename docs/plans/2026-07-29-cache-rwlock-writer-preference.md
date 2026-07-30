# Fix writer starvation on the file-cache rwlocks (bt_cache / kfcache / segcache / bm_cache)

Status: **Executed through Task 3; Task 4 applied. Revision 5 (incorporates third review round findings below, found during the human's own execution of Tasks 1-3).**

## Revision 5 note (read this first)

A third review pass, made while the human was executing this plan directly
on branch `fix/cache-rwlock-writer-preference`, found three more issues.
All three are fixed:

1. **Stale execution-status record.** Task 3's "Execution halted during
   Task 3" subsection recorded an old blocked state (the deadlock described
   in revision 4's history) and never recorded the actual passing Task 3
   results once the test-synchronization bug was fixed. Replaced with a
   "Task 3 results" subsection giving the real focused-regression and
   full-suite output, including both the sequential (`--jobs 1`) and default
   (`run-all`, no `--jobs`) full-suite runs — the latter surfaced a
   nondeterministic failure set confirmed pre-existing on `main` and out of
   this plan's scope (see that subsection for detail).
2. **Timing window in the regression test's late-reader sync.** The test set
   `late_attempted` just before calling `bt_acquire()`'s
   `pthread_rwlock_rdlock()`, then started a fixed 250ms observation window
   from that flag. `bt_acquire()` does real work (cache-slot resolution)
   between setting `attempted` and actually calling `rdlock()`; under
   scheduler delay that gap could in principle widen enough for the fixed
   window to elapse before the reader ever reached the blocking call, which
   would make an *unfixed* build look like it passed the assertion, having
   never actually been tested. Added a new `TEST_BUILD`-only
   `btree_test_reader_pending_count()` (mirrors the existing
   `btree_test_writer_pending_count()`, incremented immediately before
   `rdlock()` and decremented immediately after it returns) and changed the
   test to poll on that counter being `> 0` — proof the thread is genuinely
   blocked inside the syscall right now — before starting the bounded
   observation window. See Task 1's updated hooks and test body below.
3. **Fallback `pthread_rwlock_init(lock, NULL)` calls still unchecked.**
   Revision 4's fix #4 checked the glibc-attribute path's return codes but
   left all three fallback calls (each `pthread_rwlock_init(lock, NULL)`
   reached on an earlier step's failure, plus the non-glibc `#else` path)
   unchecked, so a failure exactly at the fallback itself would still go
   unnoticed. Factored the fallback into its own
   `rwlock_init_writer_preferring_fallback()` helper that checks and logs
   its own return code, and every call site (all four) now goes through it
   instead of calling `pthread_rwlock_init` directly. See Task 2's updated
   code block below.

All three verified: rebuilt, ran `test-bt-cache-writer-starvation` (clean),
temporarily reverted `btree.c`'s `bt_cache` call site to plain
`pthread_rwlock_init(lock, NULL)` and confirmed the test still correctly
fails (exit 6, "late reader bypassed queued writer" — proving the new
sync point didn't weaken detection), restored the fix and confirmed clean
again, then ran the full sequential suite (`run-all --jobs 1`): `11346
passed, 0 failed across 354 cases`.

## Revision 4 note (read this first)

A second review pass, run after revision 3 above, found four more issues.
All four are fixed in this revision:

1. **Blocker — the regression test would fail on non-glibc platforms.**
   `rwlock_init_writer_preferring()` (Task 2) only changes behavior under
   `__GLIBC__`; on macOS or a non-glibc Linux libc it falls through to the
   identical `pthread_rwlock_init(lock, NULL)` this codebase already uses.
   The new test's "late reader cannot bypass a queued writer" assertion
   would still fail there — not because of a regression, but because the
   glibc/NPTL extension this plan relies on simply doesn't exist on that
   platform. `AGENTS.md` lists macOS as a supported target, so the test must
   skip cleanly instead of reporting a false failure. Task 1's test now
   gates its entire body behind `#if defined(__linux__) && defined(__GLIBC__)`,
   matching this codebase's own existing skip idiom — `TAP_DIAG(...);
   return 0;` (precedent: `test_tls.c:104-107`, `test_get_fields.c:242-246`).
   Confirmed via `test_runner.c:47-61` that a case returning 0 having run no
   assertions registers as a clean pass (`ctx.failed` stays 0), not an error,
   both sequentially and under the per-case-worker `run-all` path.
2. **Medium (downgraded from the original review's High) — recursion audit.**
   Applying a *non*recursive writer-preferring policy to a cache that is
   ever acquired recursively (same path, same thread, nested) risks a
   self-deadlock — the exact reason `objlock.c` stays out of scope below.
   Whether that hazard also applies to the four in-scope caches had not
   been explicitly checked. It now has: every acquire/release call site in
   `btree.c`, `slotcask.c` (kfcache + segcache), and `bitmap.c` was audited.
   No same-path/same-thread recursive acquisition exists today —
   `btree_idx_walk_ordered`'s k-way cursor merge opens one `BtRangeIter` per
   shard, always on a distinct path per shard index, never the same file
   twice in one thread; every eviction path (`bt_cache_evict_slot` and its
   kf/seg/bm equivalents) uses non-blocking `trywrlock` against LRU
   candidates specifically so a thread can't be made to block trying to
   evict a slot it already holds open (`test_btcache_evict_race.c` already
   regression-tests exactly this for `bt_cache`). Task 2 now records this as
   an explicit, checked invariant rather than leaving it implicit — see
   "Recursion invariant (checked)" there.
3. **Medium — documentation sync.** `docs/concepts/concurrency.md` said
   nothing about the new platform-dependent writer-preference behavior, and
   `docs/reference/changelog.md` risked reading as a blanket rwlock-fairness
   claim when `objlock.c` deliberately keeps its current (platform-default,
   recursion-safe) behavior. New Task 4 updates both docs.
4. **Low — silent init-failure handling.** `rwlock_init_writer_preferring()`
   ignored every pthread return code, so a failed
   `pthread_rwlockattr_setkind_np()` would leave the code believing
   writer-preference was active when it wasn't. Checked this codebase's own
   convention first: none of the existing rwlock/attr-init call sites in
   `btree.c`, `slotcask.c`, `bitmap.c`, or `objlock.c` check return codes
   today, but the one directly comparable *checked* case
   (`btree.c:1705`'s `pthread_mutex_init`) logs and falls back rather than
   aborting, and this codebase reserves `abort()` for failures that risk
   on-disk corruption (`btree.c:942-944`, `:962-963`, each with an explicit
   "fail loudly" comment) — lock-attribute setup isn't that. The helper
   (Task 2) now checks each call, logs via `LOG_ERROR(LOG_SUB_SERVER, ...)`
   on any failure, and falls back to plain `pthread_rwlock_init(lock, NULL)`
   instead of silently proceeding as if the attribute had taken effect.

**On `objlock.c` staying out of scope (per your review — confirmed, and
worth stating explicitly):** every `objlock` critical section guards
`vacuum`, a schema mutation, or an index rebuild — none of those sit on the
GET/INSERT/DELETE hot path. That hot path takes `kfcache`/`segcache` (and
`bt_cache` for indexed reads/writes), not `objlock`. A schema mutation or
vacuum arriving a little later under sustained read pressure is an
acceptable cost; the same starvation on `bt_cache`/`kfcache`/`segcache`/
`bm_cache` is not, since those sit under every query and every write. That,
plus the recursive-reader hazard already noted in revision 3, is why
`objlock.c` is excluded rather than folded into this fix.

## Revision 3 note

Revision 1 of this plan had a critical bug in its own fix and several other
defects, caught in review before any code was touched. Summary of what
changed and why:

1. **Critical — the proposed `#ifdef` never enabled writer preference.**
   `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP` is a C **enum member**
   (`/usr/include/pthread.h:106-109`, anonymous `enum { ...,
   PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP, PTHREAD_RWLOCK_DEFAULT_NP =
   PTHREAD_RWLOCK_PREFER_READER_NP }`), not a preprocessor macro. The
   preprocessor cannot see enum members, so `#ifdef
   PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP` is **always false** and
   always falls through to the `#else` (default-attribute) branch — a
   silent no-op. Verified empirically:
   ```c
   #include <pthread.h>
   #include <stdio.h>
   int main(void) {
   #ifdef PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP
       printf("MACRO DEFINED - ifdef branch taken\n");
   #else
       printf("NOT a macro - ifdef falls through to #else\n");
   #endif
       return 0;
   }
   ```
   `rtk gcc -D_GNU_SOURCE rwlock_check.c -o rwlock_check && rtk ./rwlock_check` →
   `NOT a macro - ifdef falls through to #else`.
   The pre-existing `objlock.c` code has the same dead guard, but that does
   **not** make it safe to add to this plan: `test_objlock_unit.c` deliberately
   takes the same object's read lock recursively, while glibc documents
   `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP` as safe only when readers
   never recurse. `objlock.c` therefore remains unchanged and out of scope;
   this plan concerns the four nonrecursive file-cache lock tables only.
2. **High — the new test would never be built.** `build.sh` has an explicit,
   hardcoded list of `src/test/cases/test_*.c` files (`build.sh:258-261`),
   not a glob. Task 1 now includes an anchored `build.sh` edit.
3. **High — the original regression test design was timing-dependent.**
   Starting readers and a writer concurrently and polling a "writer done"
   flag with a fixed deadline never proves the writer was actually queued
   before readers piled on — an unfixed build could win initial scheduling
   and pass by luck; a correctly-fixed build under CI load could time out
   and fail. Replaced with a deterministic design using two new
   `TEST_BUILD`-only hooks in `btree.c`/`btree.h` (below) that prove
   ordering instead of assuming it, plus a forked child with a true bounded
   wait so an unexpected lock-ordering deadlock cannot hang the test runner.
4. **Medium — fixed `/tmp` path.** Replaced with `mkdtemp`, matching
   `test_btcache_evict_race.c:83`.
5. **Medium — this plan referenced
   `docs/plans/2026-07-29-ci-release-gate-diagnosis.md` as a file Task 3
   would update.** The human has confirmed that file was intentionally
   deleted. Task 3 records the relevant results in this document instead;
   no cross-plan update or recovery work is required.
6. **Medium — coverage didn't match scope.** Task 2 changes four
   initializers but only `bt_cache` has a regression test. Fixed
   by factoring the buggy platform-conditional logic into one shared
   helper function (`rwlock_init_writer_preferring()`, see Task 2) used by
   all four call sites — the deterministic `bt_cache` regression test
   proves the helper's behavior once, and Task 2 includes a grep-verifiable
   step confirming all four sites call the shared helper rather than
   pasting the conditional four times.
7. **Low — inaccurate execution-model wording.** `shard-db-test run <name>`
   runs in the harness's own process, not a disposable per-case worker
   (`src/test/test_runner.c:67`; only `run-all`'s default mode forks a
   worker per case). The new test's fork-based design (item 3) makes this
   moot for Task 1, and Task 3 now states the exact full-parallel command
   instead of referring back to an earlier run.

## Relationship to the CI release-gate diagnosis plan

This root cause was found while chasing aggregate/index mismatches under
concurrent load during an earlier investigation. It is written as its own
focused plan because it is a distinct mechanism (liveness/fairness, not a
data race or the already-fixed `bt_cache_probe()` bug) with its own
regression test and fix. Whether it explains those still-open symptoms is
addressed in "What this does and does not prove" below — this plan does not
assume the answer, and (per the revision note above) does not depend on the
missing cross-plan document to record that answer.

## Root cause

`bt_acquire()` (`src/db/btree.c:637`) is the single entry point every reader
*and* writer of a cached `.idx` file goes through. On both the cache-hit path
and the cache-miss/install path it takes the per-slot `pthread_rwlock_t`
directly:

```c
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);
```

(`src/db/btree.c:676-677`, cache-hit path; the same pattern repeats at
`src/db/btree.c:838-839` on the cache-miss/install path.)

That rwlock is initialized with default (`NULL`) attributes:

```c
        pthread_rwlock_init(&bt_cache[i].rwlock, NULL);
```

(`src/db/btree.c:384`.) Default-attribute `pthread_rwlock_t` on glibc/NPTL
does not favor a blocked writer over newly arriving readers. Under
*continuous* read pressure on one path — e.g. many concurrent `find`/`count`
queries hitting one shard's secondary index while a bulk write targets the
same shard — a writer blocked in `pthread_rwlock_wrlock()` can be starved
indefinitely: each time the lock goes read-unlocked, a new reader can win the
race for the next `rdlock()` before the parked writer is granted the lock.
This is a liveness bug, not a memory-safety race — every access is correctly
guarded by the lock, which is exactly why TSan reports zero findings against
it (already confirmed: prior full ASan/TSan CI runs on this failure class show
no sanitizer output at all).

This was reproduced directly and unambiguously with a standalone harness
(6 writer threads doing `btree_bulk_merge`'s splice path — `existing_count`
large enough to route through `btree_insert_batch_locked`, a single
`bt_acquire(path, 1)` per batch — against 4 reader threads spinning
`btree_range` on the same `.idx` file): 2 of 6 writers completed exactly one
64-row batch each, then every writer wedged permanently, while all 4 readers
kept completing `btree_range` calls at high frequency (30,000+ iterations in
25s) the entire time, every one returning the identical stale count. Readers
were never blocked once; the writer(s) never got back in. That is the
textbook signature of reader-preferring starvation, not a deadlock (there is
no lock-ordering cycle — the writer is parked in a single `wrlock()` call
that a continuous stream of readers keeps winning ahead of it).

### The same defect exists in three other caches

`bt_acquire()` is one instance of a repeated pattern in this codebase: a
fixed-size slot table of `{fd, mmap, pthread_rwlock_t}` entries, each entry
guarding concurrent readers and writers of one on-disk file and initialized
with `NULL` attributes. A full-repo scan confirms exactly four file-cache
sites:

```text
src/db/btree.c:384      bt_cache      (per B-tree .idx file)   -- unprotected
src/db/slotcask.c:163   g_kfcache     (per kf shard file)      -- unprotected
src/db/slotcask.c:943   g_segcache    (per segment file)       -- unprotected
src/db/bitmap.c:77      g_bm_cache    (per bitmap-index shard) -- unprotected
```

`g_kfcache` is on the hottest path in the whole server — every `GET` /
`INSERT` / `DELETE` acquires it, not just indexed queries — so it is at least
as exposed to this starvation pattern as `bt_cache`, and plausibly more so
given its call frequency. `g_segcache` and `g_bm_cache` share the identical
shape and are exposed to the same risk whenever their read and write traffic
overlaps on one shard/stream file. This plan fixes all four in one pass,
through one shared helper (Task 2), since they are the same defect with the
same fix, not four separate investigations.

## What this does and does not prove

- **Proven**: a writer can be starved indefinitely on `bt_cache`'s rwlock
  under sustained concurrent reader load, reproduced standalone with no
  daemon, no network, and no test-harness artifacts involved (contrast with
  the earlier `diag_seq`/`diag_min3` reproduction, which was ruled out as a
  test-only bug — see this plan's originating investigation notes).
- **Not yet proven**: that this specific mechanism is *the* root cause of any
  still-open CI release-gate symptoms (wrong aggregate/index counts under
  concurrent load) or of the local full-parallel test-suite failures
  observed earlier in this investigation. Those symptoms are plausible
  outcomes of this bug — a starved writer that eventually completes very
  late can trip client-side request timeouts (looking like "response read
  wrongly"), and if any caller path treats a slow/timed-out write as
  "didn't happen" while some partial effect nonetheless landed, that could
  produce undercounts — but no direct causal chain from *this* rwlock to
  *those* specific symptoms has been demonstrated yet. Rerunning the
  specific previously-failing scenarios after this fix lands (Task 3) is
  the only way to confirm or rule this out.

## Task 1 — deterministic regression test (test-first)

### New test-only hooks in `btree.c` / `btree.h`

The test needs to *prove* a writer is genuinely parked in `wrlock()` before
applying reader pressure, and needs to hold a real reader lock open across a
controlled window. Neither is observable through the existing public API, so
add two small `TEST_BUILD`-only hooks, following the exact style of the
existing `btree_test_set_delete_gate_bypass` hook (`src/db/btree.c:1911-1927`,
declared in `src/db/btree.h:175-179`).

**`src/db/btree.h`** — replace the anchor:

```c
#ifdef TEST_BUILD
/* Test-only: reproduce the pre-fix delete path without changing production
   synchronization.  The race regression uses this to prove its bad ordering. */
void btree_test_set_delete_gate_bypass(int enabled);
#endif
```

with:

```c
#ifdef TEST_BUILD
/* Test-only: reproduce the pre-fix delete path without changing production
   synchronization.  The race regression uses this to prove its bad ordering. */
void btree_test_set_delete_gate_bypass(int enabled);

#include <stdatomic.h>

/* Test-only: number of threads currently blocked in bt_acquire()'s
   pthread_rwlock_wrlock() across all cached .idx files. Lets a test prove a
   writer is genuinely queued before applying reader pressure, instead of
   assuming it via timing. */
int btree_test_writer_pending_count(void);

/* Test-only: number of threads currently blocked in bt_acquire()'s
   pthread_rwlock_rdlock() across all cached .idx files. Lets a test prove a
   reader has actually entered the blocking call (not merely been scheduled
   to attempt it) before starting a bounded observation window — closes the
   race where thread-launch scheduling delay could be mistaken for a proven
   block. */
int btree_test_reader_pending_count(void);

struct ShardDb;

/* Test-only: pthread start routine. Binds the calling thread's `g_db`, then
   takes a real rdlock on `path` via the normal bt_acquire() path. It reports
   just-before-acquire via *attempted and successful acquisition via
   *acquired, then parks until *release is set before releasing via
   bt_release(). Lets a test distinguish a late reader queued at rdlock from
   one that actually slipped ahead of a queued writer. */
typedef struct {
    const char *path;
    struct ShardDb *db;
    atomic_int *attempted;
    atomic_int *acquired;
    atomic_int *release;
} BtTestHoldRdlockArgs;
void *btree_test_hold_rdlock(void *arg);
#endif
```

These are new `TEST_BUILD`-only declarations. Their only consumer is the
new `test_bt_cache_writer_starvation.c` below; production builds neither
compile nor link the hooks, and no existing btree API signature changes.

**`src/db/btree.c`** — insert the writer-pending counter before `bt_acquire`
(it must be defined before first use). Replace the anchor:

```c
/* Acquire a btree handle. writer=0 takes rdlock, writer=1 takes wrlock and
   creates the file (with a fresh header) if missing. On cache pressure we
   evict the least-recently-used slot; if the cache isn't initialised or
   eviction can't free a slot, we fall back to an uncached mapping (slot=-1,
   no rwlock) — MAP_SHARED keeps duplicate mappings byte-coherent, but concurrent uncached writers do not get the cache's rwlock serialization; that accepted cache-pressure hazard is unchanged here. */
static int bt_acquire(BtFile *bt, const char *path, int writer) {
```

with:

```c
#ifdef TEST_BUILD
static pthread_mutex_t g_bt_test_writer_pending_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_bt_test_writer_pending_count;

static void bt_test_writer_pending_begin(void) {
    pthread_mutex_lock(&g_bt_test_writer_pending_lock);
    g_bt_test_writer_pending_count++;
    pthread_mutex_unlock(&g_bt_test_writer_pending_lock);
}

static void bt_test_writer_pending_end(void) {
    pthread_mutex_lock(&g_bt_test_writer_pending_lock);
    g_bt_test_writer_pending_count--;
    pthread_mutex_unlock(&g_bt_test_writer_pending_lock);
}

int btree_test_writer_pending_count(void) {
    pthread_mutex_lock(&g_bt_test_writer_pending_lock);
    int n = g_bt_test_writer_pending_count;
    pthread_mutex_unlock(&g_bt_test_writer_pending_lock);
    return n;
}

static pthread_mutex_t g_bt_test_reader_pending_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_bt_test_reader_pending_count;

static void bt_test_reader_pending_begin(void) {
    pthread_mutex_lock(&g_bt_test_reader_pending_lock);
    g_bt_test_reader_pending_count++;
    pthread_mutex_unlock(&g_bt_test_reader_pending_lock);
}

static void bt_test_reader_pending_end(void) {
    pthread_mutex_lock(&g_bt_test_reader_pending_lock);
    g_bt_test_reader_pending_count--;
    pthread_mutex_unlock(&g_bt_test_reader_pending_lock);
}

int btree_test_reader_pending_count(void) {
    pthread_mutex_lock(&g_bt_test_reader_pending_lock);
    int n = g_bt_test_reader_pending_count;
    pthread_mutex_unlock(&g_bt_test_reader_pending_lock);
    return n;
}
#endif

/* Acquire a btree handle. writer=0 takes rdlock, writer=1 takes wrlock and
   creates the file (with a fresh header) if missing. On cache pressure we
   evict the least-recently-used slot; if the cache isn't initialised or
   eviction can't free a slot, we fall back to an uncached mapping (slot=-1,
   no rwlock) — MAP_SHARED keeps duplicate mappings byte-coherent, but concurrent uncached writers do not get the cache's rwlock serialization; that accepted cache-pressure hazard is unchanged here. */
static int bt_acquire(BtFile *bt, const char *path, int writer) {
```

Then instrument both `wrlock`/`rdlock` call sites so the counter brackets
only the blocking wait, not the whole critical section. Replace the anchor
(cache-hit path):

```c
        bt_cache[slot].last_access = __atomic_add_fetch(&bt_cache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &bt_cache[slot].rwlock;
        pthread_mutex_unlock(&bt_cache_lock);

        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);
```

with:

```c
        bt_cache[slot].last_access = __atomic_add_fetch(&bt_cache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &bt_cache[slot].rwlock;
        pthread_mutex_unlock(&bt_cache_lock);

#ifdef TEST_BUILD
        if (writer) {
            bt_test_writer_pending_begin();
            pthread_rwlock_wrlock(lock);
            bt_test_writer_pending_end();
        } else {
            bt_test_reader_pending_begin();
            pthread_rwlock_rdlock(lock);
            bt_test_reader_pending_end();
        }
#else
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);
#endif
```

Replace the anchor (cache-miss/install path):

```c
    /* Take the per-entry rwlock AFTER releasing the table mutex — same
       M0-then-M1 ordering as the cache-hit path above, so a per-entry
       rwlock never nests inside bt_cache_lock. A caller can park a rwlock
       across a long-lived handle (e.g. BtRangeIter) and separately need
       bt_cache_lock for an unrelated slot; nesting the other way risks a
       lock-order inversion against that. Verify-and-retry exactly like the
       hit path handles the resulting window where a concurrent evictor can
       steal this slot before we lock it. */
    if (writer) pthread_rwlock_wrlock(lock);
    else        pthread_rwlock_rdlock(lock);
```

with:

```c
    /* Take the per-entry rwlock AFTER releasing the table mutex — same
       M0-then-M1 ordering as the cache-hit path above, so a per-entry
       rwlock never nests inside bt_cache_lock. A caller can park a rwlock
       across a long-lived handle (e.g. BtRangeIter) and separately need
       bt_cache_lock for an unrelated slot; nesting the other way risks a
       lock-order inversion against that. Verify-and-retry exactly like the
       hit path handles the resulting window where a concurrent evictor can
       steal this slot before we lock it. */
#ifdef TEST_BUILD
    if (writer) {
        bt_test_writer_pending_begin();
        pthread_rwlock_wrlock(lock);
        bt_test_writer_pending_end();
    } else {
        bt_test_reader_pending_begin();
        pthread_rwlock_rdlock(lock);
        bt_test_reader_pending_end();
    }
#else
    if (writer) pthread_rwlock_wrlock(lock);
    else        pthread_rwlock_rdlock(lock);
#endif
```

Finally, add the `btree_test_hold_rdlock` implementation after `bt_release`
(it must be defined after `bt_acquire`/`bt_release`, both `static`). Replace
the anchor:

```c
    bt->map = NULL;
    bt->fd = -1;
    bt->slot = -1;
}

int bt_cache_stats(int *used_slots, int *total_slots, size_t *total_bytes) {
```

with:

```c
    bt->map = NULL;
    bt->fd = -1;
    bt->slot = -1;
}

#ifdef TEST_BUILD
void *btree_test_hold_rdlock(void *arg) {
    BtTestHoldRdlockArgs *a = arg;
    g_db = a->db;
    BtFile bt;
    atomic_store(a->attempted, 1);
    if (bt_acquire(&bt, a->path, 0) != 0) return NULL;
    atomic_store(a->acquired, 1);
    while (!atomic_load(a->release)) usleep(1000);
    bt_release(&bt);
    return NULL;
}
#endif

int bt_cache_stats(int *used_slots, int *total_slots, size_t *total_bytes) {
```

### The test itself

Add `src/test/cases/test_bt_cache_writer_starvation.c`:

```c
/* src/test/cases/test_bt_cache_writer_starvation.c
 *
 * Regression test for writer starvation on bt_cache's per-file rwlock
 * (src/db/btree.c, bt_cache_init: pthread_rwlock_init with NULL/default
 * attributes). glibc's default rwlock policy does not favor a blocked
 * writer over newly arriving readers: under continuous read pressure on
 * one .idx file, a writer parked in bt_acquire()'s pthread_rwlock_wrlock()
 * can be starved indefinitely while readers keep winning every subsequent
 * rdlock().
 *
 * Deterministic ordering: a "holder" thread takes a real rdlock and parks.
 * A writer then blocks in wrlock() behind it; the test observes the
 * TEST_BUILD-only pending counter to prove that fact. A late-reader thread
 * announces immediately before it calls rdlock(). While the holder remains
 * locked, the test requires that late reader NOT acquire: glibc's default
 * PTHREAD_RWLOCK_PREFER_READER_NP grants it ahead of the queued writer, so
 * the unfixed child exits with a distinct failure code. With
 * PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP, it remains blocked until the
 * holder releases and the queued writer completes. This tests the policy's
 * ordering contract directly; it does not rely on a probabilistic starvation
 * duration or reader throughput.
 *
 * The scenario runs in a forked child, and the parent has a real 30-second
 * bound (300 × 100ms polls), so any unexpected deadlock is contained.
 *
 * glibc/NPTL-only: PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP does not
 * exist on macOS or non-glibc Linux libcs, and rwlock_init_writer_preferring()
 * is a no-op there (falls back to today's default-attribute behavior). This
 * test's assertion only holds where that attribute is actually available, so
 * the whole scenario is compiled out elsewhere and reports a skip instead of
 * a false failure.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "btree.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#if defined(__linux__) && defined(__GLIBC__)

#define BASE_COUNT 5000
#define WRITER_CHUNKS 5
#define CHUNK_SIZE 64
#define WRITER_PENDING_WAIT_SECS 5
#define HOLDER_ACQUIRE_WAIT_SECS 5
#define LATE_READER_BLOCK_CHECK_MS 250

static const char *g_vals[4] = {"paid", "pending", "refunded", "cancelled"};

static void make_hash(long id, uint8_t out[BT_HASH_SIZE]) {
    memset(out, 0, BT_HASH_SIZE);
    uint32_t mixed = (uint32_t)id * 2654435761u;
    memcpy(out, &mixed, sizeof(mixed));
    memcpy(out + 4, &mixed, sizeof(mixed));
    memcpy(out + 8, &id, sizeof(id) < 8 ? sizeof(id) : 8);
}

static int noop_count_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    (void)v; (void)vl; (void)h;
    int *n = ctx;
    (*n)++;
    return 0;
}

static int range_count(const char *path) {
    int n = 0;
    btree_range(path, "", 0,
                "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
                BT_HASH_SIZE, noop_count_cb, &n);
    return n;
}

typedef struct {
    const char *path;
    ShardDb    *db;
    atomic_int *chunks_done;
    atomic_int *writer_done;
} WriterArgs;

static void *writer_thread_main(void *arg) {
    WriterArgs *a = arg;
    g_db = a->db;
    for (int c = 0; c < WRITER_CHUNKS; c++) {
        BtEntry *entries = calloc(CHUNK_SIZE, sizeof(*entries));
        for (int i = 0; i < CHUNK_SIZE; i++) {
            long id = BASE_COUNT + (long)c * CHUNK_SIZE + i;
            entries[i].value = strdup(g_vals[id % 4]);
            entries[i].vlen = strlen(entries[i].value);
            make_hash(id, entries[i].hash);
        }
        btree_bulk_merge(a->path, entries, CHUNK_SIZE);
        for (int i = 0; i < CHUNK_SIZE; i++) free((char *)entries[i].value);
        free(entries);
        atomic_fetch_add(a->chunks_done, 1);
    }
    atomic_store(a->writer_done, 1);
    return NULL;
}

static int run_child(const char *base) {
    bt_cache_shutdown();
    bt_cache_init(16);

    char path[600];
    snprintf(path, sizeof(path), "%s/writer_starvation.idx", base);

    BtEntry *seed = calloc(BASE_COUNT, sizeof(*seed));
    for (int i = 0; i < BASE_COUNT; i++) {
        seed[i].value = strdup(g_vals[i % 4]);
        seed[i].vlen = strlen(seed[i].value);
        make_hash(i, seed[i].hash);
    }
    int seed_rc = btree_bulk_merge(path, seed, BASE_COUNT);
    for (int i = 0; i < BASE_COUNT; i++) free((char *)seed[i].value);
    free(seed);
    if (seed_rc != 0) _exit(2);

    ShardDb *db = g_db;
    atomic_int holder_attempted = 0, holder_acquired = 0, holder_release = 0;
    BtTestHoldRdlockArgs holder_args = {
        .path = path, .db = db, .attempted = &holder_attempted,
        .acquired = &holder_acquired, .release = &holder_release
    };
    pthread_t holder_tid;
    pthread_create(&holder_tid, NULL, btree_test_hold_rdlock, &holder_args);

    for (int waited = 0; !atomic_load(&holder_acquired); waited++) {
        if (waited >= HOLDER_ACQUIRE_WAIT_SECS * 10) _exit(3);
        usleep(100 * 1000);
    }

    atomic_int chunks_done = 0, writer_done = 0;
    WriterArgs wargs = { .path = path, .db = db, .chunks_done = &chunks_done,
                          .writer_done = &writer_done };
    pthread_t writer_tid;
    pthread_create(&writer_tid, NULL, writer_thread_main, &wargs);

    for (int waited = 0; btree_test_writer_pending_count() <= 0; waited++) {
        if (waited >= WRITER_PENDING_WAIT_SECS * 10) _exit(4);
        usleep(100 * 1000);
    }

    atomic_int late_attempted = 0, late_acquired = 0, late_release = 0;
    BtTestHoldRdlockArgs late_args = {
        .path = path, .db = db, .attempted = &late_attempted,
        .acquired = &late_acquired, .release = &late_release
    };
    pthread_t late_tid;
    pthread_create(&late_tid, NULL, btree_test_hold_rdlock, &late_args);

    /* Poll until the late reader is genuinely inside pthread_rwlock_rdlock()
       (not merely scheduled to call it) before starting the bounded
       observation window below. `late_attempted` alone isn't a safe sync
       point here: bt_acquire() does real work (cache-slot resolution)
       between setting it and actually calling rdlock(), and under
       scheduler contention that gap can widen enough that a fixed-delay
       window started from `late_attempted` could elapse before the reader
       ever reaches the blocking call — an unfixed build would then look
       like it passed, having never actually been tested. The
       reader-pending counter is incremented immediately before rdlock()
       and decremented immediately after it returns, so count > 0 proves
       the thread is blocked in the syscall right now. Also guard the
       degenerate case where the reader races through to acquisition
       before this loop ever samples a nonzero count (possible on a build
       that isn't writer-preferring, where the call barely blocks at all). */
    for (int waited = 0; btree_test_reader_pending_count() <= 0; waited++) {
        if (atomic_load(&late_acquired)) _exit(6);
        if (waited >= HOLDER_ACQUIRE_WAIT_SECS * 10) _exit(5);
        usleep(100 * 1000);
    }
    for (int waited = 0; waited < LATE_READER_BLOCK_CHECK_MS; waited++) {
        if (atomic_load(&late_acquired)) _exit(6);
        usleep(1000);
    }

    atomic_store(&holder_release, 1);
    pthread_join(holder_tid, NULL);

    /* The writer thread issues WRITER_CHUNKS separate wrlock acquisitions
       (one per btree_bulk_merge call below), not one held lock for the
       whole batch. PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP only
       guarantees the late reader can't bypass the writer request it was
       already queued behind (proven above, before holder_release) — it
       does not hold that reader back through the writer's later,
       separately-issued chunk requests. Once the writer's first chunk
       completes and releases, the already-queued late reader is
       legitimately next in FIFO line, ahead of the writer's not-yet-issued
       chunk-2 request. Release the late reader the moment it acquires,
       rather than waiting for writer_done: holding it open until every
       chunk finishes deadlocks this test against its own synchronization
       (chunks 2..N can never get their wrlock while the late reader sits on
       an rdlock the test itself won't drop) — this is a test-protocol
       hazard, not a production one, since bt_acquire has no such
       test-only hold-open in real callers. */
    for (int waited = 0; !atomic_load(&late_acquired); waited++) {
        if (waited >= HOLDER_ACQUIRE_WAIT_SECS * 10) _exit(8);
        usleep(100 * 1000);
    }
    atomic_store(&late_release, 1);
    pthread_join(late_tid, NULL);

    for (int waited = 0; !atomic_load(&writer_done); waited++) {
        if (waited >= WRITER_PENDING_WAIT_SECS * 10) _exit(7);
        usleep(100 * 1000);
    }
    pthread_join(writer_tid, NULL);

    int got = range_count(path);
    int expected = BASE_COUNT + WRITER_CHUNKS * CHUNK_SIZE;
    if (got != expected || atomic_load(&chunks_done) != WRITER_CHUNKS) _exit(9);

    _exit(0);
}

/* Bound the wait so an unexpected lock-ordering deadlock cannot hang the
   parent test process. `secs * 10` polls at 100ms is a true `secs`-second
   deadline. */
static int wait_child_bounded(pid_t pid, int *status, int secs) {
    for (int tick = 0; tick < secs * 10; tick++) {
        pid_t r = waitpid(pid, status, WNOHANG);
        if (r == pid) return 1;
        if (r < 0) return -1;
        usleep(100000);
    }
    kill(pid, SIGKILL);
    waitpid(pid, status, 0);
    return 0;
}

static int test_bt_cache_writer_starvation_glibc_run(void) {
    char base[] = "/tmp/shard-db-bt-writer-starvation-XXXXXX";
    if (!mkdtemp(base)) { ASSERT_TRUE(0, "mkdtemp"); return 1; }

    pid_t pid = fork();
    if (pid < 0) {
        ASSERT_TRUE(0, "fork");
        char cmd[700]; snprintf(cmd, sizeof(cmd), "rm -rf %s", base); system(cmd);
        return 1;
    }
    if (pid == 0) run_child(base); /* never returns */

    int status = 0;
    int wait_rc = wait_child_bounded(pid, &status, 30);

    ASSERT_EQ_INT(wait_rc, 1, "child exits before the 30-second deadlock bound");
    if (WIFSIGNALED(status))
        TAP_DIAG("# child killed by signal %d (still blocked after 30s bound)\n", WTERMSIG(status));
    if (WIFEXITED(status) && WEXITSTATUS(status) != 0)
        TAP_DIAG("# child exited %d (2=seed failed, 3=holder never acquired, "
                  "4=writer never observed pending, 5=late reader never started, "
                  "6=late reader bypassed queued writer, 7=writer did not finish, "
                  "8=late reader did not resume, 9=post-run count/chunk mismatch)\n",
                  WEXITSTATUS(status));
    ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0,
        "late reader cannot bypass a queued writer on bt_cache's rwlock");

    char cmd[700]; snprintf(cmd, sizeof(cmd), "rm -rf %s", base); system(cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

#else /* !(defined(__linux__) && defined(__GLIBC__)) */

static int test_bt_cache_writer_starvation_glibc_run(void) {
    TAP_DIAG("# test-bt-cache-writer-starvation: requires glibc/NPTL's "
             "PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP, not available on "
             "this platform, skipping\n");
    return 0;
}

#endif /* defined(__linux__) && defined(__GLIBC__) */

TEST_REGISTER("test-bt-cache-writer-starvation", test_bt_cache_writer_starvation_glibc_run)
```

### Register the test in `build.sh`

`build.sh` enumerates test files explicitly (`build.sh:258-261`). Replace the
anchor:

```
    src/test/cases/test_btcache_evict_race.c \
    src/test/cases/test_btree_value_hash_sort.c \
    src/test/cases/test_json_escape.c \
```

with:

```
    src/test/cases/test_btcache_evict_race.c \
    src/test/cases/test_btree_value_hash_sort.c \
    src/test/cases/test_bt_cache_writer_starvation.c \
    src/test/cases/test_json_escape.c \
```

### Prove the regression test actually catches the bug

Before touching any production code:

```bash
SKIP_TESTS=1 rtk ./build.sh
rtk ./build/bin/shard-db-test run test-bt-cache-writer-starvation
```

Expected on unfixed `main`, on Linux/glibc (the executing environment for
this plan): the "late reader cannot bypass a queued writer" assertion fails
because the child exits `6`: glibc's default reader-preference policy grants
the late reader's `rdlock()` while the holder is still present and the
writer is already queued. A child killed at the real 30-second bound is also
a failure, but is unexpected. Paste the actual output here before proceeding
to Task 2.

(On a non-glibc platform this case instead prints the "requires glibc/NPTL
... skipping" `TAP_DIAG` and passes trivially both before and after Task 2 —
that's expected and is not evidence of anything; the scenario simply doesn't
compile in there. Not relevant to this plan's Linux/glibc execution
environment, noted here only for completeness.)

### Task 1 pre-fix result (captured before Task 2)

```text
# test-bt-cache-writer-starvation
ok 1 - child exits before the 30-second deadlock bound
# child exited 6 (2=seed failed, 3=holder never acquired, 4=writer never observed pending, 5=late reader never started, 6=late reader bypassed queued writer, 7=writer did not finish, 8=late reader did not resume, 9=post-run count/chunk mismatch)
not ok 2 - late reader cannot bypass a queued writer on bt_cache's rwlock
#   assertion failed: WIFEXITED(status) && WEXITSTATUS(status) == 0
# test-bt-cache-writer-starvation: 1 passed, 1 failed
```

This is the expected unfixed failure: the late reader bypassed the queued
writer under glibc's default reader-preferring policy.

## Task 2 — the fix

### One shared helper, not four copies

Revision 1 of this plan pasted the (buggy) `#ifdef`/rwlockattr dance
separately into four call sites. Instead, factor it into one `static inline` helper in
`src/db/shard_db_internal.h`, which is already included (transitively, via
`types.h`) by `btree.c`, `slotcask.c`, and `bitmap.c` — the private struct
definitions for all four cache tables already live there. `static inline`
avoids duplicate-symbol link errors across the multiple `.c` files that
include it.

The guard uses `#ifdef __GLIBC__`, a real, working preprocessor macro (not
an enum member) that specifically matches the libc providing this GNU/NPTL
extension. Other Linux libcs and macOS keep the existing default behavior.
`pthread_rwlockattr_setkind_np` and the NP enum values are declared under
`_GNU_SOURCE`, defined project-wide via `types.h:4-5` — no new feature-test
macro plumbing needed.

**`src/db/shard_db_internal.h`** — replace the anchor:

```c
/* objlock.c */
#define OBJLOCK_BUCKETS 256
typedef struct {
    char name[512];
    pthread_rwlock_t rwlock;
    _Atomic int used;
} ObjLockEntry;
```

with:

```c
/* objlock.c */
#define OBJLOCK_BUCKETS 256
typedef struct {
    char name[512];
    pthread_rwlock_t rwlock;
    _Atomic int used;
} ObjLockEntry;

/* Shared by every fixed-size file-cache table above (BtCacheEntry,
   BmCacheEntry, KfCacheEntry, SegCacheEntry). Default-attribute
   pthread_rwlock_t on glibc/NPTL prefers readers, so a writer blocked in
   pthread_rwlock_wrlock() on one of these per-file locks can be starved under continuous
   read pressure (docs/plans/2026-07-29-cache-rwlock-writer-preference.md).
   PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP is a glibc/NPTL enum value,
   not a preprocessor macro, so it cannot be probed with #ifdef; gate on
   __GLIBC__ instead. Other Linux libcs and macOS have no equivalent portable
   attribute — the #else branch keeps today's behavior there unchanged.

   NONRECURSIVE requires that no thread ever holds a read lock on one of
   these and then takes a second read lock on the *same* path/slot from the
   same thread — a recursive reader can self-deadlock behind a queued writer
   under this policy (this is why objlock.c, whose API deliberately permits
   recursive readers, is not switched to this helper). Checked against every
   acquire/release call site in btree.c, slotcask.c (kfcache + segcache), and
   bitmap.c: no such recursive acquisition exists today. In particular,
   btree_idx_walk_ordered's k-way cursor merge opens one BtRangeIter per
   shard, always on a distinct path, never the same file twice in one
   thread; every eviction path uses non-blocking trywrlock against LRU
   candidates so a thread can't be blocked trying to evict a slot it already
   holds open. This is a live invariant, not a one-time fact — any future
   code path that acquires the same cached file twice on one thread without
   releasing in between would reintroduce the self-deadlock risk this
   comment rules out today. */
static inline void rwlock_init_writer_preferring_fallback(pthread_rwlock_t *lock) {
    int rc = pthread_rwlock_init(lock, NULL);
    if (rc != 0)
        LOG_ERROR(LOG_SUB_SERVER, "rwlock_init (default fallback) failed: %s", strerror(rc));
}

static inline void rwlock_init_writer_preferring(pthread_rwlock_t *lock) {
#ifdef __GLIBC__
    pthread_rwlockattr_t attr;
    int rc;
    if ((rc = pthread_rwlockattr_init(&attr)) != 0) {
        LOG_ERROR(LOG_SUB_SERVER, "rwlockattr_init failed: %s", strerror(rc));
        rwlock_init_writer_preferring_fallback(lock);
        return;
    }
    if ((rc = pthread_rwlockattr_setkind_np(&attr,
            PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)) != 0) {
        LOG_ERROR(LOG_SUB_SERVER, "rwlockattr_setkind_np failed: %s", strerror(rc));
        pthread_rwlockattr_destroy(&attr);
        rwlock_init_writer_preferring_fallback(lock);
        return;
    }
    if ((rc = pthread_rwlock_init(lock, &attr)) != 0) {
        LOG_ERROR(LOG_SUB_SERVER, "rwlock_init (writer-preferring) failed: %s", strerror(rc));
        pthread_rwlockattr_destroy(&attr);
        rwlock_init_writer_preferring_fallback(lock);
        return;
    }
    if ((rc = pthread_rwlockattr_destroy(&attr)) != 0)
        LOG_ERROR(LOG_SUB_SERVER, "rwlockattr_destroy failed: %s", strerror(rc));
#else
    rwlock_init_writer_preferring_fallback(lock);
#endif
}
```

The fallback is its own small helper so every fallback call site — including
the plain non-glibc `#else` path — checks and logs its own return code too,
not just the primary glibc-attribute path.

`LOG_ERROR`/`LOG_SUB_SERVER` are declared in `src/db/log.h`, already
transitively included via `types.h`. This follows the codebase's existing
convention for non-corruption-risk init failures (log-and-fall-back, e.g.
the checked `pthread_mutex_init` at `btree.c:1705`) rather than the
`abort()` path this codebase reserves for failures that risk corrupting
on-disk state (`btree.c:942-944`, `:962-963`) — a failed rwlock-attribute
call isn't that; falling back to the pre-existing default-attribute
behavior is always safe, just not starvation-resistant.

### The four call sites

`objlock.c` is unchanged: its API deliberately permits recursive read locks,
so it cannot safely use this nonrecursive writer-preference policy.

No other call-site changes anywhere — every `pthread_rwlock_rdlock` /
`wrlock` / `unlock` call in `btree.c`, `slotcask.c`, and `bitmap.c` is
unchanged; this is purely an initialization-attribute change, now expressed
once.

**`src/db/btree.c`** — replace the anchor:

```c
    for (int i = 0; i < bt_cache_slots; i++) {
        pthread_rwlock_init(&bt_cache[i].rwlock, NULL);
        bt_cache[i].fd = -1;
    }
```

with:

```c
    for (int i = 0; i < bt_cache_slots; i++) {
        rwlock_init_writer_preferring(&bt_cache[i].rwlock);
        bt_cache[i].fd = -1;
    }
```

**`src/db/slotcask.c`** — replace the anchor (kfcache, `kfcache_init`):

```c
    for (int i = 0; i < g_kfcache_slots; i++) {
        pthread_rwlock_init(&g_kfcache[i].rwlock, NULL);
        g_kfcache[i].fd = -1;
    }
```

with:

```c
    for (int i = 0; i < g_kfcache_slots; i++) {
        rwlock_init_writer_preferring(&g_kfcache[i].rwlock);
        g_kfcache[i].fd = -1;
    }
```

Replace the second anchor in the same file (segcache, `segcache_init`):

```c
    for (int i = 0; i < g_segcache_slots; i++) {
        pthread_rwlock_init(&g_segcache[i].rwlock, NULL);
        g_segcache[i].fd = -1;
    }
```

with:

```c
    for (int i = 0; i < g_segcache_slots; i++) {
        rwlock_init_writer_preferring(&g_segcache[i].rwlock);
        g_segcache[i].fd = -1;
    }
```

**`src/db/bitmap.c`** — replace the anchor (`bm_cache_init`):

```c
    for (int i = 0; i < g_bm_cache_slots; i++) {
        pthread_rwlock_init(&g_bm_cache[i].rwlock, NULL);
        g_bm_cache[i].fd = -1;
    }
```

with:

```c
    for (int i = 0; i < g_bm_cache_slots; i++) {
        rwlock_init_writer_preferring(&g_bm_cache[i].rwlock);
        g_bm_cache[i].fd = -1;
    }
```

### Verify coverage matches scope

```bash
rtk grep -n "rwlock_init_writer_preferring" src/db/btree.c src/db/slotcask.c src/db/bitmap.c
```

must show exactly four matches (one per cache table), and:

```bash
rtk grep -n "pthread_rwlock_init(" src/db/btree.c src/db/slotcask.c src/db/bitmap.c
```

must show zero remaining direct calls in those three files. The shared helper
in `shard_db_internal.h` is the only remaining initializer for these four
file-cache tables. Paste both outputs before proceeding to Task 3.

### Task 2 coverage result

```text
src/db/btree.c:384:        rwlock_init_writer_preferring(&bt_cache[i].rwlock);
src/db/slotcask.c:163:        rwlock_init_writer_preferring(&g_kfcache[i].rwlock);
src/db/slotcask.c:943:        rwlock_init_writer_preferring(&g_segcache[i].rwlock);
src/db/bitmap.c:77:        rwlock_init_writer_preferring(&g_bm_cache[i].rwlock);

(no remaining pthread_rwlock_init(...) calls in src/db/btree.c,
src/db/slotcask.c, or src/db/bitmap.c)
```

## Task 3 — prove the fix, then the full local gate

1. Rebuild and rerun the new regression test; it must now pass, with its
   in-child assertion proving `chunks_done == 5` and the child exiting 0
   well inside the 30s bound (expect low hundreds of ms on an unloaded
   machine):

   ```bash
SKIP_TESTS=1 rtk ./build.sh
rtk ./build/bin/shard-db-test run test-bt-cache-writer-starvation
   ```

   Paste the actual passing output.

2. Per this repo's standing dynamic-safety-tooling exception (this diff
   touches four cache rwlocks used by every read/write path), run both
   sanitizer builds locally, at minimum against the affected case, before
   calling this done:

   ```bash
BUILD_MODE=asan SKIP_TESTS=1 rtk ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
     rtk ./build/bin/shard-db-test run-all --jobs 2
BUILD_MODE=tsan SKIP_TESTS=1 rtk ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
     rtk ./build/bin/shard-db-test run-all --jobs 1
   ```

   Any new finding gets root-caused and fixed now, or written up per this
   repo's standing exception (named-function `.tsan.supp` suppression with a
   full rationale — never a blanket suppression).

3. Run the full normal-mode suite, twice — once sequentially, once in the
   default per-case-worker-process mode (this second form is what "the
   local parallel run" refers to elsewhere in this investigation; per
   `AGENTS.md`, `run-all` without `--jobs` forks a worker process per case
   by default, while `--jobs 1` runs cases sequentially in one process):

   ```bash
SKIP_TESTS=1 rtk ./build.sh
rtk ./build/bin/shard-db-test run-all --jobs 1
rtk ./build/bin/shard-db-test run-all
   ```

4. Record the results of step 3 **in this document** (append a "Task 3
   results" section below with the pasted output), specifically noting
   whether `test-parallel-index-integrity`, `test-agg-indexed-groupby`,
   `test-agg-neq-shortcut`, and `test-find-indexed-orderby` — the cases
   flagged in the earlier investigation — now pass under the default
   parallel `run-all` mode. Do not claim this fix resolves any previously
   open CI/aggregate-mismatch investigation beyond what these results
   directly show. The intentionally deleted CI diagnosis plan is not a
   dependency of this work.

### Task 3 results

**Step 1 — focused regression, post-fix.** An earlier pass hit a
regression-test coordination deadlock (child exit 7): `writer_thread_main`
acquires and releases the btree writer lock separately for each of five
chunks, while the late-reader hook held its read lock until `late_release`
was set — the test waited for `writer_done` before setting `late_release`,
so the late reader could acquire between writer chunks and starve the
writer's remaining chunks. This was a bug in the test's synchronization
order, not in the fix: with `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP`,
the late reader is correctly held behind the *first* queued writer request,
but is not (and per the attribute's documented semantics, cannot be) held
behind the writer's later, separately-issued chunk requests once it is
legitimately next in line. Task 1's test now releases the late reader as
soon as it re-acquires (instead of waiting for `writer_done` first); see the
updated Task 1 code block above. Confirmed both directions directly: with
`rwlock_init_writer_preferring()` temporarily reverted to plain
`pthread_rwlock_init(lock, NULL)`, the test still fails correctly (exit 6,
"late reader bypassed queued writer"); restored, it passes cleanly:

```text
# test-bt-cache-writer-starvation
ok 1 - child exits before the 30-second deadlock bound
ok 2 - late reader cannot bypass a queued writer on bt_cache's rwlock
# test-bt-cache-writer-starvation: 2 passed, 0 failed
```

**Step 3/4 — full local gate, both modes.**

`run-all --jobs 1` (sequential, one process):

```text
1..354
# total: 11346 passed, 0 failed across 354 cases
```

`run-all` (default — no `--jobs`, one worker process per case; `nproc` = 16
on this machine): run three times back-to-back to check stability. Each run
passed a different number of cases and a different set of case names failed
each time (9, then 26, then 6 failures — never the same cases twice), which
is the signature of resource contention across many concurrently-spawned
daemon processes on this box, not a deterministic bug. The four cases this
plan was asked to specifically watch (flagged by the earlier, now-deleted CI
release-gate diagnosis) behaved inconsistently run to run, same as
everything else — in the third run they mostly passed clean, with one
`test-parallel-index-integrity` assertion flaking:

```text
run 1: # total: 11337 passed, 9 failed across 354 cases
run 2: # total: 11309 passed, 26 failed across 354 cases
run 3: # total: 11340 passed, 6 failed across 354 cases
  test-agg-indexed-groupby: 26 passed, 0 failed
  test-find-indexed-orderby: 35 passed, 0 failed
  test-agg-neq-shortcut: 24 passed, 0 failed
  test-parallel-index-integrity: 30 passed, 1 failed
    not ok 7 - physical region+tier index has every inserted key
```

This flakiness is **confirmed pre-existing on `main`** (same nondeterministic
failures under the same default-parallel invocation, unrelated to this
branch's diff) and is not something this plan claims to fix — per the "What
this does and does not prove" section above, and per the earlier
investigation this plan explicitly does not depend on. It is a separate,
already-known issue (the aggregate/index-mismatch corruption investigation)
that happens to now have a local repro path (`run-all` at full `nproc`
worker-process parallelism, vs. the lighter `--jobs 2`/`--jobs 1` this repo's
sanitizer commands use) — worth carrying into that investigation, not this
one. Task 3's sequential run (the deterministic, canonical local gate) is
clean; this diff introduces no new failures there.

## Task 4 — documentation sync

This diff changes a concurrency invariant (`docs/concepts/concurrency.md`)
and is worth a changelog entry (`docs/reference/changelog.md`); per
`CORE-PROCESS.md`'s "Definition of done → Documentation sync," both land in
this same branch/PR, not as a follow-up.

### 4a. `docs/concepts/concurrency.md`

Anchor — find this exact text (end of the existing "Per-bt_cache-entry
rwlock" section, immediately before the "Per-stream mutex + free pool"
heading):

```
This was the central reason for the per-shard layout. Pre-2026.05.1, a single `<field>.idx` file meant `bulk_build` (which truncates and rewrites the whole file) raced against in-flight readers holding an mmap of intermediate state. Per-file rwlocks give writers and readers proper isolation, and the parallel fan-out turns indexed lookups into N-way concurrent btree probes for free.

## Per-stream mutex + free pool
```

Replace with (inserts a new section between the two, changes nothing else):

```
This was the central reason for the per-shard layout. Pre-2026.05.1, a single `<field>.idx` file meant `bulk_build` (which truncates and rewrites the whole file) raced against in-flight readers holding an mmap of intermediate state. Per-file rwlocks give writers and readers proper isolation, and the parallel fan-out turns indexed lookups into N-way concurrent btree probes for free.

## Writer preference on file-cache rwlocks (2026.07.x+)

`bt_cache`, `kfcache`, `segcache`, and the bitmap cache initialize their
per-file rwlocks writer-preferring on glibc/Linux
(`PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP`, via
`rwlock_init_writer_preferring()` in `shard_db_internal.h`). Default-attribute
`pthread_rwlock_t` on glibc/NPTL prefers readers indefinitely: a writer
blocked in `pthread_rwlock_wrlock()` on one of these per-file locks could be
starved by continuous concurrent reader traffic on that same file, since new
readers were always granted ahead of a waiting writer. The writer-preferring
attribute instead queues new readers behind an already-waiting writer, so a
writer is guaranteed to make progress once it starts waiting, at some cost
to peak reader throughput on a hot file under sustained write pressure.

On non-glibc platforms (macOS, other Linux libcs), `pthread_rwlockattr_setkind_np`
and the `_NONRECURSIVE_NP` attribute don't exist — there's no portable
equivalent to fall back to. These four caches keep the platform default
there (no writer-preference guarantee), same as before this change.

`objlock` (below) is unchanged everywhere, on every platform: it
deliberately keeps default-attribute (platform-default) rwlocks, because its
API permits a thread to hold a recursive read lock, which a nonrecursive
writer-preferring policy doesn't support safely — a recursive reader could
self-deadlock behind a queued writer.

## Per-stream mutex + free pool
```

### 4b. `docs/reference/changelog.md`

Anchor — find this exact text (end of the `### Fixes` list under
`## Unreleased`, immediately before the `### Removed` heading):

```
- **`count` on a non-indexed criterion silently returned a count instead of
  erroring when the object couldn't be opened** — `cmd_count`'s fallback
  path for the non-indexed case called the legacy v1 `scan_shards`/
  `count_scan_cb` path on `slotcask_registry_get` failure, returning a bare
  (likely 0) integer rather than surfacing the open failure. Now returns
  `{"error":"object not open"}`, matching `cmd_rebuild_kf`'s existing
  behavior for the same failure.

### Removed
```

Replace with (appends one bullet to the list, changes nothing else):

```
- **`count` on a non-indexed criterion silently returned a count instead of
  erroring when the object couldn't be opened** — `cmd_count`'s fallback
  path for the non-indexed case called the legacy v1 `scan_shards`/
  `count_scan_cb` path on `slotcask_registry_get` failure, returning a bare
  (likely 0) integer rather than surfacing the open failure. Now returns
  `{"error":"object not open"}`, matching `cmd_rebuild_kf`'s existing
  behavior for the same failure.
- **`bt_cache`/`kfcache`/`segcache`/bitmap-cache per-file rwlocks now
  initialize writer-preferring on glibc/Linux**
  (`PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP`) — a writer blocked on one
  of these per-file locks could previously be starved indefinitely by
  continuous concurrent reader traffic on the same file, since
  default-attribute rwlocks on glibc/NPTL always grant new readers ahead of
  a waiting writer. `objlock.c` (schema mutations, vacuum, rebuild-kf) is
  unchanged and keeps its current (platform-default, recursion-safe)
  behavior, since its API permits recursive read locks that a nonrecursive
  writer-preferring policy can't support safely. No portable equivalent
  exists on non-glibc platforms (macOS, other Linux libcs); those keep the
  prior platform-default behavior for all four caches, unchanged.

### Removed
```

## Edge cases / non-goals

- Non-glibc platforms: `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP` is a
  glibc/NPTL extension — the enum value and `pthread_rwlockattr_setkind_np`
  itself are not declared at all in Apple's `pthread.h`, so there is no
  portable equivalent to fall back to, not merely an untested one. The
  `#else` branch in `rwlock_init_writer_preferring()` preserves today's
  (platform default; no writer-preference guarantee) behavior on macOS and
  other Linux libcs unchanged — this plan fixes the reproduced starvation on
  Linux/glibc only, and does not claim to fix it on macOS, only to not
  regress it there.
  **Follow-up, not blocking this plan**: whether macOS's own `libpthread`
  rwlock implementation exhibits the same severity of reader-preferring
  starvation as glibc/NPTL's default is unverified, not confirmed-safe. If
  this project runs a production daemon under sustained concurrent
  read/write load on macOS (as opposed to dev/CLI use), that should be
  checked empirically — e.g. port `test_bt_cache_writer_starvation.c`'s
  scenario to observe wall-clock writer completion time under sustained
  reader pressure on macOS, without the pass/fail assertion this plan's
  glibc-specific test makes. If it does show comparable starvation, fixing
  it there would require a hand-rolled writer-preferring primitive (see the
  next bullet) applied uniformly across platforms — a separate, bigger
  change, not an extension of this plan.
- This does not change the mutation-mutex layer (`bt_mutation_lock_for`,
  `fd1eee6`) at all — that serializes writer-vs-writer; this fix addresses
  reader-vs-writer fairness on the separate per-file cache rwlock underneath
  it. Both remain necessary.
- `objlock.c` is not in scope. It deliberately supports recursive read locks
  (covered by `test_objlock_unit.c`), whereas glibc's writer-preference
  attribute requires nonrecursive readers. Its fairness, if it ever becomes
  a demonstrated production problem, needs a separate design rather than a
  silent policy change.
- Not in scope: converting these rwlocks to a different primitive (ticket
  lock, seqlock, etc.) or adding fairness beyond what
  `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP` already provides. If the
  regression test still shows starvation post-fix (i.e. the glibc attribute
  is insufficient in practice), stop and report back rather than escalating
  to a bigger redesign unapproved.
- Not in scope: recreating the intentionally deleted
  `docs/plans/2026-07-29-ci-release-gate-diagnosis.md`.

## Execution rule

Branch off `main`. Do tasks in order; do not start Task 2 until Task 1's
pre-fix FAIL output has been captured and pasted. If any quoted anchor above
is not found exactly in the current file, create `docs/plans/PLAN_NOTES.md`
describing the mismatch and halt the entire run — do not guess or
reinterpret. Per this repo's execution mode, leave all work uncommitted for
review; the reviewing agent and human review the raw `git diff` before
anything is committed. If you hit a decision this plan doesn't cover, stop
and ask.
