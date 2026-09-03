# Plan: fix nested kf-rdlock in `shard_count_worker` (self-deadlock on the bitmap count path)

## Status

**READY FOR HUMAN APPROVAL — full CORE-PROCESS plan (rewritten 2026-09-03).**
Supersedes the 2026-08-27 pre-plan that previously occupied this file. The
pre-plan had the right mechanism but named the wrong hazard call site
(`slotcask_bulk_resolve_and_fetch` / its Pass-4 `continue`) and sketched an
unreachable EDEADLK-undercount mode. Re-verification against the current tree
(c09b661) corrected both; see Root cause. Anchors below are quoted text, not
line numbers; all were verified 2026-09-03. If a quoted anchor is not found
exactly, follow Embedded execution rules: write `PLAN_NOTES.md` and halt.

Do not execute until the human approves this plan explicitly.

## Root cause (verified 2026-09-03, tree c09b661)

`shard_count_worker` (the per-shard-group worker of the indexed-count
two-pass executor, `src/db/query.c`) pre-opens its shard group's kf reader
for the pass-1 inline bitmap probe, **only when bitmap post-filters are
present**:

```c
    if (sc->n_bm_postfilter > 0 && sc->entry_count > 0) {
        ...
        /* Pre-open KF handle — acquire once, probe inline in the hot
         * loop without per-hash kfcache_acquire/release overhead. */
        if (sdb) {
            char kf_p[PATH_MAX];
            kf_path_for(kf_p, sdb->data_dir, shard_id);
            if (kfcache_acquire(&kh, kf_p, sdb->slots_per_shard, 0) != 0) {
```

The handle `kh` is held through all of pass-1 (the inline `kh.map` probe) and
**into pass-2**. Pass-2 dispatches on whether locations were already
resolved — and whenever `kh` is held, `resolved != NULL`, so the fetcher is
`slotcask_bulk_fetch_resolved`, *not* `slotcask_bulk_resolve_and_fetch`:

```c
            CountBatchCbCtx cb_ctx = { sc, &local };
            if (resolved) {
                /* Bitmap path: already have resolved locations, skip KF re-probe */
                slotcask_bulk_fetch_resolved(sdb, resolved,
                                              (size_t)n_need_fetch,
                                              &cb_ctx, count_batch_cb);
            } else {
                /* Non-bitmap path: use combined resolve+fetch */
                slotcask_bulk_resolve_and_fetch(sdb, fetch_hashes, ...
```

`kh` is released only at cleanup (`if (kh.map) kfcache_release(&kh);`).

Inside the fetcher, `slotcask_bulk_fetch_resolved` (`src/db/slotcask.c`)
partitions the batch by kf shard and runs one pipeline per partition:

```c
        if (nparts <= 3) {
            for (int i = 0; i < nparts; i++) kf_reval_fetch_one(&fargs[i]);
        } else {
            parallel_for_io(kf_reval_fetch_worker, fargs, nparts,
                            sizeof(KfRevalFetchArg));
        }
```

Every entry in a count worker's group shares the same `hash[0..1]` (same
data shard — see the worker's own comment "All entries in this group share
the same hash[0..1] → same data shard"), so `nparts == 1` and
`kf_reval_fetch_one` runs **inline on the count worker's own thread**, where
it does:

```c
    kf_path_for(kf_path, fa->db->data_dir, fa->kf_shard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, fa->db->slots_per_shard, 0) != 0) {
        /* Whole partition unreadable — retire every record in it. */
        for (size_t i = 0; i < fa->count; i++)
            fa->recs[fa->start + i].sid = 0xFF;
        return;
    }
```

Same `data_dir`, same shard id → **same kf path → same kfcache entry → same
pthread rwlock the worker already holds as a reader**. Recursive reader, same
thread, on every bitmap-path count that fetches at least one record.

The kfcache rwlocks are `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP` on
glibc (`rwlock_init_writer_preferring`, `src/db/shard_db_internal.h`), whose
header documents the exact invariant this violates:

```
   acquire/release call site in btree.c, slotcask.c (kfcache + segcache), and
   bitmap.c: no such recursive acquisition exists today. [...] This is a live
   invariant, not a one-time fact — any future
   code path that acquires the same cached file twice on one thread without
   releasing in between would reintroduce the self-deadlock risk this
   comment rules out today.
```

### Failure modes (corrected from the pre-plan)

- **Writer queued in the gap → permanent hang.** The count worker holds the
  probe reader (acquire #1). A concurrent mutation on the same object (which
  does *not* exclude a normal read at the object-rwlock level) reaches its
  kf-shard wrlock and queues. Under `PREFER_WRITER_NONRECURSIVE_NP`, a new
  reader blocks while a writer waits, so the worker's nested acquire
  (acquire #2) parks behind the queued writer while the writer waits for
  acquire #1 — a one-thread self-cycle. The worker never returns; the count
  request never answers; the mutation never commits. Repeated occurrences
  permanently drain the query/dispatch thread pool.
- **No writer in the gap → silent recursion.** The nested rdlock succeeds
  and nothing visibly happens. This is why the suite never caught it: no
  test runs this worker while a mutation is queued on the same shard.
- **EDEADLK → under-count (pre-plan's mode 2) is NOT reachable here.** glibc
  returns `EDEADLK` for *read-after-write* recursion by the same thread; this
  path is read-after-read while holding a reader, so the nested acquire
  either blocks (writer queued → hang) or succeeds. The under-count symptom
  would require `kfcache_acquire` to fail for an unrelated reason (the
  fetcher then retires the whole partition — records silently dropped).
- **Platform note:** non-glibc builds (macOS) fall back to a default
  attribute rwlock where reader-after-reader recursion silently succeeds —
  the hang is Linux/glibc-specific, and so is the red state of the
  regression test below.

### Reachability

Requires ALL of: (a) a `count` whose plan is `FP_PRIMARY_LEAF` with at least
one bitmap eq/in post-filter leaf (`n_bm_postfilter > 0` — this is what
pre-opens `kh`) and at least one non-bitmap post-filter
(`all_postfilters_are_bm == 0` — this is what forces record fetches into
pass-2); (b) ≥1 candidate row needing fetch; (c) a concurrent insert/update/
delete/bulk on the same object whose kf wrlock for the candidate shard is
queued between acquire #1 and acquire #2. Pass-1 (full bitmap probe of every
candidate) sits between the two acquires, so the window is realistic under
concurrent write load. This is a liveness bug (hang), not corruption; no
data is lost, but the daemon's query capacity degrades permanently per
occurrence.

## Fix shape (one hunk)

Scope the worker's probe handle strictly around pass-1: release `kh` (and
NULL it) immediately before the pass-2 block, after the last inline probe.
The fetcher already manages its own per-shard acquire→validate→copy under
the 2026-08-21 window contract, so nothing else changes: no new lock
ordering, no change to `slotcask_bulk_fetch_resolved`, no change to nesting
depth (`parallel_for_io` inside the fetcher is unchanged), kfcache-before-
bitmap ordering untouched (this worker holds only `kh` and bitmap handles,
and bitmap handles are closed only at cleanup). The existing
`if (kh.map)` cleanup stays as the backstop for the early-exit `goto
cleanup` paths between the acquire and the new release (`entry_count <= 0`,
`fetch_hashes` calloc failure, `resolved` calloc failure).

## Call-site / consumer audit (required before touching this code)

Nothing in this plan changes a signature, format, protocol, or public API —
it reorders lock scope inside one function and adds TEST_BUILD-only
instrumentation. Consumers audited 2026-09-03:

- `kfcache_acquire` call sites in `src/db/query.c`: the count worker (the
  hazard, fixed here), and four sites in `bitmap_emit_generic_for_shard`,
  `bitmap_emit_for_shard`, `build_keyset_from_bitmap`,
  `build_keyset_bitmap_complement`. None of those four call
  `slotcask_bulk_fetch_resolved` / `slotcask_bulk_resolve_and_fetch` while
  their handle is open.
- `slotcask_bulk_fetch_resolved` callers: the count worker (fix site) and
  `slotcask_bulk_resolve_and_fetch` (slotcask.c, which never holds an outer
  handle).
- `slotcask_bulk_resolve_and_fetch` callers: `src/db/storage.c` (find
  executor) and two aggregate paths in `query.c` — none pre-opens a kf
  handle, so they cannot nest.

Executor verification step (Task 4, cheap): re-run
`grep -n "kfcache_acquire" src/db/query.c src/db/storage.c` and
`grep -n "slotcask_bulk_fetch_resolved\|slotcask_bulk_resolve_and_fetch"
src/db/*.c` and confirm each site still matches the audit above. Any
mismatch → `PLAN_NOTES.md`, halt.

## Embedded execution rules

- Branch `fix/shard-count-nested-kf-read` off the default branch. Per this
  repo's standing execution mode, leave ALL work **uncommitted** at the end
  for the review pass; the human owns every git write operation.
- Do tasks strictly in order. Never weaken, skip, or reorder a test step.
- Build/test commands: build with `SKIP_TESTS=1 ./build.sh`; run the new
  case with `./build/bin/shard-db-test run test-shard-count-nested-kf-read`;
  full suite `./build/bin/shard-db-test run-all`.
- If any quoted anchor is not found exactly, write `PLAN_NOTES.md`
  describing the mismatch and halt the entire execution run immediately —
  do not guess, reinterpret, or continue to any further task, even an
  unrelated one. Resuming requires the human (or the planning model,
  re-engaged) to read `PLAN_NOTES.md` and hand back a patched or fresh plan.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.
- Sanitizer gate (AGENTS.md, locks are touched): after Task 3, run the FULL
  gate — `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` then three fresh
  `./build/bin/shard-db-test run-all`; then `BUILD_MODE=tsan SKIP_TESTS=1
  ./build.sh` then three fresh `TSAN_OPTIONS="second_deadlock_stack=1:
  print_stacktrace=1" ./build/bin/shard-db-test run-all`. No
  `halt_on_error=0` anywhere. Default all-core parallelism; no `--jobs`.
- Dynamic-safety caveat for the red proof: the base-tree hang is a lock
  wedge, not memory unsafety — sanitizers will not report it; the timed-join
  assertion is the detector.

## Task 1 — TEST_BUILD pass-1 gap seam (inert until armed)

Test-first note: this task adds test infrastructure only; it has no
behavioral test of its own. Its verification is (a) the production binary
gains zero new code (everything is `#ifdef TEST_BUILD`), and (b) the full
suite still passes. The behavioral red arrives in Task 2.

### 1a. `src/db/shard_test_ctl.h` — new seam atomics

Locate (quoted anchor):

```c
/* Task B1 regression hook (docs/plans/2026-08-28-eliminate-tsan-supp.md):
   parks slotcask_bulk_lookup_in_kfshard between its probe and verify
   phases so a test can run kf-slot churn in the gap. */
extern _Atomic int g_shard_test_bulk_lookup_gap;
extern _Atomic int g_shard_test_bulk_lookup_gap_hit;
extern _Atomic int g_shard_test_bulk_lookup_gap_release;
```

Immediately after that block, add:

```c
/* Count-worker pass-1 gap hook (docs/plans/2026-08-27-shard-count-worker-
   nested-kf-read.md Task 1): parks shard_count_worker after pass-1's
   inline KF probe — while the probe reader is still held — so the
   cross-process test control channel can queue a mutation writer on the
   same shard before the batch fetch re-acquires the same kfcache entry.
   Armed by test_control.c on INSTALL kind 1; the daemon-side park itself
   waits on the control-channel condvar (see test_control.c), so unlike
   the B1 gap there is no release atomic here. */
extern _Atomic int g_shard_test_count_gap;
extern _Atomic int g_shard_test_count_gap_hit;
```

Then locate (quoted anchor, in `shard_test_ctl_reset`):

```c
    atomic_store(&g_shard_test_bulk_lookup_gap, 0);
    atomic_store(&g_shard_test_bulk_lookup_gap_hit, 0);
    atomic_store(&g_shard_test_bulk_lookup_gap_release, 0);
```

Immediately after, add:

```c
    atomic_store(&g_shard_test_count_gap, 0);
    atomic_store(&g_shard_test_count_gap_hit, 0);
```

### 1b. `src/db/slotcask.c` — definitions

Locate (quoted anchor):

```c
_Atomic int g_shard_test_bulk_lookup_gap;
_Atomic int g_shard_test_bulk_lookup_gap_hit;
_Atomic int g_shard_test_bulk_lookup_gap_release;
```

Immediately after, add:

```c
_Atomic int g_shard_test_count_gap;
_Atomic int g_shard_test_count_gap_hit;
```

### 1c. `src/db/test_control.h` — declaration

Locate (quoted anchor):

```c
#ifdef TEST_BUILD
int shard_db_test_control_start(int fd);
void shard_db_test_control_stop(void);
#endif
```

Replace with:

```c
#ifdef TEST_BUILD
int shard_db_test_control_start(int fd);
void shard_db_test_control_stop(void);
/* Count-worker pass-1 gap seam (docs/plans/2026-08-27-shard-count-worker-
   nested-kf-read.md Task 1): called by shard_count_worker with the pass-1
   KF reader still held. Reports REACHED (phase 2) on the control channel
   and blocks until the runner releases or the daemon stops. */
void shard_db_test_control_count_gap_wait(void);
#endif
```

### 1d. `src/db/test_control.c` — dispatch + park function

Add the include (quoted anchor near the top, after `#include "test_control.h"`):

```c
#include "slotcask.h"
#include "test_control.h"
```

Replace with:

```c
#include "slotcask.h"
#include "shard_test_ctl.h"
#include "test_control.h"
```

Add a hook-kind selector next to the private message enum. Locate:

```c
enum {
    TEST_HOOK_INSTALL = 1,
    TEST_HOOK_RELEASE = 2,
    TEST_HOOK_CLEAR   = 3,
    TEST_HOOK_ACK     = 4,
    TEST_HOOK_REACHED = 5,
};
```

Replace with:

```c
enum {
    TEST_HOOK_INSTALL = 1,
    TEST_HOOK_RELEASE = 2,
    TEST_HOOK_CLEAR   = 3,
    TEST_HOOK_ACK     = 4,
    TEST_HOOK_REACHED = 5,
};

/* INSTALL message phase selects which daemon-side hook to arm.
   0 = legacy slotcask after-old hook; 1 = count-worker pass-1 gap. */
#define TEST_HOOK_KIND_AFTER_OLD 0
#define TEST_HOOK_KIND_COUNT_GAP 1
```

Locate the INSTALL case:

```c
        case TEST_HOOK_INSTALL:
            slotcask_test_set_after_old_hook(test_control_after_old, c);
            break;
```

Replace with:

```c
        case TEST_HOOK_INSTALL:
            if (msg.phase == TEST_HOOK_KIND_COUNT_GAP) {
                atomic_store(&g_shard_test_count_gap, 1);
                atomic_store(&g_shard_test_count_gap_hit, 0);
            } else {
                slotcask_test_set_after_old_hook(test_control_after_old, c);
            }
            break;
```

Locate the CLEAR case:

```c
        case TEST_HOOK_CLEAR:
            slotcask_test_set_after_old_hook(NULL, NULL);
```

Replace with:

```c
        case TEST_HOOK_CLEAR:
            slotcask_test_set_after_old_hook(NULL, NULL);
            atomic_store(&g_shard_test_count_gap, 0);
```

After the existing `test_control_after_old` function (anchor: its closing
lines), insert the new park function:

```c
/* Runs on the count worker thread inside the pass-1 gap seam: reports
   REACHED with phase 2 and blocks until the control thread broadcasts a
   release (or the daemon stops). Same contract as test_control_after_old;
   the caller holds the pass-1 KF reader while parked — that is the point. */
void shard_db_test_control_count_gap_wait(void) {
    TestControl *c = &g_test_control;
    TestHookMessage reached = { .kind = TEST_HOOK_REACHED, .phase = 2 };
    pthread_mutex_lock(&c->lock);
    c->waiting_for_release = 1;
    c->release = 0;
    pthread_cond_broadcast(&c->cond);
    pthread_mutex_unlock(&c->lock);

    if (test_control_write_full(c->fd, &reached, sizeof(reached)) != 0) {
        pthread_mutex_lock(&c->lock);
        c->release = 1;
        pthread_cond_broadcast(&c->cond);
        c->waiting_for_release = 0;
        pthread_mutex_unlock(&c->lock);
        return;
    }

    pthread_mutex_lock(&c->lock);
    while (c->running && !c->release)
        pthread_cond_wait(&c->cond, &c->lock);
    c->waiting_for_release = 0;
    pthread_mutex_unlock(&c->lock);
}
```

(The insertion anchor is the end of `test_control_after_old`:

```c
    pthread_mutex_lock(&c->lock);
    while (c->running && !c->release)
        pthread_cond_wait(&c->cond, &c->lock);
    c->waiting_for_release = 0;
    pthread_mutex_unlock(&c->lock);
}

static void *test_control_thread_main(void *arg) {
```

— insert the new function between `}` and `static void *test_control_thread_main`.)

Note: `shard_db_test_control_stop` already sets `running = 0`, `release =
1` and broadcasts on the same condvar, so a count worker parked in this
seam at daemon teardown unwinds; no stop-path change is needed.

### 1e. `src/test/fixtures.h` + `src/test/fixtures.c` — runner side

In `fixtures.h`, locate:

```c
int test_env_test_hook_install(TestEnv *env);
int test_env_test_hook_wait(TestEnv *env, int *out_under_kf_wrlock);
```

Replace with:

```c
int test_env_test_hook_install(TestEnv *env);
int test_env_test_hook_install_kind(TestEnv *env, int kind);
int test_env_test_hook_wait(TestEnv *env, int *out_phase);
```

In `fixtures.c`, locate `test_env_test_hook_install`:

```c
int test_env_test_hook_install(TestEnv *env) {
    if (!env || env->test_control_fd < 0) return -1;
    TestHookMessage msg = { .kind = TEST_HOOK_INSTALL, .phase = 0 };
```

Replace with:

```c
int test_env_test_hook_install(TestEnv *env) {
    /* kind 0 = legacy slotcask after-old hook. The kind constants live in
       test_control.c; the runner duplicates them by convention (same as
       the TestHookMessage layout). */
    return test_env_test_hook_install_kind(env, 0);
}

int test_env_test_hook_install_kind(TestEnv *env, int kind) {
    if (!env || env->test_control_fd < 0) return -1;
    TestHookMessage msg = { .kind = TEST_HOOK_INSTALL, .phase = kind };
```

(the remainder of the original function body — write/read/ACK checks and
`return 0;` — is unchanged and stays inside the renamed function).

In `test_env_test_hook_wait`, locate:

```c
    if (rep.kind != TEST_HOOK_REACHED) return -1;
    if (rep.phase != 0 && rep.phase != 1) return -1;
    *out_under_kf_wrlock = rep.phase;
```

Replace with:

```c
    if (rep.kind != TEST_HOOK_REACHED) return -1;
    /* phase: 0 = stale snapshot, 1 = under kf wrlock (after-old hook),
       2 = count worker parked in the pass-1 gap (probe reader held). */
    if (rep.phase < 0 || rep.phase > 2) return -1;
    *out_phase = rep.phase;
```

(If the parameter is still named `out_under_kf_wrlock` in the signature,
rename it to `out_phase` there too — header and definition together.)

### 1f. `src/db/query.c` — the seam call

Add TEST_BUILD includes. Locate the top-of-file include block:

```c
#include "types.h"
#include "slotcask.h"
#include "simd.h"
#include "bitmap.h"
#include "trigram.h"
#include "io_direct.h"
#include "query_internal.h"
#include <math.h>
#include <dirent.h>
```

Replace with:

```c
#include "types.h"
#include "slotcask.h"
#include "simd.h"
#include "bitmap.h"
#include "trigram.h"
#include "io_direct.h"
#include "query_internal.h"
#include <math.h>
#include <dirent.h>
#ifdef TEST_BUILD
#include <stdatomic.h>
#include "shard_test_ctl.h"
#include "test_control.h"
#endif
```

Insert the seam. Locate the end of pass-1's bitmap branch:

```c
            } else {
                /* Bitmaps passed, non-bitmap post-filters need record fetch */
                memcpy(resolved[n_need_fetch].hash, sc->entries[ei].hash, 16);
                resolved[n_need_fetch].sid = found_sid;
                resolved[n_need_fetch].fid = found_fid;
                resolved[n_need_fetch].off = found_off;
                n_need_fetch++;
            }
        }
    }
```

Replace with:

```c
            } else {
                /* Bitmaps passed, non-bitmap post-filters need record fetch */
                memcpy(resolved[n_need_fetch].hash, sc->entries[ei].hash, 16);
                resolved[n_need_fetch].sid = found_sid;
                resolved[n_need_fetch].fid = found_fid;
                resolved[n_need_fetch].off = found_off;
                n_need_fetch++;
            }
        }

#ifdef TEST_BUILD
        /* Regression seam (docs/plans/2026-08-27-shard-count-worker-
         * nested-kf-read.md Task 1): parks the worker after pass-1's
         * inline KF probe — the probe reader is still held here — so the
         * count-gap test can queue a mutation writer on this shard before
         * the batch fetch re-acquires the same kfcache entry. One-shot:
         * disarmed on first hit so every later count passes through. */
        if (atomic_load(&g_shard_test_count_gap) &&
            atomic_fetch_add(&g_shard_test_count_gap_hit, 1) == 0) {
            atomic_store(&g_shard_test_count_gap, 0);
            shard_db_test_control_count_gap_wait();
        }
#endif
    }
```

Task 1 verification:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all        # full suite must stay green
grep -n "shard_test_count_gap" src/db/query.c   # confirm TEST_BUILD-only placement
```

Paste the tail of both commands. The seam is compiled out of the production
binary (guard), so no behavior change is possible; if run-all is not green,
halt and report.

## Task 2 — regression test `test-shard-count-nested-kf-read` (red on base)

Create `src/test/cases/test_shard_count_nested_kf_read.c` with EXACTLY this
content (name mirrors the case file per repo convention):

```c
/* Regression for the shard_count_worker nested kf-rdlock self-deadlock
 * (docs/plans/2026-08-27-shard-count-worker-nested-kf-read.md).
 *
 * Shape: the indexed-count two-pass worker pre-opens its shard's kf reader
 * for the pass-1 bitmap probe and used to carry it into the pass-2 batch
 * fetch. Every batch entry routes to the worker's OWN shard, so
 * slotcask_bulk_fetch_resolved's kf_reval_fetch_one re-acquired the SAME
 * kfcache entry on the same thread. The kfcache rwlocks are writer-
 * preferring NONRECURSIVE (shard_db_internal.h): with a mutation writer
 * queued behind the probe reader, the recursive reader blocks behind the
 * waiter while the waiter waits for the probe reader — a one-thread
 * self-cycle. Pre-fix that is a permanent hang: the count never answers
 * and the update never commits (the timed update join fails ~30s in).
 * Post-fix the worker releases the probe reader before pass-2, the update
 * commits, and the count returns the exact expected total.
 *
 * Determinism: the TEST_BUILD count-gap seam parks the worker after
 * pass-1 (probe reader still held); the test then starts the update,
 * which can only queue on the parked shard's kf wrlock, waits 500 ms so
 * the writer is certainly blocked inside pthread_rwlock_wrlock (that
 * state is sticky — it cannot clear until the worker releases), then
 * releases the park. Watchdog-safe by construction: client-side io
 * timeouts, timed joins, kill-daemon teardown; a wedged daemon never
 * touches the runner process. Red state is Linux/glibc-specific (the
 * non-glibc rwlock fallback tolerates reader recursion silently).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "fixtures.h"
#include "test_client.h"

#include <errno.h>
#include <ftw.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static const char *g_obj = "cntgap";

#define FIXTURE_ROWS 40
#define EXPECT_HITS 12
#define COUNT_IO_TIMEOUT_MS 25000

/* Index-drive leaf: score eq (btree). Bitmap post-filter leaf: stage eq
 * (explicit bitmap index). Record-fetch post-filter leaf: title contains
 * (unindexed). all_postfilters_are_bm == 0, so every bitmap-passing row
 * is fetched in pass-2 — which is where the nested acquire used to
 * happen. n_bm_postfilter > 0 is what pre-opens the probe reader. */
static const char *COUNT_CRITERIA =
    "{\"and\":[{\"field\":\"score\",\"op\":\"eq\",\"value\":\"5\"},"
    "{\"field\":\"stage\",\"op\":\"eq\",\"value\":\"a\"},"
    "{\"field\":\"title\",\"op\":\"contains\",\"value\":\"hit\"}]}";

static int remove_tree_entry(const char *path, const struct stat *st,
                             int typeflag, struct FTW *ftwbuf) {
    (void)st;
    (void)typeflag;
    (void)ftwbuf;
    return remove(path);
}

static int remove_tree(const char *path) {
    if (!path || !path[0]) { errno = EINVAL; return -1; }
    if (nftw(path, remove_tree_entry, 32, FTW_DEPTH | FTW_PHYS) == 0)
        return 0;
    return errno == ENOENT ? 0 : -1;
}

/* Route EVERY row onto kf shard 0 so the single non-empty shard group
 * owns the shard whose kf reader/wrlock collide. */
static int build_fixture(TestEnv *env, int splits,
                         char *out_miss_key, size_t miss_key_sz) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[768], *resp = NULL;

    if (tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
                   &resp) != 0) { tc_close(tc); return -1; }
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":%d,\"streams\":2,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:64\",\"stage:varchar:8\"],"
        "\"indexes\":[\"score\",\"stage:bitmap\"]}", g_obj, splits);
    if (tc_request(tc, req, &resp) != 0 ||
        !SAFE_STRSTR(resp, "\"status\":\"created\"")) {
        free(resp); tc_close(tc); return -1;
    }
    free(resp); resp = NULL;

    int placed = 0, miss_picked = 0;
    out_miss_key[0] = '\0';
    for (int cand = 0; placed < FIXTURE_ROWS; cand++) {
        char k[32];
        snprintf(k, sizeof(k), "r%04d", cand);
        uint8_t h[16];
        compute_hash_raw(k, strlen(k), h);
        if (compute_record_shard(h, splits) != 0) continue;  /* shard 0 only */
        int is_hit = (placed < EXPECT_HITS);
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
            "\"key\":\"%s\",\"value\":{\"score\":5,\"stage\":\"a\","
            "\"title\":\"%s%04d\"}}",
            g_obj, k, is_hit ? "hit" : "miss", placed);
        resp = NULL;
        if (tc_request(tc, req, &resp) != 0 ||
            !SAFE_STRSTR(resp, "\"status\":\"inserted\"")) {
            free(resp); tc_close(tc); return -1;
        }
        free(resp); resp = NULL;
        if (!is_hit && !miss_picked) {
            snprintf(out_miss_key, miss_key_sz, "%s", k);
            miss_picked = 1;
        }
        placed++;
    }
    tc_close(tc);
    return miss_picked ? 0 : -1;
}

static int run_exact_count(TestEnv *env, long *out) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[768];
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
             "\"criteria\":%s}", g_obj, COUNT_CRITERIA);
    char *resp = NULL;
    int crc = tc_request(tc, req, &resp);
    long n = (crc == 0 && resp) ? strtol(resp, NULL, 10) : -1;
    free(resp);
    tc_close(tc);
    *out = n;
    return (n >= 0) ? 0 : -1;
}

typedef struct {
    TestEnv  *env;
    pthread_t tid;
} CountArg;

/* Returns 0 iff the count round-tripped AND returned the exact total.
 * On the base tree this thread's daemon-side worker is wedged, so the
 * client times out and the rc is nonzero. */
static void *count_thread(void *p) {
    CountArg *a = (CountArg *)p;
    TestClientCfg cfg = { .port = a->env->port,
                          .io_timeout_ms = COUNT_IO_TIMEOUT_MS };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return (void *)(intptr_t)-2;
    char req[768];
    snprintf(req, sizeof(req),
             "{\"timeout_ms\":20000,\"mode\":\"count\",\"dir\":\"default\","
             "\"object\":\"%s\",\"criteria\":%s}", g_obj, COUNT_CRITERIA);
    char *resp = NULL;
    int crc = tc_request(tc, req, &resp);
    intptr_t rc = -1;
    if (crc == 0 && resp && !strstr(resp, "\"error\"")) {
        long n = strtol(resp, NULL, 10);
        rc = (n == EXPECT_HITS) ? 0 : -1;
    }
    free(resp);
    tc_close(tc);
    return (void *)rc;
}

typedef struct {
    TestEnv *env;
    char     key[32];
    int      rc;
    TuJoinSignal js;
    pthread_t tid;
} UpdArg;

static void *update_thread(void *p) {
    UpdArg *a = (UpdArg *)p;
    TestClientCfg cfg = { .port = a->env->port,
                          .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { a->rc = -1; tu_join_signal_mark_done(&a->js); return NULL; }
    char req[768], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"%s\","
             "\"key\":\"%s\",\"value\":{\"score\":5,\"stage\":\"a\","
             "\"title\":\"upd\"}}", g_obj, a->key);
    int crc = tc_request(tc, req, &resp);
    a->rc = (crc == 0 && resp && !strstr(resp, "\"error\"")) ? 0 : -1;
    free(resp);
    tc_close(tc);
    tu_join_signal_mark_done(&a->js);
    return NULL;
}

static int test_shard_count_nested_kf_read_run(void) {
    TestEnv env;
    memset(&env, 0, sizeof(env));
    const int splits = 8;
    ASSERT_EQ_INT(test_env_start(&env), 0, "daemon starts");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }

    char miss_key[32];
    ASSERT_EQ_INT(build_fixture(&env, splits, miss_key, sizeof(miss_key)), 0,
                  "build single-shard indexed fixture");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }

    /* Baseline with the seam unarmed: exact count, no park. */
    long base = -1;
    ASSERT_EQ_INT(run_exact_count(&env, &base), 0,
                  "baseline count round-trips");
    if (!t_ctx->failed)
        ASSERT_TRUE(base == EXPECT_HITS, "baseline count is exact");

    if (!t_ctx->failed) {
        ASSERT_EQ_INT(test_env_test_hook_install_kind(&env, 1), 0,
                      "arm count-gap hook");   /* kind 1 = count-worker
                                                pass-1 gap (test_control.c) */
        CountArg ca = { .env = &env };
        ASSERT_EQ_INT(pthread_create(&ca.tid, NULL, count_thread, &ca), 0,
                      "spawn parked count");
        int phase = -1;
        ASSERT_EQ_INT(test_env_test_hook_wait(&env, &phase), 0,
                      "count worker reached the pass-1 gap");
        ASSERT_EQ_INT(phase, 2,
                      "worker parked after pass-1 (probe reader held)");
        if (!t_ctx->failed) {
            UpdArg ua;
            memset(&ua, 0, sizeof(ua));
            ua.env = &env;
            snprintf(ua.key, sizeof(ua.key), "%s", miss_key);
            tu_join_signal_init(&ua.js);
            ASSERT_EQ_INT(pthread_create(&ua.tid, NULL, update_thread, &ua),
                          0, "spawn concurrent update");
            /* The worker holds the probe reader, so the update can only
             * be QUEUED on the kf wrlock. 500 ms makes "blocked inside
             * pthread_rwlock_wrlock" certain; that state is sticky until
             * the worker releases. */
            nanosleep(&(struct timespec){0, 500000000L}, NULL);

            test_env_test_hook_release(&env);

            /* Fixed build: the update acquires the wrlock the moment the
             * (now-released) worker drops the probe reader, commits, and
             * the count fetch completes. Base tree: worker wedged in the
             * nested acquire, update wedged behind it -> join times out. */
            int joined_upd = tu_timed_join(ua.tid, &ua.js, 30);
            ASSERT_EQ_INT(joined_upd, 0,
                          "update finishes after release (base tree: "
                          "wedged behind the recursive reader)");
            if (joined_upd == 0 && !t_ctx->failed) {
                ASSERT_EQ_INT(ua.rc, 0, "update committed");
                void *cres = NULL;
                pthread_join(ca.tid, &cres);
                ASSERT_EQ_INT((int)(intptr_t)cres, 0,
                              "count completed with the exact expected "
                              "total (base tree: client timeout)");
                long after = -1;
                ASSERT_EQ_INT(run_exact_count(&env, &after), 0,
                              "post-window count round-trips");
                if (!t_ctx->failed)
                    ASSERT_TRUE(after == EXPECT_HITS,
                                "totals converge after the window");
            } else {
                /* Wedged threads may still touch ua.js / sockets — leak
                 * them per the timed-join contract (daemon is killed
                 * below either way). */
                pthread_detach(ua.tid);
                pthread_detach(ca.tid);
            }
            if (joined_upd == 0) tu_join_signal_destroy(&ua.js);
        }
        test_env_test_hook_clear(&env);
    }

    test_env_kill(&env);
    char base_path[sizeof(env.db_root)];
    char *slash = strrchr(env.db_root, '/');
    if (slash && slash != env.db_root) {
        size_t base_len = (size_t)(slash - env.db_root);
        memcpy(base_path, env.db_root, base_len);
        base_path[base_len] = '\0';
        ASSERT_EQ_INT(remove_tree(base_path), 0,
                      "remove test fixture tree");
    }
    return t_ctx->failed ? 1 : 0;
}

TEST_REGISTER("test-shard-count-nested-kf-read",
              test_shard_count_nested_kf_read_run)
```

Design notes (invariants, spelled out per CORE-PROCESS):

- **Isolation:** own daemon on a free port + own tmpdir via
  `test_env_start`; no process-local db, no env vars, no fixed paths;
  worker-process default and `--jobs 1` sequential are both safe.
  `test_env_kill` + `remove_tree` teardown; wedged base-tree threads are
  detached, never joined unboundedly.
- **One-shot seam:** the seam disarms itself on first hit, so the
  baseline count (before install), the parked count (the hit), and any
  later counts cannot re-park. `test_env_test_hook_clear` disarms again
  after release.
- **Red detection is time-bounded on both sides:** client io timeout 25 s
  for the count, `tu_timed_join` 30 s for the update, daemon SIGKILL'd at
  teardown. The case can never wedge `run-all`.
- **Why the fixture values are what they are:** `stage` is an explicit
  bitmap-indexed varchar (the production `type=story` shape, dict-encoded
  consistently on insert and query sides); `title contains` is the
  unindexed post-filter that forces record fetches; `score eq` is the
  btree primary leaf. `n_nonbitmap == 1` in the intersect scan, so the
  plan cannot become `FP_INTERSECT` — it stays `FP_PRIMARY_LEAF` with the
  bitmap leaf as a post-filter, which is the exact precondition for the
  probe reader. If the planner ever routes differently, the
  `worker parked after pass-1` wait fails loudly rather than passing
  vacuously.

Run the red proof (fix not yet applied — Task 3 comes next):

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-shard-count-nested-kf-read
```

**Expected on the base tree: FAIL, taking ~35–60 s**, with the failing
assertion `update finishes after release (base tree: wedged behind the
recursive reader)` (and, absent the join guard, the count client would time
out). Paste the complete output into the execution log. If the case PASSES
on the base tree, do NOT proceed: write `PLAN_NOTES.md` with the actual
output and halt (possible causes: planner routed the count off the two-pass
bitmap path, or the seam did not fire — both are plan-invalidating).

## Task 3 — the fix + revert-verify proof

Apply exactly this hunk in `src/db/query.c`. Locate (quoted anchor):

```c
    /* Pass 2: batch fetch all needs-fetch entries */
    if (n_need_fetch > 0) {
```

Insert immediately BEFORE it:

```c
    /* The pass-1 probe reader must die before the batch fetch: every
     * partition the fetcher builds routes to THIS worker's shard, so
     * kf_reval_fetch_one would re-acquire the same kfcache entry on this
     * thread while we still hold it. The kfcache rwlocks are writer-
     * preferring NONRECURSIVE (shard_db_internal.h) — a recursive reader
     * parked behind a queued mutation writer self-deadlocks (worker waits
     * on acquire #2, the writer waits on acquire #1). */
    if (kh.map) {
        kfcache_release(&kh);
        memset(&kh, 0, sizeof(kh));
    }
```

Nothing else changes in the function: the `if (kh.map) kfcache_release
(&kh);` in `cleanup:` stays as the backstop for the `goto cleanup` paths
between the acquire and this release, and the `memset` prevents any
double-release. `sdb` / registry-ref handling is untouched.

Build and run the case:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-shard-count-nested-kf-read
```

**Expected: PASS in a few seconds** (fixture build dominates). Paste the
output.

### Revert-verify proof (both outputs get pasted)

1. Temporarily delete ONLY the 10 inserted lines from Task 3 (the comment
   block plus the `if (kh.map) { ... }`) — do NOT revert the seam or the
   test. Rebuild (`SKIP_TESTS=1 ./build.sh`) and run the case; paste the
   full FAIL output (same wedge signature as Task 2).
2. Re-apply the hunk exactly as written, rebuild, run again; paste the full
   PASS output.

If either direction behaves differently, halt and report — do not tune
timeouts or weaken assertions to make it fit.

## Task 4 — full suite, audit re-check, sanitizer gate

```bash
./build/bin/shard-db-test run-all
grep -n "kfcache_acquire" src/db/query.c src/db/storage.c
grep -n "slotcask_bulk_fetch_resolved\|slotcask_bulk_resolve_and_fetch" src/db/*.c
```

Confirm the greps still match the Call-site audit above (the count worker is
the only caller holding an outer kf handle across a bulk fetch). Then the
AGENTS.md dynamic-safety gate, exactly:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all    # 3 consecutive fresh runs
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all    # 3 consecutive fresh runs
```

All six runs must exit zero with no findings. Paste tails. Also confirm the
build log shows no new compiler warnings and no leftover debug prints from
this work.

## Task 5 — closure

- No documentation sync required: no wire, CLI, config, on-disk, or public
  API surface changes; the fix restores the already-documented invariant in
  `shard_db_internal.h` ("no such recursive acquisition exists today") and
  `docs/concepts/concurrency.md`. Flag to the human if they want a
  changelog line at release time.
- No new dependencies.
- Leave the full diff **uncommitted**; hand back for the review pass with
  the pasted outputs from Tasks 1–4.

## Edge cases & invariants (summary)

- Release placement covers every flow that reaches pass-2 with `kh` held
  (normal pass-1 completion and the deadline-tick `break` inside the pass-1
  loop), and the cleanup backstop covers every `goto cleanup` between
  acquire and release. `memset` after release makes double-release
  impossible.
- The non-bitmap path (`shard_id < 0`) never acquires `kh`; the added
  release is a no-op there.
- The seam is `#ifdef TEST_BUILD`, one-shot, daemon-side, and disarmed by
  CLEAR and by daemon stop; production binaries compile it out entirely.
- `count_batch_cb` fires under the fetcher's own kf reader and does not
  re-enter slotcask/btree APIs — unchanged by this fix.
- The nested `parallel_for_io` fan-out inside `slotcask_bulk_fetch_resolved`
  is unchanged (the fix only releases a handle earlier); the executor
  caution about inline fallback for nested fan-outs therefore does not
  apply to any new code.

## Execution deviations (2026-09-03 run — flagged for review)

Two mechanical repairs were required during Task 1; neither changes the
approved design (same seam semantics, same test, same fix), but both touch
files this plan did not list, so the reviewer should see them here:

1. **Seam fire site moved behind a runtime hook (link-graph repair).**
   Task 1f as written called `shard_db_test_control_count_gap_wait()`
   directly from `query.c` — but `test_control.c` is linked only into the
   `shard-db-test-server` target, while `query.c` is also compiled with
   `-DTEST_BUILD` into the `shard-db-test` runner (and the seam therefore
   broke that link). The codebase's established pattern for exactly this
   is the after-old hook: fire site + function pointer in `slotcask.c`
   (linked into every TEST_BUILD binary), callback installed by
   `test_control.c` at INSTALL time. Final shape: `query.c` calls
   `slotcask_test_count_gap_park()` (defined in `slotcask.c`; checks the
   arm/hit atomics, one-shot disarms, invokes the installed hook);
   `test_control.c` installs `test_control_count_gap_block` (the REACHED/
   cond-wait body) via `shard_db_test_set_count_gap_hook` on INSTALL
   kind 1 and clears it on CLEAR/stop; `test_control.h` is unchanged from
   baseline. Also dropped the never-needed
   `g_shard_test_count_gap_release` atomic from Task 1a (the B1 gap needs
   one because it spins in-process; this seam waits on the control
   channel's condvar instead).
2. **`build.sh` case registration.** The runner target enumerates test
   case files explicitly; `src/test/cases/test_shard_count_nested_kf_read.c`
   was added to that list (one line, after
   `test_stream_find_chunk_resume.c`).
3. **TSan legs ran with `SHARD_TEST_WATCHDOG_SEC=1200` at `--jobs 2`
   (human-directed).** The AGENTS.md local gate line carries no watchdog
   env, and `test-auto-reshard-throttle` needs ~220 s under TSan on this
   machine (verified standalone: 3m41s, 9/9 pass, unrelated to this diff)
   — the default 180 s per-case watchdog killed TSan run 1 at that case.
   CI already runs sanitizers with the 1200 s watchdog. Separately, 16-way
   parallel TSan on this box proved memory-pressure-flaky today (two of
   three runs failed rebuild-path assertions with zero TSan reports,
   different subsets each time, none reproducible standalone; the machine
   also suffered two kernel-level crashes under sanitizer load, the second
   a PID-1 segfault panic — hardware suspected, Memtest86+ recommended).
   Per the human's direction the three TSan legs were rerun at
   `--jobs 2` with the 1200 s watchdog: **3× green, 12,831 passed /
   0 failed each, zero TSan reports.**
