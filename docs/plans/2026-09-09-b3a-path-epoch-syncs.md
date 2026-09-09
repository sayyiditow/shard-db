# B3a — Path-keyed durability epochs (index files + marker dirs)

Date: 2026-09-09 (rev 5 — rev 4 plus execution amendments; see
"Execution amendments (rev 5)" and PLAN_NOTES.md)
Status: draft for review — not approved for execution.
Follows: 2026-09-09-b2-per-shard-pipeline-tasks.md (executed, measurement
verdict MIXED). Part 1 of B3; the second B3 mechanism (dynamic per-shard
window batching for concurrent single-record writes) is explicitly **out
of scope** here and stays parked.

## Review findings addressed (rev 2)

1. **Failure bookkeeping was incorrect** — `failed_upto =
   min(failed_upto ?: target, target)` never advanced the failed
   high-water past the first failure, so requests above it retried
   forever under persistent failure, and `failed_upto = 0` on success
   let stale failure state be wiped while waiters were still evaluating.
   Replaced with monotone `completed` / `failed_upto` result high-waters
   that are never reset and `completed` checked first, plus errno
   propagation via `last_errno` (rev 3 weakens the errno guarantee to
   "a failed round's errno" — finding 8 below). Scenario 5 of the new
   test pins the
   retry-forever failure mode; the rewritten Soundness section states
   the exact return invariants.
2. **The btree sync lost its mutation lock** — the raw op now keeps the
   btree file's writer rwlock (`bt_acquire(path, 1)` … `bt_release`)
   held across the `fdatasync`, byte-identical to the pre-epoch body;
   the epoch changes only who performs the sync (D2).
3. **Registry insertion race** — a process-wide registry mutex now
   guards all `used`/`path` access; entry mutexes guard protocol state
   only. Explicit handling for path-too-long, table-full fallback,
   one-time mutex init (no UB on entry reuse), drop/recreate reuse, and
   reset (D1, "Registry soundness").
4. **Plan format** — every edit site is located by quoted anchor text;
   every new/changed function, struct, and hunk is a complete code
   block; no line-number references remain.
5. **Test registration** — the new test's exact `TEST_REGISTER` line is
   given; it registers `test-durability-epoch-sync` /
   `test_durability_epoch_sync_run` (the reviewed draft's "mirrors"
   example duplicated the existing
   `TEST_REGISTER("test-durability-sync-failures",
   test_durability_msync_injection_run)` registration verbatim).
6. **Test globals** — no bare extern mutable globals; all test state
   lives behind `g_durepo_test_mu` with setter/getter functions,
   mirroring the existing `durability.c` injection-hook pattern.
7. **Regression proof** — Task 2 includes the explicit revert-proof:
   temporarily bypass the epoch in `btree_sync_path`, reproduce the red
   (raw count 4), re-apply, confirm green; both outputs pasted.
   Additionally, the Task-1 red is stated honestly: the new test cannot
   compile before the module exists, so the pasted red for Task 1 is
   the build failure, and scenario 1's discriminating power is proven
   by the Task-2 revert-proof instead.

Rev 3 (second review round):

8. **`last_errno` was not per-round** — a waiter failed by round N
   could wake after round N+1 failed with a different errno and return
   the wrong one. Exactly per-round errno bookkeeping needs per-round
   storage, which a fixed struct cannot give unbounded concurrent
   registrations. Resolution, per the reviewer's alternative: the
   invariant is **explicitly weakened** — a group-failed waiter receives
   an errno from *a* failed round that claimed its registration, and
   under sustained mixed failures on one path it may be a later failed
   round's errno. This is safe because (a) every consumer treats -1 as
   fail-and-recover with diagnostic errno (the index sync issuers
   already document errno tolerance), and (b) the one errno-branching
   consumer — the bitmap commit leg's benign-ENOENT skip — is
   structurally insulated: the module's file raw op converts ENOENT to
   success ("missing file means nothing was ever written"), so an epoch
   failure can never deliver a masquerading ENOENT. The protocol
   semantics section states the weakened guarantee; scenario 6 pins the
   ENOENT→0 behavior.
9. **Tasks 2–4 lacked test-first steps** — Task 2 now opens by
   re-confirming the Task-1 red; Task 3 now begins with a new red test
   (`test-durability-epoch-marker-dir`: two concurrent bulk requests
   through the marker-dir path, asserting the dir-fsync epoch count is
   nonzero — 0 on the pre-D4 tree because the B2 per-request coalescer
   never calls the epoch); Task 4's test-first step is a grep proving
   the stale per-request doc claim exists before the docs edit removes
   it.
10. **Line-number references** — the plan body contains none (verified
    by grep over this file); the two citations in the second review
    (`btree.c` and the durability test registration) appeared in the
    review-pass chat summary, not in this plan. No edit instruction in
    this plan locates a site by line number.
11. **Callback identity was not part of the epoch key** — the registry
    key is now `(domain, path)` with an explicit `DurEpochDomain` per
    raw-op family. This is load-bearing, not hygiene: a btree
    registration must never be covered by a flusher that performs the
    sync without the btree writer rwlock (that was rev 1's finding 2),
    so different-domain callers on the same path can never coalesce
    into one round.
12. **D4 fail-fast needs a human decision** — replacing B2's
    re-arm-and-retry dir-fsync behavior with immediate group-failure is
    a durability/error-semantics tradeoff; Task 3 now has an explicit
    STOP-and-approve checkpoint before those hunks may be applied.

Rev 4 (third review round):

13. **Null path reached the raw callback** — when the registry lookup
    falls back (table full / path too long), a NULL path would have
    flowed straight into the raw op (`open(NULL, …)`). The protocol
    entry point now rejects NULL with EINVAL before any registry or
    raw-op work; scenario 6 asserts it.
14. **Task 4's verification grep was too broad** — `grep -rn
    "per-request" docs/` sweeps historical plan records (never edited)
    and legitimate unrelated uses (per-request `timeout_ms`,
    `MAX_REQUEST_SIZE`, tracing). The gate is now scoped to the current
    architecture trees and to the marker-dir/fsync/coalescing phrasing.

## Execution amendments (rev 5)

Found while executing rev 4 (full details + rationale in PLAN_NOTES.md;
all are mechanical corrections with repo precedent, none change the
protocol's semantics):

a. **Test workers bind thread-local `g_db`.** `g_db` is `__thread` and
   all btree cache state hangs off it (`#define bt_cache
   (g_db->bt_cache)`, shard_db_internal.h:508); spawned test pthreads
   start NULL, so every raw op failed ENODEV. Fix: capture
   `ShardDb *proc_db = g_db` on main and set `g_db = w->db;` as each
   worker's first statement (precedent:
   test_bt_cache_writer_starvation.c). Production is unaffected —
   parallel.c binds `g_db` per pooled task.

b. **New TEST hook `durability_test_epoch_set_register_hold(N)`.** A
   raw start gate cannot make N concurrent callers coalesce
   deterministically: the first thread to take the entry mutex claims
   `target` before its peers register, so late registrants correctly
   drive a second round. The hook holds the next N registrants after
   registration, before claim. In the module's TEST block:

```c
/* Test-only: make the next N registrants of ANY epoch sync wait until
   all N have registered, before any of them evaluates/claims. Turns
   "N concurrent callers join one round" into a deterministic property
   (a plain start gate cannot: the first thread to grab the entry mutex
   claims target before its peers have registered). */
static int g_durepo_test_reg_hold;
static int g_durepo_test_reg_arrived;

static void durepo_test_reg_hold_wait(void) {
    pthread_mutex_lock(&g_durepo_test_mu);
    if (g_durepo_test_reg_hold <= 0) {
        pthread_mutex_unlock(&g_durepo_test_mu);
        return;
    }
    if (++g_durepo_test_reg_arrived >= g_durepo_test_reg_hold)
        pthread_cond_broadcast(&g_durepo_test_reg_cv);
    else
        while (g_durepo_test_reg_arrived < g_durepo_test_reg_hold)
            pthread_cond_wait(&g_durepo_test_reg_cv, &g_durepo_test_mu);
    pthread_mutex_unlock(&g_durepo_test_mu);
}

void durability_test_epoch_set_register_hold(int n) {
    pthread_mutex_lock(&g_durepo_test_mu);
    g_durepo_test_reg_hold = n > 0 ? n : 0;
    g_durepo_test_reg_arrived = 0;
    pthread_mutex_unlock(&g_durepo_test_mu);
}
```

   with `static pthread_cond_t g_durepo_test_reg_cv =
   PTHREAD_COND_INITIALIZER;` added beside `g_durepo_test_mu`, and the
   call site in `durepo_sync` — registration and claim are split so the
   hold runs with NO epoch mutex held:

```c
    pthread_mutex_lock(&e->mu);
    uint64_t my = ++e->requested;
    pthread_mutex_unlock(&e->mu);
#ifdef TEST_BUILD
    /* Registration hold: wait until the test's quota of registrants has
       landed, so the first claimer's target covers all of them. Entered
       WITHOUT any epoch mutex — no deadlock with the protocol. */
    durepo_test_reg_hold_wait();
#endif
    /* The claim/wait loop ALWAYS runs under e->mu in every build. The
       release above is only so the TEST-only hold cannot deadlock other
       registrants; production re-locks immediately. */
    pthread_mutex_lock(&e->mu);
    while (1) {
```

   plus `g_durepo_test_reg_hold`/`..._arrived` zeroed in
   `durability_test_epoch_reset`, and the declaration added to the D5
   TEST block:

```c
void durability_test_epoch_set_register_hold(int n);
```

c. **Scenario 3 uses a regular FILE as parent (ENOTDIR), not a missing
   dir.** `bt_open_file(writer)` runs an ignored `mkdirp(parent)` then
   `open(O_CREAT)` — a missing parent gets created and the sync
   succeeds (observed). The scenario now creates `<dir>/blocker` as a
   file and asserts rc −1 / errno ENOTDIR for the group and the late
   registrant; the test file's shaping helpers collapse to
   `run_workers` + `run_one_round_workers` (register-hold + 25 ms
   delay) and the start gate is gone.

d. **Task 4 gate restated against observation.** The scoped grep
   matches exactly one benign line pre-edit
   (`docs/reference/limits.md:112`, "…**Direction-C compaction**…" in
   the 2026.05 summary — matches `dir`). Gate: post-edit output must be
   baseline-unchanged (that one line) and contain no marker-dir fsync
   claim; verified.

e. **D4 has a fourth `dir_dirty` publish site.** Besides the two call
   sites, the per-window marker-publish step re-arms `req->dir_dirty`
   under `dir_sync_mu` right after `rw->published = 1;`. Removed with
   the others (compiler-enforced: the struct fields are gone). Under
   the epoch, registration at the M barrier IS the dirty mark.

f. **Full-suite gate mode.** All-core `run-all` is not stably green in
   this environment even on `main` (A/B in PLAN_NOTES.md: base 215
   failures across 448 cases, random disjoint subsets per run, /tmp
   tmpfs ENOSPC under parallel worker spawn). The gate used:
   `run-all --jobs 1` plus focused runs; Task 5's sanitizer suites give
   the authoritative signal.

## Measured context (what is actually unclaimed)

The B2 coalescer already makes segment-file syncs coverage-correct: a
barrierer arriving while a flush is in flight waits for it, re-checks the
dirty flag, and skips when a completed sync covered its bytes. Multiple
barrierers in the same dirty window chain onto one flush. The residual
multi-flush under continuous concurrent writing is genuine work — bytes
written *during* a flush genuinely need the next flush. **Segments are
therefore NOT this plan's target.** K is not epoch-able at all (the
writer gate means only one window is ever in flight per kf shard — there
is nobody to group with).

Two sync classes have **zero** cross-writer coalescing today:

1. **Index files (btree + trigram).** Every non-bitmap index sync funnels
   through `btree_sync_path`:

   ```c
   int btree_sync_path(const char *path) {
   #ifdef TEST_BUILD
       atomic_fetch_add(&g_test_btree_sync_count, 1);
   #endif
       BtFile bt;
       if (bt_acquire(&bt, path, 1) != 0) return -1;
       int rc = fdatasync(bt.fd);
       bt_release(&bt);
       return rc;
   }
   ```

   — plain `bt_acquire` + `fdatasync`, one per calling pipeline. At
   splits=8, `index_splits_for(8) = 2`, so 8 shard pipelines of one
   request and the 40 pipelines of five concurrent requests all
   fdatasync the **same 2 files per field**. The bench-kv case has no
   indexes (index_sync n=0), but every indexed ingest pays this:
   N concurrent requests × touched shards × fields × idx-shards syncs
   where `fields × idx-shards` unique files exist. **This is the plan's
   primary win.**
2. **The marker directory.** B2 coalesces dir fsyncs per *request*
   (`SlotcaskBulkRequest.dir_sync_*`); two concurrent requests each pay
   their own M and C dir fsyncs on the same directory.

**Expectation setting for the bench gate:** `bench-kv-parallel` (no
indexes) should move only modestly (dir-fsync sharing and second-order
effects); the indexed-ingest benches (`bench-invoice` / 14-index
parallel) are where B3a must show a clear win. Plan the measurement
accordingly — do not judge B3a by the KV number alone.

## Design

### D1 — The epoch primitive (src/db/durability_epoch.c, new module)

A fixed-capacity open-addressed table keyed by `(domain, path)`, where
the domain identifies the raw-op family (`DurEpochDomain`). Entries
never change identity once installed (a dropped object's entry idles;
table full or path too long → callers fall back to a plain uncoalesced
sync, which is always correct). A single registry mutex guards the
table scan/install (`used` + `domain` + `path`); each entry's own
mutex guards only that entry's protocol state.

Complete module (new file `src/db/durability_epoch.c`):

```c
/* src/db/durability_epoch.c — B3a path-keyed durability epochs.

   Coalesces concurrent fdatasync/fsync calls that target one path.
   Every caller registers a barrier AFTER its writes; the first caller
   with no round in flight claims target = requested and performs the
   raw sync, which covers every request registered up to the claim.
   Concurrent registrants sleep until that round's result covers or
   fails them. Full-file sync semantics make any successful round cover
   every earlier registration on the path, so `completed >= my` is
   durable truth; monotone result high-waters (never reset) mean a
   failed round fails exactly the requests it claimed. A group-failed
   waiter receives an errno from a failed round that claimed it (under
   sustained mixed failures it may be a later round's errno — every
   consumer treats it as diagnostic; see "Protocol semantics"). The
   registry key is (domain, path): different raw-op families on one
   path never coalesce into each other's rounds. Table full / path too
   long → plain uncoalesced sync, which is always correct. */

#include "types.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define DUREPOCH_CAP 256

typedef struct {
    char            path[PATH_MAX];
    unsigned        domain;       /* DurEpochDomain — part of the key     */
    int             used;         /* registry-level: entry installed      */
    pthread_mutex_t mu;           /* guards everything below              */
    pthread_cond_t  cv;
    uint64_t        requested;    /* barriers registered (monotone)       */
    uint64_t        completed;    /* covered by a SUCCESSFUL full-file sync;
                                     monotone, never reset                */
    uint64_t        failed_upto;  /* claimed by a FAILED sync (monotone,
                                     never reset)                         */
    int             last_errno;   /* an errno from the latest failed round */
    int             in_flight;
} DurEpoch;

static DurEpoch g_durepo[DUREPOCH_CAP];
static pthread_mutex_t g_durepo_registry_mu = PTHREAD_MUTEX_INITIALIZER;

/* Entry mutexes/conds are initialized exactly once, up front, under a
   private init mutex — re-installing a recycled entry later must never
   re-run pthread_mutex_init on a live mutex (UB), and per-entry lazy
   init would race the registry scan. ~1.1 MB BSS at CAP=256. */
static pthread_mutex_t g_durepo_init_mu = PTHREAD_MUTEX_INITIALIZER;
static int g_durepo_mu_ready;

static void durepo_init_all(void) {
    pthread_mutex_lock(&g_durepo_init_mu);
    if (!g_durepo_mu_ready) {
        for (int i = 0; i < DUREPOCH_CAP; i++) {
            pthread_mutex_init(&g_durepo[i].mu, NULL);
            pthread_cond_init(&g_durepo[i].cv, NULL);
        }
        g_durepo_mu_ready = 1;
    }
    pthread_mutex_unlock(&g_durepo_init_mu);
}

#ifdef TEST_BUILD
static pthread_mutex_t g_durepo_test_mu = PTHREAD_MUTEX_INITIALIZER;
static int g_durepo_test_delay_ms;
static int g_durepo_test_fail_remaining;
static int g_durepo_test_fail_errno;
static int g_durepo_test_file_syncs;
static int g_durepo_test_dir_syncs;

static void durepo_test_delay(void) {
    int ms;
    pthread_mutex_lock(&g_durepo_test_mu);
    ms = g_durepo_test_delay_ms;
    pthread_mutex_unlock(&g_durepo_test_mu);
    if (ms > 0) usleep((useconds_t)ms * 1000);
}

/* Count one attempted module raw op (file or dir), successful or not. */
static void durepo_test_count(int is_dir) {
    pthread_mutex_lock(&g_durepo_test_mu);
    if (is_dir) g_durepo_test_dir_syncs++;
    else        g_durepo_test_file_syncs++;
    pthread_mutex_unlock(&g_durepo_test_mu);
}

/* One-shot failure injection for the module FILE raw op only. Returns 1
   with *out_err set when this attempt must fail. */
static int durepo_test_consume_fail(int *out_err) {
    pthread_mutex_lock(&g_durepo_test_mu);
    if (g_durepo_test_fail_remaining > 0) {
        g_durepo_test_fail_remaining--;
        *out_err = g_durepo_test_fail_errno;
        pthread_mutex_unlock(&g_durepo_test_mu);
        return 1;
    }
    pthread_mutex_unlock(&g_durepo_test_mu);
    return 0;
}

void durability_test_epoch_set_delay_ms(int ms) {
    pthread_mutex_lock(&g_durepo_test_mu);
    g_durepo_test_delay_ms = ms > 0 ? ms : 0;
    pthread_mutex_unlock(&g_durepo_test_mu);
}

void durability_test_epoch_fail_next(int count, int err) {
    pthread_mutex_lock(&g_durepo_test_mu);
    g_durepo_test_fail_remaining = count > 0 ? count : 0;
    g_durepo_test_fail_errno = err > 0 ? err : EIO;
    pthread_mutex_unlock(&g_durepo_test_mu);
}

int durability_test_epoch_file_sync_count(void) {
    pthread_mutex_lock(&g_durepo_test_mu);
    int n = g_durepo_test_file_syncs;
    pthread_mutex_unlock(&g_durepo_test_mu);
    return n;
}

int durability_test_epoch_dir_sync_count(void) {
    pthread_mutex_lock(&g_durepo_test_mu);
    int n = g_durepo_test_dir_syncs;
    pthread_mutex_unlock(&g_durepo_test_mu);
    return n;
}

/* Test-only: wipes the registry and all test knobs/counters. Call only
   when no durability_epoch_* call is in flight anywhere — the harness's
   default separate-worker mode (and --jobs 1, which runs cases one at a
   time) guarantees that for a case that owns the table. */
void durability_test_epoch_reset(void) {
    pthread_mutex_lock(&g_durepo_test_mu);
    g_durepo_test_delay_ms = 0;
    g_durepo_test_fail_remaining = 0;
    g_durepo_test_fail_errno = 0;
    g_durepo_test_file_syncs = 0;
    g_durepo_test_dir_syncs = 0;
    pthread_mutex_unlock(&g_durepo_test_mu);
    durepo_init_all();
    /* Only place that holds registry_mu while taking an entry mu; no
       protocol path ever takes registry_mu under an entry mu. */
    pthread_mutex_lock(&g_durepo_registry_mu);
    for (int i = 0; i < DUREPOCH_CAP; i++) {
        DurEpoch *e = &g_durepo[i];
        pthread_mutex_lock(&e->mu);
        e->requested = 0;
        e->completed = 0;
        e->failed_upto = 0;
        e->last_errno = 0;
        e->in_flight = 0;
        e->used = 0;
        pthread_mutex_unlock(&e->mu);
    }
    pthread_mutex_unlock(&g_durepo_registry_mu);
}
#endif  /* TEST_BUILD */

/* Registry lookup/install. The registry mutex is the ONLY guard for
   used/domain/path; entry mutexes guard protocol state only. Never
   called while holding any entry mutex. Returns NULL when the path
   does not fit or the table is full — the caller falls back to a plain
   uncoalesced raw sync, which is always correct. Lookup is O(CAP)
   strcmp — noise next to any fdatasync. */
static DurEpoch *durepo_find_or_install(const char *path,
                                        unsigned domain) {
    if (!path || strlen(path) >= PATH_MAX) return NULL;
    DurEpoch *free_slot = NULL;
    pthread_mutex_lock(&g_durepo_registry_mu);
    for (int i = 0; i < DUREPOCH_CAP; i++) {
        DurEpoch *e = &g_durepo[i];
        if (!e->used) {
            if (!free_slot) free_slot = e;
            continue;
        }
        if (e->domain == domain && strcmp(e->path, path) == 0) {
            pthread_mutex_unlock(&g_durepo_registry_mu);
            return e;
        }
    }
    if (free_slot) {
        snprintf(free_slot->path, PATH_MAX, "%s", path);
        free_slot->domain = domain;
        free_slot->requested = 0;
        free_slot->completed = 0;
        free_slot->failed_upto = 0;
        free_slot->last_errno = 0;
        free_slot->in_flight = 0;
        free_slot->used = 1;   /* publish last, under the registry mutex */
    }
    pthread_mutex_unlock(&g_durepo_registry_mu);
    return free_slot;          /* NULL → table full */
}

/* Shared epoch protocol. `raw` performs the actual durability op on the
   CURRENT inode at `path` and owns whatever mutation discipline it
   needs (for btrees: the file's writer rwlock held across fdatasync,
   exactly as on the pre-epoch path). The flusher invokes it while
   holding NO epoch mutex. `domain` separates raw-op families in the
   registry: a btree registration can never be covered by a flusher
   that would sync without the btree writer rwlock. *out_synced (may be
   NULL) is set to 1 iff THIS caller attempted the raw op. */
static int durepo_sync(const char *path, unsigned domain,
                       int (*raw)(const char *path, void *ctx),
                       void *ctx, int *out_synced) {
    if (out_synced) *out_synced = 0;
    if (!path) {                   /* reject before the registry OR the
                                      raw callback — a fallback raw op
                                      must never see a null path */
        errno = EINVAL;
        return -1;
    }
    durepo_init_all();
    DurEpoch *e = durepo_find_or_install(path, domain);
    if (!e) {                          /* table full / path too long */
        int rc = raw(path, ctx);
        if (out_synced) *out_synced = 1;
        return rc;
    }
    pthread_mutex_lock(&e->mu);
    uint64_t my = ++e->requested;
    while (1) {
        if (e->completed >= my) {      /* a successful round covered us  */
            pthread_mutex_unlock(&e->mu);
            return 0;
        }
        if (e->failed_upto >= my) {    /* a failed round claimed us      */
            int err = e->last_errno ? e->last_errno : EIO;
            pthread_mutex_unlock(&e->mu);
            errno = err;               /* an errno from A failed round
                                          that claimed us — see the
                                          weakened errno invariant in
                                          "Protocol semantics" */
            return -1;
        }
        if (!e->in_flight) {
            e->in_flight = 1;
            uint64_t target = e->requested;  /* claim AFTER our own
                                                registration: every
                                                request ≤ target had its
                                                writes before its
                                                registration, hence
                                                before the raw op */
            pthread_mutex_unlock(&e->mu);
#ifdef TEST_BUILD
            durepo_test_delay();
#endif
            int rc = raw(path, ctx);
            int raw_errno = errno;
            if (out_synced) *out_synced = 1;
            pthread_mutex_lock(&e->mu);
            e->in_flight = 0;
            if (rc == 0) {
                if (target > e->completed) e->completed = target;
            } else {
                if (target > e->failed_upto) e->failed_upto = target;
                e->last_errno = raw_errno ? raw_errno : EIO;
            }
            pthread_cond_broadcast(&e->cv);
            continue;                  /* re-evaluate our own my */
        }
        pthread_cond_wait(&e->cv, &e->mu);
    }
}

/* Module raw ops. The dir raw preserves the exact pre-epoch behavior of
   its call sites (errno from the failing syscall reaches the caller;
   close() errors are not surfaced, matching slotcask's fsync_dir
   pattern). The FILE raw encodes the bitmap commit leg's documented
   rationale — "A missing file means nothing was ever written — not an
   error" — by converting ENOENT to success: there are no bytes to
   flush, so a covering round is vacuously true. This also structurally
   insulates the one errno-branching consumer (the bitmap call site's
   `errno != ENOENT` skip) from the weakened group-failure errno: an
   epoch failure on a file path can never deliver ENOENT. */

static int durepo_raw_fdatasync_file(const char *path, void *ctx) {
    (void)ctx;
#ifdef TEST_BUILD
    durepo_test_count(0);
    int injected;
    if (durepo_test_consume_fail(&injected)) {
        errno = injected;
        return -1;
    }
#endif
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno == ENOENT) return 0;   /* nothing was ever written */
        return -1;
    }
    int rc = fdatasync(fd);
    int saved = errno;
    close(fd);
    if (rc != 0) errno = saved;
    return rc;
}

static int durepo_raw_fsync_dir(const char *path, void *ctx) {
    (void)ctx;
#ifdef TEST_BUILD
    durepo_test_count(1);
#endif
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return -1;
    int rc = fsync(fd);
    int saved = errno;
    close(fd);
    if (rc != 0) errno = saved;
    return rc;
}

/* Public API. `domain` must be a distinct DurEpochDomain per raw-op
   family; coalescing only ever happens within one domain. */

int durability_epoch_sync_cb(const char *path, unsigned domain,
                             int (*raw)(const char *path, void *ctx),
                             void *ctx, int *out_synced) {
    if (!raw || domain == DUREPOCH_DOMAIN_NONE) {
        errno = EINVAL;
        return -1;
    }
    return durepo_sync(path, domain, raw, ctx, out_synced);
}

int durability_epoch_fdatasync_path_ex(const char *path, int *out_synced) {
    return durepo_sync(path, DUREPOCH_DOMAIN_FILE,
                       durepo_raw_fdatasync_file, NULL, out_synced);
}

int durability_epoch_fsync_dir(const char *path) {
    return durepo_sync(path, DUREPOCH_DOMAIN_DIR,
                       durepo_raw_fsync_dir, NULL, NULL);
}
```

#### Protocol semantics (what each return value means)

These invariants replace the reviewed draft's looser "every request ≤
target fails together" claim, which the protocol does not and need not
provide:

- **No false success.** rc == 0 ⟹ a successful full-file raw sync ran
  for a target T ≥ my claimed after my's registration. Barrier
  registrations happen after the caller's writes (call-site
  discipline), so all of this caller's bytes precede the claim, hence
  the successful sync — every byte is durable. `completed` is monotone
  and never reset, so this holds even across intervening failures: a
  request from a previously failed round that observes
  `completed >= my` after a later successful round genuinely has its
  bytes on disk (the later full-file sync flushed them).
- **No false failure; weakened errno (rev 3).** rc == -1 ⟹
  `completed < my` (no successful sync has covered this registration
  yet — the caller's bytes are not known-durable) and `failed_upto >=
  my` (a round that claimed ≥ my failed). Failing is always safe. The
  errno is **an** errno from a failed round that claimed this
  registration — *not* guaranteed to be the first one: `last_errno` is
  a single slot, so a waiter failed by round N may observe round N+1's
  errno if a later mixed failure lands before it wakes. Exactly
  per-round errno bookkeeping would need per-registration storage,
  which a fixed struct cannot provide; instead the guarantee is
  explicitly weakened and made safe: every consumer treats -1 as
  fail-and-recover with diagnostic errno (the index sync issuers
  already document errno tolerance — "real errno, or EIO if none was
  captured"), and the one errno-branching consumer — the bitmap
  commit leg's benign-`ENOENT` skip — is insulated because the
  module's file raw op converts ENOENT to success, so an epoch
  failure can never deliver a masquerading ENOENT.
- **Domain separation.** The registry key is `(domain, path)`; rounds
  only ever coalesce registrations of the same `DurEpochDomain`. This
  is load-bearing: a btree registration must never be covered by a
  flusher whose raw op would sync without holding the btree writer
  rwlock (torn-mutation hazard), and a dir-fsync round can never be
  "covered" by a file fdatasync or vice versa.
- **Group failure, best-effort.** After a failed round with target T,
  every registrant with my ≤ T returns -1 (with a failed round's
  errno, per the weakened guarantee above) — unless a later successful
  round completes first, in which case rc == 0 is durably correct by
  the first invariant. No request may report durability that a
  successful full-file sync has not covered.
- **Termination (the fix for finding 1).** Both high-waters are
  monotone and never reset. Every round ends with `completed = T` or
  `failed_upto = T` where T is that round's claim; every live
  registrant with my ≤ T returns on its next loop pass; a registrant
  with my > T eventually drives its own round (nobody else is in
  flight), whose result covers or fails it. There is no state in which
  a caller retries forever: the reviewed draft's `min(failed_upto ?:
  target, target)` froze the failed high-water at its first value and
  made requests above it re-claim rounds indefinitely under persistent
  failure; monotone `max`-style advancement makes the driver's own
  round always terminate it. Scenario 5 of the new test pins this
  (a stale-high-water implementation hangs the test).
- **Waits-for graph.** A waiter blocks only on its entry's cv and holds
  no other lock (call sites are lock-free at barrier time). The
  flusher releases the entry mutex before the raw op, so the raw op's
  own discipline (e.g. the btree file's writer rwlock) is never held
  together with the entry mutex, and never with the registry mutex.
  The only registry→entry nesting is `durability_test_epoch_reset`;
  no protocol path takes the registry mutex under an entry mutex. No
  cycles.

#### Registry soundness (finding 3)

- **Insertion race:** `used`, `domain`, and `path` are read/written
  only under `g_durepo_registry_mu`; a lookup and an install can never
  interleave (install writes `path`/`domain` fully, then sets
  `used = 1`, both inside the mutex — publication is atomic to all
  other registry holders). Entry protocol fields have their own mutex;
  the registry mutex is released before any entry mutex is taken.
- **Mutex lifetime:** all entry mutexes/conds are initialized exactly
  once up front under `g_durepo_init_mu`, so recycling an entry after
  `durability_test_epoch_reset` never re-initializes a live mutex.
- **Path too long / NULL:** `strlen(path) >= PATH_MAX` → NULL → plain
  uncoalesced sync (all current call sites pass PATH_MAX-sized
  buffers, so this is defensive only; the `snprintf` install cannot
  truncate a path that was accepted). A NULL path never reaches the
  registry or the raw callback at all: the protocol entry rejects it
  with EINVAL first (scenario 6 asserts both the rc and the errno).
- **Table full:** NULL → plain uncoalesced sync. Never an error, never
  a durability change — the raw op is identical to today's.
- **Path reuse / drop-recreate:** an entry is never removed; a dropped
  object's entry idles holding stale counters. A recreated object at
  the same path reuses that entry — still sound: counters only move
  forward, so `completed >= my` can only be satisfied by a successful
  raw sync that ran after THIS registration, and the raw op syncs
  whatever inode currently lives at the path. Concurrent
  drop-vs-traffic on one object is excluded by the existing per-object
  rwlock (schema mutations take it exclusive), so a registrant's bytes
  and the synced inode cannot belong to different file generations.
- **Reset:** test-only, documented single-owner discipline (the test
  harness runs cases in isolation by default, and `--jobs 1` runs them
  one at a time).

### D2 — Wire the index choke point (keeps the btree writer lock)

`btree_sync_path` keeps its signature, call sites, and TEST-counter
semantics (sequential single-op counts unchanged — `btree_test_sync_count()`
is asserted to exact per-op values by `test_single_op_index_sync`, and
every sequential call still performs its own raw sync, because a new
registration's `my` strictly exceeds the previous round's `completed`).
The load-bearing change from the reviewed draft: the raw op is now the
entire pre-epoch body — including the writer-rwlock hold across
`fdatasync` — so concurrent btree mutations (which take the same
per-file wrlock) can never interleave with the flush, and grow-time
remap propagation via `bt_release` is unchanged. The epoch decides only
*who* runs that body: waiters for the same path never touch the file at
all.

In `src/db/btree.c`, replace the entire existing function

```c
int btree_sync_path(const char *path) {
#ifdef TEST_BUILD
    atomic_fetch_add(&g_test_btree_sync_count, 1);
#endif
    BtFile bt;
    if (bt_acquire(&bt, path, 1) != 0) return -1;
    int rc = fdatasync(bt.fd);
    bt_release(&bt);
    return rc;
}
```

with:

```c
/* B3a raw leg: the sync itself keeps the exact pre-epoch discipline —
   the file's writer rwlock is held across fdatasync, so concurrent
   btree mutations (which need the same wrlock) can never interleave
   with the flush, and bt_release still propagates grow-time remaps.
   The epoch picks one flusher per registration window; concurrent
   same-path syncers wait on the epoch instead of queueing on the
   wrlock to perform redundant fdatasyncs. */
static int btree_sync_path_raw(const char *path, void *ctx) {
    (void)ctx;
    BtFile bt;
    if (bt_acquire(&bt, path, 1) != 0) return -1;
    int rc = fdatasync(bt.fd);
    bt_release(&bt);
    return rc;
}

int btree_sync_path(const char *path) {
#ifdef TEST_BUILD
    int synced = 0;
    int rc = durability_epoch_sync_cb(path, DUREPOCH_DOMAIN_BTREE,
                                      btree_sync_path_raw, NULL, &synced);
    if (synced) atomic_fetch_add(&g_test_btree_sync_count, 1);
    return rc;
#else
    return durability_epoch_sync_cb(path, DUREPOCH_DOMAIN_BTREE,
                                    btree_sync_path_raw, NULL, NULL);
#endif
}
```

Counter semantics: `synced == 1` iff this caller attempted the raw op
(success or failure), matching the pre-epoch at-entry counting for
every sequential path. Coalesced waiters do not count (they performed
no sync) — that is the coalescing the new test asserts.

Because both index issuers (`index_sync_path_set`'s
`path_sync_thread_fn`, whose body is

```c
static void *path_sync_thread_fn(void *p) {
    PathSyncArg *a = (PathSyncArg *)p;
    a->rc = btree_sync_path(a->path);
    a->err = a->rc != 0 ? errno : 0;
    return NULL;
}
```

and `index_sync_record_fields`'s `idx_sync_thread_fn`, whose body is

```c
static void *idx_sync_thread_fn(void *p) {
    IdxSyncArg *a = (IdxSyncArg *)p;
    a->rc = btree_sync_path(a->path);
    a->err = a->rc != 0 ? errno : 0;
    return NULL;
}
```

) call `btree_sync_path`, every btree + trigram sync in the process
coalesces with no further call-site changes — including the two direct
`btree_sync_path` calls in the add-index flush path and both OOM
serial fallbacks, all of which appear as

```c
    a->rc = btree_sync_path(a->path);
```

or

```c
            if (btree_sync_path(path) != 0) {
```

at their call sites in `src/db/index.c`.

### D3 — Wire the bitmap site

`slotcask.c`'s plain-fd helper (the bitmap commit flush leg) routes
through the same primitive. The bitmap site's own comment — "Plain-fd
fdatasync: the dirty bitmap pages belong to the inode … syncing any fd
of the file is equivalent … A missing file means nothing was ever
written — not an error." — moves INTO the raw op: the module's file raw
converts ENOENT to success, so a missing bitmap file yields rc 0 from
`fdatasync_path` exactly as the call site's `errno != ENOENT` skip
produces today. Two deliberate consequences:

1. The call site's `errno != ENOENT` branch becomes **vestigial** — it
   is kept unchanged (smaller diff, and it stays correct as defense in
   depth), but on the B3a tree `fdatasync_path` can no longer return
   -1/ENOENT, so the branch is unreachable.
2. On the never-in-practice missing-file path, the call site now falls
   through to `__atomic_add_fetch(&g_commit_index_sync_ops_total, 1, …)`
   and counts the op, where today the `continue` skips the count. The
   commit-phase metric may differ by that one count on that path; the
   durability behavior is identical.

This relocation is also what makes the weakened group-failure errno
safe (see "Protocol semantics"): an epoch failure on a file path can
never deliver ENOENT, so a later round's errno can never masquerade as
the benign skip.

In `src/db/slotcask.c`, replace

```c
/* fdatasync a file by path without touching any cache. */
static int fdatasync_path(const char *path) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    int rc = fdatasync(fd);
    int saved = errno;
    close(fd);
    if (rc != 0) errno = saved;
    return rc;
}
```

with:

```c
/* fdatasync a file by path without touching any cache. B3a: routed
   through the path-keyed epoch so concurrent commit flushes syncing
   the same bitmap file coalesce. The raw op converts ENOENT to success
   (missing file = nothing ever written), so this returns 0 for a
   missing file — the call site's errno != ENOENT skip is now
   vestigial but kept unchanged. */
static int fdatasync_path(const char *path) {
    return durability_epoch_fdatasync_path_ex(path, NULL);
}
```

The call site keeps its existing form:

```c
            if (fdatasync_path(bpath) != 0) {
                if (errno != ENOENT) { free(all); return -1; }
                continue;
            }
```

— no change needed there.

### D4 — Marker-dir fsyncs: per-request → process-wide

Replace the B2 per-request coalescer with the shared dir epoch. The
epoch subsumes the `dir_dirty` flag (a registration *is* the dirty
mark; a round's completion is the clean), and the `any` published /
`any_unlinked` guards stay at the call sites, so requests that
published nothing still never barrier.

Behavior delta vs. B2 (deliberate): B2's waiters re-armed `dir_dirty`
and retried after a failed round; under the epoch a failed round
group-fails its registrants immediately with the raw errno. Both end
in pipeline failure → markers retained → startup forward-replay; the
epoch fails fast instead of re-syncing inline. The
`SHARD_TEST_NOTE_SYNC` probes stay at the call sites (they fire
per-request *after* the epoch call returns), so scenarios 10/11's
failure injection is unaffected. `slotcask.c`'s static `fsync_dir`
helper keeps its other callers; only the B2 helper's use of it goes
away.

Edits, all in `src/db/slotcask.c`:

**(a)** In `struct SlotcaskBulkRequest`, replace

```c
    int         any_pending;           /* any shard retained markers       */
    int         any_failed;            /* any shard errored or pending     */
    int         saved_errno;           /* first hard failure's errno       */
    /* B2: marker-dir fsync coalescing — concurrent pipelines publish and
       clear against one shared data/kf dir; the fsync runs once and
       every waiter is covered. */
    pthread_mutex_t dir_sync_mu;
    pthread_cond_t  dir_sync_cv;
    int             dir_dirty;
    int             dir_sync_in_flight;
};
```

with:

```c
    int         any_pending;           /* any shard retained markers       */
    int         any_failed;            /* any shard errored or pending     */
    int         saved_errno;           /* first hard failure's errno       */
};
```

**(b)** In `slotcask_bulk_request_begin`, replace

```c
    SlotcaskBulkRequest *req = calloc(1, sizeof(*req));
    if (!req) return NULL;
    pthread_mutex_init(&req->dir_sync_mu, NULL);
    pthread_cond_init(&req->dir_sync_cv, NULL);
    req->db = db;
```

with:

```c
    SlotcaskBulkRequest *req = calloc(1, sizeof(*req));
    if (!req) return NULL;
    req->db = db;
```

**(c)** In `slotcask_bulk_request_end`, replace

```c
static void slotcask_bulk_request_end(SlotcaskBulkRequest *req) {
    if (!req) return;
    pthread_mutex_destroy(&req->dir_sync_mu);
    pthread_cond_destroy(&req->dir_sync_cv);
    free(req->shards);
```

with:

```c
static void slotcask_bulk_request_end(SlotcaskBulkRequest *req) {
    if (!req) return;
    free(req->shards);
```

**(d)** Delete the entire B2 helper, from its comment

```c
/* B2: coalesced fsync(data/kf dir). Concurrent pipelines publish and
   clear against one shared directory; the fsync runs once per dirty
   episode and every concurrent fsync caller is covered by it (claim-
   before-IO: callers arriving during the IO see dir_dirty re-set by the
   publishers/clears themselves and run their own — a no-op on a clean
   dir is cheap). */
static int slotcask_bulk_request_fsync_dir_coalesced(
    SlotcaskBulkRequest *req) {
    pthread_mutex_lock(&req->dir_sync_mu);
    if (!req->dir_dirty) {
        pthread_mutex_unlock(&req->dir_sync_mu);
        return 0;
    }
    while (req->dir_sync_in_flight)
        pthread_cond_wait(&req->dir_sync_cv, &req->dir_sync_mu);
    if (!req->dir_dirty) {
        pthread_mutex_unlock(&req->dir_sync_mu);
        return 0;
    }
    req->dir_sync_in_flight = 1;
    req->dir_dirty = 0;
    pthread_mutex_unlock(&req->dir_sync_mu);
    int rc = fsync_dir(req->kf_dir);
    pthread_mutex_lock(&req->dir_sync_mu);
    req->dir_sync_in_flight = 0;
    if (rc != 0) req->dir_dirty = 1;
    pthread_cond_broadcast(&req->dir_sync_cv);
    pthread_mutex_unlock(&req->dir_sync_mu);
    return rc;
}
```

through its closing brace (the following
`/* M barrier for shard kf_shard_id: … */` comment stays).

**(e)** In `slotcask_bulk_shard_flush_marker_dir`, replace

```c
    if (!any) return 0;
    int rc = slotcask_bulk_request_fsync_dir_coalesced(req);
```

with:

```c
    if (!any) return 0;
    int rc = durability_epoch_fsync_dir(req->kf_dir);
```

**(f)** In the batched-clear step of the shard commit flush, replace

```c
    if (any_unlinked) {
        pthread_mutex_lock(&req->dir_sync_mu);
        req->dir_dirty = 1;
        pthread_mutex_unlock(&req->dir_sync_mu);
    }
    int dir_rc = 0;
    uint64_t t0c = now_us();
    if (any_unlinked &&
        slotcask_bulk_request_fsync_dir_coalesced(req) != 0) dir_rc = -1;
```

with:

```c
    int dir_rc = 0;
    uint64_t t0c = now_us();
    if (any_unlinked &&
        durability_epoch_fsync_dir(req->kf_dir) != 0) dir_rc = -1;
```

### D5 — types.h declarations

In `src/db/types.h`, immediately after the block

```c
/* Returns 0 after a durable replacement, 1 if rename succeeded but the
   parent-directory sync failed, and -1 before rename. */
int durability_publish_replace(const char *target, const char *tmp_path,
                               durability_after_rename_fn after_rename,
                               void *after_rename_ctx);
```

and immediately before

```c
void durability_test_pause(const char *data_dir, const char *phase);
```

insert:

```c
/* ── Path-keyed durability epochs (B3a) ────────────────────────────────
   Coalesce concurrent fdatasync/fsync calls on one path: barrierers
   register after their writes; one flusher per registration window
   performs the raw sync (owning whatever mutation discipline it needs,
   e.g. the btree writer rwlock); overlapping callers wait for that
   round's result. Returns 0 only when a successful full-file sync
   covered this registration; group-fails otherwise (errno: A failed
   round's errno — diagnostic, see docs/concepts/concurrency.md).
   Coalescing only happens within one DurEpochDomain. Table-full /
   path-too-long falls back to an uncoalesced raw call. */
typedef enum {
    DUREPOCH_DOMAIN_NONE  = 0,  /* invalid — EINVAL                     */
    DUREPOCH_DOMAIN_BTREE = 1,  /* btree/trigram idx: raw holds the file
                                   writer rwlock across fdatasync       */
    DUREPOCH_DOMAIN_FILE  = 2,  /* plain-fd fdatasync (bitmap commit leg;
                                   ENOENT → success, nothing written)   */
    DUREPOCH_DOMAIN_DIR   = 3   /* directory fsync (marker dir M/C)      */
} DurEpochDomain;

int durability_epoch_sync_cb(const char *path, unsigned domain,
                             int (*raw)(const char *path, void *ctx),
                             void *ctx, int *out_synced);
int durability_epoch_fdatasync_path_ex(const char *path, int *out_synced);
int durability_epoch_fsync_dir(const char *path);
#ifdef TEST_BUILD
void durability_test_epoch_set_delay_ms(int ms);
void durability_test_epoch_fail_next(int count, int err);
int  durability_test_epoch_file_sync_count(void);
int  durability_test_epoch_dir_sync_count(void);
void durability_test_epoch_reset(void);
#endif
```

## Call-site / consumer inventory

- `btree_sync_path` (btree.c, declared in btree.h) — body swap (D2).
  All callers inherit coalescing:
  - `index.c` `idx_sync_thread_fn` + its OOM serial fallback
    (`index_sync_record_fields` — single-record indexed writes),
  - `index.c` `path_sync_thread_fn` + its OOM serial fallback
    (`index_sync_path_set` — bulk window flush),
  - the two direct btree/trigram flush calls in `index.c`'s
    parallel-build flush.
- `btree_test_sync_reset` / `btree_test_sync_count` (btree.c, TEST) —
  kept; only consumer is `test_single_op_index_sync`, which asserts
  exact sequential per-op counts (preserved — sequential registrations
  always drive their own round).
- `fdatasync_path` (slotcask.c, static) — body swap (D3); exactly one
  caller: the bitmap leg of the shard commit flush, whose
  `errno != ENOENT` skip is preserved via `last_errno` propagation.
- `slotcask_bulk_request_fsync_dir_coalesced` (slotcask.c, static) —
  deleted (D4); exactly two callers rewired
  (`slotcask_bulk_shard_flush_marker_dir`, the batched-clear step).
  `fsync_dir` (slotcask.c, static) keeps its five other callers.
- `SlotcaskBulkRequest` — loses the four B2 dir-sync fields (D4a).
- `build.sh` — `src/db/durability_epoch.c` added next to **every**
  `src/db/durability.c` occurrence (five sites: the daemon gcc line,
  `LIB_SRCS`, the test-binary gcc line, the test-binary source list,
  the bench-binary source list — the lists are explicit, not globbed,
  and every list that links `btree.c`/`slotcask.c` needs the new
  module). The new test case file is added to the test-case list.
- `types.h` — epoch declarations (D5).
- New tests: `src/test/cases/test_durability_epoch_sync.c` (Task 1)
  and `src/test/cases/test_durability_epoch_marker_dir.c` (Task 3).
- Docs: `docs/concepts/concurrency.md` (coalescing bullet),
  `docs/reference/changelog.md` Unreleased.
- Segment coalescer (`bulk_seg_sync_one_group`), K barrier, wave/legacy
  paths — untouched.
- No wire/on-disk format changes; no external consumers of the new
  symbols outside the daemon + test binaries.

## Tasks

Execution rules from the B1/B2 plans apply: work on a fresh branch off
main (e.g. `perf/b3a-path-epoch-syncs`), work left **uncommitted** for
review, tasks in order, if a quoted anchor isn't found exactly write
`PLAN_NOTES.md` describing the mismatch and halt the entire execution
run immediately, stop-and-ask on uncovered decisions, RTK-prefixed
commands, executor never runs benches, sanitizer gate human-run.
Build: `rtk proxy env SKIP_TESTS=1 ./build.sh`; focused:
`rtk proxy ./build/bin/shard-db-test run <name>`; suite:
`rtk proxy ./build/bin/shard-db-test run-all`.

### Task 1 — Test-first: the epoch test, red

Create `src/test/cases/test_durability_epoch_sync.c` with **exactly**
this registration at the end (do not duplicate any existing
registration):

```c
TEST_REGISTER("test-durability-epoch-sync", test_durability_epoch_sync_run)
```

and add the file to `build.sh`'s test-case list — after

```c
    src/test/cases/test_durability_sync_failures.c \
```

insert

```c
    src/test/cases/test_durability_epoch_sync.c \
```

Complete test file:

```c
/* src/test/cases/test_durability_epoch_sync.c
 *
 * B3a path-keyed durability epochs: concurrent same-path syncs coalesce
 * into one raw op; sequential registrations never skip (a completed
 * sync is not reused by a later registration); a failed round
 * group-fails exactly its registrants with a failed round's errno, a
 * late registrant after a failed round fails promptly (monotone failed
 * high-water — a stale-high-water implementation retries forever
 * here); an injected failure group-fails and the path recovers on the
 * next round; a missing file is success for the FILE raw (bitmap-leg
 * rationale); the dir variant coalesces the same way.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

extern void btree_test_sync_reset(void);
extern int  btree_test_sync_count(void);

/* Portable start gate — Apple libc has no pthread_barrier_t. */
typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int             need;
    int             arrived;
} StartGate;

static void start_gate_init(StartGate *g, int need) {
    pthread_mutex_init(&g->mu, NULL);
    pthread_cond_init(&g->cv, NULL);
    g->need = need;
    g->arrived = 0;
}

static void start_gate_wait(StartGate *g) {
    pthread_mutex_lock(&g->mu);
    if (++g->arrived >= g->need) pthread_cond_broadcast(&g->cv);
    else while (g->arrived < g->need) pthread_cond_wait(&g->cv, &g->mu);
    pthread_mutex_unlock(&g->mu);
}

/* Re-arm between scenarios; race-free because run_workers() joins all
   workers before returning. */
static void start_gate_reset(StartGate *g) {
    pthread_mutex_lock(&g->mu);
    g->arrived = 0;
    pthread_mutex_unlock(&g->mu);
}

typedef enum { EPOCH_OP_BTREE, EPOCH_OP_FILE, EPOCH_OP_DIR } EpochOp;

typedef struct {
    StartGate *gate;
    char       path[PATH_MAX];
    EpochOp    op;
    int        rc;
    int        err;
} EpochWorker;

static void *epoch_worker_fn(void *p) {
    EpochWorker *w = p;
    start_gate_wait(w->gate);
    switch (w->op) {
    case EPOCH_OP_BTREE:
        w->rc = btree_sync_path(w->path);
        break;
    case EPOCH_OP_FILE:
        w->rc = durability_epoch_fdatasync_path_ex(w->path, NULL);
        break;
    case EPOCH_OP_DIR:
        w->rc = durability_epoch_fsync_dir(w->path);
        break;
    }
    w->err = w->rc != 0 ? errno : 0;
    return NULL;
}

static void run_workers(EpochWorker *w, int n) {
    pthread_t th[4];
    for (int i = 0; i < n; i++)
        ASSERT_EQ_INT(pthread_create(&th[i], NULL, epoch_worker_fn, &w[i]),
                      0, "spawn epoch worker");
    for (int i = 0; i < n; i++)
        pthread_join(th[i], NULL);
}

static int test_durability_epoch_sync_run(void) {
    char cache_dir[] = "/tmp/shard-db-durepo-XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(cache_dir), "create epoch fixture dir");
    if (!cache_dir[0]) return 1;
    slotcask_shutdown();
    slotcask_init(16, 16);   /* bt_cache must exist: writer bt_acquire
                                without it fails ENODEV */

    char bt_path[PATH_MAX];
    snprintf(bt_path, sizeof(bt_path), "%s/value.idx", cache_dir);
    uint8_t hash[16] = {0};
    btree_insert(bt_path, "value", 5, hash);

    StartGate gate;
    EpochWorker w[4];

    /* 1) Four concurrent same-path btree syncs → ONE raw op.
       The delay pins all four registrations into one in-flight round;
       on a naive (uncoalesced) implementation the count is 4 regardless
       of timing. Discriminating power for this scenario is proven
       directly by Task 2's revert-proof. */
    durability_test_epoch_reset();
    durability_test_epoch_set_delay_ms(25);
    btree_test_sync_reset();
    start_gate_init(&gate, 4);
    for (int i = 0; i < 4; i++) {
        w[i].gate = &gate;
        snprintf(w[i].path, sizeof(w[i].path), "%s", bt_path);
        w[i].op = EPOCH_OP_BTREE;
        w[i].rc = 0; w[i].err = 0;
    }
    run_workers(w, 4);
    for (int i = 0; i < 4; i++)
        ASSERT_EQ_INT(w[i].rc, 0, "coalesced same-path sync succeeds");
    ASSERT_EQ_INT(btree_test_sync_count(), 1,
                  "four concurrent same-path syncs perform one raw op");

    /* 2) No over-skipping: strictly sequential syncs each drive their
       own round (test_single_op_index_sync's exact counts depend on
       this). */
    durability_test_epoch_reset();
    btree_test_sync_reset();
    ASSERT_EQ_INT(btree_sync_path(bt_path), 0, "sequential sync 1");
    ASSERT_EQ_INT(btree_sync_path(bt_path), 0, "sequential sync 2");
    ASSERT_EQ_INT(btree_sync_path(bt_path), 0, "sequential sync 3");
    ASSERT_EQ_INT(btree_test_sync_count(), 3,
                  "sequential registrations each perform a raw op");

    /* 3) Group failure + late registrant. The raw op fails ENOENT (the
       parent dir does not exist; btree bt_acquire O_CREAT cannot create
       it). All four fail with the raw errno — today's independent syncs
       also fail 4/4, so this pins that grouping must not turn failures
       into successes. The trailing sequential call is the monotone
       high-water pin: bookkeeping that freezes the failed high-water at
       its first value (min-style) would retry this call forever. */
    durability_test_epoch_reset();
    durability_test_epoch_set_delay_ms(25);
    char missing_path[PATH_MAX];
    snprintf(missing_path, sizeof(missing_path), "%s/nope/missing.idx",
             cache_dir);
    start_gate_reset(&gate);
    for (int i = 0; i < 4; i++) {
        w[i].gate = &gate;
        snprintf(w[i].path, sizeof(w[i].path), "%s", missing_path);
        w[i].op = EPOCH_OP_BTREE;
        w[i].rc = 0; w[i].err = 0;
    }
    run_workers(w, 4);
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ_INT(w[i].rc, -1, "failed round group-fails");
        ASSERT_EQ_INT(w[i].err, ENOENT, "group failure keeps raw errno");
    }
    errno = 0;
    ASSERT_EQ_INT(btree_sync_path(missing_path), -1,
                  "late registrant after a failed round fails promptly");
    ASSERT_EQ_INT(errno, ENOENT, "late registrant sees the raw errno");

    /* 4) Dir variant coalesces the same way. */
    durability_test_epoch_reset();
    durability_test_epoch_set_delay_ms(25);
    start_gate_reset(&gate);
    for (int i = 0; i < 4; i++) {
        w[i].gate = &gate;
        snprintf(w[i].path, sizeof(w[i].path), "%s", cache_dir);
        w[i].op = EPOCH_OP_DIR;
        w[i].rc = 0; w[i].err = 0;
    }
    run_workers(w, 4);
    for (int i = 0; i < 4; i++)
        ASSERT_EQ_INT(w[i].rc, 0, "coalesced dir sync succeeds");
    ASSERT_EQ_INT(durability_test_epoch_dir_sync_count(), 1,
                  "four concurrent same-dir syncs perform one raw op");

    /* 5) Injected failure: one round fails EIO and group-fails all four
       with the injected errno; the next round succeeds and the failure
       does not stick. Raw-op count == 2 proves one round per
       registration window. */
    durability_test_epoch_reset();
    char plain_path[] = "/tmp/shard-db-durepo-file-XXXXXX";
    int pfd = mkstemp(plain_path);
    ASSERT_TRUE(pfd >= 0, "create plain-file fixture");
    if (pfd >= 0) close(pfd);
    durability_test_epoch_fail_next(1, EIO);
    /* The delay is load-bearing here too: reset() cleared it, and
       without a held-open round a slow worker could register after the
       failed round completed and drive a second (successful) round,
       making the all-fail assertion flaky. */
    durability_test_epoch_set_delay_ms(25);
    start_gate_reset(&gate);
    for (int i = 0; i < 4; i++) {
        w[i].gate = &gate;
        snprintf(w[i].path, sizeof(w[i].path), "%s", plain_path);
        w[i].op = EPOCH_OP_FILE;
        w[i].rc = 0; w[i].err = 0;
    }
    run_workers(w, 4);
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ_INT(w[i].rc, -1, "injected failure group-fails");
        ASSERT_EQ_INT(w[i].err, EIO, "injected errno reaches all four");
    }
    ASSERT_EQ_INT(durability_test_epoch_file_sync_count(), 1,
                  "failed registration window performed one raw op");
    start_gate_reset(&gate);
    for (int i = 0; i < 4; i++) {
        w[i].gate = &gate;
        snprintf(w[i].path, sizeof(w[i].path), "%s", plain_path);
        w[i].op = EPOCH_OP_FILE;
        w[i].rc = 0; w[i].err = 0;
    }
    run_workers(w, 4);
    for (int i = 0; i < 4; i++)
        ASSERT_EQ_INT(w[i].rc, 0, "next round recovers");
    ASSERT_EQ_INT(durability_test_epoch_file_sync_count(), 2,
                  "recovery is one more round, not per-caller retries");
    unlink(plain_path);

    /* 6) Missing file (bitmap-leg rationale): the FILE raw converts
       ENOENT to success — nothing was ever written, so a covering
       round is vacuously true. This is also what keeps the weakened
       group-failure errno away from the bitmap call site's
       errno != ENOENT skip. */
    durability_test_epoch_reset();
    char absent_path[PATH_MAX];
    snprintf(absent_path, sizeof(absent_path), "%s/nope/absent.bm",
             cache_dir);
    int synced6 = -1;
    ASSERT_EQ_INT(durability_epoch_fdatasync_path_ex(absent_path,
                                                     &synced6), 0,
                  "missing file: nothing to sync is success");
    ASSERT_EQ_INT(synced6, 1, "missing-file round attempted the raw op");
    ASSERT_EQ_INT(durability_test_epoch_file_sync_count(), 1,
                  "missing-file sync counted as one attempt");

    errno = 0;
    synced6 = -1;
    ASSERT_EQ_INT(durability_epoch_fdatasync_path_ex(NULL, &synced6), -1,
                  "null path is rejected at the protocol entry");
    ASSERT_EQ_INT(errno, EINVAL, "null path fails with EINVAL");
    ASSERT_EQ_INT(synced6, 0, "rejected call performed no raw op");
    ASSERT_EQ_INT(durability_test_epoch_file_sync_count(), 1,
                  "rejected null-path call is not counted");

    slotcask_shutdown();
    rmrf(cache_dir);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-durability-epoch-sync", test_durability_epoch_sync_run)
```

**Verify (red):** build with `rtk proxy env SKIP_TESTS=1 ./build.sh`.
The expected red is the **build failure** — the test calls
`durability_epoch_*` / `durability_test_epoch_*` symbols that no
translation unit defines yet (undeclarable before the module exists).
Paste the compiler/linker error and continue. (The count-based
discriminating red for scenario 1 is produced against the real module
by Task 2's revert-proof.)

### Task 2 — The epoch module + index/bitmap wiring

**Test-first:** the failing test for this task is Task 1's
`test-durability-epoch-sync`, red at build. Before any implementation
edit, re-run the focused build and paste that red — do not start from a
green tree. (A second in-tree red for scenario 1's discriminator is
produced after implementation by the revert-proof below.)

**Edits:**

1. Create `src/db/durability_epoch.c` exactly as in D1 (registry +
   protocol + raw ops + TEST hooks — the complete file is given above).
2. `types.h`: insert the D5 block at its quoted anchor.
3. `build.sh`: add `src/db/durability_epoch.c` next to every
   `src/db/durability.c` occurrence (five sites). For the two
   single-line gcc commands, e.g. replace

   ```sh
   gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/durability.c src/db/parallel.c
   ```

   (prefix of the daemon link line) with

   ```sh
   gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c
   ```

   and likewise in the test-binary gcc line. For `LIB_SRCS`, replace

   ```sh
   LIB_SRCS="src/db/util.c src/db/durability.c src/db/parallel.c src/db/storage.c src/db/index.c \
   ```

   with

   ```sh
   LIB_SRCS="src/db/util.c src/db/durability.c src/db/durability_epoch.c src/db/parallel.c src/db/storage.c src/db/index.c \
   ```

   For the two multi-line source lists (test binary and bench binary),
   after each standalone line

   ```sh
       src/db/durability.c \
   ```

   (exactly two occurrences) insert

   ```sh
       src/db/durability_epoch.c \
   ```

4. `btree.c`: apply the D2 body swap at its quoted anchor.
5. `slotcask.c`: apply the D3 body swap at its quoted anchor.

**Verify (green):** build; `rtk proxy ./build/bin/shard-db-test run
test-durability-epoch-sync` → all six scenarios green;
`run test-single-op-index-sync` green (exact sequential counts
preserved); full `run-all` green; `run test-parallel-index-integrity`
×4 green. Paste all outputs.

**Revert-proof (regression evidence, required):** in `btree.c`,
temporarily replace the D2 body with a direct raw call

```c
int btree_sync_path(const char *path) {
#ifdef TEST_BUILD
    atomic_fetch_add(&g_test_btree_sync_count, 1);
#endif
    return btree_sync_path_raw(path, NULL);
}
```

rebuild, and run `test-durability-epoch-sync`: scenario 1 must fail
with `btree_test_sync_count() == 4` (uncoalesced) — paste that failing
output. Restore the D2 body exactly, rebuild, rerun → green — paste.
Both outputs go in the notes; the diff at handback must show the D2
body, not the revert.

### Task 3 — Marker-dir epoch swap

**Test-first (red):** create
`src/test/cases/test_durability_epoch_marker_dir.c` with **exactly**
this registration:

```c
TEST_REGISTER("test-durability-epoch-marker-dir",
              test_durability_epoch_marker_dir_run)
```

and add the file to `build.sh`'s test-case list — after

```c
    src/test/cases/test_durability_epoch_sync.c \
```

insert

```c
    src/test/cases/test_durability_epoch_marker_dir.c \
```

Complete test file (scaffold mirrors `test_request_flush_batching.c`'s
`RfDb`/`rf_run_request` pattern, minus the pause hooks):

```c
/* src/test/cases/test_durability_epoch_marker_dir.c
 *
 * B3a D4: marker-directory fsyncs route through the process-wide
 * durability epoch. Two concurrent deferred bulk upsert requests on
 * one object (one shared data/kf dir) both succeed, and at least one
 * raw dir fsync runs through the epoch.
 *
 * Red before D4: the B2 per-request coalescer never calls the epoch,
 * so the dir count is 0 while the requests still succeed.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);

#define MD_SPLITS 8
#define MD_NRECS  4

typedef struct {
    SlotcaskDb db;
    char base[PATH_MAX];
} MdDb;

static int md_db_open(MdDb *w) {
    slotcask_init(64, 64);
    char b[] = "/tmp/shard-db-durepo-mkdir-XXXXXX";
    if (!mkdtemp(b)) return -1;
    snprintf(w->base, sizeof(w->base), "%s", b);
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", w->base);
    snprintf(k, sizeof(k), "%s/data/kf", w->base);
    if (mkdir(d, 0755) != 0 || mkdir(k, 0755) != 0) return -1;
    memset(&w->db, 0, sizeof(w->db));
    if (slotcask_open(&w->db, w->base, MD_SPLITS, 1, 64) != 0) return -1;
    w->db.bulk_commit_window = 16;
    return 0;
}

static void md_db_close(MdDb *w) {
    slotcask_close(&w->db);
    rmrf(w->base);
    slotcask_shutdown();
}

typedef struct {
    char keys[MD_NRECS][24];
    char vals[MD_NRECS][24];
    SlotcaskBulkRec recs[MD_NRECS];
    SlotcaskBulkRec perm[MD_NRECS];  /* bucketed copy the inputs point
                                        into; lives for the request   */
} MdBatch;

static int md_shard_of(const char *key) {
    uint8_t h[16];
    compute_hash_raw(key, strlen(key), h);
    return compute_record_shard(h, MD_SPLITS);
}

static void md_batch_fill(MdBatch *b, int prefix) {
    for (int i = 0; i < MD_NRECS; i++) {
        snprintf(b->keys[i], sizeof(b->keys[i]), "md-%d-%02d", prefix, i);
        snprintf(b->vals[i], sizeof(b->vals[i]), "v-%d-%02d", prefix, i);
        memset(&b->recs[i], 0, sizeof(b->recs[i]));
        b->recs[i].key = b->keys[i];
        b->recs[i].klen = strlen(b->keys[i]);
        b->recs[i].value = b->vals[i];
        b->recs[i].vlen = strlen(b->vals[i]);
    }
}

static int md_noop_prepare(SlotcaskBulkRec *recs, const size_t *active,
                           size_t nactive, void *ctx,
                           void **out_window_state) {
    (void)recs; (void)active; (void)nactive; (void)ctx;
    *out_window_state = NULL;
    return 0;
}
static int md_noop_apply(SlotcaskBulkRec *recs, const size_t *active,
                         size_t nactive, void *ctx, void *window_state) {
    (void)recs; (void)active; (void)nactive; (void)ctx;
    (void)window_state;
    return 0;
}
static void md_noop_terminal(void *ctx, void *window_state) {
    (void)ctx; (void)window_state;
}

static int md_run_request(MdDb *w, MdBatch *b) {
    SlotcaskBulkShardInput inputs[MD_SPLITS];
    size_t ninputs = 0;
    size_t bucket_n[MD_SPLITS] = {0};
    for (int i = 0; i < MD_NRECS; i++)
        bucket_n[md_shard_of(b->keys[i])]++;
    size_t run = 0;
    int bucket_start[MD_SPLITS];
    for (int s = 0; s < MD_SPLITS; s++) {
        bucket_start[s] = (int)run;
        run += bucket_n[s];
    }
    if (run != MD_NRECS) return -1;
    size_t cursors[MD_SPLITS];
    for (int s = 0; s < MD_SPLITS; s++) cursors[s] = bucket_start[s];
    for (int i = 0; i < MD_NRECS; i++) {
        int s = md_shard_of(b->keys[i]);
        b->perm[cursors[s]++] = b->recs[i];
    }
    SlotcaskBulkOpts opts;
    memset(&opts, 0, sizeof(opts));
    opts.has_indexed_fields = 1;   /* markers published → M/C barriers */
    opts.prepare_window = md_noop_prepare;
    opts.apply_window = md_noop_apply;
    opts.commit_done = md_noop_terminal;
    opts.release_window = md_noop_terminal;
    opts.abort_window = md_noop_terminal;
    for (int s = 0; s < MD_SPLITS; s++) {
        if (bucket_n[s] == 0) continue;
        inputs[ninputs].kf_shard_id = s;
        inputs[ninputs].recs = b->perm + bucket_start[s];
        inputs[ninputs].nrecs = bucket_n[s];
        inputs[ninputs].kind = SLOTCASK_BULK_INPUT_UPSERT;
        inputs[ninputs].opts.upsert = opts;
        inputs[ninputs].rc = 0;
        ninputs++;
    }
    return slotcask_bulk_request_execute(&w->db, inputs, ninputs);
}

typedef struct { MdDb *w; MdBatch *b; int rc; } MdReqThr;

static void *md_req_thread(void *raw) {
    MdReqThr *a = raw;
    a->rc = md_run_request(a->w, a->b);
    return NULL;
}

static int test_durability_epoch_marker_dir_run(void) {
    MdDb w;
    memset(&w, 0, sizeof(w));
    ASSERT_EQ_INT(md_db_open(&w), 0, "open marker-dir epoch fixture");
    if (w.base[0] != '/') { slotcask_shutdown(); return 1; }

    durability_test_epoch_reset();

    MdBatch ba, bb;
    md_batch_fill(&ba, 1);
    md_batch_fill(&bb, 2);
    MdReqThr ta = { &w, &ba, 0 }, tb = { &w, &bb, 0 };
    pthread_t tha, thb;
    ASSERT_EQ_INT(pthread_create(&tha, NULL, md_req_thread, &ta), 0,
                  "spawn request A");
    ASSERT_EQ_INT(pthread_create(&thb, NULL, md_req_thread, &tb), 0,
                  "spawn request B");
    pthread_join(tha, NULL);
    pthread_join(thb, NULL);

    ASSERT_EQ_INT(ta.rc, 0, "request A succeeds");
    ASSERT_EQ_INT(tb.rc, 0, "request B succeeds");
    /* RED pre-D4: the B2 per-request coalescer never calls the epoch,
       so this count is 0 while both requests still succeed. GREEN
       post-D4: at least one marker-dir fsync ran through it. The
       coalescing DEGREE is deliberately not asserted here — that is
       the bench's job (Task 5). */
    ASSERT_TRUE(durability_test_epoch_dir_sync_count() >= 1,
                "marker-dir fsyncs route through the process-wide epoch");

    durability_test_epoch_reset();
    md_db_close(&w);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-durability-epoch-marker-dir",
              test_durability_epoch_marker_dir_run)
```

**Verify (red):** build; run focused. Request A/B both succeed, but
`durability_test_epoch_dir_sync_count()` is 0 because the B2
per-request coalescer never calls the epoch. Paste the failing output.

**STOP — approval checkpoint (human decision required).** The D4 hunks
below replace B2's failure behavior, not just its plumbing: today a
failed marker-dir fsync re-arms `dir_dirty` and concurrent waiters
*retry inline*; under the epoch a failed round **fails its whole
registration window immediately** with a failed round's errno. Both
designs end in pipeline failure → markers retained → startup
forward-replay (the crash-set tests below pin recovery either way),
but the caller-visible error semantics change from
retry-until-converged to fail-fast. Per the working model, this
tradeoff is the human's to make: present the delta exactly as written
in D4's "Behavior delta vs. B2" paragraph, record the question and the
human's explicit answer in `PLAN_NOTES.md`, and do not apply the hunks
without an explicit go-ahead.

On approval, apply D4 hunks (a)–(f) at their quoted anchors.

**Verify (green):** `rtk proxy ./build/bin/shard-db-test run
test-durability-epoch-marker-dir` green (count ≥ 1, both requests
succeed); `run test-request-flush-batching` green (the NOTE_SYNC M/C
injection probes live at the call sites, so scenarios 10/11 are
unaffected); full suite ×2 green; crash set green — `test-marker-v2`,
`test-durability-bulk-pipeline-multishard-crash`,
`test-durability-bulk-window-boundary`,
`test-durability-bulk-window-boundary-mixed-indexes`,
`test-durability-bulk-window-prepared-recovers`,
`test-durability-bulk-window-applied-recovers`. Paste outputs.

### Task 4 — Docs

**Test-first (doc-red):** run `grep -n "coalesce" docs/concepts/concurrency.md`
and paste the output: the coalescing bullet describes only per-file
coalescing and never mentions process-wide epochs or the marker dir —
that missing claim is this task's failing state. The edits below are
green when the bullet states the B3a process-wide epochs and the
changelog carries the B3a sentence.

**Verification gate (targeted, not a bare "per-request" grep):** the
string "per-request" legitimately appears in current docs for unrelated
features (per-request `timeout_ms`, `MAX_REQUEST_SIZE`, tracing) and
throughout `docs/plans/` as historical record — never edit those. The
gate is:

```bash
grep -rniE 'per-request' \
    docs/concepts docs/reference docs/operations \
    docs/getting-started docs/query-protocol \
    | grep -iE 'dir|fsync|coalesc'
```

which must output nothing after the edit (it outputs nothing today —
no current architecture doc describes marker-directory fsync as
per-request; the hits are all `timeout_ms`/size/tracing).

- `docs/concepts/concurrency.md`: replace the bullet

  ```
  - concurrent pipelines **coalesce durability syncs per file** (a sync runs
    once; overlapping syncers skip once a completed sync covered their
    bytes).
  ```

  with:

  ```
  - concurrent pipelines **coalesce durability syncs per file** (a sync runs
    once; overlapping syncers skip once a completed sync covered their
    bytes). Since B3a this coalescing is **process-wide** for index
    files (btree + trigram + bitmap-leg path syncs) and the marker
    directory via path-keyed durability epochs
    (`src/db/durability_epoch.c`): concurrent requests syncing the same
    file or the same `data/kf` dir share one raw fdatasync/fsync, and a
    failed raw op fails its whole registration window with the raw
    errno. Segments keep the B2 dirty-flag per-window coalescer; the K
    barrier needs none (only one window is ever in flight per shard).
  ```

- `docs/reference/changelog.md`: in the `## Unreleased` section, in the
  paragraph beginning

  ```
  **Per-shard bulk commit pipelines + marker V2 (2026.09).** Indexed bulk
  ```

  append one sentence at the end of that paragraph: "Since B3a, index
  and marker-dir durability syncs coalesce **across** concurrent
  requests via path-keyed epochs (`src/db/durability_epoch.c`), not
  just within one request."

- **Verify:** the Task-4 targeted gate above (scoped trees, marker-dir
  phrasing) is empty; no doc claims segments use path-keyed epochs.

### Task 5 — Sanitizer gate + measurement (human-run)

ASan build + 3 fresh `run-all`:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all   # ×3, fresh each time
```

TSan build + 3 fresh `run-all` (TSan is the primary reviewer of the
epoch protocol — new condvar wait set shared across concurrent
requests):

```bash
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all   # ×3
```

Measurement (5+5 medians, same protocol as B2's; human-run, executor
never runs benches):

- **Primary:** `bench-invoice` (14-index parallel) — expect a clear
  aggregate win; record before/after on the same tree (before = current
  branch tip, after = B3a).
- **Guard:** `bench-kv-parallel` splits=8 — within 2% of B2's recorded
  numbers (1.33 / 1.75 / 0.87 / 1.04); the plan's win is NOT here.
- Single-conn KV within 2%.

If the indexed win does not materialize: report and stop — the epoch
layer stays (it is strictly less work than uncoalesced syncs) but B3b
planning gets the measured numbers first.

## Out of scope (parked for B3b)

- **Dynamic per-shard window batching for single-record writes** (the
  "second mechanism"): a forming window that concurrent singles join so
  N same-shard writers share one P/marker/K/clear chain instead of
  serializing N full windows on the gate. This is the mechanism that
  moves many-client single-insert throughput; it touches the fast-path
  transaction shape, wakeup, and per-record failure attribution — plan
  separately, only after B3a's numbers.
- Segment coalescer changes (B2's protocol already chains same-window
  syncers; residual multi-flush is genuine work).
- K barrier (no concurrent sharer exists by admission design).
- Any background flusher thread (leader/follower only — no polling).
