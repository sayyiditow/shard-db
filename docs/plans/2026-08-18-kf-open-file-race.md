# kf_open_file race on concurrent first-open of a kf shard

**Status:** planned — ready for execution (see "Implementation plan"
below). Root-caused and design-decided; not yet implemented. Discovered
while gating
`fix/bool-literal-merge-bug` under mandatory full-core-parallelism ASan
(`AGENTS.md`'s sanitizer gate). Unrelated to that diff's content:
`src/db/slotcask.c` is not touched by it. Documented here per
`AGENTS.md`'s standing exception ("New findings get root-caused and either
fixed now ... or written up as a `docs/plans/<date>-<slug>.md`").

## Symptom

Under full 16-way-parallel `./build/bin/shard-db-test run-all` (default
worker count = `nproc`, no `--jobs` override), a small, varying subset of
otherwise-unrelated, pre-existing tests intermittently fail
`create-object` / daemon-startup with:

```
{"error":"slotcask_open failed for <dir>/<object>"}
```

Observed across `test-warmup-vacuum-race`, `test-secure-random-keys`,
`test-auto-key-multi`, `test-varchar-overflow`, and a per-tenant-auth test
(`tenant_a/users`). No two runs hit the same set of tests — consistent
with a timing-dependent race, not a deterministic logic bug. Reproduces
only under load; standalone and low-parallelism (`--jobs 8`) runs never
hit it, which is what makes it invisible unless the gate genuinely runs
at full core count, exactly as `AGENTS.md` intends.

Not a sanitizer finding: zero ASan/LeakSanitizer reports appeared across
5+ full 3-pass gate attempts. The bug is a filesystem-level TOCTOU race
between two openers of the same path — outside what either ASan's
redzone/leak model or TSan's same-address shadow-memory model can see, so
this would very plausibly evade the TSan half of the gate too, not just
ASan.

## Concurrency scope: intra-process, not cross-process

Corrected after review: the bug's supported scope is **intra-process**
concurrency, not a cross-process/shared-`DB_ROOT` scenario. The earlier
draft of this doc speculated about two daemon processes racing; that
premise is removed below. The actual mechanism is entirely within one
daemon process:

`kfcache_acquire_ex()`'s cache-miss path (`src/db/slotcask.c:519-531`)
intentionally unlocks `g_kfcache_lock` before calling the blocking
`kf_open_file()` ("Drop table lock during open since it can block on
disk"), and only re-takes the lock afterward to install the result. Two
worker threads in the *same* daemon process that both miss the kfcache
for the same not-yet-cached path (e.g. the daemon's multi-threaded
request pool serving concurrent `create-object`/first-touch requests
against the same object) fall through this window with no
synchronization between them at all, and both call `kf_open_file()`
concurrently for the same path. That dropped-lock window is deliberate
and correct on its own terms — it exists so one slow disk open doesn't
serialize unrelated paths behind it — but it means any fix must add
same-process, per-path exclusion around the miss path, not assume the
race requires two separate processes.

## Root cause

`kf_open_file()` (`src/db/slotcask.c`, ~line 330–406) opens/creates a kf
shard file in four steps that are atomic neither within one call nor
across concurrent callers of the same path:

1. `open(path, O_RDWR | O_CREAT | O_EXCL, 0644)` — race-free file
   creation. On `EEXIST`, falls back to `open(path, O_RDWR)` (tolerated,
   by design — this part is correct).
2. `fstat` + (if `created_fresh` or `st_size == 0`) `ftruncate(fd, want)`
   to size the file to its full slot-table size.
3. `mmap(..., MAP_SHARED, fd, 0)`.
4. If `created_fresh`: stamp `hdr->magic = SLOTCASK_KF_MAGIC` (+ version,
   zero counters). Else: check `hdr->magic == SLOTCASK_KF_MAGIC`, and
   `return -1` if not — **without setting `errno`**.

Steps 2–4 are not covered by any lock relative to *other* openers of the
same path. Timeline that reproduces the bug:

- **Caller A** (the true first creator) wins `O_CREAT|O_EXCL`, reaches
  step 2, `ftruncate`s the file to full size — but hasn't yet reached
  step 3/4 (mmap + magic stamp).
- **Caller B** — a second worker thread in the *same* daemon process,
  racing A through `kfcache_acquire_ex()`'s miss path (see "Concurrency
  scope" above: `g_kfcache_lock` is dropped before either thread calls
  `kf_open_file()`, so nothing serializes them) — loses the `O_EXCL` race
  (`EEXIST`, tolerated per design), falls back to `open(path, O_RDWR)`,
  `fstat`s and sees
  `st_size == want` already (A's `ftruncate` already landed) — so B's
  `else if (st_size < want)` branch is skipped, B does **no** truncate of
  its own, and proceeds straight to `mmap`.
- B's `hdr->magic` read observes `0` — A hasn't stamped it yet. B hits
  the `else if (hdr->magic != SLOTCASK_KF_MAGIC)` branch and `return -1`.
- That branch never sets `errno`. Whatever `errno` was left over from an
  **earlier, already-handled, non-fatal** step in the same call — B's own
  tolerated `EEXIST` from its `O_CREAT|O_EXCL` attempt a few lines above —
  leaks straight through to the caller. This is why every observed
  instance of this bug surfaced as `errno=17 (File exists)` at the
  `slotcask_open` call site (`src/db/query_schema.c:1320`), regardless of
  which object/test actually hit it: it's not reporting the real cause,
  it's reporting B's own stale, harmless, already-recovered-from errno.

### Confirmation

Reproduced directly (temporary instrumentation, added and then fully
reverted — `git diff --stat src/db/slotcask.c src/db/query_schema.c` is
clean on `fix/bool-literal-merge-bug` as of this writing):

```
DIAG kf_open_file: bad magic path=/tmp/shard-db-pta-1054561/db/tenant_a/users/data/kf/001.kf created_fresh=0 st_size=25165848 want=25165848 magic=0x0 expected=0x31464b53 pre_errno=17 (File exists)
DIAG kf_open_file: bad magic path=/tmp/shard-db-pta-1054561/db/tenant_a/users/data/kf/004.kf created_fresh=0 st_size=25165848 want=25165848 magic=0x0 expected=0x31464b53 pre_errno=17 (File exists)
DIAG kf_open_file: bad magic path=/tmp/shard-db-pta-1054561/db/tenant_a/users/data/kf/000.kf created_fresh=0 st_size=25165848 want=25165848 magic=0x0 expected=0x31464b53 pre_errno=17 (File exists)
DIAG slotcask_open fail dir=tenant_a obj=users errno=117 (Structure needs cleaning)
```

`created_fresh=0`, `st_size == want` (fully sized by the true creator),
`magic=0x0` (not yet stamped by the true creator) — exactly the window
described above. Three shards hit it in the same burst, meaning at least
two concurrent `slotcask_open` calls for the same object (`tenant_a/users`)
were in flight — from two worker threads within that one test's single
daemon process, both missing the kfcache for the same not-yet-installed
shard path and falling through to `kf_open_file()` with no
synchronization between them (per "Concurrency scope" above). No second
process or shared `DB_ROOT` is involved.

Ruled out as explanations before landing on this: `vm.max_map_count`
(1048576, not the bottleneck), cgroup memory/cpu/pids limits
(inaccessible/not configured on this host), `ulimit -n`/`-u` (524288 /
118969, generous). Heavier load doesn't exhaust any of those — it simply
widens the A-to-B timing window between `ftruncate` and `mmap`+stamp,
making an already-possible race more likely to actually land, which is
why this only ever appears under genuine full-core parallelism and not
at reduced `--jobs`.

## Why this wasn't fixed inline originally (historical)

This section records the original deferral reasoning from when the bug
was first root-caused, while gating an unrelated diff. It's kept for
context; the single-flight-guard idea in bullet 2 below is, after two
rounds of revisiting the choice, the approach actually landed on — see
"Design decision" below for the final call and why.

- `slotcask.c` is not part of the `fix/bool-literal-merge-bug` diff, so
  fixing this was out of scope for that diff regardless of approach.
- A correct fix needs same-process synchronization across the create+init
  sequence for concurrent misses on the same path — one candidate
  considered was a per-path single-flight/initialization guard in
  `kfcache_acquire_ex()`'s miss path: mark the path as "opening" (under
  `g_kfcache_lock`, cheaply) before dropping the lock for the blocking
  `kf_open_file()` call, so a second thread that misses on the same path
  waits for the first opener's install instead of independently calling
  `kf_open_file()` itself. `flock()` was ruled out regardless of which
  candidate won: the race is intra-process, so a per-path in-memory guard
  would be both simpler and avoid reintroducing cross-syscall
  file-locking cost. Either way, a guard of this kind would touch the
  `g_kfcache_lock`-drop-across-blocking-I/O tradeoff (`slotcask.c:519-520`)
  that exists so one slow disk open doesn't serialize unrelated paths —
  a locking-path change per `AGENTS.md`'s own gate criteria, requiring
  its own full 3x ASan + 3x TSan local gate. Not something to bundle into
  an already-large, unrelated diff at the time.
- The bug also needed a real errno contract decision (what should the
  "corrupt or still-initializing?" branch actually report, and should it
  retry a bounded number of times instead of failing immediately, since
  "another opener is mid-initialization" is not actually a fatal
  condition for the *losing* opener — it could poll/backoff and re-check
  magic instead of giving up). **Decided** (see "Design decision" below):
  the single-flight guard from the bullet above is the primary fix;
  bounded retry/backoff on the magic field is kept, demoted to a
  narrowly-scoped defense-in-depth backstop for the paths the guard
  can't reach (see Task 2).

## Design decision: single-flight guard, not bounded retry alone

Two candidate fixes were weighed:

1. **Bounded retry/backoff in `kf_open_file()`** — on magic mismatch with
   `created_fresh == 0` and `st_size == want`, poll `hdr->magic` a short,
   bounded number of times before concluding the file is genuinely
   corrupt, instead of failing on the first read.
2. **Per-path single-flight guard** — new shared state (an in-flight-path
   registry) plus a wait/signal mechanism so a second thread that misses
   the cache for the same path blocks until the first opener finishes,
   instead of also calling `kf_open_file()`.

**Originally chosen: option 1**, then revisited and replaced with
**option 2** after two follow-up questions surfaced a better answer:
"if we already tolerate two openers racing, what bug are we even fixing?"
and "is this on the hot path, or is there a more elegant way so we never
hit an error at all?" Both are addressed by the same finding:

This codebase already implements exactly the mechanism option 2
describes — one layer up. `slotcask_registry_get()`
(`slotcask.c:8122-8207`) de-dupes concurrent `slotcask_open()` calls for
the same `(dir,obj)` key via a per-entry `opening` flag plus a broadcast
condvar (`RegEntry.opening`, `g_reg_cond` —
`shard_db_internal.h:69-76,341-342`). It just doesn't reach this bug,
because the actual racing caller — a direct `kfcache_acquire()` for one
kf shard from `query.c`, `index.c`, `server.c`, or the registry's own
warmup fan-out — bypasses that object-level registry and opens the shard
directly. **Decided:** mirror the exact same pattern one layer down, at
kf-path granularity, inside `kfcache_acquire_ex()`'s miss path
(`slotcask.c:519-531`), so a second thread that misses the cache for a
path another thread is already opening waits for that thread instead of
also calling `kf_open_file()`.

This is a better answer to both follow-up questions, not just a
different one:

- **Scope.** `kfcache_acquire()` is called on essentially every
  operation, but the *race* itself only exists while the underlying kf
  shard file doesn't exist on disk yet — `kf_open_file()`'s
  `created_fresh` branch is the only place with anything to race on.
  That confines the exposure to a brand-new object's first-touch window
  (including the documented `create-object` → parallel bulk-insert
  pattern), not the steady-state hot loop of a warm cache. A guard here
  only ever runs on a cache miss, which is already paying for blocking
  disk I/O — the added bookkeeping under a lock the code already takes
  to check the cache is negligible next to that, and it doesn't change
  the existing "drop `g_kfcache_lock` across blocking I/O" tradeoff: the
  thread that actually calls `kf_open_file()` still does so with the
  table lock dropped, exactly as today. Only a *second* thread that
  would otherwise also call `kf_open_file()` for the identical path
  waits instead — under the table lock, on a condvar, for exactly as
  long as the real open takes.
- **"Never hit an error" is achievable, not just softened.** Bounded
  retry (option 1) still has a losing case: if the true creator is ever
  delayed past the retry budget (page fault storm, cgroup throttling,
  a slow `msync`), the second opener gives up and fails closed with
  `EILSEQ`, having also wasted a redundant `open`/`ftruncate`/`mmap` on
  the way there. A single-flight guard has no such ceiling — the waiter
  is woken exactly when the real opener finishes (success or failure),
  deterministically, with no polling and no duplicate filesystem work in
  the common case.

Because the hand-off is now a real `pthread_mutex`/`pthread_cond` pair,
POSIX's own happens-before guarantee covers `hdr->magic` (and the header
fields around it) for the guarded path — a waiter only wakes after the
opener's result is fully installed or the opener has fully failed, so no
manual `__atomic_*` dance is needed there. Option 1's bounded-retry fix
is still worth keeping, but demoted: it's the correctness backstop for
the two paths the guard structurally can't cover — see Task 2.

## Implementation plan

Branch: `fix/kf-open-file-magic-race` off `main`.
Build/test per `AGENTS.md`: `SKIP_TESTS=1 ./build.sh`, then
`./build/bin/shard-db-test run-all --filter <name>` for the new test, and
the full sanitizer gate (Task 3) before calling this done — this diff
touches `kfcache`, shared/cached state, per `AGENTS.md`'s standing
exception.

### Task 1 (test-first): add the regression test, prove it fails today

The test forces the exact interleaving from "Root cause" above
deterministically: it starts one thread that creates a brand-new kf
shard through the real production path (`kfcache_acquire(writer=1)` →
`kfcache_acquire_ex` → `kf_open_file`), holds it — via a new test-only
knob — between `ftruncate` and the magic stamp, and meanwhile the main
thread (acting as the racing second opener) calls `kfcache_acquire()` on
the identical path and must observe success, not the pre-fix immediate
failure. This task only adds the test and the hold knob — it does not
depend on anything from Task 2, and is unchanged by the design pivot
from bounded-retry to a single-flight guard, because it asserts
black-box behavior (`rc2 == 0`), not which mechanism produced it. Task 2
comes back and extends this same test file with one more assertion that
only the guard makes meaningful (see 2k).

**1a. Add the test-only hold knob.**

In `src/db/shard_db_internal.h`, the `ShardDb` struct already carries
`kfcache_test_hold_ms` (test-only, used by the shutdown-race test) right
above `warmup_test_delay_ms`. Locate this exact block:

```c
    int kfcache_test_hold_ms; /* test-only; 0 = off in production */
    int warmup_test_delay_ms; /* test-only; 0 = off in production */
```

and insert a new field between them:

```c
    int kfcache_test_hold_ms; /* test-only; 0 = off in production */
    int kf_open_create_test_hold_ms; /* test-only; 0 = off in production.
        Widens the window inside kf_open_file() between a freshly-created
        kf shard's ftruncate and its magic stamp, so a concurrent second
        opener's read of hdr->magic deterministically lands mid-init
        instead of depending on incidental scheduling (see
        test-kf-open-file-race). Set directly on the ShardDb struct by
        that test; deliberately not wired to db.env (KFCACHE_TEST_HOLD_MS
        is, because that knob's test drives a real daemon subprocess —
        this one drives a direct in-process call, so no config file is
        involved and wiring one would be unused surface). */
    int warmup_test_delay_ms; /* test-only; 0 = off in production */
```

Then locate the `/* slotcask.c */` macro block:

```c
#define g_kfcache_lock              (g_db->kfcache_lock)
#define g_kfcache_clock             (g_db->kfcache_clock)
```

and insert the new macro directly after it:

```c
#define g_kfcache_lock              (g_db->kfcache_lock)
#define g_kfcache_clock             (g_db->kfcache_clock)
#define g_kf_open_create_test_hold_ms (g_db->kf_open_create_test_hold_ms)
```

**1b. Add the hold into `kf_open_file()`'s `created_fresh` branch.**

In `src/db/slotcask.c`, locate:

```c
    SlotcaskKfHeader *hdr = (SlotcaskKfHeader *)m;
    if (created_fresh) {
        /* Stamp magic + version; counters start at 0. */
        hdr->magic = SLOTCASK_KF_MAGIC;
```

and insert the hold immediately before the existing comment:

```c
    SlotcaskKfHeader *hdr = (SlotcaskKfHeader *)m;
    if (created_fresh) {
        if (g_db && g_kf_open_create_test_hold_ms > 0) {
            /* Test-only hook (test-kf-open-file-race): widens the window
               between ftruncate (above) and the magic stamp below,
               deterministically, so a concurrent second opener's
               kf_open_file() call reliably observes magic==0 instead of
               depending on incidental scheduling. 0 in production. */
            struct timespec hold_ts = {
                g_kf_open_create_test_hold_ms / 1000,
                (long)(g_kf_open_create_test_hold_ms % 1000) * 1000000L
            };
            int hold_rc;
            do {
                hold_rc = nanosleep(&hold_ts, &hold_ts);
            } while (hold_rc != 0 && errno == EINTR);
        }
        /* Stamp magic + version; counters start at 0. */
        hdr->magic = SLOTCASK_KF_MAGIC;
```

This is test-only scaffolding, not the fix — `g_kf_open_create_test_hold_ms`
is 0 in production, so this branch never runs outside the new test. It
must land in the same commit as the test (Task 1), not Task 2, since the
point of Task 1 is to prove the test fails for the right reason *before*
the fix exists.

**1c. Write the test.**

New file `src/test/cases/test_kf_open_file_race.c`. Modeled on
`test_kfcache_staleness.c` (direct-call unit test, reuses the runner's
process-local `ShardDb`) and on `test_ordered_walk_kfcache_deadlock.c`'s
pattern for binding a spawned thread's thread-local `g_db` (`slotcask.c`'s
kfcache macros all resolve through `g_db`, which is `__thread` — a freshly
spawned pthread has a NULL `g_db` until it's explicitly bound):

```c
/* src/test/cases/test_kf_open_file_race.c
 *
 * Regression test for the kf_open_file() magic-mismatch race documented
 * in docs/plans/2026-08-18-kf-open-file-race.md: kfcache_acquire_ex's
 * miss path (slotcask.c:519-531) intentionally drops g_kfcache_lock
 * before calling the blocking kf_open_file(), so two threads that both
 * miss the kfcache for the same not-yet-cached path can call
 * kf_open_file() concurrently. The true creator wins O_CREAT|O_EXCL,
 * ftruncates the file to full size, then mmaps and stamps hdr->magic. A
 * second opener that lands between the ftruncate and the stamp, pre-fix,
 * also calls kf_open_file() itself and hard-fails on the still-zero
 * magic instead of waiting for the creator to finish (see the "Design
 * decision" section of the plan doc for why a single-flight guard, not
 * just a bounded retry, is the fix).
 *
 * Uses KF_OPEN_CREATE_TEST_HOLD_MS (test-only, set directly on the
 * ShardDb struct, 0/off in production) to deterministically widen the
 * window: the creator thread holds between ftruncate and the stamp long
 * enough for the main thread's racing kfcache_acquire() call to land
 * inside it every time, rather than relying on incidental scheduling.
 */
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "types.h"
#include "shard_db_internal.h"
#include "slotcask.h"
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define RACE_SLOTS_CAPACITY 8
#define RACE_HOLD_MS 50
#define RACE_POLL_TIMEOUT_MS 5000

typedef struct {
    ShardDb *db;
    const char *path;
    int rc;
} CreatorArgs;

static void *creator_thread_fn(void *arg) {
    CreatorArgs *a = arg;
    g_db = a->db;
    SlotcaskKfHandle h;
    a->rc = kfcache_acquire(&h, a->path, RACE_SLOTS_CAPACITY, 1);
    if (a->rc == 0) kfcache_release(&h);
    return NULL;
}

static int test_kf_open_file_race_run(void) {
    ShardDb *db = test_get_process_db();

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-kf-open-race-%d", getpid());
    mkdir(tmpdir, 0755);
    char path[300];
    snprintf(path, sizeof(path), "%s/000.kf", tmpdir);
    size_t want = SLOTCASK_KF_HDR_SIZE +
                  (size_t)RACE_SLOTS_CAPACITY * sizeof(SlotcaskKfEntry);

    db->kf_open_create_test_hold_ms = RACE_HOLD_MS;

    CreatorArgs cargs = { .db = db, .path = path, .rc = -2 };
    pthread_t creator;
    ASSERT_EQ_INT(pthread_create(&creator, NULL, creator_thread_fn, &cargs),
                  0, "spawn creator thread");

    /* Wait for the creator's ftruncate to land (raw stat, no kfcache
       state touched) without waiting for the stamp — this is the exact
       window the bug lives in. Bounded so a stalled creator fails the
       test loudly instead of hanging it. */
    int waited_ms = 0;
    struct stat st;
    while (waited_ms < RACE_POLL_TIMEOUT_MS) {
        if (stat(path, &st) == 0 && (size_t)st.st_size == want) break;
        struct timespec poll_ts = { 0, 1000000L }; /* 1ms */
        nanosleep(&poll_ts, NULL);
        waited_ms += 1;
    }
    ASSERT_TRUE(waited_ms < RACE_POLL_TIMEOUT_MS,
           "creator's ftruncate must land within timeout");

    /* Racer: must land while the creator is still asleep in its 50ms
       hold (RACE_HOLD_MS) so this exercises the guard's wait path, not
       just a plain cache hit. Pre-fix: the racer also calls
       kf_open_file() itself and fails immediately on the still-zero
       magic. Post-fix: the racer sees the creator's in-flight
       reservation for this path, waits on it instead of opening the
       file itself, then finds the fully-installed cache entry once the
       creator finishes — no second kf_open_file() call. */
    SlotcaskKfHandle h2;
    int rc2 = kfcache_acquire(&h2, path, RACE_SLOTS_CAPACITY, 1);
    ASSERT_EQ_INT(rc2, 0,
        "racer must succeed once the creator's magic stamp lands, not "
        "fail on a still-initializing header");
    if (rc2 == 0) {
        ASSERT_EQ_INT((int)h2.hdr->total, 0,
            "racer must see a properly-initialized fresh header");
        kfcache_release(&h2);
    }

    pthread_join(creator, NULL);
    ASSERT_EQ_INT(cargs.rc, 0, "creator's own acquire must succeed");

    db->kf_open_create_test_hold_ms = 0;
    slotcask_shutdown();
    system("rm -rf /tmp/shard-db-kf-open-race-*");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-kf-open-file-race", test_kf_open_file_race_run)
```

**Initialization/cleanup contract (verified against the runner, not
deferred to execution time):**

- **Do not call `slotcask_init()`.** `test_init_process_db()`
  (`embedded.c:148`) runs once before the *first* case in this process
  (`if (g_db) return;` — it's a no-op for every case after that) and
  already leaves a fully-initialized kfcache in place via
  `shard_db_open_internal()` → `slotcask_init()`
  (`embedded.c:132`). By the time this test's body runs, under both the
  default parallel (fork-per-case) runner and `--jobs 1` sequential
  mode, `g_kfcache` is already non-NULL. `test_kfcache_staleness.c`'s own
  `slotcask_init(64, 64)` call (line 35 of that file) is consequently a
  harmless no-op there too — `kfcache_init()` returns immediately when
  `g_kfcache` is already set (`slotcask.c:164`) — not a step this test
  needs to copy.
- **Do call `slotcask_shutdown()` at the end**, matching
  `test_kfcache_staleness.c`'s convention. This is safe in both run
  modes: the default parallel runner forks a fresh process per case, so
  tearing down this process's caches has no effect on any other case;
  under `--jobs 1`, `run_all_sequential()` calls `test_reset_caches()`
  (`config.c:3968`) after *every* case specifically to undo this —  its
  own comment states the exact contract: "tests deliberately call
  `*_shutdown()` ... and never restore it afterward... `slotcask_init`
  [is] a no-op when already initialized, so this is a safe unconditional
  call regardless of what the prior test left behind." No extra handling
  is needed in this test beyond calling `slotcask_shutdown()` once, same
  as the existing test it's modeled on.

**1d. Prove the test fails today, for the right reason.** Before writing
the Task 2 fix:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-kf-open-file-race
```

Expected (pre-fix): the test fails on the `rc2 == 0` assertion (racer's
`kfcache_acquire()` returns non-zero) — confirming the test reproduces
the documented bug, not some unrelated setup problem. Paste this failing
run's output before proceeding to Task 2.

### Task 2: implement the fix

**2a. New struct + capacity constant.**

In `src/db/shard_db_internal.h`, locate this exact block:

```c
    dev_t    file_dev;      /* identity of the file open at install time — */
    ino_t    file_ino;      /* detects rename-away-and-recreate at `path` */
} KfCacheEntry;

/* slotcask.c — segcache */
```

and insert a new struct between them:

```c
    dev_t    file_dev;      /* identity of the file open at install time — */
    ino_t    file_ino;      /* detects rename-away-and-recreate at `path` */
} KfCacheEntry;

/* slotcask.c — kf_open_file() single-flight guard: at most one thread
   may call kf_open_file() for a given path at a time (see
   kfcache_acquire_ex's miss path). Sized well past any realistic daemon
   thread count (THREADS in db.env) so the "table full" uncoordinated
   fallback below is not expected to be exercised in practice;
   kf_open_file()'s own bounded-retry backstop (see the magic-mismatch
   branch) still covers that fallback — the one case that reaches
   kf_open_file() without going through this guard at all. */
#define KF_OPEN_INFLIGHT_SLOTS 256
typedef struct {
    char path[PATH_MAX];
    int  used;
} KfOpenInflight;

/* slotcask.c — segcache */
```

**2b. New `ShardDb` fields.**

Locate this exact block:

```c
    /* slotcask kfcache */
    KfCacheEntry        *kfcache;
    int                  kfcache_slots;
    int                  kfcache_count;
    pthread_mutex_t      kfcache_lock;
    volatile uint64_t    kfcache_clock;

    /* slotcask segcache */
```

and insert the new fields directly after `kfcache_clock`:

```c
    /* slotcask kfcache */
    KfCacheEntry        *kfcache;
    int                  kfcache_slots;
    int                  kfcache_count;
    pthread_mutex_t      kfcache_lock;
    volatile uint64_t    kfcache_clock;
    KfOpenInflight       kf_open_inflight[KF_OPEN_INFLIGHT_SLOTS];
    pthread_cond_t       kf_open_inflight_cond; /* broadcast whenever any
        kf_open_inflight[] slot's `used` clears; see kfcache_acquire_ex's
        miss path and kf_open_inflight_release(). Guarded by the existing
        kfcache_lock — no new mutex. */
    uint64_t             kf_open_file_call_count; /* diagnostic only;
        unconditionally incremented at the top of kf_open_file(), mirrors
        kfcache_count's always-on convention. Lets tests assert how many
        real opens actually happened, not just whether they succeeded. */

    /* slotcask segcache */
```

**2c. New macros.**

Locate this exact block:

```c
#define g_kfcache_lock              (g_db->kfcache_lock)
#define g_kfcache_clock             (g_db->kfcache_clock)
#define g_segcache                  (g_db->segcache)
```

and insert the new macros between them:

```c
#define g_kfcache_lock              (g_db->kfcache_lock)
#define g_kfcache_clock             (g_db->kfcache_clock)
#define g_kf_open_inflight          (g_db->kf_open_inflight)
#define g_kf_open_inflight_cond     (g_db->kf_open_inflight_cond)
#define g_kf_open_file_call_count   (g_db->kf_open_file_call_count)
#define g_segcache                  (g_db->segcache)
```

**2d. Init the new condvar.**

In `src/db/embedded.c`, locate this exact line:

```c
    pthread_mutex_init(&g_kfcache_lock,          NULL);
```

and insert directly after it:

```c
    pthread_mutex_init(&g_kfcache_lock,          NULL);
    pthread_cond_init(&g_kf_open_inflight_cond,  NULL);
```

**2e. Destroy the new condvar.**

Locate this exact line:

```c
    pthread_mutex_destroy(&g_kfcache_lock);
```

and insert directly after it:

```c
    pthread_mutex_destroy(&g_kfcache_lock);
    pthread_cond_destroy(&g_kf_open_inflight_cond);
```

**2f. New release helper.**

In `src/db/slotcask.c`, locate this exact line (confirmed immediately
before `kfcache_acquire_ex`'s definition):

```c
static int kfcache_acquire_ex(SlotcaskKfHandle *h, const char *path,
```

and insert the new static helper directly before it:

```c
/* Releases this thread's kf_open_file() single-flight reservation (if
   any — a no-op if *slot_ptr is already -1) and wakes any threads
   waiting on the inflight table to re-check it. Idempotent: safe to call
   more than once for the same slot pointer, which the recursive-call
   site in kfcache_acquire_ex relies on (see the explicit call there).
   Matches this file's existing __attribute__((cleanup(...))) convention
   (mirrors MAX_CONCURRENT_QUERIES's semaphore-release cleanup in
   server.c's dispatch_json_query) so every return/goto path out of the
   miss path releases the reservation automatically. */
static void kf_open_inflight_release(int *slot_ptr) {
    if (*slot_ptr < 0) return;
    pthread_mutex_lock(&g_kfcache_lock);
    g_kf_open_inflight[*slot_ptr].used = 0;
    pthread_cond_broadcast(&g_kf_open_inflight_cond);
    pthread_mutex_unlock(&g_kfcache_lock);
    *slot_ptr = -1;
}

static int kfcache_acquire_ex(SlotcaskKfHandle *h, const char *path,
```

**2g. Rewrite the miss path.**

Locate the exact current block:

```c
    /* Miss path: open + install. Drop table lock during open since it can
       block on disk. In nonblocking mode, bail here instead of opening —
       the "try" contract (kfcache_try_acquire_rd / kfcache_try_acquire_direct)
       is fast-path-only: it never touches the filesystem, so a genuine
       cold/evicted shard is treated exactly like lock contention. */
    if (nonblocking) {
        pthread_mutex_unlock(&g_kfcache_lock);
        errno = EBUSY;
        return -1;
    }
    pthread_mutex_unlock(&g_kfcache_lock);
    int fd; uint8_t *base; size_t sz; dev_t dev; ino_t ino;
    if (kf_open_file(path, slots_capacity, writer, &fd, &base, &sz, &dev, &ino) < 0) return -1;
    pthread_mutex_lock(&g_kfcache_lock);
```

Replace it with:

```c
    /* Miss path: open + install. Drop table lock during open since it can
       block on disk. In nonblocking mode, bail here instead of opening —
       the "try" contract (kfcache_try_acquire_rd / kfcache_try_acquire_direct)
       is fast-path-only: it never touches the filesystem, so a genuine
       cold/evicted shard is treated exactly like lock contention. */
    if (nonblocking) {
        pthread_mutex_unlock(&g_kfcache_lock);
        errno = EBUSY;
        return -1;
    }

    /* Single-flight guard: at most one thread calls kf_open_file() for a
       given path at a time. Mirrors slotcask_registry_get()'s
       RegEntry.opening/g_reg_cond pattern (slotcask.c:8122-8207) one
       layer down, at kf-path granularity — that registry only de-dupes
       whole-object slotcask_open() calls and doesn't see a direct
       per-shard kfcache_acquire() miss from query.c/index.c/server.c. */
    int kf_open_inflight_found = -1;
    for (int i = 0; i < KF_OPEN_INFLIGHT_SLOTS; i++) {
        if (g_kf_open_inflight[i].used &&
            strcmp(g_kf_open_inflight[i].path, path) == 0) {
            kf_open_inflight_found = i;
            break;
        }
    }
    if (kf_open_inflight_found >= 0) {
        /* Another thread is already opening this exact path. Wait for it
           to finish (install or fail), then restart from the top instead
           of opening it ourselves. Common case: the cache now has it, so
           we return immediately with zero extra I/O. Rare case: that
           opener failed, or its entry was evicted before we got here —
           we fall back through this same miss path again, re-checking
           the inflight table first (a third thread may now hold it). */
        pthread_cond_wait(&g_kf_open_inflight_cond, &g_kfcache_lock);
        pthread_mutex_unlock(&g_kfcache_lock);
        goto retry_kfcache_acquire;
    }

    int kf_inflight_slot __attribute__((cleanup(kf_open_inflight_release))) = -1;
    for (int i = 0; i < KF_OPEN_INFLIGHT_SLOTS; i++) {
        if (!g_kf_open_inflight[i].used) {
            g_kf_open_inflight[i].used = 1;
            strncpy(g_kf_open_inflight[i].path, path, PATH_MAX - 1);
            g_kf_open_inflight[i].path[PATH_MAX - 1] = '\0';
            kf_inflight_slot = i;
            break;
        }
    }
    /* Table full (sized well past any realistic thread count — see 2a
       above): fall through uncoordinated. kf_open_file()'s own
       bounded-retry backstop (2j below) still protects this path even
       without a reservation. */
    pthread_mutex_unlock(&g_kfcache_lock);
    int fd; uint8_t *base; size_t sz; dev_t dev; ino_t ino;
    if (kf_open_file(path, slots_capacity, writer, &fd, &base, &sz, &dev, &ino) < 0) return -1;
    pthread_mutex_lock(&g_kfcache_lock);
```

Note what changed structurally: the reservation is released — via
`kf_inflight_slot`'s cleanup attribute — on every return path below this
point (the re-probe-after-open returns, the eviction-failure return, the
cache-full return, both existing `goto retry_kfcache_acquire;` sites, and
the final success return), because a backward `goto` out of a variable's
declaring scope runs its cleanup before the jump, same as a normal
`return`. The one exception is 2h below.

**2h. Explicit release before the recursive call.**

Locate the exact current block (the final verify-or-recurse check, after
the entry is published and its rwlock re-taken):

```c
    if (!e->used || strcmp(e->path, path) != 0 || e->file_dev != dev ||
        e->file_ino != ino) {
        pthread_rwlock_unlock(lock);
        return kfcache_acquire_ex(h, path, slots_capacity, writer, nonblocking);
    }
```

Replace it with:

```c
    if (!e->used || strcmp(e->path, path) != 0 || e->file_dev != dev ||
        e->file_ino != ino) {
        pthread_rwlock_unlock(lock);
        /* Explicit release before recursing: kf_inflight_slot's cleanup
           attribute only fires when THIS stack frame returns, which
           doesn't happen until the recursive call below does. The
           recursive call re-enters the function fresh with the same
           path, so without this it would see our own still-held
           reservation and wait on it forever — a single-thread
           self-deadlock. Releasing explicitly first (idempotent — the
           later automatic cleanup call on this frame's own return
           becomes a no-op, since kf_open_inflight_release() checks for
           slot < 0) avoids that. */
        kf_open_inflight_release(&kf_inflight_slot);
        return kfcache_acquire_ex(h, path, slots_capacity, writer, nonblocking);
    }
```

**2i. Diagnostic call counter.**

Locate this exact line (`kf_open_file`'s first statement):

```c
    int created_fresh = 0;  /* track first-time creation for header init */
```

and insert directly after it:

```c
    int created_fresh = 0;  /* track first-time creation for header init */
    if (g_db) __atomic_fetch_add(&g_kf_open_file_call_count, 1, __ATOMIC_RELAXED);
```

**2j. Layer 2 backstop: retain the bounded-retry magic fix, reframed.**

The single-flight guard (2f-2h) is the primary fix and covers every
caller that reaches `kfcache_acquire_ex()`'s miss path with the cache
enabled — which is every caller in this codebase (`kf_open_file` is
`static` and has exactly two call sites, both inside
`kfcache_acquire_ex`). One path inside that same function still bypasses
the guard by design and genuinely needs its own protection: the
inflight-table-full fallback (2g above) — deliberately sized to be
practically unreachable, but not structurally impossible. If it's ever
hit, two writer threads can independently call `kf_open_file()` for the
same brand-new path with no coordination at all, reintroducing the exact
create/create race this plan closes.

The `g_kfcache` disabled fallback (`slotcask.c:444-455`) does **not**
need this backstop for that same reason — it was previously listed
alongside the table-full case, which overstated the exposure. Re-reading
that branch: a `writer=1` caller with the cache disabled returns
`ENODEV` immediately (`slotcask.c:446-449`) without ever calling
`kf_open_file()`, and `kf_open_file()` only takes the `O_CREAT|O_EXCL`
path that sets `created_fresh` when `writer` is true. So with the cache
disabled, no thread can create a fresh kf file through this API at all —
there is no writer-vs-writer create race for this branch to protect
against. The only caller that reaches `kf_open_file()` here is
`writer=0`, opening an already-existing file `O_RDWR` with no `O_CREAT`;
if the file doesn't exist yet, that open fails immediately with no
retry involved. The retry loop below still incidentally runs for this
branch too (it's the same shared code in `kf_open_file()`, not a
separate copy), but it's defensive-only there — tolerating a
transiently-invalid header from some cause other than an intra-process
create/create race (this plan's stated scope, per "Concurrency scope"
above), not closing one. Kept because it's cheap and harmless, not
because this branch needs it.

So the original bounded-retry-on-magic fix stays as the correctness
backstop specifically for the table-full fallback. Locate the exact
block (now including the Task 1 hold from 1b, so match on the tail of
it):

```c
        /* Stamp magic + version; counters start at 0. */
        hdr->magic = SLOTCASK_KF_MAGIC;
        hdr->version = SLOTCASK_KF_VERSION;
        hdr->total = 0;
        hdr->deleted = 0;
        msync(m, SLOTCASK_KF_HDR_SIZE, MS_ASYNC);
    } else if (hdr->magic != SLOTCASK_KF_MAGIC) {
        /* Magic missing/wrong — pre-release we don't migrate. */
        munmap(m, want); close(fd);
        return -1;
    }
```

Replace it with:

```c
        /* Stamp version/counters first, magic last, and publish the
           magic with a release store: a reader that observes magic via
           the matching acquire load below is then guaranteed to see
           version/total/deleted too, not just on x86's TSO but on
           ARM64 (this project targets both). Plain (non-atomic)
           read/write of a shared mmap'd field from multiple threads is
           also exactly what TSan's race detector flags; the ordering
           fix and the TSan fix are the same fix. hdr->magic stays a
           plain uint32_t (this is a byte-exact on-disk layout, not a
           process-local struct we can freely mark _Atomic) — these are
           the same GCC/Clang __atomic builtins already used elsewhere
           in this file on plain fields (e.g. g_kfcache_count). This
           backstop closes the genuinely uncoordinated create/create
           race for the inflight-table-exhausted fallback (see 2j in the
           plan doc); it also runs, harmlessly and incidentally, for the
           g_kfcache-disabled read-only path, which cannot itself race
           on file creation (writer=1 returns ENODEV before reaching
           this function when the cache is disabled). */
        hdr->version = SLOTCASK_KF_VERSION;
        hdr->total = 0;
        hdr->deleted = 0;
        __atomic_store_n(&hdr->magic, SLOTCASK_KF_MAGIC, __ATOMIC_RELEASE);
        msync(m, SLOTCASK_KF_HDR_SIZE, MS_ASYNC);
    } else if (__atomic_load_n(&hdr->magic, __ATOMIC_ACQUIRE) != SLOTCASK_KF_MAGIC) {
        /* Magic missing/wrong. Two causes look identical here: genuine
           corruption, or a second opener that lost the O_EXCL race
           above and is reading the header before the true creator has
           stamped magic a few lines above. Bounded and short: the true
           creator's own gap between ftruncate and the stamp is a
           handful of instructions plus one mmap() syscall, so this
           resolves the legitimate race almost immediately without
           meaningfully delaying the genuinely-corrupt case. */
        int attempt;
        for (attempt = 0; attempt < KF_OPEN_MAGIC_WAIT_ATTEMPTS; attempt++) {
            struct timespec wait_ts = { 0, KF_OPEN_MAGIC_WAIT_INTERVAL_MS * 1000000L };
            int wait_rc;
            do {
                wait_rc = nanosleep(&wait_ts, &wait_ts);
            } while (wait_rc != 0 && errno == EINTR);
            if (__atomic_load_n(&hdr->magic, __ATOMIC_ACQUIRE) == SLOTCASK_KF_MAGIC)
                break;
        }
        if (__atomic_load_n(&hdr->magic, __ATOMIC_ACQUIRE) != SLOTCASK_KF_MAGIC) {
            /* Exhausted the retry budget — genuinely corrupt/mismatched
               evidence, not a transient race. EILSEQ matches this
               file's existing convention for that distinction (see
               kf_abort_read_exact's "corrupt evidence" branch). */
            munmap(m, want); close(fd);
            errno = EILSEQ;
            return -1;
        }
    }
```

Add the two new constants directly above `kf_open_file()`. Locate this
exact line (confirmed at `slotcask.c:332`):

```c
/* Open + size + mmap a keyfile shard. Caller may NOT hold g_kfcache_lock when
```

and insert directly before it:

```c
/* Bounded retry budget for kf_open_file()'s magic-mismatch branch — a
   Layer 2 backstop for kfcache_acquire_ex's inflight-table-exhausted
   fallback, the one path that bypasses the single-flight guard and can
   still produce a genuine create/create race (see 2j in the plan doc):
   a second opener that lands between the true creator's ftruncate and
   its magic stamp waits up to ATTEMPTS * INTERVAL_MS (200ms) before
   concluding the header is genuinely corrupt rather than merely
   mid-initialization. Also runs, harmlessly, for the g_kfcache-disabled
   read-only path, which cannot itself race on creation (see 2j). */
#define KF_OPEN_MAGIC_WAIT_ATTEMPTS    100
#define KF_OPEN_MAGIC_WAIT_INTERVAL_MS 2

/* Open + size + mmap a keyfile shard. Caller may NOT hold g_kfcache_lock when
   the file system call could block, so we do the heavy lifting outside the
   table mutex (matching bt_open_file's contract in btree.c). */
```

**2k. Extend Task 1's test: assert exactly one real open occurred.**

This is what actually proves the guard eliminated the redundant
`kf_open_file()` call, not just the error — a call-count assertion is
meaningless until `kf_open_file_call_count` exists (2b/2i), which is why
it lands here instead of in Task 1. In
`src/test/cases/test_kf_open_file_race.c`, locate this exact line:

```c
    db->kf_open_create_test_hold_ms = RACE_HOLD_MS;
```

and insert directly after it:

```c
    db->kf_open_create_test_hold_ms = RACE_HOLD_MS;
    db->kf_open_file_call_count = 0;
```

Then locate this exact block:

```c
    pthread_join(creator, NULL);
    ASSERT_EQ_INT(cargs.rc, 0, "creator's own acquire must succeed");

    db->kf_open_create_test_hold_ms = 0;
```

and insert the new assertion between them:

```c
    pthread_join(creator, NULL);
    ASSERT_EQ_INT(cargs.rc, 0, "creator's own acquire must succeed");
    ASSERT_EQ_INT((int)db->kf_open_file_call_count, 1,
        "exactly one thread should have called kf_open_file() for this "
        "path — the racer must wait on the guard, not open it too");

    db->kf_open_create_test_hold_ms = 0;
```

**Edge cases / invariants this task must preserve:**
- The guard removes the timing dependency the earlier bounded-retry-only
  design had: the racer in Task 1's test now waits on a condvar with no
  timeout, woken exactly when the creator finishes, so `RACE_HOLD_MS`
  only needs to be long enough to reliably land the racer's attempt
  *after* the creator has reserved its inflight slot and *before* it
  finishes installing — it no longer has to fit inside any retry budget.
  `KF_OPEN_MAGIC_WAIT_ATTEMPTS * KF_OPEN_MAGIC_WAIT_INTERVAL_MS` (200ms,
  2j) is now backstop-only and this test does not exercise it (it never
  disables the kfcache or exhausts the inflight table) — don't add a
  timing coupling between the two that doesn't exist anymore.
- The explicit release in 2h (before the recursive call) is easy to
  regress silently — a future edit to that branch that removes it
  wouldn't fail any existing test until two threads happen to race on a
  path that also loses the install race after both already hold/held an
  inflight reservation, which `test-kf-open-file-race` does not
  specifically construct. Flag this exact interaction (self-deadlock via
  recursion into `kfcache_acquire_ex` while still holding an inflight
  reservation) explicitly during review of this diff.
- `g_kfcache_lock` must be held for the entire inflight-table
  check-and-reserve sequence (2g) and released before, not during,
  `kf_open_file()` — this preserves the existing
  "no blocking I/O under the table lock" invariant; only a *second*
  thread waiting on the condvar blocks under the lock, and
  `pthread_cond_wait` releases it for the duration of the wait.
- No signature changes anywhere (`kf_open_file`, `kfcache_acquire_ex`,
  `kfcache_acquire`) — confirmed no caller of `slotcask_open` /
  `kfcache_acquire` branches on `errno` (checked `query_schema.c:1319`,
  `query_find.c:1036-1039`, and the only `errno == EEXIST` check in
  `db/*.c` is `kf_open_file`'s own O_EXCL fallback at line 354, unrelated
  to this branch) — so introducing `errno = EILSEQ` on the Layer 2
  backstop path changes no caller's control flow, only what gets
  logged/reported.
- The Layer 2 retry loop must remain bounded (no `while(1)`) — an
  actually-corrupt file must still fail, just after the budget, not
  hang.
- Don't hand-roll a timeout on `pthread_cond_wait` in the guard — the
  guard's whole advantage over Layer 2 is that it has no ceiling to
  tune; a waiter is always woken by the matching
  `kf_open_inflight_release()` broadcast, never by a clock.

### Task 3: sanitizer gate (before calling this done)

Per `AGENTS.md`'s standing exception (diff touches `kfcache`, shared
cached state, and adds a new mutex/condvar hand-off):

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all   # x3
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all   # x3
```

Specifically watch for:
- Any TSan report on `g_kf_open_inflight[]` or `kf_open_file_call_count`
  in `test-kf-open-file-race` — every access must be under
  `g_kfcache_lock` (or, for the call counter, via `__atomic_fetch_add`)
  per 2b/2g/2i above; a report here means a code path was missed.
- A TSan deadlock report naming `kfcache_acquire_ex` and itself — the
  signature of the self-recursion issue fixed in 2h regressing. If this
  fires, re-check that the explicit `kf_open_inflight_release()` call
  before the recursive call in 2h is still present and still runs before
  the recursive call, not after.
- A TSan report on `hdr->magic` — this is the race the Layer 2
  acquire/release pair (2j) is meant to close for the inflight-table-
  exhausted fallback, the one path that bypasses the guard and can
  genuinely race on file creation. If TSan reports it on any other
  path — including the g_kfcache-disabled path, which per 2j cannot
  itself create a competing fresh file — the guard (or the "disabled
  path can't race on creation" analysis in 2j) has a gap; find it rather
  than papering over it with a suppression.

Also run `./build/bin/shard-db-test run test-kf-open-file-race`
standalone a handful of times outside the full suite to confirm it's not
flaky on its own before trusting a green full-suite run.

If any new finding surfaces outside this test, root-cause and fix it
inline if simple, or stop and write it up as its own
`docs/plans/<date>-<slug>.md` per the same standing exception this doc
was created under — do not fold an unrelated finding into this fix.
