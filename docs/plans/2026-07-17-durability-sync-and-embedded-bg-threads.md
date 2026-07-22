# Periodic durability sync + embedded background maintenance threads (Findings 2 & 9)

Source: `docs/plans/2026-07-16-storage-durability-and-recovery-findings.md`,
Findings 2 and 9.

Status: **ready to execute**. This revision resolves the cache-eviction,
uncached-write, sync-error, bitmap-locking, embedded-test, logging, warmup-default,
and same-process-reopen issues found during plan review.

## Outcome and guarantee

Implement a default-on durability-sync thread in daemon and embedded mode, and use
one lifecycle module for durability sync, auto-vacuum, auto-reshard, and warmup.

`DURABILITY_SYNC_MS` is a **target maximum dirty age on healthy storage**, not a
hard real-time or transactional durability guarantee:

- cached writes are marked dirty and periodically `msync(MS_SYNC)`'d;
- an entry that cannot be try-locked on its first eligible pass is blocking-locked
  after it reaches the configured dirty-age target, preventing indefinite
  try-lock starvation;
- dirty ordinary evictions are synchronously flushed before their mappings can be
  discarded;
- mutating acquisitions never use an untracked mapping: transient contention
  and identity races retry until a cache slot is obtained, blocking on real
  lock acquisition rather than spinning, while concrete I/O/resource failures
  return before mutation and read-only acquisitions may retain the uncached
  fallback;
- failed syncs remain dirty and are retried;
- shutdown retains its unconditional `MS_SYNC` backstop.

The elapsed bound can still be exceeded by an in-progress writer, a long earlier
sync in the same pass, or a device/filesystem error. Logs and documentation must
describe the setting as a target/attempt interval, not promise “at most exactly X
milliseconds.”

This work covers mapped **file contents**. It does not turn every acknowledged
operation into a WAL-style durable transaction, and it does not add missing parent
directory `fsync`s for ordinary file creation. Structural replace/rebuild paths
must retain their existing `MS_SYNC` + rename + parent-directory `fsync` protocol.
Full create/rename metadata durability is a separate finding.

## Resolved decisions

1. `DURABILITY_SYNC_MS` defaults to `1000`; `0` explicitly disables it; nonzero
   values below `50` are rejected and leave the prior/default value unchanged.
2. A configured durability thread is required infrastructure. If it cannot be
   created, daemon startup or `shard_db_open()` fails cleanly instead of silently
   running without the advertised protection.
3. Auto-vacuum, auto-reshard, and warmup remain optional. Failure to create one of
   those threads logs a warning but does not fail database startup.
4. Daemon warmup keeps its current default of `async`.
5. Embedded warmup defaults to `off` unless `WARMUP=` was explicitly present in
   `db.env`; explicit `off`, `sync`, and `async` are honored. This avoids adding
   surprise background startup I/O to existing embedders while still fixing the
   missing-thread behavior for callers that opted in.
6. Every asynchronous maintenance thread is joinable and is joined before pools,
   caches, mutexes, or the `ShardDb` instance are torn down.
7. `server_running` remains process-global for now because the public API already
   enforces one open `ShardDb` per process. Start sets it to `1`; stop sets it to
   `0`; same-process open→close→open is covered by a dedicated harness test.
8. Ordinary eviction and structural discard are distinct operations. A dirty
   ordinary eviction must sync. A mapping may be discarded without syncing only
   when the caller is deleting it or has already durably published a replacement.
9. `used` remains a plain `int`. New lock-free reads use the repo's existing
   `__atomic_load_n`/`__atomic_store_n` builtins. Do not call C11
   `atomic_load_explicit` on a pointer to the non-atomic `used` field.
10. Writer-side slot acquisition (`must_cache=1`) retries until it obtains a
    tracked slot when the only obstacle is transient slot contention or a lost
    identity/install race; it does not fail out on a bounded attempt count and
    never falls back to an untracked mapping. Non-contention failures still
    propagate with their original `errno`: an uninitialized required cache
    (`ENODEV`), allocation or requested-file `open`/`fstat`/`ftruncate`/`mmap`
    failures, and eviction-sync failure when no other candidate can be used.
    `EBUSY` is never returned merely because all slots are temporarily held.
    This intentionally trades an uncapped write-path latency tail under
    sustained cache pressure for never propagating a write failure caused
    merely by transient slot contention. See Task 2c.

## Core invariants

- A cache slot is published in this order: identity/map fields, `dirty=0`,
  `dirty_since_ms=0`, then `used=1`.
- A slot is never reused with dirty state inherited from its previous mapping.
- A cached mutation changes `dirty` from 0→1 and records the first dirty timestamp.
  Further writes do not move that timestamp forward.
- Every acknowledged mmap mutation belongs to a published cache entry. A writer
  waits/retries when transient contention or an identity race prevents a tracked
  install, propagates concrete non-contention failures before mutation, and
  never silently falls back to an untracked mapping.
- The sweep claims work by clearing `dirty` before `MS_SYNC`. A concurrent segment
  writer can therefore set it back to 1 and force another pass.
- `msync` success leaves the claimed generation clean. `msync` failure restores
  dirty state using the earliest relevant timestamp and logs `errno`.
- Only successful `msync` calls increment `synced`; failures increment `failed`.
- An ordinary dirty entry cannot be unmapped merely because the sweep has not
  reached it yet.
- A sweep holds the entry rwlock for identity verification, dirty claim, `msync`,
  and final bookkeeping. Every eviction path, including bitmap eviction, honors
  the same rwlock.
- `bg_threads_stop()` completes before cache or parallel-pool shutdown.

## Task 0 — Baseline and deterministic sync test seam

Before changing behavior, capture these baselines:

```bash
rtk ./build/bin/shard-db-test run test-auto-vacuum
rtk ./build/bin/shard-db-test run test-auto-reshard
rtk ./build/bin/shard-db-test run test-auto-reshard-shutdown-race
rtk ./build/bin/shard-db-test run test-warmup-vacuum-race
rtk ./build/bin/shard-db-test run test-rebuild-txn-embedded-lock
```

Add a small internal wrapper used by every new durability-related synchronous
flush:

```c
int durability_msync(void *addr, size_t len);
```

Production behavior is exactly `msync(addr, len, MS_SYNC)`. Under `TEST_BUILD`,
provide test-only controls to:

- fail the next N calls with a chosen `errno`;
- count successful and failed attempts;
- reset/read those counters.

Keep the controls internal (`types.h` or a test-only header), not in
`shard_db.h` and not in `db.env`. This seam is required to prove that failed
syncs are re-dirtied and that dirty eviction paths actually call the blocking
sync helper. Existing structural `MS_SYNC` calls need not be converted unless a
test specifically needs to observe them.

This fault injector is **new test infrastructure**; the repository's existing
test knobs are delay/pause hooks, not syscall-failure injection. Keep it narrow:
only `durability_msync` consults it, production builds contain no mutable fault
state, TEST_BUILD state is atomic or mutex-protected, and every test resets it
in cleanup so a parallel/later case cannot inherit a pending failure.

Add new test files to `build.sh`'s explicit `src/test/cases/*.c` source list; the
build does not glob test sources.

## Task 1 — State and configuration

### 1a. Cache-entry state

In `src/db/shard_db_internal.h`, add both fields to `BtCacheEntry`,
`BmCacheEntry`, `KfCacheEntry`, and `SegCacheEntry`, immediately after `used`:

```c
_Atomic int      dirty;
_Atomic uint64_t dirty_since_ms;
```

`dirty_since_ms` records the first transition from clean to dirty. `calloc`
initialization is not sufficient because slots are reused; every install and
detach path must explicitly reset both fields as described in Tasks 2 and 3.

### 1b. Instance configuration and lifecycle fields

Add to `ShardDb`:

```c
int durability_sync_ms;
int warmup_explicit;          /* WARMUP= was present in db.env */

pthread_t bg_auto_vac_tid;
int       bg_auto_vac_spawned;
pthread_t bg_auto_reshard_tid;
int       bg_auto_reshard_spawned;
pthread_t bg_warmup_tid;
int       bg_warmup_spawned;
pthread_t bg_durability_tid;
int       bg_durability_spawned;
```

Add `g_durability_sync_ms` alongside the existing config macros. Thread IDs and
spawn flags are accessed only through the passed `ShardDb *`.

In `db_defaults_set`:

```c
db->durability_sync_ms = 1000;
db->warmup_explicit = 0;
```

Keep the existing `warmup_mode="async"` daemon default.

### 1c. Parser

In `config.c`:

- parse `DURABILITY_SYNC_MS=` with `strtol`, requiring the entire value to be a
  valid base-10 integer;
- accept `0` or values `>=50`;
- reject negative, malformed, overflowed, and 1–49 values without turning them
  into the `0` off-switch;
- set `warmup_explicit=1` only when a valid `WARMUP=off|sync|async` entry is
  parsed.

## Task 2 — Mark every mutation and prohibit untracked writers

Add a shared inline helper for cached entries:

```c
static inline void durability_mark_dirty(_Atomic int *dirty,
                                         _Atomic uint64_t *dirty_since_ms);
```

On the 0→1 transition it stores `now_ms()` as the first dirty time. Preserve an
existing nonzero timestamp so concurrent writes cannot move the age forward.
It uses release/acquire-capable atomic ordering; do not use all-relaxed
operations for the segment path, whose writer and sweeper can both hold rdlocks
concurrently. If a clear-vs-redirty race temporarily produces `dirty=1` with
`dirty_since_ms=0`, the sweep treats it as already due and takes the blocking
path; it must never treat a missing timestamp as “fresh.”

### 2a. Cached kf/btree/bitmap mutations

- `kfcache_release`: if `h->writer` and `slot>=0`, mark the entry before
  unlocking.
- `bt_release`: after publishing any grow-time remap, if `bt->writer` and
  `slot>=0`, mark the entry before unlocking.
- `bm_close`: if `bm->writer` and `slot>=0`, mark the entry before unlocking.

### 2b. Segment mutations

The segment write path deliberately uses rdlocks, so do not gate dirty marking
on `SlotcaskSegHandle.writer`. Mark at the four actual mutation sites:

- `seg_write_record`;
- `seg_write_record_varlen`;
- `seg_write_flag`;
- the inline VARIABLE tombstone flag write in
  `slotcask_tombstone_and_push_back`.

### 2c. No uncached writer fallback

All four caches currently have `slot=-1` fallbacks. Those mappings have no dirty
entry to retain after a failed sync, so mutating operations must not use them:

**Execution order:** land the behavior-preserving signature migration from 2d
first, add dirty marking/state, complete Task 3's sync-safe, failure-aware
blocking eviction, and only then switch mutation sites to the required-cache
policy described here. There must be no intermediate commit that enables
`must_cache=1` while eviction can still discard dirty state or collapse a
concrete I/O/resource failure into generic slot exhaustion.

- kf, btree, and bitmap acquires may return an uncached mapping only when
  `writer==0`;
- when a writer cannot evict/install a tracked slot solely because candidates
  are held or identities changed, it waits/retries; a concrete non-contention
  error is returned before mutation and the containing write/index operation
  propagates it instead of acknowledging an untracked mutation;
- add an explicit `mutating`/`must_cache` parameter to segment acquire, separate
  from its rwlock mode. Segment writers continue to take an rdlock for their
  unique reserved offsets, but `must_cache=1` prevents `slot=-1` at the four
  mutation sites;
- read-only callers retain the current uncached fallback behavior;
- offline maintenance paths initialize the needed caches before mutating, as
  the existing migrate/compact/rebuild commands already do;
- audit every `slot=-1` return so no writer reaches it indirectly through an
  install-race or retry-limit branch.

The termination policy is fixed, not left to implementation choice, and it is
the one deliberate exception to “no unbounded retry” elsewhere in this plan
(superseding an earlier bounded-attempt/`EBUSY` revision of this section — see
decision 10):

- a `must_cache=1` acquisition whose only obstacle is contention or a lost
  identity/install race retries until it obtains a tracked slot. It does not
  stop at any fixed attempt count, and it never falls through to an uncached
  mapping;
- victim selection for `must_cache=1` uses each cache's existing **blocking**
  drop-slot path (`segcache_drop_slot`, `kfcache_drop_slot`, and the btree/bmap
  equivalents from Task 3), i.e. a real `pthread_rwlock_wrlock` on the chosen
  victim, not `trywrlock`. This is a blocking wait with no attempt or time
  deadline; it does not busy-spin, and it is expected to finish when the
  holder's in-progress operation releases the lock;
- losing an install race, or finding the victim's identity no longer matches
  once its wrlock is obtained, is expected under contention and simply drives
  another attempt — it is not a failure and must not be counted toward any
  exhaustion limit;
- `segcache_acquire`'s existing `retries >= 4` counter keeps its original,
  unrelated purpose (stale-entry identity reconciliation, for any acquire,
  reader or writer). Reaching it does **not** terminate a `must_cache=1` call:
  dispose any freshly opened local fd/mapping and loop back into a fresh
  probe/open/install attempt instead of returning `-1` or an uncached mapping;
- waiting is reserved for contention and identity/install races. A required
  cache that was never initialized (`!g_segcache` and the kf/bt/bm
  equivalents) fails before opening a mapping with `errno=ENODEV`. Allocation
  and requested-file `open`/`fstat`/`ftruncate`/`mmap` failures also return
  immediately with their original `errno`; retrying them inside cache
  acquisition would turn persistent storage/resource failure into a hang;
- eviction scans preserve the first concrete sync/drop error and continue
  trying other distinct candidates. A dirty candidate whose
  `durability_msync` fails remains installed and dirty. If every usable
  candidate fails for a non-contention reason, return that preserved `errno`.
  If any remaining candidate is merely busy, block on a busy candidate and
  retry instead, because a contention-resolvable path still exists. Never map
  temporary contention to `EBUSY`;
- kf, btree, and bitmap writer paths get the same treatment: replace their
  existing bounded-retry-then-uncached terminal return with a genuine
  retry-until-slot loop for `writer==1` when the obstacle is contention or an
  identity race, while preserving concrete non-contention errors;
- read-only callers are unaffected and retain the current
  bounded-retry-then-uncached-fallback behavior.

This intentionally makes write latency unbounded under sustained, pathological
cache pressure (every candidate perpetually busy) rather than ever surfacing a
write failure caused only by transient slot contention. That trade — an
uncapped latency tail vs. a caller-visible write error for a condition that
resolves itself as soon as any holder releases — is the point of this
decision; do not reintroduce a bounded attempt count or an `EBUSY` return for
ordinary `must_cache=1` slot contention. A writer must still never mutate
first and discover only at release time that the write cannot be represented
in the retry machinery.

### 2d. Segment-acquire call-site migration

Treat the `segcache_acquire` signature change as its own mechanical subtask. It
has more than 30 call sites in `slotcask.c`, plus wrappers such as direct-acquire
helpers, so compile-time migration is deliberately separated from behavior:

1. add the explicit `must_cache` parameter;
2. update **every** direct and wrapper call site, initially passing `0` so this
   commit is behavior-preserving;
3. build and run the segment/slotcask-focused tests;
4. after Task 3 has made eviction sync-safe, blocking for contention, and able
   to propagate non-contention failures, change only the four mutation sites
   from Task 2b to `must_cache=1`;
5. use the final `rtk rg` audit to confirm there is no call relying on an
   implicit/default argument and no mutation site still passes `0`.

Do not combine this mechanical migration with dirty-state or eviction changes;
keeping it isolated makes omissions and accidental policy flips visible in the
diff.

Under `TEST_BUILD`, add a narrow countdown hook that makes the next N
post-wrlock segment identity checks behave as lost identity races. Task 5c sets
N above the old four-attempt threshold and proves that `must_cache=1` reprobes
and eventually succeeds after the forced races are consumed. This new test
instrumentation is internal, synchronized, reset in test cleanup, and absent
from production behavior/configuration. Do not use an attempt counter to test a
thread blocked in `pthread_rwlock_wrlock`: no attempts occur while that call is
waiting. Test contention separately with a completion flag/latch.

### 2e. Slot installation

At every cache miss installation, before publishing `used=1`:

```c
atomic_store(&e->dirty, 0);
atomic_store(&e->dirty_since_ms, 0);
```

Cover kfcache, segcache, btcache, and bmcache. Also clear both fields after a
successful detach/discard, before the slot can be reused.

## Task 3 — Make eviction durability- and lock-safe

Introduce an internal drop reason:

```c
typedef enum {
    CACHE_DROP_EVICT,       /* live mapping leaving cache: sync if dirty */
    CACHE_DROP_DISCARD      /* deleted or durably superseded mapping */
} CacheDropReason;
```

Do not infer the reason from `MS_ASYNC` call location. Pass it explicitly from
each caller.

### 3a. Bitmap locking prerequisite

Fix `bm_cache_drop_slot` before adding the sweep:

- change it to return success/busy/failure;
- never detach or unmap an entry without its wrlock;
- follow the kfcache/segcache identity-recheck pattern: snapshot identity under
  `g_bm_cache_lock`, release the table lock before waiting for the entry wrlock,
  reacquire the table lock, then verify `used` and path/identity;
- nonblocking/read-only LRU selection may try another candidate if the entry is
  busy. After a finite scan finds no immediate slot, a `must_cache=1` writer
  blocks on a selected busy victim and retries after any identity race;
- invalidation may block rather than unmapping beneath a holder. Schema/index
  maintenance callers already have object-level exclusion; the missing-file
  creation path must recheck path/entry state after acquiring the wrlock instead
  of assuming that exclusion.

This fixes the existing holder-vs-LRU unmap race as well as making the new
sweeper's rdlock meaningful.

### 3b. Required-sync eviction

For `CACHE_DROP_EVICT`:

1. hold the entry wrlock and verify identity;
2. atomically claim dirty state;
3. if dirty, call `durability_msync` before detach/unmap;
4. on failure, restore dirty state/timestamp and leave the entry installed;
5. on success, detach, clear dirty metadata, and permit slot reuse.

Apply this to:

- `kfcache_drop_slot` ordinary LRU calls;
- `segcache_drop_slot` ordinary LRU calls;
- btree LRU eviction, refactored so the entry remains installed and protected by
  its wrlock until a required sync succeeds; release/reacquire the table lock
  with an identity recheck rather than detaching first and becoming unable to
  restore a failed sync;
- `bm_cache_drop_slot` ordinary LRU calls.

If all candidates are busy, read-only acquisition may use its existing uncached
fallback while writer acquisition blocks/retries until a slot is available. If
all usable candidates instead fail required sync/drop work, writer acquisition
returns the preserved underlying error rather than hanging or escaping the
dirty-tracking system. When a scan contains both failures and busy candidates,
the writer waits on a busy candidate before concluding that only hard failures
remain.

### 3c. Structural discard

Use `CACHE_DROP_DISCARD` only when one of these is true:

- the file is being deleted under the object/schema maintenance lock; or
- a replacement was already `MS_SYNC`'d, renamed into place, and its parent
  directory fsync'd.

Document that precondition at each call site. Prefix invalidations for
drop/rebuild and the old mapping in `kfcache_resplit_locked` are discard paths.
Stale-inode detection after a rename may discard the old inode only after
confirming the path now names a different inode.

### 3d. `MS_ASYNC` audit

After 3a–3c, remove/replace the seven cache-related `MS_ASYNC` calls:

- ordinary LRU sites become conditional `durability_msync` under
  `CACHE_DROP_EVICT`;
- structural invalidation/resplit sites become documented
  `CACHE_DROP_DISCARD` paths with no misleading async call.

Leave the one-time fresh-kf-header `MS_ASYNC` call alone in this change; the
writer-release dirty mark covers its eventual periodic sync, and changing the
creation protocol belongs with the separate metadata-durability scope.

Keep all shutdown-time and rebuild-finalization `MS_SYNC` calls. Check and log
their failures where the current function can do so safely.

## Task 4 — Durability sweep and failure semantics

Add:

```c
typedef struct {
    int kf_synced;
    int seg_synced;
    int bt_synced;
    int bm_synced;
    int failed;
    int skipped;
    int escalated;
} DurabilitySyncStats;
```

Implement one cache-entry helper and `durability_sync_one_pass`. The helper:

1. uses `__atomic_load_n` for the plain `used` field;
2. reads `dirty` and `dirty_since_ms`;
3. uses `pthread_rwlock_tryrdlock` while dirty age is below the configured
   interval;
4. uses blocking `pthread_rwlock_rdlock` once dirty age reaches the interval,
   preventing indefinite starvation behind kf/btree/bitmap writers;
5. rechecks identity, `used`, and dirty state under the lock;
6. claims the dirty generation by clearing `dirty` before syncing and preserves
   the claimed timestamp;
7. calls `durability_msync`;
8. increments the cache's synced count only on success;
9. on failure, restores `dirty=1`, restores the earliest timestamp if a
   concurrent writer already re-dirtied the entry, increments `failed`, and logs
   cache/path/length/`errno`;
10. unlocks the entry.

Segment writers may continue under a concurrent sweep because both use rdlocks.
The clear-before-sync protocol ensures a write that begins after the claim
causes another pass.

The thread:

- binds `g_db = g_shard_db_instance`;
- blocks SIGTERM/SIGINT as the other maintenance threads do;
- uses a monotonic next-deadline schedule so pass duration does not silently add
  permanent drift to every later interval;
- sleeps in chunks no larger than 100ms so stop/join remains responsive;
- logs:

```text
DURABILITY-SYNC tick: kf=N seg=N bt=N bm=N failed=N skipped=N escalated=N in Xms
```

Do not log `synced` for attempted or failed calls.

Expose `durability_sync_one_pass` to tests only under `TEST_BUILD`; keep it
internal in production.

## Task 5 — Durability tests

### 5a. Per-cache daemon smoke test

Add `src/test/cases/test_durability_sync.c` and register it explicitly in
`build.sh`.

The test:

- starts a daemon with `DURABILITY_SYNC_MS=100`, `WARMUP=off`, and a normal
  cache size;
- creates an object with fields `v:int` and `flag:bool` and indexes
  `v:btree` and `flag:bitmap`;
- inserts/updates a record so kf, segment, btree, and bitmap mappings are all
  mutated;
- polls up to two seconds for a durability tick with `kf>=1`, `seg>=1`,
  `bt>=1`, `bm>=1`, and `failed=0` across the observed post-start tick lines
  (the four caches need not all be nonzero in the same tick);
- verifies the record and both indexed queries still return the correct data;
- shuts the daemon down normally.

Scan every `*-info.log` file under the configured `LOG_DIR`. Do not inspect the
stdout/stderr capture (`daemon.log`), and do not assume a filename such as
`shard-db.log`; normal `LOG_INFO` routing is date-prefixed `*-info.log`.

### 5b. Failure/retry unit test

Add `test_durability_sync_failures.c` using the Task 0 test seam:

- dirty one entry;
- fail the next `durability_msync` call with `EIO`;
- run one pass and assert `failed=1`, synced count remains zero, and the entry is
  still dirty with its original/earlier dirty timestamp;
- reset the hook, run another pass, and assert the sync succeeds and clears it.

Cover at least one segment entry because it exercises concurrent-rdlock dirty
semantics, plus one wrlock-serialized cache entry.

### 5c. Eviction and writer-fallback paths

Add `test_durability_sync_cache_paths.c`:

- use the minimum cache size and enough distinct files to force ordinary LRU
  eviction before the periodic thread runs;
- assert a dirty ordinary eviction calls `durability_msync` before unmap;
- inject `EIO` and assert the dirty entry is retained rather than discarded;
- hold all eviction candidates busy, start a `must_cache=1` acquire on another
  thread for a distinct, not-yet-cached path that requires installation, and
  assert via an atomic completion flag/latch that it remains blocked — does not
  return and does not receive or mutate a `slot=-1` mapping — for as long as
  every candidate is held, while a reader can still use the uncached fallback
  concurrently;
- release the busy holder and, within the test deadline, assert the blocked
  `must_cache=1` acquire then completes successfully. This is the regression
  against accidentally reintroducing a bounded attempt count or an `EBUSY`
  return for ordinary transient slot contention;
- separately set the Task 2d forced identity-race countdown to at least five,
  run a segment `must_cache=1` acquisition, and assert it consumes every forced
  race and then succeeds. This deterministically proves that the existing
  `retries >= 4` reconciliation threshold cannot terminate a required-cache
  writer;
- exercise the cache-disabled `must_cache=1` branch and require immediate
  `ENODEV` without opening a mapping;
- use an invalid requested-file path to force a deterministic `open` failure
  and assert, under a test deadline, that the original `errno` is returned
  before mutation rather than retried as though it were slot contention;
- force required eviction sync to fail with `EIO` for every eligible dirty
  victim and assert, under a test deadline, that all failed victims remain
  installed/dirty and the writer returns `EIO` rather than hanging. Also cover
  one failed candidate followed by another syncable candidate and require the
  writer to use the latter successfully;
- assert a reused slot begins with `dirty=0` and `dirty_since_ms=0`;
- exercise bitmap LRU while a holder owns the entry rdlock and assert eviction
  reports busy/chooses another entry instead of unmapping beneath the holder.

Run these tests under ASan and TSan in CI where those modes already exist.

## Task 6 — Central background-thread lifecycle

In `types.h`, add:

```c
typedef enum {
    BG_RUNTIME_DAEMON,
    BG_RUNTIME_EMBEDDED
} BgRuntimeMode;

int  bg_threads_start(ShardDb *db, BgRuntimeMode mode);
void bg_threads_stop(ShardDb *db);
```

Implement both in `server.c` next to the existing thread functions.

### Start rules

`bg_threads_start`:

1. sets `server_running=1` and zeroes all spawned flags;
2. starts durability first when `durability_sync_ms>0`;
3. returns `-1` immediately if the required durability thread cannot be
   created;
4. starts enabled auto-vacuum and auto-reshard threads, warning on allocation
   or create failure;
5. chooses warmup policy from runtime mode:
   - daemon: current `warmup_mode` default/explicit value;
   - embedded: `off` unless `warmup_explicit`, otherwise the explicit value;
6. runs `sync` warmup inline and makes `async` warmup joinable;
7. returns `0` on successful required startup.

Optional thread arguments are freed on create failure. Every failure produces a
clear diagnostic naming the thread. Daemon mode uses the initialized logger;
embedded startup also writes the diagnostic to `stderr` because callers cannot
register `shard_db_set_log_handler` until after `shard_db_open` returns.

### Stop rules

`bg_threads_stop`:

- sets `server_running=0` before joining;
- joins durability, auto-vacuum, auto-reshard, and warmup if spawned;
- resets thread IDs and spawned flags;
- is safe after partial startup and safe when durability was disabled;
- is called exactly once for each successful start.

### Daemon wiring

In `cmd_server`:

- remove the local auto-vacuum/auto-reshard/warmup/durability handles and spawn
  blocks;
- call `bg_threads_start(g_shard_db_instance, BG_RUNTIME_DAEMON)` after caches
  and CPU/I/O pools are initialized but before the work queue and request worker
  threads are spawned;
- on required-thread startup failure, remove the pid file, close the listening
  socket, shut down pools/caches/TLS/logging in reverse-init order, release the
  DB-root lock, and return nonzero;
- during normal shutdown, join request workers/in-flight writes first, then call
  `bg_threads_stop`, then tear down pools and caches;
- add the currently missing `bm_cache_shutdown()` immediately after
  `bt_cache_shutdown()` and before `slotcask_shutdown()`.

Retain the existing detailed shutdown-order comment, updating it to cover all
four maintenance threads and bmcache.

## Task 7 — Embedded wiring and executable harness

### 7a. Embedded open/close

In `shard_db_open`, after CPU/I/O pools are successfully initialized:

```c
if (bg_threads_start(db, BG_RUNTIME_EMBEDDED) != 0) {
    /* reverse pool/cache/init state, release lock, clear globals/guard */
    return NULL;
}
```

The failure path must:

- call `bg_threads_stop` safely if startup became partial;
- shut down I/O and CPU pools before cache cleanup;
- clear `g_shard_db_instance`;
- release the DB-root lock;
- free the instance;
- reset `g_instance_open` so a later open can succeed.

In `shard_db_close`:

1. bind `g_db=db`;
2. call `bg_threads_stop(db)`;
3. shut down I/O/CPU pools;
4. flush/shut down bt, bm, and slotcask caches;
5. clear `g_shard_db_instance` before freeing `db`;
6. destroy instance resources and reset the single-instance guard.

### 7b. Harness

Add `src/test/embedded_bg_harness.c`, linking it against the already-built
`build/bin/libshard-db.a` exactly like `embedded_lock_harness`; do not re-list all
daemon source files.

Usage:

```text
embedded-bg-harness <env-dir> <db-root> <hold-ms> <cycles> <callback-log>
```

The harness:

- `chdir`s to `env-dir` so `db.env` is loaded;
- uses the real public type and API: `ShardDb *db = shard_db_open(db_root)`;
- never passes `NULL` as the DB root;
- immediately registers a thread-safe `shard_db_set_log_handler` callback that
  appends to `callback-log`;
- on the first cycle, uses `shard_db_query` to create/write an indexed object so
  the durability thread has real dirty work;
- sleeps for `hold-ms`, verifies a count/query, then closes;
- repeats open→handler→query→close in the **same process** for `cycles=2`;
- writes explicit cycle markers to the callback log so the test can require a
  durability tick during each open lifetime;
- exits nonzero on any open/query/verification failure.

The handler is installed after open because that is the public API contract.
Startup log lines may be missed; the test relies on periodic tick lines emitted
after the handler is registered.

### 7c. Embedded test

Add `test_embedded_bg_threads.c` and register it explicitly in `build.sh`.

Use `DURABILITY_SYNC_MS=100` and `WARMUP=off` for the core lifecycle test. Fork
the harness with `cycles=2` and enforce a real timeout:

- poll `waitpid(..., WNOHANG)` up to five seconds;
- on timeout, terminate/reap only that child and fail the test;
- assert exit status 0;
- assert each cycle's callback-log section contains a durability tick with
  `failed=0`;
- assert the final data is readable after the second close.

Add two warmup-policy subtests with a pre-existing object:

- no `WARMUP=` line: embedded open/close completes without a warmup tick/done
  event, proving the resolved embedded default is off. Set the existing
  `WARMUP_TEST_DELAY_MS` hook so an accidentally-started default warmup would
  remain alive long enough for the post-open callback to observe it;
- explicit `WARMUP=async`: use the same delay hook and assert the callback sees
  both the delayed-task marker and `WARMUP done`, proving opt-in embedded warmup
  runs and is joined.

The existing daemon auto-vacuum/auto-reshard tests remain their end-to-end
behavior coverage. This test proves the shared embedded start/stop lifecycle,
same-process reuse, durability activity, explicit warmup policy, and prompt join.

## Task 8 — Documentation

Update:

- `docs/getting-started/configuration.md` with `DURABILITY_SYNC_MS`, default
  `1000`, `0=off`, 50ms floor, target-age semantics, and failure logging;
- `docs/getting-started/embedded-mode.md` to document the durability thread,
  background callbacks, close/join behavior, embedded warmup default-off unless
  explicitly configured, and same-process sequential reuse;
- `src/db/shard_db.h` comments to state that `shard_db_open` may return NULL when
  required background infrastructure cannot start and that `shard_db_close`
  joins internal maintenance threads;
- any generated/copied public header through the normal build, not by editing
  `build/bin/shard_db.h` directly.

Do not claim WAL/transactional durability or full directory-entry durability.

## Task 9 — Final verification

Run focused tests first:

```bash
rtk ./build/bin/shard-db-test run test-durability-sync
rtk ./build/bin/shard-db-test run test-durability-sync-failures
rtk ./build/bin/shard-db-test run test-durability-sync-cache-paths
rtk ./build/bin/shard-db-test run test-embedded-bg-threads
rtk ./build/bin/shard-db-test run test-auto-vacuum
rtk ./build/bin/shard-db-test run test-auto-reshard
rtk ./build/bin/shard-db-test run test-auto-reshard-shutdown-race
rtk ./build/bin/shard-db-test run test-warmup-vacuum-race
```

Then run:

```bash
rtk ./build/bin/shard-db-test run-all
rtk proxy env BUILD_MODE=asan ./build.sh
rtk proxy env BUILD_MODE=tsan ./build.sh
```

Use the repo's normal sanitizer test invocation after each sanitizer build. If
the sanitizer modes require narrower case selection in CI, at minimum run the
three durability tests, embedded lifecycle test, bitmap-index tests, cache race
tests, warmup race test, and auto-reshard shutdown-race test.

Finally audit:

```bash
rtk rg -n "MS_ASYNC" src/db
rtk rg -n "bg_threads_(start|stop)" src/db
rtk rg -n "durability_(mark_dirty|msync)|dirty_since_ms" src/db
rtk rg -n "segcache_acquire(_direct)?\\(" src/db/slotcask.c
```

Expected `MS_ASYNC` result: only the explicitly retained one-time fresh-kf-header
call remains.

## Suggested commit boundaries

1. `test: add narrow durability msync fault seam and regressions` (red tests)
2. `refactor: make segment cache fallback policy explicit` (all call sites pass
   the behavior-preserving read/allow value)
3. `feat: track dirty age on cached mutations` (segment call sites still pass
   the behavior-preserving value)
4. `fix: make cache eviction sync-safe, bitmap-lock-safe, and error-aware`
5. `feat: reject untracked writer fallbacks` (the four segment mutation sites
   switch to `must_cache=1` only after commit 4)
6. `feat: add retrying periodic durability sweep`
7. `refactor: centralize maintenance thread lifecycle`
8. `feat: run configured maintenance threads in embedded mode`
9. `fix: flush bitmap cache on daemon shutdown`
10. `docs: document durability interval and embedded thread policy`

Each commit must build; behavior-changing commits must run their focused tests
before proceeding. The full suite and sanitizer runs are required before the
branch is considered complete.
