# Plan: SEGV in slotcask_pool_rebuild_worker under ASan (RESOLVED)

## Status: fixed. Root cause was one layer above everything this doc traced.

The actual bug was the UAF fixed under
`docs/plans/2026-07-20-warmup-kftask-stale-sdb-uaf.md`: `warmup_kf_task_fn`
captured a raw `SlotcaskDb *sdb` pointer in `WarmupKfTask` during phase 1
(the serial tree walk) and dereferenced it later in phase 2 from an IO-pool
worker thread, with nothing preventing a concurrent `auto_vacuum_thread`
sweep from fully invalidating+freeing that same object in the gap between
capture and use — exactly the "warmup racing a concurrent rebuild" trigger
condition this doc documented empirically (see "Trigger context" below)
without identifying the mechanism. The fix (already landed on this branch,
`src/db/server.c`) drops the raw `sdb` field from `WarmupKfTask` entirely;
`warmup_kf_task_fn` now reloads the schema and re-resolves `sdb` fresh from
`slotcask_registry_get()` under `objlock_rdlock` at execution time, so it
never dereferences a pointer captured before the lock was taken.

This explains why every hypothesis below was correctly ruled out:
`slotcask_pool_rebuild_worker` and every kfcache-internal mutator of
`e->base` were never the problem — by the time `slotcask_pool_rebuild_worker`
ran, it was already operating through a stale/freed `SlotcaskDb*` handed to
it by the caller, no kfcache-entry-lock discipline could have prevented
that.

**Verification**: fresh full-suite ASan run (`BUILD_MODE=asan`, CI-matching
`ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1"`,
`./build/bin/shard-db-test run-all --jobs 2`) — 10613/10613 assertions
passed, 0 ASan/LSan findings, exit 0. `test-bitmap-index` standalone:
0/20 reproductions (previously 5/5). Dedicated regression coverage
(`test-warmup-vacuum-race`, `test-warmup-vacuum-norace`) both green (13/13,
15/15) in the same run.

## Original status (kept for history): not fixed, root cause not conclusively
identified. Found via the newly-broadened full-suite ASan run
(`detect_leaks=1`, full test suite instead of the old curated subset) on
`feat/durability-sync-embedded-bg-threads`.

## Severity

100% reproducible SEGV (`AddressSanitizer:DEADLYSIGNAL`, raw kernel SIGSEGV,
not an ASan-attributed heap error) when running `test-bitmap-index` standalone
under ASan (`./build/bin/shard-db-test run test-bitmap-index`, 5/5 runs with
default `ASAN_OPTIONS`). Also observed once in a full-suite run. This is a
crash, not a benign race — it currently blocks calling the broadened ASan CI
clean.

## Confirmed facts

- **Exact crash site**: disassembly (`objdump`/`addr2line`) cross-referenced
  against the ELF symbol table (`nm`/`objdump -t`) shows the faulting
  instruction (`cmpb $0x2,(%rax)`) falls within `slotcask_pool_rebuild_worker`'s
  compiled address range — consistent with the `kf[i].flag == 2` check in its
  loop (`slotcask.c:1564`). The raw ASan summary line (`slotcask.c:1400`,
  inside `kfcache_resplit_locked`) is a misleading line-table artifact from
  `-O2` reordering; the *function* attribution (via symbol-table address
  ranges, which optimization can't perturb the way it perturbs line tables)
  is independently confirmed.
- **Why ASan doesn't attribute it as a heap error**: `kh.map` points into a
  raw `mmap()`'d kf-shard file region, not a `malloc()`'d allocation. ASan's
  shadow-memory poisoning doesn't cover plain mmap regions by default, so a
  genuine post-`munmap()` dereference surfaces as a bare kernel SIGSEGV
  instead of an ASan "heap-use-after-free" report with a free-site stack
  trace — which is exactly the diagnostic information we're missing (see
  "Suggested next steps").
- **Trigger context**: reproduces specifically when warmup (async mode) runs
  concurrently with the test's `add-index force=true` rebuild step — i.e.
  this is a warmup-vs-concurrent-client-write race, not a single-threaded bug.
- This is **not new code from this branch**; it's pre-existing and newly
  surfaced by broadening the sanitizer workflows to the full suite.

## Ruled out (each verified by direct code reading, not inference)

1. **`kfcache_resplit_locked` missing lock exclusion** — initially suspected
   (its own body never touches `e->rwlock`), but all three call sites
   (`kf_put_new:1592`, `slotcask_pregrow_worker:1491`,
   `slotcask_compact_kf:7479`) call it while still holding the wrlock
   obtained by their own `kfcache_acquire(..., writer=1)` — which is held
   across the entire handle lifetime (`kfcache_acquire` returns with the lock
   held; `kfcache_release` unlocks it). A wrlock excludes concurrent rdlock
   holders on the same slot, so `slotcask_pool_rebuild_worker` (which holds
   rdlock for its *entire* loop, acquire-to-release — confirmed by reading
   its full body) cannot be running concurrently with a resplit on the same
   slot.
2. **`kfcache_drop_slot`** (LRU eviction and stale-entry-discard paths) —
   explicitly wrlocks `e->rwlock` before `munmap`, with a comment naming
   `slotcask_pool_rebuild_worker` as the exact hazard being excluded.
   Correct in both call modes (`trywrlock` during the eviction scan,
   blocking `wrlock` for the last-resort candidate).
3. **`kfcache_invalidate_prefix`** — also wrlocks `e->rwlock` before
   `munmap`. Correct.
4. **`kfcache_shutdown`** — does *not* take `e->rwlock` at all before
   `munmap`, which looked like a smoking gun, but it's only ever called
   after `bg_threads_stop()` has `pthread_join`'d `bg_warmup_tid` — verified
   in both shutdown sequences that exist in this codebase: the daemon path
   (`server.c` `cmd_server`, `bg_threads_stop()` at line 3902 strictly before
   `slotcask_shutdown()` at line 3910) and the embedded path
   (`embedded.c:shard_db_close`, same ordering at lines 466/472). Since
   `parallel_for_io` (see next point) blocks until every
   `slotcask_pool_rebuild_worker` instance finishes, joining `bg_warmup_tid`
   guarantees no pool-rebuild reader is still active by the time
   `kfcache_shutdown` runs.
5. **`parallel_for_io` async/non-blocking theory** — read the full
   implementation (`parallel.c:409-482`): it genuinely blocks the calling
   thread on `group.remaining` via `pthread_cond_wait` (or a help-drain loop
   for nested callers) until every dispatched task completes. Ruled out.
6. **Duplicate concurrent `slotcask_open()` fan-outs for the same object** —
   `slotcask_registry_get`'s `opening` flag + `pthread_cond_wait` on
   `g_reg_cond` correctly serializes concurrent misses on the same key
   (this exists specifically because of a prior incident — see its comment,
   "2026-07-03 hn-explorer incident"). Confirmed by reading the full function.
7. **The already-tracked `slotcask_registry_invalidate` UAF
   (`.tsan.supp`: "frees a SlotcaskDb that concurrent callers may still hold
   pointers to")** — plausible-looking lead, but does not explain *this*
   crash: `slotcask_registry_get` only publishes `db` into
   `g_reg[slot].db`/`used=1` *after* `slotcask_open()` — including its
   internal `parallel_for_io(slotcask_pool_rebuild_worker, ...)` fan-out —
   has fully returned (confirmed by reading `slotcask_registry_get` and the
   call site inside `slotcask_open` at `slotcask.c:3085`). So
   `slotcask_registry_invalidate` cannot reach a `db` whose own pool-rebuild
   workers are still in flight; that specific `SlotcaskDb` isn't
   registry-visible yet. (The underlying `SlotcaskDb`-refcounting gap this
   `.tsan.supp` entry describes is still real and still untouched — it's
   just not the mechanism for this particular SEGV.)

## What's still unknown

Every explicit mutator of `KfCacheEntry.base` (`kfcache_drop_slot`,
`kfcache_invalidate_prefix`, `kfcache_resplit_locked` via its caller's held
wrlock, `kfcache_shutdown` via join-ordering) is correctly excluded from a
concurrent `slotcask_pool_rebuild_worker` reader by `e->rwlock` — yet the
crash is 100% reproducible. Possible explanations, none confirmed:

- A protocol hole not yet found despite this level of tracing across two
  sessions (~12 functions read in full, both shutdown sequences verified,
  the io-pool implementation verified line-by-line).
- A fourth mutation path touching `e->base`/`KfCacheEntry` state that a
  `grep` for `->base =` / `munmap(e->base` didn't catch (e.g. a struct-copy
  or aliasing path).
- Something unrelated to the kfcache protocol entirely — considered less
  likely, since a heap/stack corruption bug would typically produce an
  ASan-attributed report (ASan tracks heap and stack allocations precisely),
  whereas this is a bare `DEADLYSIGNAL` on mmap'd memory, which is the
  signature of a genuine post-munmap dereference rather than corruption.

## Why this isn't a quick fix

The root cause isn't conclusively identified, so there is nothing to
"quickly fix" yet without risking a guess that papers over the real
mechanism. Live/post-mortem debugging is unavailable in the current
investigation environment: `gdb -p <pid>` fails with `ptrace: Operation not
permitted.` (confirmed on 20/20 attach attempts via a racer script), and no
core dump is ever produced despite `ulimit -c unlimited` and
`ASAN_OPTIONS=abort_on_error=1` (environment/sandbox restriction, not a
local misconfiguration). Static source analysis has been pushed about as far
as it usefully can go without empirical confirmation of the actual
interleaving.

## Suggested next steps (for whoever picks this up)

1. **Highest-leverage, no-ptrace-required diagnostic**: manually poison the
   `e->base` region with `ASAN_POISON_MEMORY_REGION`/`ASAN_UNPOISON_MEMORY_REGION`
   (`<sanitizer/asan_interface.h>`) at every munmap/remap site in
   `slotcask.c` (`kfcache_drop_slot`, `kfcache_invalidate_prefix`,
   `kfcache_resplit_locked` Phase E, `kfcache_shutdown`) — poison
   immediately after `munmap`, unpoison immediately after a fresh `mmap` is
   installed into the same slot. ASan's shadow memory *does* track manually
   poisoned regions regardless of the underlying allocation type (mmap vs.
   heap), so this converts the current bare `SIGSEGV` into a proper
   ASan "use-after-poison" report with a full stack trace at both the
   poisoning (free) site and the faulting (use) site — which would settle
   the open question directly. Caveat: needs careful pairing (poison/unpoison
   must bracket exactly the invalid window) to avoid flagging legitimate
   accesses to a freshly-remapped region if the kernel happens to reuse the
   same VA range.
2. Alternatively, reproduce in an environment where `ptrace`/core dumps
   work, and capture a live two-thread backtrace at the moment of the fault.
3. Once root cause is confirmed, apply the fix and add a regression test
   that deterministically exercises concurrent warmup pool-rebuild against a
   resplit/invalidate on the same shard under ASan (the existing
   `KFCACHE_TEST_HOLD_MS` hook pattern used by the shutdown-race regression
   test may be reusable to widen this race's window deterministically).
4. Re-run the full ASan suite (`detect_leaks=1`) and confirm
   `test-bitmap-index` no longer crashes, standalone and in the full run.

## Not done in this pass because

Per standing project instruction, issues are either fixed immediately (if
simple) or written up as a plan (if not). This is a crash whose root cause
survived exhaustive static tracing across two sessions without being pinned
down, in an environment that cannot support the live debugging needed to go
further — it is not safe to guess at a fix. It belongs in its own follow-up
session with proper debugging tooling, not a rushed patch bundled into
`durability-sync` under time pressure.

## Impact on this branch

This blocks claiming the broadened full-suite ASan CI workflow
(`.github/workflows/sanitizers.yml`) is genuinely clean — `test-bitmap-index`
crashes reliably under ASan today. Before pushing, the user needs to decide
between: (a) hold the branch until this is root-caused and fixed, (b)
temporarily and explicitly quarantine `test-bitmap-index` from the
broadened ASan run with a tracked skip referencing this doc (not a silent
grep-backstop bypass), or (c) push the CI-hardening + `g_kfcache_count` fix
now and track this SEGV as an immediate, explicit follow-up issue. This is a
product/risk decision, not a technical one — flagged for the user rather
than decided here.
