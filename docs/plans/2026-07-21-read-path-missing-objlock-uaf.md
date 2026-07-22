# Read path (get/exists/count/describe-object/...): missing objlock, genuine UAF

**STATUS: root-caused and fixed.** See "Fix shipped" below.

Found during TSan re-verification of the durability-sync branch
(`feat/durability-sync-embedded-bg-threads`). Not a regression from this
branch's diff — `server.c`'s dispatch locking policy and `objlock.c` are
untouched by this branch — pre-existing. Fixed on this branch anyway per
the "never paper over, fix it when we see it" standard, given the
severity: this is a genuine use-after-free on the ordinary GET path, not
a documentation nit or a rare deadlock.

## Signature

TSan, full-suite run, `test-auto-reshard-throttle`:

```
WARNING: ThreadSanitizer: heap-use-after-free (pid=...)
  Write of size 8 at 0x... by thread T.. (auto_reshard_thread):
    #0 memset
    #1 slotcask_close src/db/slotcask.c:3125
    #2 slotcask_registry_invalidate src/db/slotcask.c:5474
    ...
  Previous read of size 8 at 0x... by thread T.. :
    #0 resolve_counts_with_schema src/db/storage.c:309
    #1 resolve_counts src/db/storage.c:337
    #2 get_live_count_ll_for_schema src/db/storage.c:372  (or get_live_count / cmd_get / cmd_get_fields)
```

Six occurrences across the run (3x `slotcask.c:3125` in `slotcask_close`,
3x `slotcask.c:5475` the `free(db)` in `slotcask_registry_invalidate`),
all racing a reader that went through `slotcask_registry_get()` with no
lock at all.

## Root cause

`slotcask_registry_get()` (`slotcask.c:5374`) hands back a raw
`SlotcaskDb *` from the registry table with **no reference counting**.
`slotcask_registry_invalidate()` (`slotcask.c:5454`), called only from
rebuild/vacuum while the caller holds `objlock_wrlock`, removes the
table entry under `g_reg_lock`, releases that lock, then calls
`slotcask_close(db); free(db);` — with no check for, and no way to
detect, an outstanding pointer some other thread already got back from
an earlier `slotcask_registry_get()` call and hasn't finished using yet.

The only mechanism in the codebase that could serialize "someone reading
through an open `SlotcaskDb*`" against "rebuild about to invalidate and
free that struct" is the per-object `objlock` rwlock
(`objlock_rdlock`/`objlock_wrlock`, `objlock.c`). Every mutating command
correctly takes `objlock_rdlock` before touching the registry (so
rebuild's `objlock_wrlock` blocks until in-flight writes finish) — that
part of the design works. But `dispatch_json_query`'s per-mode
classification (`server.c:1396-1397`) only takes a lock for
`mode_is_schema` (wrlock) or `mode_is_write` (rdlock):

```c
int took_wrlock = mode_is_schema(mode);
int took_rdlock = !took_wrlock && mode_is_write(mode);
if (took_wrlock) objlock_wrlock(db_root, object);
else if (took_rdlock) objlock_rdlock(db_root, object);
```

Every mode that isn't a write or a schema change — `get`, `get`-with-`keys`
(`cmd_get_multi`), `exists`, `not-exists`, `size`, `count`,
`estimate-index`, `find`, `keys`, `fetch`, `shard-stats`, `backup`,
`get-file`, `get-file-path`, `list-files`, `aggregate` — takes **no
lock at all** before reaching a `slotcask_registry_get()` call (directly,
or via `resolve_counts_with_schema` for the count-based ones). Same gap
in the legacy `\x1F`-delimited fast path (`server_process_fast`,
`server.c:2281-2282`, same `mode_is_write`-only condition). Same gap
again for `describe-object`, which does not even reach the generic
block — it returns early (`server.c:1389-1393`) with a comment claiming
no lock is needed because it "just reads... static metadata + an atomic
counter." That's false: `cmd_describe_object` calls `get_live_count`,
which calls `resolve_counts` → `resolve_counts_with_schema`, which calls
`slotcask_registry_get()` then `slotcask_sum_kf_totals()` — a heap
dereference of the (possibly-just-freed) `SlotcaskDb*` plus a `pread()`
per shard, not an atomic read of anything.

The root of the false assumption is `objlock.c`'s own design comment
(lines 5-12):

> Reads (get/find/search/range): do NOT take this lock (MAP_SHARED
> gives a live view; the read-side retry loop validates hash+key after
> the fact and retries on a concurrent move, so no lock is needed for
> correctness)

This conflates two different hazards. The retry-validate loop
(`slotcask.c`, the hash+key recheck after a slot read) correctly handles
"the record moved to a different slot inside a *still-open*
`SlotcaskDb`" — that part of the reasoning is fine and unaffected by
this fix. It says nothing about, and does nothing for, "the
`SlotcaskDb` struct itself was `free()`'d because rebuild invalidated
the registry entry" — a dangling-pointer dereference no retry loop can
detect, because by the time you'd retry, `db->data_dir` is already a
read of freed memory. A prior pass on this codebase (unrelated session,
2026-07-16) flagged this same comment as inaccurate but concluded it was
a documentation-only nit (wrong claim about `MAP_PRIVATE` vs.
`MAP_SHARED`) and rated it "optional priority" — that pass validated the
"no lock needed for reads" conclusion instead of questioning it. The
TSan evidence from this session shows that conclusion itself is wrong,
not just its stated rationale.

## Why `cmd_insert_v2`/`cmd_update_v2`/`cmd_delete_v2`'s own
`slotcask_registry_get()` calls (`storage.c:802`, `:1111`, `:1429`) are
NOT part of this bug

Those three are reached only through `insert`/`update`/`delete` (and
their bulk variants), all `mode_is_write` — already correctly protected
by the existing `objlock_rdlock` taken before dispatch. Confirmed by
reading `dispatch_json_query`'s full mode table: only modes *outside*
`mode_is_write`/`mode_is_schema` reach the generic block with zero lock.

## Fix shipped

- `src/db/server.c`, `mode_is_schema()`: added `"compact"`. `compact`
  was previously unclassified (neither write nor schema), so it hit the
  same zero-lock gap for its own `slotcask_registry_get()` call
  (`server.c` compact branch) before the code's own explicit
  `objlock_wrlock()`/`objlock_wrunlock()` around the actual
  `slotcask_compact()` call — i.e. it had already independently
  hand-rolled a partial version of this exact fix for the mutating part
  but left the initial format-check read unprotected. Reclassifying it
  as schema (like `vacuum`/`truncate`, which it resembles) closes that
  and lets it drop the now-redundant (and, after the dispatch-level
  fix, self-deadlocking — wrlock is non-recursive) internal
  `objlock_wrlock`/`objlock_wrunlock` pair; the outer wrlock now covers
  the whole branch.
- `src/db/server.c`, `dispatch_json_query`: `took_rdlock` broadened from
  `!took_wrlock && mode_is_write(mode)` to `!took_wrlock` — every mode
  that reaches this block (i.e. every object-scoped command that isn't
  schema-exclusive) now takes `objlock_rdlock` for its duration. Default
  is now fail-safe (lock unless explicitly exclusive) instead of
  fail-open (no lock unless explicitly a write) — a mode added to this
  block in the future is protected by default instead of silently
  reintroducing this class of bug.
- `src/db/server.c`, `describe-object`'s early-return special case: now
  wrapped in `objlock_rdlock(db_root, object)` /
  `objlock_rdunlock(db_root, object)` around the `cmd_describe_object`
  call, since it bypasses the generic block entirely. Confirmed
  `db_root` (from `build_effective_root(db_root, dir)` at the top of the
  function) is the same `(effective_root, object)` key
  `cmd_describe_object` → `get_live_count` → `resolve_counts_with_schema`
  resolves internally (the latter re-splits a possible `"dir/object"`
  joined form back to the same pair) — the lock key matches what a
  concurrent rebuild's `objlock_wrlock` uses, so it actually provides
  mutual exclusion rather than locking on a mismatched key.
- `src/db/server.c`, `server_process_fast` (legacy `\x1F` protocol):
  same broadening, `fast_rd` from `!fast_wr && mode_is_write(cmd)` to
  `!fast_wr`. Checked every command in this path's dispatch table for
  the same self-deadlock risk `compact` had (an internal explicit
  `objlock_wrlock` after being reclassified) — none of the legacy
  commands (`get`, `exists`, `size`, `orphaned`, `keys`, `fetch`,
  `find`, `backup`, `restore`, `vacuum`, `rebuild-kf`, `recount`,
  `truncate`, ...) take their own internal objlock; they all already
  follow the "caller holds the lock" convention documented throughout
  `index.c`/`config.c`/`query_bulk.c`/`slotcask.c`. `compact` isn't
  reachable via the legacy protocol at all, so no legacy-path
  equivalent fix was needed there.
- `src/db/embedded.c`: no separate fix needed — `shard_db_query()` is a
  thin wrapper that calls `dispatch_json_query()` directly, so embedded
  mode inherits the fix automatically.
- `src/db/objlock.c`: corrected the file-header design comment. It no
  longer claims reads need no lock; it now documents the actual
  invariant — rdlock is required for both writes and reads, wrlock for
  schema/rebuild — and explains the retry-validate loop's real, narrower
  scope (in-object slot moves, not registry-entry lifetime).

### Early-return lock-leak hazard found while implementing the broadening

Broadening `took_rdlock` to `!took_wrlock` is not a safe one-line change
on its own: `dispatch_json_query`'s mode dispatch chain has 4 early
`return;` statements inside the now-locked region that bypass the
function's single common exit point (the `objlock_w/rdunlock` pair right
before the final `free(mode); free(dir); free(object);`) —
1 in `find` (the `offset < 0` validation error) and 3 in `compact`
(not-found/wrong-format, `slotcask_compact()` failure, and the success
path). Before this fix, none of these leaked anything: `find` took no
lock at all, and `compact`'s 3 returns all sit after `compact`'s own
internal `objlock_wrlock`/`objlock_wrunlock` pair had already balanced
out. After reclassifying `compact` into `mode_is_schema` and removing
its internal lock pair (so the *outer* wrlock now covers the whole
branch), those same 3 returns would each leak the outer wrlock —
permanently, since `pthread_rwlock_t` has no timeout and no owner
recovery. That's strictly worse than the UAF being fixed: every future
request against that object (from any client, forever, until process
restart) would hang. Fixed by adding the matching
`objlock_w/rdunlock` call immediately before each of the 4 early
returns, using the already-in-scope `took_wrlock`/`took_rdlock` flags.

A second, related bug was introduced and caught by GCC's
`-Wuse-after-free` during the very next build (not by inspection): the
first pass at this fix put the unlock call *after* `free(mode); free(dir);
free(object);` at all 4 sites, using `object` (now dangling) as the
unlock's lock-table key. Fixed by reordering to unlock-then-free at all
4 sites (`server.c`), matching the order the function's own common exit
path already used. Flagging this because it's exactly the kind of subtle
ordering mistake this whole class of bug is made of — worth calling out
that the fix itself needed a second pass, verified by a clean
`-Wuse-after-free`-free rebuild before moving on.

## Sibling finding: `dispatch_nql_query` had the identical gap

While writing up this doc, re-checked the NQL text-protocol dispatcher
(`dispatch_nql_query`, `server.c`, handles `find`/`count`/`aggregate` over
the `\n`-terminated NQL wire format) for the same class of bug — and found
it. It had previously taken an unconditional `objlock_rdlock` before its
`switch (cmd.mode)`, but that was deliberately removed in an earlier,
separate session (`docs/plans/2026-07-17-nql-drop-unconditional-objlock.md`,
"Finding 4") on the theory that JSON's `find`/`count`/`aggregate` already
took zero lock, so NQL should match for consistency. That premise was
correct about JSON's *behavior* at the time but wrong about it being
*safe* — it was the same UAF documented above, just not yet diagnosed as
one. Result: both wire protocols were unlocked on the read path,
independently reachable, same root cause.

Fixed by restoring the lock: `objlock_rdlock(db_root, cmd.obj)` before the
switch, `objlock_rdunlock(db_root, cmd.obj)` after it closes (before
`nql_free_command`). Verified the switch body has no early `return`s and
doesn't free/reassign `cmd.obj`/`db_root` mid-switch, so no lock-leak
hazard analogous to the `find`/`compact` one above applies here.

The regression test this "Finding 4" work added,
`test_nql_no_objlock_contention.c` (which asserted, as correct, that an
NQL `find` does *not* block behind a held schema wrlock — proving the
absence of the lock, not the absence of a bug), has been renamed twice —
through `test_nql_objlock_contention.c` to its final name,
`test_read_objlock_contention.c` — and every assertion inverted. It now
fires a JSON `get` and an NQL `find` concurrently (both requests sent via
`tc_send` before either response is awaited, so they're genuinely in
flight together) against the same object held under a test-delayed
schema wrlock, and proves BOTH wire-protocol read paths block until the
wrlock releases — covering the JSON-side fix earlier in this doc as well
as this NQL sibling in one test. `docs/plans/2026-07-17-nql-drop-unconditional-objlock.md`
is marked superseded at its top rather than deleted, since it accurately
records what was done and why at the time.

## What this does NOT touch (scope boundary)

`create-object`, `drop-object`, `restore` (JSON mode) all return early,
before the generic lock block, same as `describe-object` did. No TSan
evidence implicates any of them in this bug class, and auditing their
locking from scratch is a separate exercise, not a mechanical extension
of the fix already applied elsewhere (unlike `describe-object`, which
was directly named in the TSan reader-side stack trace). Left alone.

## Verification

- [x] Targeted plain-build run: `test-auto-reshard-throttle` (the test
      whose run produced the original 6 TSan findings, and whose own
      "found at least 2 AUTO-RESHARD done log lines" assertion was the
      one under active suspicion per the governing "if this is a bug we
      fix it now" directive) — 7/7 passed.
- [x] Full suite, plain build: 10605/10605 passed, 0 failed, 306/306
      cases, no port-picker flake this run.
- [x] Full suite, ASan: 10605/10605 passed, 0 failed, 0 sanitizer findings
      (AddressSanitizer/LeakSanitizer grep count = 0, `not ok` grep count
      = 0). Re-run after the `dispatch_nql_query` sibling fix and test
      rename landed, so this covers both fixes.
- [x] `test-nql-objlock-contention` (renamed from
      `test-nql-no-objlock-contention`) passes 15/15 standalone against the
      fixed `dispatch_nql_query` — confirms NQL `find` blocks ~2000ms
      behind the held schema wrlock instead of racing the free.
- [x] Full suite, TSan, on the code state that includes BOTH the
      `dispatch_json_query`/`server_process_fast` fix AND the
      `dispatch_nql_query` fix: 0 occurrences of `slotcask.c:3125`
      (the `resolve_counts_with_schema`/`get_live_count` UAF site), 0
      `heap-use-after-free` reports anywhere in the run. The fix holds.
      10601/10605 passed (4 failed, all pre-existing and unrelated — see
      below); 3 `WARNING: ThreadSanitizer` blocks total, all pre-existing
      and already tracked elsewhere:
      - 2x `bitmap.c:454` lock-order-inversion in `bm_open` (`shard-db` and
        `embedded_bg_harness` binaries) — this is the same issue documented
        in `docs/plans/2026-07-20-bitmap-kfcache-lock-order-inversion.md`
        ("not fixed, plan only" — real deadlock risk, ~40 call sites across
        7 files, deliberately deferred to its own reviewed change, not this
        branch). It's what fails `test-embedded-bg-threads`'s "harness
        exits cleanly" assertion (TSan aborts the process on the warning,
        exit code 66).
      - 1x `slotcask.c:294` data race in `kfcache_invalidate_prefix` vs.
        `slotcask_open_kf_worker` — the pre-existing, separately-tracked
        4th race mentioned above (not yet root-caused; distinct from the
        UAF this doc fixes — it's a plain data race on an atomic int, not
        a free-vs-access).
      - `test-binary-index`'s 3 failing assertions correlate with none of
        the 3 warnings above and don't reproduce standalone (22/22 passed,
        3/3 isolated runs) — root-caused as a full-suite-parallel-load-only
        flake, pre-existing (same failure count seen before this session's
        `dispatch_nql_query` fix), unrelated to any code this session
        touched. See
        `docs/plans/2026-07-21-test-binary-index-tsan-fullsuite-flake.md`.
- [ ] New regression test proving a concurrent `get` blocks briefly
      behind an in-flight `vacuum`/rebuild instead of racing it, using
      the same test-only delay-knob pattern as
      `WARMUP_TEST_PRELOCK_DELAY_MS` (see
      `docs/plans/2026-07-20-warmup-kftask-stale-sdb-uaf.md`).

## Severity assessment

High. True use-after-free (not a data race on inert data — a freed heap
struct dereferenced for its `data_dir` string, `slots_per_shard`, etc.,
and disk I/O issued against paths built from it), reachable from the
ordinary client `get` path (not just an admin/maintenance path), and
present since at least whenever `auto_reshard_thread`/the objlock module
were introduced (`objlock.c`'s design comment predates this session
entirely). Only did not manifest as visible corruption/crashes before
now because it requires a concurrent rebuild/vacuum racing a read on the
*same* object within a narrow window — routine in production once
auto-reshard or manual vacuum runs against live traffic, but rare enough
in ad hoc/light testing to go unnoticed until TSan's instrumentation
(and a test that deliberately drives concurrent reshards,
`test-auto-reshard-throttle`) caught it.
