# warmup_kf_task_fn: stale SlotcaskDb pointer across phase 1 -> phase 2 gap

Found during ASan/TSan re-verification of the durability-sync branch
(`feat/durability-sync-embedded-bg-threads`). Not a regression from that
branch's diff — pre-existing, but confirmed live and structurally
identical in class to the `warmup_thread` UAF fixed on this branch
(`server.c:2838`, phase-1 `num_shards` read, now wrapped correctly).
This one is deeper and needs its own fix + regression test rather than a
rushed patch, given the operation touches live query data.

Branch: continue on `feat/durability-sync-embedded-bg-threads` (or split
off once that branch merges — reviewer's call). Build:
`SKIP_TESTS=1 ./build.sh`. Test:
`./build/bin/shard-db-test run <name>`.

## Root cause

`warmup_thread` (`src/db/server.c`) runs in three phases:

- **Phase 1** (serial, this thread): walks every `(dir, object)` under
  `db_root`, calls `warmup_object_open()` -> `slotcask_registry_get()` to
  get a `SlotcaskDb *sdb`, and for every shard appends a `WarmupKfTask`
  (`server.c:2655-2661`) that stores the raw `sdb` pointer by value
  (`kt->sdb = sdb`, in the per-object shard-collection loop).
- **Phase 2**: after phase 1 finishes walking the *entire* tree (every
  dir, every object), all accumulated `kf_tasks` are dispatched to the IO
  pool via `parallel_for_io`, running `warmup_kf_task_fn`.
- `warmup_kf_task_fn` (`server.c:2663-2699`) takes
  `objlock_rdlock(t->eff, t->obj)`, then dereferences `t->sdb->data_dir`
  and `t->sdb->slots_per_shard` (lines 2681-2682), then releases the lock.

The rdlock only proves no *concurrent* `objlock_wrlock` (vacuum) is
running at the moment of the dereference — it says nothing about whether
`t->sdb` was already freed by a vacuum that fully ran (wrlock acquired,
`rebuild_txn_abort` -> `slotcask_registry_invalidate` ->
`slotcask_close`+`free`, wrlock released) *before* `warmup_kf_task_fn`
even started. That's a live window: phase 1 walks the whole `db_root`
before phase 2 dispatches anything, so an early object's `sdb` can sit
captured-but-unused for the entire rest of the tree walk — easily long
enough for a concurrent `auto_vacuum_thread` sweep to invalidate that
exact object in between. When that happens, `warmup_kf_task_fn`'s rdlock
succeeds trivially (no one holds the wrlock any more) and
`t->sdb->data_dir` dereferences already-freed memory.

This is the same *class* of bug as the just-fixed `server.c:2838` read
(a pointer captured outside/before the lock that's later trusted without
re-validation), but the fix that shipped for that one (commit `f81d0f9`,
"guard warmup thread against concurrent vacuum/rebuild UAF") did not
close this occurrence — `t->sdb` is still captured in phase 1, long
before the lock in `warmup_kf_task_fn` is taken.

## Why this isn't a same-session drive-by fix

Closing it properly means `warmup_kf_task_fn` must **not** trust
`t->sdb` at all — it must re-resolve a live `SlotcaskDb*` from the
registry itself, inside the rdlock, the same way the phase-1 fix now
does. That requires:

1. `slotcask_registry_get(effective_root, object, info)` needs a valid
   `SlotcaskSchemaInfo` (`splits`, `slot_size`, `streams`) even on the
   cache-hit path (it's checked before the `used` fast-path branch) —
   `WarmupKfTask` doesn't currently carry this, only `eff`/`obj` strings
   and the (now-suspect) `sdb` pointer. Needs a `SlotcaskSchemaInfo info`
   field added to the struct, populated in phase 1 alongside `eff`/`obj`.
2. `warmup_kf_task_fn` needs to call `slotcask_registry_get(t->eff,
   t->obj, &t->info)` *inside* the rdlock to get a live `sdb`, use it for
   `kf_path`/`slots_per_shard`, then release — never touching `t->sdb`.
3. A regression test proving the fix, mirroring the existing
   `WARMUP_TEST_DELAY_MS` pattern (`server.c`, test-only) used by
   `f81d0f9`'s own regression test: widen the phase-1-capture-to-phase-2-
   dereference gap deterministically, run a vacuum in that gap, assert no
   UAF (ASan-clean) instead of just "doesn't crash" (which is
   nondeterministic without the delay hook).

That's a struct change, two call-site changes, and a new deterministic
regression test — enough surface area, on a path that touches live
object data, to warrant its own review pass rather than folding into an
unrelated durability-sync branch under time pressure.

## Suggested fix sketch

```c
typedef struct {
    char eff[PATH_MAX];
    char obj[256];
    SlotcaskSchemaInfo info;   /* NEW: needed for a fresh registry_get */
    int shard_idx;
    _Atomic int *kf_count;
} WarmupKfTask;                /* sdb field removed */

static void *warmup_kf_task_fn(void *arg) {
    WarmupKfTask *t = (WarmupKfTask *)arg;
    if (!server_running) return NULL;

    char kf_path[PATH_MAX];
    int slots_per_shard;
    objlock_rdlock(t->eff, t->obj);
    SlotcaskDb *sdb = slotcask_registry_get(t->eff, t->obj, &t->info);
    if (!sdb) { objlock_rdunlock(t->eff, t->obj); return NULL; }
    slotcask_kf_path(kf_path, sizeof(kf_path), sdb->data_dir, t->shard_idx);
    slots_per_shard = sdb->slots_per_shard;
    objlock_rdunlock(t->eff, t->obj);
    ...
}
```

Phase 1's collection loop needs the matching `SlotcaskSchemaInfo` handy
already (it's what it passed to `warmup_object_open` in the first place)
— check whether `warmup_object_open` can hand it back out, or whether
phase 1 needs to keep its own copy alongside `sdb`.

## Definition of done

- [x] `WarmupKfTask` re-resolves `sdb` fresh under `objlock_rdlock` in
      `warmup_kf_task_fn`; no field of the struct is dereferenced across
      the phase-1/phase-2 gap without going back through the registry.
      Shipped as: `WarmupKfTask` carries only `eff`/`obj`/`shard_idx`/
      `kf_count` (no `sdb` pointer, and — after a follow-up fix, see
      below — no `SlotcaskSchemaInfo` either); `warmup_kf_task_fn` calls
      `load_schema()` + `slotcask_registry_get()` fresh, inside its own
      `objlock_rdlock`.
- [x] **Follow-up correctness fix (found during self-review of the first
      pass, before any commit):** the first version of this fix carried
      a `SlotcaskSchemaInfo info` field captured in phase 1 and reused it
      in `slotcask_registry_get()` on the phase-2 side. That's still
      unsafe: `rebuild_object_v2` (`query_find.c`) calls
      `slotcask_registry_invalidate()` (twice, plus once more on the
      abort path) but never repopulates the registry itself — the
      *next* caller's `slotcask_registry_get()` is the one that decides
      the reopened shape via whatever `SlotcaskSchemaInfo` it passes on
      a cache miss (`slotcask_open()` trusts `splits`/`slot_size`/
      `streams` as authoritative, no on-disk cross-check). Phase-1-aged
      info reused on a miss could therefore reopen the object with a
      stale (pre-rebuild) shape and silently poison the registry entry
      for every subsequent caller — a correctness/data-corruption bug,
      not just a UAF. Fixed by dropping the `info` field entirely and
      having `warmup_kf_task_fn` call `load_schema(t->eff, t->obj)`
      itself, inside its `objlock_rdlock` — matching the pattern every
      other `slotcask_registry_get()` call site in the codebase already
      uses (load `Schema` fresh, immediately before the call). Since
      `rebuild_object_v2` holds `objlock_wrlock` across both the
      `schema.conf` rewrite and the registry invalidate, the rdlock'd
      reload is guaranteed to see either the fully-pre-rebuild or
      fully-post-rebuild schema, never a torn/stale one.
- [x] New regression test using a `WARMUP_TEST_DELAY_MS`-style hook to
      deterministically reproduce "vacuum completes between phase-1
      capture and phase-2 dereference"; fails (UAF/ASan) on old code,
      passes on new. `test_warmup_vacuum_race.c` exists but only proves
      mutual exclusion *during* concurrent access (delay hook fires
      after the rdlock is taken) — it doesn't cover the no-overlap case
      where the vacuum completes entirely before phase 2 ever attempts
      the lock. Shipped as `src/test/cases/test_warmup_vacuum_norace.c`:
      adds a `WARMUP_TEST_PRELOCK_DELAY_MS` knob (`server.c`,
      `warmup_kf_task_fn`) that sleeps *before* taking `objlock_rdlock`
      at all (no lock held during the sleep), fires a `vacuum --splits`
      immediately, asserts it completes fast/unblocked (proving genuine
      no-overlap), then lets the delayed tasks run past their sleep and
      asserts the daemon stays alive/responsive and the object is
      readable/writable at its new post-vacuum shape (proving
      `warmup_kf_task_fn` re-resolved fresh rather than poisoning the
      registry with stale info). 15/15 passed under ASan, full-suite run
      (`full_asan_after_fix.log`).
- [x] Full suite green under a plain (no sanitizer) build: 10590/10590
      passed, 0 `not ok`, `/tmp/shard-db-plain-tests.log`.
- [x] Full suite green under ASan (grep exact sanitizer markers, not just
      pass-count): 0 `AddressSanitizer`/`LeakSanitizer` occurrences across
      the entire suite (`full_asan_after_fix.log`, run after the
      typed-schema-cache `schema_caches_shutdown()` fix landed — see
      `docs/plans/2026-07-20-typed-schema-cache-enum-values-leak.md`).
      3 `not ok` in this run were a pre-existing, unrelated test-harness
      port-picker TOCTOU race (see
      `docs/plans/2026-07-21-test-harness-port-toctou-flake.md`) —
      confirmed via 4 prior full-suite ASan runs from earlier this
      session showing the identical shape (a different random victim
      test each time, 0-2 failures per run, zero sanitizer findings).
      TSan run in progress, not yet confirmed.
- [x] `docs/concepts/concurrency.md` updated alongside the other
      warmup/objlock entries: new "Warmup thread vs. concurrent
      vacuum/rebuild" section documenting the phase-1/phase-2 gap, the
      re-resolve-under-rdlock invariant, and both regression tests.
