# Plan: split index pre_commit into prepare/apply phases (fix daemon-abort on bitmap-cap insert)

## Root cause

Branch `fix/durability-write-ordering`, in fixing a real bug (crash between
`kf_put_new` and its recovery marker leaving an unrecoverable inconsistency —
tracked as this branch's Task #6), reordered the **new-key insert** path so
`kf_put_new` + `kfcache_sync_slots_locked` (durably fsynced) now run
**before** `opts->pre_commit` (which applies bitmap/btree/trigram index
updates and can legitimately reject, e.g. a bitmap index over its configured
`bitmap(N)` distinct-value cap).

Once kf is durable, the branch's commit-intent rule says a later failure can
never be rolled back — only replayed (idempotent retry) or, if replay also
fails, `kf_marker_fail_closed()` → `abort()` (whole-daemon crash, by design,
for genuine unrecoverable I/O/corruption). `pre_commit` returning non-zero
for an ordinary, deterministic business-logic rejection (cap already full)
gets funneled into that same path: replay re-attempts the identical
index-apply, fails identically, and the daemon aborts.

This contradicts `pre_commit_fn`'s original, still-documented contract
(`src/db/slotcask.h:397-401`): *"returning non-zero aborts: the freshly-written
slot is tombstoned and pushed to the free pool, no kf change happens."* The
**update** path in the same functions still honors that contract (pre_commit
runs before `kf_repoint_at_slot`); only the **insert** branch was reordered,
creating an asymmetry. `src/test/cases/test_bitmap_index.c:965-1009` (`capovf`)
already asserts the contract — it fails/crashes on current `main`-diverged
branch state, confirming this is a genuine regression, not a stale test.

Confirmed via `strace`: `abort()` fires from `kf_marker_fail_closed()`
(`slotcask.c:844-850`), logged as *"commit-intent marker for kf shard 001...
could not be replayed post-fsync (index apply after insert)"*.

## Exact bug sites (all confirmed via diff against `main` + direct read)

| Site | Function | Branch | kf durable when pre_commit runs? |
|---|---|---|---|
| `slotcask.c:4431-4443` | `slotcask_upsert_with_hooks`, slow path | new-key insert | **yes** (bug) |
| `slotcask.c:4676` (post-`kf_put_new`+sync at 4597/4666) | `slotcask_insert_with_hooks`, indexed | insert | **yes** (bug) |
| `slotcask.c:5373` (windowed bulk commit, `BULK_COMMIT_MAX_RECORDS=256`) | `bulk_upsert_slow_in_kfshard` | new keys in window | **yes** (bug — `out_durability_degraded` set, marker retained, for the whole window) |

All update/delete `pre_commit` call sites (`slotcask.c:4354`, `4100`, `4282`,
`5631`, `5801`, `6283`) already run before any kf mutation and are
unaffected — this plan does not touch them.

## Design: prepare / apply split

Two-phase hook contract, replacing the single `pre_commit` call at the three
bug sites above (non-indexed and update/delete paths keep calling the
existing `pre_commit` unchanged):

- **`prepare_commit`** — runs **before** the commit-intent marker is written.
  Performs every check that can legitimately reject the write (today: only
  the bitmap distinct-value cap). Must not durably mutate index state.
  Non-zero return = ordinary rejection: marker is never written, the
  speculative segment slot is tombstoned, caller gets a normal
  `{"error":...}` — exactly the pre-existing, still-correct contract.
- **`apply_commit`** — runs **after** the marker is durable (fsynced), before
  kf is committed. Performs the actual index mutation (bitmap set/clear +
  sync, btree/trigram writes). A non-zero return here is, by construction,
  never a policy rejection (that already happened in `prepare_commit`) — it's
  a genuine I/O/OOM failure, so the existing fail-closed/replay machinery is
  the *correct* handler and is unchanged.

This mirrors how the **update** branch already behaves (index-apply before
kf mutation, both gated by the marker fsync) — it removes the insert/update
asymmetry rather than inventing new semantics. Recovery
(`marker_recovery_sweep_object`, `slotcask.c:479+`) already reconciles kf
state and index state independently from the marker content regardless of
which one the live path finished first, so moving index-apply to run before
kf-publish (both still after the marker fsync) stays crash-safe — no changes
needed to the recovery sweep.

### Closing the cap-check-then-apply race without transferring locks

The bitmap writer lock is the reservation, but a `BitmapShard *` may **not**
be passed through two independent `parallel_for` calls: `bm_open(...,
writer=1)` acquires a pthread rwlock and `bm_close()` unlocks it, so the same
thread must close it. Unlocking from a later, unrelated worker is undefined
behavior.

Introduce a call-scoped `BitmapPrepareSet` owned by the caller of the hooks.
For a single record it contains one `BitmapShard *` plus its extracted key per
changed bitmap field. `bitmap_prepare_set_begin()` and
`bitmap_prepare_set_apply_or_abort()` always run on the slotcask caller's
thread, never through `parallel_for`:

1. `begin` opens each bitmap file writer-locked, checks its cap without
   mutation, and retains the handle on success;
2. on a normal rejection or marker-write failure, `abort` closes every
   retained handle and frees every key;
3. after marker fsync, `apply_or_abort` uses the same handles to perform the
   existing clear/set/sync work, then closes every handle before it returns.

The kf-shard writer lock stays held throughout this interval. Bitmap files are
also keyed by that kf shard, so another write capable of consuming this
file's final dictionary value cannot intervene. Btree/trigram work may remain
parallel inside `apply_commit`, but it must not own or close a bitmap handle.

Bulk uses a **window** prepare set rather than one handle per record: it opens
each affected `(field, kf_shard)` bitmap exactly once on the bulk worker
thread, validates a per-field pending-distinct-value set against
`n_values + pending_distinct <= max_values`, and holds those handles through
the window's apply phase. This avoids recursive acquisition of the same
non-recursive writer rwlock and preserves the existing lock-amortisation.

### Knowing the physical (shard, slot) before kf is durable (point 2)

Bitmap addresses records by physical `(kf_shard, kf_slot)`, not by key.
Today the insert paths only learn `kf_slot` as a side effect of
`kf_put_new`'s single mutating probe. Split `kf_put_new` into:

- `kf_plan_insert_slot(kh, hash, key, klen, data_dir, size_t *out_slot)` —
  **non-mutating**: reuses the existing probe walk (today inlined in
  `kf_put_new`, `slotcask.c` ~1780-1871: linear probe from
  `kf_slot_for(hash, cap)`, tracking first empty/tombstone) to find the slot
  this key would land at. Returns 0 (found target slot), 1 (duplicate key
  already live), or -1 (shard full/error). Confirmed safe under the existing
  kf-shard wrlock: `kfcache_acquire(..., 1)` is held across the whole
  operation at every call site (Explore-confirmed, no release/reacquire
  between planning and commit in the current code), so the planned slot
  cannot be stolen by a concurrent writer before we commit it.
- `kf_commit_planned_slot(kh, slot, hash, stream_id, file_id, offset, key, klen, data_dir, size_t *used_delta)` —
  **mutating**: writes the record at exactly the planned slot (handles the
  tombstone-resurrection case identically to today's `kf_put_new`), sets
  `flag=1`, bumps `hdr->total`. This is the actual commit point.
- `kf_put_new` is reimplemented as `kf_plan_insert_slot` immediately followed
  by `kf_commit_planned_slot`, preserving its existing signature/behavior for
  every caller not part of this fix (delete, update, non-indexed insert,
  legacy bulk paths) — no other call site changes.

## New ordering at the three bug sites

```
segment write (flag=1, speculative)
  → kf_plan_insert_slot        (non-mutating; know the future slot)
  → prepare_commit(planned_slot)   (bitmap cap-check per changed field,
                                     retained on the same caller thread)
       reject → tombstone segment, return normal error   [NEW: no crash]
  → kf_marker_write + fsync    (commit-intent boundary)
  → apply_commit(planned_slot)     (bitmap set/clear+sync using the
                                     same-thread retained handle; btree/
                                     trigram writes)
       fail  → kf_marker_replay_current → fail_closed on unrecoverable  [unchanged]
  → kf_commit_planned_slot + kfcache_sync_slots_locked
       fail  → kf_marker_replay_current → fail_closed on unrecoverable  [unchanged]
  → kf_marker_clear
```

## Task breakdown

### Task 1 — `slotcask.h`: new hook types

Add alongside the existing `slotcask_pre_commit_fn` (unchanged, still used by
every non-indexed and update/delete path):

```c
/* Two-phase hooks for indexed new-key inserts only (single-record and bulk
 * windowed paths). See docs/plans/2026-07-27-index-prepare-apply-split.md.
 *
 * prepare_commit — fires AFTER the segment write, BEFORE the commit-intent
 *   marker exists. Must perform every check that can legitimately reject
 *   the write (e.g. bitmap distinct-value cap) and MUST NOT durably mutate
 *   index state. planned_kf_slot is the physical slot this record will
 *   commit to (see kf_plan_insert_slot). Returning non-zero rejects: no
 *   marker is ever written, the speculative segment slot is tombstoned,
 *   caller gets an ordinary error — never routed through fail-closed.
 *
 * apply_commit — fires AFTER the marker is durable, BEFORE kf is committed.
 *   Performs the actual index mutation. Returning non-zero here is always a
 *   genuine failure (I/O/OOM), never a policy rejection — per the
 *   commit-intent rule this is never rolled back; replayed (idempotent) or
 *   fails closed exactly like every other post-marker-fsync failure today.
 *
 * Return 0 to proceed, non-zero to reject/fail. These hooks are required
 * for an indexed fresh insert that also supplies pre_commit; slotcask must
 * reject a partial/missing pair with EINVAL rather than silently falling
 * back to the unsafe single-phase ordering. */
typedef int (*slotcask_prepare_commit_fn)(const uint8_t *new_value, size_t new_vlen,
                                           uint32_t planned_kf_slot, void *ctx);
typedef int (*slotcask_apply_commit_fn)(const uint8_t *new_value, size_t new_vlen,
                                         uint32_t planned_kf_slot, void *ctx);
```

Add two optional fields to `SlotcaskUpsertOpts`:
```c
slotcask_prepare_commit_fn prepare_commit;   /* NULL = skip prepare phase */
slotcask_apply_commit_fn   apply_commit;     /* NULL = skip apply phase */
```

Add the analogous pair to `SlotcaskBulkOpts`, but make it window-scoped so a
bulk worker can validate and apply its coalesced index work while retaining
one same-thread bitmap lock per field:

```c
typedef int (*slotcask_bulk_prepare_window_fn)(SlotcaskBulkRec *recs,
                                                const size_t *active,
                                                size_t nactive, void *ctx);
typedef int (*slotcask_bulk_apply_window_fn)(SlotcaskBulkRec *recs,
                                              const size_t *active,
                                              size_t nactive, void *ctx);
```

Add these fields immediately after `value_compute` in `SlotcaskBulkOpts`:

```c
slotcask_bulk_prepare_window_fn prepare_window;
slotcask_bulk_apply_window_fn   apply_window;
void                            *bulk_hook_ctx;
```

`active[]` contains only records with a valid segment write and a planned
Kf target. The prepare callback may set `recs[active[i]].status = -1` for a
normal per-record rejection; slotcask rebuilds `active[]` before writing the
marker. The apply callback may not reject a policy condition. Both callbacks
receive a single caller-owned context (`bulk_hook_ctx` in `SlotcaskBulkOpts`),
not a worker-local lock handle transferred through `parallel_for`.

For a fresh indexed branch, enforce this guard before any marker write:

```c
if (opts->has_indexed_fields &&
    ((!!opts->prepare_commit != !!opts->apply_commit) ||
     (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
    errno = EINVAL;
    return -1;
}
```

The bulk equivalent checks its window-hook pair. This preserves the legacy
single-phase hook for non-indexed tests and update/delete paths, while making
a missed production migration fail loudly instead of retaining this bug.

### Task 2 — `slotcask.c`: split `kf_put_new`

Anchor: `kf_put_new`'s body (`slotcask.c:1780` onward, the linear-probe loop
that both finds and mutates the target slot). Extract the probe into
`kf_plan_insert_slot` (returns target slot / duplicate / full, no writes),
and the write into `kf_commit_planned_slot` (writes at a caller-supplied
slot found by a prior plan call — including the tombstone-resurrection
case). Reimplement `kf_put_new` as the composition of the two, so its
existing callers (everywhere except the two single-record bug sites, the
bulk window, and the new tests) are unaffected byte-for-byte.

For an indexed bulk window, a plain per-record planner is not enough: several
new keys can probe to the same empty/tombstone slot before any Kf mutation is
allowed. Add `KfInsertPlan plans[BULK_COMMIT_MAX_RECORDS]` plus a
window-local reservation overlay. `kf_plan_window_insert_slot` performs the
same linear probe while treating an earlier planned slot as occupied; it also
detects duplicate keys within the window with the same semantics as today's
serial `kf_put_new` loop. Set `rec->kf_shard` and `rec->kf_slot` from the
result before the window prepare callback. Only after every surviving record
has a plan may the window marker be created. Commit the planned entries in
input order with `kf_commit_planned_slot`; do not re-probe after index apply.

### Task 3 — `bitmap.c` / `index.c`: same-thread bitmap prepare sets

`bitmap.c`: add the non-mutating helper below beside `bm_set`. It must be
called while the caller holds the bitmap writer handle.

```c
/* Writer handle required. Returns 1 only when adding value would exceed the
 * file's cap; an existing dictionary value is always acceptable. */
int bm_dict_would_exceed_cap(const BitmapShard *bm,
                             const uint8_t *value, size_t vlen);
```

`index.c`: replace the single-shot bitmap portion of `bitmap_update()` with a
call-scoped prepare-set API. `BitmapPreparedField` owns the `BitmapShard *`,
the copied new/old index keys, and the eventual Kf slot; `BitmapPrepareSet`
owns an array of those fields. Its complete API is:

```c
int bitmap_prepare_set_add(BitmapPrepareSet *set, const UpdateIdxArg *arg);
int bitmap_prepare_set_apply(BitmapPrepareSet *set);
void bitmap_prepare_set_abort(BitmapPrepareSet *set);
```

`add` opens the field's bitmap writer handle on the current thread, calls
`bm_dict_would_exceed_cap`, and retains the handle only on success. `apply`
does the existing grow/clear/set/fdatasync sequence using those retained
handles, then closes and frees all fields even if one operation fails.
`abort` only closes/frees them. No `BitmapShard *` goes in `UpdateIdxArg`, and
no prepare/apply bitmap operation goes through `parallel_for`.

Add a bulk-only companion:

```c
int bitmap_prepare_window_add(BitmapPrepareWindow *window,
                              const UpdateIdxArg *arg);
int bitmap_prepare_window_validate(BitmapPrepareWindow *window,
                                   SlotcaskBulkRec *recs,
                                   size_t *active, size_t *nactive);
int bitmap_prepare_window_apply(BitmapPrepareWindow *window);
void bitmap_prepare_window_abort(BitmapPrepareWindow *window);
```

It opens each field once, tracks pending distinct values in memory while
walking records in their deterministic input order, and marks only the record
that would exceed the cap as rejected. It never changes the on-disk bitmap in
prepare. `apply` writes all accepted `(value, slot)` pairs, syncs each file,
then closes the same-thread handles. The pending-value set must compare the
full binary key (length plus bytes), not a hash alone.

### Task 4 — `storage.c`: split `v2_insert_pre_commit`

New `v2_insert_prepare_commit(new_value, new_vlen, planned_kf_slot, ctx)`:
for the fresh-insert branch (`is_update` false path,
`v2_insert_pre_commit`'s "else" branch, `storage.c:640+`), construct copied
keys for every changed bitmap field and add them serially to a
caller-thread-owned `BitmapPrepareSet` in `V2InsertCtx`. Set
`c->kf_shard`/`c->kf_slot` from the planned location before constructing the
arguments. A cap rejection or key-construction failure calls
`bitmap_prepare_set_abort`, records the current actionable message through
`capture_index_update_error`, and returns -1 with no marker.

`v2_insert_pre_commit` becomes `v2_insert_apply_commit`: it first performs
btree/trigram writes, then calls `bitmap_prepare_set_apply` for the prepared
bitmap fields. On *any* apply error it must call
`bitmap_prepare_set_abort` before returning so the kf writer lock cannot be
held while replay tries to reacquire a bitmap writer lock. The bitmap portion
does not reopen files and therefore cannot hit the normal cap-rejection path;
btree/trigram dispatch (`index_parallel`, `storage.c:641-644` onward) remains
parallel because it owns no retained bitmap lock.

The update branch (`storage.c:555-639`) keeps calling the single
`pre_commit`-equivalent unchanged — updates aren't affected by this bug and
don't need the split (see "all update/delete sites" table above).

### Task 5 — `slotcask.c`: rewire the three bug sites

**5a. `slotcask_upsert_with_hooks` insert branch** (`slotcask.c:4384-4446`).
Replace:
```c
if (kf_marker_write(db->data_dir, sid_kf, &marker) != 0) { ... }
durability_test_pause(db->data_dir, "marker-after-write");

size_t used_delta = 0;
size_t insert_slot = 0;
int kr = kf_put_new(db, &kh, hash, target_stream, target_fid, target_off,
                    key, klen, db->data_dir, &used_delta, &insert_slot);
...
{ size_t cs[] = { insert_slot };
  if (kfcache_sync_slots_locked(&kh, cs, 1, 1) != 0) { ... }
}

if (opts->pre_commit) {
    int rc = opts->pre_commit(NULL, value, vlen, 0, opts->pre_commit_ctx);
    if (rc != 0) { /* replay/fail-closed */ }
}
kf_marker_clear(db->data_dir, sid_kf);
```
with: plan the slot first (`kf_plan_insert_slot`, non-mutating — duplicate
check still bails cleanly with no marker written, matching today's
duplicate-handling contract); if `opts->prepare_commit` is set, call it with
the planned slot **before** `kf_marker_write` — reject → tombstone segment,
return -1, no marker ever touched; only after both plan+prepare succeed does
`kf_marker_write` run; then `apply_commit` (if set, else the legacy hook for
a non-indexed caller) runs post-marker-fsync **before**
`kf_commit_planned_slot`; then `kf_commit_planned_slot` +
`kfcache_sync_slots_locked` (failure here
still goes through replay/fail-closed, unchanged); then `kf_marker_clear`.

**5b. `slotcask_insert_with_hooks`** (`slotcask.c:4557-4680` region): same
restructure — `kf_plan_insert_slot` before `kf_marker_write`
(`slotcask.c:4574-4593`), `prepare_commit` before the marker write, `kf_commit_planned_slot`
replacing the current post-marker `kf_put_new` call (`slotcask.c:4597`),
`apply_commit`/`pre_commit` before that commit (not after, matching the new
ordering), marker clear last.

**5c. `bulk_upsert_slow_in_kfshard` windowed path** (`slotcask.c:5224-5382`,
the `BULK_COMMIT_MAX_RECORDS=256` window). This path needs a window
transaction, not a mechanically moved per-record callback:

1. Build `KfInsertPlan[]` for every new key with the reservation overlay from
   Task 2; existing keys use their captured Kf slot. Build a stable `active[]`
   array of records that still need writing.
2. Call `prepare_window` on the bulk worker thread. It stages *all* index
   diffs in a `BulkIndexWindow` (btree inserts/deletes, trigram work, and the
   bitmap window prepare set), and may reject only individual active records
   for a cap/key-validation condition. Abort and rebuild the staged window
   after each rejection so no pair from a rejected record remains. Rebuild
   `active[]` and marker slots from the survivors.
3. Only then write+fsync the batch marker for those survivors.
4. Call `apply_window` before every Kf publish. It must perform the actual
   btree merge, trigram mutation, bitmap set/clear+sync, and release all
   retained bitmap handles before returning. The existing
   `v2_bulk_ins_pre_commit_bulk` is therefore split into a pure staging
   routine plus this apply routine; its current post-return bitmap/B-tree
   flushes must be removed from `bulk_insert_shard_worker_v2`.
5. Commit the preplanned Kf slots in input order, sync the targeted Kf pages,
   and clear the batch marker. A genuine apply/Kf failure first releases any
   retained bitmap handles, then retains the marker and follows the existing
   replay/degraded path. It must never mark a policy rejection as degraded.

This is deliberately not a performance regression: bitmap files are still
opened once per `(field, kf-shard, window)` and btree entries remain merged in
batches. It closes the existing gap where the old bulk hook only queued index
pairs and `bulk_insert_shard_worker_v2` flushed them **after**
`slotcask_bulk_upsert_in_kfshard` had returned and its marker could already
have been cleared.

### Task 6 — call-site wiring (`storage.c`, `query_bulk.c`)

The required inventory has been completed with:

```bash
rtk proxy rg -n '\\.pre_commit\\s*=' src --glob '*.{c,h}'
rtk proxy rg -n 'slotcask_(upsert_with_hooks|insert_with_hooks|bulk_upsert_in_kfshard)\\(' src --glob '*.{c,h}'
```

| Assignment | Reaches | Classification | Required action |
|---|---|---|---|
| `storage.c:914`, `v2_insert_pre_commit` | `storage.c:933` insert-only and `:936` upsert | Fresh insert capable; `has_indexed_fields = nfields > 0` | Replace with `prepare_commit = v2_insert_prepare_commit` and `apply_commit = v2_insert_apply_commit`; remove `pre_commit` for this context. |
| `query_bulk.c:719`, `v2_bulk_ins_pre_commit_bulk` | `query_bulk.c:730` bulk upsert | Fresh insert capable; `has_indexed_fields = sw->nidx > 0` | Replace with the window `prepare_window`/`apply_window` pair and `bulk_hook_ctx = sw`; remove the post-return index flushes. |
| `storage.c:1367`, `v2_update_pre_commit` | upsert | `require_existing = 1`; update only | Leave unchanged. |
| `query_bulk.c:2939`, `3365`, `3829` | bulk upsert | each has `require_existing = 1`; update only | Leave unchanged. |
| `storage.c:1610`, `query_bulk.c:2379`, `4326` | delete primitives | delete hooks, not a target primitive | Leave unchanged. |
| `test_slotcask_cas.c:225` | test-only upsert | `has_indexed_fields = 0` | Leave as legacy-hook coverage. |

`test_durability_ordering.c` uses `has_indexed_fields = 1` but supplies no
`pre_commit`, so it exercises marker mechanics only and needs no migration.
The source-wide grep found no `embedded.c` or other production assignment
feeding the three target primitives.

After wiring, add the Task 1 fail-loud guard in each fresh indexed branch.
The two production rows above are the only rows it can reject; update/delete
and non-indexed legacy callers retain their documented behavior.

## Tests (test-first)

1. **Regression test — the bug, reproduced then fixed**: before any code
   change, run `./build/bin/shard-db-test run test-bitmap-index` and paste
   the crash (daemon aborts, non-zero exit / signal). This is the existing
   `capovf` test at `test_bitmap_index.c:965-1009` — no new test file needed
   for the base case, but add:
2. **New: daemon survives repeated cap rejections.** Extend `capovf` (or add
   a case) asserting: after the cap trips, (a) the daemon is still alive and
   answers a subsequent unrelated request, (b) the rejected key is
   `exists:false` both immediately and after a `stop`/`start` cycle (proves
   no marker was retained — nothing to replay), (c) a fresh insert with a
   value already in the bitmap dict still succeeds afterward (cap rejection
   didn't corrupt the dict/lock state).
3. **New: concurrent cap-boundary race.** N threads inserting distinct new
   values against a `bitmap(K)` field sitting at `K-1` distinct values
   concurrently — exactly one must succeed, the rest must get the ordinary
   cap error (never a crash, never more than K distinct values persisted).
   Exercises the same-thread retained-lock mechanism from Task 3; repeat a
   successful write afterward to prove all rejection paths released handles.
4. **New: bulk window partial rejection.** A `BULK_COMMIT_MAX_RECORDS`-sized
   (257-record, matching the existing boundary test's convention) bulk
   insert where one record in the middle of a window trips the bitmap cap —
   assert that record alone gets an error in the bulk response, every other
   record in the window (before and after it) still commits, and the daemon
   doesn't crash/degrade the whole window. Query by each affected bitmap,
   btree, and trigram field before restart and after restart, proving the
   former post-return bulk flush has moved inside the marker-protected apply.
5. **Crash-point tests**: extend the existing `durability_test_pause` crash
   hooks (already used for marker-after-write) with a pause point between
   `prepare_commit` succeeding and `kf_marker_write`, and between
   `apply_commit` and `kf_commit_planned_slot`, to verify recovery still
   converges correctly at each new boundary (mirrors this branch's existing
   crash-hook matrix — see `docs/plans/2026-07-18-crash-hook-matrix.md` for
   the established pattern).

Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run-all`.
Per AGENTS.md standing exceptions (this diff touches locks, kfcache,
bitmap cache, and object lifetimes), finish with these exact fresh runs — not
CI substitutes:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1
```

## Execution readiness

The call-site inventory is complete in Task 6, including the explicit check
that `embedded.c` has no relevant production caller. The fail-loud guards and
the same-thread/window ownership rules make an omitted future migration
observable rather than silently restoring the bad ordering. No design choice
remains open before execution; execution may begin only after the human
approves this revised plan.
