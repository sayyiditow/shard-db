# TSan release gate

## Goal

Make the current `main` clean under an unsuppressed, default-parallel TSan
run before release or feature work resumes. This is a lock-order repair, not a
test-harness or suppression-only change.

## Evidence and feedback loop

Built from clean `main` (`1d86d8b`) with:

```sh
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all > /tmp/shard-db-tsan-unsuppressed-full-20260815.log 2>&1
```

The completed run reports `12033 passed, 5 failed across 419 cases`. The five
case processes exit 66 after their functional assertions pass:

- `test-varlen-compact-recipient-resync`
- `test-varlen-compact-donor-resync`
- `test-varlen-compact-stat-resync`
- `test-compact-crash-recipient-sync`
- `test-compact-crash-kf-repoint`

All ten TSan summaries are real `kfcache`/`segcache` lock-order inversions,
not five independent bugs. The log proves both edges:

```
kfcache wrlock -> verify_stored_key() -> segcache rdlock
segcache recipient handle -> varlen_compact_cb() -> kfcache wrlock
```

The first edge occurs in `slotcask_delete`, `slotcask_get`, and live-walk
paths. The reverse edge is in varlen compaction, which keeps the recipient
segment handle from `compact_migrate_records_varlen()` while the scan callback
acquires a kf write handle. This can deadlock two threads; the existing
`.tsan.supp` claim that these locks are never nested is false.

`test-auto-reshard-throttle` passed this same run: both real 1,050,000-record
objects reshaped inside the sanitizer-scaled deadline, its two completion logs
were present, and no watchdog probe timed out. It is a mandatory regression
gate, not a proposed source change.

## Invariants

- The global cache-lock order is **kfcache handle, then segcache handle**.
  No code may acquire a kf handle while any segcache handle is retained.
- Varlen compaction keeps its existing crash order: emit recipient record,
  sync it, then repoint the kf entry; an uncommitted recipient copy is inert.
- The compaction caller already holds the object's write lock, so rebuilding
  the recipient free list before the scan remains valid for the scan.
- A failed kf lookup must not repoint or delete a donor. A failed recipient
  acquire/sync must not repoint the kf entry.
- Remove only suppressions disproven by the captured stack traces. Do not add
  a race/deadlock suppression.

## Task 1 — lock the red reproduction in before changing production code

Test first. Run each command below on the base revision, preserving the logs.
Each compaction command must exit 66 and include a
`SUMMARY: ThreadSanitizer: lock-order-inversion`; the throttle command must
pass its existing deadline and watchdog assertions.

```sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1" \
  ./build/bin/shard-db-test run test-varlen-compact-recipient-resync
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1" \
  ./build/bin/shard-db-test run test-varlen-compact-donor-resync
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1" \
  ./build/bin/shard-db-test run test-varlen-compact-stat-resync
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1" \
  ./build/bin/shard-db-test run test-compact-crash-recipient-sync
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1" \
  ./build/bin/shard-db-test run test-compact-crash-kf-repoint
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1" \
  ./build/bin/shard-db-test run test-auto-reshard-throttle
```

These existing cases are the correct regression seam: they build the real
varlen donor/recipient topology, execute the production callback, and cover
the crash pause points. Do not add a synthetic lock-only test.

## Task 2 — remove the reverse cache-lock edge from varlen compaction

Root cause: `compact_migrate_records_varlen()` retains `rh`, a recipient
`SlotcaskSegHandle`, across `seg_scan_o_direct()`. Its callback
`varlen_compact_cb()` then acquires a kf writer handle. Other valid paths hold
the kf handle while they verify/read a segment, creating a genuine cycle.

In `src/db/slotcask.c`, locate the quoted anchor
`"typedef struct {\n    SlotcaskDb *db;\n    int         stream_id;"` for
`VarlenCompactCtx`. Replace its recipient-map fields with the path needed to
acquire the recipient inside the callback:

```c
typedef struct {
    SlotcaskDb *db;
    int         stream_id;
    uint32_t    donor_fid;
    uint32_t    recipient_fid;
    char        recipient_path[PATH_MAX];
    uint32_t   *free_offs;
    uint32_t   *free_caps;
    size_t      free_count;
    size_t      free_next;
    int         rc;
    uint32_t    kf_lookup_failed;
} VarlenCompactCtx;
```

In the function anchored by
`"static int varlen_compact_cb(const uint8_t *rec, size_t vlen,"`, preserve
the existing free-slot choice and kf lookup semantics, but make its lock
lifetimes exactly this order:

```c
SlotcaskKfHandle kh;
if (kfcache_acquire(&kh, kfp, c->db->slots_per_shard, 1) != 0) {
    c->rc = -1;
    return 1;
}

/* Existing kf_lookup_with_slot(), orphan, and stale-location handling.
 * Every early return releases kh. */

SlotcaskSegHandle rh;
if (segcache_acquire(&rh, c->recipient_path, 0, 0, 0) != 0) {
    kfcache_release(&kh);
    c->rc = -1;
    return 1;
}
seg_record_emit(rh.map + target_off, (int)donor_rec_size,
                hash16, key, (size_t)klen, value, vlen);
if (durability_msync_range(rh.map, target_off, donor_rec_size) != 0) {
    segcache_release(&rh);
    kfcache_release(&kh);
    c->rc = -1;
    return 1;
}
durability_test_pause(c->db->data_dir, "compact-after-recipient-sync");
kf_repoint_at_slot(&kh, kf_slot_idx, (uint8_t)c->stream_id,
                    (uint16_t)c->recipient_fid, target_off);
segcache_release(&rh);
kfcache_release(&kh);
durability_test_pause(c->db->data_dir, "compact-after-kf-repoint");
return 0;
```

In the function anchored by
`"static int compact_migrate_records_varlen(SlotcaskDb *db, int stream_id,"`,
keep `rh` only while scanning the recipient to construct `free_offs` and
`free_caps`. Before creating `VarlenCompactCtx` and before
`seg_scan_o_direct()`, call `segcache_release(&rh)`. Initialise
`recipient_path` from the already constructed `recipient_path` local with a
bounded copy. Delete each later `segcache_release(&rh)` that would release the
now-ended setup handle. Its cleanup becomes:

```c
if (drc < 0) {
    free(free_offs);
    free(free_caps);
    return -1;
}
if (out_kf_failed) *out_kf_failed = ctx.kf_lookup_failed;
free(free_offs);
free(free_caps);
return ctx.rc;
```

Retain the existing `slotcask_compact_segs()` object-write-lock contract; do
not relax it, cache raw `rh.map` after releasing `rh`, or change record/marker
write ordering.

Run the five Task-1 compaction commands immediately after the change. They
must become green with no TSan output. Temporarily revert only this production
hunk, rerun `test-varlen-compact-recipient-resync`, and record its expected
exit-66 inversion; re-apply the hunk and record the clean green result.

## Task 3 — remove the disproven deadlock suppressions

In `.tsan.supp`, locate the quoted anchor
`"# segcache_acquire / kfcache_acquire follow the same verify-retry pattern"`.
Delete that entire explanatory paragraph and these two entries:

```text
deadlock:segcache_acquire
deadlock:kfcache_acquire
```

Leave unrelated entries untouched. In particular, do not expand a suppression
to cover `slotcask.c`, a whole test case, or a lock family. Re-run the five
focused compaction cases with the normal release option string (including the
remaining suppression file); they must be green because the code no longer
creates the cycle, not because the warning is hidden.

## Task 4 — full TSan release verification

Build and run the full suite twice: first unsuppressed to expose every issue,
then with the repository's remaining narrowly documented suppressions.

```sh
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all
```

Both runs use default all-core parallelism. Each must finish all 419 cases,
report zero failed cases, contain no TSan `WARNING`/`SUMMARY`, and include a
passing `test-auto-reshard-throttle`. If any report appears, group all stacks
by their full lock graph, prove whether it is a real nested acquisition, and
write a new focused plan task before making a change. Do not add a suppression
unless a minimal proof establishes a true tool false positive and documents
the exact function and synchronization rationale.

## Execution rules

- Start execution only after explicit approval, on a fresh
  `fix/tsan-release-gate` branch from current `main`.
- If any quoted anchor is absent, write `PLAN_NOTES.md` and halt; do not
  reinterpret the plan.
- Leave all work uncommitted for raw-diff review.
- No GitHub workflow, release, feature, benchmark, timeout expansion, or test
  skip is in scope. The local default-parallel gate is authoritative; CI is
  checked only after it is clean.

## Amendment — warmup/vacuum kfcache-table race (observed 2026-08-15)

The third post-fix, unsuppressed default-parallel full run found a separate
real race in `test-warmup-vacuum-race`:

```
warmup_kf_task_fn -> kfcache_acquire_ex -> kfcache_probe -> strcmp(e->path)
rebuild_txn_begin -> slotcask_registry_invalidate ->
  kfcache_invalidate_prefix -> e->path[0] = '\\0'
```

`kfcache_probe()` correctly reads table metadata under `g_kfcache_lock`.
`kfcache_invalidate_prefix()` incorrectly reads and mutates the same
`used/path` table metadata with only an entry rwlock. The object rwlock in the
test protects `SlotcaskDb` lifetime, not the cache table. This is unrelated to
the compaction inversion and must not be suppressed.

### Task 5 — synchronize prefix invalidation with kfcache table metadata

Test first. On the current repaired tree, preserve a red log from:

```sh
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1" \
  ./build/bin/shard-db-test run test-warmup-vacuum-race
```

It must report the `kfcache_probe` versus `kfcache_invalidate_prefix` data
race. The existing test is the correct regression seam: its deterministic
warmup delay creates a real in-flight cache acquire while vacuum rebuild
invalidates the object’s cache entries.

In `src/db/slotcask.c`, locate the quoted anchor
`"static void kfcache_invalidate_prefix(const char *prefix) {"`. Replace the
open-coded loop with a helper that snapshots candidate identity under the
table mutex, waits for that entry lock *without* the table mutex, then
reacquires the table mutex to revalidate and clear metadata. The helper must
follow this complete lock/lifetime shape:

```c
static int kfcache_invalidate_slot_if_prefix(int slot, const char *prefix,
                                             size_t prefix_len) {
    KfCacheEntry *e = &g_kfcache[slot];
    uint64_t expected_gen;
    char expected_path[PATH_MAX];

    pthread_mutex_lock(&g_kfcache_lock);
    if (!atomic_load_explicit(&e->used, memory_order_acquire) ||
        strncmp(e->path, prefix, prefix_len) != 0) {
        pthread_mutex_unlock(&g_kfcache_lock);
        return 0;
    }
    expected_gen = atomic_load_explicit(&e->gen, memory_order_acquire);
    snprintf(expected_path, sizeof(expected_path), "%s", e->path);
    pthread_mutex_unlock(&g_kfcache_lock);

    pthread_rwlock_wrlock(&e->rwlock);
    pthread_mutex_lock(&g_kfcache_lock);
    if (!atomic_load_explicit(&e->used, memory_order_acquire) ||
        atomic_load_explicit(&e->gen, memory_order_acquire) != expected_gen ||
        strcmp(e->path, expected_path) != 0 ||
        strncmp(e->path, prefix, prefix_len) != 0) {
        pthread_mutex_unlock(&g_kfcache_lock);
        pthread_rwlock_unlock(&e->rwlock);
        return 0;
    }

    /* Preserve the existing test hold, munmap/close, field clears, dirty
       reset, generation increment, used=false publication, and count
       decrement here while both locks are held. */

    pthread_mutex_unlock(&g_kfcache_lock);
    pthread_rwlock_unlock(&e->rwlock);
    return 1;
}
```

`kfcache_invalidate_prefix()` must iterate slots and call that helper; retain
its test-only early exit after one successful invalidation. Do not hold
`g_kfcache_lock` while waiting for an entry rwlock: `kfcache_acquire_ex()` can
hold an entry lock before retrying the table mutex, so table→entry waiting
would introduce a deadlock. All reads or writes of `e->used` and `e->path`
outside a held entry lock must occur while `g_kfcache_lock` is held.

After the change, run the focused test unsuppressed until it is clean, then
temporarily revert only this helper/invalidation hunk and prove the focused
test is red with the same `kfcache_probe` race; re-apply and record green.
Re-run the three existing compaction tests and two crash variants to ensure
the cache-table repair does not regress the prior lock-order fix.

### Task 6 — repeat the release gate after Task 5

Run the default-parallel unsuppressed full suite three consecutive times,
retaining each log. Each must complete all 419 cases with `12033 passed, 0
failed` (or the then-current assertion total) and contain no TSan warning or
summary. Then run the repository-standard three default-parallel full suites
with the remaining `.tsan.supp` file. Any new report reopens this plan; do not
add a suppression without a separately documented false-positive proof.
