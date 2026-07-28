# Propagate unindexed keyfile-sync failures

## Goal and root cause

Never return success from an unindexed single-record insert or upsert after
its targeted keyfile `msync(MS_SYNC)` fails. `kfcache_sync_slots_locked()`
correctly returns `-1`, but five callers discard it: the fast upsert new and
existing-key branches, the unindexed slow upsert new and existing-key
branches, and `slotcask_insert_with_hooks`.

The indexed and bulk paths already check this result, which is why a review
that verified use of the helper missed whether every use propagated failure.

## Invariants

- Keep the current order: synchronously persist the segment record before
  publishing the keyfile reference.
- A failed keyfile flush produces `-1`, never an acknowledged success.
- Do not roll back after a failed flush: the keyfile page may have persisted.
  Release the writer so its dirty state remains available for background retry.
- No marker, recovery, index, bulk, protocol, or format behavior changes.

## Task 1 — test first: target the second sync and expose the bug

The current injector only fails the next sync, which is the segment flush.
Add a test-only `fail_on_call` hook, then add a regression test that fails the
second sync: the segment call succeeds and the kf call fails. Before Task 2,
the test must fail because all five APIs return `0`; paste that output.

Insert after the exact `types.h` anchor
`void durability_test_msync_fail_next(int count, int err);`:

```c
void durability_test_msync_fail_on_call(int call_number, int err);
```

In `durability.c`, add these test-only state variables beside the existing
`g_durability_msync_fail_remaining` declaration:

```c
static int g_durability_msync_fail_on_call;
static int g_durability_msync_call_count;
```

Add this complete test-only function after the exact anchor
`void durability_test_msync_fail_next(int count, int err) {` function body:

```c
void durability_test_msync_fail_on_call(int call_number, int err) {
    pthread_mutex_lock(&g_durability_msync_test_lock);
    g_durability_msync_fail_remaining = 0;
    g_durability_msync_fail_on_call = call_number > 0 ? call_number : 0;
    g_durability_msync_fail_errno = err > 0 ? err : EIO;
    pthread_mutex_unlock(&g_durability_msync_test_lock);
}
```

Extend `durability_test_msync_reset()` by inserting these two assignments
immediately after `g_durability_msync_fail_remaining = 0;`:

```c
    g_durability_msync_fail_on_call = 0;
    g_durability_msync_call_count = 0;
```

Extend `durability_test_msync_fail_next()` by inserting this assignment
immediately after its `g_durability_msync_fail_remaining` assignment:

```c
    g_durability_msync_fail_on_call = 0;
```

Replace the condition in the first `#ifdef TEST_BUILD` block of
`durability_msync()` with this complete block (the body remains otherwise
unchanged):

```c
#ifdef TEST_BUILD
    pthread_mutex_lock(&g_durability_msync_test_lock);
    g_durability_msync_call_count++;
    if (g_durability_msync_fail_remaining > 0 ||
        (g_durability_msync_fail_on_call > 0 &&
         g_durability_msync_call_count == g_durability_msync_fail_on_call)) {
        if (g_durability_msync_fail_remaining > 0)
            g_durability_msync_fail_remaining--;
        g_durability_msync_fail_on_call = 0;
        g_durability_msync_failed++;
        int injected_errno = g_durability_msync_fail_errno;
        pthread_mutex_unlock(&g_durability_msync_test_lock);
        errno = injected_errno;
        return -1;
    }
    pthread_mutex_unlock(&g_durability_msync_test_lock);
#endif
```

In `test_durability_sync_failures.c`, add a test helper and a new registered
case. It opens an unindexed object, seeds the two update cases, then calls
`durability_test_msync_fail_on_call(2, EIO)` before each of: fast insert,
fast update, slow insert (`check_needs_old=1`), slow update, and
`slotcask_insert_with_hooks`. Each must return `-1`; after each call, assert
the injector saw exactly one success and one failure. The helper must reset
the injector after every assertion. Register it as:

```c
TEST_REGISTER("test-unindexed-kf-sync-failure",
              test_unindexed_kf_sync_failure_propagates)
```

## Task 2 — propagate all five failure returns

After Task 1 fails as specified, edit `src/db/slotcask.c`. At each anchor
below, replace the unchecked call with the complete block shown. In each case
the release happens before returning because the write lock is still held.

At the fast new-key anchor `size_t cs[] = { put_slot };`:

```c
        {
            size_t cs[] = { put_slot };
            if (kfcache_sync_slots_locked(&kh, cs, 1, 1) != 0) {
                kfcache_release(&kh);
                return -1;
            }
        }
```

At the fast existing-key anchor `size_t cs[] = { ex_slot };`:

```c
        {
            size_t cs[] = { ex_slot };
            if (kfcache_sync_slots_locked(&kh, cs, 1, 0) != 0) {
                kfcache_release(&kh);
                free(old_buf);
                return -1;
            }
        }
```

At the unindexed slow-path existing-key anchor
`{ size_t cs[] = { kf_slot }; kfcache_sync_slots_locked(&kh, cs, 1, 0); }`:

```c
            {
                size_t cs[] = { kf_slot };
                if (kfcache_sync_slots_locked(&kh, cs, 1, 0) != 0) {
                    kfcache_release(&kh);
                    free(old_buf);
                    return -1;
                }
            }
```

At the unindexed slow-path new-key anchor
`{ size_t cs[] = { insert_slot }; kfcache_sync_slots_locked(&kh, cs, 1, 1); }`:

```c
            {
                size_t cs[] = { insert_slot };
                if (kfcache_sync_slots_locked(&kh, cs, 1, 1) != 0) {
                    kfcache_release(&kh);
                    free(old_buf);
                    return -1;
                }
            }
```

At the insert-only anchor
`{ size_t cs[] = { put_slot }; kfcache_sync_slots_locked(&kh, cs, 1, 1); }`:

```c
    {
        size_t cs[] = { put_slot };
        if (kfcache_sync_slots_locked(&kh, cs, 1, 1) != 0) {
            kfcache_release(&kh);
            return -1;
        }
    }
```

## Verification

1. Run `./build/bin/shard-db-test run test-unindexed-kf-sync-failure` before
   Task 2; it must fail because the unchecked paths return success.
2. Apply Task 2 and rerun that test; it must pass.
3. Run `SKIP_TESTS=1 ./build.sh`, then
   `./build/bin/shard-db-test run test-durability-sync-failures` and the new
   regression test.
4. Run the required affected-test sanitizer gates:
   `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` then
   `ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run test-unindexed-kf-sync-failure`;
   `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh` then
   `TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run test-unindexed-kf-sync-failure`.
5. Inspect the raw diff and leave it uncommitted.

## Execution rules

- Execute tasks in order; do not change behavior beyond this failure path.
- If any quoted anchor is absent, write `PLAN_NOTES.md` and halt rather than
  reinterpret the plan.
- Do not weaken tests or rerun unexpected failures until green.
