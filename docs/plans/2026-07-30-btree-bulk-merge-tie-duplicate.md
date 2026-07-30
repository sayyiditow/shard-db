# Fix: `btree_bulk_merge` writes a physical duplicate on an exact `(value,hash)` tie

## Root cause

`btree_bulk_merge`'s rebuild path (`src/db/btree.c`) merges the on-disk
tree's current contents (`existing`, from `bt_extract_all`) with the
caller's new batch (`new_entries`) via a standard two-pointer sorted merge,
then rebuilds the whole file from `combined`:

```c
        size_t ei = 0, ni = 0, ci = 0;
        while (ei < exist_count && ni < new_count) {
            if (val_hash_cmp(existing[ei].value, existing[ei].vlen, existing[ei].hash,
                              new_entries[ni].value, new_entries[ni].vlen, new_entries[ni].hash) <= 0)
                combined[ci++] = existing[ei++];
            else
                combined[ci++] = new_entries[ni++];
        }
        while (ei < exist_count) combined[ci++] = existing[ei++];
        while (ni < new_count)   combined[ci++] = new_entries[ni++];
```

`val_hash_cmp` (`src/db/btree.c`, used identically elsewhere in this file
for bsearch) returns `0` only when both `value` and the 16-byte `hash`
compare equal — i.e. the two `BtEntry`s are the *same* index entry, not
just value-adjacent.

The `<= 0` branch treats a `0` (exact match) the same as a `< 0` (existing
sorts first): it takes `existing[ei]`, advances `ei`, and leaves `ni`
untouched. The loop's next iteration compares the *same* `new_entries[ni]`
against `existing[ei+1]`. Since `new_entries[ni]` was never consumed, it
survives to be appended by the trailing `while (ni < new_count)` drain (or
picked up on a later loop iteration) — so the entry that was tied gets
written into `combined[]`, and hence into the rebuilt tree via
`btree_bulk_build_locked`, **twice**.

`BtEntry` (`src/db/btree.h`) carries nothing beyond `(value, vlen, hash)`:

```c
typedef struct {
    const char *value;
    size_t vlen;
    uint8_t hash[BT_HASH_SIZE];
} BtEntry;
```

so on an exact tie the two copies are fully interchangeable — the correct
merge output is one copy, not two.

This is a deterministic logic bug, not a race: any call to
`btree_bulk_merge` whose rebuild path sees the same `(value,hash)` present
in both `existing` and `new_entries` produces a duplicate leaf entry,
100% of the time, regardless of scheduling. (How such an overlap arises in
production call paths — e.g. re-submission of an already-applied index
diff — is a separate question from a separate investigation thread and is
not addressed by this plan; this plan only fixes the merge logic itself,
which is wrong on its own terms independent of how a tie is produced.)

## Call sites (no signature/contract change — internal fix only)

`btree_bulk_merge(const char *path, BtEntry *new_entries, size_t new_count)`
keeps its exact signature and return contract. Callers, for reference,
none requiring changes:

- `src/db/query_bulk.c:27` — `idx_build_worker` (unused, `__attribute__((unused))`)
- `src/db/query_bulk.c:95` — `idx_build_field_worker`, the live production path
- `src/test/cases/test_btree_value_hash_sort.c:128,261`
- `src/test/cases/test_bt_cache_writer_starvation.c:101,123`
- `src/test/cases/test_btree_bulk_merge_delete_race.c:81`

## Task 1 — regression test (test-first)

Add `src/test/cases/test_btree_bulk_merge_tie_duplicate.c`:

```c
/* src/test/cases/test_btree_bulk_merge_tie_duplicate.c
 *
 * Regression for btree_bulk_merge's rebuild-path merge loop: on an exact
 * (value,hash) tie between the on-disk snapshot and the incoming batch,
 * the old code took the existing-side copy but never advanced the
 * new-side cursor, so the tied entry from new_entries survived to be
 * appended a second time by the trailing drain loop — a physical
 * duplicate leaf entry. This seeds a tree, then merges a batch containing
 * one entry that exactly duplicates an already-seeded entry (same value
 * AND hash) plus one genuinely new entry, and asserts the duplicated
 * entry is present exactly once.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "btree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_count;
static int count_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    (void)v; (void)vl; (void)h; (void)ctx;
    g_count++;
    return 0;
}
static int search_count(const char *path, const char *value, size_t vlen) {
    g_count = 0;
    btree_search(path, value, vlen, count_cb, NULL);
    return g_count;
}

#define SEED_COUNT 5

static int test_btree_bulk_merge_tie_duplicate_run(void) {
    char tmpl[] = "/tmp/shard-db-bulk-merge-tie-XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd < 0) { ASSERT_TRUE(0, "mkstemp"); return 1; }
    close(fd);
    unlink(tmpl);
    char path[sizeof(tmpl) + 4];
    snprintf(path, sizeof(path), "%s.idx", tmpl);
    unlink(path);

    BtEntry seed[SEED_COUNT];
    char seed_vals[SEED_COUNT][32];
    for (int i = 0; i < SEED_COUNT; i++) {
        int vlen = snprintf(seed_vals[i], sizeof(seed_vals[i]), "seed_%02d", i);
        seed[i].value = seed_vals[i];
        seed[i].vlen = (size_t)vlen;
        memset(seed[i].hash, 0, BT_HASH_SIZE);
        memcpy(seed[i].hash, &i, sizeof(int));
    }
    ASSERT_EQ_INT(btree_bulk_merge(path, seed, SEED_COUNT), 0, "seed rebuild");
    ASSERT_EQ_INT(search_count(path, seed_vals[2], seed[2].vlen), 1,
        "seed entry present once before merge");

    /* new_entries: one EXACT duplicate of seed[2] (same value + hash),
       plus one genuinely new entry. bt_cmp_entry sorts by (value,hash),
       so after qsort the duplicate is new_entries[0] or [1] depending on
       value ordering — either way it ties exactly with seed[2] during
       the merge. */
    char dup_val[32];
    memcpy(dup_val, seed_vals[2], sizeof(dup_val));
    char new_val[32];
    int new_vlen = snprintf(new_val, sizeof(new_val), "zzz_new");

    BtEntry batch[2];
    batch[0].value = dup_val;
    batch[0].vlen = seed[2].vlen;
    memcpy(batch[0].hash, seed[2].hash, BT_HASH_SIZE);
    batch[1].value = new_val;
    batch[1].vlen = (size_t)new_vlen;
    memset(batch[1].hash, 0, BT_HASH_SIZE);
    batch[1].hash[0] = 0xAB;

    ASSERT_EQ_INT(btree_bulk_merge(path, batch, 2), 0, "merge with tie");

    ASSERT_EQ_INT(search_count(path, seed_vals[2], seed[2].vlen), 1,
        "tied entry present exactly once after merge, not duplicated");
    ASSERT_EQ_INT(search_count(path, new_val, (size_t)new_vlen), 1,
        "genuinely new entry present exactly once after merge");
    for (int i = 0; i < SEED_COUNT; i++) {
        if (i == 2) continue;
        ASSERT_EQ_INT(search_count(path, seed_vals[i], seed[i].vlen), 1,
            "untouched seed entry still present exactly once");
    }

    unlink(path);
    bt_cache_shutdown();
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-btree-bulk-merge-tie-duplicate", test_btree_bulk_merge_tie_duplicate_run)
```

Register it in `build.sh`'s explicit test-file list. Anchor (exact current
text):

```
    src/test/cases/test_btree_bulk_merge_delete_race.c \
    src/test/cases/test_btree_inplace_leaf.c \
```

New text:

```
    src/test/cases/test_btree_bulk_merge_delete_race.c \
    src/test/cases/test_btree_bulk_merge_tie_duplicate.c \
    src/test/cases/test_btree_inplace_leaf.c \
```

**Prove the regression test actually catches the bug** before touching
`btree.c`:

1. Build: `SKIP_TESTS=1 ./build.sh`
2. Run: `./build/bin/shard-db-test run test-btree-bulk-merge-tie-duplicate`
3. Confirm it **fails** on the assertion `"tied entry present exactly once
   after merge, not duplicated"` (expect actual=2). Paste the real output.
4. Only then proceed to Task 2.

## Task 2 — the fix

File: `src/db/btree.c`. Anchor (exact current text, inside
`btree_bulk_merge`'s "Merge two sorted arrays" block):

```c
        size_t ei = 0, ni = 0, ci = 0;
        while (ei < exist_count && ni < new_count) {
            if (val_hash_cmp(existing[ei].value, existing[ei].vlen, existing[ei].hash,
                              new_entries[ni].value, new_entries[ni].vlen, new_entries[ni].hash) <= 0)
                combined[ci++] = existing[ei++];
            else
                combined[ci++] = new_entries[ni++];
        }
        while (ei < exist_count) combined[ci++] = existing[ei++];
        while (ni < new_count)   combined[ci++] = new_entries[ni++];
```

Replace with:

```c
        size_t ei = 0, ni = 0, ci = 0;
        while (ei < exist_count && ni < new_count) {
            int cmp = val_hash_cmp(existing[ei].value, existing[ei].vlen, existing[ei].hash,
                                    new_entries[ni].value, new_entries[ni].vlen, new_entries[ni].hash);
            if (cmp == 0) {
                /* Exact (value,hash) match between the on-disk snapshot
                   and the incoming batch. BtEntry carries no payload
                   beyond (value,hash), so the two copies are
                   interchangeable — keep one and advance both cursors.
                   Advancing only `ei` here previously left new_entries[ni]
                   unconsumed, so the trailing drain loop re-appended it,
                   writing a physical duplicate leaf entry. */
                combined[ci++] = new_entries[ni++];
                ei++;
            } else if (cmp < 0) {
                combined[ci++] = existing[ei++];
            } else {
                combined[ci++] = new_entries[ni++];
            }
        }
        while (ei < exist_count) combined[ci++] = existing[ei++];
        while (ni < new_count)   combined[ci++] = new_entries[ni++];
```

No other change needed: `combined` is already sized `exist_count +
new_count` (still a valid upper bound — dedup only ever makes `ci`
smaller), and `btree_bulk_build_locked(path, combined, ci)` already uses
`ci`, not the buffer capacity, as the entry count.

### Edge cases / invariants

- **Internal duplicates within `new_entries` itself** (two entries in the
  same call with identical `(value,hash)`, not just tying against
  `existing`): not handled by this fix. Every current production call
  site stages `new_entries` from one commit window's per-record inserts
  (`v2_bulk_ins_prepare_window` in `query_bulk.c`), where each active
  record contributes at most one `BtEntry` per indexed field and record
  hashes are unique — so a single call's `new_entries` cannot legitimately
  contain an internal duplicate. This plan fixes the existing-vs-new tie
  only, matching the confirmed bug; it does not add defensive handling for
  a case no current caller can produce.
- **Pre-existing duplicates already present in the on-disk tree** are not
  repaired by this change. The regression assumes the extracted existing
  tree contains one copy of each `(value,hash)` tuple, and the fix removes
  only the one duplicate formed when that tuple appears once in `existing`
  and once in `new_entries`. Cleaning historical duplicate leaves would be a
  separate repair/rebuild requirement, not part of this merge-logic fix.
- **Locking**: the entire merge runs inside the existing
  `bt_mutation_lock_for(path)` / `bt_mutation_lock(lock)` critical section
  (already held before this code). No new locking is introduced or needed.
- **On-disk format**: unchanged. This only changes which entries get
  written, not `BtEntry`, `BtFileHeader`, or the leaf encoding.
- **`entry_count` in `BtFileHeader`**: `btree_bulk_build_locked` derives
  `entry_count` from the array it's given (`ci`), so a deduped `ci` yields
  a correct (smaller) count instead of the previously-inflated one — this
  is a corollary fix, not a separate change.

## Definition of done for this plan

- [ ] Test written first; confirmed **failing** against unfixed code
      (paste output).
- [ ] Fix applied; test confirmed **passing** (paste output).
- [ ] `./build/bin/shard-db-test run-all` — full suite, zero failures,
      fresh run (paste summary line).
- [ ] `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` then
      `ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 2`
      — zero findings (this touches the mutation-locked shared-cache write
      path).
- [ ] `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh` then
      `TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all --jobs 1`
      — zero findings.
- [ ] No leftover diagnostic instrumentation from the investigation
      (`bt_trace`-adjacent additions) — this plan's diff touches only the
      quoted anchors above plus the new test file and its `build.sh`
      registration.
- [ ] Diff left **uncommitted** for review per this repo's standing
      execution-mode exception — human + reviewing agent review the raw
      `git diff` before anything is committed.

## Execution rules

- Branch off `main` (this plan's diff is left uncommitted regardless, per
  this repo's standing exception, but work should still happen off a
  named branch for later commit).
- Do the two tasks in order (test first, confirm red, then fix, confirm
  green).
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run
  test-btree-bulk-merge-tie-duplicate` for the targeted case,
  `./build/bin/shard-db-test run-all` for the full suite.
- If any quoted anchor above doesn't match exactly what's in the tree,
  stop immediately, write `PLAN_NOTES.md` describing the mismatch, and
  halt — do not guess or improvise a fix around it.
- If you hit any decision this plan doesn't cover, stop and ask.
