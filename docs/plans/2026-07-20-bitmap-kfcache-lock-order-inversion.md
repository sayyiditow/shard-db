# Plan: eliminate bitmap-cache / kfcache lock-order inversion

## Status

Implementation and verification complete; changes remain uncommitted for
review. Normal, ASan+UBSan, and TSan full suites each passed 336 cases and
10,797 assertions with zero failures. The five focused lock-order cases passed
68 assertions under normal, ASan+UBSan, and TSan. Temporarily restoring the
old equality path reproduced the actual bitmap-to-kfcache deadlock; the
pre-existing `deadlock:kfcache_acquire` suppression prevented TSan from
flushing a separate lock-order report before the hung process was stopped.

This plan fixes a real TSan-reported lock-order inversion between the
per-kfcache-entry rwlock and the per-bitmap-cache-entry rwlock. Execution must
remain uncommitted for review, per `AGENTS.md`.

## Root cause

CRUD writes acquire the target kfcache entry as a writer and invoke bitmap
index maintenance from the pre-commit hook before releasing that kfcache
handle:

```text
slotcask_upsert_with_hooks / slotcask_delete_with_hooks
  kfcache_acquire(..., writer=1)
  opts->pre_commit(...)
    v2_*_pre_commit(...)
      update_idx_fn(...)
        bitmap_update(...)
          bm_open(..., writer=1)
```

That establishes the required order:

```text
kfcache entry -> bitmap-cache entry
```

Five current paths establish the reverse order:

1. `bitmap_emit_for_shard` opens a bitmap reader, then acquires its kf shard.
2. `bitmap_emit_generic_for_shard` opens and scans a bitmap reader, then
   acquires its kf shard.
3. `build_keyset_from_bitmap` Pass B opens a bitmap reader, then acquires its
   kf shard.
4. `build_keyset_bitmap_complement` opens a bitmap reader, then acquires its
   kf shard.
5. `bm_shard_walk_worker` opens a bitmap writer and calls
   `slotcask_walk_one_shard_slots`, which acquires the kf reader internally.

The first four can deadlock directly against a CRUD writer on the same shard:

```text
writer: holds kfcache wrlock -> waits for bitmap wrlock
reader: holds bitmap rdlock  -> waits for kfcache rdlock
```

The fifth path is normally protected from CRUD by the per-object schema
wrlock, but it still violates the global cross-cache order and contributes a
bitmap-to-kfcache edge to TSan's lock graph. It must be fixed rather than
declared an exception.

## Completed call-path audit

The audit was performed against current `main` with:

```bash
rtk rg -n "bm_open|kfcache_acquire|slotcask_walk_one_shard_slots" \
  src/db/index.c src/db/query.c src/db/query_aggregate.c \
  src/db/query_bulk.c src/db/query_maint.c src/db/query_plan.c \
  src/db/query_schema.c src/db/slotcask.c
```

**Correction (post-halt):** the command above now includes
`src/db/query_maint.c`, which the first pass of this audit omitted. See
"Signature-change consumer audit" below for the consumer this added.

Results:

- The only functions that directly hold both entry types are the four
  `query.c` paths listed above.
- The only additional transitive bitmap-to-kfcache paths are
  `bm_shard_walk_worker -> slotcask_walk_one_shard_slots` in `index.c` and
  `cmd_estimate_index -> slotcask_walk_one_shard_slots` in `query_maint.c`.
  Neither of `query_maint.c`'s callers ever holds a bitmap handle, so this
  second path carries no bitmap-cache lock edge; it only needs the new
  required-handle parameter satisfied.
- `query_aggregate.c` holds bitmap handles only while performing bitmap-only
  operations or calls the already-listed `bitmap_emit_for_shard`.
- `query_plan.c` opens a bitmap only for cardinality counting and closes it
  before returning.
- `query_schema.c` opens bitmap writers only to materialize empty files.
- `query_bulk.c` flushes queued bitmap updates after
  `slotcask_bulk_upsert_batch` has returned and released its kfcache handle.
- `resolve_bitmaps` in `index.c` uses independent read-only `mmap` mappings of
  kf files, not kfcache handles, and therefore does not add a kfcache lock
  edge.
- `shard_count_worker` already acquires kfcache before all bitmap handles and
  remains unchanged.

### Signature-change consumer audit

Task 5 renames the internal function
`slotcask_walk_one_shard_slots` to
`slotcask_walk_one_shard_slots_locked` and adds a pre-acquired
`SlotcaskKfHandle` parameter.

**Correction (post-halt):** the original audit command in this section did
not include `src/db/query_maint.c` in its search path, so it missed a second
call site. The complete consumer list is:

- Declaration: `src/db/slotcask.h`.
- Definition: `src/db/slotcask.c`.
- Call site 1: `bm_shard_walk_worker` in `src/db/index.c`.
- Call site 2: `cmd_estimate_index`'s trigram-sampling loop in
  `src/db/query_maint.c`.

There are no other repository call sites and no external consumer: the
installed embedded API is `src/db/shard_db.h`; `slotcask.h` is internal.

`cmd_estimate_index` samples up to `TG_ESTIMATE_SAMPLE` records per shard to
project a hypothetical trigram-index size; it has no bitmap interaction, so
it needs only a local, per-shard `kfcache_acquire`/`kfcache_release` around
the walk to satisfy the new required-handle parameter — no cross-cache
ordering decision applies here since no bitmap handle is ever held. Task 5.3
below now covers this second call site as well as `index.c`'s.

## Design decision

Standardize every nested acquisition on:

```text
kfcache entry -> bitmap-cache entry
```

The write-side order is structural to the commit/pre-commit protocol, so
reversing it would require moving index maintenance outside the kf commit
critical section and would change data-integrity semantics.

The query-side change holds the kfcache reader slightly longer—across
`bm_open`, and for the generic path across the bitmap dictionary scan. That is
the deliberate tradeoff for a consistent snapshot of the slot-addressed
bitmap and its kf entries. Closing and reopening the bitmap to shorten the kf
hold would introduce a mutation window between dictionary discovery and
bitmap walking.

The rebuild path will make ownership explicit: `index.c` acquires kfcache,
then bitmap, calls a walker that is documented to require the already-held
kfcache handle, then closes bitmap and releases kfcache.

Approval of this plan also confirms the TDD seam: tests exercise the public
newline-delimited JSON protocol through `TestEnv` and `TestClient`. TSan is
the lock-order oracle; response assertions independently verify query and
rebuild behavior. No private function mocking or test-only production hook is
introduced.

The fixture declares exactly one bitmap field. Each ordinary insert therefore
calls `parallel_for` with `n == 1`, which executes `bitmap_update` inline while
the same thread holds the kfcache write handle. This deterministically records
the write-side kfcache-to-bitmap edge before each focused test exercises one
opposite-order path.

## Invariants and edge cases

- Any path holding both handles acquires kfcache first and bitmap second.
- When acquisition is partial, the already-acquired handle is released on
  every return/continue path.
- Successful paths close the bitmap before releasing kfcache.
- A missing bitmap file preserves the existing result semantics: the shard
  contributes no matches.
- A kfcache acquisition failure preserves the existing result semantics: the
  shard contributes no matches and no bitmap handle is opened.
- Generic dictionary scans with zero matching dictionary values close both
  handles and emit nothing.
- Callback stop, timeout, and allocation behavior remain unchanged.
- The bitmap bit position continues to be interpreted against the kf slot
  array from the same shard.
- The rebuild walker never acquires or releases the supplied kfcache handle;
  its caller owns that handle for the entire walk.
- No wire format, on-disk format, CLI flag, or public embedded API changes.
- No dependency is added.
- No benchmark is run; benchmarks remain user-owned.

## Execution rules

1. Begin from current `main` and create `fix/bitmap-kfcache-lock-order`.
2. Preserve all unrelated tracked and untracked work. In particular, do not
   add, modify, or remove other files under `docs/plans/`.
3. Execute the tasks below in order. Each task is a vertical red/green slice.
4. Leave every change uncommitted. Do not stage, commit, push, or open a PR.
5. Paste the real output of every build, test, and sanitizer command into the
   execution report. Never report a command as passed without its output.
6. Never weaken, skip, delete, or loosen a test to make a failure disappear.
7. A failing test that passes on rerun is still a bug. Stop and root-cause it;
   do not rerun until green.
8. If any quoted anchor is not found exactly, create `PLAN_NOTES.md`
   describing the mismatch and halt the entire execution run immediately.
   Do not guess, reinterpret, or continue to another task. Execution may
   resume only after the human or planning model decides whether the plan has
   a stale anchor or a wrong assumption and supplies a patched or replacement
   plan.
9. If execution reaches any decision not covered here, stop and ask the
   human. Do not improvise.

Start with:

```bash
rtk git status --short --branch
rtk git switch -c fix/bitmap-kfcache-lock-order
```

If that branch already exists, halt and ask rather than reusing or deleting
it.

## Focused TSan red/green protocol

Each task below names one focused test. For every red or green run, use a
fresh report directory:

```bash
bitmap_lock_tsan_dir="$(rtk mktemp -d /tmp/shard-db-bitmap-lock-tsan.XXXXXX)"
BUILD_MODE=tsan SKIP_TESTS=1 rtk ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:report_signal_unsafe=0:report_atomic_races=0:suppressions=$PWD/.tsan.supp:log_path=$bitmap_lock_tsan_dir/tsan" \
  rtk ./build/bin/shard-db-test run <test-name>
```

For a red run, prove the expected failure with:

```bash
rtk rg -n "WARNING: ThreadSanitizer: lock-order-inversion|bm_open|kfcache_acquire" \
  "$bitmap_lock_tsan_dir"
```

The red run is valid only when the report contains
`WARNING: ThreadSanitizer: lock-order-inversion` and stacks naming both
`bm_open` and `kfcache_acquire`. A TAP-green test with that sanitizer report
is still red.

For a green run, fail the shell step if the inversion remains:

```bash
if rtk rg -n "WARNING: ThreadSanitizer: lock-order-inversion" \
  "$bitmap_lock_tsan_dir"; then
  exit 1
fi
```

Keep each report directory until the final execution report has quoted the
required red and green evidence.

## Task 1 — expose the finding and fix equality bitmap emission

### 1.1 Add the focused public-protocol test first

Create `src/test/cases/test_bitmap_kfcache_lock_order.c` with this complete
content:

```c
/* Public-protocol TSan regressions for the kfcache -> bitmap-cache order.
 * Every test performs ordinary inserts first, establishing the write-side
 * order, then exercises exactly one read/rebuild route. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    TestEnv env;
    TestClient *tc;
} LockOrderFixture;

static void lock_order_fixture_stop(LockOrderFixture *f) {
    if (f->tc) tc_close(f->tc);
    f->tc = NULL;
    test_env_stop(&f->env);
}

static int request_contains(TestClient *tc, const char *request,
                            const char *needle, const char *description) {
    char *resp = NULL;
    int rc = tc_request(tc, request, &resp);
    int ok = rc == 0 && resp && strstr(resp, needle) != NULL;
    ASSERT_TRUE(ok, description);
    free(resp);
    return ok ? 0 : -1;
}

static int lock_order_fixture_start(LockOrderFixture *f) {
    memset(f, 0, sizeof(*f));
    if (test_env_start(&f->env) != 0) {
        ASSERT_TRUE(0, "start isolated daemon");
        return -1;
    }

    TestClientCfg cfg = {
        .port = f->env.port,
        .io_timeout_ms = 30000,
    };
    f->tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(f->tc, "connect to isolated daemon");
    if (!f->tc) {
        lock_order_fixture_stop(f);
        return -1;
    }

    if (request_contains(
            f->tc,
            "{\"mode\":\"add-dir\",\"dir\":\"lock_order\"}",
            "\"dir\":\"lock_order\"",
            "create lock-order test tenant") != 0 ||
        request_contains(
            f->tc,
            "{\"mode\":\"create-object\",\"dir\":\"lock_order\","
            "\"object\":\"rows\",\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"bucket:varchar:8\",\"kind:varchar:8\","
            "\"score:int\"],"
            "\"indexes\":[\"bucket\",\"kind:bitmap\",\"score\"]}",
            "\"status\":\"created\"",
            "create object with bitmap and btree indexes") != 0) {
        lock_order_fixture_stop(f);
        return -1;
    }

    static const char *keys[] = {
        "k1", "k2", "k3", "k4", "k5", "k6"
    };
    static const char *buckets[] = {
        "g1", "g1", "g2", "g2", "g2", "g2"
    };
    static const char *kinds[] = {
        "alpha", "alpha", "alpha", "alpha", "alpha", "beta"
    };
    static const int scores[] = { 10, 20, 30, 40, 50, 60 };

    for (int i = 0; i < 6; i++) {
        char request[512];
        snprintf(
            request, sizeof(request),
            "{\"mode\":\"insert\",\"dir\":\"lock_order\","
            "\"object\":\"rows\",\"key\":\"%s\","
            "\"value\":{\"bucket\":\"%s\",\"kind\":\"%s\","
            "\"score\":%d}}",
            keys[i], buckets[i], kinds[i], scores[i]);
        if (request_contains(f->tc, request, "\"status\":\"inserted\"",
                             "insert bitmap-indexed row") != 0) {
            lock_order_fixture_stop(f);
            return -1;
        }
    }
    return 0;
}

static int test_bitmap_kfcache_lock_order_eq(void) {
    LockOrderFixture f;
    if (lock_order_fixture_start(&f) != 0) return 1;

    char *resp = NULL;
    int rc = tc_request(
        f.tc,
        "{\"mode\":\"find\",\"dir\":\"lock_order\",\"object\":\"rows\","
        "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\","
        "\"value\":\"alpha\"}],\"limit\":10}",
        &resp);
    ASSERT_TRUE(rc == 0 && resp != NULL, "bitmap equality find returns");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k1\"") != NULL,
                "bitmap equality includes first alpha row");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k5\"") != NULL,
                "bitmap equality includes last alpha row");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k6\"") == NULL,
                "bitmap equality excludes beta row");
    free(resp);

    lock_order_fixture_stop(&f);
    return 0;
}

TEST_REGISTER("test-bitmap-kfcache-lock-order-eq",
              test_bitmap_kfcache_lock_order_eq)
```

In `build.sh`, find this exact anchor:

```bash
    src/test/cases/test_bitmap_index.c \
    src/test/cases/test_bm_intersect_count.c \
```

Replace it with:

```bash
    src/test/cases/test_bitmap_index.c \
    src/test/cases/test_bitmap_kfcache_lock_order.c \
    src/test/cases/test_bm_intersect_count.c \
```

### 1.2 Remove the suppression before the red run

In `.tsan.supp`, remove this exact complete block:

```text
# bm_open (bitmap.c:454) reports a lock-order-inversion (potential deadlock)
# between the kfcache lock and the bitmap lock. The write path takes
# kfcache-then-bitmap (slotcask_upsert_with_hooks -> kfcache_acquire held
# across pre_commit -> bitmap_update -> bm_open), structural across 20+
# sites in slotcask.c. The read path takes bitmap-then-kfcache at 4
# confirmed sites in query.c (bitmap_emit_for_shard, bitmap_emit_generic_
# for_shard, build_keyset_from_bitmap Pass B, build_smaller_bitmap_keyset).
# Real bug -- classic AB-BA cross-thread deadlock risk under load. Root-
# caused and call-site-audited in
# docs/plans/2026-07-20-bitmap-kfcache-lock-order-inversion.md; fix
# (standardize read-path lock order to kf-then-bitmap, matching
# shard_count_worker's existing correct precedent) deliberately deferred
# to its own reviewed branch, not bundled into durability-sync. Tracked
# as a backlog item.
deadlock:bm_open
```

Run the focused TSan protocol with
`test-bitmap-kfcache-lock-order-eq`. It must be red for the expected
`bm_open` / `kfcache_acquire` inversion. Paste the report.

### 1.3 Reorder `bitmap_emit_for_shard`

In `src/db/query.c`, find this exact function:

```c
int bitmap_emit_for_shard(const char *db_root, const char *object,
                                 const char *field, int shard_idx,
                                 const uint8_t *value, size_t vlen,
                                 bt_result_cb cb, void *ctx, SlotcaskDb *sdb) {
    char bp[1024];
    bm_build_path(bp, sizeof(bp), db_root, object, field, shard_idx);
    BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
    if (!bm) return 0;

    char kfp[PATH_MAX];
    slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, shard_idx);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0) {
        bm_close(bm);
        return 0;
    }

    BmEmitCtx ec = { kh.map, kh.capacity, value, vlen, cb, ctx, 0 };
    bm_walk(bm, value, vlen, bm_emit_cb, &ec);

    idx_count_cb_flush_thread();

    kfcache_release(&kh);
    bm_close(bm);
    return ec.stop;
}
```

Replace it with:

```c
int bitmap_emit_for_shard(const char *db_root, const char *object,
                                 const char *field, int shard_idx,
                                 const uint8_t *value, size_t vlen,
                                 bt_result_cb cb, void *ctx, SlotcaskDb *sdb) {
    char kfp[PATH_MAX];
    slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, shard_idx);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0)
        return 0;

    char bp[1024];
    bm_build_path(bp, sizeof(bp), db_root, object, field, shard_idx);
    BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
    if (!bm) {
        kfcache_release(&kh);
        return 0;
    }

    BmEmitCtx ec = { kh.map, kh.capacity, value, vlen, cb, ctx, 0 };
    bm_walk(bm, value, vlen, bm_emit_cb, &ec);

    idx_count_cb_flush_thread();

    bm_close(bm);
    kfcache_release(&kh);
    return ec.stop;
}
```

Run the focused TSan protocol again with
`test-bitmap-kfcache-lock-order-eq`. It must be green.

Then prove sensitivity exactly as required by `CORE-PROCESS.md`:

1. Temporarily replace the new function with the old function block above.
2. Rebuild and rerun the focused protocol; paste the expected red report.
3. Restore the new function block exactly.
4. Rebuild and rerun; paste the green output and no-finding check.

Do not continue until all four observations are recorded: initial red, green,
temporary-revert red, restored green.

## Task 2 — fix generic bitmap emission

### 2.1 Add the next focused test first

In `src/test/cases/test_bitmap_kfcache_lock_order.c`, find this exact anchor:

```c
TEST_REGISTER("test-bitmap-kfcache-lock-order-eq",
              test_bitmap_kfcache_lock_order_eq)
```

Append this complete block immediately after it:

```c

static int test_bitmap_kfcache_lock_order_generic(void) {
    LockOrderFixture f;
    if (lock_order_fixture_start(&f) != 0) return 1;

    char *resp = NULL;
    int rc = tc_request(
        f.tc,
        "{\"mode\":\"find\",\"dir\":\"lock_order\",\"object\":\"rows\","
        "\"criteria\":[{\"field\":\"kind\",\"op\":\"lt\","
        "\"value\":\"beta\"}],\"limit\":10}",
        &resp);
    ASSERT_TRUE(rc == 0 && resp != NULL, "generic bitmap find returns");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k1\"") != NULL,
                "generic bitmap find includes first alpha row");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k2\"") != NULL,
                "generic bitmap find includes second alpha row");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k6\"") == NULL,
                "generic bitmap find excludes beta row");
    free(resp);

    lock_order_fixture_stop(&f);
    return 0;
}

TEST_REGISTER("test-bitmap-kfcache-lock-order-generic",
              test_bitmap_kfcache_lock_order_generic)
```

Run the focused TSan protocol with
`test-bitmap-kfcache-lock-order-generic`. It must be red and name
`bitmap_emit_generic_for_shard`.

### 2.2 Reorder `bitmap_emit_generic_for_shard`

In `src/db/query.c`, find this exact function:

```c
static int bitmap_emit_generic_for_shard(const char *db_root, const char *object,
                                          const char *field, int shard_idx,
                                          SearchCriterion *crit,
                                          const TypedField *tf,
                                          bt_result_cb cb, void *ctx,
                                          SlotcaskDb *sdb) {
    char bp[1024];
    bm_build_path(bp, sizeof(bp), db_root, object, field, shard_idx);
    BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
    if (!bm) return 0;

    BmDictMatchCtx m = { .crit = crit, .tf = tf, .n_match = 0 };
    bm_iter_values(bm, bm_dict_match_cb, &m);
    if (m.n_match == 0) { bm_close(bm); return 0; }

    char kfp[PATH_MAX];
    slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, shard_idx);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0) {
        bm_close(bm);
        return 0;
    }

    int stop = 0;
    for (int i = 0; i < m.n_match && !stop; i++) {
        BmEmitCtx ec = { kh.map, kh.capacity, m.vals[i], m.vlens[i],
                         cb, ctx, 0 };
        bm_walk(bm, m.vals[i], m.vlens[i], bm_emit_cb, &ec);
        if (ec.stop) stop = 1;
    }
    idx_count_cb_flush_thread();

    kfcache_release(&kh);
    bm_close(bm);
    return stop;
}
```

Replace it with:

```c
static int bitmap_emit_generic_for_shard(const char *db_root, const char *object,
                                          const char *field, int shard_idx,
                                          SearchCriterion *crit,
                                          const TypedField *tf,
                                          bt_result_cb cb, void *ctx,
                                          SlotcaskDb *sdb) {
    char kfp[PATH_MAX];
    slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, shard_idx);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0)
        return 0;

    char bp[1024];
    bm_build_path(bp, sizeof(bp), db_root, object, field, shard_idx);
    BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
    if (!bm) {
        kfcache_release(&kh);
        return 0;
    }

    BmDictMatchCtx m = { .crit = crit, .tf = tf, .n_match = 0 };
    bm_iter_values(bm, bm_dict_match_cb, &m);
    if (m.n_match == 0) {
        bm_close(bm);
        kfcache_release(&kh);
        return 0;
    }

    int stop = 0;
    for (int i = 0; i < m.n_match && !stop; i++) {
        BmEmitCtx ec = { kh.map, kh.capacity, m.vals[i], m.vlens[i],
                         cb, ctx, 0 };
        bm_walk(bm, m.vals[i], m.vlens[i], bm_emit_cb, &ec);
        if (ec.stop) stop = 1;
    }
    idx_count_cb_flush_thread();

    bm_close(bm);
    kfcache_release(&kh);
    return stop;
}
```

Run the focused TSan protocol for the generic test and require green. Then
temporarily restore only the old generic function, rerun and paste the
expected red report, restore the new function, and rerun to green. Do not
continue until the red/green/revert-red/reapply-green evidence is recorded.

## Task 3 — fix bitmap KeySet Pass B

### 3.1 Add the KeySet-path test first

In `src/test/cases/test_bitmap_kfcache_lock_order.c`, find this exact anchor:

```c
TEST_REGISTER("test-bitmap-kfcache-lock-order-generic",
              test_bitmap_kfcache_lock_order_generic)
```

Append:

```c

static int test_bitmap_kfcache_lock_order_keyset(void) {
    LockOrderFixture f;
    if (lock_order_fixture_start(&f) != 0) return 1;

    char *resp = NULL;
    int rc = tc_request(
        f.tc,
        "{\"mode\":\"find\",\"dir\":\"lock_order\",\"object\":\"rows\","
        "\"criteria\":[{\"or\":["
        "{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"beta\"},"
        "{\"field\":\"score\",\"op\":\"eq\",\"value\":\"10\"}]}],"
        "\"limit\":10}",
        &resp);
    ASSERT_TRUE(rc == 0 && resp != NULL, "bitmap KeySet find returns");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k6\"") != NULL,
                "bitmap OR KeySet includes beta row");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k1\"") != NULL,
                "bitmap OR KeySet includes score=10 row");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"k2\"") == NULL,
                "bitmap OR KeySet excludes non-matching row");
    free(resp);

    lock_order_fixture_stop(&f);
    return 0;
}

TEST_REGISTER("test-bitmap-kfcache-lock-order-keyset",
              test_bitmap_kfcache_lock_order_keyset)
```

The fully indexed OR forces `build_or_keyset`, whose bitmap child is built
through `build_keyset_from_bitmap` Pass B. It does not use the
single-primary streaming emitter already fixed in Tasks 1 and 2.

Run the focused TSan protocol with
`test-bitmap-kfcache-lock-order-keyset`. It must be red and include the
`build_keyset_from_bitmap` stack.

### 3.2 Reorder the Pass B per-shard hunk

In `src/db/query.c`, under the exact anchor:

```c
    /* Pass B: walk the bitmaps and lift matching hashes via kf lookup.
       Serial across shards — the inserts themselves run lock-free
       (keyset_insert uses per-bucket CAS) so a parallel-walk would be
       safe, but kfcache_acquire / page faults on cold kf are the real
       cost and that doesn't trivially parallelise. For each shard we
       walk every value's bitmap into the same keyset; duplicates are
       impossible (values are distinct → bitmaps disjoint) so no extra
       check needed. */
```

Find this exact loop:

```c
    for (int s = 0; s < splits; s++) {
        if (dl && dl->timed_out) {
            keyset_free(ks); free(vals); free(vlens); return NULL;
        }

        char bp[1024];
        bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
        if (!bm) continue;

        char kfp[PATH_MAX];
        slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0) {
            bm_close(bm);
            continue;
        }

        BmCollectCtx c = { kh.map, kh.capacity, ks, dl };
        for (int i = 0; i < n_kept; i++) {
            bm_walk(bm, vals[i], vlens[i], bm_collect_to_keyset_cb, &c);
        }

        kfcache_release(&kh);
        bm_close(bm);
    }
```

Replace it with:

```c
    for (int s = 0; s < splits; s++) {
        if (dl && dl->timed_out) {
            keyset_free(ks); free(vals); free(vlens); return NULL;
        }

        char kfp[PATH_MAX];
        slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0)
            continue;

        char bp[1024];
        bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
        if (!bm) {
            kfcache_release(&kh);
            continue;
        }

        BmCollectCtx c = { kh.map, kh.capacity, ks, dl };
        for (int i = 0; i < n_kept; i++) {
            bm_walk(bm, vals[i], vlens[i], bm_collect_to_keyset_cb, &c);
        }

        bm_close(bm);
        kfcache_release(&kh);
    }
```

Run the focused KeySet test to green. Then temporarily restore only the old
loop, rerun to the expected red report, restore the new loop, and rerun to
green. Paste all evidence before continuing.

## Task 4 — fix the smaller-side complement builder

### 4.1 Add the complement-path test first

In `src/test/cases/test_bitmap_kfcache_lock_order.c`, find:

```c
TEST_REGISTER("test-bitmap-kfcache-lock-order-keyset",
              test_bitmap_kfcache_lock_order_keyset)
```

Append:

```c

static int test_bitmap_kfcache_lock_order_complement(void) {
    LockOrderFixture f;
    if (lock_order_fixture_start(&f) != 0) return 1;

    char *resp = NULL;
    int rc = tc_request(
        f.tc,
        "{\"mode\":\"aggregate\",\"dir\":\"lock_order\","
        "\"object\":\"rows\",\"group_by\":[\"bucket\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\","
        "\"value\":\"alpha\"}],\"order_by\":\"n\","
        "\"order\":\"desc\",\"limit\":2}",
        &resp);
    ASSERT_TRUE(rc == 0 && resp != NULL,
                "majority-bitmap top-N aggregate returns");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"bucket\":\"g2\",\"n\":3") != NULL,
                "majority-bitmap complement keeps g2 count");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"bucket\":\"g1\",\"n\":2") != NULL,
                "majority-bitmap complement keeps g1 count");
    free(resp);

    lock_order_fixture_stop(&f);
    return 0;
}

TEST_REGISTER("test-bitmap-kfcache-lock-order-complement",
              test_bitmap_kfcache_lock_order_complement)
```

Five of six rows match `kind=alpha`, so the smaller side is the one-row
complement and `build_keyset_bitmap_complement` is required.

Run the focused TSan protocol with
`test-bitmap-kfcache-lock-order-complement`. It must be red and include the
complement-builder stack.

### 4.2 Reorder the complement per-shard hunk

In `src/db/query.c`, inside
`build_keyset_bitmap_complement`, find:

```c
    for (int s = 0; s < splits; s++) {
        if (dl && dl->timed_out) { keyset_free(ks); return NULL; }
        char bp[1024];
        bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
        if (!bm) continue;

        char kfp[PATH_MAX];
        slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0) {
            bm_close(bm);
            continue;
        }

        BmCollectCtx c = { kh.map, kh.capacity, ks, dl };
        BmComplementCtx cc = { bm, tvals, tvlens, nt, &c };
        bm_iter_values(bm, bm_complement_value_cb, &cc);

        kfcache_release(&kh);
        bm_close(bm);
    }
```

Replace it with:

```c
    for (int s = 0; s < splits; s++) {
        if (dl && dl->timed_out) { keyset_free(ks); return NULL; }

        char kfp[PATH_MAX];
        slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0)
            continue;

        char bp[1024];
        bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
        if (!bm) {
            kfcache_release(&kh);
            continue;
        }

        BmCollectCtx c = { kh.map, kh.capacity, ks, dl };
        BmComplementCtx cc = { bm, tvals, tvlens, nt, &c };
        bm_iter_values(bm, bm_complement_value_cb, &cc);

        bm_close(bm);
        kfcache_release(&kh);
    }
```

Run the focused complement test to green. Then temporarily restore only the
old loop, rerun to the expected red report, restore the new loop, and rerun
to green. Paste all evidence.

## Task 5 — fix the indirect bitmap rebuild order

### 5.1 Add the add-index rebuild test first

In `src/test/cases/test_bitmap_kfcache_lock_order.c`, find:

```c
TEST_REGISTER("test-bitmap-kfcache-lock-order-complement",
              test_bitmap_kfcache_lock_order_complement)
```

Append:

```c

static int test_bitmap_kfcache_lock_order_rebuild(void) {
    LockOrderFixture f;
    if (lock_order_fixture_start(&f) != 0) return 1;

    char *resp = NULL;
    int rc = tc_request(
        f.tc,
        "{\"mode\":\"add-index\",\"dir\":\"lock_order\","
        "\"object\":\"rows\",\"field\":\"kind:bitmap\","
        "\"force\":true}",
        &resp);
    ASSERT_TRUE(rc == 0 && resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "forced bitmap rebuild succeeds");
    free(resp);
    resp = NULL;

    rc = tc_request(
        f.tc,
        "{\"mode\":\"count\",\"dir\":\"lock_order\",\"object\":\"rows\","
        "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\","
        "\"value\":\"alpha\"}]}",
        &resp);
    ASSERT_TRUE(rc == 0 && resp != NULL,
                "post-rebuild bitmap count returns");
    ASSERT_EQ_INT(tu_parse_count(resp), 5,
                  "post-rebuild bitmap count preserves five alpha rows");
    free(resp);

    lock_order_fixture_stop(&f);
    return 0;
}

TEST_REGISTER("test-bitmap-kfcache-lock-order-rebuild",
              test_bitmap_kfcache_lock_order_rebuild)
```

Run the focused TSan protocol with
`test-bitmap-kfcache-lock-order-rebuild`. It must be red with the path
`bm_shard_walk_worker -> slotcask_walk_one_shard_slots ->
kfcache_acquire`.

### 5.2 Make the walker require a pre-acquired kfcache handle

In `src/db/slotcask.h`, find:

```c
/* Same as slotcask_walk_one_shard but the callback also receives the
   kf slot index. Used by the bitmap-index reindex path which needs to
   key bit positions by (kf_shard, kf_slot). */
typedef int (*SlotcaskScanSlotCb)(uint32_t slot, const uint8_t hash16[16],
                                   const void *key, size_t klen,
                                   const void *value, size_t vlen,
                                   void *ctx);
int slotcask_walk_one_shard_slots(SlotcaskDb *db, int kf_shard_id,
                                   SlotcaskScanSlotCb cb, void *ctx);
```

Replace it with:

```c
/* Same as slotcask_walk_one_shard but the callback also receives the
   kf slot index. Used by the bitmap-index reindex path which needs to
   key bit positions by (kf_shard, kf_slot).

   Caller must hold `kh` for `kf_shard_id` for the entire call. This
   function neither acquires nor releases it; making ownership explicit
   lets callers establish any outer cross-cache lock order before walking. */
typedef int (*SlotcaskScanSlotCb)(uint32_t slot, const uint8_t hash16[16],
                                   const void *key, size_t klen,
                                   const void *value, size_t vlen,
                                   void *ctx);
int slotcask_walk_one_shard_slots_locked(SlotcaskDb *db, int kf_shard_id,
                                          const SlotcaskKfHandle *kh,
                                          SlotcaskScanSlotCb cb, void *ctx);
```

In `src/db/slotcask.c`, find the complete function beginning with this exact
anchor:

```c
/* Slot-aware walker: iterates kf entries in slot order, calling cb per
   live entry with the slot index alongside the usual (hash, key, value).
   Used by the bitmap-index reindex path. Single-threaded — no fan-out;
   reindex callers typically already parallel_for over shards externally. */
```

Replace the old function:

```c
int slotcask_walk_one_shard_slots(SlotcaskDb *db, int kf_shard_id,
                                   SlotcaskScanSlotCb cb, void *ctx) {
    if (!db || !cb || kf_shard_id < 0 || kf_shard_id >= db->num_shards)
        return -1;
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) return -1;

    int rc = 0;
    for (size_t s = 0; s < kh.capacity; s++) {
        SlotcaskKfEntry *e = &kh.map[s];
        if (e->flag != 1) continue;
        /* read_record_value verifies the key matches; for reindex we
           trust the kf entry's pointer (kf is authoritative), so pass
           the kf entry's known-good record header. We need the key to
           call read_record_value, so we read the seg's key-prefix
           first via a small inline buffer. */
        uint8_t *vbuf = NULL;
        size_t   vlen = 0;
        /* The seg record header is 24B: 16B hash + 2B klen + 1B flag +
           1B reserved + 4B vlen. The key starts at offset+24. We read
           klen first, then call read_record_value with the in-seg key
           bytes. To avoid that two-step, lean on segcache_acquire +
           direct mmap read for simplicity. */
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, e->stream_id, e->file_id);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) continue;
        const uint8_t *base = (const uint8_t *)h.map + (size_t)e->offset;
        if ((size_t)e->offset + 24 > h.map_size) { segcache_release(&h); continue; }
        uint16_t klen_be = (uint16_t)base[16] | ((uint16_t)base[17] << 8);
        uint32_t vlen_be = (uint32_t)base[20] | ((uint32_t)base[21] << 8)
                         | ((uint32_t)base[22] << 16) | ((uint32_t)base[23] << 24);
        if ((size_t)e->offset + 24 + klen_be + vlen_be > h.map_size) {
            segcache_release(&h); continue;
        }
        const uint8_t *key_p = base + 24;
        const uint8_t *val_p = base + 24 + klen_be;
        int crc = cb((uint32_t)s, e->hash, key_p, klen_be, val_p, vlen_be, ctx);
        segcache_release(&h);
        (void)vbuf; (void)vlen;
        if (crc != 0) { rc = crc; break; }
    }

    kfcache_release(&kh);
    return rc;
}
```

with:

```c
int slotcask_walk_one_shard_slots_locked(SlotcaskDb *db, int kf_shard_id,
                                          const SlotcaskKfHandle *kh,
                                          SlotcaskScanSlotCb cb, void *ctx) {
    if (!db || !kh || !kh->map || !cb ||
        kf_shard_id < 0 || kf_shard_id >= db->num_shards)
        return -1;

    int rc = 0;
    for (size_t s = 0; s < kh->capacity; s++) {
        SlotcaskKfEntry *e = &kh->map[s];
        if (e->flag != 1) continue;
        /* read_record_value verifies the key matches; for reindex we
           trust the kf entry's pointer (kf is authoritative), so pass
           the kf entry's known-good record header. We need the key to
           call read_record_value, so we read the seg's key-prefix
           first via a small inline buffer. */
        uint8_t *vbuf = NULL;
        size_t   vlen = 0;
        /* The seg record header is 24B: 16B hash + 2B klen + 1B flag +
           1B reserved + 4B vlen. The key starts at offset+24. We read
           klen first, then call read_record_value with the in-seg key
           bytes. To avoid that two-step, lean on segcache_acquire +
           direct mmap read for simplicity. */
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, e->stream_id, e->file_id);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) continue;
        const uint8_t *base = (const uint8_t *)h.map + (size_t)e->offset;
        if ((size_t)e->offset + 24 > h.map_size) { segcache_release(&h); continue; }
        uint16_t klen_be = (uint16_t)base[16] | ((uint16_t)base[17] << 8);
        uint32_t vlen_be = (uint32_t)base[20] | ((uint32_t)base[21] << 8)
                         | ((uint32_t)base[22] << 16) | ((uint32_t)base[23] << 24);
        if ((size_t)e->offset + 24 + klen_be + vlen_be > h.map_size) {
            segcache_release(&h); continue;
        }
        const uint8_t *key_p = base + 24;
        const uint8_t *val_p = base + 24 + klen_be;
        int crc = cb((uint32_t)s, e->hash, key_p, klen_be, val_p, vlen_be, ctx);
        segcache_release(&h);
        (void)vbuf; (void)vlen;
        if (crc != 0) { rc = crc; break; }
    }

    return rc;
}
```

### 5.3 Acquire kfcache before bitmap in the rebuild worker

In `src/db/index.c`, find:

```c
/* Bitmap reindex pass — rebuilds every .bm shard for one field by
   walking live records in their kf shards via slotcask_walk_one_shard_slots,
   encoding the field value (matching the encoding bitmap_update uses on
   the CRUD path), and bm_set'ing the bit at the record's kf slot. */
```

Replace it with:

```c
/* Bitmap reindex pass — rebuilds every .bm shard for one field by
   walking live records in its already-locked kf shard via
   slotcask_walk_one_shard_slots_locked, encoding the field value
   (matching the encoding bitmap_update uses on the CRUD path), and
   bm_set'ing the bit at the record's kf slot. */
```

Then find:

```c
/* Per-kf-shard worker for parallel bitmap rebuild. Each worker handles
   one (kf_shard, .bm) pair — opens its own .bm writer, walks the
   matching kf shard, and bm_sets per record. Files don't overlap, so
   no locking; mmap absorbs the writes directly. */
```

Replace it with:

```c
/* Per-kf-shard worker for parallel bitmap rebuild. Each worker handles
   one (kf_shard, .bm) pair: acquire the kf reader first, then open its
   .bm writer and walk the already-locked kf shard. Distinct workers use
   distinct files; the cache-entry locks protect mapping lifetime. */
```

In `src/db/index.c`, find:

```c
static void *bm_shard_walk_worker(void *arg) {
    BmShardWalkArg *a = (BmShardWalkArg *)arg;
    BitmapShard *bm = bm_open(a->path, a->slots_per_shard, 0, 0, 0,
                              1 /* writer: reindex bm_set's */);
    if (!bm) {
        LOG_ERROR(LOG_SUB_BITMAP, "bm_shard_walk_worker: bm_open failed for %s (kf_shard=%d); bitmap left stale for this shard", a->path, a->kf_shard);
        return NULL;
    }
    BmRebuildCtx c = { bm, a->fi, a->ts };
    slotcask_walk_one_shard_slots(a->sdb, a->kf_shard, bm_rebuild_cb, &c);
    bm_close(bm);
    return NULL;
}
```

Replace it with:

```c
static void *bm_shard_walk_worker(void *arg) {
    BmShardWalkArg *a = (BmShardWalkArg *)arg;

    char kf_path[PATH_MAX];
    slotcask_kf_path(kf_path, sizeof(kf_path),
                     a->sdb->data_dir, a->kf_shard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path,
                        a->sdb->slots_per_shard, 0) != 0) {
        LOG_ERROR(LOG_SUB_BITMAP,
                  "bm_shard_walk_worker: kfcache_acquire failed for %s",
                  kf_path);
        return NULL;
    }

    BitmapShard *bm = bm_open(a->path, a->slots_per_shard, 0, 0, 0,
                              1 /* writer: reindex bm_set's */);
    if (!bm) {
        LOG_ERROR(LOG_SUB_BITMAP, "bm_shard_walk_worker: bm_open failed for %s (kf_shard=%d); bitmap left stale for this shard", a->path, a->kf_shard);
        kfcache_release(&kh);
        return NULL;
    }

    BmRebuildCtx c = { bm, a->fi, a->ts };
    slotcask_walk_one_shard_slots_locked(
        a->sdb, a->kf_shard, &kh, bm_rebuild_cb, &c);

    bm_close(bm);
    kfcache_release(&kh);
    return NULL;
}
```

Run the focused rebuild test to green. Then temporarily restore all three old
Task 5 blocks together, rerun to the expected red report, restore all three
new blocks, and rerun to green. Paste all evidence.

### 5.4 Update the second consumer: `cmd_estimate_index`'s trigram sampler

This call site has no bitmap interaction and predates this plan's TSan
finding, so no new focused test is required for it: the rename itself is a
compile-time break, and the existing `estimate-index` test coverage already
exercises this loop for functional correctness. Satisfy the new required
parameter with a local per-shard kfcache acquisition.

In `src/db/query_maint.c`, find this exact anchor:

```c
    /* Walk shards in order, accumulating up to TG_ESTIMATE_SAMPLE
       records. Stops early once the cap is hit. */
    TgEstimateCtx c = {
        .field_index = fi, .ts = ts,
        .sampled = 0, .distinct_sum = 0, .max_sample = TG_ESTIMATE_SAMPLE,
    };
    for (int s = 0; s < sch.splits && c.sampled < c.max_sample; s++) {
        slotcask_walk_one_shard_slots(sdb, s, tg_estimate_cb, &c);
    }
```

Replace it with:

```c
    /* Walk shards in order, accumulating up to TG_ESTIMATE_SAMPLE
       records. Stops early once the cap is hit. Each shard's kf handle is
       acquired locally: this sampler never touches a bitmap, so there is
       no cross-cache order to establish, only the walker's now-required
       pre-acquired handle. */
    TgEstimateCtx c = {
        .field_index = fi, .ts = ts,
        .sampled = 0, .distinct_sum = 0, .max_sample = TG_ESTIMATE_SAMPLE,
    };
    for (int s = 0; s < sch.splits && c.sampled < c.max_sample; s++) {
        char kf_path[PATH_MAX];
        slotcask_kf_path(kf_path, sizeof(kf_path), sdb->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, sdb->slots_per_shard, 0) != 0)
            continue;
        slotcask_walk_one_shard_slots_locked(sdb, s, &kh, tg_estimate_cb, &c);
        kfcache_release(&kh);
    }
```

Rebuild in the normal (non-TSan) mode and confirm `cmd_estimate_index`'s
existing `estimate-index` test cases still pass unchanged — this is a
mechanical signature fix, not a behavior change, so no assertions should need
updating.

## Task 6 — document the enforced hierarchy

This task changes documentation only after all behavioral slices are green.

In `docs/concepts/concurrency.md`, find:

```markdown
## Lock hierarchy (bottom up)

| Scope | Lock type | Purpose |
|---|---|---|
| Per kfcache entry (one kf shard mmap) | rwlock | Readers share; a writer takes exclusive. Commits go through here. |
| Per segcache entry (one seg file mmap) | rwlock | Routine record writes take rdlock (each owns a unique offset); eviction/recovery takes wrlock. |
| Per bt_cache entry (one btree mmap) | rwlock | Same model, separate cache. One entry per per-shard idx file. |
| Per stream (one append lane) | mutex + try-lock pool | Tail reservation serialised per stream; free-pool consumers use try-lock. |
| Per object (logical) | rwlock ("objlock") | Normal ops take read; schema mutations take write. |
| Global maps (schemas, indexes, dirs, slotcask registry) | mutex | Short-held, protects cache-lookup structures. |
| Process wide | atomic counters | `in_flight_writes`, `active_threads`, `server_running` — no locks, just atomics. |

## Per-kfcache-entry rwlock — the commit lock
```

Replace it with:

```markdown
## Lock hierarchy (bottom up)

| Scope | Lock type | Purpose |
|---|---|---|
| Per kfcache entry (one kf shard mmap) | rwlock | Readers share; a writer takes exclusive. Commits go through here. |
| Per bitmap-cache entry (one bitmap shard mmap) | rwlock | Readers share; bitmap mutation and rebuild take exclusive. When both kfcache and bitmap are needed, kfcache is acquired first. |
| Per segcache entry (one seg file mmap) | rwlock | Routine record writes take rdlock (each owns a unique offset); eviction/recovery takes wrlock. |
| Per bt_cache entry (one btree mmap) | rwlock | Same model, separate cache. One entry per per-shard idx file. |
| Per stream (one append lane) | mutex + try-lock pool | Tail reservation serialised per stream; free-pool consumers use try-lock. |
| Per object (logical) | rwlock ("objlock") | Normal ops take read; schema mutations take write. |
| Global maps (schemas, indexes, dirs, slotcask registry) | mutex | Short-held, protects cache-lookup structures. |
| Process wide | atomic counters | `in_flight_writes`, `active_threads`, `server_running` — no locks, just atomics. |

### Cross-cache rule: kfcache before bitmap cache

Bitmap bit positions are kf slot numbers, so any operation that resolves
bitmap bits through the corresponding kf shard may need both cache entries at
once. Those paths always acquire the kfcache entry first and the bitmap-cache
entry second. That acquisition order is mandatory; release operations do not
block and therefore do not add lock-order edges. CRUD already has this shape:
the kfcache write lock is the commit lock and bitmap maintenance runs from the
pre-commit hook while it is held. Query emission, KeySet construction, and
bitmap rebuild use the same acquisition order.

Opening the bitmap first is forbidden even for read/read nesting. A concurrent
CRUD writer can hold the kfcache wrlock while waiting for the bitmap wrlock;
a reader holding the bitmap rdlock while waiting for the kfcache rdlock would
complete an AB-BA deadlock.

## Per-kfcache-entry rwlock — the commit lock
```

## Task 7 — final verification and uncommitted handoff

### 7.1 Static audit

Run:

```bash
rtk rg -n "slotcask_walk_one_shard_slots\\(" src
rtk rg -n "slotcask_walk_one_shard_slots_locked\\(" src
rtk rg -n "deadlock:bm_open" .tsan.supp
rtk rg -n "bm_open|kfcache_acquire" \
  src/db/index.c src/db/query.c src/db/query_aggregate.c \
  src/db/query_bulk.c src/db/query_maint.c src/db/query_plan.c \
  src/db/query_schema.c src/db/slotcask.c
```

Expected:

- The old `slotcask_walk_one_shard_slots(` search has no match.
- The locked-name search returns exactly the declaration, the definition,
  the `index.c` caller, and the `query_maint.c` caller.
- `deadlock:bm_open` has no match.
- Manual inspection of every function/call path in the final search confirms
  no bitmap handle is held across a later kfcache acquisition.

### 7.2 Fresh normal build and full suite

```bash
SKIP_TESTS=1 rtk ./build.sh
rtk ./build/bin/shard-db-test run-all
```

Require zero build warnings introduced by this work and a fully passing fresh
suite.

### 7.3 ASan+UBSan focused and full suite

```bash
BUILD_MODE=asan SKIP_TESTS=1 rtk ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  rtk ./build/bin/shard-db-test run-all --filter bitmap-kfcache-lock-order --jobs 2
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  rtk ./build/bin/shard-db-test run-all --jobs 2
```

Require all five focused cases and the full suite to pass with no sanitizer
finding.

### 7.4 TSan focused and full suite

Use fresh report directories:

```bash
bitmap_lock_tsan_focused_dir="$(rtk mktemp -d /tmp/shard-db-bitmap-lock-tsan-focused.XXXXXX)"
BUILD_MODE=tsan SKIP_TESTS=1 rtk ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:report_signal_unsafe=0:report_atomic_races=0:suppressions=$PWD/.tsan.supp:log_path=$bitmap_lock_tsan_focused_dir/tsan" \
  rtk ./build/bin/shard-db-test run-all --filter bitmap-kfcache-lock-order --jobs 1
if rtk rg -n "WARNING: ThreadSanitizer|lock-order-inversion|data race|deadlock" \
  "$bitmap_lock_tsan_focused_dir"; then
  exit 1
fi

bitmap_lock_tsan_full_dir="$(rtk mktemp -d /tmp/shard-db-bitmap-lock-tsan-full.XXXXXX)"
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:report_signal_unsafe=0:report_atomic_races=0:suppressions=$PWD/.tsan.supp:log_path=$bitmap_lock_tsan_full_dir/tsan" \
  rtk ./build/bin/shard-db-test run-all --jobs 1
if rtk rg -n "WARNING: ThreadSanitizer|lock-order-inversion|data race|deadlock" \
  "$bitmap_lock_tsan_full_dir"; then
  exit 1
fi
```

Require all focused tests and the complete sequential suite to pass, with no
unsuppressed TSan report. Do not add or broaden any suppression.

### 7.5 Definition-of-done inspection

Run:

```bash
rtk git diff --check
rtk git status --short
rtk git diff -- \
  .tsan.supp build.sh docs/concepts/concurrency.md \
  src/db/query.c src/db/index.c src/db/query_maint.c \
  src/db/slotcask.c src/db/slotcask.h
rtk git diff --no-index /dev/null \
  src/test/cases/test_bitmap_kfcache_lock_order.c
rtk rg -n '[[:blank:]]+$' \
  src/test/cases/test_bitmap_kfcache_lock_order.c
rtk rg -n "TODO|FIXME|printf\\(|fprintf\\(stderr" \
  src/db/query.c src/db/index.c src/db/query_maint.c \
  src/db/slotcask.c src/db/slotcask.h \
  src/test/cases/test_bitmap_kfcache_lock_order.c
```

Inspect the output and confirm:

- The `git diff --no-index` command exits 1 because the new file differs from
  `/dev/null`; that status is expected. Its output shows the complete untracked
  test file for review.
- The trailing-whitespace search has no match.
- The diff contains only the planned files and hunks.
- Unrelated pre-existing untracked plans remain untouched.
- No new TODO, FIXME, debug print, commented-out code, warning, dependency,
  protocol change, or on-disk change was introduced.
- The new `LOG_ERROR` is intentional production diagnostics, not a debug
  print.
- Every handle acquired by a changed path is released exactly once on every
  exit.
- The work is still unstaged and uncommitted.

### 7.6 Review handoff

Stop execution and hand the raw uncommitted `git diff` to a reviewer who has
not seen this plan. Because the diff changes concurrency behavior, that
plan-blind review is mandatory. The reviewer must independently inspect lock
ordering, failure cleanup, rebuild safety, test sensitivity, scope, and
sanitizer evidence before the human authorizes any commit.
