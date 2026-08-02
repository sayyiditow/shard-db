# Plan A — atomic index publication

**Status: COMPLETE — landed in main via commit 846e78d (PR #277,
2026-08-02), including the corrective round in
2026-08-01-index-atomic-publication-review-fixes.md.**

**Revised 2026-07-31:** result propagation now includes both the singular and
plural add-index paths; force/reindex cleanup is explicitly deferred until
publication completes; parent-fsync tests use a deterministic test seam; and
the publish-gate ordering covers online bulk-index rebuilds as well as
maintenance. This revision additionally makes every scan/spill failure a
pre-publication stop (rather than a status folded after replacements have been
published), and gives bitmap-cache acquires the same acquire-vs-publish
exclusion as B-tree cache acquires.

**Dependency:** none. Plan B (2026-07-31-indexed-write-atomicity.md) may be
executed only after this plan has completed and passed raw-diff review.

## Goal and binding invariants

This plan makes every rebuilt B-tree (.idx/.tg) and bitmap (.bm) shard
atomic at publication time.

1. A failed rebuild leaves the completed live target intact. It may leave only
   a generated temporary sibling.
2. After rename succeeds, the new complete target is live. A later
   parent-directory fsync failure is a distinct durability-warning state, not
   a failed build or an excuse to delete the target.
3. A B-tree acquire that begins after publication completes cannot obtain an
   old inode through the pathname cache. An acquire that began beforehand may
   finish on its old mapping.
4. An empty rebuild publishes a valid empty shard, never stale old contents.
5. Obsolete legacy/high-shard files are removed only after every requested
   replacement was renamed.
6. Continuous readers must not starve publication.

**Scope and lock ordering of the new publish gate (`g_bt_publish_gate`,
Task 2):** this is one process-lifetime global lock, not scoped to a
path/shard, unlike every other lock in this codebase's B-tree/kf/object
layers (all per-resource). Every `bt_acquire()` for every object, field, and
shard takes it as reader; every publish takes it as writer for the full
`btree_cache_invalidate(target)` + `rename()` + `fsync_parent_dir()`
critical section — including the parent-directory fsync, which is a
blocking disk-flush syscall. Consequence: a slow parent fsync, or a reindex
publishing many shards back-to-back, briefly stalls unrelated B-tree reads
server-wide, not just reads to the shard being published. This is the
accepted cost of invariant 3 (acquire-vs-rename exclusion) without adding
per-path lock plumbing; it is not free, and should be called out to
whoever approves this plan as a deliberate tradeoff, not a hidden one.

Lock ordering relative to existing locks (verified again before coding, and
required after this change): `g_bt_publish_gate` is acquired before either
`bt_cache_lock` or a cached-file rwlock in every newly introduced path.
`bt_acquire` takes gate-reader → existing cache machinery; publication takes
gate-writer → invalidate temporary cache entry → invalidate target cache
entry → rename → parent fsync. It must never invalidate either path before
taking the writer gate.

`bt_publish_replace_locked` is reached both from maintenance (which holds the
object rwlock exclusively) and from online bulk indexing through
`idx_build_field_worker` → `btree_bulk_merge` (which may hold only the normal
shared object lock). The publication helper must not acquire an object lock,
and no introduced code may acquire an object lock after taking the gate. Add
a bounded regression that runs an online bulk merge concurrently with a
reindex publish and proves both complete; this validates the actual lock
graph instead of relying on the maintenance-only assumption. `bt_acquire`
releases the gate before returning its handle and `bt_release` never touches
it, so no helper may call the publisher while retaining a gate-reader lock.

## Fixed result contract

At the unique anchor ~static int merge_spills_into_index(~ in src/db/index.c,
introduce this private result type and use it for B-tree, trigram, and bitmap
force-build aggregation:

~~~c
typedef enum {
    INDEX_BUILD_OK = 0,
    INDEX_BUILD_DURABILITY_UNCONFIRMED,
    INDEX_BUILD_FAILED,
} IndexBuildStatus;

typedef struct {
    IndexBuildStatus status;
    int all_requested_shards_published;
} IndexBuildResult;
~~~

A pre-rename error produces INDEX_BUILD_FAILED and zero. If every target was
renamed but one or more parent fsync calls fails, produce
INDEX_BUILD_DURABILITY_UNCONFIRMED and one. Online callers treat either
non-OK state as nonzero; maintenance uses the second field only for safe
obsolete-file cleanup.

### Call sites this type change touches

`merge_spills_into_index` is not a leaf function — it is called from a
per-shard parallel worker, whose result is folded across shards and then
across fields before reaching the two public entry points that decide
whether obsolete files may be removed. Every hop in that chain changes type
from `int` to `IndexBuildResult`; each is named explicitly so no hop is left
for the executor to improvise:

1. `static int merge_spills_into_index(` in src/db/index.c becomes
   `static IndexBuildResult merge_spills_into_index(` — same parameters,
   returns the new struct per the rule above instead of 0/-1.
2. `MergeShardArg.rc` (the struct immediately following
   `merge_spills_into_index`, consumed by `merge_shard_worker_fn`) changes
   from `int rc;` to `IndexBuildResult rc;`. `merge_shard_worker_fn`'s body
   (`m->rc = merge_spills_into_index(...)`) needs no other change.
3. The per-field fold at the `parallel_for_io(merge_shard_worker_fn, ...)`
   call site in `build_indexes_streaming_multi`, currently
   `for (int s = 0; s < idx_n; s++) if (margs[s].rc != 0) merge_rc = -1;`,
   becomes a proper fold instead of a boolean OR of failure:
   ~~~c
   IndexBuildResult field_result = { INDEX_BUILD_OK, 1 };
   for (int s = 0; s < idx_n; s++) {
       if (margs[s].rc.status > field_result.status)
           field_result.status = margs[s].rc.status;
       if (!margs[s].rc.all_requested_shards_published)
           field_result.all_requested_shards_published = 0;
   }
   ~~~
   (status ordering relies on the enum's declaration order — OK <
   DURABILITY_UNCONFIRMED < FAILED — so `>` picks the worst of the two.)
   Fold `field_result` into the object-level result the same way, across
   fields, in place of the current `merge_rc = -1`.
4. `static int resolve_bitmaps(` in src/db/index.c becomes
   `static IndexBuildResult resolve_bitmaps(`, folding its own per-shard
   results (Task 3 adds the per-shard rc/failed fields to `BmShardWalkArg`
   this reads) the same way as step 3.
5. `static int build_indexes_streaming_multi(` — both the forward
   declaration and the definition — becomes
   `static IndexBuildResult build_indexes_streaming_multi(`. Its final fold
   (currently `return (any_error || merge_rc != 0 || bm_rc != 0) ? -1 : 0;`)
   becomes: `any_error` (segment-scan phase failures) is an **early return**:
   before allocating `MergeShardArg`, opening a stream builder, opening a
   bitmap temp, or calling `resolve_bitmaps`, remove the current invocation's
   spill files/directories and return `{ INDEX_BUILD_FAILED, 0 }`. It must not
   merely be folded into the final result after Phase 2 has published a
   replacement. Otherwise fold the field-loop result from step 3 with the
   `resolve_bitmaps` result from step 4 using the same
   worst-status/AND-published rule.
6. `static int seg_seq_build_spills(` becomes `static IndexBuildResult
   seg_seq_build_spills(` so the B-tree/trigram merge result cannot be
   collapsed to `int` before either public path observes it. All setup or
   scan errors return `{ INDEX_BUILD_FAILED, 0 }`; an object with no segments
   still invokes the empty-shard publication phase and returns its folded
   result. Replace its early empty-object return and its worker-cap logic with
   this exact block so Phase 2 runs once with no spill files instead of being
   skipped:
   ~~~c
   int n_segs = 0;
   SegRef *segs = enumerate_segments(sdb->data_dir, sch->streams, &n_segs);

   int pool_size = parallel_pool_size();
   if (pool_size < 1) pool_size = 1;
   int P = pool_size;
   if (n_segs > 0 && P > n_segs) P = n_segs;
   ~~~
   With `n_segs == 0`, the one zero-work worker writes no spills; the Phase-2
   B-tree/trigram merge publishes valid empty trees, and the bitmap resolver
   publishes valid empty bitmap shards. Do not add a new early return before
   both phases complete.
7. The singular builders `build_btree_streaming`, `build_trigram_pass`, and
   `build_bitmap_pass` each become `IndexBuildResult` functions. Their only
   production caller is `cmd_add_index`; update their forward declarations
   and that call site together. Do not retain a lossy `int` wrapper.
8. Both external call sites of `build_indexes_streaming_multi` update to
   consume the struct:
   - In `cmd_add_indexes`, the call currently discards its return value
     entirely (`build_indexes_streaming_multi(db_root, object, &sch,
     ts_for_idx, descs, n_desc);` with no assignment). Capture it; this is
     the value the later "write/sync index.conf only after a non-failed
     build" change (Task 3) gates on.
   - In `reindex_object_checked`, `int build_rc = 0; ... build_rc =
     build_indexes_streaming_multi(...); ... if (build_rc != 0) { ...
     return -1; }` becomes `IndexBuildResult build_result = { INDEX_BUILD_OK,
     1 }; ... if (build_result.status == INDEX_BUILD_FAILED) { ... return
     -1; }`, and `build_result.all_requested_shards_published` is what gates
     the obsolete-file cleanup this task moves out of the upfront wipe (see
     the reindex_object_checked change below).
9. `cmd_add_index` captures the singular result from step 7. Both singular
   and plural commands write and sync `index.conf` only when
   `status != INDEX_BUILD_FAILED`; an `INDEX_BUILD_DURABILITY_UNCONFIRMED`
   result writes metadata, emits an explicit durability-warning response,
   and returns nonzero. A failed result leaves existing metadata untouched.

## Task 1 — fail closed before B-tree rebuild

### Test first

In src/test/cases/test_coverity_disk_corruption_btree.c, immediately before
the unique anchor
~TEST_REGISTER("test-coverity-btree-nextleaf-cycle", test_coverity_btree_nextleaf_cycle_run);~,
add two tests. Seed a valid target, retain its inode and complete range result,
then corrupt (a) root/page bounds and (b) the forward leaf chain. Each
btree_bulk_merge call must return nonzero, retain the original inode and range
result, and not add the new entry.

**Known pre-fix symptom, capture accordingly:** `bt_extract_all`'s root-to-
leftmost-leaf descent loop (the `while (1) { ... if (ph->page_type == 1)
break; page_id = ph->next_leaf; }` loop, distinct from the leaf-chain scan
loop below it) has no hop-count bound today, unlike every other page-chasing
loop in this file (`iter_init_desc_leaves`, `btree_walk_all_values` —
CID 1696448 / CID 1696465). A corrupted `root_page` can drive that loop into
a page whose reinterpreted bytes keep `page_type != 1` and whose `next_leaf`-
aliased bytes chase back into mapped file content indefinitely — this hangs,
it does not return an error. The root-corruption case above will therefore
hang on the unfixed base implementation; do not run it un-timed. Capture the
red base-branch output with a wall-clock bound the harness itself doesn't
provide, e.g. `timeout 30 ./build/bin/shard-db-test run
test-coverity-btree-bulk-merge-bad-root` — a `timeout`-killed run (exit 124)
*is* the expected red-run evidence for this case, alongside the leaf-chain
case's ordinary (non-hanging) assertion failure. Paste both.

**This is not a halt condition.** A `timeout`-killed (exit 124) run of
`test-coverity-btree-bulk-merge-bad-root` against the current, unmodified
tree is the correct and only expected outcome of this step — it is the
regression test proving the bug exists, exactly as CORE-PROCESS.md's
test-first discipline requires, and it confirms the root cause diagnosed
above (no hop bound in `bt_extract_all`'s descent loop). Once you have this
output captured, proceed immediately to the **Change** section below and
implement the hop-bounded `bt_extract_all` there — do not stop and wait
after observing the timeout. Re-run the same bounded command
(`timeout 30 ./build/bin/shard-db-test run
test-coverity-btree-bulk-merge-bad-root`) after the fix: it must now exit 0
well under the 30s bound (no hang, `btree_bulk_merge` returns nonzero). That
green, fast run — not the earlier timeout — is what confirms the fix and is
the only outcome that should be reported as this task's regression-test
result.

### Change

At the unique anchors ~static BtEntry *bt_extract_all(~ and
~int btree_bulk_merge(~ in src/db/btree.c, use this exact extraction contract:

~~~c
/* NULL with out_failed == 0 means a valid empty or absent (ENOENT) target.
 * NULL with out_failed == 1 is open, format, traversal, allocation, or
 * entry-count failure; callers must not rebuild the target. */
static BtEntry *bt_extract_all(const char *path, size_t *out_count,
                               int *out_failed);
~~~

Initialize both out parameters. A failed bt_acquire is absent only for ENOENT.
Before allocating, reject a header whose `page_count`, `root_page`, or
`entry_count` cannot fit the mapped file or whose entry count would overflow
either `entry_count + 64` or `cap * sizeof(BtEntry)`. Validate magic, map size,
root page, every traversed page type and bounds, each decoded entry, and final
count == `fh->entry_count`. On error free every entry/value, preserve errno,
set `out_failed`, and return NULL.

**Root cause the regression test in "Test first" above exposes:** the
existing root-to-leftmost-leaf descent loop (`while (1) { if (... >
bt.map_size) break; ... if (ph->page_type == 1) break; page_id =
ph->next_leaf; }`) and the leaf-chain scan loop that follows it both walk
on-disk `next_leaf` pointers with only a `map_size` bound and no hop-count
bound. Every other page-chasing loop in this file already learned this
lesson — `iter_init_desc_leaves` and `btree_walk_all_values` both cap total
hops at `fh->page_count` and cite CID 1696448 / CID 1696465 for it — but
`bt_extract_all` was never brought in line, which is exactly why a corrupted
`root_page` hangs it instead of failing it. Replace the unique anchor text
(the two loops immediately following `size_t cap = (size_t)fh->entry_count +
64;` and its `malloc`) with the hop-bounded form, matching
`btree_walk_all_values`'s established pattern:

~~~c
    /* Walk down to leftmost leaf via next_leaf (= leftmost child for internal).
       Bounded exactly as btree_walk_all_values (CID 1696448 / CID 1696465):
       page_id comes from an on-disk pointer that may be corrupted, so cap
       total hops at page_count instead of trusting page_type to eventually
       be 1. */
    uint32_t page_id = fh->root_page;
    uint32_t extract_page_count = fh->page_count;
    uint32_t descend_hops = 0;
    while (1) {
        if (page_id >= extract_page_count || ++descend_hops > extract_page_count)
            goto extract_failed;
        if ((size_t)page_id * bt_page_size + bt_page_size > bt.map_size)
            goto extract_failed;
        uint8_t *pg = bt.map + (size_t)page_id * bt_page_size;
        BtPageHeader *ph = (BtPageHeader *)pg;
        if (ph->page_type == 1) break;
        page_id = ph->next_leaf;
    }

    /* Scan leaf chain — sequential decode via LeafIter. Same hop bound as
       above: a corrupted next_leaf can point at another in-bounds, valid
       page_type==1 leaf and cycle forever without this. */
    uint32_t chain_hops = 0;
    while (page_id != 0) {
        if (page_id >= extract_page_count || ++chain_hops > extract_page_count)
            goto extract_failed;
        if ((size_t)page_id * bt_page_size + bt_page_size > bt.map_size)
            goto extract_failed;
        uint8_t *pg = bt.map + (size_t)page_id * bt_page_size;
        BtPageHeader *ph = (BtPageHeader *)pg;
        if (ph->page_type != 1) goto extract_failed;
~~~

`extract_failed:` is a new label at the existing `extract_done:` cleanup site
(rename neither — add `extract_failed` immediately before `extract_done`,
setting `*out_failed = 1` and freeing every entry/value already decoded
before falling through to the same `bt_release`/return NULL as the other
failure paths). Every other `break`/`goto extract_done` already present in
the leaf-decode body (malloc failures, etc.) is unchanged by this edit —
only the two traversal loops' termination conditions change from "silently
stop, return whatever was collected" to "reject the whole extraction."

Replace (do not retain) the unique call-site text
~existing = bt_extract_all(path, &exist_count);~ with:

~~~c
int extract_failed = 0;
existing = bt_extract_all(path, &exist_count, &extract_failed);
if (extract_failed) {
    rc = -1;
    goto done;
}
~~~

Only valid empty/absent extraction may enter the replacement builder.

## Task 2 — atomic B-tree and trigram publication

### Test first

Immediately before the unique anchor
~TEST_REGISTER("test-btree", test_btree_run)~ in src/test/cases/test_btree.c,
add direct tests for (1) a temporary build failure, (2) post-rename
parent-fsync failure, and (3) an empty rebuild. They prove, respectively: old
target survives; new target is readable but result is a durability warning;
and old entries are replaced by a valid empty tree. test_btree.c calls
btree.c functions directly in-process (no ShardDb/g_db, no daemon spawn), so
these tests use the process-global, g_db-independent `durability_fsync`
fault seam defined below (not `durability_test_pause`, which is g_db-gated).
Use `durability_test_fsync_fail_on_call(1, EIO)` for (1) and call 2 for (2),
with `durability_test_fsync_reset()` called both immediately before arming
each case and again after its assertions, so no pending injected failure or
stale call count leaks into a later case sharing the same process under
`--jobs 1`; do not use permissions or a full disk to simulate either error.

Immediately before the single final registration anchor
~TEST_REGISTER("test-bt-cache-writer-starvation", test_bt_cache_writer_starvation_glibc_run)~
in src/test/cases/test_bt_cache_writer_starvation.c, add a glibc-only,
bounded-child test. It starts a publish pause, waits for a reader-pending test
counter, releases publication, and proves the reader sees the replacement. A
continuous-reader variant proves a queued publisher enters the gate. In the
same bounded child, run `btree_bulk_merge` on a second target while an
add-index/reindex publisher is paused; both must finish after release. This is
the regression for the online-bulk/maintenance lock graph described above.

Immediately before
~TEST_REGISTER("test-reindex-spill-collision", test_reindex_spill_collision_run);~,
add a daemon reindex test that pauses at bt-publish-before-rename, SIGKILLs,
restarts, and proves the previous target remains queryable.

### Change

At the unique initialization anchor ~void bt_cache_init(int cap)~ in
src/db/btree.c, initialize the process-lifetime global gate with the existing
writer-preferring helper, not PTHREAD_RWLOCK_INITIALIZER:

~~~c
static pthread_rwlock_t g_bt_publish_gate;
static pthread_once_t g_bt_publish_gate_once = PTHREAD_ONCE_INIT;

static void bt_publish_gate_init(void) {
    rwlock_init_writer_preferring(&g_bt_publish_gate);
}
~~~

Every acquire/publisher calls pthread_once before locking. Do not destroy this
process-lifetime gate from btree_cache_shutdown: pthread_once cannot safely
re-run it for a later test/database lifecycle. Rename the current
body of ~static int bt_acquire(~ to bt_acquire_impl and use this complete
wrapper:

~~~c
static int bt_acquire(BtFile *bt, const char *path, int writer) {
    int rc, gate_rc;
    pthread_once(&g_bt_publish_gate_once, bt_publish_gate_init);
#ifdef TEST_BUILD
    bt_publish_reader_pending_begin();
#endif
    gate_rc = pthread_rwlock_rdlock(&g_bt_publish_gate);
#ifdef TEST_BUILD
    bt_publish_reader_pending_end();
#endif
    if (gate_rc != 0) { errno = gate_rc; return -1; }
    rc = bt_acquire_impl(bt, path, writer);
    pthread_rwlock_unlock(&g_bt_publish_gate);
    return rc;
}
~~~

Immediately after the existing `#ifdef TEST_BUILD` block that defines
`btree_test_reader_pending_count` (the one guarding
`g_bt_test_reader_pending_count`/`g_bt_test_reader_pending_lock`), add a
second, distinct counter for the new gate using the same mutex-protected-int
pattern already used by every other counter in this block — this file
has no existing `<stdatomic.h>` usage and none should be introduced:

~~~c
#ifdef TEST_BUILD
static pthread_mutex_t g_bt_publish_reader_pending_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_bt_publish_reader_pending_count;

static void bt_publish_reader_pending_begin(void) {
    pthread_mutex_lock(&g_bt_publish_reader_pending_lock);
    g_bt_publish_reader_pending_count++;
    pthread_mutex_unlock(&g_bt_publish_reader_pending_lock);
}

static void bt_publish_reader_pending_end(void) {
    pthread_mutex_lock(&g_bt_publish_reader_pending_lock);
    g_bt_publish_reader_pending_count--;
    pthread_mutex_unlock(&g_bt_publish_reader_pending_lock);
}

int btree_test_publish_reader_pending_count(void) {
    pthread_mutex_lock(&g_bt_publish_reader_pending_lock);
    int n = g_bt_publish_reader_pending_count;
    pthread_mutex_unlock(&g_bt_publish_reader_pending_lock);
    return n;
}
#endif
~~~

Declare `int btree_test_publish_reader_pending_count(void);` beside the
existing `int btree_test_reader_pending_count(void);` in src/db/btree.h. This
is a distinct counter from the pre-existing pair (`btree_test_writer_pending_count`/
`btree_test_reader_pending_count`), which instrument contention on the
per-cache-slot rwlock inside `bt_acquire_impl` — not this new global gate;
the writer-starvation test added below must call
`btree_test_publish_reader_pending_count()`, not the pre-existing one, or it
will observe the wrong lock. The gate's reader coverage spans cache hit, cold
open, cache install, and uncached fallback (everything inside
`bt_acquire_impl`); `bt_release` does not retain it.

Immediately before ~static int btree_bulk_build_locked(~, add:

~~~c
static BtPublishResult bt_publish_replace_locked(const char *target,
                                                  const char *tmp_path) {
    int saved_errno, gate_rc;
    char parent[PATH_MAX];
    if (parent_dir_copy(target, parent, sizeof(parent)) != 0 ||
        fsync_file_path(tmp_path) != 0)
        return BT_PUBLISH_PRE_RENAME_FAILED;
    pthread_once(&g_bt_publish_gate_once, bt_publish_gate_init);
    gate_rc = pthread_rwlock_wrlock(&g_bt_publish_gate);
    if (gate_rc != 0) { errno = gate_rc; return BT_PUBLISH_PRE_RENAME_FAILED; }
    btree_cache_invalidate(tmp_path);
    btree_cache_invalidate(target);
    durability_test_pause(parent, "bt-publish-before-rename");
    if (rename(tmp_path, target) != 0) {
        saved_errno = errno;
        pthread_rwlock_unlock(&g_bt_publish_gate);
        errno = saved_errno;
        return BT_PUBLISH_PRE_RENAME_FAILED;
    }
    if (fsync_parent_dir(target) != 0) {
        saved_errno = errno;
        pthread_rwlock_unlock(&g_bt_publish_gate);
        errno = saved_errno;
        return BT_PUBLISH_POST_RENAME_FSYNC_FAILED;
    }
    pthread_rwlock_unlock(&g_bt_publish_gate);
    return BT_PUBLISH_OK;
}
~~~

Immediately before ~static int btree_bulk_build_locked(~, add the following
temporary-name helper. Every rebuild uses it; no caller may derive a temporary
name from a PID, timestamp, or a non-exclusive `open`.

~~~c
static int bt_rebuild_temp_path(const char *target, char out[PATH_MAX]) {
    char parent[PATH_MAX];
    if (parent_dir_copy(target, parent, sizeof(parent)) != 0) return -1;
    int n = snprintf(out, PATH_MAX, "%s/.rebuild-XXXXXX", parent);
    if (n < 0 || n >= PATH_MAX) { errno = ENAMETOOLONG; return -1; }
    int fd = mkstemp(out);
    if (fd < 0) return -1;
    if (close(fd) != 0) { int e = errno; unlink(out); errno = e; return -1; }
    return 0;
}
~~~

Refactor the existing raw builder at ~static int btree_bulk_build_locked(~ and
the stream builder at ~BtStreamBuilder *bt_stream_build_open(~ so both write
only to the `mkstemp` sibling returned above. Add `target_path` and `tmp_path`
to `BtStreamBuilder`; it retains the target's mutation lock but acquires its
`BtFile` only for `tmp_path`. `bt_stream_build_finish` must first finish the
valid empty/non-empty tree, release its temp mapping, call
`bt_publish_replace_locked(target_path, tmp_path)`, then unlock the mutation
lock.

At `btree.h`, replace the consecutive declarations beginning
~typedef struct BtStreamBuilder BtStreamBuilder;~ and ending
~int  bt_stream_build_finish(BtStreamBuilder *b);~ with this public result and
complete declaration block:

~~~c
typedef enum {
    BT_PUBLISH_OK = 0,
    BT_PUBLISH_PRE_RENAME_FAILED,
    BT_PUBLISH_POST_RENAME_FSYNC_FAILED,
} BtPublishResult;

typedef struct BtStreamBuilder BtStreamBuilder;
BtStreamBuilder *bt_stream_build_open(const char *path);
int bt_stream_build_add(BtStreamBuilder *b, const char *value, size_t vlen,
                        const uint8_t hash[BT_HASH_SIZE]);
BtPublishResult bt_stream_build_finish(BtStreamBuilder *b);
~~~

Update its sole production caller, `merge_spills_into_index`, to map that
public enum to the private `IndexBuildResult` in `index.c` with this complete
helper placed immediately before ~static int merge_spills_into_index(~:

~~~c
static IndexBuildResult index_result_from_bt_publish(BtPublishResult r) {
    switch (r) {
        case BT_PUBLISH_OK:
            return (IndexBuildResult){ INDEX_BUILD_OK, 1 };
        case BT_PUBLISH_POST_RENAME_FSYNC_FAILED:
            return (IndexBuildResult){ INDEX_BUILD_DURABILITY_UNCONFIRMED, 1 };
        default:
            return (IndexBuildResult){ INDEX_BUILD_FAILED, 0 };
    }
}
~~~

Keep `btree_bulk_build` / `btree_bulk_merge` as `int` APIs for existing online
callers: map both non-OK statuses to `-1`, but log the post-rename state as a
durability warning before returning. No builder may unlink its live target.

At ~static int merge_spills_into_index(~ in src/db/index.c, remove
~if (reader_count == 0) goto cleanup;~. Always build and publish an empty
temporary B-tree, and make every `bt_stream_build_add` error fatal.  Missing
spill files remain the sole benign absence (a worker had no entries for that
output shard).  After a spill file is opened, every `fstat`, run-header
`pread`, body-boundary calculation, `spill_run_reader_init`, and
`mh_advance_top` failure is fatal; so is any nonzero trailing byte count that
cannot form a complete run header.  On such an error, dispose/unlink only the
temporary builder output and return `{ INDEX_BUILD_FAILED, 0 }` without
renaming a target.  Do not turn a malformed run into an empty shard.

At the point in `seg_seq_build_spills` immediately after the loop that derives
`any_error` from all phase-1 workers and before the `/* Phase 2a: merge ... */`
comment, add this exact publication barrier:

~~~c
if (any_error) {
    for (int fi = 0; fi < n_fields; fi++) {
        char spill_dir[PATH_MAX];
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill_%d",
                 db_root, object, descs[fi].name, fi);
        for (int w = 0; w < P; w++) {
            for (int s = 0; s < idx_n; s++) {
                char path[PATH_MAX];
                snprintf(path, sizeof(path), "%s/w%d_s%d.bin", spill_dir, w, s);
                unlink(path);
            }
            char bm_path[PATH_MAX];
            snprintf(bm_path, sizeof(bm_path), "%s/bmw%d.bin", spill_dir, w);
            unlink(bm_path);
        }
        rmdir(spill_dir);
    }
    return (IndexBuildResult){ INDEX_BUILD_FAILED, 0 };
}
~~~

This barrier is required even though the final result also carries failure:
the invariant forbids a phase-1 failure from reaching any later rename.

Immediately before the Phase-2a field loop in the same function, call
`durability_test_pause` with that field's spill directory and phase
`"idx-spills-before-merge"`.  Immediately before
~TEST_REGISTER("test-reindex-spill-collision", test_reindex_spill_collision_run);~,
add a daemon malformed-spill regression through the public reindex path: wait
for that marker, truncate one non-empty `w*_s*.bin` run in the requested
field, then continue. The request must fail and every pre-existing B-tree and
trigram target for that field must remain byte-identical. Cover a truncated run
header, a declared run body extending beyond EOF, and a truncated entry body.
It fails on the base branch because `merge_spills_into_index` treats each as an
empty or finished run and can publish the remaining subset.

Add `parent_dir_copy`, `fsync_file_path`, and `fsync_parent_dir` immediately
before ~char *dirname_of(~ in src/db/util.c and declare them beside
`dirname_of` in src/db/types.h. Both fsync helpers preserve the first errno;
the latter opens a local parent buffer with `O_DIRECTORY`. They call the new
`durability_fsync(fd)` wrapper, never `fsync(fd)` directly.

Immediately before the unique anchor ~void durability_test_pause(~ in
src/db/durability.c, insert this complete state block and wrapper. It
mirrors the existing `durability_msync` test-fault pattern at the top of
this same file (`g_durability_msync_test_lock` /
`durability_test_msync_fail_on_call`, durability.c:10-52) rather than
inventing a new shape — `durability_fsync` is the single choke point
`fsync_file_path` and `fsync_parent_dir` (added above) call instead of the
raw `fsync(2)`, so both count against the same call counter:

~~~c
#ifdef TEST_BUILD
static pthread_mutex_t g_durability_fsync_test_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_durability_fsync_call_count;
static int g_durability_fsync_fail_on_call;
static int g_durability_fsync_fail_errno;

void durability_test_fsync_reset(void) {
    pthread_mutex_lock(&g_durability_fsync_test_lock);
    g_durability_fsync_call_count = 0;
    g_durability_fsync_fail_on_call = 0;
    g_durability_fsync_fail_errno = 0;
    pthread_mutex_unlock(&g_durability_fsync_test_lock);
}

void durability_test_fsync_fail_on_call(int call_number, int err) {
    pthread_mutex_lock(&g_durability_fsync_test_lock);
    g_durability_fsync_fail_on_call = call_number > 0 ? call_number : 0;
    g_durability_fsync_fail_errno = err > 0 ? err : EIO;
    pthread_mutex_unlock(&g_durability_fsync_test_lock);
}
#endif

int durability_fsync(int fd) {
#ifdef TEST_BUILD
    pthread_mutex_lock(&g_durability_fsync_test_lock);
    g_durability_fsync_call_count++;
    if (g_durability_fsync_fail_on_call > 0 &&
        g_durability_fsync_call_count == g_durability_fsync_fail_on_call) {
        int injected_errno = g_durability_fsync_fail_errno;
        g_durability_fsync_fail_on_call = 0;
        pthread_mutex_unlock(&g_durability_fsync_test_lock);
        errno = injected_errno;
        return -1;
    }
    pthread_mutex_unlock(&g_durability_fsync_test_lock);
#endif
    return fsync(fd);
}
~~~

Declare `int durability_fsync(int fd);` unconditionally, and
`void durability_test_fsync_reset(void);` /
`void durability_test_fsync_fail_on_call(int call_number, int err);` under
the existing `#ifdef TEST_BUILD` block in src/db/types.h, beside
`durability_test_pause`'s declaration. The call counter is process-lifetime
and shared by every `durability_fsync` call in the process, not reset
automatically: any test that arms it must call
`durability_test_fsync_reset()` both before arming and after its assertions,
so no pending injected failure or stale count leaks into a later case
sharing the same process under `--jobs 1`. Within one call to
`bt_publish_replace_locked`, `durability_fsync` is reached first via
`fsync_file_path(tmp_path)` (call N) and then via `fsync_parent_dir(target)`
(call N+1) — with the counter freshly reset, call 1 is always the pre-rename
fsync and call 2 is always the post-rename fsync, which is what the Task 2
tests above rely on to fail each independently. At
~void durability_test_pause(~, add `!data_dir` to the early-return guard.

## Task 3 — atomic bitmap publication and propagation

**Bitmap cache visibility invariant:** a `bm_open()` that begins after
`bm_publish_replace()` completes must not obtain the pre-rename inode through
`g_bm_cache`. An acquire that began before the writer gate is taken may finish
on its old mapping. This is the bitmap counterpart to invariant 3, not merely
an on-disk rename guarantee: serving an old bitmap after rebuild can produce
incorrect candidate selection.

Use a process-lifetime, writer-preferring `g_bm_publish_gate` in `bitmap.c`.
`bm_open` becomes a wrapper around a renamed `bm_open_impl`, exactly as
`bt_acquire` becomes a wrapper in Task 2: call `pthread_once`, take the gate as
reader, call `bm_open_impl` (covering cache hit, cold-open, cache install, and
uncached fallback), release the gate, then return the handle. Do not make
`bm_close` touch this gate. `bm_publish_replace` takes the writer gate before
invalidating either cache entry and keeps it through `rename` and
`fsync_parent_dir`; it releases the gate on every return path. The required
order is `g_bm_publish_gate` → `g_bm_cache_lock` → a cache-entry rwlock. No
object, kf, or B-tree lock may be acquired after taking the bitmap writer gate.
Like the B-tree gate, this is one process-wide gate: a slow bitmap parent
directory fsync briefly stalls unrelated bitmap acquires. This is an explicit
approval tradeoff for cache-correct publication, not a per-path lock.

At the unique anchor ~static int bm_next_pow2(~ in `src/db/bitmap.c`, add this
process-lifetime gate and rename the current body of `bm_open` to
`bm_open_impl`; immediately before that renamed definition add this wrapper:

~~~c
static pthread_rwlock_t g_bm_publish_gate;
static pthread_once_t g_bm_publish_gate_once = PTHREAD_ONCE_INIT;

static void bm_publish_gate_init(void) {
    rwlock_init_writer_preferring(&g_bm_publish_gate);
}

static BitmapShard *bm_open_impl(const char *path, int slots, int create,
                                 int bool_fastpath, uint32_t max_values,
                                 int writer);

BitmapShard *bm_open(const char *path, int slots, int create,
                     int bool_fastpath, uint32_t max_values, int writer) {
    int gate_rc;
    BitmapShard *bm;
    pthread_once(&g_bm_publish_gate_once, bm_publish_gate_init);
#ifdef TEST_BUILD
    bm_publish_reader_pending_begin();
#endif
    gate_rc = pthread_rwlock_rdlock(&g_bm_publish_gate);
#ifdef TEST_BUILD
    bm_publish_reader_pending_end();
#endif
    if (gate_rc != 0) { errno = gate_rc; return NULL; }
    bm = bm_open_impl(path, slots, create, bool_fastpath, max_values, writer);
    pthread_rwlock_unlock(&g_bm_publish_gate);
    return bm;
}
~~~

### Test first

The existing pause hook is used only to create deterministic race/crash
windows. Read/decode failures use the direct on-disk-corruption technique
already established by `test_coverity_disk_corruption_btree.c`; fsync failures
use Task 2's new deterministic fsync seam.

Immediately before ~TEST_REGISTER("test-bitmap-index", test_bitmap_index_run)~
in src/test/cases/test_bitmap_index.c, add two tests. (1) A parallel
single-field `cmd_add_index(..., force=1)` rebuild where one live kf shard file
under `data/kf/` is truncated to a partial header before the rebuild starts,
so that `bm_shard_walk_worker` fails: assert the command returns nonzero and
that every shard of that field's pre-existing `.bm` target is byte-identical
to its pre-rebuild contents (compare full file contents, not just
presence/mtime). (2) A daemon multi-field reindex pause/restart
using `durability_test_pause` at phase `bm-publish-before-rename`, SIGKILLed
mid-pause, restarted, proving every previous `.bm` target is still queryable
and no `.rebuild-` sibling was mistaken for a live target. A crash may leave a
temporary sibling, which is permitted by invariant 1.

Immediately before
~TEST_REGISTER("test-reindex-spill-collision", test_reindex_spill_collision_run);~,
add a daemon malformed-bitmap-spill test. Add a
`durability_test_pause(index_dir, "bm-resolve-before-open")` immediately
before the resolver opens that field's temporary bitmap writers. Start
reindex, wait for the marker, truncate one `bmw*.bin` spill mid-record, then
let it continue. Assert the request fails and every one of that field's `.bm`
targets—not only the affected shard—is byte-identical to its pre-call
contents. This exercises the static resolver through its real public path;
the test must not call it directly.

In the first bitmap test, after seeding a non-empty bitmap index, delete every
record through the public API and force-rebuild it. Assert every published
`.bm` shard is a valid empty bitmap and that no old candidate bit remains. This
is the bitmap counterpart to Task 2's empty B-tree rebuild regression.

Immediately before the same bitmap registration anchor, add a third,
bounded-child cache-visibility regression. Pause one `bm_publish_replace` at
`bm-publish-before-rename`, start a reader which calls `bm_open` for that exact
target and waits at the new reader-pending counter, release the publisher,
then assert the reader observes the replacement—not the old bitmap. A
continuous-reader variant must prove a queued bitmap publisher enters the
writer-preferring gate. Cache invalidation alone is not sufficient: without
this gate, a new reader can open the old pathname between invalidation and
rename.

### Change

At `bitmap.h`, immediately before the `BitmapShard` declarations, add a public
three-value `BmPublishResult` enum with `BM_PUBLISH_OK`,
`BM_PUBLISH_PRE_RENAME_FAILED`, and `BM_PUBLISH_POST_RENAME_FSYNC_FAILED`.

~~~c
typedef enum {
    BM_PUBLISH_OK = 0,
    BM_PUBLISH_PRE_RENAME_FAILED,
    BM_PUBLISH_POST_RENAME_FSYNC_FAILED,
} BmPublishResult;
~~~

In the same public declaration block, add this prototype; `index.c` calls the
function, so declaring only its enum is insufficient:

~~~c
BmPublishResult bm_publish_replace(const char *target, const char *tmp_path);
~~~

Immediately after ~static int bm_publish(~ in src/db/bitmap.c, add a
from-scratch `BmPublishResult bm_publish_replace(const char *target, const
char *tmp_path)` mirroring the B-tree three-state contract: `fsync_file_path`
the temp, invalidate its cache entry and the target cache entry, pause at
`bm-publish-before-rename`, rename, then `fsync_parent_dir`. It accepts paths,
not an already-open live `BitmapShard`; callers close their temporary mapping
before invoking it. It must preserve the rename result on a later parent-fsync
failure. In `index.c`, map this public enum to `IndexBuildResult` with the same
three-state mapping used for `BtPublishResult`.

Immediately after the existing bitmap-cache test counters (or, if none are
present, beside the new gate declaration), add a `TEST_BUILD` mutex-protected
`g_bm_publish_reader_pending_count` and expose
`int bitmap_test_publish_reader_pending_count(void);` in `bitmap.h`. The
`bm_open` wrapper increments it immediately before waiting for the gate reader
lock and decrements it immediately afterward. The bounded-child test above
waits on this counter; it must not use a cache-entry lock counter because that
would miss the invalidate/rename race this gate closes.

Use this complete test-only counter block immediately before the wrapper above
(and declare `bitmap_test_publish_reader_pending_count` beside the other test
prototypes in `bitmap.h` under `TEST_BUILD`):

~~~c
#ifdef TEST_BUILD
static pthread_mutex_t g_bm_publish_reader_pending_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_bm_publish_reader_pending_count;

static void bm_publish_reader_pending_begin(void) {
    pthread_mutex_lock(&g_bm_publish_reader_pending_lock);
    g_bm_publish_reader_pending_count++;
    pthread_mutex_unlock(&g_bm_publish_reader_pending_lock);
}

static void bm_publish_reader_pending_end(void) {
    pthread_mutex_lock(&g_bm_publish_reader_pending_lock);
    g_bm_publish_reader_pending_count--;
    pthread_mutex_unlock(&g_bm_publish_reader_pending_lock);
}

int bitmap_test_publish_reader_pending_count(void) {
    pthread_mutex_lock(&g_bm_publish_reader_pending_lock);
    int n = g_bm_publish_reader_pending_count;
    pthread_mutex_unlock(&g_bm_publish_reader_pending_lock);
    return n;
}
#endif
~~~

The `bm_publish_replace` body is exactly:

~~~c
BmPublishResult bm_publish_replace(const char *target, const char *tmp_path) {
    int gate_rc, saved_errno;
    char parent[PATH_MAX];
    if (parent_dir_copy(target, parent, sizeof(parent)) != 0 ||
        fsync_file_path(tmp_path) != 0)
        return BM_PUBLISH_PRE_RENAME_FAILED;
    pthread_once(&g_bm_publish_gate_once, bm_publish_gate_init);
    gate_rc = pthread_rwlock_wrlock(&g_bm_publish_gate);
    if (gate_rc != 0) { errno = gate_rc; return BM_PUBLISH_PRE_RENAME_FAILED; }
    bm_cache_invalidate(tmp_path);
    bm_cache_invalidate(target);
    durability_test_pause(parent, "bm-publish-before-rename");
    if (rename(tmp_path, target) != 0) {
        saved_errno = errno;
        pthread_rwlock_unlock(&g_bm_publish_gate);
        errno = saved_errno;
        return BM_PUBLISH_PRE_RENAME_FAILED;
    }
    if (fsync_parent_dir(target) != 0) {
        saved_errno = errno;
        pthread_rwlock_unlock(&g_bm_publish_gate);
        errno = saved_errno;
        return BM_PUBLISH_POST_RENAME_FSYNC_FAILED;
    }
    pthread_rwlock_unlock(&g_bm_publish_gate);
    return BM_PUBLISH_OK;
}
~~~

At the `BmRebuildCtx` definition, add `int failed; int saved_errno;`; replace
the unique callback statement ~bm_set(c->bm, key_buf, key_len, slot);~ with a
checked call that records its first errno and returns `-1`. At the
`BmShardWalkArg` definition immediately preceding ~static void
*bm_shard_walk_worker(~, add `target_path`, `tmp_path`, `IndexBuildResult
result`, and an initialized `BmRebuildCtx`. The worker must propagate failures
from Kf acquire, temp `bm_open`, the slot walk's return code, callback,
`bm_sync`, and `bm_close`; on a pre-publication failure it closes/unlinks only
its temp and returns `{ INDEX_BUILD_FAILED, 0 }`. `build_bitmap_pass` folds
all workers, and only if every worker materialised successfully does it publish
the closed temps and fold those publication results.

At ~static int resolve_bitmaps(~, open all writers at sibling temps. Reject a
missing/truncated kf shard, partial spill records (including any nonzero tail
shorter than the four-byte record header), out-of-range Kf shards,
unresolvable hashes, and all open/mmap/read/set/sync/close errors. Close every
temp before publishing. A materialisation error before the first rename unlinks
every temp for that field and publishes none. If a later per-shard rename fails,
earlier shards remain published and the returned result is
`{ INDEX_BUILD_FAILED, 0 }`: atomicity is per shard, not an unimplementable
multi-file transaction.

Replace (do not supplement) every force-time live-file deletion at these
anchors:

~~~c
if (force) tg_idx_unlink_all(db_root, object, field, sch->splits);
if (force) btree_idx_unlink_all(db_root, object, field, sch->splits);
tg_idx_unlink_all(db_root, object, names[i], sch.splits);
btree_idx_unlink_all(db_root, object, fields[i], sch.splits);
~~~

with no deletion. `force` means “rebuild and publish replacements”; it must
not change files before successful publication. At the unique definition
comment ~build_indexes_streaming_multi — build every index~, propagate
`IndexBuildResult` through every call site listed above. At ~int cmd_add_index(~
and ~int cmd_add_indexes(~, write and `fsync_file_path(index.conf)` only after
`status != INDEX_BUILD_FAILED`; a durability warning writes metadata, emits
`{"warning":"index published but directory durability is unconfirmed"}` and
returns nonzero.

In `reindex_object_checked`, replace the entire upfront-wipe block — both the
full-reindex path and the composites-only path — with nothing (publication
now happens in place; there is no more "clear the slate first"):

~~~c
    if (!composites_only) {
        reindex_wipe_idx_dirs(eff_root, object);
    } else {
        /* Wipe only the composite field dirs to leave non-composite indexes intact. */
        char idx_root[PATH_MAX];
        snprintf(idx_root, sizeof(idx_root), "%s/%s/indexes", eff_root, object);
        for (int i = 0; i < nf; i++) {
            char fname[512]; strncpy(fname, field_specs[i], 511); fname[511] = '\0';
            char *colon = strchr(fname, ':'); if (colon) *colon = '\0';
            char fdir[PATH_MAX];
            snprintf(fdir, sizeof(fdir), "%s/%s", idx_root, fname);
            DIR *dd = opendir(fdir);
            if (dd) {
                struct dirent *de;
                while ((de = readdir(dd))) {
                    if (de->d_name[0] == '.') continue;
                    char sp[PATH_MAX];
                    snprintf(sp, sizeof(sp), "%s/%s", fdir, de->d_name);
                    btree_cache_invalidate(sp);
                }
                closedir(dd);
            }
            rmrf(fdir);
        }
    }
~~~

Immediately before ~int reindex_object_checked(~, add this post-build cleanup
helper. It runs only after a build result is known, so it never removes a
file that publication hasn't already replaced — legacy single-file artefacts
and out-of-range numeric shards from a prior (larger) split count are the
only things it touches; `.rebuild-*` siblings are skipped because they are
dotfiles and the loop already skips dotfiles:

~~~c
/* Post-build cleanup for reindex: with the upfront wipe removed, indexes
   are published in place via bt_publish_replace_locked/bm_publish_replace,
   so a partially-failed run always leaves the previous live shard intact.
   Once every requested shard for this reindex has published (checked by the
   caller via IndexBuildResult.all_requested_shards_published), this sweeps
   only what the new layout can never reference again: a legacy pre-2026.05.1
   single-file <field>.idx, and numeric shard files left behind by a higher
   split count than the object's current index_splits_for(splits). It must
   not run before publication completes — see reindex_object_checked. */
static void reindex_cleanup_obsolete(const char *eff_root, const char *object,
                                     char (*field_specs)[512], int nf,
                                     int new_idx_splits) {
    char idx_root[PATH_MAX];
    snprintf(idx_root, sizeof(idx_root), "%s/%s/indexes", eff_root, object);

    for (int i = 0; i < nf; i++) {
        char fname[512];
        strncpy(fname, field_specs[i], 511);
        fname[511] = '\0';
        char *colon = strchr(fname, ':');
        if (colon) *colon = '\0';

        char legacy[PATH_MAX];
        snprintf(legacy, sizeof(legacy), "%s/%s.idx", idx_root, fname);
        struct stat lst;
        if (lstat(legacy, &lst) == 0 && S_ISREG(lst.st_mode)) {
            btree_cache_invalidate(legacy);
            unlink(legacy);
        }

        char fdir[PATH_MAX];
        snprintf(fdir, sizeof(fdir), "%s/%s", idx_root, fname);
        DIR *d = opendir(fdir);
        if (!d) continue;
        int dfd = dirfd(d);
        struct dirent *e;
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            unsigned shard_idx;
            if (sscanf(e->d_name, "%3x.", &shard_idx) != 1) continue;
            if ((int)shard_idx < new_idx_splits) continue;

            char sp[PATH_MAX];
            snprintf(sp, sizeof(sp), "%s/%s", fdir, e->d_name);
            btree_cache_invalidate(sp);
            unlinkat(dfd, e->d_name, 0);
        }
        closedir(d);
    }
}
~~~

At the call site added by item 8 above — immediately after `build_result` is
assigned from `build_indexes_streaming_multi` and before `free(descs);
free(field_specs);` — add:

~~~c
    if (build_result.all_requested_shards_published)
        reindex_cleanup_obsolete(eff_root, object, field_specs, nf,
                                 index_splits_for(sch.splits));
~~~

This must run before `free(field_specs)` since it reads `field_specs`.

## Task 4 — docs and verification

### Review amendments (2026-08-01)

The execution review adds these non-negotiable corrections to Tasks 2 and 3:

- `enumerate_segments` must distinguish a successful empty enumeration from
  every setup failure. Allocation, directory-open, and directory-read failures
  abort before worker allocation and before any shard publication; only a
  successful enumeration with zero `.dat` files may publish empty shards.
- Every `bt_stream_build_add` failure aborts and disposes its temporary builder
  output before `bt_stream_build_finish`; no partial merge may reach rename.
- Add the Task 2/3 regressions at their stated anchors in `test_btree.c`,
  `test_bt_cache_writer_starvation.c`, `test_reindex_spill_collision.c`, and
  `test_bitmap_index.c`, including an enumeration/setup-failure case proving
  existing targets survive an inaccessible stream directory.
- Keep public and private C type names snake_case. Comments describe the
  invariant or return contract, never a caller. Factor the common durable
  replacement sequence into `durability.c`; B-tree and bitmap wrappers retain
  only their gate/cache-specific preparation and cleanup.

The review also requires recorded base failure, fix-removed failure, restored
pass, normal full-suite, ASan, and TSan evidence before handoff.

During execution, `test-bitmap-index` exposed a cleanup defect: reindex kept
only `index_splits_for(splits)` files for every field, which deleted valid
bitmap shards because bitmaps retain one file per keyfile shard. Cleanup now
uses each field's resolved index type, retains `splits` bitmap shards, and
invalidates the matching bitmap cache entries before deleting obsolete files.

At ~static void *idx_build_field_worker(~ in src/db/query_bulk.c, log field,
shard, target, state, and errno for failures/warnings. Update
docs/concepts/indexes.md immediately after ~# Indexes~ with an
~## Index build and maintenance~ section describing atomic per-shard
publication and durability warnings.

For every regression, record base failure, failure with its fix temporarily
removed, and pass after restoration. Then run exactly:

~~~bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 2
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all --jobs 1
~~~

Do not run benchmarks. Leave the implementation uncommitted.

## Execution rules

- Branch from main only after explicit approval.
- Execute Tasks 1–4 in order.
- If an anchor is absent or non-unique, write PLAN_NOTES.md and halt the
  entire execution run.
- If a new index writer cannot use a sibling temporary without a format
  change, halt and obtain a revised plan.
