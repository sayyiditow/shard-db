# Superseded: index-integrity revision

**Status: SUPERSEDED — do NOT execute.**

This monolithic draft has been replaced by two ordered, independently
reviewable plans:

1. [Plan A — atomic index publication](2026-07-31-index-atomic-publication.md)
2. [Plan B — atomic indexed writes](2026-07-31-indexed-write-atomicity.md)

Plan B depends on Plan A. Together they retain the original requirement: every
indexed mutation path has the same durable all-or-nothing visibility guarantee.
This file is historical research only and must not be used as an execution plan.

---

# Historical draft: atomic B-tree/bitmap replacement and durable bulk-index rejection

**Status when superseded: DRAFT (rev 3) — do NOT execute.** This supersedes
`2026-07-30-bulk-index-apply-hard-reject.md`. It is intentionally one plan:
the bulk failure is unsafe because an index replacement can fail, and an
in-place index replacement can itself create the unreadable index that
causes the bulk failure.

**Rev 3 changes:** incorporates the second review pass over rev 2. It closes
the remaining caller-propagation, bitmap-error, abort-durability, fault-seam,
partial-marker, and source-anchor gaps.

**Rev 4 changes:** closes the final pre-execution review findings: publication
now has an acquire-vs-rename exclusion protocol (not merely an invalidate
call), `bt_extract_all` distinguishes ENOENT from a failed open, the abort
resolver's live-worker caller derives its currently-missing object identity,
and all new tests have concrete case-file anchors.  No product/API decision
changed in this revision.

**Rev 2 changes:** incorporates an independent review pass over rev 1. Every
finding below was independently verified against the current source before
being folded in; two were sharpened past how the review stated them (#3, #4)
and one was downgraded from "crash" to "unverified contract" (#1) after
tracing the actual null-handling. See each task's "Rev 2" note for what
changed and why.

## Binding invariants

1. A successful indexed bulk record is Kf-visible only after every required
   index change has completed and is durable.
2. A bulk request that receives an index-apply error never becomes visible
   later. Its abort decision is durable before that error is reported.
3. If undo cannot complete, the process fails closed with durable evidence;
   it does not clear a marker, reuse a segment slot, or serve potentially
   wrong indexed results. This includes an abort sidecar that exists but is
   corrupt or truncated: fail closed, never treat corrupt as absent.
4. A failed index-file rebuild (B-tree, trigram, or bitmap) leaves the prior
   completed on-disk file intact. A crash may leave a temporary sibling,
   never a truncated live file. If a rebuild's publish step (atomic rename)
   itself succeeds but a subsequent durability step (parent-directory fsync)
   fails, the new content is live and correct — that is a distinct state
   from "rebuild failed," and callers must not conflate the two (see Task 2).
5. Reindex/add-index must not remove a completed live shard — of any index
   type — before its replacement shard has been built, synced, and
   atomically renamed.
6. A batch id that names an unresolved or unvalidated abort sidecar is never
   reused by a new writer, even across unrelated requests. Recovery (both
   the per-shard write gate and startup sweep) must discover a sidecar by
   its own filename pattern, independent of whether its paired marker file
   still exists.

## Decisions fixed by this plan

No product decision is required before execution. The following policy is
binding:

- A publish result is summarized as `INDEX_BUILD_OK`,
  `INDEX_BUILD_DURABILITY_UNCONFIRMED`, or `INDEX_BUILD_FAILED`.
- `INDEX_BUILD_DURABILITY_UNCONFIRMED` means every requested shard was
  renamed successfully, but at least one parent-directory fsync failed. The
  target is live and counts as published; index metadata is written and
  synced, but the command returns a distinct nonzero durability-warning
  result so the operator knows a crash immediately after the operation may
  require verification.
- `INDEX_BUILD_FAILED` means at least one requested shard was not renamed.
  New index metadata is not written for that operation. Any shards that did
  publish remain complete and are retried by the next build; obsolete shards
  are removed only when the whole requested replacement set published.
- Ordinary online bulk indexing treats either non-OK result as an index
  apply failure. It persists the abort decision and does not publish the Kf
  record, even when the target index file is already live after a post-rename
  fsync failure.

`reindex` remains an operator maintenance command and must be allowed to
finish. This plan nevertheless preserves the prior completed index files on
failure; it does **not** promise all index shards switch as one database-wide
transaction. The object write lock already prevents writes during reindex, so
a completed old or completed new shard is correct for the same record set.

## Confirmed causes

### A. Empty and unreadable B-trees are conflated

`bt_extract_all()` returns `(NULL, 0)` for a missing/empty tree and also for
failed open/mmap, bad magic, truncation, initial allocation failure, and a
partial leaf scan. `btree_bulk_merge()` treats `exist_count == 0` as proof
that the tree is empty and calls its destructive rebuild path. Therefore it
can replace a previously populated but unreadable index with only the incoming
entries.

The cache retry itself is not a failure cause: after its bounded verify retry,
the reader falls back to a direct mapping. `bt_acquire()` can still fail when
that direct open/fstat/mmap fails, and corrupt/truncated files are independent
failure sources.

### B. The current rebuild is destructive before success

`btree_bulk_build_locked()` and `bt_stream_build_open()` call
`btree_cache_invalidate(path); unlink(path);` before writing the replacement.
An allocation, I/O, or process failure after that point removes the live file
or leaves a partial replacement. The streaming reindex pipeline's `.spill_*`
files are temporary inputs only; its final `.idx`/`.tg` output currently uses
this destructive live path. Separately, `merge_spills_into_index()`
(`index.c:1935`) exits early when no spill entries exist for a shard —
already a latent bug once the destructive pre-unlink above is removed (see
Task 2, Rev 2 note).

### C. Failed index apply currently still publishes Kf

`slotcask_bulk_upsert_in_kfshard()` sets `keep_marker` after a non-zero
`apply_window()` return but then unconditionally runs its Kf commit/repoint
loop. `v2_bulk_ins_apply_window()` deliberately keeps applying other index
operations after the first failure, so the visible record can have an
arbitrary subset of indexes.

### D. The superseded rollback was not durable

The prior draft proposed inverse index application, ignored inverse failures,
discarded the forward-replay marker, and immediately returned segment slots to
the pool. That can retain orphan indexes, make an old update index disappear,
resurrect a rejected record after a crash, or reuse a segment slot whose
tombstone write failed. None is acceptable.

### E. Bitmap force/reindex rebuilds are destructive before success (Rev 2)

Both bitmap-rebuild call sites unlink the live `.bm` shard and recreate it in
place *before* any replacement data exists, the same defect as cause B but for
bitmaps, and untouched by rev 1's Task 2:

- `build_bitmap_pass()` (`index.c:2181-2188`, the single-field `cmd_add_index`
  path): `bm_cache_invalidate(bp); unlink(bp); bm_open(bp, slots_per_shard,
  1, bool_fastpath, max_values, 1)` runs once per shard, then a **parallel** per-shard walk
  (`bm_shard_walk_worker`, one thread per kf shard) populates each freshly
  emptied file directly via its mmap.
- `resolve_bitmaps()` (`index.c:~3168-3172`, the multi-field
  `build_indexes_streaming_multi` path used by `reindex` and
  `cmd_add_indexes`): the same wipe-then-create sequence runs once per shard
  per bitmap field, up front, before a **single-threaded** spill-merge loop
  that interleaves writes across every shard of that field as it reads spill
  files in arbitrary order.

An allocation, I/O, or process failure after either wipe leaves the shard
permanently and silently empty — dictionary and bitmaps both gone — with no
prior data to fall back to and no `*.rebuild-*` sibling to signal a resumable
failure. `bm_publish()` (`bitmap.c:230`) already implements a
build-tmp/rename/remap primitive, but it's shaped for `bm_dict_add`'s
single-writer, already-open-handle dictionary growth (`bitmap.c:723`,
`bitmap.c:904`); neither bitmap-rebuild call site uses it, and neither can
adopt it unmodified — see Task 3.

## Task 1 — Make B-tree extraction fail closed

### Test first

Keep the superseded plan's corrupt-existing-tree regression, but extend it
with a test-only leaf-chain corruption case. The test must seed a valid tree,
then make the root/leaf chain invalid while retaining a plausible header. Both
`btree_bulk_merge()` calls must return non-zero and the target path must still
refer to the pre-call inode/content. The test must run before and after the
fix.

Add the two direct corruption cases to
`src/test/cases/test_coverity_disk_corruption_btree.c`, immediately before
the quoted registration anchor
`TEST_REGISTER("test-coverity-btree-nextleaf-cycle", test_coverity_btree_nextleaf_cycle_run);`; do not create a
separate case file. The test creates its temporary DB root under `/tmp` using
that file's existing `test_env_start_at` fixture and removes it through its
existing `tu_run_cmd("rm -rf %s", base)` cleanup path.

### Change

Replace the `bt_extract_all` contract with:

```c
/* NULL + out_failed==0 means either a valid empty tree or an absent target
 * (and errno==ENOENT in the latter case).  A failed open other than ENOENT,
 * invalid header/page, impossible page link, allocation failure, incomplete
 * scan, or entry_count mismatch sets out_failed=1. */
static BtEntry *bt_extract_all(const char *path, size_t *out_count,
                               int *out_failed);
```

The implementation must validate the header magic, root page bounds, each
page type, and forward leaf progression. It must bound traversal by
`fh->page_count` (reject a cycle rather than looping forever) and require
`count == fh->entry_count`. It must free every allocated entry before returning
failure.

In `btree_bulk_merge`, the only legal rebuild-from-`new_entries` case is a
valid empty or absent target.  `bt_acquire(&bt, path, 0)` failing with
`errno == ENOENT` is the sole absent-target case; every other open failure is
an extraction failure. Any `extract_failed` result must preserve `errno`,
unlock the mutation gate, and return `-1` without calling a build or unlink
function.

## Task 2 — Atomically publish B-tree and trigram rebuilds

### Rev 2 notes

- **Publish contract (finding #4, sharpened).** The original draft had
  `rename()` before `fsync_parent_dir()` in `bt_publish_replace_locked` and
  said "on failure target remains unchanged." That ordering (sync tmp →
  rename → fsync parent dir) is the *correct* crash-safety sequence — the
  bug is that the function collapsed two different failure states into one
  return code. If `rename()` succeeds and only the trailing
  `fsync_parent_dir()` fails, the target **already holds the new content**
  and `tmp_path` **no longer exists**; a caller that reads the old contract
  ("failure ⇒ unchanged, clean up tmp_path") will call `unlink(tmp_path)` on
  a path that's already gone (harmless but wrong) and may report the shard
  as unbuilt/unreplaced when it is in fact live and correct (misleading, and
  breaks the "remove obsolete shard only after every replacement has
  published" bookkeeping in the force-rebuild callers below). Fixed by
  splitting the return into three explicit states.
- **Empty-shard regression (finding #5, confirmed as-is).**
  `merge_spills_into_index()` early-returns when `reader_count == 0`,
  skipping the build/publish entirely. Once the destructive pre-unlink is
  removed, a shard whose spill contribution is legitimately empty (e.g. every
  matching record was deleted, or an add-index on a sparse field) keeps its
  stale prior content forever instead of converging to a valid empty index.
  `bt_stream_build_finish()` already produces a correct empty (header-only)
  tree for zero adds (`btree.c:2969-2976`), so the fix is to stop
  special-casing zero and always build+publish.
- **Test seam (finding #6, confirmed as-is, different fix).** The originally
  proposed `btree_test_set_publish_fault()` is a test-process-side global;
  `shard-db-test`'s daemon fixture spawns the daemon via `execl()`
  (`src/test/fixtures.c:284`), which replaces the process image, so nothing
  set in the test process is visible to the daemon. The codebase already has
  a production-safe (default-off, config-gated) mechanism for exactly this —
  `durability_test_pause(data_dir, phase)` (`durability.c:103`), used by the
  existing bulk-window durability tests (`slotcask.c:5725` etc.): it writes a
  `.durability-test-<phase>.active` marker file the harness can poll for,
  sleeps for a configured duration, then removes the marker. Reuse it instead
  of inventing a new mechanism.

### Test first

1. Seed a target with entries, force the in-process publish fault (test #1/#2
   below), call `btree_bulk_merge`, and prove the old entries remain and no
   `*.rebuild-*` file is treated as a live index.
2. Run `btree_bulk_build` under the same injected failure and prove its old
   target remains readable.
3. Start a daemon reindex/add-index force build with
   `DURABILITY_TEST_PAUSE_PHASE=bt-publish-before-rename` and a nonzero pause
   set in `db.env` before spawn, poll for
   `<data_dir>/.durability-test-bt-publish-before-rename.active`, `SIGKILL`
   the daemon once it appears, restart, and prove the previous index still
   returns its old records. A left-over sibling temporary file is allowed and
   must be removed by the next successful build or startup cleanup; it must
   never be opened as the target.
4. **New (finding #4):** inject `DURABILITY_FAULT_PARENT_FSYNC` so
   `fsync_parent_dir()` fails after a successful `rename()` (in-process fault,
   no daemon needed). Assert the
   target now serves the *new* content, the function returns
   `BT_PUBLISH_POST_RENAME_FSYNC_FAILED` (not treated identically to a
   pre-rename failure), and the caller does not attempt to `unlink(tmp_path)`
   or report the shard as unreplaced in force-rebuild bookkeeping.
5. **New (finding #5):** build an index with entries, delete every matching
   record, force a rebuild (`reindex` or `add-index -f`), and assert the
   resulting shard is a valid empty tree (`bt_extract_all` returns
   `(NULL, 0, out_failed=0)`), not the stale pre-delete content.
6. **New (publication-gate regression):** seed and cache an old tree, start
   `btree_bulk_merge` with its publish pause armed, then start a reader for
   the target while the pause is active. Assert the reader is pending on the
   publication gate (not serving the old cache entry); release the pause and
   assert it sees the newly published entry. Repeat from a cold cache: a
   racing reader must either finish its complete acquire before the writer
   gate begins or wait behind it; after `btree_bulk_merge` returns, every new
   acquire must observe the replacement inode.

Both #1/#2 use the common in-process countdown seam
`durability_test_fail_after(DURABILITY_FAULT_BTREE_BUILD, 1)` (test binary
and code under test share an address space there — no exec involved),
distinct from #3's out-of-band mechanism. The seam is consumed before the
temporary B-tree is published, so the old target remains readable.

Add direct B-tree extraction/build/publish and publication-gate tests to
`src/test/cases/test_btree.c`, immediately before
`TEST_REGISTER("test-btree", test_btree_run)`. Add the deterministic
concurrent gate test to `src/test/cases/test_bt_cache_writer_starvation.c`,
immediately before `static int test_bt_cache_writer_starvation_glibc_run(void)`
and extend that file's existing final `TEST_REGISTER` block. The latter must
remain glibc-gated and use its existing bounded child-process timeout; no test
may hang the shared runner.

### Change

Introduce a B-tree publication gate and a private three-state publication
primitive. The gate closes the otherwise fatal cache race: the current cache
is keyed only by pathname, and a cold reader opens outside the cache-table
mutex. An invalidate-before-rename sequence alone lets that reader install an
old inode after invalidation. Every `bt_acquire` must complete its lookup/open
and cache installation under the gate's reader side; replacement owns its
writer side from target invalidation through rename and directory fsync.

Rename the current body of `bt_acquire` to `bt_acquire_impl` without changing
its logic, then make its sole public-to-this-TU entry point this wrapper:

```c
static pthread_rwlock_t g_bt_publish_gate = PTHREAD_RWLOCK_INITIALIZER;
#ifdef TEST_BUILD
static atomic_int g_bt_publish_reader_pending;
#endif

static int bt_acquire(BtFile *bt, const char *path, int writer) {
    int rc, gate_rc;
#ifdef TEST_BUILD
    atomic_fetch_add(&g_bt_publish_reader_pending, 1);
#endif
    gate_rc = pthread_rwlock_rdlock(&g_bt_publish_gate);
#ifdef TEST_BUILD
    atomic_fetch_sub(&g_bt_publish_reader_pending, 1);
#endif
    if (gate_rc != 0) {
        errno = gate_rc;
        return -1;
    }
    rc = bt_acquire_impl(bt, path, writer);
    pthread_rwlock_unlock(&g_bt_publish_gate);
    return rc;
}
```

Under the existing `#ifdef TEST_BUILD` B-tree hook section, add this complete
observable for the publication-gate test (the counter is incremented before
the blocking lock call and decremented immediately after it succeeds):

```c
int btree_test_publish_reader_pending_count(void) {
    return atomic_load(&g_bt_publish_reader_pending);
}
```

Declare `int btree_test_publish_reader_pending_count(void);` beside the
existing TEST_BUILD B-tree test hooks in `src/db/btree.h`. The regression must
wait for this counter rather than using a timing sleep, and must release the
pause marker before joining its reader/writer threads.

The reader lock covers the whole existing `bt_acquire_impl` function,
including its cache-hit, cold-open, cache-install, uncached-fallback, and all
error-return paths. `bt_release` does **not** retain the publication lock:
readers already holding an old mmap may complete as operations that began
before publication, while `btree_cache_invalidate(target)` drains cached
readers before the writer gate is released. This gate is deliberately global
because it is held only for the final replace window; it does not cover the
potentially long temporary-file build.

The publication primitive is:

```c
typedef enum {
    BT_PUBLISH_OK = 0,
    /* target unchanged; tmp_path's content is preserved; caller owns
       cleanup (unlink) of tmp_path. */
    BT_PUBLISH_PRE_RENAME_FAILED,
    /* rename() succeeded: target now holds tmp_path's content and
       tmp_path no longer exists. Only the parent-directory fsync that
       persists the rename's directory-entry update failed. The build is
       live and correct; only its durability across an immediate crash is
       unconfirmed. Callers must NOT unlink tmp_path (already gone) and
       must NOT report this shard as unbuilt/unreplaced. */
    BT_PUBLISH_POST_RENAME_FSYNC_FAILED,
} BtPublishResult;

/* Caller holds the target mutation gate. tmp_path is on target's filesystem. */
static BtPublishResult bt_publish_replace_locked(const char *target,
                                                  const char *tmp_path,
                                                  const char *data_dir) {
    int saved_errno, gate_rc;
    if (btree_sync_path(tmp_path) != 0) return BT_PUBLISH_PRE_RENAME_FAILED;
    btree_cache_invalidate(tmp_path);
    gate_rc = pthread_rwlock_wrlock(&g_bt_publish_gate);
    if (gate_rc != 0) {
        errno = gate_rc;
        return BT_PUBLISH_PRE_RENAME_FAILED;
    }
    btree_cache_invalidate(target);
    durability_test_pause(data_dir, "bt-publish-before-rename");
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
```

`fsync_parent_dir()` is a private B-tree helper using a local, non-static
parent-path buffer; do not use `dirname_of()` twice in one expression because
it returns static storage. It must open the parent with `O_DIRECTORY`, fsync,
close, and propagate errors.

Refactor the existing raw page builder into an internal function that receives
an already-created **output path** and never unlinks its target. The two
public build shapes use it as follows:

```c
/* Array build and merge rebuild: target mutation gate held. */
tmp = bt_make_sibling_temp_path(target, ".rebuild-");
rc = bt_build_file_locked(tmp, entries, count);
BtPublishResult prc = (rc == 0)
    ? bt_publish_replace_locked(target, tmp, data_dir)
    : BT_PUBLISH_PRE_RENAME_FAILED;
if (prc == BT_PUBLISH_PRE_RENAME_FAILED) { unlink(tmp); rc = -1; }
else if (prc == BT_PUBLISH_POST_RENAME_FSYNC_FAILED) {
    LOG_ERROR(LOG_SUB_BTREE,
        "bt_publish_replace_locked: %s published but parent directory "
        "fsync failed (errno=%d); rename durability unconfirmed",
        target, errno);
    rc = -1;  /* still surfaced to the operator, but see below: shard-
                 replacement bookkeeping must treat this as published. */
} else rc = 0;

```

For the streaming shape, `merge_spills_into_index` creates the temporary
builder, passes every heap entry to `bt_stream_build_add`, checks each add,
and passes `bt_stream_build_finish`'s result through the exact `prc` handling
shown above. It releases the builder before calling the publisher and unlinks
only the temporary path on a pre-rename failure.

Every force-rebuild caller that removes obsolete legacy/high-shard files
"only after every requested replacement shard has published successfully"
(below) must treat `BT_PUBLISH_POST_RENAME_FSYNC_FAILED` as **published** for
that bookkeeping decision — target holds the new content — while still
surfacing the `-1`/error through its own result path. Only
`BT_PUBLISH_PRE_RENAME_FAILED` means the old target is unchanged and the
attempted replacement did not happen.

The builder's `BtFile` cache key must be the temporary path. It must release
that mapping before target cache invalidation/rename. `bt_publish_replace_locked`
must be the only code path that takes `g_bt_publish_gate`'s writer side; it
must never call `bt_acquire` while holding it. No code path may call
`unlink(target)` as part of `btree_bulk_build_locked`, `bt_stream_build_open`,
or `btree_bulk_merge`.

Update `merge_spills_into_index()` to request a temporary output for each
`.idx` and `.tg` target, publish only after `bt_stream_build_finish()`
succeeds, and clean only that temporary output on failure. Do not alter or
delete the target at build start. **Remove the `if (reader_count == 0) goto
cleanup;` early return** — always open the temp builder, add whatever entries
exist (zero is valid), finish, and publish, so a shard whose spill
contribution is empty converges to a correctly-published empty tree instead
of retaining stale prior content.

Update force rebuild callers (`cmd_add_index`, `cmd_add_indexes`,
`build_trigram_pass`, `build_btree_streaming`, and `reindex_object_checked`):
remove their pre-build `*_unlink_all`/`reindex_wipe_idx_dirs` calls for shards
that are about to be rebuilt. Remove obsolete legacy/high-shard files only
after every requested replacement shard has published successfully (per the
`BT_PUBLISH_POST_RENAME_FSYNC_FAILED`-counts-as-published rule above). A
failed run therefore leaves a mix of old and newly published complete shards,
never a missing one. Because the object write lock is held and no record
changes during the maintenance command, either version is a correct index of
the same records.

### Propagate publish state through every caller

Do not leave the three-state result trapped inside `btree.c`. Add an internal
`IndexBuildResult` used by B-tree, trigram, and bitmap force-build wrappers:

```c
typedef enum {
    INDEX_BUILD_OK = 0,
    INDEX_BUILD_DURABILITY_UNCONFIRMED,
    INDEX_BUILD_FAILED,
} IndexBuildStatus;

typedef struct {
    IndexBuildStatus status;
    int all_requested_shards_published;
} IndexBuildResult;
```

The result aggregation rules are exact: a pre-rename failure makes the
operation `INDEX_BUILD_FAILED` and `all_requested_shards_published=0`; one or
more post-rename fsync failures with every shard renamed makes it
`INDEX_BUILD_DURABILITY_UNCONFIRMED` and `all_requested_shards_published=1`.
The low-level online `btree_bulk_*` APIs continue to return nonzero for either
error state, so `query_bulk.c` aborts the Kf transaction on both states. The
force-build wrappers retain the richer result for maintenance bookkeeping.
Change the internal signatures of `build_bitmap_pass`,
`build_trigram_pass`, `build_btree_streaming`, and
`build_indexes_streaming_multi` from plain `int` to `IndexBuildResult`; keep
small compatibility wrappers only where an existing public declaration must
continue returning `int`. The wrappers map both non-OK statuses to nonzero.
This makes it impossible for `cmd_add_index` or `cmd_add_indexes` to discard
the post-rename distinction accidentally.

Apply this contract at the actual callers, not only in the builders:

- `cmd_add_index` must check the result from `build_bitmap_pass`,
  `build_trigram_pass`, or `build_btree_streaming` before changing
  `index.conf`. On `INDEX_BUILD_FAILED`, leave metadata unchanged and return
  failure. On `INDEX_BUILD_DURABILITY_UNCONFIRMED`, write the canonical index
  line, fsync the file and its parent, return the distinct durability-warning
  result, and never unlink the live replacement.
- `cmd_add_indexes` must check `build_indexes_streaming_multi` before writing
  any requested index metadata. A pre-rename failure leaves all new metadata
  unchanged. If every requested shard published but directory durability is
  uncertain, record the requested lines, sync metadata, and return the
  durability-warning result. A partially published failed request must not
  remove old metadata or obsolete shards.
- `reindex_object_checked` must use `all_requested_shards_published` when
  deciding whether legacy/high-shard cleanup is legal, while propagating a
  post-rename durability warning to its caller.
- `idx_build_field_worker`, `query_bulk.c`, and every direct test caller must
  preserve the existing nonzero-on-error behavior. No caller may turn a
  post-rename result into Kf success merely because the target path exists.

The metadata write itself uses the same `fsync_file_path` plus
`fsync_parent_dir` helpers. A metadata parent-fsync failure is reported as
`INDEX_BUILD_DURABILITY_UNCONFIRMED` after the line is written, never as a
reason to delete a newly published index.

## Task 3 — Atomically publish bitmap rebuilds (Rev 2, new)

### Rev 2 notes

Finding #7, confirmed and concretized. Rev 1 gestured at "audit each bitmap
force path and use its existing publish primitive" without a design; there
are two call sites with materially different write topology (see Cause E),
so both need explicit handling, neither is a drop-in reuse of `bm_publish()`.

### Test first

1. Force a bitmap rebuild via `cmd_add_index` (single field), inject a
   failure mid-walk (a test-only countdown fault
   `DURABILITY_FAULT_BITMAP_WALK` in `bm_set` or the walk callback), and assert
   the previously-live `.bm` shard for every *other*,
   unaffected kf shard is untouched, and the affected shard's *prior* `.bm`
   content is still readable (not empty, not partial). A worker that fails to
   acquire Kf, open the temp bitmap, walk the shard, set a bit, or close the
   file must mark its temp unpublishable; it must never be published as an
   empty or partial replacement.
2. Same for the `reindex`/`cmd_add_indexes` path through `resolve_bitmaps`
   (multi-field, single-threaded merge loop): inject a failure partway
   through the spill-merge loop using `DURABILITY_FAULT_BITMAP_SPILL`,
   including a malformed/truncated spill record and a failed `bm_set`, and
   assert no temp for that field is
   published. Every live `.bm` file is therefore either fully old or fully
   new, never partially written.
3. Daemon variant of #2 using `durability_test_pause` (same mechanism as
   Task 2 test #3): pause before a shard's publish, `SIGKILL`, restart,
   assert the old bitmap for that field/shard still serves correct results.

Add the in-process single-field and daemon multi-field tests to
`src/test/cases/test_bitmap_index.c`, immediately before
`TEST_REGISTER("test-bitmap-index", test_bitmap_index_run)`. Add the
reindex-spill malformed-record regression to
`src/test/cases/test_reindex_spill_collision.c`, immediately before
`TEST_REGISTER("test-reindex-spill-collision", test_reindex_spill_collision_run);`.

### Change

Add a bitmap-side mirror of `bt_publish_replace_locked` in `bitmap.c`:

```c
typedef enum {
    BM_PUBLISH_OK = 0,
    BM_PUBLISH_PRE_RENAME_FAILED,
    BM_PUBLISH_POST_RENAME_FSYNC_FAILED,  /* same semantics as BtPublishResult */
} BmPublishResult;

/* Builds a fresh, complete bitmap file at tmp_path (caller already wrote and
   fsynced it) and atomically replaces target. Does not require an open
   BitmapShard handle at target — unlike bm_publish(), this is for a
   from-scratch rebuild, not a live handle's dictionary-growth resize. Both
   callers hold objlock_wrlock for this object, which excludes bitmap readers
   for the entire invalidate/rename interval. */
static BmPublishResult bm_publish_new(const char *target, const char *tmp_path,
                                      const char *data_dir) {
    if (fsync_file_path(tmp_path) != 0) return BM_PUBLISH_PRE_RENAME_FAILED;
    bm_cache_invalidate(tmp_path);
    bm_cache_invalidate(target);
    durability_test_pause(data_dir, "bm-publish-before-rename");
    if (rename(tmp_path, target) != 0) return BM_PUBLISH_PRE_RENAME_FAILED;
    if (fsync_parent_dir(target) != 0) return BM_PUBLISH_POST_RENAME_FSYNC_FAILED;
    return BM_PUBLISH_OK;
}
```

(`fsync_parent_dir` is shared with Task 2's B-tree helper; move it to a
common location — e.g. `util.c` — used by both `btree.c` and `bitmap.c`
rather than duplicating it.)

### Shared durability helpers and deterministic fault seams

Add these helpers to the common durability/util module and declare them in
the shared internal header:

```c
int fsync_file_path(const char *path);
int fsync_parent_dir(const char *path);
```

`fsync_parent_dir` must use a local parent-path buffer, `O_DIRECTORY`,
`fsync`, and `close`, and must preserve the first failing `errno`. Both
publish primitives call it after `rename`; neither duplicates the helper.
The internal force-build entry points receive `data_dir` explicitly for
`durability_test_pause`. Public path-only B-tree APIs pass `NULL`, which
disables the pause for ordinary online/test calls; make that null behavior
explicit in the existing helper so `NULL` cannot reach its marker formatting:

```c
void durability_test_pause(const char *data_dir, const char *phase) {
    if (!data_dir || !g_db || g_durability_test_pause_ms <= 0 ||
        strcmp(g_durability_test_pause_phase, phase) != 0)
        return;
    /* Existing marker-write, timed-wait, and marker-unlink body remains here. */
}
```

All reindex/add-index
force paths pass the object data directory. This avoids guessing an object
directory from an arbitrary test path and covers every daemon call site.

Add a default-off, atomic, in-process countdown fault seam in the same module:

```c
typedef enum {
    DURABILITY_FAULT_NONE = 0,
    DURABILITY_FAULT_BTREE_BUILD,
    DURABILITY_FAULT_PARENT_FSYNC,
    DURABILITY_FAULT_BITMAP_WALK,
    DURABILITY_FAULT_BITMAP_SPILL,
    DURABILITY_FAULT_INDEX_DIFF,
    DURABILITY_FAULT_SEGMENT_FLAG,
} DurabilityTestFault;

void durability_test_fail_after(DurabilityTestFault fault, unsigned count);
int durability_test_should_fail(DurabilityTestFault fault);
```

The test-only seam is used only by in-process tests: parent-fsync injection
is consumed by `fsync_parent_dir`, bitmap faults by the walk/merge paths,
inverse failures immediately before the abort resolver calls the recovery
callback (never in a forward apply), and segment failures by the abort-only
durable flag helper. Daemon crash tests continue to use the existing
`durability_test_pause` marker because process-local state cannot cross
`execl()`.

**`build_bitmap_pass()` (`index.c`, single-field, parallel-per-shard):**
replace the up-front wipe loop's `bm_cache_invalidate(bp); unlink(bp);
bm_open(bp, slots_per_shard, 1, bool_fastpath, max_values, 1)` with
`bm_make_sibling_temp_path(bp, ".rebuild-")` followed by
`bm_open(tmp, slots_per_shard, 1, bool_fastpath, max_values, 1)` — i.e. create the fresh bitmap at a
temp sibling of the target, not at the target itself. Add a `target_path`
field to `BmShardWalkArg` alongside the existing `path` (which becomes the
temp working path the worker writes into):

```c
typedef struct {
    char         path[PATH_MAX];         /* temp working path — worker writes here */
    char         target_path[PATH_MAX];  /* live path — published after the walk */
    int          kf_shard;
    int          slots_per_shard;
    int          fi;
    int          rc;                     /* 0 only if walk, sync, and close succeeded */
    int          failed;                 /* callback stops on first write failure */
    TypedSchema *ts;
    SlotcaskDb  *sdb;
} BmShardWalkArg;
```

`bm_shard_walk_worker` must gain an `rc`/`failed` field and return a failure
status through the argument array. It must check the return value of
`kfcache_acquire`, `bm_open`, `slotcask_walk_one_shard_slots_locked`, every
`bm_set`, and `bm_sync` before `bm_close`; the callback must stop the walk on
the first failed `bm_set`. `parallel_for()` must aggregate worker failures.
After it returns,
publish only args with `rc == 0`; unlink failed temps and log that the live
target remains intact. Publish successful temps with
`bm_publish_new(args[s].target_path, args[s].path, data_dir)`. A pre-rename
failure leaves the old target and makes the aggregate result failed; a
post-rename fsync failure counts as published but makes the aggregate result
durability-unconfirmed. A worker failure must never be represented by an empty
successful temp.

**`resolve_bitmaps()` (`index.c`, multi-field, single-threaded merge):**
same substitution in its wipe loop — open each shard's writer handle at a
temp sibling path instead of at `bp`, then make the spill loop fail closed:
reject a trailing partial record, reject an out-of-range record that cannot be
resolved, and propagate every `bm_set`, `bm_sync`, mmap, close, and spill-file error into
the field's `merge_rc`. Close all temps before publishing. If any error
occurred for the field, unlink every temp and publish none of that field's
shards. Otherwise publish each complete temp over its target, treating
post-rename fsync failure as published-but-uncertain, before `rmdir(spill_dir)`.
Track the field result explicitly in `build_indexes_streaming_multi`; do not
let a successful non-bitmap field hide a bitmap failure.

Both call sites must invalidate the `bm_cache` entry for `target_path` (not
just `path`) as part of publish, matching the existing "invalidate before
mutate" rule already used for the destructive path, so no reader-visible
stale mmap survives the rename.

Bitmap temporary names must be cleaned at the start of a rebuild and during
startup maintenance using the same narrowly-scoped sibling prefix and
directory scan as B-tree rebuild temps. Cleanup may remove only a temp whose
name matches the generated prefix; never remove a live `.bm` target.

## Task 4 — Gate Kf on index success and persist an abort decision

### Rev 2 notes

- **`apply_index_diff` null-safety (finding #1, downgraded but kept).**
  Traced the actual code: `build_index_key_from_record_into()` and
  `build_index_key_from_record()` both null-check their `record` parameter
  and return "absent" rather than dereferencing
  (`index.c:1121`, `index.c:1158`), so a `NULL` `a->new_value` does not crash
  today. But `apply_index_diff()` reaches that safety net implicitly and
  asymmetrically — the `old_value` branch has an explicit null guard
  : 0` guard before calling in, the `new_value` branch does not
  (`storage.c:1160` vs `storage.c:1165`) — and no existing caller passes
  `new_value == NULL` today (only `old_value == NULL`, for insert). Since
  Task 4's abort resolver is about to become the first caller to rely on the
  NULL-`new_value` direction (see below), make the contract explicit rather
  than resting on an implicit callee guarantee, and add a dedicated test.
- **Abort-sidecar read result (finding #2, confirmed).** The rev 1 gate
  logic (a binary `kf_batch_abort_read` success/failure check) treats every
  nonzero result as "no sidecar," which conflates "absent" (correct: replay
  forward) with "present but corrupt/truncated" (must fail closed, never
  replay forward past an indeterminate abort decision). Fixed with a
  three-state result.
- **Sidecar-only startup/gate state (finding #3, confirmed and sharpened).**
  `batch_id = w_start` (`slotcask.c:5617`) is deterministic per request
  offset, not a monotonic counter — batch id 0 is reused across *every*
  unrelated bulk request's first window on a given kf shard, not just across
  crash-retries of the same request. `kf_batch_marker_gate()`
  (`slotcask.c:2404`) and `marker_recovery_sweep_object()`
  (`slotcask.c:2480`) both enumerate only `*_marker.dat` via `sscanf`; a
  leftover `*_abort.dat` sidecar (marker already cleared, sidecar cleanup
  itself failed/crashed per the state diagram's last two steps) is invisible
  to both. Because `kf_batch_abort_write()` uses `O_CREAT|O_EXCL`, the *next*
  unrelated request that reuses that same batch id and later needs to write
  its own abort sidecar would hit `EEXIST` on a completely unrelated stale
  file and trip `kf_marker_fail_closed` — taking the shard down over a
  situation that was actually fully recoverable. Both the gate and the
  startup sweep must discover and resolve orphaned sidecars before a batch id
  is reused.

### Durable state machine

Keep the current batch intent marker as the durable **forward** record. Add a
same-directory abort sidecar, named:

```
%s/data/kf/%03x_batch_%u_abort.dat
```

Its contents must be a fixed, checksummed header containing a distinct magic,
version, `kf_shard`, `batch_id`, and marker-entry count. It is written with
`O_CREAT|O_EXCL`, `write_all`, `fsync(fd)`, `close`, and `fsync_dir(data/kf)`.
An existing valid sidecar is idempotent success; invalid/truncated content is
fail-closed.

The state transition is:

```
intent marker durable → apply indexes
                         ├─ success → publish+sync Kf → clear intent marker
                         └─ error   → abort sidecar durable → undo indexes
                                      → tombstone new segments → clear intent
                                      → clear abort sidecar → report error
```

A process death before the abort sidecar is durable has no completed client
response, so existing forward recovery remains valid for that indeterminate
request. A process death after the sidecar is durable must **never** replay
forward: recovery resolves the abort. The client is not sent an apply error
until the abort sidecar exists durably. A process death **between** clearing
the intent marker and clearing the abort sidecar (the last two steps) leaves
a sidecar with no matching marker — see the gate/sweep changes below for how
that state is discovered and resolved.

### Change

Add private helpers in `slotcask.c`:

```c
static void kf_batch_abort_path(char *buf, size_t cap, const char *data_dir,
                                int kf_shard, uint32_t batch_id);
static int kf_batch_abort_write(const char *data_dir, int kf_shard,
                                uint32_t batch_id, uint32_t marker_count);

typedef enum {
    KF_BATCH_ABORT_ABSENT = 0,   /* no sidecar file — forward replay applies */
    KF_BATCH_ABORT_VALID,        /* sidecar present, checksum/header valid */
    KF_BATCH_ABORT_CORRUPT,      /* sidecar present but invalid/truncated —
                                     fail closed, never treat as absent */
} KfBatchAbortStatus;
static KfBatchAbortStatus kf_batch_abort_read(const char *data_dir, int kf_shard,
                                              uint32_t batch_id, uint32_t *marker_count);
static int kf_batch_abort_clear(const char *data_dir, int kf_shard,
                                uint32_t batch_id);
```

Add one resolver that is used by both the current worker and startup/gate
recovery:

```c
/* Holds the kf-shard writer lock. Returns 0 only after every inverse index
 * update and every new-segment tombstone succeeds. On any error it leaves
 * both marker files in place and returns -1. It never publishes/repoints Kf
 * and never pushes a slot into the in-memory free pool. */
static int kf_batch_marker_abort_locked(const char *eff_root,
                                        const char *object,
                                        const char *data_dir,
                                        int kf_shard, SlotcaskKfHandle *kh,
                                        uint32_t batch_id);
```

`slotcask_bulk_upsert_in_kfshard()` does not receive `eff_root` or an object
name, so derive them once after its input/options validation and before it
enters the indexed window loop. The resolver call below must use these locals,
not undeclared identifiers:

```c
char abort_eff_root[PATH_MAX];
char abort_object[256];
split_data_dir(db->data_dir, abort_eff_root, sizeof(abort_eff_root),
               abort_object, sizeof(abort_object));
```

For each marker entry, the resolver loads the NEW segment and, for updates,
the OLD segment. It reverses the forward diff by calling the recovery
callback with the arguments swapped relative to a forward apply — i.e.
`apply_index_diff(old_value = NEW, new_value = OLD-or-NULL)` — so a field
whose forward diff would have inserted-new/deleted-old instead
inserts-old/deletes-new. For a pure insert being aborted, `OLD` is absent, so
this calls the diff with `new_value == NULL`, deleting whatever index entries
were written for `NEW` and inserting nothing. It checks the callback's return
value, then tombstones the NEW segment. Any callback, segment-open, or
tombstone error is fatal. It must not call `pool_push_free`: an unreachable
tombstoned slot may be reclaimed by normal maintenance, while duplicate
free-pool entries could corrupt later writes after a retry/crash.

Place the inverse-only failure seam in this resolver, immediately before its
recovery callback invocation; do not place it in `apply_index_diff`, where it
could fail the forward apply instead:

```c
if (durability_test_should_fail(DURABILITY_FAULT_INDEX_DIFF)) {
    errno = EIO;
    return -1;
}
if (g_recovery_index_diff_fn &&
    g_recovery_index_diff_fn(eff_root, object, kf_shard, (uint32_t)resolved_kf_slot,
                             hash, new_value, new_vlen, old_value, old_vlen,
                             err_buf, sizeof(err_buf)) != 0)
    return -1;
```

The tombstone step is a durability boundary, not merely a release-store.
Add a private `seg_write_flag_durable`/equivalent path used only by this
resolver. It keeps the segment mapping acquired, writes the flag, performs
`msync(MS_SYNC)` for the mapped range and `fdatasync` on the segment fd, then
releases the mapping. The variable-length tombstone branch must use the same
durable helper rather than duplicating a dirty mmap store. If any sync fails,
leave both marker files in place and fail closed. Only after every NEW segment
tombstone is durable may the resolver clear the intent marker and abort
sidecar. This closes the crash window in which ordinary `seg_write_flag`'s
dirty-cache update could be followed by evidence removal.

Only after every entry resolves may the resolver clear the intent marker and
then the abort sidecar, each with a directory sync. If either clear fails,
leave the abort sidecar and fail closed; a later resolver call is idempotent.
If the intent marker is already absent but an abort sidecar remains, validate
the sidecar and remove it as completed cleanup — never reuse a batch id while
an unvalidated sidecar exists.

**Make `apply_index_diff`'s null handling for both directions explicit**
(`storage.c:1143`), so the abort resolver's `new_value == NULL` call does not
rely on an implicit callee guarantee it doesn't own:

```c
if (arena) {
    int ro = a->old_value
        ? build_index_key_from_record_into(a->idx_ts, a->old_value,
                                           a->idx_fields[i],
                                           old_slot, INDEX_KEY_MAX, &old_len)
        : 0;
    /* a->new_value is NULL for an abort/delete-direction diff (recovery
       restoring to no-value, or to OLD via the resolver's argument swap
       above); a->old_value is NULL for a plain forward insert. Both must be
       explicit here, matching each other, rather than relying on
       build_index_key_from_record_into's internal !record guard. */
    int rn = a->new_value
        ? build_index_key_from_record_into(a->idx_ts, a->new_value,
                                           a->idx_fields[i],
                                           new_slot, INDEX_KEY_MAX, &new_len)
        : 0;
    have_old = (ro == 1);
    have_new = (rn == 1);
    old_buf = have_old ? old_slot : NULL;
    new_buf = have_new ? new_slot : NULL;
    if (ro == -1) {
        have_old = a->old_value
            ? build_index_key_from_record(a->idx_ts, a->old_value,
                                          a->idx_fields[i], &old_buf, &old_len)
            : 0;
        if (have_old) fb_bufs[n_fb++] = old_buf;
    }
    if (rn == -1) {
        have_new = a->new_value
            ? build_index_key_from_record(a->idx_ts, a->new_value,
                                          a->idx_fields[i], &new_buf, &new_len)
            : 0;
        if (have_new) fb_bufs[n_fb++] = new_buf;
    }
} else {
    have_old = a->old_value
        ? build_index_key_from_record(a->idx_ts, a->old_value,
                                      a->idx_fields[i], &old_buf, &old_len)
        : 0;
    have_new = a->new_value
        ? build_index_key_from_record(a->idx_ts, a->new_value,
                                      a->idx_fields[i], &new_buf, &new_len)
        : 0;
}
```

Change `kf_batch_marker_gate()` and startup recovery to select the resolver
via the three-state read:

```c
switch (kf_batch_abort_read(data_dir, shard, batch_id, &count)) {
case KF_BATCH_ABORT_ABSENT:
    rc = kf_batch_marker_replay_locked(eff_root, object, data_dir, shard,
                                       kh, batch_id);
    break;
case KF_BATCH_ABORT_VALID:
    rc = kf_batch_marker_abort_locked(eff_root, object, data_dir, shard,
                                      kh, batch_id);
    break;
case KF_BATCH_ABORT_CORRUPT: {
    char why[128];
    snprintf(why, sizeof(why), "corrupt bulk abort sidecar for batch %u",
             batch_id);
    kf_marker_fail_closed(data_dir, shard, why);
    rc = -1;
    break;
}
}
```

They must verify the sidecar count equals the complete marker record count.
`stat(marker_path).st_size` must be a multiple of `sizeof(KfMarkerSlot)`;
the reader must reject a short trailing record, checksum failure, extra
records beyond the sidecar count, or EOF before the declared count as
`KF_BATCH_ABORT_CORRUPT`. It must read exactly the declared count and then
confirm EOF. A valid prefix is never sufficient evidence for forward replay
or sidecar cleanup.
Any resolver failure invokes `kf_marker_fail_closed`; do not continue serving
an object whose Kf and indexes may disagree.

**Sidecar-aware gate and startup scanning.** Extend both
`kf_batch_marker_gate()`'s directory scan (`slotcask.c:2416-2431`) and
`marker_recovery_sweep_object()`'s scan (`slotcask.c:2492-2507`) to also match
`%x_batch_%u_abort.dat` and collect those `(kf_shard, batch_id)` pairs into a
second id set, alongside the existing marker-id set. After the existing
per-marker-id loop completes (which clears every marker it replays/aborts,
including its paired sidecar via the resolver above), process any collected
abort-sidecar id that has **no** corresponding marker: that state is only
reachable after `kf_batch_marker_abort_locked` already completed successfully
for that batch (the marker only clears once the resolver returns 0), so the
index-side undo is already done — resolve it as pure sidecar cleanup:
`kf_batch_abort_read` to revalidate, then `kf_batch_abort_clear` with a
directory sync. If revalidation instead returns `KF_BATCH_ABORT_CORRUPT`,
fail closed exactly as above rather than clearing it. This must run **before**
either function returns control to a caller that might pick a new batch id
for that shard, since `batch_id = w_start` makes id reuse the common case
(every request's first window), not an edge case.

In `slotcask_bulk_upsert_in_kfshard`, replace the old `keep_marker` behavior
for `apply_window` failure with:

```c
if (opts->apply_window(recs, apply_active, napply_active,
                       opts->bulk_hook_ctx) != 0) {
    if (kf_batch_abort_write(db->data_dir, kf_shard_id, batch_id,
                             (uint32_t)napply_active) != 0)
        kf_marker_fail_closed(db->data_dir, kf_shard_id,
                              "could not persist bulk abort decision");
    if (kf_batch_marker_abort_locked(abort_eff_root, abort_object, db->data_dir,
                                     kf_shard_id, &kh, batch_id) != 0)
        kf_marker_fail_closed(db->data_dir, kf_shard_id,
                              "could not complete bulk index abort");
    for (size_t a = 0; a < napply_active; a++)
        recs[apply_active[a]].status = -1;
    window_rejected = 1;
}
```

The Kf commit/repoint loop is inside `if (!window_rejected)`. The success
path's marker lifecycle and degraded Kf-sync behavior are unchanged. Because
the gate/sweep changes above guarantee no valid-but-orphaned sidecar survives
past a batch id's reuse, `kf_batch_abort_write`'s `O_CREAT|O_EXCL` collision
here indicates a genuine gate bug or double-abort, not an expected race —
failing loudly via `kf_marker_fail_closed` remains correct.

### Test first

Retain the two-field corrupt-shard daemon test from the superseded plan, then
add all of the following deterministic cases:

1. **Abort survives crash:** after `apply_window` returns failure and after
   `kf_batch_abort_write` completes its sidecar fsync and Kf-directory fsync,
   call `durability_test_pause(data_dir, "bulk-abort-after-sidecar-fsync")`,
   poll the existing marker, SIGKILL, and restart. Assert marker and sidecar
   are consumed by abort recovery; every new key is absent; the unaffected
   index has no orphan; and the seed record remains queryable. This phase is
   inserted immediately after sidecar durability, before inverse application.
2. **Undo failure fails closed:** use the countdown
   `DURABILITY_FAULT_INDEX_DIFF` seam for one inverse index callback failure.
   Assert both marker files remain and the daemon terminates/declines further
   writes rather than clearing them or publishing Kf. Restart with injection
   removed and assert abort recovery completes.
3. **Segment tombstone failure fails closed:** use
   `DURABILITY_FAULT_SEGMENT_FLAG` during the durable abort tombstone. Assert
   no `pool_push_free` occurs, both markers remain, and no new key is visible.
4. **Update abort restores old visibility:** seed an indexed record, induce a
   partial apply for an update, then assert the old value is returned through
   both primary lookup and indexed count, and the attempted new value has zero
   matches.
5. **New (finding #1): pure-insert abort direction.** Seed nothing, induce a
   partial apply for a fresh insert (no OLD segment), and directly assert
   `apply_index_diff` is invoked with `new_value == NULL` in this path (via
   the resolver), that no index entry is inserted for the aborted key, and
   that the entries built for `old_value = NEW` are deleted. This exercises
   the null-`new_value` direction `apply_index_diff` has never had a real
   caller for before this change.
6. **New (finding #2): corrupt sidecar/marker fails closed, not open.**
   Truncate a valid abort sidecar to a partial header and separately truncate
   a marker file to a partial `KfMarkerSlot` (simulating a crash mid-write),
   then run recovery. Assert `KF_BATCH_ABORT_CORRUPT` is returned, recovery
   invokes `kf_marker_fail_closed` rather than replaying forward, and both
   files are left in place for operator investigation.
7. **New (finding #3): orphaned sidecar across unrelated requests.** Run one
   bulk request to a failure that leaves an abort sidecar for batch id 0 on
   some kf shard, then — instead of restarting — force-remove only the
   marker file (simulating a crash between "clear intent marker" and "clear
   abort sidecar") so the sidecar is orphaned, then issue a second, entirely
   unrelated bulk request whose first window lands on batch id 0 for the same
   shard. Assert the gate discovers and cleans the orphaned sidecar before
   the second request proceeds, and that the second request's own eventual
   abort sidecar write (if it also fails) does not hit a spurious `EEXIST`
   against the first request's leftover file.

Existing `bulk-window-applied` recovery coverage remains unchanged for a
successful apply interrupted before Kf publish. The new abort-specific pause
must be distinct, because a failed apply with durable abort intent must take
the opposite recovery branch.

Add every Task 4 daemon/restart test to
`src/test/cases/test_durability_ordering.c`, immediately before the quoted
registration anchor `TEST_REGISTER("test-msync-range", test_msync_range_raw_fails_on_main)`. Add the
in-process marker/sidecar parser and fault-seam tests to
`src/test/cases/test_slotcask_v2_crash.c`, immediately before
`TEST_REGISTER("test-slotcask-v2-crash", test_slotcask_v2_crash_run)`. Reuse
the existing `append_durability_pause_config`, `wait_for_path`, and
`test_env_stop_keep` helpers; each test creates its own object name and port.

## Task 5 — Observability and documentation

Log the B-tree merge failure with field and shard in `idx_build_field_worker`.
Add error logs for: abort-sidecar write/validation, corrupt-sidecar
discovery (finding #2), inverse-index failure, segment-tombstone failure,
atomic B-tree/trigram publish failure including the post-rename-fsync-only
case (finding #4), atomic bitmap publish failure (Task 3), and orphaned
sidecar cleanup at gate/startup time (finding #3) — including the target
path and `errno` where applicable.

Update `src/db/slotcask.h`'s `apply_window` contract to state the abort state
machine and the swapped-argument convention `kf_batch_marker_abort_locked`
uses when calling `apply_index_diff`. Update the storage-model crash-safety
documentation: normal crash recovery may finish an indeterminate request
forward; a durable index-apply failure decision is always recovered by abort
and is never replayed forward; a durable abort sidecar is discoverable and
resolvable independent of whether its paired marker file still exists.

## Verification

- Run every new test red on the base branch and record the expected failure.
- Build with `SKIP_TESTS=1 ./build.sh` and run
  `./build/bin/shard-db-test run-all`.
- Run ASan and TSan as required by `AGENTS.md`, including the new crash,
  publish (B-tree and bitmap), marker, durability, and slotcask-v2 tests.
- Review the raw diff for lock ordering: target mutation gate → index-file
  cache invalidation (B-tree, trigram, and bitmap), Kf writer lock →
  marker/abort resolver, and no index callback may recursively acquire the
  same Kf writer lock.
- Leave the implementation uncommitted for raw-diff review.

## Execution rules

- Branch from `main` only after this draft is explicitly approved.
- Execute in order: Task 1, Task 2, Task 3, Task 4, Task 5.
- If an anchor differs, write `PLAN_NOTES.md` and stop; do not reinterpret
  the plan.
- If atomic publishing exposes a non-B-tree/bitmap index writer that cannot
  stage its output without changing an on-disk format, stop and obtain a
  revised plan.

## Pinned implementation anchors

The implementation must edit the code at these quoted anchors; line numbers
are informational and must not be substituted for the quoted text:

| Work | Current source anchor |
|---|---|
| B-tree extraction/build/merge | `static BtEntry *bt_extract_all(`, `static int btree_bulk_build_locked(`, `int btree_bulk_merge(`, and `BtStreamBuilder *bt_stream_build_open(` in `src/db/btree.c` |
| B-tree streaming publish | `merge_spills_into_index(` in `src/db/index.c` and `bt_stream_build_finish(` in `src/db/btree.c` |
| Singular add-index result propagation | `if (type == IT_BITMAP) {` in `src/db/index.c`, followed by the `build_bitmap_pass(` / `build_trigram_pass(` / `build_btree_streaming(` calls |
| Multi-field result propagation | `build_indexes_streaming_multi(` and `int bm_rc = resolve_bitmaps(` in `src/db/index.c`, plus `cmd_add_indexes(` |
| Bitmap worker failure propagation | `static int bm_rebuild_cb(`, `static void *bm_shard_walk_worker(`, and `parallel_for(bm_shard_walk_worker` in `src/db/index.c` |
| Bitmap spill failure propagation | `if (slot >= 0)` inside `resolve_bitmaps(` in `src/db/index.c` |
| Index diff null handling | `static int apply_index_diff(` in `src/db/storage.c` |
| Abort marker resolver/gate | `static void kf_marker_fail_closed(`, `static int kf_batch_marker_gate(`, and `marker_recovery_sweep_object(` in `src/db/slotcask.c` |
| Segment abort durability | `static int seg_write_flag(` and `slotcask_tombstone_and_push_back(` in `src/db/slotcask.c` |
| Bulk failure transition | `if (opts->apply_window(` inside `slotcask_bulk_upsert_in_kfshard` in `src/db/slotcask.c` |
| Existing cross-process pause | `void durability_test_pause(` in `src/db/durability.c` |

The new common helpers are anchored at the existing durability utility
declarations adjacent to `durability_test_pause`; their exact definitions
and declarations are part of Task 2, not an executor choice. Before coding,
the executor must run one final `rg` audit for each anchor above and record
any changed signature in `PLAN_NOTES.md`; no implementation may proceed from
line numbers alone.

The caller audit is now closed by the explicit propagation rules above: all
direct force-build callers, all online `btree_bulk_*` callers, both bitmap
call sites, the recovery callback, both marker scanners, and the segment
tombstone path have specified behavior for each result state. The only
remaining pre-execution action is mechanical anchor verification; it does not
require a product decision or reinterpretation of this plan.
