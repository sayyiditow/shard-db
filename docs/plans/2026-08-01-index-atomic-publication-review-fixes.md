# Corrective plan — atomic index publication review fixes

**Status: COMPLETE — landed in main as part of commit 846e78d (PR #277,
2026-08-02); every fix below is included in that squash commit.**

**Execution base (historical):** the then-uncommitted worktree on
`fix/index-atomic-publication`, after the implementation of
`2026-07-31-index-atomic-publication.md`. That diff was committed as part of
846e78d; the imperative below no longer applies.

## Goal

Close every finding from the 2026-08-01 raw-diff review and complete the
regressions and verification evidence required by `CORE-PROCESS.md` and the
original plan.

This plan deliberately replaces the original process-global publication gate.
That gate introduced this lock cycle:

~~~text
reader holds cache entry A
  -> publisher holds global publication gate and waits for entry A
  -> reader attempts entry B and waits for the global gate
~~~

The replacement is a publication-generation protocol. Publication never holds
a global gate and never blocks on a live target cache entry. A successful
rename advances a generation before publication returns. Cache entries record
the generation at which their open inode was validated. The first acquire after
a generation change compares the cached fd's `(st_dev, st_ino)` with the
current path. A mismatched entry is retired non-blockingly when possible;
otherwise that acquire opens the current path uncached. An acquire overlapping
publication may finish on the old inode, but an acquire beginning after
publication completes cannot.

## Binding invariants

1. Publication cannot deadlock with a reader retaining one or more B-tree or
   bitmap handles, including handles for different shards and fields.
2. An acquire overlapping publication may return the old or new complete
   inode. An acquire beginning after publication completes returns the new
   inode.
3. Publication must not wait for a live target cache entry while holding any
   lock needed by an acquire of another target.
4. Only `ENOENT` means a spill file is benignly absent. Every other open/read/
   decode/setup failure prevents publication of the affected build.
5. No command may write `index.conf` unless schema loading, descriptor
   allocation, field resolution, and index materialisation reached a non-failed
   result.
6. Metadata replacement is itself atomic. A failed metadata write leaves the
   prior `index.conf` intact; a post-rename directory-sync failure is reported
   as durability-unconfirmed.
7. Obsolete cleanup matches both the resolved index type and the exact file
   extension. A B-tree/trigram cleanup pass cannot unlink `.bm` files.
8. Every bitmap temporary is synced and closed through checked APIs before
   rename. Close, unmap, cache-discard, and unlock failures are propagated.
9. Error text must not claim all old shards were untouched after a per-shard
   publication failure. Atomicity remains per shard, not per multi-shard index.
10. Crash-abandoned `.rebuild-*` siblings are inert and removed during the next
    daemon or embedded startup before requests/builders begin.
11. The final build emits no new warnings and the diff contains no unused test
    hooks, stale force-unlink comments, fixed test paths, or unrelated fixture
    changes.

## Files in scope

- `src/db/shard_db_internal.h`
- `src/db/types.h`
- `src/db/durability.c`
- `src/db/btree.c`, `src/db/btree.h`
- `src/db/bitmap.c`, `src/db/bitmap.h`
- `src/db/index.c`
- `src/db/query_bulk.c`
- `src/db/server.c`, `src/db/embedded.c`
- `src/test/fixtures.c`
- `src/test/cases/test_btree.c`
- `src/test/cases/test_bt_cache_writer_starvation.c`
- `src/test/cases/test_bitmap_index.c`
- `src/test/cases/test_reindex_spill_collision.c`
- `src/test/cases/test_coverity_disk_corruption_btree.c`
- `src/test/cases/test_trigram_index.c`, which owns the current singular and
  plural add/remove-index metadata protocol assertions
- `docs/concepts/indexes.md`

No dependency or on-disk-format change is permitted.

## Task 1 — replace the deadlocking publication gates

### Root cause

At the unique anchors `static int bt_acquire(BtFile *bt, const char *path, int
writer)` and `BitmapShard *bm_open(const char *path, int slots, int create,`,
the gate reader is released before the returned handle releases its cache-entry
rwlock. Publishers take the inverse order: gate writer, then blocking cache
invalidation. Existing query paths retain several handles while acquiring the
next, so the documented gate ordering is not globally enforceable.

### Test first

1. In `test_bt_cache_writer_starvation.c`, immediately before its final
   `TEST_REGISTER` anchor, add a bounded-child regression which:
   - creates two cached B-tree targets A and B;
   - opens and retains a range iterator on A;
   - pauses publication of A at `bt-publish-before-rename`;
   - starts the publisher and then acquires/uses B while A remains retained;
   - requires B to complete before releasing A;
   - releases A, lets publication finish, and proves a new A acquire sees only
     the replacement contents.
   The current implementation must time out in the child for the expected lock
   cycle. The fixed implementation must exit normally within the bound.
2. At the same anchor, add an online-bulk-versus-reindex case using the real
   public paths and retaining multiple `BtRangeIter`s. Both operations must
   complete and all expected keys must remain queryable.
3. In `test_bitmap_index.c`, immediately before
   `TEST_REGISTER("test-bitmap-index",`, add the bitmap counterpart: retain A,
   publish A, acquire/use B before releasing A, then prove a post-publication A
   acquire sees the new bitmap only.
4. Keep each child timeout at 30 seconds or less. Create all paths underneath a
   per-test `mkdtemp` directory; do not use a fixed `/tmp` filename.

Capture the red output before changing production code.

### Implementation

At the cache-entry definitions in `shard_db_internal.h`, add a generation field
to both cache types:

~~~c
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    _Atomic int used;
    _Atomic int dirty;
    _Atomic uint64_t dirty_since_ms;
    _Atomic uint64_t validated_publish_generation;
    uint64_t last_access;
} BtCacheEntry;

typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    _Atomic int used;
    _Atomic int dirty;
    _Atomic uint64_t dirty_since_ms;
    _Atomic uint64_t validated_publish_generation;
    uint64_t last_access;
} BmCacheEntry;
~~~

At the current publication-gate globals in `btree.c` and `bitmap.c`, remove the
gate, `pthread_once`, writer-preference setup, pending-reader counters, and their
header declarations. Replace each gate with one process-lifetime atomic:

~~~c
static _Atomic uint64_t g_bt_publish_generation = 1;
~~~

~~~c
static _Atomic uint64_t g_bm_publish_generation = 1;
~~~

On every cache-miss path, capture the matching generation immediately before
opening the pathname—not at cache install time:

~~~c
uint64_t opened_generation =
    atomic_load_explicit(&g_bt_publish_generation, memory_order_acquire);
if (bt_open_file(path, writer, &fd, &map, &sz) < 0) {
    /* existing cleanup/return */
}
~~~

At the unique cache-install assignment `e->used = BT_CACHE_LIVE;`, store that
captured value:

~~~c
atomic_store_explicit(&e->validated_publish_generation,
                      opened_generation, memory_order_release);
~~~

Use the corresponding capture around `bm_file_open_mmap` and store at the
bitmap `e->used = 1;` install. This ordering is required: a reader can open the
old inode, lose the race to rename, and install only afterwards. Loading the
generation at install would falsely bless that old inode as current. Reset the
generation field to zero whenever an entry is detached.

Immediately before `static int bt_acquire_impl(` and `static BitmapShard
*bm_open_impl(`, add matching inode validators. They do not mutate a cache
entry and are called only while its rwlock is held:

~~~c
static int same_open_inode(int fd, const char *path) {
    struct stat opened;
    struct stat current;
    if (fstat(fd, &opened) != 0) return 0;
    if (stat(path, &current) != 0) return 0;
    return opened.st_dev == current.st_dev && opened.st_ino == current.st_ino;
}
~~~

Give these helpers component-specific names (`bt_same_open_inode` and
`bm_same_open_inode`) because the translation units are separate.

Add non-blocking invalidators beside the existing blocking invalidators. They
must use the existing `*_cache_drop_slot(..., wait=0, ...)` machinery and
return `1` when detached, `0` when absent/busy, and `-1` on a real cleanup
error. They must never wait on a cache-entry rwlock:

~~~c
static int btree_cache_invalidate_nowait(const char *path) {
    int rc = 0;
    pthread_mutex_lock(&bt_cache_lock);
    if (bt_cache) {
        int found = 0;
        int slot = bt_cache_probe(path, &found);
        if (found) {
            int fd = -1;
            uint8_t *map = NULL;
            size_t map_size = 0;
            rc = bt_cache_evict_slot(slot, CACHE_DROP_DISCARD, 0,
                                     &fd, &map, &map_size);
            pthread_mutex_unlock(&bt_cache_lock);
            if (rc > 0) bt_dispose_mapping(fd, map, map_size);
            return rc;
        }
    }
    pthread_mutex_unlock(&bt_cache_lock);
    return rc;
}
~~~

Implement the bitmap equivalent through `bm_cache_drop_slot(..., wait=0)`.
Do not expose either helper publicly.

At each confirmed cache-hit block, before handing the cached mapping to the
caller, compare its validated generation with the current generation. If it
differs:

~~~c
uint64_t current_generation =
    atomic_load_explicit(&g_bt_publish_generation, memory_order_acquire);
uint64_t validated_generation = atomic_load_explicit(
    &e->validated_publish_generation, memory_order_acquire);
if (validated_generation != current_generation) {
    if (!bt_same_open_inode(e->fd, path)) {
        pthread_rwlock_unlock(lock);
        if (btree_cache_invalidate_nowait(path) > 0)
            goto retry_bt_acquire;
        bt->slot = -1;
        return bt_open_file(path, writer, &bt->fd, &bt->map, &bt->map_size);
    }
    atomic_store_explicit(&e->validated_publish_generation,
                          current_generation, memory_order_release);
}
~~~

Use the bitmap equivalent. On a busy stale bitmap entry, release its entry
lock, open the current path uncached with `bm_file_open_mmap`, populate a
`BitmapShard` with `slot = -1`, and return it. Do not block trying to retire a
stale target.

Change the common publication helper at the exact anchor
`int durability_publish_replace(const char *target, const char *tmp_path)` so
the cache wrapper can advance its generation immediately after rename and
before the parent-directory fsync:

~~~c
typedef void (*durability_after_rename_fn)(const char *target, void *ctx);

int durability_publish_replace(const char *target, const char *tmp_path,
                               durability_after_rename_fn after_rename,
                               void *after_rename_ctx) {
    if (fsync_file_path(tmp_path) != 0) return -1;
    if (rename(tmp_path, target) != 0) return -1;
    if (after_rename) after_rename(target, after_rename_ctx);
    if (fsync_parent_dir(target) != 0) return 1;
    return 0;
}
~~~

Declare the callback type and new signature in `types.h`. Update every caller
found by `rg "durability_publish_replace\\(" src`—there must be exactly the
B-tree wrapper, bitmap wrapper, and any metadata helper introduced by Task 5.

Rename `bt_publish_replace_locked` to `bt_publish_replace` and update both of
its call sites; no lock remains for the old name to describe. Replace its body
and the body of `bm_publish_replace`.
The wrappers may synchronously invalidate the generated temporary path because
no caller may retain a temp handle after materialisation, but they must never
block invalidating the live target:

~~~c
static void bt_after_rename(const char *target, void *ctx) {
    (void)ctx;
    atomic_fetch_add_explicit(&g_bt_publish_generation, 1,
                              memory_order_acq_rel);
    (void)btree_cache_invalidate_nowait(target);
}

static bt_publish_result bt_publish_replace_locked(const char *target,
                                                   const char *tmp_path) {
    char parent[PATH_MAX];
    if (parent_dir_copy(target, parent, sizeof(parent)) != 0)
        return BT_PUBLISH_PRE_RENAME_FAILED;
    btree_cache_invalidate(tmp_path);
    durability_test_pause(parent, "bt-publish-before-rename");
    int rc = durability_publish_replace(target, tmp_path,
                                        bt_after_rename, NULL);
    if (rc < 0) return BT_PUBLISH_PRE_RENAME_FAILED;
    if (rc > 0) return BT_PUBLISH_POST_RENAME_FSYNC_FAILED;
    return BT_PUBLISH_OK;
}
~~~

Use the same complete block with `bm_after_rename`, bitmap cache helpers, and
bitmap result constants for `bm_publish_replace`.

Remove `bt_acquire` and `bm_open` gate wrappers entirely: keep the public/private
function names but make them direct calls to `bt_acquire_impl` and
`bm_open_impl`. Update comments to state the generation/inode contract and the
only remaining lock order.

### Green proof

Temporarily restore either old global gate around acquire/publish, rebuild, and
record the new bounded deadlock test timing out. Restore the generation fix,
rebuild, and record all Task 1 tests passing.

## Task 2 — make every setup and spill failure fail closed

### Test first

In `test_reindex_spill_collision.c`, before its final registration anchor, add:

1. A deterministic non-`ENOENT` spill-open failure. Use a TEST_BUILD injection
   seam immediately before the real `open(path, O_RDONLY)` so the test does not
   depend on chmod behavior when run as root. Assert the request fails and all
   pre-existing target shards remain byte-identical.
2. An inaccessible/missing stream-directory enumeration failure. Assert the
   request fails before worker allocation/publication and every prior target is
   byte-identical.
3. A descriptor/setup failure through the plural public add-index command.
   Temporarily make `fields.conf` unreadable or use a narrow TEST_BUILD
   allocation-failure seam. Assert a nonzero response, unchanged `index.conf`,
   and unchanged target files.

### Implementation

At the unique block:

~~~c
int fd = open(path, O_RDONLY);
if (fd < 0) continue;
~~~

replace it with:

~~~c
int fd = index_spill_open(path);
if (fd < 0) {
    int saved_errno = errno;
    if (saved_errno == ENOENT) continue;
    LOG_ERROR(LOG_SUB_REINDEX,
              "merge_spills_into_index: open(%s) failed: %s",
              path, strerror(saved_errno));
    result = (index_build_result){
        .status = INDEX_BUILD_FAILED,
        .all_requested_shards_published = 0,
        .error_errno = saved_errno,
    };
    goto cleanup;
}
~~~

`index_spill_open()` is a static wrapper that calls `open` normally and exposes
only a one-shot errno injection under `TEST_BUILD`. Reset the injection in test
cleanup so process-wide state cannot leak into sequential `run-all --jobs 1`.

At `cmd_add_indexes`, immediately after loading schema and typed schema, reject
invalid setup before parsing/promoting fields:

~~~c
Schema sch = load_schema(db_root, object);
TypedSchema *ts_for_idx = load_typed_schema(db_root, object);
if (sch.splits <= 0 || sch.streams <= 0 || !ts_for_idx) {
    OUT("{\"error\":\"cannot load object schema for index build\"}\n");
    return -1;
}
~~~

Immediately after descriptor allocation, reject failure rather than allowing
`n_desc == 0` to masquerade as success:

~~~c
MFFieldDesc *descs = calloc((size_t)total_fields, sizeof(*descs));
if (!descs) {
    OUT("{\"error\":\"cannot allocate index build descriptors\"}\n");
    return -1;
}
~~~

Resolve every requested field before the build. A missing simple field or a
composite with any unresolved component is a command error; do not silently
skip it and later write metadata. Preserve the existing skip-if-already-indexed
behavior only after successful validation.

Keep `enumerate_segments`'s checked empty-versus-failure contract, but add the
missing regression above and verify `readdir`'s terminal `errno` is checked:

~~~c
errno = 0;
while ((entry = readdir(dir)) != NULL) {
    /* existing validated .dat collection */
}
if (errno != 0) {
    int saved_errno = errno;
    closedir(dir);
    free(segs);
    errno = saved_errno;
    return -1;
}
~~~

Use designated initializers for every `index_build_result`. Replace every
positional two-field initializer found by
`rg -n "index_build_result\\)\\{|index_build_result .* = \\{" src/db/index.c`
with all three named fields. After the replacement, that search must show no
positional initializer and a normal build must emit no
`-Wmissing-field-initializers` warnings.

Immediately after the `index_build_result` type, add constructors and use them
where a complete result is needed without repeating positional literals:

~~~c
static index_build_result index_build_ok(void) {
    return (index_build_result){
        .status = INDEX_BUILD_OK,
        .all_requested_shards_published = 1,
        .error_errno = 0,
    };
}

static index_build_result index_build_failed(int error_errno) {
    return (index_build_result){
        .status = INDEX_BUILD_FAILED,
        .all_requested_shards_published = 0,
        .error_errno = error_errno ? error_errno : EIO,
    };
}

static index_build_result
index_build_durability_unconfirmed(int error_errno) {
    return (index_build_result){
        .status = INDEX_BUILD_DURABILITY_UNCONFIRMED,
        .all_requested_shards_published = 1,
        .error_errno = error_errno ? error_errno : EIO,
    };
}
~~~

On any failed per-shard result, replace “left untouched” command text with the
truthful contract:

~~~c
OUT("{\"error\":\"index build failed; index metadata was not changed; "
    "one or more shards may already have been published\"}\n");
~~~

For a failure proven to occur before any rename, the more specific “existing
shards left untouched” wording is allowed only when the result type explicitly
records zero published shards. Do not infer this from
`all_requested_shards_published == 0` because that flag also represents partial
publication.

### Green proof

For each new case, record base failure, failure with its specific fix removed,
and pass after restoration.

## Task 3 — make obsolete cleanup extension- and type-exact

### Test first

In `test_bitmap_index.c`, add a public reindex regression for one field having
both bitmap and B-tree/trigram index files. Use an object with `splits` greater
than `index_splits_for(splits)`. Seed every valid `.bm` shard, run reindex, and
assert:

- every `.bm` shard `0 .. splits-1` remains present and queryable;
- only obsolete `.idx`/`.tg` shards at or above `tree_splits` are removed;
- unrelated extensions and noncanonical names are not unlinked;
- cache invalidation uses bitmap for `.bm` and B-tree for `.idx`/`.tg`.

The current implementation must lose bitmap shards at or above `tree_splits`.

### Implementation

At `static void reindex_cleanup_obsolete(`, derive the exact extension and
shard count from each descriptor, then compare against a canonical filename:

~~~c
const char *extension;
int live_shards;
switch (descs[i].type) {
    case MF_BITMAP:
        extension = ".bm";
        live_shards = bitmap_splits;
        break;
    case STREAM_TRIGRAM:
        extension = ".tg";
        live_shards = tree_splits;
        break;
    case STREAM_BTREE:
        extension = ".idx";
        live_shards = tree_splits;
        break;
    default:
        continue;
}

struct dirent *entry;
for (;;) {
    errno = 0;
    entry = readdir(d);
    if (!entry) {
        if (errno != 0)
            LOG_WARN(LOG_SUB_REINDEX,
                     "reindex cleanup: readdir(%s) failed: %s",
                     fdir, strerror(errno));
        break;
    }
    size_t name_len = strlen(entry->d_name);
    size_t extension_len = strlen(extension);
    if (name_len != 3 + extension_len) continue;
    if (strcmp(entry->d_name + 3, extension) != 0) continue;

    char shard_text[4] = {
        entry->d_name[0], entry->d_name[1], entry->d_name[2], '\0'
    };
    char *end = NULL;
    unsigned long shard = strtoul(shard_text, &end, 16);
    if (end != shard_text + 3 || shard > INT_MAX) continue;
    if ((int)shard < live_shards) continue;

    char full_path[PATH_MAX];
    snprintf(full_path, sizeof(full_path), "%s/%s", fdir, entry->d_name);
    if (descs[i].type == MF_BITMAP)
        bm_cache_invalidate(full_path);
    else
        btree_cache_invalidate(full_path);
    if (unlinkat(dfd, entry->d_name, 0) != 0 && errno != ENOENT) {
        LOG_WARN(LOG_SUB_REINDEX,
                 "reindex cleanup: unlink %s failed: %s",
                 full_path, strerror(errno));
    }
}
~~~

Invalidate before `unlinkat`, not after. Do not use `sscanf` with an extension-
agnostic pattern. Require an exact extension match and a complete three-digit
hex parse.

Update the stale comment beginning `wipe stale on-disk idx files` so it describes
post-publication, type-specific cleanup.

## Task 4 — propagate bitmap close and cache-discard failures

### Test first

In `test_bitmap_index.c` and `test_reindex_spill_collision.c`, add deterministic
one-shot TEST_BUILD failures for:

- temporary writer unlock/close during `build_bitmap_pass`;
- temporary cache discard/unmap/close before publication;
- resolver close/discard after a fully materialised temporary;
- malformed bitmap spill paused at the required phase
  `bm-resolve-before-open`.

Each pre-rename failure must return nonzero, unlink only generated temporary
siblings, leave every live `.bm` target byte-identical, and reset its injection
state during cleanup.

### Implementation

Do not change every existing `bm_close` caller to handle an `int`. Preserve the
public convenience wrapper and add checked variants in `bitmap.h`:

~~~c
int bm_close_checked(BitmapShard *bm);
void bm_close(BitmapShard *bm);
int bm_cache_invalidate_checked(const char *path);
~~~

Implement `bm_close_checked` as the single owner of release logic. Preserve the
first failure from `pthread_rwlock_unlock`, `munmap`, or `close`, free the handle
exactly once, and return `-1` with that errno. `bm_close` becomes:

~~~c
void bm_close(BitmapShard *bm) {
    (void)bm_close_checked(bm);
}
~~~

Refactor `bm_cache_drop_slot` so checked invalidation preserves and returns the
first `durability_flush_dirty`, `munmap`, or `close` error after detaching only
what can safely be detached. Keep `bm_cache_invalidate` as a compatibility
wrapper around the checked form.

At `bm_shard_walk_worker` and `resolve_bitmaps`, replace temp-handle calls with:

~~~c
if (bm_sync(bm) != 0) {
    record_first_bitmap_failure(ctx, errno);
}
if (bm_close_checked(bm) != 0) {
    record_first_bitmap_failure(ctx, errno);
}
bm = NULL;
if (!ctx->failed && bm_cache_invalidate_checked(tmp_path) != 0) {
    record_first_bitmap_failure(ctx, errno);
}
~~~

Only closed and successfully discarded temps may enter the publication loop.
Add the missing pause immediately before resolver temp writers are opened:

~~~c
durability_test_pause(index_dir, "bm-resolve-before-open");
~~~

Do not publish any field temp if materialisation/close/discard failed before its
first rename.

## Task 5 — publish `index.conf` atomically and report exact state

### Test first

In the existing singular/plural index command test, add deterministic failures
for metadata temp open, write, flush, close, rename, and post-rename parent
fsync. For singular and plural commands assert:

- a pre-rename metadata failure returns nonzero and preserves the previous
  `index.conf` byte-for-byte;
- already-published index shards remain valid and the response says shards were
  published but metadata was not changed;
- a metadata post-rename fsync failure returns the explicit durability warning
  and the new canonical metadata remains readable;
- setup/descriptor failure never changes metadata;
- retry is idempotent and produces no duplicate lines.

### Implementation

At the first metadata-writing helper anchor in `index.c`, add a private atomic
writer shared by singular and plural commands:

~~~c
static index_build_result publish_index_conf(const char *conf_path,
                                             const char *contents,
                                             size_t contents_len) {
    char parent[PATH_MAX];
    char tmp_path[PATH_MAX];
    if (parent_dir_copy(conf_path, parent, sizeof(parent)) != 0)
        return index_build_failed(errno);
    mkdirp(parent);
    int n = snprintf(tmp_path, sizeof(tmp_path),
                     "%s/.index-conf-XXXXXX", parent);
    if (n < 0 || n >= (int)sizeof(tmp_path))
        return index_build_failed(ENAMETOOLONG);

    int fd = mkstemp(tmp_path);
    if (fd < 0) return index_build_failed(errno);
    int saved_errno = 0;
    if (write_all(fd, contents, contents_len) != 0)
        saved_errno = errno;
    if (!saved_errno && close(fd) != 0)
        saved_errno = errno;
    else if (saved_errno)
        close(fd);
    if (saved_errno) {
        unlink(tmp_path);
        return index_build_failed(saved_errno);
    }

    int rc = durability_publish_replace(conf_path, tmp_path, NULL, NULL);
    if (rc < 0) {
        saved_errno = errno;
        unlink(tmp_path);
        return index_build_failed(saved_errno);
    }
    if (rc > 0)
        return index_build_durability_unconfirmed(errno);
    return index_build_ok();
}
~~~

Use the repository's existing checked full-write helper if one exists; otherwise
add the shown EINTR/short-write-safe `write_all` as a private static helper.
Build the complete desired file in memory first:

- singular add: read the current file, preserve all lines, append the canonical
  line only if absent;
- plural append: preserve current lines and append each absent canonical line;
- promotion rewrite: generate the complete canonical set required by the
  command, including all previously retained lines that are not being replaced.

Reject any read error other than `ENOENT`. Check every allocation and bounds
calculation against `QUERY_BUFFER_MB` or a small explicit metadata maximum.
Remove the existing unchecked `fopen`/`fprintf`/`fclose` blocks and the separate
`fsync_file_path(conf_path)` calls.

Fold metadata durability status with build durability status. On build failure,
do not call the metadata helper. On metadata pre-rename failure after shard
publication, use truthful output:

~~~c
OUT("{\"error\":\"index shards published but index metadata update failed; "
    "retry add-index\"}\n");
~~~

On post-rename directory-sync failure:

~~~c
OUT("{\"warning\":\"index and metadata published but directory durability "
    "is unconfirmed\"}\n");
~~~

## Task 6 — finish diagnostics, temp recovery, path safety, and cleanup

### Test first

1. Extend the online bulk publication test to inject pre-rename and post-rename
   failures and assert logs contain `field`, `shard`, `target`, exact `state`,
   and `errno`.
2. In `test_reindex_spill_collision.c`, leave `.rebuild-*` siblings under
   B-tree, trigram, and bitmap field directories, restart the daemon, and prove
   startup removes only regular generated siblings—not live index files,
   symlinks, or unrelated dotfiles. Add an embedded-open counterpart if daemon
   and embedded startup do not share one helper.
3. Convert `test_btree.c`'s fixed publication and ordinary B-tree paths to one
   per-test `mkdtemp` root, and assert cleanup.

### Implementation

At `idx_build_field_worker`, map the publication result/errno to an exact state.
If `btree_bulk_merge` remains an `int` API, add a thread-local or explicit
out-parameter accessor that distinguishes pre-rename failure from
post-rename durability warning; do not label both `bulk-merge-failed`:

~~~c
LOG_ERROR(LOG_SUB_QUERY,
          "idx_build_field_worker: field=%s shard=%d target=%s "
          "state=%s errno=%d (%s)",
          fa->field, s, path, state_name, saved_errno,
          strerror(saved_errno));
~~~

Ensure the empty B-tree branch logs the same post-rename durability warning as
the non-empty branch before returning nonzero.

Replace both new `mkdirp(dirname_of(path));` calls in B-tree builders with local
buffers:

~~~c
char parent[PATH_MAX];
if (parent_dir_copy(path, parent, sizeof(parent)) != 0) return -1;
mkdirp(parent);
~~~

Use the corresponding disposal path in `bt_stream_build_open`; do not call
thread-unsafe `dirname_of()` from parallel builders.

Add one shared startup helper declared in `types.h`:

~~~c
int index_rebuild_temp_sweep(const char *db_root);
~~~

Implement it with `openat`/`fdopendir`/`fstatat(AT_SYMLINK_NOFOLLOW)` and
`unlinkat`. Remove regular `.index-conf-*` files directly under each
`<db_root>/<tenant>/<object>/indexes/` directory and regular `.rebuild-*` files
under its field subdirectories. Never follow or unlink symlinks. Treat `ENOENT`
as benign and propagate all other traversal/unlink errors. Call it from daemon
and embedded startup before caches, builders, background threads, or requests
can use index paths.

In `src/test/fixtures.c`, restore the values promised by the existing comment:

~~~c
"export WORKERS=16\n"
"export IO_THREADS=8\n"
~~~

Remove the unused publish-gate TEST_BUILD counters and declarations. Update all
comments which still say force “wipe”, “unlink”, or “left untouched”; comments
must describe the per-shard publication invariant and result contract, not a
caller. Run:

~~~bash
rtk proxy rg -n "publish_reader_pending|force.*wipe|force.*unlink|left untouched|TODO|FIXME|DEBUG" src/db src/test
rtk git diff --check
~~~

Every hit must be either removed or documented as unrelated pre-existing code.

## Task 7 — add every missing original-plan regression

This task is mandatory even if Tasks 1–6 already exercise part of the same
behavior.

### `test_coverity_disk_corruption_btree.c`

Extend both new corruption tests. First prove the valid seed has the complete
range result. After deliberately corrupting it, snapshot the full corrupted
file bytes and inode immediately before invoking the merge. Then assert:

- merge returns nonzero within a bound;
- original inode remains;
- every file byte is identical to the immediate pre-call corrupted snapshot;
- the proposed new key is absent using a bounds-checked raw leaf/value walker
  that does not require the intentionally corrupted root/chain to be valid.

This explicitly corrects the original plan's impossible wording that a range
query must remain complete *after the test itself corrupts the live target*.
The meaningful fail-closed contract is that `btree_bulk_merge` makes no further
change and adds no entry. Do not weaken the corruption to make the postcondition
easy.

### `test_btree.c`

Keep direct pre-rename fsync, post-rename parent-fsync, and valid-empty
publication coverage. Add explicit warning-state assertions and remove every
fixed path.

### `test_bt_cache_writer_starvation.c`

Add B-tree post-publication cache visibility, bounded multi-handle deadlock,
online-bulk/reindex concurrency, and continuous-reader progress tests. Since
the flawed global gate is removed, test the invariant—publication finishes and
post-completion readers see the new inode—not a particular lock implementation.

### `test_reindex_spill_collision.c`

Add malformed B-tree spill, malformed bitmap spill through
`bm-resolve-before-open`, non-`ENOENT` spill open, inaccessible enumeration,
startup sibling cleanup, and pre-publication byte-identity tests.

### `test_bitmap_index.c`

Add truncated Kf materialisation failure, valid empty bitmap rebuild, exact
cache visibility, bounded multi-handle deadlock, close/discard failures, and
mixed-extension obsolete cleanup.

For every test, use public command/reindex paths unless the original plan
explicitly calls for a direct primitive test. Compare complete file bytes where
the contract requires the old target to survive. Do not use mtimes alone.

## Task 8 — documentation and CORE-PROCESS evidence

Update `docs/concepts/indexes.md` under `## Index build and maintenance`:

- publication atomicity is per shard;
- a concurrent reader may complete on the old inode, while a reader beginning
  after publication completes sees the new inode;
- cache visibility is enforced by generation plus inode validation, not a
  global gate;
- a later shard failure can leave a mixed old/new set, so retry is required;
- index target publication and metadata publication have distinct failure and
  durability-warning states;
- crash-abandoned generated siblings are removed on startup.

Do not document internal lock names as public guarantees.

### Required red/green evidence

For each regression, paste into the execution handoff:

1. base/current failure for the expected reason;
2. failure with the specific fix temporarily removed;
3. pass after restoring the fix.

A timeout is acceptable only for the deliberately bounded deadlock regression
and must be the expected red result. A test that passes only after rerunning is
still a bug and must be root-caused.

### Required final verification

Run exactly, waiting for every build to complete before invoking its tests:

~~~bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all

BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 2

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all --jobs 1
~~~

Also record a normal build-log search proving no new warnings:

~~~bash
SKIP_TESTS=1 ./build.sh 2>&1 | rtk proxy tee /tmp/index-atomic-review-build.log
rtk proxy rg -n "warning:" /tmp/index-atomic-review-build.log
~~~

The second command must produce no warnings introduced by this diff. Do not run
benchmarks. Leave the complete implementation uncommitted for raw-diff review.

## Execution rules

- Obtain explicit human approval of this plan before editing production/test
  code.
- Execute Tasks 1–8 in order on the current
  `fix/index-atomic-publication` worktree. Preserve unrelated user changes.
- Use `SKIP_TESTS=1 ./build.sh` and wait for completion before every test run.
- If any quoted anchor is absent or non-unique, write `PLAN_NOTES.md` describing
  the mismatch and halt the entire execution. Do not guess or continue.
- If generation/inode validation cannot satisfy the bounded deadlock and cache-
  visibility regressions without adding a blocking global gate, stop and obtain
  a revised design. Do not restore the original gate.
- If any new failure is found, root-cause and fix it when simple; otherwise add
  a dated follow-up plan and obtain explicit approval before deferring it.
- Do not weaken, skip, reorder, or delete tests to obtain a green run.
- Do not commit, push, open a PR, or merge. The reviewing agent and human must
  inspect the raw diff first.
