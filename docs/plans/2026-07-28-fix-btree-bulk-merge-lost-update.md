# Fix lost B-tree updates during durable bulk-index apply

Status: **reviewed; ready for execution approval.**

## Decision, scope, and performance

Use one mutation gate per B-tree file path.  The registry creates a mutex on
the first mutation of a path and retains it until the owning `ShardDb` shuts
down.  Its hash table may grow, but each mutex lives in a separately allocated
node and is never moved.  A caller holds that gate for a complete logical
index mutation, not merely while it has a `BtFile` mapping open.  In
particular, a bulk merge holds it across:

```text
extract existing entries -> merge entries in memory -> replace index contents
```

Every ordinary B-tree writer to that same path (`insert`, batch insert,
`delete`, bulk build, and the streaming builder) must take the same gate.
The existing object lock is deliberately **not** changed: normal writes hold
the object's shared lock, while only maintenance/schema operations hold its
exclusive lock.  Making every object write exclusive would serialize unrelated
shards and fields and would be a much larger performance regression.

This is a real lost-update bug, not a sanitizer-only timing artifact.  The
new durable bulk apply path in `v2_bulk_ins_apply_window` calls
`delete_index_entry()` followed by `btree_bulk_merge()` while ordinary
bulk-write workers and requests may hold the same object's shared lock.  The
old merge mutex serializes merge-with-merge only.  `btree_delete()` uses only
the short-lived `BtFile` writer lock.  It can therefore complete after
`bt_extract_all()` releases its read lock but before `btree_bulk_build()`
publishes the old snapshot, causing the deleted entry to return.

The fix is writer-only on purpose.  It adds no lock to index reads and does
not change the query fast path.  It adds one uncontended mutex lock/unlock to
an index mutation.  When two writes hit the *same* indexed field and index
shard, the latter waits for the former's complete mutation; different fields
and shards remain parallel.  The registry grows with actual touched index
paths, so it has no fixed 256-path contention cliff.  This is the required
correctness tradeoff.

Do not combine this work with a reader/writer gate or an atomic temp-file
replacement redesign.  Those are worthwhile independent questions about the
existing in-place replacement visibility window, but they are not needed to
fix the demonstrated lost-write mechanism and would put a new lock on every
index lookup.

## Invariants

- A completed index delete cannot be overwritten by an older bulk-merge
  snapshot of the same B-tree path.
- All writers of one B-tree path acquire the mutation gate before acquiring
  the existing cache/file writer lock.  They release in reverse order.
- `btree_bulk_merge()` never recursively acquires the gate through a public
  helper it calls.
- Every touched index path has its own stable mutex for the owning `ShardDb`'s
  lifetime.  Registry growth may move bucket pointers, never a mutex node.
- An allocation failure while creating a gate fails that index mutation before
  it changes the B-tree; it never falls back to unsynchronised mutation.
- The gate does not change index format, wire protocol, object-lock class,
  durability ordering, or read-path behavior.
- Existing secondary indexes may already be stale or duplicate.  After the
  fixed binary is deployed, `reindex` repairs them; `rebuild-kf` is unrelated.

## Call sites and consumers checked

`rg -n -E 'btree_bulk_merge\\(|btree_bulk_build\\(|bt_stream_build_open\\(|btree_insert\\(|btree_delete\\(' src/db`
currently identifies these mutation consumers:

| Consumer | Mutation entry point | Required treatment |
| --- | --- | --- |
| `src/db/index.c` | ordinary B-tree index insert/delete | acquire the gate in public `btree_insert` / `btree_delete` |
| `src/db/query_bulk.c` | durable window apply and legacy bulk paths | `btree_bulk_merge` retains the gate throughout its full transaction |
| `src/db/index.c` reindex pipeline | streaming rebuild | builder owns the gate from open through finish |
| direct callers/tests/bench | `btree_bulk_build` | public build takes the gate; merge uses a private no-gate helper |

`btree_sync_path()` remains outside the mutation-gate interface.  It does not
alter logical contents; its existing `BtFile` writer lock waits for an active
build before syncing.

## Bitmap and trigram scope check

This fix is for B-tree-backed indexes, including trigram indexes: trigram
updates reach the public B-tree insert/delete functions and therefore take
the same mutation gate.

Bitmap indexes do not use `btree_bulk_merge`, `bt_extract_all`, or an
unlink-and-rebuild snapshot transaction.  `bitmap_prepare_window_add()` opens
each `(field, kf-shard)` bitmap with `bm_open(..., writer=1)` and retains that
handle in `BitmapPrepareWindow` until `bitmap_prepare_window_apply()` has
performed its `bm_clear`/`bm_set` mutations and called `bm_close()`.  The
path-keyed bitmap cache's writer lock therefore covers bitmap preflight and
apply as one operation; a competing bitmap writer queues.  It cannot take the
same stale-snapshot path as B-tree merge, so do not add the B-tree gate to
bitmap code.

Task 3 must additionally run the existing bitmap concurrency/regression
coverage as a guard against accidentally changing adjacent prepare/apply
behavior:

```bash
./build/bin/shard-db-test run test-bitmap-index
./build/bin/shard-db-test run test-bitmap-kfcache-lock-order-eq
./build/bin/shard-db-test run test-bitmap-kfcache-lock-order-generic
./build/bin/shard-db-test run test-bitmap-kfcache-lock-order-keyset
./build/bin/shard-db-test run test-bitmap-kfcache-lock-order-complement
./build/bin/shard-db-test run test-bitmap-kfcache-lock-order-rebuild
```

## Task 1 — deterministic regression first

### Root-cause proof

Create `src/test/cases/test_btree_bulk_merge_delete_race.c`, registered beside
`test_btree.c` in `build.sh`.  The test must not use a timeout or retry loop
to create a race.  Add a small test seam invoked only after
`bt_extract_all()` has copied and released the old tree, then force this
ordering:

```text
merge thread:  snapshot copied ---- waits at hook ---- rebuild old snapshot
delete thread:                         delete matching old entry
test thread:                                      releases merge thread
```

Seed more than 1,000 entries and use a one-entry merge so
`SHARDKV_BULK_RATIO=0` selects the extract-and-rebuild branch.  The seed and
the delete target must be in the same local `.idx` file.  After both threads
join, assert that the deleted `(value, hash)` pair has zero search results and
the newly merged pair has exactly one.  On the unmodified base tree, the
delete completes before rebuild and the deleted pair is present again; the
test fails deterministically.

Use the portable generation-count barrier shape from
`src/test/cases/test_registry_single_flight.c`; do not use
`pthread_barrier_t`, which is unavailable on macOS.  The test owns a unique
`mkstemp`-derived index path under its `TestEnv` temporary directory, frees
all `BtEntry.value` allocations, unregisters its hook, and removes the file
on every exit path.

Add the following test-only interface after the `btree_bulk_merge` declaration
in `src/db/btree.h`.  It is deliberately a narrowly named test seam rather
than an environment-variable timing knob; production callers never register
it and its only runtime cost is a null function-pointer check in the bulk
rebuild branch.

```c
typedef void (*btree_test_after_extract_fn)(void *ctx);

/* Test-only deterministic interleaving seam.  Pass NULL to disable. */
void btree_test_set_after_extract_hook(btree_test_after_extract_fn fn,
                                       void *ctx);
```

Immediately before the `/* Sort new_entries... */` comment in
`src/db/btree.c`, add this complete hook implementation:

```c
static pthread_mutex_t g_btree_test_hook_lock = PTHREAD_MUTEX_INITIALIZER;
static btree_test_after_extract_fn g_btree_test_after_extract_hook;
static void *g_btree_test_after_extract_ctx;

void btree_test_set_after_extract_hook(btree_test_after_extract_fn fn,
                                       void *ctx) {
    pthread_mutex_lock(&g_btree_test_hook_lock);
    g_btree_test_after_extract_hook = fn;
    g_btree_test_after_extract_ctx = ctx;
    pthread_mutex_unlock(&g_btree_test_hook_lock);
}

static void btree_test_after_extract(void) {
    pthread_mutex_lock(&g_btree_test_hook_lock);
    btree_test_after_extract_fn fn = g_btree_test_after_extract_hook;
    void *ctx = g_btree_test_after_extract_ctx;
    pthread_mutex_unlock(&g_btree_test_hook_lock);
    if (fn) fn(ctx);
}
```

In `btree_bulk_merge`, find:

```c
BtEntry *existing = bt_extract_all(path, &exist_count);

if (exist_count == 0) {
```

and insert the complete call below between those two blocks:

```c
btree_test_after_extract();
```

Run only the new case.  Paste its base-branch failure showing the resurrected
entry before proceeding.

## Task 2 — widen the existing gate to every writer

### Implementation

In `src/db/shard_db_internal.h`, immediately before the exact anchor:

```c
/* ── ShardDb: one instance per open data directory ── */
```

add this complete stable-node type.  The node is deliberately separate from
the bucket array: resizing buckets can never relocate a `pthread_mutex_t`
that a waiting thread has already addressed.

```c
typedef struct BtMutationLockEntry {
    struct BtMutationLockEntry *next;
    char                       *path;
    pthread_mutex_t             mutex;
} BtMutationLockEntry;
```

In `struct ShardDb`, replace this exact btree-cache tail:

```c
pthread_mutex_t      bt_merge_table_lock;
```

with this complete registry state:

```c
BtMutationLockEntry **bt_mutation_lock_buckets;
size_t                bt_mutation_lock_bucket_count;
size_t                bt_mutation_lock_count;
pthread_mutex_t       bt_mutation_lock_table_lock;
```

Replace the `g_bt_merge_table_lock` macro in the btree alias block with:

```c
#define g_bt_mutation_lock_buckets      (g_db->bt_mutation_lock_buckets)
#define g_bt_mutation_lock_bucket_count (g_db->bt_mutation_lock_bucket_count)
#define g_bt_mutation_lock_count        (g_db->bt_mutation_lock_count)
#define g_bt_mutation_lock_table_lock   (g_db->bt_mutation_lock_table_lock)
```

In `src/db/embedded.c`, replace the exact init/destroy references:

```c
pthread_mutex_init(&g_bt_merge_table_lock,   NULL);
```

and:

```c
pthread_mutex_destroy(&g_bt_merge_table_lock);
```

with these respective calls:

```c
pthread_mutex_init(&g_bt_mutation_lock_table_lock, NULL);
```

```c
btree_mutation_locks_shutdown();
pthread_mutex_destroy(&g_bt_mutation_lock_table_lock);
```

The shutdown call occurs only after pools and B-tree cache are shut down, so
no worker can still hold or wait for a path mutex.  Add its declaration beside
the B-tree cache lifecycle declarations in `src/db/btree.h`:

```c
void btree_mutation_locks_shutdown(void);
```

In `src/db/btree.c`, replace the existing merge-only lock declaration and
`bt_merge_lock_for` helper, anchored by:

```c
typedef struct { char path[PATH_MAX]; pthread_mutex_t mutex; int used; } BtMergeLock;
```

with this complete implementation.  Initial allocation and every grow happen
under the registry table mutex.  The returned node remains allocated until
shutdown, so callers may drop the table mutex before locking its path mutex.
At most a short lookup/allocation critical section is global; index mutation
is never globally serialised.

```c
#define BT_MUTATION_LOCK_INITIAL_BUCKETS 64u

static int bt_mutation_locks_grow_locked(size_t new_count) {
    BtMutationLockEntry **buckets = calloc(new_count, sizeof(*buckets));
    if (!buckets) return -1;
    for (size_t i = 0; i < g_bt_mutation_lock_bucket_count; i++) {
        BtMutationLockEntry *entry = g_bt_mutation_lock_buckets[i];
        while (entry) {
            BtMutationLockEntry *next = entry->next;
            size_t slot = bt_path_hash(entry->path) % new_count;
            entry->next = buckets[slot];
            buckets[slot] = entry;
            entry = next;
        }
    }
    free(g_bt_mutation_lock_buckets);
    g_bt_mutation_lock_buckets = buckets;
    g_bt_mutation_lock_bucket_count = new_count;
    return 0;
}

static int bt_mutation_lock_for(const char *path, pthread_mutex_t **out) {
    *out = NULL;
    pthread_mutex_lock(&g_bt_mutation_lock_table_lock);
    if (g_bt_mutation_lock_bucket_count == 0 &&
        bt_mutation_locks_grow_locked(BT_MUTATION_LOCK_INITIAL_BUCKETS) != 0)
        goto oom;
    size_t slot = bt_path_hash(path) % g_bt_mutation_lock_bucket_count;
    for (BtMutationLockEntry *entry = g_bt_mutation_lock_buckets[slot];
         entry; entry = entry->next) {
        if (strcmp(entry->path, path) == 0) {
            *out = &entry->mutex;
            pthread_mutex_unlock(&g_bt_mutation_lock_table_lock);
            return 0;
        }
    }
    if (g_bt_mutation_lock_count >=
        g_bt_mutation_lock_bucket_count * 3u / 4u &&
        bt_mutation_locks_grow_locked(g_bt_mutation_lock_bucket_count * 2u) != 0)
        goto oom;
    slot = bt_path_hash(path) % g_bt_mutation_lock_bucket_count;
    BtMutationLockEntry *entry = calloc(1, sizeof(*entry));
    if (!entry) goto oom;
    entry->path = strdup(path);
    if (!entry->path) { free(entry); goto oom; }
    if (pthread_mutex_init(&entry->mutex, NULL) != 0) {
        free(entry->path); free(entry); errno = EAGAIN; goto oom;
    }
    entry->next = g_bt_mutation_lock_buckets[slot];
    g_bt_mutation_lock_buckets[slot] = entry;
    g_bt_mutation_lock_count++;
    *out = &entry->mutex;
    pthread_mutex_unlock(&g_bt_mutation_lock_table_lock);
    return 0;
oom:
    pthread_mutex_unlock(&g_bt_mutation_lock_table_lock);
    errno = ENOMEM;
    return -1;
}

void btree_mutation_locks_shutdown(void) {
    pthread_mutex_lock(&g_bt_mutation_lock_table_lock);
    BtMutationLockEntry **buckets = g_bt_mutation_lock_buckets;
    size_t bucket_count = g_bt_mutation_lock_bucket_count;
    g_bt_mutation_lock_buckets = NULL;
    g_bt_mutation_lock_bucket_count = 0;
    g_bt_mutation_lock_count = 0;
    pthread_mutex_unlock(&g_bt_mutation_lock_table_lock);
    for (size_t i = 0; i < bucket_count; i++) {
        BtMutationLockEntry *entry = buckets[i];
        while (entry) {
            BtMutationLockEntry *next = entry->next;
            pthread_mutex_destroy(&entry->mutex);
            free(entry->path);
            free(entry);
            entry = next;
        }
    }
    free(buckets);
}
```

Add two private helpers immediately after it.  Every public writer uses these
helpers so only the outermost operation owns the gate:

```c
static inline void bt_mutation_lock(pthread_mutex_t *lock) {
    pthread_mutex_lock(lock);
}

static inline void bt_mutation_unlock(pthread_mutex_t *lock) {
    pthread_mutex_unlock(lock);
}
```

Refactor the bodies of the public `btree_insert`, `btree_insert_batch`,
`btree_delete`, and `btree_bulk_build` functions into same-file private
helpers named respectively:

```c
static int btree_insert_locked(const char *path, const char *value, size_t vlen,
                               const uint8_t hash[BT_HASH_SIZE]);
static int btree_insert_batch_locked(const char *path, BtEntry *entries, size_t count);
static int btree_delete_locked(const char *path, const char *value, size_t vlen,
                               const uint8_t hash[BT_HASH_SIZE]);
static int btree_bulk_build_locked(const char *path, BtEntry *entries, size_t count);
```

Each helper receives the original function body unchanged.  Each public
function becomes this complete wrapper shape; retain its pre-existing
argument validation before acquiring the gate where applicable:

```c
pthread_mutex_t *lock = NULL;
if (bt_mutation_lock_for(path, &lock) != 0) return -1;
bt_mutation_lock(lock);
int rc = btree_<operation>_locked(path, /* original arguments */);
int saved_errno = errno;
bt_mutation_unlock(lock);
errno = saved_errno;
return rc;
```

For `btree_bulk_merge`, replace every use of `bt_merge_lock_for`, direct
mutex call, and public recursive writer call with this exact ownership
pattern:

```c
pthread_mutex_t *lock = NULL;
if (bt_mutation_lock_for(path, &lock) != 0) return -1;
bt_mutation_lock(lock);
/* existing header check, extract, merge, and allocation work */
/* call btree_insert_batch_locked or btree_bulk_build_locked, never public wrappers */
int saved_errno = errno;
bt_mutation_unlock(lock);
errno = saved_errno;
return rc;
```

All error exits in `btree_bulk_merge` must funnel through one `done:` cleanup
block that frees only allocations already owned on that path, captures
`errno`, unlocks the mutation gate once, restores `errno`, and returns.  Do
not retain any early return after the gate is acquired.  This prevents an
OOM/error path from permanently blocking an index shard.

Replace the inaccurate comment in the adaptive insert branch:

```c
btree_insert_batch's own bt_acquire serialises against
any concurrent btree_insert / btree_delete on the same path.
```

with:

```c
The outer mutation gate serialises every logical writer of this path;
btree_insert_batch_locked then takes the existing BtFile writer lock.
```

### Streaming rebuild ownership

The streaming index-rebuild builder is a writer whose operation spans three
public calls.  It must have one cleanup module rather than hand-written
unlock/free sequences at each return.  Add these fields to
`struct BtStreamBuilder` directly after its `BtFile bt;` member:

```c
pthread_mutex_t *mutation_lock;  /* held from open until finish */
int              bt_held;        /* bt_acquire succeeded; dispose must release */
```

Immediately after `struct BtStreamBuilder`, add this complete private cleanup
module:

```c
static int bt_stream_build_dispose(BtStreamBuilder *b, int rc) {
    if (!b) return rc;
    if (b->bt_held) {
        bt_release(&b->bt);
        b->bt_held = 0;
    }
    if (b->mutation_lock) {
        bt_mutation_unlock(b->mutation_lock);
        b->mutation_lock = NULL;
    }
    free(b->leaf_ids);
    free(b);
    return rc;
}
```

Replace the full `bt_stream_build_open` body, anchored by:

```c
BtStreamBuilder *bt_stream_build_open(const char *path) {
```

with this complete body.  The mutation gate is acquired before cache
invalidation/unlink and remains held only if every open step succeeds:

```c
BtStreamBuilder *bt_stream_build_open(const char *path) {
    BtStreamBuilder *b = calloc(1, sizeof(*b));
    if (!b) {
        LOG_ERROR(LOG_SUB_BTREE, "bt_stream_build_open %s: calloc(BtStreamBuilder) failed", path);
        return NULL;
    }
    if (bt_mutation_lock_for(path, &b->mutation_lock) != 0) {
        free(b);
        return NULL;
    }
    bt_mutation_lock(b->mutation_lock);
    btree_cache_invalidate(path);
    unlink(path);
    if (bt_acquire(&b->bt, path, 1) != 0) {
        bt_stream_build_dispose(b, -1);
        return NULL;
    }
    b->bt_held = 1;
    b->leaf_cap = 256;
    b->leaf_ids = malloc(b->leaf_cap * sizeof(uint32_t));
    if (!b->leaf_ids) {
        LOG_ERROR(LOG_SUB_BTREE, "bt_stream_build_open %s: malloc(leaf_ids, cap=%zu) failed", path, b->leaf_cap);
        bt_stream_build_dispose(b, -1);
        return NULL;
    }
    b->cur_leaf = 1;
    b->leaf_ids[b->leaf_count++] = b->cur_leaf;
    return b;
}
```

In `bt_stream_build_finish`, replace each of the following exact exit blocks
with the stated one-line return.  These are all the exit sites in the current
function and must remain the only exits after this change:

```c
if (b->fatal) { bt_release(&b->bt); free(b->leaf_ids); free(b); return -1; }
```

becomes:

```c
if (b->fatal) return bt_stream_build_dispose(b, -1);
```

```c
bt_release(&b->bt);
free(b->leaf_ids);
free(b);
return 0;
```

in the `b->total_entries == 0` branch becomes:

```c
return bt_stream_build_dispose(b, 0);
```

```c
bt_release(&b->bt);
free(b->leaf_ids);
free(b);
return -1;
```

in the `malloc(parent_ids, cap=%zu) failed` branch becomes:

```c
return bt_stream_build_dispose(b, -1);
```

Finally, replace the final success cleanup:

```c
if (child_ids != b->leaf_ids) free(child_ids);
free(b->leaf_ids);
bt_release(&b->bt);
free(b);
return 0;
```

with:

```c
if (child_ids != b->leaf_ids) free(child_ids);
return bt_stream_build_dispose(b, 0);
```

`bt_stream_build_add` takes no additional lock.  This preserves the required
lock order and prevents a normal index update from interleaving with reindex's
unlink-and-build window.

Do not gate `btree_sync_path`, search/range functions, or the object-lock
dispatcher in this task.

## Task 3 — prove the repair and run required gates

1. Re-run the new race case.  It must pass.  Temporarily remove only the
   public-writer gate from `btree_delete`, rebuild, and run it again: it must
   fail by resurrecting the deleted entry.  Restore the wrapper and re-run it
   to pass.  Paste all three outputs.
2. Run the directly affected existing cases:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-btree
./build/bin/shard-db-test run test-parallel-index-integrity
./build/bin/shard-db-test run test-agg-neq-shortcut
./build/bin/shard-db-test run test-bulk-update-delimited
```

3. Run the normal full suite, then the mandatory dynamic-safety gates.  This
   change touches shared locks and index-file lifetimes, so the full local
   sanitizer runs are required:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all

BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \\
  ./build/bin/shard-db-test run-all --jobs 2

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \\
  ./build/bin/shard-db-test run-all --jobs 1
```

4. After deployment, run `./shard-db reindex` once during a maintenance
   window to rebuild indexes that could contain stale or duplicate entries
   from the affected binary.  No kf rebuild, vacuum, or data restore is
   required solely for this race.

## Execution rules

- Branch from the current default branch as `fix/btree-bulk-merge-lost-update`.
- Execute Tasks 1 → 2 → 3 in order.  If any quoted anchor is absent, write
  `docs/plans/PLAN_NOTES.md` with the mismatch and halt; do not reinterpret
  the plan or continue to another task.
- If the deterministic base test does not resurrect the deleted entry, stop
  and record the observed ordering in `PLAN_NOTES.md`; do not replace it with
  a retry-until-green stress test.
- Do not change on-disk format, index query protocol, or object-lock modes.
- Leave all work uncommitted for raw-diff review.  The reviewer must inspect
  lock ordering, every error exit after gate acquisition, streaming-builder
  cleanup, and the deterministic regression before handoff.
