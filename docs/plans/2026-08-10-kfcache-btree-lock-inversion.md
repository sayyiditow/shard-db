# Fix: kfcache/btree-cache lock-order inversion deadlock

## Status

Root cause confirmed via live production `gdb` evidence (a hung deployed
process, backtraces captured while stuck) cross-referenced against source.
Fix design agreed with human: **trylock + close/reopen on contention**
(not always-batch — see "Design tradeoff considered and rejected" below).

## Root cause

Two independent lock subsystems — the per-kf-shard cache (`kfcache`,
`slotcask.c`) and the per-btree-file cache (`bt_cache`, `btree.c`) — are
each protected by a `pthread_rwlock_t` (writer-preferring, confirmed via
`rwlock_init_writer_preferring()` at `src/db/shard_db_internal.h:111-144`;
this is not a writer-starvation bug). Two code paths acquire them in
opposite orders:

**Write path** (`bulk_upsert_slow_in_kfshard`, `src/db/slotcask.c:6468`):
acquires the target kf-shard's `kfcache` **wrlock** at line 6474
(`kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1)`) and holds it
continuously across the entire windowed batch-commit protocol (Phase 4),
including the call to `opts->apply_window(...)` at line 6871. For indexed
bulk-inserts this hook is `v2_bulk_ins_apply_window`
(`src/db/query_bulk.c:853-913`), which calls `idx_build_field_worker`
(`src/db/query_bulk.c:54-96`) for each indexed field, which calls
`btree_bulk_merge(path, ...)` — requiring the btree file's per-path
mutation lock **and** the `bt_cache` slot's **wrlock**. Lock order:
**kfcache wrlock → bt_cache wrlock**.

This hold is not incidental — it is load-bearing for crash safety. The
windowed commit protocol writes a commit-intent marker durably (fsync'd,
`src/db/slotcask.c:6772-6834`), then calls `apply_window`, and on failure
calls `kf_batch_marker_abort_locked(..., &kh, ...)`
(`src/db/slotcask.c:6887-6890`) — which takes the **same held `kh`
handle** to keep the kf-shard's on-disk state consistent with the marker
file through the abort/recovery sequence. Narrowing this lock's hold is a
crash-safety redesign, not a surgical fix, and is explicitly **rejected**
as this fix's approach (see below). The mirrored bulk-delete function
(`src/db/slotcask.c` ~7260-7668, `kfcache_acquire(..., 1)` at line ~7266,
`opts->apply_window` at line 7668) has the identical shape — both
bulk-insert and bulk-delete are write-side triggers for this deadlock, and
both are covered symmetrically by the read-side fix below (which doesn't
care which writer holds kfcache).

**This is not bulk-specific: single insert/update share the identical
hazard via a separate call chain.** Verified by direct read of
`src/db/slotcask.c`'s single-record upsert function (the one storage.c's
`cmd_insert_v2`/`cmd_update_v2` route through): `opts->apply_commit(...)`
is called at line 5686-5687 (fresh-insert branch) and line 5946-5947
(update-existing branch), and in every path — success and every error
path alike — `kfcache_release(&kh)` does not run until *after*
`apply_commit` returns (e.g. lines 5706, 5715-5732, 5965). For an indexed
v2 object this hook is `v2_update_apply_commit`
(`src/db/storage.c:1433-1448`), which calls `apply_index_diff` →
`parallel_for(update_idx_fn, ...)` (`src/db/storage.c:1352-1353`) →
`tg_idx_insert`/`tg_idx_delete` (`src/db/index.c:91/101`) →
`btree_idx_insert`/`btree_idx_delete` — taking the affected index shard's
`bt_cache` **wrlock** while `kh` (the kfcache wrlock) is still held. Same
lock order, same hazard, different function: `btree_idx_insert` here
instead of `btree_bulk_merge` on the bulk path. Both call chains converge
on the identical two-lock ordering, so **any indexed single insert or
update can trigger this deadlock too, not just bulk-insert/bulk-delete** —
this is a property of the kfcache-wrlock-held-across-index-mutation
pattern generally, not of the bulk windowed-commit protocol specifically.
The read-side fix below is writer-agnostic (it only cares that the reader
never blocks on kfcache while holding `bt_cache` rdlocks), so it already
covers this call chain with no additional design work — but the severity
assessment needs to account for it: this can fire on ordinary write
traffic, not only during bulk-load windows. (The production gdb evidence
below happened to catch the bulk-insert variant — consistent with this
incident's batch-ingestion workload — but that is one instance of the
hazard, not its full scope.)

**Read path** (`btree_idx_walk_ordered`, `src/db/index.c:272-327`): opens
one `BtRangeIter` per index shard up front (`index_splits_for(splits)`
shards, e.g. up to 128) and keeps **all of them open — holding every
shard's `bt_cache` **rdlock** — for the k-way merge's entire lifetime**.
The per-match callback `cb()` (line 310) runs while these locks are held.
Three of the five callback families that drive this walk call
`read_record_ref()` → `slotcask_lookup_by_hash()` → `kfcache_acquire_direct()`
inside that callback, acquiring the matched record's kf-shard **rdlock**
mid-merge. Lock order: **bt_cache rdlock → kfcache rdlock**.

This is the exact inverse of the write path's order → classic AB-BA
deadlock whenever a bulk indexed write and a concurrent ordered-index
read race on overlapping kf-shards/index-shards.

**Why batch 1 succeeds and batch 2 always hangs** (the reported symptom):
batch 1's index-merge has no concurrent ordered-index `find` to race
against yet — the app hasn't queried anything. By batch 2, the app's
refresh-tick has already issued a `find` (sorted by an indexed field)
against batch 1's freshly-committed data. That `find`'s ordered walk lands
on the same kf-shards/index-shards batch 2 is now writing to, and the
inversion fires deterministically — not probabilistically — which is why
three production restarts didn't change the outcome.

**Live gdb evidence corroborating the source-level finding** (production
process, 57 threads, captured before this session): ~30 threads blocked in
`btree_bulk_merge` (3 already inside it holding the per-path mutation
lock, blocked on `pthread_rwlock_wrlock` for the `bt_cache` slot); a Bun
pool thread blocked in `btree_range_iter_open` ← `btree_idx_walk_ordered`
← `cmd_find`, `pthread_rwlock_rdlock`; and — the thread that pins the
mechanism — a Bun pool thread blocked in `kfcache_acquire_direct` ←
`slotcask_lookup_by_hash` ← `read_record_ref` ← ... ← `btree_idx_walk_ordered`
← `cmd_find`, on the kfcache rdlock. Two more pool threads similarly
blocked in `kfcache_acquire_direct` ← `slotcask_get`/`read_record_ref`.

## Call sites enumerated

`btree_idx_walk_ordered` has 5 distinct callback functions across all its
call sites. Read in full and classified by whether the callback calls
`read_record_ref`/`slotcask_lookup_by_hash` (kfcache-dependent → vulnerable)
or operates only on the btree-supplied `(value, hash16)` bytes (not
vulnerable):

| Callback | Defined at | Call sites | kfcache dependent? |
|---|---|---|---|
| `order_index_walk_cb` | `query.c:2267` | `query.c:2409` (`find_via_order_index_walk`), via `btree_idx_walk_ordered` | **Yes** — `read_record_ref` at query.c:2280 |
| `composite_prefix_cb` | `query.c:1626` | `query.c:2096` (composite-index find), via `btree_idx_walk_ordered` | **Yes** — `read_record_ref` at query.c:1639 |
| `cursor_find_cb` | `query.c:5636` | `query.c:7289`, `query.c:7303`, `query.c:7844` (cursor-paginated find, asc/desc/dict variants), via `btree_idx_walk_ordered` | **Yes** — `read_record_ref` at query.c:5698 |
| `card_count_cb` | `query_plan.c:2535` | `query_plan.c:2579, 2588, 2597, 2606, 2615, 2625, 2639, 2658` (cardinality-estimation, capped count) | No — pure counter increment on `(v, vl, h)`, no fetch |
| `topn_walk_cb` | `query_aggregate.c:597` | `query_aggregate.c:779` (group-by top-N) | No — aggregates the encoded index-key bytes directly (`enc_val`); prefilter is `keyset_contains` (in-memory KeySet, no kfcache) |

**Manual (non-`btree_idx_walk_ordered`) call sites — missed by the first
pass of this enumeration, found by external review and confirmed by
direct read of each site**: `composite_prefix_cb` and `cursor_find_cb`
are *also* called directly, outside `btree_idx_walk_ordered`, by
bespoke merge loops elsewhere in `query.c`. Each such site needed its
own read to classify:

| Callback | Call site | Context | Vulnerable? |
|---|---|---|---|
| `composite_prefix_cb` | `query.c:1976` | `find_via_composite_prefix`'s `OP_IN` handling — a **second, independent k-way merge** (`CompMergeCursor` array, `comp_cursor_pull`, `comp_merge_sift_down`, read query.c:1780-2029 in full) holding its own open `BtRangeIter`s across the merge loop, structurally identical to `btree_idx_walk_ordered`'s exposure but not routed through it | **Yes** — same vulnerability, needs its own release/reopen mechanism (section 6a below) |
| `cursor_find_cb` | `query.c:6394` | Small-prefilter path: records already resolved via `slotcask_bulk_resolve_and_fetch` into an in-memory `out[]` array *before* this call; `cc.prefilter_ks = NULL`, `cc.has_cursor = 0` (confirmed by reading query.c:6330-6402) | No — no `BtRangeIter`/`bt_cache` lock is held at this point; only needs a signature-compatible `NULL` handle |
| `cursor_find_cb` | `query.c:7209` | Small-prefilter path: records already sorted into an in-memory `sp_rows[]` array *before* this call; `cc.prefilter_ks = NULL` (confirmed by reading query.c:7130-7242) | No — same as above, `NULL` handle |
| `cursor_find_cb` | `query.c:7784` | Small-prefilter path: records already sorted into an in-memory `rows[]` array *before* this call; `cc.prefilter_ks = NULL` (confirmed by reading query.c:7690-7799) | No — same as above, `NULL` handle |

The three `cursor_find_cb` manual sites are safe because nothing a
writer could contend on is held at that point — they only need to keep
compiling against the callback's new signature. `composite_prefix_cb`'s
manual site is a genuine second instance of the deadlock and needs its
own fix; see section 6a.

`segcache` (the segment-file cache, a **third**, independent lock
subsystem also touched inside `slotcask_lookup_by_hash`) and
`g_kfcache_lock` (the kfcache table mutex, used only for in-memory
slot-table bookkeeping) are confirmed **out of scope for the deadlock,
but not free of cost** — see "Non-blocking scope, precisely" in the fix
design below and the edge cases section for the full reasoning:
- `grep -n "segcache_acquire" src/db/*.c` shows every call site confined
  to `slotcask.c`/`storage.c`; no code path holds `segcache` and calls
  into `btree_*`/`bt_cache`, so there is no second AB-BA cycle through
  it. It keeps its existing blocking behavior unchanged by this fix —
  blocking on it while a reader still holds `bt_cache` rdlocks is a
  latency cost (delays that walk, and transitively anything waiting on
  the shards it's iterating), not a deadlock, since nothing on the
  other end of a `segcache` wait needs a `bt_cache` lock this reader
  holds.
- `grep -n "pthread_mutex_.*g_kfcache_lock" src/db/*.c` shows every call
  site confined to `slotcask.c`; `btree.c`/`index.c` never reference it.
  Same reasoning applies, and it is additionally cheap (in-memory table
  bookkeeping only, never held across I/O — see `kfcache_acquire`'s own
  comment at the miss-path: "Drop table lock during open since it can
  block on disk").

## Design tradeoff considered and rejected

**Write-side option** (shrink the kfcache wrlock's hold in
`bulk_upsert_slow_in_kfshard` to release before `apply_window`): rejected.
The windowed commit-intent-marker protocol requires the kf wrlock held
through the abort/recovery path (`kf_batch_marker_abort_locked` takes the
live `kh` handle) so the kf-shard's on-disk state and the marker file
never observably diverge. Changing this is a crash-safety redesign
touching exactly the kind of interrupt/crash-safety invariant
CORE-PROCESS.md's review checklist flags for extra scrutiny, for a much
larger diff, to fix a problem the read side can fix alone.

**Read-side, always-batch option** (periodically close all iterators
every N records regardless of contention, refetch, reopen): considered
and presented to the human as an alternative to the chosen design. Simpler
code (no new non-blocking lock API), but pays a periodic iterator-reopen
cost on every large/unbounded walk (full scans, big group-bys) even with
zero write contention. **Rejected in favor of trylock + close/reopen only
on actual contention**, which costs nothing in the (overwhelmingly common)
uncontended case — a `pthread_rwlock_tryrdlock` that succeeds immediately
is the same cost as today's blocking `rdlock` — and only pays the
close/reopen cost during a genuine race with a concurrent indexed write.

**Reopen frequency under sustained contention**: closing and reopening
all `n` per-shard iterators isn't free — each reopen re-descends the
B+tree per shard to find the resume position. Reopening after literally
every contended entry would degrade a scan running concurrently with a
long-lived bulk write (touching many shards over its whole duration, not
just once) toward O(matches × n) reseek cost. A fixed "reopen every N
yields" counter was considered to amortize this, but rejected: it doesn't
change how many entries get processed per reopen (a counter-based delay
still has to reopen to learn what the next entry even is), and picking N
means guessing a constant that's right for no particular workload. The
chosen mechanism instead lets the merge **drain every already-buffered
head entry still sitting in the heap** before paying for a reopen (see
`sc_pull`'s NULL-iterator guard and the drain loop in
`btree_idx_walk_ordered`, section 5) — entries that were already fetched
from disk before the contention hit keep flowing through `cb()` for
free, and a shard only drops out of the round once its one buffered
entry is consumed. This batches up to `n` entries per reopen automatically,
scaling with shard count (where reopen cost is highest) with no
tunable constant, and degrades gracefully to "one reopen per entry" only
in the worst case where contention hits on literally the first entry of
every round.

## Fix design

### 1. Non-blocking kfcache acquire (`src/db/slotcask.c`)

`kfcache_acquire()` (lines 410-632) has three `pthread_rwlock_rdlock`
call sites reachable on the reader path (line 445 fast-hit, line 503
miss-path reprobe hit, line 621 newly-installed-slot hit) plus one
self-recursive call on a stale-entry retry (line 626). Rather than
duplicate this ~220-line function, thread a `nonblocking` parameter
through via a renamed internal implementation, keeping the existing
public `kfcache_acquire()` signature and behavior for its ~20 existing
callers untouched.

**Non-blocking scope, precisely**: making the three rwlock sites
non-blocking is *not* sufficient on its own — the function has three
other places that can block regardless of those rwlocks: the
cache-globally-disabled fallback (line 428, unconditional `kf_open_file`
disk I/O), the identity-mismatch/stale-entry retry (lines 470-486,
which drops to a blocking `pthread_rwlock_wrlock` inside
`kfcache_drop_slot(..., wait=1)` and loops), and the miss-path
"open + install" section (line 488 onward, unconditional `kf_open_file`
disk I/O — this is the one a genuine cold/evicted kf-shard lookup always
hits). All three are fixed below alongside the three rwlock sites, so
that `nonblocking=1` is bounded to exactly: one `g_kfcache_lock`
mutex lock/unlock (in-memory), one in-memory probe, one
`pthread_rwlock_tryrdlock`/`trywrlock`, and (only on a confirmed hit)
one `stat()` — the same `stat()` cost the existing blocking hot path
already pays on every hit. Any miss, staleness, or contention bails
immediately with `errno=EBUSY` instead of falling through to disk I/O
or a further blocking wait.

**Anchor**: `int kfcache_acquire(SlotcaskKfHandle *h, const char *path,
                    size_t slots_capacity, int writer) {`

Replace the function signature line and rename the body to
`kfcache_acquire_ex`, adding an `int nonblocking` parameter:

```c
static int kfcache_acquire_ex(SlotcaskKfHandle *h, const char *path,
                              size_t slots_capacity, int writer,
                              int nonblocking) {
```

(the rest of the existing body is unchanged except for the three rwlock
call sites, the recursive call, and three additional fail-fast bails
that close off every other blocking path, all below).

**Anchor** (cache-globally-disabled fallback — `g_kfcache` is only NULL
when caching is administratively disabled; this path always does raw
I/O, so nonblocking mode has nothing to try and must bail immediately):
```c
retry_kfcache_acquire:
    if (!g_kfcache) {
        if (writer) {
            errno = ENODEV;
            return -1;
        }
```
Replace with:
```c
retry_kfcache_acquire:
    if (!g_kfcache) {
        if (nonblocking) { errno = EBUSY; return -1; }
        if (writer) {
            errno = ENODEV;
            return -1;
        }
```

**Anchor** (identity-mismatch / stale-entry retry — reached when a
probed slot's path matches but `stat()` shows it's been superseded by a
rebuild-rename race, or when the probed slot's path no longer matches at
all; both cases currently evict-and-retry or loop, and the eviction call
itself can take a blocking `pthread_rwlock_wrlock` via
`kfcache_drop_slot(..., wait=1)`):
```c
            pthread_rwlock_unlock(lock);
            pthread_mutex_lock(&g_kfcache_lock);
            if (g_kfcache[slot].used && strcmp(g_kfcache[slot].path, path) == 0) {
                kfcache_drop_slot(slot, CACHE_DROP_DISCARD, 1);
            }
            if (++retries >= 4) break;
            continue;
        }
        pthread_rwlock_unlock(lock);
        if (++retries >= 4) {
            /* slot/found get re-set by the kfcache_probe call below
               in the install path (Coverity CID 1693833). */
            pthread_mutex_lock(&g_kfcache_lock);
            break;
        }
        pthread_mutex_lock(&g_kfcache_lock);
    }
```
Replace with:
```c
            pthread_rwlock_unlock(lock);
            if (nonblocking) { errno = EBUSY; return -1; }
            pthread_mutex_lock(&g_kfcache_lock);
            if (g_kfcache[slot].used && strcmp(g_kfcache[slot].path, path) == 0) {
                kfcache_drop_slot(slot, CACHE_DROP_DISCARD, 1);
            }
            if (++retries >= 4) break;
            continue;
        }
        pthread_rwlock_unlock(lock);
        if (nonblocking) { errno = EBUSY; return -1; }
        if (++retries >= 4) {
            /* slot/found get re-set by the kfcache_probe call below
               in the install path (Coverity CID 1693833). */
            pthread_mutex_lock(&g_kfcache_lock);
            break;
        }
        pthread_mutex_lock(&g_kfcache_lock);
    }
```

**Anchor** (miss path — the reader hit neither the fast-hit nor the
retry path, i.e. this is a genuine cache miss; this is the site the
non-blocking design originally missed entirely, per external review —
`kf_open_file` here is unconditional real disk I/O regardless of how the
`while(1)` loop above was exited):
```c
    /* Miss path: open + install. Drop table lock during open since it can
       block on disk. */
    pthread_mutex_unlock(&g_kfcache_lock);
    int fd; uint8_t *base; size_t sz; dev_t dev; ino_t ino;
    if (kf_open_file(path, slots_capacity, writer, &fd, &base, &sz, &dev, &ino) < 0) return -1;
    pthread_mutex_lock(&g_kfcache_lock);
```
Replace with:
```c
    /* Miss path: open + install. Drop table lock during open since it can
       block on disk. In nonblocking mode, bail here instead of opening —
       the "try" contract (kfcache_try_acquire_rd / kfcache_try_acquire_direct)
       is fast-path-only: it never touches the filesystem, so a genuine
       cold/evicted shard is treated exactly like lock contention. */
    if (nonblocking) {
        pthread_mutex_unlock(&g_kfcache_lock);
        errno = EBUSY;
        return -1;
    }
    pthread_mutex_unlock(&g_kfcache_lock);
    int fd; uint8_t *base; size_t sz; dev_t dev; ino_t ino;
    if (kf_open_file(path, slots_capacity, writer, &fd, &base, &sz, &dev, &ino) < 0) return -1;
    pthread_mutex_lock(&g_kfcache_lock);
```

**Anchor** (fast-hit path, currently):
```c
        pthread_mutex_unlock(&g_kfcache_lock);
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);

        /* coverity[atomicity] CID 1693850: `slot` came from the prior
```
Replace the two-line lock branch with:
```c
        pthread_mutex_unlock(&g_kfcache_lock);
        if (writer) {
            pthread_rwlock_wrlock(lock);
        } else if (nonblocking) {
            if (pthread_rwlock_tryrdlock(lock) != 0) { errno = EBUSY; return -1; }
        } else {
            pthread_rwlock_rdlock(lock);
        }
```

**Anchor** (miss-path reprobe hit, currently — this is the *second*
occurrence of this exact three-line pattern in the function, immediately
following `slot = kfcache_probe(path, &found);` under the `if (found) {`
block that starts the miss-path re-probe):
```c
        pthread_mutex_unlock(&g_kfcache_lock);
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);
        KfCacheEntry *e = &g_kfcache[slot];
        int matched = e->used && strcmp(e->path, path) == 0;
```
Apply the identical replacement pattern as above (branch on `nonblocking`
before the `else` case).

**Anchor** (newly-installed-slot path):
```c
    pthread_rwlock_t *lock = &e->rwlock;
    pthread_mutex_unlock(&g_kfcache_lock);
    if (writer) pthread_rwlock_wrlock(lock);
    else        pthread_rwlock_rdlock(lock);

    if (!e->used || strcmp(e->path, path) != 0 || e->file_dev != dev ||
        e->file_ino != ino) {
        pthread_rwlock_unlock(lock);
        return kfcache_acquire(h, path, slots_capacity, writer);
    }
```
Replace with:
```c
    pthread_rwlock_t *lock = &e->rwlock;
    pthread_mutex_unlock(&g_kfcache_lock);
    if (writer) {
        pthread_rwlock_wrlock(lock);
    } else if (nonblocking) {
        if (pthread_rwlock_tryrdlock(lock) != 0) { errno = EBUSY; return -1; }
    } else {
        pthread_rwlock_rdlock(lock);
    }

    if (!e->used || strcmp(e->path, path) != 0 || e->file_dev != dev ||
        e->file_ino != ino) {
        pthread_rwlock_unlock(lock);
        return kfcache_acquire_ex(h, path, slots_capacity, writer, nonblocking);
    }
```

Note: the `writer && nonblocking` combination is never exercised by any
caller added in this fix (only readers use the nonblocking path) and is
not separately audited — the `if (writer) { wrlock }` branch above
deliberately ignores `nonblocking` for writers, so a future writer-side
nonblocking caller would silently get blocking semantics. Add a comment
at the top of `kfcache_acquire_ex` documenting this:

```c
    /* nonblocking is currently only exercised with writer=0 (see
       kfcache_try_acquire_rd). A writer=1,nonblocking=1 caller would
       still block on wrlock — not audited/supported; add tryrwlock
       handling here first if a future caller needs it. */
```

Now add the two public entry points right after the (unchanged-signature)
existing `kfcache_acquire`:

**Anchor**:
```c
int kfcache_acquire(SlotcaskKfHandle *h, const char *path,
                    size_t slots_capacity, int writer) {
```
This line's body becomes just the delegating wrapper (its prior ~220-line
body having moved into `kfcache_acquire_ex` above):
```c
int kfcache_acquire(SlotcaskKfHandle *h, const char *path,
                    size_t slots_capacity, int writer) {
    return kfcache_acquire_ex(h, path, slots_capacity, writer, 0);
}

/* Non-blocking reader acquire for callers that must not block while
   holding an unrelated lock (see btree_idx_walk_ordered's use via
   read_record_ref_try / slotcask_lookup_by_hash_try). Returns 0 on
   success, -1 with errno=EBUSY if the rdlock would have blocked, -1 with
   a different errno for a genuine I/O/OOM failure. */
int kfcache_try_acquire_rd(SlotcaskKfHandle *h, const char *path,
                           size_t slots_capacity) {
    return kfcache_acquire_ex(h, path, slots_capacity, 0, 1);
}
```

Declare both new functions in `src/db/types.h` next to the existing
`kfcache_acquire`/`kfcache_release` declarations (find via
`grep -n "kfcache_acquire\|kfcache_release" src/db/types.h`).

### 2. Non-blocking `kfcache_acquire_direct` (`src/db/slotcask.c:1210-1246`)

This is the actual warm-path function on the read hot path (confirmed by
the gdb evidence: the blocked frame is `kfcache_acquire_direct`, not
`kfcache_acquire` — production's per-thread `SlotRef` cache is warm by
the second batch). Same treatment: rename the body to an `_ex` internal
function with a `nonblocking` parameter, add two public wrappers.

**Anchor**: entire existing function body
```c
int kfcache_acquire_direct(SlotcaskKfHandle *h, SlotRef *ref,
                            const char *path, size_t slots_capacity,
                            void *db, int kf_shard_id) {
    (void)db;          /* used only to make the signature future-proof */
    (void)kf_shard_id; /* same */

    if (ref && ref->slot >= 0) {
        int s = ref->slot;
        KfCacheEntry *e = &g_kfcache[s];
        uint64_t cur_gen = atomic_load_explicit(&e->gen, memory_order_acquire);
        if (cur_gen == ref->gen) {
            /* Gen matches — slot should still hold our entry.
               Take rdlock and verify identity before returning. */
            pthread_rwlock_rdlock(&e->rwlock);
            if (atomic_load_explicit(&e->used, memory_order_acquire) &&
                strcmp(e->path, path) == 0) {
                /* Warm hit confirmed. */
                h->slot = s;
                h->writer = 0;
                kf_handle_from_entry(h, e);
                return 0;
            }
            /* Identity check failed (concurrent eviction between gen-check
               and rdlock). Drop lock and fall through to slow path. */
            pthread_rwlock_unlock(&e->rwlock);
        }
    }

    /* Slow path: standard kfcache_acquire, then refresh the SlotRef. */
    int rc = kfcache_acquire(h, path, slots_capacity, 0);
    if (rc == 0 && ref && h->slot >= 0) {
        ref->slot = h->slot;
        ref->gen  = atomic_load_explicit(&g_kfcache[h->slot].gen,
                                          memory_order_acquire);
    }
    return rc;
}
```
Replace with:
```c
static int kfcache_acquire_direct_ex(SlotcaskKfHandle *h, SlotRef *ref,
                                     const char *path, size_t slots_capacity,
                                     void *db, int kf_shard_id, int nonblocking) {
    (void)db;          /* used only to make the signature future-proof */
    (void)kf_shard_id; /* same */

    if (ref && ref->slot >= 0) {
        int s = ref->slot;
        KfCacheEntry *e = &g_kfcache[s];
        uint64_t cur_gen = atomic_load_explicit(&e->gen, memory_order_acquire);
        if (cur_gen == ref->gen) {
            /* Gen matches — slot should still hold our entry.
               Take rdlock and verify identity before returning. */
            if (nonblocking) {
                if (pthread_rwlock_tryrdlock(&e->rwlock) != 0) { errno = EBUSY; return -1; }
            } else {
                pthread_rwlock_rdlock(&e->rwlock);
            }
            if (atomic_load_explicit(&e->used, memory_order_acquire) &&
                strcmp(e->path, path) == 0) {
                /* Warm hit confirmed. */
                h->slot = s;
                h->writer = 0;
                kf_handle_from_entry(h, e);
                return 0;
            }
            /* Identity check failed (concurrent eviction between gen-check
               and rdlock). Drop lock and fall through to slow path. */
            pthread_rwlock_unlock(&e->rwlock);
        }
    }

    /* Slow path: standard kfcache_acquire, then refresh the SlotRef. */
    int rc = kfcache_acquire_ex(h, path, slots_capacity, 0, nonblocking);
    if (rc == 0 && ref && h->slot >= 0) {
        ref->slot = h->slot;
        ref->gen  = atomic_load_explicit(&g_kfcache[h->slot].gen,
                                          memory_order_acquire);
    }
    return rc;
}

int kfcache_acquire_direct(SlotcaskKfHandle *h, SlotRef *ref,
                            const char *path, size_t slots_capacity,
                            void *db, int kf_shard_id) {
    return kfcache_acquire_direct_ex(h, ref, path, slots_capacity, db, kf_shard_id, 0);
}

/* Non-blocking counterpart. Returns 0 on success, -1 with errno=EBUSY if
   the rdlock would have blocked. See kfcache_try_acquire_rd. */
int kfcache_try_acquire_direct(SlotcaskKfHandle *h, SlotRef *ref,
                                const char *path, size_t slots_capacity,
                                void *db, int kf_shard_id) {
    return kfcache_acquire_direct_ex(h, ref, path, slots_capacity, db, kf_shard_id, 1);
}
```
Declare `kfcache_try_acquire_direct` in `src/db/types.h` next to the
existing `kfcache_acquire_direct` declaration.

### 3. Non-blocking record scan (`src/db/slotcask.c:9094-9142`)

Factor the existing scan loop out of `slotcask_lookup_by_hash` into a
shared static helper (taking an already-acquired `kh`), then add a
non-blocking sibling that uses `kfcache_try_acquire_direct`.

**Anchor**: entire existing function
```c
int slotcask_lookup_by_hash(SlotcaskDb *db, const uint8_t hash16[16],
                            SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return -1;
    int sid_kf = shard_for_hash(hash16, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    /* Same lock-free warm-hit fast path slotcask_get already uses:
       db->kf_slot_refs[sid_kf] is a per-shard SlotRef owned by the
       SlotcaskDb instance, so it stays warm across every call against
       this shard regardless of caller. kfcache_acquire_direct falls
       back to the plain kfcache_acquire slow path (and refreshes the
       ref) on a cold miss or eviction race — same correctness, fewer
       table-mutex acquisitions on the common warm path. */
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[sid_kf] : NULL;
    if (kfcache_acquire_direct(&kh, kf_ref, kf_path, db->slots_per_shard,
                               db, sid_kf) != 0) return -1;

    size_t cap = kh.capacity;
    SlotcaskKfEntry *kf = kh.map;
    size_t start = kf_slot_for(hash16, cap);
    int stop = 0;
    for (size_t i = 0; i < cap && !stop; i++) {
        size_t slot = (start + i) % cap;
        SlotcaskKfEntry *e = &kf[slot];
        uint8_t flag = __atomic_load_n(&e->flag, __ATOMIC_ACQUIRE);
        if (flag == 0) break;                      /* probe end */
        if (flag != 1) continue;                    /* tombstone */
        if (memcmp(e->hash, hash16, 16) != 0) continue;

        char seg_path[PATH_MAX];
        seg_path_for(seg_path, db->data_dir, e->stream_id, e->file_id);
        SlotcaskSegHandle sh;
        if (segcache_acquire(&sh, seg_path, 0, 0, 0) != 0) continue;
        const uint8_t *rec = sh.map + e->offset;
        if (!seg_rec_live_with_hash(rec, hash16)) {
            segcache_release(&sh);
            continue;
        }
        uint16_t klen = seg_rec_klen(rec);
        uint32_t vlen = seg_rec_vlen(rec);
        const uint8_t *key   = rec + 24;
        const uint8_t *value = rec + 24 + klen;
        if (cb(e->hash, key, klen, value, vlen, ctx) != 0) stop = 1;
        segcache_release(&sh);
    }
    kfcache_release(&kh);
    return 0;
}
```
Replace with:
```c
/* Shared scan body for slotcask_lookup_by_hash / _try — kh must already
   be acquired (reader) by the caller, who also releases it. segcache
   stays blocking in both callers: segcache_acquire call sites are
   confined to slotcask.c/storage.c and never nest under a btree_* call,
   so it isn't part of the kfcache<->bt_cache inversion this function's
   nonblocking sibling exists to avoid. */
static void slotcask_lookup_scan_kf(SlotcaskDb *db, const uint8_t hash16[16],
                                    SlotcaskKfHandle *kh,
                                    SlotcaskScanCb cb, void *ctx) {
    size_t cap = kh->capacity;
    SlotcaskKfEntry *kf = kh->map;
    size_t start = kf_slot_for(hash16, cap);
    int stop = 0;
    for (size_t i = 0; i < cap && !stop; i++) {
        size_t slot = (start + i) % cap;
        SlotcaskKfEntry *e = &kf[slot];
        uint8_t flag = __atomic_load_n(&e->flag, __ATOMIC_ACQUIRE);
        if (flag == 0) break;                      /* probe end */
        if (flag != 1) continue;                    /* tombstone */
        if (memcmp(e->hash, hash16, 16) != 0) continue;

        char seg_path[PATH_MAX];
        seg_path_for(seg_path, db->data_dir, e->stream_id, e->file_id);
        SlotcaskSegHandle sh;
        if (segcache_acquire(&sh, seg_path, 0, 0, 0) != 0) continue;
        const uint8_t *rec = sh.map + e->offset;
        if (!seg_rec_live_with_hash(rec, hash16)) {
            segcache_release(&sh);
            continue;
        }
        uint16_t klen = seg_rec_klen(rec);
        uint32_t vlen = seg_rec_vlen(rec);
        const uint8_t *key   = rec + 24;
        const uint8_t *value = rec + 24 + klen;
        if (cb(e->hash, key, klen, value, vlen, ctx) != 0) stop = 1;
        segcache_release(&sh);
    }
}

int slotcask_lookup_by_hash(SlotcaskDb *db, const uint8_t hash16[16],
                            SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return -1;
    int sid_kf = shard_for_hash(hash16, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[sid_kf] : NULL;
    if (kfcache_acquire_direct(&kh, kf_ref, kf_path, db->slots_per_shard,
                               db, sid_kf) != 0) return -1;
    slotcask_lookup_scan_kf(db, hash16, &kh, cb, ctx);
    kfcache_release(&kh);
    return 0;
}

/* Non-blocking counterpart used by btree_idx_walk_ordered's vulnerable
   callbacks (order_index_walk_cb, composite_prefix_cb, cursor_find_cb):
   they hold every open index shard's bt_cache rdlock for the k-way
   merge's lifetime and must not then block on this hash's kfcache rdlock
   — doing so inverts the write path's kfcache-wrlock -> bt_cache-wrlock
   order (bulk_upsert_slow_in_kfshard's apply_window -> btree_bulk_merge)
   and deadlocks. Returns 0 (scan ran; check caller's found-flag), 1 (kf
   acquire genuinely failed — cache disabled edge cases aside, essentially
   "not found" for our purposes), or 2 (would block — caller must release
   its own held locks before retrying with the blocking
   slotcask_lookup_by_hash). */
int slotcask_lookup_by_hash_try(SlotcaskDb *db, const uint8_t hash16[16],
                                SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return 1;
    int sid_kf = shard_for_hash(hash16, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[sid_kf] : NULL;
    if (kfcache_try_acquire_direct(&kh, kf_ref, kf_path, db->slots_per_shard,
                                   db, sid_kf) != 0) {
        return (errno == EBUSY) ? 2 : 1;
    }
    slotcask_lookup_scan_kf(db, hash16, &kh, cb, ctx);
    kfcache_release(&kh);
    return 0;
}
```
Declare `slotcask_lookup_by_hash_try` in `src/db/types.h` next to
`slotcask_lookup_by_hash`.

### 4. Non-blocking record fetch (`src/db/query_find.c`)

**Anchor**:
```c
int read_record_ref(const char *db_root, const char *object,
                    const Schema *sch, const uint8_t hash[16],
                    RecordRef *out) {
    memset(out, 0, sizeof(*out));
    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "read_record_ref: slotcask_registry_get failed for %s/%s", db_root, object);
        return -1;
    }
    slotcask_lookup_by_hash(sdb, hash, v2_record_capture_cb, out);
    return out->v2_buf ? 0 : -1;
}
```
Add immediately after (keep `read_record_ref` itself unchanged):
```c
/* Non-blocking counterpart. Returns 0 (found), 1 (not found / registry
   lookup failed), or 2 (would block on the kfcache rdlock — caller must
   release whatever else it holds and retry with the blocking
   read_record_ref). See slotcask_lookup_by_hash_try. */
int read_record_ref_try(const char *db_root, const char *object,
                        const Schema *sch, const uint8_t hash[16],
                        RecordRef *out) {
    memset(out, 0, sizeof(*out));
    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "read_record_ref_try: slotcask_registry_get failed for %s/%s", db_root, object);
        return 1;
    }
    int rc = slotcask_lookup_by_hash_try(sdb, hash, v2_record_capture_cb, out);
    if (rc == 2) return 2;
    return out->v2_buf ? 0 : 1;
}
```
Declare `read_record_ref_try` in `src/db/query_internal.h` next to the
existing `read_record_ref` declaration.

### 5. `btree_idx_walk_ordered` yield/resume protocol (`src/db/index.c`)

New opaque handle type + release function, declared in `src/db/types.h`
right above the existing `btree_idx_walk_ordered` declaration
(`types.h:1046-1054`):

```c
/* Opaque handle passed to bt_ordered_result_cb. A callback that is about
   to take a lock it cannot safely hold alongside the walk's per-shard
   bt_cache rdlocks (concretely: the kfcache rdlock, via
   read_record_ref_try / slotcask_lookup_by_hash_try) must call
   btree_ordered_walk_release_for_blocking(h) FIRST — this closes every
   currently-open per-shard BtRangeIter (releasing all bt_cache rdlocks
   the walk holds), after which it is safe to make the blocking call.
   The callback must not return a value indicating "yield again" for the
   SAME entry after calling this — btree_idx_walk_ordered always resumes
   by re-scanning past the entry just delivered, so a callback that keeps
   yielding on the same entry would loop forever. In practice: call this,
   then do a genuinely blocking fetch (e.g. read_record_ref, not
   read_record_ref_try) and finish processing the entry normally.

   Generalized (not tied to btree_idx_walk_ordered's own ShardCursor
   array): find_via_composite_prefix's OP_IN handling (query.c) runs an
   independent k-way merge over its own CompMergeCursor array with the
   identical bt_cache-rdlocks-held-across-a-blocking-fetch exposure —
   see section 6a — and reuses this same handle type and release
   function rather than duplicating them. h->slots[i] points at cursor
   i's `iter` field wherever that cursor array actually lives, so
   release works the same way regardless of which merge owns the array.
   btree_ordered_walk_release_for_blocking is NULL-safe: call sites that
   never hold an iterator to begin with (query.c's three manual,
   already-in-memory-resolved cursor_find_cb calls — see section 6) pass
   NULL and the release becomes a no-op.

   Deliberately NOT opaque: both index.c's btree_idx_walk_ordered and
   query.c's find_via_composite_prefix (section 6a) construct and reset
   this struct directly via aggregate initializers (`BtOrderedWalkHandle
   h = { .slots = ..., .n = ..., .released = 0 };`, and `h.released = 0;`
   on each resume-loop iteration) rather than through accessor functions —
   an opaque-pointer-plus-constructor design was considered but rejected
   as unnecessary indirection for a 3-field struct with no invariants
   beyond what btree_ordered_walk_release_for_blocking itself enforces.
   The struct must therefore be fully defined here in types.h (included
   by both index.c and query.c), not just forward-declared — a
   forward-declared incomplete type cannot be aggregate-initialized or
   have `.released` assigned from query.c, which would fail to compile. */
typedef struct BtOrderedWalkHandle {
    BtRangeIter **slots;      /* slots[i] == &cursor[i].iter, wherever that
                                  cursor array lives (ShardCursor in
                                  index.c, CompMergeCursor in query.c) */
    int           n;
    int           released;   /* 1 once release_for_blocking has run for
                                  the entry currently being delivered */
} BtOrderedWalkHandle;
void btree_ordered_walk_release_for_blocking(BtOrderedWalkHandle *h);

typedef int (*bt_ordered_result_cb)(const char *value, size_t vlen,
                                    const uint8_t hash[BT_HASH_SIZE],
                                    BtOrderedWalkHandle *h, void *ctx);
```

**Anchor** (the `btree_idx_walk_ordered` declaration):
```c
void btree_idx_walk_ordered(const char *db_root, const char *object,
                            const char *field, int splits,
                            const char *min_val, size_t min_len, int min_exclusive,
                            const char *max_val, size_t max_len, int max_exclusive,
                            int desc, bt_result_cb cb, void *ctx);
```
Change the callback parameter's type:
```c
void btree_idx_walk_ordered(const char *db_root, const char *object,
                            const char *field, int splits,
                            const char *min_val, size_t min_len, int min_exclusive,
                            const char *max_val, size_t max_len, int max_exclusive,
                            int desc, bt_ordered_result_cb cb, void *ctx);
```
(Also update the stale doc comment immediately above this declaration —
`types.h:1046-1049` currently says "Implementation: collect all matches
into a buffer, qsort by (value, hash16) tie-break, replay through `cb` in
order... k-way streaming merge is a perf-only follow-up" — the k-way merge
is the *current* implementation, not a follow-up; rewrite to describe the
actual streaming design and the new yield/resume protocol.)

**Anchor** (`src/db/index.c`, the `ShardCursor` struct and everything
through the end of `btree_idx_walk_ordered`, lines 211-327 — full
replacement of this block):

```c
typedef struct {
    BtRangeIter *iter;
    /* Currently-buffered head entry — copied out of the iterator since the
       iterator's internal buffer gets overwritten on next(). */
    char    value[BT_MAX_VAL_LEN];
    size_t  vlen;
    uint8_t hash[BT_HASH_SIZE];
    int     has_entry;       /* 1 if value/hash hold a valid head, 0 if drained */
    int     shard_id;        /* tie-break ordering when (value,hash) collide */
} ShardCursor;

/* BtOrderedWalkHandle itself is defined in types.h (section 5's first
   anchor above), not here — it's shared verbatim with query.c's
   find_via_composite_prefix (section 6a), which constructs and resets it
   directly via aggregate initializers, so it must be a complete type in
   both translation units, not forward-declared/opaque. */

void btree_ordered_walk_release_for_blocking(BtOrderedWalkHandle *h) {
    /* NULL-safe: call sites that never hold an iterator to begin with
       (the three manual cursor_find_cb calls in query.c that resolve
       records into an in-memory array before this point — section 6)
       pass NULL, and this is a no-op; their fetch always falls straight
       to the blocking path, which is safe since nothing is held. */
    if (!h || h->released) return;
    for (int s = 0; s < h->n; s++) {
        if (*h->slots[s]) {
            btree_range_iter_close(*h->slots[s]);
            *h->slots[s] = NULL;
        }
    }
    h->released = 1;
}

static int sc_cmp_asc(const ShardCursor *a, const ShardCursor *b) {
    size_t m = a->vlen < b->vlen ? a->vlen : b->vlen;
    int r = memcmp(a->value, b->value, m);
    if (r != 0) return r;
    if (a->vlen != b->vlen) return a->vlen < b->vlen ? -1 : 1;
    r = memcmp(a->hash, b->hash, BT_HASH_SIZE);
    if (r != 0) return r;
    return a->shard_id - b->shard_id;
}

/* Refill the head entry of cursor c by pulling one from its iterator.
   If c->iter is NULL (closed mid-round by
   btree_ordered_walk_release_for_blocking — see below), there is nothing
   left to pull for this shard until the next reopen; mark it drained
   rather than dereferencing a NULL iterator. This is what makes the
   post-release drain in the merge loop self-terminating: a force-closed
   shard can still deliver whatever entry was already buffered in it, but
   drops out of the heap for good once that one entry is consumed. */
static void sc_pull(ShardCursor *c) {
    if (!c->iter) { c->has_entry = 0; return; }
    const char *v;
    size_t vl;
    const uint8_t *h;
    if (btree_range_iter_next(c->iter, &v, &vl, &h)) {
        c->vlen = vl > BT_MAX_VAL_LEN ? BT_MAX_VAL_LEN : vl;
        memcpy(c->value, v, c->vlen);
        memcpy(c->hash, h, BT_HASH_SIZE);
        c->has_entry = 1;
    } else {
        c->has_entry = 0;
    }
}

/* Heap helpers for the k-way merge below.
   Heap holds shard-cursor *indices* into cursors[]; we only swap small ints,
   the underlying ShardCursor structs stay put.  Comparison delegates to
   sc_cmp_asc and is negated for desc walks. */
static inline int merge_cmp(int a, int b, const ShardCursor *cursors, int desc) {
    int r = sc_cmp_asc(&cursors[a], &cursors[b]);
    return desc ? -r : r;
}

static inline void merge_swap(int *heap, int i, int j) {
    int t = heap[i]; heap[i] = heap[j]; heap[j] = t;
}

static void merge_sift_down(int *heap, int n, int i,
                            const ShardCursor *cursors, int desc) {
    for (;;) {
        int l = 2 * i + 1, r = 2 * i + 2, best = i;
        if (l < n && merge_cmp(heap[l], heap[best], cursors, desc) < 0) best = l;
        if (r < n && merge_cmp(heap[r], heap[best], cursors, desc) < 0) best = r;
        if (best == i) return;
        merge_swap(heap, i, best);
        i = best;
    }
}

void btree_idx_walk_ordered(const char *db_root, const char *object,
                            const char *field, int splits,
                            const char *min_val, size_t min_len, int min_exclusive,
                            const char *max_val, size_t max_len, int max_exclusive,
                            int desc, bt_ordered_result_cb cb, void *ctx) {
    int n = index_splits_for(splits);
    ShardCursor *cursors = calloc((size_t)n, sizeof(ShardCursor));
    int *heap = calloc((size_t)n, sizeof(int));
    BtRangeIter **slots = malloc((size_t)n * sizeof(BtRangeIter *));
    if (!cursors || !heap || !slots) { free(cursors); free(heap); free(slots); return; }
    for (int s = 0; s < n; s++) slots[s] = &cursors[s].iter;

    BtOrderedWalkHandle h = { .slots = slots, .n = n, .released = 0 };

    /* [lo_val,lo_excl) and [hi_val,hi_excl) bound every (re)open. On a
       fresh walk these are exactly the caller's [min_val,min_exclusive]
       and [max_val,max_exclusive]. After a contention-triggered resume,
       only the bound on the side the walk is progressing TOWARD tightens
       to the last entry actually delivered — ASC scans upward and has
       already covered everything below, so ASC tightens lo_*; DESC scans
       downward and has already covered everything above, so DESC
       tightens hi_*. The other side is left exactly as the caller
       supplied it, since nothing past it has been ruled out yet.
       resume_val/resume_len/resume_hash are separate from both and are
       direction-agnostic: they only drive the tie-break floor check
       below (skip, don't redeliver, an entry at the resume value whose
       hash was already delivered), regardless of which of lo_val/hi_val
       the reopen bound actually came from.

       (This corrects an earlier version of this design that always
       tightened lo_val regardless of desc — for a descending walk that
       replays every previously-delivered larger value on reopen, since
       the reopen's lo_val stayed pinned at the original min_val and only
       hi_val should have moved.) */
    char lo_val[BT_MAX_VAL_LEN];
    size_t lo_len = min_len > sizeof(lo_val) ? sizeof(lo_val) : min_len;
    if (lo_len) memcpy(lo_val, min_val, lo_len);
    int lo_excl = min_exclusive;

    char hi_val[BT_MAX_VAL_LEN];
    size_t hi_len = max_len > sizeof(hi_val) ? sizeof(hi_val) : max_len;
    if (hi_len) memcpy(hi_val, max_val, hi_len);
    int hi_excl = max_exclusive;

    char resume_val[BT_MAX_VAL_LEN];
    size_t resume_len = 0;
    int have_resume_hash = 0;
    uint8_t resume_hash[BT_HASH_SIZE];

    for (;;) {
        int nh = 0;
        h.released = 0;
        for (int s = 0; s < n; s++) {
            char idx_path[PATH_MAX];
            build_idx_path(idx_path, sizeof(idx_path), db_root, object, field, s);
            cursors[s].shard_id = s;
            cursors[s].iter = btree_range_iter_open(idx_path,
                                                    lo_val, lo_len, lo_excl,
                                                    hi_val, hi_len, hi_excl,
                                                    desc);
            if (cursors[s].iter) sc_pull(&cursors[s]);
            if (cursors[s].has_entry) heap[nh++] = s;
        }

        for (int i = nh / 2 - 1; i >= 0; i--)
            merge_sift_down(heap, nh, i, cursors, desc);

        /* Drain the heap fully before deciding whether to reopen. This is
           the adaptive-batching mechanism: on contention, cb() calls
           btree_ordered_walk_release_for_blocking(&h), which closes every
           shard's BtRangeIter and sets h.released=1 (see below) — but the
           loop does NOT stop there. It keeps delivering whatever entries
           are still buffered in the heap (sc_pull is a no-op once a
           shard's iter is NULL — see sc_pull above — so a force-closed
           shard drops out of the heap once its one buffered entry is
           consumed, instead of being refillable). Only once nh reaches 0
           — meaning every shard either exhausted its range naturally or
           was force-closed and had its last buffered entry consumed — do
           we pay for a reopen. This means one reopen can amortize across
           up to n delivered entries (everything that happened to be
           in-flight across shards at the moment of the first yield),
           self-scaling with shard count with no tunable threshold: light,
           isolated contention drains almost nothing extra and reopens
           almost immediately (same cost as always reopening one-by-one);
           sustained contention (a bulk write actively holding many
           shards) tends to force-close several shards in quick succession
           within the same round, so the batch that gets drained before
           the eventual reopen grows automatically. */
        while (nh > 0) {
            ShardCursor *bc = &cursors[heap[0]];

            /* Tie-break floor: skip entries at-or-before the last entry
               delivered before a contention-triggered resume, without
               invoking cb again for them. Only matters on the exact
               boundary value. Compared against resume_val (direction-
               agnostic), not lo_val/hi_val — those may not even be the
               value the resume floor sits at (e.g. DESC leaves lo_val
               pinned at the caller's original min_val). */
            if (have_resume_hash && bc->vlen == resume_len &&
                memcmp(bc->value, resume_val, resume_len) == 0) {
                int hc = memcmp(bc->hash, resume_hash, BT_HASH_SIZE);
                int already_delivered = desc ? (hc >= 0) : (hc <= 0);
                if (already_delivered) {
                    sc_pull(bc);
                    if (bc->has_entry) merge_sift_down(heap, nh, 0, cursors, desc);
                    else { heap[0] = heap[--nh]; merge_sift_down(heap, nh, 0, cursors, desc); }
                    continue;
                }
            }

            int rc = cb(bc->value, bc->vlen, bc->hash, &h, ctx);
            if (rc < 0) {
                for (int s = 0; s < n; s++)
                    if (cursors[s].iter) btree_range_iter_close(cursors[s].iter);
                free(cursors);
                free(heap);
                free(slots);
                return;
            }

            if (h.released) {
                /* Either this call or an earlier one in the same round
                   triggered a release. Track the last entry actually
                   delivered so far this round as the resume floor — once
                   the heap fully drains, this is where the eventual
                   reopen (if any) starts from. Recorded unconditionally
                   here (not just on the call that first released) so the
                   floor always reflects the LAST delivered entry, however
                   many get drained before the heap empties. Only the
                   bound on the walk's progression side moves. */
                resume_len = bc->vlen > sizeof(resume_val) ? sizeof(resume_val) : bc->vlen;
                memcpy(resume_val, bc->value, resume_len);
                memcpy(resume_hash, bc->hash, BT_HASH_SIZE);
                have_resume_hash = 1;
                if (desc) {
                    hi_len = resume_len;
                    memcpy(hi_val, resume_val, hi_len);
                    hi_excl = 0;
                } else {
                    lo_len = resume_len;
                    memcpy(lo_val, resume_val, lo_len);
                    lo_excl = 0;
                }
            }

            sc_pull(bc);
            if (bc->has_entry) {
                merge_sift_down(heap, nh, 0, cursors, desc);
            } else {
                heap[0] = heap[--nh];
                merge_sift_down(heap, nh, 0, cursors, desc);
            }
        }

        if (!h.released) break;  /* natural end of range, nothing force-closed */
        /* Otherwise nh hit 0 because the drained round ran out of
           buffered entries, not because the range is exhausted — loop
           back and reopen from the updated [lo_val,hi_val) window. */
    }

    for (int s = 0; s < n; s++)
        if (cursors[s].iter) btree_range_iter_close(cursors[s].iter);
    free(cursors);
    free(heap);
    free(slots);
}
```

Note the natural-exhaustion path (`nh` reaches 0 without any release ever
happening) doesn't double-close: `h.released` stays 0 for the whole round,
the `for(;;)` loop's `if (!h.released) break;` fires, and the function's
final cleanup loop closes the (already fully drained but still-open)
iterators exactly once.

### 6. Vulnerable callback updates (`src/db/query.c`)

All three follow the same shape. `order_index_walk_cb` shown in full;
`composite_prefix_cb` and `cursor_find_cb` get the identical treatment at
their own `read_record_ref` call sites.

**Anchor**:
```c
static int order_index_walk_cb(const char *val, size_t vlen,
                                const uint8_t *hash16, void *ctx_ptr) {
#ifdef TEST_BUILD
    g_order_walk_scanned++;
#endif
    (void)val; (void)vlen;   /* walk value not needed; hash16 is the record key */
    OrderIndexWalkCtx *c = (OrderIndexWalkCtx *)ctx_ptr;
    g_out = c->parent_out;
    if (query_deadline_tick(c->dl, &c->dl_counter)) return -1;
    if (c->printed >= c->limit) return -1;

    /* Fetch the record by hash16. */
    RecordRef rr;
    if (read_record_ref(c->db_root, c->object, c->sch, hash16, &rr) != 0) return 0;
```
Replace the signature and the fetch with:
```c
static int order_index_walk_cb(const char *val, size_t vlen,
                                const uint8_t *hash16,
                                BtOrderedWalkHandle *wh, void *ctx_ptr) {
#ifdef TEST_BUILD
    g_order_walk_scanned++;
#endif
    (void)val; (void)vlen;   /* walk value not needed; hash16 is the record key */
    OrderIndexWalkCtx *c = (OrderIndexWalkCtx *)ctx_ptr;
    g_out = c->parent_out;
    if (query_deadline_tick(c->dl, &c->dl_counter)) return -1;
    if (c->printed >= c->limit) return -1;

    /* Fetch the record by hash16. Try non-blocking first — order_index_walk_cb
       runs while btree_idx_walk_ordered holds every shard's bt_cache rdlock,
       and blocking here on the kfcache rdlock would invert the write path's
       kfcache-wrlock -> bt_cache-wrlock order (see docs/plans/2026-08-10-
       kfcache-btree-lock-inversion.md). On contention, release the walk's
       locks first, then do the normal blocking fetch. */
    RecordRef rr;
    int rc = read_record_ref_try(c->db_root, c->object, c->sch, hash16, &rr);
    if (rc == 2) {
        btree_ordered_walk_release_for_blocking(wh);
        rc = read_record_ref(c->db_root, c->object, c->sch, hash16, &rr) != 0 ? 1 : 0;
    }
    if (rc != 0) return 0;
```

Apply the same two edits (signature: add `BtOrderedWalkHandle *wh,` before
`void *ctx`; fetch: try-then-release-then-block) to:

- `composite_prefix_cb` (`query.c:1626-1639`, anchor:
  `if (read_record_ref(c->db_root, c->object, c->sch, hash16, &rr) != 0) return 0;`
  — identical replacement using `c->db_root/c->object/c->sch`).
- `cursor_find_cb` (`query.c:5636` onward, anchor:
  `if (read_record_ref(c->db_root, c->object, c->sch, hash16, &rr) != 0) return 0;`
  at line 5698 — identical replacement; `CursorFindCtx` also carries
  `db_root`/`object`/`sch`, confirm exact field names at the anchor before
  editing since this struct wasn't pasted in full above).

**Call sites**, once the two signatures above change, must all pass a
`BtOrderedWalkHandle *` as the new 4th argument:

- `order_index_walk_cb`'s one call site (`query.c:2409`, via
  `btree_idx_walk_ordered`) needs no source change — `btree_idx_walk_ordered`
  passes `&h` to `cb` internally (section 5).
- `composite_prefix_cb`'s call site at `query.c:2096` (via
  `btree_idx_walk_ordered`) — same, no source change.
- `composite_prefix_cb`'s **second, manual** call site at `query.c:1976`
  (found by external review, missed by the first pass of call-site
  enumeration — see "Call sites enumerated" above) is inside
  `find_via_composite_prefix`'s own independent k-way merge, not routed
  through `btree_idx_walk_ordered` at all. This merge gets its own
  yield/resume loop and its own `BtOrderedWalkHandle` — see section 6a.
  Its call site changes from `composite_prefix_cb(bc->value, bc->vlen,
  bc->hash, &ctx)` to `composite_prefix_cb(bc->value, bc->vlen, bc->hash,
  &h, &ctx)`, where `h` is section 6a's handle.
- `cursor_find_cb`'s three call sites via `btree_idx_walk_ordered`
  (`query.c:7289, 7303, 7844`) need no source change.
- `cursor_find_cb`'s three **manual** call sites (found by external
  review) require a `NULL` handle argument, since none of them hold a
  `BtRangeIter` at the call point — records are already resolved into an
  in-memory array earlier in each function (see "Call sites enumerated"
  above for why each is safe):
  - `query.c:6394`, anchor `if (cursor_find_cb("", 0, out[i].hash, &cc) < 0) break;`
    → `if (cursor_find_cb("", 0, out[i].hash, NULL, &cc) < 0) break;`
  - `query.c:7209-7211`, anchor:
    ```c
                    if (cursor_find_cb((const char *)sp_rows[i].sort_key,
                                       sp_rows[i].sort_key_len,
                                       sp_rows[i].hash, &cc) < 0) break;
    ```
    →
    ```c
                    if (cursor_find_cb((const char *)sp_rows[i].sort_key,
                                       sp_rows[i].sort_key_len,
                                       sp_rows[i].hash, NULL, &cc) < 0) break;
    ```
  - `query.c:7784`, anchor `if (cursor_find_cb("", 0, rows[i].hash, &cc) < 0) break;`
    → `if (cursor_find_cb("", 0, rows[i].hash, NULL, &cc) < 0) break;`

### 6a. Composite OP_IN k-way merge yield/resume (`src/db/query.c`,
`find_via_composite_prefix`)

`find_via_composite_prefix`'s `OP_IN` handling (query.c:1873-1991, read in
full during review) runs an **independent** k-way merge over its own
`CompMergeCursor` array — one cursor per `(IN value, shard)` pair — with
the identical exposure as `btree_idx_walk_ordered`: it holds every
cursor's `BtRangeIter` open (each an active `bt_cache` rdlock) across the
whole drain loop, and `composite_prefix_cb` calls `read_record_ref` mid-loop.
It is not routed through `btree_idx_walk_ordered`, so section 5's fix does
not cover it; this needed its own yield/resume design, reusing section
5's generalized `BtOrderedWalkHandle`/`btree_ordered_walk_release_for_blocking`
rather than duplicating them.

**Structural difference from section 5**: each `IN` value `iv` has its
own `[lo,hi]` byte sub-range (a distinct encoded-prefix region of the
composite index's keyspace — see the existing per-`iv` bound computation
at query.c:1883-1943), and these `nv` sub-ranges are disjoint (different
seed-value prefixes sort into non-overlapping regions). The single
global `resume_val` floor (same idea as section 5) must be intersected
with each `iv`'s own saved bound on reopen, not substituted for a single
shared `lo_val`/`hi_val` — that's the one new idea this section adds
beyond section 5's design; the drain/tie-break/release-tracking logic
below is otherwise a direct copy of section 5's.

**`comp_cursor_pull` needs the same NULL-iterator guard as `sc_pull`
(section 5).** Unlike `sc_pull`, which is new code introduced by this fix,
`comp_cursor_pull` already exists (`query.c:1816-1826`) and is
unconditionally dereferencing `c->iter`. Once
`btree_ordered_walk_release_for_blocking` closes every cursor in this
merge (nulling every `slots[i]`, which includes whichever cursor the
in-flight callback is for), every subsequent `comp_cursor_pull` call in
the *same drain round* — not just the one for the just-released
cursor, since release closes all of them at once — would dereference a
NULL `iter` without this guard: both the resume skip-ahead pull (used
when a reopened cursor's floor-clamped range replays an already-delivered
entry) and the post-callback pull hit this.

**Anchor** (`src/db/query.c:1816-1826`, current):
```c
static void comp_cursor_pull(CompMergeCursor *c) {
    const char *v; size_t vl; const uint8_t *h;
    if (btree_range_iter_next(c->iter, &v, &vl, &h)) {
        c->vlen = vl > BT_MAX_VAL_LEN ? BT_MAX_VAL_LEN : vl;
        memcpy(c->value, v, c->vlen);
        memcpy(c->hash, h, BT_HASH_SIZE);
        c->has_entry = 1;
    } else {
        c->has_entry = 0;
    }
}
```
Replace with:
```c
static void comp_cursor_pull(CompMergeCursor *c) {
    if (!c->iter) { c->has_entry = 0; return; }
    const char *v; size_t vl; const uint8_t *h;
    if (btree_range_iter_next(c->iter, &v, &vl, &h)) {
        c->vlen = vl > BT_MAX_VAL_LEN ? BT_MAX_VAL_LEN : vl;
        memcpy(c->value, v, c->vlen);
        memcpy(c->hash, h, BT_HASH_SIZE);
        c->has_entry = 1;
    } else {
        c->has_entry = 0;
    }
}
```

**Anchor** (current, the entire `OP_IN` block — query.c:1873-1991):
```c
    if (seed->op == OP_IN && seed->in_count > 0) {
        int nv = seed->in_count;
        int ns = index_splits_for(sch->splits);
        CompMergeCursor *cursors = calloc((size_t)(ns * nv), sizeof(CompMergeCursor));
        int *heap = calloc((size_t)(ns * nv), sizeof(int));
        if (!cursors || !heap) { free(cursors); free(heap); return 0; }

        const TypedField *seed_tf = resolve_idx_field(fs ? fs->ts : NULL, seed->field);

        int nh = 0, ci = 0;
        for (int iv = 0; iv < nv; iv++) {
            uint8_t lo[1024 + 8]; size_t len_lo = 0;
            encode_criterion_value(seed_tf, seed->in_values[iv],
                                   strlen(seed->in_values[iv]), lo, &len_lo);
            size_t pfx = len_lo;
            uint8_t hi[1024 + 8];
            memcpy(hi, lo, len_lo);
            size_t len_hi;
            if (len_lo > 0 && (!seed_tf || len_lo < (size_t)seed_tf->size)) {
                int pos = (int)len_lo - 1;
                while (pos >= 0 && lo[pos] == 0xff) pos--;
                if (pos >= 0) {
                    memcpy(hi, lo, (size_t)pos);
                    hi[pos] = lo[pos] + 1;
                    len_hi = (size_t)pos + 1;
                } else {
                    memcpy(hi, lo, len_lo);
                    hi[len_lo] = 0x00;
                    len_hi = len_lo + 1;
                }
            } else {
                memset(hi + len_lo, 0xff, 4);
                len_hi = len_lo + 4;
            }
            int min_excl = 0, max_excl = 0;

            /* order_range fold */
            if (order_range) {
                const TypedField *ord_tf = resolve_idx_field(fs ? fs->ts : NULL, order_by);
                const char *lowv = NULL; int low_excl = 0;
                const char *highv = NULL; int high_excl = 0;
                switch (order_range->op) {
                    case OP_GREATER_EQ: lowv = order_range->value; break;
                    case OP_GREATER:    lowv = order_range->value; low_excl = 1; break;
                    case OP_LESS_EQ:    highv = order_range->value; break;
                    case OP_LESS:       highv = order_range->value; high_excl = 1; break;
                    case OP_EQUAL:      lowv = highv = order_range->value; break;
                    case OP_BETWEEN:
                        lowv  = order_range->value;  low_excl  = order_range->min_exclusive;
                        highv = order_range->value2; high_excl = order_range->max_exclusive;
                        break;
                    default: break;
                }
                if (lowv) {
                    uint8_t enc[1024]; size_t el = 0;
                    encode_criterion_value(ord_tf, lowv, strlen(lowv), enc, &el);
                    if (pfx + el <= sizeof(lo)) {
                        memcpy(lo + pfx, enc, el); len_lo = pfx + el;
                        min_excl = low_excl;
                    }
                }
                if (highv) {
                    uint8_t enc[1024]; size_t el = 0;
                    encode_criterion_value(ord_tf, highv, strlen(highv), enc, &el);
                    if (pfx + el <= sizeof(hi)) {
                        memcpy(hi + pfx, enc, el); len_hi = pfx + el;
                        max_excl = high_excl;
                    }
                }
            }

            for (int s = 0; s < ns; s++) {
                char ip[PATH_MAX];
                build_idx_path(ip, sizeof(ip), db_root, object, composite_field, s);
                cursors[ci].iter = btree_range_iter_open(ip,
                                     (const char *)lo, len_lo, min_excl,
                                     (const char *)hi, len_hi, max_excl,
                                     order_desc);
                cursors[ci].stream_id = iv;
                if (cursors[ci].iter) comp_cursor_pull(&cursors[ci]);
                if (cursors[ci].has_entry) heap[nh++] = ci;
                ci++;
            }
        }

        int total_cursors = ci;

        for (int i = nh/2-1; i >= 0; i--)
            comp_merge_sift_down(heap, nh, i, cursors, order_desc);

        CompositePrefixCtx ctx;
        memset(&ctx, 0, sizeof(ctx));
        ctx.db_root = db_root; ctx.object = object; ctx.sch = sch; ctx.fs = fs;
        ctx.tree = tree; ctx.excluded = excluded;
        ctx.proj_fields = proj_fields; ctx.proj_count = proj_count;
        ctx.dict_fmt = dict_fmt;
        ctx.skip_remaining = (offset > 0) ? offset : 0;
        ctx.limit = (limit > 0)  ? limit  : INT_MAX;
        ctx.dl = dl; ctx.dl_counter = 0; ctx.parent_out = g_out;
        pthread_mutex_init(&ctx.lock, NULL);

        while (nh > 0) {
            CompMergeCursor *bc = &cursors[heap[0]];
            if (composite_prefix_cb(bc->value, bc->vlen, bc->hash, &ctx) < 0) break;
            comp_cursor_pull(bc);
            if (bc->has_entry) {
                comp_merge_sift_down(heap, nh, 0, cursors, order_desc);
            } else {
                heap[0] = heap[--nh];
                if (nh > 0) comp_merge_sift_down(heap, nh, 0, cursors, order_desc);
            }
        }

        pthread_mutex_destroy(&ctx.lock);
        for (int i = 0; i < total_cursors; i++)
            if (cursors[i].iter) btree_range_iter_close(cursors[i].iter);
        free(cursors); free(heap);
        return ctx.printed;
    }
```

Replace with:
```c
    if (seed->op == OP_IN && seed->in_count > 0) {
        int nv = seed->in_count;
        int ns = index_splits_for(sch->splits);
        int total = ns * nv;
        CompMergeCursor *cursors = calloc((size_t)total, sizeof(CompMergeCursor));
        int *heap = calloc((size_t)total, sizeof(int));
        BtRangeIter **slots = malloc((size_t)total * sizeof(BtRangeIter *));
        /* Per-iv sub-range bounds, computed once below; every reopen
           clamps these saved bounds against the resume floor instead of
           recomputing them from seed->in_values. */
        uint8_t (*iv_lo)[1024 + 8]  = malloc((size_t)nv * sizeof(*iv_lo));
        size_t  *iv_lo_len          = malloc((size_t)nv * sizeof(size_t));
        int     *iv_min_excl        = malloc((size_t)nv * sizeof(int));
        uint8_t (*iv_hi)[1024 + 8]  = malloc((size_t)nv * sizeof(*iv_hi));
        size_t  *iv_hi_len          = malloc((size_t)nv * sizeof(size_t));
        int     *iv_max_excl        = malloc((size_t)nv * sizeof(int));
        if (!cursors || !heap || !slots || !iv_lo || !iv_lo_len || !iv_min_excl ||
            !iv_hi || !iv_hi_len || !iv_max_excl) {
            free(cursors); free(heap); free(slots);
            free(iv_lo); free(iv_lo_len); free(iv_min_excl);
            free(iv_hi); free(iv_hi_len); free(iv_max_excl);
            return 0;
        }

        const TypedField *seed_tf = resolve_idx_field(fs ? fs->ts : NULL, seed->field);

        for (int iv = 0; iv < nv; iv++) {
            uint8_t lo[1024 + 8]; size_t len_lo = 0;
            encode_criterion_value(seed_tf, seed->in_values[iv],
                                   strlen(seed->in_values[iv]), lo, &len_lo);
            size_t pfx = len_lo;
            uint8_t hi[1024 + 8];
            memcpy(hi, lo, len_lo);
            size_t len_hi;
            if (len_lo > 0 && (!seed_tf || len_lo < (size_t)seed_tf->size)) {
                int pos = (int)len_lo - 1;
                while (pos >= 0 && lo[pos] == 0xff) pos--;
                if (pos >= 0) {
                    memcpy(hi, lo, (size_t)pos);
                    hi[pos] = lo[pos] + 1;
                    len_hi = (size_t)pos + 1;
                } else {
                    memcpy(hi, lo, len_lo);
                    hi[len_lo] = 0x00;
                    len_hi = len_lo + 1;
                }
            } else {
                memset(hi + len_lo, 0xff, 4);
                len_hi = len_lo + 4;
            }
            int min_excl = 0, max_excl = 0;

            /* order_range fold */
            if (order_range) {
                const TypedField *ord_tf = resolve_idx_field(fs ? fs->ts : NULL, order_by);
                const char *lowv = NULL; int low_excl = 0;
                const char *highv = NULL; int high_excl = 0;
                switch (order_range->op) {
                    case OP_GREATER_EQ: lowv = order_range->value; break;
                    case OP_GREATER:    lowv = order_range->value; low_excl = 1; break;
                    case OP_LESS_EQ:    highv = order_range->value; break;
                    case OP_LESS:       highv = order_range->value; high_excl = 1; break;
                    case OP_EQUAL:      lowv = highv = order_range->value; break;
                    case OP_BETWEEN:
                        lowv  = order_range->value;  low_excl  = order_range->min_exclusive;
                        highv = order_range->value2; high_excl = order_range->max_exclusive;
                        break;
                    default: break;
                }
                if (lowv) {
                    uint8_t enc[1024]; size_t el = 0;
                    encode_criterion_value(ord_tf, lowv, strlen(lowv), enc, &el);
                    if (pfx + el <= sizeof(lo)) {
                        memcpy(lo + pfx, enc, el); len_lo = pfx + el;
                        min_excl = low_excl;
                    }
                }
                if (highv) {
                    uint8_t enc[1024]; size_t el = 0;
                    encode_criterion_value(ord_tf, highv, strlen(highv), enc, &el);
                    if (pfx + el <= sizeof(hi)) {
                        memcpy(hi + pfx, enc, el); len_hi = pfx + el;
                        max_excl = high_excl;
                    }
                }
            }

            memcpy(iv_lo[iv], lo, len_lo); iv_lo_len[iv] = len_lo; iv_min_excl[iv] = min_excl;
            memcpy(iv_hi[iv], hi, len_hi); iv_hi_len[iv] = len_hi; iv_max_excl[iv] = max_excl;
        }

        for (int i = 0; i < total; i++) slots[i] = &cursors[i].iter;
        BtOrderedWalkHandle h = { .slots = slots, .n = total, .released = 0 };

        char resume_val[BT_MAX_VAL_LEN];
        size_t resume_len = 0;
        int have_resume_hash = 0;
        uint8_t resume_hash[BT_HASH_SIZE];

        CompositePrefixCtx ctx;
        memset(&ctx, 0, sizeof(ctx));
        ctx.db_root = db_root; ctx.object = object; ctx.sch = sch; ctx.fs = fs;
        ctx.tree = tree; ctx.excluded = excluded;
        ctx.proj_fields = proj_fields; ctx.proj_count = proj_count;
        ctx.dict_fmt = dict_fmt;
        ctx.skip_remaining = (offset > 0) ? offset : 0;
        ctx.limit = (limit > 0)  ? limit  : INT_MAX;
        ctx.dl = dl; ctx.dl_counter = 0; ctx.parent_out = g_out;
        pthread_mutex_init(&ctx.lock, NULL);

        int total_cursors = 0;
        int abort_requested = 0;
        for (;;) {
            int nh = 0, ci = 0;
            h.released = 0;
            for (int iv = 0; iv < nv; iv++) {
                const uint8_t *eff_lo = iv_lo[iv]; size_t eff_lo_len = iv_lo_len[iv]; int eff_min_excl = iv_min_excl[iv];
                const uint8_t *eff_hi = iv_hi[iv]; size_t eff_hi_len = iv_hi_len[iv]; int eff_max_excl = iv_max_excl[iv];
                /* Clamp this iv's saved sub-range against the resume
                   floor. Ranges are disjoint across iv, so at most one
                   iv actually straddles the floor; for the rest this is
                   either a no-op (range entirely after the floor,
                   untouched so far) or produces an empty range (entirely
                   before the floor, already fully delivered — an empty
                   btree_range_iter_open range simply yields no entries,
                   same as any other exhausted cursor). */
                if (have_resume_hash) {
                    if (!order_desc) {
                        size_t m = resume_len < eff_lo_len ? resume_len : eff_lo_len;
                        int c = memcmp(resume_val, eff_lo, m);
                        if (c > 0 || (c == 0 && resume_len > eff_lo_len)) {
                            eff_lo = (const uint8_t *)resume_val;
                            eff_lo_len = resume_len;
                            eff_min_excl = 0;
                        }
                    } else {
                        size_t m = resume_len < eff_hi_len ? resume_len : eff_hi_len;
                        int c = memcmp(resume_val, eff_hi, m);
                        if (c < 0 || (c == 0 && resume_len < eff_hi_len)) {
                            eff_hi = (const uint8_t *)resume_val;
                            eff_hi_len = resume_len;
                            eff_max_excl = 0;
                        }
                    }
                }
                for (int s = 0; s < ns; s++) {
                    char ip[PATH_MAX];
                    build_idx_path(ip, sizeof(ip), db_root, object, composite_field, s);
                    cursors[ci].iter = btree_range_iter_open(ip,
                                         (const char *)eff_lo, eff_lo_len, eff_min_excl,
                                         (const char *)eff_hi, eff_hi_len, eff_max_excl,
                                         order_desc);
                    cursors[ci].stream_id = iv;
                    if (cursors[ci].iter) comp_cursor_pull(&cursors[ci]);
                    if (cursors[ci].has_entry) heap[nh++] = ci;
                    ci++;
                }
            }
            total_cursors = ci;

            for (int i = nh/2-1; i >= 0; i--)
                comp_merge_sift_down(heap, nh, i, cursors, order_desc);

            while (nh > 0) {
                CompMergeCursor *bc = &cursors[heap[0]];

                if (have_resume_hash && bc->vlen == resume_len &&
                    memcmp(bc->value, resume_val, resume_len) == 0) {
                    int hc = memcmp(bc->hash, resume_hash, BT_HASH_SIZE);
                    int already_delivered = order_desc ? (hc >= 0) : (hc <= 0);
                    if (already_delivered) {
                        comp_cursor_pull(bc);
                        if (bc->has_entry) comp_merge_sift_down(heap, nh, 0, cursors, order_desc);
                        else { heap[0] = heap[--nh]; if (nh > 0) comp_merge_sift_down(heap, nh, 0, cursors, order_desc); }
                        continue;
                    }
                }

                if (composite_prefix_cb(bc->value, bc->vlen, bc->hash, &h, &ctx) < 0) {
                    abort_requested = 1;
                    break;
                }

                if (h.released) {
                    resume_len = bc->vlen > sizeof(resume_val) ? sizeof(resume_val) : bc->vlen;
                    memcpy(resume_val, bc->value, resume_len);
                    memcpy(resume_hash, bc->hash, BT_HASH_SIZE);
                    have_resume_hash = 1;
                }

                comp_cursor_pull(bc);
                if (bc->has_entry) {
                    comp_merge_sift_down(heap, nh, 0, cursors, order_desc);
                } else {
                    heap[0] = heap[--nh];
                    if (nh > 0) comp_merge_sift_down(heap, nh, 0, cursors, order_desc);
                }
            }

            if (abort_requested || !h.released) break;
        }

        pthread_mutex_destroy(&ctx.lock);
        for (int i = 0; i < total_cursors; i++)
            if (cursors[i].iter) btree_range_iter_close(cursors[i].iter);
        free(cursors); free(heap); free(slots);
        free(iv_lo); free(iv_lo_len); free(iv_min_excl);
        free(iv_hi); free(iv_hi_len); free(iv_max_excl);
        return ctx.printed;
    }
```

Note `total_cursors` is now set inside the loop (it's the same `ns * nv`
on every round, since every `iv` always opens `ns` cursors even when its
effective range is empty — an empty-range `btree_range_iter_open` call
still returns a valid, immediately-exhausted iterator, not NULL; treated
identically to any other cursor that starts out with `has_entry=0`), so
the final cleanup loop's bound is correct however many rounds ran.

### 7. Non-vulnerable callback signature updates (mechanical)

`card_count_cb` (`query_plan.c:2535`) and `topn_walk_cb`
(`query_aggregate.c:597`) must match the new `bt_ordered_result_cb`
typedef to keep compiling as arguments to `btree_idx_walk_ordered`. Add
the ignored parameter:

**Anchor**:
```c
static int card_count_cb(const char *v, size_t vl, const uint8_t h[16], void *ctx) {
    (void)v; (void)vl; (void)h;
```
Replace with:
```c
static int card_count_cb(const char *v, size_t vl, const uint8_t h[16],
                         BtOrderedWalkHandle *wh, void *ctx) {
    (void)v; (void)vl; (void)h; (void)wh;
```

**Anchor**:
```c
static int topn_walk_cb(const char *enc_val, size_t enc_val_len,
                        const uint8_t *hash16, void *ctx_v) {
    TopNWalkCtx *c = (TopNWalkCtx *)ctx_v;
```
Replace with:
```c
static int topn_walk_cb(const char *enc_val, size_t enc_val_len,
                        const uint8_t *hash16,
                        BtOrderedWalkHandle *wh, void *ctx_v) {
    (void)wh;
    TopNWalkCtx *c = (TopNWalkCtx *)ctx_v;
```

No call-site changes needed for either — `btree_idx_walk_ordered(...,
card_count_cb, &cctx)` / `(..., topn_walk_cb, &ctx)` keep compiling
unchanged once the function pointers' types match.

## Edge cases and invariants

- **Resume tie-break correctness**: after a yield on entry `(V, H)`,
  reopening with the progression-side bound at `V` (inclusive) would
  re-deliver `(V, H)` itself and any other-shard entry with value `V`
  and a hash on the already-delivered side of `H` (`<= H` for ASC,
  `>= H` for DESC) unless filtered. The `have_resume_hash` floor check
  in the merge loop (section 5), compared against `resume_val`/
  `resume_hash` rather than `lo_val`/`hi_val` directly (since for DESC
  the reopen bound that moved is `hi_val`, not `lo_val`), exists
  specifically for this — mirrors the identical tie-break `cursor_find_cb`
  already does for cursor-pagination resume (`query.c:5636` area,
  `vcmp`/`hcmp` comparison against `c->cursor_value_bytes`/
  `c->cursor_hash16`).
- **Resume must tighten the bound on the progression side, not always
  `lo_val`.** An earlier version of this design always updated `lo_val`
  regardless of `desc` — for a descending walk (which scans from
  `max_val` downward) that pins the reopen's lower bound at the
  caller's original `min_val` forever and leaves the upper bound at the
  original `max_val`, so every reopen re-scans and redelivers every
  larger value already handed to the callback before the first yield.
  ASC tightens `lo_val` (already covered everything below); DESC
  tightens `hi_val` (already covered everything above) — see the
  `if (desc) { ... } else { ... }` branch in section 5's drain loop.
- **A callback must never yield twice on the same entry.** After
  `btree_ordered_walk_release_for_blocking` is called, the callback must
  complete the fetch with a genuinely blocking call (`read_record_ref`,
  not `_try`) — this is what guarantees forward progress and termination;
  document this explicitly in the `BtOrderedWalkHandle` doc comment
  (already included above).
- **`h.released` must be reset to 0 at the top of each reopen iteration**
  (`h.released = 0;` right after `int nh = 0;` in the `for(;;)` loop) —
  otherwise a second yield later in the same walk would be silently
  treated as a no-op by `btree_ordered_walk_release_for_blocking`'s
  `if (h->released) return;` guard, leaking the newly-opened iterators'
  locks on the next yield.
- **Drained-round resume point must track the LAST delivered entry, not
  the first-yielded one.** Once `h.released` is true, the resume-floor
  update (`lo_val`/`lo_excl`/`resume_hash`) runs after every `cb()` call
  for the rest of that round, not just the call that first triggered the
  release — otherwise a round that drains several more buffered entries
  after the initial yield would reopen from a stale, too-early position
  and redeliver everything already drained past it. The tie-break floor
  check (`have_resume_hash`) is what makes redelivery merely inefficient
  rather than a correctness bug even if this were gotten wrong, but the
  implementation should get it right regardless (see the `if (h.released)`
  block inside the drain loop, section 5).
- **`sc_pull`'s NULL-iterator guard is what makes draining
  self-terminating.** Without it, a force-closed shard's cursor would
  either crash (dereferencing a NULL `BtRangeIter *`) or need a separate
  "closed shards" bitmask threaded through the loop. Guarding inside
  `sc_pull` itself (`if (!c->iter) { c->has_entry = 0; return; }`) means
  the existing heap-removal path (`heap[0] = heap[--nh]`) already handles
  it for free — a closed shard just looks like an exhausted one to the
  rest of the loop.
- **`writer=1, nonblocking=1` is unsupported** in `kfcache_acquire_ex` —
  documented in-line (section 1); no current caller needs it, but a
  future one must not assume it works.
- **`card_count_cb`/`topn_walk_cb` never yield** — they don't call
  `read_record_ref`/`_try` at all, so `BtOrderedWalkHandle` is unused
  (parameter present only to satisfy the shared typedef) and their
  call sites in `query_plan.c`/`query_aggregate.c` need zero behavioral
  changes.
- **`segcache` stays out of scope** (verified via
  `grep -n "segcache_acquire" src/db/*.c` — confined to
  `slotcask.c`/`storage.c`, never nested under `btree_*`). If a future
  change ever calls into `btree_*` while `segcache` is held, re-audit
  this fix's scoping assumption.
- **Memory bound preserved**: the walk still holds O(n) `ShardCursor`
  buffers (`BT_MAX_VAL_LEN` each) regardless of how many times it
  reopens — reopening doesn't grow any buffer, it just re-runs the same
  O(n) open loop.
- **`limit`/`offset`/deadline semantics unaffected**: those checks
  (`c->printed >= c->limit`, `query_deadline_tick`, `c->skip_remaining`)
  all still run inside the callback exactly as before the fetch;
  reopening mid-walk only changes *where* the btree iterators are
  positioned, not the callback's own bookkeeping, which lives in `ctx`
  and survives across yields untouched.
- **Cursor pagination (`cursor_find_cb`) doubly benefits**: its own
  existing cursor-resume bookmark (`has_cursor`/`cursor_value_bytes`/
  `cursor_hash16`) is a *client-visible* pagination cursor across
  separate requests; the new yield/resume mechanism is a completely
  separate, request-internal, invisible-to-the-client mechanism for
  surviving lock contention within a single request. They don't
  interact — the internal resume just narrows `[lo_val, lo_excl)` for
  the remainder of the same request's walk.

## Tasks

### Task 1 — Regression test (test-first)

Add `src/test/cases/test_ordered_walk_kfcache_deadlock.c`, modeled
directly on the existing hook-based deterministic race pattern in
`src/test/cases/test_btree_bulk_merge_delete_race.c` (barrier + `pthread_cond_wait`,
no sleep-based timing).

New `TEST_BUILD`-only synchronization hook needed in `src/db/query.c`,
mirroring `btree_test_set_after_extract_hook`'s existing pattern
(`src/db/btree.c`): fires immediately before `order_index_walk_cb`'s
record-fetch attempt, letting a test park the walk there with its
`bt_cache` rdlocks held.

**Anchor** (`src/db/query.c`, right after the
`OrderIndexWalkCtx`/`g_order_walk_scanned` block, before
`order_index_walk_cb`'s definition):
```c
#ifdef TEST_BUILD
/* One-shot pause hook: fires just before order_index_walk_cb attempts its
   record fetch, while btree_idx_walk_ordered still holds every shard's
   bt_cache rdlock. Lets test_ordered_walk_kfcache_deadlock.c park the
   walk there deterministically, then start a concurrent indexed write
   that needs the SAME kf-shard's wrlock and the SAME index shard's
   bt_cache wrlock — reproducing the production AB-BA deadlock without
   relying on scheduling luck. */
typedef void (*order_walk_test_pause_fn)(void *ctx);
static order_walk_test_pause_fn g_order_walk_pause_fn = NULL;
static void *g_order_walk_pause_ctx = NULL;
void order_walk_test_set_pause_hook(order_walk_test_pause_fn fn, void *ctx) {
    g_order_walk_pause_fn = fn;
    g_order_walk_pause_ctx = ctx;
}
#endif
```
Declare `order_walk_test_set_pause_hook` in `src/db/query_internal.h`
under an `#ifdef TEST_BUILD` guard matching existing test-hook
declarations there.

**Anchor** (inside `order_index_walk_cb`, right before the
`RecordRef rr;` / fetch block added in section 6 above):
```c
#ifdef TEST_BUILD
    if (g_order_walk_pause_fn) {
        order_walk_test_pause_fn fn = g_order_walk_pause_fn;
        g_order_walk_pause_fn = NULL;
        fn(g_order_walk_pause_ctx);
    }
#endif
```

Test structure (`test_ordered_walk_kfcache_deadlock.c`):

1. **Setup**: create a v2 object with `splits=8` (→ 2 index shards per
   `index_splits_for(8)=2`) and one indexed integer field `F`. Search for
   two keys `seed_key` and `writer_key` (via `compute_hash_raw` +
   `idx_shard_for_hash` from `types.h`, looping candidate key strings
   until both land on the same index shard for field `F`'s encoded value
   *and* the documented kf-shard formula `hash[0..1] % splits` — per
   AGENTS.md's storage-model section — puts them in the same kf shard;
   assert the search succeeds within a bounded number of attempts, e.g.
   100,000, failing loudly with a clear message rather than looping
   forever if the routing assumption doesn't hold).

   **Pre-populate the target index shard past the 1,000-entry rebuild/
   splice crossover** (`src/db/btree.c:3561`,
   `if (ratio > 0 && existing_count > 1000 && existing_count > new_count *
   (size_t)ratio)`): with only 1 existing entry, `btree_bulk_merge` takes
   the small-tree *rebuild* path (`bt_extract_all` → merge → sorted
   rebuild → `bt_publish_replace`, an atomic rename plus
   `btree_cache_invalidate_nowait` — which by its own doc comment at
   `btree.c:717-727` "never blocks on a live target cache entry"). That
   path does **not** take the `bt_cache` wrlock at all, so a writer racing
   through it would never contend with the parked reader and the test
   would silently prove nothing. Only the *splice* path
   (`btree_insert_batch_locked`, taken when `existing_count > 1000`) calls
   `bt_acquire(&bt, path, 1)` — a real, blocking `bt_cache` wrlock — which
   is the lock the reader is holding. So: continue the same routing-check
   loop used for `seed_key`/`writer_key` above (same `idx_shard_for_hash`
   call, same target index shard — no kf-shard constraint needed for
   these, only the index-shard match) to collect **at least 1,050**
   further distinct keys that route to that same index shard, each with
   `F` set to a distinct value `>= 1000` (so they sort *after*
   `seed_key`/`writer_key` in ascending order and don't change which
   record the reader's ordered walk visits first). Insert all of them via
   one `mode:"bulk-insert"` call — this is a deterministic, one-shot setup
   step with no race timing, run to completion before the reader thread
   even starts. Then insert `seed_key` with `F=1`. The target index
   shard's `existing_count` is now >= 1,051 by the time the race's
   single-record `writer_key` bulk-insert (step 4 below) runs against it,
   so that bulk-insert is guaranteed to take the blocking splice path.
2. **Reader thread**: runs a real `find` with `order_by=F` (through the
   real `find_via_order_index_walk` → `btree_idx_walk_ordered` →
   `order_index_walk_cb` path) against the object.
3. Register the pause hook before starting the reader thread; the hook's
   callback signals a `RaceSync`-style condvar ("reader has bt_cache
   rdlock, parked before fetch") and blocks on `release`.
4. **Main test thread** waits for that signal, then starts the **writer
   thread**: a real `mode:"bulk-insert"` request (`{"mode":"bulk-insert",
   "dir":...,"object":...,"records":{"<writer_key>":{"F":2}}}`) — **not**
   a single `insert`. This targets the `apply_window` → `btree_bulk_merge`
   call chain specifically (the one the production gdb capture actually
   caught — 30 threads blocked inside `btree_bulk_merge`), as opposed to
   single insert/update's separate `apply_commit` → `apply_index_diff` →
   `btree_idx_insert` call chain (`storage.c:1433-1448`,
   `storage.c:1352-1353`, `index.c:91/101`) — **note the single-insert
   chain is equally vulnerable to this same deadlock** (see "Root cause":
   `slotcask.c`'s single-record upsert also holds the kfcache wrlock
   across the `apply_commit` call, lines 5686-5687/5946-5947, before
   `kfcache_release` at 5706/5732/5965), it is simply a *different*
   function acquiring the second (`bt_cache`) lock. This test exercises
   the bulk chain because that's what production actually hit; it does
   not imply the single-insert chain is safe, and the fix does not
   distinguish between them (it is purely read-side, so it protects
   against whichever writer holds the lock pair). The new batch itself
   only needs to be a **single record** (`apply_window` fires for any
   `napply_active > 0`, `slotcask.c:6790-6889`, no minimum-batch-size gate
   at that layer, and `idx_build_field_worker` calls `btree_bulk_merge`
   per-shard for any `new_count > 0`, `query_bulk.c:95`) — but reaching
   `btree_bulk_merge` at all is not the same as reaching the `bt_cache`
   **wrlock**: with a small *existing* tree, `btree_bulk_merge` takes the
   non-blocking rebuild/publish path instead (see step 1's pre-population
   note and `btree.c:3561`). It is step 1's >=1,051-entry pre-population
   of the target index shard — not this batch's own size — that forces
   `btree_bulk_merge` into the blocking `btree_insert_batch_locked` splice
   path here. This bulk-insert acquires the kfcache wrlock, then blocks
   inside `btree_insert_batch_locked`'s `bt_acquire(&bt, path, 1)` trying
   to acquire the SAME index shard's `bt_cache` wrlock (still held by the
   parked reader) — genuinely
   blocked, not simulated.
5. Main thread confirms the writer is actually blocked (bounded poll,
   e.g. up to 2s, on a `TEST_BUILD`-only introspection already available
   or added minimally for this test — if no existing "is a write pending
   on this bt_cache slot" introspection exists, a fixed short delay after
   starting the writer thread is acceptable here specifically because
   step 6 has its own hang-detecting timeout regardless — the test's
   correctness doesn't depend on this poll being exact).
6. Main thread releases the reader's pause hook. **Pre-fix**: the reader
   now calls the old blocking `read_record_ref`, which deadlocks against
   the writer forever — join both threads with a bounded timeout (e.g.
   `pthread_timedjoin_np` or a watchdog thread + `pthread_kill`-free
   bail-out) and **fail the test** if either thread hasn't finished within
   a few seconds, printing which thread(s) are still stuck. **Post-fix**:
   `read_record_ref_try` detects contention, yields, `btree_idx_walk_ordered`
   releases the reader's `bt_cache` rdlocks, the writer's `btree_bulk_merge`
   unblocks and finishes, the reader's now-safe blocking fetch completes,
   the walk reopens and finishes — both threads join within the timeout.
7. Assert: both threads joined within the timeout: the write succeeded
   (`writer_key` is findable with `F=2`); the reader's `find` returned
   both records in `F` order.

**Regression-test proof step** (per CORE-PROCESS.md): before implementing
the fix, temporarily revert the fix commit's non-test changes (or, since
this is task-ordered, simply implement Task 1 alone first against the
current — unfixed — codebase), run
`./build/bin/shard-db-test run test-ordered-walk-kfcache-deadlock`, and
paste the actual failing/hanging output showing the timeout fires with
the expected "reader still blocked in read_record_ref" /
"writer still blocked in btree_bulk_merge" diagnostic. Then implement
Tasks 2-6 and re-run the same command, pasting the passing output.

### Task 1b — Second regression test: single insert/update chain (test-first)

Task 1 proves the fix against the *bulk* writer chain
(`apply_window` → `btree_bulk_merge` → `btree_insert_batch_locked` →
`bt_acquire(&bt, path, 1)`). But "Root cause" above establishes a second,
independent writer chain with the identical `kfcache`-wrlock → `bt_cache`-
wrlock ordering: a plain single insert or update on an indexed v2 object
goes `apply_commit` (`v2_update_apply_commit`, `storage.c:1433-1448`) →
`apply_index_diff` → `update_idx_fn` → `tg_idx_insert`/`btree_idx_insert`
(`index.c:66-74`) → `btree_insert` → `btree_insert_locked`
(`btree.c:1954-1957`) → `bt_acquire(&bt, path, 1)` — the *same* blocking
`bt_cache` wrlock, via a completely different function. Since the fix is
read-side only, one test proves the mechanism works in general — but
without a second test, this second writer chain is only argued to be
covered, never actually exercised by anything that would fail on the
base branch. Add it now, so both of the two independently-discovered
vulnerable writer chains have direct regression coverage.

Add `src/test/cases/test_ordered_walk_kfcache_deadlock_single_write.c`,
identical in structure to Task 1's test (same `TEST_BUILD` pause hook
from `order_walk_test_set_pause_hook`, same `RaceSync`-style barrier
pattern, same pre-fix/post-fix bounded-timeout-join assertion shape),
with exactly these differences:

1. **No 1,051-entry pre-population needed.** Unlike `btree_bulk_merge`,
   `btree_insert_locked` has no tree-size-based branch — it calls
   `bt_acquire(&bt, path, 1)` unconditionally on every call, empty tree or
   not (`btree.c:1954-1957`, confirmed by direct read: no size check,
   no rebuild/publish alternative exists for the single-entry insert
   path). So setup is just: create the object, find a `seed_key`/
   `writer_key` pair on the same kf-shard and same index-shard (same
   routing-loop as Task 1), insert `seed_key` with `F=1`. Nothing else.
2. **Writer thread issues a plain `mode:"insert"` request** —
   `{"mode":"insert","dir":...,"object":...,"key":"<writer_key>",
   "value":{"F":2}}` — not a bulk-insert. `writer_key` does not exist yet,
   so this takes the "fresh insert" branch of the slotcask upsert function
   (`slotcask.c`'s `else { /* Fresh insert: ... */ }` block, `apply_commit`
   call at line 5686-5687), which holds `kh` (the kfcache wrlock) across
   the `apply_commit` call exactly like the update branch does (line
   5946-5947) — confirmed by direct read that `kfcache_release(&kh)` does
   not run until after `apply_commit` returns on every path in both
   branches.
3. **Pre-fix hang diagnostic** should reference "writer still blocked in
   `btree_insert_locked`" (not `btree_bulk_merge`) in its failure message,
   to keep the two tests' diagnostics distinguishable when triaging a
   future regression.

Everything else — reader thread, pause hook registration, blocked-writer
poll, release/join/assert steps, and the regression-test proof step (red
before the fix, green after Tasks 2-6) — is identical to Task 1's, just
against this test's own binary name
(`test-ordered-walk-kfcache-deadlock-single-write`).

### Task 2 — `kfcache_acquire_ex` / `kfcache_try_acquire_rd` (section 1)

Build: `SKIP_TESTS=1 ./build.sh`. No test run yet (both regression tests
still red at this point per Task 1's and Task 1b's proof steps — don't
re-run either until Task 6).

### Task 3 — `kfcache_acquire_direct_ex` / `kfcache_try_acquire_direct` (section 2)

### Task 4 — `slotcask_lookup_scan_kf` factor + `slotcask_lookup_by_hash_try` (section 3)

### Task 5 — `read_record_ref_try` (section 4)

### Task 6 — `btree_idx_walk_ordered` yield/resume + all callback updates (sections 5, 6, 6a, 7)

Includes the composite `OP_IN` k-way merge's own reopen loop
(section 6a) — `find_via_composite_prefix` is a second, independent
k-way merge that holds `bt_cache` rdlocks across a `cursor_find_cb`-style
fetch exactly like `btree_idx_walk_ordered` does, and needs the same
fix using the same generalized `BtOrderedWalkHandle`.

After this task, run the full regression proof:
`./build/bin/shard-db-test run test-ordered-walk-kfcache-deadlock` (must
now pass) and `./build/bin/shard-db-test run-all` (must show no new
failures vs. the pre-change baseline).

### Task 7 — Dynamic-safety verification (per this repo's AGENTS.md standing exception)

This diff touches shared/cached state (`kfcache`, `bt_cache`) and lock
ordering directly — both sanitizer runs are required before this is done,
not deferred to CI:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1
```

Pay particular attention to TSAN output for
`test-ordered-walk-kfcache-deadlock` and any existing `btree_idx_walk_ordered`
/ `kfcache_acquire` / bulk-insert tests. Any new finding gets root-caused
and fixed now, or written up per AGENTS.md's suppression-with-rationale
process — never silently suppressed.

### Task 8 — Documentation sync

- `AGENTS.md`'s "Concurrency" bullet point / `docs/concepts/concurrency.md`
  (deep dive) should note the kfcache/bt_cache lock-ordering rule this fix
  establishes for future callback authors: **callbacks invoked from
  `btree_idx_walk_ordered` must use `read_record_ref_try` (via the
  `BtOrderedWalkHandle`) if they need to fetch the underlying record, not
  `read_record_ref` directly.**
- `docs/concepts/indexes.md`'s description of `btree_idx_walk_ordered`
  should mention the yield/resume protocol and its opaque handle.

## Execution rules

- Branch off `main`: `fix/kfcache-btree-lock-inversion`.
- Per this repo's standing exception, leave all work **uncommitted** after
  execution — human + a reviewer blind to this plan review the raw
  `git diff` before anything is committed.
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run-all`
  or `run <name>` for a single case.
- Do tasks in order (1 → 1b → 2 → 3 → 4 → 5 → 6 → 7 → 8). Task 1's and
  Task 1b's regression tests must both be written and proven to fail
  (hang/timeout) against the unfixed codebase before any of Tasks 2-6's
  fix code is written.
- If a quoted anchor in this plan isn't found verbatim in the file at
  execution time, **stop immediately** — write `PLAN_NOTES.md` at the repo
  root describing exactly which anchor didn't match and what was found
  instead, and halt the entire run, including any remaining unrelated
  task. Do not guess, reinterpret, or continue. Resuming requires a human
  (or the planning model, re-engaged) to read `PLAN_NOTES.md` and either
  hand back a patched plan or a fresh one.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.

## Addendum (2026-08-11): per-shard/per-cursor resume-floor defects found in review

### Status

Tasks 1–8 above were implemented and merged into this branch's working tree
(not yet committed, per this repo's standing execution-mode exception) and
passed the full suite plus ASan/UBSan/TSan. A review pass (human +
CORE-PROCESS.md's concurrency-diff checklist) found that the yield/resume
protocol itself — not the lock-order-inversion fix it implements — has a
correctness defect: **the "floor" that decides where each index shard/cursor
resumes scanning from after a release-and-reopen is tracked as a single
scalar shared by every shard/cursor, instead of per-shard/per-cursor state.**
This section documents the root cause, why Tasks 1–8's own test suite (and
the plan that specified them) didn't catch it, and the fix. It is additive:
Tasks 1–8 and everything above are unchanged and already done; this
addendum adds Tasks 9–15.

**This defect is independent of, and does not reopen, the original AB-BA
deadlock fix.** The lock-release/reopen *mechanism* (drop all `bt_cache`
rdlocks, do the blocking fetch, reopen) is sound — the bug is purely in
*where* each shard/cursor resumes from afterward.

### Root cause

`btree_idx_walk_ordered` (`src/db/index.c`) and `find_via_composite_prefix`'s
`OP_IN` k-way merge (`src/db/query.c`) each merge N independent per-shard (or
per-`(IN value, shard)`) B+tree range iterators into one globally-ordered
stream via a min/max-heap. When a callback hits kfcache contention, it calls
`btree_ordered_walk_release_for_blocking`, which closes **every** shard's
iterator at once (not just the one being fetched), does the blocking fetch,
and lets the walk fall through into a full reopen. The two implementations
diverge in how they reopen, and each diverges badly in its own way:

**`index.c` — silent skip.** The current code keeps exactly one pair of
scalars, `lo_val`/`hi_val` (plus a `resume_val`/`resume_hash` tie-break
pair), shared by all N shards. On release, whichever record is *currently
being delivered* — and, because `h.released` stays true for the rest of that
drain round, every subsequent record delivered in the same round — overwrites
this single scalar. Since the drain proceeds in ascending (or descending, for
`desc`) merged order, the value it ends up holding when the round ends is the
**highest** value delivered in that round, not the one that triggered the
release. On the next round, *every* shard reopens with `lo_val` (or `hi_val`
for `desc`) advanced to that single new floor — including shards whose own
last-delivered value was lower and which still have real, un-pulled entries
sitting between their own position and the new floor. Because the
force-close mechanism only ever lets each shard deliver the *one* entry it
already had buffered before dropping out of the heap (its iterator is
`NULL`, so its next `sc_pull` fails immediately), those in-between entries
are never pulled, never delivered, and never reachable again — the reopen
window starts strictly at-or-after the new floor. **Concretely**: shard A
buffers F=1 (delivered, triggers release) while shard B has F=3 buffered
(delivered in the same drain, becomes the new floor); shard A's *second*,
not-yet-pulled record F=2 is permanently lost, because round 2 reopens shard
A starting at F=3.

**`query.c` — silent duplication (a different, and independently
present, defect).** The `OP_IN` merge's per-cursor `lo`/`hi` (`cursors[i].lo`,
`cursors[i].hi`) are computed once, before the round loop, and — verified by
direct read of the current file, `src/db/query.c:1959-2002` — are **never
mutated inside the round loop**. Every reopen therefore restarts *every*
cursor from its original, pristine sub-range, relying entirely on the
`resume_val`/`resume_hash` exact-value-and-hash tie-break check to avoid
redelivering. But that check only recognizes the *one* specific
`(value, hash)` pair recorded at the *last* release-triggered update (same
"last write wins across the whole round" mechanism as `index.c`) — every
other value below the current floor, from this round or any earlier round,
has no such protection and gets redelivered in full on the next reopen. This
is not the plan's originally-specified design: plan Section 6a
(`docs/plans/2026-08-10-kfcache-btree-lock-inversion.md`, then lines
~1543–1569) called for maintaining `iv_lo[iv]`/`iv_hi[iv]` per-IN-value
saved sub-ranges that narrow on resume, mirroring `index.c`'s intent — the
narrowing step was dropped somewhere between plan and implementation, and
nothing caught the gap because (see below) the composite `OP_IN` path has no
deterministic test seam at all.

**Why the plan didn't catch the `index.c` skip.** The flawed design is
written into the plan's own pseudocode and its own stated invariant. Plan
lines ~1073–1082 (Section 5):

> "Recorded unconditionally here (not just on the call that first released)
> so the floor always reflects the LAST delivered entry, however many get
> drained before the heap empties. Only the bound on the walk's progression
> side moves."

and plan "Edge cases and invariants" (~lines 1718–1728):

> "Drained-round resume point must track the LAST delivered entry, not the
> first-yielded one... The tie-break floor check (`have_resume_hash`) is
> what makes redelivery merely inefficient rather than a correctness bug
> even if this were gotten wrong..."

Both statements are true only for a *single-shard* walk, or for the
duplicate-redelivery direction specifically. The plan's authors reasoned
about the tie-break check protecting against **re-delivering the same
value twice**, and never considered that a single shared floor **skips**
other shards' distinct, lower, un-pulled values. The plan's Section 6a for
`query.c` inherited the same single-global-floor shape by design (not an
implementation slip there — the *narrowing* was the slip), so the same blind
spot was duplicated into a second call site.

**Why Tasks 1 and 1b's tests didn't catch either bug.** Both existing
regression tests (`test_ordered_walk_kfcache_deadlock.c`,
`test_ordered_walk_kfcache_deadlock_single_write.c`) route **all** seed data
— `seed_key`, `writer_key`, and all 1,050 filler records — onto the *same*
single index shard (`idx_shard_for_hash(hash, DEADLOCK_SPLITS) != target_idx)
continue;` appears in the bulk-seed loop; `find_seed_and_writer` requires
both keys land on `target_idx`). With only one shard ever populated, there is
no second shard to lose data to, and the tests' own assertions
(`ASSERT_CONTAINS`, no exact-count check) wouldn't have caught a duplicate
either. The tests correctly prove the *lock-ordering* fix; they are
structurally incapable of exercising the *resume-floor* bug — a single-shard
walk's global floor and its (correct) per-shard floor are the same thing.
Separately, `find_via_composite_prefix`'s `composite_prefix_cb` (the `OP_IN`
merge's callback) has **no `TEST_BUILD` pause hook at all** — only
`order_index_walk_cb` does (`src/db/query.c`, the `g_order_walk_pause_fn`
block). The composite path's contention branch is reachable only by genuine,
unscheduled timing luck; nothing in the existing suite could hit it
deterministically even before considering what it does once hit.

### A pitfall discovered while designing the fix (not a currently-live bug)

A naive per-shard fix — just replace the single scalar with an array indexed
by shard/cursor — introduces a *new* duplicate risk: `sc_pull` (`index.c`)
and `comp_cursor_pull` (`query.c`) report "no more entries" identically
whether the iterator was force-closed by a release (real entries may remain
beyond it) or genuinely exhausted (`btree_range_iter_next` returned false —
truly nothing left, ever, for that shard within the original query range).
If every shard is unconditionally reopened every round, a shard that
genuinely exhausted itself in an earlier round — before some *other* shard's
later release moves a floor **below** its already-fully-delivered maximum —
would be reopened at that lower floor and re-walk (and redeliver) entries it
already emitted. The fix below distinguishes these two cases explicitly (a
`done` flag, set only when a pull fails with a still-non-`NULL` iterator,
i.e. genuine exhaustion, captured *before* any release can `NULL` it) so a
truly-finished shard/cursor is never reopened again, regardless of where the
floor moves afterward. This wasn't a live bug in the reviewed diff (no
existing test shape triggers it and the reviewed diff didn't attempt
per-shard tracking at all), but it's an invariant the fix must hold, so it's
called out here.

### A third defect: delivery reordering across a release boundary (found while re-validating the fix design)

Even with the per-shard/per-cursor `have`/`done` tracking above, the fix as
first drafted (Tasks 12/13, pre-correction) does not preserve `order_by`'s
ordering guarantee across a release event. This defect is independent of
the skip and duplicate bugs above; it survives fixing both of them, and
was found only while hand-tracing the drafted fix's exact round-by-round
mechanics against Task 10's own test layout — not by running anything.

**Mechanism.** `btree_ordered_walk_release_for_blocking` (called from
*inside* `cb()`, i.e. from inside the popped shard's own delivery)
force-closes *every* shard's iterator at once — including shards that
were not contended and are not the one currently being delivered. It does
**not** touch each cursor's already-buffered head value (`cursors[s].value`
/ `vlen` / `hash`); a shard that had already pulled its next entry before
the release keeps that entry available for delivery in the *same* round,
no fresh pull required. But the shard whose delivery *triggered* the
release has not yet pulled its own next entry — that pull happens *after*
`cb()` returns, via `sc_pull(bc)` — and by then its iterator is `NULL`, so
the pull fails immediately and that next (real, un-delivered) entry is
dropped for the round. The merge loop, as drafted in Tasks 12/13, keeps
draining the rest of the heap after a release (only the `while (nh > 0)`
loop's natural exhaustion or an `rc < 0` from the callback ends a round)
— so it goes on to pop and deliver whatever else is left in the heap,
including a now-stale buffered head from a shard that was never
contended, ahead of the record the triggering shard's next pull would
have produced.

**Concrete trace** (matches Task 10's own key layout): shard A
(`target_idx`) buffers `F=1`; shard B (`other_idx`) buffers `F=3`. Shard
A's `F=1` is popped and delivered, triggering the release (both shards'
iterators close). Per the drafted merge loop, `sc_pull` is then called for
shard A regardless — it now fails immediately (`iter == NULL`), dropping
shard A from the heap *without ever buffering its true next entry, `F=2`*.
Shard B's previously-buffered `F=3` is untouched by the close and gets
popped and delivered next. The round then drains to empty (both shards'
iterators are now `NULL`, so both drop out) and reopens: shard A resumes
from `F=1` (its own last delivery — correct), shard B has no resume floor
(it never triggered anything) and reopens from the pristine original
bound, refinding `F=3` again but skipping it via the exact-value tie-break
— meanwhile shard A's reopened cursor correctly reaches `F=2` this round
and delivers it, *after* `F=3` already went out in the previous round. Net
effect: the client sees `F=1, F=3, F=2` for an ascending `order_by=F`
query — the skip and duplicate bugs are both fixed (every value is
delivered, and delivered exactly once), but the order is wrong. This is a
real, client-visible `ORDER BY` violation, not an internal bookkeeping
detail: it would corrupt the output of any cursor-paginated or `order_by`
query that hits contention mid-walk under Tasks 12/13 as originally
drafted.

**Fix**: stop draining the round the instant a release happens. Once
`h.released` becomes true inside the loop body, record that one delivery's
resume point and `break` out of the `while (nh > 0)` loop immediately —
do not call `sc_pull`/re-sift for it, and do not continue popping the rest
of the round's heap. Every shard — the one that triggered the release, and
every other shard that still had a buffered-but-undelivered head — then
reopens fresh on the next round: the triggering shard resumes exactly
where it left off, and every other shard reopens from its own true
last-delivered point (or the pristine original bound, if it hasn't
delivered anything yet), never from a stale in-memory buffer that predates
the close. Because a fresh reopen always re-derives each shard's genuinely
current smallest remaining value before the heap is rebuilt, the heap's
merge order is valid again from that point on. This changes only the
`if (h.released) { ... }` branch in both call sites (`index.c`'s
`btree_idx_walk_ordered` and `query.c`'s `OP_IN` composite merge) — the
`ShardResume`/`CompResume` structures and `have`/`done` semantics from the
fix designs above are unchanged. It also *removes* code rather than adding
any (the post-release `sc_pull`/re-sift call for the releasing shard is no
longer reached), so this is a net simplification of Tasks 12/13's diff,
not an expansion. The fix design code below already has this correction
applied.

### Fix design — `src/db/index.c` (`btree_idx_walk_ordered`)

Replace the single shared `resume_val`/`resume_len`/`have_resume_hash`/
`resume_hash` scalars with a per-shard `ShardResume` array, and stop
mutating `lo_val`/`hi_val` (they become the walk's pristine, original,
never-mutated caller bounds; each shard computes its own effective open
bound from either its own resume point or these pristine bounds).

**Anchor** (`src/db/index.c`, immediately after `} ShardCursor;`):
```c
} ShardCursor;

/* Per-shard/per-cursor resume state for the release/reopen protocol.
 * `have` marks a shard that was force-closed mid-scan (real entries may
 * remain past `val`); `done` marks a shard that hit genuine end-of-range
 * (btree_range_iter_next returned false while its iterator was still
 * open) and must never be reopened again, however the floor moves for
 * other shards afterward. */
typedef struct {
    char    val[BT_MAX_VAL_LEN];
    size_t  len;
    uint8_t hash[BT_HASH_SIZE];
    int     have;
    int     done;
} ShardResume;
```

**Anchor** (`src/db/index.c`, replace the entire body of
`btree_idx_walk_ordered` — from its signature through the closing `}` — with
the version below; this is the exact current function, verified by direct
read, with the per-shard resume/done tracking substituted in):
```c
void btree_idx_walk_ordered(const char *db_root, const char *object,
                            const char *field, int splits,
                            const char *min_val, size_t min_len, int min_exclusive,
                            const char *max_val, size_t max_len, int max_exclusive,
                            int desc, bt_ordered_result_cb cb, void *ctx) {
    int n = index_splits_for(splits);
    ShardCursor *cursors = calloc((size_t)n, sizeof(ShardCursor));
    int *heap = calloc((size_t)n, sizeof(int));
    BtRangeIter ***slots = malloc((size_t)n * sizeof(BtRangeIter **));
    ShardResume *resume = calloc((size_t)n, sizeof(ShardResume));
    if (!cursors || !heap || !slots || !resume) {
        free(cursors); free(heap); free(slots); free(resume);
        return;
    }
    for (int s = 0; s < n; s++) slots[s] = &cursors[s].iter;
    BtOrderedWalkHandle h = { .slots = slots, .n = n, .released = 0 };

    char lo_val[BT_MAX_VAL_LEN];
    size_t lo_len = min_len > sizeof(lo_val) ? sizeof(lo_val) : min_len;
    if (lo_len) memcpy(lo_val, min_val, lo_len);
    int lo_excl = min_exclusive;
    char hi_val[BT_MAX_VAL_LEN];
    size_t hi_len = max_len > sizeof(hi_val) ? sizeof(hi_val) : max_len;
    if (hi_len) memcpy(hi_val, max_val, hi_len);
    int hi_excl = max_exclusive;

    for (;;) {
        int nh = 0;
        h.released = 0;
        for (int s = 0; s < n; s++) {
            if (resume[s].done) { cursors[s].iter = NULL; cursors[s].has_entry = 0; continue; }
            char idx_path[PATH_MAX];
            build_idx_path(idx_path, sizeof(idx_path), db_root, object, field, s);
            cursors[s].shard_id = s;
            const char *open_lo = lo_val, *open_hi = hi_val;
            size_t open_lo_len = lo_len, open_hi_len = hi_len;
            int open_lo_excl = lo_excl, open_hi_excl = hi_excl;
            if (resume[s].have) {
                if (desc) { open_hi = resume[s].val; open_hi_len = resume[s].len; open_hi_excl = 0; }
                else      { open_lo = resume[s].val; open_lo_len = resume[s].len; open_lo_excl = 0; }
            }
            cursors[s].iter = btree_range_iter_open(idx_path,
                                                    open_lo, open_lo_len, open_lo_excl,
                                                    open_hi, open_hi_len, open_hi_excl,
                                                    desc);
            if (cursors[s].iter) sc_pull(&cursors[s]);
            if (cursors[s].has_entry) heap[nh++] = s;
        }
        for (int i = nh / 2 - 1; i >= 0; i--)
            merge_sift_down(heap, nh, i, cursors, desc);

        while (nh > 0) {
            int sid = heap[0];
            ShardCursor *bc = &cursors[sid];
            if (resume[sid].have && bc->vlen == resume[sid].len &&
                memcmp(bc->value, resume[sid].val, resume[sid].len) == 0) {
                int hc = memcmp(bc->hash, resume[sid].hash, BT_HASH_SIZE);
                int already_delivered = desc ? (hc >= 0) : (hc <= 0);
                if (already_delivered) {
                    sc_pull(bc);
                    if (bc->has_entry) merge_sift_down(heap, nh, 0, cursors, desc);
                    else { heap[0] = heap[--nh]; if (nh > 0) merge_sift_down(heap, nh, 0, cursors, desc); }
                    continue;
                }
            }

            int rc = cb(bc->value, bc->vlen, bc->hash, &h, ctx);
            if (rc < 0) {
                for (int s = 0; s < n; s++)
                    if (cursors[s].iter) btree_range_iter_close(cursors[s].iter);
                free(cursors); free(heap); free(slots); free(resume);
                return;
            }
            if (h.released) {
                /* Stop draining this round immediately — the rest of the
                 * heap holds pre-release buffered heads that no longer
                 * reflect true global order across shards whose iterator
                 * just got force-closed before their own next (possibly
                 * smaller) entry could be pulled. Record only this one
                 * delivery's resume point; every shard, including this
                 * one, reopens fresh next round instead of continuing to
                 * drain stale buffered heads. See "A third defect: delivery
                 * reordering across a release boundary" above. */
                resume[sid].len = bc->vlen > sizeof(resume[sid].val) ? sizeof(resume[sid].val) : bc->vlen;
                memcpy(resume[sid].val, bc->value, resume[sid].len);
                memcpy(resume[sid].hash, bc->hash, BT_HASH_SIZE);
                resume[sid].have = 1;
                break;
            }
            int had_iter = (bc->iter != NULL);
            sc_pull(bc);
            if (bc->has_entry) {
                merge_sift_down(heap, nh, 0, cursors, desc);
            } else {
                if (had_iter) resume[sid].done = 1;
                heap[0] = heap[--nh];
                if (nh > 0) merge_sift_down(heap, nh, 0, cursors, desc);
            }
        }
        if (!h.released) break;
    }

    for (int s = 0; s < n; s++)
        if (cursors[s].iter) btree_range_iter_close(cursors[s].iter);
    free(cursors);
    free(heap);
    free(slots);
    free(resume);
}
```

### Fix design — `src/db/query.c` (`find_via_composite_prefix`'s `OP_IN` merge)

Same shape: a `CompResume` array indexed by cursor index `i` (stable across
rounds — `cursors[i]` is always reopened against the same `(iv, shard)` pair
every round, since the per-cursor `lo`/`hi` are computed once before the
round loop and the `for (i = 0; i < total_cursors; i++)` reopen loop
preserves index order). Unlike the reviewed diff, this version *does* narrow
each cursor's effective open bound from its own resume point (fixing the
duplicate) and adds the same `done` tracking as `index.c` (fixing the same
pitfall described above).

**Anchor** (`src/db/query.c`, immediately after `} CompMergeCursor;`):
```c
} CompMergeCursor;

/* Per-cursor resume state — see the identical ShardResume in index.c for
 * the have/done rationale. Indexed by cursor index i (stable across
 * rounds: i always maps to the same (IN value, shard) pair). */
typedef struct {
    char    val[BT_MAX_VAL_LEN];
    size_t  len;
    uint8_t hash[BT_HASH_SIZE];
    int     have;
    int     done;
} CompResume;
```

**Anchor** (`src/db/query.c`, the `OP_IN` block inside
`find_via_composite_prefix` — replace from
`if (seed->op == OP_IN && seed->in_count > 0) {` through that block's
closing `}` verbatim as follows; everything through the per-`(iv, shard)`
`cursors[ci]` population loop, lines ~1886–1970, is unchanged and omitted
below for brevity except where noted — only the allocation line and
everything from `int total_cursors = ci;` onward changes):
```c
    if (seed->op == OP_IN && seed->in_count > 0) {
        int nv = seed->in_count;
        int ns = index_splits_for(sch->splits);
        int total = ns * nv;
        CompMergeCursor *cursors = calloc((size_t)total, sizeof(CompMergeCursor));
        int *heap = calloc((size_t)total, sizeof(int));
        BtRangeIter ***slots = malloc((size_t)total * sizeof(BtRangeIter **));
        CompResume *resume = calloc((size_t)total, sizeof(CompResume));
        if (!cursors || !heap || !slots || !resume) {
            free(cursors); free(heap); free(slots); free(resume);
            return 0;
        }

        /* ... unchanged: seed_tf resolution and the per-(iv, shard) loop
           populating cursors[ci].{stream_id,lo,lo_len,lo_exclusive,hi,
           hi_len,hi_exclusive} and slots[ci], lines ~1895-1970 ... */

        int total_cursors = ci;
        BtOrderedWalkHandle h = { .slots = slots, .n = total_cursors, .released = 0 };

        CompositePrefixCtx ctx;
        memset(&ctx, 0, sizeof(ctx));
        ctx.db_root = db_root; ctx.object = object; ctx.sch = sch; ctx.fs = fs;
        ctx.tree = tree; ctx.excluded = excluded;
        ctx.proj_fields = proj_fields; ctx.proj_count = proj_count;
        ctx.dict_fmt = dict_fmt;
        ctx.skip_remaining = (offset > 0) ? offset : 0;
        ctx.limit = (limit > 0)  ? limit  : INT_MAX;
        ctx.dl = dl; ctx.dl_counter = 0; ctx.parent_out = g_out;
        pthread_mutex_init(&ctx.lock, NULL);

        int abort_requested = 0;
        for (;;) {
            int nh = 0;
            h.released = 0;
            for (int i = 0; i < total_cursors; i++) {
                if (resume[i].done) { cursors[i].iter = NULL; cursors[i].has_entry = 0; continue; }
                char ip[PATH_MAX];
                int shard = i % ns;
                build_idx_path(ip, sizeof(ip), db_root, object, composite_field, shard);
                const uint8_t *open_lo = cursors[i].lo, *open_hi = cursors[i].hi;
                size_t open_lo_len = cursors[i].lo_len, open_hi_len = cursors[i].hi_len;
                int open_lo_excl = cursors[i].lo_exclusive, open_hi_excl = cursors[i].hi_exclusive;
                if (resume[i].have) {
                    if (order_desc) { open_hi = (const uint8_t *)resume[i].val; open_hi_len = resume[i].len; open_hi_excl = 0; }
                    else            { open_lo = (const uint8_t *)resume[i].val; open_lo_len = resume[i].len; open_lo_excl = 0; }
                }
                cursors[i].iter = btree_range_iter_open(ip,
                    (const char *)open_lo, open_lo_len, open_lo_excl,
                    (const char *)open_hi, open_hi_len, open_hi_excl, order_desc);
                if (cursors[i].iter) comp_cursor_pull(&cursors[i]);
                if (cursors[i].has_entry) heap[nh++] = i;
            }

            for (int i = nh/2-1; i >= 0; i--)
                comp_merge_sift_down(heap, nh, i, cursors, order_desc);

            while (nh > 0) {
                int sid = heap[0];
                CompMergeCursor *bc = &cursors[sid];
                if (resume[sid].have && bc->vlen == resume[sid].len &&
                    memcmp(bc->value, resume[sid].val, resume[sid].len) == 0) {
                    int hc = memcmp(bc->hash, resume[sid].hash, BT_HASH_SIZE);
                    int already_delivered = order_desc ? (hc >= 0) : (hc <= 0);
                    if (already_delivered) {
                        comp_cursor_pull(bc);
                        if (bc->has_entry) comp_merge_sift_down(heap, nh, 0, cursors, order_desc);
                        else { heap[0] = heap[--nh]; if (nh > 0) comp_merge_sift_down(heap, nh, 0, cursors, order_desc); }
                        continue;
                    }
                }

                if (composite_prefix_cb(bc->value, bc->vlen, bc->hash, &h, &ctx) < 0) {
                    abort_requested = 1;
                    break;
                }
                if (h.released) {
                    /* See the identical comment in index.c's
                     * btree_idx_walk_ordered — stop draining the rest of
                     * this round's heap immediately, every cursor reopens
                     * fresh next round instead of delivering stale
                     * pre-release buffered heads out of true order. */
                    resume[sid].len = bc->vlen > sizeof(resume[sid].val) ? sizeof(resume[sid].val) : bc->vlen;
                    memcpy(resume[sid].val, bc->value, resume[sid].len);
                    memcpy(resume[sid].hash, bc->hash, BT_HASH_SIZE);
                    resume[sid].have = 1;
                    break;
                }
                int had_iter = (bc->iter != NULL);
                comp_cursor_pull(bc);
                if (bc->has_entry) {
                    comp_merge_sift_down(heap, nh, 0, cursors, order_desc);
                } else {
                    if (had_iter) resume[sid].done = 1;
                    heap[0] = heap[--nh];
                    if (nh > 0) comp_merge_sift_down(heap, nh, 0, cursors, order_desc);
                }
            }
            if (abort_requested || !h.released) break;
        }

        pthread_mutex_destroy(&ctx.lock);
        for (int i = 0; i < total_cursors; i++)
            if (cursors[i].iter) btree_range_iter_close(cursors[i].iter);
        free(cursors); free(heap); free(slots); free(resume);
        return ctx.printed;
    }
```
`cursors[i].lo`/`cursors[i].hi` are declared `uint8_t [1024 + 8]` (see
`CompMergeCursor`, `src/db/query.c:1810-1815`); `open_lo`/`open_hi` above are
typed `const uint8_t *` to match, then cast to `const char *` at the
`btree_range_iter_open` call site the same way the surrounding code already
does implicitly (`(const char *)cursors[i].lo` pattern used elsewhere in this
function).

### Adding a deterministic test seam to `composite_prefix_cb`

The `OP_IN` merge currently has no way to park deterministically on
contention (see root cause above). Reuse the existing one-shot
`order_walk_test_set_pause_hook`/`g_order_walk_pause_fn` machinery for both
callbacks instead of adding a second hook: relocate its definition earlier in
`src/db/query.c` (it currently sits directly above `order_index_walk_cb`,
after `composite_prefix_cb`, sharing one `#ifdef TEST_BUILD` block with the
`g_order_walk_scanned` counter definitions that precede it) so
`composite_prefix_cb` can also call it. The counter definitions
(`g_order_walk_scanned`/`order_walk_scanned_for_test`/
`order_walk_scanned_reset_for_test`) stay exactly where they are — they're
referenced via `extern` elsewhere in this file, including already inside
`composite_prefix_cb` itself (`src/db/query.c:1630`) — only the pause-hook
typedef/statics/setter move.

> **Correction (2026-08-11, post-halt):** an earlier version of this anchor
> quoted the pause-hook block as if it opened its own standalone
> `#ifdef TEST_BUILD`. The actual current source wraps the scan-counter
> definitions and the pause-hook definitions in one shared `#ifdef
> TEST_BUILD ... #endif`, with the counters first. Task 9 execution halted
> on this exact mismatch (see `PLAN_NOTES.md` from that run) before any
> source change was made. The anchors below are corrected against the
> verified current source and split that shared block in two: the counter
> half stays in place under its own `#ifdef`/`#endif`, and only the
> pause-hook half is deleted from here and reinserted before
> `composite_prefix_cb` under a new `#ifdef`/`#endif` of its own.

**Anchor** (`src/db/query.c`, replace this block — verified exact current
text, immediately above `static int order_index_walk_cb(`):
```c
#ifdef TEST_BUILD
/* Counts index entries visited by the order-by walks, so a test can prove a
 * windowed query stops at the window instead of scanning the whole index. */
long g_order_walk_scanned = 0;
long order_walk_scanned_for_test(void)   { return g_order_walk_scanned; }
void order_walk_scanned_reset_for_test(void) { g_order_walk_scanned = 0; }

/* One-shot pause hook: fires just before order_index_walk_cb attempts its
   record fetch, while btree_idx_walk_ordered still holds every shard's
   bt_cache rdlock. Lets test_ordered_walk_kfcache_deadlock.c park the
   walk there deterministically, then start a concurrent indexed write
   that needs the SAME kf-shard's wrlock and the SAME index shard's
   bt_cache wrlock — reproducing the production AB-BA deadlock without
   relying on scheduling luck. */
typedef void (*order_walk_test_pause_fn)(void *ctx);
static order_walk_test_pause_fn g_order_walk_pause_fn = NULL;
static void *g_order_walk_pause_ctx = NULL;
void order_walk_test_set_pause_hook(order_walk_test_pause_fn fn, void *ctx) {
    g_order_walk_pause_fn = fn;
    g_order_walk_pause_ctx = ctx;
}
#endif

static int order_index_walk_cb(const char *val, size_t vlen,
```
with:
```c
#ifdef TEST_BUILD
/* Counts index entries visited by the order-by walks, so a test can prove a
 * windowed query stops at the window instead of scanning the whole index. */
long g_order_walk_scanned = 0;
long order_walk_scanned_for_test(void)   { return g_order_walk_scanned; }
void order_walk_scanned_reset_for_test(void) { g_order_walk_scanned = 0; }
#endif

static int order_index_walk_cb(const char *val, size_t vlen,
```
(The pause-hook typedef/statics/setter and its comment are removed from
here entirely — not kept in any form at this location — and reappear only
at the new site below.) `order_index_walk_cb`'s own existing
`#ifdef TEST_BUILD if (g_order_walk_pause_fn) {...}` call block, later in
its body, is unchanged by this edit.

**Anchor** (`src/db/query.c`, insert the relocated block — comment updated
to reflect both call sites — immediately before
`static int composite_prefix_cb(const char *val, size_t vlen,`):
```c
#ifdef TEST_BUILD
/* One-shot pause hook: fires just before order_index_walk_cb or
   composite_prefix_cb attempts its record fetch, while the owning k-way
   merge still holds every shard's bt_cache rdlock. Lets a test park the
   walk there deterministically, then start a concurrent indexed write
   that needs the SAME kf-shard's wrlock and the SAME index shard's
   bt_cache wrlock — reproducing contention without relying on scheduling
   luck. Shared by both callbacks: only one walk type runs per test. */
typedef void (*order_walk_test_pause_fn)(void *ctx);
static order_walk_test_pause_fn g_order_walk_pause_fn = NULL;
static void *g_order_walk_pause_ctx = NULL;
void order_walk_test_set_pause_hook(order_walk_test_pause_fn fn, void *ctx) {
    g_order_walk_pause_fn = fn;
    g_order_walk_pause_ctx = ctx;
}
#endif

static int composite_prefix_cb(const char *val, size_t vlen,
```

**Anchor** (`src/db/query.c`, inside `composite_prefix_cb`, immediately
before its fetch — verified exact current text):
```c
    (void)val; (void)vlen;   /* composite leaf value not needed; hash16 is the key */
    CompositePrefixCtx *c = (CompositePrefixCtx *)ctx;
    g_out = c->parent_out;
    if (query_deadline_tick(c->dl, &c->dl_counter)) return -1;
    if (c->printed >= c->limit) return -1;

    /* Fetch the record by hash16. */
    RecordRef rr;
    int rc = read_record_ref_try(c->db_root, c->object, c->sch, hash16, &rr);
```
becomes:
```c
    (void)val; (void)vlen;   /* composite leaf value not needed; hash16 is the key */
    CompositePrefixCtx *c = (CompositePrefixCtx *)ctx;
    g_out = c->parent_out;
    if (query_deadline_tick(c->dl, &c->dl_counter)) return -1;
    if (c->printed >= c->limit) return -1;

#ifdef TEST_BUILD
    if (g_order_walk_pause_fn) {
        order_walk_test_pause_fn fn = g_order_walk_pause_fn;
        g_order_walk_pause_fn = NULL;
        fn(g_order_walk_pause_ctx);
    }
#endif

    /* Fetch the record by hash16. */
    RecordRef rr;
    int rc = read_record_ref_try(c->db_root, c->object, c->sch, hash16, &rr);
```

### Edge cases and invariants (addendum)

- **A shard/cursor with no delivery this round reopens from the pristine
  original bound, not some intermediate point — this is correct, desired
  behavior, not a hazard.** If shard S never delivered anything in a given
  round (e.g. it was already past its own resume point and simply had
  nothing new to contribute while other shards drained), `resume[S].have`
  stays whatever it was from an earlier round (or 0 if S has never yet
  delivered). Reopening it from that point (or from the true original
  caller bound, if it's never delivered at all) is exactly right — it also
  means a shard that hasn't started yet, or has genuinely fallen behind,
  still gets a chance to pick up brand-new concurrent inserts that land in
  its range before the walk finishes. This must not be "fixed" — it's the
  point of not narrowing a shard's bound until it has actually delivered
  from it.
- **`done` must be set only when the iterator was non-`NULL` going into the
  failed pull**, i.e. `btree_range_iter_next` itself returned false. If the
  iterator was already `NULL` (force-closed by a release — either this
  shard's own, or another shard's, since release closes every slot at
  once), that shard/cursor's failed pull carries no information about
  whether more entries exist past its current position; `done` must stay 0
  so it gets a real reopen-and-check next round. Capture "was the iterator
  non-`NULL`" (`had_iter`) *before* calling `sc_pull`/`comp_cursor_pull`,
  since the pull call itself doesn't preserve that information afterward.
- **The `have_resume`-equal-value tie-break check still applies per-shard/
  per-cursor**, unchanged in spirit from the original (buggy) global
  version — it exists to skip re-delivering the exact `(value, hash)` pair
  a shard/cursor was reopened *at* (since the reopen bound is inclusive).
  It is now keyed by `resume[sid]` instead of a shared global, so it can no
  longer falsely suppress a *different* shard's delivery of the same value
  (a real possibility with ties across shards) the way reusing one global
  hash would.
- **At most one shard/cursor advances per round once a release fires** — the
  `break` on `h.released` (see "A third defect" above) means a round either
  completes with no release at all (every shard/cursor drains normally, no
  reordering possible since nothing was force-closed mid-drain), or it
  delivers exactly one more record — the one whose callback triggered the
  release — before ending. This trades a small amount of throughput under
  sustained contention (more frequent close/reopen cycles) for the ordering
  guarantee; it does not change the skip/duplicate fix's correctness, which
  depends only on `have`/`done` per shard/cursor, not on how many records a
  round delivers.
- **This fix does not change on-disk format, wire protocol, or any
  caller-visible behavior** — `btree_idx_walk_ordered`'s and
  `find_via_composite_prefix`'s signatures and callback contracts are
  unchanged from Tasks 1–8; this is a pure internal-correctness fix to
  code that was never released (this branch is not yet merged).
- **Memory**: both fixes add one `calloc((size_t)n_or_total, sizeof(...))`
  each, freed on every exit path (early `rc < 0` abort, normal completion,
  and the initial allocation-failure guard) — mirrors the existing
  `cursors`/`heap`/`slots` lifetime exactly.

### Tasks (continued)

Tasks 1–8 above are done. The following continue that numbering.

### Task 9 — Test seam: relocate the pause hook and wire it into `composite_prefix_cb` (test-first prerequisite)

Apply the three anchored edits under "Adding a deterministic test seam to
`composite_prefix_cb`" above. Build only (`SKIP_TESTS=1 ./build.sh`); no
behavior change yet (the hook is inert until a test sets it), so the
existing suite must still pass unchanged —
`./build/bin/shard-db-test run-all` — before continuing.

### Task 10 — Regression test: `index.c` multi-shard skip (test-first)

> **Correction (2026-08-11, post-halt):** execution reached this test and
> hit `ETIMEDOUT` waiting for the pause hook — the planner never took the
> `order_index_walk_cb` path at all. Root cause: `pick_sort_or_walk`
> (`src/db/query_plan.c:3025`) only forces `FP_ORDER_INDEX_WALK` when the
> seed leaf's capped cardinality estimate is unbounded/saturated
> (`card_est_leaf`, `query_plan.c:2548`, budget ≈ `N /
> RANDOM_SEQ_COST_RATIO` ≈ 131 for this test's ~1053-record object,
> `selectivity_budget`, `query_plan.c:2822`). The two-leaf criteria below
> (`F >= 1 AND F < 1000`) makes `F < 1000` — matching only the 3 real rows
> — the more selective, non-saturated leaf; `most_selective_indexed` picks
> it as the seed, `prefer_fetch_sort(3, 1053, 0, 10, 0)` evaluates true
> (`3² = 9 < 10 × 1053`), and the planner falls back to `FP_ORDER_SORT`
> (fetch + in-memory sort), which never calls `order_index_walk_cb`. The
> fix is a single, unbounded leaf on `F` instead: every one of the 1050
> filler records also has `F >= 1`, so that leaf's own capped estimate
> saturates against the whole object, forcing `pick_sort_or_walk`'s
> `!se.estimable || se.saturated` branch to return `FP_ORDER_INDEX_WALK`
> unconditionally (`order_field_drivable` confirms `F` is btree-indexed,
> so `driv` is true) — regardless of the true 3-row match count, which
> `limit=10` still bounds correctly. The criteria below reflects this
> fix; the test's assertions (including the strict F-order check, which
> now also doubles as regression coverage for "A third defect" above) are
> unchanged, verified by hand-trace to still hold against the corrected
> fix design.

Add `src/test/cases/test_ordered_walk_multishard_skip.c`:

```c
/* Regression for the per-shard resume-floor bug in btree_idx_walk_ordered's
 * release/reopen protocol (docs/plans/2026-08-10-kfcache-btree-lock-inversion.md,
 * addendum). A single global resume floor, shared across all index shards,
 * silently strands any shard's un-pulled entries that fall between its own
 * last-delivered value and the round's globally-highest delivered value once
 * every shard reopens from that single floor. This test constructs exactly
 * that shape: two records on the SAME index shard (F=1, F=2) and one record
 * on the OTHER index shard (F=3) that sorts higher and gets delivered in the
 * same release-triggered drain, becoming the (broken) global floor. F=2 is
 * then permanently unreachable on reopen. The criteria is a single,
 * unbounded `F >= 1` leaf (not `F >= 1 AND F < 1000`) — deliberately, so
 * its own capped cardinality estimate saturates against the object's 1050
 * filler records and the planner is forced into the order-index-walk path
 * unconditionally; a bounded/selective leaf lets the planner choose
 * fetch+sort instead, which never reaches this walk at all. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "query_internal.h"
#include "shard_db_internal.h"
#include "types.h"
#include "btree.h"
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define SKIP_SPLITS 8               /* index_splits_for(8) == 2 shards */
#define BULK_FILLER_COUNT 1050
#define KEY_SEARCH_LIMIT 100000
#define JOIN_TIMEOUT_SEC 5

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int reached;
    int release;
} RaceSync;

static void race_sync_init(RaceSync *s) {
    pthread_mutex_init(&s->lock, NULL);
    pthread_cond_init(&s->cond, NULL);
    s->reached = 0;
    s->release = 0;
}

static void race_sync_destroy(RaceSync *s) {
    pthread_mutex_destroy(&s->lock);
    pthread_cond_destroy(&s->cond);
}

static void skip_pause(void *ctx) {
    RaceSync *s = ctx;
    pthread_mutex_lock(&s->lock);
    s->reached = 1;
    pthread_cond_broadcast(&s->cond);
    while (!s->release) pthread_cond_wait(&s->cond, &s->lock);
    pthread_mutex_unlock(&s->lock);
}

typedef struct {
    ShardDb *db;
    const char *dir;
    const char *object;
    const char *writer_key;
    int role; /* 1 = ordered find, 2 = indexed write */
    char *response;
    int rc;
} QueryArgs;

static void *query_thread_main(void *arg) {
    QueryArgs *a = arg;
    char db_root[PATH_MAX];
    snprintf(db_root, sizeof(db_root), "%s/%s", a->db->db_root, a->dir);
    g_db = a->db;
    size_t out_len = 0;
    FILE *out = open_memstream(&a->response, &out_len);
    if (!out) { a->rc = -1; return NULL; }
    g_out = out;
    if (a->role == 1) {
        a->rc = cmd_find(db_root, a->object,
            "[{\"field\":\"F\",\"op\":\"gte\",\"value\":\"1\"}]",
            0, 10, NULL, NULL, NULL, NULL, NULL, "F", "asc", NULL, 0);
    } else {
        char bulk[256];
        snprintf(bulk, sizeof(bulk),
            "[{\"key\":\"%s\",\"value\":{\"F\":500}}]",
            a->writer_key);
        a->rc = cmd_bulk_insert_string(db_root, a->object, bulk, 0);
    }
    fflush(out);
    fclose(out);
    g_out = NULL;
    return NULL;
}

static int timed_join(pthread_t tid, int seconds) {
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += seconds;
    return pthread_timedjoin_np(tid, NULL, &deadline);
}

static int request_ok(ShardDb *db, const char *request, char **response) {
    size_t out_len = 0;
    *response = NULL;
    int rc = shard_db_query(db, request, response, &out_len);
    return rc == 0 && *response && strstr(*response, "\"error\"") == NULL;
}

static int route_same(const uint8_t a[16], const uint8_t b[16]) {
    int kf_a = compute_record_shard(a, SKIP_SPLITS);
    int kf_b = compute_record_shard(b, SKIP_SPLITS);
    return kf_a == kf_b && idx_shard_for_hash(a, SKIP_SPLITS) ==
        idx_shard_for_hash(b, SKIP_SPLITS);
}

static int find_seed_and_writer(char *seed_key, size_t seed_cap,
                                char *writer_key, size_t writer_cap,
                                int *target_idx) {
    uint8_t seed_hash[16];
    for (int i = 0; i < KEY_SEARCH_LIMIT; i++) {
        char candidate[64];
        snprintf(candidate, sizeof(candidate), "seed_%d", i);
        compute_hash_raw(candidate, strlen(candidate), seed_hash);
        for (int j = i + 1; j < KEY_SEARCH_LIMIT; j++) {
            char other[64];
            snprintf(other, sizeof(other), "writer_%d", j);
            uint8_t other_hash[16];
            compute_hash_raw(other, strlen(other), other_hash);
            if (!route_same(seed_hash, other_hash)) continue;
            snprintf(seed_key, seed_cap, "%s", candidate);
            snprintf(writer_key, writer_cap, "%s", other);
            *target_idx = idx_shard_for_hash(seed_hash, SKIP_SPLITS);
            return 1;
        }
    }
    return 0;
}

static int find_key_on_idx_shard(char *out_key, size_t cap, int wanted_idx,
                                 const char *prefix) {
    for (int i = 0; i < KEY_SEARCH_LIMIT; i++) {
        char candidate[64];
        snprintf(candidate, sizeof(candidate), "%s_%d", prefix, i);
        uint8_t hash[16];
        compute_hash_raw(candidate, strlen(candidate), hash);
        if (idx_shard_for_hash(hash, SKIP_SPLITS) == wanted_idx) {
            snprintf(out_key, cap, "%s", candidate);
            return 1;
        }
    }
    return 0;
}

static int append_bulk_record(char *buf, size_t cap, size_t *used,
                              const char *key, int field, int first) {
    int n = snprintf(buf + *used, cap - *used,
        "%s{\"key\":\"%s\",\"value\":{\"F\":%d}}",
        first ? "" : ",", key, field);
    if (n < 0 || (size_t)n >= cap - *used) return -1;
    *used += (size_t)n;
    return 0;
}

static int count_occurrences(const char *haystack, const char *needle) {
    int n = 0;
    const char *p = haystack;
    size_t nl = strlen(needle);
    while (haystack && (p = strstr(p, needle)) != NULL) { n++; p += nl; }
    return n;
}

static int test_ordered_walk_multishard_skip_run(void) {
    ShardDb *db = test_get_process_db();
    const char *dir = "ordered_skip";
    const char *object = "rows";
    char request[512];
    char *response = NULL;
    char seed_key[64], writer_key[64], k2_key[64], k3_key[64];
    int target_idx = -1, other_idx = -1;

    ASSERT_TRUE(db != NULL, "process-local database available");
    if (!db) return 1;
    snprintf(request, sizeof(request),
        "{\"mode\":\"add-dir\",\"dir\":\"%s\"}", dir);
    request_ok(db, request, &response);
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"create-object\",\"dir\":\"%s\","
        "\"object\":\"%s\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"F:int\"],\"indexes\":[\"F\"]}", dir, object);
    ASSERT_TRUE(request_ok(db, request, &response), "create indexed object");
    free(response); response = NULL;

    ASSERT_TRUE(find_seed_and_writer(seed_key, sizeof(seed_key),
                                     writer_key, sizeof(writer_key),
                                     &target_idx),
                "find same kf/index shard seed/writer pair");
    if (target_idx < 0) goto cleanup;
    other_idx = 1 - target_idx; /* index_splits_for(8) == 2 */

    ASSERT_TRUE(find_key_on_idx_shard(k2_key, sizeof(k2_key), target_idx, "k2cand"),
                "find second key on the same index shard as seed");
    ASSERT_TRUE(find_key_on_idx_shard(k3_key, sizeof(k3_key), other_idx, "k3cand"),
                "find key on the other index shard");

    size_t cap = 130000;
    char *bulk = malloc(cap);
    ASSERT_NOT_NULL(bulk, "allocate bulk filler request");
    if (!bulk) goto cleanup;
    size_t used = (size_t)snprintf(bulk, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"%s\","
        "\"object\":\"%s\",\"records\":[", dir, object);
    int count = 0;
    for (int i = 0; i < KEY_SEARCH_LIMIT && count < BULK_FILLER_COUNT; i++) {
        char key[64];
        snprintf(key, sizeof(key), "bulk_%d", i);
        uint8_t hash[16];
        compute_hash_raw(key, strlen(key), hash);
        if (idx_shard_for_hash(hash, SKIP_SPLITS) != target_idx) continue;
        if (append_bulk_record(bulk, cap, &used, key, 1000 + i, count == 0) != 0) {
            ASSERT_TRUE(0, "bulk filler request fits in buffer");
            free(bulk);
            goto cleanup;
        }
        count++;
    }
    ASSERT_EQ_INT(count, BULK_FILLER_COUNT, "collect 1050 same-shard filler records");
    if (count != BULK_FILLER_COUNT) { free(bulk); goto cleanup; }
    if (used + 3 >= cap) { free(bulk); goto cleanup; }
    memcpy(bulk + used, "]}", 3);
    used += 2;
    bulk[used] = '\0';
    ASSERT_TRUE(request_ok(db, bulk, &response), "prepopulate splice-path index shard");
    free(response); response = NULL;
    free(bulk);

    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"F\":1}}", dir, object, seed_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert seed record F=1");
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"F\":2}}", dir, object, k2_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert same-shard record F=2");
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"F\":3}}", dir, object, k3_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert other-shard record F=3");
    free(response); response = NULL;

    RaceSync sync;
    race_sync_init(&sync);
    order_walk_test_set_pause_hook(skip_pause, &sync);

    QueryArgs reader = { .db = db, .dir = dir, .object = object, .role = 1 };
    pthread_t reader_tid;
    ASSERT_EQ_INT(pthread_create(&reader_tid, NULL, query_thread_main, &reader),
                  0, "start ordered reader");

    pthread_mutex_lock(&sync.lock);
    struct timespec hook_deadline;
    clock_gettime(CLOCK_REALTIME, &hook_deadline);
    hook_deadline.tv_sec += JOIN_TIMEOUT_SEC;
    int hook_wait_rc = 0;
    while (!sync.reached && hook_wait_rc == 0)
        hook_wait_rc = pthread_cond_timedwait(&sync.cond, &sync.lock, &hook_deadline);
    int hook_reached = sync.reached;
    pthread_mutex_unlock(&sync.lock);
    if (!hook_reached) {
        TAP_DIAG("# reader never reached the parked order-index fetch: %s\n",
                 hook_wait_rc == ETIMEDOUT ? "ETIMEDOUT" : strerror(hook_wait_rc));
        ASSERT_TRUE(0, "ordered reader reaches pause hook before timeout");
        order_walk_test_set_pause_hook(NULL, NULL);
        _exit(1);
    }

    QueryArgs writer = {
        .db = db, .dir = dir, .object = object, .writer_key = writer_key, .role = 2
    };
    pthread_t writer_tid;
    ASSERT_EQ_INT(pthread_create(&writer_tid, NULL, query_thread_main, &writer),
                  0, "start indexed writer");

    int writer_pending = 0;
    for (int waited = 0; waited < JOIN_TIMEOUT_SEC * 10; waited++) {
        if (btree_test_writer_pending_count() > 0) { writer_pending = 1; break; }
        struct timespec poll = { 0, 100 * 1000000L };
        nanosleep(&poll, NULL);
    }
    ASSERT_TRUE(writer_pending, "writer reaches bt_cache writer acquisition");
    pthread_mutex_lock(&sync.lock);
    sync.release = 1;
    pthread_cond_broadcast(&sync.cond);
    pthread_mutex_unlock(&sync.lock);

    int reader_join = timed_join(reader_tid, JOIN_TIMEOUT_SEC);
    int writer_join = timed_join(writer_tid, JOIN_TIMEOUT_SEC);
    if (reader_join != 0 || writer_join != 0) {
        ASSERT_TRUE(0, "ordered walk and indexed writer finish before timeout");
        fflush(NULL);
        _exit(1);
    }

    ASSERT_EQ_INT(reader.rc, 0, "ordered reader request succeeds");
    ASSERT_EQ_INT(writer.rc, 0, "indexed writer request succeeds");
    ASSERT_CONTAINS(reader.response, seed_key, "reader returns F=1 seed record");

    /* The regression: F=2's key must survive the release/reopen. Pre-fix
       (single global resume floor), it is permanently skipped once F=3
       (the other shard) advances the shared floor past it. */
    ASSERT_CONTAINS(reader.response, k2_key, "reader returns F=2 record (regression)");
    ASSERT_CONTAINS(reader.response, k3_key, "reader returns F=3 record");
    ASSERT_EQ_INT(count_occurrences(reader.response, seed_key), 1,
                  "F=1 record appears exactly once");
    ASSERT_EQ_INT(count_occurrences(reader.response, k2_key), 1,
                  "F=2 record appears exactly once");
    ASSERT_EQ_INT(count_occurrences(reader.response, k3_key), 1,
                  "F=3 record appears exactly once");

    const char *p1 = reader.response ? strstr(reader.response, seed_key) : NULL;
    const char *p2 = reader.response ? strstr(reader.response, k2_key) : NULL;
    const char *p3 = reader.response ? strstr(reader.response, k3_key) : NULL;
    ASSERT_TRUE(p1 && p2 && p3 && p1 < p2 && p2 < p3,
                "records returned in F order (1, 2, 3)");

    free(reader.response);
    free(writer.response);
    order_walk_test_set_pause_hook(NULL, NULL);
    race_sync_destroy(&sync);

cleanup:
    free(response);
    tu_pdb_drop_object(db, dir, object);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-ordered-walk-multishard-skip",
              test_ordered_walk_multishard_skip_run)
```

**Regression-test proof step** (per CORE-PROCESS.md): before Task 12's fix,
run `./build/bin/shard-db-test run test-ordered-walk-multishard-skip`
against the codebase as it stands after Tasks 1–9 (deadlock already fixed,
skip bug still present) and paste the actual failing output — expect the
"reader returns F=2 record (regression)" assertion to fail (k2_key absent
from `reader.response`). Then implement Task 12 (including its `break`-
on-release correction — see "A third defect" above) and re-run, pasting
the passing output. This same test's `"records returned in F order (1, 2,
3)"` assertion is also the only regression coverage for the third
(reordering) defect — if Task 12 is implemented without the `break`
correction, expect *that* assertion to fail instead (order `1, 3, 2`)
even though the skip assertion now passes; both must pass together.

### Task 11 — Regression test: `query.c` `OP_IN` composite-merge duplicate (test-first)

Add `src/test/cases/test_composite_in_multishard_duplicate.c`:

```c
/* Regression for the missing per-cursor bound narrowing in
 * find_via_composite_prefix's OP_IN k-way merge (docs/plans/
 * 2026-08-10-kfcache-btree-lock-inversion.md, addendum). Every cursor
 * reopens from its pristine original sub-range on every round — nothing
 * narrows lo/hi on release — so entries already delivered before the
 * round's release point get redelivered in full on reopen, protected only
 * by an exact-(value,hash) tie-break that doesn't cover them. This test
 * puts two records (F=1, F=2) on the same composite-index shard as the
 * IN-seed match that triggers contention, and a third (F=3) on the other
 * shard that becomes the resume floor; F=1 and F=2 are expected to be
 * delivered twice. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "query_internal.h"
#include "shard_db_internal.h"
#include "types.h"
#include "btree.h"
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define DUP_SPLITS 8               /* index_splits_for(8) == 2 shards */
#define BULK_FILLER_COUNT 1050
#define KEY_SEARCH_LIMIT 100000
#define JOIN_TIMEOUT_SEC 5

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int reached;
    int release;
} RaceSync;

static void race_sync_init(RaceSync *s) {
    pthread_mutex_init(&s->lock, NULL);
    pthread_cond_init(&s->cond, NULL);
    s->reached = 0;
    s->release = 0;
}

static void race_sync_destroy(RaceSync *s) {
    pthread_mutex_destroy(&s->lock);
    pthread_cond_destroy(&s->cond);
}

static void dup_pause(void *ctx) {
    RaceSync *s = ctx;
    pthread_mutex_lock(&s->lock);
    s->reached = 1;
    pthread_cond_broadcast(&s->cond);
    while (!s->release) pthread_cond_wait(&s->cond, &s->lock);
    pthread_mutex_unlock(&s->lock);
}

typedef struct {
    ShardDb *db;
    const char *dir;
    const char *object;
    const char *writer_key;
    int role; /* 1 = OP_IN ordered find, 2 = indexed write */
    char *response;
    int rc;
} QueryArgs;

static void *query_thread_main(void *arg) {
    QueryArgs *a = arg;
    char db_root[PATH_MAX];
    snprintf(db_root, sizeof(db_root), "%s/%s", a->db->db_root, a->dir);
    g_db = a->db;
    size_t out_len = 0;
    FILE *out = open_memstream(&a->response, &out_len);
    if (!out) { a->rc = -1; return NULL; }
    g_out = out;
    if (a->role == 1) {
        a->rc = cmd_find(db_root, a->object,
            "[{\"field\":\"CAT\",\"op\":\"in\",\"value\":[\"cat1\"]}]",
            0, 10, NULL, NULL, NULL, NULL, NULL, "F", "asc", NULL, 0);
    } else {
        char bulk[256];
        snprintf(bulk, sizeof(bulk),
            "[{\"key\":\"%s\",\"value\":{\"CAT\":\"filler\",\"F\":500}}]",
            a->writer_key);
        a->rc = cmd_bulk_insert_string(db_root, a->object, bulk, 0);
    }
    fflush(out);
    fclose(out);
    g_out = NULL;
    return NULL;
}

static int timed_join(pthread_t tid, int seconds) {
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += seconds;
    return pthread_timedjoin_np(tid, NULL, &deadline);
}

static int request_ok(ShardDb *db, const char *request, char **response) {
    size_t out_len = 0;
    *response = NULL;
    int rc = shard_db_query(db, request, response, &out_len);
    return rc == 0 && *response && strstr(*response, "\"error\"") == NULL;
}

static int route_same(const uint8_t a[16], const uint8_t b[16]) {
    int kf_a = compute_record_shard(a, DUP_SPLITS);
    int kf_b = compute_record_shard(b, DUP_SPLITS);
    return kf_a == kf_b && idx_shard_for_hash(a, DUP_SPLITS) ==
        idx_shard_for_hash(b, DUP_SPLITS);
}

static int find_seed_and_writer(char *seed_key, size_t seed_cap,
                                char *writer_key, size_t writer_cap,
                                int *target_idx) {
    uint8_t seed_hash[16];
    for (int i = 0; i < KEY_SEARCH_LIMIT; i++) {
        char candidate[64];
        snprintf(candidate, sizeof(candidate), "seed_%d", i);
        compute_hash_raw(candidate, strlen(candidate), seed_hash);
        for (int j = i + 1; j < KEY_SEARCH_LIMIT; j++) {
            char other[64];
            snprintf(other, sizeof(other), "writer_%d", j);
            uint8_t other_hash[16];
            compute_hash_raw(other, strlen(other), other_hash);
            if (!route_same(seed_hash, other_hash)) continue;
            snprintf(seed_key, seed_cap, "%s", candidate);
            snprintf(writer_key, writer_cap, "%s", other);
            *target_idx = idx_shard_for_hash(seed_hash, DUP_SPLITS);
            return 1;
        }
    }
    return 0;
}

static int find_key_on_idx_shard(char *out_key, size_t cap, int wanted_idx,
                                 const char *prefix) {
    for (int i = 0; i < KEY_SEARCH_LIMIT; i++) {
        char candidate[64];
        snprintf(candidate, sizeof(candidate), "%s_%d", prefix, i);
        uint8_t hash[16];
        compute_hash_raw(candidate, strlen(candidate), hash);
        if (idx_shard_for_hash(hash, DUP_SPLITS) == wanted_idx) {
            snprintf(out_key, cap, "%s", candidate);
            return 1;
        }
    }
    return 0;
}

static int append_bulk_record(char *buf, size_t cap, size_t *used,
                              const char *key, int field, int first) {
    int n = snprintf(buf + *used, cap - *used,
        "%s{\"key\":\"%s\",\"value\":{\"CAT\":\"filler\",\"F\":%d}}",
        first ? "" : ",", key, field);
    if (n < 0 || (size_t)n >= cap - *used) return -1;
    *used += (size_t)n;
    return 0;
}

static int count_occurrences(const char *haystack, const char *needle) {
    int n = 0;
    const char *p = haystack;
    size_t nl = strlen(needle);
    while (haystack && (p = strstr(p, needle)) != NULL) { n++; p += nl; }
    return n;
}

static int test_composite_in_multishard_duplicate_run(void) {
    ShardDb *db = test_get_process_db();
    const char *dir = "composite_in_dup";
    const char *object = "rows";
    char request[512];
    char *response = NULL;
    char seed_key[64], writer_key[64], k2_key[64], k3_key[64];
    int target_idx = -1, other_idx = -1;

    ASSERT_TRUE(db != NULL, "process-local database available");
    if (!db) return 1;
    snprintf(request, sizeof(request),
        "{\"mode\":\"add-dir\",\"dir\":\"%s\"}", dir);
    request_ok(db, request, &response);
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"create-object\",\"dir\":\"%s\","
        "\"object\":\"%s\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"CAT:varchar:20\",\"F:int\"],"
        "\"indexes\":[\"CAT+F\"]}", dir, object);
    ASSERT_TRUE(request_ok(db, request, &response), "create composite-indexed object");
    free(response); response = NULL;

    ASSERT_TRUE(find_seed_and_writer(seed_key, sizeof(seed_key),
                                     writer_key, sizeof(writer_key),
                                     &target_idx),
                "find same kf/index shard seed/writer pair");
    if (target_idx < 0) goto cleanup;
    other_idx = 1 - target_idx; /* index_splits_for(8) == 2 */

    ASSERT_TRUE(find_key_on_idx_shard(k2_key, sizeof(k2_key), target_idx, "k2cand"),
                "find second key on the same index shard as seed");
    ASSERT_TRUE(find_key_on_idx_shard(k3_key, sizeof(k3_key), other_idx, "k3cand"),
                "find key on the other index shard");

    size_t cap = 150000;
    char *bulk = malloc(cap);
    ASSERT_NOT_NULL(bulk, "allocate bulk filler request");
    if (!bulk) goto cleanup;
    size_t used = (size_t)snprintf(bulk, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"%s\","
        "\"object\":\"%s\",\"records\":[", dir, object);
    int count = 0;
    for (int i = 0; i < KEY_SEARCH_LIMIT && count < BULK_FILLER_COUNT; i++) {
        char key[64];
        snprintf(key, sizeof(key), "bulk_%d", i);
        uint8_t hash[16];
        compute_hash_raw(key, strlen(key), hash);
        if (idx_shard_for_hash(hash, DUP_SPLITS) != target_idx) continue;
        if (append_bulk_record(bulk, cap, &used, key, 9000 + i, count == 0) != 0) {
            ASSERT_TRUE(0, "bulk filler request fits in buffer");
            free(bulk);
            goto cleanup;
        }
        count++;
    }
    ASSERT_EQ_INT(count, BULK_FILLER_COUNT, "collect 1050 same-shard filler records");
    if (count != BULK_FILLER_COUNT) { free(bulk); goto cleanup; }
    if (used + 3 >= cap) { free(bulk); goto cleanup; }
    memcpy(bulk + used, "]}", 3);
    used += 2;
    bulk[used] = '\0';
    /* Fillers use CAT="filler" — outside the "cat1" IN-value's composite
       prefix range entirely, but they still grow this composite index
       shard's on-disk entry count past the splice-path crossover so the
       writer below genuinely blocks on bt_cache. */
    ASSERT_TRUE(request_ok(db, bulk, &response), "prepopulate splice-path index shard");
    free(response); response = NULL;
    free(bulk);

    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"CAT\":\"cat1\",\"F\":1}}", dir, object, seed_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert seed record F=1");
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"CAT\":\"cat1\",\"F\":2}}", dir, object, k2_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert same-shard record F=2");
    free(response); response = NULL;
    snprintf(request, sizeof(request),
        "{\"mode\":\"insert\",\"dir\":\"%s\",\"object\":\"%s\","
        "\"key\":\"%s\",\"value\":{\"CAT\":\"cat1\",\"F\":3}}", dir, object, k3_key);
    ASSERT_TRUE(request_ok(db, request, &response), "insert other-shard record F=3");
    free(response); response = NULL;

    RaceSync sync;
    race_sync_init(&sync);
    order_walk_test_set_pause_hook(dup_pause, &sync);

    QueryArgs reader = { .db = db, .dir = dir, .object = object, .role = 1 };
    pthread_t reader_tid;
    ASSERT_EQ_INT(pthread_create(&reader_tid, NULL, query_thread_main, &reader),
                  0, "start OP_IN ordered reader");

    pthread_mutex_lock(&sync.lock);
    struct timespec hook_deadline;
    clock_gettime(CLOCK_REALTIME, &hook_deadline);
    hook_deadline.tv_sec += JOIN_TIMEOUT_SEC;
    int hook_wait_rc = 0;
    while (!sync.reached && hook_wait_rc == 0)
        hook_wait_rc = pthread_cond_timedwait(&sync.cond, &sync.lock, &hook_deadline);
    int hook_reached = sync.reached;
    pthread_mutex_unlock(&sync.lock);
    if (!hook_reached) {
        TAP_DIAG("# reader never reached the parked composite-prefix fetch: %s\n",
                 hook_wait_rc == ETIMEDOUT ? "ETIMEDOUT" : strerror(hook_wait_rc));
        ASSERT_TRUE(0, "OP_IN reader reaches pause hook before timeout");
        order_walk_test_set_pause_hook(NULL, NULL);
        _exit(1);
    }

    QueryArgs writer = {
        .db = db, .dir = dir, .object = object, .writer_key = writer_key, .role = 2
    };
    pthread_t writer_tid;
    ASSERT_EQ_INT(pthread_create(&writer_tid, NULL, query_thread_main, &writer),
                  0, "start indexed writer");

    int writer_pending = 0;
    for (int waited = 0; waited < JOIN_TIMEOUT_SEC * 10; waited++) {
        if (btree_test_writer_pending_count() > 0) { writer_pending = 1; break; }
        struct timespec poll = { 0, 100 * 1000000L };
        nanosleep(&poll, NULL);
    }
    ASSERT_TRUE(writer_pending, "writer reaches bt_cache writer acquisition");
    pthread_mutex_lock(&sync.lock);
    sync.release = 1;
    pthread_cond_broadcast(&sync.cond);
    pthread_mutex_unlock(&sync.lock);

    int reader_join = timed_join(reader_tid, JOIN_TIMEOUT_SEC);
    int writer_join = timed_join(writer_tid, JOIN_TIMEOUT_SEC);
    if (reader_join != 0 || writer_join != 0) {
        ASSERT_TRUE(0, "OP_IN walk and indexed writer finish before timeout");
        fflush(NULL);
        _exit(1);
    }

    ASSERT_EQ_INT(reader.rc, 0, "OP_IN reader request succeeds");
    ASSERT_EQ_INT(writer.rc, 0, "indexed writer request succeeds");

    /* The regression: F=1 and F=2's keys must appear exactly once each.
       Pre-fix (no per-cursor bound narrowing on reopen), the reopened
       round restarts this shard's cursor from its pristine start and
       redelivers both — protected only by an exact-(value,hash) tie-break
       against F=3, which doesn't match either. */
    ASSERT_EQ_INT(count_occurrences(reader.response, seed_key), 1,
                  "F=1 record appears exactly once (regression)");
    ASSERT_EQ_INT(count_occurrences(reader.response, k2_key), 1,
                  "F=2 record appears exactly once (regression)");
    ASSERT_EQ_INT(count_occurrences(reader.response, k3_key), 1,
                  "F=3 record appears exactly once");

    const char *p1 = reader.response ? strstr(reader.response, seed_key) : NULL;
    const char *p2 = reader.response ? strstr(reader.response, k2_key) : NULL;
    const char *p3 = reader.response ? strstr(reader.response, k3_key) : NULL;
    ASSERT_TRUE(p1 && p2 && p3 && p1 < p2 && p2 < p3,
                "records returned in F order (1, 2, 3)");

    free(reader.response);
    free(writer.response);
    order_walk_test_set_pause_hook(NULL, NULL);
    race_sync_destroy(&sync);

cleanup:
    free(response);
    tu_pdb_drop_object(db, dir, object);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-composite-in-multishard-duplicate",
              test_composite_in_multishard_duplicate_run)
```

**Regression-test proof step**: run
`./build/bin/shard-db-test run test-composite-in-multishard-duplicate`
against the codebase after Task 9 (hook wired, fix not yet applied) and
paste the failing output — expect the "F=1 record appears exactly once
(regression)" and/or "F=2 record appears exactly once (regression)"
assertions to fail with `count_occurrences` returning 2. Then implement
Task 13 (including its `break`-on-release correction — see "A third
defect" above) and re-run, pasting the passing output. As with Task 10,
this test's `"records returned in F order (1, 2, 3)"` assertion is the
only regression coverage for the third (reordering) defect on this call
site — if Task 13 is implemented without the `break` correction, expect
that assertion to fail (order `1, 3, 2`) even once the duplicate
assertions pass; both must pass together.

Unlike Task 10, this test's criteria (`{"field":"CAT","op":"in",
"value":["cat1"]}`, a single `OP_IN` leaf) does not need a routing
correction: `find_covering_composite` (`query_plan.c:2919`) accepts
`OP_IN` as a valid composite-seed op (`op_caps(OP_IN).composite_seed ==
1`, `query_plan.c:2157`), and with only one leaf in the tree the
`skip_composite` selectivity guard (`query_plan.c:3436`) can never fire
(it requires a *second*, more-selective sibling leaf, which doesn't
exist here) — so this query always reaches `FP_ORDER_COMPOSITE` →
`find_via_composite_prefix` regardless of cardinality, unlike Task 10's
plain single-field D2/D3 choice.

### Task 12 — Fix: `index.c` per-shard resume (section above)

Apply the `ShardResume` struct and `btree_idx_walk_ordered` replacement
under "Fix design — `src/db/index.c`" above — this version includes the
`break`-on-release correction from "A third defect: delivery reordering
across a release boundary" (fixes skip + reordering together). Build:
`SKIP_TESTS=1 ./build.sh`. Run
`./build/bin/shard-db-test run test-ordered-walk-multishard-skip` — must now
pass, including its `"records returned in F order"` assertion (this is
Task 10's regression-proof green run).

### Task 13 — Fix: `query.c` per-cursor resume + bound narrowing (section above)

Apply the `CompResume` struct and `OP_IN` block replacement under "Fix
design — `src/db/query.c`" above — this version includes the same
`break`-on-release correction as Task 12. Build: `SKIP_TESTS=1 ./build.sh`.
Run `./build/bin/shard-db-test run test-composite-in-multishard-duplicate`
— must now pass, including its `"records returned in F order"` assertion
(Task 11's regression-proof green run). Then run
`./build/bin/shard-db-test run-all` — must show no new failures against the
pre-addendum baseline (Tasks 1–9's tests, plus Tasks 10/11's new ones, all
passing).

### Task 14 — Dynamic-safety verification (per this repo's AGENTS.md standing exception)

Tasks 12/13 touch the same shared `bt_cache`/kfcache-adjacent walk state as
Tasks 2–6; both sanitizer runs are required again, not deferred to CI:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1
```

Pay particular attention to TSAN/ASAN output for
`test-ordered-walk-multishard-skip` and
`test-composite-in-multishard-duplicate` specifically (new `calloc`d
`resume`/`ShardResume`/`CompResume` arrays on every exit path, including the
new `break`-on-`h.released` early-exit added by the third-defect fix — confirm
it still reaches the loop's normal cursor/heap/slots cleanup on every round,
not just the terminal round), plus the existing
`test-ordered-walk-kfcache-deadlock*` tests (must still pass — this addendum
must not reintroduce the original deadlock). Any new finding gets root-caused
and fixed now, or written up per AGENTS.md's suppression-with-rationale
process — never silently suppressed.

### Task 15 — Documentation sync

- `docs/concepts/indexes.md`'s "Cost" section currently reads (as of
  Task 8):

  ```
  `btree_idx_walk_ordered` uses a k-way streaming merge across the per-shard
  btree files. Its callback receives an opaque `BtOrderedWalkHandle`; callbacks
  that fetch records release the open iterators through that handle before any
  blocking kfcache fetch, and the walk resumes after the last delivered
  `(value, hash)` pair.
  ```

  This describes a single shared floor, which is the buggy Tasks 1–8
  behavior (Bug #1/#2), not this addendum's fix. Replace that paragraph
  with:

  ```
  `btree_idx_walk_ordered` uses a k-way streaming merge across the per-shard
  btree files. Its callback receives an opaque `BtOrderedWalkHandle`; callbacks
  that fetch records release *every* open iterator through that handle before
  any blocking kfcache fetch. On release, the walk records a per-shard/
  per-cursor resume point (the last delivered `(value, hash)` for the shard
  that was about to advance) and stops draining the rest of that round's
  heap immediately — at most one shard/cursor advances per round once a
  release fires. All iterators then reopen fresh on the next round, each
  from its own resume point (or its pristine bound if it never advanced),
  which is what keeps global `(value, hash)` delivery order correct across
  a release/reopen boundary even though several shards' iterators were
  closed and reopened concurrently with in-flight kfcache contention.
  ```

  Same correction applies to the identical `find_via_composite_prefix` /
  `composite_prefix_cb` description if `docs/concepts/indexes.md` documents
  that path separately (grep for "composite" near the Cost section before
  editing — apply the same per-shard/per-cursor + at-most-one-advances-
  per-round framing there too if a separate paragraph exists).
- `docs/concepts/concurrency.md`'s lock-ordering-rule note (added by Task 8)
  is unaffected by this addendum (the release/reopen *mechanism* it
  documents is unchanged) and needs no edit.

## Execution rules (addendum)

- Continues the same branch (`fix/kfcache-btree-lock-inversion`) and the
  same standing exceptions as the base plan's "Execution rules" above
  (uncommitted until reviewed, same build/test commands, same
  quoted-anchor/`PLAN_NOTES.md`-halt rule, same "stop and ask on any
  uncovered decision" rule).
- Do tasks in order (9 → 10 → 11 → 12 → 13 → 14 → 15). Tasks 10 and 11's
  regression tests must both be written and proven to fail against the
  Tasks-1–9 codebase (deadlock fixed, resume-floor bugs still present)
  before Tasks 12/13's fix code is written — same test-first discipline as
  the base plan's Task 1/1b → Tasks 2–6 ordering.
- Task 9 must land and build clean, with the existing suite still fully
  green, before Task 10/11 are written (they depend on the relocated
  pause-hook infrastructure and, for Task 11, on `composite_prefix_cb`
  actually having a hook to set).

---

# Post-review addendum (2026-08-11): late-release resume and failed-reopen correctness

## Status and precedence

The Tasks 9–15 implementation passed the normal build, all four focused
regressions, and the full normal suite (11,838 assertions across 403 cases),
but the required plan-blind concurrency review found two additional
correctness defects and two test gaps. This addendum supersedes the affected
loop fragments and test instructions in Tasks 9–15. Do not treat the normal
green suite as completion evidence; Tasks 16–21 below must be executed in
order and the ASan+UBSan and TSan gates must be rerun afterward.

The duplicated merge-loop shape in `index.c` and `query.c` is a review smell,
not a correctness requirement. Do not extract a shared generic merge engine
in this fix: the cursor types, bounds, callback contexts, and abort semantics
are different, and such an extraction would enlarge a concurrency-sensitive
bug fix. Record it as a possible follow-up refactor only after this fix is
green under both sanitizers.

## Fourth defect: only the releasing cursor records progress

**Root cause.** The Tasks 12/13 code writes `resume[sid]` only inside
`if (h.released)`. A cursor that delivered one or more rows earlier in the
same round, remained non-exhausted, and did not itself trigger the release
has no resume point. `btree_ordered_walk_release_for_blocking` nevertheless
closes that cursor. On the next round it therefore reopens from its pristine
bound and redelivers its already-returned prefix. Duplicates can reorder the
response and, under a limit, displace unseen rows.

Concrete ascending trace: shard A delivers `F=1` and `F=2`, then shard B's
`F=3` fetch contends and releases every iterator. The reviewed code records
only shard B's `F=3`. Shard A reopens from the original lower bound and emits
`F=1,F=2` again. The existing tests pause on the first callback, so no other
cursor can have prior progress and the defect is invisible.

**Invariant.** After every callback delivery that returns `>= 0`, copy that
cursor's delivered `(value, hash)` into its own resume slot *before* checking
`h.released`. A later release can then reopen every previously advanced
cursor after its own last delivery. Cursors that have never delivered retain
their pristine bounds. The callback that requests release is still delivered
exactly once, and the round still stops immediately after it.

## Fifth defect: failed reopen reuses a stale buffered head

**Root cause.** Closing an iterator does not clear its cursor's copied
`has_entry/value/hash`. On the next round the code assigns the result of
`btree_range_iter_open`, but calls `sc_pull`/`comp_cursor_pull` only when that
result is non-NULL. If reopen fails, the old `has_entry == 1` survives and the
stale pre-release head is pushed into the new heap as if the reopen had
succeeded. It can be duplicated, emitted after deletion, or violate the
fresh-heap ordering invariant.

**Invariant.** Before every reopen attempt, set both `iter = NULL` and
`has_entry = 0`. Only a successful open followed by a successful pull may set
`has_entry = 1` and enter the heap. Preserve the pre-existing behavior that a
shard whose iterator cannot open contributes nothing to that walk. Mark that
cursor `done` immediately: without retirement, a later release can reopen the
failed cursor and emit a smaller row behind rows already delivered by
surviving cursors.

## Test-seam changes

### Pause on a later callback

The late-release regression needs to park on the third record fetch, after a
different cursor has already delivered rows. Keep the existing setter as a
zero-skip compatibility wrapper and add an explicit delayed setter.

**Anchor** (`src/db/query.c`, replace the complete TEST_BUILD pause-hook block
beginning `typedef void (*order_walk_test_pause_fn)(void *ctx);`):

```c
#ifdef TEST_BUILD
typedef void (*order_walk_test_pause_fn)(void *ctx);
static order_walk_test_pause_fn g_order_walk_pause_fn = NULL;
static void *g_order_walk_pause_ctx = NULL;
static size_t g_order_walk_pause_skip = 0;

void order_walk_test_set_pause_hook_after(order_walk_test_pause_fn fn,
                                          void *ctx,
                                          size_t callbacks_to_skip) {
    g_order_walk_pause_fn = fn;
    g_order_walk_pause_ctx = ctx;
    g_order_walk_pause_skip = callbacks_to_skip;
}

void order_walk_test_set_pause_hook(order_walk_test_pause_fn fn, void *ctx) {
    order_walk_test_set_pause_hook_after(fn, ctx, 0);
}

static void order_walk_test_maybe_pause(void) {
    if (!g_order_walk_pause_fn) return;
    if (g_order_walk_pause_skip > 0) {
        g_order_walk_pause_skip--;
        return;
    }
    order_walk_test_pause_fn fn = g_order_walk_pause_fn;
    void *ctx = g_order_walk_pause_ctx;
    g_order_walk_pause_fn = NULL;
    g_order_walk_pause_ctx = NULL;
    fn(ctx);
}
#endif
```

**Anchor** (`src/db/query.c`, replace the complete pause block in both
`composite_prefix_cb` and `order_index_walk_cb`):

```c
#ifdef TEST_BUILD
    order_walk_test_maybe_pause();
#endif
```

**Anchor** (`src/db/query_internal.h`, immediately after the existing
`order_walk_test_set_pause_hook` declaration):

```c
void order_walk_test_set_pause_hook_after(order_walk_test_pause_fn fn,
                                          void *ctx,
                                          size_t callbacks_to_skip);
```

### Fail one selected shard's next range-iterator open

The stale-head regression must force a reopen failure without unlinking a live
index or relying on OOM. Add a TEST_BUILD-only, one-shot shard-specific seam.

**Anchor** (`src/db/btree.c`, inside the existing TEST_BUILD block containing
`g_bt_test_publish_fail_stage`, after that declaration):

```c
static _Atomic int g_bt_test_range_open_fail_shard = -1;

void btree_test_fail_next_range_open_shard(int shard) {
    atomic_store_explicit(&g_bt_test_range_open_fail_shard, shard,
                          memory_order_release);
}

static int bt_test_take_range_open_failure(const char *path) {
    int wanted = atomic_load_explicit(&g_bt_test_range_open_fail_shard,
                                      memory_order_acquire);
    if (wanted < 0) return 0;
    char suffix[16];
    snprintf(suffix, sizeof(suffix), "/%03x.idx", wanted);
    size_t path_len = strlen(path), suffix_len = strlen(suffix);
    if (path_len < suffix_len ||
        memcmp(path + path_len - suffix_len, suffix, suffix_len) != 0)
        return 0;
    return atomic_compare_exchange_strong_explicit(
        &g_bt_test_range_open_fail_shard, &wanted, -1,
        memory_order_acq_rel, memory_order_acquire);
}
```

**Anchor** (`src/db/btree.c`, at the start of `btree_range_iter_open`, before
`BtRangeIter *it = calloc(...)`):

```c
#ifdef TEST_BUILD
    if (bt_test_take_range_open_failure(path)) {
        errno = EIO;
        return NULL;
    }
#endif
```

**Anchor** (`src/db/btree.h`, in the existing TEST_BUILD declarations after
`btree_test_publish_fail_stage`):

```c
/* Fail the next btree_range_iter_open for the selected index shard. */
void btree_test_fail_next_range_open_shard(int shard);
```

## Corrected merge-loop fragments

Apply both fragments symmetrically. These replace the corresponding reopen
and post-callback fragments from Tasks 12 and 13.

**Anchor** (`src/db/index.c`, replace the complete per-shard reopen loop in
`btree_idx_walk_ordered`):

```c
        for (int s = 0; s < n; s++) {
            cursors[s].iter = NULL;
            cursors[s].has_entry = 0;
            if (resume[s].done) continue;
            char idx_path[PATH_MAX];
            build_idx_path(idx_path, sizeof(idx_path), db_root, object, field, s);
            cursors[s].shard_id = s;
            const char *open_lo = lo_val, *open_hi = hi_val;
            size_t open_lo_len = lo_len, open_hi_len = hi_len;
            int open_lo_excl = lo_excl, open_hi_excl = hi_excl;
            if (resume[s].have) {
                if (desc) {
                    open_hi = resume[s].val;
                    open_hi_len = resume[s].len;
                    open_hi_excl = 0;
                } else {
                    open_lo = resume[s].val;
                    open_lo_len = resume[s].len;
                    open_lo_excl = 0;
                }
            }
            cursors[s].iter = btree_range_iter_open(
                idx_path,
                open_lo, open_lo_len, open_lo_excl,
                open_hi, open_hi_len, open_hi_excl,
                desc);
            if (!cursors[s].iter) {
                resume[s].done = 1;
                continue;
            }
            sc_pull(&cursors[s]);
            if (cursors[s].has_entry) heap[nh++] = s;
        }
```

**Anchor** (`src/db/index.c`, replace from `int rc = cb(` through the end of
that delivery's pull/drop branch):

```c
            int rc = cb(bc->value, bc->vlen, bc->hash, &h, ctx);
            if (rc < 0) {
                for (int s = 0; s < n; s++)
                    if (cursors[s].iter) btree_range_iter_close(cursors[s].iter);
                free(cursors); free(heap); free(slots); free(resume);
                return;
            }

            resume[sid].len = bc->vlen > sizeof(resume[sid].val)
                                ? sizeof(resume[sid].val) : bc->vlen;
            memcpy(resume[sid].val, bc->value, resume[sid].len);
            memcpy(resume[sid].hash, bc->hash, BT_HASH_SIZE);
            resume[sid].have = 1;

            if (h.released) {
                /* Every cursor that delivered earlier in this round already
                 * owns its own resume point. Stop immediately so no stale
                 * buffered head is emitted after the release. */
                break;
            }

            int had_iter = (bc->iter != NULL);
            sc_pull(bc);
            if (bc->has_entry) {
                merge_sift_down(heap, nh, 0, cursors, desc);
            } else {
                if (had_iter) resume[sid].done = 1;
                heap[0] = heap[--nh];
                if (nh > 0) merge_sift_down(heap, nh, 0, cursors, desc);
            }
```

**Anchor** (`src/db/query.c`, replace the complete per-cursor reopen loop in
the `OP_IN` branch of `find_via_composite_prefix`):

```c
            for (int i = 0; i < total_cursors; i++) {
                cursors[i].iter = NULL;
                cursors[i].has_entry = 0;
                if (resume[i].done) continue;
                char ip[PATH_MAX];
                int shard = i % ns;
                build_idx_path(ip, sizeof(ip), db_root, object,
                               composite_field, shard);
                const uint8_t *open_lo = cursors[i].lo;
                const uint8_t *open_hi = cursors[i].hi;
                size_t open_lo_len = cursors[i].lo_len;
                size_t open_hi_len = cursors[i].hi_len;
                int open_lo_excl = cursors[i].lo_exclusive;
                int open_hi_excl = cursors[i].hi_exclusive;
                if (resume[i].have) {
                    if (order_desc) {
                        open_hi = (const uint8_t *)resume[i].val;
                        open_hi_len = resume[i].len;
                        open_hi_excl = 0;
                    } else {
                        open_lo = (const uint8_t *)resume[i].val;
                        open_lo_len = resume[i].len;
                        open_lo_excl = 0;
                    }
                }
                cursors[i].iter = btree_range_iter_open(
                    ip,
                    (const char *)open_lo, open_lo_len, open_lo_excl,
                    (const char *)open_hi, open_hi_len, open_hi_excl,
                    order_desc);
                if (!cursors[i].iter) {
                    resume[i].done = 1;
                    continue;
                }
                comp_cursor_pull(&cursors[i]);
                if (cursors[i].has_entry) heap[nh++] = i;
            }
```

**Anchor** (`src/db/query.c`, replace from the
`if (composite_prefix_cb(` call through the end of that delivery's pull/drop
branch):

```c
                if (composite_prefix_cb(bc->value, bc->vlen, bc->hash,
                                        &h, &ctx) < 0) {
                    abort_requested = 1;
                    break;
                }

                resume[sid].len = bc->vlen > sizeof(resume[sid].val)
                                    ? sizeof(resume[sid].val) : bc->vlen;
                memcpy(resume[sid].val, bc->value, resume[sid].len);
                memcpy(resume[sid].hash, bc->hash, BT_HASH_SIZE);
                resume[sid].have = 1;

                if (h.released) {
                    /* All previously advanced cursors already have their own
                     * resume point; rebuild a fresh heap immediately. */
                    break;
                }

                int had_iter = (bc->iter != NULL);
                comp_cursor_pull(bc);
                if (bc->has_entry) {
                    comp_merge_sift_down(heap, nh, 0, cursors, order_desc);
                } else {
                    if (had_iter) resume[sid].done = 1;
                    heap[0] = heap[--nh];
                    if (nh > 0)
                        comp_merge_sift_down(heap, nh, 0, cursors, order_desc);
                }
```

## Regression changes

### Late-contention route shape

Update both `test_ordered_walk_multishard_skip.c` and
`test_composite_in_multishard_duplicate.c` so the paused third record is on
the writer's kf shard but the *other* index shard. This preserves the AB-BA
contention while ensuring the writer's index shard has already delivered
`F=1,F=2` and remains non-exhausted.

**Anchor** (both files, add after `find_key_on_idx_shard`):

```c
static int find_key_on_routes(char *out_key, size_t cap,
                              int wanted_kf, int wanted_idx,
                              const char *prefix) {
    for (int i = 0; i < KEY_SEARCH_LIMIT; i++) {
        char candidate[64];
        snprintf(candidate, sizeof(candidate), "%s_%d", prefix, i);
        uint8_t hash[16];
        compute_hash_raw(candidate, strlen(candidate), hash);
        if (compute_record_shard(hash, SKIP_SPLITS) == wanted_kf &&
            idx_shard_for_hash(hash, SKIP_SPLITS) == wanted_idx) {
            snprintf(out_key, cap, "%s", candidate);
            return 1;
        }
    }
    return 0;
}
```

In the composite test use `DUP_SPLITS` instead of `SKIP_SPLITS` in that
complete helper. After `find_seed_and_writer`, hash `writer_key`, derive its
kf shard, and use `find_key_on_routes` for `k3_key`. Replace the first-callback
setter with:

```c
    /* Pause on F=3: F=1 and F=2 have already advanced target_idx. */
    order_walk_test_set_pause_hook_after(skip_pause, &sync, 2);
```

The composite test uses `dup_pause` in the corresponding complete statement.
Keep all existing exact-once and `F=1,F=2,F=3` order assertions. Against the
reviewed Tasks 9–15 code, both tests must fail because the already-advanced
target cursor reopens from its pristine bound and duplicates `F=1,F=2`.
Paste both red outputs before applying the corrected merge fragments.

### Failed-reopen stale-head coverage

After proving the late-contention red run, add a second run/mode to each of
those two test files. At the same third-callback pause, call
`btree_test_fail_next_range_open_shard(target_idx)` immediately before
releasing the reader. Capture the first target-shard filler key while building
the bulk request and assert that it is absent from the response: the selected
shard failed to reopen and therefore must not contribute its stale buffered
head. The mode keeps the existing assertions that `F=1,F=2,F=3` were each
already delivered exactly once and in order. Against the reviewed code this
mode must fail because the failed reopen leaves `has_entry` set and pushes the
buffered filler into the new heap. Paste the red output for both the plain and
composite paths.

Add `test-ordered-walk-failed-reopen-stays-retired` at the public
`btree_idx_walk_ordered` seam. Arrange shard 0 as `F=1,F=3` and shard 1 as
`F=2`; release after `F=1`, force shard 1's next open to fail, then release
again after `F=3`. Before the retirement fix the response is `1,3,2`; after
the fix it is `1,3`. Paste that exact red-to-green proof.

Use separate object/directory names for the late-resume and failed-reopen
modes so they remain isolated when the test runner uses `--jobs 1`. Clear both
test seams on every normal cleanup path:

```c
    order_walk_test_set_pause_hook(NULL, NULL);
    btree_test_fail_next_range_open_shard(-1);
```

### In-flight deadlock-result assertions

**Anchor** (`test_ordered_walk_kfcache_deadlock.c`, immediately after the
existing `ordered reader returns seed record` assertion):

```c
    ASSERT_CONTAINS(reader.response, writer_key,
                    "in-flight ordered reader returns writer record");
    const char *reader_seed_pos = reader.response
                                    ? strstr(reader.response, seed_key) : NULL;
    const char *reader_writer_pos = reader.response
                                      ? strstr(reader.response, writer_key) : NULL;
    ASSERT_TRUE(reader_seed_pos && reader_writer_pos &&
                reader_seed_pos < reader_writer_pos,
                "in-flight ordered reader returns both records in F order");
```

The bulk and single-write registrations share this helper, so these assertions
cover both original write paths. Keep the post-race query as an independent
durability/index-consistency assertion.

## Tasks 16–21

### Task 16 — Add the delayed-pause and failed-open test seams

Apply only the TEST_BUILD seam changes above. Build with
`SKIP_TESTS=1 ./build.sh`, then run the existing four focused tests and the
full normal suite. They must remain green before changing regression timing.

### Task 17 — Write and prove the strengthened regressions red

Apply the late-contention route/timing changes, both failed-reopen modes, and
the in-flight assertions. Do not change either production merge loop yet.
Run all four focused tests. Paste failures showing:

- duplicate `F=1` and/or `F=2` on the late third-callback release in both
  plain and composite walks;
- a stale filler emitted after the forced failed reopen in both walks; and
- the original bulk and single-write deadlock tests still complete (their new
  in-flight assertions may already pass).

If either late-contention test does not reach `writer_pending`, stop: the key
route does not reproduce the intended AB-BA edge and must not be replaced by
timing sleeps.

### Task 18 — Apply the corrected production loops

Apply both corrected reopen fragments and both unconditional per-delivery
resume fragments above. Build, then run the strengthened plain and composite
tests. All uniqueness, ordering, stale-head-absence, and timeout assertions
must pass.

### Task 19 — Documentation correction

Update `docs/concepts/indexes.md` and the matching `AGENTS.md` summary so they
state that every cursor continuously records its own last successful
delivery—not only the cursor that triggers release—and that a failed reopen
cannot reuse a pre-release buffered head. `docs/concepts/concurrency.md`'s
lock-order rule remains correct.

### Task 20 — Fresh normal verification

Run:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-ordered-walk-kfcache-deadlock
./build/bin/shard-db-test run test-ordered-walk-kfcache-deadlock-single-write
./build/bin/shard-db-test run test-ordered-walk-multishard-skip
./build/bin/shard-db-test run test-composite-in-multishard-duplicate
./build/bin/shard-db-test run-all
```

Paste every command's output. A rerun after a failure is not completion until
the first failure is root-caused.

### Task 21 — Repeat dynamic-safety verification

Because Tasks 16–19 again touch shared test seams and bt_cache/kfcache-adjacent
cursor state, rerun the complete local gates; prior Tasks 14 results are stale:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1
```

Paste both complete summaries and root-cause every new finding under the
repo's sanitizer policy. Leave all work uncommitted for the next raw-diff
review.

## Execution rules (post-review addendum)

- Continue on `fix/kfcache-btree-lock-inversion`; do not restart from `main`.
- Execute Tasks 16 → 17 → 18 → 19 → 20 → 21 in order.
- All original quoted-anchor, `PLAN_NOTES.md` halt, no-improvisation,
  uncommitted-review, and sanitizer rules remain in force.
- The red runs in Task 17 are mandatory proof for defects four and five;
  normal-suite green before those tests existed is not substitute evidence.

---

# Refactor addendum: one ordered-range merge implementation

## Status of the correctness work

Tasks 16–21 are the accepted correctness baseline for this refactor. The
per-cursor resume fix, immediate round termination after release, stale-head
clearing, and permanent retirement after a failed reopen are correct. This
addendum must not redesign or weaken those semantics.

The remaining issue is maintainability, not a known correctness defect:
`btree_idx_walk_ordered` in `src/db/index.c` and the composite `OP_IN` branch
of `find_via_composite_prefix` in `src/db/query.c` independently implement the
same cursor heap, release/reopen, resume, retirement, and cleanup protocol.
That duplication made the correctness changes in Tasks 18–19 necessary in
two places. The goal below is one authoritative implementation with two thin
adapters.

## Refactor boundary and invariants

Place the seam at an explicit set of btree file ranges. Add the following
small interface to `src/db/btree.h`; put its implementation and the private
cursor/heap/resume state in `src/db/btree.c`:

```c
typedef struct BtOrderedWalkHandle BtOrderedWalkHandle;

typedef struct {
    const char *path;
    const char *min_val;
    size_t min_len;
    int min_exclusive;
    const char *max_val;
    size_t max_len;
    int max_exclusive;
    int tie_id;
} BtOrderedRangeSpec;

typedef int (*bt_ordered_result_cb)(const char *value, size_t vlen,
                                    const uint8_t hash[BT_HASH_SIZE],
                                    BtOrderedWalkHandle *handle,
                                    void *ctx);

void btree_ordered_walk_release_for_blocking(BtOrderedWalkHandle *handle);

void btree_walk_ordered_ranges(const BtOrderedRangeSpec *ranges,
                               size_t range_count,
                               int desc,
                               bt_ordered_result_cb cb,
                               void *ctx);
```

The range specifications and all pointed-to path/bound bytes are borrowed,
immutable, and valid until the synchronous call returns. A bound pointer may
be `NULL` only when its length is zero. The implementation copies iterator
heads and resume positions internally exactly as the current loops do.

`BtOrderedWalkHandle` is opaque outside `btree.c`. Callbacks may only pass it
to `btree_ordered_walk_release_for_blocking`; callers must not inspect or
construct its iterator-slot state.

The shared implementation must preserve all current observable behavior:

- merge by `(value bytes, value length, hash, tie_id)`, reversing the complete
  comparison for descending walks;
- invoke callbacks while the current iterator locks remain held unless the
  callback explicitly releases them;
- treat a negative callback result as an immediate, clean early stop;
- continuously record the last successfully delivered `(value, hash)` for
  every cursor, not merely the cursor that triggers a release;
- after release, stop the round immediately, reopen every live cursor from
  its own resume point, and suppress the inclusive resume entry by hash;
- clear `iter` and `has_entry` before every reopen;
- permanently retire a cursor after genuine exhaustion or any failed reopen;
- never emit a buffered head retained from before release;
- retain bounded streaming memory: one copied head and one resume point per
  range, plus the heap; never collect the complete result set;
- retain the existing allocation/open-failure behavior: allocation failure
  ends the walk without callbacks, and one range failing to open does not
  abort the other ranges.

Do not introduce open/pull/resume function-pointer hooks or a caller-managed
cursor interface. Those would expose nearly the whole merge protocol and
produce a shallow module. The range list is the only variation needed by the
two real adapters.

## Adapter responsibilities

`btree_idx_walk_ordered` remains the stable index-layer interface used by the
existing query, aggregate, and planner callers. It becomes a thin adapter:

1. calculate `index_splits_for(splits)`;
2. allocate one path and one `BtOrderedRangeSpec` per index shard;
3. preserve its current `BT_MAX_VAL_LEN` truncation of the caller's lower and
   upper bounds;
4. assign `tie_id = shard_id`;
5. call `btree_walk_ordered_ranges`; and
6. free the adapter-owned path/specification storage.

The composite `OP_IN` branch remains responsible only for query-domain work:
encoding each IN prefix, folding `order_range` into the prefix bounds,
building one path/specification for every `(IN value, index shard)` pair,
initialising `CompositePrefixCtx`, and returning `ctx.printed`. Assign
`tie_id = IN-value index` to preserve the current `stream_id` tie-break.
Its path and bound storage may remain query-local, but it must contain no
iterator, heap, resume, release/reopen, or stale-head state.

## Regression strategy

This is a behavior-preserving refactor, so the existing regressions are the
pre-change characterization rather than an artificial red test. Before
moving production code, run and retain green output for:

```bash
./build/bin/shard-db-test run test-ordered-walk-kfcache-deadlock
./build/bin/shard-db-test run test-ordered-walk-kfcache-deadlock-single-write
./build/bin/shard-db-test run test-ordered-walk-multishard-skip
./build/bin/shard-db-test run test-ordered-walk-failed-reopen-stays-retired
./build/bin/shard-db-test run test-composite-in-multishard-duplicate
```

Add focused coverage at the new `btree_walk_ordered_ranges` seam using at
least two real temporary btree files with different bounds. The direct test
must cover:

- globally merged ascending and descending output;
- different lower/upper ranges in the same walk;
- early callback stop;
- a callback release followed by exact-once ordered resume; and
- forced failure of one cursor's reopen, proving that cursor stays retired
  while other ranges continue.

Use public behavior only: collected callback results and return/completion
state. Do not expose cursor, heap, resume, or iterator-slot internals for the
test. Keep the existing ordinary-index and composite end-to-end regressions;
they verify the two adapters and the original lock-inversion routes.

## Tasks 22–29

### Task 22 — Capture the accepted baseline

Build normally and run the five focused registrations listed above before
any refactor. They must all pass. If one fails, stop and root-cause it as a
correctness regression; do not proceed under the assumption that duplication
is responsible.

### Task 23 — Add characterization for explicit range sets

Add the direct ordered-range test using the intended public interface and
real temporary btree files. If necessary, introduce the declarations and a
temporary forwarding implementation only far enough to compile the test;
do not copy a third merge loop. Prove the test exercises distinct bounds,
both directions, release/resume, early stop, and failed-reopen retirement.

### Task 24 — Move the ordered merge behind the btree seam

Move `ShardCursor`, `ShardResume`, comparison, pull, heap, handle release,
round rebuild, resume suppression, retirement, and cleanup behavior from
`index.c` into private `btree.c` implementation state. Implement
`btree_walk_ordered_ranges` against the caller's immutable range list.

At the end of this task there must be exactly one implementation of:

- ordered cursor comparison and heap maintenance;
- iterator release/reopen rounds;
- per-cursor resume bookkeeping and inclusive-bound suppression; and
- failed-reopen/exhaustion retirement.

Run the new direct test before changing the composite caller.

### Task 25 — Convert the ordinary index walk to an adapter

Replace the body-specific merge machinery in `btree_idx_walk_ordered` with
range/path construction plus one call to `btree_walk_ordered_ranges`.
Preserve its public declaration and all existing callers. Run the four
ordinary ordered-walk registrations, including the direct failed-reopen
registration.

### Task 26 — Convert composite IN to an adapter

Replace `CompMergeCursor`, `CompResume`, `comp_cursor_cmp`,
`comp_cursor_pull`, `comp_merge_sift_down`, and the custom merge loop with
query-local range/path/bound construction and one call to
`btree_walk_ordered_ranges`. Preserve `CompositePrefixCtx`,
`composite_prefix_cb`, deadline behavior, offset/limit behavior, and
`ctx.printed` exactly. Run `test-composite-in-multishard-duplicate` and all
other tests whose name contains `composite` or `cursor`.

### Task 27 — Make the callback handle opaque and document ownership

Remove the concrete `BtOrderedWalkHandle` layout and duplicate ordered
callback declarations from `types.h`; consumers receive them through
`btree.h`. Document the synchronous borrowed-lifetime rule for range
specifications and the release-before-blocking callback rule. Update
`docs/concepts/indexes.md` only where needed to identify the shared
range-set walker beneath the ordinary and composite adapters. Do not change
the already-correct concurrency guarantee.

### Task 28 — Structural and normal verification

Confirm with source search that `CompMergeCursor`, `CompResume`,
`comp_merge_sift_down`, and the index-local ordered heap/resume loop no longer
exist, and that only `btree.c` manipulates `BtOrderedWalkHandle` internals.
Then run:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-ordered-walk-kfcache-deadlock
./build/bin/shard-db-test run test-ordered-walk-kfcache-deadlock-single-write
./build/bin/shard-db-test run test-ordered-walk-multishard-skip
./build/bin/shard-db-test run test-ordered-walk-failed-reopen-stays-retired
./build/bin/shard-db-test run test-composite-in-multishard-duplicate
./build/bin/shard-db-test run-all
git diff --check
```

Compare the focused results with Task 22. Any output order, multiplicity,
timeout, failed-reopen, or callback-stop difference is a regression to fix,
not an acceptable consequence of the refactor.

### Task 29 — Repeat both dynamic-safety gates

The refactor moves lock-owning iterator state and release behavior, so run
the complete local sanitizer gates even though the intended behavior is
unchanged:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1
```

Paste the complete summaries and root-cause every new finding under the
repository sanitizer policy. Do not run benchmarks; performance benches are
user-owned. Leave the complete refactor uncommitted for raw-diff review.

## Execution rules (ordered-range refactor addendum)

- Begin only from the reviewed, green Tasks 16–21 correctness baseline.
- Execute Tasks 22 → 23 → 24 → 25 → 26 → 27 → 28 → 29 in order.
- Treat this as a structural refactor. Do not change ordering, pagination,
  query filtering, error behavior, or lock semantics while removing the
  duplication.
- Do not retain the old loops as fallback paths after the adapters pass.
- All original quoted-anchor, `PLAN_NOTES.md` halt, no-improvisation,
  uncommitted-review, and sanitizer rules remain in force.
