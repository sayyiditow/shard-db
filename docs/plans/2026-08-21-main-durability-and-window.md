# Plan: per-Kf-window ACID durability, configurable windows, and bulk-path cleanup

Date: 2026-08-21  
Status: proposed — requires human approval before execution

## Decision and contract

This plan replaces the previous attempted implementation.  Execution starts
from the local `main` baseline; no partial durability/window code is carried
forward.

The existing **per-Kf-shard reader/writer lock** is the only commit-visibility
boundary.  There is no `SlotcaskDb.visibility_lock`, no object-wide mutation
gate, and no new syscall.  The existing object lock retains its current role:
schema/rebuild lifetime exclusion.

A commit window contains records from **exactly one Kf shard**.  A single
insert/update/delete is a one-record window.  A multi-shard bulk request may
stage work concurrently and may commit its Kf-shard windows concurrently, but
it is not an all-or-nothing transaction: an earlier shard window may remain
committed if a later one fails.  This is the requested atomicity boundary.

For one acknowledged window, the durable order is:

```text
upsert: P -> M -> A -> I -> K -> T? -> C
delete:      M ->      I -> K -> T  -> C

P = write new segment payload with flag=0, then msync + fdatasync
M = publish complete marker with temp-file fsync + atomic no-clobber publication
    + kf-directory fsync
A = change the new segment flag to 1, then msync + fdatasync
I = apply and durably sync every affected secondary-index shard
K = update and durably sync Kf slots and, when counts change, the Kf header
T = tombstone old segment payloads and durably sync them
C = unlink marker and fsync the marker directory
```

Within a bulk window, every sync is batched by its durable file/page unit:
one segment-file sync for P, A, or T; one complete marker-file sync for M;
one sync per touched secondary-index file for I; coalesced Kf-page ranges plus
one header sync for K; and one directory sync for C. Single-record mutations
are one-record windows and retain the same ordering.

The Kf writer lock is acquired before the marker gate and retained through
`C`.  Thus a normal reader of that shard observes either the pre-window state
or the complete post-window state. `M` occurs the instant the marker is
linked into its final name; from that instant it is irrevocable: no abort
sidecar or rollback exists, and every later failure — including a publication
whose temp-file unlink or kf-directory fsync failed after the link — is a
post-M failure. A failure after `M` first attempts synchronous
forward replay; it returns ordinary success only if replay reaches C. If it
cannot reach C, it retains the marker and returns `EINPROGRESS` /
`"durability outcome pending; do not retry blindly"`. Kf-validated readers
remain safe while replay is pending.

A mutation that reaches the marker gate while a final marker is retained is
never silently rejected: the gate emits exactly one
`LOG_ERROR(LOG_SUB_DURABILITY, ...)` naming the shard and marker, then
attempts synchronous forward replay under the shard writer lock. If replay
reaches C, the marker is cleared and the new mutation proceeds normally; if
it cannot, the gate rejects the mutation with `EINPROGRESS`. No write passes
on a shard with a retained marker until its replay succeeds. Startup
recovery keeps its existing sweep role.

The required lock order is:

```text
object lifecycle lock -> one Kf shard rwlock -> segment-cache lock
                                     -> one secondary-index operation lock
```

No reader may wait for a Kf lock while retaining a B-tree/bitmap cache lock.
The existing `read_record_ref_try()` release-and-retry convention remains the
required indexed-read rule.  A writer must never acquire a second Kf shard
while it owns the first.

## Root causes addressed

1. `seg_write_flag_durable()` currently obtains a segment writer handle while
   marker replay still owns a read handle for that same mapped segment.  A
   same-thread read-to-write upgrade deadlocks.  Unique-offset flag writes
   need only pin the mapping with a read handle — exactly the rdlock
   discipline `seg_write_flag()` already documents — so the fix is to make
   every flag-write path take the read handle and never attempt an upgrade.
   No cross-thread sync lock is needed: concurrent msync/fdatasync of the
   same segment file from different window workers is safe, and fdatasync is
   file-wide, so each window's barrier also flushes another window's
   already-ordered bytes. (`g_segment_sync_lock`, named in an earlier draft
   of this plan, does not exist on the baseline and is not introduced.)
2. Marker writers use `O_TRUNC`; an in-process retained marker can therefore
   be overwritten by a fresh writer.  All mutations now have markers, so the
   gate must be unconditional and publication must be atomic.
3. Some Kf-changing paths sync with `header_changed=0`, leaving durable
   `total/deleted` counters stale after a power loss.
4. Raw O_DIRECT query scans enumerate segment `flag=1` records directly.
   During A/T they can return stale records, duplicates, or a gap.  They
   bypass the Kf visibility boundary.
5. Several normal read helpers release their Kf read lock after resolving an
   address but before copying/validating the referenced segment record.
6. The current bulk coordinators retain the Kf writer lock while staging P,
   use fixed 256-record stack arrays, duplicate mutation paths, and leave
   obsolete fast/fallback branches.

## Complete caller and consumer inventory

Before changing a signature, the executor must reconcile these callers found
from the `main` baseline.  A missing caller is a plan-anchor failure.

| API/seam | Current production consumers |
| --- | --- |
| `slotcask_bulk_upsert_in_kfshard` | `query_bulk.c`: direct bulk insert, structured update worker, delimited update worker, JSON update worker |
| `slotcask_bulk_delete_in_kfshard` | `query_bulk.c`: collected delete and criteria delete worker |
| `slotcask_bulk_update` | public `slotcask.h` API; test-only `test_slotcask_api.c` today |
| single hook mutations | `storage.c` insert/update/delete/CAS paths |
| Kf-indexed record fetch | `query_find.c` `read_record_ref[_try]`; ordered callbacks in `query.c` |
| multi-key fetch | `storage.c` multi-get/multi-exists; `query.c` batched indexed/bitmap/count/ordered fetches through both `slotcask_bulk_resolve_and_fetch` and `slotcask_bulk_fetch_resolved` |
| full query scan | `query_find.c` `scan_shards_v2_streaming`, `scan_shards_v2_o_direct`, and `scan_shards_v2_o_direct_match`; `query.c` inline count caller |
| index-only query shortcuts | `query.c` B-tree count/keyset/intersection/union paths and bitmap popcount/count paths; these must not publish an answer without Kf validation |
| offset fetch and O(1) count | `slotcask_walk_live_skip` and `slotcask_sum_kf_totals`; both currently read Kf data outside the cached shard rwlock |
| maintenance raw scans | `index.c` reindex segment scanner and maintenance/rebuild callers under object write exclusion |
| marker recovery | `marker_recovery_sweep_object`, `kf_shard_marker_gate`, `kf_marker_gate`, and `kf_batch_marker_gate` |

`BULK_COMMIT_WINDOW` is an operator configuration key, default `1024`, with
an inclusive valid range of `16..16384`.  Invalid, malformed, negative, and
overflow input leaves the current/default value unchanged and logs one
configuration warning.  It is not an on-disk format change and has no
migration requirement.

## Task 1 — write regression tests before changing implementation

**Anchors:** `TEST_REGISTER` blocks in
`src/test/cases/test_durability_ordering.c` and
`src/test/cases/test_durability_sync_failures.c`.

**Red tests first.** Add independent temp-root test cases; do not mutate
process-wide environment or rely on test order.

1. Crash/restart matrix for indexed and non-indexed insert, update, and
   delete at P, M, A, I, K, T, and C.  Each case checks point get, indexed
   find, Kf-driven full find/count, and post-restart reindex.  Expected state
   is precisely pre-window or complete post-window, never a mix.
2. Force an update and a delete through retained-marker replay.  The test
   must time out on the baseline deadlock and pass after the read-handle fix.
3. Force gate replay failure, then issue non-indexed insert/update/delete.
   Prove the existing marker remains rather than being truncated/replaced,
   every later mutation on that shard is rejected with `EINPROGRESS` while
   the marker is retained, exactly one error log line is emitted per gate
   encounter, and a later mutation succeeds once replay reaches C and clears
   the marker.
4. Inject sync failure at every Kf mutation category.  Assert separate
   expected msync counts: insert has payload + activation + replayed Kf slot
   + replayed Kf header; update additionally has the old tombstone; delete
   has its Kf header sync.  Do not use one shared expectation.
   For every post-M failure, assert no abort sidecar exists, recovery only
   converges to the post-window state, and the live request returns ordinary
   success after synchronous replay or the distinct pending outcome. Also
   inject failure into the post-link steps of marker publication (temp-file
   unlink and kf-directory fsync): these must be classified as post-M —
   replay then success or `EINPROGRESS` — never as a plain pre-M failure, and
   the writer must never publish a second marker for the same window.
5. Set a test-local `BULK_COMMIT_WINDOW=16` (either write it into the test's
   own temp `db.env`, following the existing fixture pattern in
   `test_durability_sync_failures.c`, or set `bulk_commit_window` directly on
   the test's own `SlotcaskDb` instance); exercise 17 records in one Kf
   shard and a later failing window.  Assert the first window survives and
   the second is not partially visible.  Add default, boundaries, malformed,
   overflow, and out-of-range knob tests using the temp-`db.env` parse
   pattern, saving and restoring any global they touch.
6. Deterministically pause an upsert after `I` and before `K`.  A point read,
   indexed find, index-only count, KeySet intersection/union count, bitmap
   count, multi-get, offset fetch, O(1) count, and Kf-driven full scan of
   that shard must block or return only a whole pre/post state; an unrelated
   Kf shard must progress.
7. Add a full-scan update test that pauses between A and T.  The baseline raw
   O_DIRECT scan exposes two versions; the replacement scan returns one.
8. Add sync-count regressions for a multi-record indexed update and delete in
   one window. Assert exactly one marker-file fsync, at most one segment sync
   per touched file in each P/A/T phase, one index sync per touched index file,
   and coalesced Kf page syncs rather than one msync per slot. Force each
   window-end sync to fail and assert forward-only marker replay/pending
   semantics.
9. Craft a legacy-shaped marker file — and, separately, a legacy abort-sidecar
   filename — inside an object's kf marker directory. Startup must fail closed
   with the documented upgrade error rather than forward-replay, truncate, or
   delete the evidence; a clean object in the same DB_ROOT must still open.

For every bug regression, capture the baseline failure, temporarily restore
the baseline implementation after the green test, capture the expected red
failure, then reapply the fix and capture green output in the execution log.

## Task 2 — make Kf ownership the complete read-side visibility boundary

**Anchors:** `int slotcask_get(`, `int slotcask_exists(`,
`int slotcask_lookup_by_hash(`, `int slotcask_lookup_by_hash_try(`,
`SlotcaskResolvedRec *slotcask_bulk_resolve_hashes(`,
`int slotcask_bulk_fetch_resolved(`,
`int slotcask_bulk_resolve_and_fetch(`, and
`static int walk_one_shard_inner(`,
`int slotcask_walk_live_skip(`, and
`int slotcask_sum_kf_totals(` in `src/db/slotcask.c`; plus
`void scan_shards_v2_streaming(` and
`void scan_shards_v2_o_direct(`,
`void scan_shards_v2_o_direct_match(`, and
`int scan_dispatch(` in `src/db/query_find.c`; plus
`int cmd_count_with_tree(`, `idx_count_cb`, `intersect_indexed_leaves`,
the KeySet-union path, `bm_popcount_for_crit`, and
`bm_popcount_generic_shard_worker` in `src/db/query.c`.

**Test first for this task.** Add Task 1's A/T full-scan regression and the
same-shard/unrelated-shard pause test; demonstrate they fail on the baseline
before editing these anchors.

**Implementation.** Do not introduce a visibility field or helper in
`SlotcaskDb`.  Change Kf-based reads so the Kf read handle stays live until
the selected segment record has been checked against its hash/key and copied
into caller-owned memory.  On retry, release segment first and Kf second.

`slotcask_bulk_resolve_hashes` must no longer expose an address that a later
fetch treats as current without a Kf lock.  Make it private to the combined
helper if no caller remains.  `slotcask_bulk_fetch_resolved` has live callers
in bitmap/index query code, so it remains public but partitions supplied
`SlotcaskResolvedRec` values by the hash's Kf shard, acquires one Kf reader
per partition, revalidates that each Kf entry still names its supplied
`(stream,file,offset)`, and fetches only those validated records before
releasing that Kf reader.  It never holds two Kf readers and never dispatches
a file-fetch worker after releasing the partition's Kf reader.

Replace query O_DIRECT segment enumeration with a Kf-driven streaming bridge:
one worker owns one Kf shard read handle, enumerates live Kf slots, groups
their authoritative `(stream,file,offset,hash)` locations by segment file,
then fetches/validates those locations before releasing the Kf handle.  It may
use direct I/O for those grouped reads; it must not enumerate live segment
flags as the source of truth.  Keep the raw segment scanner only for
maintenance/reindex operations that already hold the object write lock.

The shard-id-to-handle acquire uses the real kfcache API — the baseline has
no `kfcache_acquire_for_shard`; `kfcache_acquire()` (`src/db/slotcask.h`)
takes a kf path and slot capacity:

```c
static int kf_shard_acquire(SlotcaskKfHandle *kh, const SlotcaskDb *db,
                            int kf_shard_id, int writer) {
    char path[PATH_MAX];
    slotcask_kf_path(path, sizeof(path), db->data_dir, kf_shard_id);
    return kfcache_acquire(kh, path, db->slots_per_shard, writer);
}
```

The new complete private work item is:

```c
typedef struct {
    SlotcaskDb *db;
    int kf_shard_id;
    SlotcaskScanCb cb;
    void *ctx;
    _Atomic int *stop;
    int rc;
} KfLiveScanWork;

static void *slotcask_scan_live_kf_worker(void *raw) {
    KfLiveScanWork *w = raw;
    SlotcaskKfHandle kh;
    KfLiveAddress *live = NULL;
    size_t nlive = 0;

    if (kf_shard_acquire(&kh, w->db, w->kf_shard_id, 0) != 0)
        goto failed;
    if (collect_live_kf_addresses_locked(w->db, &kh, &live, &nlive) != 0)
        goto release_kf;
    if (fetch_live_addresses_grouped_locked(w->db, &kh, live, nlive,
                                            w->cb, w->ctx, w->stop) != 0)
        goto release_kf;
    kfcache_release(&kh);
    free(live);
    return NULL;

release_kf:
    kfcache_release(&kh);
failed:
    free(live);
    w->rc = -1;
    atomic_store_explicit(w->stop, 1, memory_order_release);
    return NULL;
}
```

`collect_live_kf_addresses_locked` and
`fetch_live_addresses_grouped_locked` are private helpers.  Their contracts:
the first emits only live Kf entries; the second accepts only a segment record
whose hash/key/value header matches the live address supplied by that Kf
entry.  Both run while the one shard read handle remains held.  No worker
holds two Kf locks. The query bridge dispatches this worker across Kf shards
with `parallel_for_io`; it merges callback/stop state only after all workers
return.

`fetch_live_addresses_grouped_locked` sorts the shard's collected addresses
by `(stream_id, file_id, offset)`, takes one segcache read handle per segment
file, and walks the mapping in ascending offset order — the page-fault
pattern is then near-sequential per file (readahead-friendly), there are zero
per-record syscalls while pages are resident, and at the default low split
counts one shard's records occupy roughly every `splits`-th slot so the walk
touches the same bytes the old raw scan read. Cold files may instead be read
through the existing mincore-adaptive O_DIRECT seam; the mmap walk is the
default. Complete implementations:

```c
typedef struct {
    uint8_t  stream_id;
    uint16_t file_id;
    uint32_t offset;
    uint8_t  hash[16];
} KfLiveAddress;

static int collect_live_kf_addresses_locked(SlotcaskDb *db,
                                            const SlotcaskKfHandle *kh,
                                            KfLiveAddress **out,
                                            size_t *nout) {
    KfLiveAddress *v = NULL;
    size_t n = 0, cap = 0;
    (void)db;

    for (size_t s = 0; s < kh->capacity; s++) {
        const SlotcaskKfEntry *e = &kh->map[s];
        if (__atomic_load_n(&e->flag, __ATOMIC_ACQUIRE) != 1) continue;
        if (n == cap) {
            size_t ncap = cap ? cap * 2 : 256;
            KfLiveAddress *nv = reallocarray(v, ncap, sizeof(*nv));
            if (!nv) { free(v); return -1; }
            v = nv; cap = ncap;
        }
        memcpy(v[n].hash, e->hash, 16);
        v[n].stream_id = e->stream_id;
        v[n].file_id   = e->file_id;
        v[n].offset    = e->offset;
        n++;
    }
    *out = v;
    *nout = n;
    return 0;
}

static int kf_live_addr_cmp(const void *a, const void *b) {
    const KfLiveAddress *x = a, *y = b;
    if (x->stream_id != y->stream_id) return x->stream_id < y->stream_id ? -1 : 1;
    if (x->file_id   != y->file_id)   return x->file_id   < y->file_id   ? -1 : 1;
    if (x->offset    != y->offset)    return x->offset    < y->offset    ? -1 : 1;
    return 0;
}

/* Accepts a segment record only when its header hash matches the live Kf
   address that named it; anything else (torn/stale/reused slot) is skipped.
   Runs entirely under the shard read handle plus one segcache rdlock per
   segment file. */
static int fetch_live_addresses_grouped_locked(SlotcaskDb *db,
                                               const SlotcaskKfHandle *kh,
                                               KfLiveAddress *live, size_t nlive,
                                               SlotcaskScanCb cb, void *ctx,
                                               _Atomic int *stop) {
    (void)kh;
    qsort(live, nlive, sizeof(*live), kf_live_addr_cmp);

    size_t i = 0;
    while (i < nlive) {
        size_t j = i + 1;
        while (j < nlive && live[j].stream_id == live[i].stream_id &&
               live[j].file_id == live[i].file_id)
            j++;
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, live[i].stream_id, live[i].file_id);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;
        for (size_t k = i; k < j; k++) {
            const uint8_t *rec = h.map + live[k].offset;
            if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) != 1) continue;
            if (memcmp(rec, live[k].hash, 16) != 0) continue;
            uint16_t klen = seg_rec_klen(rec);
            uint32_t vlen = seg_rec_vlen(rec);
            if (cb(live[k].hash, rec + 24, klen,
                   rec + 24 + klen, vlen, ctx) != 0) {
                segcache_release(&h);
                atomic_store_explicit(stop, 1, memory_order_release);
                return 0;
            }
        }
        segcache_release(&h);
        i = j;
    }
    return 0;
}
```

Design note — why Kf-major, not segment-major with a pre-gathered dead set:
gathering Kf tombstone positions and filtering a sequential segment scan by
them is incomplete, because a superseded update version has no Kf entry at
all (the slot was repointed to the new location, so the old segment position
appears in neither the live nor the tombstone set) — between A and T both
versions carry segment flag=1 and the dead set cannot disambiguate, which is
exactly Task 1's A/T duplicate case. It is also racy: a pre-gathered set is
a skewed snapshot, so a window that commits after one shard was gathered but
before its segment bytes are read leaves the gathered position stale — the
record then fails both the set check and the flag check, and a live record
vanishes from the scan. Only the Kf-major form (snapshot the shard's live
addresses under its held read lock, then fetch exactly those frozen
locations) delivers the per-shard pre-or-post contract: while the read
handle is held, no window on that shard can run A/I/K/T, so the fetched
bytes cannot change underneath the scan.

`read_record_ref_try()` retains its current contract: return `2` rather than
blocking if the Kf reader lock would block while an ordered B-tree walk owns a
B-tree reader lock.  Its caller releases the B-tree walk handle and retries
with `read_record_ref()`.

Replace `slotcask_walk_live_skip`'s raw `kf_scan_o_direct` path with the
Kf-lock-owning walker.  The walker consumes its global skip counter only while
the current shard reader is held; it preserves current offset/limit semantics
but does not read a Kf file outside `kfcache_acquire`.  Change
`slotcask_sum_kf_totals` to acquire/release one Kf reader per shard before it
reads that shard header.  Both operations intentionally provide a
per-shard—not whole-object—snapshot, matching the bulk-window contract.

All query plans that currently return a result directly from an index must
validate it at the Kf boundary before returning it:

```text
B-tree count / KeySet shortcut:
  collect a bounded chunk of candidate hashes while holding only the index lock
  release the index lock
  partition hashes by Kf shard
  acquire one Kf reader, validate/copy/count that partition, release it

Bitmap count:
  acquire one Kf reader for the Kf shard
  inspect the corresponding bitmap/index shard while retaining that Kf reader
  count only live Kf slots whose current record satisfies the criterion
  release the bitmap/index handle, then the Kf reader
```

The B-tree form must never block on Kf while retaining its B-tree lock.  The
bitmap form uses Kf -> bitmap order, which matches mutation order.  Candidate
collection obeys `QUERY_BUFFER_MB`: it is chunked and must not materialize an
unbounded KeySet merely to count.  Delete the direct `keyset_size()` and raw
bitmap-popcount success paths for queries that require an observable count;
they remain usable only as internal candidate producers.  A candidate that is
no longer the current Kf record is discarded.  This gives each Kf shard one
visibility point without serialising queries on unrelated shards.

`scan_shards_v2_o_direct_match` must be deleted or made a thin callback
adapter over the same Kf-driven walker; it may not retain a direct
segment-flag count implementation.  `scan_dispatch` must route through that
same Kf-driven adapter.  Raw segment O_DIRECT scans remain maintenance-only
under the object write lock.

## Task 3 — repair marker publication, replay, and all durability primitives

**Anchors:** `static int kf_marker_write_impl(`,
`static int kf_batch_marker_write_impl(`,
`static int kf_shard_marker_gate(`,
`static int seg_write_flag_durable(`,
`static int seg_write_flag(`,
`static inline int slotcask_tombstone_mark(` (originally
`slotcask_tombstone_and_push_back`, split — see the deferred-reclaim fix
under Task 3's `bulk_tombstone_old_payloads_locked` code block below),
`static void bulk_reclaim_old_payloads_locked(`,
`static int pool_split_leftover(`, and
`static int kf_marker_replay_upsert_entry_locked(` in `src/db/slotcask.c`.

**Test first for this task.** Add Task 1's retained-update/delete replay,
non-indexed gate, marker-collision, and header-durability regressions; record
their baseline failures before changing these primitives.

**Implementation.** Both single and batch marker writers use one common
atomic-publication helper.  It rejects an existing final marker with
`EEXIST`; callers must first run the marker gate under the Kf writer lock.
Temporary names contain the shard, batch identity when present, and a unique
nonce.  Recovery ignores/removes recognised temporary files only; they never
mean M occurred.

```c
/* New helper. Tri-state contract:
 *   0  = marker published (linked) and its publication is durable
 *   1  = marker IS published but publication durability is unconfirmed
 *        (post-link unlink/fsync_dir failure) — caller MUST treat this as a
 *        post-M failure: forward replay, else EINPROGRESS
 *   -1 = marker was never linked — safe plain pre-M failure */
static int marker_publish_atomic(const char *kf_dir, const char *final_name,
                                 const void *bytes, size_t len) {
    char tmp_path[PATH_MAX], final_path[PATH_MAX];
    const char *p = bytes;
    size_t left = len;
    int fd = -1;

    if (marker_make_unique_paths(kf_dir, final_name, tmp_path,
                                 sizeof(tmp_path), final_path,
                                 sizeof(final_path)) != 0)
        return -1;
    fd = open(tmp_path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return -1;
    while (left > 0) {
        ssize_t n = write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            goto fail_open;
        }
        p += n;
        left -= (size_t)n;
    }
    if (fsync(fd) != 0) goto fail_open;
    if (close(fd) != 0) { fd = -1; goto fail_open; }
    fd = -1;
    if (link(tmp_path, final_path) != 0) goto fail_open;

    /* Past this point the final marker exists: every remaining failure is
     * a post-M outcome and is reported as published-but-pending. */
    if (unlink(tmp_path) == 0 && fsync_dir(kf_dir) == 0)
        return 0;
    return 1;

fail_open:
    if (fd >= 0) close(fd);
    unlink(tmp_path);
    return -1;
}
```

`marker_make_unique_paths` is also new (snprintf of
`<final>.tmp.<shard>.<batch>.<nonce>` plus the final path, with truncation
checks); `fsync_dir` already exists in `src/db/slotcask.c`.

Use `rename(tmp_path, final_path)` only after a non-overwriting final-name
reservation is obtained atomically; this preserves the no-clobber contract on
filesystems where `rename` replaces its destination.  The executor must use
one supported atomic primitive consistently and add a collision regression.

Publishing wrappers (`kf_marker_write_impl`, `kf_batch_marker_write_impl`,
and the coordinator's `bulk_publish_window_marker_locked`) propagate the
tri-state unchanged. On `1` they must never re-attempt publication or write
a second marker for the same window — they proceed directly to forward
replay, then success or `EINPROGRESS`.

Upgrade gate: this release requires a clean shutdown of the previous
release before upgrade; that is the supported upgrade path. The new replay
cannot read legacy KFM1 markers anyway (they lack the hash/key/value spans
the redo record needs), and a legacy marker paired with a legacy abort
sidecar is a completed abort — forward replaying it would resurrect a
mutation the old binary rolled back. Rather than retain any sidecar
semantics, startup fails closed: if the marker sweep finds any file in the
kf marker namespace that is not a recognised KFM2 batch marker (legacy
single/batch markers, abort-sidecar debris, or unrecognised files —
recognised publication temporaries are still cleaned harmlessly), the
object refuses to open with an error directing the operator to boot the
previous release once to complete its recovery, then upgrade. This mirrors
the existing 2026.08.1 clean-open-evidence gating and keeps the failure
loud instead of silently corrupting. No sidecar code survives Task 5.

All single and bulk mutations call `kf_shard_marker_gate()` after acquiring
their shard writer and before deriving OLD/Kf state or publishing M,
regardless of `has_indexed_fields`.  When the gate finds a retained final
marker it emits exactly one
`LOG_ERROR(LOG_SUB_DURABILITY, "kf shard %d: retained commit marker %s; attempting synchronous forward replay", ...)`
(`slotcask.c` gains the `log.h` include), attempts synchronous forward
replay under the shard writer lock, clears the marker and continues the new
mutation if replay reaches C, and otherwise rejects the mutation with
`EINPROGRESS`.  No write passes on that shard while its marker is retained.
Pass `header_changed=1` to every live or
replay Kf operation that inserts or tombstones an entry; use `0` only for a
pure address repoint with unchanged counters.

Flag-writing helpers acquire segment cache handles with `writer=0` — the
rdlock discipline `seg_write_flag()` already documents for single-byte
writes to unique offsets — and never attempt an rwlock upgrade.  Concurrent
msync/fdatasync of one segment file from different window workers is
allowed: both are safe on overlapping ranges, and fdatasync is file-wide, so
each window's barrier also flushes other windows' already-ordered bytes.  Marker replay retains the marker until all P/A/I/K/T actions are
durable and always replays them forward idempotently. A failed I/K/T action
retains only the marker and returns pending after one synchronous forward
replay attempt; it never applies inverse index work, creates an abort
sidecar, or deletes evidence before C.

The marker payload is the complete redo record: operation kind; Kf shard and
slot; hash/key identity; OLD and NEW segment locations; and enough typed
OLD/NEW value information to regenerate every secondary-index delta. Marker
construction validates all CAS/policy conditions, destination reservations,
and index-plan allocation before M. Therefore no legitimate rejection path
exists after M. Each forward replay step is idempotent: activating an
already-live NEW record, inserting/deleting an already-converged index entry,
repointing an already-correct Kf slot, and tombstoning an already-dead OLD
record all succeed as no-ops after verifying identity.

## Task 4 — replace all mutation paths with one window coordinator

**Anchors:** `int slotcask_insert(`, `int slotcask_update(`,
`int slotcask_delete(`, `int slotcask_bulk_update(`,
`int slotcask_upsert_with_hooks(`,
`int slotcask_delete_with_hooks(`,
`int slotcask_bulk_upsert_in_kfshard(`, and
`int slotcask_bulk_delete_in_kfshard(` in `src/db/slotcask.c`; plus, in
`src/db/slotcask.h`, the window-hook surface bounded by these four quoted
lines (everything between them — the three hook typedefs, `SlotcaskBulkOpts`,
and `SlotcaskBulkDeleteOpts` — is the anchor surface whose hook semantics
Task 4 reshuffles):

```text
typedef int (*slotcask_bulk_prepare_window_fn)(SlotcaskBulkRec *recs,
typedef void (*slotcask_bulk_abort_window_fn)(void *ctx);
} SlotcaskBulkOpts;
} SlotcaskBulkDeleteOpts;
```

(The doc comment above the first typedef opens with `Two-phase,
window-scoped hooks for indexed bulk-insert windows` and closes several
lines later; it is a landmark only — never match on it.)

**Test first for this task.** Add Task 1's P/M/A/I/K/T/C matrix, small-window
partial-success case, and fixed-value-P reader-progress case before replacing
the coordinators.

**Implementation.** Replace divergent slow/fast/single/bulk implementations
with public adapters over one transaction coordinator.  Keep existing public
function signatures and existing query hook semantics; only the private
coordinator interface is new.

```c
typedef enum {
    BULK_MUTATION_UPSERT,
    BULK_MUTATION_DELETE,
} BulkMutationKind;

typedef struct {
    int kf_shard_id;
    SlotcaskBulkRec *recs;
    SlotcaskBulkState *st;      /* per-record scratch; parallel to recs */
    size_t nrecs;
    size_t cursor;
    BulkMutationKind kind;
    const SlotcaskBulkOpts *upsert_opts;
    const SlotcaskBulkDeleteOpts *delete_opts;
    int rc;
} BulkMutationShard;

typedef struct {
    SlotcaskDb *db;
    BulkMutationShard *shards;
    size_t nshards;
    size_t window_cap;
    _Atomic int cancelled;
} BulkMutationTxn;

static int slotcask_bulk_mutation_transaction(BulkMutationTxn *txn) {
    if (bulk_stage_payload_wave(txn) != 0) return -1;
    if (bulk_commit_kf_windows_wave(txn) != 0) return -1;
    return bulk_finish_status(txn);
}
```

`bulk_stage_payload_wave` is used only for upserts with fixed NEW bytes.  It
allocates/reserves all segment destinations, groups writes by `(stream,file)`,
uses `parallel_for_io` to write `flag=0` payloads, then msyncs the merged
dirty ranges and fdatasyncs each affected file once. It waits for every worker
and affected-file sync before any marker can be published. Failed/cancelled
staging leaves inactive payloads only; recovery may reclaim them.

`bulk_commit_kf_windows_wave` dispatches independent Kf-shard work with
`parallel_for_io`.  Each worker processes consecutive `window_cap` records of
its own shard only, and each window executes the following complete function:

```c
static int bulk_commit_one_kf_window(BulkMutationTxn *txn,
                                     BulkMutationShard *shard,
                                     size_t begin, size_t end) {
    SlotcaskKfHandle kh;
    BulkWindowPlan plan = {0};
    int prc, rc = -1;

    if (kf_shard_acquire(&kh, txn->db, shard->kf_shard_id, 1) != 0)
        return -1;
    if (kf_shard_marker_gate(shard->kf_shard_id, &kh, txn->db->data_dir) != 0) {
        rc = -2;                 /* retained marker; replay did not converge */
        errno = EINPROGRESS;
        goto out;
    }
    if (bulk_plan_window_locked(txn, shard, begin, end, &kh, &plan) != 0)
        goto out;
    prc = bulk_publish_window_marker_locked(txn, &kh, &plan);
    if (prc < 0) goto out;        /* pre-M: nothing was published */
    if (prc > 0) goto replay;     /* published, durability unconfirmed */
    if (bulk_activate_new_payloads_locked(txn, &plan) != 0) goto replay;
    if (bulk_apply_and_sync_indexes_locked(txn, &plan) != 0) goto replay;
    if (bulk_apply_and_sync_kf_locked(txn, &kh, &plan) != 0) goto replay;
    if (bulk_tombstone_old_payloads_locked(txn, &plan) != 0) goto replay;
    if (bulk_clear_window_marker_locked(txn, &plan) != 0) goto replay;
    rc = 0;

replay:
    if (rc != 0 && bulk_replay_window_forward_locked(txn, &kh, &plan) == 0)
        rc = 0;
    if (rc != 0) { errno = EINPROGRESS; rc = -2; }  /* -2 = outcome pending */
out:
    bulk_window_plan_destroy(&plan);
    kfcache_release(&kh);
    return rc;
}
```

`BulkWindowPlan` is heap-owned.  It owns the marker entries, active/rejected
record index vectors, Kf slot vector, and index touch set; no array scales
with `BULK_COMMIT_WINDOW` on the stack.  The executor must use checked
`calloc`/`reallocarray` arithmetic for every allocation.
`kf_shard_acquire()` is the private wrapper over the real
`kfcache_acquire()` path/capacity API introduced in Task 2 (writer=1 here);
`bulk_publish_window_marker_locked` propagates
`marker_publish_atomic`'s tri-state (`0` durable, `-1` nothing published,
`1` published-pending → replay, then success or `EINPROGRESS`).

### Complete window-step implementations

`SlotcaskBulkState` gains a `KfInsertPlan kf_plan;` member (the
plan/commit seam at `slotcask.c:2340-2503`) and a `uint8_t staged_in_wave;`
flag set by the P wave; the existing `plan_slot` / `plan_reused_tomb` /
`has_plan` fields fold into it. Window hooks keep
their signatures but their work is reshuffled to the new order: every
durable index mutation moves into `apply_window` (post-M);
`prepare_window` only validates and stages. Hook index callbacks become
idempotent on convergence (inserting an identical entry or deleting an
absent one is a no-op) because replay re-invokes `apply_window`.

```c
typedef struct { uint8_t sid; uint16_t fid; uint32_t off; } SegLoc;

static int segloc_cmp(const void *a, const void *b) {
    const SegLoc *x = a, *y = b;
    if (x->sid != y->sid) return x->sid < y->sid ? -1 : 1;
    if (x->fid != y->fid) return x->fid < y->fid ? -1 : 1;
    if (x->off != y->off) return x->off < y->off ? -1 : 1;
    return 0;
}

typedef struct {
    char     field[128];        /* matches BitmapPrepareEntry.field width */
    int      idx_shard;
    int      type;              /* enum IndexType */
    uint8_t  hash16[16];        /* representative hash for the flush call */
} IdxTouch;

typedef struct { IdxTouch *v; size_t n, cap; } IdxTouchSet;

/* Installed by the coordinator while apply_window fires. The query_bulk.c
   window hooks call this instead of setting sync_after=1; a NULL target
   means the single-record path, where the hook still syncs itself. */
static _Thread_local IdxTouchSet *tls_idx_touch;

static void idx_touch_record(const char *field, int idx_shard, int type,
                             const uint8_t hash16[16]) {
    IdxTouchSet *s = tls_idx_touch;
    if (!s) return;
    if (s->n == s->cap) {
        size_t ncap = s->cap ? s->cap * 2 : 16;
        IdxTouch *nv = reallocarray(s->v, ncap, sizeof(*nv));
        if (!nv) return;               /* I barrier reports the OOM instead */
        s->v = nv; s->cap = ncap;
    }
    snprintf(s->v[s->n].field, sizeof(s->v[0].field), "%s", field);
    s->v[s->n].idx_shard = idx_shard;
    s->v[s->n].type = type;
    memcpy(s->v[s->n].hash16, hash16, 16);
    s->n++;
}

enum { KF_BATCH_MARKER_MAGIC = 0x4B464D32u };  /* "KFM2" */

typedef struct __attribute__((packed)) {
    uint32_t magic;             /* KF_BATCH_MARKER_MAGIC */
    uint32_t version;
    uint32_t count;
    uint32_t reserved;
} BatchMarkerHeader;

_Static_assert(sizeof(BatchMarkerHeader) == 16, "fixed on-disk header");

/* One redo record: the unchanged 32-byte KfMarkerSlot plus the identity and
   typed-value spans needed to regenerate every index delta. Serialized as
   header + entries, each entry followed by key, old_value, new_value. */
typedef struct {
    KfMarkerSlot slot;          /* slot.checksum = 0 when written; patched
                                   below to cover the whole entry */
    uint8_t  hash[16];
    uint16_t klen;
    uint16_t old_vlen;
    uint16_t new_vlen;
} BatchMarkerEntry;

typedef struct {
    uint32_t           batch_id;   /* window's begin index — unique per shard */
    int                kf_shard_id;
    BulkMutationShard *shard;
    BatchMarkerEntry  *entries;    /* parallel to active[] */
    size_t            *active;     /* indices into shard->recs[] */
    size_t             nactive;
    size_t            *kf_slots;   /* filled by K; deduped before sync */
    size_t             nkf_slots;
    int                kf_header_changed;
    IdxTouchSet        touch;
} BulkWindowPlan;

static void bulk_window_plan_destroy(BulkWindowPlan *plan) {
    if (!plan) return;
    free(plan->entries);
    free(plan->active);
    free(plan->kf_slots);
    free(plan->touch.v);
    memset(plan, 0, sizeof(*plan));
}
```

Planning runs entirely under the shard writer lock. Fixed-value records
reuse the payload staged by the P wave; OLD-derived records (`value_compute`,
CAS via `pre_commit`, or `require_existing`) read OLD here and stage NEW
synchronously — the deliberate progress sacrifice the contract describes.

```c
/* Grouped OLD reads under the writer — same file-run discipline as the
   current Phase 1b (sort by (sid,fid,off), one segcache rdlock per file). */
static int bulk_read_old_values(SlotcaskDb *db, SlotcaskBulkRec *recs,
                                SlotcaskBulkState *st,
                                int *idx, int nidx) {
    SLOTCASK_SORT_IDX_BY_SEG_LOC(idx, nidx, st);
    int k = 0;
    while (k < nidx) {
        int run_end = k + 1;
        uint8_t sid = st[idx[k]].old_sid;
        uint16_t fid = st[idx[k]].old_fid;
        while (run_end < nidx &&
               st[idx[run_end]].old_sid == sid && st[idx[run_end]].old_fid == fid)
            run_end++;
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, sid, fid);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) {
            for (int j = k; j < run_end; j++) recs[idx[j]].status = -1;
            k = run_end;
            continue;
        }
        for (int j = k; j < run_end; j++) {
            SlotcaskBulkRec *r = &recs[idx[j]];
            SlotcaskBulkState *s = &st[idx[j]];
            const uint8_t *rec = h.map + s->old_off;
            if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) != 1) { r->status = -1; continue; }
            uint16_t klen = seg_rec_klen(rec);
            uint32_t vlen = seg_rec_vlen(rec);
            if (klen != r->klen || memcmp(rec + 24, r->key, r->klen) != 0) { r->status = -1; continue; }
            uint8_t *buf = malloc(vlen ? vlen : 1);
            if (!buf) { r->status = -1; continue; }
            if (vlen) memcpy(buf, rec + 24 + r->klen, vlen);
            s->old_buf = buf;
            s->old_vlen = vlen;
        }
        segcache_release(&h);
        k = run_end;
    }
    return 0;
}

static int bulk_plan_window_locked(BulkMutationTxn *txn,
                                   BulkMutationShard *shard,
                                   size_t begin, size_t end,
                                   SlotcaskKfHandle *kh,
                                   BulkWindowPlan *plan) {
    SlotcaskBulkRec *recs = shard->recs;
    SlotcaskBulkState *st = shard->st;
    const SlotcaskBulkOpts *uo = shard->kind == BULK_MUTATION_UPSERT
                                 ? txn->upsert_opts : NULL;
    size_t span = end - begin;
    int needs_old = 0;
    int *old_idx = NULL;

    plan->batch_id = (uint32_t)begin;
    plan->kf_shard_id = shard->kf_shard_id;
    plan->shard = shard;
    plan->entries = calloc(span, sizeof(*plan->entries));
    plan->active = calloc(span, sizeof(*plan->active));
    old_idx = malloc(span * sizeof(int));
    if (!plan->entries || !plan->active || !old_idx) goto oom;
    int nold = 0;

    for (size_t i = begin; i < end; i++) {
        SlotcaskBulkRec *r = &recs[i];
        SlotcaskBulkState *s = &st[i];
        uint8_t old_flag = 0;
        int found;

        r->status = 0;
        r->was_update = 0;
        if (r->klen > UINT16_MAX || r->vlen > UINT32_MAX ||
            (size_t)24 + r->klen + r->vlen > (size_t)txn->db->slot_size) {
            r->status = -1;
            continue;               /* staged payload (if any) stays flag=0 */
        }
        compute_hash(r->key, r->klen, s->hash);
        found = (kf_lookup_with_slot(kh, s->hash, r->key, r->klen,
                                     txn->db->data_dir, &old_flag,
                                     &s->old_sid, &s->old_fid,
                                     &s->old_off, &s->old_kf_slot) == 0);
        s->old_found = (uint8_t)(found ? 1 : 0);
        s->target_stream = (uint8_t)((unsigned)s->hash[15] %
                                     (unsigned)txn->db->num_streams);

        if (found && uo && (uo->if_not_exists || r->if_not_exists)) {
            r->status = -2; r->was_update = 1;
            continue;               /* CAS reject: retire staged payload */
        }
        if (!found && uo && uo->require_existing) { r->status = -2; continue; }

        if (uo && (uo->value_compute || uo->pre_commit_needs_old)) {
            if (found && r->old_value == NULL) old_idx[nold++] = (int)i;
        }
        s->needs_write = 1;
    }

    if (nold > 0) {
        bulk_read_old_values(txn->db, recs, st, old_idx, nold);
        for (int j = 0; j < nold; j++) {
            int i = old_idx[j];
            if (recs[i].status != 0) continue;
            const void *ov = recs[i].old_value ? recs[i].old_value : st[i].old_buf;
            size_t ol = recs[i].old_value ? recs[i].old_vlen : st[i].old_vlen;
            SlotcaskOldRecord old_rec = { (const uint8_t *)ov, ol };
            if (uo->value_compute(st[i].old_found ? &old_rec : NULL,
                                  &recs[i]) != 0) {
                recs[i].status = -2;
                st[i].needs_write = 0;
            }
        }
    }

    for (size_t i = begin; i < end; i++) {
        SlotcaskBulkRec *r = &recs[i];
        SlotcaskBulkState *s = &st[i];
        if (r->status != 0 || !s->needs_write) continue;

        if (!s->old_found) {
            if (kf_plan_window_insert_slot(txn->db, kh, s->hash,
                                           r->key, r->klen,
                                           &s->kf_plan) != 0) {
                r->status = -1;
                continue;
            }
            s->has_plan = 1;
        }
        /* OLD-derived records never went through the P wave: stage NEW
           synchronously here (seg_write_record_varlen per the anchor at
           slotcask.c:3908) so M still covers a durable payload. */
        if (!s->staged_in_wave &&
            seg_write_record_varlen(txn->db, s->target_stream, r->key,
                                    r->klen, r->value, r->vlen,
                                    &s->target_fid, &s->target_off) != 0) {
            r->status = -1;
            continue;
        }
    }

    if (uo && uo->prepare_window && uo->apply_window) {
        size_t n = 0;
        for (size_t i = begin; i < end; i++)
            if (recs[i].status == 0 && st[i].needs_write)
                plan->active[n++] = i;
        if (uo->prepare_window(recs, plan->active, n,
                               uo->bulk_hook_ctx) != 0) goto hard_fail;
        /* hook may have set status=-1/-2 (policy); rebuild below */
    }

    plan->nactive = 0;
    for (size_t i = begin; i < end; i++) {
        SlotcaskBulkState *s = &st[i];
        BatchMarkerEntry *e;
        if (recs[i].status != 0 || !s->needs_write) continue;
        e = &plan->entries[plan->nactive];
        memset(e, 0, sizeof(*e));
        e->slot.magic = KF_MARKER_MAGIC;
        e->slot.op = shard->kind == BULK_MUTATION_DELETE
                     ? KF_MARKER_OP_DELETE : KF_MARKER_OP_UPSERT;
        e->slot.kf_slot = s->old_found ? (uint32_t)s->old_kf_slot : UINT32_MAX;
        e->slot.has_old = s->old_found;
        e->slot.old_stream_id = s->old_sid;
        e->slot.old_file_id = s->old_fid;
        e->slot.old_offset = s->old_off;
        e->slot.new_stream_id = s->target_stream;
        e->slot.new_file_id = s->target_fid;
        e->slot.new_offset = s->target_off;
        memcpy(e->hash, s->hash, 16);
        e->klen = (uint16_t)recs[i].klen;
        e->new_vlen = (uint16_t)recs[i].vlen;
        e->old_vlen = (uint16_t)s->old_vlen;
        plan->active[plan->nactive++] = i;
    }
    free(old_idx);
    return 0;

oom:
hard_fail:
    free(old_idx);
    return -1;
}
```

(The delete planning path mirrors this loop with `SlotcaskBulkDeleteOpts`
and its per-record `pre_commit` firing exactly as
`slotcask_bulk_delete_in_kfshard` does today. `SlotcaskBulkState` gains the
`staged_in_wave` flag set by the P wave; fixed-value records skip the
synchronous re-stage.)

Publication serializes the checked array once and hands it to
`marker_publish_atomic`:

```c
static int buf_append(uint8_t **buf, size_t *len, size_t *cap,
                      const void *src, size_t n) {
    if (*len + n > *cap) {
        size_t ncap = *cap ? *cap : 256;
        while (ncap < *len + n) ncap *= 2;
        uint8_t *nb = reallocarray(*buf, ncap, 1);
        if (!nb) return -1;
        *buf = nb; *cap = ncap;
    }
    memcpy(*buf + *len, src, n);
    *len += n;
    return 0;
}

static int bulk_publish_window_marker_locked(BulkMutationTxn *txn,
                                             SlotcaskKfHandle *kh,
                                             BulkWindowPlan *plan) {
    char kf_dir[PATH_MAX], final[64];
    BatchMarkerHeader hdr = { KF_BATCH_MARKER_MAGIC, 1,
                              (uint32_t)plan->nactive, 0 };
    uint8_t *buf = NULL;
    size_t len = 0, cap = 0;
    int rc;

    (void)kh;
    if (plan->nactive == 0) return 0;
    if (buf_append(&buf, &len, &cap, &hdr, sizeof(hdr)) != 0) goto oom;
    for (size_t i = 0; i < plan->nactive; i++) {
        const BatchMarkerEntry *e = &plan->entries[i];
        const SlotcaskBulkRec *r = &plan->shard->recs[plan->active[i]];
        const SlotcaskBulkState *s = &plan->shard->st[plan->active[i]];
        size_t at;
        if (buf_append(&buf, &len, &cap, e, sizeof(*e)) != 0) goto oom;
        at = len - sizeof(*e);
        if (e->klen     && buf_append(&buf, &len, &cap, r->key,   e->klen)     != 0) goto oom;
        if (e->old_vlen && buf_append(&buf, &len, &cap, s->old_buf, e->old_vlen) != 0) goto oom;
        if (e->new_vlen && buf_append(&buf, &len, &cap, r->value, e->new_vlen) != 0) goto oom;
        uint32_t sum = XXH32(buf + at, len - at, 0);   /* entry written with
                                                          slot.checksum = 0 */
        memcpy(buf + at + offsetof(BatchMarkerEntry, slot) +
               offsetof(KfMarkerSlot, checksum), &sum, sizeof(sum));
    }
    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", txn->db->data_dir);
    snprintf(final, sizeof(final), "%d_batch_%u_marker.dat",
             plan->kf_shard_id, plan->batch_id);
    rc = marker_publish_atomic(kf_dir, final, buf, len);
    free(buf);
    return rc;

oom:
    free(buf);
    return -1;
}
```

A and T share one grouped flag-write + sync pass; K syncs through the
existing checked slot-sync helper after sorting and deduping slots:

```c
/* store=0: sync-only pass (P barrier and T after tombstone_and_push_back).
   store=1: atomic-store `flag` at each offset first. One msync pass with
   gaps <= 4096 merged and one fdatasync per file. */
static int bulk_seg_apply_and_sync(SlotcaskDb *db, const SegLoc *locs,
                                   size_t n, int store, uint8_t flag) {
    size_t i = 0;
    while (i < n) {
        size_t j = i + 1;
        while (j < n && locs[j].sid == locs[i].sid && locs[j].fid == locs[i].fid)
            j++;
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, locs[i].sid, locs[i].fid);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;
        for (size_t k = i; k < j; k++) {
            if (store)
                __atomic_store_n(&h.map[locs[k].off + 18], flag,
                                 __ATOMIC_RELEASE);
        }
        size_t lo = locs[i].off, hi = locs[i].off + 1;
        for (size_t k = i + 1; k < j; k++) {
            if (locs[k].off <= hi + 4096) { hi = locs[k].off + 1; continue; }
            if (durability_msync_range(h.map, lo, hi - lo) != 0) {
                segcache_release(&h); return -1;
            }
            lo = locs[k].off; hi = lo + 1;
        }
        if (durability_msync_range(h.map, lo, hi - lo) != 0 ||
            fdatasync(h.fd) != 0) {
            segcache_release(&h);
            return -1;
        }
        segcache_release(&h);
        i = j;
    }
    return 0;
}

static int bulk_activate_new_payloads_locked(BulkMutationTxn *txn,
                                             BulkWindowPlan *plan) {
    SegLoc *locs;
    size_t n = 0;
    int rc;

    if (plan->shard->kind == BULK_MUTATION_DELETE) return 0;
    locs = calloc(plan->nactive, sizeof(*locs));
    if (!locs) return -1;
    for (size_t i = 0; i < plan->nactive; i++) {
        const BatchMarkerEntry *e = &plan->entries[i];
        if (e->slot.op != KF_MARKER_OP_UPSERT) continue;
        locs[n].sid = e->slot.new_stream_id;
        locs[n].fid = e->slot.new_file_id;
        locs[n].off = e->slot.new_offset;
        n++;
    }
    qsort(locs, n, sizeof(*locs), segloc_cmp);
    rc = bulk_seg_apply_and_sync(txn->db, locs, n, 1, 1);
    free(locs);
    return rc;
}

static int idx_touch_cmp(const void *a, const void *b) {
    const IdxTouch *x = a, *y = b;
    int c = strcmp(x->field, y->field);
    if (c) return c;
    return x->idx_shard < y->idx_shard ? -1 : x->idx_shard > y->idx_shard;
}

static int bulk_apply_and_sync_indexes_locked(BulkMutationTxn *txn,
                                              BulkWindowPlan *plan) {
    BulkMutationShard *shard = plan->shard;
    char eff_root[PATH_MAX], object[256];
    int rc = 0;

    if (shard->kind == BULK_MUTATION_DELETE) {
        if (!txn->delete_opts || !txn->delete_opts->apply_window) return 0;
        tls_idx_touch = &plan->touch;
        rc = txn->delete_opts->apply_window(shard->recs, plan->active,
                                            plan->nactive,
                                            txn->delete_opts->bulk_hook_ctx);
        tls_idx_touch = NULL;
    } else {
        if (!txn->upsert_opts || !txn->upsert_opts->apply_window) return 0;
        tls_idx_touch = &plan->touch;
        rc = txn->upsert_opts->apply_window(shard->recs, plan->active,
                                            plan->nactive,
                                            txn->upsert_opts->bulk_hook_ctx);
        tls_idx_touch = NULL;
    }
    if (rc != 0) return -1;

    /* one durable sync per touched (field, idx shard): dedupe, then flush
       via the existing per-record flush seam with one representative hash */
    qsort(plan->touch.v, plan->touch.n, sizeof(IdxTouch), idx_touch_cmp);
    size_t w = 0;
    for (size_t i = 0; i < plan->touch.n; i++)
        if (!w || idx_touch_cmp(&plan->touch.v[w - 1], &plan->touch.v[i]) != 0)
            plan->touch.v[w++] = plan->touch.v[i];
    plan->touch.n = w;

    split_data_dir(txn->db->data_dir, eff_root, sizeof(eff_root),
                   object, sizeof(object));
    for (size_t i = 0; i < plan->touch.n; i++) {
        const IdxTouch *t = &plan->touch.v[i];
        const char *field = t->field;
        if (index_sync_record_fields(eff_root, object, txn->db->splits,
                                     t->hash16, &field,
                                     (const enum IndexType *)&t->type,
                                     1) != 0)
            return -1;
    }
    return 0;
}

/* Hoisted unchanged from the kf-tombstone step inside
   slotcask_bulk_delete_in_kfshard's window loop: writes flag=2 and bumps
   the shard's `deleted` counter under the held writer. */
static int kf_tombstone_entry_locked(SlotcaskKfHandle *kh,
                                     const uint8_t hash[16],
                                     const void *key, size_t klen,
                                     const char *data_dir);

static int size_cmp(const void *a, const void *b) {
    size_t x = *(const size_t *)a, y = *(const size_t *)b;
    return x < y ? -1 : x > y ? 1 : 0;
}

static int bulk_apply_and_sync_kf_locked(BulkMutationTxn *txn,
                                         SlotcaskKfHandle *kh,
                                         BulkWindowPlan *plan) {
    BulkMutationShard *shard = plan->shard;
    size_t used_delta = 0;

    plan->kf_slots = calloc(plan->nactive, sizeof(size_t));
    if (!plan->kf_slots) return -1;
    for (size_t i = 0; i < plan->nactive; i++) {
        SlotcaskBulkRec *r = &shard->recs[plan->active[i]];
        SlotcaskBulkState *s = &shard->st[plan->active[i]];
        BatchMarkerEntry *e = &plan->entries[i];
        size_t out_slot;

        if (e->slot.op == KF_MARKER_OP_DELETE) {
            if (kf_tombstone_entry_locked(kh, e->hash, r->key, r->klen,
                                          txn->db->data_dir) != 0) return -1;
            out_slot = s->old_kf_slot;
            plan->kf_header_changed = 1;
        } else if (s->old_found) {
            if (kf_repoint(kh, s->hash, s->target_stream, s->target_fid,
                           s->target_off, r->key, r->klen,
                           txn->db->data_dir) != 0) return -1;
            out_slot = s->old_kf_slot;
        } else {
            kf_commit_planned_slot(kh, &s->kf_plan, s->target_stream,
                                   s->target_fid, s->target_off,
                                   &used_delta, &out_slot);
            plan->kf_header_changed = 1;
        }
        plan->kf_slots[plan->nkf_slots++] = out_slot;
    }

    qsort(plan->kf_slots, plan->nkf_slots, sizeof(size_t), size_cmp);
    size_t w = 0;
    for (size_t i = 0; i < plan->nkf_slots; i++)
        if (!w || plan->kf_slots[w - 1] != plan->kf_slots[i])
            plan->kf_slots[w++] = plan->kf_slots[i];
    plan->nkf_slots = w;
    return kfcache_sync_slots_locked(kh, plan->kf_slots, plan->nkf_slots,
                                     plan->kf_header_changed);
}

/* T marks OLD dead (flag=2) but must NOT return its capacity to the free
   pool here. bulk_stage_payload_wave (P) runs transaction-wide, for every
   shard, BEFORE bulk_commit_kf_windows_wave (M..C) touches any shard (see
   the two-wave split below) -- so if T's own pool_push_free_cap ran
   immediately, a *later, unrelated* transaction's P-phase pop
   (pool_try_pop_for_size) could steal and overwrite this exact OLD slot
   with a fresh flag=0 pending payload before THIS window's marker is
   durably cleared. A subsequent Gap-C replay of that retained marker would
   then read flag==0 at old_offset (or a foreign record entirely) instead
   of the flag==2 it requires, and incorrectly conclude OLD was never
   tombstoned -- permanently wedging that Kf shard. The fix: split "mark"
   from "reclaim" and defer reclaim to bulk_reclaim_old_payloads_locked,
   called only after bulk_clear_window_marker_locked (C) succeeds, in both
   the live straight-line path and the inline forward-replay path. This
   changes reclaim TIMING only, not whether/when the flag=2 write happens
   (T still writes it synchronously, unconditionally, exactly as before) --
   on the fault-free path T and C already run back-to-back with nothing in
   between, so this costs nothing extra; it only means a transaction's OLD
   capacity stays quarantined (not corrupt, just not yet reusable) for the
   rare, bounded, self-healing duration of an outstanding recovery window. */
static int bulk_tombstone_old_payloads_locked(BulkMutationTxn *txn,
                                              BulkWindowPlan *plan) {
    SegLoc *locs = calloc(plan->nactive, sizeof(*locs));
    size_t n = 0;
    int rc;

    if (!locs) return -1;
    for (size_t i = 0; i < plan->nactive; i++) {
        const BatchMarkerEntry *e = &plan->entries[i];
        if (!e->slot.has_old) continue;      /* fresh insert: nothing dead */
        locs[n].sid = e->slot.old_stream_id;
        locs[n].fid = e->slot.old_file_id;
        locs[n].off = e->slot.old_offset;
        n++;
    }
    for (size_t i = 0; i < n; i++) {
        char path[PATH_MAX];
        SlotcaskSegHandle h;
        int dead;
        seg_path_for(path, txn->db->data_dir, locs[i].sid, locs[i].fid);
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) { free(locs); return -1; }
        dead = __atomic_load_n(&h.map[locs[i].off + 18], __ATOMIC_ACQUIRE) == 2;
        segcache_release(&h);
        if (dead) continue;                  /* idempotent re-run */
        if (slotcask_tombstone_mark(txn->db, locs[i].sid,
                                    locs[i].fid,
                                    locs[i].off) != 0) {
            free(locs);
            return -1;
        }
    }
    qsort(locs, n, sizeof(*locs), segloc_cmp);
    rc = bulk_seg_apply_and_sync(txn->db, locs, n, 0, 0);
    free(locs);
    return rc;
}

static int bulk_clear_window_marker_locked(BulkMutationTxn *txn,
                                           BulkWindowPlan *plan) {
    char kf_dir[PATH_MAX], path[PATH_MAX];

    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", txn->db->data_dir);
    snprintf(path, sizeof(path), "%s/%d_batch_%u_marker.dat",
             kf_dir, plan->kf_shard_id, plan->batch_id);
    if (unlink(path) != 0) return -1;
    return fsync_dir(kf_dir);
}

/* Called only once C (bulk_clear_window_marker_locked) has succeeded.
   Re-reads klen/vlen from the (already-tombstoned) segment header rather
   than caching them from T, since tombstoning only touches the flag byte. */
static void bulk_reclaim_old_payloads_locked(BulkMutationTxn *txn,
                                             BulkWindowPlan *plan) {
    for (size_t i = 0; i < plan->nactive; i++) {
        const BatchMarkerEntry *e = &plan->entries[i];
        if (!e->slot.has_old) continue;
        char path[PATH_MAX];
        SlotcaskSegHandle h;
        seg_path_for(path, txn->db->data_dir, e->slot.old_stream_id,
                    e->slot.old_file_id);
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) continue;
        uint16_t klen = seg_rec_klen(h.map + e->slot.old_offset);
        uint32_t vlen = seg_rec_vlen(h.map + e->slot.old_offset);
        uint32_t cap = (uint32_t)slotcask_record_size_varlen(klen, vlen);
        segcache_release(&h);
        pool_push_free_cap(&txn->db->streams[e->slot.old_stream_id],
                           e->slot.old_file_id, e->slot.old_offset, cap,
                           txn->db->slot_size);
    }
}

static int bulk_replay_window_forward_locked(BulkMutationTxn *txn,
                                             SlotcaskKfHandle *kh,
                                             BulkWindowPlan *plan) {
    /* Forward-only idempotent convergence to C; every step verifies
       identity before acting, so re-running a partial failure is safe. */
    if (bulk_activate_new_payloads_locked(txn, plan) != 0) return -1;
    if (bulk_apply_and_sync_indexes_locked(txn, plan) != 0) return -1;
    if (bulk_apply_and_sync_kf_locked(txn, kh, plan) != 0) return -1;
    if (bulk_tombstone_old_payloads_locked(txn, plan) != 0) return -1;
    if (bulk_clear_window_marker_locked(txn, plan) != 0) return -1;
    bulk_reclaim_old_payloads_locked(txn, plan);
    return 0;
}
```

`bulk_commit_one_kf_window`'s straight-line path gets the same addition:
call `bulk_reclaim_old_payloads_locked(txn, &plan)` immediately after
`bulk_clear_window_marker_locked` returns `0`, before setting `rc = 0`.

`slotcask_tombstone_and_push_back` is split into `slotcask_tombstone_mark`
(flag=2 write only, used by T above) and the reclaim call folded directly
into `bulk_reclaim_old_payloads_locked`; the combined mark+push helper is
deleted, since T was its only caller.

**Companion gap in the gate-replay path (found while designing the above
fix, not part of the original Task 3 scope, but in the same subsystem and
required for the same invariant to actually hold end-to-end):**
`kf_marker_replay_upsert_entry_locked`'s steps-3-5 branch — reached when a
*separate* transaction (or the pre-open startup sweep,
`marker_recovery_sweep_object`) discovers a retained marker whose crash
happened *before* the original write's own T step ever ran (OLD was still
live, not yet flag=2) — repoints the Kf slot and reconciles indexes but
never tombstones OLD at all. Left as-is, this permanently strands OLD at
flag==1 (live): a duplicate live record visible to any raw segment scan,
and a leaked slot the free pool never gets back. `kf_marker_replay_delete_entry_locked`
does not have this gap — its completion path already calls
`seg_write_flag_durable(..., 2)` on the old record.

Fix: after `kf_marker_apply_recovery_diff` succeeds in the steps-3-5
branch, when `marker->has_old`, call
`seg_write_flag_durable(data_dir, marker->old_stream_id, marker->old_file_id, marker->old_offset, 2)`
before returning success. This does **not** also call `pool_push_free_cap`
— unlike the live coordinator, this function runs both from the live gate
(`kf_shard_marker_gate`, called with a real, open `SlotcaskDb`) and from
the pre-open startup sweep (`marker_recovery_sweep_object`, called before
`slotcask_open` — no live `SlotcaskDb`/free-pool exists yet to push into).
Durably marking flag=2 is sufficient in both contexts:
`slotcask_pool_rebuild_worker`, run unconditionally during `slotcask_open`,
sweeps every flag==2 Kf entry into the free pool on next open, and by that
point every retained marker for the object has already been resolved (the
pre-open sweep runs to completion before `slotcask_open` proceeds), so
there is no premature-reuse risk in leaving reclaim to that later scan.
The narrower cost is that a marker resolved by the *live* gate (not the
startup sweep) leaves its OLD capacity unclaimed by the in-memory free pool
until the next process restart, rather than being reusable immediately —
capacity is not lost, just not reused until then. This is called out
explicitly as an accepted, documented tradeoff rather than folded in
silently: closing it fully would require threading a `SlotcaskDb *`
through `kf_shard_marker_gate` → `kf_batch_marker_gate` →
`kf_marker_replay_entry_locked` → `kf_marker_replay_upsert_entry_locked`,
made optional/nullable to keep working from the pre-open sweep — a larger,
separable follow-up, not required to close the corruption bug above.

The two waves and the status fold:

```c
typedef struct { BulkMutationTxn *txn; size_t shard_idx; } BulkStageWork;

static void *bulk_stage_one_shard(void *raw) {
    BulkStageWork *w = raw;
    BulkMutationTxn *txn = w->txn;
    BulkMutationShard *shard = &txn->shards[w->shard_idx];
    SlotcaskBulkRec *recs = shard->recs;
    SlotcaskBulkState *st = shard->st;
    int *stream_counts = NULL;
    int **stream_idx = NULL;

    if (shard->kind == BULK_MUTATION_DELETE) return NULL;
    for (size_t i = 0; i < shard->nrecs; i++) {
        SlotcaskBulkRec *r = &recs[i];
        SlotcaskBulkState *s = &st[i];
        r->status = 0;
        s->needs_write = 0;
        if (r->klen > UINT16_MAX || r->vlen > UINT32_MAX ||
            (size_t)24 + r->klen + r->vlen > (size_t)txn->db->slot_size) {
            r->status = -1;
            continue;
        }
        compute_hash(r->key, r->klen, s->hash);
        s->target_stream = (uint8_t)((unsigned)s->hash[15] %
                                     (unsigned)txn->db->num_streams);
        s->needs_write = 1;
        s->staged_in_wave = 0;
    }
    if (bulk_phase2_bucket_by_stream(txn->db, st, shard->nrecs,
                                     &stream_counts, &stream_idx) != 0)
        goto fail;
    bulk_phase3_seg_writes(txn->db, recs, st, stream_counts, stream_idx);
    free(stream_counts); free(stream_idx);
    stream_counts = NULL; stream_idx = NULL;

    /* P barrier: every surviving flag=0 payload durable — one msync pass +
       one fdatasync per touched file. Records phase3 failed carry
       status=-1 and are excluded. */
    {
        SegLoc *locs = calloc(shard->nrecs, sizeof(*locs));
        size_t n = 0;
        if (!locs) goto fail;
        for (size_t i = 0; i < shard->nrecs; i++) {
            if (recs[i].status != 0 || !st[i].needs_write) continue;
            locs[n].sid = st[i].target_stream;
            locs[n].fid = st[i].target_fid;
            locs[n].off = st[i].target_off;
            n++;
            st[i].staged_in_wave = 1;
        }
        qsort(locs, n, sizeof(*locs), segloc_cmp);
        if (n > 0 && bulk_seg_apply_and_sync(txn->db, locs, n, 0, 0) != 0) {
            free(locs);
            goto fail;
        }
        free(locs);
    }
    return NULL;

fail:
    free(stream_counts); free(stream_idx);
    shard->rc = -1;
    atomic_store_explicit(&txn->cancelled, 1, memory_order_release);
    return NULL;
}

static int bulk_stage_payload_wave(BulkMutationTxn *txn) {
    BulkStageWork *works;
    int any_upsert = 0;

    for (size_t s = 0; s < txn->nshards; s++) {
        BulkMutationShard *shard = &txn->shards[s];
        if (shard->kind != BULK_MUTATION_DELETE) any_upsert = 1;
        shard->st = calloc(shard->nrecs, sizeof(*shard->st));
        if (!shard->st) return -1;
    }
    if (!any_upsert) return 0;           /* deletes have no P phase */
    works = calloc(txn->nshards, sizeof(*works));
    if (!works) return -1;
    for (size_t s = 0; s < txn->nshards; s++) {
        works[s].txn = txn;
        works[s].shard_idx = s;
    }
    parallel_for_io(bulk_stage_one_shard, works, (int)txn->nshards,
                    sizeof(*works));
    free(works);
    return atomic_load_explicit(&txn->cancelled,
                                memory_order_acquire) ? -1 : 0;
}

typedef struct { BulkMutationTxn *txn; BulkMutationShard *shard; }
    BulkShardCommitWork;

static void *bulk_commit_one_shard(void *raw) {
    BulkShardCommitWork *w = raw;

    while (w->shard->cursor < w->shard->nrecs &&
           !atomic_load_explicit(&w->txn->cancelled, memory_order_acquire)) {
        size_t begin = w->shard->cursor;
        size_t end = begin + w->txn->window_cap;
        if (end > w->shard->nrecs) end = w->shard->nrecs;
        int rc = bulk_commit_one_kf_window(w->txn, w->shard, begin, end);
        if (rc != 0) {
            atomic_store_explicit(&w->txn->cancelled, 1,
                                  memory_order_release);
            w->shard->rc = rc;
            return NULL;
        }
        w->shard->cursor = end;
    }
    w->shard->rc = 0;
    return NULL;
}

static int bulk_commit_kf_windows_wave(BulkMutationTxn *txn) {
    BulkShardCommitWork *works = calloc(txn->nshards, sizeof(*works));
    if (!works) return -1;
    for (size_t s = 0; s < txn->nshards; s++) {
        works[s].txn = txn;
        works[s].shard = &txn->shards[s];
    }
    parallel_for_io(bulk_commit_one_shard, works, (int)txn->nshards,
                    sizeof(*works));
    free(works);
    return 0;
}

static int bulk_finish_status(BulkMutationTxn *txn) {
    int pending = 0, failed = 0;

    for (size_t s = 0; s < txn->nshards; s++) {
        if (txn->shards[s].rc == 0) continue;
        failed = 1;
        if (txn->shards[s].rc == -2) pending = 1;
    }
    if (!failed) return 0;
    if (pending) errno = EINPROGRESS;
    return -1;
}
```

Argument shapes for existing internals the executor reconciles literally
at their definitions: `kf_plan_window_insert_slot` (`slotcask.c:2424`),
`kf_commit_planned_slot` (`:2501`), `kf_repoint` (`:3706`),
`seg_write_record_varlen` (`:3908`), and the `splits` value
`index_sync_record_fields` callers pass today. The `st` arrays are freed by
the public adapters when the txn completes; single-record public APIs
construct a one-shard, one-record txn over the same coordinator with
`window_cap=1` and their existing opts, which is what makes every mutation
class share one M/A/I/K/T/C path.

For OLD-derived mutations (`value_compute`, CAS, or a hook requiring OLD),
`bulk_plan_window_locked` reads OLD and derives/revalidates NEW under the Kf
writer.  That deliberately sacrifices P-stage reader progress for those
records; it is required to avoid applying a calculation to stale OLD state.
Fixed-value inserts/upserts stage P before the Kf writer, then revalidate
their target slot under the lock; an obsolete staged record is retired and is
never published.

Index work is batched by `(field,index shard)` within a Kf window.  Multiple
Kf workers may contend on the same secondary-index shard through its existing
lock, but no worker takes another Kf lock.  The window waits for index apply
and exactly one sync of every touched B-tree, trigram, and bitmap file before
K. `sync_after=1` is retained only for a single-record window; bulk index
callbacks record touched file identities and defer sync to this barrier. A/T
group flag writes by segment file, coalesce their dirty page ranges, and call
fdatasync once per file. K coalesces duplicate/adjacent Kf slot pages before
msync and separately syncs the Kf header once when it changed. Do not launch
nested `parallel_for_io` from an I/O worker.

Every public mutation adapter preserves its existing successful return value.
For a post-M window that cannot synchronously replay through C, it returns
failure with `errno=EINPROGRESS`; all command/wire callers in `storage.c` and
`query_bulk.c` map that errno to exactly
`{"error":"durability outcome pending; do not retry blindly"}` rather than
their ordinary retryable error. The same mapping is documented in the query
protocol error reference and regression-tested for single and bulk mutations.

## Task 5 — configuration propagation and deletion-only cleanup

**Anchors:** `struct ShardDb {` in `src/db/shard_db_internal.h`,
`typedef struct SlotcaskDb {` in `src/db/slotcask.h`, the `DURABILITY_SYNC_MS=`
parse branch in `src/db/config.c`, `slotcask_open(`,
`slotcask_registry_get(`, and `#define BULK_COMMIT_MAX_RECORDS 256`.

**Test first for this task.** Add Task 1's configuration parsing and
17-record/two-window tests before adding the knob or deleting the old paths.

Add `int bulk_commit_window;` to the database configuration and Slotcask
instance, default it to 1024, parse `BULK_COMMIT_WINDOW=`, and copy it into
every newly opened registry instance.  The parse branch is complete:

```c
static int parse_bulk_commit_window(const char *text, int *out) {
    char *end = NULL;
    long value;

    errno = 0;
    value = strtol(text, &end, 10);
    while (end && (*end == ' ' || *end == '\t' || *end == '\r' || *end == '\n'))
        end++;
    if (errno == ERANGE || end == text || !end || *end != '\0' ||
        value < 16 || value > 16384)
        return -1;
    *out = (int)value;
    return 0;
}
```

**Bug found and fixed during Task 3's follow-on work (execution, not
scoped in the original plan): off-by-one in the `db.env` dispatch
branch.** The branch that calls into the parser above was written as
`strncmp(p, "BULK_COMMIT_WINDOW=", 20) == 0`. The literal
`"BULK_COMMIT_WINDOW="` is 19 bytes, not 20 — `strncmp` with `n=20`
therefore also compares the literal's NUL terminator (byte 20) against
the first character of the actual value (e.g. `'1'` of `16`), which can
never match. The branch was consequently unreachable for every input,
silently no-op'ing: `BULK_COMMIT_WINDOW=` in `db.env` was parsed by
nothing and `bulk_commit_window` stayed at its 1024 default regardless
of what the file said. Root-caused via `test-bulk-commit-window-config`
(the regression test this same task calls for) failing with "valid
window value applied: expected 16/1024/16384/512 got 777" — the parse
itself succeeded (`load_db_root` returned 0) but the value was never
applied, which pointed at the dispatch match rather than
`parse_bulk_commit_window`. Fixed by correcting `n` to `19` (and `v = p
+ 19`, not `p + 20`) so the byte offset lines up with the true prefix
length. This is a plain string-length bug, not a design issue — no
alternative behavior was intended.

Delete, after Task 4 is green, rather than leaving disabled branches:

1. `#define BULK_COMMIT_MAX_RECORDS 256` and every fixed-window stack array.
2. `bulk_upsert_fast_in_kfshard`, its declaration, and its fast/no-marker
   body.
3. The old body of `slotcask_bulk_update`; it becomes a bucketing adapter to
   `BulkMutationTxn` and is no longer test-only dead code.
4. Legacy `if (1)` / `apply_window` fallback scaffolding, any duplicate
   post-window tombstone sweep, and every single-record path that bypasses
   the transaction coordinator.
5. Inline `O_TRUNC` marker writers and stale comments saying markers apply
   only to indexed mutations, Kf headers need no durable sync, or raw segment
   scans are query truth.
6. Every `KfAbort*`, `kf_abort_*`, `kf_marker_abort_*`,
   `kf_batch_marker_abort_*`, inverse-index callback, and abort-sidecar scan
   branch. Replace recovery directory handling with recognised final markers
   plus harmless unpublished temporary marker cleanup only.

   **Execution note (confirmed during Task 5 cleanup):** `kfm2_namespace_gate`
   (a fail-closed startup check guarding against upgrading over leftover
   abort-sidecar-shaped debris from a prior release) was found implemented
   and tested but never wired into production, and was deleted rather than
   wired in. It is **not required**: its own write-path dependency
   (`kf_marker_write`, the single-record marker writer) has zero production
   callers post-Task-4 — every mutation, single or bulk, now writes KFM2
   batch markers via `bulk_publish_window_marker_locked`. Abort sidecars
   *were* real, shipped, on-disk artifacts in the 2026.08.1 release (the
   `KfAbortHeader` format existed and was written in production); this
   branch removes that mechanism going forward and does not need to
   interpret legacy abort-sidecar files, because upgrading onto this
   release is defined to require the data directory already be in a
   clean, recovery-complete state (confirmed as intended product
   behavior) — not because the format never existed historically. The
   already-rewritten `marker_recovery_sweep_object` forward-replays or
   fails closed on any marker shape it can parse, which is sufficient on
   its own given that upgrade precondition. Also deleted
   as dead code in the same pass: `kfm2_publish_batch_marker`,
   `kfm2_batch_marker_validate`, `kf_batch_marker_write`/`_impl` (all zero
   callers).
7. The inline bulk marker loops that call `fsync(fd)` after each `pwrite`, and
   the per-record bulk index sync path. The sole marker helper writes the
   complete checked marker array, fsyncs it once, atomically publishes it, and
   fsyncs its directory once. Keep only the file/page-batched sync helpers.
8. `DURABILITY_SYNC_MS` and its background thread, fully superseded by
   synchronous per-window durability: the `DURABILITY_SYNC_MS=` parse branch
   in `config.c`, the `durability_sync_ms` field and `g_durability_sync_ms`
   macro in `shard_db_internal.h`, `durability_sync_thread` in
   `src/db/durability.c` and its `server.c` launch condition
   (`if (db->durability_sync_ms > 0)`), the `embedded.c` default, the
   `export DURABILITY_SYNC_MS=1000` line in `db.env`, and its row in
   `docs/getting-started/configuration.md`. Where `durability_mark_dirty()`
   calls existed only to feed that thread, remove them with it;
   `durability_msync_range()` stays where the window barriers use it.
   Retire `src/test/cases/test_durability_sync.c` (it asserts background
   thread tick logs), delete the `DURABILITY_SYNC_MS` parse cases from
   `test_durability_sync_failures.c` (the file stays; its sync-failure
   injection cases are re-targeted at the window barriers in Task 1), and
   update the `db.env` fixture in `test_embedded_bg_threads.c`.

The task ends with searches proving zero matches for
`BULK_COMMIT_MAX_RECORDS`, `bulk_upsert_fast_in_kfshard`,
`DURABILITY_SYNC_MS` / `durability_sync_ms`, and durability
`if (1)` branches; it also proves exactly one production coordinator invokes
M/A/I/K/T/C.

Update `db.env` (add `BULK_COMMIT_WINDOW`, remove `DURABILITY_SYNC_MS`),
`docs/getting-started/configuration.md` (new `BULK_COMMIT_WINDOW` row,
removed `DURABILITY_SYNC_MS` row), `docs/concepts/storage-model.md`,
`docs/concepts/concurrency.md`, `docs/reference/error-codes.md` (the
`EINPROGRESS` → `"durability outcome pending; do not retry blindly"`
mapping for single and bulk mutations), `docs/reference/changelog.md`
(marker format KFM1 → KFM2, per-window synchronous durability, the
clean-shutdown upgrade requirement and fail-closed gate, the
`BULK_COMMIT_WINDOW` knob, and `DURABILITY_SYNC_MS` removal), and
`AGENTS.md` (rewrite the
"Indexed-write crash safety" storage-model paragraph for the
unconditional-marker, forward-replay-only, no-abort-sidecar window protocol)
with the knob, per-Kf-window semantics, lock order, and the fact that a
multi-shard bulk request is not globally atomic.

## Execution rules and verification

Execute tasks in order on this feature branch and leave all changes
uncommitted for raw-diff review.  If any quoted anchor above is absent,
create `PLAN_NOTES.md` explaining the mismatch and halt the entire execution
run; do not reinterpret the plan or continue without a new human-approved
plan.  If an uncovered design decision appears, stop and ask the human.
Anchor preflights search for each quoted fragment verbatim, as a substring,
exactly as written — never normalize quoted text (for example, appending a
`*/` terminator to a comment fragment) before declaring an anchor missing.

Build and test commands, with real output captured:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-durability-ordering
./build/bin/shard-db-test run test-durability-sync-failures
./build/bin/shard-db-test run-all
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all
```

Do not run benchmarks.  Before handoff, inspect the raw diff and perform the
required independent concurrency/on-disk-format review.
