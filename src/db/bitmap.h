#ifndef BITMAP_H
#define BITMAP_H
/* bitmap.h — per-shard bitmap index.
 *
 * One file per (object, field, data-shard) tuple at
 *   <db_root>/<dir>/<object>/indexes/<field>/NNN.bm
 *
 * Shard alignment is 1:1 with data shards — bit position N within a
 * bitmap directly corresponds to slot N in the same-numbered data shard.
 * This avoids any kind of index_splits_for() routing for bitmap reads;
 * to evaluate `field eq value` we walk the bitmap and emit each set
 * slot's hash via slot lookup.
 *
 * On-disk layout (little-endian throughout):
 *
 *     File header (32 bytes):
 *       0  : magic       u32  'BM01' ASCII LE
 *       4  : version     u16  1
 *       6  : flags       u16  bit 0 = bool fast-path
 *       8  : slots       u32  data shard's slots_per_shard matches the data shard's slots_per_shard
 *       12 : n_values    u32  number of distinct values in dictionary
 *       16 : dict_off    u32  byte offset of dictionary
 *       20 : bitmaps_off u32  byte offset of bitmap area
 *       24 : stride      u32  bytes per single-value bitmap = (slots+7)/8
 *       28 : reserved    u32  zero
 *
 *     Dictionary @ dict_off (variable):
 *       repeat n_values times:
 *         u16 value_len
 *         u8  value[value_len]
 *
 *     Bitmaps @ bitmaps_off (n_values * stride bytes):
 *       N contiguous dense bitmaps. Bit i of bitmap k corresponds to
 *       "slot i has field value `dict[k]`".
 *
 * The bool fast-path (flags bit 0) hardcodes n_values=2, dict[0]=0x00
 * (false), dict[1]=0x01 (true). Lookups skip the dictionary scan.
 *
 * Spec: [[index-types-roadmap]] / [[bitmap-impl-map]].
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

/* Opaque shard handle. Wraps a writable mmap of the file. Thread-safe
   for concurrent readers; writers must hold a per-shard rwlock that the
   caller (slotcask) owns. */
typedef struct BitmapShard BitmapShard;

typedef enum {
    BM_PUBLISH_OK = 0,
    BM_PUBLISH_PRE_RENAME_FAILED,
    BM_PUBLISH_POST_RENAME_FSYNC_FAILED,
} bm_publish_result;

/* Flags */
#define BM_FLAG_BOOL_FASTPATH  0x0001u

/* Distinct-value limits.
 *
 * Each bitmap file declares its own `max_values` in its header. The
 * value is set at create-object via `field:bitmap` (default
 * BM_DEFAULT_MAX_VALUES) or `field:bitmap(N)` (operator override). If
 * a user's data has more than `max_values` distinct values, bm_set
 * returns -1 and the wire layer surfaces an actionable error pointing
 * the operator at btree.
 *
 * Storage is dynamic regardless of the cap: a bitmap file only holds
 * `n_values` bitmaps at any given moment, where `n_values` is the
 * actual number of distinct values seen so far. The cap is an upper
 * bound, not a pre-allocation reservation.
 *
 * The hard ceiling (BM_HARD_CEILING) is the fixed maximum for any
 * single bitmap: 65535. Past that the dict's uint16 length-prefix and
 * value-index encoding would need to widen, which would be a format
 * change. Bool fast-path is unaffected — it always has exactly 2
 * values. */
#define BM_DEFAULT_MAX_VALUES 256u
#define BM_HARD_CEILING       65535u

/* Legacy alias kept for any callers still wired to the previous name.
   Resolves to the default cap. */
#define BM_MAX_VALUES BM_DEFAULT_MAX_VALUES

/* Build the canonical bitmap shard path for a (object, field, NNN) tuple.
   `db_root` is the effective root (i.e. `$DB_ROOT/<dir>`). Mirrors
   build_idx_path() in storage.c for consistency. */
void bm_build_path(char *out, size_t outlen,
                   const char *db_root, const char *object,
                   const char *field, int shard_idx);

/* Global bitmap shard cache (mirrors bt_cache / kfcache). Path-keyed
   slot table with per-entry rwlock and LRU eviction. Initialise once
   at daemon startup with the desired capacity (number of cached .bm
   files); leave uninitialised in test fixtures to fall back to fresh
   mmap-per-call. */
void bm_cache_init(int cap);
void bm_cache_shutdown(void);

/* Drop the cache entry for `path` (e.g. after a reindex unlinks it).
   The blocking form returns -1 with the entry acquisition's exact errno when
   the lock acquisition fails (e.g. EDEADLK when the calling thread holds the
   entry's write lock); ordinary teardown errors propagate the same way.
   Caller must ensure no OTHER thread holds the entry's rwlock. The void form
   deliberately discards the result. */
int bm_cache_invalidate_checked(const char *path);
void bm_cache_invalidate(const char *path);

/* Publish a complete, closed temporary bitmap at target. */
bm_publish_result bm_publish_replace(const char *target, const char *tmp_path);

/* Open the bitmap shard file. If it doesn't exist:
     - If `create == 0`, returns NULL.
     - If `create == 1` and `bool_fastpath == 1`, creates a bool-flavoured
       file pre-initialised with the two values 0x00 and 0x01.
     - If `create == 1` and `bool_fastpath == 0`, creates an empty
       dictionary (n_values=0). Callers add values via bm_set on demand.
   `slots` must match the data shard's current slots_per_shard.
   `max_values` is the per-file cap from the create-object declaration:
   pass 0 to use BM_DEFAULT_MAX_VALUES; pass any value in
   [2, BM_HARD_CEILING] to override. When opening an existing file
   (create == 0), the `max_values` arg is ignored — the value baked
   into the header wins. The file is mmap'd MAP_SHARED, writable. */
/* `writer`: 0 takes an rdlock on the cache slot (multiple concurrent
   readers allowed), 1 takes a wrlock (exclusive). Writers must use 1
   so bm_set / bm_clear / bm_grow are serialised. */
/* Failure: NULL with errno set. Lock-acquisition errors surface the exact
   pthread result (e.g. EDEADLK when a writer open's selected eviction
   candidate is write-held by this thread); ordinary contention on another
   thread's candidate still waits and succeeds on release. */
BitmapShard *bm_open(const char *path, int slots, int create,
                     int bool_fastpath, uint32_t max_values, int writer);

/* Read the per-file cap baked into the header. Returns the resolved
   value (never 0 — defaults to BM_DEFAULT_MAX_VALUES). */
uint32_t bm_max_values(const BitmapShard *bm);

/* Close + unmap. Checked form returns the first release failure; the
   compatibility wrapper intentionally discards it. Both are safe on NULL. */
int bm_close_checked(BitmapShard *bm);
void bm_close(BitmapShard *bm);

#ifdef TEST_BUILD
void bm_test_fail_close_next(int count);
void bm_test_fail_invalidate_next(int count);
void bm_test_fail_reset(void);
#endif

/* Bitmap-cache seam site identifiers: the four bm_open_impl acquisition
 * sites (hit/fill × reader/writer) and the two bm_cache_drop_slot modes
 * (nonblocking try / selected blocking wait). Plain identifiers in all
 * builds; the seam functions below are TEST_BUILD-only. */
typedef enum {
    BM_TEST_LOCK_HIT_RD = 0,
    BM_TEST_LOCK_HIT_WR,
    BM_TEST_LOCK_FILL_RD,
    BM_TEST_LOCK_FILL_WR,
    BM_TEST_LOCK_DROP_TRY,
    BM_TEST_LOCK_DROP_WAIT,
    BM_TEST_LOCK_SITE_COUNT
} BmTestLockSite;

#ifdef TEST_BUILD
/* TEST_BUILD test seams: observation + one-shot fault injection for the
 * bitmap cache's lock acquisition sites. Production builds contain none of
 * this. */

/* Arm one thread-local, per-site one-shot fault: the next acquisition at
 * `site` on THIS thread returns `error` without calling pthread. Arming 0
 * or an out-of-range site is a no-op; never synthesizes success. */
void bm_test_fail_lock_next(int site, int error);

/* How many one-shot faults this thread has consumed at `site`. */
int  bm_test_lock_failure_consumed(int site);

/* Zero this thread's armed faults and consumption counts. */
void bm_test_reset_lock_failure(void);

/* Process-wide count of acquisition attempts entered at `site` (published
 * immediately before the pthread call, so a contended waiter is visible to
 * the coordinating test thread before completion). Pure observation. */
int  bm_test_lock_entered(int site);

/* Thread-local count of cache-entry rwlock unlocks performed by this thread
 * through bitmap.c (matched handoffs, evictions, closes). Scoped measurement
 * for "a failed acquisition performed no unlock" assertions. */
int  bm_test_entry_unlock_count(void);

/* Startup-only bitmap cache capacity override for tests: 0 resets to the
 * configured default, 16 forces the 16-entry/32-slot regression geometry.
 * Any other value, or a call while a cache is initialized, is rejected
 * (-1 with errno EINVAL / EBUSY). Not a supported configuration knob. */
int  bm_test_set_cache_cap(int cap);
#endif

/* Synchronously fdatasync the bitmap file. `bm` must be opened with
   writer=1. Used by the marker-governed CRUD path. */
int bm_sync(BitmapShard *bm);

#ifdef TEST_BUILD
void bm_test_sync_reset(void);
int  bm_test_sync_count(void);
#endif

/* Non-mutating cap preflight: would inserting `value` (if it isn't already
   in the dict) push n_values past max_values? `bm` must be a writer handle
   (same-thread ownership requirement mirrors bm_set/bm_clear). Returns 1 if
   it would exceed the cap, 0 if it's already present or there's room, -1 on
   error (e.g. value too long for the dict's uint16 length prefix). Reading
   the header + walking the dict does no I/O beyond the existing mmap. */
int  bm_dict_would_exceed_cap(BitmapShard *bm, const uint8_t *value, size_t vlen);

/* Non-mutating membership check: is `value` already in the dict? Used by
   window-scoped cap accounting, which must distinguish "already on disk"
   from "new distinct value" separately from the cap arithmetic. */
int  bm_dict_contains(BitmapShard *bm, const uint8_t *value, size_t vlen);

/* Set / clear / test a single bit for a (value, slot) pair. On first
   set of a new value, the dictionary grows and the file is rewritten —
   so set is more expensive on the very first write of a given value.
   Returns 0 on success, -1 on failure. */
int  bm_set(BitmapShard *bm, const uint8_t *value, size_t vlen, uint32_t slot);
int  bm_clear(BitmapShard *bm, const uint8_t *value, size_t vlen, uint32_t slot);
int  bm_test(const BitmapShard *bm, const uint8_t *value, size_t vlen, uint32_t slot);

/* Walk every set bit for `value`, calling `cb(slot, ctx)` per match.
   `cb` returns 0 to continue, non-zero to stop. Returns the number of
   slots visited. */
int  bm_walk(const BitmapShard *bm, const uint8_t *value, size_t vlen,
             int (*cb)(uint32_t slot, void *ctx), void *ctx);

/* Iterate every (value, vlen) pair in the bitmap's dictionary.
   `cb` returns 0 to continue, non-zero to stop. Used by the planner's
   generic "any criterion → walk dict, evaluate per value, union matching
   bitmaps" path so operators that don't have a direct popcount fast
   path (LIKE / CONTAINS / range / regex / len_*) still route through
   the bitmap instead of falling back to a full data-shard scan.
   Returns the number of dict entries visited. */
int  bm_iter_values(const BitmapShard *bm,
                    int (*cb)(const uint8_t *value, size_t vlen, void *ctx),
                    void *ctx);

/* Population count for a value's bitmap — used by the query planner
   for selectivity-aware AND ordering. O(stride / 8). */
uint32_t bm_count(const BitmapShard *bm, const uint8_t *value, size_t vlen);

/* Return pointer to the bit-array for `value` in `bm`, or NULL if not
 * found.  Sets *out_stride to the bitmap stride (bytes per value).  The
 * returned pointer is valid while the bitmap remains open.
 * Used by word-level AND intersect popcount — avoids exposing dict-lookup
 * internals to callers outside bitmap.c. */
const uint8_t *bm_get_value_bitmap(BitmapShard *bm, const uint8_t *value,
                                    size_t vlen, uint32_t *out_stride);

/* Grow the bitmap shard's stride to match a new `slots` value (called
   when the data shard doubles its slots_per_shard). Extends every
   bitmap with zero bytes. Returns 0 on success. */
int  bm_grow(BitmapShard *bm, uint32_t new_slots);

/* Keyfile-rebuild support. The keyfile resplit / compaction renumbers kf slots;
   bit i of a bitmap means "kf slot i", so the bits must move with the entries.
   Three steps, all on one writer handle:
     bm_slotmap_prepare - write a copy of `bm`'s file to a sibling temporary
        (`<path>.remap`, name returned in tmp_path) in which every set bit `s`
        is moved to `map_fn(s, ctx)` and the slot space is resized to
        `new_slots`. map_fn returns UINT32_MAX to drop a bit (dead slot); a
        return >= new_slots is corruption (errno EUCLEAN). The live file is not
        modified; the temporary is fsynced.
     bm_slotmap_stage   - make the live file and mapping large enough for the
        new image (blocks are reserved with posix_fallocate) and map the
        temporary read-only. Everything that can fail runs here, before the
        caller's point of no return; the old content stays valid and the old
        header still describes it.
     bm_slotmap_apply   - copy the staged image over the live mapping in place
        and msync it. No allocation, no rename, no reopen: the copy cannot
        fail. The return value only reports whether the msync confirmed
        durability; the in-memory (and page-cache) content is correct either way.
   Release the stage and discard the temporary afterwards. */
typedef struct {
    const void *src;
    size_t      len;
} BmSlotmapStage;

int  bm_slotmap_prepare(BitmapShard *bm, uint32_t new_slots,
                        uint32_t (*map_fn)(uint32_t old_slot, void *ctx), void *ctx,
                        char *tmp_path, size_t tmp_len);
int  bm_slotmap_stage(BitmapShard *bm, const char *tmp_path, BmSlotmapStage *st);
int  bm_slotmap_apply(BitmapShard *bm, const BmSlotmapStage *st);
void bm_slotmap_stage_release(BmSlotmapStage *st);
/* Remove an unpublished temporary (safe on "" / NULL). */
void bm_slotmap_discard(const char *tmp_path);

/* Open up to BM_OPEN_MANY_MAX bitmap shards for reading in canonical lock
   order (ascending path, which is ascending field name within one object and
   shard) and return the handles in the caller's order. Every code path that
   holds more than one bitmap shard lock at once acquires them in this order
   (the keyfile-rebuild remap does too), so two such paths can never wait on
   each other. Returns 0, or -1 with every handle closed and out[] cleared. */
/* Canonical lock order for two field names of one object and shard: the order
   bm_open_many_ro uses (compares the paths "<field>/NNN.bm"). */
int  bm_field_lock_cmp(const char *a, const char *b);
#define BM_OPEN_MANY_MAX 16
int  bm_open_many_ro(const char *const *paths, int n, BitmapShard **out);
/* Close handles opened by bm_open_many_ro: repeated paths share one handle (a
   second read lock on the same non-recursive lock could deadlock behind a
   queued writer), so each distinct handle is closed once. Clears out[]. */
void bm_close_many(BitmapShard **h, int n);
/* 1 if `bm` holds its cache entry's rwlock (a writer handle without it cannot
   be used for an in-place rewrite). */
int  bm_handle_cached(const BitmapShard *bm);
/* 1 if `path` exists but its header does not describe a file of that size
   (bm_open refuses such a file with EUCLEAN); 0 otherwise. */
int  bm_file_is_corrupt(const char *path);

/* Observability — used by tests + describe-object debugging paths. */
uint32_t bm_n_values(const BitmapShard *bm);
uint32_t bm_slots(const BitmapShard *bm);
uint32_t bm_stride(const BitmapShard *bm);

#ifdef __cplusplus
}
#endif

#endif /* BITMAP_H */
