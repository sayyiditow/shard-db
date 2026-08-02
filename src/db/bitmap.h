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
   Caller must ensure no thread holds the entry's rwlock. */
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

/* Synchronously fdatasync the bitmap file. `bm` must be opened with
   writer=1. Used by the marker-governed CRUD path. */
int bm_sync(BitmapShard *bm);

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

/* Observability — used by tests + describe-object debugging paths. */
uint32_t bm_n_values(const BitmapShard *bm);
uint32_t bm_slots(const BitmapShard *bm);
uint32_t bm_stride(const BitmapShard *bm);

#ifdef __cplusplus
}
#endif

#endif /* BITMAP_H */
