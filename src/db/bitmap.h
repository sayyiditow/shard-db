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
 *       8  : slots       u32  data shard's slots_per_shard (matches ShardHeader)
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

/* Flags */
#define BM_FLAG_BOOL_FASTPATH  0x0001u

/* Maximum distinct values a single bitmap index may hold. Bitmap is the
   right tool for bool + low-cardinality enums; if a user's data has
   more than this many distinct values, btree is the correct index
   choice (shard-db's planner uses indexes when available regardless of
   selectivity, so a btree of any size beats a bitmap that's outgrown
   its design point). bm_set returns -1 when this cap is exceeded and
   the wire layer surfaces an actionable error pointing the operator
   at btree. Bool fast-path is unaffected — it always has exactly 2
   values. */
#define BM_MAX_VALUES 256

/* Build the canonical bitmap shard path for a (object, field, NNN) tuple.
   `db_root` is the effective root (i.e. `$DB_ROOT/<dir>`). Mirrors
   build_idx_path() in storage.c for consistency. */
void bm_build_path(char *out, size_t outlen,
                   const char *db_root, const char *object,
                   const char *field, int shard_idx);

/* Open the bitmap shard file. If it doesn't exist:
     - If `create == 0`, returns NULL.
     - If `create == 1` and `bool_fastpath == 1`, creates a bool-flavoured
       file pre-initialised with the two values 0x00 and 0x01.
     - If `create == 1` and `bool_fastpath == 0`, creates an empty
       dictionary (n_values=0). Callers add values via bm_set on demand.
   `slots` must match the data shard's current slots_per_shard. The file
   is mmap'd MAP_SHARED, writable. */
BitmapShard *bm_open(const char *path, int slots, int create, int bool_fastpath);

/* Close + unmap. Safe on NULL. */
void bm_close(BitmapShard *bm);

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

/* Population count for a value's bitmap — used by the query planner
   for selectivity-aware AND ordering. O(stride / 8). */
uint32_t bm_count(const BitmapShard *bm, const uint8_t *value, size_t vlen);

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
