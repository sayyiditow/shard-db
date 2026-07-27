/*
 * B+ Tree Index for shard-db
 *
 * Single mmap'd file per indexed field.
 * Entry: value_bytes (variable) + key_hash (16 raw bytes)
 * Pages: 4096 bytes, slotted page layout
 * O(log n) insert/delete/search, O(log n + k) range scan
 *
 * Usage:
 *   btree_insert(path, value, vlen, hash16)
 *   btree_delete(path, value, vlen, hash16)
 *   btree_search(path, value, vlen, callback, ctx)
 *   btree_range(path, min, minlen, max, maxlen, callback, ctx)
 *   btree_bulk_build(path, entries, count)  // sorted array
 */

#ifndef BTREE_H
#define BTREE_H

#include <stdint.h>
#include <stddef.h>

#define BT_HASH_SIZE    16      /* raw xxh128 bytes */
#define BT_MAX_VAL_LEN  512    /* max value length */

/* File magic. "BTRH" (current, 2026.05.5+) = (value, hash) leaf sort +
   internal-page separators include hash, so single-key delete lands
   directly on the target leaf in O(log N) instead of walking the
   value-cluster chain. Previous "BTRG" (v3) added last_leaf_page +
   prev_leaf for O(1)-step DESC iteration; the v2 "BTRF" and v1 "BTRE"
   magics predate that. All non-current magics are rejected on open —
   run ./migrate to rebuild every btree. */
#define BT_MAGIC_V1  0x42545245u  /* legacy: string-keyed */
#define BT_MAGIC_V2  0x42545246u  /* legacy: binary keys without prev_leaf */
#define BT_MAGIC_V3  0x42545247u  /* legacy: BTRG, prev_leaf + last_leaf_page */
#define BT_MAGIC     0x42545248u  /* current: BTRH — (value, hash) sort */

/* Configurable page size — set before first use */
extern int bt_page_size; /* default 4096, set from db.env INDEX_PAGE_SIZE */

/* File header (page 0). Fixed-name fields total 56 bytes; the remainder
   of the 4 KB page is `reserved` and zero-initialised so future fields
   can be added without bumping BT_MAGIC unless the meaning of existing
   ones changes. */
typedef struct __attribute__((packed)) {
    uint32_t magic;         /* BT_MAGIC */
    uint32_t root_page;     /* page id of root */
    uint32_t page_count;    /* total pages allocated */
    uint32_t height;        /* tree height (1 = root is leaf) */
    uint64_t entry_count;   /* total entries (lifetime inserts - lifetime deletes) */
    uint8_t  key_type;      /* FT_* of indexed field (FT_VARCHAR for composite) */
    uint8_t  key_signed;    /* 1 if sign-flip applied at encode, 0 for raw-bytes */
    uint8_t  _pad[2];
    uint32_t last_leaf_page;/* rightmost leaf in chain — DESC iter starts here.
                               Maintained on split (when the rightmost leaf
                               splits, the new right half becomes the last). */
    /* Per-shard lifetime counters (added 2026.05.5). Used by shard-stats
       and as a "reindex needed?" heuristic. Updated under the bt rwlock
       on every mutate; no read-path cost. */
    uint64_t insert_count;   /* total btree_insert calls that landed an entry */
    uint64_t delete_count;   /* total btree_delete calls that removed an entry */
    uint64_t tombstone_count;/* slots currently in tomb state (recovered on
                                next leaf rebuild / compaction) */
    uint8_t  reserved[4096 - 56]; /* header always occupies first page */
} BtFileHeader;

/* Page header (first 24 bytes of every page) */
typedef struct __attribute__((packed)) {
    uint32_t page_type;     /* 0=internal, 1=leaf */
    uint32_t count;         /* number of entries */
    uint32_t next_leaf;     /* next leaf page id (0=none) / leftmost child for internal */
    uint32_t prev_leaf;     /* previous leaf page id (0=none); 0 for internal nodes */
    uint32_t data_end;      /* byte offset where entry data ends (grows from end of page) */
    /* Bytes orphaned in the entry data region after in-place leaf inserts
       re-encode entries at new offsets and leave the old offsets unreachable
       from the slot directory. `page_free_space + dead_bytes` is the total
       reclaimable space; compaction (page_leaf_compact) walks the slot
       directory and rewrites live entries tightly, resetting this to 0.
       Pre-2026.05.5 pages have this field zero-initialised, which the new
       code treats as "no dead bytes yet" — correct for any page produced
       by the old full-rebuild algorithm. */
    uint32_t dead_bytes;
} BtPageHeader;

/* Within a page after the header:
 *   Slot directory: uint16_t offsets[count]  (offset into page where each entry starts)
 *   ...free space...
 *   Entry data (packed from end of page toward the slots)
 *
 * Leaf page entry (prefix-compressed):
 *   [uint16_t data_len] [uint8_t prefix_len] [suffix_bytes] [hash: 16 bytes]
 *   data_len = 1 + suffix_len + BT_HASH_SIZE
 *   full_value = prev_entry_value[0:prefix_len] + suffix_bytes
 *   Every BT_LEAF_RESTART_K-th slot (0, K, 2K...) is an anchor: prefix_len = 0,
 *   suffix holds the full value. Decode is sequential from the nearest anchor.
 *
 * Internal page entry (flat):
 *   [uint16_t data_len] [value_bytes] [child_page_id: 4 bytes]
 *   data_len = vlen + 4
 */

#define BT_LEAF_RESTART_K 16

#define BT_PAGE_DATA_START (sizeof(BtPageHeader))

/* Insert results for marker-replay idempotence */
#define BT_INSERT_NO_SPLIT   (-1)
#define BT_INSERT_SPLIT       0
#define BT_INSERT_DUPLICATE   1

/* Callback for search/range results: return 0 to continue, -1 to stop early */
typedef int (*bt_result_cb)(const char *value, size_t vlen,
                            const uint8_t *hash16, void *ctx);

/* --- Public API --- */

/* Insert a value + 16-byte hash into the index. Creates file if needed. */
int btree_insert(const char *path, const char *value, size_t vlen,
                 const uint8_t hash[BT_HASH_SIZE]);

/* Delete entry matching value + hash. */
int btree_delete(const char *path, const char *value, size_t vlen,
                 const uint8_t hash[BT_HASH_SIZE]);

/* Synchronously fdatasync the btree file at `path`. Acquires the writer
   lock for that path, calls fdatasync on its fd, releases. Used by the
   marker-governed CRUD path to make index mutations durable before the
   marker clear. Not used by index builds/reindex. */
int btree_sync_path(const char *path);

/* Search for all entries matching value exactly. Calls cb for each. */
void btree_search(const char *path, const char *value, size_t vlen,
                  bt_result_cb cb, void *ctx);

/* Range scan: all entries within [min, max].
   min_exclusive/max_exclusive: if true, exclude the boundary value. */
void btree_range(const char *path,
                 const char *min_val, size_t min_len,
                 const char *max_val, size_t max_len,
                 bt_result_cb cb, void *ctx);
void btree_range_ex(const char *path,
                    const char *min_val, size_t min_len, int min_exclusive,
                    const char *max_val, size_t max_len, int max_exclusive,
                    bt_result_cb cb, void *ctx);

/* Same as btree_range_ex but emits entries in DESCENDING order. Used by
   find-cursor's DESC pagination. Implementation walks the leaf chain
   forward once to collect page IDs (no prev_leaf pointer in our header),
   then iterates leaves right-to-left; within each leaf, entries are
   decoded forward into a local array and played back in reverse. */
void btree_range_desc_ex(const char *path,
                         const char *min_val, size_t min_len, int min_exclusive,
                         const char *max_val, size_t max_len, int max_exclusive,
                         bt_result_cb cb, void *ctx);

/* Bulk build from sorted array. Destroys existing index. */
typedef struct {
    const char *value;
    size_t vlen;
    uint8_t hash[BT_HASH_SIZE];
} BtEntry;

int btree_bulk_build(const char *path, BtEntry *entries, size_t count);
int btree_insert_batch(const char *path, BtEntry *entries, size_t count);
/* Sort new_entries, merge with existing tree contents, rebuild — O(N log N) sequential.
   Use for bulk insert operations instead of btree_insert_batch. */
int btree_bulk_merge(const char *path, BtEntry *new_entries, size_t new_count);

/* Streaming bulk build — same output as btree_bulk_build but accepts entries
   one-at-a-time so the caller never materialises the full sorted input array
   in memory. Used by the streaming external-merge-sort index pipeline: k-way
   merge of per-worker spill files feeds directly into bt_stream_build_add,
   so per-output-shard memory stays O(leaf_buffer + leaf_id_list) — a few MB
   — regardless of how many entries pass through.

   Caller must call entries in ASCENDING sorted order (by value, then hash).
   Out-of-order adds corrupt the on-disk tree silently. */
typedef struct BtStreamBuilder BtStreamBuilder;
BtStreamBuilder *bt_stream_build_open(const char *path);
int  bt_stream_build_add(BtStreamBuilder *b, const char *value, size_t vlen,
                         const uint8_t hash[BT_HASH_SIZE]);
int  bt_stream_build_finish(BtStreamBuilder *b);

/* Streaming range iterator. Pulls one entry at a time so callers can drive a
   k-way merge across multiple btree files without buffering everything per
   file. desc=1 walks DESC, otherwise ASC. Bounds match btree_range_ex /
   btree_range_desc_ex semantics (NULL bounds disallowed; pass "" / 0 + the
   sentinel "\xff\xff\xff\xff" for open ends, same as those callers).

   Holds the underlying btree's rdlock for the iterator's lifetime — callers
   must call btree_range_iter_close to release. Returned (value, vlen, hash)
   pointers are valid until the next btree_range_iter_next call on the same
   iterator. */
typedef struct BtRangeIter BtRangeIter;

BtRangeIter *btree_range_iter_open(const char *path,
                                   const char *min_val, size_t min_len, int min_exclusive,
                                   const char *max_val, size_t max_len, int max_exclusive,
                                   int desc);
int  btree_range_iter_next(BtRangeIter *it,
                           const char **value, size_t *vlen,
                           const uint8_t **hash16);
void btree_range_iter_close(BtRangeIter *it);

/* Tight full-leaf walker for single-pass aggregates (SUM/AVG/COUNT) that
   don't need the 16-byte per-entry hash, range bounds, or DESC ordering.
   Walks the leaf chain forward from the leftmost leaf, decoding each
   prefix-compressed key inline into a stack buffer and invoking cb with
   the full key bytes only. Tombstoned entries are skipped silently.
   Returns cb's last return value (0 success / non-zero stop). The btree
   read-lock is held for the entire walk; do not call other btree APIs on
   the same file from cb. Cuts agg leaf-walk CPU by ~3× vs BtRangeIter
   (no hash memcpy, no per-entry val_cmp, no yield buffer copy). */
typedef int (*bt_value_only_cb)(const char *value, size_t vlen, void *ctx);
int btree_walk_all_values(const char *path, bt_value_only_cb cb, void *ctx);

/* Read-only cache of mmap'd B+ tree files. Capacity from db.env BT_CACHE_MAX. */
void bt_cache_init(int cap);
void bt_cache_shutdown(void);
void btree_cache_invalidate(const char *path);

#endif /* BTREE_H */
