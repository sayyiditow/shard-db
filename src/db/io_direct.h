#ifndef IO_DIRECT_H
#define IO_DIRECT_H
/*
 * io_direct.h — aligned O_DIRECT pread helpers for cache-bypassing full scans.
 *
 * These helpers are used by 1e.4 (FP_FULL_SCAN re-wiring); this header only
 * declares them.  No existing query path is changed here.
 *
 * Design locked 2026-05-28:
 *   - Linux:   O_DIRECT flag on open; aligned pread; silent fallback to
 *              buffered + POSIX_FADV_SEQUENTIAL|DONTNEED on EINVAL.
 *   - macOS:   F_NOCACHE via fcntl after open; no alignment requirements
 *              from the OS but we keep aligned buffers for consistency.
 *   - Chunk:   4 MB (sweet spot for NVMe sequential throughput).
 *   - Double-buffering: one worker thread pre-fetches the next chunk while
 *     the main thread parses the current chunk.
 */

#include <stddef.h>
#include <sys/types.h>
#include <stdint.h>

#define ODIRECT_BUF_SIZE  (4 * 1024 * 1024)   /* 4 MB aligned chunk       */
#define ODIRECT_ALIGN     4096                 /* O_DIRECT alignment unit  */

/* ---------------------------------------------------------------------------
 * Low-level helpers
 * ------------------------------------------------------------------------- */

/* Open `path` for unbuffered scanning.  Returns fd >= 0 on success.
 * Linux: tries O_DIRECT; macOS: applies F_NOCACHE.
 * On failure to enable cache-bypass (EINVAL / ENOTSUP / unsupported):
 *   silently opens buffered and applies posix_fadvise(SEQUENTIAL|DONTNEED).
 * Caller cannot distinguish buffered from unbuffered — that is intentional. */
int od_open(const char *path);

/* Aligned pread wrapper.  `buf` must be posix_memalign'd to ODIRECT_ALIGN;
 * `len` and `off` must be multiples of ODIRECT_ALIGN (the caller aligns via
 * ODIRECT_BUF_SIZE strides).  Returns bytes read (may be < len at EOF) or
 * -errno on error.  Works on a buffered fallback fd too. */
ssize_t od_pread(int fd, void *buf, size_t len, off_t off);

/* Allocate one ODIRECT_BUF_SIZE-aligned scan buffer.  Caller frees with
 * free().  Returns NULL on allocation failure. */
void *od_alloc_buf(void);

/* ---------------------------------------------------------------------------
 * Scan callback types
 * ------------------------------------------------------------------------- */

/* Callback for seg-file records.
 *   rec      — pointer into the current chunk buffer (valid until next swap)
 *   vlen     — value length (from record header)
 *   hash16   — 16-byte xxh128 key (first 16 bytes of record header)
 *   ctx      — caller context
 * Return 0 to continue, non-zero to stop the scan. */
typedef int (*od_record_cb)(const uint8_t *rec, size_t vlen,
                            const uint8_t hash16[16], void *ctx);

/* Callback for btree leaf entries.
 *   value    — decoded (prefix-decompressed) btree value bytes
 *   vlen     — byte count
 *   hash16   — 16-byte hash stored in the leaf entry
 *   ctx      — caller context
 * Return 0 to continue, non-zero to stop. */
typedef int (*od_leaf_cb)(const uint8_t *value, size_t vlen,
                          const uint8_t hash16[16], void *ctx);

/* ---------------------------------------------------------------------------
 * Double-buffered seg-file walker
 * ------------------------------------------------------------------------- */

/* Walk all live slots in `seg_path` end-to-end using O_DIRECT + double-
 * buffered async pre-fetch.  Calls `cb` for every live slot (flag == 1).
 * `slot_size` is the fixed per-record byte width for this object (from
 * SlotcaskDb.slot_size / Schema.slot_size).
 *
 * Returns:
 *   0    — success (all slots visited)
 *  +1    — cb returned non-zero (early stop)
 *  -errno — I/O error
 */
int seg_scan_o_direct(const char *seg_path, int slot_size,
                      od_record_cb cb, void *ctx);

/* ---------------------------------------------------------------------------
 * Double-buffered btree leaf-page walker
 * ------------------------------------------------------------------------- */

/* Walk all non-tombstoned leaf entries in the btree file at `btree_path`
 * using O_DIRECT + double-buffered async pre-fetch.  Calls `cb` per entry
 * with the decoded (prefix-decompressed) value and its 16-byte hash.
 *
 * Returns:
 *   0    — success
 *  +1    — cb returned non-zero (early stop)
 *  -errno — I/O error
 *
 * NOTE: this is a FULL-FILE scan that bypasses bt_cache.  Do NOT call while
 * holding a bt_cache lock on the same file. */
int btree_leaf_scan_o_direct(const char *btree_path,
                             od_leaf_cb cb, void *ctx);

#endif /* IO_DIRECT_H */
