#ifndef IO_DIRECT_H
#define IO_DIRECT_H
/*
 * io_direct.h — aligned O_DIRECT pread helpers for cache-bypassing full scans.
 *
 * These helpers are used by 1e.4 (FP_FULL_SCAN re-wiring); this header only
 * declares them.  No existing query path is changed here.
 *
 * Design:
 *   - Linux:   O_DIRECT flag on open; aligned pread; silent fallback to
 *              buffered + POSIX_FADV_SEQUENTIAL|DONTNEED on EINVAL.
 *   - macOS:   F_NOCACHE via fcntl after open; no alignment requirements
 *              from the OS but we keep aligned buffers for consistency.
 *   - Chunk:   32 MB (reduced syscall rate on modern NVMe).
 *   - Double-buffering: one worker thread pre-fetches the next chunk while
 *     the main thread parses the current chunk.
 */

#include <stddef.h>
#include <sys/types.h>
#include <stdint.h>

#include "types.h"  /* FieldSchema, CompiledCriterion, CriteriaNode, QueryDeadline */

#define ODIRECT_BUF_SIZE_DEFAULT  (32 * 1024 * 1024)  /* 32 MB: fits one shard at splits≤256 */
#define ODIRECT_ALIGN            4096                 /* O_DIRECT alignment unit */

/* Configurable chunk size. Override via env var DB_ODIRECT_BUF_MB=N (integer MB).
 * _Atomic: lazily initialised from multiple io_pool_worker threads
 * concurrently in odirect_init_buf_size(); plain read/write raced under
 * TSan even though racing initializers always compute the same value. */
extern _Atomic size_t odirect_buf_size;

/* ---------------------------------------------------------------------------
 * Low-level helpers
 * ------------------------------------------------------------------------- */

/* Open `path` for unbuffered scanning.  Returns fd >= 0 on success.
 * Linux: probes page-cache residency via mincore first; if ≥80% of sampled
 *   pages are resident, opens buffered (RAM speed beats O_DIRECT on warm data).
 *   Otherwise tries O_DIRECT; falls back to buffered+FADV_DONTNEED on EINVAL.
 * macOS: applies F_NOCACHE via fcntl after open.
 * Caller cannot distinguish buffered from unbuffered — that is intentional. */
int od_open(const char *path);

/* Aligned pread wrapper.  `buf` must be posix_memalign'd to ODIRECT_ALIGN;
 * `len` and `off` must be multiples of ODIRECT_ALIGN (the caller aligns via
 * ODIRECT_BUF_SIZE strides).  Returns bytes read (may be < len at EOF) or
 * -errno on error.  Works on a buffered fallback fd too. */
ssize_t od_pread(int fd, void *buf, size_t len, off_t off);

/* Read DB_ODIRECT_BUF_MB from env and set odirect_buf_size.
 * Does nothing if the env var is unset/empty/zero/parse-error.
 * Call only once (from startup), or let the lazy init in od_alloc_buf handle it. */
void odirect_init_buf_size(void);

/* Allocate one odirect_buf_size-aligned scan buffer.  Caller frees with
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

/* Callback for value-only seg-file records.
 *   value    — pointer to record value bytes (contiguous, after the key)
 *   vlen     — value byte count
 *   ctx      — caller context
 * Return 0 to continue, non-zero to stop the scan. */
typedef int (*od_value_cb)(const uint8_t *value, size_t vlen, void *ctx);

/* Callback for btree leaf entries.
 *   value    — decoded (prefix-decompressed) btree value bytes
 *   vlen     — byte count
 *   hash16   — 16-byte hash stored in the leaf entry
 *   ctx      — caller context
 * Return 0 to continue, non-zero to stop. */
typedef int (*od_leaf_cb)(const uint8_t *value, size_t vlen,
                          const uint8_t hash16[16], void *ctx);

/* Variable-length variant: walks a varlen-format segment file where records
   are not at fixed stride.  Scans each record's 24-byte header to determine
   the padded record size, strides by that amount, and calls `cb` for every
   live record (flag == 1).  Max single-record carry buffer is 256 KB.
   `max_slot_size` is the owning object's schema-derived maximum on-disk
   record capacity (SlotcaskDb.slot_size); it is not the segment file cap.

   Returns:
    0    — success
   +1    — cb returned non-zero (early stop)
   -errno — I/O error */
int seg_scan_o_direct(const char *seg_path, size_t max_slot_size,
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
