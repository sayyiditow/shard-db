/* seg_scan_varlen.h — shared resync/validation helpers for VARLEN
 * segment scanning. Used by the O_DIRECT scanner (io_direct.c) and the
 * mmap-based maintenance walks (slotcask.c: seg_stat_one_varlen,
 * compact_migrate_records_varlen) to recover from reused-slot
 * zero-padding gaps that don't match the scanning record's own natural
 * size. See docs/plans/2026-08-07-varlen-segment-scan-resync.md for the
 * root cause.
 */
#ifndef SHARD_DB_SEG_SCAN_VARLEN_H
#define SHARD_DB_SEG_SCAN_VARLEN_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include "types.h"   /* compute_hash_raw visibility (declared in types.h, not slotcask.h) */

/* Every VARLEN record is written 8-byte aligned, on-disk size
   (24 + klen + vlen) rounded up to 8 — must match
   slotcask.c's slotcask_record_size_varlen() / io_direct.c's
   od_varlen_rec_size() exactly (both already carry this same formula
   independently; this is a third deliberate copy, not a refactor, to
   avoid a header depending on either TU's private helper). */
static inline size_t seg_scan_varlen_size(uint16_t klen, uint32_t vlen) {
    size_t raw = 24 + (size_t)klen + (size_t)vlen;
    return (raw + 7) & ~(size_t)7;
}

/* Validate the record header at map[pos]: flag <= 2, header + rec_size
   fits within map_size, and rec_size itself <= max_slot_size. Does NOT
   verify the hash. max_slot_size is the owning object's cached
   SlotcaskDb.slot_size; it is deliberately not the segment file cap.
   Returns 1 and fills *out_rec_size/out_flag/
   out_klen/out_vlen on success; returns 0 on failure (outputs
   unspecified). */
static inline int seg_scan_varlen_struct_ok(const uint8_t *map, size_t map_size,
                                             size_t pos, size_t max_slot_size,
                                             size_t *out_rec_size,
                                             uint8_t *out_flag, uint16_t *out_klen,
                                             uint32_t *out_vlen) {
    if (pos + 24 > map_size) return 0;
    if (max_slot_size < 32) return 0;
    const uint8_t *rec = map + pos;
    uint8_t flag = rec[18];
    if (flag > 2) return 0;
    uint16_t klen;
    uint32_t vlen;
    memcpy(&klen, rec + 16, 2);
    memcpy(&vlen, rec + 20, 4);
    size_t rec_size = seg_scan_varlen_size(klen, vlen);
    if (rec_size > max_slot_size) return 0;
    if (pos + rec_size > map_size) return 0;
    *out_rec_size = rec_size;
    *out_flag = flag;
    *out_klen = klen;
    *out_vlen = vlen;
    return 1;
}

/* Verify the record at map[pos] carries a hash matching its key bytes
   (xxh128). Caller must already have validated the header (klen known
   in-bounds) via seg_scan_varlen_struct_ok. Only meaningful for
   flag in {1, 2} (live/tombstone) — flag == 0 (padding) records have no
   real key content and are never passed here. */
static inline int seg_scan_varlen_hash_ok(const uint8_t *map, size_t pos, uint16_t klen) {
    const uint8_t *rec = map + pos;
    uint8_t computed[16];
    compute_hash_raw((const char *)(rec + 24), (size_t)klen, computed);
    return memcmp(computed, rec, 16) == 0;
}

/* Search forward from `pos` (need not be 8-byte aligned) for the next
   offset within [pos, pos+window] that holds a structurally valid,
   hash-verified flag in {1,2} record header — the only kind of header
   safe to resume scanning from after a desync.

   Search only ever tests 8-byte-aligned candidates (record headers are
   only ever written at 8-byte-aligned offsets), computed by first
   flooring `pos` to the 8-byte grid (`pos & ~7`), not by stepping +8
   from `pos` itself — stepping from an unaligned `pos` would test only
   offsets sharing `pos`'s own residue mod 8, silently skipping the true
   grid.

   flag == 0 (padding / never-written) candidates are always skipped and
   never terminate the search: a zero-filled reuse gap reads back as a
   structurally "valid" flag==0 header (klen=0, vlen=0) at every
   8-byte-aligned offset inside it, so accepting flag==0 as a resync
   target would make resync stop at the first padding byte instead of
   finding the next real record — this was blocker #1 in the prior
   revision of this plan.

   `max_slot_size` is the owning object's cached maximum record capacity;
   `window` is the maximum forward distance to a candidate header and is
   inclusive at the upper bound. Returns 1 and sets *out_off on success (offset relative to `map`,
   i.e. an absolute offset if `map` is the whole file / a
   window-relative offset if `map` is a bounded buffer — caller adds its
   own base). Returns 0 if no valid record is found within the window. */
static inline int seg_scan_varlen_resync(const uint8_t *map, size_t map_size,
                                          size_t pos, size_t max_slot_size,
                                          size_t window, size_t *out_off) {
    if (pos >= map_size) return 0;
    size_t start = pos & ~(size_t)7;
    size_t limit = (window > map_size - pos) ? map_size : pos + window;
    for (size_t cand = start; cand < map_size && cand <= limit; cand += 8) {
        if (cand < pos) continue;
        size_t rec_size;
        uint8_t flag;
        uint16_t klen;
        uint32_t vlen;
        if (!seg_scan_varlen_struct_ok(map, map_size, cand, max_slot_size,
                                        &rec_size,
                                        &flag, &klen, &vlen)) {
            continue;
        }
        if (flag == 0) continue;
        if (!seg_scan_varlen_hash_ok(map, cand, klen)) continue;
        *out_off = cand;
        return 1;
    }
    return 0;
}

#endif /* SHARD_DB_SEG_SCAN_VARLEN_H */
