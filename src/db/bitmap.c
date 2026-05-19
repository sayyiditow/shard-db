/* bitmap.c — per-shard bitmap index, MVP for 2026.05.7.
 *
 * Storage layout per [[bitmap-impl-map]]. Mmap-based for read speed;
 * any size-changing operation (new dict value or stride doubling on
 * shard-grow) rewrites the whole shard file atomically via tmp+rename.
 * Set/clear/test on existing values are constant-time byte ops.
 *
 * The bool fast-path bypasses the dict scan: the file is created with
 * exactly two values (0x00, 0x01) and the value→bitmap index is the
 * value byte itself.
 *
 * Concurrency: this file is single-writer (the per-shard rwlock owned
 * by the caller serialises mutations). Readers see consistent state
 * via mmap because we publish the rewritten file via rename.
 */

#define _GNU_SOURCE
#include "bitmap.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define BM_MAGIC 0x31304D42u   /* 'BM01' little-endian */
#define BM_VERSION 1

/* Header is 32 bytes; layout pinned in bitmap.h. */
struct __attribute__((packed)) BmHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint32_t slots;
    uint32_t n_values;
    uint32_t dict_off;
    uint32_t bitmaps_off;
    uint32_t stride;
    uint32_t reserved;
};

struct BitmapShard {
    int fd;
    void *mmap_ptr;
    size_t mmap_size;
    char path[1024];
    /* Cached header view — re-loaded after every rewrite. */
    struct BmHeader hdr;
};

/* ─────────────────────── small helpers ─────────────────────── */

static uint32_t bm_stride_for(uint32_t slots) {
    return (slots + 7u) / 8u;
}

static size_t bm_file_size(const struct BmHeader *h) {
    /* bitmaps_off + n_values * stride */
    return (size_t)h->bitmaps_off + (size_t)h->n_values * (size_t)h->stride;
}

static int bm_remap(BitmapShard *bm) {
    if (bm->mmap_ptr && bm->mmap_size > 0) {
        munmap(bm->mmap_ptr, bm->mmap_size);
        bm->mmap_ptr = NULL;
        bm->mmap_size = 0;
    }
    struct stat st;
    if (fstat(bm->fd, &st) != 0) return -1;
    if (st.st_size < (off_t)sizeof(struct BmHeader)) return -1;
    bm->mmap_ptr = mmap(NULL, st.st_size, PROT_READ | PROT_WRITE,
                        MAP_SHARED, bm->fd, 0);
    if (bm->mmap_ptr == MAP_FAILED) {
        bm->mmap_ptr = NULL;
        return -1;
    }
    bm->mmap_size = st.st_size;
    memcpy(&bm->hdr, bm->mmap_ptr, sizeof(struct BmHeader));
    return 0;
}

/* Atomically replace `bm->path` with `tmp_path`'s contents, then re-mmap.
   On any failure, leaves the live file unchanged and bm in a sane state. */
static int bm_publish(BitmapShard *bm, const char *tmp_path) {
    if (rename(tmp_path, bm->path) != 0) {
        unlink(tmp_path);
        return -1;
    }
    /* Old fd is now pointing at an unlinked inode — close + reopen. */
    if (bm->fd >= 0) close(bm->fd);
    bm->fd = open(bm->path, O_RDWR);
    if (bm->fd < 0) return -1;
    return bm_remap(bm);
}

static int bm_mkdir_p(const char *path) {
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    return mkdir(tmp, 0755);
}

void bm_build_path(char *out, size_t outlen,
                   const char *db_root, const char *object,
                   const char *field, int shard_idx) {
    snprintf(out, outlen, "%s/%s/indexes/%s/%03x.bm",
             db_root, object, field, shard_idx);
}

/* Total bytes occupied by the packed dictionary (sum of every entry's
   2-byte length prefix + value bytes). The region between dict_off and
   dict_off + dict_used_bytes is the actual data; anything between
   dict_off + dict_used_bytes and bitmaps_off is alignment padding and
   must not be read. */
static uint32_t bm_dict_used_bytes(const BitmapShard *bm) {
    if (bm->hdr.flags & BM_FLAG_BOOL_FASTPATH) return 6; /* [01 00 00][01 00 01] */
    const uint8_t *p = (const uint8_t *)bm->mmap_ptr + bm->hdr.dict_off;
    uint32_t off = 0;
    for (uint32_t i = 0; i < bm->hdr.n_values; i++) {
        uint16_t len = (uint16_t)p[off] | ((uint16_t)p[off + 1] << 8);
        off += 2u + len;
    }
    return off;
}

/* Locate a value in the dictionary. Returns the value index (0..n_values-1)
   or -1 if not found. Linear scan — fine for the low-cardinality enums
   bitmap is designed for; for bool fast-path the caller skips this
   entirely and uses the value byte directly.

   Pointer arithmetic over a packed mmap region, treated as bytes. */
static int bm_dict_lookup(const BitmapShard *bm, const uint8_t *value, size_t vlen) {
    if (bm->hdr.flags & BM_FLAG_BOOL_FASTPATH) {
        if (vlen != 1) return -1;
        if (value[0] == 0x00) return 0;
        if (value[0] == 0x01) return 1;
        return -1;
    }
    const uint8_t *p = (const uint8_t *)bm->mmap_ptr + bm->hdr.dict_off;
    const uint8_t *end = (const uint8_t *)bm->mmap_ptr + bm->hdr.bitmaps_off;
    for (uint32_t i = 0; i < bm->hdr.n_values; i++) {
        if (p + 2 > end) return -1;
        uint16_t len = (uint16_t)p[0] | ((uint16_t)p[1] << 8);
        if (p + 2 + len > end) return -1;
        if (len == vlen && memcmp(p + 2, value, vlen) == 0) return (int)i;
        p += 2 + len;
    }
    return -1;
}

/* ─────────────────────── creation ─────────────────────── */

static int bm_write_initial(const char *path, uint32_t slots, int bool_fastpath) {
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    int fd = open(tmp, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;

    struct BmHeader hdr = {0};
    hdr.magic = BM_MAGIC;
    hdr.version = BM_VERSION;
    hdr.flags = bool_fastpath ? BM_FLAG_BOOL_FASTPATH : 0;
    hdr.slots = slots;
    hdr.stride = bm_stride_for(slots);

    /* Dict is PACKED — no pre-reserved capacity. Holding leading zero
       bytes would be read by bm_dict_lookup() as `len=0` entries and
       break the scan. When a new dict value lands, the file is
       rewritten with a larger dict; this is rare for bitmap's
       low-cardinality target (bool, small enums). */
    hdr.dict_off = sizeof(struct BmHeader);

    uint32_t dict_size = 0;
    if (bool_fastpath) {
        hdr.n_values = 2;
        dict_size = 6;   /* two 1-byte entries: [01 00 00][01 00 01] */
    }
    /* 8-byte align bitmaps_off so the per-bitmap region starts on a
       word boundary — keeps popcount loops uint64-friendly later. */
    hdr.bitmaps_off = hdr.dict_off + dict_size;
    if (hdr.bitmaps_off & 7u) hdr.bitmaps_off = (hdr.bitmaps_off + 7u) & ~7u;

    /* Compute total size and ftruncate to it. */
    size_t total = bm_file_size(&hdr);
    if (ftruncate(fd, total) != 0) {
        close(fd); unlink(tmp); return -1;
    }

    /* Write header. */
    if (pwrite(fd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) {
        close(fd); unlink(tmp); return -1;
    }

    /* Write dictionary for bool — packed, [len][value] per entry. */
    if (bool_fastpath) {
        uint8_t dict_bytes[6] = { 0x01, 0x00,  /* len=1 */ 0x00,
                                  0x01, 0x00,  /* len=1 */ 0x01 };
        if (pwrite(fd, dict_bytes, sizeof(dict_bytes), hdr.dict_off)
                != (ssize_t)sizeof(dict_bytes)) {
            close(fd); unlink(tmp); return -1;
        }
    }

    /* Bitmap area is already zero-filled by ftruncate(). */
    fsync(fd);
    close(fd);
    if (rename(tmp, path) != 0) { unlink(tmp); return -1; }
    return 0;
}

BitmapShard *bm_open(const char *path, int slots, int create, int bool_fastpath) {
    /* Ensure parent dir exists. The caller's `<obj>/indexes/<field>/`
       layer might be brand new on first insert. */
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) {
        *slash = '\0';
        bm_mkdir_p(dir);
    }

    int fd = open(path, O_RDWR);
    if (fd < 0) {
        if (!create) return NULL;
        if (bm_write_initial(path, (uint32_t)slots, bool_fastpath) != 0) return NULL;
        fd = open(path, O_RDWR);
        if (fd < 0) return NULL;
    }

    BitmapShard *bm = calloc(1, sizeof(*bm));
    if (!bm) { close(fd); return NULL; }
    bm->fd = fd;
    snprintf(bm->path, sizeof(bm->path), "%s", path);
    if (bm_remap(bm) != 0) {
        close(fd);
        free(bm);
        return NULL;
    }
    /* Sanity check magic. */
    if (bm->hdr.magic != BM_MAGIC) {
        bm_close(bm);
        return NULL;
    }
    return bm;
}

void bm_close(BitmapShard *bm) {
    if (!bm) return;
    if (bm->mmap_ptr && bm->mmap_size > 0) {
        munmap(bm->mmap_ptr, bm->mmap_size);
    }
    if (bm->fd >= 0) close(bm->fd);
    free(bm);
}

/* ─────────────────────── set / clear / test ─────────────────────── */

/* Add a new value to the dictionary by rewriting the file. Returns the
   new value index, or -1 on failure (including the BM_MAX_VALUES cap).

   The dict is PACKED (no internal padding) so the lookup walker stays
   correct. Any 8-byte alignment for bitmaps_off lives AFTER the packed
   dict's last entry, outside the scan region. */
static int bm_dict_add(BitmapShard *bm, const uint8_t *value, size_t vlen) {
    const uint8_t *old = (const uint8_t *)bm->mmap_ptr;
    uint32_t old_n = bm->hdr.n_values;

    /* Enforce the cardinality contract. Past BM_MAX_VALUES, bitmap
       isn't the right index — btree is. The wire layer translates
       this -1 into a user-actionable error. */
    if (old_n >= BM_MAX_VALUES) return -1;

    /* Actual packed bytes — NOT bitmaps_off - dict_off, which would
       include alignment padding. */
    uint32_t old_dict_used = bm_dict_used_bytes(bm);
    uint32_t new_dict_used = old_dict_used + 2u + (uint32_t)vlen;

    /* New bitmaps_off: right after the new dict, 8-byte aligned. */
    uint32_t new_bitmaps_off = bm->hdr.dict_off + new_dict_used;
    if (new_bitmaps_off & 7u) new_bitmaps_off = (new_bitmaps_off + 7u) & ~7u;

    struct BmHeader nhdr = bm->hdr;
    nhdr.n_values = old_n + 1;
    nhdr.bitmaps_off = new_bitmaps_off;
    size_t total = bm_file_size(&nhdr);

    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s.tmp", bm->path);
    int fd = open(tmp, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    if (ftruncate(fd, total) != 0) { close(fd); unlink(tmp); return -1; }

    /* Write header. */
    if (pwrite(fd, &nhdr, sizeof(nhdr), 0) != (ssize_t)sizeof(nhdr)) {
        close(fd); unlink(tmp); return -1;
    }
    /* Copy old PACKED dict (skip any alignment padding from the old file). */
    if (old_dict_used > 0) {
        if (pwrite(fd, old + bm->hdr.dict_off, old_dict_used, nhdr.dict_off)
                != (ssize_t)old_dict_used) {
            close(fd); unlink(tmp); return -1;
        }
    }
    /* Append the new entry right after the packed dict. */
    uint8_t lenbuf[2] = { (uint8_t)(vlen & 0xff), (uint8_t)((vlen >> 8) & 0xff) };
    off_t new_entry_off = (off_t)nhdr.dict_off + (off_t)old_dict_used;
    if (pwrite(fd, lenbuf, 2, new_entry_off) != 2 ||
        pwrite(fd, value, vlen, new_entry_off + 2) != (ssize_t)vlen) {
        close(fd); unlink(tmp); return -1;
    }
    /* Copy old bitmaps to new bitmaps_off (stride unchanged). */
    if (old_n > 0) {
        size_t old_bytes = (size_t)old_n * (size_t)bm->hdr.stride;
        if (pwrite(fd, old + bm->hdr.bitmaps_off, old_bytes, nhdr.bitmaps_off)
                != (ssize_t)old_bytes) {
            close(fd); unlink(tmp); return -1;
        }
    }
    /* The new value's bitmap occupies the LAST stride bytes; ftruncate
       already zero-filled it. */
    fsync(fd);
    close(fd);

    if (bm_publish(bm, tmp) != 0) return -1;
    return (int)old_n; /* the new value's index */
}

/* Pointer to the start of value-i's bitmap inside the mmap. */
static uint8_t *bm_bitmap_ptr(BitmapShard *bm, int vidx) {
    return (uint8_t *)bm->mmap_ptr + bm->hdr.bitmaps_off +
           (size_t)vidx * (size_t)bm->hdr.stride;
}

int bm_set(BitmapShard *bm, const uint8_t *value, size_t vlen, uint32_t slot) {
    if (!bm || slot >= bm->hdr.slots) return -1;

    int vidx = bm_dict_lookup(bm, value, vlen);
    if (vidx < 0) {
        /* New value — grow dict (file rewrite). Bool fastpath rejects
           unknown values; the wire layer should have prevented this. */
        if (bm->hdr.flags & BM_FLAG_BOOL_FASTPATH) return -1;
        vidx = bm_dict_add(bm, value, vlen);
        if (vidx < 0) return -1;
    }

    uint8_t *bmap = bm_bitmap_ptr(bm, vidx);
    bmap[slot >> 3] |= (uint8_t)(1u << (slot & 7u));
    return 0;
}

int bm_clear(BitmapShard *bm, const uint8_t *value, size_t vlen, uint32_t slot) {
    if (!bm || slot >= bm->hdr.slots) return -1;
    int vidx = bm_dict_lookup(bm, value, vlen);
    if (vidx < 0) return 0; /* nothing to clear */
    uint8_t *bmap = bm_bitmap_ptr(bm, vidx);
    bmap[slot >> 3] &= (uint8_t)~(1u << (slot & 7u));
    return 0;
}

int bm_test(const BitmapShard *bm, const uint8_t *value, size_t vlen, uint32_t slot) {
    if (!bm || slot >= bm->hdr.slots) return 0;
    int vidx = bm_dict_lookup(bm, value, vlen);
    if (vidx < 0) return 0;
    const uint8_t *bmap = (const uint8_t *)bm->mmap_ptr + bm->hdr.bitmaps_off +
                          (size_t)vidx * (size_t)bm->hdr.stride;
    return (bmap[slot >> 3] >> (slot & 7u)) & 1u;
}

/* ─────────────────────── walk + count ─────────────────────── */

int bm_walk(const BitmapShard *bm, const uint8_t *value, size_t vlen,
            int (*cb)(uint32_t slot, void *ctx), void *ctx) {
    if (!bm || !cb) return 0;
    int vidx = bm_dict_lookup(bm, value, vlen);
    if (vidx < 0) return 0;
    const uint8_t *bmap = (const uint8_t *)bm->mmap_ptr + bm->hdr.bitmaps_off +
                          (size_t)vidx * (size_t)bm->hdr.stride;
    int n = 0;
    for (uint32_t byte_idx = 0; byte_idx < bm->hdr.stride; byte_idx++) {
        uint8_t b = bmap[byte_idx];
        while (b) {
            int bit = __builtin_ctz(b);
            uint32_t slot = byte_idx * 8u + (uint32_t)bit;
            if (slot >= bm->hdr.slots) return n;
            if (cb(slot, ctx) != 0) return n + 1;
            n++;
            b &= b - 1; /* clear lowest set bit */
        }
    }
    return n;
}

uint32_t bm_count(const BitmapShard *bm, const uint8_t *value, size_t vlen) {
    if (!bm) return 0;
    int vidx = bm_dict_lookup(bm, value, vlen);
    if (vidx < 0) return 0;
    const uint8_t *bmap = (const uint8_t *)bm->mmap_ptr + bm->hdr.bitmaps_off +
                          (size_t)vidx * (size_t)bm->hdr.stride;
    uint32_t total = 0;
    for (uint32_t i = 0; i < bm->hdr.stride; i++) {
        total += (uint32_t)__builtin_popcount(bmap[i]);
    }
    return total;
}

/* ─────────────────────── grow ─────────────────────── */

int bm_grow(BitmapShard *bm, uint32_t new_slots) {
    if (!bm || new_slots <= bm->hdr.slots) return 0;

    uint32_t new_stride = bm_stride_for(new_slots);
    if (new_stride == bm->hdr.stride) {
        /* Same stride byte count (slot count grew but not over a byte
           boundary). Just update the slots field in the header. */
        bm->hdr.slots = new_slots;
        memcpy(bm->mmap_ptr, &bm->hdr, sizeof(struct BmHeader));
        return 0;
    }

    /* Rewrite the file with extended bitmaps. Each bitmap is copied as-is
       into the leading bytes of its expanded slot, then padded with
       zeros (ftruncate semantics). */
    struct BmHeader nhdr = bm->hdr;
    nhdr.slots = new_slots;
    nhdr.stride = new_stride;
    size_t total = bm_file_size(&nhdr);

    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s.tmp", bm->path);
    int fd = open(tmp, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    if (ftruncate(fd, total) != 0) { close(fd); unlink(tmp); return -1; }

    /* Header + dict pass through unchanged. */
    if (pwrite(fd, &nhdr, sizeof(nhdr), 0) != (ssize_t)sizeof(nhdr)) {
        close(fd); unlink(tmp); return -1;
    }
    if (bm->hdr.bitmaps_off > bm->hdr.dict_off) {
        size_t dict_bytes = bm->hdr.bitmaps_off - bm->hdr.dict_off;
        if (pwrite(fd, (const uint8_t *)bm->mmap_ptr + bm->hdr.dict_off,
                   dict_bytes, nhdr.dict_off) != (ssize_t)dict_bytes) {
            close(fd); unlink(tmp); return -1;
        }
    }
    /* Per-bitmap copy at expanded stride. */
    for (uint32_t i = 0; i < bm->hdr.n_values; i++) {
        const uint8_t *src = (const uint8_t *)bm->mmap_ptr + bm->hdr.bitmaps_off +
                             (size_t)i * (size_t)bm->hdr.stride;
        off_t dst = (off_t)nhdr.bitmaps_off + (off_t)i * (off_t)new_stride;
        if (pwrite(fd, src, bm->hdr.stride, dst) != (ssize_t)bm->hdr.stride) {
            close(fd); unlink(tmp); return -1;
        }
        /* The tail bytes are zero-filled by ftruncate. */
    }
    fsync(fd);
    close(fd);

    return bm_publish(bm, tmp);
}

/* ─────────────────────── observability ─────────────────────── */

uint32_t bm_n_values(const BitmapShard *bm) { return bm ? bm->hdr.n_values : 0; }
uint32_t bm_slots(const BitmapShard *bm)    { return bm ? bm->hdr.slots    : 0; }
uint32_t bm_stride(const BitmapShard *bm)   { return bm ? bm->hdr.stride   : 0; }
