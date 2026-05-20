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
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define BM_MAGIC 0x31304D42u   /* 'BM01' little-endian */
#define BM_VERSION 1

/* Header is 32 bytes; layout pinned in bitmap.h. The former `reserved`
   slot is now `max_values` — the per-file distinct-value cap set by
   create-object. Stored as uint32 to allow future expansion past
   BM_HARD_CEILING without another format bump (the on-disk slot is
   already wide enough; the .h ceiling is the policy gate). Files
   produced before this field existed read it as 0 thanks to the
   ftruncate zero-fill; bm_open treats 0 as BM_DEFAULT_MAX_VALUES for
   forward compatibility. */
struct __attribute__((packed)) BmHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint32_t slots;
    uint32_t n_values;
    uint32_t dict_off;
    uint32_t bitmaps_off;
    uint32_t stride;
    uint32_t max_values;
};

/* ─────────────────── global bm cache ───────────────────
   Mirrors src/db/btree.c's bt_cache. Path-keyed open-addressed slot
   table; per-entry rwlock; LRU eviction once half-full. The cache
   keeps one mmap'd handle per .bm path, alive across requests.
   Concurrent rdlock holders are fine (bitmap reads are mmap loads);
   writers serialise via the per-entry wrlock.

   Initialised by bm_cache_init() at daemon startup, mirroring
   bt_cache_init(). If uninitialised, bm_open falls back to a fresh
   mmap per call (test fixtures use this — no cache thrash). */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
} BmCacheEntry;

static BmCacheEntry    *g_bm_cache = NULL;
static int              g_bm_cache_slots = 0;
static int              g_bm_cache_count = 0;
static pthread_mutex_t  g_bm_cache_lock = PTHREAD_MUTEX_INITIALIZER;
static volatile uint64_t g_bm_cache_clock = 0;

static int bm_next_pow2(int n) { int p = 1; while (p < n) p <<= 1; return p; }

void bm_cache_init(int cap) {
    if (g_bm_cache) return;
    if (cap < 16) cap = 16;
    g_bm_cache_slots = bm_next_pow2(cap * 2);
    g_bm_cache = calloc((size_t)g_bm_cache_slots, sizeof(BmCacheEntry));
    g_bm_cache_count = 0;
    for (int i = 0; i < g_bm_cache_slots; i++) {
        pthread_rwlock_init(&g_bm_cache[i].rwlock, NULL);
        g_bm_cache[i].fd = -1;
    }
}

void bm_cache_shutdown(void) {
    pthread_mutex_lock(&g_bm_cache_lock);
    if (g_bm_cache) {
        for (int i = 0; i < g_bm_cache_slots; i++) {
            BmCacheEntry *e = &g_bm_cache[i];
            if (!e->used) continue;
            if (e->map && e->map_size > 0) msync(e->map, e->map_size, MS_SYNC);
            if (e->map) munmap(e->map, e->map_size);
            if (e->fd >= 0) close(e->fd);
            pthread_rwlock_destroy(&e->rwlock);
        }
        free(g_bm_cache);
        g_bm_cache = NULL;
        g_bm_cache_slots = 0;
        g_bm_cache_count = 0;
    }
    pthread_mutex_unlock(&g_bm_cache_lock);
}

static uint32_t bm_path_hash(const char *s) {
    uint32_t h = 5381;
    while (*s) h = h * 33u + (unsigned char)*s++;
    return h;
}

static int bm_cache_probe(const char *path, int *out_found) {
    uint32_t h = bm_path_hash(path);
    int mask = g_bm_cache_slots - 1;
    int idx = (int)(h & (uint32_t)mask);
    for (int i = 0; i < g_bm_cache_slots; i++) {
        int s = (idx + i) & mask;
        if (!g_bm_cache[s].used) { *out_found = 0; return s; }
        if (strcmp(g_bm_cache[s].path, path) == 0) { *out_found = 1; return s; }
    }
    *out_found = 0;
    return -1;
}

/* Caller holds g_bm_cache_lock and has ensured no rwlock holder. */
static void bm_cache_drop_slot(int slot) {
    BmCacheEntry *e = &g_bm_cache[slot];
    if (!e->used) return;
    if (e->map && e->map_size > 0) msync(e->map, e->map_size, MS_ASYNC);
    if (e->map) munmap(e->map, e->map_size);
    if (e->fd >= 0) close(e->fd);
    e->map = NULL;
    e->fd = -1;
    e->map_size = 0;
    e->used = 0;
    e->path[0] = '\0';
    g_bm_cache_count--;
}

void bm_cache_invalidate(const char *path) {
    if (!g_bm_cache) return;
    pthread_mutex_lock(&g_bm_cache_lock);
    int found = 0;
    int slot = bm_cache_probe(path, &found);
    if (found && slot >= 0) {
        /* Caller must ensure no rwlock holder — used by bm_grow's
           rewrite-and-publish path. */
        bm_cache_drop_slot(slot);
    }
    pthread_mutex_unlock(&g_bm_cache_lock);
}

/* ─────────────────── BitmapShard handle ─────────────────── */

struct BitmapShard {
    int      slot;        /* cache slot index, or -1 if uncached fallback */
    int      writer;      /* 1 = held wrlock, 0 = held rdlock; only when slot >= 0 */
    int      fd;
    void    *mmap_ptr;
    size_t   mmap_size;
    char     path[PATH_MAX];
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

/* Remap the handle's fd. If `bm` is cached (slot>=0), the cache entry's
   map/size are updated in lockstep — caller already holds the wrlock
   that excludes readers/writers, so no one observes a torn map. */
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

    /* Republish to the cache entry so future bm_acquire of this path
       sees the new map/size. */
    if (bm->slot >= 0 && g_bm_cache) {
        BmCacheEntry *e = &g_bm_cache[bm->slot];
        e->fd = bm->fd;
        e->map = bm->mmap_ptr;
        e->map_size = bm->mmap_size;
    }
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

static int bm_write_initial(const char *path, uint32_t slots, int bool_fastpath,
                            uint32_t max_values) {
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
    hdr.max_values = max_values ? max_values : BM_DEFAULT_MAX_VALUES;

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

/* Open and mmap the on-disk file. Used by both the cached and uncached
   paths. Reads the header into *out_hdr. Returns 0/fd on success. */
static int bm_file_open_mmap(const char *path,
                              int *out_fd, uint8_t **out_map, size_t *out_size,
                              struct BmHeader *out_hdr) {
    int fd = open(path, O_RDWR);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < (off_t)sizeof(struct BmHeader)) {
        close(fd); return -1;
    }
    uint8_t *map = mmap(NULL, st.st_size, PROT_READ | PROT_WRITE,
                        MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) { close(fd); return -1; }
    memcpy(out_hdr, map, sizeof(struct BmHeader));
    if (out_hdr->magic != BM_MAGIC) {
        munmap(map, st.st_size); close(fd); return -1;
    }
    *out_fd = fd;
    *out_map = map;
    *out_size = (size_t)st.st_size;
    return 0;
}

BitmapShard *bm_open(const char *path, int slots, int create,
                     int bool_fastpath, uint32_t max_values, int writer) {
    /* Ensure parent dir + the on-disk file exists if creation was asked. */
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) { *slash = '\0'; bm_mkdir_p(dir); }
    {
        struct stat st;
        if (stat(path, &st) != 0) {
            if (!create) return NULL;
            if (bm_write_initial(path, (uint32_t)slots, bool_fastpath, max_values) != 0)
                return NULL;
        }
    }

    /* Uncached fallback: cache not initialised (test fixtures). Direct
       mmap, no rwlock. */
    if (!g_bm_cache) {
        BitmapShard *bm = calloc(1, sizeof(*bm));
        if (!bm) return NULL;
        bm->slot = -1;
        bm->writer = writer;
        snprintf(bm->path, sizeof(bm->path), "%s", path);
        if (bm_file_open_mmap(path, &bm->fd, (uint8_t **)&bm->mmap_ptr,
                              &bm->mmap_size, &bm->hdr) != 0) {
            free(bm); return NULL;
        }
        if (bm->hdr.max_values == 0) {
            bm->hdr.max_values = BM_DEFAULT_MAX_VALUES;
            memcpy(bm->mmap_ptr, &bm->hdr, sizeof(struct BmHeader));
        }
        return bm;
    }

    /* Cached path — mirror bt_acquire's verify-and-retry pattern. */
    int retries = 0;
    int found = 0;
    int slot = -1;
    pthread_mutex_lock(&g_bm_cache_lock);
    while (1) {
        slot = bm_cache_probe(path, &found);
        if (!found) break;

        g_bm_cache[slot].last_access =
            __atomic_add_fetch(&g_bm_cache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &g_bm_cache[slot].rwlock;
        pthread_mutex_unlock(&g_bm_cache_lock);

        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);

        BmCacheEntry *e = &g_bm_cache[slot];
        if (e->used && strcmp(e->path, path) == 0) {
            /* Confirmed hit. Hand the rwlock + cached map to caller. */
            BitmapShard *bm = calloc(1, sizeof(*bm));
            if (!bm) { pthread_rwlock_unlock(lock); return NULL; }
            bm->slot = slot;
            bm->writer = writer;
            bm->fd = e->fd;
            bm->mmap_ptr = e->map;
            bm->mmap_size = e->map_size;
            snprintf(bm->path, sizeof(bm->path), "%s", path);
            memcpy(&bm->hdr, e->map, sizeof(struct BmHeader));
            if (bm->hdr.max_values == 0 && writer) {
                bm->hdr.max_values = BM_DEFAULT_MAX_VALUES;
                memcpy(bm->mmap_ptr, &bm->hdr, sizeof(struct BmHeader));
            }
            return bm;
        }

        /* Slot was evicted+reused while we were blocked on the rwlock. */
        pthread_rwlock_unlock(lock);
        if (++retries >= 4) {
            slot = -1; found = 0;
            pthread_mutex_lock(&g_bm_cache_lock);
            break;
        }
        pthread_mutex_lock(&g_bm_cache_lock);
    }

    /* Cache-miss path: load from disk + install into the slot. */
    int fd; uint8_t *map; size_t sz; struct BmHeader hdr;
    if (bm_file_open_mmap(path, &fd, &map, &sz, &hdr) != 0) {
        pthread_mutex_unlock(&g_bm_cache_lock);
        return NULL;
    }

    if (slot < 0 || g_bm_cache_count >= g_bm_cache_slots / 2) {
        int lru = -1;
        uint64_t oldest = UINT64_MAX;
        for (int i = 0; i < g_bm_cache_slots; i++) {
            if (g_bm_cache[i].used && g_bm_cache[i].last_access < oldest) {
                oldest = g_bm_cache[i].last_access;
                lru = i;
            }
        }
        if (lru >= 0) {
            bm_cache_drop_slot(lru);
            slot = lru;
        }
    }

    BitmapShard *bm = calloc(1, sizeof(*bm));
    if (!bm) { munmap(map, sz); close(fd); pthread_mutex_unlock(&g_bm_cache_lock); return NULL; }

    if (slot < 0) {
        /* Cache full — uncached fallback. */
        pthread_mutex_unlock(&g_bm_cache_lock);
        bm->slot = -1;
        bm->writer = writer;
        bm->fd = fd;
        bm->mmap_ptr = map;
        bm->mmap_size = sz;
        bm->hdr = hdr;
        snprintf(bm->path, sizeof(bm->path), "%s", path);
        if (bm->hdr.max_values == 0 && writer) {
            bm->hdr.max_values = BM_DEFAULT_MAX_VALUES;
            memcpy(bm->mmap_ptr, &bm->hdr, sizeof(struct BmHeader));
        }
        return bm;
    }

    BmCacheEntry *e = &g_bm_cache[slot];
    strncpy(e->path, path, PATH_MAX - 1);
    e->path[PATH_MAX - 1] = '\0';
    e->fd = fd;
    e->map = map;
    e->map_size = sz;
    e->used = 1;
    e->last_access = __atomic_add_fetch(&g_bm_cache_clock, 1, __ATOMIC_RELAXED);
    g_bm_cache_count++;

    pthread_rwlock_t *lock = &e->rwlock;
    if (writer) pthread_rwlock_wrlock(lock);
    else        pthread_rwlock_rdlock(lock);
    pthread_mutex_unlock(&g_bm_cache_lock);

    bm->slot = slot;
    bm->writer = writer;
    bm->fd = fd;
    bm->mmap_ptr = map;
    bm->mmap_size = sz;
    bm->hdr = hdr;
    snprintf(bm->path, sizeof(bm->path), "%s", path);
    if (bm->hdr.max_values == 0 && writer) {
        bm->hdr.max_values = BM_DEFAULT_MAX_VALUES;
        memcpy(bm->mmap_ptr, &bm->hdr, sizeof(struct BmHeader));
    }
    return bm;
}

uint32_t bm_max_values(const BitmapShard *bm) {
    return bm ? bm->hdr.max_values : 0;
}

void bm_close(BitmapShard *bm) {
    if (!bm) return;
    if (bm->slot >= 0 && g_bm_cache) {
        /* Cached: release the rwlock — the cache keeps the mmap + fd
           alive across releases (LRU evicts later under memory pressure). */
        pthread_rwlock_unlock(&g_bm_cache[bm->slot].rwlock);
    } else {
        /* Uncached fallback: tear the mapping down per call. */
        if (bm->mmap_ptr && bm->mmap_size > 0) munmap(bm->mmap_ptr, bm->mmap_size);
        if (bm->fd >= 0) close(bm->fd);
    }
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

    /* Enforce the per-file cardinality contract. Past `max_values`,
       bitmap isn't the right index for this dataset — btree is, or the
       operator can declare a higher cap at create-object via
       `field:bitmap(N)`. The wire layer translates this -1 into an
       actionable error pointing them at the override. */
    if (old_n >= bm->hdr.max_values) return -1;

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
