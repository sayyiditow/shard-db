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
#include "types.h"
#include "bitmap.h"
#include <errno.h>

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
/* BmCacheEntry moved to shard_db_internal.h; g_bm_cache* moved to ShardDb struct */

static int bm_next_pow2(int n) { int p = 1; while (p < n) p <<= 1; return p; }

/* Publication generation (see the contract beside durability_same_open_inode).
   Advanced by one on every successful publish rename; cache entries record
   the generation at which their open inode was validated. */
static _Atomic uint64_t g_bm_publish_generation = 1;

#ifdef TEST_BUILD
static _Atomic int g_bm_test_fail_close_count;
static _Atomic int g_bm_test_fail_invalidate_count;
void bm_test_fail_close_next(int count) {
    atomic_store_explicit(&g_bm_test_fail_close_count, count,
                          memory_order_release);
}
void bm_test_fail_invalidate_next(int count) {
    atomic_store_explicit(&g_bm_test_fail_invalidate_count, count,
                          memory_order_release);
}
void bm_test_fail_reset(void) {
    bm_test_fail_close_next(0);
    bm_test_fail_invalidate_next(0);
}

/* Consume one positive failure, or preserve a negative persistent failure. */
static int bm_test_consume_failure(_Atomic int *counter) {
    int current = atomic_load_explicit(counter, memory_order_acquire);
    while (current != 0) {
        if (current < 0) return 1;
        if (atomic_compare_exchange_weak_explicit(
                counter, &current, current - 1,
                memory_order_acq_rel, memory_order_acquire))
            return 1;
    }
    return 0;
}
#endif

void bm_cache_init(int cap) {
    if (g_bm_cache) return;
    if (cap < 16) cap = 16;
    g_bm_cache_slots = bm_next_pow2(cap * 2);
    g_bm_cache = calloc((size_t)g_bm_cache_slots, sizeof(BmCacheEntry));
    g_bm_cache_count = 0;
    for (int i = 0; i < g_bm_cache_slots; i++) {
        rwlock_init_writer_preferring(&g_bm_cache[i].rwlock);
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

/* Caller holds g_bm_cache_lock. The entry lock is acquired without holding
   the table mutex, then identity is rechecked after the mutex is restored. */
static int bm_cache_drop_slot(int slot, int wait) {
    BmCacheEntry *e = &g_bm_cache[slot];
    if (!e->used) return 1;
    char expected_path[PATH_MAX];
    snprintf(expected_path, sizeof(expected_path), "%s", e->path);
    pthread_mutex_unlock(&g_bm_cache_lock);
    int lock_rc = wait ? pthread_rwlock_wrlock(&e->rwlock)
                       : pthread_rwlock_trywrlock(&e->rwlock);
    pthread_mutex_lock(&g_bm_cache_lock);
    if (lock_rc != 0) return 0;
    if (!e->used) {
        pthread_rwlock_unlock(&e->rwlock);
        return 1;
    }
    if (strcmp(e->path, expected_path) != 0) {
        pthread_rwlock_unlock(&e->rwlock);
        return 0;
    }
    if (e->map && e->map_size > 0 &&
        durability_flush_dirty(&e->dirty, &e->dirty_since_ms,
                               e->map, e->map_size) < 0) {
        pthread_rwlock_unlock(&e->rwlock);
        return -1;
    }
    /* A failed munmap leaves a live mapping owned by this entry. Preserve the
       complete entry so a later invalidation can safely retry teardown. */
    if (e->map && munmap(e->map, e->map_size) != 0) {
        int saved_errno = errno;
        pthread_rwlock_unlock(&e->rwlock);
        errno = saved_errno;
        return -1;
    }
    e->map = NULL;
    e->map_size = 0;
    int saved_errno = 0;
    if (e->fd >= 0 && close(e->fd) != 0) saved_errno = errno;
    e->fd = -1;
    atomic_store_explicit(&e->dirty, 0, memory_order_relaxed);
    atomic_store_explicit(&e->dirty_since_ms, 0, memory_order_relaxed);
    atomic_store_explicit(&e->validated_publish_generation, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&e->used, 0, memory_order_relaxed);
    e->path[0] = '\0';
    g_bm_cache_count--;
    pthread_rwlock_unlock(&e->rwlock);
    if (saved_errno) {
        errno = saved_errno;
        return -1;
    }
    return 1;
}

int bm_cache_invalidate_checked(const char *path) {
    if (!g_bm_cache) return 0;
    int rc = 0;
    pthread_mutex_lock(&g_bm_cache_lock);
    int found = 0;
    int slot = bm_cache_probe(path, &found);
    if (found && slot >= 0) {
        /* Structural discard after the path has been unlinked/recreated;
           identity is rechecked after taking the entry wrlock. */
        rc = bm_cache_drop_slot(slot, 1);
    }
    pthread_mutex_unlock(&g_bm_cache_lock);
    if (rc < 0) return -1;
#ifdef TEST_BUILD
    if (bm_test_consume_failure(&g_bm_test_fail_invalidate_count)) {
        errno = EIO;
        return -1;
    }
#endif
    return 0;
}

void bm_cache_invalidate(const char *path) {
    (void)bm_cache_invalidate_checked(path);
}

/* Publication contract (replaces the removed global publication gate):
   publication never holds a global lock and never blocks on a live target
   cache entry. A successful rename advances g_bm_publish_generation before
   publication returns. Cache entries record the generation at which their
   open inode was validated; the first acquire after a generation change
   compares the cached fd's (st_dev, st_ino) with the current path. A
   mismatched entry is retired non-blockingly when possible; otherwise that
   acquire opens the current path uncached. An acquire overlapping
   publication may finish on the old inode, but an acquire beginning after
   publication completes cannot. The only remaining lock order is
   g_bm_cache_lock -> per-entry rwlock (never the reverse). */

/* Non-blocking cache invalidation for `path`: detaches the entry only if
   nobody holds it. Returns 1 when detached, 0 when absent/busy, and -1 on a
   real cleanup error. Never waits on a cache-entry rwlock. */
static int bm_cache_invalidate_nowait(const char *path) {
    if (!g_bm_cache) return 0;
    pthread_mutex_lock(&g_bm_cache_lock);
    int found = 0;
    int slot = bm_cache_probe(path, &found);
    if (found && slot >= 0) {
        int rc = bm_cache_drop_slot(slot, 0);
        pthread_mutex_unlock(&g_bm_cache_lock);
        return rc;
    }
    pthread_mutex_unlock(&g_bm_cache_lock);
    return 0;
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

/* Advance the publication generation and retire a stale target cache entry
   without ever blocking on it. Runs immediately after the rename (via
   durability_publish_replace), before the parent-directory fsync. */
static void bm_after_rename(const char *target, void *ctx) {
    (void)ctx;
    atomic_fetch_add_explicit(&g_bm_publish_generation, 1,
                              memory_order_acq_rel);
    (void)bm_cache_invalidate_nowait(target);
}

bm_publish_result bm_publish_replace(const char *target, const char *tmp_path) {
    int publish_rc;
    char parent[PATH_MAX];
    if (parent_dir_copy(target, parent, sizeof(parent)) != 0)
        return BM_PUBLISH_PRE_RENAME_FAILED;
    /* The generated temporary is never retained by a caller, so blocking
       invalidation of only the temp path is safe; the live target is
       retired non-blockingly in bm_after_rename. */
    if (bm_cache_invalidate_checked(tmp_path) != 0)
        return BM_PUBLISH_PRE_RENAME_FAILED;
    durability_test_pause(parent, "bm-publish-before-rename");
    publish_rc = durability_publish_replace(target, tmp_path,
                                            bm_after_rename, NULL);
    if (publish_rc < 0) return BM_PUBLISH_PRE_RENAME_FAILED;
    if (publish_rc > 0) return BM_PUBLISH_POST_RENAME_FSYNC_FAILED;
    return BM_PUBLISH_OK;
}

static int bm_mkdir_p(const char *path) {
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
            *p = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
    return 0;
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
    const uint8_t *end = (const uint8_t *)bm->mmap_ptr + bm->hdr.bitmaps_off;
    uint32_t off = 0;
    for (uint32_t i = 0; i < bm->hdr.n_values; i++) {
        /* n_values is an on-disk header field with no inherent bound;
           mirror bm_dict_lookup's bounds-checked walk instead of trusting
           it blindly (CID 1696403). A corrupted/oversized n_values now
           just truncates the walk at the mapped region's edge instead of
           reading past it. */
        if (p + off + 2 > end) break;
        uint16_t len = (uint16_t)p[off] | ((uint16_t)p[off + 1] << 8);
        if (p + off + 2 + len > end) break;
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

static BitmapShard *bm_open_impl(const char *path, int slots, int create,
                                 int bool_fastpath, uint32_t max_values,
                                 int writer) {
    if (writer && !g_bm_cache) {
        errno = ENODEV;
        return NULL;
    }

    /* Ensure parent dir + the on-disk file exists if creation was asked. */
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) { *slash = '\0'; if (bm_mkdir_p(dir) != 0) return NULL; }
    {
        struct stat st;
        if (stat(path, &st) != 0) {
            if (!create) return NULL;
            bm_cache_invalidate(path); /* evict stale entry if file was unlinked+recreated */
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
retry_bm_acquire:;
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
            /* Confirmed hit. A publication may have completed since this
               entry's inode was validated; serve the entry only if its
               inode still matches the current path. */
            uint64_t current_generation = atomic_load_explicit(
                &g_bm_publish_generation, memory_order_acquire);
            uint64_t validated_generation = atomic_load_explicit(
                &e->validated_publish_generation, memory_order_acquire);
            if (validated_generation != current_generation) {
                if (!durability_same_open_inode(e->fd, path)) {
                    /* Stale target: retire it non-blockingly, never waiting
                       on its entry lock. If it is busy, serve the current
                       path from a fresh uncached mapping. */
                    pthread_rwlock_unlock(lock);
                    if (bm_cache_invalidate_nowait(path) > 0)
                        goto retry_bm_acquire;
                    int nfd; uint8_t *nmap; size_t nsz; struct BmHeader nhdr;
                    if (bm_file_open_mmap(path, &nfd, &nmap, &nsz, &nhdr) != 0)
                        return NULL;
                    BitmapShard *nbm = calloc(1, sizeof(*nbm));
                    if (!nbm) { munmap(nmap, nsz); close(nfd); return NULL; }
                    nbm->slot = -1;
                    nbm->writer = writer;
                    nbm->fd = nfd;
                    nbm->mmap_ptr = nmap;
                    nbm->mmap_size = nsz;
                    nbm->hdr = nhdr;
                    snprintf(nbm->path, sizeof(nbm->path), "%s", path);
                    if (nbm->hdr.max_values == 0 && writer) {
                        nbm->hdr.max_values = BM_DEFAULT_MAX_VALUES;
                        memcpy(nbm->mmap_ptr, &nbm->hdr, sizeof(struct BmHeader));
                    }
                    return nbm;
                }
                atomic_store_explicit(&e->validated_publish_generation,
                                      current_generation, memory_order_release);
            }
            /* Hand the rwlock + cached map to caller. bm_close() is the
               matched unlock. The verify-retry loop above already
               eliminates the evict-during-rwlock-wait window (used +
               path re-checked under rwlock), so slot stability here is
               guaranteed by the rwlock hold, mirroring bt_acquire_impl
               (see btree.c:838-851).
               coverity[missing_unlock] rwlock handoff to caller is intentional
               coverity[atomicity] slot stability guaranteed by rwlock + verify */
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

    /* Cache-miss path: load from disk + install into the slot. Capture the
       publication generation immediately before opening the pathname — not
       at cache-install time: a reader can open the old inode, lose the race
       to rename, and install only afterwards. Loading the generation at
       install would falsely bless that old inode as current. */
    int fd; uint8_t *map; size_t sz; struct BmHeader hdr;
    uint64_t opened_generation = atomic_load_explicit(
        &g_bm_publish_generation, memory_order_acquire);
    if (bm_file_open_mmap(path, &fd, &map, &sz, &hdr) != 0) {
        pthread_mutex_unlock(&g_bm_cache_lock);
        return NULL;
    }

    if (slot < 0 || g_bm_cache_count >= g_bm_cache_slots / 2) {
        slot = -1;
        int first_error = 0;
        int wait_candidate = -1;
        uint64_t floor_ts = 0;
        for (int attempt = 0; attempt < g_bm_cache_slots; attempt++) {
            int lru = -1;
            uint64_t oldest = UINT64_MAX;
            for (int i = 0; i < g_bm_cache_slots; i++) {
                if (g_bm_cache[i].used &&
                    g_bm_cache[i].last_access >= floor_ts &&
                    g_bm_cache[i].last_access < oldest) {
                    oldest = g_bm_cache[i].last_access;
                    lru = i;
                }
            }
            if (lru < 0) break;
            int drop_rc = bm_cache_drop_slot(lru, 0);
            if (drop_rc > 0) {
                slot = lru;
                break;
            }
            if (drop_rc < 0 && first_error == 0) first_error = errno;
            if (drop_rc == 0 && wait_candidate < 0) wait_candidate = lru;
            floor_ts = oldest + 1;
        }
        if (slot < 0 && writer && wait_candidate >= 0) {
            int drop_rc = bm_cache_drop_slot(wait_candidate, 1);
            if (drop_rc > 0) slot = wait_candidate;
            else if (drop_rc < 0 && first_error == 0) first_error = errno;
            else if (drop_rc == 0) first_error = 0;
        }
        if (slot < 0 && writer && first_error != 0) {
            pthread_mutex_unlock(&g_bm_cache_lock);
            munmap(map, sz);
            close(fd);
            errno = first_error;
            return NULL;
        }
    }

    if (slot < 0 && writer) {
        pthread_mutex_unlock(&g_bm_cache_lock);
        munmap(map, sz);
        close(fd);
        goto retry_bm_acquire;
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
    atomic_store_explicit(&e->dirty, 0, memory_order_relaxed);
    atomic_store_explicit(&e->dirty_since_ms, 0, memory_order_relaxed);
    atomic_store_explicit(&e->validated_publish_generation,
                          opened_generation, memory_order_release);
    atomic_store_explicit(&e->used, 1, memory_order_relaxed);
    e->last_access = __atomic_add_fetch(&g_bm_cache_clock, 1, __ATOMIC_RELAXED);
    g_bm_cache_count++;
    pthread_rwlock_t *lock = &e->rwlock;
    pthread_mutex_unlock(&g_bm_cache_lock);

    /* Take the per-entry rwlock AFTER releasing the table mutex — same
       M0-then-M1 ordering as the cache-hit path above, so a per-entry
       rwlock never nests inside g_bm_cache_lock. A caller can park a rwlock
       across a long-lived handle and separately need g_bm_cache_lock for an
       unrelated slot; nesting the other way risks a lock-order inversion
       against that. Verify-and-retry exactly like the hit path handles the
       resulting window where a concurrent evictor can steal this slot
       before we lock it. */
    if (writer) pthread_rwlock_wrlock(lock);
    else        pthread_rwlock_rdlock(lock);
    if (!e->used || strcmp(e->path, path) != 0) {
        /* Stolen by a concurrent evictor; they now own disposing our
           fd/map. Retry from scratch. */
        pthread_rwlock_unlock(lock);
        free(bm);
        goto retry_bm_acquire;
    }

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
    /* Hand the rwlock + freshly-opened map to caller. bm_close() is the
       matched unlock, mirroring bt_acquire_impl's cache-miss-fill return
       (see btree.c:1056-1058).
       coverity[missing_unlock] rwlock handoff to caller is intentional
       coverity[atomicity] slot stability guaranteed by rwlock + verify */
    return bm;
}

BitmapShard *bm_open(const char *path, int slots, int create,
                     int bool_fastpath, uint32_t max_values, int writer) {
    /* Public/private acquire name — direct call into the implementation.
       The removed global publication gate used to wrap this; cache
       visibility is now enforced by the publication generation + inode
       validation described above bm_open_impl. */
    return bm_open_impl(path, slots, create, bool_fastpath, max_values, writer);
}

uint32_t bm_max_values(const BitmapShard *bm) {
    return bm ? bm->hdr.max_values : 0;
}

#ifdef TEST_BUILD
#include <stdatomic.h>
static _Atomic int g_test_bm_sync_count;
void bm_test_sync_reset(void) { atomic_store(&g_test_bm_sync_count, 0); }
int  bm_test_sync_count(void) { return atomic_load(&g_test_bm_sync_count); }
#endif

int bm_sync(BitmapShard *bm) {
    if (!bm || !bm->writer || bm->fd < 0) { errno = EINVAL; return -1; }
#ifdef TEST_BUILD
    atomic_fetch_add(&g_test_bm_sync_count, 1);
#endif
    return fdatasync(bm->fd);
}

int bm_close_checked(BitmapShard *bm) {
    if (!bm) return 0;
    int saved_errno = 0;
    if (bm->slot >= 0 && g_bm_cache) {
        /* Cached: release the rwlock — the cache keeps the mmap + fd
           alive across releases (LRU evicts later under memory pressure). */
        if (bm->writer) {
            BmCacheEntry *e = &g_bm_cache[bm->slot];
            durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
        }
        int rc = pthread_rwlock_unlock(&g_bm_cache[bm->slot].rwlock);
        if (rc != 0) saved_errno = rc;
    } else {
        /* Uncached fallback: tear the mapping down per call. */
        if (bm->mmap_ptr && bm->mmap_size > 0 &&
            munmap(bm->mmap_ptr, bm->mmap_size) != 0)
            saved_errno = errno;
        if (bm->fd >= 0 && close(bm->fd) != 0 && !saved_errno)
            saved_errno = errno;
    }
    free(bm);
    if (saved_errno) {
        errno = saved_errno;
        return -1;
    }
#ifdef TEST_BUILD
    if (bm_test_consume_failure(&g_bm_test_fail_close_count)) {
        errno = EIO;
        return -1;
    }
#endif
    return 0;
}

void bm_close(BitmapShard *bm) {
    (void)bm_close_checked(bm);
}

/* ─────────────────────── set / clear / test ─────────────────────── */

int bm_dict_would_exceed_cap(BitmapShard *bm, const uint8_t *value, size_t vlen) {
    if (!bm) { errno = EINVAL; return -1; }
    if (vlen > 0xffff) return -1;
    if (bm_dict_lookup(bm, value, vlen) >= 0) return 0; /* already present */
    return (bm->hdr.n_values >= bm->hdr.max_values) ? 1 : 0;
}

int bm_dict_contains(BitmapShard *bm, const uint8_t *value, size_t vlen) {
    if (!bm) return 0;
    return bm_dict_lookup(bm, value, vlen) >= 0;
}

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
    /* Defense-in-depth: the dict's uint16 length-prefix format can't
       represent a value longer than 65535 bytes. Today every real caller
       already stays under this via varchar's own 65535-byte on-disk
       ceiling, but don't rely on that coincidence holding for future
       field types (CID 1696430). */
    if (vlen > 0xffff) return -1;

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

/* Return pointer to the bit-array for `value`, or NULL if not found.
   Sets *out_stride to the bitmap stride.  Used by word-level AND
   intersect popcount so callers outside bitmap.c don't need to reach
   into the opaque BitmapShard or call the static bm_dict_lookup. */
const uint8_t *bm_get_value_bitmap(BitmapShard *bm, const uint8_t *value,
                                    size_t vlen, uint32_t *out_stride) {
    if (!bm || !value || !out_stride) return NULL;
    int vidx = bm_dict_lookup(bm, value, vlen);
    if (vidx < 0) return NULL;
    *out_stride = bm->hdr.stride;
    return (const uint8_t *)bm->mmap_ptr + bm->hdr.bitmaps_off +
           (size_t)vidx * (size_t)bm->hdr.stride;
}

/* Iterate every (value, vlen) pair in the dictionary. Walks the on-disk
   layout described at the top of this file; respects the bool fast-path
   which doesn't have an explicit dict region. Used by the planner's
   generic dict-scan dispatch — for ops without a direct popcount path
   (range / LIKE / CONTAINS / REGEX / len_*) we evaluate the criterion
   against each decoded dict value and union the matching value-bitmaps,
   so the operator still routes through the index instead of falling
   back to a full data-shard scan. */
int bm_iter_values(const BitmapShard *bm,
                   int (*cb)(const uint8_t *value, size_t vlen, void *ctx),
                   void *ctx) {
    if (!bm || !cb) return 0;
    if (bm->hdr.flags & BM_FLAG_BOOL_FASTPATH) {
        /* Hardcoded bool dict: 0x00, 0x01. */
        uint8_t v0 = 0x00, v1 = 0x01;
        if (cb(&v0, 1, ctx) != 0) return 1;
        if (cb(&v1, 1, ctx) != 0) return 2;
        return 2;
    }
    const uint8_t *p = (const uint8_t *)bm->mmap_ptr + bm->hdr.dict_off;
    const uint8_t *end = (const uint8_t *)bm->mmap_ptr + bm->hdr.bitmaps_off;
    int n = 0;
    for (uint32_t i = 0; i < bm->hdr.n_values; i++) {
        if (p + 2 > end) break;
        uint16_t len = (uint16_t)p[0] | ((uint16_t)p[1] << 8);
        if (p + 2 + len > end) break;
        n++;
        if (cb(p + 2, len, ctx) != 0) return n;
        p += 2 + len;
    }
    return n;
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
