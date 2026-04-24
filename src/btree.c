/*
 * B+ Tree Index Implementation
 * Page-based, mmap'd, variable-length entries.
 * Like MapDB TreeSet: entry = value_bytes + 16-byte raw key hash.
 */

#define _GNU_SOURCE
#include "btree.h"
int bt_page_size = 4096;

/* Monitoring counters (defined in config.c, updated atomically). */
extern uint64_t g_bt_cache_hits;
extern uint64_t g_bt_cache_misses;
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <pthread.h>
#include <limits.h>

/* ========== Page helpers ========== */

/* Slot directory starts right after page header */
static inline uint16_t *page_slots(uint8_t *page) {
    return (uint16_t *)(page + sizeof(BtPageHeader));
}

/* Get entry pointer within a page */
static inline uint8_t *page_entry(uint8_t *page, int slot_idx) {
    return page + page_slots(page)[slot_idx];
}

/* Entry layout:
 *   Internal: [uint16 data_len][value_bytes][child_id:4]   data_len = vlen + 4
 *   Leaf:     [uint16 data_len][uint8 prefix_len][suffix][hash:16]
 *             data_len = 1 + suffix_len + 16; full_value = prev[0:prefix_len] + suffix
 *             Every BT_LEAF_RESTART_K-th slot is an anchor (prefix_len=0). */
/* Entry data_len. High bit is the leaf-tombstone flag; mask it out for size. */
static inline uint16_t entry_data_len(uint8_t *entry) {
    return *(uint16_t *)entry & 0x7FFF;
}

/* Leaf-only: tombstone flag (high bit of the uint16_t data_len field). */
static inline int leaf_entry_is_tomb(uint8_t *entry) {
    return (*(uint16_t *)entry & 0x8000) != 0;
}
static inline void leaf_entry_set_tomb(uint8_t *entry) {
    *(uint16_t *)entry |= 0x8000;
}

/* Internal-page entry helpers (flat format) */
static inline const char *int_entry_value(uint8_t *entry) {
    return (const char *)(entry + 2);
}
static inline size_t int_entry_vlen(uint8_t *entry) {
    return (size_t)entry_data_len(entry) - 4;
}
static inline uint32_t entry_child(uint8_t *entry) {
    uint16_t dlen = entry_data_len(entry);
    uint32_t cid;
    memcpy(&cid, entry + 2 + dlen - 4, 4);
    return cid;
}

/* Leaf-page entry helpers (compressed format) — only give raw bytes; full
   value requires sequential decode (LeafIter) or random access (leaf_read_slot). */
static inline uint8_t leaf_entry_prefix_len(uint8_t *entry) { return entry[2]; }
static inline size_t leaf_entry_suffix_len(uint8_t *entry) {
    return (size_t)entry_data_len(entry) - 1 - BT_HASH_SIZE;
}
static inline const uint8_t *leaf_entry_suffix(uint8_t *entry) { return entry + 3; }
static inline const uint8_t *leaf_entry_hash(uint8_t *entry) {
    return entry + 2 + (size_t)entry_data_len(entry) - BT_HASH_SIZE;
}

/* Encoded size of a leaf entry with given suffix_len (including 2-byte len field). */
static inline size_t leaf_entry_bytes(size_t suffix_len) {
    return 2 + 1 + suffix_len + BT_HASH_SIZE;
}

/* Space available on a page for new entries */
static size_t page_free_space(uint8_t *page) {
    BtPageHeader *ph = (BtPageHeader *)page;
    size_t slots_end = sizeof(BtPageHeader) + (ph->count + 1) * sizeof(uint16_t);
    if (ph->data_end <= slots_end) return 0;
    return ph->data_end - slots_end;
}

/* memcmp-with-length-tiebreak value comparison */
static inline int val_cmp(const void *v1, size_t l1, const void *v2, size_t l2) {
    size_t c = l1 < l2 ? l1 : l2;
    int r = memcmp(v1, v2, c);
    if (r != 0) return r;
    if (l1 < l2) return -1;
    if (l1 > l2) return 1;
    return 0;
}

/* Longest common prefix length (capped at 255 to fit in uint8_t prefix_len). */
static size_t common_prefix_len(const char *a, size_t la, const char *b, size_t lb) {
    size_t m = la < lb ? la : lb;
    if (m > 255) m = 255;
    size_t i = 0;
    while (i < m && a[i] == b[i]) i++;
    return i;
}

/* ========== Leaf iterator (sequential decode) ========== */

typedef struct {
    uint8_t *page;
    int slot_idx;       /* current slot (-1 before first next()) */
    int count;
    char key_buf[BT_MAX_VAL_LEN];
    size_t key_len;
    const uint8_t *hash; /* valid after next() returns 1 */
} LeafIter;

static void leaf_iter_init(LeafIter *it, uint8_t *page) {
    it->page = page;
    it->slot_idx = -1;
    it->count = ((BtPageHeader *)page)->count;
    it->key_len = 0;
    it->hash = NULL;
}

/* Advance to next non-tombstoned slot. Tombstones are still decoded into key_buf
   (so the prefix chain stays valid for following entries) but not returned. */
static int leaf_iter_next(LeafIter *it) {
    while (1) {
        int next = it->slot_idx + 1;
        if (next >= it->count) return 0;
        uint8_t *e = page_entry(it->page, next);
        uint8_t prefix_len = leaf_entry_prefix_len(e);
        size_t suffix_len = leaf_entry_suffix_len(e);
        if ((next & (BT_LEAF_RESTART_K - 1)) == 0) {
            it->key_len = suffix_len;
            if (it->key_len > BT_MAX_VAL_LEN) it->key_len = BT_MAX_VAL_LEN;
            memcpy(it->key_buf, leaf_entry_suffix(e), it->key_len);
        } else {
            size_t klen = (size_t)prefix_len + suffix_len;
            if (klen > BT_MAX_VAL_LEN) klen = BT_MAX_VAL_LEN;
            memcpy(it->key_buf + prefix_len, leaf_entry_suffix(e), suffix_len);
            it->key_len = klen;
        }
        it->hash = leaf_entry_hash(e);
        it->slot_idx = next;
        if (!leaf_entry_is_tomb(e)) return 1;
        /* Tombstoned — key_buf updated for chain; loop to next slot. */
    }
}

/* Seek to (or past) a physical slot; decodes from nearest anchor forward.
   Returns 1 if positioned on a live (non-tombstoned) slot at-or-past target_slot,
   0 if no live slot exists at-or-past target. */
static int leaf_iter_seek(LeafIter *it, int target_slot) {
    if (target_slot < 0 || target_slot >= it->count) return 0;
    int anchor = target_slot & ~(BT_LEAF_RESTART_K - 1);
    it->slot_idx = anchor - 1;
    do {
        if (!leaf_iter_next(it)) return 0;
    } while (it->slot_idx < target_slot);
    return 1;
}

/* ========== Binary search ========== */

static int page_bsearch_leaf(uint8_t *page, const char *target, size_t tlen) {
    BtPageHeader *ph = (BtPageHeader *)page;
    int n = ph->count;
    if (n == 0) return 0;
    int K = BT_LEAF_RESTART_K;
    int n_anchors = (n + K - 1) / K;

    /* Stage 1: bsearch over anchors. Anchors have prefix_len=0 so suffix is full value. */
    int lo = 0, hi = n_anchors;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        uint8_t *e = page_entry(page, mid * K);
        const uint8_t *av = leaf_entry_suffix(e);
        size_t al = leaf_entry_suffix_len(e);
        if (val_cmp(av, al, target, tlen) < 0) lo = mid + 1;
        else hi = mid;
    }

    /* lo = first anchor index >= target (or n_anchors if all anchors < target). */
    if (lo == 0) return 0; /* target <= first anchor → insertion at 0 */

    int start = (lo - 1) * K;
    int end = lo * K;
    if (end > n) end = n;

    /* Stage 2: linear decode within group [start, end), find first slot >= target. */
    LeafIter it;
    leaf_iter_init(&it, page);
    it.slot_idx = start - 1;
    while (leaf_iter_next(&it) && it.slot_idx < end) {
        if (val_cmp(it.key_buf, it.key_len, target, tlen) >= 0) return it.slot_idx;
    }
    return end;
}

static int page_bsearch_internal(uint8_t *page, const char *target, size_t tlen) {
    BtPageHeader *ph = (BtPageHeader *)page;
    int lo = 0, hi = ph->count;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        uint8_t *e = page_entry(page, mid);
        if (val_cmp(int_entry_value(e), int_entry_vlen(e), target, tlen) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

/* Binary search within a page. Returns index of first entry >= target. */
static int page_bsearch(uint8_t *page, const char *target, size_t target_len) {
    BtPageHeader *ph = (BtPageHeader *)page;
    if (ph->page_type == 1) return page_bsearch_leaf(page, target, target_len);
    return page_bsearch_internal(page, target, target_len);
}

/* ========== File management ========== */

typedef struct {
    int fd;
    uint8_t *map;
    size_t map_size;
    int cache_slot;  /* -1 = uncached (overflow or write-mode), else hashmap slot */
} BtFile;

/* ========== Read-only B+ tree mmap cache ==========
   Open-addressing hashmap keyed by path. Single mutex, atomic lookup-or-insert.
   No LRU: entries only leave on btree_cache_invalidate (write) or shutdown.
   On overflow, serves uncached (cache_slot=-1) and the caller munmaps directly. */

typedef struct {
    char     path[PATH_MAX];
    uint8_t *map;
    size_t   map_size;
    int      refcount;
    int      stale;
} BtCacheEntry;

static BtCacheEntry    *bt_cache = NULL;
static int              bt_cache_slots = 0;  /* power of 2 */
static int              bt_cache_count = 0;
static pthread_mutex_t  bt_cache_lock = PTHREAD_MUTEX_INITIALIZER;

static int bt_next_pow2(int n) { int p = 1; while (p < n) p <<= 1; return p; }

void bt_cache_init(int cap) {
    if (bt_cache) return;
    if (cap < 16) cap = 16;
    bt_cache_slots = bt_next_pow2(cap * 2);
    bt_cache = calloc(bt_cache_slots, sizeof(BtCacheEntry));
    bt_cache_count = 0;
}

void bt_cache_shutdown(void) {
    pthread_mutex_lock(&bt_cache_lock);
    if (bt_cache) {
        for (int i = 0; i < bt_cache_slots; i++) {
            if (bt_cache[i].path[0] && bt_cache[i].map)
                munmap(bt_cache[i].map, bt_cache[i].map_size);
        }
        free(bt_cache);
        bt_cache = NULL;
        bt_cache_slots = 0;
        bt_cache_count = 0;
    }
    pthread_mutex_unlock(&bt_cache_lock);
}

/* djb2 — no xxhash dep in btree.c */
static uint32_t bt_path_hash(const char *s) {
    uint32_t h = 5381;
    while (*s) h = h * 33 + (unsigned char)*s++;
    return h;
}

static int bt_cache_probe(const char *path, int *out_found) {
    uint32_t h = bt_path_hash(path);
    int mask = bt_cache_slots - 1;
    int idx = h & mask;
    for (int i = 0; i < bt_cache_slots; i++) {
        int s = (idx + i) & mask;
        if (bt_cache[s].path[0] == '\0') { *out_found = 0; return s; }
        if (!bt_cache[s].stale && strcmp(bt_cache[s].path, path) == 0) {
            *out_found = 1; return s;
        }
    }
    *out_found = 0;
    return -1;
}

static void bt_cache_drop_slot(int slot) {
    BtCacheEntry *e = &bt_cache[slot];
    if (e->map) munmap(e->map, e->map_size);
    memset(e, 0, sizeof(*e));
    bt_cache_count--;
    int mask = bt_cache_slots - 1;
    int i = (slot + 1) & mask;
    while (bt_cache[i].path[0]) {
        uint32_t h = bt_path_hash(bt_cache[i].path);
        int ideal = h & mask;
        int hole_dist = (i - slot) & mask;
        int entry_dist = (i - ideal) & mask;
        if (entry_dist >= hole_dist) {
            bt_cache[slot] = bt_cache[i];
            memset(&bt_cache[i], 0, sizeof(BtCacheEntry));
            slot = i;
        }
        i = (i + 1) & mask;
    }
}

static int bt_open_cached(BtFile *bt, const char *path) {
    pthread_mutex_lock(&bt_cache_lock);
    if (bt_cache) {
        int found = 0;
        int slot = bt_cache_probe(path, &found);
        if (found) {
            __atomic_add_fetch(&g_bt_cache_hits, 1, __ATOMIC_RELAXED);
            bt_cache[slot].refcount++;
            bt->fd = -1;
            bt->map = bt_cache[slot].map;
            bt->map_size = bt_cache[slot].map_size;
            bt->cache_slot = slot;
            pthread_mutex_unlock(&bt_cache_lock);
            return 0;
        }
        __atomic_add_fetch(&g_bt_cache_misses, 1, __ATOMIC_RELAXED);
        /* Miss — open under the lock so no two threads cold-open the same file. */
        int fd = open(path, O_RDONLY);
        if (fd < 0) { pthread_mutex_unlock(&bt_cache_lock); return -1; }
        struct stat st;
        if (fstat(fd, &st) < 0 || st.st_size == 0) { close(fd); pthread_mutex_unlock(&bt_cache_lock); return -1; }
        uint8_t *map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (map == MAP_FAILED) { pthread_mutex_unlock(&bt_cache_lock); return -1; }
        madvise(map, st.st_size, MADV_RANDOM);

        if (slot >= 0 && bt_cache_count < bt_cache_slots / 2) {
            BtCacheEntry *e = &bt_cache[slot];
            strncpy(e->path, path, PATH_MAX - 1);
            e->path[PATH_MAX - 1] = '\0';
            e->map = map;
            e->map_size = st.st_size;
            e->refcount = 1;
            e->stale = 0;
            bt_cache_count++;
            bt->fd = -1;
            bt->map = map;
            bt->map_size = st.st_size;
            bt->cache_slot = slot;
            pthread_mutex_unlock(&bt_cache_lock);
            return 0;
        }
        /* Overflow — uncached */
        pthread_mutex_unlock(&bt_cache_lock);
        bt->fd = -1;
        bt->map = map;
        bt->map_size = st.st_size;
        bt->cache_slot = -1;
        return 0;
    }
    pthread_mutex_unlock(&bt_cache_lock);

    /* Not initialized — direct mmap, cache_slot=-1 */
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size == 0) { close(fd); return -1; }
    uint8_t *map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) return -1;
    madvise(map, st.st_size, MADV_RANDOM);
    bt->fd = -1;
    bt->map = map;
    bt->map_size = st.st_size;
    bt->cache_slot = -1;
    return 0;
}

static void bt_close_cached(BtFile *bt) {
    if (!bt->map) return;
    if (bt->cache_slot < 0) {
        /* Uncached — munmap directly */
        munmap(bt->map, bt->map_size);
        bt->map = NULL;
        return;
    }
    pthread_mutex_lock(&bt_cache_lock);
    if (bt_cache && bt->cache_slot < bt_cache_slots &&
        bt_cache[bt->cache_slot].map == bt->map) {
        bt_cache[bt->cache_slot].refcount--;
        if (bt_cache[bt->cache_slot].stale && bt_cache[bt->cache_slot].refcount == 0) {
            bt_cache_drop_slot(bt->cache_slot);
        }
    }
    pthread_mutex_unlock(&bt_cache_lock);
    bt->map = NULL;
}

int bt_cache_stats(int *used_slots, int *total_slots, size_t *total_bytes) {
    int used = 0;
    size_t bytes = 0;
    pthread_mutex_lock(&bt_cache_lock);
    if (bt_cache) {
        for (int i = 0; i < bt_cache_slots; i++) {
            if (bt_cache[i].path[0] && bt_cache[i].map) {
                used++;
                bytes += bt_cache[i].map_size;
            }
        }
    }
    if (used_slots)  *used_slots  = used;
    if (total_slots) *total_slots = bt_cache_slots;
    if (total_bytes) *total_bytes = bytes;
    pthread_mutex_unlock(&bt_cache_lock);
    return 0;
}

/* Mark cache entry as stale — evicted when refcount drops to 0 */
void btree_cache_invalidate(const char *path) {
    pthread_mutex_lock(&bt_cache_lock);
    if (bt_cache) {
        int found = 0;
        int slot = bt_cache_probe(path, &found);
        if (found) {
            if (bt_cache[slot].refcount == 0) {
                bt_cache_drop_slot(slot);
            } else {
                bt_cache[slot].stale = 1;
            }
        }
    }
    pthread_mutex_unlock(&bt_cache_lock);
}

/* mode: 0=read-only (MAP_PRIVATE, no lock), 1=create/write (MAP_SHARED, LOCK_EX) */
static int bt_open(BtFile *bt, const char *path, int create) {
    int readonly = !create;
    int flags = readonly ? O_RDONLY : (O_RDWR | O_CREAT);
    bt->fd = open(path, flags, 0644);
    if (bt->fd < 0) return -1;

    if (!readonly) flock(bt->fd, LOCK_EX);

    struct stat st;
    fstat(bt->fd, &st);

    if (st.st_size == 0 && create) {
        size_t init_size = 2 * bt_page_size;
        ftruncate(bt->fd, init_size);
        bt->map = mmap(NULL, init_size, PROT_READ | PROT_WRITE, MAP_SHARED, bt->fd, 0);
        if (bt->map == MAP_FAILED) { flock(bt->fd, LOCK_UN); close(bt->fd); return -1; }
        bt->map_size = init_size;

        BtFileHeader *fh = (BtFileHeader *)bt->map;
        fh->magic = BT_MAGIC;
        fh->root_page = 1;
        fh->page_count = 2;
        fh->height = 1;
        fh->entry_count = 0;
        fh->key_type = 0;   /* FT_NONE — populated by typed bulk_build / caller */
        fh->key_signed = 0;

        uint8_t *leaf = bt->map + bt_page_size;
        memset(leaf, 0, bt_page_size);
        BtPageHeader *lh = (BtPageHeader *)leaf;
        lh->page_type = 1;
        lh->count = 0;
        lh->next_leaf = 0;
        lh->data_end = bt_page_size;
    } else {
        bt->map_size = st.st_size;
        int prot = readonly ? PROT_READ : (PROT_READ | PROT_WRITE);
        int mflags = readonly ? MAP_PRIVATE : MAP_SHARED;
        bt->map = mmap(NULL, bt->map_size, prot, mflags, bt->fd, 0);
        if (bt->map == MAP_FAILED) {
            if (!readonly) flock(bt->fd, LOCK_UN);
            close(bt->fd); return -1;
        }
    }
    return 0;
}

static void bt_close(BtFile *bt) {
    if (bt->map && bt->map != MAP_FAILED) {
        /* Trim file to actual page count (remove pre-allocated slack) */
        int flags = fcntl(bt->fd, F_GETFL);
        if (flags != -1 && (flags & O_ACCMODE) != O_RDONLY) {
            BtFileHeader *fh = (BtFileHeader *)bt->map;
            size_t actual = (size_t)fh->page_count * bt_page_size;
            if (actual < bt->map_size) ftruncate(bt->fd, actual);
            flock(bt->fd, LOCK_UN);
        }
        munmap(bt->map, bt->map_size);
    } else {
        int flags = fcntl(bt->fd, F_GETFL);
        if (flags != -1 && (flags & O_ACCMODE) != O_RDONLY)
            flock(bt->fd, LOCK_UN);
    }
    close(bt->fd);
}

/* Get page pointer */
static inline uint8_t *bt_page(BtFile *bt, uint32_t page_id) {
    return bt->map + (size_t)page_id * bt_page_size;
}

/* Allocate a new page. Returns page_id. Grows file in chunks to avoid per-page remap. */
static uint32_t bt_alloc_page(BtFile *bt) {
    BtFileHeader *fh = (BtFileHeader *)bt->map;
    uint32_t new_id = fh->page_count;
    size_t needed = (size_t)(new_id + 1) * bt_page_size;

    if (needed > bt->map_size) {
        /* Grow in chunks: double or add 1MB, whichever is larger */
        size_t new_size = bt->map_size * 2;
        if (new_size < bt->map_size + 1024 * 1024)
            new_size = bt->map_size + 1024 * 1024;
        if (new_size < needed) new_size = needed;
        munmap(bt->map, bt->map_size);
        ftruncate(bt->fd, new_size);
        bt->map = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, bt->fd, 0);
        bt->map_size = new_size;
        fh = (BtFileHeader *)bt->map;
    }

    fh->page_count = new_id + 1;

    uint8_t *pg = bt_page(bt, new_id);
    memset(pg, 0, bt_page_size);
    return new_id;
}

/* ========== Page insert / remove ========== */

/* LeafRec: materialized leaf entry used during page rebuild. */
typedef struct {
    const char *value;
    size_t vlen;
    const uint8_t *hash;
} LeafRec;

/* Rewrite a leaf page in place from a materialized record array.
   Preserves page_type=leaf and next_leaf; resets count/data_end/slots.
   Returns 0 on success, -1 if the records don't fit. */
static int leaf_rebuild(uint8_t *page, LeafRec *recs, int count) {
    BtPageHeader *ph = (BtPageHeader *)page;
    uint32_t next_leaf = ph->next_leaf;
    int K = BT_LEAF_RESTART_K;

    /* Pre-compute encoded size to validate fit. */
    size_t data_bytes = 0;
    for (int i = 0; i < count; i++) {
        size_t suffix_len;
        if ((i & (K - 1)) == 0) {
            suffix_len = recs[i].vlen;
        } else {
            size_t p = common_prefix_len(recs[i - 1].value, recs[i - 1].vlen,
                                         recs[i].value, recs[i].vlen);
            suffix_len = recs[i].vlen - p;
        }
        data_bytes += leaf_entry_bytes(suffix_len);
    }
    size_t slots_end = sizeof(BtPageHeader) + (size_t)count * sizeof(uint16_t);
    if (slots_end + data_bytes > (size_t)bt_page_size) return -1;

    ph->page_type = 1;
    ph->count = 0;
    ph->next_leaf = next_leaf;
    ph->data_end = bt_page_size;

    uint16_t *slots = page_slots(page);
    for (int i = 0; i < count; i++) {
        uint8_t prefix_len = 0;
        const char *suffix = recs[i].value;
        size_t suffix_len = recs[i].vlen;
        if ((i & (K - 1)) != 0) {
            size_t p = common_prefix_len(recs[i - 1].value, recs[i - 1].vlen,
                                         recs[i].value, recs[i].vlen);
            prefix_len = (uint8_t)p;
            suffix = recs[i].value + p;
            suffix_len = recs[i].vlen - p;
        }
        size_t entry_bytes = leaf_entry_bytes(suffix_len);
        ph->data_end -= entry_bytes;
        uint8_t *e = page + ph->data_end;
        uint16_t dlen = (uint16_t)(1 + suffix_len + BT_HASH_SIZE);
        *(uint16_t *)e = dlen;
        e[2] = prefix_len;
        memcpy(e + 3, suffix, suffix_len);
        memcpy(e + 3 + suffix_len, recs[i].hash, BT_HASH_SIZE);
        slots[i] = ph->data_end;
        ph->count++;
    }
    return 0;
}

/* Fast append-only leaf write: uses running last-key state. O(1), no decode.
   Returns 0 success, -1 if no space. */
static int leaf_append(uint8_t *page, const char *value, size_t vlen,
                       const uint8_t *hash, char *last_key, size_t *last_key_len) {
    BtPageHeader *ph = (BtPageHeader *)page;
    int i = ph->count;

    uint8_t prefix_len = 0;
    const char *suffix = value;
    size_t suffix_len = vlen;
    if ((i & (BT_LEAF_RESTART_K - 1)) != 0) {
        size_t p = common_prefix_len(last_key, *last_key_len, value, vlen);
        prefix_len = (uint8_t)p;
        suffix = value + p;
        suffix_len = vlen - p;
    }
    size_t entry_bytes = leaf_entry_bytes(suffix_len);
    size_t needed = entry_bytes + sizeof(uint16_t);
    if (page_free_space(page) < needed) return -1;

    ph->data_end -= entry_bytes;
    uint8_t *e = page + ph->data_end;
    uint16_t dlen = (uint16_t)(1 + suffix_len + BT_HASH_SIZE);
    *(uint16_t *)e = dlen;
    e[2] = prefix_len;
    memcpy(e + 3, suffix, suffix_len);
    memcpy(e + 3 + suffix_len, hash, BT_HASH_SIZE);
    page_slots(page)[i] = ph->data_end;
    ph->count++;

    memcpy(last_key, value, vlen);
    *last_key_len = vlen;
    return 0;
}

/* Insert leaf entry at position pos (pos is the physical slot index returned by
   page_bsearch_leaf — first non-tomb slot >= target). Decompresses, drops
   tombstones, inserts new entry, rebuilds. */
static int page_insert_at_leaf(uint8_t *page, int pos, const char *value, size_t vlen,
                               const uint8_t *hash) {
    BtPageHeader *ph = (BtPageHeader *)page;
    int old_count = ph->count;
    int cap = old_count + 1;

    /* Worst-case size: every entry up to BT_MAX_VAL_LEN after decompression. */
    size_t bufsz = (size_t)cap * BT_MAX_VAL_LEN + vlen;
    LeafRec *recs = malloc((size_t)cap * sizeof(LeafRec));
    char *buf = malloc(bufsz);
    uint8_t *hashbuf = malloc((size_t)cap * BT_HASH_SIZE);
    if (!recs || !buf || !hashbuf) { free(recs); free(buf); free(hashbuf); return -1; }

    LeafIter it;
    leaf_iter_init(&it, page);
    char *p = buf;
    uint8_t *h = hashbuf;
    int dst = 0;
    int inserted = 0;
    while (leaf_iter_next(&it)) {
        if (!inserted && it.slot_idx >= pos) {
            memcpy(p, value, vlen);
            memcpy(h, hash, BT_HASH_SIZE);
            recs[dst].value = p; recs[dst].vlen = vlen; recs[dst].hash = h;
            p += vlen; h += BT_HASH_SIZE; dst++;
            inserted = 1;
        }
        memcpy(p, it.key_buf, it.key_len);
        memcpy(h, it.hash, BT_HASH_SIZE);
        recs[dst].value = p; recs[dst].vlen = it.key_len; recs[dst].hash = h;
        p += it.key_len; h += BT_HASH_SIZE; dst++;
    }
    if (!inserted) {
        memcpy(p, value, vlen);
        memcpy(h, hash, BT_HASH_SIZE);
        recs[dst].value = p; recs[dst].vlen = vlen; recs[dst].hash = h;
        dst++;
    }

    int rc = leaf_rebuild(page, recs, dst);
    free(recs); free(buf); free(hashbuf);
    return rc;
}

/* Insert entry into a page at position pos. Returns 0 on success, -1 if no space.
   For leaves: suffix is 16-byte hash (suffix_len must be BT_HASH_SIZE).
   For internals: suffix is child_page_id (suffix_len must be 4). */
static int page_insert_at(uint8_t *page, int pos, const char *value, size_t vlen,
                          const void *suffix, size_t suffix_len) {
    BtPageHeader *ph = (BtPageHeader *)page;
    if (ph->page_type == 1) {
        return page_insert_at_leaf(page, pos, value, vlen, (const uint8_t *)suffix);
    }
    /* Internal — flat layout */
    uint16_t data_len = (uint16_t)(vlen + suffix_len);
    size_t entry_bytes = 2 + data_len;
    size_t needed = entry_bytes + sizeof(uint16_t);
    if (page_free_space(page) < needed) return -1;

    ph->data_end -= entry_bytes;
    uint8_t *entry = page + ph->data_end;
    *(uint16_t *)entry = data_len;
    memcpy(entry + 2, value, vlen);
    memcpy(entry + 2 + vlen, suffix, suffix_len);

    uint16_t *slots = page_slots(page);
    if (pos < (int)ph->count)
        memmove(&slots[pos + 1], &slots[pos], (ph->count - pos) * sizeof(uint16_t));
    slots[pos] = ph->data_end;
    ph->count++;
    return 0;
}

/* Remove entry at physical slot pos from a leaf page — O(1) tombstone.
   The slot stays in place so following entries' prefix-decode chain remains
   valid; LeafIter skips tombstones. Space is reclaimed on the next full rebuild
   (split, insert-that-triggers-rebuild, or bulk_merge). */
static void page_remove_at_leaf(uint8_t *page, int pos) {
    BtPageHeader *ph = (BtPageHeader *)page;
    if (pos < 0 || pos >= (int)ph->count) return;
    uint8_t *e = page_entry(page, pos);
    leaf_entry_set_tomb(e);
}

/* Remove entry at position pos. */
static void page_remove_at(uint8_t *page, int pos) {
    BtPageHeader *ph = (BtPageHeader *)page;
    if (ph->page_type == 1) { page_remove_at_leaf(page, pos); return; }
    /* Internal — lazy slot removal */
    uint16_t *slots = page_slots(page);
    if (pos < (int)ph->count - 1)
        memmove(&slots[pos], &slots[pos + 1], (ph->count - 1 - pos) * sizeof(uint16_t));
    ph->count--;
}

/* ========== Split ========== */

/* Split a leaf page. Returns new page_id. Promotes middle key via out params. */
static uint32_t bt_split_leaf(BtFile *bt, uint32_t page_id,
                              char *promote_val, size_t *promote_vlen) {
    uint32_t new_id = bt_alloc_page(bt);
    uint8_t *old_pg = bt_page(bt, page_id);
    uint8_t *new_pg = bt_page(bt, new_id);

    BtPageHeader *old_h = (BtPageHeader *)old_pg;
    int total = old_h->count;

    /* Init new page as leaf; inherits old's next_leaf. */
    BtPageHeader *new_h = (BtPageHeader *)new_pg;
    new_h->page_type = 1;
    new_h->count = 0;
    new_h->next_leaf = old_h->next_leaf;
    new_h->data_end = bt_page_size;

    /* Decode all non-tombstoned entries. Worst-case buffer: total * BT_MAX_VAL_LEN. */
    LeafRec *recs = malloc((size_t)total * sizeof(LeafRec));
    char *buf = malloc((size_t)total * BT_MAX_VAL_LEN);
    uint8_t *hashbuf = malloc((size_t)total * BT_HASH_SIZE);
    if (!recs || !buf || !hashbuf) {
        free(recs); free(buf); free(hashbuf);
        return page_id; /* fatal; caller treats as no-split */
    }

    LeafIter it;
    leaf_iter_init(&it, old_pg);
    char *p = buf;
    uint8_t *h = hashbuf;
    int live = 0;
    while (leaf_iter_next(&it)) {
        memcpy(p, it.key_buf, it.key_len);
        memcpy(h, it.hash, BT_HASH_SIZE);
        recs[live].value = p; recs[live].vlen = it.key_len; recs[live].hash = h;
        p += it.key_len; h += BT_HASH_SIZE; live++;
    }
    int split_at = live / 2;

    /* Promote first key of new page (recs[split_at]). */
    *promote_vlen = recs[split_at].vlen;
    if (*promote_vlen > BT_MAX_VAL_LEN) *promote_vlen = BT_MAX_VAL_LEN;
    memcpy(promote_val, recs[split_at].value, *promote_vlen);

    /* Rebuild new first (inherits old's next_leaf already). */
    leaf_rebuild(new_pg, &recs[split_at], live - split_at);
    /* Set old's next_leaf to new before rebuild so rebuild preserves it. */
    old_h->next_leaf = new_id;
    leaf_rebuild(old_pg, recs, split_at);

    free(recs); free(buf); free(hashbuf);
    return new_id;
}

/* Split an internal page. Similar to leaf split. */
static uint32_t bt_split_internal(BtFile *bt, uint32_t page_id,
                                  char *promote_val, size_t *promote_vlen) {
    uint32_t new_id = bt_alloc_page(bt);
    uint8_t *old_pg = bt_page(bt, page_id);
    uint8_t *new_pg = bt_page(bt, new_id);

    BtPageHeader *old_h = (BtPageHeader *)old_pg;
    int mid = old_h->count / 2;

    BtPageHeader *new_h = (BtPageHeader *)new_pg;
    new_h->page_type = 0;
    new_h->count = 0;
    new_h->next_leaf = 0;
    new_h->data_end = bt_page_size;

    /* The middle entry gets promoted. Its right child becomes leftmost of new page. */
    uint8_t *mid_entry = page_entry(old_pg, mid);
    *promote_vlen = int_entry_vlen(mid_entry);
    if (*promote_vlen > BT_MAX_VAL_LEN) *promote_vlen = BT_MAX_VAL_LEN;
    memcpy(promote_val, int_entry_value(mid_entry), *promote_vlen);
    new_h->next_leaf = entry_child(mid_entry); /* leftmost child of new page */

    for (int i = mid + 1; i < (int)old_h->count; i++) {
        uint8_t *e = page_entry(old_pg, i);
        size_t vlen = int_entry_vlen(e);
        uint32_t child = entry_child(e);
        page_insert_at(new_pg, new_h->count, int_entry_value(e), vlen, &child, 4);
    }

    old_h = (BtPageHeader *)bt_page(bt, page_id);
    old_h->count = mid;

    return new_id;
}

/* ========== Insert ========== */

/* Recursive insert. Returns -1 if no split, otherwise new_page_id and sets promote key. */
static int bt_insert_rec(BtFile *bt, uint32_t page_id,
                         const char *value, size_t vlen, const uint8_t *hash,
                         char *promote_val, size_t *promote_vlen, uint32_t *new_child) {
    uint8_t *page = bt_page(bt, page_id);
    BtPageHeader *ph = (BtPageHeader *)page;

    if (ph->page_type == 1) {
        /* Leaf page */
        int pos = page_bsearch(page, value, vlen);

        /* Check for duplicate */
        if (pos < (int)ph->count) {
            LeafIter dit; leaf_iter_init(&dit, page);
            if (leaf_iter_seek(&dit, pos) &&
                val_cmp(dit.key_buf, dit.key_len, value, vlen) == 0 &&
                memcmp(dit.hash, hash, BT_HASH_SIZE) == 0) {
                return -1; /* duplicate, skip */
            }
        }

        /* Try to insert */
        if (page_insert_at(page, pos, value, vlen, hash, BT_HASH_SIZE) == 0) {
            return -1; /* success, no split */
        }

        /* Page full — split */
        /* First insert into a temp buffer, then split */
        /* For simplicity: split first, then insert into correct half */
        *new_child = bt_split_leaf(bt, page_id, promote_val, promote_vlen);

        /* Determine which page to insert into */
        page = bt_page(bt, page_id); /* re-fetch */
        uint8_t *new_pg = bt_page(bt, *new_child);
        if (memcmp(value, promote_val, vlen < *promote_vlen ? vlen : *promote_vlen) >= 0 ||
            (vlen >= *promote_vlen && memcmp(value, promote_val, *promote_vlen) >= 0)) {
            pos = page_bsearch(new_pg, value, vlen);
            page_insert_at(new_pg, pos, value, vlen, hash, BT_HASH_SIZE);
        } else {
            pos = page_bsearch(page, value, vlen);
            page_insert_at(page, pos, value, vlen, hash, BT_HASH_SIZE);
        }

        return 0; /* split happened */
    } else {
        /* Internal page — find child.
           next_leaf stores leftmost child. Entries store [key, right_child].
           Find rightmost key <= value, follow its right child.
           If value < all keys, follow leftmost child. */
        int pos = page_bsearch(page, value, vlen);
        uint32_t child_id;

        if (pos == 0) {
            child_id = ph->next_leaf; /* leftmost child */
        } else {
            child_id = entry_child(page_entry(page, pos - 1));
        }

        char sub_promote[BT_MAX_VAL_LEN];
        size_t sub_promote_len;
        uint32_t sub_new_child;

        int result = bt_insert_rec(bt, child_id, value, vlen, hash,
                                   sub_promote, &sub_promote_len, &sub_new_child);
        if (result == -1) return -1; /* no split below */

        /* Child split — insert promoted key + new child into this page */
        page = bt_page(bt, page_id); /* re-fetch after potential remap */
        ph = (BtPageHeader *)page;
        int ipos = page_bsearch(page, sub_promote, sub_promote_len);

        if (page_insert_at(page, ipos, sub_promote, sub_promote_len,
                          &sub_new_child, 4) == 0) {
            return -1; /* inserted into this page, no further split */
        }

        /* This internal page is full — split it too */
        *new_child = bt_split_internal(bt, page_id, promote_val, promote_vlen);

        page = bt_page(bt, page_id);
        uint8_t *new_pg = bt_page(bt, *new_child);
        if (memcmp(sub_promote, promote_val,
                   sub_promote_len < *promote_vlen ? sub_promote_len : *promote_vlen) >= 0) {
            ipos = page_bsearch(new_pg, sub_promote, sub_promote_len);
            page_insert_at(new_pg, ipos, sub_promote, sub_promote_len, &sub_new_child, 4);
        } else {
            ipos = page_bsearch(page, sub_promote, sub_promote_len);
            page_insert_at(page, ipos, sub_promote, sub_promote_len, &sub_new_child, 4);
        }

        return 0; /* split propagated */
    }
}

/* ========== Public API ========== */

void btree_insert(const char *path, const char *value, size_t vlen,
                  const uint8_t hash[BT_HASH_SIZE]) {
    if (vlen > BT_MAX_VAL_LEN) return;
    btree_cache_invalidate(path);

    BtFile bt;
    if (bt_open(&bt, path, 1) != 0) return;

    BtFileHeader *fh = (BtFileHeader *)bt.map;
    char promote_val[BT_MAX_VAL_LEN];
    size_t promote_vlen;
    uint32_t new_child;

    int result = bt_insert_rec(&bt, fh->root_page, value, vlen, hash,
                               promote_val, &promote_vlen, &new_child);

    if (result == 0) {
        /* Root was split — create new root */
        fh = (BtFileHeader *)bt.map; /* re-fetch */
        uint32_t new_root = bt_alloc_page(&bt);
        fh = (BtFileHeader *)bt.map; /* re-fetch after alloc */
        uint8_t *root_pg = bt_page(&bt, new_root);

        BtPageHeader *rh = (BtPageHeader *)root_pg;
        rh->page_type = 0; /* internal */
        rh->count = 0;
        rh->next_leaf = 0;
        rh->data_end = bt_page_size;

        /* old_root is leftmost child (in next_leaf), promote key points to new_child */
        uint32_t old_root = fh->root_page;
        rh->next_leaf = old_root; /* leftmost child pointer */
        page_insert_at(root_pg, 0, promote_val, promote_vlen, &new_child, 4);

        fh->root_page = new_root;
        fh->height++;
    }

    fh = (BtFileHeader *)bt.map;
    fh->entry_count++;
    bt_close(&bt);
}

/* Batch insert — opens file once, inserts all entries, closes once.
   Much faster and safer than calling btree_insert N times. */
void btree_insert_batch(const char *path, BtEntry *entries, size_t count) {
    if (count == 0) return;
    btree_cache_invalidate(path);

    BtFile bt;
    if (bt_open(&bt, path, 1) != 0) return;

    for (size_t i = 0; i < count; i++) {
        if (entries[i].vlen > BT_MAX_VAL_LEN) continue;

        BtFileHeader *fh = (BtFileHeader *)bt.map;
        char promote_val[BT_MAX_VAL_LEN];
        size_t promote_vlen;
        uint32_t new_child;

        int result = bt_insert_rec(&bt, fh->root_page, entries[i].value, entries[i].vlen,
                                   entries[i].hash, promote_val, &promote_vlen, &new_child);
        if (result == 0) {
            fh = (BtFileHeader *)bt.map;
            uint32_t new_root = bt_alloc_page(&bt);
            fh = (BtFileHeader *)bt.map;
            uint8_t *root_pg = bt_page(&bt, new_root);
            BtPageHeader *rh = (BtPageHeader *)root_pg;
            rh->page_type = 0;
            rh->count = 0;
            rh->next_leaf = fh->root_page;
            rh->data_end = bt_page_size;
            page_insert_at(root_pg, 0, promote_val, promote_vlen, &new_child, 4);
            fh->root_page = new_root;
            fh->height++;
        }
        fh = (BtFileHeader *)bt.map;
        fh->entry_count++;
    }

    bt_close(&bt);
}

void btree_delete(const char *path, const char *value, size_t vlen,
                  const uint8_t hash[BT_HASH_SIZE]) {
    btree_cache_invalidate(path);
    BtFile bt;
    if (bt_open(&bt, path, 1) != 0) return; /* write mode — needs to modify pages */

    BtFileHeader *fh = (BtFileHeader *)bt.map;

    /* Traverse to leaf */
    uint32_t page_id = fh->root_page;
    while (1) {
        uint8_t *page = bt_page(&bt, page_id);
        BtPageHeader *ph = (BtPageHeader *)page;
        if (ph->page_type == 1) break; /* leaf */
        int pos = page_bsearch(page, value, vlen);
        if (pos == 0) page_id = ph->next_leaf;
        else page_id = entry_child(page_entry(page, pos - 1));
    }

    /* Search within leaf */
    uint8_t *page = bt_page(&bt, page_id);
    int pos = page_bsearch(page, value, vlen);

    LeafIter it;
    leaf_iter_init(&it, page);
    if (leaf_iter_seek(&it, pos)) {
        do {
            if (val_cmp(it.key_buf, it.key_len, value, vlen) != 0) break;
            if (memcmp(it.hash, hash, BT_HASH_SIZE) == 0) {
                page_remove_at(page, it.slot_idx);
                fh->entry_count--;
                break;
            }
        } while (leaf_iter_next(&it));
    }

    bt_close(&bt);
}

void btree_search(const char *path, const char *value, size_t vlen,
                  bt_result_cb cb, void *ctx) {
    BtFile bt;
    if (bt_open_cached(&bt, path) != 0) return;

    BtFileHeader *fh = (BtFileHeader *)bt.map;

    /* Traverse to leaf */
    uint32_t page_id = fh->root_page;
    while (1) {
        uint8_t *page = bt_page(&bt, page_id);
        BtPageHeader *ph = (BtPageHeader *)page;
        if (ph->page_type == 1) break;
        int pos = page_bsearch(page, value, vlen);
        if (pos == 0) page_id = ph->next_leaf;
        else page_id = entry_child(page_entry(page, pos - 1));
    }

    /* Scan leaf chain for all matching entries */
    while (page_id != 0) {
        uint8_t *page = bt_page(&bt, page_id);
        BtPageHeader *ph = (BtPageHeader *)page;
        int pos = page_bsearch(page, value, vlen);

        LeafIter it;
        leaf_iter_init(&it, page);
        if (leaf_iter_seek(&it, pos)) {
            do {
                if (val_cmp(it.key_buf, it.key_len, value, vlen) != 0) goto done;
                if (cb(it.key_buf, it.key_len, it.hash, ctx) < 0) goto done;
            } while (leaf_iter_next(&it));
        }
        page_id = ph->next_leaf;
    }
done:
    bt_close_cached(&bt);
}

void btree_range_ex(const char *path,
                    const char *min_val, size_t min_len, int min_exclusive,
                    const char *max_val, size_t max_len, int max_exclusive,
                    bt_result_cb cb, void *ctx) {
    BtFile bt;
    if (bt_open_cached(&bt, path) != 0) return;

    BtFileHeader *fh = (BtFileHeader *)bt.map;

    /* Traverse to leaf for min_val */
    uint32_t page_id = fh->root_page;
    while (1) {
        uint8_t *page = bt_page(&bt, page_id);
        BtPageHeader *ph = (BtPageHeader *)page;
        if (ph->page_type == 1) break;
        int pos = page_bsearch(page, min_val, min_len);
        if (pos == 0) page_id = ph->next_leaf;
        else page_id = entry_child(page_entry(page, pos - 1));
    }

    /* Scan leaf chain from min to max */
    while (page_id != 0) {
        uint8_t *page = bt_page(&bt, page_id);
        BtPageHeader *ph = (BtPageHeader *)page;
        int start = page_bsearch(page, min_val, min_len);

        LeafIter it;
        leaf_iter_init(&it, page);
        if (leaf_iter_seek(&it, start)) {
            do {
                int cmp_max = val_cmp(it.key_buf, it.key_len, max_val, max_len);
                if (cmp_max > 0) goto range_done;
                if (max_exclusive && cmp_max == 0) goto range_done;
                if (min_exclusive && val_cmp(it.key_buf, it.key_len, min_val, min_len) == 0)
                    continue;
                if (cb(it.key_buf, it.key_len, it.hash, ctx) < 0) goto range_done;
            } while (leaf_iter_next(&it));
        }
        page_id = ph->next_leaf;
    }
range_done:
    bt_close_cached(&bt);
}

void btree_range(const char *path,
                 const char *min_val, size_t min_len,
                 const char *max_val, size_t max_len,
                 bt_result_cb cb, void *ctx) {
    btree_range_ex(path, min_val, min_len, 0, max_val, max_len, 0, cb, ctx);
}

/* Descending range scan. See header for rationale. */
typedef struct {
    uint8_t key[BT_MAX_VAL_LEN];
    size_t  key_len;
    uint8_t hash[BT_HASH_SIZE];
} DescEntrySnap;

void btree_range_desc_ex(const char *path,
                         const char *min_val, size_t min_len, int min_exclusive,
                         const char *max_val, size_t max_len, int max_exclusive,
                         bt_result_cb cb, void *ctx) {
    BtFile bt;
    if (bt_open_cached(&bt, path) != 0) return;
    BtFileHeader *fh = (BtFileHeader *)bt.map;

    /* Step 1: descend to leftmost leaf, then walk next_leaf chain to collect
       every leaf page ID in forward order. We don't have prev_leaf in the
       header, so reverse iteration has to replay this list backward. */
    uint32_t *leaves = NULL;
    size_t leaf_count = 0, leaf_cap = 0;

    uint32_t page_id = fh->root_page;
    while (1) {
        uint8_t *page = bt_page(&bt, page_id);
        BtPageHeader *ph = (BtPageHeader *)page;
        if (ph->page_type == 1) break;
        /* Internal node: next_leaf stores the leftmost child. */
        page_id = ph->next_leaf;
    }
    while (page_id != 0) {
        if (leaf_count >= leaf_cap) {
            leaf_cap = leaf_cap ? leaf_cap * 2 : 1024;
            uint32_t *nl = realloc(leaves, leaf_cap * sizeof(uint32_t));
            if (!nl) { free(leaves); bt_close_cached(&bt); return; }
            leaves = nl;
        }
        leaves[leaf_count++] = page_id;
        BtPageHeader *ph = (BtPageHeader *)bt_page(&bt, page_id);
        page_id = ph->next_leaf;
    }

    /* Step 2: iterate leaves right-to-left. Within each leaf, decode entries
       forward (prefix compression forces forward reconstruction) into a
       local snapshot array, then replay entries in reverse applying the
       [min, max] range filter. Stop early if the callback returns < 0.

       Leaves are key-ordered ascending. Before paying the decode cost on a
       leaf, peek its slot-0 anchor key:
         - If first_key > max_val → every entry in leaf is > max_val, skip.
         - If first_key < min_val → every entry here still ≥ first_key but
           prior (leftward) leaves can only have smaller keys. We stop the
           whole walk after processing this leaf. */
    int stop = 0;
    for (int li = (int)leaf_count - 1; li >= 0 && !stop; li--) {
        uint8_t *page = bt_page(&bt, leaves[li]);
        BtPageHeader *ph = (BtPageHeader *)page;
        if (ph->count == 0) continue;

        /* Cheap first-key peek: slot 0 is always an anchor, so one leaf_iter
           step materialises its full key without touching the rest of the
           leaf. */
        LeafIter peek;
        leaf_iter_init(&peek, page);
        if (!leaf_iter_next(&peek)) continue;
        int peek_vs_max = val_cmp(peek.key_buf, peek.key_len, max_val, max_len);
        if (peek_vs_max > 0) continue;
        if (max_exclusive && peek_vs_max == 0 && ph->count == 1) continue;

        DescEntrySnap *snaps = malloc((size_t)ph->count * sizeof(DescEntrySnap));
        if (!snaps) break;
        size_t n = 0;

        LeafIter it;
        leaf_iter_init(&it, page);
        while (leaf_iter_next(&it) && n < ph->count) {
            size_t kl = it.key_len;
            if (kl > BT_MAX_VAL_LEN) kl = BT_MAX_VAL_LEN;
            memcpy(snaps[n].key, it.key_buf, kl);
            snaps[n].key_len = kl;
            memcpy(snaps[n].hash, it.hash, BT_HASH_SIZE);
            n++;
        }

        for (int i = (int)n - 1; i >= 0 && !stop; i--) {
            int cmp_max = val_cmp(snaps[i].key, snaps[i].key_len, max_val, max_len);
            if (cmp_max > 0) continue;                 /* beyond max, skip */
            if (max_exclusive && cmp_max == 0) continue;

            int cmp_min = val_cmp(snaps[i].key, snaps[i].key_len, min_val, min_len);
            if (cmp_min < 0) { stop = 1; break; }      /* below min, done */
            if (min_exclusive && cmp_min == 0) continue;

            if (cb((const char *)snaps[i].key, snaps[i].key_len,
                   snaps[i].hash, ctx) < 0) {
                stop = 1;
                break;
            }
        }
        free(snaps);
    }

    free(leaves);
    bt_close_cached(&bt);
}

void btree_bulk_build(const char *path, BtEntry *entries, size_t count) {
    btree_cache_invalidate(path);
    /* Delete existing file */
    unlink(path);
    if (count == 0) return;

    BtFile bt;
    if (bt_open(&bt, path, 1) != 0) return;

    /* Fill leaf pages left to right */
    uint32_t *leaf_ids = NULL;
    size_t leaf_count = 0, leaf_cap = 256;
    leaf_ids = malloc(leaf_cap * sizeof(uint32_t));

    /* First leaf is page 1 (already allocated) */
    uint32_t cur_leaf = 1;
    leaf_ids[leaf_count++] = cur_leaf;

    /* Running last-key buffer for prefix compression within current leaf. */
    char last_key[BT_MAX_VAL_LEN];
    size_t last_key_len = 0;

    for (size_t i = 0; i < count; i++) {
        uint8_t *page = bt_page(&bt, cur_leaf);
        int result = leaf_append(page, entries[i].value, entries[i].vlen,
                                 entries[i].hash, last_key, &last_key_len);
        if (result == -1) {
            /* Page full — allocate new leaf */
            uint32_t new_leaf = bt_alloc_page(&bt);
            /* Re-fetch after potential remap */
            page = bt_page(&bt, cur_leaf);
            BtPageHeader *ph = (BtPageHeader *)page;
            ph->next_leaf = new_leaf;

            uint8_t *new_pg = bt_page(&bt, new_leaf);
            BtPageHeader *nh = (BtPageHeader *)new_pg;
            nh->page_type = 1;
            nh->count = 0;
            nh->next_leaf = 0;
            nh->data_end = bt_page_size;

            cur_leaf = new_leaf;
            if (leaf_count >= leaf_cap) {
                leaf_cap *= 2;
                leaf_ids = realloc(leaf_ids, leaf_cap * sizeof(uint32_t));
            }
            leaf_ids[leaf_count++] = cur_leaf;

            /* Reset prefix buffer for new leaf — first slot is always an anchor. */
            last_key_len = 0;
            /* If this also fails, the entry is too large to fit in any page
               (value > bt_page_size). Skip the entry — record still exists
               in the data shard, just not indexed. */
            int ins = leaf_append(bt_page(&bt, cur_leaf),
                                  entries[i].value, entries[i].vlen,
                                  entries[i].hash, last_key, &last_key_len);
            (void)ins;
        }
    }

    /* Build internal nodes bottom-up */
    uint32_t *child_ids = leaf_ids;
    size_t child_count = leaf_count;

    BtFileHeader *fh;
    while (child_count > 1) {
        size_t parent_cap = (child_count + 1) / 2 + 1;
        uint32_t *parent_ids = malloc(parent_cap * sizeof(uint32_t));
        size_t parent_count = 0;

        uint32_t cur_parent = bt_alloc_page(&bt);
        uint8_t *ppg = bt_page(&bt, cur_parent);
        BtPageHeader *pph = (BtPageHeader *)ppg;
        pph->page_type = 0;
        pph->count = 0;
        pph->next_leaf = child_ids[0]; /* leftmost child pointer */
        pph->data_end = bt_page_size;
        parent_ids[parent_count++] = cur_parent;

        for (size_t i = 1; i < child_count; i++) {
            /* Get first key from child — need to re-fetch after any alloc */
            uint8_t *child_pg = bt_page(&bt, child_ids[i]);
            BtPageHeader *ch = (BtPageHeader *)child_pg;
            /* Defensive: skip any empty child. page_entry(pg, 0) on a page
               with count=0 reads slot[0] of uninitialized memory. */
            if (ch->count == 0) continue;
            uint8_t *first_e = page_entry(child_pg, 0);
            int child_is_leaf = (ch->page_type == 1);
            size_t kvlen;
            char key_buf[BT_MAX_VAL_LEN];
            if (child_is_leaf) {
                /* Slot 0 is an anchor — prefix_len=0, suffix is full value. */
                kvlen = leaf_entry_suffix_len(first_e);
                if (kvlen > BT_MAX_VAL_LEN) kvlen = BT_MAX_VAL_LEN;
                memcpy(key_buf, leaf_entry_suffix(first_e), kvlen);
            } else {
                kvlen = int_entry_vlen(first_e);
                if (kvlen > BT_MAX_VAL_LEN) kvlen = BT_MAX_VAL_LEN;
                memcpy(key_buf, int_entry_value(first_e), kvlen);
            }

            ppg = bt_page(&bt, cur_parent);
            pph = (BtPageHeader *)ppg;

            if (page_insert_at(ppg, pph->count,
                              key_buf, kvlen,
                              &child_ids[i], 4) == -1) {
                /* Parent full — new parent */
                cur_parent = bt_alloc_page(&bt);
                ppg = bt_page(&bt, cur_parent);
                pph = (BtPageHeader *)ppg;
                pph->page_type = 0;
                pph->count = 0;
                pph->next_leaf = child_ids[i]; /* leftmost child of new parent */
                pph->data_end = bt_page_size;
                parent_ids[parent_count++] = cur_parent;
            }
        }

        if (child_ids != leaf_ids) free(child_ids);
        child_ids = parent_ids;
        child_count = parent_count;
    }

    /* Set root */
    fh = (BtFileHeader *)bt.map;
    fh->root_page = child_ids[0];
    fh->entry_count = count;

    /* Count height */
    uint32_t pg = fh->root_page;
    fh->height = 0;
    while (1) {
        fh->height++;
        BtPageHeader *ph = (BtPageHeader *)bt_page(&bt, pg);
        if (ph->page_type == 1) break;
        if (ph->count == 0) break;
        pg = entry_child(page_entry(bt_page(&bt, pg), 0));
    }

    if (child_ids != leaf_ids) free(child_ids);
    free(leaf_ids);
    bt_close(&bt);
}

/* ========== Merge-rebuild: extract existing + merge + bulk_build ========== */

/* Extract all entries from an existing B+ tree via sequential leaf scan.
   Returns malloc'd array of BtEntry (each .value is malloc'd). Sets *out_count.
   Returns NULL if file doesn't exist or is empty. */
static BtEntry *bt_extract_all(const char *path, size_t *out_count) {
    *out_count = 0;

    int fd = open(path, O_RDONLY);
    if (fd < 0) return NULL;

    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size < (off_t)bt_page_size * 2) {
        close(fd); return NULL;
    }

    size_t map_size = (size_t)st.st_size;
    uint8_t *map = mmap(NULL, map_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) return NULL;
    madvise(map, map_size, MADV_SEQUENTIAL);

    BtFileHeader *fh = (BtFileHeader *)map;
    if (fh->magic != BT_MAGIC || fh->entry_count == 0) {
        munmap(map, map_size); return NULL;
    }

    size_t cap = (size_t)fh->entry_count + 64;
    BtEntry *entries = malloc(cap * sizeof(BtEntry));
    if (!entries) { munmap(map, map_size); return NULL; }
    size_t count = 0;

    /* Walk down to leftmost leaf via next_leaf (= leftmost child for internal) */
    uint32_t page_id = fh->root_page;
    while (1) {
        if ((size_t)page_id * bt_page_size + bt_page_size > map_size) break;
        uint8_t *pg = map + (size_t)page_id * bt_page_size;
        BtPageHeader *ph = (BtPageHeader *)pg;
        if (ph->page_type == 1) break;
        page_id = ph->next_leaf;
    }

    /* Scan leaf chain — sequential decode via LeafIter */
    while (page_id != 0 && (size_t)page_id * bt_page_size + bt_page_size <= map_size) {
        uint8_t *pg = map + (size_t)page_id * bt_page_size;
        BtPageHeader *ph = (BtPageHeader *)pg;
        if (ph->page_type != 1) break;

        LeafIter it;
        leaf_iter_init(&it, pg);
        while (leaf_iter_next(&it)) {
            if (count >= cap) {
                cap *= 2;
                BtEntry *tmp = realloc(entries, cap * sizeof(BtEntry));
                if (!tmp) goto extract_done;
                entries = tmp;
            }
            char *vcopy = malloc(it.key_len + 1);
            if (!vcopy) goto extract_done;
            memcpy(vcopy, it.key_buf, it.key_len);
            vcopy[it.key_len] = '\0';
            entries[count].value = vcopy;
            entries[count].vlen = it.key_len;
            memcpy(entries[count].hash, it.hash, BT_HASH_SIZE);
            count++;
        }
        page_id = ph->next_leaf;
    }

extract_done:
    munmap(map, map_size);
    *out_count = count;
    return entries;
}

static int bt_cmp_entry(const void *a, const void *b) {
    const BtEntry *ea = a, *eb = b;
    return val_cmp(ea->value, ea->vlen, eb->value, eb->vlen);
}

/* Sort new_entries, merge with existing B+ tree contents, rebuild.
   Use instead of btree_insert_batch for bulk operations — much faster because
   existing tree is extracted via sequential leaf scan and combined via merge-sort,
   then rebuilt with btree_bulk_build (sequential write).

   Concurrency: parallel bulk-inserts to an indexed object call this on the
   same .idx file. The read-merge-write sequence is inherently non-atomic, so
   serialize via a per-path mutex. Different .idx files still parallelize. */

typedef struct { char path[PATH_MAX]; pthread_mutex_t mutex; int used; } BtMergeLock;
#define BT_MERGE_LOCK_BUCKETS 256
static BtMergeLock g_bt_merge_locks[BT_MERGE_LOCK_BUCKETS];
static pthread_mutex_t g_bt_merge_table_lock = PTHREAD_MUTEX_INITIALIZER;

static pthread_mutex_t *bt_merge_lock_for(const char *path) {
    uint32_t idx = bt_path_hash(path) % BT_MERGE_LOCK_BUCKETS;
    pthread_mutex_lock(&g_bt_merge_table_lock);
    for (int i = 0; i < BT_MERGE_LOCK_BUCKETS; i++) {
        int slot = (idx + i) % BT_MERGE_LOCK_BUCKETS;
        if (g_bt_merge_locks[slot].used &&
            strcmp(g_bt_merge_locks[slot].path, path) == 0) {
            pthread_mutex_unlock(&g_bt_merge_table_lock);
            return &g_bt_merge_locks[slot].mutex;
        }
        if (!g_bt_merge_locks[slot].used) {
            strncpy(g_bt_merge_locks[slot].path, path, PATH_MAX - 1);
            g_bt_merge_locks[slot].path[PATH_MAX - 1] = '\0';
            pthread_mutex_init(&g_bt_merge_locks[slot].mutex, NULL);
            g_bt_merge_locks[slot].used = 1;
            pthread_mutex_unlock(&g_bt_merge_table_lock);
            return &g_bt_merge_locks[slot].mutex;
        }
    }
    pthread_mutex_unlock(&g_bt_merge_table_lock);
    return NULL;
}

void btree_bulk_merge(const char *path, BtEntry *new_entries, size_t new_count) {
    if (new_count == 0) return;

    pthread_mutex_t *m = bt_merge_lock_for(path);
    if (m) pthread_mutex_lock(m);

    /* Adaptive strategy: if the existing tree is much larger than the batch,
       point-insert into it is cheaper (O(M log N)) than extract+merge+rebuild
       (O(N + M)). Crossover is roughly M*log(N) < N+M, i.e. N/log(N) > M.
       Empirical threshold: existing >= 10 * new AND existing > 1000 entries.
       Cheap check via the file header — no full extract. */
    size_t existing_count = 0;
    int hfd = open(path, O_RDONLY);
    if (hfd >= 0) {
        BtFileHeader hdr;
        if (read(hfd, &hdr, sizeof(hdr)) == (ssize_t)sizeof(hdr) &&
            hdr.magic == BT_MAGIC) {
            existing_count = (size_t)hdr.entry_count;
        }
        close(hfd);
    }

    /* Adaptive strategy threshold. Empirical finding (bench-incremental.sh):
       point-insert wins only when existing >> new batch, crossover around 100:1.
       At 90:1 the point path is already 10x slower than rebuild — B+tree
       random access + per-insert cache invalidation dominates. Default is
       conservative (prefer rebuild); tunable via SHARDKV_BULK_RATIO. */
    int ratio = 100;
    const char *env = getenv("SHARDKV_BULK_RATIO");
    if (env) ratio = atoi(env);

    if (ratio > 0 && existing_count > 1000 &&
        existing_count > new_count * (size_t)ratio) {
        /* Small batch into a large tree — point-insert each entry. Avoids
           reading+rewriting the whole tree. btree_insert handles its own
           file locking; our per-path mutex above already serializes callers. */
        for (size_t i = 0; i < new_count; i++) {
            btree_insert(path, new_entries[i].value,
                         new_entries[i].vlen, new_entries[i].hash);
        }
        if (m) pthread_mutex_unlock(m);
        return;
    }

    /* Large batch (or empty tree) — use the rebuild path. */
    qsort(new_entries, new_count, sizeof(BtEntry), bt_cmp_entry);

    size_t exist_count = 0;
    BtEntry *existing = bt_extract_all(path, &exist_count);

    if (exist_count == 0) {
        if (existing) free(existing);
        btree_bulk_build(path, new_entries, new_count);
        if (m) pthread_mutex_unlock(m);
        return;
    }

    /* Merge two sorted arrays */
    size_t total = exist_count + new_count;
    BtEntry *combined = malloc(total * sizeof(BtEntry));
    if (!combined) {
        for (size_t xi = 0; xi < exist_count; xi++) free((char *)existing[xi].value);
        free(existing);
        if (m) pthread_mutex_unlock(m);
        return;
    }

    size_t ei = 0, ni = 0, ci = 0;
    while (ei < exist_count && ni < new_count) {
        if (val_cmp(existing[ei].value, existing[ei].vlen,
                    new_entries[ni].value, new_entries[ni].vlen) <= 0)
            combined[ci++] = existing[ei++];
        else
            combined[ci++] = new_entries[ni++];
    }
    while (ei < exist_count) combined[ci++] = existing[ei++];
    while (ni < new_count)   combined[ci++] = new_entries[ni++];

    btree_bulk_build(path, combined, ci);

    free(combined);
    for (size_t xi = 0; xi < exist_count; xi++) free((char *)existing[xi].value);
    free(existing);

    if (m) pthread_mutex_unlock(m);
}
