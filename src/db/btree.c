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

/* Defined in util.c — forward-declared here to avoid pulling all of types.h
   (types.h carries heavy server/storage deps that btree.c doesn't need). */
extern void  mkdirp(const char *path);
extern char *dirname_of(const char *path);

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

/* ========== File management ==========
   Unified ucache-style btree cache: one MAP_SHARED mapping per file,
   per-entry pthread_rwlock_t (readers share, writers exclusive). One open
   path for both modes — no MAP_PRIVATE snapshot, no separate writer flock,
   no refcount-based invalidation dance. Mirrors storage.c's UCacheEntry
   model for shard files. */

typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
} BtCacheEntry;

typedef struct {
    int      slot;       /* cache slot index, or -1 if uncached fallback */
    int      writer;     /* 1 if held wrlock, 0 if rdlock (only when slot >= 0) */
    int      fd;         /* mirror of cache entry fd; used for grow remap */
    uint8_t *map;
    size_t   map_size;
} BtFile;

static BtCacheEntry    *bt_cache = NULL;
static int              bt_cache_slots = 0;  /* power of 2 */
static int              bt_cache_count = 0;
static pthread_mutex_t  bt_cache_lock = PTHREAD_MUTEX_INITIALIZER;
static volatile uint64_t bt_cache_clock = 0;  /* monotonic LRU counter */

static int bt_next_pow2(int n) { int p = 1; while (p < n) p <<= 1; return p; }

void bt_cache_init(int cap) {
    if (bt_cache) return;
    if (cap < 16) cap = 16;
    bt_cache_slots = bt_next_pow2(cap * 2);
    bt_cache = calloc(bt_cache_slots, sizeof(BtCacheEntry));
    bt_cache_count = 0;
    for (int i = 0; i < bt_cache_slots; i++) {
        pthread_rwlock_init(&bt_cache[i].rwlock, NULL);
        bt_cache[i].fd = -1;
    }
}

void bt_cache_shutdown(void) {
    pthread_mutex_lock(&bt_cache_lock);
    if (bt_cache) {
        for (int i = 0; i < bt_cache_slots; i++) {
            BtCacheEntry *e = &bt_cache[i];
            if (!e->used) continue;
            if (e->map && e->map_size > 0)
                msync(e->map, e->map_size, MS_SYNC);
            if (e->map) munmap(e->map, e->map_size);
            if (e->fd >= 0) close(e->fd);
            pthread_rwlock_destroy(&e->rwlock);
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

/* Linear probe. Returns slot index of match (out_found=1) or first empty slot
   for insertion (out_found=0). Returns -1 only if the table is completely full. */
static int bt_cache_probe(const char *path, int *out_found) {
    uint32_t h = bt_path_hash(path);
    int mask = bt_cache_slots - 1;
    int idx = h & mask;
    int first_empty = -1;
    for (int i = 0; i < bt_cache_slots; i++) {
        int s = (idx + i) & mask;
        if (!bt_cache[s].used) {
            *out_found = 0;
            return first_empty >= 0 ? first_empty : s;
        }
        if (strcmp(bt_cache[s].path, path) == 0) {
            *out_found = 1;
            return s;
        }
    }
    *out_found = 0;
    return first_empty;
}

/* Tear down a cache slot. Caller holds bt_cache_lock and ensures no holder
   of the rwlock. Mirrors storage.c's LRU eviction — does not check the
   rwlock and does not compact the probe chain. The dropped slot leaves a
   probe-chain gap; a subsequent probe for a path that hashed past this
   slot may install a duplicate at the gap. Both copies are MAP_SHARED of
   the same file (coherent via the kernel page cache); the orphaned copy
   gets LRU-evicted eventually. Bounded by working-set sizing
   (BT_CACHE_MAX = FCACHE_MAX/4). */
static void bt_cache_drop_slot(int slot) {
    BtCacheEntry *e = &bt_cache[slot];
    if (!e->used) return;
    if (e->map && e->map_size > 0) msync(e->map, e->map_size, MS_ASYNC);
    if (e->map) munmap(e->map, e->map_size);
    if (e->fd >= 0) close(e->fd);
    e->map = NULL;
    e->fd = -1;
    e->map_size = 0;
    e->used = 0;
    e->path[0] = '\0';
    bt_cache_count--;
}

/* Initialise a fresh btree file at `map` of `bt_page_size * 2` bytes. */
static void bt_init_file(uint8_t *map) {
    BtFileHeader *fh = (BtFileHeader *)map;
    fh->magic = BT_MAGIC;
    fh->root_page = 1;
    fh->page_count = 2;
    fh->height = 1;
    fh->entry_count = 0;
    fh->key_type = 0;
    fh->key_signed = 0;
    uint8_t *leaf = map + bt_page_size;
    memset(leaf, 0, bt_page_size);
    BtPageHeader *lh = (BtPageHeader *)leaf;
    lh->page_type = 1;
    lh->count = 0;
    lh->next_leaf = 0;
    lh->data_end = bt_page_size;
}

/* Open the file (creating with a fresh header on writer=1 if absent) and
   mmap MAP_SHARED. Returns 0 on success and fills *out_fd, *out_map,
   *out_size; -1 on failure. */
static int bt_open_file(const char *path, int writer,
                        int *out_fd, uint8_t **out_map, size_t *out_size) {
    int fd;
    if (writer) {
        /* dirname_of returns a static-buffer pointer — do not free. */
        mkdirp(dirname_of(path));
        fd = open(path, O_RDWR | O_CREAT, 0644);
    } else {
        fd = open(path, O_RDWR);
    }
    if (fd < 0) return -1;

    struct stat st;
    if (fstat(fd, &st) < 0) { close(fd); return -1; }

    int fresh = 0;
    size_t sz;
    if (st.st_size == 0) {
        if (!writer) { close(fd); return -1; }
        size_t init_size = (size_t)bt_page_size * 2;
        if (ftruncate(fd, init_size) < 0) { close(fd); return -1; }
        sz = init_size;
        fresh = 1;
    } else {
        sz = (size_t)st.st_size;
    }

    uint8_t *map = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) { close(fd); return -1; }
    madvise(map, sz, MADV_RANDOM);

    if (fresh) bt_init_file(map);

    *out_fd = fd;
    *out_map = map;
    *out_size = sz;
    return 0;
}

/* Acquire a btree handle. writer=0 takes rdlock, writer=1 takes wrlock and
   creates the file (with a fresh header) if missing. On cache pressure we
   evict the least-recently-used slot; if the cache isn't initialised or
   eviction can't free a slot, we fall back to an uncached mapping (slot=-1,
   no rwlock) — same hazard tradeoff as storage.c's ucache. */
static int bt_acquire(BtFile *bt, const char *path, int writer) {
    bt->slot = -1;
    bt->writer = writer;
    bt->fd = -1;
    bt->map = NULL;
    bt->map_size = 0;

    if (!bt_cache) {
        /* Cache not initialised — direct mmap, no locking. */
        return bt_open_file(path, writer, &bt->fd, &bt->map, &bt->map_size);
    }

    pthread_mutex_lock(&bt_cache_lock);

    int found = 0;
    int slot = bt_cache_probe(path, &found);

    if (found) {
        __atomic_add_fetch(&g_bt_cache_hits, 1, __ATOMIC_RELAXED);
        bt_cache[slot].last_access = __atomic_add_fetch(&bt_cache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &bt_cache[slot].rwlock;
        pthread_mutex_unlock(&bt_cache_lock);

        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);

        BtCacheEntry *e = &bt_cache[slot];
        bt->slot = slot;
        bt->fd = e->fd;
        bt->map = e->map;
        bt->map_size = e->map_size;
        return 0;
    }

    __atomic_add_fetch(&g_bt_cache_misses, 1, __ATOMIC_RELAXED);

    int fd;
    uint8_t *map;
    size_t sz;
    if (bt_open_file(path, writer, &fd, &map, &sz) < 0) {
        pthread_mutex_unlock(&bt_cache_lock);
        return -1;
    }

    /* Evict LRU when over half-full or the probe couldn't find an empty slot. */
    if (slot < 0 || bt_cache_count >= bt_cache_slots / 2) {
        int lru = -1;
        uint64_t oldest = UINT64_MAX;
        for (int i = 0; i < bt_cache_slots; i++) {
            if (bt_cache[i].used && bt_cache[i].last_access < oldest) {
                oldest = bt_cache[i].last_access;
                lru = i;
            }
        }
        if (lru >= 0) {
            bt_cache_drop_slot(lru);
            slot = lru;
        }
    }

    if (slot < 0) {
        /* Cache truly full — serve uncached. */
        pthread_mutex_unlock(&bt_cache_lock);
        bt->slot = -1;
        bt->fd = fd;
        bt->map = map;
        bt->map_size = sz;
        return 0;
    }

    BtCacheEntry *e = &bt_cache[slot];
    strncpy(e->path, path, PATH_MAX - 1);
    e->path[PATH_MAX - 1] = '\0';
    e->fd = fd;
    e->map = map;
    e->map_size = sz;
    e->used = 1;
    e->last_access = __atomic_add_fetch(&bt_cache_clock, 1, __ATOMIC_RELAXED);
    bt_cache_count++;

    pthread_rwlock_t *lock = &e->rwlock;
    pthread_mutex_unlock(&bt_cache_lock);

    if (writer) pthread_rwlock_wrlock(lock);
    else        pthread_rwlock_rdlock(lock);

    bt->slot = slot;
    bt->fd = fd;
    bt->map = map;
    bt->map_size = sz;
    return 0;
}

static void bt_release(BtFile *bt) {
    if (bt->slot >= 0) {
        if (bt->writer) {
            /* Propagate any grow-time remap back into the cache entry so the
               next reader picks up the new mapping. Safe: we hold wrlock. */
            BtCacheEntry *e = &bt_cache[bt->slot];
            e->map = bt->map;
            e->map_size = bt->map_size;
        }
        pthread_rwlock_unlock(&bt_cache[bt->slot].rwlock);
    } else {
        /* Uncached fallback — manage manually. */
        if (bt->map && bt->map != MAP_FAILED) munmap(bt->map, bt->map_size);
        if (bt->fd >= 0) close(bt->fd);
    }
    bt->map = NULL;
    bt->fd = -1;
    bt->slot = -1;
}

int bt_cache_stats(int *used_slots, int *total_slots, size_t *total_bytes) {
    int used = 0;
    size_t bytes = 0;
    pthread_mutex_lock(&bt_cache_lock);
    if (bt_cache) {
        for (int i = 0; i < bt_cache_slots; i++) {
            if (bt_cache[i].used && bt_cache[i].map) {
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

/* Drop any cache entry for `path`. Used by remove-index before unlink so the
   next acquirer reopens. With the unified MAP_SHARED + rwlock model this is
   no longer needed for write/read coherence (writers and readers share one
   live mapping); invalidate is only required for filesystem operations like
   unlink that need the cached fd/mmap released. Same hazard tradeoff as
   bt_cache_drop_slot: admin paths (remove-index) are already serialized via
   the per-object rwlock so concurrent traffic to this index doesn't happen
   in practice. */
void btree_cache_invalidate(const char *path) {
    pthread_mutex_lock(&bt_cache_lock);
    if (bt_cache) {
        int found = 0;
        int slot = bt_cache_probe(path, &found);
        if (found) bt_cache_drop_slot(slot);
    }
    pthread_mutex_unlock(&bt_cache_lock);
}

/* Get page pointer */
static inline uint8_t *bt_page(BtFile *bt, uint32_t page_id) {
    return bt->map + (size_t)page_id * bt_page_size;
}

/* Allocate a new page. Returns page_id. Grows file in chunks to avoid per-page remap.
   When the file grows we munmap+remap; the caller holds wrlock on the cache slot
   so no concurrent reader is dereferencing the old map, and we update the cache
   entry's map/map_size in place so the next acquirer (waiting on the rwlock)
   picks up the new mapping. */
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
        if (bt->slot >= 0) {
            BtCacheEntry *e = &bt_cache[bt->slot];
            e->map = bt->map;
            e->map_size = bt->map_size;
        }
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

    BtFile bt;
    if (bt_acquire(&bt, path, 1) != 0) return;

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
    bt_release(&bt);
}

/* Batch insert — opens file once, inserts all entries, closes once.
   Much faster and safer than calling btree_insert N times. */
void btree_insert_batch(const char *path, BtEntry *entries, size_t count) {
    if (count == 0) return;

    BtFile bt;
    if (bt_acquire(&bt, path, 1) != 0) return;

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

    bt_release(&bt);
}

void btree_delete(const char *path, const char *value, size_t vlen,
                  const uint8_t hash[BT_HASH_SIZE]) {
    BtFile bt;
    if (bt_acquire(&bt, path, 1) != 0) return; /* write mode — needs to modify pages */

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

    bt_release(&bt);
}

void btree_search(const char *path, const char *value, size_t vlen,
                  bt_result_cb cb, void *ctx) {
    BtFile bt;
    if (bt_acquire(&bt, path, 0) != 0) return;

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
    bt_release(&bt);
}

void btree_range_ex(const char *path,
                    const char *min_val, size_t min_len, int min_exclusive,
                    const char *max_val, size_t max_len, int max_exclusive,
                    bt_result_cb cb, void *ctx) {
    BtFile bt;
    if (bt_acquire(&bt, path, 0) != 0) return;

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
    bt_release(&bt);
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
    if (bt_acquire(&bt, path, 0) != 0) return;
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
            if (!nl) { free(leaves); bt_release(&bt); return; }
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
    bt_release(&bt);
}

/* ========== Streaming range iterator ==========
   Pulls one entry at a time so a caller can drive a k-way merge across
   multiple btrees (cursor pagination over per-shard indexes) without
   collecting every entry into a per-shard buffer. Holds the btree's rdlock
   for its lifetime. */

struct BtRangeIter {
    BtFile bt;
    int    valid;            /* 0 once finished — bt has been released */
    int    desc;
    /* Bounds */
    char   min_val[BT_MAX_VAL_LEN];
    size_t min_len;
    int    min_exclusive;
    char   max_val[BT_MAX_VAL_LEN];
    size_t max_len;
    int    max_exclusive;
    /* Last-yielded snapshot — pointers returned to caller stay valid until
       the next iter_next call. */
    char    yield_value[BT_MAX_VAL_LEN];
    size_t  yield_vlen;
    uint8_t yield_hash[BT_HASH_SIZE];
    /* ASC state — straight forward leaf-chain walk. */
    uint32_t fwd_page_id;    /* current leaf page (0 = exhausted) */
    LeafIter fwd_leaf;       /* prefix-decode state for the current page */
    int      fwd_pending;    /* leaf_iter is positioned AT a valid entry — yield
                                without advancing (set by initial seek and by
                                advancing into a fresh leaf). */
    /* DESC state — pre-collected leaf list, walked right-to-left, and a
       per-leaf decoded snapshot consumed back-to-front. */
    uint32_t      *desc_leaves;
    size_t         desc_leaf_count;
    int            desc_li;       /* index into desc_leaves[] */
    DescEntrySnap *desc_snaps;
    int            desc_snap_n;
    int            desc_snap_i;   /* index walking right-to-left */
};

/* Walk to the leaf containing min_val and position fwd_leaf at the first
   slot >= min_val. The leaf-iter prefix-decode state must be primed by
   walking from the nearest anchor — leaf_iter_seek does that for us. Sets
   fwd_page_id = 0 if no such leaf exists. */
static void iter_seek_fwd(BtRangeIter *it) {
    BtFileHeader *fh = (BtFileHeader *)it->bt.map;
    uint32_t page_id = fh->root_page;
    while (1) {
        uint8_t *page = bt_page(&it->bt, page_id);
        BtPageHeader *ph = (BtPageHeader *)page;
        if (ph->page_type == 1) break;
        int pos = page_bsearch(page, it->min_val, it->min_len);
        if (pos == 0) page_id = ph->next_leaf;
        else page_id = entry_child(page_entry(page, pos - 1));
    }
    /* Walk forward across leaves until we find one with an in-range slot. */
    while (page_id != 0) {
        uint8_t *page = bt_page(&it->bt, page_id);
        int start = page_bsearch(page, it->min_val, it->min_len);
        leaf_iter_init(&it->fwd_leaf, page);
        if (leaf_iter_seek(&it->fwd_leaf, start)) {
            it->fwd_page_id = page_id;
            it->fwd_pending = 1;  /* yield this entry without advancing */
            return;
        }
        /* Empty leaf or all entries past max — try next. */
        BtPageHeader *ph = (BtPageHeader *)page;
        page_id = ph->next_leaf;
    }
    it->fwd_page_id = 0;
}

/* Walk forward leaf chain until a slot is in-range, copy into yield_*, and
   return 1. Returns 0 when exhausted. */
static int iter_next_fwd(BtRangeIter *it) {
    while (it->fwd_page_id != 0) {
        if (it->fwd_pending) {
            /* Initial-seek state or fresh-leaf state — fwd_leaf is already
               positioned on a valid entry; yield without advancing. */
            it->fwd_pending = 0;
        } else if (!leaf_iter_next(&it->fwd_leaf)) {
            /* Page exhausted — advance to next leaf and retry. */
            BtPageHeader *ph = (BtPageHeader *)bt_page(&it->bt, it->fwd_page_id);
            uint32_t next = ph->next_leaf;
            if (next == 0) { it->fwd_page_id = 0; return 0; }
            it->fwd_page_id = next;
            leaf_iter_init(&it->fwd_leaf, bt_page(&it->bt, next));
            /* Slot 0 of every leaf is an anchor (full-key suffix), so the
               first leaf_iter_next decodes correctly without seek. */
            continue;
        }

        int cmp_max = val_cmp(it->fwd_leaf.key_buf, it->fwd_leaf.key_len,
                              it->max_val, it->max_len);
        if (cmp_max > 0) { it->fwd_page_id = 0; return 0; }
        if (it->max_exclusive && cmp_max == 0) { it->fwd_page_id = 0; return 0; }
        if (it->min_exclusive &&
            val_cmp(it->fwd_leaf.key_buf, it->fwd_leaf.key_len,
                    it->min_val, it->min_len) == 0) continue;

        it->yield_vlen = it->fwd_leaf.key_len;
        if (it->yield_vlen > BT_MAX_VAL_LEN) it->yield_vlen = BT_MAX_VAL_LEN;
        memcpy(it->yield_value, it->fwd_leaf.key_buf, it->yield_vlen);
        memcpy(it->yield_hash, it->fwd_leaf.hash, BT_HASH_SIZE);
        return 1;
    }
    return 0;
}

static int iter_load_desc_snap(BtRangeIter *it);

/* Pre-collect the leaf-chain page IDs for DESC walks. Mirrors the prologue
   of btree_range_desc_ex. */
static int iter_init_desc_leaves(BtRangeIter *it) {
    BtFileHeader *fh = (BtFileHeader *)it->bt.map;
    uint32_t page_id = fh->root_page;
    while (1) {
        uint8_t *page = bt_page(&it->bt, page_id);
        BtPageHeader *ph = (BtPageHeader *)page;
        if (ph->page_type == 1) break;
        page_id = ph->next_leaf;
    }
    size_t cap = 0;
    while (page_id != 0) {
        if (it->desc_leaf_count >= cap) {
            cap = cap ? cap * 2 : 1024;
            uint32_t *nl = realloc(it->desc_leaves, cap * sizeof(uint32_t));
            if (!nl) return -1;
            it->desc_leaves = nl;
        }
        it->desc_leaves[it->desc_leaf_count++] = page_id;
        BtPageHeader *ph = (BtPageHeader *)bt_page(&it->bt, page_id);
        page_id = ph->next_leaf;
    }
    /* Pre-load the rightmost leaf's snapshot so the first iter_next_desc call
       has a valid in-leaf cursor; subsequent calls advance leftward. If the
       rightmost leaf is empty / beyond max, iter_load_desc_snap walks left
       internally until it finds a usable one or the iterator drains. */
    it->desc_li = (int)it->desc_leaf_count - 1;
    it->desc_snap_i = -1;
    if (it->desc_li >= 0) iter_load_desc_snap(it);
    return 0;
}

/* Decode the current desc_li leaf into desc_snaps and reset desc_snap_i to
   the rightmost entry. Returns 0 if there's no more snapshot data possible. */
static int iter_load_desc_snap(BtRangeIter *it) {
    while (it->desc_li >= 0) {
        uint8_t *page = bt_page(&it->bt, it->desc_leaves[it->desc_li]);
        BtPageHeader *ph = (BtPageHeader *)page;
        if (ph->count == 0) { it->desc_li--; continue; }

        /* Cheap left-bound check: anchor at slot 0 carries the leaf's first
           full key. If first_key > max_val every entry here is past max — skip. */
        LeafIter peek;
        leaf_iter_init(&peek, page);
        if (!leaf_iter_next(&peek)) { it->desc_li--; continue; }
        int peek_vs_max = val_cmp(peek.key_buf, peek.key_len, it->max_val, it->max_len);
        if (peek_vs_max > 0) { it->desc_li--; continue; }
        if (it->max_exclusive && peek_vs_max == 0 && ph->count == 1) { it->desc_li--; continue; }

        free(it->desc_snaps);
        it->desc_snaps = malloc((size_t)ph->count * sizeof(DescEntrySnap));
        if (!it->desc_snaps) return 0;
        it->desc_snap_n = 0;

        LeafIter lit;
        leaf_iter_init(&lit, page);
        while (leaf_iter_next(&lit) && it->desc_snap_n < (int)ph->count) {
            size_t kl = lit.key_len;
            if (kl > BT_MAX_VAL_LEN) kl = BT_MAX_VAL_LEN;
            memcpy(it->desc_snaps[it->desc_snap_n].key, lit.key_buf, kl);
            it->desc_snaps[it->desc_snap_n].key_len = kl;
            memcpy(it->desc_snaps[it->desc_snap_n].hash, lit.hash, BT_HASH_SIZE);
            it->desc_snap_n++;
        }
        it->desc_snap_i = it->desc_snap_n - 1;
        return 1;
    }
    return 0;
}

/* Backward walk: pop snaps right-to-left, advance to previous leaf when a
   leaf is drained, applying [min, max] bounds. Returns 1 on yield, 0 when
   exhausted. */
static int iter_next_desc(BtRangeIter *it) {
    if (!it->desc_leaves) {
        if (iter_init_desc_leaves(it) < 0) return 0;
    }

    while (1) {
        if (it->desc_snap_i < 0) {
            it->desc_li--;
            if (it->desc_li < 0) return 0;
            if (!iter_load_desc_snap(it)) return 0;
            continue;
        }
        DescEntrySnap *s = &it->desc_snaps[it->desc_snap_i--];

        int cmp_max = val_cmp(s->key, s->key_len, it->max_val, it->max_len);
        if (cmp_max > 0) continue;
        if (it->max_exclusive && cmp_max == 0) continue;

        int cmp_min = val_cmp(s->key, s->key_len, it->min_val, it->min_len);
        if (cmp_min < 0) return 0;  /* below min — every leftward entry is smaller */
        if (it->min_exclusive && cmp_min == 0) continue;

        it->yield_vlen = s->key_len;
        memcpy(it->yield_value, s->key, it->yield_vlen);
        memcpy(it->yield_hash, s->hash, BT_HASH_SIZE);
        return 1;
    }
}

BtRangeIter *btree_range_iter_open(const char *path,
                                   const char *min_val, size_t min_len, int min_exclusive,
                                   const char *max_val, size_t max_len, int max_exclusive,
                                   int desc) {
    BtRangeIter *it = calloc(1, sizeof(*it));
    if (!it) return NULL;
    if (bt_acquire(&it->bt, path, 0) != 0) { free(it); return NULL; }
    it->valid = 1;
    it->desc = desc;
    if (min_len > BT_MAX_VAL_LEN) min_len = BT_MAX_VAL_LEN;
    if (max_len > BT_MAX_VAL_LEN) max_len = BT_MAX_VAL_LEN;
    memcpy(it->min_val, min_val, min_len); it->min_len = min_len;
    memcpy(it->max_val, max_val, max_len); it->max_len = max_len;
    it->min_exclusive = min_exclusive;
    it->max_exclusive = max_exclusive;

    if (!desc) iter_seek_fwd(it);
    /* DESC defers the leaf-list collection to the first next() call so that
       trees the caller never reads from skip the up-front cost. */
    return it;
}

int btree_range_iter_next(BtRangeIter *it,
                          const char **value, size_t *vlen,
                          const uint8_t **hash16) {
    if (!it || !it->valid) return 0;
    int got = it->desc ? iter_next_desc(it) : iter_next_fwd(it);
    if (got) {
        *value  = it->yield_value;
        *vlen   = it->yield_vlen;
        *hash16 = it->yield_hash;
    }
    return got;
}

void btree_range_iter_close(BtRangeIter *it) {
    if (!it) return;
    if (it->valid) bt_release(&it->bt);
    free(it->desc_leaves);
    free(it->desc_snaps);
    free(it);
}

void btree_bulk_build(const char *path, BtEntry *entries, size_t count) {
    /* Drop the cached fd/mapping before unlink — otherwise the cache holds
       the orphaned inode alive and the next acquire opens the new file via
       a fresh fd while old writers still see the deleted one. */
    btree_cache_invalidate(path);
    unlink(path);
    if (count == 0) return;

    BtFile bt;
    if (bt_acquire(&bt, path, 1) != 0) return;

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
    bt_release(&bt);
}

/* ========== Merge-rebuild: extract existing + merge + bulk_build ========== */

/* Extract all entries from an existing B+ tree via sequential leaf scan.
   Returns malloc'd array of BtEntry (each .value is malloc'd). Sets *out_count.
   Returns NULL if file doesn't exist or is empty. */
static BtEntry *bt_extract_all(const char *path, size_t *out_count) {
    *out_count = 0;

    /* Use the unified btree open path — same rdlock that every other
       reader takes, so a concurrent btree_insert blocks briefly on the
       per-file wrlock rather than racing this MAP_PRIVATE view. The
       caller (btree_bulk_merge) holds the per-path bulk-merge mutex and
       runs under objlock, but going through bt_acquire keeps the access
       pattern uniform with the rest of the read path. */
    BtFile bt;
    if (bt_acquire(&bt, path, 0) != 0) return NULL;
    if (bt.map_size < (size_t)bt_page_size * 2) { bt_release(&bt); return NULL; }

    BtFileHeader *fh = (BtFileHeader *)bt.map;
    if (fh->magic != BT_MAGIC || fh->entry_count == 0) {
        bt_release(&bt); return NULL;
    }

    size_t cap = (size_t)fh->entry_count + 64;
    BtEntry *entries = malloc(cap * sizeof(BtEntry));
    if (!entries) { bt_release(&bt); return NULL; }
    size_t count = 0;

    /* Walk down to leftmost leaf via next_leaf (= leftmost child for internal) */
    uint32_t page_id = fh->root_page;
    while (1) {
        if ((size_t)page_id * bt_page_size + bt_page_size > bt.map_size) break;
        uint8_t *pg = bt.map + (size_t)page_id * bt_page_size;
        BtPageHeader *ph = (BtPageHeader *)pg;
        if (ph->page_type == 1) break;
        page_id = ph->next_leaf;
    }

    /* Scan leaf chain — sequential decode via LeafIter */
    while (page_id != 0 && (size_t)page_id * bt_page_size + bt_page_size <= bt.map_size) {
        uint8_t *pg = bt.map + (size_t)page_id * bt_page_size;
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
    bt_release(&bt);
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

    /* Adaptive strategy threshold. The pre-2026.05.1 measurement set this
       at 100:1 and noted point-insert was 10x slower than rebuild at 90:1
       because every btree_insert call did its own
       cache-invalidate + open + lock cycle. Per-call overhead is now ~1µs
       (single bt_acquire wrlock; ucache keeps the file mapped) so the
       crossover moved much closer to the algorithmic prediction:
       insert wins when K * log(N) < N + K  →  K < N / log(N).
       For N=62K (per-shard tree size in the invoice bench),
       log(N)≈16, so insert wins for K < ~3.9K per shard. Lowered default
       ratio to 16; tunable via SHARDKV_BULK_RATIO. */
    int ratio = 16;
    const char *env = getenv("SHARDKV_BULK_RATIO");
    if (env) ratio = atoi(env);

    if (ratio > 0 && existing_count > 1000 &&
        existing_count > new_count * (size_t)ratio) {
        /* Small batch into a large tree — splice via btree_insert_batch.
           Single bt_acquire(write) for the whole batch, no per-entry
           wrlock cycle. The per-path mutex above serialises bulk_merge
           callers; btree_insert_batch's own bt_acquire serialises against
           any concurrent btree_insert / btree_delete on the same path. */
        btree_insert_batch(path, new_entries, new_count);
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
