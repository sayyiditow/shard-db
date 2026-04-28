#include "types.h"

/* ========== Hashing & Addressing ========== */

void compute_hash_raw(const char *key, size_t key_len, uint8_t hash_out[16]) {
    XXH128_hash_t h = XXH3_128bits(key, key_len);
    XXH128_canonical_t c;
    XXH128_canonicalFromHash(&c, h);
    memcpy(hash_out, c.digest, 16);
}

/* Derive shard_id and raw slot position from hash. Slot is 32-bit to
   support dynamic per-shard growth beyond 65K slots; callers mask with
   (slots_per_shard - 1) when probing. */
void addr_from_hash(const uint8_t hash[16], int splits, int *shard_id, int *slot) {
    /* Bytes 0-1: shard selection */
    unsigned int h4 = ((unsigned)hash[0] << 8) | hash[1];
    *shard_id = h4 % splits;
    /* Bytes 2-5: 32 bits of slot entropy */
    uint32_t raw = ((uint32_t)hash[2] << 24) | ((uint32_t)hash[3] << 16)
                 | ((uint32_t)hash[4] << 8)  |  (uint32_t)hash[5];
    *slot = (int)raw;
}

void compute_addr(const char *key, size_t key_len, int splits,
                         uint8_t hash_out[16], int *shard_id, int *slot) {
    compute_hash_raw(key, key_len, hash_out);
    addr_from_hash(hash_out, splits, shard_id, slot);
}

/* Shard filename format: <dir>/NNN.bin (3 hex digits, supports up to MAX_SPLITS=4096).
   Single source of truth — change here if the format ever changes. */
void build_shard_filename(char *buf, size_t buflen,
                          const char *data_dir, int shard_id) {
    snprintf(buf, buflen, "%s/%03x.bin", data_dir, shard_id & 0xFFF);
}

void build_shard_path(char *buf, size_t buflen,
                             const char *db_root, const char *object, int shard_id) {
    char dd[PATH_MAX];
    snprintf(dd, sizeof(dd), "%s/%s/data", db_root, object);
    build_shard_filename(buf, buflen, dd, shard_id);
}

/* Canonical layout for per-shard indexes:
       <db_root>/<object>/indexes/<field>/<NNN>.idx
   where NNN is 3 hex digits matching the data shard filename pattern.
   Composite indexes (field name contains '+') get the literal name as
   the directory; the path-encoded form is fine on POSIX filesystems. */
void build_idx_path(char *buf, size_t buflen,
                           const char *db_root, const char *object,
                           const char *field, int idx_shard_id) {
    snprintf(buf, buflen, "%s/%s/indexes/%s/%03x.idx",
             db_root, object, field, idx_shard_id & 0xFFF);
}

/* ========== Unified Shard Cache (ucache) ==========
   Single persistent MAP_SHARED mmap per shard file. Serves both reads and writes.
   - Striped lookup locks (UCACHE_STRIPES mutexes) for fast hash-table probe
   - Per-entry pthread_rwlock_t: readers share, writers exclusive
   - Growth handled under write lock via munmap+mmap (or mremap on Linux)
   - msync all dirty entries on shutdown for durability
   - Bulk insert uses slot_bits/dirty for fast activation pass */

/* Single global mutex for ucache_ensure / eviction / grow. The previous
   UCACHE_STRIPES[64] design was racy: ucache_probe() walks the entire
   hash table across stripes, so two threads installing different paths
   that probe into the same slot could both claim it — neither stripe
   lock guarded the slot. The race was invisible while bulk-insert was
   single-threaded; the shard-grouped parallel port (16 workers per
   request) exposes it at high split counts (~1 shard/run dropped at
   splits=1024, 10M records). A proper per-slot lock would be a larger
   refactor — for now a single mutex is correct and cheap: cache ops
   hit no I/O on warm hits and run in microseconds, so global
   serialisation is not visible in profiles. */
static UCacheEntry     *g_ucache = NULL;
static int              g_ucache_slots = 0;
static int              g_ucache_count = 0;
static pthread_mutex_t  g_ucache_table_mutex;
static volatile uint64_t g_ucache_clock = 0;  /* monotonic counter for LRU */

uint8_t *mmap_with_hints(void *addr, size_t len, int prot, int flags, int fd, off_t off) {
    uint8_t *p = mmap(addr, len, prot, flags, fd, off);
    if (p != MAP_FAILED && len > 0) {
        madvise(p, len, MADV_RANDOM);
        /* Hint kernel to back with 2MB huge pages — 512× fewer page table
           entries and first-touch faults for a given data region. Harmless
           if the kernel can't satisfy the hint. */
        madvise(p, len, MADV_HUGEPAGE);
    }
    return p;
}

static int next_pow2(int n) {
    int p = 1;
    while (p < n) p <<= 1;
    return p;
}

static uint32_t path_hash(const char *path) {
    return (uint32_t)XXH3_64bits(path, strlen(path));
}

void fcache_init(int cap) {
    if (g_ucache) return;
    if (cap < 16) cap = 16;
    g_ucache_slots = next_pow2(cap * 2);
    g_ucache = calloc(g_ucache_slots, sizeof(UCacheEntry));
    g_ucache_count = 0;
    for (int i = 0; i < g_ucache_slots; i++) {
        pthread_rwlock_init(&g_ucache[i].rwlock, NULL);
        g_ucache[i].fd = -1;
    }
    pthread_mutex_init(&g_ucache_table_mutex, NULL);
}

void fcache_shutdown(void) {
    if (!g_ucache) return;
    for (int i = 0; i < g_ucache_slots; i++) {
        UCacheEntry *e = &g_ucache[i];
        if (!e->used) continue;
        /* msync before closing — flush dirty pages to disk */
        if (e->map && e->map_size > 0)
            msync(e->map, e->map_size, MS_SYNC);
        if (e->map) munmap(e->map, e->map_size);
        if (e->fd >= 0) close(e->fd);
        if (e->slot_bits) free(e->slot_bits);
        if (e->retired_map) munmap(e->retired_map, e->retired_size);
        if (e->retired_fd >= 0) close(e->retired_fd);
        pthread_rwlock_destroy(&e->rwlock);
    }
    free(g_ucache);
    g_ucache = NULL;
    g_ucache_slots = 0;
    g_ucache_count = 0;
}

/* Probe hash table for path. Returns slot index.
   Caller must hold the appropriate stripe lock. */
static int ucache_probe(const char *path, int *out_found) {
    uint32_t h = path_hash(path);
    int mask = g_ucache_slots - 1;
    int idx = h & mask;
    int first_empty = -1;
    for (int i = 0; i < g_ucache_slots; i++) {
        int s = (idx + i) & mask;
        if (!g_ucache[s].used) {
            *out_found = 0;
            return first_empty >= 0 ? first_empty : s;
        }
        if (strcmp(g_ucache[s].path, path) == 0) {
            *out_found = 1;
            return s;
        }
    }
    *out_found = 0;
    return first_empty;
}

/* Read ShardHeader from an open fd. On a fresh/empty file, writes a new
   header with INITIAL_SLOTS and ftruncates to the correct file size.
   Returns slots_per_shard, or 0 on failure. */
static uint32_t shard_init_or_read_header(int fd, int slot_size_for_create, int prealloc_mb) {
    struct stat st;
    if (fstat(fd, &st) < 0) return 0;

    if (st.st_size == 0) {
        /* Fresh file: initialise with INITIAL_SLOTS */
        if (slot_size_for_create <= 0) return 0;
        ShardHeader hdr = {0};
        hdr.magic = SHARD_MAGIC;
        hdr.version = SHARD_VERSION;
        hdr.slots_per_shard = INITIAL_SLOTS;
        hdr.record_count = 0;
        size_t need = shard_file_size(INITIAL_SLOTS, slot_size_for_create);
        int use_prealloc = 0;
        if (prealloc_mb > 0) {
            size_t chunk = (size_t)prealloc_mb * 1024 * 1024;
            if (chunk > need) need = chunk;
            use_prealloc = 1;
        }
        if (use_prealloc) {
            if (posix_fallocate(fd, 0, need) != 0) return 0;
        } else {
            if (ftruncate(fd, need) < 0) return 0;
        }
        if (pwrite(fd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) return 0;
        return INITIAL_SLOTS;
    }

    if ((size_t)st.st_size < SHARD_HDR_SIZE) return 0;
    ShardHeader hdr;
    if (pread(fd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) return 0;
    if (hdr.magic != SHARD_MAGIC) return 0;
    if (hdr.slots_per_shard == 0) return 0;
    return hdr.slots_per_shard;
}

/* Internal: find or create cache entry, open fd+mmap if needed.
   Does NOT acquire rwlock — caller does that after this returns.
   slot_size_for_create<=0 means read-only (don't create file).
   Returns slot index, or -1 on failure. */
static int ucache_ensure(const char *path, int slot_size_for_create, int prealloc_mb) {
    pthread_mutex_lock(&g_ucache_table_mutex);

    int found = 0;
    int slot = ucache_probe(path, &found);

    if (found) {
        __atomic_add_fetch(&g_ucache_hits, 1, __ATOMIC_RELAXED);
        g_ucache[slot].last_access = __atomic_add_fetch(&g_ucache_clock, 1, __ATOMIC_RELAXED);
        pthread_mutex_unlock(&g_ucache_table_mutex);
        return slot;
    }

    __atomic_add_fetch(&g_ucache_misses, 1, __ATOMIC_RELAXED);

    /* Cache miss — need to install a new entry */
    int fd;
    size_t sz;
    uint32_t slots_per_shard;

    if (slot_size_for_create <= 0) {
        /* Read-only: open existing file */
        fd = open(path, O_RDWR);
        if (fd < 0) { pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        struct stat st;
        if (fstat(fd, &st) < 0 || st.st_size == 0) { close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        slots_per_shard = shard_init_or_read_header(fd, 0, 0);
        if (slots_per_shard == 0) { close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        sz = st.st_size;
    } else {
        /* Write: create file if needed, write header + ftruncate */
        mkdirp(dirname_of(path));
        fd = open(path, O_RDWR | O_CREAT, 0644);
        if (fd < 0) { pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        slots_per_shard = shard_init_or_read_header(fd, slot_size_for_create, prealloc_mb);
        if (slots_per_shard == 0) { close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        struct stat st; fstat(fd, &st);
        sz = st.st_size;
    }

    /* If table is full, evict LRU entry */
    if (slot < 0 || g_ucache_count >= g_ucache_slots / 2) {
        /* Find least recently used entry */
        int lru = -1;
        uint64_t oldest = UINT64_MAX;
        for (int i = 0; i < g_ucache_slots; i++) {
            if (g_ucache[i].used && g_ucache[i].last_access < oldest) {
                oldest = g_ucache[i].last_access;
                lru = i;
            }
        }
        if (lru >= 0) {
            UCacheEntry *victim = &g_ucache[lru];
            if (victim->map) { msync(victim->map, victim->map_size, MS_ASYNC); munmap(victim->map, victim->map_size); }
            if (victim->fd >= 0) close(victim->fd);
            if (victim->slot_bits) free(victim->slot_bits);
            victim->map = NULL; victim->fd = -1; victim->slot_bits = NULL;
            victim->used = 0; victim->path[0] = '\0';
            victim->map_size = 0; victim->dirty = 0; victim->max_dirty_slot = -1;
            g_ucache_count--;
            slot = lru;
        } else {
            close(fd);
            pthread_mutex_unlock(&g_ucache_table_mutex);
            return -1;
        }
    }

    UCacheEntry *e = &g_ucache[slot];
    strncpy(e->path, path, PATH_MAX - 1); e->path[PATH_MAX - 1] = '\0';
    e->fd = fd;
    e->map_size = sz;
    e->slots_per_shard = slots_per_shard;
    e->map = mmap_with_hints(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (e->map == MAP_FAILED) { e->map = NULL; close(fd); e->fd = -1; pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
    e->used = 1;
    e->dirty = 0;
    e->slot_bits = NULL;
    e->max_dirty_slot = -1;
    e->retired_map = NULL;
    e->retired_size = 0;
    e->retired_fd = -1;
    e->last_access = __atomic_add_fetch(&g_ucache_clock, 1, __ATOMIC_RELAXED);
    g_ucache_count++;
    pthread_mutex_unlock(&g_ucache_table_mutex);
    return slot;
}

/* Get cached read handle. No locking — MAP_SHARED gives live view,
   writers serialize via rwlock but readers are lock-free.
   slots_per_shard is captured at open time; if the shard grows after capture,
   the old mapping is retained via entry->retired_map so the reader stays valid. */
FcacheRead fcache_get_read(const char *path) {
    FcacheRead r = { .map = NULL, .size = 0, .slots_per_shard = 0, .slot = -1 };
    if (!g_ucache) return r;

    int slot = ucache_ensure(path, 0, 0);
    if (slot < 0) return r;

    r.map = g_ucache[slot].map;
    r.size = g_ucache[slot].map_size;
    r.slots_per_shard = g_ucache[slot].slots_per_shard;
    r.slot = slot;
    return r;
}

void fcache_release(FcacheRead h) {
    (void)h; /* no-op — reads are lock-free */
}

/* Get cached write handle. Acquires exclusive rwlock.
   slot_size > 0 creates the shard file (with INITIAL_SLOTS) if missing.
   Caller must call ucache_write_release(). */
FcacheRead ucache_get_write(const char *path, int slot_size, int prealloc_mb) {
    FcacheRead r = { .map = NULL, .size = 0, .slots_per_shard = 0, .slot = -1 };
    if (!g_ucache) return r;

    int slot = ucache_ensure(path, slot_size, prealloc_mb);
    if (slot < 0) return r;

    pthread_rwlock_wrlock(&g_ucache[slot].rwlock);

    UCacheEntry *e = &g_ucache[slot];
    r.map = e->map;
    r.size = e->map_size;
    r.slots_per_shard = e->slots_per_shard;
    r.slot = slot;
    return r;
}

void ucache_write_release(FcacheRead h) {
    if (h.slot < 0) return;
    if (g_ucache && h.slot < g_ucache_slots)
        pthread_rwlock_unlock(&g_ucache[h.slot].rwlock);
}

/* Ask the kernel to start flushing this shard's dirty pages to disk —
   non-blocking. sync_file_range(SYNC_FILE_RANGE_WRITE) is Linux-specific;
   on other platforms we compile to a no-op (portable behavior: rely on the
   kernel's background writeback daemons + the final msync-on-close for
   durability). Called at end of bulk-insert shard workers so Phase 2's
   dirty pages start draining while later phases run. */
void ucache_nudge_writeback(int ucache_slot) {
    if (!g_ucache || ucache_slot < 0 || ucache_slot >= g_ucache_slots) return;
#ifdef __linux__
    UCacheEntry *e = &g_ucache[ucache_slot];
    if (e->fd >= 0)
        sync_file_range(e->fd, 0, 0, SYNC_FILE_RANGE_WRITE);
#endif
}

/* Update ShardHeader.record_count atomically (mmap page is live). */
void ucache_bump_record_count(int ucache_slot, int delta) {
    if (!g_ucache || ucache_slot < 0 || ucache_slot >= g_ucache_slots) return;
    UCacheEntry *e = &g_ucache[ucache_slot];
    if (!e->map) return;
    ShardHeader *hdr = (ShardHeader *)e->map;
    if (hdr->magic != SHARD_MAGIC) return;
    if (delta > 0) hdr->record_count += (uint32_t)delta;
    else if (delta < 0) {
        uint32_t d = (uint32_t)(-delta);
        hdr->record_count = (hdr->record_count > d) ? hdr->record_count - d : 0;
    }
}

/* Parallel-rehash worker. Walks a range of OLD slots, atomically claims a
   destination slot via CAS on SlotHeader.flag (0→1), then fills hash/lengths
   and copies the payload. False sharing at the cache-line level is possible
   (SlotHeader is 24 B, ~2-3 per 64 B line) but the destination is 2× the
   source so probe collisions are rare at 50 % load. */
typedef struct {
    uint32_t start_slot;
    uint32_t end_slot;
    uint8_t *old_map;
    uint8_t *nmap;
    uint32_t old_slots;
    uint32_t new_slots;
    uint32_t new_mask;
    int      slot_size;
    _Atomic uint32_t *live_counter;
} GrowRehashArg;

static void *grow_rehash_worker(void *arg) {
    GrowRehashArg *w = (GrowRehashArg *)arg;
    uint32_t local_live = 0;

    for (uint32_t s = w->start_slot; s < w->end_slot; s++) {
        SlotHeader *h = (SlotHeader *)(w->old_map + zoneA_off(s));
        /* Migrate activated records (flag=1) AND pending bulk-insert records
           (flag=0 with key_len>0) — activation may not have fired yet. */
        int pending_bulk = (h->flag == 0 && h->key_len > 0);
        if (h->flag != 1 && !pending_bulk) continue;

        uint32_t raw = ((uint32_t)h->hash[2] << 24) | ((uint32_t)h->hash[3] << 16)
                     | ((uint32_t)h->hash[4] << 8)  |  (uint32_t)h->hash[5];
        uint32_t start = raw & w->new_mask;

        int placed = 0;
        for (uint32_t i = 0; i < w->new_slots; i++) {
            uint32_t ns = (start + i) & w->new_mask;
            SlotHeader *nh = (SlotHeader *)(w->nmap + zoneA_off(ns));
            uint16_t expected = 0;
            if (__atomic_compare_exchange_n(&nh->flag, &expected, 1,
                                             0,
                                             __ATOMIC_ACQUIRE,
                                             __ATOMIC_RELAXED)) {
                /* Won the slot — write remaining header fields + payload.
                   Other workers see flag=1 and skip past us. */
                memcpy(nh->hash, h->hash, 16);
                nh->key_len = h->key_len;
                nh->value_len = h->value_len;
                uint8_t *op = w->old_map + zoneB_off(s, w->old_slots, w->slot_size);
                uint8_t *np = w->nmap  + zoneB_off(ns, w->new_slots, w->slot_size);
                memcpy(np, op, (size_t)h->key_len + h->value_len);
                placed = 1;
                break;
            }
            /* CAS failed — slot taken by another worker, keep probing */
        }
        if (placed) local_live++;
    }

    atomic_fetch_add_explicit(w->live_counter, local_live, memory_order_relaxed);
    return NULL;
}

/* Double slots_per_shard for this shard: build `.new`, rehash live
   records, atomic rename, swap mapping. Caller must NOT hold rwlock. */
int ucache_grow_shard(const char *path, int slot_size, int prealloc_mb) {
    if (!g_ucache) return -1;
    int slot = ucache_ensure(path, slot_size, prealloc_mb);
    if (slot < 0) return -1;

    UCacheEntry *e = &g_ucache[slot];
    uint32_t observed_slots = e->slots_per_shard;  /* snapshot before lock */
    pthread_rwlock_wrlock(&e->rwlock);

    ShardHeader *old_hdr = (ShardHeader *)e->map;
    if (!old_hdr || old_hdr->magic != SHARD_MAGIC) {
        pthread_rwlock_unlock(&e->rwlock);
        return -1;
    }
    uint32_t old_slots = e->slots_per_shard;
    /* Another writer grew this shard while we waited for the lock. */
    if (old_slots != observed_slots) {
        pthread_rwlock_unlock(&e->rwlock);
        return 0;
    }
    uint32_t new_slots = old_slots * 2;
    char new_path[PATH_MAX];
    snprintf(new_path, sizeof(new_path), "%s.new", path);
    unlink(new_path);

    int nfd = open(new_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (nfd < 0) { pthread_rwlock_unlock(&e->rwlock); return -1; }

    size_t new_size = shard_file_size(new_slots, slot_size);
    int use_prealloc = 0;
    if (prealloc_mb > 0) {
        size_t chunk = (size_t)prealloc_mb * 1024 * 1024;
        if (chunk > new_size) new_size = chunk;
        use_prealloc = 1;
    }
    if (use_prealloc) {
        if (posix_fallocate(nfd, 0, new_size) != 0) { close(nfd); unlink(new_path); pthread_rwlock_unlock(&e->rwlock); return -1; }
    } else {
        if (ftruncate(nfd, new_size) < 0) { close(nfd); unlink(new_path); pthread_rwlock_unlock(&e->rwlock); return -1; }
    }

    uint8_t *nmap = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, nfd, 0);
    if (nmap == MAP_FAILED) { close(nfd); unlink(new_path); pthread_rwlock_unlock(&e->rwlock); return -1; }
    madvise(nmap, new_size, MADV_HUGEPAGE);

    ShardHeader *nhdr = (ShardHeader *)nmap;
    nhdr->magic = SHARD_MAGIC;
    nhdr->version = SHARD_VERSION;
    nhdr->slots_per_shard = new_slots;
    nhdr->record_count = 0;
    memset(nhdr->reserved, 0, sizeof(nhdr->reserved));

    uint32_t new_mask = new_slots - 1;
    _Atomic uint32_t live_atomic = 0;

    /* Parallel rehash. Raw pthreads (not the global pool) because grow is
       often called from a pool task (bulk_insert_shard_worker) — submitting
       nested parallel_for would deadlock when the pool is saturated.
       Small shards stay serial to skip pthread_create overhead. */
    int ng_threads = 1;
    if (old_slots >= 10000) {
        long nproc = sysconf(_SC_NPROCESSORS_ONLN);
        ng_threads = (nproc > 1) ? (int)nproc : 1;
        if (ng_threads > 8) ng_threads = 8;
        /* Don't spawn more threads than source chunks of 2048+ slots. */
        uint32_t max_useful = old_slots / 2048;
        if (max_useful == 0) max_useful = 1;
        if ((uint32_t)ng_threads > max_useful) ng_threads = (int)max_useful;
    }

    GrowRehashArg *gargs = calloc((size_t)ng_threads, sizeof(GrowRehashArg));
    uint32_t chunk = old_slots / (uint32_t)ng_threads;
    for (int t = 0; t < ng_threads; t++) {
        gargs[t].start_slot = (uint32_t)t * chunk;
        gargs[t].end_slot   = (t == ng_threads - 1) ? old_slots : ((uint32_t)t + 1) * chunk;
        gargs[t].old_map    = e->map;
        gargs[t].nmap       = nmap;
        gargs[t].old_slots  = old_slots;
        gargs[t].new_slots  = new_slots;
        gargs[t].new_mask   = new_mask;
        gargs[t].slot_size  = slot_size;
        gargs[t].live_counter = &live_atomic;
    }

    if (ng_threads == 1) {
        grow_rehash_worker(&gargs[0]);
    } else {
        pthread_t *tids = malloc((size_t)ng_threads * sizeof(pthread_t));
        for (int t = 0; t < ng_threads; t++)
            pthread_create(&tids[t], NULL, grow_rehash_worker, &gargs[t]);
        for (int t = 0; t < ng_threads; t++)
            pthread_join(tids[t], NULL);
        free(tids);
    }
    free(gargs);

    uint32_t live = atomic_load_explicit(&live_atomic, memory_order_relaxed);
    nhdr->record_count = live;

    msync(nmap, new_size, MS_SYNC);
    fsync(nfd);

    if (rename(new_path, path) != 0) {
        munmap(nmap, new_size);
        close(nfd);
        unlink(new_path);
        pthread_rwlock_unlock(&e->rwlock);
        return -1;
    }

    /* Free any prior retired mapping (grace period ended). */
    if (e->retired_map) { munmap(e->retired_map, e->retired_size); e->retired_map = NULL; }
    if (e->retired_fd >= 0) { close(e->retired_fd); e->retired_fd = -1; }

    /* Retain current mapping for in-flight readers. */
    e->retired_map = e->map;
    e->retired_size = e->map_size;
    e->retired_fd = e->fd;

    e->map = nmap;
    e->map_size = new_size;
    e->fd = nfd;
    e->slots_per_shard = new_slots;
    /* Bulk-insert bitmap sized for old slots is now obsolete. */
    if (e->slot_bits) { free(e->slot_bits); e->slot_bits = NULL; }
    e->max_dirty_slot = -1;
    e->dirty = 0;

    pthread_rwlock_unlock(&e->rwlock);
    return 1;
}

/* Check threshold; caller holds entry wrlock during the insert but MUST release
   before calling this (we re-acquire inside). ucache_slot is the entry index. */
void ucache_maybe_grow(int ucache_slot, int slot_size, int prealloc_mb) {
    if (!g_ucache || ucache_slot < 0 || ucache_slot >= g_ucache_slots) return;
    UCacheEntry *e = &g_ucache[ucache_slot];
    /* Probe without lock — false positives are OK, grow re-checks under lock. */
    ShardHeader *hdr = (ShardHeader *)e->map;
    if (!hdr || hdr->magic != SHARD_MAGIC) return;
    uint32_t count = hdr->record_count;
    uint32_t slots = e->slots_per_shard;
    if ((uint64_t)count * GROW_LOAD_DEN < (uint64_t)slots * GROW_LOAD_NUM) return;
    char path_copy[PATH_MAX];
    strncpy(path_copy, e->path, PATH_MAX - 1);
    path_copy[PATH_MAX - 1] = '\0';
    ucache_grow_shard(path_copy, slot_size, prealloc_mb);
}

/* Crash recovery: unlink any leftover `*.bin.new` files under db_root.
   These are incomplete grow artifacts from a prior crash. */
static void grow_recovery_dir(const char *dir) {
    DIR *d = opendir(dir);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        char p[PATH_MAX];
        snprintf(p, sizeof(p), "%s/%s", dir, e->d_name);
        struct stat st;
        if (lstat(p, &st) != 0) continue;
        if (S_ISDIR(st.st_mode)) {
            grow_recovery_dir(p);
        } else if (S_ISREG(st.st_mode)) {
            size_t nl = strlen(e->d_name);
            if (nl >= 4 && strcmp(e->d_name + nl - 4, ".new") == 0) {
                unlink(p);
                log_msg(2, "grow_recovery: unlinked stale %s", p);
            }
        }
    }
    closedir(d);
}
void grow_recovery(const char *db_root) {
    grow_recovery_dir(db_root);
}

int ucache_slot_count(void) {
    return g_ucache_slots;
}

int ucache_stats(int *used_slots, int *total_slots, size_t *total_bytes) {
    int used = 0;
    size_t bytes = 0;
    for (int i = 0; i < g_ucache_slots; i++) {
        if (g_ucache[i].used) { used++; bytes += g_ucache[i].map_size; }
    }
    if (used_slots)  *used_slots  = used;
    if (total_slots) *total_slots = g_ucache_slots;
    if (total_bytes) *total_bytes = bytes;
    return 0;
}

/* Get raw UCacheEntry pointer for bulk ops that need slot_bits/dirty.
   Caller must hold write lock via ucache_get_write first. */
UCacheEntry *ucache_entry(int slot) {
    if (!g_ucache || slot < 0 || slot >= g_ucache_slots) return NULL;
    return &g_ucache[slot];
}

/* Mark entries for invalidation — msync, munmap, close matching entries.
   Prefix match so callers can invalidate all shards of an object. */
void fcache_invalidate(const char *path_prefix) {
    if (!g_ucache) return;
    size_t plen = strlen(path_prefix);
    for (int i = 0; i < g_ucache_slots; i++) {
        UCacheEntry *e = &g_ucache[i];
        if (!e->used) continue;
        if (strncmp(e->path, path_prefix, plen) != 0) continue;
        /* Take write lock to ensure no readers */
        pthread_rwlock_wrlock(&e->rwlock);
        if (e->map) { msync(e->map, e->map_size, MS_ASYNC); munmap(e->map, e->map_size); e->map = NULL; }
        if (e->fd >= 0) { close(e->fd); e->fd = -1; }
        if (e->slot_bits) { free(e->slot_bits); e->slot_bits = NULL; }
        if (e->retired_map) { munmap(e->retired_map, e->retired_size); e->retired_map = NULL; e->retired_size = 0; }
        if (e->retired_fd >= 0) { close(e->retired_fd); e->retired_fd = -1; }
        e->map_size = 0;
        e->slots_per_shard = 0;
        e->dirty = 0;
        e->max_dirty_slot = -1;
        e->used = 0;
        e->path[0] = '\0';
        g_ucache_count--;
        pthread_rwlock_unlock(&e->rwlock);
    }
}

/* ========== Pre-allocation ========== */

/* Schema now supports: dir:object:splits:max_key:max_value:prealloc_mb */
/* prealloc_mb is the initial allocation per shard in MB (0 = grow on demand) */

/* ========== Record Count ==========
   Both live and deleted counts share one file ($obj/metadata/counts)
   formatted as "<live> <deleted>\n", protected by a single counts.lock.
   Collapses two flock cycles per delete into one. */

static void counts_paths(char *cpath, char *lpath, const char *db_root, const char *object) {
    char mdir[PATH_MAX];
    snprintf(mdir, sizeof(mdir), "%s/%s/metadata", db_root, object);
    mkdirp(mdir);
    snprintf(cpath, PATH_MAX, "%s/counts", mdir);
    snprintf(lpath, PATH_MAX, "%s/counts.lock", mdir);
}

static void counts_read_locked(const char *cpath, int *live, int *del) {
    *live = 0; *del = 0;
    FILE *f = fopen(cpath, "r");
    if (!f) return;
    fscanf(f, "%d %d", live, del);
    fclose(f);
}

static void counts_write_locked(const char *cpath, int live, int del) {
    FILE *f = fopen(cpath, "w");
    if (!f) return;
    fprintf(f, "%d %d\n", live, del);
    fclose(f);
}

/* Apply deltas to both counts atomically under one lock. */
static void update_counts(const char *db_root, const char *object, int live_delta, int del_delta) {
    char cpath[PATH_MAX], lpath[PATH_MAX];
    counts_paths(cpath, lpath, db_root, object);
    int lockfd = open(lpath, O_RDWR | O_CREAT, 0644);
    flock(lockfd, LOCK_EX);
    int live, del;
    counts_read_locked(cpath, &live, &del);
    live += live_delta; if (live < 0) live = 0;
    del  += del_delta;  if (del  < 0) del  = 0;
    counts_write_locked(cpath, live, del);
    flock(lockfd, LOCK_UN);
    close(lockfd);
}

void update_count(const char *db_root, const char *object, int delta) {
    update_counts(db_root, object, delta, 0);
}

void update_deleted_count(const char *db_root, const char *object, int delta) {
    update_counts(db_root, object, 0, delta);
}

void set_count(const char *db_root, const char *object, int count) {
    char cpath[PATH_MAX], lpath[PATH_MAX];
    counts_paths(cpath, lpath, db_root, object);
    int lockfd = open(lpath, O_RDWR | O_CREAT, 0644);
    flock(lockfd, LOCK_EX);
    int live, del;
    counts_read_locked(cpath, &live, &del);
    counts_write_locked(cpath, count, del);
    flock(lockfd, LOCK_UN);
    close(lockfd);
}

void reset_deleted_count(const char *db_root, const char *object) {
    char cpath[PATH_MAX], lpath[PATH_MAX];
    counts_paths(cpath, lpath, db_root, object);
    int lockfd = open(lpath, O_RDWR | O_CREAT, 0644);
    flock(lockfd, LOCK_EX);
    int live, del;
    counts_read_locked(cpath, &live, &del);
    counts_write_locked(cpath, live, 0);
    flock(lockfd, LOCK_UN);
    close(lockfd);
}

int get_deleted_count(const char *db_root, const char *object) {
    char cpath[PATH_MAX];
    snprintf(cpath, sizeof(cpath), "%s/%s/metadata/counts", db_root, object);
    int live = 0, del = 0;
    counts_read_locked(cpath, &live, &del);
    return del;
}

int get_live_count(const char *db_root, const char *object) {
    char cpath[PATH_MAX];
    snprintf(cpath, sizeof(cpath), "%s/%s/metadata/counts", db_root, object);
    int live = 0, del = 0;
    counts_read_locked(cpath, &live, &del);
    return live;
}

/* Forward declaration */
int is_number(const char *s);

/* ========== GET ========== */

int cmd_get(const char *db_root, const char *object, const char *key) {
    Schema sc = load_schema(db_root, object);
    uint8_t hash[16]; int shard_id, start_slot;
    size_t klen = strlen(key);
    compute_addr(key, klen, sc.splits, hash, &shard_id, &start_slot);

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), db_root, object, shard_id);

    FcacheRead fc = fcache_get_read(shard);
    if (!fc.map) { OUT("{\"error\":\"Not found\"}\n"); return 1; }

    /* Probe Zone A (metadata-only) */
    uint32_t slots = fc.slots_per_shard;
    uint32_t mask = slots - 1;
    int slot = -1;
    for (uint32_t i = 0; i < slots; i++) {
        uint32_t s = ((uint32_t)start_slot + i) & mask;
        SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
        if (h->flag == 0) break;
        if (h->flag == 2) continue;
        if (h->flag == 1 && memcmp(h->hash, hash, 16) == 0 &&
            h->key_len == klen &&
            memcmp(fc.map + zoneB_off(s, slots, sc.slot_size), key, klen) == 0) {
            slot = (int)s; break;
        }
    }

    if (slot < 0) { fcache_release(fc); OUT("{\"error\":\"Not found\"}\n"); return 1; }

    SlotHeader *hdr = (SlotHeader *)(fc.map + zoneA_off(slot));
    log_msg(4, "GET %s.%s (%u bytes)", object, key, hdr->value_len);

    const char *raw = (const char *)(fc.map + zoneB_off(slot, slots, sc.slot_size) + hdr->key_len);

    TypedSchema *ts = load_typed_schema(db_root, object);
    char *json = typed_decode(ts, (const uint8_t *)raw, hdr->value_len);
    OUT("{\"key\":\"%s\",\"value\":%s}\n", key, json);
    free(json);
    fcache_release(fc);
    return 0;
}

/* ========== CAS (Compare-and-Swap) helper ========== */

/* Check all criteria against the current record value (typed binary).
   Returns 1 if ALL criteria match, 0 on first failure. */
int cas_check(TypedSchema *ts, const uint8_t *value_ptr,
              SearchCriterion *crit, int ncrit) {
    for (int i = 0; i < ncrit; i++) {
        char *val_str = NULL;
        if (strchr(crit[i].field, '+')) {
            /* Composite field: concatenate sub-fields */
            char fb[256]; strncpy(fb, crit[i].field, 255); fb[255] = '\0';
            char cat[4096]; int cp = 0; int ok = 1;
            char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
            while (tok) {
                int fi = typed_field_index(ts, tok);
                if (fi >= 0) {
                    char *v = typed_get_field_str(ts, value_ptr, fi);
                    if (v) { int sl = strlen(v); memcpy(cat + cp, v, sl); cp += sl; free(v); }
                    else { ok = 0; break; }
                } else { ok = 0; break; }
                tok = strtok_r(NULL, "+", &_tok_save);
            }
            cat[cp] = '\0';
            val_str = (ok && cp > 0) ? strdup(cat) : NULL;
        } else {
            int fi = typed_field_index(ts, crit[i].field);
            if (fi >= 0) val_str = typed_get_field_str(ts, value_ptr, fi);
        }
        int matched = match_criterion(val_str, &crit[i]);
        free(val_str);
        if (!matched) return 0;
    }
    return 1;
}

/* ========== INSERT (mmap + atomic flag flip) ========== */

int cmd_insert(const char *db_root, const char *object,
               const char *key, const char *value,
               const char *if_json, int if_not_exists) {
    Schema sc = load_schema(db_root, object);
    uint8_t hash[16]; int shard_id, start_slot;
    size_t klen = strlen(key);

    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"Object [%s] not found. Use create-object first.\"}\n", object);
        return 1;
    }
    uint8_t *typed_buf = NULL;
    const char *store_value;
    size_t vlen;

    typed_buf = malloc(ts->total_size);
    int enc = typed_encode_defaults(ts, value, typed_buf, ts->total_size, db_root, object);
    if (enc < 0) {
        free(typed_buf);
        OUT("{\"error\":\"Typed encode failed\"}\n");
        return 1;
    }
    store_value = (const char *)typed_buf;
    vlen = ts->total_size;

    if ((int)klen > sc.max_key) {
        fprintf(stderr, "Error: Key too large (%zu > %d)\n", klen, sc.max_key);
        free(typed_buf);
        return 1;
    }
    if ((int)vlen > sc.max_value) {
        fprintf(stderr, "Error: Value too large (%zu > %d)\n", vlen, sc.max_value);
        free(typed_buf);
        return 1;
    }

    compute_addr(key, klen, sc.splits, hash, &shard_id, &start_slot);

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), db_root, object, shard_id);

    /* Acquire write lock on cached shard (creates with INITIAL_SLOTS if missing) */
    FcacheRead wh = ucache_get_write(shard, sc.slot_size, sc.prealloc_mb);
    if (!wh.map) { free(typed_buf); OUT("{\"error\":\"Cannot open shard\"}\n"); return 1; }
    uint8_t *map = wh.map;
    uint32_t slots = wh.slots_per_shard;
    uint32_t mask = slots - 1;

    /* Probe Zone A */
    int slot = -1, first_tomb = -1;
    for (uint32_t i = 0; i < slots; i++) {
        uint32_t s = ((uint32_t)start_slot + i) & mask;
        SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
        if (h->flag == 0 && h->key_len == 0) { slot = (first_tomb >= 0) ? first_tomb : (int)s; break; }
        if (h->flag == 2) { if (first_tomb < 0) first_tomb = (int)s; continue; }
        if (memcmp(h->hash, hash, 16) == 0 && h->key_len == klen &&
            memcmp(map + zoneB_off(s, slots, sc.slot_size), key, klen) == 0) { slot = (int)s; break; }
    }
    if (slot < 0 && first_tomb >= 0) slot = first_tomb;
    if (slot < 0) { ucache_write_release(wh); log_msg(1, "HASH TABLE FULL %s.%s shard=%d", object, key, shard_id); OUT("{\"error\":\"Hash table full\"}\n"); free(typed_buf); return 1; }

    SlotHeader *existing = (SlotHeader *)(map + zoneA_off(slot));
    int is_update = (existing->flag == 1 && memcmp(existing->hash, hash, 16) == 0);

    /* CAS: check conditions before writing */
    if (if_not_exists && is_update) {
        /* Record already exists — CAS failure */
        TypedSchema *ts2 = load_typed_schema(db_root, object);
        const uint8_t *old_raw = map + zoneB_off(slot, slots, sc.slot_size) + existing->key_len;
        char *cur = typed_decode(ts2, old_raw, existing->value_len);
        ucache_write_release(wh);
        OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n", cur ? cur : "null");
        free(cur); free(typed_buf);
        return 1;
    }
    if (if_json) {
        if (!is_update) {
            /* Record doesn't exist — condition can't match */
            ucache_write_release(wh);
            OUT("{\"error\":\"condition_not_met\",\"current\":null}\n");
            free(typed_buf);
            return 1;
        }
        SearchCriterion *crit = NULL; int ncrit = 0;
        parse_criteria_json(if_json, &crit, &ncrit);
        TypedSchema *ts2 = load_typed_schema(db_root, object);
        const uint8_t *old_raw = map + zoneB_off(slot, slots, sc.slot_size) + existing->key_len;
        int pass = cas_check(ts2, old_raw, crit, ncrit);
        if (!pass) {
            char *cur = typed_decode(ts2, old_raw, existing->value_len);
            ucache_write_release(wh);
            OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n", cur ? cur : "null");
            free(cur); free_criteria(crit, ncrit); free(typed_buf);
            return 1;
        }
        free_criteria(crit, ncrit);
    }

    /* Save old value as JSON for index diff (only if updating) */
    char *old_value = NULL;
    if (is_update && existing->value_len > 0) {
        const char *old_raw = (const char *)(map + zoneB_off(slot, slots, sc.slot_size) + existing->key_len);
        old_value = typed_decode(ts, (const uint8_t *)old_raw, existing->value_len);
    }

    /* Write header+payload and activate atomically while holding wrlock.
       Growth can safely relocate this slot on the next grow tick because the
       record is fully active (flag=1) before the lock is released. */
    SlotHeader *hdr = (SlotHeader *)(map + zoneA_off(slot));
    memset(hdr, 0, HEADER_SIZE);
    memcpy(hdr->hash, hash, 16);
    hdr->flag = 0;
    hdr->key_len = (uint16_t)klen;
    hdr->value_len = (uint32_t)vlen;
    uint8_t *payload = map + zoneB_off(slot, slots, sc.slot_size);
    memcpy(payload, key, klen);
    memcpy(payload + klen, store_value, vlen);
    hdr->flag = 1;
    if (!is_update) ucache_bump_record_count(wh.slot, 1);
    int u_slot = wh.slot;
    ucache_write_release(wh);

    /* Indexing — parallel per field, skip unchanged values */
    char fields[MAX_FIELDS][256];
    int nfields = load_index_fields(db_root, object, fields, MAX_FIELDS);
    if (nfields > 0 && is_update && old_value) {
        TypedSchema *idx_ts = load_typed_schema(db_root, object);
        for (int i = 0; i < nfields; i++) {
            uint8_t *new_key = NULL, *old_key = NULL;
            size_t new_len = 0, old_len = 0;
            int have_new = build_index_key_from_json(idx_ts, value, fields[i], &new_key, &new_len);
            int have_old = build_index_key_from_json(idx_ts, old_value, fields[i], &old_key, &old_len);
            int changed = 0;
            if (have_new && !have_old) changed = 1;
            else if (have_new && have_old) {
                if (new_len != old_len || memcmp(new_key, old_key, new_len) != 0) changed = 1;
            }
            if (changed) {
                if (have_old) delete_index_entry(db_root, object, fields[i], sc.splits, old_key, old_len, hash);
                if (have_new) write_index_entry(db_root, object, fields[i], sc.splits, new_key, new_len, hash);
            }
            free(new_key); free(old_key);
        }
    } else {
        index_parallel(db_root, object, sc.splits, value, hash, fields, nfields);
    }
    free(old_value);

    if (!is_update) update_count(db_root, object, 1);
    log_msg(3, "%s %s.%s (shard=%d slot=%d)", is_update ? "UPDATE" : "INSERT",
            object, key, shard_id, slot);

    /* Post-insert: check if this shard should grow (50% load factor). */
    if (!is_update) ucache_maybe_grow(u_slot, sc.slot_size, sc.prealloc_mb);

    free(typed_buf);
    OUT("{\"status\":\"%s\",\"key\":\"%s\"}\n", is_update ? "updated" : "inserted", key);
    return 0;
}

/* ========== PARTIAL UPDATE ========== */

int cmd_update(const char *db_root, const char *object,
               const char *key, const char *partial_json,
               const char *if_json, int dry_run) {
    Schema sc = load_schema(db_root, object);
    uint8_t hash[16]; int shard_id, start_slot;
    size_t klen = strlen(key);
    compute_addr(key, klen, sc.splits, hash, &shard_id, &start_slot);

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), db_root, object, shard_id);

    FcacheRead wh = ucache_get_write(shard, 0, 0);
    if (!wh.map) { OUT("{\"error\":\"Not found\"}\n"); return 1; }
    uint8_t *map = wh.map;
    uint32_t slots = wh.slots_per_shard;
    uint32_t mask = slots - 1;

    /* Find the record (Zone A probe) */
    int slot = -1;
    for (uint32_t i = 0; i < slots; i++) {
        uint32_t s = ((uint32_t)start_slot + i) & mask;
        SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
        if (h->flag == 0 && h->key_len == 0) break;
        if (h->flag == 2) continue;
        if (h->flag == 1 && memcmp(h->hash, hash, 16) == 0 &&
            h->key_len == klen &&
            memcmp(map + zoneB_off(s, slots, sc.slot_size), key, klen) == 0) {
            slot = (int)s; break;
        }
    }

    if (slot < 0) {
        ucache_write_release(wh);
        OUT("{\"error\":\"Not found\"}\n"); return 1;
    }

    SlotHeader *hdr = (SlotHeader *)(map + zoneA_off(slot));
    uint8_t *value_ptr = map + zoneB_off(slot, slots, sc.slot_size) + hdr->key_len;

    /* Load typed schema for in-place update */
    TypedSchema *ts = load_typed_schema(db_root, object);

    /* CAS: check conditions before writing */
    if (if_json && ts) {
        SearchCriterion *crit = NULL; int ncrit = 0;
        parse_criteria_json(if_json, &crit, &ncrit);
        int pass = cas_check(ts, value_ptr, crit, ncrit);
        if (!pass) {
            char *cur = typed_decode(ts, value_ptr, hdr->value_len);
            ucache_write_release(wh);
            OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n", cur ? cur : "null");
            free(cur); free_criteria(crit, ncrit);
            return 1;
        }
        free_criteria(crit, ncrit);
    }

    if (dry_run) {
        ucache_write_release(wh);
        OUT("{\"status\":\"would_update\",\"key\":\"%s\"}\n", key);
        return 0;
    }

    /* Index fields — collect old values before modifying */
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    uint8_t *old_idx_bufs[MAX_FIELDS];
    size_t  old_idx_lens[MAX_FIELDS];
    int     old_idx_have[MAX_FIELDS];
    memset(old_idx_bufs, 0, sizeof(old_idx_bufs));
    memset(old_idx_lens, 0, sizeof(old_idx_lens));
    memset(old_idx_have, 0, sizeof(old_idx_have));

    if (ts) {
        /* ===== TYPED BINARY: in-place field update at known offsets ===== */

        /* Extract fields from partial JSON */
        const char *field_names[MAX_FIELDS];
        char *field_vals[MAX_FIELDS];
        for (int i = 0; i < ts->nfields; i++) field_names[i] = ts->fields[i].name;
        json_get_fields(partial_json, field_names, ts->nfields, field_vals);

        /* Save old index values (as index-key bytes) before modifying. */
        for (int i = 0; i < nidx; i++) {
            old_idx_have[i] = build_index_key_from_record(ts, value_ptr, idx_fields[i],
                                                         &old_idx_bufs[i], &old_idx_lens[i]);
        }

        /* Write changed fields directly at byte offsets — no decode/merge/re-encode.
           Tombstoned fields are skipped (their bytes stay reserved but unused). */
        for (int i = 0; i < ts->nfields; i++) {
            if (field_vals[i]) {
                if (!ts->fields[i].removed)
                    encode_field(&ts->fields[i], field_vals[i], value_ptr + ts->fields[i].offset);
                free(field_vals[i]);
            }
        }

        /* auto_update fields: always stamp current datetime on every update */
        for (int i = 0; i < ts->nfields; i++) {
            if (ts->fields[i].removed) continue;
            if (ts->fields[i].default_kind == DK_AUTO_UPDATE) {
                char tbuf[20];
                time_t now = time(NULL);
                struct tm tm;
                localtime_r(&now, &tm);
                if (ts->fields[i].type == FT_DATE)
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
                else
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                             tm.tm_hour, tm.tm_min, tm.tm_sec);
                encode_field(&ts->fields[i], tbuf, value_ptr + ts->fields[i].offset);
            }
        }

        /* Read new index values while we still hold the write lock (same mmap) */
        if (nidx > 0) {
            uint8_t *new_val = map + zoneB_off(slot, slots, sc.slot_size) + klen;
            for (int i = 0; i < nidx; i++) {
                uint8_t *new_buf = NULL;
                size_t new_len = 0;
                int have_new = build_index_key_from_record(ts, new_val, idx_fields[i],
                                                          &new_buf, &new_len);
                int changed = 0;
                if (have_new && !old_idx_have[i]) changed = 1;
                else if (!have_new && old_idx_have[i]) changed = 1;
                else if (have_new && old_idx_have[i]) {
                    if (new_len != old_idx_lens[i] ||
                        memcmp(new_buf, old_idx_bufs[i], new_len) != 0) changed = 1;
                }
                if (changed) {
                    if (old_idx_have[i])
                        delete_index_entry(db_root, object, idx_fields[i], sc.splits,
                                           old_idx_bufs[i], old_idx_lens[i], hash);
                    if (have_new)
                        write_index_entry(db_root, object, idx_fields[i], sc.splits,
                                          new_buf, new_len, hash);
                }
                free(old_idx_bufs[i]);
                free(new_buf);
            }
        }

        ucache_write_release(wh);
    }
    log_msg(3, "UPDATE %s.%s (shard=%d slot=%d)", object, key, shard_id, slot);
    OUT("{\"status\":\"updated\",\"key\":\"%s\"}\n", key);
    return 0;
}

/* ========== DELETE helpers ========== */
typedef struct {
    const char *db_root;
    const char *object;
    const char *field;
    int splits;
    const uint8_t *val;
    size_t vlen;
    const uint8_t *hash;
} DelIdxArg;
static void *del_idx_fn(void *arg) {
    DelIdxArg *a = (DelIdxArg *)arg;
    delete_index_entry(a->db_root, a->object, a->field, a->splits,
                       a->val, a->vlen, a->hash);
    return NULL;
}

/* ========== DELETE (with probing) ========== */

int cmd_delete(const char *db_root, const char *object, const char *key,
               const char *if_json, int dry_run) {
    Schema sc = load_schema(db_root, object);
    uint8_t hash[16]; int shard_id, start_slot;
    size_t klen = strlen(key);
    compute_addr(key, klen, sc.splits, hash, &shard_id, &start_slot);

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), db_root, object, shard_id);

    FcacheRead wh = ucache_get_write(shard, 0, 0);
    if (!wh.map) { OUT("{\"status\":\"not_found\",\"key\":\"%s\"}\n", key); return 0; }
    uint8_t *map = wh.map;
    uint32_t slots = wh.slots_per_shard;
    uint32_t mask = slots - 1;

    int slot = -1;
    for (uint32_t i = 0; i < slots; i++) {
        uint32_t s = ((uint32_t)start_slot + i) & mask;
        SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
        if (h->flag == 0 && h->key_len == 0) break;
        if (h->flag == 2) continue;
        if (h->flag == 1 && memcmp(h->hash, hash, 16) == 0 &&
            h->key_len == klen &&
            memcmp(map + zoneB_off(s, slots, sc.slot_size), key, klen) == 0) {
            slot = (int)s; break;
        }
    }

    if (slot >= 0) {
        SlotHeader *h = (SlotHeader *)(map + zoneA_off(slot));

        /* CAS: check conditions before deleting */
        if (if_json) {
            const uint8_t *raw = (const uint8_t *)(map + zoneB_off(slot, slots, sc.slot_size) + h->key_len);
            TypedSchema *ts_cas = load_typed_schema(db_root, object);
            SearchCriterion *crit = NULL; int ncrit = 0;
            parse_criteria_json(if_json, &crit, &ncrit);
            int pass = cas_check(ts_cas, raw, crit, ncrit);
            if (!pass) {
                char *cur = typed_decode(ts_cas, raw, h->value_len);
                ucache_write_release(wh);
                OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n", cur ? cur : "null");
                free(cur); free_criteria(crit, ncrit);
                return 1;
            }
            free_criteria(crit, ncrit);
        }

        if (dry_run) {
            ucache_write_release(wh);
            OUT("{\"status\":\"would_delete\",\"key\":\"%s\"}\n", key);
            return 0;
        }

        /* Extract indexed field values BEFORE tombstoning, for index cleanup */
        char idx_fields[MAX_FIELDS][256];
        int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
        uint8_t *idx_bufs[MAX_FIELDS];
        size_t   idx_lens[MAX_FIELDS];
        int      idx_have[MAX_FIELDS];
        memset(idx_bufs, 0, sizeof(idx_bufs));
        memset(idx_lens, 0, sizeof(idx_lens));
        memset(idx_have, 0, sizeof(idx_have));

        if (nidx > 0) {
            const uint8_t *raw = (const uint8_t *)(map + zoneB_off(slot, slots, sc.slot_size) + h->key_len);
            TypedSchema *ts = load_typed_schema(db_root, object);
            for (int i = 0; i < nidx; i++) {
                idx_have[i] = build_index_key_from_record(ts, raw, idx_fields[i],
                                                         &idx_bufs[i], &idx_lens[i]);
            }
        }

        /* Tombstone the record */
        h->flag = 2;
        ucache_bump_record_count(wh.slot, -1);

        ucache_write_release(wh);

        /* Clean up index entries — parallel across indexes via shared pool */
        if (nidx > 0) {
            DelIdxArg dia[MAX_FIELDS];
            int dic = 0;
            for (int i = 0; i < nidx; i++) {
                if (idx_have[i]) {
                    dia[dic++] = (DelIdxArg){ db_root, object, idx_fields[i], sc.splits,
                                              idx_bufs[i], idx_lens[i], hash };
                }
            }
            parallel_for(del_idx_fn, dia, dic, sizeof(DelIdxArg));
        }
        for (int i = 0; i < nidx; i++) free(idx_bufs[i]);

        update_counts(db_root, object, -1, 1);
        log_msg(3, "DELETE %s.%s", object, key);
        OUT("{\"status\":\"deleted\",\"key\":\"%s\"}\n", key);
    } else {
        ucache_write_release(wh);
        OUT("{\"status\":\"not_found\",\"key\":\"%s\"}\n", key);
    }
    return 0;
}

/* ========== MULTI-KEY GET ========== */

/* ========== Parallel multi-key EXISTS ========== */

typedef struct {
    char *key;
    uint8_t hash[16];
    int shard_id;
    int start_slot;
    int found;
} MultiExistsEntry;

typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    MultiExistsEntry *entries;
    int count;
} MultiExistsShardWork;

static void *multi_exists_shard_worker(void *arg) {
    MultiExistsShardWork *sw = (MultiExistsShardWork *)arg;
    if (sw->count == 0) return NULL;

    int sid = sw->entries[0].shard_id;
    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), sw->db_root, sw->object, sid);
    FcacheRead fc = fcache_get_read(shard);
    if (!fc.map) return NULL;
    uint32_t slots = fc.slots_per_shard;
    uint32_t mask = slots - 1;

    for (int ei = 0; ei < sw->count; ei++) {
        MultiExistsEntry *e = &sw->entries[ei];
        size_t klen = strlen(e->key);
        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)e->start_slot + p) & mask;
            SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag == 2) continue;
            if (h->flag == 1 && memcmp(h->hash, e->hash, 16) == 0 &&
                h->key_len == klen &&
                memcmp(fc.map + zoneB_off(s, slots, sw->sch->slot_size), e->key, klen) == 0) {
                e->found = 1; break;
            }
        }
    }
    fcache_release(fc);
    return NULL;
}

/* mode=exists with keys[], returns {"k1":true,"k2":false,...} */
int cmd_exists_multi(const char *db_root, const char *object, const char *keys_json,
                     const char *format, const char *delimiter) {
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    int key_count = 0, key_cap = 256;
    MultiExistsEntry *entries = malloc(key_cap * sizeof(MultiExistsEntry));
    Schema sc = load_schema(db_root, object);

    const char *p = json_skip(keys_json);
    if (*p == '[') p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p == '"') {
            p++;
            const char *start = p;
            while (*p && *p != '"') p++;
            size_t klen = p - start;
            if (*p == '"') p++;
            if (key_count >= key_cap) { key_cap *= 2; entries = realloc(entries, key_cap * sizeof(MultiExistsEntry)); }
            MultiExistsEntry *e = &entries[key_count++];
            e->key = malloc(klen + 1); memcpy(e->key, start, klen); e->key[klen] = '\0';
            compute_addr(e->key, klen, sc.splits, e->hash, &e->shard_id, &e->start_slot);
            e->found = 0;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("{}\n"); return 0; }

    /* Sort by shard */
    int *sorted = malloc(key_count * sizeof(int));
    for (int i = 0; i < key_count; i++) sorted[i] = i;
    for (int i = 1; i < key_count; i++) {
        int j = i;
        while (j > 0 && entries[sorted[j-1]].shard_id > entries[sorted[j]].shard_id) {
            int tmp = sorted[j]; sorted[j] = sorted[j-1]; sorted[j-1] = tmp; j--;
        }
    }

    int nshard = 0, gstarts[4096], gcounts[4096], prev_sid = -1;
    for (int i = 0; i < key_count && nshard < 4096; i++) {
        int sid = entries[sorted[i]].shard_id;
        if (sid != prev_sid) {
            gstarts[nshard] = i;
            if (nshard > 0) gcounts[nshard-1] = i - gstarts[nshard-1];
            prev_sid = sid; nshard++;
        }
    }
    if (nshard > 0) gcounts[nshard-1] = key_count - gstarts[nshard-1];

    MultiExistsShardWork *workers = calloc(nshard, sizeof(MultiExistsShardWork));
    for (int g = 0; g < nshard; g++) {
        workers[g].db_root = db_root;
        workers[g].object = object;
        workers[g].sch = &sc;
        workers[g].count = gcounts[g];
        workers[g].entries = malloc(gcounts[g] * sizeof(MultiExistsEntry));
        for (int i = 0; i < gcounts[g]; i++)
            workers[g].entries[i] = entries[sorted[gstarts[g] + i]];
    }

    parallel_for(multi_exists_shard_worker, workers, nshard, sizeof(MultiExistsShardWork));

    /* Copy results back */
    for (int g = 0; g < nshard; g++)
        for (int i = 0; i < workers[g].count; i++)
            entries[sorted[gstarts[g] + i]].found = workers[g].entries[i].found;

    /* Output in original order */
    if (csv_delim) {
        OUT("key");
        char d[2] = { csv_delim, '\0' }; OUT("%s", d);
        OUT("exists\n");
        for (int i = 0; i < key_count; i++) {
            csv_emit_cell(entries[i].key, csv_delim);
            OUT("%s%s\n", d, entries[i].found ? "true" : "false");
        }
    } else {
        OUT("{");
        for (int i = 0; i < key_count; i++)
            OUT("%s\"%s\":%s", i ? "," : "", entries[i].key, entries[i].found ? "true" : "false");
        OUT("}\n");
    }

    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers); free(sorted);
    for (int i = 0; i < key_count; i++) free(entries[i].key);
    free(entries);
    return 0;
}

/* mode=not-exists with keys[], returns keys that don't exist */
int cmd_not_exists(const char *db_root, const char *object, const char *keys_json) {
    int key_count = 0, key_cap = 256;
    MultiExistsEntry *entries = malloc(key_cap * sizeof(MultiExistsEntry));
    Schema sc = load_schema(db_root, object);

    const char *p = json_skip(keys_json);
    if (*p == '[') p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p == '"') {
            p++;
            const char *start = p;
            while (*p && *p != '"') p++;
            size_t klen = p - start;
            if (*p == '"') p++;
            if (key_count >= key_cap) { key_cap *= 2; entries = realloc(entries, key_cap * sizeof(MultiExistsEntry)); }
            MultiExistsEntry *e = &entries[key_count++];
            e->key = malloc(klen + 1); memcpy(e->key, start, klen); e->key[klen] = '\0';
            compute_addr(e->key, klen, sc.splits, e->hash, &e->shard_id, &e->start_slot);
            e->found = 0;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("[]\n"); return 0; }

    /* Reuse exists parallel logic */
    int *sorted = malloc(key_count * sizeof(int));
    for (int i = 0; i < key_count; i++) sorted[i] = i;
    for (int i = 1; i < key_count; i++) {
        int j = i;
        while (j > 0 && entries[sorted[j-1]].shard_id > entries[sorted[j]].shard_id) {
            int tmp = sorted[j]; sorted[j] = sorted[j-1]; sorted[j-1] = tmp; j--;
        }
    }

    int nshard = 0, gstarts[4096], gcounts[4096], prev_sid = -1;
    for (int i = 0; i < key_count && nshard < 4096; i++) {
        int sid = entries[sorted[i]].shard_id;
        if (sid != prev_sid) {
            gstarts[nshard] = i;
            if (nshard > 0) gcounts[nshard-1] = i - gstarts[nshard-1];
            prev_sid = sid; nshard++;
        }
    }
    if (nshard > 0) gcounts[nshard-1] = key_count - gstarts[nshard-1];

    MultiExistsShardWork *workers = calloc(nshard, sizeof(MultiExistsShardWork));
    for (int g = 0; g < nshard; g++) {
        workers[g].db_root = db_root;
        workers[g].object = object;
        workers[g].sch = &sc;
        workers[g].count = gcounts[g];
        workers[g].entries = malloc(gcounts[g] * sizeof(MultiExistsEntry));
        for (int i = 0; i < gcounts[g]; i++)
            workers[g].entries[i] = entries[sorted[gstarts[g] + i]];
    }

    parallel_for(multi_exists_shard_worker, workers, nshard, sizeof(MultiExistsShardWork));

    for (int g = 0; g < nshard; g++)
        for (int i = 0; i < workers[g].count; i++)
            entries[sorted[gstarts[g] + i]].found = workers[g].entries[i].found;

    OUT("[");
    int first = 1;
    for (int i = 0; i < key_count; i++) {
        if (!entries[i].found) {
            OUT("%s\"%s\"", first ? "" : ",", entries[i].key);
            first = 0;
        }
    }
    OUT("]\n");

    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers); free(sorted);
    for (int i = 0; i < key_count; i++) free(entries[i].key);
    free(entries);
    return 0;
}

/* ========== Parallel multi-key GET ========== */

typedef struct {
    char *key;
    uint8_t hash[16];
    int shard_id;
    int start_slot;
    char *result_json; /* NULL if not found */
} MultiGetEntry;

typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    MultiGetEntry *entries;
    int count;
    FieldSchema *fs;
} MultiGetShardWork;

static void *multi_get_shard_worker(void *arg) {
    MultiGetShardWork *sw = (MultiGetShardWork *)arg;
    if (sw->count == 0) return NULL;

    int sid = sw->entries[0].shard_id;
    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), sw->db_root, sw->object, sid);
    FcacheRead fc = fcache_get_read(shard);
    if (!fc.map) return NULL;
    uint32_t slots = fc.slots_per_shard;
    uint32_t mask = slots - 1;

    for (int ei = 0; ei < sw->count; ei++) {
        MultiGetEntry *e = &sw->entries[ei];
        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)e->start_slot + p) & mask;
            SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag == 2) continue;
            size_t klen = strlen(e->key);
            if (h->flag == 1 && memcmp(h->hash, e->hash, 16) == 0 &&
                h->key_len == klen &&
                memcmp(fc.map + zoneB_off(s, slots, sw->sch->slot_size), e->key, klen) == 0) {
                const char *raw = (const char *)(fc.map + zoneB_off(s, slots, sw->sch->slot_size) + h->key_len);
                char *val = typed_decode(sw->fs->ts, (const uint8_t *)raw, h->value_len);
                /* Pre-render full JSON */
                size_t jlen = strlen(e->key) + (val ? strlen(val) : 4) + 32;
                e->result_json = malloc(jlen);
                snprintf(e->result_json, jlen, "{\"key\":\"%s\",\"value\":%s}", e->key, val ? val : "null");
                free(val);
                break;
            }
        }
    }
    fcache_release(fc);
    return NULL;
}

int cmd_get_multi(const char *db_root, const char *object, const char *keys_json,
                  const char *format, const char *delimiter) {
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    /* Parse keys */
    int key_count = 0, key_cap = 256;
    MultiGetEntry *entries = malloc(key_cap * sizeof(MultiGetEntry));
    Schema sc = load_schema(db_root, object);

    const char *p = json_skip(keys_json);
    if (*p == '[') p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p == '"') {
            p++;
            const char *start = p;
            while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
            size_t klen = p - start;
            if (*p == '"') p++;
            if (key_count >= key_cap) { key_cap *= 2; entries = realloc(entries, key_cap * sizeof(MultiGetEntry)); }
            MultiGetEntry *e = &entries[key_count++];
            e->key = malloc(klen + 1); memcpy(e->key, start, klen); e->key[klen] = '\0';
            compute_addr(e->key, klen, sc.splits, e->hash, &e->shard_id, &e->start_slot);
            e->result_json = NULL;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("[]\n"); return 0; }

    /* Sort by shard_id (preserve original order via stable sort for output) */
    /* Use index array to maintain original order */
    int *sorted = malloc(key_count * sizeof(int));
    for (int i = 0; i < key_count; i++) sorted[i] = i;
    for (int i = 1; i < key_count; i++) {
        int j = i;
        while (j > 0 && entries[sorted[j-1]].shard_id > entries[sorted[j]].shard_id) {
            int tmp = sorted[j]; sorted[j] = sorted[j-1]; sorted[j-1] = tmp; j--;
        }
    }

    /* Group by shard */
    int nshard = 0;
    int gstarts[4096], gcounts[4096];
    int prev_sid = -1;
    for (int i = 0; i < key_count && nshard < 4096; i++) {
        int sid = entries[sorted[i]].shard_id;
        if (sid != prev_sid) {
            gstarts[nshard] = i;
            if (nshard > 0) gcounts[nshard-1] = i - gstarts[nshard-1];
            prev_sid = sid; nshard++;
        }
    }
    if (nshard > 0) gcounts[nshard-1] = key_count - gstarts[nshard-1];

    /* Build shard-grouped entry arrays */
    FieldSchema fs; init_field_schema(&fs, db_root, object);
    MultiGetShardWork *workers = calloc(nshard, sizeof(MultiGetShardWork));
    for (int g = 0; g < nshard; g++) {
        workers[g].db_root = db_root;
        workers[g].object = object;
        workers[g].sch = &sc;
        workers[g].fs = (fs.ts || fs.nfields > 0) ? &fs : NULL;
        workers[g].count = gcounts[g];
        workers[g].entries = malloc(gcounts[g] * sizeof(MultiGetEntry));
        for (int i = 0; i < gcounts[g]; i++)
            workers[g].entries[i] = entries[sorted[gstarts[g] + i]];
    }

    /* Parallel fetch */
    parallel_for(multi_get_shard_worker, workers, nshard, sizeof(MultiGetShardWork));

    /* Copy results back to entries array (workers have copies) */
    for (int g = 0; g < nshard; g++)
        for (int i = 0; i < workers[g].count; i++)
            entries[sorted[gstarts[g] + i]].result_json = workers[g].entries[i].result_json;

    /* Output in original key order */
    if (csv_delim) {
        /* Header: key + schema fields (no projection on get-multi). */
        OUT("key");
        if (fs.ts) {
            for (int i = 0; i < fs.ts->nfields; i++) {
                if (fs.ts->fields[i].removed) continue;
                char d[2] = { csv_delim, '\0' }; OUT("%s", d);
                csv_emit_cell(fs.ts->fields[i].name, csv_delim);
            }
        }
        OUT("\n");
        for (int i = 0; i < key_count; i++) {
            if (!entries[i].result_json) continue;
            /* result_json shape: {"key":"k","value":{...}} — fields live under
               "value". Parse outer once, parse the value sub-object once, then
               index per-field from the struct. Previously this walked each
               record's JSON 1 + N times (once for value extraction, then once
               per schema field). */
            csv_emit_cell(entries[i].key, csv_delim);
            JsonObj outer, value_obj;
            json_parse_object(entries[i].result_json, strlen(entries[i].result_json), &outer);
            const char *val_raw; size_t val_rawl;
            int have_value = json_obj_get(&outer, "value", &val_raw, &val_rawl) &&
                             json_parse_object(val_raw, val_rawl, &value_obj) >= 0;
            if (fs.ts) {
                for (int fi = 0; fi < fs.ts->nfields; fi++) {
                    if (fs.ts->fields[fi].removed) continue;
                    char d[2] = { csv_delim, '\0' }; OUT("%s", d);
                    char *pv = have_value ? json_obj_strdup(&value_obj, fs.ts->fields[fi].name) : NULL;
                    csv_emit_cell(pv, csv_delim);
                    free(pv);
                }
            }
            OUT("\n");
            free(entries[i].result_json);
        }
    } else {
        OUT("[");
        int printed = 0;
        for (int i = 0; i < key_count; i++) {
            if (entries[i].result_json) {
                OUT("%s%s", printed ? "," : "", entries[i].result_json);
                free(entries[i].result_json);
                printed++;
            }
        }
        OUT("]\n");
    }

    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers); free(sorted);
    for (int i = 0; i < key_count; i++) free(entries[i].key);
    free(entries);
    return 0;
}

