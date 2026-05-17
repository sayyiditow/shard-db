#include "types.h"
#include "slotcask.h"

/* ========== Hashing & Addressing ==========
 * compute_hash_raw lives in util.c (single source of truth — slotcask.c
 * also calls it, so the canonical-XXH128 byte layout stays bit-identical
 * across the engine and the storage layer). */

/* Derive shard_id and raw slot position from hash. Slot is 32-bit to
   support dynamic per-shard growth beyond 65K slots; callers mask with
   (slots_per_shard - 1) when probing. */
void addr_from_hash(const uint8_t hash[16], int splits, int *shard_id, int *slot) {
    /* v1 byte order — addr_from_hash is the v1 path's helper. */
    *shard_id = compute_record_shard(hash, splits, 1);
    /* Bytes 2-5: 32 bits of slot entropy (v1 zone-A probe; v2 ignores). */
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
#ifdef MADV_HUGEPAGE
        /* Linux only; macOS lacks transparent hugepage hints. */
        madvise(p, len, MADV_HUGEPAGE);
#endif
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

/* Probe hash table for path. Returns slot index, or -1 if the table is
   completely full (every slot used, no path match). Caller must hold the
   appropriate stripe lock.

   No tombstones — deletions clear `used` to 0 outright, so a probe that
   reaches an empty slot means "path is not in the table". */
static int ucache_probe(const char *path, int *out_found) {
    uint32_t h = path_hash(path);
    int mask = g_ucache_slots - 1;
    int idx = h & mask;
    for (int i = 0; i < g_ucache_slots; i++) {
        int s = (idx + i) & mask;
        if (!g_ucache[s].used) {
            *out_found = 0;
            return s;
        }
        if (strcmp(g_ucache[s].path, path) == 0) {
            *out_found = 1;
            return s;
        }
    }
    *out_found = 0;
    return -1;
}

/* Read ShardHeader from an open fd. On a fresh/empty file, writes a new
   header with INITIAL_SLOTS and ftruncates to the correct file size.
   Returns slots_per_shard, or 0 on failure. */
static uint32_t shard_init_or_read_header(int fd, int slot_size_for_create) {
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
        if (ftruncate(fd, need) < 0) return 0;
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
static int ucache_ensure(const char *path, int slot_size_for_create) {
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
        slots_per_shard = shard_init_or_read_header(fd, 0);
        if (slots_per_shard == 0) { close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        sz = st.st_size;
    } else {
        /* Write: create file if needed, write header + ftruncate */
        mkdirp(dirname_of(path));
        fd = open(path, O_RDWR | O_CREAT, 0644);
        if (fd < 0) { pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        slots_per_shard = shard_init_or_read_header(fd, slot_size_for_create);
        if (slots_per_shard == 0) { close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
        struct stat st;
        if (fstat(fd, &st) < 0) { close(fd); pthread_mutex_unlock(&g_ucache_table_mutex); return -1; }
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

    int slot = ucache_ensure(path, 0);
    if (slot < 0) return r;

    r.map = g_ucache[slot].map;
    r.size = g_ucache[slot].map_size;
    /* Lock-free atomic load — same pattern as ucache_grow_to. The retired_map
       mechanism keeps any earlier mmap valid across grows. */
    r.slots_per_shard =
        atomic_load_explicit(&g_ucache[slot].slots_per_shard, memory_order_relaxed);
    r.slot = slot;
    return r;
}

void fcache_release(FcacheRead h) {
    (void)h; /* no-op — reads are lock-free */
}

/* Get cached write handle. Acquires exclusive rwlock.
   slot_size > 0 creates the shard file (with INITIAL_SLOTS) if missing.
   Caller must call ucache_write_release(). */
FcacheRead ucache_get_write(const char *path, int slot_size) {
    FcacheRead r = { .map = NULL, .size = 0, .slots_per_shard = 0, .slot = -1 };
    if (!g_ucache) return r;

    int slot = ucache_ensure(path, slot_size);
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

/* Grow `path` to exactly `target_slots`. Body of the per-shard rebuild;
   ucache_grow_shard is a thin wrapper that supplies target = old * 2. */
int ucache_grow_to(const char *path, uint32_t target_slots,
                   int slot_size) {
    if (!g_ucache) return -1;
    /* target_slots must be a power of 2. */
    if (target_slots == 0 || (target_slots & (target_slots - 1)) != 0) return -1;

    int slot = ucache_ensure(path, slot_size);
    if (slot < 0) return -1;

    UCacheEntry *e = &g_ucache[slot];
    /* Deliberate lock-free snapshot. atomic_load_explicit makes the C11
       atomic semantics explicit to Coverity (the bare-field read at
       `e->slots_per_shard` is also atomic since the field is _Atomic, but
       Coverity's "lock evasion" checker fired anyway despite the
       coverity[lock_evasion] annotation). Snapshotting before the lock
       avoids serialising every concurrent grow-detection through the
       wrlock; if the snapshot is stale we just fall through to the
       authoritative locked recheck at line 452 below. */
    uint32_t observed_slots =
        atomic_load_explicit(&e->slots_per_shard, memory_order_relaxed);
    if (observed_slots >= target_slots) return 0; /* already at/past target */

    pthread_rwlock_wrlock(&e->rwlock);

    ShardHeader *old_hdr = (ShardHeader *)e->map;
    if (!old_hdr || old_hdr->magic != SHARD_MAGIC) {
        pthread_rwlock_unlock(&e->rwlock);
        return -1;
    }
    uint32_t old_slots = e->slots_per_shard;
    /* Another writer already grew us at/past target. */
    if (old_slots >= target_slots) {
        pthread_rwlock_unlock(&e->rwlock);
        return 0;
    }
    uint32_t new_slots = target_slots;
    char new_path[PATH_MAX];
    snprintf(new_path, sizeof(new_path), "%s.new", path);
    unlink(new_path);

    int nfd = open(new_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (nfd < 0) { pthread_rwlock_unlock(&e->rwlock); return -1; }

    size_t new_size = shard_file_size(new_slots, slot_size);
    if (ftruncate(nfd, new_size) < 0) {
        close(nfd); unlink(new_path); pthread_rwlock_unlock(&e->rwlock); return -1;
    }

    uint8_t *nmap = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, nfd, 0);
    if (nmap == MAP_FAILED) { close(nfd); unlink(new_path); pthread_rwlock_unlock(&e->rwlock); return -1; }
#ifdef MADV_HUGEPAGE
    madvise(nmap, new_size, MADV_HUGEPAGE);
#endif

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
        /* Don't spawn more threads than source chunks of 2048+ slots.
           old_slots >= 10000 here, so max_useful >= 4 — no zero-guard needed. */
        uint32_t max_useful = old_slots / 2048;
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
            db_thread_create(&tids[t], grow_rehash_worker, &gargs[t]);
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

/* Double slots_per_shard for this shard. Thin wrapper over ucache_grow_to. */
int ucache_grow_shard(const char *path, int slot_size) {
    if (!g_ucache) return -1;
    int slot = ucache_ensure(path, slot_size);
    if (slot < 0) return -1;
    uint32_t observed = g_ucache[slot].slots_per_shard;
    if (observed == 0) return -1;
    return ucache_grow_to(path, observed * 2, slot_size);
}

uint32_t ucache_peek_slots(const char *path, int slot_size) {
    if (!g_ucache) return 0;
    int slot = ucache_ensure(path, slot_size);
    if (slot < 0) return 0;
    return g_ucache[slot].slots_per_shard;
}

/* Check threshold; caller holds entry wrlock during the insert but MUST release
   before calling this (we re-acquire inside). ucache_slot is the entry index. */
void ucache_maybe_grow(int ucache_slot, int slot_size) {
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
    ucache_grow_shard(path_copy, slot_size);
}

/* Crash recovery: unlink any leftover `*.bin.new` files under db_root.
   These are incomplete grow artifacts from a prior crash. */
static void grow_recovery_dir(const char *dir) {
    DIR *d = opendir(dir);
    if (!d) return;
    int dfd = dirfd(d);
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        struct stat st;
        /* fstatat against the open dirfd: kernel resolves the entry against
           that exact inode, so a swap between stat and unlink can't change
           which file we touch (TOCTOU-safe). */
        if (fstatat(dfd, e->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) continue;
        if (S_ISDIR(st.st_mode)) {
            char p[PATH_MAX];
            snprintf(p, sizeof(p), "%s/%s", dir, e->d_name);
            grow_recovery_dir(p);
        } else if (S_ISREG(st.st_mode)) {
            size_t nl = strlen(e->d_name);
            if (nl >= 4 && strcmp(e->d_name + nl - 4, ".new") == 0) {
                if (unlinkat(dfd, e->d_name, 0) == 0)
                    log_msg(2, "grow_recovery: unlinked stale %s/%s",
                            dir, e->d_name);
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
        /* coverity[lock_evasion] coverity[missing_lock] lock-free skip-empty —
           `_Atomic int used` gives torn-read-free visibility. If a concurrent
           ucache_ensure is in the middle of installing this slot we may miss
           it on this pass; that's acceptable for an invalidate sweep (the
           next sweep or the subsequent reads of the freshly-installed entry
           will see the stale state and the reload-on-miss path handles it). */
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

/* Schema format: dir:object:splits:max_key (max_value derived from
   fields.conf, slot_size = max_key + max_value rounded to 8). */

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
    /* Short read or parse failure → leave both as 0 (the function's
       expected default for missing/empty/corrupt counts files). */
    if (fscanf(f, "%d %d", live, del) != 2) { *live = 0; *del = 0; }
    fclose(f);
}

static void counts_write_locked(const char *cpath, int live, int del) {
    FILE *f = fopen(cpath, "w");
    if (!f) return;
    fprintf(f, "%d %d\n", live, del);
    fclose(f);
}

/* ========== In-memory counts cache ==========
 *
 * The on-disk counts file (text "<live> <deleted>\n") was being
 * read+written under flock on every insert/delete — ~9 syscalls per call,
 * ~24 µs single-thread. For single-conn DELETE x10K that was the
 * dominant cost (DELETE 17k op/s vs UPDATE 29k op/s in bench-kv).
 *
 * Now: per-object atomic int64s in a process-wide hash table. update at
 * insert/delete is a single atomic_fetch_add. Reads (cmd_size, etc.) hit
 * the atomic load. Disk file is the persistence layer — flushed on
 * demand via counts_flush() (called from shutdown / vacuum / recount /
 * server stop, plus opportunistically every FLUSH_INTERVAL atomic
 * updates). Counts may be slightly stale on crash; recount rebuilds.
 */
typedef struct {
    char             path[PATH_MAX];
    _Atomic int64_t  live;
    _Atomic int64_t  deleted;
    _Atomic uint64_t pending_writes;   /* atomic ops since last flush */
    _Atomic int      used;             /* atomic so the lookup hot path can be lock-free */
} CountsCacheEntry;

#define COUNTS_CACHE_BUCKETS 1024
#define COUNTS_FLUSH_INTERVAL 10000   /* atomic updates between auto-flushes */

static CountsCacheEntry g_counts_cache[COUNTS_CACHE_BUCKETS];
static pthread_mutex_t  g_counts_lock = PTHREAD_MUTEX_INITIALIZER;

static unsigned counts_hash_path(const char *p) {
    unsigned h = 5381;
    while (*p) h = ((h << 5) + h) + (unsigned char)(*p++);
    return h;
}

/* Lock-free lookup — entries are write-once after install (we never
   evict). Read used with acquire ordering to pair with the
   release-store at install time, then strcmp the path. Hot path on
   bench-kv DELETE x10000: was 10K mutex_lock/unlock pairs, now zero. */
static CountsCacheEntry *counts_cache_lookup_lockfree(const char *path) {
    unsigned h = counts_hash_path(path);
    for (int i = 0; i < COUNTS_CACHE_BUCKETS; i++) {
        int idx = (int)((h + (unsigned)i) % COUNTS_CACHE_BUCKETS);
        int u = atomic_load_explicit(&g_counts_cache[idx].used,
                                       memory_order_acquire);
        if (!u) return NULL;
        if (strcmp(g_counts_cache[idx].path, path) == 0)
            return &g_counts_cache[idx];
    }
    return NULL;
}

/* Slow path — used at install time only. Caller holds g_counts_lock. */
static CountsCacheEntry *counts_cache_lookup_locked(const char *path) {
    unsigned h = counts_hash_path(path);
    for (int i = 0; i < COUNTS_CACHE_BUCKETS; i++) {
        int idx = (int)((h + (unsigned)i) % COUNTS_CACHE_BUCKETS);
        if (!g_counts_cache[idx].used) return NULL;
        if (strcmp(g_counts_cache[idx].path, path) == 0)
            return &g_counts_cache[idx];
    }
    return NULL;
}

static CountsCacheEntry *counts_cache_get(const char *path) {
    /* Hot path: try lock-free lookup first. */
    CountsCacheEntry *e = counts_cache_lookup_lockfree(path);
    if (e) return e;

    /* Slow path: take the mutex, install. */
    pthread_mutex_lock(&g_counts_lock);
    e = counts_cache_lookup_locked(path);
    if (e) { pthread_mutex_unlock(&g_counts_lock); return e; }

    /* Install — find empty slot, init from disk. */
    unsigned h = counts_hash_path(path);
    int idx = -1;
    for (int i = 0; i < COUNTS_CACHE_BUCKETS; i++) {
        int probe = (int)((h + (unsigned)i) % COUNTS_CACHE_BUCKETS);
        if (!g_counts_cache[probe].used) { idx = probe; break; }
    }
    if (idx < 0) {
        /* Cache full — fall back to direct file I/O via a NULL return.
           Callers handle this gracefully. */
        pthread_mutex_unlock(&g_counts_lock);
        return NULL;
    }

    int live = 0, del = 0;
    counts_read_locked(path, &live, &del);
    strncpy(g_counts_cache[idx].path, path, PATH_MAX - 1);
    g_counts_cache[idx].path[PATH_MAX - 1] = '\0';
    atomic_init(&g_counts_cache[idx].live, (int64_t)live);
    atomic_init(&g_counts_cache[idx].deleted, (int64_t)del);
    atomic_init(&g_counts_cache[idx].pending_writes, 0);
    /* Release-store on used so the lock-free reader's acquire-load sees
       the path + atomics fully initialised before observing used=1. */
    atomic_store_explicit(&g_counts_cache[idx].used, 1, memory_order_release);
    pthread_mutex_unlock(&g_counts_lock);
    return &g_counts_cache[idx];
}

/* Persist current cached counts back to disk. Called on shutdown and
   periodically from update_counts after FLUSH_INTERVAL ops. */
static void counts_flush_entry(const char *cpath, const char *lpath,
                                CountsCacheEntry *e) {
    int live = (int)atomic_load_explicit(&e->live, memory_order_relaxed);
    int del  = (int)atomic_load_explicit(&e->deleted, memory_order_relaxed);
    int lockfd = open(lpath, O_RDWR | O_CREAT, 0644);
    if (lockfd < 0) return;
    flock(lockfd, LOCK_EX);
    counts_write_locked(cpath, live, del);
    flock(lockfd, LOCK_UN);
    close(lockfd);
    atomic_store_explicit(&e->pending_writes, 0, memory_order_relaxed);
}

void counts_flush(const char *db_root, const char *object) {
    char cpath[PATH_MAX], lpath[PATH_MAX];
    counts_paths(cpath, lpath, db_root, object);
    pthread_mutex_lock(&g_counts_lock);
    CountsCacheEntry *e = counts_cache_lookup_locked(cpath);
    if (e) counts_flush_entry(cpath, lpath, e);
    pthread_mutex_unlock(&g_counts_lock);
}

/* Drop the in-memory entry for an object — used after drop-object /
   create-object so a recreated object starts from on-disk state instead
   of inheriting the stale cached counters. Does NOT touch the disk file
   (drop-object's rmrf removes that). */
void counts_invalidate(const char *db_root, const char *object) {
    char cpath[PATH_MAX], lpath[PATH_MAX];
    counts_paths(cpath, lpath, db_root, object);
    pthread_mutex_lock(&g_counts_lock);
    CountsCacheEntry *e = counts_cache_lookup_locked(cpath);
    if (e) {
        /* Atomic store so concurrent lock-free readers see used=0
           promptly (and skip this entry on subsequent lookups). */
        atomic_store_explicit(&e->used, 0, memory_order_release);
        e->path[0] = '\0';
        atomic_store_explicit(&e->live, 0, memory_order_relaxed);
        atomic_store_explicit(&e->deleted, 0, memory_order_relaxed);
        atomic_store_explicit(&e->pending_writes, 0, memory_order_relaxed);
    }
    pthread_mutex_unlock(&g_counts_lock);
}

/* Flush every cached entry. Called from server-shutdown paths so the
   on-disk counts file is up-to-date when the daemon stops cleanly. */
void counts_flush_all(void) {
    pthread_mutex_lock(&g_counts_lock);
    for (int i = 0; i < COUNTS_CACHE_BUCKETS; i++) {
        if (!atomic_load_explicit(&g_counts_cache[i].used,
                                    memory_order_relaxed)) continue;
        char lpath[PATH_MAX];
        snprintf(lpath, PATH_MAX, "%s.lock", g_counts_cache[i].path);
        counts_flush_entry(g_counts_cache[i].path, lpath, &g_counts_cache[i]);
    }
    pthread_mutex_unlock(&g_counts_lock);
}

/* Apply deltas to both counts. Atomic in-memory; opportunistic flush to
   disk every COUNTS_FLUSH_INTERVAL updates. Drops the per-call flock +
   open + read + write + close + funlock cycle the original did. */
static void update_counts(const char *db_root, const char *object, int live_delta, int del_delta) {
    char cpath[PATH_MAX], lpath[PATH_MAX];
    counts_paths(cpath, lpath, db_root, object);
    CountsCacheEntry *e = counts_cache_get(cpath);
    if (!e) {
        /* Cache full — fall back to direct file I/O so the counts still
           progress (slow path; bumping COUNTS_CACHE_BUCKETS is the fix). */
        int lockfd = open(lpath, O_RDWR | O_CREAT, 0644);
        if (lockfd < 0) return;
        flock(lockfd, LOCK_EX);
        int live, del;
        counts_read_locked(cpath, &live, &del);
        live += live_delta; if (live < 0) live = 0;
        del  += del_delta;  if (del  < 0) del  = 0;
        counts_write_locked(cpath, live, del);
        flock(lockfd, LOCK_UN);
        close(lockfd);
        return;
    }

    if (live_delta) {
        int64_t after = atomic_fetch_add_explicit(&e->live, (int64_t)live_delta,
                                                    memory_order_relaxed)
                        + (int64_t)live_delta;
        if (after < 0) atomic_store_explicit(&e->live, 0, memory_order_relaxed);
    }
    if (del_delta) {
        int64_t after = atomic_fetch_add_explicit(&e->deleted, (int64_t)del_delta,
                                                    memory_order_relaxed)
                        + (int64_t)del_delta;
        if (after < 0) atomic_store_explicit(&e->deleted, 0, memory_order_relaxed);
    }

    /* Opportunistic flush. */
    uint64_t p = atomic_fetch_add_explicit(&e->pending_writes, 1,
                                             memory_order_relaxed) + 1;
    if (p % COUNTS_FLUSH_INTERVAL == 0) {
        counts_flush_entry(cpath, lpath, e);
    }
}

void update_count(const char *db_root, const char *object, int delta) {
    /* v2: kf header is the source of truth; slotcask_put / slotcask_delete
       have already updated it atomically under the kf wrlock. Skip the
       legacy text-counts write to avoid a stale-on-crash divergence
       between cache and kf. v1 still uses the text counts file. */
    /* Same dual-form normalisation as resolve_counts. */
    char eff_root[PATH_MAX]; const char *bare_obj;
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t dir_len = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s",
                 db_root, (int)dir_len, object);
        bare_obj = slash + 1;
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        bare_obj = object;
    }
    Schema sc = load_schema(eff_root, bare_obj);
    if (sc.storage_version == 2) return;
    update_counts(db_root, object, delta, 0);
}

void update_deleted_count(const char *db_root, const char *object, int delta) {
    /* Same dual-form normalisation as resolve_counts. */
    char eff_root[PATH_MAX]; const char *bare_obj;
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t dir_len = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s",
                 db_root, (int)dir_len, object);
        bare_obj = slash + 1;
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        bare_obj = object;
    }
    Schema sc = load_schema(eff_root, bare_obj);
    if (sc.storage_version == 2) return;
    update_counts(db_root, object, 0, delta);
}

void set_count(const char *db_root, const char *object, int count) {
    /* v2: kf header is the source of truth; truncate / rebuild / vacuum
       already update it atomically. Skip the legacy text-counts write. */
    /* Same dual-form normalisation as resolve_counts. */
    char eff_root[PATH_MAX]; const char *bare_obj;
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t dir_len = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s",
                 db_root, (int)dir_len, object);
        bare_obj = slash + 1;
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        bare_obj = object;
    }
    Schema sc = load_schema(eff_root, bare_obj);
    if (sc.storage_version == 2) return;
    char cpath[PATH_MAX], lpath[PATH_MAX];
    counts_paths(cpath, lpath, db_root, object);
    CountsCacheEntry *e = counts_cache_get(cpath);
    if (e) {
        atomic_store_explicit(&e->live, (int64_t)count, memory_order_relaxed);
        counts_flush_entry(cpath, lpath, e);  /* set_count callers expect persistence */
        return;
    }
    /* Cache-full fallback — direct disk write. */
    int lockfd = open(lpath, O_RDWR | O_CREAT, 0644);
    if (lockfd < 0) { log_msg(1, "set_count: open(%s) failed: %s", lpath, strerror(errno)); return; }
    flock(lockfd, LOCK_EX);
    int live, del;
    counts_read_locked(cpath, &live, &del);
    counts_write_locked(cpath, count, del);
    flock(lockfd, LOCK_UN);
    close(lockfd);
}

void reset_deleted_count(const char *db_root, const char *object) {
    /* Same dual-form normalisation as resolve_counts. */
    char eff_root[PATH_MAX]; const char *bare_obj;
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t dir_len = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s",
                 db_root, (int)dir_len, object);
        bare_obj = slash + 1;
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        bare_obj = object;
    }
    Schema sc = load_schema(eff_root, bare_obj);
    if (sc.storage_version == 2) return;
    char cpath[PATH_MAX], lpath[PATH_MAX];
    counts_paths(cpath, lpath, db_root, object);
    CountsCacheEntry *e = counts_cache_get(cpath);
    if (e) {
        atomic_store_explicit(&e->deleted, 0, memory_order_relaxed);
        counts_flush_entry(cpath, lpath, e);
        return;
    }
    int lockfd = open(lpath, O_RDWR | O_CREAT, 0644);
    if (lockfd < 0) { log_msg(1, "reset_deleted_count: open(%s) failed: %s", lpath, strerror(errno)); return; }
    flock(lockfd, LOCK_EX);
    int live, del;
    counts_read_locked(cpath, &live, &del);
    counts_write_locked(cpath, live, 0);
    flock(lockfd, LOCK_UN);
    close(lockfd);
}

/* Resolve (live, deleted) for an object. v2 sums kf headers — the kf header
   is updated atomically inside slotcask_put / slotcask_delete and is the
   single source of truth for record counts (cannot go stale across daemon
   crashes the way a separate counts file can). v1 falls back to the legacy
   text counts file.

   Callers pass `object` in two forms historically:
     1. (db_root, "object")          — most call sites
     2. (db_root, "dir/object")      — describe-object, list-objects, list-dirs
   load_schema and slotcask_registry_get expect (effective_root, bare_object)
   so we split joined form here. */
static int resolve_counts(const char *db_root, const char *object,
                          uint64_t *out_live, uint64_t *out_deleted) {
    char eff_root[PATH_MAX];
    const char *bare_obj;
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t dir_len = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s",
                 db_root, (int)dir_len, object);
        bare_obj = slash + 1;
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        bare_obj = object;
    }
    Schema sc = load_schema(eff_root, bare_obj);
    if (sc.storage_version == 2) {
        SlotcaskSchemaInfo info = {
            .splits = sc.splits, .slot_size = sc.slot_size,
            .streams = sc.streams, .storage_version = 2,
        };
        SlotcaskDb *sdb = slotcask_registry_get(eff_root, bare_obj, &info);
        if (!sdb) { *out_live = 0; *out_deleted = 0; return -1; }
        uint64_t total = 0, deleted = 0;
        slotcask_sum_kf_totals(sdb, &total, &deleted);
        *out_live    = total > deleted ? total - deleted : 0;
        *out_deleted = deleted;
        return 0;
    }
    /* v1 fallback (legacy text counts file). */
    char cpath[PATH_MAX], lpath[PATH_MAX];
    counts_paths(cpath, lpath, db_root, object);
    int live = 0, del = 0;
    CountsCacheEntry *e = counts_cache_get(cpath);
    if (e) {
        live = (int)atomic_load_explicit(&e->live, memory_order_relaxed);
        del  = (int)atomic_load_explicit(&e->deleted, memory_order_relaxed);
    } else {
        counts_read_locked(cpath, &live, &del);
    }
    *out_live = (uint64_t)live;
    *out_deleted = (uint64_t)del;
    return 0;
}

int get_deleted_count(const char *db_root, const char *object) {
    uint64_t live = 0, del = 0;
    resolve_counts(db_root, object, &live, &del);
    return (int)del;
}

int get_live_count(const char *db_root, const char *object) {
    uint64_t live = 0, del = 0;
    resolve_counts(db_root, object, &live, &del);
    return (int)live;
}

/* Forward declaration */
int is_number(const char *s);

/* ========== GET ========== */

int cmd_get(const char *db_root, const char *object,
            const char *key, size_t klen) {
    Schema sc = load_schema(db_root, object);

    /* v2 dispatch: route through slotcask. The wire response shape (bare
       value dict for single-key get, per 2026.05.1) stays the same. */
    if (sc.storage_version == 2) {
        SlotcaskSchemaInfo info = {
            .splits = sc.splits, .slot_size = sc.slot_size,
            .streams = sc.streams, .storage_version = 2,
        };
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
        if (!sdb) { OUT("{\"error\":\"Not found\"}\n"); return 1; }
        void *val = NULL; size_t vlen = 0;
        if (slotcask_get(sdb, key, klen, &val, &vlen) != 0) {
            OUT("{\"error\":\"Not found\"}\n");
            return 1;
        }
        log_msg(4, "GET %s (klen=%zu, %zu bytes)", object, klen, vlen);
        TypedSchema *ts = load_typed_schema(db_root, object);
        typed_decode_stream(ts, (const uint8_t *)val, (uint32_t)vlen,
                             g_out ? g_out : stdout);
        fputc('\n', g_out ? g_out : stdout);
        free(val);
        return 0;
    }

    uint8_t hash[16]; int shard_id, start_slot;
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
    log_msg(4, "GET %s (klen=%zu, %u bytes)", object, klen, hdr->value_len);

    const char *raw = (const char *)(fc.map + zoneB_off(slot, slots, sc.slot_size) + hdr->key_len);

    TypedSchema *ts = load_typed_schema(db_root, object);
    /* Stream straight into g_out — saves a malloc + memcpy on every GET. */
    typed_decode_stream(ts, (const uint8_t *)raw, hdr->value_len,
                        g_out ? g_out : stdout);
    fputc('\n', g_out ? g_out : stdout);
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

/* ========== INSERT — v2 (slotcask) helper ==========
 *
 * Closure carries everything the upsert callbacks need. check_fn validates
 * if_json criteria; pre_commit_fn diffs and updates indexes between data
 * write and kf commit (Option B). The ts pointer is borrowed from
 * load_typed_schema's cache and stays valid for the request's lifetime.
 */
typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*fields)[256];
    int               nfields;
    const char       *value_json;
    TypedSchema      *idx_ts;
    SearchCriterion  *crit;
    int               ncrit;
} V2InsertCtx;

static int v2_insert_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2InsertCtx *c = (V2InsertCtx *)ctx_ptr;
    if (c->ncrit > 0) {
        /* if_json criteria require an existing record; reject if missing. */
        if (!old) return 0;
        if (!cas_check(c->idx_ts, old->value, c->crit, c->ncrit)) return 0;
    }
    return 1;
}

/* Per-field index update worker — fires from v2_insert_pre_commit's
   parallel diff path. Either old_key or new_key may be NULL (pure
   insert / pure delete of an indexed value); both NULL means no work. */
typedef struct {
    const char    *db_root;
    const char    *object;
    const char    *field;
    int            splits;
    uint8_t       *new_key;
    size_t         new_len;
    uint8_t       *old_key;
    size_t         old_len;
    const uint8_t *hash;
} UpdateIdxArg;

static void *update_idx_fn(void *arg) {
    UpdateIdxArg *a = (UpdateIdxArg *)arg;
    if (a->old_key)
        delete_index_entry(a->db_root, a->object, a->field, a->splits,
                           a->old_key, a->old_len, a->hash);
    if (a->new_key)
        write_index_entry(a->db_root, a->object, a->field, a->splits,
                          a->new_key, a->new_len, a->hash);
    return NULL;
}

static int v2_insert_pre_commit(const SlotcaskOldRecord *old,
                                const uint8_t *new_value, size_t new_vlen,
                                int is_update, void *ctx_ptr) {
    (void)new_value; (void)new_vlen;
    V2InsertCtx *c = (V2InsertCtx *)ctx_ptr;
    if (c->nfields == 0) return 0;

    if (is_update && old) {
        /* Per-field diff: write/delete only entries that changed.
           Phase 1 (serial): build all (new_key, old_key) pairs, decide
           which fields changed. Phase 2 (parallel): apply the changes
           via parallel_for. For 12-index workloads with N changed fields,
           this drops the index-update wall time from N×~1µs sequential
           to ~1µs parallel (limited by core count). */
        char *old_json = typed_decode(c->idx_ts, old->value, (uint32_t)old->vlen);
        UpdateIdxArg args[MAX_FIELDS];
        int n_args = 0;
        for (int i = 0; i < c->nfields; i++) {
            uint8_t *new_key = NULL, *old_key = NULL;
            size_t new_len = 0, old_len = 0;
            int have_new = build_index_key_from_json(c->idx_ts, c->value_json,
                                                     c->fields[i], &new_key, &new_len);
            int have_old = old_json
                ? build_index_key_from_json(c->idx_ts, old_json,
                                            c->fields[i], &old_key, &old_len)
                : 0;
            int changed = 0;
            if (have_new && !have_old) changed = 1;
            else if (have_new && have_old) {
                if (new_len != old_len ||
                    memcmp(new_key, old_key, new_len) != 0) changed = 1;
            }
            else if (!have_new && have_old) {
                /* Field cleared on update — the old btree entry must be
                   removed or it leaks a stale (key → record_hash) edge.
                   update_idx_fn already handles new_key=NULL as a
                   delete-only operation. */
                changed = 1;
            }
            if (changed) {
                args[n_args].db_root = c->db_root;
                args[n_args].object  = c->object;
                args[n_args].field   = c->fields[i];
                args[n_args].splits  = c->splits;
                /* update_idx_fn treats NULL keys as "skip" — so a
                   delete-only op (have_old, !have_new) gets new_key=NULL
                   and old_key=old_key, an insert-only op (have_new,
                   !have_old) gets new_key=new_key and old_key=NULL,
                   and a real update gets both. */
                args[n_args].new_key = have_new ? new_key : NULL;
                args[n_args].new_len = new_len;
                args[n_args].old_key = have_old ? old_key : NULL;
                args[n_args].old_len = old_len;
                args[n_args].hash    = c->hash;
                n_args++;
            } else {
                /* Unchanged — free immediately, nothing to dispatch. */
                free(new_key); free(old_key);
            }
        }
        if (n_args > 0) {
            parallel_for(update_idx_fn, args, n_args, sizeof(UpdateIdxArg));
            for (int i = 0; i < n_args; i++) {
                free(args[i].new_key);
                free(args[i].old_key);
            }
        }
        free(old_json);
    } else {
        /* Fresh insert: parallel write of all index entries. */
        index_parallel(c->db_root, c->object, c->splits,
                       c->value_json, c->hash, c->fields, c->nfields);
    }
    return 0;
}

static int cmd_insert_v2(const char *db_root, const char *object,
                         const char *key, size_t klen, const char *value,
                         const char *if_json, int if_not_exists,
                         const Schema *sc) {
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"Object [%s] not found. Use create-object first.\"}\n", object);
        return 1;
    }

    if ((int)klen > sc->max_key) {
        fprintf(stderr, "Error: Key too large (%zu > %d)\n", klen, sc->max_key);
        return 1;
    }

    uint8_t *typed_buf = malloc(ts->total_size);
    if (!typed_buf) { OUT("{\"error\":\"oom\"}\n"); return 1; }
    int enc = typed_encode_defaults(ts, value, typed_buf, ts->total_size,
                                    db_root, object);
    if (enc < 0) {
        free(typed_buf);
        OUT("{\"error\":\"Typed encode failed\"}\n");
        return 1;
    }
    size_t vlen = ts->total_size;
    if ((int)vlen > sc->max_value) {
        free(typed_buf);
        fprintf(stderr, "Error: Value too large (%zu > %d)\n", vlen, sc->max_value);
        return 1;
    }

    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams, .storage_version = 2,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        free(typed_buf);
        OUT("{\"error\":\"Cannot open shard\"}\n");
        return 1;
    }

    /* Index fields + criteria (only parsed if if_json is present). */
    char fields[MAX_FIELDS][256];
    int nfields = load_index_fields(db_root, object, fields, MAX_FIELDS);
    for (int _i = 0; _i < nfields; _i++) fields[_i][255] = '\0';

    SearchCriterion *crit = NULL;
    int ncrit = 0;
    if (if_json) parse_criteria_json(if_json, &crit, &ncrit);

    V2InsertCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .fields = fields, .nfields = nfields,
        .value_json = value,
        .idx_ts = ts,
        .crit = crit, .ncrit = ncrit,
    };
    compute_hash_raw(key, klen, ctx.hash);

    SlotcaskUpsertOpts opts = {
        .if_not_exists  = if_not_exists,
        .check          = v2_insert_check_fn,
        .check_ctx      = &ctx,
        .pre_commit     = v2_insert_pre_commit,
        .pre_commit_ctx = &ctx,
    };
    SlotcaskUpsertResult result = {0};
    int rc;
    /* Fast path: pure INSERT semantics (if_not_exists=true) without a CAS
       criterion lets us skip the kf_lookup-with-verify pass that the upsert
       path always pays. The duplicate detection still happens implicitly
       inside kf_put_new — caller sees -2 with was_update=1 if a duplicate
       is found. v2_insert_check_fn / v2_insert_pre_commit handle old=NULL
       correctly (they only diff against old when is_update=1). */
    if (if_not_exists && !if_json) {
        rc = slotcask_insert_with_hooks(sdb, -1, key, klen,
                                        typed_buf, vlen, &opts, &result);
    } else {
        rc = slotcask_upsert_with_hooks(sdb, -1, key, klen,
                                        typed_buf, vlen, &opts, &result);
    }

    if (rc == -2) {
        char *cur = result.current_value
            ? typed_decode(ts, result.current_value, (uint32_t)result.current_vlen)
            : NULL;
        OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
            cur ? cur : "null");
        free(cur);
        free(result.current_value);
        free_criteria(crit, ncrit);
        free(typed_buf);
        return 1;
    }
    if (rc != 0) {
        free(result.current_value);
        free_criteria(crit, ncrit);
        free(typed_buf);
        OUT("{\"error\":\"upsert failed\"}\n");
        return 1;
    }

    if (!result.was_update) update_count(db_root, object, 1);
    char wire_key[1100];
    format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
    log_msg(3, "%s %s.%s (slotcask)",
            result.was_update ? "UPDATE" : "INSERT", object, wire_key);

    free(result.current_value);
    free_criteria(crit, ncrit);
    free(typed_buf);
    OUT("{\"status\":\"%s\",\"key\":\"%s\"}\n",
        result.was_update ? "updated" : "inserted", wire_key);
    return 0;
}

/* ========== INSERT (mmap + atomic flag flip) ========== */

int cmd_insert(const char *db_root, const char *object,
               const char *key, size_t klen, const char *value,
               const char *if_json, int if_not_exists) {
    Schema sc = load_schema(db_root, object);

    if (sc.storage_version == 2) {
        return cmd_insert_v2(db_root, object, key, klen, value, if_json,
                             if_not_exists, &sc);
    }

    uint8_t hash[16]; int shard_id, start_slot;

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
    FcacheRead wh = ucache_get_write(shard, sc.slot_size);
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
    if (slot < 0) { ucache_write_release(wh); log_msg(1, "HASH TABLE FULL %s (klen=%zu) shard=%d", object, klen, shard_id); OUT("{\"error\":\"Hash table full\"}\n"); free(typed_buf); return 1; }

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
    /* load_index_fields null-terminates each entry within 256 bytes
       (config.c:698 cache-hit, :720 cache-miss). Re-asserting the term
       in caller scope so Coverity STRING_NULL stops chaining through
       every fields[i] consumer downstream. Cheap: one byte write per
       index field on the schema-load path (once per query). */
    for (int _i = 0; _i < nfields; _i++) fields[_i][255] = '\0';
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
    char wire_key[1100];
    format_wire_key(&sc, key, klen, wire_key, sizeof(wire_key));
    log_msg(3, "%s %s.%s (shard=%d slot=%d)", is_update ? "UPDATE" : "INSERT",
            object, wire_key, shard_id, slot);

    /* Post-insert: check if this shard should grow (50% load factor). */
    if (!is_update) ucache_maybe_grow(u_slot, sc.slot_size);

    free(typed_buf);
    OUT("{\"status\":\"%s\",\"key\":\"%s\"}\n", is_update ? "updated" : "inserted", wire_key);
    return 0;
}

/* ========== PARTIAL UPDATE — v2 (slotcask) helper ==========
 *
 * Two-phase: slotcask_get → apply partial → upsert(require_existing=1) with a
 * pre_commit hook that diffs old vs new typed records for index updates.
 * The two reads of OLD (one outside the wrlock to compute new, one inside
 * the wrlock that the upsert handles) introduce a tiny last-writer-wins race
 * window vs v1's full-shard wrlock. Acceptable for partial-update workloads;
 * documented behavior change.
 */
typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*idx_fields)[256];
    int               nidx;
    TypedSchema      *idx_ts;
    /* CAS criteria — verified inside check_fn under the kf-shard wrlock
       so the check + commit are atomic against concurrent writers. NULL
       when the caller didn't pass `if`. */
    SearchCriterion  *crit;
    int               ncrit;
} V2UpdateCtx;

static int v2_update_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2UpdateCtx *c = (V2UpdateCtx *)ctx_ptr;
    if (!old) return 0;  /* require_existing handles this, but defensive */
    if (c->crit && c->ncrit > 0 &&
        !cas_check(c->idx_ts, old->value, c->crit, c->ncrit)) return 0;
    return 1;
}

static int v2_update_pre_commit(const SlotcaskOldRecord *old,
                                const uint8_t *new_value, size_t new_vlen,
                                int is_update, void *ctx_ptr) {
    (void)new_vlen; (void)is_update;
    V2UpdateCtx *c = (V2UpdateCtx *)ctx_ptr;
    if (!old || c->nidx == 0) return 0;
    for (int i = 0; i < c->nidx; i++) {
        uint8_t *old_buf = NULL, *new_buf = NULL;
        size_t   old_len = 0, new_len = 0;
        int have_old = build_index_key_from_record(c->idx_ts, old->value,
                                                   c->idx_fields[i],
                                                   &old_buf, &old_len);
        int have_new = build_index_key_from_record(c->idx_ts, new_value,
                                                   c->idx_fields[i],
                                                   &new_buf, &new_len);
        int changed = 0;
        if (have_new && !have_old) changed = 1;
        else if (!have_new && have_old) changed = 1;
        else if (have_new && have_old) {
            if (new_len != old_len ||
                memcmp(new_buf, old_buf, new_len) != 0) changed = 1;
        }
        if (changed) {
            if (have_old)
                delete_index_entry(c->db_root, c->object, c->idx_fields[i],
                                   c->splits, old_buf, old_len, c->hash);
            if (have_new)
                write_index_entry(c->db_root, c->object, c->idx_fields[i],
                                  c->splits, new_buf, new_len, c->hash);
        }
        free(old_buf); free(new_buf);
    }
    return 0;
}

static int cmd_update_v2(const char *db_root, const char *object,
                         const char *key, size_t klen,
                         const char *partial_json,
                         const char *if_json, int dry_run, const Schema *sc) {
    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams, .storage_version = 2,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("{\"error\":\"Not found\"}\n"); return 1; }

    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) { OUT("{\"error\":\"Object not found\"}\n"); return 1; }

    void *old_val = NULL; size_t old_vlen = 0;
    if (slotcask_get(sdb, key, klen, &old_val, &old_vlen) != 0) {
        OUT("{\"error\":\"Not found\"}\n");
        return 1;
    }

    /* dry_run validates criteria but doesn't write — race-tolerant. */
    if (dry_run) {
        if (if_json) {
            SearchCriterion *crit = NULL; int ncrit = 0;
            parse_criteria_json(if_json, &crit, &ncrit);
            int pass = cas_check(ts, old_val, crit, ncrit);
            free_criteria(crit, ncrit);
            if (!pass) {
                char *cur = typed_decode(ts, old_val, (uint32_t)old_vlen);
                OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
                    cur ? cur : "null");
                free(cur); free(old_val);
                return 1;
            }
        }
        free(old_val);
        char wire_key[1100];
        format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
        OUT("{\"status\":\"would_update\",\"key\":\"%s\"}\n", wire_key);
        return 0;
    }

    /* Build new typed buffer = copy of old, with partial fields applied. */
    uint8_t *new_buf = malloc(old_vlen);
    if (!new_buf) {
        free(old_val);
        OUT("{\"error\":\"oom\"}\n"); return 1;
    }
    memcpy(new_buf, old_val, old_vlen);
    free(old_val);

    const char *field_names[MAX_FIELDS];
    char *field_vals[MAX_FIELDS] = {0};
    for (int i = 0; i < ts->nfields; i++) field_names[i] = ts->fields[i].name;
    json_get_fields(partial_json, field_names, ts->nfields, field_vals);

    for (int i = 0; i < ts->nfields; i++) {
        if (field_vals[i]) {
            if (!ts->fields[i].removed)
                encode_field(&ts->fields[i], field_vals[i],
                             new_buf + ts->fields[i].offset);
            free(field_vals[i]);
        }
    }

    /* auto_update fields: stamp current datetime/date on every update. */
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed) continue;
        if (ts->fields[i].default_kind == DK_AUTO_UPDATE) {
            char tbuf[20];
            time_t now = time(NULL);
            struct tm tmv;
            localtime_r(&now, &tmv);
            if (ts->fields[i].type == FT_DATE)
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                         tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday);
            else
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                         tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                         tmv.tm_hour, tmv.tm_min, tmv.tm_sec);
            encode_field(&ts->fields[i], tbuf, new_buf + ts->fields[i].offset);
        }
    }

    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';

    /* Parse `if` once; check_fn runs cas_check under the kf-shard wrlock
       so the verify + commit are atomic against concurrent writers. */
    SearchCriterion *crit = NULL;
    int ncrit = 0;
    if (if_json) parse_criteria_json(if_json, &crit, &ncrit);

    V2UpdateCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .idx_fields = idx_fields, .nidx = nidx,
        .idx_ts = ts,
        .crit = crit, .ncrit = ncrit,
    };
    compute_hash_raw(key, klen, ctx.hash);

    SlotcaskUpsertOpts opts = {
        .require_existing = 1,
        .check            = v2_update_check_fn,
        .check_ctx        = &ctx,
        .pre_commit       = v2_update_pre_commit,
        .pre_commit_ctx   = &ctx,
    };
    SlotcaskUpsertResult result = {0};
    int rc = slotcask_upsert_with_hooks(sdb, -1, key, klen,
                                        new_buf, old_vlen, &opts, &result);
    free(new_buf);

    if (rc == -2) {
        /* Either require_existing fired (record vanished) or check_fn
           rejected (criteria didn't match). Disambiguate by whether the
           result has an old value attached. */
        if (result.was_update && result.condition_not_met) {
            char *cur = result.current_value
                ? typed_decode(ts, result.current_value, (uint32_t)result.current_vlen)
                : NULL;
            OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
                cur ? cur : "null");
            free(cur);
        } else {
            OUT("{\"error\":\"Not found\"}\n");
        }
        free(result.current_value);
        free_criteria(crit, ncrit);
        return 1;
    }
    if (rc != 0) {
        free(result.current_value);
        free_criteria(crit, ncrit);
        OUT("{\"error\":\"update failed\"}\n");
        return 1;
    }
    free_criteria(crit, ncrit);

    char wire_key[1100];
    format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
    log_msg(3, "UPDATE %s.%s (slotcask)", object, wire_key);
    free(result.current_value);
    OUT("{\"status\":\"updated\",\"key\":\"%s\"}\n", wire_key);
    return 0;
}

/* ========== PARTIAL UPDATE ========== */

int cmd_update(const char *db_root, const char *object,
               const char *key, size_t klen,
               const char *partial_json,
               const char *if_json, int dry_run) {
    Schema sc = load_schema(db_root, object);

    if (sc.storage_version == 2) {
        return cmd_update_v2(db_root, object, key, klen, partial_json,
                             if_json, dry_run, &sc);
    }

    uint8_t hash[16]; int shard_id, start_slot;
    compute_addr(key, klen, sc.splits, hash, &shard_id, &start_slot);

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), db_root, object, shard_id);

    FcacheRead wh = ucache_get_write(shard, 0);
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
        char wire_key[1100];
        format_wire_key(&sc, key, klen, wire_key, sizeof(wire_key));
        OUT("{\"status\":\"would_update\",\"key\":\"%s\"}\n", wire_key);
        return 0;
    }

    /* Index fields — collect old values before modifying */
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* see fields[] re-term comment above */
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
    char wire_key[1100];
    format_wire_key(&sc, key, klen, wire_key, sizeof(wire_key));
    log_msg(3, "UPDATE %s.%s (shard=%d slot=%d)", object, wire_key, shard_id, slot);
    OUT("{\"status\":\"updated\",\"key\":\"%s\"}\n", wire_key);
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

/* ========== DELETE — v2 (slotcask) helper ========== */

typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*idx_fields)[256];
    int               nidx;
    TypedSchema      *idx_ts;
    SearchCriterion  *crit;
    int               ncrit;
} V2DeleteCtx;

static int v2_delete_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2DeleteCtx *c = (V2DeleteCtx *)ctx_ptr;
    if (c->ncrit > 0) {
        if (!old) return 0;
        if (!cas_check(c->idx_ts, old->value, c->crit, c->ncrit)) return 0;
    }
    return 1;
}

static int v2_delete_pre_commit(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2DeleteCtx *c = (V2DeleteCtx *)ctx_ptr;
    if (!old || c->nidx == 0) return 0;
    for (int i = 0; i < c->nidx; i++) {
        uint8_t *ikey = NULL;
        size_t   ilen = 0;
        if (build_index_key_from_record(c->idx_ts, old->value,
                                        c->idx_fields[i], &ikey, &ilen)) {
            delete_index_entry(c->db_root, c->object, c->idx_fields[i],
                               c->splits, ikey, ilen, c->hash);
            free(ikey);
        }
    }
    return 0;
}

static int cmd_delete_v2(const char *db_root, const char *object,
                         const char *key, size_t klen,
                         const char *if_json, int dry_run,
                         const Schema *sc) {
    char wire_key[1100];
    format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));

    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams, .storage_version = 2,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        OUT("{\"status\":\"not_found\",\"key\":\"%s\"}\n", wire_key);
        return 0;
    }

    TypedSchema *ts = load_typed_schema(db_root, object);

    /* dry_run: read + validate, never tombstone. */
    if (dry_run) {
        void *val = NULL; size_t vlen = 0;
        if (slotcask_get(sdb, key, klen, &val, &vlen) != 0) {
            OUT("{\"status\":\"not_found\",\"key\":\"%s\"}\n", wire_key);
            return 0;
        }
        if (if_json) {
            SearchCriterion *crit = NULL; int ncrit = 0;
            parse_criteria_json(if_json, &crit, &ncrit);
            int pass = cas_check(ts, val, crit, ncrit);
            if (!pass) {
                char *cur = typed_decode(ts, val, (uint32_t)vlen);
                OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
                    cur ? cur : "null");
                free(cur); free(val); free_criteria(crit, ncrit);
                return 1;
            }
            free_criteria(crit, ncrit);
        }
        free(val);
        OUT("{\"status\":\"would_delete\",\"key\":\"%s\"}\n", wire_key);
        return 0;
    }

    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';

    SearchCriterion *crit = NULL;
    int ncrit = 0;
    if (if_json) parse_criteria_json(if_json, &crit, &ncrit);

    V2DeleteCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .idx_fields = idx_fields, .nidx = nidx,
        .idx_ts = ts,
        .crit = crit, .ncrit = ncrit,
    };
    compute_hash_raw(key, klen, ctx.hash);

    /* Only set the check hook when there's actual CAS criteria — otherwise
       v2_delete_check_fn would just return 1 unconditionally but the
       primitive doesn't know that and reads OLD anyway. Combined with
       skip_old_read on non-indexed paths, a plain DELETE bypasses
       read_record_value entirely. */
    int has_cas = (crit && ncrit > 0);
    SlotcaskDeleteOpts opts = {
        .check          = has_cas ? v2_delete_check_fn : NULL,
        .check_ctx      = &ctx,
        .pre_commit     = v2_delete_pre_commit,
        .pre_commit_ctx = &ctx,
        /* pre_commit only dereferences old when there are index entries
           to drop. On non-indexed + non-CAS delete, opt out of
           read_record_value — saves a segcache_acquire + 100B memcpy +
           malloc/free per call. v2_delete_pre_commit handles old=NULL. */
        .skip_old_read  = (nidx == 0),
    };
    SlotcaskDeleteResult result = {0};
    int rc = slotcask_delete_with_hooks(sdb, key, klen, &opts, &result);

    if (result.not_found) {
        OUT("{\"status\":\"not_found\",\"key\":\"%s\"}\n", wire_key);
        free(result.current_value);
        free_criteria(crit, ncrit);
        return 0;
    }
    if (rc == -2 && result.condition_not_met) {
        char *cur = result.current_value
            ? typed_decode(ts, result.current_value, (uint32_t)result.current_vlen)
            : NULL;
        OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
            cur ? cur : "null");
        free(cur); free(result.current_value); free_criteria(crit, ncrit);
        return 1;
    }
    if (rc != 0) {
        free(result.current_value);
        free_criteria(crit, ncrit);
        OUT("{\"error\":\"delete failed\"}\n");
        return 1;
    }

    update_counts(db_root, object, -1, 1);
    log_msg(3, "DELETE %s.%s (slotcask)", object, wire_key);
    free(result.current_value);
    free_criteria(crit, ncrit);
    OUT("{\"status\":\"deleted\",\"key\":\"%s\"}\n", wire_key);
    return 0;
}

/* ========== DELETE (with probing) ========== */

int cmd_delete(const char *db_root, const char *object,
               const char *key, size_t klen,
               const char *if_json, int dry_run) {
    Schema sc = load_schema(db_root, object);

    if (sc.storage_version == 2) {
        return cmd_delete_v2(db_root, object, key, klen, if_json, dry_run, &sc);
    }

    char wire_key[1100];
    format_wire_key(&sc, key, klen, wire_key, sizeof(wire_key));

    uint8_t hash[16]; int shard_id, start_slot;
    compute_addr(key, klen, sc.splits, hash, &shard_id, &start_slot);

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), db_root, object, shard_id);

    FcacheRead wh = ucache_get_write(shard, 0);
    if (!wh.map) { OUT("{\"status\":\"not_found\",\"key\":\"%s\"}\n", wire_key); return 0; }
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
            OUT("{\"status\":\"would_delete\",\"key\":\"%s\"}\n", wire_key);
            return 0;
        }

        /* Extract indexed field values BEFORE tombstoning, for index cleanup */
        char idx_fields[MAX_FIELDS][256];
        int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
        for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* see fields[] re-term comment above */
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
        log_msg(3, "DELETE %s.%s", object, wire_key);
        OUT("{\"status\":\"deleted\",\"key\":\"%s\"}\n", wire_key);
    } else {
        ucache_write_release(wh);
        OUT("{\"status\":\"not_found\",\"key\":\"%s\"}\n", wire_key);
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
    int orig_idx;
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

    /* v2 dispatch: bulk_lookup_in_kfshard amortises kfcache_acquire +
       segcache_acquire across the worker's records — vs the old per-
       record slotcask_exists path that took both caches per call. The
       dispatcher already aligned shard_id with compute_record_shard so
       all entries here hash to the same kf shard. */
    if (sw->sch->storage_version == 2) {
        SlotcaskSchemaInfo info = {
            .splits = sw->sch->splits, .slot_size = sw->sch->slot_size,
            .streams = sw->sch->streams, .storage_version = 2,
        };
        SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info);
        if (!sdb) return NULL;

        SlotcaskBulkRec *batch = malloc(sw->count * sizeof(SlotcaskBulkRec));
        if (!batch) return NULL;
        for (int ei = 0; ei < sw->count; ei++) {
            MultiExistsEntry *e = &sw->entries[ei];
            batch[ei].key       = e->key;
            batch[ei].klen      = strlen(e->key);
            batch[ei].value     = NULL;
            batch[ei].vlen      = 0;
            batch[ei].user_ctx  = NULL;
            batch[ei].old_value = NULL;
            batch[ei].old_vlen  = 0;
            batch[ei].status    = 0;
            batch[ei].was_update = 0;
        }
        int kf_shard_id = sw->entries[0].shard_id;  /* aligned by dispatcher */
        slotcask_bulk_lookup_in_kfshard(sdb, kf_shard_id, batch, (size_t)sw->count);
        for (int ei = 0; ei < sw->count; ei++) {
            sw->entries[ei].found = (batch[ei].status == 0) ? 1 : 0;
        }
        free(batch);
        return NULL;
    }

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
            if (key_count >= key_cap) {
                key_cap *= 2;
                MultiExistsEntry *t = xrealloc_or_free(entries, key_cap * sizeof(*t));
                if (!t) {
                    /* xrealloc_or_free freed the old buffer; per-key
                       strings inside it are leaked (no double-free,
                       since the array holding their pointers is gone)
                       — acceptable on OOM. Coverity CID 1693843: the
                       previous "entries=NULL; break" left key_count>0
                       and the loop below would deref NULL. */
                    OUT("{\"error\":\"oom: bulk_exists keys\"}\n");
                    return 1;
                }
                entries = t;
            }
            MultiExistsEntry *e = &entries[key_count++];
            e->key = malloc(klen + 1); memcpy(e->key, start, klen); e->key[klen] = '\0';
            compute_addr(e->key, klen, sc.splits, e->hash, &e->shard_id, &e->start_slot);
            /* v2 alignment — see cmd_get_multi for rationale. */
            if (sc.storage_version == 2)
                e->shard_id = compute_record_shard(e->hash, sc.splits, 2);
            e->found = 0;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("{}\n"); return 0; }

    /* Bucket-sort by shard_id — replaces O(n²) insertion sort. For 10K
       keys with ~128 shards that's ~50M swaps before parallel_for even
       starts (was the dominant cost in BULK EXISTS / BULK GET). */
    for (int i = 0; i < key_count; i++) entries[i].orig_idx = i;
    int *shard_counts = calloc(sc.splits, sizeof(int));
    int *shard_to_worker = malloc(sc.splits * sizeof(int));
    for (int i = 0; i < key_count; i++) shard_counts[entries[i].shard_id]++;
    int nshard = 0;
    for (int s = 0; s < sc.splits; s++) if (shard_counts[s] > 0) nshard++;

    MultiExistsShardWork *workers = calloc(nshard, sizeof(MultiExistsShardWork));
    {
        int g = 0;
        for (int s = 0; s < sc.splits; s++) {
            if (shard_counts[s] > 0) {
                workers[g].db_root = db_root;
                workers[g].object = object;
                workers[g].sch = &sc;
                workers[g].count = 0;
                workers[g].entries = malloc(shard_counts[s] * sizeof(MultiExistsEntry));
                shard_to_worker[s] = g;
                g++;
            } else {
                shard_to_worker[s] = -1;
            }
        }
    }
    /* Single pass — place each entry into its bucket. */
    for (int i = 0; i < key_count; i++) {
        int w = shard_to_worker[entries[i].shard_id];
        workers[w].entries[workers[w].count++] = entries[i];
    }

    parallel_for(multi_exists_shard_worker, workers, nshard, sizeof(MultiExistsShardWork));

    /* Copy results back via orig_idx (no sorted[] indirection needed). */
    for (int g = 0; g < nshard; g++)
        for (int i = 0; i < workers[g].count; i++)
            entries[workers[g].entries[i].orig_idx].found = workers[g].entries[i].found;
    free(shard_counts); free(shard_to_worker);

    /* Output in original order — build the response in a single buffer
       and OUT it once. Was 10K fprintf() calls in a loop, each taking
       the per-FILE stdio lock + parsing the format string (~15+ ms for
       10K keys at 1-2 µs/call). One snprintf-into-buffer + one OUT is
       hundreds of µs. */
    if (csv_delim) {
        OUT("key%cexists\n", csv_delim);
        size_t cap = (size_t)key_count * 64 + 64;
        char *buf = malloc(cap);
        size_t pos = 0;
        if (buf) for (int i = 0; i < key_count; i++) {
            size_t klen = strlen(entries[i].key);
            if (pos + klen + 16 > cap) {
                cap = (pos + klen + 16) * 2;
                char *t = realloc(buf, cap);
                if (!t) { free(buf); buf = NULL; break; }
                buf = t;
            }
            /* Coverity: re-assert the post-grow invariant in subtractive
               form (no addition-overflow path). The pre-grow above already
               guarantees this — the check is dead code on the happy path
               and the compiler DCEs it — but Coverity's flow analysis
               loses the size-aliasing through the realloc→t→buf chain
               and flags the memcpy below as OVERRUN. CID 1693857/1693869. */
            if (cap < pos || cap - pos < klen) { free(buf); buf = NULL; break; }
            /* csv_emit_cell quotes if needed; do it via the existing helper but
               into our buffer via snprintf — replicate the no-quote-needed shape
               here to avoid re-implementing the quote logic. */
            /* CID 1693857/1693869 - bounds checked above, triage */
            memcpy(buf + pos, entries[i].key, klen); pos += klen;
            /* Use SB_APPEND for bounded write — pre-grow above guarantees
               room, but the macro silences the CodeQL "potentially
               overflowing snprintf" finding by clamping to cap-1. */
            SB_APPEND(buf, pos, cap, "%c%s\n",
                       csv_delim, entries[i].found ? "true" : "false");
        }
        if (buf) {
            fwrite(buf, 1, pos, g_out ? g_out : stdout);
            free(buf);
        }
    } else {
        size_t cap = (size_t)key_count * 32 + 32;
        char *buf = malloc(cap);
        size_t pos = 0;
        if (buf) {
            buf[pos++] = '{';
            for (int i = 0; i < key_count; i++) {
                size_t klen = strlen(entries[i].key);
                if (pos + klen + 16 > cap) {
                    cap = (pos + klen + 16) * 2;
                    char *t = realloc(buf, cap);
                    if (!t) { free(buf); buf = NULL; break; }
                    buf = t;
                }
                /* Coverity: subtractive re-assertion of the post-grow
                   invariant — see the equivalent comment in the CSV branch
                   above for rationale. Tautological on the happy path. */
                if (cap < pos || cap - pos < klen + 16) { free(buf); buf = NULL; break; }
                if (i) buf[pos++] = ',';
                buf[pos++] = '"';
                memcpy(buf + pos, entries[i].key, klen); pos += klen;
                buf[pos++] = '"'; buf[pos++] = ':';
                if (entries[i].found) {
                    memcpy(buf + pos, "true", 4); pos += 4;
                } else {
                    memcpy(buf + pos, "false", 5); pos += 5;
                }
            }
        }
        if (buf) {
            buf[pos++] = '}'; buf[pos++] = '\n';
            fwrite(buf, 1, pos, g_out ? g_out : stdout);
            free(buf);
        } else {
            OUT("{}\n");
        }
    }

    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers);
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
            if (key_count >= key_cap) {
                key_cap *= 2;
                MultiExistsEntry *t = xrealloc_or_free(entries, key_cap * sizeof(*t));
                if (!t) {
                    /* See cmd_exists_multi above for rationale (Coverity
                       CID 1693844). */
                    OUT("{\"error\":\"oom: bulk_get keys\"}\n");
                    return 1;
                }
                entries = t;
            }
            MultiExistsEntry *e = &entries[key_count++];
            e->key = malloc(klen + 1); memcpy(e->key, start, klen); e->key[klen] = '\0';
            compute_addr(e->key, klen, sc.splits, e->hash, &e->shard_id, &e->start_slot);
            /* v2 alignment — see cmd_get_multi for rationale. */
            if (sc.storage_version == 2)
                e->shard_id = compute_record_shard(e->hash, sc.splits, 2);
            e->found = 0;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("[]\n"); return 0; }

    /* Bucket-sort by shard_id — same fix as cmd_exists_multi above. */
    for (int i = 0; i < key_count; i++) entries[i].orig_idx = i;
    int *shard_counts = calloc(sc.splits, sizeof(int));
    int *shard_to_worker = malloc(sc.splits * sizeof(int));
    for (int i = 0; i < key_count; i++) shard_counts[entries[i].shard_id]++;
    int nshard = 0;
    for (int s = 0; s < sc.splits; s++) if (shard_counts[s] > 0) nshard++;

    MultiExistsShardWork *workers = calloc(nshard, sizeof(MultiExistsShardWork));
    {
        int g = 0;
        for (int s = 0; s < sc.splits; s++) {
            if (shard_counts[s] > 0) {
                workers[g].db_root = db_root;
                workers[g].object = object;
                workers[g].sch = &sc;
                workers[g].count = 0;
                workers[g].entries = malloc(shard_counts[s] * sizeof(MultiExistsEntry));
                shard_to_worker[s] = g;
                g++;
            } else {
                shard_to_worker[s] = -1;
            }
        }
    }
    for (int i = 0; i < key_count; i++) {
        int w = shard_to_worker[entries[i].shard_id];
        workers[w].entries[workers[w].count++] = entries[i];
    }

    parallel_for(multi_exists_shard_worker, workers, nshard, sizeof(MultiExistsShardWork));

    for (int g = 0; g < nshard; g++)
        for (int i = 0; i < workers[g].count; i++)
            entries[workers[g].entries[i].orig_idx].found = workers[g].entries[i].found;
    free(shard_counts); free(shard_to_worker);

    /* Build output in one buffer; one fwrite. */
    size_t cap = (size_t)key_count * 32 + 16;
    char *buf = malloc(cap);
    if (buf) {
        size_t pos = 0;
        buf[pos++] = '[';
        int first = 1;
        for (int i = 0; i < key_count; i++) {
            if (entries[i].found) continue;
            size_t klen = strlen(entries[i].key);
            if (pos + klen + 8 > cap) {
                cap = (pos + klen + 8) * 2;
                char *t = realloc(buf, cap);
                if (!t) { free(buf); buf = NULL; break; }
                buf = t;
            }
            /* Coverity: subtractive re-assertion of the post-grow invariant. CID 1693871 */
            if (cap < pos || cap - pos < klen + 8) { free(buf); buf = NULL; break; }
            if (!first) buf[pos++] = ',';
            buf[pos++] = '"';
            /* CID 1693871 - bounds checked above, triage */
            memcpy(buf + pos, entries[i].key, klen); pos += klen;
            buf[pos++] = '"';
            first = 0;
        }
        if (buf) {
            buf[pos++] = ']'; buf[pos++] = '\n';
            fwrite(buf, 1, pos, g_out ? g_out : stdout);
            free(buf);
        }
    }
    if (!buf) OUT("[]\n");

    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers);
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
    int orig_idx;
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

    /* v2 dispatch: bulk_get_in_kfshard amortises kfcache + segcache
       across the worker's records — vs the old per-record slotcask_get
       which took both caches per call. typed_decode still happens per
       record (it's per-record-shape work). The dispatcher already
       aligned shard_id with compute_record_shard. */
    if (sw->sch->storage_version == 2) {
        SlotcaskSchemaInfo info = {
            .splits = sw->sch->splits, .slot_size = sw->sch->slot_size,
            .streams = sw->sch->streams, .storage_version = 2,
        };
        SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info);
        if (!sdb) return NULL;

        SlotcaskBulkRec *batch = malloc(sw->count * sizeof(SlotcaskBulkRec));
        void **vals = calloc(sw->count, sizeof(void *));
        size_t *vlens = calloc(sw->count, sizeof(size_t));
        if (!batch || !vals || !vlens) {
            free(batch); free(vals); free(vlens);
            return NULL;
        }
        for (int ei = 0; ei < sw->count; ei++) {
            MultiGetEntry *e = &sw->entries[ei];
            batch[ei].key       = e->key;
            batch[ei].klen      = strlen(e->key);
            batch[ei].value     = NULL;
            batch[ei].vlen      = 0;
            batch[ei].user_ctx  = NULL;
            batch[ei].old_value = NULL;
            batch[ei].old_vlen  = 0;
            batch[ei].status    = 0;
            batch[ei].was_update = 0;
        }
        int kf_shard_id = sw->entries[0].shard_id;
        slotcask_bulk_get_in_kfshard(sdb, kf_shard_id, batch, (size_t)sw->count,
                                       vals, vlens);
        for (int ei = 0; ei < sw->count; ei++) {
            MultiGetEntry *e = &sw->entries[ei];
            if (batch[ei].status == 0 && vals[ei]) {
                char *decoded = sw->fs ? typed_decode(sw->fs->ts,
                                                       (const uint8_t *)vals[ei],
                                                       (uint32_t)vlens[ei]) : NULL;
                e->result_json = decoded ? decoded : strdup("null");
                free(vals[ei]);
            }
        }
        free(batch); free(vals); free(vlens);
        return NULL;
    }

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
                /* Store just the decoded value JSON. */
                e->result_json = val ? val : strdup("null");
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
            if (key_count >= key_cap) {
                key_cap *= 2;
                MultiGetEntry *t = xrealloc_or_free(entries, key_cap * sizeof(*t));
                if (!t) { entries = NULL; break; }
                entries = t;
            }
            MultiGetEntry *e = &entries[key_count++];
            e->key = malloc(klen + 1); memcpy(e->key, start, klen); e->key[klen] = '\0';
            compute_addr(e->key, klen, sc.splits, e->hash, &e->shard_id, &e->start_slot);
            /* For v2, override the bucketing shard with slotcask's kf-shard
               mapping so each parallel_for worker owns one kf shard and
               doesn't queue on cross-worker kf-cache contention. */
            if (sc.storage_version == 2)
                e->shard_id = compute_record_shard(e->hash, sc.splits, 2);
            e->result_json = NULL;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("{}\n"); return 0; }

    /* Bucket-sort by shard_id — same fix as cmd_exists_multi above. */
    for (int i = 0; i < key_count; i++) entries[i].orig_idx = i;
    int *shard_counts = calloc(sc.splits, sizeof(int));
    int *shard_to_worker = malloc(sc.splits * sizeof(int));
    for (int i = 0; i < key_count; i++) shard_counts[entries[i].shard_id]++;
    int nshard = 0;
    for (int s = 0; s < sc.splits; s++) if (shard_counts[s] > 0) nshard++;

    FieldSchema fs; init_field_schema(&fs, db_root, object);
    MultiGetShardWork *workers = calloc(nshard, sizeof(MultiGetShardWork));
    {
        int g = 0;
        for (int s = 0; s < sc.splits; s++) {
            if (shard_counts[s] > 0) {
                workers[g].db_root = db_root;
                workers[g].object = object;
                workers[g].sch = &sc;
                workers[g].fs = (fs.ts || fs.nfields > 0) ? &fs : NULL;
                workers[g].count = 0;
                workers[g].entries = malloc(shard_counts[s] * sizeof(MultiGetEntry));
                shard_to_worker[s] = g;
                g++;
            } else {
                shard_to_worker[s] = -1;
            }
        }
    }
    for (int i = 0; i < key_count; i++) {
        int w = shard_to_worker[entries[i].shard_id];
        workers[w].entries[workers[w].count++] = entries[i];
    }

    /* Parallel fetch */
    parallel_for(multi_get_shard_worker, workers, nshard, sizeof(MultiGetShardWork));

    /* Copy results back to entries[] via orig_idx. */
    for (int g = 0; g < nshard; g++)
        for (int i = 0; i < workers[g].count; i++)
            entries[workers[g].entries[i].orig_idx].result_json = workers[g].entries[i].result_json;
    free(shard_counts); free(shard_to_worker);

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
            csv_emit_cell(entries[i].key, csv_delim);
            JsonObj value_obj;
            int have_value = json_parse_object(entries[i].result_json,
                                               strlen(entries[i].result_json),
                                               &value_obj) >= 0;
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
        /* Build the response in one buffer + one fwrite. Was 10K+
           fprintf calls in a loop (each takes the per-FILE stdio lock
           + parses the format string), the dominant cost in BULK GET
           on top of the bucket-sort fix. */
        size_t cap = (size_t)key_count * 256 + 64;
        char *buf = malloc(cap);
        if (buf) {
            size_t pos = 0;
            buf[pos++] = '{';
            int first = 1;
            for (int i = 0; i < key_count; i++) {
                size_t klen = strlen(entries[i].key);
                size_t vlen = entries[i].result_json ? strlen(entries[i].result_json) : 4;
                if (pos + klen + vlen + 16 > cap) {
                    cap = (pos + klen + vlen + 16) * 2;
                    char *t = realloc(buf, cap);
                    if (!t) { free(buf); buf = NULL; break; }
                    buf = t;
                }
                /* Coverity: subtractive re-assertion of the post-grow invariant. CID 1693870 */
                if (cap < pos || cap - pos < klen + vlen + 16) { free(buf); buf = NULL; break; }
                if (!first) buf[pos++] = ',';
                first = 0;
                buf[pos++] = '"';
                /* CID 1693870 - bounds checked above, triage */
                memcpy(buf + pos, entries[i].key, klen); pos += klen;
                buf[pos++] = '"'; buf[pos++] = ':';
                if (entries[i].result_json) {
                    /* CID 1693872 - bounds checked above, triage */
                    memcpy(buf + pos, entries[i].result_json, vlen); pos += vlen;
                    free(entries[i].result_json);
                } else {
                    memcpy(buf + pos, "null", 4); pos += 4;
                }
            }
            if (buf) {
                buf[pos++] = '}'; buf[pos++] = '\n';
                fwrite(buf, 1, pos, g_out ? g_out : stdout);
                free(buf);
            }
        }
        if (!buf) {
            /* OOM fallback — old per-record path. */
            OUT("{");
            int first = 1;
            for (int i = 0; i < key_count; i++) {
                if (!first) OUT(",");
                first = 0;
                if (entries[i].result_json) {
                    OUT("\"%s\":%s", entries[i].key, entries[i].result_json);
                    free(entries[i].result_json);
                } else {
                    OUT("\"%s\":null", entries[i].key);
                }
            }
            OUT("}\n");
        }
    }

    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers);
    for (int i = 0; i < key_count; i++) free(entries[i].key);
    free(entries);
    return 0;
}

