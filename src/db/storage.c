#include "types.h"
#include "slotcask.h"

/* Hashing & shard derivation live in util.c (compute_hash_raw) and
   slotcask.c (compute_record_shard) — single source of truth across the
   engine and storage layer. */

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
/* g_ucache* moved to ShardDb struct */

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

void ucache_shutdown(void) {
    pthread_mutex_lock(&g_ucache_table_mutex);
    if (g_ucache) {
        for (int i = 0; i < g_ucache_slots; i++) {
            UCacheEntry *e = &g_ucache[i];
            if (e->map && e->map_size)
                munmap(e->map, e->map_size);
            if (e->fd >= 0)
                close(e->fd);
        }
        free(g_ucache);
        g_ucache = NULL;
        g_ucache_slots = 0;
        g_ucache_count = 0;
    }
    pthread_mutex_unlock(&g_ucache_table_mutex);
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
            /* Invalidate any SlotRef pointing at this slot. */
            atomic_fetch_add_explicit(&victim->gen, 1, memory_order_release);
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
                    LOG_WARN(LOG_SUB_SLOTCASK, "grow_recovery: unlinked stale %s/%s",
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
        atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
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
/* CountsCacheEntry, COUNTS_CACHE_BUCKETS moved to shard_db_internal.h;
   g_counts_cache, g_counts_lock moved to ShardDb struct */
#define COUNTS_FLUSH_INTERVAL 10000   /* atomic updates between auto-flushes */

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

/* The slotcask kf header is the source of truth for record counts —
   slotcask_put / slotcask_delete update it atomically under the kf-shard
   wrlock. The four mutators below remain as no-ops so existing callers
   (bulk-insert / vacuum / truncate / etc.) keep their bookkeeping shape
   without forcing every site to dispatch on storage layout. */
void update_count(const char *db_root, const char *object, int delta) {
    (void)db_root; (void)object; (void)delta;
}

void update_deleted_count(const char *db_root, const char *object, int delta) {
    (void)db_root; (void)object; (void)delta;
}

void set_count(const char *db_root, const char *object, int count) {
    (void)db_root; (void)object; (void)count;
}

void reset_deleted_count(const char *db_root, const char *object) {
    (void)db_root; (void)object;
}

/* Resolve (live, deleted) for an object. Sums kf headers — each is updated
   atomically inside slotcask_put / slotcask_delete and is the single source
   of truth for record counts (cannot go stale across daemon crashes the
   way a separate counts file would).

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
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size,
        .streams = sc.streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(eff_root, bare_obj, &info);
    if (!sdb) { *out_live = 0; *out_deleted = 0; return -1; }
    uint64_t total = 0, deleted = 0;
    slotcask_sum_kf_totals(sdb, &total, &deleted);
    *out_live    = total > deleted ? total - deleted : 0;
    *out_deleted = deleted;
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

    /* Route through slotcask. Wire response shape (bare value dict for
       single-key get, per 2026.05.1) is preserved. */
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size,
        .streams = sc.streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("{\"error\":\"Not found\"}\n"); return 1; }
    void *val = NULL; size_t vlen = 0;
    if (slotcask_get(sdb, key, klen, &val, &vlen) != 0) {
        OUT("{\"error\":\"Not found\"}\n");
        return 1;
    }
    LOG_DEBUG(LOG_SUB_SLOTCASK, "GET %s (klen=%zu, %zu bytes)", object, klen, vlen);
    TypedSchema *ts = load_typed_schema(db_root, object);
    typed_decode_stream(ts, (const uint8_t *)val, (uint32_t)vlen,
                         g_out ? g_out : stdout);
    fputc('\n', g_out ? g_out : stdout);
    free(val);
    return 0;
}

/* ========== CAS (Compare-and-Swap) helper ========== */

/* Check all criteria against the current record value (typed binary).
   value_len is the number of valid bytes in value_ptr (may be < ts->total_size
   for trim-encoded records). Returns 1 if ALL criteria match, 0 on first failure. */
int cas_check(TypedSchema *ts, const uint8_t *value_ptr, int value_len,
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
                    char *v = typed_get_field_str(ts, value_ptr, value_len, fi);
                    if (v) { int sl = strlen(v); memcpy(cat + cp, v, sl); cp += sl; free(v); }
                    else { ok = 0; break; }
                } else { ok = 0; break; }
                tok = strtok_r(NULL, "+", &_tok_save);
            }
            cat[cp] = '\0';
            val_str = (ok && cp > 0) ? strdup(cat) : NULL;
        } else {
            int fi = typed_field_index(ts, crit[i].field);
            if (fi >= 0) val_str = typed_get_field_str(ts, value_ptr, value_len, fi);
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
    enum IndexType   *idx_types;       /* parallel to fields[], loaded once */
    const char       *value_json;
    TypedSchema      *idx_ts;
    SearchCriterion  *crit;
    int               ncrit;
    /* Populated by slotcask BEFORE pre_commit so bitmap update_idx_fn can
       address the just-written record by (shard, slot). */
    int               kf_shard;
    uint32_t          kf_slot;
    /* Populated by pre_commit when a bitmap-index field's per-file cap
       is exceeded. cmd_insert_v2 surfaces this as the wire-level error
       so the operator gets an actionable message + the field name. */
    char              err_buf[256];
} V2InsertCtx;

static int v2_insert_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2InsertCtx *c = (V2InsertCtx *)ctx_ptr;
    if (c->ncrit > 0) {
        /* if_json criteria require an existing record; reject if missing. */
        if (!old) return 0;
        if (!cas_check(c->idx_ts, old->value, (int)old->vlen, c->crit, c->ncrit)) return 0;
    }
    return 1;
}

/* UpdateIdxArg / update_idx_fn now live in index.c (declared in types.h)
   so query.c can dispatch the same per-field worker for its bulk
   update/delete pre_commits. */

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
                args[n_args].type    = c->idx_types ? c->idx_types[i] : IT_BTREE;
                args[n_args].kf_shard = c->kf_shard;
                args[n_args].kf_slot  = c->kf_slot;
                args[n_args].bm_max_values = 0;  /* default cap — header wins on existing */
                n_args++;
            } else {
                /* Unchanged — free immediately, nothing to dispatch. */
                free(new_key); free(old_key);
            }
        }
        int bm_overflow = 0;
        if (n_args > 0) {
            parallel_for(update_idx_fn, args, n_args, sizeof(UpdateIdxArg));
            for (int i = 0; i < n_args; i++) {
                if (args[i].out_error == -1 && !c->err_buf[0]) {
                    bm_overflow = 1;
                    snprintf(c->err_buf, sizeof(c->err_buf),
                        "bitmap index on field '%s' exceeded distinct-value cap "
                        "(this insert/update would push the dict past the per-file limit). "
                        "Either declare a higher cap with field:bitmap(N), or switch to btree: "
                        "remove-index then add-index without :bitmap.",
                        args[i].field);
                }
                free(args[i].new_key);
                free(args[i].old_key);
            }
        }
        free(old_json);
        bm_flush_thread_bitmap_cache();
        if (bm_overflow) return -1;
    } else {
        /* Fresh insert: parallel write of all index entries. Btree entries
           still go through the original index_parallel path (which knows
           about composites and shares unique-key extraction); bitmap
           entries dispatch through update_idx_fn so the type-aware code
           in index.c maintains them. */
        index_parallel(c->db_root, c->object, c->splits,
                       c->value_json, c->hash, c->fields, c->nfields,
                       c->idx_types);

        if (c->idx_types) {
            UpdateIdxArg bm_args[MAX_FIELDS];
            int n_bm = 0;
            for (int i = 0; i < c->nfields; i++) {
                if (c->idx_types[i] != IT_BITMAP) continue;
                /* Composite + bitmap is rejected at create-object; defensive. */
                if (strchr(c->fields[i], '+')) continue;
                uint8_t *nk = NULL;
                size_t nl = 0;
                if (!build_index_key_from_json(c->idx_ts, c->value_json,
                                                c->fields[i], &nk, &nl))
                    continue;
                bm_args[n_bm].db_root = c->db_root;
                bm_args[n_bm].object  = c->object;
                bm_args[n_bm].field   = c->fields[i];
                bm_args[n_bm].splits  = c->splits;
                bm_args[n_bm].new_key = nk;
                bm_args[n_bm].new_len = nl;
                bm_args[n_bm].old_key = NULL;
                bm_args[n_bm].old_len = 0;
                bm_args[n_bm].hash    = c->hash;
                bm_args[n_bm].type    = IT_BITMAP;
                bm_args[n_bm].kf_shard = c->kf_shard;
                bm_args[n_bm].kf_slot  = c->kf_slot;
                bm_args[n_bm].bm_max_values = 0;
                n_bm++;
            }
            int bm_overflow = 0;
            if (n_bm > 0) {
                parallel_for(update_idx_fn, bm_args, n_bm, sizeof(UpdateIdxArg));
                for (int i = 0; i < n_bm; i++) {
                    if (bm_args[i].out_error == -1 && !c->err_buf[0]) {
                        bm_overflow = 1;
                        snprintf(c->err_buf, sizeof(c->err_buf),
                            "bitmap index on field '%s' exceeded distinct-value cap "
                            "(this insert would push the dict past the per-file limit). "
                            "Either declare a higher cap with field:bitmap(N), or switch to btree.",
                            bm_args[i].field);
                    }
                    free(bm_args[i].new_key);
                }
            }
            bm_flush_thread_bitmap_cache();
            if (bm_overflow) return -1;

            /* Trigram indexes — same dispatch shape as bitmap above, but
               no overflow path (no per-file cap). update_idx_fn's
               IT_TRIGRAM branch extracts distinct trigrams from new_key
               and writes one .tg leaf entry per (trigram, record hash). */
            UpdateIdxArg tg_args[MAX_FIELDS];
            int n_tg = 0;
            for (int i = 0; i < c->nfields; i++) {
                if (c->idx_types[i] != IT_TRIGRAM) continue;
                if (strchr(c->fields[i], '+')) continue;  /* composite + trigram = rejected upstream */
                uint8_t *nk = NULL;
                size_t nl = 0;
                if (!build_index_key_from_json(c->idx_ts, c->value_json,
                                                c->fields[i], &nk, &nl))
                    continue;
                tg_args[n_tg].db_root  = c->db_root;
                tg_args[n_tg].object   = c->object;
                tg_args[n_tg].field    = c->fields[i];
                tg_args[n_tg].splits   = c->splits;
                tg_args[n_tg].new_key  = nk;
                tg_args[n_tg].new_len  = nl;
                tg_args[n_tg].old_key  = NULL;
                tg_args[n_tg].old_len  = 0;
                tg_args[n_tg].hash     = c->hash;
                tg_args[n_tg].type     = IT_TRIGRAM;
                tg_args[n_tg].kf_shard = c->kf_shard;
                tg_args[n_tg].kf_slot  = c->kf_slot;
                tg_args[n_tg].bm_max_values = 0;
                n_tg++;
            }
            if (n_tg > 0) {
                parallel_for(update_idx_fn, tg_args, n_tg, sizeof(UpdateIdxArg));
                for (int i = 0; i < n_tg; i++) free(tg_args[i].new_key);
            }
        }
    }
    return 0;
}

/* Produce the "now" string for an auto_create / auto_update timestamp field in
   the form its type expects. buf must be >= 24 bytes.
     FT_TIMESTAMP  — Unix epoch ms (decimal)
     FT_DATETIMEMS — yyyyMMddHHmmssSSS
     FT_DATE       — yyyyMMdd
     other (FT_DATETIME / fallback) — yyyyMMddHHmmss */
void auto_now_str(const TypedField *f, char *buf, size_t bufsz) {
    if (f->type == FT_TIMESTAMP) {
        struct timespec tsn; clock_gettime(CLOCK_REALTIME, &tsn);
        long long ms = (long long)tsn.tv_sec * 1000LL + tsn.tv_nsec / 1000000LL;
        snprintf(buf, bufsz, "%lld", ms);
    } else if (f->type == FT_DATETIMEMS) {
        struct timespec tsn; clock_gettime(CLOCK_REALTIME, &tsn);
        time_t nowsec = tsn.tv_sec; struct tm tm; localtime_r(&nowsec, &tm);
        int msec = (int)(tsn.tv_nsec / 1000000L);
        snprintf(buf, bufsz, "%04d%02d%02d%02d%02d%02d%03d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec, msec);
    } else {
        time_t now = time(NULL); struct tm tm; localtime_r(&now, &tm);
        if (f->type == FT_DATE)
            snprintf(buf, bufsz, "%04d%02d%02d",
                     tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
        else
            snprintf(buf, bufsz, "%04d%02d%02d%02d%02d%02d",
                     tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                     tm.tm_hour, tm.tm_min, tm.tm_sec);
    }
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
    char enc_err[512] = {0};
    int enc = typed_encode_defaults(ts, value, typed_buf, ts->total_size,
                                    db_root, object, enc_err, sizeof(enc_err));
    if (enc == -2) {
        /* Strict enum (or future typed) validation rejection — actionable
           error already in enc_err. Caller never sees a successfully-
           encoded but semantically-corrupt record. */
        free(typed_buf);
        OUT("{\"error\":\"%s\"}\n", enc_err);
        return 1;
    }
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
        .streams = sc->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        free(typed_buf);
        OUT("{\"error\":\"Cannot open shard\"}\n");
        return 1;
    }

    /* Wire up compact trim for VARIABLE-format typed objects. */
    if (sdb->format == SLOTCASK_FORMAT_VARIABLE && !sdb->trim_fn) {
        sdb->trim_fn  = schema_trim_fn;
        sdb->trim_ctx = (void *)ts;
    }

    /* auto_create: stamp now() on first insert only; preserve the stored value
       on any update / re-insert. This path is an upsert, so a re-insert of an
       existing key would otherwise zero the create-time (typed_encode_defaults
       leaves DK_AUTO_CREATE fields blank). We consult the prior record — but
       only when the schema actually declares an auto_create field, so ordinary
       objects pay nothing. */
    {
        int has_ac = 0;
        for (int i = 0; i < ts->nfields; i++)
            if (!ts->fields[i].removed &&
                ts->fields[i].default_kind == DK_AUTO_CREATE) { has_ac = 1; break; }
        if (has_ac) {
            void *ac_old = NULL; size_t ac_old_vlen = 0;
            int existed = (slotcask_get(sdb, key, klen, &ac_old, &ac_old_vlen) == 0);
            for (int i = 0; i < ts->nfields; i++) {
                if (ts->fields[i].removed ||
                    ts->fields[i].default_kind != DK_AUTO_CREATE) continue;
                size_t off = (size_t)ts->fields[i].offset;
                size_t w   = (size_t)ts->fields[i].size;
                if (existed && ac_old && ac_old_vlen >= off + w) {
                    memcpy(typed_buf + off, (uint8_t *)ac_old + off, w);
                } else if (!existed) {
                    /* Re-stamp unconditionally, even though typed_encode_defaults
                       already stamped now() for a client-omitted field. Do NOT
                       "optimize" this away: if the client explicitly supplied a
                       value for this field, typed_encode_defaults wrote THAT
                       value (seen[i]=1 skips generate_default), and this is the
                       only place that overwrites it — removing this branch lets
                       a client-supplied auto_create value survive a fresh
                       insert, which the field's contract forbids. The extra
                       clock_gettime on fresh inserts is negligible. */
                    char tbuf[24];
                    auto_now_str(&ts->fields[i], tbuf, sizeof(tbuf));
                    encode_field(&ts->fields[i], tbuf, typed_buf + off);
                }
                /* existed but old too short (field added post-hoc): leave blank. */
            }
            free(ac_old);
        }
    }

    /* Index fields + criteria (only parsed if if_json is present). */
    char fields[MAX_FIELDS][256];
    int nfields = load_index_fields(db_root, object, fields, MAX_FIELDS);
    for (int _i = 0; _i < nfields; _i++) fields[_i][255] = '\0';

    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    SearchCriterion *crit = NULL;
    int ncrit = 0;
    if (if_json && parse_criteria_json(if_json, &crit, &ncrit) != 0) {
        free(typed_buf);
        OUT("{\"error\":\"invalid if condition\"}\n");
        return 1;
    }

    V2InsertCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .fields = fields, .nfields = nfields,
        .idx_types = idx_types,
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
        /* Bitmap index needs (shard, slot) — slotcask writes them here
           before invoking pre_commit. update_idx_fn reads them via
           V2InsertCtx (same struct, no second indirection). */
        .out_kf_shard   = &ctx.kf_shard,
        .out_kf_slot    = &ctx.kf_slot,
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
        if (ctx.err_buf[0]) {
            OUT("{\"error\":\"%s\"}\n", ctx.err_buf);
        } else {
            OUT("{\"error\":\"upsert failed\"}\n");
        }
        return 1;
    }

    if (!result.was_update) update_count(db_root, object, 1);
    char wire_key[1100];
    format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
    LOG_INFO(LOG_SUB_SLOTCASK, "%s %s.%s (slotcask)",
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
    return cmd_insert_v2(db_root, object, key, klen, value, if_json,
                         if_not_exists, &sc);
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
    enum IndexType   *idx_types;        /* parallel to idx_fields[] */
    TypedSchema      *idx_ts;
    /* CAS criteria — verified inside check_fn under the kf-shard wrlock
       so the check + commit are atomic against concurrent writers. NULL
       when the caller didn't pass `if`. */
    SearchCriterion  *crit;
    int               ncrit;
    /* Populated by slotcask BEFORE pre_commit (bitmap addresses records
       by physical slot, not by hash). */
    int               kf_shard;
    uint32_t          kf_slot;
    /* Populated by pre_commit on bitmap-index cap overflow. */
    char              err_buf[256];
} V2UpdateCtx;

static int v2_update_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2UpdateCtx *c = (V2UpdateCtx *)ctx_ptr;
    if (!old) return 0;  /* require_existing handles this, but defensive */
    if (c->crit && c->ncrit > 0 &&
        !cas_check(c->idx_ts, old->value, (int)old->vlen, c->crit, c->ncrit)) return 0;
    return 1;
}

static int v2_update_pre_commit(const SlotcaskOldRecord *old,
                                const uint8_t *new_value, size_t new_vlen,
                                int is_update, void *ctx_ptr) {
    (void)new_vlen; (void)is_update;
    V2UpdateCtx *c = (V2UpdateCtx *)ctx_ptr;
    if (!old || c->nidx == 0) return 0;

    /* Phase 1 (serial): diff each field; build a dispatch list of only
       the fields that actually changed. Phase 2 (parallel): apply the
       changes via parallel_for + update_idx_fn. For a 14-idx workload,
       parallel drops the field-update wall time from N*~1µs serial to
       ~1µs parallel.

       Index keys live in a single arena slab allocated once per call.
       Slot size INDEX_KEY_MAX (4096) matches the composite-cap that
       build_index_key_from_record_into honours. Replaces 2*nidx
       malloc/free pairs with 1 per call. Oversized varchar indexes
       (>4096) fall back via the malloc'd variant. */
    enum { INDEX_KEY_MAX = 4096 };
    size_t arena_bytes = (size_t)c->nidx * (size_t)(2 * INDEX_KEY_MAX);
    uint8_t *arena = malloc(arena_bytes);
    UpdateIdxArg args[MAX_FIELDS];
    uint8_t *fb_bufs[2 * MAX_FIELDS]; int n_fb = 0;
    int n_args = 0;

    for (int i = 0; i < c->nidx; i++) {
        uint8_t *old_slot = arena ? arena + (size_t)i * 2 * INDEX_KEY_MAX : NULL;
        uint8_t *new_slot = old_slot ? old_slot + INDEX_KEY_MAX : NULL;
        size_t old_len = 0, new_len = 0;
        int have_old = 0, have_new = 0;
        uint8_t *old_buf = NULL, *new_buf = NULL;

        if (arena) {
            int ro = build_index_key_from_record_into(c->idx_ts, old->value,
                                                       c->idx_fields[i],
                                                       old_slot, INDEX_KEY_MAX, &old_len);
            int rn = build_index_key_from_record_into(c->idx_ts, new_value,
                                                       c->idx_fields[i],
                                                       new_slot, INDEX_KEY_MAX, &new_len);
            have_old = (ro == 1);
            have_new = (rn == 1);
            old_buf = have_old ? old_slot : NULL;
            new_buf = have_new ? new_slot : NULL;
            /* If either side overflowed the arena slot, fall back to malloc
               for this field — preserves correctness for jumbo varchar
               indexes. */
            if (ro == -1) {
                have_old = build_index_key_from_record(c->idx_ts, old->value,
                                                       c->idx_fields[i], &old_buf, &old_len);
                if (have_old) fb_bufs[n_fb++] = old_buf;
            }
            if (rn == -1) {
                have_new = build_index_key_from_record(c->idx_ts, new_value,
                                                       c->idx_fields[i], &new_buf, &new_len);
                if (have_new) fb_bufs[n_fb++] = new_buf;
            }
        } else {
            /* Arena alloc failed — fall back to per-field malloc. */
            have_old = build_index_key_from_record(c->idx_ts, old->value,
                                                   c->idx_fields[i], &old_buf, &old_len);
            have_new = build_index_key_from_record(c->idx_ts, new_value,
                                                   c->idx_fields[i], &new_buf, &new_len);
            if (have_old) fb_bufs[n_fb++] = old_buf;
            if (have_new) fb_bufs[n_fb++] = new_buf;
        }

        int changed = 0;
        if (have_new && !have_old) changed = 1;
        else if (!have_new && have_old) changed = 1;
        else if (have_new && have_old) {
            if (new_len != old_len ||
                memcmp(new_buf, old_buf, new_len) != 0) changed = 1;
        }
        if (changed) {
            args[n_args].db_root = c->db_root;
            args[n_args].object  = c->object;
            args[n_args].field   = c->idx_fields[i];
            args[n_args].splits  = c->splits;
            args[n_args].new_key = have_new ? new_buf : NULL;
            args[n_args].new_len = new_len;
            args[n_args].old_key = have_old ? old_buf : NULL;
            args[n_args].old_len = old_len;
            args[n_args].hash    = c->hash;
            args[n_args].type    = c->idx_types ? c->idx_types[i] : IT_BTREE;
            args[n_args].kf_shard = c->kf_shard;
            args[n_args].kf_slot  = c->kf_slot;
            args[n_args].bm_max_values = 0;
            n_args++;
        }
    }

    int bm_overflow = 0;
    if (n_args > 0) {
        parallel_for(update_idx_fn, args, n_args, sizeof(UpdateIdxArg));
        for (int i = 0; i < n_args; i++) {
            if (args[i].out_error == -1 && !c->err_buf[0]) {
                bm_overflow = 1;
                snprintf(c->err_buf, sizeof(c->err_buf),
                    "bitmap index on field '%s' exceeded distinct-value cap "
                    "(this update would push the dict past the per-file limit). "
                    "Either declare a higher cap with field:bitmap(N), or switch to btree.",
                    args[i].field);
            }
        }
    }
    for (int i = 0; i < n_fb; i++) free(fb_bufs[i]);
    free(arena);
    bm_flush_thread_bitmap_cache();
    return bm_overflow ? -1 : 0;
}

static int cmd_update_v2(const char *db_root, const char *object,
                         const char *key, size_t klen,
                         const char *partial_json,
                         const char *if_json, int dry_run, const Schema *sc) {
    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("{\"error\":\"Not found\"}\n"); return 1; }

    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) { OUT("{\"error\":\"Object not found\"}\n"); return 1; }

    /* Wire up compact trim for VARIABLE-format typed objects. */
    if (sdb->format == SLOTCASK_FORMAT_VARIABLE && !sdb->trim_fn) {
        sdb->trim_fn  = schema_trim_fn;
        sdb->trim_ctx = (void *)ts;
    }

    void *old_val = NULL; size_t old_vlen = 0;
    if (slotcask_get(sdb, key, klen, &old_val, &old_vlen) != 0) {
        OUT("{\"error\":\"Not found\"}\n");
        return 1;
    }

    /* dry_run validates criteria but doesn't write — race-tolerant. */
    if (dry_run) {
        if (if_json) {
            SearchCriterion *crit = NULL; int ncrit = 0;
            if (parse_criteria_json(if_json, &crit, &ncrit) != 0) {
                free(old_val);
                OUT("{\"error\":\"invalid if condition\"}\n");
                return 1;
            }
            int pass = cas_check(ts, old_val, (int)old_vlen, crit, ncrit);
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
            if (!ts->fields[i].removed) {
                if (ts->fields[i].type == FT_VARCHAR) {
                    int content_max = ts->fields[i].size - 2;
                    size_t vlen = strlen(field_vals[i]);
                    if ((int)vlen > content_max) {
                        char err[256];
                        snprintf(err, sizeof(err),
                            "value for field '%s' is %zu bytes; exceeds max %d for varchar",
                            ts->fields[i].name, vlen, content_max);
                        free(field_vals[i]);
                        for (int j = i + 1; j < ts->nfields; j++) free(field_vals[j]);
                        free(new_buf);
                        OUT("{\"error\":\"%s\"}\n", err);
                        return 1;
                    }
                }
                encode_field(&ts->fields[i], field_vals[i],
                             new_buf + ts->fields[i].offset);
            }
            free(field_vals[i]);
        }
    }

    /* auto_update fields: stamp current value on every update.
       Each typed type gets its appropriate now-form:
         FT_DATE      — yyyyMMdd (8-char int32 packed)
         FT_TIMESTAMP — Unix epoch ms (int64 BE; 2026.05.6+)
         everything else — yyyyMMddHHmmss (FT_DATETIME / fallback) */
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed) continue;
        if (ts->fields[i].default_kind != DK_AUTO_UPDATE) continue;

        char tbuf[24];
        if (ts->fields[i].type == FT_TIMESTAMP) {
            struct timespec tsn;
            clock_gettime(CLOCK_REALTIME, &tsn);
            long long ms = (long long)tsn.tv_sec * 1000LL + tsn.tv_nsec / 1000000LL;
            snprintf(tbuf, sizeof(tbuf), "%lld", ms);
        } else if (ts->fields[i].type == FT_DATETIMEMS) {
            struct timespec tsn;
            clock_gettime(CLOCK_REALTIME, &tsn);
            time_t nowsec = tsn.tv_sec;
            struct tm tmv;
            localtime_r(&nowsec, &tmv);
            int msec = (int)(tsn.tv_nsec / 1000000L);
            snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d%03d",
                     tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                     tmv.tm_hour, tmv.tm_min, tmv.tm_sec, msec);
        } else {
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
        }
        encode_field(&ts->fields[i], tbuf, new_buf + ts->fields[i].offset);
    }

    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';

    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    /* Parse `if` once; check_fn runs cas_check under the kf-shard wrlock
       so the verify + commit are atomic against concurrent writers. */
    SearchCriterion *crit = NULL;
    int ncrit = 0;
    if (if_json && parse_criteria_json(if_json, &crit, &ncrit) != 0) {
        free(new_buf);
        OUT("{\"error\":\"invalid if condition\"}\n");
        return 1;
    }

    V2UpdateCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .idx_fields = idx_fields, .nidx = nidx,
        .idx_types = idx_types,
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
        .out_kf_shard     = &ctx.kf_shard,
        .out_kf_slot      = &ctx.kf_slot,
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
        if (ctx.err_buf[0]) {
            OUT("{\"error\":\"%s\"}\n", ctx.err_buf);
        } else {
            OUT("{\"error\":\"update failed\"}\n");
        }
        return 1;
    }
    free_criteria(crit, ncrit);

    char wire_key[1100];
    format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
    LOG_INFO(LOG_SUB_SLOTCASK, "UPDATE %s.%s (slotcask)", object, wire_key);
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
    return cmd_update_v2(db_root, object, key, klen, partial_json,
                         if_json, dry_run, &sc);
}

/* ========== DELETE — v2 (slotcask) helper ========== */

typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*idx_fields)[256];
    int               nidx;
    enum IndexType   *idx_types;
    TypedSchema      *idx_ts;
    SearchCriterion  *crit;
    int               ncrit;
    int               kf_shard;     /* populated by slotcask before pre_commit */
    uint32_t          kf_slot;
} V2DeleteCtx;

static int v2_delete_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2DeleteCtx *c = (V2DeleteCtx *)ctx_ptr;
    if (c->ncrit > 0) {
        if (!old) return 0;
        if (!cas_check(c->idx_ts, old->value, (int)old->vlen, c->crit, c->ncrit)) return 0;
    }
    return 1;
}

static int v2_delete_pre_commit(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2DeleteCtx *c = (V2DeleteCtx *)ctx_ptr;
    if (!old || c->nidx == 0) return 0;

    /* Same parallel-fanout + arena allocation pattern as
       v2_update_pre_commit. update_idx_fn with new_key=NULL is a
       pure delete. */
    enum { INDEX_KEY_MAX = 4096 };
    size_t arena_bytes = (size_t)c->nidx * (size_t)INDEX_KEY_MAX;
    uint8_t *arena = malloc(arena_bytes);
    UpdateIdxArg args[MAX_FIELDS];
    uint8_t *fb_bufs[MAX_FIELDS]; int n_fb = 0;
    int n_args = 0;

    for (int i = 0; i < c->nidx; i++) {
        uint8_t *ikey = NULL;
        size_t ilen = 0;
        int have = 0;
        if (arena) {
            uint8_t *slot = arena + (size_t)i * INDEX_KEY_MAX;
            int rc = build_index_key_from_record_into(c->idx_ts, old->value,
                                                       c->idx_fields[i],
                                                       slot, INDEX_KEY_MAX, &ilen);
            if (rc == 1) { ikey = slot; have = 1; }
            else if (rc == -1) {
                have = build_index_key_from_record(c->idx_ts, old->value,
                                                   c->idx_fields[i],
                                                   &ikey, &ilen);
                if (have) fb_bufs[n_fb++] = ikey;
            }
        } else {
            have = build_index_key_from_record(c->idx_ts, old->value,
                                               c->idx_fields[i],
                                               &ikey, &ilen);
            if (have) fb_bufs[n_fb++] = ikey;
        }
        if (!have) continue;
        args[n_args].db_root = c->db_root;
        args[n_args].object  = c->object;
        args[n_args].field   = c->idx_fields[i];
        args[n_args].splits  = c->splits;
        args[n_args].new_key = NULL;
        args[n_args].new_len = 0;
        args[n_args].old_key = ikey;
        args[n_args].old_len = ilen;
        args[n_args].hash    = c->hash;
        args[n_args].type    = c->idx_types ? c->idx_types[i] : IT_BTREE;
        args[n_args].kf_shard = c->kf_shard;
        args[n_args].kf_slot  = c->kf_slot;
        args[n_args].bm_max_values = 0;
        n_args++;
    }

    if (n_args > 0) parallel_for(update_idx_fn, args, n_args, sizeof(UpdateIdxArg));
    for (int i = 0; i < n_fb; i++) free(fb_bufs[i]);
    free(arena);
    bm_flush_thread_bitmap_cache();
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
        .streams = sc->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        OUT("{\"status\":\"not_found\",\"key\":\"%s\"}\n", wire_key);
        return 0;
    }

    TypedSchema *ts = load_typed_schema(db_root, object);

    /* Wire up compact trim for VARIABLE-format typed objects. */
    if (sdb->format == SLOTCASK_FORMAT_VARIABLE && !sdb->trim_fn) {
        sdb->trim_fn  = schema_trim_fn;
        sdb->trim_ctx = (void *)ts;
    }

    /* dry_run: read + validate, never tombstone. */
    if (dry_run) {
        void *val = NULL; size_t vlen = 0;
        if (slotcask_get(sdb, key, klen, &val, &vlen) != 0) {
            OUT("{\"status\":\"not_found\",\"key\":\"%s\"}\n", wire_key);
            return 0;
        }
        if (if_json) {
            SearchCriterion *crit = NULL; int ncrit = 0;
            if (parse_criteria_json(if_json, &crit, &ncrit) != 0) {
                free(val);
                OUT("{\"error\":\"invalid if condition\"}\n");
                return 1;
            }
            int pass = cas_check(ts, val, (int)vlen, crit, ncrit);
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

    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    SearchCriterion *crit = NULL;
    int ncrit = 0;
    if (if_json && parse_criteria_json(if_json, &crit, &ncrit) != 0) {
        OUT("{\"error\":\"invalid if condition\"}\n");
        return 1;
    }

    V2DeleteCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .idx_fields = idx_fields, .nidx = nidx,
        .idx_types = idx_types,
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
        .out_kf_shard   = &ctx.kf_shard,
        .out_kf_slot    = &ctx.kf_slot,
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
    LOG_INFO(LOG_SUB_SLOTCASK, "DELETE %s.%s (slotcask)", object, wire_key);
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
    return cmd_delete_v2(db_root, object, key, klen, if_json, dry_run, &sc);
}

/* ========== MULTI-KEY GET ========== */

/* ========== Parallel multi-key EXISTS ========== */

typedef struct {
    char *key;          /* storage form: binary for auto_key, string otherwise */
    size_t klen;        /* binary length (was strlen(key) before auto-key) */
    char *wire_key;     /* wire-form string for response output */
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

/* Parse one wire-form key from a JSON array element into its storage
   form, computing hash + kf-shard id along the way. Shared by every
   multi-key path (bulk_exists, bulk_get). Auto-key UUID/seq forms get
   parsed to binary; otherwise the storage form is verbatim bytes.
   On malformed auto-key forms returns 0 — caller treats the row as a
   miss (found=0 / result_json=NULL) so clients see the same "missing"
   shape as a non-existent key. Returns 1 on success.
   Heap allocations land in *out_wire_key and *out_storage_key — caller
   owns both. */
static int parse_multi_key(const char *src, size_t klen, const Schema *sc,
                            char  **out_wire_key,
                            uint8_t **out_storage_key, size_t *out_storage_klen,
                            uint8_t out_hash[16], int *out_shard_id) {
    char *wire = malloc(klen + 1);
    if (!wire) return -1;
    memcpy(wire, src, klen);
    wire[klen] = '\0';
    *out_wire_key = wire;

    uint8_t *skey = NULL;
    size_t   slen = 0;
    if (sc->auto_key == AK_UUID) {
        uint8_t bin[16];
        if (parse_uuid_string(wire, bin) == 0) {
            skey = malloc(16);
            if (skey) { memcpy(skey, bin, 16); slen = 16; }
        }
    } else if (sc->auto_key == AK_SEQ) {
        int64_t v;
        if (parse_seq_key(wire, &v) == 0) {
            skey = malloc(8);
            if (skey) {
                for (int b = 7; b >= 0; b--) { skey[b] = (uint8_t)(v & 0xFF); v >>= 8; }
                slen = 8;
            }
        }
    } else {
        skey = malloc(klen + 1);
        if (skey) { memcpy(skey, src, klen); skey[klen] = '\0'; slen = klen; }
    }
    *out_storage_key = skey;
    *out_storage_klen = slen;
    if (skey) {
        compute_hash_raw((const char *)skey, slen, out_hash);
        *out_shard_id = compute_record_shard(out_hash, sc->splits);
    } else {
        /* Malformed auto-key wire form → never matches; keep shard
           deterministic so bucket-sort works. */
        memset(out_hash, 0, 16);
        *out_shard_id = 0;
    }
    return 1;
}

static void *multi_exists_shard_worker(void *arg) {
    MultiExistsShardWork *sw = (MultiExistsShardWork *)arg;
    if (sw->count == 0) return NULL;

    /* bulk_lookup_in_kfshard amortises kfcache_acquire + segcache_acquire
       across the worker's records. The dispatcher already aligned
       shard_id with compute_record_shard so all entries here hash to the
       same kf shard. */
    SlotcaskSchemaInfo info = {
        .splits = sw->sch->splits, .slot_size = sw->sch->slot_size,
        .streams = sw->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) return NULL;

    SlotcaskBulkRec *batch = calloc(sw->count, sizeof(SlotcaskBulkRec));
    if (!batch) return NULL;
    for (int ei = 0; ei < sw->count; ei++) {
        MultiExistsEntry *e = &sw->entries[ei];
        batch[ei].key       = e->key;
        batch[ei].klen      = e->klen;
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

/* Bucket-sort entries by shard_id, dispatch multi_exists_shard_worker
   in parallel across shards, copy `.found` results back into `entries`
   in original order. Shared by cmd_exists_multi, cmd_not_exists, and
   cmd_get_multi — they each parse their key list into `entries[]` then
   call this; afterwards they format their type-specific response from
   the populated `entries[].found`.

   Replaces the O(n²) insertion sort that previously dominated BULK
   EXISTS / BULK GET: at 10K keys × ~128 shards the sort was ~50M
   swaps before parallel_for even started. The bucket-sort is a
   single pass over entries plus one pass to fan out into per-shard
   buckets. */
static void multi_bucket_dispatch(MultiExistsEntry *entries, int key_count,
                                  const Schema *sc,
                                  const char *db_root, const char *object) {
    for (int i = 0; i < key_count; i++) entries[i].orig_idx = i;
    int *shard_counts    = calloc(sc->splits, sizeof(int));
    int *shard_to_worker = malloc(sc->splits * sizeof(int));
    for (int i = 0; i < key_count; i++) shard_counts[entries[i].shard_id]++;
    int nshard = 0;
    for (int s = 0; s < sc->splits; s++) if (shard_counts[s] > 0) nshard++;

    MultiExistsShardWork *workers = calloc(nshard, sizeof(MultiExistsShardWork));
    {
        int g = 0;
        for (int s = 0; s < sc->splits; s++) {
            if (shard_counts[s] > 0) {
                workers[g].db_root = db_root;
                workers[g].object  = object;
                workers[g].sch     = sc;
                workers[g].count   = 0;
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

    parallel_for_io(multi_exists_shard_worker, workers, nshard, sizeof(MultiExistsShardWork));

    /* Copy results back via orig_idx (no sorted[] indirection). */
    for (int g = 0; g < nshard; g++)
        for (int i = 0; i < workers[g].count; i++)
            entries[workers[g].entries[i].orig_idx].found = workers[g].entries[i].found;

    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers);
    free(shard_counts);
    free(shard_to_worker);
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
            parse_multi_key(start, klen, &sc,
                             &e->wire_key,
                             (uint8_t **)&e->key, &e->klen,
                             e->hash, &e->shard_id);
            e->start_slot = 0;
            e->found = 0;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("{}\n"); return 0; }

    multi_bucket_dispatch(entries, key_count, &sc, db_root, object);

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
            const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
            size_t klen = strlen(out_key);
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
            memcpy(buf + pos, out_key, klen); pos += klen;
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
                const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
                size_t klen = strlen(out_key);
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
                memcpy(buf + pos, out_key, klen); pos += klen;
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

    /* (workers + per-worker entries freed inside multi_bucket_dispatch.) */
    for (int i = 0; i < key_count; i++) { free(entries[i].key); free(entries[i].wire_key); }
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
            parse_multi_key(start, klen, &sc,
                             &e->wire_key,
                             (uint8_t **)&e->key, &e->klen,
                             e->hash, &e->shard_id);
            e->start_slot = 0;
            e->found = 0;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("[]\n"); return 0; }

    multi_bucket_dispatch(entries, key_count, &sc, db_root, object);

    /* Build output in one buffer; one fwrite. */
    size_t cap = (size_t)key_count * 32 + 16;
    char *buf = malloc(cap);
    if (buf) {
        size_t pos = 0;
        buf[pos++] = '[';
        int first = 1;
        for (int i = 0; i < key_count; i++) {
            if (entries[i].found) continue;
            const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
            size_t klen = strlen(out_key);
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
            memcpy(buf + pos, out_key, klen); pos += klen;
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

    /* (workers + per-worker entries freed inside multi_bucket_dispatch.) */
    for (int i = 0; i < key_count; i++) { free(entries[i].key); free(entries[i].wire_key); }
    free(entries);
    return 0;
}

/* ========== Parallel multi-key GET ========== */

typedef struct {
    char *key;          /* storage form: binary for auto_key, string otherwise */
    size_t klen;        /* binary length (was strlen(key) before auto-key) */
    char *wire_key;     /* wire-form string for response output (NULL → use key) */
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

/* Callback for multi-get: decodes value inline into entry's result_json. */
static int multi_get_fetch_cb(const uint8_t hash[16],
                               const void *key, size_t klen,
                               const void *value, size_t vlen,
                               void *ctx_ptr) {
    (void)key; (void)klen;
    MultiGetShardWork *sw = (MultiGetShardWork *)ctx_ptr;
    for (int ei = 0; ei < sw->count; ei++) {
        if (memcmp(sw->entries[ei].hash, hash, 16) == 0) {
            char *decoded = sw->fs ? typed_decode(sw->fs->ts,
                                                   (const uint8_t *)value,
                                                   (uint32_t)vlen) : NULL;
            sw->entries[ei].result_json = decoded ? decoded : strdup("null");
            break;
        }
    }
    return 0;
}

static void *multi_get_shard_worker(void *arg) {
    MultiGetShardWork *sw = (MultiGetShardWork *)arg;
    if (sw->count == 0) return NULL;

    SlotcaskSchemaInfo info = {
        .splits = sw->sch->splits, .slot_size = sw->sch->slot_size,
        .streams = sw->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) return NULL;

    /* Extract pre-computed hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)sw->count * sizeof(*hashes));
    if (!hashes) return NULL;
    for (int ei = 0; ei < sw->count; ei++)
        memcpy(hashes[ei], sw->entries[ei].hash, 16);

    /* Batch resolve+fetch — two-phase model resolves KF shards internally
       and parallelizes segment reads. Callback decodes each found record. */
    slotcask_bulk_resolve_and_fetch(sdb, hashes, (size_t)sw->count,
                                     sw, multi_get_fetch_cb);

    free(hashes);
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
                /* Plain realloc (not xrealloc_or_free): on failure we still
                   need the old `entries` array intact to free each already-
                   parsed entry's key/wire_key before dropping the array
                   itself (CID 1696478). Resetting key_count to 0 here also
                   makes the `key_count == 0` early-return below fire
                   correctly instead of falling through to a NULL entries[]
                   dereference. */
                MultiGetEntry *t = realloc(entries, key_cap * sizeof(*t));
                if (!t) {
                    for (int j = 0; j < key_count; j++) {
                        free(entries[j].key);
                        free(entries[j].wire_key);
                    }
                    free(entries);
                    entries = NULL;
                    key_count = 0;
                    break;
                }
                entries = t;
            }
            MultiGetEntry *e = &entries[key_count++];
            /* Wire form for response output; storage form (binary for
               auto_key=uuid/seq, verbatim otherwise); hash + kf-shard
               bucket — see parse_multi_key. */
            parse_multi_key(start, klen, &sc,
                             &e->wire_key,
                             (uint8_t **)&e->key, &e->klen,
                             e->hash, &e->shard_id);
            e->start_slot = 0;
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
    parallel_for_io(multi_get_shard_worker, workers, nshard, sizeof(MultiGetShardWork));

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
            const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
            csv_emit_cell(out_key, csv_delim);
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
                const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
                size_t klen = strlen(out_key);
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
                memcpy(buf + pos, out_key, klen); pos += klen;
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
                const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
                if (entries[i].result_json) {
                    OUT("\"%s\":%s", out_key, entries[i].result_json);
                    free(entries[i].result_json);
                } else {
                    OUT("\"%s\":null", out_key);
                }
            }
            OUT("}\n");
        }
    }

    /* cmd_get_multi has its own MultiGetEntry/MultiGetShardWork types
       (extra `fs` FieldSchema field, different result type) so it can't
       share the multi_bucket_dispatch helper used by exists/not_exists;
       clean up workers explicitly. */
    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers);
    for (int i = 0; i < key_count; i++) { free(entries[i].key); free(entries[i].wire_key); }
    free(entries);
    return 0;
}

