/* slotcask.c — bitcask + snake-game free-slot reuse, ported from
 * src/proto_bitcask/proto_bitcask_v2.c (validated 2026-05-07 at 5M scale).
 *
 * Production adaptations vs the prototype:
 *   - Keyfile shards live in the global `kfcache` (mmap MAP_SHARED, per-entry
 *     rwlock, LRU). Modeled on bt_cache in btree.c. Cap from db.env FCACHE_MAX.
 *   - Data segments live in the global `segcache` (same model). Each segment
 *     is mmap'd MAP_SHARED at full SLOTCASK_SEG_MAX_BYTES (sparse file via
 *     ftruncate) so writes are memcpy-into-mmap, eliminating the prototype's
 *     open/close/pwrite per-call bottleneck.
 *
 * Per-object DB state (SlotcaskDb): only per-stream rotation/pool primitives
 * — keyfile and segment data live in the global caches, not the DB struct.
 * That keeps SlotcaskDb tiny (a handful of mutexes + ints per stream) so the
 * engine can keep many of them resident.
 */
#define _GNU_SOURCE
#include "slotcask.h"
#include "types.h"        /* compute_record_shard — single byte-order source */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <errno.h>
#include <dirent.h>
#include <time.h>
#include <pthread.h>
#include "io_direct.h"

/* Single source of truth for primary-key hashing — defined in util.c. We
   forward-declare it instead of pulling in types.h so slotcask stays
   decoupled from the engine's wider header surface (it links cleanly into
   the standalone test binary alongside util.c). */
extern void compute_hash_raw(const char *key, size_t key_len,
                              uint8_t hash_out[16]);

/* Linux-only mmap hints — collapse to no-ops on macOS. MADV_HUGEPAGE is
   the transparent-hugepage opt-in (kernel still ignores if THP is off, so
   missing it is purely a perf hint, not a correctness one). MAP_POPULATE
   prefaults pages at mmap time; without it the kernel faults on demand,
   which we already handle. */
#ifdef MADV_HUGEPAGE
#define SHARD_MADV_HUGEPAGE(p, sz) madvise((p), (sz), MADV_HUGEPAGE)
#else
#define SHARD_MADV_HUGEPAGE(p, sz) ((void)0)
#endif
#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

/* ============================================================ Helpers */

/* Sort an int[] index array such that the indexed entries' (old_sid,
   old_fid) tuples are non-decreasing. Used by every bulk path (upsert
   + delete) to batch seg-file reads / writes by physical locality —
   one batch hits one segment file once instead of scattering. Different
   bulk paths use slightly different state structs (different field
   names beyond old_sid/old_fid), so this is a macro that takes the
   state-array variable name. Insertion sort because typical batch
   sizes are <200K records and qsort's callback overhead loses by ~2×
   at that scale (measured during 2026.05 perf passes). */
#define SLOTCASK_SORT_IDX_BY_SEG_LOC(idx, n, st) do {              \
    for (int _a = 1; _a < (n); _a++) {                             \
        int _tmp = (idx)[_a];                                      \
        uint8_t  _ta_sid = (st)[_tmp].old_sid;                     \
        uint16_t _ta_fid = (st)[_tmp].old_fid;                     \
        int _b = _a - 1;                                           \
        while (_b >= 0) {                                          \
            int _bi = (idx)[_b];                                   \
            if ((st)[_bi].old_sid <  _ta_sid ||                    \
                ((st)[_bi].old_sid == _ta_sid &&                   \
                 (st)[_bi].old_fid <= _ta_fid)) break;             \
            (idx)[_b + 1] = (idx)[_b];                             \
            _b--;                                                  \
        }                                                          \
        (idx)[_b + 1] = _tmp;                                      \
    }                                                              \
} while (0)

static int next_pow2(int n) { int p = 1; while (p < n) p <<= 1; return p; }

static uint32_t path_hash(const char *s) {
    uint32_t h = 5381;
    while (*s) h = h * 33 + (unsigned char)*s++;
    return h;
}

static inline void compute_hash(const void *key, size_t klen, uint8_t out[16]) {
    compute_hash_raw((const char *)key, klen, out);
}

/* Delegates to the version-aware compute_record_shard so the byte-order
   logic lives in exactly one place (storage.c). v2 = little-endian. */
static int shard_for_hash(const uint8_t hash[16], int num_shards) {
    return compute_record_shard(hash, num_shards);
}

size_t kf_slot_for(const uint8_t hash[16], size_t cap) {
    uint64_t v;
    memcpy(&v, hash, 8);
    return (size_t)(v % cap);
}

/* mkdir -p */
static int mkdirp_local(const char *path) {
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s", path);
    size_t n = strlen(tmp);
    if (n > 0 && tmp[n - 1] == '/') tmp[n - 1] = 0;
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
            *p = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
    return 0;
}

/* On-disk layout:
     <data_dir>/data/kf/NNN.kf              keyfile shards (sharded by `splits`)
     <data_dir>/data/streams/NNN/NNNNNN.dat rotating segment files per stream
   The `data/` umbrella keeps engine internals out of the obj root, so
   fields.conf, indexes/, files/, etc. aren't visually mixed with kf/seg
   files. */
void kf_path_for(char out[PATH_MAX], const char *data_dir, int shard_id) {
    snprintf(out, PATH_MAX, "%s/data/kf/%03d.kf", data_dir, shard_id);
}

/* Public wrapper for the bitmap-index read path in query.c. Same layout as
   kf_path_for; exposed so callers don't duplicate the convention. */
void slotcask_kf_path(char *out, size_t outlen,
                      const char *data_dir, int shard_id) {
    snprintf(out, outlen, "%s/data/kf/%03d.kf", data_dir, shard_id);
}
static void stream_dir_for(char out[PATH_MAX], const char *data_dir, int stream_id) {
    snprintf(out, PATH_MAX, "%s/data/streams/%03d", data_dir, stream_id);
}
static void seg_path_for(char out[PATH_MAX], const char *data_dir,
                         int stream_id, uint32_t file_id) {
    snprintf(out, PATH_MAX, "%s/data/streams/%03d/%06u.dat",
             data_dir, stream_id, (unsigned)file_id);
}

/* ============================================================ kfcache */
/* KfCacheEntry moved to shard_db_internal.h; g_kfcache* moved to ShardDb struct */

void kfcache_init(int cap) {
    if (g_kfcache) return;
    if (cap < 16) cap = 16;
    g_kfcache_slots = next_pow2(cap * 2);
    g_kfcache = calloc(g_kfcache_slots, sizeof(KfCacheEntry));
    g_kfcache_count = 0;
    for (int i = 0; i < g_kfcache_slots; i++) {
        pthread_rwlock_init(&g_kfcache[i].rwlock, NULL);
        g_kfcache[i].fd = -1;
    }
}

void kfcache_shutdown(void) {
    pthread_mutex_lock(&g_kfcache_lock);
    if (g_kfcache) {
        for (int i = 0; i < g_kfcache_slots; i++) {
            KfCacheEntry *e = &g_kfcache[i];
            if (!e->used) continue;
            if (e->base && e->map_size > 0) msync(e->base, e->map_size, MS_SYNC);
            if (e->base) munmap(e->base, e->map_size);
            if (e->fd >= 0) close(e->fd);
            pthread_rwlock_destroy(&e->rwlock);
        }
        free(g_kfcache);
        g_kfcache = NULL;
        g_kfcache_slots = 0;
        g_kfcache_count = 0;
    }
    pthread_mutex_unlock(&g_kfcache_lock);
}

static int kfcache_probe(const char *path, int *out_found) {
    uint32_t h = path_hash(path);
    int mask = g_kfcache_slots - 1;
    int idx = h & mask;
    for (int i = 0; i < g_kfcache_slots; i++) {
        int s = (idx + i) & mask;
        if (!g_kfcache[s].used) { *out_found = 0; return s; }
        if (strcmp(g_kfcache[s].path, path) == 0) { *out_found = 1; return s; }
    }
    *out_found = 0;
    return -1;
}

static void kfcache_drop_slot(int slot) {
    KfCacheEntry *e = &g_kfcache[slot];
    if (!e->used) return;
    if (e->base && e->map_size > 0) msync(e->base, e->map_size, MS_ASYNC);
    if (e->base) munmap(e->base, e->map_size);
    if (e->fd >= 0) close(e->fd);
    e->base = NULL;
    e->fd = -1;
    e->map_size = 0;
    e->capacity = 0;
    e->used = 0;
    e->path[0] = '\0';
    /* Increment gen under g_kfcache_lock (caller always holds it).
       Any SlotRef pointing at this slot will fail its gen check after
       this store, forcing the slow-path re-probe. */
    atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
    g_kfcache_count--;
}

/* Drop every cached kf shard whose path starts with `prefix`. Used by
   slotcask_registry_invalidate to flush stale mmap regions before the
   on-disk files move (rebuild_object_v2) or vanish (drop-object).

   Lock-ordering: kfcache_acquire's install path holds g_kfcache_lock
   while taking the per-entry rwlock. To avoid deadlock we must NOT
   hold the rwlock while reaching for the table mutex. Pattern: read
   (path, used) without mutex (slots array is fixed-size, fields are
   pointer/byte-sized so torn reads aren't a concern; install only
   writes them under mutex), take rwlock for matching slots, do the
   munmap/close/clear under rwlock alone, decrement g_kfcache_count
   atomically. The caller (per-object wrlock) guarantees no concurrent
   ops on THIS object so the rwlock contention is bounded. */
static void kfcache_invalidate_prefix(const char *prefix) {
    if (!g_kfcache || !prefix || !prefix[0]) return;
    size_t pl = strlen(prefix);
    for (int i = 0; i < g_kfcache_slots; i++) {
        KfCacheEntry *e = &g_kfcache[i];
        if (!__atomic_load_n(&e->used, __ATOMIC_ACQUIRE)) continue;
        if (strncmp(e->path, prefix, pl) != 0) continue;
        pthread_rwlock_wrlock(&e->rwlock);
        if (__atomic_load_n(&e->used, __ATOMIC_ACQUIRE) &&
            strncmp(e->path, prefix, pl) == 0) {
            if (e->base && e->map_size > 0) msync(e->base, e->map_size, MS_ASYNC);
            if (e->base) munmap(e->base, e->map_size);
            if (e->fd >= 0) close(e->fd);
            e->base = NULL;
            e->fd = -1;
            e->map_size = 0;
            e->capacity = 0;
            e->path[0] = '\0';
            atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
            __atomic_store_n(&e->used, 0, __ATOMIC_RELEASE);
            __sync_fetch_and_sub(&g_kfcache_count, 1);
        }
        pthread_rwlock_unlock(&e->rwlock);
    }
}

/* Open + size + mmap a keyfile shard. Caller may NOT hold g_kfcache_lock when
   the file system call could block, so we do the heavy lifting outside the
   table mutex (matching bt_open_file's contract in btree.c). */
static int kf_open_file(const char *path, size_t slots_capacity, int writer,
                        int *out_fd, uint8_t **out_base, size_t *out_size) {
    int fd;
    int created_fresh = 0;  /* track first-time creation for header init */
    if (writer) {
        char dir[PATH_MAX];
        snprintf(dir, sizeof(dir), "%s", path);
        char *slash = strrchr(dir, '/');
        if (slash) { *slash = 0; mkdirp_local(dir); }
        /* Race-free first-create detection via O_EXCL: try to create
           exclusively first; on EEXIST the file already existed and we
           reopen without O_CREAT. The previous stat()-then-open()
           pattern had a TOCTOU window (Coverity CID 1693847) where a
           concurrent creator between the two calls would leave us
           treating an existing file as fresh and zeroing its header. */
        fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
        if (fd >= 0) {
            created_fresh = 1;
        } else if (errno == EEXIST) {
            fd = open(path, O_RDWR);
        }
    } else {
        fd = open(path, O_RDWR);
    }
    if (fd < 0) return -1;

    size_t want = SLOTCASK_KF_HDR_SIZE + slots_capacity * sizeof(SlotcaskKfEntry);
    struct stat st;
    if (fstat(fd, &st) < 0) { close(fd); return -1; }

    if (created_fresh || (size_t)st.st_size == 0) {
        /* Brand-new file. Size + initialise header. */
        if (!writer) { close(fd); return -1; }
        if (ftruncate(fd, (off_t)want) < 0) { close(fd); return -1; }
    } else if ((size_t)st.st_size < want) {
        /* Pre-existing file but smaller than requested. Pre-release we treat
           any headerless/old-format kf as garbage and refuse — operator is
           expected to wipe DB_ROOT before upgrading. */
        if (!writer) { close(fd); return -1; }
        if (ftruncate(fd, (off_t)want) < 0) { close(fd); return -1; }
    } else if ((size_t)st.st_size > want) {
        /* Existing file is bigger than the default (auto-resplit grew it).
           Use the actual size for the mmap; slot count derives from it. */
        want = (size_t)st.st_size;
    }

    void *m = mmap(NULL, want, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (m == MAP_FAILED) { close(fd); return -1; }
    SHARD_MADV_HUGEPAGE(m, want);  /* THP hint for kf mmap */

    SlotcaskKfHeader *hdr = (SlotcaskKfHeader *)m;
    if (created_fresh) {
        /* Stamp magic + version; counters start at 0. */
        hdr->magic = SLOTCASK_KF_MAGIC;
        hdr->version = SLOTCASK_KF_VERSION;
        hdr->total = 0;
        hdr->deleted = 0;
        msync(m, SLOTCASK_KF_HDR_SIZE, MS_ASYNC);
    } else if (hdr->magic != SLOTCASK_KF_MAGIC) {
        /* Magic missing/wrong — pre-release we don't migrate. */
        munmap(m, want); close(fd);
        return -1;
    }

    *out_fd = fd;
    *out_base = (uint8_t *)m;
    *out_size = want;
    return 0;
}

/* Populate handle's hdr/map/capacity from the cache entry's stored base. */
static inline void kf_handle_from_entry(SlotcaskKfHandle *h, KfCacheEntry *e) {
    h->fd = e->fd;
    h->hdr = (SlotcaskKfHeader *)e->base;
    h->map = (SlotcaskKfEntry *)(e->base + SLOTCASK_KF_HDR_SIZE);
    h->map_size = e->map_size;
    h->capacity = e->capacity;
}

/* Populate handle's hdr/map/capacity from a freshly-opened uncached mmap. */
static inline void kf_handle_from_uncached(SlotcaskKfHandle *h,
                                            int fd, uint8_t *base, size_t sz) {
    h->slot = -1;
    h->fd = fd;
    h->hdr = (SlotcaskKfHeader *)base;
    h->map = (SlotcaskKfEntry *)(base + SLOTCASK_KF_HDR_SIZE);
    h->map_size = sz;
    h->capacity = (sz - SLOTCASK_KF_HDR_SIZE) / sizeof(SlotcaskKfEntry);
}

int kfcache_acquire(SlotcaskKfHandle *h, const char *path,
                    size_t slots_capacity, int writer) {
    h->slot = -1;
    h->writer = writer;
    h->fd = -1;
    h->hdr = NULL;
    h->map = NULL;
    h->map_size = 0;
    h->capacity = 0;

    if (!g_kfcache) {
        /* Cache not initialised — direct mmap, no locking. */
        int fd; uint8_t *base; size_t sz;
        if (kf_open_file(path, slots_capacity, writer, &fd, &base, &sz) < 0) return -1;
        kf_handle_from_uncached(h, fd, base, sz);
        return 0;
    }

    /* Verify-and-retry on cache hit (mirrors bt_acquire). */
    int retries = 0;
    int found = 0, slot = -1;
    pthread_mutex_lock(&g_kfcache_lock);
    while (1) {
        slot = kfcache_probe(path, &found);
        if (!found) break;
        g_kfcache[slot].last_access =
            __atomic_add_fetch(&g_kfcache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &g_kfcache[slot].rwlock;
        pthread_mutex_unlock(&g_kfcache_lock);
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);

        /* coverity[atomicity] CID 1693850: `slot` came from the prior
           locked section, but we re-verify identity below
           (e->used && strcmp(e->path, path) == 0). On mismatch we
           drop the rwlock and retry — the verify is the consistency
           barrier the analyzer can't see. */
        KfCacheEntry *e = &g_kfcache[slot];
        if (e->used && strcmp(e->path, path) == 0) {
            h->slot = slot;
            kf_handle_from_entry(h, e);
            /* coverity[missing_unlock] intentional: returning with the
               per-slot rwlock held; caller releases via kfcache_release. */
            return 0;
        }
        pthread_rwlock_unlock(lock);
        if (++retries >= 4) {
            /* slot/found get re-set by the kfcache_probe call below
               in the install path (Coverity CID 1693833). */
            pthread_mutex_lock(&g_kfcache_lock);
            break;
        }
        pthread_mutex_lock(&g_kfcache_lock);
    }

    /* Miss path: open + install. Drop table lock during open since it can
       block on disk. */
    pthread_mutex_unlock(&g_kfcache_lock);
    int fd; uint8_t *base; size_t sz;
    if (kf_open_file(path, slots_capacity, writer, &fd, &base, &sz) < 0) return -1;
    pthread_mutex_lock(&g_kfcache_lock);

    /* Re-probe — another thread may have installed it while we were opening. */
    slot = kfcache_probe(path, &found);
    if (found) {
        /* Lost the install race. Discard our open; use the cached entry. */
        munmap(base, sz);
        close(fd);
        g_kfcache[slot].last_access =
            __atomic_add_fetch(&g_kfcache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &g_kfcache[slot].rwlock;
        pthread_mutex_unlock(&g_kfcache_lock);
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);
        KfCacheEntry *e = &g_kfcache[slot];
        if (e->used && strcmp(e->path, path) == 0) {
            h->slot = slot;
            kf_handle_from_entry(h, e);
            /* coverity[missing_unlock] intentional: returning with the
               per-slot rwlock held; caller releases via kfcache_release. */
            return 0;
        }
        /* Slot was evicted under us; serve uncached this once. */
        pthread_rwlock_unlock(lock);
        int fd2; uint8_t *base2; size_t sz2;
        if (kf_open_file(path, slots_capacity, writer, &fd2, &base2, &sz2) < 0) return -1;
        kf_handle_from_uncached(h, fd2, base2, sz2);
        return 0;
    }

    /* Evict LRU if half-full or no empty slot. */
    if (slot < 0 || g_kfcache_count >= g_kfcache_slots / 2) {
        int lru = -1;
        uint64_t oldest = UINT64_MAX;
        for (int i = 0; i < g_kfcache_slots; i++) {
            if (g_kfcache[i].used && g_kfcache[i].last_access < oldest) {
                oldest = g_kfcache[i].last_access;
                lru = i;
            }
        }
        if (lru >= 0) { kfcache_drop_slot(lru); slot = lru; }
    }

    if (slot < 0) {
        /* Cache truly full — serve uncached. */
        pthread_mutex_unlock(&g_kfcache_lock);
        kf_handle_from_uncached(h, fd, base, sz);
        return 0;
    }

    KfCacheEntry *e = &g_kfcache[slot];
    strncpy(e->path, path, PATH_MAX - 1);
    e->path[PATH_MAX - 1] = '\0';
    e->fd = fd;
    e->base = base;
    e->map_size = sz;
    e->capacity = (sz - SLOTCASK_KF_HDR_SIZE) / sizeof(SlotcaskKfEntry);
    e->used = 1;
    e->last_access = __atomic_add_fetch(&g_kfcache_clock, 1, __ATOMIC_RELAXED);
    g_kfcache_count++;

    /* Take rwlock before releasing table mutex (closes evict-after-install race). */
    pthread_rwlock_t *lock = &e->rwlock;
    if (writer) pthread_rwlock_wrlock(lock);
    else        pthread_rwlock_rdlock(lock);
    pthread_mutex_unlock(&g_kfcache_lock);

    h->slot = slot;
    kf_handle_from_entry(h, e);
    return 0;
}

void kfcache_release(SlotcaskKfHandle *h) {
    if (h->slot >= 0) {
        pthread_rwlock_unlock(&g_kfcache[h->slot].rwlock);
    } else if (h->hdr) {
        /* Uncached fallback. */
        munmap((void *)h->hdr, h->map_size);
        if (h->fd >= 0) close(h->fd);
    }
    h->slot = -1;
    h->fd = -1;
    h->hdr = NULL;
    h->map = NULL;
    h->map_size = 0;
    h->capacity = 0;
}

/* Fast-path kfcache acquire for read-only callers that hold a SlotRef.
 *
 * Warm hit (common case, no lock):
 *   1. Load ref->slot — skip if -1 (not yet populated).
 *   2. Atomic-load e->gen and compare with ref->gen.
 *   3. If equal: take per-slot rdlock, verify identity (path match + used),
 *      fill handle, return 0. Total cost: 1 atomic load + 1 rdlock.
 *
 * Cold/evicted (uncommon):
 *   Fall through to kfcache_acquire (existing slow path). On success,
 *   update ref->slot and ref->gen so the next call is a warm hit.
 *
 * writer must be 0 — this function is for read-only callers only.
 * db and kf_shard_id are accepted but only used to update ref on the
 * slow path (so the caller's stored ref stays current after a miss).
 */
int kfcache_acquire_direct(SlotcaskKfHandle *h, SlotRef *ref,
                            const char *path, size_t slots_capacity,
                            void *db, int kf_shard_id) {
    (void)db;          /* used only to make the signature future-proof */
    (void)kf_shard_id; /* same */

    if (ref && ref->slot >= 0) {
        int s = ref->slot;
        KfCacheEntry *e = &g_kfcache[s];
        uint64_t cur_gen = atomic_load_explicit(&e->gen, memory_order_acquire);
        if (cur_gen == ref->gen) {
            /* Gen matches — slot should still hold our entry.
               Take rdlock and verify identity before returning. */
            pthread_rwlock_rdlock(&e->rwlock);
            if (__atomic_load_n(&e->used, __ATOMIC_ACQUIRE) &&
                strcmp(e->path, path) == 0) {
                /* Warm hit confirmed. */
                h->slot = s;
                h->writer = 0;
                kf_handle_from_entry(h, e);
                return 0;
            }
            /* Identity check failed (concurrent eviction between gen-check
               and rdlock). Drop lock and fall through to slow path. */
            pthread_rwlock_unlock(&e->rwlock);
        }
    }

    /* Slow path: standard kfcache_acquire, then refresh the SlotRef. */
    int rc = kfcache_acquire(h, path, slots_capacity, 0);
    if (rc == 0 && ref && h->slot >= 0) {
        ref->slot = h->slot;
        ref->gen  = atomic_load_explicit(&g_kfcache[h->slot].gen,
                                          memory_order_acquire);
    }
    return rc;
}

/* ============================================================ segcache */
/* SegCacheEntry moved to shard_db_internal.h; g_segcache* moved to ShardDb struct */

void segcache_init(int cap) {
    if (g_segcache) return;
    if (cap < 16) cap = 16;
    g_segcache_slots = next_pow2(cap * 2);
    g_segcache = calloc(g_segcache_slots, sizeof(SegCacheEntry));
    g_segcache_count = 0;
    for (int i = 0; i < g_segcache_slots; i++) {
        pthread_rwlock_init(&g_segcache[i].rwlock, NULL);
        g_segcache[i].fd = -1;
    }
}

void segcache_shutdown(void) {
    pthread_mutex_lock(&g_segcache_lock);
    if (g_segcache) {
        for (int i = 0; i < g_segcache_slots; i++) {
            SegCacheEntry *e = &g_segcache[i];
            if (!e->used) continue;
            if (e->map && e->map_size > 0) msync(e->map, e->map_size, MS_SYNC);
            if (e->map) munmap(e->map, e->map_size);
            if (e->fd >= 0) close(e->fd);
            pthread_rwlock_destroy(&e->rwlock);
        }
        free(g_segcache);
        g_segcache = NULL;
        g_segcache_slots = 0;
        g_segcache_count = 0;
    }
    pthread_mutex_unlock(&g_segcache_lock);
}

static int segcache_probe(const char *path, int *out_found) {
    uint32_t h = path_hash(path);
    int mask = g_segcache_slots - 1;
    int idx = h & mask;
    for (int i = 0; i < g_segcache_slots; i++) {
        int s = (idx + i) & mask;
        if (!g_segcache[s].used) { *out_found = 0; return s; }
        if (strcmp(g_segcache[s].path, path) == 0) { *out_found = 1; return s; }
    }
    *out_found = 0;
    return -1;
}

/* Mirrors kfcache_invalidate_prefix — drop every cached segment under a
   given path prefix. Same lock-ordering rules: never hold the entry
   rwlock while reaching for g_segcache_lock; that would deadlock against
   segcache_acquire's install path. */
static void segcache_invalidate_prefix(const char *prefix) {
    if (!g_segcache || !prefix || !prefix[0]) return;
    size_t pl = strlen(prefix);
    for (int i = 0; i < g_segcache_slots; i++) {
        SegCacheEntry *e = &g_segcache[i];
        if (!__atomic_load_n(&e->used, __ATOMIC_ACQUIRE)) continue;
        if (strncmp(e->path, prefix, pl) != 0) continue;
        pthread_rwlock_wrlock(&e->rwlock);
        if (__atomic_load_n(&e->used, __ATOMIC_ACQUIRE) &&
            strncmp(e->path, prefix, pl) == 0) {
            if (e->map && e->map_size > 0) msync(e->map, e->map_size, MS_ASYNC);
            if (e->map) munmap(e->map, e->map_size);
            if (e->fd >= 0) close(e->fd);
            e->map = NULL;
            e->fd = -1;
            e->map_size = 0;
            e->path[0] = '\0';
            atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
            __atomic_store_n(&e->used, 0, __ATOMIC_RELEASE);
            __sync_fetch_and_sub(&g_segcache_count, 1);
        }
        pthread_rwlock_unlock(&e->rwlock);
    }
}

static void segcache_drop_slot(int slot) {
    SegCacheEntry *e = &g_segcache[slot];
    if (!e->used) return;
    if (e->map && e->map_size > 0) msync(e->map, e->map_size, MS_ASYNC);
    if (e->map) munmap(e->map, e->map_size);
    if (e->fd >= 0) close(e->fd);
    e->map = NULL;
    e->fd = -1;
    e->map_size = 0;
    e->used = 0;
    e->path[0] = '\0';
    /* Increment gen under g_segcache_lock (caller always holds it).
       Any SlotRef pointing at this slot will fail its gen check, forcing
       the slow-path re-probe. */
    atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
    g_segcache_count--;
}

/* Open + ftruncate to SLOTCASK_SEG_MAX_BYTES (sparse) + mmap MAP_SHARED. */
static int seg_open_file(const char *path, int create,
                         int *out_fd, uint8_t **out_map, size_t *out_size) {
    int fd;
    if (create) {
        char dir[PATH_MAX];
        snprintf(dir, sizeof(dir), "%s", path);
        char *slash = strrchr(dir, '/');
        if (slash) { *slash = 0; mkdirp_local(dir); }
        fd = open(path, O_RDWR | O_CREAT, 0644);
    } else {
        fd = open(path, O_RDWR);
    }
    if (fd < 0) return -1;

    struct stat st;
    if (fstat(fd, &st) < 0) { close(fd); return -1; }
    if ((size_t)st.st_size < SLOTCASK_SEG_MAX_BYTES) {
        if (!create) { close(fd); return -1; }
        if (ftruncate(fd, (off_t)SLOTCASK_SEG_MAX_BYTES) < 0) {
            close(fd); return -1;
        }
    }
    void *m = mmap(NULL, SLOTCASK_SEG_MAX_BYTES, PROT_READ | PROT_WRITE,
                   MAP_SHARED, fd, 0);
    if (m == MAP_FAILED) { close(fd); return -1; }
    /* Transparent huge pages hint — segments are 128 MB sparse files
       walked sequentially during scans and randomly during point reads.
       2 MB hugepages (vs 4 KB) cut TLB entries by 500× over the
       working set. Kernel ignores if THP is off; no functional impact. */
    SHARD_MADV_HUGEPAGE(m, SLOTCASK_SEG_MAX_BYTES);
    *out_fd = fd;
    *out_map = (uint8_t *)m;
    *out_size = SLOTCASK_SEG_MAX_BYTES;
    return 0;
}

int segcache_acquire(SlotcaskSegHandle *h, const char *path,
                     int create, int writer) {
    h->slot = -1;
    h->writer = writer;
    h->fd = -1;
    h->map = NULL;
    h->map_size = 0;

    if (!g_segcache) {
        if (seg_open_file(path, create, &h->fd, &h->map, &h->map_size) < 0) return -1;
        return 0;
    }

    int retries = 0;
    int found = 0, slot = -1;
    pthread_mutex_lock(&g_segcache_lock);
    while (1) {
        slot = segcache_probe(path, &found);
        if (!found) break;
        g_segcache[slot].last_access =
            __atomic_add_fetch(&g_segcache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &g_segcache[slot].rwlock;
        pthread_mutex_unlock(&g_segcache_lock);
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);

        /* coverity[atomicity] CID 1693837: `slot` came from the prior
           locked section, but we re-verify identity below
           (e->used && strcmp(e->path, path) == 0). On mismatch we
           drop the rwlock and retry — the verify is the consistency
           barrier the analyzer can't see. */
        SegCacheEntry *e = &g_segcache[slot];
        if (e->used && strcmp(e->path, path) == 0) {
            h->slot = slot;
            h->fd = e->fd;
            h->map = e->map;
            h->map_size = e->map_size;
            /* coverity[missing_unlock] intentional: returning with the
               per-slot rwlock held; caller releases via segcache_release. */
            return 0;
        }
        pthread_rwlock_unlock(lock);
        if (++retries >= 4) {
            /* slot/found get re-set by the segcache_probe call below
               in the install path (Coverity CID 1693845). */
            pthread_mutex_lock(&g_segcache_lock);
            break;
        }
        pthread_mutex_lock(&g_segcache_lock);
    }

    pthread_mutex_unlock(&g_segcache_lock);
    int fd; uint8_t *map; size_t sz;
    if (seg_open_file(path, create, &fd, &map, &sz) < 0) return -1;
    pthread_mutex_lock(&g_segcache_lock);

    slot = segcache_probe(path, &found);
    if (found) {
        munmap(map, sz);
        close(fd);
        g_segcache[slot].last_access =
            __atomic_add_fetch(&g_segcache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &g_segcache[slot].rwlock;
        pthread_mutex_unlock(&g_segcache_lock);
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);
        SegCacheEntry *e = &g_segcache[slot];
        if (e->used && strcmp(e->path, path) == 0) {
            h->slot = slot;
            h->fd = e->fd;
            h->map = e->map;
            h->map_size = e->map_size;
            /* coverity[missing_unlock] intentional: returning with the
               per-slot rwlock held; caller releases via segcache_release. */
            return 0;
        }
        pthread_rwlock_unlock(lock);
        if (seg_open_file(path, create, &h->fd, &h->map, &h->map_size) < 0) return -1;
        return 0;
    }

    if (slot < 0 || g_segcache_count >= g_segcache_slots / 2) {
        int lru = -1;
        uint64_t oldest = UINT64_MAX;
        for (int i = 0; i < g_segcache_slots; i++) {
            if (g_segcache[i].used && g_segcache[i].last_access < oldest) {
                oldest = g_segcache[i].last_access;
                lru = i;
            }
        }
        if (lru >= 0) { segcache_drop_slot(lru); slot = lru; }
    }

    if (slot < 0) {
        pthread_mutex_unlock(&g_segcache_lock);
        h->slot = -1;
        h->fd = fd;
        h->map = map;
        h->map_size = sz;
        return 0;
    }

    SegCacheEntry *e = &g_segcache[slot];
    strncpy(e->path, path, PATH_MAX - 1);
    e->path[PATH_MAX - 1] = '\0';
    e->fd = fd;
    e->map = map;
    e->map_size = sz;
    e->used = 1;
    e->last_access = __atomic_add_fetch(&g_segcache_clock, 1, __ATOMIC_RELAXED);
    g_segcache_count++;

    pthread_rwlock_t *lock = &e->rwlock;
    if (writer) pthread_rwlock_wrlock(lock);
    else        pthread_rwlock_rdlock(lock);
    pthread_mutex_unlock(&g_segcache_lock);

    h->slot = slot;
    h->fd = fd;
    h->map = map;
    h->map_size = sz;
    return 0;
}

void segcache_release(SlotcaskSegHandle *h) {
    if (h->slot >= 0) {
        pthread_rwlock_unlock(&g_segcache[h->slot].rwlock);
    } else if (h->map) {
        munmap(h->map, h->map_size);
        if (h->fd >= 0) close(h->fd);
    }
    h->slot = -1;
    h->fd = -1;
    h->map = NULL;
    h->map_size = 0;
}

/* Return a pointer to db->seg_slot_refs[stream_id][file_id], growing the
   per-stream array if file_id is past the current capacity.  Returns NULL
   on OOM (caller falls back to slow path).  Guarded by the stream's existing
   pool_lock so concurrent read threads racing on the same stream_id cannot
   race on the realloc. */
static SlotRef *seg_ref_for(SlotcaskDb *db, int stream_id, uint32_t file_id) {
    if (!db->seg_slot_refs || !db->seg_slot_caps) return NULL;
    if (stream_id < 0 || stream_id >= db->num_streams) return NULL;
    pthread_mutex_lock(&db->streams[stream_id].pool_lock);
    int cap = db->seg_slot_caps[stream_id];
    if ((int)file_id >= cap) {
        int new_cap = cap ? cap * 2 : 4;
        while (new_cap <= (int)file_id) new_cap *= 2;
        SlotRef *arr = realloc(db->seg_slot_refs[stream_id],
                                (size_t)new_cap * sizeof(SlotRef));
        if (!arr) {
            pthread_mutex_unlock(&db->streams[stream_id].pool_lock);
            return NULL;
        }
        /* Zero-init the new portion (slot = 0, gen = 0 is NOT "invalid"
           because slot 0 is a valid slot.  We distinguish "not yet populated"
           by initialising slot to -1 in the new entries. */
        for (int i = cap; i < new_cap; i++) arr[i].slot = -1;
        db->seg_slot_refs[stream_id] = arr;
        db->seg_slot_caps[stream_id] = new_cap;
    }
    SlotRef *result = &db->seg_slot_refs[stream_id][file_id];
    pthread_mutex_unlock(&db->streams[stream_id].pool_lock);
    return result;
}

/* Fast-path segcache acquire for read-only callers that hold a SlotRef.
 *
 * Warm hit (common case):
 *   1. Atomic-load e->gen; compare with ref->gen.
 *   2. If equal: take per-slot rdlock, verify identity, fill handle.
 *      No g_segcache_lock touched. Cost: 1 atomic load + 1 rdlock.
 *
 * Cold/evicted: fall through to segcache_acquire(create=0, writer=0),
 * then update *ref.
 *
 * IMPORTANT: ref may be NULL (if the per-stream array is not yet
 * allocated, or if file_id >= seg_slot_caps[stream_id]). In that case
 * we fall straight through to the slow path without crashing.
 */
int segcache_acquire_direct(SlotcaskSegHandle *h, SlotRef *ref,
                             const char *path) {
    if (ref && ref->slot >= 0) {
        int s = ref->slot;
        SegCacheEntry *e = &g_segcache[s];
        uint64_t cur_gen = atomic_load_explicit(&e->gen, memory_order_acquire);
        if (cur_gen == ref->gen) {
            pthread_rwlock_rdlock(&e->rwlock);
            if (__atomic_load_n(&e->used, __ATOMIC_ACQUIRE) &&
                strcmp(e->path, path) == 0) {
                /* Warm hit confirmed. */
                h->slot = s;
                h->writer = 0;
                h->fd = e->fd;
                h->map = e->map;
                h->map_size = e->map_size;
                return 0;
            }
            pthread_rwlock_unlock(&e->rwlock);
        }
    }

    /* Slow path. */
    int rc = segcache_acquire(h, path, 0, 0);
    if (rc == 0 && ref && h->slot >= 0) {
        ref->slot = h->slot;
        ref->gen  = atomic_load_explicit(&g_segcache[h->slot].gen,
                                          memory_order_acquire);
    }
    return rc;
}

/* ============================================================ Init / shutdown */

void slotcask_init(int kfcache_cap, int segcache_cap) {
    kfcache_init(kfcache_cap);
    segcache_init(segcache_cap);
}

void slotcask_shutdown(void) {
    /* Close all per-object DBs first — they msync segments through segcache,
       so the caches must still be alive when slotcask_close runs. */
    slotcask_registry_shutdown();
    kfcache_shutdown();
    segcache_shutdown();
}

int slotcask_streams_for_nproc(void) {
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n <= 0) return 4;
    if (n <= 8) return (int)n;
    if (n <= 16) return 8;
    return 16;
}

/* Pick kf slots/shard from a tier table keyed on `splits`. Per-tier
   targets ~50 % load at 78K-200K rec/shard (CLAUDE.md sweet spot).
   Total kf footprint stays bounded (24 MB at splits=8 → 6 GB at
   splits=4096); auto-resplit handles any tier overshoot. See the
   comment block in slotcask.h for the full table + rationale. */
size_t slotcask_default_slots_for_splits(int splits) {
    if (splits <= 16)   return 1024u * 1024;   /* 1M  */
    if (splits <= 128)  return 256u  * 1024;   /* 256K */
    if (splits <= 1024) return 128u  * 1024;   /* 128K */
    return 64u * 1024;                         /* 64K  */
}

/* ============================================================ Keyfile ops
 *
 * All keyfile mutations route through kfcache_acquire(writer=1). The cache
 * entry's rwlock serializes writers per shard; readers go via rdlock + atomic
 * loads. The 24B SlotcaskKfEntry layout keeps the trailing 8B (flag + stream +
 * file_id + offset) 8-byte aligned, so kf_repoint commits via a single
 * __atomic_store_n on that uint64. */

/* Slot-header field accessors — the on-disk slot layout is:
   [0..16) hash  [16..18) klen u16  [18] flag  [19] reserved  [20..24) vlen u32
   [24..24+klen) key bytes  [24+klen..) value bytes.
   These wrappers keep the magic numbers in one place and document intent
   at the read sites. Writes still go through seg_record_emit. */
static inline uint16_t seg_rec_klen(const uint8_t *rec) {
    uint16_t k; memcpy(&k, rec + 16, 2); return k;
}
static inline uint32_t seg_rec_vlen(const uint8_t *rec) {
    uint32_t v; memcpy(&v, rec + 20, 4); return v;
}
/* True iff the slot is live (flag=1) and its 16B hash matches `hash`.
   Common pattern for fast-pruning a probe-walk hit before key-byte verify.
   The flag byte read uses acquire ordering to pair with the writer's
   release-store in seg_record_emit / seg_write_flag — that's what
   makes the rest of the record (hash, key, value) safe to read after
   seeing flag==1. */
static inline int seg_rec_live_with_hash(const uint8_t *rec,
                                          const uint8_t hash[16]) {
    return __atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) == 1 &&
           memcmp(rec, hash, 16) == 0;
}

/* Verify the on-disk record's stored key matches `key`. Returns 1 if match,
   0 if different (hash collision), -1 on I/O error. */
static int verify_stored_key(const char *data_dir, uint8_t stream_id,
                             uint16_t file_id, uint32_t offset,
                             const void *key, size_t klen) {
    char path[PATH_MAX];
    seg_path_for(path, data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0) != 0) return -1;
    const uint8_t *rec = h.map + offset;
    uint16_t k_stored = seg_rec_klen(rec);
    if (k_stored != klen) { segcache_release(&h); return 0; }
    int match = (memcmp(rec + 24, key, klen) == 0);
    segcache_release(&h);
    return match ? 1 : 0;
}

/* Insert NEW key (no upsert). Returns 0 ok, 1 already exists, -1 error.
   Caller holds the kf shard's wrlock via the kfcache handle.
   First-tombstone strategy: probe the full chain to detect existence,
   remembering the first flag=2 (tombstone) slot we passed. If the key
   isn't found by chain end (flag=0), insert at the first-tombstone slot
   if any, else at the empty slot. This reuses kf tombstones for any new
   key — same shape as the seg pool — so kf doesn't accumulate dead
   entries in delete-heavy workloads.
   Same-key resurrection still gets a hot-path shortcut: when we hit a
   flag=2 slot whose hash matches, we reuse it directly without
   continuing the probe. That's the most-common reuse case (delete-and-
   reinsert) and is one less probe step. */
/* Double a kf shard's on-disk + mmap'd capacity. Caller holds kh's wrlock
   (acquired via kfcache_acquire(writer=1)), so no other thread is touching
   this entry. Crash-safe via kf.new staging + atomic rename + parent-dir
   fsync. After return, kh and the underlying KfCacheEntry both reflect the
   new capacity; the old fd/map are torn down.

   Streaming: walks the old kf in order and writes each flag=1 entry
   directly into kf.new's mmap via linear probing at the new capacity.
   Zero malloc; memory cost stays flat regardless of shard size.
   Tombstones (flag=2) are dropped — the new kf is fully compacted
   (header.total == header.live, header.deleted = 0).
   On error, kh is unchanged (the old kf is the live file). */
/* High-resolution monotonic timer in microseconds. Used by resplit
   instrumentation to break down the cost of each phase. */
static inline uint64_t kf_now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

static int kfcache_resplit_locked(SlotcaskKfHandle *kh, size_t new_cap) {
    if (kh->slot < 0 || !kh->writer) return -1;
    /* Allow same-cap rebuild: used by vacuum's kf-compaction path to
       drop tombstones in place without changing capacity. The probe-
       rebuild loop below copies only flag=1 (live) entries, so the
       resulting kf has total = live, deleted = 0. */
    if (new_cap < kh->capacity) return -1;

    KfCacheEntry *e = &g_kfcache[kh->slot];
    size_t old_cap = kh->capacity;
    size_t new_size = SLOTCASK_KF_HDR_SIZE + new_cap * sizeof(SlotcaskKfEntry);

    uint64_t t_start = kf_now_us();

    /* === Phase A: open + ftruncate + mmap new file === */
    char new_path[PATH_MAX];
    snprintf(new_path, sizeof(new_path), "%s.new", e->path);
    int new_fd = open(new_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (new_fd < 0) return -1;
    if (ftruncate(new_fd, (off_t)new_size) < 0) {
        close(new_fd); unlink(new_path); return -1;
    }
    /* MAP_POPULATE prefaults every page of the new mapping in one syscall,
       so the probe-rebuild loop below doesn't pay per-page faults inline.
       Without it, each cache-cold write into the 12.6 MB sparse file
       triggers a page allocation + dirty-tracking hook (~30-80 µs amortised).
       With ~3,150 pages and uneven write order, that page-fault tax adds
       up to 50-100 ms per resplit. MAP_POPULATE consolidates it into one
       up-front cost the kernel can batch. */
    uint8_t *new_base = mmap(NULL, new_size, PROT_READ | PROT_WRITE,
                              MAP_SHARED | MAP_POPULATE, new_fd, 0);
    if (new_base == MAP_FAILED) {
        close(new_fd); unlink(new_path); return -1;
    }
    SHARD_MADV_HUGEPAGE(new_base, new_size);  /* THP hint for kf rebuild */

    SlotcaskKfHeader *new_hdr = (SlotcaskKfHeader *)new_base;
    new_hdr->magic = SLOTCASK_KF_MAGIC;
    new_hdr->version = SLOTCASK_KF_VERSION;
    new_hdr->total = 0;
    new_hdr->deleted = 0;
    SlotcaskKfEntry *new_entries =
        (SlotcaskKfEntry *)(new_base + SLOTCASK_KF_HDR_SIZE);
    uint64_t t_after_setup = kf_now_us();

    /* === Phase B: probe-rebuild loop === */
    uint64_t live_copied = 0;
    for (size_t i = 0; i < old_cap; i++) {
        SlotcaskKfEntry *src = &kh->map[i];
        if (src->flag != 1) continue;
        size_t start = kf_slot_for(src->hash, new_cap);
        for (size_t j = 0; j < new_cap; j++) {
            size_t s = (start + j) % new_cap;
            if (new_entries[s].flag == 0) {
                new_entries[s] = *src;
                live_copied++;
                break;
            }
        }
    }
    new_hdr->total = live_copied;
    new_hdr->deleted = 0;
    uint64_t t_after_rebuild = kf_now_us();

    /* === Phase C: msync(MS_SYNC) === */
    msync(new_base, new_size, MS_SYNC);
    munmap(new_base, new_size);
    close(new_fd);
    uint64_t t_after_msync = kf_now_us();

    /* === Phase D: rename + parent-dir fsync === */
    if (rename(new_path, e->path) != 0) {
        unlink(new_path);
        return -1;
    }
    char parent[PATH_MAX];
    snprintf(parent, sizeof(parent), "%s", e->path);
    char *slash = strrchr(parent, '/');
    if (slash) {
        *slash = '\0';
        int dfd = open(parent, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
    }
    uint64_t t_after_rename = kf_now_us();

    /* === Phase E: remap (close old, open new path, mmap fresh) === */
    int reopen_fd = open(e->path, O_RDWR);
    if (reopen_fd < 0) return -1;
    void *fresh = mmap(NULL, new_size, PROT_READ | PROT_WRITE,
                        MAP_SHARED, reopen_fd, 0);
    if (fresh == MAP_FAILED) { close(reopen_fd); return -1; }
    SHARD_MADV_HUGEPAGE(fresh, new_size);  /* THP hint for resplit remap */

    if (e->base && e->map_size > 0) msync(e->base, e->map_size, MS_ASYNC);
    if (e->base) munmap(e->base, e->map_size);
    if (e->fd >= 0) close(e->fd);

    e->fd = reopen_fd;
    e->base = (uint8_t *)fresh;
    e->map_size = new_size;
    e->capacity = new_cap;

    kh->fd = reopen_fd;
    kh->hdr = (SlotcaskKfHeader *)e->base;
    kh->map = (SlotcaskKfEntry *)(e->base + SLOTCASK_KF_HDR_SIZE);
    kh->map_size = new_size;
    kh->capacity = new_cap;
    uint64_t t_end = kf_now_us();

    /* Phase breakdown: setup / rebuild / msync / rename+fsync / remap.
       `live` is records actually re-inserted at new capacity (= header.total
       on the new kf). Routed through LOG_INFO so it lands in the daemon's
       info log instead of spamming stderr (the bench harness was getting
       hundreds of lines during 25M inserts because raw fprintf bypasses
       LOG_LEVEL filtering). Bench fixture's daemon log is at $DB_ROOT/logs/.
       Suppressed for same-cap rebuilds (vacuum's tombstone-compaction path)
       — those are routine maintenance, not a notable resize event. */
    if (new_cap != old_cap) {
        LOG_INFO(LOG_SUB_SLOTCASK,
            "KF_RESPLIT path=%s old_cap=%zu new_cap=%zu live=%lu "
            "total_us=%lu setup_us=%lu rebuild_us=%lu msync_us=%lu rename_us=%lu remap_us=%lu",
            e->path, old_cap, new_cap, (unsigned long)live_copied,
            (unsigned long)(t_end - t_start),
            (unsigned long)(t_after_setup   - t_start),
            (unsigned long)(t_after_rebuild - t_after_setup),
            (unsigned long)(t_after_msync   - t_after_rebuild),
            (unsigned long)(t_after_rename  - t_after_msync),
            (unsigned long)(t_end           - t_after_rename));
    }
    return 0;
}

/* Forward decl — defined further down with the rest of the free-pool
   primitives. Needed here because the parallel_for workers below
   (slotcask_pool_rebuild_worker) call it before its file position. */
static int pool_push_free(SlotcaskStream *p, uint16_t file_id, uint32_t offset, int max_slot_size);

/* Pre-grow worker: opens one kf shard with wrlock, projects post-insert
   load, resplits in-place until projected load <= 75% (or hits the
   per-shard cap). Called via parallel_for_io from slotcask_pregrow_kf
   so all shards' resplits overlap with each other but not with any
   concurrent inserter. */
typedef struct {
    SlotcaskDb *db;
    int         kf_shard_id;
    size_t      add_records;
} SlotcaskPregrowArg;

static void *slotcask_pregrow_worker(void *raw) {
    SlotcaskPregrowArg *a = (SlotcaskPregrowArg *)raw;
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, a->db->data_dir, a->kf_shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, a->db->slots_per_shard, 1) != 0)
        return NULL;
    if (kh.hdr) {
        uint64_t cur_total = kh.hdr->total;
        uint64_t projected = cur_total + (uint64_t)a->add_records;
        /* 75% load trigger matches kf_put_new's inline check. Resplit in
           a loop in case projected load exceeds even the doubled cap. */
        while (kh.capacity < SLOTCASK_MAX_SLOTS_PER_SHARD &&
               projected * 4 >= (uint64_t)kh.capacity * 3) {
            if (kfcache_resplit_locked(&kh, kh.capacity * 2) != 0) break;
        }
    }
    kfcache_release(&kh);
    return NULL;
}

/* Pre-grow every kf shard to absorb `total_new` upcoming inserts without
   triggering inline resplits during the bulk path. Distributes the count
   uniformly across shards (hash-routing assumption), then resplits each
   shard in parallel. Pays the resplit cost once, up-front, in a "quiet"
   moment — avoids the I/O queue contention we'd otherwise hit when
   resplit msync collides with concurrent segment writes (4-5 sec per
   resplit at 25M scale, vs ~50ms when run pre-insert). */
int slotcask_pregrow_kf(SlotcaskDb *db, size_t total_new) {
    if (!db || total_new == 0 || db->num_shards <= 0) return 0;
    int n = db->num_shards;
    size_t per_shard = (total_new + (size_t)n - 1) / (size_t)n;
    SlotcaskPregrowArg *args = calloc((size_t)n, sizeof(SlotcaskPregrowArg));
    if (!args) return -1;
    for (int s = 0; s < n; s++) {
        args[s].db = db;
        args[s].kf_shard_id = s;
        args[s].add_records = per_shard;
    }
    parallel_for_io(slotcask_pregrow_worker, args, n, sizeof(SlotcaskPregrowArg));
    free(args);
    return 0;
}

/* Per-shard kf-materialise worker for slotcask_open's parallel init.
   Cleans any leftover .new staging file from a prior crashed resplit,
   then opens + mmaps the shard so subsequent accesses hit the cache. */
typedef struct {
    SlotcaskDb *db;
    int         shard_id;
    int         ok;          /* 1 = success, 0 = open failed (any error) */
    int         needs_pool;  /* 1 if hdr->deleted > 0 (pool-rebuild required) */
} SlotcaskOpenArg;

static void *slotcask_open_kf_worker(void *raw) {
    SlotcaskOpenArg *a = (SlotcaskOpenArg *)raw;
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, a->db->data_dir, a->shard_id);
    char kf_new[PATH_MAX];
    snprintf(kf_new, sizeof(kf_new), "%s.new", kf_path);
    (void)unlink(kf_new);  /* idempotent — ENOENT is fine */
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, a->db->slots_per_shard, 1) != 0) {
        a->ok = 0;
        a->needs_pool = 0;
        return NULL;
    }
    a->ok = 1;
    a->needs_pool = (kh.hdr && kh.hdr->deleted > 0) ? 1 : 0;
    kfcache_release(&kh);
    return NULL;
}

/* Per-shard pool-rebuild worker. Walks the kf shard for flag=2 entries
   and pushes each (file_id, offset) onto the right stream's free pool.
   Only invoked for shards whose header reports deleted > 0; clean kf
   shards skip this O(slots_per_shard) memory walk entirely. */
static void *slotcask_pool_rebuild_worker(void *raw) {
    SlotcaskOpenArg *a = (SlotcaskOpenArg *)raw;
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, a->db->data_dir, a->shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, a->db->slots_per_shard, 0) != 0) return NULL;
    size_t cap = kh.capacity;
    SlotcaskKfEntry *kf = kh.map;
    for (size_t i = 0; i < cap; i++) {
        if (kf[i].flag == 2 && kf[i].stream_id < a->db->num_streams) {
            pool_push_free(&a->db->streams[kf[i].stream_id],
                            kf[i].file_id, kf[i].offset, a->db->slot_size);
        }
    }
    kfcache_release(&kh);
    return NULL;
}

/* Insert a brand-new kf entry. Probes via linear hashing; on first-tombstone
   reuse, repurposes that slot. Before probing, checks the per-shard
   header.total — when it crosses 75 % of capacity, the shard doubles via
   kfcache_resplit_locked. Header reads/writes are plain mmap loads/stores
   under the kf wrlock — no atomics, no syscalls. */
static int kf_put_new(SlotcaskDb *db, SlotcaskKfHandle *kh, const uint8_t hash[16],
                      uint8_t stream_id, uint16_t file_id, uint32_t offset,
                      const void *key, size_t klen, const char *data_dir,
                      size_t *used_delta, size_t *out_slot) {
    (void)db;  /* db param retained for ABI; per-shard load lives in the header */
    /* Load-triggered resplit. The header tracks `total` (= live + tombstoned),
       which matches the linear-probe pressure that drives chain length —
       tombstones still force probing past them on lookup. 75 % trigger keeps
       average probe chains around 4 (textbook). No upper cap: streaming
       resplit has flat memory cost and shard-stats surfaces operator-visible
       skew if a single shard ever becomes unwieldy. */
    if (kh->hdr) {
        uint64_t total = kh->hdr->total;
        uint64_t cap = (uint64_t)kh->capacity;
        if (cap > 0 && total * 4 >= cap * 3) {
            (void)kfcache_resplit_locked(kh, kh->capacity * 2);
            /* Resplit failure isn't fatal — fall through and try the put on
               the old capacity. If THAT fails too (truly full), caller
               bubbles the error up. */
        }
    }

    size_t cap = kh->capacity;
    /* Defensive: kfcache_resplit_locked never sets capacity to 0 on
       failure (success path is the only writer), but Coverity can't
       trace that across the call (CID 1693834: divide-by-zero in
       kf_slot_for below). Bail explicitly so a future regression in
       resplit can't silently turn into UB. */
    if (cap == 0) return -1;
    SlotcaskKfEntry *kf = kh->map;
    SlotcaskKfHeader *hdr = kh->hdr;
    size_t start = kf_slot_for(hash, cap);
    size_t first_tomb = (size_t)-1;
    for (size_t i = 0; i < cap; i++) {
        size_t slot = (start + i) % cap;
        SlotcaskKfEntry *e = &kf[slot];
        if (e->flag == 0) {
            /* End of probe chain. Insert at first-tombstone if we saw one
               (decrements deleted; total stays — slot was already counted),
               otherwise at this empty slot (increments total). */
            size_t target_idx = (first_tomb != (size_t)-1) ? first_tomb : slot;
            SlotcaskKfEntry *t = &kf[target_idx];
            int reused_tomb = (first_tomb != (size_t)-1);
            memcpy(t->hash, hash, 16);
            t->stream_id = stream_id;
            t->file_id = file_id;
            t->offset = offset;
            __atomic_thread_fence(__ATOMIC_RELEASE);
            t->flag = 1;
            if (hdr) {
                if (reused_tomb) hdr->deleted--;   /* tombstone reclaimed */
                else             hdr->total++;    /* fresh slot occupied */
            }
            (*used_delta)++;
            if (out_slot) *out_slot = target_idx;
            return 0;
        }
        if (e->flag == 1 && memcmp(e->hash, hash, 16) == 0) {
            int km = verify_stored_key(data_dir, e->stream_id, e->file_id,
                                       e->offset, key, klen);
            if (km < 0) return -1;
            if (km == 1) return 1;
        }
        if (e->flag == 2 && memcmp(e->hash, hash, 16) == 0) {
            /* Same-key resurrection — reuse this exact slot, skip probe. */
            e->stream_id = stream_id;
            e->file_id = file_id;
            e->offset = offset;
            __atomic_thread_fence(__ATOMIC_RELEASE);
            e->flag = 1;
            if (hdr) hdr->deleted--;  /* tombstone reclaimed; total unchanged */
            (*used_delta)++;
            if (out_slot) *out_slot = slot;
            return 0;
        }
        if (e->flag == 2 && first_tomb == (size_t)-1) {
            first_tomb = slot;
        }
    }
    /* Probed full chain without finding flag=0. If we saw a tombstone,
       reuse it. Otherwise the table is genuinely full. */
    if (first_tomb != (size_t)-1) {
        SlotcaskKfEntry *t = &kf[first_tomb];
        memcpy(t->hash, hash, 16);
        t->stream_id = stream_id;
        t->file_id = file_id;
        t->offset = offset;
        __atomic_thread_fence(__ATOMIC_RELEASE);
        t->flag = 1;
        if (hdr) hdr->deleted--;  /* tombstone reclaimed */
        (*used_delta)++;
        if (out_slot) *out_slot = first_tomb;
        return 0;
    }
    return -1;
}

/* Probe-chain iterator. Encapsulates the open-addressed walk so callers
   only express their per-match action (verify + commit). Initialise via
   kf_probe_init; each kf_probe_next call returns the slot index of the
   next hash-matching entry along the chain, or (size_t)-1 when the chain
   terminates (flag=0 sentinel hit) or the table is exhausted. Caller
   inspects e->flag at each returned slot to discriminate live (1) vs
   tombstone (2), and continues iterating to walk past true xxh128
   collisions whose verify_stored_key fails. */
typedef struct {
    SlotcaskKfHandle *kh;
    const uint8_t    *hash;
    size_t            cap;
    size_t            start;
    size_t            i;
} KfProbeIter;

static inline void kf_probe_init(KfProbeIter *it, SlotcaskKfHandle *kh,
                                  const uint8_t hash[16]) {
    it->kh    = kh;
    it->hash  = hash;
    it->cap   = kh->capacity;
    it->start = kf_slot_for(hash, it->cap);
    it->i     = 0;
}

static size_t kf_probe_next(KfProbeIter *it) {
    SlotcaskKfEntry *kf = it->kh->map;
    while (it->i < it->cap) {
        size_t slot = (it->start + it->i) % it->cap;
        SlotcaskKfEntry *e = &kf[slot];
        if (e->flag == 0) { it->i = it->cap; return (size_t)-1; }
        it->i++;
        if (memcmp(e->hash, it->hash, 16) == 0) return slot;
    }
    return (size_t)-1;
}

/* Look up. Returns 0 found (writes outputs), -1 not present. */
static int kf_lookup(SlotcaskKfHandle *kh, const uint8_t hash[16],
                     const void *key, size_t klen, const char *data_dir,
                     uint8_t *flag_out, uint8_t *stream_id_out,
                     uint16_t *file_id_out, uint32_t *offset_out) {
    KfProbeIter it; kf_probe_init(&it, kh, hash);
    size_t slot;
    while ((slot = kf_probe_next(&it)) != (size_t)-1) {
        SlotcaskKfEntry *e = &kh->map[slot];
        if (e->flag == 2) return -1;
        int km = verify_stored_key(data_dir, e->stream_id, e->file_id,
                                   e->offset, key, klen);
        if (km < 0) return -1;
        if (km == 1) {
            *flag_out = e->flag;
            *stream_id_out = e->stream_id;
            *file_id_out = e->file_id;
            *offset_out = e->offset;
            return 0;
        }
    }
    return -1;
}

/* Probe-only variant — returns the kf entry's location without calling
   verify_stored_key. Used by bulk_lookup / bulk_get which defer the
   verify to a batched seg-read phase keyed by (sid, fid). False
   positives on raw hash match are filtered out by the batched verify;
   on a true match the (sid, fid, off, slot) is authoritative. */
static int kf_lookup_no_verify(SlotcaskKfHandle *kh, const uint8_t hash[16],
                                uint8_t *flag_out, uint8_t *stream_id_out,
                                uint16_t *file_id_out, uint32_t *offset_out,
                                size_t *slot_out) {
    KfProbeIter it; kf_probe_init(&it, kh, hash);
    size_t slot = kf_probe_next(&it);
    if (slot == (size_t)-1) return -1;
    SlotcaskKfEntry *e = &kh->map[slot];
    if (e->flag == 2) return -1;          /* tombstone */
    *flag_out = e->flag;
    *stream_id_out = e->stream_id;
    *file_id_out = e->file_id;
    *offset_out = e->offset;
    *slot_out = slot;
    return 0;
}

/* Variant of kf_lookup that ALSO returns the slot index it found the entry
   at. The bulk primitives use this so Phase 4 (kf_repoint) and Phase 2
   (kf_tombstone) can skip re-probing — under a held kf wrlock the slot
   index from Phase 1a is still authoritative. */
static int kf_lookup_with_slot(SlotcaskKfHandle *kh, const uint8_t hash[16],
                                const void *key, size_t klen,
                                const char *data_dir,
                                uint8_t *flag_out, uint8_t *stream_id_out,
                                uint16_t *file_id_out, uint32_t *offset_out,
                                size_t *slot_out) {
    KfProbeIter it; kf_probe_init(&it, kh, hash);
    size_t slot;
    while ((slot = kf_probe_next(&it)) != (size_t)-1) {
        SlotcaskKfEntry *e = &kh->map[slot];
        if (e->flag == 2) return -1;
        int km = verify_stored_key(data_dir, e->stream_id, e->file_id,
                                   e->offset, key, klen);
        if (km < 0) return -1;
        if (km == 1) {
            *flag_out = e->flag;
            *stream_id_out = e->stream_id;
            *file_id_out = e->file_id;
            *offset_out = e->offset;
            *slot_out = slot;
            return 0;
        }
    }
    return -1;
}

/* Direct kf_repoint at a known slot — skips probe + verify since the
   caller (bulk primitive Phase 4) already holds the wrlock that was
   acquired BEFORE the kf_lookup_with_slot call, so the slot is still
   authoritative. The on-disk state stays self-consistent on crash via
   the same single-uint64 atomic store kf_repoint uses. */
static inline void kf_repoint_at_slot(SlotcaskKfHandle *kh, size_t slot,
                                       uint8_t new_stream_id,
                                       uint16_t new_file_id,
                                       uint32_t new_offset) {
    SlotcaskKfEntry *e = &kh->map[slot];
    union {
        struct {
            uint8_t  flag;
            uint8_t  stream_id;
            uint16_t file_id;
            uint32_t offset;
        } parts;
        uint64_t u64;
    } combo;
    combo.parts.flag = 1;
    combo.parts.stream_id = new_stream_id;
    combo.parts.file_id = new_file_id;
    combo.parts.offset = new_offset;
    __atomic_store_n((uint64_t *)((uint8_t *)e + 16), combo.u64,
                     __ATOMIC_RELEASE);
}

/* Direct tombstone at a known slot — see kf_repoint_at_slot rationale.
   Bumps the header's deleted counter; total is unchanged (the slot was
   already counted as non-empty when it was inserted). */
static inline void kf_tombstone_at_slot(SlotcaskKfHandle *kh, size_t slot) {
    kh->map[slot].flag = 2;
    if (kh->hdr) kh->hdr->deleted++;
}

/* Repoint EXISTING key's slot. Atomic 8B store on the trailing group keeps
   the on-disk state self-consistent if we crash mid-flight. */
static int kf_repoint(SlotcaskKfHandle *kh, const uint8_t hash[16],
                      uint8_t new_stream_id, uint16_t new_file_id,
                      uint32_t new_offset, const void *key, size_t klen,
                      const char *data_dir) {
    KfProbeIter it; kf_probe_init(&it, kh, hash);
    size_t slot;
    while ((slot = kf_probe_next(&it)) != (size_t)-1) {
        SlotcaskKfEntry *e = &kh->map[slot];
        if (e->flag != 1) continue;       /* tombstone or other → keep walking */
        int km = verify_stored_key(data_dir, e->stream_id, e->file_id,
                                   e->offset, key, klen);
        if (km < 0) return -1;
        if (km == 1) {
            union {
                struct {
                    uint8_t  flag;
                    uint8_t  stream_id;
                    uint16_t file_id;
                    uint32_t offset;
                } parts;
                uint64_t u64;
            } combo;
            combo.parts.flag = 1;
            combo.parts.stream_id = new_stream_id;
            combo.parts.file_id = new_file_id;
            combo.parts.offset = new_offset;
            __atomic_store_n((uint64_t *)((uint8_t *)e + 16), combo.u64,
                             __ATOMIC_RELEASE);
            return 0;
        }
    }
    return -1;
}

/* ============================================================ Pool bucket helper */

int slotcask_bucket_for(uint32_t capacity, int max_slot_size) {
    if (capacity < 256) return 0;
    if (capacity < 1024) return 1;
    if (capacity < 8192) return 2;
    (void)max_slot_size;
    return 3;
}

/* ============================================================ Free pool */

static int pool_push_free_cap(SlotcaskStream *p, uint16_t file_id,
                               uint32_t offset, uint32_t capacity,
                               int max_slot_size) {
    int b = slotcask_bucket_for(capacity, max_slot_size);
    pthread_mutex_lock(&p->pool_lock);
    if (p->free_count[b] == p->free_cap[b]) {
        size_t new_cap = p->free_cap[b] ? p->free_cap[b] * 2 : 4096;
        SlotcaskFreeSlot *na = realloc(p->free_slots[b],
                                       new_cap * sizeof(SlotcaskFreeSlot));
        if (!na) { pthread_mutex_unlock(&p->pool_lock); return -1; }
        p->free_slots[b] = na;
        p->free_cap[b]   = new_cap;
    }
    p->free_slots[b][p->free_count[b]].file_id  = file_id;
    p->free_slots[b][p->free_count[b]].offset   = offset;
    p->free_slots[b][p->free_count[b]].capacity = capacity;
    p->free_count[b]++;
    pthread_mutex_unlock(&p->pool_lock);
    return 0;
}

/* Convenience wrapper for fixed-format (capacity == db->slot_size always). */
static int pool_push_free(SlotcaskStream *p, uint16_t file_id,
                           uint32_t offset, int max_slot_size) {
    return pool_push_free_cap(p, file_id, offset,
                              (uint32_t)max_slot_size, max_slot_size);
}

/* Pop one slot that can fit needed_size bytes. Tries smallest fitting bucket
   first, then larger buckets. Returns 0 and fills *out on success. Returns 1
   if trylock contested, 2 if no fitting slot available. */
static int pool_try_pop_for_size(SlotcaskStream *p, uint32_t needed_size,
                                  int max_slot_size, SlotcaskFreeSlot *out) {
    if (pthread_mutex_trylock(&p->pool_lock) != 0) return 1;
    int start_b = slotcask_bucket_for(needed_size, max_slot_size);
    for (int b = start_b; b < SLOTCASK_POOL_BUCKETS; b++) {
        if (p->free_count[b] == 0) continue;
        p->free_count[b]--;
        *out = p->free_slots[b][p->free_count[b]];
        pthread_mutex_unlock(&p->pool_lock);
        return 0;
    }
    pthread_mutex_unlock(&p->pool_lock);
    return 2;
}

/* Fixed-format compat: pop any slot (all same size). */
static int pool_try_pop_n(SlotcaskStream *p, size_t n, SlotcaskFreeSlot *out) {
    if (n != 1) {
        /* bulk fixed-format path — try bucket 3 (catch-all) */
        if (pthread_mutex_trylock(&p->pool_lock) != 0) return 1;
        size_t total = 0;
        for (int b = 0; b < SLOTCASK_POOL_BUCKETS; b++) total += p->free_count[b];
        if (total < n) { pthread_mutex_unlock(&p->pool_lock); return 2; }
        size_t got = 0;
        for (int b = SLOTCASK_POOL_BUCKETS - 1; b >= 0 && got < n; b--) {
            while (p->free_count[b] > 0 && got < n) {
                p->free_count[b]--;
                out[got++] = p->free_slots[b][p->free_count[b]];
            }
        }
        pthread_mutex_unlock(&p->pool_lock);
        return 0;
    }
    /* n==1: pop from any non-empty bucket */
    if (pthread_mutex_trylock(&p->pool_lock) != 0) return 1;
    for (int b = 0; b < SLOTCASK_POOL_BUCKETS; b++) {
        if (p->free_count[b] == 0) continue;
        p->free_count[b]--;
        out[0] = p->free_slots[b][p->free_count[b]];
        pthread_mutex_unlock(&p->pool_lock);
        return 0;
    }
    pthread_mutex_unlock(&p->pool_lock);
    return 2;
}

/* ============================================================ Append path */

/* Reserve N consecutive fixed-size slots in the active segment of stream `p`.
   Rotates if the active segment is full. Returns 0 on success, -1 on error. */
static int append_reserve_n(SlotcaskDb *db, SlotcaskStream *p,
                            size_t n, uint32_t *file_id_out,
                            uint32_t *offsets_out) {
    pthread_mutex_lock(&p->rotation_lock);
    size_t need = n * (size_t)db->slot_size;
    if (p->reserve_off + need > SLOTCASK_SEG_MAX_BYTES) {
        /* Rotate. */
        p->active_file_id++;
        p->reserve_off = 0;
    }
    *file_id_out = p->active_file_id;
    for (size_t i = 0; i < n; i++) {
        offsets_out[i] = (uint32_t)(p->reserve_off + i * (size_t)db->slot_size);
    }
    p->reserve_off += need;
    pthread_mutex_unlock(&p->rotation_lock);
    return 0;
}

/* Reserve a single variable-length slot. rec_size includes the header
   + key + value + alignment padding. Rotates if not enough space in the
   active segment. Returns 0 on success, -1 on error. */
static int append_reserve_single_varlen(SlotcaskDb *db, SlotcaskStream *p,
                                         size_t rec_size,
                                         uint32_t *file_id_out,
                                         uint32_t *offset_out) {
    (void)db;
    pthread_mutex_lock(&p->rotation_lock);
    if (p->reserve_off + rec_size > SLOTCASK_SEG_MAX_BYTES) {
        p->active_file_id++;
        p->reserve_off = 0;
    }
    *file_id_out = p->active_file_id;
    *offset_out = (uint32_t)(p->reserve_off);
    p->reserve_off += rec_size;
    pthread_mutex_unlock(&p->rotation_lock);
    return 0;
}

/* Compute the on-disk record size (including 8-byte alignment padding)
   for a variable-length record. */
static inline size_t slotcask_record_size_varlen(size_t klen, size_t vlen) {
    size_t raw = 24 + klen + vlen;
    return (raw + 7) & ~(size_t)7;
}



/* ============================================================ Record I/O */


/* Build a slot record in `buf` (caller-allocated, slot_size bytes). */
static void build_record_buf(uint8_t *buf, int slot_size,
                             const uint8_t hash[16], uint8_t flag,
                             const void *key, size_t klen,
                             const void *value, size_t vlen) {
    memset(buf, 0, slot_size);
    memcpy(buf, hash, 16);
    uint16_t k16 = (uint16_t)klen;
    memcpy(buf + 16, &k16, 2);
    buf[18] = flag;
    buf[19] = 0;
    uint32_t v32 = (uint32_t)vlen;
    memcpy(buf + 20, &v32, 4);
    memcpy(buf + 24, key, klen);
    memcpy(buf + 24 + klen, value, vlen);
}

/* Emit one record into a slot at `dst`: 24-B header (hash, klen, flag=0,
   _, vlen) → key bytes → value bytes → zero pad to slot_size → release
   fence → flag=1. Caller owns dst (a slot offset inside an mmap'd seg
   file) and any lock needed against eviction (segcache rdlock or
   vacuum's exclusive). The fence + flag-byte-last ordering is what
   makes a mid-write crash safe — partial bytes stay flag=0 until the
   final store. */
static inline void seg_record_emit(uint8_t *dst, int slot_size,
                                    const uint8_t hash[16],
                                    const void *key, size_t klen,
                                    const void *value, size_t vlen) {
    memcpy(dst, hash, 16);
    uint16_t k16 = (uint16_t)klen;
    memcpy(dst + 16, &k16, 2);
    /* Flag stays 0 until payload is fully in place. Use atomic_store with
       relaxed ordering for the initial 0 — readers that see 0 simply skip
       the record (no acquire needed). */
    __atomic_store_n(&dst[18], 0, __ATOMIC_RELAXED);
    dst[19] = 0;
    uint32_t v32 = (uint32_t)vlen;
    memcpy(dst + 20, &v32, 4);
    memcpy(dst + 24, key, klen);
    memcpy(dst + 24 + klen, value, vlen);
    size_t used = 24 + klen + vlen;
    if (used < (size_t)slot_size) {
        memset(dst + used, 0, (size_t)slot_size - used);
    }
    /* Release-store on the flag commits all the payload writes above.
       Any reader doing acquire-load on flag==1 (seg_rec_live_with_hash)
       will see the full hash/key/value as a coherent snapshot. */
    __atomic_store_n(&dst[18], 1, __ATOMIC_RELEASE);
}

/* Memcpy a complete record (key+value already concatenated) into the segment
   mmap with crash-safe ordering: payload first, fence, flag byte last. */
static int seg_write_record(const SlotcaskDb *db, uint8_t stream_id,
                             uint16_t file_id, uint32_t offset,
                             const uint8_t hash[16],
                             const void *key, size_t klen,
                             const void *value, size_t vlen) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    /* rdlock is sufficient: each caller owns a unique reserved offset
       (via append_reserve_n / pool_try_pop_n), so concurrent writes
       don't race; the segcache rwlock only serialises us against
       eviction, which takes wrlock and waits for all rdlock holders.
       create=1: first writer to a freshly-rotated segment file
       materialises it (open O_CREAT + ftruncate to max). */
    if (segcache_acquire(&h, path, 1, 0) != 0) return -1;
    seg_record_emit(h.map + offset, db->slot_size, hash, key, klen, value, vlen);
    segcache_release(&h);
    return 0;
}

/* Variable-length variant: writes a record without padding to slot_size.
   rec_size must be slotcask_record_size_varlen(klen, vlen). */
static int seg_write_record_varlen(const SlotcaskDb *db, uint8_t stream_id,
                                    uint16_t file_id, uint32_t offset,
                                    const uint8_t hash[16],
                                    const void *key, size_t klen,
                                    const void *value, size_t vlen,
                                    size_t rec_size) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 1, 0) != 0) return -1;
    seg_record_emit(h.map + offset, (int)rec_size, hash, key, klen, value, vlen);
    segcache_release(&h);
    return 0;
}

/* Forward decl — seg_write_flag is defined below this function but
   called from the fixed-format branch. */
static int seg_write_flag(const SlotcaskDb *db, uint8_t stream_id,
                           uint16_t file_id, uint32_t offset, uint8_t flag);

/* Tombstone an old seg slot and return it to its stream pool.
   For variable-length format, reads the record's klen/vlen from the
   segment header to determine the slot's capacity. For fixed format,
   uses db->slot_size. Must be called with a non-const db because it
   modifies the stream's free pool. */
static inline void slotcask_tombstone_and_push_back(SlotcaskDb *db,
                                                     uint8_t stream_id,
                                                     uint16_t file_id,
                                                     uint32_t offset) {
    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, stream_id, file_id);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0) != 0) return;
        __atomic_store_n(&h.map[offset + 18], 2, __ATOMIC_RELEASE);
        uint16_t klen = seg_rec_klen(h.map + offset);
        uint32_t vlen = seg_rec_vlen(h.map + offset);
        uint32_t cap = (uint32_t)slotcask_record_size_varlen(klen, vlen);
        segcache_release(&h);
        pool_push_free_cap(&db->streams[stream_id], file_id, offset,
                           cap, db->slot_size);
    } else {
        seg_write_flag(db, stream_id, file_id, offset, 2);
        pool_push_free(&db->streams[stream_id], file_id, offset, db->slot_size);
    }
}

/* Set the flag byte at slot (file_id, offset) to `flag`. Used for tombstones. */
static int seg_write_flag(const SlotcaskDb *db, uint8_t stream_id,
                          uint16_t file_id, uint32_t offset, uint8_t flag) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    /* rdlock — same reasoning as seg_write_record: single-byte write to
       a unique offset, only need to keep eviction at bay. The target
       file always exists (we're tombstoning a previously-written
       record), so create=0. */
    if (segcache_acquire(&h, path, 0, 0) != 0) return -1;
    /* Release-store: tombstones flip flag 1→2 (deleted). Concurrent
       readers doing acquire-load on the flag byte either still see 1
       (and proceed with the live record) or see 2 (and skip). */
    __atomic_store_n(&h.map[offset + 18], flag, __ATOMIC_RELEASE);
    segcache_release(&h);
    return 0;
}

/* ============================================================ Public CRUD */

int slotcask_insert(SlotcaskDb *db, int stream_id_hint,
                    const void *key, size_t klen,
                    const void *value, size_t vlen) {
    if (klen > UINT16_MAX || vlen > UINT32_MAX) return -1;
    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);

    int sid_data = stream_id_hint;
    if (sid_data < 0 || sid_data >= db->num_streams)
        sid_data = (int)((unsigned)hash[15] % (unsigned)db->num_streams);
    SlotcaskStream *pool = &db->streams[sid_data];

    uint32_t slot_capacity;
    SlotcaskFreeSlot fs;
    uint8_t target_stream = (uint8_t)sid_data;
    uint16_t target_fid;
    uint32_t target_off;
    int got_pool;
    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                           db->slot_size, &fs) == 0);
        if (got_pool) {
            target_fid = fs.file_id;
            target_off = fs.offset;
            slot_capacity = fs.capacity;
        } else {
            size_t rec_size = slotcask_record_size_varlen(klen, vlen);
            uint32_t fid, off;
            if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0)
                return -1;
            target_fid = (uint16_t)fid;
            target_off = off;
            slot_capacity = (uint32_t)rec_size;
        }
    } else {
        slot_capacity = (uint32_t)db->slot_size;
        got_pool = (pool_try_pop_n(pool, 1, &fs) == 0);
        if (got_pool) {
            target_fid = fs.file_id;
            target_off = fs.offset;
        } else {
            uint32_t fid;
            uint32_t off;
            if (append_reserve_n(db, pool, 1, &fid, &off) != 0) return -1;
            target_fid = (uint16_t)fid;
            target_off = off;
        }
    }

    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        if (seg_write_record_varlen(db, target_stream, target_fid, target_off,
                                     hash, key, klen, value, vlen,
                                     slot_capacity) != 0) {
            if (got_pool) pool_push_free_cap(pool, target_fid, target_off,
                                              slot_capacity, db->slot_size);
            return -1;
        }
    } else {
        if (seg_write_record(db, target_stream, target_fid, target_off,
                              hash, key, klen, value, vlen) != 0) {
            if (got_pool) pool_push_free(pool, target_fid, target_off, db->slot_size);
            return -1;
        }
    }

    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) {
        seg_write_flag(db, target_stream, target_fid, target_off, 2);
        if (db->format == SLOTCASK_FORMAT_VARIABLE)
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
        else
            pool_push_free(pool, target_fid, target_off, db->slot_size);
        return -1;
    }
    size_t used_delta = 0;
    int put_rc = kf_put_new(db, &kh, hash, target_stream, target_fid, target_off,
                            key, klen, db->data_dir, &used_delta, NULL);
    kfcache_release(&kh);
    if (put_rc != 0) {
        seg_write_flag(db, target_stream, target_fid, target_off, 2);
        if (db->format == SLOTCASK_FORMAT_VARIABLE)
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
        else
            pool_push_free(pool, target_fid, target_off, db->slot_size);
        return (put_rc == 1) ? -2 : -1;
    }
    return 0;
}

int slotcask_update(SlotcaskDb *db, int stream_id_hint,
                    const void *key, size_t klen,
                    const void *value, size_t vlen) {
    if (klen > UINT16_MAX || vlen > UINT32_MAX) return -1;
    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);

    int sid_data = stream_id_hint;
    if (sid_data < 0 || sid_data >= db->num_streams)
        sid_data = (int)((unsigned)hash[15] % (unsigned)db->num_streams);
    SlotcaskStream *pool = &db->streams[sid_data];
    uint8_t target_stream = (uint8_t)sid_data;

    /* Single kf wrlock window: lookup → reserve → seg_write_record →
       repoint. Pre-2026.05.6 this used two acquire/release cycles
       with the seg write outside; the seg write is a memcpy into
       mmap (segcache rdlock only) so holding the kf wrlock across
       it costs nothing extra for single-conn workloads and only
       serialises writers that happen to route to the same kf shard
       (1/splits probability — ~0.8 % at splits=128). Tombstone of
       the old slot stays OUTSIDE the lock: it touches a different
       segment file and readers tolerate the brief in-between window
       via hash verification on retry. */
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) return -1;

    uint8_t old_flag, old_sid;
    uint16_t old_fid;
    uint32_t old_off;
    size_t kf_slot;
    int lookup_rc = kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                         &old_flag, &old_sid, &old_fid, &old_off,
                                         &kf_slot);
    if (lookup_rc < 0) {
        kfcache_release(&kh);
        return -1;
    }

    uint32_t slot_capacity;
    SlotcaskFreeSlot fs;
    uint16_t target_fid;
    uint32_t target_off;
    int got_pool;
    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                           db->slot_size, &fs) == 0);
        if (got_pool) {
            target_fid = fs.file_id;
            target_off = fs.offset;
            slot_capacity = fs.capacity;
        } else {
            size_t rec_size = slotcask_record_size_varlen(klen, vlen);
            uint32_t fid, off;
            if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0) {
                kfcache_release(&kh);
                return -1;
            }
            target_fid = (uint16_t)fid;
            target_off = off;
            slot_capacity = (uint32_t)rec_size;
        }
    } else {
        slot_capacity = (uint32_t)db->slot_size;
        got_pool = (pool_try_pop_n(pool, 1, &fs) == 0);
        if (got_pool) {
            target_fid = fs.file_id;
            target_off = fs.offset;
        } else {
            uint32_t fid, off;
            if (append_reserve_n(db, pool, 1, &fid, &off) != 0) {
                kfcache_release(&kh);
                return -1;
            }
            target_fid = (uint16_t)fid;
            target_off = off;
        }
    }

    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        if (seg_write_record_varlen(db, target_stream, target_fid, target_off,
                                     hash, key, klen, value, vlen,
                                     slot_capacity) != 0) {
            kfcache_release(&kh);
            if (got_pool) pool_push_free_cap(pool, target_fid, target_off,
                                              slot_capacity, db->slot_size);
            return -1;
        }
    } else {
        if (seg_write_record(db, target_stream, target_fid, target_off,
                              hash, key, klen, value, vlen) != 0) {
            kfcache_release(&kh);
            if (got_pool) pool_push_free(pool, target_fid, target_off, db->slot_size);
            return -1;
        }
    }

    /* Repoint at the slot we already probed — same wrlock window, so the
       slot index from kf_lookup_with_slot is still authoritative. Skips
       a second probe + a second verify_stored_key seg read. */
    kf_repoint_at_slot(&kh, kf_slot, target_stream, target_fid, target_off);
    kfcache_release(&kh);

    /* Tombstone old slot + return it to its stream pool — unlocked.
       For varlen, reads the old record's header to determine capacity. */
    slotcask_tombstone_and_push_back(db, old_sid, old_fid, old_off);
    return 0;
}

int slotcask_delete(SlotcaskDb *db,
                    const void *key, size_t klen) {
    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);

    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) return -1;

    /* lookup_with_slot + tombstone_at_slot avoids the second hash-chain
       probe + verify_stored_key seg-read that the original kf_tombstone
       performs. Same wrlock window holds, so the captured slot stays
       authoritative. */
    uint8_t old_flag, old_sid;
    uint16_t old_fid;
    uint32_t old_off;
    size_t kf_slot;
    int found = (kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                      &old_flag, &old_sid, &old_fid, &old_off,
                                      &kf_slot) == 0);
    if (!found) {
        kfcache_release(&kh);
        return -1;
    }
    kf_tombstone_at_slot(&kh, kf_slot);
    kfcache_release(&kh);

    slotcask_tombstone_and_push_back(db, old_sid, old_fid, old_off);
    return 0;
}

#define SLOTCASK_GET_MAX_RETRIES 4

int slotcask_get(SlotcaskDb *db,
                 const void *key, size_t klen,
                 void **val_out, size_t *vlen_out) {
    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[sid_kf] : NULL;

    for (int attempt = 0; attempt < SLOTCASK_GET_MAX_RETRIES; attempt++) {
        SlotcaskKfHandle kh;
        if (kfcache_acquire_direct(&kh, kf_ref, kf_path,
                                    db->slots_per_shard, db, sid_kf) != 0) return -1;
        uint8_t flag, stream_id;
        uint16_t file_id;
        uint32_t offset;
        int rc = kf_lookup(&kh, hash, key, klen, db->data_dir,
                           &flag, &stream_id, &file_id, &offset);
        kfcache_release(&kh);
        if (rc < 0) return -1;

        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, stream_id, file_id);
        SlotcaskSegHandle sh;
        SlotRef *seg_ref = seg_ref_for(db, stream_id, file_id);
        if (segcache_acquire_direct(&sh, seg_ref, path) != 0) return -1;

        const uint8_t *rec = sh.map + offset;
        if (!seg_rec_live_with_hash(rec, hash)) {
            segcache_release(&sh);
            continue;
        }
        uint16_t k_stored = seg_rec_klen(rec);
        uint32_t v_stored = seg_rec_vlen(rec);
        if (k_stored != klen || memcmp(rec + 24, key, klen) != 0) {
            segcache_release(&sh);
            continue;
        }
        void *vbuf = malloc(v_stored ? v_stored : 1);
        if (!vbuf) { segcache_release(&sh); return -1; }
        if (v_stored) memcpy(vbuf, rec + 24 + klen, v_stored);
        segcache_release(&sh);
        *val_out = vbuf;
        *vlen_out = v_stored;
        return 0;
    }
    return -1;
}

/* ============================================================ Bulk update */

typedef struct {
    size_t   orig_idx;
    uint8_t  hash[16];
    uint8_t  old_sid;
    uint16_t old_fid;
    uint32_t old_off;
    uint8_t  target_sid;
    SlotcaskFreeSlot target;
    int      old_found;
} BulkInfo;

int slotcask_bulk_update(SlotcaskDb *db, const SlotcaskRecord *recs, size_t n) {
    if (n == 0) return 0;
    BulkInfo *infos = calloc(n, sizeof(BulkInfo));
    if (!infos) return -1;

    /* 1. Hash + lookup old slot for each record. */
    for (size_t i = 0; i < n; i++) {
        infos[i].orig_idx = i;
        compute_hash(recs[i].key, recs[i].klen, infos[i].hash);
        infos[i].target_sid = (uint8_t)((unsigned)infos[i].hash[15] %
                                        (unsigned)db->num_streams);
        int sid_kf = shard_for_hash(infos[i].hash, db->num_shards);
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, sid_kf);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) {
            free(infos); return -1;
        }
        uint8_t f;
        int rc = kf_lookup(&kh, infos[i].hash, recs[i].key, recs[i].klen,
                           db->data_dir, &f, &infos[i].old_sid,
                           &infos[i].old_fid, &infos[i].old_off);
        kfcache_release(&kh);
        infos[i].old_found = (rc == 0);
        if (!infos[i].old_found) { free(infos); return -1; }
    }

    /* 2. Reserve destination slots. */
    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        /* Varlen: each record has a different size, so reserve per-record. */
        for (size_t i = 0; i < n; i++) {
            int s = infos[i].target_sid;
            size_t klen = recs[i].klen, vlen = recs[i].vlen;
            size_t rec_size = slotcask_record_size_varlen(klen, vlen);
            SlotcaskFreeSlot fs;
            if (pool_try_pop_for_size(&db->streams[s], (uint32_t)(24 + klen + vlen),
                                      db->slot_size, &fs) == 0) {
                infos[i].target = fs;
            } else {
                uint32_t fid, off;
                if (append_reserve_single_varlen(db, &db->streams[s],
                                                 rec_size, &fid, &off) != 0) {
                    free(infos); return -1;
                }
                infos[i].target.file_id  = (uint16_t)fid;
                infos[i].target.offset   = off;
                infos[i].target.capacity = (uint32_t)rec_size;
            }
        }
    } else {
        /* Fixed: all slots are slot_size bytes; batch reserve per stream. */
        size_t per_stream[SLOTCASK_MAX_STREAMS] = {0};
        for (size_t i = 0; i < n; i++) per_stream[infos[i].target_sid]++;

        for (int s = 0; s < db->num_streams; s++) {
            size_t sub_n = per_stream[s];
            if (sub_n == 0) continue;
            SlotcaskFreeSlot *targets = malloc(sub_n * sizeof(SlotcaskFreeSlot));
            if (!targets) { free(infos); return -1; }
            int from_pool = (pool_try_pop_n(&db->streams[s], sub_n, targets) == 0);
            if (!from_pool) {
                uint32_t *offsets = malloc(sub_n * sizeof(uint32_t));
                uint32_t fid;
                if (!offsets || append_reserve_n(db, &db->streams[s], sub_n,
                                                 &fid, offsets) != 0) {
                    free(offsets); free(targets); free(infos); return -1;
                }
                for (size_t j = 0; j < sub_n; j++) {
                    targets[j].file_id = (uint16_t)fid;
                    targets[j].offset = offsets[j];
                }
                free(offsets);
            }
            size_t tgt_idx = 0;
            for (size_t i = 0; i < n; i++) {
                if (infos[i].target_sid != s) continue;
                infos[i].target = targets[tgt_idx++];
            }
            free(targets);
        }
    }

    /* 3. Write + repoint + tombstone. */
    for (size_t i = 0; i < n; i++) {
        int write_rc;
        if (db->format == SLOTCASK_FORMAT_VARIABLE) {
            size_t rec_size = slotcask_record_size_varlen(recs[i].klen, recs[i].vlen);
            write_rc = seg_write_record_varlen(db, infos[i].target_sid,
                                               infos[i].target.file_id,
                                               infos[i].target.offset,
                                               infos[i].hash,
                                               recs[i].key, recs[i].klen,
                                               recs[i].value, recs[i].vlen,
                                               (uint32_t)rec_size);
        } else {
            write_rc = seg_write_record(db, infos[i].target_sid,
                                        infos[i].target.file_id,
                                        infos[i].target.offset,
                                        infos[i].hash,
                                        recs[i].key, recs[i].klen,
                                        recs[i].value, recs[i].vlen);
        }
        if (write_rc != 0) { free(infos); return -1; }
        int sid_kf = shard_for_hash(infos[i].hash, db->num_shards);
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, sid_kf);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) {
            free(infos); return -1;
        }
        kf_repoint(&kh, infos[i].hash, infos[i].target_sid,
                   infos[i].target.file_id, infos[i].target.offset,
                   recs[i].key, recs[i].klen, db->data_dir);
        kfcache_release(&kh);
        slotcask_tombstone_and_push_back(db, infos[i].old_sid,
                                          infos[i].old_fid, infos[i].old_off);
    }

    free(infos);
    return 0;
}

/* ============================================================ Open / close */

static void dirty_marker_path(const SlotcaskDb *db, char out[PATH_MAX]) {
    snprintf(out, PATH_MAX, "%s/.dirty", db->data_dir);
}
static int touch_dirty_marker(const SlotcaskDb *db) {
    char p[PATH_MAX]; dirty_marker_path(db, p);
    int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    close(fd); return 0;
}
static int dirty_marker_exists(const SlotcaskDb *db) {
    char p[PATH_MAX]; dirty_marker_path(db, p);
    struct stat st; return stat(p, &st) == 0;
}
static int remove_dirty_marker(const SlotcaskDb *db) {
    char p[PATH_MAX]; dirty_marker_path(db, p);
    if (unlink(p) != 0 && errno != ENOENT) return -1;
    return 0;
}

/* ---- format marker (.format) ---- */
static void format_file_path(const SlotcaskDb *db, char out[PATH_MAX]) {
    snprintf(out, PATH_MAX, "%s/.format", db->data_dir);
}
static int read_format_marker(const SlotcaskDb *db) {
    char p[PATH_MAX]; format_file_path(db, p);
    int fd = open(p, O_RDONLY);
    if (fd < 0) return -1;
    char buf[2] = {0};
    int n = (int)read(fd, buf, 1);
    close(fd);
    if (n != 1) return -1;
    if (buf[0] == '0') return SLOTCASK_FORMAT_FIXED;
    if (buf[0] == '1') return SLOTCASK_FORMAT_VARIABLE;
    return -1;
}
static int write_format_marker(const SlotcaskDb *db, int fmt) {
    char p[PATH_MAX]; format_file_path(db, p);
    int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    char c = (fmt == SLOTCASK_FORMAT_VARIABLE) ? '1' : '0';
    int rc = (write(fd, &c, 1) == 1) ? 0 : -1;
    close(fd);
    return rc;
}

static int data_file_id_from_name(const char *name) {
    /* Segment files: <data_dir>/data/streams/SSS/NNNNNN.dat */
    int id;
    return (sscanf(name, "%d.dat", &id) == 1) ? id : -1;
}
static int cmp_int(const void *a, const void *b) {
    int ia = *(const int *)a, ib = *(const int *)b;
    return (ia > ib) - (ia < ib);
}

/* Per-stream recovery worker arg. Used by recover_streams to fan out
   independent per-stream I/O via parallel_for_io. */
typedef struct {
    SlotcaskDb *db;
    int         sid;
    int         rc;  /* 0 = success, -1 = error */
} RecoverStreamArg;

/* Compute a buffer size that is a multiple of both ODIRECT_ALIGN and
   slot_size. This guarantees every chunk holds an integer number of slots
   so the scan loop needs no carry-over state.
   slot_size is always a multiple of 8 (floor 32) so gcd(4096, slot_size)
   >= 8 and the computed lcm is always manageable (< 8 MB for any valid
   slot_size). */
static size_t recover_od_buf_size(int slot_size) {
    size_t a = (size_t)ODIRECT_ALIGN;
    size_t b = (size_t)slot_size;
    size_t x = a, y = b;
    while (y) { size_t t = x % y; x = y; y = t; }  /* x = gcd(a,b) */
    size_t lcm = a / x * b;
    /* Scale up to ~4 MB so we amortise syscall overhead across many slots. */
    size_t n = (ODIRECT_BUF_SIZE_DEFAULT + lcm - 1) / lcm;
    if (n < 1) n = 1;
    return lcm * n;
}

/* O_DIRECT scan of a non-active segment file for tombstoned (flag==2) slots.
   Pages never enter the page cache. Called for every file_id != last_id
   in recover_one_stream. Returns 0 on success; errors are non-fatal
   (missed tombstones just reduce free-pool size until next vacuum). */
static int recover_scan_tombstones_od(SlotcaskDb *db, int sid,
                                       int file_id, const char *path) {
    int fd = od_open(path);
    if (fd < 0) return -1;

    size_t buf_size = recover_od_buf_size(db->slot_size);
    uint8_t *buf = aligned_alloc(ODIRECT_ALIGN, buf_size);
    if (!buf) { close(fd); return -1; }

    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        /* Varlen: records are not at fixed stride, so use a carry buffer to
           handle records that straddle O_DIRECT chunk boundaries. */
        size_t carry_cap = 256u * 1024u;
        uint8_t *carry = malloc(carry_cap);
        if (!carry) { free(buf); close(fd); return -1; }
        int carry_len = 0;
        uint32_t carry_off = 0;
        off_t file_off2 = 0;
        for (;;) {
            ssize_t nr = od_pread(fd, buf, buf_size, file_off2);
            if (nr <= 0) break;
            size_t pos = 0;
            if (carry_len > 0) {
                if (carry_len < 24) {
                    int need = 24 - carry_len;
                    if ((ssize_t)need > nr) {
                        if ((size_t)(carry_len + nr) > carry_cap) {
                            carry_cap = (size_t)(carry_len + nr);
                            uint8_t *nc = realloc(carry, carry_cap);
                            if (!nc) { free(carry); free(buf); close(fd); return -1; }
                            carry = nc;
                        }
                        memcpy(carry + carry_len, buf, (size_t)nr);
                        carry_len += (int)nr;
                        file_off2 += nr; continue;
                    }
                    memcpy(carry + carry_len, buf, (size_t)need);
                    pos += (size_t)need; carry_len = 24;
                }
                uint16_t klen; memcpy(&klen, carry + 16, 2);
                uint32_t vlen; memcpy(&vlen, carry + 20, 4);
                size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
                int need2 = (int)rec_size - carry_len;
                if (need2 > 0) {
                    if ((ssize_t)need2 > nr) {
                        if ((size_t)(carry_len + nr) > carry_cap) {
                            carry_cap = (size_t)(carry_len + nr);
                            uint8_t *nc = realloc(carry, carry_cap);
                            if (!nc) { free(carry); free(buf); close(fd); return -1; }
                            carry = nc;
                        }
                        memcpy(carry + carry_len, buf, (size_t)nr);
                        carry_len += (int)nr;
                        file_off2 += nr; continue;
                    }
                    if (rec_size > carry_cap) {
                        carry_cap = rec_size;
                        uint8_t *nc = realloc(carry, carry_cap);
                        if (!nc) { free(carry); free(buf); close(fd); return -1; }
                        carry = nc;
                    }
                    memcpy(carry + carry_len, buf, (size_t)need2);
                    pos += (size_t)need2; carry_len = (int)rec_size;
                }
                if (carry[18] == 2)
                    pool_push_free_cap(&db->streams[sid], (uint16_t)file_id,
                                       carry_off, (uint32_t)rec_size, db->slot_size);
                carry_len = 0;
            }
            while (pos + 24 <= (size_t)nr) {
                uint8_t *rec = buf + pos;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                uint32_t vlen; memcpy(&vlen, rec + 20, 4);
                size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
                if (pos + rec_size > (size_t)nr) break;
                if (rec[18] == 2)
                    pool_push_free_cap(&db->streams[sid], (uint16_t)file_id,
                                       (uint32_t)(file_off2 + (off_t)pos),
                                       (uint32_t)rec_size, db->slot_size);
                pos += rec_size;
            }
            if (pos < (size_t)nr) {
                size_t tail = (size_t)nr - pos;
                if (tail > carry_cap) {
                    carry_cap = tail;
                    uint8_t *nc = realloc(carry, carry_cap);
                    if (!nc) { free(carry); free(buf); close(fd); return -1; }
                    carry = nc;
                }
                carry_len = (int)tail;
                carry_off = (uint32_t)(file_off2 + (off_t)pos);
                memcpy(carry, buf + pos, tail);
            }
            file_off2 += (off_t)nr;
            if (nr < (ssize_t)buf_size) break;
        }
        free(carry);
        free(buf);
        close(fd);
        return 0;
    }

    int slot_size = db->slot_size;
    off_t file_off = 0;
    for (;;) {
        ssize_t nr = od_pread(fd, buf, buf_size, file_off);
        if (nr <= 0) break;
        /* buf_size is a multiple of slot_size, so nr is also a multiple
           (or a short final read whose incomplete trailing bytes are safely
           skipped by the <= condition). */
        for (ssize_t off = 0; off + slot_size <= nr; off += slot_size) {
            if (buf[off + 18] == 2) {
                pool_push_free(&db->streams[sid], (uint16_t)file_id,
                               (uint32_t)(file_off + off), db->slot_size);
            }
        }
        file_off += (off_t)nr;
        if (nr < (ssize_t)buf_size) break; /* EOF */
    }

    free(buf);
    close(fd);
    return 0;
}

/* Walk every segment for a single stream, populate the in-memory free-slot
   pool from flag=2 slots, and position reserve_off past the last live slot
   in the highest-numbered segment. Returns 0 on success, -1 on error. */
static int recover_one_stream(SlotcaskDb *db, int sid) {
    char dir[PATH_MAX];
    stream_dir_for(dir, db->data_dir, sid);
    DIR *d = opendir(dir);
    if (!d) {
        if (errno == ENOENT) return 0;
        return -1;
    }
    int *ids = NULL; size_t n_ids = 0, cap_ids = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        int id = data_file_id_from_name(de->d_name);
        if (id < 0) continue;
        if (n_ids == cap_ids) {
            cap_ids = cap_ids ? cap_ids * 2 : 64;
            ids = realloc(ids, cap_ids * sizeof(int));
            if (!ids) { closedir(d); return -1; }
        }
        ids[n_ids++] = id;
    }
    closedir(d);
    if (n_ids == 0) { free(ids); return 0; }
    qsort(ids, n_ids, sizeof(int), cmp_int);

    int last_id = ids[n_ids - 1];
    off_t last_offset = 0;

    for (size_t fi = 0; fi < n_ids; fi++) {
        int file_id = ids[fi];
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, sid, (uint32_t)file_id);

        if (file_id != last_id) {
            /* Non-active segment: O_DIRECT scan for tombstones only.
               Read-once at startup, never written — pages must not enter
               the page cache and displace KF/index pages loaded by warmup. */
            recover_scan_tombstones_od(db, sid, file_id, path);
            continue;
        }

        /* Active (last) segment: mmap via segcache so the first post-startup
           insert doesn't take a cold segcache miss. Also scans tombstones
           and locates the reserve frontier. */
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0) != 0) { free(ids); return -1; }
        off_t pos = 0;
        off_t lim = (off_t)h.map_size;
        if (db->format == SLOTCASK_FORMAT_VARIABLE) {
            while (pos + 24 <= lim) {
                uint8_t flag = __atomic_load_n(&h.map[pos + 18], __ATOMIC_ACQUIRE);
                if (flag == 0) break;
                uint16_t klen; memcpy(&klen, h.map + pos + 16, 2);
                uint32_t vlen; memcpy(&vlen, h.map + pos + 20, 4);
                size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
                if (flag == 2)
                    pool_push_free_cap(&db->streams[sid], (uint16_t)file_id,
                                       (uint32_t)pos, (uint32_t)rec_size, db->slot_size);
                pos += (off_t)rec_size;
            }
        } else {
            while (pos + db->slot_size <= lim) {
                uint8_t flag = __atomic_load_n(&h.map[pos + 18], __ATOMIC_ACQUIRE);
                if (flag == 2) {
                    pool_push_free(&db->streams[sid], (uint16_t)file_id,
                                   (uint32_t)pos, db->slot_size);
                } else if (flag == 0) {
                    break;
                }
                pos += db->slot_size;
            }
        }
        last_offset = pos;
        segcache_release(&h);
    }
    db->streams[sid].active_file_id = (uint32_t)last_id;
    db->streams[sid].reserve_off = (uint64_t)last_offset;
    free(ids);
    return 0;
}

static void *recover_stream_worker(void *raw) {
    RecoverStreamArg *a = (RecoverStreamArg *)raw;
    a->rc = recover_one_stream(a->db, a->sid);
    return NULL;
}

/* Walk every segment for every stream, populate the in-memory free-slot pool
   from flag=2 slots, and position each stream's reserve_off past the last
   live slot in the highest-numbered segment.
   Streams are independent — dispatched in parallel via parallel_for_io. */
static int recover_streams(SlotcaskDb *db) {
    if (db->num_streams <= 1) {
        if (db->num_streams == 1) {
            return recover_one_stream(db, 0);
        }
        return 0;
    }

    RecoverStreamArg *args = calloc((size_t)db->num_streams, sizeof(RecoverStreamArg));
    if (!args) return -1;
    for (int i = 0; i < db->num_streams; i++) {
        args[i].db = db;
        args[i].sid = i;
        args[i].rc = 0;
    }
    parallel_for_io(recover_stream_worker, args, db->num_streams, sizeof(RecoverStreamArg));
    for (int i = 0; i < db->num_streams; i++) {
        if (args[i].rc != 0) { free(args); return -1; }
    }
    free(args);
    return 0;
}

int slotcask_open(SlotcaskDb *db, const char *data_dir,
                  int num_shards, int num_streams, int slot_size) {
    memset(db, 0, sizeof(*db));
    if (num_shards < 1 || num_shards > SLOTCASK_MAX_SHARDS) return -1;
    if (num_streams < 1 || num_streams > SLOTCASK_MAX_STREAMS) return -1;
    if (slot_size < 32) return -1;
    snprintf(db->data_dir, sizeof(db->data_dir), "%s", data_dir);
    db->num_shards = num_shards;
    db->num_streams = num_streams;
    db->slot_size = slot_size;
    db->format = SLOTCASK_FORMAT_FIXED;
    db->slots_per_shard = slotcask_default_slots_for_splits(num_shards);

    if (mkdirp_local(data_dir) != 0) return -1;

    /* Detect or persist the segment format marker (.format).
       If the marker already exists, its value is authoritative.
       If not, write one with the default (FIXED) so future opens
       read the correct value.  This ensures existing fixed-format
       databases are automatically annotated on first open after
       upgrade. */
    {
        int fmt = read_format_marker(db);
        if (fmt >= 0) {
            db->format = fmt;
        } else {
            /* No marker yet — write one now so subsequent opens
               don't need to guess. */
            if (write_format_marker(db, SLOTCASK_FORMAT_FIXED) != 0) {
                /* Non-fatal — we'll try again on next open. */
            }
        }
    }

    db->streams = calloc(num_streams, sizeof(SlotcaskStream));
    if (!db->streams) return -1;
    for (int i = 0; i < num_streams; i++) {
        SlotcaskStream *s = &db->streams[i];
        s->stream_id = i;
        stream_dir_for(s->stream_dir, data_dir, i);
        if (mkdirp_local(s->stream_dir) != 0) goto fail;
        pthread_mutex_init(&s->rotation_lock, NULL);
        pthread_mutex_init(&s->pool_lock, NULL);
        s->active_file_id = 0;
        s->reserve_off = 0;
        /* free_slots[b], free_count[b], free_cap[b] zeroed by calloc */
    }

    /* Eagerly create file_000 in each stream so the first append doesn't
       race the create path through the cache. */
    for (int i = 0; i < num_streams; i++) {
        char path[PATH_MAX];
        seg_path_for(path, data_dir, i, 0);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 1, 1) != 0) goto fail;
        /* Prime the seg slot ref for file_id 0 so point reads hit the
           fast path immediately after open. */
        if (db->seg_slot_refs && h.slot >= 0) {
            SlotRef *ref = seg_ref_for(db, i, 0);
            if (ref) {
                ref->slot = h.slot;
                ref->gen  = atomic_load_explicit(&g_segcache[h.slot].gen,
                                                  memory_order_acquire);
            }
        }
        segcache_release(&h);
    }

    /* Eagerly materialize every keyfile shard on disk. Mirrors the prototype's
       pb_open behavior (shard_init opens + ftruncates each kf upfront). Costs
       splits * 12 MB of sparse-file address space but gives operators a fully
       formed on-disk shape that's safe to inspect with `ls`/`du` immediately
       after create-object. The mmaps drop out of memory under cache pressure
       just like any other kfcache entry.
       Also clean up any leftover .new staging files from a prior crashed
       resplit — kf.new is only valid mid-rebuild, never persistent.
       Parallelised: at splits=256 on slow shared CI disks the serial
       open + ftruncate + mmap + madvise(HUGEPAGE) loop ran 30+ s and
       blocked every concurrent registry_get behind g_reg_lock. Per-shard
       work is independent (distinct kf paths); kfcache's internal mutex
       still serialises the actual cache-table install. */
    SlotcaskOpenArg *open_args = calloc((size_t)num_shards, sizeof(SlotcaskOpenArg));
    if (!open_args) goto fail;
    for (int i = 0; i < num_shards; i++) {
        open_args[i].db = db;
        open_args[i].shard_id = i;
    }
    parallel_for_io(slotcask_open_kf_worker, open_args, num_shards,
                     sizeof(SlotcaskOpenArg));
    for (int i = 0; i < num_shards; i++) {
        if (!open_args[i].ok) { free(open_args); goto fail; }
    }

    /* Populate per-shard kf slot refs so the hot read path can skip the
       table mutex on cache hits. The kfcache already has all shards
       installed from the parallel init above; we re-acquire each one as
       a reader (rdlock, no table mutation) solely to record (slot, gen). */
    db->kf_slot_refs = calloc((size_t)num_shards, sizeof(SlotRef));
    if (!db->kf_slot_refs) { free(open_args); goto fail; }
    for (int i = 0; i < num_shards; i++) db->kf_slot_refs[i].slot = -1;
    for (int i = 0; i < num_shards; i++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, data_dir, i);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) == 0) {
            if (kh.slot >= 0) {
                db->kf_slot_refs[i].slot = kh.slot;
                db->kf_slot_refs[i].gen  =
                    atomic_load_explicit(&g_kfcache[kh.slot].gen,
                                         memory_order_acquire);
            }
            kfcache_release(&kh);
        }
    }

    /* Allocate per-stream segment slot ref arrays (all start NULL / cap 0). */
    db->seg_slot_refs = calloc((size_t)num_streams, sizeof(SlotRef *));
    db->seg_slot_caps = calloc((size_t)num_streams, sizeof(int));
    if (!db->seg_slot_refs || !db->seg_slot_caps) { free(open_args); goto fail; }

    /* Always run recover_streams — reserve_off / active_file_id aren't
       persisted, so a clean close + reopen would otherwise leave them
       at 0 and the next write would clobber a live record at the head
       of the active segment. No-op when the directory is empty. */
    (void)dirty_marker_exists;  /* silence unused warning */
    if (recover_streams(db) != 0) { free(open_args); goto fail; }
    if (touch_dirty_marker(db) != 0) {
        /* Non-fatal — recovery will simply re-walk on the next open. */
    }

    /* Rebuild the per-stream free-slot pool from kf state. The pool is
       in-memory only — every daemon start would otherwise lose track of
       tombstoned seg slots, leaving them unreusable until the next
       vacuum compaction. Walking each kf shard's flag=2 entries and
       pushing (file_id, offset) to the right stream's pool restores the
       snake-game property across restarts.
       Skip shards with hdr->deleted == 0 — clean kf shards have no
       tombstones to harvest, so the O(slots_per_shard) walk is wasted.
       Parallelise the rest using the same per-shard arg array as the
       open loop above. */
    int needs_pool_count = 0;
    for (int s = 0; s < num_shards; s++) {
        if (open_args[s].needs_pool) needs_pool_count++;
    }
    if (needs_pool_count > 0) {
        SlotcaskOpenArg *pool_args = calloc((size_t)needs_pool_count,
                                              sizeof(SlotcaskOpenArg));
        if (!pool_args) { free(open_args); goto fail; }
        int p = 0;
        for (int s = 0; s < num_shards; s++) {
            if (!open_args[s].needs_pool) continue;
            pool_args[p].db = db;
            pool_args[p].shard_id = s;
            p++;
        }
        parallel_for_io(slotcask_pool_rebuild_worker, pool_args,
                         needs_pool_count, sizeof(SlotcaskOpenArg));
        free(pool_args);
    }
    free(open_args);

    return 0;

fail:
    if (db->streams) {
        for (int i = 0; i < num_streams; i++) {
            pthread_mutex_destroy(&db->streams[i].rotation_lock);
            pthread_mutex_destroy(&db->streams[i].pool_lock);
            for (int _b = 0; _b < SLOTCASK_POOL_BUCKETS; _b++)
                free(db->streams[i].free_slots[_b]);
        }
        free(db->streams);
        db->streams = NULL;
    }
    return -1;
}

void slotcask_close(SlotcaskDb *db) {
    if (db->streams) {
        for (int i = 0; i < db->num_streams; i++) {
            pthread_mutex_destroy(&db->streams[i].rotation_lock);
            pthread_mutex_destroy(&db->streams[i].pool_lock);
            for (int _b = 0; _b < SLOTCASK_POOL_BUCKETS; _b++)
                free(db->streams[i].free_slots[_b]);
        }
        free(db->streams);
    }
    free(db->kf_slot_refs);
    if (db->seg_slot_refs) {
        for (int i = 0; i < db->num_streams; i++)
            free(db->seg_slot_refs[i]);
        free(db->seg_slot_refs);
    }
    free(db->seg_slot_caps);
    remove_dirty_marker(db);
    memset(db, 0, sizeof(*db));
}

/* Sum the per-shard kf headers into total/deleted. Single source of truth
   for record counts — kf header is updated atomically inside slotcask_put /
   slotcask_delete under the kf shard's wrlock, so it cannot go stale across
   ungraceful shutdowns the way a separate text counts file can.

   Reads the 24-byte header of each shard via direct pread() rather than
   kfcache_acquire to avoid evicting hot shards from the kf cache when an
   object has more shards than FCACHE_MAX/4 (e.g. splits=4096 against a
   1024-entry cache would churn). The header is updated under the kf
   wrlock and is a power-of-2 aligned 8-byte field — pread is not torn at
   this granularity on x86_64. Worst case we read a slightly stale header
   (off by an in-flight insert) which is fine for sizing decisions.

   Cost at splits=4096: 4096 × pread(24B) ≈ 1-3ms cold, sub-ms warm.
   Returns 0 on success. */
int slotcask_sum_kf_totals(SlotcaskDb *db,
                           uint64_t *out_total, uint64_t *out_deleted) {
    if (out_total) *out_total = 0;
    if (out_deleted) *out_deleted = 0;
    if (!db || db->num_shards <= 0) return -1;
    uint64_t total = 0, deleted = 0;
    for (int s = 0; s < db->num_shards; s++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, s);
        /* Intentionally unsynchronized open + pread, NOT kfcache_acquire.
           The cache path takes the per-shard rwlock which serialises
           against the writer that every insert/update/delete holds —
           under heavy concurrent write load (stress test, real OLTP) a
           bare-count probe would block waiting for every in-flight
           writer on every shard.  The header read here is 24 B at byte
           0; on x86_64 the total/deleted u64s are aligned so we tolerate
           a single-update tear (worst case we read a sizing decision
           off by one record).  The original lazy-cold cost (256 open+
           pread+close on cold disk = ~10 s) is now amortised by warmup
           populating the OS page cache for these kf headers — see
           warmup_object_via_caches in server.c.  Warm-cache cost here
           is ~50 µs per shard. */
        int fd = open(kf_path, O_RDONLY);
        if (fd < 0) continue;
        SlotcaskKfHeader hdr;
        ssize_t n = pread(fd, &hdr, sizeof(hdr), 0);
        close(fd);
        if (n != (ssize_t)sizeof(hdr)) continue;
        if (hdr.magic != SLOTCASK_KF_MAGIC) continue;
        total   += hdr.total;
        deleted += hdr.deleted;
    }
    if (out_total)   *out_total   = total;
    if (out_deleted) *out_deleted = deleted;
    return 0;
}

/* TEST-ONLY: write synthetic counters into a kf shard's header. Used by
   the resplit stress test to trip the 75 % trigger without inserting
   millions of records. Acquires the kf wrlock briefly, mutates the
   header's `total` and `deleted` fields, releases. NOT for production. */
int slotcask_test_set_kf_total(SlotcaskDb *db, int shard_id,
                               uint64_t total, uint64_t deleted) {
    if (!db || shard_id < 0 || shard_id >= db->num_shards) return -1;
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) return -1;
    if (kh.hdr) {
        kh.hdr->total = total;
        kh.hdr->deleted = deleted;
    }
    kfcache_release(&kh);
    return 0;
}

/* ============================================================ CAS hooks
 *
 * Atomic upsert / delete with caller-supplied check + pre-commit callbacks.
 * Held under the kf-shard wrlock for the whole CAS path. Slow under
 * contention but correctness-simple. The plain insert/update/delete paths
 * above stay fast for the non-CAS bulk write workloads.
 */

/* Read a record's value bytes into a malloc'd buffer. Returns 0 ok, -1 on
   error. *out_buf is malloc'd (caller frees) even when vlen==0 (1-byte
   sentinel) so callers can free unconditionally. */
static int read_record_value(const SlotcaskDb *db, uint8_t stream_id,
                              uint16_t file_id, uint32_t offset,
                              const void *key, size_t klen,
                              uint8_t **out_buf, size_t *out_vlen) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0) != 0) return -1;
    const uint8_t *rec = h.map + offset;
    if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) != 1) { segcache_release(&h); return -1; }
    uint16_t k_stored = seg_rec_klen(rec);
    uint32_t v_stored = seg_rec_vlen(rec);
    if (k_stored != klen || memcmp(rec + 24, key, klen) != 0) {
        segcache_release(&h);
        return -1;
    }
    uint8_t *buf = malloc(v_stored ? v_stored : 1);
    if (!buf) { segcache_release(&h); return -1; }
    if (v_stored) memcpy(buf, rec + 24 + klen, v_stored);
    segcache_release(&h);
    *out_buf = buf;
    *out_vlen = v_stored;
    return 0;
}

int slotcask_exists(SlotcaskDb *db, const void *key, size_t klen) {
    if (klen > UINT16_MAX) return -1;
    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[sid_kf] : NULL;
    if (kfcache_acquire_direct(&kh, kf_ref, kf_path,
                                db->slots_per_shard, db, sid_kf) != 0) return -1;
    uint8_t flag, sid;
    uint16_t fid;
    uint32_t off;
    int rc = kf_lookup(&kh, hash, key, klen, db->data_dir,
                       &flag, &sid, &fid, &off);
    kfcache_release(&kh);
    return (rc == 0) ? 1 : 0;
}

/* Slow upsert path: lookup → check → reserve → seg-write → pre_commit →
   kf commit. Required when the caller needs OLD before deciding to commit
   (require_existing or check_needs_old) — the fast path can't satisfy CAS
   semantics that depend on old. */
static int upsert_slow_path(SlotcaskDb *db, int stream_id_hint,
                             const void *key, size_t klen,
                             const void *value, size_t vlen,
                             const SlotcaskUpsertOpts *opts,
                             SlotcaskUpsertResult *result,
                             const uint8_t hash[16], int sid_kf);

int slotcask_upsert_with_hooks(SlotcaskDb *db, int stream_id_hint,
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                const SlotcaskUpsertOpts *opts,
                                SlotcaskUpsertResult *result) {
    if (result) {
        result->was_update = 0;
        result->condition_not_met = 0;
        result->current_value = NULL;
        result->current_vlen = 0;
    }
    if (klen > UINT16_MAX || vlen > UINT32_MAX) return -1;

    /* Trim value to field boundary for compact varlen storage. */
    if (db->format == SLOTCASK_FORMAT_VARIABLE && db->trim_fn)
        vlen = db->trim_fn(value, vlen, db->trim_ctx);

    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;
    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);

    /* Slow path is required when the caller's check fn needs OLD (CAS) or
       when require_existing is set (must know existence to reject missing). */
    if (opts->require_existing || opts->check_needs_old) {
        return upsert_slow_path(db, stream_id_hint, key, klen, value, vlen,
                                opts, result, hash, sid_kf);
    }

    /* ===== FAST PATH =====
       Skip the up-front kf_lookup; let kf_put_new probe and decide.
       For new keys: 1 probe (vs 2 in slow path). For existing keys:
       upgrade-to-update branch loads OLD and runs the same diff path
       the slow path would. Order: seg → kf → pre_commit so a duplicate
       rejection bails cleanly without leaving stale index entries. */
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);

    /* Reserve seg slot. */
    int sid_data = stream_id_hint;
    if (sid_data < 0 || sid_data >= db->num_streams)
        sid_data = (int)((unsigned)hash[15] % (unsigned)db->num_streams);
    SlotcaskStream *pool = &db->streams[sid_data];

    uint32_t slot_capacity;
    SlotcaskFreeSlot fs;
    uint8_t  target_stream = (uint8_t)sid_data;
    uint16_t target_fid;
    uint32_t target_off;
    int got_pool;
    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                           db->slot_size, &fs) == 0);
        if (got_pool) {
            target_fid = fs.file_id;
            target_off = fs.offset;
            slot_capacity = fs.capacity;
        } else {
            size_t rec_size = slotcask_record_size_varlen(klen, vlen);
            uint32_t fid, off;
            if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0)
                return -1;
            target_fid = (uint16_t)fid;
            target_off = off;
            slot_capacity = (uint32_t)rec_size;
        }
    } else {
        slot_capacity = (uint32_t)db->slot_size;
        got_pool = (pool_try_pop_n(pool, 1, &fs) == 0);
        if (got_pool) {
            target_fid = fs.file_id;
            target_off = fs.offset;
        } else {
            uint32_t fid, off;
            if (append_reserve_n(db, pool, 1, &fid, &off) != 0) return -1;
            target_fid = (uint16_t)fid;
            target_off = off;
        }
    }

    /* Write seg. */
    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        if (seg_write_record_varlen(db, target_stream, target_fid, target_off,
                                     hash, key, klen, value, vlen,
                                     slot_capacity) != 0) {
            if (got_pool) pool_push_free_cap(pool, target_fid, target_off,
                                              slot_capacity, db->slot_size);
            return -1;
        }
    } else {
        if (seg_write_record(db, target_stream, target_fid, target_off,
                              hash, key, klen, value, vlen) != 0) {
            if (got_pool) pool_push_free(pool, target_fid, target_off, db->slot_size);
            return -1;
        }
    }

    /* Acquire kf wrlock + commit attempt. */
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) {
        seg_write_flag(db, target_stream, target_fid, target_off, 2);
        pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
        return -1;
    }

    size_t used_delta = 0;
    size_t put_slot = 0;
    int put_rc = kf_put_new(db, &kh, hash, target_stream, target_fid, target_off,
                            key, klen, db->data_dir, &used_delta, &put_slot);

    if (put_rc == 0) {
        /* NEW key path. Run check (with NULL old) and pre_commit. */
        /* Publish (shard, slot) to opts out-params for index-update hooks
           that key by physical location (bitmap). Done BEFORE check + pre_commit
           so user code sees the location of the just-written kf entry. */
        if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;
        if (opts->out_kf_slot)  *opts->out_kf_slot  = (uint32_t)put_slot;
        if (opts->check && opts->check(NULL, opts->check_ctx) == 0) {
            /* Check rejected. Roll back: tombstone the seg + clear the
               kf entry we just wrote. We need to find our slot to clear it. */
            uint8_t  tmp_flag = 0, tmp_sid = 0;
            uint16_t tmp_fid = 0;
            uint32_t tmp_off = 0;
            size_t   tmp_slot = 0;
            if (kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                     &tmp_flag, &tmp_sid, &tmp_fid, &tmp_off,
                                     &tmp_slot) == 0) {
                kf_tombstone_at_slot(&kh, tmp_slot);
            }
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            if (result) result->condition_not_met = 1;
            return -2;
        }
        if (opts->pre_commit) {
            int rc = opts->pre_commit(NULL, value, vlen, 0, opts->pre_commit_ctx);
            if (rc != 0) {
                /* pre_commit failed AFTER kf commit. Best effort: tombstone
                   our kf entry + seg. Indexes may be partially populated;
                   matches the existing-path's pre_commit-failure behavior
                   (just on the opposite side of kf). */
                uint8_t  tmp_flag = 0, tmp_sid = 0;
                uint16_t tmp_fid = 0;
                uint32_t tmp_off = 0;
                size_t   tmp_slot = 0;
                if (kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                         &tmp_flag, &tmp_sid, &tmp_fid, &tmp_off,
                                         &tmp_slot) == 0) {
                    kf_tombstone_at_slot(&kh, tmp_slot);
                }
                kfcache_release(&kh);
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                return -1;
            }
        }
        kfcache_release(&kh);
        if (result) result->was_update = 0;
        return 0;
    }

    if (put_rc == 1) {
        /* EXISTING key path. Either the caller wanted insert-only
           (if_not_exists) → reject; or this is an upsert → upgrade. */
        if (opts->if_not_exists) {
            /* Read existing record so caller sees current_value. */
            uint8_t  ex_flag = 0, ex_sid = 0;
            uint16_t ex_fid = 0;
            uint32_t ex_off = 0;
            size_t   ex_slot = 0;
            uint8_t *old_buf = NULL;
            size_t   old_vlen = 0;
            if (kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                     &ex_flag, &ex_sid, &ex_fid, &ex_off,
                                     &ex_slot) == 0) {
                (void)read_record_value(db, ex_sid, ex_fid, ex_off, key, klen,
                                         &old_buf, &old_vlen);
            }
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            if (result) {
                result->was_update = 1;
                result->condition_not_met = 1;
                result->current_value = old_buf;
                result->current_vlen = old_vlen;
            } else {
                free(old_buf);
            }
            return -2;
        }

        /* UPGRADE TO UPDATE: load existing record, run pre_commit with
           old, repoint kf to our new seg, tombstone OLD seg. */
        uint8_t  ex_flag = 0, ex_sid = 0;
        uint16_t ex_fid = 0;
        uint32_t ex_off = 0;
        size_t   ex_slot = 0;
        if (kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                 &ex_flag, &ex_sid, &ex_fid, &ex_off,
                                 &ex_slot) != 0) {
            /* kf_put_new said it exists but lookup can't find it — race or
                corruption. Bail safely. */
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            return -1;
        }
        uint8_t *old_buf = NULL;
        size_t   old_vlen = 0;
        if (read_record_value(db, ex_sid, ex_fid, ex_off, key, klen,
                              &old_buf, &old_vlen) != 0) {
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            return -1;
        }
        SlotcaskOldRecord old_rec = { old_buf, old_vlen };
        /* check_fn might inspect old (CAS-style) even when check_needs_old
           wasn't asserted — call it now that we have old loaded. If it
           rejects, transfer current_value to caller and bail. */
        if (opts->check && opts->check(&old_rec, opts->check_ctx) == 0) {
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            if (result) {
                result->was_update = 1;
                result->condition_not_met = 1;
                result->current_value = old_buf;     /* transfer ownership */
                result->current_vlen = old_vlen;
            } else {
                free(old_buf);
            }
            return -2;
        }
        /* Publish (shard, ex_slot) for index hooks that key by physical
           location (bitmap). On an in-place update the slot doesn't
           move — kf_repoint_at_slot below rewrites the entry without
           changing its index. */
        if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;
        if (opts->out_kf_slot)  *opts->out_kf_slot  = (uint32_t)ex_slot;
        if (opts->pre_commit) {
            int rc = opts->pre_commit(&old_rec, value, vlen, 1, opts->pre_commit_ctx);
            if (rc != 0) {
                kfcache_release(&kh);
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                free(old_buf);
                return -1;
            }
        }
        /* Atomic 8B repoint of the existing kf entry to our new seg. */
        kf_repoint_at_slot(&kh, ex_slot, target_stream, target_fid, target_off);
        kfcache_release(&kh);
        /* Tombstone the OLD seg (after dropping kf wrlock so segcache wrlock
           doesn't compete with concurrent reads on the kf shard). */
        slotcask_tombstone_and_push_back(db, ex_sid, ex_fid, ex_off);
        if (result) result->was_update = 1;
        free(old_buf);
        return 0;
    }

    /* kf_put_new returned other error. */
    kfcache_release(&kh);
    seg_write_flag(db, target_stream, target_fid, target_off, 2);
    pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
    return -1;
}

/* Original lookup-first path, kept for require_existing / check_needs_old
   callers that can't take the fast path. */
static int upsert_slow_path(SlotcaskDb *db, int stream_id_hint,
                             const void *key, size_t klen,
                             const void *value, size_t vlen,
                             const SlotcaskUpsertOpts *opts,
                             SlotcaskUpsertResult *result,
                             const uint8_t hash[16], int sid_kf) {
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);

    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) return -1;

    /* Lookup current state. kf_lookup_with_slot captures the slot index
       so the commit phase below can call kf_repoint_at_slot directly,
       skipping the second probe + verify_stored_key under the held
       wrlock. Same fix applied to the bulk_upsert primitive. */
    uint8_t old_flag = 0, old_sid = 0;
    uint16_t old_fid = 0;
    uint32_t old_off = 0;
    size_t   kf_slot = 0;
    int found = (kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                       &old_flag, &old_sid, &old_fid, &old_off,
                                       &kf_slot) == 0);

    /* Read OLD value bytes (only if present — we'll need them for check_fn /
       pre_commit / current_value-on-rejection). */
    uint8_t *old_buf = NULL;
    size_t   old_vlen = 0;
    if (found) {
        if (read_record_value(db, old_sid, old_fid, old_off, key, klen,
                              &old_buf, &old_vlen) != 0) {
            kfcache_release(&kh);
            return -1;
        }
    }

    SlotcaskOldRecord old_rec = { old_buf, old_vlen };
    const SlotcaskOldRecord *old_ptr = found ? &old_rec : NULL;

    /* Built-in CAS gates (if_not_exists / require_existing) before user check. */
    int rejected = 0;
    if (found && opts->if_not_exists)        rejected = 1;
    if (!found && opts->require_existing)    rejected = 1;
    if (!rejected && opts->check) {
        if (opts->check(old_ptr, opts->check_ctx) == 0) rejected = 1;
    }

    if (rejected) {
        kfcache_release(&kh);
        if (result) {
            result->was_update = found ? 1 : 0;
            result->condition_not_met = 1;
            result->current_value = old_buf;     /* transfer ownership */
            result->current_vlen = old_vlen;
        } else {
            free(old_buf);
        }
        return -2;
    }

    /* Reserve target slot. */
    int sid_data = stream_id_hint;
    if (sid_data < 0 || sid_data >= db->num_streams)
        sid_data = (int)((unsigned)hash[15] % (unsigned)db->num_streams);
    SlotcaskStream *pool = &db->streams[sid_data];

    uint32_t slot_capacity;
    SlotcaskFreeSlot fs;
    uint8_t  target_stream = (uint8_t)sid_data;
    uint16_t target_fid;
    uint32_t target_off;
    int got_pool;
    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                           db->slot_size, &fs) == 0);
        if (got_pool) {
            target_fid = fs.file_id;
            target_off = fs.offset;
            slot_capacity = fs.capacity;
        } else {
            size_t rec_size = slotcask_record_size_varlen(klen, vlen);
            uint32_t fid, off;
            if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0) {
                kfcache_release(&kh);
                free(old_buf);
                return -1;
            }
            target_fid = (uint16_t)fid;
            target_off = off;
            slot_capacity = (uint32_t)rec_size;
        }
    } else {
        slot_capacity = (uint32_t)db->slot_size;
        got_pool = (pool_try_pop_n(pool, 1, &fs) == 0);
        if (got_pool) {
            target_fid = fs.file_id;
            target_off = fs.offset;
        } else {
            uint32_t fid, off;
            if (append_reserve_n(db, pool, 1, &fid, &off) != 0) {
                kfcache_release(&kh);
                free(old_buf);
                return -1;
            }
            target_fid = (uint16_t)fid;
            target_off = off;
        }
    }

    /* Write new record. */
    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        if (seg_write_record_varlen(db, target_stream, target_fid, target_off,
                                     hash, key, klen, value, vlen,
                                     slot_capacity) != 0) {
            if (got_pool) pool_push_free_cap(pool, target_fid, target_off,
                                              slot_capacity, db->slot_size);
            kfcache_release(&kh);
            free(old_buf);
            return -1;
        }
    } else {
        if (seg_write_record(db, target_stream, target_fid, target_off,
                              hash, key, klen, value, vlen) != 0) {
            if (got_pool) pool_push_free(pool, target_fid, target_off, db->slot_size);
            kfcache_release(&kh);
            free(old_buf);
            return -1;
        }
    }

    /* Publish (shard, slot) for index hooks that key by physical location
       (bitmap). For updates kf_slot is the existing slot from
       kf_lookup_with_slot. For inserts the slot isn't determined yet in
       this slow path — kf_put_new below picks it. Callers using bitmap
       through the slow path can't rely on the slot during pre_commit for
       fresh inserts; that corner is currently rare (slow path runs only
       when check_needs_old / require_existing is set). */
    if (found) {
        if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;
        if (opts->out_kf_slot)  *opts->out_kf_slot  = (uint32_t)kf_slot;
    }

    /* Pre-commit hook (Option B index ordering). Caller updates indexes
       here; if it errors out we tombstone the new slot and bail. */
    if (opts->pre_commit) {
        int rc = opts->pre_commit(old_ptr, value, vlen, found,
                                  opts->pre_commit_ctx);
        if (rc != 0) {
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            kfcache_release(&kh);
            free(old_buf);
            return -1;
        }
    }

    /* Commit point: direct atomic store at the captured slot for updates
       (no probe/verify), kf_put_new for fresh inserts. */
    int kf_rc;
    if (found) {
        kf_repoint_at_slot(&kh, kf_slot, target_stream, target_fid, target_off);
        kf_rc = 0;
    } else {
        size_t used_delta = 0;
        kf_rc = kf_put_new(db, &kh, hash, target_stream, target_fid, target_off,
                           key, klen, db->data_dir, &used_delta, NULL);
        /* Under wrlock; a "1 = already exists" return here would mean a race
           we haven't seen evidence of, but treat it as a hard error. */
        if (kf_rc == 1) kf_rc = -1;
    }

    if (kf_rc != 0) {
        seg_write_flag(db, target_stream, target_fid, target_off, 2);
        pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
        kfcache_release(&kh);
        free(old_buf);
        return -1;
    }

    /* kf is now committed to the new slot. Drop wrlock before tombstoning
       the old slot — that's a separate segment write. */
    kfcache_release(&kh);

    if (found) {
        slotcask_tombstone_and_push_back(db, old_sid, old_fid, old_off);
    }

    if (result) {
        result->was_update = found ? 1 : 0;
        result->condition_not_met = 0;
    }
    free(old_buf);
    return 0;
}

/* INSERT-only with hooks. See slotcask.h for semantics. The order
   (seg write → kf_put_new → pre_commit) lets a duplicate rejection bail
   cleanly without leaving stale index entries; pre_commit only runs
   after kf is committed. */
int slotcask_insert_with_hooks(SlotcaskDb *db, int stream_id_hint,
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                const SlotcaskUpsertOpts *opts,
                                SlotcaskUpsertResult *result) {
    if (result) {
        result->was_update = 0;
        result->condition_not_met = 0;
        result->current_value = NULL;
        result->current_vlen = 0;
    }
    if (klen > UINT16_MAX || vlen > UINT32_MAX) return -1;

    /* Trim value to field boundary for compact varlen storage. */
    if (db->format == SLOTCASK_FORMAT_VARIABLE && db->trim_fn)
        vlen = db->trim_fn(value, vlen, db->trim_ctx);

    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;
    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;

    /* require_existing is incompatible with INSERT-only semantics; the caller
       should route through slotcask_upsert_with_hooks for that. */
    if (opts->require_existing) return -1;

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);

    /* check hook fires with old=NULL on insert; gives the caller a chance
       to reject before any work happens. */
    if (opts->check) {
        if (opts->check(NULL, opts->check_ctx) == 0) {
            if (result) result->condition_not_met = 1;
            return -2;
        }
    }

    /* Reserve a target seg slot (free pool, else append). */
    int sid_data = stream_id_hint;
    if (sid_data < 0 || sid_data >= db->num_streams)
        sid_data = (int)((unsigned)hash[15] % (unsigned)db->num_streams);
    SlotcaskStream *pool = &db->streams[sid_data];

    uint32_t slot_capacity;
    SlotcaskFreeSlot fs;
    uint8_t  target_stream = (uint8_t)sid_data;
    uint16_t target_fid;
    uint32_t target_off;
    int got_pool;
    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                           db->slot_size, &fs) == 0);
        if (got_pool) {
            target_fid = fs.file_id;
            target_off = fs.offset;
            slot_capacity = fs.capacity;
        } else {
            size_t rec_size = slotcask_record_size_varlen(klen, vlen);
            uint32_t fid, off;
            if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0)
                return -1;
            target_fid = (uint16_t)fid;
            target_off = off;
            slot_capacity = (uint32_t)rec_size;
        }
    } else {
        slot_capacity = (uint32_t)db->slot_size;
        got_pool = (pool_try_pop_n(pool, 1, &fs) == 0);
        if (got_pool) {
            target_fid = fs.file_id;
            target_off = fs.offset;
        } else {
            uint32_t fid, off;
            if (append_reserve_n(db, pool, 1, &fid, &off) != 0) return -1;
            target_fid = (uint16_t)fid;
            target_off = off;
        }
    }

    /* Write seg with flag=1 set so kf can point to valid live data. */
    if (db->format == SLOTCASK_FORMAT_VARIABLE) {
        if (seg_write_record_varlen(db, target_stream, target_fid, target_off,
                                     hash, key, klen, value, vlen,
                                     slot_capacity) != 0) {
            if (got_pool) pool_push_free_cap(pool, target_fid, target_off,
                                              slot_capacity, db->slot_size);
            return -1;
        }
    } else {
        if (seg_write_record(db, target_stream, target_fid, target_off,
                              hash, key, klen, value, vlen) != 0) {
            if (got_pool) pool_push_free(pool, target_fid, target_off, db->slot_size);
            return -1;
        }
    }

    /* Acquire kf wrlock and attempt the insert. */
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) {
        seg_write_flag(db, target_stream, target_fid, target_off, 2);
        pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
        return -1;
    }

    size_t used_delta = 0;
    size_t put_slot = 0;
    int put_rc = kf_put_new(db, &kh, hash, target_stream, target_fid, target_off,
                            key, klen, db->data_dir, &used_delta, &put_slot);
    if (put_rc != 0) {
        if (put_rc == 1) {
            /* Duplicate — read the existing record so the caller can
               report it via result->current_value (matches the upsert
               path's behavior for condition_not_met). The lookup pays
               one extra probe + segment read here, but this branch
               only fires on actual duplicates (rare); the common-case
               new-key path stays single-probe. */
            uint8_t  ex_flag = 0, ex_sid = 0;
            uint16_t ex_fid = 0;
            uint32_t ex_off = 0;
            size_t   ex_slot = 0;
            uint8_t *old_buf = NULL;
            size_t   old_vlen = 0;
            if (kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                     &ex_flag, &ex_sid, &ex_fid, &ex_off,
                                     &ex_slot) == 0) {
                (void)read_record_value(db, ex_sid, ex_fid, ex_off, key, klen,
                                         &old_buf, &old_vlen);
            }
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            if (result) {
                result->was_update = 1;
                result->condition_not_met = 1;
                result->current_value = old_buf;     /* transfer ownership */
                result->current_vlen = old_vlen;
            } else {
                free(old_buf);
            }
            return -2;
        }
        kfcache_release(&kh);
        seg_write_flag(db, target_stream, target_fid, target_off, 2);
        pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
        return -1;
    }

    /* kf committed. Run pre_commit for index updates. If it fails AFTER kf
       is committed, the kf entry is already live — best we can do is
       tombstone the seg (so reads see the stale kf entry as a miss) and
       return error. The orphan kf entry will be cleaned up by vacuum's
       compact_kf pass. Same trade-off the existing upsert path makes for
       pre_commit failure (just on the opposite side of kf commit). */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = (uint32_t)put_slot;
    if (opts->pre_commit) {
        int rc = opts->pre_commit(NULL, value, vlen, 0, opts->pre_commit_ctx);
        if (rc != 0) {
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            return -1;
        }
    }

    kfcache_release(&kh);
    return 0;
}

/* ============================================================ Bulk upsert
 *
 * Whole batch under one kfcache wrlock. The savings vs calling
 * slotcask_upsert_with_hooks per record:
 *
 *     N=78K records / shard
 *     before: 78K × 2 lock ops (wrlock acquire + release) = 156K ops
 *     after:  2 lock ops (one acquire, one release) for the whole shard
 *
 * Per-record cost still pays:
 *  - kf_lookup (linear-probe inside the held mmap, no syscall)
 *  - read_record_value if upsert (reads OLD record from segment cache)
 *  - per-stream rotation_lock + segcache wrlock for the seg_write
 *  - kf_put_new / kf_repoint (mmap atomic store, no syscall)
 *
 * Tombstoning of old slots (for upserts) happens AFTER kfcache_release
 * so segcache wrlocks for old segments don't hold the kf lock too.
 */
/* Per-record state carried across the four phases of bulk_upsert. Computed
   in Phase 1 (kf lookup), populated in Phase 3 (slot reservation), consumed
   in Phase 4 (kf commit) and Phase 5 (old-slot tombstoning). */
typedef struct {
    uint8_t  hash[16];
    uint8_t  old_sid;
    uint16_t old_fid;
    uint32_t old_off;
    size_t   old_kf_slot;   /* kf entry slot — captured in Phase 1a so
                              Phase 4 can repoint without re-probing */
    uint8_t *old_buf;       /* malloc'd if old_found, NULL otherwise */
    size_t   old_vlen;
    uint8_t  old_found;
    uint8_t  needs_write;   /* 0 = early-skipped (validation, if_not_exists, lookup err) */
    uint8_t  target_stream;
    uint16_t target_fid;
    uint32_t target_off;
    uint8_t  got_pool;      /* 1 = slot came from free pool (rollback path differs) */
} SlotcaskBulkState;

/* ----- Phase helpers shared by the slow and fast bulk-upsert paths.
   The two paths differ only in Phase 1 (kf lookup vs skip) and Phase 4
   (per-record commit shape). Phase 2 (bucket-by-stream), Phase 3 (per-
   stream batched reserve + seg write), and Phase 5 (tombstone OLD slots)
   are byte-identical between them — extracted here so future tuning to
   either phase lands in one place. */

/* Free the stream_counts/stream_idx pair allocated by Phase 2. Safe for
   NULL inputs (cleanup-after-OOM is the common case). */
static void bulk_stream_arrays_free(SlotcaskDb *db,
                                     int *stream_counts, int **stream_idx) {
    if (stream_idx) {
        for (int s = 0; s < db->num_streams; s++) free(stream_idx[s]);
        free(stream_idx);
    }
    free(stream_counts);
}

/* Phase 2 — bucket records-needing-write by target stream. Outputs
   stream_counts (size db->num_streams) and stream_idx (array of
   pointers; NULL slot means that stream has no records). On OOM the
   helper releases its partial allocations, sets *out_counts /
   *out_idx to NULL, and returns -1. */
static int bulk_phase2_bucket_by_stream(SlotcaskDb *db,
                                         SlotcaskBulkState *st, size_t n,
                                         int **out_counts, int ***out_idx) {
    int *stream_counts = calloc(db->num_streams, sizeof(int));
    int **stream_idx   = calloc(db->num_streams, sizeof(int *));
    int *stream_pos    = calloc(db->num_streams, sizeof(int));
    if (!stream_counts || !stream_idx || !stream_pos) goto oom;

    for (size_t i = 0; i < n; i++) {
        if (st[i].needs_write) stream_counts[st[i].target_stream]++;
    }
    for (int s = 0; s < db->num_streams; s++) {
        if (stream_counts[s] > 0) {
            stream_idx[s] = malloc(stream_counts[s] * sizeof(int));
            if (!stream_idx[s]) goto oom;
        }
    }
    for (size_t i = 0; i < n; i++) {
        if (st[i].needs_write) {
            int s = st[i].target_stream;
            stream_idx[s][stream_pos[s]++] = (int)i;
        }
    }

    free(stream_pos);
    *out_counts = stream_counts;
    *out_idx    = stream_idx;
    return 0;

oom:
    free(stream_pos);
    bulk_stream_arrays_free(db, stream_counts, stream_idx);
    *out_counts = NULL;
    *out_idx    = NULL;
    return -1;
}

/* Phase 3 — per-stream batched reserve + seg write. For each stream
   with non-zero count: try the free pool first (sort by file_id and
   batch one segcache_acquire per file); if the pool is empty or short,
   append_reserve_n a contiguous run + single segcache_acquire for the
   whole bucket. On per-stream failure marks affected recs status=-1
   and continues — never propagates failure as a return value. */
static void bulk_phase3_seg_writes(SlotcaskDb *db,
                                    SlotcaskBulkRec *recs, SlotcaskBulkState *st,
                                    int *stream_counts, int **stream_idx) {
    int is_varlen = (db->format == SLOTCASK_FORMAT_VARIABLE);

    for (int s = 0; s < db->num_streams; s++) {
        int cnt = stream_counts[s];
        if (cnt == 0) continue;
        SlotcaskStream *pool = &db->streams[s];

        if (is_varlen) {
            /* Variable-length path: each record may have a different size.
               Pop from pool per-record (no batch pop possible). If the pool
               doesn't have a fitting slot, fall through to the append path. */
            for (int k = 0; k < cnt; k++) {
                int i = stream_idx[s][k];
                SlotcaskBulkRec *r = &recs[i];
                SlotcaskFreeSlot fs;
                size_t needed = 24 + r->klen + r->vlen;
                size_t rec_size = slotcask_record_size_varlen(r->klen, r->vlen);
                if (pool_try_pop_for_size(pool, (uint32_t)needed,
                                           db->slot_size, &fs) == 0) {
                    st[i].target_fid = fs.file_id;
                    st[i].target_off = fs.offset;
                    st[i].got_pool   = 1;
                    r->slot_capacity = fs.capacity;
                } else {
                    uint32_t fid, off;
                    if (append_reserve_single_varlen(db, pool, rec_size,
                                                      &fid, &off) != 0) {
                        r->status = -1;
                        continue;
                    }
                    st[i].target_fid = (uint16_t)fid;
                    st[i].target_off = off;
                    st[i].got_pool   = 0;
                    r->slot_capacity = (uint32_t)rec_size;
                }
                char path[PATH_MAX];
                seg_path_for(path, db->data_dir, (uint8_t)s, st[i].target_fid);
                SlotcaskSegHandle h;
                if (segcache_acquire(&h, path, 1, 0) != 0) {
                    if (st[i].got_pool)
                        pool_push_free_cap(pool, st[i].target_fid,
                                            st[i].target_off,
                                            r->slot_capacity, db->slot_size);
                    r->status = -1;
                    continue;
                }
                seg_record_emit(h.map + st[i].target_off, (int)rec_size,
                                 st[i].hash, r->key, r->klen,
                                 r->value, r->vlen);
                segcache_release(&h);
            }
            continue;
        }

        /* ——— Fixed-format path (unchanged) ——— */
        SlotcaskFreeSlot *fs = malloc((size_t)cnt * sizeof(SlotcaskFreeSlot));
        if (!fs) {
            for (int k = 0; k < cnt; k++) recs[stream_idx[s][k]].status = -1;
            continue;
        }
        int got_pool = (pool_try_pop_n(pool, (size_t)cnt, fs) == 0);

        if (got_pool) {
            typedef struct { uint16_t fid; uint32_t off; int rec_idx; } PoolItem;
            PoolItem *items = malloc((size_t)cnt * sizeof(PoolItem));
            if (!items) {
                for (int k = 0; k < cnt; k++) {
                    int i = stream_idx[s][k];
                    pool_push_free(pool, fs[k].file_id, fs[k].offset, db->slot_size);
                    recs[i].status = -1;
                }
                free(fs);
                continue;
            }
            for (int k = 0; k < cnt; k++) {
                items[k].fid     = fs[k].file_id;
                items[k].off     = fs[k].offset;
                items[k].rec_idx = stream_idx[s][k];
            }
            free(fs);
            for (int a = 1; a < cnt; a++) {
                PoolItem tmp = items[a];
                int b = a - 1;
                while (b >= 0 && items[b].fid > tmp.fid) {
                    items[b + 1] = items[b];
                    b--;
                }
                items[b + 1] = tmp;
            }

            int k = 0;
            while (k < cnt) {
                int run_end = k + 1;
                while (run_end < cnt && items[run_end].fid == items[k].fid)
                    run_end++;
                char path[PATH_MAX];
                seg_path_for(path, db->data_dir, (uint8_t)s, items[k].fid);
                SlotcaskSegHandle h;
                if (segcache_acquire(&h, path, 0, 0) != 0) {
                    for (int j = k; j < run_end; j++) {
                        int i = items[j].rec_idx;
                        pool_push_free(pool, items[j].fid, items[j].off, db->slot_size);
                        recs[i].status = -1;
                    }
                    k = run_end;
                    continue;
                }
                for (int j = k; j < run_end; j++) {
                    int i = items[j].rec_idx;
                    SlotcaskBulkRec *r = &recs[i];
                    st[i].target_fid = items[j].fid;
                    st[i].target_off = items[j].off;
                    st[i].got_pool   = 1;
                    seg_record_emit(h.map + items[j].off, db->slot_size,
                                     st[i].hash, r->key, r->klen,
                                     r->value, r->vlen);
                }
                segcache_release(&h);
                k = run_end;
            }
            free(items);
            continue;
        }
        free(fs);

        uint32_t *offsets = malloc((size_t)cnt * sizeof(uint32_t));
        if (!offsets) {
            for (int k = 0; k < cnt; k++) recs[stream_idx[s][k]].status = -1;
            continue;
        }
        uint32_t base_fid = 0;
        if (append_reserve_n(db, pool, (size_t)cnt, &base_fid, offsets) != 0) {
            free(offsets);
            for (int k = 0; k < cnt; k++) recs[stream_idx[s][k]].status = -1;
            continue;
        }
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, (uint8_t)s, base_fid);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 1, 0) != 0) {
            free(offsets);
            for (int k = 0; k < cnt; k++) recs[stream_idx[s][k]].status = -1;
            continue;
        }
        for (int k = 0; k < cnt; k++) {
            int i = stream_idx[s][k];
            SlotcaskBulkRec *r = &recs[i];
            st[i].target_fid = (uint16_t)base_fid;
            st[i].target_off = offsets[k];
            st[i].got_pool   = 0;
            seg_record_emit(h.map + offsets[k], db->slot_size,
                             st[i].hash, r->key, r->klen,
                             r->value, r->vlen);
        }
        segcache_release(&h);
        free(offsets);
    }
}

/* Phase 5 — tombstone OLD seg slots for successful upserts. Done
   outside the kf wrlock so the seg write doesn't hold both locks.
   Records are sorted by (old_sid, old_fid) and walked in runs so
   one segcache_acquire covers every tombstone in the same file —
   mirrors the bulk-delete Phase 3 batching. Cuts the acquire/release
   pair count from N to (unique-files), which at indexed-update scale
   is the dominant cost (random-write into cold segment pages). */
static void bulk_phase5_tombstone_olds(SlotcaskDb *db,
                                        SlotcaskBulkRec *recs,
                                        SlotcaskBulkState *st, size_t n) {
    int *tomb_idx = malloc(n * sizeof(int));
    if (!tomb_idx) {
        /* OOM: fall back to per-record tombstone (handles varlen correctly). */
        for (size_t i = 0; i < n; i++) {
            if (recs[i].status != 0) continue;
            if (!st[i].old_found) continue;
            slotcask_tombstone_and_push_back(db, st[i].old_sid,
                                              st[i].old_fid, st[i].old_off);
        }
        return;
    }

    int tcount = 0;
    for (size_t i = 0; i < n; i++) {
        if (recs[i].status != 0) continue;
        if (!st[i].old_found) continue;
        tomb_idx[tcount++] = (int)i;
    }

    SLOTCASK_SORT_IDX_BY_SEG_LOC(tomb_idx, tcount, st);

    int k = 0;
    while (k < tcount) {
        int run_end = k + 1;
        uint8_t  sid = st[tomb_idx[k]].old_sid;
        uint16_t fid = st[tomb_idx[k]].old_fid;
        while (run_end < tcount &&
               st[tomb_idx[run_end]].old_sid == sid &&
               st[tomb_idx[run_end]].old_fid == fid)
            run_end++;

        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, sid, fid);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0) != 0) {
            /* Acquire failed: flag stays at 1; vacuum recovers via kf reverse-lookup.
               Skip pool push for varlen — can't read capacity without the map. */
            if (db->format != SLOTCASK_FORMAT_VARIABLE) {
                for (int j = k; j < run_end; j++) {
                    int i = tomb_idx[j];
                    pool_push_free(&db->streams[sid], st[i].old_fid,
                                   st[i].old_off, db->slot_size);
                }
            }
            k = run_end;
            continue;
        }
        for (int j = k; j < run_end; j++) {
            int i = tomb_idx[j];
            __atomic_store_n(&h.map[st[i].old_off + 18], 2, __ATOMIC_RELEASE);
            if (db->format == SLOTCASK_FORMAT_VARIABLE) {
                uint16_t klen = seg_rec_klen(h.map + st[i].old_off);
                uint32_t vlen = seg_rec_vlen(h.map + st[i].old_off);
                uint32_t cap  = (uint32_t)slotcask_record_size_varlen(
                                    (size_t)klen, (size_t)vlen);
                pool_push_free_cap(&db->streams[sid], st[i].old_fid,
                                   st[i].old_off, cap, db->slot_size);
            } else {
                pool_push_free(&db->streams[sid], st[i].old_fid,
                               st[i].old_off, db->slot_size);
            }
        }
        segcache_release(&h);
        k = run_end;
    }
    free(tomb_idx);
}

/* Slow bulk upsert: lookup → optional OLD read → seg write → kf commit.
   Required when callers depend on OLD before commit (require_existing,
   pre_commit_needs_old, value_compute). */
static int bulk_upsert_slow_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                        SlotcaskBulkRec *recs, size_t n,
                                        const SlotcaskBulkOpts *opts);

/* Fast bulk upsert: skip Phase 1a's kf_lookup. For each record, attempt
   kf_put_new in the commit phase; on duplicate, upgrade-to-update inline
   (lookup + read OLD + pre_commit + kf_repoint). For pure-insert workloads
   (the common bulk-insert case with all-new keys), this saves one probe
   per record — at 200K records / kf shard, that's ~20-40ms of pure probe
   cost per shard (~12-25% of bulk-insert wall time). */
static int bulk_upsert_fast_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                        SlotcaskBulkRec *recs, size_t n,
                                        const SlotcaskBulkOpts *opts);

int slotcask_bulk_upsert_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                     SlotcaskBulkRec *recs, size_t n,
                                     const SlotcaskBulkOpts *opts) {
    if (n == 0) return 0;
    SlotcaskBulkOpts blank = {0};
    if (!opts) opts = &blank;

    /* Slow path required when caller needs OLD bytes before commit:
       - require_existing: bulk-update gate, must reject missing keys
       - pre_commit_needs_old: indexed update needs old value for diff
       - value_compute: bulk-update derives NEW from OLD */
    if (opts->require_existing || opts->pre_commit_needs_old ||
        opts->value_compute != NULL) {
        return bulk_upsert_slow_in_kfshard(db, kf_shard_id, recs, n, opts);
    }
    return bulk_upsert_fast_in_kfshard(db, kf_shard_id, recs, n, opts);
}

static int bulk_upsert_slow_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                        SlotcaskBulkRec *recs, size_t n,
                                        const SlotcaskBulkOpts *opts) {
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) return -1;

    SlotcaskBulkState *st = calloc(n, sizeof(SlotcaskBulkState));
    if (!st) { kfcache_release(&kh); return -1; }
    int *stream_counts = NULL;
    int **stream_idx   = NULL;

    /* ===== Phase 1a — kf_lookup per record. No segcache touched here. */
    for (size_t i = 0; i < n; i++) {
        SlotcaskBulkRec *r = &recs[i];
        r->status = 0;
        r->was_update = 0;

        if (r->klen > UINT16_MAX || r->vlen > UINT32_MAX ||
            (size_t)24 + r->klen + r->vlen > (size_t)db->slot_size) {
            r->status = -1;
            continue;
        }

        compute_hash(r->key, r->klen, st[i].hash);
        uint8_t old_flag = 0;
        int found = (kf_lookup_with_slot(&kh, st[i].hash, r->key, r->klen,
                                          db->data_dir,
                                          &old_flag, &st[i].old_sid,
                                          &st[i].old_fid, &st[i].old_off,
                                          &st[i].old_kf_slot) == 0);
        st[i].old_found = found ? 1 : 0;

        /* Per-record CAS OR-combines with batch-level opts.if_not_exists.
           Auto-key bulk-insert flags omit-key records (server-generated
           UUID / seq.next) as strict-insert while leaving provided-key
           records as upsert in the same batch. */
        if (found && (opts->if_not_exists || r->if_not_exists)) {
            r->status = -2;
            r->was_update = 1;
            continue;
        }
        if (!found && opts->require_existing) {
            /* bulk-update gate: don't auto-insert when caller wanted update. */
            r->status = -2;
            r->was_update = 0;
            continue;
        }

        st[i].target_stream = (uint8_t)((unsigned)st[i].hash[15] %
                                         (unsigned)db->num_streams);
        st[i].needs_write = 1;
    }

    /* ===== Phase 1b — batched OLD-value reads.
       Sort the indices that need OLD reads by (old_sid, old_fid) and walk
       linearly: take the segcache rdlock once per unique file, reuse the
       handle for every record in that file. Drops one segcache_acquire/
       release pair per record on indexed update-heavy workloads (where
       pre_commit_needs_old=1). For non-indexed workloads
       pre_commit_needs_old=0, so this whole phase is a no-op.
       Skips records whose caller already supplied recs[i].old_value —
       e.g. bulk-update workers, which read OLD to compute NEW and don't
       want to re-read here. value_compute also implies needs_old since
       the callback derives NEW from OLD. */
    int needs_old = opts->pre_commit_needs_old || opts->value_compute != NULL;
    if (needs_old) {
        int *read_idx = malloc(n * sizeof(int));
        if (read_idx) {
            int rcount = 0;
            for (size_t i = 0; i < n; i++) {
                if (recs[i].status == 0 && st[i].old_found && st[i].needs_write &&
                    recs[i].old_value == NULL)
                    read_idx[rcount++] = (int)i;
            }
            SLOTCASK_SORT_IDX_BY_SEG_LOC(read_idx, rcount, st);

            int k = 0;
            while (k < rcount) {
                int run_end = k + 1;
                uint8_t  sid = st[read_idx[k]].old_sid;
                uint16_t fid = st[read_idx[k]].old_fid;
                while (run_end < rcount &&
                       st[read_idx[run_end]].old_sid == sid &&
                       st[read_idx[run_end]].old_fid == fid)
                    run_end++;
                /* records [k..run_end) all read from the same seg file. */
                char path[PATH_MAX];
                seg_path_for(path, db->data_dir, sid, fid);
                SlotcaskSegHandle h;
                if (segcache_acquire(&h, path, 0, 0) != 0) {
                    for (int j = k; j < run_end; j++) recs[read_idx[j]].status = -1;
                    k = run_end;
                    continue;
                }
                for (int j = k; j < run_end; j++) {
                    int i = read_idx[j];
                    SlotcaskBulkRec *r = &recs[i];
                    const uint8_t *rec = h.map + st[i].old_off;
                    if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) != 1) { r->status = -1; continue; }
                    uint16_t k_stored = seg_rec_klen(rec);
                    uint32_t v_stored = seg_rec_vlen(rec);
                    if (k_stored != r->klen || memcmp(rec + 24, r->key, r->klen) != 0) {
                        r->status = -1;
                        continue;
                    }
                    uint8_t *buf = malloc(v_stored ? v_stored : 1);
                    if (!buf) { r->status = -1; continue; }
                    if (v_stored) memcpy(buf, rec + 24 + r->klen, v_stored);
                    st[i].old_buf  = buf;
                    st[i].old_vlen = v_stored;
                }
                segcache_release(&h);
                k = run_end;
            }
            free(read_idx);
        }
    }

    /* ===== Phase 1c — per-record value_compute (NEW-from-OLD).
       Bulk-update workers populate rec->value/vlen here from the OLD
       record they just had read in Phase 1b. Non-zero return marks the
       record skipped (e.g. CAS rejection) and excludes it from the
       seg-write phase. */
    if (opts->value_compute) {
        for (size_t i = 0; i < n; i++) {
            if (recs[i].status != 0) continue;
            if (!st[i].needs_write) continue;
            const void *old_v = recs[i].old_value ? recs[i].old_value : st[i].old_buf;
            size_t      old_l = recs[i].old_value ? recs[i].old_vlen  : st[i].old_vlen;
            SlotcaskOldRecord old_rec = { (const uint8_t *)old_v, old_l };
            int rc = opts->value_compute(st[i].old_found ? &old_rec : NULL,
                                          &recs[i]);
            if (rc != 0) {
                recs[i].status = -2;
                st[i].needs_write = 0;
            }
        }
    }

    /* ===== Phase 2 — bucket records-needing-write by target stream. */
    if (bulk_phase2_bucket_by_stream(db, st, n, &stream_counts, &stream_idx) != 0)
        goto oom;

    /* ===== Phase 3 — per-stream batched reserve + seg write. */
    bulk_phase3_seg_writes(db, recs, st, stream_counts, stream_idx);

    /* ===== Phase 4 — per-record kf commit + pre_commit (under held kf wrlock).
       For UPDATES the slot index is known from Phase 1a (kf_lookup) — we
       publish (shard, slot) and run pre_commit before kf_repoint. For
       INSERTS the slot only exists after kf_put_new, so we commit the
       kf entry first, publish (shard, slot), then run pre_commit; if it
       returns non-zero we kf_tombstone the entry we just wrote. This
       gives bitmap index hooks a stable (shard, slot) handle regardless
       of insert vs update path. */
    for (size_t i = 0; i < n; i++) {
        if (recs[i].status != 0) continue;
        if (!st[i].needs_write) continue;
        SlotcaskBulkRec *r = &recs[i];

        size_t pub_slot = 0;
        int kf_committed = 0;
        if (st[i].old_found) {
            pub_slot = st[i].old_kf_slot;
        } else {
            size_t used_delta = 0;
            int kf_rc = kf_put_new(db, &kh, st[i].hash, st[i].target_stream,
                                   st[i].target_fid, st[i].target_off,
                                   r->key, r->klen, db->data_dir,
                                   &used_delta, &pub_slot);
            if (kf_rc == 1) kf_rc = -1;
            if (kf_rc != 0) {
                seg_write_flag(db, st[i].target_stream, st[i].target_fid,
                                st[i].target_off, 2);
                pool_push_free(&db->streams[st[i].target_stream],
                                st[i].target_fid, st[i].target_off, db->slot_size);
                r->status = -1;
                continue;
            }
            kf_committed = 1;
        }

        r->kf_shard = kf_shard_id;
        r->kf_slot  = (uint32_t)pub_slot;

        if (opts->pre_commit) {
            const void *old_v = r->old_value ? r->old_value : st[i].old_buf;
            size_t      old_l = r->old_value ? r->old_vlen  : st[i].old_vlen;
            SlotcaskOldRecord old_rec = { (const uint8_t *)old_v, old_l };
            int rc = opts->pre_commit(st[i].old_found ? &old_rec : NULL,
                                       r, st[i].old_found);
            if (rc != 0) {
                /* Roll back: tombstone the kf entry we just wrote (if any)
                   and the seg, push the free slot back. */
                if (kf_committed) {
                    kf_tombstone_at_slot(&kh, pub_slot);
                }
                seg_write_flag(db, st[i].target_stream, st[i].target_fid,
                                st[i].target_off, 2);
                pool_push_free(&db->streams[st[i].target_stream],
                                st[i].target_fid, st[i].target_off, db->slot_size);
                r->status = -1;
                continue;
            }
        }

        int kf_rc;
        if (st[i].old_found) {
            /* Skip kf_repoint's probe + verify_stored_key — the slot index
               from Phase 1a is still valid because the kf wrlock has been
               held continuously. Direct atomic 8B store. */
            kf_repoint_at_slot(&kh, st[i].old_kf_slot, st[i].target_stream,
                                st[i].target_fid, st[i].target_off);
            kf_rc = 0;
        } else {
            /* Already committed in the pre_commit publish phase above. */
            kf_rc = 0;
        }
        if (kf_rc != 0) {
            seg_write_flag(db, st[i].target_stream, st[i].target_fid,
                            st[i].target_off, 2);
            pool_push_free(&db->streams[st[i].target_stream],
                            st[i].target_fid, st[i].target_off, db->slot_size);
            r->status = -1;
            continue;
        }
        r->was_update = st[i].old_found ? 1 : 0;
    }

    kfcache_release(&kh);

    /* ===== Phase 5 — tombstone OLD slots for successful upserts. */
    bulk_phase5_tombstone_olds(db, recs, st, n);

    for (size_t i = 0; i < n; i++) free(st[i].old_buf);
    bulk_stream_arrays_free(db, stream_counts, stream_idx);
    free(st);
    return 0;

oom:
    kfcache_release(&kh);
    bulk_stream_arrays_free(db, stream_counts, stream_idx);
    for (size_t i = 0; i < n; i++) free(st[i].old_buf);
    free(st);
    return -1;
}

/* ============================================================ Bulk fast path
 *
 * Skip Phase 1a's kf_lookup. Pre-commit fires AFTER kf commit so a
 * duplicate detected by kf_put_new can roll back without leaving stale
 * index entries. For records that turn out to already exist (rare in
 * pure-insert workloads), the upgrade-to-update path runs INLINE in
 * Phase 4: kf_lookup + read OLD + pre_commit(old) + kf_repoint. Same
 * total cost as slow path for that record; net win comes from new keys
 * skipping the upfront probe. Gated to bulk-insert callers.
 */
static int bulk_upsert_fast_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                        SlotcaskBulkRec *recs, size_t n,
                                        const SlotcaskBulkOpts *opts) {
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) return -1;

    SlotcaskBulkState *st = calloc(n, sizeof(SlotcaskBulkState));
    if (!st) { kfcache_release(&kh); return -1; }
    int *stream_counts = NULL;
    int **stream_idx   = NULL;

    /* Phase 1: validate, hash, route — NO kf lookup. */
    for (size_t i = 0; i < n; i++) {
        SlotcaskBulkRec *r = &recs[i];
        r->status = 0;
        r->was_update = 0;
        if (r->klen > UINT16_MAX || r->vlen > UINT32_MAX ||
            (size_t)24 + r->klen + r->vlen > (size_t)db->slot_size) {
            r->status = -1;
            continue;
        }
        compute_hash(r->key, r->klen, st[i].hash);
        st[i].target_stream = (uint8_t)((unsigned)st[i].hash[15] %
                                         (unsigned)db->num_streams);
        st[i].needs_write = 1;
    }

    /* Phase 2: bucket by stream. */
    if (bulk_phase2_bucket_by_stream(db, st, n, &stream_counts, &stream_idx) != 0)
        goto fast_oom;

    /* Phase 3: per-stream batched reserve + seg write. */
    bulk_phase3_seg_writes(db, recs, st, stream_counts, stream_idx);

    /* Phase 4: kf commit + on-duplicate upgrade-to-update. */
    for (size_t i = 0; i < n; i++) {
        if (recs[i].status != 0) continue;
        if (!st[i].needs_write) continue;
        SlotcaskBulkRec *r = &recs[i];

        size_t used_delta = 0;
        size_t put_slot = 0;
        int put_rc = kf_put_new(db, &kh, st[i].hash,
                                 st[i].target_stream, st[i].target_fid,
                                 st[i].target_off, r->key, r->klen,
                                 db->data_dir, &used_delta, &put_slot);

        if (put_rc == 0) {
            /* New key. Publish (shard, slot) then run pre_commit. */
            r->kf_shard = kf_shard_id;
            r->kf_slot  = (uint32_t)put_slot;
            if (opts->pre_commit) {
                int rc = opts->pre_commit(NULL, r, 0);
                if (rc != 0) {
                    /* Rollback: tombstone the slot we just wrote + the seg. */
                    kf_tombstone_at_slot(&kh, put_slot);
                    seg_write_flag(db, st[i].target_stream, st[i].target_fid,
                                    st[i].target_off, 2);
                    pool_push_free(&db->streams[st[i].target_stream],
                                    st[i].target_fid, st[i].target_off, db->slot_size);
                    r->status = -1;
                }
            }
            r->was_update = 0;
            continue;
        }

        if (put_rc == 1) {
            /* Race-detection branch: kf_put_new found a concurrent insert
               for this key (Phase 1a's lookup said missing). Per-record
               CAS OR-combines with batch-level (auto-key strict-insert
               needs to surface as condition_not_met on collision). */
            if (opts->if_not_exists || r->if_not_exists) {
                seg_write_flag(db, st[i].target_stream, st[i].target_fid,
                                st[i].target_off, 2);
                pool_push_free(&db->streams[st[i].target_stream],
                                st[i].target_fid, st[i].target_off, db->slot_size);
                r->status = -2;
                r->was_update = 1;
                continue;
            }
            /* Upgrade to update inline. */
            uint8_t  ex_flag = 0, ex_sid = 0;
            uint16_t ex_fid = 0;
            uint32_t ex_off = 0;
            size_t   ex_slot = 0;
            if (kf_lookup_with_slot(&kh, st[i].hash, r->key, r->klen,
                                     db->data_dir, &ex_flag, &ex_sid, &ex_fid,
                                     &ex_off, &ex_slot) != 0) {
                seg_write_flag(db, st[i].target_stream, st[i].target_fid,
                                st[i].target_off, 2);
                pool_push_free(&db->streams[st[i].target_stream],
                                st[i].target_fid, st[i].target_off, db->slot_size);
                r->status = -1;
                continue;
            }
            uint8_t *old_buf = NULL;
            size_t   old_vlen = 0;
            if (read_record_value(db, ex_sid, ex_fid, ex_off, r->key, r->klen,
                                   &old_buf, &old_vlen) != 0) {
                seg_write_flag(db, st[i].target_stream, st[i].target_fid,
                                st[i].target_off, 2);
                pool_push_free(&db->streams[st[i].target_stream],
                                st[i].target_fid, st[i].target_off, db->slot_size);
                r->status = -1;
                continue;
            }
            SlotcaskOldRecord old_rec = { old_buf, old_vlen };
            /* Publish (shard, slot) for bitmap hooks before pre_commit
               fires. The slot is the existing kf entry's index — same
               slot kf_repoint_at_slot below will overwrite. */
            r->kf_shard = kf_shard_id;
            r->kf_slot  = (uint32_t)ex_slot;
            if (opts->pre_commit) {
                int rc = opts->pre_commit(&old_rec, r, 1);
                if (rc != 0) {
                    seg_write_flag(db, st[i].target_stream, st[i].target_fid,
                                    st[i].target_off, 2);
                    pool_push_free(&db->streams[st[i].target_stream],
                                    st[i].target_fid, st[i].target_off, db->slot_size);
                    free(old_buf);
                    r->status = -1;
                    continue;
                }
            }
            kf_repoint_at_slot(&kh, ex_slot, st[i].target_stream,
                                st[i].target_fid, st[i].target_off);
            st[i].old_found = 1;
            st[i].old_sid = ex_sid;
            st[i].old_fid = ex_fid;
            st[i].old_off = ex_off;
            free(old_buf);
            r->was_update = 1;
            continue;
        }

        /* Other kf error. */
        seg_write_flag(db, st[i].target_stream, st[i].target_fid,
                        st[i].target_off, 2);
        pool_push_free(&db->streams[st[i].target_stream],
                        st[i].target_fid, st[i].target_off, db->slot_size);
        r->status = -1;
    }

    kfcache_release(&kh);

    /* Phase 5: tombstone OLD seg slots for upgraded records (after wrlock). */
    bulk_phase5_tombstone_olds(db, recs, st, n);

    bulk_stream_arrays_free(db, stream_counts, stream_idx);
    free(st);
    return 0;

fast_oom:
    kfcache_release(&kh);
    bulk_stream_arrays_free(db, stream_counts, stream_idx);
    free(st);
    return -1;
}

/* ============================================================ Bulk delete
 *
 * Mirror of slotcask_bulk_upsert_in_kfshard for deletes. Per-record state
 * is leaner — no target slot reservation, no NEW seg write — but the
 * lock-amortisation pattern is identical: one kf wrlock for the whole
 * batch, batched OLD reads sorted by old slot, batched tombstone flag-
 * flips sorted by old slot. Caller pre-buckets records so all hash to
 * `kf_shard_id`. */
typedef struct {
    uint8_t  hash[16];
    uint8_t  old_sid;
    uint16_t old_fid;
    uint32_t old_off;
    size_t   kf_slot;       /* kf entry slot — captured in Phase 1a so
                              Phase 2 can tombstone without re-probing */
    uint8_t *old_buf;       /* malloc'd if pre_commit_needs_old, NULL otherwise */
    size_t   old_vlen;
    uint8_t  found;         /* 1 if kf entry exists, 0 otherwise */
    uint8_t  committed;     /* 1 once kf_tombstone has succeeded */
} SlotcaskBulkDelState;

int slotcask_bulk_delete_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                     SlotcaskBulkRec *recs, size_t n,
                                     const SlotcaskBulkDeleteOpts *opts) {
    if (n == 0) return 0;
    SlotcaskBulkDeleteOpts blank = {0};
    if (!opts) opts = &blank;

    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) return -1;

    SlotcaskBulkDelState *st = calloc(n, sizeof(SlotcaskBulkDelState));
    if (!st) { kfcache_release(&kh); return -1; }

    /* ===== Phase 1a — kf_lookup per record. */
    for (size_t i = 0; i < n; i++) {
        SlotcaskBulkRec *r = &recs[i];
        r->status = 0;
        r->was_update = 0;

        if (r->klen > UINT16_MAX) {
            r->status = -1;
            continue;
        }
        compute_hash(r->key, r->klen, st[i].hash);
        uint8_t old_flag = 0;
        int found = (kf_lookup_with_slot(&kh, st[i].hash, r->key, r->klen,
                                          db->data_dir,
                                          &old_flag, &st[i].old_sid,
                                          &st[i].old_fid, &st[i].old_off,
                                          &st[i].kf_slot) == 0);
        st[i].found = found ? 1 : 0;
        if (!found) {
            r->status = -2;        /* not found — skipped */
            continue;
        }
    }

    /* ===== Phase 1b — batched OLD reads (sorted by old_sid/old_fid).
       Skipped entirely when pre_commit_needs_old=0. */
    if (opts->pre_commit_needs_old) {
        int *read_idx = malloc(n * sizeof(int));
        if (read_idx) {
            int rcount = 0;
            for (size_t i = 0; i < n; i++) {
                if (recs[i].status == 0 && st[i].found)
                    read_idx[rcount++] = (int)i;
            }
            SLOTCASK_SORT_IDX_BY_SEG_LOC(read_idx, rcount, st);
            int k = 0;
            while (k < rcount) {
                int run_end = k + 1;
                uint8_t  sid = st[read_idx[k]].old_sid;
                uint16_t fid = st[read_idx[k]].old_fid;
                while (run_end < rcount &&
                       st[read_idx[run_end]].old_sid == sid &&
                       st[read_idx[run_end]].old_fid == fid)
                    run_end++;
                char path[PATH_MAX];
                seg_path_for(path, db->data_dir, sid, fid);
                SlotcaskSegHandle h;
                if (segcache_acquire(&h, path, 0, 0) != 0) {
                    for (int j = k; j < run_end; j++) recs[read_idx[j]].status = -1;
                    k = run_end;
                    continue;
                }
                for (int j = k; j < run_end; j++) {
                    int i = read_idx[j];
                    SlotcaskBulkRec *r = &recs[i];
                    const uint8_t *rec = h.map + st[i].old_off;
                    if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) != 1) { r->status = -1; continue; }
                    uint16_t k_stored = seg_rec_klen(rec);
                    uint32_t v_stored = seg_rec_vlen(rec);
                    if (k_stored != r->klen || memcmp(rec + 24, r->key, r->klen) != 0) {
                        r->status = -1;
                        continue;
                    }
                    uint8_t *buf = malloc(v_stored ? v_stored : 1);
                    if (!buf) { r->status = -1; continue; }
                    if (v_stored) memcpy(buf, rec + 24 + r->klen, v_stored);
                    st[i].old_buf  = buf;
                    st[i].old_vlen = v_stored;
                }
                segcache_release(&h);
                k = run_end;
            }
            free(read_idx);
        }
    }

    /* ===== Phase 2 — per-record pre_commit + kf_tombstone (under wrlock). */
    for (size_t i = 0; i < n; i++) {
        if (recs[i].status != 0) continue;
        if (!st[i].found) continue;
        SlotcaskBulkRec *r = &recs[i];

        /* Publish (shard, slot) for bitmap hooks before pre_commit fires. */
        r->kf_shard = kf_shard_id;
        r->kf_slot  = (uint32_t)st[i].kf_slot;

        if (opts->pre_commit) {
            SlotcaskOldRecord old_rec = { st[i].old_buf, st[i].old_vlen };
            int rc = opts->pre_commit(opts->pre_commit_needs_old ? &old_rec : NULL, r);
            if (rc != 0) { r->status = -1; continue; }
        }

        /* Skip kf_tombstone's probe + verify_stored_key — Phase 1a captured
           the slot, and the kf wrlock has been held continuously, so the
           slot is still authoritative. Direct flag flip. */
        kf_tombstone_at_slot(&kh, st[i].kf_slot);
        st[i].committed = 1;
        r->was_update = 1;
    }

    kfcache_release(&kh);

    /* ===== Phase 3 — batched seg tombstones, post-kf-release. Sort by
       (old_sid, old_fid) and reuse one segcache rdlock per unique file.
       Mirror of bulk_upsert's Phase 5 batching. */
    int *tomb_idx = malloc(n * sizeof(int));
    if (tomb_idx) {
        int tcount = 0;
        for (size_t i = 0; i < n; i++) {
            if (st[i].committed) tomb_idx[tcount++] = (int)i;
        }
        SLOTCASK_SORT_IDX_BY_SEG_LOC(tomb_idx, tcount, st);
        int k = 0;
        while (k < tcount) {
            int run_end = k + 1;
            uint8_t  sid = st[tomb_idx[k]].old_sid;
            uint16_t fid = st[tomb_idx[k]].old_fid;
            while (run_end < tcount &&
                   st[tomb_idx[run_end]].old_sid == sid &&
                   st[tomb_idx[run_end]].old_fid == fid)
                run_end++;
            char path[PATH_MAX];
            seg_path_for(path, db->data_dir, sid, fid);
            SlotcaskSegHandle h;
            if (segcache_acquire(&h, path, 0, 0) != 0) { k = run_end; continue; }
            for (int j = k; j < run_end; j++) {
                int i = tomb_idx[j];
                __atomic_store_n(&h.map[st[i].old_off + 18], 2, __ATOMIC_RELEASE);
                pool_push_free(&db->streams[sid], st[i].old_fid, st[i].old_off, db->slot_size);
            }
            segcache_release(&h);
            k = run_end;
        }
        free(tomb_idx);
    } else {
        /* OOM: fall back to per-record tombstone via the existing helper. */
        for (size_t i = 0; i < n; i++) {
            if (!st[i].committed) continue;
            seg_write_flag(db, st[i].old_sid, st[i].old_fid, st[i].old_off, 2);
            pool_push_free(&db->streams[st[i].old_sid],
                            st[i].old_fid, st[i].old_off, db->slot_size);
        }
    }

    for (size_t i = 0; i < n; i++) free(st[i].old_buf);
    free(st);
    return 0;
}

/* ============================================================ Bulk lookup
 *
 * For multi-exists / multi-get on slotcask. Mirror of bulk_upsert /
 * bulk_delete: one kf rdlock per call (vs per-record in
 * slotcask_exists / slotcask_get), batched verify_stored_key sorted by
 * (sid, fid) so the segcache rdlock is held once per unique seg file
 * instead of once per record.
 *
 * Per-record state for both lookup and get phases. */
typedef struct {
    uint8_t  hash[16];
    uint8_t  sid;
    uint16_t fid;
    uint32_t off;
    uint8_t  kf_found;          /* 1 = kf_lookup_no_verify hit, 0 = miss */
    uint8_t  verified;          /* 1 = batched verify confirmed */
} SlotcaskBulkLookupState;

/* For each rec: sets rec->status = 0 if found+verified, -2 if not found,
   -1 on hard error. Reads no value bytes. */
int slotcask_bulk_lookup_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                      SlotcaskBulkRec *recs, size_t n) {
    if (n == 0) return 0;

    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[kf_shard_id] : NULL;
    if (kfcache_acquire_direct(&kh, kf_ref, kf_path,
                                db->slots_per_shard, db, kf_shard_id) != 0) return -1;

    SlotcaskBulkLookupState *st = calloc(n, sizeof(SlotcaskBulkLookupState));
    if (!st) { kfcache_release(&kh); return -1; }

    /* Phase 1: probe-only lookup under one rdlock. */
    for (size_t i = 0; i < n; i++) {
        SlotcaskBulkRec *r = &recs[i];
        r->status = 0;
        r->was_update = 0;
        if (r->klen > UINT16_MAX) { r->status = -1; continue; }
        compute_hash(r->key, r->klen, st[i].hash);
        uint8_t flag = 0;
        size_t slot;
        int rc = kf_lookup_no_verify(&kh, st[i].hash, &flag,
                                      &st[i].sid, &st[i].fid, &st[i].off, &slot);
        if (rc < 0) { r->status = -2; continue; }
        st[i].kf_found = 1;
    }
    kfcache_release(&kh);

    /* Phase 2: batched verify_stored_key — sort kf-hits by (sid, fid),
       take segcache rdlock once per unique file, verify each record
       under the held handle. Records that fail verify (hash collision
       on a different stored key) get status=-2. */
    int *vidx = malloc(n * sizeof(int));
    if (vidx) {
        int vcount = 0;
        for (size_t i = 0; i < n; i++) {
            if (recs[i].status == 0 && st[i].kf_found) vidx[vcount++] = (int)i;
        }
        for (int a = 1; a < vcount; a++) {
            int tmp = vidx[a];
            uint8_t  ta_sid = st[tmp].sid; uint16_t ta_fid = st[tmp].fid;
            int b = a - 1;
            while (b >= 0) {
                int bi = vidx[b];
                if (st[bi].sid < ta_sid ||
                    (st[bi].sid == ta_sid && st[bi].fid <= ta_fid)) break;
                vidx[b + 1] = vidx[b];
                b--;
            }
            vidx[b + 1] = tmp;
        }
        int k = 0;
        while (k < vcount) {
            int run_end = k + 1;
            uint8_t  sid = st[vidx[k]].sid;
            uint16_t fid = st[vidx[k]].fid;
            while (run_end < vcount &&
                   st[vidx[run_end]].sid == sid &&
                   st[vidx[run_end]].fid == fid)
                run_end++;
            char path[PATH_MAX];
            seg_path_for(path, db->data_dir, sid, fid);
            SlotcaskSegHandle h;
            SlotRef *seg_ref = seg_ref_for(db, (int)sid, (uint32_t)fid);
            if (segcache_acquire_direct(&h, seg_ref, path) != 0) {
                for (int j = k; j < run_end; j++) recs[vidx[j]].status = -1;
                k = run_end;
                continue;
            }
            for (int j = k; j < run_end; j++) {
                int i = vidx[j];
                SlotcaskBulkRec *r = &recs[i];
                const uint8_t *rec = h.map + st[i].off;
                if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) != 1) { r->status = -2; continue; }
                uint16_t k_stored = seg_rec_klen(rec);
                if (k_stored != r->klen) { r->status = -2; continue; }
                if (memcmp(rec + 24, r->key, r->klen) != 0) { r->status = -2; continue; }
                st[i].verified = 1;
            }
            segcache_release(&h);
            k = run_end;
        }
        free(vidx);
    } else {
        /* OOM fallback — per-record verify via the existing helper. */
        for (size_t i = 0; i < n; i++) {
            if (recs[i].status != 0 || !st[i].kf_found) continue;
            int km = verify_stored_key(db->data_dir, st[i].sid, st[i].fid,
                                        st[i].off, recs[i].key, recs[i].klen);
            if (km == 1) st[i].verified = 1;
            else recs[i].status = -2;
        }
    }

    free(st);
    return 0;
}

/* ============================================================ Two-phase bulk fetch */

/* Internal parallel_for_io worker for one segment file.
   Reads all records at their offsets, calls cb per live verified record. */
typedef struct {
    char                    path[PATH_MAX];
    SlotcaskResolvedRec    *recs;
    size_t                  count;
    void                   *ctx;
    SlotcaskScanCb          cb;
} SegFetchArg;


static void *seg_fetch_worker(void *arg) {
    SegFetchArg *fa = (SegFetchArg *)arg;
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, fa->path, 0, 0) != 0) return NULL;
    for (size_t i = 0; i < fa->count; i++) {
        const uint8_t *rec = h.map + fa->recs[i].off;
        if (!seg_rec_live_with_hash(rec, fa->recs[i].hash)) continue;
        uint16_t klen = seg_rec_klen(rec);
        uint32_t vlen = seg_rec_vlen(rec);
        if (fa->cb(fa->recs[i].hash, rec + 24, klen,
                     rec + 24 + klen, vlen, fa->ctx) != 0)
            break;
    }
    segcache_release(&h);
    return NULL;
}

/* qsort comparison: sort by (sid, fid). */
static int compare_sid_fid(const void *a, const void *b) {
    const SlotcaskResolvedRec *ra = (const SlotcaskResolvedRec *)a;
    const SlotcaskResolvedRec *rb = (const SlotcaskResolvedRec *)b;
    if (ra->sid != rb->sid) return (int)ra->sid - (int)rb->sid;
    return (int)ra->fid - (int)rb->fid;
}

/* Phase 1: resolve hashes to segment locations.
   Buckets by shard, probes each KF shard sequentially. */
SlotcaskResolvedRec *slotcask_bulk_resolve_hashes(SlotcaskDb *db,
                                                    const uint8_t (*hashes)[16],
                                                    size_t n,
                                                    size_t *out_n) {
    if (n == 0 || !out_n) { if (out_n) *out_n = 0; return NULL; }

    SlotcaskResolvedRec *resolved = calloc(n, sizeof(SlotcaskResolvedRec));
    if (!resolved) { *out_n = 0; return NULL; }

    size_t resolved_n = 0;
    int nshards = db->num_shards;

    /* Pass 1: count hashes per KF shard */
    size_t *per_shard_count = calloc((size_t)nshards, sizeof(size_t));
    if (!per_shard_count) { free(resolved); *out_n = 0; return NULL; }

    for (size_t i = 0; i < n; i++) {
        int sid = shard_for_hash(hashes[i], nshards);
        per_shard_count[sid]++;
    }

    /* Pass 2: allocate per-shard index arrays */
    size_t **per_shard_idxs = calloc((size_t)nshards, sizeof(size_t *));
    size_t *per_shard_pos   = calloc((size_t)nshards, sizeof(size_t));
    if (!per_shard_idxs || !per_shard_pos) {
        free(per_shard_count);
        free(per_shard_idxs);
        free(per_shard_pos);
        free(resolved);
        *out_n = 0;
        return NULL;
    }

    int alloc_ok = 1;
    for (int s = 0; s < nshards; s++) {
        if (per_shard_count[s] > 0) {
            per_shard_idxs[s] = malloc(per_shard_count[s] * sizeof(size_t));
            if (!per_shard_idxs[s]) { alloc_ok = 0; break; }
        }
    }
    if (!alloc_ok) {
        for (int s = 0; s < nshards; s++) free(per_shard_idxs[s]);
        free(per_shard_idxs);
        free(per_shard_pos);
        free(per_shard_count);
        free(resolved);
        *out_n = 0;
        return NULL;
    }

    /* Pass 3: fill per-shard index arrays */
    for (size_t i = 0; i < n; i++) {
        int sid = shard_for_hash(hashes[i], nshards);
        per_shard_idxs[sid][per_shard_pos[sid]++] = i;
    }

    /* Pass 4: for each non-empty shard, acquire KF, probe all its hashes */
    char kf_path[PATH_MAX];
    for (int s = 0; s < nshards; s++) {
        size_t cnt = per_shard_count[s];
        if (cnt == 0) continue;

        kf_path_for(kf_path, db->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0)
            continue;  /* skip shard on error */

        for (size_t j = 0; j < cnt; j++) {
            size_t hash_idx = per_shard_idxs[s][j];
            uint8_t flag = 0;
            uint8_t sid;
            uint16_t fid;
            uint32_t off;
            size_t slot;
            int rc = kf_lookup_no_verify(&kh, hashes[hash_idx], &flag,
                                          &sid, &fid, &off, &slot);
            if (rc == 0) {
                SlotcaskResolvedRec *rr = &resolved[resolved_n++];
                memcpy(rr->hash, hashes[hash_idx], 16);
                rr->sid = sid;
                rr->fid = fid;
                rr->off = off;
            }
        }
        kfcache_release(&kh);
    }

    /* Cleanup per-shard structures */
    for (int s = 0; s < nshards; s++) free(per_shard_idxs[s]);
    free(per_shard_idxs);
    free(per_shard_pos);
    free(per_shard_count);

    *out_n = resolved_n;
    if (resolved_n == 0) { free(resolved); return NULL; }
    return resolved;
}

/* Phase 2: fetch records from pre-resolved locations.
   Groups by (sid, fid), dispatches parallel_for_io across unique segment files. */
int slotcask_bulk_fetch_resolved(SlotcaskDb *db,
                                  const SlotcaskResolvedRec *recs,
                                  size_t n,
                                  void *ctx,
                                  SlotcaskScanCb cb) {
    if (n == 0 || !cb) return 0;

    /* Sort a copy by (sid, fid) */
    SlotcaskResolvedRec *sorted = malloc(n * sizeof(SlotcaskResolvedRec));
    if (!sorted) return -1;
    memcpy(sorted, recs, n * sizeof(SlotcaskResolvedRec));
    qsort(sorted, n, sizeof(SlotcaskResolvedRec), compare_sid_fid);

    /* Count unique (sid, fid) pairs */
    int nfiles = 1;
    for (size_t i = 1; i < n; i++) {
        if (sorted[i].sid != sorted[i-1].sid ||
            sorted[i].fid != sorted[i-1].fid)
            nfiles++;
    }

    /* Build SegFetchArg array — one per unique segment file */
    SegFetchArg *args = calloc((size_t)nfiles, sizeof(SegFetchArg));
    if (!args) { free(sorted); return -1; }

    int fi = 0;
    size_t run_start = 0;
    for (size_t i = 0; i < n; i++) {
        int last = (i == n - 1);
        if (!last && sorted[i].sid == sorted[i+1].sid &&
                     sorted[i].fid == sorted[i+1].fid)
            continue;

        seg_path_for(args[fi].path, db->data_dir,
                     sorted[run_start].sid, sorted[run_start].fid);
        args[fi].recs  = &sorted[run_start];
        args[fi].count = i - run_start + 1;
        args[fi].ctx   = ctx;
        args[fi].cb    = cb;
        fi++;
        run_start = i + 1;
    }

    /* Dispatch: sequential for few files, parallel for many */
    if (nfiles <= 3) {
        for (int i = 0; i < nfiles; i++)
            seg_fetch_worker(&args[i]);
    } else {
        parallel_for_io(seg_fetch_worker, args, nfiles, sizeof(SegFetchArg));
    }

    free(args);
    free(sorted);
    return 0;
}

/* Combined resolve + fetch. */
int slotcask_bulk_resolve_and_fetch(SlotcaskDb *db,
                                     const uint8_t (*hashes)[16],
                                     size_t n,
                                     void *ctx,
                                     SlotcaskScanCb cb) {
    size_t resolved_n = 0;
    SlotcaskResolvedRec *resolved = slotcask_bulk_resolve_hashes(db, hashes, n, &resolved_n);
    if (!resolved || resolved_n == 0) return 0;
    int rc = slotcask_bulk_fetch_resolved(db, resolved, resolved_n, ctx, cb);
    free(resolved);
    return rc;
}

int slotcask_delete_with_hooks(SlotcaskDb *db,
                                const void *key, size_t klen,
                                const SlotcaskDeleteOpts *opts,
                                SlotcaskDeleteResult *result) {
    if (result) {
        result->not_found = 0;
        result->condition_not_met = 0;
        result->current_value = NULL;
        result->current_vlen = 0;
    }
    if (klen > UINT16_MAX) return -1;
    SlotcaskDeleteOpts blank = {0};
    if (!opts) opts = &blank;

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);

    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) return -1;

    /* kf_lookup_with_slot captures the kf entry's slot index so the
       commit phase below can flip the flag directly via
       kf_tombstone_at_slot — skips the second probe + verify_stored_key
       that the original kf_tombstone call did under the same held
       wrlock. Same fix already applied to the bulk primitive. */
    uint8_t old_flag = 0, old_sid = 0;
    uint16_t old_fid = 0;
    uint32_t old_off = 0;
    size_t   kf_slot = 0;
    int found = (kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                       &old_flag, &old_sid, &old_fid, &old_off,
                                       &kf_slot) == 0);

    if (!found) {
        kfcache_release(&kh);
        if (result) result->not_found = 1;
        return -2;
    }

    /* Read OLD value unless caller explicitly opts out via skip_old_read.
       check needs OLD by definition (it inspects the record). pre_commit
       might or might not — caller signals via skip_old_read. Default
       behavior (flag = 0) reads OLD whenever any hook is set, matching
       the original contract. Saves one segcache_acquire + 100B memcpy +
       malloc/free pair per call when set on non-indexed delete. */
    int needs_old = (opts->check != NULL) ||
                    (opts->pre_commit != NULL && !opts->skip_old_read);
    uint8_t *old_buf = NULL;
    size_t   old_vlen = 0;
    if (needs_old) {
        if (read_record_value(db, old_sid, old_fid, old_off, key, klen,
                              &old_buf, &old_vlen) != 0) {
            kfcache_release(&kh);
            return -1;
        }
    }
    SlotcaskOldRecord old_rec = { old_buf, old_vlen };

    if (opts->check && opts->check(&old_rec, opts->check_ctx) == 0) {
        kfcache_release(&kh);
        if (result) {
            result->condition_not_met = 1;
            result->current_value = old_buf;
            result->current_vlen = old_vlen;
        } else {
            free(old_buf);
        }
        return -2;
    }

    /* Publish (shard, slot) for index hooks that key by physical location
       (bitmap). Done before pre_commit so the bitmap branch of
       update_idx_fn can address the slot we're about to tombstone. */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = (uint32_t)kf_slot;

    if (opts->pre_commit) {
        int rc = opts->pre_commit(needs_old ? &old_rec : NULL,
                                   opts->pre_commit_ctx);
        if (rc != 0) {
            kfcache_release(&kh);
            free(old_buf);
            return -1;
        }
    }

    /* Direct flag flip at the captured slot — no probe, no verify. */
    kf_tombstone_at_slot(&kh, kf_slot);
    kfcache_release(&kh);

    slotcask_tombstone_and_push_back(db, old_sid, old_fid, old_off);
    free(old_buf);
    return 0;
}

/* ============================================================ Registry */
/* RegEntry + SLOTCASK_REG_BUCKETS moved to shard_db_internal.h; g_reg* moved to ShardDb struct */

static void reg_key(char out[PATH_MAX], const char *effective_root,
                    const char *object) {
    snprintf(out, PATH_MAX, "%s:%s", effective_root, object);
}

/* Linear probe from path_hash. Returns the matching bucket (used=1, key matches),
   or the first empty bucket (used=0) suitable for insertion. -1 if the table is
   completely full. */
static int reg_probe(const char *key) {
    uint32_t h = path_hash(key);
    int idx = (int)(h % SLOTCASK_REG_BUCKETS);
    for (int i = 0; i < SLOTCASK_REG_BUCKETS; i++) {
        int s = (idx + i) % SLOTCASK_REG_BUCKETS;
        if (!g_reg[s].used) return s;
        if (strcmp(g_reg[s].key, key) == 0) return s;
    }
    return -1;
}

SlotcaskDb *slotcask_registry_get(const char *effective_root,
                                  const char *object,
                                  const SlotcaskSchemaInfo *info) {
    if (!info) return NULL;
    if (info->splits <= 0 || info->slot_size <= 0 || info->streams <= 0)
        return NULL;

    char key[PATH_MAX];
    reg_key(key, effective_root, object);

    /* Fast path: probe under the lock; hit returns immediately. */
    pthread_mutex_lock(&g_reg_lock);
    int slot = reg_probe(key);
    if (slot < 0) {
        pthread_mutex_unlock(&g_reg_lock);
        fprintf(stderr, "slotcask_registry: table full (%d buckets)\n",
                SLOTCASK_REG_BUCKETS);
        return NULL;
    }
    if (g_reg[slot].used) {
        SlotcaskDb *db = g_reg[slot].db;
        pthread_mutex_unlock(&g_reg_lock);
        return db;
    }
    pthread_mutex_unlock(&g_reg_lock);

    /* Miss — open OUTSIDE the registry lock.  slotcask_open mmaps 8 stream
       segments × 128 MiB and can take seconds on large objects (e.g.
       hn/comments at splits=256).  Holding g_reg_lock across that would
       block every other registry operation (other opens via warmup's
       parallel_for, drop-object's slotcask_registry_invalidate, any
       slotcask_registry_get caller in a query handler).  Pre-fix this
       caused drop-object to wait 7-12s under warmup contention.

       Race: two concurrent misses for the same key both reach this
       point.  Re-probe inside the install lock below; the loser frees
       its own SlotcaskDb and returns the winner's. */
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s", effective_root, object);

    SlotcaskDb *db = calloc(1, sizeof(SlotcaskDb));
    if (!db) return NULL;
    if (slotcask_open(db, data_dir, info->splits, info->streams,
                      info->slot_size) != 0) {
        free(db);
        fprintf(stderr, "slotcask_registry: open failed for %s/%s\n",
                effective_root, object);
        return NULL;
    }

    /* Install (or lose the race + free ours). */
    pthread_mutex_lock(&g_reg_lock);
    slot = reg_probe(key);
    if (slot < 0) {
        pthread_mutex_unlock(&g_reg_lock);
        slotcask_close(db);
        free(db);
        fprintf(stderr, "slotcask_registry: table full (%d buckets)\n",
                SLOTCASK_REG_BUCKETS);
        return NULL;
    }
    if (g_reg[slot].used) {
        /* Another thread installed while we were opening.  Discard ours
           and use theirs. */
        SlotcaskDb *winner = g_reg[slot].db;
        pthread_mutex_unlock(&g_reg_lock);
        slotcask_close(db);
        free(db);
        return winner;
    }
    snprintf(g_reg[slot].key, sizeof(g_reg[slot].key), "%s", key);
    g_reg[slot].db = db;
    g_reg[slot].used = 1;
    pthread_mutex_unlock(&g_reg_lock);
    return db;
}

void slotcask_registry_invalidate(const char *effective_root,
                                  const char *object) {
    char key[PATH_MAX];
    reg_key(key, effective_root, object);

    /* Drop cached kf + seg mmaps for this object's data_dir. Without this
       flush, rebuild_object_v2 (which renames the data files into
       .legacy/) would have a fresh slotcask_open hit a cached path entry
       and keep writing into the moved-away inodes. */
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/", effective_root, object);

    pthread_mutex_lock(&g_reg_lock);
    int slot = reg_probe(key);
    if (slot >= 0 && g_reg[slot].used) {
        SlotcaskDb *db = g_reg[slot].db;
        g_reg[slot].used = 0;
        g_reg[slot].key[0] = '\0';
        g_reg[slot].db = NULL;
        pthread_mutex_unlock(&g_reg_lock);
        slotcask_close(db);
        free(db);
        kfcache_invalidate_prefix(data_dir);
        segcache_invalidate_prefix(data_dir);
        return;
    }
    pthread_mutex_unlock(&g_reg_lock);
    /* No registry entry — still flush any cache entries that linger from
       earlier opens (e.g. cmd_create_object opens + closes a SlotcaskDb
       directly without registering it). */
    kfcache_invalidate_prefix(data_dir);
    segcache_invalidate_prefix(data_dir);
}

void slotcask_registry_shutdown(void) {
    pthread_mutex_lock(&g_reg_lock);
    for (int i = 0; i < SLOTCASK_REG_BUCKETS; i++) {
        if (g_reg[i].used && g_reg[i].db) {
            slotcask_close(g_reg[i].db);
            free(g_reg[i].db);
            g_reg[i].db = NULL;
            g_reg[i].used = 0;
            g_reg[i].key[0] = '\0';
        }
    }
    pthread_mutex_unlock(&g_reg_lock);
}

/* ============================================================ Query primitives
 *
 * Phase 3A: walk_live + lookup_by_hash. Both feed the query layer's scan
 * primitives (find/count/aggregate/keys/fetch + index-driven access).
 */

/* Walk one keyfile shard. For each flag=1 entry, follow the pointer to the
   segment, hold the segcache rdlock, invoke cb. cb returning 1 stops. */
/* Per-record reference used by walk_one_shard's batched scan. */
typedef struct {
    uint32_t kf_idx;     /* index into kf array (for hash + flag verify) */
    uint8_t  sid;
    uint16_t fid;
    uint32_t offset;
} WalkRecRef;

static int walk_recref_cmp(const void *a, const void *b) {
    const WalkRecRef *ra = a, *rb = b;
    if (ra->sid != rb->sid) return (int)ra->sid - (int)rb->sid;
    if (ra->fid != rb->fid) return (int)ra->fid - (int)rb->fid;
    if (ra->offset < rb->offset) return -1;
    if (ra->offset > rb->offset) return 1;
    return 0;
}

static int walk_one_shard_inner(SlotcaskDb *db, int kf_shard_id,
                                 SlotcaskScanCb cb, void *ctx,
                                 int *stop_flag);

static int walk_one_shard(SlotcaskDb *db, int kf_shard_id,
                          SlotcaskScanCb cb, void *ctx) {
    return walk_one_shard_inner(db, kf_shard_id, cb, ctx, NULL);
}

static int walk_one_shard_inner(SlotcaskDb *db, int kf_shard_id,
                                 SlotcaskScanCb cb, void *ctx,
                                 int *stop_flag) {
    /* Cheap stop check before any allocation — early-exit queries
       (limit-bounded find, etc.) win the most from this. */
    if (stop_flag && __atomic_load_n(stop_flag, __ATOMIC_ACQUIRE)) return 0;

    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) return -1;

    /* Cold-walk readahead hint: pass 1 reads the entire kf shard
       sequentially (slot 0..cap). The default kernel heuristic faults
       4-8 KB at a time; MADV_SEQUENTIAL switches to 128 KB+ readahead
       I/Os so a cold 100-300 MB kf shard streams in instead of stalling
       on per-page disk seeks. Restored to MADV_NORMAL on every exit so
       concurrent point lookups against the same cached kf (via the kf
       cache LRU) don't pay readahead they don't want. Mirrors the btree
       MADV_SEQUENTIAL fix that won 10× cold on sum/avg in PR #33. */
    int kf_set_seq = (kh.hdr && kh.map_size > 0 &&
                      madvise(kh.hdr, kh.map_size, MADV_SEQUENTIAL) == 0);

    size_t cap = kh.capacity;
    SlotcaskKfEntry *kf = kh.map;

    /* Size the refs buffer by live count (= header.total - header.deleted),
       not full slot capacity. For typical data the live count is tiny
       compared to cap (e.g., 15K live in a 256K-slot shard at low load),
       so this saves ~16× memory + the kernel page-fault tail. */
    size_t alloc_n = cap;
    if (kh.hdr) {
        uint64_t live = kh.hdr->total > kh.hdr->deleted
                          ? kh.hdr->total - kh.hdr->deleted : 0;
        if (live > 0 && live < (uint64_t)cap) alloc_n = (size_t)live;
    }

    /* Pass 1: collect live entries' (sid, fid, offset) refs. */
    WalkRecRef *refs = malloc(alloc_n * sizeof(WalkRecRef));
    if (!refs) {
        /* Allocation failure → fall back to per-record acquire. Slower
           but correct. */
        int stop = 0;
        for (size_t i = 0; i < cap && !stop; i++) {
            SlotcaskKfEntry *e = &kf[i];
            uint8_t flag = __atomic_load_n(&e->flag, __ATOMIC_ACQUIRE);
            if (flag != 1) continue;
            char seg_path[PATH_MAX];
            seg_path_for(seg_path, db->data_dir, e->stream_id, e->file_id);
            SlotcaskSegHandle sh;
            if (segcache_acquire(&sh, seg_path, 0, 0) != 0) continue;
            const uint8_t *rec = sh.map + e->offset;
            if (seg_rec_live_with_hash(rec, e->hash)) {
                uint16_t klen = seg_rec_klen(rec);
                uint32_t vlen = seg_rec_vlen(rec);
                if (cb(e->hash, rec + 24, klen, rec + 24 + klen, vlen, ctx) != 0)
                    stop = 1;
            }
            segcache_release(&sh);
        }
        if (kf_set_seq) madvise(kh.hdr, kh.map_size, MADV_NORMAL);
        kfcache_release(&kh);
        return stop;
    }
    size_t nrefs = 0;
    int sf_check = 0;
    for (size_t i = 0; i < cap; i++) {
        /* Periodic stop_flag check so concurrent workers' early-exit
           propagates: another shard finds enough matches and sets the
           shared flag, this worker bails without finishing pass 1.
           Check every 256 iterations (was 4096) — at 256K-slot kf shards
           the old cadence let a worker finish 4096 wasted slot-probes
           after another shard already collected the limit, adding ~5-7ms
           to limit-bound queries (KEYS first 100, FIND limit 10). The
           atomic_load is cheap (~1ns) and amortised across 256 iters. */
        if (stop_flag && (++sf_check & 0xFF) == 0 &&
            __atomic_load_n(stop_flag, __ATOMIC_ACQUIRE)) {
            free(refs);
            if (kf_set_seq) madvise(kh.hdr, kh.map_size, MADV_NORMAL);
            kfcache_release(&kh);
            return 0;
        }
        SlotcaskKfEntry *e = &kf[i];
        uint8_t flag = __atomic_load_n(&e->flag, __ATOMIC_ACQUIRE);
        if (flag != 1) continue;
        /* live count is a stat snapshot; concurrent inserts can grow it
           past alloc_n. Bound writes to alloc_n; surplus entries fall
           back to the un-batched per-record acquire loop below. */
        if (nrefs >= alloc_n) {
            /* Edge case: more live entries than the header reported.
               Walk the remaining slots inline (slow path) so correctness
               isn't tied to the live-count snapshot. */
            for (; i < cap; i++) {
                e = &kf[i];
                flag = __atomic_load_n(&e->flag, __ATOMIC_ACQUIRE);
                if (flag != 1) continue;
                /* Grow BEFORE writing — entering this slow path means
                   nrefs == alloc_n already, so the previous "write
                   then check" order overran refs[alloc_n] by one
                   element on the first iteration (Coverity CID 1693842).
                   Bound = max(nrefs+1, alloc_n + remaining slots) so
                   we can comfortably hold every remaining live entry
                   without re-growing. */
                if (nrefs >= alloc_n) {
                    size_t new_n = alloc_n + (cap - i) + 1;
                    WalkRecRef *grown = realloc(refs, new_n * sizeof(WalkRecRef));
                    if (!grown) goto done_collect;
                    refs = grown;
                    alloc_n = new_n;
                }
                refs[nrefs].kf_idx = (uint32_t)i;
                refs[nrefs].sid    = e->stream_id;
                refs[nrefs].fid    = e->file_id;
                refs[nrefs].offset = e->offset;
                nrefs++;
            }
            break;
        }
        refs[nrefs].kf_idx = (uint32_t)i;
        refs[nrefs].sid    = e->stream_id;
        refs[nrefs].fid    = e->file_id;
        refs[nrefs].offset = e->offset;
        nrefs++;
    }
done_collect:
    ;

    /* Pass 2: sort by (sid, fid, offset). Same-segment entries become
       consecutive so we acquire/release each segment once instead of N
       times — replaces ~N calls into the segcache (each contending on
       g_segcache_lock) with ~num_segments calls. The sort cost
       (n log n × ~30 ns) is dwarfed by the lock-acquire savings. */
    qsort(refs, nrefs, sizeof(WalkRecRef), walk_recref_cmp);

    /* Pass 3: walk sorted refs, holding one segment handle across runs
       of records sharing (sid, fid). */
    int stop = 0;
    SlotcaskSegHandle sh = { .slot = -1 };
    int held_sid = -1, held_fid = -1;
    int sf_check2 = 0;
    for (size_t i = 0; i < nrefs && !stop; i++) {
        /* Periodic shared-stop check propagates one worker's early-exit
           (e.g., limit-bounded find finding its 10th match) to siblings. */
        if (stop_flag && (++sf_check2 & 0xFF) == 0 &&
            __atomic_load_n(stop_flag, __ATOMIC_ACQUIRE)) {
            stop = 1; break;
        }
        WalkRecRef *r = &refs[i];
        if ((int)r->sid != held_sid || (int)r->fid != held_fid) {
            if (sh.slot >= 0 || sh.fd >= 0) segcache_release(&sh);
            sh.slot = -1; sh.fd = -1;
            char seg_path[PATH_MAX];
            seg_path_for(seg_path, db->data_dir, r->sid, r->fid);
            if (segcache_acquire(&sh, seg_path, 0, 0) != 0) {
                held_sid = held_fid = -1;
                continue;
            }
            held_sid = r->sid;
            held_fid = r->fid;
            /* Prefetch hint for the run we're about to walk: refs are
               sorted by (sid, fid, offset), so the next consecutive
               refs with the same (sid, fid) cover a known byte range
               in this segment. madvise(WILLNEED) tells the kernel to
               start reading those pages from disk before we touch
               them — turns synchronous page faults during the walk
               into background prefetches. Cold-cache full scans at
               25M+ benefit most (3-10× win in our bench when the
               working set exceeds page cache). */
            uint32_t lo = r->offset;
            uint32_t hi = r->offset;
            for (size_t j = i + 1; j < nrefs; j++) {
                if (refs[j].sid != r->sid || refs[j].fid != r->fid) break;
                if (refs[j].offset > hi) hi = refs[j].offset;
            }
            /* Round to page boundary at the low end; pad slot_size at
               the high end so the last record's bytes are included. */
            size_t pg = 4096;
            uintptr_t lo_aligned = (uintptr_t)(sh.map + lo) & ~(uintptr_t)(pg - 1);
            uintptr_t hi_aligned = ((uintptr_t)(sh.map + hi) + (size_t)db->slot_size + pg - 1)
                                    & ~(uintptr_t)(pg - 1);
            if (hi_aligned > lo_aligned)
                madvise((void *)lo_aligned, hi_aligned - lo_aligned, MADV_WILLNEED);
        }
        SlotcaskKfEntry *e = &kf[r->kf_idx];
        const uint8_t *rec = sh.map + r->offset;
        if (!seg_rec_live_with_hash(rec, e->hash)) {
            /* Stale-pointer race: another writer repointed mid-walk. */
            continue;
        }
        uint16_t klen = seg_rec_klen(rec);
        uint32_t vlen = seg_rec_vlen(rec);
        if (cb(e->hash, rec + 24, klen, rec + 24 + klen, vlen, ctx) != 0)
            stop = 1;
    }
    if (sh.slot >= 0 || sh.fd >= 0) segcache_release(&sh);
    free(refs);
    if (kf_set_seq) madvise(kh.hdr, kh.map_size, MADV_NORMAL);
    kfcache_release(&kh);
    return stop;
}

typedef struct {
    SlotcaskDb     *db;
    int             kf_shard_id;
    SlotcaskScanCb  cb;
    void           *ctx;
    int            *stop_flag;
} WalkWorkerArg;

static void *walk_worker(void *arg) {
    WalkWorkerArg *w = (WalkWorkerArg *)arg;
    if (__atomic_load_n(w->stop_flag, __ATOMIC_ACQUIRE)) return NULL;
    int stop = walk_one_shard(w->db, w->kf_shard_id, w->cb, w->ctx);
    if (stop) __atomic_store_n(w->stop_flag, 1, __ATOMIC_RELEASE);
    return NULL;
}

int slotcask_walk_live(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return -1;
    /* Sequential walk. Parallelism is the engine's job (it knows about
       thread-local output streams and other engine-side state); the
       storage primitive just exposes a per-shard walker. See
       slotcask_walk_one_shard for the per-shard entry point used by the
       engine's parallel scan_shards_v2. */
    int stop_flag = 0;
    for (int s = 0; s < db->num_shards; s++) {
        if (__atomic_load_n(&stop_flag, __ATOMIC_ACQUIRE)) break;
        WalkWorkerArg arg = {
            .db = db, .kf_shard_id = s,
            .cb = cb, .ctx = ctx,
            .stop_flag = &stop_flag,
        };
        walk_worker(&arg);
    }
    return 0;
}

/* Walk one kf shard's live entries into `cb`. Same semantics as
   slotcask_walk_live but scoped to a single shard so the engine can
   parallelise across shards (with g_out propagation, etc.). The shared
   `stop_flag` (uint8_t in caller's scope) preserves the early-cb-return
   semantics across parallel workers. */
int slotcask_walk_one_shard(SlotcaskDb *db, int kf_shard_id,
                             SlotcaskScanCb cb, void *ctx,
                             int *stop_flag) {
    if (!db || !cb || kf_shard_id < 0 || kf_shard_id >= db->num_shards)
        return -1;
    return walk_one_shard_inner(db, kf_shard_id, cb, ctx, stop_flag);
}

/* Slot-aware walker: iterates kf entries in slot order, calling cb per
   live entry with the slot index alongside the usual (hash, key, value).
   Used by the bitmap-index reindex path. Single-threaded — no fan-out;
   reindex callers typically already parallel_for over shards externally. */
int slotcask_walk_one_shard_slots(SlotcaskDb *db, int kf_shard_id,
                                   SlotcaskScanSlotCb cb, void *ctx) {
    if (!db || !cb || kf_shard_id < 0 || kf_shard_id >= db->num_shards)
        return -1;
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) return -1;

    int rc = 0;
    for (size_t s = 0; s < kh.capacity; s++) {
        SlotcaskKfEntry *e = &kh.map[s];
        if (e->flag != 1) continue;
        /* read_record_value verifies the key matches; for reindex we
           trust the kf entry's pointer (kf is authoritative), so pass
           the kf entry's known-good record header. We need the key to
           call read_record_value, so we read the seg's key-prefix
           first via a small inline buffer. */
        uint8_t *vbuf = NULL;
        size_t   vlen = 0;
        /* The seg record header is 24B: 16B hash + 2B klen + 1B flag +
           1B reserved + 4B vlen. The key starts at offset+24. We read
           klen first, then call read_record_value with the in-seg key
           bytes. To avoid that two-step, lean on segcache_acquire +
           direct mmap read for simplicity. */
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, e->stream_id, e->file_id);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0) != 0) continue;
        const uint8_t *base = (const uint8_t *)h.map + (size_t)e->offset;
        if ((size_t)e->offset + 24 > h.map_size) { segcache_release(&h); continue; }
        uint16_t klen_be = (uint16_t)base[16] | ((uint16_t)base[17] << 8);
        uint32_t vlen_be = (uint32_t)base[20] | ((uint32_t)base[21] << 8)
                         | ((uint32_t)base[22] << 16) | ((uint32_t)base[23] << 24);
        if ((size_t)e->offset + 24 + klen_be + vlen_be > h.map_size) {
            segcache_release(&h); continue;
        }
        const uint8_t *key_p = base + 24;
        const uint8_t *val_p = base + 24 + klen_be;
        int crc = cb((uint32_t)s, e->hash, key_p, klen_be, val_p, vlen_be, ctx);
        segcache_release(&h);
        (void)vbuf; (void)vlen;
        if (crc != 0) { rc = crc; break; }
    }

    kfcache_release(&kh);
    return rc;
}

/* Streaming per-shard walker — fires cb() per record as kf is scanned, no
   Pass-1 ref-buffer. Better than slotcask_walk_one_shard for limit-bound
   queries: cb's stop signal propagates immediately (next iteration), so
   workers don't waste time collecting refs that Pass 2 would never read.

   Trades the per-segment-batched acquire optimisation (sort refs by
   sid+fid + acquire each seg once per shard) for per-record acquire
   (~50ns extra per record). For a 100-record limit across 64 shards
   that's ~150K records skipped × 0ns saved (we never read them) vs
   the full-scan path's ~15K records read × ~50ns saved per shard. */
int slotcask_walk_one_shard_streaming(SlotcaskDb *db, int kf_shard_id,
                                       SlotcaskScanCb cb, void *ctx,
                                       int *stop_flag) {
    if (!db || !cb || kf_shard_id < 0 || kf_shard_id >= db->num_shards)
        return -1;

    /* Cheap stop-flag check before any allocation. */
    if (stop_flag && __atomic_load_n(stop_flag, __ATOMIC_ACQUIRE)) return 0;

    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) return 0;

    /* Same cold-walk readahead hint as the buffered walker — see
       walk_one_shard_inner for the rationale. */
    int kf_set_seq = (kh.hdr && kh.map_size > 0 &&
                      madvise(kh.hdr, kh.map_size, MADV_SEQUENTIAL) == 0);

    size_t cap = kh.capacity;
    SlotcaskKfEntry *kf = kh.map;
    int stop = 0;
    int sf_check = 0;
    for (size_t i = 0; i < cap && !stop; i++) {
        /* Stop-flag check every 256 iterations — same cadence as the
           buffered walker's Pass-1 stop check. */
        if (stop_flag && (++sf_check & 0xFF) == 0 &&
            __atomic_load_n(stop_flag, __ATOMIC_ACQUIRE)) {
            break;
        }
        SlotcaskKfEntry *e = &kf[i];
        uint8_t flag = __atomic_load_n(&e->flag, __ATOMIC_ACQUIRE);
        if (flag != 1) continue;

        char seg_path[PATH_MAX];
        seg_path_for(seg_path, db->data_dir, e->stream_id, e->file_id);
        SlotcaskSegHandle sh;
        if (segcache_acquire(&sh, seg_path, 0, 0) != 0) continue;
        const uint8_t *rec = sh.map + e->offset;
        if (seg_rec_live_with_hash(rec, e->hash)) {
            uint16_t klen = seg_rec_klen(rec);
            uint32_t vlen = seg_rec_vlen(rec);
            if (cb(e->hash, rec + 24, klen, rec + 24 + klen, vlen, ctx) != 0)
                stop = 1;
        }
        segcache_release(&sh);
    }
    if (kf_set_seq) madvise(kh.hdr, kh.map_size, MADV_NORMAL);
    kfcache_release(&kh);
    return 0;
}

/* Count-only variant — sums live kf entries (flag=1) across every kf
   shard without touching the segcache. Pure mmap reads of 24-byte kf
   entries. Used by cmd_recount on v2 — was paying full segcache_acquire
   + 100B value memcpy per record (1M records → ~270ms) just to bump a
   counter; this is ~5ms on the same dataset. */
int64_t slotcask_count_live(SlotcaskDb *db) {
    if (!db) return 0;
    int64_t total = 0;
    for (int s = 0; s < db->num_shards; s++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) continue;
        size_t cap = kh.capacity;
        SlotcaskKfEntry *kf = kh.map;
        for (size_t i = 0; i < cap; i++) {
            uint8_t flag = __atomic_load_n(&kf[i].flag, __ATOMIC_ACQUIRE);
            if (flag == 1) total++;
        }
        kfcache_release(&kh);
    }
    return total;
}

/* Skip-aware variant — walks all kf shards but skips the first `skip_n`
   live entries WITHOUT touching the segcache. Used by cmd_fetch with
   offset/cursor: we need to advance past N records before emitting, and
   loading their value bytes just to throw them away is ~100ns of wasted
   work per skipped record (segcache_acquire + 100B header read + release).
   At offset=5000 that's ~500µs of pure overhead — was the headline
   regression vs v1 fetch with offset.

   Concurrent-writer caveat: a kf entry with flag=1 here could be racing a
   repoint that hasn't yet flipped on the new seg. We count it as live
   anyway (matches what walk_one_shard would have decided). For the
   emit phase we still verify rec[18]==1 and hash match — concurrent
   repoints get skipped correctly there. The skip count can be off by
   a tiny amount under heavy concurrent writes; cursor pagination has
   never promised exact resume across writes. */
/* O_DIRECT sequential scan of a KF shard file. Reads entries in chunks of
   12 MB (aligned to both ODIRECT_ALIGN=4096 and sizeof(SlotcaskKfEntry)=24).
   Calls cb(entry, ctx) for every entry including empty and tombstoned ones —
   the caller decides which flags to act on. Stops early if cb returns != 0. */
#define KF_OD_BUF_SIZE (12 * 1024 * 1024)  /* 12 MB: lcm(4096,24)*1024 */
static int kf_scan_o_direct(const char *kf_path,
                              int (*cb)(const SlotcaskKfEntry *, void *),
                              void *ctx) {
    int fd = od_open(kf_path);
    if (fd < 0) return -1;
    uint8_t *buf = aligned_alloc(ODIRECT_ALIGN, KF_OD_BUF_SIZE);
    if (!buf) { close(fd); return -1; }

    off_t file_off = 0;
    int stopped = 0;
    while (!stopped) {
        ssize_t nr = od_pread(fd, buf, KF_OD_BUF_SIZE, file_off);
        if (nr <= 0) break;
        /* First chunk: skip the 24-byte KF file header. */
        size_t start = (file_off == 0) ? SLOTCASK_KF_HDR_SIZE : 0;
        for (size_t off = start;
             off + sizeof(SlotcaskKfEntry) <= (size_t)nr && !stopped;
             off += sizeof(SlotcaskKfEntry)) {
            if (cb((const SlotcaskKfEntry *)(buf + off), ctx) != 0)
                stopped = 1;
        }
        file_off += (off_t)nr;
    }

    free(buf);
    close(fd);
    return 0;
}

/* Callback context for slotcask_walk_live_skip's O_DIRECT KF scan. */
typedef struct {
    SlotcaskDb    *db;
    int64_t        remaining_skip;
    int            stop;
    SlotcaskScanCb cb;
    void          *ctx;
} KfOdSkipCtx;

static int kf_od_skip_emit_cb(const SlotcaskKfEntry *e, void *raw) {
    KfOdSkipCtx *c = (KfOdSkipCtx *)raw;
    if (c->stop) return 1;
    if (e->flag != 1) return 0;

    /* Cheap skip: count live entries without touching segments. */
    if (c->remaining_skip > 0) { c->remaining_skip--; return 0; }

    /* Past the skip window — load the segment record and emit. */
    char seg_path[PATH_MAX];
    seg_path_for(seg_path, c->db->data_dir, e->stream_id, e->file_id);
    SlotcaskSegHandle sh;
    if (segcache_acquire(&sh, seg_path, 0, 0) != 0) return 0;
    const uint8_t *rec = sh.map + e->offset;
    if (!seg_rec_live_with_hash(rec, e->hash)) {
        segcache_release(&sh);
        return 0;
    }
    uint16_t klen = seg_rec_klen(rec);
    uint32_t vlen = seg_rec_vlen(rec);
    const uint8_t *key   = rec + 24;
    const uint8_t *value = rec + 24 + klen;
    if (c->cb(e->hash, key, klen, value, vlen, c->ctx) != 0) {
        c->stop = 1;
        segcache_release(&sh);
        return 1;
    }
    segcache_release(&sh);
    return 0;
}

int slotcask_walk_live_skip(SlotcaskDb *db, int64_t skip_n,
                              SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return -1;
    KfOdSkipCtx c = {
        .db = db, .remaining_skip = skip_n, .stop = 0, .cb = cb, .ctx = ctx
    };
    for (int s = 0; s < db->num_shards && !c.stop; s++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, s);
        kf_scan_o_direct(kf_path, kf_od_skip_emit_cb, &c);
    }
    return 0;
}

int slotcask_lookup_by_hash(SlotcaskDb *db, const uint8_t hash16[16],
                            SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return -1;
    int sid_kf = shard_for_hash(hash16, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) return -1;

    size_t cap = kh.capacity;
    SlotcaskKfEntry *kf = kh.map;
    size_t start = kf_slot_for(hash16, cap);
    int stop = 0;
    for (size_t i = 0; i < cap && !stop; i++) {
        size_t slot = (start + i) % cap;
        SlotcaskKfEntry *e = &kf[slot];
        uint8_t flag = __atomic_load_n(&e->flag, __ATOMIC_ACQUIRE);
        if (flag == 0) break;                      /* probe end */
        if (flag != 1) continue;                    /* tombstone */
        if (memcmp(e->hash, hash16, 16) != 0) continue;

        char seg_path[PATH_MAX];
        seg_path_for(seg_path, db->data_dir, e->stream_id, e->file_id);
        SlotcaskSegHandle sh;
        if (segcache_acquire(&sh, seg_path, 0, 0) != 0) continue;
        const uint8_t *rec = sh.map + e->offset;
        if (!seg_rec_live_with_hash(rec, hash16)) {
            segcache_release(&sh);
            continue;
        }
        uint16_t klen = seg_rec_klen(rec);
        uint32_t vlen = seg_rec_vlen(rec);
        const uint8_t *key   = rec + 24;
        const uint8_t *value = rec + 24 + klen;
        if (cb(e->hash, key, klen, value, vlen, ctx) != 0) stop = 1;
        segcache_release(&sh);
    }
    kfcache_release(&kh);
    return 0;
}

int kf_find_slot_for_hash(const SlotcaskDb *db,
                           const uint8_t hash16[16],
                           uint32_t *out_slot) {
    if (!db || !out_slot) return -1;
    int sid_kf = shard_for_hash(hash16, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) return -1;
    size_t cap = kh.capacity;
    SlotcaskKfEntry *kf = kh.map;
    size_t start = kf_slot_for(hash16, cap);
    int found = -1;
    for (size_t i = 0; i < cap; i++) {
        size_t slot = (start + i) % cap;
        SlotcaskKfEntry *e = &kf[slot];
        uint8_t flag = __atomic_load_n(&e->flag, __ATOMIC_ACQUIRE);
        if (flag == 0) break;
        if (flag != 1) continue;
        if (memcmp(e->hash, hash16, 16) != 0) continue;
        *out_slot = (uint32_t)slot;
        found = 0;
        break;
    }
    kfcache_release(&kh);
    return found;
}

/* ============================================================ Compaction
 *
 * Direction-C seg compaction. Pair-merges sparse non-active seg files within
 * a stream. Donor's live records get migrated into recipient's tombstone
 * holes (or post-watermark unused slots), kf entries are repointed at the
 * commit boundary, and the donor file is unlinked. The active seg of each
 * stream is never touched.
 *
 * Per-record migration protocol (one record at a time):
 *   1. write recipient slot (header bytes, payload, fence, flag=1 last).
 *      Crash here = orphan flag=1 in recipient with no kf reference; cleaned
 *      by the next compaction or rebuild.
 *   2. kf wrlock + kf_lookup_with_slot to find the kf entry.
 *   3. Verify kf still points at (donor_fid, donor_off). If repointed by
 *      an interleaved op (shouldn't happen under objlock_wrlock but harmless
 *      to check), skip. The recipient slot becomes another orphan.
 *   4. kf_repoint_at_slot(recipient_fid, target_off) — atomic 8B store, the
 *      commit point for this record.
 *   5. release kf wrlock. Donor slot stays flag=1; the file is unlinked
 *      wholesale at the end. Stale readers reading donor pre-unlink see
 *      correct bytes; segcache_invalidate_prefix at the unlink step
 *      drains them via the per-entry rwlock.
 *
 * Caller invariants:
 *   - objlock_wrlock held for this object (no concurrent writes).
 *   - The light path of cmd_vacuum already runs under that lock. */

typedef struct {
    int      stream_id;
    uint32_t file_id;
    uint32_t total_slots;
    uint32_t live_count;
} SegStat;

static int seg_stat_cmp_live_asc(const void *a, const void *b) {
    const SegStat *x = (const SegStat *)a;
    const SegStat *y = (const SegStat *)b;
    if (x->live_count != y->live_count)
        return (x->live_count < y->live_count) ? -1 : 1;
    return 0;
}

/* Walk a non-active seg file; count flag==1 slots and capacity. */
static int seg_stat_one(SlotcaskDb *db, int stream_id, uint32_t file_id,
                        uint32_t *out_live, uint32_t *out_total) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0) != 0) return -1;
    size_t total = h.map_size / (size_t)db->slot_size;
    uint32_t live = 0;
    for (size_t s = 0; s < total; s++) {
        const uint8_t *rec = h.map + s * (size_t)db->slot_size;
        if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) == 1) live++;
    }
    segcache_release(&h);
    *out_live = live;
    *out_total = (uint32_t)total;
    return 0;
}

/* Variable-length variant: walk records by reading headers sequentially. */
static int seg_stat_one_varlen(SlotcaskDb *db, int stream_id, uint32_t file_id,
                               uint32_t *out_live, uint32_t *out_total) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0) != 0) return -1;

    size_t file_size = h.map_size;
    uint32_t live = 0, total = 0;
    size_t off = 0;

    while (off + 24 <= file_size) {
        const uint8_t *rec = h.map + off;
        uint16_t klen;
        uint32_t vlen;
        uint8_t flag;
        memcpy(&klen, rec + 16, 2);
        memcpy(&vlen, rec + 20, 4);
        flag = rec[18];
        size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
        if (off + rec_size > file_size) break;
        if (flag != 0) total++;
        if (flag == 1) live++;
        off += rec_size;
    }

    segcache_release(&h);
    *out_live = live;
    *out_total = total;
    return 0;
}

/* Filter a stream's free-pool: drop entries whose file_id matches `fid`.
   Called immediately after the donor file is unlinked so popping callers
   never see stale (file_id, offset) pairs. */
static void pool_drop_for_file(SlotcaskStream *p, uint16_t fid) {
    pthread_mutex_lock(&p->pool_lock);
    for (int b = 0; b < SLOTCASK_POOL_BUCKETS; b++) {
        size_t w = 0;
        for (size_t r = 0; r < p->free_count[b]; r++) {
            if (p->free_slots[b][r].file_id != fid)
                p->free_slots[b][w++] = p->free_slots[b][r];
        }
        p->free_count[b] = w;
    }
    pthread_mutex_unlock(&p->pool_lock);
}

/* Migrate every flag==1 record from donor → recipient (both non-active).
   Caller pre-verified `recipient_free >= donor_live` so all donor records
   fit. After return, donor still holds (now stale) flag=1 records bytewise;
   caller invokes compact_drop_seg_file to evict + unlink it. */
/* Context for compact_od_cb — carries recipient mmap and free-slot list. */
typedef struct {
    SlotcaskDb *db;
    int         stream_id;
    uint32_t    donor_fid;
    uint32_t    recipient_fid;
    uint8_t    *rmap;        /* recipient mmap base (MAP_SHARED, writeable) */
    uint32_t   *free_offs;   /* free slot byte-offsets in recipient */
    size_t      free_count;
    size_t      free_idx;
    int         rc;
} CompactOdCtx;

/* od_record_cb adapter: called by seg_scan_o_direct for each live donor slot.
   Writes the record to the next free recipient slot, then repoints the kf entry
   under the kf shard's wrlock. */
static int compact_od_cb(const uint8_t *rec, size_t vlen,
                          const uint8_t hash16[16], void *raw) {
    CompactOdCtx *c = (CompactOdCtx *)raw;
    if (c->rc != 0) return 1;  /* already failed — abort scan */

    if (c->free_idx >= c->free_count) { c->rc = -1; return 1; }

    uint32_t target_off = c->free_offs[c->free_idx];

    uint16_t klen;
    memcpy(&klen, rec + 16, 2);
    const uint8_t *key   = rec + 24;
    const uint8_t *value = rec + 24 + (size_t)klen;

    /* Step 1: write record into recipient free slot (vacuum holds objlock_wrlock
       so no concurrent writer can race on this offset). */
    seg_record_emit(c->rmap + target_off, c->db->slot_size,
                    hash16, key, (size_t)klen, value, (size_t)vlen);

    /* Step 2: repoint kf entry under the kf shard wrlock. */
    int kfshard = shard_for_hash(hash16, c->db->num_shards);
    char kfp[PATH_MAX];
    kf_path_for(kfp, c->db->data_dir, kfshard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kfp, c->db->slots_per_shard, 1) != 0) {
        c->rc = -1; return 1;
    }

    uint8_t cur_flag, cur_sid;
    uint16_t cur_fid;
    uint32_t cur_off;
    size_t kf_slot_idx;
    int lr = kf_lookup_with_slot(&kh, hash16, key, klen, c->db->data_dir,
                                  &cur_flag, &cur_sid, &cur_fid,
                                  &cur_off, &kf_slot_idx);
    if (lr != 0 || (int)cur_sid != c->stream_id ||
        (uint32_t)cur_fid != c->donor_fid) {
        /* Orphan (lr!=0) or already repointed (cur_fid != donor_fid):
           recipient slot was written but kf won't point to it — the slot
           becomes a recoverable orphan, same as the original mmap path. */
        kfcache_release(&kh);
        c->free_idx++;
        return 0;
    }

    kf_repoint_at_slot(&kh, kf_slot_idx, (uint8_t)c->stream_id,
                        (uint16_t)c->recipient_fid, target_off);
    kfcache_release(&kh);
    c->free_idx++;
    return 0;
}

/* Context for varlen compact_cb. */
typedef struct {
    SlotcaskDb *db;
    int         stream_id;
    uint32_t    donor_fid;
    uint32_t    recipient_fid;
    uint8_t    *rmap;        /* recipient mmap base */
    size_t      rmap_size;   /* total mapped bytes */
    uint32_t   *free_offs;   /* free slot byte-offsets in recipient */
    uint32_t   *free_caps;   /* capacity of each free slot (0 = unbounded) */
    size_t      free_count;
    size_t      free_next;   /* next index to try (cached linear scan position) */
    int         rc;
} VarlenCompactCtx;

/* Callback for varlen compaction: find a free recipient slot with capacity
   >= donor record size, emit the record, repoint kf entry. */
static int varlen_compact_cb(const uint8_t *rec, size_t vlen,
                              const uint8_t hash16[16], void *raw) {
    VarlenCompactCtx *c = (VarlenCompactCtx *)raw;
    if (c->rc != 0) return 1;

    uint16_t klen;
    memcpy(&klen, rec + 16, 2);
    size_t donor_rec_size = slotcask_record_size_varlen((size_t)klen, vlen);

    /* Find a free slot with enough capacity.  Linear scan from cached
       position (free bip — tombstones at the front are reused first). */
    size_t slot_idx = c->free_next;
    while (slot_idx < c->free_count) {
        uint32_t cap = c->free_caps[slot_idx];
        if (cap == 0 || (size_t)cap >= donor_rec_size) break;
        slot_idx++;
    }
    if (slot_idx >= c->free_count) {
        /* Check from the beginning in case we passed some. */
        slot_idx = 0;
        while (slot_idx < c->free_next) {
            uint32_t cap = c->free_caps[slot_idx];
            if (cap == 0 || (size_t)cap >= donor_rec_size) break;
            slot_idx++;
        }
    }
    if (slot_idx >= c->free_count) { c->rc = -1; return 1; }

    uint32_t target_off = c->free_offs[slot_idx];
    c->free_next = slot_idx + 1;

    /* Remove this slot from the free list by swapping with the last used
       position and shrinking free_count.  This avoids O(n²) compaction. */
    if (slot_idx < c->free_count - 1) {
        c->free_offs[slot_idx] = c->free_offs[c->free_count - 1];
        c->free_caps[slot_idx] = c->free_caps[c->free_count - 1];
    }
    c->free_count--;

    const uint8_t *key   = rec + 24;
    const uint8_t *value = rec + 24 + (size_t)klen;
    seg_record_emit(c->rmap + target_off, (int)donor_rec_size,
                    hash16, key, (size_t)klen, value, vlen);

    /* Repoint kf entry. */
    int kfshard = shard_for_hash(hash16, c->db->num_shards);
    char kfp[PATH_MAX];
    kf_path_for(kfp, c->db->data_dir, kfshard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kfp, c->db->slots_per_shard, 1) != 0) {
        c->rc = -1; return 1;
    }

    uint8_t cur_flag, cur_sid;
    uint16_t cur_fid;
    uint32_t cur_off;
    size_t kf_slot_idx;
    int lr = kf_lookup_with_slot(&kh, hash16, key, klen, c->db->data_dir,
                                  &cur_flag, &cur_sid, &cur_fid,
                                  &cur_off, &kf_slot_idx);
    if (lr != 0 || (int)cur_sid != c->stream_id ||
        (uint32_t)cur_fid != c->donor_fid) {
        kfcache_release(&kh);
        return 0;
    }

    kf_repoint_at_slot(&kh, kf_slot_idx, (uint8_t)c->stream_id,
                        (uint16_t)c->recipient_fid, target_off);
    kfcache_release(&kh);
    return 0;
}

/* Migrate donor → recipient for varlen streams.  Recipient is mmap'd
   for read/write; donor is O_DIRECT-scanned (or mmap'd) through the
   fixed-size O_DIRECT helper using slot_size as the max record stride,
   with each live record forwarded to varlen_compact_cb. */
static int compact_migrate_records_varlen(SlotcaskDb *db, int stream_id,
                                           uint32_t donor_fid,
                                           uint32_t recipient_fid) {
    char donor_path[PATH_MAX], recipient_path[PATH_MAX];
    seg_path_for(donor_path, db->data_dir, stream_id, donor_fid);
    seg_path_for(recipient_path, db->data_dir, stream_id, recipient_fid);

    /* Recipient: mmap for writes and for building the free-slot list. */
    SlotcaskSegHandle rh;
    if (segcache_acquire(&rh, recipient_path, 0, 0) != 0) return -1;

    size_t rmap_size = rh.map_size;

    /* Walk recipient records to find free slots (flag != 1) with capacity.
       Also collects the total number of records in the file for stats. */
    uint32_t *free_offs = NULL;
    uint32_t *free_caps = NULL;
    size_t free_count = 0, free_cap = 0;
    size_t off = 0;

    while (off + 24 <= rmap_size) {
        const uint8_t *rec = rh.map + off;
        uint8_t flag = rec[18];
        if (flag != 1) {
            uint16_t klen;
            uint32_t vlen;
            memcpy(&klen, rec + 16, 2);
            memcpy(&vlen, rec + 20, 4);
            size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
            /* Only add as free if it's a complete record. */
            if (off + rec_size <= rmap_size) {
                if (free_count == free_cap) {
                    size_t nc = free_cap ? free_cap * 2 : 256;
                    uint32_t *old_o = free_offs;
                    uint32_t *old_c = free_caps;
                    uint32_t *t = realloc(free_offs, nc * sizeof(uint32_t));
                    uint32_t *c = realloc(free_caps, nc * sizeof(uint32_t));
                    if (!t) { free(old_o); free(c ? c : old_c); segcache_release(&rh); return -1; }
                    if (!c) { free(t); free(old_c); segcache_release(&rh); return -1; }
                    free_offs = t;
                    free_caps = c;
                    free_cap = nc;
                }
                free_offs[free_count] = (uint32_t)off;
                free_caps[free_count] = (uint32_t)rec_size;
                free_count++;
            }
            off += rec_size;
        } else {
            uint16_t klen;
            uint32_t vlen;
            memcpy(&klen, rec + 16, 2);
            memcpy(&vlen, rec + 20, 4);
            size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
            off += rec_size;
        }
    }

    VarlenCompactCtx ctx = {
        .db = db, .stream_id = stream_id,
        .donor_fid = donor_fid, .recipient_fid = recipient_fid,
        .rmap = rh.map, .rmap_size = rmap_size,
        .free_offs = free_offs, .free_caps = free_caps,
        .free_count = free_count, .free_next = 0, .rc = 0,
    };

    /* Scan donor using the fixed O_DIRECT scanner with db->slot_size.
       For varlen, this strides by slot_size, which is the fixed max-slot
       size.  Most records are smaller, so we'll skip padding regions
       (flag=0) just like the fixed format does.  Records are still at
       variable offsets but the scanner doesn't need to care — it only
       processes flag==1 records, which have their header set correctly
       at their real offset.  The key insight: in varlen format, each
       record occupies exactly its padded size, and flag=0 regions between
       records don't exist (no fixed slot grid).  However, seg_scan_o_direct
       uses slot_size stride which is wrong for varlen records that are
       shorter than slot_size.  So we use a simpler mmap-based scan
       instead for the donor. */
    /* Donor: mmap walk of headers (not O_DIRECT — varlen records aren't
       at fixed stride).  Since the donor will be unlinked after migration,
       cache pollution is short-lived. */
    {
        SlotcaskSegHandle dh;
        if (segcache_acquire(&dh, donor_path, 0, 0) != 0) {
            free(free_offs);
            free(free_caps);
            segcache_release(&rh);
            return -1;
        }
        size_t donor_size = dh.map_size;
        size_t doff = 0;
        while (doff + 24 <= donor_size) {
            const uint8_t *rec = dh.map + doff;
            uint8_t flag = rec[18];
            if (flag == 1) {
                uint32_t vlen;
                memcpy(&vlen, rec + 20, 4);
                if (varlen_compact_cb(rec, (size_t)vlen, rec, &ctx) != 0) {
                    break;
                }
            }
            uint16_t klen;
            uint32_t vlen;
            memcpy(&klen, rec + 16, 2);
            memcpy(&vlen, rec + 20, 4);
            doff += slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
        }
        segcache_release(&dh);
    }

    free(free_offs);
    free(free_caps);
    segcache_release(&rh);
    return ctx.rc;
}

static int compact_migrate_records(SlotcaskDb *db, int stream_id,
                                    uint32_t donor_fid, uint32_t recipient_fid) {
    char donor_path[PATH_MAX], recipient_path[PATH_MAX];
    seg_path_for(donor_path, db->data_dir, stream_id, donor_fid);
    seg_path_for(recipient_path, db->data_dir, stream_id, recipient_fid);

    /* Recipient: mmap for writes and for building the free-slot list. */
    SlotcaskSegHandle rh;
    if (segcache_acquire(&rh, recipient_path, 0, 0) != 0) return -1;

    int slot_size = db->slot_size;
    size_t total = rh.map_size / (size_t)slot_size;

    /* Build recipient free-offset list (every slot whose flag != 1). */
    uint32_t *free_offs = NULL;
    size_t free_count = 0, free_cap = 0;
    for (size_t s = 0; s < total; s++) {
        const uint8_t *rec = rh.map + s * (size_t)slot_size;
        if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) == 1) continue;
        if (free_count == free_cap) {
            size_t nc = free_cap ? free_cap * 2 : 256;
            uint32_t *t = realloc(free_offs, nc * sizeof(uint32_t));
            if (!t) {
                free(free_offs);
                segcache_release(&rh);
                return -1;
            }
            free_offs = t;
            free_cap = nc;
        }
        free_offs[free_count++] = (uint32_t)(s * (size_t)slot_size);
    }

    /* Donor: O_DIRECT scan — read-once, then unlinked; pages must not enter
       the page cache. compact_od_cb handles kf repoint per live record. */
    CompactOdCtx ctx = {
        .db = db, .stream_id = stream_id,
        .donor_fid = donor_fid, .recipient_fid = recipient_fid,
        .rmap = rh.map,
        .free_offs = free_offs, .free_count = free_count,
        .free_idx = 0, .rc = 0,
    };
    seg_scan_o_direct(donor_path, slot_size, compact_od_cb, &ctx);

    free(free_offs);
    segcache_release(&rh);
    return ctx.rc;
}

/* Evict from segcache (msync + munmap + close under entry wrlock), unlink,
   fsync parent dir, drop pool entries pointing at this file_id. */
static int compact_drop_seg_file(SlotcaskDb *db, int stream_id,
                                  uint32_t file_id) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);

    /* Eviction is keyed by exact path here (prefix-match on a full path is
       an exact match). The wrlock inside ensures every reader has finished
       its mmap-protected read before we close the fd. */
    segcache_invalidate_prefix(path);

    if (unlink(path) != 0 && errno != ENOENT) return -1;

    char parent[PATH_MAX];
    snprintf(parent, sizeof(parent), "%s", path);
    char *slash = strrchr(parent, '/');
    if (slash) {
        *slash = '\0';
        int dfd = open(parent, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
    }

    pool_drop_for_file(&db->streams[stream_id], (uint16_t)file_id);
    return 0;
}

/* Direction-C compaction for one stream. Returns # of files dropped. */
static int compact_one_stream(SlotcaskDb *db, int stream_id) {
    char dir[PATH_MAX];
    stream_dir_for(dir, db->data_dir, stream_id);

    SlotcaskStream *p = &db->streams[stream_id];
    pthread_mutex_lock(&p->rotation_lock);
    uint32_t active = p->active_file_id;
    pthread_mutex_unlock(&p->rotation_lock);

    DIR *dh = opendir(dir);
    if (!dh) return 0;

    SegStat *files = NULL;
    size_t nfiles = 0, fcap = 0;
    struct dirent *de;
    while ((de = readdir(dh)) != NULL) {
        if (de->d_name[0] == '.') continue;
        size_t nlen = strlen(de->d_name);
        if (nlen != 10 || strcmp(de->d_name + 6, ".dat") != 0) continue;
        uint32_t fid = (uint32_t)strtoul(de->d_name, NULL, 10);
        if (fid == active) continue;

        if (nfiles == fcap) {
            size_t nc = fcap ? fcap * 2 : 16;
            SegStat *t = realloc(files, nc * sizeof(SegStat));
            if (!t) { free(files); closedir(dh); return 0; }
            files = t;
            fcap = nc;
        }
        files[nfiles].stream_id = stream_id;
        files[nfiles].file_id = fid;
        files[nfiles].live_count = 0;
        files[nfiles].total_slots = 0;
        nfiles++;
    }
    closedir(dh);

    if (nfiles == 0) { free(files); return 0; }

    for (size_t i = 0; i < nfiles; i++) {
        if (seg_stat_one(db, stream_id, files[i].file_id,
                          &files[i].live_count, &files[i].total_slots) != 0) {
            files[i].live_count = files[i].total_slots = 0;
        }
    }

    qsort(files, nfiles, sizeof(SegStat), seg_stat_cmp_live_asc);

    int dropped = 0;

    /* Two-pointer pair-merge: i = sparsest donor, j = densest recipient.
       Skip empties first (drop unconditionally), then merge i→j when j
       has free room for all of i's live records. */
    size_t i = 0;
    size_t j = nfiles - 1;
    while (i < j) {
        if (files[i].total_slots == 0) { i++; continue; }
        if (files[i].live_count == 0) {
            if (compact_drop_seg_file(db, stream_id, files[i].file_id) == 0)
                dropped++;
            i++;
            continue;
        }
        uint32_t recip_free = files[j].total_slots - files[j].live_count;
        if (recip_free >= files[i].live_count) {
            if (compact_migrate_records(db, stream_id,
                                          files[i].file_id, files[j].file_id) == 0) {
                if (compact_drop_seg_file(db, stream_id, files[i].file_id) == 0)
                    dropped++;
                files[j].live_count += files[i].live_count;
            }
            i++;
        } else {
            /* Recipient too full for this donor; donors only get sparser
               below i so no point retrying with a sparser donor. Move
               recipient down. */
            if (j == i + 1) break;
            j--;
        }
    }

    free(files);
    return dropped;
}

/* Variable-length variant: same two-pointer merge as compact_one_stream
   but uses seg_stat_one_varlen and compact_migrate_records_varlen. */
static int compact_one_stream_varlen(SlotcaskDb *db, int stream_id) {
    char dir[PATH_MAX];
    stream_dir_for(dir, db->data_dir, stream_id);

    SlotcaskStream *p = &db->streams[stream_id];
    pthread_mutex_lock(&p->rotation_lock);
    uint32_t active = p->active_file_id;
    pthread_mutex_unlock(&p->rotation_lock);

    DIR *dh = opendir(dir);
    if (!dh) return 0;

    SegStat *files = NULL;
    size_t nfiles = 0, fcap = 0;
    struct dirent *de;
    while ((de = readdir(dh)) != NULL) {
        if (de->d_name[0] == '.') continue;
        size_t nlen = strlen(de->d_name);
        if (nlen != 10 || strcmp(de->d_name + 6, ".dat") != 0) continue;
        uint32_t fid = (uint32_t)strtoul(de->d_name, NULL, 10);
        if (fid == active) continue;

        if (nfiles == fcap) {
            size_t nc = fcap ? fcap * 2 : 16;
            SegStat *t = realloc(files, nc * sizeof(SegStat));
            if (!t) { free(files); closedir(dh); return 0; }
            files = t;
            fcap = nc;
        }
        files[nfiles].stream_id = stream_id;
        files[nfiles].file_id = fid;
        files[nfiles].live_count = 0;
        files[nfiles].total_slots = 0;
        nfiles++;
    }
    closedir(dh);

    if (nfiles == 0) { free(files); return 0; }

    for (size_t i = 0; i < nfiles; i++) {
        if (seg_stat_one_varlen(db, stream_id, files[i].file_id,
                                 &files[i].live_count,
                                 &files[i].total_slots) != 0) {
            files[i].live_count = files[i].total_slots = 0;
        }
    }

    qsort(files, nfiles, sizeof(SegStat), seg_stat_cmp_live_asc);

    int dropped = 0;
    size_t i = 0;
    size_t j = nfiles - 1;
    while (i < j) {
        if (files[i].total_slots == 0) { i++; continue; }
        if (files[i].live_count == 0) {
            if (compact_drop_seg_file(db, stream_id, files[i].file_id) == 0)
                dropped++;
            i++;
            continue;
        }
        uint32_t recip_free = files[j].total_slots - files[j].live_count;
        if (recip_free >= files[i].live_count) {
            if (compact_migrate_records_varlen(db, stream_id,
                                                files[i].file_id,
                                                files[j].file_id) == 0) {
                if (compact_drop_seg_file(db, stream_id, files[i].file_id) == 0)
                    dropped++;
                files[j].live_count += files[i].live_count;
            }
            i++;
        } else {
            if (j == i + 1) break;
            j--;
        }
    }

    free(files);
    return dropped;
}

/* Public entry point. Caller must hold objlock_wrlock for the object. */
int slotcask_compact_segs(SlotcaskDb *db, int *out_dropped) {
    if (!db) return -1;
    int (*compact_fn)(SlotcaskDb *, int) =
        (db->format == SLOTCASK_FORMAT_VARIABLE)
        ? compact_one_stream_varlen
        : compact_one_stream;
    int total = 0;
    for (int s = 0; s < db->num_streams; s++) {
        total += compact_fn(db, s);
    }
    if (out_dropped) *out_dropped = total;
    return 0;
}

/* ─── offline fixed → varlen migration ─── */

/* Migrate an object from fixed-size to variable-length segment format.
   Daemon must NOT be running.  Walks every KF shard, reads each record
   from the old fixed-format segment, writes it to a new varlen segment,
   repoints the KF entry, then removes old segment files. */
#define MIGRATE_STREAM_BASE 60000u
#define COMPACT_STREAM_BASE 30000u   /* dest range for slotcask_compact; fits uint16_t, below MIGRATE_STREAM_BASE */

int slotcask_migrate_to_varlen(SlotcaskDb *db) {
    if (!db) return -1;
    if (db->format == SLOTCASK_FORMAT_VARIABLE) return 0;
    if (db->format != SLOTCASK_FORMAT_FIXED) return -1;

    int n_streams = db->num_streams;

    /* Phase 0: open all source segment files as direct mmaps.
       Bypassing segcache eliminates per-record mutex/probe overhead. */
    typedef struct { uint8_t *base; size_t sz; } SrcMap;
    SrcMap *src[SLOTCASK_MAX_STREAMS];
    uint32_t src_cnt[SLOTCASK_MAX_STREAMS];
    memset(src, 0, sizeof(src));
    memset(src_cnt, 0, sizeof(src_cnt));

    for (int s = 0; s < n_streams; s++) {
        uint32_t cnt = 0;
        for (;;) {
            char p[PATH_MAX];
            seg_path_for(p, db->data_dir, s, cnt);
            if (access(p, F_OK) != 0) break;
            cnt++;
        }
        src_cnt[s] = cnt;
        if (cnt == 0) continue;
        src[s] = calloc((size_t)cnt, sizeof(SrcMap));
        if (!src[s]) goto fail;
        for (uint32_t f = 0; f < cnt; f++) {
            char p[PATH_MAX];
            seg_path_for(p, db->data_dir, s, f);
            int fd = open(p, O_RDONLY);
            if (fd < 0) continue;
            struct stat st;
            if (fstat(fd, &st) == 0 && st.st_size > 0) {
                void *m = mmap(NULL, (size_t)st.st_size,
                               PROT_READ, MAP_SHARED, fd, 0);
                if (m != MAP_FAILED) {
                    src[s][f].base = (uint8_t *)m;
                    src[s][f].sz   = (size_t)st.st_size;
                }
            }
            close(fd);
        }
    }

    /* Per-stream dest segment state (one open mmap at a time per stream). */
    typedef struct { uint8_t *base; size_t alloc; int fd; } DestMap;
    DestMap dest[SLOTCASK_MAX_STREAMS];
    memset(dest, 0, sizeof(dest));
    for (int s = 0; s < n_streams; s++) dest[s].fd = -1;
    uint32_t dest_fid[SLOTCASK_MAX_STREAMS];
    size_t   dest_off[SLOTCASK_MAX_STREAMS];
    for (int s = 0; s < n_streams; s++) {
        dest_fid[s] = MIGRATE_STREAM_BASE + (uint32_t)s * 1000u;
        dest_off[s] = 0;
    }

    /* Phase 1: KF walk with direct-mmap reads and writes — no per-record locks. */
    for (int shard = 0; shard < db->num_shards; shard++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, shard);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0)
            goto fail;

        size_t cap = kh.capacity;
        SlotcaskKfEntry *kf = kh.map;

        for (size_t slot = 0; slot < cap; slot++) {
            uint8_t flag = kf[slot].flag;
            /* Skip empty (0) and tombstoned (2) KF entries — tombstones are
               logically deleted so we don't copy them to the dest segment.
               They keep their existing file_id/offset (pointing to the old
               source segments about to be deleted), which is harmless since
               flag==2 offsets are never dereferenced for actual data. */
            if (flag != 1) continue;

            uint8_t  sid = kf[slot].stream_id;
            uint16_t fid = kf[slot].file_id;
            uint32_t off = kf[slot].offset;

            if (sid >= (uint8_t)n_streams) continue;
            if (!src[sid] || fid >= src_cnt[sid]) continue;
            SrcMap *sm = &src[sid][fid];
            if (!sm->base || (size_t)off + 24 > sm->sz) continue;

            uint16_t klen; memcpy(&klen, sm->base + off + 16, 2);
            uint32_t vlen; memcpy(&vlen, sm->base + off + 20, 4);
            if ((size_t)off + 24 + klen + vlen > sm->sz) continue;

            const uint8_t *key   = sm->base + off + 24;
            const uint8_t *value = key + klen;
            size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);

            /* Open dest if needed or rotate when full. */
            if (!dest[sid].base ||
                dest_off[sid] + rec_size > SLOTCASK_SEG_MAX_BYTES) {
                if (dest[sid].base) {
                    size_t used = dest_off[sid];
                    munmap(dest[sid].base, dest[sid].alloc);
                    ftruncate(dest[sid].fd, (off_t)used);
                    close(dest[sid].fd);
                    dest[sid].base = NULL;
                    dest[sid].fd   = -1;
                    dest_fid[sid]++;
                    dest_off[sid] = 0;
                }
                char np[PATH_MAX];
                seg_path_for(np, db->data_dir, sid, dest_fid[sid]);
                { char d2[PATH_MAX]; snprintf(d2, sizeof(d2), "%s", np);
                  char *sl = strrchr(d2, '/'); if (sl) { *sl = '\0'; mkdirp_local(d2); } }
                int fd = open(np, O_RDWR | O_CREAT | O_TRUNC, 0644);
                if (fd < 0) { kfcache_release(&kh); goto fail; }
                if (ftruncate(fd, (off_t)SLOTCASK_SEG_MAX_BYTES) < 0)
                    { close(fd); kfcache_release(&kh); goto fail; }
                void *dm = mmap(NULL, SLOTCASK_SEG_MAX_BYTES,
                                PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                if (dm == MAP_FAILED)
                    { close(fd); kfcache_release(&kh); goto fail; }
                dest[sid].base  = (uint8_t *)dm;
                dest[sid].alloc = SLOTCASK_SEG_MAX_BYTES;
                dest[sid].fd    = fd;
            }

            uint32_t new_off = (uint32_t)dest_off[sid];
            dest_off[sid] += rec_size;

            seg_record_emit(dest[sid].base + new_off, (int)rec_size,
                            kf[slot].hash, key, (size_t)klen, value, (size_t)vlen);

            kf_repoint_at_slot(&kh, slot, sid,
                               (uint16_t)dest_fid[sid], new_off);
        }
        kfcache_release(&kh);
    }

    /* Close dest maps.  Do NOT ftruncate the last file for each stream —
       segcache_acquire(grow=0) in recover_one_stream requires the file to be
       SLOTCASK_SEG_MAX_BYTES.  Intermediate rotation files were already
       truncated when they were closed mid-migration. */
    for (int s = 0; s < n_streams; s++) {
        if (dest[s].base) {
            munmap(dest[s].base, dest[s].alloc);
            close(dest[s].fd);
            dest[s].base = NULL;
        }
        /* Update stream state so the daemon resumes from the right file.
           reserve_off must reflect actual bytes written — offset 0 would
           cause the next online insert to overwrite the start of the
           compacted segment. Offline restarts re-derive via recover_one_stream
           anyway, but correct it here for the server-side path too. */
        pthread_mutex_lock(&db->streams[s].rotation_lock);
        db->streams[s].active_file_id = dest_fid[s];
        db->streams[s].reserve_off    = dest_off[s];
        pthread_mutex_unlock(&db->streams[s].rotation_lock);
    }

    /* Close source mmaps. */
    for (int s = 0; s < n_streams; s++) {
        if (src[s]) {
            for (uint32_t f = 0; f < src_cnt[s]; f++)
                if (src[s][f].base) munmap(src[s][f].base, src[s][f].sz);
            free(src[s]);
            src[s] = NULL;
        }
    }

    /* Phase 2: write format marker before deleting old files.
       This ordering ensures a crash between here and cleanup leaves the
       object readable: .format=VARIABLE, KF already repointed, old files
       still present but unreferenced (cleaned up on next migrate run). */
    db->format = SLOTCASK_FORMAT_VARIABLE;
    if (write_format_marker(db, SLOTCASK_FORMAT_VARIABLE) != 0)
        return -1;

    /* Phase 3: delete old segment files (file_id < MIGRATE_STREAM_BASE). */
    for (int s = 0; s < n_streams; s++) {
        char dir[PATH_MAX];
        stream_dir_for(dir, db->data_dir, s);
        DIR *dh = opendir(dir);
        if (!dh) continue;
        struct dirent *de;
        while ((de = readdir(dh)) != NULL) {
            if (de->d_name[0] == '.') continue;
            size_t nlen = strlen(de->d_name);
            if (nlen != 10 || strcmp(de->d_name + 6, ".dat") != 0) continue;
            uint32_t file_id = (uint32_t)strtoul(de->d_name, NULL, 10);
            if (file_id >= MIGRATE_STREAM_BASE) continue;
            char full[PATH_MAX];
            snprintf(full, sizeof(full), "%s/%s", dir, de->d_name);
            segcache_invalidate_prefix(full);
            unlink(full);
        }
        closedir(dh);
    }
    return 0;

fail:
    for (int s = 0; s < n_streams; s++) {
        if (dest[s].base) { munmap(dest[s].base, dest[s].alloc); close(dest[s].fd); }
        if (src[s]) {
            for (uint32_t f = 0; f < src_cnt[s]; f++)
                if (src[s][f].base) munmap(src[s][f].base, src[s][f].sz);
            free(src[s]);
        }
    }
    return -1;
}

/* Per-stream source map (read-only mmap of one source segment file). */
typedef struct { uint8_t *base; size_t sz; } CmpSegMap;
typedef struct { CmpSegMap *maps; uint32_t count; } CmpStreamMaps;

/* Per-stream worker arg for parallel compact. */
typedef struct {
    SlotcaskDb     *db;
    CmpStreamMaps  *smaps;        /* shared read-only across all workers */
    uint32_t        src_min;
    uint32_t        dest_base;
    int             sid;          /* stream this worker owns */
    SlotcaskTrimFn  trim_fn;
    void           *trim_ctx;
    uint32_t        dest_fid_out; /* final active file id after compaction */
    size_t          dest_off_out; /* bytes written to final file */
    int             rc;
} CmpStreamArg;

/* Repack one stream: scan all KF shards for entries belonging to sid,
   trim and write each to a fresh dest segment file, repoint KF entries.
   Workers for different streams run concurrently; they serialise briefly
   on kfcache_acquire per shard (exclusive lock, held for one shard scan). */
static void *compact_stream_worker(void *arg_ptr) {
    CmpStreamArg *a = arg_ptr;
    SlotcaskDb *db = a->db;
    int sid = a->sid;

    uint32_t dest_fid  = a->dest_base + (uint32_t)sid * 1000u;
    size_t   dest_off  = 0;
    uint8_t *dest_ptr  = NULL;
    size_t   dest_alloc = 0;
    int      dest_fd   = -1;

    for (int shard = 0; shard < db->num_shards; shard++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, shard);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) {
            a->rc = -1;
            goto worker_fail;
        }

        size_t cap = kh.capacity;
        SlotcaskKfEntry *kf = kh.map;

        for (size_t slot = 0; slot < cap; slot++) {
            if (kf[slot].flag != 1) continue;
            if ((int)kf[slot].stream_id != sid) continue;

            uint32_t fid = kf[slot].file_id;
            uint32_t off = kf[slot].offset;

            if (!a->smaps[sid].maps) continue;
            uint32_t lo = a->src_min + (uint32_t)sid * 1000u;
            if (fid < lo || fid - lo >= a->smaps[sid].count) continue;
            CmpSegMap *sm = &a->smaps[sid].maps[fid - lo];
            if (!sm->base || (size_t)off + 24 > sm->sz) continue;

            uint16_t klen; memcpy(&klen, sm->base + off + 16, 2);
            uint32_t vlen; memcpy(&vlen, sm->base + off + 20, 4);
            if ((size_t)off + 24 + klen + vlen > sm->sz) continue;

            const uint8_t *key   = sm->base + off + 24;
            const uint8_t *value = key + klen;

            size_t trimmed_vlen = a->trim_fn(value, (size_t)vlen, a->trim_ctx);
            size_t rec_size = slotcask_record_size_varlen((size_t)klen, trimmed_vlen);

            if (!dest_ptr || dest_off + rec_size > SLOTCASK_SEG_MAX_BYTES) {
                if (dest_ptr) {
                    munmap(dest_ptr, dest_alloc);
                    ftruncate(dest_fd, (off_t)dest_off);
                    close(dest_fd);
                    dest_ptr = NULL; dest_fd = -1;
                    dest_fid++;
                    dest_off = 0;
                }
                char np[PATH_MAX];
                seg_path_for(np, db->data_dir, sid, dest_fid);
                { char d2[PATH_MAX]; snprintf(d2, sizeof(d2), "%s", np);
                  char *sl = strrchr(d2, '/'); if (sl) { *sl = '\0'; mkdirp_local(d2); } }
                int fd = open(np, O_RDWR | O_CREAT | O_TRUNC, 0644);
                if (fd < 0) { kfcache_release(&kh); a->rc = -1; goto worker_fail; }
                if (ftruncate(fd, (off_t)SLOTCASK_SEG_MAX_BYTES) < 0) {
                    close(fd); kfcache_release(&kh); a->rc = -1; goto worker_fail;
                }
                void *dm = mmap(NULL, SLOTCASK_SEG_MAX_BYTES,
                                PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                if (dm == MAP_FAILED) {
                    close(fd); kfcache_release(&kh); a->rc = -1; goto worker_fail;
                }
                dest_ptr   = (uint8_t *)dm;
                dest_alloc = SLOTCASK_SEG_MAX_BYTES;
                dest_fd    = fd;
            }

            uint32_t new_off = (uint32_t)dest_off;
            dest_off += rec_size;

            seg_record_emit(dest_ptr + new_off, (int)rec_size,
                            kf[slot].hash, key, (size_t)klen, value, trimmed_vlen);
            kf_repoint_at_slot(&kh, slot, (uint8_t)sid,
                               (uint16_t)dest_fid, new_off);
        }
        kfcache_release(&kh);
    }

    if (dest_ptr) { munmap(dest_ptr, dest_alloc); close(dest_fd); }
    a->dest_fid_out = dest_fid;
    a->dest_off_out = dest_off;
    a->rc = 0;
    return NULL;

worker_fail:
    if (dest_ptr) { munmap(dest_ptr, dest_alloc); close(dest_fd); }
    return NULL;
}

int slotcask_compact(SlotcaskDb *db, SlotcaskTrimFn trim_fn, void *trim_ctx) {
    if (!db || db->format != SLOTCASK_FORMAT_VARIABLE) return -1;
    if (!trim_fn) return 0;

    int n_streams = db->num_streams;
    if (n_streams <= 0 || n_streams > SLOTCASK_MAX_STREAMS) return -1;

    uint32_t cur_max = 0;
    for (int s = 0; s < n_streams; s++) {
        pthread_mutex_lock(&db->streams[s].rotation_lock);
        uint32_t fid = db->streams[s].active_file_id;
        pthread_mutex_unlock(&db->streams[s].rotation_lock);
        if (fid > cur_max) cur_max = fid;
    }
    uint32_t dest_base = (cur_max >= MIGRATE_STREAM_BASE)
        ? COMPACT_STREAM_BASE : MIGRATE_STREAM_BASE;
    uint32_t src_min = (dest_base == COMPACT_STREAM_BASE)
        ? MIGRATE_STREAM_BASE : COMPACT_STREAM_BASE;

    CmpStreamMaps *smaps = calloc((size_t)n_streams, sizeof(CmpStreamMaps));
    if (!smaps) return -1;

    for (int s = 0; s < n_streams; s++) {
        char dir[PATH_MAX];
        stream_dir_for(dir, db->data_dir, s);
        DIR *dh = opendir(dir);
        if (!dh) continue;
        uint32_t lo = src_min + (uint32_t)s * 1000u;
        uint32_t hi = lo + 1000u;
        uint32_t cnt = 0;
        struct dirent *de;
        while ((de = readdir(dh)) != NULL) {
            if (de->d_name[0] == '.') continue;
            uint32_t fid = (uint32_t)strtoul(de->d_name, NULL, 10);
            if (fid >= lo && fid < hi && fid >= cnt + lo) cnt = fid - lo + 1;
        }
        closedir(dh);
        if (cnt == 0) continue;
        smaps[s].maps = calloc((size_t)cnt, sizeof(CmpSegMap));
        smaps[s].count = cnt;
        if (!smaps[s].maps) goto fail_smaps;
        for (uint32_t i = 0; i < cnt; i++) {
            char p[PATH_MAX];
            seg_path_for(p, db->data_dir, s, lo + i);
            int fd = open(p, O_RDONLY);
            if (fd < 0) continue;
            struct stat st;
            if (fstat(fd, &st) == 0 && st.st_size > 0) {
                void *m = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED, fd, 0);
                if (m != MAP_FAILED) {
                    smaps[s].maps[i].base = (uint8_t *)m;
                    smaps[s].maps[i].sz   = (size_t)st.st_size;
                }
            }
            close(fd);
        }
    }

    CmpStreamArg *args = calloc((size_t)n_streams, sizeof(CmpStreamArg));
    if (!args) goto fail_smaps;
    for (int s = 0; s < n_streams; s++) {
        args[s].db        = db;
        args[s].smaps     = smaps;
        args[s].src_min   = src_min;
        args[s].dest_base = dest_base;
        args[s].sid       = s;
        args[s].trim_fn   = trim_fn;
        args[s].trim_ctx  = trim_ctx;
        args[s].rc        = 0;
    }

    parallel_for_io(compact_stream_worker, args, n_streams, sizeof(CmpStreamArg));

    int any_fail = 0;
    for (int s = 0; s < n_streams; s++)
        if (args[s].rc != 0) { any_fail = 1; break; }

    if (!any_fail) {
        for (int s = 0; s < n_streams; s++) {
            pthread_mutex_lock(&db->streams[s].rotation_lock);
            db->streams[s].active_file_id = args[s].dest_fid_out;
            db->streams[s].reserve_off    = args[s].dest_off_out;
            pthread_mutex_unlock(&db->streams[s].rotation_lock);
        }
    }
    free(args);

    for (int s = 0; s < n_streams; s++) {
        if (smaps[s].maps) {
            for (uint32_t i = 0; i < smaps[s].count; i++)
                if (smaps[s].maps[i].base)
                    munmap(smaps[s].maps[i].base, smaps[s].maps[i].sz);
            free(smaps[s].maps);
        }
    }
    free(smaps);
    if (any_fail) return -1;

    for (int s = 0; s < n_streams; s++) {
        char dir[PATH_MAX];
        stream_dir_for(dir, db->data_dir, s);
        DIR *dh = opendir(dir);
        if (!dh) continue;
        uint32_t lo = src_min + (uint32_t)s * 1000u;
        uint32_t hi = lo + 1000u;
        struct dirent *de;
        while ((de = readdir(dh)) != NULL) {
            if (de->d_name[0] == '.') continue;
            size_t nlen = strlen(de->d_name);
            if (nlen != 10 || strcmp(de->d_name + 6, ".dat") != 0) continue;
            uint32_t fid = (uint32_t)strtoul(de->d_name, NULL, 10);
            if (fid < lo || fid >= hi) continue;
            char full[PATH_MAX];
            snprintf(full, sizeof(full), "%s/%s", dir, de->d_name);
            segcache_invalidate_prefix(full);
            unlink(full);
        }
        closedir(dh);
    }
    return 0;

fail_smaps:
    for (int s = 0; s < n_streams; s++) {
        if (smaps[s].maps) {
            for (uint32_t i = 0; i < smaps[s].count; i++)
                if (smaps[s].maps[i].base)
                    munmap(smaps[s].maps[i].base, smaps[s].maps[i].sz);
            free(smaps[s].maps);
        }
    }
    free(smaps);
    return -1;
}

/* Rebuild every kf shard in place to drop tombstones (flag=2 entries).
   Reuses the resplit machinery with new_cap = current_cap, so the rebuilt
   kf has total = live count and deleted = 0. Holds each shard's wrlock
   for the rebuild duration; bypassed (silently) for shards that fail to
   acquire. Used by cmd_vacuum so the user-visible "orphaned" count drops
   to 0 after vacuum (matching pre-kf-derived-counts behavior). */
int slotcask_compact_kf(SlotcaskDb *db) {
    if (!db || db->num_shards <= 0) return -1;
    for (int s = 0; s < db->num_shards; s++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) continue;
        if (kh.hdr && kh.hdr->deleted > 0) {
            (void)kfcache_resplit_locked(&kh, kh.capacity);
        }
        kfcache_release(&kh);
    }
    return 0;
}

/* Suppress unused-static warnings for build_record_buf which remains in the
   file as a reference for migrators / future bulk paths but isn't called
   from the current code (seg_write_record builds inline into the mmap). */
__attribute__((unused)) static void *_silence_build_record_buf =
    (void *)build_record_buf;
