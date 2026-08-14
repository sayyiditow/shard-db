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
#include "seg_scan_varlen.h"

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

#define BULK_COMMIT_MAX_RECORDS 256

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
        rwlock_init_writer_preferring(&g_kfcache[i].rwlock);
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

/* Caller holds g_kfcache_lock. Returns with it held. Lock entries before
   re-taking the table mutex so every cache path follows entry -> table and
   nested kf/segment walks cannot form a table -> entry -> table cycle. */
static int kfcache_drop_slot(int slot, CacheDropReason reason, int wait) {
    KfCacheEntry *e = &g_kfcache[slot];
    if (!e->used) return 1;
    uint64_t expected_gen = atomic_load_explicit(&e->gen, memory_order_acquire);
    char expected_path[PATH_MAX];
    snprintf(expected_path, sizeof(expected_path), "%s", e->path);
    /* Exclude any thread still holding this slot's rwlock from an earlier
       kfcache_acquire() (e.g. slotcask_pool_rebuild_worker's long
       rdlock-held walk over kh.map) before munmapping under it — the
       caller only holds g_kfcache_lock, which guards slot-table
       bookkeeping, not live e->base access. Without this, LRU eviction
       or a stale-entry drop can munmap out from under a concurrent
       reader, producing a SEGV. */
    pthread_mutex_unlock(&g_kfcache_lock);
    int lock_rc = wait ? pthread_rwlock_wrlock(&e->rwlock)
                       : pthread_rwlock_trywrlock(&e->rwlock);
    pthread_mutex_lock(&g_kfcache_lock);
    if (lock_rc != 0) return 0;
    if (!e->used ||
        atomic_load_explicit(&e->gen, memory_order_acquire) != expected_gen ||
        strcmp(e->path, expected_path) != 0) {
        pthread_rwlock_unlock(&e->rwlock);
        return 0;
    }
    if (reason == CACHE_DROP_EVICT && e->base && e->map_size > 0 &&
        durability_flush_dirty(&e->dirty, &e->dirty_since_ms,
                               e->base, e->map_size) < 0) {
        pthread_rwlock_unlock(&e->rwlock);
        return -1;
    }
    if (e->base) munmap(e->base, e->map_size);
    if (e->fd >= 0) close(e->fd);
    e->base = NULL;
    e->fd = -1;
    e->map_size = 0;
    e->capacity = 0;
    atomic_store_explicit(&e->dirty, 0, memory_order_relaxed);
    atomic_store_explicit(&e->dirty_since_ms, 0, memory_order_relaxed);
    e->used = 0;
    e->path[0] = '\0';
    /* Increment gen under g_kfcache_lock (caller always holds it).
       Any SlotRef pointing at this slot will fail its gen check after
       this store, forcing the slow-path re-probe. */
    atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
    __atomic_fetch_sub(&g_kfcache_count, 1, __ATOMIC_RELAXED);
    pthread_rwlock_unlock(&e->rwlock);
    return 1;
}

/* Drop every cached kf shard whose path starts with `prefix`. Used by
   slotcask_registry_invalidate to flush stale mmap regions before the
   on-disk files move (rebuild_object_v2) or vanish (drop-object).

   Lock-ordering: cache eviction and installation never hold the table mutex
   while waiting for an entry rwlock. Prefix invalidation likewise takes only
   the entry rwlock. The slots array is fixed-size; `used` is published
   atomically, and identity is rechecked after taking the entry lock. The
   caller (per-object wrlock) guarantees no concurrent ops on THIS object, so
   rwlock contention is bounded. */
static void kfcache_invalidate_prefix(const char *prefix) {
    if (!g_kfcache || !prefix || !prefix[0]) return;
    size_t pl = strlen(prefix);
    for (int i = 0; i < g_kfcache_slots; i++) {
        KfCacheEntry *e = &g_kfcache[i];
        if (!atomic_load_explicit(&e->used, memory_order_acquire)) continue;
        if (strncmp(e->path, prefix, pl) != 0) continue;
        pthread_rwlock_wrlock(&e->rwlock);
        if (atomic_load_explicit(&e->used, memory_order_acquire) &&
            strncmp(e->path, prefix, pl) == 0) {
            if (g_db && g_kfcache_test_hold_ms > 0) {
                /* Test-only hook (KFCACHE_TEST_HOLD_MS): widens this
                   window deterministically for the shutdown-race
                   regression test. 0 in production. */
                struct timespec hold_ts = { g_kfcache_test_hold_ms / 1000,
                                             (long)(g_kfcache_test_hold_ms % 1000) * 1000000L };
                int ret;
                do {
                    ret = nanosleep(&hold_ts, &hold_ts);
                } while (ret != 0 && errno == EINTR);
            }
            /* Structural discard under the object wrlock: the caller is
               deleting this file or has durably published its replacement. */
            if (e->base) munmap(e->base, e->map_size);
            if (e->fd >= 0) close(e->fd);
            e->base = NULL;
            e->fd = -1;
            e->map_size = 0;
            e->capacity = 0;
            e->path[0] = '\0';
            atomic_store_explicit(&e->dirty, 0, memory_order_relaxed);
            atomic_store_explicit(&e->dirty_since_ms, 0, memory_order_relaxed);
            atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
            atomic_store_explicit(&e->used, 0, memory_order_release);
            __sync_fetch_and_sub(&g_kfcache_count, 1);
            /* Test-only early exit: unlock this entry, then leave the
               remaining prefix-matched entries alone.  One held entry is
               enough for the shutdown-race regression test; iterating
               the rest would multiply HOLD_MS and blow the test's 10s
               waitpid timeout at high splits.  Production
               (g_kfcache_test_hold_ms=0) never takes this path — all
               matching entries are invalidated in one pass. */
            if (g_db && g_kfcache_test_hold_ms > 0) {
                pthread_rwlock_unlock(&e->rwlock);
                break;
            }
        }
        pthread_rwlock_unlock(&e->rwlock);
    }
}

/* Open + size + mmap a keyfile shard. Caller may NOT hold g_kfcache_lock when
   the file system call could block, so we do the heavy lifting outside the
   table mutex (matching bt_open_file's contract in btree.c). */
static int kf_open_file(const char *path, size_t slots_capacity, int writer,
                        int *out_fd, uint8_t **out_base, size_t *out_size,
                        dev_t *out_dev, ino_t *out_ino) {
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
    *out_dev = st.st_dev;
    *out_ino = st.st_ino;
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

static int kfcache_acquire_ex(SlotcaskKfHandle *h, const char *path,
                              size_t slots_capacity, int writer,
                              int nonblocking) {
    /* nonblocking is currently only exercised with writer=0 (see
       kfcache_try_acquire_rd). A writer=1,nonblocking=1 caller would
       still block on wrlock — not audited/supported; add tryrwlock
       handling here first if a future caller needs it. */
    h->slot = -1;
    h->writer = writer;
    h->fd = -1;
    h->hdr = NULL;
    h->map = NULL;
    h->map_size = 0;
    h->capacity = 0;

retry_kfcache_acquire:
    if (!g_kfcache) {
        if (nonblocking) { errno = EBUSY; return -1; }
        if (writer) {
            errno = ENODEV;
            return -1;
        }
        /* Read-only cache-disabled fallback: direct mmap, no locking. */
        int fd; uint8_t *base; size_t sz; dev_t dev; ino_t ino;
        if (kf_open_file(path, slots_capacity, writer, &fd, &base, &sz, &dev, &ino) < 0) return -1;
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
        if (writer) {
            pthread_rwlock_wrlock(lock);
        } else if (nonblocking) {
            if (pthread_rwlock_tryrdlock(lock) != 0) { errno = EBUSY; return -1; }
        } else {
            pthread_rwlock_rdlock(lock);
        }

        /* coverity[atomicity] CID 1693850: `slot` came from the prior
           locked section, but we re-verify identity below
           (e->used && strcmp(e->path, path) == 0). On mismatch we
           drop the rwlock and retry — the verify is the consistency
           barrier the analyzer can't see. */
        KfCacheEntry *e = &g_kfcache[slot];
        if (e->used && strcmp(e->path, path) == 0) {
            struct stat pst;
            if (stat(path, &pst) == 0 &&
                pst.st_dev == e->file_dev && pst.st_ino == e->file_ino) {
                h->slot = slot;
                kf_handle_from_entry(h, e);
                /* coverity[missing_unlock] intentional: returning with the
                   per-slot rwlock held; caller releases via kfcache_release. */
                return 0;
            }
            /* Cached entry no longer matches the file currently at
               `path` — e.g. rebuild_object_v2 renamed data/ away and
               recreated it after this entry was installed by a racing
               kfcache_acquire(writer=1) (commonly warmup's
               slotcask_open() fan-out). Evict it so the retry below (or
               the miss path) re-opens the real current file instead of
               aliasing stale pre-rebuild data. */
            pthread_rwlock_unlock(lock);
            if (nonblocking) { errno = EBUSY; return -1; }
            pthread_mutex_lock(&g_kfcache_lock);
            if (g_kfcache[slot].used && strcmp(g_kfcache[slot].path, path) == 0) {
                kfcache_drop_slot(slot, CACHE_DROP_DISCARD, 1);
            }
            if (++retries >= 4) break;
            continue;
        }
        pthread_rwlock_unlock(lock);
        if (nonblocking) { errno = EBUSY; return -1; }
        if (++retries >= 4) {
            /* slot/found get re-set by the kfcache_probe call below
               in the install path (Coverity CID 1693833). */
            pthread_mutex_lock(&g_kfcache_lock);
            break;
        }
        pthread_mutex_lock(&g_kfcache_lock);
    }

    /* Miss path: open + install. Drop table lock during open since it can
       block on disk. In nonblocking mode, bail here instead of opening —
       the "try" contract (kfcache_try_acquire_rd / kfcache_try_acquire_direct)
       is fast-path-only: it never touches the filesystem, so a genuine
       cold/evicted shard is treated exactly like lock contention. */
    if (nonblocking) {
        pthread_mutex_unlock(&g_kfcache_lock);
        errno = EBUSY;
        return -1;
    }
    pthread_mutex_unlock(&g_kfcache_lock);
    int fd; uint8_t *base; size_t sz; dev_t dev; ino_t ino;
    if (kf_open_file(path, slots_capacity, writer, &fd, &base, &sz, &dev, &ino) < 0) return -1;
    pthread_mutex_lock(&g_kfcache_lock);

    /* Re-probe — another thread may have installed it while we were opening. */
    slot = kfcache_probe(path, &found);
    if (found) {
        g_kfcache[slot].last_access =
            __atomic_add_fetch(&g_kfcache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &g_kfcache[slot].rwlock;
        pthread_mutex_unlock(&g_kfcache_lock);
        if (writer) {
            pthread_rwlock_wrlock(lock);
        } else if (nonblocking) {
            if (pthread_rwlock_tryrdlock(lock) != 0) {
                munmap(base, sz);
                close(fd);
                errno = EBUSY;
                return -1;
            }
        } else {
            pthread_rwlock_rdlock(lock);
        }
        KfCacheEntry *e = &g_kfcache[slot];
        int matched = e->used && strcmp(e->path, path) == 0;
        if (matched) {
            struct stat pst;
            if (stat(path, &pst) == 0 &&
                pst.st_dev == e->file_dev && pst.st_ino == e->file_ino) {
                /* Genuinely valid — lost the install race. Discard our
                   own open; use the cached entry. */
                munmap(base, sz);
                close(fd);
                h->slot = slot;
                kf_handle_from_entry(h, e);
                /* coverity[missing_unlock] intentional: returning with the
                   per-slot rwlock held; caller releases via kfcache_release. */
                return 0;
            }
        }
        pthread_rwlock_unlock(lock);
        if (matched) {
            /* The racing installer's entry is itself stale (same
               rebuild-rename race, one level deeper). Evict it so it
               can't alias stale data for the next caller. */
            pthread_mutex_lock(&g_kfcache_lock);
            if (g_kfcache[slot].used && strcmp(g_kfcache[slot].path, path) == 0) {
                kfcache_drop_slot(slot, CACHE_DROP_DISCARD, 1);
            }
            pthread_mutex_unlock(&g_kfcache_lock);
        }
        /* Slot was evicted under us, or was just evicted above for
           staleness. Our own fd/base/sz (opened moments ago) are
           current — serve them uncached this once instead of opening
           a third time. */
        if (writer) {
            munmap(base, sz);
            close(fd);
            goto retry_kfcache_acquire;
        }
        kf_handle_from_uncached(h, fd, base, sz);
        return 0;
    }

    /* Evict LRU if half-full or no empty slot. A failed dirty sync leaves
       that candidate installed; continue to another distinct victim. */
    if (slot < 0 || __atomic_load_n(&g_kfcache_count, __ATOMIC_RELAXED) >= g_kfcache_slots / 2) {
        slot = -1;
        int first_error = 0;
        int wait_candidate = -1;
        uint64_t floor_ts = 0;
        for (int attempt = 0; attempt < g_kfcache_slots; attempt++) {
            int lru = -1;
            uint64_t oldest = UINT64_MAX;
            for (int i = 0; i < g_kfcache_slots; i++) {
                if (g_kfcache[i].used &&
                    g_kfcache[i].last_access >= floor_ts &&
                    g_kfcache[i].last_access < oldest) {
                    oldest = g_kfcache[i].last_access;
                    lru = i;
                }
            }
            if (lru < 0) break;
            int drop_rc = kfcache_drop_slot(lru, CACHE_DROP_EVICT, 0);
            if (drop_rc > 0) {
                slot = lru;
                break;
            }
            if (drop_rc < 0 && first_error == 0) first_error = errno;
            if (drop_rc == 0 && wait_candidate < 0) wait_candidate = lru;
            floor_ts = oldest + 1;
        }
        if (slot < 0 && writer && wait_candidate >= 0) {
            int drop_rc = kfcache_drop_slot(wait_candidate, CACHE_DROP_EVICT, 1);
            if (drop_rc > 0) slot = wait_candidate;
            else if (drop_rc < 0 && first_error == 0) first_error = errno;
            else if (drop_rc == 0) first_error = 0;
        }
        if (slot < 0 && writer && first_error != 0) {
            pthread_mutex_unlock(&g_kfcache_lock);
            munmap(base, sz);
            close(fd);
            errno = first_error;
            return -1;
        }
    }

    if (slot < 0) {
        pthread_mutex_unlock(&g_kfcache_lock);
        if (writer) {
            munmap(base, sz);
            close(fd);
            goto retry_kfcache_acquire;
        }
        /* Cache truly full — read-only callers may serve uncached. */
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
    atomic_store_explicit(&e->dirty, 0, memory_order_relaxed);
    atomic_store_explicit(&e->dirty_since_ms, 0, memory_order_relaxed);
    e->used = 1;
    e->last_access = __atomic_add_fetch(&g_kfcache_clock, 1, __ATOMIC_RELAXED);
    e->file_dev = dev;
    e->file_ino = ino;
    __atomic_fetch_add(&g_kfcache_count, 1, __ATOMIC_RELAXED);

    /* Publish under the table mutex, then take the entry lock without holding
       the table mutex. An evictor that wins this race closes the just-opened
       mapping; identity verification detects that and retries safely. */
    pthread_rwlock_t *lock = &e->rwlock;
    pthread_mutex_unlock(&g_kfcache_lock);
    if (writer) {
        pthread_rwlock_wrlock(lock);
    } else if (nonblocking) {
        if (pthread_rwlock_tryrdlock(lock) != 0) { errno = EBUSY; return -1; }
    } else {
        pthread_rwlock_rdlock(lock);
    }

    if (!e->used || strcmp(e->path, path) != 0 || e->file_dev != dev ||
        e->file_ino != ino) {
        pthread_rwlock_unlock(lock);
        return kfcache_acquire_ex(h, path, slots_capacity, writer, nonblocking);
    }

    h->slot = slot;
    kf_handle_from_entry(h, e);
    return 0;
}

void kfcache_release(SlotcaskKfHandle *h) {
    if (h->slot >= 0) {
        if (h->writer) {
            KfCacheEntry *e = &g_kfcache[h->slot];
            durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
        }
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

int kfcache_acquire(SlotcaskKfHandle *h, const char *path,
                    size_t slots_capacity, int writer) {
    return kfcache_acquire_ex(h, path, slots_capacity, writer, 0);
}

/* Non-blocking reader acquire for callers that must not block while
   holding an unrelated lock (see btree_idx_walk_ordered's use via
   read_record_ref_try / slotcask_lookup_by_hash_try). Returns 0 on
   success, -1 with errno=EBUSY if the rdlock would have blocked, -1 with
   a different errno for a genuine I/O/OOM failure. */
int kfcache_try_acquire_rd(SlotcaskKfHandle *h, const char *path,
                           size_t slots_capacity) {
    return kfcache_acquire_ex(h, path, slots_capacity, 0, 1);
}

/* ── Marker file helpers (durability write-ordering intent) ── */

/* Set once by storage.c (via __attribute__((constructor))) before any
   recovery sweep can run. See declaration/rationale in
   shard_db_internal.h. */
RecoveryIndexDiffFn g_recovery_index_diff_fn = NULL;

static void kf_marker_path(char *buf, size_t cap, const char *data_dir,
                           int kf_shard) {
    snprintf(buf, cap, "%s/data/kf/%03x_marker.dat",
             data_dir, (unsigned)kf_shard);
}

static void kf_marker_dir_path(char *buf, size_t cap, const char *data_dir) {
    snprintf(buf, cap, "%s/data/kf", data_dir);
}

static int fsync_dir(const char *dir_path) {
    int dfd = open(dir_path, O_RDONLY | O_DIRECTORY);
    if (dfd < 0) return -1;
    int rc = fsync(dfd);
    close(dfd);
    return rc;
}

/* Accumulate time spent in marker and targeted kf durability barriers. */
static void commit_sync_us_record(uint64_t t0) {
    if (g_db) __atomic_add_fetch(&g_commit_sync_us_total, now_us() - t0, __ATOMIC_RELAXED);
}

/* ── Abort sidecars (durable abort decision paired with a marker) ──
 *
 * A sidecar is the durable, ordered decision that its paired commit-intent
 * marker must be ABORTED (inverse index diff applied, speculative NEW
 * segment tombstoned where one exists) instead of forward-replayed. The
 * write-time gates and the startup recovery sweep pair a marker with its
 * sidecar before choosing a direction: no sidecar → forward replay; valid
 * sidecar → abort; corrupt/short/extra evidence → kf_marker_fail_closed. */

static void kf_abort_path(char *buf, size_t cap, const char *data_dir,
                          uint16_t kind, int kf_shard, uint32_t batch_id) {
    if (kind == KF_ABORT_BATCH)
        snprintf(buf, cap, "%s/data/kf/%03x_batch_%u_abort.dat",
                 data_dir, (unsigned)kf_shard, batch_id);
    else
        snprintf(buf, cap, "%s/data/kf/%03x_marker_abort.dat",
                 data_dir, (unsigned)kf_shard);
}

/* Create one abort sidecar with O_EXCL: one complete pwrite, fsync(fd),
   close(fd), fsync_dir(data/kf). A pre-existing sidecar (O_EXCL → EEXIST)
   is never silently overwritten: the caller validates it via
   kf_abort_read_exact first (idempotent redo) and fails closed on any
   mismatch. */
static int kf_abort_write_sidecar_impl(const char *data_dir, uint16_t kind,
                                       int kf_shard, uint32_t batch_id,
                                       uint32_t marker_count) {
    char path[PATH_MAX], dpath[PATH_MAX];
    KfAbortHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic = KF_ABORT_MAGIC;
    hdr.version = KF_ABORT_VERSION;
    hdr.kind = kind;
    hdr.kf_shard = (uint32_t)kf_shard;
    hdr.batch_id = kind == KF_ABORT_BATCH ? batch_id : 0;
    hdr.marker_count = marker_count;
    hdr.checksum = XXH32(&hdr, offsetof(KfAbortHeader, checksum), 0);
    kf_abort_path(path, sizeof(path), data_dir, kind, kf_shard, batch_id);
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "abort-sidecar: open(%s) failed shard=%03x kind=%u "
                  "batch=%u errno=%d (%s)", path, kf_shard, kind, batch_id,
                  errno, strerror(errno));
        return -1;
    }
    ssize_t n = pwrite(fd, &hdr, sizeof(hdr), 0);
    if (n != (ssize_t)sizeof(hdr)) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "abort-sidecar: pwrite failed shard=%03x kind=%u "
                  "batch=%u errno=%d (%s)", kf_shard, kind, batch_id,
                  errno, strerror(errno));
        close(fd); return -1;
    }
    if (fsync(fd) != 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "abort-sidecar: fsync failed shard=%03x kind=%u "
                  "batch=%u errno=%d (%s)", kf_shard, kind, batch_id,
                  errno, strerror(errno));
        close(fd); return -1;
    }
    close(fd);
    kf_marker_dir_path(dpath, sizeof(dpath), data_dir);
    if (fsync_dir(dpath) != 0) return -1;
    durability_test_pause(data_dir, "abort-sidecar-after-fsync");
    return 0;
}

int kf_abort_write_sidecar(const char *data_dir, uint16_t kind, int kf_shard,
                           uint32_t batch_id, uint32_t marker_count) {
    uint64_t t0 = now_us();
    int rc = kf_abort_write_sidecar_impl(data_dir, kind, kf_shard, batch_id,
                                         marker_count);
    commit_sync_us_record(t0);
    return rc;
}

/* Parse one sidecar exactly: the file must be exactly sizeof(KfAbortHeader),
   every header field must match the requested (kind, shard, batch, count),
   and the checksum must verify over [0, offsetof(checksum)). Returns 0 on a
   valid match, 1 when the file is absent, -1 (errno set; EILSEQ for corrupt
   evidence) on anything else. Never modifies or unlinks the file. */
int kf_abort_read_exact(const char *path, uint16_t want_kind,
                        uint32_t want_shard, uint32_t want_batch,
                        uint32_t want_count, KfAbortHeader *out) {
    struct stat st;
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return errno == ENOENT ? 1 : -1;
    if (fstat(fd, &st) != 0) {
        /* fstat() itself failed — errno is reliably set by the failing
           syscall, never stale. Genuine transient I/O. */
        int saved = errno; close(fd); errno = saved; return -1;
    }
    if (st.st_size != (off_t)sizeof(*out)) {
        /* fstat() succeeded (doesn't touch errno on success) but the file
           is the wrong size — that's corrupt/truncated evidence, not an
           I/O error, regardless of whatever stale errno an earlier,
           unrelated syscall left lying around. Must not fall through to
           "errno ? errno : EILSEQ", which would misreport this as
           transient if errno happened to be nonzero from something else. */
        close(fd); errno = EILSEQ; return -1;
    }
    ssize_t n = pread(fd, out, sizeof(*out), 0);
    int saved = errno;
    close(fd);
    if (n != (ssize_t)sizeof(*out)) { errno = n < 0 ? saved : EILSEQ; return -1; }
    if (out->magic != KF_ABORT_MAGIC || out->version != KF_ABORT_VERSION ||
        out->kind != want_kind || out->kf_shard != want_shard ||
        out->batch_id != want_batch || out->marker_count != want_count ||
        out->checksum != XXH32(out, offsetof(KfAbortHeader, checksum), 0)) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "abort-sidecar: corrupt evidence at %s "
                  "(magic=%x ver=%u kind=%u shard=%u batch=%u count=%u "
                  "want_kind=%u want_shard=%u want_batch=%u want_count=%u)",
                  path, out->magic, out->version, out->kind,
                  out->kf_shard, out->batch_id, out->marker_count,
                  want_kind, want_shard, want_batch, want_count);
        errno = EILSEQ;
        return -1;
    }
    return 0;
}

/* Parse a sidecar header only (orphan-sidecar revalidation: the marker that
   would pin marker_count is already gone, so only the self-consistent fixed
   fields can be checked). Returns 0 valid, 1 absent, -1 corrupt/I-O. */
static int kf_abort_read_header(const char *path, KfAbortHeader *out) {
    struct stat st;
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return errno == ENOENT ? 1 : -1;
    if (fstat(fd, &st) != 0) {
        /* fstat() itself failed — errno is reliably set by the failing
           syscall, never stale. Genuine transient I/O. */
        int saved = errno; close(fd); errno = saved; return -1;
    }
    if (st.st_size != (off_t)sizeof(*out)) {
        /* fstat() succeeded (doesn't touch errno on success) but the file
           is the wrong size — that's corrupt/truncated evidence, not an
           I/O error, regardless of whatever stale errno an earlier,
           unrelated syscall left lying around. Must not fall through to
           "errno ? errno : EILSEQ", which would misreport this as
           transient if errno happened to be nonzero from something else. */
        close(fd); errno = EILSEQ; return -1;
    }
    ssize_t n = pread(fd, out, sizeof(*out), 0);
    int saved = errno;
    close(fd);
    if (n != (ssize_t)sizeof(*out)) { errno = n < 0 ? saved : EILSEQ; return -1; }
    if (out->magic != KF_ABORT_MAGIC || out->version != KF_ABORT_VERSION ||
        out->checksum != XXH32(out, offsetof(KfAbortHeader, checksum), 0)) {
        errno = EILSEQ;
        return -1;
    }
    return 0;
}

/* Cleanup step 3 of the binding order: unlink the sidecar and fsync
   data/kf. Only reachable after the forward marker was already unlinked and
   synced (step 2) — the crash-window pair "no marker + valid sidecar" is
   exactly this completed-abort state. */
int kf_abort_clear_after_marker(const char *abort_path, const char *kf_dir) {
    if (unlink(abort_path) != 0 && errno != ENOENT) {
        LOG_WARN(LOG_SUB_SLOTCASK,
                 "abort-sidecar: unlink(%s) failed errno=%d (%s)",
                 abort_path, errno, strerror(errno));
        return -1;
    }
    if (fsync_dir(kf_dir) != 0) {
        LOG_WARN(LOG_SUB_SLOTCASK,
                 "abort-sidecar: fsync_dir(%s) failed errno=%d (%s)",
                 kf_dir, errno, strerror(errno));
        return -1;
    }
    return 0;
}

/* Create-or-validate one abort sidecar against the requested parameters.
   An existing sidecar is only acceptable when every header field matches
   (idempotent redo of an earlier abort attempt); anything else is corrupt
   evidence and fails closed. Returns 0 on a valid, durable sidecar. */
static int kf_abort_sidecar_ensure(const char *data_dir, uint16_t kind,
        int kf_shard, uint32_t batch_id, uint32_t marker_count) {
    char abort_path[PATH_MAX];
    KfAbortHeader hdr;
    kf_abort_path(abort_path, sizeof(abort_path), data_dir, kind, kf_shard,
                  batch_id);
    int rc = kf_abort_read_exact(abort_path, kind, (uint32_t)kf_shard,
                                 batch_id, marker_count, &hdr);
    if (rc == 0) return 0;   /* already present and valid (redo) */
    if (rc == -1) return -1; /* corrupt or mismatched evidence */
    if (kf_abort_write_sidecar(data_dir, kind, kf_shard, batch_id,
                               marker_count) != 0) {
        /* Raced with a concurrent abort of the same pair: revalidate
           instead of treating our own create as the only winner. */
        rc = kf_abort_read_exact(abort_path, kind, (uint32_t)kf_shard,
                                 batch_id, marker_count, &hdr);
        return rc == 0 ? 0 : -1;
    }
    return 0;
}

/* Forward declarations for the recovery helpers defined after the marker
   replay block; the write-time gates run earlier in this file. */
static void kf_marker_fail_closed(const char *data_dir, int kf_shard,
                                  const char *why);
static int kf_marker_apply_abort_diff(const char *eff_root,
        const char *object, const char *data_dir, int kf_shard,
        uint32_t kf_slot, const KfMarkerSlot *marker);
static int seg_write_marker_new_tombstone_durable(const char *data_dir,
        const KfMarkerSlot *marker);

static int kf_marker_write_impl(const char *data_dir, int kf_shard,
                    const KfMarkerSlot *slot) {
    char path[PATH_MAX], dpath[PATH_MAX];
    KfMarkerSlot durable = *slot;
    durable.checksum = XXH32(&durable, offsetof(KfMarkerSlot, checksum), 0);
    kf_marker_path(path, sizeof(path), data_dir, kf_shard);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    ssize_t n = pwrite(fd, &durable, sizeof(durable), 0);
    if (n != (ssize_t)sizeof(durable)) { close(fd); return -1; }
    if (fsync(fd) != 0) { close(fd); return -1; }
    close(fd);
    kf_marker_dir_path(dpath, sizeof(dpath), data_dir);
    return fsync_dir(dpath);
}

int kf_marker_write(const char *data_dir, int kf_shard,
                    const KfMarkerSlot *slot) {
    uint64_t t0 = now_us();
    int rc = kf_marker_write_impl(data_dir, kf_shard, slot);
    commit_sync_us_record(t0);
    return rc;
}

static int kf_marker_clear_impl(const char *data_dir, int kf_shard) {
    char path[PATH_MAX], dpath[PATH_MAX];
    kf_marker_path(path, sizeof(path), data_dir, kf_shard);
    if (unlink(path) != 0 && errno != ENOENT) return -1;
    kf_marker_dir_path(dpath, sizeof(dpath), data_dir);
    return fsync_dir(dpath);
}

int kf_marker_clear(const char *data_dir, int kf_shard) {
    uint64_t t0 = now_us();
    int rc = kf_marker_clear_impl(data_dir, kf_shard);
    commit_sync_us_record(t0);
    return rc;
}

/* Return 0=valid, 1=absent, 2=zero-byte, -1=corrupt/I/O. */
int kf_marker_read(const char *data_dir, int kf_shard, KfMarkerSlot *out) {
    char path[PATH_MAX];
    struct stat st;
    kf_marker_path(path, sizeof(path), data_dir, kf_shard);
    int fd = open(path, O_RDONLY);
    if (fd < 0) return errno == ENOENT ? 1 : -1;
    if (fstat(fd, &st) != 0) { close(fd); return -1; }
    if (st.st_size == 0) { close(fd); return 2; }
    if (st.st_size != (off_t)sizeof(*out)) { close(fd); errno = EILSEQ; return -1; }
    ssize_t n = pread(fd, out, sizeof(*out), 0);
    int saved = errno;
    close(fd);
    if (n != (ssize_t)sizeof(*out)) { errno = n < 0 ? saved : EILSEQ; return -1; }
    if (out->magic != KF_MARKER_MAGIC ||
        out->checksum != XXH32(out, offsetof(KfMarkerSlot, checksum), 0) ||
        !kf_marker_op_valid(out)) {
        errno = EILSEQ;
        return -1;
    }
    return 0;
}

/* Sync only the pages containing the given kf slots, while h's writer
   lock remains held. header_changed: sync the 24-byte shard header too.
   h must be a writer-acquired handle with non-NULL hdr and map. */
static int kfcache_sync_slots_locked_impl(SlotcaskKfHandle *h,
                              const size_t *slots, size_t nslots,
                              int header_changed) {
    if (!h || !h->writer || !h->hdr || (!slots && nslots)) {
        errno = EINVAL;
        return -1;
    }
    for (size_t i = 0; i < nslots; i++) {
        if (slots[i] >= h->capacity) { errno = EINVAL; return -1; }
        size_t off = SLOTCASK_KF_HDR_SIZE + slots[i] * sizeof(*h->map);
        if (durability_msync_range(h->hdr, off, sizeof(*h->map)) < 0)
            return -1;
    }
    return !header_changed ||
           durability_msync_range(h->hdr, 0, SLOTCASK_KF_HDR_SIZE) == 0
               ? 0 : -1;
}

int kfcache_sync_slots_locked(SlotcaskKfHandle *h,
                              const size_t *slots, size_t nslots,
                              int header_changed) {
    uint64_t t0 = now_us();
    int rc = kfcache_sync_slots_locked_impl(h, slots, nslots, header_changed);
    commit_sync_us_record(t0);
    return rc;
}

/* data_dir is always eff_root/object (tenant dir + object name — see
   AGENTS.md storage model), so both can be recovered by splitting on the
   final path separator rather than threading extra fields through
   SlotcaskDb. Object names are validated via valid_filename() and never
   contain '/', so this split is unambiguous. */
static void split_data_dir(const char *data_dir, char *eff_root, size_t eff_root_len,
                            char *object, size_t object_len) {
    const char *slash = strrchr(data_dir, '/');
    if (!slash) {
        if (eff_root_len) eff_root[0] = '\0';
        snprintf(object, object_len, "%s", data_dir);
        return;
    }
    size_t root_len = (size_t)(slash - data_dir);
    if (root_len >= eff_root_len) root_len = eff_root_len - 1;
    memcpy(eff_root, data_dir, root_len);
    eff_root[root_len] = '\0';
    snprintf(object, object_len, "%s", slash + 1);
}

/* Apply a pinned single-record abort while the kf writer lock is held:
   inverse index diff, speculative NEW segment tombstone (upserts only),
   then the binding cleanup order — unlink the forward marker and fsync,
   then unlink the abort sidecar and fsync. */
static int kf_marker_abort_single_locked(const char *eff_root,
        const char *object, const char *data_dir, int kf_shard,
        SlotcaskKfHandle *kh, const KfAbortHeader *hdr,
        const char *marker_path, const char *abort_path) {
    char kf_dir[PATH_MAX];
    KfMarkerSlot marker;

    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", data_dir);
    if ((hdr && hdr->marker_count != 1) ||
        kf_marker_read(data_dir, kf_shard, &marker) != 0) {
        kf_marker_fail_closed(data_dir, kf_shard,
                              "abort sidecar without its marker");
        return -1;
    }
    if (kf_marker_apply_abort_diff(eff_root, object, data_dir, kf_shard,
                                   marker.kf_slot, &marker) != 0 ||
        (marker.op != KF_MARKER_OP_DELETE &&
         seg_write_marker_new_tombstone_durable(data_dir, &marker) != 0)) {
        kf_marker_fail_closed(data_dir, kf_shard, "abort recovery");
        return -1;
    }
    if (unlink(marker_path) != 0 || fsync_dir(kf_dir) != 0) {
        kf_marker_fail_closed(data_dir, kf_shard, "marker unlink after abort");
        return -1;
    }
    if (kf_abort_clear_after_marker(abort_path, kf_dir) != 0) {
        kf_marker_fail_closed(data_dir, kf_shard, "sidecar unlink after abort");
        return -1;
    }
    return 0;
}

/* Write-time single-record abort: ensure the abort sidecar is durable
   (create or revalidate), then apply the inverse diff, tombstone the
   speculative NEW segment record, and clear marker + sidecar while the kf
   writer lock is held. Called by an insert/update producer whose indexed
   apply failed after the commit-intent marker was fsynced. On any abort
   failure the marker and sidecar are retained and the process aborts via
   kf_marker_fail_closed, so startup recovery re-runs the same inverse. */
static int kf_marker_abort_single_current_locked(const char *data_dir,
        int kf_shard, const KfMarkerSlot *marker) {
    char eff_root[PATH_MAX], object[256];
    char marker_path[PATH_MAX], abort_path[PATH_MAX];

    if (kf_abort_sidecar_ensure(data_dir, KF_ABORT_SINGLE, kf_shard, 0, 1) != 0) {
        kf_marker_fail_closed(data_dir, kf_shard, "abort sidecar write");
        return -1;
    }
    split_data_dir(data_dir, eff_root, sizeof(eff_root), object, sizeof(object));
    kf_marker_path(marker_path, sizeof(marker_path), data_dir, kf_shard);
    kf_abort_path(abort_path, sizeof(abort_path), data_dir, KF_ABORT_SINGLE,
                  kf_shard, 0);
    return kf_marker_abort_single_locked(eff_root, object, data_dir, kf_shard,
                                         NULL, NULL, marker_path, abort_path);
}

/* Retained-marker gate: check for existing marker before allowing a new write.
   Must be called while kf writer lock is held, before any marker is created.
   Returns 0 to proceed with new write, -1 to fail and abort.
   On success, caller may proceed; if replay was needed, marker is now cleared. */
static int kf_marker_gate(int kf_shard, SlotcaskKfHandle *kh,
                           const char *data_dir) {
    KfMarkerSlot marker;
    KfAbortHeader hdr;
    char abort_path[PATH_MAX];
    int rc = kf_marker_read(data_dir, kf_shard, &marker);

    if (rc != 0) {
        /* No (1), torn (2), or corrupt (-1) marker. A sidecar here only
           means completed cleanup when the marker file is fully gone; a
           torn or corrupt marker beside a sidecar is corrupt evidence. */
        kf_abort_path(abort_path, sizeof(abort_path), data_dir,
                      KF_ABORT_SINGLE, kf_shard, 0);
        int arc = kf_abort_read_exact(abort_path, KF_ABORT_SINGLE,
                                      (uint32_t)kf_shard, 0, 1, &hdr);
        if (arc == 0) {
            if (rc == 1) {
                /* Orphan sidecar: the abort completed and the forward
                   marker was already unlinked (cleanup step 2 before 3).
                   Revalidate-and-clear is the only allowed removal. */
                char kf_dir[PATH_MAX];
                snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", data_dir);
                if (kf_abort_clear_after_marker(abort_path, kf_dir) != 0)
                    return -1;
                return 0;
            }
            kf_marker_fail_closed(data_dir, kf_shard,
                                  "torn/corrupt marker with abort sidecar");
            return -1;
        }
        if (arc == -1) {
            if (errno != EILSEQ) {
                /* Transient I/O (EIO, EMFILE, EACCES, ...) reading the
                   sidecar — not evidence of corruption. Fail only this
                   gate check so the caller's write fails cleanly and a
                   later retry can re-read once the condition clears;
                   terminating the daemon over a momentary I/O hiccup on
                   every write's gate check would turn a transient error
                   into a full outage. */
                LOG_WARN(LOG_SUB_SLOTCASK,
                        "abort-sidecar: transient I/O reading %s errno=%d (%s); "
                        "failing this write, not terminating", abort_path,
                        errno, strerror(errno));
                return -1;
            }
            kf_marker_fail_closed(data_dir, kf_shard,
                                  "corrupt abort sidecar");
            return -1;
        }
        if (rc == 1) {
            /* No marker present (common case) — proceed. */
            return 0;
        }
        if (rc == 2) {
            /* Zero-byte torn create (crash before fsync) — safe to unlink. */
            return kf_marker_clear(data_dir, kf_shard);
        }
        /* rc == -1: corrupt marker, no sidecar. Leave it untouched and
           fail closed — this signals an operator-visible issue that must
           be investigated. */
        return -1;
    }

    /* Valid marker present — decide direction from its paired sidecar. */
    kf_abort_path(abort_path, sizeof(abort_path), data_dir, KF_ABORT_SINGLE,
                  kf_shard, 0);
    int arc = kf_abort_read_exact(abort_path, KF_ABORT_SINGLE,
                                  (uint32_t)kf_shard, 0, 1, &hdr);
    if (arc == 0) {
        char eff_root[PATH_MAX], object[256];
        char marker_path[PATH_MAX];
        split_data_dir(data_dir, eff_root, sizeof(eff_root), object,
                       sizeof(object));
        kf_marker_path(marker_path, sizeof(marker_path), data_dir, kf_shard);
        return kf_marker_abort_single_locked(eff_root, object, data_dir,
                                             kf_shard, kh, &hdr,
                                             marker_path, abort_path);
    }
    if (arc == -1) {
        if (errno != EILSEQ) {
            LOG_WARN(LOG_SUB_SLOTCASK,
                    "abort-sidecar: transient I/O reading %s errno=%d (%s); "
                    "failing this write, not terminating", abort_path,
                    errno, strerror(errno));
            return -1;
        }
        kf_marker_fail_closed(data_dir, kf_shard, "corrupt abort sidecar");
        return -1;
    }
    /* No sidecar — the marker predates any abort decision; forward replay. */
    char eff_root[PATH_MAX], object[256];
    split_data_dir(data_dir, eff_root, sizeof(eff_root), object, sizeof(object));
    int replay_rc = kf_marker_replay_locked(eff_root, object, data_dir, kf_shard, kh, &marker);
    if (replay_rc != 0) return -1;
    /* Replay succeeded and cleared marker. */
    return 0;
}

/* A marker is the durable commit-intent point: once its fsync has returned,
   the operation it describes is never rolled back. If a kf or index step
   fails afterward, the only correct recovery is the same synchronous replay
   startup recovery uses, run here while the kf writer lock is still held.
   If replay still cannot converge, the daemon must not silently discard the
   marker or hand the caller an ordinary failure for an operation whose
   commit-intent is already durable — it fails closed so the next start's
   mandatory recovery sweep finishes the job. */
static int kf_marker_replay_current(const char *data_dir, int kf_shard,
                                     SlotcaskKfHandle *kh,
                                     const KfMarkerSlot *marker) {
    char eff_root[PATH_MAX], object[256];
    split_data_dir(data_dir, eff_root, sizeof(eff_root), object, sizeof(object));
    return kf_marker_replay_locked(eff_root, object, data_dir, kf_shard, kh, marker);
}

static void kf_marker_fail_closed(const char *data_dir, int kf_shard, const char *why) {
    LOG_ERROR(LOG_SUB_SLOTCASK,
              "commit-intent marker for kf shard %03x under %s could not be "
              "replayed post-fsync (%s); terminating so the next start's "
              "recovery sweep completes it rather than serving unknown state",
              kf_shard, data_dir, why);
    abort();
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
static int kfcache_acquire_direct_ex(SlotcaskKfHandle *h, SlotRef *ref,
                                     const char *path, size_t slots_capacity,
                                     void *db, int kf_shard_id, int nonblocking) {
    (void)db;          /* used only to make the signature future-proof */
    (void)kf_shard_id; /* same */

    if (ref && ref->slot >= 0) {
        int s = ref->slot;
        KfCacheEntry *e = &g_kfcache[s];
        uint64_t cur_gen = atomic_load_explicit(&e->gen, memory_order_acquire);
        if (cur_gen == ref->gen) {
            /* Gen matches — slot should still hold our entry.
               Take rdlock and verify identity before returning. */
            if (nonblocking) {
                if (pthread_rwlock_tryrdlock(&e->rwlock) != 0) {
                    errno = EBUSY;
                    return -1;
                }
            } else {
                pthread_rwlock_rdlock(&e->rwlock);
            }
            if (atomic_load_explicit(&e->used, memory_order_acquire) &&
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
    int rc = kfcache_acquire_ex(h, path, slots_capacity, 0, nonblocking);
    if (rc == 0 && ref && h->slot >= 0) {
        ref->slot = h->slot;
        ref->gen  = atomic_load_explicit(&g_kfcache[h->slot].gen,
                                          memory_order_acquire);
    }
    return rc;
}

int kfcache_acquire_direct(SlotcaskKfHandle *h, SlotRef *ref,
                            const char *path, size_t slots_capacity,
                            void *db, int kf_shard_id) {
    return kfcache_acquire_direct_ex(h, ref, path, slots_capacity,
                                     db, kf_shard_id, 0);
}

/* Non-blocking counterpart. Returns 0 on success, -1 with errno=EBUSY if
   the rdlock would have blocked. See kfcache_try_acquire_rd. */
int kfcache_try_acquire_direct(SlotcaskKfHandle *h, SlotRef *ref,
                                const char *path, size_t slots_capacity,
                                void *db, int kf_shard_id) {
    return kfcache_acquire_direct_ex(h, ref, path, slots_capacity,
                                     db, kf_shard_id, 1);
}

/* ============================================================ segcache */
/* SegCacheEntry moved to shard_db_internal.h; g_segcache* moved to ShardDb struct */

#ifdef TEST_BUILD
static _Atomic int g_segcache_test_identity_mismatches;

void segcache_test_force_identity_mismatches(int count) {
    atomic_store_explicit(&g_segcache_test_identity_mismatches,
                          count > 0 ? count : 0, memory_order_release);
}

int segcache_test_identity_mismatches_remaining(void) {
    return atomic_load_explicit(&g_segcache_test_identity_mismatches,
                                memory_order_acquire);
}

static int segcache_test_consume_identity_mismatch(void) {
    int remaining = atomic_load_explicit(&g_segcache_test_identity_mismatches,
                                         memory_order_acquire);
    while (remaining > 0) {
        if (atomic_compare_exchange_weak_explicit(
                &g_segcache_test_identity_mismatches, &remaining,
                remaining - 1, memory_order_acq_rel, memory_order_acquire)) {
            return 1;
        }
    }
    return 0;
}
#else
static int segcache_test_consume_identity_mismatch(void) { return 0; }
#endif

#ifdef TEST_BUILD
/* One-shot TEST_BUILD pause hook: the invocation atomically takes and
   clears the stored function/context pair before calling it, so at most
   one call site per install fires (mirrors btree_test_set_after_extract_hook,
   plus the consume semantics of segcache_test_consume_identity_mismatch). */
static pthread_mutex_t g_after_old_lock = PTHREAD_MUTEX_INITIALIZER;
static slotcask_test_after_old_fn g_after_old_fn = NULL;
static void *g_after_old_ctx = NULL;

void slotcask_test_set_after_old_hook(slotcask_test_after_old_fn fn, void *ctx) {
    pthread_mutex_lock(&g_after_old_lock);
    g_after_old_fn = fn;
    g_after_old_ctx = ctx;
    pthread_mutex_unlock(&g_after_old_lock);
}

void slotcask_test_after_old(int under_kf_wrlock) {
    slotcask_test_after_old_fn fn;
    void *ctx;
    pthread_mutex_lock(&g_after_old_lock);
    fn = g_after_old_fn;
    ctx = g_after_old_ctx;
    g_after_old_fn = NULL;
    g_after_old_ctx = NULL;
    pthread_mutex_unlock(&g_after_old_lock);
    if (fn) fn(under_kf_wrlock, ctx);
}
#endif

#ifdef TEST_BUILD
static _Atomic size_t g_slotcask_test_seg_max_bytes = 0;

size_t slotcask_seg_max_bytes(void) {
    size_t bytes = atomic_load_explicit(&g_slotcask_test_seg_max_bytes,
                                        memory_order_acquire);
    return bytes ? bytes : SLOTCASK_SEG_MAX_BYTES;
}

void slotcask_test_set_seg_max_bytes(size_t bytes) {
    atomic_store_explicit(&g_slotcask_test_seg_max_bytes, bytes,
                          memory_order_release);
}
#endif

void segcache_init(int cap) {
    if (g_segcache) return;
    if (cap < 16) cap = 16;
    g_segcache_slots = next_pow2(cap * 2);
    g_segcache = calloc(g_segcache_slots, sizeof(SegCacheEntry));
    g_segcache_count = 0;
    for (int i = 0; i < g_segcache_slots; i++) {
        rwlock_init_writer_preferring(&g_segcache[i].rwlock);
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

/* Mirrors kfcache_invalidate_prefix -- drop every cached segment under a
   given path prefix while holding only the matching entry rwlock. */
static void segcache_invalidate_prefix(const char *prefix) {
    if (!g_segcache || !prefix || !prefix[0]) return;
    size_t pl = strlen(prefix);
    for (int i = 0; i < g_segcache_slots; i++) {
        SegCacheEntry *e = &g_segcache[i];
        if (!atomic_load_explicit(&e->used, memory_order_acquire)) continue;
        if (strncmp(e->path, prefix, pl) != 0) continue;
        pthread_rwlock_wrlock(&e->rwlock);
        if (atomic_load_explicit(&e->used, memory_order_acquire) &&
            strncmp(e->path, prefix, pl) == 0) {
            /* Structural discard: the caller is deleting or has already
               durably published a replacement under the object wrlock. */
            if (e->map) munmap(e->map, e->map_size);
            if (e->fd >= 0) close(e->fd);
            e->map = NULL;
            e->fd = -1;
            e->map_size = 0;
            e->path[0] = '\0';
            atomic_store_explicit(&e->dirty, 0, memory_order_relaxed);
            atomic_store_explicit(&e->dirty_since_ms, 0, memory_order_relaxed);
            atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
            atomic_store_explicit(&e->used, 0, memory_order_release);
            __sync_fetch_and_sub(&g_segcache_count, 1);
        }
        pthread_rwlock_unlock(&e->rwlock);
    }
}

/* Caller holds g_segcache_lock. Returns with it held. See the kf-cache
   equivalent for the entry -> table lock-order invariant. */
static int segcache_drop_slot(int slot, CacheDropReason reason, int wait) {
    SegCacheEntry *e = &g_segcache[slot];
    if (!e->used) return 1;
    uint64_t expected_gen = atomic_load_explicit(&e->gen, memory_order_acquire);
    char expected_path[PATH_MAX];
    snprintf(expected_path, sizeof(expected_path), "%s", e->path);
    /* See kfcache_drop_slot's comment: exclude any thread still holding
       this slot's rwlock from an earlier segcache_acquire() before
       munmapping under it — the caller only holds g_segcache_lock, which
       guards slot-table bookkeeping, not live e->map access. */
    pthread_mutex_unlock(&g_segcache_lock);
    int lock_rc = wait ? pthread_rwlock_wrlock(&e->rwlock)
                       : pthread_rwlock_trywrlock(&e->rwlock);
    pthread_mutex_lock(&g_segcache_lock);
    if (lock_rc != 0) return 0;
    if (!e->used ||
        atomic_load_explicit(&e->gen, memory_order_acquire) != expected_gen ||
        strcmp(e->path, expected_path) != 0) {
        pthread_rwlock_unlock(&e->rwlock);
        return 0;
    }
    if (reason == CACHE_DROP_EVICT && e->map && e->map_size > 0 &&
        durability_flush_dirty(&e->dirty, &e->dirty_since_ms,
                               e->map, e->map_size) < 0) {
        pthread_rwlock_unlock(&e->rwlock);
        return -1;
    }
    if (e->map) munmap(e->map, e->map_size);
    if (e->fd >= 0) close(e->fd);
    e->map = NULL;
    e->fd = -1;
    e->map_size = 0;
    atomic_store_explicit(&e->dirty, 0, memory_order_relaxed);
    atomic_store_explicit(&e->dirty_since_ms, 0, memory_order_relaxed);
    e->used = 0;
    e->path[0] = '\0';
    /* Increment gen under g_segcache_lock (caller always holds it).
       Any SlotRef pointing at this slot will fail its gen check, forcing
       the slow-path re-probe. */
    atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
    g_segcache_count--;
    pthread_rwlock_unlock(&e->rwlock);
    return 1;
}

/* Open + ftruncate to slotcask_seg_max_bytes() (sparse) + mmap MAP_SHARED. */
static int seg_open_file(const char *path, int create,
                         int *out_fd, uint8_t **out_map, size_t *out_size,
                         dev_t *out_dev, ino_t *out_ino) {
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

    size_t seg_max = slotcask_seg_max_bytes();
    struct stat st;
    if (fstat(fd, &st) < 0) { close(fd); return -1; }
    if ((size_t)st.st_size < seg_max) {
        if (!create) { close(fd); return -1; }
        if (ftruncate(fd, (off_t)seg_max) < 0) {
            close(fd); return -1;
        }
    }
    void *m = mmap(NULL, seg_max, PROT_READ | PROT_WRITE,
                   MAP_SHARED, fd, 0);
    if (m == MAP_FAILED) { close(fd); return -1; }
    /* Transparent huge pages hint — segments are 128 MB sparse files
       walked sequentially during scans and randomly during point reads.
       2 MB hugepages (vs 4 KB) cut TLB entries by 500× over the
       working set. Kernel ignores if THP is off; no functional impact. */
    SHARD_MADV_HUGEPAGE(m, seg_max);
    *out_fd = fd;
    *out_map = (uint8_t *)m;
    *out_size = seg_max;
    *out_dev = st.st_dev;
    *out_ino = st.st_ino;
    return 0;
}

int segcache_acquire(SlotcaskSegHandle *h, const char *path,
                     int create, int writer, int must_cache) {
    h->slot = -1;
    h->writer = writer;
    h->fd = -1;
    h->map = NULL;
    h->map_size = 0;

retry_segcache_acquire:
    if (!g_segcache) {
        if (must_cache) {
            errno = ENODEV;
            return -1;
        }
        dev_t dev; ino_t ino;
        if (seg_open_file(path, create, &h->fd, &h->map, &h->map_size, &dev, &ino) < 0) return -1;
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
        if (segcache_test_consume_identity_mismatch()) {
            pthread_rwlock_unlock(lock);
            if (++retries >= 4) {
                pthread_mutex_lock(&g_segcache_lock);
                break;
            }
            pthread_mutex_lock(&g_segcache_lock);
            continue;
        }
        if (e->used && strcmp(e->path, path) == 0) {
            struct stat pst;
            if (stat(path, &pst) == 0 &&
                pst.st_dev == e->file_dev && pst.st_ino == e->file_ino) {
                h->slot = slot;
                h->fd = e->fd;
                h->map = e->map;
                h->map_size = e->map_size;
                /* coverity[missing_unlock] intentional: returning with the
                   per-slot rwlock held; caller releases via segcache_release. */
                return 0;
            }
            /* Cached entry no longer matches the file currently at
               `path` — same rebuild-rename staleness class as
               kfcache_acquire. Evict it so the retry below (or the miss
               path) re-opens the real current file. */
            pthread_rwlock_unlock(lock);
            pthread_mutex_lock(&g_segcache_lock);
            if (g_segcache[slot].used && strcmp(g_segcache[slot].path, path) == 0) {
                segcache_drop_slot(slot, CACHE_DROP_DISCARD, 1);
            }
            if (++retries >= 4) break;
            continue;
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
    int fd; uint8_t *map; size_t sz; dev_t dev; ino_t ino;
    if (seg_open_file(path, create, &fd, &map, &sz, &dev, &ino) < 0) return -1;
    pthread_mutex_lock(&g_segcache_lock);

    slot = segcache_probe(path, &found);
    if (found) {
        g_segcache[slot].last_access =
            __atomic_add_fetch(&g_segcache_clock, 1, __ATOMIC_RELAXED);
        pthread_rwlock_t *lock = &g_segcache[slot].rwlock;
        pthread_mutex_unlock(&g_segcache_lock);
        if (writer) pthread_rwlock_wrlock(lock);
        else        pthread_rwlock_rdlock(lock);
        SegCacheEntry *e = &g_segcache[slot];
        int matched = !segcache_test_consume_identity_mismatch() &&
                      e->used && strcmp(e->path, path) == 0;
        if (matched) {
            struct stat pst;
            if (stat(path, &pst) == 0 &&
                pst.st_dev == e->file_dev && pst.st_ino == e->file_ino) {
                munmap(map, sz);
                close(fd);
                h->slot = slot;
                h->fd = e->fd;
                h->map = e->map;
                h->map_size = e->map_size;
                /* coverity[missing_unlock] intentional: returning with the
                   per-slot rwlock held; caller releases via segcache_release. */
                return 0;
            }
        }
        pthread_rwlock_unlock(lock);
        if (matched) {
            pthread_mutex_lock(&g_segcache_lock);
            if (g_segcache[slot].used && strcmp(g_segcache[slot].path, path) == 0) {
                segcache_drop_slot(slot, CACHE_DROP_DISCARD, 1);
            }
            pthread_mutex_unlock(&g_segcache_lock);
        }
        h->slot = -1;
        h->fd = fd;
        h->map = map;
        h->map_size = sz;
        if (must_cache) {
            munmap(map, sz);
            close(fd);
            h->fd = -1;
            h->map = NULL;
            h->map_size = 0;
            goto retry_segcache_acquire;
        }
        return 0;
    }

    if (slot < 0 || g_segcache_count >= g_segcache_slots / 2) {
        slot = -1;
        int first_error = 0;
        int wait_candidate = -1;
        uint64_t floor_ts = 0;
        for (int attempt = 0; attempt < g_segcache_slots; attempt++) {
            int lru = -1;
            uint64_t oldest = UINT64_MAX;
            for (int i = 0; i < g_segcache_slots; i++) {
                if (g_segcache[i].used &&
                    g_segcache[i].last_access >= floor_ts &&
                    g_segcache[i].last_access < oldest) {
                    oldest = g_segcache[i].last_access;
                    lru = i;
                }
            }
            if (lru < 0) break;
            int drop_rc = segcache_drop_slot(lru, CACHE_DROP_EVICT, 0);
            if (drop_rc > 0) {
                slot = lru;
                break;
            }
            if (drop_rc < 0 && first_error == 0) first_error = errno;
            if (drop_rc == 0 && wait_candidate < 0) wait_candidate = lru;
            floor_ts = oldest + 1;
        }
        if (slot < 0 && must_cache && wait_candidate >= 0) {
            int drop_rc = segcache_drop_slot(wait_candidate,
                                             CACHE_DROP_EVICT, 1);
            if (drop_rc > 0) slot = wait_candidate;
            else if (drop_rc < 0 && first_error == 0) first_error = errno;
            else if (drop_rc == 0) first_error = 0;
        }
        if (slot < 0 && must_cache && first_error != 0) {
            pthread_mutex_unlock(&g_segcache_lock);
            munmap(map, sz);
            close(fd);
            errno = first_error;
            return -1;
        }
    }

    if (slot < 0) {
        pthread_mutex_unlock(&g_segcache_lock);
        if (must_cache) {
            munmap(map, sz);
            close(fd);
            goto retry_segcache_acquire;
        }
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
    atomic_store_explicit(&e->dirty, 0, memory_order_relaxed);
    atomic_store_explicit(&e->dirty_since_ms, 0, memory_order_relaxed);
    e->used = 1;
    e->last_access = __atomic_add_fetch(&g_segcache_clock, 1, __ATOMIC_RELAXED);
    e->file_dev = dev;
    e->file_ino = ino;
    g_segcache_count++;

    pthread_rwlock_t *lock = &e->rwlock;
    pthread_mutex_unlock(&g_segcache_lock);
    if (writer) pthread_rwlock_wrlock(lock);
    else        pthread_rwlock_rdlock(lock);

    if (segcache_test_consume_identity_mismatch() ||
        !e->used || strcmp(e->path, path) != 0 || e->file_dev != dev ||
        e->file_ino != ino) {
        pthread_rwlock_unlock(lock);
        return segcache_acquire(h, path, create, writer, must_cache);
    }

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
            if (atomic_load_explicit(&e->used, memory_order_acquire) &&
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
    int rc = segcache_acquire(h, path, 0, 0, 0);
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
    if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;
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

    /* The replacement was MS_SYNC'd and renamed before this old-inode
       mapping is discarded, so no flush of the superseded inode is needed. */
    if (e->base) munmap(e->base, e->map_size);
    if (e->fd >= 0) close(e->fd);

    e->fd = reopen_fd;
    e->base = (uint8_t *)fresh;
    e->map_size = new_size;
    e->capacity = new_cap;
    atomic_store_explicit(&e->dirty, 0, memory_order_relaxed);
    atomic_store_explicit(&e->dirty_since_ms, 0, memory_order_relaxed);

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
static int pool_push_free_cap(SlotcaskStream *p, uint16_t file_id,
                               uint32_t offset, uint32_t capacity,
                               int max_slot_size);

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
            pool_push_free_cap(&a->db->streams[kf[i].stream_id],
                               kf[i].file_id, kf[i].offset,
                               (uint32_t)a->db->slot_size, a->db->slot_size);
        }
    }
    kfcache_release(&kh);
    return NULL;
}

/* Non-mutating probe result for a not-yet-committed new-key insert. Produced
   by kf_plan_insert_slot / kf_plan_window_insert_slot, consumed by
   kf_commit_planned_slot. Carries the (hash, key, klen) that produced it so
   window-local duplicate detection can compare in-memory instead of doing
   I/O via verify_stored_key. */
typedef struct {
    size_t      target_slot;
    int         reused_tomb;   /* 1 = target was a tombstone (deleted--), 0 = fresh (total++) */
    uint8_t     hash[16];
    const void *key;           /* borrowed; valid for the caller's window/call lifetime */
    size_t      klen;
} KfInsertPlan;

/* Non-mutating probe for a brand-new kf entry. Finds the slot a subsequent
   kf_commit_planned_slot call should write to, without touching the table.
   Returns 0 (planned into *out_plan), 1 (key already live — duplicate), or
   -1 (shard full / error). Triggers the same load-based resplit as the old
   kf_put_new — resplit only grows capacity/reshapes existing entries, it
   never touches this key's own (not-yet-existing) entry, so doing it during
   planning is safe. */
static int kf_plan_insert_slot(SlotcaskDb *db, SlotcaskKfHandle *kh, const uint8_t hash[16],
                                const void *key, size_t klen, const char *data_dir,
                                KfInsertPlan *out_plan) {
    (void)db;  /* db param retained for ABI; per-shard load lives in the header */
    if (kh->hdr) {
        uint64_t total = kh->hdr->total;
        uint64_t cap = (uint64_t)kh->capacity;
        if (cap > 0 && total * 4 >= cap * 3) {
            (void)kfcache_resplit_locked(kh, kh->capacity * 2);
        }
    }

    size_t cap = kh->capacity;
    if (cap == 0) return -1;
    SlotcaskKfEntry *kf = kh->map;
    size_t start = kf_slot_for(hash, cap);
    size_t first_tomb = (size_t)-1;
    for (size_t i = 0; i < cap; i++) {
        size_t slot = (start + i) % cap;
        SlotcaskKfEntry *e = &kf[slot];
        if (e->flag == 0) {
            out_plan->target_slot = (first_tomb != (size_t)-1) ? first_tomb : slot;
            out_plan->reused_tomb = (first_tomb != (size_t)-1);
            memcpy(out_plan->hash, hash, 16);
            out_plan->key = key;
            out_plan->klen = klen;
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
            out_plan->target_slot = slot;
            out_plan->reused_tomb = 1;
            memcpy(out_plan->hash, hash, 16);
            out_plan->key = key;
            out_plan->klen = klen;
            return 0;
        }
        if (e->flag == 2 && first_tomb == (size_t)-1) {
            first_tomb = slot;
        }
    }
    if (first_tomb != (size_t)-1) {
        out_plan->target_slot = first_tomb;
        out_plan->reused_tomb = 1;
        memcpy(out_plan->hash, hash, 16);
        out_plan->key = key;
        out_plan->klen = klen;
        return 0;
    }
    return -1;
}

/* Window-scoped variant of kf_plan_insert_slot: `reserved`/`nreserved` are
   plans already produced earlier in the same not-yet-committed bulk window.
   A slot claimed by an earlier plan is treated as occupied (matches the
   post-commit table state those earlier plans will produce), and a
   duplicate key within the window is detected in-memory (no I/O — the
   duplicate's probe necessarily reaches the earlier plan's target_slot
   first, since identical keys share the same hash and probe start). */
static int kf_plan_window_insert_slot(SlotcaskDb *db, SlotcaskKfHandle *kh,
                                       const uint8_t hash[16],
                                       const void *key, size_t klen,
                                       const char *data_dir,
                                       const KfInsertPlan *reserved, size_t nreserved,
                                       KfInsertPlan *out_plan) {
    (void)db;
    if (kh->hdr) {
        uint64_t total = kh->hdr->total;
        uint64_t cap = (uint64_t)kh->capacity;
        if (cap > 0 && total * 4 >= cap * 3) {
            (void)kfcache_resplit_locked(kh, kh->capacity * 2);
        }
    }

    size_t cap = kh->capacity;
    if (cap == 0) return -1;
    SlotcaskKfEntry *kf = kh->map;
    size_t start = kf_slot_for(hash, cap);
    size_t first_tomb = (size_t)-1;
    for (size_t i = 0; i < cap; i++) {
        size_t slot = (start + i) % cap;

        int reserved_here = 0;
        for (size_t r = 0; r < nreserved; r++) {
            if (reserved[r].target_slot != slot) continue;
            reserved_here = 1;
            if (memcmp(reserved[r].hash, hash, 16) == 0 &&
                reserved[r].klen == klen &&
                memcmp(reserved[r].key, key, klen) == 0) {
                return 1; /* duplicate key within this window */
            }
            break;
        }
        if (reserved_here) continue; /* treat as occupied, keep probing */

        SlotcaskKfEntry *e = &kf[slot];
        if (e->flag == 0) {
            out_plan->target_slot = (first_tomb != (size_t)-1) ? first_tomb : slot;
            out_plan->reused_tomb = (first_tomb != (size_t)-1);
            memcpy(out_plan->hash, hash, 16);
            out_plan->key = key;
            out_plan->klen = klen;
            return 0;
        }
        if (e->flag == 1 && memcmp(e->hash, hash, 16) == 0) {
            int km = verify_stored_key(data_dir, e->stream_id, e->file_id,
                                       e->offset, key, klen);
            if (km < 0) return -1;
            if (km == 1) return 1;
        }
        if (e->flag == 2 && memcmp(e->hash, hash, 16) == 0) {
            out_plan->target_slot = slot;
            out_plan->reused_tomb = 1;
            memcpy(out_plan->hash, hash, 16);
            out_plan->key = key;
            out_plan->klen = klen;
            return 0;
        }
        if (e->flag == 2 && first_tomb == (size_t)-1) {
            first_tomb = slot;
        }
    }
    if (first_tomb != (size_t)-1) {
        out_plan->target_slot = first_tomb;
        out_plan->reused_tomb = 1;
        memcpy(out_plan->hash, hash, 16);
        out_plan->key = key;
        out_plan->klen = klen;
        return 0;
    }
    return -1;
}

/* Mutating write at a slot found by a prior kf_plan_insert_slot /
   kf_plan_window_insert_slot call. Identical bookkeeping to the old
   kf_put_new's write branches (tombstone reclaim vs fresh-slot occupy). */
static void kf_commit_planned_slot(SlotcaskKfHandle *kh, const KfInsertPlan *plan,
                                    uint8_t stream_id, uint16_t file_id, uint32_t offset,
                                    size_t *used_delta, size_t *out_slot) {
    SlotcaskKfEntry *kf = kh->map;
    SlotcaskKfHeader *hdr = kh->hdr;
    SlotcaskKfEntry *t = &kf[plan->target_slot];
    memcpy(t->hash, plan->hash, 16);
    t->stream_id = stream_id;
    t->file_id = file_id;
    t->offset = offset;
    __atomic_store_n(&t->flag, 1, __ATOMIC_RELEASE);
    if (hdr) {
        if (plan->reused_tomb) hdr->deleted--;
        else                   hdr->total++;
    }
    (*used_delta)++;
    if (out_slot) *out_slot = plan->target_slot;
}

/* Insert a brand-new kf entry. Probes via linear hashing; on first-tombstone
   reuse, repurposes that slot. Before probing, checks the per-shard
   header.total — when it crosses 75 % of capacity, the shard doubles via
   kfcache_resplit_locked. Header reads/writes are plain mmap loads/stores
   under the kf wrlock — no atomics, no syscalls.

   Implemented as kf_plan_insert_slot (probe) + kf_commit_planned_slot
   (write) so callers that need the two phases split around an index
   prepare/apply boundary can call them separately; this composition keeps
   every other existing caller byte-for-byte unaffected. */
static int kf_put_new(SlotcaskDb *db, SlotcaskKfHandle *kh, const uint8_t hash[16],
                      uint8_t stream_id, uint16_t file_id, uint32_t offset,
                      const void *key, size_t klen, const char *data_dir,
                      size_t *used_delta, size_t *out_slot) {
    KfInsertPlan plan;
    int rc = kf_plan_insert_slot(db, kh, hash, key, klen, data_dir, &plan);
    if (rc != 0) return rc;
    kf_commit_planned_slot(kh, &plan, stream_id, file_id, offset, used_delta, out_slot);
    return 0;
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

/* ── Marker record readers (shared by forward and abort recovery) ── */

static int seg_write_flag_durable(const char *data_dir, uint8_t stream_id,
                                  uint16_t file_id, uint32_t offset,
                                  uint8_t flag);

static int kf_marker_replay_upsert_entry_locked(const char *eff_root,
        const char *object, const char *data_dir, int kf_shard,
        void *kh_opaque, const KfMarkerSlot *marker);

typedef struct {
    SlotcaskSegHandle handle;
    const uint8_t *key;
    const uint8_t *value;
    uint8_t hash[16];
    uint16_t klen;
    uint32_t vlen;
    int open;
} MarkerRecord;

static void marker_record_destroy(MarkerRecord *record) {
    if (record && record->open) segcache_release(&record->handle);
    if (record) memset(record, 0, sizeof(*record));
}

static int marker_record_read_live(const char *data_dir, uint8_t stream_id,
        uint16_t file_id, uint32_t offset, MarkerRecord *out) {
    char path[PATH_MAX];
    const uint8_t *record;

    if (!data_dir || !out) { errno = EINVAL; return -1; }
    memset(out, 0, sizeof(*out));
    seg_path_for(path, data_dir, stream_id, file_id);
    if (segcache_acquire(&out->handle, path, 0, 0, 0) != 0) return -1;
    out->open = 1;
    record = out->handle.map + offset;
    if (__atomic_load_n(&record[18], __ATOMIC_ACQUIRE) != 1) {
        errno = EILSEQ;
        marker_record_destroy(out);
        return -1;
    }
    memcpy(out->hash, record, sizeof(out->hash));
    out->klen = seg_rec_klen(record);
    out->vlen = seg_rec_vlen(record);
    out->key = record + 24;
    out->value = out->key + out->klen;
    return 0;
}

static int read_marker_old_live(const char *data_dir,
        const KfMarkerSlot *marker, MarkerRecord *out) {
    if (!marker || !marker->has_old) { errno = EILSEQ; return -1; }
    return marker_record_read_live(data_dir, marker->old_stream_id,
                                   marker->old_file_id, marker->old_offset,
                                   out);
}

static int read_marker_new_live(const char *data_dir,
        const KfMarkerSlot *marker, MarkerRecord *out) {
    if (!marker || marker->op == KF_MARKER_OP_DELETE) {
        errno = EILSEQ;
        return -1;
    }
    return marker_record_read_live(data_dir, marker->new_stream_id,
                                   marker->new_file_id, marker->new_offset,
                                   out);
}

/* Probe a segment record's flag byte without requiring it to be live.
   Returns 1 if tombstoned (flag==2), 0 if live (flag==1), -1 on I/O error
   or a flag value that means the record was never fully written. Used by
   the idempotent-redo paths: an abort redo or delete forward-replay redo
   can find the segment state already advanced by a prior partial run. */
static int marker_record_tombstoned(const char *data_dir, uint8_t stream_id,
                                    uint16_t file_id, uint32_t offset) {
    char path[PATH_MAX];
    SlotcaskSegHandle h;
    int rc;

    seg_path_for(path, data_dir, stream_id, file_id);
    if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;
    rc = (int)__atomic_load_n(&h.map[offset + 18], __ATOMIC_ACQUIRE);
    segcache_release(&h);
    return rc == 2 ? 1 : (rc == 1 ? 0 : -1);
}

/* Reconcile one index entry to match the durable record state. Pass
   exactly one of OLD/NEW for a forward insert (new only) or forward
   delete (old only); both for a forward update; both, swapped, for an
   abort of an update; new only for an abort of an insert. A NULL record
   is never passed to build_index_key_from_record_into. */
static int kf_marker_apply_recovery_diff(const char *eff_root,
        const char *object, int kf_shard, uint32_t kf_slot,
        const MarkerRecord *old_record, const MarkerRecord *new_record) {
    char err_buf[256] = {0};
    const MarkerRecord *identity = new_record ? new_record : old_record;

    if (!identity) { errno = EINVAL; return -1; }
    if (!g_recovery_index_diff_fn) return 0;
    return g_recovery_index_diff_fn(eff_root, object, kf_shard, kf_slot,
        identity->hash,
        old_record ? old_record->value : NULL,
        old_record ? old_record->vlen : 0,
        new_record ? new_record->value : NULL,
        new_record ? new_record->vlen : 0, err_buf, sizeof(err_buf));
}

/* The kf slot named by a delete marker must still hold the exact OLD
   record the marker describes. Anything else — slot reuse by another key,
   a different record at the slot, a tombstone — is corrupt evidence and
   fails closed (Gap B: only a live exact-match record passes here). */
static int kf_marker_verify_kf_old_at_slot(SlotcaskKfHandle *kh,
        size_t expected_slot, const MarkerRecord *old_record,
        const KfMarkerSlot *marker, const char *data_dir) {
    uint8_t flag, stream_id;
    uint16_t file_id;
    uint32_t offset;
    size_t found_slot;

    if (!kh || !old_record || !marker ||
        kf_lookup_with_slot(kh, old_record->hash, old_record->key,
                            old_record->klen, data_dir, &flag, &stream_id,
                            &file_id, &offset, &found_slot) != 0 ||
        found_slot != expected_slot || flag != 1 ||
        stream_id != marker->old_stream_id || file_id != marker->old_file_id ||
        offset != marker->old_offset) {
        errno = EILSEQ;
        return -1;
    }
    return 0;
}

static int seg_write_marker_new_tombstone_durable(const char *data_dir,
        const KfMarkerSlot *marker) {
    if (!marker || marker->op == KF_MARKER_OP_DELETE) {
        errno = EILSEQ;
        return -1;
    }
    return seg_write_flag_durable(data_dir, marker->new_stream_id,
                                  marker->new_file_id, marker->new_offset, 2);
}

/* Inverse index diff pinned by the abort sidecar: forward delete
   (old=NULL,new=OLD), update undo (new=NEW,old=OLD), insert undo
   (new=NEW,old=NULL). The direction comes from the sidecar, never from
   current index state. */
static int kf_marker_apply_abort_diff(const char *eff_root,
        const char *object, const char *data_dir, int kf_shard,
        uint32_t kf_slot, const KfMarkerSlot *marker) {
    MarkerRecord old_record = {0}, new_record = {0};
    int rc;

    if (!kf_marker_op_valid(marker)) { errno = EILSEQ; return -1; }
    if (marker->op == KF_MARKER_OP_DELETE) {
        rc = read_marker_old_live(data_dir, marker, &old_record) == 0
            ? kf_marker_apply_recovery_diff(eff_root, object, kf_shard,
                                            kf_slot, NULL, &old_record)
            : -1;
        marker_record_destroy(&old_record);
        return rc;
    }
    if (read_marker_new_live(data_dir, marker, &new_record) != 0) {
        /* Gap A (approved): an abort redo after cleanup step 1 already
           tombstoned the speculative NEW segment — the inverse diff was
           already applied in the prior partial run. Treat as applied and
           let the caller proceed to marker/sidecar unlink. Genuine I/O
           errors and never-written records still fail closed. */
        int tomb = marker_record_tombstoned(data_dir, marker->new_stream_id,
                                            marker->new_file_id,
                                            marker->new_offset);
        return tomb == 1 ? 0 : -1;
    }
    if (marker->has_old && read_marker_old_live(data_dir, marker,
                                                &old_record) != 0) {
        marker_record_destroy(&new_record);
        return -1;
    }
    rc = kf_marker_apply_recovery_diff(eff_root, object, kf_shard, kf_slot,
                                       &new_record,
                                       marker->has_old ? &old_record : NULL);
    marker_record_destroy(&old_record);
    marker_record_destroy(&new_record);
    return rc;
}

/* Forward delete replay: remove OLD from the indexes, verify then
   durably tombstone its kf slot, durably tombstone the OLD segment
   record, clear the marker. Every step is idempotent, so a crash
   mid-replay can restart safely (Gap B, approved: a prior partial run
   that already tombstoned the kf slot or the segment completes as 0). */
static int kf_marker_replay_delete_entry_locked(const char *eff_root,
        const char *object, const char *data_dir, int kf_shard,
        SlotcaskKfHandle *kh, const KfMarkerSlot *marker) {
    MarkerRecord old_rec = {0};
    int rc = -1;

    if (!kf_marker_op_valid(marker) || marker->op != KF_MARKER_OP_DELETE)
        goto out;
    if (read_marker_old_live(data_dir, marker, &old_rec) != 0) {
        /* Segment already tombstoned by a prior partial replay: complete
           only if the kf slot is tombstoned too — the forward delete then
           fully committed and only the marker clear remains. */
        int tomb = marker_record_tombstoned(data_dir, marker->old_stream_id,
                                            marker->old_file_id,
                                            marker->old_offset);
        if (tomb == 1 && marker->kf_slot < kh->capacity &&
            kh->map[marker->kf_slot].flag == 2)
            rc = 0;
        goto out;
    }
    if (kf_marker_apply_recovery_diff(eff_root, object, kf_shard,
                                      marker->kf_slot, &old_rec, NULL) != 0)
        goto out;
    if (kf_marker_verify_kf_old_at_slot(kh, marker->kf_slot, &old_rec,
                                        marker, data_dir) != 0) {
        /* Gap B (approved): verify failed because a prior partial replay
           already tombstoned the kf slot. Complete the pending segment
           tombstone, then clear. A different live record at the slot
           (slot reuse) or genuine I/O errors still fail closed. */
        if (marker->kf_slot < kh->capacity &&
            kh->map[marker->kf_slot].flag == 2) {
            if (seg_write_flag_durable(data_dir, marker->old_stream_id,
                                       marker->old_file_id,
                                       marker->old_offset, 2) != 0)
                goto out;
            rc = 0;
        }
        goto out;
    }
    size_t slot = marker->kf_slot;
    kf_tombstone_at_slot(kh, slot);
    if (kfcache_sync_slots_locked(kh, &slot, 1, 1) != 0 ||
        seg_write_flag_durable(data_dir, marker->old_stream_id,
                               marker->old_file_id, marker->old_offset, 2) != 0)
        goto out;
    rc = 0;
out:
    marker_record_destroy(&old_rec);
    return rc;
}

/* Reconcile one marker entry while the shard writer lock is held.  Clearing
   is deliberately owned by the caller: a batch marker contains many entries
   and must never remove the unrelated single-marker filename. */
static int kf_marker_replay_entry_locked(const char *eff_root,
        const char *object, const char *data_dir, int kf_shard,
        void *kh_opaque, const KfMarkerSlot *marker) {
    SlotcaskKfHandle *kh = (SlotcaskKfHandle *)kh_opaque;

    if (!kh || !kh->writer || !kf_marker_op_valid(marker)) {
        errno = EILSEQ;
        return -1;
    }
    if (marker->op == KF_MARKER_OP_DELETE)
        return kf_marker_replay_delete_entry_locked(eff_root, object,
                                                     data_dir, kf_shard, kh,
                                                     marker);
    return kf_marker_replay_upsert_entry_locked(eff_root, object, data_dir,
                                                 kf_shard, kh, marker);
}
static int kf_marker_replay_upsert_entry_locked(const char *eff_root, const char *object,
                                         const char *data_dir, int kf_shard,
                                         void *kh_opaque, const KfMarkerSlot *marker) {
    SlotcaskKfHandle *kh = (SlotcaskKfHandle *)kh_opaque;
    MarkerRecord new_rec = {0}, old_rec = {0};
    int rc = -1;

    if (!kh || !kh->writer || !marker || marker->op == KF_MARKER_OP_DELETE ||
        !data_dir) {
        errno = EILSEQ;
        return -1;
    }

    /* Step 1: read new record from segment (verifies the live flag). */
    if (read_marker_new_live(data_dir, marker, &new_rec) != 0) return -1;

    /* Step 2: if update, read old record. Keep the segcache handle open —
       old_value must stay valid through the index-diff call in steps 4-5
       below, which reads raw bytes directly out of the mmap'd segment. */
    if (marker->has_old && read_marker_old_live(data_dir, marker,
                                                &old_rec) != 0)
        goto out;

    /* Step 3: establish/sync kf mapping. resolved_kf_slot is the physical
       slot the record now lives at — for updates this is marker->kf_slot
       unchanged; for inserts it's only known after kf_put_new (or after
       the idempotent-replay lookup below finds it already present). Index
       reconciliation (steps 4-5) addresses records by physical slot, same
       as the live-write pre_commit path, so this must be the real slot. */
    int step3_rc = 0;
    size_t resolved_kf_slot = (size_t)marker->kf_slot;
    if (marker->has_old) {
        /* Update: repoint to new record. */
        size_t slot = (size_t)marker->kf_slot;
        kf_repoint_at_slot(kh, slot, marker->new_stream_id,
                          marker->new_file_id, marker->new_offset);
        size_t slots[] = { slot };
        if (kfcache_sync_slots_locked(kh, slots, 1, 0) != 0) step3_rc = -1;
    } else {
        /* Insert: place the new record into kf if it isn't already there
           (idempotent — a prior partial replay may have already inserted
           it before crashing again). Re-derive hash/key from the segment
           record itself rather than trusting a stale kf_slot hint, since
           a resplit between the original write and this replay can move
           slots. */
        uint16_t klen2 = new_rec.klen;
        const uint8_t *key2 = new_rec.key;

        uint8_t flag_out; uint8_t stream_out; uint16_t file_out;
        uint32_t off_out; size_t existing_slot;
        int found = (kf_lookup_with_slot(kh, new_rec.hash, key2, klen2, data_dir,
                                         &flag_out, &stream_out, &file_out,
                                         &off_out, &existing_slot) == 0);

        if (!found) {
            size_t used_delta = 0, out_slot = 0;
            if (marker->kf_slot != UINT32_MAX) {
                /* Bulk markers persist the exact slot selected before
                   index apply.  Do not re-probe here: a recovery-time
                   resplit could otherwise move the kf record while its
                   already-durable bitmap bit still names the old slot. */
                if (marker->kf_slot >= kh->capacity ||
                    kh->map[marker->kf_slot].flag == 1) {
                    step3_rc = -1;
                } else {
                    KfInsertPlan replay_plan = {
                        .target_slot = marker->kf_slot,
                        .reused_tomb = kh->map[marker->kf_slot].flag == 2,
                        .key = key2,
                        .klen = klen2,
                    };
                    memcpy(replay_plan.hash, new_rec.hash,
                           sizeof(replay_plan.hash));
                    kf_commit_planned_slot(kh, &replay_plan,
                                           marker->new_stream_id,
                                           marker->new_file_id,
                                           marker->new_offset,
                                           &used_delta, &out_slot);
                    size_t slots[] = { out_slot };
                    if (kfcache_sync_slots_locked(kh, slots, 1, 1) != 0)
                        step3_rc = -1;
                    resolved_kf_slot = out_slot;
                }
            } else {
                char keybuf[1024];
                if (klen2 > sizeof(keybuf)) {
                    step3_rc = -1;
                } else {
                    memcpy(keybuf, key2, klen2);
                    if (kf_put_new(NULL, kh, new_rec.hash, marker->new_stream_id,
                                   marker->new_file_id, marker->new_offset,
                                   keybuf, klen2, data_dir, &used_delta, &out_slot) != 0) {
                        step3_rc = -1;
                    } else {
                        size_t slots[] = { out_slot };
                        if (kfcache_sync_slots_locked(kh, slots, 1, 1) != 0) step3_rc = -1;
                        resolved_kf_slot = out_slot;
                    }
                }
            }
        } else {
            resolved_kf_slot = existing_slot;
        }
    }

    if (step3_rc != 0) goto out;

    /* Steps 4-5: reconcile index state via the registered recovery
       callback. slotcask.c stays decoupled from schema/index logic (owned
       by storage.c) — see g_recovery_index_diff_fn in shard_db_internal.h.
       NULL registration is a no-op, for kf-layer-only test builds. Old/new
       records are explicit and nullable, so the recovery callback never
       receives a NULL value buffer. */
    if (kf_marker_apply_recovery_diff(eff_root, object, kf_shard,
                                      (uint32_t)resolved_kf_slot,
                                      marker->has_old ? &old_rec : NULL,
                                      &new_rec) != 0)
        goto out;
    rc = 0;
out:
    marker_record_destroy(&old_rec);
    marker_record_destroy(&new_rec);
    return rc;
}

/* Single-marker compatibility wrapper.  Batch callers use the entry helper
   and clear only their own batch file after every entry has replayed. */
int kf_marker_replay_locked(const char *eff_root, const char *object,
                            const char *data_dir, int kf_shard,
                            void *kh_opaque, const KfMarkerSlot *marker) {
    if (kf_marker_replay_entry_locked(eff_root, object, data_dir, kf_shard,
                                      kh_opaque, marker) != 0)
        return -1;
    return kf_marker_clear(data_dir, kf_shard);
}

/* Clean shutdown flag — written at graceful stop, unlinked at startup.
   Presence means previous shutdown was clean; absence means unclean (needs recovery). */
int clean_flag_write(const char *data_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/.shard-db.clean", data_dir);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    int sync_rc0 = fsync(fd);
    int close_rc0 = close(fd);
    if (sync_rc0 != 0 || close_rc0 != 0) {
        (void)unlink(path);
        (void)fsync_dir(data_dir);
        return -1;
    }
    fd = open(data_dir, O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        (void)unlink(path);
        (void)fsync_dir(data_dir);
        return -1;
    }
    int sync_rc = fsync(fd);
    int close_rc = close(fd);
    if (sync_rc != 0 || close_rc != 0) {
        (void)unlink(path);
        (void)fsync_dir(data_dir);
        return -1;
    }
    return 0;
}

int clean_flag_exists(const char *data_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/.shard-db.clean", data_dir);
    return (access(path, F_OK) == 0) ? 1 : 0;
}

int clean_flag_remove(const char *data_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/.shard-db.clean", data_dir);
    if (unlink(path) != 0 && errno != ENOENT) return -1;
    /* Fsync dir after unlink, including the no-op ENOENT case. */
    int fd = open(data_dir, O_RDONLY | O_DIRECTORY);
    if (fd < 0) return -1;
    int sync_rc = fsync(fd);
    int close_rc = close(fd);
    return sync_rc == 0 && close_rc == 0 ? 0 : -1;
}

/* Bulk marker I/O: one file contains one KfMarkerSlot per record. */

static void kf_batch_marker_path(char *buf, size_t cap, const char *data_dir,
                                  int kf_shard, uint32_t batch_id) {
    snprintf(buf, cap, "%s/data/kf/%03x_batch_%u_marker.dat",
             data_dir, (unsigned)kf_shard, batch_id);
}

/* Write array of markers for a bulk operation (one per record in batch).
   Returns 0 on success, -1 on failure. */
static int kf_batch_marker_write_impl(const char *data_dir, int kf_shard, uint32_t batch_id,
                          const KfMarkerSlot *markers, size_t count) {
    if (!markers || count == 0) { errno = EINVAL; return -1; }

    char path[PATH_MAX];
    kf_batch_marker_path(path, sizeof(path), data_dir, kf_shard, batch_id);

    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;

    /* Write all markers with checksum */
    for (size_t i = 0; i < count; i++) {
        KfMarkerSlot durable = markers[i];
        durable.checksum = XXH32(&durable, offsetof(KfMarkerSlot, checksum), 0);
        ssize_t n = pwrite(fd, &durable, sizeof(durable), (off_t)(i * sizeof(durable)));
        if (n != (ssize_t)sizeof(durable)) { close(fd); return -1; }
    }

    if (fsync(fd) != 0) { close(fd); return -1; }
    close(fd);

    /* Fsync directory. */
    int dfd = open(data_dir, O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) { fsync(dfd); close(dfd); }

    return 0;
}

int kf_batch_marker_write(const char *data_dir, int kf_shard, uint32_t batch_id,
                          const KfMarkerSlot *markers, size_t count) {
    uint64_t t0 = now_us();
    int rc = kf_batch_marker_write_impl(data_dir, kf_shard, batch_id, markers, count);
    commit_sync_us_record(t0);
    return rc;
}

/* Clear batch marker file after recovery/completion. */
static int kf_batch_marker_clear_impl(const char *data_dir, int kf_shard, uint32_t batch_id) {
    char path[PATH_MAX], dpath[PATH_MAX];
    kf_batch_marker_path(path, sizeof(path), data_dir, kf_shard, batch_id);
    if (unlink(path) != 0 && errno != ENOENT) return -1;
    /* The marker lives in data/kf, so the parent directory that must be
       synced is data/kf itself.  Treat a failed directory sync as a failed
       clear: otherwise the caller could reuse this batch id before the
       unlink is durable. */
    kf_marker_dir_path(dpath, sizeof(dpath), data_dir);
    return fsync_dir(dpath);
}

int kf_batch_marker_clear(const char *data_dir, int kf_shard, uint32_t batch_id) {
    uint64_t t0 = now_us();
    int rc = kf_batch_marker_clear_impl(data_dir, kf_shard, batch_id);
    commit_sync_us_record(t0);
    return rc;
}

/* Bulk counterpart to kf_marker_gate().  A retained batch marker belongs to
   this kf shard, so replay it while the shard writer lock is held before a
   new bulk window can reuse its batch-id/path.  Without this gate a later
   call starts again at batch 0 and O_TRUNC can discard the recovery intent
   left by a failed apply or kf sync. */
static int batch_marker_id_cmp(const void *a, const void *b) {
    const uint32_t aa = *(const uint32_t *)a;
    const uint32_t bb = *(const uint32_t *)b;
    return (aa > bb) - (aa < bb);
}

/* Read a batch-marker file exactly: size must be a positive multiple of
   sizeof(KfMarkerSlot), every entry must carry KF_MARKER_MAGIC, a valid
   checksum, and a valid op/reserved layout, and a final pread must confirm
   EOF. Returns 0 on success, 1 when absent, -1 (errno; EILSEQ for corrupt
   evidence) otherwise. Never used as "read until EOF": a partial entry or an
   extra trailing byte is corrupt evidence, not a shorter valid file. */
static int kf_batch_marker_read_exact(const char *path, KfMarkerSlot **out,
                                      size_t *out_count) {
    struct stat st;
    KfMarkerSlot *markers = NULL;
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return errno == ENOENT ? 1 : -1;
    if (fstat(fd, &st) != 0 || st.st_size <= 0 ||
        (size_t)st.st_size % sizeof(KfMarkerSlot) != 0) {
        int saved = errno ? errno : EILSEQ; close(fd); errno = saved; return -1;
    }
    size_t count = (size_t)st.st_size / sizeof(KfMarkerSlot);
    markers = calloc(count, sizeof(*markers));
    if (!markers) { close(fd); return -1; }
    ssize_t n = pread(fd, markers, (size_t)st.st_size, 0);
    if (n != (ssize_t)st.st_size) {
        int saved = errno ? errno : EILSEQ;
        free(markers); close(fd); errno = saved;
        return -1;
    }
    for (size_t i = 0; i < count; i++) {
        if (markers[i].magic != KF_MARKER_MAGIC ||
            markers[i].checksum !=
                XXH32(&markers[i], offsetof(KfMarkerSlot, checksum), 0) ||
            !kf_marker_op_valid(&markers[i])) {
            errno = EILSEQ;
            free(markers); close(fd);
            return -1;
        }
    }
    char probe;
    if (pread(fd, &probe, 1, (off_t)st.st_size) != 0) {
        errno = EILSEQ;
        free(markers); close(fd);
        return -1;
    }
    close(fd);
    *out = markers;
    *out_count = count;
    return 0;
}

/* Apply a pinned batch abort while the kf writer lock is held, in the
   binding cleanup order: every inverse index diff (+ speculative NEW segment
   tombstone for upserts), then unlink the marker and fsync, then unlink the
   sidecar and fsync. On any failure leave both files and fail closed. */
static int kf_batch_marker_abort_locked(const char *eff_root,
        const char *object, const char *data_dir, int kf_shard,
        SlotcaskKfHandle *kh, const KfMarkerSlot *markers, size_t count,
        const char *marker_path, const char *abort_path) {
    char kf_dir[PATH_MAX];
    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", data_dir);
    for (size_t i = 0; i < count; i++) {
        const KfMarkerSlot *marker = &markers[i];
        if (!kf_marker_op_valid(marker)) { errno = EILSEQ; goto failed; }
        if (marker->op == KF_MARKER_OP_DELETE) {
            if (kf_marker_apply_abort_diff(eff_root, object, data_dir,
                    kf_shard, marker->kf_slot, marker) != 0)
                goto failed;
            continue; /* OLD remains live and Kf was never tombstoned. */
        }
        if (kf_marker_apply_abort_diff(eff_root, object, data_dir,
                kf_shard, marker->kf_slot, marker) != 0 ||
            seg_write_marker_new_tombstone_durable(data_dir, marker) != 0)
            goto failed;
    }
    if (unlink(marker_path) != 0 || fsync_dir(kf_dir) != 0) goto failed;
    if (kf_abort_clear_after_marker(abort_path, kf_dir) != 0) goto failed;
    return 0;
failed:
    kf_marker_fail_closed(data_dir, kf_shard, "abort recovery");
    return -1;
}

/* Batch equivalent of kf_marker_replay_current, used only after a
   post-publication Kf sync failure: re-run the forward diff for every entry
   and clear the marker so the committed state and marker-cleanup state both
   converge synchronously. */
static int kf_batch_marker_replay_current_locked(const char *data_dir,
        int kf_shard, SlotcaskKfHandle *kh, const KfMarkerSlot *markers,
        size_t count, const char *marker_path) {
    char eff_root[PATH_MAX], object[256], kf_dir[PATH_MAX];

    if (!data_dir || !kh || !kh->writer || !markers || count == 0 ||
        !marker_path) {
        errno = EINVAL;
        return -1;
    }
    split_data_dir(data_dir, eff_root, sizeof(eff_root), object,
                   sizeof(object));
    for (size_t i = 0; i < count; i++) {
        if (kf_marker_replay_entry_locked(eff_root, object, data_dir,
                                          kf_shard, kh, &markers[i]) != 0)
            return -1;
    }
    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", data_dir);
    if (unlink(marker_path) != 0 || fsync_dir(kf_dir) != 0)
        return -1;
    return 0;
}

/* Bulk counterpart to kf_marker_gate().  A retained batch marker belongs to
   this kf shard, so replay or abort it while the shard writer lock is held
   before a new bulk window can reuse its batch-id/path.  Without this gate a
   later call starts again at batch 0 and O_TRUNC can discard the recovery
   intent left by a failed apply or kf sync. */
static int kf_batch_marker_gate(int kf_shard, SlotcaskKfHandle *kh,
                                const char *data_dir) {
    char kf_dir[PATH_MAX];
    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", data_dir);
    DIR *d = opendir(kf_dir);
    if (!d) return errno == ENOENT ? 0 : -1;

    char eff_root[PATH_MAX], object[256];
    split_data_dir(data_dir, eff_root, sizeof(eff_root), object, sizeof(object));
    uint32_t *ids = NULL;
    size_t nids = 0, cap_ids = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        int marker_shard = -1, consumed = 0;
        unsigned batch_id = 0;
        int is_marker = sscanf(e->d_name, "%x_batch_%u_marker.dat%n",
                               &marker_shard, &batch_id, &consumed) == 2 &&
                        consumed == (int)strlen(e->d_name);
        consumed = 0;
        int is_abort = sscanf(e->d_name, "%x_batch_%u_abort.dat%n",
                              &marker_shard, &batch_id, &consumed) == 2 &&
                       consumed == (int)strlen(e->d_name);
        if ((!is_marker && !is_abort) || marker_shard != kf_shard)
            continue;
        int duplicate = 0;
        for (size_t i = 0; i < nids; i++) {
            if (ids[i] == (uint32_t)batch_id) {
                duplicate = 1;
                break;
            }
        }
        if (duplicate) continue;
        if (nids == cap_ids) {
            size_t next = cap_ids ? cap_ids * 2 : 4;
            uint32_t *grown = realloc(ids, next * sizeof(*ids));
            if (!grown) { free(ids); closedir(d); return -1; }
            ids = grown;
            cap_ids = next;
        }
        ids[nids++] = (uint32_t)batch_id;
    }
    closedir(d);
    if (nids) qsort(ids, nids, sizeof(*ids), batch_marker_id_cmp);

    int rc = 0;
    for (size_t i = 0; i < nids && rc == 0; i++) {
        char path[PATH_MAX], abort_path[PATH_MAX];
        KfMarkerSlot *markers = NULL;
        size_t count = 0;
        kf_batch_marker_path(path, sizeof(path), data_dir, kf_shard, ids[i]);
        struct stat marker_st;
        int marker_present = stat(path, &marker_st) == 0;
        int mrc = kf_batch_marker_read_exact(path, &markers, &count);

        kf_abort_path(abort_path, sizeof(abort_path), data_dir,
                      KF_ABORT_BATCH, kf_shard, ids[i]);
        KfAbortHeader hdr;
        int arc;
        if (mrc == 0) {
            arc = kf_abort_read_exact(abort_path, KF_ABORT_BATCH,
                                      (uint32_t)kf_shard, ids[i],
                                      (uint32_t)count, &hdr);
        } else {
            /* A sidecar without its marker is only the cleanup window after
               the marker unlink was durable. Revalidate its self-consistent
               identity before clearing it; never let a stale/corrupt sidecar
               be hidden by a new batch reusing this id. */
            arc = kf_abort_read_header(abort_path, &hdr);
            if (arc == 0 &&
                (hdr.kind != KF_ABORT_BATCH ||
                 hdr.kf_shard != (uint32_t)kf_shard ||
                 hdr.batch_id != ids[i] || hdr.marker_count == 0)) {
                errno = EILSEQ;
                arc = -1;
            }
        }
        if (!marker_present && mrc != 1) {
            /* kf_batch_marker_read_exact only ever returns 0/1/-1; reaching
               here (mrc != 1) means mrc is 0 or -1. The marker may have
               vanished between stat and read only when another gate
               completed it (mrc == 1, excluded above) — anything else,
               including a read that raced a concurrent write (mrc == 0) or
               a genuinely corrupt marker (mrc == -1), is evidence, not an
               orphan-cleanup success. */
            free(markers);
            rc = -1;
            break;
        }
        if (marker_present && mrc != 0) {
            free(markers);
            rc = -1;
            break;
        }
        if (!marker_present && arc == 0) {
            if (kf_abort_clear_after_marker(abort_path, kf_dir) != 0)
                rc = -1;
            free(markers);
            continue;
        }
        if (!marker_present && arc == 1) {
            free(markers);
            continue;
        }
        if (!marker_present) {
            free(markers);
            if (errno != EILSEQ) {
                LOG_WARN(LOG_SUB_SLOTCASK,
                         "abort-sidecar: transient I/O reading %s errno=%d (%s); "
                         "failing this write, not terminating", abort_path,
                         errno, strerror(errno));
                rc = -1;
            } else {
                kf_marker_fail_closed(data_dir, kf_shard,
                                      "corrupt orphan batch abort sidecar");
            }
            break;
        }
        if (arc == 0) {
            if (kf_batch_marker_abort_locked(eff_root, object, data_dir,
                                             kf_shard, kh, markers, count,
                                             path, abort_path) != 0)
                rc = -1;
        } else if (arc == 1) {
            /* No sidecar — forward replay to completion. */
            for (size_t j = 0; j < count && rc == 0; j++) {
                if (kf_marker_replay_entry_locked(eff_root, object, data_dir,
                                                  kf_shard, kh,
                                                  &markers[j]) != 0)
                    rc = -1;
            }
            if (rc == 0 && kf_batch_marker_clear(data_dir, kf_shard,
                                                 ids[i]) != 0)
                rc = -1;
        } else if (errno != EILSEQ) {
            LOG_WARN(LOG_SUB_SLOTCASK,
                    "abort-sidecar: transient I/O reading %s errno=%d (%s); "
                    "failing this write, not terminating", abort_path,
                    errno, strerror(errno));
            rc = -1;
        } else {
            kf_marker_fail_closed(data_dir, kf_shard,
                                  "corrupt batch abort sidecar");
            rc = -1;
        }
        free(markers);
    }
    free(ids);
    return rc;
}

/* Every indexed writer must recover both marker formats before it plans a
   slot or opens a bitmap writer handle.  Treating the formats as independent
   let a batch replay clear an unrelated single marker, and let INSERT plan
   against a keyfile that replay was about to change. */
static int kf_shard_marker_gate(int kf_shard, SlotcaskKfHandle *kh,
                                const char *data_dir) {
    if (kf_marker_gate(kf_shard, kh, data_dir) != 0) return -1;
    return kf_batch_marker_gate(kf_shard, kh, data_dir);
}

/* Marker recovery sweep: scan one object's data/kf/ for leftover marker
   files (single-record or batch) and replay each to completion.
   Called at startup when unclean shutdown was detected, before the
   server accepts connections. Caller must already hold the object's
   write lock (objlock_wrlock) for the duration.
   Returns 0 if every marker found was replayed/cleared, -1 if any marker
   is corrupt or fails to replay (fail-closed — operator must investigate;
   the marker is left on disk so nothing is silently lost).
   If out_replayed is non-NULL, it is incremented once per marker file this
   call actually found (regardless of replay outcome) — callers use this to
   report whether recovery replayed anything, as opposed to merely running. */
int marker_recovery_sweep_object(const char *eff_root, const char *data_dir, const char *object_name,
                                  int *out_replayed) {
    char kf_dir[PATH_MAX];
    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", data_dir);

    DIR *d = opendir(kf_dir);
    if (!d) return (errno == ENOENT) ? 0 : -1;

    int rc = 0;
    int *batch_shards = NULL;
    size_t n_batch_shards = 0, cap_batch_shards = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        int kf_shard = -1;
        unsigned batch_id = 0;
        int is_batch = 0;
        int is_abort = 0;
        int name_len = (int)strlen(e->d_name);
        int consumed = 0;

        if (sscanf(e->d_name, "%x_batch_%u_marker.dat%n", &kf_shard, &batch_id, &consumed) == 2 &&
            consumed == name_len) {
            is_batch = 1;
        } else if ((consumed = 0, sscanf(e->d_name, "%x_marker.dat%n", &kf_shard, &consumed) == 1) &&
                   consumed == name_len) {
            /* single-record marker match */
        } else if ((consumed = 0, sscanf(e->d_name, "%x_batch_%u_abort.dat%n", &kf_shard, &batch_id, &consumed) == 2) &&
                   consumed == name_len) {
            is_abort = 1;
            is_batch = 1;
        } else if ((consumed = 0, sscanf(e->d_name, "%x_marker_abort.dat%n", &kf_shard, &consumed) == 1) &&
                   consumed == name_len) {
            is_abort = 1;
        } else {
            continue;
        }
        if (kf_shard < 0) continue;

        if (is_abort) {
            /* Sidecar entry. Paired sidecars are consumed by the marker
               branches / batch gate below; a sidecar whose marker file is
               fully gone is completed cleanup (binding order: marker
               unlinked before sidecar) and may be revalidated and cleared
               here. Any other combination — marker still present, or the
               sidecar itself corrupt — fails closed. */
            char marker_path[PATH_MAX], abort_path[PATH_MAX];
            if (is_batch)
                kf_batch_marker_path(marker_path, sizeof(marker_path),
                                     data_dir, kf_shard, batch_id);
            else
                kf_marker_path(marker_path, sizeof(marker_path), data_dir,
                               kf_shard);
            kf_abort_path(abort_path, sizeof(abort_path), data_dir,
                          is_batch ? KF_ABORT_BATCH : KF_ABORT_SINGLE,
                          kf_shard, batch_id);
            struct stat st;
            if (stat(marker_path, &st) == 0) continue; /* paired — handled below */
            if (errno != ENOENT) { rc = -1; continue; }
            KfAbortHeader hdr;
            int hrc = kf_abort_read_header(abort_path, &hdr);
            if (hrc == 0) {
                int valid = hdr.kind == (is_batch ? KF_ABORT_BATCH : KF_ABORT_SINGLE) &&
                            hdr.kf_shard == (uint32_t)kf_shard &&
                            hdr.batch_id == (is_batch ? batch_id : 0) &&
                            hdr.marker_count > 0 &&
                            (is_batch || hdr.marker_count == 1);
                if (!valid) {
                    errno = EILSEQ;
                    rc = -1;
                } else if (kf_abort_clear_after_marker(abort_path, kf_dir) != 0) {
                    rc = -1;
                }
            } else if (hrc == 1) {
                /* Sidecar vanished between readdir() and here — cleared by
                   a concurrent gate/sweep already (same benign race the
                   marker branch below tolerates for rrc==1). Nothing left
                   to do for this dirent. */
            } else {
                rc = -1; /* corrupt orphan sidecar — fail closed, leave it */
            }
            continue;
        }

        if (out_replayed) (*out_replayed)++;

        if (is_batch) {
            size_t i;
            for (i = 0; i < n_batch_shards; i++)
                if (batch_shards[i] == kf_shard) break;
            if (i == n_batch_shards) {
                if (n_batch_shards == cap_batch_shards) {
                    size_t next = cap_batch_shards ? cap_batch_shards * 2 : 4;
                    int *grown = realloc(batch_shards, next * sizeof(*batch_shards));
                    if (!grown) { rc = -1; break; }
                    batch_shards = grown;
                    cap_batch_shards = next;
                }
                batch_shards[n_batch_shards++] = kf_shard;
            }
            continue;
        }

        char kf_path[PATH_MAX];
        kf_path_for(kf_path, data_dir, kf_shard);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, 0, 1) != 0) { rc = -1; continue; }

        KfMarkerSlot marker;
        int rrc = kf_marker_read(data_dir, kf_shard, &marker);
        char abort_path[PATH_MAX];
        KfAbortHeader hdr;
        kf_abort_path(abort_path, sizeof(abort_path), data_dir,
                      KF_ABORT_SINGLE, kf_shard, 0);
        int arc = kf_abort_read_exact(abort_path, KF_ABORT_SINGLE,
                                      (uint32_t)kf_shard, 0, 1, &hdr);
        if (rrc == 0 && arc == 0) {
            /* Valid marker + valid sidecar: the abort is pinned — redo it. */
            char marker_path[PATH_MAX];
            kf_marker_path(marker_path, sizeof(marker_path), data_dir,
                           kf_shard);
            if (kf_marker_abort_single_locked(eff_root, object_name,
                                              data_dir, kf_shard, &kh, &hdr,
                                              marker_path, abort_path) != 0)
                rc = -1;
        } else if (rrc == 0) {
            if (arc == -1) {
                rc = -1; /* corrupt sidecar — fail closed, leave both */
            } else if (kf_marker_replay_locked(eff_root, object_name,
                                               data_dir, kf_shard, &kh,
                                               &marker) != 0) {
                rc = -1;
            }
        } else if (rrc == 1) {
            /* Marker vanished (replayed by a concurrent gate already);
               an orphan sidecar is cleared by the abort-file branch. */
        } else if (rrc == 2) {
            if (arc == 0) {
                rc = -1; /* torn marker beside a sidecar — corrupt evidence */
            } else if (arc == 1) {
                if (kf_marker_clear(data_dir, kf_shard) != 0) rc = -1;
            } else {
                rc = -1;
            }
        } else {
            rc = -1; /* corrupt — fail closed, leave marker in place */
        }

        kfcache_release(&kh);
    }
    closedir(d);
    for (size_t i = 0; i < n_batch_shards; i++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, data_dir, batch_shards[i]);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, 0, 1) != 0) {
            rc = -1;
            continue;
        }
        if (kf_batch_marker_gate(batch_shards[i], &kh, data_dir) != 0)
            rc = -1;
        kfcache_release(&kh);
    }
    free(batch_shards);
    return rc;
}

/* Non-replaying marker check for graceful shutdown. Deliberately does not read/verify marker
   contents — a corrupt marker still means "not safe to skip recovery next
   time," so mere presence of a matching filename is enough to answer 1. */
int object_has_pending_markers(const char *data_dir) {
    char kf_dir[PATH_MAX];
    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", data_dir);

    DIR *d = opendir(kf_dir);
    if (!d) return (errno == ENOENT) ? 0 : -1;

    int found = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        int kf_shard = -1;
        unsigned batch_id = 0;
        int name_len = (int)strlen(e->d_name);
        int consumed = 0;
        int is_batch_match = sscanf(e->d_name, "%x_batch_%u_marker.dat%n", &kf_shard, &batch_id, &consumed) == 2 &&
                              consumed == name_len;
        consumed = 0;
        int is_single_match = !is_batch_match &&
                               sscanf(e->d_name, "%x_marker.dat%n", &kf_shard, &consumed) == 1 &&
                               consumed == name_len;
        consumed = 0;
        int is_abort_batch_match = !is_batch_match && !is_single_match &&
            sscanf(e->d_name, "%x_batch_%u_abort.dat%n", &kf_shard, &batch_id, &consumed) == 2 &&
            consumed == name_len;
        consumed = 0;
        int is_abort_single_match = !is_batch_match && !is_single_match &&
            !is_abort_batch_match &&
            sscanf(e->d_name, "%x_marker_abort.dat%n", &kf_shard, &consumed) == 1 &&
            consumed == name_len;
        if (is_batch_match || is_single_match || is_abort_batch_match ||
            is_abort_single_match) {
            found = 1;
            break;
        }
    }
    closedir(d);
    return found;
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

/* Conservative fallback for rollback paths that do not retain the encoded
   record length. A max-capacity bucket remains safe; normal tombstone and
   recovery paths preserve the exact record capacity. */
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
        /* Bucket membership is a coarse capacity range (see
           slotcask_bucket_for), not a single fixed size, now that every
           object uses variable-length records — a bucket can hold entries
           smaller than needed_size (e.g. bucket 0 spans [0,256)). Scan for
           one that actually fits rather than trusting the top of the
           stack; swap-remove keeps this O(1) once found. */
        for (size_t i = p->free_count[b]; i > 0; i--) {
            SlotcaskFreeSlot *cand = &p->free_slots[b][i - 1];
            if (cand->capacity >= needed_size) {
                *out = *cand;
                p->free_slots[b][i - 1] = p->free_slots[b][p->free_count[b] - 1];
                p->free_count[b]--;
                pthread_mutex_unlock(&p->pool_lock);
                return 0;
            }
        }
    }
    pthread_mutex_unlock(&p->pool_lock);
    return 2;
}

/* A free-pool slot popped by pool_try_pop_for_size() may be larger than
   the record about to be written into it (coarse bucket matching, see the
   comment above) — never let the excess be silently folded into that
   record's zero-padding, or every reader that recomputes stride from the
   record's own header (24+klen+vlen) will under-advance and misalign
   against genuinely live data past it. Zero the excess in place and
   return it to the pool as its own independent, correctly-capacitied
   entry so the invariant "on-disk footprint == header-computed size"
   holds for every record unconditionally. */
static int pool_split_leftover(SlotcaskDb *db, uint8_t stream_id,
                                uint16_t file_id, uint32_t offset,
                                uint32_t len) {
    if (len == 0) return 0;
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0, 1) != 0) return -1;
    memset(h.map + offset, 0, len);
    if (h.slot >= 0) {
        SegCacheEntry *e = &g_segcache[h.slot];
        durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
    }
    segcache_release(&h);
    return pool_push_free_cap(&db->streams[stream_id], file_id, offset, len,
                              db->slot_size);
}

/* ============================================================ Append path */

/* Reserve a single variable-length slot. rec_size includes the header
   + key + value + alignment padding. Rotates if not enough space in the
   active segment. Returns 0 on success, -1 on error. */
static int append_reserve_single_varlen(SlotcaskDb *db, SlotcaskStream *p,
                                         size_t rec_size,
                                         uint32_t *file_id_out,
                                         uint32_t *offset_out) {
    (void)db;
    size_t seg_max = slotcask_seg_max_bytes();
    if (!file_id_out || !offset_out || rec_size > seg_max)
        return -1;
    pthread_mutex_lock(&p->rotation_lock);
    if (p->reserve_off > seg_max - rec_size) {
        if (p->active_file_id >= UINT16_MAX) {
            pthread_mutex_unlock(&p->rotation_lock);
            errno = EFBIG;
            return -1;
        }
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
/* Variable-length variant: writes a record without padding to slot_size.
   rec_size must be slotcask_record_size_varlen(klen, vlen). */
static int seg_write_record_varlen(const SlotcaskDb *db, uint8_t stream_id,
                                    uint16_t file_id, uint32_t offset,
                                    const uint8_t hash[16],
                                    const void *key, size_t klen,
                                    const void *value, size_t vlen,
                                    size_t rec_size, int sync_now) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 1, 0, 1) != 0) return -1;
    seg_record_emit(h.map + offset, (int)rec_size, hash, key, klen, value, vlen);
    if (h.slot >= 0) {
        SegCacheEntry *e = &g_segcache[h.slot];
        durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
    }
    int rc = 0;
    if (sync_now && durability_msync_range(h.map, offset, rec_size) != 0)
        rc = -1;
    segcache_release(&h);
    return rc;
}

/* Tombstone an old seg slot and return it to its stream pool.
   Reads the record's klen/vlen from the segment header to determine the
   slot's capacity. Must be called with a non-const db because it modifies
   the stream's free pool. */
static inline int slotcask_tombstone_and_push_back(SlotcaskDb *db,
                                                    uint8_t stream_id,
                                                    uint16_t file_id,
                                                    uint32_t offset) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0, 1) != 0) return -1;
    __atomic_store_n(&h.map[offset + 18], 2, __ATOMIC_RELEASE);
    if (h.slot >= 0) {
        SegCacheEntry *e = &g_segcache[h.slot];
        durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
    }
    uint16_t klen = seg_rec_klen(h.map + offset);
    uint32_t vlen = seg_rec_vlen(h.map + offset);
    uint32_t cap = (uint32_t)slotcask_record_size_varlen(klen, vlen);
    segcache_release(&h);
    pool_push_free_cap(&db->streams[stream_id], file_id, offset,
                       cap, db->slot_size);
    return 0;
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
    if (segcache_acquire(&h, path, 0, 0, 1) != 0) return -1;
    /* Release-store: tombstones flip flag 1→2 (deleted). Concurrent
       readers doing acquire-load on the flag byte either still see 1
       (and proceed with the live record) or see 2 (and skip). */
    __atomic_store_n(&h.map[offset + 18], flag, __ATOMIC_RELEASE);
    if (h.slot >= 0) {
        SegCacheEntry *e = &g_segcache[h.slot];
        durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
    }
    segcache_release(&h);
    return 0;
}

/* seg_write_flag, then make the byte durable: msync the touched page and
   fdatasync the segment. Used by recovery paths whose outcome is judged
   after a restart, where a non-durable tombstone is indistinguishable
   from no tombstone at all. */
static int seg_write_flag_durable(const char *data_dir, uint8_t stream_id,
                                  uint16_t file_id, uint32_t offset,
                                  uint8_t flag) {
    char path[PATH_MAX];
    SlotcaskSegHandle h;
    int rc = -1;

    seg_path_for(path, data_dir, stream_id, file_id);
    if (segcache_acquire(&h, path, 0, 0, 1) != 0) return -1;
    __atomic_store_n(&h.map[offset + 18], flag, __ATOMIC_RELEASE);
    if (h.slot >= 0) {
        SegCacheEntry *entry = &g_segcache[h.slot];
        durability_mark_dirty(&entry->dirty, &entry->dirty_since_ms);
    }
    if (durability_msync_range(h.map, offset + 18, 1) == 0 &&
        fdatasync(h.fd) == 0)
        rc = 0;
    segcache_release(&h);
    return rc;
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
    size_t rec_size = slotcask_record_size_varlen(klen, vlen);
    int got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                           db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = (uint32_t)rec_size;
        if (fs.capacity > slot_capacity)
            pool_split_leftover(db, target_stream, target_fid,
                                target_off + slot_capacity,
                                fs.capacity - slot_capacity);
    } else {
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0)
            return -1;
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }

    if (seg_write_record_varlen(db, target_stream, target_fid, target_off,
                                 hash, key, klen, value, vlen,
                                 slot_capacity, 1) != 0) {
        if (got_pool) pool_push_free_cap(pool, target_fid, target_off,
                                          slot_capacity, db->slot_size);
        return -1;
    }

    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) {
        seg_write_flag(db, target_stream, target_fid, target_off, 2);
        pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
        return -1;
    }
    size_t used_delta = 0;
    int put_rc = kf_put_new(db, &kh, hash, target_stream, target_fid, target_off,
                            key, klen, db->data_dir, &used_delta, NULL);
    kfcache_release(&kh);
    if (put_rc != 0) {
        seg_write_flag(db, target_stream, target_fid, target_off, 2);
        pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
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
    size_t rec_size = slotcask_record_size_varlen(klen, vlen);
    int got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                           db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = (uint32_t)rec_size;
        if (fs.capacity > slot_capacity)
            pool_split_leftover(db, target_stream, target_fid,
                                target_off + slot_capacity,
                                fs.capacity - slot_capacity);
    } else {
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0) {
            kfcache_release(&kh);
            return -1;
        }
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }

    if (seg_write_record_varlen(db, target_stream, target_fid, target_off,
                                 hash, key, klen, value, vlen,
                                 slot_capacity, 1) != 0) {
        kfcache_release(&kh);
        if (got_pool) pool_push_free_cap(pool, target_fid, target_off,
                                          slot_capacity, db->slot_size);
        return -1;
    }

    /* Repoint at the slot we already probed — same wrlock window, so the
       slot index from kf_lookup_with_slot is still authoritative. Skips
       a second probe + a second verify_stored_key seg read. */
    kf_repoint_at_slot(&kh, kf_slot, target_stream, target_fid, target_off);
    kfcache_release(&kh);

    /* Tombstone old slot + return it to its stream pool — unlocked.
       For varlen, reads the old record's header to determine capacity. */
    return slotcask_tombstone_and_push_back(db, old_sid, old_fid, old_off);
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

    return slotcask_tombstone_and_push_back(db, old_sid, old_fid, old_off);
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
    for (size_t i = 0; i < n; i++) {
        int s = infos[i].target_sid;
        size_t rec_size = slotcask_record_size_varlen(recs[i].klen, recs[i].vlen);
        SlotcaskFreeSlot fs;
        if (pool_try_pop_for_size(&db->streams[s],
                                  (uint32_t)(24 + recs[i].klen + recs[i].vlen),
                                  db->slot_size, &fs) == 0) {
            infos[i].target = fs;
            infos[i].target.capacity = (uint32_t)rec_size;
            if (fs.capacity > (uint32_t)rec_size)
                pool_split_leftover(db, (uint8_t)s, fs.file_id,
                                    fs.offset + (uint32_t)rec_size,
                                    fs.capacity - (uint32_t)rec_size);
        } else {
            uint32_t fid, off;
            if (append_reserve_single_varlen(db, &db->streams[s], rec_size,
                                             &fid, &off) != 0) {
                free(infos); return -1;
            }
            infos[i].target.file_id = (uint16_t)fid;
            infos[i].target.offset = off;
            infos[i].target.capacity = (uint32_t)rec_size;
        }
    }

    /* 3. Write + repoint + tombstone. */
    for (size_t i = 0; i < n; i++) {
        size_t rec_size = slotcask_record_size_varlen(recs[i].klen, recs[i].vlen);
        int write_rc = seg_write_record_varlen(db, infos[i].target_sid,
                                               infos[i].target.file_id,
                                               infos[i].target.offset,
                                               infos[i].hash,
                                               recs[i].key, recs[i].klen,
                                               recs[i].value, recs[i].vlen,
                                               (uint32_t)rec_size, 0);
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
        if (slotcask_tombstone_and_push_back(db, infos[i].old_sid,
                                             infos[i].old_fid,
                                             infos[i].old_off) != 0) {
            free(infos);
            return -1;
        }
    }

    free(infos);
    return 0;
}

/* ============================================================ Open / close */

static int data_file_id_from_name(const char *name) {
    size_t len = strlen(name);
    if (len < 4 || strcmp(name + len - 4, ".dat") != 0) return -1;
    if (len != 10) return -2;
    uint32_t id = 0;
    for (int i = 0; i < 6; i++) {
        if (name[i] < '0' || name[i] > '9') return -2;
        id = id * 10u + (uint32_t)(name[i] - '0');
    }
    return id <= UINT16_MAX ? (int)id : -2;
}

enum StreamSegmentState {
    STREAM_SEGMENTS_ERROR = -1,
    STREAM_SEGMENTS_INVALID = -2,
    STREAM_SEGMENTS_EMPTY = 0,
    STREAM_SEGMENTS_PRESENT = 1
};

static int stream_segment_state(const char *stream_dir) {
    DIR *dir = opendir(stream_dir);
    if (!dir) return STREAM_SEGMENTS_ERROR;
    int has = 0;
    int saved_errno = 0;
    errno = 0;
    for (struct dirent *de = readdir(dir); de; de = readdir(dir)) {
        int parsed = data_file_id_from_name(de->d_name);
        if (parsed == -1) continue;
        if (parsed == -2) {
            closedir(dir);
            return STREAM_SEGMENTS_INVALID;
        }
        has = 1;
    }
    saved_errno = errno;
    int close_rc = closedir(dir);
    if (saved_errno != 0 || close_rc != 0) return STREAM_SEGMENTS_ERROR;
    return has ? STREAM_SEGMENTS_PRESENT : STREAM_SEGMENTS_EMPTY;
}

int slotcask_validate_segment_files(const char *data_dir, int num_streams) {
    if (!data_dir || num_streams <= 0 || num_streams > SLOTCASK_MAX_STREAMS) {
        errno = EINVAL;
        return -1;
    }
    char streams_dir[PATH_MAX];
    snprintf(streams_dir, sizeof(streams_dir), "%s/streams", data_dir);
    for (int stream_id = 0; stream_id < num_streams; stream_id++) {
        char stream_dir[PATH_MAX];
        snprintf(stream_dir, sizeof(stream_dir), "%s/%03d", streams_dir,
                 stream_id);
        errno = 0;
        int state = stream_segment_state(stream_dir);
        if (state == STREAM_SEGMENTS_ERROR && errno == ENOENT) {
            /* schema.conf's declared stream count can legitimately exceed
               what's on disk (CPU-upgrade resize, hand-edited schema.conf) --
               slotcask_open() transparently mkdirp's any missing stream
               directory on the next real open, and `vacuum` rebalances
               existing segments across the new count. That's the documented
               self-heal path, so a missing-directory count mismatch alone
               isn't corruption and must not block startup. */
            continue;
        }
        if (state == STREAM_SEGMENTS_INVALID) {
            errno = EUCLEAN;
            return -1;
        }
        if (state == STREAM_SEGMENTS_ERROR) return -1;
    }
    return 0;
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

    /* Records are variable-length, so use a carry buffer to handle records
       that straddle O_DIRECT chunk boundaries. */
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
                    /* Bytes actually remaining in this chunk starting at
                       buf + pos — NOT the full chunk size nr, since Stage 1
                       (header completion) may have already consumed pos
                       bytes from the front of this same chunk
                       (CID 1696471). */
                    size_t remain = (size_t)nr - pos;
                    if ((size_t)need2 > remain) {
                        if ((size_t)(carry_len + remain) > carry_cap) {
                            carry_cap = (size_t)(carry_len + remain);
                            uint8_t *nc = realloc(carry, carry_cap);
                            if (!nc) { free(carry); free(buf); close(fd); return -1; }
                            carry = nc;
                        }
                        memcpy(carry + carry_len, buf + pos, remain);
                        carry_len += (int)remain;
                        file_off2 += nr; continue;
                    }
                    if (rec_size > carry_cap) {
                        carry_cap = rec_size;
                        uint8_t *nc = realloc(carry, carry_cap);
                        if (!nc) { free(carry); free(buf); close(fd); return -1; }
                        carry = nc;
                    }
                    memcpy(carry + carry_len, buf + pos, (size_t)need2);
                    pos += (size_t)need2;
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
    errno = 0;
    while ((de = readdir(d)) != NULL) {
        int id = data_file_id_from_name(de->d_name);
        if (id == -1) continue;
        if (id == -2) {
            free(ids);
            closedir(d);
            errno = EUCLEAN;
            return -1;
        }
        if (n_ids == cap_ids) {
            cap_ids = cap_ids ? cap_ids * 2 : 64;
            int *nids = realloc(ids, cap_ids * sizeof(int));
            if (!nids) { free(ids); closedir(d); return -1; }
            ids = nids;
        }
        ids[n_ids++] = id;
    }
    int read_errno = errno;
    int close_rc = closedir(d);
    if (read_errno != 0 || close_rc != 0) {
        free(ids);
        return -1;
    }
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
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) { free(ids); return -1; }
        off_t pos = 0;
        off_t lim = (off_t)h.map_size;
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
        last_offset = pos;
        segcache_release(&h);
    }
    /* Every writer of active_file_id/reserve_off follows the rotation lock:
       every writer of active_file_id/reserve_off holds rotation_lock
       (CID 1696416, CID 1696410). rotation_lock for this stream was already
       pthread_mutex_init'd earlier in slotcask_open, so this is always safe
       to take here. */
    pthread_mutex_lock(&db->streams[sid].rotation_lock);
    db->streams[sid].active_file_id = (uint32_t)last_id;
    db->streams[sid].reserve_off = (uint64_t)last_offset;
    pthread_mutex_unlock(&db->streams[sid].rotation_lock);
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
    db->slots_per_shard = slotcask_default_slots_for_splits(num_shards);

    if (mkdirp_local(data_dir) != 0) return -1;

    pthread_mutex_init(&db->trim_init_lock, NULL);
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

    int *segment_states = calloc((size_t)num_streams, sizeof(int));
    if (!segment_states) { free(open_args); goto fail; }
    for (int i = 0; i < num_streams; i++) {
        segment_states[i] = stream_segment_state(db->streams[i].stream_dir);
        if (segment_states[i] < 0) {
            errno = segment_states[i] == STREAM_SEGMENTS_INVALID ? EUCLEAN : errno;
            free(segment_states);
            free(open_args);
            goto fail;
        }
    }

    /* Always run recover_streams — reserve_off / active_file_id aren't
       persisted, so a clean close + reopen would otherwise leave them
       at 0 and the next write would clobber a live record at the head
       of the active segment. No-op when the directory is empty. */
    if (recover_streams(db) != 0) {
        free(segment_states);
        free(open_args);
        goto fail;
    }
    for (int i = 0; i < num_streams; i++) {
        if (segment_states[i] != STREAM_SEGMENTS_EMPTY) continue;
        char path[PATH_MAX];
        seg_path_for(path, data_dir, i, 0);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 1, 1, 0) != 0) {
            free(segment_states);
            free(open_args);
            goto fail;
        }
        if (h.slot >= 0) {
            SlotRef *ref = seg_ref_for(db, i, 0);
            if (ref) {
                ref->slot = h.slot;
                ref->gen = atomic_load_explicit(&g_segcache[h.slot].gen,
                                                memory_order_acquire);
            }
        }
        segcache_release(&h);
    }
    free(segment_states);
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
    pthread_mutex_destroy(&db->trim_init_lock);
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
    pthread_mutex_destroy(&db->trim_init_lock);
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
   Returns 0 on success, -1 if any shard header cannot be read or validated. */
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
        if (fd < 0) return -1;
        SlotcaskKfHeader hdr;
        ssize_t n = pread(fd, &hdr, sizeof(hdr), 0);
        close(fd);
        if (n != (ssize_t)sizeof(hdr)) return -1;
        if (hdr.magic != SLOTCASK_KF_MAGIC) return -1;
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
    if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;
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
    SlotcaskTrimFn trim_fn = atomic_load_explicit(&db->trim_fn, memory_order_acquire);
    if (trim_fn)
        vlen = trim_fn(value, vlen, db->trim_ctx);

    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;
    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;

    /* Fail loud on a missed two-phase migration: an indexed, fresh-insert
       capable object with a legacy pre_commit alongside only one half of
       the prepare/apply pair (or with prepare/apply set but the other
       missing) would silently run the fresh-insert path with an
       incomplete hook set. require_existing=1 callers are update-only
       (e.g. v2_update_pre_commit) and do not create fresh keyfile slots. */
    if (opts->has_indexed_fields && !opts->require_existing &&
        ((!!opts->prepare_commit != !!opts->apply_commit) ||
         (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);

    /* Slow path is required when the caller's check fn needs OLD (CAS) or
       when require_existing is set (must know existence to reject missing).
       It is also required for any object with indexed fields: the fast
       path's single-probe kf_put_new decides new-vs-existing by mutating kf
       as part of the probe itself, which makes it structurally impossible to
       have a correctly-shaped marker (has_old depends on that verdict)
       durable before kf commits. upsert_slow_path knows new-vs-existing from
       its own lookup before touching kf, so it can honor the required
       marker-before-kf ordering; the fast path cannot, so it is restricted
       to has_indexed_fields=0 objects, where that ordering doesn't matter. */
    if (opts->require_existing || opts->check_needs_old || opts->has_indexed_fields ||
        opts->new_from_old) {
        return upsert_slow_path(db, stream_id_hint, key, klen, value, vlen,
                                opts, result, hash, sid_kf);
    }

    /* ===== FAST PATH =====
       Skip the up-front kf_lookup; let kf_put_new probe and decide.
       For new keys: 1 probe (vs 2 in slow path). For existing keys:
       upgrade-to-update branch loads OLD and runs the same diff path
       the slow path would. Order: seg → kf → pre_commit so a duplicate
       rejection bails cleanly without leaving stale index entries. */

    /* Fail-closed guard: unreachable with the corrected dispatch condition
       above; keeps a future fast-path dispatch edit from silently writing a
       new_from_old caller's NULL value. */
    if (opts->new_from_old) {
        errno = EINVAL;
        return -1;
    }
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
    size_t rec_size = slotcask_record_size_varlen(klen, vlen);
    got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                       db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = (uint32_t)rec_size;
        if (fs.capacity > slot_capacity)
            pool_split_leftover(db, target_stream, target_fid,
                                target_off + slot_capacity,
                                fs.capacity - slot_capacity);
    } else {
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0)
            return -1;
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }

    /* Write seg. */
    if (seg_write_record_varlen(db, target_stream, target_fid, target_off,
                                 hash, key, klen, value, vlen,
                                 slot_capacity, 1) != 0) {
        if (got_pool) pool_push_free_cap(pool, target_fid, target_off,
                                          slot_capacity, db->slot_size);
        return -1;
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
        /* NEW key path. Run check (with NULL old) and commit. */
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

        /* has_indexed_fields=1 objects are routed to upsert_slow_path before
           kf_put_new above ever runs (see the dispatch in
           slotcask_upsert_with_hooks): this fast path's single-probe
           kf_put_new decides new-vs-existing by mutating kf as part of the
           probe, which makes a correctly-shaped marker (has_old depends on
           that verdict) impossible to make durable before kf commits. So
           this branch only ever runs for zero-index objects, where
           seg-write-then-kf-publish is already crash-safe without a marker. */
        if (opts->pre_commit) {
            int rc = opts->pre_commit(NULL, value, vlen, 0, opts->pre_commit_ctx);
            if (rc != 0) {
                kf_tombstone_at_slot(&kh, put_slot);
                kfcache_release(&kh);
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                return -1;
            }
        }
        {
            size_t cs[] = { put_slot };
            if (kfcache_sync_slots_locked(&kh, cs, 1, 1) != 0) {
                kfcache_release(&kh);
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

        /* has_indexed_fields=1 objects are routed to upsert_slow_path above
           (see the dispatch in slotcask_upsert_with_hooks), so this branch —
           reached only via the fast path's single-probe kf_put_new — only
           ever runs for zero-index objects: seg-write-then-kf-repoint is
           already crash-safe there, no marker needed. pre_commit (if set)
           fires BEFORE the kf repoint: on abort, kf stays untouched (pointing
           at the old record) and the speculative new segment slot is
           tombstoned. */
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
        kf_repoint_at_slot(&kh, ex_slot, target_stream, target_fid, target_off);
        {
            size_t cs[] = { ex_slot };
            if (kfcache_sync_slots_locked(&kh, cs, 1, 0) != 0) {
                kfcache_release(&kh);
                free(old_buf);
                return -1;
            }
        }
        kfcache_release(&kh);
        if (slotcask_tombstone_and_push_back(db, ex_sid, ex_fid, ex_off) != 0) {
            if (result) result->was_update = 1;
            free(old_buf);
            return -1;
        }
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

    /* Opt-in NEW-from-OLD: after the built-in gates above have accepted
       old_ptr, build the replacement from the OLD bytes read under the held
       wrlock — never from a caller-supplied earlier snapshot. Runs before
       any pool_try_pop_* / append_reserve_* call. write_value/write_vlen
       are used for every reservation, segment write, and pre_commit below.
       Only reached via the callers that set new_from_old. */
    size_t write_vlen = vlen;
    const uint8_t *write_value = value;
    uint8_t *callback_value = NULL;

    if (opts->new_from_old) {
        if (!found || !old_ptr) goto new_from_old_failed;
        if ((size_t)db->slot_size < (size_t)24 + klen)
            goto new_from_old_failed;

        size_t out_capacity = (size_t)db->slot_size - 24 - klen;
        callback_value = malloc(out_capacity ? out_capacity : 1);
        if (!callback_value) goto new_from_old_failed;

        write_vlen = 0;
        if (opts->new_from_old(old_ptr, callback_value, out_capacity,
                               &write_vlen, opts->new_from_old_ctx) != 0 ||
            write_vlen > out_capacity) {
            goto new_from_old_failed;
        }
        SlotcaskTrimFn trim_fn = atomic_load_explicit(&db->trim_fn, memory_order_acquire);
        if (trim_fn)
            write_vlen = trim_fn(callback_value, write_vlen, db->trim_ctx);
        if ((size_t)24 + klen + write_vlen > (size_t)db->slot_size)
            goto new_from_old_failed;
        write_value = callback_value;
    }
    goto new_from_old_done;

new_from_old_failed:
    /* Callback-mode abort: no segment was reserved or written, no tombstone
       was created, no marker was written. */
    kfcache_release(&kh);
    free(callback_value);
    free(old_buf);
    return -1;

new_from_old_done:;

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
    size_t rec_size = slotcask_record_size_varlen(klen, write_vlen);
    got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + write_vlen),
                                       db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = (uint32_t)rec_size;
        if (fs.capacity > slot_capacity)
            pool_split_leftover(db, target_stream, target_fid,
                                target_off + slot_capacity,
                                fs.capacity - slot_capacity);
    } else {
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0) {
            kfcache_release(&kh);
            free(callback_value);
            free(old_buf);
            return -1;
        }
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }

    /* Write new record. */
    if (seg_write_record_varlen(db, target_stream, target_fid, target_off,
                                 hash, key, klen, write_value, write_vlen,
                                 slot_capacity, 1) != 0) {
        if (got_pool) pool_push_free_cap(pool, target_fid, target_off,
                                          slot_capacity, db->slot_size);
        kfcache_release(&kh);
        free(callback_value);
        free(old_buf);
        return -1;
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

    if (!opts->has_indexed_fields) {
        /* No marker path — seg-write-then-kf-repoint is already crash-safe.
           pre_commit (if set) still fires unconditionally, BEFORE the kf
           commit: on abort, kf stays untouched and the speculative new
           segment slot is tombstoned. */
        if (opts->pre_commit) {
            int rc = opts->pre_commit(old_ptr, write_value, write_vlen, found, opts->pre_commit_ctx);
            if (rc != 0) {
                kfcache_release(&kh);
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                free(callback_value);
                free(old_buf);
                return -1;
            }
        }
        if (found) {
            kf_repoint_at_slot(&kh, kf_slot, target_stream, target_fid, target_off);
            {
                size_t cs[] = { kf_slot };
                if (kfcache_sync_slots_locked(&kh, cs, 1, 0) != 0) {
                    kfcache_release(&kh);
                    free(callback_value);
                    free(old_buf);
                    return -1;
                }
            }
        } else {
            size_t used_delta = 0;
            size_t insert_slot = 0;
            int kr = kf_put_new(db, &kh, hash, target_stream, target_fid, target_off,
                                key, klen, db->data_dir, &used_delta, &insert_slot);
            if (kr == 1) kr = -1;
            if (kr != 0) {
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                kfcache_release(&kh);
                free(callback_value);
                free(old_buf);
                return -1;
            }
            {
                size_t cs[] = { insert_slot };
                if (kfcache_sync_slots_locked(&kh, cs, 1, 1) != 0) {
                    kfcache_release(&kh);
                    free(callback_value);
                    free(old_buf);
                    return -1;
                }
            }
        }
        kfcache_release(&kh);
        if (found && slotcask_tombstone_and_push_back(db, old_sid, old_fid, old_off) != 0) {
            free(callback_value);
            free(old_buf);
            return -1;
        }
        if (result) { result->was_update = found ? 1 : 0; result->condition_not_met = 0; }
        free(callback_value);
        free(old_buf);
        return 0;
    }

    /* Retained-marker gate before creating any new marker. */
    if (kf_shard_marker_gate(sid_kf, &kh, db->data_dir) != 0) {
        seg_write_flag(db, target_stream, target_fid, target_off, 2);
        pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
        kfcache_release(&kh);
        free(callback_value);
        free(old_buf);
        return -1;
    }

    /* Marker-guarded commit (has_indexed_fields=1). Every branch below
       writes and fsyncs the marker *before* the kf mutation it protects —
       for `found` the old/new locations are already known from the lookup
       above; for insert, the marker's kf_slot is the pre-planned target
       slot (resolved before this point), so its content doesn't depend on
       kf_put_new's result. Once the marker fsync returns, the operation is
       never rolled back: a later kf/index failure is retried via the same
       replay helper startup recovery uses, while this lock is still held,
       and fails closed (aborts, so the next start runs the mandatory
       recovery sweep) if replay still can't converge. */
    if (found) {
        KfMarkerSlot marker = {
            .magic = KF_MARKER_MAGIC, .kf_slot = (uint32_t)kf_slot, .has_old = 1,
            .op = KF_MARKER_OP_UPSERT,
            .old_stream_id = old_sid, .old_file_id = old_fid, .old_offset = old_off,
            .new_stream_id = target_stream, .new_file_id = target_fid, .new_offset = target_off,
        };
        if (kf_marker_write(db->data_dir, sid_kf, &marker) != 0) {
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            kfcache_release(&kh);
            free(callback_value);
            free(old_buf);
            return -1;
        }
        durability_test_pause(db->data_dir, "marker-after-write");

        if (opts->pre_commit) {
            int rc = opts->pre_commit(old_ptr, write_value, write_vlen, 1, opts->pre_commit_ctx);
            if (rc != 0) {
                /* pre_commit IS the forward apply for updates (it has OLD
                   and writes the index diff). For has_indexed_fields=1
                   objects its mutations are durable (commit-intent marker
                   already fsynced), so failures are only legal for genuine
                   I/O/OOM — never a clean rollback. Write the abort sidecar
                   and apply the inverse (re-insert old index entries,
                   tombstone NEW, return the slot to the pool) while the
                   writer lock is held, then reject the record. */
                int saved = errno ? errno : EIO;
                if (kf_marker_abort_single_current_locked(db->data_dir, sid_kf,
                                                          &marker) != 0)
                    kf_marker_fail_closed(db->data_dir, sid_kf,
                                          "abort after index apply on update");
                if (opts->prepare_commit && opts->abort_commit)
                    opts->abort_commit(opts->pre_commit_ctx);
                pool_push_free_cap(pool, target_fid, target_off,
                                   slot_capacity, db->slot_size);
                kfcache_release(&kh);
                free(callback_value);
                free(old_buf);
                errno = saved;
                return -1;
            }
        }

        kf_repoint_at_slot(&kh, kf_slot, target_stream, target_fid, target_off);
        { size_t cs[] = { kf_slot };
          if (kfcache_sync_slots_locked(&kh, cs, 1, 0) != 0) {
              if (kf_marker_replay_current(db->data_dir, sid_kf, &kh, &marker) != 0)
                  kf_marker_fail_closed(db->data_dir, sid_kf, "kf-slot sync after repoint");
              kfcache_release(&kh);
              free(callback_value);
              free(old_buf);
              if (result) { result->was_update = 1; result->condition_not_met = 0; }
              return 0;
          }
        }
        if (kf_marker_clear(db->data_dir, sid_kf) != 0 &&
            opts->out_durability_degraded)
            *opts->out_durability_degraded = 1;
        kfcache_release(&kh);
        if (slotcask_tombstone_and_push_back(db, old_sid, old_fid, old_off) != 0) {
            free(callback_value);
            free(old_buf);
            return -1;
        }
    } else {
        /* Fresh insert: plan the physical (shard, slot) up front — without
           mutating kf — so prepare_commit can see it and any legitimate
           rejection (e.g. bitmap cap) is reported as an ordinary error
           instead of routing through the post-fsync replay/fail-closed
           path. Ordering: plan slot -> prepare_commit -> marker
           write+fsync -> apply_commit -> kf_commit_planned_slot+sync ->
           marker clear. */
        KfInsertPlan plan;
        int prc = kf_plan_insert_slot(db, &kh, hash, key, klen, db->data_dir, &plan);
        if (prc == 1) prc = -1; /* unreachable in practice: !found already ruled this out under the same held wrlock */
        if (prc != 0) {
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            kfcache_release(&kh);
            free(callback_value);
            free(old_buf);
            return -1;
        }

        if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;
        if (opts->out_kf_slot)  *opts->out_kf_slot  = (uint32_t)plan.target_slot;

        if (opts->prepare_commit) {
            int rc = opts->prepare_commit(value, vlen, (uint32_t)plan.target_slot, opts->pre_commit_ctx);
            if (rc != 0) {
                /* No durable mutation happened yet (kf untouched, no
                   marker) — safe to bail cleanly, same as any other
                   pre-commit rejection. */
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                kfcache_release(&kh);
                free(callback_value);
                free(old_buf);
                return -1;
            }
        }

        KfMarkerSlot marker = {
            .magic = KF_MARKER_MAGIC, .kf_slot = (uint32_t)plan.target_slot, .has_old = 0,
            .op = KF_MARKER_OP_UPSERT,
            .new_stream_id = target_stream, .new_file_id = target_fid, .new_offset = target_off,
        };
        if (kf_marker_write(db->data_dir, sid_kf, &marker) != 0) {
            if (opts->prepare_commit && opts->abort_commit)
                opts->abort_commit(opts->pre_commit_ctx);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            kfcache_release(&kh);
            free(callback_value);
            free(old_buf);
            return -1;
        }
        durability_test_pause(db->data_dir, "marker-after-write");

        if (opts->apply_commit) {
            int rc = opts->apply_commit(value, vlen, (uint32_t)plan.target_slot, opts->pre_commit_ctx);
            if (rc != 0) {
                /* Marker is durable. apply_commit failures are only legal
                   for genuine I/O/OOM (prepare_commit already rejected
                   every legitimate policy failure). Unlike the pre-flight
                   check, the commit-intent here must not be converted into
                   a success: write the abort sidecar, apply the inverse
                   (drop the just-written index entries, tombstone NEW,
                   return the slot to the pool), reject the record, and
                   surface the original apply error. */
                int saved = errno ? errno : EIO;
                if (kf_marker_abort_single_current_locked(db->data_dir, sid_kf,
                                                          &marker) != 0)
                    kf_marker_fail_closed(db->data_dir, sid_kf,
                                          "abort after index apply on insert");
                if (opts->prepare_commit && opts->abort_commit)
                    opts->abort_commit(opts->pre_commit_ctx);
                pool_push_free_cap(pool, target_fid, target_off,
                                   slot_capacity, db->slot_size);
                kfcache_release(&kh);
                free(callback_value);
                free(old_buf);
                errno = saved;
                return -1;
            }
        }

        size_t used_delta = 0;
        kf_commit_planned_slot(&kh, &plan, target_stream, target_fid, target_off, &used_delta, NULL);

        { size_t cs[] = { plan.target_slot };
          if (kfcache_sync_slots_locked(&kh, cs, 1, 1) != 0) {
              if (kf_marker_replay_current(db->data_dir, sid_kf, &kh, &marker) != 0)
                  kf_marker_fail_closed(db->data_dir, sid_kf, "kf-slot sync after insert");
              kfcache_release(&kh);
              free(callback_value);
              free(old_buf);
              if (result) { result->was_update = 0; result->condition_not_met = 0; }
              return 0;
          }
        }

        if (kf_marker_clear(db->data_dir, sid_kf) != 0 &&
            opts->out_durability_degraded)
            *opts->out_durability_degraded = 1;
        kfcache_release(&kh);
    }

    if (result) {
        result->was_update = found ? 1 : 0;
        result->condition_not_met = 0;
    }
    free(callback_value);
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
    SlotcaskTrimFn trim_fn = atomic_load_explicit(&db->trim_fn, memory_order_acquire);
    if (trim_fn)
        vlen = trim_fn(value, vlen, db->trim_ctx);

    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;
    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;

    /* require_existing is incompatible with INSERT-only semantics; the caller
       should route through slotcask_upsert_with_hooks for that. */
    if (opts->require_existing) return -1;

    /* See the identical guard in slotcask_upsert_with_hooks — same missed-
       migration hazard for INSERT-only callers. */
    if (opts->has_indexed_fields &&
        ((!!opts->prepare_commit != !!opts->apply_commit) ||
         (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
        errno = EINVAL;
        return -1;
    }

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
    size_t rec_size = slotcask_record_size_varlen(klen, vlen);
    got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                       db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = (uint32_t)rec_size;
        if (fs.capacity > slot_capacity)
            pool_split_leftover(db, target_stream, target_fid,
                                target_off + slot_capacity,
                                fs.capacity - slot_capacity);
    } else {
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0)
            return -1;
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }

    /* Write seg with flag=1 set so kf can point to valid live data. */
    if (seg_write_record_varlen(db, target_stream, target_fid, target_off,
                                 hash, key, klen, value, vlen,
                                 slot_capacity, 1) != 0) {
        if (got_pool) pool_push_free_cap(pool, target_fid, target_off,
                                          slot_capacity, db->slot_size);
        return -1;
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

    if (opts->has_indexed_fields &&
        kf_shard_marker_gate(sid_kf, &kh, db->data_dir) != 0) {
        kfcache_release(&kh);
        seg_write_flag(db, target_stream, target_fid, target_off, 2);
        pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
        return -1;
    }

    if (opts->has_indexed_fields) {
        /* Fresh insert with index maintenance: plan the slot before any
           durable mutation exists, so prepare_commit can legitimately
           reject (e.g. bitmap cap) before the marker or kf entry exist.
           Ordering: plan slot -> prepare_commit -> marker write+fsync ->
           apply_commit -> kf_commit_planned_slot+sync -> marker clear. */
        KfInsertPlan plan;
        int prc = kf_plan_insert_slot(db, &kh, hash, key, klen, db->data_dir, &plan);
        if (prc == 1) {
            /* Duplicate — read the existing record so the caller can
               report it via result->current_value (matches the upsert
               path's behavior for condition_not_met). */
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
        if (prc != 0) {
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            return -1;
        }

        if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;
        if (opts->out_kf_slot)  *opts->out_kf_slot  = (uint32_t)plan.target_slot;

        if (opts->prepare_commit) {
            int rc = opts->prepare_commit(value, vlen, (uint32_t)plan.target_slot, opts->pre_commit_ctx);
            if (rc != 0) {
                kfcache_release(&kh);
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                return -1;
            }
        }

        KfMarkerSlot marker = {
            .magic = KF_MARKER_MAGIC, .kf_slot = (uint32_t)plan.target_slot, .has_old = 0,
            .op = KF_MARKER_OP_UPSERT,
            .new_stream_id = target_stream, .new_file_id = target_fid, .new_offset = target_off,
        };
        if (kf_marker_write(db->data_dir, sid_kf, &marker) != 0) {
            if (opts->prepare_commit && opts->abort_commit)
                opts->abort_commit(opts->pre_commit_ctx);
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            return -1;
        }
        durability_test_pause(db->data_dir, "marker-after-write");

        if (opts->apply_commit) {
            int rc = opts->apply_commit(value, vlen, (uint32_t)plan.target_slot, opts->pre_commit_ctx);
            if (rc != 0) {
                /* Marker is durable; apply_commit failures are only legal
                   for genuine I/O/OOM (prepare_commit already rejected
                   every legitimate policy failure). Write the abort sidecar
                   and apply the inverse (drop the just-written index
                   entries, tombstone NEW, return the slot to the pool),
                   then reject the record with the original apply error —
                   never convert the failed commit into a success. */
                int saved = errno ? errno : EIO;
                if (kf_marker_abort_single_current_locked(db->data_dir, sid_kf,
                                                          &marker) != 0)
                    kf_marker_fail_closed(db->data_dir, sid_kf,
                                          "abort after index apply on insert");
                if (opts->prepare_commit && opts->abort_commit)
                    opts->abort_commit(opts->pre_commit_ctx);
                pool_push_free_cap(pool, target_fid, target_off,
                                   slot_capacity, db->slot_size);
                kfcache_release(&kh);
                if (result) { result->was_update = 0; result->condition_not_met = 0; }
                errno = saved;
                return -1;
            }
        }

        size_t used_delta = 0;
        kf_commit_planned_slot(&kh, &plan, target_stream, target_fid, target_off, &used_delta, NULL);

        { size_t cs[] = { plan.target_slot };
          if (kfcache_sync_slots_locked(&kh, cs, 1, 1) != 0) {
              if (kf_marker_replay_current(db->data_dir, sid_kf, &kh, &marker) != 0)
                  kf_marker_fail_closed(db->data_dir, sid_kf, "kf-slot sync after insert");
              kfcache_release(&kh);
              if (result) { result->was_update = 0; result->condition_not_met = 0; }
              return 0;
          }
        }
        if (kf_marker_clear(db->data_dir, sid_kf) != 0 &&
            opts->out_durability_degraded)
            *opts->out_durability_degraded = 1;
        kfcache_release(&kh);
        return 0;
    }

    /* !has_indexed_fields: unaffected by the two-phase split (no marker,
       no cap-bearing index to reject on) — unchanged legacy single-phase
       path via the composed kf_put_new. */
    size_t used_delta = 0;
    size_t put_slot = 0;
    int put_rc = kf_put_new(db, &kh, hash, target_stream, target_fid, target_off,
                            key, klen, db->data_dir, &used_delta, &put_slot);
    if (put_rc != 0) {
        if (put_rc == 1) {
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

    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = (uint32_t)put_slot;

    if (opts->pre_commit) {
        int rc = opts->pre_commit(NULL, value, vlen, 0, opts->pre_commit_ctx);
        if (rc != 0) {
            kf_tombstone_at_slot(&kh, put_slot);
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            return -1;
        }
    }
    {
        size_t cs[] = { put_slot };
        if (kfcache_sync_slots_locked(&kh, cs, 1, 1) != 0) {
            kfcache_release(&kh);
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
    size_t   plan_slot;         /* kf_plan_window_insert_slot() result for a
                                    fresh-insert (not old_found) record —
                                    populated before the window's marker is
                                    written, consumed by kf_commit_planned_slot
                                    after apply_window runs */
    int      plan_reused_tomb;
    uint8_t  has_plan;
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

/* Phase 3 — per-stream variable-length reserve and segment writes. */
static void bulk_phase3_seg_writes(SlotcaskDb *db,
                                    SlotcaskBulkRec *recs, SlotcaskBulkState *st,
                                    int *stream_counts, int **stream_idx) {
    for (int s = 0; s < db->num_streams; s++) {
        int cnt = stream_counts[s];
        if (cnt == 0) continue;
        SlotcaskStream *pool = &db->streams[s];

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
                st[i].got_pool = 1;
                r->slot_capacity = (uint32_t)rec_size;
                if (fs.capacity > (uint32_t)rec_size)
                    pool_split_leftover(db, (uint8_t)s, fs.file_id,
                                        fs.offset + (uint32_t)rec_size,
                                        fs.capacity - (uint32_t)rec_size);
            } else {
                uint32_t fid, off;
                if (append_reserve_single_varlen(db, pool, rec_size,
                                                 &fid, &off) != 0) {
                    r->status = -1;
                    continue;
                }
                st[i].target_fid = (uint16_t)fid;
                st[i].target_off = off;
                st[i].got_pool = 0;
                r->slot_capacity = (uint32_t)rec_size;
            }
            char path[PATH_MAX];
            seg_path_for(path, db->data_dir, (uint8_t)s, st[i].target_fid);
            SlotcaskSegHandle h;
            if (segcache_acquire(&h, path, 1, 0, 1) != 0) {
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
            SegCacheEntry *e = &g_segcache[h.slot];
            durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
            segcache_release(&h);
        }
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
            if (slotcask_tombstone_and_push_back(db, st[i].old_sid,
                                                 st[i].old_fid,
                                                 st[i].old_off) != 0)
                recs[i].status = -1;
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
        if (segcache_acquire(&h, path, 0, 0, 1) != 0) {
            /* The kf repoint already committed. Surface the cleanup failure,
               but never recycle an old slot whose live flag was not cleared. */
            for (int j = k; j < run_end; j++)
                recs[tomb_idx[j]].status = -1;
            k = run_end;
            continue;
        }
        for (int j = k; j < run_end; j++) {
            int i = tomb_idx[j];
            __atomic_store_n(&h.map[st[i].old_off + 18], 2, __ATOMIC_RELEASE);
            uint16_t klen = seg_rec_klen(h.map + st[i].old_off);
            uint32_t vlen = seg_rec_vlen(h.map + st[i].old_off);
            uint32_t cap  = (uint32_t)slotcask_record_size_varlen(
                                (size_t)klen, (size_t)vlen);
            pool_push_free_cap(&db->streams[sid], st[i].old_fid,
                               st[i].old_off, cap, db->slot_size);
        }
        SegCacheEntry *e = &g_segcache[h.slot];
        durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
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
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;

    /* Bulk equivalent of the single-record guard: fail loud rather than
       silently run an indexed window with half of the prepare_window/
       apply_window pair missing, or a legacy pre_commit alongside only
       one of them. require_existing=1 callers are the bulk-update-only
       sites and do not create fresh keyfile slots. */
    if (opts->has_indexed_fields && !opts->require_existing &&
        ((!!opts->prepare_window != !!opts->apply_window) ||
         (opts->pre_commit && (!opts->prepare_window || !opts->apply_window)))) {
        errno = EINVAL;
        return -1;
    }

    /* Slow path required when caller needs OLD bytes before commit:
       - require_existing: bulk-update gate, must reject missing keys
       - pre_commit_needs_old: indexed update needs old value for diff
       - value_compute: bulk-update derives NEW from OLD
       - prepare_window/apply_window: the two-phase windowed hooks are only
         wired into the slow path (see below) — a caller that sets a valid
         pair but happens not to also set one of the flags above must still
         be routed here, or its hooks would be silently skipped entirely by
         the fast path (which knows nothing about prepare_window/
         apply_window), running the batch with none of the durability
         protection the hook pair is there to provide. */
    if (opts->require_existing || opts->pre_commit_needs_old ||
        opts->value_compute != NULL ||
        (opts->prepare_window && opts->apply_window)) {
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
    if (kf_shard_marker_gate(kf_shard_id, &kh, db->data_dir) != 0) {
        kfcache_release(&kh);
        return -1;
    }

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
                if (segcache_acquire(&h, path, 0, 0, 0) != 0) {
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

    /* ===== Phase 4 — per-record kf commit + pre_commit (under held kf wrlock). */

    if (opts->has_indexed_fields &&
        (opts->pre_commit != NULL || opts->prepare_window != NULL)) {
        /* Indexed path: windowed batch-marker protocol. */
        size_t wi = 0;
        while (wi < n) {
            size_t w_start = wi;
            size_t w_valid = 0;
            size_t w_end = wi;
            while (w_end < n && w_valid < (size_t)BULK_COMMIT_MAX_RECORDS) {
                if (recs[w_end].status == 0 && st[w_end].needs_write)
                    w_valid++;
                w_end++;
            }

            int fd = -1;
            char bpath[PATH_MAX];
            char dpath[PATH_MAX];
            snprintf(dpath, sizeof(dpath), "%s/data/kf", db->data_dir);
            KfMarkerSlot *mslots = NULL;
            /* w_start is strictly increasing across windows within this call,
               so it doubles as a unique batch_id — this keeps each window's
               marker file distinct (a retained/degraded window's marker must
               never be O_TRUNC'd by the next window reusing the same path)
               and matches kf_batch_marker_path's "<shard>_batch_<id>_marker.dat"
               naming, which is what the startup recovery sweep's sscanf
               pattern actually recognizes. */
            uint32_t batch_id = (uint32_t)w_start;
            int keep_marker = 0;
            int marker_cleared = 0;
            int apply_failed = 0;

            if (opts->prepare_window && opts->apply_window) {
                /* ---- Two-phase window protocol. Stages+validates every
                   active record (fresh inserts AND old_found updates
                   alike — a cap rejection must be caught for both) before
                   any durable marker exists, applies the real index
                   mutation once the marker is durable, then commits kf. */
                KfInsertPlan new_plans[BULK_COMMIT_MAX_RECORDS];
                size_t active[BULK_COMMIT_MAX_RECORDS];
                size_t survive[BULK_COMMIT_MAX_RECORDS];
                size_t nactive = 0, nsurvive = 0;

                /* A prepare-window cap rejection invalidates the original
                   reservation overlay: a later fresh key may have probed
                   past the rejected key's planned slot.  Abort the staged
                   hooks, then plan + prepare the surviving set again under
                   a fresh overlay before publishing anything.  Otherwise a
                   committed survivor can sit after an empty probe-chain hole
                   and become unreachable. */
                for (;;) {
                    size_t nnew_plans = 0;
                    nactive = 0;
                    for (size_t j = w_start; j < w_end; j++) {
                        if (recs[j].status != 0 || !st[j].needs_write) continue;
                        SlotcaskBulkRec *r = &recs[j];
                        if (st[j].old_found) {
                            r->kf_shard = kf_shard_id;
                            r->kf_slot  = (uint32_t)st[j].old_kf_slot;
                            r->was_update = 1;
                            if (!r->old_value) {
                                r->old_value = st[j].old_buf;
                                r->old_vlen  = st[j].old_vlen;
                            }
                        } else {
                            KfInsertPlan plan;
                            int prc = kf_plan_window_insert_slot(db, &kh, st[j].hash,
                                                                  r->key, r->klen,
                                                                  db->data_dir,
                                                                  new_plans, nnew_plans, &plan);
                            if (prc == 1) prc = -1;
                            if (prc != 0) {
                                seg_write_flag(db, st[j].target_stream, st[j].target_fid,
                                               st[j].target_off, 2);
                                pool_push_free(&db->streams[st[j].target_stream],
                                               st[j].target_fid, st[j].target_off, db->slot_size);
                                r->status = -1;
                                st[j].needs_write = 0;
                                continue;
                            }
                            new_plans[nnew_plans++] = plan;
                            st[j].plan_slot = plan.target_slot;
                            st[j].plan_reused_tomb = plan.reused_tomb;
                            st[j].has_plan = 1;
                            r->kf_shard = kf_shard_id;
                            r->kf_slot  = (uint32_t)plan.target_slot;
                            r->was_update = 0;
                        }
                        active[nactive++] = j;
                    }

                    if (nactive > 0 &&
                        opts->prepare_window(recs, active, nactive, opts->bulk_hook_ctx) != 0) {
                        for (size_t a = 0; a < nactive; a++) {
                            size_t j = active[a];
                            if (recs[j].status == 0) recs[j].status = -1;
                        }
                    }

                    nsurvive = 0;
                    for (size_t a = 0; a < nactive; a++) {
                        size_t j = active[a];
                        if (recs[j].status != 0) {
                            seg_write_flag(db, st[j].target_stream, st[j].target_fid,
                                           st[j].target_off, 2);
                            pool_push_free(&db->streams[st[j].target_stream],
                                           st[j].target_fid, st[j].target_off, db->slot_size);
                            st[j].needs_write = 0;
                            st[j].has_plan = 0;
                            continue;
                        }
                        survive[nsurvive++] = j;
                    }
                    if (nsurvive == nactive || nsurvive == 0) break;
                    /* Re-preparing a subset is safe only if the caller can
                       tear down the first pass's staged resources.  Current
                       indexed callers provide this hook; refuse to publish a
                       partially prepared generic window rather than reuse
                       stale state if a future caller forgets it. */
                    if (!opts->abort_window) {
                        for (size_t a = 0; a < nsurvive; a++) {
                            size_t j = survive[a];
                            seg_write_flag(db, st[j].target_stream, st[j].target_fid,
                                           st[j].target_off, 2);
                            pool_push_free(&db->streams[st[j].target_stream],
                                           st[j].target_fid, st[j].target_off, db->slot_size);
                            recs[j].status = -1;
                            st[j].needs_write = 0;
                            st[j].has_plan = 0;
                        }
                        nsurvive = 0;
                        break;
                    }
                    opts->abort_window(opts->bulk_hook_ctx);
                }

                if (nsurvive > 0)
                    durability_test_pause(db->data_dir, "bulk-window-prepared");

                if (nsurvive > 0) {
                    kf_batch_marker_path(bpath, sizeof(bpath), db->data_dir,
                                          kf_shard_id, batch_id);
                    mslots = calloc(nsurvive, sizeof(KfMarkerSlot));
                    if (mslots) {
                        fd = open(bpath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
                        if (fd >= 0) {
                            if (fsync_dir(dpath) != 0) { close(fd); fd = -1; unlink(bpath); }
                        }
                        if (fd < 0) { free(mslots); mslots = NULL; }
                    }

                    size_t vi = 0;
                    for (size_t a = 0; a < nsurvive; a++) {
                        size_t j = survive[a];
                        SlotcaskBulkRec *r = &recs[j];
                        if (fd >= 0 && mslots) {
                            KfMarkerSlot *ms = &mslots[vi];
                            memset(ms, 0, sizeof(*ms));
                            ms->magic = KF_MARKER_MAGIC;
                            ms->op = KF_MARKER_OP_UPSERT;
                            ms->new_stream_id = st[j].target_stream;
                            ms->new_file_id = st[j].target_fid;
                            ms->new_offset = st[j].target_off;
                            if (st[j].old_found) {
                                ms->kf_slot = (uint32_t)st[j].old_kf_slot;
                                ms->has_old = 1;
                                ms->old_stream_id = st[j].old_sid;
                                ms->old_file_id = st[j].old_fid;
                                ms->old_offset = st[j].old_off;
                            } else {
                                /* Persist the exact reservation used by
                                   apply_window's bitmap operation. Recovery
                                   must commit this same slot rather than
                                   re-probing after a possible resplit. */
                                ms->kf_slot = (uint32_t)st[j].plan_slot;
                                ms->has_old = 0;
                            }
                            ms->checksum = XXH32(ms, offsetof(KfMarkerSlot, checksum), 0);
                            off_t off = (off_t)(vi * sizeof(KfMarkerSlot));
                            if (pwrite(fd, ms, sizeof(*ms), off) != (ssize_t)sizeof(*ms) ||
                                fsync(fd) != 0) {
                                close(fd); fd = -1;
                                /* This marker file already has a durably fsynced
                                   prefix of valid slots (0..vi-1) from earlier loop
                                   iterations. unlink() alone only removes the
                                   directory entry from the page cache — without a
                                   matching fsync_dir(), a crash before that entry
                                   reaches disk can leave bpath fully intact on
                                   restart, and recovery streams whatever complete,
                                   checksum-valid slots it finds as a legitimate
                                   batch (by design, for the genuine-crash-mid-write
                                   case). That would resurrect this rejected
                                   window's already-written prefix even though the
                                   caller was told the whole batch failed. Fold the
                                   removal into the same synchronous fsync_dir()
                                   barrier the rest of this file uses for durable
                                   unlinks so no such prefix can survive us. */
                                if (unlink(bpath) != 0 || fsync_dir(dpath) != 0)
                                    kf_marker_fail_closed(db->data_dir, kf_shard_id,
                                                          "could not durably discard partial bulk marker");
                                seg_write_flag(db, st[j].target_stream, st[j].target_fid,
                                                st[j].target_off, 2);
                                pool_push_free(&db->streams[st[j].target_stream],
                                                st[j].target_fid, st[j].target_off, db->slot_size);
                                r->status = -1;
                                vi++;
                                continue;
                            }
                        }
                        vi++;
                    }

                    if (fd >= 0 && mslots)
                        durability_test_pause(db->data_dir, "bulk-marker-after-write");

                    /* fd<0 always implies mslots==NULL by this point (either
                       alloc/open/fsync_dir failed upfront, which nulls mslots
                       immediately above, or a mid-loop pwrite/fsync failure
                       set fd=-1 after already unlinking bpath) — so the marker
                       is definitively not durable either way. Checking fd<0
                       alone (not "&& mslots") is what actually rejects the
                       survivors that never got a marker slot written. */
                    if (fd < 0) {
                        for (size_t a = 0; a < nsurvive; a++) {
                            size_t j = survive[a];
                            if (recs[j].status == 0) {
                                seg_write_flag(db, st[j].target_stream, st[j].target_fid,
                                                st[j].target_off, 2);
                                pool_push_free(&db->streams[st[j].target_stream],
                                                st[j].target_fid, st[j].target_off, db->slot_size);
                                recs[j].status = -1;
                            }
                        }
                    }
                }

                size_t apply_active[BULK_COMMIT_MAX_RECORDS];
                size_t napply_active = 0;
                for (size_t a = 0; a < nsurvive; a++) {
                    size_t j = survive[a];
                    if (recs[j].status == 0) apply_active[napply_active++] = j;
                }

                /* ---- apply_window: fires once the window's batch marker
                   is durable, before kf is committed for the surviving
                   records. A nonzero return is always a genuine I/O/OOM
                   failure. Pin ABORT before returning that error: this
                   window must never publish Kf or be forward-replayed on a
                   later restart. */
                if (napply_active > 0) {
                    if (opts->apply_window(recs, apply_active, napply_active, opts->bulk_hook_ctx) != 0) {
                        int saved = errno ? errno : EIO;
                        char abort_path[PATH_MAX];
                        kf_abort_path(abort_path, sizeof(abort_path),
                                      db->data_dir, KF_ABORT_BATCH,
                                      kf_shard_id, batch_id);
                        if (kf_abort_write_sidecar(db->data_dir,
                                                   KF_ABORT_BATCH, kf_shard_id,
                                                   batch_id,
                                                   (uint32_t)nsurvive) != 0)
                            kf_marker_fail_closed(db->data_dir, kf_shard_id,
                                                  "bulk upsert abort sidecar write");
                        char eff_root[PATH_MAX], object[256];
                        split_data_dir(db->data_dir, eff_root,
                                       sizeof(eff_root), object,
                                       sizeof(object));
                        if (kf_batch_marker_abort_locked(
                                eff_root, object, db->data_dir, kf_shard_id,
                                &kh, mslots, nsurvive, bpath,
                                abort_path) != 0)
                            kf_marker_fail_closed(db->data_dir, kf_shard_id,
                                                  "bulk upsert abort recovery");
                        if (opts->abort_window)
                            opts->abort_window(opts->bulk_hook_ctx);
                        for (size_t a = 0; a < napply_active; a++)
                            recs[apply_active[a]].status = -1;
                        for (size_t j = w_end; j < n; j++) {
                            if (recs[j].status == 0 && st[j].needs_write) {
                                seg_write_flag(db, st[j].target_stream,
                                               st[j].target_fid,
                                               st[j].target_off, 2);
                                pool_push_free(&db->streams[st[j].target_stream],
                                               st[j].target_fid,
                                               st[j].target_off,
                                               db->slot_size);
                                recs[j].status = -1;
                            }
                        }
                        marker_cleared = 1;
                        apply_failed = 1;
                        errno = saved;
                    }
                } else if (nactive > 0 && opts->abort_window) {
                    /* prepare_window staged resources (open bitmap writer
                       handles, tracked buffers, queued ops) for this window
                       but every record was rejected before or during marker
                       write — apply_window will never run, so release them
                       here instead of leaking them. */
                    opts->abort_window(opts->bulk_hook_ctx);
                }

                if (!apply_failed)
                    durability_test_pause(db->data_dir, "bulk-window-applied");

                size_t vslots[BULK_COMMIT_MAX_RECORDS];
                size_t nvslots = 0;
                for (size_t a = 0; !apply_failed && a < napply_active; a++) {
                    size_t j = apply_active[a];
                    SlotcaskBulkRec *r = &recs[j];
                    size_t pub_slot;
                    if (st[j].old_found) {
                        pub_slot = st[j].old_kf_slot;
                        kf_repoint_at_slot(&kh, st[j].old_kf_slot,
                                            st[j].target_stream,
                                            st[j].target_fid, st[j].target_off);
                    } else {
                        KfInsertPlan plan;
                        memcpy(plan.hash, st[j].hash, sizeof(plan.hash));
                        plan.target_slot = st[j].plan_slot;
                        plan.reused_tomb = st[j].plan_reused_tomb;
                        plan.key = r->key;
                        plan.klen = r->klen;
                        size_t used_delta = 0;
                        pub_slot = 0;
                        kf_commit_planned_slot(&kh, &plan, st[j].target_stream,
                                                st[j].target_fid, st[j].target_off,
                                                &used_delta, &pub_slot);
                    }
                    r->kf_shard = kf_shard_id;
                    r->kf_slot  = (uint32_t)pub_slot;
                    vslots[nvslots++] = pub_slot;
                }

                if (!apply_failed && nvslots > 0 &&
                    kfcache_sync_slots_locked(&kh, vslots, nvslots, 0) != 0) {
                    int saved = errno ? errno : EIO;
                    if (kf_batch_marker_replay_current_locked(
                            db->data_dir, kf_shard_id, &kh, mslots, nsurvive,
                            bpath) != 0) {
                        errno = saved;
                        kf_marker_fail_closed(db->data_dir, kf_shard_id,
                                              "indexed bulk Kf sync");
                    } else {
                        marker_cleared = 1;
                    }
                }
            } else {
                /* ---- Legacy single-phase path: pre_commit fires per
                   record after kf is already committed and the marker is
                   durable. Preserved unchanged for callers without a batch
                   (bulk-update sites; require_existing = 1). */
                if (w_valid > 0) {
                    kf_batch_marker_path(bpath, sizeof(bpath), db->data_dir,
                                          kf_shard_id, batch_id);
                    mslots = calloc(w_valid, sizeof(KfMarkerSlot));
                    if (mslots) {
                        fd = open(bpath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
                        if (fd >= 0) {
                            if (fsync_dir(dpath) != 0) { close(fd); fd = -1; unlink(bpath); }
                        }
                        if (fd < 0) { free(mslots); mslots = NULL; }
                    }

                    size_t vi = 0;
                    for (size_t j = w_start; j < w_end; j++) {
                        if (recs[j].status != 0 || !st[j].needs_write) continue;
                        SlotcaskBulkRec *r = &recs[j];

                        if (fd >= 0 && mslots) {
                            KfMarkerSlot *ms = &mslots[vi];
                            memset(ms, 0, sizeof(*ms));
                            ms->magic = KF_MARKER_MAGIC;
                            ms->new_stream_id = st[j].target_stream;
                            ms->new_file_id = st[j].target_fid;
                            ms->new_offset = st[j].target_off;
                            if (st[j].old_found) {
                                ms->kf_slot = (uint32_t)st[j].old_kf_slot;
                                ms->has_old = 1;
                                ms->old_stream_id = st[j].old_sid;
                                ms->old_file_id = st[j].old_fid;
                                ms->old_offset = st[j].old_off;
                            } else {
                                ms->kf_slot = UINT32_MAX;
                                ms->has_old = 0;
                            }
                            ms->checksum = XXH32(ms, offsetof(KfMarkerSlot, checksum), 0);
                            off_t off = (off_t)(vi * sizeof(KfMarkerSlot));
                            if (pwrite(fd, ms, sizeof(*ms), off) != (ssize_t)sizeof(*ms) ||
                                fsync(fd) != 0) {
                                close(fd); fd = -1;
                                /* This marker file already has a durably fsynced
                                   prefix of valid slots (0..vi-1) from earlier loop
                                   iterations. unlink() alone only removes the
                                   directory entry from the page cache — without a
                                   matching fsync_dir(), a crash before that entry
                                   reaches disk can leave bpath fully intact on
                                   restart, and recovery streams whatever complete,
                                   checksum-valid slots it finds as a legitimate
                                   batch (by design, for the genuine-crash-mid-write
                                   case). That would resurrect this rejected
                                   window's already-written prefix even though the
                                   caller was told the whole batch failed. Fold the
                                   removal into the same synchronous fsync_dir()
                                   barrier the rest of this file uses for durable
                                   unlinks so no such prefix can survive us. */
                                if (unlink(bpath) != 0 || fsync_dir(dpath) != 0)
                                    kf_marker_fail_closed(db->data_dir, kf_shard_id,
                                                          "could not durably discard partial bulk marker");
                                seg_write_flag(db, st[j].target_stream, st[j].target_fid,
                                                st[j].target_off, 2);
                                pool_push_free(&db->streams[st[j].target_stream],
                                                st[j].target_fid, st[j].target_off, db->slot_size);
                                r->status = -1;
                                vi++;
                                continue;
                            }
                        }
                        vi++;
                    }

                    if (fd >= 0 && mslots)
                        durability_test_pause(db->data_dir, "bulk-marker-after-write");

                    if (fd < 0 && mslots) {
                        for (size_t j = w_start; j < w_end; j++) {
                            if (recs[j].status == 0 && st[j].needs_write) {
                                seg_write_flag(db, st[j].target_stream, st[j].target_fid,
                                                st[j].target_off, 2);
                                pool_push_free(&db->streams[st[j].target_stream],
                                                st[j].target_fid, st[j].target_off, db->slot_size);
                                recs[j].status = -1;
                            }
                        }
                        w_valid = 0;
                    }
                }

                size_t vslots[BULK_COMMIT_MAX_RECORDS];
                size_t nvslots = 0;

                for (size_t j = w_start; j < w_end; j++) {
                    if (recs[j].status != 0 || !st[j].needs_write) continue;
                    SlotcaskBulkRec *r = &recs[j];

                    size_t pub_slot = st[j].old_found ? st[j].old_kf_slot : 0;

                    if (!st[j].old_found) {
                        size_t used_delta = 0;
                        int kf_rc = kf_put_new(db, &kh, st[j].hash,
                                                st[j].target_stream,
                                                st[j].target_fid, st[j].target_off,
                                                r->key, r->klen, db->data_dir,
                                                &used_delta, &pub_slot);
                        if (kf_rc == 1) kf_rc = -1;
                        if (kf_rc != 0) {
                            keep_marker = 1;
                            r->status = -1;
                            continue;
                        }
                    }

                    r->kf_shard = kf_shard_id;
                    r->kf_slot  = (uint32_t)pub_slot;

                    if (st[j].old_found) {
                        kf_repoint_at_slot(&kh, st[j].old_kf_slot,
                                            st[j].target_stream,
                                            st[j].target_fid, st[j].target_off);
                    }

                    vslots[nvslots++] = pub_slot;
                }

                if (nvslots > 0 &&
                    kfcache_sync_slots_locked(&kh, vslots, nvslots, 0) != 0) {
                    keep_marker = 1;
                    if (opts->out_durability_degraded)
                        *opts->out_durability_degraded = 1;
                }

                if (!keep_marker && !marker_cleared) {
                    for (size_t j = w_start; j < w_end; j++) {
                        if (recs[j].status != 0 || !st[j].needs_write) continue;
                        SlotcaskBulkRec *r = &recs[j];

                        if (opts->pre_commit) {
                            const void *old_v = r->old_value ? r->old_value : st[j].old_buf;
                            size_t      old_l = r->old_value ? r->old_vlen  : st[j].old_vlen;
                            SlotcaskOldRecord old_rec = { (const uint8_t *)old_v, old_l };
                            int rc = opts->pre_commit(st[j].old_found ? &old_rec : NULL,
                                                       r, st[j].old_found);
                            if (rc != 0) {
                                keep_marker = 1;
                                if (opts->out_durability_degraded)
                                    *opts->out_durability_degraded = 1;
                                r->status = -1;
                                continue;
                            }
                        }
                        r->was_update = st[j].old_found ? 1 : 0;
                    }
                }
            }

            if (fd >= 0) {
                close(fd);
                if (!keep_marker) {
                    char dpath[PATH_MAX];
                    snprintf(dpath, sizeof(dpath), "%s/data/kf", db->data_dir);
                    if (unlink(bpath) != 0 || fsync_dir(dpath) != 0) {
                        if (opts->out_durability_degraded)
                            *opts->out_durability_degraded = 1;
                    } else {
                        durability_test_pause(db->data_dir, "bulk-window-cleared");
                    }
                } else {
                    if (keep_marker && opts->out_durability_degraded)
                        *opts->out_durability_degraded = 1;
                }
                free(mslots);
            } else if (mslots) {
                free(mslots);
            }

            if (keep_marker) {
                for (size_t j = w_start; j < w_end; j++) {
                    if (recs[j].status == 0 && st[j].needs_write && st[j].old_found)
                        st[j].old_found = 0;
                }
            }

            if (apply_failed) {
                wi = n;
                break;
            }
            wi = w_end;
        }
    } else {
        /* Non-indexed path: original single-loop Phase 4. */
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
                kf_repoint_at_slot(&kh, st[i].old_kf_slot, st[i].target_stream,
                                    st[i].target_fid, st[i].target_off);
                kf_rc = 0;
            } else {
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
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;

    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, kf_shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) return -1;
    if (opts->has_indexed_fields &&
        kf_shard_marker_gate(kf_shard_id, &kh, db->data_dir) != 0) {
        kfcache_release(&kh);
        return -1;
    }

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
       Skipped when neither pre_commit_needs_old nor the indexed window path
       needs OLD (apply_window requires the old value for the forward diff). */
    if (opts->pre_commit_needs_old || (opts->has_indexed_fields && opts->apply_window)) {
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
                if (segcache_acquire(&h, path, 0, 0, 0) != 0) {
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

    /* ===== Indexed path: windowed batch-marker protocol (has_indexed_fields
       + apply_window). The contract mirrors bulk-upsert's two-phase window
       but for deletes the "mutation" is the forward index diff
       (old=OLD, new=NULL), which fires AFTER the batch marker is durable.
       On apply_window failure the batch abort sidecar is written and every
       inverse (old=NULL, new=OLD) runs while the kf wrlock is held, then
       every record in the window is rejected. */
    int aborted = 0;
    if (opts->has_indexed_fields && opts->apply_window) {
        size_t wi = 0;
        while (wi < n) {
            size_t w_start = wi;
            size_t w_end   = wi;
            size_t nactive = 0;
            size_t active[BULK_COMMIT_MAX_RECORDS];

            while (w_end < n && nactive < (size_t)BULK_COMMIT_MAX_RECORDS) {
                if (recs[w_end].status == 0 && st[w_end].found)
                    active[nactive++] = w_end;
                w_end++;
            }

            if (nactive == 0) { wi = w_end; continue; }

            /* ---- Publish OLD values into recs early so prepare_window
               can read them for criteria/CAS checks before the marker. */
            for (size_t a = 0; a < nactive; a++) {
                size_t j = active[a];
                SlotcaskBulkRec *r = &recs[j];
                r->kf_shard = kf_shard_id;
                r->kf_slot  = (uint32_t)st[j].kf_slot;
                if (!r->old_value && st[j].old_buf) {
                    r->old_value = st[j].old_buf;
                    r->old_vlen  = st[j].old_vlen;
                }
            }

            /* ---- prepare_window: fires BEFORE the marker, allowing
               callers (e.g. criteria-based bulk delete) to reject records
               that fail CAS or criteria checks before any durable state
               is created. Survivors are compacted into the active array. */
            if (opts->prepare_window &&
                opts->prepare_window(recs, active, nactive,
                                     opts->bulk_hook_ctx) != 0) {
                /* prepare_window returned non-zero = all records rejected. */
                for (size_t a = 0; a < nactive; a++)
                    recs[active[a]].status = -1;
                if (opts->abort_window)
                    opts->abort_window(opts->bulk_hook_ctx);
                nactive = 0;
            }

            /* ---- Compact active[] to remove any records that
               prepare_window rejected (status != 0). */
            if (opts->prepare_window && nactive > 0) {
                size_t wi2 = 0;
                for (size_t a = 0; a < nactive; a++) {
                    if (recs[active[a]].status == 0)
                        active[wi2++] = active[a];
                }
                nactive = wi2;
            }

            if (nactive == 0) { wi = w_end; continue; }

            uint32_t batch_id = (uint32_t)w_start;
            int fd = -1;
            char bpath[PATH_MAX], dpath[PATH_MAX];
            KfMarkerSlot *mslots = NULL;
            int keep_marker = 0;

            snprintf(dpath, sizeof(dpath), "%s/data/kf", db->data_dir);
            kf_batch_marker_path(bpath, sizeof(bpath), db->data_dir,
                                  kf_shard_id, batch_id);

            /* ---- Write batch delete markers (op=DELETE, has_old=1). */
            mslots = calloc(nactive, sizeof(KfMarkerSlot));
            if (mslots) {
                fd = open(bpath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
                if (fd >= 0) {
                    if (fsync_dir(dpath) != 0) {
                        close(fd); fd = -1; unlink(bpath);
                    }
                }
                if (fd < 0) { free(mslots); mslots = NULL; }
            }

            if (!mslots) {
                for (size_t a = 0; a < nactive; a++)
                    recs[active[a]].status = -1;
                wi = w_end;
                continue;
            }

            int marker_ok = 1;
            for (size_t a = 0; a < nactive && marker_ok; a++) {
                size_t j = active[a];
                KfMarkerSlot *ms = &mslots[a];
                memset(ms, 0, sizeof(*ms));
                ms->magic         = KF_MARKER_MAGIC;
                ms->kf_slot       = (uint32_t)st[j].kf_slot;
                ms->op            = KF_MARKER_OP_DELETE;
                ms->has_old       = 1;
                ms->old_stream_id = st[j].old_sid;
                ms->old_file_id   = st[j].old_fid;
                ms->old_offset    = st[j].old_off;
                ms->checksum      = XXH32(ms, offsetof(KfMarkerSlot, checksum), 0);
                off_t off = (off_t)(a * sizeof(KfMarkerSlot));
                if (pwrite(fd, ms, sizeof(*ms), off) != (ssize_t)sizeof(*ms) ||
                    fsync(fd) != 0) {
                    marker_ok = 0;
                }
            }

            if (marker_ok) {
                durability_test_pause(db->data_dir, "bulk-delete-marker-after-write");
            } else {
                close(fd); fd = -1;
                if (unlink(bpath) != 0 || fsync_dir(dpath) != 0)
                    kf_marker_fail_closed(db->data_dir, kf_shard_id,
                                          "could not durably discard partial bulk marker");
                for (size_t a = 0; a < nactive; a++)
                    recs[active[a]].status = -1;
                free(mslots);
                wi = w_end;
                continue;
            }

            /* ---- apply_window: forward index diff (old=OLD, new=NULL). */
            if (opts->apply_window(recs, active, nactive,
                                   opts->bulk_hook_ctx) != 0) {
                /* Abort: write sidecar, run inverse per entry, reject. */
                int saved = errno ? errno : EIO;
                char abort_path[PATH_MAX];
                kf_abort_path(abort_path, sizeof(abort_path), db->data_dir,
                              KF_ABORT_BATCH, kf_shard_id, batch_id);
                if (kf_abort_write_sidecar(db->data_dir, KF_ABORT_BATCH,
                                            kf_shard_id, batch_id,
                                            (uint32_t)nactive) != 0) {
                    kf_marker_fail_closed(db->data_dir, kf_shard_id,
                                          "bulk delete abort sidecar write");
                }
                {
                    char eff_root[PATH_MAX], object[256];
                    split_data_dir(db->data_dir, eff_root, sizeof(eff_root),
                                   object, sizeof(object));
                    if (kf_batch_marker_abort_locked(eff_root, object,
                                                     db->data_dir, kf_shard_id,
                                                     &kh, mslots, nactive,
                                                     bpath, abort_path) != 0)
                        kf_marker_fail_closed(db->data_dir, kf_shard_id,
                                              "bulk delete abort recovery");
                }
                if (opts->abort_window)
                    opts->abort_window(opts->bulk_hook_ctx);
                if (fd >= 0) close(fd);
                free(mslots);
                for (size_t a = 0; a < nactive; a++)
                    recs[active[a]].status = -1;
                /* Everything from w_end onward was never attempted — the
                   batch stops here. Reject those records too so a caller
                   counting recs[i].status==0 as "deleted" (the default,
                   untouched value) never mistakes an un-attempted record
                   for a successful one. */
                for (size_t i = w_end; i < n; i++)
                    if (recs[i].status == 0) recs[i].status = -1;
                errno = saved;
                aborted = 1;
                /* Break out of the window loop and fall through to the
                   shared kfcache_release() + Phase 3 segment-tombstone
                   sweep below, so windows that committed successfully
                   before this one aborted still get their old segment
                   slots reclaimed. */
                break;
            }

            /* ---- Commit: tombstone kf slots synchronously, then sync. */
            for (size_t a = 0; a < nactive; a++) {
                size_t j = active[a];
                kf_tombstone_at_slot(&kh, st[j].kf_slot);
                st[j].committed = 1;
                recs[j].was_update = 1;
            }

            {
                size_t vslots[BULK_COMMIT_MAX_RECORDS];
                size_t nvslots = 0;
                for (size_t a = 0; a < nactive; a++)
                    vslots[nvslots++] = st[active[a]].kf_slot;
                if (kfcache_sync_slots_locked(&kh, vslots, nvslots, 0) != 0) {
                    keep_marker = 1;
                    if (opts->out_durability_degraded)
                        *opts->out_durability_degraded = 1;
                }
            }

            /* ---- Marker cleanup (binding order: marker unlink, then
               sidecar if present). keep_marker=1 means the committed
               state is converged but marker/sidecar cleanup failed —
               startup recovery handles this. */
            if (fd >= 0) close(fd);
            if (!keep_marker) {
                if (unlink(bpath) != 0 || fsync_dir(dpath) != 0) {
                    if (opts->out_durability_degraded)
                        *opts->out_durability_degraded = 1;
                } else {
                    durability_test_pause(db->data_dir,
                                          "bulk-delete-window-cleared");
                }
            } else {
                if (opts->out_durability_degraded)
                    *opts->out_durability_degraded = 1;
            }
            free(mslots);

            wi = w_end;
        }
    } else {
    /* ===== Legacy Phase 2 — per-record pre_commit + kf_tombstone
       (under wrlock). Unchanged for non-indexed objects. */
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
    } /* end legacy Phase 2 else block */

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
            if (segcache_acquire(&h, path, 0, 0, 1) != 0) {
                for (int j = k; j < run_end; j++)
                    recs[tomb_idx[j]].status = -1;
                k = run_end;
                continue;
            }
            for (int j = k; j < run_end; j++) {
                int i = tomb_idx[j];
                __atomic_store_n(&h.map[st[i].old_off + 18], 2, __ATOMIC_RELEASE);
                pool_push_free(&db->streams[sid], st[i].old_fid, st[i].old_off, db->slot_size);
            }
            SegCacheEntry *e = &g_segcache[h.slot];
            durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
            segcache_release(&h);
            k = run_end;
        }
        free(tomb_idx);
    } else {
        /* OOM: fall back to per-record tombstone via the existing helper. */
        for (size_t i = 0; i < n; i++) {
            if (!st[i].committed) continue;
            if (seg_write_flag(db, st[i].old_sid, st[i].old_fid,
                               st[i].old_off, 2) != 0) {
                recs[i].status = -1;
            } else {
                pool_push_free(&db->streams[st[i].old_sid],
                               st[i].old_fid, st[i].old_off, db->slot_size);
            }
        }
    }

    for (size_t i = 0; i < n; i++) free(st[i].old_buf);
    free(st);
    return aborted ? -1 : 0;
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
    if (segcache_acquire(&h, fa->path, 0, 0, 0) != 0) return NULL;
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
    if (!resolved || resolved_n == 0) { free(resolved); return 0; }
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
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;
    if (opts->has_indexed_fields &&
        (!opts->apply_commit || opts->pre_commit)) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);

    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0) return -1;
    if (opts->has_indexed_fields &&
        kf_shard_marker_gate(sid_kf, &kh, db->data_dir) != 0) {
        kfcache_release(&kh);
        return -1;
    }

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
       might or might not — caller signals via skip_old_read. The delete
       state machine (has_indexed_fields + apply_commit) always needs OLD:
       the forward diff is (old=OLD,new=NULL). Default behavior (flag = 0)
       reads OLD whenever any hook is set, matching the original contract.
       Saves one segcache_acquire + 100B memcpy + malloc/free pair per call
       when set on non-indexed delete. */
    int needs_old = (opts->check != NULL) ||
                    (opts->pre_commit != NULL && !opts->skip_old_read) ||
                    opts->has_indexed_fields;
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

    if (opts->has_indexed_fields) {
        if (opts->prepare_commit &&
            opts->prepare_commit(needs_old ? &old_rec : NULL,
                                  (uint32_t)kf_slot,
                                  opts->pre_commit_ctx) != 0) {
            if (opts->abort_commit)
                opts->abort_commit(opts->pre_commit_ctx);
            kfcache_release(&kh);
            free(old_buf);
            return -1;
        }
    } else if (opts->pre_commit) {
        int rc = opts->pre_commit(needs_old ? &old_rec : NULL,
                                   opts->pre_commit_ctx);
        if (rc != 0) {
            kfcache_release(&kh);
            free(old_buf);
            return -1;
        }
    }

    /* Marker-guarded indexed delete. The forward diff for a delete is
       (old=OLD, new=NULL): the marker is written and fsynced first, then
       apply_commit drops the index entries, then the kf slot is tombstoned
       synchronously (the primitive's own commit point). On apply failure
       the abort sidecar is written, the inverse (old=NULL, new=OLD) runs
       while the writer lock is still held, the record is rejected, and the
       original apply error is returned only after the sidecar was fsynced.
       has_indexed_fields=1 routes here unconditionally when hooks are set. */
    if (opts->has_indexed_fields && opts->apply_commit) {
        KfMarkerSlot marker = {
            .magic = KF_MARKER_MAGIC, .kf_slot = (uint32_t)kf_slot, .has_old = 1,
            .op = KF_MARKER_OP_DELETE,
            .old_stream_id = old_sid, .old_file_id = old_fid,
            .old_offset = old_off,
        };
        if (kf_marker_write(db->data_dir, sid_kf, &marker) != 0) {
            if (opts->prepare_commit && opts->abort_commit)
                opts->abort_commit(opts->pre_commit_ctx);
            kfcache_release(&kh);
            free(old_buf);
            return -1;
        }
        durability_test_pause(db->data_dir, "marker-after-write");

        {
            int arc = opts->apply_commit(needs_old ? &old_rec : NULL,
                                         (uint32_t)kf_slot,
                                         opts->pre_commit_ctx);
            if (arc != 0) {
                int saved = errno ? errno : EIO;
                if (kf_marker_abort_single_current_locked(db->data_dir, sid_kf,
                                                          &marker) != 0)
                    kf_marker_fail_closed(db->data_dir, sid_kf,
                                          "abort after index apply on delete");
                if (opts->abort_commit) opts->abort_commit(opts->pre_commit_ctx);
                kfcache_release(&kh);
                free(old_buf);
                errno = saved;
                return -1;
            }
        }

        /* Commit point: tombstone the kf slot, sync, then clear the marker. */
        kf_tombstone_at_slot(&kh, kf_slot);
        {
            size_t cs[] = { kf_slot };
            if (kfcache_sync_slots_locked(&kh, cs, 1, 0) != 0) {
                if (kf_marker_replay_current(db->data_dir, sid_kf, &kh,
                                             &marker) != 0)
                    kf_marker_fail_closed(db->data_dir, sid_kf,
                                          "kf-slot sync after delete");
                kfcache_release(&kh);
                free(old_buf);
                return 0;
            }
        }
        if (kf_marker_clear(db->data_dir, sid_kf) != 0 &&
            opts->out_durability_degraded)
            *opts->out_durability_degraded = 1;
        kfcache_release(&kh);
        if (slotcask_tombstone_and_push_back(db, old_sid, old_fid,
                                             old_off) != 0) {
            free(old_buf);
            return -1;
        }
        free(old_buf);
        return 0;
    }

    /* Direct flag flip at the captured slot — no probe, no verify. */
    kf_tombstone_at_slot(&kh, kf_slot);
    kfcache_release(&kh);

    if (slotcask_tombstone_and_push_back(db, old_sid, old_fid,
                                         old_off) != 0) {
        free(old_buf);
        return -1;
    }
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

    /* Fast path: probe under the lock; hit returns immediately.  On a
       miss, this thread either becomes the sole opener for `key` (and
       reserves its slot before releasing the lock) or, if another
       thread is already opening the same key, waits on g_reg_cond
       instead of redundantly repeating slotcask_open — which itself
       fans out up to three parallel_for_io() waves across the shared
       IO pool.  Concurrent misses on the same key used to each pay
       that cost independently; on a cold restart with several callers
       missing on the same hot object at once, that duplicated,
       wasted work is what inflated query latency to minutes (2026-07-03
       hn-explorer incident) even though nothing was truly deadlocked. */
    pthread_mutex_lock(&g_reg_lock);
    for (;;) {
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
        if (g_reg[slot].opening) {
            /* Someone else is opening this key (or a colliding one) —
               wait for them to finish, then re-probe from scratch. */
            pthread_cond_wait(&g_reg_cond, &g_reg_lock);
            continue;
        }

        /* We are the sole opener. Reserve the slot by index — do NOT
           re-probe after opening (see plan invariant 5): a concurrent
           slotcask_registry_invalidate() of an unrelated, earlier-in-chain
           key could free a slot that a fresh reg_probe() would stop at
           first, orphaning this reservation. */
        int reserved = slot;
        snprintf(g_reg[reserved].key, sizeof(g_reg[reserved].key), "%s", key);
        g_reg[reserved].opening = 1;
        pthread_mutex_unlock(&g_reg_lock);

        char data_dir[PATH_MAX];
        snprintf(data_dir, sizeof(data_dir), "%s/%s", effective_root, object);

        SlotcaskDb *db = calloc(1, sizeof(SlotcaskDb));
        int open_rc = db ? slotcask_open(db, data_dir, info->splits,
                                          info->streams, info->slot_size)
                          : -1;

        pthread_mutex_lock(&g_reg_lock);
        if (open_rc != 0 || !db) {
            if (db) free(db);
            g_reg[reserved].opening = 0;
            g_reg[reserved].key[0] = '\0';
            pthread_cond_broadcast(&g_reg_cond);
            pthread_mutex_unlock(&g_reg_lock);
            fprintf(stderr, "slotcask_registry: open failed for %s/%s\n",
                    effective_root, object);
            return NULL;
        }
        g_reg[reserved].db = db;
        g_reg[reserved].used = 1;
        g_reg[reserved].opening = 0;
        pthread_cond_broadcast(&g_reg_cond);
        pthread_mutex_unlock(&g_reg_lock);
        return db;
    }
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

int slotcask_validate_live_refs(SlotcaskDb *db, uint64_t *out_invalid) {
    if (!db || !out_invalid) return -1;
    *out_invalid = 0;

    for (int shard = 0; shard < db->num_shards; shard++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, shard);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0)
            return -1;

        for (size_t slot = 0; slot < kh.capacity; slot++) {
            SlotcaskKfEntry *entry = &kh.map[slot];
            if (__atomic_load_n(&entry->flag, __ATOMIC_ACQUIRE) != 1)
                continue;

            int invalid = entry->stream_id >= db->num_streams;
            SlotcaskSegHandle sh = { .slot = -1, .fd = -1 };
            if (!invalid) {
                char seg_path[PATH_MAX];
                seg_path_for(seg_path, db->data_dir, entry->stream_id,
                             entry->file_id);
                if (segcache_acquire(&sh, seg_path, 0, 0, 0) != 0) {
                    invalid = 1;
                } else if (entry->offset > sh.map_size ||
                           sh.map_size - entry->offset < 24) {
                    invalid = 1;
                } else {
                    const uint8_t *record = sh.map + entry->offset;
                    uint16_t klen = seg_rec_klen(record);
                    uint32_t vlen = seg_rec_vlen(record);
                    size_t record_size = slotcask_record_size_varlen(
                        (size_t)klen, (size_t)vlen);
                    size_t encoded_size = 24u + (size_t)klen + (size_t)vlen;

                    if (record_size > (size_t)db->slot_size ||
                        encoded_size > (size_t)db->slot_size ||
                        record_size > sh.map_size - entry->offset ||
                        !seg_rec_live_with_hash(record, entry->hash))
                        invalid = 1;
                }
            }
            if (sh.slot >= 0 || sh.fd >= 0) segcache_release(&sh);
            if (invalid) (*out_invalid)++;
        }
        kfcache_release(&kh);
    }
    return *out_invalid == 0 ? 0 : 1;
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
            if (segcache_acquire(&sh, seg_path, 0, 0, 0) != 0) continue;
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
            if (segcache_acquire(&sh, seg_path, 0, 0, 0) != 0) {
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
int slotcask_walk_one_shard_slots_locked(SlotcaskDb *db, int kf_shard_id,
                                          const SlotcaskKfHandle *kh,
                                          SlotcaskScanSlotCb cb, void *ctx) {
    if (!db || !kh || !kh->map || !cb ||
        kf_shard_id < 0 || kf_shard_id >= db->num_shards)
        return -1;

    int rc = 0;
    for (size_t s = 0; s < kh->capacity; s++) {
        SlotcaskKfEntry *e = &kh->map[s];
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
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) continue;
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
        if (segcache_acquire(&sh, seg_path, 0, 0, 0) != 0) continue;
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
    if (segcache_acquire(&sh, seg_path, 0, 0, 0) != 0) return 0;
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

/* Shared scan body for slotcask_lookup_by_hash / _try — kh must already
   be acquired (reader) by the caller, who also releases it. segcache
   stays blocking in both callers: segcache_acquire call sites are
   confined to slotcask.c/storage.c and never nest under a btree_* call,
   so it isn't part of the kfcache<->bt_cache inversion this function's
   nonblocking sibling exists to avoid. */
static void slotcask_lookup_scan_kf(SlotcaskDb *db, const uint8_t hash16[16],
                                    SlotcaskKfHandle *kh,
                                    SlotcaskScanCb cb, void *ctx) {
    size_t cap = kh->capacity;
    SlotcaskKfEntry *kf = kh->map;
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
        if (segcache_acquire(&sh, seg_path, 0, 0, 0) != 0) continue;
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
}

int slotcask_lookup_by_hash(SlotcaskDb *db, const uint8_t hash16[16],
                            SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return -1;
    int sid_kf = shard_for_hash(hash16, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[sid_kf] : NULL;
    if (kfcache_acquire_direct(&kh, kf_ref, kf_path, db->slots_per_shard,
                               db, sid_kf) != 0) return -1;
    slotcask_lookup_scan_kf(db, hash16, &kh, cb, ctx);
    kfcache_release(&kh);
    return 0;
}

/* Non-blocking counterpart used by btree_idx_walk_ordered's vulnerable
   callbacks (order_index_walk_cb, composite_prefix_cb, cursor_find_cb):
   they hold every open index shard's bt_cache rdlock for the k-way
   merge's lifetime and must not then block on this hash's kfcache rdlock.
   Returns 0 (scan ran), 1 (kf acquire failed), or 2 (would block). */
int slotcask_lookup_by_hash_try(SlotcaskDb *db, const uint8_t hash16[16],
                                SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return 1;
    int sid_kf = shard_for_hash(hash16, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[sid_kf] : NULL;
    if (kfcache_try_acquire_direct(&kh, kf_ref, kf_path, db->slots_per_shard,
                                   db, sid_kf) != 0)
        return (errno == EBUSY) ? 2 : 1;
    slotcask_lookup_scan_kf(db, hash16, &kh, cb, ctx);
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
/* Variable-length variant: walk records by reading headers sequentially.
   Returns -1 (leaving the output counters unset) if the scan hits an
   unrecoverable desync. The caller treats that as unknown stats and
   preserves the file rather than deleting it as empty. */
static int seg_stat_one_varlen(SlotcaskDb *db, int stream_id, uint32_t file_id,
                               uint32_t *out_live, uint32_t *out_total) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;

    size_t file_size = h.map_size;
    uint32_t live = 0, total = 0;
    size_t off = 0;

    while (off + 24 <= file_size) {
        size_t rec_size;
        uint8_t flag;
        uint16_t klen;
        uint32_t vlen;
        int ok = seg_scan_varlen_struct_ok(h.map, file_size, off,
                                            db->slot_size, &rec_size,
                                            &flag, &klen, &vlen);
        if (ok && flag == 0) {
            size_t next;
            if (!seg_scan_varlen_resync(h.map, file_size, off,
                                         db->slot_size, db->slot_size,
                                         &next))
                break; /* ordinary sparse tail */
            off = next;
            continue;
        }
        if (ok && flag != 0)
            ok = seg_scan_varlen_hash_ok(h.map, off, klen);
        if (!ok) {
            size_t next;
            if (!seg_scan_varlen_resync(h.map, file_size, off,
                                         db->slot_size, db->slot_size,
                                         &next)) {
                segcache_release(&h);
                return -1;
            }
            off = next;
            continue;
        }
        total++;
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
    uint32_t    kf_lookup_failed; /* live records where kf lookup returned -1 */
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

    if (durability_msync_range(c->rmap, target_off, donor_rec_size) != 0) {
        c->rc = -1;
        return 1;
    }
    durability_test_pause(c->db->data_dir, "compact-after-recipient-sync");

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
    if (lr != 0) {
        /* Lookup failed: check whether a live kf entry exists for this hash.
           If yes, the entry's stored segment location is inaccessible (file
           missing or corrupt) — count this as a failed update so the donor
           is not deleted.  If the hash has no live entry (deleted or unknown),
           it is a legitimate orphan and we skip silently. */
        size_t cap = kh.capacity;
        size_t kstart = kf_slot_for(hash16, cap);
        for (size_t ki = 0; ki < cap; ki++) {
            size_t kslot = (kstart + ki) % cap;
            SlotcaskKfEntry *ke = &kh.map[kslot];
            if (ke->flag == 0) break;
            if (memcmp(ke->hash, hash16, 16) == 0) {
                if (ke->flag == 1) c->kf_lookup_failed++;
                break;
            }
        }
        kfcache_release(&kh);
        return 0;
    }
    if ((int)cur_sid != c->stream_id || (uint32_t)cur_fid != c->donor_fid) {
        /* Already repointed elsewhere (legitimate orphan). */
        kfcache_release(&kh);
        return 0;
    }

    kf_repoint_at_slot(&kh, kf_slot_idx, (uint8_t)c->stream_id,
                        (uint16_t)c->recipient_fid, target_off);
    kfcache_release(&kh);
    durability_test_pause(c->db->data_dir, "compact-after-kf-repoint");
    return 0;
}

/* Migrate donor → recipient for varlen streams.  Recipient is mmap'd
   for read/write; donor is O_DIRECT-scanned (or mmap'd) through the
   fixed-size O_DIRECT helper using slot_size as the max record stride,
   with each live record forwarded to varlen_compact_cb. */
static int compact_migrate_records_varlen(SlotcaskDb *db, int stream_id,
                                           uint32_t donor_fid,
                                           uint32_t recipient_fid,
                                           uint32_t *out_kf_failed) {
    char donor_path[PATH_MAX], recipient_path[PATH_MAX];
    seg_path_for(donor_path, db->data_dir, stream_id, donor_fid);
    seg_path_for(recipient_path, db->data_dir, stream_id, recipient_fid);

    /* Recipient: mmap for writes and for building the free-slot list. */
    SlotcaskSegHandle rh;
    if (segcache_acquire(&rh, recipient_path, 0, 0, 0) != 0) return -1;

    size_t rmap_size = rh.map_size;

    /* Walk recipient records to find tombstone slots (flag == 2) with
       capacity. Never add flag==0 padding or sparse tail bytes to the free
       list: those bytes are not records and may contain a later record after
       slot reuse. */
    uint32_t *free_offs = NULL;
    uint32_t *free_caps = NULL;
    size_t free_count = 0, free_cap = 0;
    size_t off = 0;

    while (off + 24 <= rmap_size) {
        size_t rec_size;
        uint8_t flag;
        uint16_t klen;
        uint32_t vlen;
        int ok = seg_scan_varlen_struct_ok(rh.map, rmap_size, off,
                                            db->slot_size, &rec_size,
                                            &flag, &klen, &vlen);
        if (!ok) {
            size_t next;
            if (!seg_scan_varlen_resync(rh.map, rmap_size, off,
                                         db->slot_size, db->slot_size,
                                         &next)) {
                free(free_offs);
                free(free_caps);
                segcache_release(&rh);
                return -1;
            }
            off = next;
            continue;
        }
        if (flag == 0) {
            size_t next;
            if (!seg_scan_varlen_resync(rh.map, rmap_size, off,
                                         db->slot_size, db->slot_size,
                                         &next))
                break; /* ordinary sparse tail */
            off = next;
            continue;
        }
        if (!seg_scan_varlen_hash_ok(rh.map, off, klen)) {
            size_t next;
            if (!seg_scan_varlen_resync(rh.map, rmap_size, off,
                                         db->slot_size, db->slot_size,
                                         &next)) {
                free(free_offs);
                free(free_caps);
                segcache_release(&rh);
                return -1;
            }
            off = next;
            continue;
        }
        if (flag == 2) {
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
    }

    VarlenCompactCtx ctx = {
        .db = db, .stream_id = stream_id,
        .donor_fid = donor_fid, .recipient_fid = recipient_fid,
        .rmap = rh.map, .rmap_size = rmap_size,
        .free_offs = free_offs, .free_caps = free_caps,
        .free_count = free_count, .free_next = 0, .rc = 0,
        .kf_lookup_failed = 0,
    };

    /* Donor is read-only, so use the hardened VARLEN scanner. It validates
       headers and resynchronizes across reused-slot zero-padding gaps. */
    {
        int drc = seg_scan_o_direct(donor_path, db->slot_size,
                                           varlen_compact_cb, &ctx);
        if (drc < 0) {
            free(free_offs);
            free(free_caps);
            segcache_release(&rh);
            return -1;
        }
    }

    if (out_kf_failed) *out_kf_failed = ctx.kf_lookup_failed;
    free(free_offs);
    free(free_caps);
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
    if (!dh) return -1;

    SegStat *files = NULL;
    size_t nfiles = 0, fcap = 0;
    int read_errno = 0;
    for (;;) {
        errno = 0;
        struct dirent *de = readdir(dh);
        if (!de) {
            read_errno = errno;
            break;
        }
        int parsed = data_file_id_from_name(de->d_name);
        if (parsed == -1) continue;
        if (parsed < 0) {
            free(files);
            closedir(dh);
            return -1;
        }
        uint32_t fid = (uint32_t)parsed;
        if (fid == active) continue;
        if (nfiles == fcap) {
            size_t nc = fcap ? fcap * 2 : 16;
            SegStat *next = realloc(files, nc * sizeof(*files));
            if (!next) {
                free(files);
                closedir(dh);
                return -1;
            }
            files = next;
            fcap = nc;
        }
        files[nfiles++] = (SegStat){
            .stream_id = stream_id,
            .file_id = fid,
            .live_count = 0,
            .total_slots = 0
        };
    }
    int close_rc = closedir(dh);
    if (read_errno != 0 || close_rc != 0) {
        free(files);
        return -1;
    }
    if (nfiles == 0) {
        free(files);
        return 0;
    }

    for (size_t k = 0; k < nfiles; k++) {
        if (seg_stat_one_varlen(db, stream_id, files[k].file_id,
                                &files[k].live_count,
                                &files[k].total_slots) != 0) {
            free(files);
            return -1;
        }
    }
    qsort(files, nfiles, sizeof(*files), seg_stat_cmp_live_asc);

    int dropped = 0;
    size_t i = 0, j = nfiles - 1;
    while (i < j) {
        if (files[i].total_slots == 0) {
            i++;
            continue;
        }
        if (files[i].live_count == 0) {
            if (compact_drop_seg_file(db, stream_id, files[i].file_id) != 0) {
                free(files);
                return -1;
            }
            dropped++;
            i++;
            continue;
        }
        uint32_t recipient_free =
            files[j].total_slots - files[j].live_count;
        if (recipient_free >= files[i].live_count) {
            uint32_t kf_failed = 0;
            if (compact_migrate_records_varlen(db, stream_id,
                                                files[i].file_id,
                                                files[j].file_id,
                                                &kf_failed) != 0 ||
                kf_failed != 0 ||
                compact_drop_seg_file(db, stream_id,
                                      files[i].file_id) != 0) {
                free(files);
                return -1;
            }
            dropped++;
            files[j].live_count += files[i].live_count;
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
    int total = 0;
    for (int s = 0; s < db->num_streams; s++) {
        int rc = compact_one_stream(db, s);
        if (rc < 0) return -1;
        total += rc;
    }
    if (out_dropped) *out_dropped = total;
    return 0;
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
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 1) != 0)
            return -1;
        int rc = 0;
        if (kh.hdr && kh.hdr->deleted > 0)
            rc = kfcache_resplit_locked(&kh, kh.capacity);
        kfcache_release(&kh);
        if (rc != 0) return -1;
    }
    return 0;
}
