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

/* Linux exposes EUCLEAN for detected on-disk corruption; Darwin does not.
   EIO is the portable errno for an unreadable/corrupt storage object. */
#ifndef EUCLEAN
#define EUCLEAN EIO
#endif
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <errno.h>
#include <dirent.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>
#include "io_direct.h"
#include "seg_scan_varlen.h"

/* TEST_BUILD-only window instrumentation (shard_test_ctl.h); inert unless
 * a regression test arms the controls. */
#ifdef TEST_BUILD
#include "shard_test_ctl.h"
#include "log.h"  /* LOG_AUDIT/LOG_SUB_SLOTCASK — round-5 diagnostic seam only */
long g_shard_test_sync_counts[SHARD_TEST_PHASE_COUNT];
long g_shard_test_fail_phase = -1;
long g_shard_test_fail_occurrence;
int  g_shard_test_fail_postlink;
int  g_shard_test_fail_sticky;
int  g_shard_test_pause_phase = -1;
int  g_shard_test_pause_occurrence = 1;
_Atomic int g_shard_test_pause_hits;
_Atomic int g_shard_test_pause_release;
_Atomic int g_shard_test_bulk_lookup_gap;
_Atomic int g_shard_test_bulk_lookup_gap_hit;
_Atomic int g_shard_test_bulk_lookup_gap_release;
#define SHARD_TEST_NOTE_SYNC(p) (shard_test_note_sync(p))
#define SHARD_TEST_PHASE_PAUSE(p) (shard_test_phase_pause(p))
#define SHARD_TEST_FAIL_POSTLINK g_shard_test_fail_postlink
#else
#define SHARD_TEST_NOTE_SYNC(p) (0)
#define SHARD_TEST_PHASE_PAUSE(p) ((void)0)
#define SHARD_TEST_FAIL_POSTLINK 0
#endif

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
    atomic_store_explicit(&e->used, 0, memory_order_relaxed);
    e->path[0] = '\0';
    /* Increment gen under g_kfcache_lock (caller always holds it).
       Any SlotRef pointing at this slot will fail its gen check after
       this store, forcing the slow-path re-probe. */
    atomic_fetch_add_explicit(&e->gen, 1, memory_order_release);
    __atomic_fetch_sub(&g_kfcache_count, 1, __ATOMIC_RELAXED);
    pthread_rwlock_unlock(&e->rwlock);
    return 1;
}

/* Invalidate one matching slot. Cache table metadata (used/path) is always
   read or written under g_kfcache_lock. Take the entry lock before re-taking
   the table lock: kfcache_acquire_ex can hold an entry lock before retrying
   the table, so waiting entry <- table would deadlock. */
static int kfcache_invalidate_slot_if_prefix(int slot, const char *prefix,
                                             size_t pl) {
    KfCacheEntry *e = &g_kfcache[slot];
    uint64_t expected_gen;
    char expected_path[PATH_MAX];

    pthread_mutex_lock(&g_kfcache_lock);
    if (!atomic_load_explicit(&e->used, memory_order_acquire) ||
        strncmp(e->path, prefix, pl) != 0) {
        pthread_mutex_unlock(&g_kfcache_lock);
        return 0;
    }
    expected_gen = atomic_load_explicit(&e->gen, memory_order_acquire);
    snprintf(expected_path, sizeof(expected_path), "%s", e->path);
    pthread_mutex_unlock(&g_kfcache_lock);

    pthread_rwlock_wrlock(&e->rwlock);
    pthread_mutex_lock(&g_kfcache_lock);
    if (atomic_load_explicit(&e->used, memory_order_acquire) &&
        atomic_load_explicit(&e->gen, memory_order_acquire) == expected_gen &&
        strcmp(e->path, expected_path) == 0 &&
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
        pthread_mutex_unlock(&g_kfcache_lock);
        pthread_rwlock_unlock(&e->rwlock);
        return 1;
    }
    pthread_mutex_unlock(&g_kfcache_lock);
    pthread_rwlock_unlock(&e->rwlock);
    return 0;
}

/* Drop every cached kf shard whose path starts with `prefix`. Used by
   slotcask_registry_invalidate to flush stale mmap regions before the
   on-disk files move (rebuild_object_v2) or vanish (drop-object). */
static void kfcache_invalidate_prefix(const char *prefix) {
    if (!g_kfcache || !prefix || !prefix[0]) return;
    size_t pl = strlen(prefix);
    for (int i = 0; i < g_kfcache_slots; i++) {
        if (kfcache_invalidate_slot_if_prefix(i, prefix, pl) &&
            g_db && g_kfcache_test_hold_ms > 0)
            break;
    }
}

/* Bounded fallback for the uncoordinated inflight-table-full path. */
#define KF_OPEN_MAGIC_WAIT_ATTEMPTS    100
#define KF_OPEN_MAGIC_WAIT_INTERVAL_MS 2

/* Open + size + mmap a keyfile shard. Caller may NOT hold g_kfcache_lock when
   the file system call could block, so we do the heavy lifting outside the
   table mutex (matching bt_open_file's contract in btree.c). */
static int kf_open_file(const char *path, size_t slots_capacity, int writer,
                        int *out_fd, uint8_t **out_base, size_t *out_size,
                        dev_t *out_dev, ino_t *out_ino) {
    int fd;
    int created_fresh = 0;  /* track first-time creation for header init */
    if (g_db) __atomic_fetch_add(&g_kf_open_file_call_count, 1, __ATOMIC_RELAXED);
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
        if (g_db && g_kf_open_create_test_hold_ms > 0) {
            struct timespec hold_ts = {
                g_kf_open_create_test_hold_ms / 1000,
                (long)(g_kf_open_create_test_hold_ms % 1000) * 1000000L
            };
            int hold_rc;
            do {
                hold_rc = nanosleep(&hold_ts, &hold_ts);
            } while (hold_rc != 0 && errno == EINTR);
        }
        /* Stamp version/counters first, magic last, and publish magic with
           release semantics for the bounded fallback reader below. */
        hdr->version = SLOTCASK_KF_VERSION;
        hdr->total = 0;
        hdr->deleted = 0;
        __atomic_store_n(&hdr->magic, SLOTCASK_KF_MAGIC, __ATOMIC_RELEASE);
        msync(m, SLOTCASK_KF_HDR_SIZE, MS_ASYNC);
    } else if (__atomic_load_n(&hdr->magic, __ATOMIC_ACQUIRE) != SLOTCASK_KF_MAGIC) {
        int attempt;
        for (attempt = 0; attempt < KF_OPEN_MAGIC_WAIT_ATTEMPTS; attempt++) {
            struct timespec wait_ts = { 0, KF_OPEN_MAGIC_WAIT_INTERVAL_MS * 1000000L };
            int wait_rc;
            do {
                wait_rc = nanosleep(&wait_ts, &wait_ts);
            } while (wait_rc != 0 && errno == EINTR);
            if (__atomic_load_n(&hdr->magic, __ATOMIC_ACQUIRE) == SLOTCASK_KF_MAGIC)
                break;
        }
        if (__atomic_load_n(&hdr->magic, __ATOMIC_ACQUIRE) != SLOTCASK_KF_MAGIC) {
            munmap(m, want); close(fd);
            errno = EILSEQ;
            return -1;
        }
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

static void kf_open_inflight_release(int *slot_ptr) {
    if (*slot_ptr < 0) return;
    pthread_mutex_lock(&g_kfcache_lock);
    g_kf_open_inflight[*slot_ptr].used = 0;
    pthread_cond_broadcast(&g_kf_open_inflight_cond);
    pthread_mutex_unlock(&g_kfcache_lock);
    *slot_ptr = -1;
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

    /* A thread that never bound its own g_db (e.g. a raw pthread spawned
       directly by a caller using the low-level SlotcaskDb API, bypassing
       shard_db_open/embedded.c) falls back to the process's one exposed
       instance, mirroring objlock.c's get_lock/objlock_init pattern. */
    if (!g_db && g_shard_db_instance) g_db = g_shard_db_instance;

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

    int kf_open_inflight_found = -1;
    for (int i = 0; i < KF_OPEN_INFLIGHT_SLOTS; i++) {
        if (g_kf_open_inflight[i].used &&
            strcmp(g_kf_open_inflight[i].path, path) == 0) {
            kf_open_inflight_found = i;
            break;
        }
    }
    if (kf_open_inflight_found >= 0) {
        pthread_cond_wait(&g_kf_open_inflight_cond, &g_kfcache_lock);
        pthread_mutex_unlock(&g_kfcache_lock);
        goto retry_kfcache_acquire;
    }

    int kf_inflight_slot __attribute__((cleanup(kf_open_inflight_release))) = -1;
    for (int i = 0; i < KF_OPEN_INFLIGHT_SLOTS; i++) {
        if (!g_kf_open_inflight[i].used) {
            g_kf_open_inflight[i].used = 1;
            strncpy(g_kf_open_inflight[i].path, path, PATH_MAX - 1);
            g_kf_open_inflight[i].path[PATH_MAX - 1] = '\0';
            kf_inflight_slot = i;
            break;
        }
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
        /* nonblocking is unreachable here: the miss-path gate above
           (line ~524) already returns -1 for nonblocking before
           kf_open_file() is ever called, so nonblocking is always 0
           by this point (Coverity CID 1700136, occurrence 1 of 2). */
        if (writer) {
            pthread_rwlock_wrlock(lock);
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
    atomic_store_explicit(&e->used, 1, memory_order_release);
    e->last_access = __atomic_add_fetch(&g_kfcache_clock, 1, __ATOMIC_RELAXED);
    e->file_dev = dev;
    e->file_ino = ino;
    __atomic_fetch_add(&g_kfcache_count, 1, __ATOMIC_RELAXED);

    /* Publish under the table mutex, then take the entry lock without holding
       the table mutex. An evictor that wins this race closes the just-opened
       mapping; identity verification detects that and retries safely. */
    pthread_rwlock_t *lock = &e->rwlock;
    pthread_mutex_unlock(&g_kfcache_lock);
    /* nonblocking is unreachable here for the same reason as the re-probe
       branch above: the miss-path gate already returned -1 for nonblocking
       before we ever reached the open+install code (CID 1700136, 2 of 2). */
    if (writer) {
        pthread_rwlock_wrlock(lock);
    } else {
        pthread_rwlock_rdlock(lock);
    }

    if (!e->used || strcmp(e->path, path) != 0 || e->file_dev != dev ||
        e->file_ino != ino) {
        pthread_rwlock_unlock(lock);
        kf_open_inflight_release(&kf_inflight_slot);
        return kfcache_acquire_ex(h, path, slots_capacity, writer, nonblocking);
    }

    h->slot = slot;
    kf_handle_from_entry(h, e);
    /* coverity[missing_unlock] intentional: returning with the per-slot
       rwlock held; caller releases via kfcache_release. */
    return 0;
}

void kfcache_release(SlotcaskKfHandle *h) {
    if (h->slot >= 0) {
        KfCacheEntry *e = &g_kfcache[h->slot];
        if (h->writer) {
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

/* ── Atomic marker publication (Task 3) ──
 *
 * Markers become visible via link() from a uniquely-named temporary: the
 * final name either exists in full (M occurred) or not at all. Temporary
 * names embed pid + process-local counter + clock nonce so two windows on
 * the same shard can never collide; recovery ignores/removes recognised
 * temporaries and never treats them as M. */
static uint64_t g_marker_nonce_seq;

static int marker_make_unique_paths(const char *kf_dir, const char *final_name,
                                    char *tmp_path, size_t tmp_len,
                                    char *final_path, size_t final_len) {
    int n = snprintf(final_path, final_len, "%s/%s", kf_dir, final_name);
    if (n < 0 || (size_t)n >= final_len) { errno = ENAMETOOLONG; return -1; }
    uint64_t seq = __atomic_add_fetch(&g_marker_nonce_seq, 1,
                                      __ATOMIC_RELAXED);
    n = snprintf(tmp_path, tmp_len, "%s/%s.tmp.%d.%llu",
                 kf_dir, final_name, (int)getpid(),
                 (unsigned long long)(now_us() ^ (seq << 32)));
    if (n < 0 || (size_t)n >= tmp_len) { errno = ENAMETOOLONG; return -1; }
    return 0;
}

/* Tri-state contract:
 *   0  = marker published (linked) and its publication is durable
 *   1  = marker IS published but publication durability is unconfirmed
 *        (post-link unlink/fsync_dir failure) — caller MUST treat this as a
 *        post-M failure: forward replay, else EINPROGRESS
 *   -1 = marker was never linked — safe plain pre-M failure */
static int marker_publish_atomic(const char *kf_dir, const char *final_name,
                                 const void *bytes, size_t len) {
    char tmp_path[PATH_MAX], final_path[PATH_MAX];
    const char *p = bytes;
    size_t left = len;
    int fd = -1;

    if (marker_make_unique_paths(kf_dir, final_name, tmp_path,
                                 sizeof(tmp_path), final_path,
                                 sizeof(final_path)) != 0)
        return -1;
    fd = open(tmp_path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return -1;
    while (left > 0) {
        ssize_t n = write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            goto fail_open;
        }
        p += n;
        left -= (size_t)n;
    }
    if (fsync(fd) != 0) goto fail_open;
    if (close(fd) != 0) { fd = -1; goto fail_open; }
    fd = -1;
    if (link(tmp_path, final_path) != 0) goto fail_open;

    /* Past this point the final marker exists: every remaining failure is
     * a post-M outcome and is reported as published-but-pending. */
    if (unlink(tmp_path) == 0 && fsync_dir(kf_dir) == 0)
        return 0;
    return 1;

fail_open:
    if (fd >= 0) close(fd);
    unlink(tmp_path);
    return -1;
}

/* ── KFM2 batch markers (per-Kf-window redo records) ──
 *
 * One file per committed window: <shard>_batch_<begin>_marker.dat, the
 * final name <shard>_batch_<id>_marker.dat. Each entry is the complete redo
 * record — the fixed
 * 32-byte KfMarkerSlot plus identity and typed-value spans — so forward
 * replay can regenerate every secondary-index delta without reading any
 * other state. Serialized as header + entries, each entry followed by
 * key, old_value, new_value. */
enum { KF_BATCH_MARKER_MAGIC = 0x4B464D32u };  /* "KFM2" */
enum { KF_BATCH_MARKER_VERSION = 1 };

typedef struct __attribute__((packed)) {
    uint32_t magic;             /* KF_BATCH_MARKER_MAGIC */
    uint32_t version;
    uint32_t count;
    uint32_t reserved;
} BatchMarkerHeader;

_Static_assert(sizeof(BatchMarkerHeader) == 16, "fixed on-disk header");

typedef struct __attribute__((packed)) {
    KfMarkerSlot slot;          /* slot.checksum = 0 when written; patched
                                   below to cover the whole entry */
    uint8_t  hash[16];
    uint16_t klen;
    uint16_t old_vlen;
    uint16_t new_vlen;
} BatchMarkerEntry;

_Static_assert(sizeof(BatchMarkerEntry) == 54, "fixed on-disk entry");

static int buf_append(uint8_t **buf, size_t *len, size_t *cap,
                      const void *src, size_t n) {
    if (*len + n > *cap) {
        size_t ncap = *cap ? *cap : 256;
        while (ncap < *len + n) ncap *= 2;
        uint8_t *nb = realloc(*buf, ncap);
        if (!nb) return -1;
        *buf = nb;
        *cap = ncap;
    }
    memcpy(*buf + *len, src, n);
    *len += n;
    return 0;
}

/* C (commit): unlink the window's marker and make the unlink durable.
 * Returns 0 ok, -1 on failure (safe degraded state — the retained marker
 * forward-replays idempotently on the next open). */
static int kfm2_clear_batch_marker(const char *data_dir, int kf_shard_id,
                                   uint32_t batch_id) {
    char kf_dir[PATH_MAX], path[PATH_MAX];

    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", data_dir);
    snprintf(path, sizeof(path), "%s/%03x_batch_%u_marker.dat",
             kf_dir, (unsigned)kf_shard_id, batch_id);
    if (unlink(path) != 0 && errno != ENOENT) return -1;
    return fsync_dir(kf_dir);
}

/* Full parse of a retained KFM2 batch marker for cross-process gate replay
 * (the process that published it died before clearing it, so there is no
 * live BulkMutationTxn/hook context to resume — only this file). Walks the
 * variable-length key/old_value/new_value spans exactly as
 * bulk_publish_window_marker_locked wrote them, verifying each entry's checksum
 * (computed over the entry-with-zeroed-checksum plus its spans) before
 * trusting it. Only the fixed BatchMarkerEntry array is returned — replay
 * itself (kf_marker_replay_entry_locked) re-derives OLD/NEW from the live
 * segment records named by entry->slot, so the spans exist to make the
 * on-disk marker self-checksummed, not as replay's data source.
 * Returns 0 (out_entries/out_count populated, caller frees out_entries),
 * 1 (file absent), -1 (short/corrupt/checksum-mismatched — fail closed). */
static int kfm2_read_batch_marker(const char *path, BatchMarkerEntry **out_entries,
                                  size_t *out_count) {
    struct stat st;
    uint8_t *buf = NULL;
    BatchMarkerEntry *entries = NULL;
    int fd = -1, rc = -1;

    *out_entries = NULL;
    *out_count = 0;
    fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return errno == ENOENT ? 1 : -1;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
        st.st_size < (off_t)sizeof(BatchMarkerHeader) ||
        st.st_size > (off_t)(64 * 1024 * 1024))
        goto out;
    buf = malloc((size_t)st.st_size);
    if (!buf) goto out;
    {
        size_t left = (size_t)st.st_size;
        uint8_t *p = buf;
        while (left > 0) {
            ssize_t n = read(fd, p, left);
            if (n < 0) { if (errno == EINTR) continue; goto out; }
            if (n == 0) goto out;              /* truncated mid-read */
            p += n;
            left -= (size_t)n;
        }
    }

    BatchMarkerHeader hdr;
    memcpy(&hdr, buf, sizeof(hdr));
    if (hdr.magic != KF_BATCH_MARKER_MAGIC ||
        hdr.version != KF_BATCH_MARKER_VERSION || hdr.count == 0)
        goto out;

    entries = calloc(hdr.count, sizeof(*entries));
    if (!entries) goto out;

    size_t at = sizeof(hdr);
    size_t total = (size_t)st.st_size;
    for (uint32_t i = 0; i < hdr.count; i++) {
        if (at + sizeof(BatchMarkerEntry) > total) goto out;
        size_t entry_start = at;
        memcpy(&entries[i], buf + at, sizeof(BatchMarkerEntry));
        at += sizeof(BatchMarkerEntry);
        size_t klen = entries[i].klen;
        size_t old_vlen = entries[i].old_vlen;
        size_t new_vlen = entries[i].new_vlen;
        if (at + klen + old_vlen + new_vlen > total) goto out;
        at += klen + old_vlen + new_vlen;

        size_t span_len = at - entry_start;
        uint8_t *verify_buf = malloc(span_len);
        if (!verify_buf) goto out;
        memcpy(verify_buf, buf + entry_start, span_len);
        uint32_t stored_sum = entries[i].slot.checksum;
        memset(verify_buf + offsetof(BatchMarkerEntry, slot) +
                   offsetof(KfMarkerSlot, checksum),
               0, sizeof(uint32_t));
        uint32_t computed = XXH32(verify_buf, span_len, 0);
        free(verify_buf);
        if (computed != stored_sum) goto out;
        if (!kf_marker_op_valid(&entries[i].slot)) goto out;
    }

    *out_entries = entries;
    *out_count = hdr.count;
    entries = NULL;
    rc = 0;
out:
    free(buf);
    free(entries);
    if (fd >= 0) close(fd);
    return rc;
}

/* Test-only accessor: corrupt the first entry's kf_slot in a durable
 * KFM2 batch marker at <kf_shard>/<batch_id>, recomputing that entry's
 * checksum so the file still validates as well-formed KFM2 (magic,
 * version, per-entry checksum) with only kf_slot semantically out of
 * range -- regression coverage for the kf_slot bounds check in
 * kf_marker_replay_upsert_entry_locked. Returns 0 (patched, *out_has_old
 * set), 1 (no marker file at this shard/batch), -1 (I/O or parse error). */
static void kf_batch_marker_path(char *buf, size_t cap, const char *data_dir,
                                  int kf_shard, uint32_t batch_id);

int kf_batch_marker_corrupt_first_kf_slot_for_test(const char *data_dir,
                                                   int kf_shard,
                                                   uint32_t batch_id,
                                                   uint32_t bad_kf_slot,
                                                   int *out_has_old) {
    char path[PATH_MAX];
    struct stat st;
    uint8_t *buf = NULL;
    int fd = -1, rc = -1;

    kf_batch_marker_path(path, sizeof(path), data_dir, kf_shard, batch_id);
    fd = open(path, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return errno == ENOENT ? 1 : -1;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) ||
        st.st_size < (off_t)(sizeof(BatchMarkerHeader) + sizeof(BatchMarkerEntry)))
        goto out;
    buf = malloc((size_t)st.st_size);
    if (!buf) goto out;
    {
        size_t left = (size_t)st.st_size;
        uint8_t *p = buf;
        while (left > 0) {
            ssize_t n = read(fd, p, left);
            if (n < 0) { if (errno == EINTR) continue; goto out; }
            if (n == 0) goto out;
            p += n;
            left -= (size_t)n;
        }
    }

    BatchMarkerHeader hdr;
    memcpy(&hdr, buf, sizeof(hdr));
    if (hdr.magic != KF_BATCH_MARKER_MAGIC ||
        hdr.version != KF_BATCH_MARKER_VERSION || hdr.count == 0)
        goto out;

    {
        size_t entry_start = sizeof(hdr);
        if (entry_start + sizeof(BatchMarkerEntry) > (size_t)st.st_size) goto out;
        BatchMarkerEntry e;
        memcpy(&e, buf + entry_start, sizeof(e));
        size_t span_len = sizeof(e) + e.klen + e.old_vlen + e.new_vlen;
        if (entry_start + span_len > (size_t)st.st_size) goto out;

        if (out_has_old) *out_has_old = e.slot.has_old;
        e.slot.kf_slot = bad_kf_slot;
        e.slot.checksum = 0;
        memcpy(buf + entry_start, &e, sizeof(e));
        uint32_t sum = XXH32(buf + entry_start, span_len, 0);
        memcpy(buf + entry_start + offsetof(BatchMarkerEntry, slot) +
                   offsetof(KfMarkerSlot, checksum), &sum, sizeof(sum));
    }

    if (pwrite(fd, buf, (size_t)st.st_size, 0) != (ssize_t)st.st_size) goto out;
    if (fsync(fd) != 0) goto out;
    rc = 0;
out:
    free(buf);
    if (fd >= 0) close(fd);
    if (rc == 0) {
        char kf_dir[PATH_MAX];
        snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", data_dir);
        int dfd = open(kf_dir, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
    }
    return rc;
}

/* Test-only accessor: read the entry count and each entry's
 * (checksum-validated) KfMarkerSlot from a durable KFM2 batch marker at
 * <kf_shard>/<batch_id>. slots_out must have room for at least max_slots
 * entries; entries beyond max_slots are still counted in *out_count but
 * not copied. Returns 0 (found and validated), 1 (no marker file at this
 * shard/batch), -1 (I/O, parse, or checksum error). */
int kf_batch_marker_read_slots_for_test(const char *data_dir, int kf_shard,
                                        uint32_t batch_id,
                                        KfMarkerSlot *slots_out,
                                        size_t max_slots,
                                        size_t *out_count) {
    char path[PATH_MAX];
    BatchMarkerEntry *entries = NULL;
    size_t count = 0;
    int rc;

    kf_batch_marker_path(path, sizeof(path), data_dir, kf_shard, batch_id);
    rc = kfm2_read_batch_marker(path, &entries, &count);
    if (rc != 0) return rc;
    if (out_count) *out_count = count;
    for (size_t i = 0; i < count && i < max_slots; i++)
        slots_out[i] = entries[i].slot;
    free(entries);
    return 0;
}

/* Accumulate time spent in marker and targeted kf durability barriers. */
static void commit_sync_us_record(uint64_t t0) {
    if (g_db) __atomic_add_fetch(&g_commit_sync_us_total, now_us() - t0, __ATOMIC_RELAXED);
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

    /* Warm path touches g_kfcache = g_db->kfcache: a raw thread with an
       unbound TLS g_db must bind here, or it indexes a garbage table
       (the slow path below already binds; objlock.c:41 precedent). */
    if (!g_db && g_shard_db_instance) g_db = g_shard_db_instance;

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
static int segcache_drop_slot(int slot, CacheDropReason reason, int wait);

static void segcache_invalidate_prefix(const char *prefix) {
    if (!g_segcache || !prefix || !prefix[0]) return;
    size_t pl = strlen(prefix);
    pthread_mutex_lock(&g_segcache_lock);
    for (int i = 0; i < g_segcache_slots; i++) {
        SegCacheEntry *e = &g_segcache[i];
        if (!atomic_load_explicit(&e->used, memory_order_acquire)) continue;
        if (strncmp(e->path, prefix, pl) != 0) continue;
        /* segcache_drop_slot re-verifies identity under the entry wrlock
           and returns with g_segcache_lock held (entry -> table order,
           mirroring kfcache_invalidate_slot_if_prefix). */
        segcache_drop_slot(i, CACHE_DROP_DISCARD, 1);
    }
    pthread_mutex_unlock(&g_segcache_lock);
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

    /* See the matching fallback in kfcache_acquire_ex above / objlock.c's
       get_lock: a thread that never bound its own g_db falls back to the
       process's one exposed instance. */
    if (!g_db && g_shard_db_instance) g_db = g_shard_db_instance;

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
    /* Warm path touches g_segcache = g_db->segcache: bind an unbound
       TLS g_db here too (same rationale as kfcache_acquire_direct_ex). */
    if (!g_db && g_shard_db_instance) g_db = g_shard_db_instance;
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
    int         error;       /* worker-local errno propagated to opener */
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
        a->error = errno ? errno : EIO;
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
    if (kfcache_acquire(&kh, kf_path, a->db->slots_per_shard, 0) != 0)
        return NULL;

    size_t count = 0;
    for (size_t i = 0; i < kh.capacity; i++)
        if (kh.map[i].flag == 2 && kh.map[i].stream_id < a->db->num_streams)
            count++;
    SlotcaskKfEntry *candidates = calloc(count, sizeof(*candidates));
    if (!candidates && count != 0) { kfcache_release(&kh); return NULL; }
    size_t n = 0;
    for (size_t i = 0; i < kh.capacity; i++) {
        if (kh.map[i].flag == 2 && kh.map[i].stream_id < a->db->num_streams)
            candidates[n++] = kh.map[i];
    }
    kfcache_release(&kh);

    for (size_t i = 0; i < n; i++) {
        SlotcaskKfEntry *e = &candidates[i];
        char path[PATH_MAX];
        seg_path_for(path, a->db->data_dir, e->stream_id, e->file_id);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) continue;
        size_t rec_size;
        uint8_t flag;
        uint16_t klen;
        uint32_t vlen;
        int valid = seg_scan_varlen_struct_ok(h.map, h.map_size, e->offset,
                                               (size_t)a->db->slot_size,
                                               &rec_size, &flag, &klen, &vlen);
        if (valid && flag == 2)
            valid = memcmp(h.map + e->offset, e->hash, sizeof(e->hash)) == 0 &&
                    seg_scan_varlen_hash_ok(h.map, e->offset, klen);
        if (valid && flag == 2)
            pool_push_free_cap(&a->db->streams[e->stream_id], e->file_id,
                               e->offset, (uint32_t)rec_size,
                               a->db->slot_size);
        segcache_release(&h);
    }
    free(candidates);
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

/* Writes a tombstone at a kf slot that kf_plan_window_insert_slot reserved
   for a NEW-insert record but that record was ultimately rejected (by
   pre_commit or prepare_window) after the reservation was made. The slot
   is physically still flag==0 at this point -- nothing else in this
   window's commit ever writes it, since the record was excluded from
   plan->active. Left at flag==0, it hard-stops kf_probe_next's chain walk
   (its only stop condition) and can orphan another record in the same
   window whose accepted slot lands further along the same probe chain.
   flag==2 does not stop the walk, so tombstoning restores reachability.
   No header total/deleted bookkeeping: no live record was ever committed
   here, so get_live_count/get_deleted_count's total-minus-deleted
   accounting must not count it either way. */
static void kf_write_tombstone_at_slot(SlotcaskKfHandle *kh, size_t slot,
                                        const uint8_t hash[16]) {
    SlotcaskKfEntry *t = &kh->map[slot];
    memcpy(t->hash, hash, 16);
    t->stream_id = 0;
    t->file_id = 0;
    t->offset = 0;
    __atomic_store_n(&t->flag, 2, __ATOMIC_RELEASE);
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
        const KfMarkerSlot *marker_in, MarkerRecord *out) {
    if (!marker_in) { errno = EILSEQ; return -1; }
    KfMarkerSlot ms_al;
    memcpy(&ms_al, marker_in, sizeof(ms_al)); /* may be unaligned: KFM2 entry stride is 54B */
    const KfMarkerSlot *marker = &ms_al;
    if (!marker->has_old) { errno = EILSEQ; return -1; }
    return marker_record_read_live(data_dir, marker->old_stream_id,
                                   marker->old_file_id, marker->old_offset,
                                   out);
}

static int read_marker_new_live(const char *data_dir,
        const KfMarkerSlot *marker_in, MarkerRecord *out) {
    if (!marker_in) { errno = EILSEQ; return -1; }
    KfMarkerSlot ms_al;
    memcpy(&ms_al, marker_in, sizeof(ms_al)); /* may be unaligned: KFM2 entry stride is 54B */
    const KfMarkerSlot *marker = &ms_al;
    if (marker->op == KF_MARKER_OP_DELETE) {
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

/* Peek a segment record's raw flag byte, distinguishing flag==0 (staged,
   never activated) from a genuine I/O/acquire failure -- unlike
   marker_record_tombstoned, which folds both into -1. The upsert
   forward-replay path needs this distinction to complete a crash that
   landed between marker publication (M) and activation (A): flag==0 there
   is a valid, recoverable state, not corruption. */
static int seg_peek_flag(const char *data_dir, uint8_t stream_id,
                         uint16_t file_id, uint32_t offset) {
    char path[PATH_MAX];
    SlotcaskSegHandle h;
    int rc;

    seg_path_for(path, data_dir, stream_id, file_id);
    if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;
    rc = (int)__atomic_load_n(&h.map[offset + 18], __ATOMIC_ACQUIRE);
    segcache_release(&h);
    return rc;
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

/* Forward delete replay: remove OLD from the indexes, verify then
   durably tombstone its kf slot, durably tombstone the OLD segment
   record, clear the marker. Every step is idempotent, so a crash
   mid-replay can restart safely (Gap B, approved: a prior partial run
   that already tombstoned the kf slot or the segment completes as 0). */
static int kf_marker_replay_delete_entry_locked(const char *eff_root,
        const char *object, const char *data_dir, int kf_shard,
        SlotcaskKfHandle *kh, const KfMarkerSlot *marker_in) {
    MarkerRecord old_rec = {0};
    int rc = -1;
    KfMarkerSlot ms_al;
    memcpy(&ms_al, marker_in, sizeof(ms_al)); /* may be unaligned: KFM2 entry stride is 54B */
    const KfMarkerSlot *marker = &ms_al;

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
        void *kh_opaque, const KfMarkerSlot *marker_in) {
    SlotcaskKfHandle *kh = (SlotcaskKfHandle *)kh_opaque;
    KfMarkerSlot ms_al;
    memcpy(&ms_al, marker_in, sizeof(ms_al)); /* may be unaligned: KFM2 entry stride is 54B */
    const KfMarkerSlot *marker = &ms_al;

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
                                         void *kh_opaque, const KfMarkerSlot *marker_in) {
    SlotcaskKfHandle *kh = (SlotcaskKfHandle *)kh_opaque;
    MarkerRecord new_rec = {0}, old_rec = {0};
    KfMarkerSlot ms_al;
    memcpy(&ms_al, marker_in, sizeof(ms_al)); /* may be unaligned: KFM2 entry stride is 54B */
    const KfMarkerSlot *marker = &ms_al;
    int rc = -1;

    if (!kh || !kh->writer || !marker || marker->op == KF_MARKER_OP_DELETE ||
        !data_dir) {
        errno = EILSEQ;
        return -1;
    }

    /* Step 0: a crash between marker publication (M) and activation (A)
       leaves the NEW payload durably written (P) but still flag==0
       (staged, not yet live) -- read_marker_new_live below requires
       flag==1 and would otherwise fail this replay closed permanently on
       a state that is fully recoverable. Complete the interrupted
       activation now, mirroring bulk_activate_new_payloads_locked's
       durable flip+sync, before proceeding as if A had already run. */
    {
        int new_flag = seg_peek_flag(data_dir, marker->new_stream_id,
                                     marker->new_file_id, marker->new_offset);
        if (new_flag == 0) {
            if (seg_write_flag_durable(data_dir, marker->new_stream_id,
                                       marker->new_file_id,
                                       marker->new_offset, 1) != 0)
                return -1;
        } else if (new_flag != 1) {
            /* Tombstoned or unreadable -- not a valid forward-replay state
               for the NEW payload; fail closed. */
            errno = EILSEQ;
            return -1;
        }
    }

    /* Step 1: read new record from segment (verifies the live flag). */
    if (read_marker_new_live(data_dir, marker, &new_rec) != 0) return -1;

    /* Step 2: if update, read old record. Keep the segcache handle open —
       old_value must stay valid through the index-diff call in steps 4-5
       below, which reads raw bytes directly out of the mmap'd segment. */
    if (marker->has_old && read_marker_old_live(data_dir, marker,
                                                &old_rec) != 0) {
        /* Gap C (approved): the OLD record is already tombstoned because a
           prior run's T phase (tombstone-old) durably completed before
           crashing during the C-phase marker-clear -- the whole update (A,
           I, K, T) already succeeded live, including the index diff in
           steps 4-5 below, and only marker cleanup remains. Confirm via the
           kf slot already repointed to NEW before treating this as done;
           anything else (slot still pointing elsewhere, or a genuinely
           unreadable/corrupt OLD record) still fails closed. */
        int tomb = marker_record_tombstoned(data_dir, marker->old_stream_id,
                                            marker->old_file_id,
                                            marker->old_offset);
        if (tomb == 1 && marker->kf_slot < kh->capacity &&
            kh->map[marker->kf_slot].flag == 1 &&
            kh->map[marker->kf_slot].stream_id == marker->new_stream_id &&
            kh->map[marker->kf_slot].file_id == marker->new_file_id &&
            kh->map[marker->kf_slot].offset == marker->new_offset)
            rc = 0;
        goto out;
    }

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
        if (marker->kf_slot >= kh->capacity) {
            step3_rc = -1;
        } else {
            kf_repoint_at_slot(kh, slot, marker->new_stream_id,
                              marker->new_file_id, marker->new_offset);
            size_t slots[] = { slot };
            if (kfcache_sync_slots_locked(kh, slots, 1, 0) != 0) step3_rc = -1;
        }
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

    /* Gate/recovery replay of the K-equivalent kf-slot write shares the same
       fault-injection phase as the live coordinator's K barrier: a sticky
       test fault must defeat every path that durably lands a kf slot, not
       just the coordinator's own inline retry, or a "stays pending across
       every subsequent call" scenario would be unreachable in tests. */
    if (step3_rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_K))
        step3_rc = -1;

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

    /* T-equivalent: this branch means the crash happened before the live
       coordinator's own T step ran (OLD was still live, not the
       already-tombstoned Gap-C case above) -- replay must still converge
       to the same terminal state the live path would have reached, or the
       OLD record is left permanently live (duplicate-record hazard, and a
       resource leak since its capacity is never returned to the pool).
       Pool reclaim is intentionally NOT done here: this function runs both
       from the live gate (kf_shard_marker_gate, where a SlotcaskDb exists)
       and from the pre-open startup sweep (marker_recovery_sweep_object,
       which runs before slotcask_open and has no live SlotcaskDb/free-pool
       to push into). Durably marking flag=2 is sufficient either way --
       slotcask_open's pool-rebuild scan (slotcask_pool_rebuild_worker)
       unconditionally sweeps every flag==2 kf entry into the free pool on
       next open, and by then every marker has already resolved (the
       startup sweep runs to completion before slotcask_open proceeds), so
       there is no premature-reuse risk in deferring reclaim that far. */
    if (marker->has_old && seg_write_flag_durable(data_dir, marker->old_stream_id,
                                                   marker->old_file_id,
                                                   marker->old_offset, 2) != 0)
        goto out;
    rc = 0;
out:
    marker_record_destroy(&old_rec);
    marker_record_destroy(&new_rec);
    return rc;
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

/* KFM2 marker I/O. */

static void kf_batch_marker_path(char *buf, size_t cap, const char *data_dir,
                                  int kf_shard, uint32_t batch_id) {
    snprintf(buf, cap, "%s/data/kf/%03x_batch_%u_marker.dat",
             data_dir, (unsigned)kf_shard, batch_id);
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

static int batch_marker_id_cmp(const void *a, const void *b) {
    const uint32_t aa = *(const uint32_t *)a;
    const uint32_t bb = *(const uint32_t *)b;
    return (aa > bb) - (aa < bb);
}

/* Retained KFM2 markers replay while their shard writer lock is held, before
   a new window can plan against the keyfile. */
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
        if (!is_marker || marker_shard != kf_shard)
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

    /* KFM2 batch markers never spawn abort sidecars: the coordinator's
       M step validates every CAS/policy/reservation before publishing, so
       there is no legitimate post-M rejection and nothing to roll back —
       only forward replay to C, or EINPROGRESS. */
    int rc = 0;
    for (size_t i = 0; i < nids && rc == 0; i++) {
        char path[PATH_MAX];
        BatchMarkerEntry *entries = NULL;
        size_t count = 0;
        kf_batch_marker_path(path, sizeof(path), data_dir, kf_shard, ids[i]);
        int mrc = kfm2_read_batch_marker(path, &entries, &count);
        if (mrc == 1) {
            /* Vanished between our directory scan and this read: another
               gate already replayed and cleared it. */
            continue;
        }
        if (mrc != 0) {
            /* Short/corrupt/checksum-mismatched: fail closed rather than
               silently proceed past unreplayed durability state. */
            rc = -1;
            break;
        }
        LOG_ERROR(LOG_SUB_DURABILITY,
                  "kf shard %d: retained commit marker %s; attempting "
                  "synchronous forward replay", kf_shard, path);
        for (size_t j = 0; j < count && rc == 0; j++) {
            if (kf_marker_replay_entry_locked(eff_root, object, data_dir,
                                              kf_shard, kh,
                                              &entries[j].slot) != 0)
                rc = -1;
        }
        if (rc == 0 && kfm2_clear_batch_marker(data_dir, kf_shard,
                                               ids[i]) != 0)
            rc = -1;
        free(entries);
    }
    free(ids);
    return rc;
}

/* Every writer recovers retained KFM2 markers before it plans a slot or opens
   a bitmap writer handle. */
static int kf_shard_marker_gate(int kf_shard, SlotcaskKfHandle *kh,
                                const char *data_dir) {
    return kf_batch_marker_gate(kf_shard, kh, data_dir);
}

/* Marker recovery sweep: scan one object's data/kf/ for retained KFM2 final
   markers and replay each to completion. Recognised publication temporaries
   are inert pre-M debris and are removed; all other marker-namespace files
   fail closed because this release only understands KFM2 redo records.
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
        int name_len = (int)strlen(e->d_name);
        int consumed = 0;

        if (sscanf(e->d_name, "%x_batch_%u_marker.dat%n", &kf_shard, &batch_id, &consumed) == 2 &&
            consumed == name_len) {
            /* recognised KFM2 final marker */
        } else if (strstr(e->d_name, "_marker.dat") != NULL ||
                   strstr(e->d_name, "_marker.dat.tmp.") != NULL) {
            /* A stale publication temporary is safe to discard only when it
               exactly derives from a KFM2 final name. */
            int tmp_shard = -1;
            unsigned tmp_batch = 0;
            unsigned tmp_pid = 0;
            unsigned long long tmp_nonce = 0;
            consumed = 0;
            if (sscanf(e->d_name, "%x_batch_%u_marker.dat.tmp.%u.%llu%n",
                       &tmp_shard, &tmp_batch, &tmp_pid, &tmp_nonce,
                       &consumed) == 4 &&
                consumed == name_len) {
                char tmp_path[PATH_MAX];
                snprintf(tmp_path, sizeof(tmp_path), "%s/%s", kf_dir, e->d_name);
                if (unlink(tmp_path) != 0 && errno != ENOENT) rc = -1;
                continue;
            }
            errno = EILSEQ;
            rc = -1;
            continue;
        } else {
            continue;
        }
        if (kf_shard < 0) continue;

        if (out_replayed) (*out_replayed)++;

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
        if (is_batch_match) {
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

/* Task 4 coordinator's K step for a delete entry — probe-and-verify the
   same way kf_repoint does, then flag=2 + bump the shard's deleted
   counter. Idempotent: re-running on an already-tombstoned slot (forward
   replay after a partial K) finds flag!=1 while probing and keeps
   walking, so a second call over the same window is a safe no-op once
   the target slot itself already reads flag=2 -- the probe simply will
   not find a flag==1 match for this key.  */
static int kf_tombstone_entry_locked(SlotcaskKfHandle *kh,
                                     const uint8_t hash[16],
                                     const void *key, size_t klen,
                                     const char *data_dir) {
    KfProbeIter it; kf_probe_init(&it, kh, hash);
    size_t slot;
    while ((slot = kf_probe_next(&it)) != (size_t)-1) {
        SlotcaskKfEntry *e = &kh->map[slot];
        if (e->flag != 1) continue;
        int km = verify_stored_key(data_dir, e->stream_id, e->file_id,
                                   e->offset, key, klen);
        if (km < 0) return -1;
        if (km == 1) {
            kf_tombstone_at_slot(kh, slot);
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
    __atomic_store_n(&dst[18], 0, __ATOMIC_RELEASE);
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

/* Task 4's P step: identical payload write to seg_record_emit, but the
   flag is left at 0 (relaxed store, already the initial value) instead of
   being release-stored to 1. The record stays invisible to every reader
   (seg_rec_live_with_hash, O_DIRECT scans, kf-validated reads) until the
   coordinator's A step (bulk_activate_new_payloads_locked) performs the
   real release-store to 1 after the window's marker is durably published.
   Caller still owns msync/fdatasync of the payload bytes (the P barrier). */
static inline void seg_record_emit_pending(uint8_t *dst, int slot_size,
                                           const uint8_t hash[16],
                                           const void *key, size_t klen,
                                           const void *value, size_t vlen) {
    memcpy(dst, hash, 16);
    uint16_t k16 = (uint16_t)klen;
    memcpy(dst + 16, &k16, 2);
    __atomic_store_n(&dst[18], 0, __ATOMIC_RELEASE);
    dst[19] = 0;
    uint32_t v32 = (uint32_t)vlen;
    memcpy(dst + 20, &v32, 4);
    memcpy(dst + 24, key, klen);
    memcpy(dst + 24 + klen, value, vlen);
    size_t used = 24 + klen + vlen;
    if (used < (size_t)slot_size) {
        memset(dst + used, 0, (size_t)slot_size - used);
    }
}

/* Mark an old seg slot dead (flag=2) WITHOUT returning it to its stream
   pool. Pool reclaim is deferred to bulk_reclaim_old_payloads_locked,
   called only after the owning window's marker is durably cleared (C) --
   see that function's comment for why. */
static inline int slotcask_tombstone_mark(SlotcaskDb *db, uint8_t stream_id,
                                          uint16_t file_id, uint32_t offset) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0, 1) != 0) return -1;
    __atomic_store_n(&h.map[offset + 18], 2, __ATOMIC_RELEASE);
    if (h.slot >= 0) {
        SegCacheEntry *e = &g_segcache[h.slot];
        durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
    }
    segcache_release(&h);
    return 0;
}

/* Set the flag byte at slot (file_id, offset) to `flag`, then make it
   durable: msync the touched page and fdatasync the segment. Used by
   recovery paths whose outcome is judged after a restart, where a
   non-durable tombstone is indistinguishable from no tombstone at all. */
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
                                   db->slots_per_shard, db, sid_kf) != 0)
            return -1;
        uint8_t flag, stream_id;
        uint16_t file_id;
        uint32_t offset;
        int rc = kf_lookup(&kh, hash, key, klen, db->data_dir,
                           &flag, &stream_id, &file_id, &offset);
        if (rc < 0) { kfcache_release(&kh); return -1; }

        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, stream_id, file_id);
        SlotcaskSegHandle sh;
        SlotRef *seg_ref = seg_ref_for(db, stream_id, file_id);
        if (segcache_acquire_direct(&sh, seg_ref, path) != 0) {
            kfcache_release(&kh);
            return -1;
        }

        /* Kf read handle stays live until the segment record has been
           validated against this lookup and copied into caller memory —
           the Kf entry is the visibility boundary (plan 2026-08-21 T2). */
        const uint8_t *rec = sh.map + offset;
        if (!seg_rec_live_with_hash(rec, hash)) {
            segcache_release(&sh);
            kfcache_release(&kh);
            continue;
        }
        uint16_t k_stored = seg_rec_klen(rec);
        uint32_t v_stored = seg_rec_vlen(rec);
        if (k_stored != klen || memcmp(rec + 24, key, klen) != 0) {
            segcache_release(&sh);
            kfcache_release(&kh);
            continue;
        }
        void *vbuf = malloc(v_stored ? v_stored : 1);
        if (!vbuf) {
            segcache_release(&sh);
            kfcache_release(&kh);
            return -1;
        }
        if (v_stored) memcpy(vbuf, rec + 24 + klen, v_stored);
        segcache_release(&sh);
        kfcache_release(&kh);
        *val_out = vbuf;
        *vlen_out = v_stored;
        return 0;
    }
    return -1;
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
    int         error;
} RecoverStreamArg;

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

        if (file_id != last_id)
            continue;

        /* Active (last) segment: mmap via segcache so the first post-startup
           insert doesn't take a cold segcache miss and locate the reserve
           frontier. Free-pool reconstruction is KF-driven after this walk. */
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) { free(ids); return -1; }
        size_t pos = 0;
        size_t lim = h.map_size;
        while (pos + 24 <= lim) {
            size_t rec_size;
            uint8_t flag;
            uint16_t klen;
            uint32_t vlen;
            int valid = seg_scan_varlen_struct_ok(h.map, lim, pos,
                                                   (size_t)db->slot_size,
                                                   &rec_size, &flag,
                                                   &klen, &vlen);
            if (valid && flag != 0)
                valid = seg_scan_varlen_hash_ok(h.map, pos, klen);

            if (!valid || flag == 0) {
                size_t next;
                if (seg_scan_varlen_resync(h.map, lim, pos,
                                            (size_t)db->slot_size,
                                            (size_t)db->slot_size, &next)) {
                    pos = next;
                    continue;
                }
                if (valid && flag == 0)
                    break; /* ordinary unwritten tail */
                segcache_release(&h);
                free(ids);
                errno = EUCLEAN;
                return -1;
            }

            pos += rec_size;
        }
        last_offset = (off_t)pos;
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
    if (a->rc != 0) a->error = errno ? errno : EIO;
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
        if (args[i].rc != 0) {
            errno = args[i].error ? args[i].error : EIO;
            free(args);
            return -1;
        }
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
        if (!open_args[i].ok) {
            errno = open_args[i].error ? open_args[i].error : EIO;
            free(open_args);
            goto fail;
        }
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
    if (errno == 0) errno = EIO;
    pthread_mutex_destroy(&db->trim_init_lock);
    /* slotcask_registry_get frees the outer SlotcaskDb after this return.
       Release every auxiliary allocation made before a failed open too;
       otherwise a deliberately refused reopen leaks its shard/segment
       reference arrays on each request. */
    free(db->kf_slot_refs);
    db->kf_slot_refs = NULL;
    free(db->seg_slot_refs);
    db->seg_slot_refs = NULL;
    free(db->seg_slot_caps);
    db->seg_slot_caps = NULL;
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
        /* Primary path — deliberately LOCK-FREE: read the 24-byte header
         * straight from the OS page cache via pread(2), never through
         * kfcache.
         *
         * An earlier revision took each shard's kf reader here "because
         * the rwlock is the visibility boundary for the counters" — but
         * a mutation window holds one shard's WRLOCK for its entire
         * M..C span (up to BULK_COMMIT_WINDOW records of fsyncs and
         * index applies), which stalled every object-wide counter —
         * bare counts, find-total hints — for the whole window
         * (liveness regression found 2026-08-27 via the
         * bt-kf-inversion test's mid-window probe matrix).
         *
         * Tolerance contract: header counters are advisory metadata.
         * Each shard contributes an internally-consistent pair captured
         * at some instant during this call; concurrent windows mean the
         * object-level result can mix one shard's pre-window value with
         * another's post-window value — indistinguishable from the
         * interleaving a plain concurrent write already produced before
         * windowed commits existed. Dirty mmap pages back pread().
         */
        int hdr_ok = 0;
        SlotcaskKfHeader h;
        int fd = open(kf_path, O_RDONLY);
        if (fd >= 0) {
            uint8_t raw[SLOTCASK_KF_HDR_SIZE];
            size_t got = 0;
            while (got < sizeof(raw)) {
                ssize_t r = pread(fd, raw + got, sizeof(raw) - got,
                                  (off_t)got);
                if (r < 0 && errno == EINTR) continue;
                if (r <= 0) break;
                got += (size_t)r;
            }
            close(fd);
            if (got == sizeof(raw)) {
                memcpy(&h, raw, sizeof(h));
                hdr_ok = (h.magic == SLOTCASK_KF_MAGIC);
            }
        }
        if (!hdr_ok) {
            /* Direct read unavailable (e.g. permissions revoked after
             * first open, or a torn partial header mid-resize): fall back
             * to the ALREADY-MAPPED cached entry, which survives both.
             * Non-blocking on purpose — under contention the caller gets
             * an error instead of stalling behind a mutation window. */
            SlotcaskKfHandle kh;
            if (kfcache_try_acquire_rd(&kh, kf_path,
                                       db->slots_per_shard) != 0)
                return -1;
            if (!kh.hdr || kh.hdr->magic != SLOTCASK_KF_MAGIC) {
                kfcache_release(&kh);
                return -1;
            }
            total   += kh.hdr->total;
            deleted += kh.hdr->deleted;
            kfcache_release(&kh);
            continue;
        }
        total   += h.total;
        deleted += h.deleted;
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
    /* Task 4 coordinator (bulk_plan_window_locked / bulk_apply_and_sync_kf_locked)
       fields — additive, unused by the legacy bulk_upsert_fast/slow paths
       above so those keep working unmodified until Task 5 deletes them. */
    KfInsertPlan kf_plan;
    uint8_t      staged_in_wave;  /* 1 once bulk_stage_payload_wave wrote this
                                      record's NEW segment payload */
} SlotcaskBulkState;

/* ============================================================ Task 4:
 * single window coordinator for every mutation path (single-record and
 * bulk, upsert and delete). See docs/plans/2026-08-21-main-durability-and-window.md
 * Task 4. Runs each Kf shard's window through P (stage payload) -> M
 * (publish marker) -> A (activate) -> I (index) -> K (kf commit) ->
 * T (tombstone OLD) -> C (clear marker), replaying forward on any
 * post-M failure instead of rolling back. */

typedef enum {
    BULK_MUTATION_UPSERT,
    BULK_MUTATION_DELETE,
} BulkMutationKind;

typedef struct {
    int kf_shard_id;
    SlotcaskBulkRec *recs;
    SlotcaskBulkState *st;      /* per-record scratch; parallel to recs */
    size_t nrecs;
    size_t cursor;
    BulkMutationKind kind;
    const SlotcaskBulkOpts *upsert_opts;
    const SlotcaskBulkDeleteOpts *delete_opts;
    int rc;
} BulkMutationShard;

typedef struct {
    SlotcaskDb *db;
    BulkMutationShard *shards;
    size_t nshards;
    size_t window_cap;
    const SlotcaskBulkOpts *upsert_opts;
    const SlotcaskBulkDeleteOpts *delete_opts;
    _Atomic int cancelled;
} BulkMutationTxn;

typedef struct { uint8_t sid; uint16_t fid; uint32_t off; } SegLoc;

static int segloc_cmp(const void *a, const void *b) {
    const SegLoc *x = a, *y = b;
    if (x->sid != y->sid) return x->sid < y->sid ? -1 : 1;
    if (x->fid != y->fid) return x->fid < y->fid ? -1 : 1;
    if (x->off != y->off) return x->off < y->off ? -1 : 1;
    return 0;
}

/* One (field, idx_shard) index flush owed by the current window; deduped
   and flushed once per unique pair after apply_window returns. */
typedef struct {
    char     field[128];        /* matches BitmapPrepareEntry.field width */
    int      idx_shard;
    int      type;              /* enum IndexType */
    uint8_t  hash16[16];        /* representative hash for the flush call */
} IdxTouch;

typedef struct { IdxTouch *v; size_t n, cap; } IdxTouchSet;

/* Installed by bulk_apply_and_sync_indexes_locked for the duration of the
   caller's apply_window hook so query_bulk.c's per-index-op callbacks can
   record what they touched instead of syncing per-record themselves.
   NULL outside of apply_window (single-record hooks still sync directly). */
static _Thread_local IdxTouchSet *tls_idx_touch;

static void __attribute__((unused)) idx_touch_record(const char *field, int idx_shard, int type,
                             const uint8_t hash16[16]) {
    IdxTouchSet *s = tls_idx_touch;
    if (!s) return;
    if (s->n == s->cap) {
        IdxTouch *nv;
        /* Guard BEFORE doubling: s->cap * 2 itself must not overflow. */
        if (s->cap > SIZE_MAX / (2 * sizeof(IdxTouch))) return;
        size_t ncap = s->cap ? s->cap * 2 : 16;
        nv = realloc(s->v, ncap * sizeof(IdxTouch));
        if (!nv) return;
        s->v = nv; s->cap = ncap;
    }
    snprintf(s->v[s->n].field, sizeof(s->v[0].field), "%s", field);
    s->v[s->n].idx_shard = idx_shard;
    s->v[s->n].type = type;
    memcpy(s->v[s->n].hash16, hash16, 16);
    s->n++;
}

typedef struct {
    uint32_t           batch_id;
    int                kf_shard_id;
    BulkMutationShard *shard;
    BatchMarkerEntry  *entries;
    size_t            *active;
    size_t             nactive;
    size_t            *kf_slots;
    size_t             nkf_slots;
    int                kf_header_changed;
    IdxTouchSet        touch;
    KfInsertPlan      *abandoned;   /* reserved-but-rejected NEW-insert slots */
    size_t             nabandoned;
    /* prepare_window ran OK: the caller's bulk_hook_ctx now owns staged
       state that exactly one of {commit_done, abort_window, release_window}
       must release at this window's exit (see bulk_commit_one_kf_window). */
    int                hooks_staged;
} BulkWindowPlan;

static void bulk_window_plan_destroy(BulkWindowPlan *plan) {
    if (!plan) return;
    free(plan->entries); free(plan->active); free(plan->kf_slots);
    free(plan->touch.v); free(plan->abandoned);
    memset(plan, 0, sizeof(*plan));
}

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
/* Free a bucket set produced by bulk_phase2_bucket_by_stream: the outer
 * array plus every per-stream index array it owns. */
static void bulk_free_stream_buckets(SlotcaskDb *db, int **stream_idx) {
    if (!stream_idx || !db) return;
    for (int s = 0; s < db->num_streams; s++) free(stream_idx[s]);
}

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

/* Task 4's P wave — reserve + write staged via seg_record_emit_pending
   (flag left at 0). The window coordinator's A step activates these once
   the marker is durable. */
static void bulk_phase3_stage_pending(SlotcaskDb *db,
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
            seg_record_emit_pending(h.map + st[i].target_off, (int)rec_size,
                                    st[i].hash, r->key, r->klen,
                                    r->value, r->vlen);
            SegCacheEntry *e = &g_segcache[h.slot];
            durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
            segcache_release(&h);
        }
    }
}

/* Single-record staging for OLD-derived Task 4 records that skipped the
   parallel P wave (value_compute/require_existing/pre_commit_needs_old
   need OLD before the payload can be computed). Reserve + emit-pending +
   durable sync inline, mirroring slotcask_insert's allocation shape. */
static int bulk_stage_single_pending(SlotcaskDb *db, uint8_t stream_id,
                                     const uint8_t hash[16],
                                     const void *key, size_t klen,
                                     const void *value, size_t vlen,
                                     uint16_t *out_fid, uint32_t *out_off,
                                     uint32_t *out_cap) {
    SlotcaskStream *pool = &db->streams[stream_id];
    SlotcaskFreeSlot fs;
    size_t rec_size = slotcask_record_size_varlen(klen, vlen);
    uint16_t fid; uint32_t off; int got_pool;

    if (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                              db->slot_size, &fs) == 0) {
        fid = fs.file_id; off = fs.offset; got_pool = 1;
        if (fs.capacity > (uint32_t)rec_size)
            pool_split_leftover(db, stream_id, fid, off + (uint32_t)rec_size,
                                fs.capacity - (uint32_t)rec_size);
    } else {
        uint32_t f32, o32;
        if (append_reserve_single_varlen(db, pool, rec_size, &f32, &o32) != 0)
            return -1;
        fid = (uint16_t)f32; off = o32; got_pool = 0;
    }

    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, fid);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 1, 0, 1) != 0) {
        if (got_pool)
            pool_push_free_cap(pool, fid, off, (uint32_t)rec_size, db->slot_size);
        return -1;
    }
    seg_record_emit_pending(h.map + off, (int)rec_size, hash, key, klen,
                            value, vlen);
    int rc = durability_msync_range(h.map, off, rec_size);
    if (rc == 0 && fdatasync(h.fd) != 0) rc = -1;
    segcache_release(&h);
    if (rc != 0) {
        if (got_pool)
            pool_push_free_cap(pool, fid, off, (uint32_t)rec_size, db->slot_size);
        return -1;
    }
    *out_fid = fid; *out_off = off; *out_cap = (uint32_t)rec_size;
    return 0;
}

/* ============================================================ Task 4:
 * window coordinator step functions (P/A/I/K/T/C + replay), built on the
 * Phase 2/3 helpers above and the marker/kf primitives earlier in this
 * file. See docs/plans/2026-08-21-main-durability-and-window.md Task 4. */

static int kf_shard_acquire(SlotcaskKfHandle *kh, const SlotcaskDb *db,
                            int kf_shard_id, int writer);

/* Grouped OLD reads under the writer — same file-run discipline as the
   Phase 1b grouping above (sort by (sid,fid,off), one segcache rdlock
   per file). */
static int bulk_read_old_values(SlotcaskDb *db, SlotcaskBulkRec *recs,
                                SlotcaskBulkState *st,
                                int *idx, int nidx) {
    SLOTCASK_SORT_IDX_BY_SEG_LOC(idx, nidx, st);
    int k = 0;
    while (k < nidx) {
        int run_end = k + 1;
        uint8_t sid = st[idx[k]].old_sid;
        uint16_t fid = st[idx[k]].old_fid;
        while (run_end < nidx &&
               st[idx[run_end]].old_sid == sid && st[idx[run_end]].old_fid == fid)
            run_end++;
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, sid, fid);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) {
            for (int j = k; j < run_end; j++) recs[idx[j]].status = -1;
            k = run_end;
            continue;
        }
        for (int j = k; j < run_end; j++) {
            SlotcaskBulkRec *r = &recs[idx[j]];
            SlotcaskBulkState *s = &st[idx[j]];
            const uint8_t *rec = h.map + s->old_off;
            if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) != 1) { r->status = -1; continue; }
            uint16_t klen = seg_rec_klen(rec);
            uint32_t vlen = seg_rec_vlen(rec);
            if (klen != r->klen || memcmp(rec + 24, r->key, r->klen) != 0) { r->status = -1; continue; }
            uint8_t *buf = malloc(vlen ? vlen : 1);
            if (!buf) { r->status = -1; continue; }
            if (vlen) memcpy(buf, rec + 24 + r->klen, vlen);
            s->old_buf = buf;
            s->old_vlen = vlen;
            /* Echo back onto the public rec so a caller-supplied
               apply_window (query_bulk.c) can see OLD too -- it reads
               r->old_value directly and has no access to this file's
               internal st[] array, unlike value_compute/pre_commit/marker
               composition which already fall back to s->old_buf. Freed by
               the s->old_buf cleanup loop at the end of
               bulk_commit_one_kf_window, after every consumer (including
               apply_window) has run. */
            r->old_value = buf;
            r->old_vlen = vlen;
        }
        segcache_release(&h);
        k = run_end;
    }
    return 0;
}

static int bulk_plan_window_locked(BulkMutationTxn *txn,
                                   BulkMutationShard *shard,
                                   size_t begin, size_t end,
                                   SlotcaskKfHandle *kh,
                                   BulkWindowPlan *plan) {
    SlotcaskBulkRec *recs = shard->recs;
    SlotcaskBulkState *st = shard->st;
    const SlotcaskBulkOpts *uo = shard->kind == BULK_MUTATION_UPSERT
                                 ? txn->upsert_opts : NULL;
    size_t span = end - begin;
    int *old_idx = NULL;
    KfInsertPlan *reserved_plans = NULL;
    size_t nreserved = 0;

    SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_P);
    durability_test_pause(txn->db->data_dir, "win-P");

    plan->batch_id = (uint32_t)begin;
    plan->kf_shard_id = shard->kf_shard_id;
    plan->shard = shard;
    plan->entries = calloc(span, sizeof(*plan->entries));
    plan->active = calloc(span, sizeof(*plan->active));
    plan->abandoned = calloc(span, sizeof(*plan->abandoned));
    old_idx = malloc(span * sizeof(int));
    reserved_plans = calloc(span, sizeof(*reserved_plans));
    if (!plan->entries || !plan->active || !plan->abandoned || !old_idx ||
        !reserved_plans) goto oom;
    int nold = 0;

    for (size_t i = begin; i < end; i++) {
        SlotcaskBulkRec *r = &recs[i];
        SlotcaskBulkState *s = &st[i];
        uint8_t old_flag = 0;
        int found;

        r->status = 0;
        r->was_update = 0;
        if (r->klen > UINT16_MAX || r->vlen > UINT32_MAX ||
            (size_t)24 + r->klen + r->vlen > (size_t)txn->db->slot_size) {
            r->status = -1;
            continue;               /* staged payload (if any) stays flag=0 */
        }
        compute_hash(r->key, r->klen, s->hash);
        found = (kf_lookup_with_slot(kh, s->hash, r->key, r->klen,
                                     txn->db->data_dir, &old_flag,
                                     &s->old_sid, &s->old_fid,
                                     &s->old_off, &s->old_kf_slot) == 0);
        s->old_found = (uint8_t)(found ? 1 : 0);
        s->target_stream = (uint8_t)((unsigned)s->hash[15] %
                                     (unsigned)txn->db->num_streams);

        if (shard->kind == BULK_MUTATION_DELETE) {
            if (!found) { r->status = -2; continue; }
            /* prepare_window's and apply_window's documented contracts
               (slotcask.h) both require OLD -- CAS re-verification plus
               the forward index diff (old=OLD, new=NULL) -- exactly as
               much as pre_commit does, so their presence must gate the
               batched old-value fetch too, not just pre_commit_needs_old
               (which the has_indexed_fields branch leaves unset since it
               uses prepare_window/apply_window instead of pre_commit).
               Without this, every indexed delete (CAS or plain) always
               saw old_value == NULL: CAS-deletes rejected every record,
               and plain indexed deletes silently skipped index removal. */
            if (txn->delete_opts &&
                (txn->delete_opts->pre_commit_needs_old ||
                 txn->delete_opts->prepare_window ||
                 txn->delete_opts->apply_window) &&
                r->old_value == NULL) {
                old_idx[nold++] = (int)i;
            }
            s->needs_write = 1;
            continue;
        }

        if (found && uo && (uo->if_not_exists || r->if_not_exists)) {
            r->status = -2; r->was_update = 1;
            continue;               /* CAS reject: retire staged payload */
        }
        if (!found && uo && uo->require_existing) { r->status = -2; continue; }

        if (uo && uo->value_compute && !found) {
            /* No OLD to fetch for a fresh insert: call straight away with
               old=NULL (mirrors upsert_slow_path's check_fn(NULL, ...) for
               the not-found case). */
            if (uo->value_compute(NULL, r) != 0) { r->status = -2; continue; }
        } else if (uo && (uo->value_compute || uo->pre_commit_needs_old)) {
            if (found && r->old_value == NULL) old_idx[nold++] = (int)i;
        }
        r->was_update = found ? 1 : 0;
        s->needs_write = 1;
    }

    if (nold > 0) {
        bulk_read_old_values(txn->db, recs, st, old_idx, nold);
        for (int j = 0; j < nold; j++) {
            int i = old_idx[j];
            if (recs[i].status != 0) continue;
            const void *ov = recs[i].old_value ? recs[i].old_value : st[i].old_buf;
            size_t ol = recs[i].old_value ? recs[i].old_vlen : st[i].old_vlen;
            SlotcaskOldRecord old_rec = { (const uint8_t *)ov, ol };
            if (uo && uo->value_compute &&
                uo->value_compute(st[i].old_found ? &old_rec : NULL,
                                  &recs[i]) != 0) {
                recs[i].status = -2;
                st[i].needs_write = 0;
            }
        }
    }

    if (shard->kind == BULK_MUTATION_UPSERT) {
        for (size_t i = begin; i < end; i++) {
            SlotcaskBulkRec *r = &recs[i];
            SlotcaskBulkState *s = &st[i];
            if (r->status != 0 || !s->needs_write) continue;

            if (!s->old_found) {
                if (kf_plan_window_insert_slot(txn->db, kh, s->hash,
                                               r->key, r->klen,
                                               txn->db->data_dir,
                                               reserved_plans, nreserved,
                                               &s->kf_plan) != 0) {
                    r->status = -1;
                    continue;
                }
                s->has_plan = 1;
                reserved_plans[nreserved++] = s->kf_plan;
            }
            /* OLD-derived records never went through the P wave: stage NEW
               synchronously here so M still covers a durable payload. */
            if (!s->staged_in_wave) {
                uint32_t cap;
                if (bulk_stage_single_pending(txn->db, s->target_stream,
                                              s->hash, r->key, r->klen,
                                              r->value, r->vlen,
                                              &s->target_fid, &s->target_off,
                                              &cap) != 0) {
                    r->status = -1;
                    continue;
                }
                r->slot_capacity = cap;
            }
        }
    }

    /* Physical kf location + pre_commit: fired under the held kf wrlock,
       after the target slot (existing or freshly planned) is known and the
       NEW payload is staged, before the window marker is published — same
       ordering guarantee slotcask_upsert_with_hooks / bulk_upsert_in_kfshard
       give today. kf_shard/kf_slot are written unconditionally (even on a
       pre_commit rejection) so a caller that already captured them via an
       earlier hook sees a consistent value, matching the struct's "written
       BEFORE pre_commit fires" contract. */
    if (shard->kind == BULK_MUTATION_UPSERT) {
        for (size_t i = begin; i < end; i++) {
            SlotcaskBulkRec *r = &recs[i];
            SlotcaskBulkState *s = &st[i];
            if (r->status != 0 || !s->needs_write) continue;
            r->kf_shard = shard->kf_shard_id;
            r->kf_slot = s->old_found ? (uint32_t)s->old_kf_slot
                                       : (uint32_t)s->kf_plan.target_slot;
            if (uo && uo->pre_commit) {
                const void *ov = r->old_value ? r->old_value : s->old_buf;
                size_t ol = r->old_value ? r->old_vlen : s->old_vlen;
                SlotcaskOldRecord old_rec = { (const uint8_t *)ov, ol };
                if (uo->pre_commit(s->old_found ? &old_rec : NULL, r,
                                   s->old_found) != 0) {
                    r->status = -1;
                    s->needs_write = 0;
                }
            }
        }
    } else if (shard->kind == BULK_MUTATION_DELETE) {
        const SlotcaskBulkDeleteOpts *dopt = txn->delete_opts;
        for (size_t i = begin; i < end; i++) {
            SlotcaskBulkRec *r = &recs[i];
            SlotcaskBulkState *s = &st[i];
            if (r->status != 0 || !s->needs_write) continue;
            r->kf_shard = shard->kf_shard_id;
            r->kf_slot = (uint32_t)s->old_kf_slot;
            if (dopt && dopt->pre_commit) {
                const void *ov = r->old_value ? r->old_value : s->old_buf;
                size_t ol = r->old_value ? r->old_vlen : s->old_vlen;
                SlotcaskOldRecord old_rec = { (const uint8_t *)ov, ol };
                if (dopt->pre_commit(&old_rec, r) != 0) {
                    r->status = -2;
                    s->needs_write = 0;
                }
            }
        }
    }

    if (shard->kind == BULK_MUTATION_UPSERT && uo && uo->prepare_window && uo->apply_window) {
        size_t n = 0;
        for (size_t i = begin; i < end; i++)
            if (recs[i].status == 0 && st[i].needs_write)
                plan->active[n++] = i;
        if (uo->prepare_window(recs, plan->active, n,
                               uo->bulk_hook_ctx) != 0) goto hard_fail;
        plan->hooks_staged = 1;
        /* hook may have set status=-1/-2 (policy); rebuild below */
    } else if (shard->kind == BULK_MUTATION_DELETE && txn->delete_opts &&
              txn->delete_opts->prepare_window) {
        size_t n = 0;
        for (size_t i = begin; i < end; i++)
            if (recs[i].status == 0 && st[i].needs_write)
                plan->active[n++] = i;
        if (txn->delete_opts->prepare_window(recs, plan->active, n,
                               txn->delete_opts->bulk_hook_ctx) != 0) goto hard_fail;
        plan->hooks_staged = 1;
    }

    plan->nactive = 0;
    plan->nabandoned = 0;
    for (size_t i = begin; i < end; i++) {
        SlotcaskBulkState *s = &st[i];
        BatchMarkerEntry *e;
        if (recs[i].status != 0) {
            /* A NEW-insert's kf slot was reserved by kf_plan_window_insert_
               slot above (s->has_plan) but the record never made it into
               plan->active -- rejected by pre_commit or prepare_window
               after the reservation. Nothing else in this window's commit
               writes that slot, so it stays flag==0 and can hard-stop the
               probe chain for another record in the same window whose
               accepted slot lands further along it. Queue it for
               tombstoning in the K phase. */
            if (shard->kind == BULK_MUTATION_UPSERT && s->has_plan)
                plan->abandoned[plan->nabandoned++] = s->kf_plan;
            continue;
        }
        if (!s->needs_write) continue;
        e = &plan->entries[plan->nactive];
        memset(e, 0, sizeof(*e));
        e->slot.magic = KF_BATCH_MARKER_ENTRY_MAGIC;
        e->slot.op = shard->kind == BULK_MUTATION_DELETE
                     ? KF_MARKER_OP_DELETE : KF_MARKER_OP_UPSERT;
        /* Inserts must persist their pre-selected target slot: replay's
           idempotent-insert path (kf_marker_replay_upsert_entry_locked)
           trusts marker->kf_slot verbatim for a bulk marker and refuses to
           re-probe it, to avoid a recovery-time resplit relocating the
           record out from under an already-durable bitmap bit. */
        e->slot.kf_slot = s->old_found ? (uint32_t)s->old_kf_slot
                                        : (uint32_t)s->kf_plan.target_slot;
        e->slot.has_old = s->old_found;
        e->slot.old_stream_id = s->old_sid;
        e->slot.old_file_id = s->old_fid;
        e->slot.old_offset = s->old_off;
        /* A delete never has a NEW payload -- kf_marker_op_valid() requires
           new_stream_id/new_file_id/new_offset to stay exactly 0 for
           op==DELETE. s->target_stream is computed unconditionally above
           (from the key hash, before the delete/upsert split) even though
           delete never stages or consumes a NEW location; copying it here
           regardless of shard->kind produced a marker with a nonzero
           new_stream_id that kfm2_read_batch_marker's op_valid check
           rejects as corrupt on replay -- only reachable once something
           (a crash) leaves the marker on disk long enough to be read back,
           since a normal run always clears it first. */
        if (shard->kind != BULK_MUTATION_DELETE) {
            e->slot.new_stream_id = s->target_stream;
            e->slot.new_file_id = s->target_fid;
            e->slot.new_offset = s->target_off;
        }
        memcpy(e->hash, s->hash, 16);
        e->klen = (uint16_t)recs[i].klen;
        e->new_vlen = shard->kind == BULK_MUTATION_DELETE ? 0 : (uint16_t)recs[i].vlen;
        e->old_vlen = (uint16_t)s->old_vlen;
        plan->active[plan->nactive++] = i;
    }
    free(old_idx);
    free(reserved_plans);
    return 0;

oom:
hard_fail:
    free(old_idx);
    free(reserved_plans);
    return -1;
}

static int bulk_publish_window_marker_locked(BulkMutationTxn *txn,
                                             SlotcaskKfHandle *kh,
                                             BulkWindowPlan *plan) {
    char kf_dir[PATH_MAX], final[64];
    BatchMarkerHeader hdr = { KF_BATCH_MARKER_MAGIC, KF_BATCH_MARKER_VERSION,
                              (uint32_t)plan->nactive, 0 };
    uint8_t *buf = NULL;
    size_t len = 0, cap = 0;
    int rc;

    (void)kh;
    SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_M);
    durability_test_pause(txn->db->data_dir, "win-M");
    durability_test_pause(txn->db->data_dir, "bulk-window-prepared");
    if (plan->nactive == 0) return 0;
    if (buf_append(&buf, &len, &cap, &hdr, sizeof(hdr)) != 0) goto oom;
    for (size_t i = 0; i < plan->nactive; i++) {
        BatchMarkerEntry *e = &plan->entries[i];
        const SlotcaskBulkRec *r = &plan->shard->recs[plan->active[i]];
        const SlotcaskBulkState *s = &plan->shard->st[plan->active[i]];
        size_t at;
        if (buf_append(&buf, &len, &cap, e, sizeof(*e)) != 0) goto oom;
        at = len - sizeof(*e);
        if (e->klen     && buf_append(&buf, &len, &cap, r->key,   e->klen)     != 0) goto oom;
        if (e->old_vlen && buf_append(&buf, &len, &cap, s->old_buf, e->old_vlen) != 0) goto oom;
        if (e->new_vlen && buf_append(&buf, &len, &cap, r->value, e->new_vlen) != 0) goto oom;
        uint32_t sum = XXH32(buf + at, len - at, 0);   /* entry written with
                                                          slot.checksum = 0 */
        memcpy(buf + at + offsetof(BatchMarkerEntry, slot) +
               offsetof(KfMarkerSlot, checksum), &sum, sizeof(sum));
    }
    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", txn->db->data_dir);
    snprintf(final, sizeof(final), "%03x_batch_%u_marker.dat",
             (unsigned)plan->kf_shard_id, plan->batch_id);
    {
        int fail_now = SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_M);
        if (fail_now && !SHARD_TEST_FAIL_POSTLINK) {
            free(buf);
            return -1;
        }
        rc = marker_publish_atomic(kf_dir, final, buf, len);
        free(buf);
        if (fail_now && SHARD_TEST_FAIL_POSTLINK && rc == 0) rc = 1;
    }
    return rc;

oom:
    free(buf);
    return -1;
}

/* store=0: sync-only pass (P barrier and T after tombstone_and_push_back).
   store=1: atomic-store `flag` at each offset first. One msync pass with
   gaps <= 4096 merged and one fdatasync per file. */
static int bulk_seg_apply_and_sync(SlotcaskDb *db, const SegLoc *locs,
                                   size_t n, int store, uint8_t flag) {
    size_t i = 0;
    while (i < n) {
        size_t j = i + 1;
        while (j < n && locs[j].sid == locs[i].sid && locs[j].fid == locs[i].fid)
            j++;
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, locs[i].sid, locs[i].fid);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;
        for (size_t k = i; k < j; k++) {
            if (store)
                __atomic_store_n(&h.map[locs[k].off + 18], flag,
                                 __ATOMIC_RELEASE);
        }
        size_t lo = locs[i].off, hi = locs[i].off + 1;
        for (size_t k = i + 1; k < j; k++) {
            if (locs[k].off <= hi + 4096) { hi = locs[k].off + 1; continue; }
            if (durability_msync_range(h.map, lo, hi - lo) != 0) {
                segcache_release(&h); return -1;
            }
            lo = locs[k].off; hi = lo + 1;
        }
        if (durability_msync_range(h.map, lo, hi - lo) != 0 ||
            fdatasync(h.fd) != 0) {
            segcache_release(&h);
            return -1;
        }
        segcache_release(&h);
        i = j;
    }
    return 0;
}

static int bulk_activate_new_payloads_locked(BulkMutationTxn *txn,
                                             BulkWindowPlan *plan) {
    SegLoc *locs;
    size_t n = 0;
    int rc;

    SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_A);
    durability_test_pause(txn->db->data_dir, "win-A");

    if (plan->shard->kind == BULK_MUTATION_DELETE) return 0;
    locs = calloc(plan->nactive, sizeof(*locs));
    if (!locs) return -1;
    for (size_t i = 0; i < plan->nactive; i++) {
        const BatchMarkerEntry *e = &plan->entries[i];
        if (e->slot.op != KF_MARKER_OP_UPSERT) continue;
        locs[n].sid = e->slot.new_stream_id;
        locs[n].fid = e->slot.new_file_id;
        locs[n].off = e->slot.new_offset;
        n++;
    }
    qsort(locs, n, sizeof(*locs), segloc_cmp);
    rc = bulk_seg_apply_and_sync(txn->db, locs, n, 1, 1);
    free(locs);
    if (rc == 0 && n > 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_A)) rc = -1;
    return rc;
}

static int idx_touch_cmp(const void *a, const void *b) {
    const IdxTouch *x = a, *y = b;
    int c = strcmp(x->field, y->field);
    if (c) return c;
    return x->idx_shard < y->idx_shard ? -1 : x->idx_shard > y->idx_shard;
}

static int bulk_apply_and_sync_indexes_locked(BulkMutationTxn *txn,
                                              BulkWindowPlan *plan) {
    BulkMutationShard *shard = plan->shard;
    char eff_root[PATH_MAX], object[256];
    int rc = 0;

    SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_I);
    durability_test_pause(txn->db->data_dir, "win-I");

    if (shard->kind == BULK_MUTATION_DELETE) {
        if (!txn->delete_opts || !txn->delete_opts->apply_window) return 0;
        tls_idx_touch = &plan->touch;
        rc = txn->delete_opts->apply_window(shard->recs, plan->active,
                                            plan->nactive,
                                            txn->delete_opts->bulk_hook_ctx);
        tls_idx_touch = NULL;
    } else {
        if (!txn->upsert_opts || !txn->upsert_opts->apply_window) return 0;
        tls_idx_touch = &plan->touch;
        rc = txn->upsert_opts->apply_window(shard->recs, plan->active,
                                            plan->nactive,
                                            txn->upsert_opts->bulk_hook_ctx);
        tls_idx_touch = NULL;
    }
    if (rc != 0) return -1;

    /* one durable sync per touched (field, idx shard): dedupe, then flush
       via the existing per-record flush seam with one representative hash */
    if (plan->touch.n > 0)     /* delete windows carry no touches: v is NULL */
        qsort(plan->touch.v, plan->touch.n, sizeof(IdxTouch), idx_touch_cmp);
    size_t w = 0;
    for (size_t i = 0; i < plan->touch.n; i++)
        if (!w || idx_touch_cmp(&plan->touch.v[w - 1], &plan->touch.v[i]) != 0)
            plan->touch.v[w++] = plan->touch.v[i];
    plan->touch.n = w;

    split_data_dir(txn->db->data_dir, eff_root, sizeof(eff_root),
                   object, sizeof(object));
    for (size_t i = 0; i < plan->touch.n; i++) {
        const IdxTouch *t = &plan->touch.v[i];
        const char *field = t->field;
        if (index_sync_record_fields(eff_root, object, txn->db->num_shards,
                                     t->hash16, &field,
                                     (const enum IndexType *)&t->type,
                                     1) != 0)
            return -1;
    }
    if (plan->touch.n > 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_I)) return -1;
    return 0;
}

static int size_cmp(const void *a, const void *b) {
    size_t x = *(const size_t *)a, y = *(const size_t *)b;
    return x < y ? -1 : x > y ? 1 : 0;
}

static int bulk_apply_and_sync_kf_locked(BulkMutationTxn *txn,
                                         SlotcaskKfHandle *kh,
                                         BulkWindowPlan *plan) {
    BulkMutationShard *shard = plan->shard;
    size_t used_delta = 0;

    SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_K);
    durability_test_pause(txn->db->data_dir, "win-K");
    durability_test_pause(txn->db->data_dir, "bulk-window-applied");

    /* K is re-runnable by contract (forward replay may re-enter this step
     * on the SAME plan after a post-M failure). Reset the slot vector
     * rather than appending onto a prior run's count/pointer — reusing a
     * stale nkf_slots here overflowed the buffer under ASan
     * (test-win-gate-pending-then-success / -replay-no-deadlock /
     *  -window16-two-windows). */
    free(plan->kf_slots);
    plan->kf_slots = NULL;
    plan->nkf_slots = 0;
    plan->kf_slots = calloc(plan->nactive + plan->nabandoned, sizeof(size_t));
    if (!plan->kf_slots) return -1;
    for (size_t i = 0; i < plan->nabandoned; i++) {
        const KfInsertPlan *ap = &plan->abandoned[i];
        kf_write_tombstone_at_slot(kh, ap->target_slot, ap->hash);
        plan->kf_slots[plan->nkf_slots++] = ap->target_slot;
    }
    for (size_t i = 0; i < plan->nactive; i++) {
        SlotcaskBulkRec *r = &shard->recs[plan->active[i]];
        SlotcaskBulkState *s = &shard->st[plan->active[i]];
        BatchMarkerEntry *e = &plan->entries[i];
        size_t out_slot;

        if (e->slot.op == KF_MARKER_OP_DELETE) {
            if (kf_tombstone_entry_locked(kh, e->hash, r->key, r->klen,
                                          txn->db->data_dir) != 0) return -1;
            out_slot = s->old_kf_slot;
            plan->kf_header_changed = 1;
        } else if (s->old_found) {
            if (kf_repoint(kh, s->hash, s->target_stream, s->target_fid,
                           s->target_off, r->key, r->klen,
                           txn->db->data_dir) != 0) return -1;
            out_slot = s->old_kf_slot;
        } else {
            kf_commit_planned_slot(kh, &s->kf_plan, s->target_stream,
                                   s->target_fid, s->target_off,
                                   &used_delta, &out_slot);
            plan->kf_header_changed = 1;
        }
        plan->kf_slots[plan->nkf_slots++] = out_slot;
    }

    if (plan->nkf_slots > 0)   /* zero-record windows leave kf_slots NULL */
        qsort(plan->kf_slots, plan->nkf_slots, sizeof(size_t), size_cmp);
    size_t w = 0;
    for (size_t i = 0; i < plan->nkf_slots; i++)
        if (!w || plan->kf_slots[w - 1] != plan->kf_slots[i])
            plan->kf_slots[w++] = plan->kf_slots[i];
    plan->nkf_slots = w;
    {
        int rc = kfcache_sync_slots_locked(kh, plan->kf_slots, plan->nkf_slots,
                                           plan->kf_header_changed);
        if (rc == 0 && plan->nactive > 0 &&
            SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_K)) rc = -1;
        return rc;
    }
}

static int bulk_tombstone_old_payloads_locked(BulkMutationTxn *txn,
                                              BulkWindowPlan *plan) {
    SegLoc *locs = calloc(plan->nactive, sizeof(*locs));
    size_t n = 0;
    int rc;

    SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_T);
    durability_test_pause(txn->db->data_dir, "win-T");

    if (!locs) return -1;
    for (size_t i = 0; i < plan->nactive; i++) {
        const BatchMarkerEntry *e = &plan->entries[i];
        if (!e->slot.has_old) continue;      /* fresh insert: nothing dead */
        locs[n].sid = e->slot.old_stream_id;
        locs[n].fid = e->slot.old_file_id;
        locs[n].off = e->slot.old_offset;
        n++;
    }
    for (size_t i = 0; i < n; i++) {
        char path[PATH_MAX];
        SlotcaskSegHandle h;
        int dead;
        seg_path_for(path, txn->db->data_dir, locs[i].sid, locs[i].fid);
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) { free(locs); return -1; }
        dead = __atomic_load_n(&h.map[locs[i].off + 18], __ATOMIC_ACQUIRE) == 2;
        segcache_release(&h);
        if (dead) continue;                  /* idempotent re-run */
        if (slotcask_tombstone_mark(txn->db, locs[i].sid,
                                    locs[i].fid,
                                    locs[i].off) != 0) {
            free(locs);
            return -1;
        }
    }
    qsort(locs, n, sizeof(*locs), segloc_cmp);
    rc = bulk_seg_apply_and_sync(txn->db, locs, n, 0, 0);
    free(locs);
    if (rc == 0 && n > 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_T)) rc = -1;
    return rc;
}

/* Returns OLD payload capacity to the free pool. Must only be called after
   the window's marker is durably cleared (bulk_clear_window_marker_locked
   succeeded) -- reclaiming any earlier (e.g. right after T, alongside the
   flag=2 write) would let a *different* transaction's P-phase pop and
   overwrite that slot while this window's marker is still retained, so a
   later Gap-C replay reading the slot for "was OLD already tombstoned?"
   would see garbage/a foreign record instead of flag==2. Deferring the
   push (not the flag=2 write itself, which T still performs synchronously)
   costs nothing on the fault-free path -- T and C already run back-to-back
   with no other work between them -- and only delays reclaiming a
   transaction's OLD capacity while its marker is genuinely unresolved,
   which is rare and self-limiting. Re-reads klen/vlen from the segment
   header rather than caching them, since tombstoning only touches the
   flag byte at offset+18. */
static void bulk_reclaim_old_payloads_locked(BulkMutationTxn *txn,
                                             BulkWindowPlan *plan) {
    for (size_t i = 0; i < plan->nactive; i++) {
        const BatchMarkerEntry *e = &plan->entries[i];
        if (!e->slot.has_old) continue;
        char path[PATH_MAX];
        SlotcaskSegHandle h;
        seg_path_for(path, txn->db->data_dir, e->slot.old_stream_id,
                    e->slot.old_file_id);
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) continue;
        uint16_t klen = seg_rec_klen(h.map + e->slot.old_offset);
        uint32_t vlen = seg_rec_vlen(h.map + e->slot.old_offset);
        uint32_t cap = (uint32_t)slotcask_record_size_varlen(klen, vlen);
        segcache_release(&h);
        pool_push_free_cap(&txn->db->streams[e->slot.old_stream_id],
                           e->slot.old_file_id, e->slot.old_offset, cap,
                           txn->db->slot_size);
    }
}

static int bulk_clear_window_marker_locked(BulkMutationTxn *txn,
                                           BulkWindowPlan *plan) {
    char kf_dir[PATH_MAX], path[PATH_MAX];
    int rc;

    SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_C);
    durability_test_pause(txn->db->data_dir, "win-C");

    if (plan->nactive == 0) return 0;
    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", txn->db->data_dir);
    snprintf(path, sizeof(path), "%s/%03x_batch_%u_marker.dat",
             kf_dir, (unsigned)plan->kf_shard_id, plan->batch_id);
    if (unlink(path) != 0) return -1;
    rc = fsync_dir(kf_dir);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_C)) rc = -1;
    return rc;
}

static int bulk_replay_window_forward_locked(BulkMutationTxn *txn,
                                             SlotcaskKfHandle *kh,
                                             BulkWindowPlan *plan) {
    /* Forward-only idempotent convergence to C; every step verifies
       identity before acting, so re-running a partial failure is safe. */
    if (bulk_activate_new_payloads_locked(txn, plan) != 0) return -1;
    if (bulk_apply_and_sync_indexes_locked(txn, plan) != 0) return -1;
    if (bulk_apply_and_sync_kf_locked(txn, kh, plan) != 0) return -1;
    if (bulk_tombstone_old_payloads_locked(txn, plan) != 0) return -1;
    if (bulk_clear_window_marker_locked(txn, plan) != 0) return -1;
    bulk_reclaim_old_payloads_locked(txn, plan);
    return 0;
}

/* Surfaces the same "your write's durability outcome is unresolved" signal
   the legacy bulk paths set on every EINPROGRESS-shaped exit; the window
   coordinator itself only ever produced rc=-2/errno=EINPROGRESS without
   flipping this caller-visible flag. */
static void bulk_mark_durability_degraded(BulkMutationShard *shard) {
    int *out = shard->kind == BULK_MUTATION_DELETE
             ? (shard->delete_opts ? shard->delete_opts->out_durability_degraded : NULL)
             : (shard->upsert_opts ? shard->upsert_opts->out_durability_degraded : NULL);
    if (out) *out = 1;
}

static int bulk_commit_one_kf_window(BulkMutationTxn *txn,
                                     BulkMutationShard *shard,
                                     size_t begin, size_t end) {
    SlotcaskKfHandle kh;
    BulkWindowPlan plan = {0};
    int prc, rc = -1;
    int published = 0;   /* a marker file was actually created (M reached
                            with nactive > 0): durable evidence may exist */

    if (kf_shard_acquire(&kh, txn->db, shard->kf_shard_id, 1) != 0)
        return -1;
    if (kf_shard_marker_gate(shard->kf_shard_id, &kh, txn->db->data_dir) != 0) {
        rc = -2;                 /* retained marker; replay did not converge */
        errno = EINPROGRESS;
        bulk_mark_durability_degraded(shard);
        goto out;
    }
    if (bulk_plan_window_locked(txn, shard, begin, end, &kh, &plan) != 0)
        goto out;
    prc = bulk_publish_window_marker_locked(txn, &kh, &plan);
    /* publish returns 0 WITHOUT creating any marker when the window
       planned zero active records (all records policy-rejected) — that is
       not a commit point. Only nactive > 0 with prc >= 0 means durable
       marker evidence exists (prc > 0: created but durability
       unconfirmed). */
    if (prc >= 0 && plan.nactive > 0) published = 1;
    if (prc < 0) goto out;        /* pre-M: nothing was published */
    if (prc > 0) goto replay;     /* published, durability unconfirmed */
    if (bulk_activate_new_payloads_locked(txn, &plan) != 0) goto replay;
    if (bulk_apply_and_sync_indexes_locked(txn, &plan) != 0) goto replay;
    if (bulk_apply_and_sync_kf_locked(txn, &kh, &plan) != 0) goto replay;
    if (bulk_tombstone_old_payloads_locked(txn, &plan) != 0) goto replay;
    if (bulk_clear_window_marker_locked(txn, &plan) != 0) goto replay;
    bulk_reclaim_old_payloads_locked(txn, &plan);
    durability_test_pause(txn->db->data_dir, "bulk-window-cleared");
    rc = 0;
    goto out;

replay:
    if (bulk_replay_window_forward_locked(txn, &kh, &plan) == 0)
        rc = 0;
    if (rc != 0) {
        errno = EINPROGRESS;
        bulk_mark_durability_degraded(shard);
        rc = -2;  /* outcome pending */
    }
out:
    /* Exactly one release route per staged window. No marker evidence →
       abort_window (publish failed pre-M, or the window planned zero
       active records and M was skipped: nothing was ever committed, even
       when the batch rc is 0; v2's prepare already self-released the
       all-rejected case, so this is an idempotent no-op there). Marker
       evidence + rc==0 → commit_done (durable; no further hook re-entry).
       Marker evidence + failure → release_window (the marker owns
       recovery; gate and startup replay re-derive from disk and never
       re-enter these hooks). Pointers stay const: BulkMutationShard
       members are const-qualified. All routes NULL-guarded. */
    if (plan.hooks_staged) {
        const SlotcaskBulkOpts *uo = shard->upsert_opts;
        const SlotcaskBulkDeleteOpts *dopt = shard->delete_opts;
        if (!published) {
            if (uo && uo->abort_window)
                uo->abort_window(uo->bulk_hook_ctx);
            else if (dopt && dopt->abort_window)
                dopt->abort_window(dopt->bulk_hook_ctx);
        } else if (rc == 0) {
            if (uo && uo->commit_done)
                uo->commit_done(uo->bulk_hook_ctx);
        } else {
            if (uo && uo->release_window)
                uo->release_window(uo->bulk_hook_ctx);
        }
    }
    bulk_window_plan_destroy(&plan);
    kfcache_release(&kh);
    /* Phase-1b OLD-read scratch (bulk_read_old_values) is window-private:
       every consumer (value_compute, pre_commit, marker composition) has
       already run by this point, success or failure. */
    for (size_t i = begin; i < end; i++) {
        free(shard->st[i].old_buf);
        shard->st[i].old_buf = NULL;
    }
    return rc;
}

typedef struct { BulkMutationTxn *txn; size_t shard_idx; } BulkStageWork;

static void *bulk_stage_one_shard(void *raw) {
    BulkStageWork *w = raw;
    BulkMutationTxn *txn = w->txn;
    BulkMutationShard *shard = &txn->shards[w->shard_idx];
    SlotcaskBulkRec *recs = shard->recs;
    SlotcaskBulkState *st = shard->st;
    int *stream_counts = NULL;
    int **stream_idx = NULL;

    SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_P);
    durability_test_pause(txn->db->data_dir, "win-P");

    if (shard->kind == BULK_MUTATION_DELETE) return NULL;
    /* value_rewrites_payload means the real NEW bytes are only known after
       value_compute runs against OLD, which happens in the M phase (the
       kf lookup hasn't happened yet here in the P wave). Staging here
       would durably write whatever placeholder rec->value the caller
       passed up front (partial-update callers pass NULL/0, relying
       entirely on value_compute to fill it in) and then the M-phase's
       `!s->staged_in_wave` fallback would skip re-staging because
       staged_in_wave was already (wrongly) set here, silently committing
       the placeholder instead of the real NEW value. Skip the P wave
       entirely for these records — bulk_commit_one_kf_window already
       stages the correct NEW value synchronously once value_compute has
       run, via that same fallback. Plain check-only callers (value_compute
       non-NULL but value_rewrites_payload == 0) keep the normal P-wave
       staging of their already-final payload. */
    if (shard->upsert_opts && shard->upsert_opts->value_rewrites_payload)
        return NULL;
    for (size_t i = 0; i < shard->nrecs; i++) {
        SlotcaskBulkRec *r = &recs[i];
        SlotcaskBulkState *s = &st[i];
        r->status = 0;
        s->needs_write = 0;
        if (r->klen > UINT16_MAX || r->vlen > UINT32_MAX ||
            (size_t)24 + r->klen + r->vlen > (size_t)txn->db->slot_size) {
            r->status = -1;
            continue;
        }
        compute_hash(r->key, r->klen, s->hash);
        s->target_stream = (uint8_t)((unsigned)s->hash[15] %
                                     (unsigned)txn->db->num_streams);
        s->needs_write = 1;
        s->staged_in_wave = 0;
    }
    if (bulk_phase2_bucket_by_stream(txn->db, st, shard->nrecs,
                                     &stream_counts, &stream_idx) != 0)
        goto fail;
    bulk_phase3_stage_pending(txn->db, recs, st, stream_counts, stream_idx);
    free(stream_counts);
    bulk_free_stream_buckets(txn->db, stream_idx);
    free(stream_idx);
    stream_counts = NULL; stream_idx = NULL;

    /* P barrier: every surviving flag=0 payload durable — one msync pass +
       one fdatasync per touched file. Records phase3 failed carry
       status=-1 and are excluded. */
    {
        SegLoc *locs = calloc(shard->nrecs, sizeof(*locs));
        size_t n = 0;
        if (!locs) goto fail;
        for (size_t i = 0; i < shard->nrecs; i++) {
            if (recs[i].status != 0 || !st[i].needs_write) continue;
            locs[n].sid = st[i].target_stream;
            locs[n].fid = st[i].target_fid;
            locs[n].off = st[i].target_off;
            n++;
            st[i].staged_in_wave = 1;
        }
        qsort(locs, n, sizeof(*locs), segloc_cmp);
        if (n > 0 && bulk_seg_apply_and_sync(txn->db, locs, n, 0, 0) != 0) {
            free(locs);
            goto fail;
        }
        free(locs);
        if (n > 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_P)) goto fail;
    }
    return NULL;

fail:
    free(stream_counts);
    bulk_free_stream_buckets(txn->db, stream_idx);
    free(stream_idx);
    shard->rc = -1;
    atomic_store_explicit(&txn->cancelled, 1, memory_order_release);
    return NULL;
}

static int bulk_stage_payload_wave(BulkMutationTxn *txn) {
    BulkStageWork *works;
    int any_upsert = 0;

    for (size_t s = 0; s < txn->nshards; s++) {
        BulkMutationShard *shard = &txn->shards[s];
        if (shard->kind != BULK_MUTATION_DELETE) any_upsert = 1;
        shard->st = calloc(shard->nrecs, sizeof(*shard->st));
        if (!shard->st) return -1;
    }
    if (!any_upsert) return 0;           /* deletes have no P phase */
    works = calloc(txn->nshards, sizeof(*works));
    if (!works) return -1;
    for (size_t s = 0; s < txn->nshards; s++) {
        works[s].txn = txn;
        works[s].shard_idx = s;
    }
    parallel_for_io(bulk_stage_one_shard, works, (int)txn->nshards,
                    sizeof(*works));
    free(works);
    return atomic_load_explicit(&txn->cancelled,
                                memory_order_acquire) ? -1 : 0;
}

typedef struct { BulkMutationTxn *txn; BulkMutationShard *shard; }
    BulkShardCommitWork;

static void *bulk_commit_one_shard(void *raw) {
    BulkShardCommitWork *w = raw;

    while (w->shard->cursor < w->shard->nrecs &&
           !atomic_load_explicit(&w->txn->cancelled, memory_order_acquire)) {
        size_t begin = w->shard->cursor;
        size_t end = begin + w->txn->window_cap;
        if (end > w->shard->nrecs) end = w->shard->nrecs;
        int rc = bulk_commit_one_kf_window(w->txn, w->shard, begin, end);
        if (rc != 0) {
            atomic_store_explicit(&w->txn->cancelled, 1,
                                  memory_order_release);
            w->shard->rc = rc;
            return NULL;
        }
        w->shard->cursor = end;
    }
    w->shard->rc = 0;
    return NULL;
}

static int bulk_commit_kf_windows_wave(BulkMutationTxn *txn) {
    BulkShardCommitWork *works = calloc(txn->nshards, sizeof(*works));
    if (!works) return -1;
    for (size_t s = 0; s < txn->nshards; s++) {
        works[s].txn = txn;
        works[s].shard = &txn->shards[s];
    }
    parallel_for_io(bulk_commit_one_shard, works, (int)txn->nshards,
                    sizeof(*works));
    free(works);
    return 0;
}

static int bulk_finish_status(BulkMutationTxn *txn) {
    int pending = 0, failed = 0;

    for (size_t s = 0; s < txn->nshards; s++) {
        if (txn->shards[s].rc == 0) continue;
        failed = 1;
        if (txn->shards[s].rc == -2) pending = 1;
    }
    if (!failed) return 0;
    if (pending) errno = EINPROGRESS;
    return -1;
}

/* bulk_stage_payload_wave() callocs shard->st for every shard up front
   (including delete-only shards, which never touch it); nothing else in
   the coordinator owns that array, so every exit path must free it here. */
static void bulk_mutation_txn_free_state(BulkMutationTxn *txn) {
    for (size_t s = 0; s < txn->nshards; s++) {
        free(txn->shards[s].st);
        txn->shards[s].st = NULL;
    }
}

static int slotcask_bulk_mutation_transaction(BulkMutationTxn *txn) {
    int rc;
    if (bulk_stage_payload_wave(txn) != 0) {
        bulk_mutation_txn_free_state(txn);
        return -1;
    }
    if (bulk_commit_kf_windows_wave(txn) != 0) {
        bulk_mutation_txn_free_state(txn);
        return -1;
    }
    rc = bulk_finish_status(txn);
    bulk_mutation_txn_free_state(txn);
    return rc;
}

/* ============================================================ Task 4:
 * SlotcaskUpsertOpts -> SlotcaskBulkOpts adapter.
 *
 * slotcask_upsert_with_hooks is the primary indexed single-record write
 * path; the plan (Task 4) names it as a thin adapter over the bulk window
 * coordinator. Its opts (SlotcaskUpsertOpts) predate the bulk hook contract
 * and use a richer single-record hook shape (check / new_from_old /
 * pre_commit / prepare_commit / apply_commit / abort_commit) than
 * SlotcaskBulkOpts's array-shaped hooks. This section translates one onto
 * the other for a one-record, one-shard, window_cap=1 transaction — every
 * adapter closure below operates on exactly one active record. External
 * signature, external return-value contract (0/-1/-2), and
 * SlotcaskUpsertResult's fields are preserved; only the internal wiring
 * changes. */

typedef struct {
    const SlotcaskUpsertOpts *uo;
    SlotcaskDb *db;
    uint8_t *callback_buf;      /* new_from_old scratch; freed after the txn */
    /* Set when new_from_old rejects the record (e.g. varchar-overflow,
       malformed-escape) rather than check_fn rejecting on a CAS mismatch.
       Both paths return -1 from value_compute and collapse to
       rec.status == -2 in the generic bulk framework, but callers must
       still tell a hard validation failure (has a message in
       uo->new_from_old_ctx's err_buf) apart from a soft condition_not_met
       — see slotcask_upsert_with_hooks/slotcask_insert_with_hooks. */
    int hard_error;
} UpsertAdapterCtx;

/* Folds check_fn + new_from_old into the bulk value_compute slot. Fires once
   per record (found or not), before segment reservation — same ordering
   upsert_slow_path gave check_fn/new_from_old. */
static int upsert_adapter_value_compute(const SlotcaskOldRecord *old,
                                        SlotcaskBulkRec *rec) {
    UpsertAdapterCtx *actx = (UpsertAdapterCtx *)rec->user_ctx;
    const SlotcaskUpsertOpts *uo = actx->uo;

    if (uo->check && uo->check(old, uo->check_ctx) == 0) return -1;
    if (!uo->new_from_old) return 0;
    if (!old) return -1;               /* new_from_old requires an existing record */

    size_t out_capacity = (size_t)actx->db->slot_size - 24 - rec->klen;
    uint8_t *buf = malloc(out_capacity ? out_capacity : 1);
    if (!buf) return -1;
    size_t out_vlen = 0;
    if (uo->new_from_old(old, buf, out_capacity, &out_vlen,
                         uo->new_from_old_ctx) != 0 ||
        out_vlen > out_capacity) {
        free(buf);
        actx->hard_error = 1;
        return -1;
    }
    SlotcaskTrimFn trim_fn = atomic_load_explicit(&actx->db->trim_fn,
                                                  memory_order_acquire);
    if (trim_fn) out_vlen = trim_fn(buf, out_vlen, actx->db->trim_ctx);
    if ((size_t)24 + rec->klen + out_vlen > (size_t)actx->db->slot_size) {
        free(buf);
        actx->hard_error = 1;
        return -1;
    }
    actx->callback_buf = buf;
    rec->value = buf;
    rec->vlen = out_vlen;
    return 0;
}

static int upsert_adapter_pre_commit(const SlotcaskOldRecord *old,
                                     SlotcaskBulkRec *rec, int is_update) {
    UpsertAdapterCtx *actx = (UpsertAdapterCtx *)rec->user_ctx;
    const SlotcaskUpsertOpts *uo = actx->uo;
    if (!uo->pre_commit) return 0;
    return uo->pre_commit(old, rec->value, rec->vlen, is_update,
                          uo->pre_commit_ctx) != 0 ? -1 : 0;
}

/* prepare_commit/apply_commit fire for both fresh-insert and update-resolved
   records — it is up to the wired hook implementation to branch on
   rec->was_update (mirrored into the ctx by pre_commit) if its durable work
   differs between the two. Gating on was_update here would silently skip
   the post-marker apply half for any caller that DOES implement the update
   branch (e.g. cmd_update_v2's v2_update_apply_commit), leaving its durable
   index writes with no home but the pre-marker pre_commit hook. */
static int upsert_adapter_prepare_window(SlotcaskBulkRec *recs,
                                         const size_t *active, size_t nactive,
                                         void *ctx) {
    if (nactive == 0) return 0;
    SlotcaskBulkRec *rec = &recs[active[0]];
    UpsertAdapterCtx *actx = (UpsertAdapterCtx *)ctx;
    const SlotcaskUpsertOpts *uo = actx->uo;
    if (!uo->prepare_commit) return 0;
    if (uo->prepare_commit(rec->value, rec->vlen, rec->kf_slot,
                           uo->pre_commit_ctx) != 0) {
        /* Self-clean contract (slotcask.h): a failed prepare_window must
           release its staging before returning non-zero. Inner two-phase
           hooks express that release as abort_commit, and the primitive
           frees are idempotent (free+zero), so running it here is safe
           even when prepare_commit already cleaned itself up. The
           coordinator fires no route for this window — hooks_staged is
           only set after a successful prepare. */
        if (uo->abort_commit) uo->abort_commit(uo->pre_commit_ctx);
        return -1;
    }
    return 0;
}

static int upsert_adapter_apply_window(SlotcaskBulkRec *recs,
                                       const size_t *active, size_t nactive,
                                       void *ctx) {
    if (nactive == 0) return 0;
    SlotcaskBulkRec *rec = &recs[active[0]];
    UpsertAdapterCtx *actx = (UpsertAdapterCtx *)ctx;
    const SlotcaskUpsertOpts *uo = actx->uo;
    if (!uo->apply_commit) return 0;
    return uo->apply_commit(rec->value, rec->vlen, rec->kf_slot,
                            uo->pre_commit_ctx) != 0 ? -1 : 0;
}

static void upsert_adapter_abort_window(void *ctx) {
    UpsertAdapterCtx *actx = (UpsertAdapterCtx *)ctx;
    const SlotcaskUpsertOpts *uo = actx->uo;
    if (uo->prepare_commit && uo->abort_commit) uo->abort_commit(uo->pre_commit_ctx);
}

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

    /* Fail loud on a missed two-phase migration: an indexed object with a
       legacy pre_commit alongside only one half of the prepare/apply pair
       (or with prepare/apply set but the other missing) would silently run
       with an incomplete hook set. This applies to require_existing=1
       (update-only) callers too — an update-resolved record's durable
       index write is just as pre-marker if it lives in a bare pre_commit
       instead of being staged there and applied post-marker by
       prepare/apply. */
    if (opts->has_indexed_fields &&
        ((!!opts->prepare_commit != !!opts->apply_commit) ||
         (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    /* kf_shard is a pure hash-routing constant — known before the
       transaction starts, unlike kf_slot which segment reservation only
       resolves mid-transaction. Publish it now so any caller hook that
       reads it via a ctx struct (e.g. pre_commit, which fires before slot
       resolution) sees the real value instead of a stale/zero default. */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;

    UpsertAdapterCtx actx = { .uo = opts, .db = db, .callback_buf = NULL };
    SlotcaskBulkOpts bopts = {0};
    bopts.if_not_exists = opts->if_not_exists;
    bopts.require_existing = opts->require_existing;
    bopts.pre_commit = upsert_adapter_pre_commit;
    /* Single-record call: always fetch OLD when the key is found, so
       check/pre_commit/new_from_old (any of which may dereference it) always
       see it — the batch-amortised skip this flag otherwise enables doesn't
       apply to a window_cap=1 transaction. */
    bopts.pre_commit_needs_old = 1;
    bopts.value_compute = (opts->check || opts->new_from_old)
                          ? upsert_adapter_value_compute : NULL;
    bopts.value_rewrites_payload = opts->new_from_old != NULL;
    bopts.abort_window = upsert_adapter_abort_window;
    bopts.bulk_hook_ctx = &actx;
    bopts.has_indexed_fields = opts->has_indexed_fields;
    bopts.out_durability_degraded = opts->out_durability_degraded;
    /* prepare_window/apply_window are only meaningful together (two-phase);
       the coordinator only runs the window-hook path when both are
       non-NULL. Applies to both fresh-insert and update-resolved records —
       see the doc comment on upsert_adapter_prepare_window/apply_window. */
    if (opts->prepare_commit && opts->apply_commit) {
        bopts.prepare_window = upsert_adapter_prepare_window;
        bopts.apply_window = upsert_adapter_apply_window;
    }

    SlotcaskBulkRec rec = {0};
    rec.key = key; rec.klen = klen; rec.value = value; rec.vlen = vlen;
    rec.user_ctx = &actx;

    SlotcaskBulkState st = {0};
    BulkMutationShard shard = {0};
    shard.kf_shard_id = sid_kf;
    shard.recs = &rec; shard.st = &st; shard.nrecs = 1;
    shard.kind = BULK_MUTATION_UPSERT;
    shard.upsert_opts = &bopts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1; txn.window_cap = 1;
    txn.upsert_opts = &bopts;

    int rc = slotcask_bulk_mutation_transaction(&txn);
    free(actx.callback_buf);

    if (opts->out_kf_shard) *opts->out_kf_shard = rec.kf_shard;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = rec.kf_slot;

    if (rc != 0 || shard.rc != 0) {
        /* Hard failure: kf_acquire, staging I/O, or a post-marker window
           that didn't converge synchronously (coordinator already set
           errno=EINPROGRESS for that case). */
        return -1;
    }
    if (rec.status == -2 && !actx.hard_error) {
        if (result) {
            result->was_update = rec.was_update;
            result->condition_not_met = 1;
            void *cv = NULL; size_t cvl = 0;
            if (slotcask_get(db, key, klen, &cv, &cvl) == 0) {
                result->current_value = cv;
                result->current_vlen = cvl;
            }
        }
        return -2;
    }
    if (rec.status != 0) return -1;

    if (result) {
        result->was_update = rec.was_update;
        result->condition_not_met = 0;
    }
    return 0;
}

/* INSERT-only with hooks. See slotcask.h for semantics. Routes through the
   same bulk-mutation coordinator as slotcask_upsert_with_hooks, forcing
   if_not_exists so an existing key always rejects instead of overwriting. */
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

    SlotcaskTrimFn trim_fn = atomic_load_explicit(&db->trim_fn, memory_order_acquire);
    if (trim_fn)
        vlen = trim_fn(value, vlen, db->trim_ctx);
    if ((size_t)24 + klen + vlen > (size_t)db->slot_size) return -1;

    SlotcaskUpsertOpts blank = {0};
    if (!opts) opts = &blank;
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;

    /* require_existing is incompatible with INSERT-only semantics; the
       caller should route through slotcask_upsert_with_hooks for that. */
    if (opts->require_existing) return -1;

    if (opts->has_indexed_fields &&
        ((!!opts->prepare_commit != !!opts->apply_commit) ||
         (opts->pre_commit && (!opts->prepare_commit || !opts->apply_commit)))) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    /* See slotcask_upsert_with_hooks's identical comment: kf_shard is known
       before the transaction starts, so publish it now for hooks (e.g.
       pre_commit) that fire before slot resolution. */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;

    UpsertAdapterCtx actx = { .uo = opts, .db = db, .callback_buf = NULL };
    SlotcaskBulkOpts bopts = {0};
    /* Insert-only: always reject an existing key regardless of what the
       caller passed — the one contract difference from the upsert path. */
    bopts.if_not_exists = 1;
    bopts.pre_commit = upsert_adapter_pre_commit;
    bopts.pre_commit_needs_old = 1;
    bopts.value_compute = (opts->check || opts->new_from_old)
                          ? upsert_adapter_value_compute : NULL;
    bopts.value_rewrites_payload = opts->new_from_old != NULL;
    bopts.abort_window = upsert_adapter_abort_window;
    bopts.bulk_hook_ctx = &actx;
    bopts.has_indexed_fields = opts->has_indexed_fields;
    bopts.out_durability_degraded = opts->out_durability_degraded;
    if (opts->prepare_commit && opts->apply_commit) {
        bopts.prepare_window = upsert_adapter_prepare_window;
        bopts.apply_window = upsert_adapter_apply_window;
    }

    SlotcaskBulkRec rec = {0};
    rec.key = key; rec.klen = klen; rec.value = value; rec.vlen = vlen;
    rec.user_ctx = &actx;

    SlotcaskBulkState st = {0};
    BulkMutationShard shard = {0};
    shard.kf_shard_id = sid_kf;
    shard.recs = &rec; shard.st = &st; shard.nrecs = 1;
    shard.kind = BULK_MUTATION_UPSERT;
    shard.upsert_opts = &bopts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1; txn.window_cap = 1;
    txn.upsert_opts = &bopts;

    int rc = slotcask_bulk_mutation_transaction(&txn);
    free(actx.callback_buf);

    if (opts->out_kf_shard) *opts->out_kf_shard = rec.kf_shard;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = rec.kf_slot;

    if (rc != 0 || shard.rc != 0) {
        /* Hard failure: kf_acquire, staging I/O, or a post-marker window
           that didn't converge synchronously (coordinator already set
           errno=EINPROGRESS for that case). */
        return -1;
    }
    if (rec.status == -2 && !actx.hard_error) {
        if (result) {
            result->was_update = rec.was_update;
            result->condition_not_met = 1;
            void *cv = NULL; size_t cvl = 0;
            if (slotcask_get(db, key, klen, &cv, &cvl) == 0) {
                result->current_value = cv;
                result->current_vlen = cvl;
            }
        }
        return -2;
    }
    if (rec.status != 0) return -1;

    if (result) {
        result->was_update = rec.was_update;
        result->condition_not_met = 0;
    }
    return 0;
}

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

    BulkMutationShard shard = {0};
    shard.kf_shard_id = kf_shard_id;
    shard.recs = recs; shard.nrecs = n;
    shard.kind = BULK_MUTATION_UPSERT;
    shard.upsert_opts = opts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1;
    txn.window_cap = db->bulk_commit_window > 0
                    ? (size_t)db->bulk_commit_window : 1024;
    txn.upsert_opts = opts;

    int rc = slotcask_bulk_mutation_transaction(&txn);
    return (rc != 0 || shard.rc != 0) ? -1 : 0;
}

int slotcask_bulk_delete_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                     SlotcaskBulkRec *recs, size_t n,
                                     const SlotcaskBulkDeleteOpts *opts) {
    if (n == 0) return 0;
    SlotcaskBulkDeleteOpts blank = {0};
    if (!opts) opts = &blank;
    if (opts->out_durability_degraded)
        *opts->out_durability_degraded = 0;

    BulkMutationShard shard = {0};
    shard.kf_shard_id = kf_shard_id;
    shard.recs = recs; shard.nrecs = n;
    shard.kind = BULK_MUTATION_DELETE;
    shard.delete_opts = opts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1;
    txn.window_cap = db->bulk_commit_window > 0
                    ? (size_t)db->bulk_commit_window : 1024;
    txn.delete_opts = opts;

    int rc = slotcask_bulk_mutation_transaction(&txn);
    return (rc != 0 || shard.rc != 0) ? -1 : 0;
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
    /* The kf reader stays held through phase 2: the window contract
       ("Kf read handle stays live until the segment record has been
       checked against its hash/key and copied into caller-owned
       memory") requires it. Released here, a window could tombstone
       (T) and re-emit (P) this slot while phase 2 reads it under an
       independent segcache rdlock — plain-vs-plain, zero common lock.
       Same discipline as slotcask_get and kf_reval_fetch_one. */
#ifdef TEST_BUILD
    /* Regression hook (docs/plans/2026-08-28-eliminate-tsan-supp.md
       Task B1): parks the caller after the probe phase so a test can
       run a full window's worth of slot churn in the gap. Post-fix the
       kf reader is still held here, so any window T step in the gap
       blocks — which is exactly the assertion the test makes. */
    if (atomic_load(&g_shard_test_bulk_lookup_gap) &&
        atomic_fetch_add(&g_shard_test_bulk_lookup_gap_hit, 1) == 0) {
        while (!atomic_load(&g_shard_test_bulk_lookup_gap_release))
            nanosleep(&(struct timespec){0, 1000000L}, NULL);
    }
#endif

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

    kfcache_release(&kh);
    free(st);
    return 0;
}

/* ============================================================ Two-phase bulk fetch */

/* Phase 1: resolve hashes to segment locations.
   Buckets by shard, probes each KF shard sequentially. */
static SlotcaskResolvedRec *slotcask_bulk_resolve_hashes(SlotcaskDb *db,
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

/* Per-kf-shard partition pipeline for slotcask_bulk_fetch_resolved:
 * revalidates every resolved address and copies the referenced segment
 * bytes while ONE kf reader handle stays live for the whole shard
 * (2026-08-21 window plan L214-217: "the Kf read handle stays live until
 * the selected segment record has been checked against its hash/key and
 * copied into caller-owned memory"). While the reader is held, no mutation
 * window on this shard can run its apply/publish steps, so a validated
 * address cannot change between check and copy — the per-shard pre-or-post
 * contract. Runs on parallel_for_io workers; a worker holds at most one kf
 * handle at any time (plan L75-76). Segment handles nest freely under a kf
 * reader: writers acquire seg locks before or while holding their own kf
 * lock, never after releasing it, so this nesting adds no cycle. */
typedef struct {
    SlotcaskDb          *db;
    SlotcaskResolvedRec *recs;
    size_t               start;
    size_t               count;
    int                  kf_shard;
    SlotcaskScanCb       cb;
    void                *ctx;
} KfRevalFetchArg;

/* qsort comparison: order a shard slice by segment location. A single
 * kf shard's records span MANY segment files (stream_id routes by
 * hash[15], independently of the kf shard), so the group key is the full
 * (sid, fid) pair with off as tiebreak — never fid alone. */
static int compare_sid_fid_off(const void *a, const void *b) {
    const SlotcaskResolvedRec *ra = (const SlotcaskResolvedRec *)a;
    const SlotcaskResolvedRec *rb = (const SlotcaskResolvedRec *)b;
    if (ra->sid != rb->sid) return (int)ra->sid - (int)rb->sid;
    if (ra->fid != rb->fid) return (int)ra->fid - (int)rb->fid;
    if (ra->off != rb->off) return (int)ra->off - (int)rb->off;
    return memcmp(ra->hash, rb->hash, 16);
}

static void kf_reval_fetch_one(KfRevalFetchArg *fa) {
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, fa->db->data_dir, fa->kf_shard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, fa->db->slots_per_shard, 0) != 0) {
        /* Whole partition unreadable — retire every record in it. */
#ifdef TEST_BUILD
        /* Round-5 diagnostic seam — per-hash trace when the entire kf
           partition for this shard is unreadable (kfcache_acquire
           failure), so this silent whole-partition retirement is
           distinguishable from a per-record kf_reval mismatch (below)
           or an upstream drop in slotcask_bulk_resolve_hashes (which
           would show as no kf_acquire_fail AND no kf_reval line at all
           for that hash). Temporary — delete with the plan close-out. */
        for (size_t i = 0; i < fa->count; i++) {
            const uint8_t *nb2_h = fa->recs[fa->start + i].hash;
            char nb2_hex[33] = {0};
            for (int nb2_j = 0; nb2_j < 16; nb2_j++)
                snprintf(nb2_hex + nb2_j * 2, 3, "%02x", nb2_h[nb2_j]);
            LOG_AUDIT(LOG_SUB_SLOTCASK,
                      "NB2TRACE5 kf_acquire_fail hash=%s kf_shard=%d",
                      nb2_hex, fa->kf_shard);
        }
#endif
        for (size_t i = 0; i < fa->count; i++)
            fa->recs[fa->start + i].sid = 0xFF;
        return;
    }

    for (size_t i = 0; i < fa->count; i++) {
        SlotcaskResolvedRec *r = &fa->recs[fa->start + i];
        uint8_t flag = 0, sid = 0;
        uint16_t fid = 0;
        uint32_t off = 0;
        size_t slot = 0;
#ifdef TEST_BUILD
        /* Round-5 diagnostic seam — per-record outcome of the kf-boundary
           revalidation probe. A resolve-time (r->sid/fid/off, captured by
           slotcask_bulk_resolve_hashes moments earlier under a SEPARATE
           kfcache_acquire on this same shard) that disagrees with this
           second, still-single-threaded lookup — with no writer able to
           run between the two calls in this test — points at the
           revalidation probe itself rather than a genuine repoint/delete.
           Temporary — delete with the plan close-out. */
        int nb2_rc = kf_lookup_no_verify(&kh, r->hash, &flag, &sid, &fid,
                                          &off, &slot);
        int nb2_mismatch = nb2_rc != 0 || flag != 1 || sid != r->sid ||
                            fid != r->fid || off != r->off;
        {
            char nb2_hex[33] = {0};
            for (int nb2_j = 0; nb2_j < 16; nb2_j++)
                snprintf(nb2_hex + nb2_j * 2, 3, "%02x", r->hash[nb2_j]);
            LOG_AUDIT(LOG_SUB_SLOTCASK,
                      "NB2TRACE5 kf_reval hash=%s rc=%d mismatch=%d "
                      "resolve_sid=%u resolve_fid=%u resolve_off=%u "
                      "reval_flag=%u reval_sid=%u reval_fid=%u reval_off=%u",
                      nb2_hex, nb2_rc, nb2_mismatch,
                      (unsigned)r->sid, (unsigned)r->fid, (unsigned)r->off,
                      (unsigned)flag, (unsigned)sid, (unsigned)fid,
                      (unsigned)off);
        }
        if (nb2_mismatch)
            r->sid = 0xFF;  /* repointed or gone since resolve */
#else
        if (kf_lookup_no_verify(&kh, r->hash, &flag, &sid, &fid, &off,
                                &slot) != 0 ||
            flag != 1 || sid != r->sid || fid != r->fid || off != r->off)
            r->sid = 0xFF;  /* repointed or gone since resolve */
#endif
    }

    /* Compact survivors within the slice (disjoint per partition). */
    size_t live_n = 0;
    for (size_t i = 0; i < fa->count; i++) {
        SlotcaskResolvedRec *r = &fa->recs[fa->start + i];
        if (r->sid != 0xFF) fa->recs[fa->start + live_n++] = *r;
    }

    if (live_n > 0) {
        qsort(&fa->recs[fa->start], live_n, sizeof(SlotcaskResolvedRec),
              compare_sid_fid_off);
        /* Copy every survivor's bytes under the STILL-HELD reader. */
        size_t run_start = 0;
        for (size_t i = 0; i < live_n; i++) {
            int last = (i == live_n - 1);
            if (!last &&
                fa->recs[fa->start + i].sid ==
                    fa->recs[fa->start + i + 1].sid &&
                fa->recs[fa->start + i].fid ==
                    fa->recs[fa->start + i + 1].fid)
                continue;
            char seg_path[PATH_MAX];
            SlotcaskSegHandle h;
            seg_path_for(seg_path, fa->db->data_dir,
                         fa->recs[fa->start + run_start].sid,
                         fa->recs[fa->start + run_start].fid);
            if (segcache_acquire(&h, seg_path, 0, 0, 0) == 0) {
                for (size_t j = run_start; j <= i; j++) {
                    const SlotcaskResolvedRec *r =
                        &fa->recs[fa->start + j];
                    const uint8_t *rec = h.map + r->off;
#ifdef TEST_BUILD
                    /* Round-5 diagnostic seam — per-record outcome of the
                       final segment-level liveness+hash check, the last
                       gate before a resolved-and-kf-revalidated candidate
                       reaches count_batch_cb. Temporary — delete with the
                       plan close-out. */
                    {
                        int nb2_live = seg_rec_live_with_hash(rec, r->hash);
                        char nb2_hex[33] = {0};
                        for (int nb2_j = 0; nb2_j < 16; nb2_j++)
                            snprintf(nb2_hex + nb2_j * 2, 3, "%02x",
                                     r->hash[nb2_j]);
                        LOG_AUDIT(LOG_SUB_SLOTCASK,
                                  "NB2TRACE5 seg_live hash=%s live=%d",
                                  nb2_hex, nb2_live);
                        if (!nb2_live) continue;
                    }
#else
                    if (!seg_rec_live_with_hash(rec, r->hash)) continue;
#endif
                    uint16_t klen = seg_rec_klen(rec);
                    uint32_t vlen = seg_rec_vlen(rec);
                    if (fa->cb(r->hash, rec + 24, klen,
                               rec + 24 + klen, vlen, fa->ctx) != 0)
                        break;
                }
                segcache_release(&h);
            }
#ifdef TEST_BUILD
            else {
                /* Round-5 diagnostic seam — per-hash trace when the
                   segment file backing this run is unreadable
                   (segcache_acquire failure), so this silent whole-run
                   skip is distinguishable from a per-record
                   seg_rec_live_with_hash rejection (above). Every hash
                   in [run_start, i] is affected — none of them get a
                   seg_live line for this call. Temporary — delete with
                   the plan close-out. */
                for (size_t j = run_start; j <= i; j++) {
                    const uint8_t *nb2_h = fa->recs[fa->start + j].hash;
                    char nb2_hex[33] = {0};
                    for (int nb2_k = 0; nb2_k < 16; nb2_k++)
                        snprintf(nb2_hex + nb2_k * 2, 3, "%02x",
                                 nb2_h[nb2_k]);
                    LOG_AUDIT(LOG_SUB_SLOTCASK,
                              "NB2TRACE5 seg_acquire_fail hash=%s "
                              "seg_path=%s",
                              nb2_hex, seg_path);
                }
            }
#endif
            run_start = i + 1;
        }
    }

    kfcache_release(&kh);
}

/* parallel_for_io entry — pthread-style fn returning void*. */
static void *kf_reval_fetch_worker(void *arg) {
    kf_reval_fetch_one((KfRevalFetchArg *)arg);
    return NULL;
}

/* Phase 2: fetch records from pre-resolved locations.
 * Partitions by Kf shard, then revalidates and copies each partition
 * entirely under one continuously-held Kf reader (see kf_reval_fetch_one).
 * NOTE on callback context: cb fires under a held kf reader and must not
 * re-enter slotcask/btree APIs. Callback arrival order is per-shard-grouped
 * rather than globally (sid,fid)-sorted; every caller keys results by
 * hash16 or serialises emits itself, so no consumer depends on the old
 * global ordering. */
int slotcask_bulk_fetch_resolved(SlotcaskDb *db,
                                  SlotcaskResolvedRec *recs,
                                  size_t n,
                                  void *ctx,
                                  SlotcaskScanCb cb) {
    if (n == 0 || !cb) return 0;

    /* Partition in place by KF shard: single pass with an index-keyed
       bucket layout (avoids a comparator that needs db->num_shards). */
    int nshards = db->num_shards;
    size_t *part_start = calloc((size_t)nshards, sizeof(size_t));
    size_t *part_count = calloc((size_t)nshards, sizeof(size_t));
    if (!part_start || !part_count) {
        free(part_start); free(part_count); return -1;
    }
    for (size_t i = 0; i < n; i++)
        part_count[shard_for_hash(recs[i].hash, nshards)]++;
    size_t acc = 0;
    for (int s = 0; s < nshards; s++) {
        part_start[s] = acc;
        acc += part_count[s];
    }
    SlotcaskResolvedRec *sorted = malloc(n * sizeof(SlotcaskResolvedRec));
    size_t *fill = calloc((size_t)nshards, sizeof(size_t));
    if (!sorted || !fill) {
        free(part_start); free(part_count); free(fill); free(sorted);
        return -1;
    }
    for (size_t i = 0; i < n; i++) {
        int s = shard_for_hash(recs[i].hash, nshards);
        sorted[part_start[s] + fill[s]++] = recs[i];
    }
    free(fill);

    /* Dispatch one reval+fetch pipeline per non-empty partition. */
    int nparts = 0;
    for (int s = 0; s < nshards; s++)
        if (part_count[s] > 0) nparts++;
    if (nparts > 0) {
        KfRevalFetchArg *fargs = calloc((size_t)nparts, sizeof(KfRevalFetchArg));
        if (!fargs) {
            free(part_start); free(part_count); free(sorted);
            return -1;
        }
        int fi = 0;
        for (int s = 0; s < nshards; s++) {
            if (part_count[s] == 0) continue;
            fargs[fi].db = db;
            fargs[fi].recs = sorted;
            fargs[fi].start = part_start[s];
            fargs[fi].count = part_count[s];
            fargs[fi].kf_shard = s;
            fargs[fi].cb = cb;
            fargs[fi].ctx = ctx;
            fi++;
        }
        if (nparts <= 3) {
            for (int i = 0; i < nparts; i++) kf_reval_fetch_one(&fargs[i]);
        } else {
            parallel_for_io(kf_reval_fetch_worker, fargs, nparts,
                            sizeof(KfRevalFetchArg));
        }
        free(fargs);
    }
    free(part_start);
    free(part_count);
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

/* ============================================================ Task 4:
 * SlotcaskDeleteOpts -> SlotcaskBulkDeleteOpts adapter.
 *
 * Same rationale as the upsert adapter above: slotcask_delete_with_hooks is
 * a thin adapter over a one-record, one-shard, window_cap=1 delete
 * transaction. SlotcaskDeleteOpts's prepare_commit/apply_commit both take
 * `old` directly; the bulk delete window hooks don't (only pre_commit does,
 * per-record). old is stashed in the ctx during the pre_commit-shaped
 * closure and handed to prepare_window/apply_window from there — the same
 * "capture during pre_commit, dispatch from *_window" pattern query_bulk.c's
 * own hook implementations already use. The pointer stays valid: it
 * addresses shard->st[0].old_buf, which lives for the whole window-commit
 * call (freed only after bulk_commit_one_kf_window's window finishes). */

typedef struct {
    const SlotcaskDeleteOpts *dopt;
    int pre_commit_ran;
    const uint8_t *old_ptr;
    size_t old_len;
    int has_old;
} DeleteAdapterCtx;

static int delete_adapter_pre_commit(const SlotcaskOldRecord *old,
                                     SlotcaskBulkRec *rec) {
    DeleteAdapterCtx *actx = (DeleteAdapterCtx *)rec->user_ctx;
    const SlotcaskDeleteOpts *dopt = actx->dopt;
    actx->pre_commit_ran = 1;
    if (dopt->check && dopt->check(old, dopt->check_ctx) == 0) return -1;
    actx->has_old = old != NULL;
    actx->old_ptr = old ? old->value : NULL;
    actx->old_len = old ? old->vlen : 0;
    if (dopt->has_indexed_fields) return 0;   /* prepare/apply_commit handle it */
    if (dopt->pre_commit && dopt->pre_commit(old, dopt->pre_commit_ctx) != 0)
        return -1;
    return 0;
}

static int delete_adapter_prepare_window(SlotcaskBulkRec *recs,
                                         const size_t *active, size_t nactive,
                                         void *ctx) {
    (void)recs;
    if (nactive == 0) return 0;
    DeleteAdapterCtx *actx = (DeleteAdapterCtx *)ctx;
    const SlotcaskDeleteOpts *dopt = actx->dopt;
    if (!dopt->prepare_commit) return 0;
    SlotcaskBulkRec *rec = &recs[active[0]];
    SlotcaskOldRecord old_rec = { actx->old_ptr, actx->old_len };
    return dopt->prepare_commit(actx->has_old ? &old_rec : NULL, rec->kf_slot,
                                dopt->pre_commit_ctx) != 0 ? -1 : 0;
}

static int delete_adapter_apply_window(SlotcaskBulkRec *recs,
                                       const size_t *active, size_t nactive,
                                       void *ctx) {
    if (nactive == 0) return 0;
    DeleteAdapterCtx *actx = (DeleteAdapterCtx *)ctx;
    const SlotcaskDeleteOpts *dopt = actx->dopt;
    if (!dopt->apply_commit) return 0;
    SlotcaskBulkRec *rec = &recs[active[0]];
    SlotcaskOldRecord old_rec = { actx->old_ptr, actx->old_len };
    return dopt->apply_commit(actx->has_old ? &old_rec : NULL, rec->kf_slot,
                              dopt->pre_commit_ctx) != 0 ? -1 : 0;
}

static void delete_adapter_abort_window(void *ctx) {
    DeleteAdapterCtx *actx = (DeleteAdapterCtx *)ctx;
    const SlotcaskDeleteOpts *dopt = actx->dopt;
    if (dopt->abort_commit) dopt->abort_commit(dopt->pre_commit_ctx);
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
    /* When has_indexed_fields is set, apply_commit is mandatory and there is
       no legacy single-phase (pre_commit) path — same guard as before. */
    if (opts->has_indexed_fields &&
        (!opts->apply_commit || opts->pre_commit)) {
        errno = EINVAL;
        return -1;
    }

    uint8_t hash[16];
    compute_hash(key, klen, hash);
    int sid_kf = shard_for_hash(hash, db->num_shards);
    /* See slotcask_upsert_with_hooks's identical comment: kf_shard is known
       before the transaction starts, so publish it now for hooks (e.g.
       apply_commit, which otherwise only sees the ctx-cached value written
       after the whole transaction returns). */
    if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;

    DeleteAdapterCtx actx = { .dopt = opts };
    SlotcaskBulkDeleteOpts bopts = {0};
    bopts.pre_commit = delete_adapter_pre_commit;
    /* check needs OLD by definition; pre_commit needs it unless the caller
       opted out; the indexed state machine always needs it for the forward
       diff — same needs_old computation the legacy body used. */
    bopts.pre_commit_needs_old = (opts->check != NULL) ||
                                 (opts->pre_commit != NULL && !opts->skip_old_read) ||
                                 opts->has_indexed_fields;
    if (opts->prepare_commit || opts->apply_commit) {
        bopts.prepare_window = delete_adapter_prepare_window;
        bopts.apply_window = delete_adapter_apply_window;
    }
    bopts.abort_window = delete_adapter_abort_window;
    bopts.bulk_hook_ctx = &actx;
    bopts.has_indexed_fields = opts->has_indexed_fields;
    bopts.out_durability_degraded = opts->out_durability_degraded;

    SlotcaskBulkRec rec = {0};
    rec.key = key; rec.klen = klen;
    rec.user_ctx = &actx;

    SlotcaskBulkState st = {0};
    BulkMutationShard shard = {0};
    shard.kf_shard_id = sid_kf;
    shard.recs = &rec; shard.st = &st; shard.nrecs = 1;
    shard.kind = BULK_MUTATION_DELETE;
    shard.delete_opts = &bopts;

    BulkMutationTxn txn = {0};
    txn.db = db; txn.shards = &shard; txn.nshards = 1; txn.window_cap = 1;
    txn.delete_opts = &bopts;

    int rc = slotcask_bulk_mutation_transaction(&txn);

    if (opts->out_kf_shard) *opts->out_kf_shard = rec.kf_shard;
    if (opts->out_kf_slot)  *opts->out_kf_slot  = rec.kf_slot;

    if (rc != 0 || shard.rc != 0) return -1;

    if (rec.status == -2) {
        if (!actx.pre_commit_ran) {
            if (result) result->not_found = 1;
            return -2;
        }
        if (result) {
            result->condition_not_met = 1;
            void *cv = NULL; size_t cvl = 0;
            if (slotcask_get(db, key, klen, &cv, &cvl) == 0) {
                result->current_value = cv;
                result->current_vlen = cvl;
            }
        }
        return -2;
    }
    if (rec.status != 0) return -1;
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
/* The registry table, lock, and cond live in the g_db ShardDb, which is
   THREAD-LOCAL (embedded.c __thread g_db). Threads spawned outside the
   embedded/server bind paths (direct API callers, test pthreads) start with
   TLS g_db == NULL and would otherwise lock/cond-wait on garbage addresses —
   silently bypassing the registry protocol. Bind lazily from the process
   instance, same precedent as objlock.c's g_db guard. */
static void registry_bind_g_db(void) {
    if (!g_db && g_shard_db_instance) g_db = g_shard_db_instance;
}

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
    registry_bind_g_db();

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
            __atomic_add_fetch(&db->reg_refs, 1, __ATOMIC_ACQ_REL);
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
        int open_rc;
        if (db) {
            open_rc = slotcask_open(db, data_dir, info->splits,
                                    info->streams, info->slot_size);
        } else {
            errno = ENOMEM;
            open_rc = -1;
        }
        int open_errno = errno;

        pthread_mutex_lock(&g_reg_lock);
        if (open_rc != 0 || !db) {
            if (db) free(db);
            g_reg[reserved].opening = 0;
            g_reg[reserved].key[0] = '\0';
            pthread_cond_broadcast(&g_reg_cond);
            pthread_mutex_unlock(&g_reg_lock);
            fprintf(stderr, "slotcask_registry: open failed for %s/%s: %s\n",
                    effective_root, object, strerror(open_errno));
            errno = open_errno;
            return NULL;
        }
        g_reg[reserved].db = db;
        /* Table reference plus the opener's caller reference. */
        __atomic_store_n(&db->reg_refs, 2, __ATOMIC_RELAXED);
        g_reg[reserved].used = 1;
        g_reg[reserved].opening = 0;
        pthread_cond_broadcast(&g_reg_cond);
        pthread_mutex_unlock(&g_reg_lock);
        return db;
    }
}

void slotcask_registry_put(SlotcaskDb *db) {
    if (!db) return;
    if (__atomic_sub_fetch(&db->reg_refs, 1, __ATOMIC_ACQ_REL) == 0) {
        slotcask_close(db);
        free(db);
    }
}

void slotcask_registry_invalidate(const char *effective_root,
                                  const char *object) {
    registry_bind_g_db();

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
    SlotcaskDb *db = NULL;
    if (slot >= 0 && g_reg[slot].used) {
        db = g_reg[slot].db;
        g_reg[slot].used = 0;
        g_reg[slot].key[0] = '\0';
        g_reg[slot].db = NULL;
    }
    pthread_mutex_unlock(&g_reg_lock);

    kfcache_invalidate_prefix(data_dir);
    segcache_invalidate_prefix(data_dir);

    /* Drop the table reference after unlinking and cache invalidation. */
    if (db) slotcask_registry_put(db);
}

void slotcask_registry_shutdown(void) {
    registry_bind_g_db();
    pthread_mutex_lock(&g_reg_lock);
    for (int i = 0; i < SLOTCASK_REG_BUCKETS; i++) {
        if (g_reg[i].used && g_reg[i].db) {
            SlotcaskDb *db = g_reg[i].db;
            g_reg[i].db = NULL;
            g_reg[i].used = 0;
            g_reg[i].key[0] = '\0';
            pthread_mutex_unlock(&g_reg_lock);
            slotcask_registry_put(db);
            pthread_mutex_lock(&g_reg_lock);
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

/* ============================================================ Kf-driven live scan
 *
 * Query-side full-scan bridge: one worker owns one Kf shard read handle,
 * snapshots that shard's live (stream,file,offset,hash) addresses, then
 * fetches/validates exactly those frozen locations before releasing the
 * handle. While the reader is held no mutation window on the shard can
 * run its apply/publish steps, so the fetched bytes cannot change under
 * the scan — the per-shard pre-or-post contract. This replaces raw
 * segment-flag enumeration as the source of truth for queries; raw
 * segment O_DIRECT scans remain maintenance-only under objlock_wrlock.
 */
static int kf_shard_acquire(SlotcaskKfHandle *kh, const SlotcaskDb *db,
                            int kf_shard_id, int writer) {
    char path[PATH_MAX];
    slotcask_kf_path(path, sizeof(path), db->data_dir, kf_shard_id);
    return kfcache_acquire(kh, path, db->slots_per_shard, writer);
}

typedef struct {
    uint8_t  stream_id;
    uint16_t file_id;
    uint32_t offset;
    uint8_t  hash[16];
} KfLiveAddress;

static int collect_live_kf_addresses_locked(SlotcaskDb *db,
                                            const SlotcaskKfHandle *kh,
                                            KfLiveAddress **out,
                                            size_t *nout) {
    KfLiveAddress *v = NULL;
    size_t n = 0, cap = 0;
    (void)db;

    for (size_t s = 0; s < kh->capacity; s++) {
        const SlotcaskKfEntry *e = &kh->map[s];
        if (__atomic_load_n(&e->flag, __ATOMIC_ACQUIRE) != 1) continue;
        if (n == cap) {
            size_t ncap = cap ? cap * 2 : 256;
            KfLiveAddress *nv = realloc(v, ncap * sizeof(*nv));
            if (!nv) { free(v); return -1; }
            v = nv; cap = ncap;
        }
        memcpy(v[n].hash, e->hash, 16);
        v[n].stream_id = e->stream_id;
        v[n].file_id   = e->file_id;
        v[n].offset    = e->offset;
        n++;
    }
    *out = v;
    *nout = n;
    return 0;
}

static int kf_live_addr_cmp(const void *a, const void *b) {
    const KfLiveAddress *x = a, *y = b;
    if (x->stream_id != y->stream_id) return x->stream_id < y->stream_id ? -1 : 1;
    if (x->file_id   != y->file_id)   return x->file_id   < y->file_id   ? -1 : 1;
    if (x->offset    != y->offset)    return x->offset    < y->offset    ? -1 : 1;
    return 0;
}

/* Accepts a segment record only when its header hash matches the live Kf
   address that named it; anything else (torn/stale/reused slot) is skipped.
   Runs entirely under the shard read handle plus one segcache handle per
   segment file. */
static int fetch_live_addresses_grouped_locked(SlotcaskDb *db,
                                               const SlotcaskKfHandle *kh,
                                               KfLiveAddress *live, size_t nlive,
                                               SlotcaskScanCb cb, void *ctx,
                                               _Atomic int *stop) {
    (void)kh;
    if (nlive > 0)             /* zero-live shards have a NULL vector */
        qsort(live, nlive, sizeof(*live), kf_live_addr_cmp);

    size_t i = 0;
    while (i < nlive) {
        if (stop && atomic_load_explicit(stop, memory_order_acquire)) return 0;
        size_t j = i + 1;
        while (j < nlive && live[j].stream_id == live[i].stream_id &&
               live[j].file_id == live[i].file_id)
            j++;
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, live[i].stream_id, live[i].file_id);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;
        for (size_t k = i; k < j; k++) {
            const uint8_t *rec = h.map + live[k].offset;
            if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) != 1) continue;
            if (memcmp(rec, live[k].hash, 16) != 0) continue;
            uint16_t klen = seg_rec_klen(rec);
            uint32_t vlen = seg_rec_vlen(rec);
            if (cb(live[k].hash, rec + 24, klen,
                   rec + 24 + klen, vlen, ctx) != 0) {
                segcache_release(&h);
                if (stop) atomic_store_explicit(stop, 1, memory_order_release);
                return 0;
            }
        }
        segcache_release(&h);
        i = j;
    }
    return 0;
}

typedef struct {
    SlotcaskDb *db;
    int kf_shard_id;
    SlotcaskScanCb cb;
    void *ctx;
    _Atomic int *stop;
    int rc;
} KfLiveScanWork;

static void *slotcask_scan_live_kf_worker(void *raw) {
    KfLiveScanWork *w = raw;
    SlotcaskKfHandle kh;
    KfLiveAddress *live = NULL;
    size_t nlive = 0;

    if (kf_shard_acquire(&kh, w->db, w->kf_shard_id, 0) != 0)
        goto failed;
    if (collect_live_kf_addresses_locked(w->db, &kh, &live, &nlive) != 0)
        goto release_kf;
    if (fetch_live_addresses_grouped_locked(w->db, &kh, live, nlive,
                                            w->cb, w->ctx, w->stop) != 0)
        goto release_kf;
    kfcache_release(&kh);
    free(live);
    return NULL;

release_kf:
    kfcache_release(&kh);
failed:
    free(live);
    w->rc = -1;
    if (w->stop) atomic_store_explicit(w->stop, 1, memory_order_release);
    return NULL;
}

/* Single-shard form: the caller (query bridge) owns the parallel fan-out
   so it can also set per-worker thread state (g_out). Returns 0 on
   success, -1 on failure (and sets *stop_flag when non-NULL). */
int slotcask_scan_live_kf_one(SlotcaskDb *db, int kf_shard_id,
                              SlotcaskScanCb cb, void *ctx,
                              int *stop_flag) {
    if (!db || !cb || kf_shard_id < 0 || kf_shard_id >= db->num_shards)
        return -1;
    KfLiveScanWork w = {
        .db = db, .kf_shard_id = kf_shard_id, .cb = cb, .ctx = ctx,
        .stop = (_Atomic int *)stop_flag, .rc = 0,
    };
    slotcask_scan_live_kf_worker(&w);
    return w.rc;
}

/* Parallel full-scan entry point for the query engine: dispatches one
   KfLiveScanWork per shard via parallel_for_io. Returns 0 when every
   shard worker ran clean, -1 otherwise. `stop` may be NULL. */
int slotcask_scan_live_kf(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx,
                          int *stop_flag) {
    if (!db || !cb) return -1;
    _Atomic int local_stop = 0;
    _Atomic int *stop = stop_flag ? (_Atomic int *)stop_flag : &local_stop;

    KfLiveScanWork *args = calloc((size_t)db->num_shards, sizeof(*args));
    if (!args) return -1;
    for (int s = 0; s < db->num_shards; s++) {
        args[s].db = db;
        args[s].kf_shard_id = s;
        args[s].cb = cb;
        args[s].ctx = ctx;
        args[s].stop = stop;
        args[s].rc = 0;
    }
    parallel_for_io(slotcask_scan_live_kf_worker, args, db->num_shards,
                    sizeof(*args));
    int rc = 0;
    for (int s = 0; s < db->num_shards; s++)
        if (args[s].rc != 0) rc = -1;
    free(args);
    return rc;
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
/* Context for slotcask_walk_live_skip's per-shard locked scan. */
typedef struct {
    SlotcaskDb    *db;
    int64_t        remaining_skip;
    int            stop;
    SlotcaskScanCb cb;
    void          *ctx;
} KfOdSkipCtx;

/* Returns 1 when the walk should stop (cb asked to stop). The caller
   holds the Kf reader for the shard across the segment validation and
   copy, so a mid-window entry can never be observed half-published. */
static int kf_skip_emit_entry(SlotcaskDb *db, const SlotcaskKfEntry *e,
                              KfOdSkipCtx *c) {
    uint8_t flag = __atomic_load_n(&e->flag, __ATOMIC_ACQUIRE);
    if (flag != 1) return 0;

    /* Cheap skip: count live entries without touching segments. */
    if (c->remaining_skip > 0) { c->remaining_skip--; return 0; }

    /* Past the skip window — load the segment record and emit. */
    char seg_path[PATH_MAX];
    seg_path_for(seg_path, db->data_dir, e->stream_id, e->file_id);
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
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0)
            continue;
        SlotcaskKfEntry *kf = kh.map;
        for (size_t i = 0; i < kh.capacity && !c.stop; i++)
            if (kf_skip_emit_entry(db, &kf[i], &c)) break;
        kfcache_release(&kh);
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
    char        recipient_path[PATH_MAX];
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

    /* The cache lock order is kfcache then segcache.  Do not retain the
       recipient segcache handle across this kf acquire: ordinary lookup and
       walk paths hold a kf handle while verifying a segment record. */
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

    SlotcaskSegHandle rh;
    if (segcache_acquire(&rh, c->recipient_path, 0, 0, 0) != 0) {
        kfcache_release(&kh);
        c->rc = -1;
        return 1;
    }
    seg_record_emit(rh.map + target_off, (int)donor_rec_size,
                    hash16, key, (size_t)klen, value, vlen);
    if (durability_msync_range(rh.map, target_off, donor_rec_size) != 0) {
        segcache_release(&rh);
        kfcache_release(&kh);
        c->rc = -1;
        return 1;
    }
    durability_test_pause(c->db->data_dir, "compact-after-recipient-sync");

    kf_repoint_at_slot(&kh, kf_slot_idx, (uint8_t)c->stream_id,
                        (uint16_t)c->recipient_fid, target_off);
    segcache_release(&rh);
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

    /* The callback acquires kfcache before briefly reacquiring this recipient
       handle, matching every other nested cache acquisition path. */
    segcache_release(&rh);

    VarlenCompactCtx ctx = {
        .db = db, .stream_id = stream_id,
        .donor_fid = donor_fid, .recipient_fid = recipient_fid,
        .free_offs = free_offs, .free_caps = free_caps,
        .free_count = free_count, .free_next = 0, .rc = 0,
        .kf_lookup_failed = 0,
    };
    snprintf(ctx.recipient_path, sizeof(ctx.recipient_path), "%s",
             recipient_path);

    /* Donor is read-only, so use the hardened VARLEN scanner. It validates
       headers and resynchronizes across reused-slot zero-padding gaps. */
    {
        int drc = seg_scan_o_direct(donor_path, db->slot_size,
                                           varlen_compact_cb, &ctx);
        if (drc < 0) {
            free(free_offs);
            free(free_caps);
            return -1;
        }
    }

    if (out_kf_failed) *out_kf_failed = ctx.kf_lookup_failed;
    free(free_offs);
    free(free_caps);
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
