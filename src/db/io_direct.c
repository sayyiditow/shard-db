/*
 * io_direct.c — aligned O_DIRECT pread helpers for cache-bypassing scans.
 *
 * Three public entry points:
 *   od_open / od_pread / od_alloc_buf   — low-level primitives
 *   seg_scan_o_direct                   — double-buffered seg-file walker
 *   btree_leaf_scan_o_direct            — double-buffered btree leaf walker
 *
 * No existing query path is changed here; 1e.4 wires these into FP_FULL_SCAN.
 *
 * Double-buffer threading model
 * ─────────────────────────────
 * Two 4 MB aligned buffers: buf[0] and buf[1].
 * One worker thread, one main thread.
 *
 * Shared state (protected by `lock`):
 *   state ∈ { IDLE, WORKING, READY, DONE }
 *   inactive — index of the buffer the worker is filling / has filled
 *   next_len — bytes written into buf[inactive] on last fill
 *   next_off — file offset the worker should read next
 *   err      — errno from worker I/O failure (0 = ok)
 *
 * Protocol:
 *   1. dbctx_init fills buf[0] synchronously (main thread, no worker yet).
 *      Sets active=0, state=IDLE, next_off=<bytes read>.
 *   2. pthread_create launches the worker, which starts in IDLE state
 *      and immediately waits on `prefetch_needed`.
 *   3. After init returns, main calls dbctx_kickoff() — locks, sets
 *      state=IDLE (worker is free), signals `prefetch_needed` so the
 *      worker begins filling buf[1].
 *   4. Main parses buf[0]. When done: dbctx_swap() —
 *        a. Wait until state == READY (worker has filled the inactive buf).
 *        b. Swap active ↔ inactive.
 *        c. Set state = IDLE, signal `prefetch_needed` so worker fills the
 *           newly-freed buffer.
 *        d. Return next_len (0 = EOF/done).
 *   5. Worker loop (state machine):
 *        IDLE  → wait on prefetch_needed
 *        Wake  → if worker_quit or next_off >= file_size: state=DONE, signal, exit
 *              → else: unlock, pread into buf[inactive], lock, state=READY,
 *                       next_off += got, signal prefetch_done, loop back to wait
 *
 * This guarantees: the signal on prefetch_done is only emitted when state==READY,
 * and main only waits on prefetch_done when state!=READY. No lost wakeups.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "io_direct.h"
#include "seg_scan_varlen.h"
#include "btree.h"    /* BtFileHeader, BtPageHeader, BT_MAGIC*, bt_page_size,
                         BT_PAGE_DATA_START, BT_LEAF_RESTART_K,
                         BT_MAX_VAL_LEN, BT_HASH_SIZE               */
#include "slotcask.h"
#include "simd.h"    /* simd_memmem */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pthread.h>
#include <errno.h>
#include <stdint.h>

static inline uint16_t bts_data_len(const uint8_t *e);
static inline uint8_t bts_prefix_len(const uint8_t *e);
static inline const uint8_t *bts_suffix(const uint8_t *e);
static inline size_t bts_suffix_len(const uint8_t *e);
static inline const uint8_t *bts_hash(const uint8_t *e);
static inline int bts_is_tomb(const uint8_t *e);
static inline uint16_t bts_slot_off(const uint8_t *page, uint32_t s);

/* Compiler-portable prefetch.  Non-faulting on x86/ARM64; noop elsewhere. */
static inline void od_prefetch(const void *addr)
{
#if defined(__GNUC__) || defined(__clang__)
    __builtin_prefetch(addr, 0, 0);
#else
    (void)addr;
#endif
}

#if defined(__GNUC__) || defined(__clang__)
#define od_likely(x)   __builtin_expect(!!(x), 1)
#define od_unlikely(x) __builtin_expect(!!(x), 0)
#else
#define od_likely(x)   (x)
#define od_unlikely(x) (x)
#endif

/* posix_fadvise is Linux-specific; macOS lacks it. */
#ifndef POSIX_FADV_SEQUENTIAL
#  define POSIX_FADV_SEQUENTIAL 2
#  define POSIX_FADV_DONTNEED   4
#endif
/* Configurable buffer size — reads DB_ODIRECT_BUF_MB env var.
   Defaults to 4 MB. Lazily initialised from multiple io_pool_worker
   threads; _Atomic since concurrent initializers race on the plain read
   and write (they always compute the same value, but the C11 memory
   model still calls that a data race, and TSan agrees). */
_Atomic size_t odirect_buf_size = 0;

void odirect_init_buf_size(void)
{
    if (atomic_load_explicit(&odirect_buf_size, memory_order_relaxed) > 0)
        return;
    const char *env = getenv("DB_ODIRECT_BUF_MB");
    if (env && env[0]) {
        char *end = NULL;
        long mb = strtol(env, &end, 10);
        if (end != env && mb > 0 && mb <= 1048576) {
            atomic_store_explicit(&odirect_buf_size, (size_t)mb * 1024 * 1024,
                                  memory_order_relaxed);
            return;
        }
    }
    atomic_store_explicit(&odirect_buf_size, ODIRECT_BUF_SIZE_DEFAULT,
                          memory_order_relaxed);
}


/* ============================================================
 * Page-cache residency probe (Linux only)
 * ========================================================= */

#ifdef __linux__
#include <sys/mman.h>

/* Returns 1 if at least `threshold_pct` percent of the file's pages
   are resident in the OS page cache, 0 otherwise (including any error).
   Uses N_SAMPLES evenly-spaced page probes so the cost is O(1) regardless
   of file size — no full mincore vec allocation.

   Mechanism:
     mmap(PROT_NONE | MAP_PRIVATE | MAP_NORESERVE) reserves virtual address
     space without touching any physical pages.  mincore() then queries which
     4 KB pages in that mapping are backed by the page cache.  munmap releases
     the virtual reservation.  Total syscall cost: ~5–15 µs per file.

   Conservative on error: returns 0 so the caller falls through to O_DIRECT. */
#define OD_MINCORE_SAMPLES 8
static int file_is_resident(const char *path, int threshold_pct)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0) return 0;

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size <= 0) {
        close(fd);
        return 0;
    }

    size_t sz = (size_t)st.st_size;
    void *addr = mmap(NULL, sz, PROT_NONE,
                      MAP_PRIVATE | MAP_NORESERVE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) return 0;

    long pgsz = sysconf(_SC_PAGESIZE);
    if (pgsz <= 0) pgsz = 4096;
    long total_pages = ((long)sz + pgsz - 1) / pgsz;

    int resident = 0;
    for (int i = 0; i < OD_MINCORE_SAMPLES; i++) {
        long page_idx = (long)i * total_pages / OD_MINCORE_SAMPLES;
        unsigned char vec = 0;
        /* mincore on a single page at each sample offset. */
        if (mincore((char *)addr + page_idx * pgsz,
                    (size_t)pgsz, &vec) == 0 && (vec & 1))
            resident++;
    }
    munmap(addr, sz);

    return resident * 100 / OD_MINCORE_SAMPLES >= threshold_pct;
}
#undef OD_MINCORE_SAMPLES
#endif /* __linux__ */


/* ============================================================
 * Low-level helpers
 * ========================================================= */

int od_open(const char *path)
{
    int fd = -1;

#ifdef __linux__
    /* If the file is already resident in the page cache, open it buffered.
       od_pread is plain pread() so it works on any fd.  RAM bandwidth
       (~30–50 GB/s) far exceeds NVMe O_DIRECT bandwidth (~3–4 GB/s), so
       skipping O_DIRECT on warm files is a strict win.
       Threshold 80%: tolerates a few evicted pages at the edges of a large
       file without wrongly forcing O_DIRECT. */
    if (file_is_resident(path, 80))
        return open(path, O_RDONLY);
#endif

#if defined(__APPLE__)
    fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    (void)fcntl(fd, F_NOCACHE, 1);   /* best-effort; ignore failures */
    return fd;

#elif defined(O_DIRECT) && O_DIRECT != 0
    fd = open(path, O_RDONLY | O_DIRECT);
    if (fd >= 0) return fd;
    /* EINVAL — filesystem doesn't support O_DIRECT (tmpfs, overlayfs …).
       Fall back to buffered + fadvise. */
    fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
#  ifdef POSIX_FADV_SEQUENTIAL
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL); /* best-effort; ignore failures */
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);   /* best-effort; ignore failures */
#  endif
    return fd;

#else
    fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
#  ifdef POSIX_FADV_SEQUENTIAL
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL); /* best-effort; ignore failures */
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);   /* best-effort; ignore failures */
#  endif
    return fd;
#endif
}

ssize_t od_pread(int fd, void *buf, size_t len, off_t off)
{
    return pread(fd, buf, len, off);
}

/* Convert an already-open scan fd from unbuffered to buffered in place, so
   a caller can pread at offsets that are only 8-byte (record) aligned, not
   device-sector aligned.  Keeping the same fd for the whole scan pins the
   file's inode — the path is never re-resolved, so a file swapped out from
   under us cannot change what the scan reads.  Best-effort: on Linux F_SETFL
   can clear O_DIRECT (a no-op when the fd is already buffered); on macOS
   F_NOCACHE 0 re-enables caching (mirror of od_open's F_NOCACHE 1); other
   platforms never set O_DIRECT at all. */
static void od_disable_odirect(int fd)
{
#if defined(__linux__)
    int flags = fcntl(fd, F_GETFL);
    if (flags >= 0 && (flags & O_DIRECT))
        (void)fcntl(fd, F_SETFL, flags & ~O_DIRECT);
#elif defined(__APPLE__)
    (void)fcntl(fd, F_NOCACHE, 0);
#else
    (void)fd;
#endif
}

void *od_alloc_buf(void)
{
    if (odirect_buf_size == 0) odirect_init_buf_size();
    void *p = NULL;
    if (posix_memalign(&p, ODIRECT_ALIGN, odirect_buf_size) != 0)
        return NULL;
    return p;
}

/* ============================================================
 * Double-buffer state machine
 * ========================================================= */

typedef enum {
    DBS_IDLE  = 0,   /* worker is waiting; inactive buf is free for a new fill */
    DBS_WORKING,     /* worker is filling buf[inactive] right now (lock dropped) */
    DBS_READY,       /* worker finished; buf[inactive] has next_len valid bytes  */
    DBS_DONE         /* worker has nothing more to prefetch (EOF or error)       */
} DbState;

typedef struct {
    int               fd;
    off_t             file_size;

    uint8_t          *buf[2];        /* posix_memalign'd 4 MB each */

    pthread_mutex_t   lock;
    pthread_cond_t    prefetch_needed; /* main → worker: inactive buf is free  */
    pthread_cond_t    prefetch_done;   /* worker → main: inactive buf is ready */

    /* all fields below protected by `lock` */
    DbState           state;
    int               active;        /* index of buf currently owned by main  */
    int               inactive;      /* index of buf currently owned by worker */
    ssize_t           active_len;    /* valid bytes in buf[active]             */
    ssize_t           next_len;      /* valid bytes in buf[inactive] after fill*/
    off_t             next_off;      /* file offset for the NEXT fill          */
    int               work_pending;  /* level-triggered: 1 = worker has a job */
    int               worker_quit;   /* set by destroy to tell worker to exit  */
    int               err;           /* errno from worker I/O failure          */
    int               single_shot;   /* 1 = file fits in buf[0]; no thread     */
} DbCtx;

/* Worker: waits for prefetch_needed, fills buf[inactive], signals prefetch_done. */
static void *prefetch_worker(void *arg)
{
    DbCtx *c = (DbCtx *)arg;

    pthread_mutex_lock(&c->lock);
    for (;;) {
        /* Wait until work_pending is set (level-triggered: avoids lost-wakeup
           if kickoff/swap signals before we reach cond_wait) or quit. */
        while (!c->work_pending && !c->worker_quit)
            pthread_cond_wait(&c->prefetch_needed, &c->lock);

        if (c->worker_quit) break;
        c->work_pending = 0;   /* consume the work ticket */

        /* Check if there's actually data to read. */
        if (c->next_off >= c->file_size) {
            c->state    = DBS_DONE;
            c->next_len = 0;
            pthread_cond_signal(&c->prefetch_done);
            break;
        }

        /* Transition to WORKING, drop lock, do pread. */
        c->state = DBS_WORKING;
        int    inactive = c->inactive;
        off_t  off      = c->next_off;
        off_t  remain   = c->file_size - off;
        size_t want     = (remain >= (off_t)odirect_buf_size)
                              ? odirect_buf_size : (size_t)remain;
        /* O_DIRECT needs alignment; round up (pread stops at real EOF). */
        size_t want_a   = (want + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
        if (want_a > odirect_buf_size) want_a = odirect_buf_size;
        pthread_mutex_unlock(&c->lock);

        ssize_t got = pread(c->fd, c->buf[inactive], want_a, off);

        pthread_mutex_lock(&c->lock);
        if (c->worker_quit) break;

        if (got < 0) {
            c->err      = errno;
            c->next_len = 0;
            c->state    = DBS_DONE;
            pthread_cond_signal(&c->prefetch_done);
            break;
        }

        c->next_off += got;
        c->next_len  = got;
        c->state     = DBS_READY;
        pthread_cond_signal(&c->prefetch_done);

        /* Now loop back and wait on prefetch_needed (state will be changed
           to IDLE by the main thread in dbctx_swap before it signals us). */
    }
    pthread_mutex_unlock(&c->lock);
    return NULL;
}

/*
 * dbctx_init: fill buf[0] synchronously; set up state for the worker.
 * Returns 0 on success, -errno on failure.
 * After this call, main should:
 *   1. Launch the worker via pthread_create.
 *   2. Call dbctx_kickoff() to start the first async prefetch into buf[1].
 *   3. Parse buf[0] (dc.active=0, dc.active_len bytes valid).
 */
static int dbctx_init(DbCtx *c, int fd, off_t file_size, off_t start_off, int single_shot)
{
    memset(c, 0, sizeof(*c));
    c->fd          = fd;
    c->file_size   = file_size;
    c->active      = 0;
    c->inactive    = 1;
    c->state       = DBS_IDLE;
    c->single_shot = single_shot;

    off_t remain = file_size - start_off;
    if (remain < 0) remain = 0;

    if (single_shot) {
        /* Allocate exactly what we need — no second buffer. */
        size_t exact = ((size_t)remain + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
        if (posix_memalign((void **)&c->buf[0], ODIRECT_ALIGN, exact) != 0)
            return -ENOMEM;
        c->buf[1] = NULL;
    } else {
        c->buf[0] = od_alloc_buf();
        c->buf[1] = od_alloc_buf();
        if (!c->buf[0] || !c->buf[1]) {
            free(c->buf[0]); free(c->buf[1]);
            return -ENOMEM;
        }
    }

    pthread_mutex_init(&c->lock, NULL);
    pthread_cond_init(&c->prefetch_needed, NULL);
    pthread_cond_init(&c->prefetch_done,   NULL);

    /* Fill buf[0] synchronously. */
    size_t wanta;
    if (single_shot) {
        wanta = ((size_t)remain + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
    } else {
        size_t want = (remain >= (off_t)odirect_buf_size) ? odirect_buf_size : (size_t)remain;
        wanta = (want + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
        if (wanta > odirect_buf_size) wanta = odirect_buf_size;
    }

    ssize_t got = pread(fd, c->buf[0], wanta, start_off);
    if (got < 0) {
        int e = errno;
        free(c->buf[0]); free(c->buf[1]);
        pthread_mutex_destroy(&c->lock);
        pthread_cond_destroy(&c->prefetch_needed);
        pthread_cond_destroy(&c->prefetch_done);
        return -e;
    }
    c->active_len = got;
    c->next_off   = start_off + (off_t)got;
    return 0;
}

/*
 * dbctx_kickoff: tell the worker to start the first async prefetch into
 * buf[inactive].  Call after pthread_create and before the main parse loop.
 */
static void dbctx_kickoff(DbCtx *c)
{
    pthread_mutex_lock(&c->lock);
    c->work_pending = 1;   /* level-triggered: safe even if worker not yet waiting */
    pthread_cond_signal(&c->prefetch_needed);
    pthread_mutex_unlock(&c->lock);
}

/*
 * dbctx_swap: main thread finished with buf[active].
 *   1. Wait until state == READY or DONE.
 *   2. Swap active ↔ inactive (main takes the filled buf, worker gets freed buf).
 *   3. Reset state to IDLE, signal worker to start the next prefetch.
 * Returns new active_len (0 = EOF/done), or negative errno on I/O error.
 */
static ssize_t dbctx_swap(DbCtx *c)
{
    /* Single-shot: whole file was in buf[0]; no worker, no more data. */
    if (c->single_shot) return 0;

    pthread_mutex_lock(&c->lock);

    /* Wait until worker has finished its current fill (READY or DONE). */
    while (c->state == DBS_IDLE || c->state == DBS_WORKING)
        pthread_cond_wait(&c->prefetch_done, &c->lock);

    /* Capture results. */
    int     err      = c->err;
    ssize_t next_len = c->next_len;
    DbState st       = c->state;

    if (err) {
        pthread_mutex_unlock(&c->lock);
        return -(ssize_t)err;
    }
    if (st == DBS_DONE || next_len == 0) {
        pthread_mutex_unlock(&c->lock);
        return 0;
    }

    /* Swap active ↔ inactive. */
    int old_active  = c->active;
    c->active       = c->inactive;
    c->inactive     = old_active;
    c->active_len   = next_len;

    /* Reset state to IDLE and give the worker a new work ticket. */
    c->state        = DBS_IDLE;
    c->work_pending = 1;
    pthread_cond_signal(&c->prefetch_needed);

    pthread_mutex_unlock(&c->lock);
    return next_len;
}

static void dbctx_destroy(DbCtx *c, pthread_t worker_tid)
{
    if (!c->single_shot) {
        pthread_mutex_lock(&c->lock);
        c->worker_quit  = 1;
        c->work_pending = 1;   /* ensure worker unblocks from its wait */
        pthread_cond_signal(&c->prefetch_needed);
        pthread_cond_signal(&c->prefetch_done);
        pthread_mutex_unlock(&c->lock);

        pthread_join(worker_tid, NULL);
    }

    free(c->buf[0]);
    free(c->buf[1]);
    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->prefetch_needed);
    pthread_cond_destroy(&c->prefetch_done);
}

/* ── Field-value load helpers (BE decoders) ── */
static inline int od_varchar_content_max(const uint8_t *p) {
    int len = ((int)p[0] << 8) | (int)p[1];
    return len < 0 ? 0 : len;
}
static inline int64_t od_load_i64_be(const uint8_t *p) {
    return (int64_t)((uint64_t)p[0] << 56 | (uint64_t)p[1] << 48 |
                     (uint64_t)p[2] << 40 | (uint64_t)p[3] << 32 |
                     (uint64_t)p[4] << 24 | (uint64_t)p[5] << 16 |
                     (uint64_t)p[6] << 8  | p[7]);
}
static inline int64_t od_load_i32_be(const uint8_t *p) {
    uint32_t v = (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 |
                 (uint32_t)p[2] << 8 | p[3];
    return (int64_t)(int32_t)v;
}
static inline int64_t od_load_i16_be(const uint8_t *p) {
    uint16_t v = (uint16_t)p[0] << 8 | p[1];
    return (int64_t)(int16_t)v;
}
static inline double od_load_f64(const uint8_t *p) {
    double v; memcpy(&v, p, 8); return v;
}
static inline float od_load_f32_le(const uint8_t *p) {
    float v; memcpy(&v, p, 4); return v;
}

/* ============================================================
 * seg_scan_o_direct — variable-length O_DIRECT scan
 * ========================================================= */

/* Max carry buffer for a single variable-length record (256 KB).
   Only used at chunk boundaries where one record straddles two chunks. */
#define OD_VARLEN_CARRY_SIZE (256 * 1024)

static inline size_t od_varlen_rec_size(uint16_t klen, uint32_t vlen) {
    size_t raw = 24 + (size_t)klen + (size_t)vlen;
    return (raw + 7) & ~(size_t)7;
}

/* Standalone resync search: reads through the caller's scan fd (which the
   caller has made quiescent — the prefetch worker is joined — and buffered
   via od_disable_odirect before calling), entirely independent of any live
   DbCtx/O_DIRECT scan state, so a failure here leaves the caller's current
   scan context completely untouched and its existing teardown path works
   unmodified. Reads a bounded window starting at the 8-byte-aligned floor
   of desync_off and looks for the next structurally valid, hash-verified
   record header via seg_scan_varlen_resync(). Returns 0 and sets
   *out_resume_off on success; 1 if the bounded window has no such record;
   -1 on I/O or allocation failure. A flag==0 scan hit may treat the 1
   result as the ordinary sparse tail; every other desync must treat it as
   an unrecoverable scan failure and must not delete or otherwise trust the
   file beyond desync_off. */
static int od_varlen_resync_find(int fd, const char *seg_path, off_t file_size,
                                  size_t max_slot_size, off_t desync_off,
                                  off_t *out_resume_off)
{
    off_t aligned_off = desync_off & ~(off_t)7;
    if (aligned_off < 0) aligned_off = 0;
    if (aligned_off >= file_size) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "od_varlen_resync_find: %s desync offset %lld at/past EOF (%lld)",
                  seg_path, (long long)desync_off, (long long)file_size);
        return -1;
    }

    size_t window = max_slot_size;
    off_t remain = file_size - aligned_off;
    if ((off_t)window > remain) window = (size_t)remain;
    size_t read_cap = (max_slot_size > (size_t)remain - window)
        ? (size_t)remain : window + max_slot_size;

    uint8_t *buf = malloc(read_cap);
    if (!buf) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "od_varlen_resync_find: %s OOM allocating %zu-byte resync buffer",
                  seg_path, read_cap);
        return -1;
    }

    size_t got_total = 0;
    while (got_total < read_cap) {
        ssize_t got = pread(fd, buf + got_total, read_cap - got_total,
                             aligned_off + (off_t)got_total);
        if (got < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "od_varlen_resync_find: %s pread failed at %lld: %s",
                      seg_path, (long long)(aligned_off + (off_t)got_total),
                      strerror(errno));
            free(buf);
            return -1;
        }
        if (got == 0) break;
        got_total += (size_t)got;
    }

    size_t next;
    size_t search_from = (desync_off > aligned_off)
        ? (size_t)(desync_off - aligned_off) : 0;
    int found = (search_from < got_total) &&
                seg_scan_varlen_resync(buf, got_total, search_from,
                                       max_slot_size,
                                       got_total - search_from < window
                                           ? got_total - search_from : window,
                                       &next);
    free(buf);

    if (!found) {
        LOG_DEBUG(LOG_SUB_SLOTCASK,
                  "od_varlen_resync_find: %s no valid record found within "
                  "%zu-byte per-object window starting at %lld",
                  seg_path, window, (long long)aligned_off);
        return 1;
    }

    *out_resume_off = aligned_off + (off_t)next;
    return 0;
}

int seg_scan_o_direct(const char *seg_path, size_t max_slot_size,
                              od_record_cb cb, void *ctx)
{
    if (!seg_path || max_slot_size < 32 || !cb) return -EINVAL;

    if (odirect_buf_size == 0) odirect_init_buf_size();

    /* Open first, then fstat the fd: seg_path is resolved exactly once per
       scan, so the file that bounds the scan (file_size) is the same inode
       the scan reads.  (CodeQL CWE-367: stat-then-open on the same path.) */
    int fd = od_open(seg_path);
    if (fd < 0) return -errno;

    struct stat st;
    if (fstat(fd, &st) != 0) {
        int e = errno;
        close(fd);
        return -e;
    }
    off_t file_size = st.st_size;
    if (file_size == 0) { close(fd); return 0; }

    int single_shot = (file_size <= (off_t)odirect_buf_size);

    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size, 0, single_shot);
    if (rc != 0) { close(fd); return rc; }

    size_t carry_cap = OD_VARLEN_CARRY_SIZE;
    uint8_t *carry = malloc(carry_cap);
    if (!carry) {
        if (single_shot) free(dc.buf[0]);
        else { free(dc.buf[0]); free(dc.buf[1]); }
        pthread_mutex_destroy(&dc.lock);
        pthread_cond_destroy(&dc.prefetch_needed);
        pthread_cond_destroy(&dc.prefetch_done);
        close(fd);
        return -ENOMEM;
    }
    int carry_len = 0;
    off_t base_off = 0;
    off_t resync_from = 0;

    pthread_t worker_tid = (pthread_t)0;
    if (!single_shot) {
        int e2;
        if (pthread_create(&worker_tid, NULL, prefetch_worker, &dc) != 0) {
            e2 = errno;
            free(carry);
            free(dc.buf[0]); free(dc.buf[1]);
            pthread_mutex_destroy(&dc.lock);
            pthread_cond_destroy(&dc.prefetch_needed);
            pthread_cond_destroy(&dc.prefetch_done);
            close(fd);
            return -e2;
        }
        dbctx_kickoff(&dc);
    }

    int ret = 0;
    int padding_desync = 0;

scan_top:
    for (;;) {
        ssize_t chunk_len = dc.active_len;
        if (chunk_len <= 0) {
            if (chunk_len < 0) ret = (int)chunk_len;
            break;
        }

        uint8_t *chunk = dc.buf[dc.active];
        size_t   pos   = 0;

        /* Reassemble a record that straddled the previous chunk boundary. */
        if (carry_len > 0) {
            off_t rec_start_off = base_off - (off_t)carry_len;
            /* Stage 1: ensure we have the 24-byte header in carry. */
            if (carry_len < 24) {
                int need = 24 - carry_len;
                if ((ssize_t)need > chunk_len) {
                    size_t need_cap = (size_t)(carry_len + chunk_len);
                    if (need_cap > carry_cap) {
                        uint8_t *nc = realloc(carry, need_cap);
                        if (!nc) { ret = -ENOMEM; goto done; }
                        carry = nc; carry_cap = need_cap;
                    }
                    memcpy(carry + carry_len, chunk, (size_t)chunk_len);
                    carry_len += (int)chunk_len;
                    goto next_chunk;
                }
                memcpy(carry + carry_len, chunk, (size_t)need);
                pos += (size_t)need;
                carry_len = 24;
            }

            /* Stage 2: carry has 24-byte header; complete the record. */
            uint16_t klen;
            uint32_t vlen;
            uint8_t  flag;
            memcpy(&klen, carry + 16, 2);
            memcpy(&vlen, carry + 20, 4);
            flag = carry[18];
            size_t rec_size = od_varlen_rec_size(klen, (uint32_t)vlen);

            /* flag==0 is sparse/pool-reuse padding, not a real record
               start for this scanner. A flag outside 0/1/2, or a
               rec_size (derived from an on-disk, unvalidated vlen) past
               the largest legitimate record, likewise means this offset
               isn't a real record start. Otherwise a reused-slot gap or
               corrupted header could either narrow into a huge/negative
               `need` below (CID 1696466) or walk `pos` through garbage.
               Resync forward instead of aborting the whole scan. */
            if (flag == 0 || flag > 2 || rec_size > max_slot_size) {
                padding_desync = (flag == 0);
                resync_from = rec_start_off;
                goto do_resync;
            }

            int need = (int)rec_size - carry_len;
            if (need > 0) {
                if ((ssize_t)need > chunk_len) {
                    size_t need_cap = (size_t)(carry_len + chunk_len);
                    if (need_cap > carry_cap) {
                        uint8_t *nc = realloc(carry, need_cap);
                        if (!nc) { ret = -ENOMEM; goto done; }
                        carry = nc; carry_cap = need_cap;
                    }
                    memcpy(carry + carry_len, chunk, (size_t)chunk_len);
                    carry_len += (int)chunk_len;
                    goto next_chunk;
                }
                if (rec_size > carry_cap) {
                    uint8_t *nc = realloc(carry, rec_size);
                    if (!nc) { ret = -ENOMEM; goto done; }
                    carry = nc; carry_cap = rec_size;
                }
                memcpy(carry + carry_len, chunk, (size_t)need);
                pos   += (size_t)need;
            }

            if (flag == 1) {
                if (cb(carry, (size_t)vlen, carry, ctx) != 0) {
                    ret = 1; goto done;
                }
            }
            carry_len = 0;
        }

        /* Stride through whole records in this chunk. */
        while (pos + 24 <= (size_t)chunk_len) {
            uint8_t *rec  = chunk + pos;
            uint8_t  flag = rec[18];
            uint16_t klen;
            uint32_t vlen;
            memcpy(&klen, rec + 16, 2);
            memcpy(&vlen, rec + 20, 4);
            size_t rec_size = od_varlen_rec_size(klen, (uint32_t)vlen);

            if (flag == 0 || flag > 2 || rec_size > max_slot_size) {
                /* Cheap in-buffer fast path: the vast majority of gaps
                   (undersized-record padding tails, tiny reused-slot
                   remnants) are fully contained in the 32 MiB chunk
                   already resident here, with the next real record only
                   a handful of bytes further in. Search this buffer
                   directly via the same primitive the slow path uses
                   (seg_scan_varlen_resync) before paying for a full
                   prefetch-context teardown/rebuild (dbctx_destroy +
                   pread probe + dbctx_init [+ pthread_create]) via
                   do_resync below. Only genuinely unresolvable gaps —
                   one that runs past the end of this chunk, or actual
                   corruption — fall through to the slow path, which is
                   unchanged. */
                size_t remain = (size_t)chunk_len - pos;
                size_t window = (remain < max_slot_size) ? remain : max_slot_size;
                size_t next_in_chunk;
                if (seg_scan_varlen_resync(chunk, (size_t)chunk_len, pos,
                                           max_slot_size, window,
                                           &next_in_chunk)) {
                    pos = next_in_chunk;
                    continue;
                }
                padding_desync = (flag == 0);
                resync_from = base_off + (off_t)pos;
                goto do_resync;
            }

            if (pos + rec_size > (size_t)chunk_len) {
                /* Record straddles chunk boundary — save tail in carry. */
                break;
            }

            if (flag == 1) {
                if (cb(rec, (size_t)vlen, rec, ctx) != 0) {
                    ret = 1; goto done;
                }
            }
            pos += rec_size;
        }

        /* Save any partial bytes at the chunk tail. */
        if (pos < (size_t)chunk_len) {
            size_t tail = (size_t)chunk_len - pos;
            if (tail > carry_cap) {
                uint8_t *nc = realloc(carry, tail);
                if (!nc) { ret = -ENOMEM; goto done; }
                carry = nc; carry_cap = tail;
            }
            carry_len = (int)tail;
            memcpy(carry, chunk + pos, tail);
        }

next_chunk:
        base_off += chunk_len;
        {
            ssize_t next = dbctx_swap(&dc);
            if (next < 0) { ret = (int)next; goto done; }
            if (next == 0) {
                dc.active_len = 0;
                break;
            }
        }
    }

    goto done;

do_resync:
    {
        off_t resume_off;

        /* Tear the scan context down first so the fd is quiescent: joining
           the prefetch worker guarantees no concurrent pread can touch it
           while the resync probe and the restarted scan use it. */
        dbctx_destroy(&dc, worker_tid);

        /* `resume_off` (and the resync probe's own window reads) is
           guaranteed only to be 8-byte aligned (record alignment), not
           aligned to the device sector required by O_DIRECT.  Drop O_DIRECT
           in place instead of closing and re-opening the path: the fd pins
           this file's inode, so the path is never re-resolved and a file
           swapped out from under us between the original open and here
           cannot change what the scan reads.  The normal scan still uses
           od_open/O_DIRECT; this bounded fallback avoids turning every
           legitimate resync into EINVAL. */
        od_disable_odirect(fd);

        int rrc = od_varlen_resync_find(fd, seg_path, file_size,
                                        max_slot_size, resync_from,
                                        &resume_off);
        if (rrc != 0) {
            /* A flag==0 header with no later real record is the normal
               sparse tail; an I/O/allocation failure is still an error.
               Non-padding desyncs are never silently truncated. */
            ret = (rrc > 0 && padding_desync) ? 0 : -EIO;
            if (rrc > 0 && !padding_desync)
                LOG_ERROR(LOG_SUB_SLOTCASK,
                          "od_varlen_resync_find: %s unrecoverable desync at "
                          "%lld: no valid record found within %zu-byte "
                          "per-object window",
                          seg_path, (long long)resync_from, max_slot_size);
            close(fd);
            free(carry);
            return ret;
        }

        single_shot = ((file_size - resume_off) <= (off_t)odirect_buf_size);
        rc = dbctx_init(&dc, fd, file_size, resume_off, single_shot);
        if (rc != 0) {
            ret = rc;
            close(fd);
            free(carry);
            return ret;
        }

        carry_len = 0;
        base_off = resume_off;

        if (!single_shot) {
            int e2;
            if (pthread_create(&worker_tid, NULL, prefetch_worker, &dc) != 0) {
                e2 = errno;
                free(dc.buf[0]); free(dc.buf[1]);
                pthread_mutex_destroy(&dc.lock);
                pthread_cond_destroy(&dc.prefetch_needed);
                pthread_cond_destroy(&dc.prefetch_done);
                close(fd);
                free(carry);
                return -e2;
            }
            dbctx_kickoff(&dc);
        } else {
            worker_tid = (pthread_t)0;
        }

        ret = 0;
        goto scan_top;
    }

done:
    dbctx_destroy(&dc, worker_tid);
    free(carry);
    close(fd);
    return ret;
}
static inline uint16_t bts_data_len(const uint8_t *e)
{
    uint16_t v; memcpy(&v, e, 2); return v;
}
static inline uint8_t bts_prefix_len(const uint8_t *e) { return e[2]; }
static inline const uint8_t *bts_suffix(const uint8_t *e) { return e + 3; }
static inline size_t bts_suffix_len(const uint8_t *e)
{
    return (size_t)bts_data_len(e) - 1 - (size_t)BT_HASH_SIZE;
}
static inline const uint8_t *bts_hash(const uint8_t *e)
{
    return e + 2 + (size_t)bts_data_len(e) - (size_t)BT_HASH_SIZE;
}
static inline int bts_is_tomb(const uint8_t *e)
{
    return (bts_prefix_len(e) & 0x80) != 0;
}
static inline uint16_t bts_slot_off(const uint8_t *page, uint32_t s)
{
    uint16_t off;
    memcpy(&off, page + (size_t)BT_PAGE_DATA_START + (size_t)s * 2, 2);
    return off;
}

static int btree_decode_leaves_in_range(const uint8_t *range, size_t range_len,
                                        off_t start_off, int page_sz,
                                        od_leaf_cb cb, void *ctx, int *stop_out)
{
    size_t off = 0;
    /* Skip the file header on chunk 0 — page 0 holds BtFileHeader, not a
     * BtPageHeader.  Everywhere else, every aligned page_sz-sized window
     * starts with a BtPageHeader. */
    if (start_off == 0 && range_len >= sizeof(BtFileHeader)) {
        const BtFileHeader *fh = (const BtFileHeader *)range;
        uint32_t magic; memcpy(&magic, &fh->magic, 4);
        if (magic != BT_MAGIC) return -EINVAL;
        uint64_t ec; memcpy(&ec, &fh->entry_count, 8);
        if (ec == 0) { *stop_out = 1; return 0; }   /* empty btree → done */
        off = (size_t)page_sz;                       /* skip page 0 (header) */
    }

    char   key_buf[BT_MAX_VAL_LEN];
    size_t key_len = 0;

    while (off + (size_t)page_sz <= range_len) {
        const uint8_t *pg = range + off;
        const BtPageHeader *ph = (const BtPageHeader *)pg;
        uint32_t ptype; memcpy(&ptype, &ph->page_type, 4);

        /* Skip internal nodes (ptype==0) and any non-page bytes.
         * Only leaves (ptype==1) carry user entries. */
        if (ptype == 1) {
            uint32_t cnt; memcpy(&cnt, &ph->count, 4);
            /* cnt is read straight from an on-disk page header; clamp it
               to the maximum number of 2-byte slot-table entries that can
               possibly fit after the page header before iterating, so a
               corrupted cnt can't walk bts_slot_off() past the page
               (CID 1696431). */
            size_t max_slots = ((size_t)page_sz > BT_PAGE_DATA_START)
                                    ? ((size_t)page_sz - BT_PAGE_DATA_START) / 2
                                    : 0;
            if (cnt > max_slots) cnt = (uint32_t)max_slots;

            for (uint32_t s = 0; s < cnt; s++) {
                uint16_t eoff = bts_slot_off(pg, s);
                if ((size_t)eoff + 3 > (size_t)page_sz) break;  /* corrupt */
                const uint8_t *e = pg + eoff;
                uint8_t  plen = bts_prefix_len(e);
                size_t   slen = bts_suffix_len(e);

                if ((s & (uint32_t)(BT_LEAF_RESTART_K - 1)) == 0) {
                    key_len = slen;
                    if (key_len > BT_MAX_VAL_LEN) key_len = BT_MAX_VAL_LEN;
                    memcpy(key_buf, bts_suffix(e), key_len);
                } else {
                    size_t klen = (size_t)(plen & 0x7f) + slen;
                    if (klen > BT_MAX_VAL_LEN) klen = BT_MAX_VAL_LEN;
                    size_t take = (klen > (size_t)(plen & 0x7f))
                                      ? (klen - (size_t)(plen & 0x7f)) : 0;
                    memcpy(key_buf + (plen & 0x7f), bts_suffix(e), take);
                    key_len = klen;
                }

                if (bts_is_tomb(e)) continue;

                const uint8_t *hash16 = bts_hash(e);
                if (cb((const uint8_t *)key_buf, key_len, hash16, ctx) != 0) {
                    *stop_out = 1;
                    return 0;
                }
            }
        }
        /* page_type==0 (internal) and unrecognised types are silently skipped. */

        off += (size_t)page_sz;
    }
    return 0;
}

int btree_leaf_scan_o_direct(const char *btree_path,
                             od_leaf_cb cb, void *ctx)
{
    if (!btree_path || !cb) return -EINVAL;

    struct stat st;
    if (stat(btree_path, &st) != 0) return -errno;
    off_t file_size = st.st_size;

    int page_sz = bt_page_size;
    if (page_sz < 512) page_sz = 4096;
    if (file_size < (off_t)page_sz) return 0;

    int fd = od_open(btree_path);
    if (fd < 0) return -errno;

    /* Stream-decode strategy.
     *
     * For unordered leaf scans (the only caller — btree_dispatch's
     * default branch for contains/like-substring/ends/regex/len_X/
     * exists), we don't need to follow the leaf chain in sort order.
     * Every leaf page in the file is independently decodable (prefix
     * compression is leaf-local), so we can walk the file linearly,
     * processing each leaf as it arrives in the double-buffered
     * prefetch.
     *
     * The previous "gather every chunk into chunks[] then decode"
     * pattern allocated file_size bytes of RAM upfront (300 MB for
     * a 300 MB index) and serialised I/O with decode, defeating the
     * whole point of the double-buffer.  Streaming uses O(2 buffers)
     * = 8 MB total and pipelines I/O with decode (worker preads
     * chunk N+1 while main decodes chunk N).
     *
     * Carry handling: pages are smaller than ODIRECT_BUF_SIZE (page_sz
     * = 4 KB typical, buffer = 4 MB), so at most one page straddles
     * each chunk boundary.  We stash the tail bytes and prepend to
     * next chunk before parsing.  Same pattern as seg_scan_o_direct.
     */
    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size, 0, 0);
    if (rc != 0) { close(fd); return rc; }

    uint8_t *carry = malloc((size_t)page_sz);
    if (!carry) {
        free(dc.buf[0]); free(dc.buf[1]);
        pthread_mutex_destroy(&dc.lock);
        pthread_cond_destroy(&dc.prefetch_needed);
        pthread_cond_destroy(&dc.prefetch_done);
        close(fd);
        return -ENOMEM;
    }
    int carry_len = 0;

    pthread_t worker_tid;
    if (pthread_create(&worker_tid, NULL, prefetch_worker, &dc) != 0) {
        int e = errno;
        free(carry);
        free(dc.buf[0]); free(dc.buf[1]);
        pthread_mutex_destroy(&dc.lock);
        pthread_cond_destroy(&dc.prefetch_needed);
        pthread_cond_destroy(&dc.prefetch_done);
        close(fd);
        return -e;
    }

    dbctx_kickoff(&dc);

    int ret = 0;
    int stop = 0;
    off_t chunk_off = 0;       /* byte offset of dc.buf[dc.active] in file */

    for (;;) {
        ssize_t chunk_len = dc.active_len;
        if (chunk_len <= 0) {
            if (chunk_len < 0) ret = (int)chunk_len;
            break;
        }

        uint8_t *chunk = dc.buf[dc.active];
        size_t   pos   = 0;

        /* Reassemble a page that straddled the previous chunk boundary. */
        if (carry_len > 0) {
            int need = page_sz - carry_len;
            if ((ssize_t)need > chunk_len) {
                /* Whole chunk still not enough (impossible at page_sz <
                 * ODIRECT_BUF_SIZE, but defensive). */
                memcpy(carry + carry_len, chunk, (size_t)chunk_len);
                carry_len += (int)chunk_len;
                goto next_chunk;
            }
            memcpy(carry + carry_len, chunk, (size_t)need);
            pos = (size_t)need;
            carry_len = 0;

            /* Decode the now-complete page (carry buf). */
            int err = btree_decode_leaves_in_range(carry, (size_t)page_sz,
                                                   /*start_off=*/chunk_off - page_sz + need,
                                                   page_sz, cb, ctx, &stop);
            if (err) { ret = err; goto done; }
            if (stop) goto done;
        }

        /* Stride through full pages in this chunk.  The decoder skips
         * page 0 internally when start_off==0. */
        size_t whole_len = chunk_len - (chunk_len % (ssize_t)page_sz);
        if (whole_len > pos) {
            int err = btree_decode_leaves_in_range(chunk + pos, whole_len - pos,
                                                   chunk_off + (off_t)pos,
                                                   page_sz, cb, ctx, &stop);
            if (err) { ret = err; goto done; }
            if (stop) goto done;
            pos = whole_len;
        }

        /* Save any partial page at the chunk tail. */
        if (pos < (size_t)chunk_len) {
            carry_len = (int)((size_t)chunk_len - pos);
            memcpy(carry, chunk + pos, (size_t)carry_len);
        }

next_chunk:
        {
            chunk_off += (off_t)chunk_len;
            ssize_t next = dbctx_swap(&dc);
            if (next < 0) { ret = (int)next; goto done; }
            if (next == 0) {
                dc.active_len = 0;
                break;
            }
        }
    }

done:
    dbctx_destroy(&dc, worker_tid);
    free(carry);
    close(fd);
    return ret;
}
