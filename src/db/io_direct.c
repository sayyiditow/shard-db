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
static int dbctx_init(DbCtx *c, int fd, off_t file_size, int single_shot)
{
    memset(c, 0, sizeof(*c));
    c->fd          = fd;
    c->file_size   = file_size;
    c->active      = 0;
    c->inactive    = 1;
    c->state       = DBS_IDLE;
    c->single_shot = single_shot;

    if (single_shot) {
        /* Allocate exactly what we need — no second buffer. */
        size_t exact = ((size_t)file_size + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
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
    off_t  fsz  = file_size;
    size_t wanta;
    if (single_shot) {
        wanta = ((size_t)fsz + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
    } else {
        size_t want = (fsz >= (off_t)odirect_buf_size) ? odirect_buf_size : (size_t)fsz;
        wanta = (want + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
        if (wanta > odirect_buf_size) wanta = odirect_buf_size;
    }

    ssize_t got = pread(fd, c->buf[0], wanta, 0);
    if (got < 0) {
        int e = errno;
        free(c->buf[0]); free(c->buf[1]);
        pthread_mutex_destroy(&c->lock);
        pthread_cond_destroy(&c->prefetch_needed);
        pthread_cond_destroy(&c->prefetch_done);
        return -e;
    }
    c->active_len = got;
    c->next_off   = (off_t)got;
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
 * seg_scan_o_direct
 * ========================================================= */

/*
 * On-disk record header (24 bytes, from slotcask.c):
 *   [0..15]  hash16   — 16-byte xxh128
 *   [16..17] klen     — uint16_t little-endian
 *   [18]     flag     — 0=empty, 1=live, 2=tombstone
 *   [19]     reserved
 *   [20..23] vlen     — uint32_t little-endian
 *   [24..]   key bytes, then value bytes, then zero-pad to slot_size
 *
 * Seg files are flat arrays of fixed-size slot_size records.  We stride
 * through each chunk by slot_size, examining each record's flag byte.
 *
 * Slot-spanning-chunk-boundary case: slot_size <= a few KB << ODIRECT_BUF_SIZE,
 * so at most one slot straddles each boundary.  We carry the tail into a
 * small stack buffer and prepend it to the next chunk.
 */

int seg_scan_o_direct(const char *seg_path, int slot_size,
                      od_record_cb cb, void *ctx)
{
    if (!seg_path || slot_size < 32 || !cb) return -EINVAL;

    if (odirect_buf_size == 0) odirect_init_buf_size();

    struct stat st;
    if (stat(seg_path, &st) != 0) return -errno;
    off_t file_size = st.st_size;
    if (file_size == 0) return 0;

    /* Single-shot when the shard fits in one read — no prefetch thread needed. */
    int single_shot = (file_size <= (off_t)odirect_buf_size);

    int fd = od_open(seg_path);
    if (fd < 0) return -errno;

    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size, single_shot);
    if (rc != 0) { close(fd); return rc; }

    /* Carry buffer: holds partial slot at a chunk boundary. */
    uint8_t *carry = malloc((size_t)slot_size);
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

    pthread_t worker_tid = (pthread_t)0;
    if (!single_shot) {
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

        /* Kick off the first async prefetch of buf[1]. */
        dbctx_kickoff(&dc);
    }

    int ret = 0;

    /* Main loop: parse active chunk, then swap. */
    for (;;) {
        ssize_t chunk_len = dc.active_len;
        if (chunk_len <= 0) {
            if (chunk_len < 0) ret = (int)chunk_len;
            break;
        }

        uint8_t *chunk = dc.buf[dc.active];
        size_t   pos   = 0;

        /* Reassemble a slot that straddled the previous chunk boundary. */
        if (carry_len > 0) {
            int need = slot_size - carry_len;
            if ((ssize_t)need > chunk_len) {
                /* Whole chunk still not enough (shouldn't happen at 4MB). */
                memcpy(carry + carry_len, chunk, (size_t)chunk_len);
                carry_len += (int)chunk_len;
                goto next_chunk;
            }
            memcpy(carry + carry_len, chunk, (size_t)need);
            pos       = (size_t)need;
            carry_len = 0;

            uint8_t flag = carry[18];
            if (flag == 1) {
                uint32_t vlen;
                memcpy(&vlen, carry + 20, 4);
                if (cb(carry, (size_t)vlen, carry, ctx) != 0) {
                    ret = 1; goto done;
                }
            }
        }

        /* Stride through whole slots in this chunk. */
        while ((ssize_t)(pos + (size_t)slot_size) <= chunk_len) {
            uint8_t *rec  = chunk + pos;
            uint8_t  flag = rec[18];
            if (flag == 1) {
                uint32_t vlen;
                memcpy(&vlen, rec + 20, 4);
                if (cb(rec, (size_t)vlen, rec, ctx) != 0) {
                    ret = 1; goto done;
                }
            }
            /* flag 0 (empty) or 2 (tombstone): skip */
            pos += (size_t)slot_size;
        }

        /* Save any partial slot at the chunk tail. */
        if (pos < (size_t)chunk_len) {
            carry_len = (int)((size_t)chunk_len - pos);
            memcpy(carry, chunk + pos, (size_t)carry_len);
        }

next_chunk:
        {
            ssize_t next = dbctx_swap(&dc);
            if (next < 0) { ret = (int)next; goto done; }
            if (next == 0) {
                /* EOF — no more data. dbctx_swap did NOT update active_len
                   on the DONE path; force it to 0 so the top-of-loop check
                   exits cleanly rather than re-parsing the old buffer. */
                dc.active_len = 0;
                break;
            }
            /* dc.active and dc.active_len updated by swap for next_len > 0. */
        }
    }

done:
    dbctx_destroy(&dc, worker_tid);
    free(carry);
    close(fd);
    return ret;
}

/* ============================================================
 * seg_scan_o_direct_varlen — variable-length O_DIRECT scan
 * ========================================================= */

/* Max carry buffer for a single variable-length record (256 KB).
   Only used at chunk boundaries where one record straddles two chunks. */
#define OD_VARLEN_CARRY_SIZE (256 * 1024)

static inline size_t od_varlen_rec_size(uint16_t klen, uint32_t vlen) {
    size_t raw = 24 + (size_t)klen + (size_t)vlen;
    return (raw + 7) & ~(size_t)7;
}

int seg_scan_o_direct_varlen(const char *seg_path,
                              od_record_cb cb, void *ctx)
{
    if (!seg_path || !cb) return -EINVAL;

    if (odirect_buf_size == 0) odirect_init_buf_size();

    struct stat st;
    if (stat(seg_path, &st) != 0) return -errno;
    off_t file_size = st.st_size;
    if (file_size == 0) return 0;

    int single_shot = (file_size <= (off_t)odirect_buf_size);

    int fd = od_open(seg_path);
    if (fd < 0) return -errno;

    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size, single_shot);
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
            /* Stage 1: ensure we have the 24-byte header in carry. */
            if (carry_len < 24) {
                int need = 24 - carry_len;
                if ((ssize_t)need > chunk_len) {
                    /* Still not enough — stay in carry. */
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

            /* rec_size is derived from an on-disk, unvalidated vlen.
               A corrupted vlen can make rec_size enormous; narrowing
               it into `int` below would silently wrap and produce a
               small or negative `need`, skipping the "need more data"
               branch and passing a huge vlen straight to cb() against
               the small carry buffer (CID 1696466). Reject anything
               past the largest a legitimate segment record could be. */
            if (rec_size > SLOTCASK_SEG_MAX_BYTES) {
                ret = -EIO;
                goto done;
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
        {
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

/* ============================================================
 * seg_scan_o_direct_match — inline-match O_DIRECT scan
 * ========================================================= */

/*
 * Zero-callback variant: walks live slots, extracts the value, and
 * calls match_typed() or an inlined per-type matcher directly —
 * no function-pointer indirection. Counts matches in *out_count.
 *
 * On-disk record layout:
 *   [0..15]  hash16   — 16-byte xxh128
 *   [16..17] klen     — uint16_t little-endian
 *   [18]     flag     — 0=empty, 1=live, 2=tombstone
 *   [19]     reserved
 *   [20..23] vlen     — uint32_t little-endian
 *   [24..]   key bytes, then value bytes, then zero-pad to slot_size
 *
 * Same double-buffer prefetch pattern as seg_scan_o_direct.
 * QueryDeadline is checked once per chunk.
 * Returns 0 on success, -errno on I/O error, -ETIMEDOUT on timeout.
 * *out_count is always set (may be non-zero even on error).
 */

int seg_scan_o_direct_match(const char *seg_path, int slot_size,
                             FieldSchema *fs,
                             const CompiledCriterion *single_cc,
                             const CriteriaNode *tree,
                             QueryDeadline *dl,
                             int64_t *out_count)
{
    if (!seg_path || slot_size < 32) return -EINVAL;
    if (odirect_buf_size == 0) odirect_init_buf_size();

    struct stat st;
    if (stat(seg_path, &st) != 0) return -errno;
    off_t file_size = st.st_size;
    if (file_size == 0) { *out_count = 0; return 0; }

    int single_shot = (file_size <= (off_t)odirect_buf_size);

    int fd = od_open(seg_path);
    if (fd < 0) return -errno;

    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size, single_shot);
    if (rc != 0) { close(fd); return rc; }

    uint8_t *carry = malloc((size_t)slot_size);
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

    pthread_t worker_tid = (pthread_t)0;
    if (!single_shot) {
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
    }

    int ret = 0;
    int64_t local_count = 0;
    int more = 1;
    FieldSchema *fs_mut = fs;

    /* ── Pre-loop setup: hoist all single_cc fields into locals ── */
    int field_offset = 0;
    int rhs_offset = 0;
    int varchar_cmax = 0;
    const char *s1 = NULL;
    const char *needle = NULL;
    size_t s1_len = 0;
    size_t needle_len = 0;
    int64_t i1 = 0, i2 = 0;
    double d1 = 0.0, d2 = 0.0;
    int8_t min_exc = 0, max_exc = 0;
    const int64_t *in_i64 = NULL;
    const double  *in_f64 = NULL;
    uint16_t in_count = 0;

    enum {
        MATCH_FALLBACK = 0,
        MATCH_VARCHAR_EQ,
        MATCH_VARCHAR_NEQ,
        MATCH_VARCHAR_CONTAINS,
        MATCH_VARCHAR_NOT_CONTAINS,
        MATCH_VARCHAR_STARTS,
        MATCH_VARCHAR_ENDS,
        MATCH_VARCHAR_EXISTS,
        MATCH_VARCHAR_NOT_EXISTS,
        MATCH_VARCHAR_LEN_EQ,
        MATCH_VARCHAR_LEN_NEQ,
        MATCH_VARCHAR_LEN_LESS,
        MATCH_VARCHAR_LEN_GREATER,
        MATCH_VARCHAR_LEN_LESS_EQ,
        MATCH_VARCHAR_LEN_GREATER_EQ,
        MATCH_VARCHAR_LEN_BETWEEN,
        MATCH_INT64_EQ, MATCH_INT64_NEQ, MATCH_INT64_LESS, MATCH_INT64_GREATER,
        MATCH_INT64_LESS_EQ, MATCH_INT64_GREATER_EQ, MATCH_INT64_BETWEEN,
        MATCH_INT64_IN, MATCH_INT64_NOT_IN,
        MATCH_INT32_EQ, MATCH_INT32_NEQ, MATCH_INT32_LESS, MATCH_INT32_GREATER,
        MATCH_INT32_LESS_EQ, MATCH_INT32_GREATER_EQ, MATCH_INT32_BETWEEN,
        MATCH_INT32_IN, MATCH_INT32_NOT_IN,
        MATCH_SHORT_EQ, MATCH_SHORT_NEQ, MATCH_SHORT_LESS, MATCH_SHORT_GREATER,
        MATCH_SHORT_LESS_EQ, MATCH_SHORT_GREATER_EQ, MATCH_SHORT_BETWEEN,
        MATCH_SHORT_IN, MATCH_SHORT_NOT_IN,
        MATCH_BYTE_EQ, MATCH_BYTE_NEQ, MATCH_BYTE_LESS, MATCH_BYTE_GREATER,
        MATCH_BYTE_LESS_EQ, MATCH_BYTE_GREATER_EQ, MATCH_BYTE_BETWEEN,
        MATCH_BYTE_IN, MATCH_BYTE_NOT_IN,
        MATCH_DOUBLE_EQ, MATCH_DOUBLE_NEQ, MATCH_DOUBLE_LESS, MATCH_DOUBLE_GREATER,
        MATCH_DOUBLE_LESS_EQ, MATCH_DOUBLE_GREATER_EQ, MATCH_DOUBLE_BETWEEN,
        MATCH_FLOAT_EQ, MATCH_FLOAT_NEQ, MATCH_FLOAT_LESS, MATCH_FLOAT_GREATER,
        MATCH_FLOAT_LESS_EQ, MATCH_FLOAT_GREATER_EQ, MATCH_FLOAT_BETWEEN,
        MATCH_FLOAT_IN, MATCH_FLOAT_NOT_IN,

        /* ── Field-vs-field inline matchers ── */
        MATCH_FIELD_VARCHAR_EQ, MATCH_FIELD_VARCHAR_NEQ,
        MATCH_FIELD_VARCHAR_LT, MATCH_FIELD_VARCHAR_GT,
        MATCH_FIELD_VARCHAR_LTE, MATCH_FIELD_VARCHAR_GTE,

        MATCH_FIELD_INT64_EQ, MATCH_FIELD_INT64_NEQ,
        MATCH_FIELD_INT64_LT, MATCH_FIELD_INT64_GT,
        MATCH_FIELD_INT64_LTE, MATCH_FIELD_INT64_GTE,

        MATCH_FIELD_INT32_EQ, MATCH_FIELD_INT32_NEQ,
        MATCH_FIELD_INT32_LT, MATCH_FIELD_INT32_GT,
        MATCH_FIELD_INT32_LTE, MATCH_FIELD_INT32_GTE,

        MATCH_FIELD_SHORT_EQ, MATCH_FIELD_SHORT_NEQ,
        MATCH_FIELD_SHORT_LT, MATCH_FIELD_SHORT_GT,
        MATCH_FIELD_SHORT_LTE, MATCH_FIELD_SHORT_GTE,

        MATCH_FIELD_DOUBLE_EQ, MATCH_FIELD_DOUBLE_NEQ,
        MATCH_FIELD_DOUBLE_LT, MATCH_FIELD_DOUBLE_GT,
        MATCH_FIELD_DOUBLE_LTE, MATCH_FIELD_DOUBLE_GTE,

        MATCH_FIELD_FLOAT_EQ, MATCH_FIELD_FLOAT_NEQ,
        MATCH_FIELD_FLOAT_LT, MATCH_FIELD_FLOAT_GT,
        MATCH_FIELD_FLOAT_LTE, MATCH_FIELD_FLOAT_GTE,

        MATCH_FIELD_BYTE_EQ, MATCH_FIELD_BYTE_NEQ,
        MATCH_FIELD_BYTE_LT, MATCH_FIELD_BYTE_GT,
        MATCH_FIELD_BYTE_LTE, MATCH_FIELD_BYTE_GTE,

        MATCH_FIELD_DATE_EQ, MATCH_FIELD_DATE_NEQ,
        MATCH_FIELD_DATE_LT, MATCH_FIELD_DATE_GT,
        MATCH_FIELD_DATE_LTE, MATCH_FIELD_DATE_GTE,

        MATCH_FIELD_MISMATCH,  /* rhs_tf == NULL (type mismatch) */

        MATCH_COUNT_ALL,
        MATCH_COUNT_NONE,
    } match_kind = MATCH_FALLBACK;

    if (single_cc && single_cc->tf) {
        field_offset = single_cc->tf->offset;

        /* Field-vs-field ops: inline when types match */
        int field_vs_field_op = (single_cc->op == OP_EQ_FIELD ||
                                 single_cc->op == OP_NEQ_FIELD ||
                                 single_cc->op == OP_LT_FIELD ||
                                 single_cc->op == OP_GT_FIELD ||
                                 single_cc->op == OP_LTE_FIELD ||
                                 single_cc->op == OP_GTE_FIELD);
        if (field_vs_field_op) {
            if (!single_cc->rhs_tf) {
                match_kind = MATCH_FIELD_MISMATCH;
            } else {
                rhs_offset = single_cc->rhs_tf->offset;
                switch (single_cc->tf->type) {
                case FT_VARCHAR:
                    varchar_cmax = single_cc->tf->size - 2;
                    switch (single_cc->op) {
                    case OP_EQ_FIELD:  match_kind = MATCH_FIELD_VARCHAR_EQ;  break;
                    case OP_NEQ_FIELD: match_kind = MATCH_FIELD_VARCHAR_NEQ; break;
                    case OP_LT_FIELD:  match_kind = MATCH_FIELD_VARCHAR_LT;  break;
                    case OP_GT_FIELD:  match_kind = MATCH_FIELD_VARCHAR_GT;  break;
                    case OP_LTE_FIELD: match_kind = MATCH_FIELD_VARCHAR_LTE; break;
                    case OP_GTE_FIELD: match_kind = MATCH_FIELD_VARCHAR_GTE; break;
                    default: break;
                    }
                    break;
                case FT_LONG: case FT_NUMERIC: case FT_TIMESTAMP:
                    switch (single_cc->op) {
                    case OP_EQ_FIELD:  match_kind = MATCH_FIELD_INT64_EQ;  break;
                    case OP_NEQ_FIELD: match_kind = MATCH_FIELD_INT64_NEQ; break;
                    case OP_LT_FIELD:  match_kind = MATCH_FIELD_INT64_LT;  break;
                    case OP_GT_FIELD:  match_kind = MATCH_FIELD_INT64_GT;  break;
                    case OP_LTE_FIELD: match_kind = MATCH_FIELD_INT64_LTE; break;
                    case OP_GTE_FIELD: match_kind = MATCH_FIELD_INT64_GTE; break;
                    default: break;
                    }
                    break;
                case FT_INT:
                    switch (single_cc->op) {
                    case OP_EQ_FIELD:  match_kind = MATCH_FIELD_INT32_EQ;  break;
                    case OP_NEQ_FIELD: match_kind = MATCH_FIELD_INT32_NEQ; break;
                    case OP_LT_FIELD:  match_kind = MATCH_FIELD_INT32_LT;  break;
                    case OP_GT_FIELD:  match_kind = MATCH_FIELD_INT32_GT;  break;
                    case OP_LTE_FIELD: match_kind = MATCH_FIELD_INT32_LTE; break;
                    case OP_GTE_FIELD: match_kind = MATCH_FIELD_INT32_GTE; break;
                    default: break;
                    }
                    break;
                case FT_SHORT:
                    switch (single_cc->op) {
                    case OP_EQ_FIELD:  match_kind = MATCH_FIELD_SHORT_EQ;  break;
                    case OP_NEQ_FIELD: match_kind = MATCH_FIELD_SHORT_NEQ; break;
                    case OP_LT_FIELD:  match_kind = MATCH_FIELD_SHORT_LT;  break;
                    case OP_GT_FIELD:  match_kind = MATCH_FIELD_SHORT_GT;  break;
                    case OP_LTE_FIELD: match_kind = MATCH_FIELD_SHORT_LTE; break;
                    case OP_GTE_FIELD: match_kind = MATCH_FIELD_SHORT_GTE; break;
                    default: break;
                    }
                    break;
                case FT_DOUBLE:
                    switch (single_cc->op) {
                    case OP_EQ_FIELD:  match_kind = MATCH_FIELD_DOUBLE_EQ;  break;
                    case OP_NEQ_FIELD: match_kind = MATCH_FIELD_DOUBLE_NEQ; break;
                    case OP_LT_FIELD:  match_kind = MATCH_FIELD_DOUBLE_LT;  break;
                    case OP_GT_FIELD:  match_kind = MATCH_FIELD_DOUBLE_GT;  break;
                    case OP_LTE_FIELD: match_kind = MATCH_FIELD_DOUBLE_LTE; break;
                    case OP_GTE_FIELD: match_kind = MATCH_FIELD_DOUBLE_GTE; break;
                    default: break;
                    }
                    break;
                case FT_FLOAT:
                    switch (single_cc->op) {
                    case OP_EQ_FIELD:  match_kind = MATCH_FIELD_FLOAT_EQ;  break;
                    case OP_NEQ_FIELD: match_kind = MATCH_FIELD_FLOAT_NEQ; break;
                    case OP_LT_FIELD:  match_kind = MATCH_FIELD_FLOAT_LT;  break;
                    case OP_GT_FIELD:  match_kind = MATCH_FIELD_FLOAT_GT;  break;
                    case OP_LTE_FIELD: match_kind = MATCH_FIELD_FLOAT_LTE; break;
                    case OP_GTE_FIELD: match_kind = MATCH_FIELD_FLOAT_GTE; break;
                    default: break;
                    }
                    break;
                case FT_BOOL: case FT_BYTE:
                    switch (single_cc->op) {
                    case OP_EQ_FIELD:  match_kind = MATCH_FIELD_BYTE_EQ;  break;
                    case OP_NEQ_FIELD: match_kind = MATCH_FIELD_BYTE_NEQ; break;
                    case OP_LT_FIELD:  match_kind = MATCH_FIELD_BYTE_LT;  break;
                    case OP_GT_FIELD:  match_kind = MATCH_FIELD_BYTE_GT;  break;
                    case OP_LTE_FIELD: match_kind = MATCH_FIELD_BYTE_LTE; break;
                    case OP_GTE_FIELD: match_kind = MATCH_FIELD_BYTE_GTE; break;
                    default: break;
                    }
                    break;
                case FT_DATE:
                    switch (single_cc->op) {
                    case OP_EQ_FIELD:  match_kind = MATCH_FIELD_DATE_EQ;  break;
                    case OP_NEQ_FIELD: match_kind = MATCH_FIELD_DATE_NEQ; break;
                    case OP_LT_FIELD:  match_kind = MATCH_FIELD_DATE_LT;  break;
                    case OP_GT_FIELD:  match_kind = MATCH_FIELD_DATE_GT;  break;
                    case OP_LTE_FIELD: match_kind = MATCH_FIELD_DATE_LTE; break;
                    case OP_GTE_FIELD: match_kind = MATCH_FIELD_DATE_GTE; break;
                    default: break;
                    }
                    break;
                default:
                    break;
                }
            }
        } else {
            /* ── Existing scalar-constant setup (UNCHANGED) ── */
            switch (single_cc->tf->type) {
            /* ── VARCHAR ── */
            case FT_VARCHAR: {
                varchar_cmax = single_cc->tf->size - 2;
                i1 = single_cc->i1; i2 = single_cc->i2;
                min_exc = single_cc->raw ? (int8_t)single_cc->raw->min_exclusive : 0;
                max_exc = single_cc->raw ? (int8_t)single_cc->raw->max_exclusive : 0;
                switch (single_cc->op) {
                case OP_EQUAL:       s1 = single_cc->s1; s1_len = (size_t)single_cc->s1_len; match_kind = MATCH_VARCHAR_EQ; break;
                case OP_NOT_EQUAL:   s1 = single_cc->s1; s1_len = (size_t)single_cc->s1_len; match_kind = MATCH_VARCHAR_NEQ; break;
                case OP_CONTAINS:    needle = single_cc->needle_lc; needle_len = (size_t)single_cc->needle_len; match_kind = MATCH_VARCHAR_CONTAINS; break;
                case OP_NOT_CONTAINS: needle = single_cc->needle_lc; needle_len = (size_t)single_cc->needle_len; match_kind = MATCH_VARCHAR_NOT_CONTAINS; break;
                case OP_STARTS_WITH: needle = single_cc->needle_lc; needle_len = (size_t)single_cc->needle_len; match_kind = MATCH_VARCHAR_STARTS; break;
                case OP_ENDS_WITH:   needle = single_cc->needle_lc; needle_len = (size_t)single_cc->needle_len; match_kind = MATCH_VARCHAR_ENDS; break;
                case OP_EXISTS:      match_kind = MATCH_VARCHAR_EXISTS; break;
                case OP_NOT_EXISTS:  match_kind = MATCH_VARCHAR_NOT_EXISTS; break;
                case OP_LEN_EQ:      match_kind = MATCH_VARCHAR_LEN_EQ; break;
                case OP_LEN_NEQ:     match_kind = MATCH_VARCHAR_LEN_NEQ; break;
                case OP_LEN_LESS:    match_kind = MATCH_VARCHAR_LEN_LESS; break;
                case OP_LEN_GREATER: match_kind = MATCH_VARCHAR_LEN_GREATER; break;
                case OP_LEN_LESS_EQ: match_kind = MATCH_VARCHAR_LEN_LESS_EQ; break;
                case OP_LEN_GREATER_EQ: match_kind = MATCH_VARCHAR_LEN_GREATER_EQ; break;
                case OP_LEN_BETWEEN: match_kind = MATCH_VARCHAR_LEN_BETWEEN; break;
                default: break;
                }
                break;
            }
            /* ── INT64 (LONG / NUMERIC / TIMESTAMP) ── */
            case FT_LONG: case FT_NUMERIC: case FT_TIMESTAMP: {
                i1 = single_cc->i1; i2 = single_cc->i2;
                in_i64 = single_cc->in_i64; in_f64 = single_cc->in_f64; in_count = (uint16_t)single_cc->in_count;
                min_exc = single_cc->raw ? (int8_t)single_cc->raw->min_exclusive : 0;
                max_exc = single_cc->raw ? (int8_t)single_cc->raw->max_exclusive : 0;
                switch (single_cc->op) {
                case OP_EXISTS:      match_kind = MATCH_COUNT_ALL; break;
                case OP_NOT_EXISTS:  match_kind = MATCH_COUNT_NONE; break;
                case OP_EQUAL:       match_kind = MATCH_INT64_EQ; break;
                case OP_NOT_EQUAL:   match_kind = MATCH_INT64_NEQ; break;
                case OP_LESS:        match_kind = MATCH_INT64_LESS; break;
                case OP_GREATER:     match_kind = MATCH_INT64_GREATER; break;
                case OP_LESS_EQ:     match_kind = MATCH_INT64_LESS_EQ; break;
                case OP_GREATER_EQ:  match_kind = MATCH_INT64_GREATER_EQ; break;
                case OP_BETWEEN:     match_kind = MATCH_INT64_BETWEEN; break;
                case OP_IN:          match_kind = MATCH_INT64_IN; break;
                case OP_NOT_IN:      match_kind = MATCH_INT64_NOT_IN; break;
                default: break;
                }
                break;
            }
            /* ── INT32 (INT) ── */
            case FT_INT: {
                i1 = single_cc->i1; i2 = single_cc->i2;
                in_i64 = single_cc->in_i64; in_count = (uint16_t)single_cc->in_count;
                min_exc = single_cc->raw ? (int8_t)single_cc->raw->min_exclusive : 0;
                max_exc = single_cc->raw ? (int8_t)single_cc->raw->max_exclusive : 0;
                switch (single_cc->op) {
                case OP_EXISTS:      match_kind = MATCH_COUNT_ALL; break;
                case OP_NOT_EXISTS:  match_kind = MATCH_COUNT_NONE; break;
                case OP_EQUAL:       match_kind = MATCH_INT32_EQ; break;
                case OP_NOT_EQUAL:   match_kind = MATCH_INT32_NEQ; break;
                case OP_LESS:        match_kind = MATCH_INT32_LESS; break;
                case OP_GREATER:     match_kind = MATCH_INT32_GREATER; break;
                case OP_LESS_EQ:     match_kind = MATCH_INT32_LESS_EQ; break;
                case OP_GREATER_EQ:  match_kind = MATCH_INT32_GREATER_EQ; break;
                case OP_BETWEEN:     match_kind = MATCH_INT32_BETWEEN; break;
                case OP_IN:          match_kind = MATCH_INT32_IN; break;
                case OP_NOT_IN:      match_kind = MATCH_INT32_NOT_IN; break;
                default: break;
                }
                break;
            }
            /* ── DATE (falls back for zero-date semantics) ── */
            case FT_DATE:
                break;
            /* ── SHORT ── */
            case FT_SHORT: {
                i1 = single_cc->i1; i2 = single_cc->i2;
                in_i64 = single_cc->in_i64; in_count = (uint16_t)single_cc->in_count;
                min_exc = single_cc->raw ? (int8_t)single_cc->raw->min_exclusive : 0;
                max_exc = single_cc->raw ? (int8_t)single_cc->raw->max_exclusive : 0;
                switch (single_cc->op) {
                case OP_EXISTS:      match_kind = MATCH_COUNT_ALL; break;
                case OP_NOT_EXISTS:  match_kind = MATCH_COUNT_NONE; break;
                case OP_EQUAL:       match_kind = MATCH_SHORT_EQ; break;
                case OP_NOT_EQUAL:   match_kind = MATCH_SHORT_NEQ; break;
                case OP_LESS:        match_kind = MATCH_SHORT_LESS; break;
                case OP_GREATER:     match_kind = MATCH_SHORT_GREATER; break;
                case OP_LESS_EQ:     match_kind = MATCH_SHORT_LESS_EQ; break;
                case OP_GREATER_EQ:  match_kind = MATCH_SHORT_GREATER_EQ; break;
                case OP_BETWEEN:     match_kind = MATCH_SHORT_BETWEEN; break;
                case OP_IN:          match_kind = MATCH_SHORT_IN; break;
                case OP_NOT_IN:      match_kind = MATCH_SHORT_NOT_IN; break;
                default: break;
                }
                break;
            }
            /* ── BOOL / BYTE ── */
            case FT_BOOL: case FT_BYTE: {
                i1 = (int64_t)single_cc->b1; i2 = 0;
                in_i64 = single_cc->in_i64; in_count = (uint16_t)single_cc->in_count;
                min_exc = single_cc->raw ? (int8_t)single_cc->raw->min_exclusive : 0;
                max_exc = single_cc->raw ? (int8_t)single_cc->raw->max_exclusive : 0;
                switch (single_cc->op) {
                case OP_EXISTS:      match_kind = MATCH_COUNT_ALL; break;
                case OP_NOT_EXISTS:  match_kind = MATCH_COUNT_NONE; break;
                case OP_EQUAL:       match_kind = MATCH_BYTE_EQ; break;
                case OP_NOT_EQUAL:   match_kind = MATCH_BYTE_NEQ; break;
                case OP_LESS:        match_kind = MATCH_BYTE_LESS; break;
                case OP_GREATER:     match_kind = MATCH_BYTE_GREATER; break;
                case OP_LESS_EQ:     match_kind = MATCH_BYTE_LESS_EQ; break;
                case OP_GREATER_EQ:  match_kind = MATCH_BYTE_GREATER_EQ; break;
                case OP_BETWEEN:     match_kind = MATCH_BYTE_BETWEEN; break;
                case OP_IN:          match_kind = MATCH_BYTE_IN; break;
                case OP_NOT_IN:      match_kind = MATCH_BYTE_NOT_IN; break;
                default: break;
                }
                break;
            }
            /* ── ENUM ── */
            case FT_ENUM: {
                i1 = single_cc->i1; i2 = single_cc->i2;
                in_i64 = single_cc->in_i64; in_count = (uint16_t)single_cc->in_count;
                min_exc = single_cc->raw ? (int8_t)single_cc->raw->min_exclusive : 0;
                max_exc = single_cc->raw ? (int8_t)single_cc->raw->max_exclusive : 0;
                /* 1-byte enums can use the BYTE inline matchers; 2-byte enums fall through. */
                if (single_cc->tf && single_cc->tf->enum_width <= 1) {
                    switch (single_cc->op) {
                    case OP_EXISTS:      match_kind = MATCH_COUNT_ALL; break;
                    case OP_NOT_EXISTS:  match_kind = MATCH_COUNT_NONE; break;
                    case OP_EQUAL:       match_kind = MATCH_BYTE_EQ; break;
                    case OP_NOT_EQUAL:   match_kind = MATCH_BYTE_NEQ; break;
                    case OP_LESS:        match_kind = MATCH_BYTE_LESS; break;
                    case OP_GREATER:     match_kind = MATCH_BYTE_GREATER; break;
                    case OP_LESS_EQ:     match_kind = MATCH_BYTE_LESS_EQ; break;
                    case OP_GREATER_EQ:  match_kind = MATCH_BYTE_GREATER_EQ; break;
                    case OP_BETWEEN:     match_kind = MATCH_BYTE_BETWEEN; break;
                    case OP_IN:          match_kind = MATCH_BYTE_IN; break;
                    case OP_NOT_IN:      match_kind = MATCH_BYTE_NOT_IN; break;
                    default: break;
                    }
                }
                /* enum_width == 2 falls through to MATCH_FALLBACK */
                break;
            }
            /* ── DOUBLE ── */
            case FT_DOUBLE: {
                d1 = single_cc->d1; d2 = single_cc->d2;
                in_f64 = single_cc->in_f64; in_count = (uint16_t)single_cc->in_count;
                min_exc = single_cc->raw ? (int8_t)single_cc->raw->min_exclusive : 0;
                max_exc = single_cc->raw ? (int8_t)single_cc->raw->max_exclusive : 0;
                switch (single_cc->op) {
                case OP_EXISTS:      match_kind = MATCH_COUNT_ALL; break;
                case OP_NOT_EXISTS:  match_kind = MATCH_COUNT_NONE; break;
                case OP_EQUAL:       match_kind = MATCH_DOUBLE_EQ; break;
                case OP_NOT_EQUAL:   match_kind = MATCH_DOUBLE_NEQ; break;
                case OP_LESS:        match_kind = MATCH_DOUBLE_LESS; break;
                case OP_GREATER:     match_kind = MATCH_DOUBLE_GREATER; break;
                case OP_LESS_EQ:     match_kind = MATCH_DOUBLE_LESS_EQ; break;
                case OP_GREATER_EQ:  match_kind = MATCH_DOUBLE_GREATER_EQ; break;
                case OP_BETWEEN:     match_kind = MATCH_DOUBLE_BETWEEN; break;
                default: break;
                }
                break;
            }
            /* ── FLOAT ── */
            case FT_FLOAT: {
                float f1 = (float)single_cc->d1;
                float f2 = (float)single_cc->d2;
                d1 = (double)f1; d2 = (double)f2;
                in_f64 = single_cc->in_f64; in_count = (uint16_t)single_cc->in_count;
                min_exc = single_cc->raw ? (int8_t)single_cc->raw->min_exclusive : 0;
                max_exc = single_cc->raw ? (int8_t)single_cc->raw->max_exclusive : 0;
                switch (single_cc->op) {
                case OP_EXISTS:      match_kind = MATCH_COUNT_ALL; break;
                case OP_NOT_EXISTS:  match_kind = MATCH_COUNT_NONE; break;
                case OP_EQUAL:       match_kind = MATCH_FLOAT_EQ; break;
                case OP_NOT_EQUAL:   match_kind = MATCH_FLOAT_NEQ; break;
                case OP_LESS:        match_kind = MATCH_FLOAT_LESS; break;
                case OP_GREATER:     match_kind = MATCH_FLOAT_GREATER; break;
                case OP_LESS_EQ:     match_kind = MATCH_FLOAT_LESS_EQ; break;
                case OP_GREATER_EQ:  match_kind = MATCH_FLOAT_GREATER_EQ; break;
                case OP_BETWEEN:     match_kind = MATCH_FLOAT_BETWEEN; break;
                case OP_IN:          match_kind = MATCH_FLOAT_IN; break;
                case OP_NOT_IN:      match_kind = MATCH_FLOAT_NOT_IN; break;
                default: break;
                }
                break;
            }
            default: break;
            }
        }
    }

    const int field_base = 24 + field_offset;

    /* ── Chunk loop ── */
    while (more) {
        ssize_t chunk_len = dc.active_len;
        if (chunk_len <= 0) { if (chunk_len < 0) ret = (int)chunk_len; break; }
        uint8_t *restrict chunk = dc.buf[dc.active];
        size_t pos = 0;

        /* Reassemble carry slot from previous chunk boundary. */
        if (carry_len > 0) {
            int need = slot_size - carry_len;
            if ((ssize_t)need > chunk_len) {
                memcpy(carry + carry_len, chunk, (size_t)chunk_len);
                carry_len += (int)chunk_len;
                goto do_swap_m;
            }
            memcpy(carry + carry_len, chunk, (size_t)need);
            pos = (size_t)need;
            carry_len = 0;
            uint8_t flag = carry[18];
            if (flag == 1) {
                uint16_t klen; memcpy(&klen, carry + 16, 2);
                const uint8_t *val = carry + 24 + (size_t)klen;
                int matched;
                if (single_cc) matched = match_typed(val, single_cc, fs_mut);
                else if (tree) matched = criteria_match_tree(val, tree, fs_mut);
                else matched = 1;
                if (matched) local_count++;
            }
        }

        /* Deadline: check once per chunk. */
        if (dl && dl->timeout_ms > 0) {
            if (atomic_load_explicit(&dl->timed_out, memory_order_relaxed))
                { ret = -ETIMEDOUT; goto done_m; }
            uint64_t now = now_ms_coarse();
            if (now - dl->t0_ms > (uint64_t)dl->timeout_ms) {
                atomic_store_explicit(&dl->timed_out, 1, memory_order_relaxed);
                ret = -ETIMEDOUT; goto done_m;
            }
        }

        const int ss = slot_size;
        const uint8_t *chunk_end = chunk + chunk_len - ss;
        const uint8_t *rec = chunk + pos;

        switch (match_kind) {

        /* ── VARCHAR EQ/NEQ ── */
        case MATCH_VARCHAR_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *p = rec + field_base + klen;
                int elen = od_varchar_content_max(p);
                if (elen > varchar_cmax) elen = varchar_cmax;
                if (elen == (int)s1_len && memcmp(p + 2, s1, s1_len) == 0)
                    local_count++;
            }
            break;
        case MATCH_VARCHAR_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *p = rec + field_base + klen;
                int elen = od_varchar_content_max(p);
                if (elen > varchar_cmax) elen = varchar_cmax;
                if (elen != (int)s1_len || memcmp(p + 2, s1, s1_len) != 0)
                    local_count++;
            }
            break;

        /* ── VARCHAR CONTAINS/NOT_CONTAINS ── */
        case MATCH_VARCHAR_CONTAINS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *p = rec + field_base + klen;
                int elen = od_varchar_content_max(p);
                if (elen > varchar_cmax) elen = varchar_cmax;
                if (elen >= (int)needle_len &&
                    simd_memmem(p + 2, (size_t)elen, needle, needle_len) != NULL)
                    local_count++;
            }
            break;
        case MATCH_VARCHAR_NOT_CONTAINS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *p = rec + field_base + klen;
                int elen = od_varchar_content_max(p);
                if (elen > varchar_cmax) elen = varchar_cmax;
                if (elen < (int)needle_len ||
                    simd_memmem(p + 2, (size_t)elen, needle, needle_len) == NULL)
                    local_count++;
            }
            break;

        /* ── VARCHAR STARTS/ENDS ── */
        case MATCH_VARCHAR_STARTS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *p = rec + field_base + klen;
                int elen = od_varchar_content_max(p);
                if (elen > varchar_cmax) elen = varchar_cmax;
                if (elen >= (int)needle_len && memcmp(p + 2, needle, needle_len) == 0)
                    local_count++;
            }
            break;
        case MATCH_VARCHAR_ENDS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *p = rec + field_base + klen;
                int elen = od_varchar_content_max(p);
                if (elen > varchar_cmax) elen = varchar_cmax;
                if (elen >= (int)needle_len &&
                    memcmp(p + 2 + elen - needle_len, needle, needle_len) == 0)
                    local_count++;
            }
            break;

        /* ── VARCHAR EXISTS/NOT_EXISTS ── */
        case MATCH_VARCHAR_EXISTS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int elen = od_varchar_content_max(rec + field_base + klen);
                if (od_likely(elen > 0 && varchar_cmax > 0))
                    local_count++;
            }
            break;
        case MATCH_VARCHAR_NOT_EXISTS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int elen = od_varchar_content_max(rec + field_base + klen);
                if (od_unlikely(elen == 0 || varchar_cmax == 0))
                    local_count++;
            }
            break;

        /* ── VARCHAR LEN_* ── */
        case MATCH_VARCHAR_LEN_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int elen = od_varchar_content_max(rec + field_base + klen);
                if (elen > varchar_cmax) elen = varchar_cmax;
                if ((int64_t)elen == i1) local_count++;
            }
            break;
        case MATCH_VARCHAR_LEN_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int elen = od_varchar_content_max(rec + field_base + klen);
                if (elen > varchar_cmax) elen = varchar_cmax;
                if ((int64_t)elen != i1) local_count++;
            }
            break;
        case MATCH_VARCHAR_LEN_LESS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int elen = od_varchar_content_max(rec + field_base + klen);
                if (elen > varchar_cmax) elen = varchar_cmax;
                if ((int64_t)elen < i1) local_count++;
            }
            break;
        case MATCH_VARCHAR_LEN_GREATER:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int elen = od_varchar_content_max(rec + field_base + klen);
                if (elen > varchar_cmax) elen = varchar_cmax;
                if ((int64_t)elen > i1) local_count++;
            }
            break;
        case MATCH_VARCHAR_LEN_LESS_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int elen = od_varchar_content_max(rec + field_base + klen);
                if (elen > varchar_cmax) elen = varchar_cmax;
                if ((int64_t)elen <= i1) local_count++;
            }
            break;
        case MATCH_VARCHAR_LEN_GREATER_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int elen = od_varchar_content_max(rec + field_base + klen);
                if (elen > varchar_cmax) elen = varchar_cmax;
                if ((int64_t)elen >= i1) local_count++;
            }
            break;
        case MATCH_VARCHAR_LEN_BETWEEN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int elen = od_varchar_content_max(rec + field_base + klen);
                if (elen > varchar_cmax) elen = varchar_cmax;
                int64_t v = (int64_t)elen;
                int ok = 1;
                if (min_exc ? (v <= i1) : (v < i1)) ok = 0;
                if (max_exc ? (v >= i2) : (v > i2)) ok = 0;
                if (ok) local_count++;
            }
            break;

        /* ── INT64 ── */
        case MATCH_INT64_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i64_be(rec + 24 + klen + field_offset) == i1) local_count++;
            }
            break;
        case MATCH_INT64_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i64_be(rec + 24 + klen + field_offset) != i1) local_count++;
            }
            break;
        case MATCH_INT64_LESS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i64_be(rec + 24 + klen + field_offset) < i1) local_count++;
            }
            break;
        case MATCH_INT64_GREATER:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i64_be(rec + 24 + klen + field_offset) > i1) local_count++;
            }
            break;
        case MATCH_INT64_LESS_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i64_be(rec + 24 + klen + field_offset) <= i1) local_count++;
            }
            break;
        case MATCH_INT64_GREATER_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i64_be(rec + 24 + klen + field_offset) >= i1) local_count++;
            }
            break;
        case MATCH_INT64_BETWEEN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t v = od_load_i64_be(rec + 24 + klen + field_offset);
                int ok = 1;
                if (min_exc ? (v <= i1) : (v < i1)) ok = 0;
                if (max_exc ? (v >= i2) : (v > i2)) ok = 0;
                if (ok) local_count++;
            }
            break;
        case MATCH_INT64_IN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t v = od_load_i64_be(rec + 24 + klen + field_offset);
                for (uint16_t j = 0; j < in_count; j++)
                    if (in_i64[j] == v) { local_count++; break; }
            }
            break;
        case MATCH_INT64_NOT_IN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t v = od_load_i64_be(rec + 24 + klen + field_offset);
                int found = 0;
                for (uint16_t j = 0; j < in_count; j++)
                    if (in_i64[j] == v) { found = 1; break; }
                if (!found) local_count++;
            }
            break;

        /* ── INT32 ── */
        case MATCH_INT32_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i32_be(rec + 24 + klen + field_offset) == i1) local_count++;
            }
            break;
        case MATCH_INT32_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i32_be(rec + 24 + klen + field_offset) != i1) local_count++;
            }
            break;
        case MATCH_INT32_LESS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i32_be(rec + 24 + klen + field_offset) < i1) local_count++;
            }
            break;
        case MATCH_INT32_GREATER:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i32_be(rec + 24 + klen + field_offset) > i1) local_count++;
            }
            break;
        case MATCH_INT32_LESS_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i32_be(rec + 24 + klen + field_offset) <= i1) local_count++;
            }
            break;
        case MATCH_INT32_GREATER_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i32_be(rec + 24 + klen + field_offset) >= i1) local_count++;
            }
            break;
        case MATCH_INT32_BETWEEN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t v = od_load_i32_be(rec + 24 + klen + field_offset);
                int ok = 1;
                if (min_exc ? (v <= i1) : (v < i1)) ok = 0;
                if (max_exc ? (v >= i2) : (v > i2)) ok = 0;
                if (ok) local_count++;
            }
            break;
        case MATCH_INT32_IN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t v = od_load_i32_be(rec + 24 + klen + field_offset);
                for (uint16_t j = 0; j < in_count; j++)
                    if (in_i64[j] == v) { local_count++; break; }
            }
            break;
        case MATCH_INT32_NOT_IN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t v = od_load_i32_be(rec + 24 + klen + field_offset);
                int found = 0;
                for (uint16_t j = 0; j < in_count; j++)
                    if (in_i64[j] == v) { found = 1; break; }
                if (!found) local_count++;
            }
            break;

        /* ── SHORT ── */
        case MATCH_SHORT_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i16_be(rec + 24 + klen + field_offset) == i1) local_count++;
            }
            break;
        case MATCH_SHORT_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i16_be(rec + 24 + klen + field_offset) != i1) local_count++;
            }
            break;
        case MATCH_SHORT_LESS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i16_be(rec + 24 + klen + field_offset) < i1) local_count++;
            }
            break;
        case MATCH_SHORT_GREATER:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i16_be(rec + 24 + klen + field_offset) > i1) local_count++;
            }
            break;
        case MATCH_SHORT_LESS_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i16_be(rec + 24 + klen + field_offset) <= i1) local_count++;
            }
            break;
        case MATCH_SHORT_GREATER_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_i16_be(rec + 24 + klen + field_offset) >= i1) local_count++;
            }
            break;
        case MATCH_SHORT_BETWEEN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t v = od_load_i16_be(rec + 24 + klen + field_offset);
                int ok = 1;
                if (min_exc ? (v <= i1) : (v < i1)) ok = 0;
                if (max_exc ? (v >= i2) : (v > i2)) ok = 0;
                if (ok) local_count++;
            }
            break;
        case MATCH_SHORT_IN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t v = od_load_i16_be(rec + 24 + klen + field_offset);
                for (uint16_t j = 0; j < in_count; j++)
                    if (in_i64[j] == v) { local_count++; break; }
            }
            break;
        case MATCH_SHORT_NOT_IN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t v = od_load_i16_be(rec + 24 + klen + field_offset);
                int found = 0;
                for (uint16_t j = 0; j < in_count; j++)
                    if (in_i64[j] == v) { found = 1; break; }
                if (!found) local_count++;
            }
            break;

        /* ── BYTE/BOOL/ENUM(1-byte) ── */
        case MATCH_BYTE_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if ((int64_t)rec[24 + klen + field_offset] == i1) local_count++;
            }
            break;
        case MATCH_BYTE_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if ((int64_t)rec[24 + klen + field_offset] != i1) local_count++;
            }
            break;
        case MATCH_BYTE_LESS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if ((int64_t)rec[24 + klen + field_offset] < i1) local_count++;
            }
            break;
        case MATCH_BYTE_GREATER:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if ((int64_t)rec[24 + klen + field_offset] > i1) local_count++;
            }
            break;
        case MATCH_BYTE_LESS_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if ((int64_t)rec[24 + klen + field_offset] <= i1) local_count++;
            }
            break;
        case MATCH_BYTE_GREATER_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if ((int64_t)rec[24 + klen + field_offset] >= i1) local_count++;
            }
            break;
        case MATCH_BYTE_BETWEEN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t v = (int64_t)rec[24 + klen + field_offset];
                int ok = 1;
                if (min_exc ? (v <= i1) : (v < i1)) ok = 0;
                if (max_exc ? (v >= i2) : (v > i2)) ok = 0;
                if (ok) local_count++;
            }
            break;
        case MATCH_BYTE_IN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t v = (int64_t)rec[24 + klen + field_offset];
                for (uint16_t j = 0; j < in_count; j++)
                    if (in_i64[j] == v) { local_count++; break; }
            }
            break;
        case MATCH_BYTE_NOT_IN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t v = (int64_t)rec[24 + klen + field_offset];
                int found = 0;
                for (uint16_t j = 0; j < in_count; j++)
                    if (in_i64[j] == v) { found = 1; break; }
                if (!found) local_count++;
            }
            break;

        /* ── DOUBLE ── */
        case MATCH_DOUBLE_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_f64(rec + 24 + klen + field_offset) == d1) local_count++;
            }
            break;
        case MATCH_DOUBLE_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_f64(rec + 24 + klen + field_offset) != d1) local_count++;
            }
            break;
        case MATCH_DOUBLE_LESS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_f64(rec + 24 + klen + field_offset) < d1) local_count++;
            }
            break;
        case MATCH_DOUBLE_GREATER:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_f64(rec + 24 + klen + field_offset) > d1) local_count++;
            }
            break;
        case MATCH_DOUBLE_LESS_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_f64(rec + 24 + klen + field_offset) <= d1) local_count++;
            }
            break;
        case MATCH_DOUBLE_GREATER_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_f64(rec + 24 + klen + field_offset) >= d1) local_count++;
            }
            break;
        case MATCH_DOUBLE_BETWEEN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                double v = od_load_f64(rec + 24 + klen + field_offset);
                int ok = 1;
                if (min_exc ? (v <= d1) : (v < d1)) ok = 0;
                if (max_exc ? (v >= d2) : (v > d2)) ok = 0;
                if (ok) local_count++;
            }
            break;

        /* ── FLOAT ── */
        case MATCH_FLOAT_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_f32_le(rec + 24 + klen + field_offset) == (float)d1) local_count++;
            }
            break;
        case MATCH_FLOAT_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_f32_le(rec + 24 + klen + field_offset) != (float)d1) local_count++;
            }
            break;
        case MATCH_FLOAT_LESS:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_f32_le(rec + 24 + klen + field_offset) < (float)d1) local_count++;
            }
            break;
        case MATCH_FLOAT_GREATER:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_f32_le(rec + 24 + klen + field_offset) > (float)d1) local_count++;
            }
            break;
        case MATCH_FLOAT_LESS_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_f32_le(rec + 24 + klen + field_offset) <= (float)d1) local_count++;
            }
            break;
        case MATCH_FLOAT_GREATER_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                if (od_load_f32_le(rec + 24 + klen + field_offset) >= (float)d1) local_count++;
            }
            break;
        case MATCH_FLOAT_BETWEEN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                float v = od_load_f32_le(rec + 24 + klen + field_offset);
                int ok = 1;
                if (min_exc ? (v <= (float)d1) : (v < (float)d1)) ok = 0;
                if (max_exc ? (v >= (float)d2) : (v > (float)d2)) ok = 0;
                if (ok) local_count++;
            }
            break;
        case MATCH_FLOAT_IN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                float v = od_load_f32_le(rec + 24 + klen + field_offset);
                for (uint16_t j = 0; j < in_count; j++)
                    if (v == (float)in_f64[j]) { local_count++; break; }
            }
            break;
        case MATCH_FLOAT_NOT_IN:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                float v = od_load_f32_le(rec + 24 + klen + field_offset);
                int found = 0;
                for (uint16_t j = 0; j < in_count; j++)
                    if (v == (float)in_f64[j]) { found = 1; break; }
                if (!found) local_count++;
            }
            break;

        /* ── FIELD-VS-FIELD VARCHAR ── */
        case MATCH_FIELD_VARCHAR_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *lhs = rec + 24 + klen + field_offset;
                const uint8_t *rhs = rec + 24 + klen + rhs_offset;
                int llen = od_varchar_content_max(lhs);
                int rlen = od_varchar_content_max(rhs);
                if (llen > varchar_cmax) llen = varchar_cmax;
                if (rlen > varchar_cmax) rlen = varchar_cmax;
                if (llen == rlen && memcmp(lhs + 2, rhs + 2, llen) == 0)
                    local_count++;
            }
            break;
        case MATCH_FIELD_VARCHAR_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *lhs = rec + 24 + klen + field_offset;
                const uint8_t *rhs = rec + 24 + klen + rhs_offset;
                int llen = od_varchar_content_max(lhs);
                int rlen = od_varchar_content_max(rhs);
                if (llen > varchar_cmax) llen = varchar_cmax;
                if (rlen > varchar_cmax) rlen = varchar_cmax;
                if (llen != rlen || memcmp(lhs + 2, rhs + 2, llen) != 0)
                    local_count++;
            }
            break;
        case MATCH_FIELD_VARCHAR_LT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *lhs = rec + 24 + klen + field_offset;
                const uint8_t *rhs = rec + 24 + klen + rhs_offset;
                int llen = od_varchar_content_max(lhs);
                int rlen = od_varchar_content_max(rhs);
                if (llen > varchar_cmax) llen = varchar_cmax;
                if (rlen > varchar_cmax) rlen = varchar_cmax;
                int cmp_n = llen < rlen ? llen : rlen;
                int r = memcmp(lhs + 2, rhs + 2, cmp_n);
                if (r < 0 || (r == 0 && llen < rlen))
                    local_count++;
            }
            break;
        case MATCH_FIELD_VARCHAR_GT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *lhs = rec + 24 + klen + field_offset;
                const uint8_t *rhs = rec + 24 + klen + rhs_offset;
                int llen = od_varchar_content_max(lhs);
                int rlen = od_varchar_content_max(rhs);
                if (llen > varchar_cmax) llen = varchar_cmax;
                if (rlen > varchar_cmax) rlen = varchar_cmax;
                int cmp_n = llen < rlen ? llen : rlen;
                int r = memcmp(lhs + 2, rhs + 2, cmp_n);
                if (r > 0 || (r == 0 && llen > rlen))
                    local_count++;
            }
            break;
        case MATCH_FIELD_VARCHAR_LTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *lhs = rec + 24 + klen + field_offset;
                const uint8_t *rhs = rec + 24 + klen + rhs_offset;
                int llen = od_varchar_content_max(lhs);
                int rlen = od_varchar_content_max(rhs);
                if (llen > varchar_cmax) llen = varchar_cmax;
                if (rlen > varchar_cmax) rlen = varchar_cmax;
                int cmp_n = llen < rlen ? llen : rlen;
                int r = memcmp(lhs + 2, rhs + 2, cmp_n);
                if (r < 0 || (r == 0 && llen <= rlen))
                    local_count++;
            }
            break;
        case MATCH_FIELD_VARCHAR_GTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *lhs = rec + 24 + klen + field_offset;
                const uint8_t *rhs = rec + 24 + klen + rhs_offset;
                int llen = od_varchar_content_max(lhs);
                int rlen = od_varchar_content_max(rhs);
                if (llen > varchar_cmax) llen = varchar_cmax;
                if (rlen > varchar_cmax) rlen = varchar_cmax;
                int cmp_n = llen < rlen ? llen : rlen;
                int r = memcmp(lhs + 2, rhs + 2, cmp_n);
                if (r > 0 || (r == 0 && llen >= rlen))
                    local_count++;
            }
            break;

        /* ── FIELD-VS-FIELD INT64 ── */
        case MATCH_FIELD_INT64_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i64_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i64_be(rec + 24 + klen + rhs_offset);
                if (lhs == rhs) local_count++;
            }
            break;
        case MATCH_FIELD_INT64_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i64_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i64_be(rec + 24 + klen + rhs_offset);
                if (lhs != rhs) local_count++;
            }
            break;
        case MATCH_FIELD_INT64_LT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i64_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i64_be(rec + 24 + klen + rhs_offset);
                if (lhs < rhs) local_count++;
            }
            break;
        case MATCH_FIELD_INT64_GT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i64_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i64_be(rec + 24 + klen + rhs_offset);
                if (lhs > rhs) local_count++;
            }
            break;
        case MATCH_FIELD_INT64_LTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i64_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i64_be(rec + 24 + klen + rhs_offset);
                if (lhs <= rhs) local_count++;
            }
            break;
        case MATCH_FIELD_INT64_GTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i64_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i64_be(rec + 24 + klen + rhs_offset);
                if (lhs >= rhs) local_count++;
            }
            break;

        /* ── FIELD-VS-FIELD INT32 ── */
        case MATCH_FIELD_INT32_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i32_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i32_be(rec + 24 + klen + rhs_offset);
                if (lhs == rhs) local_count++;
            }
            break;
        case MATCH_FIELD_INT32_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i32_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i32_be(rec + 24 + klen + rhs_offset);
                if (lhs != rhs) local_count++;
            }
            break;
        case MATCH_FIELD_INT32_LT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i32_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i32_be(rec + 24 + klen + rhs_offset);
                if (lhs < rhs) local_count++;
            }
            break;
        case MATCH_FIELD_INT32_GT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i32_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i32_be(rec + 24 + klen + rhs_offset);
                if (lhs > rhs) local_count++;
            }
            break;
        case MATCH_FIELD_INT32_LTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i32_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i32_be(rec + 24 + klen + rhs_offset);
                if (lhs <= rhs) local_count++;
            }
            break;
        case MATCH_FIELD_INT32_GTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i32_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i32_be(rec + 24 + klen + rhs_offset);
                if (lhs >= rhs) local_count++;
            }
            break;

        /* ── FIELD-VS-FIELD SHORT ── */
        case MATCH_FIELD_SHORT_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i16_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i16_be(rec + 24 + klen + rhs_offset);
                if (lhs == rhs) local_count++;
            }
            break;
        case MATCH_FIELD_SHORT_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i16_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i16_be(rec + 24 + klen + rhs_offset);
                if (lhs != rhs) local_count++;
            }
            break;
        case MATCH_FIELD_SHORT_LT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i16_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i16_be(rec + 24 + klen + rhs_offset);
                if (lhs < rhs) local_count++;
            }
            break;
        case MATCH_FIELD_SHORT_GT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i16_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i16_be(rec + 24 + klen + rhs_offset);
                if (lhs > rhs) local_count++;
            }
            break;
        case MATCH_FIELD_SHORT_LTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i16_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i16_be(rec + 24 + klen + rhs_offset);
                if (lhs <= rhs) local_count++;
            }
            break;
        case MATCH_FIELD_SHORT_GTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i16_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i16_be(rec + 24 + klen + rhs_offset);
                if (lhs >= rhs) local_count++;
            }
            break;

        /* ── FIELD-VS-FIELD DOUBLE ── */
        case MATCH_FIELD_DOUBLE_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                double lhs, rhs;
                memcpy(&lhs, rec + 24 + klen + field_offset, 8);
                memcpy(&rhs, rec + 24 + klen + rhs_offset, 8);
                if (lhs == rhs) local_count++;
            }
            break;
        case MATCH_FIELD_DOUBLE_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                double lhs, rhs;
                memcpy(&lhs, rec + 24 + klen + field_offset, 8);
                memcpy(&rhs, rec + 24 + klen + rhs_offset, 8);
                if (lhs != rhs) local_count++;
            }
            break;
        case MATCH_FIELD_DOUBLE_LT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                double lhs, rhs;
                memcpy(&lhs, rec + 24 + klen + field_offset, 8);
                memcpy(&rhs, rec + 24 + klen + rhs_offset, 8);
                if (lhs < rhs) local_count++;
            }
            break;
        case MATCH_FIELD_DOUBLE_GT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                double lhs, rhs;
                memcpy(&lhs, rec + 24 + klen + field_offset, 8);
                memcpy(&rhs, rec + 24 + klen + rhs_offset, 8);
                if (lhs > rhs) local_count++;
            }
            break;
        case MATCH_FIELD_DOUBLE_LTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                double lhs, rhs;
                memcpy(&lhs, rec + 24 + klen + field_offset, 8);
                memcpy(&rhs, rec + 24 + klen + rhs_offset, 8);
                if (lhs <= rhs) local_count++;
            }
            break;
        case MATCH_FIELD_DOUBLE_GTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                double lhs, rhs;
                memcpy(&lhs, rec + 24 + klen + field_offset, 8);
                memcpy(&rhs, rec + 24 + klen + rhs_offset, 8);
                if (lhs >= rhs) local_count++;
            }
            break;

        /* ── FIELD-VS-FIELD FLOAT ── */
        case MATCH_FIELD_FLOAT_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                float lhs, rhs;
                memcpy(&lhs, rec + 24 + klen + field_offset, 4);
                memcpy(&rhs, rec + 24 + klen + rhs_offset, 4);
                if (lhs == rhs) local_count++;
            }
            break;
        case MATCH_FIELD_FLOAT_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                float lhs, rhs;
                memcpy(&lhs, rec + 24 + klen + field_offset, 4);
                memcpy(&rhs, rec + 24 + klen + rhs_offset, 4);
                if (lhs != rhs) local_count++;
            }
            break;
        case MATCH_FIELD_FLOAT_LT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                float lhs, rhs;
                memcpy(&lhs, rec + 24 + klen + field_offset, 4);
                memcpy(&rhs, rec + 24 + klen + rhs_offset, 4);
                if (lhs < rhs) local_count++;
            }
            break;
        case MATCH_FIELD_FLOAT_GT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                float lhs, rhs;
                memcpy(&lhs, rec + 24 + klen + field_offset, 4);
                memcpy(&rhs, rec + 24 + klen + rhs_offset, 4);
                if (lhs > rhs) local_count++;
            }
            break;
        case MATCH_FIELD_FLOAT_LTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                float lhs, rhs;
                memcpy(&lhs, rec + 24 + klen + field_offset, 4);
                memcpy(&rhs, rec + 24 + klen + rhs_offset, 4);
                if (lhs <= rhs) local_count++;
            }
            break;
        case MATCH_FIELD_FLOAT_GTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                float lhs, rhs;
                memcpy(&lhs, rec + 24 + klen + field_offset, 4);
                memcpy(&rhs, rec + 24 + klen + rhs_offset, 4);
                if (lhs >= rhs) local_count++;
            }
            break;

        /* ── FIELD-VS-FIELD BYTE ── */
        case MATCH_FIELD_BYTE_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int lhs = (int)rec[24 + klen + field_offset];
                int rhs = (int)rec[24 + klen + rhs_offset];
                if (lhs == rhs) local_count++;
            }
            break;
        case MATCH_FIELD_BYTE_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int lhs = (int)rec[24 + klen + field_offset];
                int rhs = (int)rec[24 + klen + rhs_offset];
                if (lhs != rhs) local_count++;
            }
            break;
        case MATCH_FIELD_BYTE_LT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int lhs = (int)rec[24 + klen + field_offset];
                int rhs = (int)rec[24 + klen + rhs_offset];
                if (lhs < rhs) local_count++;
            }
            break;
        case MATCH_FIELD_BYTE_GT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int lhs = (int)rec[24 + klen + field_offset];
                int rhs = (int)rec[24 + klen + rhs_offset];
                if (lhs > rhs) local_count++;
            }
            break;
        case MATCH_FIELD_BYTE_LTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int lhs = (int)rec[24 + klen + field_offset];
                int rhs = (int)rec[24 + klen + rhs_offset];
                if (lhs <= rhs) local_count++;
            }
            break;
        case MATCH_FIELD_BYTE_GTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int lhs = (int)rec[24 + klen + field_offset];
                int rhs = (int)rec[24 + klen + rhs_offset];
                if (lhs >= rhs) local_count++;
            }
            break;

        /* ── FIELD-VS-FIELD DATE ── */
        case MATCH_FIELD_DATE_EQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i32_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i32_be(rec + 24 + klen + rhs_offset);
                if (lhs == rhs) local_count++;
            }
            break;
        case MATCH_FIELD_DATE_NEQ:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i32_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i32_be(rec + 24 + klen + rhs_offset);
                if (lhs != rhs) local_count++;
            }
            break;
        case MATCH_FIELD_DATE_LT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i32_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i32_be(rec + 24 + klen + rhs_offset);
                if (lhs < rhs) local_count++;
            }
            break;
        case MATCH_FIELD_DATE_GT:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i32_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i32_be(rec + 24 + klen + rhs_offset);
                if (lhs > rhs) local_count++;
            }
            break;
        case MATCH_FIELD_DATE_LTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i32_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i32_be(rec + 24 + klen + rhs_offset);
                if (lhs <= rhs) local_count++;
            }
            break;
        case MATCH_FIELD_DATE_GTE:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                int64_t lhs = od_load_i32_be(rec + 24 + klen + field_offset);
                int64_t rhs = od_load_i32_be(rec + 24 + klen + rhs_offset);
                if (lhs >= rhs) local_count++;
            }
            break;

        /* ── FIELD-VS-FIELD MISMATCH ── */
        case MATCH_FIELD_MISMATCH:
            /* Type mismatch — no record can match. Skip the chunk. */
            rec = chunk + chunk_len;
            break;

        /* ── COUNT_ALL / COUNT_NONE (for EXISTS/NOT_EXISTS on non-varchar) ── */
        case MATCH_COUNT_ALL:
            for (; rec <= chunk_end; rec += ss) {
                if (rec + ss * 2 <= chunk_end)
                    od_prefetch(rec + ss * 2);
                if (rec[18] == 1) local_count++;
            }
            break;
        case MATCH_COUNT_NONE:
            /* never matches — advance past chunk; count stays 0 */
            rec = chunk + chunk_len;
            break;

        /* ── FALLBACK ── */
        case MATCH_FALLBACK:
        default:
            for (; rec <= chunk_end; rec += ss) {
                if (rec[18] != 1) continue;
                uint16_t klen; memcpy(&klen, rec + 16, 2);
                const uint8_t *val = rec + 24 + (size_t)klen;
                int matched;
                if (single_cc) matched = match_typed(val, single_cc, fs_mut);
                else if (tree) matched = criteria_match_tree(val, tree, fs_mut);
                else matched = 1;
                if (matched) local_count++;
            }
            break;
        }

        pos = (size_t)(rec - chunk);

        /* Save any partial slot at the chunk tail. */
        if (pos < (size_t)chunk_len) {
            carry_len = (int)((size_t)chunk_len - pos);
            memcpy(carry, chunk + pos, (size_t)carry_len);
        }

    do_swap_m:
        {
            ssize_t next = dbctx_swap(&dc);
            if (next < 0) { ret = (int)next; goto done_m; }
            if (next == 0) { dc.active_len = 0; more = 0; }
        }
    }

done_m:
    dbctx_destroy(&dc, worker_tid);
    free(carry);
    close(fd);
    *out_count = local_count;
    return ret;
}

/* ============================================================
 * btree_leaf_scan_o_direct
 * ========================================================= */

/*
 * Strategy: slurp the entire btree file into memalign'd chunks (using
 * the double-buffer prefetch for I/O), collect them into a chunk array,
 * then decode leaf pages from the in-memory chunks by computing each
 * page's byte offset as page_id * bt_page_size.
 *
 * This two-phase approach avoids page-spanning-chunk-boundary complexity
 * for the btree case (page size ≤ 65536 << ODIRECT_BUF_SIZE = 4 MB).
 *
 * Leaf page entry (prefix-compressed):
 *   [uint16_t data_len][uint8_t prefix_len][suffix_bytes][hash: 16 bytes]
 *   data_len = 1 + suffix_len + BT_HASH_SIZE
 *   full_value = prev_value[0:prefix_len] + suffix_bytes
 *   Anchor (slot % BT_LEAF_RESTART_K == 0): prefix_len = 0, full value in suffix.
 *   Tombstone: high bit of prefix_len is set (prefix_len & 0x80).
 *
 * Slot directory: uint16_t offsets[count] immediately after BtPageHeader.
 */

/* Accessor helpers (mirrors btree.c static inlines). */
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
    memcpy(&off,
           page + (size_t)BT_PAGE_DATA_START + (size_t)s * 2,
           2);
    return off;
}

/* Decode every leaf in a contiguous byte range and call cb per live entry.
 * `range` points to the first byte of the range, `range_len` is its length.
 * `start_off` is the byte offset of `range[0]` in the file (used only by the
 * page-0 header check on the first chunk).
 *
 * Pages are processed independently — prefix compression is leaf-local
 * (anchors at every BT_LEAF_RESTART_K slots restart the prefix), so any
 * leaf in any order decodes correctly without needing prior leaves.
 * Non-leaf pages (internal nodes, empty) are skipped via page_type
 * inspection.  Callback signals early-stop with non-zero return; bubble
 * that up via *stop_out.
 *
 * Returns 0 on success, non-zero on validation/decode error. */
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
    int rc = dbctx_init(&dc, fd, file_size, 0);
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
