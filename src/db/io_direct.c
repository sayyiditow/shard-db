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

/* posix_fadvise is Linux-specific; macOS lacks it. */
#ifndef POSIX_FADV_SEQUENTIAL
#  define POSIX_FADV_SEQUENTIAL 2
#  define POSIX_FADV_DONTNEED   4
#endif

/* ============================================================
 * Low-level helpers
 * ========================================================= */

int od_open(const char *path)
{
    int fd = -1;

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
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
#  endif
    return fd;

#else
    fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
#  ifdef POSIX_FADV_SEQUENTIAL
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
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
    void *p = NULL;
    if (posix_memalign(&p, ODIRECT_ALIGN, ODIRECT_BUF_SIZE) != 0)
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
        size_t want     = (remain >= (off_t)ODIRECT_BUF_SIZE)
                              ? ODIRECT_BUF_SIZE : (size_t)remain;
        /* O_DIRECT needs alignment; round up (pread stops at real EOF). */
        size_t want_a   = (want + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
        if (want_a > ODIRECT_BUF_SIZE) want_a = ODIRECT_BUF_SIZE;
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
static int dbctx_init(DbCtx *c, int fd, off_t file_size)
{
    memset(c, 0, sizeof(*c));
    c->fd        = fd;
    c->file_size = file_size;
    c->active    = 0;
    c->inactive  = 1;
    c->state     = DBS_IDLE;

    c->buf[0] = od_alloc_buf();
    c->buf[1] = od_alloc_buf();
    if (!c->buf[0] || !c->buf[1]) {
        free(c->buf[0]); free(c->buf[1]);
        return -ENOMEM;
    }

    pthread_mutex_init(&c->lock, NULL);
    pthread_cond_init(&c->prefetch_needed, NULL);
    pthread_cond_init(&c->prefetch_done,   NULL);

    /* Fill buf[0] synchronously. */
    off_t  fsz  = file_size;
    size_t want = (fsz >= (off_t)ODIRECT_BUF_SIZE) ? ODIRECT_BUF_SIZE : (size_t)fsz;
    size_t wanta = (want + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
    if (wanta > ODIRECT_BUF_SIZE) wanta = ODIRECT_BUF_SIZE;

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
    c->next_off   = (off_t)got;    /* worker starts from here */
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
    pthread_mutex_lock(&c->lock);
    c->worker_quit  = 1;
    c->work_pending = 1;   /* ensure worker unblocks from its wait */
    pthread_cond_signal(&c->prefetch_needed);
    pthread_cond_signal(&c->prefetch_done);
    pthread_mutex_unlock(&c->lock);

    pthread_join(worker_tid, NULL);

    free(c->buf[0]);
    free(c->buf[1]);
    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->prefetch_needed);
    pthread_cond_destroy(&c->prefetch_done);
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

    struct stat st;
    if (stat(seg_path, &st) != 0) return -errno;
    off_t file_size = st.st_size;
    if (file_size == 0) return 0;

    int fd = od_open(seg_path);
    if (fd < 0) return -errno;

    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size);
    if (rc != 0) { close(fd); return rc; }

    /* Carry buffer: holds partial slot at a chunk boundary. */
    uint8_t *carry = malloc((size_t)slot_size);
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

    /* Kick off the first async prefetch of buf[1]. */
    dbctx_kickoff(&dc);

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

/* Resolve page_id to a pointer within the collected chunks. */
static const uint8_t *bts_page(uint8_t **chunks, size_t n_chunks,
                                size_t total_bytes,
                                uint32_t pid, int page_sz)
{
    off_t byte_off = (off_t)pid * (off_t)page_sz;
    if (byte_off < 0) return NULL;
    if ((size_t)byte_off + (size_t)page_sz > total_bytes) return NULL;
    size_t ci  = (size_t)byte_off / ODIRECT_BUF_SIZE;
    size_t off = (size_t)byte_off % ODIRECT_BUF_SIZE;
    if (ci >= n_chunks) return NULL;
    /* Verify the entire page is within this chunk (true when
       page_sz < ODIRECT_BUF_SIZE, which is always the case). */
    if (off + (size_t)page_sz > ODIRECT_BUF_SIZE) {
        /* Page straddles chunk boundary — shouldn't happen at bt_page_size
           ≤ 64 KB << 4 MB, but be defensive: only possible if a page_sz
           change lands exactly at a 4 MB boundary.  Skip silently. */
        return NULL;
    }
    return chunks[ci] + off;
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

    /* ---- Collect all chunks via double-buffered I/O ---- */

    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size);
    if (rc != 0) { close(fd); return rc; }

    /* Chunk pointer array. */
    size_t   nchunks_cap = 64;
    uint8_t **chunks     = malloc(nchunks_cap * sizeof(uint8_t *));
    size_t   nchunks     = 0;
    size_t   total_bytes = 0;

    if (!chunks) {
        free(dc.buf[0]); free(dc.buf[1]);
        pthread_mutex_destroy(&dc.lock);
        pthread_cond_destroy(&dc.prefetch_needed);
        pthread_cond_destroy(&dc.prefetch_done);
        close(fd); return -ENOMEM;
    }

    pthread_t worker_tid;
    if (pthread_create(&worker_tid, NULL, prefetch_worker, &dc) != 0) {
        int e = errno;
        free(chunks);
        free(dc.buf[0]); free(dc.buf[1]);
        pthread_mutex_destroy(&dc.lock);
        pthread_cond_destroy(&dc.prefetch_needed);
        pthread_cond_destroy(&dc.prefetch_done);
        close(fd); return -e;
    }

    dbctx_kickoff(&dc);

    /* Gather all chunks. We cannot parse in-place because btree pages
       are addressed by page_id (not sequential order); we need random
       access across the whole file.  So collect, then decode. */
    int gather_err = 0;
    for (;;) {
        ssize_t chunk_len = dc.active_len;
        if (chunk_len <= 0) {
            if (chunk_len < 0) gather_err = (int)(-chunk_len);
            break;
        }

        /* Take ownership of this buffer: allocate a fresh one to replace
           it in the DbCtx so the worker can reuse the slots safely.
           Actually simpler: just memcpy out of the active buf, then swap.
           The buffer belongs to DbCtx; we must copy it. */
        if (nchunks >= nchunks_cap) {
            nchunks_cap *= 2;
            uint8_t **t = realloc(chunks, nchunks_cap * sizeof(uint8_t *));
            if (!t) { gather_err = ENOMEM; break; }
            chunks = t;
        }
        uint8_t *copy = od_alloc_buf();
        if (!copy) { gather_err = ENOMEM; break; }
        memcpy(copy, dc.buf[dc.active], (size_t)chunk_len);
        chunks[nchunks++] = copy;
        total_bytes += (size_t)chunk_len;

        ssize_t next = dbctx_swap(&dc);
        if (next < 0) { gather_err = (int)(-next); break; }
        if (next == 0) { dc.active_len = 0; break; }  /* EOF */
    }

    dbctx_destroy(&dc, worker_tid);
    close(fd);

    if (gather_err) {
        for (size_t i = 0; i < nchunks; i++) free(chunks[i]);
        free(chunks);
        return -gather_err;
    }
    if (nchunks == 0) { free(chunks); return 0; }

    /* ---- Decode leaf pages ---- */

    int ret = 0;

    /* Validate file header (page 0). */
    const uint8_t *hdr_page = bts_page(chunks, nchunks, total_bytes, 0, page_sz);
    if (!hdr_page) goto done_free;

    {
        const BtFileHeader *fh = (const BtFileHeader *)hdr_page;
        uint32_t magic; memcpy(&magic, &fh->magic, 4);
        if (magic != BT_MAGIC) goto done_free;

        uint64_t ec; memcpy(&ec, &fh->entry_count, 8);
        if (ec == 0) goto done_free;
    }

    {
        const BtFileHeader *fh = (const BtFileHeader *)hdr_page;

        /* Walk internal nodes to find the first leaf. */
        uint32_t page_id; memcpy(&page_id, &fh->root_page, 4);
        for (;;) {
            const uint8_t *pg = bts_page(chunks, nchunks, total_bytes,
                                         page_id, page_sz);
            if (!pg) goto done_free;
            const BtPageHeader *ph = (const BtPageHeader *)pg;
            uint32_t ptype; memcpy(&ptype, &ph->page_type, 4);
            if (ptype == 1) break;   /* leaf */
            uint32_t nlp; memcpy(&nlp, &ph->next_leaf, 4);
            if (nlp == 0) goto done_free;
            page_id = nlp;
        }

        /* Walk the leaf chain. */
        char   key_buf[BT_MAX_VAL_LEN];
        size_t key_len = 0;

        while (page_id != 0) {
            const uint8_t *pg = bts_page(chunks, nchunks, total_bytes,
                                         page_id, page_sz);
            if (!pg) break;
            const BtPageHeader *ph = (const BtPageHeader *)pg;
            uint32_t cnt; memcpy(&cnt, &ph->count, 4);

            for (uint32_t s = 0; s < cnt; s++) {
                uint16_t eoff = bts_slot_off(pg, s);
                const uint8_t *e = pg + eoff;
                uint8_t  plen = bts_prefix_len(e);
                size_t   slen = bts_suffix_len(e);

                if ((s & (uint32_t)(BT_LEAF_RESTART_K - 1)) == 0) {
                    /* Anchor: full key. */
                    key_len = slen;
                    if (key_len > BT_MAX_VAL_LEN) key_len = BT_MAX_VAL_LEN;
                    memcpy(key_buf, bts_suffix(e), key_len);
                } else {
                    /* Prefix-compressed. */
                    size_t klen = (size_t)(plen & 0x7f) + slen;
                    if (klen > BT_MAX_VAL_LEN) klen = BT_MAX_VAL_LEN;
                    size_t take = (klen > (size_t)(plen & 0x7f))
                                      ? (klen - (size_t)(plen & 0x7f)) : 0;
                    memcpy(key_buf + (plen & 0x7f), bts_suffix(e), take);
                    key_len = klen;
                }

                if (bts_is_tomb(e)) continue;

                const uint8_t *hash16 = bts_hash(e);
                int cbrc = cb((const uint8_t *)key_buf, key_len, hash16, ctx);
                if (cbrc != 0) { ret = 1; goto done_free; }
            }

            uint32_t nlp; memcpy(&nlp, &ph->next_leaf, 4);
            page_id = nlp;
        }
    }

done_free:
    for (size_t i = 0; i < nchunks; i++) free(chunks[i]);
    free(chunks);
    return ret;
}
