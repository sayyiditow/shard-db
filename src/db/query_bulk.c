#include "typed_index.h"
#include "types.h"
#include "typed_bulk_insert.h"
#include "slotcask.h"
#include "query_internal.h"
#include <stdatomic.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>

#ifdef TEST_BUILD
/* Deterministic OOM injection for bulk_upd_json_run (precedent:
   index.c's g_index_spill_open_fail_errno). Armed with the number of
   allocations to fail: each checked allocation site decrements and the
   site that reaches 0 behaves as if malloc returned NULL. 0 = never. */
static int g_bulk_upd_test_fail_alloc;

void bulk_upd_json_test_set_fail_alloc(int fail_n) {
    g_bulk_upd_test_fail_alloc = fail_n;
}
#endif

#ifdef TEST_BUILD
/* Deterministic injection for the v2 bulk-insert bitmap/trigram staging
   allocation (same precedent as g_bulk_upd_test_fail_alloc): armed N
   fails the Nth staging allocation; 0 = never. */
static int g_bulk_ins_payload_test_fail_alloc;

void bulk_ins_payload_test_set_fail_alloc(int fail_n) {
    g_bulk_ins_payload_test_fail_alloc = fail_n;
}
#endif

/* ========== BULK INSERT ========== */

/* Bulk ops use the v2 slotcask storage backend (registry-cached SlotcaskDb handles). */

/* ---- Shared worker types for parallel index builds ---- */
typedef struct {
    char ipath[PATH_MAX];
    BtEntry *pairs;
    size_t pair_count;
    int out_error;
    int out_errno;
} IdxBuildArg;
static void *idx_build_worker(void *arg) __attribute__((unused));
static void *idx_build_worker(void *arg) {
    IdxBuildArg *ib = (IdxBuildArg *)arg;
    /* Merge-rebuild: sort new entries, merge with existing tree, rebuild from scratch.
       Much faster than btree_insert_batch for large datasets because it uses sequential
       I/O (leaf scan + bulk_build) instead of random B+ tree insertions. */
    ib->out_error = btree_bulk_merge(ib->ipath, ib->pairs, ib->pair_count);
    ib->out_errno = ib->out_error ? errno : 0;
    return NULL;
}

/* Per-field bulk-merge that runs all idx_n shards' merges serially inside one
   worker. Trades shard-level parallelism for thread-pool dispatch efficiency:
   the prior layout dispatched nfields × idx_n tiny tasks (e.g. 14 × 16 = 224)
   into the 16-thread pool, paying full task-dispatch + parallel_for queue
   overhead per shard. The bench showed insert-with-pre-existing-indexes
   ~30 % slower than the pre-2026.05.1 single-file layout for that reason —
   bulk_merge's actual work-per-call is tiny (a few ms once warm), so the
   per-task overhead dominates. With nfields workers (≤16 in practice) the
   pool dispatches in one wave; each worker streams the 16 per-shard merges
   sequentially using its own thread, which the kernel page-cache handles
   well because consecutive shards of the same field share access patterns. */
typedef struct {
    const char *db_root;
    const char *object;
    const char *field;
    int splits;
    BtEntry *new_entries;     /* not owned; values freed by caller */
    size_t   new_count;
    int      out_error;
    int      out_errno;
    void *memory; /* packed bridge only */
} IdxFieldArg;

static void *idx_build_field_worker(void *arg) {
    IdxFieldArg *fa = (IdxFieldArg *)arg;
    fa->out_error = 0;
    fa->out_errno = 0;
    if (fa->new_count == 0) return NULL;
    int idx_n = index_splits_for(fa->splits);

    size_t allocation_bytes=(size_t)idx_n*3*sizeof(size_t)+fa->new_count*sizeof(BtEntry);
    if(typed_bulk_index_memory_take(fa->memory,allocation_bytes)){
        fa->out_error=-1;fa->out_errno=ENOMEM;return NULL;
    }
    /* Bucket-sort new_entries by idx_shard. */
    size_t *counts  = calloc((size_t)idx_n, sizeof(size_t));
    size_t *offsets = calloc((size_t)idx_n, sizeof(size_t));
    BtEntry *parted = malloc(fa->new_count * sizeof(BtEntry));
    if (!counts || !offsets || !parted) {
        LOG_ERROR(LOG_SUB_QUERY, "idx_build_field_worker: alloc failed for field %s (new_count=%zu)", fa->field, fa->new_count);
        free(counts); free(offsets); free(parted);
        fa->out_error = -1;
        fa->out_errno = ENOMEM;
        typed_bulk_index_memory_release(fa->memory,allocation_bytes);
        return NULL;
    }
    for (size_t i = 0; i < fa->new_count; i++)
        counts[idx_shard_for_hash(fa->new_entries[i].hash, fa->splits)]++;
    size_t acc = 0;
    for (int s = 0; s < idx_n; s++) { offsets[s] = acc; acc += counts[s]; }
    size_t *cursor = calloc((size_t)idx_n, sizeof(size_t));
    if (!cursor) {
        LOG_ERROR(LOG_SUB_QUERY, "idx_build_field_worker: calloc cursor failed (idx_n=%d)", idx_n);
        free(counts); free(offsets); free(parted);
        fa->out_error = -1;
        fa->out_errno = ENOMEM;
        typed_bulk_index_memory_release(fa->memory,allocation_bytes);
        return NULL;
    }
    for (size_t i = 0; i < fa->new_count; i++) {
        int s = idx_shard_for_hash(fa->new_entries[i].hash, fa->splits);
        parted[offsets[s] + cursor[s]++] = fa->new_entries[i];
    }
    free(cursor);

    /* Serial per-shard bulk_merge — same ops as before, just in one thread. */
    for (int s = 0; s < idx_n; s++) {
        if (counts[s] == 0) continue;
        char path[PATH_MAX];
        build_idx_path(path, sizeof(path), fa->db_root, fa->object, fa->field, s);
        if (btree_bulk_merge(path, parted + offsets[s], counts[s]) != 0) {
            fa->out_error = -1;
            fa->out_errno = errno;
            bt_publish_result publish = btree_bulk_merge_publish_result();
            const char *state = publish == BT_PUBLISH_PRE_RENAME_FAILED
                              ? "pre-rename-failed"
                              : publish == BT_PUBLISH_POST_RENAME_FSYNC_FAILED
                              ? "post-rename-durability-unconfirmed"
                              : "bulk-merge-failed";
            LOG_ERROR(LOG_SUB_QUERY,
                      "idx_build_field_worker: field=%s shard=%d target=%s state=%s errno=%d (%s)",
                      fa->field, s, path, state, fa->out_errno,
                      strerror(fa->out_errno));
            break;
        }
    }

    free(parted);
    free(counts);
    free(offsets);
    typed_bulk_index_memory_release(fa->memory,allocation_bytes);
    return NULL;
}

/* Partition `pairs` (of total `count`) by idx_shard and append per-shard
   IdxBuildArg slices to `out_args` (which must have room for at least
   index_splits_for(splits) entries). Returns the number of non-empty
   shard buckets appended. The pairs array is reordered in place; on
   return pairs[offset .. offset+counts[s]] holds shard s's entries. The
   caller still owns the BtEntry value strings (one allocation per pair,
   freed exactly once after the build). */
static int partition_pairs_by_idx_shard(BtEntry *pairs, size_t count,
                                        const char *db_root, const char *object,
                                        const char *field, int splits,
                                        IdxBuildArg *out_args) __attribute__((unused));
static int partition_pairs_by_idx_shard(BtEntry *pairs, size_t count,
                                        const char *db_root, const char *object,
                                        const char *field, int splits,
                                        IdxBuildArg *out_args) {
    int n = index_splits_for(splits);
    /* First pass: tally per-shard sizes. */
    size_t *counts = calloc((size_t)n, sizeof(size_t));
    if (!counts) return 0;
    for (size_t i = 0; i < count; i++) {
        int s = idx_shard_for_hash(pairs[i].hash, splits);
        counts[s]++;
    }
    /* Compute prefix-sum offsets. */
    size_t *offsets = calloc((size_t)n, sizeof(size_t));
    if (!offsets) { free(counts); return 0; }
    size_t acc = 0;
    for (int s = 0; s < n; s++) { offsets[s] = acc; acc += counts[s]; }
    /* Second pass: scatter pairs into a temporary array. */
    BtEntry *tmp = malloc(count * sizeof(BtEntry));
    if (!tmp) { free(counts); free(offsets); return 0; }
    size_t *cursor = calloc((size_t)n, sizeof(size_t));
    if (!cursor) { free(counts); free(offsets); free(tmp); return 0; }
    for (size_t i = 0; i < count; i++) {
        int s = idx_shard_for_hash(pairs[i].hash, splits);
        tmp[offsets[s] + cursor[s]++] = pairs[i];
    }
    memcpy(pairs, tmp, count * sizeof(BtEntry));
    free(tmp);
    free(cursor);

    int out_count = 0;
    for (int s = 0; s < n; s++) {
        if (counts[s] == 0) continue;
        build_idx_path(out_args[out_count].ipath, sizeof(out_args[out_count].ipath),
                       db_root, object, field, s);
        out_args[out_count].pairs = pairs + offsets[s];
        out_args[out_count].pair_count = counts[s];
        out_count++;
    }
    free(counts);
    free(offsets);
    return out_count;
}

/* ---- Fast bulk insert using mmap ---- */

/* Bump/arena allocator for phase-1 record buffers. Replaces 2 mallocs per
   record (id + typed payload) with O(1) pointer-advance into pre-allocated
   slabs. Freed as a whole after phase-2 workers join — arena pointers must
   not outlive arena_free(). Chain grows by doubling when the current slab
   fills; initial slab is 256 KB so tiny inputs don't over-reserve and big
   inputs reach ~1 GB in ~12 doublings (≪ the malloc count avoided). */
typedef struct BulkArena {
    struct BulkArena *next;
    uint8_t *base;
    size_t used;
    size_t cap;
} BulkArena;

#ifdef TEST_BUILD
/* Deterministic OOM injection (like bulk_upd_json_test_set_fail_alloc):
   the Nth arena_new call after arming fails (N=1 is the next call). */
static _Atomic int g_arena_fail_n = 0;
void bulk_arena_test_set_fail(int n) { atomic_store(&g_arena_fail_n, n); }
#else
#define arena_test_should_fail() 0
#endif





/* Per-record buffered state collected in phase 1 (parse) and consumed in
   phase 2 (write). `id` and `payload` are pointers into a BulkArena owned
   by the caller of phase-1; records stay valid until arena_free() runs,
   which happens after all phase-2 workers have joined. */
typedef struct {
    char     *id;           /* arena-owned null-terminated key */
    uint8_t  *payload;      /* arena-owned typed payload (ts->total_size bytes) */
    size_t    klen;
    uint8_t   hash[16];
    int       start_slot;
    int       shard_id;
    /* Per-record strict-insert override. Auto-key bulk-insert flags
       omit-key records (server-generated UUID / seq.next) so a collision
       with a pre-existing key surfaces as condition_not_met instead of
       a silent update. Zero for provided-key records → today's
       behaviour (upsert under the batch-level opts). */
    int       if_not_exists;
} BulkInsRecord;

/* A B-tree old-entry deletion staged in prepare_window for real dispatch
   in apply_window, once the window's marker is durable. `key` may point
   into the worker's per-record arena (stable for the worker's lifetime)
   or into a buffer tracked via bw_track_buf — either way it outlives the
   window's apply call. */
typedef struct {
    int            fi;
    const uint8_t *key;
    size_t         klen;
    uint8_t        hash[16];
} BtDeleteOp;

/* Per-shard bucket + worker arguments. Each bucket targets exactly one
   shard so the worker can take the kf-shard wrlock **once**, write every
   record in the bucket, and release **once** — avoiding per-record
   acquire/release churn. Idx entries are collected into per-worker arrays
   and merged into the caller's global arrays after the worker returns
   (same shape bulk-delete's bulk_del_shard_worker uses). */
typedef struct {
    const char     *db_root;
    const char     *object;
    const Schema   *sch;
    const TypedSchema *ts;
    int             shard_id;
    /* Per-record data — all records target sw->shard_id */
    BulkInsRecord  *records;
    size_t          count;
    /* Index metadata (read-only, shared across workers) */
    int             nidx;
    const char    (*idx_fields)[256];
    const int     (*idx_field_indices)[16];
    const int      *idx_field_counts;
    const int      *idx_is_composite;
    const enum IndexType *idx_types;  /* [nidx] — IT_BTREE / IT_BITMAP / IT_TRIGRAM */
    /* Two-phase window state for indexed bulk inserts. Populated by
       v2_bulk_ins_prepare_window (before the window's kf marker exists),
       consumed (possibly more than once, on replay) by
       v2_bulk_ins_apply_window (after the marker is durable, before kf is
       committed for surviving records), and torn down exactly once by
       v2_bulk_ins_commit_done (after the window is fully durable).
       Bitmap cap rejection happens inside bitmap_prepare_window_add,
       synchronously, so it can reject an individual record before any
       marker exists — closing both the daemon-abort bug (rejection after
       fsync) and the silent-cap-bypass bug (old post-return bm_pairs
       flush that ignored bm_set's return value). */
    BitmapPrepareWindow bw_window;
    void               **bw_bufs;       /* malloc'd key buffers kept alive
                                            until apply/abort closes bw_window */
    size_t               bw_nbufs, bw_bufs_cap;
    /* Trigram mutations deferred from prepare_window to apply_window — a
       real on-disk write must not happen before the window's marker is
       durable (a sibling bitmap field's cap rejection must not leave a
       half-written record). */
    UpdateIdxArg        *tg_ops;
    size_t                tg_nops, tg_cap;
    /* B-tree old-entry deletions deferred from prepare_window to
       apply_window — same rationale as tg_ops: a durable btree delete
       must not happen before the window's marker is durable. New-entry
       inserts still accumulate in idx_pairs[]/idx_pair_counts[] below,
       but apply_window now flushes (merges to disk) and resets them once
       per window instead of the caller deferring the merge until after
       every kf-shard worker in the whole bulk op has finished — that gap
       let a crash between this window's marker-clear and the old
       post-join Phase 4 merge lose btree entries for records already
       durable in kf. */
    BtDeleteOp          *bt_del_ops;
    size_t                bt_del_nops, bt_del_cap;
    /* Results (written by worker) */
    int             inserted;   /* new keys — updates do NOT increment */
    int             errors;
    /* CAS: when if_not_exists is set, an existing-key probe match is treated
       as a no-op and counted in `skipped` instead of overwriting. */
    int             if_not_exists;
    int             skipped;
    /* Phase-2 profiling: total worker wall time and time spent inside
       time spent growing a shard. Aggregated post-join to show "of this much
       Phase 2 time, X ms was grow." Helps isolate rehash cost. */
    uint64_t        wall_ms;
    uint64_t        grow_ms;
    int             grow_count;
    /* Per-worker index entry collection; merged post-phase into caller's
       global arrays. Each BtEntry.value is an owned malloc'd string. */
    BtEntry       **idx_pairs;        /* [nidx] */
    size_t         *idx_pair_counts;  /* [nidx] */
    size_t         *idx_pair_caps;    /* [nidx] */
    /* Deferred-request input. The worker prepares it (batch, per-record
       ctxs, arenas, hook context — all owned HERE and alive until
       bulk_ins_finish_request); the command thread executes one request
       over every worker's input and only then folds results and frees. */
    SlotcaskBulkShardInput input;
    int             prepared;
    SlotcaskBulkRec *batch;
    void            *ctxs;      /* V2BulkInsCtx[], freed opaque here */
    int             *kf_shard_of;
    int             *kf_counts;
    int             *kf_offsets;
    int             *kf_cursors;
    uint8_t         *old_arena;
    void *index_memory; /* packed bridge only: existing BulkMemory owner */
} BulkInsShardWork;

/* Build the shard→worker mapping used by every parallel bulk path
   (bulk_insert, bulk_update, bulk_delete, both JSON and delimited
   forms). Caller has already counted records per shard.

   Outputs (caller frees on success):
     *out_worker_shards[g] = shard id of worker g, g in [0, nworkers)
     *out_s2w[s]           = worker index for shard s, or -1 if shard
                             has no records

   Returns nworkers (count of non-empty shards) on success. Returns -1
   on OOM with both out pointers set to NULL (nothing to free).

   Callers still allocate their own per-worker struct array (the
   worker type varies per bulk operation) and the per-worker record
   buffer (record type varies). This helper covers only the
   stable scaffolding. */
static int build_shard_worker_map(const int *shard_counts, int splits,
                                   int **out_worker_shards, int **out_s2w) {
    int nw = 0;
    for (int s = 0; s < splits; s++) if (shard_counts[s] > 0) nw++;
    int *worker_shards = nw > 0 ? malloc((size_t)nw * sizeof(int)) : NULL;
    int *s2w = malloc((size_t)splits * sizeof(int));
    if ((nw > 0 && !worker_shards) || !s2w) {
        LOG_ERROR(LOG_SUB_QUERY, "build_shard_worker_map: malloc failed (splits=%d, nw=%d)", splits, nw);
        free(worker_shards); free(s2w);
        *out_worker_shards = NULL; *out_s2w = NULL;
        return -1;
    }
    int g = 0;
    for (int s = 0; s < splits; s++) {
        if (shard_counts[s] > 0) { worker_shards[g] = s; s2w[s] = g; g++; }
        else                       s2w[s] = -1;
    }
    *out_worker_shards = worker_shards;
    *out_s2w = s2w;
    return nw;
}

/* === Slotcask bulk-insert worker ===
 *
 * Each parent worker's records all hash to one kf-shard, so successive
 * slotcask_upsert_with_hooks calls all touch the same kfcache rwlock —
 * uncontended across workers (different shards). Stream-lock contention
 * across workers is per-stream try_lock; brief enough not to need the
 * prototype's all-or-nothing batching for the first cut.
 *
 * Per-record pre_commit hook fires under the kf-shard wrlock and accumulates
 * idx entries into sw->idx_pairs[fi]. The downstream per-field btree merge
 * phase (idx_build_field_worker) is reused unchanged. */
typedef struct {
    BulkInsShardWork *sw;
    BulkInsRecord    *rec;
    /* Per-worker arena for OLD index-key extraction during update upserts.
       nidx slots × INDEX_KEY_MAX bytes, reused across every record in this
       worker's kf-shard slice. pre_commits fire serially under the kf
       wrlock so reuse is safe. NEW keys can't share the arena — they're
       queued into sw->idx_pairs[fi] and consumed by btree_bulk_merge
       after the pre_commit returns. */
    uint8_t          *old_arena;
    size_t            old_arena_slot;
    void *original_user;
    int (*original_compute)(const SlotcaskOldRecord *, SlotcaskBulkRec *);
    int error;
} V2BulkInsCtx;

/* value_compute hook: corrects :auto_create fields before the segment
   write happens (Phase 1c of slotcask_bulk_upsert_in_kfshard — strictly
   before the record is persisted, unlike pre_commit which fires after).
   Only installed when the schema actually declares an auto_create field
   (see has_ac gate in bulk_insert_shard_worker_v2), so ordinary bulk
   inserts never pay for this.

   rec->value already holds the fully-encoded payload from phase 1
   (typed_encode_defaults for the JSON path; direct encode_field_len for
   the delimited/CSV path) — that encode had no way to know whether this
   key already exists, so any auto_create field in it is either a fresh
   now() stamp (client omitted it) or whatever the client explicitly
   supplied. This hook is the only place that corrects it:
     - key already existed (old->value != NULL): the field must NOT change
       on an upsert — restore the original bytes from the old record.
     - key is a genuine fresh insert (old == NULL): re-stamp now()
       unconditionally, even though phase 1 may already have stamped it,
       to overwrite any client-supplied override, matching cmd_insert_v2. */

/* Per-window staged index state. With deferred request batching the
 * coordinator's publish pass prepares EVERY window of a shard before any
 * finalize pass applies one, so per-window staging can no longer live in
 * the shared worker struct — prepare_window allocates one of these, the
 * coordinator carries it opaquely, apply_window consumes it, and exactly
 * one terminal hook frees it. */
typedef struct {
    BitmapPrepareWindow bw_window;
    void               **bw_bufs;       /* malloc'd key buffers kept alive
                                            until apply/abort closes
                                            bw_window */
    size_t               bw_nbufs, bw_bufs_cap;
    UpdateIdxArg        *tg_ops;
    size_t                tg_nops, tg_cap;
    BtDeleteOp          *bt_del_ops;
    size_t                bt_del_nops, bt_del_cap;
    /* Per-window staged new-entry B-tree inserts, [nidx] arrays —
       consumed (flushed) by apply_window, freed by the terminal hook. */
    BtEntry             **idx_pairs;
    size_t               *idx_pair_counts;
    size_t               *idx_pair_caps;
    int                   nidx;
    void *memory;
    size_t bitmap_charge;
} BulkInsWindowState;

/* Packed windows charge actual allocation requests through the adapter's
 * existing atomic BulkMemory. Legacy windows keep their prior allocation policy. */
typedef union {max_align_t align;size_t size;} BwAllocationHeader;
static void *bw_alloc(BulkInsWindowState *ws,size_t n){
    if(!ws->memory)return malloc(n);
    if(n>SIZE_MAX-sizeof(BwAllocationHeader)){errno=ENOMEM;return NULL;}
    size_t charge=n+sizeof(BwAllocationHeader);
    if(typed_bulk_index_memory_take(ws->memory,charge))return NULL;
    BwAllocationHeader *h=malloc(charge);
    if(!h){typed_bulk_index_memory_release(ws->memory,charge);return NULL;}
    h->size=n;return h+1;
}
static void bw_dealloc(BulkInsWindowState *ws,void *ptr){
    if(!ptr)return;
    if(!ws->memory){free(ptr);return;}
    BwAllocationHeader *h=(BwAllocationHeader*)ptr-1;
    typed_bulk_index_memory_release(ws->memory,h->size+sizeof(*h));free(h);
}
static void *bw_calloc(BulkInsWindowState *ws,size_t count,size_t size){
    if(size&&count>SIZE_MAX/size){errno=ENOMEM;return NULL;}
    size_t n=count*size;void *p=bw_alloc(ws,n);if(p)memset(p,0,n);return p;
}
static void *bw_realloc(BulkInsWindowState *ws,void *ptr,size_t n){
    if(!ws->memory)return realloc(ptr,n);
    void *p=bw_alloc(ws,n);if(!p)return NULL;
    if(ptr){size_t old=((BwAllocationHeader*)ptr-1)->size;memcpy(p,ptr,old<n?old:n);bw_dealloc(ws,ptr);}
    return p;
}
static int bw_record_key(BulkInsWindowState *ws,const TypedSchema *ts,
    const uint8_t *value,size_t n,uint8_t flags,const char *field,int payload,
    int old,uint8_t **out,size_t *len){
    *out=NULL;*len=0;
    int rc=payload?typed_index_record_payload_into(ts,value,n,flags,field,NULL,0,len):
        old?typed_index_old_record_key_into(ts,value,n,flags,field,NULL,0,len):
        typed_index_record_key_into(ts,value,n,flags,field,NULL,0,len);
    if(rc!=1)return rc;
    size_t need=*len;uint8_t *p=bw_alloc(ws,need?need:1);if(!p){*len=0;return -1;}
    rc=payload?typed_index_record_payload_into(ts,value,n,flags,field,p,need,len):
        typed_index_record_key_into(ts,value,n,flags,field,p,need,len);
    if(rc!=1){bw_dealloc(ws,p);*len=0;return rc;}
    *out=p;return 1;
}

typedef struct {
    void *memory;uint8_t *buffers[MAX_FIELDS];size_t lengths[MAX_FIELDS];
    int have[MAX_FIELDS],owned[MAX_FIELDS];
} BwOldKeys;
static void bw_old_keys_free(BwOldKeys *keys){
    BulkInsWindowState temporary={.memory=keys->memory};
    for(int i=0;i<MAX_FIELDS;i++)if(keys->owned[i]==1)bw_dealloc(&temporary,keys->buffers[i]);
}

/* The worker-ctx variants of the staging helpers, re-keyed onto the
   per-window state. */
static int bw_track_buf(BulkInsWindowState *ws, void *buf) {
    if (!buf) return 0;
    if (ws->bw_nbufs >= ws->bw_bufs_cap) {
        size_t ncap = ws->bw_bufs_cap ? ws->bw_bufs_cap * 2 : 32;
        void **t = bw_realloc(ws,ws->bw_bufs, ncap * sizeof(void *));
        if (!t) return -1;
        ws->bw_bufs = t;
        ws->bw_bufs_cap = ncap;
    }
    ws->bw_bufs[ws->bw_nbufs++] = buf;
    return 0;
}

static void bw_free_bufs(BulkInsWindowState *ws) {
    for (size_t i = 0; i < ws->bw_nbufs; i++) bw_dealloc(ws,ws->bw_bufs[i]);
    bw_dealloc(ws,ws->bw_bufs);
    ws->bw_bufs = NULL;
    ws->bw_nbufs = 0;
    ws->bw_bufs_cap = 0;
}

static uint8_t *bw_stage_old_key(BulkInsWindowState *ws, uint8_t *buf,
                                  size_t len, int owned, int *err) {
    if (owned) {
        if (bw_track_buf(ws, buf) != 0) { bw_dealloc(ws,buf); *err = -1; return NULL; }
        return buf;
    }
    uint8_t *copy = bw_alloc(ws,len);
    if (!copy) { *err = -1; return NULL; }
    memcpy(copy, buf, len);
    if (bw_track_buf(ws, copy) != 0) { bw_dealloc(ws,copy); *err = -1; return NULL; }
    return copy;
}

static int tg_track_op(BulkInsWindowState *ws, const UpdateIdxArg *a) {
    if (ws->tg_nops >= ws->tg_cap) {
        size_t ncap = ws->tg_cap ? ws->tg_cap * 2 : 32;
        UpdateIdxArg *t = bw_realloc(ws,ws->tg_ops, ncap * sizeof(UpdateIdxArg));
        if (!t) return -1;
        ws->tg_ops = t;
        ws->tg_cap = ncap;
    }
    ws->tg_ops[ws->tg_nops++] = *a;
    return 0;
}

static int bt_track_del_op(BulkInsWindowState *ws, const BtDeleteOp *op) {
    if (ws->bt_del_nops >= ws->bt_del_cap) {
        size_t ncap = ws->bt_del_cap ? ws->bt_del_cap * 2 : 32;
        BtDeleteOp *t = bw_realloc(ws,ws->bt_del_ops, ncap * sizeof(BtDeleteOp));
        if (!t) return -1;
        ws->bt_del_ops = t;
        ws->bt_del_cap = ncap;
    }
    ws->bt_del_ops[ws->bt_del_nops++] = *op;
    return 0;
}

/* Releases every resource a window's prepare_window staged, and the
 * window-state object itself. Used by all three terminal routes
 * (commit_done / release_window / abort_window) and by prepare's own
 * all-rejected teardown. NULL state is a no-op (prepare staged nothing). */
static void v2_bulk_ins_window_release_state(void *ctx, void *window_state) {
    BulkInsShardWork *sw = ctx;
    BulkInsWindowState *ws = window_state;
    (void)sw;
    if (!ws) return;
    bitmap_prepare_window_free(&ws->bw_window);
    bw_free_bufs(ws);
    bw_dealloc(ws,ws->tg_ops); ws->tg_ops = NULL; ws->tg_nops = ws->tg_cap = 0;
    bw_dealloc(ws,ws->bt_del_ops); ws->bt_del_ops = NULL; ws->bt_del_nops = ws->bt_del_cap = 0;
    if (ws->idx_pairs) {
        for (int fi = 0; fi < ws->nidx; fi++) {
            for (size_t k = 0; ws->idx_pair_counts && k < ws->idx_pair_counts[fi]; k++)
                bw_dealloc(ws,(void *)ws->idx_pairs[fi][k].value);
            bw_dealloc(ws,ws->idx_pairs[fi]);
        }
        bw_dealloc(ws,ws->idx_pairs);
    }
    bw_dealloc(ws,ws->idx_pair_counts);
    bw_dealloc(ws,ws->idx_pair_caps);
    typed_bulk_index_memory_release(ws->memory,ws->bitmap_charge+sizeof(*ws));
    free(ws);
}

/* Two-phase replacement for the old single-phase bulk index hook.
 *
 * prepare_window fires once per commit window, on the bulk worker thread,
 * BEFORE the window's kf marker exists. For every active record it builds
 * the new/old index keys (same logic the old single-phase hook used) and:
 *   - IT_BTREE: old-entry deletes are queued into sw->bt_del_ops and
 *     new-entry inserts accumulate into sw->idx_pairs[fi] — both are
 *     dispatched for real only by apply_window, once this window's
 *     marker is durable (deleting/merging during prepare would be a
 *     durable mutation for a record that might still be rejected).
 *   - IT_TRIGRAM: queue into sw->tg_ops for apply_window instead of
 *     dispatching immediately — a sibling bitmap field's cap rejection
 *     must not leave a real trigram mutation for a rejected record.
 *   - IT_BITMAP: bitmap_prepare_window_add both validates the cap AND
 *     queues the real set/clear for apply_window. A per-record cap
 *     rejection here happens before any marker exists, so it can never
 *     reach the post-fsync fail-closed/abort() path.
 *
 * A record can have index diffs queued for one field and then hit a
 * rejection on a later field; since fields for one record are processed
 * back-to-back, any partially-queued state for that record is unwound
 * before moving to the next record: tg_ops and bt_del_ops are truncated
 * back to this record's start, idx_pairs[fi] counts are truncated back
 * per field (freeing the now-orphaned key buffers), and the bitmap
 * window's ops/pending-value bookkeeping is rolled back to a checkpoint
 * taken before this record's first field — so a rejected record leaves
 * zero staged index side effects across every field type. */
static int v2_bulk_ins_prepare_window(SlotcaskBulkRec *recs, const size_t *active,
                                       size_t nactive, void *ctx,
                                       void **out_window_state) {
    BulkInsShardWork *sw = (BulkInsShardWork *)ctx;
    *out_window_state = NULL;
    if (sw->nidx == 0 || nactive == 0) return 0;

    if(typed_bulk_index_memory_take(sw->index_memory,sizeof(BulkInsWindowState)))return -1;
    BulkInsWindowState *ws = calloc(1, sizeof(*ws));
    if (!ws){typed_bulk_index_memory_release(sw->index_memory,sizeof(*ws));return -1;}
    ws->memory=sw->index_memory;
    ws->nidx = sw->nidx;
    ws->idx_pairs = bw_calloc(ws,(size_t)sw->nidx, sizeof(BtEntry *));
    ws->idx_pair_counts = bw_calloc(ws,(size_t)sw->nidx, sizeof(size_t));
    ws->idx_pair_caps = bw_calloc(ws,(size_t)sw->nidx, sizeof(size_t));
    if (!ws->idx_pairs || !ws->idx_pair_counts || !ws->idx_pair_caps) {
        v2_bulk_ins_window_release_state(sw, ws);
        return -1;
    }
    for (int fi = 0; fi < sw->nidx; fi++) {
        ws->idx_pair_caps[fi] = 64;
        ws->idx_pairs[fi] = bw_alloc(ws,ws->idx_pair_caps[fi] * sizeof(BtEntry));
        if (!ws->idx_pairs[fi]) {
            v2_bulk_ins_window_release_state(sw, ws);
            return -1;
        }
    }

    size_t bitmap_bytes=(size_t)sw->nidx*sizeof(BitmapWindowEntry)+
        nactive*(size_t)sw->nidx*sizeof(BitmapWindowOp);
    if(typed_bulk_index_memory_take(ws->memory,bitmap_bytes)){
        v2_bulk_ins_window_release_state(sw,ws);return -1;
    }
    ws->bitmap_charge=bitmap_bytes;
    if (bitmap_prepare_window_init(&ws->bw_window, (size_t)sw->nidx,
                                    nactive * (size_t)sw->nidx) != 0) {
        v2_bulk_ins_window_release_state(sw, ws);
        return -1;
    }
    snprintf(ws->bw_window.db_root, sizeof(ws->bw_window.db_root),
             "%s", sw->db_root);
    snprintf(ws->bw_window.object, sizeof(ws->bw_window.object),
             "%s", sw->object);
    ws->bw_window.splits = sw->sch->splits;

    /* F1: preopen every bitmap field's handle for this window's shard in
       canonical lock order before any record is processed. The old lazy
       first-use open could pin kind while later opening flag and deadlock
       against canonical readers / the keyfile-rebuild remap. */
    {
        BitmapPreopenField pre[MAX_FIELDS];
        size_t npre = 0;
        for (int fi = 0; fi < sw->nidx && npre < MAX_FIELDS; fi++) {
            if (!sw->idx_types || sw->idx_types[fi] != IT_BITMAP) continue;
            int fi_t = typed_field_index(sw->ts, sw->idx_fields[fi]);
            pre[npre].field = sw->idx_fields[fi];
            pre[npre].bool_fastpath = (fi_t >= 0 && sw->ts->fields[fi_t].type == FT_BOOL);
            npre++;
        }
        char pre_err[128];
        if (bitmap_prepare_window_preopen(&ws->bw_window, sw->shard_id,
                                          pre, npre, pre_err, sizeof(pre_err)) != 0) {
            v2_bulk_ins_window_release_state(sw, ws);
            return -1;
        }
    }

    size_t nsurvive = 0;

    for (size_t a = 0; a < nactive; a++) {
        size_t j = active[a];
        SlotcaskBulkRec *rec = &recs[j];
        V2BulkInsCtx *rctx = (V2BulkInsCtx *)rec->user_ctx;
        BulkInsRecord *r = rctx->rec;
        int is_update = rec->was_update;
        SlotcaskOldRecord old_rec;
        const SlotcaskOldRecord *old = NULL;
        if (is_update && rec->old_value) {
            old_rec.value = (const uint8_t *)rec->old_value;
            old_rec.vlen  = (uint32_t)rec->old_vlen;
            old_rec.hdr2  = rec->old_hdr2;
            old = &old_rec;
        }

        BwOldKeys old_keys __attribute__((cleanup(bw_old_keys_free)))={.memory=ws->memory};
        uint8_t **old_idx_bufs=old_keys.buffers;
        size_t *old_idx_lens=old_keys.lengths;
        int *old_idx_have=old_keys.have,*old_idx_owned=old_keys.owned;
        if (is_update && old) {
            for (int fi = 0; fi < sw->nidx; fi++) {
                /* Old-key encoding must match the new-key encoding for
                   this index type: tiered BTRI keys for btree, raw
                   payloads for bitmap/trigram (§2.11) — otherwise the
                   unchanged-compare misses and deletes target keys that
                   were never written. */
                enum IndexType old_ity =
                    sw->idx_types ? sw->idx_types[fi] : IT_BTREE;
                int rc;
                if(ws->memory){
                    old_idx_have[fi]=bw_record_key(ws,sw->ts,old->value,old->vlen,
                        old->hdr2,sw->idx_fields[fi],old_ity!=IT_BTREE,1,
                        &old_idx_bufs[fi],&old_idx_lens[fi]);
                    old_idx_owned[fi]=old_idx_have[fi];continue;
                }
                if (old_ity == IT_BTREE) {
                    if (rctx->old_arena) {
                        uint8_t *slot = rctx->old_arena + (size_t)fi * rctx->old_arena_slot;
                        rc = typed_index_old_record_key_into(
                            sw->ts, old->value, old->vlen, old->hdr2, sw->idx_fields[fi],
                            slot, rctx->old_arena_slot, &old_idx_lens[fi]);
                        if (rc == 1) {
                            old_idx_bufs[fi] = slot;
                            old_idx_have[fi] = 1;
                        } else if (rc == -1) {
                            old_idx_have[fi] = typed_index_old_record_key(
                                sw->ts, old->value, old->vlen, old->hdr2, sw->idx_fields[fi],
                                &old_idx_bufs[fi], &old_idx_lens[fi]);
                            old_idx_owned[fi] = old_idx_have[fi];
                        }
                    } else {
                        old_idx_have[fi] = typed_index_old_record_key(
                            sw->ts, old->value, old->vlen, old->hdr2, sw->idx_fields[fi],
                            &old_idx_bufs[fi], &old_idx_lens[fi]);
                        old_idx_owned[fi] = old_idx_have[fi];
                    }
                    continue;
                }
                rc = 0;
                uint8_t *slot = NULL;
                if (rctx->old_arena) {
                    slot = rctx->old_arena + (size_t)fi * rctx->old_arena_slot;
                    rc = build_index_payload_from_record_into(
                        sw->ts, old->value, old->vlen, old->hdr2,
                        sw->idx_fields[fi], slot, rctx->old_arena_slot,
                        &old_idx_lens[fi]);
                    if (rc == 1) old_idx_bufs[fi] = slot;
                }
                if (rc != 1) {
                    old_idx_have[fi] = build_index_payload_from_record(
                        sw->ts, old->value, old->vlen, old->hdr2, sw->idx_fields[fi],
                        &old_idx_bufs[fi], &old_idx_lens[fi]);
                    old_idx_owned[fi] = old_idx_have[fi];
                } else {
                    old_idx_have[fi] = 1;
                }
            }
        }

        for(int fi=0;fi<sw->nidx;fi++)if(old_idx_have[fi]<0){
            v2_bulk_ins_window_release_state(sw,ws);return -1;
        }

        const uint8_t *new_value = (const uint8_t *)rec->value;
        int record_rejected = 0;
        size_t tg_start = ws->tg_nops;
        size_t bt_del_start = ws->bt_del_nops;
        size_t idx_pair_start[MAX_FIELDS];
        for (int fi = 0; fi < sw->nidx; fi++) idx_pair_start[fi] = ws->idx_pair_counts[fi];
        BitmapWindowCheckpoint bm_cp;
        bitmap_prepare_window_checkpoint(&ws->bw_window, &bm_cp);

        for (int fi = 0; fi < sw->nidx; fi++) {
            enum IndexType itype = sw->idx_types ? sw->idx_types[fi] : IT_BTREE;
            uint8_t *key_buf = NULL;
            size_t   key_len = 0;

            /* BTRI (§2.11): tiered key from the validated record view —
               build_index_key_from_record_into handles composite specs,
               the [tier][payload] layout, and NULL fields. Capacity is
               checked inside; ENOBUFS is an error, never a missing key. */
            if (itype == IT_BTREE) {
                int rc=ws->memory?bw_record_key(ws,sw->ts,new_value,rec->vlen,
                    rec->hdr2,sw->idx_fields[fi],0,0,&key_buf,&key_len):
                    build_index_key_from_record(sw->ts,new_value,rec->vlen,
                    rec->hdr2,sw->idx_fields[fi],&key_buf,&key_len);
                if(rc<0){v2_bulk_ins_window_release_state(sw,ws);return -1;}
            } else if(ws->memory){
                int rc=bw_record_key(ws,sw->ts,new_value,rec->vlen,rec->hdr2,
                    sw->idx_fields[fi],1,0,&key_buf,&key_len);
                if(rc<0){v2_bulk_ins_window_release_state(sw,ws);return -1;}
            } else {
                /* Bitmap/trigram payloads keep the raw untiered layout.
                   build_index_payload_from_record_into resolves the
                   field base through the record's own generation view —
                   the field region sits nullmap_prefix into the record
                   for nullmap records. NULL fields produce no key. */
                size_t cap = BT_MAX_VAL_LEN;
                key_buf = malloc(cap);
#ifdef TEST_BUILD
                if (g_bulk_ins_payload_test_fail_alloc > 0 &&
                    --g_bulk_ins_payload_test_fail_alloc == 0) {
                    free(key_buf);
                    key_buf = NULL;
                    errno = ENOMEM;
                }
#endif
                if (!key_buf) {
                    /* Staging allocation failure: abort preparation, never
                       treat the missing buffer as an absent key. */
                    v2_bulk_ins_window_release_state(sw,ws);return -1;
                }
                int rc = build_index_payload_from_record_into(
                    sw->ts, new_value, rec->vlen, rec->hdr2,
                    sw->idx_fields[fi], key_buf, cap, &key_len);
                if (rc == -1) {
                    /* Payload beyond the staging cap (a trigram varchar
                       past ~542 content bytes): the key exists — rebuild
                       through the allocating wrapper, never skip it (a
                       skipped NEW key drops the index update silently,
                       §2.11). A wrapper allocation or decoding failure
                       also aborts preparation. */
                    bw_dealloc(ws,key_buf); key_buf = NULL; key_len = 0;
                    int prc = build_index_payload_from_record(
                        sw->ts, new_value, rec->vlen, rec->hdr2,
                        sw->idx_fields[fi], &key_buf, &key_len);
                    if (prc < 0) {
                        v2_bulk_ins_window_release_state(sw,ws);return -1;
                    }
                    if (prc == 0) { key_buf = NULL; key_len = 0; }
                } else if (rc != 1) {
                    /* 0 = NULL/empty field: a legitimately absent key. */
                    bw_dealloc(ws,key_buf); key_buf = NULL; key_len = 0;
                }
            }

            int have_new  = (key_buf != NULL && key_len > 0);
            int unchanged = old_idx_have[fi] && have_new &&
                            key_len == old_idx_lens[fi] &&
                            memcmp(key_buf, old_idx_bufs[fi], key_len) == 0;

            int tg_bm_change = !unchanged && (have_new || old_idx_have[fi]);

            if (itype == IT_TRIGRAM) {
                if (tg_bm_change) {
                    int err = 0;
                    uint8_t *staged_old = old_idx_have[fi]
                        ? bw_stage_old_key(ws, old_idx_bufs[fi], old_idx_lens[fi],
                                          old_idx_owned[fi], &err)
                        : NULL;
                    if(old_idx_owned[fi]==1)old_idx_owned[fi]=0;
                    if (err) {
                        bw_dealloc(ws,key_buf);
                        v2_bulk_ins_window_release_state(sw, ws);
                        return -1;
                    }
                    UpdateIdxArg ta = {0};
                    ta.db_root = sw->db_root; ta.object = sw->object;
                    ta.field = sw->idx_fields[fi]; ta.splits = sw->sch->splits;
                    ta.new_key = have_new ? key_buf : NULL;
                    ta.new_len = have_new ? key_len : 0;
                    ta.old_key = staged_old;
                    ta.old_len = staged_old ? old_idx_lens[fi] : 0;
                    ta.hash = r->hash; ta.type = IT_TRIGRAM;
                    ta.sync_after = 0;  /* flush seam syncs once per touched
                                           (field, idx shard) after apply */
                    int tracked = !have_new || (bw_track_buf(ws, key_buf) == 0);
                    if (!tracked || tg_track_op(ws, &ta) != 0) {
                        if (!tracked) bw_dealloc(ws,key_buf);
                        v2_bulk_ins_window_release_state(sw, ws);
                        return -1;
                    }
                } else {
                    bw_dealloc(ws,key_buf);
                    if (old_idx_have[fi] && old_idx_owned[fi]){bw_dealloc(ws,old_idx_bufs[fi]);old_idx_owned[fi]=0;}
                }
                continue;
            }

            if (itype == IT_BITMAP) {
                if (tg_bm_change) {
                    int err = 0;
                    uint8_t *staged_old = old_idx_have[fi]
                        ? bw_stage_old_key(ws, old_idx_bufs[fi], old_idx_lens[fi],
                                          old_idx_owned[fi], &err)
                        : NULL;
                    if(old_idx_owned[fi]==1)old_idx_owned[fi]=0;
                    if (err) {
                        bw_dealloc(ws,key_buf);
                        v2_bulk_ins_window_release_state(sw, ws);
                        return -1;
                    }
                    UpdateIdxArg ba = {0};
                    ba.db_root = sw->db_root; ba.object = sw->object;
                    ba.field = sw->idx_fields[fi]; ba.splits = sw->sch->splits;
                    ba.new_key = have_new ? key_buf : NULL;
                    ba.new_len = have_new ? key_len : 0;
                    ba.old_key = staged_old;
                    ba.old_len = staged_old ? old_idx_lens[fi] : 0;
                    ba.hash = r->hash; ba.type = IT_BITMAP;
                    ba.kf_shard = rec->kf_shard; ba.kf_slot = rec->kf_slot;
                    char errf[128];
                    size_t old_pending_cap=0,pending_reserved=0;
                    if(ws->memory){
                        for(size_t k=0;k<ws->bw_window.n_entries;k++){
                            BitmapWindowEntry *e=&ws->bw_window.entries[k];
                            if(e->kf_shard==ba.kf_shard&&!strcmp(e->field,ba.field)){
                                if(e->npending==e->pending_cap)old_pending_cap=e->pending_cap;
                                else old_pending_cap=SIZE_MAX;
                                break;
                            }
                        }
                        if(have_new&&old_pending_cap!=SIZE_MAX){
                            pending_reserved=(old_pending_cap?2*old_pending_cap:8)*sizeof(BitmapPendingValue);
                            if(typed_bulk_index_memory_take(ws->memory,pending_reserved)){
                                bw_dealloc(ws,key_buf);v2_bulk_ins_window_release_state(sw,ws);return -1;
                            }
                        }
                    }
                    int rc = bitmap_prepare_window_add(&ws->bw_window, &ba,
                                                        errf, sizeof(errf));
                    if(pending_reserved){
                        size_t newcap=old_pending_cap;
                        for(size_t k=0;k<ws->bw_window.n_entries;k++){
                            BitmapWindowEntry *e=&ws->bw_window.entries[k];
                            if(e->kf_shard==ba.kf_shard&&!strcmp(e->field,ba.field)){newcap=e->pending_cap;break;}
                        }
                        if(newcap>old_pending_cap){
                            size_t oldbytes=old_pending_cap*sizeof(BitmapPendingValue);
                            typed_bulk_index_memory_release(ws->memory,oldbytes);
                            ws->bitmap_charge+=newcap*sizeof(BitmapPendingValue)-oldbytes;
                        }else typed_bulk_index_memory_release(ws->memory,pending_reserved);
                    }
                    if (rc == 0) {
                        if (have_new && bw_track_buf(ws, key_buf) != 0) {
                            bw_dealloc(ws,key_buf);
                            v2_bulk_ins_window_release_state(sw, ws);
                            return -1;
                        }
                    } else if (rc == 1) {
                        bw_dealloc(ws,key_buf);
                        record_rejected = 1;
                    } else {
                        bw_dealloc(ws,key_buf);
                        v2_bulk_ins_window_release_state(sw, ws);
                        return -1;
                    }
                } else {
                    bw_dealloc(ws,key_buf);
                    if (old_idx_have[fi] && old_idx_owned[fi]){bw_dealloc(ws,old_idx_bufs[fi]);old_idx_owned[fi]=0;}
                }
                if (record_rejected) break;
                continue;
            }

            /* IT_BTREE: old-entry delete and new-entry insert are both
               deferred to apply_window (queued here, dispatched only once
               the window's marker is durable). */
            if (old_idx_have[fi] && !unchanged) {
                int err = 0;
                uint8_t *staged_old = bw_stage_old_key(ws, old_idx_bufs[fi], old_idx_lens[fi],
                                                       old_idx_owned[fi], &err);
                if(old_idx_owned[fi]==1)old_idx_owned[fi]=0;
                if (err) {
                    bw_dealloc(ws,key_buf);
                    v2_bulk_ins_window_release_state(sw, ws);
                    return -1;
                }
                BtDeleteOp bdop;
                bdop.fi = fi;
                bdop.key = staged_old;
                bdop.klen = old_idx_lens[fi];
                memcpy(bdop.hash, r->hash, 16);
                if (bt_track_del_op(ws, &bdop) != 0) {
                    bw_dealloc(ws,key_buf);
                    v2_bulk_ins_window_release_state(sw, ws);
                    return -1;
                }
            } else if (old_idx_have[fi] && old_idx_owned[fi]) {
                bw_dealloc(ws,old_idx_bufs[fi]); old_idx_owned[fi]=0;
            }

            if (unchanged) { bw_dealloc(ws,key_buf); continue; }

            if (have_new) {
                if (ws->idx_pair_counts[fi] >= ws->idx_pair_caps[fi]) {
                    size_t new_cap = ws->idx_pair_caps[fi] ? ws->idx_pair_caps[fi] * 2 : 64;
                    /* Keep the existing entries live on OOM so the common
                       window-release path can free every already allocated
                       key.  xrealloc_or_bw_dealloc(ws,) releases only the outer
                       array, leaking those per-entry values. */
                    BtEntry *t = bw_realloc(ws,ws->idx_pairs[fi], new_cap * sizeof(BtEntry));
                    if (!t) {
                        /* Abort the whole pre-marker window rather than
                           silently committing records with a missing B-tree
                           entry for this field. */
                        LOG_ERROR(LOG_SUB_INDEX, "INDEX_OOM shard=%d field=%s (aborting bulk window)",
                                sw->shard_id, sw->idx_fields[fi]);
                        bw_dealloc(ws,key_buf);
                        v2_bulk_ins_window_release_state(sw, ws);
                        return -1;
                    }
                    ws->idx_pairs[fi] = t;
                    ws->idx_pair_caps[fi] = new_cap;
                }
                BtEntry *bp = &ws->idx_pairs[fi][ws->idx_pair_counts[fi]++];
                bp->value = (const char *)key_buf;
                bp->vlen  = key_len;
                memcpy(bp->hash, r->hash, 16);
            } else {
                bw_dealloc(ws,key_buf);
            }
        }

        if (record_rejected) {
            /* Undo every index diff this record staged so far, across all
               field types, so a rejected record leaves zero side effects. */
            ws->tg_nops = tg_start;
            ws->bt_del_nops = bt_del_start;
            for (int fi = 0; fi < sw->nidx; fi++) {
                for (size_t k = idx_pair_start[fi]; k < ws->idx_pair_counts[fi]; k++)
                    bw_dealloc(ws,(void *)ws->idx_pairs[fi][k].value);
                ws->idx_pair_counts[fi] = idx_pair_start[fi];
            }
            bitmap_prepare_window_rollback(&ws->bw_window, &bm_cp);
            recs[j].status = -1;
            continue;
        }
        nsurvive++;
    }

    /* Release the parked bitmap writer handles before this call returns:
       cache-entry locks must never span the deferred request's wave
       boundaries (publish → finalize). apply_window reopens each handle
       within its own call stack, serialized by the request's writer gate.
       The staged ops/pending bookkeeping is untouched. */
    bitmap_prepare_window_unpark(&ws->bw_window);

    if (nsurvive == 0) {
        /* Every active record in this window was individually rejected;
           apply_window will never be invoked for an empty active set, so
           release the window's staged resources here instead. */
        v2_bulk_ins_window_release_state(sw, ws);
        ws = NULL;
    }
    *out_window_state = ws;
    return 0;
}

/* apply_window fires once the window's kf marker is durable, before kf is
 * committed for the surviving records. Performs the real trigram, B-tree,
 * and bitmap mutations staged by prepare_window, in that order, always
 * running every staged op even if an earlier one fails (mirrors
 * bitmap_prepare_window_apply's own "keep going, report at the end"
 * pattern) so a partial apply doesn't leave some of this window's
 * surviving records indexed and others not. A non-zero return is always
 * a genuine I/O/OOM failure and is routed by the caller through the
 * existing degraded/replay path — the marker is retained, never a
 * policy rejection (those are handled entirely in prepare_window).
 *
 * MUST be safe to call more than once: forward replay after a post-marker
 * failure re-invokes this on the same staged state, so it never frees or
 * resets ws->tg_ops / ws->bt_del_ops / ws->idx_pairs / ws->bw_window —
 * re-running update_idx_fn/delete_index_entry/idx_build_field_worker on
 * already-converged entries is a no-op by construction of those
 * primitives. Release of the staged state happens once, later, in the
 * terminal hook — never here. */
static int v2_bulk_ins_apply_window(SlotcaskBulkRec *recs, const size_t *active,
                                     size_t nactive, void *ctx,
                                     void *window_state) {
    BulkInsShardWork *sw = (BulkInsShardWork *)ctx;
    BulkInsWindowState *ws = window_state;
    (void)recs; (void)active;
    if (sw->nidx == 0 || nactive == 0 || !ws) return 0;

    int rc = 0;

    for (size_t i = 0; i < ws->tg_nops; i++) {
        update_idx_fn(&ws->tg_ops[i]);
        if (ws->tg_ops[i].out_error) rc = -1;
    }

    for (size_t i = 0; i < ws->bt_del_nops; i++) {
        BtDeleteOp *op = &ws->bt_del_ops[i];
        if (delete_index_entry(sw->db_root, sw->object, sw->idx_fields[op->fi],
                               sw->sch->splits, op->key, op->klen, op->hash) != 0)
            rc = -1;
        idx_touch_record(sw->idx_fields[op->fi],
                         idx_shard_for_hash(op->hash, sw->sch->splits),
                         IT_BTREE, op->hash);
    }

    for (int fi = 0; fi < sw->nidx; fi++) {
        size_t count = ws->idx_pair_counts[fi];
        if (count == 0) continue;
        IdxFieldArg fa = {0};
        fa.db_root = sw->db_root; fa.object = sw->object;
        fa.field = sw->idx_fields[fi]; fa.splits = sw->sch->splits;
        fa.new_entries = ws->idx_pairs[fi]; fa.new_count = count;
        fa.memory=sw->index_memory;
        idx_build_field_worker(&fa);
        if (fa.out_error) rc = -1;
        for (size_t pi = 0; pi < count; pi++)
            idx_touch_record(sw->idx_fields[fi],
                             idx_shard_for_hash(ws->idx_pairs[fi][pi].hash,
                                                sw->sch->splits),
                             IT_BTREE, ws->idx_pairs[fi][pi].hash);
    }

    /* btree_bulk_merge/delete_index_entry only dirty mmap'd pages — they do
       not fsync. Durability for this window's btree/trigram/bitmap files is
       now owned by the post-apply flush seam (unique (field, shard) files
       synced exactly once, before the marker is cleared — invariant I1).
       btree_bulk_merge's rename-published shards are already durable; their
       recorded touch re-fdatasyncs an already-clean file, which is cheap
       and keeps one uniform rule. Never-touched shards are never synced, so
       bt_acquire(writer)'s O_CREAT can no longer materialize empty .idx
       files (index.c warns about exactly this). */
    /* Bitmap touches MUST be recorded here, not in prepare_window: the
       coordinator installs the TLS touch collector only around apply_window,
       so a prepare-time idx_touch_record is a silent no-op and the window
       would clear its marker with dirty bitmap pages (finding-1 regression).
       Entries are 1:1 with staged (field, kf shard) pairs. */
    for (size_t i = 0; i < ws->bw_window.n_entries; i++)
        idx_touch_record(ws->bw_window.entries[i].field,
                         ws->bw_window.entries[i].kf_shard,
                         IT_BITMAP, NULL);
    if (bitmap_prepare_window_apply(&ws->bw_window) != 0) rc = -1;
    return rc;
}

/* Releases every resource prepare_window staged for a window that will
 * never reach apply — the window's marker failed to become durable, so
 * none of its staged index mutations may run (that would index records
 * whose kf entry was never committed). Mirrors v2_bulk_ins_window_release
 * exactly; kept as a separate name for the SlotcaskBulkOpts.abort_window
 * wiring so the call site reads as "abort", not "release". */
static void v2_bulk_ins_abort_window(void *ctx, void *window_state) {
    v2_bulk_ins_window_release_state(ctx, window_state);
}

/* Fires at most once, after the window is fully durable (kf published, old
 * payloads tombstoned, marker cleared) — no further apply_window replay can
 * occur for this window past this point, so it is now safe to release what
 * prepare_window/apply_window staged. Mirrors v2_bulk_ins_window_release
 * exactly; kept as a separate name for the SlotcaskBulkOpts.commit_done
 * wiring so the call site reads as "commit done", not "release". */
static void v2_bulk_ins_commit_done(void *ctx, void *window_state) {
    v2_bulk_ins_window_release_state(ctx, window_state);
}

/* Post-M unresolved exit (EINPROGRESS): identical release body to
   commit_done/abort_window. The retained marker now owns durable recovery
   and gate/startup replay re-derives every mutation from disk, so the
   staged state must be released now even though durability never
   confirmed. Kept as a distinct name so the SlotcaskBulkOpts.release_window
   wiring reads honestly. */
static void v2_bulk_ins_release_window(void *ctx, void *window_state) {
    v2_bulk_ins_window_release_state(ctx, window_state);
}

typedef struct {
    BulkInsShardWork sw;
    V2BulkInsCtx *contexts;
    BulkInsRecord *records;
    SlotcaskBulkRec *batch;
    size_t count;
} TypedBulkIndexBridge;
size_t typed_bulk_index_bridge_bytes(size_t count){
    size_t unit=sizeof(V2BulkInsCtx)+sizeof(BulkInsRecord);
    return count>(SIZE_MAX-sizeof(TypedBulkIndexBridge))/unit?SIZE_MAX:
        sizeof(TypedBulkIndexBridge)+count*unit;
}
static int typed_bulk_index_compute(const SlotcaskOldRecord *old,SlotcaskBulkRec *rec){
    V2BulkInsCtx *c=rec->user_ctx;
    rec->user_ctx=c->original_user;
    int rc=c->original_compute(old,rec);
    rec->user_ctx=c;
    return rc;
}
void *typed_bulk_index_bridge_create(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,const char fields[][256],
    const enum IndexType *types,int nidx,SlotcaskBulkShardInput *input,void *memory){
    size_t charge=typed_bulk_index_bridge_bytes(input->nrecs);
    if(typed_bulk_index_memory_take(memory,charge))return NULL;
    TypedBulkIndexBridge *b=calloc(1,sizeof(*b));
    if(!b){typed_bulk_index_memory_release(memory,charge);return NULL;}
    b->count=input->nrecs;b->batch=input->recs;
    b->contexts=calloc(b->count,sizeof(*b->contexts));
    b->records=calloc(b->count,sizeof(*b->records));
    if(!b->contexts||!b->records){free(b->contexts);free(b->records);free(b);typed_bulk_index_memory_release(memory,charge);return NULL;}
    b->sw=(BulkInsShardWork){.db_root=root,.object=object,.sch=sc,.ts=ts,
        .shard_id=input->kf_shard_id,.nidx=nidx,.idx_fields=fields,.idx_types=types,.index_memory=memory};
    for(size_t i=0;i<b->count;i++){
        compute_hash_raw(input->recs[i].key,input->recs[i].klen,b->records[i].hash);
        b->contexts[i]=(V2BulkInsCtx){.sw=&b->sw,.rec=&b->records[i],
            .original_user=input->recs[i].user_ctx,
            .original_compute=input->opts.upsert.value_compute};
        input->recs[i].user_ctx=&b->contexts[i];
    }
    SlotcaskBulkOpts *o=&input->opts.upsert;
    o->value_compute=typed_bulk_index_compute;
    o->prepare_window=v2_bulk_ins_prepare_window;o->apply_window=v2_bulk_ins_apply_window;
    o->commit_done=v2_bulk_ins_commit_done;o->abort_window=v2_bulk_ins_abort_window;
    o->release_window=v2_bulk_ins_release_window;o->bulk_hook_ctx=&b->sw;
    o->pre_commit_needs_old=1;o->has_indexed_fields=1;
    return b;
}
void typed_bulk_index_bridge_free(void *ptr){
    TypedBulkIndexBridge *b=ptr;if(!b)return;
    for(size_t i=0;i<b->count;i++)b->batch[i].user_ctx=b->contexts[i].original_user;
    typed_bulk_index_memory_release(b->sw.index_memory,typed_bulk_index_bridge_bytes(b->count));
    free(b->contexts);free(b->records);free(b);
}

/* Command-thread compaction: gather every prepared worker's input into
   one contiguous heap array (workers arrive one-per-shard in ascending
   shard order; the coordinator re-validates). Returns NULL when nothing
   is prepared or on OOM — check ninputs. */


/* Shared tail for every bulk command: exactly one synchronous deferred
   request over the compacted per-shard inputs. Pre-grow is NOT run here:
   it is folded into the request's own stage wave (under the coordinator's
   writer gates). A parallel pre-grow outside the request would let
   gate-blocked pregrow tasks from concurrent requests occupy every
   IO-pool worker, starving the running request's waves — a circular
   wait. pregrow_count is retained in the signature for call-site
   stability; slotcask_pregrow_kf remains for standalone (non-request)
   callers only. */
static int bulk_execute_prepared(SlotcaskDb *sdb,
                                 SlotcaskBulkShardInput *inputs,
                                 size_t ninputs, size_t pregrow_count) {
    (void)pregrow_count;
    if (ninputs == 0) return 0;
    return slotcask_bulk_request_execute(sdb, inputs, ninputs);
}


/* Command-thread tail for the insert trio: fold the request outcome into
   the worker counters, then release the worker-owned scratch. */

/* Probe + write every record in one shard's bucket under a single
   kf-shard wrlock held from start to finish. On shard-full, release the
   lock, grow the shard, reacquire, and retry the **same** record index —
   avoids per-record churn. Collects index entries into sw->idx_pairs for
   later merge/bulk-build. pthread-compatible signature: workers in
   different shard buckets never touch each other's shards, so the
   wrlocks are disjoint and no cross-worker coordination is needed. */

/* Internal: bulk insert from a json string already in memory (no file I/O) */
int cmd_bulk_insert_string(const char *db_root, const char *object, char *json_str, int if_not_exists);

/* ---- bulk_ins_run: shared parser + write body --------------------------
   Caller owns `data` (null-terminated, len bytes). This helper never
   frees or munmaps it — all cleanup of the buffer belongs to the caller.
   Returns 0 on success, 1 on parse/insert error (JSON error already
   written via OUT() before returning). */
static int bulk_ins_run(const char *db_root, const char *object,
                        const char *data, size_t len, int if_not_exists) {
    Schema sc = load_schema(db_root, object);
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        /* Missing/untyped/unparseable fields.conf (including the fail-closed
           over-long-line refusal) — there is no record layout to encode
           against, so the batch cannot proceed. */
        fprintf(stderr, "Error: object [%s] has no typed schema; cannot bulk-insert\n",
                object);
        return 1;
    }
    return typed_bulk_insert_command(db_root, object, &sc, ts, NULL, data, len,
                                     0, 0, if_not_exists);
}

int cmd_bulk_insert(const char *db_root, const char *object, const char *input,
                    int if_not_exists) {
    Schema schema = load_schema(db_root, object);
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        fprintf(stderr, "Error: object [%s] has no typed schema; cannot bulk-insert\n",
                object);
        return 1;
    }
    return typed_bulk_insert_source(db_root, object, &schema, ts, input, 0, 0,
                                    if_not_exists);
}

/* Bulk insert from a string already in memory — calls bulk_ins_run directly,
   no memfd/mmap dance needed. */
int cmd_bulk_insert_string(const char *db_root, const char *object, char *json_str,
                           int if_not_exists) {
    return bulk_ins_run(db_root, object, json_str, strlen(json_str), if_not_exists);
}

/* ========== BULK INSERT (DELIMITED TEXT FILE) ========== */

/* Shared body for bulk-insert-delimited (file + inline string entry points).
   Caller owns `data`/`size` lifetime — this helper doesn't free them, just
   parses CSV/TSV rows and dispatches workers. Returns 0 on success, 1 on
   error (after writing the appropriate JSON response). */
static int bulk_ins_delim_run(const char *db_root, const char *object,
                               const char *data, size_t size,
                               char delimiter, int if_not_exists) {
    /* Must have typed schema — delimited values map to fields.conf order */
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"Delimited import requires typed fields (fields.conf)\"}\n");
        return 1;
    }
    if (!data || size == 0) {
        OUT("{\"error\":\"Empty input\"}\n");
        return 1;
    }
    Schema sc = load_schema(db_root, object);
    return typed_bulk_insert_command(db_root, object, &sc, ts, NULL, data, size,
                                     1, delimiter, if_not_exists);
}

int cmd_bulk_insert_delimited(const char *db_root, const char *object,
                               const char *filepath, char delimiter,
                               int if_not_exists) {
    if (!filepath) { OUT("{\"error\":\"file is required\"}\n"); return 1; }
    Schema schema = load_schema(db_root, object);
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"Delimited import requires typed fields (fields.conf)\"}\n");
        return 1;
    }
    return typed_bulk_insert_source(db_root, object, &schema, ts, filepath, 1,
                                    delimiter, if_not_exists);
}

/* In-memory variant — wire dispatch uses this for inline `data` so the
   request body doesn't round-trip through /tmp. Caller (wire dispatch)
   keeps ownership of the buffer and frees after this returns. */
int cmd_bulk_insert_delimited_string(const char *db_root, const char *object,
                                       const char *data, size_t size,
                                       char delimiter, int if_not_exists) {
    return bulk_ins_delim_run(db_root, object, data, size, delimiter, if_not_exists);
}

/* ========== BULK DELETE ========== */

/* Per-shard bulk delete worker */
typedef struct {
    const char *db_root;
    const char *object;
    Schema *sch;
    /* Keys for this shard */
    char **keys;
    uint8_t (*hashes)[16];
    int *shard_slots;
    int key_count;
    /* Index info */
    char (*idx_fields)[256];
    const enum IndexType *idx_types;  /* [nidx] — NULL = all btree */
    int nidx;
    TypedSchema *ts;
    /* Results */
    int deleted;
    int durability_degraded;
    /* Collected index deletions: [nidx][key_count] — parallel (val, vlen).
       val is malloc'd index-key bytes (or NULL). */
    uint8_t ***idx_vals;
    size_t  **idx_lens;
    /* Deferred-request input + worker-owned scratch, alive until
       bulk_del_finish_request. */
    SlotcaskBulkShardInput input;
    int             prepared;
    SlotcaskBulkRec *batch;
} BulkDelShardWork;

/* === v2 bulk-delete (key-list) worker ===
 *
 * Indexed deletes use the two-phase window callbacks below. Non-indexed
 * deletes need no callback: the slotcask primitive tombstones the captured
 * Kf entries directly. */
/* Builds a fully-populated UpdateIdxArg for one field's forward index diff
   (old_key -> new_key), including kf_shard/kf_slot/bm_max_values/sync_after
   — required whenever type==IT_BITMAP (index.c's update_idx_fn dispatches
   IT_BITMAP straight into bitmap_update using these raw). Callers either
   dispatch immediately via update_idx_fn() or batch several into a
   parallel_for() array; either way out_error must be checked afterward.
   sync_after is 0: during a bulk window, update_idx_fn records an
   idx_touch_record instead of syncing, and the window's flush seam does
   one fdatasync per unique (field, idx shard) before marker clear. */
static UpdateIdxArg make_index_diff_arg(const char *db_root, const char *object,
                                         const char *field, int splits,
                                         enum IndexType itype,
                                         uint8_t *new_key, size_t new_len,
                                         uint8_t *old_key, size_t old_len,
                                         const uint8_t *hash,
                                         int kf_shard, uint32_t kf_slot) {
    UpdateIdxArg a = {0};
    a.db_root = db_root; a.object = object;
    a.field = field; a.splits = splits;
    a.new_key = new_key; a.new_len = new_len;
    a.old_key = old_key; a.old_len = old_len;
    a.hash = hash; a.type = itype;
    a.kf_shard = kf_shard; a.kf_slot = kf_slot;
    a.bm_max_values = 0;  /* default cap — header wins on existing */
    /* Bulk windows record touched (field, shard) files instead of syncing
       per mutation; bulk_apply_and_sync_indexes_locked flushes each unique
       file once before the marker is cleared (invariant I1). All five call
       sites are bulk apply_window callbacks (key-list delete, structured /
       delimited / JSON update, criteria delete) — no single-record caller. */
    a.sync_after = 0;
    return a;
}

/* Batch apply_window for indexed bulk deletes: fires once per window after
   the batch delete marker is durable, performs the forward index diff
   (old=OLD, new=NULL) for every active record. BulkDelShardWork* is passed
   via ctx. Returns 0 on success, non-zero on I/O/OOM (triggers batch
   abort sidecar + inverse in the primitive). */
static int v2_bulk_del_apply_window(SlotcaskBulkRec *recs,
                                     const size_t *active, size_t nactive,
                                     void *ctx, void *window_state) {
    (void)window_state;
    BulkDelShardWork *sw = (BulkDelShardWork *)ctx;
    if (!sw || sw->nidx == 0 || !sw->ts) return 0;

    int idx_failed = 0;
    for (size_t a = 0; a < nactive; a++) {
        size_t j = active[a];
        SlotcaskBulkRec *r = &recs[j];
        if (r->status != 0) continue;
        if (!r->old_value) continue;

        uint8_t hash[16];
        compute_hash_raw(r->key, r->klen, hash);

        for (int fi = 0; fi < sw->nidx; fi++) {
            enum IndexType itype = sw->idx_types ? sw->idx_types[fi] : IT_BTREE;
            /* tg/bm consumers index the RAW payload (no tier byte) —
               see v2_bulk_upd_delim_pre_commit_bulk. */
            int raw_payload = (itype == IT_BITMAP || itype == IT_TRIGRAM);
            uint8_t *buf = NULL; size_t blen = 0;
            int have = raw_payload
                ? build_index_payload_from_record(sw->ts, r->old_value, r->old_vlen, r->old_hdr2,
                                                  sw->idx_fields[fi], &buf, &blen)
                : typed_index_old_record_key(sw->ts, r->old_value, r->old_vlen, r->old_hdr2,
                                              sw->idx_fields[fi],
                                              &buf, &blen);
            if(have<0){idx_failed=1;free(buf);continue;}
            if (!have)
                continue;
            UpdateIdxArg a2 = make_index_diff_arg(sw->db_root, sw->object,
                                                   sw->idx_fields[fi], sw->sch->splits,
                                                   itype, NULL, 0, buf, blen, hash,
                                                   r->kf_shard, r->kf_slot);
            update_idx_fn(&a2);
            if (a2.out_error) idx_failed = 1;
            free(buf);
        }
    }
    return idx_failed ? -1 : 0;
}

static void *bulk_del_shard_worker_v2(BulkDelShardWork *sw) {
    SlotcaskSchemaInfo info = {
        .splits = sw->sch->splits, .slot_size = sw->sch->slot_size,
        .streams = sw->sch->streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "bulk_del_shard_worker_v2: slotcask_registry_get failed for %s/%s", sw->db_root, sw->object);
        return NULL;
    }

    /* All keys in this worker hash to the same kf shard (dispatcher
       aligned shard_id with compute_record_shard, see cmd_bulk_delete). */
    int kf_shard_id = compute_record_shard(sw->hashes[0], sw->sch->splits);

    SlotcaskBulkRec *batch = calloc(sw->key_count, sizeof(SlotcaskBulkRec));
    if (!batch) {
        LOG_ERROR(LOG_SUB_QUERY, "bulk_del_shard_worker_v2: alloc failed, dropping %d deletes", sw->key_count);
        free(batch); return NULL;
    }

    for (int ki = 0; ki < sw->key_count; ki++) {
        batch[ki].key       = sw->keys[ki];
        batch[ki].klen      = strlen(sw->keys[ki]);
        batch[ki].value     = NULL;
        batch[ki].vlen      = 0;
        batch[ki].user_ctx  = NULL;
        batch[ki].old_value = NULL;
        batch[ki].old_vlen  = 0;
        batch[ki].status    = 0;
        batch[ki].was_update = 0;
    }

    SlotcaskBulkDeleteOpts opts = {
        .has_indexed_fields = (sw->nidx > 0),
    };
    sw->durability_degraded = 0;
    opts.out_durability_degraded = &sw->durability_degraded;
    if (sw->nidx > 0) {
        opts.apply_window = v2_bulk_del_apply_window;
        opts.bulk_hook_ctx = sw;
    }
    /* Deferred-request preparation: no mutation here. */
    sw->batch = batch;
    memset(&sw->input, 0, sizeof(sw->input));
    sw->input.kf_shard_id = kf_shard_id;
    sw->input.recs = batch;
    sw->input.nrecs = (size_t)sw->key_count;
    sw->input.kind = SLOTCASK_BULK_INPUT_DELETE;
    sw->input.opts.delete_ = opts;
    sw->prepared = 1;
    return NULL;
}

/* Command-thread tail for the key-list delete workers. */
static void bulk_del_finish_request(BulkDelShardWork *sw) {
    if (!sw->prepared) return;
    SlotcaskBulkRec *batch = sw->batch;
    if (sw->input.rc != 0) {
        for (int ki = 0; ki < sw->key_count; ki++)
            if (batch[ki].status == 0) batch[ki].status = -1;
    }
    for (int ki = 0; ki < sw->key_count; ki++) {
        if (batch[ki].status == 0) sw->deleted++;
        /* status=-2 (not found) and status=-1 (error) are not counted. */
    }
    free(sw->batch);
    sw->batch = NULL;
}

static void *bulk_del_shard_worker(void *arg) {
    BulkDelShardWork *sw = (BulkDelShardWork *)arg;
    if (sw->key_count == 0) return NULL;
    return bulk_del_shard_worker_v2(sw);
}

/* Internal: takes a malloc'd buffer of keys (JSON array OR newline-separated)
   and runs the parse + parallel tombstone. Always frees `raw` before
   returning. Both cmd_bulk_delete (file/stdin path) and
   cmd_bulk_delete_string (in-memory path from wire dispatch) call this so
   the inline path doesn't need a /tmp round-trip. */
static int bulk_delete_run(const char *db_root, const char *object,
                            char *raw, size_t len) {
    if (!raw) { fprintf(stderr, "Error: Cannot read input\n"); return 1; }
    (void)len;

    /* Parse all keys */
    char **keys = NULL;
    int key_count = 0, key_cap = 1024;
    keys = malloc(key_cap * sizeof(char *));

    const char *p = json_skip(raw);
    if (*p == '[') {
        p++;
        while (*p) {
            p = json_skip(p);
            if (*p == ']') break;
            if (*p == ',') { p++; continue; }
            if (*p == '"') {
                p++;
                const char *start = p;
                while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
                size_t klen = p - start;
                if (key_count >= key_cap) {
                    key_cap *= 2;
                    /* Plain realloc + walk: keys[] holds heap-malloc'd entries. */
                    char **t = realloc(keys, key_cap * sizeof(char *));
                    if (!t) {
                        for (int k = 0; k < key_count; k++) free(keys[k]);
                        free(keys);
                        keys = NULL;
                        key_count = 0;
                        break;
                    }
                    keys = t;
                }
                keys[key_count] = malloc(klen + 1);
                memcpy(keys[key_count], start, klen);
                keys[key_count][klen] = '\0';
                key_count++;
                if (*p == '"') p++;
            } else p++;
        }
    } else {
        char *_line_save = NULL; char *line = strtok_r(raw, "\n", &_line_save);
        while (line) {
            if (line[0] != '\0') {
                if (key_count >= key_cap) {
                    key_cap *= 2;
                    /* Plain realloc + walk: keys[] holds heap-malloc'd entries. */
                    char **t = realloc(keys, key_cap * sizeof(char *));
                    if (!t) {
                        for (int k = 0; k < key_count; k++) free(keys[k]);
                        free(keys);
                        keys = NULL;
                        key_count = 0;
                        break;
                    }
                    keys = t;
                }
                keys[key_count++] = strdup(line);
            }
            line = strtok_r(NULL, "\n", &_line_save);
        }
    }

    if (key_count == 0) { free(keys); free(raw); return 0; }

    Schema sch = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */
    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);
    TypedSchema *ts = load_typed_schema(db_root, object);

    /* Compute hashes + shard_ids; bucket-sort into per-shard worker arrays.
       Was insertion sort O(n²) — at 10K keys with random shard distribution
       that's ~50M swaps before parallel_for even starts. Bucket-sort is
       O(n + splits), matches the bulk-insert / bulk-update pattern. */
    uint8_t (*hashes)[16] = malloc(key_count * sizeof(uint8_t[16]));
    int *shard_ids = malloc(key_count * sizeof(int));
    int *start_slots = malloc(key_count * sizeof(int));

    for (int i = 0; i < key_count; i++) {
        compute_hash_raw(keys[i], strlen(keys[i]), hashes[i]);
        shard_ids[i] = compute_record_shard(hashes[i], sch.splits);
        start_slots[i] = 0;
    }

    int *shard_counts = calloc(sch.splits, sizeof(int));
    for (int i = 0; i < key_count; i++) shard_counts[shard_ids[i]]++;
    int *worker_shards = NULL, *shard_to_worker = NULL;
    int nshard_groups = build_shard_worker_map(shard_counts, sch.splits,
                                                &worker_shards, &shard_to_worker);

    BulkDelShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkDelShardWork)) : NULL;
    for (int g = 0; g < nshard_groups; g++) {
        int s = worker_shards[g];
        int cnt = shard_counts[s];
        workers[g].db_root = db_root;
        workers[g].object = object;
        workers[g].sch = &sch;
        workers[g].keys = malloc(cnt * sizeof(char *));
        workers[g].hashes = malloc(cnt * sizeof(uint8_t[16]));
        workers[g].shard_slots = malloc(cnt * sizeof(int));
        workers[g].key_count = 0;
        workers[g].idx_fields = idx_fields;
        workers[g].idx_types = idx_types;
        workers[g].nidx = nidx;
        workers[g].ts = ts;
        workers[g].deleted = 0;
        workers[g].idx_vals = calloc(nidx, sizeof(uint8_t **));
        workers[g].idx_lens = calloc(nidx, sizeof(size_t *));
        for (int fi = 0; fi < nidx; fi++) {
            workers[g].idx_vals[fi] = calloc(cnt, sizeof(uint8_t *));
            workers[g].idx_lens[fi] = calloc(cnt, sizeof(size_t));
        }
    }
    /* Single pass — place each record into its bucket's next slot. */
    for (int i = 0; i < key_count; i++) {
        int w = shard_to_worker[shard_ids[i]];
        int slot = workers[w].key_count++;
        workers[w].keys[slot] = keys[i];
        memcpy(workers[w].hashes[slot], hashes[i], 16);
        workers[w].shard_slots[slot] = start_slots[i];
    }
    free(shard_counts); free(worker_shards); free(shard_to_worker);

    /* Phase 1: Parallel shard tombstoning */
    parallel_for_io(bulk_del_shard_worker, workers, nshard_groups, sizeof(BulkDelShardWork));

    /* Request execution: one deferred request over every worker's prepared
       input (ascending shard order). Exactly one execute per command. */
    {
        size_t np = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) np++;
        SlotcaskBulkShardInput *inputs = np ? calloc(np, sizeof(*inputs)) : NULL;
        size_t k = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) inputs[k++] = workers[g].input;
        if (np && !inputs) {
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = -1;
        } else if (np) {
            SlotcaskSchemaInfo info = {
                .splits = sch.splits, .slot_size = sch.slot_size,
                .streams = sch.streams,
            };
            SlotcaskDb *xsdb SDB_REG_REF =
                slotcask_registry_get(db_root, object, &info);
            uint64_t _commit_t0 = now_us();
            int xrc = xsdb ? bulk_execute_prepared(xsdb, inputs, np, 0) : -1;
            commit_lock_hold_record(_commit_t0, db_root, object);
            (void)xrc;
            k = 0;
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = inputs[k++].rc;
        }
        free(inputs);
        for (int g = 0; g < nshard_groups; g++)
            bulk_del_finish_request(&workers[g]);
    }

    /* Phase 2: Parallel index cleanup — one thread per index. Skipped for
       v2: the per-record pre_commit hook in bulk_del_shard_worker_v2 already
       drops the btree entries under the kf-shard wrlock. */
    int total_deleted = 0;
    int any_durability_degraded = 0;
    for (int g = 0; g < nshard_groups; g++) {
        total_deleted += workers[g].deleted;
        any_durability_degraded |= workers[g].durability_degraded;
    }

    /* v2 drops btree index entries inside the per-record pre_commit hook
       in bulk_del_shard_worker_v2 (under the kf-shard wrlock), so no
       post-pass index cleanup is needed here. */

    /* Cleanup workers */
    for (int g = 0; g < nshard_groups; g++) {
        free(workers[g].keys);
        free(workers[g].hashes);
        free(workers[g].shard_slots);
        for (int fi = 0; fi < nidx; fi++) {
            free(workers[g].idx_vals[fi]);
            free(workers[g].idx_lens[fi]);
        }
        free(workers[g].idx_vals);
        free(workers[g].idx_lens);
    }
    free(workers);

    if (total_deleted > 0) {
        update_count(db_root, object, -total_deleted);
        update_deleted_count(db_root, object, total_deleted);
    }

    if (any_durability_degraded)
        OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");
    else
        OUT("{\"deleted\":%d}\n", total_deleted);
    for (int i = 0; i < key_count; i++) free(keys[i]);
    free(keys); free(hashes); free(shard_ids); free(start_slots); free(raw);
    return any_durability_degraded ? 1 : 0;
}

int cmd_bulk_delete(const char *db_root, const char *object, const char *input) {
    size_t len = 0;
    char *raw = NULL;
    if (input) {
        raw = read_file(input, &len);
    } else {
        size_t cap = 65536, pos = 0;
        raw = malloc(cap);
        if (raw) {
            int c;
            while ((c = fgetc(stdin)) != EOF) {
                if (pos >= cap - 1) {
                    cap *= 2;
                    char *t = xrealloc_or_free(raw, cap);
                    if (!t) { raw = NULL; break; }
                    raw = t;
                }
                raw[pos++] = c;
            }
            if (raw) { raw[pos] = '\0'; len = pos; }
        }
    }
    return bulk_delete_run(db_root, object, raw, len);
}

/* In-memory variant — wire dispatch uses this for inline `keys` to avoid
   the /tmp round-trip cmd_bulk_delete used to do. Mirrors
   cmd_bulk_insert_string. Takes ownership of `keys_str` (frees inside
   bulk_delete_run via free(raw)). */
int cmd_bulk_delete_string(const char *db_root, const char *object,
                            char *keys_str) {
    size_t len = keys_str ? strlen(keys_str) : 0;
    return bulk_delete_run(db_root, object, keys_str, len);
}

/* ========== BULK-UPDATE / BULK-DELETE WITH CRITERIA ========== */

static int bulk_criteria_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx) {
    BulkCriteriaCtx *bc = (BulkCriteriaCtx *)ctx;
    /* coverity[lock_evasion] coverity[missing_lock] intentional fast-skip in
       the per-record hot path — `_Atomic int` gives torn-read-free visibility
       against the writers under bc->lock; staleness here just costs one
       extra iteration before the locked re-check at line 2232 catches the
       budget overflow. Taking the mutex per record would gate every callback
       through a kernel call. */
    if (bc->budget_exceeded) return 1;
    /* coverity[lock_evasion] coverity[missing_lock] same rationale — count is _Atomic. */
    if (bc->limit > 0 && bc->count >= bc->limit) return 1;
    if (query_deadline_tick(bc->deadline, &bc->dl_counter)) return 1;

    const uint8_t *raw = block + hdr->key_len;

    int matched=0;
    if(criteria_match_tree_checked(raw,hdr->value_len,bc->tree,bc->fs,(uint8_t)(hdr->flag>>8),&matched)){
        int error=errno?errno:EILSEQ;pthread_mutex_lock(&bc->lock);bc->predicate_error=error;pthread_mutex_unlock(&bc->lock);return 1;}
    if(!matched)return 0;

    /* Match — grow array and append under internal mutex. criteria_match_tree
       above runs lock-free; only the shared-state mutation is serialized. */
    size_t key_bytes = sizeof(char *) + hdr->key_len + 1;
    char *key = malloc(hdr->key_len + 1);
    memcpy(key, block, hdr->key_len);
    key[hdr->key_len] = '\0';

    pthread_mutex_lock(&bc->lock);
    if (bc->budget_exceeded || (bc->limit > 0 && bc->count >= bc->limit)) {
        pthread_mutex_unlock(&bc->lock);
        free(key);
        return 1;
    }
    if (bc->buffer_bytes + key_bytes > g_query_buffer_max_bytes) {
        bc->budget_exceeded = 1;
        pthread_mutex_unlock(&bc->lock);
        free(key);
        return 1;
    }
    bc->buffer_bytes += key_bytes;
    if (bc->count >= bc->cap) {
        int new_cap = bc->cap ? bc->cap * 2 : 1024;
        char **t = xrealloc_or_free(bc->keys, (size_t)new_cap * sizeof(char *));
        if (!t) {
            bc->keys = NULL;
            bc->count = 0;
            bc->cap = 0;
            bc->budget_exceeded = 1;
            pthread_mutex_unlock(&bc->lock);
            free(key);
            return 1;
        }
        bc->keys = t;
        bc->cap = new_cap;
    }
    bc->keys[bc->count++] = key;
    pthread_mutex_unlock(&bc->lock);
    return 0;
}


int cmd_bulk_update(const char *db_root, const char *object,
                    const char *criteria_json, const char *value_json,
                    const char *if_json, int limit, int dry_run) {
    (void)criteria_json; (void)value_json; (void)if_json;
    (void)limit; (void)dry_run;
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        /* Same text the bulk-update dispatch in server.c emits for an object
           whose fields.conf is missing. */
        OUT("{\"error\":\"Object [%s] not found. Use create-object first.\"}\n", object);
        return 1;
    }
    /* Packed criteria updates are a pre-existing product gap (AUD-PACKED-
       BULKUPDATE), pinned by test-packed-bulk-update-criteria-refused. */
    OUT("{\"error\":\"packed criteria updates await query integration\"}\n");
    return 1;
}

/* Shared body for bulk-update-delimited (file + inline string entry points).
   Caller owns `data`/`size` lifetime — this helper doesn't free them, just
   parses and dispatches workers. Returns 0 on success / 1 on validation
   error after writing the appropriate JSON response. */
static int bulk_upd_delim_run(const char *db_root, const char *object,
                               const char *data, size_t size,
                               char delimiter) {
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"Delimited update requires typed fields (fields.conf)\"}\n");
        return 1;
    }
    if (!data || size == 0) {
        OUT("{\"error\":\"Empty input\"}\n");
        return 1;
    }
    Schema sc = load_schema(db_root, object);
    return typed_bulk_update_command(db_root, object, &sc, ts, NULL, data, size,
                                     1, delimiter);
}

int cmd_bulk_update_delimited(const char *db_root, const char *object,
                               const char *filepath, char delimiter) {
    if (!filepath) { OUT("{\"error\":\"file is required\"}\n"); return 1; }

    int ifd = open(filepath, O_RDONLY);
    if (ifd < 0) { OUT("{\"error\":\"Cannot open file\"}\n"); return 1; }
    struct stat st;
    if (fstat(ifd, &st) < 0) { close(ifd); OUT("{\"error\":\"Cannot stat file\"}\n"); return 1; }
    if (st.st_size == 0) { close(ifd); OUT("{\"error\":\"Empty file\"}\n"); return 1; }
    const char *data = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, ifd, 0);
    int data_mmaped = 1;
    if (data == MAP_FAILED) {
        char *buf = malloc(st.st_size);
        if (!buf) { close(ifd); return 1; }
        lseek(ifd, 0, SEEK_SET);
        size_t rd = 0;
        while (rd < (size_t)st.st_size) {
            ssize_t n = read(ifd, buf + rd, st.st_size - rd);
            if (n <= 0) break;
            rd += n;
        }
        data = buf;
        data_mmaped = 0;
    } else {
        madvise((void *)data, st.st_size, MADV_SEQUENTIAL);
    }
    close(ifd);

    int rc = bulk_upd_delim_run(db_root, object, data, (size_t)st.st_size, delimiter);

    if (data_mmaped) munmap((void *)data, st.st_size);
    else free((void *)data);
    return rc;
}

/* In-memory variant — wire dispatch uses this for inline `data` so the
   request body doesn't round-trip through /tmp. Caller (wire dispatch)
   keeps ownership of `data` and frees it after this returns. */
int cmd_bulk_update_delimited_string(const char *db_root, const char *object,
                                       const char *data, size_t size,
                                       char delimiter) {
    return bulk_upd_delim_run(db_root, object, data, size, delimiter);
}

/* ===== bulk-update JSON form =====
   Shape: [{"key":"k","value":{...}}, ...]
   Semantics: update-only, key must exist; only fields present in `data`
   are overwritten, fields absent from `data` keep their existing value.
   Same shard-grouped parallel pattern as bulk-update-delimited; the worker
   patches each touched field in place at its known offset and applies the
   drop-old/insert-new index delta only where the indexed value changed. */





static int bulk_upd_json_run(const char *db_root, const char *object,
                              const char *input, int input_is_file) {
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"bulk-update-json requires typed fields (fields.conf)\"}\n");
        return 1;
    }
    Schema sc = load_schema(db_root, object);
    return typed_bulk_update_source(db_root, object, &sc, ts, NULL, input,
                                    input_is_file);
}

int cmd_bulk_update_json(const char *db_root, const char *object, const char *input) {
    return bulk_upd_json_run(db_root, object, input, 1);
}

int cmd_bulk_update_json_string(const char *db_root, const char *object, char *json_str) {
    return bulk_upd_json_run(db_root, object, json_str, 0);
}

/* === v2 bulk-delete-criteria parallel-shard infrastructure ===
 * The criteria scan in Phase 1 produces a flat list of matched keys.
 * Phase 2 buckets them by their kf shard (slotcask byte order) and fans
 * out one worker per bucket. Each worker calls
 * slotcask_bulk_delete_in_kfshard with the bucket's records under one
 * kf wrlock. The pre_commit hook fires per record under the wrlock and
 * folds CAS re-verification + index drop into a single callback —
 * non-zero return skips the record (CAS rejection) so the kf entry
 * stays live. */

typedef struct {
    SlotcaskDb     *sdb;
    const char     *db_root;
    const char     *object;
    const Schema   *sch;
    TypedSchema    *ts;
    CriteriaNode   *tree;
    FieldSchema    *fs;
    CriteriaNode *cas_tree;
    FieldSchema *cas_fs;
    char          (*idx_fields)[256];
    const enum IndexType *idx_types;  /* [nidx] — NULL = all btree */
    int             nidx;
    /* per-shard records */
    char          **keys;
    uint8_t       (*hashes)[16];
    int             count;
    /* result */
    int             deleted;
    int             skipped;
    int             errors;
    int             durability_degraded;
    /* Deferred-request input + worker-owned scratch. */
    SlotcaskBulkShardInput input;
    int             prepared;
    SlotcaskBulkRec *batch;
    void            *ctxs;
} BulkDelCritShardWork;

typedef struct {
    BulkDelCritShardWork *w;
    int                    ki;
    int                    error;
} V2BulkDelCritCtx;

/* Legacy per-record hook for criteria-based bulk delete (no indexes).
   Only does criteria + CAS check; index drops are handled by the
   windowed apply_window path when nidx > 0. */
static int bulk_del_crit_check(V2BulkDelCritCtx *ctx,const uint8_t *value,
    size_t len,uint8_t flags){
    BulkDelCritShardWork *w=ctx->w;int matched=0;
    if(criteria_match_tree_checked(value,len,w->tree,w->fs,flags,&matched)){
        ctx->error=errno?errno:EILSEQ;return -1;
    }
    if(!matched)return -1;
    if(w->cas_tree){
        int result=cas_match_tree_checked(value,len,w->cas_tree,w->cas_fs,flags);
        if(result<0)ctx->error=errno?errno:EILSEQ;
        if(result<=0)return -1;
    }
    return 0;
}

static int v2_bulk_del_crit_pre_commit_bulk(const SlotcaskOldRecord *old,
    SlotcaskBulkRec *rec){
    V2BulkDelCritCtx *ctx=rec->user_ctx;
    if(!old)return -1;
    return bulk_del_crit_check(ctx,old->value,old->vlen,old->hdr2);
}

/* Batch prepare_window for criteria-based indexed bulk deletes: fires once
   per window BEFORE the batch marker, rejects records that fail criteria
   match or CAS check. BulkDelCritShardWork* is passed via ctx. Returns 0
   (some records survived) or -1 (all rejected — the primitive will skip
   the marker for this window). */
static int v2_bulk_del_crit_prepare_window(SlotcaskBulkRec *recs,
    const size_t *active,size_t nactive,void *ctx,void **out_window_state){
    (void)ctx;*out_window_state=NULL;
    for(size_t a=0;a<nactive;a++){
        SlotcaskBulkRec *r=&recs[active[a]];
        if(r->status!=0)continue;
        if(!r->old_value){r->status=-1;continue;}
        if(bulk_del_crit_check(r->user_ctx,r->old_value,r->old_vlen,r->old_hdr2))
            r->status=-1;
    }
    return 0;
}

/* Batch apply_window for criteria-based indexed bulk deletes: fires once
   per window AFTER the batch marker, performs the forward index diff
   (old=OLD, new=NULL) for every active record. BulkDelCritShardWork* is
   passed via ctx. Returns 0 on success, non-zero on I/O/OOM (triggers
   batch abort sidecar + inverse in the primitive). */
static int v2_bulk_del_crit_apply_window(SlotcaskBulkRec *recs,
                                          const size_t *active, size_t nactive,
                                          void *ctx, void *window_state) {
    (void)window_state;
    BulkDelCritShardWork *w = (BulkDelCritShardWork *)ctx;
    int idx_failed = 0;
    for (size_t a = 0; a < nactive; a++) {
        size_t j = active[a];
        SlotcaskBulkRec *r = &recs[j];
        if (r->status != 0) continue;
        if (!r->old_value) continue;

        uint8_t hash[16];
        compute_hash_raw(r->key, r->klen, hash);

        for (int fi = 0; fi < w->nidx; fi++) {
            enum IndexType itype = w->idx_types ? w->idx_types[fi] : IT_BTREE;
            /* tg/bm consumers index the RAW payload (no tier byte) —
               see v2_bulk_upd_delim_pre_commit_bulk. */
            int raw_payload = (itype == IT_BITMAP || itype == IT_TRIGRAM);
            uint8_t *buf = NULL; size_t blen = 0;
            int have = raw_payload
                ? build_index_payload_from_record(w->ts, r->old_value, r->old_vlen, r->old_hdr2,
                                                  w->idx_fields[fi], &buf, &blen)
                : typed_index_old_record_key(w->ts, r->old_value, r->old_vlen, r->old_hdr2,
                                              w->idx_fields[fi], &buf, &blen);
            if(have<0){idx_failed=1;free(buf);continue;}
            if (!have)
                continue;
            UpdateIdxArg a2 = make_index_diff_arg(w->db_root, w->object,
                                                   w->idx_fields[fi], w->sch->splits,
                                                   itype, NULL, 0, buf, blen, hash,
                                                   r->kf_shard, r->kf_slot);
            update_idx_fn(&a2);
            if (a2.out_error) idx_failed = 1;
            free(buf);
        }
    }
    return idx_failed ? -1 : 0;
}

static void *bulk_del_crit_shard_worker(void *arg) {
    BulkDelCritShardWork *w = (BulkDelCritShardWork *)arg;
    if (w->count == 0) return NULL;

    /* All keys in this worker hash to the same kf shard (dispatcher
       pre-sorted by compute_record_shard). */
    int kf_shard_id = compute_record_shard(w->hashes[0], w->sch->splits);

    SlotcaskBulkRec  *batch = calloc((size_t)w->count, sizeof(SlotcaskBulkRec));
    V2BulkDelCritCtx *ctxs = calloc((size_t)w->count,sizeof(*ctxs));
    if (!batch || !ctxs) {
        LOG_ERROR(LOG_SUB_QUERY, "bulk_del_crit_shard_worker: alloc failed, skipping %d deletes", w->count);
        free(batch); free(ctxs);
        w->skipped = w->count;
        return NULL;
    }
    for (int i = 0; i < w->count; i++) {
        ctxs[i].w  = w;
        ctxs[i].ki = i;
        batch[i].key       = w->keys[i];
        batch[i].klen      = strlen(w->keys[i]);
        batch[i].value     = NULL;
        batch[i].vlen      = 0;
        batch[i].user_ctx  = &ctxs[i];
        batch[i].old_value = NULL;
        batch[i].old_vlen  = 0;
        batch[i].status    = 0;
        batch[i].was_update = 0;
    }
    SlotcaskBulkDeleteOpts opts = {
        .has_indexed_fields = (w->nidx > 0),
    };
    if (w->nidx > 0) {
        opts.prepare_window = v2_bulk_del_crit_prepare_window;
        opts.apply_window   = v2_bulk_del_crit_apply_window;
        opts.bulk_hook_ctx  = w;
    } else {
        opts.pre_commit           = v2_bulk_del_crit_pre_commit_bulk;
        /* CAS re-verification needs OLD even if there are no indexes;
           force the batched read regardless of nidx. */
        opts.pre_commit_needs_old = 1;
    }
    /* Deferred-request preparation: no mutation here. */
    w->durability_degraded = 0;
    opts.out_durability_degraded = &w->durability_degraded;
    w->batch = batch; w->ctxs = ctxs;
    memset(&w->input, 0, sizeof(w->input));
    w->input.kf_shard_id = kf_shard_id;
    w->input.recs = batch;
    w->input.nrecs = (size_t)w->count;
    w->input.kind = SLOTCASK_BULK_INPUT_DELETE;
    w->input.opts.delete_ = opts;
    w->prepared = 1;
    return NULL;
}

/* Command-thread tail for the criteria-delete workers. */
static void bulk_del_crit_finish_request(BulkDelCritShardWork *w) {
    if (!w->prepared) return;
    SlotcaskBulkRec *batch = w->batch;
    if (w->input.rc != 0) {
        for (int i = 0; i < w->count; i++)
            if (batch[i].status == 0) batch[i].status = -1;
    }
    for (int i = 0; i < w->count; i++) {
        V2BulkDelCritCtx *contexts=w->ctxs;
        if(contexts[i].error)w->errors++;
        else if(batch[i].status==0)w->deleted++;
        else w->skipped++;
    }
    free(w->batch); free(w->ctxs);
    w->batch = NULL; w->ctxs = NULL;
}

int cmd_bulk_delete_criteria(const char *db_root, const char *object,
                             const char *criteria_json, const char *if_json,
                             int limit, int dry_run) {
    Schema sch = load_schema(db_root, object);
    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &perr);
    if (perr) {
        OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
        free_criteria_tree(tree);
        return 1;
    }
    if (!tree) {
        OUT("{\"error\":\"Missing criteria\"}\n");
        return 1;
    }

    /* Optional `if` for per-record CAS, re-verified under wrlock in phase 2. */
    FieldSchema cas_fs;
    init_field_schema(&cas_fs, db_root, object);
    char cas_err[256];
    CriteriaNode *cas_tree = NULL;
    if (if_json && if_json[0]) {
        cas_tree = cas_prepare_tree(if_json, cas_fs.ts, 1, cas_err, sizeof(cas_err));
        if (!cas_tree && cas_err[0]) {
            OUT("{\"error\":\"invalid if condition\"}\n");
            free_criteria_tree(tree);
            return 1;
        }
    }

    /* Phase 1: resolve matching keys — indexed path first, full scan fallback */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    if(compile_criteria_tree_checked(tree,fs.ts,NULL,0)){free_criteria_tree(cas_tree);free_criteria_tree(tree);OUT("{\"error\":\"invalid criteria value\"}\n");return -1;}
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    BulkCriteriaCtx ctx = { tree, &fs, NULL, 0, 0, limit, &dl, 0, 0, 0,
                            PTHREAD_MUTEX_INITIALIZER, 0 };
    if (!bulk_delete_phase1_indexed(db_root, object, &sch, &fs,
                                     tree, limit, &dl, &ctx)) {
        scan_dispatch(db_root, object, &sch, data_dir, bulk_criteria_scan_cb, &ctx);
    }
    pthread_mutex_destroy(&ctx.lock);
    int matched = ctx.count;
    if(ctx.predicate_error){
        int error=ctx.predicate_error;free_criteria_tree(cas_tree);
        for(int i=0;i<matched;i++){free(ctx.keys[i]);}
        free(ctx.keys);free_criteria_tree(tree);
        OUT("{\"error\":\"invalid typed record\"}\n");errno=error;return -1;
    }

    if (dl.timed_out) {
        LOG_WARN(LOG_SUB_QUERY, "bulk-delete: query deadline exceeded while matching criteria");
        OUT("{\"error\":\"query_timeout\"}\n");
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }
    if (ctx.budget_exceeded) {
        LOG_WARN(LOG_SUB_QUERY, "bulk-delete: query buffer cap exceeded while matching criteria");
        OUT(QUERY_BUFFER_ERR);
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }

    if (dry_run) {
        OUT("{\"matched\":%d,\"deleted\":0,\"skipped\":0,\"dry_run\":true}\n", matched);
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 0;
    }

    /* Phase 2: Write — for each key, acquire wrlock, re-verify, tombstone */
    TypedSchema *ts = load_typed_schema(db_root, object);
    int deleted = 0, skipped = 0, errors = 0;
    int any_durability_degraded = 0;

    /* v2 fast path: bucket matched keys by kf shard, then fan out one
       worker per bucket. Each worker calls slotcask_bulk_delete_in_kfshard
       once — same lock-amortisation pattern bulk-insert / bulk-update /
       bulk-delete (key-list) all use. The pre_commit hook re-verifies
       criteria + CAS under the kf wrlock (returning non-zero skips the
       record) and drops index entries for the deleted record. */
    SlotcaskSchemaInfo info = {
        .splits = sch.splits, .slot_size = sch.slot_size,
        .streams = sch.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';
    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    if (!sdb || matched == 0) {
        LOG_INFO(LOG_SUB_QUERY, "BULK-DELETE %s matched=%d deleted=0 skipped=%d (v2)",
                 object, matched, sdb ? 0 : matched);
        OUT("{\"matched\":%d,\"deleted\":0,\"skipped\":%d}\n",
            matched, sdb ? 0 : matched);
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 0;
    }

    /* Compute hash + shard-id per matched key; bucket-sort into per-shard
       workers. Same O(n + splits) pattern as cmd_bulk_insert and
       cmd_bulk_delete (replaces the old O(n²) insertion sort). */
    uint8_t (*hashes)[16] = malloc((size_t)matched * sizeof(uint8_t[16]));
    int      *shard_ids   = malloc((size_t)matched * sizeof(int));
    if (!hashes || !shard_ids) {
        free(hashes); free(shard_ids);
        OUT("{\"error\":\"oom: bulk_delete_criteria\"}\n");
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 1;
    }
    for (int i = 0; i < matched; i++) {
        compute_hash_raw(ctx.keys[i], strlen(ctx.keys[i]), hashes[i]);
        shard_ids[i] = compute_record_shard(hashes[i], sch.splits);
    }

    int *shard_counts = calloc(sch.splits, sizeof(int));
    if (!shard_counts) {
        free(hashes); free(shard_ids);
        OUT("{\"error\":\"oom: bulk_delete_criteria buckets\"}\n");
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 1;
    }
    for (int i = 0; i < matched; i++) shard_counts[shard_ids[i]]++;
    int *worker_shards = NULL, *shard_to_worker = NULL;
    int nshard_groups = build_shard_worker_map(shard_counts, sch.splits,
                                                &worker_shards, &shard_to_worker);

    BulkDelCritShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkDelCritShardWork)) : NULL;
    if (nshard_groups < 0 || (nshard_groups > 0 && !workers)) {
        free(workers); free(worker_shards); free(shard_to_worker);
        free(shard_counts); free(hashes); free(shard_ids);
        OUT("{\"error\":\"oom: bulk_delete_criteria workers\"}\n");
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 1;
    }
    for (int g = 0; g < nshard_groups; g++) {
        int s = worker_shards[g];
        int cnt = shard_counts[s];
        workers[g].sdb = sdb;
        workers[g].db_root = db_root;
        workers[g].object  = object;
        workers[g].sch     = &sch;
        workers[g].ts      = ts;
        workers[g].tree    = tree;
        workers[g].fs      = &fs;
        workers[g].cas_tree = cas_tree;
        workers[g].cas_fs = &cas_fs;
        workers[g].idx_fields = idx_fields;
        workers[g].idx_types  = idx_types;
        workers[g].nidx       = nidx;
        workers[g].keys   = malloc((size_t)cnt * sizeof(char *));
        workers[g].hashes = malloc((size_t)cnt * sizeof(uint8_t[16]));
        workers[g].count  = 0;
    }
    for (int i = 0; i < matched; i++) {
        int w = shard_to_worker[shard_ids[i]];
        int slot = workers[w].count++;
        workers[w].keys[slot] = ctx.keys[i];   /* shallow ref; freed once below */
        memcpy(workers[w].hashes[slot], hashes[i], 16);
    }

    parallel_for(bulk_del_crit_shard_worker, workers, nshard_groups,
                  sizeof(BulkDelCritShardWork));

    /* Request execution: one deferred request over every worker's prepared
       input (ascending shard order). Exactly one execute per command. */
    {
        size_t np = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) np++;
        SlotcaskBulkShardInput *inputs = np ? calloc(np, sizeof(*inputs)) : NULL;
        size_t k = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) inputs[k++] = workers[g].input;
        if (np && !inputs) {
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = -1;
        } else if (np) {
            uint64_t _commit_t0 = now_us();
            int xrc = bulk_execute_prepared(sdb, inputs, np, 0);
            commit_lock_hold_record(_commit_t0, db_root, object);
            (void)xrc;
            k = 0;
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = inputs[k++].rc;
        }
        free(inputs);
        for (int g = 0; g < nshard_groups; g++)
            bulk_del_crit_finish_request(&workers[g]);
    }

    for (int g = 0; g < nshard_groups; g++) {
        deleted += workers[g].deleted;
        skipped += workers[g].skipped;
        errors += workers[g].errors;
        any_durability_degraded |= workers[g].durability_degraded;
        free(workers[g].keys);
        free(workers[g].hashes);
    }
    free(workers); free(hashes); free(shard_ids);
    free(shard_counts); free(worker_shards); free(shard_to_worker);

    if (deleted > 0) {
        update_count(db_root, object, -deleted);
        update_deleted_count(db_root, object, deleted);
    }
    LOG_INFO(LOG_SUB_QUERY, "BULK-DELETE %s matched=%d deleted=%d skipped=%d (v2)",
             object, matched, deleted, skipped);
    if (any_durability_degraded)
        OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");
    else if(errors)
        OUT("{\"error\":\"typed condition evaluation failed\",\"matched\":%d,\"deleted\":%d,\"skipped\":%d,\"errors\":%d}\n",
            matched,deleted,skipped,errors);
    else
        OUT("{\"matched\":%d,\"deleted\":%d,\"skipped\":%d}\n",
            matched, deleted, skipped);

    free_criteria_tree(cas_tree);
    for (int i = 0; i < matched; i++) free(ctx.keys[i]);
    free(ctx.keys); free_criteria_tree(tree);
    return any_durability_degraded || errors ? 1 : 0;
}

/* ========== VACUUM ========== */

/* ========== rebuild_object ==========
   Full rewrite of an object's data files. Used by vacuum --compact,
   vacuum --splits, and add-field (task #6). Creates data.new/ with
   live records re-slotted according to the new schema/splits, then
   atomically swaps it in for data/.

   Same-splits, same-layout rebuild preserves hash16 → (shard, slot)
   mapping, so existing index files survive the swap intact.

   Caller must hold objlock_wrlock on the object. */

/* Update the splits field for one object's line in $g_db_root/schema.conf.
   Format: dir:object:splits:max_key. Serialised by locking
   schema.conf itself via flock since this file is shared by all objects. */
/* Update the splits and/or streams field on the schema.conf line for
   <dir>/<object>. Pass new_splits<=0 to keep existing splits;
   new_streams<=0 to keep existing streams. */
