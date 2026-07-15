#include "types.h"
#include "slotcask.h"
#include "bitmap.h"
#include "query_internal.h"
#include <dirent.h>
#include <math.h>
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>

/* ========== BULK INSERT ========== */

/* Bulk ops use ucache (unified shard cache in storage.c) */

/* ---- Shared worker types for parallel index builds ---- */
typedef struct { char ipath[PATH_MAX]; BtEntry *pairs; size_t pair_count; } IdxBuildArg;
static void *idx_build_worker(void *arg) __attribute__((unused));
static void *idx_build_worker(void *arg) {
    IdxBuildArg *ib = (IdxBuildArg *)arg;
    /* Merge-rebuild: sort new entries, merge with existing tree, rebuild from scratch.
       Much faster than btree_insert_batch for large datasets because it uses sequential
       I/O (leaf scan + bulk_build) instead of random B+ tree insertions. */
    btree_bulk_merge(ib->ipath, ib->pairs, ib->pair_count);
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
} IdxFieldArg;

static void *idx_build_field_worker(void *arg) {
    IdxFieldArg *fa = (IdxFieldArg *)arg;
    if (fa->new_count == 0) return NULL;
    int idx_n = index_splits_for(fa->splits);

    /* Bucket-sort new_entries by idx_shard. */
    size_t *counts  = calloc((size_t)idx_n, sizeof(size_t));
    size_t *offsets = calloc((size_t)idx_n, sizeof(size_t));
    BtEntry *parted = malloc(fa->new_count * sizeof(BtEntry));
    if (!counts || !offsets || !parted) {
        LOG_ERROR(LOG_SUB_QUERY, "idx_build_field_worker: alloc failed for field %s (new_count=%zu)", fa->field, fa->new_count);
        free(counts); free(offsets); free(parted);
        return NULL;
    }
    for (size_t i = 0; i < fa->new_count; i++)
        counts[idx_shard_for_hash(fa->new_entries[i].hash, fa->splits)]++;
    size_t acc = 0;
    for (int s = 0; s < idx_n; s++) { offsets[s] = acc; acc += counts[s]; }
    size_t *cursor = calloc((size_t)idx_n, sizeof(size_t));
    if (!cursor) {
        LOG_ERROR(LOG_SUB_QUERY, "idx_build_field_worker: calloc cursor failed (idx_n=%d)", idx_n);
        free(counts); free(offsets); free(parted); return NULL;
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
        btree_bulk_merge(path, parted + offsets[s], counts[s]);
    }

    free(parted);
    free(counts);
    free(offsets);
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

static BulkArena *arena_new(size_t cap) {
    BulkArena *a = malloc(sizeof(BulkArena));
    if (!a) {
        LOG_ERROR(LOG_SUB_QUERY, "arena_new: malloc BulkArena header failed");
        return NULL;
    }
    a->base = malloc(cap);
    if (!a->base) {
        LOG_ERROR(LOG_SUB_QUERY, "arena_new: malloc %zu bytes failed", cap);
        free(a); return NULL;
    }
    a->next = NULL;
    a->used = 0;
    a->cap = cap;
    return a;
}

static void *arena_alloc(BulkArena **head, size_t n) {
    n = (n + 7) & ~(size_t)7;  /* 8-byte align */
    BulkArena *a = *head;
    if (a->used + n > a->cap) {
        size_t new_cap = a->cap * 2;
        if (n > new_cap) new_cap = n;
        BulkArena *na = arena_new(new_cap);
        if (!na) return NULL;
        na->next = a;
        *head = na;
        a = na;
    }
    void *p = a->base + a->used;
    a->used += n;
    return p;
}

static char *arena_strndup(BulkArena **head, const char *src, size_t n) {
    char *p = arena_alloc(head, n + 1);
    if (!p) return NULL;
    memcpy(p, src, n);
    p[n] = '\0';
    return p;
}

static void arena_free(BulkArena *a) {
    while (a) {
        BulkArena *next = a->next;
        free(a->base);
        free(a);
        a = next;
    }
}

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

/* Per-shard bucket + worker arguments. Each bucket targets exactly one
   shard so the worker can take the ucache wrlock **once**, write every
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
    /* Per-field bitmap accumulators — mirror btree's idx_pairs[] but
       carry (slot, value) instead of (value, hash). Filled by
       v2_bulk_ins_pre_commit_bulk under the kf wrlock; flushed AFTER
       each per-shard slotcask_bulk_upsert call returns, holding the
       bm wrlock once per (shard, field) instead of once per record.
       Counts reset between shard iterations. */
    struct BmPair **bm_pairs;        /* [nidx] */
    size_t         *bm_pair_counts;  /* [nidx] */
    size_t         *bm_pair_caps;    /* [nidx] */
    /* Results (written by worker) */
    int             inserted;   /* new keys — updates do NOT increment */
    int             errors;
    /* CAS: when if_not_exists is set, an existing-key probe match is treated
       as a no-op and counted in `skipped` instead of overwriting. */
    int             if_not_exists;
    int             skipped;
    /* Phase-2 profiling: total worker wall time and time spent inside
       ucache_grow_shard. Aggregated post-join to show "of this much
       Phase 2 time, X ms was grow." Helps isolate rehash cost. */
    uint64_t        wall_ms;
    uint64_t        grow_ms;
    int             grow_count;
    /* Per-worker index entry collection; merged post-phase into caller's
       global arrays. Each BtEntry.value is an owned malloc'd string. */
    BtEntry       **idx_pairs;        /* [nidx] */
    size_t         *idx_pair_counts;  /* [nidx] */
    size_t         *idx_pair_caps;    /* [nidx] */
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
       to overwrite any client-supplied override (mirrors cmd_insert_v2's
       Task 3 fix in storage.c — same contract, same reasoning). */
static int v2_bulk_ins_ac_value_compute(const SlotcaskOldRecord *old,
                                         SlotcaskBulkRec *rec) {
    V2BulkInsCtx *ctx = (V2BulkInsCtx *)rec->user_ctx;
    const TypedSchema *ts = ctx->sw->ts;
    uint8_t *buf = (uint8_t *)rec->value;

    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed ||
            ts->fields[i].default_kind != DK_AUTO_CREATE) continue;
        size_t off = (size_t)ts->fields[i].offset;
        size_t w   = (size_t)ts->fields[i].size;
        if (old && old->value && old->vlen >= off + w) {
            memcpy(buf + off, old->value + off, w);
        } else if (!old) {
            char tbuf[24];
            auto_now_str(&ts->fields[i], tbuf, sizeof(tbuf));
            encode_field(&ts->fields[i], tbuf, buf + off);
        }
        /* existed but old record too short (field added post-hoc): leave
           whatever phase 1 already encoded, same as the single-insert
           fix's equivalent case. */
    }
    return 0;
}

/* (slot, value) pair queued for batched bitmap flush. Inline value
   buffer covers bool (1 B) + typical varchar enum values (≤32 B);
   longer encodings fall back to the per-record bm_open path. */
#define BM_PAIR_INLINE  64
typedef struct BmPair {
    uint32_t slot;
    uint16_t vlen;
    uint8_t  value[BM_PAIR_INLINE];
} BmPair;

/* Per-record pre_commit hook fired under the kf-shard wrlock by
   slotcask_bulk_upsert_in_kfshard. Reads the V2BulkInsCtx via
   rec->user_ctx and accumulates idx entries into sw->idx_pairs[fi]. */
static int v2_bulk_ins_pre_commit_bulk(const SlotcaskOldRecord *old,
                                        SlotcaskBulkRec *rec,
                                        int is_update) {
    V2BulkInsCtx *ctx = (V2BulkInsCtx *)rec->user_ctx;
    BulkInsShardWork *sw = ctx->sw;
    BulkInsRecord    *r  = ctx->rec;
    if (sw->nidx == 0) return 0;

    /* Old idx values (only on update + when caller has an old record).
       Bufs point either into the worker's shared arena (no per-field
       malloc) or to malloc'd memory when the arena overflowed for a
       particular field. old_idx_owned[fi] tracks ownership so cleanup
       only frees malloc'd entries. */
    uint8_t *old_idx_bufs[MAX_FIELDS];
    size_t   old_idx_lens[MAX_FIELDS];
    int      old_idx_have[MAX_FIELDS];
    int      old_idx_owned[MAX_FIELDS];
    /* Zero-init bufs + lens too — Coverity CIDs 1693836/1693840 flagged
       reads of old_idx_bufs[fi]/old_idx_lens[fi] inside an
       old_idx_have[fi]-guarded condition. The short-circuit means the
       read is functionally safe, but the analyzer can't track the
       array-element relationship. Cheap defense (~2 KB stack). */
    memset(old_idx_bufs, 0, sizeof(old_idx_bufs));
    memset(old_idx_lens, 0, sizeof(old_idx_lens));
    memset(old_idx_have, 0, sizeof(old_idx_have));
    memset(old_idx_owned, 0, sizeof(old_idx_owned));
    if (is_update && old) {
        for (int fi = 0; fi < sw->nidx; fi++) {
            if (ctx->old_arena) {
                uint8_t *slot = ctx->old_arena + (size_t)fi * ctx->old_arena_slot;
                int rc = build_index_key_from_record_into(
                    sw->ts, old->value, sw->idx_fields[fi],
                    slot, ctx->old_arena_slot, &old_idx_lens[fi]);
                if (rc == 1) {
                    old_idx_bufs[fi] = slot;
                    old_idx_have[fi] = 1;
                } else if (rc == -1) {
                    /* Oversized index key — fall back to malloc. */
                    old_idx_have[fi] = build_index_key_from_record(
                        sw->ts, old->value, sw->idx_fields[fi],
                        &old_idx_bufs[fi], &old_idx_lens[fi]);
                    old_idx_owned[fi] = old_idx_have[fi];
                }
            } else {
                old_idx_have[fi] = build_index_key_from_record(
                    sw->ts, old->value, sw->idx_fields[fi],
                    &old_idx_bufs[fi], &old_idx_lens[fi]);
                old_idx_owned[fi] = old_idx_have[fi];
            }
        }
    }

    const uint8_t *new_value = (const uint8_t *)rec->value;

    for (int fi = 0; fi < sw->nidx; fi++) {
        enum IndexType itype = sw->idx_types ? sw->idx_types[fi] : IT_BTREE;
        uint8_t *key_buf = NULL;
        size_t   key_len = 0;

        if (sw->idx_is_composite[fi]) {
            char cat[4096]; int cp = 0; int ok = 1;
            for (int si = 0; si < sw->idx_field_counts[fi]; si++) {
                int tidx = sw->idx_field_indices[fi][si];
                if (tidx < 0) { ok = 0; break; }
                size_t blen = 0;
                typed_field_to_index_key(sw->ts, new_value, tidx,
                                          (uint8_t *)cat + cp, &blen);
                if (blen == 0) { ok = 0; break; }
                if (cp + (int)blen < (int)sizeof(cat)) { cp += (int)blen; }
                else { ok = 0; break; }
            }
            if (ok && cp > 0) {
                key_buf = malloc((size_t)cp);
                memcpy(key_buf, cat, (size_t)cp);
                key_len = (size_t)cp;
            }
        } else {
            int tidx = sw->idx_field_indices[fi][0];
            if (tidx >= 0) {
                const TypedField *f = &sw->ts->fields[tidx];
                size_t cap = (size_t)(f->size > 8 ? f->size : 8);
                key_buf = malloc(cap);
                typed_field_to_index_key(sw->ts, new_value, tidx,
                                          key_buf, &key_len);
                if (key_len == 0) { free(key_buf); key_buf = NULL; }
            }
        }

        int have_new  = (key_buf != NULL && key_len > 0);
        int unchanged = old_idx_have[fi] && have_new &&
                        key_len == old_idx_lens[fi] &&
                        memcmp(key_buf, old_idx_bufs[fi], key_len) == 0;

        /* Trigram fields: dispatch via update_idx_fn so the IT_TRIGRAM
           branch in index.c extracts distinct trigrams and writes one
           .tg leaf entry per (trigram, hash). No accumulator like
           bitmap — each trigram is already its own btree_insert at the
           index.c layer, and per-shard bt_acquire amortises the file
           lock automatically. */
        if (itype == IT_TRIGRAM) {
            if (!unchanged && have_new) {
                UpdateIdxArg a = {0};
                a.db_root = sw->db_root; a.object = sw->object;
                a.field = sw->idx_fields[fi]; a.splits = sw->sch->splits;
                a.new_key = key_buf; a.new_len = key_len;
                a.old_key = old_idx_have[fi] ? old_idx_bufs[fi] : NULL;
                a.old_len = old_idx_have[fi] ? old_idx_lens[fi] : 0;
                a.hash = r->hash; a.type = IT_TRIGRAM;
                update_idx_fn(&a);
            }
            if (old_idx_have[fi] && old_idx_owned[fi]) free(old_idx_bufs[fi]);
            free(key_buf);
            continue;
        }

        /* Bitmap fields: enqueue (slot, value) into the per-field
           accumulator. The post-shard flush (bulk_insert_shard_worker_v2)
           opens the .bm shard with writer=1 once, applies all bm_set's,
           then releases — same lock-amortisation pattern btree uses
           via idx_pairs[] + btree_bulk_merge. */
        if (itype == IT_BITMAP) {
            if (!unchanged && have_new && key_len <= BM_PAIR_INLINE && sw->bm_pairs) {
                if (sw->bm_pair_counts[fi] >= sw->bm_pair_caps[fi]) {
                    size_t new_cap = sw->bm_pair_caps[fi]
                                     ? sw->bm_pair_caps[fi] * 2 : 256;
                    BmPair *t = xrealloc_or_free(sw->bm_pairs[fi],
                                                  new_cap * sizeof(BmPair));
                    if (!t) {
                        LOG_ERROR(LOG_SUB_INDEX, "INDEX_OOM shard=%d field=%s (dropped bitmap pair)",
                                sw->shard_id, sw->idx_fields[fi]);
                        sw->bm_pairs[fi] = NULL;
                        sw->bm_pair_counts[fi] = 0;
                        sw->bm_pair_caps[fi] = 0;
                    } else {
                        sw->bm_pairs[fi] = t;
                        sw->bm_pair_caps[fi] = new_cap;
                    }
                }
                if (sw->bm_pairs[fi]) {
                    BmPair *p = &sw->bm_pairs[fi][sw->bm_pair_counts[fi]++];
                    p->slot = rec->kf_slot;
                    p->vlen = (uint16_t)key_len;
                    memcpy(p->value, key_buf, key_len);
                }
            } else if (!unchanged && have_new) {
                /* Oversized value or no accumulator — fall back to the
                   per-record path. Same correctness, slower. */
                UpdateIdxArg a = {0};
                a.db_root = sw->db_root; a.object = sw->object;
                a.field = sw->idx_fields[fi]; a.splits = sw->sch->splits;
                a.new_key = key_buf; a.new_len = key_len;
                a.hash = r->hash; a.type = IT_BITMAP;
                a.kf_shard = rec->kf_shard; a.kf_slot = rec->kf_slot;
                update_idx_fn(&a);
            }
            if (old_idx_have[fi] && old_idx_owned[fi]) free(old_idx_bufs[fi]);
            free(key_buf);
            continue;
        }

        if (old_idx_have[fi] && !unchanged) {
            delete_index_entry(sw->db_root, sw->object, sw->idx_fields[fi],
                               sw->sch->splits,
                               old_idx_bufs[fi], old_idx_lens[fi], r->hash);
        }
        /* Arena-backed bufs are owned by the worker (freed after the
           full batch); only malloc'd fallback bufs are freed here. */
        if (old_idx_have[fi] && old_idx_owned[fi]) free(old_idx_bufs[fi]);

        if (unchanged) { free(key_buf); continue; }

        if (have_new) {
            if (sw->idx_pair_counts[fi] >= sw->idx_pair_caps[fi]) {
                size_t new_cap = sw->idx_pair_caps[fi] ? sw->idx_pair_caps[fi] * 2 : 64;
                BtEntry *t = xrealloc_or_free(sw->idx_pairs[fi], new_cap * sizeof(BtEntry));
                if (!t) {
                    LOG_ERROR(LOG_SUB_INDEX, "INDEX_OOM shard=%d field=%s (dropped index pair on realloc; rerun reindex)",
                            sw->shard_id, sw->idx_fields[fi]);
                    sw->idx_pairs[fi] = NULL;
                    sw->idx_pair_counts[fi] = 0;
                    sw->idx_pair_caps[fi] = 0;
                    free(key_buf);
                    continue;
                }
                sw->idx_pairs[fi] = t;
                sw->idx_pair_caps[fi] = new_cap;
            }
            BtEntry *bp = &sw->idx_pairs[fi][sw->idx_pair_counts[fi]++];
            bp->value = (const char *)key_buf;
            bp->vlen  = key_len;
            memcpy(bp->hash, r->hash, 16);
        } else {
            free(key_buf);
        }
    }
    return 0;
}

static void *bulk_insert_shard_worker_v2(BulkInsShardWork *sw) {
    uint64_t t_worker_start = now_ms_coarse();

    SlotcaskSchemaInfo info = {
        .splits = sw->sch->splits, .slot_size = sw->sch->slot_size,
        .streams = sw->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "INSERT_DROP shard=%d (cannot open slotcask, dropping %zu records)",
                sw->shard_id, sw->count);
        sw->errors += (int)sw->count;
        return NULL;
    }

    /* Sub-bucket records by their kf shard. The bulk primitive holds
       one kf wrlock per call, so calling it once per kf shard amortises
       the lock acquisition across all records in that shard. */
    int splits = sw->sch->splits;
    SlotcaskBulkRec *batch = calloc(sw->count, sizeof(SlotcaskBulkRec));
    V2BulkInsCtx    *ctxs  = malloc(sw->count * sizeof(V2BulkInsCtx));
    int *kf_shards = malloc(sw->count * sizeof(int));
    int *counts    = calloc(splits, sizeof(int));
    int *offsets   = malloc(splits * sizeof(int));
    int *cursors   = calloc(splits, sizeof(int));
    if (!batch || !ctxs || !kf_shards || !counts || !offsets || !cursors) {
        LOG_ERROR(LOG_SUB_QUERY, "bulk_insert_shard_worker_v2: alloc failed, dropping %zu records for shard=%d", sw->count, sw->shard_id);
        free(batch); free(ctxs); free(kf_shards);
        free(counts); free(offsets); free(cursors);
        sw->errors += (int)sw->count;
        sw->wall_ms = now_ms_coarse() - t_worker_start;
        return NULL;
    }

    for (size_t i = 0; i < sw->count; i++) {
        kf_shards[i] = compute_record_shard(sw->records[i].hash, splits);
        counts[kf_shards[i]]++;
    }
    int run = 0;
    for (int s = 0; s < splits; s++) { offsets[s] = run; run += counts[s]; }

    /* Per-worker arena: replaces (count × nidx) per-field mallocs in the
       pre_commit hook with one allocation reused across every record's
       OLD-key extraction. NEW keys still come from malloc — they outlive
       the pre_commit return (queued into sw->idx_pairs[fi] and consumed
       by btree_bulk_merge). */
    enum { INDEX_KEY_MAX = 4096 };
    uint8_t *old_arena = (sw->nidx > 0)
        ? malloc((size_t)sw->nidx * INDEX_KEY_MAX)
        : NULL;

    /* Bitmap accumulators — per-field BmPair arrays. Reset between shard
       iterations (each shard flushes its own pending bm_set's, then
       counts go back to 0). Only allocated when there's at least one
       bitmap-typed field. */
    int has_bm_field = 0;
    if (sw->idx_types) {
        for (int fi = 0; fi < sw->nidx; fi++) {
            if (sw->idx_types[fi] == IT_BITMAP) { has_bm_field = 1; break; }
        }
    }
    if (has_bm_field) {
        sw->bm_pairs       = calloc((size_t)sw->nidx, sizeof(BmPair *));
        sw->bm_pair_counts = calloc((size_t)sw->nidx, sizeof(size_t));
        sw->bm_pair_caps   = calloc((size_t)sw->nidx, sizeof(size_t));
    }

    /* Pack records into [batch] grouped by kf shard, with [ctxs] mirrored. */
    for (size_t i = 0; i < sw->count; i++) {
        int s = kf_shards[i];
        int pos = offsets[s] + cursors[s]++;
        BulkInsRecord *r = &sw->records[i];
        ctxs[pos].sw  = sw;
        ctxs[pos].rec = r;
        ctxs[pos].old_arena      = old_arena;
        ctxs[pos].old_arena_slot = INDEX_KEY_MAX;
        batch[pos].key       = r->id;
        batch[pos].klen      = r->klen;
        batch[pos].value     = r->payload;
        batch[pos].vlen      = sw->ts->total_size;
        batch[pos].user_ctx  = &ctxs[pos];
        batch[pos].old_value = NULL;
        batch[pos].old_vlen  = 0;
        batch[pos].status    = 0;
        batch[pos].was_update = 0;
        batch[pos].if_not_exists = r->if_not_exists;
    }

    /* has_ac gate: only wire up the auto_create value_compute hook (and
       the OLD-record read it implies) when the schema actually declares
       an auto_create field. Ordinary bulk inserts pay nothing — this is
       the same has_ac gate the single-insert fix uses in
       storage.c's cmd_insert_v2. */
    int has_ac = 0;
    for (int i = 0; i < sw->ts->nfields; i++) {
        if (!sw->ts->fields[i].removed &&
            sw->ts->fields[i].default_kind == DK_AUTO_CREATE) { has_ac = 1; break; }
    }

    SlotcaskBulkOpts opts = {
        .if_not_exists        = sw->if_not_exists,
        .pre_commit           = v2_bulk_ins_pre_commit_bulk,
        /* OLD value only needed when there are indexes to update; otherwise
           the hook returns immediately. Tells the primitive to skip the
           per-record read_record_value on UPDATE. */
        .pre_commit_needs_old = sw->nidx > 0,
        .value_compute        = has_ac ? v2_bulk_ins_ac_value_compute : NULL,
    };
    for (int s = 0; s < splits; s++) {
        if (counts[s] == 0) continue;
        int rc = slotcask_bulk_upsert_in_kfshard(sdb, s,
                                                  batch + offsets[s],
                                                  (size_t)counts[s], &opts);
        if (rc != 0) {
            for (int k = 0; k < counts[s]; k++) {
                if (batch[offsets[s] + k].status == 0)
                    batch[offsets[s] + k].status = -1;
            }
        }

        /* Flush accumulated bitmap pairs for this shard. One bm_open
           per (shard, field) — wrlock acquired once, all bm_set's
           applied, wrlock released. Same amortisation pattern btree
           uses with btree_insert_batch in the merge phase. */
        if (has_bm_field && sw->bm_pairs) {
            int slots_per_shard =
                (int)slotcask_default_slots_for_splits(sw->sch->splits);
            for (int fi = 0; fi < sw->nidx; fi++) {
                if (sw->idx_types[fi] != IT_BITMAP) continue;
                if (sw->bm_pair_counts[fi] == 0) continue;
                if (!sw->bm_pairs[fi]) {
                    sw->bm_pair_counts[fi] = 0;
                    continue;
                }
                char bp[PATH_MAX];
                bm_build_path(bp, sizeof(bp), sw->db_root, sw->object,
                              sw->idx_fields[fi], s);
                /* bool_fastpath autodetect: first queued pair is 1 byte? */
                int bool_fast = (sw->bm_pair_counts[fi] > 0 &&
                                 sw->bm_pairs[fi][0].vlen == 1);
                BitmapShard *bm = bm_open(bp, slots_per_shard, 1,
                                          bool_fast, 0, 1 /* writer */);
                if (bm) {
                    /* Auto-grow if any pair exceeds current stride. The
                       largest slot in this batch bounds the requirement. */
                    uint32_t max_slot = 0;
                    for (size_t k = 0; k < sw->bm_pair_counts[fi]; k++) {
                        if (sw->bm_pairs[fi][k].slot > max_slot)
                            max_slot = sw->bm_pairs[fi][k].slot;
                    }
                    if (max_slot >= bm_slots(bm)) {
                        uint32_t want = max_slot + 1, grown = 1;
                        while (grown < want && grown < 0x80000000u) grown <<= 1;
                        bm_grow(bm, grown);
                    }
                    for (size_t k = 0; k < sw->bm_pair_counts[fi]; k++) {
                        BmPair *p = &sw->bm_pairs[fi][k];
                        bm_set(bm, p->value, p->vlen, p->slot);
                    }
                    bm_close(bm);
                }
                sw->bm_pair_counts[fi] = 0;
            }
        }
    }

    for (size_t i = 0; i < sw->count; i++) {
        if (batch[i].status == 0) {
            if (!batch[i].was_update) sw->inserted++;
        } else if (batch[i].status == -2) {
            sw->skipped++;
        } else {
            sw->errors++;
        }
    }
    free(batch); free(ctxs); free(kf_shards);
    free(counts); free(offsets); free(cursors);
    free(old_arena);
    /* Free per-field bitmap accumulators (all already flushed above). */
    if (sw->bm_pairs) {
        for (int fi = 0; fi < sw->nidx; fi++) free(sw->bm_pairs[fi]);
        free(sw->bm_pairs); free(sw->bm_pair_counts); free(sw->bm_pair_caps);
        sw->bm_pairs = NULL;
        sw->bm_pair_counts = NULL;
        sw->bm_pair_caps = NULL;
    }
    bm_flush_thread_bitmap_cache();  /* no-op shim, kept for symmetry */
    sw->wall_ms = now_ms_coarse() - t_worker_start;
    return NULL;
}

/* Probe + write every record in one shard's bucket under a single ucache
   wrlock held from start to finish. On shard-full, release the lock, grow
   the shard, reacquire, and retry the **same** record index — avoids
   per-record churn. Collects index entries into sw->idx_pairs for later
   merge/bulk-build. pthread-compatible signature: workers in different
   shard buckets never touch each other's shards, so the wrlocks are
   disjoint and no cross-worker coordination is needed. */
static void *bulk_insert_shard_worker(void *arg) {
    BulkInsShardWork *sw = (BulkInsShardWork *)arg;
    if (sw->count == 0) return NULL;
    return bulk_insert_shard_worker_v2(sw);
}

/* Internal: bulk insert from a json string already in memory (no file I/O) */
int cmd_bulk_insert_string(const char *db_root, const char *object, char *json_str, int if_not_exists);

/* ---- bulk_ins_run: shared parser + write body --------------------------
   Caller owns `data` (null-terminated, len bytes). This helper never
   frees or munmaps it — all cleanup of the buffer belongs to the caller.
   Returns 0 on success, 1 on parse/insert error (JSON error already
   written via OUT() before returning). */
static int bulk_ins_run(const char *db_root, const char *object,
                        const char *data, size_t len, int if_not_exists) {
    const char *json = data;
    uint64_t t0 = now_ms_coarse();

    /* Load config ONCE */
    Schema sc = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nfields = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nfields; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */
    (void)nfields; /* indexes are walked per-shard later via load_index_fields */

    TypedSchema *ts = load_typed_schema(db_root, object);

    /* Invariant check hoisted out of the per-record loop — ts->total_size
       and sc.max_value don't change during a bulk insert. */
    if (ts && ts->total_size > sc.max_value) {
        fprintf(stderr, "Error: typed record size %d exceeds max_value %d\n",
                ts->total_size, sc.max_value);
        return 1;
    }

    /* ucache handles shard caching */

    /* Pre-allocate BtEntry collectors for bulk B+ tree build at end.
       Clamp nfields explicitly so GCC's LTO range analysis can see the
       upper bound (load_index_fields already clamps to MAX_FIELDS, but
       gcc loses track across TUs and warns about theoretical size_t
       overflow in the calloc). */
    if (nfields < 0) nfields = 0;
    if (nfields > MAX_FIELDS) nfields = MAX_FIELDS;
    size_t nf_sz = (size_t)nfields;
    BtEntry **idx_pairs = calloc(nf_sz, sizeof(BtEntry *));
    size_t *idx_pair_counts = calloc(nf_sz, sizeof(size_t));
    size_t *idx_pair_caps = calloc(nf_sz, sizeof(size_t));
    for (int i = 0; i < nfields; i++) {
        idx_pair_caps[i] = 4096;
        idx_pairs[i] = malloc(idx_pair_caps[i] * sizeof(BtEntry));
    }

    /* Pre-resolve index field indices so the per-record loop doesn't re-scan
       the schema for every record × every index. Saves O(records × indexes ×
       nfields) strcmps on large bulk inserts. Composite indexes: up to 16
       sub-fields each. */
    int idx_field_indices[MAX_FIELDS][16];
    int idx_field_counts[MAX_FIELDS];
    int idx_is_composite[MAX_FIELDS];
    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);
    for (int fi = 0; fi < nfields; fi++) {
        idx_is_composite[fi] = (strchr(idx_fields[fi], '+') != NULL);
        idx_field_counts[fi] = 0;
        if (idx_is_composite[fi]) {
            char fb[256]; strncpy(fb, idx_fields[fi], 255); fb[255] = '\0';
            char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
            while (tok && idx_field_counts[fi] < 16) {
                int fidx = ts ? typed_field_index(ts, tok) : -1;
                if (fidx < 0) {
                    LOG_ERROR(LOG_SUB_CONFIG,
                              "index.conf drift on object '%s': index '%s' "
                              "references unknown sub-field '%s' (bulk-insert)",
                              object, idx_fields[fi], tok);
                }
                idx_field_indices[fi][idx_field_counts[fi]++] = fidx;
                tok = strtok_r(NULL, "+", &_tok_save);
            }
        } else {
            int fidx = ts ? typed_field_index(ts, idx_fields[fi]) : -1;
            if (fidx < 0) {
                LOG_ERROR(LOG_SUB_CONFIG,
                          "index.conf drift on object '%s': field '%s' "
                          "indexed but not in fields.conf (bulk-insert)",
                          object, idx_fields[fi]);
            }
            idx_field_indices[fi][0] = fidx;
            idx_field_counts[fi] = 1;
        }
    }

    const char *p = json_skip(json);
    int is_object_format = (*p == '{'); /* {"k1":{...},"k2":{...}} */
    int is_array_format = (*p == '[');  /* [{"key":"k1","value":{...}},...] */

    if (!is_object_format && !is_array_format) {
        fprintf(stderr, "Error: Expected JSON object or array\n");
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        return 1;
    }
    p++;

    int count = 0, errors = 0;

    /* Auto-key bookkeeping: when the object has auto_key set, the wire
       form (36-char dashed UUID or decimal int) is parsed to binary for
       storage, and the wire string is captured per-record so the final
       response can emit a `keys[]` array. omit-key records are marked
       with NULL wire_key here; we fill them in below after parse with a
       single batched generation. */
    int auto_key_mode = sc.auto_key;
    char **wire_keys = NULL;       /* per-record rendered key (NULL → fill from omit pool) */
    size_t wire_cap = 0;
    int n_omits = 0;
    int validation_failed_idx = -1;

    /* Per-request statement timeout (timeout_ms / global TIMEOUT). Trip
       check is in the parse loop only — the parallel write/index phases
       run on records already in memory, so a deadline trip mid-parse
       aborts before any disk write. Records past the trip point are
       discarded; nothing is committed. */
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    int dl_counter = 0;

    /* ===== Phase 1: parse every record into a buffered records[] array. Per-record
       `id` + `payload` (ts->total_size bytes) are bump-allocated from a single
       BulkArena so the 2-mallocs-per-record cost is replaced by O(1) pointer
       advance. Arena freed after Phase-2 workers join — ordering holds because
       the post-phase merge loop reads records[i].id/.payload BEFORE arena_free. */
    BulkArena *arena = arena_new(256 * 1024);
    size_t rec_cap = 1024, rec_count = 0;
    BulkInsRecord *records = calloc(rec_cap, sizeof(BulkInsRecord));

    while (*p) {
        if (query_deadline_tick(&dl, &dl_counter)) break;
        p = json_skip(p);
        if (*p == ']' || *p == '}') break;
        if (*p == ',') { p++; continue; }

        char *id = NULL;
        size_t klen = 0;
        const char *data_ptr = NULL;  /* span into json mmap (object fmt) or obj_str (array fmt) */
        /* obj_str owns the array-format record's NUL-terminated copy so
           json_parse_object can walk it; must stay alive through the
           typed_encode_defaults call below. Freed at end of iteration. */
        char obj_buf[8192];
        char *obj_str = NULL;
        int obj_heap = 0;

        if (is_object_format) {
            /* Object format: "key": {...} */
            if (*p != '"') { p++; continue; }
            p++;
            const char *key_start = p;
            while (*p && *p != '"') p++;
            klen = p - key_start;
            id = arena_strndup(&arena, key_start, klen);
            if (*p == '"') p++;
            p = json_skip(p);
            if (*p == ':') p = json_skip(p + 1);

            /* Data span points into the original mmap — NUL-terminated at
               buffer end; typed_encode_defaults stops at the matching brace. */
            data_ptr = p;
            p = json_skip_value(p);
        } else {
            /* Array format: {"key":"k1","value":{...}} */
            if (*p != '{') { p++; continue; }
            const char *obj_start = p;
            const char *obj_end = json_skip_value(p);
            size_t obj_len = obj_end - obj_start;

            if (obj_len < sizeof(obj_buf)) {
                memcpy(obj_buf, obj_start, obj_len);
                obj_buf[obj_len] = '\0';
                obj_str = obj_buf;
            } else {
                obj_str = malloc(obj_len + 1);
                memcpy(obj_str, obj_start, obj_len);
                obj_str[obj_len] = '\0';
                obj_heap = 1;
            }

            JsonObj rec;
            json_parse_object(obj_str, obj_len, &rec);
            const char *iv; size_t ivl;
            if (json_obj_unquoted(&rec, "key", &iv, &ivl)) {
                id = arena_strndup(&arena, iv, ivl);
                klen = ivl;
            }
            const char *dv; size_t dl;
            if (json_obj_get(&rec, "value", &dv, &dl)) {
                data_ptr = dv;  /* span into obj_str */
                (void)dl;       /* encoder finds matching brace itself */
            }
            p = obj_end;
        }

        /* For auto-key objects: if the parsed id is a provided wire-form
           key, normalise to the storage binary form. If it's an
           omit-key record (only possible in array format on auto-key),
           defer key assignment to the post-parse batch. */
        char *wire_for_record = NULL;
        if (data_ptr && auto_key_mode != AK_NONE) {
            if (id) {
                /* Provided wire key — parse to binary and replace id+klen.
                   The wire string lives in arena, which is freed before the
                   final response emit; strdup so wire_keys[] outlives the
                   arena. Heap pointer is freed alongside the wire_keys
                   array after the response is written. */
                wire_for_record = strdup(id);
                if (auto_key_mode == AK_UUID) {
                    uint8_t bin[16];
                    if (parse_uuid_string(id, bin) == 0) {
                        id = (char *)arena_alloc(&arena, 16);
                        memcpy(id, bin, 16);
                        klen = 16;
                    } else {
                        if (validation_failed_idx < 0) validation_failed_idx = (int)rec_count;
                        errors++;
                        /* Parse failed: id is nulled below, so the id&&data_ptr
                           block that would otherwise store-or-free
                           wire_for_record is never reached — free it here. */
                        free(wire_for_record);
                        wire_for_record = NULL;
                        id = NULL;
                    }
                } else { /* AK_SEQ */
                    int64_t v;
                    if (parse_seq_key(id, &v) == 0) {
                        id = (char *)arena_alloc(&arena, 8);
                        for (int b = 7; b >= 0; b--) { id[b] = (char)(v & 0xFF); v >>= 8; }
                        klen = 8;
                    } else {
                        if (validation_failed_idx < 0) validation_failed_idx = (int)rec_count;
                        errors++;
                        free(wire_for_record);
                        wire_for_record = NULL;
                        id = NULL;
                    }
                }
            } else {
                /* Omit-key record — placeholder; id+klen filled in post-parse. */
                id = (char *)arena_alloc(&arena, auto_key_mode == AK_UUID ? 16 : 8);
                memset(id, 0, auto_key_mode == AK_UUID ? 16 : 8);
                klen = auto_key_mode == AK_UUID ? 16 : 8;
                n_omits++;
                wire_for_record = NULL;
            }
        }

        if (id && data_ptr) {
            if ((int)klen > sc.max_key) {
                errors++;
                /* id lives in arena — dropped bytes are trivial, no free here */
            } else {
                /* Allocate payload in arena up front and encode the record
                   directly into it — skips the typed_tmp bounce + memcpy.
                   Strict enum validation: -2 → record has an unknown
                   enum value. Count as a per-record error + skip the
                   insert (best-effort batch semantics, same as varchar-
                   too-long), unlike auto-key parse failures which abort
                   the whole bulk because the key isn't usable. */
                uint8_t *payload = arena_alloc(&arena, ts->total_size);
                int _enc = typed_encode_defaults(ts, data_ptr, payload,
                                                  ts->total_size,
                                                  db_root, object, NULL, 0);
                if (_enc == -2) {
                    errors++;
                    if (obj_heap) free(obj_str);
                    free(wire_for_record);
                    continue;
                }

                if (rec_count >= rec_cap) {
                    rec_cap *= 2;
                    BulkInsRecord *t = xrealloc_or_free(records, rec_cap * sizeof(*t));
                    if (!t) {
                        /* Reset rec_count so phase 1.5 below sees an empty
                           set instead of dereferencing NULL records. */
                        records = NULL;
                        rec_count = 0;
                        break;
                    }
                    records = t;
                }
                /* Grow wire_keys sidecar in lockstep with records when
                   auto_key is active. */
                if (auto_key_mode != AK_NONE) {
                    if (rec_count >= wire_cap) {
                        size_t new_wc = wire_cap ? wire_cap * 2 : 1024;
                        if (new_wc < rec_cap) new_wc = rec_cap;
                        char **t2 = realloc(wire_keys, new_wc * sizeof(char *));
                        if (t2) { wire_keys = t2; wire_cap = new_wc; }
                    }
                    if (wire_keys && rec_count < wire_cap) {
                        wire_keys[rec_count] = wire_for_record;
                    }
                }
                BulkInsRecord *r = &records[rec_count++];
                r->id = id;
                r->payload = payload;
                r->klen = klen;
                r->if_not_exists = 0;  /* default; auto-key omit post-fill sets to 1 */
                compute_hash_raw(id, klen, r->hash);
                /* Shard mapping that keeps each v2 worker owning one kf
                   shard — no cross-worker kf-wrlock contention. */
                r->shard_id = compute_record_shard(r->hash, sc.splits);
            }
        }
        if (obj_heap) free(obj_str);
    }

    /* Validation failure: refuse the whole batch up front so we don't
       half-write some records with mangled keys. */
    if (validation_failed_idx >= 0) {
        if (wire_keys) {
            for (size_t i = 0; i < rec_count; i++) free(wire_keys[i]);
        }
        free(records); free(wire_keys);
        arena_free(arena);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        OUT("{\"error\":\"bulk-insert validation failed at record %d: malformed key for auto_key mode\"}\n",
            validation_failed_idx);
        return 1;
    }

    /* Post-parse: for auto-key omit records, batch-generate keys in one
       shot (single fill_random call for UUID, single seq flock for seq)
       and fill in id+klen + wire_keys per record. */
    if (auto_key_mode != AK_NONE && n_omits > 0) {
        uint8_t *uuid_pool = NULL;
        long long seq_start = 0;
        int keygen_failed = 0;
        if (auto_key_mode == AK_UUID) {
            uuid_pool = malloc((size_t)n_omits * 16);
            if (!uuid_pool || gen_uuid4_batch(uuid_pool, n_omits) != 0)
                keygen_failed = 1;
        } else {
            seq_start = seq_next_val_batch(db_root, object, sc.auto_key_seq_name, n_omits);
            if (seq_start < 0) keygen_failed = 1;
        }
        if (keygen_failed) {
            free(uuid_pool);
            if (wire_keys) {
                for (size_t i = 0; i < rec_count; i++) free(wire_keys[i]);
            }
            free(records); free(wire_keys);
            arena_free(arena);
            for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
            free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
            OUT("{\"error\":\"bulk-insert key generation failed: %s\"}\n",
                auto_key_mode == AK_UUID ? "random source unavailable"
                                         : "sequence unavailable");
            return 1;
        }
        int omit_idx = 0;
        for (size_t i = 0; i < rec_count; i++) {
            if (wire_keys && wire_keys[i]) continue;  /* provided key */
            BulkInsRecord *r = &records[i];
            if (auto_key_mode == AK_UUID) {
                memcpy(r->id, uuid_pool + omit_idx * 16, 16);
                char wbuf[37];
                format_uuid_string((const uint8_t *)r->id, wbuf);
                if (wire_keys) wire_keys[i] = strdup(wbuf);
            } else {
                int64_t v = seq_start + omit_idx;
                for (int b = 7; b >= 0; b--) { r->id[b] = (char)(v & 0xFF); v >>= 8; }
                char wbuf[24];
                snprintf(wbuf, sizeof(wbuf), "%lld", (long long)(seq_start + omit_idx));
                if (wire_keys) wire_keys[i] = strdup(wbuf);
            }
            /* Recompute hash + shard since id was a placeholder during parse. */
            compute_hash_raw(r->id, r->klen, r->hash);
            r->shard_id = compute_record_shard(r->hash, sc.splits);
            /* Strict-insert on collision — see BulkInsRecord.if_not_exists.
               UUID can't collide in practice; seq can collide if the
               operator pre-inserted manual numeric keys at or above the
               watermark, and silent overwrite would corrupt their data. */
            r->if_not_exists = 1;
            omit_idx++;
        }
        free(uuid_pool);
    }

    /* If parse tripped the deadline, abort before any write phase. Same
       cleanup order as the OOM bail below. */
    if (dl.timed_out) {
        if (wire_keys) {
            for (size_t i = 0; i < rec_count; i++) free(wire_keys[i]);
        }
        free(records); free(wire_keys);
        arena_free(arena);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        OUT("{\"error\":\"query_timeout\"}\n");
        return 1;
    }

    /* ===== Phase 1.5: bucket records by shard_id so each worker owns one shard's
       writes and can hold the ucache wrlock once for the entire bucket.
       OOM at any of the allocs below frees every prior allocation
       (records, arena, idx_pairs[], idx_pair_*, json buffer) in reverse
       order before bailing — same cleanup the success path runs at the
       function tail, just earlier. */
    int *shard_counts = calloc(sc.splits, sizeof(int));
    if (!shard_counts) {
        free(records);
        arena_free(arena);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        OUT("{\"error\":\"oom: bulk_insert shard_counts\"}\n");
        return 1;
    }
    for (size_t i = 0; i < rec_count; i++) shard_counts[records[i].shard_id]++;

    /* ===== Phase 1.6: pre-grow shards once, sized to the incoming batch.
       Letting the worker hit the in-loop grow path repeatedly costs O(slots)
       per grow, and each subsequent grow re-buckets a growing record set —
       so total rebuild work scales with the number of incremental doublings.
       One up-front grow to the right size avoids that.
       Slotcask keyfile shards are pre-sized at slotcask_open time and
       resplit per-shard internally on load — slotcask_pregrow_kf below
       absorbs the same role for the kf. */
    {
        /* Pre-grow kf shards to fit the incoming records up-front. Without
           this, kf resplits trigger inline during the bulk insert and
           their msync(MS_SYNC) joins the segment writeback queue — a
           12.6 MB kf flush balloons from ~50ms to 4-5 sec at 25M scale
           because it's stuck behind hundreds of MB of concurrent segment
           dirty pages. Doing it now (no inserter active yet) keeps each
           per-shard resplit at its quiet-system cost. */
        SlotcaskSchemaInfo info = {
            .splits = sc.splits, .slot_size = sc.slot_size,
            .streams = sc.streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
        if (sdb) (void)slotcask_pregrow_kf(sdb, rec_count);
    }

    int *worker_shards = NULL, *shard_to_worker = NULL;
    int nshard_groups = build_shard_worker_map(shard_counts, sc.splits,
                                                &worker_shards, &shard_to_worker);
    BulkInsShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkInsShardWork)) : NULL;
    if (nshard_groups < 0 || (nshard_groups > 0 && !workers)) {
        free(workers); free(worker_shards); free(shard_to_worker);
        free(shard_counts); free(records);
        arena_free(arena);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        OUT("{\"error\":\"oom: bulk_insert workers\"}\n");
        return 1;
    }
    for (int g = 0; g < nshard_groups; g++) {
        int s = worker_shards[g];
        workers[g].shard_id = s;
        workers[g].records = calloc(shard_counts[s], sizeof(BulkInsRecord));
        workers[g].count = 0;
    }
    /* Invariant: rec_count > 0 ⇒ at least one shard has count > 0 ⇒
       nshard_groups > 0 ⇒ workers != NULL (post-OOM-guard above).
       Coverity can't trace the chain, so the loop body's workers[w]
       deref is flagged FORWARD_NULL — annotate. */
    for (size_t i = 0; i < rec_count; i++) {
        int w = shard_to_worker[records[i].shard_id];
        /* coverity[forward_null] workers is non-NULL when rec_count > 0 */
        workers[w].records[workers[w].count++] = records[i];  /* shallow copy, ownership transferred */
    }
    free(records);
    free(shard_counts);
    free(worker_shards);
    free(shard_to_worker);

    /* Wire read-only worker context + allocate per-worker idx-entry collectors. */
    for (int wi = 0; wi < nshard_groups; wi++) {
        /* coverity[forward_null] same invariant — nshard_groups > 0 ⇒ workers non-NULL */
        BulkInsShardWork *ws = &workers[wi];
        ws->db_root = db_root;
        ws->object = object;
        ws->sch = &sc;
        ws->ts = ts;
        ws->nidx = nfields;
        ws->idx_fields = (const char (*)[256])idx_fields;
        ws->idx_field_indices = (const int (*)[16])idx_field_indices;
        ws->idx_field_counts = idx_field_counts;
        ws->idx_is_composite = idx_is_composite;
        ws->idx_types = idx_types;
        ws->inserted = 0;
        ws->errors = 0;
        ws->skipped = 0;
        ws->if_not_exists = if_not_exists;
        ws->idx_pairs = nfields > 0 ? calloc(nfields, sizeof(BtEntry *)) : NULL;
        ws->idx_pair_counts = nfields > 0 ? calloc(nfields, sizeof(size_t)) : NULL;
        ws->idx_pair_caps = nfields > 0 ? calloc(nfields, sizeof(size_t)) : NULL;
    }

    uint64_t t1 = now_ms_coarse();  /* end of Phase 1 (parse + bucket) */

    /* ===== Phase 2: run shard workers in parallel. Each worker owns one shard's
       writes so ucache wrlocks are disjoint across workers — no cross-worker
       coordination needed. Batched pthread_create/join pattern matches
       bulk_del_shard_worker. Serial fallback when thread count ≤ 1 or workload
       is small enough that spawn/join overhead would dominate. */
    parallel_for_io(bulk_insert_shard_worker, workers, nshard_groups,
                 sizeof(BulkInsShardWork));
    uint64_t t2 = now_ms_coarse();  /* end of Phase 2 (parallel shard write) */

    /* Phase-2 breakdown across workers. */
    uint64_t grow_ms_total = 0, wall_ms_max = 0;
    int grow_count_total = 0;
    for (int wi = 0; wi < nshard_groups; wi++) {
        grow_ms_total  += workers[wi].grow_ms;
        grow_count_total += workers[wi].grow_count;
        if (workers[wi].wall_ms > wall_ms_max) wall_ms_max = workers[wi].wall_ms;
    }

    /* Merge per-worker results into the caller's global counters and index arrays,
       then release per-worker scratch. BtEntry.value ownership transfers into
       the global idx_pairs — freed later by the idx-build cleanup. */
    int skipped_total = 0;
    for (int wi = 0; wi < nshard_groups; wi++) {
        BulkInsShardWork *ws = &workers[wi];
        count  += ws->inserted;
        errors += ws->errors;
        skipped_total += ws->skipped;

        for (int fi = 0; fi < nfields; fi++) {
            size_t add = ws->idx_pair_counts[fi];
            if (add == 0) { free(ws->idx_pairs[fi]); continue; }
            if (idx_pair_counts[fi] + add > idx_pair_caps[fi]) {
                size_t new_cap = idx_pair_caps[fi];
                while (idx_pair_counts[fi] + add > new_cap) new_cap *= 2;
                BtEntry *t = xrealloc_or_free(idx_pairs[fi], new_cap * sizeof(BtEntry));
                if (!t) {
                    LOG_ERROR(LOG_SUB_INDEX, "INDEX_OOM merge field_idx=%d (dropped %zu pairs; rerun reindex)",
                            fi, idx_pair_counts[fi] + add);
                    idx_pairs[fi] = NULL;
                    idx_pair_counts[fi] = 0;
                    idx_pair_caps[fi] = 0;
                    /* free the per-worker key_bufs we'd have copied in */
                    for (size_t k = 0; k < add; k++) free((void *)ws->idx_pairs[fi][k].value);
                    free(ws->idx_pairs[fi]);
                    continue;
                }
                idx_pairs[fi] = t;
                idx_pair_caps[fi] = new_cap;
            }
            memcpy(idx_pairs[fi] + idx_pair_counts[fi],
                   ws->idx_pairs[fi], add * sizeof(BtEntry));
            idx_pair_counts[fi] += add;
            free(ws->idx_pairs[fi]);
        }

        /* records[ri].id / .payload live in the arena — freed in bulk below */
        free(ws->records);
        free(ws->idx_pairs);
        free(ws->idx_pair_counts);
        free(ws->idx_pair_caps);
    }
    free(workers);
    arena_free(arena);

    /* Activate all dirty shards for THIS object — filter by path prefix.
       v2 (slotcask) commits each record individually via the keyfile flip,
       so there's no batched .new→active activation step to run here. */
    uint64_t t3 = now_ms_coarse();  /* end of Phase 3 (activate) */

    /* ucache keeps mmaps open — OS flushes dirty pages */
    /* (caller owns the json buffer — no free/munmap here) */

    /* Bulk write indexes — one worker per field; the worker streams the per-
       shard merges sequentially. Halves dispatch overhead vs the old
       per-(field, shard) layout that flooded the 16-thread pool with
       nfields × idx_n tiny tasks. */
    if (nfields > 0) {
        IdxFieldArg *fa = malloc((size_t)nfields * sizeof(IdxFieldArg));
        int fa_count = 0;
        for (int fi = 0; fi < nfields; fi++) {
            if (idx_pair_counts[fi] == 0) continue;
            fa[fa_count++] = (IdxFieldArg){
                .db_root = db_root, .object = object, .field = idx_fields[fi],
                .splits = sc.splits,
                .new_entries = idx_pairs[fi], .new_count = idx_pair_counts[fi],
            };
        }
        parallel_for(idx_build_field_worker, fa, fa_count, sizeof(IdxFieldArg));
        free(fa);
        /* Free the value strings — owned by idx_pairs[fi]. */
        for (int fi = 0; fi < nfields; fi++) {
            for (size_t ei = 0; ei < idx_pair_counts[fi]; ei++)
                free((char *)idx_pairs[fi][ei].value);
            free(idx_pairs[fi]);
        }
    }
    free(idx_pairs);
    free(idx_pair_counts);
    free(idx_pair_caps);

    if (count > 0) update_count(db_root, object, count);

    uint64_t t4 = now_ms_coarse();  /* end of Phase 4 (index build) */
    LOG_INFO(LOG_SUB_QUERY, "BULK-INSERT %s: rows=%d phase1_parse=%lums phase2_write=%lums (grows=%d grow_total=%lums per_worker_max=%lums) phase3_activate=%lums phase4_index=%lums total=%lums",
            object, count,
            (unsigned long)(t1 - t0),
            (unsigned long)(t2 - t1),
            grow_count_total,
            (unsigned long)grow_ms_total,
            (unsigned long)wall_ms_max,
            (unsigned long)(t3 - t2),
            (unsigned long)(t4 - t3),
            (unsigned long)(t4 - t0));

    /* Auto-key response includes the resolved keys[] array (rendered
       wire form, in input order). Today's plain {"inserted":N,...} shape
       stays for AK_NONE objects to keep wire compatibility. */
    if (auto_key_mode != AK_NONE && wire_keys) {
        OUT("{\"status\":\"bulk-inserted\",\"count\":%d,\"skipped\":%d,\"keys\":[",
            count, skipped_total);
        for (size_t i = 0; i < rec_count; i++) {
            OUT("%s\"%s\"", i ? "," : "", wire_keys[i] ? wire_keys[i] : "");
        }
        OUT("]}\n");
        for (size_t i = 0; i < rec_count; i++) free(wire_keys[i]);
        free(wire_keys);
        wire_keys = NULL;
    } else if (errors) {
        OUT("{\"inserted\":%d,\"skipped\":%d,\"errors\":%d,\"error\":\"some_records_dropped\"}\n",
            count, skipped_total, errors);
        fprintf(stderr, "%d errors during bulk insert (see info log for dropped keys)\n", errors);
    } else if (skipped_total > 0) {
        OUT("{\"inserted\":%d,\"skipped\":%d}\n", count, skipped_total);
    } else {
        OUT("{\"inserted\":%d}\n", count);
    }
    free(wire_keys);  /* no-op if already freed via the auto-key branch */
    return errors > 0 ? 1 : 0;
}

int cmd_bulk_insert(const char *db_root, const char *object, const char *input,
                    int if_not_exists) {
    size_t len;
    char *json;
    int json_mmaped = 0;
    if (input) {
        /* mmap the file instead of malloc — OS pages in/out as we scan */
        int ifd = open(input, O_RDONLY);
        if (ifd < 0) { fprintf(stderr, "Error: Cannot open %s\n", input); return 1; }
        struct stat st;
        if (fstat(ifd, &st) < 0) { close(ifd); fprintf(stderr, "Error: Cannot stat %s\n", input); return 1; }
        len = st.st_size;
        if (len == 0) { close(ifd); fprintf(stderr, "Error: Empty input\n"); return 1; }
        /* MAP_PRIVATE on a file fd lets us write the trailing NUL into
           the partial last page (zero-filled slack, copy-on-write). But
           if `len` is *exactly* page-aligned, there is no partial last
           page — byte [len] lives on a NEW page that the kernel does
           NOT back. Writing there SIGBUSes. Skip the mmap path in that
           case and fall through to alloc+read, which owns its memory.
           Reproduced via ASan when a 5000-row bulk-insert chunk landed
           on exactly 0x86000 bytes (548 KB) after schema-shrink. */
        long pgsize_l = sysconf(_SC_PAGESIZE);
        size_t pgsize = (pgsize_l > 0) ? (size_t)pgsize_l : 4096;
        int try_mmap = (len % pgsize) != 0;
        json = try_mmap
            ? mmap(NULL, len + 1, PROT_READ | PROT_WRITE, MAP_PRIVATE, ifd, 0)
            : MAP_FAILED;
        if (json == MAP_FAILED) {
            /* Fallback: allocate and read */
            json = malloc(len + 1);
            if (!json) { close(ifd); fprintf(stderr, "Error: Cannot allocate\n"); return 1; }
            lseek(ifd, 0, SEEK_SET);
            size_t rd = 0;
            while (rd < len) {
                ssize_t n = read(ifd, json + rd, len - rd);
                if (n <= 0) break;
                rd += n;
            }
            json[len] = '\0';
        } else {
            json_mmaped = 1;
            madvise(json, len, MADV_SEQUENTIAL);
            /* Null-terminate — MAP_PRIVATE so write is COW on last page only */
            json[len] = '\0';
        }
        close(ifd);
    } else {
        size_t cap = 65536, pos = 0;
        json = malloc(cap);
        if (!json) { fprintf(stderr, "Error: out of memory reading stdin\n"); return 1; }
        int c;
        while ((c = fgetc(stdin)) != EOF) {
            if (pos >= cap - 1) {
                cap *= 2;
                char *t = xrealloc_or_free(json, cap);
                if (!t) { json = NULL; break; }
                json = t;
            }
            json[pos++] = c;
        }
        if (!json) { fprintf(stderr, "Error: out of memory reading stdin\n"); return 1; }
        json[pos] = '\0'; len = pos;
    }

    int rc = bulk_ins_run(db_root, object, json, len, if_not_exists);
    if (json_mmaped) munmap(json, len + 1); else free(json);
    return rc;
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
    uint64_t t0 = now_ms_coarse();

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
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    /* Clamp to make the bound visible to -Walloc-size-larger-than: the
       loader caps at MAX_FIELDS internally, but the signed int return
       leaves the static analyzer treating `(size_t)nidx` as potentially
       huge. */
    if (nidx < 0) nidx = 0;
    if (nidx > MAX_FIELDS) nidx = MAX_FIELDS;
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */

    /* Synthesise a stat-like view so the existing body that referred to
       st.st_size continues to compile after the helper split. The helper
       never owns data — caller frees. */
    struct stat st = { .st_size = (off_t)size };

    /* Invariant check hoisted out of the per-record loop — ts->total_size
       and sc.max_value don't change during a bulk insert. */
    if (ts->total_size > sc.max_value) {
        OUT("{\"error\":\"typed record size exceeds max_value\"}\n");
        return 1;
    }

    size_t nidx_sz = (size_t)nidx;  /* now demonstrably ≤ MAX_FIELDS */
    BtEntry **idx_pairs = calloc(nidx_sz, sizeof(BtEntry *));
    size_t *idx_pair_counts = calloc(nidx_sz, sizeof(size_t));
    size_t *idx_pair_caps = calloc(nidx_sz, sizeof(size_t));
    for (int i = 0; i < nidx; i++) {
        idx_pair_caps[i] = 4096;
        idx_pairs[i] = malloc(idx_pair_caps[i] * sizeof(BtEntry));
    }

    /* Pre-resolve index field indices */
    int idx_field_indices[MAX_FIELDS][16]; /* sub-field indices for each index */
    int idx_field_counts[MAX_FIELDS];
    int idx_is_composite[MAX_FIELDS];
    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);
    for (int fi = 0; fi < nidx; fi++) {
        idx_is_composite[fi] = (strchr(idx_fields[fi], '+') != NULL);
        idx_field_counts[fi] = 0;
        if (idx_is_composite[fi]) {
            char fb[256]; strncpy(fb, idx_fields[fi], 255); fb[255] = '\0';
            char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
            while (tok && idx_field_counts[fi] < 16) {
                int fidx = typed_field_index(ts, tok);
                if (fidx < 0) {
                    LOG_ERROR(LOG_SUB_CONFIG,
                              "index.conf drift on object '%s': index '%s' "
                              "references unknown sub-field '%s' (bulk-delete)",
                              object, idx_fields[fi], tok);
                }
                idx_field_indices[fi][idx_field_counts[fi]++] = fidx;
                tok = strtok_r(NULL, "+", &_tok_save);
            }
        } else {
            int fidx = typed_field_index(ts, idx_fields[fi]);
            if (fidx < 0) {
                LOG_ERROR(LOG_SUB_CONFIG,
                          "index.conf drift on object '%s': field '%s' "
                          "indexed but not in fields.conf (bulk-delete)",
                          object, idx_fields[fi]);
            }
            idx_field_indices[fi][0] = fidx;
            idx_field_counts[fi] = 1;
        }
    }

    int count = 0, errors = 0;
    const char *rp = data;                /* read pointer — never written to */
    const char *data_end = data + st.st_size;

    /* Auto-key bookkeeping — mirrors cmd_bulk_insert (JSON path).
       When the object has auto_key set, the wire form of the first
       column is parsed to binary for storage, or — when the first
       column is empty — the record is flagged as omit-key and gets a
       batch-generated key after parse. wire_keys[] tracks rendered
       forms in input order so the final response can emit `keys[]`. */
    int auto_key_mode = sc.auto_key;
    char **wire_keys = NULL;
    size_t wire_cap = 0;
    int n_omits = 0;
    int validation_failed_idx = -1;

    /* Compute active-field mapping once — tombstone set is fixed for the
       lifetime of this bulk insert. Common case (no tombstones) hits the
       fast path that matches direct positional encoding. */
    int active_indices[MAX_FIELDS];
    int active_count = 0;
    int has_tombstones = 0;
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed) has_tombstones = 1;
        else active_indices[active_count++] = i;
    }

    /* Length-based parsing: we work directly against the mmap'd page cache
       with (ptr, len) spans and never copy the line body. This removes the
       ~100 B-per-line memcpy that dominated memory bandwidth under parallel
       load (multiple workers each churning ~1 GB of memcpy on a 10 M-record
       CSV). encode_field_len / memcpy-with-length handle the null-terminator-
       free parse on the numeric / varchar / index sides. */
    struct { const char *ptr; size_t len; } vals[MAX_FIELDS];

    /* Per-request statement timeout — matches the JSON bulk-insert path. */
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    int dl_counter = 0;

    /* ===== Phase 1: parse every CSV line into a buffered records[] array. The
       (ptr, len) span path stays zero-copy for the CSV body; `id` + typed
       `payload` come from a single BulkArena (bump alloc, 8-byte aligned) so
       the 2-mallocs-per-record cost disappears. Arena freed after Phase-2
       workers join. */
    BulkArena *arena = arena_new(256 * 1024);
    size_t rec_cap = 1024, rec_count = 0;
    BulkInsRecord *records = calloc(rec_cap, sizeof(BulkInsRecord));

    while (rp < data_end) {
        if (query_deadline_tick(&dl, &dl_counter)) break;
        /* Find end of line without modifying the buffer */
        const char *eol = rp;
        while (eol < data_end && *eol != '\n' && *eol != '\r') eol++;
        size_t line_len = eol - rp;
        if (line_len == 0) { /* empty line */
            rp = eol + 1;
            if (rp < data_end && *(rp - 1) == '\r' && *rp == '\n') rp++;
            continue;
        }

        const char *line_start = rp;
        const char *line_end   = eol;

        /* Advance read pointer past this line (consume \r\n / \n / \r). */
        rp = eol;
        if (rp < data_end && *rp == '\r') rp++;
        if (rp < data_end && *rp == '\n') rp++;

        /* First field is the key; remaining fields in fields.conf order.
           For auto-key objects, an empty first field means "generate";
           a non-empty wire form is parsed to the binary storage form. */
        const char *key_end = line_start;
        while (key_end < line_end && *key_end != delimiter) key_end++;
        if (key_end == line_end) continue;  /* no delimiter — skip line */
        const char *id_start = line_start;
        size_t klen = key_end - line_start;

        /* Auto-key path: normalise the wire-form first column or flag
           as omit for post-parse batch generation. */
        char *auto_id = NULL;          /* arena binary key when set */
        size_t auto_klen = 0;
        char *wire_for_record = NULL;
        int is_omit = 0;
        if (auto_key_mode != AK_NONE) {
            if (klen == 0) {
                is_omit = 1;
                auto_klen = (auto_key_mode == AK_UUID) ? 16 : 8;
                auto_id = (char *)arena_alloc(&arena, auto_klen);
                memset(auto_id, 0, auto_klen);
                n_omits++;
            } else {
                /* strdup the wire form for the response — line buffer is
                   mmap-owned and arena dies before response emit. */
                char *wbuf = malloc(klen + 1);
                if (wbuf) { memcpy(wbuf, id_start, klen); wbuf[klen] = '\0'; }
                wire_for_record = wbuf;
                if (auto_key_mode == AK_UUID) {
                    uint8_t bin[16];
                    char tmp[64];
                    if (klen < sizeof(tmp)) {
                        memcpy(tmp, id_start, klen); tmp[klen] = '\0';
                        if (parse_uuid_string(tmp, bin) == 0) {
                            auto_id = (char *)arena_alloc(&arena, 16);
                            memcpy(auto_id, bin, 16);
                            auto_klen = 16;
                        }
                    }
                    if (!auto_id) {
                        if (validation_failed_idx < 0)
                            validation_failed_idx = (int)rec_count;
                        free(wbuf);
                        errors++;
                        continue;
                    }
                } else { /* AK_SEQ */
                    int64_t v;
                    char tmp[32];
                    if (klen < sizeof(tmp)) {
                        memcpy(tmp, id_start, klen); tmp[klen] = '\0';
                        if (parse_seq_key(tmp, &v) == 0) {
                            auto_id = (char *)arena_alloc(&arena, 8);
                            for (int b = 7; b >= 0; b--) {
                                auto_id[b] = (char)(v & 0xFF); v >>= 8;
                            }
                            auto_klen = 8;
                        }
                    }
                    if (!auto_id) {
                        if (validation_failed_idx < 0)
                            validation_failed_idx = (int)rec_count;
                        free(wbuf);
                        errors++;
                        continue;
                    }
                }
            }
        }

        /* Skip oversized keys before any encode work. (ts->total_size >
           sc.max_value was already hoisted above.) Auto-key paths already
           have the correct binary klen at this point. */
        size_t check_klen = auto_id ? auto_klen : klen;
        if ((int)check_klen > sc.max_key) {
            free(wire_for_record);
            errors++; continue;
        }

        /* Walk remaining spans into vals[] without copying. */
        int nvals = 0;
        const char *vp = key_end + 1;
        while (nvals < active_count) {
            const char *v_start = vp;
            while (vp < line_end && *vp != delimiter) vp++;
            vals[nvals].ptr = v_start;
            vals[nvals].len = vp - v_start;
            nvals++;
            if (vp < line_end) vp++;  /* skip delimiter */
            else if (nvals < active_count) {
                /* Line has fewer values than schema expects — pad the
                   remainder with empty spans (legacy behaviour). */
                while (nvals < active_count) {
                    vals[nvals].ptr = line_end;
                    vals[nvals].len = 0;
                    nvals++;
                }
            }
        }

        /* Arena-allocated key + typed payload — survive until arena_free()
           post-Phase-2. Encode directly into the arena payload (zero-init
           first, then encode_field_len writes each field in place); skips
           the typed_tmp bounce + memcpy that used to sit between them.
           For auto-key paths the binary key is already in `auto_id` /
           `auto_klen`; for AK_NONE we copy the wire-form key into arena. */
        char *id;
        if (auto_id) { id = auto_id; klen = auto_klen; }
        else         { id = arena_strndup(&arena, id_start, klen); }
        uint8_t *payload = arena_alloc(&arena, ts->total_size);
        memset(payload, 0, ts->total_size);

        int row_overflow = 0;
        if (!has_tombstones) {
            for (int i = 0; i < active_count && i < nvals; i++) {
                if (vals[i].len == 0) continue;
                if (ts->fields[i].type == FT_VARCHAR &&
                    (int)vals[i].len > ts->fields[i].size - 2) { row_overflow = 1; break; }
                encode_field_len(&ts->fields[i], vals[i].ptr, vals[i].len,
                                 payload + ts->fields[i].offset);
            }
        } else {
            for (int i = 0; i < active_count && i < nvals; i++) {
                int fi = active_indices[i];
                if (vals[i].len == 0) continue;
                if (ts->fields[fi].type == FT_VARCHAR &&
                    (int)vals[i].len > ts->fields[fi].size - 2) { row_overflow = 1; break; }
                encode_field_len(&ts->fields[fi], vals[i].ptr, vals[i].len,
                                 payload + ts->fields[fi].offset);
            }
        }
        if (row_overflow) {
            /* Skip this row rather than storing a truncated value. Count it
               like every other per-row reject in this loop so the client's
               errors/skipped tally reflects the drop. */
            free(wire_for_record);
            errors++;
            continue;
        }

        if (rec_count >= rec_cap) {
            rec_cap *= 2;
            BulkInsRecord *t = xrealloc_or_free(records, rec_cap * sizeof(*t));
            if (!t) {
                /* xrealloc_or_free already freed records; reset rec_count
                   so phase 1.5 below sees an empty set and skips the
                   shard-bucket loop instead of dereferencing NULL records. */
                records = NULL;
                rec_count = 0;
                free(wire_for_record);
                break;
            }
            records = t;
        }
        /* Grow wire_keys[] in lockstep with records[] when auto_key is
           active. wire_keys[i] is NULL for omit records — filled in by
           the post-parse batch generation. */
        if (auto_key_mode != AK_NONE) {
            if (rec_count >= wire_cap) {
                size_t new_wc = wire_cap ? wire_cap * 2 : 1024;
                if (new_wc < rec_cap) new_wc = rec_cap;
                char **t2 = realloc(wire_keys, new_wc * sizeof(char *));
                if (t2) { wire_keys = t2; wire_cap = new_wc; }
            }
            if (wire_keys && rec_count < wire_cap) {
                wire_keys[rec_count] = wire_for_record;  /* heap-owned, freed at response */
            }
        } else {
            (void)is_omit;
            free(wire_for_record);  /* AK_NONE: shouldn't be set, defensive */
        }
        BulkInsRecord *r = &records[rec_count++];
        r->id = id;
        r->payload = payload;
        r->klen = klen;
        compute_hash_raw(id, klen, r->hash);
        r->shard_id = compute_record_shard(r->hash, sc.splits);
        r->start_slot = 0;
        /* v2 dispatcher-shard alignment — see cmd_bulk_insert (JSON path) */
                    r->shard_id = compute_record_shard(r->hash, sc.splits);
    }

    /* Validation failure: refuse the whole batch before any write.
       Matches the JSON path. */
    if (validation_failed_idx >= 0) {
        if (wire_keys) {
            for (size_t i = 0; i < rec_count; i++) free(wire_keys[i]);
            free(wire_keys);
        }
        free(records);
        arena_free(arena);
        for (int i = 0; i < nidx; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        OUT("{\"error\":\"bulk-insert-delimited validation failed at record %d: malformed key for auto_key mode\"}\n",
            validation_failed_idx);
        return 1;
    }

    /* Post-parse: batch-generate keys for omit records. Single
       fill_random call for UUID, single seq flock for seq. */
    if (auto_key_mode != AK_NONE && n_omits > 0) {
        uint8_t *uuid_pool = NULL;
        long long seq_start = 0;
        int keygen_failed = 0;
        if (auto_key_mode == AK_UUID) {
            uuid_pool = malloc((size_t)n_omits * 16);
            if (!uuid_pool || gen_uuid4_batch(uuid_pool, n_omits) != 0)
                keygen_failed = 1;
        } else {
            seq_start = seq_next_val_batch(db_root, object, sc.auto_key_seq_name, n_omits);
            if (seq_start < 0) keygen_failed = 1;
        }
        if (keygen_failed) {
            free(uuid_pool);
            if (wire_keys) {
                for (size_t i = 0; i < rec_count; i++) free(wire_keys[i]);
                free(wire_keys);
            }
            free(records);
            arena_free(arena);
            for (int i = 0; i < nidx; i++) free(idx_pairs[i]);
            free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
            OUT("{\"error\":\"bulk-insert key generation failed: %s\"}\n",
                auto_key_mode == AK_UUID ? "random source unavailable"
                                         : "sequence unavailable");
            return 1;
        }
        int omit_idx = 0;
        for (size_t i = 0; i < rec_count; i++) {
            if (wire_keys && wire_keys[i]) continue;  /* provided key */
            BulkInsRecord *r = &records[i];
            if (auto_key_mode == AK_UUID) {
                memcpy(r->id, uuid_pool + omit_idx * 16, 16);
                char wbuf[37];
                format_uuid_string((const uint8_t *)r->id, wbuf);
                if (wire_keys) wire_keys[i] = strdup(wbuf);
            } else {
                int64_t v = seq_start + omit_idx;
                for (int b = 7; b >= 0; b--) { r->id[b] = (char)(v & 0xFF); v >>= 8; }
                char wbuf[24];
                snprintf(wbuf, sizeof(wbuf), "%lld", (long long)(seq_start + omit_idx));
                if (wire_keys) wire_keys[i] = strdup(wbuf);
            }
            /* Recompute hash + shard now that the placeholder id is final. */
            compute_hash_raw(r->id, r->klen, r->hash);
            r->shard_id = compute_record_shard(r->hash, sc.splits);
            r->start_slot = 0;
                            r->shard_id = compute_record_shard(r->hash, sc.splits);
            omit_idx++;
        }
        free(uuid_pool);
    }

    /* If parse tripped the deadline, abort before any write phase. */
    if (dl.timed_out) {
        free(records);
        arena_free(arena);
        for (int i = 0; i < nidx; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        /* data owned by caller — see bulk_ins_delim_run / bulk_upd_delim_run */
        OUT("{\"error\":\"query_timeout\"}\n");
        return 1;
    }

    /* ===== Phase 1.5: bucket by shard — identical to the JSON path.
       OOM at any of the three allocs frees every prior allocation in
       reverse order before bailing. */
    int *shard_counts = calloc(sc.splits, sizeof(int));
    if (!shard_counts) {
        free(records);
        arena_free(arena);
        for (int i = 0; i < nidx; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        /* data owned by caller — see bulk_ins_delim_run / bulk_upd_delim_run */
        OUT("{\"error\":\"oom: bulk_insert shard_counts\"}\n");
        return 1;
    }
    for (size_t i = 0; i < rec_count; i++) shard_counts[records[i].shard_id]++;

    /* ===== Phase 1.6: pre-grow kf shards once, sized to the incoming batch.
       See cmd_bulk_insert (JSON path) for the rationale. */
    {
        SlotcaskSchemaInfo info = {
            .splits = sc.splits, .slot_size = sc.slot_size,
            .streams = sc.streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
        if (sdb) (void)slotcask_pregrow_kf(sdb, rec_count);
    }

    int *worker_shards = NULL, *shard_to_worker = NULL;
    int nshard_groups = build_shard_worker_map(shard_counts, sc.splits,
                                                &worker_shards, &shard_to_worker);
    BulkInsShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkInsShardWork)) : NULL;
    if (nshard_groups < 0 || (nshard_groups > 0 && !workers)) {
        free(workers); free(worker_shards); free(shard_to_worker);
        free(shard_counts); free(records);
        arena_free(arena);
        for (int i = 0; i < nidx; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        /* data owned by caller — see bulk_ins_delim_run / bulk_upd_delim_run */
        OUT("{\"error\":\"oom: bulk_insert workers\"}\n");
        return 1;
    }
    for (int g = 0; g < nshard_groups; g++) {
        int s = worker_shards[g];
        workers[g].shard_id = s;
        workers[g].records = calloc(shard_counts[s], sizeof(BulkInsRecord));
        workers[g].count = 0;
    }
    /* Same invariant as cmd_bulk_insert (JSON path): rec_count > 0 ⇒
       nshard_groups > 0 ⇒ workers non-NULL post-OOM-guard. */
    for (size_t i = 0; i < rec_count; i++) {
        int w = shard_to_worker[records[i].shard_id];
        /* coverity[forward_null] workers non-NULL when rec_count > 0 */
        workers[w].records[workers[w].count++] = records[i];
    }
    free(records);
    free(shard_counts);
    free(worker_shards);
    free(shard_to_worker);

    for (int wi = 0; wi < nshard_groups; wi++) {
        BulkInsShardWork *ws = &workers[wi];
        ws->db_root = db_root;
        ws->object = object;
        ws->sch = &sc;
        ws->ts = ts;
        ws->nidx = nidx;
        ws->idx_fields = (const char (*)[256])idx_fields;
        ws->idx_field_indices = (const int (*)[16])idx_field_indices;
        ws->idx_field_counts = idx_field_counts;
        ws->idx_is_composite = idx_is_composite;
        ws->idx_types = idx_types;
        ws->inserted = 0;
        ws->errors = 0;
        ws->skipped = 0;
        ws->if_not_exists = if_not_exists;
        ws->idx_pairs = nidx > 0 ? calloc(nidx, sizeof(BtEntry *)) : NULL;
        ws->idx_pair_counts = nidx > 0 ? calloc(nidx, sizeof(size_t)) : NULL;
        ws->idx_pair_caps = nidx > 0 ? calloc(nidx, sizeof(size_t)) : NULL;
    }

    uint64_t t1 = now_ms_coarse();  /* end of Phase 1 (parse + bucket) */

    /* ===== Phase 2: parallel shard workers via shared pool.
       All concurrent callers share one pool sized to ~4× cores by default;
       oversubscription hides shard-rwlock stalls. */
    parallel_for_io(bulk_insert_shard_worker, workers, nshard_groups,
                 sizeof(BulkInsShardWork));
    uint64_t t2 = now_ms_coarse();  /* end of Phase 2 (parallel shard write) */

    /* Phase-2 breakdown across workers (mirrors cmd_bulk_insert JSON path). */
    uint64_t grow_ms_total = 0, wall_ms_max = 0;
    int grow_count_total = 0;
    for (int wi = 0; wi < nshard_groups; wi++) {
        grow_ms_total    += workers[wi].grow_ms;
        grow_count_total += workers[wi].grow_count;
        if (workers[wi].wall_ms > wall_ms_max) wall_ms_max = workers[wi].wall_ms;
    }

    /* Merge per-worker results into caller's counters + index arrays. */
    int delim_skipped_total = 0;
    for (int wi = 0; wi < nshard_groups; wi++) {
        BulkInsShardWork *ws = &workers[wi];
        count  += ws->inserted;
        errors += ws->errors;
        delim_skipped_total += ws->skipped;

        for (int fi = 0; fi < nidx; fi++) {
            size_t add = ws->idx_pair_counts[fi];
            if (add == 0) { free(ws->idx_pairs[fi]); continue; }
            if (idx_pair_counts[fi] + add > idx_pair_caps[fi]) {
                size_t new_cap = idx_pair_caps[fi];
                while (idx_pair_counts[fi] + add > new_cap) new_cap *= 2;
                BtEntry *t = xrealloc_or_free(idx_pairs[fi], new_cap * sizeof(BtEntry));
                if (!t) {
                    LOG_ERROR(LOG_SUB_INDEX, "INDEX_OOM merge field_idx=%d (dropped %zu pairs; rerun reindex)",
                            fi, idx_pair_counts[fi] + add);
                    idx_pairs[fi] = NULL;
                    idx_pair_counts[fi] = 0;
                    idx_pair_caps[fi] = 0;
                    for (size_t k = 0; k < add; k++) free((void *)ws->idx_pairs[fi][k].value);
                    free(ws->idx_pairs[fi]);
                    continue;
                }
                idx_pairs[fi] = t;
                idx_pair_caps[fi] = new_cap;
            }
            memcpy(idx_pairs[fi] + idx_pair_counts[fi],
                   ws->idx_pairs[fi], add * sizeof(BtEntry));
            idx_pair_counts[fi] += add;
            free(ws->idx_pairs[fi]);
        }

        /* records[ri].id / .payload live in the arena — freed in bulk below */
        free(ws->records);
        free(ws->idx_pairs);
        free(ws->idx_pair_counts);
        free(ws->idx_pair_caps);
    }
    free(workers);
    arena_free(arena);

    /* Activate — parallel across shards for THIS object. v2 commits per
       record via keyfile flip, no batched activation needed. */
    uint64_t t3 = now_ms_coarse();  /* end of Phase 3 (activate) */

    /* data owned by caller — see bulk_ins_delim_run / bulk_upd_delim_run */

    /* Parallel index builds — one worker per field; the worker streams the
       per-shard merges sequentially. See cmd_bulk_insert (JSON path) above
       for why we no longer fan out per (field, shard). */
    if (nidx > 0) {
        IdxFieldArg *fa = malloc((size_t)nidx * sizeof(IdxFieldArg));
        int fa_count = 0;
        for (int fi = 0; fi < nidx; fi++) {
            if (idx_pair_counts[fi] == 0) continue;
            fa[fa_count++] = (IdxFieldArg){
                .db_root = db_root, .object = object, .field = idx_fields[fi],
                .splits = sc.splits,
                .new_entries = idx_pairs[fi], .new_count = idx_pair_counts[fi],
            };
        }
        parallel_for(idx_build_field_worker, fa, fa_count, sizeof(IdxFieldArg));
        free(fa);
        for (int fi = 0; fi < nidx; fi++) {
            for (size_t ei = 0; ei < idx_pair_counts[fi]; ei++)
                free((char *)idx_pairs[fi][ei].value);
            free(idx_pairs[fi]);
        }
    }
    free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);

    if (count > 0) update_count(db_root, object, count);

    uint64_t t4 = now_ms_coarse();  /* end of Phase 4 (index build) */
    LOG_INFO(LOG_SUB_QUERY, "BULK-INSERT %s: rows=%d phase1_parse=%lums phase2_write=%lums (grows=%d grow_total=%lums per_worker_max=%lums) phase3_activate=%lums phase4_index=%lums total=%lums",
            object, count,
            (unsigned long)(t1 - t0),
            (unsigned long)(t2 - t1),
            grow_count_total,
            (unsigned long)grow_ms_total,
            (unsigned long)wall_ms_max,
            (unsigned long)(t3 - t2),
            (unsigned long)(t4 - t3),
            (unsigned long)(t4 - t0));

    /* Auto-key response carries the resolved wire-form keys[] in input
       order. AK_NONE keeps today's plain shape. */
    if (auto_key_mode != AK_NONE && wire_keys) {
        OUT("{\"status\":\"bulk-inserted\",\"count\":%d,\"skipped\":%d,\"keys\":[",
            count, delim_skipped_total);
        for (size_t i = 0; i < rec_count; i++) {
            OUT("%s\"%s\"", i ? "," : "", wire_keys[i] ? wire_keys[i] : "");
        }
        OUT("]}\n");
        for (size_t i = 0; i < rec_count; i++) free(wire_keys[i]);
        free(wire_keys);
        wire_keys = NULL;
    } else if (errors) {
        OUT("{\"inserted\":%d,\"skipped\":%d,\"errors\":%d,\"error\":\"some_records_dropped\"}\n",
            count, delim_skipped_total, errors);
        fprintf(stderr, "%d errors during delimited import (see info log for dropped keys)\n", errors);
    } else if (delim_skipped_total > 0) {
        OUT("{\"inserted\":%d,\"skipped\":%d}\n", count, delim_skipped_total);
    } else {
        OUT("{\"inserted\":%d}\n", count);
    }
    free(wire_keys);  /* no-op if NULL */
    return errors > 0 ? 1 : 0;
}

int cmd_bulk_insert_delimited(const char *db_root, const char *object,
                               const char *filepath, char delimiter,
                               int if_not_exists) {
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

    int rc = bulk_ins_delim_run(db_root, object, data, (size_t)st.st_size,
                                 delimiter, if_not_exists);

    if (data_mmaped) munmap((void *)data, st.st_size);
    else free((void *)data);
    return rc;
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
    const enum IndexType *idx_types;  /* [nidx] — NULL = legacy all-btree */
    int nidx;
    TypedSchema *ts;
    /* Results */
    int deleted;
    /* Collected index deletions: [nidx][key_count] — parallel (val, vlen).
       val is malloc'd index-key bytes (or NULL). */
    uint8_t ***idx_vals;
    size_t  **idx_lens;
} BulkDelShardWork;

/* === v2 bulk-delete (key-list) worker ===
 *
 * Per-record slotcask_delete_with_hooks; the pre_commit hook drops every
 * indexed-field's old btree entry under the keyfile wrlock. Phase-2 parallel
 * index-cleanup is skipped for v2 — the hook does it inline (lower latency,
 * matches what cmd_delete_v2 does for single deletes). */
typedef struct {
    BulkDelShardWork *sw;
    int               ki;
    /* Shared scratch arena for index-key extraction. nidx slots of
       INDEX_KEY_MAX bytes each, reused across every pre_commit call
       in this shard's batch — the worker fires records serially under
       the kf wrlock so the buffer is safe to reuse. NULL if the
       worker's arena malloc failed; pre_commit then falls back to the
       per-call malloc path. */
    uint8_t          *arena;
    size_t            arena_slot;   /* INDEX_KEY_MAX, repeated for clarity */
} V2BulkDelCtx;

/* Per-record pre_commit, bulk-shaped: ctx via rec->user_ctx. */
static int v2_bulk_del_pre_commit_bulk(const SlotcaskOldRecord *old,
                                        SlotcaskBulkRec *rec) {
    V2BulkDelCtx *ctx = (V2BulkDelCtx *)rec->user_ctx;
    BulkDelShardWork *sw = ctx->sw;
    int ki = ctx->ki;
    if (!old || sw->nidx == 0 || !sw->ts) return 0;

    for (int fi = 0; fi < sw->nidx; fi++) {
        uint8_t *buf = NULL; size_t blen = 0;
        int from_arena = 0;
        if (ctx->arena) {
            uint8_t *slot = ctx->arena + (size_t)fi * ctx->arena_slot;
            int rc = build_index_key_from_record_into(sw->ts, old->value,
                                                       sw->idx_fields[fi],
                                                       slot, ctx->arena_slot, &blen);
            if (rc == 1) { buf = slot; from_arena = 1; }
            else if (rc == 0) continue;     /* missing/empty field */
            /* rc == -1: oversized index key — fall through to malloc. */
        }
        if (!buf) {
            if (!build_index_key_from_record(sw->ts, old->value, sw->idx_fields[fi],
                                              &buf, &blen))
                continue;
        }
        enum IndexType itype = sw->idx_types ? sw->idx_types[fi] : IT_BTREE;
        if (itype == IT_TRIGRAM) {
            UpdateIdxArg a = {0};
            a.db_root = sw->db_root; a.object = sw->object;
            a.field = sw->idx_fields[fi]; a.splits = sw->sch->splits;
            a.new_key = NULL; a.new_len = 0;
            a.old_key = buf;  a.old_len = blen;
            a.hash = sw->hashes[ki]; a.type = IT_TRIGRAM;
            update_idx_fn(&a);
        } else {
            delete_index_entry(sw->db_root, sw->object, sw->idx_fields[fi],
                                sw->sch->splits, buf, blen, sw->hashes[ki]);
        }
        if (!from_arena) free(buf);
    }
    return 0;
}

static void *bulk_del_shard_worker_v2(BulkDelShardWork *sw) {
    SlotcaskSchemaInfo info = {
        .splits = sw->sch->splits, .slot_size = sw->sch->slot_size,
        .streams = sw->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "bulk_del_shard_worker_v2: slotcask_registry_get failed for %s/%s", sw->db_root, sw->object);
        return NULL;
    }

    /* All keys in this worker hash to the same kf shard (dispatcher
       aligned shard_id with compute_record_shard, see cmd_bulk_delete). */
    int kf_shard_id = compute_record_shard(sw->hashes[0], sw->sch->splits);

    SlotcaskBulkRec *batch = calloc(sw->key_count, sizeof(SlotcaskBulkRec));
    V2BulkDelCtx    *ctxs  = malloc(sw->key_count * sizeof(V2BulkDelCtx));
    if (!batch || !ctxs) {
        LOG_ERROR(LOG_SUB_QUERY, "bulk_del_shard_worker_v2: alloc failed, dropping %d deletes", sw->key_count);
        free(batch); free(ctxs); return NULL;
    }

    /* One arena allocation for the whole shard's batch. Replaces
       (key_count * nidx) per-field mallocs in the pre_commit hook with
       a single malloc/free pair shared across every record. The arena
       gets reused for each record under the kf wrlock — pre_commits
       fire serially inside slotcask_bulk_delete_in_kfshard. */
    enum { INDEX_KEY_MAX = 4096 };
    uint8_t *arena = NULL;
    if (sw->nidx > 0) {
        arena = malloc((size_t)sw->nidx * INDEX_KEY_MAX);
    }

    for (int ki = 0; ki < sw->key_count; ki++) {
        ctxs[ki].sw         = sw;
        ctxs[ki].ki         = ki;
        ctxs[ki].arena      = arena;
        ctxs[ki].arena_slot = INDEX_KEY_MAX;
        batch[ki].key       = sw->keys[ki];
        batch[ki].klen      = strlen(sw->keys[ki]);
        batch[ki].value     = NULL;
        batch[ki].vlen      = 0;
        batch[ki].user_ctx  = &ctxs[ki];
        batch[ki].old_value = NULL;
        batch[ki].old_vlen  = 0;
        batch[ki].status    = 0;
        batch[ki].was_update = 0;
    }

    SlotcaskBulkDeleteOpts opts = {
        .pre_commit           = v2_bulk_del_pre_commit_bulk,
        .pre_commit_needs_old = sw->nidx > 0,
    };
    (void)slotcask_bulk_delete_in_kfshard(sdb, kf_shard_id,
                                           batch, sw->key_count, &opts);

    for (int ki = 0; ki < sw->key_count; ki++) {
        if (batch[ki].status == 0) sw->deleted++;
        /* status=-2 (not found) and status=-1 (error) are not counted. */
    }
    free(arena);
    free(batch); free(ctxs);
    return NULL;
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

    /* Phase 2: Parallel index cleanup — one thread per index. Skipped for
       v2: the per-record pre_commit hook in bulk_del_shard_worker_v2 already
       drops the btree entries under the kf-shard wrlock. */
    int total_deleted = 0;
    for (int g = 0; g < nshard_groups; g++) total_deleted += workers[g].deleted;

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

    OUT("{\"deleted\":%d}\n", total_deleted);
    for (int i = 0; i < key_count; i++) free(keys[i]);
    free(keys); free(hashes); free(shard_ids); free(start_slots); free(raw);
    return 0;
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

    if (!criteria_match_tree(raw, bc->tree, bc->fs)) return 0;

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

/* Per-record state for bulk-update's Phase 2 shard-grouped workers. */
typedef struct {
    const char *key;
    size_t      klen;
    uint8_t     hash[16];
    int         start_slot;
    int         shard_id;
} BulkUpdRec;

typedef struct {
    const char    *db_root;
    const char    *object;
    const Schema  *sch;
    TypedSchema   *ts;
    CriteriaNode  *tree;
    FieldSchema   *fs;
    const char    *value_json;
    const char   (*idx_fields)[256];
    const enum IndexType *idx_types;  /* [nidx] — IT_BTREE / IT_BITMAP / IT_TRIGRAM (NULL = all btree, legacy) */
    int            nidx;
    int            shard_id;
    BulkUpdRec    *recs;
    int            count;
    /* CAS: optional `if` condition re-verified per record under the wrlock.
       NULL/empty = no CAS check. Same SearchCriterion[] shape single-op
       cmd_update uses, which makes cas_check directly applicable. */
    SearchCriterion *cas_crit;
    int              cas_ncrit;
    /* Results */
    int            updated;
    int            skipped;
} BulkUpdShardWork;

/* === v2 worker for criteria-driven bulk-update ===
 *
 * Mirrors v1: per record, re-verify criteria + CAS, apply value_json patches
 * (same patches for every record), refresh auto_update fields, slotcask
 * upsert with require_existing=1 + pre_commit hook for index diff.
 *
 * Re-verification uses the OLD record's typed payload (carried in
 * SlotcaskOldRecord *old) — closes the same race window the v1 worker
 * does (records that no longer match between phase-1 scan and phase-2
 * write count as skipped). */
typedef struct {
    BulkUpdShardWork *w;
    BulkUpdRec       *rec;
    /* Pointer (not owner) to the shared per-worker field_vals array
       parsed once from value_json. */
    char            **field_vals;
    /* Shared arenas for OLD and NEW index-key extraction during
       pre_commit. nidx slots × INDEX_KEY_MAX bytes each, reused
       across every record in the worker's batch — pre_commit fires
       serially inside slotcask_bulk_upsert_in_kfshard. Both old and
       new keys are call-scoped (delete + write fire inline). NULL
       on arena malloc failure → pre_commit falls back to per-field
       malloc for that record. */
    uint8_t          *old_arena;
    uint8_t          *new_arena;
    size_t            arena_slot;
} V2BulkUpdCtx;

/* value_compute: rolls together the per-record steps that the old
   slotcask_upsert_with_hooks call did across .check + the inline
   patching loop:
     1. Re-verify criteria_match_tree on OLD (closes the
        scan→write race window).
     2. Re-verify CAS (`if_json` translates to cas_crit).
     3. Copy OLD into the worker scratch, apply value_json patches,
        apply auto_update fields.
   Returns 0 to write, -1 to skip (criteria/CAS rejection). */
static int v2_bulk_upd_value_compute(const SlotcaskOldRecord *old,
                                       SlotcaskBulkRec *rec) {
    V2BulkUpdCtx *ctx = (V2BulkUpdCtx *)rec->user_ctx;
    BulkUpdShardWork *w = ctx->w;
    if (!old) return -1;
    if (!criteria_match_tree(old->value, w->tree, w->fs)) return -1;
    if (w->cas_crit && w->cas_ncrit > 0 &&
        !cas_check(w->ts, old->value, (int)old->vlen, w->cas_crit, w->cas_ncrit)) return -1;

    uint8_t *new_buf = (uint8_t *)rec->value;
    if (old->vlen >= (size_t)w->ts->total_size) {
        memcpy(new_buf, old->value, w->ts->total_size);
    } else {
        memcpy(new_buf, old->value, old->vlen);
        memset(new_buf + old->vlen, 0, (size_t)w->ts->total_size - old->vlen);
    }
    rec->vlen = (size_t)w->ts->total_size;

    for (int fi = 0; fi < w->ts->nfields; fi++) {
        if (ctx->field_vals[fi] && !w->ts->fields[fi].removed) {
            encode_field(&w->ts->fields[fi], ctx->field_vals[fi],
                          new_buf + w->ts->fields[fi].offset);
        }
    }
    for (int fi = 0; fi < w->ts->nfields; fi++) {
        if (w->ts->fields[fi].removed) continue;
        if (w->ts->fields[fi].default_kind == DK_AUTO_UPDATE) {
            char tbuf[24];
            if (w->ts->fields[fi].type == FT_TIMESTAMP) {
                struct timespec tsn;
                clock_gettime(CLOCK_REALTIME, &tsn);
                long long ms = (long long)tsn.tv_sec * 1000LL + tsn.tv_nsec / 1000000LL;
                snprintf(tbuf, sizeof(tbuf), "%lld", ms);
            } else if (w->ts->fields[fi].type == FT_DATETIMEMS) {
                struct timespec tsn;
                clock_gettime(CLOCK_REALTIME, &tsn);
                time_t nowsec = tsn.tv_sec;
                struct tm tm; localtime_r(&nowsec, &tm);
                int msec = (int)(tsn.tv_nsec / 1000000L);
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d%03d",
                          tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                          tm.tm_hour, tm.tm_min, tm.tm_sec, msec);
            } else {
                time_t now = time(NULL);
                struct tm tm; localtime_r(&now, &tm);
                if (w->ts->fields[fi].type == FT_DATE)
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                              tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
                else
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                              tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                              tm.tm_hour, tm.tm_min, tm.tm_sec);
            }
            encode_field(&w->ts->fields[fi], tbuf,
                          new_buf + w->ts->fields[fi].offset);
        }
    }
    return 0;
}

static int v2_bulk_upd_pre_commit_bulk(const SlotcaskOldRecord *old,
                                        SlotcaskBulkRec *rec,
                                        int is_update) {
    (void)is_update;
    V2BulkUpdCtx *ctx = (V2BulkUpdCtx *)rec->user_ctx;
    BulkUpdShardWork *w = ctx->w;
    BulkUpdRec       *upd_rec = ctx->rec;
    if (w->nidx == 0 || !old) return 0;

    const uint8_t *new_value = (const uint8_t *)rec->value;

    /* Diff fields against the shared arenas; queue only changed fields
       into args[] for parallel dispatch via update_idx_fn. Both old and
       new keys are call-scoped (delete + write fire inline inside
       update_idx_fn), so arena reuse is safe. fb_bufs tracks malloc'd
       fallback buffers for keys too large to fit in an arena slot. */
    UpdateIdxArg args[MAX_FIELDS];
    uint8_t *fb_bufs[2 * MAX_FIELDS]; int n_fb = 0;
    int n_args = 0;

    for (int fi = 0; fi < w->nidx; fi++) {
        uint8_t *old_buf = NULL, *new_buf = NULL;
        size_t   old_len = 0,   new_len = 0;
        int have_old = 0, have_new = 0;

        if (ctx->old_arena) {
            uint8_t *slot = ctx->old_arena + (size_t)fi * ctx->arena_slot;
            int rc = build_index_key_from_record_into(w->ts, old->value,
                                                       w->idx_fields[fi],
                                                       slot, ctx->arena_slot, &old_len);
            if (rc == 1) { old_buf = slot; have_old = 1; }
            else if (rc == -1) {
                have_old = build_index_key_from_record(w->ts, old->value,
                                                       w->idx_fields[fi],
                                                       &old_buf, &old_len);
                if (have_old) fb_bufs[n_fb++] = old_buf;
            }
        } else {
            have_old = build_index_key_from_record(w->ts, old->value,
                                                   w->idx_fields[fi],
                                                   &old_buf, &old_len);
            if (have_old) fb_bufs[n_fb++] = old_buf;
        }

        if (ctx->new_arena) {
            uint8_t *slot = ctx->new_arena + (size_t)fi * ctx->arena_slot;
            int rc = build_index_key_from_record_into(w->ts, new_value,
                                                       w->idx_fields[fi],
                                                       slot, ctx->arena_slot, &new_len);
            if (rc == 1) { new_buf = slot; have_new = 1; }
            else if (rc == -1) {
                have_new = build_index_key_from_record(w->ts, new_value,
                                                       w->idx_fields[fi],
                                                       &new_buf, &new_len);
                if (have_new) fb_bufs[n_fb++] = new_buf;
            }
        } else {
            have_new = build_index_key_from_record(w->ts, new_value,
                                                   w->idx_fields[fi],
                                                   &new_buf, &new_len);
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
            args[n_args].db_root = w->db_root;
            args[n_args].object  = w->object;
            args[n_args].field   = w->idx_fields[fi];
            args[n_args].splits  = w->sch->splits;
            args[n_args].new_key = have_new ? new_buf : NULL;
            args[n_args].new_len = new_len;
            args[n_args].old_key = have_old ? old_buf : NULL;
            args[n_args].old_len = old_len;
            args[n_args].hash    = upd_rec->hash;
            args[n_args].type    = w->idx_types ? w->idx_types[fi] : IT_BTREE;
            n_args++;
        }
    }

    if (n_args > 0) parallel_for(update_idx_fn, args, n_args, sizeof(UpdateIdxArg));
    for (int i = 0; i < n_fb; i++) free(fb_bufs[i]);
    return 0;
}

static void *bulk_upd_shard_worker_v2(BulkUpdShardWork *w) {
    SlotcaskSchemaInfo info = {
        .splits = w->sch->splits, .slot_size = w->sch->slot_size,
        .streams = w->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(w->db_root, w->object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "bulk_upd_shard_worker_v2: slotcask_registry_get failed for %s/%s", w->db_root, w->object);
        w->skipped += w->count; return NULL;
    }

    /* Parse value_json once for the whole worker. */
    const char *field_names[MAX_FIELDS];
    char       *field_vals[MAX_FIELDS];
    for (int fi = 0; fi < w->ts->nfields; fi++) field_names[fi] = w->ts->fields[fi].name;
    json_get_fields(w->value_json, field_names, w->ts->nfields, field_vals);

    SlotcaskBulkRec *batch   = calloc(w->count, sizeof(SlotcaskBulkRec));
    V2BulkUpdCtx    *ctxs    = malloc(w->count * sizeof(V2BulkUpdCtx));
    uint8_t         *scratch = malloc((size_t)w->count * (size_t)w->ts->total_size);
    if (!batch || !ctxs || !scratch) {
        LOG_ERROR(LOG_SUB_QUERY, "bulk_upd_shard_worker_v2: alloc failed, skipping %d updates", w->count);
        free(batch); free(ctxs); free(scratch);
        for (int fi = 0; fi < w->ts->nfields; fi++) free(field_vals[fi]);
        w->skipped += w->count;
        return NULL;
    }
    /* Two per-worker arenas (OLD + NEW index keys) shared across every
       record in the batch. Replaces (count × nidx × 2) per-field mallocs
       in v2_bulk_upd_pre_commit_bulk with two upfront malloc/free pairs. */
    enum { INDEX_KEY_MAX = 4096 };
    uint8_t *old_arena = (w->nidx > 0) ? malloc((size_t)w->nidx * INDEX_KEY_MAX) : NULL;
    uint8_t *new_arena = (w->nidx > 0) ? malloc((size_t)w->nidx * INDEX_KEY_MAX) : NULL;

    for (int ki = 0; ki < w->count; ki++) {
        BulkUpdRec *rec = &w->recs[ki];
        ctxs[ki].w          = w;
        ctxs[ki].rec        = rec;
        ctxs[ki].field_vals = field_vals;
        ctxs[ki].old_arena  = old_arena;
        ctxs[ki].new_arena  = new_arena;
        ctxs[ki].arena_slot = INDEX_KEY_MAX;
        batch[ki].key       = rec->key;
        batch[ki].klen      = rec->klen;
        batch[ki].value     = scratch + (size_t)ki * (size_t)w->ts->total_size;
        batch[ki].vlen      = (size_t)w->ts->total_size;
        batch[ki].user_ctx  = &ctxs[ki];
        batch[ki].old_value = NULL;
        batch[ki].old_vlen  = 0;
        batch[ki].status    = 0;
        batch[ki].was_update = 0;
    }

    SlotcaskBulkOpts opts = {
        .require_existing     = 1,
        .pre_commit           = v2_bulk_upd_pre_commit_bulk,
        .pre_commit_needs_old = w->nidx > 0,
        .value_compute        = v2_bulk_upd_value_compute,
    };
    (void)slotcask_bulk_upsert_in_kfshard(sdb, w->shard_id,
                                           batch, (size_t)w->count, &opts);

    for (int ki = 0; ki < w->count; ki++) {
        if (batch[ki].status == 0) w->updated++;
        else w->skipped++;
    }

    free(batch); free(ctxs); free(scratch);
    free(old_arena); free(new_arena);
    for (int fi = 0; fi < w->ts->nfields; fi++) free(field_vals[fi]);
    return NULL;
}

/* Bulk-update phase 2 worker — one per shard, holds the ucache wrlock once
   for the whole bucket. Index updates (btree_insert/btree_delete) are
   serialised by bt_cache_lock inside the btree layer, so concurrent workers
   hitting the same index file are safe. */
static void *bulk_upd_shard_worker(void *arg) {
    BulkUpdShardWork *w = (BulkUpdShardWork *)arg;
    if (w->count == 0) return NULL;
    return bulk_upd_shard_worker_v2(w);
}

int cmd_bulk_update(const char *db_root, const char *object,
                    const char *criteria_json, const char *value_json,
                    const char *if_json, int limit, int dry_run) {
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

    /* Parse optional `if` once into the SearchCriterion[] shape that
       cas_check expects. Workers share the parsed array read-only. */
    SearchCriterion *cas_crit = NULL;
    int cas_ncrit = 0;
    if (if_json && if_json[0] &&
        parse_criteria_json(if_json, &cas_crit, &cas_ncrit) != 0) {
        OUT("{\"error\":\"invalid if condition\"}\n");
        free_criteria_tree(tree);
        return 1;
    }

    /* Phase 1: Scan — collect matching keys (read-only) */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    compile_criteria_tree(tree, fs.ts);
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    BulkCriteriaCtx ctx = { tree, &fs, NULL, 0, 0, limit, &dl, 0, 0, 0,
                            PTHREAD_MUTEX_INITIALIZER };
    scan_dispatch(db_root, object, &sch, data_dir, bulk_criteria_scan_cb, &ctx);
    pthread_mutex_destroy(&ctx.lock);
    int matched = ctx.count;

    if (dl.timed_out) {
        LOG_WARN(LOG_SUB_QUERY, "bulk-update: query deadline exceeded while matching criteria");
        OUT("{\"error\":\"query_timeout\"}\n");
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }
    if (ctx.budget_exceeded) {
        LOG_WARN(LOG_SUB_QUERY, "bulk-update: query buffer cap exceeded while matching criteria");
        OUT(QUERY_BUFFER_ERR);
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }

    if (dry_run) {
        OUT("{\"matched\":%d,\"updated\":0,\"skipped\":0,\"dry_run\":true}\n", matched);
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 0;
    }

    /* Phase 2: Write — bucket matched keys by shard and fan out one worker
       per shard. Each worker takes the ucache wrlock **once** per shard,
       walks its bucket end-to-end, and releases **once** — matching the
       bulk-insert pattern. Index updates (btree_insert/btree_delete) are
       serialised internally by bt_cache_lock, so concurrent workers are
       safe. */
    TypedSchema *ts = load_typed_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */
    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);
    int updated = 0, skipped = 0;

    /* Pre-compute each matched key's hash + shard placement so bucketing
       is a single pass over ctx.keys[]. */
    BulkUpdRec *all = matched > 0 ? malloc(matched * sizeof(BulkUpdRec)) : NULL;
    for (int i = 0; i < matched; i++) {
        all[i].key = ctx.keys[i];
        all[i].klen = strlen(ctx.keys[i]);
        compute_hash_raw(all[i].key, all[i].klen, all[i].hash);
        all[i].shard_id = compute_record_shard(all[i].hash, sch.splits);
        all[i].start_slot = 0;
    }

    /* Bucket by shard_id. */
    int *shard_counts = calloc(sch.splits, sizeof(int));
    for (int i = 0; i < matched; i++) shard_counts[all[i].shard_id]++;
    int *worker_shards = NULL, *shard_to_worker = NULL;
    int nshard_groups = build_shard_worker_map(shard_counts, sch.splits,
                                                &worker_shards, &shard_to_worker);

    BulkUpdShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkUpdShardWork)) : NULL;
    for (int g = 0; g < nshard_groups; g++) {
        int s = worker_shards[g];
        workers[g].shard_id = s;
        workers[g].recs = malloc(shard_counts[s] * sizeof(BulkUpdRec));
        workers[g].count = 0;
    }
    for (int i = 0; i < matched; i++) {
        int w = shard_to_worker[all[i].shard_id];
        workers[w].recs[workers[w].count++] = all[i];
    }
    free(shard_counts);
    free(worker_shards);
    free(shard_to_worker);
    free(all);

    for (int wi = 0; wi < nshard_groups; wi++) {
        workers[wi].db_root = db_root;
        workers[wi].object = object;
        workers[wi].sch = &sch;
        workers[wi].ts = ts;
        workers[wi].tree = tree;
        workers[wi].fs = &fs;
        workers[wi].value_json = value_json;
        workers[wi].idx_fields = (const char (*)[256])idx_fields;
        workers[wi].idx_types = idx_types;
        workers[wi].nidx = nidx;
        workers[wi].cas_crit = cas_crit;
        workers[wi].cas_ncrit = cas_ncrit;
        workers[wi].updated = 0;
        workers[wi].skipped = 0;
    }

    parallel_for_io(bulk_upd_shard_worker, workers, nshard_groups, sizeof(BulkUpdShardWork));

    for (int wi = 0; wi < nshard_groups; wi++) {
        updated += workers[wi].updated;
        skipped += workers[wi].skipped;
        free(workers[wi].recs);
    }
    free(workers);

    LOG_INFO(LOG_SUB_QUERY, "BULK-UPDATE %s matched=%d updated=%d skipped=%d", object, matched, updated, skipped);
    OUT("{\"matched\":%d,\"updated\":%d,\"skipped\":%d}\n", matched, updated, skipped);

    if (cas_crit) free_criteria(cas_crit, cas_ncrit);
    for (int i = 0; i < matched; i++) free(ctx.keys[i]);
    free(ctx.keys); free_criteria_tree(tree);
    return 0;
}

/* ========== BULK UPDATE (DELIMITED TEXT FILE) ========== */
/* Per-key partial update. Row shape: key<DELIM>v1<DELIM>v2<DELIM>... in
   fields.conf active-field order (same as bulk-insert-delimited). Semantics
   per 9a spec: update-only (key must exist — missing keys counted as
   skipped); blank cell = leave that field alone; non-blank cell overwrites.
   NOT an upsert. Phase 1 parses rows into records[] with line-span pointers
   into the mmap'd CSV; Phase 2 runs shard-grouped parallel workers that
   probe, patch, and update affected indexes under a single wrlock per
   shard. Indexes are updated only when their value actually changed. */
typedef struct {
    char       *key;           /* owned null-terminated */
    size_t      klen;
    uint8_t     hash[16];
    int         start_slot;
    int         shard_id;
    const char *line_end;      /* points into mmap'd CSV; valid until munmap */
    const char *body_start;    /* span after key_end + delimiter */
} BulkUpdDelimRec;

typedef struct {
    const char       *db_root;
    const char       *object;
    const Schema     *sch;
    TypedSchema      *ts;
    const char      (*idx_fields)[256];
    const enum IndexType *idx_types;  /* [nidx] — IT_BTREE / IT_BITMAP / IT_TRIGRAM (NULL = legacy all-btree) */
    int               nidx;
    const int        *active_indices;
    int               active_count;
    int               has_tombstones;
    char              delimiter;
    int               shard_id;
    BulkUpdDelimRec  *recs;
    int               count;
    /* Results */
    int               updated;
    int               skipped;
} BulkUpdDelimShardWork;

/* === v2 bulk-update-delimited worker ===
 *
 * Same shape as bulk_upd_json_shard_worker_v2: read existing, patch active
 * fields from the CSV row span, run auto_update fields, then upsert via
 * slotcask_upsert_with_hooks(require_existing=1). The pre_commit hook
 * (v2_bulk_upd_delim_pre_commit) handles the per-field index drop/insert
 * diff, mirroring the v1 worker block. */
typedef struct {
    BulkUpdDelimShardWork *w;
    BulkUpdDelimRec       *rec;
} V2BulkUpdDelimCtx;

/* Bulk-shaped pre_commit hook. ctx via rec->user_ctx; new value via
   rec->value (already populated by value_compute). */
static int v2_bulk_upd_delim_pre_commit_bulk(const SlotcaskOldRecord *old,
                                              SlotcaskBulkRec *rec,
                                              int is_update) {
    (void)is_update;
    V2BulkUpdDelimCtx *ctx = (V2BulkUpdDelimCtx *)rec->user_ctx;
    BulkUpdDelimShardWork *w = ctx->w;
    BulkUpdDelimRec       *delim_rec = ctx->rec;
    if (w->nidx == 0 || !old) return 0;

    const uint8_t *new_value = (const uint8_t *)rec->value;
    for (int fi = 0; fi < w->nidx; fi++) {
        uint8_t *old_buf = NULL, *new_buf = NULL;
        size_t   old_len = 0,   new_len = 0;
        int have_old = build_index_key_from_record(w->ts, old->value,
                                                    w->idx_fields[fi],
                                                    &old_buf, &old_len);
        int have_new = build_index_key_from_record(w->ts, new_value,
                                                    w->idx_fields[fi],
                                                    &new_buf, &new_len);
        int changed = 0;
        if (have_new && !have_old) changed = 1;
        else if (!have_new && have_old) changed = 1;
        else if (have_new && have_old) {
            if (new_len != old_len ||
                memcmp(new_buf, old_buf, new_len) != 0) changed = 1;
        }
        if (changed) {
            enum IndexType itype = w->idx_types ? w->idx_types[fi] : IT_BTREE;
            if (itype == IT_TRIGRAM) {
                /* Trigram needs the diff (new ∪ old → both sets) — route
                   through update_idx_fn so the IT_TRIGRAM branch handles
                   per-trigram insert/delete in one shot. */
                UpdateIdxArg a = {0};
                a.db_root = w->db_root; a.object = w->object;
                a.field = w->idx_fields[fi]; a.splits = w->sch->splits;
                a.new_key = have_new ? new_buf : NULL;
                a.new_len = new_len;
                a.old_key = have_old ? old_buf : NULL;
                a.old_len = old_len;
                a.hash = delim_rec->hash;
                a.type = IT_TRIGRAM;
                update_idx_fn(&a);
            } else {
                if (have_old)
                    delete_index_entry(w->db_root, w->object, w->idx_fields[fi],
                                        w->sch->splits, old_buf, old_len, delim_rec->hash);
                if (have_new)
                    write_index_entry(w->db_root, w->object, w->idx_fields[fi],
                                       w->sch->splits, new_buf, new_len, delim_rec->hash);
            }
        }
        free(old_buf); free(new_buf);
    }
    return 0;
}

/* value_compute hook: derive NEW from OLD by copying old into the worker-
   allocated scratch slot, then applying CSV cells + auto_update. The
   scratch slot location is rec->value (pre-pointed by the worker). */
static int v2_bulk_upd_delim_value_compute(const SlotcaskOldRecord *old,
                                            SlotcaskBulkRec *rec) {
    V2BulkUpdDelimCtx *ctx = (V2BulkUpdDelimCtx *)rec->user_ctx;
    BulkUpdDelimShardWork *w = ctx->w;
    BulkUpdDelimRec       *delim_rec = ctx->rec;
    if (!old) return -1;     /* require_existing already filtered, defence in depth */

    uint8_t *new_buf = (uint8_t *)rec->value;
    if (old->vlen >= (size_t)w->ts->total_size) {
        memcpy(new_buf, old->value, w->ts->total_size);
    } else {
        memcpy(new_buf, old->value, old->vlen);
        memset(new_buf + old->vlen, 0, (size_t)w->ts->total_size - old->vlen);
    }
    rec->vlen = (size_t)w->ts->total_size;

    /* Walk CSV row span — same as the old per-record path. */
    const char *cp = delim_rec->body_start;
    const char *line_end = delim_rec->line_end;
    for (int ai = 0; ai < w->active_count && cp < line_end; ai++) {
        const char *cell_start = cp;
        const char *cell_end = cp;
        while (cell_end < line_end && *cell_end != w->delimiter) cell_end++;
        size_t cell_len = cell_end - cell_start;
        if (cell_len > 0) {
            int tidx = w->active_indices[ai];
            if (!w->ts->fields[tidx].removed) {
                char tmpcell[256];
                char *cellbuf = (cell_len < sizeof(tmpcell))
                    ? tmpcell : malloc(cell_len + 1);
                memcpy(cellbuf, cell_start, cell_len);
                cellbuf[cell_len] = '\0';
                encode_field(&w->ts->fields[tidx], cellbuf,
                              new_buf + w->ts->fields[tidx].offset);
                if (cellbuf != tmpcell) free(cellbuf);
            }
        }
        cp = cell_end < line_end ? cell_end + 1 : line_end;
    }

    for (int fi = 0; fi < w->ts->nfields; fi++) {
        if (w->ts->fields[fi].removed) continue;
        if (w->ts->fields[fi].default_kind == DK_AUTO_UPDATE) {
            char tbuf[24];
            if (w->ts->fields[fi].type == FT_TIMESTAMP) {
                struct timespec tsn;
                clock_gettime(CLOCK_REALTIME, &tsn);
                long long ms = (long long)tsn.tv_sec * 1000LL + tsn.tv_nsec / 1000000LL;
                snprintf(tbuf, sizeof(tbuf), "%lld", ms);
            } else if (w->ts->fields[fi].type == FT_DATETIMEMS) {
                struct timespec tsn;
                clock_gettime(CLOCK_REALTIME, &tsn);
                time_t nowsec = tsn.tv_sec;
                struct tm tm; localtime_r(&nowsec, &tm);
                int msec = (int)(tsn.tv_nsec / 1000000L);
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d%03d",
                          tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                          tm.tm_hour, tm.tm_min, tm.tm_sec, msec);
            } else {
                time_t now = time(NULL);
                struct tm tm; localtime_r(&now, &tm);
                if (w->ts->fields[fi].type == FT_DATE)
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                              tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
                else
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                              tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                              tm.tm_hour, tm.tm_min, tm.tm_sec);
            }
            encode_field(&w->ts->fields[fi], tbuf,
                          new_buf + w->ts->fields[fi].offset);
        }
    }
    return 0;
}

static void *bulk_upd_delim_shard_worker_v2(BulkUpdDelimShardWork *w) {
    SlotcaskSchemaInfo info = {
        .splits = w->sch->splits, .slot_size = w->sch->slot_size,
        .streams = w->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(w->db_root, w->object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "bulk_upd_delim_shard_worker_v2: slotcask_registry_get failed for %s/%s", w->db_root, w->object);
        w->skipped += w->count; return NULL;
    }

    /* Build batch + scratch slab. value_compute will write into the
       slab; rec->value points at this worker's slot up front. */
    SlotcaskBulkRec   *batch   = calloc(w->count, sizeof(SlotcaskBulkRec));
    V2BulkUpdDelimCtx *ctxs    = malloc(w->count * sizeof(V2BulkUpdDelimCtx));
    uint8_t           *scratch = malloc((size_t)w->count * (size_t)w->ts->total_size);
    if (!batch || !ctxs || !scratch) {
        LOG_ERROR(LOG_SUB_QUERY, "bulk_upd_delim_shard_worker_v2: alloc failed, skipping %d updates", w->count);
        free(batch); free(ctxs); free(scratch);
        w->skipped += w->count;
        return NULL;
    }
    for (int ki = 0; ki < w->count; ki++) {
        BulkUpdDelimRec *rec = &w->recs[ki];
        ctxs[ki].w   = w;
        ctxs[ki].rec = rec;
        batch[ki].key       = rec->key;
        batch[ki].klen      = rec->klen;
        batch[ki].value     = scratch + (size_t)ki * (size_t)w->ts->total_size;
        batch[ki].vlen      = (size_t)w->ts->total_size;
        batch[ki].user_ctx  = &ctxs[ki];
        batch[ki].old_value = NULL;
        batch[ki].old_vlen  = 0;
        batch[ki].status    = 0;
        batch[ki].was_update = 0;
    }

    SlotcaskBulkOpts opts = {
        .require_existing     = 1,
        .pre_commit           = v2_bulk_upd_delim_pre_commit_bulk,
        .pre_commit_needs_old = w->nidx > 0,
        .value_compute        = v2_bulk_upd_delim_value_compute,
    };
    (void)slotcask_bulk_upsert_in_kfshard(sdb, w->shard_id,
                                           batch, (size_t)w->count, &opts);

    for (int ki = 0; ki < w->count; ki++) {
        if (batch[ki].status == 0) w->updated++;
        else w->skipped++;
    }

    free(batch); free(ctxs); free(scratch);
    return NULL;
}

static void *bulk_upd_delim_shard_worker(void *arg) {
    BulkUpdDelimShardWork *w = (BulkUpdDelimShardWork *)arg;
    if (w->count == 0) return NULL;
    return bulk_upd_delim_shard_worker_v2(w);
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

    Schema sch = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */
    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    /* Synthesise the same struct fields the body used to expect. */
    struct stat st = { .st_size = (off_t)size };
    int data_mmaped = 0;  /* helper never owns; caller handles lifetime */

    /* Active-field mapping — same as bulk-insert-delimited. */
    int active_indices[MAX_FIELDS];
    int active_count = 0;
    int has_tombstones = 0;
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed) has_tombstones = 1;
        else active_indices[active_count++] = i;
    }

    /* ===== Phase 1: parse every CSV row into records[]. Each record carries
       an owned key and a (body_start, line_end) span into the mmap'd CSV so
       phase 2 can re-scan the field values without buffering every span. */
    int matched = 0, skipped = 0;
    size_t rec_cap = 1024, rec_count = 0;
    BulkUpdDelimRec *records = malloc(rec_cap * sizeof(BulkUpdDelimRec));
    const char *rp = data;
    const char *data_end = data + st.st_size;

    while (rp < data_end) {
        const char *eol = rp;
        while (eol < data_end && *eol != '\n' && *eol != '\r') eol++;
        if (eol == rp) {
            rp = eol + 1;
            if (rp < data_end && *(rp - 1) == '\r' && *rp == '\n') rp++;
            continue;
        }
        const char *line_start = rp;
        const char *line_end   = eol;
        rp = eol;
        if (rp < data_end && *rp == '\r') rp++;
        if (rp < data_end && *rp == '\n') rp++;

        const char *key_end = line_start;
        while (key_end < line_end && *key_end != delimiter) key_end++;
        if (key_end == line_end) continue;

        size_t klen = key_end - line_start;
        if ((int)klen > sch.max_key) { skipped++; continue; }

        matched++;

        if (rec_count >= rec_cap) {
            rec_cap *= 2;
            /* Plain realloc + nested cleanup (same pattern as the bulk-update-json
               OOM fix): per-record `key` is heap-malloc'd, so xrealloc_or_free's
               atomic-free leaves no chance to walk records[] for cleanup. */
            BulkUpdDelimRec *t = realloc(records, rec_cap * sizeof(*t));
            if (!t) {
                for (size_t k = 0; k < rec_count; k++) free(records[k].key);
                free(records);
                records = NULL;
                rec_count = 0;
                break;
            }
            records = t;
        }
        BulkUpdDelimRec *r = &records[rec_count++];
        r->key = malloc(klen + 1);
        memcpy(r->key, line_start, klen); r->key[klen] = '\0';
        r->klen = klen;
        r->body_start = key_end + 1;
        r->line_end   = line_end;
        compute_hash_raw(r->key, klen, r->hash);
        r->shard_id = compute_record_shard(r->hash, sch.splits);
        r->start_slot = 0;
        /* v2 alignment — see cmd_bulk_insert for rationale. */
                    r->shard_id = compute_record_shard(r->hash, sch.splits);
    }

    /* ===== Phase 1.5: bucket by shard_id.
       OOM bails free the mmap/buf-backed `data` along with records[]. */
    int *shard_counts = calloc(sch.splits, sizeof(int));
    if (!shard_counts) {
        for (size_t i = 0; i < rec_count; i++) free(records[i].key);
        free(records);
        /* data owned by caller — see bulk_ins_delim_run / bulk_upd_delim_run */
        OUT("{\"error\":\"oom: bulk_update_delim shard_counts\"}\n");
        return 1;
    }
    for (size_t i = 0; i < rec_count; i++) shard_counts[records[i].shard_id]++;
    int *worker_shards = NULL, *shard_to_worker = NULL;
    int nshard_groups = build_shard_worker_map(shard_counts, sch.splits,
                                                &worker_shards, &shard_to_worker);

    BulkUpdDelimShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkUpdDelimShardWork)) : NULL;
    if (nshard_groups < 0 || (nshard_groups > 0 && !workers)) {
        free(workers); free(worker_shards); free(shard_to_worker); free(shard_counts);
        for (size_t i = 0; i < rec_count; i++) free(records[i].key);
        free(records);
        /* data owned by caller — see bulk_ins_delim_run / bulk_upd_delim_run */
        OUT("{\"error\":\"oom: bulk_update_delim workers\"}\n");
        return 1;
    }
    for (int g = 0; g < nshard_groups; g++) {
        int s = worker_shards[g];
        workers[g].shard_id = s;
        workers[g].recs = malloc(shard_counts[s] * sizeof(BulkUpdDelimRec));
        workers[g].count = 0;
    }
    for (size_t i = 0; i < rec_count; i++) {
        int w = shard_to_worker[records[i].shard_id];
        workers[w].recs[workers[w].count++] = records[i];
    }
    free(records);
    free(shard_counts);
    free(worker_shards);
    free(shard_to_worker);

    for (int wi = 0; wi < nshard_groups; wi++) {
        workers[wi].db_root = db_root;
        workers[wi].object = object;
        workers[wi].sch = &sch;
        workers[wi].ts = ts;
        workers[wi].idx_fields = (const char (*)[256])idx_fields;
        workers[wi].idx_types = idx_types;
        workers[wi].nidx = nidx;
        workers[wi].active_indices = active_indices;
        workers[wi].active_count = active_count;
        workers[wi].has_tombstones = has_tombstones;
        workers[wi].delimiter = delimiter;
        workers[wi].updated = 0;
        workers[wi].skipped = 0;
    }

    /* ===== Phase 2: parallel shard workers. */
    parallel_for_io(bulk_upd_delim_shard_worker, workers, nshard_groups,
                 sizeof(BulkUpdDelimShardWork));

    int updated = 0;
    for (int wi = 0; wi < nshard_groups; wi++) {
        updated += workers[wi].updated;
        skipped += workers[wi].skipped;
        for (int i = 0; i < workers[wi].count; i++) free(workers[wi].recs[i].key);
        free(workers[wi].recs);
    }
    free(workers);

    /* Caller owns `data`. */
    (void)data_mmaped; (void)st;

    LOG_INFO(LOG_SUB_QUERY, "BULK-UPDATE-DELIM %s matched=%d updated=%d skipped=%d",
            object, matched, updated, skipped);
    OUT("{\"matched\":%d,\"updated\":%d,\"skipped\":%d}\n", matched, updated, skipped);
    return 0;
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

typedef struct {
    char        *key;            /* heap-owned, null-terminated */
    size_t       klen;
    uint8_t      hash[16];
    int          start_slot;
    int          shard_id;
    /* Field deltas: aligned arrays of (typed-field index, owned-string value).
       Only fields present in `data` populate these — missing fields are
       left alone in the worker. */
    int          n_fields;
    int         *field_indices;  /* heap-owned, length n_fields */
    char       **field_values;   /* heap-owned strings, length n_fields */
} BulkUpdJsonRec;

typedef struct {
    const char       *db_root;
    const char       *object;
    const Schema     *sch;
    TypedSchema      *ts;
    const char      (*idx_fields)[256];
    const enum IndexType *idx_types;  /* [nidx] — NULL = legacy all-btree */
    int               nidx;
    int               shard_id;
    BulkUpdJsonRec   *recs;
    int               count;
    /* Results */
    int               updated;
    int               skipped;
} BulkUpdJsonShardWork;

/* === v2 bulk-update-json worker ===
 *
 * v1 patches fields in place under a single ucache wrlock (cheap because the
 * Zone B record lives at a fixed offset). v2 (slotcask) follows the engine's
 * locked design: every update allocates a new slot (snake-game pool reuse),
 * tombstones the old. So per record we:
 *   1. read the old typed payload via slotcask_get
 *   2. memcpy into a heap buffer; encode_field for every touched field
 *      + auto_update fields
 *   3. slotcask_upsert_with_hooks(require_existing=1) with a pre_commit hook
 *      that performs the per-field index drop/insert diff
 *
 * Index entries are written synchronously inside the hook (mirror v1: every
 * indexed field that moved gets `delete_index_entry` + `write_index_entry`).
 * No bulk btree merge phase — the merge phase belongs to bulk-INSERT where
 * entries point at fresh records. */
typedef struct {
    BulkUpdJsonShardWork *w;
    BulkUpdJsonRec       *rec;
} V2BulkUpdJsonCtx;

/* Bulk-shaped pre_commit; new value via rec->value (set by value_compute). */
static int v2_bulk_upd_json_pre_commit_bulk(const SlotcaskOldRecord *old,
                                             SlotcaskBulkRec *rec,
                                             int is_update) {
    (void)is_update;
    V2BulkUpdJsonCtx *ctx = (V2BulkUpdJsonCtx *)rec->user_ctx;
    BulkUpdJsonShardWork *w = ctx->w;
    BulkUpdJsonRec       *json_rec = ctx->rec;
    if (w->nidx == 0 || !old) return 0;

    const uint8_t *new_value = (const uint8_t *)rec->value;
    for (int fi = 0; fi < w->nidx; fi++) {
        uint8_t *old_buf = NULL, *new_buf = NULL;
        size_t   old_len = 0,   new_len = 0;
        int have_old = build_index_key_from_record(w->ts, old->value,
                                                    w->idx_fields[fi],
                                                    &old_buf, &old_len);
        int have_new = build_index_key_from_record(w->ts, new_value,
                                                    w->idx_fields[fi],
                                                    &new_buf, &new_len);
        int changed = 0;
        if (have_new && !have_old) changed = 1;
        else if (!have_new && have_old) changed = 1;
        else if (have_new && have_old) {
            if (new_len != old_len ||
                memcmp(new_buf, old_buf, new_len) != 0) changed = 1;
        }
        if (changed) {
            enum IndexType itype = w->idx_types ? w->idx_types[fi] : IT_BTREE;
            if (itype == IT_TRIGRAM) {
                UpdateIdxArg a = {0};
                a.db_root = w->db_root; a.object = w->object;
                a.field = w->idx_fields[fi]; a.splits = w->sch->splits;
                a.new_key = have_new ? new_buf : NULL;
                a.new_len = new_len;
                a.old_key = have_old ? old_buf : NULL;
                a.old_len = old_len;
                a.hash = json_rec->hash;
                a.type = IT_TRIGRAM;
                update_idx_fn(&a);
            } else {
                if (have_old)
                    delete_index_entry(w->db_root, w->object, w->idx_fields[fi],
                                        w->sch->splits, old_buf, old_len, json_rec->hash);
                if (have_new)
                    write_index_entry(w->db_root, w->object, w->idx_fields[fi],
                                       w->sch->splits, new_buf, new_len, json_rec->hash);
            }
        }
        free(old_buf); free(new_buf);
    }
    return 0;
}

/* Compute NEW from OLD: copy old to scratch, patch JSON-named fields,
   apply auto_update. Same logic the per-record path used to inline. */
static int v2_bulk_upd_json_value_compute(const SlotcaskOldRecord *old,
                                           SlotcaskBulkRec *rec) {
    V2BulkUpdJsonCtx *ctx = (V2BulkUpdJsonCtx *)rec->user_ctx;
    BulkUpdJsonShardWork *w = ctx->w;
    BulkUpdJsonRec       *json_rec = ctx->rec;
    if (!old) return -1;

    uint8_t *new_buf = (uint8_t *)rec->value;
    if (old->vlen >= (size_t)w->ts->total_size) {
        memcpy(new_buf, old->value, w->ts->total_size);
    } else {
        memcpy(new_buf, old->value, old->vlen);
        memset(new_buf + old->vlen, 0, (size_t)w->ts->total_size - old->vlen);
    }
    rec->vlen = (size_t)w->ts->total_size;

    for (int i = 0; i < json_rec->n_fields; i++) {
        int tidx = json_rec->field_indices[i];
        if (tidx < 0 || tidx >= w->ts->nfields) continue;
        if (w->ts->fields[tidx].removed) continue;
        encode_field(&w->ts->fields[tidx], json_rec->field_values[i],
                      new_buf + w->ts->fields[tidx].offset);
    }
    for (int fi = 0; fi < w->ts->nfields; fi++) {
        if (w->ts->fields[fi].removed) continue;
        if (w->ts->fields[fi].default_kind == DK_AUTO_UPDATE) {
            char tbuf[24];
            if (w->ts->fields[fi].type == FT_TIMESTAMP) {
                struct timespec tsn;
                clock_gettime(CLOCK_REALTIME, &tsn);
                long long ms = (long long)tsn.tv_sec * 1000LL + tsn.tv_nsec / 1000000LL;
                snprintf(tbuf, sizeof(tbuf), "%lld", ms);
            } else if (w->ts->fields[fi].type == FT_DATETIMEMS) {
                struct timespec tsn;
                clock_gettime(CLOCK_REALTIME, &tsn);
                time_t nowsec = tsn.tv_sec;
                struct tm tm; localtime_r(&nowsec, &tm);
                int msec = (int)(tsn.tv_nsec / 1000000L);
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d%03d",
                          tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                          tm.tm_hour, tm.tm_min, tm.tm_sec, msec);
            } else {
                time_t now = time(NULL);
                struct tm tm; localtime_r(&now, &tm);
                if (w->ts->fields[fi].type == FT_DATE)
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                              tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
                else
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                              tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                              tm.tm_hour, tm.tm_min, tm.tm_sec);
            }
            encode_field(&w->ts->fields[fi], tbuf,
                          new_buf + w->ts->fields[fi].offset);
        }
    }
    return 0;
}

/* Concurrency caveat (v2 bulk-update-json + delim, partial-field):
   the read-old → patch → upsert sequence is NOT atomic. v1 holds the
   ucache wrlock across the whole shard worker, so partial updates see
   a consistent snapshot. v2 acquires the kf-shard wrlock per record,
   which gives finer parallelism but means a concurrent writer between
   the slotcask_get and the upsert can lose the racing writer's changes
   to fields THIS bulk doesn't touch. Bulk-update has no CAS semantics
   in either version (`if_json` is not a per-record knob in the bulk
   protocol), so the loss is silent. Use single-record cmd_update with
   `if_json` for strict CAS; bulk-update is documented as
   "last-writer-wins on the touched fields, snapshot-of-read on the
   untouched fields." */
static void *bulk_upd_json_shard_worker_v2(BulkUpdJsonShardWork *w) {
    SlotcaskSchemaInfo info = {
        .splits = w->sch->splits, .slot_size = w->sch->slot_size,
        .streams = w->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(w->db_root, w->object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "bulk_upd_json_shard_worker_v2: slotcask_registry_get failed for %s/%s", w->db_root, w->object);
        w->skipped += w->count; return NULL;
    }

    SlotcaskBulkRec  *batch   = calloc(w->count, sizeof(SlotcaskBulkRec));
    V2BulkUpdJsonCtx *ctxs    = malloc(w->count * sizeof(V2BulkUpdJsonCtx));
    uint8_t          *scratch = malloc((size_t)w->count * (size_t)w->ts->total_size);
    if (!batch || !ctxs || !scratch) {
        LOG_ERROR(LOG_SUB_QUERY, "bulk_upd_json_shard_worker_v2: alloc failed, skipping %d updates", w->count);
        free(batch); free(ctxs); free(scratch);
        w->skipped += w->count;
        return NULL;
    }
    for (int ki = 0; ki < w->count; ki++) {
        BulkUpdJsonRec *rec = &w->recs[ki];
        ctxs[ki].w   = w;
        ctxs[ki].rec = rec;
        batch[ki].key       = rec->key;
        batch[ki].klen      = rec->klen;
        batch[ki].value     = scratch + (size_t)ki * (size_t)w->ts->total_size;
        batch[ki].vlen      = (size_t)w->ts->total_size;
        batch[ki].user_ctx  = &ctxs[ki];
        batch[ki].old_value = NULL;
        batch[ki].old_vlen  = 0;
        batch[ki].status    = 0;
        batch[ki].was_update = 0;
    }

    SlotcaskBulkOpts opts = {
        .require_existing     = 1,
        .pre_commit           = v2_bulk_upd_json_pre_commit_bulk,
        .pre_commit_needs_old = w->nidx > 0,
        .value_compute        = v2_bulk_upd_json_value_compute,
    };
    (void)slotcask_bulk_upsert_in_kfshard(sdb, w->shard_id,
                                           batch, (size_t)w->count, &opts);

    for (int ki = 0; ki < w->count; ki++) {
        if (batch[ki].status == 0) w->updated++;
        else w->skipped++;
    }

    free(batch); free(ctxs); free(scratch);
    return NULL;
}

static void *bulk_upd_json_shard_worker(void *arg) {
    BulkUpdJsonShardWork *w = (BulkUpdJsonShardWork *)arg;
    if (w->count == 0) return NULL;
    return bulk_upd_json_shard_worker_v2(w);
}

/* Internal helper: read input (file path or in-memory string) into a heap buffer.
   `input_is_file` is 1 for file path, 0 for in-memory string passed verbatim. */
static int bulk_upd_json_run(const char *db_root, const char *object,
                              const char *input, int input_is_file) {
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"bulk-update-json requires typed fields (fields.conf)\"}\n");
        return 1;
    }
    Schema sch = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */
    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    char *json = NULL;
    size_t len = 0;
    int json_mmaped = 0;
    int ifd = -1;
    if (input_is_file) {
        ifd = open(input, O_RDONLY);
        if (ifd < 0) { OUT("{\"error\":\"Cannot open file\"}\n"); return 1; }
        struct stat st;
        if (fstat(ifd, &st) < 0) { close(ifd); OUT("{\"error\":\"Cannot stat file\"}\n"); return 1; }
        if (st.st_size == 0) { close(ifd); OUT("{\"error\":\"Empty file\"}\n"); return 1; }
        len = st.st_size;
        json = mmap(NULL, len, PROT_READ, MAP_SHARED, ifd, 0);
        if (json == MAP_FAILED) {
            json = malloc(len + 1);
            lseek(ifd, 0, SEEK_SET);
            size_t rd = 0;
            while (rd < len) {
                ssize_t n = read(ifd, json + rd, len - rd);
                if (n <= 0) break;
                rd += n;
            }
            json[rd] = '\0';
        } else {
            madvise((void *)json, len, MADV_SEQUENTIAL);
            json_mmaped = 1;
        }
        close(ifd);
    } else {
        json = (char *)input;
        len = strlen(input);
    }

    /* Phase 1: parse the array, extract per-record (key, touched fields, hash, shard). */
    BulkUpdJsonRec *records = NULL;
    size_t rec_cap = 1024, rec_count = 0;
    records = malloc(rec_cap * sizeof(BulkUpdJsonRec));

    int matched = 0, skipped = 0;

    const char *p = json_skip(json);
    int is_object_format = (*p == '{'); /* {"k1":{...},"k2":{...}}    — round-trips with get-multi */
    int is_array_format  = (*p == '[');  /* [{"key":"k1","value":{...}},...] */
    if (!is_object_format && !is_array_format) {
        OUT("{\"error\":\"bulk-update JSON must be a top-level object or array\"}\n");
        if (json_mmaped) munmap((void *)json, len);
        else if (input_is_file) free(json);
        free(records);
        return 1;
    }
    p++;

    /* Pre-name the typed fields once so we can reuse the names array per
       record without rebuilding it. */
    const char *field_names[MAX_FIELDS];
    for (int i = 0; i < ts->nfields; i++) field_names[i] = ts->fields[i].name;

    while (*p) {
        p = json_skip(p);
        if (*p == ']' || *p == '}') break;
        if (*p == ',') { p++; continue; }

        char *key = NULL; size_t klen = 0;
        const char *data_str = NULL;
        const char *obj_end = NULL;
        char obj_buf[8192];
        char *obj_str = NULL;
        int obj_heap = 0;

        if (is_object_format) {
            /* "key": {...} */
            if (*p != '"') { p++; continue; }
            p++;
            const char *key_start = p;
            while (*p && *p != '"') p++;
            klen = p - key_start;
            key = malloc(klen + 1);
            memcpy(key, key_start, klen);
            key[klen] = '\0';
            if (*p == '"') p++;
            p = json_skip(p);
            if (*p == ':') p = json_skip(p + 1);

            /* Data span points into the original mmap; json_get_fields scans
               by brace-count so the trailing comma/} doesn't matter. */
            data_str = p;
            obj_end = json_skip_value(p);
        } else {
            if (*p != '{') { p++; continue; }
            const char *obj_start = p;
            obj_end = json_skip_value(p);
            size_t obj_len = obj_end - obj_start;

            if (obj_len < sizeof(obj_buf)) {
                memcpy(obj_buf, obj_start, obj_len);
                obj_buf[obj_len] = '\0';
                obj_str = obj_buf;
            } else {
                obj_str = malloc(obj_len + 1);
                memcpy(obj_str, obj_start, obj_len);
                obj_str[obj_len] = '\0';
                obj_heap = 1;
            }

            JsonObj rec;
            json_parse_object(obj_str, obj_len, &rec);

            const char *iv; size_t ivl;
            if (json_obj_unquoted(&rec, "key", &iv, &ivl)) {
                key = malloc(ivl + 1);
                memcpy(key, iv, ivl);
                key[ivl] = '\0';
                klen = ivl;
            }

            const char *dv; size_t dl;
            if (json_obj_get(&rec, "value", &dv, &dl)) {
                data_str = dv;
                (void)dl;
            }
        }

        if (!key || !data_str) {
            skipped++;
            free(key);
            if (obj_heap) free(obj_str);
            p = obj_end;
            continue;
        }
        if ((int)klen > sch.max_key) {
            skipped++;
            free(key);
            if (obj_heap) free(obj_str);
            p = obj_end;
            continue;
        }

        /* Pull out every typed-field name from `data`. Fields not present in
           `data` come back NULL → not touched. */
        char *vals_buf[MAX_FIELDS];
        json_get_fields(data_str, field_names, ts->nfields, vals_buf);

        int n_touched = 0;
        for (int i = 0; i < ts->nfields; i++) if (vals_buf[i]) n_touched++;

        BulkUpdJsonRec *r;
        if (rec_count >= rec_cap) {
            rec_cap *= 2;
            /* Plain realloc (not xrealloc_or_free) so we can walk records[]
               for nested free() before releasing the array. xrealloc_or_free
               frees atomically, leaving no window to clean up the per-record
               heap-owned key / field_indices / field_values mallocs. */
            BulkUpdJsonRec *t = realloc(records, rec_cap * sizeof(*t));
            if (!t) {
                /* OOM: free per-record nested allocations, then the array,
                   then the current iteration's locals (whose ownership
                   hadn't transferred to records[rec_count] yet). Reset
                   rec_count so the downstream phase-2 loop sees an empty
                   set and the rec_count==0 branch handles the response. */
                for (size_t k = 0; k < rec_count; k++) {
                    free(records[k].key);
                    for (int j = 0; j < records[k].n_fields; j++)
                        free(records[k].field_values[j]);
                    free(records[k].field_values);
                    free(records[k].field_indices);
                }
                free(records);
                records = NULL;
                rec_count = 0;
                free(key);
                for (int i = 0; i < ts->nfields; i++) free(vals_buf[i]);
                if (obj_heap) free(obj_str);
                break;
            }
            records = t;
        }
        r = &records[rec_count++];
        r->key = key;
        r->klen = klen;
        compute_hash_raw(key, klen, r->hash);
        r->shard_id = compute_record_shard(r->hash, sch.splits);
        r->start_slot = 0;
        /* v2 alignment — see cmd_bulk_insert for rationale. */
                    r->shard_id = compute_record_shard(r->hash, sch.splits);
        if (n_touched > 0) {
            r->n_fields = n_touched;
            r->field_indices = malloc(n_touched * sizeof(int));
            r->field_values = malloc(n_touched * sizeof(char *));
            int j = 0;
            for (int i = 0; i < ts->nfields; i++) {
                if (vals_buf[i]) {
                    r->field_indices[j] = i;
                    r->field_values[j] = vals_buf[i];   /* take ownership */
                    j++;
                }
            }
        } else {
            r->n_fields = 0;
            r->field_indices = NULL;
            r->field_values = NULL;
        }
        matched++;

        if (obj_heap) free(obj_str);
        p = obj_end;
    }

    if (rec_count == 0) {
        OUT("{\"matched\":0,\"updated\":0,\"skipped\":%d}\n", skipped);
        if (json_mmaped) munmap((void *)json, len);
        else if (input_is_file) free(json);
        free(records);
        return 0;
    }

    /* Bucket per shard — same pattern as bulk-update-delimited / bulk-delete. */
    int *shard_counts = calloc(sch.splits, sizeof(int));
    if (!shard_counts) {
        for (size_t i = 0; i < rec_count; i++) {
            free(records[i].key);
            for (int j = 0; j < records[i].n_fields; j++) free(records[i].field_values[j]);
            free(records[i].field_values);
            free(records[i].field_indices);
        }
        free(records);
        if (json_mmaped) munmap((void *)json, len); else if (input_is_file) free(json);
        OUT("{\"error\":\"oom: shard_counts\"}\n");
        return 1;
    }
    for (size_t i = 0; i < rec_count; i++) shard_counts[records[i].shard_id]++;

    int nshard_groups = 0;
    for (int s = 0; s < sch.splits; s++) if (shard_counts[s] > 0) nshard_groups++;

    BulkUpdJsonShardWork *workers = calloc(nshard_groups, sizeof(BulkUpdJsonShardWork));
    if (nshard_groups > 0 && !workers) {
        free(shard_counts);
        for (size_t i = 0; i < rec_count; i++) {
            free(records[i].key);
            for (int j = 0; j < records[i].n_fields; j++) free(records[i].field_values[j]);
            free(records[i].field_values);
            free(records[i].field_indices);
        }
        free(records);
        if (json_mmaped) munmap((void *)json, len); else if (input_is_file) free(json);
        OUT("{\"error\":\"oom: workers\"}\n");
        return 1;
    }
    int wi = 0;
    for (int s = 0; s < sch.splits; s++) {
        if (shard_counts[s] > 0) {
            workers[wi].db_root = db_root;
            workers[wi].object = object;
            workers[wi].sch = &sch;
            workers[wi].ts = ts;
            workers[wi].idx_fields = idx_fields;
            workers[wi].idx_types = idx_types;
            workers[wi].nidx = nidx;
            workers[wi].shard_id = s;
            workers[wi].recs = malloc(shard_counts[s] * sizeof(BulkUpdJsonRec));
            workers[wi].count = 0;
            workers[wi].updated = 0;
            workers[wi].skipped = 0;
            wi++;
        }
    }

    for (size_t i = 0; i < rec_count; i++) {
        for (int gi = 0; gi < nshard_groups; gi++) {
            if (workers[gi].shard_id == records[i].shard_id) {
                workers[gi].recs[workers[gi].count++] = records[i];
                break;
            }
        }
        /* Null the heap-owned pointers in records[i] unconditionally — the
           inner loop above ALWAYS finds a matching worker by construction
           (every records[i].shard_id was used to compute shard_counts, and
           every shard with count > 0 got a worker), but Coverity can't
           trace the invariant. NULLing outside the if covers the
           unreachable no-match case so free(records) below is leak-free
           in the static analyzer's view too. */
        records[i].key = NULL;
        records[i].field_values = NULL;
        records[i].field_indices = NULL;
    }
    free(shard_counts);

    /* Phase 2: parallel shard workers. */
    parallel_for_io(bulk_upd_json_shard_worker, workers, nshard_groups,
                 sizeof(BulkUpdJsonShardWork));

    int updated = 0;
    for (int gi = 0; gi < nshard_groups; gi++) {
        updated += workers[gi].updated;
        skipped += workers[gi].skipped;
        for (int i = 0; i < workers[gi].count; i++) {
            BulkUpdJsonRec *r = &workers[gi].recs[i];
            free(r->key);
            for (int j = 0; j < r->n_fields; j++) free(r->field_values[j]);
            free(r->field_values);
            free(r->field_indices);
        }
        free(workers[gi].recs);
    }
    free(workers);
    free(records);

    if (json_mmaped) munmap((void *)json, len);
    else if (input_is_file) free(json);

    LOG_INFO(LOG_SUB_QUERY, "BULK-UPDATE-JSON %s matched=%d updated=%d skipped=%d",
            object, matched, updated, skipped);
    OUT("{\"matched\":%d,\"updated\":%d,\"skipped\":%d}\n", matched, updated, skipped);
    return 0;
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
    int              field_idx;   /* index into w->idx_fields[] */
    enum IndexType   itype;
    uint8_t          hash[16];
    uint8_t         *idx_key;     /* heap-alloc'd; freed by bulk_delete_flush_drops */
    size_t           idx_key_len;
} BulkDelCritDropEntry;

typedef struct {
    SlotcaskDb     *sdb;
    const char     *db_root;
    const char     *object;
    const Schema   *sch;
    TypedSchema    *ts;
    CriteriaNode   *tree;
    FieldSchema    *fs;
    SearchCriterion *cas_crit;
    int             cas_ncrit;
    char          (*idx_fields)[256];
    const enum IndexType *idx_types;  /* [nidx] — NULL = legacy all-btree */
    int             nidx;
    /* per-shard records */
    char          **keys;
    uint8_t       (*hashes)[16];
    int             count;
    /* per-worker deferred index drop buffer (collected during Phase 2,
       flushed in bulk after parallel_for returns) */
    BulkDelCritDropEntry *drop_entries;
    int                   drop_count;
    int                   drop_cap;
    /* result */
    int             deleted;
    int             skipped;
} BulkDelCritShardWork;

typedef struct {
    BulkDelCritShardWork *w;
    int                    ki;
} V2BulkDelCritCtx;

static int v2_bulk_del_crit_pre_commit_bulk(const SlotcaskOldRecord *old,
                                             SlotcaskBulkRec *rec) {
    V2BulkDelCritCtx *ctx = (V2BulkDelCritCtx *)rec->user_ctx;
    BulkDelCritShardWork *w = ctx->w;
    int ki = ctx->ki;
    if (!old) return -1;
    if (!criteria_match_tree(old->value, w->tree, w->fs)) return -1;
    if (w->cas_crit && w->cas_ncrit > 0 &&
        !cas_check(w->ts, old->value, (int)old->vlen, w->cas_crit, w->cas_ncrit)) return -1;

    if (w->nidx > 0 && w->ts) {
        for (int fi = 0; fi < w->nidx; fi++) {
            uint8_t *buf = NULL; size_t blen = 0;
            if (!build_index_key_from_record(w->ts, old->value,
                                               w->idx_fields[fi], &buf, &blen))
                continue;
            enum IndexType itype = w->idx_types ? w->idx_types[fi] : IT_BTREE;
            /* Grow drop buffer if needed */
            if (w->drop_count >= w->drop_cap) {
                int new_cap = w->drop_cap ? w->drop_cap * 2 : 16;
                BulkDelCritDropEntry *tmp = realloc(w->drop_entries,
                    (size_t)new_cap * sizeof(BulkDelCritDropEntry));
                if (!tmp) { free(buf); continue; }
                w->drop_entries = tmp;
                w->drop_cap = new_cap;
            }
            BulkDelCritDropEntry *e = &w->drop_entries[w->drop_count++];
            e->field_idx  = fi;
            e->itype      = itype;
            memcpy(e->hash, w->hashes[ki], 16);
            e->idx_key    = buf;   /* ownership transferred; freed by flush */
            e->idx_key_len = blen;
        }
    }
    return 0;
}

static void *bulk_del_crit_shard_worker(void *arg) {
    BulkDelCritShardWork *w = (BulkDelCritShardWork *)arg;
    if (w->count == 0) return NULL;

    /* All keys in this worker hash to the same kf shard (dispatcher
       pre-sorted by compute_record_shard). */
    int kf_shard_id = compute_record_shard(w->hashes[0], w->sch->splits);

    SlotcaskBulkRec  *batch = calloc((size_t)w->count, sizeof(SlotcaskBulkRec));
    V2BulkDelCritCtx *ctxs  = malloc((size_t)w->count * sizeof(V2BulkDelCritCtx));
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
        .pre_commit           = v2_bulk_del_crit_pre_commit_bulk,
        /* CAS re-verification needs OLD even if there are no indexes;
           force the batched read regardless of nidx. */
        .pre_commit_needs_old = 1,
    };
    (void)slotcask_bulk_delete_in_kfshard(w->sdb, kf_shard_id,
                                           batch, (size_t)w->count, &opts);

    for (int i = 0; i < w->count; i++) {
        if (batch[i].status == 0) w->deleted++;
        else w->skipped++;
    }
    free(batch); free(ctxs);
    return NULL;
}

/* Comparator for bulk_delete_flush_drops: sort by (field_idx, hash[0:2])
   so drops to the same btree shard are adjacent (cache locality). */
static int cmp_bulk_del_drop(const void *a, const void *b) {
    const BulkDelCritDropEntry *ea = a, *eb = b;
    if (ea->field_idx != eb->field_idx) return ea->field_idx - eb->field_idx;
    return memcmp(ea->hash, eb->hash, 2);
}

/* Merge all per-worker drop buffers, sort for locality, process in order.
   Called after Phase 2's parallel_for returns. Workers own their idx_key
   buffers; this function frees them. */
static void bulk_delete_flush_drops(BulkDelCritShardWork *workers,
                                     int nshard_groups,
                                     const char *db_root,
                                     const char *object,
                                     int splits,
                                     char idx_fields[][256],
                                     const enum IndexType *idx_types) {
    /* Count total entries */
    int total = 0;
    for (int g = 0; g < nshard_groups; g++) total += workers[g].drop_count;
    if (total == 0) return;

    BulkDelCritDropEntry *all = malloc((size_t)total * sizeof(BulkDelCritDropEntry));
    if (!all) {
        /* OOM: free idx_key buffers in workers to avoid leaks */
        for (int g = 0; g < nshard_groups; g++)
            for (int i = 0; i < workers[g].drop_count; i++)
                free(workers[g].drop_entries[i].idx_key);
        return;
    }
    int pos = 0;
    for (int g = 0; g < nshard_groups; g++) {
        if (workers[g].drop_count == 0) continue;
        memcpy(&all[pos], workers[g].drop_entries,
               (size_t)workers[g].drop_count * sizeof(BulkDelCritDropEntry));
        pos += workers[g].drop_count;
    }

    qsort(all, (size_t)total, sizeof(BulkDelCritDropEntry), cmp_bulk_del_drop);

    for (int i = 0; i < total; i++) {
        BulkDelCritDropEntry *e = &all[i];
        if (e->itype == IT_TRIGRAM) {
            UpdateIdxArg a = {0};
            a.db_root  = db_root; a.object = object;
            a.field    = idx_fields[e->field_idx];
            a.splits   = splits;
            a.new_key  = NULL; a.new_len = 0;
            a.old_key  = e->idx_key; a.old_len = e->idx_key_len;
            a.hash     = e->hash; a.type = IT_TRIGRAM;
            update_idx_fn(&a);
        } else {
            delete_index_entry(db_root, object, idx_fields[e->field_idx],
                                splits, e->idx_key, e->idx_key_len, e->hash);
        }
        free(e->idx_key);
    }
    free(all);
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
    SearchCriterion *cas_crit = NULL;
    int cas_ncrit = 0;
    if (if_json && if_json[0] &&
        parse_criteria_json(if_json, &cas_crit, &cas_ncrit) != 0) {
        OUT("{\"error\":\"invalid if condition\"}\n");
        free_criteria_tree(tree);
        return 1;
    }

    /* Phase 1: resolve matching keys — indexed path first, full scan fallback */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    compile_criteria_tree(tree, fs.ts);
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    BulkCriteriaCtx ctx = { tree, &fs, NULL, 0, 0, limit, &dl, 0, 0, 0,
                            PTHREAD_MUTEX_INITIALIZER };
    if (!bulk_delete_phase1_indexed(db_root, object, &sch, &fs,
                                     tree, limit, &dl, &ctx)) {
        scan_dispatch(db_root, object, &sch, data_dir, bulk_criteria_scan_cb, &ctx);
    }
    pthread_mutex_destroy(&ctx.lock);
    int matched = ctx.count;

    if (dl.timed_out) {
        LOG_WARN(LOG_SUB_QUERY, "bulk-delete: query deadline exceeded while matching criteria");
        OUT("{\"error\":\"query_timeout\"}\n");
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }
    if (ctx.budget_exceeded) {
        LOG_WARN(LOG_SUB_QUERY, "bulk-delete: query buffer cap exceeded while matching criteria");
        OUT(QUERY_BUFFER_ERR);
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }

    if (dry_run) {
        OUT("{\"matched\":%d,\"deleted\":0,\"skipped\":0,\"dry_run\":true}\n", matched);
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 0;
    }

    /* Phase 2: Write — for each key, acquire wrlock, re-verify, tombstone */
    TypedSchema *ts = load_typed_schema(db_root, object);
    int deleted = 0, skipped = 0;

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
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
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
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
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
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
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
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
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
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
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
        workers[g].cas_crit  = cas_crit;
        workers[g].cas_ncrit = cas_ncrit;
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

    for (int g = 0; g < nshard_groups; g++) {
        deleted += workers[g].deleted;
        skipped += workers[g].skipped;
        free(workers[g].keys);
        free(workers[g].hashes);
    }
    /* Phase 3: flush deferred index drops in sorted (field, shard) order */
    bulk_delete_flush_drops(workers, nshard_groups,
                             db_root, object, sch.splits,
                             idx_fields, idx_types);
    for (int g = 0; g < nshard_groups; g++)
        free(workers[g].drop_entries);
    free(workers); free(hashes); free(shard_ids);
    free(shard_counts); free(worker_shards); free(shard_to_worker);

    if (deleted > 0) {
        update_count(db_root, object, -deleted);
        update_deleted_count(db_root, object, deleted);
    }
    LOG_INFO(LOG_SUB_QUERY, "BULK-DELETE %s matched=%d deleted=%d skipped=%d (v2)",
             object, matched, deleted, skipped);
    OUT("{\"matched\":%d,\"deleted\":%d,\"skipped\":%d}\n", matched, deleted, skipped);

    if (cas_crit) free_criteria(cas_crit, cas_ncrit);
    for (int i = 0; i < matched; i++) free(ctx.keys[i]);
    free(ctx.keys); free_criteria_tree(tree);
    return 0;
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
