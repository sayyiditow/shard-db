#include "types.h"
#include "slotcask.h"
#include "simd.h"
#include "bitmap.h"
#include "trigram.h"
#include "io_direct.h"
#include "query_internal.h"
#include <math.h>
#include <dirent.h>
/* Forward decl — defined below. */
static int agg_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *raw_ctx);

/* Run the aggregate pipeline over records keyed by an OR index-union KeySet.
   Tree-match is performed by agg_scan_cb itself (it checks ctx->tree), so
   both Shape C (pure OR) and hybrid (AND + OR) reach the correct records. */
/* Walk a pre-built KeySet and feed each record through agg_scan_cb. agg_scan_cb
   reads ctx->tree internally — caller nullifies tree to skip rematch (used by
   AND-intersection where the keyset is already exact). */
typedef struct {
    void          *agg_ctx;
    pthread_mutex_t lock;      /* guards agg_scan_cb from concurrent IO threads */
    QueryDeadline *dl;
    int            dl_counter;
} KeysetEmitAggCtx;

/* Callback for keyset_emit_agg: decodes value and feeds agg_scan_cb.
   agg_scan_cb mutates the AggCtx hash table, which is not thread-safe —
   the lock guards it across concurrent IO pool workers. */
static int keyset_emit_agg_cb(const uint8_t hash[16],
                               const void *key, size_t klen,
                               const void *value, size_t vlen,
                               void *ctx_ptr) {
    KeysetEmitAggCtx *ctx = (KeysetEmitAggCtx *)ctx_ptr;
    if (query_deadline_tick(ctx->dl, &ctx->dl_counter)) return 1;

    SlotHeader hdr = {0};
    memcpy(hdr.hash, hash, 16);
    hdr.flag = 1;
    hdr.key_len = (uint16_t)klen;
    hdr.value_len = (uint32_t)vlen;
    uint8_t stk[2048];
    uint8_t *block = (klen + vlen + 1 < sizeof(stk))
        ? stk : malloc(klen + vlen);
    if (block) {
        memcpy(block, key, klen);
        memcpy(block + klen, value, vlen);
        pthread_mutex_lock(&ctx->lock);
        agg_scan_cb(&hdr, block, ctx->agg_ctx);
        pthread_mutex_unlock(&ctx->lock);
        if (block != stk) free(block);
    }
    return 0;
}

static void keyset_emit_agg(const char *db_root, const char *object,
                            const Schema *sch, KeySet *ks, void *agg_ctx,
                            QueryDeadline *dl) {
    /* Count hashes in the keyset */
    size_t n = 0;
    for (size_t b = 0; b < ks->cap; b++)
        if (ks->state[b] == 2) n++;

    if (n == 0) return;

    /* Collect hashes */
    uint8_t (*hashes)[16] = malloc(n * sizeof(*hashes));
    if (!hashes) {
        /* Fallback: sequential per-record */
        int dl_counter = 0;
        for (size_t b = 0; b < ks->cap; b++) {
            if (query_deadline_tick(dl, &dl_counter)) break;
            if (ks->state[b] != 2) continue;
            RecordRef rr;
            if (read_record_ref(db_root, object, sch, ks->keys[b], &rr) != 0) continue;
            SlotHeader hdr = {0};
            memcpy(hdr.hash, ks->keys[b], 16);
            hdr.flag = 1;
            hdr.key_len = (uint16_t)rr.klen;
            hdr.value_len = (uint32_t)rr.vlen;
            uint8_t stk[2048];
            uint8_t *block = (rr.klen + rr.vlen + 1 < sizeof(stk))
                ? stk : malloc(rr.klen + rr.vlen);
            if (block) {
                memcpy(block, rr.key, rr.klen);
                memcpy(block + rr.klen, rr.val, rr.vlen);
                agg_scan_cb(&hdr, block, agg_ctx);
                if (block != stk) free(block);
            }
            release_record_ref(&rr);
        }
        return;
    }

    size_t idx = 0;
    for (size_t b = 0; b < ks->cap; b++)
        if (ks->state[b] == 2)
            memcpy(hashes[idx++], ks->keys[b], 16);

    /* Batch resolve+fetch */
    SlotcaskSchemaInfo sinfo = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);
    if (sdb) {
        KeysetEmitAggCtx cb_ctx;
        memset(&cb_ctx, 0, sizeof(cb_ctx));
        cb_ctx.agg_ctx = agg_ctx;
        cb_ctx.dl = dl;
        pthread_mutex_init(&cb_ctx.lock, NULL);
        slotcask_bulk_resolve_and_fetch(sdb, hashes, n, &cb_ctx, keyset_emit_agg_cb);
        pthread_mutex_destroy(&cb_ctx.lock);
    }
    free(hashes);
}

static void keyset_agg_from_or(const char *db_root, const char *object,
                               const Schema *sch, void *agg_ctx,
                               CriteriaNode *or_node, QueryDeadline *dl,
                               int *out_budget_exceeded) {
    /* Aggregate needs the full union — no short-circuit (target_count=0). */
    KeySet *ks = build_or_keyset(db_root, object, sch->splits, or_node, dl,
                                 out_budget_exceeded, 0);
    if (!ks) return;
    if (dl->timed_out) { keyset_free(ks); return; }
    keyset_emit_agg(db_root, object, sch, ks, agg_ctx, dl);
    keyset_free(ks);
}

/* AND index-intersection aggregate: build KeySet from intersection, then walk.
   For the big-primary path agg_ctx->tree must be NULL (intersection exact;
   skip redundant rematch). For small-primary agg_ctx->tree must be the full
   original tree so agg_scan_cb post-filters via criteria_match_tree.
   Returns 1 if it took the small-primary path (caller already set tree=NULL
   and may want to know it was the fallback). */
static int keyset_agg_from_intersect(const char *db_root, const char *object,
                                     const Schema *sch, void *agg_ctx,
                                     const FilterPlan *fp, CriteriaNode *full_tree,
                                     CriteriaNode **agg_ctx_tree_field,
                                     QueryDeadline *dl) {
    int small_primary = 0;
    KeySet *ks = intersect_indexed_leaves(db_root, object, sch->splits,
                                          (SearchCriterion **)fp->source_leaves,
                                          fp->n_source, dl,
                                          &small_primary);
    if (!ks) return 0;

    if (small_primary || fp->n_postfilter > 0) {
        /* Restore tree so agg_scan_cb post-filters via criteria_match_tree.
           AggCtx lives further down the file — caller passes &ctx.tree to
           avoid a forward decl. Partial intersect: at least one AND child
           (bitmap, non-rangeable op, OR sub-tree, …) was dropped from the
           intersect and survives only in the criteria tree as a post-filter. */
        *agg_ctx_tree_field = full_tree;
    }
    keyset_emit_agg(db_root, object, sch, ks, agg_ctx, dl);
    keyset_free(ks);
    return small_primary;
}

/* ========== AGGREGATE ========== */

enum AggFn { AGG_COUNT, AGG_SUM, AGG_AVG, AGG_MIN, AGG_MAX };

typedef struct {
    enum AggFn fn;
    char field[256];
    char alias[256];
} AggSpec;

typedef struct {
    double sum;
    double min;
    double max;
    int64_t count;
} AggAccum;

/* Inline raw-key cap for the integer group_by fast path. Sized to fit
   common all-integer multi-field group_bys: 4×long, 8×int, or any mix
   summing to ≤32 bytes. Wider tuples fall back to the string-key path
   (use_int_keys gated at agg setup). */
#define AGG_INT_KEY_CAP 32

typedef struct AggBucket {
    char *group_key;            /* concatenated group values separated by \x1F */
    char **group_vals;          /* individual group values for output */
    AggAccum *accums;           /* one per aggregate */
    struct AggBucket *next;     /* hash chain */
    /* Integer key optimization: raw bytes for fast hash/compare */
    uint8_t raw_key[AGG_INT_KEY_CAP];  /* raw bytes for integer group keys */
    uint8_t raw_key_len;        /* length of raw key (0 = string key) */
} AggBucket;

/* Bump-allocator arena for AggBucket and its strings. The pre-arena path
   did 5+ malloc/strdup calls per bucket (~1.3 µs amortised on glibc) which
   serialised the indexed group_by Pass 1 walk on high-cardinality fields
   — at 1M unique varchar buckets it dominated wall time. The arena lets
   bucket creation amortise to ~50 ns by carving pre-allocated slabs;
   teardown is one free() per slab instead of N per bucket. */
typedef struct AggArenaSlab {
    struct AggArenaSlab *next;
    size_t used;
    size_t cap;
    /* buf[] follows the header in the same allocation */
} AggArenaSlab;

typedef struct {
    AggArenaSlab *head;   /* most recent slab; new allocs come from here */
} AggArena;

/* AggCtx hash table sizing. Starts at AGG_HT_INIT, doubles when load
   factor crosses 1.0 (total_buckets > ht_cap). Bounded by AGG_HT_MAX so
   a runaway query can't consume unlimited memory; once at AGG_HT_MAX we
   accept longer chains and rely on the per-query QUERY_BUFFER_MB cap to
   stop allocating buckets. AGG_HT_MAX = 1<<24 = 16M slots × 8 B = 128 MB
   in the worst case — the same scale as the existing query-buffer cap. */
#define AGG_HT_INIT  256u
#define AGG_HT_MAX   (1u << 24)
#define MAX_AGG_SPECS 32

static void *agg_arena_alloc(AggArena *a, size_t bytes) {
    bytes = (bytes + 7u) & ~(size_t)7;
    if (a->head && a->head->used + bytes <= a->head->cap) {
        char *base = (char *)(a->head + 1);
        void *p = base + a->head->used;
        a->head->used += bytes;
        return p;
    }
    /* Slab geometry: start at 64 KB, double thereafter; never below the
       request size. Linked-list of slabs so we can free them in bulk. */
    size_t prev_cap = a->head ? a->head->cap : 65536 / 2;
    size_t new_cap = prev_cap * 2;
    if (new_cap < bytes) new_cap = bytes;
    AggArenaSlab *s = malloc(sizeof(AggArenaSlab) + new_cap);
    if (!s) return NULL;
    s->next = a->head;
    s->used = bytes;
    s->cap = new_cap;
    a->head = s;
    return (char *)(s + 1);
}

/* Strdup-style helper that copies into the arena. */
static char *agg_arena_strdup(AggArena *a, const char *s, size_t sl) {
    char *out = agg_arena_alloc(a, sl + 1);
    if (!out) return NULL;
    if (sl > 0) memcpy(out, s, sl);
    out[sl] = '\0';
    return out;
}

static void agg_arena_free(AggArena *a) {
    AggArenaSlab *s = a->head;
    while (s) {
        AggArenaSlab *next = s->next;
        free(s);
        s = next;
    }
    a->head = NULL;
}

/* Splice src arena's slab chain onto dst, leaving dst's head (active alloc
   target) intact. Used by the parallel merge to transfer per-partition
   arena ownership into main ctx so partition buckets stay valid until
   agg_free time. */
static void agg_arena_transfer(AggArena *dst, AggArena *src) {
    if (!src->head) return;
    if (!dst->head) {
        dst->head = src->head;
    } else {
        AggArenaSlab *tail = dst->head;
        while (tail->next) tail = tail->next;
        tail->next = src->head;
    }
    src->head = NULL;
}

/* ========== Top-N streaming heap (Phase 1) ==========
 *
 * Fixed-capacity array-backed binary heap, inverted on the agg metric.
 *
 * order:desc (default top-N): MIN-heap on metric — root is the smallest
 *   kept value; offer() replaces the root if the new metric is greater.
 *   At the end, drain in DECREASING metric order (sort desc).
 * order:asc (bottom-N): MAX-heap — root is the largest kept value;
 *   replace root when smaller arrives. Drain in increasing metric order.
 *
 * Capacity is fixed at allocation; no resize. group_key is owned by
 * each entry and freed on eviction or destroy.
 */

typedef struct {
    double   metric;       /* sort key (count / sum / min / max) */
    char    *group_key;    /* owned varchar bytes — must free on evict */
    size_t   group_key_len;
    int64_t  count;        /* per-spec running state — fields used depend on spec */
    double   sum;
    double   min;
    double   max;
} TopNHeapEntry;

typedef struct {
    TopNHeapEntry *entries;  /* 1-indexed binary heap, [0] unused */
    int            cap;
    int            size;
    int            order_desc;  /* 1 = top-N (min-heap), 0 = bottom-N (max-heap) */
} TopNHeap;

#ifdef TEST_BUILD
#define TOPN_VIS
#else
#define TOPN_VIS static
#endif

TOPN_VIS void *topn_heap_new(int cap, int order_desc) {
    if (cap <= 0) return NULL;
    TopNHeap *h = calloc(1, sizeof(TopNHeap));
    if (!h) return NULL;
    h->entries = calloc((size_t)cap + 1, sizeof(TopNHeapEntry));
    if (!h->entries) { free(h); return NULL; }
    h->cap = cap;
    h->size = 0;
    h->order_desc = order_desc;
    return h;
}

TOPN_VIS void topn_heap_destroy(void *hp) {
    TopNHeap *h = (TopNHeap *)hp;
    if (!h) return;
    for (int i = 1; i <= h->size; i++) free(h->entries[i].group_key);
    free(h->entries);
    free(h);
}

TOPN_VIS int topn_heap_size(void *hp) {
    return hp ? ((TopNHeap *)hp)->size : 0;
}

/* For top-N (desc): heap is a MIN-heap on metric → root is smallest kept.
 *   new beats root iff new.metric > root.metric.
 * For bottom-N (asc): MAX-heap → root is largest kept.
 *   new beats root iff new.metric < root.metric. */
static int topn_metric_beats(const TopNHeap *h, double a, double b) {
    return h->order_desc ? (a > b) : (a < b);
}

/* Standard sift-down — children at 2i, 2i+1. "Worse" = closer to root in
 * inverted-heap terms. */
static void topn_sift_down(TopNHeap *h, int i) {
    for (;;) {
        int l = 2 * i, r = 2 * i + 1, worst = i;
        if (l <= h->size && topn_metric_beats(h, h->entries[worst].metric,
                                              h->entries[l].metric)) worst = l;
        if (r <= h->size && topn_metric_beats(h, h->entries[worst].metric,
                                              h->entries[r].metric)) worst = r;
        if (worst == i) return;
        TopNHeapEntry tmp = h->entries[i];
        h->entries[i] = h->entries[worst];
        h->entries[worst] = tmp;
        i = worst;
    }
}

static void topn_sift_up(TopNHeap *h, int i) {
    while (i > 1) {
        int p = i / 2;
        if (topn_metric_beats(h, h->entries[p].metric, h->entries[i].metric)) {
            TopNHeapEntry tmp = h->entries[i];
            h->entries[i] = h->entries[p];
            h->entries[p] = tmp;
            i = p;
        } else return;
    }
}

/* Returns 1 if the entry was accepted (added or replaced root), 0 if
 * rejected. group_key is copied (heap owns the copy). */
TOPN_VIS int topn_heap_offer(void *hp, double metric,
                              const char *gk, size_t gklen,
                              int64_t count, double sum, double min, double max) {
    TopNHeap *h = (TopNHeap *)hp;
    if (!h) return 0;
    if (h->size < h->cap) {
        h->size++;
        h->entries[h->size].metric = metric;
        h->entries[h->size].count = count;
        h->entries[h->size].sum = sum;
        h->entries[h->size].min = min;
        h->entries[h->size].max = max;
        h->entries[h->size].group_key = malloc(gklen + 1);
        if (!h->entries[h->size].group_key) { h->size--; return 0; }
        memcpy(h->entries[h->size].group_key, gk, gklen);
        h->entries[h->size].group_key[gklen] = '\0';
        h->entries[h->size].group_key_len = gklen;
        topn_sift_up(h, h->size);
        return 1;
    }
    /* Full — compare against root. */
    if (!topn_metric_beats(h, metric, h->entries[1].metric)) return 0;
    free(h->entries[1].group_key);
    h->entries[1].metric = metric;
    h->entries[1].count = count;
    h->entries[1].sum = sum;
    h->entries[1].min = min;
    h->entries[1].max = max;
    h->entries[1].group_key = malloc(gklen + 1);
    if (!h->entries[1].group_key) { h->size--; return 0; }
    memcpy(h->entries[1].group_key, gk, gklen);
    h->entries[1].group_key[gklen] = '\0';
    h->entries[1].group_key_len = gklen;
    topn_sift_down(h, 1);
    return 1;
}

/* Drain into caller-provided arrays (must be sized >= cap). Returns
 * the number of entries written. Output is sorted by the user's
 * order_by direction (desc → metrics decreasing, asc → metrics
 * increasing). Heap is empty after drain. Caller takes ownership of
 * each gkeys_out[i] and must free(). */
TOPN_VIS int topn_heap_drain(void *hp, double *metrics_out,
                              char **gkeys_out, size_t *gklens_out,
                              int64_t *counts_out, double *sums_out,
                              double *mins_out, double *maxs_out) {
    TopNHeap *h = (TopNHeap *)hp;
    if (!h || h->size == 0) return 0;
    int n = h->size;
    /* Repeatedly extract-root → produces sorted ascending order
     * for top-N (min-heap) and descending for bottom-N (max-heap).
     * Reverse on the way out so output direction matches user's
     * order_by. */
    int idx = n;
    while (h->size > 0) {
        idx--;
        metrics_out[idx]  = h->entries[1].metric;
        gkeys_out[idx]    = h->entries[1].group_key;  /* transfer */
        gklens_out[idx]   = h->entries[1].group_key_len;
        counts_out[idx]   = h->entries[1].count;
        sums_out[idx]     = h->entries[1].sum;
        mins_out[idx]     = h->entries[1].min;
        maxs_out[idx]     = h->entries[1].max;
        h->entries[1] = h->entries[h->size];
        h->size--;
        if (h->size > 0) topn_sift_down(h, 1);
    }
    return n;
}

/* ========== Top-N streaming eligibility (Phase 1) ==========
 *
 * Pure shape + index-presence check. Phase 1 fires for:
 *   - single-field group_by
 *   - field has IT_BTREE index
 *   - order_by references an aggregate alias (not the group_by field)
 *   - limit in (0, 10000]
 *   - no HAVING clause
 *   - every spec is count() or sum/min/max on the group_by field itself
 *
 * Phase 2 (composite-covered agg on different field) and Phase 3
 * (multi-field group_by via composite) relax conditions in this same
 * function in later tasks.
 */
#ifdef TEST_BUILD
#define TOPN_ELIG_VIS
#else
#define TOPN_ELIG_VIS static
#endif

TOPN_ELIG_VIS int eligible_for_topn_stream(
    const char *db_root, const char *object,
    const AggSpec *specs, int nspecs,
    const char *group_by_csv,
    const char *order_by_alias,
    int limit,
    const char *having)
{
    if (!specs || nspecs <= 0) return 0;
    if (!group_by_csv || !*group_by_csv) return 0;
    if (!order_by_alias || !*order_by_alias) return 0;
    if (limit <= 0 || limit > 10000) return 0;
    if (having && *having) return 0;

    /* Single group_by field only (Phase 1). */
    if (strchr(group_by_csv, ',')) return 0;
    const char *gb_field = group_by_csv;

    /* order_by must reference an aggregate alias, not the group_by field. */
    if (strcmp(order_by_alias, gb_field) == 0) return 0;
    int matching_spec = -1;
    for (int i = 0; i < nspecs; i++) {
        if (strcmp(specs[i].alias, order_by_alias) == 0) {
            matching_spec = i;
            break;
        }
    }
    if (matching_spec < 0) return 0;

    /* group_by field must have a btree index. */
    if (!field_has_index_type(db_root, object, gb_field, IT_BTREE)) return 0;

    /* Phase 1: every spec is count() OR agg on the group_by field itself. */
    for (int i = 0; i < nspecs; i++) {
        if (specs[i].fn == AGG_COUNT) continue;
        if (specs[i].field[0] && strcmp(specs[i].field, gb_field) == 0) continue;
        return 0;
    }

    return 1;
}

/* Forward declarations for helpers defined later in this file that the
 * streaming top-N executor needs. */
static int  decode_index_key_to_double(const TypedField *f,
                                        const uint8_t *key, size_t klen,
                                        double *out);
static void fmt_double(char *buf, size_t sz, double v);

/* ========== Streaming top-N aggregate executor (Phase 1) ==========
 *
 * Pre: eligible_for_topn_stream returned 1.
 *
 * Walks the group_by field's btree in global value-sorted order via
 * btree_idx_walk_ordered (streaming k-way merge of per-shard
 * BtRangeIters). Run-length finalizes each group on value-change;
 * keeps a K-element top-N heap on the order_by spec's metric.
 * Memory O(K). No seg-file reads for pure count().
 *
 * Returns  0 on success (output written via OUT()),
 *         -1 on timeout/alloc error,
 *         -2 when caller should fall back (non-indexable criteria).
 */

typedef struct {
    char        *current_key;
    size_t       current_key_len;
    int64_t      current_count;
    double       current_sum;
    double       current_min;
    double       current_max;
    int          current_min_set;
    int          current_max_set;

    const AggSpec    *specs;
    int               nspecs;
    int               order_spec_idx;
    void             *heap;
    KeySet           *prefilter;
    int               prefilter_inverted; /* 1 ⇒ count when hash NOT in prefilter (complement set) */
    QueryDeadline    *dl;
    int               dl_counter;
    const TypedField *gb_tf;   /* for future sum/min/max on the group field */
} TopNWalkCtx;

static double topn_metric_from_state(const TopNWalkCtx *c) {
    const AggSpec *s = &c->specs[c->order_spec_idx];
    switch (s->fn) {
        case AGG_COUNT: return (double)c->current_count;
        case AGG_SUM:   return c->current_sum;
        case AGG_MIN:   return c->current_min_set ? c->current_min : 0.0;
        case AGG_MAX:   return c->current_max_set ? c->current_max : 0.0;
        case AGG_AVG:   return c->current_count > 0
                              ? c->current_sum / (double)c->current_count : 0.0;
        default:        return 0.0;
    }
}

static void topn_finalize_run(TopNWalkCtx *c) {
    if (!c->current_key) return;
    double metric = topn_metric_from_state(c);
    topn_heap_offer(c->heap, metric,
                    c->current_key, c->current_key_len,
                    c->current_count, c->current_sum,
                    c->current_min_set ? c->current_min : 0.0,
                    c->current_max_set ? c->current_max : 0.0);
    free(c->current_key);
    c->current_key     = NULL;
    c->current_key_len = 0;
    c->current_count   = 0;
    c->current_sum     = 0.0;
    c->current_min     = 0.0;
    c->current_max     = 0.0;
    c->current_min_set = 0;
    c->current_max_set = 0;
}

static int topn_walk_cb(const char *enc_val, size_t enc_val_len,
                        const uint8_t *hash16, void *ctx_v) {
    TopNWalkCtx *c = (TopNWalkCtx *)ctx_v;

    if (query_deadline_tick(c->dl, &c->dl_counter)) return -1;
    if (c->prefilter) {
        int in = keyset_contains(c->prefilter, hash16);
        /* Normal prefilter: skip rows NOT in the match-set. Inverted
           (complement) prefilter: skip rows that ARE in the complement. */
        if (c->prefilter_inverted ? in : !in) return 0;
    }

    int new_group = 0;
    if (!c->current_key) {
        new_group = 1;
    } else if (c->current_key_len != enc_val_len ||
               memcmp(c->current_key, enc_val, enc_val_len) != 0) {
        topn_finalize_run(c);
        new_group = 1;
    }

    if (new_group) {
        c->current_key = malloc(enc_val_len + 1);
        if (!c->current_key) return -1;
        memcpy(c->current_key, enc_val, enc_val_len);
        c->current_key[enc_val_len] = '\0';
        c->current_key_len = enc_val_len;
        c->current_count   = 0;
        c->current_sum     = 0.0;
        c->current_min     = 0.0;
        c->current_max     = 0.0;
        c->current_min_set = 0;
        c->current_max_set = 0;
    }
    c->current_count++;

    /* If the group_by field is numeric, decode the leaf bytes and
     * update sum/min/max for specs that aggregate the group_by field
     * itself. Varchar group_by → numeric specs would be a no-op
     * anyway; skip decode to avoid false positives. */
    if (c->gb_tf && c->gb_tf->type != FT_VARCHAR) {
        double v;
        if (decode_index_key_to_double(c->gb_tf,
                                        (const uint8_t *)enc_val, enc_val_len,
                                        &v)) {
            c->current_sum += v;
            if (!c->current_min_set || v < c->current_min) {
                c->current_min = v;
                c->current_min_set = 1;
            }
            if (!c->current_max_set || v > c->current_max) {
                c->current_max = v;
                c->current_max_set = 1;
            }
        }
    }
    return 0;
}

#ifdef TEST_BUILD
#define TOPN_RUN_VIS
#else
#define TOPN_RUN_VIS static
#endif

TOPN_RUN_VIS int agg_run_topn_stream(const char *db_root, const char *object,
                                      const Schema *sch, FieldSchema *fs,
                                      const AggSpec *specs, int nspecs,
                                      const char *group_by_field,
                                      const char *order_by_alias,
                                      int order_desc,
                                      int limit,
                                      CriteriaNode *tree,
                                      QueryDeadline *dl,
                                      int want_total)
{
    int order_spec_idx = -1;
    for (int i = 0; i < nspecs; i++) {
        if (strcmp(specs[i].alias, order_by_alias) == 0) {
            order_spec_idx = i;
            break;
        }
    }
    if (order_spec_idx < 0) return -1;

    void *heap = topn_heap_new(limit, order_desc);
    if (!heap) return -1;

    /* Build prefilter KeySet from criteria, or fall back if un-indexable. */
    KeySet *prefilter = NULL;
    int prefilter_inverted = 0;
    if (tree) {
        /* Phase 1c.5/1c.6: plan_filter replaces choose_primary_source.
         * order_by=NULL: the streaming top-N walk sorts by the aggregate
         * spec alias, not by an input-row field — D1/D3 overlays irrelevant.
         * fetching=0: the walk reads index leaves only, no record fetch. */
        size_t topn_N = (size_t)get_live_count(db_root, object);
        FilterPlan topn_fp = plan_filter(tree, db_root, object, fs,
                                          sch->splits, topn_N,
                                          NULL /*order_by*/, 0 /*fetching*/,
                                          0 /*limit — top-N has its own limit semantics*/);
        if (topn_fp.kind == FP_FULL_SCAN) {
            topn_heap_destroy(heap);
            return -2;  /* criteria exist but no usable index — fall back */
        }
        /* The streaming walk has NO per-record post-filter step (no record is
           fetched), so it may only trust a prefilter that represents the
           ENTIRE criteria tree. n_postfilter == 0 means every leaf is in
           the source set and nothing is left to verify. Fall back to the
           scan+hashmap path when any postfilter leaf remains. */
        if (topn_fp.n_postfilter > 0) {
            topn_heap_destroy(heap);
            return -2;  /* prefilter wouldn't cover all criteria — scan applies the full tree */
        }
        if (topn_fp.kind == FP_INTERSECT) {
            int sp = 0;
            prefilter = intersect_indexed_leaves(db_root, object, sch->splits,
                                                  topn_fp.source_leaves,
                                                  topn_fp.n_source,
                                                  dl, &sp);
            /* small_primary: intersect_indexed_leaves returned ONLY the
               smallest leaf's keyset (the rest expect a per-record
               post-filter, which the walk can't do). The prefilter is
               partial → fall back to agg_run_plan, which fetches + rechecks. */
            if (sp) {
                if (prefilter) keyset_free(prefilter);
                topn_heap_destroy(heap);
                return -2;
            }
        } else if (topn_fp.kind == FP_PRIMARY_LEAF || topn_fp.kind == FP_BITMAP_SMALLER) {
            SearchCriterion *prim = topn_fp.n_source > 0 ? topn_fp.source_leaves[0] : NULL;
            /* A bitmap eq/IN primary would materialise EVERY matching hash —
               crippling when the value is the majority (e.g. type='story').
               Build the smaller of {match-set, complement} instead; the walk
               inverts membership when we built the complement. */
            if (prim && (prim->op == OP_EQUAL || prim->op == OP_IN) &&
                field_has_index_type(db_root, object, prim->field, IT_BITMAP)) {
                prefilter = build_smaller_bitmap_keyset(
                    db_root, object, sch->splits, prim,
                    resolve_idx_field(fs->ts, prim->field),
                    dl, &prefilter_inverted);
            } else {
                prefilter = build_keyset_from_leaf(db_root, object, sch->splits,
                                                   prim, dl);
            }
        } else if (topn_fp.kind == FP_UNION) {
            int budget = 0;
            prefilter = build_or_keyset(db_root, object, sch->splits,
                                        topn_fp.or_node, dl, &budget, 0);
        } else {
            topn_heap_destroy(heap);
            return -2;
        }
        if (dl->timed_out) {
            if (prefilter) keyset_free(prefilter);
            topn_heap_destroy(heap);
            return -1;
        }
        /* Criteria existed but no prefilter could be built (budget overflow
           or builder error). Do NOT walk unfiltered — that silently drops
           the criterion and over-counts. Fall back to the scan+hashmap path
           which evaluates criteria per record. */
        if (!prefilter) {
            topn_heap_destroy(heap);
            return -2;
        }
    }

    TopNWalkCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.specs          = specs;
    ctx.nspecs         = nspecs;
    ctx.order_spec_idx = order_spec_idx;
    ctx.heap           = heap;
    ctx.prefilter      = prefilter;
    ctx.prefilter_inverted = prefilter_inverted;
    ctx.dl             = dl;
    ctx.gb_tf          = resolve_idx_field(fs->ts, group_by_field);

    btree_idx_walk_ordered(db_root, object, group_by_field, sch->splits,
                            "", 0, 0,
                            "\xff\xff\xff\xff", 4, 0,
                            0 /* asc */,
                            topn_walk_cb, &ctx);

    topn_finalize_run(&ctx);
    free(ctx.current_key);  /* in case callback returned early */
    if (prefilter) keyset_free(prefilter);

    if (dl->timed_out) {
        topn_heap_destroy(heap);
        return -1;
    }

    int n_out = topn_heap_size(heap);
    double  *metrics = calloc((size_t)n_out + 1, sizeof(double));
    char   **gkeys   = calloc((size_t)n_out + 1, sizeof(char *));
    size_t  *gklens  = calloc((size_t)n_out + 1, sizeof(size_t));
    int64_t *counts  = calloc((size_t)n_out + 1, sizeof(int64_t));
    double  *sums    = calloc((size_t)n_out + 1, sizeof(double));
    double  *mins    = calloc((size_t)n_out + 1, sizeof(double));
    double  *maxs    = calloc((size_t)n_out + 1, sizeof(double));
    if (!metrics || !gkeys || !gklens || !counts || !sums || !mins || !maxs) {
        free(metrics); free(gkeys); free(gklens);
        free(counts); free(sums); free(mins); free(maxs);
        topn_heap_destroy(heap);
        return -1;
    }
    int n = topn_heap_drain(heap, metrics, gkeys, gklens, counts, sums, mins, maxs);
    topn_heap_destroy(heap);

    /* Determine if the group_by field's btree stores raw bytes (varchar)
     * or encoded numeric bytes. For varchar, gkeys[i] is the raw string.
     * For numeric types, decode via decode_index_key_to_double + format. */
    int gb_is_varchar = ctx.gb_tf && ctx.gb_tf->type == FT_VARCHAR;

    OUT(want_total ? "{\"rows\":[" : "[");
    for (int i = 0; i < n; i++) {
        if (i > 0) OUT(",");
        OUT("{");

        /* Emit the group_by field value. */
        char val_buf[1032];
        int  vl = 0;
        if (gb_is_varchar) {
            /* Index stores raw string content for varchar. */
            vl = (int)gklens[i];
            if (vl > (int)(sizeof(val_buf) - 1)) vl = (int)(sizeof(val_buf) - 1);
            memcpy(val_buf, gkeys[i], (size_t)vl);
            val_buf[vl] = '\0';
            char *esc = json_escape_const(val_buf);
            OUT("\"%s\":\"%s\"", group_by_field, esc ? esc : "");
            free(esc);
        } else if (ctx.gb_tf) {
            /* Numeric index key — decode to double and format as quoted
             * string to match the existing IGB/scan-path output shape
             * where group_vals are always emitted as JSON strings. */
            double dv = 0.0;
            if (decode_index_key_to_double(ctx.gb_tf, (const uint8_t *)gkeys[i],
                                            gklens[i], &dv)) {
                char dbuf[64];
                fmt_double(dbuf, sizeof(dbuf), dv);
                OUT("\"%s\":\"%s\"", group_by_field, dbuf);
            } else {
                OUT("\"%s\":null", group_by_field);
            }
        } else {
            /* Unknown type — emit raw bytes as string (safe for ASCII). */
            vl = (int)gklens[i];
            if (vl > (int)(sizeof(val_buf) - 1)) vl = (int)(sizeof(val_buf) - 1);
            memcpy(val_buf, gkeys[i], (size_t)vl);
            val_buf[vl] = '\0';
            char *esc = json_escape_const(val_buf);
            OUT("\"%s\":\"%s\"", group_by_field, esc ? esc : "");
            free(esc);
        }

        /* Emit aggregate values. */
        for (int s = 0; s < nspecs; s++) {
            char vbuf[64];
            switch (specs[s].fn) {
                case AGG_COUNT:
                    OUT(",\"%s\":%lld", specs[s].alias, (long long)counts[i]);
                    break;
                case AGG_SUM:
                    fmt_double(vbuf, sizeof(vbuf), sums[i]);
                    OUT(",\"%s\":%s", specs[s].alias, vbuf);
                    break;
                case AGG_MIN:
                    fmt_double(vbuf, sizeof(vbuf), mins[i]);
                    OUT(",\"%s\":%s", specs[s].alias, vbuf);
                    break;
                case AGG_MAX:
                    fmt_double(vbuf, sizeof(vbuf), maxs[i]);
                    OUT(",\"%s\":%s", specs[s].alias, vbuf);
                    break;
                case AGG_AVG: {
                    double v = counts[i] > 0 ? sums[i] / (double)counts[i] : 0.0;
                    fmt_double(vbuf, sizeof(vbuf), v);
                    OUT(",\"%s\":%s", specs[s].alias, vbuf);
                    break;
                }
            }
        }
        OUT("}");
        free(gkeys[i]);
    }
    OUT(want_total ? "],\"total\":null}\n" : "]\n");

    free(metrics); free(gkeys); free(gklens);
    free(counts); free(sums); free(mins); free(maxs);
    return 0;
}

typedef struct {
    CriteriaNode *tree;
    FieldSchema *fs;
    /* group_by */
    char group_fields[MAX_FIELDS][256];
    int ngroups;
    const TypedField *group_tfs[MAX_FIELDS];  /* resolved typed field per group — NULL = composite/unknown */
    /* aggregates */
    AggSpec *specs;
    int nspecs;
    const TypedField *spec_tfs[MAX_AGG_SPECS]; /* resolved typed field per agg spec — NULL = count or composite */
    /* Dynamic hash table — grows on demand to keep load factor ≤ 1.0
       so chain length stays O(1). Replaces a fixed 16384-bucket array
       that became the bottleneck at high cardinality (1M unique values
       → 61-deep chains → 3 µs per insert via per-bucket strcmp). */
    AggBucket **ht;
    size_t      ht_cap;        /* power of 2 */
    size_t      ht_mask;       /* ht_cap - 1 */
    int total_buckets;
    QueryDeadline *deadline;
    int dl_counter;
    /* QUERY_BUFFER_MB accounting. Parallel workers share a single atomic counter
       (pointed to by shared_buffer_bytes); serial path reads/writes it like a
       plain size_t. budget_exceeded flips per-ctx once the cap is hit. */
    _Atomic size_t *shared_buffer_bytes;
    int budget_exceeded;
    /* Slab arena for AggBucket + group_key + group_vals + accums. All
       per-bucket allocations live here so teardown is O(slabs) not
       O(buckets) and bucket creation skips ~5 malloc/strdup calls. Per-
       worker AggCtx clones each have their own arena; the merge path
       copies what it needs into the destination's arena and frees the
       source's arena en masse. */
    AggArena arena;
    /* Integer group key optimization: 1 if all group fields are integer types */
    int use_int_keys;
} AggCtx;

/* Write a typed field's string form into a caller-provided buffer (no malloc).
   Returns the length written. Returns 0 for "empty" fields (0-value LONG/INT/SHORT/
   DOUBLE/DATE/DATETIME/zero-length VARCHAR) to preserve legacy skip-empty behavior.
   FT_NUMERIC and FT_BOOL always return non-zero. */
int typed_field_to_buf_raw(const TypedField *f, const uint8_t *p,
                                  char *buf, size_t bufsz) {
    switch (f->type) {
    case FT_VARCHAR: {
        int len = varchar_eff_len(p, f->size);
        if (len == 0) return 0;
        if ((size_t)len >= bufsz) len = bufsz - 1;
        memcpy(buf, p + 2, len);
        buf[len] = '\0';
        return len;
    }
    case FT_LONG: {
        int64_t v = ld_be_i64(p);
        if (v == 0) return 0;
        return snprintf(buf, bufsz, "%lld", (long long)v);
    }
    case FT_INT: {
        int32_t v = ld_be_i32(p);
        if (v == 0) return 0;
        return snprintf(buf, bufsz, "%d", v);
    }
    case FT_SHORT: {
        int16_t v = ld_be_i16(p);
        if (v == 0) return 0;
        return snprintf(buf, bufsz, "%d", v);
    }
    case FT_DOUBLE: {
        double v; memcpy(&v, p, 8);
        if (v == 0.0) return 0;
        return snprintf(buf, bufsz, "%g", v);
    }
    case FT_FLOAT: {
        float v; memcpy(&v, p, 4);
        if (v == 0.0f) return 0;
        return snprintf(buf, bufsz, "%g", (double)v);
    }
    case FT_BOOL:
        return snprintf(buf, bufsz, "%s", p[0] ? "true" : "false");
    case FT_BYTE:
        return snprintf(buf, bufsz, "%u", p[0]);
    case FT_NUMERIC: {
        int64_t v = ld_be_i64(p);
        if (v == 0) { buf[0] = '0'; buf[1] = '\0'; return 1; }
        int64_t scale = f->numeric_scale_mult;
        int64_t whole = v / scale;
        int64_t frac = v % scale;
        int neg = (v < 0);
        if (frac < 0) frac = -frac;
        if (whole < 0) whole = -whole;
        if (frac == 0)
            return snprintf(buf, bufsz, "%s%lld", neg ? "-" : "", (long long)whole);
        return snprintf(buf, bufsz, "%s%lld.%0*lld", neg ? "-" : "",
                        (long long)whole, f->numeric_scale, (long long)frac);
    }
    case FT_DATE: {
        int32_t v = ld_be_i32(p);
        if (v == 0) return 0;
        return snprintf(buf, bufsz, "%08d", v);
    }
    case FT_DATETIME: {
        int32_t d = ld_be_i32(p);
        uint16_t t = ld_be_u16(p + 4);
        if (d == 0 && t == 0) return 0;
        int hh = t / 3600, mm = (t % 3600) / 60, ss = t % 60;
        return snprintf(buf, bufsz, "%08d%02d%02d%02d", d, hh, mm, ss);
    }
    case FT_DATETIMEMS: {
        int32_t d = ld_be_i32(p);
        uint32_t ms = ld_be_u32(p + 4);
        if (d == 0 && ms == 0) return 0;
        int hh = ms / 3600000, mm = (ms % 3600000) / 60000,
            ss = (ms % 60000) / 1000, fff = ms % 1000;
        return snprintf(buf, bufsz, "%08d%02d%02d%02d%03d", d, hh, mm, ss, fff);
    }
    case FT_TIME: {
        uint32_t secs = ((uint32_t)p[0] << 16) | ((uint32_t)p[1] << 8) | p[2];
        if (secs == 0 && p[0]==0 && p[1]==0 && p[2]==0) return 0;
        int hh = secs / 3600, mm = (secs % 3600) / 60, ss = secs % 60;
        return snprintf(buf, bufsz, "%02d:%02d:%02d", hh, mm, ss);
    }
    case FT_UUID: {
        /* Same as decode_field_to_buf - canonical form */
        const uint8_t *b = (const uint8_t *)p;
        if (uuid_is_zero(b)) return 0;
        return uuid_format_canonical(buf, bufsz, b);
    }
    case FT_IPV4: {
        if (p[0] == 0 && p[1] == 0 && p[2] == 0 && p[3] == 0) return 0;
        char ipstr[INET_ADDRSTRLEN];
        if (!inet_ntop(AF_INET, p, ipstr, sizeof(ipstr))) return 0;
        return snprintf(buf, bufsz, "%s", ipstr);
    }
    case FT_IPV6: {
        int allzero = 1;
        for (int bi = 0; bi < 16; bi++) if (p[bi] != 0) { allzero = 0; break; }
        if (allzero) return 0;
        char ipstr[INET6_ADDRSTRLEN];
        if (!inet_ntop(AF_INET6, p, ipstr, sizeof(ipstr))) return 0;
        return snprintf(buf, bufsz, "%s", ipstr);
    }
    case FT_ENUM: {
        /* Stored bytes are the byte index. Look up enum_values[idx]
           and copy. Out-of-range index (data corruption) → empty string. */
        if (!f->enum_values || f->n_enum_values <= 0) return 0;
        int idx = (f->enum_width == 2)
                    ? (int)(((uint16_t)p[0] << 8) | (uint16_t)p[1])
                    : (int)p[0];
        if (idx < 0 || idx >= f->n_enum_values) return 0;
        const char *s = f->enum_values[idx];
        if (!s) return 0;
        size_t sl = strlen(s);
        if (sl >= bufsz) sl = bufsz - 1;
        memcpy(buf, s, sl);
        buf[sl] = '\0';
        return (int)sl;
    }
    default:
        return 0;
    }
}

/* Decode a btree index leaf entry's encoded bytes back to the original
   numeric value, matching what typed_field_to_double would emit if given
   the typed-record bytes for the same field. Used by the indexed-walk
   aggregate fast path so sum/avg/min/max can read directly from the
   btree without a per-record slot lookup.

   Encoding inverses (mirrors encode_field_for_index in config.c):
     LONG/INT/SHORT/DATE: BE signed-int with top bit XOR'd → undo XOR.
     DOUBLE: IEEE-754 total-order encoding → if top bit set (was positive)
             flip top bit; else flip all bits.
     DATETIME: 4 BE bytes int32 (top-bit-flipped) date + 2 BE bytes uint16
               seconds-of-day; output matches typed_field_to_double's
               "d*1e6 + t" form (t is left as raw seconds-of-day, same
               as the typed-record path which reads the field offset bytes).
     NUMERIC: BE int64 top-bit-flipped, divided by 10^scale.
     BOOL/BYTE: single byte stored directly.
   Returns 1 on success / 0 when the encoded buffer is too short for the
   field type (skipped by callers, matching the typed_field_to_double
   "missing field" semantics). Skip varchar — degenerate atof on names. */
static int decode_index_key_to_double(const TypedField *f,
                                      const uint8_t *p, size_t plen,
                                      double *out) {
    switch (f->type) {
    case FT_LONG: {
        if (plen < 8) return 0;
        uint64_t u = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                     ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                     ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                     ((uint64_t)p[6] << 8)  |  (uint64_t)p[7];
        int64_t v = (int64_t)(u ^ (1ULL << 63));
        if (v == 0) return 0;
        *out = (double)v; return 1;
    }
    case FT_INT: {
        if (plen < 4) return 0;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t v = (int32_t)(u ^ 0x80000000u);
        if (v == 0) return 0;
        *out = (double)v; return 1;
    }
    case FT_SHORT: {
        if (plen < 2) return 0;
        uint16_t u = ((uint16_t)p[0] << 8) | (uint16_t)p[1];
        int16_t v = (int16_t)(u ^ 0x8000u);
        if (v == 0) return 0;
        *out = (double)v; return 1;
    }
    case FT_DOUBLE: {
        if (plen < 8) return 0;
        uint64_t bits = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                        ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                        ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                        ((uint64_t)p[6] << 8)  |  (uint64_t)p[7];
        if (bits & (1ULL << 63)) bits ^= (1ULL << 63);
        else bits = ~bits;
        double v; memcpy(&v, &bits, 8);
        if (v == 0.0) return 0;
        *out = v; return 1;
    }
    case FT_FLOAT: {
        if (plen < 4) return 0;
        uint32_t bits = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                        ((uint32_t)p[2] << 8)  | (uint32_t)p[3];
        if (bits & 0x80000000u) bits ^= 0x80000000u;
        else bits = ~bits;
        float v; memcpy(&v, &bits, 4);
        if (v == 0.0f) return 0;
        *out = (double)v; return 1;
    }
    case FT_BOOL:
        if (plen < 1) return 0;
        *out = (double)(p[0] ? 1 : 0); return 1;
    case FT_BYTE:
        if (plen < 1) return 0;
        *out = (double)p[0]; return 1;
    case FT_DATE: {
        if (plen < 4) return 0;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t v = (int32_t)(u ^ 0x80000000u);
        if (v == 0) return 0;
        *out = (double)v; return 1;
    }
    case FT_DATETIME: {
        if (plen < 6) return 0;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t d = (int32_t)(u ^ 0x80000000u);
        uint16_t t = ((uint16_t)p[4] << 8) | (uint16_t)p[5];
        if (d == 0 && t == 0) return 0;
        *out = (double)d * 1000000.0 + (double)t; return 1;
    }
    case FT_DATETIMEMS: {
        if (plen < 8) return 0;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t d = (int32_t)(u ^ 0x80000000u);
        uint32_t ms = ((uint32_t)p[4] << 24) | ((uint32_t)p[5] << 16) |
                      ((uint32_t)p[6] << 8)  |  (uint32_t)p[7];
        if (d == 0 && ms == 0) return 0;
        *out = (double)d * 100000000.0 + (double)ms;
        return 1;
    }
    case FT_TIME: return 0;  /* not summable */
    case FT_NUMERIC: {
        if (plen < 8) return 0;
        uint64_t u = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                     ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                     ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                     ((uint64_t)p[6] << 8)  |  (uint64_t)p[7];
        int64_t v = (int64_t)(u ^ (1ULL << 63));
        *out = (double)v / (double)f->numeric_scale_mult; return 1;
    }
    case FT_UUID:
        /* UUIDs aren't summable */
        return 0;
    case FT_IPV4:
        /* IPv4 addresses aren't summable. */
        return 0;
    case FT_IPV6:
        /* IPv6 addresses aren't summable. */
        return 0;
    default: return 0;
    }
}

/* Mirror of typed_field_to_buf_raw, but reads from a btree leaf entry's
   encoded bytes (post encode_field_for_index). Output is byte-identical
   to what typed_field_to_buf_raw produces from the typed-record bytes,
   so bucket keys built via the indexed group_by fast path collide with
   buckets the per-record agg_scan_cb path would create on the same
   data. Returns the length written; 0 on missing/zero (matches the
   "missing" semantics). Varchar is handled via direct byte copy — the
   index stores raw varchar content, so encoded bytes ARE the original. */
int decode_idx_to_buf(const TypedField *f, const uint8_t *p, size_t plen,
                              char *buf, size_t bufsz, int skip_zero) {
    if (!f || !p) return 0;
    switch (f->type) {
    case FT_LONG: {
        if (plen < 8) return 0;
        uint64_t u = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                     ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                     ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                     ((uint64_t)p[6] << 8)  |  (uint64_t)p[7];
        int64_t v = (int64_t)(u ^ (1ULL << 63));
        if (skip_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%lld", (long long)v);
    }
    case FT_INT: {
        if (plen < 4) return 0;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t v = (int32_t)(u ^ 0x80000000u);
        if (skip_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%d", v);
    }
    case FT_SHORT: {
        if (plen < 2) return 0;
        uint16_t u = ((uint16_t)p[0] << 8) | (uint16_t)p[1];
        int16_t v = (int16_t)(u ^ 0x8000u);
        if (skip_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%d", v);
    }
    case FT_DOUBLE: {
        if (plen < 8) return 0;
        uint64_t bits = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                        ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                        ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                        ((uint64_t)p[6] << 8)  |  (uint64_t)p[7];
        if (bits & (1ULL << 63)) bits ^= (1ULL << 63);
        else bits = ~bits;
        double v; memcpy(&v, &bits, 8);
        if (skip_zero && v == 0.0) return 0;
        return snprintf(buf, bufsz, "%g", v);
    }
    case FT_FLOAT: {
        if (plen < 4) return 0;
        uint32_t bits = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                        ((uint32_t)p[2] << 8) | (uint32_t)p[3];
        if (bits & 0x80000000u) bits ^= 0x80000000u;
        else bits = ~bits;
        float v; memcpy(&v, &bits, 4);
        if (skip_zero && v == 0.0f) return 0;
        return snprintf(buf, bufsz, "%g", (double)v);
    }
    case FT_BOOL:
        if (plen < 1) return 0;
        return snprintf(buf, bufsz, "%s", p[0] ? "true" : "false");
    case FT_BYTE:
        if (plen < 1) return 0;
        return snprintf(buf, bufsz, "%u", p[0]);
    case FT_DATE: {
        if (plen < 4) return 0;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t v = (int32_t)(u ^ 0x80000000u);
        if (skip_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%08d", v);
    }
    case FT_DATETIME: {
        if (plen < 6) return 0;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t d = (int32_t)(u ^ 0x80000000u);
        uint16_t t = ((uint16_t)p[4] << 8) | (uint16_t)p[5];
        if (skip_zero && d == 0 && t == 0) return 0;
        int hh = t / 3600, mm = (t % 3600) / 60, ss = t % 60;
        return snprintf(buf, bufsz, "%08d%02d%02d%02d", d, hh, mm, ss);
    }
    case FT_DATETIMEMS: {
        if (plen < 8) return 0;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
        int32_t d = (int32_t)(u ^ 0x80000000u);
        uint32_t ms = ((uint32_t)p[4] << 24) | ((uint32_t)p[5] << 16) |
                      ((uint32_t)p[6] << 8)  |  (uint32_t)p[7];
        if (skip_zero && d == 0 && ms == 0) return 0;
        int hh = ms / 3600000, mm = (ms % 3600000) / 60000,
            ss = (ms % 60000) / 1000, fff = ms % 1000;
        return snprintf(buf, bufsz, "%08d%02d%02d%02d%03d", d, hh, mm, ss, fff);
    }
    case FT_TIME: {
        /* Index stores 3 bytes with top-bit flip - undo flip and format */
        if (plen < 3) return 0;
        uint32_t u = ((uint32_t)p[0] << 16) | ((uint32_t)p[1] << 8) | p[2];
        uint32_t secs = u ^ 0x800000u;
        if (skip_zero && secs == 0 && p[0]==0 && p[1]==0 && p[2]==0) return 0;
        int hh = secs / 3600, mm = (secs % 3600) / 60, ss = secs % 60;
        return snprintf(buf, bufsz, "%02d:%02d:%02d", hh, mm, ss);
    }
    case FT_UUID: {
        /* Index stores raw 16 bytes - decode to canonical form */
        if (plen < 16) return 0;
        const uint8_t *b = p;
        if (uuid_is_zero(b)) return 0;
        return uuid_format_canonical(buf, bufsz, b);
    }
    case FT_IPV4: {
        if (p[0] == 0 && p[1] == 0 && p[2] == 0 && p[3] == 0) return 0;
        char ipstr[INET_ADDRSTRLEN];
        if (!inet_ntop(AF_INET, p, ipstr, sizeof(ipstr))) return 0;
        return snprintf(buf, bufsz, "%s", ipstr);
    }
    case FT_IPV6: {
        int allzero = 1;
        for (int bi = 0; bi < 16; bi++) if (p[bi] != 0) { allzero = 0; break; }
        if (allzero) return 0;
        char ipstr[INET6_ADDRSTRLEN];
        if (!inet_ntop(AF_INET6, p, ipstr, sizeof(ipstr))) return 0;
        return snprintf(buf, bufsz, "%s", ipstr);
    }
    case FT_NUMERIC: {
        if (plen < 8) return 0;
        uint64_t u = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                     ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                     ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                     ((uint64_t)p[6] << 8)  |  (uint64_t)p[7];
        int64_t v = (int64_t)(u ^ (1ULL << 63));
        if (v == 0) { buf[0] = '0'; buf[1] = '\0'; return 1; }
        int64_t scale = f->numeric_scale_mult;
        int64_t whole = v / scale;
        int64_t frac = v % scale;
        int neg = (v < 0);
        if (frac < 0) frac = -frac;
        if (whole < 0) whole = -whole;
        if (frac == 0)
            return snprintf(buf, bufsz, "%s%lld", neg ? "-" : "", (long long)whole);
        return snprintf(buf, bufsz, "%s%lld.%0*lld", neg ? "-" : "",
                        (long long)whole, f->numeric_scale, (long long)frac);
    }
    case FT_VARCHAR: {
        /* Index stores raw varchar bytes. Empty-string semantics match
           the index's skip-zero rule: zero-length entries don't appear
           in the btree, so any leaf we see has plen > 0. */
        if (plen == 0) return 0;
        size_t cl = plen < bufsz - 1 ? plen : bufsz - 1;
        memcpy(buf, p, cl);
        buf[cl] = '\0';
        return (int)cl;
    }
    case FT_ENUM: {
        /* Index-key for enum = byte index (1 or 2 BE). Decode to
           enum_values[idx]. Used by the bitmap dict-scan path so a
           query like `WHERE color LIKE 'r%'` can iterate the dict,
           decode each entry to its display string, then match_criterion
           against the user's pattern. */
        if (!f->enum_values || f->n_enum_values <= 0 || plen == 0) return 0;
        int idx = (f->enum_width == 2 && plen >= 2)
                    ? (int)(((uint16_t)p[0] << 8) | (uint16_t)p[1])
                    : (int)p[0];
        if (idx < 0 || idx >= f->n_enum_values) return 0;
        const char *s = f->enum_values[idx];
        if (!s) return 0;
        size_t sl = strlen(s);
        if (sl >= bufsz) sl = bufsz - 1;
        memcpy(buf, s, sl);
        buf[sl] = '\0';
        return (int)sl;
    }
    default: return 0;
    }
}

/* Open-addressed map keyed by 16-byte hash, value = AggBucket pointer. Used
   by the indexed group_by fast path so each agg field btree entry can be
   attributed to its bucket in O(1). The first 8 bytes of hash16 are
   already a high-quality xxh128 prefix, used directly as the probe seed.
   Forward-declared AggBucket because the typedef lives further down.

   Layout choices for L3-cache friendliness on 1M-entry workloads:
     - One contiguous entry array of `{hash[16], val}` (24 B per slot)
       instead of two parallel keys[]/vals[] arrays. A single probe
       reads hash + val from one cache line; the parallel-array layout
       paid two cache misses per probe.
     - Load factor 0.75 (cap = next_pow2(1.34 × hint)) instead of 0.5.
       For 1M entries: cap = 2M (32 MB) → 1.4M (~33 MB). Probe length
       grows from 1.5 to ~3 — but probes within a probe-chain share
       cache lines (4 entries per 64-B cache line), so total cache
       miss count drops. Net L3 footprint: 48MB → 33MB, fits L3 on
       most servers. */
struct AggBucket;
typedef struct {
    uint8_t           hash[16];
    struct AggBucket *val;     /* NULL = empty slot */
} HashBktEntry;

typedef struct {
    HashBktEntry *entries;
    size_t        cap;          /* power of 2 */
    size_t        mask;
} HashBktMap;

static int hbk_init(HashBktMap *m, size_t cap_hint) {
    size_t cap = 64;
    /* Load factor target ~0.75: cap × 0.75 ≥ hint, i.e. cap ≥ hint × 4/3.
       Approximate with hint × 2 / 3 × 2 → walk pow2 until cap*3 ≥ hint*4. */
    while (cap * 3 < cap_hint * 4) cap <<= 1;
    m->entries = calloc(cap, sizeof(HashBktEntry));
    if (!m->entries) {
        m->cap = m->mask = 0;
        return -1;
    }
    m->cap = cap;
    m->mask = cap - 1;
    return 0;
}

static void hbk_free(HashBktMap *m) {
    free(m->entries);
    m->entries = NULL;
    m->cap = m->mask = 0;
}

static inline size_t hbk_seed(const uint8_t *h) {
    return ((size_t)h[0] << 56) | ((size_t)h[1] << 48) |
           ((size_t)h[2] << 40) | ((size_t)h[3] << 32) |
           ((size_t)h[4] << 24) | ((size_t)h[5] << 16) |
           ((size_t)h[6] << 8)  |  (size_t)h[7];
}

static void hbk_insert(HashBktMap *m, const uint8_t *hash16, struct AggBucket *b) {
    size_t idx = hbk_seed(hash16) & m->mask;
    while (m->entries[idx].val != NULL) idx = (idx + 1) & m->mask;
    memcpy(m->entries[idx].hash, hash16, 16);
    m->entries[idx].val = b;
}

static struct AggBucket *hbk_get(const HashBktMap *m, const uint8_t *hash16) {
    size_t idx = hbk_seed(hash16) & m->mask;
    while (m->entries[idx].val != NULL) {
        if (memcmp(m->entries[idx].hash, hash16, 16) == 0)
            return m->entries[idx].val;
        idx = (idx + 1) & m->mask;
    }
    return NULL;
}

/* Compute the probe-start index for a given hash without touching the
   table — used by the prefetch path so we can issue cache hints for
   N-ahead lookups before the actual hbk_get reads the slot. */
static inline size_t hbk_index(const HashBktMap *m, const uint8_t *hash16) {
    return hbk_seed(hash16) & m->mask;
}

/* Hash16 → variable-length string map. Used by the indexed group_by fast
   path for secondary group fields (when ngroups > 1) so we can resolve
   each record's secondary value by hash16 without fetching the record.
   Strings are appended into a single arena; entries store (offset, len).
   Same open-addressed shape as HashBktMap, sized at ~0.75 load factor. */
typedef struct {
    uint8_t  hash[16];
    uint32_t off;        /* offset into arena */
    uint16_t len;        /* string length */
    uint8_t  occupied;   /* 0 = empty slot, 1 = occupied */
} HashStrEntry;

typedef struct {
    HashStrEntry *entries;
    size_t        cap;
    size_t        mask;
    char         *arena;
    size_t        arena_used;
    size_t        arena_cap;
} HashStrMap;

static int hsm_init(HashStrMap *m, size_t cap_hint, size_t arena_hint) {
    size_t cap = 64;
    while (cap * 3 < cap_hint * 4) cap <<= 1;
    m->entries = calloc(cap, sizeof(HashStrEntry));
    if (arena_hint < 1024) arena_hint = 1024;
    m->arena = malloc(arena_hint);
    if (!m->entries || !m->arena) {
        free(m->entries); m->entries = NULL;
        free(m->arena); m->arena = NULL;
        m->cap = m->mask = m->arena_used = m->arena_cap = 0;
        return -1;
    }
    m->cap = cap;
    m->mask = cap - 1;
    m->arena_used = 0;
    m->arena_cap = arena_hint;
    return 0;
}

static void hsm_free(HashStrMap *m) {
    free(m->entries); m->entries = NULL;
    free(m->arena);   m->arena = NULL;
    m->cap = m->mask = m->arena_used = m->arena_cap = 0;
}

/* Append a string to the arena (growing it if needed) and place an
   entry into the map. Linear probing on collision. Returns 0 on success,
   -1 on allocation failure. */
static int hsm_insert(HashStrMap *m, const uint8_t hash[16],
                       const char *val, size_t vlen) {
    if (m->arena_used + vlen > m->arena_cap) {
        size_t new_cap = m->arena_cap;
        while (m->arena_used + vlen > new_cap) new_cap *= 2;
        char *na = realloc(m->arena, new_cap);
        if (!na) return -1;
        m->arena = na;
        m->arena_cap = new_cap;
    }
    uint32_t off = (uint32_t)m->arena_used;
    if (vlen > 0) memcpy(m->arena + off, val, vlen);
    m->arena_used += vlen;

    size_t idx = hbk_seed(hash) & m->mask;
    while (m->entries[idx].occupied) {
        /* Duplicate hash → keep first occurrence; the index walk visits
           each entry once so this only happens on real hash collisions. */
        if (memcmp(m->entries[idx].hash, hash, 16) == 0) return 0;
        idx = (idx + 1) & m->mask;
    }
    memcpy(m->entries[idx].hash, hash, 16);
    m->entries[idx].off = off;
    m->entries[idx].len = (uint16_t)(vlen > UINT16_MAX ? UINT16_MAX : vlen);
    m->entries[idx].occupied = 1;
    return 0;
}

/* Lookup. Returns 1 with out_val/out_len set on hit, 0 on miss. */
static int hsm_get(const HashStrMap *m, const uint8_t hash[16],
                    const char **out_val, size_t *out_len) {
    size_t idx = hbk_seed(hash) & m->mask;
    while (m->entries[idx].occupied) {
        if (memcmp(m->entries[idx].hash, hash, 16) == 0) {
            *out_val = m->arena + m->entries[idx].off;
            *out_len = m->entries[idx].len;
            return 1;
        }
        idx = (idx + 1) & m->mask;
    }
    return 0;
}

/* ========== Walk-fetch-check for MIN/MAX with arbitrary criteria ==========

   Walk the agg field's btree in MIN/MAX order. For each leaf entry,
   decode the agg value, fetch the corresponding record by hash, and
   evaluate the full criteria tree against it. The first matching record
   per shard wins (the btree iteration is sorted, so the first match
   has the smallest/largest agg value within that shard); take the
   global min/max across shards.

   Beats the keyset/intersect paths whenever criteria selectivity is
   above ~1%: the existing paths build a candidate hash set up-front
   (~30ms for a 500k-match leaf) before walking the agg btree, while
   walk-fetch-check finds the answer in a few records (~µs each) and
   exits.

   Falls back to the existing paths via a per-shard `budget` cap. When
   budget walks complete without a match, the shard is "indefinite" and
   the caller falls through to the keyset/intersect path which guarantees
   completeness regardless of selectivity. */

/* Forward declaration for wfc_batch_cb */
static int typed_field_to_double(const TypedField *f, const uint8_t *p, double *out);

typedef struct {
    CriteriaNode     *tree;
    FieldSchema      *fs;
    const TypedField *agg_tf;
    int               desc;     /* 0 = MIN (ASC walk), 1 = MAX (DESC) */
    double           *best;
    int              *found;
    pthread_mutex_t   mu;
} WfcBatchCtx;

typedef struct {
    const char       *db_root;
    const char       *object;
    const Schema     *sch;
    int               shard_id;
    const char       *agg_field;
    const TypedField *agg_tf;
    int               desc;       /* 0 = MIN (ASC walk), 1 = MAX (DESC) */
    CriteriaNode     *tree;
    FieldSchema     *fs;
    QueryDeadline    *deadline;
    int               budget;     /* max walks per shard before bailout */

    /* Per-shard outputs */
    double  best;
    int     found;
    int     budget_exceeded;
    int     dl_counter;
} WfcArg;

static int wfc_batch_cb(const uint8_t hash16[16],
                         const void *key, size_t klen,
                         const void *value, size_t vlen,
                         void *ctx) {
    (void)hash16; (void)key; (void)klen; (void)vlen;
    WfcBatchCtx *bc = (WfcBatchCtx *)ctx;
    if (!criteria_match_tree(value, bc->tree, bc->fs)) return 0;
    double v;
    if (!typed_field_to_double(bc->agg_tf,
                               (const uint8_t *)value + bc->agg_tf->offset,
                               &v))
        return 0;
    pthread_mutex_lock(&bc->mu);
    if (!*bc->found ||
        (!bc->desc && v < *bc->best) ||
        ( bc->desc && v > *bc->best)) {
        *bc->best  = v;
        *bc->found = 1;
    }
    pthread_mutex_unlock(&bc->mu);
    return 0;
}

static void flush_wfc_batch(SlotcaskDb *sdb, WfcArg *w,
                             uint8_t (*batch)[16], int bn,
                             WfcBatchCtx *bc) {
    if (sdb) {
        slotcask_bulk_resolve_and_fetch(sdb, batch, (size_t)bn, bc, wfc_batch_cb);
    } else {
        for (int i = 0; i < bn; i++) {
            RecordRef rr;
            if (read_record_ref(w->db_root, w->object, w->sch,
                                batch[i], &rr) != 0) continue;
            wfc_batch_cb(batch[i], NULL, 0, rr.val, rr.vlen, bc);
            release_record_ref(&rr);
        }
    }
}

static void *wfc_worker(void *arg) {
    WfcArg *w = (WfcArg *)arg;

    SlotcaskSchemaInfo info = {
        .splits = w->sch->splits, .slot_size = w->sch->slot_size,
        .streams = w->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(w->db_root, w->object, &info);

    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path),
                   w->db_root, w->object, w->agg_field, w->shard_id);
    BtRangeIter *it = btree_range_iter_open(
        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, w->desc);
    if (!it) return NULL;

    const char *val; size_t vlen; const uint8_t *hash16;
    int walks = 0;
#define WFC_BATCH 64
    uint8_t batch[WFC_BATCH][16];
    int bn = 0;
    WfcBatchCtx bc = {
        .tree   = w->tree,
        .fs     = w->fs,
        .agg_tf = w->agg_tf,
        .desc   = w->desc,
        .best   = &w->best,
        .found  = &w->found,
        .mu     = PTHREAD_MUTEX_INITIALIZER,
    };

    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        if (query_deadline_tick(w->deadline, &w->dl_counter)) break;
        if (++walks > w->budget) { w->budget_exceeded = 1; break; }

        double v;
        if (!decode_index_key_to_double(w->agg_tf, (const uint8_t *)val,
                                        vlen, &v)) continue;

        memcpy(batch[bn], hash16, 16);
        bn++;

        if (bn >= WFC_BATCH) {
            flush_wfc_batch(sdb, w, batch, bn, &bc);
            bn = 0;
            if (w->found) break;   /* first batch with a match → done */
        }
    }
    if (bn > 0 && !w->found)
        flush_wfc_batch(sdb, w, batch, bn, &bc);

    btree_range_iter_close(it);
    return NULL;
}

/* Per-shard worker for the SUM/AVG/MIN/MAX-with-criteria fast path. Each
   worker walks one idx shard's btree, filters entries by the candidate
   KeySet, decodes the value, and accumulates into per-spec local arrays.
   The orchestrator merges across shards after parallel_for returns. */
typedef struct {
    const char       *db_root;
    const char       *object;
    const char       *fld;
    int               shard_id;
    const TypedField *tf;
    KeySet           *crit_ks;
    const int        *sibs;       /* spec indices sharing this field */
    int               nsibs;
    QueryDeadline    *deadline;
    int               dl_counter;
    /* outputs (one slot per agg spec — only sibs[] entries are touched) */
    int64_t          *counts;     /* MAX_AGG_SPECS-wide */
    double           *sums;
    double           *mins;
    double           *maxs;
    int              *present;
} AwcShardArg;

static void *awc_shard_worker(void *raw) {
    AwcShardArg *a = (AwcShardArg *)raw;
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path),
                   a->db_root, a->object, a->fld, a->shard_id);
    BtRangeIter *it = btree_range_iter_open(
        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, 0);
    if (!it) return NULL;
    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        if (query_deadline_tick(a->deadline, &a->dl_counter)) break;
        if (!keyset_contains(a->crit_ks, hash16)) continue;
        double v;
        if (!decode_index_key_to_double(a->tf, (const uint8_t *)val,
                                        vlen, &v)) continue;
        for (int k = 0; k < a->nsibs; k++) {
            int idx = a->sibs[k];
            a->counts[idx]++;
            a->sums[idx] += v;
            if (v < a->mins[idx]) a->mins[idx] = v;
            if (v > a->maxs[idx]) a->maxs[idx] = v;
            a->present[idx] = 1;
        }
    }
    btree_range_iter_close(it);
    return NULL;
}

/* Extract a typed field as a double for SUM/AVG/MIN/MAX accumulation.
   Returns 1 if the field is "present" (non-zero/non-empty), 0 if missing
   (so the record is excluded from the aggregate — matches legacy behavior). */
static int typed_field_to_double(const TypedField *f, const uint8_t *p, double *out) {
    switch (f->type) {
    case FT_LONG: {
        int64_t v = ld_be_i64(p);
        if (v == 0) return 0;
        *out = (double)v; return 1;
    }
    case FT_INT: {
        int32_t v = ld_be_i32(p);
        if (v == 0) return 0;
        *out = (double)v; return 1;
    }
    case FT_SHORT: {
        int16_t v = ld_be_i16(p);
        if (v == 0) return 0;
        *out = (double)v; return 1;
    }
    case FT_DOUBLE: {
        double v; memcpy(&v, p, 8);
        if (v == 0.0) return 0;
        *out = v; return 1;
    }
    case FT_NUMERIC: {
        int64_t v = ld_be_i64(p);
        *out = (double)v / (double)f->numeric_scale_mult; return 1;
    }
    case FT_BOOL: *out = (double)(p[0] ? 1 : 0); return 1;
    case FT_BYTE: *out = (double)p[0]; return 1;
    case FT_DATE: {
        int32_t v = ld_be_i32(p);
        if (v == 0) return 0;
        *out = (double)v; return 1;
    }
    case FT_DATETIME: {
        int32_t d = ld_be_i32(p);
        uint16_t t = ld_be_u16(p + 4);
        if (d == 0 && t == 0) return 0;
        *out = (double)d * 1000000.0 + (double)t; return 1;
    }
    case FT_DATETIMEMS: {
        int32_t d = ld_be_i32(p);
        uint32_t ms = ld_be_u32(p + 4);
        if (d == 0 && ms == 0) return 0;
        *out = (double)d * 100000000.0 + (double)ms;
        return 1;
    }
    case FT_TIME: return 0;  /* not summable */
    case FT_UUID: return 0;  /* not summable */
    case FT_IPV4: return 0;  /* not summable */
    case FT_IPV6: return 0;  /* not summable */
    case FT_VARCHAR: {
        int len = varchar_eff_len(p, f->size);
        if (len == 0) return 0;
        char tmp[64]; int n = len < 63 ? len : 63;
        memcpy(tmp, p + 2, n); tmp[n] = '\0';
        *out = atof(tmp); return 1;
    }
    default: return 0;
    }
}

static uint32_t agg_hash(const char *s) {
    uint32_t h = 5381;
    while (*s) h = h * 33 + (uint8_t)*s++;
    return h;
}

/* Integer hash - hash raw bytes using Golden Ratio multiplier.
   Works for any size: 2, 4, or 8 bytes. */
static uint32_t agg_hash_int(const void *key, size_t len) {
    uint64_t h = 0;
    const uint8_t *p = (const uint8_t *)key;
    for (size_t i = 0; i < len; i++) {
        h = h * 0x9E3779B9 + p[i];
    }
    return (uint32_t)(h ^ (h >> 32));
}

/* Compare raw integer keys (memcmp but returns bool). */
static int agg_key_eq_int(const void *a, size_t alen, const void *b, size_t blen) {
    if (alen != blen) return 0;
    return memcmp(a, b, alen) == 0;
}

/* Width of the on-disk fixed-int encoding for an integer-class field type.
   Returns 0 for non-integer types (varchar/double/float/bool/datetime/time/uuid).
   Mirrors the typed_field_to_raw byte counts and feeds the use_int_keys
   gate at agg setup so the total raw key width is known before scan. */
static int typed_field_int_width(int ft) {
    switch (ft) {
    case FT_INT:     return 4;
    case FT_LONG:    return 8;
    case FT_SHORT:   return 2;
    case FT_BYTE:    return 1;
    case FT_NUMERIC: return 8;
    case FT_DATE:    return 4;
    default:         return 0;
    }
}

/* Copy the field's fixed-width on-disk bytes into buf for use as a hash key.
   Always emits the full natural width (no zero-skip): the on-disk encoding is
   already a stable BE form, so byte-equal ⇔ value-equal and we can memcmp
   raw keys directly. Returns bytes written, or 0 if buf is too small / type
   is not integer. Callers must size buf for the sum of all group field widths
   (see typed_field_int_width). */
static int typed_field_to_raw(const TypedField *f, const uint8_t *p,
                              uint8_t *buf, size_t bufsz) {
    int w = typed_field_int_width(f->type);
    if (w <= 0 || (size_t)w > bufsz) return 0;
    memcpy(buf, p, (size_t)w);
    return w;
}

/* Lazy-allocate the AggCtx hash table on first insert. Starting size
   AGG_HT_INIT (256 slots, 2 KB) — typical low-cardinality group_by
   queries fit comfortably without ever resizing. Returns 0 on success,
   -1 on alloc failure. */
static int agg_ht_lazy_init(AggCtx *ctx) {
    if (ctx->ht) return 0;
    ctx->ht = calloc(AGG_HT_INIT, sizeof(AggBucket *));
    if (!ctx->ht) return -1;
    ctx->ht_cap = AGG_HT_INIT;
    ctx->ht_mask = AGG_HT_INIT - 1;
    return 0;
}

/* Double the ht in place. Walks each existing chain once, re-routing
   buckets by (hash & new_mask). All AggBucket structs stay where they
   are (in the arena); only the table of head pointers is reallocated.
   Returns 0 on success, -1 on alloc failure (caller falls through to
   the existing chain — slow but correct). */
static int agg_ht_resize(AggCtx *ctx) {
    if (ctx->ht_cap >= AGG_HT_MAX) return 0;  /* hard cap reached */
    size_t new_cap = ctx->ht_cap * 2;
    AggBucket **new_ht = calloc(new_cap, sizeof(AggBucket *));
    if (!new_ht) return -1;
    size_t new_mask = new_cap - 1;
    for (size_t i = 0; i < ctx->ht_cap; i++) {
        AggBucket *b = ctx->ht[i];
        while (b) {
            AggBucket *next = b->next;
            uint32_t h = (ctx->use_int_keys && b->raw_key_len > 0)
                ? agg_hash_int(b->raw_key, (size_t)b->raw_key_len) & (uint32_t)new_mask
                : agg_hash(b->group_key) & (uint32_t)new_mask;
            b->next = new_ht[h];
            new_ht[h] = b;
            b = next;
        }
    }
    free(ctx->ht);
    ctx->ht = new_ht;
    ctx->ht_cap = new_cap;
    ctx->ht_mask = new_mask;
    return 0;
}

/* Find or create aggregate bucket. For integer group keys (use_int_keys),
   raw_key/len provides the raw bytes for fast hash/compare, while vals
   still provides strings for output. For string keys, raw_key_len=0. */
static AggBucket *agg_find_or_create(AggCtx *ctx, char **vals, int nvals,
                                      const uint8_t *raw_key, int raw_key_len) {
    /* Build composite key */
    char key[4096];
    int kp = 0;
    for (int i = 0; i < nvals; i++) {
        if (i > 0) key[kp++] = '\x1F';
        int sl = strlen(vals[i]);
        if (kp + sl >= (int)sizeof(key) - 1) break;
        memcpy(key + kp, vals[i], sl);
        kp += sl;
    }
    key[kp] = '\0';

    /* Lazy-init on first insert so an empty AggCtx pays no allocation. */
    if (!ctx->ht && agg_ht_lazy_init(ctx) != 0) {
        ctx->budget_exceeded = 1;
        return NULL;
    }

    /* Hash: use integer hash for integer keys, djb2 for string keys */
    uint32_t khash;
    uint32_t h;
    if (ctx->use_int_keys && raw_key_len > 0) {
        khash = agg_hash_int(raw_key, (size_t)raw_key_len);
        h = khash & (uint32_t)ctx->ht_mask;
        for (AggBucket *b = ctx->ht[h]; b; b = b->next) {
            if (b->raw_key_len > 0 &&
                agg_key_eq_int(raw_key, (size_t)raw_key_len,
                               b->raw_key, b->raw_key_len)) {
                return b;
            }
        }
    } else {
        khash = agg_hash(key);
        h = khash & (uint32_t)ctx->ht_mask;
        for (AggBucket *b = ctx->ht[h]; b; b = b->next) {
            if (strcmp(b->group_key, key) == 0) return b;
        }
    }

    /* New bucket: charge against the shared budget before allocating. */
    if (ctx->shared_buffer_bytes) {
        size_t bucket_bytes = sizeof(AggBucket) + (size_t)(kp + 1) +
                              (size_t)nvals * sizeof(char *) +
                              (size_t)ctx->nspecs * sizeof(AggAccum);
        for (int i = 0; i < nvals; i++) bucket_bytes += strlen(vals[i]) + 1;
        size_t prev = atomic_fetch_add_explicit(ctx->shared_buffer_bytes,
                                                bucket_bytes, memory_order_relaxed);
        if (prev + bucket_bytes > g_query_buffer_max_bytes) {
            atomic_fetch_sub_explicit(ctx->shared_buffer_bytes,
                                      bucket_bytes, memory_order_relaxed);
            ctx->budget_exceeded = 1;
            return NULL;
        }
    }

    /* Create new bucket — all storage carved from ctx->arena so teardown
       is O(slabs) not O(buckets). */
    AggBucket *b = agg_arena_alloc(&ctx->arena, sizeof(AggBucket));
    if (!b) { ctx->budget_exceeded = 1; return NULL; }
    memset(b, 0, sizeof(AggBucket));
    b->group_key = agg_arena_strdup(&ctx->arena, key, (size_t)kp);
    b->group_vals = agg_arena_alloc(&ctx->arena, (size_t)nvals * sizeof(char *));
    if (!b->group_key || !b->group_vals) {
        ctx->budget_exceeded = 1;
        return NULL;
    }
    /* Store raw key for integer optimization */
    if (ctx->use_int_keys && raw_key_len > 0 && raw_key_len <= AGG_INT_KEY_CAP) {
        memcpy(b->raw_key, raw_key, (size_t)raw_key_len);
        b->raw_key_len = (uint8_t)raw_key_len;
    }
    for (int i = 0; i < nvals; i++) {
        size_t sl = strlen(vals[i]);
        b->group_vals[i] = agg_arena_strdup(&ctx->arena, vals[i], sl);
        if (!b->group_vals[i]) { ctx->budget_exceeded = 1; return NULL; }
    }
    b->accums = agg_arena_alloc(&ctx->arena,
                                 (size_t)ctx->nspecs * sizeof(AggAccum));
    if (!b->accums) { ctx->budget_exceeded = 1; return NULL; }
    memset(b->accums, 0, (size_t)ctx->nspecs * sizeof(AggAccum));
    for (int i = 0; i < ctx->nspecs; i++) {
        b->accums[i].min = 1e308;
        b->accums[i].max = -1e308;
    }
    b->next = ctx->ht[h];
    ctx->ht[h] = b;
    ctx->total_buckets++;
    /* Grow the table when load factor exceeds 1.0 to keep chains O(1).
       Resize is amortised O(1) per insert (each bucket moves at most
       O(log N) times across all resizes). The capped AGG_HT_MAX prevents
       unbounded memory growth — past it, chains lengthen and the
       per-query QUERY_BUFFER_MB cap stops us before damage. */
    if ((size_t)ctx->total_buckets > ctx->ht_cap &&
        ctx->ht_cap < AGG_HT_MAX) {
        (void)agg_ht_resize(ctx);  /* failure → keep going on old table */
    }
    return b;
}

static int agg_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *raw_ctx) {
    AggCtx *ctx = (AggCtx *)raw_ctx;
    if (ctx->budget_exceeded) return 1;
    if (query_deadline_tick(ctx->deadline, &ctx->dl_counter)) return 1;
    const uint8_t *raw = block + hdr->key_len;

    /* Check full criteria tree (AND/OR) */
    if (!criteria_match_tree(raw, ctx->tree, ctx->fs)) return 0;

    /* Extract group_by values into stack buffers (no malloc). gbuf[i][0]
       is forced to NUL up front because typed_field_to_buf_raw can return
       without writing (e.g. v=0 on int/long/etc.) — leaving stale bytes
       from a prior record in this thread's stack frame, which would then
       leak into the string-path group_key and merge unrelated rows. */
    char gbuf[MAX_FIELDS][512];
    char *gvals[MAX_FIELDS];
    for (int i = 0; i < ctx->ngroups; i++) {
        gbuf[i][0] = '\0';
        if (ctx->group_tfs[i]) {
            typed_field_to_buf_raw(ctx->group_tfs[i],
                                   raw + ctx->group_tfs[i]->offset,
                                   gbuf[i], sizeof(gbuf[i]));
        } else {
            /* Composite/unknown — fallback to decode_field */
            char *s = decode_field((const char *)raw, hdr->value_len,
                                   ctx->group_fields[i], ctx->fs);
            if (s) {
                size_t sl = strlen(s);
                if (sl >= sizeof(gbuf[i])) sl = sizeof(gbuf[i]) - 1;
                memcpy(gbuf[i], s, sl); gbuf[i][sl] = '\0';
                free(s);
            } else {
                gbuf[i][0] = '\0';
            }
        }
        gvals[i] = gbuf[i];
    }

    /* Extract raw bytes for integer group keys (fast hash path). use_int_keys
       is only set at agg setup when the sum of field widths fits the inline
       cap, so the loop is guaranteed to consume every group field — no
       silent truncation. typed_field_to_raw emits the full natural width
       for each field (including for v=0) so byte-equal ⇔ tuple-equal. */
    uint8_t raw_key[AGG_INT_KEY_CAP];
    int raw_key_len = 0;
    if (ctx->use_int_keys) {
        int kp = 0;
        for (int i = 0; i < ctx->ngroups && kp < AGG_INT_KEY_CAP; i++) {
            if (ctx->group_tfs[i]) {
                int len = typed_field_to_raw(ctx->group_tfs[i],
                                             raw + ctx->group_tfs[i]->offset,
                                             raw_key + kp,
                                             (size_t)(AGG_INT_KEY_CAP - kp));
                if (len > 0) kp += len;
            }
        }
        raw_key_len = kp;
    }

    /* Find or create bucket + accumulate. Every parallel path now gives
       each worker its own cloned AggCtx (fresh hash table + arena), so
       hashtable insert and accumulator updates are worker-local — no
       mutex needed. */
    AggBucket *bkt;
    if (ctx->ngroups > 0) {
        bkt = agg_find_or_create(ctx, gvals, ctx->ngroups, raw_key, raw_key_len);
    } else {
        char *empty = "";
        bkt = agg_find_or_create(ctx, &empty, 1, NULL, 0);
    }
    if (!bkt) return 1;  /* budget exceeded — stop scan */

    for (int i = 0; i < ctx->nspecs; i++) {
        AggAccum *a = &bkt->accums[i];
        if (ctx->specs[i].fn == AGG_COUNT) {
            /* count(*) (no field) and count(non-varchar typed field) →
               every record counts. count(varchar field) → only records
               where the varchar has non-empty content (elen > 0), matching
               the OP_EXISTS semantics on varchar. Non-varchar typed fields
               always carry a value, so the field arg is informational. */
            if (ctx->specs[i].field[0] && ctx->spec_tfs[i] &&
                ctx->spec_tfs[i]->type == FT_VARCHAR) {
                int elen = varchar_eff_len(raw + ctx->spec_tfs[i]->offset,
                                           ctx->spec_tfs[i]->size);
                if (elen <= 0) continue;
            }
            a->count++;
            continue;
        }

        double v;
        int present = 0;
        if (ctx->spec_tfs[i]) {
            present = typed_field_to_double(ctx->spec_tfs[i],
                                            raw + ctx->spec_tfs[i]->offset, &v);
        } else {
            char *fv = decode_field((const char *)raw, hdr->value_len,
                                    ctx->specs[i].field, ctx->fs);
            if (fv && fv[0]) { v = atof(fv); present = 1; }
            free(fv);
        }
        if (present) {
            a->count++;
            a->sum += v;
            if (v < a->min) a->min = v;
            if (v > a->max) a->max = v;
        }
    }
    return 0;
}

static int parse_agg_specs(const char *json, AggSpec **out) {
    /* Parse: [{"fn":"sum","field":"total","alias":"revenue"}, ...] */
    int cap = 16, n = 0;
    AggSpec *specs = calloc(cap, sizeof(AggSpec));
    const char *p = json;
    while (*p && *p != ']') {
        const char *obj = strchr(p, '{');
        if (!obj) break;
        const char *end = strchr(obj, '}');
        if (!end) break;
        char buf[1024];
        int len = end - obj + 1;
        if (len >= (int)sizeof(buf)) { p = end + 1; continue; }
        memcpy(buf, obj, len); buf[len] = '\0';

        if (n >= cap) {
            cap *= 2;
            AggSpec *t = xrealloc_or_free(specs, cap * sizeof(*t));
            if (!t) { specs = NULL; break; }
            specs = t;
        }
        AggSpec *s = &specs[n];
        memset(s, 0, sizeof(*s));

        JsonObj sobj;
        json_parse_object(buf, len, &sobj);
        char *fn    = json_obj_strdup(&sobj, "fn");
        char *field = json_obj_strdup(&sobj, "field");
        char *alias = json_obj_strdup(&sobj, "alias");

        if (fn) {
            if (strcmp(fn, "count") == 0) s->fn = AGG_COUNT;
            else if (strcmp(fn, "sum") == 0) s->fn = AGG_SUM;
            else if (strcmp(fn, "avg") == 0) s->fn = AGG_AVG;
            else if (strcmp(fn, "min") == 0) s->fn = AGG_MIN;
            else if (strcmp(fn, "max") == 0) s->fn = AGG_MAX;
        }
        if (field) strncpy(s->field, field, 255);
        if (alias) strncpy(s->alias, alias, 255);
        else if (fn && field) snprintf(s->alias, 255, "%s_%s", fn, field);
        else if (fn) strncpy(s->alias, fn, 255);

        free(fn); free(field); free(alias);
        n++;
        p = end + 1;
    }
    *out = specs;
    return n;
}

static int parse_group_by(const char *json, char out[][256]) {
    /* Parse: ["status","currency"] */
    int n = 0;
    const char *p = json;
    while (*p && n < MAX_FIELDS) {
        while (*p && *p != '"') p++;
        if (!*p) break;
        p++; /* skip opening quote */
        const char *start = p;
        while (*p && *p != '"') p++;
        int len = p - start;
        if (len > 0 && len < 256) {
            memcpy(out[n], start, len);
            out[n][len] = '\0';
            n++;
        }
        if (*p == '"') p++;
    }
    return n;
}

/* Get aggregate value by alias from a bucket, for having filter */
static double agg_bucket_value(AggBucket *bkt, AggSpec *specs, int nspecs, const char *alias) {
    for (int i = 0; i < nspecs; i++) {
        if (strcmp(specs[i].alias, alias) == 0) {
            AggAccum *a = &bkt->accums[i];
            switch (specs[i].fn) {
                case AGG_COUNT: return (double)a->count;
                case AGG_SUM:   return a->sum;
                case AGG_AVG:   return a->count > 0 ? a->sum / a->count : 0.0;
                case AGG_MIN:   return a->count > 0 ? a->min : 0.0;
                case AGG_MAX:   return a->count > 0 ? a->max : 0.0;
            }
        }
    }
    return 0.0;
}

/* Check having criteria against a bucket */
static int agg_having_match(AggBucket *bkt, AggSpec *specs, int nspecs,
                            SearchCriterion *having, int nhaving) {
    for (int i = 0; i < nhaving; i++) {
        double val = agg_bucket_value(bkt, specs, nspecs, having[i].field);
        char val_str[64];
        snprintf(val_str, sizeof(val_str), "%.6f", val);
        if (!match_criterion(val_str, &having[i])) return 0;
    }
    return 1;
}

/* Collect all buckets into a flat array */
static AggBucket **agg_collect(AggCtx *ctx, int *out_count) {
    AggBucket **arr = malloc((size_t)ctx->total_buckets * sizeof(AggBucket *));
    int n = 0;
    if (ctx->ht) {
        for (size_t i = 0; i < ctx->ht_cap; i++) {
            for (AggBucket *b = ctx->ht[i]; b; b = b->next) {
                arr[n++] = b;
            }
        }
    }
    *out_count = n;
    return arr;
}

/* Clone shared read-only context into a per-worker AggCtx (own hash table). */
static void agg_ctx_clone_shared(AggCtx *dst, const AggCtx *src) {
    memset(dst, 0, sizeof(*dst));
    dst->tree = src->tree;
    dst->fs = src->fs;
    dst->shared_buffer_bytes = src->shared_buffer_bytes;
    memcpy(dst->group_fields, src->group_fields, sizeof(src->group_fields));
    dst->ngroups = src->ngroups;
    memcpy(dst->group_tfs, src->group_tfs, sizeof(src->group_tfs));
    dst->specs = src->specs;
    dst->nspecs = src->nspecs;
    memcpy(dst->spec_tfs, src->spec_tfs, sizeof(src->spec_tfs));
    dst->deadline = src->deadline;
    dst->use_int_keys = src->use_int_keys;
    /* dl_counter stays 0 (per-worker local) */
}

/* Forward decl — definition lives further down near agg_free. Callers
   in parallel_agg_scan_shards_v2 / parallel_indexed_agg / parallel_agg_scan_shards
   need this to release per-worker arenas after merging into main. */
static void agg_ctx_free_local(AggCtx *ctx);

/* Merge src's hash table into dst. Buckets in src live in src->arena
   so we don't free them individually — agg_find_or_create copies the
   group_vals strings into dst->arena, then the caller (or
   agg_ctx_free_local on src) frees src->arena en masse. */
static void agg_ctx_merge(AggCtx *dst, AggCtx *src) {
    int nvals = src->ngroups > 0 ? src->ngroups : 1;
    if (!src->ht) { src->total_buckets = 0; return; }
    for (size_t i = 0; i < src->ht_cap; i++) {
        for (AggBucket *b = src->ht[i]; b; b = b->next) {
            AggBucket *dbkt = agg_find_or_create(dst, b->group_vals, nvals,
                                                  b->raw_key, b->raw_key_len);
            if (!dbkt) continue;  /* dst budget exhausted — caller checks flag */
            for (int k = 0; k < src->nspecs; k++) {
                AggAccum *sa = &b->accums[k], *da = &dbkt->accums[k];
                da->sum += sa->sum;
                da->count += sa->count;
                if (sa->count > 0) {
                    if (sa->min < da->min) da->min = sa->min;
                    if (sa->max > da->max) da->max = sa->max;
                }
            }
        }
        src->ht[i] = NULL;
    }
    src->total_buckets = 0;
}

/* ── O_DIRECT per-segment aggregate fan-out ─────────────────────────────
 * One AggOdSegArg per .dat file. Each worker runs seg_scan_o_direct on its
 * file, accumulating results into a private local AggCtx. After
 * parallel_for_io joins, all locals are merged into main_ctx.
 *
 * Uses the existing od_seg_record_cb / OdSegAdapterCtx adapter so the
 * per-record hot path is identical to scan_shards_v2_o_direct. */
typedef struct {
    char       seg_path[PATH_MAX];
    int        slot_size;
    int        format;     /* SLOTCASK_FORMAT_FIXED or SLOTCASK_FORMAT_VARIABLE */
    AggCtx     local;         /* per-segment private accumulator */
    V2ScanWrap wrap;          /* .cb = agg_scan_cb, .ctx = &this->local */
    int       *stop_flag;
    FILE      *parent_out;
} AggOdSegArg;

static void *agg_od_seg_worker(void *raw) {
    AggOdSegArg *arg = (AggOdSegArg *)raw;
    g_out = arg->parent_out ? arg->parent_out : stdout;
    if (__atomic_load_n(arg->stop_flag, __ATOMIC_RELAXED)) return NULL;
    OdSegAdapterCtx actx = { .wrap = &arg->wrap, .stop_flag = arg->stop_flag };
    if (arg->format == SLOTCASK_FORMAT_VARIABLE)
        seg_scan_o_direct_varlen(arg->seg_path, od_seg_record_cb, &actx);
    else
        seg_scan_o_direct(arg->seg_path, arg->slot_size, od_seg_record_cb, &actx);
    count_scan_cb_flush_thread();
    return NULL;
}

static void parallel_agg_scan_shards_o_direct(AggCtx *main_ctx,
                                               SlotcaskDb *sdb) {
    if (!sdb || sdb->num_streams <= 0) return;

    AggOdSegArg *args = NULL;
    size_t nargs = 0, cap = 0;
    int stop_flag = 0;
    FILE *parent_out = g_out;

    for (int s = 0; s < sdb->num_streams; s++) {
        char stream_dir[PATH_MAX];
        snprintf(stream_dir, sizeof(stream_dir),
                 "%s/data/streams/%03d", sdb->data_dir, s);
        DIR *dh = opendir(stream_dir);
        if (!dh) continue;
        struct dirent *de;
        while ((de = readdir(dh)) != NULL) {
            size_t nlen = strlen(de->d_name);
            if (nlen < 4 || strcmp(de->d_name + nlen - 4, ".dat") != 0)
                continue;
            if (nargs >= cap) {
                size_t newcap = cap ? cap * 2 : 64;
                AggOdSegArg *t = realloc(args, newcap * sizeof(AggOdSegArg));
                if (!t) { closedir(dh); goto run; }
                args = t;
                cap  = newcap;
            }
            AggOdSegArg *a = &args[nargs];
            memset(a, 0, sizeof(*a));
            snprintf(a->seg_path, PATH_MAX, "%s/%s", stream_dir, de->d_name);
            a->slot_size   = sdb->slot_size;
            a->format      = sdb->format;
            agg_ctx_clone_shared(&a->local, main_ctx);
            a->wrap.cb     = agg_scan_cb;
            /* wrap.ctx set in fixup pass below — realloc may move args */
            a->stop_flag   = &stop_flag;
            a->parent_out  = parent_out;
            nargs++;
        }
        closedir(dh);
    }

run:
    if (nargs == 0) { free(args); return; }
    /* Fixup: args is final — set wrap.ctx now so pointers are valid. */
    for (size_t i = 0; i < nargs; i++)
        args[i].wrap.ctx = &args[i].local;
    g_scan_stop = 0;
    parallel_for_io(agg_od_seg_worker, args, (int)nargs, sizeof(AggOdSegArg));
    for (size_t i = 0; i < nargs; i++) {
        if (args[i].local.budget_exceeded) main_ctx->budget_exceeded = 1;
        agg_ctx_merge(main_ctx, &args[i].local);
        agg_ctx_free_local(&args[i].local);
    }
    free(args);
}


/* Per-shard aggregate worker: own AggCtx, processes this shard's hashes. */
typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    CollectedHash *entries;
    int entry_count;
    AggCtx local;
    pthread_mutex_t lock;   /* guards local during parallel_for_io callbacks */
    QueryDeadline *deadline;
    int dl_counter;
} ShardAggCtx;

/* agg_batch_cb — callback for batch lookup in shard_agg_worker.
   Copies key+val into a temp block and calls agg_scan_cb. */
static int agg_batch_cb(const uint8_t hash[16],
                         const void *key, size_t klen,
                         const void *value, size_t vlen,
                         void *ctx_ptr) {
    ShardAggCtx *sa = (ShardAggCtx *)ctx_ptr;
    if (query_deadline_tick(sa->deadline, &sa->dl_counter)) return 1;

    SlotHeader hdr = {0};
    memcpy(hdr.hash, hash, 16);
    hdr.flag = 1;
    hdr.key_len = (uint16_t)klen;
    hdr.value_len = (uint32_t)vlen;

    uint8_t stk[2048];
    uint8_t *block = (klen + vlen + 1 < sizeof(stk))
        ? stk : malloc(klen + vlen);
    if (block) {
        memcpy(block, key, klen);
        memcpy(block + klen, value, vlen);
        pthread_mutex_lock(&sa->lock);
        agg_scan_cb(&hdr, block, &sa->local);
        pthread_mutex_unlock(&sa->lock);
        if (block != stk) free(block);
    }
    return 0;
}

static void *shard_agg_worker(void *arg) {
    ShardAggCtx *sa = (ShardAggCtx *)arg;
    if (sa->entry_count == 0) return NULL;

    SlotcaskSchemaInfo info = {
        .splits = sa->sch->splits,
        .slot_size = sa->sch->slot_size,
        .streams = sa->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(sa->db_root, sa->object, &info);
    if (!sdb) return NULL;

    /* Extract hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)sa->entry_count * sizeof(*hashes));
    if (!hashes) return NULL;
    for (int ei = 0; ei < sa->entry_count; ei++)
        memcpy(hashes[ei], sa->entries[ei].hash, 16);

    /* Batch resolve+fetch — callback fires for each found record. The new
       two-phase model resolves all KF shards internally (no pre-grouping
       needed) and parallelizes segment reads across unique segment files. */
    pthread_mutex_init(&sa->lock, NULL);
    slotcask_bulk_resolve_and_fetch(sdb, hashes, (size_t)sa->entry_count,
                                     sa, agg_batch_cb);
    pthread_mutex_destroy(&sa->lock);

    free(hashes);
    return NULL;
}

/* Orchestrate parallel indexed aggregate: fan out per-shard workers with
   local AggCtx each, then merge into main_ctx. */
static void parallel_indexed_agg(AggCtx *main_ctx, const char *db_root,
                                 const char *object, const Schema *sch,
                                 CollectedHash *batch, int batch_count) {
    int group_starts[1024], group_sizes[1024];
    int nshard_groups = shard_group_batch(batch, batch_count, group_starts, group_sizes, 1024);

    ShardAggCtx *workers = calloc(nshard_groups, sizeof(ShardAggCtx));
    for (int g = 0; g < nshard_groups; g++) {
        workers[g].db_root = db_root;
        workers[g].object = object;
        workers[g].sch = sch;
        workers[g].entries = &batch[group_starts[g]];
        workers[g].entry_count = group_sizes[g];
        workers[g].deadline = main_ctx->deadline;
        agg_ctx_clone_shared(&workers[g].local, main_ctx);
    }

    if (batch_count < 1024 || nshard_groups <= 2) {
        for (int g = 0; g < nshard_groups; g++) shard_agg_worker(&workers[g]);
    } else {
        parallel_for_io(shard_agg_worker, workers, nshard_groups, sizeof(ShardAggCtx));
    }

    for (int g = 0; g < nshard_groups; g++) {
        if (workers[g].local.budget_exceeded) main_ctx->budget_exceeded = 1;
        agg_ctx_merge(main_ctx, &workers[g].local);
        agg_ctx_free_local(&workers[g].local);
    }
    free(workers);
}

/* Sort context for qsort */
static AggSpec *g_sort_specs;
static int g_sort_nspecs;
static char g_sort_field[256];
static int g_sort_desc;
static int g_sort_ngroups;
static char (*g_sort_group_fields)[256];

static int agg_sort_cmp(const void *a, const void *b) {
    AggBucket *ba = *(AggBucket **)a;
    AggBucket *bb = *(AggBucket **)b;

    /* Check if order_by is a group_by field */
    int ga = -1, gb = -1;
    for (int i = 0; i < g_sort_ngroups; i++) {
        if (strcmp(g_sort_group_fields[i], g_sort_field) == 0) {
            ga = gb = i; break;
        }
    }
    if (ga >= 0) {
        int cmp = strcmp(ba->group_vals[ga], bb->group_vals[gb]);
        return g_sort_desc ? -cmp : cmp;
    }

    /* Otherwise it's an aggregate alias */
    double va = agg_bucket_value(ba, g_sort_specs, g_sort_nspecs, g_sort_field);
    double vb = agg_bucket_value(bb, g_sort_specs, g_sort_nspecs, g_sort_field);
    if (va < vb) return g_sort_desc ? 1 : -1;
    if (va > vb) return g_sort_desc ? -1 : 1;
    return 0;
}

static void agg_free(AggCtx *ctx) {
    /* Arena owns every per-bucket allocation — one bulk free vs
       O(buckets × 5) malloc-companion frees in the pre-arena path. */
    agg_arena_free(&ctx->arena);
    free(ctx->ht);
    ctx->ht = NULL;
    ctx->ht_cap = ctx->ht_mask = 0;
    ctx->total_buckets = 0;
    /* specs is borrowed from the caller (cmd_aggregate / cmd_aggregate_tree) —
       do NOT free it here to avoid double-free. */
    ctx->specs = NULL;
}

/* Format a double, removing trailing zeros */
static void fmt_double(char *buf, size_t sz, double v) {
    snprintf(buf, sz, "%.6f", v);
    /* Trim trailing zeros after decimal point */
    char *dot = strchr(buf, '.');
    if (dot) {
        char *end = buf + strlen(buf) - 1;
        while (end > dot && *end == '0') *end-- = '\0';
        if (end == dot) *end = '\0';
    }
}

/* Same-field MIN/MAX fast path: when the agg field == the (single,
   rangeable) criterion field, we can walk the agg field's btree in
   MIN/MAX direction inside the criterion's bounds and take the first
   leaf entry per shard. No KeySet, no record fetch — answer comes
   straight from the index key.
   Two planner branches arrived at the same code (CPD flagged this as
   a 77-line dup). Centralised here.

   Returns 1 if the criterion shape qualified (caller treats it as
   "answered, return 0"). Returns 0 if not (caller falls through to a
   later plan). Encodes bounds into the caller-supplied buf1/buf2 stack
   buffers since the btree iter holds spans into them across the loop. */
static int agg_minmax_same_field_btree(
        const char *db_root, const char *object, const char *field,
        const char *alias, const TypedField *agg_tf, int splits,
        int is_max, const SearchCriterion *crit,
        const char *format, const char *delimiter, int want_total) {
    int rangeable = (crit->op == OP_EQUAL ||
                     crit->op == OP_GREATER ||
                     crit->op == OP_GREATER_EQ ||
                     crit->op == OP_LESS ||
                     crit->op == OP_LESS_EQ ||
                     crit->op == OP_BETWEEN);
    if (!rangeable || strcmp(crit->field, field) != 0) return 0;

    uint8_t buf1[1032], buf2[1032];
    size_t len1 = 0, len2 = 0;
    const char *min_v = "";    size_t min_l = 0; int min_x = 0;
    const char *max_v = "\xff\xff\xff\xff"; size_t max_l = 4; int max_x = 0;
    switch (crit->op) {
    case OP_EQUAL:
        encode_criterion_value(agg_tf, crit->value, strlen(crit->value), buf1, &len1);
        min_v = (const char *)buf1; min_l = len1;
        max_v = (const char *)buf1; max_l = len1;
        break;
    case OP_GREATER:
        encode_criterion_value(agg_tf, crit->value, strlen(crit->value), buf1, &len1);
        min_v = (const char *)buf1; min_l = len1; min_x = 1;
        break;
    case OP_GREATER_EQ:
        encode_criterion_value(agg_tf, crit->value, strlen(crit->value), buf1, &len1);
        min_v = (const char *)buf1; min_l = len1;
        break;
    case OP_LESS:
        encode_criterion_value(agg_tf, crit->value, strlen(crit->value), buf1, &len1);
        max_v = (const char *)buf1; max_l = len1; max_x = 1;
        break;
    case OP_LESS_EQ:
        encode_criterion_value(agg_tf, crit->value, strlen(crit->value), buf1, &len1);
        max_v = (const char *)buf1; max_l = len1;
        break;
    case OP_BETWEEN:
        encode_criterion_value(agg_tf, crit->value,  strlen(crit->value),  buf1, &len1);
        encode_criterion_value(agg_tf, crit->value2, strlen(crit->value2), buf2, &len2);
        min_v = (const char *)buf1; min_l = len1; min_x = crit->min_exclusive;
        max_v = (const char *)buf2; max_l = len2; max_x = crit->max_exclusive;
        break;
    default: break;
    }

    int n_idx = index_splits_for(splits);
    int desc = is_max ? 1 : 0;
    double best = 0.0;
    int have = 0;
    for (int s = 0; s < n_idx; s++) {
        char idx_path[PATH_MAX];
        build_idx_path(idx_path, sizeof(idx_path), db_root, object, field, s);
        BtRangeIter *it = btree_range_iter_open(
            idx_path, min_v, min_l, min_x,
            max_v, max_l, max_x, desc);
        if (!it) continue;
        const char *val; size_t vlen; const uint8_t *hash16;
        while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
            double v;
            if (decode_index_key_to_double(agg_tf,
                                            (const uint8_t *)val, vlen, &v)) {
                if (!have)        { best = v; have = 1; }
                else if (desc)    { if (v > best) best = v; }
                else              { if (v < best) best = v; }
                break;
            }
        }
        btree_range_iter_close(it);
    }

    char vbuf[64];
    fmt_double(vbuf, sizeof(vbuf), have ? best : 0.0);
    char csv_delim_local = (format && strcmp(format, "csv") == 0)
                             ? parse_csv_delim(delimiter) : 0;
    if (csv_delim_local) {
        csv_emit_cell(alias, csv_delim_local);
        OUT("\n");
        csv_emit_cell(vbuf, csv_delim_local);
        OUT("\n");
    } else if (want_total) {
        OUT("{\"rows\":{\"%s\":%s},\"total\":1}\n", alias, vbuf);
    } else {
        OUT("{\"%s\":%s}\n", alias, vbuf);
    }
    return 1;
}

/* Run the plan dispatcher into `ctx` for the given criteria tree.
   ctx must be already initialized (specs, group setup, deadline,
   shared_buffer_bytes). Mutates ctx.tree. Returns 0 on success,
   -1 if the deadline tripped or the buffer budget was exceeded.
   Extracted so the NEQ algebraic shortcut can call it twice with
   different trees (eq-set, full-set) and subtract scalars. */
static int agg_run_plan(AggCtx *ctx, CriteriaNode *tree,
                        const char *db_root, const char *object,
                        const Schema *sch) {
    ctx->tree = tree;

    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    /* Phase 1c.5/1c.6: plan_filter replaces choose_primary_source.
     * order_by=NULL: aggregate's input-row ordering is irrelevant to
     * candidate-source selection (GROUP BY result ordering is post-agg).
     * fetching=0: intersect stays index-only for count-like paths; the
     * executor fetches records itself when specs require it. */
    size_t agg_run_N = (size_t)get_live_count(db_root, object);
    FilterPlan agg_run_fp = plan_filter(tree, db_root, object, ctx->fs,
                                         sch->splits, agg_run_N,
                                         NULL /*order_by*/, 0 /*fetching*/,
                                         0 /*limit*/);

    SearchCriterion *agg_prim = (agg_run_fp.kind == FP_PRIMARY_LEAF ||
                                  agg_run_fp.kind == FP_BITMAP_SMALLER)
                                    ? (agg_run_fp.n_source > 0 ? agg_run_fp.source_leaves[0] : NULL)
                                    : NULL;
    if (agg_prim) {
        SearchCriterion *pc = agg_prim;
        enum SearchOp op = pc->op;
        int check_primary = op_needs_check_primary(op);

        /* Trigram primary: btree_dispatch has no trigram fan-out, so a
         * trigram-only field (no btree sibling) would return 0
         * candidates and silently empty the aggregate. Route through
         * build_keyset_from_trigram instead, then hand the verified
         * keys to parallel_indexed_agg with the full tree intact. */
        int picked = pick_index_for_leaf(db_root, object, pc);
        if (picked == IT_TRIGRAM) {
            KeySet *tg_ks = build_keyset_from_trigram(db_root, object,
                                                       sch->splits, pc, ctx->deadline);
            if (tg_ks) {
                CollectedHash *entries = NULL;
                size_t n = 0;
                keyset_to_collected_hashes(tg_ks, sch->splits, &entries, &n);
                parallel_indexed_agg(ctx, db_root, object, sch, entries, (int)n);
                free(entries);
                keyset_free(tg_ks);
            }
        } else {
            CollectCtx cc;
            collect_ctx_init(&cc);
            cc.splits = sch->splits;
            cc.primary_crit = pc;
            cc.check_primary = check_primary;
            cc.deadline = ctx->deadline;
            cc.tf = resolve_idx_field(ctx->fs->ts, pc->field);
            btree_dispatch(db_root, object, pc->field, sch->splits,
                           pc,
                           resolve_idx_field(ctx->fs->ts, pc->field),
                           collect_hash_cb, &cc);
            if (cc.budget_exceeded) ctx->budget_exceeded = 1;
            else parallel_indexed_agg(ctx, db_root, object, sch, cc.entries, (int)cc.count);
            collect_ctx_destroy(&cc);
        }
    } else if (agg_run_fp.kind == FP_INTERSECT) {
        CriteriaNode *saved = ctx->tree;
        ctx->tree = NULL;
        keyset_agg_from_intersect(db_root, object, sch, ctx, &agg_run_fp,
                                  saved, &ctx->tree, ctx->deadline);
        ctx->tree = saved;
    } else if (agg_run_fp.kind == FP_UNION) {
        int budget_exceeded = 0;
        keyset_agg_from_or(db_root, object, sch, ctx, agg_run_fp.or_node,
                           ctx->deadline, &budget_exceeded);
        if (budget_exceeded) ctx->budget_exceeded = 1;
    } else {
        /* Hash aggregation: parallel kf shard scan with per-worker cloned
           AggCtx (own hash table per kf shard), merged at end. */
        SlotcaskSchemaInfo info = {
            .splits = sch->splits, .slot_size = sch->slot_size,
            .streams = sch->streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
        if (sdb) parallel_agg_scan_shards_o_direct(ctx, sdb);
    }

    if (ctx->deadline->timed_out) return -1;
    if (ctx->budget_exceeded) return -1;
    return 0;
}

/* Free buckets owned by a cloned/local AggCtx. Counterpart to agg_free
   that omits `free(ctx->specs)` and `pthread_mutex_destroy()` because
   those resources are owned by the cloning origin. */
static void agg_ctx_free_local(AggCtx *ctx) {
    /* Same arena teardown as agg_free, but without freeing the shared
       resources (specs, mutex) — those are owned by the cloning origin. */
    agg_arena_free(&ctx->arena);
    free(ctx->ht);
    ctx->ht = NULL;
    ctx->ht_cap = ctx->ht_mask = 0;
    ctx->total_buckets = 0;
}

/* Per-shard worker for building one secondary group_by field's hash16→string
   map in parallel. Each worker processes one btree shard of one secondary
   field, inserting decoded string values into a pre-initialised HashStrMap.
   hsm_insert is NOT thread-safe; each worker operates on a distinct shard
   range of the same map.  Because HashStrMap uses open addressing keyed by
   the 16-byte hash16, two workers inserting different hash16s into the same
   table can race.  Therefore each worker writes into its own LOCAL HashStrMap;
   the orchestrator merges all per-worker maps into the shared sec_maps[g]
   serially after join. */
typedef struct {
    int              shard_id;
    const char      *db_root;
    const char      *object;
    const char      *gfield_s;       /* secondary field name */
    const TypedField *gtf_s;
    KeySet          *crit_ks;        /* shared read-only filter (may be NULL) */
    HashStrMap       local_map;      /* per-worker output */
    int              cap_hint;       /* hsm_init capacity hint */
    int              arena_hint;     /* hsm_init arena hint */
    int              aborted;
    int              dl_counter;
    QueryDeadline   *dl;
} SecMapBuildWorker;

static void *sec_map_build_worker(void *arg) {
    SecMapBuildWorker *w = (SecMapBuildWorker *)arg;
    if (hsm_init(&w->local_map, (size_t)w->cap_hint,
                  (size_t)w->arena_hint) != 0) {
        w->aborted = 1;
        return NULL;
    }
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path), w->db_root, w->object,
                   w->gfield_s, w->shard_id);
    BtRangeIter *it = btree_range_iter_open(idx_path, "", 0, 0,
                                             "\xff\xff\xff\xff", 4, 0, 0);
    if (!it) return NULL;
    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        if (query_deadline_tick(w->dl, &w->dl_counter)) {
            w->aborted = 1; break;
        }
        if (w->crit_ks && !keyset_contains(w->crit_ks, hash16)) continue;
        char dbuf[512];
        int dlen = decode_idx_to_buf(w->gtf_s, (const uint8_t *)val,
                                      vlen, dbuf, sizeof(dbuf), 0);
        if (dlen <= 0) continue;
        if (hsm_insert(&w->local_map, hash16, dbuf, (size_t)dlen) != 0) {
            w->aborted = 1; break;
        }
    }
    btree_range_iter_close(it);
    return NULL;
}

/* Per-shard worker for parallel indexed group_by Pass 1.

   Each worker walks ONE btree shard of the primary group field, builds
   buckets in its OWN cloned AggCtx (own arena, own ht), and populates
   its own local hbk (when Pass 2 needs it). After all workers join,
   the orchestrator merges each clone into the main ctx and translates
   per-worker hbks into the global hbk by hijacking `group_key` (which
   is unused post-Pass-1) as a src_bucket → main_bucket pointer slot. */
typedef struct {
    int                  shard_id;
    const char          *db_root;
    const char          *object;
    const char          *gfield;       /* primary group field name */
    const TypedField    *gtf;          /* primary group typed field */
    int                  n_sec;        /* count of secondary group fields */
    HashStrMap          *sec_maps;     /* shared read-only secondary maps */
    KeySet              *crit_ks;      /* shared read-only criteria filter */
    int                  hbk_needs;    /* allocate + populate local_hbk? */
    int                  hbk_cap_hint; /* per-worker hbk capacity hint */
    AggCtx               local;        /* clone of main */
    HashBktMap           local_hbk;    /* valid only if hbk_needs */
    int                  hbk_alloc_failed;
    int                  aborted;
    int                  dl_counter;
} IgbPass1Worker;

static void *igb_pass1_worker(void *arg) {
    IgbPass1Worker *w = (IgbPass1Worker *)arg;
    if (w->hbk_needs) {
        if (hbk_init(&w->local_hbk, (size_t)w->hbk_cap_hint) != 0) {
            w->hbk_alloc_failed = 1;
            w->aborted = 1;
            return NULL;
        }
    }
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path), w->db_root, w->object,
                   w->gfield, w->shard_id);
    BtRangeIter *it = btree_range_iter_open(idx_path, "", 0, 0,
                                             "\xff\xff\xff\xff", 4, 0, 0);
    if (!it) return NULL;
    char prev_enc[64]; size_t prev_enc_len = 0;
    struct AggBucket *prev_bkt = NULL;
    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        if (query_deadline_tick(w->local.deadline, &w->dl_counter)) {
            w->aborted = 1; break;
        }
        if (w->crit_ks && !keyset_contains(w->crit_ks, hash16)) continue;
        struct AggBucket *bkt;
        if (w->n_sec == 0 && prev_bkt && vlen == prev_enc_len &&
            memcmp(val, prev_enc, vlen) == 0) {
            bkt = prev_bkt;
        } else {
            char gbufs[MAX_FIELDS][512];
            char *gvals[MAX_FIELDS];
            uint8_t raw_key[AGG_INT_KEY_CAP];
            int raw_key_len = 0;

            /* Check if we can use raw integer key */
            int use_raw = w->local.use_int_keys && w->n_sec == 0 && vlen <= AGG_INT_KEY_CAP;
            if (use_raw) {
                memcpy(raw_key, val, vlen);
                raw_key_len = (int)vlen;
                /* For output, still need string */
                int n = decode_idx_to_buf(w->gtf, (const uint8_t *)val,
                                           vlen, gbufs[0], sizeof(gbufs[0]), 0);
                if (n <= 0) continue;
                gvals[0] = gbufs[0];
            } else {
                int n = decode_idx_to_buf(w->gtf, (const uint8_t *)val,
                                           vlen, gbufs[0], sizeof(gbufs[0]), 0);
                if (n <= 0) continue;
                gvals[0] = gbufs[0];
            }

            int multi_skip = 0;
            for (int g = 0; g < w->n_sec; g++) {
                const char *sval; size_t slen;
                if (!hsm_get(&w->sec_maps[g], hash16, &sval, &slen)) {
                    multi_skip = 1; break;
                }
                size_t cl = slen < sizeof(gbufs[g + 1]) - 1
                             ? slen : sizeof(gbufs[g + 1]) - 1;
                memcpy(gbufs[g + 1], sval, cl);
                gbufs[g + 1][cl] = '\0';
                gvals[g + 1] = gbufs[g + 1];
            }
            if (multi_skip) continue;
            bkt = (struct AggBucket *)agg_find_or_create(
                    &w->local, gvals, w->local.ngroups, raw_key, raw_key_len);
            if (!bkt) { w->aborted = 1; break; }
            if (w->n_sec == 0 && vlen <= sizeof(prev_enc)) {
                memcpy(prev_enc, val, vlen);
                prev_enc_len = vlen;
                prev_bkt = bkt;
            } else {
                prev_bkt = NULL; prev_enc_len = 0;
            }
        }
        AggBucket *ab = (AggBucket *)bkt;
        for (int i = 0; i < w->local.nspecs; i++) {
            if (w->local.specs[i].fn == AGG_COUNT) ab->accums[i].count++;
        }
        if (w->hbk_needs) hbk_insert(&w->local_hbk, hash16, bkt);
    }
    btree_range_iter_close(it);
    return NULL;
}

/* Bucket pointer array used by parallel-merge scatter queues. Each
   (Pass1 worker, partition) pair owns one BktArr; appends are single-
   threaded since the scatter worker only writes to its own row. */
typedef struct {
    AggBucket **arr;
    int         count;
    int         cap;
} BktArr;

static int bktarr_push(BktArr *a, AggBucket *b) {
    if (a->count >= a->cap) {
        int new_cap = a->cap == 0 ? 64 : a->cap * 2;
        AggBucket **nb = realloc(a->arr, (size_t)new_cap * sizeof(AggBucket *));
        if (!nb) return -1;
        a->arr = nb;
        a->cap = new_cap;
    }
    a->arr[a->count++] = b;
    return 0;
}

static void bktarr_free(BktArr *a) {
    free(a->arr);
    a->arr = NULL;
    a->count = a->cap = 0;
}

/* Scatter src ctx's buckets across partitions by hash(group_key) % npart.
   Each Pass1 worker owns one ScatterArg row containing npart queues. */
typedef struct {
    AggCtx *src;
    int     npart;
    BktArr *queues;        /* [npart] queues for this Pass1 worker */
    int     alloc_failed;
} ScatterArg;

static void *scatter_worker(void *arg) {
    ScatterArg *s = (ScatterArg *)arg;
    if (!s->src->ht) return NULL;
    for (size_t i = 0; i < s->src->ht_cap; i++) {
        for (AggBucket *b = s->src->ht[i]; b; b = b->next) {
            uint32_t h = agg_hash(b->group_key);
            int p = (int)(h % (uint32_t)s->npart);
            if (bktarr_push(&s->queues[p], b) != 0) {
                s->alloc_failed = 1;
                return NULL;
            }
        }
    }
    return NULL;
}

/* Per-partition merge: each partition merger reads from npass1 scatter
   queues (one per Pass1 worker, all routed to this partition) and folds
   them into its own AggCtx clone. Partitions are disjoint by group_key
   hash so workers don't contend on shared state. */
typedef struct {
    int       part_id;
    int       npass1;
    BktArr  **scatter_per_pass1;   /* [npass1] pointing into scatter[w][part_id] */
    AggCtx    local;               /* partition's own ctx (own arena, own ht) */
    int       nvals;
    int       aborted;
} PartMergeArg;

static void *part_merge_worker(void *arg) {
    PartMergeArg *p = (PartMergeArg *)arg;
    for (int w = 0; w < p->npass1; w++) {
        BktArr *q = p->scatter_per_pass1[w];
        for (int i = 0; i < q->count; i++) {
            AggBucket *src = q->arr[i];
            AggBucket *mb = agg_find_or_create(&p->local, src->group_vals, p->nvals,
                                                src->raw_key, src->raw_key_len);
            if (!mb) { p->aborted = 1; return NULL; }
            for (int k = 0; k < p->local.nspecs; k++) {
                AggAccum *sa = &src->accums[k], *da = &mb->accums[k];
                da->sum += sa->sum;
                da->count += sa->count;
                if (sa->count > 0) {
                    if (sa->min < da->min) da->min = sa->min;
                    if (sa->max > da->max) da->max = sa->max;
                }
            }
            /* Stash main bucket pointer in src->group_key for hbk translate
               (same trick as the serial merge path). */
            src->group_key = (char *)mb;
        }
    }
    return NULL;
}

/* Per-shard accumulator + worker for the single-spec SUM/AVG/MIN/MAX
   fast path inside cmd_aggregate. Each shard's btree leaf walk is
   independent, so parallel_for_io fans them out — at 25M records the
   sequential walk took 9-15 s cold (idx shards × ~30 MB sequential
   reads serialised); parallel cuts that close to single-shard cost. */
typedef struct {
    const char       *db_root, *object, *field;
    int               shard_id;
    const TypedField *tf;
    enum AggFn        fn;
    int               min_or_max, desc;
    /* Outputs. */
    double  accum;
    int64_t count;
    int     have;
} AggSingleArg;

/* Per-entry callback for the SUM/AVG fast walk. Decodes the leaf key
   bytes to a double via the indexed-numeric inverse, then accumulates
   into the worker's local sum + count. Returns 0 to keep iterating. */
typedef struct {
    const TypedField *tf;
    double            sum;
    int64_t           count;
} AggLeafSumCtx;

static int agg_leaf_sum_cb(const char *val, size_t vlen, void *raw) {
    AggLeafSumCtx *c = (AggLeafSumCtx *)raw;
    double v;
    if (decode_index_key_to_double(c->tf, (const uint8_t *)val, vlen, &v)) {
        c->sum += v;
        c->count++;
    }
    return 0;
}

static void *agg_single_shard_worker(void *raw) {
    AggSingleArg *a = (AggSingleArg *)raw;
    a->accum = 0; a->count = 0; a->have = 0;

    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path), a->db_root, a->object,
                    a->field, a->shard_id);

    /* SUM/AVG walk every leaf entry — take the bound-free, hash-free
       fast path that bypasses BtRangeIter's per-entry overhead. */
    if (a->fn == AGG_SUM || a->fn == AGG_AVG) {
        AggLeafSumCtx c = { a->tf, 0.0, 0 };
        btree_walk_all_values(idx_path, agg_leaf_sum_cb, &c);
        if (c.count > 0) {
            a->accum = c.sum;
            a->count = c.count;
            a->have  = 1;
        }
        return NULL;
    }

    /* MIN/MAX read one leaf entry per shard (ASC for MIN, DESC for MAX)
       so iter's break-after-first is already optimal — keep the iter
       path here to reuse its DESC handling via v3 last_leaf_page. */
    BtRangeIter *it = btree_range_iter_open(
        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, a->desc);
    if (!it) return NULL;

    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        double v;
        if (!decode_index_key_to_double(a->tf, (const uint8_t *)val, vlen, &v))
            continue;
        if (!a->have) { a->accum = v; a->have = 1; a->count = 1; }
        else {
            switch (a->fn) {
                case AGG_MIN: if (v < a->accum) a->accum = v; a->count++; break;
                case AGG_MAX: if (v > a->accum) a->accum = v; a->count++; break;
                default: break;
            }
        }
        break;   /* one leaf entry suffices per shard for min/max */
    }
    btree_range_iter_close(it);
    return NULL;
}

/* Per-emit staging slot for the varchar-streaming fast path. Holds
   tentative aggregate state for one distinct group key; only committed
   into ctx.ht once every emit's lookups have succeeded (or we abort
   cleanly to IGB if a run is too long). */
typedef struct {
    char    key[BT_MAX_VAL_LEN + 1];
    size_t  klen;
    int64_t total_count;                  /* AGG_COUNT total for this key */
    double  spec_sum[MAX_AGG_SPECS];      /* AGG_SUM / AGG_AVG running sum */
    double  spec_min[MAX_AGG_SPECS];      /* AGG_MIN running min */
    double  spec_max[MAX_AGG_SPECS];      /* AGG_MAX running max */
    int64_t spec_count[MAX_AGG_SPECS];    /* live-value count per spec */
} VSStaged;

typedef struct {
    VSStaged              *cur;
    const AggSpec         *specs;
    const TypedField     **spec_tfs;
    int                    nspecs;
} VSLookupCtx;

/* slotcask_lookup_by_hash callback for the varchar-streaming fast path.
   Decodes every SUM/AVG/MIN/MAX spec's field from the typed record bytes
   in one pass and accumulates into the per-emit staging slot. Returns 1
   to stop probing after the first match — hash16 collisions are rare and
   the iter's caller already attributed this exact entry to the run, so
   probing further could miscount. */
static int vs_lookup_cb(const uint8_t hash16[16],
                        const void *key, size_t klen,
                        const void *value, size_t vlen,
                        void *raw) {
    (void)hash16; (void)key; (void)klen; (void)vlen;
    VSLookupCtx *c = (VSLookupCtx *)raw;
    const uint8_t *rec = (const uint8_t *)value;
    for (int i = 0; i < c->nspecs; i++) {
        enum AggFn fn = c->specs[i].fn;
        if (fn == AGG_COUNT) continue;
        const TypedField *tf = c->spec_tfs[i];
        if (!tf) continue;
        double v;
        if (!typed_field_to_double(tf, rec + tf->offset, &v)) continue;
        switch (fn) {
        case AGG_SUM:
        case AGG_AVG:
            c->cur->spec_sum[i] += v;
            c->cur->spec_count[i]++;
            break;
        case AGG_MIN:
            if (c->cur->spec_count[i] == 0 || v < c->cur->spec_min[i])
                c->cur->spec_min[i] = v;
            c->cur->spec_count[i]++;
            break;
        case AGG_MAX:
            if (c->cur->spec_count[i] == 0 || v > c->cur->spec_max[i])
                c->cur->spec_max[i] = v;
            c->cur->spec_count[i]++;
            break;
        default: break;
        }
    }
    return 1;
}

/* Single-spec MIN/MAX driven by a KeySet over an indexed field.
   Walks each .idx shard ASC/DESC seeking the first hash contained in
   `ks` per shard (yields that shard's min/max), then aggregates across
   shards. Emits the formatted response (CSV or JSON) and FREES `ks`
   on return. Caller still frees criteria_tree + specs and returns 0.

   Two cmd_aggregate planner branches need this exact loop+emit logic
   (single-leaf KeySet path and AND-intersect KeySet path) — extracted
   to avoid the 45-line duplication CPD flagged. */
static void emit_min_max_via_keyset(const char *db_root, const char *object,
                                    const Schema *sch,
                                    const AggSpec *spec,
                                    const TypedField *agg_tf,
                                    KeySet *ks,
                                    const char *format,
                                    const char *delimiter,
                                    int want_total) {
    int n_idx = index_splits_for(sch->splits);
    int desc  = (spec->fn == AGG_MAX) ? 1 : 0;
    double best = 0.0;
    int have = 0;
    for (int s = 0; s < n_idx; s++) {
        char idx_path[PATH_MAX];
        build_idx_path(idx_path, sizeof(idx_path), db_root,
                       object, spec->field, s);
        BtRangeIter *it = btree_range_iter_open(
            idx_path, "", 0, 0,
            "\xff\xff\xff\xff", 4, 0, desc);
        if (!it) continue;
        const char *val; size_t vlen; const uint8_t *hash16;
        while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
            if (!keyset_contains(ks, hash16)) continue;
            double v;
            if (decode_index_key_to_double(agg_tf,
                                           (const uint8_t *)val,
                                           vlen, &v)) {
                if (!have) { best = v; have = 1; }
                else if (desc) { if (v > best) best = v; }
                else           { if (v < best) best = v; }
                break; /* per-shard min/max found */
            }
        }
        btree_range_iter_close(it);
    }
    keyset_free(ks);

    char vbuf[64];
    fmt_double(vbuf, sizeof(vbuf), have ? best : 0.0);
    char csv_delim_local = (format && strcmp(format, "csv") == 0)
                             ? parse_csv_delim(delimiter) : 0;
    if (csv_delim_local) {
        csv_emit_cell(spec->alias, csv_delim_local);
        OUT("\n");
        csv_emit_cell(vbuf, csv_delim_local);
        OUT("\n");
    } else if (want_total) {
        OUT("{\"rows\":{\"%s\":%s},\"total\":1}\n", spec->alias, vbuf);
    } else {
        OUT("{\"%s\":%s}\n", spec->alias, vbuf);
    }
}

/* Bitmap emit → hbk adapter.
   Called by bitmap_emit_for_shard once per matching record (via bm_emit_cb).
   Inserts (hash16 → bucket) into the caller's hbk.
   NOT thread-safe: must be called from a single thread or with external
   serialization. The bitmap IGB+hbm Phase 1b walks shards serially so
   this invariant holds. */
typedef struct {
    HashBktMap       *hbk;
    struct AggBucket *bucket;
    AggCtx           *actx;       /* agg context for spec lookup */
} BmHbkInsertCtx;

static int bm_hbk_insert_cb(const char *value, size_t vlen,
                             const uint8_t *hash16, void *ctx) {
    (void)value; (void)vlen;
    BmHbkInsertCtx *bx = (BmHbkInsertCtx *)ctx;
    hbk_insert(bx->hbk, hash16, bx->bucket);
    /* Increment count accumulators for every AGG_COUNT spec
       (same semantics as agg_scan_cb — each record matched by the
       bitmap contributes one to every count spec). */
    for (int i = 0; i < bx->actx->nspecs; i++) {
        if (bx->actx->specs[i].fn == AGG_COUNT)
            bx->bucket->accums[i].count++;
    }
    return 0;
}

/* Callback + context for bitmap group_count dict-value collection.
   bm_iter_values per shard; union unique raw values across shards. */
typedef struct {
    uint8_t  (*vals)[1024];
    size_t   *vlens;
    int      *n;
    int       cap;
} BmDictCollectCtx;

static int bm_collect_uniq_cb(const uint8_t *value, size_t vlen, void *ctx) {
    BmDictCollectCtx *c = (BmDictCollectCtx *)ctx;
    for (int i = 0; i < *c->n; i++) {
        if (c->vlens[i] == vlen && memcmp(c->vals[i], value, vlen) == 0)
            return 0;
    }
    if (*c->n >= c->cap) return 0;
    size_t cp = vlen < 1024 ? vlen : 1024;
    memcpy(c->vals[*c->n], value, cp);
    c->vlens[*c->n] = vlen;
    (*c->n)++;
    return 0;
}

/* Callback + context for slot-level bitmap intersect walk.
   bm_walk per group_by dict value → tests all criteria bitmaps at each slot. */
typedef struct {
    BitmapShard **cbm;
    uint8_t (*cvals)[1024];
    size_t  *cvl;
    int      nc;
    unsigned long *count;
} BmIntersectWalkCtx;

static int bm_intersect_walk_cb(uint32_t slot, void *ctx) {
    BmIntersectWalkCtx *x = (BmIntersectWalkCtx *)ctx;
    for (int i = 0; i < x->nc; i++) {
        if (!bm_test(x->cbm[i], x->cvals[i], x->cvl[i], slot))
            return 0;
    }
    (*x->count)++;
    return 0;
}

static int cmd_aggregate_do(const char *db_root, const char *object,
                            CriteriaNode *tree,
                            AggSpec *specs, int nspecs,
                            const char *group_by_json,
                            const char *having_json,
                            const char *order_by, int order_desc, int limit,
                            const char *format, const char *delimiter, int want_total) {
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;

    Schema sch = load_schema(db_root, object);
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);

    {
        char verr[256];
        if (validate_criteria_tree_fields(tree, fs.ts, verr, sizeof(verr)) < 0) {
            OUT("{\"error\":\"%s\"}\n", verr); return -1;
        }
        for (int i = 0; i < nspecs; i++) {
            /* count(*) / count() carry no field — skip. */
            if (specs[i].fn == AGG_COUNT && specs[i].field[0] == '\0') continue;
            if (validate_field(fs.ts, specs[i].field, "aggregate",
                               verr, sizeof(verr)) < 0) {
                OUT("{\"error\":\"%s\"}\n", verr); return -1;
            }
        }
        /* order_by validated below after group_by parse — it may reference
           an aggregate alias or a group_by field, not just a typed field. */
    }

    /* Fast path: count-only with no criteria and no group_by → metadata.
       Skipped when count has a varchar field — that needs a per-record
       elen>0 check (count(varchar) only counts non-empty content), which
       live_count can't satisfy. Other typed fields and the field-less
       form still take this O(1) path. */
    int no_group = (!group_by_json || group_by_json[0] == '\0' || strcmp(group_by_json, "[]") == 0);
    int no_having = (!having_json || having_json[0] == '\0');
    if (!tree && no_group && nspecs == 1 && specs[0].fn == AGG_COUNT) {
        int needs_varchar_filter = 0;
        if (specs[0].field[0] && fs.ts && !strchr(specs[0].field, '+')) {
            int fi = typed_field_index(fs.ts, specs[0].field);
            if (fi >= 0 && fs.ts->fields[fi].type == FT_VARCHAR)
                needs_varchar_filter = 1;
        }
        if (!needs_varchar_filter) {
            int n = get_live_count(db_root, object);
            if (want_total)
                OUT("{\"rows\":{\"%s\":%d},\"total\":1}\n", specs[0].alias, n);
            else
                OUT("{\"%s\":%d}\n", specs[0].alias, n);
            return 0;
        }
    }

    /* Fast path: single-spec SUM / AVG / MIN / MAX on an indexed non-varchar
       field with no criteria / group_by / having. Walk btree leaves
       directly across the field's idx shards, decode encoded leaf bytes
       via decode_index_key_to_double (no record fetch, no slot probe),
       accumulate per-shard, merge.

       MIN  → first leaf entry per shard (ASC iter), take global min.
       MAX  → last  leaf entry per shard (DESC iter), take global max.
       SUM  → walk every entry, sum decoded values.
       AVG  → SUM with running count, divide at end.

       Skipped for varchar — typed_field_to_double atof()'s string content,
       so sum/avg/min/max on names is degenerate by design and a leaf-byte
       decode wouldn't change the output. Skipped for composite ("a+b")
       since those aren't a single typed scalar. Saves 200-400ms per
       query at 1M records vs the full record-decode scan. */
    if (!tree && no_group && no_having && nspecs == 1 &&
        (specs[0].fn == AGG_SUM || specs[0].fn == AGG_AVG ||
         specs[0].fn == AGG_MIN || specs[0].fn == AGG_MAX) &&
        fs.ts && specs[0].field[0] && !strchr(specs[0].field, '+')) {
        int fi = typed_field_index(fs.ts, specs[0].field);
        if (fi >= 0 && fs.ts->fields[fi].type != FT_VARCHAR &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
            const TypedField *tf = &fs.ts->fields[fi];
            int n_idx = index_splits_for(sch.splits);
            enum AggFn fn = specs[0].fn;
            int min_or_max = (fn == AGG_MIN || fn == AGG_MAX);
            int desc = (fn == AGG_MAX) ? 1 : 0;
            double accum = 0.0;       /* sum, or running min/max */
            int64_t count = 0;
            int have = 0;

            /* Parallelise per-shard btree walks. Each shard's worker
               accumulates locally; merge after. At 25M records with
               16 idx shards this drops sum/avg from a sequential
               9-15 s cold to ~2-3 s as the shards' cold reads
               overlap on the I/O queue. */
            AggSingleArg *args = calloc((size_t)n_idx, sizeof(AggSingleArg));
            if (args) {
                for (int s = 0; s < n_idx; s++) {
                    args[s].db_root    = db_root;
                    args[s].object     = object;
                    args[s].field      = specs[0].field;
                    args[s].shard_id   = s;
                    args[s].tf         = tf;
                    args[s].fn         = fn;
                    args[s].min_or_max = min_or_max;
                    args[s].desc       = desc;
                }
                parallel_for_io(agg_single_shard_worker, args, n_idx,
                                 sizeof(AggSingleArg));
                for (int s = 0; s < n_idx; s++) {
                    if (!args[s].have) continue;
                    if (!have) {
                        accum = args[s].accum;
                        count = args[s].count;
                        have  = 1;
                    } else {
                        switch (fn) {
                            case AGG_SUM:
                            case AGG_AVG: accum += args[s].accum; count += args[s].count; break;
                            case AGG_MIN: if (args[s].accum < accum) accum = args[s].accum;
                                          count += args[s].count; break;
                            case AGG_MAX: if (args[s].accum > accum) accum = args[s].accum;
                                          count += args[s].count; break;
                            default: break;
                        }
                    }
                }
                free(args);
            } else {
                /* Sequential fallback on calloc OOM. */
                for (int s = 0; s < n_idx; s++) {
                    char idx_path[PATH_MAX];
                    build_idx_path(idx_path, sizeof(idx_path), db_root, object,
                                   specs[0].field, s);
                    BtRangeIter *it = btree_range_iter_open(
                        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, desc);
                    if (!it) continue;
                    const char *val; size_t vlen; const uint8_t *hash16;
                    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
                        double v;
                        if (decode_index_key_to_double(tf, (const uint8_t *)val,
                                                       vlen, &v)) {
                            if (!have) { accum = v; have = 1; count = 1; }
                            else {
                                switch (fn) {
                                    case AGG_SUM:
                                    case AGG_AVG: accum += v; count++; break;
                                    case AGG_MIN: if (v < accum) accum = v; count++; break;
                                    case AGG_MAX: if (v > accum) accum = v; count++; break;
                                    default: break;
                                }
                            }
                        }
                        if (min_or_max) break;
                    }
                    btree_range_iter_close(it);
                }
            }
            double result = 0.0;
            switch (fn) {
                case AGG_SUM: result = have ? accum : 0.0; break;
                case AGG_AVG: result = (have && count > 0) ? accum / (double)count : 0.0; break;
                case AGG_MIN:
                case AGG_MAX: result = have ? accum : 0.0; break;
                default: break;
            }
            char vbuf[64];
            fmt_double(vbuf, sizeof(vbuf), result);
            char csv_delim_local = (format && strcmp(format, "csv") == 0)
                                     ? parse_csv_delim(delimiter) : 0;
            if (csv_delim_local) {
                csv_emit_cell(specs[0].alias, csv_delim_local);
                OUT("\n");
                csv_emit_cell(vbuf, csv_delim_local);
                OUT("\n");
            } else if (want_total) {
                OUT("{\"rows\":{\"%s\":%s},\"total\":1}\n", specs[0].alias, vbuf);
            } else {
                OUT("{\"%s\":%s}\n", specs[0].alias, vbuf);
            }
            return 0;
        }
    }

    /* Fast path: bundled multi-spec aggregate, no criteria, no group, no
       having. Every spec must be COUNT (count(*) or count(non-varchar))
       or SUM/AVG/MIN/MAX on an indexed non-varchar non-composite field.
       Walks each unique non-count agg field's btree ONCE, accumulating
       sum/min/max into every spec sharing that field — `count + sum +
       avg + min + max balance` is one balance walk, not five. COUNT
       specs read live_count (typed records always carry every field;
       count metadata path semantics).

       Subsumes the single-spec path above when nspecs > 1; nspecs == 1
       still routes through that path so MIN/MAX retain their first-leaf
       early-exit (~50µs vs ~25ms full walk). Saves ~16x on the bench's
       `sum/avg/min/max balance` (404ms → ~25ms). */
    if (!tree && no_group && no_having && nspecs > 1 && fs.ts) {
        int eligible = 1;
        int has_noncount = 0;
        for (int i = 0; i < nspecs && eligible; i++) {
            AggSpec *sp = &specs[i];
            if (sp->fn == AGG_COUNT) {
                if (sp->field[0]) {
                    if (strchr(sp->field, '+')) { eligible = 0; break; }
                    int fi = typed_field_index(fs.ts, sp->field);
                    if (fi >= 0 && fs.ts->fields[fi].type == FT_VARCHAR) {
                        eligible = 0; break;
                    }
                }
                continue;
            }
            if (sp->fn != AGG_SUM && sp->fn != AGG_AVG &&
                sp->fn != AGG_MIN && sp->fn != AGG_MAX) {
                eligible = 0; break;
            }
            if (!sp->field[0] || strchr(sp->field, '+')) {
                eligible = 0; break;
            }
            int fi = typed_field_index(fs.ts, sp->field);
            if (fi < 0 || fs.ts->fields[fi].type == FT_VARCHAR ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
                eligible = 0; break;
            }
            has_noncount = 1;
        }

        if (eligible) {
            long live = (long)get_live_count(db_root, object);
            int64_t counts[MAX_AGG_SPECS] = {0};
            double  sums[MAX_AGG_SPECS]   = {0};
            double  mins[MAX_AGG_SPECS], maxs[MAX_AGG_SPECS];
            int     present[MAX_AGG_SPECS] = {0};
            for (int i = 0; i < nspecs; i++) {
                mins[i] = 1e308;
                maxs[i] = -1e308;
            }

            int processed[MAX_AGG_SPECS] = {0};
            int n_idx = index_splits_for(sch.splits);
            for (int i = 0; i < nspecs && has_noncount; i++) {
                if (processed[i] || specs[i].fn == AGG_COUNT) continue;
                const char *fld = specs[i].field;
                int fi = typed_field_index(fs.ts, fld);
                const TypedField *tf = &fs.ts->fields[fi];
                /* Collect every spec sharing this field. */
                int sibs[MAX_AGG_SPECS]; int nsibs = 0;
                for (int j = i; j < nspecs; j++) {
                    if (specs[j].fn == AGG_COUNT) continue;
                    if (strcmp(specs[j].field, fld) != 0) continue;
                    sibs[nsibs++] = j;
                    processed[j] = 1;
                }
                for (int s = 0; s < n_idx; s++) {
                    char idx_path[PATH_MAX];
                    build_idx_path(idx_path, sizeof(idx_path), db_root, object, fld, s);
                    BtRangeIter *it = btree_range_iter_open(
                        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, 0);
                    if (!it) continue;
                    const char *val; size_t vlen; const uint8_t *hash16;
                    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
                        double v;
                        if (!decode_index_key_to_double(tf, (const uint8_t *)val,
                                                       vlen, &v)) continue;
                        for (int k = 0; k < nsibs; k++) {
                            int idx = sibs[k];
                            counts[idx]++;
                            sums[idx] += v;
                            if (v < mins[idx]) mins[idx] = v;
                            if (v > maxs[idx]) maxs[idx] = v;
                            present[idx] = 1;
                        }
                    }
                    btree_range_iter_close(it);
                }
            }

            char csv_delim_local = (format && strcmp(format, "csv") == 0)
                                     ? parse_csv_delim(delimiter) : 0;
            if (csv_delim_local) {
                for (int i = 0; i < nspecs; i++) {
                    if (i > 0) { char d[2] = {csv_delim_local, '\0'}; OUT("%s", d); }
                    csv_emit_cell(specs[i].alias, csv_delim_local);
                }
                OUT("\n");
                for (int i = 0; i < nspecs; i++) {
                    if (i > 0) { char d[2] = {csv_delim_local, '\0'}; OUT("%s", d); }
                    char vbuf[64];
                    switch (specs[i].fn) {
                    case AGG_COUNT: snprintf(vbuf, sizeof(vbuf), "%ld", live); break;
                    case AGG_SUM:   fmt_double(vbuf, sizeof(vbuf), present[i] ? sums[i] : 0.0); break;
                    case AGG_AVG:   fmt_double(vbuf, sizeof(vbuf),
                                               counts[i] > 0 ? sums[i] / (double)counts[i] : 0.0); break;
                    case AGG_MIN:   fmt_double(vbuf, sizeof(vbuf), present[i] ? mins[i] : 0.0); break;
                    case AGG_MAX:   fmt_double(vbuf, sizeof(vbuf), present[i] ? maxs[i] : 0.0); break;
                    default: vbuf[0] = '\0'; break;
                    }
                    csv_emit_cell(vbuf, csv_delim_local);
                }
                OUT("\n");
            } else {
                OUT(want_total ? "{\"rows\":{" : "{");
                for (int i = 0; i < nspecs; i++) {
                    if (i > 0) OUT(",");
                    char vbuf[64];
                    switch (specs[i].fn) {
                    case AGG_COUNT: OUT("\"%s\":%ld", specs[i].alias, live); break;
                    case AGG_SUM:
                        fmt_double(vbuf, sizeof(vbuf), present[i] ? sums[i] : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf); break;
                    case AGG_AVG:
                        fmt_double(vbuf, sizeof(vbuf),
                                   counts[i] > 0 ? sums[i] / (double)counts[i] : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf); break;
                    case AGG_MIN:
                        fmt_double(vbuf, sizeof(vbuf), present[i] ? mins[i] : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf); break;
                    case AGG_MAX:
                        fmt_double(vbuf, sizeof(vbuf), present[i] ? maxs[i] : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf); break;
                    }
                }
                OUT(want_total ? "},\"total\":1}\n" : "}\n");
            }
            return 0;
        }
    }

    compile_criteria_tree(tree, fs.ts);

    /* Streaming top-N dispatch: eligible_for_topn_stream + agg_run_topn_stream.
     * Build a CSV form of group_by_json for the eligibility check. */
    {
        char gb_csv[2048] = {0};
        if (group_by_json && group_by_json[0]) {
            char tmp_fields[MAX_FIELDS][256];
            int tmp_ng = parse_group_by(group_by_json, tmp_fields);
            int pos = 0;
            for (int i = 0; i < tmp_ng && pos < (int)sizeof(gb_csv) - 1; i++) {
                if (i > 0 && pos < (int)sizeof(gb_csv) - 1)
                    gb_csv[pos++] = ',';
                int fl = (int)strlen(tmp_fields[i]);
                if (pos + fl >= (int)sizeof(gb_csv) - 1) fl = (int)sizeof(gb_csv) - 1 - pos;
                memcpy(gb_csv + pos, tmp_fields[i], (size_t)fl);
                pos += fl;
            }
            gb_csv[pos] = '\0';
        }
        if (gb_csv[0] && order_by && order_by[0] &&
            eligible_for_topn_stream(db_root, object, specs, nspecs,
                                      gb_csv, order_by, limit, having_json)) {
            QueryDeadline topn_dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
            int rc = agg_run_topn_stream(db_root, object, &sch, &fs,
                                          specs, nspecs, gb_csv,
                                          order_by, order_desc,
                                          limit, tree, &topn_dl,
                                          want_total);
            if (rc == 0) {
                return 0;
            }
            if (rc == -1) {
                OUT("{\"error\":\"query_timeout\"}\n");
                return -1;
            }
            /* rc == -2: fall through to existing scan-and-hashmap path. */
        }
    }

    /* Build context */
    AggCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.tree = tree;
    ctx.fs = &fs;
    ctx.specs = specs;
    ctx.nspecs = nspecs;
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    ctx.deadline = &dl;
    _Atomic size_t agg_budget_bytes = 0;
    atomic_init(&agg_budget_bytes, 0);
    ctx.shared_buffer_bytes = &agg_budget_bytes;

    /* Parse group_by */
    if (group_by_json && group_by_json[0])
        ctx.ngroups = parse_group_by(group_by_json, ctx.group_fields);

    {
        char verr[256];
        for (int i = 0; i < ctx.ngroups; i++) {
            if (validate_field(fs.ts, ctx.group_fields[i], "group_by",
                               verr, sizeof(verr)) < 0) {
                OUT("{\"error\":\"%s\"}\n", verr);
                return -1;
            }
        }
        /* order_by in aggregate may name (a) a typed field, (b) an
           aggregate alias, or (c) a group_by field. Accept any. */
        if (order_by && order_by[0]) {
            int ok = field_known(fs.ts, order_by);
            for (int i = 0; !ok && i < nspecs; i++)
                if (strcmp(specs[i].alias, order_by) == 0) ok = 1;
            for (int i = 0; !ok && i < ctx.ngroups; i++)
                if (strcmp(ctx.group_fields[i], order_by) == 0) ok = 1;
            if (!ok) {
                snprintf(verr, sizeof(verr),
                         "unknown field '%s' in order_by (no matching field, alias, or group_by)",
                         order_by);
                OUT("{\"error\":\"%s\"}\n", verr);
                return -1;
            }
        }
    }

    /* Pre-resolve TypedField pointers for group_by and agg specs — NULL means
       composite ("a+b") or unknown field, which falls back to decode_field. */
    for (int i = 0; i < ctx.ngroups; i++) {
        ctx.group_tfs[i] = NULL;
        if (fs.ts && !strchr(ctx.group_fields[i], '+')) {
            int idx = typed_field_index(fs.ts, ctx.group_fields[i]);
            if (idx >= 0) ctx.group_tfs[i] = &fs.ts->fields[idx];
        }
    }

    /* Fast integer-hash path for group_by: enabled only when every group
       field is a fixed-width integer type AND the concatenated raw key
       fits the inline cap (AGG_INT_KEY_CAP). Wider tuples fall back to
       the string-key path so the raw key can't truncate trailing fields. */
    ctx.use_int_keys = 0;
    if (ctx.ngroups > 0) {
        int all_int = 1;
        int total_width = 0;
        for (int i = 0; i < ctx.ngroups; i++) {
            if (!ctx.group_tfs[i]) { all_int = 0; break; }
            int w = typed_field_int_width(ctx.group_tfs[i]->type);
            if (w <= 0) { all_int = 0; break; }
            total_width += w;
        }
        ctx.use_int_keys = (all_int && total_width <= AGG_INT_KEY_CAP);
    }

    for (int i = 0; i < ctx.nspecs && i < MAX_AGG_SPECS; i++) {
        ctx.spec_tfs[i] = NULL;
        /* Resolve TypedField for COUNT specs too — agg_scan_cb's
           count(varchar field) elen>0 check needs ctx.spec_tfs to know
           the field's type. Composite ("a+b") still falls through to
           decode_field. */
        if (ctx.specs[i].field[0] && fs.ts && !strchr(ctx.specs[i].field, '+')) {
            int idx = typed_field_index(fs.ts, ctx.specs[i].field);
            if (idx >= 0) ctx.spec_tfs[i] = &fs.ts->fields[idx];
        }
    }

    /* Fast path: single-spec MIN/MAX where the criterion is on the SAME
       field as the agg (e.g. `min age where age > 30`). The criterion's
       btree IS the agg field's btree, so we just walk it within the
       criterion's bounds in MIN/MAX direction and take the first leaf —
       pure btree, no record fetch, no KeySet. Lifted out of Path A so it
       runs BEFORE walk-fetch-check, otherwise these queries pay an
       unnecessary record-fetch tax (12+ ms vs <1 ms). */
    if (tree && no_group && no_having && nspecs == 1 &&
        (specs[0].fn == AGG_MIN || specs[0].fn == AGG_MAX) &&
        fs.ts && specs[0].field[0] && !strchr(specs[0].field, '+')) {
        CriteriaNode *sf_leaf_node = NULL;
        if (tree->kind == CNODE_LEAF) sf_leaf_node = tree;
        else if (tree->kind == CNODE_AND && tree->n_children == 1 &&
                 tree->children[0]->kind == CNODE_LEAF)
            sf_leaf_node = tree->children[0];
        int sf_agg_fi = typed_field_index(fs.ts, specs[0].field);
        if (sf_leaf_node && sf_agg_fi >= 0 &&
            fs.ts->fields[sf_agg_fi].type != FT_VARCHAR &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
            const TypedField *agg_tf = &fs.ts->fields[sf_agg_fi];
            SearchCriterion *crit = &sf_leaf_node->leaf;
            if (agg_minmax_same_field_btree(
                    db_root, object, specs[0].field, specs[0].alias,
                    agg_tf, sch.splits,
                    specs[0].fn == AGG_MAX, crit, format, delimiter, want_total)) {
                return 0;
            }
        }
    }

    /* Fast path: single-spec MIN/MAX with arbitrary criteria — walk the
       agg field's btree in MIN/MAX order, fetch each record, evaluate the
       full criteria tree, return the first match per shard. Tried FIRST
       (before any keyset-building paths) because for any criterion with
       selectivity > ~0.05% it finds the answer within a few µs while the
       keyset/intersect paths spend 30-80ms building candidate sets up
       front. Per-shard 10K-walk budget bails out cleanly for low-
       selectivity criteria; falls through to the keyset / intersect /
       agg_run_plan paths below which guarantee completeness. */
    if (tree && no_group && no_having && nspecs == 1 &&
        (specs[0].fn == AGG_MIN || specs[0].fn == AGG_MAX) &&
        fs.ts && specs[0].field[0] && !strchr(specs[0].field, '+')) {
        int agg_fi = typed_field_index(fs.ts, specs[0].field);
        if (agg_fi >= 0 &&
            fs.ts->fields[agg_fi].type != FT_VARCHAR &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
            const TypedField *agg_tf = &fs.ts->fields[agg_fi];
            int n_idx = index_splits_for(sch.splits);
            int desc = (specs[0].fn == AGG_MAX) ? 1 : 0;
            WfcArg *wargs = calloc((size_t)n_idx, sizeof(WfcArg));
            if (wargs) {
                for (int s = 0; s < n_idx; s++) {
                    wargs[s].db_root   = db_root;
                    wargs[s].object    = object;
                    wargs[s].sch       = &sch;
                    wargs[s].shard_id  = s;
                    wargs[s].agg_field = specs[0].field;
                    wargs[s].agg_tf    = agg_tf;
                    wargs[s].desc      = desc;
                    wargs[s].tree      = tree;
                    wargs[s].fs        = &fs;
                    wargs[s].deadline  = &dl;
                    wargs[s].budget    = 10000;
                }
                parallel_for(wfc_worker, wargs, n_idx, sizeof(WfcArg));

                int any_indefinite = 0;
                double best = 0.0;
                int have = 0;
                for (int s = 0; s < n_idx; s++) {
                    if (wargs[s].budget_exceeded) any_indefinite = 1;
                    if (wargs[s].found) {
                        if (!have) { best = wargs[s].best; have = 1; }
                        else if (desc) { if (wargs[s].best > best) best = wargs[s].best; }
                        else           { if (wargs[s].best < best) best = wargs[s].best; }
                    }
                }
                free(wargs);
                if (dl.timed_out) {
                    OUT("{\"error\":\"query_timeout\"}\n");
                    return -1;
                }
                if (!any_indefinite) {
                    char vbuf[64];
                    fmt_double(vbuf, sizeof(vbuf), have ? best : 0.0);
                    char csv_delim_local = (format && strcmp(format, "csv") == 0)
                                             ? parse_csv_delim(delimiter) : 0;
                    if (csv_delim_local) {
                        csv_emit_cell(specs[0].alias, csv_delim_local);
                        OUT("\n");
                        csv_emit_cell(vbuf, csv_delim_local);
                        OUT("\n");
                    } else if (want_total) {
                        OUT("{\"rows\":{\"%s\":%s},\"total\":1}\n", specs[0].alias, vbuf);
                    } else {
                        OUT("{\"%s\":%s}\n", specs[0].alias, vbuf);
                    }
                    return 0;
                }
                /* else: fall through to keyset/intersect paths below */
            }
        }
    }

    /* Fast path: single-spec MIN/MAX narrowed by ONE indexed criterion.
       Build candidate KeySet from the criterion's index walk, then walk
       the agg field's btree in ASC (MIN) or DESC (MAX) order — the first
       leaf entry whose hash is in the KeySet is the answer. No record
       fetches; the agg value comes straight from the leaf bytes via
       decode_index_key_to_double. ~10× faster than agg_run_plan's path
       which collects all hashes then decodes per record.

       Reached only when walk-fetch-check above bailed (low-selectivity
       criterion) or when the criterion isn't indexed-eligible. Provides
       a complete answer regardless of selectivity. */
    if (tree && no_group && no_having && nspecs == 1 &&
        (specs[0].fn == AGG_MIN || specs[0].fn == AGG_MAX) &&
        fs.ts && specs[0].field[0] && !strchr(specs[0].field, '+')) {
        CriteriaNode *crit_leaf_node = NULL;
        if (tree->kind == CNODE_LEAF) crit_leaf_node = tree;
        else if (tree->kind == CNODE_AND && tree->n_children == 1 &&
                 tree->children[0]->kind == CNODE_LEAF)
            crit_leaf_node = tree->children[0];
        int agg_fi = typed_field_index(fs.ts, specs[0].field);
        if (crit_leaf_node && agg_fi >= 0 &&
            fs.ts->fields[agg_fi].type != FT_VARCHAR &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits) &&
            leaf_is_indexed(&crit_leaf_node->leaf, db_root, object, NULL, 0)) {
            const TypedField *agg_tf = &fs.ts->fields[agg_fi];
            SearchCriterion *crit = &crit_leaf_node->leaf;
            const TypedField *crit_tf = resolve_idx_field(fs.ts, crit->field);

            /* Same-field shortcut: when crit->field == specs[0].field, the
               criterion's btree IS the agg field's btree. No KeySet needed —
               see agg_minmax_same_field_btree for the full walk. */
            if (agg_minmax_same_field_btree(
                    db_root, object, specs[0].field, specs[0].alias,
                    agg_tf, sch.splits,
                    specs[0].fn == AGG_MAX, crit, format, delimiter, want_total)) {
                return 0;
            }

            /* Build the candidate KeySet inline as the criterion's btree
               is walked — no entries[] materialization, no second-pass
               iteration. Capacity floored at live_count so the open-
               addressing probe stays short under any matched-set size.
               Lock-free keyset_insert (CAS) keeps it parallel-safe across
               the shard-fan-out workers in btree_dispatch. */
            int total = get_live_count(db_root, object);
            KeySet *ks = keyset_new((size_t)(total > 0 ? total : 16) + 16);
            if (ks) {
                StreamKeysetCtx sk = {
                    .ks = ks, .primary_crit = crit,
                    .check_primary = op_needs_check_primary(crit->op),
                    .deadline = &dl, .dl_counter = 0, .full = 0,
                    .tf = crit_tf,
                };
                btree_dispatch(db_root, object, crit->field, sch.splits,
                               crit, crit_tf, stream_keyset_cb, &sk);

                if (!atomic_load_explicit(&sk.full, memory_order_relaxed) &&
                    !dl.timed_out && keyset_size(ks) > 0) {
                    emit_min_max_via_keyset(db_root, object, &sch,
                                            &specs[0], agg_tf, ks,
                                            format, delimiter, want_total);
                    return 0;
                }
                keyset_free(ks);
            }
        }
    }

    /* Fast path: single-spec MIN/MAX narrowed by 2+ indexed AND leaves on
       intersect-eligible ops. Build the intersected candidate KeySet via
       intersect_indexed_leaves (same machinery PRIMARY_INTERSECT uses for
       find/count/aggregate), then walk the agg field's btree in MIN/MAX
       direction; first in-KeySet hash per shard wins. No record fetches.

       Falls through to agg_run_plan for: not-pure-AND (OR / mixed), <2
       indexed leaves, any non-rangeable child, varchar/composite agg
       field, or small-primary (where the regular indexed-agg path beats
       walking subsequent leaves' btrees in full). */
    if (tree && tree->kind == CNODE_AND && tree->n_children >= 2 &&
        no_group && no_having && nspecs == 1 &&
        (specs[0].fn == AGG_MIN || specs[0].fn == AGG_MAX) &&
        fs.ts && specs[0].field[0] && !strchr(specs[0].field, '+')) {
        int agg_fi = typed_field_index(fs.ts, specs[0].field);
        if (agg_fi >= 0 &&
            fs.ts->fields[agg_fi].type != FT_VARCHAR &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
            const TypedField *agg_tf = &fs.ts->fields[agg_fi];
            SearchCriterion *isect_leaves[MAX_INTERSECT_LEAVES];
            char isect_paths[MAX_INTERSECT_LEAVES][PATH_MAX];
            int isect_partial = 0;
            int ni = find_intersect_leaves(tree, db_root, object,
                                           isect_leaves, isect_paths,
                                           &isect_partial);
            /* Partial intersect means at least one AND child stays in the
               criteria tree as a post-filter. emit_min_max_via_keyset has
               no rematch hook, so skipping it would over-report. Fall back. */
            if (ni >= 2 && !isect_partial) {
                int small_primary = 0;
                KeySet *ks = intersect_indexed_leaves(db_root, object,
                                                      sch.splits,
                                                      isect_leaves, ni,
                                                      &dl, &small_primary);
                if (ks && !small_primary && !dl.timed_out &&
                    keyset_size(ks) > 0) {
                    emit_min_max_via_keyset(db_root, object, &sch,
                                            &specs[0], agg_tf, ks,
                                            format, delimiter, want_total);
                    return 0;
                }
                if (ks) keyset_free(ks);
            }
        }
    }

    /* ===== NEQ algebraic shortcut =====
       Narrow eligibility: criteria is exactly one NEQ leaf on an indexed
       field, no group_by, no having, every spec is COUNT/SUM/AVG (algebraic
       under subtraction; MIN/MAX excluded — no closed form for the
       complement). Substitution: agg(neq X) = agg(*) − agg(eq X). Wins
       because the existing NEQ-on-indexed path collects ~all hashes and
       fetches every record, while this path runs one full scan plus a
       (typically tiny) indexed eq scan. */
    /* Unwrap implicit AND-of-one (which parse_criteria_tree builds for the
       array form `[{...}]`) down to its single leaf so the NEQ shortcut and
       the count-only fast path treat both surface forms identically. */
    CriteriaNode *neq_leaf_node = NULL;
    if (tree && tree->kind == CNODE_LEAF) neq_leaf_node = tree;
    else if (tree && tree->kind == CNODE_AND && tree->n_children == 1 &&
             tree->children[0]->kind == CNODE_LEAF)
        neq_leaf_node = tree->children[0];

    int neq_eligible = 0;
    if (no_group && (!having_json || having_json[0] == '\0') &&
        neq_leaf_node && neq_leaf_node->leaf.op == OP_NOT_EQUAL) {
        int algebraic = 1;
        for (int i = 0; i < nspecs; i++) {
            if (specs[i].fn != AGG_COUNT && specs[i].fn != AGG_SUM && specs[i].fn != AGG_AVG) {
                algebraic = 0; break;
            }
        }
        if (algebraic) {
            /* Per-shard layout — check the new <field>/<NNN>.idx layout. */
            Schema sch_neq = load_schema(db_root, object);
            if (btree_idx_exists(db_root, object, neq_leaf_node->leaf.field, sch_neq.splits))
                neq_eligible = 1;
        }
    }

    if (neq_eligible) {
        /* COUNT-only fast path: agg(count where neq=X) = live_count - count(eq=X).
           Skip the full-side scan_shards entirely (which decodes every record
           just to increment count) and use the metadata live_count instead.
           Saves ~150ms on a 1M table. */
        int count_only = 1;
        for (int i = 0; i < nspecs; i++) {
            if (specs[i].fn != AGG_COUNT) { count_only = 0; break; }
            /* count(varchar field) needs per-record elen>0 check, which
               idx_count_cb can't do — bail to the legacy two-side path. */
            if (specs[i].field[0] && ctx.spec_tfs[i] &&
                ctx.spec_tfs[i]->type == FT_VARCHAR) {
                count_only = 0; break;
            }
        }
        if (count_only) {
            SearchCriterion pos = neq_leaf_node->leaf;
            pos.op = OP_EQUAL;
            int pos_cp = op_needs_check_primary(pos.op);
            const TypedField *pos_tf = resolve_idx_field(fs.ts, pos.field);
            IdxCountCtx ic = { &pos, pos_cp, 0, &dl, 0, pos_tf };
            btree_dispatch(db_root, object, pos.field, sch.splits,
                           &pos, pos_tf, idx_count_cb, &ic);
            if (dl.timed_out) {
                OUT("{\"error\":\"query_timeout\"}\n");
                agg_free(&ctx); return -1;
            }
            int total = get_live_count(db_root, object);
            size_t neg = ((size_t)total > ic.count) ? (size_t)total - ic.count : 0;
            if (csv_delim) {
                for (int i = 0; i < nspecs; i++) {
                    if (i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                    csv_emit_cell(specs[i].alias, csv_delim);
                }
                OUT("\n");
                for (int i = 0; i < nspecs; i++) {
                    if (i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                    char vbuf[32]; snprintf(vbuf, sizeof(vbuf), "%zu", neg);
                    csv_emit_cell(vbuf, csv_delim);
                }
                OUT("\n");
            } else {
                OUT(want_total ? "{\"rows\":{" : "{");
                for (int i = 0; i < nspecs; i++) {
                    if (i > 0) OUT(",");
                    OUT("\"%s\":%zu", specs[i].alias, neg);
                }
                OUT(want_total ? "},\"total\":1}\n" : "}\n");
            }
            agg_free(&ctx); return 0;
        }

        /* Mixed COUNT/SUM/AVG: still need full-side for sum/avg.
           Two side-aggregations: eq(X) on the original ctx (its setup is
           already correct), and full(*) on a clone that shares specs/fs/
           buffer-budget read-only. recompile_criteria_tree (vs the
           cache-respecting compile_criteria_tree) is required after each
           op flip so match_typed sees the new op rather than the stale
           cached one. */
        SearchCriterion *leaf = &neq_leaf_node->leaf;
        enum SearchOp saved_op = leaf->op;
        leaf->op = OP_EQUAL;
        recompile_criteria_tree(tree, fs.ts);
        int rc_eq = agg_run_plan(&ctx, tree, db_root, object, &sch);
        leaf->op = saved_op;
        recompile_criteria_tree(tree, fs.ts);

        AggCtx ctx_full;
        agg_ctx_clone_shared(&ctx_full, &ctx);
        if (rc_eq == 0) (void)agg_run_plan(&ctx_full, NULL, db_root, object, &sch);

        if (dl.timed_out) {
            OUT("{\"error\":\"query_timeout\"}\n");
            agg_ctx_free_local(&ctx_full);
            agg_free(&ctx);
            return -1;
        }
        if (ctx.budget_exceeded || ctx_full.budget_exceeded) {
            OUT(QUERY_BUFFER_ERR);
            agg_ctx_free_local(&ctx_full);
            agg_free(&ctx);
            return -1;
        }

        /* Pull the single bucket from each side. Either may be empty:
           agg_eq is empty when no record matches (NEQ matches everything),
           agg_full is empty only on an empty table. */
        int n_eq = 0, n_full = 0;
        AggBucket **bs_eq = agg_collect(&ctx, &n_eq);
        AggBucket **bs_full = agg_collect(&ctx_full, &n_full);
        AggAccum *acc_eq = (n_eq > 0) ? bs_eq[0]->accums : NULL;
        AggAccum *acc_full = (n_full > 0) ? bs_full[0]->accums : NULL;

        /* Emit single-bucket no-group output, subtracting per spec. */
        if (csv_delim) {
            for (int i = 0; i < nspecs; i++) {
                if (i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                csv_emit_cell(specs[i].alias, csv_delim);
            }
            OUT("\n");
            for (int i = 0; i < nspecs; i++) {
                if (i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                int64_t cnt = (acc_full ? acc_full[i].count : 0) - (acc_eq ? acc_eq[i].count : 0);
                double sum = (acc_full ? acc_full[i].sum : 0.0) - (acc_eq ? acc_eq[i].sum : 0.0);
                char vbuf[64];
                switch (specs[i].fn) {
                    case AGG_COUNT: snprintf(vbuf, sizeof(vbuf), "%ld", (long)cnt); break;
                    case AGG_SUM:   fmt_double(vbuf, sizeof(vbuf), sum); break;
                    case AGG_AVG:   fmt_double(vbuf, sizeof(vbuf), cnt > 0 ? sum / (double)cnt : 0.0); break;
                    default:        vbuf[0] = '\0'; break;
                }
                csv_emit_cell(vbuf, csv_delim);
            }
            OUT("\n");
        } else {
            OUT(want_total ? "{\"rows\":{" : "{");
            for (int i = 0; i < nspecs; i++) {
                if (i > 0) OUT(",");
                int64_t cnt = (acc_full ? acc_full[i].count : 0) - (acc_eq ? acc_eq[i].count : 0);
                double sum = (acc_full ? acc_full[i].sum : 0.0) - (acc_eq ? acc_eq[i].sum : 0.0);
                char vbuf[64];
                switch (specs[i].fn) {
                    case AGG_COUNT: OUT("\"%s\":%ld", specs[i].alias, (long)cnt); break;
                    case AGG_SUM:
                        fmt_double(vbuf, sizeof(vbuf), sum);
                        OUT("\"%s\":%s", specs[i].alias, vbuf); break;
                    case AGG_AVG:
                        fmt_double(vbuf, sizeof(vbuf), cnt > 0 ? sum / (double)cnt : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf); break;
                    default: break;
                }
            }
            OUT(want_total ? "},\"total\":1}\n" : "}\n");
        }

        free(bs_eq); free(bs_full);
        agg_ctx_free_local(&ctx_full);
        agg_free(&ctx);
        return 0;
    }

    /* Fast path: aggregate(s) with criteria, no group_by, no having.
       Builds a candidate KeySet from the indexed criteria (LEAF /
       INTERSECT / KEYSET planner kinds), then walks each unique non-
       count agg field's btree once, accumulating sum/avg/min/max from
       leaf bytes only when the entry's hash is in the KeySet. COUNT
       specs read |KeySet| directly. No record fetches, no group bucketing.
       Replaces the prior fall-through to parallel_indexed_agg which had
       to fetch every matching record (~5µs each) just to read fields it
       could have decoded straight from index leaves.

       Eligibility:
         - no group_by, no having, nspecs >= 1
         - every spec is one of:
             * AGG_COUNT (no field, or non-varchar typed field)
             * AGG_SUM/AVG/MIN/MAX on an indexed non-varchar non-composite
         - criteria is present and index-friendly (planner returns
           PRIMARY_LEAF / PRIMARY_INTERSECT / PRIMARY_KEYSET)
       Single-spec MIN/MAX is *not* eligible here — the walk-fetch-check
       and same-field shortcuts above are faster for those. */
    if (tree && no_group && no_having && nspecs >= 1 && fs.ts &&
        agg_criteria_fully_covered(db_root, object, tree)) {
        int aw_eligible = 1;
        int aw_min_max_only = 1;
        for (int i = 0; i < nspecs && aw_eligible; i++) {
            AggSpec *sp = &specs[i];
            if (sp->fn == AGG_COUNT) {
                if (sp->field[0]) {
                    if (strchr(sp->field, '+')) { aw_eligible = 0; break; }
                    int fi = typed_field_index(fs.ts, sp->field);
                    if (fi >= 0 && fs.ts->fields[fi].type == FT_VARCHAR) {
                        aw_eligible = 0; break;
                    }
                }
                aw_min_max_only = 0;
                continue;
            }
            if (sp->fn != AGG_SUM && sp->fn != AGG_AVG &&
                sp->fn != AGG_MIN && sp->fn != AGG_MAX) {
                aw_eligible = 0; break;
            }
            if (!sp->field[0] || strchr(sp->field, '+')) {
                aw_eligible = 0; break;
            }
            int fi = typed_field_index(fs.ts, sp->field);
            if (fi < 0 || fs.ts->fields[fi].type == FT_VARCHAR ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
                aw_eligible = 0; break;
            }
            if (sp->fn != AGG_MIN && sp->fn != AGG_MAX) aw_min_max_only = 0;
        }
        /* Single-spec MIN/MAX already had a faster shortcut above (early
           per-shard exit on first matching leaf). Skip those here so we
           don't regress them. */
        if (nspecs == 1 && aw_min_max_only) aw_eligible = 0;

        if (aw_eligible) {
            /* Phase 1c.5/1c.6: plan_filter replaces choose_primary_source.
             * order_by=NULL / fetching=0: index-walk aggregate; no
             * input-row order needed. */
            size_t aw_N = (size_t)get_live_count(db_root, object);
            FilterPlan aw_fp = plan_filter(tree, db_root, object, &fs,
                                            sch.splits, aw_N,
                                            NULL /*order_by*/, 0 /*fetching*/,
                                            0 /*limit*/);
            KeySet *crit_ks = NULL;
            int aw_indef = 0;
            if (aw_fp.kind == FP_PRIMARY_LEAF || aw_fp.kind == FP_BITMAP_SMALLER) {
                SearchCriterion *aw_prim = aw_fp.n_source > 0 ? aw_fp.source_leaves[0] : NULL;
                /* build_keyset_from_leaf walks the btree but its callback
                   doesn't apply check_primary or length-op filtering — fine
                   for rangeable ops (eq/lt/gt/range/between/in/starts), but
                   over-includes for ops like contains/like/ends/regex/len_*
                   where the leaf walk visits all entries and the filter is
                   per-entry. Falling through to agg_run_plan / record-scan
                   keeps the result correct; the cost is higher but bounded. */
                if (!aw_prim) {
                    aw_indef = 1;
                } else {
                    enum SearchOp lop = aw_prim->op;
                    if (op_needs_check_primary(lop) || op_is_length(lop)) {
                        aw_indef = 1;
                    } else {
                        crit_ks = build_keyset_from_leaf(db_root, object, sch.splits,
                                                          aw_prim, &dl);
                    }
                }
            } else if (aw_fp.kind == FP_INTERSECT) {
                int small_primary = 0;
                crit_ks = intersect_indexed_leaves(db_root, object, sch.splits,
                                                    aw_fp.source_leaves,
                                                    aw_fp.n_source,
                                                    &dl, &small_primary);
                if (small_primary) {
                    if (crit_ks) keyset_free(crit_ks);
                    crit_ks = NULL;
                    aw_indef = 1;
                }
            } else if (aw_fp.kind == FP_UNION) {
                int budget_exceeded = 0;
                crit_ks = build_or_keyset(db_root, object, sch.splits,
                                           aw_fp.or_node, &dl,
                                           &budget_exceeded, 0);
                if (budget_exceeded) {
                    if (crit_ks) keyset_free(crit_ks);
                    crit_ks = NULL;
                    aw_indef = 1;
                }
            } else {
                aw_indef = 1; /* FP_FULL_SCAN */
            }

            if (!aw_indef && crit_ks && !dl.timed_out) {
                long match_count = (long)keyset_size(crit_ks);
                int64_t counts[MAX_AGG_SPECS] = {0};
                double  sums[MAX_AGG_SPECS]   = {0};
                double  mins[MAX_AGG_SPECS], maxs[MAX_AGG_SPECS];
                int     present[MAX_AGG_SPECS] = {0};
                for (int i = 0; i < nspecs; i++) {
                    mins[i] = 1e308;
                    maxs[i] = -1e308;
                }
                int processed[MAX_AGG_SPECS] = {0};
                int n_idx = index_splits_for(sch.splits);
                for (int i = 0; i < nspecs; i++) {
                    if (processed[i] || specs[i].fn == AGG_COUNT) continue;
                    const char *fld = specs[i].field;
                    int fi = typed_field_index(fs.ts, fld);
                    /* typed_field_index returns -1 when the field name
                       isn't in the schema. Skip — callers above have
                       already validated specs against the schema, but
                       Coverity CID 1693851 can't see that, and a real
                       runtime mismatch shouldn't deref fields[-1]. */
                    if (fi < 0) { processed[i] = 1; continue; }
                    const TypedField *tf = &fs.ts->fields[fi];
                    int sibs[MAX_AGG_SPECS]; int nsibs = 0;
                    for (int j = i; j < nspecs; j++) {
                        if (specs[j].fn == AGG_COUNT) continue;
                        if (strcmp(specs[j].field, fld) != 0) continue;
                        sibs[nsibs++] = j;
                        processed[j] = 1;
                    }
                    /* Parallelise across the agg field's idx shards. Each
                       worker walks one shard with local accumulators; we
                       merge after parallel_for returns. Per-worker memory
                       is small (4 × MAX_AGG_SPECS × 8B ≈ 1KB) so spawn
                       cost is the only worry — n_idx ≤ 128 here, well
                       within the pool's tolerance. */
                    AwcShardArg *wargs = calloc((size_t)n_idx, sizeof(AwcShardArg));
                    int64_t (*w_counts)[MAX_AGG_SPECS] =
                        calloc((size_t)n_idx, sizeof(*w_counts));
                    double (*w_sums)[MAX_AGG_SPECS] =
                        calloc((size_t)n_idx, sizeof(*w_sums));
                    double (*w_mins)[MAX_AGG_SPECS] =
                        calloc((size_t)n_idx, sizeof(*w_mins));
                    double (*w_maxs)[MAX_AGG_SPECS] =
                        calloc((size_t)n_idx, sizeof(*w_maxs));
                    int (*w_present)[MAX_AGG_SPECS] =
                        calloc((size_t)n_idx, sizeof(*w_present));
                    if (!wargs || !w_counts || !w_sums || !w_mins ||
                        !w_maxs || !w_present) {
                        free(wargs); free(w_counts); free(w_sums);
                        free(w_mins); free(w_maxs); free(w_present);
                        keyset_free(crit_ks);
                        OUT(QUERY_BUFFER_ERR);
                        return -1;
                    }
                    for (int s = 0; s < n_idx; s++) {
                        for (int k = 0; k < nsibs; k++) {
                            w_mins[s][sibs[k]] = 1e308;
                            w_maxs[s][sibs[k]] = -1e308;
                        }
                        wargs[s].db_root  = db_root;
                        wargs[s].object   = object;
                        wargs[s].fld      = fld;
                        wargs[s].shard_id = s;
                        wargs[s].tf       = tf;
                        wargs[s].crit_ks  = crit_ks;
                        wargs[s].sibs     = sibs;
                        wargs[s].nsibs    = nsibs;
                        wargs[s].deadline = &dl;
                        wargs[s].counts   = w_counts[s];
                        wargs[s].sums     = w_sums[s];
                        wargs[s].mins     = w_mins[s];
                        wargs[s].maxs     = w_maxs[s];
                        wargs[s].present  = w_present[s];
                    }
                    parallel_for(awc_shard_worker, wargs, n_idx, sizeof(AwcShardArg));
                    /* Merge per-shard results into the outer accumulators. */
                    for (int s = 0; s < n_idx; s++) {
                        for (int k = 0; k < nsibs; k++) {
                            int idx = sibs[k];
                            counts[idx] += w_counts[s][idx];
                            sums[idx]   += w_sums[s][idx];
                            if (w_present[s][idx]) {
                                if (w_mins[s][idx] < mins[idx]) mins[idx] = w_mins[s][idx];
                                if (w_maxs[s][idx] > maxs[idx]) maxs[idx] = w_maxs[s][idx];
                                present[idx] = 1;
                            }
                        }
                    }
                    free(wargs); free(w_counts); free(w_sums);
                    free(w_mins); free(w_maxs); free(w_present);
                }

                /* Emit. COUNT specs use match_count (= |KeySet|). */
                char csv_delim_local = (format && strcmp(format, "csv") == 0)
                                         ? parse_csv_delim(delimiter) : 0;
                if (csv_delim_local) {
                    for (int i = 0; i < nspecs; i++) {
                        if (i > 0) { char d[2] = { csv_delim_local, '\0' }; OUT("%s", d); }
                        csv_emit_cell(specs[i].alias, csv_delim_local);
                    }
                    OUT("\n");
                    for (int i = 0; i < nspecs; i++) {
                        if (i > 0) { char d[2] = { csv_delim_local, '\0' }; OUT("%s", d); }
                        char vbuf[64];
                        if (specs[i].fn == AGG_COUNT) {
                            snprintf(vbuf, sizeof(vbuf), "%ld", match_count);
                        } else {
                            double r = 0.0;
                            switch (specs[i].fn) {
                                case AGG_SUM: r = sums[i]; break;
                                case AGG_AVG: r = counts[i] > 0 ? sums[i] / (double)counts[i] : 0.0; break;
                                case AGG_MIN: r = present[i] ? mins[i] : 0.0; break;
                                case AGG_MAX: r = present[i] ? maxs[i] : 0.0; break;
                                default: break;
                            }
                            fmt_double(vbuf, sizeof(vbuf), r);
                        }
                        csv_emit_cell(vbuf, csv_delim_local);
                    }
                    OUT("\n");
                } else {
                    OUT(want_total ? "{\"rows\":{" : "{");
                    for (int i = 0; i < nspecs; i++) {
                        if (i > 0) OUT(",");
                        if (specs[i].fn == AGG_COUNT) {
                            OUT("\"%s\":%ld", specs[i].alias, match_count);
                        } else {
                            double r = 0.0;
                            switch (specs[i].fn) {
                                case AGG_SUM: r = sums[i]; break;
                                case AGG_AVG: r = counts[i] > 0 ? sums[i] / (double)counts[i] : 0.0; break;
                                case AGG_MIN: r = present[i] ? mins[i] : 0.0; break;
                                case AGG_MAX: r = present[i] ? maxs[i] : 0.0; break;
                                default: break;
                            }
                            char vbuf[64];
                            fmt_double(vbuf, sizeof(vbuf), r);
                            OUT("\"%s\":%s", specs[i].alias, vbuf);
                        }
                    }
                    OUT(want_total ? "},\"total\":1}\n" : "}\n");
                }

                keyset_free(crit_ks);
                return 0;
            }
            if (crit_ks) keyset_free(crit_ks);
            /* else: fall through to agg_run_plan / record-scan path. */
        }
    }

    int igb_done = 0;

    /* Fast path: varchar group_by + COUNT / SUM / AVG / MIN / MAX + finite
       limit, no criteria / having / order_by. The IGB path below would walk
       every leaf entry, hash each into a per-shard table, scatter+merge,
       then truncate to limit — wasting nearly all the work for high-
       cardinality varchars (e.g., `group by username count limit 10` over
       25M unique usernames runs 5-7 s building a 25M-entry hash table just
       to keep 10 rows).

       idx_shards are hash16-routed (per-record), so the SAME varchar value
       can appear across multiple shards. Each shard's btree is sorted ASC
       by key. K-way merge: keep one cursor per shard pointing at its
       current key; the global next distinct key is min(active cursors).
       For COUNT, simply sum the run lengths. For SUM/AVG/MIN/MAX, also
       collect the run's hash16s and fetch each record via
       slotcask_lookup_by_hash to decode the agg field values into a
       per-emit staging slot. Commit staging to ctx.ht at the end so the
       standard collect / sort / limit / emit pipeline applies.

       Per-run hash16 cap (VS_RUN_HASH_CAP): runs longer than this signal a
       low-cardinality dataset where IGB's hbk-driven Pass-2 walk is
       cheaper than millions of per-record lookups. Abort and fall
       through to IGB cleanly — nothing has been written to ctx.ht yet. */
#define VS_RUN_HASH_CAP 16384
    /* VS path stops at `limit` entries — ctx.ht never has the full group
       count.  When the caller needs total, fall through to IGB (which builds
       the full hash table) so agg_total_groups is exact. */
    int vs_eligible = (!want_total && tree == NULL && no_having &&
                       (!order_by || !order_by[0]) &&
                       limit > 0 && limit <= 100000 &&
                       ctx.ngroups == 1 && ctx.group_tfs[0] &&
                       ctx.group_tfs[0]->type == FT_VARCHAR &&
                       !strchr(ctx.group_fields[0], '+') &&
                       btree_idx_exists(db_root, object,
                                        ctx.group_fields[0], sch.splits));
    /* Per-spec eligibility:
        - COUNT(*) or COUNT(group_field) — no lookup needed
        - SUM/AVG/MIN/MAX on an indexed non-varchar non-composite field
          (record fetch by hash16 + typed_field_to_double). */
    int vs_need_lookup = 0;
    if (vs_eligible) {
        for (int i = 0; i < nspecs; i++) {
            AggSpec *sp = &specs[i];
            if (sp->fn == AGG_COUNT) {
                if (sp->field[0] && strcmp(sp->field, ctx.group_fields[0]) != 0) {
                    vs_eligible = 0; break;
                }
                continue;
            }
            if (sp->fn != AGG_SUM && sp->fn != AGG_AVG &&
                sp->fn != AGG_MIN && sp->fn != AGG_MAX) {
                vs_eligible = 0; break;
            }
            if (!ctx.spec_tfs[i] || ctx.spec_tfs[i]->type == FT_VARCHAR ||
                strchr(sp->field, '+') ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
                vs_eligible = 0; break;
            }
            vs_need_lookup = 1;
        }
    }
    SlotcaskDb *vs_sdb = NULL;
    if (vs_eligible && vs_need_lookup) {
        SlotcaskSchemaInfo info = {
            .splits = sch.splits, .slot_size = sch.slot_size,
            .streams = sch.streams,
        };
        vs_sdb = slotcask_registry_get(db_root, object, &info);
        if (!vs_sdb) vs_eligible = 0;
    }
    if (vs_eligible) {
        int n_idx_g = index_splits_for(sch.splits);
        BtRangeIter **iters  = calloc((size_t)n_idx_g, sizeof(BtRangeIter *));
        char       (*cur_keys)[BT_MAX_VAL_LEN + 1] =
            calloc((size_t)n_idx_g, sizeof(*cur_keys));
        size_t      *cur_klens = calloc((size_t)n_idx_g, sizeof(size_t));
        int         *has_cur   = calloc((size_t)n_idx_g, sizeof(int));
        uint8_t    (*cur_hash16)[16] =
            calloc((size_t)n_idx_g, sizeof(*cur_hash16));
        /* Per-emit staging — accumulators built up via lookups, committed
           to ctx.ht only after every emit completes. Lets us cleanly
           abort and fall through to IGB if a run exceeds the cap without
           leaving partial buckets in ctx.ht. */
        VSStaged *staged = calloc((size_t)limit, sizeof(VSStaged));
        if (!iters || !cur_keys || !cur_klens || !has_cur || !cur_hash16 ||
            !staged) {
            free(iters); free(cur_keys); free(cur_klens);
            free(has_cur); free(cur_hash16); free(staged);
            goto vs_skip;
        }
        for (int s = 0; s < n_idx_g; s++) {
            char idx_path[PATH_MAX];
            build_idx_path(idx_path, sizeof(idx_path), db_root, object,
                            ctx.group_fields[0], s);
            iters[s] = btree_range_iter_open(
                idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, 0);
            if (!iters[s]) { has_cur[s] = 0; continue; }
            const char *v; size_t vl; const uint8_t *h;
            if (btree_range_iter_next(iters[s], &v, &vl, &h) == 1) {
                size_t cap = (vl > BT_MAX_VAL_LEN) ? BT_MAX_VAL_LEN : vl;
                memcpy(cur_keys[s], v, cap);
                cur_klens[s] = cap;
                memcpy(cur_hash16[s], h, 16);
                has_cur[s] = 1;
            }
        }

        /* Reusable per-run hash16 buffer. Stack frame is ~256 KB which is
           well under the 8 MB default thread stack; bench / TCP worker
           threads use the default. */
        uint8_t run_hashes[VS_RUN_HASH_CAP][16];
        int emitted = 0;
        int aborted = 0;
        while (emitted < limit && !aborted) {
            /* Find the active cursor with the lexicographically smallest
               current key. */
            int min_s = -1;
            for (int s = 0; s < n_idx_g; s++) {
                if (!has_cur[s]) continue;
                if (min_s < 0) { min_s = s; continue; }
                size_t mn = cur_klens[s] < cur_klens[min_s]
                            ? cur_klens[s] : cur_klens[min_s];
                int c = memcmp(cur_keys[s], cur_keys[min_s], mn);
                if (c < 0 || (c == 0 && cur_klens[s] < cur_klens[min_s]))
                    min_s = s;
            }
            if (min_s < 0) break;  /* all cursors drained */

            /* Snapshot the winning key before advancing any cursor (some
               cursors share its memory via cur_keys[]). */
            char min_key[BT_MAX_VAL_LEN + 1];
            size_t min_klen = cur_klens[min_s];
            memcpy(min_key, cur_keys[min_s], min_klen);

            /* Sum the run of equal keys across every shard that's
               currently sitting on this key. Collect hash16s for the
               record fetches if any spec needs lookup. */
            int64_t total_count = 0;
            int hash_count = 0;
            for (int s = 0; s < n_idx_g; s++) {
                while (has_cur[s] && cur_klens[s] == min_klen &&
                       memcmp(cur_keys[s], min_key, min_klen) == 0) {
                    if (vs_need_lookup) {
                        if (hash_count >= VS_RUN_HASH_CAP) {
                            aborted = 1;
                            break;
                        }
                        memcpy(run_hashes[hash_count++], cur_hash16[s], 16);
                    }
                    total_count++;
                    const char *v; size_t vl; const uint8_t *h;
                    if (btree_range_iter_next(iters[s], &v, &vl, &h) == 1) {
                        size_t cap = (vl > BT_MAX_VAL_LEN) ? BT_MAX_VAL_LEN : vl;
                        memcpy(cur_keys[s], v, cap);
                        cur_klens[s] = cap;
                        memcpy(cur_hash16[s], h, 16);
                    } else {
                        has_cur[s] = 0;
                    }
                }
                if (aborted) break;
            }
            if (aborted) break;

            VSStaged *cur = &staged[emitted];
            memcpy(cur->key, min_key, min_klen);
            cur->key[min_klen] = '\0';
            cur->klen = min_klen;
            cur->total_count = total_count;
            for (int i = 0; i < nspecs; i++) {
                cur->spec_sum[i] = 0.0;
                cur->spec_min[i] = 0.0;
                cur->spec_max[i] = 0.0;
                cur->spec_count[i] = 0;
            }

            /* Lookup each hash16, decode all SUM/AVG/MIN/MAX specs from
               the typed record in one cb invocation per record. */
            if (vs_need_lookup) {
                VSLookupCtx lc = {
                    .cur     = cur,
                    .specs   = specs,
                    .spec_tfs = ctx.spec_tfs,
                    .nspecs  = nspecs,
                };
                for (int j = 0; j < hash_count; j++) {
                    slotcask_lookup_by_hash(vs_sdb, run_hashes[j],
                                             vs_lookup_cb, &lc);
                }
            }
            emitted++;
        }

        for (int s = 0; s < n_idx_g; s++)
            if (iters[s]) btree_range_iter_close(iters[s]);
        free(iters); free(cur_keys); free(cur_klens);
        free(has_cur); free(cur_hash16);

        if (aborted) {
            /* Low-cardinality data — let IGB handle it. ctx.ht hasn't
               been touched yet because we staged everything. */
            free(staged);
            goto vs_skip;
        }

        /* Commit staging to ctx.ht — one bucket per emitted distinct key,
           with accums populated for each spec. */
        for (int i = 0; i < emitted; i++) {
            char *kvp[1] = { staged[i].key };
            AggBucket *b = agg_find_or_create(&ctx, kvp, 1, NULL, 0);
            if (!b) continue;
            for (int j = 0; j < nspecs; j++) {
                switch (specs[j].fn) {
                case AGG_COUNT:
                    b->accums[j].count = staged[i].total_count;
                    break;
                case AGG_SUM:
                case AGG_AVG:
                    b->accums[j].sum   = staged[i].spec_sum[j];
                    b->accums[j].count = staged[i].spec_count[j];
                    break;
                case AGG_MIN:
                    b->accums[j].min   = staged[i].spec_min[j];
                    b->accums[j].count = staged[i].spec_count[j];
                    break;
                case AGG_MAX:
                    b->accums[j].max   = staged[i].spec_max[j];
                    b->accums[j].count = staged[i].spec_count[j];
                    break;
                }
            }
        }
        free(staged);

        igb_done = 1;  /* ctx populated — skip the IGB block, emit below. */
        goto igb_skip;
    }
vs_skip: ;  /* empty statement: pre-C23, a label cannot be followed
               directly by a declaration. Trips -Wc23-extensions on
               clang. The `;` keeps us valid C11/17 without changing
               control flow. */

    /* Fast path: indexed group_by (single field, btree or bitmap). Walks the
       group_by btree or bitmap directly to bucket per encoded value; for any
       sum/avg/min/max specs whose target field is also indexed and
       non-varchar, walks that field's btree and attributes each entry to
       its bucket via a hash16→bid map. Skips the per-record scan entirely
       (1.1s on 1M records → tens of ms), and feeds the existing having /
       order_by / limit / emit pipeline.

       Eligibility:
         - one or more group_by fields, each indexed (btree or bitmap),
           non-composite (varchar OK); bitmap-only supports single field,
           count-only, no criteria
         - every spec is one of:
             * AGG_COUNT (with no field, or non-varchar typed field)
             * AGG_SUM/AVG/MIN/MAX on an indexed non-varchar non-composite
         - if criteria is present, it must be index-friendly (planner
           returns PRIMARY_LEAF / PRIMARY_INTERSECT / PRIMARY_KEYSET) so
           we can build a candidate KeySet to filter the group_by walk.
       Records whose group_by field encodes to zero/empty (the index
       skip-zero rule) are excluded — same semantics as the existing
       no-criteria sum/avg/min/max btree fast path. */
    int igb_eligible = (ctx.ngroups >= 1 && ctx.ngroups <= MAX_FIELDS && fs.ts &&
                        agg_criteria_fully_covered(db_root, object, tree));
    int igb_group_uses_bitmap = 0;
    int igb_needs_hbm = 0;
    if (igb_eligible) {
        for (int g = 0; g < ctx.ngroups; g++) {
            if (!ctx.group_tfs[g] || strchr(ctx.group_fields[g], '+')) {
                igb_eligible = 0;
                break;
            }
            int has_btree_g = btree_idx_exists(db_root, object,
                                                ctx.group_fields[g],
                                                sch.splits);
            if (!has_btree_g &&
                !field_has_index_type(db_root, object,
                                      ctx.group_fields[g], IT_BITMAP)) {
                igb_eligible = 0;
                break;
            }
            if (!has_btree_g) igb_group_uses_bitmap = 1;
            /* No varchar size threshold: measured indexed path beats the
               scan path even at 1M unique varchar values (one btree leaf
               walk + arena-backed bucket creation finishes faster than
               parallel record decode + per-worker bucket merges). The
               arena allocator below makes this true at every cardinality
               we benchmark; the only remaining gate is "field is indexed
               and non-composite". */
        }
    }
    if (igb_eligible) {
        for (int i = 0; i < ctx.nspecs; i++) {
            AggSpec *sp = &ctx.specs[i];
            if (sp->fn == AGG_COUNT) {
                if (sp->field[0] && ctx.spec_tfs[i] &&
                    ctx.spec_tfs[i]->type == FT_VARCHAR) {
                    igb_eligible = 0; break;  /* count(varchar): per-record elen */
                }
                continue;
            }
            /* sum/avg/min/max: need indexed non-varchar agg field. */
            if (!ctx.spec_tfs[i] ||
                ctx.spec_tfs[i]->type == FT_VARCHAR ||
                strchr(sp->field, '+') ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
                igb_eligible = 0; break;
            }
            igb_needs_hbm = 1;
        }
    }
    if (igb_eligible && igb_group_uses_bitmap && ctx.ngroups > 1) {
        /* Multi-field group_by where the primary field is bitmap-only:
           no btree → secondary-map hash16 routing impossible.
           Single-field bitmap + hbm is handled in the bitmap IGB branch
           below (Phase 1b). */
        igb_eligible = 0;
    }
    /* Multi-spec indexed group_by (count + sum/avg/min/max on indexed
       non-varchar agg fields) stays on the indexed path. The earlier
       restriction here assumed parallel record scan (~25ms) beat the
       indexed path (~120ms), but that estimate was a v1 measurement —
       v2 segment-walk record scans run ~5× slower, putting the indexed
       path firmly ahead at every scale we benchmark. The gate is kept
       in place as a no-op for diff continuity, but `igb_needs_hbm` no
       longer disqualifies. */

    /* Plan the criteria (if any). FP_FULL_SCAN means we can't build a
       candidate KeySet from the index alone — fall through to the
       per-record scan path so non-indexed leaves are evaluated correctly. */
    /* Phase 1c.5/1c.6: plan_filter replaces choose_primary_source.
     * order_by=NULL / fetching=0: IGB input-row ordering is irrelevant;
     * GROUP BY result ordering is handled post-aggregation. */
    FilterPlan igb_crit_fp; memset(&igb_crit_fp, 0, sizeof(igb_crit_fp));
    igb_crit_fp.kind = FP_FULL_SCAN;
    if (igb_eligible && tree) {
        size_t igb_N = (size_t)get_live_count(db_root, object);
        igb_crit_fp = plan_filter(tree, db_root, object, &fs,
                                   sch.splits, igb_N,
                                   NULL /*order_by*/, 0 /*fetching*/,
                                   0 /*limit*/);
        if (igb_crit_fp.kind == FP_FULL_SCAN) igb_eligible = 0;
    }

    if (igb_eligible) {
        HashBktMap   hbk = {0};
        int          hbk_ready = 0;
        KeySet      *crit_ks = NULL;
        HashStrMap  *sec_maps = NULL;
        int          n_sec = 0;
        int          aborted = 0;
        int          dl_counter = 0;
        /* Bitmap-only group_by fast path: single field, count-only.
           Walk the bitmap dict across all shards, collect unique values,
           then decide:
             no criteria  → sum bm_count per value via parallel popcount;
             bitmap-criteria → slot-level walk testing group_by bitmap
             and criteria bitmaps simultaneously (no KeySet, no kf I/O).
           Avoids 12.5M btree leaf entries at 25M. */
        if (ctx.ngroups == 1 && !igb_needs_hbm && igb_group_uses_bitmap) {
            const TypedField *gtf_bm = ctx.group_tfs[0];
            uint8_t  bm_vals[256][1024];
            size_t   bm_vlens[256];
            int      bm_n = 0;
            int      bm_dl = 0;
            for (int s = 0; s < sch.splits; s++) {
                if (query_deadline_tick(&dl, &bm_dl)) goto igb_skip;
                char bp[1024];
                bm_build_path(bp, sizeof(bp), db_root, object,
                              ctx.group_fields[0], s);
                BitmapShard *bms = bm_open(bp, 0, 0, 0, 0, 0);
                if (!bms) continue;
                BmDictCollectCtx dc = { .vals = bm_vals, .vlens = bm_vlens,
                                        .n = &bm_n, .cap = 256 };
                bm_iter_values(bms, bm_collect_uniq_cb, &dc);
                bm_close(bms);
            }
            if (bm_n == 0) { igb_done = 1; goto igb_skip; }

            if (!tree) {
                for (int i = 0; i < bm_n; i++) {
                    if (query_deadline_tick(&dl, &bm_dl)) break;
                    size_t total = bm_popcount_one_value(
                        db_root, object, ctx.group_fields[0],
                        sch.splits, bm_vals[i], bm_vlens[i]);
                    if (total == 0) continue;
                    char display[512];
                    if (decode_idx_to_buf(gtf_bm, bm_vals[i], bm_vlens[i],
                                          display, sizeof(display), 0) <= 0)
                        continue;
                    char *kvp[1] = { display };
                    AggBucket *b = agg_find_or_create(&ctx, kvp, 1, NULL, 0);
                    if (!b) break;
                    for (int j = 0; j < ctx.nspecs; j++) {
                        if (ctx.specs[j].fn == AGG_COUNT)
                            b->accums[j].count = (unsigned long)total;
                    }
                }
                igb_done = 1;
                goto igb_skip;
            }

            /* Criteria present: check eligibility for slot-level bitmap
               intersect — requires all criteria leaves be bitmap equal
               with no postfilter leaves (FP_BITMAP_SMALLER or FP_INTERSECT
               with source_is_bitmap). */
            int can_slot = 0;
            int n_crit = 0;
            if (igb_crit_fp.source_is_bitmap && igb_crit_fp.n_postfilter == 0) {
                can_slot = 1;
                for (int i = 0; i < igb_crit_fp.n_source; i++) {
                    SearchCriterion *sc = igb_crit_fp.source_leaves[i];
                    if (sc && sc->op == OP_EQUAL)
                        n_crit++;
                    else
                        { can_slot = 0; break; }
                }
                if (n_crit == 0) can_slot = 0;
            }

            if (can_slot) {
                uint8_t  cvals[MAX_INTERSECT_LEAVES][1024];
                size_t   cvlens[MAX_INTERSECT_LEAVES];
                for (int ci = 0; ci < n_crit; ci++) {
                    SearchCriterion *sc = igb_crit_fp.source_leaves[ci];
                    const TypedField *ctf = resolve_idx_field(fs.ts, sc->field);
                    if (!ctf) { can_slot = 0; break; }
                    encode_criterion_value(ctf, sc->value, strlen(sc->value),
                                           cvals[ci], &cvlens[ci]);
                    if (cvlens[ci] == 0) { can_slot = 0; break; }
                }
                if (can_slot) {
                    unsigned long counts[256] = {0};
                    for (int s = 0; s < sch.splits && !dl.timed_out; s++) {
                        if (query_deadline_tick(&dl, &bm_dl)) break;
                        char bp[1024];
                        bm_build_path(bp, sizeof(bp), db_root, object,
                                      ctx.group_fields[0], s);
                        BitmapShard *gb_bm = bm_open(bp, 0, 0, 0, 0, 0);
                        if (!gb_bm) continue;

                        BitmapShard *c_bms[MAX_INTERSECT_LEAVES];
                        int c_ok = 1;
                        for (int ci = 0; ci < n_crit; ci++) {
                            char cp[1024];
                            bm_build_path(cp, sizeof(cp), db_root, object,
                                          igb_crit_fp.source_leaves[ci]->field, s);
                            c_bms[ci] = bm_open(cp, 0, 0, 0, 0, 0);
                            if (!c_bms[ci]) { c_ok = 0; break; }
                        }
                        if (!c_ok) {
                            for (int ci = 0; ci < n_crit; ci++)
                                if (c_bms[ci]) bm_close(c_bms[ci]);
                            bm_close(gb_bm);
                            continue;
                        }

                        for (int v = 0; v < bm_n; v++) {
                            BmIntersectWalkCtx wc = {
                                .cbm = c_bms,
                                .cvals = cvals,
                                .cvl = cvlens,
                                .nc = n_crit,
                                .count = &counts[v]
                            };
                            bm_walk(gb_bm, bm_vals[v], bm_vlens[v],
                                    bm_intersect_walk_cb, &wc);
                        }

                        for (int ci = 0; ci < n_crit; ci++) bm_close(c_bms[ci]);
                        bm_close(gb_bm);
                    }

                    if (!dl.timed_out) {
                        for (int i = 0; i < bm_n; i++) {
                            if (counts[i] == 0) continue;
                            char display[512];
                            if (decode_idx_to_buf(gtf_bm, bm_vals[i], bm_vlens[i],
                                                  display, sizeof(display), 0) <= 0)
                                continue;
                            char *kvp[1] = { display };
                            AggBucket *b = agg_find_or_create(&ctx, kvp, 1, NULL, 0);
                            if (!b) break;
                            for (int j = 0; j < ctx.nspecs; j++) {
                                if (ctx.specs[j].fn == AGG_COUNT)
                                    b->accums[j].count = counts[i];
                            }
                        }
                    }
                    igb_done = 1;
                    goto igb_skip;
                }
            }

            /* Not slot-eligible → fall through to btree/scan path. */
        }

        /* Phase 1b: bitmap-only group field + hbm (sum/avg/min/max agg specs
           on indexed non-varchar agg fields).

           Strategy:
             1. Collect unique bitmap dict values (reuse bm_n / bm_vals /
                bm_vlens already filled above — we re-run collection if
                bm_n==0 because this branch is also reached when ngroups==1
                && igb_needs_hbm even without going through the count-only
                block).
             2. Check hbk memory budget; bail to igb_skip if over.
             3. For each unique value create an AggBucket (count specs
                populated lazily in the bitmap walk; hbm used by Pass 2).
             4. Walk each shard's bitmap serially, emitting hash16→bucket
                into hbk via bm_hbk_insert_cb.
             5. Set hbk_ready=1 so the shared Pass 2 block (btree agg-field
                walk) runs normally and accumulates into the right buckets.

           Limit: hbk memory check gates this to ≤ ~6 M records at default
           QUERY_BUFFER_MB=256. At larger scale the else branch below falls
           to O_DIRECT as before. */
        if (igb_group_uses_bitmap && igb_needs_hbm && !tree) {
            /* Re-collect unique values if not already done by count-only branch. */
            uint8_t  bm_vals_h[256][1024];
            size_t   bm_vlens_h[256];
            int      bm_n_h = 0;
            const TypedField *gtf_bm_h = ctx.group_tfs[0];
            int      bm_dl_h = 0;
            for (int s = 0; s < sch.splits; s++) {
                if (query_deadline_tick(&dl, &bm_dl_h)) goto igb_skip;
                char bp[1024];
                bm_build_path(bp, sizeof(bp), db_root, object,
                              ctx.group_fields[0], s);
                BitmapShard *bms = bm_open(bp, 0, 0, 0, 0, 0);
                if (!bms) continue;
                BmDictCollectCtx dc = { .vals = bm_vals_h, .vlens = bm_vlens_h,
                                        .n = &bm_n_h, .cap = 256 };
                bm_iter_values(bms, bm_collect_uniq_cb, &dc);
                bm_close(bms);
            }
            if (bm_n_h == 0) { igb_done = 1; goto igb_skip; }

            /* hbk memory budget check (same formula as btree IGB path below). */
            {
                int live_h = get_live_count(db_root, object);
                if (live_h <= 0) live_h = 1024;
                size_t cap_h = 64;
                while (cap_h * 3 < (size_t)live_h * 4) cap_h <<= 1;
                size_t hbk_bytes_h = cap_h * sizeof(HashBktEntry);
                if (hbk_bytes_h > g_query_buffer_max_bytes / 2) goto igb_skip;
            }

            /* Initialise the shared hbk used by Pass 2. */
            {
                int live_h2 = get_live_count(db_root, object);
                if (live_h2 <= 0) live_h2 = 1024;
                HashBktMap hbk_bm = {0};
                if (hbk_init(&hbk_bm, (size_t)live_h2) != 0) goto igb_skip;

                /* Need sdb for kf path inside bitmap_emit_for_shard. */
                SlotcaskSchemaInfo bm_info = {
                    .splits   = sch.splits,
                    .slot_size = sch.slot_size,
                    .streams  = sch.streams
                };
                SlotcaskDb *bm_sdb = slotcask_registry_get(db_root, object,
                                                             &bm_info);
                if (!bm_sdb) { hbk_free(&hbk_bm); goto igb_skip; }

                int bm_aborted = 0;
                for (int v = 0; v < bm_n_h && !bm_aborted; v++) {
                    /* Decode display string for bucket key. */
                    char display_h[512];
                    if (decode_idx_to_buf(gtf_bm_h,
                                          bm_vals_h[v], bm_vlens_h[v],
                                          display_h, sizeof(display_h), 0) <= 0)
                        continue;
                    char *kvp_h[1] = { display_h };
                    AggBucket *bkt_h = agg_find_or_create(&ctx, kvp_h, 1,
                                                           NULL, 0);
                    if (!bkt_h) { bm_aborted = 1; break; }

                    /* Walk all bitmap shards for this value; emit hash16 → hbk. */
                    BmHbkInsertCtx bx = { .hbk = &hbk_bm, .bucket = bkt_h, .actx = &ctx };
                    for (int s = 0; s < sch.splits && !bm_aborted; s++) {
                        if (query_deadline_tick(&dl, &bm_dl_h)) {
                            bm_aborted = 1; break;
                        }
                        bitmap_emit_for_shard(db_root, object,
                                              ctx.group_fields[0], s,
                                              bm_vals_h[v], bm_vlens_h[v],
                                              bm_hbk_insert_cb, &bx, bm_sdb);
                    }
                }

                if (bm_aborted) { hbk_free(&hbk_bm); goto igb_skip; }

                /* Hand the hbk off to Pass 2.  The Pass 2 block below checks
                   `hbk_ready` and uses the `hbk` local declared at the top of
                   the btree path.  We need to assign into that variable.
                   Declare hbk + hbk_ready at the top of the outer igb_eligible
                   block instead of deep in the btree-only path — see Task 4. */
                hbk      = hbk_bm;
                hbk_ready = 1;
                crit_ks   = NULL;  /* no criteria KeySet for bitmap path */
                sec_maps  = NULL;  /* single group field, no sec maps */
                n_sec     = 0;
                /* Pass 2 runs below; skip btree Pass 1 by jumping past it. */
                goto igb_pass2;
            }
        }
        const TypedField *gtf = ctx.group_tfs[0];
        int n_idx_g = index_splits_for(sch.splits);
        if (igb_needs_hbm) {
            int live = get_live_count(db_root, object);
            if (live <= 0) live = 1024;
            /* Estimate hbk footprint before allocating. Capacity is the
               next power of 2 such that cap*3 >= live*4 (load factor ~0.75);
               each entry is 24 bytes. If the projected footprint would
               exceed half of QUERY_BUFFER_MB, skip the indexed path and
               let the per-record scan handle it — the scan is slower at
               1M scale but has constant memory, so the user always gets
               an answer instead of a buffer-exceeded error. */
            size_t cap_est = 64;
            while (cap_est * 3 < (size_t)live * 4) cap_est <<= 1;
            size_t hbk_bytes = cap_est * sizeof(HashBktEntry);
            if (hbk_bytes > g_query_buffer_max_bytes / 2) goto igb_skip;
            if (hbk_init(&hbk, (size_t)live) == 0) hbk_ready = 1;
            else goto igb_skip;
        }

        /* If we have criteria, build a candidate KeySet from the indexed
           plan and filter Pass 1 / Pass 2 by it. FP_PRIMARY_LEAF →
           build_keyset_from_leaf, FP_INTERSECT → intersect_indexed_leaves
           (with small-primary fallthrough since that path needs record-rematch
           which we don't do here), FP_UNION → build_or_keyset. On any
           failure, fall through to the scan path. */
        if (tree) {
            if (igb_crit_fp.kind == FP_PRIMARY_LEAF || igb_crit_fp.kind == FP_BITMAP_SMALLER) {
                SearchCriterion *igb_prim = igb_crit_fp.n_source > 0 ? igb_crit_fp.source_leaves[0] : NULL;
                /* Same caveat as the no-group_by path above:
                   build_keyset_from_leaf doesn't filter via check_primary,
                   so non-rangeable ops would over-include. Fall through to
                   agg_run_plan for those — correctness over speed. */
                if (!igb_prim) {
                    if (hbk_ready) hbk_free(&hbk);
                    goto igb_skip;
                }
                enum SearchOp lop = igb_prim->op;
                if (op_needs_check_primary(lop) || op_is_length(lop)) {
                    if (hbk_ready) hbk_free(&hbk);
                    goto igb_skip;
                }
                crit_ks = build_keyset_from_leaf(db_root, object, sch.splits,
                                                  igb_prim, &dl);
            } else if (igb_crit_fp.kind == FP_INTERSECT) {
                int small_primary = 0;
                crit_ks = intersect_indexed_leaves(db_root, object, sch.splits,
                                                    igb_crit_fp.source_leaves,
                                                    igb_crit_fp.n_source,
                                                    &dl, &small_primary);
                if (small_primary) {
                    /* Small-primary intersect needs criteria_match_tree per
                       record to confirm — that's the scan-path's job, not
                       ours. Fall through cleanly. */
                    if (crit_ks) keyset_free(crit_ks);
                    if (hbk_ready) hbk_free(&hbk);
                    goto igb_skip;
                }
            } else if (igb_crit_fp.kind == FP_UNION) {
                int budget_exceeded = 0;
                crit_ks = build_or_keyset(db_root, object, sch.splits,
                                           igb_crit_fp.or_node, &dl,
                                           &budget_exceeded, 0);
                if (budget_exceeded) {
                    if (crit_ks) keyset_free(crit_ks);
                    if (hbk_ready) hbk_free(&hbk);
                    goto igb_skip;
                }
            }
            if (!crit_ks || dl.timed_out) {
                if (crit_ks) keyset_free(crit_ks);
                if (hbk_ready) hbk_free(&hbk);
                goto igb_skip;
            }
        }

        /* Multi-field group_by: walk each *secondary* group field's btree
           once and build a hash16 → encoded-value map. Pass 1 then composes
           the bucket key as [primary_value, sec_map[0].lookup(hash16),
           sec_map[1].lookup(hash16), ...]. Memory budget: ~24B per slot
           plus arena (avg 8-16B per varchar value) — capped at
           QUERY_BUFFER_MB/(2*n_sec) per map; if over, falls through to
           the per-record scan path. */
         n_sec = ctx.ngroups - 1;
        sec_maps = NULL;
        if (n_sec > 0) {
            sec_maps = calloc((size_t)n_sec, sizeof(HashStrMap));
            if (!sec_maps) {
                if (crit_ks) keyset_free(crit_ks);
                if (hbk_ready) hbk_free(&hbk);
                goto igb_skip;
            }
            int live = get_live_count(db_root, object);
            if (live <= 0) live = 1024;
            int n_idx_s = index_splits_for(sch.splits);
            for (int g = 0; g < n_sec; g++) {
                /* Per-map memory cap: split QUERY_BUFFER_MB / 2 across
                   secondary maps + the (already-allocated) hbk. */
                size_t cap_est = 64;
                while (cap_est * 3 < (size_t)live * 4) cap_est <<= 1;
                size_t map_bytes = cap_est * sizeof(HashStrEntry)
                                   + (size_t)live * 16;  /* arena estimate */
                if (map_bytes > g_query_buffer_max_bytes / (size_t)(2 * (n_sec + 1))) {
                    for (int k = 0; k < g; k++) hsm_free(&sec_maps[k]);
                    free(sec_maps); sec_maps = NULL;
                    if (crit_ks) keyset_free(crit_ks);
                    if (hbk_ready) hbk_free(&hbk);
                    goto igb_skip;
                }
                if (hsm_init(&sec_maps[g], (size_t)live, (size_t)live * 8) != 0) {
                    for (int k = 0; k < g; k++) hsm_free(&sec_maps[k]);
                    free(sec_maps); sec_maps = NULL;
                    if (crit_ks) keyset_free(crit_ks);
                    if (hbk_ready) hbk_free(&hbk);
                    goto igb_skip;
                }
                const TypedField *gtf_s = ctx.group_tfs[g + 1];
                const char *gfld_s = ctx.group_fields[g + 1];
                int sec_aborted = 0;
                int live_s = get_live_count(db_root, object);
                if (live_s <= 0) live_s = 1024;
                int per_s_hint = (live_s + n_idx_s - 1) / n_idx_s;
                int per_s_arena = per_s_hint * 12; /* avg 8-12B per entry */
                SecMapBuildWorker *sw = calloc((size_t)n_idx_s,
                                               sizeof(SecMapBuildWorker));
                if (!sw) { sec_aborted = 1; goto sec_aborted_label; }
                for (int s = 0; s < n_idx_s; s++) {
                    sw[s].shard_id   = s;
                    sw[s].db_root    = db_root;
                    sw[s].object     = object;
                    sw[s].gfield_s   = gfld_s;
                    sw[s].gtf_s      = gtf_s;
                    sw[s].crit_ks    = crit_ks;
                    sw[s].cap_hint   = per_s_hint;
                    sw[s].arena_hint = per_s_arena;
                    sw[s].dl         = &dl;
                }
                parallel_for_io(sec_map_build_worker, sw, n_idx_s,
                                sizeof(SecMapBuildWorker));
                /* Serial merge: walk each per-worker local_map and insert
                   into the shared sec_maps[g].  hsm_insert on a single map
                   is safe here because the main thread is the only writer.
                   HashStrEntry stores value as (off, len) into local_map.arena;
                   dereference via lm->arena + he->off. */
                for (int s = 0; s < n_idx_s && !sec_aborted; s++) {
                    if (sw[s].aborted) { sec_aborted = 1; break; }
                    HashStrMap *lm = &sw[s].local_map;
                    if (!lm->entries) continue;
                    for (size_t bi = 0; bi < lm->cap; bi++) {
                        HashStrEntry *he = &lm->entries[bi];
                        if (!he->occupied) continue;
                        const char *sval = lm->arena + he->off;
                        size_t      slen = he->len;
                        if (hsm_insert(&sec_maps[g], he->hash, sval,
                                        slen) != 0) {
                            sec_aborted = 1; break;
                        }
                    }
                    hsm_free(lm);
                }
                for (int s = 0; s < n_idx_s; s++) {
                    if (sw[s].local_map.entries) hsm_free(&sw[s].local_map);
                }
                free(sw); sw = NULL;
sec_aborted_label:
                if (sec_aborted) {
                    for (int k = 0; k <= g; k++) hsm_free(&sec_maps[k]);
                    free(sec_maps); sec_maps = NULL;
                    if (crit_ks) keyset_free(crit_ks);
                    if (hbk_ready) hbk_free(&hbk);
                    goto igb_skip;
                }
            }
        }

        /* Pass 1: walk group_by btree, find/create bucket per encoded
           value, increment any AGG_COUNT specs, and (for multi-spec)
           record hash16 → bucket in hbk for Pass 2.

           Two paths:

             - Parallel: spawn one worker per btree shard, each with its
               own AggCtx clone + own hbk; merge into main + translate
               hbks afterwards. Wins big on high-cardinality varchar
               group_by (8× wall on Pass 1 walk + bucket creation).

             - Serial fallback: walks shards in order, writes directly
               to main ctx + global hbk. Used when n_idx_g is small,
               cardinality is low, or the per-worker hbk allocation
               wouldn't fit in the QUERY_BUFFER_MB budget. Also kept
               as the simple path for correctness diffing.

           Within a shard, btree iter is sorted ASC, so consecutive
           entries usually share the same encoded value — cache the
           last (encoded value, bucket) pair to skip a bucket re-lookup
           on each repeat. The cache only fires for ngroups==1 since the
           composite bucket key for multi-field includes secondaries that
           vary per record even when primary repeats. */
         int run_serial = 1;
        int live_for_pass1 = get_live_count(db_root, object);
        if (live_for_pass1 <= 0) live_for_pass1 = 1024;
        if (n_idx_g >= 4 && live_for_pass1 >= 100000) {
            int per_worker_hint = (live_for_pass1 + n_idx_g - 1) / n_idx_g;
            size_t per_worker_hbk_bytes = 0;
            if (hbk_ready) {
                size_t cap = 64;
                while (cap * 3 < (size_t)per_worker_hint * 4) cap <<= 1;
                per_worker_hbk_bytes = cap * sizeof(HashBktEntry);
            }
            size_t total_extra = (size_t)n_idx_g * per_worker_hbk_bytes;
            /* Total per-worker hbk + global hbk must fit; the global
               already used ≤ half the budget at hbk_init time, leaving
               at most another half for per-worker shards. */
            if (total_extra <= g_query_buffer_max_bytes / 2) {
                IgbPass1Worker *workers = calloc((size_t)n_idx_g,
                                                  sizeof(IgbPass1Worker));
                if (workers) {
                    for (int s = 0; s < n_idx_g; s++) {
                        workers[s].shard_id = s;
                        workers[s].db_root = db_root;
                        workers[s].object = object;
                        workers[s].gfield = ctx.group_fields[0];
                        workers[s].gtf = gtf;
                        workers[s].n_sec = n_sec;
                        workers[s].sec_maps = sec_maps;
                        workers[s].crit_ks = crit_ks;
                        workers[s].hbk_needs = hbk_ready;
                        workers[s].hbk_cap_hint = per_worker_hint;
                        agg_ctx_clone_shared(&workers[s].local, &ctx);
                    }

                    parallel_for_io(igb_pass1_worker, workers, n_idx_g,
                                   sizeof(IgbPass1Worker));

                    int budget_exceeded = 0;
                    for (int s = 0; s < n_idx_g; s++) {
                        if (workers[s].aborted) aborted = 1;
                        if (workers[s].local.budget_exceeded) budget_exceeded = 1;
                    }

                    /* Merge phase. Three sub-phases:

                       1. Scatter (parallel): each Pass1 worker walks its
                          local.ht and routes buckets into per-(worker,
                          partition) queues by `agg_hash(group_key) % npart`.
                          Each worker writes only to its own row, so no
                          contention.

                       2. Per-partition merge (parallel): each merger reads
                          from all (Pass1 worker → partition) queues for
                          its partition, calls agg_find_or_create on a
                          per-partition AggCtx clone, copies accums.
                          Partitions are disjoint by group_key hash so no
                          contention on the merger ctxs.

                       3. Union (serial, cheap): splice each partition_ctx's
                          buckets into main->ht using main's hash mapping;
                          transfer partition arenas into main's arena chain
                          so partition-allocated bucket strings stay valid
                          until agg_free.

                       hbk translate stays serial — its work is < 25% of
                       merge wall-time and parallelizing safely needs an
                       atomic-CAS hbk variant we'd rather not add yet. */
                    int npart = n_idx_g;
                    BktArr **scatter = NULL;
                    ScatterArg *scatter_args = NULL;
                    PartMergeArg *merge_args = NULL;

                    if (!aborted) {
                        scatter = calloc((size_t)n_idx_g, sizeof(BktArr *));
                        scatter_args = calloc((size_t)n_idx_g, sizeof(ScatterArg));
                        merge_args = calloc((size_t)npart, sizeof(PartMergeArg));
                        if (!scatter || !scatter_args || !merge_args) {
                            aborted = 1;
                            budget_exceeded = 1;
                        } else {
                            for (int w = 0; w < n_idx_g; w++) {
                                scatter[w] = calloc((size_t)npart, sizeof(BktArr));
                                if (!scatter[w]) { aborted = 1; budget_exceeded = 1; break; }
                            }
                        }
                    }

                    /* Phase 1: scatter (parallel) */
                    if (!aborted) {
                        for (int w = 0; w < n_idx_g; w++) {
                            scatter_args[w].src = &workers[w].local;
                            scatter_args[w].npart = npart;
                            scatter_args[w].queues = scatter[w];
                        }
                        parallel_for(scatter_worker, scatter_args, n_idx_g,
                                     sizeof(ScatterArg));
                        for (int w = 0; w < n_idx_g; w++) {
                            if (scatter_args[w].alloc_failed) {
                                aborted = 1;
                                budget_exceeded = 1;
                                break;
                            }
                        }
                    }

                    /* Phase 2: per-partition merge (parallel) */
                    if (!aborted) {
                        int nvals = ctx.ngroups > 0 ? ctx.ngroups : 1;
                        for (int p = 0; p < npart; p++) {
                            merge_args[p].part_id = p;
                            merge_args[p].npass1 = n_idx_g;
                            merge_args[p].scatter_per_pass1 =
                                malloc((size_t)n_idx_g * sizeof(BktArr *));
                            if (!merge_args[p].scatter_per_pass1) {
                                aborted = 1; budget_exceeded = 1; break;
                            }
                            for (int w = 0; w < n_idx_g; w++) {
                                merge_args[p].scatter_per_pass1[w] = &scatter[w][p];
                            }
                            agg_ctx_clone_shared(&merge_args[p].local, &ctx);
                            merge_args[p].nvals = nvals;
                        }
                        if (!aborted) {
                            parallel_for(part_merge_worker, merge_args, npart,
                                         sizeof(PartMergeArg));
                            for (int p = 0; p < npart; p++) {
                                if (merge_args[p].aborted) aborted = 1;
                                if (merge_args[p].local.budget_exceeded) {
                                    budget_exceeded = 1;
                                }
                            }
                        }
                    }

                    /* Phase 3: union partition_ctxs into main ctx (serial).
                       Each partition has disjoint group_keys (by hash %
                       npart) so we can splice buckets directly into
                       main->ht via main's slot mapping without dedup.
                       Partition arenas are transferred so their string
                       allocations outlive the partition_ctx free. */
                    if (!aborted) {
                        int total_buckets = 0;
                        for (int p = 0; p < npart; p++) {
                            total_buckets += merge_args[p].local.total_buckets;
                        }
                        if (!ctx.ht && total_buckets > 0) {
                            /* Load factor 1.0 to match the existing dynamic
                               resize threshold (total > cap). Sizing once
                               upfront avoids the O(N log N) doubling tax. */
                            size_t new_cap = AGG_HT_INIT;
                            while (new_cap < (size_t)total_buckets &&
                                   new_cap < AGG_HT_MAX) {
                                new_cap <<= 1;
                            }
                            ctx.ht = calloc(new_cap, sizeof(AggBucket *));
                            if (!ctx.ht) {
                                aborted = 1; budget_exceeded = 1;
                            } else {
                                ctx.ht_cap = new_cap;
                                ctx.ht_mask = new_cap - 1;
                            }
                        }
                        for (int p = 0; p < npart && !aborted; p++) {
                            AggCtx *pc = &merge_args[p].local;
                            if (pc->ht) {
                                for (size_t i = 0; i < pc->ht_cap; i++) {
                                    AggBucket *b = pc->ht[i];
                                    while (b) {
                                        AggBucket *next = b->next;
                                        uint32_t h = agg_hash(b->group_key) &
                                                     (uint32_t)ctx.ht_mask;
                                        b->next = ctx.ht[h];
                                        ctx.ht[h] = b;
                                        ctx.total_buckets++;
                                        b = next;
                                    }
                                }
                            }
                            agg_arena_transfer(&ctx.arena, &pc->arena);
                        }
                    }

                    /* Phase 4: hbk translate (serial). Each Pass1 worker's
                       local_hbk has src_bkt pointers; src_bkt->group_key was
                       overwritten with the main bucket pointer in Phase 2. */
                    if (!aborted && hbk_ready) {
                        for (int w = 0; w < n_idx_g; w++) {
                            HashBktMap *lhbk = &workers[w].local_hbk;
                            if (!lhbk->entries) continue;
                            for (size_t i = 0; i < lhbk->cap; i++) {
                                AggBucket *src_bkt = lhbk->entries[i].val;
                                if (!src_bkt) continue;
                                AggBucket *main_bkt = (AggBucket *)src_bkt->group_key;
                                if (main_bkt) {
                                    hbk_insert(&hbk, lhbk->entries[i].hash,
                                               main_bkt);
                                }
                            }
                        }
                    }

                    if (budget_exceeded) ctx.budget_exceeded = 1;

                    /* Cleanup. agg_ctx_free_local frees ht + arena; on the
                       success path the arena was transferred to main so its
                       head is NULL and the arena_free is a no-op. On the
                       abort path the arena still holds slabs and gets freed. */
                    if (merge_args) {
                        for (int p = 0; p < npart; p++) {
                            free(merge_args[p].scatter_per_pass1);
                            agg_ctx_free_local(&merge_args[p].local);
                        }
                        free(merge_args);
                    }
                    if (scatter) {
                        for (int w = 0; w < n_idx_g; w++) {
                            if (scatter[w]) {
                                for (int p = 0; p < npart; p++) bktarr_free(&scatter[w][p]);
                                free(scatter[w]);
                            }
                        }
                        free(scatter);
                    }
                    free(scatter_args);

                    for (int s = 0; s < n_idx_g; s++) {
                        if (hbk_ready) hbk_free(&workers[s].local_hbk);
                        agg_ctx_free_local(&workers[s].local);
                    }
                    free(workers);
                    run_serial = 0;
                }
            }
        }

        if (run_serial) {
            char prev_enc[64]; size_t prev_enc_len = 0;
            struct AggBucket *prev_bkt = NULL;
            for (int s = 0; s < n_idx_g && !aborted; s++) {
                char idx_path[PATH_MAX];
                build_idx_path(idx_path, sizeof(idx_path), db_root,
                               object, ctx.group_fields[0], s);
                BtRangeIter *it = btree_range_iter_open(
                    idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, 0);
                if (!it) continue;
                const char *val; size_t vlen; const uint8_t *hash16;
                prev_enc_len = 0; prev_bkt = NULL;
                while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
                    if (query_deadline_tick(&dl, &dl_counter)) { aborted = 1; break; }
                    /* Criteria filter: skip leaf entries whose record didn't
                       match the criteria-derived KeySet. crit_ks NULL means
                       no criteria, so every entry passes. */
                    if (crit_ks && !keyset_contains(crit_ks, hash16)) continue;
                    struct AggBucket *bkt;
                    if (n_sec == 0 && prev_bkt && vlen == prev_enc_len &&
                        memcmp(val, prev_enc, vlen) == 0) {
                        bkt = prev_bkt;
                    } else {
                        char gbufs[MAX_FIELDS][512];
                        char *gvals[MAX_FIELDS];
                        uint8_t raw_key[AGG_INT_KEY_CAP];
                        int raw_key_len = 0;

                        /* Check if we can use raw integer key */
                        int use_raw = ctx.use_int_keys && n_sec == 0 && vlen <= AGG_INT_KEY_CAP;
                        if (use_raw) {
                            memcpy(raw_key, val, vlen);
                            raw_key_len = (int)vlen;
                            int n = decode_idx_to_buf(gtf, (const uint8_t *)val,
                                                      vlen, gbufs[0], sizeof(gbufs[0]), 0);
                            if (n <= 0) continue;
                            gvals[0] = gbufs[0];
                        } else {
                            int n = decode_idx_to_buf(gtf, (const uint8_t *)val,
                                                      vlen, gbufs[0], sizeof(gbufs[0]), 0);
                            if (n <= 0) continue;
                            gvals[0] = gbufs[0];
                        }
                        /* Compose composite key for ngroups > 1 by looking up
                           each secondary field's value via its hash16 map.
                           A miss means the record doesn't have that field
                           indexed (skip-zero rule) — skip the entry to match
                           the per-record path's "missing field" semantics. */
                        int multi_skip = 0;
                        for (int g = 0; g < n_sec; g++) {
                            const char *sval; size_t slen;
                            if (!hsm_get(&sec_maps[g], hash16, &sval, &slen)) {
                                multi_skip = 1;
                                break;
                            }
                            size_t cl = slen < sizeof(gbufs[g + 1]) - 1
                                         ? slen : sizeof(gbufs[g + 1]) - 1;
                            memcpy(gbufs[g + 1], sval, cl);
                            gbufs[g + 1][cl] = '\0';
                            gvals[g + 1] = gbufs[g + 1];
                        }
                        if (multi_skip) continue;
                        bkt = (struct AggBucket *)agg_find_or_create(
                                &ctx, gvals, ctx.ngroups, raw_key, raw_key_len);
                        if (!bkt) { aborted = 1; break; }
                        if (n_sec == 0 && vlen <= sizeof(prev_enc)) {
                            memcpy(prev_enc, val, vlen);
                            prev_enc_len = vlen;
                            prev_bkt = bkt;
                        } else {
                            prev_bkt = NULL; prev_enc_len = 0;
                        }
                    }
                    AggBucket *ab = (AggBucket *)bkt;
                    for (int i = 0; i < ctx.nspecs; i++) {
                        if (ctx.specs[i].fn == AGG_COUNT) ab->accums[i].count++;
                    }
                    if (hbk_ready) hbk_insert(&hbk, hash16, bkt);
                }
                btree_range_iter_close(it);
            }
        }
        if (aborted) {
            if (hbk_ready) hbk_free(&hbk);
            if (crit_ks) keyset_free(crit_ks);
            if (sec_maps) {
                for (int k = 0; k < n_sec; k++) hsm_free(&sec_maps[k]);
                free(sec_maps);
            }
            goto igb_skip;
        }

igb_pass2:
        /* Pass 2 (only when there are non-count specs on indexed agg
           fields): walk each distinct agg field's btree ONCE, even when
           multiple specs target the same field (e.g. min(balance) +
           max(balance) — single walk, both specs updated per entry).
           Per agg-btree entry: one hbk_get + one decode + N accums where
           N is the number of specs sharing that field. */
        if (hbk_ready) {
            int n_idx_a = index_splits_for(sch.splits);
            int processed[MAX_AGG_SPECS] = {0};
            for (int i = 0; i < ctx.nspecs && !aborted; i++) {
                if (processed[i] || ctx.specs[i].fn == AGG_COUNT) continue;
                const char *fld = ctx.specs[i].field;
                const TypedField *atf = ctx.spec_tfs[i];
                /* Collect every spec sharing this field. */
                int sibs[MAX_AGG_SPECS]; int nsibs = 0;
                for (int j = i; j < ctx.nspecs; j++) {
                    if (ctx.specs[j].fn == AGG_COUNT) continue;
                    if (strcmp(ctx.specs[j].field, fld) != 0) continue;
                    sibs[nsibs++] = j;
                    processed[j] = 1;
                }
                /* Batched walk with prefetching: pull up to PFB entries from
                   the btree iter into a small ring, issue an L1 prefetch for
                   each entry's hbk slot, then process the ring. By the time
                   hbk_get reads the slot, the prefetched cache line has
                   arrived from L3 → cuts per-lookup latency from ~70ns
                   (cache miss) to ~10-20ns (warm). For 1M entries that's
                   ~50ms saved on the lookup phase.

                   Hash16 is COPIED into the ring (not stored as pointer)
                   because btree_range_iter_next invalidates the previous
                   call's returned pointers. */
                #define PFB 16
                struct { uint8_t hash[16]; double v; } ring[PFB];
                for (int s = 0; s < n_idx_a && !aborted; s++) {
                    char idx_path[PATH_MAX];
                    build_idx_path(idx_path, sizeof(idx_path), db_root,
                                   object, fld, s);
                    BtRangeIter *it = btree_range_iter_open(
                        idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, 0);
                    if (!it) continue;
                    const char *val; size_t vlen; const uint8_t *hash16;
                    int filled = 0;
                    while (1) {
                        /* Refill the ring (copies hash16 — iter pointer is
                           invalidated on next iter_next call). */
                        while (filled < PFB) {
                            if (btree_range_iter_next(it, &val, &vlen, &hash16) != 1) break;
                            if (query_deadline_tick(&dl, &dl_counter)) { aborted = 1; break; }
                            double v;
                            if (!decode_index_key_to_double(atf,
                                                           (const uint8_t *)val,
                                                           vlen, &v)) continue;
                            memcpy(ring[filled].hash, hash16, 16);
                            ring[filled].v = v;
                            __builtin_prefetch(&hbk.entries[hbk_index(&hbk, hash16)], 0, 0);
                            filled++;
                        }
                        if (aborted) break;
                        if (filled == 0) break;

                        /* Drain ring (cache-warm thanks to prefetch). */
                        for (int r = 0; r < filled; r++) {
                            struct AggBucket *bkt = hbk_get(&hbk, ring[r].hash);
                            if (!bkt) continue;
                            AggBucket *ab = (AggBucket *)bkt;
                            double v = ring[r].v;
                            for (int k = 0; k < nsibs; k++) {
                                AggAccum *a = &ab->accums[sibs[k]];
                                a->count++;
                                a->sum += v;
                                if (v < a->min) a->min = v;
                                if (v > a->max) a->max = v;
                            }
                        }
                        filled = 0;
                    }
                    btree_range_iter_close(it);
                }
                #undef PFB
            }
            hbk_free(&hbk);
        }
        if (crit_ks) keyset_free(crit_ks);
        if (sec_maps) {
            for (int k = 0; k < n_sec; k++) hsm_free(&sec_maps[k]);
            free(sec_maps);
        }
        if (aborted) goto igb_skip;
        igb_done = 1;
    }
igb_skip:
    if (igb_done) {
        /* fast path populated ctx — skip agg_run_plan, fall through to
           the shared having / sort / limit / emit pipeline below. */
    } else if (agg_run_plan(&ctx, tree, db_root, object, &sch) != 0) {
        if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
        else if (ctx.budget_exceeded) OUT(QUERY_BUFFER_ERR);
        agg_free(&ctx);
        return -1;
    }

    /* Collect buckets */
    int nbuckets = 0;
    AggBucket **buckets = agg_collect(&ctx, &nbuckets);

    /* Apply having filter */
    SearchCriterion *having = NULL;
    int nhaving = 0;
    if (having_json && having_json[0])
        parse_criteria_json(having_json, &having, &nhaving);

    if (nhaving > 0) {
        int dst = 0;
        for (int i = 0; i < nbuckets; i++) {
            if (agg_having_match(buckets[i], specs, nspecs, having, nhaving))
                buckets[dst++] = buckets[i];
        }
        nbuckets = dst;
    }

    /* Sort */
    if (order_by && order_by[0]) {
        g_sort_specs = specs;
        g_sort_nspecs = nspecs;
        strncpy(g_sort_field, order_by, 255);
        g_sort_desc = order_desc;
        g_sort_ngroups = ctx.ngroups;
        g_sort_group_fields = ctx.group_fields;
        qsort(buckets, nbuckets, sizeof(AggBucket *), agg_sort_cmp);
    }

    /* Apply limit — capture pre-limit group count for total. */
    if (limit <= 0) limit = g_global_limit;
    int agg_total_groups = nbuckets;   /* distinct groups BEFORE limit truncation */
    if (nbuckets > limit) nbuckets = limit;

    /* Output — CSV path short-circuits before JSON emit. */
    if (csv_delim) {
        /* Header: group fields then aggregate aliases. */
        for (int g = 0; g < ctx.ngroups; g++) {
            if (g > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
            csv_emit_cell(ctx.group_fields[g], csv_delim);
        }
        for (int i = 0; i < nspecs; i++) {
            if (ctx.ngroups > 0 || i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
            csv_emit_cell(specs[i].alias, csv_delim);
        }
        OUT("\n");

        for (int bi = 0; bi < nbuckets; bi++) {
            AggBucket *b = buckets[bi];
            for (int g = 0; g < ctx.ngroups; g++) {
                if (g > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                csv_emit_cell(b->group_vals[g], csv_delim);
            }
            for (int i = 0; i < nspecs; i++) {
                if (ctx.ngroups > 0 || i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                AggAccum *a = &b->accums[i];
                char vbuf[64];
                switch (specs[i].fn) {
                    case AGG_COUNT: snprintf(vbuf, sizeof(vbuf), "%ld", a->count); break;
                    case AGG_SUM:   fmt_double(vbuf, sizeof(vbuf), a->sum); break;
                    case AGG_AVG:   fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->sum / a->count : 0.0); break;
                    case AGG_MIN:   fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->min : 0.0); break;
                    case AGG_MAX:   fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->max : 0.0); break;
                    default:        vbuf[0] = '\0'; break;
                }
                csv_emit_cell(vbuf, csv_delim);
            }
            OUT("\n");
        }

        free(buckets);
        free_criteria(having, nhaving);
        agg_free(&ctx);
        return 0;
    }

    if (ctx.ngroups == 0 && nbuckets == 1) {
        /* No group_by: single object */
        AggBucket *b = buckets[0];
        OUT(want_total ? "{\"rows\":{" : "{");
        for (int i = 0; i < nspecs; i++) {
            AggAccum *a = &b->accums[i];
            if (i > 0) OUT(",");
            char vbuf[64];
            switch (specs[i].fn) {
                case AGG_COUNT:
                    OUT("\"%s\":%ld", specs[i].alias, a->count);
                    break;
                case AGG_SUM:
                    fmt_double(vbuf, sizeof(vbuf), a->sum);
                    OUT("\"%s\":%s", specs[i].alias, vbuf);
                    break;
                case AGG_AVG:
                    fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->sum / a->count : 0.0);
                    OUT("\"%s\":%s", specs[i].alias, vbuf);
                    break;
                case AGG_MIN:
                    fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->min : 0.0);
                    OUT("\"%s\":%s", specs[i].alias, vbuf);
                    break;
                case AGG_MAX:
                    fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->max : 0.0);
                    OUT("\"%s\":%s", specs[i].alias, vbuf);
                    break;
            }
        }
        OUT(want_total ? "},\"total\":1}\n" : "}\n");
    } else {
        /* Group_by: array of objects */
        OUT(want_total ? "{\"rows\":[" : "[");
        for (int bi = 0; bi < nbuckets; bi++) {
            AggBucket *b = buckets[bi];
            if (bi > 0) OUT(",");
            OUT("{");
            int first = 1;
            /* Group fields */
            for (int g = 0; g < ctx.ngroups; g++) {
                if (!first) OUT(",");
                char *gv = json_escape_const(b->group_vals[g]);
                OUT("\"%s\":\"%s\"", ctx.group_fields[g], gv ? gv : "");
                free(gv);
                first = 0;
            }
            /* Aggregate values */
            for (int i = 0; i < nspecs; i++) {
                AggAccum *a = &b->accums[i];
                if (!first) OUT(",");
                char vbuf[64];
                switch (specs[i].fn) {
                    case AGG_COUNT:
                        OUT("\"%s\":%ld", specs[i].alias, a->count);
                        break;
                    case AGG_SUM:
                        fmt_double(vbuf, sizeof(vbuf), a->sum);
                        OUT("\"%s\":%s", specs[i].alias, vbuf);
                        break;
                    case AGG_AVG:
                        fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->sum / a->count : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf);
                        break;
                    case AGG_MIN:
                        fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->min : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf);
                        break;
                    case AGG_MAX:
                        fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->max : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf);
                        break;
                }
                first = 0;
            }
            OUT("}");
        }
        if (want_total) OUT("],\"total\":%d}\n", agg_total_groups);
        else OUT("]\n");
    }

    free(buckets);
    free_criteria(having, nhaving);
    agg_free(&ctx);
    return 0;
}

int cmd_aggregate(const char *db_root, const char *object,
                  const char *criteria_json, const char *group_by_json,
                  const char *aggregates_json, const char *having_json,
                  const char *order_by, int order_desc, int limit,
                  const char *format, const char *delimiter, int want_total) {
    if (!aggregates_json || aggregates_json[0] == '\0') {
        OUT("{\"error\":\"Missing aggregates\"}\n");
        return -1;
    }

    AggSpec *specs = NULL;
    int nspecs = parse_agg_specs(aggregates_json, &specs);
    if (nspecs == 0) {
        OUT("{\"error\":\"No valid aggregates\"}\n");
        free(specs);
        return -1;
    }

    CriteriaNode *tree = NULL;
    if (criteria_json && criteria_json[0]) {
        const char *perr = NULL;
        tree = parse_criteria_tree(criteria_json, &perr);
        if (perr) {
            OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
            free_criteria_tree(tree);
            free(specs);
            return -1;
        }
    }

    int r = cmd_aggregate_do(db_root, object, tree, specs, nspecs,
                            group_by_json, having_json,
                            order_by, order_desc, limit,
                            format, delimiter, want_total);
    free_criteria_tree(tree);
    free(specs);
    return r;
}

int cmd_aggregate_tree(const char *db_root, const char *object,
                       CriteriaNode *criteria_tree,
                       const NqlAggSpec *aggs, int naggs,
                       const char *group_by_csv,
                       CriteriaNode *having_tree,
                       const char *order_by, int order_desc, int limit,
                       const char *format, const char *delimiter, int want_total) {
    /* Convert NqlAggSpec[] to AggSpec[] */
    AggSpec *specs = calloc(naggs, sizeof(AggSpec));
    if (!specs) {
        OUT("{\"error\":\"out of memory\"}\n");
        return -1;
    }
    for (int i = 0; i < naggs; i++) {
        if (strcmp(aggs[i].fn, "count") == 0) specs[i].fn = AGG_COUNT;
        else if (strcmp(aggs[i].fn, "sum") == 0) specs[i].fn = AGG_SUM;
        else if (strcmp(aggs[i].fn, "avg") == 0) specs[i].fn = AGG_AVG;
        else if (strcmp(aggs[i].fn, "min") == 0) specs[i].fn = AGG_MIN;
        else if (strcmp(aggs[i].fn, "max") == 0) specs[i].fn = AGG_MAX;
        strncpy(specs[i].field, aggs[i].field, sizeof(specs[i].field) - 1);
        if (aggs[i].field[0])
            snprintf(specs[i].alias, sizeof(specs[i].alias), "%s_%s", aggs[i].fn, aggs[i].field);
        else
            strncpy(specs[i].alias, aggs[i].fn, sizeof(specs[i].alias) - 1);
    }

    /* Convert group_by_csv to group_by_json (JSON array) */
    char group_by_buf[4096] = "[";
    int gpos = 1;
    if (group_by_csv && group_by_csv[0]) {
        const char *p = group_by_csv;
        while (*p) {
            while (*p == ' ' || *p == '\t') p++;
            if (!*p) break;
            if (gpos > 1 && gpos < (int)sizeof(group_by_buf) - 1)
                group_by_buf[gpos++] = ',';
            group_by_buf[gpos++] = '"';
            while (*p && *p != ',') {
                if (gpos >= (int)sizeof(group_by_buf) - 2) break;
                group_by_buf[gpos++] = *p++;
            }
            group_by_buf[gpos++] = '"';
            if (*p == ',') p++;
        }
    }
    group_by_buf[gpos] = ']';

    /* Convert CriteriaNode *having_tree to having_json */
    char having_buf[4096] = {0};
    if (having_tree) {
        int hpos = 0, hcount = 0;
        having_buf[hpos++] = '[';
        int n_nodes = (having_tree->kind == CNODE_AND) ? having_tree->n_children : 1;
        for (int i = 0; i < n_nodes; i++) {
            CriteriaNode *hn = (having_tree->kind == CNODE_AND) ? having_tree->children[i] : having_tree;
            if (hn->kind != CNODE_LEAF) continue;
            SearchCriterion *sc = &hn->leaf;
            const char *op_str = "eq";
            switch (sc->op) {
                case OP_EQUAL:       op_str = "eq"; break;
                case OP_NOT_EQUAL:   op_str = "neq"; break;
                case OP_LESS:        op_str = "lt"; break;
                case OP_GREATER:     op_str = "gt"; break;
                case OP_LESS_EQ:     op_str = "lte"; break;
                case OP_GREATER_EQ:  op_str = "gte"; break;
                default: break;
            }
            int remain = (int)sizeof(having_buf) - hpos;
            int n = snprintf(having_buf + hpos, remain,
                            "%s{\"field\":\"%s\",\"op\":\"%s\",\"value\":\"%s\"}",
                            hcount > 0 ? "," : "",
                            sc->field, op_str, sc->value);
            if (n > 0 && n < remain) hpos += n;
            hcount++;
        }
        if (hpos + 2 <= (int)sizeof(having_buf)) {
            having_buf[hpos++] = ']';
            having_buf[hpos] = '\0';
        }
    }

    int r = cmd_aggregate_do(db_root, object, criteria_tree, specs, naggs,
                            group_by_buf,
                            having_buf[0] ? having_buf : NULL,
                            order_by, order_desc, limit,
                            format, delimiter, want_total);
    free(specs);
    return r;
}
