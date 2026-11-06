#include "types.h"
#include "type_desc.h"
#include "slotcask.h"
#include "simd.h"
#include "bitmap.h"
#include "trigram.h"
#include "io_direct.h"
#include "query_internal.h"
#include "agg_exact.h"
#include "decimal_order.h"
#include "typed_row.h"
#include <math.h>
#include <sys/stat.h>
#include <dirent.h>
/* Forward decls — defined below. */
static int agg_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *raw_ctx);
static void agg_note_error(void*,int);

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
                               uint8_t hdr2, void *ctx_ptr) {
    KeysetEmitAggCtx *ctx = (KeysetEmitAggCtx *)ctx_ptr;
    if (query_deadline_tick(ctx->dl, &ctx->dl_counter)) return 1;

    SlotHeader hdr = {0};
    memcpy(hdr.hash, hash, 16);
    hdr.flag = 1 | (((unsigned)hdr2 & 0xFF) << 8);
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
    } else {
        agg_note_error(ctx->agg_ctx, ENOMEM);   /* never a silent skip */
        return 1;
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
            hdr.flag = 1 | (((unsigned)rr.hdr2 & 0xFF) << 8);
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
            } else {
                agg_note_error(agg_ctx, ENOMEM);
                release_record_ref(&rr);
                break;
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
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &sinfo);
    if (sdb) {
        KeysetEmitAggCtx cb_ctx;
        memset(&cb_ctx, 0, sizeof(cb_ctx));
        cb_ctx.agg_ctx = agg_ctx;
        cb_ctx.dl = dl;
        pthread_mutex_init(&cb_ctx.lock, NULL);
        slotcask_bulk_resolve_and_fetch(sdb, hashes, n, &cb_ctx, keyset_emit_agg_cb);
        pthread_mutex_destroy(&cb_ctx.lock);
    } else {
        agg_note_error(agg_ctx, EIO);   /* registry miss: records unverifiable */
    }
    free(hashes);
}

static void agg_note_predicate_error(void*,int);
static void keyset_agg_from_or(const char *db_root, const char *object,
                               const Schema *sch, void *agg_ctx,
                               CriteriaNode *or_node, QueryDeadline *dl,
                               int *out_budget_exceeded) {
    /* Aggregate needs the full union — no short-circuit (target_count=0). */
    KeySet *ks = build_or_keyset(db_root, object, sch->splits, or_node, dl,
                                 out_budget_exceeded, 0);
    if (!ks){if(errno)agg_note_predicate_error(agg_ctx,errno);return;}
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
    if (!ks){if(errno)agg_note_predicate_error(agg_ctx,errno);return 0;}

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

typedef struct {
    enum AggFn fn;
    char field[256];
    char alias[256];
} AggSpec;

typedef struct {
    int64_t count;          /* COUNT result / contributing-value count */
    AggNum  num;            /* sum/min/max (agg_exact) */
    char *text_min;
    size_t text_min_len;
    char *text_max;
    size_t text_max_len;
    int text_present;
} AggAccum;

typedef enum {
    AGG_VALUE_UNSUPPORTED = -1,
    AGG_VALUE_MISSING = 0,
    AGG_VALUE_PRESENT = 1
} AggValueStatus;

typedef enum {
    AGG_RESULT_NUMBER,
    AGG_RESULT_TEXT
} AggResultKind;

typedef struct {
    AggResultKind kind;
    int present;
    AggValue val;           /* AGG_RESULT_NUMBER */
    const char *text;       /* AGG_RESULT_TEXT */
    size_t text_len;
} AggResult;

/* Aggregate × type classification shared by every execution path.
   Text types carry string aggregates (varchar/calendar min+max);
   every other type is either numeric-capable or rejects non-count
   aggregates at validation. */
static int agg_type_is_text(enum FieldType t) {
    return t == FT_VARCHAR || t == FT_DATE || t == FT_DATETIME || t == FT_DATETIMEMS;
}

/* 1 when this field's index key carries its full exact value for value
   shortcuts (D1): false only for bare packed numeric, whose ordered key has
   no display scale. Callers fall through to the record-fed route. */
static int agg_index_exact_field(const TypedSchema *ts, const TypedField *tf) {
    AggNumSpec ns;
    return agg_num_spec_for(ts, (size_t)(tf - ts->fields), &ns) &&
           agg_index_exact(&ns);
}

/* count(field) presence doctrine: these types carry an encoded unset
   marker (empty varchar content, all-zero calendar, midnight time,
   all-zero uuid) that count(field) must exclude. Mirrors the OP_EXISTS
   semantics in match_typed and get. Numeric zero is a real value and
   every fixed-width numeric type counts every record. */
static int agg_count_inspects_presence(enum FieldType t) {
    return t == FT_VARCHAR || t == FT_DATE || t == FT_DATETIME ||
           t == FT_DATETIMEMS || t == FT_TIME || t == FT_UUID;
}

/* Length-aware byte comparison — the same order as btree byte order
   (memcmp to the shorter length, then length decides). */
static int agg_text_cmp(const char *a, size_t alen, const char *b, size_t blen) {
    size_t n = alen < blen ? alen : blen;
    int r = n ? memcmp(a, b, n) : 0;
    return r ? r : (alen < blen ? -1 : alen > blen ? 1 : 0);
}

/* Encoded unset-marker test for the presence-doctrine types. Callers
   must have verified the field bytes are fully present first. */
static int agg_field_is_unset(const TypedField *f, const uint8_t *p) {
    switch (f->type) {
    case FT_VARCHAR:    return varchar_eff_len(p, f->size) <= 0;
    case FT_DATE:       return ld_be_i32(p) == 0;
    case FT_DATETIME:   return ld_be_i32(p) == 0 && ld_be_u24(p + 4) == 0;
    case FT_DATETIMEMS: return ld_be_i32(p) == 0 && ld_be_u32(p + 4) == 0;
    case FT_TIME:       return p[0] == 0 && p[1] == 0 && p[2] == 0;
    case FT_UUID: {
        int unset = 1;
        for (int i = 0; i < 16; i++)
            if (p[i] != 0) { unset = 0; break; }
        return unset;
    }
    default: return 0;
    }
}



/* Inline raw-key cap for the integer group_by fast path. Sized to fit
   common all-integer multi-field group_bys: 4×long, 8×int, or any mix
   summing to ≤32 bytes. Wider tuples fall back to the string-key path
   (use_int_keys gated at agg setup). */
#define AGG_INT_KEY_CAP 32

typedef struct AggBucket {
    char *group_key;            /* tagged components, \x1F-separated (may hold NULs) */
    int   group_key_len;        /* tagged key length (embedded NULs — never strcmp) */
    uint8_t *group_nulls;       /* per-component NULL tag (Task 8 NULL own-group) */
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
    /* Header padded to 32 bytes so the first carve after (s+1) keeps the
       16-byte alignment malloc handed out: AggNum holds __int128, so
       AggAccum arrays carved here must be 16-byte aligned. */
    void   *_align_pad;
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
    /* 16-byte granularity: exact AggNum states carry __int128 (alignof 16),
       and vectorized struct assignment into arena storage faults on
       under-aligned movaps. */
    bytes = (bytes + 15u) & ~(size_t)15;
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
    if (!s) {
        LOG_ERROR(LOG_SUB_QUERY, "agg_arena_alloc: malloc %zu bytes failed", new_cap);
        return NULL;
    }
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
    if (!h) {
        LOG_ERROR(LOG_SUB_QUERY, "topn_heap_new: calloc TopNHeap failed (cap=%d)", cap);
        return NULL;
    }
    h->entries = calloc((size_t)cap + 1, sizeof(TopNHeapEntry));
    if (!h->entries) {
        LOG_ERROR(LOG_SUB_QUERY, "topn_heap_new: calloc %d entries failed", cap);
        free(h); return NULL;
    }
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
    const char *having,
    const char *format)
{
    if (!specs || nspecs <= 0) return 0;
    if (!group_by_csv || !*group_by_csv) return 0;
    if (!order_by_alias || !*order_by_alias) return 0;
    if (limit <= 0 || limit > 10000) return 0;
    if (having && *having) return 0;
    if (format && strcmp(format, "csv") == 0) return 0;

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


/* §2.4.2 tier clamp: an open-ended upper bound for a VALUE walk (min/max,
 * sum/top-N-over-values) stops before the first [0x01]-prefixed (NULL-tier)
 * key — length 1, exclusive. Never padded: NULL varchar is exactly [0x01],
 * and the length tie-break sorts it below [0x01,0x00]. */
static const char AGG_VALUE_TIER_CEIL[1] = { 0x01 };
/* Output cells own every allocation needed after the first success byte. */
typedef struct {
    char *lex, *escaped;
    int is_null, is_text;
    _Atomic size_t *budget;
    size_t limit, charged;
} AggCell;
static int agg_report_error(int);
static void agg_cells_bind(AggCell *, size_t, _Atomic size_t *, size_t);
static AggCell *agg_cells_alloc(size_t, _Atomic size_t *, size_t);
static void agg_cells_free(AggCell *, int);
static void agg_cells_dispose(AggCell *, size_t);
static int agg_cell_count(AggCell *, int64_t);
static int agg_cell_text(AggCell *, const char *, size_t, int);
static void agg_cell_emit_json(const AggCell *);

/* Group label from an index payload (tier byte already stripped; B+ tree leaf
   payloads and bitmap dictionary values share the decimal_order form for
   packed numeric). Packed numeric decodes with the D2 scale rule; everything
   else keeps the legacy renderer. Returns length >= 0, -1 on error (errno;
   E2BIG when the label would not fit - never truncated). `base` supplies the
   QUERY_BUFFER accounting (NULL = unmetered). */
static int agg_index_label_f(const TypedField *f, const AggNumSpec *ns,
                             int is_num, const AggEnv *base, const uint8_t *p,
                             size_t plen, char *buf, size_t cap) {
    buf[0] = '\0';
    if (is_num && ns->cls == AGC_DEC) {
        AggEnv env;
        agg_env_init(&env, base ? base->bytes : NULL, base ? base->limit : 0);
        int n = agg_dec_label(ns, p, plen, 1, buf, cap, &env);
        int e = errno;
        agg_env_free(&env);
        errno = e == ENAMETOOLONG ? E2BIG : e;
        return n;
    }
    if (f->type == FT_VARCHAR) {
        if (plen >= cap || (plen && memchr(p, 0, plen))) { errno = E2BIG; return -1; }
        if (plen) memcpy(buf, p, plen);
        buf[plen] = '\0';
        return (int)plen;
    }
    int n = decode_idx_to_buf(f, p, plen, buf, cap, 0);
    return n < 0 ? -1 : n;
}

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
    AggNumSpec gb_spec;        /* exact classification of the group field */
    int gb_numeric;            /* exact numeric group-label classification */
    int offer_failed;          /* heap insert dropped a group (malloc) */
    int walk_failed;           /* walk callback aborted (scratch malloc) */
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
    TopNHeap *h = (TopNHeap *)c->heap;
    int was_full = h && h->size >= h->cap;
    int beats = was_full && topn_metric_beats(h, metric, h->entries[1].metric);
    if (!topn_heap_offer(c->heap, metric,
                         c->current_key, c->current_key_len,
                         c->current_count, c->current_sum,
                         c->current_min_set ? c->current_min : 0.0,
                         c->current_max_set ? c->current_max : 0.0) &&
        (!was_full || beats)) {
        /* offer returned 0 without a full-heap decline: the group's key
           allocation failed and the group was dropped. Flag it — the walk
           stops and the whole aggregate reports one error (Task 6). */
        c->offer_failed = 1;
    }
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
                        const uint8_t *hash16,
                        BtOrderedWalkHandle *wh, void *ctx_v) {
    (void)wh;
    TopNWalkCtx *c = (TopNWalkCtx *)ctx_v;

    if (query_deadline_tick(c->dl, &c->dl_counter)) return -1;
    /* BTRI: the ordered walk delivers tiered keys ([0x00] value / [0x01]
       NULL). The NULL tier forms its own group (§2.4.5/Task 8) — the
       single [0x01] key (length 1) is the NULL group's identity; the
       tiered key stays intact through the heap: the varchar emit strips
       the tier for the label (and renders null for the NULL group), and
       the numeric emit's exact label helper re-strips internally
       (the NULL key renders null). */
    if (enc_val_len == 0) return 0;

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
        if (c->offer_failed) return -1;
        new_group = 1;
    }

    if (new_group) {
        c->current_key = malloc(enc_val_len + 1);
        if (!c->current_key) {
            LOG_ERROR(LOG_SUB_QUERY, "topn_walk_cb: malloc %zu bytes failed", enc_val_len + 1);
            c->walk_failed = 1;
            return -1;
        }
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
    if (c->gb_numeric && c->gb_spec.cls == AGC_FLT) {
        double v;
        if (agg_key_double(&c->gb_spec, (const uint8_t *)enc_val, enc_val_len,
                           &v) == 1) {
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

    /* The stream has only one row-count state, not per-field present counts.
       All value/presence-dependent specs use the exact hash executor (D7). */
    for (int i = 0; i < nspecs; i++)
        if (specs[i].fn != AGG_COUNT || specs[i].field[0]) return -2;

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
    ctx.gb_tf = resolve_idx_field(fs->ts, group_by_field);
    ctx.gb_numeric = ctx.gb_tf && ctx.gb_tf->type != FT_BOOL &&
        agg_num_spec_for(fs->ts, (size_t)(ctx.gb_tf - fs->ts->fields), &ctx.gb_spec);

    /* Open upper bound: the ordered walk must reach the NULL tier so a
       NULL group_by forms its own (tagged) group (§2.4.5/Task 8). */
    /* btree_idx_walk_ordered is void; callback aborts surface through
       dl->timed_out (deadline), walk_failed (scratch malloc) and
       offer_failed (dropped group). */
    btree_idx_walk_ordered(db_root, object, group_by_field, sch->splits,
                            NULL, 0, 0,
                            NULL, 0, 0,
                            0 /* asc */,
                            topn_walk_cb, &ctx);

    topn_finalize_run(&ctx);
    free(ctx.current_key);  /* in case callback returned early */
    if (prefilter) keyset_free(prefilter);

    if (dl->timed_out) {
        LOG_WARN(LOG_SUB_QUERY, "top-N group-by aggregate: query deadline exceeded");
        topn_heap_destroy(heap);
        return -1;
    }
    if (ctx.walk_failed || ctx.offer_failed) {
        /* The ordered walk or a group insert failed mid-stream: the heap
           holds a partial top-N. One explicit error, never a partial
           success body (Task 6). */
        LOG_WARN(LOG_SUB_QUERY, "top-N group-by aggregate: walk/heap failure, reporting buffer error");
        topn_heap_destroy(heap);
        agg_report_error(ENOBUFS);
        return -3;
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
        LOG_ERROR(LOG_SUB_QUERY, "top-N drain: calloc failed for n_out=%d", n_out);
        free(metrics); free(gkeys); free(gklens);
        free(counts); free(sums); free(mins); free(maxs);
        topn_heap_destroy(heap);
        return -1;
    }
    int n = topn_heap_drain(heap, metrics, gkeys, gklens, counts, sums, mins, maxs);
    topn_heap_destroy(heap);

    /* heap was destroyed immediately after drain. Only drained arrays and
       rendered cells remain owned here; never destroy heap a second time. */
    size_t live = ((size_t)n_out + 1) *
        (4 * sizeof(double) + sizeof(int64_t) + sizeof(char *) + sizeof(size_t));
    for (int i = 0; i < n; i++) live += gklens[i] + 1;
    _Atomic size_t render_bytes = live;
    AggCell *cellv = agg_cells_alloc((size_t)n * (size_t)nspecs,
                                     &render_bytes, g_query_buffer_max_bytes);
    int rerr = cellv ? 0 : (errno ? errno : ENOMEM);
    AggCell *labels = rerr ? NULL : agg_cells_alloc((size_t)n,
                                     &render_bytes, g_query_buffer_max_bytes);
    if (!rerr && !labels) rerr = errno ? errno : ENOMEM;
    for (int i = 0; i < n && !rerr; i++) {
        const uint8_t *raw = (const uint8_t *)gkeys[i];
        size_t raw_len = gklens[i];
        /* Fixed-width NULL keys may have padding after their tier byte. */
        if (raw_len && raw[0] == 0x01) {
            if (agg_cell_text(&labels[i], NULL, 0, 0)) rerr = errno;
        } else if (!raw_len || raw[0] != 0x00 || !ctx.gb_tf) {
            rerr = EILSEQ;
        } else {
            char label[512];
            int len = agg_index_label_f(ctx.gb_tf, &ctx.gb_spec, ctx.gb_numeric,
                                        NULL, raw + 1, raw_len - 1,
                                        label, sizeof label);
            if (len < 0) rerr = errno ? errno : EILSEQ;
            else if (agg_cell_text(&labels[i], label, (size_t)len, 1)) rerr = errno;
        }
        for (int j = 0; j < nspecs && !rerr; j++)
            if (agg_cell_count(&cellv[(size_t)i * nspecs + j], counts[i]))
                rerr = errno ? errno : ENOMEM;
    }
    if (rerr) {
        agg_cells_dispose(labels, (size_t)n);
        agg_cells_dispose(cellv, (size_t)n * nspecs);
        for (int i = 0; i < n; i++) free(gkeys[i]);
        free(metrics); free(gkeys); free(gklens);
        free(counts); free(sums); free(mins); free(maxs);
        agg_report_error(rerr);
        return -3; /* one complete error response already emitted */
    }
    OUT(want_total ? "{\"rows\":[" : "[");
    for (int i = 0; i < n; i++) {
        if (i) OUT(",");
        OUT("{\"%s\":", group_by_field);
        agg_cell_emit_json(&labels[i]);
        for (int j = 0; j < nspecs; j++) {
            OUT(",\"%s\":", specs[j].alias);
            agg_cell_emit_json(&cellv[(size_t)i * nspecs + j]);
        }
        OUT("}");
        free(gkeys[i]);
    }
    agg_cells_dispose(labels, (size_t)n);
    agg_cells_dispose(cellv, (size_t)n * nspecs);    OUT(want_total ? "],\"total\":null}\n" : "]\n");

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
    int group_idx[MAX_FIELDS];                /* schema index per group field (bitmap NULL tests) */
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
    int predicate_error;
    /* Slab arena for AggBucket + group_key + group_vals + accums. All
       per-bucket allocations live here so teardown is O(slabs) not
       O(buckets) and bucket creation skips ~5 malloc/strdup calls. Per-
       worker AggCtx clones each have their own arena; the merge path
       copies what it needs into the destination's arena and frees the
       source's arena en masse. */
    AggArena arena;
    /* Integer group key optimization: 1 if all group fields are integer types */
    int use_int_keys;
    /* Exact aggregate state (agg_exact): scratch + QUERY_BUFFER accounting,
       the first exact-state/render/key failure (errno), and the numeric
       classification of every aggregate and group field. */
    AggEnv     env;
    int        agg_error;
    AggNumSpec spec_nums[MAX_AGG_SPECS];
    uint8_t    spec_is_num[MAX_AGG_SPECS];
    AggNumSpec group_nums[MAX_FIELDS];
    uint8_t    group_is_num[MAX_FIELDS];   /* numeric-class group field (D5; bool is text) */
} AggCtx;
static void agg_note_predicate_error(void *ctx,int error){((AggCtx*)ctx)->predicate_error=error;}
static void agg_note_error(void *ctx,int error){((AggCtx*)ctx)->agg_error=error;}

/* Ctx-level wrapper: label of group component `gi` from an index payload. */
static int agg_index_label(const AggCtx *ctx, int gi, const uint8_t *p,
                           size_t plen, char *buf, size_t cap) {
    return agg_index_label_f(ctx->group_tfs[gi], &ctx->group_nums[gi],
                             ctx->group_is_num[gi], &ctx->env, p, plen, buf, cap);
}

/* ---- typed-row access for the record-fed routes ---------------------- */

/* One validated view per record (legacy, nullmap or packed). A legacy view
   failure degrades exactly as before — every field reads as missing — but a
   packed schema never degrades silently: the failure is recorded. */
typedef struct { TypedRowView v; int ok; } AggRow;

static int typed_field_int_width(int ft);
static int typed_field_to_raw(const TypedField *f, const uint8_t *p,
                              uint8_t *buf, size_t bufsz);
static int agg_row_label(AggCtx *ctx, const AggRow *r, int gi, char *buf,
                         size_t cap, uint8_t *is_null);
static int agg_span_text(const AggCtx *ctx, const TypedField *f,
                         const TypedRowSpan *s, char *buf, size_t cap,
                         const char **text, size_t *len);

static int agg_row_open(AggCtx *ctx, const uint8_t *raw, size_t len,
                        uint8_t hdr2, AggRow *r) {
    memset(r, 0, sizeof *r);
    if (typed_row_open(ctx->fs->ts, raw, len, hdr2, &r->v) == 0) {
        r->ok = 1;
        return 0;
    }
    if (ctx->fs->ts && ctx->fs->ts->packed.nfields) {
        ctx->agg_error = errno ? errno : EILSEQ;
        return -1;
    }
    return 0;
}

/* 1 when the field span is readable (it may still be NULL / incomplete). */
static int agg_row_span(const AggRow *r, size_t idx, TypedRowSpan *s) {
    memset(s, 0, sizeof *s);
    return r->ok && typed_row_span(&r->v, idx, s) == 0;
}

static int agg_row_unset(const AggCtx *ctx, const TypedField *f,
                         const TypedRowSpan *s) {
    if (f->type == FT_VARCHAR && ctx->fs->ts->packed.nfields) return s->len == 0;
    if (!s->complete) return 1;
    return agg_field_is_unset(f, s->data);
}

/* Group label of component `gi`: length >= 0 (buf NUL-terminated), -1 with
   ctx->agg_error set. *is_null reports a NULL component. Over-long values and
   embedded NULs are an explicit error (E2BIG, decision D6), never a truncated
   key. Packed numeric uses the D2 scale rule; every other field keeps the
   historical typed_field_to_buf_raw rendering. */
static int agg_row_label(AggCtx *ctx, const AggRow *r, int gi, char *buf,
                         size_t cap, uint8_t *is_null) {
    buf[0] = '\0';
    *is_null = 0;
    const TypedField *f = ctx->group_tfs[gi];
    TypedRowSpan s;
    if (!agg_row_span(r, (size_t)ctx->group_idx[gi], &s)) return 0;
    if (s.is_null) { *is_null = 1; return 0; }
    if (f->type == FT_VARCHAR) {
        const uint8_t *c; size_t n;
        if (ctx->fs->ts->packed.nfields) { c = s.data; n = s.len; }
        else {
            if (!s.complete) return 0;
            int len = varchar_eff_len(s.data, f->size);
            c = s.data + 2;
            n = len > 0 ? (size_t)len : 0;
        }
        if (n >= cap || (n && memchr(c, 0, n))) { ctx->agg_error = E2BIG; return -1; }
        if (n) memcpy(buf, c, n);
        buf[n] = '\0';
        return (int)n;
    }
    if (!s.complete) return 0;
    if (ctx->group_is_num[gi] && ctx->group_nums[gi].cls == AGC_DEC) {
        int n = agg_dec_label(&ctx->group_nums[gi], s.data, s.len, 0, buf, cap,
                              &ctx->env);
        if (n < 0) { ctx->agg_error = errno == ENAMETOOLONG ? E2BIG : errno; return -1; }
        return n;
    }
    typed_field_to_buf_raw(f, s.data, buf, cap, 1);
    return (int)strlen(buf);
}

/* Raw fixed-width key for the all-integer group fast path (same bytes as
   before: tag byte + natural width, NULL = zeroed width). */
static int agg_row_raw_key(const AggCtx *ctx, const AggRow *r,
                           const uint8_t *gnulls, uint8_t *raw_key) {
    int kp = 0;
    for (int i = 0; i < ctx->ngroups && kp < AGG_INT_KEY_CAP; i++) {
        const TypedField *gtf = ctx->group_tfs[i];
        if (!gtf) continue;
        if (kp + 1 >= AGG_INT_KEY_CAP) break;
        raw_key[kp++] = gnulls[i] ? 1 : 0;
        int w = typed_field_int_width(gtf->type);
        if (gnulls[i]) {
            if (w <= 0 || kp + w > AGG_INT_KEY_CAP) break;
            memset(raw_key + kp, 0, (size_t)w);
            kp += w;
            continue;
        }
        TypedRowSpan s;
        if (!agg_row_span(r, (size_t)ctx->group_idx[i], &s) || !s.complete) break;
        int len = typed_field_to_raw(gtf, s.data, raw_key + kp,
                                     (size_t)(AGG_INT_KEY_CAP - kp));
        if (len > 0) kp += len;
    }
    return kp;
}

/* Write a typed field's string form into a caller-provided buffer (no malloc).
   Returns the length written. Returns 0 for "empty" fields (0-value LONG/INT/SHORT/
   DOUBLE/DATE/DATETIME/zero-length VARCHAR) to preserve legacy skip-empty behavior.
   FT_NUMERIC and FT_BOOL always return non-zero. */
int typed_field_to_buf_raw(const TypedField *f, const uint8_t *p,
                                  char *buf, size_t bufsz, int keep_zero) {
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
        if (!keep_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%lld", (long long)v);
    }
    case FT_INT: {
        int32_t v = ld_be_i32(p);
        if (!keep_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%d", v);
    }
    case FT_SHORT: {
        int16_t v = ld_be_i16(p);
        if (!keep_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%d", v);
    }
    case FT_DOUBLE: {
        double v; memcpy(&v, p, 8);
        if (!keep_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%.17g", v);
    }
    case FT_FLOAT: {
        float v; memcpy(&v, p, 4);
        if (!keep_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%.9g", (double)v);
    }
    case FT_BOOL:
        return snprintf(buf, bufsz, "%s", p[0] ? "true" : "false");
    case FT_BYTE:
        return snprintf(buf, bufsz, "%u", p[0]);
    case FT_TIMESTAMP: {
        int64_t v = ld_be_i64(p);
        if (!keep_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%lld", (long long)v);
    }
    case FT_NUMERIC: {
        int64_t v = ld_be_i64(p);
        if (v == 0) { buf[0] = '0'; buf[1] = '\0'; return 1; }
        if (f->numeric_scale_mult <= 0)   /* unsupported legacy scale */
            return snprintf(buf, bufsz, "%lld", (long long)v);
        int64_t scale = f->numeric_scale_mult;
        uint64_t m = (uint64_t)v;
        int neg = v < 0;
        if (neg) m = 0u - m;   /* magnitude without negating INT64_MIN */
        uint64_t whole = m / (uint64_t)scale;
        uint64_t frac = m % (uint64_t)scale;
        if (frac == 0)
            return snprintf(buf, bufsz, "%s%llu", neg ? "-" : "",
                            (unsigned long long)whole);
        return snprintf(buf, bufsz, "%s%llu.%0*llu", neg ? "-" : "",
                        (unsigned long long)whole, f->numeric_scale,
                        (unsigned long long)frac);
    }
    case FT_DATE: {
        int32_t v = ld_be_i32(p);
        if (v == 0) return 0;
        return snprintf(buf, bufsz, "%08d", v);
    }
    case FT_DATETIME: {
        int32_t d = ld_be_i32(p);
        uint32_t t = ld_be_u24(p + 4);
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
        for (int bi = 0; bi < 16; bi++) if (p[bi] != 0) { allzero = 0; break;
                    }
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

/* Decode an indexed varchar/calendar key into the canonical textual value. */
static AggValueStatus decode_index_key_to_text(const TypedField *f,
                                               const uint8_t *p, size_t plen,
                                               char *out, size_t *out_len) {
    if (!f || !p || !out || !out_len) return AGG_VALUE_MISSING;
    /* BTRI tier dispatch (§2.4.5): strip 0x00; NULL tier skipped here.
       An empty varchar payload is MISSING for text min/max — the per-type
       unset-marker doctrine is generation- and route-independent (matches
       typed_field_to_text and count(field)); grouping keeps PRESENT-empty
       via the group-key decode instead. */
    if (plen == 0 || p[0] != 0x00) return AGG_VALUE_MISSING;
    p++; plen--;
    switch (f->type) {
    case FT_VARCHAR:
        if (plen == 0) return AGG_VALUE_MISSING;
        memcpy(out, p, plen);
        out[plen] = '\0';
        *out_len = plen;
        return AGG_VALUE_PRESENT;
    case FT_DATE: {
        if (plen < 4) return AGG_VALUE_MISSING;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8) | p[3];
        int32_t d = (int32_t)(u ^ 0x80000000u);
        if (d == 0) return AGG_VALUE_MISSING;
        *out_len = (size_t)snprintf(out, 9, "%08d", d);
        return AGG_VALUE_PRESENT;
    }
    case FT_DATETIME: {
        if (plen < 6) return AGG_VALUE_MISSING;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8) | p[3];
        int32_t d = (int32_t)(u ^ 0x80000000u);
        uint32_t t = ld_be_u24(p + 4);
        if (d == 0 && t == 0) return AGG_VALUE_MISSING;
        *out_len = (size_t)snprintf(out, 15, "%08d%02d%02d%02d", d,
                                    t / 3600, (t % 3600) / 60, t % 60);
        return AGG_VALUE_PRESENT;
    }
    case FT_DATETIMEMS: {
        if (plen < 8) return AGG_VALUE_MISSING;
        uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                     ((uint32_t)p[2] << 8) | p[3];
        int32_t d = (int32_t)(u ^ 0x80000000u);
        uint32_t ms = ((uint32_t)p[4] << 24) | ((uint32_t)p[5] << 16) |
                      ((uint32_t)p[6] << 8) | p[7];
        if (d == 0 && ms == 0) return AGG_VALUE_MISSING;
        *out_len = (size_t)snprintf(out, 18, "%08d%02d%02d%02d%03d", d,
                                    ms / 3600000, (ms % 3600000) / 60000,
                                    (ms % 60000) / 1000, ms % 1000);
        return AGG_VALUE_PRESENT;
    }
    default: return AGG_VALUE_UNSUPPORTED;
    }
}

/* Mirror of typed_field_to_buf_raw, but reads from a btree leaf entry's
   encoded bytes (post encode_field_text_for_index). Output is byte-identical
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
        return snprintf(buf, bufsz, "%.17g", v);
    }
    case FT_FLOAT: {
        if (plen < 4) return 0;
        uint32_t bits = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                        ((uint32_t)p[2] << 8) | (uint32_t)p[3];
        if (bits & 0x80000000u) bits ^= 0x80000000u;
        else bits = ~bits;
        float v; memcpy(&v, &bits, 4);
        if (skip_zero && v == 0.0f) return 0;
        return snprintf(buf, bufsz, "%.9g", (double)v);
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
        uint32_t t = ld_be_u24(p + 4);
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
    case FT_TIMESTAMP: {
        if (plen < 8) return 0;
        uint64_t u = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                     ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                     ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                     ((uint64_t)p[6] << 8)  |  (uint64_t)p[7];
        int64_t v = (int64_t)(u ^ (1ULL << 63));
        if (skip_zero && v == 0) return 0;
        return snprintf(buf, bufsz, "%lld", (long long)v);
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
        for (int bi = 0; bi < 16; bi++) if (p[bi] != 0) { allzero = 0; break;
                    }
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
        if (scale <= 0)   /* unsupported legacy scale: raw integer fallback */
            return snprintf(buf, bufsz, "%lld", (long long)v);
        uint64_t m = (uint64_t)v;
        int neg = v < 0;
        if (neg) m = 0u - m;
        uint64_t whole = m / (uint64_t)scale;
        uint64_t frac = m % (uint64_t)scale;
        if (frac == 0)
            return snprintf(buf, bufsz, "%s%llu", neg ? "-" : "",
                            (unsigned long long)whole);
        return snprintf(buf, bufsz, "%s%llu.%0*llu", neg ? "-" : "",
                        (unsigned long long)whole, f->numeric_scale,
                        (unsigned long long)frac);
    }
    case FT_VARCHAR: {
        /* Index stores raw varchar bytes. BTRI: an empty varchar stores
           the tier byte alone, so group-by callers reach this case with
           plen == 0 — PRESENT-empty (§2.4.5). Return 0 WITHOUT touching
           buf; group-by callers initialize the label themselves and
           treat 0 as the empty string, while min/max readers treat 0 as
           skip. Callers that need "" pre-initialized do so. */
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
        LOG_ERROR(LOG_SUB_QUERY, "hbk_init: calloc cap=%zu failed", cap);
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
        LOG_ERROR(LOG_SUB_QUERY, "hsm_init: alloc failed (cap=%zu, arena_hint=%zu)", cap, arena_hint);
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

/* Parse a tagged group component ([u8 is_null][u32 BE len][text]) as
   stored in the secondary-group maps. Returns the text span and its
   NULL flag. Malformed entries parse as empty non-NULL (the map is
   only written by sec_map_build_worker, so this is defensive only). */
static void hsm_parse_tagged(const char *val, size_t vlen,
                             int *out_null, const char **out_text,
                             size_t *out_text_len) {
    *out_null = 0;
    *out_text = "";
    *out_text_len = 0;
    if (vlen < 5) return;
    uint32_t bl = ((uint32_t)(uint8_t)val[1] << 24) |
                  ((uint32_t)(uint8_t)val[2] << 16) |
                  ((uint32_t)(uint8_t)val[3] << 8) |
                  (uint32_t)(uint8_t)val[4];
    if (5 + (size_t)bl > vlen) return;
    *out_null = val[0] ? 1 : 0;
    *out_text = val + 5;
    *out_text_len = bl;
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
        if (!na) {
            LOG_ERROR(LOG_SUB_QUERY, "hsm_insert: realloc arena to %zu failed", new_cap);
            return -1;
        }
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


typedef struct {
    CriteriaNode     *tree;
    FieldSchema      *fs;
    const TypedField *agg_tf;
    int               agg_tf_idx;  /* schema index of agg_tf — bitmap NULL test */
    int               desc;     /* 0 = MIN (ASC walk), 1 = MAX (DESC) */
    const AggNumSpec *spec;
    AggNum           *num;
    AggEnv           *env;
    int              *found;
    pthread_mutex_t   mu;
    int predicate_error;
    int err;
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
    const AggNumSpec *spec;
    AggNum  num;
    AggEnv  env;
    int     found;
    int     budget_exceeded;
    int     dl_counter;
    int predicate_error;
} WfcArg;

static int wfc_batch_cb(const uint8_t hash16[16],
                         const void *key, size_t klen,
                         const void *value, size_t vlen,
                         uint8_t hdr2, void *ctx) {
    (void)hash16; (void)key; (void)klen;
    WfcBatchCtx *bc = (WfcBatchCtx *)ctx;
    int matched=0;
    if(criteria_match_tree_checked(value,vlen,bc->tree,bc->fs,hdr2,&matched)){
        pthread_mutex_lock(&bc->mu);bc->predicate_error=errno?errno:EILSEQ;pthread_mutex_unlock(&bc->mu);return 1;}
    if(!matched)return 0;
    TypedRowView view;
    if (typed_row_open(bc->fs->ts, (const uint8_t *)value, vlen, hdr2, &view)) {
        pthread_mutex_lock(&bc->mu);
        bc->predicate_error = errno ? errno : EILSEQ;
        pthread_mutex_unlock(&bc->mu);
        return 1;
    }
    pthread_mutex_lock(&bc->mu);
    int r = agg_num_offer_row(bc->spec, &view, bc->num, bc->env);
    if (r < 0) bc->err = errno ? errno : EILSEQ;
    else if (r == 1) *bc->found = 1;
    pthread_mutex_unlock(&bc->mu);
    return r < 0;
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
            wfc_batch_cb(batch[i], NULL, 0, rr.val, rr.vlen, rr.hdr2, bc);
            release_record_ref(&rr);
        }
    }
    if(bc->predicate_error)w->predicate_error=bc->predicate_error;
}

static void *wfc_worker(void *arg) {
    WfcArg *w = (WfcArg *)arg;

    SlotcaskSchemaInfo info = {
        .splits = w->sch->splits, .slot_size = w->sch->slot_size,
        .streams = w->sch->streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(w->db_root, w->object, &info);

    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path),
                   w->db_root, w->object, w->agg_field, w->shard_id);

    /* Lock-safe resumable walker (see docs/plans/2026-08-27-bt-kf-lock-
       inversion-chunked-fetch.md): the OLD version flushed its kf batch
       every WFC_BATCH entries while the BtRangeIter was still open —
       blocking kfcache acquires under a held bt rdlock, i.e. the writer
       window's kf-WR -> bt-WR inverse. Now every full batch closes the
       iterator FIRST, flushes outside any bt lock, then reopens past the
       last delivered (value,hash) using the inclusive-bound + raw-hash16
       tiebreak skip convention shared with cursor pagination. */
    char last_val[BT_MAX_VAL_LEN];
    size_t last_len = 0;
    uint8_t last_hash[BT_HASH_SIZE];
    int have_last = 0;

    const char *val; size_t vlen; const uint8_t *hash16;
    int walks = 0;
#define WFC_BATCH 64
    uint8_t batch[WFC_BATCH][16];
    int bn = 0;
    agg_env_init(&w->env, NULL, 0);
    memset(&w->num, 0, sizeof w->num);
    WfcBatchCtx bc = {
        .tree   = w->tree,
        .fs     = w->fs,
        .agg_tf = w->agg_tf,
        .agg_tf_idx = (int)(w->agg_tf - w->fs->ts->fields),
        .desc   = w->desc,
        .spec   = w->spec,
        .num    = &w->num,
        .env    = &w->env,
        .found  = &w->found,
        .mu     = PTHREAD_MUTEX_INITIALIZER,
    };

    int exhausted = 0;
    int dl_stop = 0;
    for (;;) {
        char lo_buf[BT_MAX_VAL_LEN];
        const char *lo = "";  size_t lo_len = 0;
        const char *hi = AGG_VALUE_TIER_CEIL; size_t hi_len = 1;
        int hi_excl = 1;
        if (have_last) {
            memcpy(lo_buf, last_val, last_len);
            lo = lo_buf; lo_len = last_len;   /* inclusive at resume */
            if (w->desc) {
                /* DESC walks tighten the UPPER bound instead. */
                hi = lo_buf; hi_len = last_len; hi_excl = 0;
                lo = ""; lo_len = 0;
            }
        }
        BtRangeIter *it = btree_range_iter_open(
            idx_path, lo, lo_len, 0, hi, hi_len, hi_excl, w->desc);
        if (!it) {
            LOG_ERROR(LOG_SUB_QUERY,
                      "wfc_worker: btree_range_iter_open %s failed",
                      idx_path);
            return NULL;
        }

        while (!exhausted && bn < WFC_BATCH) {
            if (!btree_range_iter_next(it, &val, &vlen, &hash16)) {
                exhausted = 1;
                break;
            }
            if (query_deadline_tick(w->deadline, &w->dl_counter)) {
                dl_stop = 1;
                break;
            }
            if (++walks > w->budget) { w->budget_exceeded = 1; break;
                    }

            /* Resume floor: skip entries already delivered pre-close. */
            if (have_last && vlen == last_len &&
                memcmp(val, last_val, last_len) == 0) {
                int hc = memcmp(hash16, last_hash, BT_HASH_SIZE);
                if ((w->desc && hc >= 0) || (!w->desc && hc <= 0))
                    continue;
            }

            /* Presence of a non-NULL tier entry; the value comes from the
               fetched record in wfc_batch_cb. */
            if (vlen == 0 || (uint8_t)val[0] != 0x00) continue;

            last_len = vlen > BT_MAX_VAL_LEN ? BT_MAX_VAL_LEN : vlen;
            memcpy(last_val, val, last_len);
            memcpy(last_hash, hash16, BT_HASH_SIZE);
            have_last = 1;

            memcpy(batch[bn], hash16, 16);
            bn++;
        }
        btree_range_iter_close(it);

        if (bn > 0) {
            /* No bt lock is held here — blocking kf fetch is safe. */
            flush_wfc_batch(sdb, w, batch, bn, &bc);
            bn = 0;
        }
        if (bc.err) w->predicate_error = bc.err;
        if (w->predicate_error || w->found || w->budget_exceeded || exhausted || dl_stop) break;
    }

    return NULL;
}

/* Per-shard worker for the SUM/AVG/MIN/MAX-with-criteria fast path. Each
   worker walks one idx shard's btree, filters entries by the candidate
   KeySet, decodes the value, and accumulates into per-spec local arrays.
   The orchestrator merges across shards after parallel_for returns. */


/* Text projection for text min/max. The per-type unset-marker doctrine is
   generation-independent (matching count(field)): an empty varchar, an
   all-zero calendar, and midnight are NOT present textual values, so they
   never become min/max offers — on legacy and nullmap records alike. The
   caller's bitmap NULL check runs before this function and skips NULLs. */
static AggValueStatus typed_field_to_text(const TypedField *f, const uint8_t *p,
                                          size_t available, char *buf,
                                          size_t bufsz, size_t *out_len,
                                          uint8_t hdr2) {
    (void)hdr2;
    if (!f || !p || !buf || bufsz == 0 || available < (size_t)f->size)
        return AGG_VALUE_MISSING;
    int n = 0;
    switch (f->type) {
    case FT_VARCHAR: {
        int len = varchar_eff_len(p, f->size);
        if (len <= 0) return AGG_VALUE_MISSING;
        if ((size_t)len >= bufsz) len = (int)bufsz - 1;
        memcpy(buf, p + 2, (size_t)len);
        n = len;
        break;
    }
    case FT_DATE: {
        int32_t d = ld_be_i32(p);
        if (d == 0) return AGG_VALUE_MISSING;
        n = snprintf(buf, bufsz, "%08d", d);
        break;
    }
    case FT_DATETIME: {
        int32_t d = ld_be_i32(p); uint32_t t = ld_be_u24(p + 4);
        if (d == 0 && t == 0) return AGG_VALUE_MISSING;
        n = snprintf(buf, bufsz, "%08d%02d%02d%02d", d,
                     t / 3600, (t % 3600) / 60, t % 60);
        break;
    }
    case FT_DATETIMEMS: {
        int32_t d = ld_be_i32(p); uint32_t ms = ld_be_u32(p + 4);
        if (d == 0 && ms == 0) return AGG_VALUE_MISSING;
        n = snprintf(buf, bufsz, "%08d%02u%02u%02u%03u", d,
                     ms / 3600000, (ms % 3600000) / 60000,
                     (ms % 60000) / 1000, ms % 1000);
        break;
    }
    default:
        return AGG_VALUE_UNSUPPORTED;
    }
    if (n < 0) return AGG_VALUE_MISSING;
    if ((size_t)n >= bufsz) n = (int)bufsz - 1;
    buf[n] = '\0';
    if (out_len) *out_len = (size_t)n;
    return AGG_VALUE_PRESENT;
}
/* Borrow full varchar bytes until agg_text_offer copies them into its arena.
   Only bounded calendar representations use the supplied scratch buffer. */
static int agg_span_text(const AggCtx *ctx, const TypedField *f,
                         const TypedRowSpan *s, char *buf, size_t cap,
                         const char **text, size_t *len) {
    if (f->type == FT_VARCHAR) {
        if (ctx->fs->ts->packed.nfields) {
            *text = (const char *)s->data;
            *len = s->len;
        } else {
            if (!s->complete) return 0;
            int n = varchar_eff_len(s->data, f->size);
            *text = (const char *)s->data + 2;
            *len = n > 0 ? (size_t)n : 0;
        }
        return *len > 0;
    }
    if (!s->complete) return 0;
    *text = buf;
    return typed_field_to_text(f, s->data, s->len, buf, cap, len, 0) ==
           AGG_VALUE_PRESENT;
}

static void agg_text_offer(AggCtx *ctx, AggAccum *a, enum AggFn fn,
                           const char *text, size_t len) {
    char **dst = fn == AGG_MIN ? &a->text_min : &a->text_max;
    size_t *dst_len = fn == AGG_MIN ? &a->text_min_len : &a->text_max_len;
    int replace = !a->text_present ||
        (fn == AGG_MIN
            ? agg_text_cmp(text, len, *dst, *dst_len) < 0
            : agg_text_cmp(text, len, *dst, *dst_len) > 0);
    if (replace) {
        char *copy = agg_arena_strdup(&ctx->arena, text, len);
        if (!copy) { ctx->budget_exceeded = 1; return; }
        *dst = copy;
        *dst_len = len;
    }
    a->text_present = 1;
}
/* One error response for every route (no success-shaped partial output). */
static int agg_report_error(int err) {
    switch (err) {
    case ENOBUFS:  OUT(QUERY_BUFFER_ERR); break;
    case ENOMEM:   OUT("{\"error\":\"out of memory\"}\n"); break;
    case ERANGE:   OUT("{\"error\":\"aggregate result out of range\"}\n"); break;
    case E2BIG:    OUT("{\"error\":\"group_by value exceeds aggregate key limit (511 bytes per value, 4096 bytes per composite key) or contains NUL\"}\n"); break;
    case ETIMEDOUT: OUT("{\"error\":\"query_timeout\"}\n"); break;
    case EIO:      OUT("{\"error\":\"aggregate storage access failed\"}\n"); break;
    default:       OUT("{\"error\":\"invalid typed record\"}\n"); break;
    }
    errno = err ? err : EILSEQ;
    return -1;
}

typedef struct {
    const char *db_root, *object, *field;
    int shard_id;
    const AggNumSpec *spec;
    const KeySet *ks;               /* optional membership filter */
    int first_only, desc;           /* min/max: stop at first PRESENT entry per shard */
    const char *lo; size_t lo_len; int lo_excl;
    const char *hi; size_t hi_len; int hi_excl;
    QueryDeadline *dl; int dl_counter;
    _Atomic size_t *budget; size_t limit;
    AggNum num; int64_t count; int err; int timed_out;   /* outputs */
} AggWalkArg;

static void *agg_walk_worker(void *raw) {
    AggWalkArg *a = (AggWalkArg *)raw;
    AggEnv env;
    agg_env_init(&env, a->budget, a->limit);
    char path[PATH_MAX];
    build_idx_path(path, sizeof(path), a->db_root, a->object, a->field, a->shard_id);
    /* Untouched index shards are created lazily; ENOENT is an empty shard.
       An existing shard whose iterator cannot open still fails closed. */
    struct stat st;
    if (stat(path, &st) < 0) {
        if (errno != ENOENT) a->err = EIO;
        agg_env_free(&env);
        return NULL;
    }
    BtRangeIter *it = btree_range_iter_open(
        path, a->lo ? a->lo : "", a->lo ? a->lo_len : 0, a->lo ? a->lo_excl : 0,
        a->hi ? a->hi : AGG_VALUE_TIER_CEIL, a->hi ? a->hi_len : 1,
        a->hi ? a->hi_excl : 1, a->desc);
    if (!it) {
        /* The shard exists: an open failure must not become an empty total. */
        LOG_ERROR(LOG_SUB_QUERY, "agg_walk_worker: btree_range_iter_open %s failed", path);
        a->err = EIO;
        agg_env_free(&env);
        return NULL;
    }
    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        if (query_deadline_tick(a->dl, &a->dl_counter)) { a->timed_out = 1; break; }
        if (a->ks && !keyset_contains(a->ks, hash16)) continue;
        int r = agg_num_offer_key(a->spec, (const uint8_t *)val, vlen, &a->num, &env);
        if (r < 0) { a->err = errno ? errno : EILSEQ; break; }
        if (r == 0) continue;
        a->count++;
        if (a->first_only) break;
    }
    btree_range_iter_close(it);
    agg_env_free(&env);
    return NULL;
}

/* Walk every idx shard of `proto->field` in parallel and merge into *out /
   *count. 0 ok, -1 error (errno), 1 deadline. */
static int agg_walk_field(const AggWalkArg *proto, int n_idx, AggNum *out,
                          int64_t *count) {
    AggWalkArg *args = calloc((size_t)n_idx, sizeof *args);
    if (!args) { errno = ENOMEM; return -1; }
    for (int s = 0; s < n_idx; s++) { args[s] = *proto; args[s].shard_id = s; }
    parallel_for_io(agg_walk_worker, args, n_idx, sizeof *args);
    AggEnv env;
    agg_env_init(&env, proto->budget, proto->limit);
    int rc = 0, err = 0, timed = 0;
    for (int s = 0; s < n_idx; s++) {
        if (args[s].timed_out) timed = 1;
        if (args[s].err && !err) err = args[s].err;
    }
    for (int s = 0; s < n_idx; s++) {
        if (!err && agg_num_merge(out, &args[s].num, proto->spec->cls, &env))
            err = errno ? errno : EILSEQ;
        *count += args[s].count;
        agg_num_release(&args[s].num, proto->spec->cls, &env);
    }
    agg_env_free(&env);
    free(args);
    if (err) { errno = err; rc = -1; }
    else if (timed) rc = 1;
    return rc;
}

/* Output accounting covers arrays, full decimal lexemes and escaped text.
   Integer/float cells reserve a conservative bounded allocation; decimals
   reserve dec_text_len exactly before allocating the potentially huge text. */
#ifdef TEST_BUILD
static _Thread_local int agg_fail_cells_alloc, agg_fail_escape_alloc;
void agg_test_fail_cells_alloc(int enabled) { agg_fail_cells_alloc = enabled; }
void agg_test_fail_escape_alloc(int n) { agg_fail_escape_alloc = n; }
#endif
static int agg_render_charge(_Atomic size_t *bytes, size_t limit, size_t n) {
    if (!bytes || !n) return 0;
    size_t old = atomic_load_explicit(bytes, memory_order_relaxed);
    do {
        if (n > limit || old > limit - n) { errno = ENOBUFS; return -1; }
    } while (!atomic_compare_exchange_weak_explicit(bytes, &old, old + n,
                 memory_order_relaxed, memory_order_relaxed));
    return 0;
}
static void agg_render_uncharge(_Atomic size_t *bytes, size_t n) {
    if (bytes && n) atomic_fetch_sub_explicit(bytes, n, memory_order_relaxed);
}
static void agg_cells_bind(AggCell *c, size_t n, _Atomic size_t *bytes, size_t limit) {
    for (size_t i = 0; i < n; i++) { c[i].budget = bytes; c[i].limit = limit; }
}
static AggCell *agg_cells_alloc(size_t n, _Atomic size_t *bytes, size_t limit) {
    size_t count = n ? n : 1;
    if (count > SIZE_MAX / sizeof(AggCell)) { errno = ENOBUFS; return NULL; }
    size_t need = count * sizeof(AggCell);
    if (agg_render_charge(bytes, limit, need)) return NULL;
#ifdef TEST_BUILD
    if (agg_fail_cells_alloc) {
        agg_render_uncharge(bytes, need); errno = ENOMEM; return NULL;
    }
#endif
    AggCell *c = calloc(count, sizeof *c);
    if (!c) { agg_render_uncharge(bytes, need); errno = ENOMEM; return NULL; }
    agg_cells_bind(c, count, bytes, limit);
    return c;
}
static int agg_cell_reserve(AggCell *c, size_t n) {
    if (agg_render_charge(c->budget, c->limit, n)) return -1;
    c->charged += n;
    return 0;
}
static void agg_cells_free(AggCell *c, int n) {
    if (!c) return;
    for (int i = 0; i < n; i++) {
        free(c[i].lex); free(c[i].escaped);
        agg_render_uncharge(c[i].budget, c[i].charged);
        c[i].lex = c[i].escaped = NULL; c[i].charged = 0;
    }
}
static void agg_cells_dispose(AggCell *c, size_t n) {
    if (!c) return;
    _Atomic size_t *bytes = c[0].budget;
    agg_cells_free(c, (int)n);
    agg_render_uncharge(bytes, (n ? n : 1) * sizeof(AggCell));
    free(c);
}
static int agg_cell_count(AggCell *c, int64_t n) {
    char tmp[32];
    int len = snprintf(tmp, sizeof tmp, "%lld", (long long)n);
    if (agg_cell_reserve(c, (size_t)len + 1)) return -1;
    c->lex = strdup(tmp);
    if (!c->lex) { errno = ENOMEM; return -1; }
    return 0;
}
static int agg_cell_text(AggCell *c, const char *text, size_t len, int present) {
    c->is_text = 1; c->is_null = !present;
    if (!present) return 0;
    if (agg_cell_reserve(c, len + 1)) return -1;
    c->lex = malloc(len + 1);
    if (!c->lex) { errno = ENOMEM; return -1; }
    memcpy(c->lex, text, len); c->lex[len] = 0;
    size_t need = 1;
    for (size_t i = 0; i < len; i++) {
        unsigned char ch = (unsigned char)text[i];
        size_t add = (ch == '"' || ch == '\\' || ch == '\b' || ch == '\f' ||
                      ch == '\n' || ch == '\r' || ch == '\t') ? 2 : ch < 0x20 ? 6 : 1;
        if (need > SIZE_MAX - add) { errno = ENOBUFS; return -1; }
        need += add;
    }
    if (agg_cell_reserve(c, need)) return -1;
#ifdef TEST_BUILD
    if (agg_fail_escape_alloc > 0 && --agg_fail_escape_alloc == 0) {
        errno = ENOMEM; return -1;
    }
#endif
    c->escaped = malloc(need);
    if (!c->escaped) { errno = ENOMEM; return -1; }
    int n = json_escape_into(c->escaped, need - 1, text, len);
    if (n < 0) { errno = EILSEQ; return -1; }
    c->escaped[n] = 0;
    return 0;
}
static int agg_cell_from_num(const AggNumSpec *sp, const AggNum *num,
                             enum AggFn fn, int64_t count, AggCell *c) {
    if (count <= 0) {
        /* avg/min/max of no values have no answer: JSON null (CSV keeps its
           historical "0" cell). sum of no values is exactly zero at the
           column's declared/display scale (Task 8). */
        if (fn != AGG_SUM) { c->is_null = 1; return 0; }
        AggValue z;
        agg_value_init(&z);
        if (sp->cls == AGC_DEC) {
            dec_init(&z.own);
            z.own.dscale = sp->dscale;
            z.cls = AGC_DEC;
            z.dv = &z.own;
        } else {
            z.cls = sp->cls;
            z.i = 0;
            z.scale = sp->scale;
        }
        z.present = 1;
        size_t need = z.cls == AGC_DEC ? dec_text_len(z.dv) : 128 + (size_t)(z.scale > 0 ? z.scale : 0);
        if (agg_cell_reserve(c, need)) return -1;
        c->lex = agg_value_render(&z);
        int ze = errno;
        agg_value_release(&z);
        if (!c->lex) { errno = ze ? ze : ENOMEM; return -1; }
        return 0;
    }
    AggValue v;
    if (agg_num_value(num, sp, fn, count, &v)) return -1;
    size_t need = v.cls == AGC_DEC ? dec_text_len(v.dv)
                                 : 128 + (v.scale > 0 ? (size_t)v.scale : 0);
    if (agg_cell_reserve(c, need)) { agg_value_release(&v); return -1; }
    c->lex = agg_value_render(&v);
    int e = errno;
    agg_value_release(&v);
    if (!c->lex) { errno = e ? e : ENOMEM; return -1; }
    return 0;
}
static void agg_cell_emit_json(const AggCell *c) {
    if (c->is_null) OUT("null");
    else if (c->is_text) OUT("\"%s\"", c->escaped);
    else OUT("%s", c->lex ? c->lex : "0");
}
static void agg_cell_emit_csv(const AggCell *c, char delim) {
    csv_emit_cell(c->is_null ? (c->is_text ? "" : "0") : (c->lex ? c->lex : ""), delim);
}
static void agg_emit_delim(char d) { char s[2] = { d, '\0' }; OUT("%s", s); }
/* Scalar (no group_by) response: JSON object, rows+total wrapper, or CSV
   header+row. All values are already rendered, so nothing can fail here. */
static void agg_emit_scalar_cells(const AggSpec *specs, int nspecs,
                                  const AggCell *cells, char csv_delim,
                                  int want_total) {
    if (csv_delim) {
        for (int i = 0; i < nspecs; i++) {
            if (i > 0) agg_emit_delim(csv_delim);
            csv_emit_cell(specs[i].alias, csv_delim);
        }
        OUT("\n");
        for (int i = 0; i < nspecs; i++) {
            if (i > 0) agg_emit_delim(csv_delim);
            agg_cell_emit_csv(&cells[i], csv_delim);
        }
        OUT("\n");
        return;
    }
    OUT(want_total ? "{\"rows\":{" : "{");
    for (int i = 0; i < nspecs; i++) {
        if (i > 0) OUT(",");
        OUT("\"%s\":", specs[i].alias);
        agg_cell_emit_json(&cells[i]);
    }
    OUT(want_total ? "},\"total\":1}\n" : "}\n");
}

/* djb2 over an explicit length — the tagged group key holds embedded
   NULs (BE lengths), so strlen-based hashing would truncate. */
static uint32_t agg_hash_len(const char *s, size_t len) {
    uint32_t h = 5381;
    for (size_t i = 0; i < len; i++) h = h * 33 + (uint8_t)s[i];
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

/* Table slot hash of an existing bucket. It must be the exact function
   agg_find_or_create probes with: raw-int buckets hash their raw bytes;
   everything else hashes the tagged key over its recorded length (the
   leading 0x00 NULL tag makes a strlen-based hash see an empty string). */
static uint32_t agg_bucket_hash(const AggCtx *ctx, const AggBucket *b) {
    if (ctx->use_int_keys && b->raw_key_len > 0)
        return agg_hash_int(b->raw_key, (size_t)b->raw_key_len);
    return agg_hash_len(b->group_key, (size_t)b->group_key_len);
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
    const TypeDescriptor *d = type_desc(ft);
    return d ? d->int_width : 0;
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
            uint32_t h = agg_bucket_hash(ctx, b) & (uint32_t)new_mask;
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
                                      const uint8_t *raw_key, int raw_key_len,
                                      const uint8_t *nulls) {
    /* Build composite key — §2.4.5/Task 8 tagged components:
       [u8 is_null][u32 BE text_length][text], NULL length zero. NULL, "",
       and the literal "null" are three distinct keys. The tag/length bytes
       embed NULs, so hashing and comparison are length-based, never
       strcmp. */
    char key[4096];
    int kp = 0;
    for (int i = 0; i < nvals; i++) {
        if (i > 0) key[kp++] = '\x1F';
        int is_null = nulls ? (nulls[i] ? 1 : 0) : 0;
        uint32_t bl = is_null ? 0 : (uint32_t)strlen(vals[i]);
        if (kp + 5 + (int)bl >= (int)sizeof(key) - 1) {
            ctx->agg_error = E2BIG;   /* never truncate a group key (D6) */
            return NULL;
        }
        key[kp++] = (char)(is_null ? 1 : 0);
        key[kp++] = (char)(bl >> 24);
        key[kp++] = (char)(bl >> 16);
        key[kp++] = (char)(bl >> 8);
        key[kp++] = (char)bl;
        memcpy(key + kp, vals[i], bl);
        kp += (int)bl;
    }
    key[kp] = '\0';

    /* Lazy-init on first insert so an empty AggCtx pays no allocation. */
    if (!ctx->ht && agg_ht_lazy_init(ctx) != 0) {
        LOG_ERROR(LOG_SUB_QUERY, "agg_find_or_create: agg_ht_lazy_init failed (OOM), reported to client as buffer-exceeded");
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
        khash = agg_hash_len(key, (size_t)kp);
        h = khash & (uint32_t)ctx->ht_mask;
        for (AggBucket *b = ctx->ht[h]; b; b = b->next) {
            if (b->group_key_len == kp &&
                memcmp(b->group_key, key, (size_t)kp) == 0) return b;
        }
    }

    /* New bucket: charge against the shared budget before allocating. */
    if (ctx->shared_buffer_bytes) {
        size_t bucket_bytes = sizeof(AggBucket) + (size_t)(kp + 1) +
                              (size_t)nvals * sizeof(char *) +
                              (size_t)nvals +                    /* group_nulls */
                              (size_t)ctx->nspecs * sizeof(AggAccum);
        for (int i = 0; i < nvals; i++) bucket_bytes += strlen(vals[i]) + 1;
        size_t prev = atomic_fetch_add_explicit(ctx->shared_buffer_bytes,
                                                bucket_bytes, memory_order_relaxed);
        if (prev + bucket_bytes > g_query_buffer_max_bytes) {
            LOG_WARN(LOG_SUB_QUERY, "agg_find_or_create: query buffer cap exceeded at bucket_bytes=%zu (prev=%zu, cap=%zu)", bucket_bytes, prev, g_query_buffer_max_bytes);
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
    b->group_key_len = kp;
    b->group_vals = agg_arena_alloc(&ctx->arena, (size_t)nvals * sizeof(char *));
    b->group_nulls = agg_arena_alloc(&ctx->arena, (size_t)nvals);
    if (!b->group_key || !b->group_vals || !b->group_nulls) {
        ctx->budget_exceeded = 1;
        return NULL;
    }
    for (int i = 0; i < nvals; i++)
        b->group_nulls[i] = nulls ? (nulls[i] ? 1 : 0) : 0;
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
    if (ctx->budget_exceeded || ctx->agg_error) return 1;
    if (query_deadline_tick(ctx->deadline, &ctx->dl_counter)) return 1;
    const uint8_t *raw = block + hdr->key_len;
    const uint8_t hdr2 = (uint8_t)(hdr->flag >> 8);

    int matched = 0;
    if (criteria_match_tree_checked(raw, hdr->value_len, ctx->tree, ctx->fs, hdr2, &matched)) {
        ctx->predicate_error = errno ? errno : EILSEQ;
        return 1;
    }
    if (!matched) return 0;

    AggRow row;
    if (agg_row_open(ctx, raw, hdr->value_len, hdr2, &row)) return 1;

    /* Group components into stack buffers (no malloc). gbuf[i][0] is NUL up
       front so an empty/absent component never leaks a prior record's bytes. */
    char gbuf[MAX_FIELDS][512];
    char *gvals[MAX_FIELDS];
    uint8_t gnulls[MAX_FIELDS] = {0};
    for (int i = 0; i < ctx->ngroups; i++) {
        gbuf[i][0] = '\0';
        if (ctx->group_tfs[i]) {
            if (agg_row_label(ctx, &row, i, gbuf[i], sizeof gbuf[i], &gnulls[i]) < 0)
                return 1;
        } else if (ctx->fs->ts && ctx->fs->ts->packed.nfields) {
            size_t tl = 0;
            errno = 0;
            char *s = typed_packed_named_text(ctx->fs->ts, raw, hdr->value_len,
                                              hdr2, ctx->group_fields[i],
                                              sizeof gbuf[i], &tl);
            if (!s && errno) { ctx->agg_error = errno == ENOBUFS ? E2BIG : errno; return 1; }
            if (s) { memcpy(gbuf[i], s, tl); gbuf[i][tl] = '\0'; free(s); }
        } else {
            char *s = decode_field((const char *)raw, hdr->value_len,
                                   ctx->group_fields[i], ctx->fs, hdr2);
            if (s) {
                size_t sl = strlen(s);
                if (sl >= sizeof gbuf[i]) { free(s); ctx->agg_error = E2BIG; return 1; }
                memcpy(gbuf[i], s, sl); gbuf[i][sl] = '\0';
                free(s);
            }
        }
        gvals[i] = gbuf[i];
    }

    uint8_t raw_key[AGG_INT_KEY_CAP];
    int raw_key_len = 0;
    if (ctx->use_int_keys) raw_key_len = agg_row_raw_key(ctx, &row, gnulls, raw_key);

    AggBucket *bkt;
    if (ctx->ngroups > 0) {
        bkt = agg_find_or_create(ctx, gvals, ctx->ngroups, raw_key, raw_key_len, gnulls);
    } else {
        char *empty = "";
        bkt = agg_find_or_create(ctx, &empty, 1, NULL, 0, NULL);
    }
    if (!bkt) return 1;

    for (int i = 0; i < ctx->nspecs; i++) {
        AggAccum *a = &bkt->accums[i];
        const TypedField *stf = ctx->spec_tfs[i];
        if (ctx->specs[i].fn == AGG_COUNT) {
            /* count(*) counts every record; count(field) skips NULL and, for
               the presence-doctrine types, the encoded unset marker. */
            if (ctx->specs[i].field[0] && stf) {
                TypedRowSpan s;
                int have = agg_row_span(&row, (size_t)(stf - ctx->fs->ts->fields), &s);
                if (have && s.is_null) continue;
                if (agg_count_inspects_presence(stf->type) &&
                    (!have || agg_row_unset(ctx, stf, &s)))
                    continue;
            }
            a->count++;
            continue;
        }
        if (!stf) continue;
        size_t sidx = (size_t)(stf - ctx->fs->ts->fields);
        if ((ctx->specs[i].fn == AGG_MIN || ctx->specs[i].fn == AGG_MAX) &&
            agg_type_is_text(stf->type)) {
            TypedRowSpan s;
            char scratch[64]; const char *text = NULL; size_t text_len = 0;
            if (agg_row_span(&row, sidx, &s) && !s.is_null &&
                agg_span_text(ctx, stf, &s, scratch, sizeof scratch, &text, &text_len))
                agg_text_offer(ctx, a, ctx->specs[i].fn, text, text_len);
            continue;
        }
        if (!ctx->spec_is_num[i] || !row.ok) continue;
        int r = agg_num_offer_row(&ctx->spec_nums[i], &row.v, &a->num, &ctx->env);
        if (r < 0) { ctx->agg_error = errno ? errno : EILSEQ; return 1; }
        if (r == 1) a->count++;
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
            if (!t) { specs = NULL; break;
                    }
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
            else { free(fn); free(alias); free(field); free(specs); *out = NULL; return 0; }
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

static const char *agg_field_type_name(enum FieldType t) {
    switch (t) {
    case FT_VARCHAR: return "varchar";
    case FT_LONG: return "long";
    case FT_INT: return "int";
    case FT_SHORT: return "short";
    case FT_DOUBLE: return "double";
    case FT_FLOAT: return "float";
    case FT_BOOL: return "bool";
    case FT_BYTE: return "byte";
    case FT_NUMERIC: return "numeric";
    case FT_DATE: return "date";
    case FT_DATETIME: return "datetime";
    case FT_DATETIMEMS: return "datetimems";
    case FT_TIME: return "time";
    case FT_TIMESTAMP: return "timestamp";
    case FT_UUID: return "uuid";
    case FT_ENUM: return "enum";
    case FT_IPV4: return "ipv4";
    case FT_IPV6: return "ipv6";
    default: return "unknown";
    }
}

static int agg_validate_spec(const AggSpec *spec, const TypedSchema *ts,
                             char *err, size_t errsz) {
    if (spec->fn == AGG_COUNT && spec->field[0] == '\0') return 0;
    if (strchr(spec->field, '+')) {
        snprintf(err, errsz, "aggregate %s unsupported for field type composite",
                 spec->fn == AGG_COUNT ? "count" :
                 spec->fn == AGG_SUM ? "sum" : spec->fn == AGG_AVG ? "avg" :
                 spec->fn == AGG_MIN ? "min" : "max");
        return -1;
    }
    int fi = typed_field_index(ts, spec->field);
    if (fi < 0) return 0; /* validate_field reports the unknown field */
    enum FieldType t = ts->fields[fi].type;
    int numeric = (t == FT_LONG || t == FT_INT || t == FT_SHORT ||
                   t == FT_DOUBLE || t == FT_FLOAT || t == FT_BOOL ||
                   t == FT_BYTE || t == FT_NUMERIC || t == FT_TIMESTAMP);
    int text_minmax = agg_type_is_text(t);
    int allowed = spec->fn == AGG_COUNT ||
                  ((spec->fn == AGG_SUM || spec->fn == AGG_AVG) && numeric) ||
                  ((spec->fn == AGG_MIN || spec->fn == AGG_MAX) &&
                   (numeric || text_minmax));
    if (!allowed) {
        const char *fn = spec->fn == AGG_SUM ? "sum" :
                         spec->fn == AGG_AVG ? "avg" :
                         spec->fn == AGG_MIN ? "min" : "max";
        snprintf(err, errsz, "aggregate %s unsupported for field type %s",
                 fn, agg_field_type_name(t));
        return -1;
    }
    return 0;
}

/* Validate one having leaf against the statically known result kind of
   the field it names. The field must resolve to an aggregate alias or a
   group_by field — anything else errors here instead of silently
   comparing 0 in agg_bucket_value. The operator must be legal for the
   kind: comparison/set/exists operators on both kinds (text compares
   lexicographically via agg_text_matches), and nothing else — pattern,
   length, field-vs-field, and regex operators have no meaningful
   semantics on aggregate results. Runs before any scanning so a bad
   having is a clean validation error, never a silently-wrong result. */
static int agg_having_leaf_ok(const AggCtx *ctx, const SearchCriterion *c,
                              char *err, size_t errsz) {
    int kind = -1;   /* -1 unresolved, 0 numeric, 1 text */
    int flt = 0;     /* numeric result compares through strtod (AGC_FLT) */
    const char *tname = "unknown";
    for (int g = 0; g < ctx->ngroups; g++) {
        if (strcmp(ctx->group_fields[g], c->field) == 0) {
            if (ctx->group_tfs[g]) {
                kind = ctx->group_is_num[g] ? 0 : 1;
                tname = agg_field_type_name(ctx->group_tfs[g]->type);
            }
            break;
        }
    }
    for (int i = 0; kind < 0 && i < ctx->nspecs; i++) {
        if (strcmp(ctx->specs[i].alias, c->field) != 0) continue;
        if (ctx->specs[i].fn == AGG_COUNT || ctx->specs[i].fn == AGG_SUM ||
            ctx->specs[i].fn == AGG_AVG) {
            kind = 0; tname = "number";
            flt = ctx->spec_is_num[i] && ctx->spec_nums[i].cls == AGC_FLT;
            break;
        }
        /* min/max kind follows the aggregated field's type. */
        if (ctx->spec_tfs[i]) {
            kind = agg_type_is_text(ctx->spec_tfs[i]->type) ? 1 : 0;
            tname = agg_field_type_name(ctx->spec_tfs[i]->type);
            flt = kind == 0 && ctx->spec_is_num[i] &&
                  ctx->spec_nums[i].cls == AGC_FLT;
        }
        break;
    }
    if (kind < 0) {
        snprintf(err, errsz,
                 "unknown field '%s' in having (no matching field, alias, or group_by)",
                 c->field);
        return -1;
    }
    if (kind == 0 && c->op != OP_EXISTS && c->op != OP_NOT_EXISTS) {
        int n = (c->op == OP_IN || c->op == OP_NOT_IN) ? c->in_count
                : (c->op == OP_BETWEEN ? 2 : 1);
        for (int i = 0; i < n; i++) {
            const char *lit = (c->op == OP_IN || c->op == OP_NOT_IN)
                                  ? c->in_values[i]
                                  : (i == 0 ? search_criterion_value(c)
                                            : search_criterion_value2(c));
            int bad;
            if (flt) {
                /* Float-class results evaluate through strtod
                   (agg_value_cmp_text): validate with the same rule so the
                   accepted literal set matches on both sides. */
                char *end;
                double dv = strtod(lit, &end);
                bad = end == lit || *end != '\0' || !isfinite(dv);
            } else {
                /* dec_parse tolerates surrounding whitespace; the
                   canonical number grammar does not — reject it here so
                   validation and evaluation agree. */
                size_t ll = strlen(lit);
                bad = ll == 0 ||
                      dec_parse(lit, ll, &(dec_t){0}, NULL, 0) != DEC_OK;
                if (!bad) {
                    char c0 = lit[0], cl = lit[ll - 1];
                    bad = c0 == ' ' || c0 == '\t' || c0 == '\r' || c0 == '\n' ||
                          cl == ' ' || cl == '\t' || cl == '\r' || cl == '\n';
                }
            }
            if (bad) {
                snprintf(err, errsz, "invalid numeric value '%.64s' for having on '%s'",
                         lit, c->field);
                return -1;
            }
        }
    }
    switch (c->op) {
    case OP_EQUAL: case OP_NOT_EQUAL:
    case OP_LESS: case OP_GREATER: case OP_LESS_EQ: case OP_GREATER_EQ:
    case OP_BETWEEN: case OP_IN: case OP_NOT_IN:
    case OP_EXISTS: case OP_NOT_EXISTS:
        return 0;
    default:
        snprintf(err, errsz,
                 "aggregate having unsupported for field type %s", tname);
        return -1;
    }
}

/* Walk every leaf of the having criteria (tree form or flat JSON form)
   through agg_having_leaf_ok. */
static int agg_having_tree_ok(const AggCtx *ctx, const CriteriaNode *n,
                              char *err, size_t errsz) {
    if (!n) return 0;
    switch (n->kind) {
    case CNODE_LEAF:
        return agg_having_leaf_ok(ctx, &n->leaf, err, errsz);
    case CNODE_AND:
    case CNODE_OR:
        for (int i = 0; i < n->n_children; i++)
            if (agg_having_tree_ok(ctx, n->children[i], err, errsz) < 0)
                return -1;
        return 0;
    }
    return 0;
}

static const dec_t AGG_ZERO_DEC = {0};

/* Zero value for a numeric result with no contributors (sorting keeps the
   historical "absent compares as 0"). */
static void agg_value_zero_fill(AggValue *v) {
    v->present = 1;
    if (v->cls == AGC_DEC && !v->dv) v->dv = &AGG_ZERO_DEC;
}

static void agg_result_release(AggResult *r) { agg_value_release(&r->val); }

/* 0 ok / -1 errno. The result may borrow the bucket's decimals: it must not
   outlive the bucket and must be released with agg_result_release. */
static int agg_bucket_value(const AggCtx *ctx, AggBucket *bkt, const char *name,
                            AggResult *out) {
    memset(out, 0, sizeof *out);
    out->kind = AGG_RESULT_NUMBER;
    agg_value_init(&out->val);
    for (int g = 0; g < ctx->ngroups; g++) {
        if (strcmp(ctx->group_fields[g], name) != 0) continue;
        const char *v = bkt->group_vals[g];
        if (!ctx->group_is_num[g]) {
            out->kind = AGG_RESULT_TEXT;
            out->present = v[0] != '\0';
            out->text = v;
            out->text_len = strlen(v);
            return 0;
        }
        return agg_value_from_label(&ctx->group_nums[g], v, &out->val) ? -1
               : ((out->present = out->val.present), 0);
    }
    for (int i = 0; i < ctx->nspecs; i++) {
        if (strcmp(ctx->specs[i].alias, name) != 0) continue;
        const AggSpec *spec = &ctx->specs[i];
        AggAccum *a = &bkt->accums[i];
        if (spec->fn == AGG_COUNT) {
            agg_value_from_count(&out->val, a->count);
            out->present = 1;
            return 0;
        }
        if (ctx->spec_tfs[i] && agg_type_is_text(ctx->spec_tfs[i]->type)) {
            out->kind = AGG_RESULT_TEXT;
            out->present = a->text_present;
            out->text = spec->fn == AGG_MIN ? a->text_min : a->text_max;
            out->text_len = spec->fn == AGG_MIN ? a->text_min_len : a->text_max_len;
            return 0;
        }
        if (!ctx->spec_is_num[i]) return 0;
        if (agg_num_value(&a->num, &ctx->spec_nums[i], spec->fn, a->count, &out->val))
            return -1;
        out->present = spec->fn == AGG_SUM ? 1 : a->count > 0;
        /* AVG of an integer class is always a decimal value; resetting the
           class for an empty group would leave an INT-tagged value with no
           integer payload next to DEC-tagged siblings. */
        if (!out->val.present && spec->fn != AGG_AVG)
            out->val.cls = ctx->spec_nums[i].cls;
        return 0;
    }
    return 0;
}

static int agg_text_matches(const AggResult *r, const SearchCriterion *c) {
    if (!r->present) return c->op == OP_NOT_EXISTS;
    const char *v = r->text; size_t vl = r->text_len;
    size_t ql = strlen(search_criterion_value(c));
    int cmp = agg_text_cmp(v, vl, search_criterion_value(c), ql);
    switch (c->op) {
    case OP_EQUAL: return cmp == 0;
    case OP_NOT_EQUAL: return cmp != 0;
    case OP_LESS: return cmp < 0;
    case OP_GREATER: return cmp > 0;
    case OP_LESS_EQ: return cmp <= 0;
    case OP_GREATER_EQ: return cmp >= 0;
    case OP_BETWEEN: {
        int lo = agg_text_cmp(v, vl, search_criterion_value(c), strlen(search_criterion_value(c)));
        int hi = agg_text_cmp(v, vl, search_criterion_value2(c), strlen(search_criterion_value2(c)));
        return (c->min_exclusive ? lo > 0 : lo >= 0) &&
               (c->max_exclusive ? hi < 0 : hi <= 0);
    }
    case OP_IN:
        for (int i = 0; i < c->in_count; i++)
            if (agg_text_cmp(v, vl, c->in_values[i], strlen(c->in_values[i])) == 0) return 1;
        return 0;
    case OP_NOT_IN:
        for (int i = 0; i < c->in_count; i++)
            if (agg_text_cmp(v, vl, c->in_values[i], strlen(c->in_values[i])) == 0) return 0;
        return 1;
    case OP_EXISTS: return 1;
    default: return 0;
    }
}

/* 1 match / 0 no match / -1 error (errno). Numeric comparisons are exact:
   decimals by dec_cmp, integers as 128-bit, floats as doubles. */
static int agg_result_matches(const AggResult *r, const SearchCriterion *c) {
    if (r->kind == AGG_RESULT_TEXT) return agg_text_matches(r, c);
    if (!r->present) return c->op == OP_NOT_EXISTS;
    int cmp = 0;
    switch (c->op) {
    case OP_EXISTS: return 1;
    case OP_NOT_EXISTS: return 0;
    case OP_EQUAL: case OP_NOT_EQUAL: case OP_LESS: case OP_GREATER:
    case OP_LESS_EQ: case OP_GREATER_EQ:
        if (agg_value_cmp_text(&r->val, search_criterion_value(c), &cmp)) return -1;
        switch (c->op) {
        case OP_EQUAL: return cmp == 0;
        case OP_NOT_EQUAL: return cmp != 0;
        case OP_LESS: return cmp < 0;
        case OP_GREATER: return cmp > 0;
        case OP_LESS_EQ: return cmp <= 0;
        default: return cmp >= 0;
        }
    case OP_BETWEEN: {
        int lo, hi;
        if (agg_value_cmp_text(&r->val, search_criterion_value(c), &lo) ||
            agg_value_cmp_text(&r->val, search_criterion_value2(c), &hi))
            return -1;
        return (c->min_exclusive ? lo > 0 : lo >= 0) &&
               (c->max_exclusive ? hi < 0 : hi <= 0);
    }
    case OP_IN: case OP_NOT_IN: {
        int found = 0;
        for (int i = 0; i < c->in_count && !found; i++) {
            if (agg_value_cmp_text(&r->val, c->in_values[i], &cmp)) return -1;
            found = cmp == 0;
        }
        return c->op == OP_IN ? found : !found;
    }
    default: return 0;
    }
}

/* One leaf against one bucket: 1/0, or -1 with errno set. */
static int agg_having_leaf_match(const AggCtx *ctx, AggBucket *bkt,
                                 const SearchCriterion *leaf) {
    AggResult val;
    if (agg_bucket_value(ctx, bkt, leaf->field, &val)) return -1;
    int m = agg_result_matches(&val, leaf);
    int e = errno;
    agg_result_release(&val);
    if (m < 0) errno = e;
    return m;
}

/* Check having criteria against a bucket */
static int agg_having_match(const AggCtx *ctx, AggBucket *bkt,
                            SearchCriterion *having, int nhaving) {
    for (int i = 0; i < nhaving; i++) {
        int m = agg_having_leaf_match(ctx, bkt, &having[i]);
        if (m <= 0) return m;
    }
    return 1;
}

/* Tree-aware having evaluation — supports OR, nested AND/OR.
   Mirrors criteria_match_tree() but evaluates against aggregate bucket
   values instead of raw record bytes. */
static int agg_having_match_tree(const AggCtx *ctx, AggBucket *bkt,
                                 const CriteriaNode *n) {
    if (!n) return 1;
    switch (n->kind) {
    case CNODE_LEAF: return agg_having_leaf_match(ctx, bkt, &n->leaf);
    case CNODE_AND:
        for (int i = 0; i < n->n_children; i++) {
            int m = agg_having_match_tree(ctx, bkt, n->children[i]);
            if (m <= 0) return m;
        }
        return 1;
    case CNODE_OR:
        for (int i = 0; i < n->n_children; i++) {
            int m = agg_having_match_tree(ctx, bkt, n->children[i]);
            if (m != 0) return m;
        }
        return 0;
    }
    return 0;
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
    memcpy(dst->spec_nums, src->spec_nums, sizeof(src->spec_nums));
    memcpy(dst->spec_is_num, src->spec_is_num, sizeof(src->spec_is_num));
    memcpy(dst->group_nums, src->group_nums, sizeof(src->group_nums));
    memcpy(dst->group_is_num, src->group_is_num, sizeof(src->group_is_num));
    memcpy(dst->group_idx, src->group_idx, sizeof(src->group_idx));
    agg_env_init(&dst->env, src->shared_buffer_bytes, src->env.limit);
    /* dl_counter stays 0 (per-worker local) */
}

/* Forward decl — definition lives further down near agg_free. Callers
   in parallel_agg_scan_shards_v2 / parallel_indexed_agg / parallel_agg_scan_shards
   need this to release per-worker arenas after merging into main. */
static void agg_ctx_free_local(AggCtx *ctx);

/* Release every bucket's exact states (heap-backed for packed numeric).
   Idempotent: released states are the zero state. Callers must not walk a
   chain whose buckets were spliced elsewhere (see the partition splice). */
static void agg_ctx_release_nums(AggCtx *ctx) {
    if (!ctx->ht) return;
    for (size_t i = 0; i < ctx->ht_cap; i++)
        for (AggBucket *b = ctx->ht[i]; b; b = b->next)
            for (int k = 0; k < ctx->nspecs; k++)
                if (ctx->spec_is_num[k])
                    agg_num_release(&b->accums[k].num, ctx->spec_nums[k].cls,
                                    &ctx->env);
}

/* Merge src's hash table into dst. Buckets in src live in src->arena
   so we don't free them individually — agg_find_or_create copies the
   group_vals strings into dst->arena, then the caller (or
   agg_ctx_free_local on src) frees src->arena en masse. */
static void agg_ctx_merge(AggCtx *dst, AggCtx *src) {
    if(src->predicate_error)dst->predicate_error=src->predicate_error;
    if (src->agg_error && !dst->agg_error) dst->agg_error = src->agg_error;
    int nvals = src->ngroups > 0 ? src->ngroups : 1;
    if (!src->ht) { src->total_buckets = 0; return; }
    for (size_t i = 0; i < src->ht_cap; i++) {
        for (AggBucket *b = src->ht[i]; b; b = b->next) {
            AggBucket *dbkt = agg_find_or_create(dst, b->group_vals, nvals,
                                                  b->raw_key, b->raw_key_len,
                                                  b->group_nulls);
            if (!dbkt) continue;  /* dst budget exhausted — caller checks flag */
            for (int k = 0; k < src->nspecs; k++) {
                AggAccum *sa = &b->accums[k], *da = &dbkt->accums[k];
                if (src->spec_is_num[k] &&
                    agg_num_merge(&da->num, &sa->num, src->spec_nums[k].cls, &dst->env)) {
                    if (!dst->agg_error) dst->agg_error = errno ? errno : EILSEQ;
                }
                da->count += sa->count;
                if (src->spec_is_num[k])
                    agg_num_release(&sa->num, src->spec_nums[k].cls, &src->env);
                if (sa->text_present) {
                    if (sa->text_min)
                        agg_text_offer(dst, da, AGG_MIN, sa->text_min, sa->text_min_len);
                    if (sa->text_max)
                        agg_text_offer(dst, da, AGG_MAX, sa->text_max, sa->text_max_len);
                }
            }
        }
        src->ht[i] = NULL;
    }
    src->total_buckets = 0;
}

/* ── Kf-driven per-shard aggregate fan-out ─────────────────────────────
 * One AggKfShardArg per Kf shard. Each worker owns its shard's Kf read
 * handle via slotcask_scan_live_kf_one — live addresses are snapshotted
 * under the held reader and validated at fetch, so segment flags are
 * never the source of truth — accumulating into a private local AggCtx.
 * After parallel_for_io joins, all locals are merged into main_ctx. */
typedef struct {
    SlotcaskDb *sdb;
    int         kf_shard_id;
    AggCtx      local;         /* per-shard private accumulator */
    int        *stop_flag;
    FILE       *parent_out;
} AggKfShardArg;

/* SlotcaskScanCb adapter: key+value are contiguous in the segment
   record, so pass the key pointer as the v1-style block. */
static int agg_kf_scan_cb(const uint8_t hash[16],
                          const void *key, size_t klen,
                          const void *value, size_t vlen,
                          uint8_t hdr2, void *ctx) {
    AggCtx *ac = (AggCtx *)ctx;
    SlotHeader hdr;
    memcpy(hdr.hash, hash, 16);
    hdr.flag      = 1 | (((unsigned)hdr2 & 0xFF) << 8);
    hdr.key_len   = (uint16_t)klen;
    hdr.value_len = (uint32_t)vlen;
    return agg_scan_cb(&hdr, (const uint8_t *)key, ac);
}

static void *agg_kf_shard_worker(void *raw) {
    AggKfShardArg *arg = (AggKfShardArg *)raw;
    g_out = arg->parent_out ? arg->parent_out : stdout;
    if (__atomic_load_n(arg->stop_flag, __ATOMIC_RELAXED)) return NULL;
    slotcask_scan_live_kf_one(arg->sdb, arg->kf_shard_id,
                              agg_kf_scan_cb, &arg->local, arg->stop_flag);
    return NULL;
}

static void parallel_agg_scan_shards_o_direct(AggCtx *main_ctx,
                                               SlotcaskDb *sdb) {
    if (!sdb || sdb->num_shards <= 0) return;

    int stop_flag = 0;
    FILE *parent_out = g_out;

    AggKfShardArg *args = calloc((size_t)sdb->num_shards, sizeof(*args));
    if (!args) { main_ctx->agg_error = ENOMEM; return; }
    for (int s = 0; s < sdb->num_shards; s++) {
        args[s].sdb = sdb;
        args[s].kf_shard_id = s;
        agg_ctx_clone_shared(&args[s].local, main_ctx);
        args[s].stop_flag  = &stop_flag;
        args[s].parent_out = parent_out;
    }
    g_scan_stop = 0;
    parallel_for_io(agg_kf_shard_worker, args, sdb->num_shards,
                    sizeof(*args));
    for (int s = 0; s < sdb->num_shards; s++) {
        if (args[s].local.budget_exceeded) main_ctx->budget_exceeded = 1;
        agg_ctx_merge(main_ctx, &args[s].local);
        agg_ctx_free_local(&args[s].local);
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
                         uint8_t hdr2, void *ctx_ptr) {
    ShardAggCtx *sa = (ShardAggCtx *)ctx_ptr;
    if (query_deadline_tick(sa->deadline, &sa->dl_counter)) return 1;

    SlotHeader hdr = {0};
    memcpy(hdr.hash, hash, 16);
    hdr.flag = 1 | (((unsigned)hdr2 & 0xFF) << 8);
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
    } else {
        sa->local.agg_error = ENOMEM;   /* never a silent skip */
        return 1;
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
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(sa->db_root, sa->object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "shard_agg_worker: slotcask_registry_get failed for %s/%s", sa->db_root, sa->object);
        sa->local.agg_error = EIO;
        return NULL;
    }

    /* Extract hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)sa->entry_count * sizeof(*hashes));
    if (!hashes) {
        LOG_ERROR(LOG_SUB_QUERY, "shard_agg_worker: malloc %d hashes failed", sa->entry_count);
        sa->local.agg_error = ENOMEM;
        return NULL;
    }
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
    if (!workers) { main_ctx->agg_error = ENOMEM; return; }
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

/* Exact sort keys are materialised once per bucket (so the comparator never
   allocates) and the permutation is sorted through pointers (AggValue may
   borrow its own storage and must not be moved by qsort). */
typedef struct {
    AggBucket *b;
    AggResult  r;        /* aggregate alias, or numeric group label */
    int        use_label;/* text group field: compare group_vals with strcmp */
    int        desc;     /* sort direction, per row (no shared state) */
} AggSortRow;

static int agg_sort_row_cmp(const void *pa, const void *pb) {
    const AggSortRow *x = *(const AggSortRow *const *)pa;
    const AggSortRow *y = *(const AggSortRow *const *)pb;
    int cmp;
    if (x->use_label) {
        cmp = strcmp(x->r.text, y->r.text);
    } else if (x->r.kind == AGG_RESULT_TEXT || y->r.kind == AGG_RESULT_TEXT) {
        cmp = agg_text_cmp(x->r.present ? x->r.text : "", x->r.present ? x->r.text_len : 0,
                           y->r.present ? y->r.text : "", y->r.present ? y->r.text_len : 0);
    } else {
        cmp = agg_value_cmp(&x->r.val, &y->r.val);
    }
    return x->desc ? -cmp : cmp;
}

static int agg_sort_buckets(const AggCtx *ctx, AggBucket **buckets, int n,
                            const char *order_by, int order_desc) {
    if (n < 2) return 0;
    AggSortRow *rows = calloc((size_t)n, sizeof *rows);
    AggSortRow **ptrs = malloc((size_t)n * sizeof *ptrs);
    if (!rows || !ptrs) { free(rows); free(ptrs); errno = ENOMEM; return -1; }
    int rc = 0;
    for (int i = 0; i < n && !rc; i++) {
        rows[i].b = buckets[i];
        rows[i].desc = order_desc;
        ptrs[i] = &rows[i];
        int is_group = -1;
        for (int g = 0; g < ctx->ngroups; g++)
            if (strcmp(ctx->group_fields[g], order_by) == 0) { is_group = g; break; }
        if (is_group >= 0 && !ctx->group_is_num[is_group]) {
            rows[i].use_label = 1;
            rows[i].r.kind = AGG_RESULT_TEXT;
            rows[i].r.text = buckets[i]->group_vals[is_group];
            continue;
        }
        if (agg_bucket_value(ctx, buckets[i], order_by, &rows[i].r)) { rc = -1; break; }
        if (rows[i].r.kind == AGG_RESULT_NUMBER && !rows[i].r.val.present)
            agg_value_zero_fill(&rows[i].r.val);
    }
    if (!rc) {
        qsort(ptrs, (size_t)n, sizeof *ptrs, agg_sort_row_cmp);
        for (int i = 0; i < n; i++) buckets[i] = ptrs[i]->b;
    }
    int e = errno;
    for (int i = 0; i < n; i++) agg_result_release(&rows[i].r);
    free(rows); free(ptrs);
    if (rc) errno = e;
    return rc;
}
#ifdef TEST_BUILD
/* Structural check for the per-row sort direction: sort one array ascending
   and one descending through the real comparator back to back, with no
   direction state set in between (a shared static would make the second
   sort reuse the first call's direction). */
int agg_test_sort_dirs(void) {
    AggSortRow a[3], b[3];
    AggSortRow *pa[3], *pb[3];
    for (int i = 0; i < 3; i++) {
        memset(&a[i], 0, sizeof a[i]);
        memset(&b[i], 0, sizeof b[i]);
        agg_value_init(&a[i].r.val);
        agg_value_init(&b[i].r.val);
        a[i].r.kind = b[i].r.kind = AGG_RESULT_NUMBER;
        a[i].r.present = b[i].r.present = 1;
        a[i].r.val.cls = b[i].r.val.cls = AGC_INT;
        a[i].r.val.present = b[i].r.val.present = 1;
        a[i].r.val.i = b[i].r.val.i = i;   /* b starts ascending like a */
        a[i].desc = 0;
        b[i].desc = 1;
        pa[i] = &a[i];
        pb[i] = &b[i];
    }
    qsort(pa, 3, sizeof *pa, agg_sort_row_cmp);
    qsort(pb, 3, sizeof *pb, agg_sort_row_cmp);
    for (int i = 0; i < 3; i++) {
        if (pa[i]->r.val.i != i) return -1;          /* asc array stayed asc */
        if (pb[i]->r.val.i != 2 - i) return -1;      /* desc array inverted */
    }
    return 0;
}
#endif
static void agg_free(AggCtx *ctx) {
    /* Exact decimal states are heap-backed (decnum buffers) — release them
       before the arena disappears; then the per-query env scratch. */
    agg_ctx_release_nums(ctx);
    agg_env_free(&ctx->env);
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



/* Kind-aware emitters for min/max specs whose field is a text type
   (varchar/calendar). Textual results emit the arena-held canonical
   string — quoted JSON or a raw CSV cell — or JSON null / an empty CSV
   cell when no value was seen. Each returns 1 when it produced the
   output (textual spec) so the caller skips the numeric path, 0 when
   the spec is numeric and must render through the exact cell renderer. */
static int agg_minmax_text_indexed(const char *db_root, const char *object,
                                   const Schema *sch, const AggSpec *spec,
                                   const TypedField *tf, const char *format,
                                   const char *delimiter, int want_total) {
    int desc = spec->fn == AGG_MAX, have = 0, error = 0;
    char best[1024]; size_t best_len = 0;
    for (int sh = 0; sh < index_splits_for(sch->splits) && !error; sh++) {
        char path[PATH_MAX]; struct stat st;
        build_idx_path(path, sizeof path, db_root, object, spec->field, sh);
        if (stat(path, &st) < 0) { if (errno != ENOENT) error = EIO; continue; }
        BtRangeIter *it = btree_range_iter_open(path, "", 0, 0,
                                               AGG_VALUE_TIER_CEIL, 1, 1, desc);
        if (!it) { error = EIO; break; }
        const char *value; size_t vlen; const uint8_t *hash;
        while (btree_range_iter_next(it, &value, &vlen, &hash) == 1) {
            char text[1024]; size_t len = 0;
            if (vlen >= sizeof text) { error = E2BIG; break; }
            if (decode_index_key_to_text(tf, (const uint8_t *)value, vlen,
                                         text, &len) != AGG_VALUE_PRESENT) continue;
            if (!have || (desc ? agg_text_cmp(text, len, best, best_len) > 0
                              : agg_text_cmp(text, len, best, best_len) < 0)) {
                memcpy(best, text, len + 1); best_len = len; have = 1;
            }
            break;
        }
        btree_range_iter_close(it);
    }
    _Atomic size_t bytes = 0;
    AggCell cell = {0}; agg_cells_bind(&cell, 1, &bytes, g_query_buffer_max_bytes);
    if (!error && agg_cell_text(&cell, best, have ? best_len : 0, have)) error = errno;
    if (error) { agg_cells_free(&cell, 1); return agg_report_error(error); }
    char delim = format && !strcmp(format, "csv") ? parse_csv_delim(delimiter) : 0;
    agg_emit_scalar_cells(spec, 1, &cell, delim, want_total);
    agg_cells_free(&cell, 1);
    return 1;
}
static int agg_minmax_same_field_btree(
        const char *db_root, const char *object, const char *field,
        const char *alias, const TypedField *agg_tf, const TypedSchema *ts,
        int splits,
        int is_max, const SearchCriterion *crit,
        const char *format, const char *delimiter, int want_total) {
    const TypedSchema *literal_schema = ts;
    if (!search_criterion_indexable(ts, crit)) return 0;
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
    /* §2.4.2 tier clamp: open high side stops below the NULL tier. */
    const char *max_v = AGG_VALUE_TIER_CEIL; size_t max_l = 1; int max_x = 1;
    switch (crit->op) {
    case OP_EQUAL:
        if(encode_criterion_value_checked(literal_schema,agg_tf,search_criterion_value(crit),strlen(search_criterion_value(crit)),buf1,sizeof(buf1),&len1)<0){return -1;}
        min_v = (const char *)buf1; min_l = len1;
        max_v = (const char *)buf1; max_l = len1;
        break;
    case OP_GREATER:
        if(encode_criterion_value_checked(literal_schema,agg_tf,search_criterion_value(crit),strlen(search_criterion_value(crit)),buf1,sizeof(buf1),&len1)<0){return -1;}
        min_v = (const char *)buf1; min_l = len1; min_x = 1;
        break;
    case OP_GREATER_EQ:
        if(encode_criterion_value_checked(literal_schema,agg_tf,search_criterion_value(crit),strlen(search_criterion_value(crit)),buf1,sizeof(buf1),&len1)<0){return -1;}
        min_v = (const char *)buf1; min_l = len1;
        break;
    case OP_LESS:
        if(encode_criterion_value_checked(literal_schema,agg_tf,search_criterion_value(crit),strlen(search_criterion_value(crit)),buf1,sizeof(buf1),&len1)<0){return -1;}
        max_v = (const char *)buf1; max_l = len1; max_x = 1;
        break;
    case OP_LESS_EQ:
        if(encode_criterion_value_checked(literal_schema,agg_tf,search_criterion_value(crit),strlen(search_criterion_value(crit)),buf1,sizeof(buf1),&len1)<0){return -1;}
        max_v = (const char *)buf1; max_l = len1;
        break;
    case OP_BETWEEN:
        if(encode_criterion_value_checked(literal_schema,agg_tf,search_criterion_value(crit),strlen(search_criterion_value(crit)),buf1,sizeof(buf1),&len1)<0){return -1;}
        if(encode_criterion_value_checked(literal_schema,agg_tf,search_criterion_value2(crit),strlen(search_criterion_value2(crit)),buf2,sizeof(buf2),&len2)<0){return -1;}
        min_v = (const char *)buf1; min_l = len1; min_x = crit->min_exclusive;
        max_v = (const char *)buf2; max_l = len2; max_x = crit->max_exclusive;
        break;
    default: break;
    }

    /* Checked literals are untiered payloads; iterator bounds include tier 0.
       Leave the default tier-1 ceiling untouched. The tier shift moves the
       payload up one byte: a full-capacity literal has no headroom, so the
       shortcut declines and the general record-fed path answers instead
       (defensive — encode_criterion_value_checked currently caps below
       capacity). */
    if (len1 >= sizeof buf1 || len2 >= sizeof buf2) return 0;
    if (min_v == (const char *)buf1 || max_v == (const char *)buf1) {
        memmove(buf1 + 1, buf1, len1); buf1[0] = 0x00;
        if (min_v == (const char *)buf1) min_l++;
        if (max_v == (const char *)buf1) max_l++;
    }
    if (max_v == (const char *)buf2) {
        memmove(buf2 + 1, buf2, len2); buf2[0] = 0x00; max_l++;
    }
    if (crit->op == OP_EQUAL || crit->op == OP_LESS_EQ) max_x = 0;
    AggNumSpec ns;
    if (!agg_num_spec_for(ts, (size_t)(agg_tf - ts->fields), &ns) ||
        !agg_index_exact(&ns))
        return 0;                     /* record-fed route decides (D1) */
    QueryDeadline wdl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    _Atomic size_t wbytes = 0;
    atomic_init(&wbytes, 0);
    AggWalkArg proto = { .db_root = db_root, .object = object, .field = field,
        .spec = &ns, .dl = &wdl, .budget = &wbytes,
        .limit = g_query_buffer_max_bytes, .first_only = 1, .desc = is_max,
        .lo = min_v, .lo_len = min_l, .lo_excl = min_x,
        .hi = max_v, .hi_len = max_l, .hi_excl = max_x };
    AggNum num; memset(&num, 0, sizeof num);
    int64_t cnt = 0;
    int rc = agg_walk_field(&proto, index_splits_for(splits), &num, &cnt);
    AggCell cell; memset(&cell, 0, sizeof cell);
    agg_cells_bind(&cell, 1, &wbytes, g_query_buffer_max_bytes);
    AggSpec one = { .fn = is_max ? AGG_MAX : AGG_MIN };
    snprintf(one.alias, sizeof one.alias, "%s", alias);
    int err = rc < 0 ? (errno ? errno : EILSEQ) : (rc > 0 || wdl.timed_out) ? ETIMEDOUT : 0;
    if (!err && agg_cell_from_num(&ns, &num, one.fn, cnt, &cell)) err = errno;
    AggEnv env; agg_env_init(&env, &wbytes, g_query_buffer_max_bytes);
    agg_num_release(&num, ns.cls, &env);
    agg_env_free(&env);
    if (err) { agg_cells_free(&cell, 1); return agg_report_error(err); }
    char csv_delim_local = (format && strcmp(format, "csv") == 0)
                             ? parse_csv_delim(delimiter) : 0;
    agg_emit_scalar_cells(&one, 1, &cell, csv_delim_local, want_total);
    agg_cells_free(&cell, 1);
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
            if (btree_dispatch_schema_checked(ctx->fs->ts, db_root, object,
                    pc->field, sch->splits, pc,
                    resolve_idx_field(ctx->fs->ts, pc->field),
                    collect_hash_cb, &cc) < 0)
                ctx->predicate_error = errno ? errno : EILSEQ;
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
        SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
        if (sdb) parallel_agg_scan_shards_o_direct(ctx, sdb);
        else ctx->agg_error = EIO;   /* no scan possible: error, not empty */
    }

    if (ctx->deadline->timed_out) return -1;
    if (ctx->budget_exceeded || ctx->predicate_error || ctx->agg_error) return -1;
    return 0;
}

/* Free buckets owned by a cloned/local AggCtx. Counterpart to agg_free
   that omits `free(ctx->specs)` and `pthread_mutex_destroy()` because
   those resources are owned by the cloning origin. */
static void agg_ctx_free_local(AggCtx *ctx) {
    /* Same arena teardown as agg_free, but without freeing the shared
       resources (specs, mutex) — those are owned by the cloning origin. */
    agg_ctx_release_nums(ctx);
    agg_env_free(&ctx->env);
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
    const AggCtx    *actx;           /* owning ctx for exact labels (D2) */
    int              gi;             /* group component index + 1 */
    int              err;            /* first label failure (errno) */
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
    if (!it) {
        LOG_ERROR(LOG_SUB_QUERY, "sec_map_build_worker: btree_range_iter_open %s failed", idx_path);
        return NULL;
    }
    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        if (query_deadline_tick(w->dl, &w->dl_counter)) {
            w->aborted = 1; break;
        }
        /* BTRI: store the TAGGED group component per hash — [u8 is_null]
           [u32 BE len][text]. NULL-tier keys map to the tagged NULL
           component so a secondary group field's NULL forms its own
           bucket (a map miss stays an absent candidate, never NULL). */
        if (vlen == 0) continue;
        if (w->crit_ks && !keyset_contains(w->crit_ks, hash16)) continue;
        char centry[5 + 512];
        int is_null = ((uint8_t)val[0] != 0x00);
        uint32_t bl = 0;
        if (!is_null) {
            char dbuf[512];
            int dlen = agg_index_label(w->actx, w->gi, (const uint8_t *)val + 1,
                                        vlen - 1, dbuf, sizeof(dbuf));
            if (dlen < 0) { w->err = errno ? errno : EILSEQ; w->aborted = 1; break; }
            bl = (uint32_t)dlen;
            memcpy(centry + 5, dbuf, (size_t)dlen);
        }
        centry[0] = (char)(is_null ? 1 : 0);
        centry[1] = (char)(bl >> 24);
        centry[2] = (char)(bl >> 16);
        centry[3] = (char)(bl >> 8);
        centry[4] = (char)bl;
        if (hsm_insert(&w->local_map, hash16, centry, 5 + (size_t)bl) != 0) {
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
    if (!it) {
        LOG_ERROR(LOG_SUB_QUERY, "igb_pass1_worker: btree_range_iter_open %s failed", idx_path);
        return NULL;
    }
    char prev_enc[64]; size_t prev_enc_len = 0;
    int prev_null = 0;
    struct AggBucket *prev_bkt = NULL;
    const char *val; size_t vlen; const uint8_t *hash16;
    while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
        if (query_deadline_tick(w->local.deadline, &w->dl_counter)) {
            w->aborted = 1; break;
        }
        /* BTRI: the primary group component comes from the tiered leaf
           key — tier-1 entries form the NULL bucket via the tagged
           group-key representation (§2.4.5/Task 8). */
        if (vlen == 0) continue;
        int prim_null = ((uint8_t)val[0] != 0x00);
        const uint8_t *prim = (const uint8_t *)val + 1;
        size_t prim_len = vlen - 1;
        if (w->crit_ks && !keyset_contains(w->crit_ks, hash16)) continue;
        struct AggBucket *bkt;
        /* The run compare must include the tier: a NULL key and a
           zero-valued key share the same zeroed payload bytes (bool
           false [0x00][00] vs NULL [0x01][00]) — ignoring the tier
           folds the NULL bucket into the previous value bucket. */
        if (w->n_sec == 0 && prev_bkt && (int)prim_null == prev_null &&
            prim_len == prev_enc_len &&
            memcmp(prim, prev_enc, prim_len) == 0) {
            bkt = prev_bkt;
        } else {
            char gbufs[MAX_FIELDS][512];
            char *gvals[MAX_FIELDS];
            uint8_t gnulls[MAX_FIELDS] = {0};
            uint8_t raw_key[AGG_INT_KEY_CAP];
            int raw_key_len = 0;

            gnulls[0] = (uint8_t)prim_null;
            if (prim_null) gbufs[0][0] = '\0';
            /* Check if we can use raw integer key */
            int use_raw = w->local.use_int_keys && w->n_sec == 0 && prim_len <= AGG_INT_KEY_CAP;
            if (use_raw) {
                /* Scan-path byte identity: tag byte + zeroed natural
                   width for NULL, tag + on-disk BE bytes otherwise. */
                int w0 = typed_field_int_width(w->gtf->type);
                if (w0 <= 0 || 1 + w0 > AGG_INT_KEY_CAP) use_raw = 0;
                else {
                    raw_key[0] = gnulls[0];
                    if (prim_null) memset(raw_key + 1, 0, (size_t)w0);
                    else memcpy(raw_key + 1, prim, (size_t)prim_len);
                    raw_key_len = 1 + w0;
                }
            }
            if (!prim_null) {
                /* decode_idx_to_buf leaves the buffer untouched for a
                   zero-length payload (an empty varchar's [0x00]-alone
                   leaf) and returns 0 — PRESENT-empty, not missing (its
                   internal "plen > 0 always" comment predates BTRI).
                   Initialize the label first or the bucket key reads
                   uninitialized stack (§2.4.5). */
                gbufs[0][0] = '\0';
                int n = agg_index_label(&w->local, 0, prim, prim_len,
                                        gbufs[0], sizeof(gbufs[0]));
                if (n < 0) { w->local.agg_error = errno ? errno : EILSEQ; w->aborted = 1; break; }
                gvals[0] = gbufs[0];
            } else {
                gvals[0] = gbufs[0];
            }

            int multi_skip = 0;
            for (int g = 0; g < w->n_sec; g++) {
                const char *sval; size_t slen;
                if (!hsm_get(&w->sec_maps[g], hash16, &sval, &slen)) {
                    multi_skip = 1; break;
                }
                int snull = 0;
                const char *stext = ""; size_t stext_len = 0;
                hsm_parse_tagged(sval, slen, &snull, &stext, &stext_len);
                gnulls[g + 1] = (uint8_t)snull;
                size_t cl = stext_len < sizeof(gbufs[g + 1]) - 1
                             ? stext_len : sizeof(gbufs[g + 1]) - 1;
                memcpy(gbufs[g + 1], stext, cl);
                gbufs[g + 1][cl] = '\0';
                gvals[g + 1] = gbufs[g + 1];
            }
            if (multi_skip) continue;
            bkt = (struct AggBucket *)agg_find_or_create(
                    &w->local, gvals, w->local.ngroups, raw_key, raw_key_len,
                    gnulls);
            if (!bkt) { w->aborted = 1; break;
                    }
            if (w->n_sec == 0 && prim_len <= sizeof(prev_enc)) {
                memcpy(prev_enc, prim, prim_len);
                prev_enc_len = prim_len;
                prev_null = (int)prim_null;
                prev_bkt = bkt;
            } else {
                prev_bkt = NULL; prev_enc_len = 0; prev_null = 0;
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
        if (!nb) {
            LOG_ERROR(LOG_SUB_QUERY, "bktarr_push: realloc to cap=%d failed", new_cap);
            return -1;
        }
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
            uint32_t h = agg_bucket_hash(s->src, b);
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
                                                src->raw_key, src->raw_key_len,
                                                src->group_nulls);
            if (!mb) { p->aborted = 1; return NULL; }
            for (int k = 0; k < p->local.nspecs; k++) {
                AggAccum *sa = &src->accums[k], *da = &mb->accums[k];
                if (p->local.spec_is_num[k] &&
                    agg_num_merge(&da->num, &sa->num, p->local.spec_nums[k].cls,
                                  &p->local.env)) {
                    p->local.agg_error = errno ? errno : EILSEQ;
                    p->aborted = 1;
                    return NULL;
                }
                da->count += sa->count;
                if (p->local.spec_is_num[k])
                    agg_num_release(&sa->num, p->local.spec_nums[k].cls,
                                    &p->local.env);
                if (sa->text_present) {
                    if (sa->text_min)
                        agg_text_offer(&p->local, da, AGG_MIN,
                                       sa->text_min, sa->text_min_len);
                    if (sa->text_max)
                        agg_text_offer(&p->local, da, AGG_MAX,
                                       sa->text_max, sa->text_max_len);
                }
            }
            /* Stash main bucket pointer in src->group_key for hbk translate
               (same trick as the serial merge path). */
            src->group_key = (char *)mb;
        }
    }
    return NULL;
}

/* Per-emit staging slot for the varchar-streaming fast path. Holds
   tentative aggregate state for one distinct group key; only committed
   into ctx.ht once every emit's lookups have succeeded (or we abort
   cleanly to IGB if a run is too long). */
typedef struct {
    char    key[BT_MAX_VAL_LEN + 1];
    size_t  klen;
    int64_t total_count;
} VSStaged;

/* One deferred lookup: a hash16 plus the staged[] index its record feeds. */
typedef struct {
    uint8_t h[BT_HASH_SIZE];
    int     slot;
} VSPendPair;

typedef struct {
    VSStaged              *staged;         /* base of staging array */
    const AggSpec         *specs;
    const TypedField     **spec_tfs;
    const int             *spec_tidx;      /* schema index per spec (NULL-bit test) */
    int                    nspecs;
    const TypedSchema     *ts;             /* for typed_row_open */
    AggNum                *nums;           /* [slot * nspecs + i] exact states */
    int64_t               *counts;         /* contributing values per slot/spec */
    const AggNumSpec      *spec_nums;      /* per-spec exact classification */
    AggEnv                *env;            /* owning ctx env (QUERY_BUFFER accounting) */
    int                    err;            /* first offer failure (errno) */
    const VSPendPair      *pairs;          /* sorted by hash16 */
    size_t                 n;
    /* Partition workers deliver concurrently; staged accums are shared
     * per slot set, so attribution writes serialize through this mu. */
    pthread_mutex_t        mu;
} VSLookupCtx;

/* Binary search the flushed pending set for `hash16`; -1 when absent. */
static int vs_pair_find(const VSLookupCtx *c, const uint8_t hash16[16]) {
    size_t lo = 0, hi = c->n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int r = memcmp(c->pairs[mid].h, hash16, BT_HASH_SIZE);
        if (r == 0) return (int)mid;
        if (r < 0) lo = mid + 1;
        else hi = mid;
    }
    return -1;
}

/* slotcask_lookup_by_hash callback for the varchar-streaming fast path.
   Decodes every SUM/AVG/MIN/MAX spec's field from the typed record bytes
   in one pass and accumulates into the per-emit staging slot. Returns 1
   to stop probing after the first match — hash16 collisions are rare and
   the iter's caller already attributed this exact entry to the run, so
   probing further could miscount. */
static int vs_lookup_cb(const uint8_t hash16[16],
                        const void *key, size_t klen,
                        const void *value, size_t vlen,
                        uint8_t hdr2, void *raw) {
    (void)key; (void)klen;
    VSLookupCtx *c = (VSLookupCtx *)raw;
    int p = vs_pair_find(c, hash16);
    if (p < 0) return 0;
    pthread_mutex_lock(&c->mu);
    const uint8_t *rec = (const uint8_t *)value;
    TypedRowView view;
    int vok = typed_row_open(c->ts, rec, vlen, hdr2, &view) == 0;
    for (int i = 0; i < c->nspecs; i++) {
        enum AggFn fn = c->specs[i].fn;
        if (fn == AGG_COUNT || !c->spec_tfs[i] || !vok) continue;
        /* NULL spec value skips the accumulator (§2.5: sum/avg/min/max
           skip NULLs; avg's denominator counts non-NULL only). */
        if (c->spec_tidx && c->spec_tidx[i] >= 0) {
            TypedRowSpan sp;
            if (typed_row_span(&view, (size_t)c->spec_tidx[i], &sp) == 0 &&
                sp.is_null) continue;
        }
        size_t k = (size_t)c->pairs[p].slot * (size_t)c->nspecs + (size_t)i;
        int r = agg_num_offer_row(&c->spec_nums[i], &view, &c->nums[k], c->env);
        if (r < 0) { c->err = errno ? errno : EILSEQ; break; }
        if (r == 1) c->counts[k]++;
    }
    /* Batched attribution is exact via the pair map; a non-zero return
       would abort the enclosing bulk-fetch run and drop the rest of the
       flushed batch. */
    pthread_mutex_unlock(&c->mu);
    return 0;
}


/* Flush one batch of deferred VS lookups (see the merge loop below).
 * Closes every open cursor FIRST so the blocking kf fetch happens with
 * no bt_cache rdlocks held (2026-08-27 lock-inversion fix), then — when
 * want_reopen is set — reopens each live cursor at its own cur_keys[]
 * snapshot (head snapshots were advanced past consumed runs before the
 * close, and leaf order is monotone ascending, so nothing redelivers). */
static int vs_pend_cmp(const void *a, const void *b) {
    const VSPendPair *pa = a, *pb = b;
    return memcmp(pa->h, pb->h, BT_HASH_SIZE);
}

static void vs_flush_pending(SlotcaskDb *sdb,
                             VSLookupCtx *lc,
                             VSPendPair *pairs, size_t pend_n,
                             BtRangeIter **iters, int *has_cur,
                             char (*cur_keys)[BT_MAX_VAL_LEN + 1],
                             size_t *cur_klens, uint8_t (*cur_hash16)[16],
                             int n_idx_g, const char *db_root,
                             const char *object, const char *group_field,
                             int want_reopen) {
    if (!sdb || pend_n == 0) return;

    for (int s = 0; s < n_idx_g; s++) {
        if (iters[s]) { btree_range_iter_close(iters[s]); iters[s] = NULL; }
    }

    qsort(pairs, pend_n, sizeof(VSPendPair), vs_pend_cmp);
    lc->pairs = pairs;
    lc->n     = pend_n;
    {int hits=0; (void)hits;}
    /* Dense parallel hash matrix for the bulk resolver (pairs stride is
       24 B, not 16 B, so materialize contiguity explicitly). */
    uint8_t (*dense)[16] = malloc(pend_n * 16);
    if (!dense) return;
    for (size_t i = 0; i < pend_n; i++)
        memcpy(dense[i], pairs[i].h, 16);
    slotcask_bulk_resolve_and_fetch(sdb, (const uint8_t (*)[16])dense,
                                    pend_n, lc, vs_lookup_cb);
    free(dense);

    if (!want_reopen) return;
    for (int s = 0; s < n_idx_g; s++) {
        if (!has_cur[s]) continue;
        char idx_path[PATH_MAX];
        build_idx_path(idx_path, sizeof(idx_path), db_root, object,
                       group_field, s);
        iters[s] = btree_range_iter_open(
            idx_path, cur_keys[s], cur_klens[s], 0,
            "\xff\xff\xff\xff", 4, 0, 0);
        if (!iters[s]) { has_cur[s] = 0; continue; }
        /* Restore the head entry. Leaf order is monotone, so the first
         * yield is >= the snapshot; accept anything at-or-after it. */
        int restored = 0;
        for (int guard = 0; guard < 4096 && !restored; guard++) {
            const char *v; size_t vl; const uint8_t *h;
            if (btree_range_iter_next(iters[s], &v, &vl, &h) != 1) {
                has_cur[s] = 0;
                break;
            }
            if (vl < cur_klens[s] ||
                memcmp(v, cur_keys[s], cur_klens[s]) < 0)
                continue;                       /* stale-ish; skip forward */
            size_t cap = (vl > BT_MAX_VAL_LEN) ? BT_MAX_VAL_LEN : vl;
            memcpy(cur_keys[s], v, cap);
            cur_klens[s] = cap;
            memcpy(cur_hash16[s], h, 16);
            restored = 1;
        }
        if (!restored && iters[s]) {
            btree_range_iter_close(iters[s]);
            iters[s] = NULL;
            has_cur[s] = 0;
        }
    }
}

/* Single-spec MIN/MAX driven by a KeySet over an indexed field.
   Walks each .idx shard ASC/DESC seeking the first hash contained in
   `ks` per shard (yields that shard's min/max), then aggregates across
   shards. Emits the formatted response (CSV or JSON) and FREES `ks`
   on return. Caller still frees criteria_tree + specs and returns 0.

   Two cmd_aggregate planner branches need this exact loop+emit logic
   (single-leaf KeySet path and AND-intersect KeySet path) — extracted
   to avoid the 45-line duplication CPD flagged. */
static int emit_min_max_via_keyset(const char *db_root, const char *object,
                                    const Schema *sch,
                                    const AggSpec *spec,
                                    const TypedField *agg_tf,
                                    const TypedSchema *ts,
                                    KeySet *ks,
                                    const char *format,
                                    const char *delimiter,
                                    int want_total) {
    AggNumSpec ns;
    if (!agg_num_spec_for(ts, (size_t)(agg_tf - ts->fields), &ns) ||
        !agg_index_exact(&ns)) {
        keyset_free(ks);
        return 0;                     /* record-fed route decides (D1) */
    }
    QueryDeadline wdl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    _Atomic size_t wbytes = 0;
    atomic_init(&wbytes, 0);
    AggWalkArg proto = { .db_root = db_root, .object = object,
        .field = spec->field, .spec = &ns, .ks = ks, .dl = &wdl,
        .budget = &wbytes, .limit = g_query_buffer_max_bytes,
        .first_only = 1, .desc = spec->fn == AGG_MAX };
    AggNum num; memset(&num, 0, sizeof num);
    int64_t cnt = 0;
    int rc = agg_walk_field(&proto, index_splits_for(sch->splits), &num, &cnt);
    keyset_free(ks);
    AggCell cell; memset(&cell, 0, sizeof cell);
    agg_cells_bind(&cell, 1, &wbytes, g_query_buffer_max_bytes);
    int err = rc < 0 ? (errno ? errno : EILSEQ) : (rc > 0 || wdl.timed_out) ? ETIMEDOUT : 0;
    if (!err && agg_cell_from_num(&ns, &num, spec->fn, cnt, &cell)) err = errno;
    AggEnv env; agg_env_init(&env, &wbytes, g_query_buffer_max_bytes);
    agg_num_release(&num, ns.cls, &env);
    agg_env_free(&env);
    if (err) { agg_cells_free(&cell, 1); return agg_report_error(err); }
    char csv_delim_local = (format && strcmp(format, "csv") == 0)
                             ? parse_csv_delim(delimiter) : 0;
    agg_emit_scalar_cells(spec, 1, &cell, csv_delim_local, want_total);
    agg_cells_free(&cell, 1);
    return 1;
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
                            CriteriaNode *having_tree,   /* NEW — priority over having_json */
                            const char *order_by, int order_desc, int limit,
                            const char *format, const char *delimiter, int want_total,
                            const Schema *sch_in, const FieldSchema *fs_in) {
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;

    Schema sch;
    FieldSchema fs;
    if (sch_in && fs_in) { sch = *sch_in; fs = *fs_in; }   /* test seam: private schema */
    else {
        sch = load_schema(db_root, object);
        init_field_schema(&fs, db_root, object);
    }

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
            if (agg_validate_spec(&specs[i], fs.ts, verr, sizeof(verr)) < 0) {
                OUT("{\"error\":\"%s\"}\n", verr); return -1;
            }
        }
        /* order_by validated below after group_by parse — it may reference
           an aggregate alias or a group_by field, not just a typed field. */
    }

    if(compile_criteria_tree_checked(tree,fs.ts,NULL,0)){OUT("{\"error\":\"invalid criteria value\"}\n");return -1;}

    /* Fast path: count-only with no criteria and no group_by → metadata.
       Skip fields whose presence semantics require inspecting the value. */
    int no_group = (!group_by_json || group_by_json[0] == '\0' || strcmp(group_by_json, "[]") == 0);
    int no_having = !having_tree && (!having_json || having_json[0] == '\0');
    if (!tree && no_group && no_having && nspecs == 1 && specs[0].fn == AGG_COUNT) {
        int needs_presence_filter = 0;
        if (specs[0].field[0] && fs.ts && !strchr(specs[0].field, '+')) {
            int fi = typed_field_index(fs.ts, specs[0].field);
            if (fi >= 0 && agg_count_inspects_presence(fs.ts->fields[fi].type))
                needs_presence_filter = 1;
        }
        if (!needs_presence_filter) {
            int n = get_live_count(db_root, object);
            if (want_total)
                OUT("{\"rows\":{\"%s\":%d},\"total\":1}\n", specs[0].alias, n);
            else
                OUT("{\"%s\":%d}\n", specs[0].alias, n);
            return 0;
        }
    }

    /* Fast path: single-spec text min/max on an indexed calendar field.
       Varchar declines this bounded text decoder and uses full record spans,
       including with an index present (D8). */
    if (!tree && no_group && no_having && nspecs == 1 &&
        (specs[0].fn == AGG_MIN || specs[0].fn == AGG_MAX) &&
        fs.ts && specs[0].field[0] && !strchr(specs[0].field, '+')) {
        int fi = typed_field_index(fs.ts, specs[0].field);
        if (fi >= 0 &&
            agg_type_is_text(fs.ts->fields[fi].type) &&
            fs.ts->fields[fi].type != FT_VARCHAR &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
            int tr = agg_minmax_text_indexed(db_root, object, &sch, &specs[0],
                                    &fs.ts->fields[fi], format, delimiter,
                                    want_total);
            return tr < 0 ? -1 : 0;
        }
    }

    /* Fast path: no criteria, no group, no having. Every spec is count(*),
       count(field) on an indexed numeric-class field, or sum/avg/min/max on
       an indexed numeric-class field whose index key carries its full value
       (D1). One exact walk per distinct field; count(*) reads metadata. */
    if (!tree && no_group && no_having && fs.ts) {
        int eligible = 1, has_walk = 0;
        for (int i = 0; i < nspecs && eligible; i++) {
            AggSpec *sp = &specs[i];
            if (sp->fn == AGG_COUNT && !sp->field[0]) continue;
            AggNumSpec ns;
            int fi = sp->field[0] && !strchr(sp->field, '+')
                         ? typed_field_index(fs.ts, sp->field) : -1;
            if (fi < 0 || !agg_num_spec_for(fs.ts, (size_t)fi, &ns) ||
                !agg_index_exact(&ns) ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits))
                eligible = 0;
            else has_walk = 1;
        }
        if (eligible && has_walk) {
            QueryDeadline wdl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
            _Atomic size_t wbytes = 0;
            atomic_init(&wbytes, 0);
            AggCell cells[MAX_AGG_SPECS];
            memset(cells, 0, sizeof cells);
            agg_cells_bind(cells, nspecs, &wbytes, g_query_buffer_max_bytes);
            int done[MAX_AGG_SPECS] = {0}, err = 0;
            int n_idx = index_splits_for(sch.splits);
            for (int i = 0; i < nspecs && !err; i++) {
                if (done[i] || !specs[i].field[0]) continue;
                int fi = typed_field_index(fs.ts, specs[i].field);
                AggNumSpec ns;
                agg_num_spec_for(fs.ts, (size_t)fi, &ns);
                int sibs[MAX_AGG_SPECS], nsibs = 0, nmin = 0, nmax = 0, nfull = 0;
                for (int j = i; j < nspecs; j++) {
                    if (done[j] || strcmp(specs[j].field, specs[i].field) != 0) continue;
                    sibs[nsibs++] = j; done[j] = 1;
                    if (specs[j].fn == AGG_MIN) nmin++;
                    else if (specs[j].fn == AGG_MAX) nmax++;
                    else nfull++;
                }
                AggWalkArg proto = { .db_root = db_root, .object = object,
                    .field = specs[i].field, .spec = &ns, .dl = &wdl,
                    .budget = &wbytes, .limit = g_query_buffer_max_bytes };
                if (!nfull && (nmin == 0 || nmax == 0)) {   /* pure min or pure max */
                    proto.first_only = 1;
                    proto.desc = nmax > 0;
                }
                AggNum num; memset(&num, 0, sizeof num);
                int64_t cnt = 0;
                AggEnv env; agg_env_init(&env, &wbytes, g_query_buffer_max_bytes);
                int rc = agg_walk_field(&proto, n_idx, &num, &cnt);
                if (rc < 0) err = errno ? errno : EILSEQ;
                else if (rc > 0 || wdl.timed_out) err = ETIMEDOUT;
                for (int k = 0; k < nsibs && !err; k++) {
                    int si = sibs[k];
                    if (specs[si].fn == AGG_COUNT) {
                        if (agg_cell_count(&cells[si], cnt)) err = errno;
                    } else if (agg_cell_from_num(&ns, &num, specs[si].fn, cnt, &cells[si])) {
                        err = errno;
                    }
                }
                agg_num_release(&num, ns.cls, &env);
                agg_env_free(&env);
            }
            for (int i = 0; i < nspecs && !err; i++)
                if (specs[i].fn == AGG_COUNT && !specs[i].field[0] &&
                    agg_cell_count(&cells[i], (int64_t)get_live_count(db_root, object)))
                    err = errno;
            if (err) {
                agg_cells_free(cells, nspecs);
                return agg_report_error(err);
            }
            agg_emit_scalar_cells(specs, nspecs, cells, csv_delim, want_total);
            agg_cells_free(cells, nspecs);
            return 0;
        }
    }

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
                                      gb_csv, order_by, limit,
                                      having_tree ? "1" : having_json,
                                      format) &&
            /* The streaming heap stores only numeric metrics. Disable the
               optimization when order_by resolves to a textual aggregate,
               or when any min/max spec aggregates a textual field (given
               top-N eligibility such specs sit on the group field, where
               the leaf decode yields no numeric value). Count-only and
               numeric-aggregate streams over a textual group field keep
               the documented streaming fallback. */
            ({ int text_order = 0;
               for (int si = 0; si < nspecs; si++) {
                   if (specs[si].fn != AGG_MIN && specs[si].fn != AGG_MAX)
                       continue;
                   if (!specs[si].field[0] || strchr(specs[si].field, '+'))
                       continue;
                   int fi = typed_field_index(fs.ts, specs[si].field);
                   if (fi >= 0 && agg_type_is_text(fs.ts->fields[fi].type))
                       text_order = 1;
               }
               !text_order; })) {
            QueryDeadline topn_dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
            int rc = agg_run_topn_stream(db_root, object, &sch, &fs,
                                          specs, nspecs, gb_csv,
                                          order_by, order_desc,
                                          limit, tree, &topn_dl,
                                          want_total);
            if (rc == 0) {
                return 0;
            }
            if (rc == -3) return -1; /* error response was already written */
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
        ctx.group_idx[i] = -1;
        if (fs.ts && !strchr(ctx.group_fields[i], '+')) {
            int idx = typed_field_index(fs.ts, ctx.group_fields[i]);
            if (idx >= 0) {
                ctx.group_tfs[i] = &fs.ts->fields[idx];
                ctx.group_idx[i] = idx;
            }
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
            if (!ctx.group_tfs[i]) { all_int = 0; break;
                    }
            int w = typed_field_int_width(ctx.group_tfs[i]->type);
            if (w <= 0) { all_int = 0; break;
                    }
            total_width += w + 1;   /* +1: Task 8 NULL tag byte per component */
        }
        ctx.use_int_keys = (all_int && total_width <= AGG_INT_KEY_CAP);
    }

    for (int i = 0; i < ctx.nspecs && i < MAX_AGG_SPECS; i++) {
        ctx.spec_tfs[i] = NULL;
        /* Resolve TypedField for COUNT specs too — agg_scan_cb's
           count(field) presence-doctrine check needs ctx.spec_tfs to
           know the field's type. Composite ("a+b") still resolves NULL. */
        if (ctx.specs[i].field[0] && fs.ts && !strchr(ctx.specs[i].field, '+')) {
            int idx = typed_field_index(fs.ts, ctx.specs[i].field);
            if (idx >= 0) ctx.spec_tfs[i] = &fs.ts->fields[idx];
        }
    }

    agg_env_init(&ctx.env, &agg_budget_bytes, g_query_buffer_max_bytes);
    for (int i = 0; i < ctx.nspecs && i < MAX_AGG_SPECS; i++)
        if (ctx.spec_tfs[i] && ctx.specs[i].fn != AGG_COUNT)
            ctx.spec_is_num[i] = (uint8_t)agg_num_spec_for(
                fs.ts, (size_t)(ctx.spec_tfs[i] - fs.ts->fields), &ctx.spec_nums[i]);
    for (int i = 0; i < ctx.ngroups; i++)
        if (ctx.group_tfs[i])
            ctx.group_is_num[i] = (uint8_t)(agg_num_spec_for(
                fs.ts, (size_t)ctx.group_idx[i], &ctx.group_nums[i]) &&
                ctx.group_tfs[i]->type != FT_BOOL);
    /* Packed decimals are variable-width values. Legacy numeric's 8-byte
       raw grouping shortcut would collide or disagree with index keys. */
    if (fs.ts && fs.ts->packed.nfields)
        for (int i = 0; i < ctx.ngroups; i++)
            if (ctx.group_is_num[i] && ctx.group_nums[i].cls == AGC_DEC)
                ctx.use_int_keys = 0;

    /* Pre-scan having validation: every leaf's field must resolve to an
       aggregate alias or group_by field, and its operator must fit the
       statically known result kind — before any scan or index walk. */
    if (having_tree || (having_json && having_json[0])) {
        char verr[256];
        int hv_bad = 0;
        if (having_tree) {
            hv_bad = agg_having_tree_ok(&ctx, having_tree, verr, sizeof(verr)) < 0;
        } else {
            SearchCriterion *hv = NULL; int nhv = 0;
            /* Parse failures surface at the existing late-parse site. */
            if (parse_criteria_json(having_json, &hv, &nhv) == 0) {
                for (int i = 0; i < nhv; i++) {
                    if (agg_having_leaf_ok(&ctx, &hv[i], verr, sizeof(verr)) < 0) {
                        hv_bad = 1; break;
                    }
                }
                free_criteria(hv, nhv);
            }
        }
        if (hv_bad) {
            OUT("{\"error\":\"%s\"}\n", verr);
            agg_free(&ctx);
            return -1;
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
            !agg_type_is_text(fs.ts->fields[sf_agg_fi].type) &&
            agg_index_exact_field(fs.ts, &fs.ts->fields[sf_agg_fi]) &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
            const TypedField *agg_tf = &fs.ts->fields[sf_agg_fi];
            SearchCriterion *crit = &sf_leaf_node->leaf;
            int minmax_status0=agg_minmax_same_field_btree(
                    db_root, object, specs[0].field, specs[0].alias,
                    agg_tf, fs.ts, sch.splits,
                    specs[0].fn == AGG_MAX, crit, format, delimiter, want_total);
            if(minmax_status0<0)return -1;
            if(minmax_status0) {
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
            !agg_type_is_text(fs.ts->fields[agg_fi].type) &&
            agg_index_exact_field(fs.ts, &fs.ts->fields[agg_fi]) &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits)) {
            const TypedField *agg_tf = &fs.ts->fields[agg_fi];
            AggNumSpec wfc_ns;
            agg_num_spec_for(fs.ts, (size_t)agg_fi, &wfc_ns);
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
                    wargs[s].spec      = &wfc_ns;
                }
                parallel_for(wfc_worker, wargs, n_idx, sizeof(WfcArg));

                int any_indefinite = 0;
                int predicate_error=0;
                int found = 0;
                AggNum total; memset(&total, 0, sizeof total);
                AggEnv wenv; agg_env_init(&wenv, NULL, 0);
                for (int s = 0; s < n_idx; s++) {
                    if (wargs[s].budget_exceeded) any_indefinite = 1;
                    if(wargs[s].predicate_error)predicate_error=wargs[s].predicate_error;
                    if (!predicate_error &&
                        agg_num_merge(&total, &wargs[s].num, wfc_ns.cls, &wenv))
                        predicate_error = errno ? errno : EILSEQ;
                    agg_num_release(&wargs[s].num, wfc_ns.cls, &wenv);
                    agg_env_free(&wargs[s].env);   /* worker scratch (T3) */
                    if (wargs[s].found) found = 1;
                }
                agg_env_free(&wenv);
                free(wargs);
                if(predicate_error){int e=predicate_error;agg_num_release(&total,wfc_ns.cls,&wenv);return agg_report_error(e);}
                if (dl.timed_out) {
                    LOG_WARN(LOG_SUB_QUERY, "cmd_aggregate_do: query deadline exceeded (single-shard fast path)");
                    agg_num_release(&total, wfc_ns.cls, &wenv);
                    OUT("{\"error\":\"query_timeout\"}\n");
                    return -1;
                }
                if (!any_indefinite) {
                    AggCell cell; memset(&cell, 0, sizeof cell);
                    agg_cells_bind(&cell, 1, ctx.shared_buffer_bytes, g_query_buffer_max_bytes);
                    int err = 0;
                    if (agg_cell_from_num(&wfc_ns, &total, specs[0].fn, found ? 1 : 0, &cell))
                        err = errno ? errno : EILSEQ;
                    agg_num_release(&total, wfc_ns.cls, &wenv);
                    if (err) { agg_cells_free(&cell, 1); return agg_report_error(err); }
                    char csv_delim_local = (format && strcmp(format, "csv") == 0)
                                             ? parse_csv_delim(delimiter) : 0;
                    agg_emit_scalar_cells(&specs[0], 1, &cell, csv_delim_local, want_total);
                    agg_cells_free(&cell, 1);
                    return 0;
                }
                agg_num_release(&total, wfc_ns.cls, &wenv);
                /* else: fall through to keyset/intersect paths below */
            }
        }
    }

    /* Fast path: single-spec MIN/MAX narrowed by ONE indexed criterion.
       Build candidate KeySet from the criterion's index walk, then walk
       the agg field's btree in ASC (MIN) or DESC (MAX) order — the first
       leaf entry whose hash is in the KeySet is the answer. No record
       fetches; the agg value comes straight from the leaf bytes via
       the exact key decoder. ~10× faster than agg_run_plan's path
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
            !agg_type_is_text(fs.ts->fields[agg_fi].type) &&
            agg_index_exact_field(fs.ts, &fs.ts->fields[agg_fi]) &&
            btree_idx_exists(db_root, object, specs[0].field, sch.splits) &&
            leaf_is_indexed(&crit_leaf_node->leaf, db_root, object, NULL, 0)) {
            const TypedField *agg_tf = &fs.ts->fields[agg_fi];
            SearchCriterion *crit = &crit_leaf_node->leaf;
            const TypedField *crit_tf = resolve_idx_field(fs.ts, crit->field);

            /* Same-field shortcut: when crit->field == specs[0].field, the
               criterion's btree IS the agg field's btree. No KeySet needed —
               see agg_minmax_same_field_btree for the full walk. */
            int minmax_status1=agg_minmax_same_field_btree(
                    db_root, object, specs[0].field, specs[0].alias,
                    agg_tf, fs.ts, sch.splits,
                    specs[0].fn == AGG_MAX, crit, format, delimiter, want_total);
            if(minmax_status1<0)return -1;
            if(minmax_status1) {
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
                if(btree_dispatch_checked(db_root, object, crit->field, sch.splits,
                               crit, crit_tf, stream_keyset_cb, &sk)<0){keyset_free(ks);OUT("{\"error\":\"invalid index criterion\"}\n");return -1;}

                if (!atomic_load_explicit(&sk.full, memory_order_relaxed) &&
                    !dl.timed_out && keyset_size(ks) > 0) {
                    int ks_rc = emit_min_max_via_keyset(db_root, object, &sch,
                                            &specs[0], agg_tf, fs.ts, ks,
                                            format, delimiter, want_total);
                    if (ks_rc > 0) return 0;        /* answered */
                    if (ks_rc < 0) return -1;       /* error already emitted */
                    /* 0: declined — fall through to the general path. The
                       callee frees ks itself on the declined path. Today
                       unreachable: both callers gate on agg_index_exact_field,
                       the same condition the callee declines on (defense in
                       depth for D1), so no behavioural test can force it. */
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
            !agg_type_is_text(fs.ts->fields[agg_fi].type) &&
            agg_index_exact_field(fs.ts, &fs.ts->fields[agg_fi]) &&
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
                    int ks_rc = emit_min_max_via_keyset(db_root, object, &sch,
                                            &specs[0], agg_tf, fs.ts, ks,
                                            format, delimiter, want_total);
                    if (ks_rc > 0) return 0;        /* answered */
                    if (ks_rc < 0) return -1;       /* error already emitted */
                    /* 0: declined — fall through to the general path. The
                       callee frees ks itself on the declined path. Today
                       unreachable: both callers gate on agg_index_exact_field,
                       the same condition the callee declines on (defense in
                       depth for D1), so no behavioural test can force it. */
                }
                if (ks) keyset_free(ks);
            }
        }
    }

    /* No NEQ algebraic split: agg(neq X) = agg(*) − agg(eq X) silently
       assigns NULL fields to the full side (a NULL belongs to neither the
       positive nor the negative value predicate). NEQ aggregates route
       through the normal planner path with the original criteria tree;
       matches are verified with generation-correct semantics. */

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
                    if (strchr(sp->field, '+')) { aw_eligible = 0; break;
                    }
                    int fi = typed_field_index(fs.ts, sp->field);
                    /* Presence-doctrine counts need per-record marker
                       checks — bail to the general scan path. */
                    if (fi >= 0 && agg_count_inspects_presence(fs.ts->fields[fi].type)) {
                        aw_eligible = 0; break;
                    }
                    /* count(field) on this route = walked PRESENT entries
                       (scan parity: NULLs excluded), so the field must be
                       walked here too. */
                    if (fi < 0 || !agg_index_exact_field(fs.ts, &fs.ts->fields[fi]) ||
                        !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
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
            /* This walk accumulates leaf bytes as doubles only. Textual
               min/max must route to the general text-aware path. */
            int fi = typed_field_index(fs.ts, sp->field);
            AggNumSpec aw_ns;
            if (fi < 0 || !agg_num_spec_for(fs.ts, (size_t)fi, &aw_ns) ||
                !agg_index_exact(&aw_ns) ||
                agg_type_is_text(fs.ts->fields[fi].type) ||
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
                AggCell cells[MAX_AGG_SPECS];
                memset(cells, 0, sizeof cells);
                agg_cells_bind(cells, nspecs, ctx.shared_buffer_bytes, g_query_buffer_max_bytes);
                int done[MAX_AGG_SPECS] = {0}, err = 0;
                int n_idx = index_splits_for(sch.splits);
                for (int i = 0; i < nspecs && !err; i++) {
                    if (done[i] || !specs[i].field[0]) continue;
                    int fi = typed_field_index(fs.ts, specs[i].field);
                    if (fi < 0) continue;
                    AggNumSpec ns;
                    agg_num_spec_for(fs.ts, (size_t)fi, &ns);
                    int sibs[MAX_AGG_SPECS], nsibs = 0;
                    for (int j = i; j < nspecs; j++) {
                        if (done[j] || strcmp(specs[j].field, specs[i].field) != 0) continue;
                        sibs[nsibs++] = j; done[j] = 1;
                    }
                    AggWalkArg proto = { .db_root = db_root, .object = object,
                        .field = specs[i].field, .spec = &ns, .ks = crit_ks,
                        .dl = &dl, .budget = ctx.shared_buffer_bytes,
                        .limit = g_query_buffer_max_bytes };
                    AggNum num; memset(&num, 0, sizeof num);
                    int64_t cnt = 0;
                    int rc = agg_walk_field(&proto, n_idx, &num, &cnt);
                    if (rc < 0) err = errno ? errno : EILSEQ;
                    else if (rc > 0 || dl.timed_out) err = ETIMEDOUT;
                    for (int k = 0; k < nsibs && !err; k++) {
                        int si = sibs[k];
                        if (specs[si].fn == AGG_COUNT) {
                            /* count(field) = walked PRESENT entries within the
                               criteria set — scan parity (NULLs excluded). */
                            if (agg_cell_count(&cells[si], cnt)) err = errno;
                        } else if (agg_cell_from_num(&ns, &num, specs[si].fn, cnt, &cells[si])) {
                            err = errno;
                        }
                    }
                    agg_num_release(&num, ns.cls, &ctx.env);
                }
                for (int i = 0; i < nspecs && !err; i++)
                    if (specs[i].fn == AGG_COUNT && !specs[i].field[0] &&
                        agg_cell_count(&cells[i], match_count))
                        err = errno;   /* count(*) = |KeySet| */
                if (err) {
                    agg_cells_free(cells, nspecs);
                    keyset_free(crit_ks);
                    return agg_report_error(err);
                }
                agg_emit_scalar_cells(specs, nspecs, cells, csv_delim, want_total);
                agg_cells_free(cells, nspecs);
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
/* Deferred-lookup batch cap for the same fast path. Pending hashes are
 * flushed (cursors closed -> blocking kf fetch -> reopen) whenever they
 * cross this, so no bt rdlock is ever held across kfcache acquires
 * (2026-08-27 lock-inversion fix). */
#define VS_FLUSH_CAP 4096
#define VS_PEND_CAP   (VS_FLUSH_CAP + VS_RUN_HASH_CAP)
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
          (record fetch by hash16 + exact row offer). */
    int vs_need_lookup = 0;
    if (vs_eligible) {
        for (int i = 0; i < nspecs; i++) {
            AggSpec *sp = &specs[i];
            if (sp->fn == AGG_COUNT) {
                if (sp->field[0]) {
                    /* Even the group field can be NULL or an unset marker. */
                    vs_eligible = 0; break;
                }
                continue;
            }
            if (sp->fn != AGG_SUM && sp->fn != AGG_AVG &&
                sp->fn != AGG_MIN && sp->fn != AGG_MAX) {
                vs_eligible = 0; break;
            }
            if (!ctx.spec_tfs[i] || !ctx.spec_is_num[i] ||
                ctx.spec_nums[i].cls == AGC_DEC ||
                ctx.spec_tfs[i]->type == FT_VARCHAR ||
                strchr(sp->field, '+') ||
                !btree_idx_exists(db_root, object, sp->field, sch.splits)) {
                vs_eligible = 0; break;
            }
            vs_need_lookup = 1;
        }
    }
    SlotcaskDb *vs_sdb SDB_REG_REF = NULL;
    if (vs_eligible && vs_need_lookup) {
        SlotcaskSchemaInfo info = {
            .splits = sch.splits, .slot_size = sch.slot_size,
            .streams = sch.streams,
        };
        vs_sdb = slotcask_registry_get(db_root, object, &info);
        if (!vs_sdb) vs_eligible = 0;
    }
    /* Schema index per spec for the NULL-bit test in vs_lookup_cb. */
    int vs_spec_tidx[MAX_AGG_SPECS];
    if (vs_eligible) {
        for (int i = 0; i < nspecs; i++)
            vs_spec_tidx[i] = (ctx.spec_tfs[i] && fs.ts)
                ? (int)(ctx.spec_tfs[i] - fs.ts->fields) : -1;
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
        AggNum *vs_nums = (!vs_need_lookup || limit <= 0) ? NULL
            : calloc((size_t)limit * (size_t)nspecs, sizeof(AggNum));
        int64_t *vs_counts = (!vs_need_lookup || limit <= 0) ? NULL
            : calloc((size_t)limit * (size_t)nspecs, sizeof(int64_t));
        if (!iters || !cur_keys || !cur_klens || !has_cur || !cur_hash16 ||
            !staged || (vs_need_lookup && (!vs_nums || !vs_counts))) {
            free(iters); free(cur_keys); free(cur_klens);
            free(has_cur); free(cur_hash16); free(staged);
            free(vs_nums); free(vs_counts);
            goto vs_skip;
        }
        for (int s = 0; s < n_idx_g; s++) {
            char idx_path[PATH_MAX];
            build_idx_path(idx_path, sizeof(idx_path), db_root, object,
                            ctx.group_fields[0], s);
            /* Whole-index walk: must reach the NULL tier — group-by forms
               a NULL bucket from [0x01] keys (§2.4.5/Task 8). */
            iters[s] = btree_range_iter_open(
                idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, 0);
            if (!iters[s]) { has_cur[s] = 0; continue; }
            const char *v; size_t vl; const uint8_t *h;
            /* BTRI: cursor state keeps the stored (tiered) key — k-way
               comparisons and the flush-reopen bounds stay internally
               consistent. NULL-tier keys ([0x01], one per NULL row) join
               the merge like any key and commit as the tagged NULL group
               (§2.4.5/Task 8) — skipping them here would drop the NULL
               group from a limited aggregate the scan path reports. */
            while (btree_range_iter_next(iters[s], &v, &vl, &h) == 1) {
                if (vl == 0) continue;
                size_t cap = (vl > BT_MAX_VAL_LEN) ? BT_MAX_VAL_LEN : vl;
                memcpy(cur_keys[s], v, cap);
                cur_klens[s] = cap;
                memcpy(cur_hash16[s], h, 16);
                has_cur[s] = 1;
                break;
            }
        }

        /* Deferred-lookup pending set (see VS_FLUSH_CAP above). */
        VSPendPair *pend = calloc(VS_PEND_CAP, sizeof(VSPendPair));
        if (!pend) {
            free(iters); free(cur_keys); free(cur_klens);
            free(has_cur); free(cur_hash16); free(staged);
            goto vs_skip;
        }
        size_t pend_n = 0;
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
            if (min_s < 0) break;
                     /* all cursors drained */

            /* Snapshot the winning key before advancing any cursor (some
               cursors share its memory via cur_keys[]). */
            char min_key[BT_MAX_VAL_LEN + 1];
            size_t min_klen = cur_klens[min_s];
            memcpy(min_key, cur_keys[min_s], min_klen);

            /* Sum the run of equal keys across every shard that's
               currently sitting on this key. Collect hash16s for the
               record fetches if any spec needs lookup. */
            int64_t total_count = 0;
            int this_slot = emitted;      /* staged row for this run */
            int hash_count = 0;
            for (int s = 0; s < n_idx_g; s++) {
                while (has_cur[s] && cur_klens[s] == min_klen &&
                       memcmp(cur_keys[s], min_key, min_klen) == 0) {
                    if (vs_need_lookup) {
                        if (hash_count >= VS_RUN_HASH_CAP ||
                            pend_n >= VS_PEND_CAP) {
                            aborted = 1;
                            break;
                        }
                        memcpy(pend[pend_n].h, cur_hash16[s], BT_HASH_SIZE);
                        pend[pend_n].slot = this_slot;
                        pend_n++;
                        hash_count++;
                    }
                    total_count++;
                    const char *v; size_t vl; const uint8_t *h;
                    int advanced = 0;
                    while (btree_range_iter_next(iters[s], &v, &vl, &h) == 1) {
                        if (vl == 0) continue;
                        size_t cap = (vl > BT_MAX_VAL_LEN) ? BT_MAX_VAL_LEN : vl;
                        memcpy(cur_keys[s], v, cap);
                        cur_klens[s] = cap;
                        memcpy(cur_hash16[s], h, 16);
                        advanced = 1;
                        break;
                    }
                    if (!advanced) has_cur[s] = 0;
                }
                if (aborted) break;
            }
            if (aborted) break;

            VSStaged *cur = &staged[emitted];
            memcpy(cur->key, min_key, min_klen);
            cur->key[min_klen] = '\0';
            cur->klen = min_klen;
            cur->total_count = total_count;

            emitted++;

            /* Batched deferred fetch: close every cursor first so the
               blocking kf work runs with zero bt rdlocks held, then
               reopen at the surviving head snapshots (VS_FLUSH_CAP). */
            if (vs_need_lookup && pend_n >= VS_FLUSH_CAP) {
                VSLookupCtx lc = {
                    .staged   = staged,
                    .specs    = specs,
                    .spec_tfs = ctx.spec_tfs,
                    .spec_tidx = vs_spec_tidx,
                    .nspecs   = nspecs,
                    .ts       = fs.ts,
                    .nums     = vs_nums,
                    .counts   = vs_counts,
                    .spec_nums = ctx.spec_nums,
                    .env      = &ctx.env,
                };
                pthread_mutex_init(&lc.mu, NULL);
                vs_flush_pending(vs_sdb, &lc, pend, pend_n,
                                 iters, has_cur, cur_keys, cur_klens,
                                 cur_hash16, n_idx_g, db_root, object,
                                 ctx.group_fields[0], 1 /* reopen */);
                pthread_mutex_destroy(&lc.mu);
                pend_n = 0;
                if (lc.err && !ctx.agg_error) { ctx.agg_error = lc.err; aborted = 1; }
            }
        }

        for (int s = 0; s < n_idx_g; s++)
            if (iters[s]) { btree_range_iter_close(iters[s]); iters[s] = NULL; }

        if (!aborted && vs_need_lookup && pend_n > 0) {
            VSLookupCtx lc = {
                .staged   = staged,
                .specs    = specs,
                .spec_tfs = ctx.spec_tfs,
                .spec_tidx = vs_spec_tidx,
                .nspecs   = nspecs,
                .ts       = fs.ts,
                .nums     = vs_nums,
                .counts   = vs_counts,
                .spec_nums = ctx.spec_nums,
                .env      = &ctx.env,
            };
            pthread_mutex_init(&lc.mu, NULL);
            vs_flush_pending(vs_sdb, &lc, pend, pend_n,
                             iters, has_cur, cur_keys, cur_klens,
                             cur_hash16, n_idx_g, db_root, object,
                             ctx.group_fields[0], 0 /* no reopen */);
            pthread_mutex_destroy(&lc.mu);
            pend_n = 0;
            if (lc.err && !ctx.agg_error) { ctx.agg_error = lc.err; aborted = 1; }
        }
        free(pend);

        free(iters); free(cur_keys); free(cur_klens);
        free(has_cur); free(cur_hash16);

        if (aborted) {
            /* Low-cardinality data — let IGB handle it. ctx.ht hasn't
               been touched yet because we staged everything. Released
               exact states were never committed into ctx buckets. */
            for (size_t k = 0; k < (size_t)limit * (size_t)nspecs; k++)
                if (vs_nums) agg_num_release(&vs_nums[k], ctx.spec_nums[k % nspecs].cls, &ctx.env);
            free(vs_counts); free(vs_nums); free(staged);
            goto vs_skip;
        }

        /* Commit staging to ctx.ht — one bucket per emitted distinct key,
           with accums populated for each spec. Labels strip the BTRI
           tier byte (cursor state stays tiered; the bucket key is the
           tagged component, byte-identical to the per-record scan path).
           NULL-tier keys form the tagged NULL group (§2.4.5/Task 8). */
        for (int i = 0; i < emitted; i++) {
            char label[BT_MAX_VAL_LEN + 1];
            size_t klen = staged[i].klen;
            const char *ksrc = staged[i].key;
            uint8_t knull[1] = { 0 };
            if (klen > 0 && (uint8_t)ksrc[0] == 0x00) { ksrc++; klen--; }
            else if (klen > 0 && (uint8_t)ksrc[0] == 0x01) {
                knull[0] = 1; klen = 0;
            }
            memcpy(label, ksrc, klen);
            label[klen] = '\0';
            char *kvp[1] = { label };
            AggBucket *b = agg_find_or_create(&ctx, kvp, 1, NULL, 0, knull);
            if (!b) {
                /* Budget/key failure (already flagged on ctx): release every
                   staged num not yet committed — ownership moved only for
                   buckets committed before this one — and fail the whole
                   aggregate. Never a silent partial success. */
                for (int ri = i; ri < emitted; ri++)
                    for (int rj = 0; rj < nspecs; rj++) {
                        if (specs[rj].fn == AGG_COUNT) continue;
                        agg_num_release(&vs_nums[(size_t)ri * (size_t)nspecs + (size_t)rj],
                                        ctx.spec_nums[rj].cls, &ctx.env);
                    }
                free(vs_counts); free(vs_nums); free(staged);
                goto igb_skip;
            }
            for (int j = 0; j < nspecs; j++) {
                if (specs[j].fn == AGG_COUNT) {
                    b->accums[j].count = staged[i].total_count;
                    continue;
                }
                size_t k = (size_t)i * (size_t)nspecs + (size_t)j;
                b->accums[j].num   = vs_nums[k];   /* ownership moves */
                b->accums[j].count = vs_counts[k];
            }
        }
        free(vs_counts);
        free(vs_nums);
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
                if (sp->field[0]) {
                    /* Numeric fields can be NULL too: grouping-key counts
                       cannot stand in for count(field)'s present counts. */
                    igb_eligible = 0; break;
                }
                continue;
            }
            /* sum/avg/min/max: need an indexed numeric agg field. The
               pass-2 walk accumulates leaf bytes as doubles only, so
               textual min/max must route to the general scan path. */
            if (!ctx.spec_tfs[i] || !ctx.spec_is_num[i] ||
                !agg_index_exact(&ctx.spec_nums[i]) ||
                agg_type_is_text(ctx.spec_tfs[i]->type) ||
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
                    int dlen = agg_index_label(&ctx, 0, bm_vals[i], bm_vlens[i],
                                               display, sizeof(display));
                    if (dlen < 0) { ctx.agg_error = errno ? errno : EILSEQ; goto igb_skip; }
                    char *kvp[1] = { display };
                    AggBucket *b = agg_find_or_create(&ctx, kvp, 1, NULL, 0, NULL);
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
                        { can_slot = 0; break;
                    }
                }
                if (n_crit == 0) can_slot = 0;
            }

            if (can_slot) {
                uint8_t  cvals[MAX_INTERSECT_LEAVES][1024];
                size_t   cvlens[MAX_INTERSECT_LEAVES];
                for (int ci = 0; ci < n_crit; ci++) {
                    SearchCriterion *sc = igb_crit_fp.source_leaves[ci];
                    const TypedField *ctf = resolve_idx_field(fs.ts, sc->field);
                    if (!ctf) { can_slot = 0; break;
                    }
                    if(encode_criterion_value_checked(fs.ts,ctf,search_criterion_value(sc),strlen(search_criterion_value(sc)),cvals[ci],sizeof(cvals[ci]),&cvlens[ci])<0){can_slot=0;break;}
                    if (cvlens[ci] == 0) { can_slot = 0; break;
                    }
                }
                if (can_slot) {
                    unsigned long counts[256] = {0};
                    for (int s = 0; s < sch.splits && !dl.timed_out; s++) {
                        if (query_deadline_tick(&dl, &bm_dl)) break;
                        char bp[1024];
                        bm_build_path(bp, sizeof(bp), db_root, object,
                                      ctx.group_fields[0], s);
                        /* Group bitmap + criteria bitmaps are held together:
                           canonical lock order (see bm_open_many_ro). */
                        BitmapShard *held[MAX_INTERSECT_LEAVES + 1];
                        char cps[MAX_INTERSECT_LEAVES][1024];
                        const char *hpaths[MAX_INTERSECT_LEAVES + 1];
                        hpaths[0] = bp;
                        for (int ci = 0; ci < n_crit; ci++) {
                            bm_build_path(cps[ci], sizeof(cps[ci]), db_root, object,
                                          igb_crit_fp.source_leaves[ci]->field, s);
                            hpaths[1 + ci] = cps[ci];
                        }
                        if (bm_open_many_ro(hpaths, 1 + n_crit, held) != 0) continue;
                        BitmapShard *gb_bm = held[0];
                        BitmapShard **c_bms = &held[1];

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

                        bm_close_many(held, 1 + n_crit);
                    }

                    if (!dl.timed_out) {
                        for (int i = 0; i < bm_n; i++) {
                            if (counts[i] == 0) continue;
                            char display[512];
                            if (agg_index_label(&ctx, 0, bm_vals[i], bm_vlens[i],
                                                display, sizeof(display)) < 0) {
                                ctx.agg_error = errno ? errno : EILSEQ;
                                goto igb_skip;
                            }
                            char *kvp[1] = { display };
                            AggBucket *b = agg_find_or_create(&ctx, kvp, 1, NULL, 0, NULL);
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
                SlotcaskDb *bm_sdb SDB_REG_REF = slotcask_registry_get(db_root, object,
                                                             &bm_info);
                if (!bm_sdb) { hbk_free(&hbk_bm); goto igb_skip; }

                int bm_aborted = 0;
                for (int v = 0; v < bm_n_h && !bm_aborted; v++) {
                    /* Decode display string for bucket key (D2 exact label). */
                    char display_h[512];
                    if (agg_index_label(&ctx, 0, bm_vals_h[v], bm_vlens_h[v],
                                        display_h, sizeof(display_h)) < 0) {
                        ctx.agg_error = errno ? errno : EILSEQ;
                        goto igb_skip;
                    }
                    char *kvp_h[1] = { display_h };
                    AggBucket *bkt_h = agg_find_or_create(&ctx, kvp_h, 1,
                                                           NULL, 0, NULL);
                    if (!bkt_h) { bm_aborted = 1; break;
                    }

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
                    sw[s].actx       = &ctx;
                    sw[s].gi         = g + 1;
                }
                parallel_for_io(sec_map_build_worker, sw, n_idx_s,
                                sizeof(SecMapBuildWorker));
                for (int s = 0; s < n_idx_s; s++)
                    if (sw[s].err && !ctx.agg_error) ctx.agg_error = sw[s].err;
                /* Serial merge: walk each per-worker local_map and insert
                   into the shared sec_maps[g].  hsm_insert on a single map
                   is safe here because the main thread is the only writer.
                   HashStrEntry stores value as (off, len) into local_map.arena;
                   dereference via lm->arena + he->off. */
                for (int s = 0; s < n_idx_s && !sec_aborted; s++) {
                    if (sw[s].aborted) { sec_aborted = 1; break;
                    }
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
                          partition) queues by `agg_bucket_hash(b) % npart`.
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
                                if (!scatter[w]) { aborted = 1; budget_exceeded = 1; break;
                    }
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
                                        uint32_t h = agg_bucket_hash(&ctx, b) &
                                                     (uint32_t)ctx.ht_mask;
                                        b->next = ctx.ht[h];
                                        ctx.ht[h] = b;
                                        ctx.total_buckets++;
                                        b = next;
                                    }
                                }
                            }
                            agg_arena_transfer(&ctx.arena, &pc->arena);
                            /* Splice done: these buckets now belong to the
                               main ctx and their exact states are released by
                               agg_free's release walk over ctx.ht. Drop the
                               partition's own table so the later
                               agg_ctx_free_local release walk cannot traverse
                               the spliced (now shared) chains — walking them
                               here would zero main-ctx AggNum state. */
                            free(pc->ht);
                            pc->ht = NULL;
                            pc->ht_cap = pc->ht_mask = 0;
                            pc->total_buckets = 0;
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
            int prev_null = 0;
            struct AggBucket *prev_bkt = NULL;
            for (int s = 0; s < n_idx_g && !aborted; s++) {
                char idx_path[PATH_MAX];
                build_idx_path(idx_path, sizeof(idx_path), db_root,
                               object, ctx.group_fields[0], s);
                /* Whole-index walk: must reach the NULL tier — group-by
                   forms a NULL bucket from [0x01] keys (§2.4.5/Task 8). */
                BtRangeIter *it = btree_range_iter_open(
                    idx_path, "", 0, 0, "\xff\xff\xff\xff", 4, 0, 0);
                if (!it) continue;
                const char *val; size_t vlen; const uint8_t *hash16;
                prev_enc_len = 0; prev_bkt = NULL;
                while (btree_range_iter_next(it, &val, &vlen, &hash16) == 1) {
                    if (query_deadline_tick(&dl, &dl_counter)) { aborted = 1; break;
                    }
                    /* BTRI: the primary component comes from the tiered
                       leaf key — tier-1 forms the NULL bucket (same rule
                       as igb_pass1_worker). */
                    if (vlen == 0) continue;
                    int prim_null = ((uint8_t)val[0] != 0x00);
                    const uint8_t *prim = (const uint8_t *)val + 1;
                    size_t prim_len = vlen - 1;
                    /* Criteria filter: skip leaf entries whose record didn't
                       match the criteria-derived KeySet. crit_ks NULL means
                       no criteria, so every entry passes. */
                    if (crit_ks && !keyset_contains(crit_ks, hash16)) continue;
                    struct AggBucket *bkt;
                    /* Tier-aware run compare — see igb_pass1_worker: a
                       NULL key and a zero-valued key share zeroed payload
                       bytes and must not fold into one bucket. */
                    if (n_sec == 0 && prev_bkt && (int)prim_null == prev_null &&
                        prim_len == prev_enc_len &&
                        memcmp(prim, prev_enc, prim_len) == 0) {
                        bkt = prev_bkt;
                    } else {
                        char gbufs[MAX_FIELDS][512];
                        char *gvals[MAX_FIELDS];
                        uint8_t gnulls[MAX_FIELDS] = {0};
                        uint8_t raw_key[AGG_INT_KEY_CAP];
                        int raw_key_len = 0;

                        gnulls[0] = (uint8_t)prim_null;
                        if (prim_null) gbufs[0][0] = '\0';
                        /* Check if we can use raw integer key */
                        int use_raw = ctx.use_int_keys && n_sec == 0 && prim_len <= AGG_INT_KEY_CAP;
                        if (use_raw) {
                            int w0 = typed_field_int_width(gtf->type);
                            if (w0 <= 0 || 1 + w0 > AGG_INT_KEY_CAP) use_raw = 0;
                            else {
                                raw_key[0] = gnulls[0];
                                if (prim_null) memset(raw_key + 1, 0, (size_t)w0);
                                else memcpy(raw_key + 1, prim, (size_t)prim_len);
                                raw_key_len = 1 + w0;
                            }
                        }
                        if (!prim_null) {
                            /* Same empty-varchar init as igb_pass1_worker:
                               decode_idx_to_buf returns 0 for a zero-length
                               payload without writing the buffer. */
                            gbufs[0][0] = '\0';
                            int n = agg_index_label(&ctx, 0, prim, prim_len,
                                                    gbufs[0], sizeof(gbufs[0]));
                            if (n < 0) { ctx.agg_error = errno ? errno : EILSEQ; aborted = 1; break; }
                            gvals[0] = gbufs[0];
                        } else {
                            gvals[0] = gbufs[0];
                        }
                        /* Compose composite key for ngroups > 1 by looking up
                           each secondary field's tagged component via its
                           hash16 map. A miss means the record doesn't have
                           that field indexed (skip-zero rule) — skip the
                           entry to match the per-record path's "missing
                           field" semantics. */
                        int multi_skip = 0;
                        for (int g = 0; g < n_sec; g++) {
                            const char *sval; size_t slen;
                            if (!hsm_get(&sec_maps[g], hash16, &sval, &slen)) {
                                multi_skip = 1;
                                break;
                            }
                            int snull = 0;
                            const char *stext = ""; size_t stext_len = 0;
                            hsm_parse_tagged(sval, slen, &snull, &stext, &stext_len);
                            gnulls[g + 1] = (uint8_t)snull;
                            size_t cl = stext_len < sizeof(gbufs[g + 1]) - 1
                                         ? stext_len : sizeof(gbufs[g + 1]) - 1;
                            memcpy(gbufs[g + 1], stext, cl);
                            gbufs[g + 1][cl] = '\0';
                            gvals[g + 1] = gbufs[g + 1];
                        }
                        if (multi_skip) continue;
                        bkt = (struct AggBucket *)agg_find_or_create(
                                &ctx, gvals, ctx.ngroups, raw_key, raw_key_len,
                                gnulls);
                        if (!bkt) { aborted = 1; break;
                    }
                        if (n_sec == 0 && prim_len <= sizeof(prev_enc)) {
                            memcpy(prev_enc, prim, prim_len);
                            prev_enc_len = prim_len;
                            prev_null = (int)prim_null;
                            prev_bkt = bkt;
                        } else {
                            prev_bkt = NULL; prev_enc_len = 0; prev_null = 0;
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
                struct { uint8_t hash[16]; uint16_t klen; uint8_t key[560]; } ring[PFB];
                for (int s = 0; s < n_idx_a && !aborted; s++) {
                    char idx_path[PATH_MAX];
                    build_idx_path(idx_path, sizeof(idx_path), db_root,
                                   object, fld, s);
                    BtRangeIter *it = btree_range_iter_open(
                        idx_path, "", 0, 0, AGG_VALUE_TIER_CEIL, 1, 1, 0);
                    if (!it) continue;
                    const char *val; size_t vlen; const uint8_t *hash16;
                    int filled = 0;
                    while (1) {
                        /* Refill the ring (copies hash16 + key — iter pointers
                           are invalidated on the next iter_next call). */
                        while (filled < PFB) {
                            if (btree_range_iter_next(it, &val, &vlen, &hash16) != 1) break;
                            if (query_deadline_tick(&dl, &dl_counter)) { aborted = 1; break;
                    }
                            if (vlen == 0 || vlen > sizeof ring[0].key) continue;
                            memcpy(ring[filled].hash, hash16, 16);
                            memcpy(ring[filled].key, val, vlen);
                            ring[filled].klen = (uint16_t)vlen;
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
                            for (int k = 0; k < nsibs; k++) {
                                AggAccum *a = &ab->accums[sibs[k]];
                                int rr = agg_num_offer_key(&ctx.spec_nums[sibs[k]],
                                                           ring[r].key, ring[r].klen,
                                                           &a->num, &ctx.env);
                                if (rr < 0) { ctx.agg_error = errno ? errno : EILSEQ; aborted = 1; break; }
                                if (rr == 1) a->count++;
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
    if (ctx.agg_error) {
        int e = ctx.agg_error;
        agg_free(&ctx);
        return agg_report_error(e);
    }
    if (ctx.budget_exceeded) {
        /* A fast path hit the per-query buffer cap: the whole aggregate is
           one explicit error — never a partial success body (Task 6). */
        agg_free(&ctx);
        LOG_WARN(LOG_SUB_QUERY, "cmd_aggregate_do: query buffer cap exceeded (fast path)");
        return agg_report_error(ENOBUFS);
    }
    if (igb_done) {
        /* fast path populated ctx — skip agg_run_plan, fall through to
           the shared having / sort / limit / emit pipeline below. */
    } else if (agg_run_plan(&ctx, tree, db_root, object, &sch) != 0) {
        if (dl.timed_out) {
            LOG_WARN(LOG_SUB_QUERY, "cmd_aggregate_do: query deadline exceeded");
            OUT("{\"error\":\"query_timeout\"}\n");
        } else if (ctx.agg_error) {
            agg_report_error(ctx.agg_error);
        } else if(ctx.predicate_error){
            OUT("{\"error\":\"invalid typed record\"}\n");
        } else if (ctx.budget_exceeded) {
            LOG_WARN(LOG_SUB_QUERY, "cmd_aggregate_do: query buffer cap exceeded");
            OUT(QUERY_BUFFER_ERR);
        }
        agg_free(&ctx);
        return -1;
    }

    if (ctx.agg_error) { int e = ctx.agg_error; agg_free(&ctx); return agg_report_error(e); }
    if(ctx.predicate_error){int error=ctx.predicate_error;agg_free(&ctx);OUT("{\"error\":\"invalid typed record\"}\n");errno=error;return -1;}
    /* Collect buckets */
    int nbuckets = 0;
    AggBucket **buckets = agg_collect(&ctx, &nbuckets);

    /* Apply having filter — tree path takes priority (supports OR/nested);
       flat-array JSON path kept for having_json callers. `having`/`nhaving`
       stay declared at this scope (not narrowed into a branch) because two
       later exit paths in this function call free_criteria(having, nhaving)
       unconditionally — narrowing their scope would break those. */
    SearchCriterion *having = NULL;
    int nhaving = 0;
    if (having_tree) {
        int dst = 0;
        for (int i = 0; i < nbuckets; i++) {
            int m = agg_having_match_tree(&ctx, buckets[i], having_tree);
            if (m < 0) { int e = errno; free(buckets); agg_free(&ctx); return agg_report_error(e); }
            if (m) buckets[dst++] = buckets[i];
        }
        nbuckets = dst;
    } else {
        if (having_json && having_json[0] &&
            parse_criteria_json(having_json, &having, &nhaving) != 0) {
            OUT("{\"error\":\"invalid having condition\"}\n");
            free(buckets);
            return -1;
        }

        if (nhaving > 0) {
            int dst = 0;
            for (int i = 0; i < nbuckets; i++) {
                int m = agg_having_match(&ctx, buckets[i], having, nhaving);
                if (m < 0) {
                    int e = errno;
                    free(buckets); free_criteria(having, nhaving); agg_free(&ctx);
                    return agg_report_error(e);
                }
                if (m) buckets[dst++] = buckets[i];
            }
            nbuckets = dst;
        }
    }

    /* Sort */
    if (order_by && order_by[0]) {
        /* Validate order_by: must be a group_by field or an aggregate alias */
        int valid_order = 0;
        for (int i = 0; i < ctx.ngroups; i++)
            if (strcmp(ctx.group_fields[i], order_by) == 0) { valid_order = 1; break;
                    }
        if (!valid_order)
            for (int i = 0; i < nspecs; i++)
                if (strcmp(specs[i].alias, order_by) == 0) { valid_order = 1; break;
                    }
        if (!valid_order) {
            OUT("{\"error\":\"order_by '%s' is not a group_by field or aggregate alias\"}\n",
                order_by);
            free(buckets);
            return -1;
        }
        if (agg_sort_buckets(&ctx, buckets, nbuckets, order_by, order_desc)) {
            int e = errno;
            free(buckets); free_criteria(having, nhaving); agg_free(&ctx);
            return agg_report_error(e);
        }
    }

    /* Apply limit — capture pre-limit group count for total. */
    if (limit <= 0) limit = g_global_limit;
    int agg_total_groups = nbuckets;   /* distinct groups BEFORE limit truncation */
    if (nbuckets > limit) nbuckets = limit;

    size_t ncell = (size_t)nbuckets * (size_t)nspecs;
    size_t nlabel = (size_t)nbuckets * (size_t)ctx.ngroups;
    AggCell *cells = agg_cells_alloc(ncell, ctx.shared_buffer_bytes, g_query_buffer_max_bytes);
    int output_error = cells ? 0 : (errno ? errno : ENOMEM);
    AggCell *labels = output_error ? NULL : agg_cells_alloc(nlabel,
                         ctx.shared_buffer_bytes, g_query_buffer_max_bytes);
    if (!output_error && !labels) output_error = errno ? errno : ENOMEM;
    for (int bi = 0; bi < nbuckets && !output_error; bi++) {
        AggBucket *bucket = buckets[bi];
        for (int g = 0; g < ctx.ngroups && !output_error; g++) {
            int present = !(bucket->group_nulls && bucket->group_nulls[g]);
            const char *text = bucket->group_vals[g];
            if (agg_cell_text(&labels[(size_t)bi * ctx.ngroups + g],
                              text, present ? strlen(text) : 0, present))
                output_error = errno ? errno : ENOMEM;
        }
        for (int i = 0; i < nspecs && !output_error; i++) {
            AggAccum *acc = &bucket->accums[i];
            AggCell *cell = &cells[(size_t)bi * nspecs + i];
            if (specs[i].fn == AGG_COUNT) {
                if (agg_cell_count(cell, acc->count)) output_error = errno;
            } else if (ctx.spec_is_num[i]) {
                if (agg_cell_from_num(&ctx.spec_nums[i], &acc->num, specs[i].fn,
                                      acc->count, cell)) output_error = errno;
            } else if (ctx.spec_tfs[i] && agg_type_is_text(ctx.spec_tfs[i]->type) &&
                       (specs[i].fn == AGG_MIN || specs[i].fn == AGG_MAX)) {
                const char *text = specs[i].fn == AGG_MIN ? acc->text_min : acc->text_max;
                size_t len = specs[i].fn == AGG_MIN ? acc->text_min_len : acc->text_max_len;
                if (agg_cell_text(cell, text, acc->text_present ? len : 0, acc->text_present))
                    output_error = errno;
            }
        }
    }
    if (output_error) {
        agg_cells_dispose(labels, nlabel); agg_cells_dispose(cells, ncell);
        free(buckets); free_criteria(having, nhaving); agg_free(&ctx);
        return agg_report_error(output_error);
    }

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
            for (int g = 0; g < ctx.ngroups; g++) {
                if (g > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                agg_cell_emit_csv(&labels[(size_t)bi * ctx.ngroups + g], csv_delim);
            }
            for (int i = 0; i < nspecs; i++) {
                if (ctx.ngroups > 0 || i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                agg_cell_emit_csv(&cells[(size_t)bi * nspecs + i], csv_delim);
            }
            OUT("\n");
        }

        agg_cells_dispose(labels, nlabel);
        agg_cells_dispose(cells, ncell);
        free(buckets);
        free_criteria(having, nhaving);
        agg_free(&ctx);
        return 0;
    }

    if (ctx.ngroups == 0 && nbuckets == 1) {
        /* No group_by: single object */
        OUT(want_total ? "{\"rows\":{" : "{");
        for (int i = 0; i < nspecs; i++) {
            if (i > 0) OUT(",");
            OUT("\"%s\":", specs[i].alias);
            agg_cell_emit_json(&cells[i]);
        }
        OUT(want_total ? "},\"total\":1}\n" : "}\n");
    } else {
        /* Group_by: array of objects */
        OUT(want_total ? "{\"rows\":[" : "[");
        for (int bi = 0; bi < nbuckets; bi++) {
            if (bi > 0) OUT(",");
            OUT("{");
            int first = 1;
            /* Group fields — a NULL component renders JSON null (Task 8);
               "" and the literal "null" stay quoted strings. */
            for (int g = 0; g < ctx.ngroups; g++) {
                if (!first) OUT(",");
                OUT("\"%s\":", ctx.group_fields[g]);
                agg_cell_emit_json(&labels[(size_t)bi * ctx.ngroups + g]);
                first = 0;
            }
            /* Aggregate values */
            for (int i = 0; i < nspecs; i++) {
                if (!first) OUT(",");
                OUT("\"%s\":", specs[i].alias);
                agg_cell_emit_json(&cells[(size_t)bi * nspecs + i]);
                first = 0;
            }
            OUT("}");
        }
        if (want_total) OUT("],\"total\":%d}\n", agg_total_groups);
        else OUT("]\n");
    }
    agg_cells_dispose(labels, nlabel);
    agg_cells_dispose(cells, ncell);

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

    CriteriaNode *having_tree = NULL;
    if (having_json && having_json[0]) {
        const char *herr = NULL;
        having_tree = parse_criteria_tree(having_json, &herr);
        if (herr) {
            OUT("{\"error\":\"bad having: %s\"}\n", herr);
            free_criteria_tree(having_tree);
            free_criteria_tree(tree);
            free(specs);
            return -1;
        }
    }

    int r = cmd_aggregate_do(db_root, object, tree, specs, nspecs,
                            group_by_json, NULL,
                            having_tree,
                            order_by, order_desc, limit,
                            format, delimiter, want_total, NULL, NULL);
    free_criteria_tree(having_tree);
    free_criteria_tree(tree);
    free(specs);
    return r;
}

/* Convert a CSV field list ("a,b,c") into a JSON string array
   ("["a","b","c"]") into out[0..out_sz). Every byte offset is
   bounds-checked *before* the write that would produce it — the
   original inline version instead wrote its closing quote/comma/bracket
   unconditionally after a bounds-checked copy loop, which let a long
   enough group_by_csv drive an unbounded write loop past the end of a
   fixed 4096-byte stack buffer (CID 1696446). Fields beyond what fits
   are silently dropped rather than truncated mid-name, matching the
   sibling having_buf conversion just below this function's caller. */
void group_by_csv_to_json(const char *csv, char *out, size_t out_sz) {
    if (out_sz == 0) return;
    if (out_sz < 3) { out[0] = '\0'; return; }
    out[0] = '[';
    int pos = 1;
    int first = 1;
    if (csv && csv[0]) {
        const char *p = csv;
        while (*p) {
            while (*p == ' ' || *p == '\t') p++;
            if (!*p) break;
            const char *start = p;
            while (*p && *p != ',') p++;
            int flen = (int)(p - start);
            int remain = (int)out_sz - pos;
            if (remain <= 0) break;
            int n = snprintf(out + pos, (size_t)remain, "%s\"%.*s\"",
                             first ? "" : ",", flen, start);
            if (n < 0 || n >= remain) break;
                      /* would truncate — stop here */
            pos += n;
            first = 0;
            if (*p == ',') p++;
        }
    }
    if ((size_t)pos + 2 <= out_sz) {
        out[pos++] = ']';
        out[pos] = '\0';
    } else {
        out[out_sz - 2] = ']';
        out[out_sz - 1] = '\0';
    }
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
        LOG_ERROR(LOG_SUB_QUERY, "cmd_aggregate_tree: calloc %d AggSpec failed", naggs);
        OUT("{\"error\":\"out of memory\"}\n");
        return -1;
    }
    for (int i = 0; i < naggs; i++) {
        if (strcmp(aggs[i].fn, "count") == 0) specs[i].fn = AGG_COUNT;
        else if (strcmp(aggs[i].fn, "sum") == 0) specs[i].fn = AGG_SUM;
        else if (strcmp(aggs[i].fn, "avg") == 0) specs[i].fn = AGG_AVG;
        else if (strcmp(aggs[i].fn, "min") == 0) specs[i].fn = AGG_MIN;
        else if (strcmp(aggs[i].fn, "max") == 0) specs[i].fn = AGG_MAX;
        else {
            OUT("{\"error\":\"unknown aggregate function '%s'; use count, sum, avg, min, or max\"}\n",
                aggs[i].fn);
            free(specs);
            return -1;
        }
        strncpy(specs[i].field, aggs[i].field, sizeof(specs[i].field) - 1);
        if (aggs[i].field[0])
            snprintf(specs[i].alias, sizeof(specs[i].alias), "%s_%s", aggs[i].fn, aggs[i].field);
        else
            strncpy(specs[i].alias, aggs[i].fn, sizeof(specs[i].alias) - 1);
    }

    /* Convert group_by_csv to group_by_json (JSON array) */
    char group_by_buf[4096];
    group_by_csv_to_json(group_by_csv, group_by_buf, sizeof(group_by_buf));

    int r = cmd_aggregate_do(db_root, object, criteria_tree, specs, naggs,
                            group_by_buf,
                            NULL,                  /* having_json — not used */
                            having_tree,           /* passed directly */
                            order_by, order_desc, limit,
                            format, delimiter, want_total, NULL, NULL);
    free(specs);
    return r;
}
#ifdef TEST_BUILD
/* Aggregate over a caller-supplied (e.g. private packed) schema; the rest of
   the pipeline is byte-for-byte the production path. */
int cmd_aggregate_with_schema_for_test(const char *db_root, const char *object,
        const Schema *schema, const FieldSchema *fields,
        const char *criteria_json, const char *group_by_json,
        const char *aggregates_json, const char *having_json,
        const char *order_by, int order_desc, int limit,
        const char *format, const char *delimiter, int want_total) {
    if (!schema || !fields || !fields->ts) { errno = EINVAL; return -1; }
    AggSpec *specs = NULL;
    int nspecs = parse_agg_specs(aggregates_json, &specs);
    if (nspecs <= 0) { free(specs); errno = EINVAL; return -1; }
    const char *perr = NULL;
    CriteriaNode *tree = criteria_json && criteria_json[0]
        ? parse_criteria_tree(criteria_json, &perr) : NULL;
    CriteriaNode *having = having_json && having_json[0]
        ? parse_criteria_tree(having_json, &perr) : NULL;
    int rc = perr ? -1 : cmd_aggregate_do(db_root, object, tree, specs, nspecs,
                         group_by_json, NULL, having, order_by, order_desc,
                         limit, format, delimiter, want_total, schema, fields);
    free_criteria_tree(having);
    free_criteria_tree(tree);
    free(specs);
    return rc;
}
#endif
