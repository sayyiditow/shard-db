#include "types.h"
#include "slotcask.h"
#include "simd.h"
#include "bitmap.h"
#include "trigram.h"
#include "io_direct.h"
#include "query_internal.h"
#include <math.h>
#include <dirent.h>

/* ========== ADVANCED SEARCH (multi-criteria, all operators) ========== */

/* AdvSearchCtx is defined in query_internal.h */

/* Vlen-aware match. Identical semantics to match_criterion but
   callers that already know the value's byte length (every cb on
   the btree-scan hot path: idx_count_cb, collect_hash_cb,
   stream_keyset_cb, agg's per-leaf path) can skip a per-entry
   strlen scan on the haystack. Concretely OP_ENDS_WITH used to
   compute strlen(val_str) per leaf entry; at 25M emails averaging
   ~25 bytes that is ~625 MB of redundant memory scans just to
   find string lengths the cb already has in hand. The win is
   small on hot cache (~10% vs the libc strstr/strcmp paths which
   are aggressively SIMD-optimised) but real and durable.

   Implementation notes:
   - Specialised for the four ops where vlen genuinely changes
     the per-entry cost shape: ENDS_WITH, STARTS_WITH, CONTAINS,
     NOT_CONTAINS. All four become a single memcmp / memmem call.
   - Other ops still need C-string semantics — numeric coercion,
     regex on NUL-terminated buffers, equality strcmp — so we
     fall back to match_criterion. Callers on the cb hot path
     already do tmp[vlen] = '\\0' before invoking, so val is
     guaranteed NUL-terminated for the fallback.
   - First-pass attempt of this helper appeared to regress ends
     queries by ~10× in one bench run, leading to a revert.
     Subsequent benches showed that was cold-page-cache jitter
     between runs, not a real regression — re-applied here. */
int match_criterion_vlen(const char *val, size_t vlen,
                         const SearchCriterion *c) {
    if (!val && c->op != OP_NOT_EXISTS && c->op != OP_EXISTS) return 0;
    switch (c->op) {
    case OP_ENDS_WITH: {
        size_t cl = strlen(c->value);
        if (vlen < cl) return 0;
        return memcmp(val + vlen - cl, c->value, cl) == 0;
    }
    case OP_STARTS_WITH: {
        size_t cl = strlen(c->value);
        if (vlen < cl) return 0;
        return memcmp(val, c->value, cl) == 0;
    }
    case OP_CONTAINS: {
        size_t cl = strlen(c->value);
        if (cl == 0) return 1;
        if (vlen < cl) return 0;
        return memmem(val, vlen, c->value, cl) != NULL;
    }
    case OP_NOT_CONTAINS: {
        size_t cl = strlen(c->value);
        if (cl == 0) return 0;
        if (vlen < cl) return 1;
        return memmem(val, vlen, c->value, cl) == NULL;
    }
    default:
        return match_criterion(val, c);
    }
}

int match_criterion(const char *val_str, const SearchCriterion *c) {
    if (!val_str && c->op != OP_NOT_EXISTS && c->op != OP_EXISTS) return 0;

    switch (c->op) {
        case OP_EXISTS:
            return val_str != NULL;
        case OP_NOT_EXISTS:
            return val_str == NULL;
        case OP_EQUAL:
            return strcmp(val_str, c->value) == 0;
        case OP_NOT_EQUAL:
            return strcmp(val_str, c->value) != 0;
        case OP_LESS:
            if (is_number(val_str) && is_number(c->value))
                return atof(val_str) < atof(c->value);
            return strcmp(val_str, c->value) < 0;
        case OP_GREATER:
            if (is_number(val_str) && is_number(c->value))
                return atof(val_str) > atof(c->value);
            return strcmp(val_str, c->value) > 0;
        case OP_LESS_EQ:
            if (is_number(val_str) && is_number(c->value))
                return atof(val_str) <= atof(c->value);
            return strcmp(val_str, c->value) <= 0;
        case OP_GREATER_EQ:
            if (is_number(val_str) && is_number(c->value))
                return atof(val_str) >= atof(c->value);
            return strcmp(val_str, c->value) >= 0;
        case OP_BETWEEN:
            if (is_number(val_str) && is_number(c->value) && is_number(c->value2)) {
                double v = atof(val_str);
                int lo = c->min_exclusive ? (v > atof(c->value)) : (v >= atof(c->value));
                int hi = c->max_exclusive ? (v < atof(c->value2)) : (v <= atof(c->value2));
                return lo && hi;
            }
            {
                int rl = strcmp(val_str, c->value);
                int rh = strcmp(val_str, c->value2);
                int lo = c->min_exclusive ? (rl >  0) : (rl >= 0);
                int hi = c->max_exclusive ? (rh <  0) : (rh <= 0);
                return lo && hi;
            }
        case OP_LIKE: {
            /* SQL-style LIKE with `%` wildcard (no `_`). Case-sensitive —
               use `ilike` for case-insensitive. */
            const char *pat = c->value;
            if (pat[0] == '%' && pat[strlen(pat)-1] == '%') {
                char sub[MAX_LINE];
                snprintf(sub, sizeof(sub), "%s", pat + 1);
                sub[strlen(sub)-1] = '\0';
                return strstr(val_str, sub) != NULL;
            }
            if (pat[0] == '%') return strstr(val_str, pat + 1) != NULL;
            if (pat[strlen(pat)-1] == '%') {
                return strncmp(val_str, pat, strlen(pat)-1) == 0;
            }
            return strcmp(val_str, pat) == 0;
        }
        case OP_NOT_LIKE: {
            SearchCriterion tmp = *c;
            tmp.op = OP_LIKE;
            return !match_criterion(val_str, &tmp);
        }
        case OP_CONTAINS:
            return strstr(val_str, c->value) != NULL;
        case OP_NOT_CONTAINS:
            return strstr(val_str, c->value) == NULL;
        case OP_STARTS_WITH:
            return strncmp(val_str, c->value, strlen(c->value)) == 0;
        case OP_ENDS_WITH: {
            size_t vl = strlen(val_str), cl = strlen(c->value);
            if (vl < cl) return 0;
            return strcmp(val_str + vl - cl, c->value) == 0;
        }
        /* Case-insensitive variants (ASCII tolower). Indexed paths fall
           through to the default full-scan branch in btree_dispatch and
           filter per entry in the callbacks below. */
        case OP_ILIKE: {
            const char *pat = c->value;
            if (pat[0] == '%' && pat[strlen(pat)-1] == '%') {
                char sub[MAX_LINE];
                snprintf(sub, sizeof(sub), "%s", pat + 1);
                sub[strlen(sub)-1] = '\0';
                return strcasestr(val_str, sub) != NULL;
            }
            if (pat[0] == '%') return strcasestr(val_str, pat + 1) != NULL;
            if (pat[strlen(pat)-1] == '%') {
                return strncasecmp(val_str, pat, strlen(pat)-1) == 0;
            }
            return strcasecmp(val_str, pat) == 0;
        }
        case OP_INOT_LIKE: {
            SearchCriterion tmp = *c;
            tmp.op = OP_ILIKE;
            return !match_criterion(val_str, &tmp);
        }
        case OP_ICONTAINS:
            return strcasestr(val_str, c->value) != NULL;
        case OP_INOT_CONTAINS:
            return strcasestr(val_str, c->value) == NULL;
        case OP_ISTARTS_WITH:
            return strncasecmp(val_str, c->value, strlen(c->value)) == 0;
        case OP_IENDS_WITH: {
            size_t vl = strlen(val_str), cl = strlen(c->value);
            if (vl < cl) return 0;
            return strcasecmp(val_str + vl - cl, c->value) == 0;
        }
        case OP_IN:
            for (int i = 0; i < c->in_count; i++)
                if (strcmp(val_str, c->in_values[i]) == 0) return 1;
            return 0;
        case OP_NOT_IN:
            for (int i = 0; i < c->in_count; i++)
                if (strcmp(val_str, c->in_values[i]) == 0) return 0;
            return 1;
        case OP_REGEX:
        case OP_NOT_REGEX:
            /* Legacy/composite path: no pre-compiled regex on SearchCriterion
               (CompiledCriterion lives in the typed path). Per-thread cache
               of (pattern → regex_t) so regcomp fires once per thread per
               distinct pattern, not once per match. Hot on indexed-regex
               callbacks (collect_hash_cb / idx_count_cb) where every leaf
               entry hits this branch. */
            {
                static __thread regex_t  re_cached;
                static __thread char     pat_cached[1024];
                static __thread int      pat_compiled = 0;
                if (!pat_compiled || strcmp(pat_cached, c->value) != 0) {
                    if (pat_compiled) { regfree(&re_cached); pat_compiled = 0; }
                    if (regcomp(&re_cached, c->value, REG_EXTENDED | REG_NOSUB) != 0)
                        return c->op == OP_REGEX ? 0 : 1;
                    strncpy(pat_cached, c->value, sizeof(pat_cached) - 1);
                    pat_cached[sizeof(pat_cached) - 1] = '\0';
                    pat_compiled = 1;
                }
                int hit = (regexec(&re_cached, val_str, 0, NULL, 0) == 0);
                return c->op == OP_REGEX ? hit : !hit;
            }
        case OP_LEN_EQ:
        case OP_LEN_NEQ:
        case OP_LEN_LESS:
        case OP_LEN_GREATER:
        case OP_LEN_LESS_EQ:
        case OP_LEN_GREATER_EQ:
        case OP_LEN_BETWEEN: {
            /* Legacy/composite path: strlen reads the user-visible length.
               Embedded NULs in varchar (rare) under-report here; the typed
               fast path uses the precise length-prefix and is the canonical
               implementation. */
            int64_t L = (int64_t)strlen(val_str);
            int64_t q1 = c->len_target;
            int64_t q2 = c->len_target2;
            switch (c->op) {
                case OP_LEN_EQ:         return L == q1;
                case OP_LEN_NEQ:        return L != q1;
                case OP_LEN_LESS:       return L <  q1;
                case OP_LEN_GREATER:    return L >  q1;
                case OP_LEN_LESS_EQ:    return L <= q1;
                case OP_LEN_GREATER_EQ: return L >= q1;
                case OP_LEN_BETWEEN:    return L >= q1 && L <= q2;
                default:                return 0;
            }
        }
        /* Field-vs-field: only reachable through the typed fast path
           (match_typed); legacy/composite path returns no match. */
        case OP_EQ_FIELD: case OP_NEQ_FIELD:
        case OP_LT_FIELD: case OP_GT_FIELD:
        case OP_LTE_FIELD: case OP_GTE_FIELD:
            return 0;
    }
    return 0;
}


/* ========== Indexed Find: Collect ALL → Batch Process in Parallel ========== */

/* Collecting callback for find — gathers hashes, applies primary filter.
   Lock-free: the entries buffer is a single mmap MAP_ANONYMOUS|MAP_NORESERVE
   reservation up to QUERY_BUFFER_MB. Workers claim slot indices via
   atomic_fetch_add; the kernel lazy-commits pages on first write, so a
   query that only collects 100 hashes pays for ~1 page (4 KB) of physical
   RAM despite the large virtual reservation. No realloc → no pointer
   migration → no need for a lock around appends. The btree_idx_* wrappers
   fan out per shard with parallel_for so this callback is invoked from
   multiple worker threads simultaneously. */
/* CollectCtx, StreamKeysetCtx, IdxCountCtx, collect_ctx_init/destroy,
   ld_be_*, varchar_eff_len are now in query_internal.h */

/* Forward decls — defined alongside btree_dispatch below; declared here so
   collect_hash_cb can route LEN_* ops through the vlen-only fast path. */
int op_is_length(enum SearchOp op);
int match_length_vlen(size_t vlen, const SearchCriterion *c);

int collect_hash_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    CollectCtx *cc = (CollectCtx *)ctx;
    if (query_deadline_tick(cc->deadline, &cc->dl_counter)) return -1;

    /* Per-entry filter — fully lock-free, parallel-safe. */
    if (cc->primary_crit && op_is_length(cc->primary_crit->op)) {
        if (!match_length_vlen(vlen, cc->primary_crit)) return 0;
    } else if (cc->check_primary && cc->primary_crit) {
        char tmp[1028];
        int matched;
        if (cc->tf) {
            int dlen = decode_idx_to_buf(cc->tf, (const uint8_t*)val, vlen,
                                          tmp, sizeof(tmp), 0);
            if (dlen <= 0) return 0;
            matched = match_criterion(tmp, cc->primary_crit);
        } else {
            size_t cl = vlen < sizeof(tmp) - 1 ? vlen : sizeof(tmp) - 1;
            memcpy(tmp, val, cl); tmp[cl] = '\0';
            matched = match_criterion_vlen(tmp, cl, cc->primary_crit);
        }
        if (!matched) return 0;
    }

    /* Atomic slot allocation. Beyond cap or the caller-supplied early-stop
       limit → bail; the buffer is mmap'd up to the per-query cap so the
       former realloc-with-mutex path is gone. */
    size_t idx = __atomic_fetch_add(&cc->count, 1, __ATOMIC_RELAXED);
    
    if (cc->collect_cap > 0 && idx >= (size_t)cc->collect_cap) return -1;
    if (idx >= cc->cap) {
        __atomic_store_n(&cc->budget_exceeded, 1, __ATOMIC_RELAXED);
        return -1;
    }

    CollectedHash *e = &cc->entries[idx];
    memcpy(e->hash, hash16, 16);
    e->shard_id = compute_record_shard(hash16, cc->splits);
    e->start_slot = 0;
    return 0;
}

/* Streaming KeySet builder — alternative to collect_hash_cb for paths
   that only need O(1) hash membership and never fetch records. Skips
   the entries[] malloc + post-walk iteration entirely; each matching
   leaf entry inserts directly into the KeySet (lock-free CAS in
   keyset_insert keeps it parallel-safe under shard fan-out).
    Used by the min/max-with-criteria fast path in cmd_aggregate. */

int stream_keyset_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    StreamKeysetCtx *sk = (StreamKeysetCtx *)ctx;
    if (query_deadline_tick(sk->deadline, &sk->dl_counter)) return -1;
    if (sk->primary_crit && op_is_length(sk->primary_crit->op)) {
        if (!match_length_vlen(vlen, sk->primary_crit)) return 0;
    } else if (sk->check_primary && sk->primary_crit) {
        char tmp[1028];
        int matched;
        if (sk->tf) {
            int dlen = decode_idx_to_buf(sk->tf, (const uint8_t*)val, vlen,
                                          tmp, sizeof(tmp), 0);
            if (dlen <= 0) return 0;
            matched = match_criterion(tmp, sk->primary_crit);
        } else {
            size_t cl = vlen < sizeof(tmp) - 1 ? vlen : sizeof(tmp) - 1;
            memcpy(tmp, val, cl); tmp[cl] = '\0';
            matched = match_criterion_vlen(tmp, cl, sk->primary_crit);
        }
        if (!matched) return 0;
    }
    (void)val;
    if (keyset_insert(sk->ks, hash16) < 0) {
        atomic_store_explicit(&sk->full, 1, memory_order_relaxed);
        return -1; /* keyset full — abort walk; caller falls back. */
    }
    return 0;
}

/* Look up TypedField for a criterion's indexed field. Returns NULL for
   composite indexes (pc->field contains '+') or when the field isn't in
   the typed schema — both cases fall through to raw-byte index semantics. */
const TypedField *resolve_idx_field(const TypedSchema *ts, const char *field) {
    if (!ts || !field || !field[0]) return NULL;
    if (strchr(field, '+')) return NULL;
    int fi = typed_field_index(ts, field);
    return (fi >= 0) ? &ts->fields[fi] : NULL;
}

/* Encode a criterion value for index lookup. If tf is NULL (composite index
   or unknown field), returns the text as raw bytes. Otherwise emits
   memcmp-sortable bytes matching what the write side stored. Output written
   into caller's buf; *out_len set. */
void encode_criterion_value(const TypedField *tf,
                                   const char *val, size_t vlen,
                                   uint8_t *buf, size_t *out_len) {
    if (!tf) {
        /* No typed field to bound the copy by (composite-index field name,
           or a field absent from the typed schema) — vlen here is a raw
           user-supplied criterion value with no upper bound from the wire
           protocol. Every call site's buf is a fixed-size stack buffer of
           at least 1024 bytes (verified across all call sites in query.c /
           query_plan.c / query_aggregate.c); clamp to that so an
           over-length value can never overflow the smallest of them
           (CID 1696413). */
        size_t n = vlen < 1024 ? vlen : 1024;
        memcpy(buf, val, n);
        *out_len = n;
        return;
    }
    encode_field_for_index(tf, val, vlen, buf, out_len);
}

/* Count-mode shortcut: operator has a positive counterpart so
   count(neg) = count(*) - count(pos) is cheaper. NEQ / NOT_IN hit a
   targeted positive set instead of walking the complement; NOT_EXISTS
   also fixes a pre-existing count-inversion (the old path returned the
   full index size instead of the inverse). */
static int op_is_negatable(enum SearchOp op) {
    return op == OP_NOT_EQUAL || op == OP_NOT_LIKE || op == OP_NOT_CONTAINS ||
           op == OP_NOT_IN || op == OP_NOT_EXISTS;
}

static enum SearchOp op_invert(enum SearchOp op) {
    switch (op) {
    case OP_NOT_EQUAL:    return OP_EQUAL;
    case OP_NOT_LIKE:     return OP_LIKE;
    case OP_NOT_CONTAINS: return OP_CONTAINS;
    case OP_NOT_IN:       return OP_IN;
    case OP_NOT_EXISTS:   return OP_EXISTS;
    default:              return op;
    }
}

/* Ops where btree_dispatch falls through to the default full-leaf scan
   (no precise range), so the callback must validate per entry against
   the criterion. The CI variants (ILIKE/ICONTAINS/...) join because the
   btree is byte-sorted and case-folding isn't byte-monotonic — there's
   no range shortcut. Length ops are handled separately via op_is_length. */
int op_needs_check_primary(enum SearchOp op) {
    return op == OP_CONTAINS || op == OP_LIKE || op == OP_ENDS_WITH ||
           op == OP_NOT_LIKE || op == OP_NOT_CONTAINS || op == OP_NOT_IN ||
           op == OP_ILIKE || op == OP_ICONTAINS ||
           op == OP_ISTARTS_WITH || op == OP_IENDS_WITH ||
           op == OP_INOT_LIKE || op == OP_INOT_CONTAINS ||
           op == OP_REGEX || op == OP_NOT_REGEX;
}

/* True if the op is a length comparator answerable from (val, vlen) alone —
   no record fetch needed, just inspect the btree leaf entry's vlen. */
int op_is_length(enum SearchOp op) {
    return op == OP_LEN_EQ || op == OP_LEN_NEQ ||
           op == OP_LEN_LESS || op == OP_LEN_GREATER ||
           op == OP_LEN_LESS_EQ || op == OP_LEN_GREATER_EQ ||
           op == OP_LEN_BETWEEN;
}

/* Length match against the btree entry's vlen — exact even when the value
   contains embedded NULs (which strlen would under-report). Bypasses the
   tmp-string roundtrip in match_criterion for hot-path callbacks. */
int match_length_vlen(size_t vlen, const SearchCriterion *c) {
    int64_t L = (int64_t)vlen;
    int64_t q1 = c->len_target;
    int64_t q2 = c->len_target2;
    switch (c->op) {
        case OP_LEN_EQ:         return L == q1;
        case OP_LEN_NEQ:        return L != q1;
        case OP_LEN_LESS:       return L <  q1;
        case OP_LEN_GREATER:    return L >  q1;
        case OP_LEN_LESS_EQ:    return L <= q1;
        case OP_LEN_GREATER_EQ: return L >= q1;
        case OP_LEN_BETWEEN:    return L >= q1 && L <= q2;
        default:                return 0;
    }
}

/* Dispatch B+ tree query based on search operator. Used by find, count, aggregate.
   tf is the TypedField for pc->field (NULL for composite indexes or
   untyped objects — in that case values are passed as raw bytes, matching
   the legacy ASCII storage of composite indexes). With the per-shard index
   layout each call fans out across index_splits_for(splits) shard files
   internally. */
/* Forward decls — both definitions live near build_keyset_from_bitmap. */
enum IndexType field_index_type(const char *db_root, const char *object,
                                       const char *field);
int field_has_index_type(const char *db_root, const char *object,
                                const char *field, enum IndexType want);
int op_prefers_trigram(enum SearchOp op);
int pick_index_for_leaf(const char *db_root, const char *object,
                               const SearchCriterion *c);

/* Per-shard worker arg for parallel bitmap dispatch — mirrors btree's
   ShardWalkArg in index.c. The shared atomic stop_flag lets a worker
   that hits the caller's limit cancel its peers. */
typedef struct BmShardWalkArg {
    const char  *db_root;
    const char  *object;
    const char  *field;
    int          shard_idx;
    const uint8_t *value;
    size_t       vlen;
    bt_result_cb cb;
    void        *ctx;
    SlotcaskDb  *sdb;
    int         *stop_flag;     /* manipulated via __atomic_* */
} BmShardWalkArg;

/* Walk a single shard's bitmap for `value`, emitting cb(value, vlen, hash)
   per live matching slot. */
int bitmap_emit_for_shard(const char *db_root, const char *object,
                                 const char *field, int shard_idx,
                                 const uint8_t *value, size_t vlen,
                                 bt_result_cb cb, void *ctx, SlotcaskDb *sdb);
static void *bm_shard_walk_worker(void *arg);

/* Type-aware front for the planner's per-leaf dispatch. Bitmap-indexed
   eq leaves walk the bitmap shards directly and emit per-match
   callbacks; everything else falls through to the existing btree path. */
/* BmGenericShardArg's struct body is needed at btree_dispatch's call
   site (sizeof + initialization). The worker function body lives near
   the bitmap helpers (~L10990); we forward-declare it here. */
typedef struct BmGenericShardArg {
    const char       *db_root;
    const char       *object;
    const char       *field;
    int               shard_idx;
    SearchCriterion  *crit;
    const TypedField *tf;
    bt_result_cb      cb;
    void             *ctx;
    SlotcaskDb       *sdb;
    int              *stop_flag;
} BmGenericShardArg;
static void *bm_generic_shard_worker(void *arg);

/* ---- O_DIRECT btree leaf-scan adapter (btree_dispatch default branch) ----
 *
 * For ops that require a full index leaf scan (contains, ends, like-substring,
 * regex, len_eq/lt/gt, not_in, not_like, not_contains, exists — the
 * "default:" case in btree_dispatch), replace the mmap-based
 * btree_idx_range("","","\xff"...) with a cache-bypassing
 * btree_leaf_scan_o_direct per shard.
 *
 * Callback shape:
 *   od_leaf_cb:   (const uint8_t *value, size_t vlen, hash16, ctx)
 *   bt_result_cb: (const char    *value, size_t vlen, hash16, ctx)
 * These are identical in layout (uint8_t* vs char* is ABI-identical);
 * the adapter is a thin cast. */
typedef struct {
    char          idx_path[PATH_MAX];
    bt_result_cb  cb;
    void         *ctx;
} BtreeOdShardArg;

static int btree_od_leaf_cb(const uint8_t *value, size_t vlen,
                             const uint8_t hash16[16], void *raw_ctx)
{
    BtreeOdShardArg *arg = (BtreeOdShardArg *)raw_ctx;
    /* bt_result_cb and od_leaf_cb have identical runtime layout. */
    return arg->cb((const char *)value, vlen, hash16, arg->ctx);
}

static void *btree_od_shard_worker(void *raw)
{
    BtreeOdShardArg *arg = (BtreeOdShardArg *)raw;
    btree_leaf_scan_o_direct(arg->idx_path, btree_od_leaf_cb, arg);
    /* Flush any thread-local count accumulator just as shard_walk_worker
       does (index.c:129). No-op for callbacks that don't use TLS. */
    idx_count_cb_flush_thread();
    return NULL;
}

/* Fan out btree_leaf_scan_o_direct across index_splits_for(splits) shards,
 * mirroring the parallel_for pattern of shard_walk_dispatch in index.c. */
static void btree_idx_full_leaf_scan_o_direct(
        const char *db_root, const char *object,
        const char *field, int splits,
        bt_result_cb cb, void *ctx)
{
    int idx_n = index_splits_for(splits);
    BtreeOdShardArg *args = malloc((size_t)idx_n * sizeof(BtreeOdShardArg));
    if (!args) {
        /* OOM fallback: original mmap path. */
        btree_idx_range(db_root, object, field, splits,
                        "", 0, "\xff\xff\xff\xff", 4, cb, ctx);
        return;
    }
    for (int s = 0; s < idx_n; s++) {
        build_idx_path(args[s].idx_path, sizeof(args[s].idx_path),
                       db_root, object, field, s);
        args[s].cb  = cb;
        args[s].ctx = ctx;
    }
    parallel_for_io(btree_od_shard_worker, args, idx_n, sizeof(BtreeOdShardArg));
    free(args);
}

void btree_dispatch(const char *db_root, const char *object,
                           const char *field, int splits,
                           SearchCriterion *pc, const TypedField *tf,
                           bt_result_cb cb, void *ctx) {
    if (field_index_type(db_root, object, field) == IT_BITMAP) {
        Schema sc = load_schema(db_root, object);
        SlotcaskSchemaInfo info = {
            .splits = sc.splits, .slot_size = sc.slot_size, .streams = sc.streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
        if (!sdb) return;

        if (pc->op == OP_EQUAL || pc->op == OP_IN) {
            /* Fast path. For OP_IN we loop once per value, parallel-fanning
               each value across all shards. Per-value bitmaps are disjoint
               so emitting across values produces no duplicates. The
               stop_flag is shared across all per-value parallel_for
               invocations so an early-stop (e.g. find's limit) propagates. */
            int n_vals = (pc->op == OP_EQUAL) ? 1 : pc->in_count;
            if (n_vals <= 0) return;

            int stop_flag = 0;
            BmShardWalkArg *args = malloc((size_t)splits * sizeof(BmShardWalkArg));
            if (!args) return;

            for (int vi = 0; vi < n_vals; vi++) {
                if (__atomic_load_n(&stop_flag, __ATOMIC_RELAXED)) break;
                uint8_t valbuf[1032];
                size_t  vallen = 0;
                const char *raw = (pc->op == OP_EQUAL) ? pc->value : pc->in_values[vi];
                encode_criterion_value(tf, raw, strlen(raw), valbuf, &vallen);
                if (vallen == 0) continue;
                for (int s = 0; s < splits; s++) {
                    args[s] = (BmShardWalkArg){
                        .db_root = db_root, .object = object, .field = field,
                        .shard_idx = s, .value = valbuf, .vlen = vallen,
                        .cb = cb, .ctx = ctx, .sdb = sdb, .stop_flag = &stop_flag
                    };
                }
                parallel_for_io(bm_shard_walk_worker, args, splits, sizeof(BmShardWalkArg));
            }
            free(args);
            return;
        }

        /* Generic dict-scan path: any other op (range, LIKE, CONTAINS,
           REGEX, len_X, exists, i-variants) iterates the dict per
           shard, evaluates the criterion against each decoded value,
           and walks the matching values bitmaps. Per-shard parallel
           via bm_generic_shard_worker; shared stop_flag handles
           early-out on limit. This is the btree-parity path:
           btree default branch full-scans leaves for these ops,
           bitmap dict-scans up to 256 dict entries instead. */
        int stop_flag = 0;
        BmGenericShardArg *gargs =
            malloc((size_t)splits * sizeof(BmGenericShardArg));
        if (!gargs) return;
        for (int s = 0; s < splits; s++) {
            gargs[s] = (BmGenericShardArg){
                .db_root = db_root, .object = object, .field = field,
                .shard_idx = s, .crit = pc, .tf = tf,
                .cb = cb, .ctx = ctx, .sdb = sdb, .stop_flag = &stop_flag
            };
        }
        parallel_for_io(bm_generic_shard_worker, gargs, splits, sizeof(BmGenericShardArg));
        free(gargs);
        return;
    }

    /* Stack buffers for encoded key bytes. Max fixed-type output is 8 B;
       varchar caps at f->size - 2 ≤ 65533 — BT_MAX_VAL_LEN = 512 in practice.
       Keep generous for the "contains text + 4 sentinel bytes" suffix cases. */
    uint8_t buf1[1032], buf2[1032];
    size_t len1 = 0, len2 = 0;

    switch (pc->op) {
        case OP_EQUAL:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            btree_idx_search(db_root, object, field, splits,
                             (const char *)buf1, len1, cb, ctx);
            break;
        case OP_GREATER_EQ:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            btree_idx_range(db_root, object, field, splits,
                            (const char *)buf1, len1,
                            "\xff\xff\xff\xff", 4, cb, ctx);
            break;
        case OP_GREATER:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            btree_idx_range_ex(db_root, object, field, splits,
                               (const char *)buf1, len1, 1,
                               "\xff\xff\xff\xff", 4, 0, cb, ctx);
            break;
        case OP_LESS_EQ:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            btree_idx_range(db_root, object, field, splits,
                            "", 0, (const char *)buf1, len1, cb, ctx);
            break;
        case OP_LESS:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            btree_idx_range_ex(db_root, object, field, splits,
                               "", 0, 0,
                               (const char *)buf1, len1, 1, cb, ctx);
            break;
        case OP_BETWEEN:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            encode_criterion_value(tf, pc->value2, strlen(pc->value2), buf2, &len2);
            if (pc->min_exclusive || pc->max_exclusive) {
                btree_idx_range_ex(db_root, object, field, splits,
                                   (const char *)buf1, len1, pc->min_exclusive,
                                   (const char *)buf2, len2, pc->max_exclusive,
                                   cb, ctx);
            } else {
                btree_idx_range(db_root, object, field, splits,
                                (const char *)buf1, len1,
                                (const char *)buf2, len2, cb, ctx);
            }
            break;
        case OP_IN:
            for (int iv = 0; iv < pc->in_count; iv++) {
                encode_criterion_value(tf, pc->in_values[iv],
                                       strlen(pc->in_values[iv]), buf1, &len1);
                btree_idx_search(db_root, object, field, splits,
                                 (const char *)buf1, len1, cb, ctx);
            }
            break;
        case OP_STARTS_WITH: {
            /* Prefix-match only makes sense on raw-byte keys (varchar or
               composite). For signed numeric keys with their top-bit flip,
               "starts with X" isn't meaningful — fall through to raw bytes. */
            int raw_prefix = (!tf || tf->type == FT_VARCHAR);
            size_t plen;
            if (raw_prefix) {
                plen = strlen(pc->value);
                memcpy(buf1, pc->value, plen);
            } else {
                encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &plen);
            }
            memcpy(buf2, buf1, plen);
            memset(buf2 + plen, 0xff, 4);
            btree_idx_range(db_root, object, field, splits,
                            (const char *)buf1, plen,
                            (const char *)buf2, plen + 4, cb, ctx);
            break;
        }
        case OP_LIKE: {
            /* CS LIKE: leverage the pattern's % placement to narrow the btree
               read. Only meaningful on varchar / composite (raw-byte) keys.
                 "foo"   (no %)  → point lookup, same as OP_EQUAL.
                 "foo%"  (trail) → prefix range, same as OP_STARTS_WITH.
                 "%foo"  (lead)  → suffix match — no shortcut, full leaf scan.
                 "%foo%" (both)  → substring — no shortcut, full leaf scan.
               compile_one already classified this into pc->value's % placement;
               we re-derive here because btree_dispatch sees only SearchCriterion.
               Callbacks still apply check_primary; for LK_EXACT/LK_PREFIX that's
               a redundant per-entry confirm but cheap. */
            const char *pat = pc->value;
            size_t pl = strlen(pat);
            int lead = (pl >= 1 && pat[0] == '%');
            int trail = (pl >= 1 && pat[pl-1] == '%');
            int raw_prefix = (!tf || tf->type == FT_VARCHAR);

            if (!lead && !trail && raw_prefix) {
                /* Exact byte match — point lookup. */
                encode_criterion_value(tf, pat, pl, buf1, &len1);
                btree_idx_search(db_root, object, field, splits,
                                 (const char *)buf1, len1, cb, ctx);
                break;
            }
            if (!lead && trail && raw_prefix) {
                /* "foo%" — strip trailing % and use prefix range scan. */
                size_t needle_len = pl - 1;
                memcpy(buf1, pat, needle_len);
                memcpy(buf2, buf1, needle_len);
                memset(buf2 + needle_len, 0xff, 4);
                btree_idx_range(db_root, object, field, splits,
                                (const char *)buf1, needle_len,
                                (const char *)buf2, needle_len + 4, cb, ctx);
                break;
            }
            /* Substring / suffix / non-varchar → full leaf scan via O_DIRECT;
               per-entry filter via check_primary in the callback handles
               correctness.  Cache-bypassing: hot index pages for targeted
               reads (eq/range/prefix) are not evicted. */
            btree_idx_full_leaf_scan_o_direct(db_root, object, field, splits,
                                              cb, ctx);
            break;
        }
        case OP_NOT_EQUAL:
            /* Boolean has exactly 2 values — neq X is equivalent to eq
               other-value. Single point lookup instead of two range scans,
               which saves a second tree descent at query time. Only
               applies to FT_BOOL; every other type has too large a domain
               to enumerate "everything except X" cheaply. */
            if (tf && tf->type == FT_BOOL) {
                int is_true = (pc->value[0] == 't' || pc->value[0] == 'T' ||
                               pc->value[0] == '1');
                uint8_t inv[1] = { (uint8_t)(is_true ? 0 : 1) };
                btree_idx_search(db_root, object, field, splits,
                                 (const char *)inv, 1, cb, ctx);
                break;
            }
            /* General case: two exclusive ranges covering everything except X. */
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            btree_idx_range_ex(db_root, object, field, splits,
                               "", 0, 0,
                               (const char *)buf1, len1, 1, cb, ctx);
            btree_idx_range_ex(db_root, object, field, splits,
                               (const char *)buf1, len1, 1,
                               "\xff\xff\xff\xff", 4, 0, cb, ctx);
            break;
        default:
            /* Full index scan: contains, like, ends_with, not_like,
               not_contains, not_in, exists — use O_DIRECT leaf walk to
               avoid polluting the btree page cache used by fast paths. */
            btree_idx_full_leaf_scan_o_direct(db_root, object, field, splits,
                                              cb, ctx);
            break;
    }
}

/* Lightweight counting callback — no hash storage, just increments counter.
   Used by count mode. For single criterion: pure index counter.
   For multi-criteria: fetches record inline to verify all criteria. */
/* IdxCountCtx is now in query_internal.h */

/* Single-criterion inline counter. No record fetch — btree visit = match.
   Thread-safe: count increment is atomic (the btree_idx_* wrappers fan out
   per shard via parallel_for so this callback fires from multiple threads
   concurrently). dl_counter races are tolerated — query_deadline_tick is a
   coarse heuristic, the only consequence of a few lost increments is the
   deadline check happening slightly more or less often. */
/* Per-thread batched-count state. The earlier design called
   __atomic_add_fetch(&ic->count, 1, …) on every match — for ops where
   most rows match (len_neq, len_gt, len_gte, exists varchar,
   not_icontains, …) that's millions of contended cache-line bounces
   across the parallel-for shard workers, costing ~20-30ms vs ~6-8ms
   for low-match ops on the same data.
   The new path accumulates locally per thread; shard_walk_worker
   (index.c) calls idx_count_cb_flush_thread() after each per-shard
   btree_*() returns, atomic-adding the local total to ic->count once
   per shard worker per query. Result on the bench's 1M users: matched
   sets of ~900K go from ~28ms to ~6ms (4-5×). */
static __thread struct {
    void *bound_ic;       /* IdxCountCtx this thread is currently accumulating for */
    long  pending;
} idx_count_local = { NULL, 0 };

int idx_count_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    (void)hash16;
    IdxCountCtx *ic = (IdxCountCtx *)ctx;
    /* Rebind detection. Defensive only — shard_walk_worker flushes at the
       end of every per-shard btree call, so bound_ic should be NULL when
       a fresh ic arrives. If it isn't (some non-walker path neglected to
       flush), we'd otherwise leak the pending count to the wrong ic;
       flush it eagerly here. */
    if (idx_count_local.bound_ic != ic) {
        if (idx_count_local.bound_ic) {
            __atomic_add_fetch(
                &((IdxCountCtx *)idx_count_local.bound_ic)->count,
                idx_count_local.pending, __ATOMIC_RELAXED);
        }
        idx_count_local.bound_ic = ic;
        idx_count_local.pending = 0;
    }
    if (query_deadline_tick(ic->deadline, &ic->dl_counter)) return -1;
    if (ic->primary_crit && op_is_length(ic->primary_crit->op)) {
        if (!match_length_vlen(vlen, ic->primary_crit)) return 0;
    } else if (ic->check_primary && ic->primary_crit) {
        char tmp[1028];
        int matched;
        if (ic->tf) {
            int dlen = decode_idx_to_buf(ic->tf, (const uint8_t*)val, vlen,
                                          tmp, sizeof(tmp), 0);
            if (dlen <= 0) return 0;
            matched = match_criterion(tmp, ic->primary_crit);
        } else {
            size_t cl = vlen < sizeof(tmp) - 1 ? vlen : sizeof(tmp) - 1;
            memcpy(tmp, val, cl); tmp[cl] = '\0';
            matched = match_criterion_vlen(tmp, cl, ic->primary_crit);
        }
        if (!matched) return 0;
    }
    idx_count_local.pending++;
    /* Cap residency so a freak query (millions of matches in one shard
       worker) doesn't sit on a huge unflushed local before the per-shard
       flush at end of btree_*. 4096 keeps the atomic-add count tiny
       (4× fewer than current per-match atomics for any matched set
       under 4K, identical above) while bounding worst-case TLS to a
       single long. */
    if (idx_count_local.pending >= 4096) {
        __atomic_add_fetch(&ic->count, idx_count_local.pending, __ATOMIC_RELAXED);
        idx_count_local.pending = 0;
    }
    return 0;
}

/* Flush this thread's local count accumulator to its bound ctx and
   detach. Called by shard_walk_worker (index.c) after each per-shard
   btree_*() returns so the orchestrator's read of ic->count after
   parallel_for sees every worker's contribution. */
void idx_count_cb_flush_thread(void) {
    if (idx_count_local.bound_ic) {
        __atomic_add_fetch(
            &((IdxCountCtx *)idx_count_local.bound_ic)->count,
            idx_count_local.pending, __ATOMIC_RELAXED);
        idx_count_local.bound_ic = NULL;
        idx_count_local.pending = 0;
    }
}

/* ========== Indexed count with multi-criteria: parallel per-shard ==========
   Mirrors the old shard_find_worker's shape but accumulates a counter instead
   of collecting per-record results — called when cmd_count has secondary criteria. */
typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    CollectedHash *entries;
    int entry_count;
    CriteriaNode *tree;     /* compiled tree; full re-match per candidate */
    FieldSchema *fs;
    QueryDeadline *deadline;
    int dl_counter;
    size_t count;           /* result: matches in this shard */

    /* Bitmap post-filter shortcut (populated by parallel_indexed_count) */
    int n_bm_postfilter;                              // number of bitmap eq/in post-filter leaves
    int all_postfilters_are_bm;                       // 1 = Case A1 (index-only), 0 = Case A2 or none
    int no_bm_shortcut;                               // 1 = prevent all_postfilters_are_bm shortcut (trigram primary)
    SearchCriterion *bm_criteria[MAX_INTERSECT_LEAVES]; // bitmap post-filter SearchCriterion*
    uint8_t          bm_val[MAX_INTERSECT_LEAVES][1024]; // pre-encoded value for OP_EQUAL
    size_t           bm_vlen[MAX_INTERSECT_LEAVES];
    int              bm_in_count[MAX_INTERSECT_LEAVES];  // >1 for OP_IN
    uint8_t          bm_in_vals[MAX_INTERSECT_LEAVES][8][1024]; // OP_IN values (cap at 8)
    size_t           bm_in_vlens[MAX_INTERSECT_LEAVES][8];
    /* Pre-opened bitmap handles for this worker's shard (one per bitmap leaf) */
    void            *bm_handles[MAX_INTERSECT_LEAVES]; // BitmapShard* per leaf
} ShardCountCtx;

/* count_batch_cb — callback for batch lookup in shard_count_worker.
   Runs criteria_match_tree; increments count on match. */
typedef struct {
    ShardCountCtx *sc;
    size_t        *local;
} CountBatchCbCtx;

static int count_batch_cb(const uint8_t hash[16],
                           const void *key, size_t klen,
                           const void *value, size_t vlen,
                           void *ctx_ptr) {
    (void)hash; (void)key; (void)klen;
    CountBatchCbCtx *c = (CountBatchCbCtx *)ctx_ptr;
    if (query_deadline_tick(c->sc->deadline, &c->sc->dl_counter)) return 1;
    if (criteria_match_tree((const uint8_t *)value, c->sc->tree, c->sc->fs)) {
        __atomic_add_fetch(c->local, 1, __ATOMIC_RELAXED);
    }
    return 0;
}

static void *shard_count_worker(void *arg) {
    ShardCountCtx *sc = (ShardCountCtx *)arg;
    if (sc->entry_count == 0) return NULL;

    /* Pre-open bitmap shards and KF handle for this worker's shard group.
     * All entries in this group share the same hash[0..1] → same data shard. */
    int shard_id = -1;
    SlotcaskDb *sdb = NULL;
    SlotcaskKfHandle kh;
    memset(&kh, 0, sizeof(kh));
    if (sc->n_bm_postfilter > 0 && sc->entry_count > 0) {
        shard_id = compute_record_shard(sc->entries[0].hash, sc->sch->splits);
        SlotcaskSchemaInfo info = {
            .splits = sc->sch->splits,
            .slot_size = sc->sch->slot_size,
            .streams = sc->sch->streams,
        };
        sdb = slotcask_registry_get(sc->db_root, sc->object, &info);
        /* Pre-open KF handle — acquire once, probe inline in the hot
         * loop without per-hash kfcache_acquire/release overhead. */
        if (sdb) {
            char kf_p[PATH_MAX];
            kf_path_for(kf_p, sdb->data_dir, shard_id);
            if (kfcache_acquire(&kh, kf_p, sdb->slots_per_shard, 0) != 0)
                sdb = NULL;  // disable KF probe if acquire fails
        }
        for (int b = 0; b < sc->n_bm_postfilter; b++) {
            char bp[PATH_MAX];
            bm_build_path(bp, sizeof(bp), sc->db_root, sc->object,
                          sc->bm_criteria[b]->field, shard_id);
            sc->bm_handles[b] = bm_open(bp, 0, 0, 0, 0, 0);
        }
    }

    size_t local = 0;
    /* Collect hashes that need actual record fetch (bitmap path couldn't
       resolve them). We'll batch these to amortise cache operations. */
    int n_need_fetch = 0;
    uint8_t (*fetch_hashes)[16] = NULL;
    SlotcaskResolvedRec *resolved = NULL;

    if (sc->entry_count <= 0) {
        sc->count = 0;
        goto cleanup;
    }

    if (shard_id < 0) {
        /* No bitmap post-filters — all entries need record fetch. */
        n_need_fetch = sc->entry_count;
        fetch_hashes = calloc((size_t)n_need_fetch, sizeof(*fetch_hashes));
        if (!fetch_hashes) { sc->count = 0; goto cleanup; }
        for (int ei = 0; ei < n_need_fetch; ei++)
            memcpy(fetch_hashes[ei], sc->entries[ei].hash, 16);
    } else {
        /* Bitmap post-filters present: two-pass approach.
           Pass 1: run bitmap pre-filter, capture resolved locations. */
        resolved = calloc((size_t)sc->entry_count, sizeof(*resolved));
        if (!resolved) { sc->count = 0; goto cleanup; }

        for (int ei = 0; ei < sc->entry_count; ei++) {
            if (query_deadline_tick(sc->deadline, &sc->dl_counter)) break;

            /* --- Bitmap pre-filter --- */
            uint32_t kf_slot = 0;
            int kf_found = 0;
            uint8_t found_sid = 0;
            uint16_t found_fid = 0;
            uint32_t found_off = 0;
            {
                size_t cap = kh.capacity;
                SlotcaskKfEntry *kf = kh.map;
                size_t start = kf_slot_for(sc->entries[ei].hash, cap);
                for (size_t pi = 0; pi < cap; pi++) {
                    size_t slot = (start + pi) % cap;
                    SlotcaskKfEntry *e = &kf[slot];
                    uint8_t flag = __atomic_load_n(&e->flag, __ATOMIC_ACQUIRE);
                    if (flag == 0) break;
                    if (flag != 1) continue;
                    if (memcmp(e->hash, sc->entries[ei].hash, 16) != 0) continue;
                    kf_slot = (uint32_t)slot;
                    kf_found = 1;
                    /* Capture resolved location during KF probe —
                       eliminates re-probe in fetch phase. */
                    found_sid = e->stream_id;
                    found_fid = e->file_id;
                    found_off = e->offset;
                    break;
                }
            }
            if (!kf_found) continue;

            /* Test all bitmap post-filters */
            int bm_pass = 1;
            int bm_indeterminate = 0;
            for (int b = 0; b < sc->n_bm_postfilter; b++) {
                BitmapShard *bm = (BitmapShard *)sc->bm_handles[b];
                if (!bm || kf_slot >= bm_slots(bm)) {
                    bm_indeterminate = 1;
                    break;
                }
                int any_match = 0;
                int nv = (sc->bm_in_count[b] > 0) ? sc->bm_in_count[b] : 1;
                for (int v = 0; v < nv; v++) {
                    const uint8_t *val = (sc->bm_in_count[b] > 0)
                        ? sc->bm_in_vals[b][v] : sc->bm_val[b];
                    size_t vlen = (sc->bm_in_count[b] > 0)
                        ? sc->bm_in_vlens[b][v] : sc->bm_vlen[b];
                    if (bm_test(bm, val, vlen, kf_slot)) { any_match = 1; break; }
                }
                if (!any_match) { bm_pass = 0; break; }
            }

            if (bm_indeterminate) {
                /* Fall through — needs record fetch */
                memcpy(resolved[n_need_fetch].hash, sc->entries[ei].hash, 16);
                resolved[n_need_fetch].sid = found_sid;
                resolved[n_need_fetch].fid = found_fid;
                resolved[n_need_fetch].off = found_off;
                n_need_fetch++;
            } else if (!bm_pass) {
                continue;  /* bitmap rejection: skip */
            } else if (sc->all_postfilters_are_bm) {
                local++;  /* index-only count, no record fetch */
                continue;
            } else {
                /* Bitmaps passed, non-bitmap post-filters need record fetch */
                memcpy(resolved[n_need_fetch].hash, sc->entries[ei].hash, 16);
                resolved[n_need_fetch].sid = found_sid;
                resolved[n_need_fetch].fid = found_fid;
                resolved[n_need_fetch].off = found_off;
                n_need_fetch++;
            }
        }
    }

    /* Pass 2: batch fetch all needs-fetch entries */
    if (n_need_fetch > 0) {
        SlotcaskSchemaInfo info = {
            .splits = sc->sch->splits,
            .slot_size = sc->sch->slot_size,
            .streams = sc->sch->streams,
        };
        SlotcaskDb *batch_sdb = sdb;
        if (!batch_sdb)
            batch_sdb = slotcask_registry_get(sc->db_root, sc->object, &info);
        if (batch_sdb) {
            CountBatchCbCtx cb_ctx = { sc, &local };
            if (resolved) {
                /* Bitmap path: already have resolved locations, skip KF re-probe */
                slotcask_bulk_fetch_resolved(batch_sdb, resolved,
                                              (size_t)n_need_fetch,
                                              &cb_ctx, count_batch_cb);
            } else {
                /* Non-bitmap path: use combined resolve+fetch */
                slotcask_bulk_resolve_and_fetch(batch_sdb, fetch_hashes,
                                                  (size_t)n_need_fetch,
                                                  &cb_ctx, count_batch_cb);
            }
        }
    }

    free(fetch_hashes);
    free(resolved);

cleanup:
    /* Close pre-opened handles */
    if (kh.map) kfcache_release(&kh);
    for (int b = 0; b < sc->n_bm_postfilter; b++) {
        if (sc->bm_handles[b]) bm_close((BitmapShard *)sc->bm_handles[b]);
    }

    sc->count = local;
    return NULL;
}

/* Classify post-filter leaves: which are bitmap eq/in that can be tested
 * without record fetch. Populates the bitmap fields in ShardCountCtx. */
static void classify_bm_postfilters(ShardCountCtx *ctx,
                                     const FilterPlan *fp,
                                     const char *db_root,
                                     const char *object,
                                     FieldSchema *fs) {
    ctx->n_bm_postfilter = 0;
    ctx->all_postfilters_are_bm = 0;
    if (!fp || fp->n_postfilter == 0) return;

    int n_bm = 0;
    for (int i = 0; i < fp->n_postfilter; i++) {
        SearchCriterion *c = fp->postfilter_leaves[i];
        if (pick_index_for_leaf(db_root, object, c) != IT_BITMAP) continue;
        if (c->op != OP_EQUAL && c->op != OP_IN) continue;
        if (n_bm >= MAX_INTERSECT_LEAVES) break;

        ctx->bm_criteria[n_bm] = c;
        const TypedField *tf = resolve_idx_field(fs->ts, c->field);
        if (c->op == OP_EQUAL) {
            encode_criterion_value(tf, c->value, strlen(c->value),
                                    ctx->bm_val[n_bm], &ctx->bm_vlen[n_bm]);
            ctx->bm_in_count[n_bm] = 0;
            n_bm++;
        } else if (c->in_count <= 8) {
            /* OP_IN: encode each value */
            int nc = c->in_count;
            ctx->bm_in_count[n_bm] = nc;
            for (int v = 0; v < nc; v++) {
                encode_criterion_value(tf, c->in_values[v], strlen(c->in_values[v]),
                                        ctx->bm_in_vals[n_bm][v],
                                        &ctx->bm_in_vlens[n_bm][v]);
            }
            n_bm++;
            continue;
        } else {
            /* OP_IN with >8 values: skip bitmap classification.
             * The leaf stays as a non-bitmap post-filter; other bitmap
             * leaves still benefit from early rejection (Case A2). */
            continue;
        }
    }
    ctx->n_bm_postfilter = n_bm;
    ctx->all_postfilters_are_bm = (n_bm > 0 && n_bm == fp->n_postfilter);
    if (ctx->no_bm_shortcut) ctx->all_postfilters_are_bm = 0;
}

/* Orchestrate parallel indexed count: qsort by shard, fan out per-shard workers. */
static size_t parallel_indexed_count(const char *db_root, const char *object,
                                     const Schema *sch, CollectedHash *batch,
                                     int batch_count, CriteriaNode *tree,
                                     FieldSchema *fs, QueryDeadline *dl,
                                     const FilterPlan *fp,
                                     int no_bm_shortcut) {
    int group_starts[1024], group_sizes[1024];
    int nshard_groups = shard_group_batch(batch, batch_count, group_starts, group_sizes, 1024);

    ShardCountCtx *workers = calloc(nshard_groups, sizeof(ShardCountCtx));
    for (int g = 0; g < nshard_groups; g++) {
        workers[g].db_root = db_root;
        workers[g].object = object;
        workers[g].sch = sch;
        workers[g].entries = &batch[group_starts[g]];
        workers[g].entry_count = group_sizes[g];
        workers[g].tree = tree;
        workers[g].fs = fs;
        workers[g].deadline = dl;
        classify_bm_postfilters(&workers[g], fp, db_root, object, fs);
        workers[g].no_bm_shortcut = no_bm_shortcut;
        if (no_bm_shortcut) workers[g].all_postfilters_are_bm = 0;
    }

    if (batch_count < 1024 || nshard_groups <= 2) {
        for (int g = 0; g < nshard_groups; g++) shard_count_worker(&workers[g]);
    } else {
        parallel_for_io(shard_count_worker, workers, nshard_groups, sizeof(ShardCountCtx));
    }

    size_t total = 0;
    for (int g = 0; g < nshard_groups; g++) total += workers[g].count;
    free(workers);
    return total;
}

/* Forward decl — keyset_emit_find lives further down. v2 path calls it. */
static int keyset_emit_find(const char *db_root, const char *object,
                            const Schema *sch, KeySet *ks,
                            CriteriaNode *tree_for_rematch,
                            ExcludedKeys *excluded, int offset, int limit,
                            const char **proj_fields, int proj_count,
                            FieldSchema *fs, int rows_fmt, int dict_fmt, char csv_delim,
                            JoinSpec *joins, int njoins, QueryDeadline *dl);

/* Orchestrate: collect hashes from B+ tree, group by shard, process */
static int idx_find_parallel(const char *db_root, const char *object, const Schema *sch,
                             const char *primary_idx_path, CriteriaNode *tree,
                             SearchCriterion *primary_crit, int check_primary,
                             ExcludedKeys *excluded,
                             int offset, int limit, const char **proj_fields, int proj_count,
                             FieldSchema *fs, int rows_fmt, int dict_fmt, char csv_delim,
                             JoinSpec *joins, int njoins, QueryDeadline *dl) {
    /* Collection cap heuristic:
       - No joins, or all-LEFT joins: every collected hash emits at most one row,
         so we can safely cap at offset+limit.
       - Any INNER join: records may drop, so we must collect unbounded and
         apply limit at emission time. */
    int all_left_or_none = 1;
    for (int i = 0; i < njoins; i++)
        if (joins[i].type != JOIN_LEFT) { all_left_or_none = 0; break; }
    int collect_target = (limit > 0 && all_left_or_none) ? offset + limit : 0;
    CollectCtx cc;
    collect_ctx_init(&cc);
    cc.splits = sch->splits;
    cc.collect_cap = collect_target;
    cc.primary_crit = primary_crit;
    cc.check_primary = check_primary;
    cc.deadline = dl;
    cc.tf = resolve_idx_field(fs ? fs->ts : NULL, primary_crit->field);

    (void)primary_idx_path; /* path now derived per-shard inside btree_idx_*; arg kept for API stability */
    btree_dispatch(db_root, object, primary_crit->field, sch->splits,
                   primary_crit,
                   resolve_idx_field(fs ? fs->ts : NULL, primary_crit->field),
                   collect_hash_cb, &cc);

    if (cc.budget_exceeded) { collect_ctx_destroy(&cc); return -2; }
    if (cc.count == 0)      { collect_ctx_destroy(&cc); return 0; }

    /* v2 dispatch: process_batch is coupled to v1's Zone A layout (groups
       hashes by shard_id, opens each shard mmap once, runs shard_find_worker
       per group). For v2 the per-shard grouping doesn't help — slotcask
       routes per hash internally — so we feed the collected hashes through
       keyset_emit_find which already speaks RecordRef + read_record_ref.
       Joins handled there too via the dispatched lookup_remote (Phase 3G). */
    KeySet *ks = keyset_new(cc.count);
    if (!ks) { collect_ctx_destroy(&cc); return -2; }
    for (size_t i = 0; i < cc.count; i++)
        keyset_insert(ks, cc.entries[i].hash);
    int rc = keyset_emit_find(db_root, object, sch, ks,
                              tree, excluded, offset, limit,
                              proj_fields, proj_count, fs,
                              rows_fmt, dict_fmt, csv_delim,
                              joins, njoins, dl);
    keyset_free(ks);
    collect_ctx_destroy(&cc);
    return rc;
}

/* ========== BatchFetchBuf — batch KF-resolve + segment-fetch ========== */

typedef struct BatchFetchBuf_ {
    uint8_t   (*pending)[16];
    size_t     pending_n;
    size_t     pending_cap;
    pthread_mutex_t lock;
    pthread_cond_t  flush_done;  /* signalled when flushing transitions 1→0 */
    int             flushing;
    SlotcaskDb *sdb;
    int (*record_cb)(const uint8_t hash16[16],
                     const void *key, size_t klen,
                     const void *value, size_t vlen,
                     void *ctx);
    void       *record_ctx;
    volatile int stop;
} BatchFetchBuf;

static int batch_buf_init(BatchFetchBuf *b, SlotcaskDb *sdb,
                           size_t slot_size, int fetch_limit) {
    size_t max_cap = g_query_buffer_max_bytes / (16 + slot_size);
    if (max_cap > 65536) max_cap = 65536;
    b->pending_cap = max_cap;
    if (fetch_limit > 0 && (size_t)fetch_limit < b->pending_cap)
        b->pending_cap = (size_t)fetch_limit;
    if (b->pending_cap < 1) b->pending_cap = 1;
    b->pending = calloc(b->pending_cap, 16);
    if (!b->pending) return -1;
    b->pending_n = 0;
    pthread_mutex_init(&b->lock, NULL);
    pthread_cond_init(&b->flush_done, NULL);
    b->flushing = 0;
    b->sdb = sdb;
    b->record_cb = NULL;
    b->record_ctx = NULL;
    b->stop = 0;
    return 0;
}

static void batch_buf_flush_copy(BatchFetchBuf *b) {
    size_t n;
    uint8_t (*copy)[16] = NULL;

    pthread_mutex_lock(&b->lock);
    n = b->pending_n;
    if (n > 0) {
        copy = malloc(n * 16);
        if (copy) {
            memcpy(copy, b->pending, n * 16);
            b->pending_n = 0;
        }
    }
    pthread_mutex_unlock(&b->lock);

    if (copy) {
        slotcask_bulk_resolve_and_fetch(b->sdb, copy, n,
                                        b->record_ctx, b->record_cb);
        free(copy);
    } else if (n > 0) {
        /* malloc failed — flush under lock (rare). */
        pthread_mutex_lock(&b->lock);
        slotcask_bulk_resolve_and_fetch(b->sdb, b->pending, n,
                                        b->record_ctx, b->record_cb);
        b->pending_n = 0;
        pthread_mutex_unlock(&b->lock);
    }
}

static void batch_buf_flush(BatchFetchBuf *b) {
    batch_buf_flush_copy(b);
}

static int batch_buf_collect_hash(BatchFetchBuf *b, const uint8_t hash16[16]) {
    for (;;) {
        pthread_mutex_lock(&b->lock);
        if (__atomic_load_n(&b->stop, __ATOMIC_ACQUIRE)) {
            pthread_mutex_unlock(&b->lock);
            return -1;
        }
        if (b->pending_n < b->pending_cap) {
            memcpy(b->pending[b->pending_n], hash16, 16);
            b->pending_n++;
            pthread_mutex_unlock(&b->lock);
            return 0;
        }
        while (b->flushing) {
            /* Block until the flushing thread completes rather than
               spinning. On overloaded runners (arm64 CI, 2 cores, 200+
               concurrent test daemons) the sched_yield() loop starved the
               flush thread, causing 60 s timeouts in test-and-intersection. */
            pthread_cond_wait(&b->flush_done, &b->lock);
        }
        /* After waiting: the flush drained the buffer. Add this hash
           directly if there is now space — avoids cascading empty flushes
           where every woken waiter sets flushing=1 and re-enters
           batch_buf_flush_copy on an already-empty buffer. Each such empty
           flush still broadcasts flush_done, and under TSan the broadcast
           overhead scales with the thread count, pushing the total per-query
           time past the 60 s client timeout on assertion-14. */
        if (b->pending_n < b->pending_cap) {
            memcpy(b->pending[b->pending_n], hash16, 16);
            b->pending_n++;
            pthread_mutex_unlock(&b->lock);
            return 0;
        }
        b->flushing = 1;
        pthread_mutex_unlock(&b->lock);

        batch_buf_flush_copy(b);

        pthread_mutex_lock(&b->lock);
        b->flushing = 0;
        pthread_cond_broadcast(&b->flush_done);
        pthread_mutex_unlock(&b->lock);
    }
}

static int batch_buf_collect_cb(const char *val, size_t vlen,
                                 const uint8_t *hash16, void *ctx) {
    (void)val; (void)vlen;
    return batch_buf_collect_hash((BatchFetchBuf *)ctx, hash16);
}

static void batch_buf_destroy(BatchFetchBuf *b) {
    batch_buf_flush(b);
    pthread_cond_destroy(&b->flush_done);
    pthread_mutex_destroy(&b->lock);
    free(b->pending);
    b->pending = NULL;
}

/* ============================================================
   Streaming indexed find — fetch + post-filter + emit per btree match.
   Replaces collect-then-emit for the limit-bound case where post-filter
   siblings make the collect cap (= offset+limit) under-collect.

   Walk the primary leaf's btree (parallel across idx shards). For each
   btree match: the cb fetches the record, runs criteria_match_tree
   against the FULL tree (so post-filter siblings get applied), and
   emits if the record passes. Stops parallel walks via an atomic flag
   when emit count hits offset+limit. Concurrent shard workers
   serialise emit through an internal mutex so JSON rows don't
   interleave.

   Restrictions: no joins, no rows_fmt table envelope, no order_by
   (caller routes those to the collect-then-emit path which can sort
   the materialised batch). Supports csv_delim, dict_fmt, projections,
   excluded keys. v2 storage only (v1 emit shape stays in the older
   fcache-handle path; we only route v2 here).
   ============================================================ */
typedef struct {
    /* Inputs (immutable across cb invocations). */
    const char       *db_root;
    const char       *object;
    const Schema     *sch;
    SearchCriterion  *primary_crit;
    int               check_primary;
    const TypedField *tf;
    CriteriaNode     *tree;
    FieldSchema      *fs;
    ExcludedKeys     *excluded;
    const char      **proj_fields;
    int               proj_count;
    int               rows_fmt;
    int               dict_fmt;
    char              csv_delim;
    int               offset;
    int               limit;
    QueryDeadline    *deadline;
    FILE             *parent_out;

    /* Batch-fetch buffer. */
    BatchFetchBuf     bfb;

    /* Mutable shared state. */
    pthread_mutex_t   lock;
    int               passed;     /* records that passed both filters */
    int               printed;    /* records actually emitted */
    int               stop;       /* atomic — set when printed >= limit */
} StreamFindCtx;

static int stream_find_record_cb(const uint8_t hash16[16],
                                  const void *key, size_t klen,
                                  const void *value, size_t vlen,
                                  void *ctx) {
    (void)hash16; (void)vlen;
    StreamFindCtx *sc = (StreamFindCtx *)ctx;
    g_out = sc->parent_out;

    char keybuf[1100];
    {
        const Schema *sc_p = (sc->fs && sc->fs->auto_key != AK_NONE)
                              ? &sc->fs->auto_key_schema_snapshot : NULL;
        format_wire_key(sc_p, (const char *)key, klen, keybuf, sizeof(keybuf));
    }

    if (is_excluded(sc->excluded, keybuf)) return 0;

    if (!criteria_match_tree(value, sc->tree, sc->fs)) return 0;

    pthread_mutex_lock(&sc->lock);
    int my_seq = ++sc->passed;
    int will_emit = (my_seq > sc->offset &&
                     (sc->limit <= 0 || sc->printed < sc->limit));
    if (will_emit) {
        const uint8_t *raw = (const uint8_t *)value;
        if (sc->csv_delim) {
            csv_emit_row(keybuf, raw, (uint32_t)vlen,
                          sc->proj_count > 0 ? sc->proj_fields : NULL,
                          sc->proj_count, sc->fs, sc->csv_delim);
        } else if (sc->dict_fmt) {
            OUT("%s\"%s\":", sc->printed ? "," : "", keybuf);
            if (sc->proj_count > 0) {
                OUT("{");
                int first = 1;
                for (int j = 0; j < sc->proj_count; j++) {
                    char *pv = json_escape_field(decode_field((const char *)raw, (uint32_t)vlen,
                                             sc->proj_fields[j], sc->fs));
                    if (!pv) continue;
                    OUT("%s\"%s\":\"%s\"", first ? "" : ",", sc->proj_fields[j], pv);
                    first = 0;
                    free(pv);
                }
                OUT("}");
            } else {
                char *v = decode_value((const char *)raw, (uint32_t)vlen, sc->fs);
                OUT("%s", v);
                free(v);
            }
        } else if (sc->proj_count > 0) {
            OUT("%s{\"key\":\"%s\",\"value\":{", sc->printed ? "," : "", keybuf);
            int first = 1;
            for (int j = 0; j < sc->proj_count; j++) {
                char *pv = json_escape_field(decode_field((const char *)raw, (uint32_t)vlen,
                                         sc->proj_fields[j], sc->fs));
                if (!pv) continue;
                OUT("%s\"%s\":\"%s\"", first ? "" : ",", sc->proj_fields[j], pv);
                first = 0;
                free(pv);
            }
            OUT("}}");
        } else {
            char *v = decode_value((const char *)raw, (uint32_t)vlen, sc->fs);
            OUT("%s{\"key\":\"%s\",\"value\":%s}", sc->printed ? "," : "", keybuf, v);
            free(v);
        }
        sc->printed++;
    }
    int done = (sc->limit > 0 && sc->printed >= sc->limit);
    if (done) __atomic_store_n(&sc->stop, 1, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&sc->lock);
    return done ? -1 : 0;
}

static int stream_find_cb(const char *val, size_t vlen, const uint8_t *hash16, void *raw_ctx) {
    StreamFindCtx *sc = (StreamFindCtx *)raw_ctx;
    if (__atomic_load_n(&sc->stop, __ATOMIC_ACQUIRE)) return -1;

    BatchFetchBuf *bfb = &sc->bfb;
    g_out = sc->parent_out;

    /* Primary check (LEN_*, like patterns where check_primary == 1). */
    if (sc->primary_crit && op_is_length(sc->primary_crit->op)) {
        if (!match_length_vlen(vlen, sc->primary_crit)) return 0;
    } else if (sc->check_primary && sc->primary_crit) {
        char tmp[1028];
        int matched;
        if (sc->tf) {
            int dlen = decode_idx_to_buf(sc->tf, (const uint8_t*)val, vlen,
                                          tmp, sizeof(tmp), 0);
            if (dlen <= 0) return 0;
            matched = match_criterion(tmp, sc->primary_crit);
        } else {
            size_t cl = vlen < sizeof(tmp) - 1 ? vlen : sizeof(tmp) - 1;
            memcpy(tmp, val, cl); tmp[cl] = '\0';
            matched = match_criterion_vlen(tmp, cl, sc->primary_crit);
        }
        if (!matched) return 0;
    }

    return batch_buf_collect_hash(bfb, hash16);
}

/* Returns number of rows printed, or -2 on per-query buffer cap overrun
   (currently unused — streaming has no buffer growth). Caller has
   already emitted the JSON envelope opener (`[`) before calling. */
static int idx_find_streaming(const char *db_root, const char *object,
                               const Schema *sch,
                               SearchCriterion *primary_crit, int check_primary,
                               CriteriaNode *tree,
                               ExcludedKeys *excluded,
                               int offset, int limit,
                               const char **proj_fields, int proj_count,
                               FieldSchema *fs,
                               int rows_fmt, int dict_fmt, char csv_delim,
                               QueryDeadline *dl) {
    SlotcaskSchemaInfo sinfo = { .splits = sch->splits,
                                 .slot_size = sch->slot_size,
                                 .streams = sch->streams };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);

    StreamFindCtx sc = {0};
    sc.db_root = db_root;
    sc.object = object;
    sc.sch = sch;
    sc.primary_crit = primary_crit;
    sc.check_primary = check_primary;
    sc.tree = tree;
    sc.fs = fs;
    sc.excluded = excluded;
    sc.proj_fields = proj_fields;
    sc.proj_count = proj_count;
    sc.rows_fmt = rows_fmt;
    sc.dict_fmt = dict_fmt;
    sc.csv_delim = csv_delim;
    sc.offset = offset;
    sc.limit = limit;
    sc.deadline = dl;
    sc.parent_out = g_out;
    sc.tf = resolve_idx_field(fs ? fs->ts : NULL, primary_crit->field);
    pthread_mutex_init(&sc.lock, NULL);

    if (batch_buf_init(&sc.bfb, sdb, sch->slot_size, limit) != 0) {
        pthread_mutex_destroy(&sc.lock);
        return 0;
    }
    sc.bfb.record_cb = stream_find_record_cb;
    sc.bfb.record_ctx = &sc;

    btree_dispatch(db_root, object, primary_crit->field, sch->splits,
                    primary_crit,
                    resolve_idx_field(fs ? fs->ts : NULL, primary_crit->field),
                    stream_find_cb, &sc);

    batch_buf_destroy(&sc.bfb);
    pthread_mutex_destroy(&sc.lock);
    return sc.printed;
}

/* ============================================================
   D1 executor: composite-prefix sorted scan.

   Given a composite (a+b) btree, its leaf values are
   encoded(a) ++ encoded(b) ++ hash16.  Walking the range
   [encoded(a_val), encoded(a_val) ++ 0xff×4) in btree order
   yields every record whose `a` field == a_val, with the
   resulting entries already sorted by the encoded `b` value
   within that prefix.  This gives an O(limit) ordered answer
   for `find {a="alice"} order_by b limit N` — no in-memory
   sort, no full-shard scan.

   Constraints checked by caller:
     • fp.order == FP_ORDER_COMPOSITE && fp.kind == FP_PRIMARY_LEAF
     • no joins (need tabular materialise)
     • no rows_fmt / csv (column-headers need full materialise)
   Falls through (caller just skips this fn) for any edge case
   the planner didn't cover.
*/

typedef struct {
    const char    *db_root;
    const char    *object;
    const Schema  *sch;
    FieldSchema   *fs;
    CriteriaNode  *tree;          /* full tree for post-filter */
    ExcludedKeys  *excluded;
    const char   **proj_fields;
    int            proj_count;
    int            dict_fmt;
    int            skip_remaining;
    int            limit;
    int            printed;
    QueryDeadline *dl;
    int            dl_counter;
    FILE          *parent_out;    /* caller's socket stream */
    pthread_mutex_t lock;          /* guards printed/skip_remaining in batch path */
} CompositePrefixCtx;

static int composite_prefix_cb(const char *val, size_t vlen,
                                const uint8_t *hash16, void *ctx) {
#ifdef TEST_BUILD
    extern long g_order_walk_scanned;
    g_order_walk_scanned++;
#endif
    (void)val; (void)vlen;   /* composite leaf value not needed; hash16 is the key */
    CompositePrefixCtx *c = (CompositePrefixCtx *)ctx;
    g_out = c->parent_out;
    if (query_deadline_tick(c->dl, &c->dl_counter)) return -1;
    if (c->printed >= c->limit) return -1;

    /* Fetch the record by hash16. */
    RecordRef rr;
    if (read_record_ref(c->db_root, c->object, c->sch, hash16, &rr) != 0) return 0;
    const uint8_t *key_start = rr.key;
    const uint8_t *raw       = rr.val;
    uint32_t       value_len = (uint32_t)rr.vlen;

    /* Excluded-keys check: the key is a string in ExcludedKeys.
       is_excluded() takes a char* key so build it from the fetched record. */
    if (c->excluded && c->excluded->count > 0) {
        char keybuf[1024];
        size_t klen = rr.klen < sizeof(keybuf) - 1 ? rr.klen : sizeof(keybuf) - 1;
        memcpy(keybuf, key_start, klen); keybuf[klen] = '\0';
        if (is_excluded(c->excluded, keybuf)) {
            release_record_ref(&rr);
            return 0;
        }
    }

    /* Post-filter: apply the full criteria tree (seed leaf trivially passes
       since we're inside its prefix; any extra AND leaves still need checking). */
    if (c->tree && !criteria_match_tree(raw, c->tree, c->fs)) {
        release_record_ref(&rr);
        return 0;
    }

    /* Offset skip-after-match */
    if (c->skip_remaining > 0) {
        c->skip_remaining--;
        release_record_ref(&rr);
        return 0;
    }

    /* Emit the row.  Supports default JSON and dict_fmt; rows_fmt/csv are
       excluded by the caller guard so we don't need those branches here. */
    char key_buf[1024];
    size_t klen = rr.klen < sizeof(key_buf) - 1 ? rr.klen : sizeof(key_buf) - 1;
    memcpy(key_buf, key_start, klen);
    key_buf[klen] = '\0';

    if (c->dict_fmt) {
        OUT("%s\"%s\":", c->printed ? "," : "", key_buf);
        if (c->proj_count > 0) {
            OUT("{");
            int first = 1;
            for (int i = 0; i < c->proj_count; i++) {
                char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                        c->proj_fields[i], c->fs));
                if (!pv) continue;
                OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
                first = 0;
                free(pv);
            }
            OUT("}");
        } else {
            char *dv = decode_value((const char *)raw, value_len, c->fs);
            OUT("%s", dv ? dv : "{}");
            free(dv);
        }
    } else if (c->proj_count > 0) {
        OUT("%s{\"key\":\"%s\",\"value\":{", c->printed ? "," : "", key_buf);
        int first = 1;
        for (int i = 0; i < c->proj_count; i++) {
            char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                    c->proj_fields[i], c->fs));
            if (!pv) continue;
            OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
            first = 0;
            free(pv);
        }
        OUT("}}");
    } else {
        char *dv = decode_value((const char *)raw, value_len, c->fs);
        OUT("%s{\"key\":\"%s\",\"value\":%s}",
            c->printed ? "," : "", key_buf, dv ? dv : "{}");
        free(dv);
    }

    c->printed++;
    release_record_ref(&rr);
    return (c->printed >= c->limit) ? -1 : 0;
}

static int composite_prefix_record_cb(const uint8_t hash16[16],
                                       const void *key, size_t klen,
                                       const void *value, size_t vlen,
                                       void *ctx) {
    (void)hash16; (void)vlen;
    CompositePrefixCtx *c = (CompositePrefixCtx *)ctx;
    g_out = c->parent_out;

    if (c->excluded && c->excluded->count > 0) {
        char keybuf[1024];
        size_t kl = klen < sizeof(keybuf) - 1 ? klen : sizeof(keybuf) - 1;
        memcpy(keybuf, key, kl); keybuf[kl] = '\0';
        if (is_excluded(c->excluded, keybuf)) return 0;
    }

    if (c->tree && !criteria_match_tree(value, c->tree, c->fs)) return 0;

    pthread_mutex_lock(&c->lock);
    if (c->skip_remaining > 0) {
        c->skip_remaining--;
        pthread_mutex_unlock(&c->lock);
        return 0;
    }

    char key_buf[1024];
    size_t kl = klen < sizeof(key_buf) - 1 ? klen : sizeof(key_buf) - 1;
    memcpy(key_buf, key, kl);
    key_buf[kl] = '\0';

    if (c->dict_fmt) {
        OUT("%s\"%s\":", c->printed ? "," : "", key_buf);
        if (c->proj_count > 0) {
            OUT("{");
            int first = 1;
            for (int i = 0; i < c->proj_count; i++) {
                char *pv = json_escape_field(decode_field((const char *)value, (uint32_t)vlen,
                                        c->proj_fields[i], c->fs));
                if (!pv) continue;
                OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
                first = 0;
                free(pv);
            }
            OUT("}");
        } else {
            char *dv = decode_value((const char *)value, (uint32_t)vlen, c->fs);
            OUT("%s", dv ? dv : "{}");
            free(dv);
        }
    } else if (c->proj_count > 0) {
        OUT("%s{\"key\":\"%s\",\"value\":{", c->printed ? "," : "", key_buf);
        int first = 1;
        for (int i = 0; i < c->proj_count; i++) {
            char *pv = json_escape_field(decode_field((const char *)value, (uint32_t)vlen,
                                    c->proj_fields[i], c->fs));
            if (!pv) continue;
            OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
            first = 0;
            free(pv);
        }
        OUT("}}");
    } else {
        char *dv = decode_value((const char *)value, (uint32_t)vlen, c->fs);
        OUT("%s{\"key\":\"%s\",\"value\":%s}",
            c->printed ? "," : "", key_buf, dv ? dv : "{}");
        free(dv);
    }

    c->printed++;
    int done = (c->limit > 0 && c->printed >= c->limit);
    pthread_mutex_unlock(&c->lock);
    return done ? -1 : 0;
}

/* K-way merge cursor for composite prefix walks.
 * Replicates the per-shard merge from index.c but supports multiple
 * (lo,hi) range pairs — one per IN value — in a single heap. */
typedef struct {
    BtRangeIter *iter;
    char    value[BT_MAX_VAL_LEN];
    size_t  vlen;
    uint8_t hash[BT_HASH_SIZE];
    int     has_entry;
    int     stream_id;  /* which IN value this cursor belongs to */
} CompMergeCursor;

static int comp_cursor_cmp(const CompMergeCursor *a, const CompMergeCursor *b) {
    size_t m = a->vlen < b->vlen ? a->vlen : b->vlen;
    int r = memcmp(a->value, b->value, m);
    if (r != 0) return r;
    if (a->vlen != b->vlen) return a->vlen < b->vlen ? -1 : 1;
    r = memcmp(a->hash, b->hash, BT_HASH_SIZE);
    if (r != 0) return r;
    return a->stream_id - b->stream_id;
}

static void comp_cursor_pull(CompMergeCursor *c) {
    const char *v; size_t vl; const uint8_t *h;
    if (btree_range_iter_next(c->iter, &v, &vl, &h)) {
        c->vlen = vl > BT_MAX_VAL_LEN ? BT_MAX_VAL_LEN : vl;
        memcpy(c->value, v, c->vlen);
        memcpy(c->hash, h, BT_HASH_SIZE);
        c->has_entry = 1;
    } else {
        c->has_entry = 0;
    }
}

static void comp_merge_sift_down(int *heap, int n, int i,
                                 const CompMergeCursor *cursors, int desc) {
    for (;;) {
        int l = 2*i+1, r = 2*i+2, best = i;
        if (l < n) {
            int d = desc ? -comp_cursor_cmp(&cursors[heap[l]], &cursors[heap[best]])
                         : comp_cursor_cmp(&cursors[heap[l]], &cursors[heap[best]]);
            if (d < 0) best = l;
        }
        if (r < n) {
            int d = desc ? -comp_cursor_cmp(&cursors[heap[r]], &cursors[heap[best]])
                         : comp_cursor_cmp(&cursors[heap[r]], &cursors[heap[best]]);
            if (d < 0) best = r;
        }
        if (best == i) return;
        int t = heap[i]; heap[i] = heap[best]; heap[best] = t;
        i = best;
    }
}

/* D1 executor entry point.  seed is the equality leaf on the prefix field
   (e.g. by="alice"); order_by is the sort field (e.g. "time").
   Returns number of rows emitted (≥0) or -1 on deadline. */
static int find_via_composite_prefix(const char *db_root, const char *object,
                                     const Schema *sch, FieldSchema *fs,
                                     SearchCriterion *seed,
                                     const char *order_by,
                                     int order_desc,
                                     SearchCriterion *order_range,   /* NULL = whole-prefix walk */
                                     CriteriaNode *tree,
                                     ExcludedKeys *excluded,
                                     int offset, int limit,
                                     const char **proj_fields, int proj_count,
                                     int dict_fmt,
                                     QueryDeadline *dl)
{
    /* Composite index name = <seed_field>+<order_by>. */
    char composite_field[256];
    snprintf(composite_field, sizeof(composite_field), "%s+%s",
             seed->field, order_by);

    /* (C). OP_IN: k-way merge across per-value composite prefix ranges.
     * Each IN value gets its own bounded sub-range; per-shard BtRangeIters
     * from all values are merged in a single heap so the emitted stream is
     * globally ordered by (encoded_value ‖ encoded_order_by). */
    if (seed->op == OP_IN && seed->in_count > 0) {
        int nv = seed->in_count;
        int ns = index_splits_for(sch->splits);
        CompMergeCursor *cursors = calloc((size_t)(ns * nv), sizeof(CompMergeCursor));
        int *heap = calloc((size_t)(ns * nv), sizeof(int));
        if (!cursors || !heap) { free(cursors); free(heap); return 0; }

        const TypedField *seed_tf = resolve_idx_field(fs ? fs->ts : NULL, seed->field);

        int nh = 0, ci = 0;
        for (int iv = 0; iv < nv; iv++) {
            uint8_t lo[1024 + 8]; size_t len_lo = 0;
            encode_criterion_value(seed_tf, seed->in_values[iv],
                                   strlen(seed->in_values[iv]), lo, &len_lo);
            size_t pfx = len_lo;
            uint8_t hi[1024 + 8];
            memcpy(hi, lo, len_lo);
            size_t len_hi;
            if (len_lo > 0 && (!seed_tf || len_lo < (size_t)seed_tf->size)) {
                int pos = (int)len_lo - 1;
                while (pos >= 0 && lo[pos] == 0xff) pos--;
                if (pos >= 0) {
                    memcpy(hi, lo, (size_t)pos);
                    hi[pos] = lo[pos] + 1;
                    len_hi = (size_t)pos + 1;
                } else {
                    memcpy(hi, lo, len_lo);
                    hi[len_lo] = 0x00;
                    len_hi = len_lo + 1;
                }
            } else {
                memset(hi + len_lo, 0xff, 4);
                len_hi = len_lo + 4;
            }
            int min_excl = 0, max_excl = 0;

            /* order_range fold */
            if (order_range) {
                const TypedField *ord_tf = resolve_idx_field(fs ? fs->ts : NULL, order_by);
                const char *lowv = NULL; int low_excl = 0;
                const char *highv = NULL; int high_excl = 0;
                switch (order_range->op) {
                    case OP_GREATER_EQ: lowv = order_range->value; break;
                    case OP_GREATER:    lowv = order_range->value; low_excl = 1; break;
                    case OP_LESS_EQ:    highv = order_range->value; break;
                    case OP_LESS:       highv = order_range->value; high_excl = 1; break;
                    case OP_EQUAL:      lowv = highv = order_range->value; break;
                    case OP_BETWEEN:
                        lowv  = order_range->value;  low_excl  = order_range->min_exclusive;
                        highv = order_range->value2; high_excl = order_range->max_exclusive;
                        break;
                    default: break;
                }
                if (lowv) {
                    uint8_t enc[1024]; size_t el = 0;
                    encode_criterion_value(ord_tf, lowv, strlen(lowv), enc, &el);
                    if (pfx + el <= sizeof(lo)) {
                        memcpy(lo + pfx, enc, el); len_lo = pfx + el;
                        min_excl = low_excl;
                    }
                }
                if (highv) {
                    uint8_t enc[1024]; size_t el = 0;
                    encode_criterion_value(ord_tf, highv, strlen(highv), enc, &el);
                    if (pfx + el <= sizeof(hi)) {
                        memcpy(hi + pfx, enc, el); len_hi = pfx + el;
                        max_excl = high_excl;
                    }
                }
            }

            for (int s = 0; s < ns; s++) {
                char ip[PATH_MAX];
                build_idx_path(ip, sizeof(ip), db_root, object, composite_field, s);
                cursors[ci].iter = btree_range_iter_open(ip,
                                     (const char *)lo, len_lo, min_excl,
                                     (const char *)hi, len_hi, max_excl,
                                     order_desc);
                cursors[ci].stream_id = iv;
                if (cursors[ci].iter) comp_cursor_pull(&cursors[ci]);
                if (cursors[ci].has_entry) heap[nh++] = ci;
                ci++;
            }
        }

        int total_cursors = ci;

        for (int i = nh/2-1; i >= 0; i--)
            comp_merge_sift_down(heap, nh, i, cursors, order_desc);

        CompositePrefixCtx ctx;
        memset(&ctx, 0, sizeof(ctx));
        ctx.db_root = db_root; ctx.object = object; ctx.sch = sch; ctx.fs = fs;
        ctx.tree = tree; ctx.excluded = excluded;
        ctx.proj_fields = proj_fields; ctx.proj_count = proj_count;
        ctx.dict_fmt = dict_fmt;
        ctx.skip_remaining = (offset > 0) ? offset : 0;
        ctx.limit = (limit > 0)  ? limit  : INT_MAX;
        ctx.dl = dl; ctx.dl_counter = 0; ctx.parent_out = g_out;
        pthread_mutex_init(&ctx.lock, NULL);

        while (nh > 0) {
            CompMergeCursor *bc = &cursors[heap[0]];
            if (composite_prefix_cb(bc->value, bc->vlen, bc->hash, &ctx) < 0) break;
            comp_cursor_pull(bc);
            if (bc->has_entry) {
                comp_merge_sift_down(heap, nh, 0, cursors, order_desc);
            } else {
                heap[0] = heap[--nh];
                if (nh > 0) comp_merge_sift_down(heap, nh, 0, cursors, order_desc);
            }
        }

        pthread_mutex_destroy(&ctx.lock);
        for (int i = 0; i < total_cursors; i++)
            if (cursors[i].iter) btree_range_iter_close(cursors[i].iter);
        free(cursors); free(heap);
        return ctx.printed;
    }

    /* (single-value). Seed prefix = encode(seed value). */
    uint8_t buf_lo_sv[1024 + 8];
    size_t  len_lo_sv = 0;
    const TypedField *seed_tf_sv = resolve_idx_field(fs ? fs->ts : NULL, seed->field);
    encode_criterion_value(seed_tf_sv, seed->value, strlen(seed->value), buf_lo_sv, &len_lo_sv);
    size_t pfx_len = len_lo_sv;

    /* 2. Upper bound: byte-successor of the seed prefix.
       For fixed-width types: append 0xff bytes (original STARTS_WITH idiom).
       For variable-length encodings (VARCHAR or raw bytes): increment the last
       byte < 0xff, or append 0x00 if all 0xff, producing a tight exclusive
       bound.  The old 0xff*4 approach creates an over-wide range for
       variable-length keys because shorter prefix + 0xff can let non-matching
       values through.  Detected by comparing the encoded length to the field's
       fixed storage size (seed_tf_sv->size); when they match, the encoding is
       fixed-width; when encoded < size or seed_tf_sv is NULL (raw), it's
       variable-length. */
    uint8_t buf_hi[1024 + 8];
    memcpy(buf_hi, buf_lo_sv, len_lo_sv);
    size_t len_hi;
    if (len_lo_sv > 0 && (!seed_tf_sv || len_lo_sv < (size_t)seed_tf_sv->size)) {
        int pos = (int)len_lo_sv - 1;
        while (pos >= 0 && buf_lo_sv[pos] == 0xff) pos--;
        if (pos >= 0) {
            memcpy(buf_hi, buf_lo_sv, (size_t)pos);
            buf_hi[pos] = buf_lo_sv[pos] + 1;
            len_hi = (size_t)pos + 1;
        } else {
            memcpy(buf_hi, buf_lo_sv, len_lo_sv);
            buf_hi[len_lo_sv] = 0x00;
            len_hi = len_lo_sv + 1;
        }
    } else {
        memset(buf_hi + len_lo_sv, 0xff, 4);
        len_hi = len_lo_sv + 4;
    }
    int min_excl = 0, max_excl = 0;

    /* 2b. If an order_by range/eq leaf is present (EQ seed only — guaranteed
       by the planner), append its encoded value(s) to the seed prefix so the
       btree seeks to T instead of walking the whole prefix and post-filtering.
       Composite key = encode(seed) ‖ encode(order_by); appending encode(T)
       after the (fixed-for-EQ) seed prefix is the exact seek point. */
    if (order_range) {
        const TypedField *ord_tf = resolve_idx_field(fs ? fs->ts : NULL, order_by);
        /* low bound: >=, >, BETWEEN-low, or == */
        const char *lowv = NULL; int low_excl = 0;
        const char *highv = NULL; int high_excl = 0;
        switch (order_range->op) {
            case OP_GREATER_EQ: lowv = order_range->value; break;
            case OP_GREATER:    lowv = order_range->value; low_excl = 1; break;
            case OP_LESS_EQ:    highv = order_range->value; break;
            case OP_LESS:       highv = order_range->value; high_excl = 1; break;
            case OP_EQUAL:      lowv = highv = order_range->value; break;
            case OP_BETWEEN:
                lowv  = order_range->value;  low_excl  = order_range->min_exclusive;
                highv = order_range->value2; high_excl = order_range->max_exclusive;
                break;
            default: break;   /* leave whole-prefix bounds */
        }
        if (lowv) {
            uint8_t enc[1024]; size_t el = 0;
            encode_criterion_value(ord_tf, lowv, strlen(lowv), enc, &el);
            if (pfx_len + el <= sizeof(buf_lo_sv)) {
                memcpy(buf_lo_sv + pfx_len, enc, el);
                len_lo_sv = pfx_len + el;
                min_excl = low_excl;
            }
        }
        if (highv) {
            uint8_t enc[1024]; size_t el = 0;
            encode_criterion_value(ord_tf, highv, strlen(highv), enc, &el);
            if (pfx_len + el <= sizeof(buf_hi)) {
                memcpy(buf_hi + pfx_len, enc, el);
                len_hi = pfx_len + el;
                max_excl = high_excl;
            }
        }
    }

    /* 4. Set up the callback context. */
    CompositePrefixCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.db_root        = db_root;
    ctx.object         = object;
    ctx.sch            = sch;
    ctx.fs             = fs;
    ctx.tree           = tree;
    ctx.excluded       = excluded;
    ctx.proj_fields    = proj_fields;
    ctx.proj_count     = proj_count;
    ctx.dict_fmt       = dict_fmt;
    ctx.skip_remaining = (offset > 0) ? offset : 0;
    ctx.limit          = (limit > 0)  ? limit  : INT_MAX;
    ctx.printed        = 0;
    ctx.dl             = dl;
    ctx.dl_counter     = 0;
    ctx.parent_out     = g_out;
    pthread_mutex_init(&ctx.lock, NULL);

    /* 5. Walk the composite btree.  The k-way merge across index shards
          delivers entries already sorted by (encoded_a || encoded_b) in
          the requested direction — asc or desc. */
    btree_idx_walk_ordered(db_root, object, composite_field, sch->splits,
                           (const char *)buf_lo_sv, len_lo_sv, min_excl,
                           (const char *)buf_hi, len_hi, max_excl,
                           order_desc, composite_prefix_cb, &ctx);

    pthread_mutex_destroy(&ctx.lock);
    return ctx.printed;
}

/* Encoded order-by walk bounds derived from the order_by-field range/eq leaves.
 * Defaults to the full range ("" .. 0xffffffff). Only TOP-LEVEL AND leaves are
 * consulted (OR/nested branches can't bound the walk). The walk's per-record
 * criteria_match_tree still runs, so a loose/absent bound never changes the
 * result set — it only narrows how far the index is walked. */
/* OrderWalkBounds and order_walk_bounds are defined in query_internal.h */

/* collect_and_leaves is declared in query_internal.h */

void order_walk_bounds(CriteriaNode *tree, FieldSchema *fs,
                       const char *order_by, OrderWalkBounds *b) {
    memset(b, 0, sizeof(*b));
    /* defaults: lo = "" (len 0), hi = 4 x 0xff */
    memset(b->hi, 0xff, 4); b->hi_len = 4;
    if (!order_by || !order_by[0]) return;

    const TypedField *tf = resolve_idx_field(fs ? fs->ts : NULL, order_by);
    if (!tf) return;

    SearchCriterion *leaves[MAX_INTERSECT_LEAVES];
    int nL = collect_and_leaves(tree, leaves, MAX_INTERSECT_LEAVES);
    for (int i = 0; i < nL; i++) {
        if (strcmp(leaves[i]->field, order_by) != 0) continue;
        const char *lowv = NULL;  int low_excl = 0;
        const char *highv = NULL; int high_excl = 0;
        switch (leaves[i]->op) {
            case OP_GREATER_EQ: lowv = leaves[i]->value; break;
            case OP_GREATER:    lowv = leaves[i]->value; low_excl = 1; break;
            case OP_LESS_EQ:    highv = leaves[i]->value; break;
            case OP_LESS:       highv = leaves[i]->value; high_excl = 1; break;
            case OP_EQUAL:      lowv = highv = leaves[i]->value; break;
            case OP_BETWEEN:
                lowv  = leaves[i]->value;  low_excl  = leaves[i]->min_exclusive;
                highv = leaves[i]->value2; high_excl = leaves[i]->max_exclusive;
                break;
            default: break;   /* not a range/eq op → no bound */
        }
        if (lowv) {
            uint8_t enc[1024]; size_t el = 0;
            encode_criterion_value(tf, lowv, strlen(lowv), enc, &el);
            /* Keep the TIGHTEST lower bound (largest encoded value). First one
             * always wins over the "" default. memcmp is valid because the
             * index encoding is order-preserving. */
            if (el > 0 && (!b->has_lo ||
                           el > b->lo_len ||
                           (el == b->lo_len && memcmp(enc, b->lo, el) > 0))) {
                memcpy(b->lo, enc, el); b->lo_len = el; b->lo_excl = low_excl;
                b->has_lo = 1;
            }
        }
        if (highv) {
            uint8_t enc[1024]; size_t el = 0;
            encode_criterion_value(tf, highv, strlen(highv), enc, &el);
            /* Keep the TIGHTEST upper bound (smallest encoded value). */
            if (el > 0 && (!b->has_hi ||
                           el < b->hi_len ||
                           (el == b->hi_len && memcmp(enc, b->hi, el) < 0))) {
                memcpy(b->hi, enc, el); b->hi_len = el; b->hi_excl = high_excl;
                b->has_hi = 1;
            }
        }
    }
}

/* Forward declarations (defined later in this file). */
/* build_exact_composite_key is declared in query_internal.h */

/* Exact composite lookup: walk the composite btree for the single concatenated
 * key built from the eq leaves. composite_prefix_cb fetches each hash and
 * post-filters the full tree (so any non-composite siblings still apply). */
static int find_via_composite_key(const char *db_root, const char *object,
                                  const Schema *sch, FieldSchema *fs,
                                  const char *composite_field,
                                  SearchCriterion **eq_leaves, int n_eq,
                                  CriteriaNode *tree, ExcludedKeys *excluded,
                                  int offset, int limit,
                                  const char **proj_fields, int proj_count,
                                  int dict_fmt, QueryDeadline *dl) {
    /* Rebuild the exact key in composite field order. eq_leaves are already in
     * that order (planner filled them by walking the composite name). */
    uint8_t key[1024]; size_t klen = 0;
    if (!build_exact_composite_key(fs, composite_field, eq_leaves, n_eq, key, &klen)
        || klen == 0)
        return 0;

    SlotcaskSchemaInfo sinfo = { .splits = sch->splits,
                                 .slot_size = sch->slot_size,
                                 .streams = sch->streams };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);

    CompositePrefixCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.db_root = db_root; ctx.object = object; ctx.sch = sch; ctx.fs = fs;
    ctx.tree = tree; ctx.excluded = excluded;
    ctx.proj_fields = proj_fields; ctx.proj_count = proj_count;
    ctx.dict_fmt = dict_fmt;
    ctx.skip_remaining = (offset > 0) ? offset : 0;
    ctx.limit = (limit > 0) ? limit : INT_MAX;
    ctx.dl = dl; ctx.parent_out = g_out;
    pthread_mutex_init(&ctx.lock, NULL);

    BatchFetchBuf bfb;
    if (batch_buf_init(&bfb, sdb, sch->slot_size, limit) == 0) {
        bfb.record_cb = composite_prefix_record_cb;
        bfb.record_ctx = &ctx;
        btree_idx_search(db_root, object, composite_field, sch->splits,
                         (const char *)key, klen, batch_buf_collect_cb, &bfb);
        batch_buf_destroy(&bfb);
    }

    pthread_mutex_destroy(&ctx.lock);
    return ctx.printed;
}

/* ============================================================
   D3 executor: order-index walk + per-record post-filter.

   Walk the order_by btree across its full range in the
   requested direction (asc or desc).  For each candidate,
   fetch the record and run the full criteria tree.  Emit
   matches until limit is reached, applying offset along the
   way.  Cost is O(limit + records-walked-before-limit-fills),
   which is dramatically cheaper than scan-and-sort-millions
   when the broad predicate matches most records (e.g. the
   feed/profile shape: score>50 order_by time limit 20).

   Key difference from D1: the walk index is the order_by
   field alone (not a composite), and the criteria tree is
   checked in full for every fetched record — no leaf is
   guaranteed-matching by the walk range.

   Constraints checked by caller:
     • fp.order == FP_ORDER_INDEX_WALK
     • no joins, no rows_fmt, no csv_delim
*/

typedef struct {
    const char    *db_root;
    const char    *object;
    const Schema  *sch;
    FieldSchema   *fs;
    CriteriaNode  *tree;          /* full tree — checked per record */
    ExcludedKeys  *excluded;
    const char   **proj_fields;
    int            proj_count;
    int            dict_fmt;
    int            skip_remaining;
    int            limit;
    int            printed;
    QueryDeadline *dl;
    int            dl_counter;
    FILE          *parent_out;
} OrderIndexWalkCtx;

#ifdef TEST_BUILD
/* Counts index entries visited by the order-by walks, so a test can prove a
 * windowed query stops at the window instead of scanning the whole index. */
long g_order_walk_scanned = 0;
long order_walk_scanned_for_test(void)   { return g_order_walk_scanned; }
void order_walk_scanned_reset_for_test(void) { g_order_walk_scanned = 0; }
#endif

static int order_index_walk_cb(const char *val, size_t vlen,
                                const uint8_t *hash16, void *ctx_ptr) {
#ifdef TEST_BUILD
    g_order_walk_scanned++;
#endif
    (void)val; (void)vlen;   /* walk value not needed; hash16 is the record key */
    OrderIndexWalkCtx *c = (OrderIndexWalkCtx *)ctx_ptr;
    g_out = c->parent_out;
    if (query_deadline_tick(c->dl, &c->dl_counter)) return -1;
    if (c->printed >= c->limit) return -1;

    /* Fetch the record by hash16. */
    RecordRef rr;
    if (read_record_ref(c->db_root, c->object, c->sch, hash16, &rr) != 0) return 0;
    const uint8_t *key_start = rr.key;
    const uint8_t *raw       = rr.val;
    uint32_t       value_len = (uint32_t)rr.vlen;

    /* Excluded-keys check. */
    if (c->excluded && c->excluded->count > 0) {
        char keybuf[1024];
        size_t klen = rr.klen < sizeof(keybuf) - 1 ? rr.klen : sizeof(keybuf) - 1;
        memcpy(keybuf, key_start, klen); keybuf[klen] = '\0';
        if (is_excluded(c->excluded, keybuf)) {
            release_record_ref(&rr);
            return 0;
        }
    }

    /* Post-filter: apply the FULL criteria tree — unlike D1 nothing is
       guaranteed to match by the walk range alone. */
    if (c->tree && !criteria_match_tree(raw, c->tree, c->fs)) {
        release_record_ref(&rr);
        return 0;
    }

    /* Offset skip-after-match. */
    if (c->skip_remaining > 0) {
        c->skip_remaining--;
        release_record_ref(&rr);
        return 0;
    }

    /* Emit the row.  Supports default JSON and dict_fmt; rows_fmt/csv are
       excluded by the caller guard so we don't need those branches here. */
    char key_buf[1024];
    size_t klen = rr.klen < sizeof(key_buf) - 1 ? rr.klen : sizeof(key_buf) - 1;
    memcpy(key_buf, key_start, klen);
    key_buf[klen] = '\0';

    if (c->dict_fmt) {
        OUT("%s\"%s\":", c->printed ? "," : "", key_buf);
        if (c->proj_count > 0) {
            OUT("{");
            int first = 1;
            for (int i = 0; i < c->proj_count; i++) {
                char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                        c->proj_fields[i], c->fs));
                if (!pv) continue;
                OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
                first = 0;
                free(pv);
            }
            OUT("}");
        } else {
            char *dv = decode_value((const char *)raw, value_len, c->fs);
            OUT("%s", dv ? dv : "{}");
            free(dv);
        }
    } else if (c->proj_count > 0) {
        OUT("%s{\"key\":\"%s\",\"value\":{", c->printed ? "," : "", key_buf);
        int first = 1;
        for (int i = 0; i < c->proj_count; i++) {
            char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                    c->proj_fields[i], c->fs));
            if (!pv) continue;
            OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
            first = 0;
            free(pv);
        }
        OUT("}}");
    } else {
        char *dv = decode_value((const char *)raw, value_len, c->fs);
        OUT("%s{\"key\":\"%s\",\"value\":%s}",
            c->printed ? "," : "", key_buf, dv ? dv : "{}");
        free(dv);
    }

    c->printed++;
    release_record_ref(&rr);
    return (c->printed >= c->limit) ? -1 : 0;
}

/* D3 executor entry point.  order_by is the indexed sort field (e.g. "time").
   Walk the full btree range in the requested direction; post-filter each
   fetched record against the full criteria tree.
   Returns number of rows emitted (≥0) or -1 on deadline. */
static int find_via_order_index_walk(const char *db_root, const char *object,
                                     const Schema *sch, FieldSchema *fs,
                                     const char *order_by, int order_desc,
                                     CriteriaNode *tree,
                                     ExcludedKeys *excluded,
                                     int offset, int limit,
                                     const char **proj_fields, int proj_count,
                                     int dict_fmt,
                                     QueryDeadline *dl)
{
    /* Bound the walk by any range/eq leaf on order_by so a sparse windowed
       query stops at the window instead of scanning the whole index. The
       per-record criteria_match_tree still runs, so this only narrows the walk. */
    OrderWalkBounds owb;
    order_walk_bounds(tree, fs, order_by, &owb);
    const char *lo = owb.has_lo ? (const char *)owb.lo : "";
    size_t      lo_len  = owb.has_lo ? owb.lo_len  : 0;
    int         lo_excl = owb.has_lo ? owb.lo_excl : 0;
    const char *hi = owb.has_hi ? (const char *)owb.hi : "\xff\xff\xff\xff";
    size_t      hi_len  = owb.has_hi ? owb.hi_len  : 4;
    int         hi_excl = owb.has_hi ? owb.hi_excl : 0;

    /* Set up the callback context. */
    OrderIndexWalkCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.db_root        = db_root;
    ctx.object         = object;
    ctx.sch            = sch;
    ctx.fs             = fs;
    ctx.tree           = tree;
    ctx.excluded       = excluded;
    ctx.proj_fields    = proj_fields;
    ctx.proj_count     = proj_count;
    ctx.dict_fmt       = dict_fmt;
    ctx.skip_remaining = (offset > 0) ? offset : 0;
    ctx.limit          = (limit > 0)  ? limit  : INT_MAX;
    ctx.printed        = 0;
    ctx.dl             = dl;
    ctx.dl_counter     = 0;
    ctx.parent_out     = g_out;

    /* Walk the order_by btree across its full range.  The k-way merge
       delivers entries in (encoded_value, hash16) order — asc or desc
       per order_desc.  The callback post-filters each record against the
       full criteria tree and stops when limit is reached. */
    btree_idx_walk_ordered(db_root, object, order_by, sch->splits,
                           lo, lo_len, lo_excl,
                           hi, hi_len, hi_excl,
                           order_desc, order_index_walk_cb, &ctx);

    return ctx.printed;
}

enum SearchOp parse_op(const char *s) {
    if (strcmp(s, "eq") == 0 || strcmp(s, "equal") == 0) return OP_EQUAL;
    if (strcmp(s, "neq") == 0 || strcmp(s, "not_equal") == 0) return OP_NOT_EQUAL;
    if (strcmp(s, "lt") == 0 || strcmp(s, "less") == 0) return OP_LESS;
    if (strcmp(s, "gt") == 0 || strcmp(s, "greater") == 0) return OP_GREATER;
    if (strcmp(s, "lte") == 0 || strcmp(s, "less_eq") == 0) return OP_LESS_EQ;
    if (strcmp(s, "gte") == 0 || strcmp(s, "greater_eq") == 0) return OP_GREATER_EQ;
    if (strcmp(s, "like") == 0) return OP_LIKE;
    if (strcmp(s, "nlike") == 0 || strcmp(s, "not_like") == 0) return OP_NOT_LIKE;
    if (strcmp(s, "contains") == 0) return OP_CONTAINS;
    if (strcmp(s, "ncontains") == 0 || strcmp(s, "not_contains") == 0) return OP_NOT_CONTAINS;
    if (strcmp(s, "starts") == 0 || strcmp(s, "starts_with") == 0) return OP_STARTS_WITH;
    if (strcmp(s, "ends") == 0 || strcmp(s, "ends_with") == 0) return OP_ENDS_WITH;
    if (strcmp(s, "in") == 0) return OP_IN;
    if (strcmp(s, "nin") == 0 || strcmp(s, "not_in") == 0) return OP_NOT_IN;
    if (strcmp(s, "between") == 0) return OP_BETWEEN;
    if (strcmp(s, "exists") == 0) return OP_EXISTS;
    if (strcmp(s, "nexists") == 0 || strcmp(s, "not_exists") == 0) return OP_NOT_EXISTS;
    if (strcmp(s, "len_eq") == 0) return OP_LEN_EQ;
    if (strcmp(s, "len_neq") == 0) return OP_LEN_NEQ;
    if (strcmp(s, "len_lt") == 0) return OP_LEN_LESS;
    if (strcmp(s, "len_gt") == 0) return OP_LEN_GREATER;
    if (strcmp(s, "len_lte") == 0) return OP_LEN_LESS_EQ;
    if (strcmp(s, "len_gte") == 0) return OP_LEN_GREATER_EQ;
    if (strcmp(s, "len_between") == 0) return OP_LEN_BETWEEN;
    if (strcmp(s, "ilike") == 0) return OP_ILIKE;
    if (strcmp(s, "inlike") == 0 || strcmp(s, "inot_like") == 0 ||
        strcmp(s, "not_ilike") == 0) return OP_INOT_LIKE;
    if (strcmp(s, "icontains") == 0) return OP_ICONTAINS;
    if (strcmp(s, "incontains") == 0 || strcmp(s, "inot_contains") == 0 ||
        strcmp(s, "not_icontains") == 0) return OP_INOT_CONTAINS;
    if (strcmp(s, "istarts") == 0 || strcmp(s, "istarts_with") == 0) return OP_ISTARTS_WITH;
    if (strcmp(s, "iends") == 0 || strcmp(s, "iends_with") == 0) return OP_IENDS_WITH;
    if (strcmp(s, "eq_field") == 0) return OP_EQ_FIELD;
    if (strcmp(s, "neq_field") == 0) return OP_NEQ_FIELD;
    if (strcmp(s, "lt_field") == 0) return OP_LT_FIELD;
    if (strcmp(s, "gt_field") == 0) return OP_GT_FIELD;
    if (strcmp(s, "lte_field") == 0) return OP_LTE_FIELD;
    if (strcmp(s, "gte_field") == 0) return OP_GTE_FIELD;
    if (strcmp(s, "regex") == 0) return OP_REGEX;
    if (strcmp(s, "not_regex") == 0 || strcmp(s, "nregex") == 0) return OP_NOT_REGEX;
    return OP_EQUAL;
}

/* ========== AND index-intersection (KeySet fast path) ==========

   For pure AND trees where every child is an indexed leaf on a btree-rangeable
   operator: walk each leaf's btree, intersect candidate hash sets via KeySet,
   skip the per-record fetch + criteria_match_tree loop entirely. Win compounds
   when the primary leaf has many matches but the intersection is small —
   today's primary-leaf path pays O(primary_matches) record fetches; this path
   pays O(sum of btree walks). */

typedef struct {
    KeySet *running;       /* KeySet from prior leaves (probe target) */
    KeySet *out;           /* survivors that hit `running` for this leaf */
    QueryDeadline *deadline;
    int dl_counter;
} IntersectProbeCtx;

/* btree callback for the second-and-later leaves: drop hashes that aren't
   already in `running`, surviving hashes go into `out`. Returns -1 if the
   destination KeySet refuses an insert (capacity exhausted) — keeps a
   sizing miss from degrading inserts to O(cap) per call (ouch). */
static int intersect_probe_cb(const char *val, size_t vlen,
                              const uint8_t *hash16, void *ctx) {
    (void)val; (void)vlen;
    IntersectProbeCtx *p = (IntersectProbeCtx *)ctx;
    if (query_deadline_tick(p->deadline, &p->dl_counter)) return -1;
    if (keyset_contains(p->running, hash16)) {
        if (keyset_insert(p->out, hash16) < 0) return -1;
    }
    return 0;
}

/* Estimate KeySet capacity from index file sizes (summed across all shards).
   anchor count ≈ size / page, ~16 leaf entries per anchor block. Generous
   oversize is fine — KeySet is open-addressed and tolerates load factor
   up to 0.5 by construction. */
static size_t leaf_capacity_hint(const char *db_root, const char *object,
                                 const char *field, int splits) {
    int n = index_splits_for(splits);
    size_t total = 0;
    for (int s = 0; s < n; s++) {
        char p[PATH_MAX];
        build_idx_path(p, sizeof(p), db_root, object, field, s);
        struct stat st;
        if (stat(p, &st) == 0) total += (size_t)st.st_size;
    }
    if (total == 0) return 256;
    size_t hint = (total / 4096) * 16;
    if (hint < 256) hint = 256;
    if (hint > 10000000) hint = 10000000;
    return hint;
}

typedef struct {
    KeySet *ks;
    QueryDeadline *deadline;
    int dl_counter;
    int overflowed;   /* set when keyset_insert reports cap exhausted */
} IntersectCollectCtx;

/* btree callback for the first leaf: every hit drops into the seed KeySet.
   Returns -1 on insert failure (capacity exhausted) so the btree walk halts
   instead of paying O(cap) per insert into a full table.  Sets `overflowed`
   so the caller can distinguish "capacity exhausted" (KeySet incomplete,
   must discard) from "deadline hit" (return what we have, mark timed-out). */
static int intersect_collect_cb(const char *val, size_t vlen,
                                const uint8_t *hash16, void *ctx) {
    (void)val; (void)vlen;
    IntersectCollectCtx *c = (IntersectCollectCtx *)ctx;
    if (query_deadline_tick(c->deadline, &c->dl_counter)) return -1;
    if (keyset_insert(c->ks, hash16) < 0) { c->overflowed = 1; return -1; }
    return 0;
}

/* Walk the first leaf's btree into a fresh KeySet. Capacity is the larger
   of the file-size heuristic and the object's live-record count: at worst a
   single leaf can match every record (e.g., a low-cardinality bool index
   where one value covers half the dataset, or a range that spans the whole
   field). Without the live_count floor, the file-size hint can underestimate
   by an order of magnitude on densely-compressed btrees and the inserts
   degrade to O(cap) once the table fills. */
/* Bitmap-walk → bt_result_cb adapter. Bit positions emitted by bm_walk
   are slot indices in the matching kf shard; the kf entry at that slot
   holds the record's hash. Each live entry's hash gets emitted via the
   shared callback as if it came from a btree walk — gives every
   downstream consumer (count, find, intersect) the same shape. */
typedef struct {
    SlotcaskKfEntry *kf_map;
    size_t           kf_capacity;
    const uint8_t   *val;   /* encoded query value bytes — emitted to cb */
    size_t           vlen;
    bt_result_cb     cb;
    void            *cb_ctx;
    int              stop;  /* set when user cb returned non-zero —
                                propagates out to the outer shard loop */
} BmEmitCtx;

static int bm_emit_cb(uint32_t slot, void *ctx) {
    BmEmitCtx *c = (BmEmitCtx *)ctx;
    if (slot >= c->kf_capacity) return 0;
    SlotcaskKfEntry *e = &c->kf_map[slot];
    if (e->flag != 1) return 0;  /* empty / tombstoned */
    int rc = c->cb((const char *)c->val, c->vlen, e->hash, c->cb_ctx);
    if (rc) c->stop = 1;
    return rc;
}

/* Generic bitmap dispatch — handles every op the bitmap can answer
   that isn't covered by the OP_EQUAL/OP_IN fast paths. Strategy:
   iterate the on-disk dict, decode each value, evaluate the criterion
   against it, collect the matching values, then walk each matching
   value's bitmap and emit hashes via the normal kf-lookup path.

   This is the "btree parity" path: btree's default dispatch is a full
   leaf scan (one cb per leaf entry, ~25 M cb invocations at 25M scale).
   The bitmap equivalent is ≤256 cb invocations (dict size cap) — much
   less work — and still routes every op through the index instead of
   falling back to a data-shard scan. */

/* Forward decl — decode_idx_to_buf lives further down in the file
   (~L15596) but the generic bitmap dict-scan path needs it. Static
   to match the actual definition. */

/* Per-shard match collector. Walks the dict via bm_iter_values; for
   each entry, decodes it to display form and applies match_criterion
   (or match_length_vlen for len_* ops). Stops collecting at
   BM_DEFAULT_MAX_VALUES — that's the hard ceiling for bitmap dicts. */
#define BM_DICT_MATCH_CAP 256
typedef struct {
    SearchCriterion  *crit;
    const TypedField *tf;
    uint8_t  vals[BM_DICT_MATCH_CAP][1024];
    size_t   vlens[BM_DICT_MATCH_CAP];
    int      n_match;
} BmDictMatchCtx;

static int bm_dict_match_cb(const uint8_t *value, size_t vlen, void *ctx) {
    BmDictMatchCtx *m = (BmDictMatchCtx *)ctx;
    SearchCriterion *c = m->crit;
    int matched;
    if (op_is_length(c->op)) {
        matched = match_length_vlen(vlen, c);
    } else {
        char buf[512];
        int dlen = decode_idx_to_buf(m->tf, value, vlen, buf, sizeof(buf), 0);
        if (dlen <= 0) return 0;
        matched = match_criterion(buf, c);
    }
    if (matched && m->n_match < BM_DICT_MATCH_CAP &&
        vlen <= sizeof(m->vals[0])) {
        memcpy(m->vals[m->n_match], value, vlen);
        m->vlens[m->n_match] = vlen;
        m->n_match++;
    }
    return 0;
}

/* Walk a single shard's bitmap dict-scan style. Returns 1 if cb
   signalled stop. Mirrors bitmap_emit_for_shard's contract. */
static int bitmap_emit_generic_for_shard(const char *db_root, const char *object,
                                          const char *field, int shard_idx,
                                          SearchCriterion *crit,
                                          const TypedField *tf,
                                          bt_result_cb cb, void *ctx,
                                          SlotcaskDb *sdb) {
    char bp[1024];
    bm_build_path(bp, sizeof(bp), db_root, object, field, shard_idx);
    BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
    if (!bm) return 0;

    BmDictMatchCtx m = { .crit = crit, .tf = tf, .n_match = 0 };
    bm_iter_values(bm, bm_dict_match_cb, &m);
    if (m.n_match == 0) { bm_close(bm); return 0; }

    char kfp[PATH_MAX];
    slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, shard_idx);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0) {
        bm_close(bm);
        return 0;
    }

    int stop = 0;
    for (int i = 0; i < m.n_match && !stop; i++) {
        BmEmitCtx ec = { kh.map, kh.capacity, m.vals[i], m.vlens[i],
                         cb, ctx, 0 };
        bm_walk(bm, m.vals[i], m.vlens[i], bm_emit_cb, &ec);
        if (ec.stop) stop = 1;
    }
    idx_count_cb_flush_thread();

    kfcache_release(&kh);
    bm_close(bm);
    return stop;
}

/* Per-shard parallel worker for the generic dict-scan path.
   Struct definition + forward decl live earlier in the file so
   btree_dispatch can reference both. */
static void *bm_generic_shard_worker(void *arg) {
    BmGenericShardArg *a = (BmGenericShardArg *)arg;
    if (a->stop_flag && __atomic_load_n(a->stop_flag, __ATOMIC_RELAXED))
        return NULL;
    int stopped = bitmap_emit_generic_for_shard(a->db_root, a->object,
                                                 a->field, a->shard_idx,
                                                 a->crit, a->tf,
                                                 a->cb, a->ctx, a->sdb);
    if (stopped && a->stop_flag) {
        __atomic_store_n(a->stop_flag, 1, __ATOMIC_RELAXED);
    }
    return NULL;
}

/* bt_result_cb adapter for the generic dict-scan → KeySet path.
   Inserts each emitted hash into the keyset, respects deadline. */
typedef struct { KeySet *ks; QueryDeadline *dl; } BmKsEmitCtx;
static int bm_ks_insert_cb(const char *v, size_t vl, const uint8_t *h, void *c) {
    (void)v; (void)vl;
    BmKsEmitCtx *kc = (BmKsEmitCtx *)c;
    if (kc->dl && kc->dl->timed_out) return -1;
    keyset_insert(kc->ks, h);
    return 0;
}

/* Count fast path for any criterion: sum bm_count across (shard ×
   matching dict value). Mirrors bm_popcount_for_crit but with
   dict-scan + criterion-match instead of an explicit value list.
   Used by count's PRIMARY_LEAF path for ops other than eq/IN. */
/* Per-worker arg for parallel generic-bitmap popcount.  Value list lives
 * inside the bitmap shard's dict, so it's discovered per-shard via
 * bm_iter_values + bm_dict_match_cb — same shape as the serial version. */
typedef struct {
    const char       *db_root;
    const char       *object;
    const char       *field;
    int               shard_idx;
    SearchCriterion  *crit;
    const TypedField *tf;
    size_t            count;
} BmPopcountGenericShardArg;

static void *bm_popcount_generic_shard_worker(void *raw) {
    BmPopcountGenericShardArg *a = (BmPopcountGenericShardArg *)raw;
    char bp[1024];
    bm_build_path(bp, sizeof(bp), a->db_root, a->object, a->field, a->shard_idx);
    BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
    if (!bm) { a->count = 0; return NULL; }
    BmDictMatchCtx m = { .crit = a->crit, .tf = a->tf, .n_match = 0 };
    bm_iter_values(bm, bm_dict_match_cb, &m);
    size_t local = 0;
    for (int i = 0; i < m.n_match; i++) {
        local += bm_count(bm, m.vals[i], m.vlens[i]);
    }
    bm_close(bm);
    a->count = local;
    return NULL;
}

static size_t bm_popcount_generic_for_crit(const char *db_root, const char *object,
                                            const char *field, int splits,
                                            SearchCriterion *crit,
                                            const TypedField *tf) {
    if (splits <= 0) return 0;
    /* Parallelise across data shards — see bm_popcount_one_value for the
     * cold-cache motivation.  Generic path also pays bm_iter_values per
     * shard (small dict scan) so the cost-per-shard is slightly higher
     * than the eq fast path; parallelising matters even more here. */
    BmPopcountGenericShardArg *args =
        malloc((size_t)splits * sizeof(BmPopcountGenericShardArg));
    if (!args) {
        size_t total = 0;
        for (int s = 0; s < splits; s++) {
            char bp[1024];
            bm_build_path(bp, sizeof(bp), db_root, object, field, s);
            BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
            if (!bm) continue;
            BmDictMatchCtx m = { .crit = crit, .tf = tf, .n_match = 0 };
            bm_iter_values(bm, bm_dict_match_cb, &m);
            for (int i = 0; i < m.n_match; i++) {
                total += bm_count(bm, m.vals[i], m.vlens[i]);
            }
            bm_close(bm);
        }
        return total;
    }
    for (int s = 0; s < splits; s++) {
        args[s] = (BmPopcountGenericShardArg){
            .db_root = db_root, .object = object, .field = field,
            .shard_idx = s, .crit = crit, .tf = tf, .count = 0,
        };
    }
    parallel_for_io(bm_popcount_generic_shard_worker, args, splits,
                 sizeof(BmPopcountGenericShardArg));
    size_t total = 0;
    for (int s = 0; s < splits; s++) total += args[s].count;
    free(args);
    return total;
}

/* ========== Bitmap word-level intersect (all-bitmap AND COUNT) ==========
 * For COUNT with 2+ bitmap EQ leaves: AND bit-arrays word-by-word,
 * sum popcounts.  No KeySets, no hashes, O(splits × stride) constant
 * memory.  Byte-level popcount for unaligned-safety on all architectures. */

typedef struct {
    const char       *db_root;
    const char       *object;
    int               shard_idx;
    SearchCriterion  **leaves;
    int               n_leaves;
    const TypedSchema *ts;
    size_t            count;      /* output */
    QueryDeadline    *deadline;
} BmIntersectShardArg;

static void *bm_intersect_shard_worker(void *raw) {
    BmIntersectShardArg *a = (BmIntersectShardArg *)raw;
    a->count = 0;
    if (a->n_leaves < 2 || a->n_leaves > MAX_INTERSECT_LEAVES) return NULL;

    /* Pre-encode all leaf values */
    uint8_t  vals[MAX_INTERSECT_LEAVES][1024];
    size_t   vlens[MAX_INTERSECT_LEAVES];
    for (int i = 0; i < a->n_leaves; i++) {
        const TypedField *tf = resolve_idx_field(a->ts, a->leaves[i]->field);
        if (!tf) return NULL;
        encode_criterion_value(tf, a->leaves[i]->value,
                                strlen(a->leaves[i]->value),
                                vals[i], &vlens[i]);
    }

    /* Open all bitmaps for this shard.
     * OP_EQUAL: single-value lookup.  OP_IN: OR together all matching
     * value bit-arrays into a stack buffer.  Other ops: goto cleanup. */
    BitmapShard *bm[MAX_INTERSECT_LEAVES] = {0};
    const uint8_t *bmap_bytes[MAX_INTERSECT_LEAVES] = {0};
    uint8_t *merged[MAX_INTERSECT_LEAVES] = {0};
    uint32_t min_stride = UINT32_MAX;

    for (int i = 0; i < a->n_leaves; i++) {
        char bp[PATH_MAX];
        bm_build_path(bp, sizeof(bp), a->db_root, a->object,
                      a->leaves[i]->field, a->shard_idx);
        bm[i] = bm_open(bp, 0, 0, 0, 0, 0);
        if (!bm[i]) goto cleanup;

        if (a->leaves[i]->op == OP_EQUAL) {
            uint32_t this_stride = 0;
            bmap_bytes[i] = bm_get_value_bitmap(bm[i], vals[i], vlens[i],
                                                 &this_stride);
            if (!bmap_bytes[i]) goto cleanup;  /* value not in dict → 0 matches */
            if (this_stride < min_stride)
                min_stride = this_stride;
        } else if (a->leaves[i]->op == OP_IN) {
            const TypedField *tf = resolve_idx_field(a->ts, a->leaves[i]->field);
            if (!tf) goto cleanup;
            uint32_t stride = bm_stride(bm[i]);
            merged[i] = calloc(1, stride);
            if (!merged[i]) goto cleanup;
            int any = 0;
            for (int v = 0; v < a->leaves[i]->in_count; v++) {
                uint8_t enc[1024];
                size_t elen;
                encode_criterion_value(tf, a->leaves[i]->in_values[v],
                                       strlen(a->leaves[i]->in_values[v]),
                                       enc, &elen);
                uint32_t vs;
                const uint8_t *vmap = bm_get_value_bitmap(bm[i], enc, elen, &vs);
                if (vmap) {
                    any = 1;
                    for (uint32_t b = 0; b < stride; b++)
                        merged[i][b] |= vmap[b];
                }
            }
            if (!any) goto cleanup;  /* no IN value in dict → 0 matches */
            bmap_bytes[i] = merged[i];
            if (stride < min_stride)
                min_stride = stride;
        } else {
            goto cleanup;  /* unsupported op → fall through to KeySet path */
        }
    }

    /* Byte-level AND + popcount.  Byte-level rather than uint64_t for
     * unaligned-safety: stride is rarely a multiple of 8, so vidx>0
     * bitmaps start at unaligned offsets.  Matches bm_count's pattern. */
    size_t local = 0;
    for (uint32_t b = 0; b < min_stride; b++) {
        uint8_t and_byte = 0xFF;
        for (int i = 0; i < a->n_leaves; i++)
            and_byte &= bmap_bytes[i][b];
        local += (size_t)__builtin_popcount(and_byte);
    }
    a->count = local;

cleanup:
    for (int i = 0; i < a->n_leaves; i++) {
        if (bm[i]) bm_close(bm[i]);
        free(merged[i]);
    }
    return NULL;
}

static size_t bm_popcount_intersect(const char *db_root, const char *object,
                                     int splits,
                                     SearchCriterion **leaves, int n_leaves,
                                     const TypedSchema *ts,
                                     QueryDeadline *dl) {
    if (n_leaves < 2 || n_leaves > MAX_INTERSECT_LEAVES || splits <= 0)
        return 0;

    BmIntersectShardArg *args =
        calloc((size_t)splits, sizeof(BmIntersectShardArg));
    if (!args) {
        /* malloc failed: fall back to serial */
        size_t total = 0;
        for (int s = 0; s < splits && (!dl || !dl->timed_out); s++) {
            BmIntersectShardArg a = { .db_root = db_root, .object = object,
                                       .shard_idx = s, .leaves = leaves,
                                       .n_leaves = n_leaves, .ts = ts,
                                       .count = 0, .deadline = dl };
            bm_intersect_shard_worker(&a);
            total += a.count;
        }
        return total;
    }

    for (int s = 0; s < splits; s++) {
        args[s] = (BmIntersectShardArg){
            .db_root = db_root, .object = object, .shard_idx = s,
            .leaves = leaves, .n_leaves = n_leaves,
            .ts = ts, .count = 0, .deadline = dl,
        };
    }
    parallel_for_io(bm_intersect_shard_worker, args, splits,
                  sizeof(BmIntersectShardArg));

    size_t total = 0;
    for (int s = 0; s < splits && (!dl || !dl->timed_out); s++)
        total += args[s].count;
    free(args);
    return (!dl || !dl->timed_out) ? total : 0;
}

/* Walk a single shard's bitmap for `value`, emitting cb per live
   matching slot. Returns 1 if cb signalled stop (limit reached), so
   the outer shard loop can short-circuit and skip the remaining
   kf_acquire + bm_open syscalls. */
int bitmap_emit_for_shard(const char *db_root, const char *object,
                                 const char *field, int shard_idx,
                                 const uint8_t *value, size_t vlen,
                                 bt_result_cb cb, void *ctx, SlotcaskDb *sdb) {
    char bp[1024];
    bm_build_path(bp, sizeof(bp), db_root, object, field, shard_idx);
    BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
    if (!bm) return 0;

    char kfp[PATH_MAX];
    slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, shard_idx);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0) {
        bm_close(bm);
        return 0;
    }

    BmEmitCtx ec = { kh.map, kh.capacity, value, vlen, cb, ctx, 0 };
    bm_walk(bm, value, vlen, bm_emit_cb, &ec);

    idx_count_cb_flush_thread();

    kfcache_release(&kh);
    bm_close(bm);
    return ec.stop;
}

/* parallel_for worker for shard fan-out. Checks the shared stop flag
   before doing any I/O so a peer's limit-hit propagates immediately.
   On a cb-driven stop, raises the flag so subsequent peers also bail. */
static void *bm_shard_walk_worker(void *arg) {
    BmShardWalkArg *a = (BmShardWalkArg *)arg;
    if (a->stop_flag && __atomic_load_n(a->stop_flag, __ATOMIC_RELAXED)) {
        return NULL;
    }
    int stopped = bitmap_emit_for_shard(a->db_root, a->object, a->field,
                                        a->shard_idx, a->value, a->vlen,
                                        a->cb, a->ctx, a->sdb);
    if (stopped && a->stop_flag) {
        __atomic_store_n(a->stop_flag, 1, __ATOMIC_RELAXED);
    }
    return NULL;
}

/* Bitmap-walk → KeySet collector. Kept for compatibility with the
   build_keyset_from_bitmap helper that the planner uses for
   intersect-leaf builds. */
typedef struct {
    SlotcaskKfEntry *kf_map;
    size_t           kf_capacity;
    KeySet          *ks;
    QueryDeadline   *dl;
} BmCollectCtx;

static int bm_collect_to_keyset_cb(uint32_t slot, void *ctx) {
    BmCollectCtx *c = (BmCollectCtx *)ctx;
    if (c->dl && c->dl->timed_out) return 1;
    if (slot >= c->kf_capacity) return 0;
    SlotcaskKfEntry *e = &c->kf_map[slot];
    if (e->flag != 1) return 0;
    keyset_insert(c->ks, e->hash);
    return 0;
}

/* Does this field have an index of the given type declared? A field
   may have multiple declarations (e.g. both `text` and `text:trigram`)
   so a single-answer lookup like field_index_type can't disambiguate —
   use this predicate instead when the caller cares about a specific
   type's availability. Linear scan over the cached index.conf arrays. */
int field_has_index_type(const char *db_root, const char *object,
                                const char *field, enum IndexType want) {
    char fields[MAX_FIELDS][256];
    enum IndexType types[MAX_FIELDS];
    int n = load_index_fields(db_root, object, fields, MAX_FIELDS);
    int n2 = load_index_types(db_root, object, types, MAX_FIELDS);
    int count = n < n2 ? n : n2;
    for (int i = 0; i < count; i++) {
        if (types[i] == want && strcmp(fields[i], field) == 0) return 1;
    }
    return 0;
}

/* Operator → index-type routing rules:
 *
 *   contains, i_contains, like, not_like (with literal substring ≥3 chars)
 *     → IT_TRIGRAM if available, else fall back to whatever's declared
 *
 *   eq, neq, range, in, not_in, starts_with, ends_with, len_*, exists, regex
 *     → IT_BITMAP or IT_BTREE depending on declared type
 *
 * Used everywhere the planner asks "what index should I drive this op
 * through" — leaf_is_indexed, build_keyset_from_leaf, the dispatcher. */
int op_prefers_trigram(enum SearchOp op) {
    return op_caps(op).trigram_prefers;
}

/* Returns 1 when the op can be served by a trigram index for starts_with
 * (prefix length ≥ 3 required, checked by caller). */
static int op_allows_trigram_starts(enum SearchOp op) {
    return op_caps(op).trigram_starts;
}

/* Crossover length between btree-leaf scan and trigram intersection
 * for contains/i_contains. The trigram verify step is O(candidates ×
 * per-record fetch) while btree-leaf is O(total_leaves × per-leaf
 * memmem). At 25M scale on small-vocab data: "baker" (5 char, 833k
 * hits) costs ~160ms via btree-leaf vs ~740ms via trigram. Threshold 6
 * is empirical: 5-char patterns regress to ~700ms+ via trigram, 6-char
 * rare patterns win via trigram (6ms vs 200ms btree-leaf). Above 6 chars
 * trigram intersection prunes fast and verify cost stays manageable.
 *
 * Consumed by pick_index_for_leaf — the single dispatch decision point. */
#define TG_PREFER_BTREE_LEN 6

/* Resolve a field's IndexType from the cached index.conf. Linear scan
   over the cached arrays — cheap for the planner hot path. */
enum IndexType field_index_type(const char *db_root, const char *object,
                                       const char *field) {
    char fields[MAX_FIELDS][256];
    enum IndexType types[MAX_FIELDS];
    int n = load_index_fields(db_root, object, fields, MAX_FIELDS);
    int n2 = load_index_types(db_root, object, types, MAX_FIELDS);
    int count = n < n2 ? n : n2;
    for (int i = 0; i < count; i++) {
        if (strcmp(fields[i], field) == 0) return types[i];
    }
    return IT_BTREE;
}

/* Plan-time index picker — single source of truth for "which index
 * should this leaf use, if any?".  Returns IT_BTREE / IT_BITMAP /
 * IT_TRIGRAM, or -1 when no usable index exists for this (field, op)
 * combination (caller falls back to full scan).
 *
 * Both `leaf_is_indexed` (planner) and `build_keyset_from_leaf`
 * (executor) dispatch off this — keeping them in sync removes the
 * pre-2026-05-25 runtime cascade where the builder would try trigram,
 * then fall through to bitmap, then to btree.  When this returns a
 * type, that's *the* index for the leaf; if the corresponding builder
 * later returns NULL (transient alloc failure etc.), the caller drops
 * to full scan instead of attempting a different index type.
 *
 * Rules:
 *  - NOT_EXISTS, field-vs-field ops, regex on non-varchar → -1
 *    (existing un-indexable cases).
 *  - For contains / i_contains:
 *      * pattern < TG_PREFER_BTREE_LEN chars AND btree present →
 *        IT_BTREE (btree-leaf memmem beats trigram-verify at short
 *        patterns; threshold is empirical, see TG_PREFER_BTREE_LEN).
 *      * Otherwise, trigram present AND pattern ≥ 3 chars → IT_TRIGRAM
 *        (trigrams need 3-grams).
 *      * Otherwise btree present → IT_BTREE (catches sub-3-char contains
 *        on a btree-only field — btree-leaf scan still beats full scan).
 *      * Else → -1.
 *  - All other ops: bitmap preferred when present, then btree, else -1. */
int pick_index_for_leaf(const char *db_root, const char *object,
                               const SearchCriterion *c) {
    if (!c || !c->field[0]) return -1;
    if (c->op == OP_NOT_EXISTS) return -1;
    if (c->op == OP_EQ_FIELD || c->op == OP_NEQ_FIELD ||
        c->op == OP_LT_FIELD || c->op == OP_GT_FIELD ||
        c->op == OP_LTE_FIELD || c->op == OP_GTE_FIELD) return -1;
    if (c->op == OP_REGEX || c->op == OP_NOT_REGEX) {
        TypedSchema *ts = load_typed_schema(db_root, object);
        if (!ts) return -1;
        int fi = typed_field_index(ts, c->field);
        if (fi < 0 || ts->fields[fi].type != FT_VARCHAR) return -1;
    }
    Schema sch = load_schema(db_root, object);
    int has_btree   = field_has_index_type(db_root, object, c->field, IT_BTREE);
    int has_bitmap  = field_has_index_type(db_root, object, c->field, IT_BITMAP);
    int has_trigram = field_has_index_type(db_root, object, c->field, IT_TRIGRAM);

    if (op_prefers_trigram(c->op)) {
        size_t plen = strlen(c->value);
        if (has_btree && plen < TG_PREFER_BTREE_LEN) {
            return btree_idx_exists(db_root, object, c->field, sch.splits)
                   ? IT_BTREE : -1;
        }
        if (has_trigram && plen >= 3) return IT_TRIGRAM;
        if (has_btree) {
            return btree_idx_exists(db_root, object, c->field, sch.splits)
                   ? IT_BTREE : -1;
        }
        return -1;
    }
    /* starts_with: btree is the precise path; trigram is the fallback when
     * no btree exists AND the prefix is long enough to extract a 3-gram.
     * Shorter prefixes (<3 chars) cannot form any trigram, so they fall
     * through to full scan (-1). */
    if (op_allows_trigram_starts(c->op)) {
        size_t plen = strlen(c->value);
        if (has_btree && btree_idx_exists(db_root, object, c->field, sch.splits))
            return IT_BTREE;
        if (has_trigram && plen >= 3) return IT_TRIGRAM;
        return -1;
    }
    if (has_bitmap) return IT_BITMAP;
    if (has_btree && btree_idx_exists(db_root, object, c->field, sch.splits))
        return IT_BTREE;
    return -1;
}

/* Sum bm_count for a single encoded value across every data shard.
   Cache-friendly stride-byte popcount in each shard. Cheap (~ms-scale
   at 25M / 128 shards even cold). Shared between count's popcount
   fast path, the negation-shortcut popcount, and IN-sum below. */
/* Per-worker arg for parallel bitmap popcount fan-out. */
typedef struct {
    const char    *db_root;
    const char    *object;
    const char    *field;
    int            shard_idx;
    const uint8_t *value;
    size_t         vlen;
    size_t         count;   /* output — this worker's contribution */
} BmPopcountShardArg;

static void *bm_popcount_one_shard_worker(void *raw) {
    BmPopcountShardArg *a = (BmPopcountShardArg *)raw;
    char bp[1024];
    bm_build_path(bp, sizeof(bp), a->db_root, a->object, a->field, a->shard_idx);
    BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
    if (!bm) { a->count = 0; return NULL; }
    a->count = bm_count(bm, a->value, a->vlen);
    bm_close(bm);
    return NULL;
}

size_t bm_popcount_one_value(const char *db_root, const char *object,
                                     const char *field, int splits,
                                     const uint8_t *value, size_t vlen) {
    if (splits <= 0) return 0;
    /* Parallelise across data shards — each shard's bitmap open + bm_count
     * is independent.  Serial loops over splits=256 hit ~5s on cold cache
     * (256 × ~20ms per file open + popcount); parallel_for cuts that to
     * ~50-200ms by overlapping I/O across worker threads.  Same shape as
     * bm_shard_walk_worker uses for the value-walk fan-out. */
    BmPopcountShardArg *args = malloc((size_t)splits * sizeof(BmPopcountShardArg));
    if (!args) {
        /* OOM fallback: serial loop. */
        size_t total = 0;
        for (int s = 0; s < splits; s++) {
            char bp[1024];
            bm_build_path(bp, sizeof(bp), db_root, object, field, s);
            BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0);
            if (!bm) continue;
            total += bm_count(bm, value, vlen);
            bm_close(bm);
        }
        return total;
    }
    for (int s = 0; s < splits; s++) {
        args[s] = (BmPopcountShardArg){
            .db_root = db_root, .object = object, .field = field,
            .shard_idx = s, .value = value, .vlen = vlen, .count = 0,
        };
    }
    parallel_for_io(bm_popcount_one_shard_worker, args, splits, sizeof(BmPopcountShardArg));
    size_t total = 0;
    for (int s = 0; s < splits; s++) total += args[s].count;
    free(args);
    return total;
}

/* Total bitmap match count for an OP_EQUAL / OP_IN criterion.
   Per-value bitmaps are disjoint by construction (each record's value
   sets exactly one bit across the per-value maps), so summing across
   the IN values gives the exact match count without dedup. */
static size_t bm_popcount_for_crit(const char *db_root, const char *object,
                                    int splits, const SearchCriterion *crit,
                                    const TypedField *tf) {
    if (!crit) return 0;
    uint8_t v[1024];
    size_t  vl = 0;
    if (crit->op == OP_EQUAL) {
        encode_criterion_value(tf, crit->value, strlen(crit->value), v, &vl);
        if (vl == 0) return 0;
        return bm_popcount_one_value(db_root, object, crit->field, splits, v, vl);
    }
    if (crit->op == OP_IN) {
        size_t total = 0;
        for (int i = 0; i < crit->in_count; i++) {
            vl = 0;
            encode_criterion_value(tf, crit->in_values[i],
                                   strlen(crit->in_values[i]), v, &vl);
            if (vl == 0) continue;
            total += bm_popcount_one_value(db_root, object, crit->field,
                                            splits, v, vl);
        }
        return total;
    }
    return 0;
}

/* Walk every data shard's bitmap shard for the matching value, lift each
   live record's hash into a KeySet. Returns NULL on timeout / failure
   or when the projected keyset footprint exceeds the per-query memory
   budget — caller falls through to full scan in either case.

   Sizing: KeySet has no resize (see keyset.c). If the table fills,
   every subsequent insert linear-probes the whole capacity uselessly.
   The previous fixed `keyset_new(1024)` was a latent O(N × cap) trap
   that turned `group by age where active=true` at 25M into a 41-second
   query with silently-wrong results (only ~2048 hashes recorded out of
   12.5M matches). We now popcount across all shards first (cheap —
   bm_count is a byte popcount) to size the keyset exactly, mirroring
   the btree path's leaf_capacity_hint behaviour. */
static KeySet *build_keyset_from_bitmap(const char *db_root, const char *object,
                                        int splits,
                                        const SearchCriterion *leaf,
                                        const TypedField *tf,
                                        QueryDeadline *dl) {
    /* Bitmap serves eq and IN through the fast value-walk; everything
       else (range, LIKE, CONTAINS, REGEX, len_X, exists, i-variants)
       goes through the generic dict-scan path below — matches btree
       parity: any indexed op routes through index files, never falls
       back to a data-shard scan. NEQ + NOT_IN arrive here pre-inverted
       by the count-mode subtraction shortcut (op_invert → eq/IN). */
    if (leaf->op != OP_EQUAL && leaf->op != OP_IN) {
        Schema sc_g = load_schema(db_root, object);
        SlotcaskSchemaInfo info_g = {
            .splits = sc_g.splits, .slot_size = sc_g.slot_size, .streams = sc_g.streams,
        };
        SlotcaskDb *sdb_g = slotcask_registry_get(db_root, object, &info_g);
        if (!sdb_g) return NULL;

        size_t tot_g = bm_popcount_generic_for_crit(db_root, object,
                                                     leaf->field, splits,
                                                     (SearchCriterion *)leaf, tf);

        size_t ks_bytes_est_g = (tot_g ? tot_g : 1024)
                                * 2 * (sizeof(uint8_t[16]) + sizeof(uint32_t));
        if (ks_bytes_est_g > g_query_buffer_max_bytes) return NULL;

        KeySet *ks_g = keyset_new(tot_g > 0 ? tot_g : 1024);
        if (!ks_g) return NULL;

        BmKsEmitCtx kec = { ks_g, dl };
        for (int s = 0; s < splits; s++) {
            if (dl && dl->timed_out) { keyset_free(ks_g); return NULL; }
            bitmap_emit_generic_for_shard(db_root, object, leaf->field, s,
                                           (SearchCriterion *)leaf, tf,
                                           bm_ks_insert_cb, &kec, sdb_g);
        }
        if (dl && dl->timed_out) { keyset_free(ks_g); return NULL; }
        return ks_g;
    }

    /* Encode every query value up-front so we can both pre-count and
       walk later without re-encoding. n_vals = 1 for OP_EQUAL, in_count
       for OP_IN. Bool/byte/numeric values all fit in <= 8B; even
       varchar bitmaps (future enum-text) cap at 1024 here. */
    int n_vals = (leaf->op == OP_EQUAL) ? 1 : leaf->in_count;
    if (n_vals <= 0) return NULL;
    uint8_t (*vals)[1024] = calloc((size_t)n_vals, sizeof(*vals));
    size_t *vlens = calloc((size_t)n_vals, sizeof(*vlens));
    if (!vals || !vlens) { free(vals); free(vlens); return NULL; }
    if (leaf->op == OP_EQUAL) {
        encode_criterion_value(tf, leaf->value, strlen(leaf->value),
                               vals[0], &vlens[0]);
    } else {
        for (int i = 0; i < n_vals; i++) {
            encode_criterion_value(tf, leaf->in_values[i],
                                   strlen(leaf->in_values[i]),
                                   vals[i], &vlens[i]);
        }
    }
    /* Drop any zero-length encodings (e.g. NULL/empty); they'd cause
       bm_count/bm_walk to return 0/0 anyway but the explicit check
       keeps the inner loop branchless. */
    int n_kept = 0;
    for (int i = 0; i < n_vals; i++) {
        if (vlens[i] == 0) continue;
        if (n_kept != i) {
            memcpy(vals[n_kept], vals[i], vlens[i]);
            vlens[n_kept] = vlens[i];
        }
        n_kept++;
    }
    if (n_kept == 0) { free(vals); free(vlens); return NULL; }

    /* Need a slotcask handle to read kf entries. */
    Schema sc = load_schema(db_root, object);
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size, .streams = sc.streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { free(vals); free(vlens); return NULL; }

    /* Pass A: sum bm_count across every (shard × value). Each bm_count
       is a cache-friendly stride-byte popcount — ~ms-scale at 25M /
       128 shards even cold. Worth it to size the keyset right.
       Per-value bitmaps are disjoint by construction, so the sum is
       exact match cardinality even for IN. */
    size_t total_matches = 0;
    for (int s = 0; s < splits; s++) {
        if (dl && dl->timed_out) {
            free(vals); free(vlens); return NULL;
        }
        char bp[1024];
        bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
        if (!bm) continue;
        for (int i = 0; i < n_kept; i++) {
            total_matches += bm_count(bm, vals[i], vlens[i]);
        }
        bm_close(bm);
    }

    /* Budget check: keyset_new rounds up to next_pow2(hint*2); each slot
       is keys[16] + state[4] = 20 B. Match the btree path's guard so
       monster keysets (e.g. 50%-selective bitmap on a billion-row table)
       fall through to scan instead of OOM-ing the daemon. */
    size_t ks_bytes_est = (total_matches ? total_matches : 1024)
                           * 2 * (sizeof(uint8_t[16]) + sizeof(uint32_t));
    if (ks_bytes_est > g_query_buffer_max_bytes) {
        free(vals); free(vlens); return NULL;
    }

    KeySet *ks = keyset_new(total_matches > 0 ? total_matches : 1024);
    if (!ks) { free(vals); free(vlens); return NULL; }

    /* Pass B: walk the bitmaps and lift matching hashes via kf lookup.
       Serial across shards — the inserts themselves run lock-free
       (keyset_insert uses per-bucket CAS) so a parallel-walk would be
       safe, but kfcache_acquire / page faults on cold kf are the real
       cost and that doesn't trivially parallelise. For each shard we
       walk every value's bitmap into the same keyset; duplicates are
       impossible (values are distinct → bitmaps disjoint) so no extra
       check needed. */
    for (int s = 0; s < splits; s++) {
        if (dl && dl->timed_out) {
            keyset_free(ks); free(vals); free(vlens); return NULL;
        }

        char bp[1024];
        bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
        if (!bm) continue;

        char kfp[PATH_MAX];
        slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0) {
            bm_close(bm);
            continue;
        }

        BmCollectCtx c = { kh.map, kh.capacity, ks, dl };
        for (int i = 0; i < n_kept; i++) {
            bm_walk(bm, vals[i], vlens[i], bm_collect_to_keyset_cb, &c);
        }

        kfcache_release(&kh);
        bm_close(bm);
    }
    free(vals); free(vlens);
    if (dl && dl->timed_out) { keyset_free(ks); return NULL; }
    return ks;
}

/* ---- Bitmap COMPLEMENT keyset (records whose value is NOT a target) ----
 *
 * Used by the streaming top-N when a bitmap eq/IN criterion matches the
 * MAJORITY of rows (e.g. type='story' = 99.4%). Building the match-set then
 * costs a multi-million-entry KeySet; the complement is tiny, so we build it
 * instead and the caller inverts the membership test.
 *
 * Walks every dict value EXCEPT the target(s) via bm_iter_values — no
 * BM_DICT_MATCH_CAP ceiling, so it's complete even for high-cardinality
 * bitmaps. Correct for always-set bitmap fields (bool/enum): a NULL-valued
 * record would be wrongly included, but bitmap fields always carry a value. */
typedef struct {
    const BitmapShard  *bm;
    const uint8_t     (*tvals)[1024];
    const size_t       *tvlens;
    int                 nt;
    BmCollectCtx       *collect;
} BmComplementCtx;

static int bm_complement_value_cb(const uint8_t *value, size_t vlen, void *ctx) {
    BmComplementCtx *cc = (BmComplementCtx *)ctx;
    if (cc->collect->dl && cc->collect->dl->timed_out) return 1;
    for (int t = 0; t < cc->nt; t++) {
        if (vlen == cc->tvlens[t] && memcmp(value, cc->tvals[t], vlen) == 0)
            return 0;  /* target value — excluded from the complement */
    }
    bm_walk(cc->bm, value, vlen, bm_collect_to_keyset_cb, cc->collect);
    return 0;
}

static KeySet *build_keyset_bitmap_complement(const char *db_root, const char *object,
                                              int splits, const SearchCriterion *leaf,
                                              QueryDeadline *dl,
                                              const uint8_t (*tvals)[1024],
                                              const size_t *tvlens, int nt,
                                              size_t comp_hint) {
    size_t ks_bytes_est = (comp_hint ? comp_hint : 1024)
                           * 2 * (sizeof(uint8_t[16]) + sizeof(uint32_t));
    if (ks_bytes_est > g_query_buffer_max_bytes) return NULL;

    Schema sc = load_schema(db_root, object);
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size, .streams = sc.streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) return NULL;

    KeySet *ks = keyset_new(comp_hint > 0 ? comp_hint : 1024);
    if (!ks) return NULL;

    for (int s = 0; s < splits; s++) {
        if (dl && dl->timed_out) { keyset_free(ks); return NULL; }
        char bp[1024];
        bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
        if (!bm) continue;

        char kfp[PATH_MAX];
        slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0) {
            bm_close(bm);
            continue;
        }

        BmCollectCtx c = { kh.map, kh.capacity, ks, dl };
        BmComplementCtx cc = { bm, tvals, tvlens, nt, &c };
        bm_iter_values(bm, bm_complement_value_cb, &cc);

        kfcache_release(&kh);
        bm_close(bm);
    }
    if (dl && dl->timed_out) { keyset_free(ks); return NULL; }
    return ks;
}

/* For a bitmap-indexed eq/IN leaf, build whichever of {match-set, complement}
 * is smaller, so the streaming top-N prefilter never materialises the
 * majority side. *inverted=1 ⇒ the complement was built and the caller counts
 * a row when its hash is NOT in the set. Returns NULL on budget/error so the
 * caller falls back rather than counting unfiltered. */
KeySet *build_smaller_bitmap_keyset(const char *db_root, const char *object,
                                           int splits, const SearchCriterion *leaf,
                                           const TypedField *tf, QueryDeadline *dl,
                                           int *inverted) {
    *inverted = 0;
    if (!tf || (leaf->op != OP_EQUAL && leaf->op != OP_IN))
        return build_keyset_from_bitmap(db_root, object, splits, leaf, tf, dl);

    int n_vals = (leaf->op == OP_EQUAL) ? 1 : leaf->in_count;
    if (n_vals <= 0)
        return build_keyset_from_bitmap(db_root, object, splits, leaf, tf, dl);

    uint8_t (*tvals)[1024] = calloc((size_t)n_vals, sizeof(*tvals));
    size_t  *tvlens = calloc((size_t)n_vals, sizeof(*tvlens));
    if (!tvals || !tvlens) { free(tvals); free(tvlens); return NULL; }
    int nt = 0;
    for (int i = 0; i < n_vals; i++) {
        const char *v = (leaf->op == OP_EQUAL) ? leaf->value : leaf->in_values[i];
        encode_criterion_value(tf, v, strlen(v), tvals[nt], &tvlens[nt]);
        if (tvlens[nt] > 0) nt++;
    }
    if (nt == 0) {
        free(tvals); free(tvlens);
        return build_keyset_from_bitmap(db_root, object, splits, leaf, tf, dl);
    }

    /* Sum bm_count across shards — cheap stride popcount per (shard, value). */
    size_t matches = 0;
    for (int s = 0; s < splits; s++) {
        if (dl && dl->timed_out) { free(tvals); free(tvlens); return NULL; }
        char bp[1024];
        bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
        BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
        if (!bm) continue;
        for (int i = 0; i < nt; i++) matches += bm_count(bm, tvals[i], tvlens[i]);
        bm_close(bm);
    }
    int total = get_live_count(db_root, object);
    size_t comp = ((size_t)total > matches) ? (size_t)total - matches : 0;

    KeySet *ks;
    if (matches <= comp) {
        ks = build_keyset_from_bitmap(db_root, object, splits, leaf, tf, dl);
    } else {
        ks = build_keyset_bitmap_complement(db_root, object, splits, leaf, dl,
                                            (const uint8_t (*)[1024])tvals,
                                            tvlens, nt, comp);
        if (ks) *inverted = 1;
    }
    free(tvals); free(tvlens);
    return ks;
}

/* Collect hashes from a single trigram's posting list (range scan
   on the .tg btree shards for key=trigram). Hashes land in a fresh
   KeySet allocated by the caller. */
typedef struct {
    KeySet        *ks;
    QueryDeadline *dl;
    int            timed_out;
} TgCollectCtx;

static int tg_collect_to_keyset_cb(const char *value, size_t vlen,
                                   const uint8_t hash[16], void *ctx) {
    (void)value; (void)vlen;
    TgCollectCtx *c = (TgCollectCtx *)ctx;
    if (c->dl && c->dl->timed_out) { c->timed_out = 1; return 1; }
    keyset_insert(c->ks, hash);
    return 0;
}

/* Counting callback for the sizing pass below — counts posting-list
   entries WITHOUT building the keyset, so we can allocate the keyset
   exactly to fit. KeySet is non-resizable; under-sizing silently
   drops inserts when the table fills, which used to truncate common-
   prefix trigrams ("ali", "the", ...) to 8192 hashes and produce wrong
   intersections downstream. */
/* Per-shard count cap. Counts above this are clamped to "≥ cap" — we
   only need approximate counts to order trigrams rarest-first, not
   exact ones. Walking every entry of a common trigram (could be
   500k+) just to count it was paying full I/O twice (count + collect),
   regressing common-substring queries from 160ms → 1200ms. With
   capped counts: ~24 ms walk overhead regardless of posting size. */
#define TG_COUNT_CAP_PER_SHARD 10000

typedef struct {
    size_t         count;
    QueryDeadline *dl;
    int            timed_out;
} TgCountCtx;

static int tg_count_cb(const char *value, size_t vlen,
                       const uint8_t hash[16], void *ctx) {
    (void)value; (void)vlen; (void)hash;
    TgCountCtx *c = (TgCountCtx *)ctx;
    if (c->dl && c->dl->timed_out) { c->timed_out = 1; return 1; }
    c->count++;
    if (c->count >= TG_COUNT_CAP_PER_SHARD) return 1;  /* early-exit */
    return 0;
}

/* (tg_keyset_for_trigram used to materialise a full per-trigram keyset
   via two passes — replaced by tg_intersect_streaming below which only
   materialises the seed (rarest trigram) and streams subsequent
   intersections via membership tests. Saves N-1 keyset allocations.) */

/* Build the intersection of two KeySets via iteration. Returns a new
   KeySet (caller owns). Iterates the smaller side, checks membership
   in the larger — keeps walk cost linear in min(|a|, |b|). */
typedef struct {
    KeySet *probe;
    KeySet *out;
} PairwiseIntersectCtx;

static int pairwise_intersect_cb(const uint8_t hash[16], void *ctx) {
    PairwiseIntersectCtx *c = (PairwiseIntersectCtx *)ctx;
    if (keyset_contains(c->probe, hash)) {
        keyset_insert(c->out, hash);
    }
    return 0;
}

static KeySet *keyset_pairwise_intersect(KeySet *a, KeySet *b)
    __attribute__((unused));
static KeySet *keyset_pairwise_intersect(KeySet *a, KeySet *b) {
    if (!a || !b) return NULL;
    KeySet *small = keyset_size(a) <= keyset_size(b) ? a : b;
    KeySet *big   = (small == a) ? b : a;
    KeySet *out = keyset_new(keyset_size(small));
    if (!out) return NULL;
    PairwiseIntersectCtx c = { big, out };
    keyset_iter(small, pairwise_intersect_cb, &c);
    return out;
}

/* Trigram-driven candidate set for OP_CONTAINS / OP_ICONTAINS. Returns
   a KeySet of record hashes that contain ALL trigrams from the pattern
   (with no false negatives — false positives are filtered by the
   per-record memmem verify step downstream).
   Patterns shorter than 3 chars cannot generate trigrams; this returns
   NULL so the caller falls back to full scan. */
/* Streaming-filter callback: keep only hashes that ARE in the running
   keyset, accumulating matches into a new keyset. Replaces building
   the full posting-set for every subsequent trigram. */
typedef struct {
    KeySet        *running;   /* hashes still alive after previous trigrams */
    KeySet        *next;      /* matches accumulate here */
    QueryDeadline *dl;
    int            timed_out;
} TgFilterCtx;

static int tg_filter_cb(const char *value, size_t vlen,
                        const uint8_t hash[16], void *ctx) {
    (void)value; (void)vlen;
    TgFilterCtx *c = (TgFilterCtx *)ctx;
    if (c->dl && c->dl->timed_out) { c->timed_out = 1; return 1; }
    if (keyset_contains(c->running, hash)) keyset_insert(c->next, hash);
    return 0;
}

/* Count a single trigram's posting-list size across all shards. Used
   to pick the rarest trigram first for the streaming-intersect loop
   below — gives a small seed keyset and dramatic speedup on long
   patterns whose extracted trigrams have wildly varying selectivity
   (e.g. "alice.smith0" has common "ali" and rare "h0X"). */
static size_t tg_posting_count(const char *db_root, const char *object,
                               const char *field, int splits,
                               const uint8_t trigram[3],
                               QueryDeadline *dl) {
    int idx_n = index_splits_for(splits);
    size_t total = 0;
    for (int s = 0; s < idx_n; s++) {
        char tp[PATH_MAX];
        tg_build_path(tp, sizeof(tp), db_root, object, field, s);
        TgCountCtx cc = { 0, dl, 0 };
        btree_range(tp, (const char *)trigram, 3,
                    (const char *)trigram, 3,
                    tg_count_cb, &cc);
        if (cc.timed_out) return 0;
        total += cc.count;
    }
    return total;
}

/* Walk all .tg shards for `trigram`, filtering `running` via membership
   test. Result keyset contains the intersection. `running` is freed. */
static KeySet *tg_intersect_streaming(const char *db_root, const char *object,
                                      const char *field, int splits,
                                      const uint8_t trigram[3],
                                      KeySet *running, QueryDeadline *dl) {
    if (!running || keyset_size(running) == 0) return running;
    int idx_n = index_splits_for(splits);

    /* Result size ≤ |running|. Size to fit. */
    KeySet *next = keyset_new(keyset_size(running));
    if (!next) { keyset_free(running); return NULL; }
    TgFilterCtx c = { running, next, dl, 0 };
    for (int s = 0; s < idx_n; s++) {
        char tp[PATH_MAX];
        tg_build_path(tp, sizeof(tp), db_root, object, field, s);
        btree_range(tp, (const char *)trigram, 3,
                    (const char *)trigram, 3,
                    tg_filter_cb, &c);
        if (c.timed_out) { keyset_free(running); keyset_free(next); return NULL; }
    }
    keyset_free(running);
    return next;
}

KeySet *build_keyset_from_trigram(const char *db_root, const char *object,
                                        int splits,
                                        const SearchCriterion *leaf,
                                        QueryDeadline *dl) {
    if (!leaf) return NULL;
    size_t plen = strlen(leaf->value);
    if (plen < 3) return NULL;

    uint8_t pattern_lc[1024];
    if (plen > sizeof(pattern_lc)) plen = sizeof(pattern_lc);
    /* Lowercase the pattern — index stores lowercase trigrams, so both
       OP_CONTAINS and OP_ICONTAINS land on the same posting lists.
       Per-record verify enforces final case-sensitivity. */
    for (size_t i = 0; i < plen; i++) {
        uint8_t c = (uint8_t)leaf->value[i];
        pattern_lc[i] = (c >= 'A' && c <= 'Z') ? (uint8_t)(c + 32) : c;
    }

    uint8_t trigrams[TG_MAX_DISTINCT][3];
    size_t  n = tg_extract_distinct(pattern_lc, plen, trigrams, TG_MAX_DISTINCT);
    if (n == 0) return NULL;

    /* Sort trigrams by posting size ascending — rarest first. The
       seed keyset is the smallest posting list (saves allocation +
       avoids the worst-case bloat); subsequent trigrams stream-
       filter via membership instead of materialising their own full
       keyset. For "alice.smith0" (10 trigrams of varying selectivity),
       this is the difference between materialising 10 × 800k
       keysets vs 1 × tiny + 9 stream walks that mostly early-out. */
    typedef struct { uint8_t tg[3]; size_t count; } TgEntry;
    TgEntry *order = malloc(n * sizeof(TgEntry));
    if (!order) return NULL;
    for (size_t i = 0; i < n; i++) {
        memcpy(order[i].tg, trigrams[i], 3);
        order[i].count = tg_posting_count(db_root, object, leaf->field,
                                          splits, trigrams[i], dl);
        if (dl && dl->timed_out) { free(order); return NULL; }
        /* Any zero-posting trigram → intersection is empty. */
        if (order[i].count == 0) {
            free(order);
            return keyset_new(16);  /* empty */
        }
    }
    /* Simple insertion sort — n ≤ TG_MAX_DISTINCT (4096) but typically <50. */
    for (size_t i = 1; i < n; i++) {
        TgEntry key = order[i];
        size_t j = i;
        while (j > 0 && order[j-1].count > key.count) {
            order[j] = order[j-1]; j--;
        }
        order[j] = key;
    }

    /* Seed from rarest trigram — sized exactly to its posting count. */
    KeySet *acc = keyset_new(order[0].count);
    if (!acc) { free(order); return NULL; }
    int idx_n = index_splits_for(splits);
    TgCollectCtx cc = { acc, dl, 0 };
    for (int s = 0; s < idx_n; s++) {
        char tp[PATH_MAX];
        tg_build_path(tp, sizeof(tp), db_root, object, leaf->field, s);
        btree_range(tp, (const char *)order[0].tg, 3,
                    (const char *)order[0].tg, 3,
                    tg_collect_to_keyset_cb, &cc);
        if (cc.timed_out) { keyset_free(acc); free(order); return NULL; }
    }

    /* Streaming intersect against each subsequent trigram. Early-exit
       once running is empty. */
    for (size_t i = 1; i < n && keyset_size(acc) > 0; i++) {
        acc = tg_intersect_streaming(db_root, object, leaf->field, splits,
                                     order[i].tg, acc, dl);
        if (!acc) { free(order); return NULL; }
    }
    free(order);
    return acc;
}

/* ============================================================
   A3 executor: trigram-prefix starts_with on a trigram-only field.

   Uses only the leading 3-gram (cheaper than build_keyset_from_trigram
   which intersects all grams). False positives filtered by
   criteria_match_tree's OP_STARTS_WITH check. */
static int find_via_trigram_starts_with(const char *db_root, const char *object,
                                        const Schema *sch, FieldSchema *fs,
                                        SearchCriterion *seed,
                                        CriteriaNode *tree,
                                        ExcludedKeys *excluded,
                                        int offset, int limit,
                                        const char **proj_fields, int proj_count,
                                        int dict_fmt,
                                        QueryDeadline *dl)
{
    size_t plen = strlen(seed->value);
    if (plen < 3) return 0;

    uint8_t gram[3];
    for (int i = 0; i < 3; i++) {
        uint8_t c = (uint8_t)seed->value[i];
        gram[i] = (c >= 'A' && c <= 'Z') ? (uint8_t)(c + 32) : c;
    }

    size_t posting_count = tg_posting_count(db_root, object, seed->field,
                                            sch->splits, gram, dl);
    if (dl->timed_out) return 0;
    if (posting_count == 0) return 0;

    KeySet *ks = keyset_new(posting_count);
    if (!ks) return 0;

    int idx_n = index_splits_for(sch->splits);
    TgCollectCtx cc = { ks, dl, 0 };
    for (int s = 0; s < idx_n; s++) {
        char tp[PATH_MAX];
        tg_build_path(tp, sizeof(tp), db_root, object, seed->field, s);
        btree_range(tp, (const char *)gram, 3, (const char *)gram, 3,
                    tg_collect_to_keyset_cb, &cc);
        if (cc.timed_out) { keyset_free(ks); return 0; }
    }
    int rc = keyset_emit_find(db_root, object, sch, ks,
                              tree, excluded, offset, limit,
                              proj_fields, proj_count,
                              fs, 0 /*rows_fmt*/, dict_fmt, 0 /*csv_delim*/,
                              NULL /*joins*/, 0 /*njoins*/, dl);
    keyset_free(ks);
    return rc;
}

KeySet *build_keyset_from_leaf(const char *db_root, const char *object,
                                      int splits,
                                      SearchCriterion *leaf,
                                      QueryDeadline *dl) {
    /* Dispatch on the plan-time picker's choice — single index per leaf,
       no runtime cascade between index types.  If the picker says
       IT_TRIGRAM and the trigram builder returns NULL (e.g. transient
       alloc failure), caller drops to full scan rather than retrying
       via btree.  The picker has already considered all available
       indexes for this (field, op, pattern-length) tuple. */
    int picked = pick_index_for_leaf(db_root, object, leaf);
    if (picked < 0) return NULL;
    if (picked == IT_TRIGRAM) {
        return build_keyset_from_trigram(db_root, object, splits, leaf, dl);
    }
    if (picked == IT_BITMAP) {
        TypedSchema *ts = load_typed_schema(db_root, object);
        const TypedField *tf = resolve_idx_field(ts, leaf->field);
        return build_keyset_from_bitmap(db_root, object, splits,
                                        leaf, tf, dl);
    }
    /* IT_BTREE — tiered KeySet allocation. Pre-2026-05-25 this function pre-allocated
       a KeySet sized to (file-size hint floored at live count), which on a
       25M-row table works out to ~1.2 GB pre-allocation — exceeds the
       default QUERY_BUFFER_MB=256 budget guard → return NULL → every
       caller falls back to per-record scan. Measured: an eq lookup on a
       unique field that should return 0-1 rows took 23 seconds because
       the budget guard refused to allocate.

       Two-tier with overflow detection fixes that without per-operator
       heuristics:

         Tier 1: 64K slots (~1.5 MB) — fits the typical selective query
                 (unique-field eq, narrow range, small in-set).
         Tier 2: bounded by g_query_buffer_max_bytes — fits moderate-
                 selectivity range/in/OR queries.
         NULL  : even tier 2 overflowed → caller falls back to full
                 scan. At that selectivity (millions of matches) the
                 filter-first prefilter wouldn't have helped much anyway.

       On tier-1 overflow we pay one extra btree walk to retry at tier 2.
       Worth it: the alternative is the 23-second full ordered walk. */
    const size_t bytes_per_entry = 2 * (sizeof(uint8_t[16]) + sizeof(uint32_t));
    const size_t budget_max_hint = g_query_buffer_max_bytes / bytes_per_entry;

    size_t tier1 = 65536;
    size_t tier2 = leaf_capacity_hint(db_root, object, leaf->field, splits);
    int live = get_live_count(db_root, object);
    if (live > 0 && (size_t)live > tier2) tier2 = (size_t)live;
    if (tier2 > budget_max_hint) tier2 = budget_max_hint;
    if (tier2 < tier1) tier2 = tier1;   /* tiny table — both tiers same */

    TypedSchema *ts = load_typed_schema(db_root, object);
    const TypedField *tf = resolve_idx_field(ts, leaf->field);

    size_t hints[2] = { tier1, tier2 };
    int try_count = (tier1 == tier2) ? 1 : 2;
    for (int t = 0; t < try_count; t++) {
        /* coverity[tainted_data] CID 1693849: hint is bounded above by
           the budget_max_hint clamp during tier2 derivation. */
        KeySet *ks = keyset_new(hints[t]);
        if (!ks) return NULL;
        IntersectCollectCtx c = { ks, dl, 0, 0 };
        btree_dispatch(db_root, object, leaf->field, splits,
                       leaf, tf, intersect_collect_cb, &c);
        if (dl->timed_out) { keyset_free(ks); return NULL; }
        if (!c.overflowed) return ks;
        keyset_free(ks);
        /* Tier-1 overflowed; loop retries at tier 2.  Tier-2 overflow
           exits the loop and returns NULL. */
    }
    return NULL;
}

/* Below this threshold for the most-selective leaf, fan out the candidates as
   record fetches and post-filter via criteria_match_tree — cheaper than
   walking remaining indexed leaves' btrees in full just to confirm.

   Cost model (1M-row table, 1M-entry second leaf):
     N candidates, fetch+filter:           N × ~1.2µs
     full second-leaf btree walk + probe:  ~M × ~0.1µs
   Crossover at N ≈ M/12. 10000 is conservative-correct: falls back when
   fallback is clearly faster, stays on intersection when both sides large. */
#define INTERSECT_MIN_PRIMARY 10000

/* Per-leaf build worker for the parallel intersect path. Walks one leaf's
   btree into a fresh KeySet stored at per_leaf[my_idx]; on failure leaves
   it NULL so the caller can detect partial completion. */
typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    SearchCriterion  *leaf;
    KeySet          **slot;       /* &per_leaf[my_idx] */
    QueryDeadline    *deadline;
} LeafBuildArg;

static void *leaf_build_worker(void *arg) {
    LeafBuildArg *a = (LeafBuildArg *)arg;
    *a->slot = build_keyset_from_leaf(a->db_root, a->object, a->splits,
                                      a->leaf, a->deadline);
    return NULL;
}

/* Walks the smallest per-leaf KeySet, keeping only entries that are present
   in every other leaf's KeySet. Used by the parallel-build intersect path. */
typedef struct {
    KeySet **per_leaf;
    int      n;
    int      smallest_i;
    KeySet  *out;
    QueryDeadline *deadline;
    int      dl_counter;
} IntersectAllCtx;

static int intersect_all_cb(const uint8_t hash[16], void *ctx) {
    IntersectAllCtx *ic = (IntersectAllCtx *)ctx;
    if (query_deadline_tick(ic->deadline, &ic->dl_counter)) return -1;
    for (int i = 0; i < ic->n; i++) {
        if (i == ic->smallest_i) continue;
        if (!keyset_contains(ic->per_leaf[i], hash)) return 0;
    }
    if (keyset_insert(ic->out, hash) < 0) return -1;
    return 0;
}

/* Intersect N indexed leaves' candidate hash sets. Two strategies:

   - n <= 2: SERIAL (original path). Leaf 0 builds seed; each subsequent
     leaf walks its btree and probes the running set into next, swap.
     The seed-then-probe pattern minimises peak memory (~|seed|), and
     for 2 leaves there's no parallelism win — both walks would block
     on the same shard pool anyway.

   - n > 2: PARALLEL. Walk every leaf's btree in parallel into per-leaf
     KeySets (max wall = max per-leaf walk time), then walk the smallest
     and keep only entries present in all others. For 3+ leaves with
     similar walk costs this collapses N × walk into 1 × walk + final
     intersect. Falls back to serial if the pool can't host n outer
     tasks safely.

   Small-primary heuristic preserved for both: when the smallest set is
   under INTERSECT_MIN_PRIMARY, sets *out_small_primary=1 and returns
   that set — caller fans out to record-fetch + full-tree post-filter,
   which beats walking the rest of the leaves in full.

   Caller frees the returned KeySet. */
KeySet *intersect_indexed_leaves(const char *db_root, const char *object,
                                        int splits,
                                        SearchCriterion **leaves, int n,
                                        QueryDeadline *dl,
                                        int *out_small_primary) {
    *out_small_primary = 0;
    if (n < 2) return NULL;

    /* Parallel build per-leaf KeySets when n > 2. Nested parallel_for is
       structurally safe (work-stealing in parallel.c). For n == 2 the
       parallel build doesn't beat serial seed+probe because the final
       intersect step touches |seed| × 1 lookup, roughly cancelling the
       saved second walk. */
    if (n > 2 && parallel_pool_size() > 0) {
        /* Parallel build per-leaf sets, then walk smallest and keep entries
           present in all others. */
        KeySet **per_leaf = calloc((size_t)n, sizeof(KeySet *));
        LeafBuildArg *args = calloc((size_t)n, sizeof(LeafBuildArg));
        if (!per_leaf || !args) { free(per_leaf); free(args); return NULL; }
        for (int i = 0; i < n; i++) {
            args[i].db_root  = db_root;
            args[i].object   = object;
            args[i].splits   = splits;
            args[i].leaf     = leaves[i];
            args[i].slot     = &per_leaf[i];
            args[i].deadline = dl;
        }
        parallel_for(leaf_build_worker, args, n, sizeof(LeafBuildArg));
        free(args);

        if (dl->timed_out) {
            for (int i = 0; i < n; i++) keyset_free(per_leaf[i]);
            free(per_leaf);
            return NULL;
        }
        for (int i = 0; i < n; i++) {
            if (!per_leaf[i]) {
                /* Build failed for this leaf — clean up and bail. */
                for (int k = 0; k < n; k++) keyset_free(per_leaf[k]);
                free(per_leaf);
                return NULL;
            }
        }

        int smallest_i = 0;
        for (int i = 1; i < n; i++) {
            if (keyset_size(per_leaf[i]) < keyset_size(per_leaf[smallest_i]))
                smallest_i = i;
        }

        if (keyset_size(per_leaf[smallest_i]) < INTERSECT_MIN_PRIMARY) {
            *out_small_primary = 1;
            KeySet *result = per_leaf[smallest_i];
            for (int i = 0; i < n; i++) if (i != smallest_i) keyset_free(per_leaf[i]);
            free(per_leaf);
            return result;
        }

        KeySet *result = keyset_new(keyset_size(per_leaf[smallest_i]));
        if (!result) {
            for (int i = 0; i < n; i++) keyset_free(per_leaf[i]);
            free(per_leaf);
            return NULL;
        }
        IntersectAllCtx ic = { per_leaf, n, smallest_i, result, dl, 0 };
        keyset_iter(per_leaf[smallest_i], intersect_all_cb, &ic);

        for (int i = 0; i < n; i++) keyset_free(per_leaf[i]);
        free(per_leaf);
        if (dl->timed_out) { keyset_free(result); return NULL; }
        return result;
    }

    /* Serial path (n == 2 or pool unavailable). */
    KeySet *running = build_keyset_from_leaf(db_root, object, splits, leaves[0], dl);
    if (!running || dl->timed_out) { keyset_free(running); return NULL; }

    /* Small-primary heuristic: skip the second-leaf btree walk; let the
       caller fan out a record-fetch + post-filter pass instead. */
    if (keyset_size(running) < INTERSECT_MIN_PRIMARY) {
        *out_small_primary = 1;
        return running;
    }

    TypedSchema *ts = load_typed_schema(db_root, object);

    for (int i = 1; i < n; i++) {
        size_t cur = keyset_size(running);
        if (cur == 0) break;  /* empty intersection — short-circuit */

        KeySet *next = keyset_new(cur);  /* intersection cardinality ≤ |running| */
        if (!next) { keyset_free(running); return NULL; }

        IntersectProbeCtx p = { running, next, dl, 0 };
        btree_dispatch(db_root, object, leaves[i]->field, splits,
                       leaves[i], resolve_idx_field(ts, leaves[i]->field),
                       intersect_probe_cb, &p);

        keyset_free(running);
        running = next;
        if (dl->timed_out) { keyset_free(running); return NULL; }
    }
    return running;
}

/* Iterate a KeySet, computing shard_id + start_slot from each hash, into a
   CollectedHash[] suitable for parallel_indexed_count / process_batch /
   parallel_indexed_agg. Caller frees *out_entries. */
typedef struct {
    CollectedHash *entries;
    size_t count;
    size_t cap;
    int splits;
} KeysetToBatchCtx;

static int keyset_to_batch_cb(const uint8_t hash[16], void *ctx) {
    KeysetToBatchCtx *kc = (KeysetToBatchCtx *)ctx;
    if (kc->count >= kc->cap) return -1;  /* shouldn't happen — cap = keyset_size */
    CollectedHash *e = &kc->entries[kc->count++];
    memcpy(e->hash, hash, 16);
    e->shard_id = compute_record_shard(hash, kc->splits);
    e->start_slot = 0;
    return 0;
}

int keyset_to_collected_hashes(KeySet *ks, int splits,
                                      CollectedHash **out_entries, size_t *out_count) {
    size_t cap = keyset_size(ks);
    if (cap == 0) { *out_entries = NULL; *out_count = 0; return 0; }
    CollectedHash *entries = malloc(cap * sizeof(CollectedHash));
    if (!entries) return -1;
    KeysetToBatchCtx kc = { entries, 0, cap, splits };
    keyset_iter(ks, keyset_to_batch_cb, &kc);
    *out_entries = entries;
    *out_count = kc.count;
    return 0;
}

/* ========== OR index-union (KeySet fast path) ========== */

typedef struct {
    const char *db_root;
    const char *object;
    int splits;
    const SearchCriterion *leaf;
    KeySet *ks;
    QueryDeadline *deadline;
    int dl_counter;
    /* Short-circuit cap: when > 0, the OR-build callback returns -1 once
       the shared KeySet's live count reaches this value, halting further
       btree walks. Set by build_or_keyset's caller for pure-OR find with
       a known offset+limit; left at 0 for count/aggregate or hybrid cases
       that genuinely need every candidate. */
    int target_count;
} OrChildWorkerCtx;

/* btree callback — drops every hit into the shared KeySet. Returns -1 to
   halt the btree walk on deadline trip, on capacity exhaustion (insert
   failure → O(cap) probe per call if we kept trying), OR when the caller-
   supplied target_count has been reached (limit pushdown for find). */
static int or_collect_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    (void)val; (void)vlen;
    OrChildWorkerCtx *w = (OrChildWorkerCtx *)ctx;
    if (query_deadline_tick(w->deadline, &w->dl_counter)) return -1;
    if (keyset_insert(w->ks, hash16) < 0) return -1;
    if (w->target_count > 0 &&
        atomic_load_explicit(&w->ks->n, memory_order_relaxed) >= (size_t)w->target_count) {
        return -1;
    }
    return 0;
}

static void *or_child_worker(void *arg) {
    OrChildWorkerCtx *w = (OrChildWorkerCtx *)arg;
    TypedSchema *ts = load_typed_schema(w->db_root, w->object);
    btree_dispatch(w->db_root, w->object, w->leaf->field, w->splits,
                   (SearchCriterion *)w->leaf,
                   resolve_idx_field(ts, w->leaf->field),
                   or_collect_cb, w);
    return NULL;
}

/* Build a KeySet by unioning the index lookups of every child of `or_node`.
   Every child must be a LEAF with an index (verified by the planner).
   Caller owns the returned KeySet and must keyset_free() it.
   If the OR's estimated capacity would exceed the per-query buffer cap, returns
   NULL and sets *out_budget_exceeded = 1 when the pointer is non-NULL.

   target_count: when > 0, the OR walks halt as soon as the KeySet's live
   count reaches this value. Caller supplies this only when the result will
   be consumed without re-matching against a wider tree (i.e., need_rematch
   is false). For find with limit=N, callers pass N + offset; for count and
   aggregate, callers pass 0 (need every candidate). */
KeySet *build_or_keyset(const char *db_root, const char *object, int splits,
                               const CriteriaNode *or_node, QueryDeadline *dl,
                               int *out_budget_exceeded, int target_count) {
    int n = or_node->n_children;
    if (n <= 0) return NULL;

    /* The OR union is bounded by live_count — you can't have more matches
       than rows. Sizing the KeySet to (live × 2) handles every possible
       OR query correctly without any per-leaf cardinality guessing.

       The previous estimate (file_size / 64 summed across leaves, floored
       at live) catastrophically over-allocated for OR-of-eq: 5 leaves on
       a 1M-row int field summed to ~3.9M entries and alloc'd a 160MB
       KeySet that the actual ~50k union filled 3000x sparsely. calloc
       and first-touch on 160MB dominated wall time on every OR query.

       Using `live` directly drops that to a 32MB alloc (cap = 2 × live
       × 20 bytes for keys + state) — 5x smaller, no overflow risk, no
       per-operator/per-type heuristics that could be wrong. */
    int live_int = get_live_count(db_root, object);
    size_t est_total = (size_t)(live_int > 0 ? live_int : 1024);
    if (est_total < 1024) est_total = 1024;

    /* Budget check: KeySet alloc is O(cap_pow2 * 24 bytes) — keys[] + state[].
       Cap est_total to fit the per-query buffer cap. */
    size_t ks_cap_guess = est_total * 2; /* keyset_new doubles + rounds up */
    size_t ks_bytes = ks_cap_guess * (sizeof(uint8_t[16]) + sizeof(uint32_t));
    if (ks_bytes > g_query_buffer_max_bytes) {
        if (out_budget_exceeded) *out_budget_exceeded = 1;
        return NULL;
    }

    /* coverity[tainted_data] CID 1693838: est_total is bounded above by
       the explicit g_query_buffer_max_bytes check immediately above. */
    KeySet *ks = keyset_new(est_total);
    if (!ks) return NULL;

    OrChildWorkerCtx *ctxs = calloc(n, sizeof(OrChildWorkerCtx));
    for (int i = 0; i < n; i++) {
        CriteriaNode *c = or_node->children[i];
        ctxs[i].db_root = db_root;
        ctxs[i].object = object;
        ctxs[i].splits = splits;
        ctxs[i].leaf = &c->leaf;
        ctxs[i].ks = ks;
        ctxs[i].deadline = dl;
        ctxs[i].target_count = target_count;
    }

    /* Parallel across OR children via parallel_for. or_child_worker calls
       btree_dispatch which itself uses parallel_for for shard fan-out;
       work-stealing in parallel_for makes that nesting structurally safe
       (a thread blocked on its own group always helps drain the queue),
       so no n-vs-pool-size guard is needed.

       Stay serial only when:
         - pool not running (CLI mode) — parallel_for runs inline anyway
         - n == 1 — parallel_for runs inline anyway
         - target_count > 0 (find with limit): between-child cap check
           short-circuits remaining children once the limit is hit,
           which doesn't translate cleanly to parallel children. */
    int do_parallel = (target_count == 0 && n > 1 && parallel_pool_size() > 0);
    if (do_parallel) {
        parallel_for(or_child_worker, ctxs, n, sizeof(OrChildWorkerCtx));
    } else {
        for (int i = 0; i < n; i++) {
            if (target_count > 0 &&
                atomic_load_explicit(&ks->n, memory_order_relaxed) >= (size_t)target_count) {
                break;
            }
            or_child_worker(&ctxs[i]);
        }
    }

    free(ctxs);
    return ks;
}

/* (legacy KeysetRecordRead / read_record_by_hash / release_record_read removed
   2026-05-07 — superseded by RecordRef + read_record_ref/release_record_ref in
   the storage-version-agnostic dispatch helpers above.) */

/* Emit records matching `tree` by iterating a KeySet built from the OR branch.
   Honours offset/limit at emit time. Excluded keys, joins, projections, and
   rows_fmt all mirror the behaviour of idx_find_parallel / adv_search_cb.
   Returns the number of rows printed. */
/* Walk a pre-built KeySet, fetch each record, optionally re-match against a
   tree, and emit per the requested format. Used by both OR-union and AND-
   intersection paths (the difference is just how the KeySet was built and
   whether rematch is needed). Caller owns the KeySet lifecycle. */

typedef struct {
    uint8_t (*hashes)[16];
    size_t   n;
    uint8_t **keys; size_t *klens;
    uint8_t **vals; size_t *vlens;
    int     *found;
} KefFetchCtx;

static int kef_fetch_cb(const uint8_t hash16[16],
                         const void *key, size_t klen,
                         const void *value, size_t vlen,
                         void *ctx_ptr) {
    KefFetchCtx *c = (KefFetchCtx *)ctx_ptr;
    for (size_t i = 0; i < c->n; i++) {
        if (memcmp(hash16, c->hashes[i], 16) == 0) {
            c->keys[i] = malloc(klen);
            if (c->keys[i]) { memcpy(c->keys[i], key, klen); c->klens[i] = klen; }
            c->vals[i] = malloc(vlen);
            if (c->vals[i]) { memcpy(c->vals[i], value, vlen); c->vlens[i] = vlen; }
            c->found[i] = 1;
            break;
        }
    }
    return 0;
}

static int keyset_emit_find(const char *db_root, const char *object,
                            const Schema *sch, KeySet *ks,
                            CriteriaNode *tree_for_rematch,  /* NULL = skip */
                            ExcludedKeys *excluded, int offset, int limit,
                            const char **proj_fields, int proj_count,
                            FieldSchema *fs, int rows_fmt, int dict_fmt, char csv_delim,
                            JoinSpec *joins, int njoins, QueryDeadline *dl) {
    int need_rematch = (tree_for_rematch != NULL);
    CriteriaNode *tree = tree_for_rematch;
    (void)tree;  /* used only when need_rematch */

    /* Phase 1: collect hashes from KeySet and batch-fetch. */
    size_t n_fetch = 0;
    for (size_t b = 0; b < ks->cap; b++)
        if (ks->state[b] == 2) n_fetch++;

    int printed = 0;
    if (n_fetch == 0) return 0;

    uint8_t (*hashes)[16] = malloc(n_fetch * sizeof(*hashes));
    int *key_to_fetch = malloc(ks->cap * sizeof(int));
    if (!hashes || !key_to_fetch) {
        free(hashes); free(key_to_fetch);
        return 0;
    }
    for (size_t b = 0; b < ks->cap; b++) key_to_fetch[b] = -1;

    {
        size_t idx = 0;
        for (size_t b = 0; b < ks->cap; b++) {
            if (ks->state[b] == 2) {
                memcpy(hashes[idx], ks->keys[b], 16);
                key_to_fetch[b] = (int)idx;
                idx++;
            }
        }
    }

    SlotcaskSchemaInfo sinfo = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);
    if (!sdb) {
        free(hashes);
        free(key_to_fetch);
        return 0;
    }

    /* Batch-fetch results: parallel arrays indexed by hash position. */
    uint8_t **fkeys = calloc(n_fetch, sizeof(*fkeys));
    size_t  *fklens = calloc(n_fetch, sizeof(*fklens));
    uint8_t **fvals = calloc(n_fetch, sizeof(*fvals));
    size_t  *fvlens = calloc(n_fetch, sizeof(*fvlens));
    int     *ffound = calloc(n_fetch, sizeof(*ffound));
    if (!fkeys || !fklens || !fvals || !fvlens || !ffound) {
        free(hashes); free(key_to_fetch);
        free(fkeys); free(fklens); free(fvals); free(fvlens); free(ffound);
        return 0;
    }

    KefFetchCtx fctx = { hashes, n_fetch, fkeys, fklens, fvals, fvlens, ffound };

#define KEF_BATCH 1024
    {
        size_t processed = 0;
        while (processed < n_fetch) {
            size_t batch = n_fetch - processed;
            if (batch > KEF_BATCH) batch = KEF_BATCH;
            slotcask_bulk_resolve_and_fetch(sdb, hashes + processed,
                                             batch, &fctx, kef_fetch_cb);
            processed += batch;
        }
    }
#undef KEF_BATCH

    /* Phase 2: sequential emit using fetched records. */
    {
        int count = 0;
        int dl_counter = 0;
        for (size_t b = 0; b < ks->cap && (limit <= 0 || printed < limit); b++) {
            if (query_deadline_tick(dl, &dl_counter)) break;
            if (ks->state[b] != 2) continue;
            int fi = key_to_fetch[b];
            if (fi < 0 || !ffound[fi]) continue;

            char keybuf[1100];
            {
                const Schema *sc_p = (fs && fs->auto_key != AK_NONE)
                                      ? &fs->auto_key_schema_snapshot : NULL;
                format_wire_key(sc_p, (const char *)fkeys[fi], fklens[fi], keybuf, sizeof(keybuf));
            }

            if (is_excluded(excluded, keybuf)) continue;

            const uint8_t *raw = fvals[fi];
            uint32_t value_len = (uint32_t)fvlens[fi];
            if (need_rematch && !criteria_match_tree(raw, tree, fs)) continue;

            {  /* Single-iteration block — no probe loop in the dispatched path. */

                RecordRef *jrr = NULL;
                const uint8_t **jraws = NULL;
                int dropped = 0;
                if (njoins > 0) {
                    jrr = calloc(njoins, sizeof(RecordRef));
                    jraws = calloc(njoins, sizeof(const uint8_t *));
                    for (int i = 0; i < njoins; i++) {
                        char lk[1024];
                        int llen = extract_local_key(&joins[i], raw,
                                                     fs ? fs->ts : NULL, lk, sizeof(lk));
                        int jfound = 0;
                        if (llen > 0) {
                            jfound = lookup_remote(&joins[i], db_root, lk, (size_t)llen,
                                                   &jrr[i]);
                            if (jfound) jraws[i] = jrr[i].val;
                        }
                        if (!jfound && joins[i].type == JOIN_INNER) { dropped = 1; break; }
                    }
                }

                if (!dropped) {
                    count++;
                    if (count > offset && (limit <= 0 || printed < limit)) {
                        if (njoins > 0 && csv_delim) {
                            char buf[16384];
                            size_t n = build_joined_csv_row(
                                keybuf, raw, fs,
                                proj_count > 0 ? proj_fields : NULL, proj_count,
                                joins, njoins, jraws, csv_delim,
                                buf, sizeof(buf));
                            OUT("%.*s", (int)n, buf);
                        } else if (njoins > 0) {
                            char buf[16384];
                            size_t pos = 0;
                            SB_APPEND(buf, pos, sizeof(buf), "[\"%s\"", keybuf);
                            size_t wrote = buf_driver_values(
                                                raw, fs,
                                                proj_count > 0 ? proj_fields : NULL,
                                                proj_count,
                                                buf + pos, sizeof(buf) - pos);
                            pos += wrote;
                            if (pos >= sizeof(buf)) pos = sizeof(buf) - 1;
                            for (int i = 0; i < njoins; i++) {
                                if (!jraws[i]) {
                                    int ncols = joins[i].proj_count > 0
                                                ? joins[i].proj_count
                                                : (joins[i].remote_fs.ts
                                                   ? joins[i].remote_fs.ts->nfields : 0);
                                    if (joins[i].include_remote_key) ncols++;
                                    for (int k = 0; k < ncols; k++)
                                        SB_APPEND(buf, pos, sizeof(buf), ",null");
                                } else {
                                    wrote = buf_join_values(&joins[i], jraws[i],
                                                            buf + pos, sizeof(buf) - pos);
                                    pos += wrote;
                                    if (pos >= sizeof(buf)) pos = sizeof(buf) - 1;
                                }
                            }
                            SB_APPEND(buf, pos, sizeof(buf), "]");
                            OUT("%s%s", printed ? "," : "", buf);
                        } else if (csv_delim) {
                            csv_emit_row(keybuf, raw, value_len,
                                         proj_count > 0 ? proj_fields : NULL,
                                         proj_count, fs, csv_delim);
                        } else if (rows_fmt) {
                            OUT("%s[\"%s\"", printed ? "," : "", keybuf);
                            if (proj_count > 0) {
                                for (int j = 0; j < proj_count; j++) {
                                    char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                                            proj_fields[j], fs));
                                    OUT(",\"%s\"", pv ? pv : "");
                                    free(pv);
                                }
                            } else if (fs && fs->ts) {
                                for (int j = 0; j < fs->ts->nfields; j++) {
                                    if (fs->ts->fields[j].removed) continue;
                                    char *pv = json_escape_field(typed_get_field_str(fs->ts, raw, (int)value_len, j));
                                    OUT(",\"%s\"", pv ? pv : "");
                                    free(pv);
                                }
                            }
                            OUT("]");
                        } else if (dict_fmt) {
                            OUT("%s\"%s\":", printed ? "," : "", keybuf);
                            if (proj_count > 0) {
                                OUT("{");
                                int first = 1;
                                for (int j = 0; j < proj_count; j++) {
                                    char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                                            proj_fields[j], fs));
                                    if (!pv) continue;
                                    OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                                    first = 0;
                                    free(pv);
                                }
                                OUT("}");
                            } else {
                                char *v = decode_value((const char *)raw, value_len, fs);
                                OUT("%s", v);
                                free(v);
                            }
                        } else if (proj_count > 0) {
                            OUT("%s{\"key\":\"%s\",\"value\":{", printed ? "," : "", keybuf);
                            int first = 1;
                            for (int j = 0; j < proj_count; j++) {
                                char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                                        proj_fields[j], fs));
                                if (!pv) continue;
                                OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                                first = 0;
                                free(pv);
                            }
                            OUT("}}");
                        } else {
                            char *v = decode_value((const char *)raw, value_len, fs);
                            OUT("%s{\"key\":\"%s\",\"value\":%s}", printed ? "," : "", keybuf, v);
                            free(v);
                        }
                        printed++;
                    }
                }

                if (jrr) {
                    for (int i = 0; i < njoins; i++) release_record_ref(&jrr[i]);
                    free(jrr); free(jraws);
                }
            }
        }
    }

    /* Cleanup batch-fetched records. */
    for (size_t i = 0; i < n_fetch; i++) {
        free(fkeys[i]); free(fvals[i]);
    }
    free(hashes); free(key_to_fetch);
    free(fkeys); free(fklens); free(fvals); free(fvlens); free(ffound);

    return printed;
}

/* OR index-union find: build KeySet from indexed-OR children, then emit. */
static int keyset_find_from_or(const char *db_root, const char *object,
                               const Schema *sch, CriteriaNode *tree,
                               CriteriaNode *or_node,
                               ExcludedKeys *excluded, int offset, int limit,
                               const char **proj_fields, int proj_count,
                               FieldSchema *fs, int rows_fmt, int dict_fmt, char csv_delim,
                               JoinSpec *joins, int njoins,
                               QueryDeadline *dl, int *out_budget_exceeded,
                               size_t *out_total) {
    /* Pure-OR (root IS the or_node, OR root is a single-child AND wrapping
       the or_node — common shape parsed from `[{"or":[...]}]`). In both,
       every KeySet member already matches the full tree, so we can skip
       the rematch AND push the limit down into the OR build to short-
       circuit the btree walks. Hybrid AND+OR (multi-child AND) still
       needs the rematch and an unbounded build. */
    int rematch_unnecessary =
        (tree == or_node) || (tree->kind == CNODE_OR) ||
        (tree->kind == CNODE_AND && tree->n_children == 1 &&
         tree->children[0] == or_node);
    int need_rematch = !rematch_unnecessary;

    /* When out_total is requested, build the full union (no short-circuit)
       so |KeySet| reflects every match, not just the first offset+limit. */
    int target = (out_total == NULL && !need_rematch && limit > 0) ? offset + limit : 0;

    KeySet *ks = build_or_keyset(db_root, object, sch->splits, or_node, dl,
                                 out_budget_exceeded, target);
    if (!ks) return 0;
    if (dl->timed_out) { keyset_free(ks); return 0; }

    if (out_total) *out_total = keyset_size(ks);

    int rc = keyset_emit_find(db_root, object, sch, ks,
                              need_rematch ? tree : NULL,
                              excluded, offset, limit,
                              proj_fields, proj_count, fs, rows_fmt, dict_fmt, csv_delim,
                              joins, njoins, dl);
    keyset_free(ks);
    return rc;
}

/* AND index-intersection find: build KeySet from intersection of N indexed
   leaves (most-selective first), then emit. When the first leaf is small,
   skip the second-leaf btree walks and pass the full tree as post-filter via
   keyset_emit_find. */
static int keyset_find_from_intersect(const char *db_root, const char *object,
                                      const Schema *sch, const FilterPlan *fp,
                                      CriteriaNode *tree,
                                      ExcludedKeys *excluded, int offset, int limit,
                                      const char **proj_fields, int proj_count,
                                      FieldSchema *fs, int rows_fmt, int dict_fmt, char csv_delim,
                                      JoinSpec *joins, int njoins, QueryDeadline *dl,
                                      size_t *out_total) {
    int small_primary = 0;
    KeySet *ks = intersect_indexed_leaves(db_root, object, sch->splits,
                                          (SearchCriterion **)fp->source_leaves,
                                          fp->n_source, dl,
                                          &small_primary);
    if (!ks) return 0;
    /* Small primary OR partial intersect: pass the full tree so emit
       re-checks every leaf (including any bitmap/OR/non-rangeable child
       the planner kept out of the intersect) via criteria_match_tree.
       Big primary with full intersect: intersection already exact, skip rematch. */
    int need_rematch = small_primary || (fp->n_postfilter > 0);

    /* For FP_INTERSECT total: |KeySet| is the exact match count only when the
       full intersection was performed (big primary, no post-filter). When
       small_primary=1 the KeySet is merely the first leaf's entries (upper bound,
       not the intersection), and n_postfilter>0 means additional criteria were
       not materialized into the keyset. In both imprecise cases, leave total=null
       (out_total untouched) so the caller emits null rather than a wrong number. */
    if (out_total && !small_primary && fp->n_postfilter == 0)
        *out_total = keyset_size(ks);

    int rc = keyset_emit_find(db_root, object, sch, ks,
                              need_rematch ? tree : NULL,
                              excluded, offset, limit,
                              proj_fields, proj_count, fs, rows_fmt, dict_fmt, csv_delim,
                              joins, njoins, dl);
    keyset_free(ks);
    return rc;
}


typedef struct {
    CriteriaNode  *tree;
    FieldSchema   *fs;
    QueryDeadline *dl;
    int            dl_counter;
    size_t         count;
} OrCountCtx;

static int or_count_batch_cb(const uint8_t hash16[16],
                              const void *key, size_t klen,
                              const void *value, size_t vlen,
                              void *ctx_ptr) {
    (void)hash16; (void)key; (void)klen;
    OrCountCtx *c = (OrCountCtx *)ctx_ptr;
    if (query_deadline_tick(c->dl, &c->dl_counter)) return 1;
    if (criteria_match_tree((const uint8_t *)value, c->tree, c->fs))
        __sync_fetch_and_add(&c->count, 1);
    return 0;
}

/* Count records matching `tree` by iterating a KeySet built from the OR branch.
   For pure-OR trees (root IS the or_node), returns |KeySet| directly.
   For hybrid (AND + OR), re-matches each keyed record against the full tree. */
static size_t keyset_count_from_or(const char *db_root, const char *object,
                                   const Schema *sch, CriteriaNode *tree,
                                   CriteriaNode *or_node, FieldSchema *fs,
                                   QueryDeadline *dl, int *out_budget_exceeded) {
    /* Count needs the full union — no short-circuit. */
    KeySet *ks = build_or_keyset(db_root, object, sch->splits, or_node, dl,
                                 out_budget_exceeded, 0);
    if (!ks) return 0;
    if (dl->timed_out) { keyset_free(ks); return 0; }

    /* Pure-OR (root IS the OR, OR root is a single-child AND wrapping
       the OR — common shape from `criteria:[{"or":[...]}]` parsed as
       AND-of-one). No AND siblings → no per-record re-match needed,
       just return the keyset size. */
    int is_pure_or = (tree == or_node || tree->kind == CNODE_OR ||
                       (tree->kind == CNODE_AND && tree->n_children == 1 &&
                        tree->children[0] == or_node));
    if (is_pure_or) {
        size_t n = keyset_size(ks);
        keyset_free(ks);
        return n;
    }

    /* Hybrid: batch-fetch keyed records and apply full tree match. */
    {
        size_t n_hashes = 0;
        for (size_t b = 0; b < ks->cap; b++)
            if (ks->state[b] == 2) n_hashes++;

        if (n_hashes == 0) { keyset_free(ks); return 0; }

        uint8_t (*hashes)[16] = malloc(n_hashes * sizeof(*hashes));
        if (!hashes) {
            /* Fallback: sequential per-record */
            size_t n = 0;
            int dl_counter = 0;
            for (size_t b = 0; b < ks->cap; b++) {
                if (query_deadline_tick(dl, &dl_counter)) break;
                if (ks->state[b] != 2) continue;
                RecordRef rr;
                if (read_record_ref(db_root, object, sch, ks->keys[b], &rr) != 0) continue;
                if (criteria_match_tree(rr.val, tree, fs)) n++;
                release_record_ref(&rr);
            }
            keyset_free(ks);
            return n;
        }

        size_t idx = 0;
        for (size_t b = 0; b < ks->cap; b++)
            if (ks->state[b] == 2)
                memcpy(hashes[idx++], ks->keys[b], 16);

        SlotcaskSchemaInfo sinfo = {
            .splits = sch->splits, .slot_size = sch->slot_size,
            .streams = sch->streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);
        if (!sdb) {
            free(hashes);
            keyset_free(ks);
            return 0;
        }

        OrCountCtx cb_ctx;
        memset(&cb_ctx, 0, sizeof(cb_ctx));
        cb_ctx.tree = tree;
        cb_ctx.fs   = fs;
        cb_ctx.dl   = dl;

#define CB_BATCH 1024
        size_t processed = 0;
        while (processed < n_hashes) {
            size_t batch_n = n_hashes - processed;
            if (batch_n > CB_BATCH) batch_n = CB_BATCH;
            slotcask_bulk_resolve_and_fetch(sdb, hashes + processed,
                                             batch_n, &cb_ctx, or_count_batch_cb);
            processed += batch_n;
        }
#undef CB_BATCH

        free(hashes);
        keyset_free(ks);
        return cb_ctx.count;
    }
}

/* ========== COUNT with criteria ========== */

typedef struct {
    CriteriaNode *tree;
    FieldSchema *fs;
    int count;
    QueryDeadline *deadline;
    int dl_counter;
    /* Hot-path single-leaf pre-resolved criterion — set once before the
     * scan starts when tree is a CNODE_LEAF with a compiled typed
     * criterion.  Skips criteria_match_tree's dispatch (CNODE_AND/OR
     * recursion + indirect function-pointer chain that LTO can't
     * devirtualize through the seg-scan callback pointer cascade).
     * NULL => fall back to the general criteria_match_tree path. */
    const CompiledCriterion *single_leaf_cc;
} CountCtx;

/* Per-thread accumulator for count_scan_cb. Mirrors the idx_count_cb
   pattern: each worker increments a TLS counter on every match, and
   flushes the batched delta to the shared CountCtx with one atomic-add
   per shard (instead of per-match).  Without this, a full-scan count
   like `count exists bio` at 25M hits 25M shared atomic increments
   across 16 worker threads — the cache line ping-pongs and serialises
   the per-record critical path, taking ~4.8s warm (perf shows >50%
   in count_scan_cb itself).  With per-thread batching the atomic-add
   count drops to ≤ (num_workers × num_shard_files) = a few hundred
   per query. */
static __thread struct {
    CountCtx *bound_cc;
    int pending;
} count_local = { NULL, 0 };

static int count_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx) {
    CountCtx *cc = (CountCtx *)ctx;
    /* Defensive rebind: if a worker is reused across queries without an
       intervening flush, drain the pending count to the previously-bound
       ctx before binding to the new one.  od_seg_file_worker calls
       count_scan_cb_flush_thread() at the tail of every file, so this
       branch should never fire in practice. */
    if (count_local.bound_cc != cc) {
        if (count_local.bound_cc) {
            __atomic_add_fetch(&count_local.bound_cc->count,
                               count_local.pending, __ATOMIC_RELAXED);
        }
        count_local.bound_cc = cc;
        count_local.pending = 0;
    }
    if (query_deadline_tick(cc->deadline, &cc->dl_counter)) return 1;
    const uint8_t *raw = block + hdr->key_len;
    /* Fast path: single CNODE_LEAF tree with a compiled criterion bypasses
       criteria_match_tree() altogether — straight match_typed() call. */
    int matched;
    if (cc->single_leaf_cc) {
        matched = match_typed(raw, cc->single_leaf_cc, cc->fs);
    } else {
        matched = criteria_match_tree(raw, cc->tree, cc->fs);
    }
    if (!matched) return 0;
    count_local.pending++;
    /* Cap residency so a freak query doesn't sit on a huge unflushed
       local before the per-file flush — mirrors idx_count_cb. */
    if (count_local.pending >= 4096) {
        __atomic_add_fetch(&cc->count, count_local.pending, __ATOMIC_RELAXED);
        count_local.pending = 0;
    }
    return 0;
}

/* Drain this thread's pending count to the bound ctx and detach.
   Called by the per-file scan worker (od_seg_file_worker) after each
   seg_scan_o_direct returns so the orchestrator's read of cc->count
   after parallel_for sees every worker's contribution.  Safe to call
   when nothing is bound (no-op). */
void count_scan_cb_flush_thread(void) {
    if (count_local.bound_cc) {
        __atomic_add_fetch(&count_local.bound_cc->count,
                           count_local.pending, __ATOMIC_RELAXED);
        count_local.bound_cc = NULL;
        count_local.pending = 0;
    }
}

void cmd_explain_tree(const char *db_root, const char *object, CriteriaNode *tree,
                      const char *order_by, int fetching) {
    Schema sch = load_schema(db_root, object);
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);

    {
        char verr[256];
        if (validate_criteria_tree_fields(tree, fs.ts, verr, sizeof(verr)) < 0) {
            OUT("{\"error\":\"%s\"}\n", verr);
            return;
        }
    }
    if (tree) compile_criteria_tree(tree, fs.ts);

    int table_rows = get_live_count(db_root, object);
    FilterPlan fp = plan_filter(tree, db_root, object, &fs, sch.splits,
                                 table_rows, order_by, fetching, 0);

    const char *plan_str = "unknown";
    switch (fp.kind) {
        case FP_FULL_SCAN:       plan_str = "scan"; break;
        case FP_PRIMARY_LEAF:    plan_str = "leaf"; break;
        case FP_BITMAP_SMALLER:  plan_str = "bitmap"; break;
        case FP_INTERSECT:       plan_str = "intersect"; break;
        case FP_UNION:           plan_str = "union"; break;
    }
    const char *order_str = "none";
    switch (fp.order) {
        case FP_ORDER_NONE:              order_str = "none"; break;
        case FP_ORDER_COMPOSITE:         order_str = "composite"; break;
        case FP_ORDER_COMPOSITE_EXACT:   order_str = "composite_exact"; break;
        case FP_ORDER_SORT:              order_str = "sort"; break;
        case FP_ORDER_INDEX_WALK:        order_str = "index_walk"; break;
    }

    OUT("{\"plan\":\"%s\",\"order\":\"%s\",\"total_cheap\":%s,\"table_rows\":%d,"
        "\"source\":[", plan_str, order_str, fp.total_cheap ? "true" : "false", table_rows);

    for (int i = 0; i < fp.n_source; i++) {
        SearchCriterion *leaf = fp.source_leaves[i];
        if (!leaf) continue;
        int it = pick_index_for_leaf(db_root, object, leaf);
        const char *it_str = (it == IT_BTREE)   ? "btree"   :
                             (it == IT_BITMAP)  ? "bitmap"  :
                             (it == IT_TRIGRAM) ? "trigram" : "none";
        const TypedField *leaf_tf = resolve_idx_field(fs.ts, leaf->field);
        CardEst est = card_est_leaf(db_root, object, sch.splits, leaf,
                                    leaf_tf, selectivity_budget((size_t)table_rows));
        if (i > 0) OUT(",");
        OUT("{\"field\":\"%s\",\"op\":\"%s\",\"index\":\"%s\",\"role\":\"seed\",\"estimated_rows\":%zu}",
            leaf->field,
            (leaf->op == OP_EQUAL) ? "eq" : (leaf->op == OP_LESS) ? "lt" :
            (leaf->op == OP_GREATER) ? "gt" : (leaf->op == OP_LESS_EQ) ? "lte" :
            (leaf->op == OP_GREATER_EQ) ? "gte" : (leaf->op == OP_LIKE) ? "like" :
            (leaf->op == OP_CONTAINS) ? "contains" : (leaf->op == OP_STARTS_WITH) ? "starts" :
            (leaf->op == OP_BETWEEN) ? "between" : (leaf->op == OP_IN) ? "in" :
            (leaf->op == OP_EXISTS) ? "exists" : "other",
            it_str, est.k);
    }
    OUT("],\"postfilter\":[");

    for (int i = 0; i < fp.n_postfilter; i++) {
        SearchCriterion *leaf = fp.postfilter_leaves[i];
        if (!leaf) continue;
        int it = pick_index_for_leaf(db_root, object, leaf);
        const char *pf_it_str = (it == IT_BTREE) ? "\"btree\"" :
                                (it == IT_BITMAP) ? "\"bitmap\"" :
                                (it == IT_TRIGRAM) ? "\"trigram\"" : "null";
        if (i > 0) OUT(",");
        OUT("{\"field\":\"%s\",\"op\":\"%s\",\"index\":%s,\"role\":\"postfilter\",\"estimated_rows\":null}",
            leaf->field,
            (leaf->op == OP_EQUAL) ? "eq" : (leaf->op == OP_LESS) ? "lt" :
            (leaf->op == OP_GREATER) ? "gt" : (leaf->op == OP_LESS_EQ) ? "lte" :
            (leaf->op == OP_GREATER_EQ) ? "gte" : (leaf->op == OP_LIKE) ? "like" :
            (leaf->op == OP_CONTAINS) ? "contains" : (leaf->op == OP_STARTS_WITH) ? "starts" :
            (leaf->op == OP_BETWEEN) ? "between" : (leaf->op == OP_IN) ? "in" :
            (leaf->op == OP_EXISTS) ? "exists" : "other",
            pf_it_str);
    }
    OUT("],\"hints\":[");

    int hint_count = 0;
    for (int i = 0; i < fp.n_postfilter; i++) {
        SearchCriterion *leaf = fp.postfilter_leaves[i];
        if (!leaf) continue;
        int it = pick_index_for_leaf(db_root, object, leaf);
        if (it < 0) {
            const TypedField *tf = resolve_idx_field(fs.ts, leaf->field);
            if (tf && tf->type == FT_VARCHAR &&
                (leaf->op == OP_LIKE || leaf->op == OP_CONTAINS ||
                 leaf->op == OP_ILIKE || leaf->op == OP_ICONTAINS ||
                 leaf->op == OP_STARTS_WITH || leaf->op == OP_ISTARTS_WITH)) {
                /* trigram hint emitted below */
            } else {
                if (hint_count > 0) OUT(",");
                OUT("{\"type\":\"add_index\",\"field\":\"%s\",\"reason\":\"unindexed field in postfilter; index avoids full record scan\"}",
                    leaf->field);
                hint_count++;
            }
        }
    }

    int trigram_checked = 0;
    for (int j = 0; j < 2; j++) {
        int n = (j == 0) ? fp.n_source : fp.n_postfilter;
        SearchCriterion **leaves = (j == 0) ? fp.source_leaves : fp.postfilter_leaves;
        for (int i = 0; i < n; i++) {
            SearchCriterion *leaf = leaves[i];
            if (!leaf) continue;
            trigram_checked = 1;
            const TypedField *tf = resolve_idx_field(fs.ts, leaf->field);
            if (tf && tf->type == FT_VARCHAR &&
                (leaf->op == OP_LIKE || leaf->op == OP_CONTAINS ||
                 leaf->op == OP_ILIKE || leaf->op == OP_ICONTAINS ||
                 leaf->op == OP_STARTS_WITH || leaf->op == OP_ISTARTS_WITH ||
                 leaf->op == OP_NOT_LIKE || leaf->op == OP_NOT_CONTAINS ||
                 leaf->op == OP_INOT_LIKE || leaf->op == OP_INOT_CONTAINS ||
                 leaf->op == OP_ENDS_WITH || leaf->op == OP_IENDS_WITH)) {
                int it = pick_index_for_leaf(db_root, object, leaf);
                if (it != IT_TRIGRAM) {
                    if (hint_count > 0) OUT(",");
                    OUT("{\"type\":\"add_trigram_index\",\"field\":\"%s\",\"reason\":\"varchar text-search op on %s field; trigram index enables substring matching without full record scan\"}",
                        leaf->field, (it < 0) ? "unindexed" : "non-trigram-indexed");
                    hint_count++;
                }
            }
        }
    }

    if (!trigram_checked && tree) {
        SearchCriterion *raw_ptrs[64];
        int n_raw = collect_and_leaves(tree, raw_ptrs, 64);
        for (int i = 0; i < n_raw; i++) {
            SearchCriterion *leaf = raw_ptrs[i];
            if (!leaf) continue;
            const TypedField *tf = resolve_idx_field(fs.ts, leaf->field);
            if (tf && tf->type == FT_VARCHAR &&
                (leaf->op == OP_LIKE || leaf->op == OP_CONTAINS ||
                 leaf->op == OP_ILIKE || leaf->op == OP_ICONTAINS ||
                 leaf->op == OP_STARTS_WITH || leaf->op == OP_ISTARTS_WITH ||
                 leaf->op == OP_NOT_LIKE || leaf->op == OP_NOT_CONTAINS ||
                 leaf->op == OP_INOT_LIKE || leaf->op == OP_INOT_CONTAINS ||
                 leaf->op == OP_ENDS_WITH || leaf->op == OP_IENDS_WITH)) {
                int it = pick_index_for_leaf(db_root, object, leaf);
                if (it != IT_TRIGRAM) {
                    if (hint_count > 0) OUT(",");
                    OUT("{\"type\":\"add_trigram_index\",\"field\":\"%s\",\"reason\":\"varchar text-search op on %s field; trigram index enables substring matching without full record scan\"}",
                        leaf->field, (it < 0) ? "unindexed" : "non-trigram-indexed");
                    hint_count++;
                }
            }
        }
    }

    if (fp.order == FP_ORDER_SORT && order_by && fp.n_source > 0) {
        SearchCriterion *seed = fp.source_leaves[0];
        if (seed && strcmp(seed->field, order_by) != 0 &&
            pick_index_for_leaf(db_root, object, seed) >= 0) {
            if (hint_count > 0) OUT(",");
            OUT("{\"type\":\"composite_index\",\"field\":\"%s+%s\","
                "\"reason\":\"filter on %s + order_by %s; composite index avoids in-memory sort\"}",
                seed->field, order_by, seed->field, order_by);
            hint_count++;
        }
    }

    OUT("]}\n");
    /* tree owned by caller — not freed here */
}

static int cmd_count_with_tree(const char *db_root, const char *object,
                                CriteriaNode *tree) {
    Schema sch = load_schema(db_root, object);
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);

    {
        char verr[256];
        if (validate_criteria_tree_fields(tree, fs.ts, verr, sizeof(verr)) < 0) {
            OUT("{\"error\":\"%s\"}\n", verr);
            return 1;
        }
    }
    compile_criteria_tree(tree, fs.ts);

    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };

    {
        CriteriaNode *exists_leaf = NULL;
        if (tree->kind == CNODE_LEAF) exists_leaf = tree;
        else if (tree->kind == CNODE_AND && tree->n_children == 1 &&
                 tree->children[0]->kind == CNODE_LEAF)
            exists_leaf = tree->children[0];
        if (exists_leaf && fs.ts &&
            (exists_leaf->leaf.op == OP_EXISTS ||
             exists_leaf->leaf.op == OP_NOT_EXISTS)) {
            int fi = typed_field_index(fs.ts, exists_leaf->leaf.field);
            if (fi >= 0 && fs.ts->fields[fi].type != FT_VARCHAR) {
                int total = get_live_count(db_root, object);
                OUT("%d\n", exists_leaf->leaf.op == OP_EXISTS ? total : 0);
                return 0;
            }
            if (fi >= 0 && fs.ts->fields[fi].type == FT_VARCHAR &&
                exists_leaf->leaf.op == OP_NOT_EXISTS &&
                btree_idx_exists(db_root, object, exists_leaf->leaf.field,
                                 sch.splits)) {
                SearchCriterion pos = exists_leaf->leaf;
                pos.op = OP_EXISTS;
                const TypedField *pc_tf = &fs.ts->fields[fi];
                IdxCountCtx ic = { &pos, 0, 0, &dl, 0, pc_tf };
                btree_dispatch(db_root, object, pos.field, sch.splits,
                               &pos, pc_tf, idx_count_cb, &ic);
                if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
                else {
                    int total = get_live_count(db_root, object);
                    size_t neg = ((size_t)total > ic.count)
                                  ? (size_t)total - ic.count : 0;
                    OUT("%zu\n", neg);
                }
                return 0;
            }
        }

        if (exists_leaf && fs.ts &&
            (exists_leaf->leaf.op == OP_IN || exists_leaf->leaf.op == OP_NOT_IN)) {
            int fi = typed_field_index(fs.ts, exists_leaf->leaf.field);
            if (fi >= 0 && fs.ts->fields[fi].type == FT_BOOL) {
                int saw_t = 0, saw_f = 0;
                for (int k = 0; k < exists_leaf->leaf.in_count; k++) {
                    char c0 = exists_leaf->leaf.in_values[k][0];
                    if (c0 == 't' || c0 == 'T' || c0 == '1') saw_t = 1;
                    else                                     saw_f = 1;
                }
                if (saw_t && saw_f) {
                    int total = get_live_count(db_root, object);
                    OUT("%d\n", exists_leaf->leaf.op == OP_IN ? total : 0);
                    return 0;
                }
            }
        }
    }

    size_t N_live = (size_t)get_live_count(db_root, object);
    FilterPlan fp = plan_filter(tree, db_root, object, &fs, sch.splits,
                                N_live, NULL, 0, 0);

    if (fp.kind == FP_PRIMARY_LEAF || fp.kind == FP_BITMAP_SMALLER) {
        SearchCriterion *pc = fp.source_leaves[0];
        enum SearchOp op = pc->op;
        int check_primary = op_needs_check_primary(op);

        int is_single_leaf =
            (fp.n_postfilter == 0) &&
            ((tree->kind == CNODE_LEAF) ||
             (tree->kind == CNODE_AND && tree->n_children == 1 &&
              tree->children[0]->kind == CNODE_LEAF));

        const TypedField *pc_tf = resolve_idx_field(fs.ts, pc->field);

        if (is_single_leaf && op_is_negatable(op)) {
            SearchCriterion pos = *pc;
            pos.op = op_invert(op);
            size_t pos_count = 0;
            int pos_ok = 1;
            int pos_picked = pick_index_for_leaf(db_root, object, &pos);
            if (pos_picked == IT_TRIGRAM) {
                KeySet *tg_ks = build_keyset_from_trigram(db_root, object,
                                                           sch.splits, &pos, &dl);
                if (tg_ks) {
                    CollectedHash *entries = NULL;
                    size_t n = 0;
                    keyset_to_collected_hashes(tg_ks, sch.splits, &entries, &n);
                    CriteriaNode pos_leaf = { .kind = CNODE_LEAF, .leaf = pos,
                                              .children = NULL, .n_children = 0 };
                    pos_count = parallel_indexed_count(db_root, object, &sch,
                                                       entries, (int)n,
                                                       &pos_leaf, &fs, &dl, NULL, 0);
                    free(entries);
                    keyset_free(tg_ks);
                    pos_ok = !dl.timed_out;
                } else {
                    pos_ok = 0;
                }
            } else if (pos_picked == IT_BITMAP) {
                if (pos.op == OP_EQUAL || pos.op == OP_IN) {
                    pos_count = bm_popcount_for_crit(db_root, object, sch.splits,
                                                      &pos, pc_tf);
                } else {
                    pos_count = bm_popcount_generic_for_crit(db_root, object,
                                                              pc->field, sch.splits,
                                                              &pos, pc_tf);
                }
            } else {
                int pos_cp = op_needs_check_primary(pos.op);
                IdxCountCtx ic = { &pos, pos_cp, 0, &dl, 0, pc_tf };
                btree_dispatch(db_root, object, pc->field, sch.splits,
                               &pos, pc_tf, idx_count_cb, &ic);
                pos_count = ic.count;
                pos_ok = !dl.timed_out;
            }
            if (!pos_ok) OUT("{\"error\":\"query_timeout\"}\n");
            else {
                int total = get_live_count(db_root, object);
                size_t neg = ((size_t)total > pos_count) ? (size_t)total - pos_count : 0;
                OUT("%zu\n", neg);
            }
        } else if (is_single_leaf) {
            int picked = pick_index_for_leaf(db_root, object, pc);
            if (picked == IT_TRIGRAM) {
                KeySet *tg_ks = build_keyset_from_trigram(db_root, object,
                                                           sch.splits, pc, &dl);
                if (tg_ks) {
                    CollectedHash *entries = NULL;
                    size_t n = 0;
                    keyset_to_collected_hashes(tg_ks, sch.splits, &entries, &n);
                    size_t count = parallel_indexed_count(db_root, object, &sch,
                                                          entries, (int)n,
                                                          tree, &fs, &dl, &fp, 1);
                    if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
                    else OUT("%zu\n", count);
                    free(entries);
                    keyset_free(tg_ks);
                    return 0;
                }
                OUT("0\n");
                return 0;
            }
            if (picked == IT_BITMAP) {
                size_t total;
                if (op == OP_EQUAL || op == OP_IN) {
                    total = bm_popcount_for_crit(db_root, object,
                                                  sch.splits, pc, pc_tf);
                } else {
                    total = bm_popcount_generic_for_crit(db_root, object,
                                                          pc->field, sch.splits,
                                                          pc, pc_tf);
                }
                if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
                else OUT("%zu\n", total);
            } else {
                IdxCountCtx ic = { pc, check_primary, 0, &dl, 0, pc_tf };
                btree_dispatch(db_root, object, pc->field, sch.splits,
                               pc, pc_tf, idx_count_cb, &ic);
                if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
                else OUT("%zu\n", ic.count);
            }
        } else {
            int picked = pick_index_for_leaf(db_root, object, pc);
            if (picked == IT_TRIGRAM) {
                KeySet *tg_ks = build_keyset_from_trigram(db_root, object,
                                                           sch.splits, pc, &dl);
                if (tg_ks) {
                    CollectedHash *entries = NULL;
                    size_t n = 0;
                    keyset_to_collected_hashes(tg_ks, sch.splits, &entries, &n);
                    size_t count = parallel_indexed_count(db_root, object, &sch,
                                                          entries, (int)n,
                                                          tree, &fs, &dl, &fp, 1);
                    if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
                    else OUT("%zu\n", count);
                    free(entries);
                    keyset_free(tg_ks);
                    return 0;
                }
                OUT("0\n");
                return 0;
            }

            CollectCtx cc;
            collect_ctx_init(&cc);
            cc.splits = sch.splits;
            cc.primary_crit = pc;
            cc.check_primary = check_primary;
            cc.deadline = &dl;
            cc.tf = pc_tf;
            btree_dispatch(db_root, object, pc->field, sch.splits,
                           pc, pc_tf, collect_hash_cb, &cc);

            if (cc.budget_exceeded) {
                OUT(QUERY_BUFFER_ERR);
                collect_ctx_destroy(&cc);
                return -1;
            }
            size_t count = parallel_indexed_count(db_root, object, &sch,
                                                  cc.entries, (int)cc.count,
                                                  tree, &fs, &dl, &fp, 0);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("%zu\n", count);
            collect_ctx_destroy(&cc);
        }
    } else if (fp.kind == FP_INTERSECT) {
        if (fp.source_is_bitmap && fp.n_postfilter == 0) {
            int all_supported = 1;
            for (int i = 0; i < fp.n_source; i++) {
                if (fp.source_leaves[i]->op != OP_EQUAL &&
                    fp.source_leaves[i]->op != OP_IN) {
                    all_supported = 0; break;
                }
            }
            if (all_supported) {
                size_t total = bm_popcount_intersect(db_root, object,
                                                      sch.splits,
                                                      fp.source_leaves,
                                                      fp.n_source,
                                                      fs.ts, &dl);
                if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
                else OUT("%zu\n", total);
                return 0;
            }
        }
        int small_primary = 0;
        KeySet *result = intersect_indexed_leaves(db_root, object, sch.splits,
                                                  fp.source_leaves, fp.n_source,
                                                  &dl, &small_primary);
        if (!result) {
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("0\n");
        } else if (!small_primary && fp.n_postfilter == 0) {
            size_t n = keyset_size(result);
            keyset_free(result);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("%zu\n", n);
        } else {
            CollectedHash *batch = NULL;
            size_t batch_count = 0;
            int rc = keyset_to_collected_hashes(result, sch.splits, &batch, &batch_count);
            keyset_free(result);
            if (rc != 0 || batch_count == 0) { free(batch); OUT("0\n"); }
            else {
                size_t n = parallel_indexed_count(db_root, object, &sch, batch,
                                                  (int)batch_count, tree, &fs, &dl, &fp, 0);
                free(batch);
                if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
                else OUT("%zu\n", n);
            }
        }
    } else if (fp.kind == FP_UNION) {
        int budget_exceeded = 0;
        size_t count = keyset_count_from_or(db_root, object, &sch, tree, fp.or_node,
                                            &fs, &dl, &budget_exceeded);
        if (budget_exceeded) OUT(QUERY_BUFFER_ERR);
        else if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
        else OUT("%zu\n", count);
    } else {
        const CompiledCriterion *fast_cc = NULL;
        if (tree) {
            const CriteriaNode *leaf = NULL;
            if (tree->kind == CNODE_LEAF) leaf = tree;
            else if (tree->kind == CNODE_AND && tree->n_children == 1 &&
                     tree->children[0]->kind == CNODE_LEAF) leaf = tree->children[0];
            if (leaf && leaf->compiled) fast_cc = leaf->compiled;
        }
        SlotcaskSchemaInfo info = {
            .splits = sch.splits, .slot_size = sch.slot_size,
            .streams = sch.streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
        if (sdb) {
            int64_t match_count = 0;
            scan_shards_v2_o_direct_match(sdb, &fs, fast_cc,
                                            fast_cc ? NULL : tree,
                                            &dl, &match_count);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("%lld\n", (long long)match_count);
        } else {
            CountCtx ctx = { tree, &fs, 0, &dl, 0, fast_cc };
            scan_shards(data_dir, sch.slot_size, count_scan_cb, &ctx);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("%d\n", ctx.count);
        }
    }

    return 0;
}

int cmd_count(const char *db_root, const char *object, const char *criteria_json) {
    /* No criteria = O(1) from metadata */
    if (!criteria_json || criteria_json[0] == '\0') {
        int n = get_live_count(db_root, object);
        OUT("%d\n", n);
        return 0;
    }

    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &perr);
    if (perr) {
        OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
        free_criteria_tree(tree);
        return 1;
    }
    if (!tree) {
        int n = get_live_count(db_root, object);
        OUT("%d\n", n);
        return 0;
    }

    int r = cmd_count_with_tree(db_root, object, tree);
    free_criteria_tree(tree);
    return r;
}

int cmd_count_tree(const char *db_root, const char *object, CriteriaNode *tree) {
    if (!tree) {
        int n = get_live_count(db_root, object);
        OUT("%d\n", n);
        return 0;
    }
    return cmd_count_with_tree(db_root, object, tree);
}

/* Count the matching entries for a single indexed leaf criterion without
   fetching any records. Mirrors cmd_count's FP_PRIMARY_LEAF single-leaf
   path but takes only the leaf + schema already resolved by the caller
   (cmd_find has both by the time it needs a total count).

   Returns the count, or 0 on timeout / build failure.  Caller must check
   dl->timed_out after the call if it needs to distinguish "0 matches" from
   "timed out".

   Supports all three index dispatch paths that pick_index_for_leaf returns:
     IT_TRIGRAM  → trigram KeySet + parallel_indexed_count
     IT_BITMAP   → bm_popcount_for_crit / bm_popcount_generic_for_crit
     IT_BTREE    → btree_dispatch + idx_count_cb  */
static size_t idx_count_for_leaf(const char *db_root, const char *object,
                                 const Schema *sch, const FieldSchema *fs,
                                 SearchCriterion *leaf, QueryDeadline *dl) {
    int picked = pick_index_for_leaf(db_root, object, leaf);
    const TypedField *tf = resolve_idx_field(fs->ts, leaf->field);

    if (picked == IT_TRIGRAM) {
        KeySet *tg_ks = build_keyset_from_trigram(db_root, object,
                                                   sch->splits, leaf, dl);
        if (!tg_ks) return 0;
        CollectedHash *entries = NULL;
        size_t n = 0;
        keyset_to_collected_hashes(tg_ks, sch->splits, &entries, &n);
        /* Build a single-leaf criteria node around `leaf` so
           parallel_indexed_count verifies only that leaf (no tree).
           Must compile it (compile_one) before use: parallel_indexed_count's
           per-record verification checks node->compiled and treats NULL as
           "never matches", so an uncompiled node here silently zeroes every
           count. Heap-allocate cc: free_compiled_criteria() calls free() on
           the array pointer itself (not just inner buffers), so it must be
           paired with calloc, never a stack variable. */
        CompiledCriterion *cc = calloc(1, sizeof(CompiledCriterion));
        compile_one(cc, leaf, fs->ts);
        CriteriaNode leaf_node = { .kind = CNODE_LEAF, .leaf = *leaf,
                                   .compiled = cc,
                                   .children = NULL, .n_children = 0 };
        size_t cnt = parallel_indexed_count(db_root, object, sch,
                                            entries, (int)n,
                                            &leaf_node, (FieldSchema *)fs, dl, NULL, 1);
        free_compiled_criteria(cc, 1);
        free(entries);
        keyset_free(tg_ks);
        return cnt;
    }

    if (picked == IT_BITMAP) {
        if (leaf->op == OP_EQUAL || leaf->op == OP_IN)
            return bm_popcount_for_crit(db_root, object, sch->splits, leaf, tf);
        return bm_popcount_generic_for_crit(db_root, object,
                                             leaf->field, sch->splits, leaf, tf);
    }

    /* IT_BTREE (default) */
    int check_primary = op_needs_check_primary(leaf->op);
    IdxCountCtx ic = { leaf, check_primary, 0, dl, 0, tf };
    btree_dispatch(db_root, object, leaf->field, sch->splits,
                   leaf, tf, idx_count_cb, &ic);
    idx_count_cb_flush_thread();
    return ic.count;
}

/* find <object> <criteria_json> [offset] [limit] [fields]
   criteria_json: [{"field":"name","op":"contains","value":"ali"},{"field":"age","op":"gte","value":"18"}] */
/* ===== Ordered find: buffer matches, sort, emit slice =====
   Used when caller sets order_by on find mode. Joins are not supported in
   this path (caller rejects the combination). Full-scan based; an indexed
   ordered scan is a v2 item. */
typedef struct {
    char *key;
    size_t key_len;
    uint8_t *record;        /* malloc'd copy of [key bytes | value bytes] */
    size_t value_len;       /* length of the value portion */
    char *sort_str;         /* extracted sort field as string (may be NULL) */
    double sort_num;        /* numeric form, valid iff sort_is_num */
    int sort_is_num;
} OrderedRow;

typedef struct {
    OrderedRow *rows;
    size_t count;
    size_t cap;
    CriteriaNode *tree;
    FieldSchema *fs;
    int order_field_idx;    /* index in typed schema, -1 if field unknown/untyped */
    const char *order_field_name;
    ExcludedKeys *excluded;
    QueryDeadline *deadline;
    int dl_counter;
    int order_is_numeric;
    size_t buffer_bytes;    /* running total for QUERY_BUFFER_MB cap */
    /* Lock-free reads (line 7959 fast-skip in the per-record callback;
       line 8553 post-scan check) intentionally don't take oc->lock — _Atomic
       gives them a torn-read-free view against the writers at lines 7985, 7998
       which set the flag under oc->lock. */
    _Atomic int budget_exceeded;
    pthread_mutex_t lock;
} OrderedCollectCtx;

static int ordered_collect_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx) {
    OrderedCollectCtx *oc = (OrderedCollectCtx *)ctx;
    /* coverity[lock_evasion] coverity[missing_lock] intentional fast-skip —
       `_Atomic int` makes the read torn-read-free; if stale, the locked
       re-check at line 7984 (`if (oc->buffer_bytes + row_bytes >
       g_query_buffer_max_bytes)`) catches it. Per-callback mutex acquire
       would gate every record. */
    if (oc->budget_exceeded) return 1;  /* stop scanning once cap hit */
    if (query_deadline_tick(oc->deadline, &oc->dl_counter)) return 1;

    /* Exclusion check — render key per auto_key mode so wire-form
       excluded keys match what the user sent. */
    char keybuf[1100];
    {
        const Schema *sc_p = (oc->fs && oc->fs->auto_key != AK_NONE)
                              ? &oc->fs->auto_key_schema_snapshot : NULL;
        format_wire_key(sc_p, (const char *)block, hdr->key_len, keybuf, sizeof(keybuf));
    }
    if (is_excluded(oc->excluded, keybuf)) return 0;

    const uint8_t *raw = block + hdr->key_len;
    if (!criteria_match_tree(raw, oc->tree, oc->fs)) return 0;

    /* Extract sort key */
    char *sv = NULL;
    if (oc->fs && oc->fs->ts && oc->order_field_idx >= 0) {
        sv = typed_get_field_str(oc->fs->ts, raw, (int)hdr->value_len, oc->order_field_idx);
    } else {
        sv = decode_field((const char *)raw, hdr->value_len, oc->order_field_name, oc->fs);
    }

    size_t rec_len = (size_t)hdr->key_len + (size_t)hdr->value_len;
    size_t row_bytes = sizeof(OrderedRow) + hdr->key_len + rec_len + (sv ? strlen(sv) + 1 : 0);

    pthread_mutex_lock(&oc->lock);
    if (oc->buffer_bytes + row_bytes > g_query_buffer_max_bytes) {
        oc->budget_exceeded = 1;
        pthread_mutex_unlock(&oc->lock);
        free(sv);
        return 1;  /* stop scan */
    }
    oc->buffer_bytes += row_bytes;
    if (oc->count >= oc->cap) {
        int new_cap = oc->cap ? oc->cap * 2 : 256;
        OrderedRow *t = xrealloc_or_free(oc->rows, (size_t)new_cap * sizeof(OrderedRow));
        if (!t) {
            oc->rows = NULL;
            oc->count = 0;
            oc->cap = 0;
            oc->budget_exceeded = 1;
            pthread_mutex_unlock(&oc->lock);
            free(sv);
            return 1;
        }
        oc->rows = t;
        oc->cap = new_cap;
    }
    OrderedRow *r = &oc->rows[oc->count++];
    r->key_len = hdr->key_len;
    r->key = malloc(hdr->key_len + 1);
    memcpy(r->key, block, hdr->key_len);
    r->key[hdr->key_len] = '\0';
    r->value_len = hdr->value_len;
    r->record = malloc(rec_len);
    memcpy(r->record, block, rec_len);
    r->sort_str = sv;
    r->sort_is_num = oc->order_is_numeric;
    r->sort_num = (oc->order_is_numeric && sv) ? atof(sv) : 0.0;
    pthread_mutex_unlock(&oc->lock);
    return 0;
}

static int cmp_row_asc(const void *a, const void *b) {
    const OrderedRow *ra = (const OrderedRow *)a;
    const OrderedRow *rb = (const OrderedRow *)b;
    if (ra->sort_is_num) {
        if (ra->sort_num < rb->sort_num) return -1;
        if (ra->sort_num > rb->sort_num) return  1;
        return 0;
    }
    const char *sa = ra->sort_str ? ra->sort_str : "";
    const char *sb = rb->sort_str ? rb->sort_str : "";
    return strcmp(sa, sb);
}
static int cmp_row_desc(const void *a, const void *b) { return -cmp_row_asc(a, b); }

static int typed_field_is_numeric(uint8_t ft) {
    return ft == FT_INT || ft == FT_LONG || ft == FT_SHORT || ft == FT_DOUBLE ||
           ft == FT_NUMERIC || ft == FT_DATE || ft == FT_DATETIME ||
           ft == FT_DATETIMEMS || ft == FT_BOOL || ft == FT_BYTE;
}

/* ========== Find cursor (keyset pagination) ==========
   Client submits the cursor from the previous page's response as
       "cursor": {"<order_by>": "<value>", "key": "<primary_key>"}
   and we seek past that position in the order_by field's btree. Within a
   run of equal order_by values, we tie-break on hash16(primary_key) so
   pagination is stable even if the btree has many records sharing the
   same order_by value.

   Shape constraints (locked 2026-04-24 in cursor_design.md):
   - transparent JSON, not opaque blob
   - indexed order_by is hard-required; reject if not
   - single-field order_by only; multi-field composite indexes are for
     filter acceleration, not pagination
   - strict shape validation at parse; cursor contents are NOT validated
     against live data (stale cursors just seek to the last-known byte
     position, matching standard keyset semantics) */
typedef struct {
    int    present;
    char   value[1024];      /* textual value of the order_by field */
    size_t vlen;
    char   key[1024];        /* primary key string */
    size_t klen;
} FindCursor;

/* Return codes:
   0 with out->present=0 — "cursor" key present but empty/null (page 1 of
                           cursor pagination: use wrapper response, walk
                           from start, emit initial cursor)
   0 with out->present=1 — cursor object populated (subsequent page)
   -1                     — malformed cursor content (error message in *err)
   1                      — cursor key entirely absent (not a cursor query;
                           caller uses the regular find path) */
static int parse_cursor_object(const char *cursor_json, const char *order_by,
                               FindCursor *out, const char **err) {
    out->present = 0;
    out->vlen = 0;
    out->klen = 0;
    if (!cursor_json || !cursor_json[0]) return 1;

    /* Opt-in-to-cursor-pagination shapes: "null" or "{}" → page 1. */
    const char *p = json_skip(cursor_json);
    if (strncmp(p, "null", 4) == 0) return 0;        /* page 1, no position */
    if (*p != '{') { *err = "cursor must be a JSON object"; return -1; }

    JsonObj c;
    json_parse_object(cursor_json, strlen(cursor_json), &c);

    /* Detect empty object {}. json_obj_strdup returns NULL for both empty
       object and missing key; we distinguish by walking raw JSON. Treat
       {} as page 1 (no position). */
    int saw_any_pair = 0;
    for (const char *q = p + 1; *q; q++) {
        if (*q == '}') break;
        if (*q != ' ' && *q != '\t' && *q != '\n') { saw_any_pair = 1; break; }
    }
    if (!saw_any_pair) return 0;                     /* page 1, empty object */

    char *kv = json_obj_strdup(&c, "key");
    if (!kv || !kv[0]) { free(kv); *err = "cursor missing 'key' field"; return -1; }
    size_t klen = strlen(kv);
    if (klen >= sizeof(out->key)) { free(kv); *err = "cursor key too long"; return -1; }
    memcpy(out->key, kv, klen + 1);
    out->klen = klen;
    free(kv);

    if (!order_by || !order_by[0]) {
        *err = "cursor requires order_by";
        return -1;
    }
    char *vv = json_obj_strdup(&c, order_by);
    if (!vv) { *err = "cursor missing order_by field value"; return -1; }
    size_t vlen = strlen(vv);
    if (vlen >= sizeof(out->value)) { free(vv); *err = "cursor value too long"; return -1; }
    memcpy(out->value, vv, vlen + 1);
    out->vlen = vlen;
    free(vv);

    out->present = 1;
    return 0;
}

/* Per-callback state for the cursor-driven btree walk. */
typedef struct {
    /* Walk bounds */
    const uint8_t *cursor_value_bytes;  /* NULL on page 1 (no cursor) */
    size_t         cursor_value_len;
    uint8_t        cursor_hash16[16];   /* derived from cursor.key */
    int            has_cursor;
    int            desc;                /* 0=ASC, 1=DESC */

    /* Record fetch context */
    const char    *db_root;
    const char    *object;
    const Schema  *sch;
    FieldSchema   *fs;
    CriteriaNode  *remaining;           /* full criteria tree; order_by leaf stays in
                                           because order_by leaf is either equality
                                           (trivially satisfied by a cursor walk that
                                           already visits matching btree entries) or
                                           a range that we still want enforced. */

    /* Emission */
    const char   **proj_fields;
    int            proj_count;
    int            rows_fmt;
    int            dict_fmt;
    int            limit;
    int            printed;

    /* order_by resolution */
    const TypedField *order_tf;         /* for value-str extraction */
    int               order_field_idx;

    /* Captured last-emitted cursor (raw bytes). Heap-owned, freed by caller. */
    char          *last_value_str;
    char          *last_key_str;

    /* Offset semantics for ordered find. Decremented per entry that would
       otherwise emit; emit only fires once skip_remaining hits zero. When
       skip_remaining > 0 AND the criteria tree is empty, the callback
       returns before fetching the record at all (each btree entry is
       guaranteed to match), turning skip cost into a pure btree walk. */
    int            skip_remaining;
    int            offset_mode;          /* 1 → no `cursor` JSON field on output */

    QueryDeadline *deadline;
    int            dl_counter;

    /* Pre-filter KeySet — when non-NULL, the cursor walk skips any
       entry whose hash16 is not in this set without paying the
       per-record fetch + criteria_match cost. Built once before the
       walk from the indexed criteria leaves (any of trigram, bitmap,
       btree-eq/range, AND-intersect, OR-union — all the planner
       branches in choose_primary_source). Lets selective filters
       short-circuit: `find contains 'kubernetes' order_by time desc
       limit 25` with 0 matches over 789K records drops from 15s to
       ~5ms because the empty KeySet rejects every walk entry. Falls
       back to NULL + per-record criteria_match for broad filters
       where KeySet build would exceed the threshold or PRIMARY_NONE
       (no indexed leaves at all). */
    KeySet        *prefilter_ks;

    /* Caller's request-thread output stream. The cursor walk runs on
       pool worker threads whose own g_out is thread-local and may
       carry a stale FILE* from a previous request. The callback must
       set g_out = parent_out unconditionally at entry so OUT() writes
       reach THIS request's socket. */
    FILE          *parent_out;
} CursorFindCtx;

static int cursor_find_cb(const char *val, size_t vlen,
                          const uint8_t *hash16, void *ctx) {
#ifdef TEST_BUILD
    g_order_walk_scanned++;
#endif
    CursorFindCtx *c = (CursorFindCtx *)ctx;
    /* Pool worker threads carry a thread-local g_out across requests.
       Force this request's socket so OUT() reaches the right client —
       same bug class as stream_find_cb. */
    g_out = c->parent_out;
    if (query_deadline_tick(c->deadline, &c->dl_counter)) return -1;
    if (c->printed >= c->limit) return -1;

    /* Cursor tiebreak. Within a run of identical order_by values the btree
       physical order is hash16-based (unstable w.r.t. insertion order, but
       deterministic per-tree), so "strictly after cursor" means value >
       cursor.value OR (value == cursor.value AND hash16 > cursor.hash16).
       Mirror-flip for DESC. */
    if (c->has_cursor) {
        /* Length-aware compare: memcmp up to min length, then length tiebreak. */
        size_t mlen = vlen < c->cursor_value_len ? vlen : c->cursor_value_len;
        int vcmp = memcmp(val, c->cursor_value_bytes, mlen);
        if (vcmp == 0) {
            if (vlen < c->cursor_value_len) vcmp = -1;
            else if (vlen > c->cursor_value_len) vcmp = 1;
        }
        if (!c->desc) {
            if (vcmp < 0) return 0;                     /* shouldn't happen with range bounds */
            if (vcmp == 0) {
                int hcmp = memcmp(hash16, c->cursor_hash16, 16);
                if (hcmp <= 0) return 0;                /* at or before cursor → skip */
            }
        } else {
            if (vcmp > 0) return 0;                     /* shouldn't happen */
            if (vcmp == 0) {
                int hcmp = memcmp(hash16, c->cursor_hash16, 16);
                if (hcmp >= 0) return 0;
            }
        }
    }

    /* Pre-filter via KeySet built from indexed criteria leaves. The
       prefilter_ks check happens BEFORE the record fetch because the
       whole point is to skip the fetch when the indexed criteria
       rejects the entry — that's the win over the legacy "fetch
       every entry, run criteria_match" path. For empty KeySet the
       loop short-circuits at the first call. */
    if (c->prefilter_ks && !keyset_contains(c->prefilter_ks, hash16)) {
        return 0;
    }

    /* Skip-without-fetch: when offset_mode AND no remaining criteria, every
       btree entry is guaranteed to match, so we can decrement the skip
       counter against the bare btree iteration without paying any record
       fetch cost. Turns offset=N into ~N × 25ns, not ~N × 1µs. */
    if (c->skip_remaining > 0 && !c->remaining) {
        c->skip_remaining--;
        return 0;
    }

    /* Storage-version-agnostic fetch via the dispatch helper. */
    RecordRef rr;
    if (read_record_ref(c->db_root, c->object, c->sch, hash16, &rr) != 0) return 0;
    const uint8_t *key_start = rr.key;
    const uint8_t *raw       = rr.val;
    uint32_t       value_len = (uint32_t)rr.vlen;

    /* Remaining criteria (full tree). Order_by leaf still gets checked — it
       stays correct because the btree walk only visits entries that would
       pass a range/eq on the indexed field, and range checks in the tree
       agree with the range in the walk. */
    if (c->remaining && !criteria_match_tree(raw, c->remaining, c->fs)) {
        release_record_ref(&rr);
        return 0;
    }

    /* Skip-after-match: criteria matched but we're still inside the offset
       window. Release and decrement; the next match emits. */
    if (c->skip_remaining > 0) {
        c->skip_remaining--;
        release_record_ref(&rr);
        return 0;
    }

    /* Emit row. Supports json-default and rows_fmt. */
    char key_buf[1024];
    size_t klen = rr.klen < sizeof(key_buf) - 1 ? rr.klen : sizeof(key_buf) - 1;
    memcpy(key_buf, key_start, klen);
    key_buf[klen] = '\0';

    if (c->rows_fmt) {
        OUT("%s[\"%s\"", c->printed ? "," : "", key_buf);
        if (c->proj_count > 0) {
            for (int i = 0; i < c->proj_count; i++) {
                char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                        c->proj_fields[i], c->fs));
                OUT(",\"%s\"", pv ? pv : "");
                free(pv);
            }
        } else if (c->fs && c->fs->ts) {
            for (int i = 0; i < c->fs->ts->nfields; i++) {
                if (c->fs->ts->fields[i].removed) continue;
                char *pv = json_escape_field(typed_get_field_str(c->fs->ts, raw, (int)value_len, i));
                OUT(",\"%s\"", pv ? pv : "");
                free(pv);
            }
        }
        OUT("]");
    } else if (c->dict_fmt) {
        OUT("%s\"%s\":", c->printed ? "," : "", key_buf);
        if (c->proj_count > 0) {
            OUT("{");
            int first = 1;
            for (int i = 0; i < c->proj_count; i++) {
                char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                        c->proj_fields[i], c->fs));
                if (!pv) continue;
                OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
                first = 0;
                free(pv);
            }
            OUT("}");
        } else {
            char *dv = decode_value((const char *)raw, value_len, c->fs);
            OUT("%s", dv ? dv : "{}");
            free(dv);
        }
    } else if (c->proj_count > 0) {
        OUT("%s{\"key\":\"%s\",\"value\":{", c->printed ? "," : "", key_buf);
        int first = 1;
        for (int i = 0; i < c->proj_count; i++) {
            char *pv = json_escape_field(decode_field((const char *)raw, value_len,
                                    c->proj_fields[i], c->fs));
            if (!pv) continue;
            OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
            first = 0;
            free(pv);
        }
        OUT("}}");
    } else {
        char *dv = decode_value((const char *)raw, value_len, c->fs);
        OUT("%s{\"key\":\"%s\",\"value\":%s}",
            c->printed ? "," : "", key_buf, dv ? dv : "{}");
        free(dv);
    }

    /* Capture this row's (order_by_value, key) as the next-page cursor. Each
       emit overwrites, so after the walk the stored pair is the last row
       emitted — which is exactly what we send back. */
    free(c->last_value_str);
    free(c->last_key_str);
    /* When order_tf is non-NULL it was resolved from c->fs->ts, so both
       pointers are non-NULL here in practice — the explicit checks mirror
       the defensive pattern at the OrderedCollectCtx callback (line 8000)
       and silence Coverity's flow-insensitive FORWARD_NULL on c->fs. */
    c->last_value_str = (c->order_tf && c->fs && c->fs->ts)
        ? json_escape_field(typed_get_field_str(c->fs->ts, raw, (int)value_len, c->order_field_idx))
        : NULL;
    c->last_key_str = strndup(key_buf, klen);

    c->printed++;
    release_record_ref(&rr);
    return 0;
}

/* Threshold for the small-prefilter ordered-find shortcut.
 *
 * When the prefilter KeySet built from indexed criteria has very few
 * entries (typical case: selective single-match like
 * `eq username='alice.smith0'`), walking the order_by btree from one
 * end to the other looking for a hash16 in a 1-entry KeySet is O(N)
 * in the table size. If the matching record's order-by value lands
 * late in the desc walk, that's 25M btree entries visited for a
 * limit-10 query.
 *
 * Below the threshold we instead fetch every prefilter record directly
 * (K reads, no scan), decode the order_by field, sort in memory, and
 * emit. Costs O(K log K + K record fetches) = sub-ms at K = 1000.
 *
 * Above the threshold the in-memory fetch+sort starts to exceed the
 * btree-walk-with-limit-short-circuit cost. 1000 is a starting point
 * — bench against your real data if you tune it. */
#define SMALL_PREFILTER_THRESHOLD 1000

typedef struct {
    uint8_t hash[16];
    uint8_t sort_key[1024];   /* typed_field_to_index_key output — memcmp-sortable */
    size_t  sort_key_len;
} SmallPrefilterRow;

typedef struct {
    SmallPrefilterRow *rows;
    size_t             cap;
    size_t             i;
} SmallPrefilterCollect;

static int small_prefilter_collect_cb(const uint8_t hash[16], void *ctx) {
    SmallPrefilterCollect *c = (SmallPrefilterCollect *)ctx;
    if (c->i >= c->cap) return 1;            /* defensive */
    memcpy(c->rows[c->i].hash, hash, 16);
    c->i++;
    return 0;
}

/* ============================================================ D2 batch fetch
 *
 * Context + callback for batch fetching records in D2 (sort-in-memory) paths.
 * The callback uses bsearch on a sorted hash-to-index map to find which row
 * a fetched record corresponds to, then runs criteria_match_tree + sort key
 * extraction inline.
 *
 * The original code iterated per-record via read_record_ref; the batch
 * version fires callbacks in arbitrary order (parallel segment reads), so
 * we need the map to associate fetched records back to their rows. */

typedef struct {
    uint8_t hash[16];
    int     idx;
} D2HashIdxEntry;

static int d2_hash_idx_cmp(const void *a, const void *b) {
    return memcmp(((const D2HashIdxEntry *)a)->hash,
                  ((const D2HashIdxEntry *)b)->hash, 16);
}

static D2HashIdxEntry *d2_build_hash_map(const SmallPrefilterRow *rows, size_t n) {
    D2HashIdxEntry *map = malloc(n * sizeof(D2HashIdxEntry));
    if (!map) return NULL;
    for (size_t i = 0; i < n; i++) {
        memcpy(map[i].hash, rows[i].hash, 16);
        map[i].idx = (int)i;
    }
    qsort(map, n, sizeof(D2HashIdxEntry), d2_hash_idx_cmp);
    return map;
}

typedef struct {
    /* Input */
    SmallPrefilterRow *rows;
    size_t             n_total;
    CriteriaNode      *tree;
    FieldSchema       *fs;
    TypedSchema       *ts;
    int                order_field_idx;
    QueryDeadline     *deadline;
    int               *dl_counter;
    D2HashIdxEntry    *hash_map;        /* sorted map for bsearch */
    /* Output tracking — pass/fail per original row index.
       -1 = unprocessed, 0 = rejected, 1 = passed (sort_key stored in rows[i]). */
    int               *passed;          /* malloc'd per-row flags: 0=reject, 1=pass */
    int               *n_kept;          /* incremented on each pass */
} D2BatchCtx;

static int d2_batch_cb(const uint8_t hash16[16],
                        const void *key, size_t klen,
                        const void *value, size_t vlen,
                        void *ctx_ptr) {
    (void)key; (void)klen;
    D2BatchCtx *ctx = (D2BatchCtx *)ctx_ptr;
    if (ctx->deadline && query_deadline_tick(ctx->deadline, ctx->dl_counter)) return 1;

    D2HashIdxEntry lookup;
    memcpy(lookup.hash, hash16, 16);
    D2HashIdxEntry *found = bsearch(&lookup, ctx->hash_map, ctx->n_total,
                                     sizeof(D2HashIdxEntry), d2_hash_idx_cmp);
    if (!found) return 0;
    int i = found->idx;

    if (ctx->passed[i]) return 0;  /* already processed (hash collision edge) */

    /* Run criteria match tree on this record */
    if (ctx->tree && !criteria_match_tree((const uint8_t *)value,
                                           ctx->tree, ctx->fs))
        return 0;  /* reject — leave passed[i]=0 */

    /* Pass: extract sort key at the ORIGINAL row position.
       Compaction happens after all callbacks complete (see call site). */
    ctx->passed[i] = 1;
    __sync_fetch_and_add(ctx->n_kept, 1);
    typed_field_to_index_key(ctx->ts, (const uint8_t *)value,
                             ctx->order_field_idx,
                             ctx->rows[i].sort_key,
                             &ctx->rows[i].sort_key_len);
    return 0;
}

/* Memcmp on the index-encoded sort_key bytes — they're produced by
 * typed_field_to_index_key, which mirrors the btree's storage encoding
 * (varchar = raw bytes, int/long/short/byte = top-bit-flipped BE so
 * memcmp orders signed values correctly, date/datetime/numeric/currency
 * = BE bytes). qsort is stable enough across libcs for our use; we
 * hash16 tiebreaker matches cursor_find_cb's skip logic so page boundaries
 * inside equal-sort-key runs are handled consistently.
 *
 * Length-aware: shorter strings compare as smaller at the prefix-match
 * point, matching the btree's varchar order. */
static int small_prefilter_cmp_asc(const void *a, const void *b) {
    const SmallPrefilterRow *ra = a, *rb = b;
    size_t mlen = ra->sort_key_len < rb->sort_key_len
                  ? ra->sort_key_len : rb->sort_key_len;
    int c = memcmp(ra->sort_key, rb->sort_key, mlen);
    if (c != 0) return c;
    if (ra->sort_key_len < rb->sort_key_len) return -1;
    if (ra->sort_key_len > rb->sort_key_len) return 1;
    return memcmp(ra->hash, rb->hash, 16);
}
static int small_prefilter_cmp_desc(const void *a, const void *b) {
    return -small_prefilter_cmp_asc(a, b);
}

/* ========== C1 batched fetch worker ==========
 * Groups hashes by shard before fetching to improve cache locality and
 * enable parallel execution across shard groups. Populates
 * SmallPrefilterRow arrays for sort+emit in the C1 cursor path.
 *
 * Mirrors ShardWorkCtx / ShardCountCtx shape. */
typedef struct {
    const char    *db_root;
    const char    *object;
    const Schema  *sch;
    CollectedHash *entries;
    int            entry_count;
    CriteriaNode  *tree;
    FieldSchema   *fs;
    int            order_field_idx;
    TypedSchema   *ts;
    SmallPrefilterRow *results;   /* output array, caller frees */
    int            result_count;
    int            result_cap;
    QueryDeadline *deadline;
    int            dl_counter;
    pthread_mutex_t lock;         /* guards results[] growth + append */
} CursorFetchCtx;

/* cursor_fetch_cb — callback for batch lookup in cursor_fetch_worker.
   Runs criteria_match_tree + typed_field_to_index_key per found record
   while the segcache handle is held (pointers transient). */
static int cursor_fetch_cb(const uint8_t hash[16],
                            const void *key, size_t klen,
                            const void *value, size_t vlen,
                            void *ctx_ptr) {
    (void)key; (void)klen;
    CursorFetchCtx *ctx = (CursorFetchCtx *)ctx_ptr;
    if (query_deadline_tick(ctx->deadline, &ctx->dl_counter)) return 1;

    if (ctx->tree && !criteria_match_tree((const uint8_t *)value,
                                           ctx->tree, ctx->fs))
        return 0;

    /* Append to results array */
    pthread_mutex_lock(&ctx->lock);
    if (ctx->result_count >= ctx->result_cap) {
        int new_cap = ctx->result_cap ? ctx->result_cap * 2 : 64;
        SmallPrefilterRow *t = xrealloc_or_free(ctx->results,
            (size_t)new_cap * sizeof(SmallPrefilterRow));
        if (!t) {
            ctx->results = NULL;
            ctx->result_count = 0;
            ctx->result_cap = 0;
            pthread_mutex_unlock(&ctx->lock);
            return 1;
        }
        ctx->results = t;
        ctx->result_cap = new_cap;
    }
    SmallPrefilterRow *row = &ctx->results[ctx->result_count++];
    memcpy(row->hash, hash, 16);
    typed_field_to_index_key(ctx->ts, (const uint8_t *)value,
                             ctx->order_field_idx,
                             row->sort_key, &row->sort_key_len);
    pthread_mutex_unlock(&ctx->lock);
    return 0;
}

static void *cursor_fetch_worker(void *arg) {
    CursorFetchCtx *ctx = (CursorFetchCtx *)arg;
    if (ctx->entry_count == 0) return NULL;

    SlotcaskSchemaInfo info = {
        .splits = ctx->sch->splits,
        .slot_size = ctx->sch->slot_size,
        .streams = ctx->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(ctx->db_root, ctx->object, &info);
    if (!sdb) return NULL;

    /* Extract hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)ctx->entry_count * sizeof(*hashes));
    if (!hashes) return NULL;
    for (int i = 0; i < ctx->entry_count; i++)
        memcpy(hashes[i], ctx->entries[i].hash, 16);

    /* Batch resolve+fetch — two-phase model resolves all KF shards internally
       and parallelizes segment reads across unique segment files. The callback
       fires for each found record while the segcache handle is held. */
    pthread_mutex_init(&ctx->lock, NULL);
    slotcask_bulk_resolve_and_fetch(sdb, hashes, (size_t)ctx->entry_count,
                                     ctx, cursor_fetch_cb);
    pthread_mutex_destroy(&ctx->lock);

    free(hashes);
    return NULL;
}

/* Forward declaration — defined further down alongside the other
   filter-plan adapters; the D2 executor below uses it. */
static KeySet *build_keyset_from_plan(const FilterPlan *fp,
                                      const char *db_root,
                                      const char *object,
                                      const Schema *sch,
                                      QueryDeadline *dl);

#ifdef TEST_BUILD
/* Returns 1 if build_keyset_from_plan materializes a KeySet for `criteria`
   (count/no order_by plan), 0 if it skips (returns NULL → caller walks). */
int plan_keyset_materializes_for_test(const char *db_root, const char *object,
                                      const char *criteria_json) {
    snprintf(g_db_root, PATH_MAX, "%s", db_root);
    char eff_root[PATH_MAX], bare[256];
    const char *slash = strchr(object, '/');
    if (slash) { size_t d=(size_t)(slash-object);
        snprintf(eff_root,sizeof(eff_root),"%s/%.*s",db_root,(int)d,object);
        snprintf(bare,sizeof(bare),"%s",slash+1);
    } else { snprintf(eff_root,sizeof(eff_root),"%s",db_root);
        snprintf(bare,sizeof(bare),"%s",object); }
    const char *err = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &err);
    if (!tree) return -1;
    Schema sc = load_schema(eff_root, bare);
    FieldSchema fs; init_field_schema(&fs, eff_root, bare);
    size_t N = (size_t)get_live_count(eff_root, bare);
    FilterPlan fp = plan_filter(tree, eff_root, bare, &fs, sc.splits, N,
                                NULL /*order_by*/, 0 /*fetching*/, 0 /*limit*/);
    QueryDeadline dl = { now_ms_coarse(), 0 /*no timeout*/, 0 };
    KeySet *ks = build_keyset_from_plan(&fp, eff_root, bare, &sc, &dl);
    int materialized = (ks != NULL);
    if (ks) keyset_free(ks);
    free_criteria_tree(tree);
    return materialized;
}
#endif

/* ============================================================
   D2 executor: fetch-and-sort with streaming top-N.

   Walks every hash16 in the seed-leaf KeySet (built from the planner's
   FilterPlan), runs the full criteria tree per record to drop non-
   matches from non-indexed sibling criteria, extracts the order_by
   field via typed_field_to_index_key, and keeps only the top
   (offset+limit) candidates in a heap.

   Memory is O(offset+limit), not O(K) — heap of `M = offset+limit`
   rows replaces the previous "materialize all K, qsort, slice"
   pattern.  At HN-explorer profile-page shape (K ≈ user comments,
   limit 25), the heap is ≈25 rows total regardless of K.  When
   `limit` is 0 (unlimited), the executor falls back to full
   materialization (bounded by allocator failure).

   Per-record cost: 1 record fetch + criteria_match_tree + 1
   typed_field_to_index_key + O(log M) heap push.  No order_by btree
   walk at all — D2 fires when the seed leaf is selective enough
   that K random fetches beats walking the order_by btree (D3) until
   `limit` candidates pass the filter.  The planner's selectivity_
   budget(N) check picks D2 vs D3.

   Picked by the planner when:
     • fp.order == FP_ORDER_SORT
     • fp.kind ∈ {FP_PRIMARY_LEAF, FP_BITMAP_SMALLER, FP_INTERSECT, FP_UNION}
       (any plan kind that build_keyset_from_plan can materialize)
   Constraints checked by caller: no joins, no rows_fmt, no csv_delim.

   Returns the total number of matched records (for want_total). */

typedef struct {
    /* fetch/filter inputs */
    const char    *db_root;
    const char    *object;
    const Schema  *sch;
    FieldSchema   *fs;
    int            order_field_idx;
    CriteriaNode  *tree;
    ExcludedKeys  *excluded;

    /* heap state — bounded top-N */
    SmallPrefilterRow *heap;
    int                heap_n;
    int                heap_cap;     /* M = offset+limit; 0 = unbounded */
    int                desc;

    /* unbounded materialization (limit==0 path) */
    SmallPrefilterRow *fullbuf;
    int                full_n;
    int                full_cap;
    int                full_oom;

    /* accounting */
    size_t         n_matched;        /* total records passing the filter */
    QueryDeadline *dl;
    int            dl_counter;

    /* Mutex for parallel bulk-fetch callbacks.  Guards heap/fullbuf
       mutations run from concurrent IO pool workers. */
    pthread_mutex_t lock;
} FetchSortCtx;

/* Heap ordering: root holds the "drop candidate" — the worst entry in
   the top-M set so far.  ASC walk wants the M smallest → root = max;
   DESC wants the M largest → root = min.  `d2_topn_outranks(a,b)` answers
   "is a more root-worthy than b" with that orientation. */
static inline int d2_topn_outranks(const SmallPrefilterRow *a,
                                const SmallPrefilterRow *b, int desc) {
    int c = small_prefilter_cmp_asc(a, b);
    return desc ? (c < 0) : (c > 0);
}

static void d2_topn_sift_down(SmallPrefilterRow *heap, int n, int i, int desc) {
    for (;;) {
        int l = 2*i + 1, r = 2*i + 2, best = i;
        if (l < n && d2_topn_outranks(&heap[l], &heap[best], desc)) best = l;
        if (r < n && d2_topn_outranks(&heap[r], &heap[best], desc)) best = r;
        if (best == i) return;
        SmallPrefilterRow t = heap[i]; heap[i] = heap[best]; heap[best] = t;
        i = best;
    }
}

static void d2_topn_sift_up(SmallPrefilterRow *heap, int i, int desc) {
    while (i > 0) {
        int parent = (i - 1) / 2;
        if (d2_topn_outranks(&heap[i], &heap[parent], desc)) {
            SmallPrefilterRow t = heap[i]; heap[i] = heap[parent]; heap[parent] = t;
            i = parent;
        } else break;
    }
}

static void d2_topn_push(SmallPrefilterRow *heap, int *n, int cap,
                      const SmallPrefilterRow *cand, int desc) {
    if (*n < cap) {
        heap[*n] = *cand;
        d2_topn_sift_up(heap, *n, desc);
        (*n)++;
        return;
    }
    /* Heap full: root is the WORST in our kept set (max for ASC, min
       for DESC).  Replace root when the candidate is BETTER than it —
       i.e. when root is more root-worthy than candidate (cand "ranks
       below" root in the heap's drop-priority order).  This is the
       OPPOSITE direction from sift-up/sift-down, which maintain the
       invariant that root is the most-drop-worthy entry. */
    if (d2_topn_outranks(&heap[0], cand, desc)) {
        heap[0] = *cand;
        d2_topn_sift_down(heap, *n, 0, desc);
    }
}

/* keyset_iter callback: one fetch + filter + heap-push per candidate. */
/* slotcask_bulk_resolve_and_fetch callback: receives the record data directly
   instead of calling read_record_ref. */
static int fetch_sort_batch_cb(const uint8_t hash16[16],
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                void *ctx_ptr) {
    FetchSortCtx *c = (FetchSortCtx *)ctx_ptr;
    if (query_deadline_tick(c->dl, &c->dl_counter)) return 1;

    if (c->tree && !criteria_match_tree((const uint8_t *)value,
                                         c->tree, c->fs)) return 0;
    if (c->excluded && c->excluded->count > 0) {
        char keybuf[1024];
        size_t kl = klen < sizeof(keybuf) - 1 ? klen : sizeof(keybuf) - 1;
        memcpy(keybuf, key, kl); keybuf[kl] = '\0';
        if (is_excluded(c->excluded, keybuf)) return 0;
    }

    __sync_fetch_and_add(&c->n_matched, 1);

    SmallPrefilterRow cur;
    memcpy(cur.hash, hash16, 16);
    typed_field_to_index_key(c->fs->ts, (const uint8_t *)value,
                             c->order_field_idx,
                             cur.sort_key, &cur.sort_key_len);

    pthread_mutex_lock(&c->lock);
    if (c->heap_cap > 0) {
        d2_topn_push(c->heap, &c->heap_n, c->heap_cap, &cur, c->desc);
    } else {
        if (c->full_oom) { pthread_mutex_unlock(&c->lock); return 0; }
        if (c->full_n == c->full_cap) {
            int new_cap = c->full_cap ? c->full_cap * 2 : 64;
            SmallPrefilterRow *bigger = realloc(c->fullbuf,
                                                (size_t)new_cap * sizeof(SmallPrefilterRow));
            if (!bigger) { c->full_oom = 1; pthread_mutex_unlock(&c->lock); return 0; }
            c->fullbuf = bigger; c->full_cap = new_cap;
        }
        c->fullbuf[c->full_n++] = cur;
    }
    pthread_mutex_unlock(&c->lock);
    return 0;
}

/* D2 executor entry point.  Caller has already opened the JSON envelope
   and verified !has_joins/!rows_fmt/!csv_delim.

   `fp` is the FilterPlan from plan_filter; build_keyset_from_plan
   derives the candidate KeySet from its source leaves.  `order_by` is
   the typed field name to sort by (need NOT have its own index — D2
   sorts via the typed value extracted from each fetched record).

   Returns total matched record count for want_total accounting. */
static size_t find_via_fetch_sort(const char *db_root, const char *object,
                                  const Schema *sch, FieldSchema *fs,
                                  const FilterPlan *fp,
                                  const char *order_by, int desc,
                                  CriteriaNode *tree,
                                  ExcludedKeys *excluded,
                                  int offset, int limit,
                                  const char **proj_fields, int proj_count,
                                  int dict_fmt,
                                  QueryDeadline *dl)
{
    /* Resolve order_by → typed-field index for typed_field_to_index_key. */
    int order_field_idx = -1;
    if (fs && fs->ts) {
        for (int i = 0; i < fs->ts->nfields; i++) {
            if (strcmp(fs->ts->fields[i].name, order_by) == 0) {
                order_field_idx = i;
                break;
            }
        }
    }
    if (order_field_idx < 0) return 0;

    /* Build the candidate KeySet from the planner's source.  NULL or
       empty → nothing to emit. */
    KeySet *prefilter_ks = build_keyset_from_plan(fp, db_root, object, sch, dl);
    if (!prefilter_ks) return 0;
    if (keyset_size(prefilter_ks) == 0) { keyset_free(prefilter_ks); return 0; }

    /* For D1 composite plans where a more-selective non-seed leaf exists,
       replace the broad composite-seed KeySet with a narrow KeySet built
       from the most-selective leaf. */
    if (fp->prefilter_source_leaf &&
        fp->prefilter_card > 0 &&
        fp->prefilter_card < keyset_size(prefilter_ks)) {
        KeySet *narrow = build_keyset_from_leaf(db_root, object, sch->splits,
                                                 fp->prefilter_source_leaf, dl);
        if (narrow) {
            keyset_free(prefilter_ks);
            prefilter_ks = narrow;
        }
    }

    /* Set up the top-N heap (or fall back to unbounded materialization
       when limit==0). offset is included in the heap capacity so the
       skip is honored after sort. */
    int M = (limit > 0) ? (offset + limit) : 0;
    SmallPrefilterRow *heap = NULL;
    if (M > 0) {
        heap = calloc((size_t)M, sizeof(SmallPrefilterRow));
        if (!heap) { keyset_free(prefilter_ks); return 0; }
    }

    FetchSortCtx fc;
    memset(&fc, 0, sizeof(fc));
    fc.db_root         = db_root;
    fc.object          = object;
    fc.sch             = sch;
    fc.fs              = fs;
    fc.order_field_idx = order_field_idx;
    fc.tree            = tree;
    fc.excluded        = excluded;
    fc.heap            = heap;
    fc.heap_cap        = M;
    fc.desc            = desc;
    fc.dl              = dl;

    pthread_mutex_init(&fc.lock, NULL);

    /* Batch-fetch candidates via the IO pool.  Chunk the KeySet entries
       and issue slotcask_bulk_resolve_and_fetch per chunk — parallel
       across IO workers, amortizing cold page faults. */
    {
        SlotcaskSchemaInfo sinfo = {
            .splits = sch->splits, .slot_size = sch->slot_size,
            .streams = sch->streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);
#define FS_BATCH 1024
        uint8_t batch[FS_BATCH][16];
        int batch_n = 0;

        for (size_t b = 0; b < prefilter_ks->cap; b++) {
            uint32_t s = atomic_load_explicit(
                (_Atomic uint32_t *)&prefilter_ks->state[b],
                memory_order_acquire);
            if (s != 2) continue;

            memcpy(batch[batch_n], prefilter_ks->keys[b], 16);
            batch_n++;
            if (batch_n == FS_BATCH) {
                slotcask_bulk_resolve_and_fetch(sdb, batch, FS_BATCH,
                                                 &fc, fetch_sort_batch_cb);
                batch_n = 0;
            }
        }
        if (batch_n > 0) {
            slotcask_bulk_resolve_and_fetch(sdb, batch, (size_t)batch_n,
                                             &fc, fetch_sort_batch_cb);
        }
        keyset_free(prefilter_ks);
    }
    pthread_mutex_destroy(&fc.lock);

    /* Sort the kept rows.  For the bounded path the heap holds at most
       M ≪ K rows; for the unbounded path it's everything that matched. */
    SmallPrefilterRow *out;
    int out_n;
    if (M > 0) { out = heap;       out_n = fc.heap_n; }
    else       { out = fc.fullbuf; out_n = fc.full_n; }
    qsort(out, (size_t)out_n, sizeof(SmallPrefilterRow),
          desc ? small_prefilter_cmp_desc : small_prefilter_cmp_asc);

    /* Emit via cursor_find_cb so format handling (default/dict/proj)
       stays in one place.  We've already filtered+sorted; tell the cb
       to skip its own prefilter (NULL) and cursor (has_cursor=0). */
    CursorFindCtx cc;
    memset(&cc, 0, sizeof(cc));
    cc.db_root         = db_root;
    cc.object          = object;
    cc.sch             = sch;
    cc.fs              = (fs && (fs->ts || fs->nfields > 0)) ? fs : NULL;
    cc.remaining       = tree;
    cc.proj_fields     = proj_count > 0 ? proj_fields : NULL;
    cc.proj_count      = proj_count;
    cc.dict_fmt        = dict_fmt;
    cc.limit           = (limit > 0) ? limit : INT_MAX;
    cc.printed         = 0;
    cc.has_cursor      = 0;
    cc.desc            = desc;
    cc.skip_remaining  = offset > 0 ? offset : 0;
    cc.offset_mode     = 1;
    cc.deadline        = dl;
    cc.prefilter_ks    = NULL;
    cc.parent_out      = g_out;

    for (int i = 0; i < out_n; i++) {
        if (cursor_find_cb("", 0, out[i].hash, &cc) < 0) break;
    }

    free(heap);
    free(fc.fullbuf);
    free(cc.last_value_str);
    free(cc.last_key_str);
    return fc.n_matched;
}

/* Maximum candidate count before we fall back from filter-first to
   walk-ordered + per-record criteria_match. The threshold is set to
   100K because:

   - Below 100K: filter-first wins decisively. The KeySet build cost
     is bounded (each indexed source caps its own build by
     QUERY_BUFFER_MB) and per-walk-entry keyset_contains is O(1).
     For 0-match criteria the empty KeySet rejects every entry
     without a record fetch — turns 15s into 5ms (HN explorer
     `find contains 'kubernetes' order_by time desc limit 25`
     across 789K comments).

   - Above 100K: walk-ordered + criteria_match early-exits at limit
     because matches are dense in the order_by sweep. The KeySet
     build itself becomes expensive for very-broad criteria, and the
     KeySet would dominate per-query memory. We hand control to the
     existing walk path which already handles this case well.

   Tunable in db.env if a user finds their workload sits in the
   "100K candidates, scattered across the order_by range" regime
   where filter-first still wins (rare in practice). */
/* Ordered-find prefilter cap: above this many candidates, materializing a
   KeySet loses to walking the order_by index + post-filtering. A tunable
   global (not a #define) so tests can lower it without inserting 100k rows. */
size_t g_ordered_find_keyset_max = 100000;

/* Build a candidate KeySet from any indexed-source query plan.
   Returns NULL if no indexed plan applies (PRIMARY_NONE), or if the
   underlying builder fails (OOM, budget exceeded, trigram sub-3-char
   pattern, etc.).

   Wraps the existing builders (build_keyset_from_leaf — which itself
   dispatches by index type to trigram/bitmap/btree —
   intersect_indexed_leaves, build_or_keyset) behind a single
   plan-kind-driven entry point. Used by the cursor + ordered-walk
   paths in cmd_find to pre-filter records by hash16 before paying
   the per-record fetch + criteria_match cost. Mirrors the dispatch
   table in cmd_find's primary-source switch but isolated so the
   ordered paths can size-check the candidate set and choose
   filter-first vs walk-ordered. */
static KeySet *build_keyset_from_plan(const FilterPlan *fp,
                                      const char *db_root,
                                      const char *object,
                                      const Schema *sch,
                                      QueryDeadline *dl) {
    if (!fp) return NULL;
    switch (fp->kind) {
    case FP_PRIMARY_LEAF:
    case FP_BITMAP_SMALLER: {
        /* Cheap cardinality probe for bitmap-indexed leaves: the only
           consumers of build_keyset_from_plan are the two ordered-find
           prefilter sites, both of which discard any KeySet exceeding
           g_ordered_find_keyset_max immediately after building. For a
           ~5M-match bitmap criterion that's ~37 s of materialization
           thrown away. bm_popcount_for_crit walks the per-shard bitmaps
           popcounting only — no KeySet entries enumerated — so the
           probe costs ~ms regardless of match count. When the popcount
           exceeds the cap, skip the materialization and return NULL;
           the caller's per-record criteria_match walk with limit
           short-circuit (which is what ordered-find already does when
           prefilter is NULL) wins on broad criteria. */
        SearchCriterion *leaf = fp->n_source > 0 ? fp->source_leaves[0] : NULL;
        if (leaf && pick_index_for_leaf(db_root, object, leaf) == IT_BITMAP) {
            TypedSchema *ts = load_typed_schema(db_root, object);
            const TypedField *tf = resolve_idx_field(ts, leaf->field);
            size_t pop;
            if (leaf->op == OP_EQUAL || leaf->op == OP_IN) {
                pop = bm_popcount_for_crit(db_root, object, sch->splits,
                                           leaf, tf);
            } else {
                pop = bm_popcount_generic_for_crit(db_root, object,
                                                    leaf->field, sch->splits,
                                                    leaf, tf);
            }
            if (pop > g_ordered_find_keyset_max) return NULL;
        }
        return build_keyset_from_leaf(db_root, object, sch->splits,
                                      leaf, dl);
    }
    case FP_INTERSECT: {
        /* Broad-set guard (parity with FP_PRIMARY_LEAF above): when every
           source leaf is a bitmap eq/in, the intersection cardinality is
           computable by word-level AND popcount with NO KeySet materialized
           (~ms). If it exceeds the cap, skip building — the only consumers
           (ordered-find prefilter sites) discard oversized keysets anyway and
           fall back to walk + per-record criteria_match. Building a ~5M
           intersection just to throw it away wastes seconds (gap D). Mixed
           bitmap/btree intersects have no cheap probe, so they fall through to
           build (the reactive post-build size check still bounds them). */
        int all_bitmap_eqin = fp->n_source >= 2;
        for (int i = 0; i < fp->n_source; i++) {
            SearchCriterion *lf = fp->source_leaves[i];
            if (!lf || pick_index_for_leaf(db_root, object, lf) != IT_BITMAP ||
                (lf->op != OP_EQUAL && lf->op != OP_IN)) {
                all_bitmap_eqin = 0;
                break;
            }
        }
        if (all_bitmap_eqin) {
            TypedSchema *ts = load_typed_schema(db_root, object);
            size_t pop = bm_popcount_intersect(db_root, object, sch->splits,
                                               (SearchCriterion **)fp->source_leaves,
                                               fp->n_source, ts, dl);
            if (pop > g_ordered_find_keyset_max) return NULL;
        }
        int small_primary = 0;
        return intersect_indexed_leaves(db_root, object, sch->splits,
                                        (SearchCriterion **)fp->source_leaves,
                                        fp->n_source,
                                        dl, &small_primary);
    }
    case FP_UNION: {
        int budget_exceeded = 0;
        return build_or_keyset(db_root, object, sch->splits,
                               fp->or_node, dl,
                               &budget_exceeded, 0);
    }
    case FP_FULL_SCAN:
    default:
        return NULL;
    }
}

/* SlotcaskScanCb for bulk_delete_phase1_indexed: receives key + value from a
   slotcask_bulk_resolve_and_fetch call, re-verifies the full criteria tree,
   and appends the key to the BulkCriteriaCtx if it matches. Same locking and
   budget logic as bulk_criteria_scan_cb, different callback signature. */
static int bulk_criteria_indexed_cb(const uint8_t hash16[16],
                                     const void *key, size_t klen,
                                     const void *value, size_t vlen,
                                     void *raw_ctx) {
    (void)hash16; (void)vlen;
    BulkCriteriaCtx *bc = (BulkCriteriaCtx *)raw_ctx;
    if (bc->budget_exceeded) return 0;
    if (bc->limit > 0 && bc->count >= bc->limit) return 0;
    if (query_deadline_tick(bc->deadline, &bc->dl_counter)) return 0;

    /* Re-verify full criteria tree (index may be slightly stale) */
    if (!criteria_match_tree(value, bc->tree, bc->fs)) return 0;

    char *k = malloc(klen + 1);
    if (!k) return 0;
    memcpy(k, key, klen);
    k[klen] = '\0';

    pthread_mutex_lock(&bc->lock);
    if (bc->budget_exceeded || (bc->limit > 0 && bc->count >= bc->limit)) {
        pthread_mutex_unlock(&bc->lock);
        free(k);
        return 0;
    }
    size_t key_bytes = sizeof(char *) + klen + 1;
    if (bc->buffer_bytes + key_bytes > g_query_buffer_max_bytes) {
        bc->budget_exceeded = 1;
        pthread_mutex_unlock(&bc->lock);
        free(k);
        return 0;
    }
    bc->buffer_bytes += key_bytes;
    if (bc->count >= bc->cap) {
        int new_cap = bc->cap ? bc->cap * 2 : 64;
        char **nk = realloc(bc->keys, (size_t)new_cap * sizeof(char *));
        if (!nk) {
            bc->budget_exceeded = 1;
            pthread_mutex_unlock(&bc->lock);
            free(k);
            return 0;
        }
        bc->keys = nk;
        bc->cap  = new_cap;
    }
    bc->keys[bc->count++] = k;
    pthread_mutex_unlock(&bc->lock);
    return 0;
}

/* Index-aware Phase 1 for cmd_bulk_delete_criteria.
   Returns 1 and populates out_ctx->keys[] when the planner finds a usable
   index. Returns 0 when the caller must fall back to scan_dispatch.
   Never returns 0 with out_ctx partially populated. */
int bulk_delete_phase1_indexed(const char *db_root, const char *object,
                                       const Schema *sch, FieldSchema *fs,
                                       CriteriaNode *tree, int limit,
                                       QueryDeadline *dl,
                                       BulkCriteriaCtx *out_ctx) {
    size_t N = (size_t)get_live_count(db_root, object);
    FilterPlan fp = plan_filter(tree, db_root, object, fs,
                                sch->splits, N,
                                NULL, 0 /* fetching=0, count semantics */,
                                limit);
    if (fp.kind == FP_FULL_SCAN) return 0;

    KeySet *ks = build_keyset_from_plan(&fp, db_root, object, sch, dl);
    if (!ks) return 0;
    if (dl->timed_out) { keyset_free(ks); return 0; }

    if (keyset_size(ks) == 0) {
        /* Zero index matches → out_ctx already has count=0, correct. */
        keyset_free(ks);
        return 1;
    }

    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { keyset_free(ks); return 0; }

    /* Iterate keyset and resolve in batches of 1024 (same as ordered-find). */
#define BDI_BATCH 1024
    uint8_t batch[BDI_BATCH][16];
    int batch_n = 0;
    for (size_t b = 0; b < ks->cap; b++) {
        uint32_t s = atomic_load_explicit(
            (_Atomic uint32_t *)&ks->state[b], memory_order_acquire);
        if (s != 2) continue;
        memcpy(batch[batch_n++], ks->keys[b], 16);
        if (batch_n == BDI_BATCH) {
            slotcask_bulk_resolve_and_fetch(sdb, batch, BDI_BATCH,
                                             out_ctx, bulk_criteria_indexed_cb);
            batch_n = 0;
            if (out_ctx->budget_exceeded || dl->timed_out) break;
            if (limit > 0 && out_ctx->count >= limit) break;
        }
    }
    if (batch_n > 0 && !out_ctx->budget_exceeded && !dl->timed_out)
        slotcask_bulk_resolve_and_fetch(sdb, batch, (size_t)batch_n,
                                         out_ctx, bulk_criteria_indexed_cb);
    keyset_free(ks);
    return 1;
}
#undef BDI_BATCH

/* Phase 1d follow-up: compute the true match count for a FilterPlan when the
 * dispatch path didn't produce one for free.  Called by cmd_find's
 * want_total branches that early-return (FP_ORDER_INDEX_WALK,
 * FP_ORDER_COMPOSITE) and by the tail close fallback.  Same work the
 * client would do firing a separate count query; this consolidates it
 * into the find round-trip.
 *
 * Sets *out_null = 0 + returns the count when computed; *out_null = 1
 * + returns 0 when we can't cheaply compute (e.g. FP_FULL_SCAN, or
 * postfilter-bearing plans where the count would need a full recheck). */
/* hint_ks: when non-NULL and the plan is FP_PRIMARY_LEAF+postfilter, borrow this
   pre-built KeySet instead of re-walking the primary index.  Caller retains ownership. */
static size_t fp_compute_total(const FilterPlan *fp, CriteriaNode *tree,
                               const char *db_root, const char *object,
                               const Schema *sch, FieldSchema *fs,
                               QueryDeadline *dl, int *out_null,
                               KeySet *hint_ks) {
    (void)tree;
    *out_null = 1;
    if (!fp || dl->timed_out) return 0;
    switch (fp->kind) {
    case FP_INTERSECT: {
        int small_primary = 0;
        /* Cast away const on the source_leaves array: the function's signature
         * is SearchCriterion** (mutates nothing in practice; the const is a
         * caller-side guarantee that's just not threaded into the helper). */
        SearchCriterion **leaves = (SearchCriterion **)(uintptr_t)fp->source_leaves;
        KeySet *ks = intersect_indexed_leaves(db_root, object, sch->splits,
                                              leaves, fp->n_source,
                                              dl, &small_primary);
        if (!ks || dl->timed_out) { if (ks) keyset_free(ks); return 0; }
        size_t n = 0;
        if (!small_primary && fp->n_postfilter == 0) {
            /* Clean intersection: KeySet size IS the match count. */
            n = keyset_size(ks);
            *out_null = 0;
        } else {
            /* small_primary OR n_postfilter > 0 → KeySet is candidates,
             * not the answer. Walk it, fetch + criteria_match_tree per
             * record, count matches. Same cost as the explorer's
             * separate count query; we just consolidate it here. */
            CollectedHash *entries = NULL;
            size_t nh = 0;
            keyset_to_collected_hashes(ks, sch->splits, &entries, &nh);
            if (entries && tree) {
                n = parallel_indexed_count(db_root, object, sch,
                                           entries, (int)nh,
                                           tree, fs, dl, fp, 0);
                if (!dl->timed_out) *out_null = 0;
            } else if (entries && !tree) {
                /* No tree to verify against → every candidate is a match. */
                n = nh;
                *out_null = 0;
            }
            free(entries);
        }
        keyset_free(ks);
        return n;
    }
    case FP_PRIMARY_LEAF:
    case FP_BITMAP_SMALLER: {
        /* Composite-driven plans (Phase A ordered, Phase B exact) set
         * kind=PRIMARY_LEAF but source_leaves[0] is only the seed / first
         * composite sub-field — idx_count_for_leaf would count that leaf
         * alone and ignore the other pinned fields + siblings, overcounting.
         * Exact mode always has ≥2 pinned fields; ordered-composite overcounts
         * only when the criteria tree has siblings beyond the seed leaf. Emit
         * null total in those cases (rows are still correct; the client can
         * fire a separate count). */
        if (fp->order == FP_ORDER_COMPOSITE_EXACT) return 0;
        if (fp->order == FP_ORDER_COMPOSITE && tree && tree->kind != CNODE_LEAF)
            return 0;
        if (fp->n_source == 0) return 0;
        if (fp->n_postfilter != 0) {
            /* Count via KeySet + full-tree verify.  Prefer hint_ks (built by
               cursor path) to avoid re-walking the primary index. */
            KeySet *ks = hint_ks;
            int built = 0;
            if (!ks) {
                ks = build_keyset_from_leaf(db_root, object, sch->splits,
                                            fp->source_leaves[0], dl);
                built = 1;
            }
            if (!ks || dl->timed_out) { if (built && ks) keyset_free(ks); return 0; }
            CollectedHash *entries = NULL; size_t nh = 0;
            keyset_to_collected_hashes(ks, sch->splits, &entries, &nh);
            size_t n = 0;
            if (entries && tree) {
                int no_bm_shortcut =
                    (pick_index_for_leaf(db_root, object,
                                         fp->source_leaves[0])
                     == IT_TRIGRAM);
                n = parallel_indexed_count(db_root, object, sch,
                                           entries, (int)nh,
                                           tree, fs, dl, fp,
                                           no_bm_shortcut);
                if (!dl->timed_out) *out_null = 0;
            } else if (entries && !tree) {
                n = nh; *out_null = 0;
            }
            free(entries);
            if (built) keyset_free(ks);
            return n;
        }
        size_t n = idx_count_for_leaf(db_root, object, sch, fs,
                                      fp->source_leaves[0], dl);
        if (!dl->timed_out) *out_null = 0;
        return n;
    }
    case FP_UNION: {
        int budget_exc = 0;
        KeySet *ks = build_or_keyset(db_root, object, sch->splits,
                                     fp->or_node, dl, &budget_exc, 0);
        size_t n = 0;
        if (ks && !dl->timed_out && !budget_exc) {
            n = keyset_size(ks);
            *out_null = 0;
        }
        if (ks) keyset_free(ks);
        return n;
    }
    case FP_FULL_SCAN:
    default:
        /* Full-scan count would mean another full scan over the
         * same data — skip and emit null; caller can fire a separate
         * count if they really need it for this shape. */
        return 0;
    }
}


static int cmd_find_do(const char *db_root, const char *object,
                        CriteriaNode *tree,
                        JoinSpec *joins, int njoins,
                        int offset, int limit,
                        const char *proj_str, const char *excluded_csv,
                        const char *format, const char *delimiter,
                        const char *order_by, const char *order_dir,
                        const char *cursor_json, int want_total) {
    int rows_fmt = (format && strcmp(format, "rows") == 0);
    int dict_fmt = (format && strcmp(format, "dict") == 0);
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    int has_joins = (njoins > 0);

    /* tree is owned by caller — no free_criteria_tree here */
    Schema sch = load_schema(db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    /* Parse projection */
    const char *proj_fields[MAX_FIELDS];
    char proj_buf[MAX_LINE];
    int proj_count = 0;
    if (proj_str && proj_str[0]) {
        snprintf(proj_buf, sizeof(proj_buf), "%s", proj_str);
        char *_tok_save = NULL; char *tok = strtok_r(proj_buf, ",", &_tok_save);
        while (tok && proj_count < MAX_FIELDS) {
            proj_fields[proj_count++] = tok;
            tok = strtok_r(NULL, ",", &_tok_save);
        }
    }


    /* Apply hard limit: 0 or -1 means use server default, else cap at hard limit */
    if (limit <= 0) limit = g_global_limit;

    ExcludedKeys excluded = parse_excluded_keys(excluded_csv);

    /* Resolve joins (needs driver FieldSchema to pre-resolve local fields) */
    FieldSchema driver_fs;
    init_field_schema(&driver_fs, db_root, object);
    if (has_joins && resolve_joins(joins, njoins, db_root, object, &driver_fs) < 0) {
        free_joins(joins, njoins);
        free_excluded(&excluded);
        return -1;
    }

    {
        char verr[256];
        if (validate_criteria_tree_fields(tree, driver_fs.ts, verr, sizeof(verr)) < 0 ||
            (proj_count > 0 && validate_field_list(proj_fields, proj_count, driver_fs.ts,
                                                   "projection", verr, sizeof(verr)) < 0) ||
            (order_by && order_by[0] && validate_field(driver_fs.ts, order_by,
                                                       "order_by", verr, sizeof(verr)) < 0)) {
            OUT("{\"error\":\"%s\"}\n", verr);
            free_joins(joins, njoins);
            free_excluded(&excluded);
            return -1;
        }
    }

    compile_criteria_tree(tree, driver_fs.ts);


    /* total+csv is not supported (CSV has no envelope to wrap). */
    if (want_total && csv_delim) {
        OUT("{\"error\":\"\\\"total\\\" with format=csv is not supported\"}\n");
        free_joins(joins, njoins);
        free_excluded(&excluded);
        return -1;
    }

    /* ===== CURSOR PATH (keyset pagination) =====
       If the request carries "cursor": {...}, drive the walk off the
       order_by field's btree directly, skipping the buffer-sort path. */
    if (cursor_json && cursor_json[0]) {
        const char *cerr = NULL;
        FindCursor cur;
        int cr = parse_cursor_object(cursor_json, order_by, &cur, &cerr);
        if (cr < 0) {
            OUT("{\"error\":\"%s\"}\n", cerr ? cerr : "invalid cursor");
            free_joins(joins, njoins); free_excluded(&excluded);
            return -1;
        }
        if (!order_by || !order_by[0]) {
            OUT("{\"error\":\"cursor requires order_by\"}\n");
            free_joins(joins, njoins); free_excluded(&excluded);
            return -1;
        }
        if (has_joins) {
            OUT("{\"error\":\"cursor with join is not supported\"}\n");
            free_joins(joins, njoins); free_excluded(&excluded);
            return -1;
        }
        if (csv_delim) {
            OUT("{\"error\":\"cursor with format=csv is not supported\"}\n");
            free_joins(joins, njoins); free_excluded(&excluded);
            return -1;
        }

        /* order_by must be indexed (hard requirement for cursor). */
        if (!btree_idx_exists(db_root, object, order_by, sch.splits)) {
            OUT("{\"error\":\"cursor requires order_by field to be indexed\",\"field\":\"%s\"}\n",
                order_by);
            free_joins(joins, njoins); free_excluded(&excluded);
            return -1;
        }

        int desc = (order_dir && (strcmp(order_dir, "desc") == 0 ||
                                   strcmp(order_dir, "DESC") == 0));

        /* Resolve order_by's TypedField for encoding + value extraction. */
        const TypedField *order_tf = NULL;
        int order_field_idx = -1;
        if (driver_fs.ts) {
            for (int i = 0; i < driver_fs.ts->nfields; i++) {
                if (strcmp(driver_fs.ts->fields[i].name, order_by) == 0) {
                    order_tf = &driver_fs.ts->fields[i];
                    order_field_idx = i;
                    break;
                }
            }
        }

        /* Encode cursor value bytes for walk bounds. If cursor absent (page 1),
           walk from start (ASC) or end (DESC); else walk from cursor position,
           with tiebreak happening inside the callback. */
        uint8_t cur_value_buf[1024];
        size_t  cur_value_len = 0;
        int     has_cur_bytes = 0;
        if (cur.present) {
            if (order_tf) {
                encode_field_for_index(order_tf, cur.value, cur.vlen,
                                       cur_value_buf, &cur_value_len);
            } else {
                /* Composite/unknown — raw bytes. */
                size_t cap = sizeof(cur_value_buf);
                cur_value_len = cur.vlen < cap ? cur.vlen : cap;
                memcpy(cur_value_buf, cur.value, cur_value_len);
            }
            has_cur_bytes = 1;
        }

        /* Derive cursor hash16 from the primary key for tiebreak. */
        QueryDeadline cdl = { now_ms_coarse(), resolve_timeout_ms(), 0 };

        /* Filter-first pre-build for cursor path. Same rationale as
           the ordered-walk fast path below: a KeySet built from
           indexed criteria leaves lets the walk reject entries
           without paying the fetch + criteria_match per record. The
           threshold falls back to the legacy per-record path when
           the candidate set is too broad to materialise. */
        /* Phase 1c.2 (cleaned up in 1c.6): plan_filter drives cursor's
         * prefilter candidate-source decision directly.
         * order_by is always present in the cursor path (asserted above). */
        size_t cursor_N_live = (size_t)get_live_count(db_root, object);
        FilterPlan cursor_fp = plan_filter(tree, db_root, object, &driver_fs,
                                            sch.splits, cursor_N_live,
                                            order_by, 1 /*fetching*/, limit);
        KeySet *cursor_prefilter_ks = build_keyset_from_plan(&cursor_fp,
                                                            db_root, object,
                                                            &sch, &cdl);
        if (cursor_prefilter_ks &&
            keyset_size(cursor_prefilter_ks) > g_ordered_find_keyset_max) {
            keyset_free(cursor_prefilter_ks);
            cursor_prefilter_ks = NULL;
        }

        /* For D1 composite plans where a more-selective non-seed leaf exists,
           replace the broad composite-seed KeySet with a narrow KeySet built
           from the most-selective leaf.  This makes the C1 fetch+sort iterate
           ~the actual matches instead of the full composite partition. */
        if (cursor_prefilter_ks && cursor_fp.prefilter_source_leaf &&
            cursor_fp.prefilter_card > 0 &&
            cursor_fp.prefilter_card < keyset_size(cursor_prefilter_ks)) {
            KeySet *narrow = build_keyset_from_leaf(db_root, object, sch.splits,
                                                     cursor_fp.prefilter_source_leaf, &cdl);
            if (narrow) {
                keyset_free(cursor_prefilter_ks);
                cursor_prefilter_ks = narrow;
            }
        }

        /* Empty KeySet → no possible matches. Emit empty page +
           null cursor and return. */
        if (cursor_prefilter_ks &&
            keyset_size(cursor_prefilter_ks) == 0) {
            if (want_total)
                OUT(dict_fmt ? "{\"rows\":{},\"cursor\":null,\"total\":0}\n"
                             : "{\"rows\":[],\"cursor\":null,\"total\":0}\n");
            else
                OUT(dict_fmt ? "{\"rows\":{},\"cursor\":null}\n"
                             : "{\"rows\":[],\"cursor\":null}\n");
            keyset_free(cursor_prefilter_ks);
            free_joins(joins, njoins);
            free_excluded(&excluded);
            return 0;
        }

        /* C1 — cursor fetch+sort shortcut.  When the prefilter keyset
           is small (bounded by prefer_fetch_sort) the candidate
           records can be fetched, filtered, sorted and emitted in
           memory, beating a btree walk that must scan past non-matching
           order_index entries.  Matches the non-cursor D2 shortcut.

           prefilter_card: when set (nonzero and smaller than the KeySet
           size), it overrides the KeySet size for the decision.  This
           handles the case where the D1 composite overlay reseeded the
           plan with a broader leaf (e.g. type=job ~17k) while the true
           candidate set is narrower (e.g. after time>=T, ~6).  The
           KeySet is still built from the composite seed (correct for
           pre-filtering the walk), but the sizing decision reflects the
           tighter bound. */
        if (cursor_prefilter_ks) {
            size_t c1_ks = keyset_size(cursor_prefilter_ks);
            if (cursor_fp.prefilter_card > 0 &&
                cursor_fp.prefilter_card < c1_ks)
                c1_ks = cursor_fp.prefilter_card;
            if (prefer_fetch_sort(c1_ks, cursor_N_live, offset, limit,
                                  cursor_fp.source_is_bitmap) &&
                 order_tf && driver_fs.ts && order_field_idx >= 0) {
            size_t n_pre = keyset_size(cursor_prefilter_ks);
            struct timespec t1, t2, t3, t4;
            clock_gettime(CLOCK_MONOTONIC, &t1);
            SmallPrefilterRow *sp_rows = calloc(n_pre, sizeof(SmallPrefilterRow));
            if (sp_rows) {
                clock_gettime(CLOCK_MONOTONIC, &t2);
                int n_kept = 0;

                /* Batched fetch: group hashes by shard for cache locality,
                   parallelize across shard groups for large keysets. */
                CollectedHash *cf_entries;
                size_t cf_entry_count;
                if (keyset_to_collected_hashes(cursor_prefilter_ks, sch.splits,
                                               &cf_entries, &cf_entry_count) == 0
                    && cf_entry_count > 0) {
                    int group_starts[1024], group_sizes[1024];
                    int nshard_groups = shard_group_batch(
                        cf_entries, (int)cf_entry_count,
                        group_starts, group_sizes, 1024);

                    if (cf_entry_count < 1024 || nshard_groups <= 2) {
                        /* Sequential: per-shard-group, no thread overhead */
                        for (int g = 0; g < nshard_groups; g++) {
                            CursorFetchCtx fc;
                            memset(&fc, 0, sizeof(fc));
                            fc.db_root         = db_root;
                            fc.object          = object;
                            fc.sch             = &sch;
                            fc.entries         = &cf_entries[group_starts[g]];
                            fc.entry_count     = group_sizes[g];
                            fc.tree            = tree;
                            fc.fs              = &driver_fs;
                            fc.order_field_idx = order_field_idx;
                            fc.ts              = driver_fs.ts;
                            fc.deadline        = &cdl;
                            cursor_fetch_worker(&fc);
                            for (int r = 0; r < fc.result_count; r++) {
                                sp_rows[n_kept++] = fc.results[r];
                            }
                            free(fc.results);
                        }
                    } else {
                        /* Parallel: one worker per shard group */
                        CursorFetchCtx *workers = calloc(
                            nshard_groups, sizeof(CursorFetchCtx));
                        if (workers) {
                            for (int g = 0; g < nshard_groups; g++) {
                                workers[g].db_root         = db_root;
                                workers[g].object          = object;
                                workers[g].sch             = &sch;
                                workers[g].entries     = &cf_entries[group_starts[g]];
                                workers[g].entry_count = group_sizes[g];
                                workers[g].tree            = tree;
                                workers[g].fs              = &driver_fs;
                                workers[g].order_field_idx = order_field_idx;
                                workers[g].ts              = driver_fs.ts;
                                workers[g].deadline        = &cdl;
                            }
                            parallel_for_io(cursor_fetch_worker, workers,
                                         nshard_groups, sizeof(CursorFetchCtx));
                            for (int g = 0; g < nshard_groups; g++) {
                                for (int r = 0; r < workers[g].result_count; r++) {
                                    sp_rows[n_kept++] = workers[g].results[r];
                                }
                                free(workers[g].results);
                            }
                            free(workers);
                        } else {
                            /* OOM: fallback to sequential grouped */
                            for (int g = 0; g < nshard_groups; g++) {
                                CursorFetchCtx fc;
                                memset(&fc, 0, sizeof(fc));
                                fc.db_root         = db_root;
                                fc.object          = object;
                                fc.sch             = &sch;
                                fc.entries         = &cf_entries[group_starts[g]];
                                fc.entry_count     = group_sizes[g];
                                fc.tree            = tree;
                                fc.fs              = &driver_fs;
                                fc.order_field_idx = order_field_idx;
                                fc.ts              = driver_fs.ts;
                                fc.deadline        = &cdl;
                                cursor_fetch_worker(&fc);
                                for (int r = 0; r < fc.result_count; r++) {
                                    sp_rows[n_kept++] = fc.results[r];
                                }
                                free(fc.results);
                            }
                        }
                    }
                    free(cf_entries);
                } else {
                    /* Batch resolve+fetch via two-phase model */
                    {
                        SlotcaskSchemaInfo sinfo = {
                            .splits = sch.splits, .slot_size = sch.slot_size,
                            .streams = sch.streams,
                        };
                        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);
                        uint8_t (*hashes)[16] = malloc(n_pre * sizeof(*hashes));
                        int *passed = calloc(n_pre, sizeof(int));
                        D2HashIdxEntry *hmap = d2_build_hash_map(sp_rows, n_pre);
                        if (hashes && passed && hmap) {
                            for (size_t i = 0; i < n_pre; i++)
                                memcpy(hashes[i], sp_rows[i].hash, 16);
                            int dl_counter = 0;
                            D2BatchCtx d2_ctx = {
                                .rows = sp_rows, .n_total = n_pre,
                                .tree = tree, .fs = &driver_fs,
                                .ts = driver_fs.ts,
                                .order_field_idx = order_field_idx,
                                .deadline = &cdl, .dl_counter = &dl_counter,
                                .hash_map = hmap,
                                .passed = passed, .n_kept = &n_kept,
                            };
                            slotcask_bulk_resolve_and_fetch(sdb, hashes,
                                                             n_pre, &d2_ctx,
                                                             d2_batch_cb);
                            if (n_kept > 0) {
                                int keep = 0;
                                for (size_t i = 0; i < n_pre; i++) {
                                    if (passed[i]) {
                                        if (keep != (int)i)
                                            memcpy(sp_rows[keep].hash, sp_rows[i].hash, 16);
                                        if (keep != (int)i) {
                                            memcpy(sp_rows[keep].sort_key,
                                                   sp_rows[i].sort_key,
                                                   sp_rows[i].sort_key_len);
                                            sp_rows[keep].sort_key_len = sp_rows[i].sort_key_len;
                                        }
                                        keep++;
                                    }
                                }
                                n_kept = keep;
                            }
                        }
                        free(hashes);
                        free(passed);
                        free(hmap);
                    }
                    if (n_kept == 0) {
                        for (size_t i = 0; i < n_pre; i++) {
                            RecordRef rr;
                            if (read_record_ref(db_root, object, &sch,
                                                sp_rows[i].hash, &rr) != 0) continue;
                            if (tree && !criteria_match_tree((const uint8_t *)rr.val,
                                                              tree, &driver_fs)) {
                                release_record_ref(&rr);
                                continue;
                            }
                            if (n_kept != (int)i)
                                memcpy(sp_rows[n_kept].hash, sp_rows[i].hash, 16);
                            typed_field_to_index_key(driver_fs.ts,
                                                     (const uint8_t *)rr.val,
                                                     order_field_idx,
                                                     sp_rows[n_kept].sort_key,
                                                     &sp_rows[n_kept].sort_key_len);
                            n_kept++;
                            release_record_ref(&rr);
                        }
                    }
                }
                clock_gettime(CLOCK_MONOTONIC, &t3);
                qsort(sp_rows, (size_t)n_kept, sizeof(SmallPrefilterRow),
                      desc ? small_prefilter_cmp_desc
                           : small_prefilter_cmp_asc);
                clock_gettime(CLOCK_MONOTONIC, &t4);
                double collect_ms = (t2.tv_sec - t1.tv_sec) * 1000.0 +
                    (t2.tv_nsec - t1.tv_nsec) / 1000000.0;
                double fetch_ms = (t3.tv_sec - t2.tv_sec) * 1000.0 +
                    (t3.tv_nsec - t2.tv_nsec) / 1000000.0;
                double sort_ms = (t4.tv_sec - t3.tv_sec) * 1000.0 +
                    (t4.tv_nsec - t3.tv_nsec) / 1000000.0;
                LOG_DEBUG(LOG_SUB_QUERY, "C1_TIMING: n_pre=%zu, n_kept=%d, collect=%.1fms, fetch+filter=%.1fms, sort=%.1fms",
                         n_pre, n_kept, collect_ms, fetch_ms, sort_ms);
                CursorFindCtx cc;
                memset(&cc, 0, sizeof(cc));
                cc.cursor_value_bytes = has_cur_bytes ? cur_value_buf : NULL;
                cc.cursor_value_len   = cur_value_len;
                cc.has_cursor         = cur.present;
                if (cur.present) {
                    compute_hash_raw(cur.key, cur.klen, cc.cursor_hash16);
                }
                cc.db_root         = db_root;
                cc.object          = object;
                cc.sch             = &sch;
                cc.fs              = (driver_fs.ts || driver_fs.nfields > 0)
                                      ? &driver_fs : NULL;
                cc.desc            = desc;
                cc.remaining       = tree;
                cc.proj_fields     = proj_count > 0 ? proj_fields : NULL;
                cc.proj_count      = proj_count;
                cc.rows_fmt        = rows_fmt;
                cc.dict_fmt        = dict_fmt;
                cc.limit           = limit;
                cc.printed         = 0;
                cc.order_tf        = order_tf;
                cc.order_field_idx = order_field_idx;
                cc.skip_remaining  = offset > 0 ? offset : 0;
                cc.offset_mode     = 1;
                cc.deadline        = &cdl;
                cc.prefilter_ks    = NULL;
                cc.parent_out      = g_out;
                OUT(dict_fmt ? "{\"rows\":{" : "{\"rows\":[");
                for (int i = 0; i < n_kept; i++) {
                    if (cursor_find_cb((const char *)sp_rows[i].sort_key,
                                       sp_rows[i].sort_key_len,
                                       sp_rows[i].hash, &cc) < 0) break;
                }
                OUT(dict_fmt ? "}" : "]");
                if (cc.printed >= limit && cc.last_value_str
                    && cc.last_key_str) {
                    OUT(",\"cursor\":{\"%s\":\"%s\",\"key\":\"%s\"}",
                        order_by, cc.last_value_str, cc.last_key_str);
                } else {
                    OUT(",\"cursor\":null");
                }
                if (want_total) {
                    FilterPlan count_fp = plan_filter(tree, db_root, object,
                        &driver_fs, sch.splits, cursor_N_live,
                        NULL, 0 /*fetching*/, 0 /*limit*/);
                    int tnull = 1;
                    size_t ctotal = fp_compute_total(&count_fp, tree, db_root,
                                                     object, &sch, &driver_fs,
                                                     &cdl, &tnull,
                                                     cursor_prefilter_ks);
                    if (tnull && tree == NULL) { ctotal = cursor_N_live; tnull = 0; }
                    if (tnull) OUT(",\"total\":null");
                    else       OUT(",\"total\":%zu", ctotal);
                }
                OUT("}\n");
                free(sp_rows);
                free(cc.last_value_str);
                free(cc.last_key_str);
                keyset_free(cursor_prefilter_ks);
                free_joins(joins, njoins);
                free_excluded(&excluded);
                return 0;
            }
            /* calloc failed — fall through to btree-walk path. */
        }
        } /* if (cursor_prefilter_ks) */

        CursorFindCtx cc;
        memset(&cc, 0, sizeof(cc));
        cc.cursor_value_bytes = has_cur_bytes ? cur_value_buf : NULL;
        cc.cursor_value_len   = cur_value_len;
        cc.has_cursor         = cur.present;
        cc.desc               = desc;
        if (cur.present) {
            compute_hash_raw(cur.key, cur.klen, cc.cursor_hash16);
        }
        cc.db_root = db_root;
        cc.object  = object;
        cc.sch     = &sch;
        cc.fs      = &driver_fs;
        cc.remaining = tree;            /* full tree; filters apply per record */
        cc.proj_fields = proj_count > 0 ? proj_fields : NULL;
        cc.proj_count  = proj_count;
        cc.rows_fmt    = rows_fmt;
        cc.dict_fmt    = dict_fmt;
        cc.limit       = limit;
        cc.printed     = 0;
        cc.order_tf    = order_tf;
        cc.order_field_idx = order_field_idx;
        cc.deadline    = &cdl;
        cc.prefilter_ks = cursor_prefilter_ks;
        cc.parent_out   = g_out;

        /* Cursor response always uses the {rows:..., cursor:...} wrapper so
           clients get a single stable shape regardless of format. dict_fmt
           swaps the inner array for an object. */
        OrderWalkBounds owb;
        order_walk_bounds(tree, &driver_fs, order_by, &owb);
        OUT(dict_fmt ? "{\"rows\":{" : "{\"rows\":[");
        if (desc) {
            /* DESC: start (upper) = cursor when resuming, else window-high or
               max; stop (lower) = window-low or "". */
            const char *hi_b = has_cur_bytes ? (const char *)cur_value_buf
                             : (owb.has_hi ? (const char *)owb.hi : "\xff\xff\xff\xff");
            size_t hi_l = has_cur_bytes ? cur_value_len : (owb.has_hi ? owb.hi_len : 4);
            int    hi_e = has_cur_bytes ? 0 : (owb.has_hi ? owb.hi_excl : 0);
            const char *lo_b = owb.has_lo ? (const char *)owb.lo : "";
            size_t lo_l = owb.has_lo ? owb.lo_len : 0;
            int    lo_e = owb.has_lo ? owb.lo_excl : 0;
            btree_idx_walk_ordered(db_root, object, order_by, sch.splits,
                                   lo_b, lo_l, lo_e,
                                   hi_b, hi_l, hi_e,
                                   1, cursor_find_cb, &cc);
        } else {
            /* ASC: start (lower) = cursor when resuming, else window-low or "";
               stop (upper) = window-high or max. */
            const char *lo_b = has_cur_bytes ? (const char *)cur_value_buf
                             : (owb.has_lo ? (const char *)owb.lo : "");
            size_t lo_l = has_cur_bytes ? cur_value_len : (owb.has_lo ? owb.lo_len : 0);
            int    lo_e = has_cur_bytes ? 0 : (owb.has_lo ? owb.lo_excl : 0);
            const char *hi_b = owb.has_hi ? (const char *)owb.hi : "\xff\xff\xff\xff";
            size_t hi_l = owb.has_hi ? owb.hi_len : 4;
            int    hi_e = owb.has_hi ? owb.hi_excl : 0;
            btree_idx_walk_ordered(db_root, object, order_by, sch.splits,
                                   lo_b, lo_l, lo_e,
                                   hi_b, hi_l, hi_e,
                                   0, cursor_find_cb, &cc);
        }
        OUT(dict_fmt ? "}" : "]");

        /* Emit next-page cursor if we actually hit the limit (there might be
           more). If printed < limit the walk drained to the end → null. */
        if (cc.printed >= limit && cc.last_value_str && cc.last_key_str) {
            OUT(",\"cursor\":{\"%s\":\"%s\",\"key\":\"%s\"}",
                order_by, cc.last_value_str, cc.last_key_str);
        } else {
            OUT(",\"cursor\":null");
        }
        if (want_total) {
            FilterPlan count_fp = plan_filter(tree, db_root, object,
                &driver_fs, sch.splits, cursor_N_live,
                NULL, 0 /*fetching*/, 0 /*limit*/);
            int tnull = 1;
            size_t ctotal = fp_compute_total(&count_fp, tree, db_root,
                                             object, &sch, &driver_fs,
                                             &cdl, &tnull,
                                             cursor_prefilter_ks);
            if (tnull && tree == NULL) { ctotal = cursor_N_live; tnull = 0; }
            if (tnull) OUT(",\"total\":null");
            else       OUT(",\"total\":%zu", ctotal);
        }
        OUT("}\n");

        if (cursor_prefilter_ks) keyset_free(cursor_prefilter_ks);
        free(cc.last_value_str);
        free(cc.last_key_str);
        free_joins(joins, njoins);
        free_excluded(&excluded);
        return 0;
    }

    /* Phase 1c.2/1c.6: plan_filter is the single planner. FP_ORDER_COMPOSITE
     * and FP_ORDER_INDEX_WALK are handled by D1/D3 executors; remaining
     * ordered paths sort in-memory via the ordered-collect path.
     * order_by passed only when present and joins absent (joins use
     * rows_fmt with explicit ordering). */
    size_t find_N_live = (size_t)get_live_count(db_root, object);
    /* Pass limit=0 when want_total is set: the skip-card_est fast
     * path otherwise routes multi-leaf find through a single-seed
     * PRIMARY_LEAF, whose total accounting (idx_count_for_leaf on the
     * seed) returns the seed's match count rather than the true AND
     * intersection — wrong answer.  When want_total is set the user
     * wants accuracy over speed; FP_INTERSECT gives the exact total
     * via |KeySet|. */
    FilterPlan fp = plan_filter(tree, db_root, object, &driver_fs,
                                sch.splits, find_N_live,
                                (order_by && order_by[0] && !has_joins) ? order_by : NULL,
                                1 /*fetching=find*/,
                                want_total ? 0 : limit);

    /* Statement-timeout deadline, shared across all worker threads of this query */
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };

    /* Phase 1d.2: per-kind total tracking.
       find_total_null=1 → emit "total":null.  find_total_null=0 → emit the value.
       Populated below for the kinds that can cheaply compute a total. */
    size_t find_total = 0;
    int    find_total_null = 1;

    if (has_joins) {
        /* Joined queries are tabular by default (rows_fmt-style). format=csv
           emits an equivalent CSV table with `<as>.<field>` column names. */
        FieldSchema *fs_ptr = (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL;
        if (csv_delim) {
            emit_joined_csv_header(object, fs_ptr, joins, njoins,
                                   proj_count > 0 ? proj_fields : NULL, proj_count,
                                   csv_delim);
        } else {
            /* rows_fmt/joins always use {"rows":[...]} envelope — want_total
               just appends ,"total":null before the closing }. No open change
               needed here; the close is handled below. */
            emit_joined_columns(object, fs_ptr, joins, njoins,
                                proj_count > 0 ? proj_fields : NULL, proj_count);
        }
    } else if (csv_delim) {
        csv_emit_header(proj_count > 0 ? proj_fields : NULL, proj_count,
                        (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                        csv_delim);
    } else if (rows_fmt) {
        emit_rows_columns(proj_fields, proj_count,
                          (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL);
    } else if (dict_fmt) {
        OUT(want_total ? "{\"rows\":{" : "{");
    } else {
        OUT(want_total ? "{\"rows\":[" : "[");
    }

    int has_order = (order_by && order_by[0] && !has_joins);

    /* ===== D1: composite-prefix sorted scan (Phase 1c step 3/6) =====
       When plan_filter selected FP_ORDER_COMPOSITE the planner already
       confirmed that a "<seed>+<order_by>" composite btree exists and
       the seed criterion is selective.  Walk the composite btree range
       [encoded(seed), encoded(seed)+0xff×4) in the requested direction
       — entries arrive sorted by order_by within the prefix, so no
       in-memory sort is needed.  O(limit) cost.

       Guards: no joins (need tabular materialise for column ordering),
       no rows_fmt (table envelope needs full collect), no csv_delim
       (header row needs full schema scan).  Those edge cases fall
       through to the existing ordered-collect + in-memory sort path. */
    if (fp.order == FP_ORDER_COMPOSITE_EXACT &&
        !has_joins && !rows_fmt && !csv_delim && fp.n_source >= 1) {
        find_via_composite_key(
            db_root, object, &sch,
            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
            fp.composite_field, fp.source_leaves, fp.n_source,
            tree, &excluded, offset, limit,
            proj_fields, proj_count, dict_fmt, &dl);
        size_t ex_total = 0; int ex_null = 1;
        if (want_total) ex_total = fp_compute_total(&fp, tree, db_root, object,
                                                    &sch, &driver_fs, &dl, &ex_null, NULL);
        if (dict_fmt) {
            if (!want_total) OUT("}\n");
            else if (ex_null) OUT("},\"total\":null}\n");
            else OUT("},\"total\":%zu}\n", ex_total);
        } else {
            if (!want_total) OUT("]\n");
            else if (ex_null) OUT("],\"total\":null}\n");
            else OUT("],\"total\":%zu}\n", ex_total);
        }
        free_excluded(&excluded);
        free_joins(joins, njoins);
        return 0;
    }
    if (fp.order == FP_ORDER_COMPOSITE && fp.kind == FP_PRIMARY_LEAF &&
        !has_joins && !rows_fmt && !csv_delim && fp.n_source >= 1) {
        int desc = (order_dir && (strcmp(order_dir, "desc") == 0 ||
                                  strcmp(order_dir, "DESC") == 0));
        find_via_composite_prefix(
            db_root, object, &sch,
            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
            fp.source_leaves[0], order_by, desc,
            fp.order_range,
            tree, &excluded, offset, limit,
            proj_fields, proj_count, dict_fmt, &dl);
        /* Close the envelope opened above and return.  When want_total,
         * compute the real match count via fp_compute_total (extra walk —
         * same work the client would do firing a separate count; this
         * consolidates it into the find round-trip). */
        size_t d1_total = 0; int d1_null = 1;
        if (want_total) d1_total = fp_compute_total(&fp, tree, db_root, object,
                                                    &sch, &driver_fs, &dl, &d1_null, NULL);
        if (dict_fmt) {
            if (!want_total) OUT("}\n");
            else if (d1_null) OUT("},\"total\":null}\n");
            else OUT("},\"total\":%zu}\n", d1_total);
        } else {
            if (!want_total) OUT("]\n");
            else if (d1_null) OUT("],\"total\":null}\n");
            else OUT("],\"total\":%zu}\n", d1_total);
        }
        free_excluded(&excluded);
        free_joins(joins, njoins);
        return 0;
    } else if (fp.order == FP_ORDER_INDEX_WALK &&
               !has_joins && !rows_fmt && !csv_delim) {
        /* ===== D3: order-index walk + per-record post-filter (Phase 1c step 4/6) =====
           Walk the order_by btree full range in the requested direction;
           post-filter the full criteria tree per fetched record; stop at
           limit.  O(limit + records-walked-before-limit-fills) — dramatically
           cheaper than scan-and-sort-millions for the feed/profile shape
           (broad filter + indexed order_by, no composite).

           Guards: same as D1 — no joins (tabular materialise), no rows_fmt
           (table envelope needs full collect), no csv_delim (header row needs
           full schema scan).  Those cases fall through to the existing
           ordered-collect + in-memory sort path. */
        int desc = (order_dir && (strcmp(order_dir, "desc") == 0 ||
                                  strcmp(order_dir, "DESC") == 0));
        find_via_order_index_walk(
            db_root, object, &sch,
            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
            order_by, desc,
            tree, &excluded, offset, limit,
            proj_fields, proj_count, dict_fmt, &dl);
        /* Close the envelope opened above and return.  When want_total,
         * compute the real match count via fp_compute_total. */
        size_t d3_total = 0; int d3_null = 1;
        if (want_total) d3_total = fp_compute_total(&fp, tree, db_root, object,
                                                    &sch, &driver_fs, &dl, &d3_null, NULL);
        if (dict_fmt) {
            if (!want_total) OUT("}\n");
            else if (d3_null) OUT("},\"total\":null}\n");
            else OUT("},\"total\":%zu}\n", d3_total);
        } else {
            if (!want_total) OUT("]\n");
            else if (d3_null) OUT("],\"total\":null}\n");
            else OUT("],\"total\":%zu}\n", d3_total);
        }
        free_excluded(&excluded);
        free_joins(joins, njoins);
        return 0;
    } else if (fp.order == FP_ORDER_SORT &&
               !has_joins && !rows_fmt && !csv_delim &&
               fp.n_source > 0) {
        /* ===== D2: fetch-and-sort with streaming top-N =====
           When plan_filter selected FP_ORDER_SORT the seed leaf's
           candidate set is bounded (estimable + not saturated).  Stream
           the candidate KeySet, fetch each record, post-filter via the
           full criteria tree, extract the order_by field via the typed
           index-key encoder, and keep only the top (offset+limit) in a
           heap.  Final qsort + emit via cursor_find_cb.

           No order_by btree is required — D2 sorts via the typed value
           extracted from each fetched record, so it works for any
           order_by field in the schema.  This replaces the
           SmallPrefilterRow shortcut that used to live inside
           idx_find_parallel as a nested branch.

           Memory is O(offset+limit), not O(K) — heap of (offset+limit)
           rows.  Per-record cost: 1 fetch + criteria_match + 1
           index-key extract + O(log(offset+limit)) heap push.

           Guards: same as D1/D3 — no joins (tabular materialise), no
           rows_fmt (table envelope), no csv_delim (header row). Those
           cases fall through to idx_find_parallel's legacy ordered-
           collect + sort path. */
        int desc = (order_dir && (strcmp(order_dir, "desc") == 0 ||
                                  strcmp(order_dir, "DESC") == 0));
        size_t d2_matched = find_via_fetch_sort(
            db_root, object, &sch,
            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
            &fp, order_by, desc,
            tree, &excluded, offset, limit,
            proj_fields, proj_count, dict_fmt, &dl);
        if (dict_fmt) {
            if (!want_total) OUT("}\n");
            else OUT("},\"total\":%zu}\n", d2_matched);
        } else {
            if (!want_total) OUT("]\n");
            else OUT("],\"total\":%zu}\n", d2_matched);
        }
        free_excluded(&excluded);
        free_joins(joins, njoins);
        return 0;
    }

    /* ===== ORDERED FIND — indexed-walk fast path =====
       When order_by is indexed and there are no excluded keys, walk the
       btree in sort order, skip the first `offset` matching entries,
       emit the next `limit`. Bypasses scan_shards + qsort entirely.

       For empty criteria the skip phase is a pure btree walk (no record
       fetches), so offset=50000 limit=100 drops from ~580ms (collect 1M
       rows + qsort + slice) to ~10-15ms (btree walk + 100 fetches).

       For non-empty criteria the skip phase still fetches each candidate
       to evaluate the tree, so the win shrinks with selectivity — but we
       still skip the qsort + the buffering, which is usually a strict
       improvement. Falls through to the buffered path for unsupported
       formats / excluded keys / non-indexed order_by.

       FILTER-FIRST (2026.05.7.x+): when criteria has indexed leaves
       (PRIMARY_LEAF / PRIMARY_INTERSECT / PRIMARY_KEYSET), build a
       candidate KeySet once via build_keyset_from_plan and pass it
       into the walk callback. cursor_find_cb skips entries whose
       hash16 isn't in the KeySet WITHOUT a record fetch — the per-
       walk-entry cost drops from ~10µs (fetch + decode + match) to
       ~100ns (hash check). For 0-match criteria the empty KeySet
       short-circuits the walk entirely. Falls back to no-KeySet
       walk (per-record criteria_match) when criteria is too broad
       (KeySet build would exceed g_ordered_find_keyset_max) or
       PRIMARY_NONE (no indexed leaves). */
    if (has_order && btree_idx_exists(db_root, object, order_by, sch.splits) &&
        excluded.count == 0) {
        const TypedField *order_tf = NULL;
        if (driver_fs.ts) {
            for (int i = 0; i < driver_fs.ts->nfields; i++) {
                if (strcmp(driver_fs.ts->fields[i].name, order_by) == 0) {
                    order_tf = &driver_fs.ts->fields[i];
                    break;
                }
            }
        }

        int desc = (order_dir && (strcmp(order_dir, "desc") == 0 ||
                                  strcmp(order_dir, "DESC") == 0));

        if (!csv_delim) {
            /* Build the pre-filter KeySet from the planner-chosen
               source. NULL when PRIMARY_NONE or builder failed —
               that's fine, the walk falls back to per-record
               criteria_match. When size exceeds threshold, free and
               use the legacy path (broad filters early-exit at
               limit). When size is zero, short-circuit the walk
               entirely. Built inside the !csv_delim branch so the
               CSV fallback path doesn't waste a build. */
            KeySet *prefilter_ks = build_keyset_from_plan(&fp, db_root,
                                                         object, &sch, &dl);
            if (prefilter_ks &&
                keyset_size(prefilter_ks) > g_ordered_find_keyset_max) {
                keyset_free(prefilter_ks);
                prefilter_ks = NULL;
            }

            /* For D1 composite plans where a more-selective non-seed
               leaf exists, replace the broad composite-seed KeySet with
               a narrow KeySet built from the most-selective leaf. */
            if (prefilter_ks && fp.prefilter_source_leaf &&
                fp.prefilter_card > 0 &&
                fp.prefilter_card < keyset_size(prefilter_ks)) {
                KeySet *narrow = build_keyset_from_leaf(db_root, object,
                                                         sch.splits,
                                                         fp.prefilter_source_leaf, &dl);
                if (narrow) {
                    keyset_free(prefilter_ks);
                    prefilter_ks = narrow;
                }
            }

            /* Empty KeySet → no candidate could possibly match.
               The opening envelope is ALREADY emitted upstream (line
               14045/14047: `{` for dict, `[` for default, the rows
               header for rows_fmt). We just need to CLOSE that
               envelope and return — don't emit a fresh `[]` or `{}`
               here or you double-open and produce `[[]` / `{{}`.
               Turns 14s "scan all, find nothing" into ~constant time
               on selective filters with zero hits. */
            if (prefilter_ks && keyset_size(prefilter_ks) == 0) {
                if (dict_fmt)
                    OUT(want_total ? "},\"total\":null}\n" : "}\n");
                else if (rows_fmt)
                    OUT(want_total ? "],\"total\":null}\n" : "]\n");
                else
                    OUT(want_total ? "],\"total\":null}\n" : "]\n");
                keyset_free(prefilter_ks);
                free_joins(joins, njoins);
                free_excluded(&excluded);
                return 0;
            }

            /* Small-prefilter shortcut. When the indexed criteria
               narrowed down to ≤ SMALL_PREFILTER_THRESHOLD matches,
               walking the order_by btree end-to-end to find them is
               O(N); fetching the K records directly + in-memory sort
               is O(K log K). At K=1 with the matching record's
               order-by value at the tail of the desc walk, this
               drops queries like
                   eq username='alice.smith0' order_by age desc limit 10
               from ~1700 ms (walks 25M btree entries) to single
               digit ms. Filed as backlog-small-prefilter-orderby-
               shortcut after bench-cache-pollution surfaced the
               pathological case. */
            int order_field_idx = -1;
            if (driver_fs.ts) {
                for (int i = 0; i < driver_fs.ts->nfields; i++) {
                    if (strcmp(driver_fs.ts->fields[i].name, order_by) == 0) {
                        order_field_idx = i;
                        break;
                    }
                }
            }
            if (prefilter_ks) {
                size_t pfs_n = keyset_size(prefilter_ks);
                if (fp.prefilter_card > 0 && fp.prefilter_card < pfs_n)
                    pfs_n = fp.prefilter_card;
                if (prefer_fetch_sort(pfs_n, find_N_live, offset, limit,
                                     fp.source_is_bitmap) &&
                    order_tf && driver_fs.ts && order_field_idx >= 0) {
                size_t n_pre = keyset_size(prefilter_ks);
                SmallPrefilterRow *rows = calloc(n_pre, sizeof(SmallPrefilterRow));
                if (rows) {
                    SmallPrefilterCollect ca = { rows, n_pre, 0 };
                    keyset_iter(prefilter_ks, small_prefilter_collect_cb, &ca);

                    /* Fetch each candidate, run the full criteria tree
                       (kills any post-filter from non-indexed siblings),
                       extract the order-key bytes via the same encoder
                       the btree uses so memcmp orders them correctly. */
                    int n_kept = 0;
                    {   /* Batch resolve+fetch via two-phase model */
                        SlotcaskSchemaInfo sinfo = {
                            .splits = sch.splits, .slot_size = sch.slot_size,
                            .streams = sch.streams,
                        };
                        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);
                        uint8_t (*hashes)[16] = malloc(n_pre * sizeof(*hashes));
                        int *passed = calloc(n_pre, sizeof(int));
                        D2HashIdxEntry *hmap = d2_build_hash_map(rows, n_pre);
                        if (hashes && passed && hmap) {
                            for (size_t i = 0; i < n_pre; i++)
                                memcpy(hashes[i], rows[i].hash, 16);
                            int dl_counter = 0;
                            D2BatchCtx d2_ctx = {
                                .rows = rows, .n_total = n_pre,
                                .tree = tree, .fs = &driver_fs,
                                .ts = driver_fs.ts,
                                .order_field_idx = order_field_idx,
                                .deadline = &dl, .dl_counter = &dl_counter,
                                .hash_map = hmap,
                                .passed = passed, .n_kept = &n_kept,
                            };
                            slotcask_bulk_resolve_and_fetch(sdb, hashes,
                                                             n_pre, &d2_ctx,
                                                             d2_batch_cb);
                        }
                        if (n_kept > 0 && hashes && passed && hmap) {
                            int keep = 0;
                            for (size_t i = 0; i < n_pre; i++) {
                                if (passed[i]) {
                                    if (keep != (int)i)
                                        memcpy(rows[keep].hash, rows[i].hash, 16);
                                    if (keep != (int)i) {
                                        memcpy(rows[keep].sort_key, rows[i].sort_key,
                                               rows[i].sort_key_len);
                                        rows[keep].sort_key_len = rows[i].sort_key_len;
                                    }
                                    keep++;
                                }
                            }
                            n_kept = keep;
                        }
                        free(hashes);
                        free(passed);
                        free(hmap);
                    }
                    if (n_kept == 0) {
                        for (size_t i = 0; i < n_pre; i++) {
                            RecordRef rr;
                            if (read_record_ref(db_root, object, &sch,
                                                 rows[i].hash, &rr) != 0) continue;
                            if (tree && !criteria_match_tree((const uint8_t *)rr.val,
                                                               tree, &driver_fs)) {
                                release_record_ref(&rr);
                                continue;
                            }
                            if (n_kept != (int)i)
                                memcpy(rows[n_kept].hash, rows[i].hash, 16);
                            typed_field_to_index_key(driver_fs.ts,
                                                     (const uint8_t *)rr.val,
                                                     order_field_idx,
                                                     rows[n_kept].sort_key,
                                                     &rows[n_kept].sort_key_len);
                            n_kept++;
                            release_record_ref(&rr);
                        }
                    }

                    qsort(rows, (size_t)n_kept, sizeof(SmallPrefilterRow),
                          desc ? small_prefilter_cmp_desc
                               : small_prefilter_cmp_asc);

                    /* Emit via cursor_find_cb so format handling (rows/
                       dict/default) stays in one place. We've already
                       filtered+sorted; tell the cb to skip its own
                       prefilter (NULL) and cursor (has_cursor=0). The
                       remaining-criteria re-check inside the cb is a
                       cheap double-check on already-validated records;
                       leave it on for safety. */
                    CursorFindCtx cc;
                    memset(&cc, 0, sizeof(cc));
                    cc.db_root         = db_root;
                    cc.object          = object;
                    cc.sch             = &sch;
                    cc.fs              = (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL;
                    cc.remaining       = tree;
                    cc.proj_fields     = proj_count > 0 ? proj_fields : NULL;
                    cc.proj_count      = proj_count;
                    cc.rows_fmt        = rows_fmt;
                    cc.dict_fmt        = dict_fmt;
                    cc.limit           = limit;
                    cc.printed         = 0;
                    cc.order_tf        = order_tf;
                    cc.order_field_idx = order_field_idx;
                    cc.has_cursor      = 0;
                    cc.desc            = desc;
                    cc.skip_remaining  = offset > 0 ? offset : 0;
                    cc.offset_mode     = 1;
                    cc.deadline        = &dl;
                    cc.prefilter_ks    = NULL;   /* already filtered */
                    cc.parent_out      = g_out;

                    for (int i = 0; i < n_kept; i++) {
                        if (cursor_find_cb("", 0, rows[i].hash, &cc) < 0) break;
                    }

                    free(rows);
                    free(cc.last_value_str);
                    free(cc.last_key_str);
                    keyset_free(prefilter_ks);

                    /* Phase 1d.2: small-prefilter sorted all candidates
                       before emitting, so n_kept is the exact total.
                       Use it instead of null when want_total is set. */
                    if (dict_fmt) {
                        if (want_total) OUT("},\"total\":%d}\n", n_kept);
                        else            OUT("}\n");
                    } else if (rows_fmt) {
                        if (want_total) OUT("],\"total\":%d}\n", n_kept);
                        else            OUT("]\n");
                    } else {
                        if (want_total) OUT("],\"total\":%d}\n", n_kept);
                        else            OUT("]\n");
                    }
                    free_joins(joins, njoins);
                    free_excluded(&excluded);
                    return 0;
                }
                /* calloc failed — fall through to the btree-walk path. */
            }
            } /* if (prefilter_ks) */

            CursorFindCtx cc;
            memset(&cc, 0, sizeof(cc));
            cc.db_root         = db_root;
            cc.object          = object;
            cc.sch             = &sch;
            cc.fs              = (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL;
            cc.remaining       = tree;
            cc.proj_fields     = proj_count > 0 ? proj_fields : NULL;
            cc.proj_count      = proj_count;
            cc.rows_fmt        = rows_fmt;
            cc.dict_fmt        = dict_fmt;
            cc.limit           = limit;
            cc.printed         = 0;
            cc.order_tf        = order_tf;
            cc.has_cursor      = 0;
            cc.desc            = desc;
            cc.skip_remaining  = offset > 0 ? offset : 0;
            cc.offset_mode     = 1;
            cc.deadline        = &dl;
            cc.prefilter_ks    = prefilter_ks;
            cc.parent_out      = g_out;

            {
                OrderWalkBounds owb;
                order_walk_bounds(tree, &driver_fs, order_by, &owb);
                const char *lo_b = owb.has_lo ? (const char *)owb.lo : "";
                size_t lo_l = owb.has_lo ? owb.lo_len : 0;
                int    lo_e = owb.has_lo ? owb.lo_excl : 0;
                const char *hi_b = owb.has_hi ? (const char *)owb.hi : "\xff\xff\xff\xff";
                size_t hi_l = owb.has_hi ? owb.hi_len : 4;
                int    hi_e = owb.has_hi ? owb.hi_excl : 0;
                btree_idx_walk_ordered(db_root, object, order_by, sch.splits,
                                       lo_b, lo_l, lo_e,
                                       hi_b, hi_l, hi_e,
                                       desc, cursor_find_cb, &cc);
            }
            if (dict_fmt)
                OUT(want_total ? "},\"total\":null}\n" : "}\n");
            else if (rows_fmt)
                OUT(want_total ? "],\"total\":null}\n" : "]\n");
            else
                OUT(want_total ? "],\"total\":null}\n" : "]\n");

            if (prefilter_ks) keyset_free(prefilter_ks);
            free(cc.last_value_str);
            free(cc.last_key_str);
            free_joins(joins, njoins);
            free_excluded(&excluded);
            return 0;
        }
    }

    if (has_order) {
        /* ===== ORDERED FIND — buffered fallback =====
           Reached when order_by isn't indexed, excluded keys are present,
           or format=csv (CSV emit isn't wired into cursor_find_cb yet).
           Buffers all matches, qsort by order_by, slices [offset, offset+limit]. */

        int order_idx = -1;
        int order_is_num = 0;
        if (driver_fs.ts) {
            for (int i = 0; i < driver_fs.ts->nfields; i++) {
                if (strcmp(driver_fs.ts->fields[i].name, order_by) == 0) {
                    order_idx = i;
                    order_is_num = typed_field_is_numeric(driver_fs.ts->fields[i].type);
                    break;
                }
            }
        }

        OrderedCollectCtx oc;
        memset(&oc, 0, sizeof(oc));
        oc.tree = tree;
        oc.fs = (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL;
        oc.order_field_idx = order_idx;
        oc.order_field_name = order_by;
        oc.excluded = &excluded;
        oc.deadline = &dl;
        oc.order_is_numeric = order_is_num;
        pthread_mutex_init(&oc.lock, NULL);

        scan_dispatch(db_root, object, &sch, data_dir, ordered_collect_cb, &oc);

        if (oc.budget_exceeded) {
            for (size_t i = 0; i < oc.count; i++) {
                free(oc.rows[i].key); free(oc.rows[i].record); free(oc.rows[i].sort_str);
            }
            free(oc.rows);
            pthread_mutex_destroy(&oc.lock);
            OUT(QUERY_BUFFER_ERR);
            free_excluded(&excluded);
            free_joins(joins, njoins);
            return -1;
        }

        int desc = (order_dir && (strcmp(order_dir, "desc") == 0 || strcmp(order_dir, "DESC") == 0));
        if (oc.count > 1)
            qsort(oc.rows, oc.count, sizeof(OrderedRow), desc ? cmp_row_desc : cmp_row_asc);

        size_t start = offset > 0 ? (size_t)offset : 0;
        size_t end = (limit > 0) ? start + (size_t)limit : oc.count;
        if (end > oc.count) end = oc.count;
        int printed = 0;
        for (size_t i = start; i < end; i++) {
            OrderedRow *r = &oc.rows[i];
            const uint8_t *val = r->record + r->key_len;
            if (csv_delim) {
                csv_emit_row(r->key, val, r->value_len,
                             proj_count > 0 ? proj_fields : NULL,
                             proj_count, &driver_fs, csv_delim);
                printed++;
                continue;
            }
            if (rows_fmt) {
                OUT("%s[\"%s\"", printed ? "," : "", r->key);
                if (proj_count > 0) {
                    for (int j = 0; j < proj_count; j++) {
                        char *pv = json_escape_field(decode_field((const char *)val, r->value_len, proj_fields[j], &driver_fs));
                        OUT(",\"%s\"", pv ? pv : "");
                        free(pv);
                    }
                } else if (driver_fs.ts) {
                    for (int j = 0; j < driver_fs.ts->nfields; j++) {
                        if (driver_fs.ts->fields[j].removed) continue;
                        char *pv = json_escape_field(typed_get_field_str(driver_fs.ts, val, (int)r->value_len, j));
                        OUT(",\"%s\"", pv ? pv : "");
                        free(pv);
                    }
                }
                OUT("]");
            } else if (dict_fmt) {
                OUT("%s\"%s\":", printed ? "," : "", r->key);
                if (proj_count > 0) {
                    OUT("{");
                    int first = 1;
                    for (int j = 0; j < proj_count; j++) {
                        char *pv = json_escape_field(decode_field((const char *)val, r->value_len, proj_fields[j], &driver_fs));
                        if (!pv) continue;
                        OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                        first = 0;
                        free(pv);
                    }
                    OUT("}");
                } else {
                    char *v = decode_value((const char *)val, r->value_len, &driver_fs);
                    OUT("%s", v);
                    free(v);
                }
            } else if (proj_count > 0) {
                OUT("%s{\"key\":\"%s\",\"value\":{", printed ? "," : "", r->key);
                int first = 1;
                for (int j = 0; j < proj_count; j++) {
                    char *pv = json_escape_field(decode_field((const char *)val, r->value_len, proj_fields[j], &driver_fs));
                    if (!pv) continue;
                    OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                    first = 0;
                    free(pv);
                }
                OUT("}}");
            } else {
                char *v = decode_value((const char *)val, r->value_len, &driver_fs);
                OUT("%s{\"key\":\"%s\",\"value\":%s}", printed ? "," : "", r->key, v);
                free(v);
            }
            printed++;
        }

        for (size_t i = 0; i < oc.count; i++) {
            free(oc.rows[i].key);
            free(oc.rows[i].record);
            free(oc.rows[i].sort_str);
        }
        free(oc.rows);
        pthread_mutex_destroy(&oc.lock);
    } else if (limit > 0 && (offset + limit) <= 1000 && njoins == 0 && !rows_fmt &&
               (fp.kind == FP_INTERSECT ||
                ((fp.kind == FP_PRIMARY_LEAF || fp.kind == FP_BITMAP_SMALLER) &&
                 tree)) &&
               /* Trigram-leaf is not streaming-friendly — idx_find_streaming
                  walks the leaf's btree (.idx) which doesn't exist for
                  trigram-only fields. Fall through to FP_PRIMARY_LEAF where
                  the trigram-aware dispatch lives. */
               !((fp.kind == FP_PRIMARY_LEAF || fp.kind == FP_BITMAP_SMALLER) &&
                 fp.n_source > 0 && fp.source_leaves[0] &&
                 (op_prefers_trigram(fp.source_leaves[0]->op) ||
                  op_allows_trigram_starts(fp.source_leaves[0]->op)) &&
                 field_has_index_type(db_root, object, fp.source_leaves[0]->field, IT_TRIGRAM))) {
        /* ===== Streaming fast path for limit-bound, post-filtered finds.
           For small (offset + limit) where the tree has post-filter siblings
           (FP_INTERSECT always; FP_PRIMARY_LEAF when tree.kind != LEAF),
           walk the most-selective leaf's btree and emit-as-we-go via
           criteria_match_tree. The collect-then-emit path's cap=offset+limit
           under-collects when a sibling rejects most candidates; streaming
           walks until enough records actually PASS, no over-fetch heuristic
           needed.
           Eligibility: no joins (join semantics need full materialise),
           no rows_fmt envelope (also needs full collect for column ordering),
           no order_by (would need sort across shards). */
        SearchCriterion *primary = (fp.kind == FP_INTERSECT)
            ? fp.source_leaves[0]   /* already most-selective-first */
            : (fp.n_source > 0 ? fp.source_leaves[0] : NULL);
        if (!primary) goto find_full_scan;
        int check_primary = op_needs_check_primary(primary->op);
        idx_find_streaming(db_root, object, &sch, primary, check_primary,
                           tree, &excluded, offset, limit,
                           proj_fields, proj_count,
                           (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                           rows_fmt, dict_fmt, csv_delim, &dl);
        /* Streaming fast path: compute total after the emit walk.
           FP_PRIMARY_LEAF / FP_BITMAP_SMALLER → idx_count_for_leaf on seed leaf
           gives the exact match count.
           FP_INTERSECT → the streaming path only walked the primary leaf and
           post-filtered via criteria_match_tree; it never materialized the full
           intersection KeySet, so the primary-leaf count is only an upper bound
           (not the true intersection size). Emit null for FP_INTERSECT to stay
           consistent with keyset_find_from_intersect's small-primary behavior. */
        if (want_total && fp.kind != FP_INTERSECT && fp.n_postfilter == 0) {
            find_total = idx_count_for_leaf(db_root, object, &sch, &driver_fs,
                                             primary, &dl);
            if (!dl.timed_out) find_total_null = 0;
        }
    } else if (fp.kind == FP_INTERSECT) {
        /* ===== AND INDEX-INTERSECTION FIND ===== */
        keyset_find_from_intersect(db_root, object, &sch, &fp, tree,
                                   &excluded, offset, limit,
                                   proj_fields, proj_count,
                                   (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                                   rows_fmt, dict_fmt, csv_delim, joins, njoins, &dl,
                                   want_total ? &find_total : NULL);
        if (want_total && !dl.timed_out) find_total_null = 0;
    } else if (fp.kind == FP_PRIMARY_LEAF || fp.kind == FP_BITMAP_SMALLER) {
        /* ===== INDEXED FIND: collect → group by shard → parallel process ===== */
        SearchCriterion *pc = fp.n_source > 0 ? fp.source_leaves[0] : NULL;
        if (!pc) goto find_full_scan;
        enum SearchOp op = pc->op;
        int check_primary = op_needs_check_primary(op);

        /* Dispatch on the picker's chosen index — same rulebook the
           planner used to claim this leaf, so we can't disagree.
           IT_TRIGRAM + OP_STARTS_WITH → A3 executor (leading-gram walk +
           per-record prefix verify): cheaper than build_keyset_from_trigram
           (no multi-gram intersection, no keyset allocation) when the query
           has no joins/rows_fmt/csv_delim.
           IT_TRIGRAM + other ops → keyset_emit_find sourced from the
           trigram intersection (OP_CONTAINS/ICONTAINS).
           IT_BITMAP and IT_BTREE both ride idx_find_parallel, which routes
           bitmap via btree_dispatch's internal bitmap branch. The picker has
           already enforced plen >= 3 for trigram, so build_keyset_from_trigram
           returning NULL here is a transient failure rather than an unsupported
           pattern. */
        int rc = 0;
        int picked = pick_index_for_leaf(db_root, object, pc);
        if (picked == IT_TRIGRAM) {
            /* A3: starts_with on trigram-only field — leading-gram walk +
               per-record full-prefix verify.  Guards: no joins (tabular
               materialise), no rows_fmt (table envelope), no csv_delim
               (header row).  Those cases fall through to the keyset path. */
            if (pc->op == OP_STARTS_WITH && !has_joins && !rows_fmt && !csv_delim) {
                find_via_trigram_starts_with(
                    db_root, object, &sch,
                    (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                    pc, tree, &excluded, offset, limit,
                    proj_fields, proj_count, dict_fmt, &dl);
                /* Close the envelope opened above and return. */
                if (dict_fmt)
                    OUT(want_total ? "},\"total\":null}\n" : "}\n");
                else
                    OUT(want_total ? "],\"total\":null}\n" : "]\n");
                free_excluded(&excluded);
                free_joins(joins, njoins);
                return 0;
            }
            KeySet *tg_ks = build_keyset_from_trigram(db_root, object,
                                                       sch.splits, pc, &dl);
            if (tg_ks) {
                rc = keyset_emit_find(db_root, object, &sch, tg_ks,
                                      tree, &excluded, offset, limit,
                                      proj_fields, proj_count,
                                      (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                                      rows_fmt, dict_fmt, csv_delim,
                                      joins, njoins, &dl);
                keyset_free(tg_ks);
            }
            goto find_emit_close;
        }
        /* idx_find_parallel: primary_idx_path arg is marked (void) inside the
           function (vestigial from the legacy QueryPlan era); pass "" safely. */
        rc = idx_find_parallel(db_root, object, &sch, "", tree,
                         pc, check_primary, &excluded, offset, limit,
                         proj_fields, proj_count,
                         (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                         rows_fmt, dict_fmt, csv_delim, joins, njoins, &dl);
        /* Phase 1d.2: for PRIMARY_LEAF/BITMAP_SMALLER, compute total after emit.
           ORDER_NONE / ORDER_SORT → idx_count_for_leaf on the seed leaf.
           ORDER_INDEX_WALK / ORDER_COMPOSITE → spec says null (handled above as
           early-return paths; unreachable here unless has_joins/rows_fmt/csv_delim
           forced fall-through, in which case null is correct). */
        if (want_total && !dl.timed_out &&
            (fp.order == FP_ORDER_NONE || fp.order == FP_ORDER_SORT)) {
            if (fp.kind == FP_BITMAP_SMALLER) {
                /* Bitmap smaller path: use bitmap popcount (same as cmd_count) */
                int picked_bm = pick_index_for_leaf(db_root, object, pc);
                if (picked_bm == IT_BITMAP) {
                    const TypedField *bm_tf = resolve_idx_field(driver_fs.ts, pc->field);
                    if (pc->op == OP_EQUAL || pc->op == OP_IN)
                        find_total = bm_popcount_for_crit(db_root, object,
                                                          sch.splits, pc, bm_tf);
                    else
                        find_total = bm_popcount_generic_for_crit(db_root, object,
                                                                   pc->field, sch.splits,
                                                                   pc, bm_tf);
                    if (!dl.timed_out) find_total_null = 0;
                } else {
                    /* Fell through without bitmap (unusual but defensive): count via leaf */
                    find_total = idx_count_for_leaf(db_root, object, &sch,
                                                    &driver_fs, pc, &dl);
                    if (!dl.timed_out) find_total_null = 0;
                }
            } else {
                /* FP_PRIMARY_LEAF: idx_count_for_leaf on the seed */
                find_total = idx_count_for_leaf(db_root, object, &sch,
                                                &driver_fs, pc, &dl);
                if (!dl.timed_out) find_total_null = 0;
            }
        }

        find_emit_close:
        if (rc == -2) {
            if (csv_delim) { /* nothing to close */ }
            else if (has_joins || rows_fmt) OUT(want_total ? "],\"total\":null}}\n" : "]}\n");
            else if (dict_fmt) OUT(want_total ? "},\"total\":null}\n" : "}\n");
            else OUT(want_total ? "],\"total\":null}\n" : "]\n");
            free_excluded(&excluded); free_joins(joins, njoins);
            OUT(QUERY_BUFFER_ERR);
            return -1;
        }
    } else if (fp.kind == FP_UNION) {
        /* ===== OR INDEX-UNION FIND ===== */
        int budget_exceeded = 0;
        keyset_find_from_or(db_root, object, &sch, tree, fp.or_node,
                            &excluded, offset, limit, proj_fields, proj_count,
                            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                            rows_fmt, dict_fmt, csv_delim, joins, njoins, &dl, &budget_exceeded,
                            want_total ? &find_total : NULL);
        if (!budget_exceeded && want_total && !dl.timed_out) find_total_null = 0;
        if (budget_exceeded) {
            /* Already wrote no rows. Close the open envelope cleanly, then emit error. */
            if (csv_delim) { /* no envelope to close */ }
            else if (has_joins || rows_fmt) OUT(want_total ? "],\"total\":null}}\n" : "]}\n");
            else if (dict_fmt) OUT(want_total ? "},\"total\":null}\n" : "}\n");
            else OUT(want_total ? "],\"total\":null}\n" : "]\n");
            free_excluded(&excluded);
            free_joins(joins, njoins);
            OUT(QUERY_BUFFER_ERR);
            return -1;
        }
    } else {
    find_full_scan: ;  /* empty stmt: pre-C23 disallows label→declaration directly */
        /* ===== FULL SCAN FALLBACK ===== */
        /* Hoist single-leaf compiled criterion for inline matching
           (mirrors the COUNT FP_FULL_SCAN path). */
        const CompiledCriterion *fast_cc = NULL;
        if (tree) {
            const CriteriaNode *leaf = NULL;
            if (tree->kind == CNODE_LEAF) leaf = tree;
            else if (tree->kind == CNODE_AND && tree->n_children == 1 &&
                     tree->children[0]->kind == CNODE_LEAF) leaf = tree->children[0];
            if (leaf && leaf->compiled) fast_cc = leaf->compiled;
        }

        AdvSearchCtx ctx = { tree, fast_cc, offset, limit, 0, 0,
                             proj_fields, proj_count, excluded, &driver_fs,
                             rows_fmt, dict_fmt, csv_delim,
                             object, joins, njoins, db_root, &dl, 0,
                             PTHREAD_MUTEX_INITIALIZER };
        /* Limit-bound full scan? Use the streaming walker so cb's stop
           response is per-record. Otherwise the buffered Pass-1 path
           collects refs the limit will never read. Same threshold + form
           as cmd_keys; see scan_shards_v2_streaming. */
        int use_streaming = (limit > 0 && limit <= 1000);
        if (use_streaming) {
            SlotcaskSchemaInfo info = {
                .splits = sch.splits, .slot_size = sch.slot_size,
                .streams = sch.streams,
            };
            SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
            if (sdb) scan_shards_v2_streaming(sdb, adv_search_cb, &ctx);
            else     scan_dispatch(db_root, object, &sch, data_dir, adv_search_cb, &ctx);
        } else {
            scan_dispatch(db_root, object, &sch, data_dir, adv_search_cb, &ctx);
        }
        pthread_mutex_destroy(&ctx.lock);
    }

    if (csv_delim)
        { /* CSV body already ends with its own \n per row — nothing to close (joined or not) */ }
    else if (want_total) {
        /* Phase 1d follow-up: if the dispatch path didn't compute total
         * cheaply (FP_FULL_SCAN, postfilter-bearing plans), call the
         * shared fp_compute_total helper. Falls back to null when the
         * plan can't cheaply produce a count (e.g. FP_FULL_SCAN — caller
         * can fire a separate count query for those rare shapes). */
        if (find_total_null && !dl.timed_out) {
            int helper_null = 1;
            size_t n = fp_compute_total(&fp, tree, db_root, object,
                                         &sch, &driver_fs, &dl, &helper_null, NULL);
            if (!helper_null) {
                find_total = n;
                find_total_null = 0;
            }
        }
        const char *arr_close = (has_joins || rows_fmt) ? "]}" :
                                 dict_fmt                ? "}"  : "]";
        if (find_total_null)
            OUT("%s,\"total\":null}\n", arr_close);
        else
            OUT("%s,\"total\":%zu}\n", arr_close, find_total);
    } else if (has_joins)
        OUT("]}\n");
    else if (rows_fmt)
        OUT("]}\n");
    else if (dict_fmt)
        OUT("}\n");
    else
        OUT("]\n");
    free_excluded(&excluded);

    free_joins(joins, njoins);

    return 0;
}

int cmd_find(const char *db_root, const char *object,
                    const char *criteria_json, int offset, int limit,
                    const char *proj_str, const char *excluded_csv,
                    const char *format, const char *delimiter,
                    const char *join_json,
                    const char *order_by, const char *order_dir,
                    const char *cursor_json, int want_total) {
    int dict_fmt = (format && strcmp(format, "dict") == 0);

    /* Parse joins (if any) — forces tabular output irrespective of `format`. */
    JoinSpec *joins = NULL;
    int njoins = 0;
    if (join_json && join_json[0]) {
        if (parse_joins_json(join_json, &joins, &njoins) < 0) {
            OUT("{\"error\":\"invalid 'join' array\"}\n");
            return -1;
        }
    }

    if (dict_fmt && njoins > 0) {
        OUT("{\"error\":\"format=dict is not supported with join\"}\n");
        free_joins(joins, njoins);
        return -1;
    }

    /* Parse criteria into a tree (AND/OR supported). Empty/absent → no criteria. */
    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &perr);
    if (perr) {
        OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
        free_criteria_tree(tree);
        free_joins(joins, njoins);
        return -1;
    }

    int r = cmd_find_do(db_root, object, tree, joins, njoins,
                        offset, limit, proj_str, excluded_csv,
                        format, delimiter, order_by, order_dir,
                        cursor_json, want_total);
    free_criteria_tree(tree);
    return r;
}

int cmd_find_tree(const char *db_root, const char *object, CriteriaNode *tree,
                  int offset, int limit, const char *proj_str,
                  const char *format, const char *delimiter,
                  const char *order_by, const char *order_dir, int want_total,
                  const char *cursor_json) {
    return cmd_find_do(db_root, object, tree,
                       NULL, 0,           /* no joins */
                       offset, limit, proj_str,
                       NULL,              /* no excluded_csv */
                       format, delimiter,
                       order_by, order_dir,
                       cursor_json,
                       want_total);
}

#ifdef TEST_BUILD
int composite_prefix_walk_for_test(const char *db_root, const char *object,
                                    const char *criteria_json,
                                    const char *order_by, int order_desc,
                                    int limit) {
    snprintf(g_db_root, PATH_MAX, "%s", db_root);
    char eff_root[PATH_MAX], bare[256];
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t d = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s", db_root, (int)d, object);
        snprintf(bare, sizeof(bare), "%s", slash + 1);
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        snprintf(bare, sizeof(bare), "%s", object);
    }
    const char *err = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &err);
    if (!tree) return -1;
    Schema sc = load_schema(eff_root, bare);
    if (sc.splits <= 0) { free_criteria_tree(tree); return -1; }
    FieldSchema fs;
    init_field_schema(&fs, eff_root, bare);
    if (!fs.ts) { free_criteria_tree(tree); return -1; }
    size_t N = (size_t)get_live_count(eff_root, bare);
    FilterPlan fp = plan_filter(tree, eff_root, bare, &fs, sc.splits, N,
                                 order_by, 1, limit);
    if (fp.order != FP_ORDER_COMPOSITE || fp.kind != FP_PRIMARY_LEAF ||
        fp.n_source < 1 || !fp.source_leaves[0]) {
        free_criteria_tree(tree);
        return -2;
    }
    FILE *saved_out = g_out;
    g_out = fopen("/dev/null", "w");
    if (!g_out) { g_out = saved_out; free_criteria_tree(tree); return -1; }
    ExcludedKeys excluded = {0};
    QueryDeadline dl = { .t0_ms = 0, .timeout_ms = 0, .timed_out = 0 };
    g_order_walk_scanned = 0;
    find_via_composite_prefix(
        eff_root, bare, &sc,
        (fs.ts || fs.nfields > 0) ? &fs : NULL,
        fp.source_leaves[0], order_by, order_desc,
        fp.order_range,
        tree, &excluded, 0, limit,
        NULL, 0, 0, &dl);
    long scanned = g_order_walk_scanned;
    fclose(g_out);
    g_out = saved_out;
    free_criteria_tree(tree);
    return (int)scanned;
}

/* Test-only helper: compute the composite-prefix upper-bound bytes for a
   seed value. Returns the bound length, or -1 on error. The bound bytes
   are written to out_hi (up to 1024+8). Tests use this to verify the
   successor approach produces a tight bound for VARCHAR seeds. */
int composite_prefix_bound_for_test(const char *db_root, const char *object,
                                     const char *criteria_json,
                                     const char *order_by,
                                     uint8_t *out_hi, size_t *out_hi_len,
                                     uint8_t *out_lo, size_t *out_lo_len,
                                     int *out_min_excl, int *out_max_excl) {
    snprintf(g_db_root, PATH_MAX, "%s", db_root);
    char eff_root[PATH_MAX], bare[256];
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t d = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s", db_root, (int)d, object);
        snprintf(bare, sizeof(bare), "%s", slash + 1);
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        snprintf(bare, sizeof(bare), "%s", object);
    }
    const char *err = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &err);
    if (!tree) return -1;
    Schema sc = load_schema(eff_root, bare);
    if (sc.splits <= 0) { free_criteria_tree(tree); return -1; }
    FieldSchema fs;
    init_field_schema(&fs, eff_root, bare);
    if (!fs.ts) { free_criteria_tree(tree); return -1; }
    size_t N = (size_t)get_live_count(eff_root, bare);
    FilterPlan fp = plan_filter(tree, eff_root, bare, &fs, sc.splits, N,
                                 order_by, 1, 0);
    if (fp.order != FP_ORDER_COMPOSITE || fp.kind != FP_PRIMARY_LEAF ||
        fp.n_source < 1 || !fp.source_leaves[0]) {
        free_criteria_tree(tree);
        return -2;
    }
    const TypedField *seed_tf = resolve_idx_field(fs.ts, fp.source_leaves[0]->field);
    uint8_t buf_lo[1024 + 8];
    size_t len_lo = 0;
    encode_criterion_value(seed_tf, fp.source_leaves[0]->value,
                           strlen(fp.source_leaves[0]->value), buf_lo, &len_lo);
    uint8_t buf_hi[1024 + 8];
    memcpy(buf_hi, buf_lo, len_lo);
    size_t len_hi;
    int min_excl = 0, max_excl = 0;
    if (seed_tf && seed_tf->type == FT_VARCHAR && len_lo > 0) {
        int pos = (int)len_lo - 1;
        while (pos >= 0 && buf_lo[pos] == 0xff) pos--;
        if (pos >= 0) {
            memcpy(buf_hi, buf_lo, (size_t)pos);
            buf_hi[pos] = buf_lo[pos] + 1;
            len_hi = (size_t)pos + 1;
        } else {
            memcpy(buf_hi, buf_lo, len_lo);
            buf_hi[len_lo] = 0x00;
            len_hi = len_lo + 1;
        }
    } else {
        memset(buf_hi + len_lo, 0xff, 4);
        len_hi = len_lo + 4;
    }
    if (fp.order_range) {
        const TypedField *ord_tf = resolve_idx_field(fs.ts, order_by);
        const char *highv = NULL; int high_excl = 0;
        switch (fp.order_range->op) {
            case OP_LESS_EQ:    highv = fp.order_range->value; break;
            case OP_LESS:       highv = fp.order_range->value; high_excl = 1; break;
            case OP_EQUAL:      highv = fp.order_range->value; break;
            case OP_BETWEEN:    highv = fp.order_range->value2; high_excl = fp.order_range->max_exclusive; break;
            default: break;
        }
        if (highv) {
            uint8_t enc[1024]; size_t el = 0;
            encode_criterion_value(ord_tf, highv, strlen(highv), enc, &el);
            if (len_lo + el <= sizeof(buf_hi)) {
                memcpy(buf_hi + len_lo, enc, el);
                len_hi = len_lo + el;
                max_excl = high_excl;
            }
        }
    }
    if (out_hi && out_hi_len) { memcpy(out_hi, buf_hi, len_hi); *out_hi_len = len_hi; }
    if (out_lo && out_lo_len) { memcpy(out_lo, buf_lo, len_lo); *out_lo_len = len_lo; }
    if (out_min_excl) *out_min_excl = min_excl;
    if (out_max_excl) *out_max_excl = max_excl;
    free_criteria_tree(tree);
    return (int)len_hi;
}
#endif


