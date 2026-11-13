#include "typed_index.h"
#include "types.h"
#include "slotcask.h"
#include "simd.h"
#include "bitmap.h"
#include "trigram.h"
#include "io_direct.h"
#include "query_internal.h"
#include "typed_index.h"
#include "typed_row.h"
#include <math.h>
#include <dirent.h>
/* Bitmap-KeySet race seam stage ids: plain identifiers in all builds; the
 * seam functions below are TEST_BUILD-only (see query_internal.h). */
#define BMKS_RACE_EQ         1
#define BMKS_RACE_GENERIC    2
#define BMKS_RACE_COMPLEMENT 3
#define BMKS_RACE_OR         4

#ifdef TEST_BUILD
static void bm_defer_gate_park(void);   /* defined with the find-flush gate */

#include <stdatomic.h>
#include "shard_test_ctl.h"
#include "test_control.h"

/* Bitmap-KeySet race seam (TEST_BUILD only, inert in production builds):
 * a one-shot barrier parked between a bitmap candidate builder's sizing
 * pass and its enumeration pass so a concurrent writer can add matches
 * that overflow the already-allocated KeySet. Arming is per calling thread
 * with an explicit stage id, so only that thread's own build consumes the
 * barrier; the coordination state (parked flag, gate, observation counters)
 * is process-visible so the test's writer thread can observe the park and
 * release it. No production code path reads any of this state. */

/* Daemon-side arm (control channel SET_GLOBALS phase 4; see shard_test_ctl.h).
 * A stage armed here is consumed once by the next matching build on any
 * thread; the gate is released via the same channel or the test hook. */
_Atomic int g_shard_test_bmks_stage;
_Atomic int g_shard_test_bmks_gate;

static __thread _Atomic int bm_ks_race_armed;   /* BMKS_RACE_* stage or 0 */

static _Atomic int      bm_ks_race_reached;     /* barrier consumed */
static _Atomic int      bm_ks_race_gate;        /* 0 = parked, 1 = released */
static _Atomic int      bm_ks_race_gate_opened; /* gate released for this run */
static _Atomic unsigned bm_ks_race_cap;         /* KeySet cap behind barrier */
static _Atomic unsigned bm_ks_race_sized;       /* sizing-pass estimate */
static _Atomic unsigned bm_ks_race_fail_inserts;

int query_bm_test_arm_keyset_race(int stage) {
    if (stage <= 0) return -1;
    atomic_store(&bm_ks_race_armed, stage);
    atomic_store(&bm_ks_race_reached, 0);
    atomic_store(&bm_ks_race_gate, 0);
    atomic_store(&bm_ks_race_gate_opened, 0);
    atomic_store(&bm_ks_race_cap, 0);
    atomic_store(&bm_ks_race_sized, 0);
    atomic_store(&bm_ks_race_fail_inserts, 0);
    return 0;
}

void query_bm_test_race_open_gate(void) {
    atomic_store(&bm_ks_race_gate, 1);
    atomic_store(&bm_ks_race_gate_opened, 1);
}

int query_bm_test_race_reached(void) {
    return atomic_load(&bm_ks_race_reached);
}

int query_bm_test_race_gate_opened(void) {
    return atomic_load(&bm_ks_race_gate_opened);
}

unsigned query_bm_test_race_allocated_cap(void) {
    return atomic_load(&bm_ks_race_cap);
}

unsigned query_bm_test_race_sized_total(void) {
    return atomic_load(&bm_ks_race_sized);
}

unsigned query_bm_test_race_insert_fails(void) {
    return atomic_load(&bm_ks_race_fail_inserts);
}

void query_bm_test_race_reset(void) {
    atomic_store(&bm_ks_race_armed, 0);
    atomic_store(&bm_ks_race_gate, 0);
}

/* Barrier call site: consume the armed stage, publish the sizing result
 * and the allocated capacity, then poll the gate (bounded; the test's
 * child watchdog is the outer bound). One-shot: disarms itself. */
static void bm_ks_race_barrier(int stage, size_t sized_total, size_t cap) {
    int global_armed = atomic_load(&g_shard_test_bmks_stage) == stage;
    int local_armed = atomic_load(&bm_ks_race_armed) == stage;
    if (!local_armed && !global_armed) return;
    atomic_store(&bm_ks_race_sized, (unsigned)sized_total);
    atomic_store(&bm_ks_race_cap, (unsigned)cap);
    atomic_store(&bm_ks_race_reached, 1);
    if (global_armed) {
        atomic_store(&g_shard_test_bmks_stage, 0);   /* one-shot */
        LOG_WARN(LOG_SUB_QUERY,
                 "BMKS-BARRIER-REACHED stage=%d sized=%zu cap=%u",
                 stage, sized_total, (unsigned)cap);
        /* The find-flush gate hook blocks until the test releases it; its
         * return IS the release for the daemon path (no second wait). */
        bm_defer_gate_park();
        return;
    }
    for (int i = 0; i < 40000; i++) {          /* bounded park (~120 s max) */
        if (atomic_load_explicit(&bm_ks_race_gate, memory_order_acquire) ||
            atomic_load_explicit(&g_shard_test_bmks_gate,
                                 memory_order_acquire))
            break;
        struct timespec ts = { 0, 3 * 1000 * 1000L };
        nanosleep(&ts, NULL);
    }
    atomic_store(&bm_ks_race_armed, 0);
    atomic_store(&bm_ks_race_gate, 0);
}

/* Observation seams are thread-local and request-scoped in test builds:
 * per-thread counters for candidate allocation sizing, actual insertion
 * overflows (keyset_insert == -1 in a bitmap collector), whole-record
 * capacity fallback recoveries, and the last prepared-find budget peak.
 * Arming happens on the thread that runs the query. */
static __thread struct {
    unsigned last_alloc_slots;
    unsigned overflow_hits;
    unsigned fallbacks;
    unsigned last_budget_peak;
    int      observing;
} bm_ks_test_stats;

static __thread char bm_ks_parallel_field[256];
static __thread int bm_ks_parallel_stage;
static __thread char bm_ks_parallel_fault_field[256];
static __thread int bm_ks_parallel_fault_site;
static __thread int bm_ks_parallel_fault_error;
static __thread unsigned bm_ks_parallel_stats[6];

int query_test_parallel_candidate_race(const char *field, int stage) {
    if (stage == 0) {
        bm_ks_parallel_stage = 0;
        bm_ks_parallel_field[0] = '\0';
        return 0;
    }
    if (!field || strlen(field) >= sizeof(bm_ks_parallel_field) ||
        stage < BMKS_RACE_EQ || stage > BMKS_RACE_OR)
        return -1;
    if (query_bm_test_arm_keyset_race(stage) != 0) return -1;
    atomic_store(&bm_ks_race_armed, 0);
    snprintf(bm_ks_parallel_field, sizeof(bm_ks_parallel_field), "%s", field);
    bm_ks_parallel_stage = stage;
    memset(bm_ks_parallel_stats, 0, sizeof(bm_ks_parallel_stats));
    return 0;
}

int query_test_parallel_candidate_fault(const char *field, int site, int error) {
    if (error == 0) { bm_ks_parallel_fault_error = 0; return 0; }
    if (!field || strlen(field) >= sizeof(bm_ks_parallel_fault_field) ||
        site < 0 || site >= BM_TEST_LOCK_SITE_COUNT)
        return -1;
    snprintf(bm_ks_parallel_fault_field, sizeof(bm_ks_parallel_fault_field), "%s", field);
    bm_ks_parallel_fault_site = site;
    bm_ks_parallel_fault_error = error;
    return 0;
}

void query_test_parallel_candidate_stats(unsigned *workers, unsigned *joined,
                                         unsigned *capacity, unsigned *hard,
                                         unsigned *offthread, unsigned *consumed) {
    if (workers) *workers = bm_ks_parallel_stats[0];
    if (joined) *joined = bm_ks_parallel_stats[1];
    if (capacity) *capacity = bm_ks_parallel_stats[2];
    if (hard) *hard = bm_ks_parallel_stats[3];
    if (offthread) *offthread = bm_ks_parallel_stats[4];
    if (consumed) *consumed = bm_ks_parallel_stats[5];
}

void query_test_candidate_observe(int on) {
    memset(&bm_ks_test_stats, 0, sizeof(bm_ks_test_stats));
    bm_ks_test_stats.observing = on;
}
void query_test_candidate_stats(unsigned *last_slots, unsigned *overflow_hits,
                                unsigned *fallbacks, unsigned *budget_peak) {
    if (last_slots) *last_slots = bm_ks_test_stats.last_alloc_slots;
    if (overflow_hits) *overflow_hits = bm_ks_test_stats.overflow_hits;
    if (fallbacks) *fallbacks = bm_ks_test_stats.fallbacks;
    if (budget_peak) *budget_peak = bm_ks_test_stats.last_budget_peak;
}
#define BMKS_STAT_OVERFLOW() do { if (bm_ks_test_stats.observing) \
        bm_ks_test_stats.overflow_hits++; } while (0)
#define BMKS_STAT_FALLBACK(kind) do { \
        if (bm_ks_test_stats.observing) bm_ks_test_stats.fallbacks++; \
        if (bm_ks_test_stats.observing || atomic_load(&g_shard_test_bmlock_log)) \
            LOG_WARN(LOG_SUB_QUERY, \
                     "candidate capacity exceeded after concurrent growth; " \
                     "recovering from full record scan (%s)", kind); \
    } while (0)
#define BMKS_STAT_ALLOC(slots) do { if (bm_ks_test_stats.observing) \
        bm_ks_test_stats.last_alloc_slots = (unsigned)(slots); } while (0)
#define BMKS_STAT_BUDGET_PEAK(pk) do { if (bm_ks_test_stats.observing) \
        bm_ks_test_stats.last_budget_peak = (unsigned)(pk); } while (0)

static inline int bm_ks_counted_insert(KeySet *ks, const uint8_t *hash) {
    int rc = keyset_insert(ks, hash);
    if (rc < 0) {
        atomic_fetch_add_explicit(&bm_ks_race_fail_inserts, 1,
                                  memory_order_relaxed);
        BMKS_STAT_OVERFLOW();
    }
    return rc;
}

/* TEST_BUILD override for a prepared find's request budget limit (0 = the
 * query-buffer default). Thread-local: scoped to the calling request. */
static __thread size_t bm_ks_test_budget_limit;
void query_test_prepared_budget_limit(size_t limit) {
    bm_ks_test_budget_limit = limit;
}
size_t query_test_prepared_budget_limit_value(void) {
    return bm_ks_test_budget_limit;
}

void query_test_candidate_fallback_note(const char *kind) {
    BMKS_STAT_FALLBACK(kind);
}
#else
#define BMKS_STAT_FALLBACK(kind) ((void)0)
static inline void bm_ks_race_barrier(int stage, size_t sized_total,
                                      size_t cap) {
    (void)stage; (void)sized_total; (void)cap;
}
static inline int bm_ks_counted_insert(KeySet *ks, const uint8_t *hash) {
    return keyset_insert(ks, hash);
}
static inline size_t query_test_prepared_budget_limit_value(void) {
    return 0;
}
#endif

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
        size_t cl = strlen(search_criterion_value(c));
        if (vlen < cl) return 0;
        return memcmp(val + vlen - cl, search_criterion_value(c), cl) == 0;
    }
    case OP_STARTS_WITH: {
        size_t cl = strlen(search_criterion_value(c));
        if (vlen < cl) return 0;
        return memcmp(val, search_criterion_value(c), cl) == 0;
    }
    case OP_CONTAINS: {
        size_t cl = strlen(search_criterion_value(c));
        if (cl == 0) return 1;
        if (vlen < cl) return 0;
        return memmem(val, vlen, search_criterion_value(c), cl) != NULL;
    }
    case OP_NOT_CONTAINS: {
        size_t cl = strlen(search_criterion_value(c));
        if (cl == 0) return 0;
        if (vlen < cl) return 1;
        return memmem(val, vlen, search_criterion_value(c), cl) == NULL;
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
            return strcmp(val_str, search_criterion_value(c)) == 0;
        case OP_NOT_EQUAL:
            return strcmp(val_str, search_criterion_value(c)) != 0;
        case OP_LESS:
            if (is_number(val_str) && is_number(search_criterion_value(c)))
                return atof(val_str) < atof(search_criterion_value(c));
            return strcmp(val_str, search_criterion_value(c)) < 0;
        case OP_GREATER:
            if (is_number(val_str) && is_number(search_criterion_value(c)))
                return atof(val_str) > atof(search_criterion_value(c));
            return strcmp(val_str, search_criterion_value(c)) > 0;
        case OP_LESS_EQ:
            if (is_number(val_str) && is_number(search_criterion_value(c)))
                return atof(val_str) <= atof(search_criterion_value(c));
            return strcmp(val_str, search_criterion_value(c)) <= 0;
        case OP_GREATER_EQ:
            if (is_number(val_str) && is_number(search_criterion_value(c)))
                return atof(val_str) >= atof(search_criterion_value(c));
            return strcmp(val_str, search_criterion_value(c)) >= 0;
        case OP_BETWEEN:
            if (is_number(val_str) && is_number(search_criterion_value(c)) && is_number(search_criterion_value2(c))) {
                double v = atof(val_str);
                int lo = c->min_exclusive ? (v > atof(search_criterion_value(c))) : (v >= atof(search_criterion_value(c)));
                int hi = c->max_exclusive ? (v < atof(search_criterion_value2(c))) : (v <= atof(search_criterion_value2(c)));
                return lo && hi;
            }
            {
                int rl = strcmp(val_str, search_criterion_value(c));
                int rh = strcmp(val_str, search_criterion_value2(c));
                int lo = c->min_exclusive ? (rl >  0) : (rl >= 0);
                int hi = c->max_exclusive ? (rh <  0) : (rh <= 0);
                return lo && hi;
            }
        case OP_LIKE: {
            /* SQL-style LIKE with `%` wildcard (no `_`). Case-sensitive —
               use `ilike` for case-insensitive. */
            const char *pat = search_criterion_value(c);
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
            return strstr(val_str, search_criterion_value(c)) != NULL;
        case OP_NOT_CONTAINS:
            return strstr(val_str, search_criterion_value(c)) == NULL;
        case OP_STARTS_WITH:
            return strncmp(val_str, search_criterion_value(c), strlen(search_criterion_value(c))) == 0;
        case OP_ENDS_WITH: {
            size_t vl = strlen(val_str), cl = strlen(search_criterion_value(c));
            if (vl < cl) return 0;
            return strcmp(val_str + vl - cl, search_criterion_value(c)) == 0;
        }
        /* Case-insensitive variants (ASCII tolower). Indexed paths fall
           through to the default full-scan branch in btree_dispatch and
           filter per entry in the callbacks below. */
        case OP_ILIKE: {
            const char *pat = search_criterion_value(c);
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
            return strcasestr(val_str, search_criterion_value(c)) != NULL;
        case OP_INOT_CONTAINS:
            return strcasestr(val_str, search_criterion_value(c)) == NULL;
        case OP_ISTARTS_WITH:
            return strncasecmp(val_str, search_criterion_value(c), strlen(search_criterion_value(c))) == 0;
        case OP_IENDS_WITH: {
            size_t vl = strlen(val_str), cl = strlen(search_criterion_value(c));
            if (vl < cl) return 0;
            return strcasecmp(val_str + vl - cl, search_criterion_value(c)) == 0;
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
                if (!pat_compiled || strcmp(pat_cached, search_criterion_value(c)) != 0) {
                    if (pat_compiled) { regfree(&re_cached); pat_compiled = 0; }
                    if (regcomp(&re_cached, search_criterion_value(c), REG_EXTENDED | REG_NOSUB) != 0)
                        return c->op == OP_REGEX ? 0 : 1;
                    strncpy(pat_cached, search_criterion_value(c), sizeof(pat_cached) - 1);
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
        case OP_UNKNOWN:
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
        if (!idx_candidate_admits(cc->tf, val, vlen, cc->primary_crit))
            return 0;
    }

    /* Atomic slot allocation. Beyond cap or the caller-supplied early-stop
       limit → bail; the buffer is mmap'd up to the per-query cap so the
       former realloc-with-mutex path is gone. */
    size_t idx = __atomic_fetch_add(&cc->count, 1, __ATOMIC_RELAXED);
    
    if (cc->collect_cap > 0 && idx >= (size_t)cc->collect_cap) return -1;
    if (idx >= cc->cap) {
        LOG_WARN(LOG_SUB_QUERY, "collect_hash_cb: query buffer cap exceeded (cap=%zu)", cc->cap);
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
        if (!idx_candidate_admits(sk->tf, val, vlen, sk->primary_crit))
            return 0;
    }
    (void)val;
    if (keyset_insert(sk->ks, hash16) < 0) {
        LOG_WARN(LOG_SUB_QUERY, "stream_keyset_cb: keyset capacity exhausted, aborting walk");
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
   memcmp-sortable bytes matching what the write side stored. Libc-parsed
   scalar fields require a complete NUL-terminated spelling (val[vlen] == 0);
   raw and varchar values retain their explicit-span contract. Output written
   into caller's buf; *out_len set. */
int encode_criterion_value_checked(const TypedSchema *ts,const TypedField *field,
    const char *text,size_t len,uint8_t *out,size_t cap,size_t *out_len){
    if(!out_len){errno=EINVAL;return -1;}*out_len=0;
    if(!text||!out){errno=EINVAL;return -1;}
    if(!field){if(len>cap){errno=ENOBUFS;return -1;}if(len)memcpy(out,text,len);*out_len=len;return 1;}
    if(!ts){errno=EINVAL;return -1;}
    int index=-1;for(int i=0;i<ts->nfields;i++)if(field==&ts->fields[i]){index=i;break;}
    if(index<0){errno=EINVAL;return -1;}
    return typed_index_text_key_into(ts,(size_t)index,text,len,out,cap,out_len);
}
/* Legacy fixture compatibility; production producers use the checked schema API. */
static int bitmap_text_key_alloc(const TypedSchema *ts,const TypedField *tf,
    const char *text,size_t text_len,size_t budget,uint8_t **out,size_t *len){
    *out=NULL;*len=0;
    int field=-1;
    if(tf){if(!ts){errno=EINVAL;return -1;}field=typed_field_index(ts,tf->name);
        if(field<0){errno=EINVAL;return -1;}}
    size_t need=text_len;
    int rc=tf?typed_index_text_key_into(ts,(size_t)field,text,text_len,NULL,0,&need):1;
    if(rc!=1)return rc;
    if(need>UINT16_MAX||need>budget){errno=ENOBUFS;return -1;}
    uint8_t *buf=malloc(need?need:1);
    if(!buf){errno=ENOMEM;return -1;}
    if(tf)rc=typed_index_text_key_into(ts,(size_t)field,text,text_len,buf,need,&need);
    else if(need)memcpy(buf,text,need);
    if(rc!=1){free(buf);return rc;}
    *out=buf;*len=need;return 1;
}
static int bitmap_literal_fits(const TypedSchema *ts,const SearchCriterion *c,size_t cap){
    if(!ts||!c)return 0;
    int field=typed_field_index(ts,c->field);
    if(field<0)return 0;
    int count=c->op==OP_IN?c->in_count:1;
    for(int i=0;i<count;i++){
        const char *text=c->op==OP_IN?c->in_values[i]:search_criterion_value(c);
        size_t n=0;
        if(typed_index_text_key_into(ts,(size_t)field,text,strlen(text),NULL,0,&n)!=1||n==0||n>cap)return 0;
    }
    return 1;
}
static void bitmap_values_clear(uint8_t **values,int count){
    if(values){for(int i=0;i<count;i++)free(values[i]);free(values);}
}

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
    encode_field_text_for_index(tf, val, vlen, buf, out_len);
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
           op == OP_REGEX || op == OP_NOT_REGEX ||
           /* §2.4.2: an exists leaf drives the tier-0 walk, but legacy
              zero-markers (empty varchar, midnight, zero date) sit in
              tier 0 too — every candidate is verified per record with
              the generation-correct query-existence predicate. */
           op == OP_EXISTS;
}

/* True if the op is a length comparator answerable from (val, vlen) alone —
   no record fetch needed, just inspect the btree leaf entry's vlen. */
int op_is_length(enum SearchOp op) {
    return op == OP_LEN_EQ || op == OP_LEN_NEQ ||
           op == OP_LEN_LESS || op == OP_LEN_GREATER ||
           op == OP_LEN_LESS_EQ || op == OP_LEN_GREATER_EQ ||
           op == OP_LEN_BETWEEN;
}

/* Sound tier-0 candidate pre-filter (§2.4.2). Returns 1 to admit the leaf
   entry to per-record verification, 0 to reject it. A reject requires an
   exactly-decoded mismatch: raw/composite keys and varchar leaves (both
   record generations) carry the exact text bytes, so a 0-byte payload is
   the empty string, not a zero-marker. Everything the legacy decoder
   cannot render exactly admits instead — packed-numeric decimal_order
   payloads (the 1-byte zero marker and odd limb widths; legacy numeric is
   exactly 8 bytes per typed_index_fixed_width) and zero-shaped fixed
   values (all-zero uuid/ipv4/ipv6 decode as legacy-missing) reach the
   generation-correct record check, which is authoritative. The pre-filter
   only bounds candidate fetches; it never decides existence. */
int idx_candidate_admits(const TypedField *tf, const char *payload,
                         size_t plen, const SearchCriterion *c) {
    if (!c) return 1;
    char tmp[1028];
    if (!tf || tf->type == FT_VARCHAR) {
        size_t cl = plen < sizeof(tmp) - 1 ? plen : sizeof(tmp) - 1;
        memcpy(tmp, payload, cl);
        tmp[cl] = '\0';
        return match_criterion_vlen(tmp, cl, c);
    }
    if (tf->type == FT_NUMERIC && plen != 8)
        return 1; /* decimal_order payload: record check decides */
    int dlen = decode_idx_to_buf(tf, (const uint8_t *)payload, plen,
                                 tmp, sizeof(tmp), 0);
    if (dlen <= 0)
        return 1; /* zero-shaped/short payload: record check decides */
    return match_criterion(tmp, c);
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
    int          error;         /* worker-owned; read after parallel_for_io */
} BmShardWalkArg;

/* Walk a single shard's bitmap for `value`, emitting cb(value, vlen, hash)
   per live matching slot. Returns 1 when cb signalled stop, 0 when the shard
   was walked completely (or is logically empty), -1 with errno on a hard
   bitmap read failure. */
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
    const TypedSchema *schema;
    size_t             budget;
    int                error; /* worker-owned; read after parallel_for_io */
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

/* §2.4.2: the O_DIRECT full-leaf scan delivers the value-tier view —
   strip the tier byte, skip NULL-tier entries (mirror of index.c's
   btri_value_tier_cb, which the mmap walk paths use). */
static int btree_od_tier_cb(const uint8_t *value, size_t vlen,
                            const uint8_t hash16[16], void *raw_ctx)
{
    BtreeOdShardArg *arg = (BtreeOdShardArg *)raw_ctx;
    if (vlen == 0) return 0;
    if ((uint8_t)value[0] != 0x00) return 0;
    /* bt_result_cb and od_leaf_cb have identical runtime layout. */
    return arg->cb((const char *)value + 1, vlen - 1, hash16, arg->ctx);
}

static void *btree_od_shard_worker(void *raw)
{
    BtreeOdShardArg *arg = (BtreeOdShardArg *)raw;
    btree_leaf_scan_o_direct(arg->idx_path, btree_od_tier_cb, arg);
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

int btree_dispatch_schema_checked(const TypedSchema *literal_schema,const char *db_root, const char *object,
                           const char *field, int splits,
                           SearchCriterion *pc, const TypedField *tf,
                           bt_result_cb cb, void *ctx) {

    if (field_index_type(db_root, object, field) == IT_BITMAP) {
        Schema sc = load_schema(db_root, object);
        SlotcaskSchemaInfo info = {
            .splits = sc.splits, .slot_size = sc.slot_size, .streams = sc.streams,
        };
        SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
        if (!sdb) return -1;

        if (pc->op == OP_EQUAL || pc->op == OP_IN) {
            /* Fast path. For OP_IN we loop once per value, parallel-fanning
               each value across all shards. Per-value bitmaps are disjoint
               so emitting across values produces no duplicates. The
               stop_flag is shared across all per-value parallel_for
               invocations so an early-stop (e.g. find's limit) propagates. */
            int n_vals = (pc->op == OP_EQUAL) ? 1 : pc->in_count;
            if (n_vals <= 0) return 0;

            int stop_flag = 0;
            BmShardWalkArg *args = malloc((size_t)splits * sizeof(BmShardWalkArg));
            if (!args) return -1;

            for (int vi = 0; vi < n_vals; vi++) {
                if (__atomic_load_n(&stop_flag, __ATOMIC_RELAXED)) break;
                uint8_t *valbuf = NULL;
                size_t vallen = 0;
                const char *raw = (pc->op == OP_EQUAL) ? search_criterion_value(pc) : pc->in_values[vi];
                int present=bitmap_text_key_alloc(literal_schema,tf,raw,strlen(raw),
                    g_query_buffer_max_bytes,&valbuf,&vallen);
                if(present<0){free(args);return -1;}
                if(present==0){free(valbuf);continue;}
                for (int s = 0; s < splits; s++) {
                    args[s] = (BmShardWalkArg){
                        .db_root = db_root, .object = object, .field = field,
                        .shard_idx = s, .value = valbuf, .vlen = vallen,
                        .cb = cb, .ctx = ctx, .sdb = sdb, .stop_flag = &stop_flag,
                        .error = 0
                    };
                }
                parallel_for_io(bm_shard_walk_worker, args, splits, sizeof(BmShardWalkArg));
                free(valbuf);
                /* Every worker joined: a hard read failure on any shard —
                   even after other shards produced candidates — fails the
                   whole dispatch rather than yielding a partial set. */
                int error = 0;
                for (int s = 0; s < splits; s++)
                    if (args[s].error && !error) error = args[s].error;
                if (error) { free(args); errno = error; return -1; }
            }
            free(args);
            return 0;
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
        if (!gargs) return -1;
        for (int s = 0; s < splits; s++) {
            gargs[s] = (BmGenericShardArg){
                .db_root = db_root, .object = object, .field = field,
                .shard_idx = s, .crit = pc, .tf = tf,
                .cb = cb, .ctx = ctx, .sdb = sdb, .stop_flag = &stop_flag,
                .schema = literal_schema,
                .budget = g_query_buffer_max_bytes / (size_t)(splits > 0 ? splits : 1)
            };
        }
        parallel_for_io(bm_generic_shard_worker, gargs, splits, sizeof(BmGenericShardArg));
        int error = 0;
        for (int i = 0; i < splits; i++) if (gargs[i].error) error = gargs[i].error;
        free(gargs);
        if (error) { errno = error; return -1; }
        return 0;
    }

    /* Stack buffers for encoded key bytes. Max fixed-type output is 8 B;
       varchar caps at f->size - 2 ≤ 65533 — BT_MAX_VAL_LEN = 512 in practice.
       Keep generous for the "contains text + 4 sentinel bytes" suffix cases. */
    uint8_t buf1[1032], buf2[1032];
    size_t len1 = 0, len2 = 0;

    switch (pc->op) {
        case OP_EQUAL:
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            btree_idx_search(db_root, object, field, splits,
                             (const char *)buf1, len1, cb, ctx);
            break;
        case OP_GREATER_EQ:
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            btree_idx_range(db_root, object, field, splits,
                            (const char *)buf1, len1,
                            "\xff\xff\xff\xff", 4, cb, ctx);
            break;
        case OP_GREATER:
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            btree_idx_range_ex(db_root, object, field, splits,
                               (const char *)buf1, len1, 1,
                               "\xff\xff\xff\xff", 4, 0, cb, ctx);
            break;
        case OP_LESS_EQ:
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            btree_idx_range(db_root, object, field, splits,
                            "", 0, (const char *)buf1, len1, cb, ctx);
            break;
        case OP_LESS:
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            btree_idx_range_ex(db_root, object, field, splits,
                               "", 0, 0,
                               (const char *)buf1, len1, 1, cb, ctx);
            break;
        case OP_BETWEEN:
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value2(pc),strlen(search_criterion_value2(pc)),buf2,sizeof(buf2),&len2)<0){return -1;}
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
                if(encode_criterion_value_checked(literal_schema,tf,pc->in_values[iv],strlen(pc->in_values[iv]),buf1,sizeof(buf1),&len1)<0){return -1;}
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
                plen = search_criterion_value_len(pc);
                if(plen>sizeof(buf1)||plen>sizeof(buf2)-4){errno=ENOBUFS;return -1;}
                memcpy(buf1, search_criterion_value(pc), plen);
            } else {
                if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&plen)<0){return -1;}
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
               compile_one already classified this into search_criterion_value(pc)'s % placement;
               we re-derive here because btree_dispatch sees only SearchCriterion.
               Callbacks still apply check_primary; for LK_EXACT/LK_PREFIX that's
               a redundant per-entry confirm but cheap. */
            const char *pat = search_criterion_value(pc);
            size_t pl = strlen(pat);
            int lead = (pl >= 1 && pat[0] == '%');
            int trail = (pl >= 1 && pat[pl-1] == '%');
            int raw_prefix = (!tf || tf->type == FT_VARCHAR);

            if (!lead && !trail && raw_prefix) {
                /* Exact byte match — point lookup. */
                if(encode_criterion_value_checked(literal_schema,tf,pat,pl,buf1,sizeof(buf1),&len1)<0){return -1;}
                btree_idx_search(db_root, object, field, splits,
                                 (const char *)buf1, len1, cb, ctx);
                break;
            }
            if (!lead && trail && raw_prefix) {
                /* "foo%" — strip trailing % and use prefix range scan. */
                size_t needle_len = pl - 1;
                if(needle_len>sizeof(buf1)||needle_len>sizeof(buf2)-4){errno=ENOBUFS;return -1;}
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
                int is_true = scalar_bool_value(search_criterion_value(pc));
                uint8_t inv[1] = { (uint8_t)(is_true ? 0 : 1) };
                btree_idx_search(db_root, object, field, splits,
                                 (const char *)inv, 1, cb, ctx);
                break;
            }
            /* General case: two exclusive ranges covering everything except X. */
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
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
    return 0;
}

/* Lightweight counting callback — no hash storage, just increments counter.
   Used by count mode. For single criterion: pure index counter.
   For multi-criteria: fetches record inline to verify all criteria. */
int btree_dispatch_checked(const char *root,const char *object,const char *field,int splits,
    SearchCriterion *criterion,const TypedField *typed,bt_result_cb callback,void *ctx){
    return btree_dispatch_schema_checked(load_typed_schema(root,object),root,object,field,splits,criterion,typed,callback,ctx);
}
void btree_dispatch(const char *root,const char *object,const char *field,int splits,
    SearchCriterion *criterion,const TypedField *typed,bt_result_cb callback,void *ctx){
    (void)btree_dispatch_checked(root,object,field,splits,criterion,typed,callback,ctx);
}

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
        if (!idx_candidate_admits(ic->tf, val, vlen, ic->primary_crit))
            return 0;
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
    int predicate_error;

    /* Bitmap rejection prefilters (populated by parallel_indexed_count) */
    int n_bm_postfilter;                              // number of bitmap eq/in post-filter leaves
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
                           uint8_t hdr2, void *ctx_ptr) {
    (void)hash; (void)key; (void)klen;
    CountBatchCbCtx *c = (CountBatchCbCtx *)ctx_ptr;
    if (query_deadline_tick(c->sc->deadline, &c->sc->dl_counter)) return 1;
    int matched=0;
    if(criteria_match_tree_checked(value,vlen,c->sc->tree,c->sc->fs,hdr2,&matched)){__atomic_store_n(&c->sc->predicate_error,errno?errno:EILSEQ,__ATOMIC_RELAXED);return 1;}
    if (matched) {
        __atomic_add_fetch(c->local, 1, __ATOMIC_RELAXED);
    }
    return 0;
}

static void *shard_count_worker(void *arg) {
    ShardCountCtx *sc = (ShardCountCtx *)arg;
    if (__atomic_load_n(&sc->predicate_error,__ATOMIC_RELAXED) || sc->entry_count == 0) return NULL;

    /* Pre-open bitmap shards and KF handle for this worker's shard group.
     * All entries in this group share the same hash[0..1] → same data shard. */
    int shard_id = -1;
    SlotcaskDb *sdb SDB_REG_REF = NULL;
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
            if (kfcache_acquire(&kh, kf_p, sdb->slots_per_shard, 0) != 0) {
                slotcask_registry_put(sdb);
                sdb = NULL;  // disable KF probe if acquire fails
            }
        }
        {
            /* Several bitmaps held at once: canonical lock order. If any
               cannot be opened all are left NULL (record-fetch fallback). */
            char bps[MAX_INTERSECT_LEAVES][PATH_MAX];
            const char *bpp[MAX_INTERSECT_LEAVES];
            BitmapShard *opened[MAX_INTERSECT_LEAVES] = {0};
            for (int b = 0; b < sc->n_bm_postfilter; b++) {
                bm_build_path(bps[b], sizeof(bps[b]), sc->db_root, sc->object,
                              sc->bm_criteria[b]->field, shard_id);
                bpp[b] = bps[b];
            }
            if (bm_open_many_ro(bpp, sc->n_bm_postfilter, opened) == 0)
                for (int b = 0; b < sc->n_bm_postfilter; b++) sc->bm_handles[b] = opened[b];
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
            } else {
                /* Bitmaps only reject candidates. Verify the complete tree,
                   including the primary predicate, against the live record. */
                memcpy(resolved[n_need_fetch].hash, sc->entries[ei].hash, 16);
                resolved[n_need_fetch].sid = found_sid;
                resolved[n_need_fetch].fid = found_fid;
                resolved[n_need_fetch].off = found_off;
                n_need_fetch++;
            }
        }

#ifdef TEST_BUILD
        /* Regression seam (docs/plans/2026-08-27-shard-count-worker-
         * nested-kf-read.md Task 1): parks the worker after pass-1's
         * inline KF probe — the probe reader is still held here — so the
         * count-gap test can queue a mutation writer on this shard before
         * the batch fetch re-acquires the same kfcache entry. One-shot:
         * disarmed on first hit so every later count passes through. */
        slotcask_test_count_gap_park(1);
#endif
    }

    /* The pass-1 probe reader must die before the batch fetch: every
     * partition the fetcher builds routes to THIS worker's shard, so
     * kf_reval_fetch_one would re-acquire the same kfcache entry on this
     * thread while we still hold it. The kfcache rwlocks are writer-
     * preferring NONRECURSIVE (shard_db_internal.h) — a recursive reader
     * parked behind a queued mutation writer self-deadlocks (worker waits
     * on acquire #2, the writer waits on acquire #1). */
    if (kh.map) {
        kfcache_release(&kh);
        memset(&kh, 0, sizeof(kh));
    }
    /* The bitmap readers must die with it: pass 2 never reads them, and
     * the batch fetch takes kf readers. Holding a shard's bitmap readers
     * while waiting on its kf lock inverts the kf -> bitmap order bulk
     * insert uses (kf writer, then the window's bitmap writers), and the
     * two deadlock on the same shard. */
    {
        BitmapShard *held[MAX_INTERSECT_LEAVES] = {0};
        for (int b = 0; b < sc->n_bm_postfilter; b++) held[b] = (BitmapShard *)sc->bm_handles[b];
        bm_close_many(held, sc->n_bm_postfilter);
        for (int b = 0; b < sc->n_bm_postfilter; b++) sc->bm_handles[b] = NULL;
    }

#ifdef TEST_BUILD
    /* Regression seam (count/bulk-insert lock order): parks the worker
     * after the probe reader is gone, right before the batch fetch takes
     * kf readers again, so a test can queue a bulk insert on this shard
     * (kf writer, then bitmap writer) inside that window. */
    if (shard_id >= 0) slotcask_test_count_gap_park(2);
#endif

    /* Pass 2: batch fetch all needs-fetch entries */
    if (n_need_fetch > 0) {
        SlotcaskSchemaInfo info = {
            .splits = sc->sch->splits,
            .slot_size = sc->sch->slot_size,
            .streams = sc->sch->streams,
        };
        if (!sdb)
            sdb = slotcask_registry_get(sc->db_root, sc->object, &info);
        if (sdb) {
            CountBatchCbCtx cb_ctx = { sc, &local };
            if (resolved) {
                /* Bitmap path: already have resolved locations, skip KF re-probe */
                slotcask_bulk_fetch_resolved(sdb, resolved,
                                              (size_t)n_need_fetch,
                                              &cb_ctx, count_batch_cb);
            } else {
                /* Non-bitmap path: use combined resolve+fetch */
                slotcask_bulk_resolve_and_fetch(sdb, fetch_hashes,
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
    {
        BitmapShard *held[MAX_INTERSECT_LEAVES] = {0};
        for (int b = 0; b < sc->n_bm_postfilter; b++) held[b] = (BitmapShard *)sc->bm_handles[b];
        bm_close_many(held, sc->n_bm_postfilter);
        for (int b = 0; b < sc->n_bm_postfilter; b++) sc->bm_handles[b] = NULL;
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
    if (!fp || fp->n_postfilter == 0) return;

    int n_bm = 0;
    for (int i = 0; i < fp->n_postfilter; i++) {
        SearchCriterion *c = fp->postfilter_leaves[i];
        if (pick_index_for_leaf(db_root, object, c) != IT_BITMAP) continue;
        if (c->op != OP_EQUAL && c->op != OP_IN) continue;
        if (n_bm >= MAX_INTERSECT_LEAVES) break;
        /* Large dictionary keys remain primary-index eligible; only this
           optional inline postfilter is limited by its existing stack arrays. */
        if (!bitmap_literal_fits(fs->ts,c,sizeof(ctx->bm_val[0]))) continue;

        ctx->bm_criteria[n_bm] = c;
        const TypedField *tf = resolve_idx_field(fs->ts, c->field);
        if (c->op == OP_EQUAL) {
            if(encode_criterion_value_checked(fs->ts,tf,search_criterion_value(c),strlen(search_criterion_value(c)),ctx->bm_val[n_bm],sizeof(ctx->bm_val[n_bm]),&ctx->bm_vlen[n_bm])<0){ctx->predicate_error=errno?errno:EILSEQ;return;}
            ctx->bm_in_count[n_bm] = 0;
            n_bm++;
        } else if (c->in_count <= 8) {
            /* OP_IN: encode each value */
            int nc = c->in_count;
            ctx->bm_in_count[n_bm] = nc;
            for (int v = 0; v < nc; v++) {
                if(encode_criterion_value_checked(fs->ts,tf,c->in_values[v],strlen(c->in_values[v]),ctx->bm_in_vals[n_bm][v],sizeof(ctx->bm_in_vals[n_bm][v]),&ctx->bm_in_vlens[n_bm][v])<0){ctx->predicate_error=errno?errno:EILSEQ;return;}
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
}

/* Orchestrate parallel indexed count: qsort by shard, fan out per-shard workers. */
static size_t parallel_indexed_count(const char *db_root, const char *object,
                                     const Schema *sch, CollectedHash *batch,
                                     int batch_count, CriteriaNode *tree,
                                     FieldSchema *fs, QueryDeadline *dl,
                                     const FilterPlan *fp) {
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
    }

    if (batch_count < 1024 || nshard_groups <= 2) {
        for (int g = 0; g < nshard_groups; g++) shard_count_worker(&workers[g]);
    } else {
        parallel_for_io(shard_count_worker, workers, nshard_groups, sizeof(ShardCountCtx));
    }

    size_t total = 0;
    int error=0;
    for(int g=0;g<nshard_groups;g++){total+=workers[g].count;int pe=__atomic_load_n(&workers[g].predicate_error,__ATOMIC_RELAXED);if(pe)error=pe;}
    free(workers);
    if(error){errno=error;return SIZE_MAX;}
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
    /* Index entries are candidates: record verification, exclusions and
       joins can still reject them. Apply offset/limit only after matching
       in keyset_emit_find; the collector's query-memory cap still applies. */
    CollectCtx cc;
    collect_ctx_init(&cc);
    cc.splits = sch->splits;
    cc.collect_cap = 0;
    cc.primary_crit = primary_crit;
    cc.check_primary = check_primary;
    cc.deadline = dl;
    cc.tf = resolve_idx_field(fs ? fs->ts : NULL, primary_crit->field);

    (void)primary_idx_path; /* path now derived per-shard inside btree_idx_*; arg kept for API stability */
    if(btree_dispatch_schema_checked(fs?fs->ts:NULL,db_root, object, primary_crit->field, sch->splits,
                   primary_crit,
                   resolve_idx_field(fs ? fs->ts : NULL, primary_crit->field),
                   collect_hash_cb, &cc)<0){collect_ctx_destroy(&cc);return -1;}

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
                     uint8_t hdr2, void *ctx);
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
    if (!b->pending) {
        LOG_ERROR(LOG_SUB_QUERY, "batch_buf_init: calloc failed for pending_cap=%zu", b->pending_cap);
        return -1;
    }
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

#ifdef TEST_BUILD
/* Find-flush gate (test-control kind 2): parks the caller immediately
   before a blocking batch fetch. Inert until armed via the test-control
   channel; first caller parks, peers proceed. The atomics are defined
   here — the TU that polls them — mirroring how the count-gap atomics
   live beside slotcask_test_count_gap_park in slotcask.c. */
_Atomic int g_shard_test_find_flush_gate;
_Atomic int g_shard_test_find_flush_gate_hit;
static shard_db_test_gate_fn g_shard_test_find_flush_gate_fn;
static void *g_shard_test_find_flush_gate_ctx;

void shard_db_test_set_find_flush_gate_hook(shard_db_test_gate_fn fn,
                                            void *ctx) {
    g_shard_test_find_flush_gate_fn = fn;
    g_shard_test_find_flush_gate_ctx = ctx;
}

/* Called inside batch_buf_flush_copy on the legacy collector (kf+bitmap
   handles still held when the bitmap walk drove the flush), inside
   bm_defer_drain_if_armed (no handles held), and before each record fetch
   of the prepared small-limit bitmap iterator (no handles held: candidates
   were prepared and every bitmap handle released before emission). */
static void bm_defer_gate_park(void) {
    if (!atomic_load(&g_shard_test_find_flush_gate)) return;
    int expected = 0;
    if (!atomic_compare_exchange_strong(&g_shard_test_find_flush_gate_hit,
                                        &expected, 1))
        return;                       /* another worker already parked */
    if (g_shard_test_find_flush_gate_fn)
        g_shard_test_find_flush_gate_fn(g_shard_test_find_flush_gate_ctx);
}
#endif

static void batch_buf_flush_copy(BatchFetchBuf *b) {
    size_t n;
    uint8_t (*copy)[16] = NULL;

#ifdef TEST_BUILD
    bm_defer_gate_park();
#endif
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
   Chunked-resumable btree walkers (kf-lock-safe indexed fetch).

   A thread holding a btree file's bt_cache rdlock must never block on a
   kf-shard lock: writers hold the kf WRLOCK through the durability
   window's phase I, which itself takes the bt WRLOCK (docs/plans/
   2026-08-27-bt-kf-lock-inversion-chunked-fetch.md). So these walkers
   give every fan-out worker a PRIVATE BtRangeIter and let it drain its
   own bounded hash batch before any blocking fetch:

     open iter -> pull up to cap entries into private batch
       -> CLOSE iter (releases this shard's bt rdlock)
       -> blocking slotcask_bulk_resolve_and_fetch(batch)
       -> reopen past the last delivered (value,hash16), repeat

   Resume bounds follow the repo-wide inclusive-bound + raw-hash16
   tiebreak skip convention (leaf order is total on (value bytes,
   hash16) — same convention as cursor pagination's
   cursor_find_cb skip and btree_walk_ordered_ranges' OrderedRangeResume).

   Ownership rules: each worker touches only its own iterator, so no
   cross-thread close/release protocol is needed. Memory is bounded per
   worker by `cap` (caller derives it from QUERY_BUFFER_MB / limit /
   shard count), replacing the shared-BatchFetchBuf condvar handoff on
   these executors.
   ============================================================ */
typedef int (*chunk_filter_cb)(const char *val, size_t vlen, void *ctx);

/* State shared by every per-shard worker of ONE dispatch round. */
typedef struct {
    SlotcaskDb    *sdb;
    size_t         cap;            /* per-worker batch cap */
    volatile int  *stop;           /* may be NULL */
    SlotcaskScanCb record_cb;      /* runs OUTSIDE any bt lock */
    void          *record_ctx;
    chunk_filter_cb filter;        /* NULL = every entry passes */
    void          *fctx;
} ChunkShared;

typedef struct {
    ChunkShared *sh;
    char         path[PATH_MAX];
    char         lo[BT_MAX_VAL_LEN]; size_t lo_len; int lo_excl;
    char         hi[BT_MAX_VAL_LEN]; size_t hi_len; int hi_excl;
} ChunkWalkArg;

static void *chunk_walk_worker(void *raw) {
    ChunkWalkArg *w = (ChunkWalkArg *)raw;
    ChunkShared *sh = w->sh;

    uint8_t (*batch)[16] = malloc(sh->cap * 16);
    if (!batch) return NULL;
    size_t bn = 0;

    char last_val[BT_MAX_VAL_LEN];
    size_t last_len = 0;
    uint8_t last_hash[BT_HASH_SIZE];
    int have_last = 0;

    for (;;) {
        if (sh->stop && __atomic_load_n(sh->stop, __ATOMIC_ACQUIRE)) break;

        /* Reopen bounds: original lower bound, or inclusive-at-last
           delivered entry once a resume is in progress. */
        const char *lo; size_t lo_len; int lo_excl;
        char lo_buf[BT_MAX_VAL_LEN];
        if (have_last) {
            memcpy(lo_buf, last_val, last_len);
            lo = lo_buf; lo_len = last_len; lo_excl = 0;
        } else {
            lo = w->lo; lo_len = w->lo_len; lo_excl = w->lo_excl;
        }

        BtRangeIter *it = btree_range_iter_open(w->path,
                                                lo, lo_len, lo_excl,
                                                w->hi, w->hi_len, w->hi_excl,
                                                0 /* ASC */);
        if (!it) break;

        int exhausted = 0;
        while (bn < sh->cap) {
            const char *v; size_t vl; const uint8_t *h;
            if (!btree_range_iter_next(it, &v, &vl, &h)) {
                exhausted = 1;
                break;
            }
            if (sh->stop &&
                __atomic_load_n(sh->stop, __ATOMIC_ACQUIRE))
                break;

            /* Resume floor: skip the already-delivered boundary entry and
               any older sibling sharing its value. */
            /* last_hash is written (memcpy at the resume-point update below)
               strictly before have_last is set, and this read is short-
               circuited behind have_last — the variable can never be read
               uninitialized. cppcheck cannot track the flag-to-init
               dependency; suppressed with the user's sign-off (2026-11-13). */
            if (have_last && vl == last_len &&
                memcmp(v, last_val, last_len) == 0 &&
                // cppcheck-suppress uninitvar
                memcmp(h, last_hash, BT_HASH_SIZE) <= 0)
                continue;

            /* Record the resume point BEFORE the (possibly later) close. */
            last_len = vl > BT_MAX_VAL_LEN ? BT_MAX_VAL_LEN : vl;
            memcpy(last_val, v, last_len);
            memcpy(last_hash, h, BT_HASH_SIZE);
            have_last = 1;

            if (sh->filter && !sh->filter(v, vl, sh->fctx)) continue;

            memcpy(batch[bn], h, 16);
            bn++;
        }
        btree_range_iter_close(it);

        if (bn > 0) {
            /* No bt lock is held here — blocking on kfcache is safe. */
            slotcask_bulk_resolve_and_fetch(sh->sdb, batch, bn,
                                            sh->record_ctx, sh->record_cb);
            bn = 0;
        }
        if (exhausted) break;
    }

    free(batch);
    return NULL;
}

/* Fan one range spec out across all idx shards. Blocking record fetches
 * happen exclusively inside chunk_walk_worker with the worker's own
 * iterator closed. Returns after every worker drained or stopped. */
static void chunk_walk_fanout(const ChunkShared *sh_tmpl,
                              const char *db_root, const char *object,
                              const char *field, int splits,
                              const char *lo, size_t lo_len, int lo_excl,
                              const char *hi, size_t hi_len, int hi_excl) {
    int n = index_splits_for(splits);
    if (n <= 0) return;
    ChunkShared sh = *sh_tmpl;
    ChunkWalkArg *args = calloc((size_t)n, sizeof(ChunkWalkArg));
    if (!args) return;
    /* §2.4.2: every fanout caller passes RAW payload bounds; stored BTRI
       keys are [0x00][payload]. Prefix the 0x00 tier byte once here. The
       unbounded-above sentinel (\xff*4) becomes [0x00][\xff*4] — the
       5-byte form btree.c's desc-iterator accepts, and it clamps below
       the NULL tier automatically. Resume bounds inside chunk_walk_worker
       reuse stored keys (already tiered) and bypass this path. */
    /* A nonnull raw zero-length bound is the present empty payload and
       becomes the one-byte value-tier key. NULL alone means unbounded. */
    char lo_tiered[BT_MAX_VAL_LEN], hi_tiered[BT_MAX_VAL_LEN];
    size_t lo_t = 0, hi_t = 0;
    if (lo) {
        lo_t = (lo_len + 1 > sizeof(lo_tiered)) ? sizeof(lo_tiered) : lo_len + 1;
        lo_tiered[0] = 0x00;
        memcpy(lo_tiered + 1, lo, lo_t - 1);
    }
    if (hi) {
        hi_t = (hi_len + 1 > sizeof(hi_tiered)) ? sizeof(hi_tiered) : hi_len + 1;
        hi_tiered[0] = 0x00;
        memcpy(hi_tiered + 1, hi, hi_t - 1);
    }
    for (int s = 0; s < n; s++) {
        ChunkWalkArg *a = &args[s];
        a->sh = &sh;
        build_idx_path(a->path, sizeof(a->path), db_root, object, field, s);
        memcpy(a->lo, lo_tiered, lo_t); a->lo_len = lo_t; a->lo_excl = lo_excl;
        memcpy(a->hi, hi_tiered, hi_t); a->hi_len = hi_t; a->hi_excl = hi_excl;
    }
    parallel_for_io(chunk_walk_worker, args, n, sizeof(ChunkWalkArg));
    free(args);
}

/* Per-worker batch cap: derived from the caller's pending_cap budget so
 * total worker memory stays comparable to the old shared buffer, floored
 * to keep reopen amortization sane. */
static size_t chunk_cap_for(size_t pending_cap, int nworkers) {
    size_t c = pending_cap / (size_t)(nworkers > 0 ? nworkers : 1);
    if (c < 64) c = 64;
    if (c > 4096) c = 4096;
    return c;
}

/* Upper-bound sentinel used across btree_dispatch's range forms. */
static const char CHUNK_HI_SENTINEL[4] = { (char)0xff, (char)0xff,
                                           (char)0xff, (char)0xff };

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
    int predicate_error;
    int               stop;       /* atomic — set when printed >= limit */

    /* Deferred-collector budget (bitmap-routed fallback): per-query cap
       on candidate hashes held across worker walks, QUERY_BUFFER_MB
       class. defer_total is manipulated via __atomic builtins. */
    size_t            defer_total;
    size_t            defer_budget;
} StreamFindCtx;

static int stream_find_record_cb(const uint8_t hash16[16],
                                  const void *key, size_t klen,
                                  const void *value, size_t vlen,
                                  uint8_t hdr2, void *ctx) {
    (void)hash16;
    StreamFindCtx *sc = (StreamFindCtx *)ctx;
    g_out = sc->parent_out;

    char keybuf[1100];
    {
        const Schema *sc_p = (sc->fs && sc->fs->auto_key != AK_NONE)
                              ? &sc->fs->auto_key_schema_snapshot : NULL;
        format_wire_key(sc_p, (const char *)key, klen, keybuf, sizeof(keybuf));
    }

    if (is_excluded(sc->excluded, keybuf)) return 0;

    int matched=0;
    if(criteria_match_tree_checked(value,vlen,sc->tree,sc->fs,hdr2,&matched)){
        pthread_mutex_lock(&sc->lock);sc->predicate_error=errno?errno:EILSEQ;pthread_mutex_unlock(&sc->lock);return 1;}
    if(!matched)return 0;

    pthread_mutex_lock(&sc->lock);
    int my_seq = ++sc->passed;
    int will_emit = (my_seq > sc->offset &&
                     (sc->limit <= 0 || sc->printed < sc->limit));
    if(will_emit){
        if(typed_query_emit_row(g_out?g_out:stdout,keybuf,strlen(keybuf),value,vlen,hdr2,sc->fs,
            sc->proj_fields,sc->proj_count,sc->rows_fmt,sc->dict_fmt,sc->csv_delim,sc->printed>0,
            NULL,0,NULL,g_query_buffer_max_bytes)){
            sc->predicate_error=errno?errno:EILSEQ;
            pthread_mutex_unlock(&sc->lock);return 1;
        }
        sc->printed++;
    }
    int done = (sc->limit > 0 && sc->printed >= sc->limit);
    if (done) __atomic_store_n(&sc->stop, 1, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&sc->lock);
    return done ? -1 : 0;
}

/* Primary-leaf match shared by stream_find_cb and stream_find_defer_cb
   so both collectors agree on which entries become fetch candidates. */
static int stream_primary_check(StreamFindCtx *sc,
                                const char *val, size_t vlen) {
    if (sc->primary_crit && op_is_length(sc->primary_crit->op)) {
        if (!match_length_vlen(vlen, sc->primary_crit)) return 0;
    } else if (sc->check_primary && sc->primary_crit) {
        if (!idx_candidate_admits(sc->tf, val, vlen, sc->primary_crit))
            return 0;
    }
    return 1;
}

static int stream_find_cb(const char *val, size_t vlen, const uint8_t *hash16, void *raw_ctx) {
    StreamFindCtx *sc = (StreamFindCtx *)raw_ctx;
    if (__atomic_load_n(&sc->stop, __ATOMIC_ACQUIRE)) return -1;

    BatchFetchBuf *bfb = &sc->bfb;
    g_out = sc->parent_out;

    if (!stream_primary_check(sc, val, vlen)) return 0;

    return batch_buf_collect_hash(bfb, hash16);
}

/* Deferred collector for the bitmap-routed fallback. The bitmap
   emitters hold the shard's kf reader + bitmap handle across the walk
   (kfcache-before-bitmap order), so an inline flush here would nest a
   second kf read acquire under the held one — a
   PREFER_WRITER_NONRECURSIVE self-deadlock behind any queued window
   writer. Instead each walk thread appends to its OWN thread-local
   batch and drains it with ONE blocking bulk fetch after
   bitmap_emit_*_for_shard returned and every handle is released. The
   bitmap walk has no resume primitive, so a single post-walk fetch per
   worker (not mid-walk flushing) is the correct shape; growth is
   bounded by the per-query budget in sc->defer_budget. */
typedef struct {
    uint8_t        (*hashes)[16];
    size_t         n;
    size_t         cap;
    StreamFindCtx *sc;
} BmDeferBatch;

static __thread BmDeferBatch tl_bm_defer;

static int stream_find_defer_cb(const char *val, size_t vlen,
                                const uint8_t *hash16, void *raw_ctx) {
    StreamFindCtx *sc = (StreamFindCtx *)raw_ctx;
    if (__atomic_load_n(&sc->stop, __ATOMIC_ACQUIRE)) return -1;
    g_out = sc->parent_out;
    if (!stream_primary_check(sc, val, vlen)) return 0;

    BmDeferBatch *t = &tl_bm_defer;
    if (t->cap == 0) {
        t->cap = 64;
        t->hashes = malloc(t->cap * 16);
        if (!t->hashes) {
            __atomic_store_n(&sc->stop, 1, __ATOMIC_RELEASE);
            return -1;
        }
        t->sc = sc;
    }
    if ((size_t)__atomic_add_fetch(&sc->defer_total, 1,
                                   __ATOMIC_RELAXED) > sc->defer_budget) {
        /* Per-query candidate budget exhausted (QUERY_BUFFER_MB class).
           Stop collecting walk-wide; what was collected still drains —
           the streaming deadline's partial-result semantics, never a
           silent drop. */
        LOG_WARN(LOG_SUB_QUERY,
                 "bitmap deferred batch budget exceeded; find returns "
                 "partial results");
        __atomic_store_n(&sc->stop, 1, __ATOMIC_RELEASE);
        return -1;
    }
    if (t->n == t->cap) {
        size_t ncap = t->cap * 2;
        uint8_t (*nh)[16] = realloc(t->hashes, ncap * 16);
        if (!nh) {
            __atomic_store_n(&sc->stop, 1, __ATOMIC_RELEASE);
            return -1;
        }
        t->hashes = nh;
        t->cap = ncap;
    }
    memcpy(t->hashes[t->n], hash16, 16);
    t->n++;
    return 0;
}

/* Worker-side drain. No-op for every cb other than the deferred
   collector — all pre-existing btree_dispatch callers are byte-
   identical. Runs after bitmap_emit_*_for_shard returned: this shard's
   kf reader and bitmap handle are released, so the blocking fetch
   nests nothing (the 2026-08-27 chunked-executor invariant, adapted to
   the no-resume bitmap walk). */
static void bm_defer_drain_if_armed(bt_result_cb cb, SlotcaskDb *sdb) {
    BmDeferBatch *t = &tl_bm_defer;
    if (cb != stream_find_defer_cb || t->n == 0 || !t->sc) return;
    StreamFindCtx *sc = t->sc;
#ifdef TEST_BUILD
    bm_defer_gate_park();   /* parks with NO handles held on this build */
#endif
    slotcask_bulk_resolve_and_fetch(sdb, t->hashes, t->n,
                                    sc->bfb.record_ctx, sc->bfb.record_cb);
    free(t->hashes);
    t->hashes = NULL; t->n = 0; t->cap = 0; t->sc = NULL;
}

/* Primary-leaf match for the chunked streaming executor — mirrors the
 * primary-check half of stream_find_cb so both executors agree on which
 * entries become fetch candidates. Walked entries are stored BTRI keys:
 * strip the tier byte and skip NULL-tier entries (§2.4.2, value-tier
 * candidate production — same rule as index.c btri_value_tier_cb). */
static int stream_primary_filter(const char *val, size_t vlen, void *raw_ctx) {
    StreamFindCtx *sc = (StreamFindCtx *)raw_ctx;
    if (vlen == 0 || (uint8_t)val[0] != 0x00) return 0;
    val++; vlen--;
    if (!sc->primary_crit || !sc->check_primary) return 1;
    if (op_is_length(sc->primary_crit->op))
        return match_length_vlen(vlen, sc->primary_crit) ? 1 : 0;
    return idx_candidate_admits(sc->tf, val, vlen, sc->primary_crit) ? 1 : 0;
}

/* Chunked-resumable driver for btree-backed primary leaves. Replicates the
 * lock-holding branches of btree_dispatch (query.c OP_EQUAL..OP_NOT_EQUAL)
 * as resume-capable range specs; ops whose dispatch route holds no bt_cache
 * lock (bitmap fast/dict paths, O_DIRECT default leaf scans) must fall back
 * to btree_dispatch + stream_find_cb, whose inline batch flush is safe on
 * those no-handle branches. Bitmap branches are NOT safe for inline flush
 * (they hold kf+bitmap handles across the emit) and get the deferred
 * collector instead — see idx_find_streaming. Returns 1 when the op was
 * driven here, 0 when the caller must use the legacy path. */
static int chunked_stream_dispatch(const TypedSchema *literal_schema,ChunkShared *sh,
                                   const char *db_root, const char *object,
                                   const char *field, int splits,
                                   SearchCriterion *pc, const TypedField *tf) {

    uint8_t buf1[1032], buf2[1032];
    size_t len1 = 0, len2 = 0;

#define CHUNK_GO(LO, LLO, LLEX, HI, HLI, HIEX) \
    do { \
        chunk_walk_fanout(sh, db_root, object, field, splits, \
                          (const char *)(LO), (LLO), (LLEX), \
                          (const char *)(HI), (HLI), (HIEX)); \
        return 1; \
    } while (0)

    switch (pc->op) {
        case OP_EQUAL:
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            if (len1 == 0) return 0;
            memcpy(buf2, buf1, len1);
            memset(buf2 + len1, 0xff, 4);
            CHUNK_GO(buf1, len1, 0, buf2, len1 + 4, 0);
        case OP_GREATER_EQ:
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            if (len1 == 0) return 0;
            CHUNK_GO(buf1, len1, 0, CHUNK_HI_SENTINEL, 4, 0);
        case OP_GREATER:
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            if (len1 == 0) return 0;
            CHUNK_GO(buf1, len1, 1, CHUNK_HI_SENTINEL, 4, 0);
        case OP_LESS_EQ:
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            if (len1 == 0) return 0;
            CHUNK_GO("", 0, 0, buf1, len1, 0);
        case OP_LESS:
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            if (len1 == 0) return 0;
            CHUNK_GO("", 0, 0, buf1, len1, 1);
        case OP_BETWEEN:
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value2(pc),strlen(search_criterion_value2(pc)),buf2,sizeof(buf2),&len2)<0){return -1;}
            if (len1 == 0 || len2 == 0) return 0;
            CHUNK_GO(buf1, len1, pc->min_exclusive,
                     buf2, len2, pc->max_exclusive);
        case OP_IN:
            for (int iv = 0; iv < pc->in_count; iv++) {
                int present=encode_criterion_value_checked(literal_schema,tf,
                    pc->in_values[iv],strlen(pc->in_values[iv]),buf1,sizeof(buf1),&len1);
                if(present<0)return -1;
                if(present==0)continue;
                if(len1==0){
                    /* Present empty varchar has the exact stored key [0x00].
                       Keep the resumable worker path: no B-tree-held fetch. */
                    chunk_walk_fanout(sh,db_root,object,field,splits,
                                      "",0,0,"",0,0);
                    if(sh->stop&&__atomic_load_n(sh->stop,__ATOMIC_ACQUIRE))return 1;
                    continue;
                }
                memcpy(buf2, buf1, len1);
                memset(buf2 + len1, 0xff, 4);
                if (iv == pc->in_count - 1)
                    CHUNK_GO(buf1, len1, 0, buf2, len1 + 4, 0);
                else
                    chunk_walk_fanout(sh, db_root, object, field, splits,
                                      (const char *)buf1, len1, 0,
                                      (const char *)buf2, len1 + 4, 0);
                if (sh->stop &&
                    __atomic_load_n(sh->stop, __ATOMIC_ACQUIRE)) return 1;
            }
            return 1;
        case OP_STARTS_WITH: {
            int raw_prefix = (!tf || tf->type == FT_VARCHAR);
            size_t plen;
            if (raw_prefix) {
                plen = search_criterion_value_len(pc);
                if(plen>sizeof(buf1)||plen>sizeof(buf2)-4){errno=ENOBUFS;return -1;}
                memcpy(buf1, search_criterion_value(pc), plen);
            } else {
                if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&plen)<0){return -1;}
            }
            if (plen == 0) return 0;
            memcpy(buf2, buf1, plen);
            memset(buf2 + plen, 0xff, 4);
            CHUNK_GO(buf1, plen, 0, buf2, plen + 4, 0);
        }
        case OP_LIKE: {
            const char *pat = search_criterion_value(pc);
            size_t pl = strlen(pat);
            int lead = (pl >= 1 && pat[0] == '%');
            int trail = (pl >= 1 && pat[pl-1] == '%');
            int raw_prefix = (!tf || tf->type == FT_VARCHAR);
            if (!lead && !trail && raw_prefix) {
                if(encode_criterion_value_checked(literal_schema,tf,pat,pl,buf1,sizeof(buf1),&len1)<0){return -1;}
                if (len1 == 0) return 0;
                memcpy(buf2, buf1, len1);
                memset(buf2 + len1, 0xff, 4);
                CHUNK_GO(buf1, len1, 0, buf2, len1 + 4, 0);
            }
            if (!lead && trail && raw_prefix) {
                size_t needle_len = pl - 1;
                if(needle_len>sizeof(buf1)||needle_len>sizeof(buf2)-4){errno=ENOBUFS;return -1;}
                memcpy(buf1, pat, needle_len);
                memcpy(buf2, buf1, needle_len);
                memset(buf2 + needle_len, 0xff, 4);
                CHUNK_GO(buf1, needle_len, 0, buf2, needle_len + 4, 0);
            }
            return 0;   /* substring/suffix/non-varchar → O_DIRECT route */
        }
        case OP_NOT_EQUAL:
            if (tf && tf->type == FT_BOOL) {
                int is_true = scalar_bool_value(search_criterion_value(pc));
                uint8_t inv[1] = { (uint8_t)(is_true ? 0 : 1) };
                memcpy(buf2, inv, 1);
                memset(buf2 + 1, 0xff, 4);
                CHUNK_GO(inv, 1, 0, buf2, 5, 0);
            }
            if(encode_criterion_value_checked(literal_schema,tf,search_criterion_value(pc),strlen(search_criterion_value(pc)),buf1,sizeof(buf1),&len1)<0){return -1;}
            if (len1 == 0) return 0;
            chunk_walk_fanout(sh, db_root, object, field, splits,
                              "", 0, 0, (const char *)buf1, len1, 1);
            if (sh->stop &&
                __atomic_load_n(sh->stop, __ATOMIC_ACQUIRE)) return 1;
            CHUNK_GO(buf1, len1, 1, CHUNK_HI_SENTINEL, 4, 0);
        default:
            return 0;
    }
#undef CHUNK_GO
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
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &sinfo);

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
    sc.defer_budget = g_query_buffer_max_bytes / 16;

    if (batch_buf_init(&sc.bfb, sdb, sch->slot_size, limit) != 0) {
        pthread_mutex_destroy(&sc.lock);
        return 0;
    }
    sc.bfb.record_cb = stream_find_record_cb;
    sc.bfb.record_ctx = &sc;

    if (sdb && field_index_type(db_root, object, primary_crit->field)
               == IT_BTREE) {
        /* Lock-safe executor: per-worker chunked walkers close their own
         * bt iterator before every blocking kf fetch (AB-BA fix,
         * docs/plans/2026-08-27-bt-kf-lock-inversion-chunked-fetch.md).
         * Falls through to legacy btree_dispatch when the op routes to a
         * no-bt-lock branch anyway. */
        ChunkShared sh = {0};
        sh.sdb = sdb;
        sh.stop = &sc.stop;
        sh.record_cb = stream_find_record_cb;
        sh.record_ctx = &sc;
        sh.filter = stream_primary_filter;
        sh.fctx = &sc;
        sh.cap = chunk_cap_for(sc.bfb.pending_cap,
                               index_splits_for(sch->splits));
        int driven = chunked_stream_dispatch(fs?fs->ts:NULL,&sh, db_root, object,
                                             primary_crit->field,
                                             sch->splits, primary_crit,
                                             sc.tf);
        if(driven<0)sc.predicate_error=errno?errno:EILSEQ;
        else if (!driven) {
            if(btree_dispatch_schema_checked(fs?fs->ts:NULL,db_root, object, primary_crit->field, sch->splits,
                            primary_crit, sc.tf, stream_find_cb, &sc)<0){sc.predicate_error=errno?errno:EILSEQ;}
        }
    } else {
        /* IT_BITMAP branches hold kf(S)+bitmap(S) across the emit, so the
         * collector must never flush inline: the resolver's per-shard
         * kfcache acquires would nest a second kf(S) read acquire under
         * the held one — PREFER_WRITER_NONRECURSIVE self-deadlock behind
         * any queued window writer. The deferred collector appends to a
         * per-thread batch only; each worker fetches after its walk
         * releases the handles. O_DIRECT leaf-scan branches (btree
         * primaries the chunked dispatcher didn't drive) hold no handles
         * and keep the inline-flush collector on the !driven path. */
        if(btree_dispatch_schema_checked(fs?fs->ts:NULL,db_root, object, primary_crit->field, sch->splits,
                        primary_crit,
                        resolve_idx_field(fs ? fs->ts : NULL, primary_crit->field),
                        stream_find_defer_cb, &sc)<0){sc.predicate_error=errno?errno:EILSEQ;}
    }

    batch_buf_destroy(&sc.bfb);
    pthread_mutex_destroy(&sc.lock);
    if(sc.predicate_error){errno=sc.predicate_error;return -1;}
    return sc.printed;
}

/* ========== Prepared small-limit bitmap find iterator ==========
   Serves the former small-limit streaming shapes (no joins, no rows
   envelope, no ordering, offset+limit <= 1000) after their bitmap-dependent
   candidates were prepared before the response envelope. Walks the prepared
   KeySet and fetches one record at a time: the deadline is checked before
   every fetch, at most one RecordRef is live, and iteration stops as soon as
   the requested rows are emitted, the deadline expires, or a hard error is
   recorded. Row matching, exclusions and pagination are exactly
   stream_find_record_cb's. Not a batching optimization. */

#ifdef TEST_BUILD
/* Request-thread observer: armed on the thread that runs the find, it
   counts prepared-iterator invocations, record fetches and live RecordRefs
   (current and peak). */
static __thread struct {
    int      armed;
    unsigned calls, fetches, live, peak_live;
} prep_stream_obs;

void query_test_prepared_stream_observe(int on) {
    memset(&prep_stream_obs, 0, sizeof(prep_stream_obs));
    prep_stream_obs.armed = on;
}

void query_test_prepared_stream_stats(unsigned *calls, unsigned *fetches,
                                      unsigned *live, unsigned *peak_live) {
    if (calls) *calls = prep_stream_obs.calls;
    if (fetches) *fetches = prep_stream_obs.fetches;
    if (live) *live = prep_stream_obs.live;
    if (peak_live) *peak_live = prep_stream_obs.peak_live;
}
#define PREP_OBS_CALL()  do { if (prep_stream_obs.armed) prep_stream_obs.calls++; } while (0)
#define PREP_OBS_ACQUIRE() do { if (prep_stream_obs.armed) { \
        prep_stream_obs.fetches++; prep_stream_obs.live++; \
        if (prep_stream_obs.live > prep_stream_obs.peak_live) \
            prep_stream_obs.peak_live = prep_stream_obs.live; } } while (0)
#define PREP_OBS_RELEASE() do { if (prep_stream_obs.armed && prep_stream_obs.live) \
        prep_stream_obs.live--; } while (0)
#else
#define PREP_OBS_CALL()    ((void)0)
#define PREP_OBS_ACQUIRE() ((void)0)
#define PREP_OBS_RELEASE() ((void)0)
#endif

/* Unamortized deadline check (query_deadline_tick samples every 1024). */
static int prepared_deadline_expired(QueryDeadline *dl) {
    if (!dl || dl->timeout_ms == 0) return 0;
    if (atomic_load_explicit(&dl->timed_out, memory_order_relaxed)) return 1;
    if (now_ms_coarse() - dl->t0_ms > (uint64_t)dl->timeout_ms) {
        atomic_store_explicit(&dl->timed_out, 1, memory_order_relaxed);
        return 1;
    }
    return 0;
}

typedef struct {
    StreamFindCtx *sc;
    const char    *db_root;
    const char    *object;
    const Schema  *sch;
    QueryDeadline *dl;
} PreparedStreamIter;

static int prepared_stream_iter_cb(const uint8_t hash[16], void *raw) {
    PreparedStreamIter *it = (PreparedStreamIter *)raw;
    StreamFindCtx *sc = it->sc;
    if (__atomic_load_n(&sc->stop, __ATOMIC_ACQUIRE) || sc->predicate_error)
        return 1;
    if (prepared_deadline_expired(it->dl)) return 1;
#ifdef TEST_BUILD
    bm_defer_gate_park();   /* parks before a fetch with NO handles held */
#endif
    RecordRef ref;
    PREP_OBS_ACQUIRE();
    int rr = read_record_ref(it->db_root, it->object, it->sch, hash, &ref);
    int rc = 0;
    /* A candidate whose record is gone keeps the existing validation
       semantics: it simply does not match. */
    if (rr == 0)
        rc = stream_find_record_cb(hash, ref.key, ref.klen, ref.val, ref.vlen,
                                   ref.hdr2, sc);
    release_record_ref(&ref);
    PREP_OBS_RELEASE();
    return rc != 0 ? 1 : 0;
}

static int prepared_bitmap_find_stream(const char *db_root, const char *object,
                                       const Schema *sch, KeySet *ks,
                                       CriteriaNode *tree,
                                       ExcludedKeys *excluded,
                                       int offset, int limit,
                                       const char **proj_fields, int proj_count,
                                       FieldSchema *fs,
                                       int rows_fmt, int dict_fmt, char csv_delim,
                                       QueryDeadline *dl) {
    PREP_OBS_CALL();
    StreamFindCtx sc;
    memset(&sc, 0, sizeof(sc));
    sc.db_root = db_root;
    sc.object = object;
    sc.sch = sch;
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
    int mrc = pthread_mutex_init(&sc.lock, NULL);
    if (mrc != 0) { errno = mrc; return -1; }
    PreparedStreamIter it = { &sc, db_root, object, sch, dl };
    if (ks) keyset_iter(ks, prepared_stream_iter_cb, &it);
    pthread_mutex_destroy(&sc.lock);
    if (sc.predicate_error) { errno = sc.predicate_error; return -1; }
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

static void format_query_wire_key(const FieldSchema *fs, const uint8_t *key,
                                  size_t klen, char out[1100]) {
    const Schema *sc = (fs && fs->auto_key != AK_NONE)
                     ? &fs->auto_key_schema_snapshot : NULL;
    format_wire_key(sc, (const char *)key, klen, out, 1100);
}

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
    int predicate_error;
} CompositePrefixCtx;

#ifdef TEST_BUILD
typedef void (*order_walk_test_pause_fn)(void *ctx);
static order_walk_test_pause_fn g_order_walk_pause_fn = NULL;
static void *g_order_walk_pause_ctx = NULL;
static size_t g_order_walk_pause_skip = 0;

void order_walk_test_set_pause_hook_after(order_walk_test_pause_fn fn,
                                          void *ctx,
                                          size_t callbacks_to_skip) {
    g_order_walk_pause_fn = fn;
    g_order_walk_pause_ctx = ctx;
    g_order_walk_pause_skip = callbacks_to_skip;
}

void order_walk_test_set_pause_hook(order_walk_test_pause_fn fn, void *ctx) {
    order_walk_test_set_pause_hook_after(fn, ctx, 0);
}

static void order_walk_test_maybe_pause(void) {
    if (!g_order_walk_pause_fn) return;
    if (g_order_walk_pause_skip > 0) {
        g_order_walk_pause_skip--;
        return;
    }
    order_walk_test_pause_fn fn = g_order_walk_pause_fn;
    void *ctx = g_order_walk_pause_ctx;
    g_order_walk_pause_fn = NULL;
    g_order_walk_pause_ctx = NULL;
    fn(ctx);
}
#endif

static int composite_prefix_cb(const char *val, size_t vlen,
                                const uint8_t *hash16,
                                BtOrderedWalkHandle *wh, void *ctx) {
#ifdef TEST_BUILD
    extern long g_order_walk_scanned;
    g_order_walk_scanned++;
#endif
    (void)val; (void)vlen;   /* composite leaf value not needed; hash16 is the key */
    CompositePrefixCtx *c = (CompositePrefixCtx *)ctx;
    g_out = c->parent_out;
    if (query_deadline_tick(c->dl, &c->dl_counter)) return -1;
    if (c->printed >= c->limit) return -1;

#ifdef TEST_BUILD
    order_walk_test_maybe_pause();
#endif

    /* Fetch the record by hash16. */
    RecordRef rr;
    int rc = read_record_ref_try(c->db_root, c->object, c->sch, hash16, &rr);
    if (rc == 2) {
        btree_ordered_walk_release_for_blocking(wh);
        rc = read_record_ref(c->db_root, c->object, c->sch, hash16, &rr) != 0 ? 1 : 0;
    }
    if (rc != 0) return 0;
    const uint8_t *key_start = rr.key;
    const uint8_t *raw       = rr.val;
    uint32_t       value_len = (uint32_t)rr.vlen;

    /* Excluded-keys check: the key is a string in ExcludedKeys.
       is_excluded() takes a char* key so build it from the fetched record. */
    if (c->excluded && c->excluded->count > 0) {
        char keybuf[1100];
        format_query_wire_key(c->fs, key_start, rr.klen, keybuf);
        if (is_excluded(c->excluded, keybuf)) {
            release_record_ref(&rr);
            return 0;
        }
    }

    /* Post-filter: apply the full criteria tree (seed leaf trivially passes
       since we're inside its prefix; any extra AND leaves still need checking). */
    int matched=0;
    if(criteria_match_tree_checked(raw,value_len,c->tree,c->fs,rr.hdr2,&matched)){
        int error=errno?errno:EILSEQ;release_record_ref(&rr);
        pthread_mutex_lock(&c->lock);c->predicate_error=error;pthread_mutex_unlock(&c->lock);return -1;}
    if(!matched){release_record_ref(&rr);return 0;}

    /* Offset skip-after-match */
    if (c->skip_remaining > 0) {
        c->skip_remaining--;
        release_record_ref(&rr);
        return 0;
    }

    /* Emit the row.  Supports default JSON and dict_fmt; rows_fmt/csv are
       excluded by the caller guard so we don't need those branches here. */
    char key_buf[1100];
    format_query_wire_key(c->fs, key_start, rr.klen, key_buf);

    if(typed_query_emit_row(g_out?g_out:stdout,key_buf,strlen(key_buf),raw,value_len,rr.hdr2,c->fs,
        c->proj_fields,c->proj_count,0,c->dict_fmt,0,c->printed>0,NULL,0,NULL,g_query_buffer_max_bytes)){
        c->predicate_error=errno?errno:EILSEQ;
        release_record_ref(&rr);
        return -1;
    }

    c->printed++;
    release_record_ref(&rr);
    return (c->printed >= c->limit) ? -1 : 0;
}

static int composite_prefix_record_cb(const uint8_t hash16[16],
                                       const void *key, size_t klen,
                                       const void *value, size_t vlen,
                                       uint8_t hdr2, void *ctx) {
    (void)hash16;
    CompositePrefixCtx *c = (CompositePrefixCtx *)ctx;
    g_out = c->parent_out;

    if (c->excluded && c->excluded->count > 0) {
        char keybuf[1100];
        format_query_wire_key(c->fs, key, klen, keybuf);
        if (is_excluded(c->excluded, keybuf)) return 0;
    }

    int matched=0;
    if(criteria_match_tree_checked(value,vlen,c->tree,c->fs,hdr2,&matched)){
        pthread_mutex_lock(&c->lock);c->predicate_error=errno?errno:EILSEQ;pthread_mutex_unlock(&c->lock);return 1;}
    if(!matched)return 0;

    pthread_mutex_lock(&c->lock);
    if (c->skip_remaining > 0) {
        c->skip_remaining--;
        pthread_mutex_unlock(&c->lock);
        return 0;
    }

    char key_buf[1100];
    format_query_wire_key(c->fs, key, klen, key_buf);

    if(typed_query_emit_row(g_out?g_out:stdout,key_buf,strlen(key_buf),value,vlen,hdr2,c->fs,
        c->proj_fields,c->proj_count,0,c->dict_fmt,0,c->printed>0,NULL,0,NULL,g_query_buffer_max_bytes)){
        c->predicate_error=errno?errno:EILSEQ;
        pthread_mutex_unlock(&c->lock);
        return 1;
    }

    c->printed++;
    int done = (c->limit > 0 && c->printed >= c->limit);
    pthread_mutex_unlock(&c->lock);
    return done ? -1 : 0;
}

/* Query-owned path and encoded-bound storage for one composite IN range.
 * Iterator, heap, resume, and release/reopen state live exclusively behind
 * btree_walk_ordered_ranges. */
typedef struct {
    char    path[PATH_MAX];
    uint8_t lo[1024 + 8];
    size_t  lo_len;
    uint8_t hi[1024 + 8];
    size_t  hi_len;
} CompositeRangeStorage;

/* D1 executor entry point.  seed is the equality leaf on the prefix field
   (e.g. by="alice"); order_by is the sort field (e.g. "time").
   Returns number of rows emitted (≥0) or -1 on deadline. */
static int composite_order_bound(const TypedSchema *ts,int field,const char *text,
 size_t len,uint8_t *out,size_t *outlen){
    *outlen=0;size_t need=0;
    int rc=typed_index_text_key_into(ts,(size_t)field,text,len,NULL,0,&need);
    if(rc<0&&errno==EDOM)return 0;
    if(rc!=1)return rc;
    if(need>543)return 0;
    return typed_index_text_key_into(ts,(size_t)field,text,len,out,543,outlen);
}
/* Bounds contain all inter-component tiers but omit the first tier, which
   the ordered-index wrapper supplies. -2 means exact-scan fallback. */
static int composite_prefix_bounds(FieldSchema *fs,const SearchCriterion *seed,
 const char *seed_text,size_t seed_len,const char *order_by,const SearchCriterion *range,
 uint8_t lo[544],size_t *lo_len,int *lo_excl,uint8_t hi[544],size_t *hi_len,int *hi_excl){
    *lo_len=*hi_len=0;*lo_excl=0;*hi_excl=1;
    if(!fs||!fs->ts){errno=EINVAL;return -1;}
    int sf=typed_field_index(fs->ts,seed->field),of=typed_field_index(fs->ts,order_by);
    if(sf<0||of<0){errno=EINVAL;return -1;}
    size_t prefix=0;
    int rc=typed_index_text_component_key_into(fs->ts,(size_t)sf,seed_text,seed_len,1,NULL,0,&prefix);
    if(rc<0&&(errno==EDOM||errno==E2BIG))return -2;
    if(rc!=1)return rc<0?-1:-2;
    size_t width=0;int fixed=typed_index_fixed_width(fs->ts,(size_t)of,&width);
    if(fixed<0)return -1;
    size_t minimum_suffix=1+(fixed?width:0);
    if(prefix>543||minimum_suffix>543-prefix){errno=E2BIG;return -2;}
    if(typed_index_text_component_key_into(fs->ts,(size_t)sf,seed_text,seed_len,1,lo,543,&prefix)!=1)return -1;
    *lo_len=prefix;
    /* Strict byte successor bounds every key beginning with the complete
       component. All-ff legacy prefixes use an open upper bound. */
    size_t at=prefix;
    while(at&&lo[at-1]==255)at--;
    if(at){memcpy(hi,lo,at);hi[at-1]++;*hi_len=at;}
    if(range){
        const char *texts[2]={NULL,NULL};size_t sizes[2]={0,0};int excl[2]={0,0};
        switch(range->op){
        case OP_GREATER:excl[0]=1;/* fall through */
        case OP_GREATER_EQ:texts[0]=search_criterion_value(range);sizes[0]=search_criterion_value_len(range);break;
        case OP_LESS:excl[1]=1;/* fall through */
        case OP_LESS_EQ:texts[1]=search_criterion_value(range);sizes[1]=search_criterion_value_len(range);break;
        case OP_EQUAL:texts[0]=texts[1]=search_criterion_value(range);sizes[0]=sizes[1]=search_criterion_value_len(range);break;
        case OP_BETWEEN:texts[0]=search_criterion_value(range);sizes[0]=search_criterion_value_len(range);
            texts[1]=search_criterion_value2(range);sizes[1]=search_criterion_value2_len(range);
            excl[0]=range->min_exclusive;excl[1]=range->max_exclusive;break;
        default:break;
        }
        for(int side=0;side<2;side++)if(texts[side]){
            uint8_t encoded[544];size_t n=0;
            rc=composite_order_bound(fs->ts,of,texts[side],sizes[side],encoded,&n);
            if(rc<0)return -1;
            if(rc!=1||n>543-prefix-1)continue;
            uint8_t *dst=side?hi:lo;
            memcpy(dst,lo,prefix);dst[prefix]=0;memcpy(dst+prefix+1,encoded,n);
            if(side){*hi_len=prefix+1+n;*hi_excl=excl[side];}
            else {*lo_len=prefix+1+n;*lo_excl=excl[side];}
        }
    }
    return 0;
}
static int find_via_composite_prefix(const char *db_root,const char *object,
 const Schema *sch,FieldSchema *fs,SearchCriterion *seed,const char *order_by,int order_desc,
 SearchCriterion *order_range,CriteriaNode *tree,ExcludedKeys *excluded,int offset,int limit,
 const char **proj_fields,int proj_count,int dict_fmt,QueryDeadline *dl){
    char composite_field[256];
    int written=snprintf(composite_field,sizeof(composite_field),"%s+%s",seed->field,order_by);
    if(written<0||(size_t)written>=sizeof(composite_field)){errno=EINVAL;return -1;}
    int nv=seed->op==OP_IN?seed->in_count:1;
    if(nv<1)return 0;
    int ns=index_splits_for(sch->splits);
    BtOrderedRangeSpec *ranges=NULL;CompositeRangeStorage *storage=NULL;
    size_t count=(size_t)nv*(size_t)ns;
    int merge_ranges=seed->op==OP_IN;
    if(merge_ranges){
        if(count>g_query_buffer_max_bytes/(sizeof(*ranges)+sizeof(*storage))){errno=ENOBUFS;return -1;}
        ranges=calloc(count,sizeof(*ranges));storage=calloc(count,sizeof(*storage));
        if(!ranges||!storage){free(ranges);free(storage);errno=ENOMEM;return -1;}
    }
    uint8_t single_lo[544],single_hi[544];size_t single_ll=0,single_hl=0;
    int single_le=0,single_he=0;
    for(int v=0;v<nv;v++){
        const char *text=seed->op==OP_IN?seed->in_values[v]:search_criterion_value(seed);
        size_t len=seed->op==OP_IN?strlen(text):search_criterion_value_len(seed);
        uint8_t lo[544],hi[544];size_t ll=0,hl=0;int le=0,he=0;
        int rc=composite_prefix_bounds(fs,seed,text,len,order_by,order_range,lo,&ll,&le,hi,&hl,&he);
        if(rc){free(ranges);free(storage);return rc;}
        if(!merge_ranges){memcpy(single_lo,lo,ll);memcpy(single_hi,hi,hl);single_ll=ll;single_hl=hl;single_le=le;single_he=he;}
        else for(int shard=0;shard<ns;shard++){
            size_t i=(size_t)v*(size_t)ns+(size_t)shard;
            build_idx_path(storage[i].path,sizeof(storage[i].path),db_root,object,composite_field,shard);
            storage[i].lo[0]=0;memcpy(storage[i].lo+1,lo,ll);storage[i].lo_len=ll+1;
            if(hl){storage[i].hi[0]=0;memcpy(storage[i].hi+1,hi,hl);storage[i].hi_len=hl+1;}
            ranges[i]=(BtOrderedRangeSpec){.path=storage[i].path,.min_val=(const char*)storage[i].lo,
                .min_len=storage[i].lo_len,.min_exclusive=le,
                .max_val=hl?(const char*)storage[i].hi:NULL,.max_len=hl?storage[i].hi_len:0,
                .max_exclusive=he,.tie_id=v};
        }
    }
    CompositePrefixCtx ctx={0};ctx.db_root=db_root;ctx.object=object;ctx.sch=sch;ctx.fs=fs;
    ctx.tree=tree;ctx.excluded=excluded;ctx.proj_fields=proj_fields;ctx.proj_count=proj_count;
    ctx.dict_fmt=dict_fmt;ctx.skip_remaining=offset>0?offset:0;ctx.limit=limit>0?limit:INT_MAX;
    ctx.dl=dl;ctx.parent_out=g_out;pthread_mutex_init(&ctx.lock,NULL);
    if(!merge_ranges)btree_idx_walk_ordered(db_root,object,composite_field,sch->splits,
        (const char*)single_lo,single_ll,single_le,single_hl?(const char*)single_hi:NULL,single_hl,single_he,
        order_desc,composite_prefix_cb,&ctx);
    else btree_walk_ordered_ranges(ranges,count,order_desc,composite_prefix_cb,&ctx);
    pthread_mutex_destroy(&ctx.lock);free(ranges);free(storage);
    if(ctx.predicate_error){errno=ctx.predicate_error;return -1;}
    return ctx.printed;
}

/* Encoded order-by walk bounds derived from the order_by-field range/eq leaves.
 * Defaults to the full range ("" .. 0xffffffff). Only TOP-LEVEL AND leaves are
 * consulted (OR/nested branches can't bound the walk). The walk's per-record
 * criteria_match_tree still runs, so a loose/absent bound never changes the
 * result set — it only narrows how far the index is walked. */
/* OrderWalkBounds and order_walk_bounds are defined in query_internal.h */

/* collect_and_leaves is declared in query_internal.h */

static int order_bound_cmp(const uint8_t *a,size_t an,const uint8_t *b,size_t bn){
    size_t n=an<bn?an:bn;int c=memcmp(a,b,n);return c?c:(an>bn)-(an<bn);
}
int order_walk_bounds_checked(CriteriaNode *tree,FieldSchema *fs,
 const char *order_by,OrderWalkBounds *bounds){
    memset(bounds,0,sizeof(*bounds));memset(bounds->hi,255,4);bounds->hi_len=4;
    if(!order_by||!fs||!fs->ts)return 0;
    int field=typed_field_index(fs->ts,order_by);if(field<0)return 0;
    SearchCriterion *leaves[MAX_INTERSECT_LEAVES];
    int count=collect_and_leaves(tree,leaves,MAX_INTERSECT_LEAVES);
    for(int i=0;i<count;i++){
        SearchCriterion *leaf=leaves[i];if(strcmp(leaf->field,order_by))continue;
        const char *texts[2]={NULL,NULL};size_t sizes[2]={0,0};int exclusive[2]={0,0};
        switch(leaf->op){
        case OP_GREATER:exclusive[0]=1;/* fall through */
        case OP_GREATER_EQ:texts[0]=search_criterion_value(leaf);sizes[0]=search_criterion_value_len(leaf);break;
        case OP_LESS:exclusive[1]=1;/* fall through */
        case OP_LESS_EQ:texts[1]=search_criterion_value(leaf);sizes[1]=search_criterion_value_len(leaf);break;
        case OP_EQUAL:texts[0]=texts[1]=search_criterion_value(leaf);sizes[0]=sizes[1]=search_criterion_value_len(leaf);break;
        case OP_BETWEEN:texts[0]=search_criterion_value(leaf);sizes[0]=search_criterion_value_len(leaf);
            texts[1]=search_criterion_value2(leaf);sizes[1]=search_criterion_value2_len(leaf);
            exclusive[0]=leaf->min_exclusive;exclusive[1]=leaf->max_exclusive;break;
        default:break;
        }
        for(int side=0;side<2;side++)if(texts[side]){
            size_t need=0;errno=0;
            int rc=typed_index_text_key_into(fs->ts,(size_t)field,texts[side],sizes[side],NULL,0,&need);
            if(rc<0){if(errno==EDOM)continue;return -1;}
            /* Unknown enum or valid bound beyond the ordered admission
               domain is an omitted optimization. Exact postfilter remains. */
            if(rc==0||need>544)continue;
            uint8_t key[544];size_t len=0;
            if(typed_index_text_key_into(fs->ts,(size_t)field,texts[side],sizes[side],key,sizeof(key),&len)!=1)return -1;
            if(side==0){
                int cmp=order_bound_cmp(key,len,bounds->lo,bounds->lo_len);
                if(!bounds->has_lo||cmp>0||(cmp==0&&exclusive[0])){
                    memcpy(bounds->lo,key,len);bounds->lo_len=len;bounds->lo_excl=exclusive[0];bounds->has_lo=1;
                }
            }else {
                int cmp=order_bound_cmp(key,len,bounds->hi,bounds->hi_len);
                if(!bounds->has_hi||cmp<0||(cmp==0&&exclusive[1])){
                    memcpy(bounds->hi,key,len);bounds->hi_len=len;bounds->hi_excl=exclusive[1];bounds->has_hi=1;
                }
            }
        }
    }return 0;
}
void order_walk_bounds(CriteriaNode *tree,FieldSchema *fs,const char *order_by,OrderWalkBounds *bounds){
    (void)order_walk_bounds_checked(tree,fs,order_by,bounds);
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
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &sinfo);

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
        if (sdb) {
            /* Lock-safe chunked walk of the exact-key prefix range
             * ([encoded_key, encoded_key ++ 0xff×4)) — same executor as
             * idx_find_streaming's btree path (AB-BA fix). Falls back to
             * the inline-flush search when no slotcask handle exists. */
            ChunkShared sh = {0};
            sh.sdb = sdb;
            sh.stop = NULL;
            sh.record_cb = composite_prefix_record_cb;
            sh.record_ctx = &ctx;
            sh.filter = NULL;   /* exact-prefix leaf entries all match */
            sh.fctx = NULL;
            sh.cap = chunk_cap_for(bfb.pending_cap,
                                   index_splits_for(sch->splits));
            uint8_t hi[1024 + 4];
            memcpy(hi, key, klen);
            memset(hi + klen, 0xff, 4);
            chunk_walk_fanout(&sh, db_root, object, composite_field,
                              sch->splits, (const char *)key, klen, 0,
                              (const char *)hi, klen + 4, 0);
        } else {
            btree_idx_search(db_root, object, composite_field, sch->splits,
                             (const char *)key, klen, batch_buf_collect_cb,
                             &bfb);
        }
        batch_buf_destroy(&bfb);
    }

    pthread_mutex_destroy(&ctx.lock);
    if(ctx.predicate_error){errno=ctx.predicate_error;return -1;}
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
    int predicate_error;
} OrderIndexWalkCtx;

#ifdef TEST_BUILD
/* Counts index entries visited by the order-by walks, so a test can prove a
 * windowed query stops at the window instead of scanning the whole index. */
long g_order_walk_scanned = 0;
long order_walk_scanned_for_test(void)   { return g_order_walk_scanned; }
void order_walk_scanned_reset_for_test(void) { g_order_walk_scanned = 0; }
#endif

static int order_index_walk_cb(const char *val, size_t vlen,
                                const uint8_t *hash16,
                                BtOrderedWalkHandle *wh, void *ctx_ptr) {
#ifdef TEST_BUILD
    g_order_walk_scanned++;
#endif
    (void)val; (void)vlen;   /* walk value not needed; hash16 is the record key */
    OrderIndexWalkCtx *c = (OrderIndexWalkCtx *)ctx_ptr;
    g_out = c->parent_out;
    if (query_deadline_tick(c->dl, &c->dl_counter)) return -1;
    if (c->printed >= c->limit) return -1;

#ifdef TEST_BUILD
    order_walk_test_maybe_pause();
#endif

    /* Fetch the record by hash16. */
    RecordRef rr;
    int rc = read_record_ref_try(c->db_root, c->object, c->sch, hash16, &rr);
    if (rc == 2) {
        btree_ordered_walk_release_for_blocking(wh);
        rc = read_record_ref(c->db_root, c->object, c->sch, hash16, &rr) != 0 ? 1 : 0;
    }
    if (rc != 0) return 0;
    const uint8_t *key_start = rr.key;
    const uint8_t *raw       = rr.val;
    uint32_t       value_len = (uint32_t)rr.vlen;

    /* Excluded-keys check. */
    if (c->excluded && c->excluded->count > 0) {
        char keybuf[1100];
        format_query_wire_key(c->fs, key_start, rr.klen, keybuf);
        if (is_excluded(c->excluded, keybuf)) {
            release_record_ref(&rr);
            return 0;
        }
    }

    /* Post-filter: apply the FULL criteria tree — unlike D1 nothing is
       guaranteed to match by the walk range alone. */
    int matched=0;
    if(criteria_match_tree_checked(raw,value_len,c->tree,c->fs,rr.hdr2,&matched)){
        c->predicate_error=errno?errno:EILSEQ;release_record_ref(&rr);return -1;}
    if(!matched){release_record_ref(&rr);return 0;}

    /* Offset skip-after-match. */
    if (c->skip_remaining > 0) {
        c->skip_remaining--;
        release_record_ref(&rr);
        return 0;
    }

    /* Emit the row.  Supports default JSON and dict_fmt; rows_fmt/csv are
       excluded by the caller guard so we don't need those branches here. */
    char key_buf[1100];
    format_query_wire_key(c->fs, key_start, rr.klen, key_buf);

    if(typed_query_emit_row(g_out?g_out:stdout,key_buf,strlen(key_buf),raw,value_len,rr.hdr2,c->fs,
        c->proj_fields,c->proj_count,0,c->dict_fmt,0,c->printed>0,NULL,0,NULL,g_query_buffer_max_bytes)){
        c->predicate_error=errno?errno:EILSEQ;
        release_record_ref(&rr);
        return -1;
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
       per-record criteria_match_tree still runs, so this only narrows the
       walk. Unbounded sides pass through OPEN (NULL,0): the ordered walk
       must deliver NULL-tier rows at the ASC tail / DESC head (§2.5) — the
       legacy \xff*4 sentinel would ride btree_idx_walk_ordered's [0x00]
       prefix and clamp the walk below the NULL tier. */
    OrderWalkBounds owb;
    if(order_walk_bounds_checked(tree,fs,order_by,&owb))return -1;
    const char *lo = owb.has_lo ? (const char *)owb.lo : NULL;
    size_t      lo_len  = owb.has_lo ? owb.lo_len  : 0;
    int         lo_excl = owb.has_lo ? owb.lo_excl : 0;
    const char *hi = owb.has_hi ? (const char *)owb.hi : NULL;
    size_t      hi_len  = owb.has_hi ? owb.hi_len  : 0;
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

    if(ctx.predicate_error){errno=ctx.predicate_error;return -1;}
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
    return OP_UNKNOWN;
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
        if (keyset_insert(p->out, hash16) < 0) {
            LOG_WARN(LOG_SUB_QUERY, "intersect_probe_cb: destination keyset capacity exhausted");
            return -1;
        }
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
    if (keyset_insert(c->ks, hash16) < 0) {
        LOG_WARN(LOG_SUB_QUERY, "intersect_collect_cb: keyset capacity exhausted, marking overflow");
        c->overflowed = 1; return -1;
    }
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
    const TypedSchema *schema;
    uint8_t *vals[BM_DICT_MATCH_CAP];
    size_t   vlens[BM_DICT_MATCH_CAP];
    size_t   bytes, budget;
    int      n_match, error;
} BmDictMatchCtx;

static void bm_dict_match_clear(BmDictMatchCtx *m) {
    for (int i = 0; i < m->n_match; i++) free(m->vals[i]);
    m->n_match = 0;
    m->bytes = 0;
}

/* Numeric dictionary entries carry sortable index payloads, never packed
 * record descriptors or display text. Validate their canonical shape before
 * comparing. This does not decode or rewrite the engine's dictionary. */
static int bm_numeric_payload_valid(const TypedSchema *ts,
                                    const uint8_t *v, size_t n) {
    if (!ts->packed.nfields) return n == 8;
    if (n == 1) return v[0] == 1;
    if (n < 9 || !(n & 1) || (v[0] != 0 && v[0] != 2)) return 0;
    unsigned mask = v[0] == 0 ? 255u : 0u;
    if ((v[n-2] ^ mask) || (v[n-1] ^ mask)) return 0;
    for (size_t i = 5; i + 2 < n; i += 2) {
        unsigned limb = ((unsigned)(v[i] ^ mask) << 8) | (v[i+1] ^ mask);
        if (limb < 1 || limb > 10000) return 0;
        if ((i == 5 || i + 4 == n) && limb == 1) return 0;
    }
    return 1;
}

static int bm_numeric_literal_cmp(BmDictMatchCtx *m, const uint8_t *v,
                                  size_t n, const char *text, size_t text_n,
                                  int *cmp) {
    int field = typed_field_index(m->schema, m->crit->field);
    size_t need = 0;
    if (field < 0) { errno = EINVAL; return -1; }
    int rc = typed_index_text_key_into(m->schema, (size_t)field,
                                      text, text_n, NULL, 0, &need);
    if (rc != 1) { if (!errno) errno = EINVAL; return -1; }
    if (need > m->budget - m->bytes) { errno = ENOBUFS; return -1; }
    uint8_t *key = malloc(need ? need : 1);
    if (!key) { errno = ENOMEM; return -1; }
    rc = typed_index_text_key_into(m->schema, (size_t)field, text, text_n,
                                   key, need, &need);
    if (rc != 1) { free(key); if (!errno) errno = EINVAL; return -1; }
    size_t common = n < need ? n : need;
    int c = memcmp(v, key, common);
    *cmp = c ? c : (n > need) - (n < need);
    free(key);
    return 0;
}

static int bm_numeric_dict_match(BmDictMatchCtx *m, const uint8_t *v,
                                 size_t n, int *matched) {
    SearchCriterion *c = m->crit;
    *matched = 0;
    int valid=m->schema!=NULL;
    if(valid&&m->tf->type==FT_ENUM){
        valid=n==(size_t)m->tf->enum_width&&(n==1||n==2);
        if(valid){unsigned e=n==2?((unsigned)v[0]<<8)|v[1]:v[0];
            valid=e<(unsigned)m->tf->n_enum_values;}
    }else if(valid)valid=bm_numeric_payload_valid(m->schema,v,n);
    if(!valid){errno=EILSEQ;return -1;}
    if (c->op == OP_EXISTS) { *matched = 1; return 0; }
    if (c->op == OP_IN || c->op == OP_NOT_IN) {
        int found = 0;
        for (int i = 0; i < c->in_count; i++) {
            int cmp;
            if (bm_numeric_literal_cmp(m, v, n, c->in_values[i],
                                       strlen(c->in_values[i]), &cmp)) return -1;
            if (!cmp) { found = 1; break; }
        }
        *matched = c->op == OP_IN ? found : !found;
        return 0;
    }
    int cmp;
    if (bm_numeric_literal_cmp(m, v, n, search_criterion_value(c),
                               search_criterion_value_len(c), &cmp)) return -1;
    switch (c->op) {
    case OP_EQUAL: *matched = cmp == 0; return 0;
    case OP_NOT_EQUAL: *matched = cmp != 0; return 0;
    case OP_LESS: *matched = cmp < 0; return 0;
    case OP_LESS_EQ: *matched = cmp <= 0; return 0;
    case OP_GREATER: *matched = cmp > 0; return 0;
    case OP_GREATER_EQ: *matched = cmp >= 0; return 0;
    case OP_BETWEEN: {
        int hi;
        if (bm_numeric_literal_cmp(m, v, n, search_criterion_value2(c),
                                   search_criterion_value2_len(c), &hi)) return -1;
        *matched = (c->min_exclusive ? cmp > 0 : cmp >= 0) &&
                   (c->max_exclusive ? hi < 0 : hi <= 0);
        return 0;
    }
    default: errno = EINVAL; return -1;
    }
}

static int bm_varchar_dict_match(const uint8_t *value,size_t n,
                                  const SearchCriterion *c,int *matched){
    const char *text=search_criterion_value(c);
    int cmp=order_bound_cmp(value,n,(const uint8_t*)text,search_criterion_value_len(c));
    switch(c->op){
    case OP_EQUAL:*matched=cmp==0;return 1;
    case OP_NOT_EQUAL:*matched=cmp!=0;return 1;
    case OP_LESS:*matched=cmp<0;return 1;
    case OP_LESS_EQ:*matched=cmp<=0;return 1;
    case OP_GREATER:*matched=cmp>0;return 1;
    case OP_GREATER_EQ:*matched=cmp>=0;return 1;
    case OP_BETWEEN:{int hi=order_bound_cmp(value,n,(const uint8_t*)search_criterion_value2(c),search_criterion_value2_len(c));
        *matched=(c->min_exclusive?cmp>0:cmp>=0)&&(c->max_exclusive?hi<0:hi<=0);return 1;}
    case OP_IN:case OP_NOT_IN:{
        int found=0;
        for(int i=0;i<c->in_count;i++) {
            if(!order_bound_cmp(value,n,(const uint8_t*)c->in_values[i],strlen(c->in_values[i]))) {
                found=1;
                break;
            }
        }
        *matched=c->op==OP_IN?found:!found;
        return 1;
    }
    default:return 0;
    }
}

static int bm_dict_match_cb(const uint8_t *value, size_t vlen, void *ctx) {
    BmDictMatchCtx *m = ctx;
    SearchCriterion *c = m->crit;
    int matched = 0;
    if (m->error) return 1;
    if (op_is_length(c->op)) {
        matched = match_length_vlen(vlen, c);
    } else if (m->tf && (m->tf->type == FT_NUMERIC||m->tf->type == FT_ENUM)) {
        if (bm_numeric_dict_match(m, value, vlen, &matched)) goto failed;
    } else if (m->tf && m->tf->type==FT_VARCHAR &&
               bm_varchar_dict_match(value,vlen,c,&matched)) {
        /* Length-aware byte comparison already produced the result. */
    } else {
        if (vlen == SIZE_MAX) { errno = ENOBUFS; goto failed; }
        size_t cap = vlen + 1 > 512 ? vlen + 1 : 512;
        if (cap > m->budget - m->bytes) { errno = ENOBUFS; goto failed; }
        char *buf = malloc(cap);
        if (!buf) { errno = ENOMEM; goto failed; }
        int dlen;
        if (!m->tf || m->tf->type == FT_VARCHAR) {
            memcpy(buf, value, vlen); buf[vlen] = 0;
            dlen = (int)vlen;
        } else {
            dlen = decode_idx_to_buf(m->tf, value, vlen, buf, cap, 0);
        }
        if (dlen < 0 || (size_t)dlen >= cap) {
            free(buf); errno = EILSEQ; goto failed;
        }
        matched = match_criterion_vlen(buf, (size_t)dlen, c);
        free(buf);
    }
    if (matched) {
        if (m->n_match == BM_DICT_MATCH_CAP || vlen > m->budget - m->bytes) {
            errno = ENOBUFS; goto failed;
        }
        uint8_t *copy = malloc(vlen ? vlen : 1);
        if (!copy) { errno = ENOMEM; goto failed; }
        memcpy(copy, value, vlen);
        m->vals[m->n_match] = copy;
        m->vlens[m->n_match++] = vlen;
        m->bytes += vlen;
    }
    return 0;
failed:
    m->error = errno ? errno : EILSEQ;
    return 1;
}

/* Reader open for a bitmap read consumer. 0 = handle opened; 1 = the shard's
   file does not exist (never written: logically empty, not an error); -1 =
   hard read failure with errno set (corruption, acquisition error,
   admission exhaustion, I/O). */
static int bm_open_read(const char *path, BitmapShard **out) {
    errno = 0;
    *out = bm_open(path, 0, 0, 0, 0, 0 /* reader */);
    if (*out) return 0;
    if (errno == ENOENT) return 1;
    if (errno == 0) errno = EIO;
    return -1;
}

/* Walk a single shard's bitmap dict-scan style. Returns 1 if cb
   signalled stop. Mirrors bitmap_emit_for_shard's contract. */
static int bitmap_emit_generic_for_shard(const char *db_root, const char *object,
                                          const char *field, int shard_idx,
                                          SearchCriterion *crit,
                                          const TypedField *tf,
                                          bt_result_cb cb, void *ctx,
                                          SlotcaskDb *sdb,
                                          const TypedSchema *schema,
                                          size_t budget) {
    char kfp[PATH_MAX];
    slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, shard_idx);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0)
        return 0;

    char bp[1024];
    bm_build_path(bp, sizeof(bp), db_root, object, field, shard_idx);
    BitmapShard *bm = NULL;
    int orc = bm_open_read(bp, &bm);
    if (orc != 0) {
        int saved = errno;
        kfcache_release(&kh);
        if (orc < 0) { errno = saved; return -1; }
        return 0;
    }

    BmDictMatchCtx m = { .crit = crit, .tf = tf, .schema = schema,
                         .budget = budget };
    bm_iter_values(bm, bm_dict_match_cb, &m);
    if (m.error || m.n_match == 0) {
        int error = m.error;
        bm_dict_match_clear(&m);
        bm_close(bm);
        kfcache_release(&kh);
        if (error) { errno = error; return -1; }
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

    bm_dict_match_clear(&m);
    bm_close(bm);
    kfcache_release(&kh);
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
                                                 a->cb, a->ctx, a->sdb,
                                                 a->schema, a->budget);
    if (stopped < 0) a->error = errno ? errno : EILSEQ;
    bm_defer_drain_if_armed(a->cb, a->sdb);
    if (stopped && a->stop_flag) {
        __atomic_store_n(a->stop_flag, 1, __ATOMIC_RELAXED);
    }
    return NULL;
}

/* bt_result_cb adapter for the generic dict-scan → KeySet path.
   Inserts each emitted hash into the keyset, respects deadline. A full
   KeySet is a capacity failure: it is recorded (ENOBUFS) and stops the walk;
   the caller discards the whole collection. */
typedef struct {
    KeySet *ks;
    QueryDeadline *dl;
    int error;                  /* errno diagnostic */
    CandidateBuildFailure reason;
} BmKsEmitCtx;
static int bm_ks_insert_cb(const char *v, size_t vl, const uint8_t *h, void *c) {
    (void)v; (void)vl;
    BmKsEmitCtx *kc = (BmKsEmitCtx *)c;
    if (kc->reason != CBF_NONE) return -1;
    if (kc->dl && kc->dl->timed_out) {
        kc->reason = CBF_TIMEOUT;
        return -1;
    }
    if (bm_ks_counted_insert(kc->ks, h) < 0) {
        kc->reason = CBF_INSERT_CAPACITY;
        kc->error = ENOBUFS;
        return -1;
    }
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
    const TypedSchema *schema;
    size_t            budget;
    size_t            count;
    int               error; /* worker-owned; read after dispatch */
} BmPopcountGenericShardArg;

static void *bm_popcount_generic_shard_worker(void *raw) {
    BmPopcountGenericShardArg *a = (BmPopcountGenericShardArg *)raw;
    char bp[1024];
    bm_build_path(bp, sizeof(bp), a->db_root, a->object, a->field, a->shard_idx);
    a->count = 0;
    BitmapShard *bm = NULL;
    int orc = bm_open_read(bp, &bm);
    if (orc != 0) {
        if (orc < 0) a->error = errno;
        return NULL;
    }
    BmDictMatchCtx m = { .crit = a->crit, .tf = a->tf,
                         .schema = a->schema, .budget = a->budget };
    bm_iter_values(bm, bm_dict_match_cb, &m);
    size_t local = 0;
    for (int i = 0; !m.error && i < m.n_match; i++) {
        size_t n = bm_count(bm, m.vals[i], m.vlens[i]);
        if (n > SIZE_MAX - local) { m.error = EOVERFLOW; break; }
        local += n;
    }
    a->error = m.error;
    bm_dict_match_clear(&m);
    bm_close(bm);
    a->count = local;
    return NULL;
}

static size_t bm_popcount_generic_for_crit(const char *db_root, const char *object,
                                            const char *field, int splits,
                                            SearchCriterion *crit,
                                            const TypedField *tf,
                                            const TypedSchema *schema) {
    if (splits <= 0) return 0;
    size_t budget = g_query_buffer_max_bytes / (size_t)splits;
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
            BitmapShard *bm = NULL;
            int orc = bm_open_read(bp, &bm);
            if (orc > 0) continue;
            if (orc < 0) return SIZE_MAX;
            BmDictMatchCtx m = { .crit = crit, .tf = tf, .schema = schema,
                                 .budget = budget };
            bm_iter_values(bm, bm_dict_match_cb, &m);
            for (int i = 0; !m.error && i < m.n_match; i++) {
                size_t n = bm_count(bm, m.vals[i], m.vlens[i]);
                if (n > SIZE_MAX - total) { m.error = EOVERFLOW; break; }
                total += n;
            }
            int error = m.error;
            bm_dict_match_clear(&m);
            bm_close(bm);
            if (error) { errno = error; return SIZE_MAX; }
        }
        return total;
    }
    for (int s = 0; s < splits; s++) {
        args[s] = (BmPopcountGenericShardArg){
            .db_root = db_root, .object = object, .field = field,
            .shard_idx = s, .crit = crit, .tf = tf, .count = 0,
            .schema = schema, .budget = budget, .error = 0,
        };
    }
    parallel_for_io(bm_popcount_generic_shard_worker, args, splits,
                 sizeof(BmPopcountGenericShardArg));
    size_t total = 0;
    int error = 0;
    for (int s = 0; s < splits; s++) {
        if (args[s].error) error = args[s].error;
        if (args[s].count > SIZE_MAX - total) error = EOVERFLOW;
        else total += args[s].count;
    }
    free(args);
    if (error) { errno = error; return SIZE_MAX; }
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
    int predicate_error;
} BmIntersectShardArg;

static void *bm_intersect_shard_worker(void *raw) {
    BmIntersectShardArg *a = (BmIntersectShardArg *)raw;
    a->count = 0;
    if (a->n_leaves < 2 || a->n_leaves > MAX_INTERSECT_LEAVES) {
        LOG_ERROR(LOG_SUB_QUERY, "bm_intersect_shard_worker: invalid n_leaves=%d (expected 2..%d) — caller should have validated this", a->n_leaves, MAX_INTERSECT_LEAVES);
        return NULL;
    }

    /* Pre-encode all leaf values */
    uint8_t  vals[MAX_INTERSECT_LEAVES][1024];
    size_t   vlens[MAX_INTERSECT_LEAVES];
    for (int i = 0; i < a->n_leaves; i++) {
        const TypedField *tf = resolve_idx_field(a->ts, a->leaves[i]->field);
        if (!tf) {
            LOG_ERROR(LOG_SUB_QUERY, "bm_intersect_shard_worker: resolve_idx_field failed for already-validated indexed field '%s'", a->leaves[i]->field);
            return NULL;
        }
        if(a->leaves[i]->op==OP_EQUAL &&
           encode_criterion_value_checked(a->ts,tf,search_criterion_value(a->leaves[i]),strlen(search_criterion_value(a->leaves[i])),vals[i],sizeof(vals[i]),&vlens[i])<0){a->predicate_error=errno?errno:EILSEQ;return NULL;}
    }

    /* Open all bitmaps for this shard.
     * OP_EQUAL: single-value lookup.  OP_IN: OR together all matching
     * value bit-arrays into a stack buffer.  Other ops: goto cleanup. */
    BitmapShard *bm[MAX_INTERSECT_LEAVES] = {0};
    const uint8_t *bmap_bytes[MAX_INTERSECT_LEAVES] = {0};
    uint8_t *merged[MAX_INTERSECT_LEAVES] = {0};
    uint32_t min_stride = UINT32_MAX;

    {
        /* All of this shard's bitmaps are held at once: take the locks in the
           canonical order so a keyfile-rebuild remap (which holds several
           bitmap write locks) cannot deadlock against this reader. */
        char bps[MAX_INTERSECT_LEAVES][PATH_MAX];
        const char *bpp[MAX_INTERSECT_LEAVES];
        for (int i = 0; i < a->n_leaves; i++) {
            bm_build_path(bps[i], sizeof(bps[i]), a->db_root, a->object,
                          a->leaves[i]->field, a->shard_idx);
            bpp[i] = bps[i];
        }
        if (bm_open_many_ro(bpp, a->n_leaves, bm) != 0) {
            /* A missing AND input makes this shard's intersection logically
               empty; any other failure is a hard error, never a zero. */
            if (errno != ENOENT) a->predicate_error = errno ? errno : EIO;
            goto cleanup;
        }
    }

    for (int i = 0; i < a->n_leaves; i++) {
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
            if (!merged[i]) { a->predicate_error=ENOMEM; goto cleanup; }
            int any = 0;
            for (int v = 0; v < a->leaves[i]->in_count; v++) {
                uint8_t enc[1024];
                size_t elen;
                if(encode_criterion_value_checked(a->ts,tf,a->leaves[i]->in_values[v],strlen(a->leaves[i]->in_values[v]),enc,sizeof(enc),&elen)<0){a->predicate_error=errno?errno:EILSEQ;goto cleanup;}
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
    bm_close_many(bm, a->n_leaves);
    for (int i = 0; i < a->n_leaves; i++) free(merged[i]);
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
            if(a.predicate_error){errno=a.predicate_error;return SIZE_MAX;}
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
    int error=0;
    for (int s = 0; s < splits && (!dl || !dl->timed_out); s++){
        total += args[s].count;if(args[s].predicate_error)error=args[s].predicate_error;}
    free(args);
    if(error){errno=error;return SIZE_MAX;}
    return (!dl || !dl->timed_out) ? total : 0;
}

/* Walk a single shard's bitmap for `value`, emitting cb per live matching
   slot. Returns 1 if cb signalled stop (limit reached) so the outer shard
   loop can short-circuit, 0 when the shard was walked completely or is
   logically empty (no keyfile/bitmap file yet), -1 with errno preserved
   across the keyfile release on a hard bitmap read failure. */
int bitmap_emit_for_shard(const char *db_root, const char *object,
                                 const char *field, int shard_idx,
                                 const uint8_t *value, size_t vlen,
                                 bt_result_cb cb, void *ctx, SlotcaskDb *sdb) {
    char kfp[PATH_MAX];
    slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, shard_idx);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0)
        return 0;

    char bp[1024];
    bm_build_path(bp, sizeof(bp), db_root, object, field, shard_idx);
    BitmapShard *bm = NULL;
    int orc = bm_open_read(bp, &bm);
    if (orc != 0) {
        int saved = errno;
        kfcache_release(&kh);
        if (orc < 0) { errno = saved; return -1; }
        return 0;
    }

    BmEmitCtx ec = { kh.map, kh.capacity, value, vlen, cb, ctx, 0 };
    bm_walk(bm, value, vlen, bm_emit_cb, &ec);

    idx_count_cb_flush_thread();

    bm_close(bm);
    kfcache_release(&kh);
    return ec.stop ? 1 : 0;
}

/* parallel_for worker for shard fan-out. Checks the shared stop flag
   before doing any I/O so a peer's limit-hit propagates immediately.
   On a cb-driven stop, raises the flag so subsequent peers also bail; a
   hard read failure is recorded in the worker-owned error slot. */
static void *bm_shard_walk_worker(void *arg) {
    BmShardWalkArg *a = (BmShardWalkArg *)arg;
    if (a->stop_flag && __atomic_load_n(a->stop_flag, __ATOMIC_RELAXED)) {
        return NULL;
    }
    int stopped = bitmap_emit_for_shard(a->db_root, a->object, a->field,
                                        a->shard_idx, a->value, a->vlen,
                                        a->cb, a->ctx, a->sdb);
    if (stopped < 0) a->error = errno ? errno : EIO;
    bm_defer_drain_if_armed(a->cb, a->sdb);
    if (stopped > 0 && a->stop_flag) {
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
    int              error;   /* errno diagnostic (ENOBUFS on capacity) */
    CandidateBuildFailure reason;
} BmCollectCtx;

static int bm_collect_to_keyset_cb(uint32_t slot, void *ctx) {
    BmCollectCtx *c = (BmCollectCtx *)ctx;
    if (c->reason != CBF_NONE) return 1;
    if (c->dl && c->dl->timed_out) {
        c->reason = CBF_TIMEOUT;
        return 1;
    }
    if (slot >= c->kf_capacity) return 0;
    SlotcaskKfEntry *e = &c->kf_map[slot];
    if (e->flag != 1) return 0;
    if (bm_ks_counted_insert(c->ks, e->hash) < 0) {
        /* Actual keyset_insert -1: insertion capacity, never a truncated
           successful set. ENOBUFS stays as the errno diagnostic. */
        c->reason = CBF_INSERT_CAPACITY;
        c->error = ENOBUFS;
        return 1;
    }
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
    /* A bitmap-disabled object plans no bitmap: while the escape flag is
       set the .bm files may still name old kf slots (bmfix repair failed
       at startup), so bitmap entries don't count until reindex heals. */
    int bitmap_gated = want == IT_BITMAP &&
                       slotcask_bmfix_object_disabled(db_root, object);
    for (int i = 0; i < count; i++) {
        if (bitmap_gated && types[i] == IT_BITMAP) continue;
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
   over the cached arrays — cheap for the planner hot path. A bitmap
   entry is skipped while the object's bitmap planning is disabled
   (defense in depth behind field_has_index_type's gate): the gated
   planner has already chosen btree for such fields, so the default
   IT_BTREE keeps those callers consistent. */
enum IndexType field_index_type(const char *db_root, const char *object,
                                       const char *field) {
    char fields[MAX_FIELDS][256];
    enum IndexType types[MAX_FIELDS];
    int n = load_index_fields(db_root, object, fields, MAX_FIELDS);
    int n2 = load_index_types(db_root, object, types, MAX_FIELDS);
    int count = n < n2 ? n : n2;
    int bitmap_gated = slotcask_bmfix_object_disabled(db_root, object);
    for (int i = 0; i < count; i++) {
        if (bitmap_gated && types[i] == IT_BITMAP) continue;
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
int search_criterion_indexable(const TypedSchema *ts,const SearchCriterion *c){
    if(!ts||!c)return 0;
    int field=typed_field_index(ts,c->field);
    if(field<0)return 1;
    enum FieldType type=ts->fields[field].type;
    if(type!=FT_NUMERIC&&type!=FT_ENUM)return 1;
    /* Only encoded comparisons are answered from numeric/enum dictionaries.
       Enum BETWEEN and display predicates are unsupported by the typed matcher. */
    if(c->op!=OP_EQUAL&&c->op!=OP_NOT_EQUAL&&c->op!=OP_LESS&&c->op!=OP_GREATER&&
       c->op!=OP_LESS_EQ&&c->op!=OP_GREATER_EQ&&c->op!=OP_IN&&
       !(c->op==OP_BETWEEN&&type==FT_NUMERIC)&&
       !(c->op==OP_NOT_IN&&type==FT_ENUM)&&
       c->op!=OP_EXISTS&&c->op!=OP_NOT_EXISTS&&
       !(c->op>=OP_EQ_FIELD&&c->op<=OP_GTE_FIELD))return 0;
    if(c->op==OP_EXISTS||c->op==OP_NOT_EXISTS||
       (c->op>=OP_EQ_FIELD&&c->op<=OP_GTE_FIELD)||
       (c->op>=OP_LEN_EQ&&c->op<=OP_LEN_BETWEEN))return 1;
    size_t len=0;
    if(c->op==OP_IN||c->op==OP_NOT_IN){
        for(int i=0;i<c->in_count;i++)
            if(typed_index_text_key_into(ts,(size_t)field,c->in_values[i],
                 strlen(c->in_values[i]),NULL,0,&len)!=1)return 0;
        return 1;
    }
    if(typed_index_text_key_into(ts,(size_t)field,search_criterion_value(c),
         search_criterion_value_len(c),NULL,0,&len)!=1)return 0;
    return c->op!=OP_BETWEEN||
        typed_index_text_key_into(ts,(size_t)field,search_criterion_value2(c),
          search_criterion_value2_len(c),NULL,0,&len)==1;
}
/* This cap belongs to ordered-bound destinations only. Bitmap dictionary
 * keys have a uint16 length and trigram patterns are not ordered keys. */
static int ordered_criterion_fits(const TypedSchema *ts,const SearchCriterion *c){
    if(!ts)return 0;
    int field=typed_field_index(ts,c->field);
    if(field<0)return 1;
    if(ts->fields[field].type!=FT_VARCHAR&&ts->fields[field].type!=FT_NUMERIC)return 1;
    size_t len=0;
    if(c->op==OP_EXISTS||c->op==OP_NOT_EXISTS||op_is_length(c->op))return 1;
    /* Only these dispatch branches form literal bounds. Other operators
       retain their full-leaf producer regardless of pattern length. */
    if(c->op!=OP_EQUAL&&c->op!=OP_NOT_EQUAL&&c->op!=OP_LESS&&c->op!=OP_GREATER&&
       c->op!=OP_LESS_EQ&&c->op!=OP_GREATER_EQ&&c->op!=OP_BETWEEN&&
       c->op!=OP_IN&&c->op!=OP_STARTS_WITH&&c->op!=OP_LIKE)return 1;
    if(c->op==OP_LIKE&&search_criterion_value_len(c)&&search_criterion_value(c)[0]=='%')return 1;
    if(c->op==OP_IN||c->op==OP_NOT_IN){
        for(int i=0;i<c->in_count;i++)
            if(typed_index_text_key_into(ts,(size_t)field,c->in_values[i],strlen(c->in_values[i]),NULL,0,&len)!=1||len>543)return 0;
        return 1;
    }
    if(typed_index_text_key_into(ts,(size_t)field,search_criterion_value(c),search_criterion_value_len(c),NULL,0,&len)!=1||len>543)return 0;
    return c->op!=OP_BETWEEN||
        (typed_index_text_key_into(ts,(size_t)field,search_criterion_value2(c),search_criterion_value2_len(c),NULL,0,&len)==1&&len<=543);
}
int pick_index_for_leaf_schema(const TypedSchema *literal_schema,const char *db_root, const char *object,
                               const SearchCriterion *c) {
    if (!c || !c->field[0]) return -1;
    if(!search_criterion_indexable(literal_schema,c))return -1;
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
    int has_btree   = field_has_index_type(db_root, object, c->field, IT_BTREE) &&
                      ordered_criterion_fits(literal_schema,c);
    int has_bitmap  = field_has_index_type(db_root, object, c->field, IT_BITMAP);
    int has_trigram = field_has_index_type(db_root, object, c->field, IT_TRIGRAM);

    /* §2.4.2: btree exists drives the tier-0 walk + per-record verify.
       Bitmap/trigram cannot enumerate existence (no "any bit set" bitmap
       entry; trigram postings are substring-shaped — and a legacy midnight
       or zero date would ride its value bitmap straight past the
       query-existence predicate), so those full-scan. */
    if (c->op == OP_EXISTS) {
        return (has_btree && btree_idx_exists(db_root, object, c->field,
                                              sch.splits))
               ? IT_BTREE : -1;
    }

    if (op_prefers_trigram(c->op)) {
        size_t plen = strlen(search_criterion_value(c));
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
        size_t plen = strlen(search_criterion_value(c));
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

int pick_index_for_leaf(const char *root,const char *object,const SearchCriterion *criterion){
    return pick_index_for_leaf_schema(load_typed_schema(root,object),root,object,criterion);
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
    int            error;   /* worker-owned; hard read failure errno */
} BmPopcountShardArg;

static void *bm_popcount_one_shard_worker(void *raw) {
    BmPopcountShardArg *a = (BmPopcountShardArg *)raw;
    char bp[1024];
    bm_build_path(bp, sizeof(bp), a->db_root, a->object, a->field, a->shard_idx);
    a->count = 0;
    BitmapShard *bm = NULL;
    int orc = bm_open_read(bp, &bm);
    if (orc != 0) {
        if (orc < 0) a->error = errno;
        return NULL;
    }
    a->count = bm_count(bm, a->value, a->vlen);
    bm_close(bm);
    return NULL;
}

/* SIZE_MAX with errno on a hard bitmap read failure; a shard whose file
   does not exist contributes zero. */
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
            BitmapShard *bm = NULL;
            int orc = bm_open_read(bp, &bm);
            if (orc > 0) continue;
            if (orc < 0) return SIZE_MAX;
            total += bm_count(bm, value, vlen);
            bm_close(bm);
        }
        return total;
    }
    for (int s = 0; s < splits; s++) {
        args[s] = (BmPopcountShardArg){
            .db_root = db_root, .object = object, .field = field,
            .shard_idx = s, .value = value, .vlen = vlen, .count = 0,
            .error = 0,
        };
    }
    parallel_for_io(bm_popcount_one_shard_worker, args, splits, sizeof(BmPopcountShardArg));
    size_t total = 0;
    int error = 0;
    for (int s = 0; s < splits; s++) {
        if (args[s].error && !error) error = args[s].error;
        total += args[s].count;
    }
    free(args);
    if (error) { errno = error; return SIZE_MAX; }
    return total;
}

/* Total bitmap match count for an OP_EQUAL / OP_IN criterion.
   Per-value bitmaps are disjoint by construction (each record's value
   sets exactly one bit across the per-value maps), so summing across
   the IN values gives the exact match count without dedup. SIZE_MAX with
   errno on a hard read failure. */
static size_t bm_popcount_for_crit(const char *db_root,const char *object,
    int splits,const SearchCriterion *crit,const TypedField *tf){
    const TypedSchema *ts=load_typed_schema(db_root,object);
    if(!crit)return 0;
    int count=crit->op==OP_EQUAL?1:crit->op==OP_IN?crit->in_count:0;
    size_t total=0;
    for(int i=0;i<count;i++){
        const char *text=crit->op==OP_EQUAL?search_criterion_value(crit):crit->in_values[i];
        size_t text_len=crit->op==OP_EQUAL?search_criterion_value_len(crit):strlen(text);
        uint8_t *key=NULL;size_t len=0;
        int rc=bitmap_text_key_alloc(ts,tf,text,text_len,g_query_buffer_max_bytes,&key,&len);
        if(rc<0)return SIZE_MAX;
        if(rc==0){free(key);continue;}
        size_t n=bm_popcount_one_value(db_root,object,crit->field,splits,key,len);
        int saved=errno;
        free(key);
        if(n==SIZE_MAX){errno=saved;return SIZE_MAX;}
        if(n>SIZE_MAX-total){errno=EOVERFLOW;return SIZE_MAX;}
        total+=n;
    }
    return total;
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
/* Allocate a bitmap candidate KeySet. With a request budget the exact
   charge is reserved (NULL/ENOBUFS when it does not fit — a hard error for
   prepared find); without one the exact charge is checked against the
   per-query buffer and an over-budget set is the deliberate fallback (NULL
   with errno 0). Allocation failure is NULL with errno set. */
/* Checked-build exit: records the reason and normalizes errno so callers
 * can branch on reason alone (INSERT_CAPACITY keeps ENOBUFS as the
 * diagnostic; deliberate budget/empty/timeout exits present errno 0; hard
 * failures keep their errno). */
static KeySet *bm_build_fail(CandidateBuildOut *out, CandidateBuildFailure r,
                             int err) {
    out->reason = r;
    out->saved_errno = err;
    switch (r) {
    case CBF_INSERT_CAPACITY: errno = ENOBUFS; break;
    case CBF_HARD:            errno = err ? err : EIO; break;
    case CBF_BUDGET:          errno = err; break;
    default:                  errno = 0; break;
    }
    return NULL;
}

/* Candidate KeySet allocation for the bitmap sizing-pass count m:
 * preferred actual slots = next_power_of_two(max(8192, 2*m)) — approved
 * headroom for growth between sizing and enumeration, not a measured
 * optimum or a guarantee against sustained growth. keyset_new doubles its
 * hint, so the preferred hint is max(4096, m), never the slot count.
 * Without a budget the preferred exact charge must fit the per-query cap;
 * if only the floor exceeds it, ordinary sizing is tried once. With a
 * budget the atomic reservation decides: a preferred refusal solely for
 * lack of budget permits one ordinary attempt. ENOMEM or sizing-overflow
 * failures are never retried at the other size. Callers pass the initial
 * sizing-pass count m unchanged (no 1024 substitution). */
static KeySet *bm_candidate_keyset_new(size_t initial_matches,
                                       KeySetBudget *budget,
                                       const char *who) {
    size_t preferred_hint = initial_matches > 4096 ? initial_matches : 4096;
    size_t preferred_bytes = 0, ordinary_bytes = 0;
    if (keyset_allocation_bytes(preferred_hint, &preferred_bytes) != 0 ||
        keyset_allocation_bytes(initial_matches, &ordinary_bytes) != 0)
        return NULL;

    KeySet *ks = NULL;
    size_t chosen_hint = preferred_hint;
    if (budget) {
        ks = keyset_new_budgeted(preferred_hint, budget);
        if (!ks && errno == ENOBUFS && ordinary_bytes != preferred_bytes) {
            chosen_hint = initial_matches;
            ks = keyset_new_budgeted(chosen_hint, budget);
        }
    } else {
        if (preferred_bytes > g_query_buffer_max_bytes) {
            if (ordinary_bytes > g_query_buffer_max_bytes) {
                LOG_WARN(LOG_SUB_QUERY,
                         "%s: keyset for estimate %zu exceeds query buffer cap %zu",
                         who, initial_matches, g_query_buffer_max_bytes);
                errno = 0;
                return NULL;
            }
            chosen_hint = initial_matches;
        }
        ks = keyset_new(chosen_hint);
    }
    if (!ks) { if (!errno) errno = ENOMEM; return NULL; }
#ifdef TEST_BUILD
    BMKS_STAT_ALLOC(ks->cap);
    if (atomic_load(&g_shard_test_bmlock_log))
        LOG_WARN(LOG_SUB_QUERY,
                 "BMKS-CANDIDATE-ALLOC slots=%zu hint=%zu preferred=%d budget=%d",
                 ks->cap, chosen_hint, chosen_hint == preferred_hint, budget != NULL);
#endif
    return ks;
}

#ifdef TEST_BUILD
KeySet *query_test_candidate_keyset_alloc(size_t m, size_t *out_slots) {
    KeySet *ks = bm_candidate_keyset_new(m, NULL, "query_test_candidate_keyset_alloc");
    if (ks && out_slots) *out_slots = ks->cap;
    if (!ks && out_slots) *out_slots = 0;
    return ks;
}
#endif

static KeySet *build_keyset_from_bitmap(const char *db_root, const char *object,
                                        int splits,
                                        const SearchCriterion *leaf,
                                        const TypedField *tf,
                                        QueryDeadline *dl,
                                        const TypedSchema *literal_schema,
                                        KeySetBudget *budget,
                                        CandidateBuildOut *out) {
    cb_out_init(out);
    errno = 0;
    if (candidate_deadline_expired(dl)) return bm_build_fail(out, CBF_TIMEOUT, 0);
    /* Bitmap serves eq and IN through the fast value-walk; everything
       else (range, LIKE, CONTAINS, REGEX, len_X, exists, i-variants,
       NEQ, NOT_IN) goes through the generic dict-scan path below —
       matches btree parity: any indexed op routes through index files,
       never falls back to a data-shard scan. */
    if (leaf->op != OP_EQUAL && leaf->op != OP_IN) {
        Schema sc_g = load_schema(db_root, object);
        SlotcaskSchemaInfo info_g = {
            .splits = sc_g.splits, .slot_size = sc_g.slot_size, .streams = sc_g.streams,
        };
        SlotcaskDb *sdb_g SDB_REG_REF = slotcask_registry_get(db_root, object, &info_g);
        if (!sdb_g) {
            int saved = errno ? errno : EIO;
            LOG_WARN(LOG_SUB_SLOTCASK, "build_keyset_from_bitmap: slotcask_registry_get failed for %s/%s", db_root, object);
            return bm_build_fail(out, CBF_HARD, saved);
        }

        size_t tot_g = bm_popcount_generic_for_crit(db_root, object,
                                                     leaf->field, splits,
                                                     (SearchCriterion *)leaf, tf,
                                                     literal_schema);
        if (tot_g == SIZE_MAX)
            return bm_build_fail(out, CBF_HARD, errno);

        KeySet *ks_g = bm_candidate_keyset_new(tot_g, budget,
                                               "build_keyset_from_bitmap");
        if (!ks_g) {
            int saved = errno;
            return bm_build_fail(out, !saved || saved == ENOBUFS ? CBF_BUDGET : CBF_HARD,
                                 saved);
        }
        bm_ks_race_barrier(BMKS_RACE_GENERIC, tot_g, ks_g->cap);

        BmKsEmitCtx kec = { ks_g, dl, 0, CBF_NONE };
        for (int s = 0; s < splits; s++) {
            if (dl && dl->timed_out) {
                keyset_free(ks_g);
                return bm_build_fail(out, CBF_TIMEOUT, 0);
            }
            if (bitmap_emit_generic_for_shard(db_root, object, leaf->field, s,
                                           (SearchCriterion *)leaf, tf,
                                           bm_ks_insert_cb, &kec, sdb_g,
                                           literal_schema,
                                           g_query_buffer_max_bytes / (size_t)(splits > 0 ? splits : 1)) < 0) {
                int saved = errno;
                keyset_free(ks_g);
                return bm_build_fail(out, CBF_HARD, saved);
            }
            /* The callback stop is not an error channel: check the
               recorded reason explicitly (hard beats capacity). */
            if (kec.reason != CBF_NONE) {
                CandidateBuildFailure r = kec.reason;
                keyset_free(ks_g);
                return bm_build_fail(out, r, kec.error);
            }
        }
        if (dl && dl->timed_out) {
            keyset_free(ks_g);
            return bm_build_fail(out, CBF_TIMEOUT, 0);
        }
        return ks_g;
    }

    /* Encode every query value up-front so we can both pre-count and
       walk later without re-encoding. n_vals = 1 for OP_EQUAL, in_count
       for OP_IN. Bool/byte/numeric values all fit in <= 8B; even
       varchar bitmaps (future enum-text) cap at 1024 here. */
    int n_vals = (leaf->op == OP_EQUAL) ? 1 : leaf->in_count;
    if (n_vals <= 0) return bm_build_fail(out, CBF_NONE, 0);
    uint8_t **vals = calloc((size_t)n_vals, sizeof(*vals));
    size_t *vlens = calloc((size_t)n_vals, sizeof(*vlens));
    if (!vals || !vlens) {
        int saved = errno ? errno : ENOMEM;
        LOG_ERROR(LOG_SUB_QUERY, "build_keyset_from_bitmap: calloc failed for n_vals=%d", n_vals);
        bitmap_values_clear(vals,n_vals); free(vlens);
        return bm_build_fail(out, CBF_HARD, saved);
    }
    if (leaf->op == OP_EQUAL) {
        if(bitmap_text_key_alloc(literal_schema,tf,search_criterion_value(leaf),search_criterion_value_len(leaf),
             g_query_buffer_max_bytes/(size_t)n_vals,&vals[0],&vlens[0])<0){
            int saved = errno ? errno : EILSEQ;
            bitmap_values_clear(vals,n_vals);free(vlens);
            return bm_build_fail(out, CBF_HARD, saved);
        }
    } else {
        for (int i = 0; i < n_vals; i++) {
            if(bitmap_text_key_alloc(literal_schema,tf,leaf->in_values[i],strlen(leaf->in_values[i]),
                 g_query_buffer_max_bytes/(size_t)n_vals,&vals[i],&vlens[i])<0){
                int saved = errno ? errno : EILSEQ;
                bitmap_values_clear(vals,n_vals);free(vlens);
                return bm_build_fail(out, CBF_HARD, saved);
            }
        }
    }
    /* Drop any zero-length encodings (e.g. NULL/empty); they'd cause
       bm_count/bm_walk to return 0/0 anyway but the explicit check
       keeps the inner loop branchless. */
    int n_kept = 0;
    for (int i = 0; i < n_vals; i++) {
        if (vals[i] == NULL) continue; /* rc==0, distinct from a present empty key */
        if (n_kept != i) {
            vals[n_kept]=vals[i]; vals[i]=NULL;
            vlens[n_kept] = vlens[i];
        }
        n_kept++;
    }
    if (n_kept == 0) {
        bitmap_values_clear(vals,n_vals); free(vlens);
        return bm_build_fail(out, CBF_NONE, 0);   /* deliberate empty */
    }

    /* Need a slotcask handle to read kf entries. */
    Schema sc = load_schema(db_root, object);
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size, .streams = sc.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        int saved = errno ? errno : EIO;
        LOG_WARN(LOG_SUB_SLOTCASK, "build_keyset_from_bitmap: slotcask_registry_get failed for %s/%s", db_root, object);
        bitmap_values_clear(vals,n_vals); free(vlens);
        return bm_build_fail(out, CBF_HARD, saved);
    }

    /* Pass A: sum bm_count across every (shard × value). Each bm_count
       is a cache-friendly stride-byte popcount — ~ms-scale at 25M /
       128 shards even cold. Worth it to size the keyset right.
       Per-value bitmaps are disjoint by construction, so the sum is
       exact match cardinality even for IN. */
    size_t total_matches = 0;
    for (int s = 0; s < splits; s++) {
        if (candidate_deadline_expired(dl)) {
            bitmap_values_clear(vals,n_vals); free(vlens);
            return bm_build_fail(out, CBF_TIMEOUT, 0);
        }
        char bp[1024];
        bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
        BitmapShard *bm = NULL;
        int orc = bm_open_read(bp, &bm);
        if (orc > 0) continue;             /* never-written shard: empty */
        if (orc < 0) {
            int saved = errno;
            bitmap_values_clear(vals,n_vals); free(vlens);
            return bm_build_fail(out, CBF_HARD, saved);
        }
        for (int i = 0; i < n_kept; i++) {
            total_matches += bm_count(bm, vals[i], vlens[i]);
        }
        bm_close(bm);
    }

    /* Exact admission: the actual rounded keys + state arrays (not a hint
       estimate) are charged before allocation, so monster keysets fall
       through to scan (or fail a budgeted request) instead of being
       admitted over the per-query cap. */
    KeySet *ks = bm_candidate_keyset_new(total_matches,
                                         budget, "build_keyset_from_bitmap");
    if (!ks) {
        int saved = errno;
        bitmap_values_clear(vals,n_vals); free(vlens);
        return bm_build_fail(out, !saved || saved == ENOBUFS ? CBF_BUDGET : CBF_HARD,
                                 saved);
    }
    bm_ks_race_barrier(BMKS_RACE_EQ, total_matches, ks->cap);

    /* Pass B: walk the bitmaps and lift matching hashes via kf lookup.
       Serial across shards — the inserts themselves run lock-free
       (keyset_insert uses per-bucket CAS) so a parallel-walk would be
       safe, but kfcache_acquire / page faults on cold kf are the real
       cost and that doesn't trivially parallelise. For each shard we
       walk every value's bitmap into the same keyset; duplicates are
       impossible (values are distinct → bitmaps disjoint) so no extra
       check needed. */
    for (int s = 0; s < splits; s++) {
        if (candidate_deadline_expired(dl)) {
            keyset_free(ks); bitmap_values_clear(vals,n_vals); free(vlens);
            return bm_build_fail(out, CBF_TIMEOUT, 0);
        }

        char kfp[PATH_MAX];
        slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0) {
            int saved = errno ? errno : EIO;
            if (saved == ENOENT) continue;
            keyset_free(ks); bitmap_values_clear(vals,n_vals); free(vlens);
            return bm_build_fail(out, CBF_HARD, saved);
        }

        char bp[1024];
        bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
        BitmapShard *bm = NULL;
        int orc = bm_open_read(bp, &bm);
        if (orc != 0) {
            int saved = errno;
            kfcache_release(&kh);
            if (orc > 0) continue;         /* never-written shard: empty */
            keyset_free(ks); bitmap_values_clear(vals,n_vals); free(vlens);
            return bm_build_fail(out, CBF_HARD, saved);
        }

        BmCollectCtx c = { kh.map, kh.capacity, ks, dl, 0, CBF_NONE };
        for (int i = 0; i < n_kept && c.reason == CBF_NONE; i++) {
            bm_walk(bm, vals[i], vlens[i], bm_collect_to_keyset_cb, &c);
        }

        bm_close(bm);
        kfcache_release(&kh);
        if (c.reason != CBF_NONE) {
            /* Matches grew since sizing (capacity) or the deadline hit:
               discard the whole collection instead of returning a
               truncated set. */
            keyset_free(ks); bitmap_values_clear(vals,n_vals); free(vlens);
            return bm_build_fail(out, c.reason, c.error);
        }
    }
    bitmap_values_clear(vals,n_vals); free(vlens);
    if (dl && dl->timed_out) {
        keyset_free(ks);
        return bm_build_fail(out, CBF_TIMEOUT, 0);
    }
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
    if (cc->collect->error) return 1;
    if (cc->collect->dl && cc->collect->dl->timed_out) return 1;
    for (int t = 0; t < cc->nt; t++) {
        if (vlen == cc->tvlens[t] && memcmp(value, cc->tvals[t], vlen) == 0)
            return 0;  /* target value — excluded from the complement */
    }
    bm_walk(cc->bm, value, vlen, bm_collect_to_keyset_cb, cc->collect);
    /* A nested failure stops the dictionary iteration too. */
    return cc->collect->reason != CBF_NONE ? 1 : 0;
}

static KeySet *build_keyset_bitmap_complement(const char *db_root, const char *object,
                                              int splits, const SearchCriterion *leaf,
                                              QueryDeadline *dl,
                                              const uint8_t (*tvals)[1024],
                                              const size_t *tvlens, int nt,
                                              size_t comp_hint,
                                              KeySetBudget *budget,
                                              CandidateBuildOut *out) {
    cb_out_init(out);
    errno = 0;
    if (candidate_deadline_expired(dl)) return bm_build_fail(out, CBF_TIMEOUT, 0);

    Schema sc = load_schema(db_root, object);
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size, .streams = sc.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "build_keyset_bitmap_complement: slotcask_registry_get failed for %s/%s", db_root, object);
        return bm_build_fail(out, CBF_HARD, errno ? errno : EIO);
    }

    KeySet *ks = bm_candidate_keyset_new(comp_hint,
                                         budget, "build_keyset_bitmap_complement");
    if (!ks) {
        int saved = errno;
        return bm_build_fail(out, !saved || saved == ENOBUFS ? CBF_BUDGET : CBF_HARD,
                             saved);
    }
    bm_ks_race_barrier(BMKS_RACE_COMPLEMENT, comp_hint, ks->cap);

    for (int s = 0; s < splits; s++) {
        if (candidate_deadline_expired(dl)) {
            keyset_free(ks);
            return bm_build_fail(out, CBF_TIMEOUT, 0);
        }
        char kfp[PATH_MAX];
        slotcask_kf_path(kfp, sizeof(kfp), sdb->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0) {
            int saved = errno ? errno : EIO;
            if (saved == ENOENT) continue;
            keyset_free(ks);
            return bm_build_fail(out, CBF_HARD, saved);
        }

        char bp[1024];
        bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
        BitmapShard *bm = NULL;
        int orc = bm_open_read(bp, &bm);
        if (orc != 0) {
            int saved = errno;
            kfcache_release(&kh);
            if (orc > 0) continue;         /* never-written shard: empty */
            keyset_free(ks);
            return bm_build_fail(out, CBF_HARD, saved);
        }

        BmCollectCtx c = { kh.map, kh.capacity, ks, dl, 0, CBF_NONE };
        BmComplementCtx cc = { bm, tvals, tvlens, nt, &c };
        bm_iter_values(bm, bm_complement_value_cb, &cc);

        bm_close(bm);
        kfcache_release(&kh);
        if (c.reason != CBF_NONE) {
            keyset_free(ks);
            return bm_build_fail(out, c.reason, c.error);
        }
    }
    if (dl && dl->timed_out) {
        keyset_free(ks);
        return bm_build_fail(out, CBF_TIMEOUT, 0);
    }
    return ks;
}

/* For a bitmap-indexed eq/IN leaf, build whichever of {match-set, complement}
 * is smaller, so the streaming top-N prefilter never materialises the
 * majority side. *inverted=1 ⇒ the complement was built and the caller counts
 * a row when its hash is NOT in the set. Returns NULL on budget/error so the
 * caller falls back rather than counting unfiltered. */
KeySet *build_smaller_bitmap_keyset_ck(const char *db_root, const char *object,
                                              int splits, const SearchCriterion *leaf,
                                              const TypedField *tf, QueryDeadline *dl,
                                              int *inverted, KeySetBudget *budget,
                                              CandidateBuildOut *out) {
    cb_out_init(out);
    if (candidate_deadline_expired(dl)) return bm_build_fail(out, CBF_TIMEOUT, 0);
    const TypedSchema *literal_schema=load_typed_schema(db_root,object);
    *inverted = 0;
    errno = 0;
    if (!bitmap_literal_fits(literal_schema,leaf,1024))
        return build_keyset_from_bitmap(db_root,object,splits,leaf,tf,dl,literal_schema,budget,out);
    if (!tf || (leaf->op != OP_EQUAL && leaf->op != OP_IN))
        return build_keyset_from_bitmap(db_root, object, splits, leaf, tf, dl,literal_schema,budget,out);

    int n_vals = (leaf->op == OP_EQUAL) ? 1 : leaf->in_count;
    if (n_vals <= 0)
        return build_keyset_from_bitmap(db_root, object, splits, leaf, tf, dl,literal_schema,budget,out);

    uint8_t (*tvals)[1024] = calloc((size_t)n_vals, sizeof(*tvals));
    size_t  *tvlens = calloc((size_t)n_vals, sizeof(*tvlens));
    if (!tvals || !tvlens) {
        LOG_ERROR(LOG_SUB_QUERY, "build_smaller_bitmap_keyset: calloc failed for n_vals=%d", n_vals);
        int saved = errno ? errno : ENOMEM;
        free(tvals); free(tvlens);
        return bm_build_fail(out, CBF_HARD, saved);
    }
    int nt = 0;
    for (int i = 0; i < n_vals; i++) {
        const char *v = (leaf->op == OP_EQUAL) ? search_criterion_value(leaf) : leaf->in_values[i];
        if (encode_criterion_value_checked(literal_schema, tf, v, strlen(v),
                                             tvals[nt], sizeof(tvals[nt]),
                                             &tvlens[nt]) < 0) {
            int saved = errno ? errno : EILSEQ;
            free(tvals); free(tvlens);
            return bm_build_fail(out, CBF_HARD, saved);
        }
        if (tvlens[nt] > 0) nt++;
    }
    if (nt == 0) {
        free(tvals); free(tvlens);
        return build_keyset_from_bitmap(db_root, object, splits, leaf, tf, dl,literal_schema,budget,out);
    }

    /* Sum bm_count across shards — cheap stride popcount per (shard, value).
       A hard read failure fails the build: no complement estimate may be
       derived from a partial count. */
    size_t matches = 0;
    for (int s = 0; s < splits; s++) {
        if (dl && dl->timed_out) {
            free(tvals); free(tvlens);
            return bm_build_fail(out, CBF_TIMEOUT, 0);
        }
        char bp[1024];
        bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
        BitmapShard *bm = NULL;
        int orc = bm_open_read(bp, &bm);
        if (orc > 0) continue;
        if (orc < 0) {
            int saved = errno;
            free(tvals); free(tvlens);
            return bm_build_fail(out, CBF_HARD, saved);
        }
        for (int i = 0; i < nt; i++) matches += bm_count(bm, tvals[i], tvlens[i]);
        bm_close(bm);
    }
    int total = get_live_count(db_root, object);
    size_t comp = ((size_t)total > matches) ? (size_t)total - matches : 0;

    KeySet *ks;
    if (matches <= comp) {
        ks = build_keyset_from_bitmap(db_root, object, splits, leaf, tf, dl,literal_schema,budget,out);
    } else {
        ks = build_keyset_bitmap_complement(db_root, object, splits, leaf, dl,
                                            (const uint8_t (*)[1024])tvals,
                                            tvlens, nt, comp, budget, out);
        if (ks) *inverted = 1;
    }
    free(tvals); free(tvlens);
    return ks;
}

KeySet *build_smaller_bitmap_keyset(const char *db_root, const char *object,
                                    int splits, const SearchCriterion *leaf,
                                    const TypedField *tf, QueryDeadline *dl,
                                    int *inverted) {
    CandidateBuildOut out;
    KeySet *ks = build_smaller_bitmap_keyset_ck(db_root, object, splits, leaf,
                                                tf, dl, inverted, NULL, &out);
    if (!ks) errno = out.reason == CBF_HARD ? out.saved_errno
                    : out.reason == CBF_INSERT_CAPACITY ? ENOBUFS : 0;
    return ks;
}

/* Collect hashes from a single trigram's posting list (range scan
   on the .tg btree shards for key=trigram). Hashes land in a fresh
   KeySet allocated by the caller. */
typedef struct {
    KeySet        *ks;
    QueryDeadline *dl;
    int            timed_out;
    CandidateBuildFailure reason;
} TgCollectCtx;

static int tg_collect_to_keyset_cb(const char *value, size_t vlen,
                                   const uint8_t hash[16], void *ctx) {
    (void)value; (void)vlen;
    TgCollectCtx *c = (TgCollectCtx *)ctx;
    if (c->reason != CBF_NONE) return 1;
    if (c->dl && c->dl->timed_out) { c->timed_out = 1; c->reason = CBF_TIMEOUT; return 1; }
    if (keyset_insert(c->ks, hash) < 0) {
        c->reason = CBF_INSERT_CAPACITY;
        return 1;
    }
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
    if (!out) {
        LOG_ERROR(LOG_SUB_QUERY, "keyset_pairwise_intersect: keyset_new(%zu) failed", keyset_size(small));
        return NULL;
    }
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
    CandidateBuildFailure reason;
} TgFilterCtx;

static int tg_filter_cb(const char *value, size_t vlen,
                        const uint8_t hash[16], void *ctx) {
    (void)value; (void)vlen;
    TgFilterCtx *c = (TgFilterCtx *)ctx;
    if (c->reason != CBF_NONE) return 1;
    if (c->dl && c->dl->timed_out) { c->timed_out = 1; c->reason = CBF_TIMEOUT; return 1; }
    if (keyset_contains(c->running, hash)) {
        if (keyset_insert(c->next, hash) < 0) c->reason = CBF_INSERT_CAPACITY;
    }
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
                                      KeySet *running, QueryDeadline *dl,
                                      KeySetBudget *budget,
                                      CandidateBuildOut *out) {
    if (!running || keyset_size(running) == 0) return running;
    int idx_n = index_splits_for(splits);

    /* Result size ≤ |running|. Size to fit. */
    KeySet *next = keyset_new_budgeted(keyset_size(running), budget);
    if (!next) {
        int saved = errno;
        LOG_ERROR(LOG_SUB_QUERY, "tg_intersect_streaming: keyset_new(%zu) failed", keyset_size(running));
        keyset_free(running);
        return bm_build_fail(out, saved == ENOBUFS ? CBF_BUDGET : CBF_HARD, saved);
    }
    TgFilterCtx c = { running, next, dl, 0, CBF_NONE };
    for (int s = 0; s < idx_n; s++) {
        char tp[PATH_MAX];
        tg_build_path(tp, sizeof(tp), db_root, object, field, s);
        btree_range(tp, (const char *)trigram, 3,
                    (const char *)trigram, 3,
                    tg_filter_cb, &c);
        if (c.reason != CBF_NONE) {
            keyset_free(running); keyset_free(next);
            return bm_build_fail(out, c.reason, 0);
        }
    }
    keyset_free(running);
    return next;
}

static KeySet *build_keyset_from_trigram_b(const char *db_root, const char *object,
                                           int splits,
                                           const SearchCriterion *leaf,
                                           QueryDeadline *dl,
                                           KeySetBudget *budget,
                                           CandidateBuildOut *out) {
    cb_out_init(out);
    if (candidate_deadline_expired(dl)) return bm_build_fail(out, CBF_TIMEOUT, 0);
    if (!leaf) return bm_build_fail(out, CBF_NONE, 0);
    size_t plen = strlen(search_criterion_value(leaf));
    if (plen < 3) return bm_build_fail(out, CBF_NONE, 0);

    uint8_t pattern_lc[1024];
    if (plen > sizeof(pattern_lc)) plen = sizeof(pattern_lc);
    /* Lowercase the pattern — index stores lowercase trigrams, so both
       OP_CONTAINS and OP_ICONTAINS land on the same posting lists.
       Per-record verify enforces final case-sensitivity. */
    for (size_t i = 0; i < plen; i++) {
        uint8_t c = (uint8_t)search_criterion_value(leaf)[i];
        pattern_lc[i] = (c >= 'A' && c <= 'Z') ? (uint8_t)(c + 32) : c;
    }

    uint8_t trigrams[TG_MAX_DISTINCT][3];
    size_t  n = tg_extract_distinct(pattern_lc, plen, trigrams, TG_MAX_DISTINCT);
    if (n == 0) return bm_build_fail(out, CBF_NONE, 0);

    /* Sort trigrams by posting size ascending — rarest first. The
       seed keyset is the smallest posting list (saves allocation +
       avoids the worst-case bloat); subsequent trigrams stream-
       filter via membership instead of materialising their own full
       keyset. For "alice.smith0" (10 trigrams of varying selectivity),
       this is the difference between materialising 10 × 800k
       keysets vs 1 × tiny + 9 stream walks that mostly early-out. */
    typedef struct { uint8_t tg[3]; size_t count; } TgEntry;
    TgEntry *order = malloc(n * sizeof(TgEntry));
    if (!order) {
        int saved = errno ? errno : ENOMEM;
        LOG_ERROR(LOG_SUB_QUERY, "build_keyset_from_trigram: malloc failed for n=%zu TgEntry", n);
        return bm_build_fail(out, CBF_HARD, saved);
    }
    for (size_t i = 0; i < n; i++) {
        memcpy(order[i].tg, trigrams[i], 3);
        order[i].count = tg_posting_count(db_root, object, leaf->field,
                                          splits, trigrams[i], dl);
        if (candidate_deadline_expired(dl)) {
            free(order);
            return bm_build_fail(out, CBF_TIMEOUT, 0);
        }
        /* Any zero-posting trigram → intersection is empty. */
        if (order[i].count == 0) {
            KeySet *empty = keyset_new_budgeted(16, budget);
            if (!empty) {
                int saved = errno;
                free(order);
                return bm_build_fail(out, saved == ENOBUFS ? CBF_BUDGET : CBF_HARD,
                                     saved);
            }
            free(order);
            return empty;
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
    KeySet *acc = keyset_new_budgeted(order[0].count, budget);
    if (!acc) {
        int saved = errno;
        LOG_ERROR(LOG_SUB_QUERY, "build_keyset_from_trigram: keyset_new(%zu) failed", (size_t)order[0].count);
        free(order);
        return bm_build_fail(out, saved == ENOBUFS ? CBF_BUDGET : CBF_HARD, saved);
    }
    int idx_n = index_splits_for(splits);
    TgCollectCtx cc = { acc, dl, 0, CBF_NONE };
    for (int s = 0; s < idx_n; s++) {
        char tp[PATH_MAX];
        tg_build_path(tp, sizeof(tp), db_root, object, leaf->field, s);
        btree_range(tp, (const char *)order[0].tg, 3,
                    (const char *)order[0].tg, 3,
                    tg_collect_to_keyset_cb, &cc);
        if (cc.reason != CBF_NONE) {
            keyset_free(acc); free(order);
            return bm_build_fail(out, cc.reason, 0);
        }
    }

    /* Streaming intersect against each subsequent trigram. Early-exit
       once running is empty. */
    for (size_t i = 1; i < n && keyset_size(acc) > 0; i++) {
        acc = tg_intersect_streaming(db_root, object, leaf->field, splits,
                                     order[i].tg, acc, dl, budget, out);
        if (!acc) { free(order); return NULL; }
    }
    free(order);
    return acc;
}

KeySet *build_keyset_from_trigram(const char *db_root, const char *object,
                                  int splits, const SearchCriterion *leaf,
                                  QueryDeadline *dl) {
    CandidateBuildOut o;
    KeySet *ks = build_keyset_from_trigram_b(db_root, object, splits, leaf, dl,
                                             NULL, &o);
    if (!ks) errno = o.reason == CBF_HARD ? o.saved_errno
                    : o.reason == CBF_INSERT_CAPACITY ? ENOBUFS : 0;
    return ks;
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
    size_t plen = strlen(search_criterion_value(seed));
    if (plen < 3) return 0;

    uint8_t gram[3];
    for (int i = 0; i < 3; i++) {
        uint8_t c = (uint8_t)search_criterion_value(seed)[i];
        gram[i] = (c >= 'A' && c <= 'Z') ? (uint8_t)(c + 32) : c;
    }

    size_t posting_count = tg_posting_count(db_root, object, seed->field,
                                            sch->splits, gram, dl);
    if (dl->timed_out) return 0;
    if (posting_count == 0) return 0;

    KeySet *ks = keyset_new(posting_count);
    if (!ks) return 0;

    int idx_n = index_splits_for(sch->splits);
    TgCollectCtx cc = { .ks = ks, .dl = dl };
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

#ifdef TEST_BUILD
/* Deterministic allocation-failure injection for per-leaf KeySet builds
   (same precedent as g_bulk_upd_test_fail_alloc). Atomic arming so the
   parallel workers cannot race the countdown. */
static int g_keyset_leaf_test_fail_alloc;

void build_keyset_leaf_test_set_fail_alloc(int fail_n) {
    __atomic_store_n(&g_keyset_leaf_test_fail_alloc, fail_n, __ATOMIC_RELAXED);
}
#endif

KeySet *build_keyset_from_leaf_ck(const char *db_root, const char *object,
                                  int splits, SearchCriterion *leaf,
                                  QueryDeadline *dl, KeySetBudget *budget,
                                  CandidateBuildOut *out) {
    cb_out_init(out);
    errno=0;
    if (candidate_deadline_expired(dl)) return bm_build_fail(out, CBF_TIMEOUT, 0);
    if (!leaf) return bm_build_fail(out, CBF_NONE, 0);
    /* Dispatch on the plan-time picker's choice — single index per leaf,
       no runtime cascade between index types.  If the picker says
       IT_TRIGRAM and the trigram builder returns NULL (e.g. transient
       alloc failure), caller drops to full scan rather than retrying
       via btree.  The picker has already considered all available
       indexes for this (field, op, pattern-length) tuple. */
    int picked = pick_index_for_leaf(db_root, object, leaf);
    if (picked < 0) return bm_build_fail(out, CBF_NONE, 0);   /* not indexed */
    if (picked == IT_TRIGRAM) {
        return build_keyset_from_trigram_b(db_root, object, splits, leaf, dl,
                                           budget, out);
    }
    if (picked == IT_BITMAP) {
        TypedSchema *ts = load_typed_schema(db_root, object);
        const TypedField *tf = resolve_idx_field(ts, leaf->field);
        return build_keyset_from_bitmap(db_root, object, splits,
                                        leaf, tf, dl, ts, budget, out);
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
#ifdef TEST_BUILD
        /* Armed countdown (atomic): the worker whose tick reaches 0 sees
           the allocation fail with ENOMEM. */
        if (__atomic_load_n(&g_keyset_leaf_test_fail_alloc, __ATOMIC_ACQUIRE) > 0 &&
            __atomic_sub_fetch(&g_keyset_leaf_test_fail_alloc, 1,
                               __ATOMIC_ACQ_REL) == 0) {
            errno = ENOMEM;
            LOG_ERROR(LOG_SUB_QUERY, "build_keyset_from_leaf: injected alloc failure (tier %d)", t);
            return bm_build_fail(out, CBF_HARD, ENOMEM);
        }
#endif
        KeySet *ks = keyset_new_budgeted(hints[t], budget);
        if (!ks) {
            int saved = errno;
            LOG_ERROR(LOG_SUB_QUERY, "build_keyset_from_leaf: keyset_new(%zu) failed (tier %d)", hints[t], t);
            return bm_build_fail(out, saved == ENOBUFS ? CBF_BUDGET : CBF_HARD,
                                 saved);
        }
        IntersectCollectCtx c = { ks, dl, 0, 0 };
        if(btree_dispatch_checked(db_root, object, leaf->field, splits,
                       leaf, tf, intersect_collect_cb, &c)<0){
            int saved = errno;
            keyset_free(ks);
            return bm_build_fail(out, CBF_HARD, saved);
        }
        if (dl->timed_out) {
            keyset_free(ks);
            return bm_build_fail(out, CBF_TIMEOUT, 0);
        }
        if (!c.overflowed) return ks;
        keyset_free(ks);
        /* Tier-1 overflowed; loop retries at tier 2.  Tier-2 overflow
           exits the loop as a budget-class fallback. */
    }
    return bm_build_fail(out, CBF_BUDGET, 0);
}

KeySet *build_keyset_from_leaf(const char *db_root, const char *object,
                               int splits, SearchCriterion *leaf,
                               QueryDeadline *dl) {
    CandidateBuildOut o;
    KeySet *ks = build_keyset_from_leaf_ck(db_root, object, splits, leaf, dl,
                                          NULL, &o);
    if (!ks) errno = o.reason == CBF_HARD ? o.saved_errno
                    : o.reason == CBF_INSERT_CAPACITY ? ENOBUFS : 0;
    return ks;
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
    KeySetBudget     *budget;     /* request budget (NULL = unbudgeted) */
#ifdef TEST_BUILD
    int               test_race_stage;
    pthread_t         test_owner;
    int               test_offthread;
    int               test_fault_site;
    int               test_fault_error;
    unsigned          test_fault_consumed;
#endif
    CandidateBuildOut worker_out; /* storage; bout points here */
    CandidateBuildOut *bout;      /* passed to build_keyset_from_leaf_b */
    int               err;        /* captured errno when slot stayed NULL;
                                     0 = success or the historical
                                     budget-fallback NULL. Slots are
                                     disjoint per worker — no lock. */
} LeafBuildArg;

static void *leaf_build_worker(void *arg) {
    LeafBuildArg *a = arg;
#ifdef TEST_BUILD
    int previous_stage = atomic_load(&bm_ks_race_armed);
    if (a->test_race_stage)
        atomic_store(&bm_ks_race_armed, a->test_race_stage);
    a->test_offthread = !pthread_equal(pthread_self(), a->test_owner);
    if (a->test_fault_error) {
        BmTestLockFaultState previous_faults;
        bm_test_lock_fault_save(&previous_faults);
        bm_test_reset_lock_failure();
        bm_test_fail_lock_next(a->test_fault_site, a->test_fault_error);
        char path[1024];
        bm_build_path(path, sizeof(path), a->db_root, a->object, a->leaf->field, 0);
        BitmapShard *probe = bm_open(path, 0, 0, 0, 0, 0);
        int saved = errno;
        a->test_fault_consumed = (unsigned)bm_test_lock_failure_consumed(a->test_fault_site);
        bm_test_lock_fault_restore(&previous_faults);
        if (!probe) {
            *a->slot = bm_build_fail(a->bout, CBF_HARD, saved);
            a->err = errno;
            atomic_store(&bm_ks_race_armed, previous_stage);
            return NULL;
        }
        bm_close(probe);
    }
#endif
    errno = 0;
    *a->slot = build_keyset_from_leaf_ck(a->db_root, a->object, a->splits,
                                        a->leaf, a->deadline, a->budget,
                                        a->bout);
    a->err = *a->slot ? 0 : errno;
    if (!*a->slot && a->bout->reason == CBF_NONE && a->err) {
        a->bout->reason = CBF_HARD;
        a->bout->saved_errno = a->err;
    }
#ifdef TEST_BUILD
    atomic_store(&bm_ks_race_armed, previous_stage);
#endif
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
    if (keyset_insert(ic->out, hash) < 0) {
        LOG_WARN(LOG_SUB_QUERY, "intersect_all_cb: destination keyset capacity exhausted");
        return -1;
    }
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
KeySet *intersect_indexed_leaves_ck(const char *db_root, const char *object,
                                    int splits, SearchCriterion **leaves,
                                    int n, QueryDeadline *dl,
                                    int *out_small_primary,
                                    KeySetBudget *budget,
                                    CandidateBuildOut *out) {
    cb_out_init(out);
    errno=0;
    *out_small_primary = 0;
    if (n < 2) return bm_build_fail(out, CBF_NONE, 0);

    /* Parallel build per-leaf KeySets when n > 2. Nested parallel_for is
       structurally safe (work-stealing in parallel.c). For n == 2 the
       parallel build doesn't beat serial seed+probe because the final
       intersect step touches |seed| × 1 lookup, roughly cancelling the
       saved second walk. */
    if (n > 2 && parallel_pool_size() > 0) {
        /* Parallel build per-leaf sets, then walk smallest and keep entries
           present in all others. */
        /* Per-leaf argument/result arrays are candidate memory too: charged
           to the request budget before allocation, released with them. */
        size_t arr_bytes = (size_t)n * (sizeof(KeySet *) + sizeof(LeafBuildArg));
        if (keyset_budget_reserve(budget, arr_bytes) != 0)
            return bm_build_fail(out, CBF_BUDGET, errno);
        KeySet **per_leaf = calloc((size_t)n, sizeof(KeySet *));
        LeafBuildArg *args = calloc((size_t)n, sizeof(LeafBuildArg));
        if (!per_leaf || !args) {
            LOG_ERROR(LOG_SUB_QUERY, "intersect_indexed_leaves: calloc failed for n=%d leaves", n);
            int saved = errno ? errno : ENOMEM;
            free(per_leaf); keyset_budget_release(budget, arr_bytes); free(args);
            return bm_build_fail(out, CBF_HARD, saved);
        }
        for (int i = 0; i < n; i++) {
            args[i].db_root  = db_root;
            args[i].object   = object;
            args[i].splits   = splits;
            args[i].leaf     = leaves[i];
            args[i].slot     = &per_leaf[i];
            args[i].deadline = dl;
            args[i].budget   = budget;
            args[i].bout     = &args[i].worker_out;
#ifdef TEST_BUILD
            args[i].test_owner = pthread_self();
            if (bm_ks_parallel_stage &&
                strcmp(args[i].leaf->field, bm_ks_parallel_field) == 0)
                args[i].test_race_stage = bm_ks_parallel_stage;
            if (bm_ks_parallel_fault_error &&
                strcmp(args[i].leaf->field, bm_ks_parallel_fault_field) == 0) {
                args[i].test_fault_site = bm_ks_parallel_fault_site;
                args[i].test_fault_error = bm_ks_parallel_fault_error;
            }
#endif
        }
#ifdef TEST_BUILD
        int observe_parallel = bm_ks_parallel_stage != 0;
        bm_ks_parallel_stage = 0;
        bm_ks_parallel_field[0] = '\0';
        bm_ks_parallel_fault_error = 0;
#endif
        parallel_for(leaf_build_worker, args, n, sizeof(LeafBuildArg));
#ifdef TEST_BUILD
        if (observe_parallel) {
            bm_ks_parallel_stats[0] = (unsigned)n;
            bm_ks_parallel_stats[1] = (unsigned)n;
            for (int i = 0; i < n; i++) {
                bm_ks_parallel_stats[2] += args[i].worker_out.reason == CBF_INSERT_CAPACITY;
                bm_ks_parallel_stats[3] += args[i].worker_out.reason == CBF_HARD;
                bm_ks_parallel_stats[4] += args[i].test_offthread;
                bm_ks_parallel_stats[5] += args[i].test_fault_consumed;
            }
        }
#endif
        /* Carry the worker's captured errno through the join: the worker
           thread's thread-local errno dies with it, and losing it made
           worker allocation failures indistinguishable from an empty
           intersection for errno-checked callers. The err slots are
           disjoint per worker — no lock needed. errno stays 0 only for
           the budget-fallback NULL, the historical scan-fallback signal. */
        int worker_err = 0;
        for (int i = 0; i < n; i++)
            if (!worker_err && args[i].err) worker_err = args[i].err;

        if (dl->timed_out) {
            free(args);
            for (int i = 0; i < n; i++) keyset_free(per_leaf[i]);
            free(per_leaf); keyset_budget_release(budget, arr_bytes);
            return NULL;
        }
        for (int i = 0; i < n; i++) {
            if (!per_leaf[i]) {
                /* Build failed for this leaf — fold the worst reason
                   (hard beats insertion-capacity beats budget), surface
                   its errno, clean up, bail. */
                CandidateBuildFailure r = CBF_NONE;
                for (int k = 0; k < n; k++) {
                    CandidateBuildFailure kr = args[k].bout->reason;
                    if (kr == CBF_HARD || r == CBF_NONE) r = kr;
                    else if (r != CBF_HARD && kr == CBF_INSERT_CAPACITY)
                        r = CBF_INSERT_CAPACITY;
                }
                int saved = worker_err;
                for (int k = 0; k < n; k++)
                    if (args[k].worker_out.reason == r &&
                        args[k].worker_out.saved_errno) {
                        saved = args[k].worker_out.saved_errno;
                        break;
                    }
                free(args);
                for (int k = 0; k < n; k++) keyset_free(per_leaf[k]);
                free(per_leaf); keyset_budget_release(budget, arr_bytes);
                return bm_build_fail(out, r, saved);
            }
        }

        free(args);
        int smallest_i = 0;
        for (int i = 1; i < n; i++) {
            if (keyset_size(per_leaf[i]) < keyset_size(per_leaf[smallest_i]))
                smallest_i = i;
        }

        if (keyset_size(per_leaf[smallest_i]) < INTERSECT_MIN_PRIMARY) {
            *out_small_primary = 1;
            KeySet *result = per_leaf[smallest_i];
            for (int i = 0; i < n; i++) if (i != smallest_i) keyset_free(per_leaf[i]);
            free(per_leaf); keyset_budget_release(budget, arr_bytes);
            return result;
        }

        KeySet *result = keyset_new_budgeted(keyset_size(per_leaf[smallest_i]),
                                             budget);
        if (!result) {
            int saved = errno;
            LOG_ERROR(LOG_SUB_QUERY, "intersect_indexed_leaves: keyset_new(%zu) failed", keyset_size(per_leaf[smallest_i]));
            for (int i = 0; i < n; i++) keyset_free(per_leaf[i]);
            free(per_leaf); keyset_budget_release(budget, arr_bytes);
            return bm_build_fail(out, saved == ENOBUFS ? CBF_BUDGET : CBF_HARD,
                                 saved);
        }
        IntersectAllCtx ic = { per_leaf, n, smallest_i, result, dl, 0 };
        keyset_iter(per_leaf[smallest_i], intersect_all_cb, &ic);

        for (int i = 0; i < n; i++) keyset_free(per_leaf[i]);
        free(per_leaf); keyset_budget_release(budget, arr_bytes);
        if (dl->timed_out) { keyset_free(result); return NULL; }
        return result;
    }

    /* Serial path (n == 2 or pool unavailable). */
    KeySet *running = build_keyset_from_leaf_ck(db_root, object, splits, leaves[0],
                                               dl, budget, out);
    if (!running) return NULL;   /* reason and errno already recorded in out */
    if (dl->timed_out) {
        keyset_free(running);
        return bm_build_fail(out, CBF_TIMEOUT, 0);
    }

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

        /* Source and result coexist at their combined charge; the superseded
           running set is freed as soon as the probe finishes. */
        KeySet *next = keyset_new_budgeted(cur, budget);  /* ≤ |running| */
        if (!next) {
            int saved = errno;
            LOG_ERROR(LOG_SUB_QUERY, "intersect_indexed_leaves: keyset_new(%zu) failed (serial path)", cur);
            keyset_free(running);
            return bm_build_fail(out, saved == ENOBUFS ? CBF_BUDGET : CBF_HARD,
                                 saved);
        }

        IntersectProbeCtx p = { running, next, dl, 0 };
        if(btree_dispatch_checked(db_root, object, leaves[i]->field, splits,
                       leaves[i], resolve_idx_field(ts, leaves[i]->field),
                       intersect_probe_cb, &p)<0){
            int saved = errno;
            keyset_free(running); keyset_free(next);
            return bm_build_fail(out, CBF_HARD, saved);
        }

        keyset_free(running);
        running = next;
        if (dl->timed_out) {
            keyset_free(running);
            return bm_build_fail(out, CBF_TIMEOUT, 0);
        }
    }
    return running;
}

KeySet *intersect_indexed_leaves(const char *db_root, const char *object,
                                 int splits, SearchCriterion **leaves, int n,
                                 QueryDeadline *dl, int *out_small_primary) {
    CandidateBuildOut o;
    KeySet *ks = intersect_indexed_leaves_ck(db_root, object, splits, leaves, n,
                                            dl, out_small_primary, NULL, &o);
    if (!ks) errno = o.reason == CBF_HARD ? o.saved_errno
                    : o.reason == CBF_INSERT_CAPACITY ? ENOBUFS : 0;
    return ks;
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
    if (kc->count >= kc->cap) {
        LOG_ERROR(LOG_SUB_QUERY, "keyset_to_batch_cb: count >= cap (%zu) — internal invariant violation", kc->cap);
        return -1;  /* shouldn't happen — cap = keyset_size */
    }
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
    if (!entries) {
        LOG_ERROR(LOG_SUB_QUERY, "keyset_to_collected_hashes: malloc failed for cap=%zu", cap);
        return -1;
    }
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
    int predicate_error;
    _Atomic CandidateBuildFailure reason;
} OrChildWorkerCtx;

/* btree callback — drops every hit into the shared KeySet. Returns -1 to
   halt the btree walk on deadline trip, on capacity exhaustion (insert
   failure → O(cap) probe per call if we kept trying), OR when the caller-
   supplied target_count has been reached (limit pushdown for find). */
static int or_collect_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    (void)val; (void)vlen;
    OrChildWorkerCtx *w = (OrChildWorkerCtx *)ctx;
    if (w->reason != CBF_NONE) return -1;
    if (query_deadline_tick(w->deadline, &w->dl_counter)) {
        CandidateBuildFailure expected = CBF_NONE;
        atomic_compare_exchange_strong_explicit(&w->reason, &expected,
                                                 CBF_TIMEOUT,
                                                 memory_order_relaxed,
                                                 memory_order_relaxed);
        return -1;
    }
    if (bm_ks_counted_insert(w->ks, hash16) < 0) {
        LOG_WARN(LOG_SUB_QUERY, "or_collect_cb: destination keyset capacity exhausted");
        w->reason = CBF_INSERT_CAPACITY;
        return -1;
    }
    if (w->target_count > 0 &&
        atomic_load_explicit(&w->ks->n, memory_order_relaxed) >= (size_t)w->target_count) {
        return -1;
    }
    return 0;
}

static void *or_child_worker(void *arg) {
    OrChildWorkerCtx *w = (OrChildWorkerCtx *)arg;
    TypedSchema *ts = load_typed_schema(w->db_root, w->object);
    if(btree_dispatch_checked(w->db_root, w->object, w->leaf->field, w->splits,
                   (SearchCriterion *)w->leaf,
                   resolve_idx_field(ts, w->leaf->field),
                   or_collect_cb, w)<0){
        w->predicate_error=errno?errno:EILSEQ;
        w->reason = CBF_HARD;
    }
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
KeySet *build_or_keyset_ck(const char *db_root, const char *object, int splits,
                           const CriteriaNode *or_node, QueryDeadline *dl,
                           int *out_budget_exceeded, int target_count,
                           KeySetBudget *budget, CandidateBuildOut *out) {
    cb_out_init(out);
    errno=0;
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

    size_t ks_bytes = 0;
    if (keyset_allocation_bytes(est_total, &ks_bytes) != 0)
        return bm_build_fail(out, CBF_HARD, errno);
    if (ks_bytes > g_query_buffer_max_bytes) {
        LOG_WARN(LOG_SUB_QUERY,
                 "build_or_keyset: actual keyset size %zu exceeds query buffer cap %zu",
                 ks_bytes, g_query_buffer_max_bytes);
        if (out_budget_exceeded) *out_budget_exceeded = 1;
        return bm_build_fail(out, CBF_BUDGET, 0);
    }
    KeySet *ks = keyset_new_budgeted(est_total, budget);
    if (!ks) {
        int saved = errno;
        LOG_ERROR(LOG_SUB_QUERY, "build_or_keyset: keyset_new(%zu) failed", est_total);
        return bm_build_fail(out, saved == ENOBUFS ? CBF_BUDGET : CBF_HARD, saved);
    }

    bm_ks_race_barrier(BMKS_RACE_OR, est_total, ks->cap);
    size_t ctx_bytes = (size_t)n * sizeof(OrChildWorkerCtx);
    if (keyset_budget_reserve(budget, ctx_bytes) != 0) {
        keyset_free(ks);
        return bm_build_fail(out, CBF_BUDGET, ENOBUFS);
    }
    OrChildWorkerCtx *ctxs = calloc(n, sizeof(OrChildWorkerCtx));
    if (!ctxs) {
        keyset_budget_release(budget, ctx_bytes);
        keyset_free(ks);
        return bm_build_fail(out, CBF_HARD, ENOMEM);
    }
    for (int i = 0; i < n; i++) {
        CriteriaNode *c = or_node->children[i];
        ctxs[i].db_root = db_root;
        ctxs[i].object = object;
        ctxs[i].splits = splits;
        ctxs[i].leaf = &c->leaf;
        ctxs[i].ks = ks;
        ctxs[i].deadline = dl;
        ctxs[i].target_count = target_count;
        atomic_init(&ctxs[i].reason, CBF_NONE);
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

    int error=0;
    CandidateBuildFailure reason = CBF_NONE;
    for(int i=0;i<n;i++){
        if(ctxs[i].predicate_error)error=ctxs[i].predicate_error;
        /* Hard beats insertion-capacity beats timeout. */
        CandidateBuildFailure kr = ctxs[i].reason;
        if (kr == CBF_HARD || reason == CBF_NONE) reason = kr;
        else if (reason != CBF_HARD && kr == CBF_INSERT_CAPACITY)
            reason = CBF_INSERT_CAPACITY;
    }
    free(ctxs);
    keyset_budget_release(budget, ctx_bytes);
    if(error){
        keyset_free(ks);
        return bm_build_fail(out, reason == CBF_NONE ? CBF_HARD : reason, error);
    }
    if (reason != CBF_NONE) {
        keyset_free(ks);
        return bm_build_fail(out, reason, 0);
    }
    if (dl && dl->timed_out) {
        keyset_free(ks);
        return bm_build_fail(out, CBF_TIMEOUT, 0);
    }
    return ks;
}

KeySet *build_or_keyset(const char *db_root, const char *object, int splits,
                        const CriteriaNode *or_node, QueryDeadline *dl,
                        int *out_budget_exceeded, int target_count) {
    CandidateBuildOut o;
    KeySet *ks = build_or_keyset_ck(db_root, object, splits, or_node, dl,
                                   out_budget_exceeded, target_count, NULL, &o);
    if (!ks) errno = o.reason == CBF_HARD ? o.saved_errno
                    : o.reason == CBF_INSERT_CAPACITY ? ENOBUFS : 0;
    return ks;
}


/* (legacy KeysetRecordRead / read_record_by_hash / release_record_read removed
   2026-05-07 — superseded by RecordRef + read_record_ref/release_record_ref in
   the storage-version-agnostic dispatch helpers above.) */

/* ---- keyset_emit_find test counters and fault seams (TEST_BUILD only) ---- */
#ifdef TEST_BUILD
static struct {
    _Atomic long calls, fetch_calls, fetch_hashes, copies, copy_bytes,
                 live_copies, live_bytes, peak_bytes;
} g_kef_stats;
static _Atomic long g_kef_seam_at[KEF_SEAM_COUNT];   /* 0 = disarmed; n = fire on the n-th hit */
static _Atomic long g_kef_seam_hits[KEF_SEAM_COUNT];
static _Atomic long g_kef_budget_override;           /* > 0: record-copy budget in bytes */

void kef_test_reset(void) {
    atomic_store(&g_kef_stats.calls, 0);       atomic_store(&g_kef_stats.fetch_calls, 0);
    atomic_store(&g_kef_stats.fetch_hashes, 0); atomic_store(&g_kef_stats.copies, 0);
    atomic_store(&g_kef_stats.copy_bytes, 0);  atomic_store(&g_kef_stats.live_copies, 0);
    atomic_store(&g_kef_stats.live_bytes, 0);  atomic_store(&g_kef_stats.peak_bytes, 0);
    for (int i = 0; i < KEF_SEAM_COUNT; i++) {
        atomic_store(&g_kef_seam_at[i], 0); atomic_store(&g_kef_seam_hits[i], 0);
    }
    atomic_store(&g_kef_budget_override, 0);
}
void kef_test_arm(int seam, long arg) {
    if (seam == KEF_SEAM_BUDGET) { atomic_store(&g_kef_budget_override, arg); return; }
    if (seam <= 0 || seam >= KEF_SEAM_COUNT) return;
    atomic_store(&g_kef_seam_hits[seam], 0);
    atomic_store(&g_kef_seam_at[seam], arg);
}
void kef_test_stats(KefTestStats *o) {
    o->calls = atomic_load(&g_kef_stats.calls);             o->fetch_calls = atomic_load(&g_kef_stats.fetch_calls);
    o->fetch_hashes = atomic_load(&g_kef_stats.fetch_hashes); o->copies = atomic_load(&g_kef_stats.copies);
    o->copy_bytes = atomic_load(&g_kef_stats.copy_bytes);   o->live_copies = atomic_load(&g_kef_stats.live_copies);
    o->live_bytes = atomic_load(&g_kef_stats.live_bytes);   o->peak_bytes = atomic_load(&g_kef_stats.peak_bytes);
}
static int kef_seam_take(int seam) {
    long at = atomic_load(&g_kef_seam_at[seam]);
    if (at <= 0) return 0;
    long hit = atomic_fetch_add(&g_kef_seam_hits[seam], 1) + 1;
    if (hit != at) return 0;
    atomic_store(&g_kef_seam_at[seam], 0);              /* one-shot */
    return 1;
}
static void kef_stat_copy(size_t bytes) {
    atomic_fetch_add(&g_kef_stats.copies, 1);
    atomic_fetch_add(&g_kef_stats.copy_bytes, (long)bytes);
    atomic_fetch_add(&g_kef_stats.live_copies, 1);
    long now = atomic_fetch_add(&g_kef_stats.live_bytes, (long)bytes) + (long)bytes;
    long peak = atomic_load(&g_kef_stats.peak_bytes);
    while (peak < now && !atomic_compare_exchange_weak(&g_kef_stats.peak_bytes, &peak, now)) {}
}
static void kef_stat_release(size_t bytes) {
    atomic_fetch_sub(&g_kef_stats.live_copies, 1);
    atomic_fetch_sub(&g_kef_stats.live_bytes, (long)bytes);
}
#define KEF_SEAM(s)          kef_seam_take(s)
#define KEF_STAT_ADD(f, v)   ((void)atomic_fetch_add(&g_kef_stats.f, (long)(v)))
#define KEF_STAT_COPY(b)     kef_stat_copy(b)
#define KEF_STAT_RELEASE(b)  kef_stat_release(b)
#else
#define KEF_SEAM(s)          0
#define KEF_STAT_ADD(f, v)   ((void)0)
#define KEF_STAT_COPY(b)     ((void)0)
#define KEF_STAT_RELEASE(b)  ((void)0)
#endif

#define KEF_BATCH 1024
_Static_assert(KEF_BATCH <= 65536, "KefSortEnt.idx is uint16_t");

typedef struct { uint8_t hash[16]; uint16_t idx; } KefSortEnt;

/* One reusable fetch batch. Candidate slots 0..n-1 are in KeySet bucket order. */
typedef struct {
    uint8_t      hashes[KEF_BATCH][16];
    KefSortEnt   sorted[KEF_BATCH];     /* same hashes sorted, for kef_batch_cb's lookup */
    uint8_t     *rec[KEF_BATCH];        /* owned copy: key bytes then value bytes */
    size_t       klen[KEF_BATCH];
    size_t       vlen[KEF_BATCH];
    uint8_t      hdr2[KEF_BATCH];
    uint8_t      found[KEF_BATCH];
    size_t       n;
    KeySetBudget budget;                /* live record-copy bytes of this batch */
    _Atomic int  error;                 /* first callback failure: ENOBUFS or ENOMEM */
} KefBatch;

static int kef_sort_cmp(const void *a, const void *b) {
    return memcmp(((const KefSortEnt *)a)->hash, ((const KefSortEnt *)b)->hash, 16);
}

static void kef_set_error(KefBatch *b, int e) {
    int expect = 0;
    atomic_compare_exchange_strong(&b->error, &expect, e);
}

static size_t kef_copy_budget(void) {
#ifdef TEST_BUILD
    long o = atomic_load(&g_kef_budget_override);
    if (o > 0) return (size_t)o;
#endif
    return g_query_buffer_max_bytes;
}

/* Fetch callback. Runs on fetch-pool threads, possibly concurrently for
   different kf shards: every candidate owns its slot and shared state is
   atomic. A hash repeats only within one partition (one thread), so the
   found[] repeat check needs no lock. Copies the borrowed record; never
   re-enters slotcask/btree (a kf reader is held). */
static int kef_batch_cb(const uint8_t hash16[16], const void *key, size_t klen,
                        const void *value, size_t vlen, uint8_t hdr2, void *ctx_ptr) {
    KefBatch *b = (KefBatch *)ctx_ptr;
    if (atomic_load_explicit(&b->error, memory_order_relaxed)) return 1;
    size_t lo = 0, hi = b->n, slot = SIZE_MAX;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int c = memcmp(hash16, b->sorted[mid].hash, 16);
        if (c == 0) { slot = b->sorted[mid].idx; break; }
        if (c < 0) hi = mid; else lo = mid + 1;
    }
    if (slot == SIZE_MAX || b->found[slot]) return 0;   /* not ours, or a repeat: first copy wins */
    if (vlen > SIZE_MAX - klen) { kef_set_error(b, ENOBUFS); return 1; }
    size_t bytes = klen + vlen;
    if (keyset_budget_reserve(&b->budget, bytes) != 0) { kef_set_error(b, ENOBUFS); return 1; }
    uint8_t *p = KEF_SEAM(KEF_SEAM_COPY_ALLOC) ? NULL : malloc(bytes ? bytes : 1);
    if (!p) { keyset_budget_release(&b->budget, bytes); kef_set_error(b, ENOMEM); return 1; }
    if (klen) memcpy(p, key, klen);
    if (vlen) memcpy(p + klen, value, vlen);
    b->rec[slot] = p; b->klen[slot] = klen; b->vlen[slot] = vlen; b->hdr2[slot] = hdr2;
    b->found[slot] = 1;
    KEF_STAT_COPY(bytes);
    return 0;
}

/* Free this batch's copies and reset it for the next collection. */
static void kef_batch_release(KefBatch *b) {
    for (size_t i = 0; i < b->n; i++) {
        if (b->rec[i]) {
            size_t bytes = b->klen[i] + b->vlen[i];
            KEF_STAT_RELEASE(bytes);
            keyset_budget_release(&b->budget, bytes);
            free(b->rec[i]);
            b->rec[i] = NULL;
        }
        b->found[i] = 0;
    }
    b->n = 0;
    atomic_store_explicit(&b->error, 0, memory_order_relaxed);
}

/* Walk a pre-built KeySet in bucket order, one fetch batch at a time: fetch
   up to KEF_BATCH candidates, filter (exclusions, rematch, joins), paginate
   and emit them, free the batch, and fetch the next batch only while the
   page is not full. Holds at most one batch of record copies, charged to
   QUERY_BUFFER_MB; a batch that does not fit is retried at half size, and a
   single record over the budget fails with ENOBUFS. Returns rows printed, or
   -1 with errno on any fetch/allocation/record/join/emit failure (rows
   already printed stay printed). A deadline stop returns the rows printed.
   Used by the OR-union, AND-intersection, trigram, prepared-bitmap and
   indexed-leaf find paths; caller owns the KeySet. */
static int keyset_emit_find(const char *db_root, const char *object,
                            const Schema *sch, KeySet *ks,
                            CriteriaNode *tree_for_rematch,  /* NULL = skip */
                            ExcludedKeys *excluded, int offset, int limit,
                            const char **proj_fields, int proj_count,
                            FieldSchema *fs, int rows_fmt, int dict_fmt, char csv_delim,
                            JoinSpec *joins, int njoins, QueryDeadline *dl) {
    KEF_STAT_ADD(calls, 1);
    int need_rematch = (tree_for_rematch != NULL);
    CriteriaNode *tree = tree_for_rematch;

    size_t cursor = 0;                       /* next KeySet bucket to collect from */
    while (cursor < ks->cap && ks->state[cursor] != 2) cursor++;
    if (cursor == ks->cap) return 0;         /* no candidate: no allocation, no registry */

    KefBatch *bt = KEF_SEAM(KEF_SEAM_SETUP_ALLOC) ? NULL : calloc(1, sizeof(*bt));
    if (!bt) { errno = ENOMEM; return -1; }
    bt->budget.limit = kef_copy_budget();

    SlotcaskSchemaInfo sinfo = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    errno = 0;
    SlotcaskDb *sdb SDB_REG_REF = KEF_SEAM(KEF_SEAM_REGISTRY)
        ? (errno = EIO, (SlotcaskDb *)NULL)
        : slotcask_registry_get(db_root, object, &sinfo);
    if (!sdb) { int e = errno ? errno : EIO; free(bt); errno = e; return -1; }

    const Schema *kschema = fs && fs->auto_key != AK_NONE ? &fs->auto_key_schema_snapshot : NULL;
    size_t batch_cap = KEF_BATCH;            /* halved on budget overflow; never grows back */
    int printed = 0, count = 0, dl_counter = 0, error = 0, stop = 0;

    while (!stop && !error && cursor < ks->cap) {
        if (dl && dl->timeout_ms &&
            atomic_load_explicit(&dl->timed_out, memory_order_relaxed)) break;
        size_t next = cursor;
        bt->n = 0;
        while (next < ks->cap && bt->n < batch_cap) {
            if (ks->state[next] == 2) {
                memcpy(bt->hashes[bt->n], ks->keys[next], 16);
                memcpy(bt->sorted[bt->n].hash, ks->keys[next], 16);
                bt->sorted[bt->n].idx = (uint16_t)bt->n;
                bt->n++;
            }
            next++;
        }
        if (bt->n == 0) break;               /* only empty buckets remain */
        qsort(bt->sorted, bt->n, sizeof(bt->sorted[0]), kef_sort_cmp);

        KEF_STAT_ADD(fetch_calls, 1);
        KEF_STAT_ADD(fetch_hashes, bt->n);
        int frc;
        if (KEF_SEAM(KEF_SEAM_FETCH)) { errno = EIO; frc = -1; }
        else {
            errno = 0;
            frc = slotcask_bulk_resolve_and_fetch(sdb, (const uint8_t (*)[16])bt->hashes,
                                                  bt->n, bt, kef_batch_cb);
        }
        int ferr = errno;
        if (KEF_SEAM(KEF_SEAM_TIMEOUT) && dl)
            atomic_store_explicit(&dl->timed_out, 1, memory_order_relaxed);

        int cerr = atomic_load_explicit(&bt->error, memory_order_relaxed);
        size_t used = bt->n;
        if (cerr == ENOBUFS && used > 1) {   /* copies do not fit: retry these candidates at half size */
            kef_batch_release(bt);
            batch_cap = used / 2;
            continue;
        }
        if (cerr || frc < 0) {
            error = cerr ? cerr : (ferr ? ferr : EIO);
            kef_batch_release(bt);
            break;
        }
        cursor = next;

        for (size_t i = 0; i < bt->n; i++) {
            if (limit > 0 && printed >= limit) { stop = 1; break; }
            if (query_deadline_tick(dl, &dl_counter)) { stop = 1; break; }
            if (!bt->found[i]) continue;
            char keybuf[1100];
            if (format_wire_key(kschema, (const char *)bt->rec[i], bt->klen[i],
                                keybuf, sizeof(keybuf)) < 0) { error = EILSEQ; break; }
            if (is_excluded(excluded, keybuf)) continue;
            const uint8_t *raw = bt->rec[i] + bt->klen[i];
            size_t value_len = bt->vlen[i];
            uint8_t hdr2 = bt->hdr2[i];
            int matched = 1;
            if (need_rematch &&
                criteria_match_tree_checked(raw, value_len, tree, fs, hdr2, &matched)) {
                error = errno ? errno : EILSEQ; break;
            }
            if (!matched) continue;
            RecordRef *refs = NULL; int dropped = 0;
            size_t charge = (size_t)njoins * sizeof(*refs);
            if (njoins) {
                if (charge > g_query_buffer_max_bytes) { error = ENOBUFS; break; }
                refs = calloc((size_t)njoins, sizeof(*refs));
                if (!refs) { error = ENOMEM; break; }
                for (int j = 0; j < njoins; j++) {
                    int found = KEF_SEAM(KEF_SEAM_JOIN) ? (errno = EIO, -1)
                        : resolve_join_record(&joins[j], db_root, fs->ts, raw, value_len, hdr2,
                                              g_query_buffer_max_bytes - charge, &refs[j]);
                    if (found < 0) { error = errno ? errno : EILSEQ; break; }
                    if (!found && joins[j].type == JOIN_INNER) { dropped = 1; break; }
                }
            }
            if (!error && !dropped && ++count > offset) {
                if (typed_query_emit_row(g_out ? g_out : stdout, keybuf, strlen(keybuf), raw,
                        value_len, hdr2, fs, proj_fields, proj_count, rows_fmt, dict_fmt,
                        csv_delim, printed > 0, joins, njoins, refs,
                        g_query_buffer_max_bytes - charge))
                    error = errno ? errno : EILSEQ;
                else printed++;
            }
            if (refs) { for (int j = 0; j < njoins; j++) release_record_ref(&refs[j]); free(refs); }
            if (error) break;
        }
        if (limit > 0 && printed >= limit) stop = 1;
        kef_batch_release(bt);
    }
    free(bt);
    if (error) { errno = error; return -1; }
    return printed;
}

#ifdef TEST_BUILD
/* TEST_BUILD only: the previous whole-candidate-set implementation, kept verbatim as the differential reference for test_kef_batches.c. */

typedef struct {
    uint8_t (*hashes)[16];
    size_t   n;
    uint8_t **keys; size_t *klens;
    uint8_t **vals; size_t *vlens;
    uint8_t *hdr2s;             /* per-record generation byte */
    int     *found;
} KefRefFetchCtx;

static int kef_ref_fetch_cb(const uint8_t hash16[16],
                         const void *key, size_t klen,
                         const void *value, size_t vlen,
                         uint8_t hdr2, void *ctx_ptr) {
    KefRefFetchCtx *c = (KefRefFetchCtx *)ctx_ptr;
    for (size_t i = 0; i < c->n; i++) {
        if (memcmp(hash16, c->hashes[i], 16) == 0) {
            c->keys[i] = malloc(klen);
            c->hdr2s[i] = hdr2;
            if (c->keys[i]) { memcpy(c->keys[i], key, klen); c->klens[i] = klen; }
            c->vals[i] = malloc(vlen);
            if (c->vals[i]) { memcpy(c->vals[i], value, vlen); c->vlens[i] = vlen; }
            c->found[i] = 1;
            break;
        }
    }
    return 0;
}

static int keyset_emit_find_reference(const char *db_root, const char *object,
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
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &sinfo);
    if (!sdb) {
        free(hashes);
        free(key_to_fetch);
        return 0;
    }

    /* Batch-fetch results: parallel arrays indexed by hash position. */
    uint8_t **fkeys = calloc(n_fetch, sizeof(*fkeys));
    size_t  *fklens = calloc(n_fetch, sizeof(*fklens));
    uint8_t **fvals = calloc(n_fetch, sizeof(*fvals));
    uint8_t *fhdr2s = calloc(n_fetch, sizeof(*fhdr2s));
    size_t  *fvlens = calloc(n_fetch, sizeof(*fvlens));
    int     *ffound = calloc(n_fetch, sizeof(*ffound));
    if (!fkeys || !fklens || !fvals || !fvlens || !fhdr2s || !ffound) {
        free(hashes); free(key_to_fetch);
        free(fkeys); free(fklens); free(fvals); free(fvlens); free(fhdr2s); free(ffound);
        return 0;
    }

    KefRefFetchCtx fctx = { hashes, n_fetch, fkeys, fklens, fvals, fvlens, fhdr2s, ffound };

#define KEF_REF_BATCH 1024
    {
        size_t processed = 0;
        while (processed < n_fetch) {
            size_t batch = n_fetch - processed;
            if (batch > KEF_REF_BATCH) batch = KEF_REF_BATCH;
            slotcask_bulk_resolve_and_fetch(sdb, hashes + processed,
                                             batch, &fctx, kef_ref_fetch_cb);
            processed += batch;
        }
    }
#undef KEF_REF_BATCH

    /* Phase 2: sequential emit using fetched records. */
    int error=0;
    {
        int count=0,dl_counter=0;
        for(size_t b=0;b<ks->cap&&(limit<=0||printed<limit);b++){
            if(query_deadline_tick(dl,&dl_counter))break;
            if(ks->state[b]!=2)continue;
            int fi=key_to_fetch[b];if(fi<0||!ffound[fi])continue;
            char keybuf[1100];
            const Schema *schema=fs&&fs->auto_key!=AK_NONE?&fs->auto_key_schema_snapshot:NULL;
            if(format_wire_key(schema,(const char*)fkeys[fi],fklens[fi],keybuf,sizeof(keybuf))<0){error=EILSEQ;break;}
            if(is_excluded(excluded,keybuf))continue;
            const uint8_t *raw=fvals[fi];size_t value_len=fvlens[fi];
            int matched=1;
            if(need_rematch&&criteria_match_tree_checked(raw,value_len,tree,fs,fhdr2s[fi],&matched)){
                error=errno?errno:EILSEQ;break;
            }
            if(!matched)continue;
            RecordRef *refs=NULL;int dropped=0;
            size_t charge=(size_t)njoins*sizeof(*refs);
            if(njoins){
                if(charge>g_query_buffer_max_bytes){error=ENOBUFS;break;}
                refs=calloc((size_t)njoins,sizeof(*refs));
                if(!refs){error=ENOMEM;break;}
                for(int i=0;i<njoins;i++){
                    int found=resolve_join_record(&joins[i],db_root,fs->ts,raw,value_len,
                        fhdr2s[fi],g_query_buffer_max_bytes-charge,&refs[i]);
                    if(found<0){error=errno?errno:EILSEQ;break;}
                    if(!found&&joins[i].type==JOIN_INNER){dropped=1;break;}
                }
            }
            if(!error&&!dropped&&++count>offset){
                if(typed_query_emit_row(g_out?g_out:stdout,keybuf,strlen(keybuf),raw,value_len,
                    fhdr2s[fi],fs,proj_fields,proj_count,rows_fmt,dict_fmt,csv_delim,
                    printed>0,joins,njoins,refs,g_query_buffer_max_bytes-charge))
                    error=errno?errno:EILSEQ;
                else printed++;
            }
            if(refs){for(int i=0;i<njoins;i++)release_record_ref(&refs[i]);free(refs);}
            if(error)break;
        }
    }

    /* Cleanup batch-fetched records. */
    for (size_t i = 0; i < n_fetch; i++) {
        free(fkeys[i]); free(fvals[i]);
    }
    free(hashes); free(key_to_fetch);
    free(fkeys); free(fklens); free(fvals); free(fvlens); free(fhdr2s); free(ffound);

    if(error){errno=error;return -1;}
    return printed;
}
#endif

#ifdef TEST_BUILD
int keyset_emit_find_for_test(const char *eff_root, const char *object, KeySet *ks,
        const char *criteria_json, const char *excluded_csv, int offset, int limit,
        const char *format, const char *join_json, uint32_t timeout_ms,
        int use_reference, FILE *out) {
    if (!eff_root || !object || !ks || !out) { errno = EINVAL; return -1; }
    FILE *saved = g_out;
    g_out = out;                       /* setup errors printed via OUT land in `out`, not the TAP stream */
    Schema sch = load_schema(eff_root, object);
    FieldSchema fs;
    init_field_schema(&fs, eff_root, object);
    CriteriaNode *tree = NULL;
    if (criteria_json) {
        const char *perr = NULL;
        char verr[256];
        tree = parse_criteria_tree(criteria_json, &perr);
        if (perr || !tree ||
            validate_criteria_tree_fields(tree, fs.ts, verr, sizeof(verr)) < 0 ||
            compile_criteria_tree_checked(tree, fs.ts, NULL, 0)) {
            free_criteria_tree(tree); g_out = saved; errno = EINVAL; return -1;
        }
    }
    JoinSpec *joins = NULL; int njoins = 0;
    if (join_json && (parse_joins_json(join_json, &joins, &njoins) < 0 ||
                      resolve_joins(joins, njoins, eff_root, object, &fs) < 0)) {
        free_joins(joins, njoins); free_criteria_tree(tree); g_out = saved; errno = EINVAL; return -1;
    }
    ExcludedKeys excluded = parse_excluded_keys(excluded_csv);
    int rows_fmt = format && strcmp(format, "rows") == 0;
    int dict_fmt = format && strcmp(format, "dict") == 0;
    char csv_delim = (format && strcmp(format, "csv") == 0) ? ',' : 0;
    QueryDeadline dl = { now_ms_coarse(), timeout_ms, 0 };
    if (objlock_rdlock(eff_root, object) != 0) {
        free_excluded(&excluded); free_joins(joins, njoins); free_criteria_tree(tree);
        g_out = saved; errno = ENOMEM; return -1;
    }
    int rc = use_reference
        ? keyset_emit_find_reference(eff_root, object, &sch, ks, tree, &excluded, offset, limit,
                                     NULL, 0, &fs, rows_fmt, dict_fmt, csv_delim, joins, njoins, &dl)
        : keyset_emit_find(eff_root, object, &sch, ks, tree, &excluded, offset, limit,
                           NULL, 0, &fs, rows_fmt, dict_fmt, csv_delim, joins, njoins, &dl);
    int e = errno;
    g_out = saved;
    objlock_rdunlock(eff_root, object);
    free_excluded(&excluded); free_joins(joins, njoins); free_criteria_tree(tree);
    errno = e;
    return rc;
}
#endif

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
    if (!ks) return errno?-1:0;
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
    if (!ks) return errno?-1:0;
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
    int predicate_errors[MAX_SPLITS];
    int num_shards;
} OrCountCtx;

static int or_count_batch_cb(const uint8_t hash16[16],
                              const void *key, size_t klen,
                              const void *value, size_t vlen,
                              uint8_t hdr2, void *ctx_ptr) {
    (void)hash16; (void)key; (void)klen;
    OrCountCtx *c = (OrCountCtx *)ctx_ptr;
    if (query_deadline_tick(c->dl, &c->dl_counter)) return 1;
    int matched=0;
    if(criteria_match_tree_checked(value,vlen,c->tree,c->fs,hdr2,&matched)){
        c->predicate_errors[compute_record_shard(hash16,c->num_shards)]=errno?errno:EILSEQ;return 1;}
    if(matched)__sync_fetch_and_add(&c->count,1);
    return 0;
}

/* Count records matching `tree` by iterating a KeySet built from the OR branch.
   For pure-OR trees (root IS the or_node), returns |KeySet| directly.
   For hybrid (AND + OR), re-matches each keyed record against the full tree. */
/* Full-record count with the original compiled tree (recovery for
 * bitmap candidate insertion-capacity exhaustion). Never a candidate route:
 * every live record is fetched and matched against the complete tree.
 * 0 = *out_count valid (dl->timed_out may still be set — callers keep their
 * existing timeout rendering); -1 = real scan error in errno. */
int count_full_record_checked(const char *db_root, const char *object,
                              CriteriaNode *tree, FieldSchema *fs,
                              const Schema *sch, QueryDeadline *dl,
                              size_t *out_count) {
    if (candidate_deadline_expired(dl)) { *out_count = 0; return 0; }
    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { errno = EIO; return -1; }
    int64_t match_count = 0;
    /* fast_cc = NULL: the full tree decides every record. */
    int status = scan_shards_v2_o_direct_match_checked(sdb, fs, NULL, tree,
                                                       dl, &match_count);
    if (status) { errno = EILSEQ; return -1; }
    *out_count = (size_t)match_count;
    return 0;
}

static size_t keyset_count_from_or(const char *db_root, const char *object,
                                   const Schema *sch, CriteriaNode *tree,
                                   CriteriaNode *or_node, FieldSchema *fs,
                                   QueryDeadline *dl, int *out_budget_exceeded) {
    /* Count needs the full union — no short-circuit. */
    CandidateBuildOut cb;
    KeySet *ks = build_or_keyset_ck(db_root, object, sch->splits, or_node, dl,
                                    out_budget_exceeded, 0, NULL, &cb);
    if (!ks && cb.reason == CBF_INSERT_CAPACITY && !candidate_deadline_expired(dl)) {
        /* One-shot whole-record recovery. */
        size_t full = 0;
        if (count_full_record_checked(db_root, object, tree, fs, sch, dl,
                                      &full) != 0) {
            if (!dl->timed_out) return SIZE_MAX;
            return 0;
        }
        BMKS_STAT_FALLBACK("count");
        return full;
    }
    if (!ks) return errno?SIZE_MAX:0;
    if (dl->timed_out) { keyset_free(ks); return 0; }

    /* Every union count validates through the record fetch (which
       revalidates at the Kf reader) — the pure-OR keyset-size shortcut
       is gone; the KeySet is a candidate producer only. */
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
                int matched=0;int status=criteria_match_tree_checked(rr.val,rr.vlen,tree,fs,rr.hdr2,&matched);
                if(status){int error=errno;release_record_ref(&rr);keyset_free(ks);errno=error;return SIZE_MAX;}
                if(matched)n++;
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
        SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &sinfo);
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
        cb_ctx.num_shards=sdb->num_shards;

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
        for(int i=0;i<cb_ctx.num_shards;i++)if(cb_ctx.predicate_errors[i]){errno=cb_ctx.predicate_errors[i];return SIZE_MAX;}
        return cb_ctx.count;
    }
}

/* ========== COUNT with criteria ========== */

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
    if(compile_criteria_tree_checked(tree,fs.ts,NULL,0)){OUT("{\"error\":\"invalid criteria value\"}\n");return;}

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

size_t idx_count_for_leaf(const char *db_root, const char *object,
                          const Schema *sch, const FieldSchema *fs,
                          SearchCriterion *leaf, QueryDeadline *dl);

static int cas_tree_compiled(const CriteriaNode *n) {
    if (n->kind == CNODE_LEAF) {
        if (!n->compiled) return 0;
        if (n->leaf.op == OP_REGEX || n->leaf.op == OP_NOT_REGEX)
            return n->compiled->tf &&
                   n->compiled->tf->type == FT_VARCHAR &&
                   n->compiled->re_compiled;
        return 1;
    }
    for (int i = 0; i < n->n_children; i++)
        if (!cas_tree_compiled(n->children[i])) return 0;
    return 1;
}

/* 1 match, 0 no match, -1 evaluation failure. Regex errors are never
   converted to absence or to a successful negated predicate. */
int cas_match_tree_checked(const uint8_t *rec,size_t len,const CriteriaNode *n,FieldSchema *fs,uint8_t flags){
    if(n&&n->kind==CNODE_LEAF&&n->compiled&&
       (n->compiled->op==OP_REGEX||n->compiled->op==OP_NOT_REGEX)&&!n->compiled->re_compiled){errno=EINVAL;return -1;}
    if(n&&n->kind!=CNODE_LEAF){
        int is_and=n->kind==CNODE_AND;
        for(int i=0;i<n->n_children;i++){int m=cas_match_tree_checked(rec,len,n->children[i],fs,flags);
            if(m<0)return -1;
            if((is_and&&!m)||(!is_and&&m))return m;}
        return is_and;
    }
    int m=0;return criteria_match_tree_checked(rec,len,n,fs,flags,&m)?-1:m;
}

CriteriaNode *cas_prepare_tree(const char *json, TypedSchema *ts,
                               int allow_empty,
                               char *err_buf, size_t err_sz) {
    if (err_buf && err_sz) err_buf[0] = '\0';
    if (!json) return NULL;
    /* Empty array = historical vacuous pass for key-scoped `if`. The
       tree parser rejects it, so catch the exact spelling first. */
    if (allow_empty) {
        const char *p = json;
        while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
        if (*p == '[') {
            p++;
            while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
            if (*p == ']') {
                p++;
                while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
                if (!*p) return NULL;
            }
        }
    }
    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(json, &perr);
    if (!tree) {
        if (err_buf && err_sz)
            snprintf(err_buf, err_sz, "%s", perr ? perr : "parse error");
        return NULL;
    }
    char verr[256];
    if (validate_criteria_tree_fields(tree, ts, verr, sizeof(verr)) < 0) {
        free_criteria_tree(tree);
        if (err_buf && err_sz) snprintf(err_buf, err_sz, "%s", verr);
        return NULL;
    }
    if (!allow_empty && tree->kind == CNODE_AND && tree->n_children == 0) {
        free_criteria_tree(tree);
        if (err_buf && err_sz) snprintf(err_buf, err_sz, "empty criteria");
        return NULL;
    }
    if (compile_criteria_tree_checked(tree,ts,err_buf,err_sz)||!cas_tree_compiled(tree)) {
        free_criteria_tree(tree);
        if (err_buf && err_sz) snprintf(err_buf, err_sz, "criteria compilation failed");
        return NULL;
    }
    return tree;
}

typedef struct {
    CriteriaNode *tree;
    FieldSchema *fs;
    const void *key;
    size_t klen;
    QueryDeadline dl;
    int found, failed;
    uint8_t hashes[64][16];
    size_t nhashes;
} GuardProbe;

static int guard_deadline(GuardProbe *g) {
    if (g->dl.timeout_ms &&
        now_ms_coarse() - g->dl.t0_ms >= g->dl.timeout_ms)
        g->dl.timed_out = 1;
    return g->dl.timed_out;
}

static int guard_record(const uint8_t hash[16], const void *key, size_t klen,
                         const void *value, size_t vlen, uint8_t hdr2,
                         void *ctx) {
    GuardProbe *g = ctx;
    (void)hash;
    if (guard_deadline(g)) return 1;
    TypedRowView view;
    if(typed_row_open(g->fs->ts,value,vlen,hdr2,&view)){g->failed=1;return 1;}
    if (g->key && klen == g->klen && !memcmp(key, g->key, klen)) return 0;
    int matched = cas_match_tree_checked(value, vlen, g->tree, g->fs, hdr2);
    if (matched < 0) { g->failed = 1; return 1; }
    if (matched) { g->found = 1; return 1; }
    return 0;
}

static int guard_collect_hash(const char *v, size_t n,
                               const uint8_t *hash, void *ctx) {
    GuardProbe *g = ctx;
    (void)v; (void)n;
    if (guard_deadline(g) || g->nhashes == 64) return -1;
    memcpy(g->hashes[g->nhashes++], hash, 16);
    return 0;
}

/* Only enumerate a single exact index key when it covers every value
   the typed matcher could call equal. Unsupported encodings scan safely. */
static int guard_eq_key(const CriteriaNode *n, uint8_t out[1032], size_t *len) {
    if (!n || n->kind != CNODE_LEAF || n->leaf.op != OP_EQUAL ||
        !n->compiled || !n->compiled->tf || strchr(n->leaf.field, '+')) return 0;
    const CompiledCriterion *cc = n->compiled;
    const TypedField *f = cc->tf;
    out[0] = 0;
    if(f->type==FT_NUMERIC){
        if(!search_criterion_indexable(cc->schema,&n->leaf))return 0;
        size_t width=0;int field=(int)(f-cc->schema->fields);
        if(typed_index_text_key_into(cc->schema,(size_t)field,search_criterion_value(&n->leaf),search_criterion_value_len(&n->leaf),out+1,1031,&width)!=1)return 0;
        *len=width+1;return 1;
    }
    if (f->type == FT_VARCHAR) {
        if (cc->s1_len > 1031 || cc->s1_len > (size_t)f->size - 2) return 0;
        memcpy(out + 1, cc->s1, cc->s1_len);
        *len = cc->s1_len + 1;
        return 1;
    }
    uint64_t bits; int width;
    switch (f->type) {
    case FT_SHORT:
        if (cc->i1 < INT16_MIN || cc->i1 > INT16_MAX) return 0;
        bits = (uint16_t)cc->i1 ^ UINT16_C(0x8000); width = 2; break;
    case FT_INT:
        if (cc->i1 < INT32_MIN || cc->i1 > INT32_MAX) return 0;
        bits = (uint32_t)cc->i1 ^ UINT32_C(0x80000000); width = 4; break;
    case FT_LONG: case FT_TIMESTAMP: case FT_NUMERIC:
        bits = (uint64_t)cc->i1 ^ (UINT64_C(1) << 63); width = 8; break;
    case FT_BOOL: case FT_BYTE:
        bits = cc->b1; width = 1; break;
    case FT_FLOAT: {
        float v = (float)cc->d1;
        if (!isfinite(v) || v == 0.0f) return 0; /* +/- zero compare equal */
        uint32_t raw; memcpy(&raw, &v, 4);
        bits = raw & UINT32_C(0x80000000) ? ~raw : raw ^ UINT32_C(0x80000000);
        width = 4; break;
    }
    case FT_DOUBLE: {
        double v = cc->d1;
        if (!isfinite(v) || v == 0.0) return 0;
        memcpy(&bits, &v, 8);
        bits = bits & (UINT64_C(1) << 63) ? ~bits : bits ^ (UINT64_C(1) << 63);
        width = 8; break;
    }
    default: return 0;
    }
    for (int i = 0; i < width; i++)
        out[1 + i] = (uint8_t)(bits >> (8 * (width - 1 - i)));
    *len = (size_t)width + 1;
    return 1;
}

/* A leaf under AND is a necessary condition. Never select one OR branch. */
static CriteriaNode *guard_eq_driver(CriteriaNode *n,
                                      const char *root, const char *object, int splits) {
    if (n->kind == CNODE_LEAF) {
        uint8_t key[1032]; size_t len;
        if (guard_eq_key(n, key, &len) && len <= BT_MAX_VAL_LEN &&
            field_index_type(root, object, n->leaf.field) == IT_BTREE &&
            btree_idx_exists(root, object, n->leaf.field, splits)) return n;
        return NULL;
    }
    if (n->kind == CNODE_AND)
        for (int i = 0; i < n->n_children; i++) {
            CriteriaNode *d = guard_eq_driver(n->children[i], root, object, splits);
            if (d) return d;
        }
    return NULL;
}

/* Return 0 proven verdict, -1 failed, -3 timeout. A fallback scan is
   deliberate, not absence inferred from an incomplete candidate search. */
static int guard_has_other(const char *root, const char *object,
                            const Schema *sc, SlotcaskDb *sdb, GuardProbe *g) {
    CriteriaNode *d = guard_eq_driver(g->tree, root, object, sc->splits);
    if (d) {
        uint8_t encoded[1032]; size_t len = 0;
        if (guard_eq_key(d, encoded, &len)) {
            int complete = 1;
            for (int s = 0; s < index_splits_for(sc->splits); s++) {
                if (g->dl.timeout_ms &&
                    now_ms_coarse() - g->dl.t0_ms >= g->dl.timeout_ms) {
                    g->dl.timed_out = 1; return -3;
                }
                char path[PATH_MAX];
                build_idx_path(path, sizeof(path), root, object, d->leaf.field, s);
                g->nhashes = 0;
                int rc = btree_guard_eq(path, (const char *)encoded, len,
                                         guard_collect_hash, g);
                if (guard_deadline(g)) return -3;
                if (rc != 0) { complete = 0; break; }
                /* B-tree handle was released before these record reads. */
                for (size_t i = 0; i < g->nhashes; i++) {
                    rc = slotcask_guard_lookup(sdb, g->hashes[i], guard_record, g);
                    if (rc < 0 || g->failed) return -1;
                    if (guard_deadline(g)) return -3;
                    if (g->found) return 0;
                }
            }
            if (complete) return 0;
        }
    }
    int rc = slotcask_walk_live_strict(sdb, guard_record, g);
    if (guard_deadline(g)) return -3;
    if (rc < 0 || g->failed || (rc == 1 && !g->found)) return -1;
    return 0;
}

#ifdef TEST_BUILD
static pthread_mutex_t g_guard_park_lock = PTHREAD_MUTEX_INITIALIZER;
static shard_db_test_guard_fn g_guard_park_fn = NULL;
static void *g_guard_park_ctx = NULL;

void shard_db_test_set_guard_hook(shard_db_test_guard_fn fn, void *ctx) {
    pthread_mutex_lock(&g_guard_park_lock);
    g_guard_park_fn = fn;
    g_guard_park_ctx = ctx;
    pthread_mutex_unlock(&g_guard_park_lock);
}

void shard_db_test_guard_park(void) {
    shard_db_test_guard_fn fn;
    void *ctx;
    pthread_mutex_lock(&g_guard_park_lock);
    fn = g_guard_park_fn;
    ctx = g_guard_park_ctx;
    g_guard_park_fn = NULL;
    g_guard_park_ctx = NULL;
    pthread_mutex_unlock(&g_guard_park_lock);
    if (fn) fn(ctx);
}
#endif

int cas_guard_request(const char *db_root, const char *object,
                      const char *guard_json,
                      const char *key, size_t klen,
                      int require_match,
                      int *out_pass, size_t *out_matched) {
    *out_pass = 0; *out_matched = 0;
    Schema sc = load_schema(db_root, object);
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) return -2;
    char err[256];
    CriteriaNode *tree = cas_prepare_tree(guard_json, ts, 0, err, sizeof(err));
    if (!tree) return -2;
    /* Server supplied the exact stored key bytes, normalized while its
       shared objlock is held. auto_key_normalize is server.c-local. */
    const char *use_key = key; size_t use_klen = klen;
    if (use_klen > (size_t)sc.max_key) {
        free_criteria_tree(tree); return -2;
    }
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size, .streams = sc.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb || !slotcask_write_reservation_held(sdb)) {
        free_criteria_tree(tree); return -1;
    }
    FieldSchema fs; init_field_schema(&fs, db_root, object);
    GuardProbe g = { .tree = tree, .fs = &fs, .key = use_key, .klen = use_klen,
                    .dl = { now_ms_coarse(), resolve_timeout_ms(), 0 } };
    int rc = guard_has_other(db_root, object, &sc, sdb, &g);
    free_criteria_tree(tree);
    if (rc != 0) return rc;
    *out_matched = g.found ? 1 : 0;
    *out_pass = require_match ? g.found : !g.found;
#ifdef TEST_BUILD
    shard_db_test_guard_park();
#endif
    return 0;
}


/* Checked idx_count_for_leaf: bitmap/trigram candidate build reasons are
 * visible to the caller so insertion-capacity can recover without
 * masquerading as a hard error. */
size_t idx_count_for_leaf_ck(const char *db_root, const char *object,
                             const Schema *sch, const FieldSchema *fs,
                             SearchCriterion *leaf, QueryDeadline *dl,
                             CandidateBuildOut *out);

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
    if(compile_criteria_tree_checked(tree,fs.ts,NULL,0)){OUT("{\"error\":\"invalid criteria value\"}\n");return 1;}

    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };

    /* No count shortcuts: exists/not_exists/bool-in/metadata complements
       all bypassed per-record NULL matching (a NULL field is neither a
       positive nor a negative value match). Every predicate count routes
       through the planner + generation-correct per-record verification,
       same as find. Empty criteria keeps its O(1) metadata path. */

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

        if (is_single_leaf) {
            CandidateBuildOut cb;
            size_t count = idx_count_for_leaf_ck(db_root, object, &sch,
                                                 &fs, pc, &dl, &cb);
            if ((cb.reason == CBF_INSERT_CAPACITY ||
             (cb.reason == CBF_BUDGET && cb.saved_errno == 0)) &&
            !candidate_deadline_expired(&dl)) {
                /* One-shot whole-record recovery. */
                if (count_full_record_checked(db_root, object, tree, &fs,
                                              &sch, &dl, &count) != 0 &&
                    !dl.timed_out) {
                    OUT("{\"error\":\"invalid typed record\"}\n");
                    return -1;
                }
                BMKS_STAT_FALLBACK("count");
            }
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else if(count==SIZE_MAX)OUT("{\"error\":\"invalid typed record\"}\n");
                    else OUT("%zu\n", count);
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
                                                          tree, &fs, &dl, &fp);
                    if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
                    else if(count==SIZE_MAX)OUT("{\"error\":\"invalid typed record\"}\n");
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
            if(btree_dispatch_checked(db_root, object, pc->field, sch.splits,
                           pc, pc_tf, collect_hash_cb, &cc)<0){collect_ctx_destroy(&cc);OUT("{\"error\":\"invalid index criterion\"}\n");return -1;}

            if (cc.budget_exceeded) {
                LOG_WARN(LOG_SUB_QUERY, "cmd_count: query buffer cap exceeded");
                OUT(QUERY_BUFFER_ERR);
                collect_ctx_destroy(&cc);
                return -1;
            }
            size_t count = parallel_indexed_count(db_root, object, &sch,
                                                  cc.entries, (int)cc.count,
                                                  tree, &fs, &dl, &fp);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else if(count==SIZE_MAX)OUT("{\"error\":\"invalid typed record\"}\n");
                    else OUT("%zu\n", count);
            collect_ctx_destroy(&cc);
        }
    } else if (fp.kind == FP_INTERSECT) {
        /* The bitmap popcount shortcut is gone: every intersect count
           validates its candidates through parallel_indexed_count, whose
           record fetch revalidates at the Kf reader. The KeySet is a
           candidate producer, never the answer. */
        int small_primary = 0;
        CandidateBuildOut cb;
        KeySet *result = intersect_indexed_leaves_ck(db_root, object, sch.splits,
                                                     fp.source_leaves, fp.n_source,
                                                     &dl, &small_primary, NULL, &cb);
        if (!result && (cb.reason == CBF_INSERT_CAPACITY ||
                        (cb.reason == CBF_BUDGET && cb.saved_errno == 0)) &&
            !candidate_deadline_expired(&dl)) {
            /* One-shot whole-record recovery: never zero for
               capacity failure. */
            size_t full = 0;
            if (count_full_record_checked(db_root, object, tree, &fs, &sch,
                                          &dl, &full) != 0) {
                if (!dl.timed_out) {
                    OUT("{\"error\":\"invalid typed record\"}\n");
                    return -1;
                }
                OUT("{\"error\":\"query_timeout\"}\n");
                return 0;
            }
            if (cb.reason == CBF_INSERT_CAPACITY)
                BMKS_STAT_FALLBACK("count");
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("%zu\n", full);
            return 0;
        }
        if (!result) {
            if(errno)OUT("{\"error\":\"index candidate construction failed\"}\n");
            else if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("0\n");
        } else {
            CollectedHash *batch = NULL;
            size_t batch_count = 0;
            int rc = keyset_to_collected_hashes(result, sch.splits, &batch, &batch_count);
            keyset_free(result);
            if (rc != 0 || batch_count == 0) { free(batch); OUT("0\n"); }
            else {
                size_t n = parallel_indexed_count(db_root, object, &sch, batch,
                                                  (int)batch_count, tree, &fs, &dl, &fp);
                free(batch);
                if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
                else if(n==SIZE_MAX)OUT("{\"error\":\"invalid typed record\"}\n");
                else OUT("%zu\n", n);
            }
        }
    } else if (fp.kind == FP_UNION) {
        int budget_exceeded = 0;
        size_t count = keyset_count_from_or(db_root, object, &sch, tree, fp.or_node,
                                            &fs, &dl, &budget_exceeded);
        if (budget_exceeded) OUT(QUERY_BUFFER_ERR);
        else if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
        else if(count==SIZE_MAX)OUT("{\"error\":\"invalid typed record\"}\n");
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
        SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
        if (sdb) {
            int64_t match_count = 0;
            int match_status=scan_shards_v2_o_direct_match_checked(sdb, &fs, fast_cc,
                                            fast_cc ? NULL : tree,
                                            &dl, &match_count);
            if(match_status)OUT("{\"error\":\"invalid typed record\"}\n");
            else if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("%lld\n", (long long)match_count);
        } else {
            OUT("{\"error\":\"object not open\"}\n");
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
size_t idx_count_for_leaf_ck(const char *db_root, const char *object,
                             const Schema *sch, const FieldSchema *fs,
                             SearchCriterion *leaf, QueryDeadline *dl,
                             CandidateBuildOut *out) {
    cb_out_init(out);
    int picked = pick_index_for_leaf(db_root, object, leaf);
    const TypedField *tf = resolve_idx_field(fs->ts, leaf->field);

    /* Build a single-leaf criteria node around `leaf` so
       parallel_indexed_count verifies only that leaf (no tree).
       Must compile it (compile_one) before use: parallel_indexed_count's
       per-record verification checks node->compiled and treats NULL as
       "never matches", so an uncompiled node here silently zeroes every
       count. Heap-allocate cc: free_compiled_criteria() calls free() on
       the array pointer itself (not just inner buffers), so it must be
       paired with calloc, never a stack variable. */
    CompiledCriterion *cc = calloc(1, sizeof(CompiledCriterion));
    if(!cc||compile_one_checked(cc,leaf,fs->ts,NULL,0)){free_compiled_criteria(cc,1);return SIZE_MAX;}
    CriteriaNode leaf_node = { .kind = CNODE_LEAF, .leaf = *leaf,
                               .compiled = cc,
                               .children = NULL, .n_children = 0 };
    size_t cnt = 0;

    if (picked == IT_TRIGRAM || picked == IT_BITMAP) {
        /* Candidate producer only: the trigram/bitmap match set feeds
           parallel_indexed_count, whose record fetch revalidates every
           candidate at the Kf reader — the raw popcount success paths
           are gone. */
        KeySet *ks = (picked == IT_TRIGRAM)
            ? build_keyset_from_trigram_b(db_root, object, sch->splits, leaf, dl,
                                          NULL, out)
            : build_keyset_from_bitmap(db_root, object, sch->splits, leaf, tf, dl,fs->ts,NULL,out);
        if (ks) {
            CollectedHash *entries = NULL;
            size_t n = 0;
            keyset_to_collected_hashes(ks, sch->splits, &entries, &n);
            cnt = parallel_indexed_count(db_root, object, sch,
                                         entries, (int)n,
                                         &leaf_node, (FieldSchema *)fs, dl, NULL);
            free(entries);
            keyset_free(ks);
        } else {
            if ((out->reason == CBF_INSERT_CAPACITY ||
                 (out->reason == CBF_BUDGET && out->saved_errno == 0)) &&
                !candidate_deadline_expired(dl)) {
                /* Candidate set overflowed after concurrent growth:
                   recover once from the full record scan with the original
                   tree and deadline. */
                size_t full = 0;
                if (count_full_record_checked(db_root, object, &leaf_node,
                                              (FieldSchema *)fs, sch, dl,
                                              &full) != 0) {
                    int saved = errno;
                    free_compiled_criteria(cc, 1);
                    errno = saved;
                    out->reason = dl->timed_out ? CBF_TIMEOUT : CBF_HARD;
                    out->saved_errno = saved;
                    if (!dl->timed_out) return SIZE_MAX;
                    return 0;   /* timeout: caller renders it */
                }
                if (out->reason == CBF_INSERT_CAPACITY)
                    BMKS_STAT_FALLBACK("count");
                cb_out_init(out);
                free_compiled_criteria(cc, 1);
                return full;
            }
            if (out->reason == CBF_HARD) {
                int saved = out->saved_errno;
                free_compiled_criteria(cc, 1);
                errno = saved;
                return SIZE_MAX;
            }
            /* budget / deliberate empty keep their historical behavior */
            errno = 0;
        }
        free_compiled_criteria(cc, 1);
        return cnt;
    }

    /* IT_BTREE (default): collect candidate hashes, then validate+count
       through the Kf boundary instead of counting btree visits. */
    {
        CollectCtx col;
        collect_ctx_init(&col);
        col.splits = sch->splits;
        col.primary_crit = leaf;
        col.check_primary = op_needs_check_primary(leaf->op);
        col.deadline = dl;
        col.tf = tf;
        if(btree_dispatch_schema_checked(fs?fs->ts:NULL,db_root, object, leaf->field, sch->splits,
                       leaf, tf, collect_hash_cb, &col)<0){collect_ctx_destroy(&col);free_compiled_criteria(cc,1);return SIZE_MAX;}
        if (col.budget_exceeded) {
            collect_ctx_destroy(&col);
            free_compiled_criteria(cc, 1);
            return 0;
        }
        cnt = parallel_indexed_count(db_root, object, sch,
                                     col.entries, (int)col.count,
                                     &leaf_node, (FieldSchema *)fs, dl, NULL);
        collect_ctx_destroy(&col);
    }
    free_compiled_criteria(cc, 1);
    return cnt;
}

size_t idx_count_for_leaf(const char *db_root, const char *object,
                          const Schema *sch, const FieldSchema *fs,
                          SearchCriterion *leaf, QueryDeadline *dl) {
    CandidateBuildOut o;
    size_t n = idx_count_for_leaf_ck(db_root, object, sch, fs, leaf, dl, &o);
    if (n == SIZE_MAX && o.reason != CBF_HARD) errno = 0;
    return n;
}

/* find <object> <criteria_json> [offset] [limit] [fields]
   criteria_json: [{"field":"name","op":"contains","value":"ali"},{"field":"age","op":"gte","value":"18"}] */
/* ========== Multi-field order_by (shared by cursor + buffer-sort) ======
   order_by accepts a CSV string ("a,b") or, on the wire, a JSON array that
   server.c flattens to CSV via json_obj_string_or_array. One shared sort
   direction. Cursor mode additionally requires the exact composite index
   "a+b+…" and fixed-width (non-varchar) encodings for every non-final
   part: composite leaf values are enc(f1)‖…‖enc(fN)‖hash16 with varchar
   parts as raw content bytes, so only fixed-width prefixes guarantee that
   byte order equals tuple order. */
#define MAX_ORDER_FIELDS 4

typedef struct {
    char              name[256];
    int               idx;    /* typed-schema index, -1 = unresolved */
    const TypedField *tf;
    int               dir;    /* resolved: 0=asc, 1=desc */
} OrderSpecPart;

typedef struct {
    int           n;
    const TypedSchema *schema;
    OrderSpecPart parts[MAX_ORDER_FIELDS];
} OrderSpec;

/* Split the order_by CSV into trimmed part names with optional per-part
   direction suffixes ("name:asc" / "name:desc"; a bare name inherits
   shared_desc). Returns 0 on success, -1 with *err set. Purely lexical —
   no schema access. Field names never contain ':' (the CLI's own
   --order-by parser has always relied on that), so a suffix is
   unambiguous. */
static int order_by_split(const char *csv,
                          OrderSpecPart parts[],
                          int *nparts, const char **err) {
    *nparts = 0;
    if (!csv || !csv[0]) { *err = "cursor requires order_by"; return -1; }
    const char *p = csv;
    while (*p) {
        if (*nparts >= MAX_ORDER_FIELDS) {
            *err = "order_by supports at most 4 fields"; return -1;
        }
        /* trim leading spaces */
        while (*p == ' ' || *p == '\t') p++;
        char *dst = parts[(*nparts)].name;
        int len = 0;
        while (*p && *p != ',') {
            if (len >= 255) { *err = "order_by field name too long"; return -1; }
            dst[len++] = *p++;
        }
        /* trim trailing spaces */
        while (len > 0 && (dst[len-1] == ' ' || dst[len-1] == '\t')) len--;
        dst[len] = '\0';
        if (len == 0) { *err = "order_by contains an empty field name"; return -1; }
        /* optional per-part direction suffix (last ':') */
        parts[(*nparts)].dir = -1;
        for (int l = len - 1; l >= 0; l--) {
            if (dst[l] != ':') continue;
            const char *d = dst + l + 1;
            if (strcmp(d, "asc") == 0)       parts[(*nparts)].dir = 0;
            else if (strcmp(d, "desc") == 0) parts[(*nparts)].dir = 1;
            else {
                *err = "invalid per-field order direction; use asc or desc";
                return -1;
            }
            if (l == 0) { *err = "order_by contains an empty field name"; return -1; }
            dst[l] = '\0';
            break;
        }
        (*nparts)++;
        if (*p == ',') {
            p++;
            if (!*p) { *err = "order_by contains an empty field name"; return -1; }
        }
    }
    if (*nparts == 0) { *err = "order_by contains an empty field name"; return -1; }
    return 0;
}

/* Split + resolve every part against the typed schema, merging the shared
   direction with per-part suffixes (suffix wins). Single-field queries
   keep full legacy behavior: an unresolved name returns success with
   idx=-1 so the caller's legacy fallbacks (btree_idx_exists check,
   decode_field) apply unchanged. Multi-field requires every part to
   resolve. */
static int order_spec_resolve(const FieldSchema *fs,
                              const char *order_by_csv,
                              int shared_desc,
                              OrderSpec *out,
                              const char **err) {
    memset(out, 0, sizeof(*out));
    out->schema=fs?fs->ts:NULL;
    if (order_by_split(order_by_csv, out->parts,
                       &out->n, err) != 0) return -1;
    for (int i = 0; i < out->n; i++)
        if (out->parts[i].dir < 0) out->parts[i].dir = shared_desc ? 1 : 0;
    if (out->n == 1 && (!fs || !fs->ts)) { out->parts[0].idx = -1; return 0; }
    for (int i = 0; i < out->n; i++) {
        out->parts[i].idx = -1;
        out->parts[i].tf  = NULL;
        if (fs && fs->ts) {
            for (int j = 0; j < fs->ts->nfields; j++) {
                if (strcmp(fs->ts->fields[j].name, out->parts[i].name) == 0) {
                    out->parts[i].idx = j;
                    out->parts[i].tf  = &fs->ts->fields[j];
                    break;
                }
            }
        }
        if (out->parts[i].idx < 0) {
            if (out->n == 1) return 0;   /* legacy single-field fallback */
            *err = "multi-field order_by field is not in the typed schema";
            return -1;
        }
    }
    return 0;
}

/* "a,b,c" → "a+b+c" (the composite index directory name). */
static void order_spec_composite_name(const OrderSpec *os,
                                      char *out, size_t cap) {
    size_t pos = 0;
    out[0] = '\0';
    for (int i = 0; i < os->n && pos < cap; i++) {
        int w = snprintf(out + pos, cap - pos, "%s%s",
                         i ? "+" : "", os->parts[i].name);
        if (w < 0 || (size_t)w >= cap - pos) return;
        pos += (size_t)w;
    }
}

/* 1 = every non-final part has a fixed-width index encoding. */
static int order_spec_nonfinal_fixed(const OrderSpec *os){
    for(int i=0;i+1<os->n;i++){
        size_t width=0;
        if(!os->schema||os->parts[i].idx<0||
           typed_index_fixed_width(os->schema,(size_t)os->parts[i].idx,&width)!=1)return 0;
    }
    return 1;
}

/* 1 = all parts share one direction. The composite btree is a single byte
   order (forward = ASC, reverse = DESC), so this is the prerequisite for
   serving a query from the index walk. */
static int order_spec_uniform(const OrderSpec *os) {
    for (int i = 1; i < os->n; i++)
        if (os->parts[i].dir != os->parts[0].dir) return 0;
    return 1;
}

/* ===== Ordered find: buffer matches, sort, emit slice =====
   Used when caller sets order_by on find mode. Joins are not supported in
   this path (caller rejects the combination). Full-scan based; an indexed
   ordered scan is a v2 item. */
/* One order_by part's sort payload for a buffered row. Typed fields carry
   their memcmp-natural index-key encoding (exact for every type — a
   double conversion of the rendered string loses precision past 2^53,
   e.g. 17-digit datetimems values); the legacy untyped single field
   carries its decoded string instead. */
typedef struct {
    uint8_t *key;       /* typed index-key bytes; NULL on the legacy path */
    size_t   key_len;
    char    *str;       /* legacy decoded string */
    int      dir;       /* 0=asc, 1=desc */
} RowSortKey;

typedef struct {
    char *key;
    size_t key_len;
    uint8_t *record;        /* malloc'd copy of [key bytes | value bytes] */
    size_t value_len;       /* length of the value portion */
    uint8_t hdr2;           /* record generation (SREC_FLAG_NULLMAP) */
    RowSortKey keys[MAX_ORDER_FIELDS];
    int nsort;
} OrderedRow;

/* Release one row's per-part sort payloads. Also used on the extraction
   scratch before ownership transfers into a row. */
static void row_sort_keys_release(RowSortKey *keys) {
    for (int i = 0; i < MAX_ORDER_FIELDS; i++) {
        free(keys[i].key);
        free(keys[i].str);
    }
}

typedef struct {
    OrderedRow *rows;
    size_t count;
    size_t cap;
    CriteriaNode *tree;
    FieldSchema *fs;
    const OrderSpec *ospec;  /* resolved parts (n = 0 without order_by) */
    const char *order_field_name;    /* full order_by string (legacy single-field fallback) */
    int nsort;
    ExcludedKeys *excluded;
    QueryDeadline *deadline;
    int dl_counter;
    size_t buffer_bytes;    /* running total for QUERY_BUFFER_MB cap */
    /* Lock-free reads (line 7959 fast-skip in the per-record callback;
       line 8553 post-scan check) intentionally don't take oc->lock — _Atomic
       gives them a torn-read-free view against the writers at lines 7985, 7998
       which set the flag under oc->lock. */
    _Atomic int budget_exceeded;
    pthread_mutex_t lock;
    int predicate_error;
} OrderedCollectCtx;

/* §2.7: typed_field_to_index_key expects the field-region base; record
   bytes sit nullmap_prefix in for nullmap generations. Resolve via the
   record's own view, then encode. Zero-length output = view rejected the
   record (corrupt) — callers treat it as the empty-key case.
   §2.5/Task 8d: the output is the FULL tiered index key — [0x00][payload]
   for values, the bare [0x01] byte for NULL — byte-identical to what the
   btree stores, so buffered-sort keys interoperate with the cursor
   resume compare and NULL rows order past values exactly like the index. */
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
    int matched=0;
    if(criteria_match_tree_checked(raw,hdr->value_len,oc->tree,oc->fs,(uint8_t)(hdr->flag>>8),&matched)){
        int error=errno?errno:EILSEQ;pthread_mutex_lock(&oc->lock);oc->predicate_error=error;pthread_mutex_unlock(&oc->lock);return 1;}
    if(!matched)return 0;

    RowSortKey sv[MAX_ORDER_FIELDS]={0};size_t key_needs[MAX_ORDER_FIELDS]={0};
    size_t rec_len=(size_t)hdr->key_len+hdr->value_len;
    size_t row_bytes=(size_t)hdr->key_len+1+rec_len;
    for(int i=0;i<oc->nsort;i++){
        const OrderSpecPart *part=&oc->ospec->parts[i];sv[i].dir=part->dir;
        if(oc->fs&&oc->fs->ts&&part->idx>=0&&part->tf){
            if(typed_index_field_key_into(oc->fs->ts,(size_t)part->idx,raw,
                hdr->value_len,(uint8_t)(hdr->flag>>8),NULL,0,&key_needs[i])!=1)goto key_error;
            if(key_needs[i]>g_query_buffer_max_bytes-row_bytes){errno=ENOBUFS;goto key_error;}
            row_bytes+=key_needs[i];
        }else if(i==0){
            sv[i].str=decode_field((const char*)raw,hdr->value_len,
                    oc->order_field_name,oc->fs,(uint8_t)(hdr->flag>>8));
            if(!sv[i].str){errno=ENOMEM;goto key_error;}
            size_t n=strlen(sv[i].str)+1;
            if(n>g_query_buffer_max_bytes-row_bytes){errno=ENOBUFS;goto key_error;}
            row_bytes+=n;
        }
    }
    /* Existing collect lock serializes admission and ownership. Measure
       actual keys first; charge allocated row capacity, not only row count. */
    pthread_mutex_lock(&oc->lock);
    size_t new_cap=oc->cap;
    if(oc->count>=new_cap)new_cap=new_cap?new_cap*2:256;
    size_t growth=(new_cap-oc->cap)*sizeof(OrderedRow);
    if(oc->buffer_bytes>g_query_buffer_max_bytes||
       growth>g_query_buffer_max_bytes-oc->buffer_bytes||
       row_bytes>g_query_buffer_max_bytes-oc->buffer_bytes-growth){
        oc->budget_exceeded=1;pthread_mutex_unlock(&oc->lock);
        row_sort_keys_release(sv);return 1;
    }
    if(new_cap!=oc->cap){
        OrderedRow *rows=realloc(oc->rows,new_cap*sizeof(*rows));
        if(!rows){oc->predicate_error=ENOMEM;pthread_mutex_unlock(&oc->lock);row_sort_keys_release(sv);return 1;}
        oc->rows=rows;oc->cap=new_cap;oc->buffer_bytes+=growth;
    }
    OrderedRow candidate={0};
    candidate.key=malloc((size_t)hdr->key_len+1);candidate.record=malloc(rec_len?rec_len:1);
    if(!candidate.key||!candidate.record){
        free(candidate.key);free(candidate.record);oc->predicate_error=ENOMEM;
        pthread_mutex_unlock(&oc->lock);row_sort_keys_release(sv);return 1;
    }
    for(int i=0;i<oc->nsort;i++)if(key_needs[i]){
        sv[i].key=malloc(key_needs[i]);
        if(!sv[i].key){errno=ENOMEM;goto locked_error;}
        if(typed_index_field_key_into(oc->fs->ts,(size_t)oc->ospec->parts[i].idx,
             raw,hdr->value_len,(uint8_t)(hdr->flag>>8),sv[i].key,
             key_needs[i],&sv[i].key_len)!=1)goto locked_error;
    }
    memcpy(candidate.key,block,hdr->key_len);candidate.key[hdr->key_len]=0;
    memcpy(candidate.record,block,rec_len);candidate.key_len=hdr->key_len;
    candidate.value_len=hdr->value_len;candidate.hdr2=(uint8_t)(hdr->flag>>8);
    candidate.nsort=oc->nsort;memcpy(candidate.keys,sv,sizeof(sv));
    oc->rows[oc->count++]=candidate;oc->buffer_bytes+=row_bytes;
    pthread_mutex_unlock(&oc->lock);return 0;
 locked_error:
    oc->predicate_error=errno?errno:EILSEQ;free(candidate.key);free(candidate.record);
    pthread_mutex_unlock(&oc->lock);row_sort_keys_release(sv);return 1;
 key_error:{
    int error=errno?errno:EILSEQ;row_sort_keys_release(sv);
    pthread_mutex_lock(&oc->lock);oc->predicate_error=error;
    pthread_mutex_unlock(&oc->lock);return 1;
 }
}

static int cmp_row_asc(const void *a, const void *b) {
    const OrderedRow *ra = (const OrderedRow *)a;
    const OrderedRow *rb = (const OrderedRow *)b;
    int n = ra->nsort < rb->nsort ? ra->nsort : rb->nsort;
    for (int i = 0; i < n; i++) {
        const RowSortKey *ka = &ra->keys[i];
        const RowSortKey *kb = &rb->keys[i];
        int c;
        if (ka->key && kb->key) {
            /* Typed part: memcmp over the index-key encoding, with the
               standard length tiebreak (matches the btree's own varlen
               compare). */
            size_t m = ka->key_len < kb->key_len ? ka->key_len : kb->key_len;
            c = memcmp(ka->key, kb->key, m);
            if (c == 0)
                c = ka->key_len < kb->key_len ? -1 :
                    ka->key_len > kb->key_len ?  1 : 0;
        } else {
            const char *sa = ka->str ? ka->str : "";
            const char *sb = kb->str ? kb->str : "";
            c = strcmp(sa, sb);
        }
        if (c) return ka->dir ? -c : c;
    }
    return 0;
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
    int    nparts;
    struct { char *value; size_t vlen; } part[MAX_ORDER_FIELDS];
    int    part_tier[MAX_ORDER_FIELDS]; /* §2.5: per-field tier byte (v2 tokens) */
    int    version;                     /* 1 = legacy (all tiers 0), 2 = tiered */
    char   key[1100];        /* primary key wire string */
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
static void find_cursor_release(FindCursor *cur){
    for(int i=0;i<MAX_ORDER_FIELDS;i++){free(cur->part[i].value);cur->part[i].value=NULL;}
}
static int parse_cursor_object(const char *cursor_json, const char *order_by,
                               FindCursor *out, const char **err) {
    out->present = 0;
    out->nparts = 0;
    for (int i = 0; i < MAX_ORDER_FIELDS; i++) out->part[i].vlen = 0;
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

    /* Token version (§2.5): "v":2 tokens carry a per-field "tiers" array
       ([0x00] value / [0x01] NULL per order_by component). Legacy tokens
       (no "v") imply tier 0 for every part. Unknown versions fail closed. */
    /* Tiered tokens carry a per-field "tiers" array; its PRESENCE is the
       v2 discriminator — a bare "v" version key would collide with an
       order_by field literally named "v" (the mini JSON helpers
       first-match keys). Legacy tokens (no "tiers") imply tier 0 for
       every part. Malformed tier arrays fail closed. */
    out->version = strstr(cursor_json, "\"tiers\"") ? 2 : 1;
    for (int i = 0; i < MAX_ORDER_FIELDS; i++) out->part_tier[i] = 0;

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
    OrderSpecPart names[MAX_ORDER_FIELDS];
    int  nparts = 0;
    if (order_by_split(order_by, names, &nparts, err) != 0) return -1;
    for (int i = 0; i < nparts; i++) {
        char *vv = json_obj_strdup(&c, names[i].name);
        if (!vv) { *err = "cursor missing order_by field value"; return -1; }
        size_t vlen = strlen(vv);
        size_t used=0;
        for(int j=0;j<i;j++)used+=out->part[j].vlen+1;
        if(vlen>=g_query_buffer_max_bytes||used>g_query_buffer_max_bytes-vlen-1){
            free(vv);*err="cursor value exceeds query buffer";errno=ENOBUFS;return -1;
        }
        out->part[i].value=vv;out->part[i].vlen=vlen;
    }
    out->nparts = nparts;
    if (out->version == 2) {
        /* §2.5 tiered tokens: parse "tiers":[0|1,...] — one entry per
           order_by part, fail-closed on shape errors. */
        const char *tiers_key = strstr(cursor_json, "\"tiers\"");
        if (!tiers_key) { *err = "cursor missing 'tiers' array"; return -1; }
        const char *arr = strchr(tiers_key + 8, '[');
        const char *arr_end = arr ? strchr(arr, ']') : NULL;
        if (!arr || !arr_end) { *err = "cursor malformed 'tiers' array"; return -1; }
        int ti = 0;
        for (const char *q = arr + 1; q <= arr_end; q++) {
            if (*q == '0' || *q == '1') {
                if (ti >= nparts) { *err = "cursor 'tiers' arity mismatch"; return -1; }
                out->part_tier[ti++] = (*q == '1');
            } else if (*q == ',' || *q == ' ' || *q == '\t') {
                continue;
            } else if (*q == ']') {
                break;
            } else {
                *err = "cursor 'tiers' entries must be 0 or 1";
                return -1;
            }
        }
        if (ti != nparts) { *err = "cursor 'tiers' arity mismatch"; return -1; }
    }

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

    /* Captured last-emitted cursor: one JSON-escaped string per order_by
       part. Heap-owned, freed by caller. */
    char          *last_value[MAX_ORDER_FIELDS];
    int            last_tiers[MAX_ORDER_FIELDS]; /* §2.5 per-part tier of the last emitted row */
    char          *last_key_str;

    /* Multi-field order_by parts (single-field paths: n == 1). NULL on
       paths that never emit a cursor token. */
    const OrderSpec *ospec;

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
    int predicate_error;
} CursorFindCtx;

static int cursor_capture(CursorFindCtx *c,const uint8_t *raw,size_t len,
 uint8_t flags,const char *wire,size_t wirelen){
    if(!c->ospec)return 0;
    char *values[MAX_ORDER_FIELDS]={0};int tiers[MAX_ORDER_FIELDS]={0};
    size_t bytes=wirelen+1;
    if(c->last_key_str)bytes+=strlen(c->last_key_str)+1;
    for(int i=0;i<MAX_ORDER_FIELDS;i++)if(c->last_value[i])bytes+=strlen(c->last_value[i])+1;
    if(bytes>g_query_buffer_max_bytes){errno=ENOBUFS;return -1;}
    char *key=NULL;TypedRowView view;
    if(!c->fs||!c->fs->ts||typed_row_open(c->fs->ts,raw,len,flags,&view))return -1;
    for(int i=0;i<c->ospec->n;i++){
        size_t field=(size_t)c->ospec->parts[i].idx;TypedRowSpan span;
        if(typed_row_span(&view,field,&span))goto failed;
        tiers[i]=span.is_null;if(span.is_null)continue;
        char *text=NULL;size_t n=0;
        int present=typed_query_named_text(c->fs->ts,raw,len,flags,
            c->fs->ts->fields[field].name,g_query_buffer_max_bytes-bytes,&text,&n);
        if(present!=1){if(!errno)errno=EILSEQ;goto failed;}
        size_t escaped=0;
        for(size_t j=0;j<n;j++){
            unsigned char ch=(unsigned char)text[j];
            size_t add=(ch=='"'||ch=='\\'||ch=='\b'||ch=='\f'||ch=='\n'||ch=='\r'||ch=='\t')?2:ch<32?6:1;
            if(add>g_query_buffer_max_bytes-escaped){free(text);errno=ENOBUFS;goto failed;}
            escaped+=add;
        }
        /* Both temporary plain text and escaped token are owned together. */
        if(bytes>g_query_buffer_max_bytes||n+1>g_query_buffer_max_bytes-bytes||
           escaped+1>g_query_buffer_max_bytes-bytes-(n+1)){
            free(text);errno=ENOBUFS;goto failed;
        }
        values[i]=malloc(escaped+1);
        if(!values[i]){free(text);errno=ENOMEM;goto failed;}
        int written=json_escape_into(values[i],escaped,text,n);free(text);
        if(written<0){errno=EILSEQ;goto failed;}
        values[i][written]=0;bytes+=escaped+1;
    }
    key=malloc(wirelen+1);if(!key){errno=ENOMEM;goto failed;}
    memcpy(key,wire,wirelen);key[wirelen]=0;
    for(int i=0;i<MAX_ORDER_FIELDS;i++){
        free(c->last_value[i]);c->last_value[i]=values[i];c->last_tiers[i]=tiers[i];
    }
    free(c->last_key_str);c->last_key_str=key;return 0;
 failed:{int error=errno?errno:EILSEQ;for(int i=0;i<MAX_ORDER_FIELDS;i++)free(values[i]);free(key);errno=error;return -1;}
}

static int cursor_find_cb(const char *val, size_t vlen,
                          const uint8_t *hash16,
                          BtOrderedWalkHandle *wh, void *ctx) {
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
        /* §2.5: walked keys and cursor bytes share the same tiered shape
           ([t1][v1][t2][v2]; NULL component = tier byte, no payload), so
           the length-aware memcmp compares tuples exactly — including
           NULL components — and the hash16 tiebreak resolves tie groups
           (rows sharing the full order_by tuple) in both directions. */
        int vcmp;
        {
            size_t mlen = vlen < c->cursor_value_len ? vlen : c->cursor_value_len;
            vcmp = memcmp(val, c->cursor_value_bytes, mlen);
            if (vcmp == 0) {
                if (vlen < c->cursor_value_len) vcmp = -1;
                else if (vlen > c->cursor_value_len) vcmp = 1;
            }
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
    int rc = read_record_ref_try(c->db_root, c->object, c->sch, hash16, &rr);
    if (rc == 2) {
        btree_ordered_walk_release_for_blocking(wh);
        rc = read_record_ref(c->db_root, c->object, c->sch, hash16, &rr) != 0 ? 1 : 0;
    }
    if (rc != 0) return 0;
    const uint8_t *key_start = rr.key;
    const uint8_t *raw       = rr.val;
    uint32_t       value_len = (uint32_t)rr.vlen;

    /* Remaining criteria (full tree). Order_by leaf still gets checked — it
       stays correct because the btree walk only visits entries that would
       pass a range/eq on the indexed field, and range checks in the tree
       agree with the range in the walk. */
    int matched=0;
    if(criteria_match_tree_checked(raw,value_len,c->remaining,c->fs,rr.hdr2,&matched)){
        c->predicate_error=errno?errno:EILSEQ;release_record_ref(&rr);return -1;}
    if(!matched){release_record_ref(&rr);return 0;}

    /* Skip-after-match: criteria matched but we're still inside the offset
       window. Release and decrement; the next match emits. */
    if (c->skip_remaining > 0) {
        c->skip_remaining--;
        release_record_ref(&rr);
        return 0;
    }

    /* Emit the primary key's protocol representation, never its binary
       storage bytes.  This is also the key stored in the next cursor. */
    char key_buf[1100];
    format_query_wire_key(c->fs, key_start, rr.klen, key_buf);
    size_t klen = strlen(key_buf);

    if(cursor_capture(c,raw,value_len,rr.hdr2,key_buf,klen)){
        c->predicate_error=errno?errno:EILSEQ;release_record_ref(&rr);return -1;
    }
    if(typed_query_emit_row(c->parent_out,key_buf,klen,raw,value_len,rr.hdr2,c->fs,
         c->proj_fields,c->proj_count,c->rows_fmt,c->dict_fmt,0,c->printed>0,
         NULL,0,NULL,g_query_buffer_max_bytes)){
        c->predicate_error=errno?errno:EILSEQ;release_record_ref(&rr);return -1;
    }

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
    uint8_t sort_key[1024]; /* inline common case; struct moves stay safe */
    size_t sort_key_len;
    uint8_t *owned_sort_key;
    _Atomic size_t *owned_bytes; /* resource accounting only; no engine gate */
} SmallPrefilterRow;


static const uint8_t *small_row_key(const SmallPrefilterRow *r){
    return r->owned_sort_key?r->owned_sort_key:r->sort_key;
}
static void small_row_release(SmallPrefilterRow *r){
    if(r->owned_sort_key){
        free(r->owned_sort_key);
        atomic_fetch_sub_explicit(r->owned_bytes,r->sort_key_len,memory_order_relaxed);
    }
    r->owned_sort_key=NULL;r->owned_bytes=NULL;r->sort_key_len=0;
}
static void small_rows_release(SmallPrefilterRow *rows,size_t n){
    if(!rows)return;
    for(size_t i=0;i<n;i++)small_row_release(&rows[i]);
    free(rows);
}
static void small_row_move(SmallPrefilterRow *dst,SmallPrefilterRow *src){
    if(dst==src)return;
    small_row_release(dst);*dst=*src;
    src->owned_sort_key=NULL;src->owned_bytes=NULL;src->sort_key_len=0;
}
static int sort_bytes_reserve(_Atomic size_t *used,size_t n,size_t limit){
    size_t old=atomic_load_explicit(used,memory_order_relaxed);
    do {
        if(n>limit||old>limit-n){errno=ENOBUFS;return -1;}
    }while(!atomic_compare_exchange_weak_explicit(used,&old,old+n,
                 memory_order_relaxed,memory_order_relaxed));
    return 0;
}
static void *sort_array_resize(void *ptr,size_t old_cap,size_t new_cap,
 _Atomic size_t *used,size_t limit){
    if(new_cap>SIZE_MAX/sizeof(SmallPrefilterRow)){errno=ENOBUFS;return NULL;}
    size_t old_bytes=old_cap*sizeof(SmallPrefilterRow),new_bytes=new_cap*sizeof(SmallPrefilterRow);
    if(sort_bytes_reserve(used,new_bytes-old_bytes,limit))return NULL;
    void *next=realloc(ptr,new_bytes?new_bytes:1);
    if(!next){atomic_fetch_sub_explicit(used,new_bytes-old_bytes,memory_order_relaxed);errno=ENOMEM;return NULL;}
    if(new_bytes>old_bytes)memset((uint8_t*)next+old_bytes,0,new_bytes-old_bytes);
    return next;
}
static void sort_array_free(SmallPrefilterRow *rows,size_t count,size_t cap,
 _Atomic size_t *used){
    if(!rows)return;
    small_rows_release(rows,count);
    atomic_fetch_sub_explicit(used,cap*sizeof(SmallPrefilterRow),memory_order_relaxed);
}
static int small_row_build(SmallPrefilterRow *row,const TypedSchema *ts,
 const uint8_t *raw,size_t len,uint8_t flags,int field,
 _Atomic size_t *owned_bytes,size_t overflow_budget){
    size_t need=0;
    if(field<0||typed_index_field_key_into(ts,(size_t)field,raw,len,flags,NULL,0,&need)!=1)
        return -1;
    if(need<=sizeof(row->sort_key)){
        if(typed_index_field_key_into(ts,(size_t)field,raw,len,flags,row->sort_key,
                                       sizeof(row->sort_key),&need)!=1)return -1;
        row->sort_key_len=need;return 0;
    }
    /* Reserve before malloc. Counter follows owned keys through struct
       moves, sorting, compaction, eviction and final release. */
    if(sort_bytes_reserve(owned_bytes,need,overflow_budget))return -1;
    uint8_t *key=malloc(need);
    if(!key){atomic_fetch_sub_explicit(owned_bytes,need,memory_order_relaxed);errno=ENOMEM;return -1;}
    size_t actual=0;
    if(typed_index_field_key_into(ts,(size_t)field,raw,len,flags,key,need,&actual)!=1){
        int error=errno;free(key);atomic_fetch_sub_explicit(owned_bytes,need,memory_order_relaxed);errno=error;return -1;
    }
    row->owned_sort_key=key;row->owned_bytes=owned_bytes;row->sort_key_len=actual;return 0;
}
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
    if (!map) {
        LOG_ERROR(LOG_SUB_QUERY, "d2_build_hash_map: malloc failed for n=%zu entries", n);
        return NULL;
    }
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
    _Atomic size_t *sort_owned_bytes;
    size_t sort_overflow_budget;
} D2BatchCtx;

static int d2_batch_cb(const uint8_t hash16[16],
                        const void *key, size_t klen,
                        const void *value, size_t vlen,
                        uint8_t hdr2, void *ctx_ptr) {
    (void)key; (void)klen;
    D2BatchCtx *ctx = (D2BatchCtx *)ctx_ptr;
    if (ctx->deadline && query_deadline_tick(ctx->deadline, ctx->dl_counter)) return 1;

    D2HashIdxEntry lookup;
    lookup.idx = -1;   /* key element: comparator reads .hash only */
    memcpy(lookup.hash, hash16, 16);
    D2HashIdxEntry *found = bsearch(&lookup, ctx->hash_map, ctx->n_total,
                                     sizeof(D2HashIdxEntry), d2_hash_idx_cmp);
    if (!found) return 0;
    int i = found->idx;

    if (ctx->passed[i]) return 0;  /* already processed (hash collision edge) */

    /* Run criteria match tree on this record */
    int matched=0;
    if(criteria_match_tree_checked(value,vlen,ctx->tree,ctx->fs,hdr2,&matched)){ctx->passed[i]=-(errno?errno:EILSEQ);return 1;}
    if(!matched)return 0;

    /* Pass: extract sort key at the ORIGINAL row position.
       Compaction happens after all callbacks complete (see call site). */
    if(small_row_build(&ctx->rows[i],ctx->ts,value,vlen,hdr2,
                       ctx->order_field_idx,ctx->sort_owned_bytes,
                       ctx->sort_overflow_budget)){
        ctx->passed[i]=-(errno?errno:EILSEQ);return 1;
    }
    ctx->passed[i]=1;
    __sync_fetch_and_add(ctx->n_kept,1);
    return 0;
}

static int sort_request_error(int error){
    errno=error?error:EILSEQ;
    if(errno==ENOBUFS)OUT(QUERY_BUFFER_ERR);
    else OUT("{\"error\":\"ordered query failed: %s\"}\n",strerror(errno));
    return -1;
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
    int c = memcmp(small_row_key(ra), small_row_key(rb), mlen);
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
    int predicate_error;
    _Atomic size_t *sort_owned_bytes;
    size_t sort_overflow_budget;
} CursorFetchCtx;

/* cursor_fetch_cb — callback for batch lookup in cursor_fetch_worker.
   Runs criteria_match_tree + typed_field_to_index_key per found record
   while the segcache handle is held (pointers transient). */
static int cursor_fetch_cb(const uint8_t hash[16],
                            const void *key, size_t klen,
                            const void *value, size_t vlen,
                            uint8_t hdr2, void *ctx_ptr) {
    (void)key; (void)klen;
    CursorFetchCtx *ctx = (CursorFetchCtx *)ctx_ptr;
    if (query_deadline_tick(ctx->deadline, &ctx->dl_counter)) return 1;

    int matched=0;
    if(criteria_match_tree_checked(value,vlen,ctx->tree,ctx->fs,hdr2,&matched)){
        int error=errno?errno:EILSEQ;pthread_mutex_lock(&ctx->lock);ctx->predicate_error=error;pthread_mutex_unlock(&ctx->lock);return 1;}
    if(!matched)return 0;

    SmallPrefilterRow candidate={0};memcpy(candidate.hash,hash,16);
    if(small_row_build(&candidate,ctx->ts,value,vlen,hdr2,ctx->order_field_idx,
                       ctx->sort_owned_bytes,ctx->sort_overflow_budget)){
        pthread_mutex_lock(&ctx->lock);ctx->predicate_error=errno?errno:EILSEQ;
        pthread_mutex_unlock(&ctx->lock);return 1;
    }
    pthread_mutex_lock(&ctx->lock);
    if(ctx->result_count>=ctx->result_cap){
        int new_cap=ctx->result_cap?ctx->result_cap*2:64;
        SmallPrefilterRow *t=sort_array_resize(ctx->results,(size_t)ctx->result_cap,
            (size_t)new_cap,ctx->sort_owned_bytes,ctx->sort_overflow_budget);
        if(!t){ctx->predicate_error=errno?errno:ENOMEM;goto failed;}
        ctx->results=t;ctx->result_cap=new_cap;
    }
    ctx->results[ctx->result_count++]=candidate;
    pthread_mutex_unlock(&ctx->lock);return 0;
 failed:
    pthread_mutex_unlock(&ctx->lock);small_row_release(&candidate);return 1;
}

static void *cursor_fetch_worker(void *arg) {
    CursorFetchCtx *ctx = (CursorFetchCtx *)arg;
    if (ctx->entry_count == 0) return NULL;

    SlotcaskSchemaInfo info = {
        .splits = ctx->sch->splits,
        .slot_size = ctx->sch->slot_size,
        .streams = ctx->sch->streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(ctx->db_root, ctx->object, &info);
    if (!sdb) {
        LOG_WARN(LOG_SUB_SLOTCASK, "cursor_fetch_worker: slotcask_registry_get failed for %s/%s", ctx->db_root, ctx->object);
        ctx->predicate_error=errno?errno:EIO;return NULL;
    }

    /* Extract hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)ctx->entry_count * sizeof(*hashes));
    if (!hashes) {
        LOG_ERROR(LOG_SUB_QUERY, "cursor_fetch_worker: malloc failed for entry_count=%d", ctx->entry_count);
        ctx->predicate_error=ENOMEM;return NULL;
    }
    for (int i = 0; i < ctx->entry_count; i++)
        memcpy(hashes[i], ctx->entries[i].hash, 16);

    /* Batch resolve+fetch — two-phase model resolves all KF shards internally
       and parallelizes segment reads across unique segment files. The callback
       fires for each found record while the segcache handle is held. */
    pthread_mutex_init(&ctx->lock, NULL);
    if(slotcask_bulk_resolve_and_fetch(sdb,hashes,(size_t)ctx->entry_count,ctx,cursor_fetch_cb)<0&&
       !ctx->predicate_error)ctx->predicate_error=errno?errno:EIO;
    pthread_mutex_destroy(&ctx->lock);

    free(hashes);
    return NULL;
}

/* One-way request flag: once candidate capacity overflow forced a whole
 * record fallback, no later stage of the SAME request may rebuild bitmap
 * candidates (prefilters, probes, totals). Thread-local: one request never
 * spans threads at the orchestration level; set and cleared strictly inside
 * cmd_find_do_impl on every exit path. */
static __thread int bm_find_candidates_disabled;
static __thread int bm_find_candidates_active;
typedef struct {
    int disabled;
    int active;
} BmFindScope;

/* Scope guard: the request-scoped flag is always cleared, on every exit
 * path, when the owning cmd_find_do_impl frame unwinds. */
static void bm_find_flag_reset(BmFindScope *saved) {
    bm_find_candidates_disabled = saved->disabled;
    bm_find_candidates_active = saved->active;
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
    int predicate_error;
    _Atomic size_t *sort_owned_bytes;
    size_t sort_overflow_budget;
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

static void d2_topn_push(SmallPrefilterRow *heap,int *n,int cap,
 SmallPrefilterRow *cand,int desc){
    if(*n<cap){heap[*n]=*cand;cand->owned_sort_key=NULL;cand->owned_bytes=NULL;
        d2_topn_sift_up(heap,*n,desc);(*n)++;return;}
    if(d2_topn_outranks(&heap[0],cand,desc)){
        small_row_release(&heap[0]);heap[0]=*cand;
        cand->owned_sort_key=NULL;cand->owned_bytes=NULL;
        d2_topn_sift_down(heap,*n,0,desc);
    }
    small_row_release(cand);
}

/* keyset_iter callback: one fetch + filter + heap-push per candidate. */
/* slotcask_bulk_resolve_and_fetch callback: receives the record data directly
   instead of calling read_record_ref. */
static int fetch_sort_batch_cb(const uint8_t hash16[16],
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                uint8_t hdr2, void *ctx_ptr) {
    FetchSortCtx *c = (FetchSortCtx *)ctx_ptr;
    if (query_deadline_tick(c->dl, &c->dl_counter)) return 1;

    int matched=0;
    if(criteria_match_tree_checked(value,vlen,c->tree,c->fs,hdr2,&matched)){
        int error=errno?errno:EILSEQ;pthread_mutex_lock(&c->lock);c->predicate_error=error;pthread_mutex_unlock(&c->lock);return 1;}
    if(!matched)return 0;
    if (c->excluded && c->excluded->count > 0) {
        char keybuf[1100];
        format_query_wire_key(c->fs, key, klen, keybuf);
        if (is_excluded(c->excluded, keybuf)) return 0;
    }

    __sync_fetch_and_add(&c->n_matched, 1);

    SmallPrefilterRow cur={0};memcpy(cur.hash,hash16,16);
    if(small_row_build(&cur,c->fs->ts,value,vlen,hdr2,c->order_field_idx,
                       c->sort_owned_bytes,c->sort_overflow_budget)){
        int error=errno?errno:EILSEQ;pthread_mutex_lock(&c->lock);
        c->predicate_error=error;pthread_mutex_unlock(&c->lock);return 1;
    }

    pthread_mutex_lock(&c->lock);
    if (c->heap_cap > 0) {
        d2_topn_push(c->heap, &c->heap_n, c->heap_cap, &cur, c->desc);
    } else {
        if (c->full_oom) { pthread_mutex_unlock(&c->lock);small_row_release(&cur);return 1; }
        if (c->full_n == c->full_cap) {
            int new_cap = c->full_cap ? c->full_cap * 2 : 64;
            SmallPrefilterRow *bigger=sort_array_resize(c->fullbuf,(size_t)c->full_cap,
                (size_t)new_cap,c->sort_owned_bytes,c->sort_overflow_budget);
            if(!bigger){c->full_oom=1;c->predicate_error=errno?errno:ENOMEM;
                pthread_mutex_unlock(&c->lock);small_row_release(&cur);return 1;}
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
                                  QueryDeadline *dl,
                                  KeySet *prebuilt, int have_prebuilt,
                                  KeySetBudget *budget)
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
    if(order_field_idx<0){keyset_free(prebuilt);errno=EINVAL;return SIZE_MAX;}

    /* The candidate KeySet comes from the pre-emission preparation when a
       bitmap source is involved (ownership transferred; NULL = deadline
       expired during preparation → nothing to emit), else it is built from
       the planner's source here. */
    KeySet *prefilter_ks;
    if (have_prebuilt) {
        prefilter_ks = prebuilt;
        if (!prefilter_ks) return 0;
    } else {
        errno=0;
        prefilter_ks=build_keyset_from_plan(fp,db_root,object,sch,dl);
        if(!prefilter_ks){if(!errno)errno=EIO;return SIZE_MAX;}
    }
    if (keyset_size(prefilter_ks) == 0) { keyset_free(prefilter_ks); return 0; }

    /* For D1 composite plans where a more-selective non-seed leaf exists,
       replace the broad composite-seed KeySet with a narrow KeySet built
       from the most-selective leaf. The replacement charges the same
       request budget; the two sets coexist only until the broad one is
       freed below. On a refused/failing build the broad set stays. */
    if (fp->prefilter_source_leaf &&
        fp->prefilter_card > 0 &&
        fp->prefilter_card < keyset_size(prefilter_ks)) {
        CandidateBuildOut ncb;
        errno = 0;
        KeySet *narrow = build_keyset_from_leaf_ck(db_root, object, sch->splits,
                                                   fp->prefilter_source_leaf,
                                                   dl, budget, &ncb);
        if (narrow) {
            keyset_free(prefilter_ks);
            prefilter_ks = narrow;
        } else {
            errno = 0;   /* keep the already-built broad prefilter */
        }
    }

    /* Set up the top-N heap (or fall back to unbounded materialization
       when limit==0). offset is included in the heap capacity so the
       skip is honored after sort. */
    if(limit>0&&(offset<0||offset>INT_MAX-limit)){
        keyset_free(prefilter_ks);errno=ENOBUFS;return SIZE_MAX;
    }
    int M = (limit > 0) ? (offset + limit) : 0;
    _Atomic size_t sort_owned_bytes=0;
    SmallPrefilterRow *heap = NULL;
    if (M > 0) {
        heap=sort_array_resize(NULL,0,(size_t)M,&sort_owned_bytes,g_query_buffer_max_bytes);
        if (!heap) {int error=errno?errno:ENOMEM;keyset_free(prefilter_ks);errno=error;return SIZE_MAX;}
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
    fc.sort_owned_bytes=&sort_owned_bytes;fc.sort_overflow_budget=g_query_buffer_max_bytes;
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
        SlotcaskDb *sdb SDB_REG_REF=slotcask_registry_get(db_root,object,&sinfo);
        if(!sdb){
            int error=errno?errno:EIO;keyset_free(prefilter_ks);pthread_mutex_destroy(&fc.lock);
            sort_array_free(heap,0,(size_t)M,&sort_owned_bytes);errno=error;return SIZE_MAX;
        }
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
                if(slotcask_bulk_resolve_and_fetch(sdb,batch,FS_BATCH,&fc,fetch_sort_batch_cb)<0&&
                   !fc.predicate_error)fc.predicate_error=errno?errno:EIO;
                batch_n = 0;
            }
        }
        if (batch_n > 0) {
            if(slotcask_bulk_resolve_and_fetch(sdb,batch,(size_t)batch_n,&fc,fetch_sort_batch_cb)<0&&
               !fc.predicate_error)fc.predicate_error=errno?errno:EIO;
        }
        keyset_free(prefilter_ks);
    }
    pthread_mutex_destroy(&fc.lock);

    if(fc.predicate_error){
        sort_array_free(heap,(size_t)fc.heap_n,(size_t)M,&sort_owned_bytes);
        sort_array_free(fc.fullbuf,(size_t)fc.full_n,(size_t)fc.full_cap,&sort_owned_bytes);
        errno=fc.predicate_error;return SIZE_MAX;
    }
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
        if (cursor_find_cb("", 0, out[i].hash, NULL, &cc) < 0) break;
    }

    sort_array_free(heap,(size_t)fc.heap_n,(size_t)M,&sort_owned_bytes);
    sort_array_free(fc.fullbuf,(size_t)fc.full_n,(size_t)fc.full_cap,&sort_owned_bytes);
    for (int i = 0; i < MAX_ORDER_FIELDS; i++) free(cc.last_value[i]);
    free(cc.last_key_str);
    if(cc.predicate_error){errno=cc.predicate_error;return SIZE_MAX;}
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
static KeySet *build_keyset_from_plan_ck(const FilterPlan *fp,
                                      const char *db_root,
                                      const char *object,
                                      const Schema *sch,
                                      QueryDeadline *dl, KeySetBudget *budget,
                                         CandidateBuildOut *out) {
    cb_out_init(out);
    if (candidate_deadline_expired(dl)) return bm_build_fail(out, CBF_TIMEOUT, 0);
    if (!fp) return bm_build_fail(out, CBF_NONE, 0);
    if (bm_find_candidates_disabled) return bm_build_fail(out, CBF_NONE, 0);   /* one-way fallback */
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
                                                    leaf, tf, ts);
            }
            if (pop == SIZE_MAX) return bm_build_fail(out, CBF_HARD, errno);
            if (pop > g_ordered_find_keyset_max)
                return bm_build_fail(out, CBF_NONE, 0);
        }
        return build_keyset_from_leaf_ck(db_root, object, sch->splits,
                                         leaf, dl, budget, out);
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
                (lf->op != OP_EQUAL && lf->op != OP_IN) ||
                !bitmap_literal_fits(load_typed_schema(db_root,object),lf,1024)) {
                all_bitmap_eqin = 0;
                break;
            }
        }
        if (all_bitmap_eqin) {
            TypedSchema *ts = load_typed_schema(db_root, object);
            size_t pop = bm_popcount_intersect(db_root, object, sch->splits,
                                               (SearchCriterion **)fp->source_leaves,
                                               fp->n_source, ts, dl);
            if (pop == SIZE_MAX) return bm_build_fail(out, CBF_HARD, errno);
            if (pop > g_ordered_find_keyset_max)
                return bm_build_fail(out, CBF_NONE, 0);
        }
        int small_primary = 0;
        return intersect_indexed_leaves_ck(db_root, object, sch->splits,
                                        (SearchCriterion **)fp->source_leaves,
                                        fp->n_source,
                                        dl, &small_primary, budget, out);
    }
    case FP_UNION: {
        int budget_exceeded = 0;
        return build_or_keyset_ck(db_root, object, sch->splits,
                               fp->or_node, dl,
                               &budget_exceeded, 0, budget, out);
    }
    case FP_FULL_SCAN:
    default:
        return bm_build_fail(out, CBF_NONE, 0);
    }
}

static KeySet *build_keyset_from_plan(const FilterPlan *fp,
                                      const char *db_root, const char *object,
                                      const Schema *sch, QueryDeadline *dl) {
    CandidateBuildOut out;
    KeySet *ks = build_keyset_from_plan_ck(fp, db_root, object, sch, dl, NULL, &out);
    if (!ks && out.reason == CBF_INSERT_CAPACITY && !candidate_deadline_expired(dl)) {
        if (bm_find_candidates_active) bm_find_candidates_disabled = 1;
        BMKS_STAT_FALLBACK("prefilter");
        errno = 0;
    }
    return ks;
}

/* SlotcaskScanCb for bulk_delete_phase1_indexed: receives key + value from a
   slotcask_bulk_resolve_and_fetch call, re-verifies the full criteria tree,
   and appends the key to the BulkCriteriaCtx if it matches. Same locking and
   budget logic as bulk_criteria_scan_cb, different callback signature. */
static int bulk_criteria_indexed_cb(const uint8_t hash16[16],
                                     const void *key, size_t klen,
                                     const void *value, size_t vlen,
                                     uint8_t hdr2, void *raw_ctx) {
    (void)hash16;
    BulkCriteriaCtx *bc = (BulkCriteriaCtx *)raw_ctx;
    if (bc->budget_exceeded) return 0;
    if (bc->limit > 0 && bc->count >= bc->limit) return 0;
    if (query_deadline_tick(bc->deadline, &bc->dl_counter)) return 0;

    /* Re-verify full criteria tree (index may be slightly stale) */
    int matched=0;
    if(criteria_match_tree_checked(value,vlen,bc->tree,bc->fs,hdr2,&matched)){
        int error=errno?errno:EILSEQ;pthread_mutex_lock(&bc->lock);bc->predicate_error=error;pthread_mutex_unlock(&bc->lock);return 1;}
    if(!matched)return 0;

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
    BmFindScope bm_flag_guard __attribute__((cleanup(bm_find_flag_reset))) =
        { bm_find_candidates_disabled, bm_find_candidates_active };
    bm_find_candidates_disabled = 0;
    bm_find_candidates_active = 0;
    size_t N = (size_t)get_live_count(db_root, object);
    FilterPlan fp = plan_filter(tree, db_root, object, fs,
                                sch->splits, N,
                                NULL, 0 /* fetching=0, count semantics */,
                                limit);
    if (fp.kind == FP_FULL_SCAN) return 0;

    KeySet *ks = build_keyset_from_plan(&fp, db_root, object, sch, dl);
    if (!ks) { errno = 0; return 0; }   /* caller falls back to a full scan */
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
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
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
static size_t prepared_count_candidates(const KeySet *ks, CriteriaNode *tree,
                                        const char *root, const char *object,
                                        const Schema *sch, FieldSchema *fs,
                                        QueryDeadline *dl, int *out_null);

static size_t fp_compute_total(const FilterPlan *fp, CriteriaNode *tree,
                               const char *db_root, const char *object,
                               const Schema *sch, FieldSchema *fs,
                               QueryDeadline *dl, int *out_null,
                               KeySet *hint_ks) {
    *out_null = 1;
    if (!fp || candidate_deadline_expired(dl)) return 0;
    if (bm_find_candidates_disabled) {
        size_t total = 0;
        if (count_full_record_checked(db_root, object, tree, fs, sch, dl,
                                      &total) != 0)
            return SIZE_MAX;
        *out_null = dl->timed_out ? 1 : 0;
        return total;
    }
    CandidateBuildOut cb;
    cb_out_init(&cb);
    KeySet *ks = NULL;
    int owned = 1;
    int budget_exceeded = 0;
    switch (fp->kind) {
    case FP_INTERSECT: {
        int small_primary = 0;
        ks = intersect_indexed_leaves_ck(db_root, object, sch->splits,
                                         (SearchCriterion **)fp->source_leaves,
                                         fp->n_source, dl, &small_primary,
                                         NULL, &cb);
        break;
    }
    case FP_PRIMARY_LEAF:
    case FP_BITMAP_SMALLER:
        if (fp->order == FP_ORDER_COMPOSITE_EXACT) return 0;
        if (fp->order == FP_ORDER_COMPOSITE && tree && tree->kind != CNODE_LEAF)
            return 0;
        if (fp->n_source == 0) return 0;
        if (hint_ks) { ks = hint_ks; owned = 0; }
        else ks = build_keyset_from_leaf_ck(db_root, object, sch->splits,
                                            fp->source_leaves[0], dl, NULL, &cb);
        break;
    case FP_UNION:
        ks = build_or_keyset_ck(db_root, object, sch->splits, fp->or_node,
                                dl, &budget_exceeded, 0, NULL, &cb);
        break;
    default:
        return 0;
    }
    if (!ks) {
        if (cb.reason == CBF_INSERT_CAPACITY && !candidate_deadline_expired(dl)) {
            if (bm_find_candidates_active) bm_find_candidates_disabled = 1;
            size_t total = 0;
            if (count_full_record_checked(db_root, object, tree, fs, sch, dl,
                                          &total) != 0)
                return SIZE_MAX;
            BMKS_STAT_FALLBACK("total");
            *out_null = dl->timed_out ? 1 : 0;
            return total;
        }
        if (cb.reason == CBF_HARD) {
            errno = cb.saved_errno ? cb.saved_errno : EIO;
            return SIZE_MAX;
        }
        if (!budget_exceeded && errno && !dl->timed_out) return SIZE_MAX;
        return 0;
    }
    size_t total = prepared_count_candidates(ks, tree, db_root, object,
                                             sch, fs, dl, out_null);
    int saved = errno;
    if (owned) keyset_free(ks);
    errno = saved;
    return total;
}


/* ========== Prepared bitmap find sources (pre-emission boundary) ==========
   A find whose plan reads bitmap sources prepares them — candidate KeySets
   and any required total that would read bitmaps — before the response
   envelope, so a bitmap failure is reported as one complete error object
   instead of after rows. Records are filtered, paginated and emitted
   afterwards by the existing executors. Not a general output transaction:
   record/serialization errors after emission keep their existing
   behavior. */
typedef enum {
    PREP_NONE = 0,
    PREP_STREAM,     /* former small-limit streaming shape: 1-record iterator */
    PREP_INTERSECT,  /* keyset_emit_find over the intersection candidates */
    PREP_PRIMARY,    /* keyset_emit_find over the bitmap leaf candidates */
    PREP_UNION       /* keyset_emit_find over the union candidates */
} PrepRoute;

typedef struct {
    PrepRoute    route;
    KeySet      *ks;              /* positive candidate set; NULL = none */
    int          small_primary;   /* intersect returned only its smallest leaf */
    int          need_rematch;    /* emit must re-check the full tree */
    int          total_ready;     /* response total computed pre-emission */
    size_t       total;
    int          total_null;
    int          d2_ready;        /* D2's required prefilter prepared */
    KeySet      *d2_prefilter;    /* ownership handed to find_via_fetch_sort */
    KeySetBudget budget;          /* request-owned candidate budget */
    int          record_fallback; /* candidate overflow: whole-record mode is
                                   one-way for the rest of this request */
} PreparedBitmapFind;

static int find_leaf_is_bitmap(const char *db_root, const char *object,
                               const SearchCriterion *leaf) {
    return leaf && pick_index_for_leaf(db_root, object, leaf) == IT_BITMAP;
}

static int find_plan_reads_bitmap(const FilterPlan *fp, const char *db_root,
                                  const char *object) {
    if (!fp) return 0;
    switch (fp->kind) {
    case FP_PRIMARY_LEAF:
    case FP_BITMAP_SMALLER:
    case FP_INTERSECT:
        for (int i = 0; i < fp->n_source; i++)
            if (find_leaf_is_bitmap(db_root, object, fp->source_leaves[i]))
                return 1;
        return 0;
    case FP_UNION:
        if (!fp->or_node) return 0;
        for (int i = 0; i < fp->or_node->n_children; i++) {
            const CriteriaNode *c = fp->or_node->children[i];
            if (c && c->kind == CNODE_LEAF &&
                find_leaf_is_bitmap(db_root, object, &c->leaf))
                return 1;
        }
        return 0;
    default:
        return 0;
    }
}

static void prepared_find_destroy(PreparedBitmapFind *p) {
    keyset_free(p->ks);
    p->ks = NULL;
    keyset_free(p->d2_prefilter);
    p->d2_prefilter = NULL;
}

/* One deterministic error object for a pre-emission failure; candidate
   memory exhaustion keeps the existing buffer error text. */
static void prepared_find_report(int err) {
    if (err == ENOBUFS) OUT(QUERY_BUFFER_ERR);
    else OUT("{\"error\":\"bitmap index unavailable\"}\n");
}

/* Response total for an ordered/D-path or tail fallback: the prepared value
   when the plan's total was computed pre-emission, else the existing
   fp_compute_total. */
static size_t prepared_or_fp_total(const PreparedBitmapFind *p,
                                   const FilterPlan *fp, CriteriaNode *tree,
                                   const char *db_root, const char *object,
                                   const Schema *sch, FieldSchema *fs,
                                   QueryDeadline *dl, int *out_null) {
    if (p->total_ready) { *out_null = p->total_null; return p->total; }
    return fp_compute_total(fp, tree, db_root, object, sch, fs, dl, out_null,
                            NULL);
}

/* Candidate overflow recovery — discard the prepared candidates, flip
 * the request into whole-record mode (one-way), and compute any required
 * total before the envelope with the original deadline. No candidate route
 * runs again in this request; no new deadline; no snapshot isolation. */
static int prepared_find_recover(PreparedBitmapFind *p, CriteriaNode *tree,
                                  const char *db_root, const char *object,
                                  const Schema *sch, FieldSchema *fs,
                                  QueryDeadline *dl, int want_total,
                                  const char *kind) {
    keyset_free(p->ks);
    p->ks = NULL;
    keyset_free(p->d2_prefilter);
    p->d2_prefilter = NULL;
    p->d2_ready = 0;
    p->small_primary = 0;
    p->route = PREP_NONE;
    p->need_rematch = 0;
    p->total_ready = 0;          /* any cached candidate total is invalid */
    p->total_null = 1;
    p->record_fallback = 1;
    BMKS_STAT_FALLBACK(kind);
    if (want_total && !dl->timed_out) {
        size_t full = 0;
        if (count_full_record_checked(db_root, object, tree, fs, sch, dl,
                                      &full) != 0)
            return -1;
        p->total = full;
        p->total_null = dl->timed_out ? 1 : 0;
        p->total_ready = 1;
    }
    return 0;
}

/* 0 = prepared (or nothing to prepare); -1 = failure with errno, to be
   reported before any response byte. A deadline that expires during
   preparation leaves an empty candidate set (partial-result semantics; no
   work is restarted). */
typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    FieldSchema *fs;
    CriteriaNode *tree;
    QueryDeadline *deadline;
    int dl_counter;
    int error;
    size_t count;
} PreparedCountCtx;

static int prepared_count_candidate_cb(const uint8_t hash[16], void *raw) {
    PreparedCountCtx *c = raw;
    if (candidate_deadline_expired(c->deadline)) return 1;
    RecordRef rr;
    if (read_record_ref(c->db_root, c->object, c->sch, hash, &rr) != 0)
        return 0;
    int matched = 0;
    int status = criteria_match_tree_checked(rr.val, rr.vlen, c->tree, c->fs,
                                             rr.hdr2, &matched);
    int saved = errno;
    release_record_ref(&rr);
    if (status) {
        c->error = saved ? saved : EILSEQ;
        return 1;
    }
    if (matched) c->count++;
    return 0;
}

static size_t prepared_count_candidates(const KeySet *ks, CriteriaNode *tree,
                                        const char *root, const char *object,
                                        const Schema *sch, FieldSchema *fs,
                                        QueryDeadline *dl, int *out_null) {
    PreparedCountCtx c = { .db_root = root, .object = object, .sch = sch,
                            .fs = fs, .tree = tree, .deadline = dl };
    keyset_iter(ks, prepared_count_candidate_cb, &c);
    if (c.error) { errno = c.error; return SIZE_MAX; }
    *out_null = dl->timed_out ? 1 : 0;
    return c.count;
}

static int prepared_find_prepare(PreparedBitmapFind *p, const FilterPlan *fp,
                                 CriteriaNode *tree, const char *db_root,
                                 const char *object, const Schema *sch,
                                 FieldSchema *fs, QueryDeadline *dl,
                                 int has_order, int has_joins, int rows_fmt,
                                 int csv_delim, int njoins, int offset,
                                 int limit, int want_total,
                                 const char *order_by) {
    if (!find_plan_reads_bitmap(fp, db_root, object)) return 0;
    if (!has_order && fp->order == FP_ORDER_NONE) {
        int is_leafish = fp->kind == FP_PRIMARY_LEAF ||
                         fp->kind == FP_BITMAP_SMALLER;
        int stream = limit > 0 && (offset + limit) <= 1000 && njoins == 0 &&
                     !rows_fmt &&
                     (fp->kind == FP_INTERSECT || (is_leafish && tree)) &&
                     !(is_leafish && fp->n_source > 0 && fp->source_leaves[0] &&
                       (op_prefers_trigram(fp->source_leaves[0]->op) ||
                        op_allows_trigram_starts(fp->source_leaves[0]->op)) &&
                       field_has_index_type(db_root, object,
                                            fp->source_leaves[0]->field,
                                            IT_TRIGRAM));
        errno = 0;
        if (fp->kind == FP_INTERSECT) {
            int sp = 0;
            CandidateBuildOut cb;
            p->ks = intersect_indexed_leaves_ck(db_root, object, sch->splits,
                                               (SearchCriterion **)fp->source_leaves,
                                               fp->n_source, dl, &sp, &p->budget,
                                               &cb);
            if (!p->ks && cb.reason == CBF_INSERT_CAPACITY && !candidate_deadline_expired(dl)) {
                return prepared_find_recover(p, tree, db_root, object, sch, fs, dl,
                                      want_total, "intersect");
            }
            if (!p->ks && errno && !dl->timed_out) return -1;
            p->small_primary = sp;
            p->route = stream ? PREP_STREAM : PREP_INTERSECT;
            p->need_rematch = stream || sp || fp->n_postfilter > 0;
        } else if (is_leafish) {
            SearchCriterion *pc = fp->n_source > 0 ? fp->source_leaves[0] : NULL;
            if (!find_leaf_is_bitmap(db_root, object, pc)) return 0;
            CandidateBuildOut cb;
            p->ks = build_keyset_from_leaf_ck(db_root, object, sch->splits, pc,
                                             dl, &p->budget, &cb);
            if (!p->ks && cb.reason == CBF_INSERT_CAPACITY && !candidate_deadline_expired(dl)) {
                return prepared_find_recover(p, tree, db_root, object, sch, fs, dl,
                                      want_total, "primary");
            }
            if (!p->ks && errno && !dl->timed_out) return -1;
            p->route = stream ? PREP_STREAM : PREP_PRIMARY;
            p->need_rematch = 1;
        } else if (fp->kind == FP_UNION) {
            CriteriaNode *or_node = fp->or_node;
            int rematch_unnecessary =
                (tree == or_node) || (tree && tree->kind == CNODE_OR) ||
                (tree && tree->kind == CNODE_AND && tree->n_children == 1 &&
                 tree->children[0] == or_node);
            int need_rematch = !rematch_unnecessary;
            int target = (!want_total && !need_rematch && limit > 0)
                         ? offset + limit : 0;
            int budget_exceeded = 0;
            CandidateBuildOut cb;
            p->ks = build_or_keyset_ck(db_root, object, sch->splits, or_node, dl,
                                      &budget_exceeded, target, &p->budget, &cb);
            if (budget_exceeded) { keyset_free(p->ks); p->ks = NULL; errno = ENOBUFS; return -1; }
            if (!p->ks && cb.reason == CBF_INSERT_CAPACITY && !candidate_deadline_expired(dl)) {
                return prepared_find_recover(p, tree, db_root, object, sch, fs, dl,
                                      want_total, "union");
            }
            if (!p->ks && errno && !dl->timed_out) return -1;
            p->route = PREP_UNION;
            p->need_rematch = need_rematch;
        }
        if (want_total && p->ks && !dl->timed_out) {
            /* Candidate counts are not match counts: postfilter leaves and
               rematch shapes leave candidates that the full tree rejects,
               and raw popcounts are optimization inputs, not validated
               totals. Validate every candidate against the full tree here,
               before the envelope, so total:true never reports a count
               derived from an unverified candidate set. */
            int tnull = 1;
            size_t n = prepared_count_candidates(p->ks, tree, db_root, object,
                                                 sch, fs, dl, &tnull);
            if (n == SIZE_MAX) return -1;
            p->total = n;
            p->total_null = tnull;
            p->total_ready = 1;
        }
        return 0;
    }
    /* Ordered paths: D2's prefilter is a required source; the others use
       only optional probes (whole-record fallback). Their response totals
       would read bitmap sources after rows, so compute them now. */
    int d2 = fp->order == FP_ORDER_SORT && !has_joins && !rows_fmt &&
             !csv_delim && fp->n_source > 0 &&
             (!order_by || strchr(order_by, ',') == NULL);
    if (d2) {
        errno = 0;
        CandidateBuildOut cb;
        p->d2_prefilter = build_keyset_from_plan_ck(fp, db_root, object, sch, dl,
                                                   &p->budget, &cb);
        if (!p->d2_prefilter && cb.reason == CBF_INSERT_CAPACITY &&
            !candidate_deadline_expired(dl))
            return prepared_find_recover(p, tree, db_root, object, sch, fs, dl,
                                          want_total, "ordered");
        if (!p->d2_prefilter && !dl->timed_out) {
            if (!errno) errno = ENOBUFS;   /* no bounded candidate set */
            return -1;
        }
        if (want_total && !p->d2_prefilter && dl->timed_out) {
            /* The deadline expired before a usable prefilter existed: no
               rows can be walked and the true match count is unknown.
               D2's walk-time total is then meaningless, so the close must
               report null, not a fabricated zero. */
            p->total_null = 1;
            p->total_ready = 1;
        }
        p->d2_ready = 1;
    } else if (want_total && !dl->timed_out) {
        int tnull = 1;
        size_t n = fp_compute_total(fp, tree, db_root, object, sch, fs, dl,
                                    &tnull, NULL);
        if (n == SIZE_MAX) return -1;
        p->total = n;
        p->total_null = tnull;
        p->total_ready = 1;
    }
    return 0;
}

static int cmd_find_do_impl(const char *db_root, const char *object,
                        CriteriaNode *tree,
                        JoinSpec *joins, int njoins,
                        int offset, int limit,
                        const char *proj_str, const char *excluded_csv,
                        const char *format, const char *delimiter,
                        const char *order_by, const char *order_dir,
                        const char *cursor_json,int want_total,
                        const Schema *explicit_schema,const FieldSchema *explicit_fields) {
    BmFindScope bm_flag_guard __attribute__((cleanup(bm_find_flag_reset))) =
        { bm_find_candidates_disabled, bm_find_candidates_active };
    bm_find_candidates_disabled = 0;
    bm_find_candidates_active = 1;
    _Atomic size_t sort_owned_bytes=0;
    int sort_error=0;
    int rows_fmt = (format && strcmp(format, "rows") == 0);
    int dict_fmt = (format && strcmp(format, "dict") == 0);
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    int has_joins = (njoins > 0);

    /* tree is owned by caller — no free_criteria_tree here */
    Schema sch=explicit_schema?*explicit_schema:load_schema(db_root,object);
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
    if(explicit_fields)driver_fs=*explicit_fields;
    else init_field_schema(&driver_fs,db_root,object);
    if (has_joins && resolve_joins(joins, njoins, db_root, object, &driver_fs) < 0) {
        free_joins(joins, njoins);
        free_excluded(&excluded);
        return -1;
    }

    {
        char verr[256];
        int vbad =
            validate_criteria_tree_fields(tree, driver_fs.ts,
                                          verr, sizeof(verr)) < 0 ||
            (proj_count > 0 && validate_field_list(proj_fields, proj_count,
                                                   driver_fs.ts,
                                                   "projection", verr,
                                                   sizeof(verr)) < 0);
        /* order_by may be a CSV list ("a,b") with per-part ":asc/:desc"
           suffixes — split first, then validate each bare field name.
           Split-level errors (too many fields, empty part, bad suffix)
           surface here, before any dispatch. */
        if (!vbad && order_by && order_by[0]) {
            OrderSpecPart oparts[MAX_ORDER_FIELDS];
            int  on = 0;
            const char *oerr = NULL;
            if (order_by_split(order_by, oparts, &on, &oerr) != 0) {
                OUT("{\"error\":\"%s\"}\n", oerr);
                free_joins(joins, njoins);
                free_excluded(&excluded);
                return -1;
            }
            for (int i = 0; i < on && !vbad; i++) {
                if (validate_field(driver_fs.ts, oparts[i].name, "order_by",
                                   verr, sizeof(verr)) < 0)
                    vbad = 1;
            }
        }
        if (vbad) {
            OUT("{\"error\":\"%s\"}\n", verr);
            free_joins(joins, njoins);
            free_excluded(&excluded);
            return -1;
        }
    }

    /* Resolve order_by parts once (already validated above): names,
       typed-schema indexes, and merged directions. Consumed by the cursor
       branch, the D2 fetch-sort guard, and the buffered fallback. Plain
       finds without order_by keep an empty OrderSpec (n = 0) and are
       untouched — every ospec consumer sits behind a path that requires
       order_by (cursor parse errors out first; D2 / indexed-walk /
       buffered are gated by fp.order / has_order). */
    OrderSpec ospec;
    memset(&ospec, 0, sizeof(ospec));
    if (order_by && order_by[0]) {
        int shared_desc = (order_dir && (strcmp(order_dir, "desc") == 0 ||
                                         strcmp(order_dir, "DESC") == 0));
        const char *ospec_err = NULL;
        if (order_spec_resolve(&driver_fs, order_by, shared_desc,
                               &ospec, &ospec_err) != 0) {
            OUT("{\"error\":\"%s\"}\n",
                ospec_err ? ospec_err : "invalid order_by");
            free_joins(joins, njoins);
            free_excluded(&excluded);
            return -1;
        }
    }
    /* Normalize a single-field order_by ("name:dir" suffix stripped) so
       every downstream single-field consumer (planner hint, indexed-walk
       fast path, D2) sees the bare field name. Multi-field CSV keeps
       flowing unchanged: no index matches it, so those paths fall through
       to the buffered sort. */
    char order_norm[256];
    if (order_by && order_by[0] && ospec.n == 1 &&
        strcmp(ospec.parts[0].name, order_by) != 0) {
        snprintf(order_norm, sizeof(order_norm), "%s", ospec.parts[0].name);
        order_by = order_norm;
    }

    if(compile_criteria_tree_checked(tree,driver_fs.ts,NULL,0)){free_joins(joins,njoins);free_excluded(&excluded);OUT("{\"error\":\"invalid criteria value\"}\n");return -1;}


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
        FindCursor cur __attribute__((cleanup(find_cursor_release)))={0};
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
        char composite_name[1100] = "";   /* set below when n > 1 */
        if (ospec.n == 1) {
            if (!btree_idx_exists(db_root, object, order_by, sch.splits)) {
                OUT("{\"error\":\"cursor requires order_by field to be indexed\",\"field\":\"%s\"}\n",
                    order_by);
                free_joins(joins, njoins); free_excluded(&excluded);
                return -1;
            }
        } else {
            if (!order_spec_uniform(&ospec)) {
                OUT("{\"error\":\"cursor pagination requires one shared order direction across all order_by fields\","
                    "\"order_by\":\"%s\"}\n", order_by);
                free_joins(joins, njoins); free_excluded(&excluded);
                return -1;
            }
            if (!order_spec_nonfinal_fixed(&ospec)) {
                OUT("{\"error\":\"multi-field order_by requires all but the last field to be fixed-width (non-varchar)\","
                    "\"order_by\":\"%s\"}\n", order_by);
                free_joins(joins, njoins); free_excluded(&excluded);
                return -1;
            }
            order_spec_composite_name(&ospec, composite_name, sizeof(composite_name));
            if (!btree_idx_exists(db_root, object, composite_name, sch.splits)) {
                OUT("{\"error\":\"multi-field order_by requires a composite index\",\"index\":\"%s\"}\n",
                    composite_name);
                free_joins(joins, njoins); free_excluded(&excluded);
                return -1;
            }
        }

        /* Per-part suffixes override the shared order; uniformity was
           validated above, so part 0's direction is the walk direction. */
        int desc = ospec.parts[0].dir;

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

        /* parse_cursor_object has no schema access, so its order_by values
           are still JSON-escaped text. Unescape varchar parts before index
           encoding. */
        if (cur.present) {
            for (int i = 0; i < ospec.n; i++) {
                if (ospec.parts[i].tf && ospec.parts[i].tf->type == FT_VARCHAR) {
                    char *unesc = NULL; size_t ulen = 0;
                    if (json_unescape_string(cur.part[i].value,
                                              cur.part[i].vlen,
                                              &unesc, &ulen) != 0) {
                        OUT("{\"error\":\"cursor order_by value has a malformed JSON escape\"}\n");
                        free_joins(joins, njoins); free_excluded(&excluded);
                        return -1;
                    }
                    free(cur.part[i].value);cur.part[i].value=unesc;
                    cur.part[i].vlen=ulen;
                }
            }
        }

        /* Encode cursor value bytes for walk bounds. If cursor absent (page 1),
           walk from start (ASC) or end (DESC); else walk from cursor position,
           with tiebreak happening inside the callback. */
        uint8_t cur_value_buf[544];size_t cur_value_len=0;int has_cur_bytes=0;
        if(cur.present){
            size_t payloads[MAX_ORDER_FIELDS]={0};
            for(int i=0;i<ospec.n;i++){
                if(cur.part_tier[i]){
                    size_t width=0;int fixed=typed_index_fixed_width(driver_fs.ts,(size_t)ospec.parts[i].idx,&width);
                    if(fixed<0){free_joins(joins,njoins);free_excluded(&excluded);return sort_request_error(errno);}
                    payloads[i]=fixed?width:0;
                }else {
                    int present=typed_index_text_key_into(driver_fs.ts,(size_t)ospec.parts[i].idx,
                        cur.part[i].value,cur.part[i].vlen,NULL,0,&payloads[i]);
                    if(present<0){free_joins(joins,njoins);free_excluded(&excluded);return sort_request_error(errno?errno:EINVAL);}
                    /* A data-derived probe outside the field's grammar
                       (legacy zero/invalid stored text) encodes as an empty
                       component, never an error. */
                    if(present==0)payloads[i]=0;
                }
                if(payloads[i]>sizeof(cur_value_buf)-cur_value_len-1){
                    free_joins(joins,njoins);free_excluded(&excluded);return sort_request_error(E2BIG);
                }
                cur_value_len+=1+payloads[i];
            }
            size_t pos=0;
            for(int i=0;i<ospec.n;i++){
                cur_value_buf[pos++]=(uint8_t)cur.part_tier[i];
                if(cur.part_tier[i])memset(cur_value_buf+pos,0,payloads[i]);
                else {
                    size_t n=0;
                    if(typed_index_text_key_into(driver_fs.ts,(size_t)ospec.parts[i].idx,
                         cur.part[i].value,cur.part[i].vlen,cur_value_buf+pos,
                         sizeof(cur_value_buf)-pos,&n)<0){
                        free_joins(joins,njoins);free_excluded(&excluded);return sort_request_error(errno);
                    }
                    /* n==0: out-of-grammar probe (R4) → empty component. */
                }
                pos+=payloads[i];
            }
            has_cur_bytes=1;
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
        /* Optional probe: any failure (hard or budget) discards the whole
           optimization and the walk matches full records instead. */
        if (!cursor_prefilter_ks) errno = 0;
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
            } else {
                errno = 0;   /* keep the complete broader prefilter */
            }
        }

        /* A requested total whose count plan reads bitmap sources is
           computed before any cursor response byte: a hard bitmap error is
           reported as one error object, never after the page. */
        int cursor_total_ready = 0, cursor_total_null = 1;
        size_t cursor_total = 0;
        if (want_total) {
            FilterPlan pre_fp = plan_filter(tree, db_root, object, &driver_fs,
                                            sch.splits, cursor_N_live,
                                            NULL, 0 /*fetching*/, 0 /*limit*/);
            if (find_plan_reads_bitmap(&pre_fp, db_root, object)) {
                cursor_total = fp_compute_total(&pre_fp, tree, db_root, object,
                                                &sch, &driver_fs, &cdl,
                                                &cursor_total_null,
                                                cursor_prefilter_ks);
                if (cursor_total == SIZE_MAX) {
                    int err = errno ? errno : EIO;
                    keyset_free(cursor_prefilter_ks);
                    prepared_find_report(err);
                    free_joins(joins, njoins); free_excluded(&excluded);
                    errno = err;
                    return -1;
                }
                cursor_total_ready = 1;
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
                 ospec.n == 1 &&
                 order_tf && driver_fs.ts && order_field_idx >= 0) {
            size_t n_pre = keyset_size(cursor_prefilter_ks);
            struct timespec t1, t2, t3, t4;
            clock_gettime(CLOCK_MONOTONIC, &t1);
            SmallPrefilterRow *sp_rows=sort_array_resize(NULL,0,n_pre,&sort_owned_bytes,g_query_buffer_max_bytes);
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
                            fc.sort_owned_bytes=&sort_owned_bytes;fc.sort_overflow_budget=g_query_buffer_max_bytes;
                            fc.deadline        = &cdl;
                            cursor_fetch_worker(&fc);
                            for (int r = 0; r < fc.result_count; r++) {
                                small_row_move(&sp_rows[n_kept++],&fc.results[r]);
                            }
                            if(fc.predicate_error)sort_error=fc.predicate_error;
                            sort_array_free(fc.results,(size_t)fc.result_count,(size_t)fc.result_cap,&sort_owned_bytes);
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
                                workers[g].sort_owned_bytes=&sort_owned_bytes;workers[g].sort_overflow_budget=g_query_buffer_max_bytes;
                                workers[g].deadline        = &cdl;
                            }
                            parallel_for_io(cursor_fetch_worker, workers,
                                         nshard_groups, sizeof(CursorFetchCtx));
                            for (int g = 0; g < nshard_groups; g++) {
                                for (int r = 0; r < workers[g].result_count; r++) {
                                    small_row_move(&sp_rows[n_kept++],&workers[g].results[r]);
                                }
                                if(workers[g].predicate_error)sort_error=workers[g].predicate_error;
                                sort_array_free(workers[g].results,(size_t)workers[g].result_count,(size_t)workers[g].result_cap,&sort_owned_bytes);
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
                            fc.sort_owned_bytes=&sort_owned_bytes;fc.sort_overflow_budget=g_query_buffer_max_bytes;
                                fc.deadline        = &cdl;
                                cursor_fetch_worker(&fc);
                                for (int r = 0; r < fc.result_count; r++) {
                                    small_row_move(&sp_rows[n_kept++],&fc.results[r]);
                                }
                                if(fc.predicate_error)sort_error=fc.predicate_error;
                            sort_array_free(fc.results,(size_t)fc.result_count,(size_t)fc.result_cap,&sort_owned_bytes);
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
                        SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &sinfo);
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
                                .sort_owned_bytes=&sort_owned_bytes,.sort_overflow_budget=g_query_buffer_max_bytes,
                            };
                            if(slotcask_bulk_resolve_and_fetch(sdb,hashes,n_pre,&d2_ctx,d2_batch_cb)<0)
                                sort_error=errno?errno:EIO;
                            for(size_t pi=0;pi<n_pre;pi++)if(passed[pi]<0)sort_error=-passed[pi];
                            if (!sort_error && n_kept > 0) {
                                int keep = 0;
                                for (size_t i = 0; i < n_pre; i++) {
                                    if(passed[i]>0){small_row_move(&sp_rows[keep++],&sp_rows[i]);}
                                }
                                n_kept = keep;
                            }
                        }
                        free(hashes);
                        free(passed);
                        free(hmap);
                    }
                    if (!sort_error && n_kept == 0) {
                        for (size_t i = 0; i < n_pre; i++) {
                            RecordRef rr;
                            if (read_record_ref(db_root, object, &sch,
                                                sp_rows[i].hash, &rr) != 0) continue;
                            int matched=0;
                            if(criteria_match_tree_checked(rr.val,rr.vlen,tree,&driver_fs,rr.hdr2,&matched)){
                                sort_error=errno?errno:EILSEQ;release_record_ref(&rr);break;
                            }
                            if(!matched){release_record_ref(&rr);continue;}
                            SmallPrefilterRow candidate={0};memcpy(candidate.hash,sp_rows[i].hash,16);
                            if(small_row_build(&candidate,driver_fs.ts,rr.val,rr.vlen,rr.hdr2,
                                order_field_idx,&sort_owned_bytes,g_query_buffer_max_bytes)){
                                sort_error=errno?errno:EILSEQ;release_record_ref(&rr);break;
                            }
                            small_row_move(&sp_rows[n_kept++],&candidate);
                            release_record_ref(&rr);
                        }
                    }
                }
                if(sort_error){
                    sort_array_free(sp_rows,n_pre,n_pre,&sort_owned_bytes);
                    keyset_free(cursor_prefilter_ks);free_joins(joins,njoins);free_excluded(&excluded);
                    return sort_request_error(sort_error);
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
                cc.ospec           = &ospec;
                cc.skip_remaining  = offset > 0 ? offset : 0;
                cc.offset_mode     = 1;
                cc.deadline        = &cdl;
                cc.prefilter_ks    = NULL;
                cc.parent_out      = g_out;
                OUT(dict_fmt ? "{\"rows\":{" : "{\"rows\":[");
                for (int i = 0; i < n_kept; i++) {
                    if (cursor_find_cb((const char *)small_row_key(&sp_rows[i]),
                                       sp_rows[i].sort_key_len,
                                       sp_rows[i].hash, NULL, &cc) < 0) break;
                }
                if(cc.predicate_error){
                    int error=cc.predicate_error;
                    sort_array_free(sp_rows,n_pre,n_pre,&sort_owned_bytes);
                    for(int i=0;i<MAX_ORDER_FIELDS;i++)free(cc.last_value[i]);
            free(cc.last_key_str);
                    keyset_free(cursor_prefilter_ks);free_joins(joins,njoins);free_excluded(&excluded);
                    errno=error;return -1;
                }
                OUT(dict_fmt ? "}" : "]");
                if (!cc.predicate_error && cc.printed >= limit && cc.last_key_str) {
                    int vals_ok = 1;
                    for (int i = 0; i < ospec.n; i++)
                        /* A NULL part (tier 1) has no value string — that
                           is captured state, not a missing capture. Killing
                           the token here strands the NULL-tier rows behind
                           it (§2.5: values sort before NULLs ASC). */
                        if (!cc.last_value[i] && !cc.last_tiers[i]) vals_ok = 0;
                    if (vals_ok) {
                        /* v2 token with per-field tiers (§2.5). */
                        OUT(",\"cursor\":{");
                        for (int i = 0; i < ospec.n; i++)
                            OUT("%s\"%s\":\"%s\"", i ? "," : "",
                                ospec.parts[i].name,
                                cc.last_value[i] ? cc.last_value[i] : "");
                        OUT(",\"tiers\":[");
                        for (int i = 0; i < ospec.n; i++)
                            OUT("%s%d", i ? "," : "", cc.last_tiers[i]);
                        OUT("],\"key\":\"%s\"}", cc.last_key_str);
                    } else {
                        OUT(",\"cursor\":null");
                    }
                } else {
                    OUT(",\"cursor\":null");
                }
                if (want_total) {
                    FilterPlan count_fp = plan_filter(tree, db_root, object,
                        &driver_fs, sch.splits, cursor_N_live,
                        NULL, 0 /*fetching*/, 0 /*limit*/);
                    int tnull = 1;
                    size_t ctotal = cursor_total_ready ? cursor_total
                        : fp_compute_total(&count_fp, tree, db_root,
                                           object, &sch, &driver_fs,
                                           &cdl, &tnull,
                                           cursor_prefilter_ks);
                    if (cursor_total_ready) tnull = cursor_total_null;
                    if (tnull && tree == NULL) { ctotal = cursor_N_live; tnull = 0; }
                    if (tnull) OUT(",\"total\":null");
                    else       OUT(",\"total\":%zu", ctotal);
                }
                OUT("}\n");
                sort_array_free(sp_rows,n_pre,n_pre,&sort_owned_bytes);
                for (int i = 0; i < MAX_ORDER_FIELDS; i++) free(cc.last_value[i]);
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
        cc.ospec       = &ospec;
        cc.deadline    = &cdl;
        cc.prefilter_ks = cursor_prefilter_ks;
        cc.parent_out   = g_out;

        /* Cursor response always uses the {rows:..., cursor:...} wrapper so
           clients get a single stable shape regardless of format. dict_fmt
           swaps the inner array for an object. */
        OrderWalkBounds owb;
        if (ospec.n == 1) {
            if(order_walk_bounds_checked(tree,&driver_fs,order_by,&owb)){
                int error=errno;keyset_free(cursor_prefilter_ks);
                free_joins(joins,njoins);free_excluded(&excluded);return sort_request_error(error);
            }
        } else {
            /* Multi-field: walk the composite's full range; range/eq
               bound folding into the walk is single-field only. */
            memset(&owb, 0, sizeof(owb));
        }
        OUT(dict_fmt ? "{\"rows\":{" : "{\"rows\":[");
        const char *walk_idx = (ospec.n == 1) ? order_by : composite_name;
        if (desc) {
            /* DESC: start (upper) = cursor when resuming, else window-high
               or OPEN. The legacy \xff*4 max-sentinel is wrong here: the
               ordered walk prefixes caller bounds with the 0x00 tier byte,
               so the sentinel becomes a tier-0 ceiling and the NULL rows
               that lead a DESC stream never join the walk. An unconstrained
               first page must pass an open upper bound (§2.4.2/§2.5); a
               criteria-supplied window-high stays (a genuine value ceiling
               correctly excludes the NULL tier). On resume the walk is
               OPEN — the resume bytes carry tier bytes the wrapper would
               re-prefix, and cursor_find_cb's exact tiered tuple compare
               does the filtering (§2.5). */
            const char *hi_b = (has_cur_bytes || !owb.has_hi) ? NULL
                             : (const char *)owb.hi;
            size_t hi_l = (has_cur_bytes || !owb.has_hi) ? 0 : owb.hi_len;
            int    hi_e = owb.has_hi ? owb.hi_excl : 0;
            /* Mirror of the ASC note: on resume the unbounded low side is
               open so the walk reaches every remaining NULL-tier row. */
            const char *lo_b = (has_cur_bytes || !owb.has_lo) ? NULL
                             : (const char *)owb.lo;
            size_t lo_l = (has_cur_bytes || !owb.has_lo) ? 0 : owb.lo_len;
            int    lo_e = owb.has_lo ? owb.lo_excl : 0;
            btree_idx_walk_ordered(db_root, object, walk_idx, sch.splits,
                                   lo_b, lo_l, lo_e,
                                   hi_b, hi_l, hi_e,
                                   1, cursor_find_cb, &cc);
        } else {
            /* ASC: start (lower) = cursor when resuming, else window-low or "";
               stop (upper) = window-high or max. On resume BOTH unbounded
               sides are open — the sentinel would clamp the walk below
               the NULL tier, and NULL rows must join the stream (§2.5;
               cursor_find_cb filters exactly). */
            const char *lo_b = has_cur_bytes ? NULL
                             : (owb.has_lo ? (const char *)owb.lo : "");
            size_t lo_l = has_cur_bytes ? 0 : (owb.has_lo ? owb.lo_len : 0);
            int    lo_e = 0;
            const char *hi_b = (has_cur_bytes || !owb.has_hi) ? NULL
                             : (const char *)owb.hi;
            size_t hi_l = (has_cur_bytes || !owb.has_hi) ? 0 : owb.hi_len;
            int    hi_e = owb.has_hi ? owb.hi_excl : 0;
            btree_idx_walk_ordered(db_root, object, walk_idx, sch.splits,
                                   lo_b, lo_l, lo_e,
                                   hi_b, hi_l, hi_e,
                                   0, cursor_find_cb, &cc);
        }
        if(cc.predicate_error){
            int error=cc.predicate_error;
            if(cursor_prefilter_ks)keyset_free(cursor_prefilter_ks);
            for(int i=0;i<MAX_ORDER_FIELDS;i++)free(cc.last_value[i]);
            free(cc.last_key_str);
            free_joins(joins,njoins);free_excluded(&excluded);errno=error;return -1;
        }
        OUT(dict_fmt ? "}" : "]");

        /* Emit next-page cursor if we actually hit the limit (there might be
           more). If printed < limit the walk drained to the end → null. */
        if (!cc.predicate_error && cc.printed >= limit && cc.last_key_str) {
            int vals_ok = 1;
            for (int i = 0; i < ospec.n; i++)
                /* NULL part (tier 1) carries no value string — see the
                   ASC-note above; the token must still be emitted. */
                if (!cc.last_value[i] && !cc.last_tiers[i]) vals_ok = 0;
            if (vals_ok) {
                /* v2 token: per-field tier bytes so page N+1 rebuilds the
                   tiered resume key exactly (§2.5). */
                OUT(",\"cursor\":{");
                for (int i = 0; i < ospec.n; i++)
                    OUT("%s\"%s\":\"%s\"", i ? "," : "", ospec.parts[i].name,
                        cc.last_value[i] ? cc.last_value[i] : "");
                OUT(",\"tiers\":[");
                for (int i = 0; i < ospec.n; i++)
                    OUT("%s%d", i ? "," : "", cc.last_tiers[i]);
                OUT("],\"key\":\"%s\"}", cc.last_key_str);
            } else {
                OUT(",\"cursor\":null");
            }
        } else {
            OUT(",\"cursor\":null");
        }
        if (want_total) {
            FilterPlan count_fp = plan_filter(tree, db_root, object,
                &driver_fs, sch.splits, cursor_N_live,
                NULL, 0 /*fetching*/, 0 /*limit*/);
            int tnull = 1;
            size_t ctotal = cursor_total_ready ? cursor_total
                : fp_compute_total(&count_fp, tree, db_root,
                                   object, &sch, &driver_fs,
                                   &cdl, &tnull,
                                   cursor_prefilter_ks);
            if (cursor_total_ready) tnull = cursor_total_null;
            if (tnull && tree == NULL) { ctotal = cursor_N_live; tnull = 0; }
            if (tnull) OUT(",\"total\":null");
            else       OUT(",\"total\":%zu", ctotal);
        }
        OUT("}\n");

        if (cursor_prefilter_ks) keyset_free(cursor_prefilter_ks);
        for (int i = 0; i < MAX_ORDER_FIELDS; i++) free(cc.last_value[i]);
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

    PreparedBitmapFind prep;
    memset(&prep, 0, sizeof(prep));
    {
        size_t test_limit = query_test_prepared_budget_limit_value();
        prep.budget.limit = test_limit ? test_limit
                                       : (size_t)g_query_buffer_max_bytes;
    }
    atomic_init(&prep.budget.used, 0);
    atomic_init(&prep.budget.peak, 0);
    if (prepared_find_prepare(&prep, &fp, tree, db_root, object, &sch,
                              (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                              &dl, (order_by && order_by[0] && !has_joins),
                              has_joins, rows_fmt, csv_delim != 0, njoins,
                              offset, limit, want_total, order_by) != 0) {
        int err = errno ? errno : EIO;
        prepared_find_destroy(&prep);
        bm_find_candidates_disabled = prep.record_fallback;
        prepared_find_report(err);
        free_excluded(&excluded);
        free_joins(joins, njoins);
        errno = err;
        return -1;   /* guard clears the flag */
    }
    bm_find_candidates_disabled = prep.record_fallback;
    /* Candidate-derived totals are invalid once recovery discarded them;
       prepared_find_recover already recomputed from the full record scan. */

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
        if (want_total) ex_total = prepared_or_fp_total(&prep, &fp, tree, db_root, object,
                                                    &sch, &driver_fs, &dl, &ex_null);
        if(ex_total==SIZE_MAX){free_excluded(&excluded);free_joins(joins,njoins);return -1;}
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
        int composite_rc=find_via_composite_prefix(
            db_root, object, &sch,
            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
            fp.source_leaves[0], order_by, desc,
            fp.order_range,
            tree, &excluded, offset, limit,
            proj_fields, proj_count, dict_fmt, &dl);
        if(composite_rc==-2)goto find_ordered_fallback;
        if(composite_rc<0){free_excluded(&excluded);free_joins(joins,njoins);return -1;}

        /* Close the envelope opened above and return.  When want_total,
         * compute the real match count via fp_compute_total (extra walk —
         * same work the client would do firing a separate count; this
         * consolidates it into the find round-trip). */
        size_t d1_total = 0; int d1_null = 1;
        if (want_total) d1_total = prepared_or_fp_total(&prep, &fp, tree, db_root, object,
                                                    &sch, &driver_fs, &dl, &d1_null);
        if(d1_total==SIZE_MAX){free_excluded(&excluded);free_joins(joins,njoins);return -1;}
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
        if(find_via_order_index_walk(
            db_root, object, &sch,
            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
            order_by, desc,
            tree, &excluded, offset, limit,
            proj_fields, proj_count, dict_fmt, &dl)<0){free_excluded(&excluded);free_joins(joins,njoins);return -1;}
        /* Close the envelope opened above and return.  When want_total,
         * compute the real match count via fp_compute_total. */
        size_t d3_total = 0; int d3_null = 1;
        if (want_total) d3_total = prepared_or_fp_total(&prep, &fp, tree, db_root, object,
                                                    &sch, &driver_fs, &dl, &d3_null);
        if(d3_total==SIZE_MAX){free_excluded(&excluded);free_joins(joins,njoins);return -1;}
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
               fp.n_source > 0 &&
               !prep.record_fallback &&   /* ordered-collect instead */
               (!order_by || strchr(order_by, ',') == NULL)) {
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
        int desc = ospec.parts[0].dir;   /* per-part :asc/:desc suffixes override the shared order */
        size_t d2_matched = find_via_fetch_sort(
            db_root, object, &sch,
            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
            &fp, order_by, desc,
            tree, &excluded, offset, limit,
            proj_fields, proj_count, dict_fmt, &dl,
            prep.d2_prefilter, prep.d2_ready, &prep.budget);
        prep.d2_prefilter = NULL;   /* ownership handed off */
        if(d2_matched==SIZE_MAX){free_excluded(&excluded);free_joins(joins,njoins);return -1;}
        if (dict_fmt) {
            if (!want_total) OUT("}\n");
            else if (prep.total_ready && prep.total_null)
                OUT("},\"total\":null}\n");
            else OUT("},\"total\":%zu}\n", d2_matched);
        } else {
            if (!want_total) OUT("]\n");
            else if (prep.total_ready && prep.total_null)
                OUT("],\"total\":null}\n");
            else OUT("],\"total\":%zu}\n", d2_matched);
        }
        free_excluded(&excluded);
        free_joins(joins, njoins);
        return 0;
    }

    /* ===== ORDERED FIND — indexed-walk fast path =====
       When order_by is indexed and there are no excluded keys, walk the
       btree in sort order, skip the first `offset` matching entries,
       emit the next `limit`. Bypasses the full-table scan + qsort entirely.

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

        int desc = ospec.parts[0].dir;   /* per-part :asc/:desc suffixes override the shared order */

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
            /* Optional probe: any failure discards the whole optimization
               and the walk matches full records instead. */
            if (!prefilter_ks) errno = 0;
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
                } else {
                    errno = 0;   /* keep the complete broader prefilter */
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
                SmallPrefilterRow *rows=sort_array_resize(NULL,0,n_pre,&sort_owned_bytes,g_query_buffer_max_bytes);
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
                        SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &sinfo);
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
                                .sort_owned_bytes=&sort_owned_bytes,.sort_overflow_budget=g_query_buffer_max_bytes,
                            };
                            if(slotcask_bulk_resolve_and_fetch(sdb,hashes,n_pre,&d2_ctx,d2_batch_cb)<0)
                                sort_error=errno?errno:EIO;
                            for(size_t pi=0;pi<n_pre;pi++)if(passed[pi]<0)sort_error=-passed[pi];
                        }
                        if (!sort_error && n_kept > 0 && hashes && passed && hmap) {
                            int keep = 0;
                            for (size_t i = 0; i < n_pre; i++) {
                                if(passed[i]>0){small_row_move(&rows[keep++],&rows[i]);}
                            }
                            n_kept = keep;
                        }
                        free(hashes);
                        free(passed);
                        free(hmap);
                    }
                    if (!sort_error && n_kept == 0) {
                        for (size_t i = 0; i < n_pre; i++) {
                            RecordRef rr;
                            if (read_record_ref(db_root, object, &sch,
                                                 rows[i].hash, &rr) != 0) continue;
                            int matched=0;
                            if(criteria_match_tree_checked(rr.val,rr.vlen,tree,&driver_fs,rr.hdr2,&matched)){
                                sort_error=errno?errno:EILSEQ;release_record_ref(&rr);break;
                            }
                            if(!matched){release_record_ref(&rr);continue;}
                            SmallPrefilterRow candidate={0};memcpy(candidate.hash,rows[i].hash,16);
                            if(small_row_build(&candidate,driver_fs.ts,rr.val,rr.vlen,rr.hdr2,
                                order_field_idx,&sort_owned_bytes,g_query_buffer_max_bytes)){
                                sort_error=errno?errno:EILSEQ;release_record_ref(&rr);break;
                            }
                            small_row_move(&rows[n_kept++],&candidate);
                            release_record_ref(&rr);
                        }
                    }

                    if(sort_error){
                        sort_array_free(rows,n_pre,n_pre,&sort_owned_bytes);keyset_free(prefilter_ks);
                        OUT(dict_fmt?(want_total?"},\"total\":null}\n":"}\n"):
                            (want_total?"],\"total\":null}\n":"]\n"));
                        free_joins(joins,njoins);free_excluded(&excluded);return sort_request_error(sort_error);
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
                        if (cursor_find_cb("", 0, rows[i].hash, NULL, &cc) < 0) break;
                    }

                    sort_array_free(rows,n_pre,n_pre,&sort_owned_bytes);
                    for (int i = 0; i < MAX_ORDER_FIELDS; i++) free(cc.last_value[i]);
                    free(cc.last_key_str);
                    keyset_free(prefilter_ks);

                    if(cc.predicate_error){free_joins(joins,njoins);free_excluded(&excluded);errno=cc.predicate_error;return -1;}

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
                if(order_walk_bounds_checked(tree,&driver_fs,order_by,&owb)){
                    int error=errno;if(prefilter_ks)keyset_free(prefilter_ks);
                    free_joins(joins,njoins);free_excluded(&excluded);errno=error;return -1;
                }
                /* No keyset cursor on this path (offset_mode) — unbounded
                   sides pass through OPEN so NULL-tier rows join the
                   ordered stream (§2.5). */
                const char *lo_b = owb.has_lo ? (const char *)owb.lo : NULL;
                size_t lo_l = owb.has_lo ? owb.lo_len : 0;
                int    lo_e = owb.has_lo ? owb.lo_excl : 0;
                const char *hi_b = owb.has_hi ? (const char *)owb.hi : NULL;
                size_t hi_l = owb.has_hi ? owb.hi_len : 0;
                int    hi_e = owb.has_hi ? owb.hi_excl : 0;
                btree_idx_walk_ordered(db_root, object, order_by, sch.splits,
                                       lo_b, lo_l, lo_e,
                                       hi_b, hi_l, hi_e,
                                       desc, cursor_find_cb, &cc);
            }
            if(cc.predicate_error){
                int error=cc.predicate_error;if(prefilter_ks)keyset_free(prefilter_ks);
                for(int i=0;i<MAX_ORDER_FIELDS;i++)free(cc.last_value[i]);
            free(cc.last_key_str);
                free_joins(joins,njoins);free_excluded(&excluded);errno=error;return -1;
            }
            if (dict_fmt) {
                if (!want_total) OUT("}\n");
                else if (!tree) OUT("},\"total\":%zu}\n", find_N_live);
                else OUT("},\"total\":null}\n");
            } else if (rows_fmt) {
                if (!want_total) OUT("]\n");
                else if (!tree) OUT("],\"total\":%zu}\n", find_N_live);
                else OUT("],\"total\":null}\n");
            } else {
                if (!want_total) OUT("]\n");
                else if (!tree) OUT("],\"total\":%zu}\n", find_N_live);
                else OUT("],\"total\":null}\n");
            }

            if (prefilter_ks) keyset_free(prefilter_ks);
            for (int i = 0; i < MAX_ORDER_FIELDS; i++) free(cc.last_value[i]);
            free(cc.last_key_str);
            free_joins(joins, njoins);
            free_excluded(&excluded);
            return 0;
        }
    }

find_ordered_fallback: ;
    if (has_order) {
        /* ===== ORDERED FIND — buffered fallback =====
           Reached when order_by isn't indexed, excluded keys are present,
           or format=csv (CSV emit isn't wired into cursor_find_cb yet).
           Buffers all matches, qsort by order_by, slices [offset, offset+limit]. */

        /* ospec (validated, direction-merged) was resolved once in
           cmd_find_do; the buffered path consumes it directly. */

        OrderedCollectCtx oc;
        memset(&oc, 0, sizeof(oc));
        oc.tree = tree;
        oc.fs = (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL;
        oc.ospec = &ospec;
        oc.nsort = ospec.n;
        oc.order_field_name = order_by;
        oc.excluded = &excluded;
        oc.deadline = &dl;
        pthread_mutex_init(&oc.lock, NULL);

        scan_dispatch(db_root, object, &sch, data_dir, ordered_collect_cb, &oc);

        if (oc.budget_exceeded||oc.predicate_error) {
            LOG_WARN(LOG_SUB_QUERY, "cmd_find_do: query buffer cap exceeded during ordered sort/fetch (object=%s)", object);
            for (size_t i = 0; i < oc.count; i++) {
                free(oc.rows[i].key); free(oc.rows[i].record);
                row_sort_keys_release(oc.rows[i].keys);
            }
            free(oc.rows);
            pthread_mutex_destroy(&oc.lock);
            OUT(dict_fmt?(want_total?"},\"total\":null}\n":"}\n"):
                (want_total?"],\"total\":null}\n":"]\n"));
            free_excluded(&excluded);free_joins(joins,njoins);
            return sort_request_error(oc.predicate_error?oc.predicate_error:ENOBUFS);
        }

        if (oc.count > 1)
            qsort(oc.rows, oc.count, sizeof(OrderedRow), cmp_row_asc);

        size_t start=offset>0?(size_t)offset:0;
        size_t end=limit>0?start+(size_t)limit:oc.count;
        if(end>oc.count)end=oc.count;
        int printed=0,emit_error=0;
        for(size_t i=start;i<end;i++){
            OrderedRow *r=&oc.rows[i];char wire_key[1100];
            format_query_wire_key(&driver_fs,(const uint8_t*)r->key,r->key_len,wire_key);
            const uint8_t *value=r->record+r->key_len;
            if(typed_query_emit_row(g_out,wire_key,strlen(wire_key),value,r->value_len,r->hdr2,
                &driver_fs,proj_count>0?proj_fields:NULL,proj_count,rows_fmt,dict_fmt,
                csv_delim,printed>0,NULL,0,NULL,g_query_buffer_max_bytes)){
                emit_error=errno?errno:EILSEQ;break;
            }
            printed++;
        }

        for (size_t i = 0; i < oc.count; i++) {
            free(oc.rows[i].key);
            free(oc.rows[i].record);
            row_sort_keys_release(oc.rows[i].keys);
        }
        free(oc.rows);
        pthread_mutex_destroy(&oc.lock);
        if(emit_error){free_excluded(&excluded);free_joins(joins,njoins);errno=emit_error;return -1;}
    } else if (prep.route != PREP_NONE && !prep.record_fallback) {
        /* ===== Prepared bitmap sources (candidates built pre-emission) =====
           Bitmap reads already happened before the envelope; only record
           fetch, filtering, pagination and emission remain. The former
           small-limit streaming shapes fetch one record at a time; the
           already-buffered shapes keep keyset_emit_find. */
        FieldSchema *pfs = (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL;
        int prc = 0;
        if (prep.ks) {
            if (prep.route == PREP_STREAM)
                prc = prepared_bitmap_find_stream(db_root, object, &sch, prep.ks,
                                                  tree, &excluded, offset, limit,
                                                  proj_fields, proj_count, pfs,
                                                  rows_fmt, dict_fmt, csv_delim, &dl);
            else
                prc = keyset_emit_find(db_root, object, &sch, prep.ks,
                                       prep.need_rematch ? tree : NULL,
                                       &excluded, offset, limit,
                                       proj_fields, proj_count, pfs,
                                       rows_fmt, dict_fmt, csv_delim,
                                       joins, njoins, &dl);
        }
        prepared_find_destroy(&prep);
        if (prc < 0) { free_excluded(&excluded); free_joins(joins, njoins); return -1; }
        if (want_total && prep.total_ready) {
            find_total = prep.total;
            find_total_null = prep.total_null;
        }
    } else if (prep.record_fallback) {
        /* ===== Whole-record recovery (one-way) =====
           Candidate capacity overflow: run the existing full-record
           executors with the original criteria, exclusions, joins,
           projection, offset/limit and formatting. No bitmap candidates,
           prefilters or probes run again (build_keyset_from_plan refuses);
           the deadline is the same object; no re-plan into bitmap routes. */
        FieldSchema *rfs = (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL;
        const CompiledCriterion *fast_cc = NULL;
        if (tree) {
            const CriteriaNode *leaf = NULL;
            if (tree->kind == CNODE_LEAF) leaf = tree;
            else if (tree->kind == CNODE_AND && tree->n_children == 1 &&
                     tree->children[0]->kind == CNODE_LEAF) leaf = tree->children[0];
            if (leaf && leaf->compiled) fast_cc = leaf->compiled;
        }
        AdvSearchCtx rctx = { tree, fast_cc, offset, limit, 0, 0,
                              proj_fields, proj_count, excluded, rfs,
                              rows_fmt, dict_fmt, csv_delim,
                              object, joins, njoins, db_root, &dl, 0,
                              PTHREAD_MUTEX_INITIALIZER, 0 };
        int use_streaming = (limit > 0 && limit <= 1000) &&
                            !has_order;   /* ordered shapes use ordered-collect */
        if (use_streaming) {
            SlotcaskSchemaInfo info = {
                .splits = sch.splits, .slot_size = sch.slot_size,
                .streams = sch.streams,
            };
            SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
            if (sdb) scan_shards_v2_streaming(sdb, adv_search_cb, &rctx);
            else scan_dispatch(db_root, object, &sch, data_dir, adv_search_cb, &rctx);
        } else {
            scan_dispatch(db_root, object, &sch, data_dir, adv_search_cb, &rctx);
        }
        pthread_mutex_destroy(&rctx.lock);
        if (rctx.predicate_error) {
            free_excluded(&excluded); free_joins(joins, njoins);
            errno = rctx.predicate_error; return -1;
        }
        if (want_total && prep.total_ready) {
            find_total = prep.total;
            find_total_null = prep.total_null;
        }
        prepared_find_destroy(&prep);
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
        if(idx_find_streaming(db_root, object, &sch, primary, check_primary,
                           tree, &excluded, offset, limit,
                           proj_fields, proj_count,
                           (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                           rows_fmt, dict_fmt, csv_delim, &dl)<0){free_excluded(&excluded);free_joins(joins,njoins);return -1;}
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
        if(keyset_find_from_intersect(db_root, object, &sch, &fp, tree,
                                   &excluded, offset, limit,
                                   proj_fields, proj_count,
                                   (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                                   rows_fmt, dict_fmt, csv_delim, joins, njoins, &dl,
                                   want_total ? &find_total : NULL)<0){free_excluded(&excluded);free_joins(joins,njoins);return -1;}
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
                int trc = find_via_trigram_starts_with(
                    db_root, object, &sch,
                    (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                    pc, tree, &excluded, offset, limit,
                    proj_fields, proj_count, dict_fmt, &dl);
                if (trc < 0) { free_excluded(&excluded); free_joins(joins, njoins); return -1; }
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
                /* Observable totals validate at the Kf boundary:
                   idx_count_for_leaf routes bitmap candidates through
                   the record-fetch revalidation; raw popcount is not a
                   count answer. */
                find_total = idx_count_for_leaf(db_root, object, &sch,
                                                &driver_fs, pc, &dl);
                if (!dl.timed_out) find_total_null = 0;
            } else {
                /* FP_PRIMARY_LEAF: idx_count_for_leaf on the seed */
                find_total = idx_count_for_leaf(db_root, object, &sch,
                                                &driver_fs, pc, &dl);
                if (!dl.timed_out) find_total_null = 0;
            }
        }

        find_emit_close:
        if(rc==-1){free_excluded(&excluded);free_joins(joins,njoins);return -1;}
        if (rc == -2) {
            LOG_WARN(LOG_SUB_QUERY, "cmd_find_do: scan dispatch returned rc=-2 (query buffer cap exceeded, object=%s)", object);
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
        if(keyset_find_from_or(db_root, object, &sch, tree, fp.or_node,
                            &excluded, offset, limit, proj_fields, proj_count,
                            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                            rows_fmt, dict_fmt, csv_delim, joins, njoins, &dl, &budget_exceeded,
                            want_total ? &find_total : NULL)<0){free_excluded(&excluded);free_joins(joins,njoins);return -1;}
        if (!budget_exceeded && want_total && !dl.timed_out) find_total_null = 0;
        if (budget_exceeded) {
            LOG_WARN(LOG_SUB_QUERY, "cmd_find_do: query buffer cap exceeded during OR union (object=%s)", object);
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
                             PTHREAD_MUTEX_INITIALIZER, 0 };
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
            SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
            if (sdb) scan_shards_v2_streaming(sdb, adv_search_cb, &ctx);
            else     scan_dispatch(db_root, object, &sch, data_dir, adv_search_cb, &ctx);
        } else {
            scan_dispatch(db_root, object, &sch, data_dir, adv_search_cb, &ctx);
        }
        pthread_mutex_destroy(&ctx.lock);
        if(ctx.predicate_error){free_excluded(&excluded);free_joins(joins,njoins);errno=ctx.predicate_error;return -1;}
    }

    if(find_total==SIZE_MAX){free_excluded(&excluded);free_joins(joins,njoins);return -1;}
    if (csv_delim)
        { /* CSV body already ends with its own \n per row — nothing to close (joined or not) */ }
    else if (want_total) {
        /* Phase 1d follow-up: if the dispatch path didn't compute total
         * cheaply (FP_FULL_SCAN, postfilter-bearing plans), call the
         * shared fp_compute_total helper. Falls back to null when the
         * plan can't cheaply produce a count (e.g. FP_FULL_SCAN — caller
         * can fire a separate count query for those rare shapes). */
        if (find_total_null && prep.total_ready) {
            find_total = prep.total;
            find_total_null = prep.total_null;
        } else if (find_total_null && !dl.timed_out) {
            int helper_null = 1;
            size_t n = fp_compute_total(&fp, tree, db_root, object,
                                         &sch, &driver_fs, &dl, &helper_null, NULL);
            if(n==SIZE_MAX){free_excluded(&excluded);free_joins(joins,njoins);return -1;}
            if (!helper_null) {
                find_total = n;
                find_total_null = 0;
            }
        }
        const char *arr_close = (has_joins || rows_fmt) ? "]" :
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

static int cmd_find_do(const char *root,const char *object,CriteriaNode *tree,
 JoinSpec *joins,int njoins,int offset,int limit,const char *proj,const char *excluded,
 const char *format,const char *delimiter,const char *order_by,const char *order_dir,
 const char *cursor,int total){
    return cmd_find_do_impl(root,object,tree,joins,njoins,offset,limit,proj,excluded,
        format,delimiter,order_by,order_dir,cursor,total,NULL,NULL);
}
#ifdef TEST_BUILD
int cmd_find_with_schema_for_test(const char *root,const char *object,
 const Schema *schema,const FieldSchema *fields,CriteriaNode *tree,int offset,int limit,
 const char *format,const char *order_by,const char *order_dir,const char *cursor){
    if(!schema||!fields||!fields->ts){errno=EINVAL;return -1;}
    return cmd_find_do_impl(root,object,tree,NULL,0,offset,limit,NULL,NULL,
        format,NULL,order_by,order_dir,cursor,0,schema,fields);
}
#endif

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
                  const char *cursor_json,
                  JoinSpec *joins, int njoins) {
    int r = cmd_find_do(db_root, object, tree,
                       joins, njoins,
                       offset, limit, proj_str,
                       NULL,              /* no excluded_csv */
                       format, delimiter,
                       order_by, order_dir,
                       cursor_json,
                       want_total);
    return r;
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
    if (compile_criteria_tree_checked(tree,fs.ts,NULL,0)) {
        free_criteria_tree(tree); return -1;
    }
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
    int result = find_via_composite_prefix(
        eff_root, bare, &sc,
        (fs.ts || fs.nfields > 0) ? &fs : NULL,
        fp.source_leaves[0], order_by, order_desc,
        fp.order_range,
        tree, &excluded, 0, limit,
        NULL, 0, 0, &dl);
    int error = errno;
    long scanned = g_order_walk_scanned;
    fclose(g_out);
    g_out = saved_out;
    free_criteria_tree(tree);
    if (result < 0) { errno = error; return result; }
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
    if (compile_criteria_tree_checked(tree,fs.ts,NULL,0)) {
        free_criteria_tree(tree); return -1;
    }
    size_t N = (size_t)get_live_count(eff_root, bare);
    FilterPlan fp = plan_filter(tree, eff_root, bare, &fs, sc.splits, N,
                                 order_by, 1, 0);
    if (fp.order != FP_ORDER_COMPOSITE || fp.kind != FP_PRIMARY_LEAF ||
        fp.n_source < 1 || !fp.source_leaves[0]) {
        free_criteria_tree(tree);
        return -2;
    }
    SearchCriterion *seed = fp.source_leaves[0];
    const char *text = seed->op == OP_IN && seed->in_count > 0
        ? seed->in_values[0] : search_criterion_value(seed);
    size_t text_len = seed->op == OP_IN && seed->in_count > 0
        ? strlen(text) : search_criterion_value_len(seed);
    uint8_t lo[544], hi[544]; size_t lo_len=0, hi_len=0;
    int lo_excl=0, hi_excl=0;
    int rc=composite_prefix_bounds(&fs,seed,text,text_len,order_by,
                                  fp.order_range,lo,&lo_len,&lo_excl,
                                  hi,&hi_len,&hi_excl);
    if(rc){free_criteria_tree(tree);return rc;}
    if(out_hi&&out_hi_len){memcpy(out_hi,hi,hi_len);*out_hi_len=hi_len;}
    if(out_lo&&out_lo_len){memcpy(out_lo,lo,lo_len);*out_lo_len=lo_len;}
    if(out_min_excl)*out_min_excl=lo_excl;
    if(out_max_excl)*out_max_excl=hi_excl;
    free_criteria_tree(tree);
    return (int)hi_len;
}
#endif
