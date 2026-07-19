#ifndef QUERY_INTERNAL_H
#define QUERY_INTERNAL_H
#include "types.h"
#include <pthread.h>
/* Prototypes for helpers shared across the query_*.c translation units.
   These were `static` inside the monolithic query.c; splitting the file
   forces them to external linkage. Keep this header private to src/db. */

typedef struct SlotcaskDb SlotcaskDb;  /* defined in slotcask.h */

#define MAX_INTERSECT_LEAVES 8

/* Filter plan types — shared by query.c and query_aggregate.c */
typedef enum {
    FP_FULL_SCAN,        /* nothing indexed; cache-isolated scan + match_tree   (A5,B7,C2) */
    FP_PRIMARY_LEAF,     /* one selective indexed leaf seeds; rest post-filtered (A1,A3,A4,B2,B4,C3,B5-fetch) */
    FP_BITMAP_SMALLER,   /* lone/all-broad bitmap; smaller-of {match,complement} (A2,B3)   */
    FP_INTERSECT,        /* 2+ indexed leaves intersected index-only             (B1/B6 count, B3) */
    FP_UNION             /* pure OR, every child indexed; union keysets          (C1)   */
} FilterPlanKind;

typedef enum {
    FP_ORDER_NONE,              /* no order_by */
    FP_ORDER_COMPOSITE,         /* (filter+order) composite → sorted prefix scan       (D1) */
    FP_ORDER_COMPOSITE_EXACT,   /* all composite fields pinned by eq → exact key lookup (Phase B) */
    FP_ORDER_SORT,              /* bounded candidate set → sort in memory              (D2) */
    FP_ORDER_INDEX_WALK         /* candidates too broad → walk order_by index to limit (D3) */
} FilterOrderKind;

typedef struct FilterPlan {
    FilterPlanKind   kind;
    FilterOrderKind  order;                              /* overlay; NONE when no order_by */
    SearchCriterion *source_leaves[MAX_INTERSECT_LEAVES];/* seed: LEAF→[0]; INTERSECT→[0..n_source) */
    int              n_source;
    int              source_is_bitmap;                   /* primary seed is a bitmap (BITMAP_SMALLER, or bitmap primary) */
    CriteriaNode    *or_node;                            /* FP_UNION */
    SearchCriterion *postfilter_leaves[MAX_INTERSECT_LEAVES]; /* leaves not covered by source */
    int              n_postfilter;
    int              total_cheap;                        /* plan materializes a KeySet → |KeySet| is a free total (1d) */
    int              fetching;                           /* dimension this plan was computed for */
    char             composite_field[256];               /* set when order==FP_ORDER_COMPOSITE_EXACT */
    SearchCriterion *order_range;     /* range/eq leaf on order_by to fold into the composite seek (EQ seed only) */
    size_t           prefilter_card;  /* estimated lower-bound cardinality for cursor prefilter sizing;
                                         0 = unknown (use keyset_size directly) */
    SearchCriterion *prefilter_source_leaf; /* the leaf that gave prefilter_card (NULL = no narrow leaf) */
} FilterPlan;

/* Shared types */
typedef struct {
    uint8_t hash[16];
    int shard_id;
    int start_slot;
} CollectedHash;

typedef struct {
    CriteriaNode *tree;
    FieldSchema *fs;
    char **keys;
    _Atomic int count;
    int cap;
    int limit;
    QueryDeadline *deadline;
    int dl_counter;
    size_t buffer_bytes;
    _Atomic int budget_exceeded;
    pthread_mutex_t lock;
} BulkCriteriaCtx;

/* ========== Joins ========== */
enum JoinType { JOIN_INNER = 0, JOIN_LEFT = 1 };

typedef struct JoinSpec {
    char object[256];
    char local_field[256];
    char remote_field[256];       /* "key" OR indexed field name */
    char as_name[256];
    int  type;                    /* JOIN_INNER | JOIN_LEFT */

    char proj_fields[MAX_FIELDS][256];
    int  proj_count;              /* 0 filled by resolver if no explicit fields list */
    int  include_remote_key;      /* user requested "key" in fields array */

    /* Resolved at parse time */
    Schema       remote_sch;
    FieldSchema  remote_fs;
    int          remote_is_key;
    char         remote_idx_path[PATH_MAX];

    /* Pre-resolved TypedFields for projection (NULL → composite/unknown fallback) */
    const TypedField *proj_tfs[MAX_FIELDS];

    /* Driver-side local field (NULL iff composite or unknown) */
    const TypedField *local_tf;
    int local_is_composite;
} JoinSpec;

/* ========== v2 scan bridge (shared by query.c + query_find.c) ========== */
typedef struct {
    scan_callback cb;
    void         *ctx;
} V2ScanWrap;

/* Adapter context for od_seg_record_cb */
typedef struct {
    V2ScanWrap *wrap;      /* wraps the real scan_callback */
    int        *stop_flag;
} OdSegAdapterCtx;

/* query.c — rebuild engine (used by query_schema.c::cmd_edit_fields) */
typedef struct {
    int (*apply_metadata)(void *ctx);
    int (*rebuild_indexes)(void *ctx, int *out_rebuilt);
    void *ctx;
    int indexes_may_change;
} RebuildFinalizeOps;

int rebuild_object_v2(const char *db_root, const char *object,
                      const Schema *old_sch, const TypedSchema *old_ts,
                      const Schema *new_sch, TypedSchema *new_ts,
                      int *new_to_old, int slot_changed,
                      int splits_changed, int drop_tombstoned,
                      char added_lines[][256], int n_added,
                      const RebuildFinalizeOps *finalize);
int v2_rebuild_walk_cb(const uint8_t hash16[16],
                       const void *key, size_t klen,
                       const void *value, size_t vlen,
                       void *ctxp);
int update_schema_conf_splits_streams(const char *db_root, const char *object,
                                     int new_splits, int new_streams);
int parse_field_line(const char *line, TypedField *out);
void transform_field_value(const TypedField *old_f,
                           const TypedField *new_f,
                           const uint8_t *src,
                           uint8_t *dst);
int field_needs_transform(const TypedField *old_f,
                          const TypedField *new_f);

/* query.c — bulk criteria (used by query_bulk.c::cmd_bulk_delete_criteria) */
int bulk_delete_phase1_indexed(const char *db_root, const char *object,
                               const Schema *sch, FieldSchema *fs,
                               CriteriaNode *tree, int limit,
                               QueryDeadline *dl,
                               BulkCriteriaCtx *out_ctx);


/* query_find.c — CSV output (used by query.c find/count paths) */
void csv_emit_header(const char **proj_fields, int proj_count,
                     FieldSchema *fs, char delim);
void csv_emit_row(const char *key, const uint8_t *raw, size_t val_len,
                  const char **proj_fields, int proj_count,
                  FieldSchema *fs, char delim);

/* query_find.c — shard grouping (used by query.c find/count paths) */
int cmp_by_shard(const void *a, const void *b);
int shard_group_batch(CollectedHash *batch, int batch_count,
                      int *group_starts, int *group_sizes,
                      int max_groups);

/* query_find.c — O_DIRECT scan helpers (used by query.c find/count paths) */
int od_seg_record_cb(const uint8_t *rec, size_t vlen,
                     const uint8_t hash16[16], void *raw_ctx);
void scan_shards_v2_o_direct_match(SlotcaskDb *db,
                                   FieldSchema *fs,
                                   const CompiledCriterion *single_cc,
                                   const CriteriaNode *tree,
                                   QueryDeadline *dl,
                                   int64_t *out_count);
void scan_shards_v2_streaming(SlotcaskDb *db, scan_callback cb, void *ctx);

/* query_find.c — row output (used by query.c find/keys paths) */
void emit_rows_columns(const char **proj_fields, int proj_count,
                       FieldSchema *fs);

/* query.c — aggregate support (used by query_aggregate.c) */
int validate_criteria_tree_fields(const CriteriaNode *n,
                                  const TypedSchema *ts,
                                  char *err, size_t err_sz);
int validate_field(const TypedSchema *ts, const char *name,
                   const char *label, char *err, size_t err_sz);
int composite_subfields_known(const TypedSchema *ts, const char *name,
                              char *bad, size_t bad_sz);
int field_known(const TypedSchema *ts, const char *name);
KeySet *build_or_keyset(const char *db_root, const char *object, int splits,
                        const CriteriaNode *or_node, QueryDeadline *dl,
                        int *out_budget_exceeded, int target_count);
KeySet *intersect_indexed_leaves(const char *db_root, const char *object,
                                int splits,
                                SearchCriterion **leaves, int n,
                                QueryDeadline *dl,
                                int *out_small_primary);
int agg_criteria_fully_covered(const char *db_root, const char *object,
                               CriteriaNode *tree);

/* query.c — inline helpers (moved here so query_aggregate.c can share them) */
static inline int64_t ld_be_i64(const uint8_t *p) {
    uint64_t u = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                 ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                 ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                 ((uint64_t)p[6] << 8)  |  (uint64_t)p[7];
    return (int64_t)u;
}
static inline int32_t ld_be_i32(const uint8_t *p) {
    uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                 ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
    return (int32_t)u;
}
static inline int16_t ld_be_i16(const uint8_t *p) {
    uint16_t u = ((uint16_t)p[0] << 8) | (uint16_t)p[1];
    return (int16_t)u;
}
static inline uint16_t ld_be_u16(const uint8_t *p) {
    return ((uint16_t)p[0] << 8) | (uint16_t)p[1];
}
static inline uint32_t ld_be_u32(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
}
static inline int varchar_eff_len(const uint8_t *p, int size) {
    int len = ((int)p[0] << 8) | (int)p[1];
    int content_max = size - 2;
    if (len > content_max) len = content_max;
    if (len < 0) len = 0;
    return len;
}

/* CollectCtx: mmap-backed hash collector for find/count/aggregate */
typedef struct {
    CollectedHash *entries;
    size_t reservation_bytes;
    size_t count;
    size_t cap;
    int splits;
    int collect_cap;
    SearchCriterion *primary_crit;
    int check_primary;
    const TypedField *tf;
    QueryDeadline *deadline;
    int dl_counter;
    int budget_exceeded;
} CollectCtx;

static inline void collect_ctx_init(CollectCtx *cc) {
    memset(cc, 0, sizeof(*cc));
    cc->reservation_bytes = g_query_buffer_max_bytes;
    cc->entries = mmap(NULL, cc->reservation_bytes,
                       PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (cc->entries == MAP_FAILED) {
        cc->entries = NULL;
        cc->cap = 0;
    } else {
        cc->cap = cc->reservation_bytes / sizeof(CollectedHash);
    }
}

static inline void collect_ctx_destroy(CollectCtx *cc) {
    if (cc->entries) munmap(cc->entries, cc->reservation_bytes);
    cc->entries = NULL;
}

/* StreamKeysetCtx: streaming keyset builder for min/max-with-criteria */
typedef struct {
    KeySet           *ks;
    SearchCriterion  *primary_crit;
    int               check_primary;
    const TypedField *tf;
    QueryDeadline    *deadline;
    int               dl_counter;
    _Atomic int       full;
} StreamKeysetCtx;

/* IdxCountCtx: per-criterion index count */
typedef struct {
    SearchCriterion *primary_crit;
    int check_primary;
    size_t count;
    QueryDeadline *deadline;
    int dl_counter;
    const TypedField *tf;
} IdxCountCtx;

/* query.c — advanced search context (shared by query.c + query_join.c) */
typedef struct {
    CriteriaNode *tree;
    const CompiledCriterion *fast_cc;
    int offset;
    int limit;
    _Atomic int count;
    _Atomic int printed;
    const char **proj_fields;
    int proj_count;
    ExcludedKeys excluded;
    FieldSchema *fs;
    int rows_fmt;
    int dict_fmt;
    char csv_delim;
    const char *driver_object;
    JoinSpec *joins;
    int njoins;
    const char *db_root;
    QueryDeadline *deadline;
    int dl_counter;
    pthread_mutex_t lock;
} AdvSearchCtx;

/* query.c — callback functions (used by query_aggregate.c) */
int collect_hash_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx);
int stream_keyset_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx);
int idx_count_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx);
void idx_count_cb_flush_thread(void);

/* query.c — operator classification (used by query_aggregate.c) */
int op_is_length(enum SearchOp op);
int match_length_vlen(size_t vlen, const SearchCriterion *c);
int op_needs_check_primary(enum SearchOp op);
int op_prefers_trigram(enum SearchOp op);

/* query.c — criterion encoding (used by query_aggregate.c) */
void group_by_csv_to_json(const char *csv, char *out, size_t out_sz);
void encode_criterion_value(const TypedField *tf,
                            const char *val, size_t vlen,
                            uint8_t *buf, size_t *out_len);
const TypedField *resolve_idx_field(const TypedSchema *ts, const char *field);

/* query.c — index selection (used by query_aggregate.c) */
enum IndexType field_index_type(const char *db_root, const char *object,
                                const char *field);
int field_has_index_type(const char *db_root, const char *object,
                         const char *field, enum IndexType want);
int pick_index_for_leaf(const char *db_root, const char *object,
                        const SearchCriterion *c);

/* query.c — btree dispatch (used by query_aggregate.c) */
void btree_dispatch(const char *db_root, const char *object,
                    const char *field, int splits,
                    SearchCriterion *pc, const TypedField *tf,
                    bt_result_cb cb, void *ctx);
int bitmap_emit_for_shard(const char *db_root, const char *object,
                          const char *field, int shard_idx,
                          const uint8_t *value, size_t vlen,
                          bt_result_cb cb, void *ctx, SlotcaskDb *sdb);

/* query.c — criteria tree operations (used by query_aggregate.c) */
void recompile_criteria_tree(CriteriaNode *n, const TypedSchema *ts);
int leaf_is_indexed(const SearchCriterion *c, const char *db_root,
                    const char *object, char *out_idx_path, size_t out_sz);
int find_intersect_leaves(CriteriaNode *root,
                          const char *db_root, const char *object,
                          SearchCriterion *out_leaves[MAX_INTERSECT_LEAVES],
                          char out_paths[MAX_INTERSECT_LEAVES][PATH_MAX],
                          int *out_partial);

/* query.c — filter planner (used by query_aggregate.c) */
FilterPlan plan_filter(CriteriaNode *tree, const char *db_root,
                       const char *object, const FieldSchema *fs,
                       int splits, size_t N, const char *order_by,
                       int fetching, int limit);

/* query.c — bitmap/keyset builders (used by query_aggregate.c) */
size_t bm_popcount_one_value(const char *db_root, const char *object,
                             const char *field, int splits,
                             const uint8_t *value, size_t vlen);
KeySet *build_smaller_bitmap_keyset(const char *db_root, const char *object,
                                    int splits, const SearchCriterion *leaf,
                                    const TypedField *tf, QueryDeadline *dl,
                                    int *inverted);
KeySet *build_keyset_from_trigram(const char *db_root, const char *object,
                                  int splits,
                                  const SearchCriterion *leaf,
                                  QueryDeadline *dl);
KeySet *build_keyset_from_leaf(const char *db_root, const char *object,
                               int splits,
                               SearchCriterion *leaf,
                               QueryDeadline *dl);
int keyset_to_collected_hashes(KeySet *ks, int splits,
                               CollectedHash **out_entries, size_t *out_count);

/* query_aggregate.c — typed field encode/decode (used by query.c join paths) */
int typed_field_to_buf_raw(const TypedField *f, const uint8_t *p,
                           char *buf, size_t bufsz);
int decode_idx_to_buf(const TypedField *f, const uint8_t *p, size_t plen,
                      char *buf, size_t bufsz, int skip_zero);

/* query_join.c — join helpers (used by query.c find paths) */
int parse_joins_json(const char *json, JoinSpec **out, int *count);
void free_joins(JoinSpec *arr, int n);
int resolve_joins(JoinSpec *joins, int n, const char *db_root,
                  const char *driver_object, FieldSchema *driver_fs);
int extract_local_key(const JoinSpec *j, const uint8_t *driver_raw,
                      const TypedSchema *driver_ts,
                      char *buf, size_t bufsz);
int lookup_remote(const JoinSpec *j, const char *db_root,
                  const char *local_key, size_t local_len,
                  RecordRef *out_rr);
int buf_driver_values(const uint8_t *driver_raw, FieldSchema *driver_fs,
                      const char **driver_proj, int driver_proj_count,
                      char *buf, size_t bufsz);
int buf_join_values(const JoinSpec *j, const uint8_t *remote_raw,
                    char *buf, size_t bufsz);
size_t build_joined_csv_row(const char *key,
                            const uint8_t *driver_raw, FieldSchema *driver_fs,
                            const char **proj_fields, int proj_count,
                            const JoinSpec *joins, int njoins,
                            const uint8_t **jraws,
                            char csv_delim,
                            char *buf, size_t bufsz);
void emit_joined_csv_header(const char *driver_object,
                            FieldSchema *driver_fs,
                            const JoinSpec *joins, int njoins,
                            const char **driver_proj, int driver_proj_count,
                            char delim);
void emit_joined_columns(const char *driver_object,
                         FieldSchema *driver_fs,
                         const JoinSpec *joins, int njoins,
                         const char **driver_proj, int driver_proj_count);
int adv_search_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx);

/* query_plan.c — criteria compilation (used by query.c) */
typedef struct { size_t k; int saturated; int estimable; } CardEst;

void compile_one(CompiledCriterion *cc, const SearchCriterion *c,
                 const TypedSchema *ts);
int validate_field_list(const char *const *names, int n,
                        const TypedSchema *ts, const char *label,
                        char *err, size_t err_sz);
typedef struct {
    unsigned intersect      : 1;
    unsigned composite_seed : 1;
    unsigned composite_exact: 1;
    unsigned order_bound    : 1;
    unsigned trigram_prefers: 1;
    unsigned trigram_starts : 1;
    int      rank;
} OpCaps;
OpCaps op_caps(enum SearchOp op);
CardEst card_est_leaf(const char *db_root, const char *object,
                      int splits, const SearchCriterion *leaf,
                      const TypedField *tf, size_t cap);
size_t selectivity_budget(size_t N);
int collect_and_leaves(CriteriaNode *tree, SearchCriterion **out, int max);
int prefer_fetch_sort(size_t candidates, size_t N, int offset, int limit,
                      int is_bitmap_seed);
int build_exact_composite_key(const FieldSchema *fs, const char *composite_field,
                              SearchCriterion **leaves, int nL,
                              uint8_t *out, size_t *out_len);

/* query.c — order walk bounds (used by query_plan.c test hook) */
typedef struct {
    uint8_t lo[1024]; size_t lo_len; int lo_excl;
    uint8_t hi[1024]; size_t hi_len; int hi_excl;
    int has_lo;
    int has_hi;
} OrderWalkBounds;
void order_walk_bounds(CriteriaNode *tree, FieldSchema *fs,
                       const char *order_by, OrderWalkBounds *b);

#endif
