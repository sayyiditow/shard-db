#ifndef SHARD_DB_INTERNAL_H
#define SHARD_DB_INTERNAL_H

/* Included from the end of types.h — all types.h declarations are visible. */

/* ── Private struct definitions (moved from their .c files) ── */

/* btree.c */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
} BtCacheEntry;

/* bitmap.c */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
} BmCacheEntry;

/* slotcask.c — kfcache */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *base;
    size_t   map_size;
    size_t   capacity;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
    _Atomic uint64_t gen;   /* incremented on eviction; SlotRef validation */
    dev_t    file_dev;      /* identity of the file open at install time — */
    ino_t    file_ino;      /* detects rename-away-and-recreate at `path` */
} KfCacheEntry;

/* slotcask.c — segcache */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
    _Atomic uint64_t gen;   /* incremented on eviction; SlotRef validation */
    dev_t    file_dev;      /* identity of the file open at install time — */
    ino_t    file_ino;      /* detects rename-away-and-recreate at `path` */
} SegCacheEntry;

#define SLOTCASK_REG_BUCKETS 1024
typedef struct {
    char        key[PATH_MAX];
    struct SlotcaskDb *db;
    int         used;
    int         opening;   /* 1 while some thread's slotcask_open() is in
                               flight for this key; see slotcask_registry_get. */
} RegEntry;

/* objlock.c */
#define OBJLOCK_BUCKETS 256
typedef struct {
    char name[512];
    pthread_rwlock_t rwlock;
    _Atomic int used;
} ObjLockEntry;

/* storage.c */
#define COUNTS_CACHE_BUCKETS 1024
typedef struct {
    char             path[PATH_MAX];
    _Atomic int64_t  live;
    _Atomic int64_t  deleted;
    _Atomic uint64_t pending_writes;
    _Atomic int      used;
} CountsCacheEntry;

/* config.c */
#define SCHEMA_BUCKETS 256
struct SchemaCache { char name[512]; Schema schema; int used; };

#define FIELDS_BUCKETS 256
struct FieldsCache {
    char name[256];
    char fields[MAX_FIELDS][256];
    int nfields;
    int used;
};

#define TYPED_BUCKETS 256
struct TypedCacheEntry {
    char name[512];
    TypedSchema schema;
    int used;
};

#define IDX_BUCKETS 256
struct IdxCache {
    char name[512];
    char fields[MAX_FIELDS][256];
    enum IndexType types[MAX_FIELDS];
    int nfields;
    int used;
};

/* server.c */
#define IP_SET_BUCKETS 128

/* ── ShardDb: one instance per open data directory ── */

struct ShardDb {
    /* config scalars */
    char     db_root[PATH_MAX];
    uint32_t timeout;
    int      port;
    int      workers;
    int      io_threads;
    int      index_page_size;
    int      global_limit;
    int      max_request_size;
    int      max_concurrent_queries;
    int      fcache_cap;
    int      btcache_cap;
    size_t   query_buffer_max_bytes;
    size_t   index_build_budget_bytes;
    int      disable_localhost_trust;
    int      token_cap;

    /* TLS config */
    int  tls_enable;
    int  tls_skip_verify;
    char tls_cert[PATH_MAX];
    char tls_key[PATH_MAX];
    char tls_ca[PATH_MAX];

    /* stats counters */
    uint64_t ucache_hits;
    uint64_t ucache_misses;
    uint64_t bt_cache_hits;
    uint64_t bt_cache_misses;
    uint64_t server_start_ms;
    uint64_t slow_query_count;

    /* config / tuning */
    int slow_query_ms;
    int random_seq_ratio;
    int vacuum_recommend_pct;
    int vacuum_recommend_min_deleted;
    int auto_vacuum_enable;
    int auto_vacuum_interval_sec;
    int auto_reshard_enable;
    int auto_reshard_hour;
    int auto_reshard_throttle_ms;
    int kfcache_test_hold_ms; /* test-only; 0 = off in production */
    int warmup_test_delay_ms; /* test-only; 0 = off in production */
    char warmup_mode[16];
    int log_level;
    int log_retain_days;
    /* embedded-mode log handler (set via shard_db_set_log_handler).
       Called synchronously on the same thread as log emission when
       g_log_running == 0 (no drain thread).  Must be thread-safe.
       type: 1=ERROR 2=WARN 3=INFO 4=DEBUG 5=AUDIT 6=SLOW */
    void (*log_handler)(int type, const char *msg, void *ud);
    void *log_handler_ud;

    /* slow query ring */
    SlowQueryEntry slow_queries[SLOW_QUERY_RING];
    int            slow_query_head;
    pthread_mutex_t slow_query_lock;

    /* dirs table */
    char dirs[DIRS_BUCKETS][256];
    int  dirs_used[DIRS_BUCKETS];
    int  dirs_count;
    pthread_mutex_t dirs_lock;

    /* schema cache */
    struct SchemaCache  schema_cache[SCHEMA_BUCKETS];
    pthread_mutex_t     schema_lock;

    /* fields cache */
    struct FieldsCache  fields_cache[FIELDS_BUCKETS];
    pthread_mutex_t     fields_lock;

    /* typed schema cache */
    struct TypedCacheEntry typed_cache[TYPED_BUCKETS];
    pthread_mutex_t        typed_lock;

    /* index cache */
    struct IdxCache idx_cache[IDX_BUCKETS];
    pthread_mutex_t idx_lock;

    /* concurrency semaphore */
    sem_t query_slots;
    int   slots_inited;

    /* ucache (storage.c) */
    UCacheEntry         *ucache;
    int                  ucache_slots;
    int                  ucache_count;
    pthread_mutex_t      ucache_table_mutex;
    volatile uint64_t    ucache_clock;

    /* counts cache (storage.c) */
    CountsCacheEntry counts_cache[COUNTS_CACHE_BUCKETS];
    pthread_mutex_t  counts_lock;

    /* btree cache */
    BtCacheEntry        *bt_cache;
    int                  bt_cache_slots;
    int                  bt_cache_count;
    pthread_mutex_t      bt_cache_lock;
    volatile uint64_t    bt_cache_clock;
    pthread_mutex_t      bt_merge_table_lock;

    /* bitmap cache */
    BmCacheEntry        *bm_cache;
    int                  bm_cache_slots;
    int                  bm_cache_count;
    pthread_mutex_t      bm_cache_lock;
    volatile uint64_t    bm_cache_clock;

    /* slotcask kfcache */
    KfCacheEntry        *kfcache;
    int                  kfcache_slots;
    int                  kfcache_count;
    pthread_mutex_t      kfcache_lock;
    volatile uint64_t    kfcache_clock;

    /* slotcask segcache */
    SegCacheEntry       *segcache;
    int                  segcache_slots;
    int                  segcache_count;
    pthread_mutex_t      segcache_lock;
    volatile uint64_t    segcache_clock;

    /* slotcask object registry */
    RegEntry        reg[SLOTCASK_REG_BUCKETS];
    pthread_mutex_t reg_lock;
    pthread_cond_t  reg_cond;   /* broadcast whenever any slot's `opening`
                                   flag clears (success or failure) */

    /* object lock table */
    ObjLockEntry    objlocks[OBJLOCK_BUCKETS];
    pthread_mutex_t objlock_table_lock;

    /* IP set (server.c) */
    char            ip_set[IP_SET_BUCKETS][46];
    int             ip_set_used[IP_SET_BUCKETS];
    int             ip_set_count;
    pthread_mutex_t ip_lock;

    /* token store (server.c — heap-allocated arrays) */
    char    (*token_set)[256];
    char    (*token_scope)[256];
    char    (*token_scope_obj)[256];
    uint8_t *token_perm;
    int     *token_set_used;
    int      token_count;
    pthread_mutex_t token_lock;

    /* query.c */
    _Atomic int scan_stop;
};

typedef struct ShardDb ShardDb;

/* Thread-local pointer to the active instance.
   Set by shard_db_open (main thread init) and by every worker thread
   before calling dispatch_json_query. */
extern __thread ShardDb *g_db;

/* ── Macro aliases — defined AFTER the struct so struct field
      declarations above are not expanded. ── */

/* config.c */
#define g_db_root                   (g_db->db_root)
#define g_timeout                   (g_db->timeout)
#define g_port                      (g_db->port)
#define g_workers                   (g_db->workers)
#define g_io_threads                (g_db->io_threads)
#define g_index_page_size           (g_db->index_page_size)
#define g_global_limit              (g_db->global_limit)
#define g_max_request_size          (g_db->max_request_size)
#define g_max_concurrent_queries    (g_db->max_concurrent_queries)
#define g_fcache_cap                (g_db->fcache_cap)
#define g_btcache_cap               (g_db->btcache_cap)
#define g_query_buffer_max_bytes    (g_db->query_buffer_max_bytes)
#define g_index_build_budget_bytes  (g_db->index_build_budget_bytes)
#define g_disable_localhost_trust   (g_db->disable_localhost_trust)
#define g_token_cap                 (g_db->token_cap)
#define g_tls_enable                (g_db->tls_enable)
#define g_tls_skip_verify           (g_db->tls_skip_verify)
#define g_tls_cert                  (g_db->tls_cert)
#define g_tls_key                   (g_db->tls_key)
#define g_tls_ca                    (g_db->tls_ca)
#define g_ucache_hits               (g_db->ucache_hits)
#define g_ucache_misses             (g_db->ucache_misses)
#define g_bt_cache_hits             (g_db->bt_cache_hits)
#define g_bt_cache_misses           (g_db->bt_cache_misses)
#define g_server_start_ms           (g_db->server_start_ms)
#define g_slow_query_count          (g_db->slow_query_count)
#define g_slow_query_ms             (g_db->slow_query_ms)
#define g_random_seq_ratio          (g_db->random_seq_ratio)
#define g_vacuum_recommend_pct      (g_db->vacuum_recommend_pct)
#define g_vacuum_recommend_min_deleted (g_db->vacuum_recommend_min_deleted)
#define g_auto_vacuum_enable        (g_db->auto_vacuum_enable)
#define g_auto_vacuum_interval_sec  (g_db->auto_vacuum_interval_sec)
#define g_auto_reshard_enable       (g_db->auto_reshard_enable)
#define g_auto_reshard_hour         (g_db->auto_reshard_hour)
#define g_auto_reshard_throttle_ms  (g_db->auto_reshard_throttle_ms)
#define g_kfcache_test_hold_ms      (g_db->kfcache_test_hold_ms)
#define g_warmup_test_delay_ms      (g_db->warmup_test_delay_ms)
#define g_warmup_mode               (g_db->warmup_mode)
#define g_log_level                 (g_db->log_level)
#define g_log_retain_days           (g_db->log_retain_days)
#define g_slow_queries              (g_db->slow_queries)
#define g_slow_query_head           (g_db->slow_query_head)
#define g_slow_query_lock           (g_db->slow_query_lock)
#define g_dirs                      (g_db->dirs)
#define g_dirs_used                 (g_db->dirs_used)
#define g_dirs_count                (g_db->dirs_count)
#define g_dirs_lock                 (g_db->dirs_lock)
#define g_schema_cache              (g_db->schema_cache)
#define g_schema_lock               (g_db->schema_lock)
#define g_fields_cache              (g_db->fields_cache)
#define g_fields_lock               (g_db->fields_lock)
#define g_typed_cache               (g_db->typed_cache)
#define g_typed_lock                (g_db->typed_lock)
#define g_idx_cache                 (g_db->idx_cache)
#define g_idx_lock                  (g_db->idx_lock)
#define g_query_slots               (g_db->query_slots)
#define g_slots_inited              (g_db->slots_inited)

/* storage.c */
#define g_ucache                    (g_db->ucache)
#define g_ucache_slots              (g_db->ucache_slots)
#define g_ucache_count              (g_db->ucache_count)
#define g_ucache_table_mutex        (g_db->ucache_table_mutex)
#define g_ucache_clock              (g_db->ucache_clock)
#define g_counts_cache              (g_db->counts_cache)
#define g_counts_lock               (g_db->counts_lock)

/* btree.c — note: bt_cache is a static local name in btree.c, aliased here */
#define bt_cache                    (g_db->bt_cache)
#define bt_cache_slots              (g_db->bt_cache_slots)
#define bt_cache_count              (g_db->bt_cache_count)
#define bt_cache_lock               (g_db->bt_cache_lock)
#define bt_cache_clock              (g_db->bt_cache_clock)
#define g_bt_merge_table_lock       (g_db->bt_merge_table_lock)

/* bitmap.c */
#define g_bm_cache                  (g_db->bm_cache)
#define g_bm_cache_slots            (g_db->bm_cache_slots)
#define g_bm_cache_count            (g_db->bm_cache_count)
#define g_bm_cache_lock             (g_db->bm_cache_lock)
#define g_bm_cache_clock            (g_db->bm_cache_clock)

/* slotcask.c */
#define g_kfcache                   (g_db->kfcache)
#define g_kfcache_slots             (g_db->kfcache_slots)
#define g_kfcache_count             (g_db->kfcache_count)
#define g_kfcache_lock              (g_db->kfcache_lock)
#define g_kfcache_clock             (g_db->kfcache_clock)
#define g_segcache                  (g_db->segcache)
#define g_segcache_slots            (g_db->segcache_slots)
#define g_segcache_count            (g_db->segcache_count)
#define g_segcache_lock             (g_db->segcache_lock)
#define g_segcache_clock            (g_db->segcache_clock)
#define g_reg                       (g_db->reg)
#define g_reg_lock                  (g_db->reg_lock)
#define g_reg_cond                  (g_db->reg_cond)

/* objlock.c */
#define g_objlocks                  (g_db->objlocks)
#define g_objlock_table_lock        (g_db->objlock_table_lock)

/* server.c */
#define g_ip_set                    (g_db->ip_set)
#define g_ip_set_used               (g_db->ip_set_used)
#define g_ip_set_count              (g_db->ip_set_count)
#define g_ip_lock                   (g_db->ip_lock)
#define g_token_set                 (g_db->token_set)
#define g_token_scope               (g_db->token_scope)
#define g_token_scope_obj           (g_db->token_scope_obj)
#define g_token_perm                (g_db->token_perm)
#define g_token_set_used            (g_db->token_set_used)
#define g_token_count               (g_db->token_count)
#define g_token_lock                (g_db->token_lock)

/* query.c */
#define g_scan_stop                 (g_db->scan_stop)

/* Internal open — used by cmd_server and shard_db_open. */
ShardDb *shard_db_open_internal(const char *db_root);

/* Server-wide instance set by cmd_server before any threads spawn.
   Pool workers and the log thread bind their thread-local g_db to this. */
extern ShardDb *g_shard_db_instance;

#endif /* SHARD_DB_INTERNAL_H */
