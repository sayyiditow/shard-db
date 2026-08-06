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
    _Atomic int used;
    _Atomic int dirty;
    _Atomic uint64_t dirty_since_ms;
    _Atomic uint64_t validated_publish_generation;
    uint64_t last_access;
} BtCacheEntry;

/* bitmap.c */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    _Atomic int used;
    _Atomic int dirty;
    _Atomic uint64_t dirty_since_ms;
    _Atomic uint64_t validated_publish_generation;
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
    _Atomic int used;
    _Atomic int dirty;
    _Atomic uint64_t dirty_since_ms;
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
    _Atomic int used;
    _Atomic int dirty;
    _Atomic uint64_t dirty_since_ms;
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

/* Shared by every fixed-size file-cache table above (BtCacheEntry,
   BmCacheEntry, KfCacheEntry, SegCacheEntry). Default-attribute
   pthread_rwlock_t on glibc/NPTL prefers readers, so a writer blocked in
   pthread_rwlock_wrlock() on one of these per-file locks can be starved under continuous
   read pressure (docs/plans/2026-07-29-cache-rwlock-writer-preference.md).
   PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP is a glibc/NPTL enum value,
   not a preprocessor macro, so it cannot be probed with #ifdef; gate on
   __GLIBC__ instead. Other Linux libcs and macOS have no equivalent portable
   attribute — the #else branch keeps today's behavior there unchanged.

   NONRECURSIVE requires that no thread ever holds a read lock on one of
   these and then takes a second read lock on the *same* path/slot from the
   same thread — a recursive reader can self-deadlock behind a queued writer
   under this policy (this is why objlock.c, whose API deliberately permits
   recursive readers, is not switched to this helper). Checked against every
   acquire/release call site in btree.c, slotcask.c (kfcache + segcache), and
   bitmap.c: no such recursive acquisition exists today. In particular,
   btree_idx_walk_ordered's k-way cursor merge opens one BtRangeIter per
   shard, always on a distinct path, never the same file twice in one
   thread; every eviction path uses non-blocking trywrlock against LRU
   candidates so a thread can't be blocked trying to evict a slot it already
   holds open. This is a live invariant, not a one-time fact — any future
   code path that acquires the same cached file twice on one thread without
   releasing in between would reintroduce the self-deadlock risk this
   comment rules out today. */
static inline void rwlock_init_writer_preferring_fallback(pthread_rwlock_t *lock) {
    int rc = pthread_rwlock_init(lock, NULL);
    if (rc != 0)
        LOG_ERROR(LOG_SUB_SERVER, "rwlock_init (default fallback) failed: %s", strerror(rc));
}

static inline void rwlock_init_writer_preferring(pthread_rwlock_t *lock) {
#ifdef __GLIBC__
    pthread_rwlockattr_t attr;
    int rc;
    if ((rc = pthread_rwlockattr_init(&attr)) != 0) {
        LOG_ERROR(LOG_SUB_SERVER, "rwlockattr_init failed: %s", strerror(rc));
        rwlock_init_writer_preferring_fallback(lock);
        return;
    }
    if ((rc = pthread_rwlockattr_setkind_np(&attr,
            PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)) != 0) {
        LOG_ERROR(LOG_SUB_SERVER, "rwlockattr_setkind_np failed: %s", strerror(rc));
        pthread_rwlockattr_destroy(&attr);
        rwlock_init_writer_preferring_fallback(lock);
        return;
    }
    if ((rc = pthread_rwlock_init(lock, &attr)) != 0) {
        LOG_ERROR(LOG_SUB_SERVER, "rwlock_init (writer-preferring) failed: %s", strerror(rc));
        pthread_rwlockattr_destroy(&attr);
        rwlock_init_writer_preferring_fallback(lock);
        return;
    }
    if ((rc = pthread_rwlockattr_destroy(&attr)) != 0)
        LOG_ERROR(LOG_SUB_SERVER, "rwlockattr_destroy failed: %s", strerror(rc));
#else
    rwlock_init_writer_preferring_fallback(lock);
#endif
}

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

typedef struct BtMutationLockEntry {
    struct BtMutationLockEntry *next;
    char                       *path;
    pthread_mutex_t             mutex;
} BtMutationLockEntry;

/* ── ShardDb: one instance per open data directory ── */

struct ShardDb {
    /* config scalars */
    char     db_root[PATH_MAX];
    int      db_root_lock_fd;
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
    uint64_t bt_cache_hits;
    uint64_t bt_cache_misses;
    uint64_t server_start_ms;
    uint64_t slow_query_count;
    /* Durability commit-window instrumentation. commit_count
       and commit_lock_hold_us_total cover single-record upsert/insert
       commits and each bulk commit window; commit_sync_us_total covers
       time spent specifically inside the marker fsync / kf-slot-sync
       primitives (kf_marker_write/clear, kf_batch_marker_write/clear,
       kfcache_sync_slots_locked) — a subset of the lock-hold total, kept
       separate so the marker-fsync cost can be reported on its own. */
    uint64_t commit_count;
    uint64_t commit_lock_hold_us_total;
    uint64_t commit_sync_us_total;

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
    int durability_sync_ms;
    int warmup_explicit;          /* a valid WARMUP= was present in db.env */
    int kfcache_test_hold_ms; /* test-only; 0 = off in production */
    int warmup_test_delay_ms; /* test-only; 0 = off in production */
    int warmup_test_prelock_delay_ms; /* test-only; 0 = off in production */
    int schema_wrlock_test_delay_ms; /* test-only; 0 = off in production */
    char rebuild_test_pause_phase[32]; /* test-only; empty = disabled */
    int rebuild_test_pause_ms;         /* test-only; 0 = disabled */
    char durability_test_pause_phase[32]; /* test-only; empty = disabled */
    int durability_test_pause_ms;         /* test-only; 0 = disabled */
    char warmup_mode[16];
    int log_level;
    int log_retain_days;

    /* Joinable background-thread lifecycle. */
    pthread_t bg_auto_vac_tid;
    int       bg_auto_vac_spawned;
    pthread_t bg_auto_reshard_tid;
    int       bg_auto_reshard_spawned;
    pthread_t bg_warmup_tid;
    int       bg_warmup_spawned;
    pthread_t bg_durability_tid;
    int       bg_durability_spawned;

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

    /* counts cache (storage.c) */
    CountsCacheEntry counts_cache[COUNTS_CACHE_BUCKETS];
    pthread_mutex_t  counts_lock;

    /* btree cache */
    BtCacheEntry        *bt_cache;
    int                  bt_cache_slots;
    int                  bt_cache_count;
    pthread_mutex_t      bt_cache_lock;
    volatile uint64_t    bt_cache_clock;
    BtMutationLockEntry **bt_mutation_lock_buckets;
    size_t                bt_mutation_lock_bucket_count;
    size_t                bt_mutation_lock_count;
    pthread_mutex_t       bt_mutation_lock_table_lock;

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
extern _Atomic int server_running;
extern ShardDb *g_shard_db_instance;

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
#define g_bt_cache_hits             (g_db->bt_cache_hits)
#define g_bt_cache_misses           (g_db->bt_cache_misses)
#define g_server_start_ms           (g_db->server_start_ms)
#define g_slow_query_count          (g_db->slow_query_count)
#define g_slow_query_ms             (g_db->slow_query_ms)
#define g_commit_count              (g_db->commit_count)
#define g_commit_lock_hold_us_total (g_db->commit_lock_hold_us_total)
#define g_commit_sync_us_total      (g_db->commit_sync_us_total)
#define g_random_seq_ratio          (g_db->random_seq_ratio)
#define g_vacuum_recommend_pct      (g_db->vacuum_recommend_pct)
#define g_vacuum_recommend_min_deleted (g_db->vacuum_recommend_min_deleted)
#define g_auto_vacuum_enable        (g_db->auto_vacuum_enable)
#define g_auto_vacuum_interval_sec  (g_db->auto_vacuum_interval_sec)
#define g_auto_reshard_enable       (g_db->auto_reshard_enable)
#define g_auto_reshard_hour         (g_db->auto_reshard_hour)
#define g_auto_reshard_throttle_ms  (g_db->auto_reshard_throttle_ms)
#define g_durability_sync_ms        (g_db->durability_sync_ms)
#define g_kfcache_test_hold_ms      (g_db->kfcache_test_hold_ms)
#define g_warmup_test_delay_ms      (g_db->warmup_test_delay_ms)
#define g_warmup_test_prelock_delay_ms (g_db->warmup_test_prelock_delay_ms)
#define g_schema_wrlock_test_delay_ms (g_db->schema_wrlock_test_delay_ms)
#define g_rebuild_test_pause_phase  (g_db->rebuild_test_pause_phase)
#define g_rebuild_test_pause_ms     (g_db->rebuild_test_pause_ms)
#define g_durability_test_pause_phase (g_db->durability_test_pause_phase)
#define g_durability_test_pause_ms    (g_db->durability_test_pause_ms)
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

#define g_counts_cache              (g_db->counts_cache)
#define g_counts_lock               (g_db->counts_lock)

/* btree.c — note: bt_cache is a static local name in btree.c, aliased here */
#define bt_cache                    (g_db->bt_cache)
#define bt_cache_slots              (g_db->bt_cache_slots)
#define bt_cache_count              (g_db->bt_cache_count)
#define bt_cache_lock               (g_db->bt_cache_lock)
#define bt_cache_clock              (g_db->bt_cache_clock)
#define g_bt_mutation_lock_buckets      (g_db->bt_mutation_lock_buckets)
#define g_bt_mutation_lock_bucket_count (g_db->bt_mutation_lock_bucket_count)
#define g_bt_mutation_lock_count        (g_db->bt_mutation_lock_count)
#define g_bt_mutation_lock_table_lock   (g_db->bt_mutation_lock_table_lock)

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
void shard_db_destroy_after_storage(ShardDb *db);

/* Server-wide instance set by cmd_server before any threads spawn.
   Pool workers and the log thread bind their thread-local g_db to this. */
extern ShardDb *g_shard_db_instance;

/* ── Durable abort evidence (indexed-write atomicity) ── */

/* Fixed on-disk abort sidecar header (24 B, no trailing bytes). There is one
   sidecar per commit-intent marker: kind distinguishes the single-record
   %03x_marker_abort.dat from the batch %03x_batch_%u_abort.dat producer.
   A sidecar is the durable, ordered decision that its paired marker must be
   ABORTED (index inverse diff applied, speculative segment tombstoned where
   one exists) rather than forward-replayed. */
enum { KF_ABORT_MAGIC = 0x4b464142u, KF_ABORT_VERSION = 1 };
enum { KF_ABORT_SINGLE = 1, KF_ABORT_BATCH = 2 };

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t kind;
    uint32_t kf_shard;
    uint32_t batch_id;       /* 0 for KF_ABORT_SINGLE */
    uint32_t marker_count;   /* exactly 1 for KF_ABORT_SINGLE */
    uint32_t checksum;       /* XXH32 through marker_count */
} KfAbortHeader;

_Static_assert(sizeof(KfAbortHeader) == 24,
               "abort sidecar header is a fixed on-disk format");

/* ── Marker file (durability write-ordering) ── */

#define KF_MARKER_MAGIC 0x4B464D31u /* "KFM1" */

enum KfMarkerOp {
    KF_MARKER_OP_LEGACY_UPSERT = 0, /* v1 marker writer; recover as UPSERT */
    KF_MARKER_OP_UPSERT = 1,
    KF_MARKER_OP_DELETE = 2,
};

typedef struct {
    uint32_t magic;        /* KF_MARKER_MAGIC */
    uint32_t kf_slot;      /* UPDATE/DELETE: existing slot; INSERT: UINT32_MAX */
    uint32_t old_offset;
    uint32_t new_offset;
    uint16_t old_file_id;
    uint16_t new_file_id;
    uint8_t  old_stream_id;
    uint8_t  new_stream_id;
    uint8_t  has_old;      /* UPSERT: 0=insert, 1=update; DELETE: always 1 */
    uint8_t  op;           /* enum KfMarkerOp */
    uint8_t  reserved[4];  /* must be zero */
    uint32_t checksum;     /* XXH32 over [0, offsetof(checksum)) */
} KfMarkerSlot;

_Static_assert(sizeof(KfMarkerSlot) == 32,
               "KfMarkerSlot must stay 32 bytes");

static inline int kf_marker_op_valid(const KfMarkerSlot *marker) {
    if (!marker) return 0;
    if (marker->op != KF_MARKER_OP_LEGACY_UPSERT &&
        marker->op != KF_MARKER_OP_UPSERT &&
        marker->op != KF_MARKER_OP_DELETE)
        return 0;
    if (marker->reserved[0] || marker->reserved[1] ||
        marker->reserved[2] || marker->reserved[3])
        return 0;
    if (marker->op == KF_MARKER_OP_DELETE)
        return marker->has_old == 1 && marker->kf_slot != UINT32_MAX &&
               marker->new_offset == 0 && marker->new_file_id == 0 &&
               marker->new_stream_id == 0;
    return marker->has_old <= 1;
}

/* Marker I/O — non-static for test access (TEST_BUILD). */
int kf_marker_write(const char *data_dir, int kf_shard,
                    const KfMarkerSlot *slot);
int kf_marker_clear(const char *data_dir, int kf_shard);
int kf_marker_read(const char *data_dir, int kf_shard, KfMarkerSlot *out);

/* Abort-sidecar I/O — non-static for test access (TEST_BUILD). */
int kf_abort_write_sidecar(const char *data_dir, uint16_t kind, int kf_shard,
                           uint32_t batch_id, uint32_t marker_count);
int kf_abort_read_exact(const char *path, uint16_t want_kind,
                        uint32_t want_shard, uint32_t want_batch,
                        uint32_t want_count, KfAbortHeader *out);
int kf_abort_clear_after_marker(const char *abort_path, const char *kf_dir);

/* Marker recovery — replays a marker's intent when kf writer lock is held.
   eff_root/object identify the object for index-diff reconciliation
   (steps 4-5); eff_root is the tenant dir ($DB_ROOT/<dir>), matching the
   db_root convention used throughout config.c/storage.c.
   Returns 0 on success (marker cleared), -1 on failure (marker retained).
   Opaque kh pointer from slotcask.h (avoid cross-header typedef). */
int kf_marker_replay_locked(const char *eff_root, const char *object,
                            const char *data_dir, int kf_shard,
                            void *kh, const KfMarkerSlot *marker);

/* Recovery-time index reconciliation callback.
   slotcask.c is deliberately decoupled from schema/index logic (that lives
   in storage.c), so kf_marker_replay_locked reaches it through this
   process-wide callback rather than a direct call. Registered once by
   storage.c via an __attribute__((constructor)) initializer, so it is set
   before any recovery sweep can run. NULL (unregistered) is a silent
   no-op, matching pre_commit's "not set = no-op" convention — only
   relevant to tests that exercise the kf layer in isolation.
   old_value is NULL for a pure insert (marker->has_old == 0). */
typedef int (*RecoveryIndexDiffFn)(const char *eff_root, const char *object,
                                   int kf_shard, uint32_t kf_slot,
                                   const uint8_t *hash,
                                   const uint8_t *old_value, size_t old_vlen,
                                   const uint8_t *new_value, size_t new_vlen,
                                   char *err_buf, size_t err_buf_len);
extern RecoveryIndexDiffFn g_recovery_index_diff_fn;

/* Clean shutdown flag management. */
int clean_flag_write(const char *data_dir);
int clean_flag_exists(const char *data_dir);
int clean_flag_remove(const char *data_dir);

/* Bulk marker I/O. */
int kf_batch_marker_write(const char *data_dir, int kf_shard, uint32_t batch_id,
                          const KfMarkerSlot *markers, size_t count);
int kf_batch_marker_clear(const char *data_dir, int kf_shard, uint32_t batch_id);

/* Startup recovery sweep. Caller must hold objlock_wrlock(data_dir's
   eff_root, object_name) for the duration. Returns 0 if all markers found
   were replayed/cleared, -1 if any marker is corrupt or fails to replay.
   If out_replayed is non-NULL, it is incremented once per marker file found
   (regardless of replay outcome), letting the caller distinguish "the sweep
   ran" from "the sweep actually replayed something". */
int marker_recovery_sweep_object(const char *eff_root, const char *data_dir, const char *object_name,
                                  int *out_replayed);

/* Returns 1 if this object's data/kf/ still has a single-record or batch
   marker file, 0 if none, -1 on a directory I/O error. Non-replaying (no
   lock required) — used by graceful shutdown to decide whether writing
   .shard-db.clean is safe, without walking every marker's contents. */
int object_has_pending_markers(const char *data_dir);

/* Test-visible counter: set to 1 by cmd_server when the startup marker
   recovery sweep actually ran (unclean shutdown detected), left at 0 on a
   clean-shutdown startup. Declared here so TEST_BUILD cases can assert on
   it without racing the .shard-db.clean file's deletion. */
extern int g_marker_recovery_ran;

#endif /* SHARD_DB_INTERNAL_H */
