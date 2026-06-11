#include "types.h"
#include "slotcask.h"
#include "bitmap.h"
#include <semaphore.h>

/* Thread-local pointer to the active ShardDb instance.
   Set by shard_db_open (embedded path) or cmd_server (TCP path). */
__thread ShardDb *g_db = NULL;

/* ── Single-instance guard ── */
static _Atomic int g_instance_open = 0;

/* ── Internal helpers ── */

static void db_mutexes_init(void) {
    /* g_db must be set before calling this. */
    pthread_mutex_init(&g_slow_query_lock,      NULL);
    pthread_mutex_init(&g_dirs_lock,             NULL);
    pthread_mutex_init(&g_schema_lock,           NULL);
    pthread_mutex_init(&g_fields_lock,           NULL);
    pthread_mutex_init(&g_typed_lock,            NULL);
    pthread_mutex_init(&g_idx_lock,              NULL);
    pthread_mutex_init(&g_ucache_table_mutex,    NULL);
    pthread_mutex_init(&g_counts_lock,           NULL);
    pthread_mutex_init(&bt_cache_lock,         NULL);
    pthread_mutex_init(&g_bt_merge_table_lock,   NULL);
    pthread_mutex_init(&g_bm_cache_lock,         NULL);
    pthread_mutex_init(&g_kfcache_lock,          NULL);
    pthread_mutex_init(&g_segcache_lock,         NULL);
    pthread_mutex_init(&g_reg_lock,              NULL);
    pthread_mutex_init(&g_objlock_table_lock,    NULL);
    pthread_mutex_init(&g_ip_lock,               NULL);
    pthread_mutex_init(&g_token_lock,            NULL);
}

static void db_defaults_set(ShardDb *db) {
    db->timeout                   = 30;
    db->port                      = 9199;
    db->global_limit              = 100000;
    db->max_request_size          = 33554432;
    db->fcache_cap                = 4096;
    db->btcache_cap               = 1024;
    db->query_buffer_max_bytes    = 256ULL * 1024 * 1024;
    db->index_build_budget_bytes  = 1024ULL * 1024 * 1024;
    db->token_cap                 = 1024;
    db->slow_query_ms             = 500;
    db->random_seq_ratio          = 8;
    db->vacuum_recommend_pct      = 10;
    db->vacuum_recommend_min_deleted = 1000;
    db->auto_vacuum_interval_sec  = 3600;
    db->log_level                 = 3;
    db->log_retain_days           = 7;
    db->index_page_size           = 4096;
    memcpy(db->warmup_mode, "async", 6);
}

/* shard_db_open_internal: allocate, configure, and initialise all
   instance-scoped caches/pools.  Does NOT start the CPU/IO thread pools
   (those are started by the caller — cmd_server or shard_db_open).
   Returns the initialised instance or NULL on error. */
ShardDb *shard_db_open_internal(const char *db_root) {
    ShardDb *db = calloc(1, sizeof(ShardDb));
    if (!db) return NULL;

    db_defaults_set(db);

    /* Set thread-local early so all g_* macros work during init below. */
    g_db = db;

    db_mutexes_init();

    snprintf(db->db_root, sizeof(db->db_root), "%s", db_root);
    atomic_init(&db->scan_stop, 0);

    /* Load db.env — try cwd first, then db_root parent. */
    {
        FILE *f = fopen("db.env", "r");
        if (f) {
            fclose(f);
            load_db_root(db->db_root, sizeof(db->db_root));
        }
    }

    db->server_start_ms = now_ms();
    bt_page_size = db->index_page_size;

    slot_init();

    /* Auto-tune query_buffer_max_bytes (mirrors cmd_server logic). */
    if (db->query_buffer_max_bytes == 256ULL * 1024 * 1024) {
        long pages = sysconf(_SC_PHYS_PAGES);
        long page_sz = sysconf(_SC_PAGE_SIZE);
        if (pages > 0 && page_sz > 0) {
            size_t total_ram  = (size_t)pages * (size_t)page_sz;
            size_t budget     = total_ram / 4;
            size_t cap        = 4ULL * 1024 * 1024 * 1024;
            if (budget > cap) budget = cap;
            int slots = db->max_concurrent_queries > 0
                            ? db->max_concurrent_queries : 1;
            size_t per_slot = budget / (size_t)slots;
            if (per_slot > db->query_buffer_max_bytes)
                db->query_buffer_max_bytes = per_slot;
        }
    }

    fcache_init(db->fcache_cap);
    bt_cache_init(db->btcache_cap);
    bm_cache_init(db->btcache_cap);
    slotcask_init(db->fcache_cap, db->fcache_cap);

    load_dirs();
    load_tokens_conf(db->db_root);
    load_allowed_ips_conf(db->db_root);
    objlock_init();
    rebuild_recovery(db->db_root);
    grow_recovery(db->db_root);

    return db;
}

/* ── Test-only helper ── */

/* Called by the test runner before each in-process test case.
   Sets up a minimal ShardDb (all caches, no thread pools) if g_db is NULL,
   so unit tests that call slotcask/btree/storage APIs don't crash.
   Daemon-spawn tests ignore this because they operate via TCP. */
void test_init_process_db(void) {
    if (g_db) return;
    char tmpdir[] = "/tmp/shard-db-unit-XXXXXX";
    if (!mkdtemp(tmpdir)) return;
    shard_db_open_internal(tmpdir);  /* sets g_db as a side effect */
    /* Expose the instance so threads spawned by test code can bind their
       own g_db via the g_shard_db_instance fallback in storage functions. */
    if (!g_shard_db_instance) g_shard_db_instance = g_db;
}

/* ── Public API ── */

ShardDb *shard_db_open(const char *db_root) {
    int expected = 0;
    if (!atomic_compare_exchange_strong(&g_instance_open, &expected, 1)) {
        fprintf(stderr, "shard_db_open: only one ShardDb instance allowed per process (V1)\n");
        return NULL;
    }

    ShardDb *db = shard_db_open_internal(db_root);
    if (!db) { atomic_store(&g_instance_open, 0); return NULL; }

    /* Expose instance before starting pools so pool_worker / io_pool_worker
       can bind their thread-local g_db on entry. */
    g_shard_db_instance = db;

    /* Start CPU and I/O thread pools (shared process-global). */
    long nproc = sysconf(_SC_NPROCESSORS_ONLN);
    if (nproc <= 0) nproc = 4;
    int pool_sz = (int)(nproc > 2 ? nproc - 2 : nproc);
    if (pool_sz < 2) pool_sz = 2;
    if (pool_sz > (int)nproc) pool_sz = (int)nproc;
    parallel_pool_init(pool_sz);

    int io_pool_sz = (int)(nproc * 4);
    if (io_pool_sz < (int)nproc) io_pool_sz = (int)nproc;
    if (io_pool_sz < 4) io_pool_sz = 4;
    if (io_pool_sz > (int)nproc * 8) io_pool_sz = (int)nproc * 8;
    parallel_io_pool_init(io_pool_sz);

    return db;
}

int shard_db_query(ShardDb *db, const char *json, char **out, size_t *out_len) {
    if (!db || !json || !out || !out_len) return -1;

    g_db = db;  /* bind thread-local */

    char  *buf = NULL;
    size_t sz  = 0;
    FILE  *mf  = open_memstream(&buf, &sz);
    if (!mf) return -1;

    g_out = mf;
    dispatch_json_query(db->db_root, json, "127.0.0.1");
    fflush(mf);
    fclose(mf);
    g_out = NULL;

    /* Strip the protocol \0\n terminator if present. */
    if (sz >= 2 && buf[sz-2] == '\0' && buf[sz-1] == '\n') sz -= 2;

    *out     = buf;
    *out_len = sz;
    return 0;
}

void shard_db_free_result(char *out) {
    free(out);
}

static void db_mutexes_destroy(void) {
    /* g_db must be set before calling this. */
    pthread_mutex_destroy(&g_slow_query_lock);
    pthread_mutex_destroy(&g_dirs_lock);
    pthread_mutex_destroy(&g_schema_lock);
    pthread_mutex_destroy(&g_fields_lock);
    pthread_mutex_destroy(&g_typed_lock);
    pthread_mutex_destroy(&g_idx_lock);
    pthread_mutex_destroy(&g_ucache_table_mutex);
    pthread_mutex_destroy(&g_counts_lock);
    pthread_mutex_destroy(&bt_cache_lock);
    pthread_mutex_destroy(&g_bt_merge_table_lock);
    pthread_mutex_destroy(&g_bm_cache_lock);
    pthread_mutex_destroy(&g_kfcache_lock);
    pthread_mutex_destroy(&g_segcache_lock);
    pthread_mutex_destroy(&g_reg_lock);
    pthread_mutex_destroy(&g_objlock_table_lock);
    pthread_mutex_destroy(&g_ip_lock);
    pthread_mutex_destroy(&g_token_lock);
}

void shard_db_close(ShardDb *db) {
    if (!db) return;
    g_db = db;

    parallel_pool_shutdown();
    parallel_io_pool_shutdown();
    bt_cache_shutdown();
    bm_cache_shutdown();
    slotcask_shutdown();
    ucache_shutdown();

    /* Free token store heap arrays. */
    free(db->token_set);
    free(db->token_scope);
    free(db->token_scope_obj);
    free(db->token_perm);
    free(db->token_set_used);

    db_mutexes_destroy();

    if (db->slots_inited) sem_destroy(&db->query_slots);

    free(db);
    g_db = NULL;
    atomic_store(&g_instance_open, 0);
}
