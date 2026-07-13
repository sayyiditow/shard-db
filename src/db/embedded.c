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
    pthread_cond_init(&g_reg_cond,               NULL);
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
    db->auto_reshard_hour         = 3;
    db->auto_reshard_throttle_ms  = 0;
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

    /* Load db.env from CWD now that g_db is live so g_*-guarded settings
       (TOKEN_CAP, FCACHE_MAX, PORT, TLS_*, etc.) are applied before the
       caches they size are allocated below.  The DB_ROOT field in db.env
       is discarded: the caller's db_root is always authoritative, so we
       restore it after the call. */
    {
        FILE *f = fopen("db.env", "r");
        if (f) {
            fclose(f);
            char env_root[PATH_MAX];
            load_db_root(env_root, sizeof(env_root));
            snprintf(db->db_root, sizeof(db->db_root), "%s", db_root);
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

/* Forward decl — defined below in the impl section. */
static void db_mutexes_destroy(void);

/* Run startup migrations for every registered object:
 *   1. varlen conversion  — fixed-slot → variable-length segment format.
 *   2. rebuild-kf         — repair kf entries corrupted by a prior buggy
 *                           compact run.  Gated by a sentinel file so it
 *                           only runs once per db_root; idempotent and
 *                           non-fatal on failure (logs warning and skips).
 *
 * Called from shard_db_open before thread pools start — no registry
 * entries are open and slotcask_close is safe.
 * Returns 0 on success, -1 if a varlen migration fails (startup aborts). */
static int run_startup_migration(const char *db_root) {
    char schema_path[PATH_MAX];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", db_root);
    FILE *f = fopen(schema_path, "r");
    if (!f) return 0; /* no schema.conf — nothing to migrate */

    /* Sentinel: rebuild-kf runs once per db_root, then is skipped.
       ./migrate writes the same sentinel after its rebuild-kf phase,
       so daemon users who already ran migrate skip it here too. */
    char kf_sentinel[PATH_MAX];
    snprintf(kf_sentinel, sizeof(kf_sentinel), "%s/.kf_rebuild_done", db_root);
    int need_kf_rebuild = (access(kf_sentinel, F_OK) != 0);

    char line[4096];
    int failed = 0;
    while (!failed && fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n")] = '\0';
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || !*p) continue;

        /* Format: dir:object:splits:max_key:2:streams[...] */
        char *c1 = strchr(p, ':');
        if (!c1) continue;
        *c1 = '\0';
        char *c2 = strchr(c1 + 1, ':');
        if (!c2) continue;
        *c2 = '\0';

        const char *dir = p;
        const char *obj = c1 + 1;

        char obj_data[PATH_MAX];
        snprintf(obj_data, sizeof(obj_data), "%s/%s/%s", db_root, dir, obj);

        /* Skip objects with no materialised data directory. */
        char kf_probe[PATH_MAX];
        snprintf(kf_probe, sizeof(kf_probe), "%s/data/kf", obj_data);
        struct stat kf_st;
        if (stat(kf_probe, &kf_st) != 0) continue;

        char eff_root[PATH_MAX];
        snprintf(eff_root, sizeof(eff_root), "%s/%s", db_root, dir);
        Schema sch = load_schema(eff_root, obj);
        if (sch.splits <= 0) continue;

        SlotcaskDb sdb;
        if (slotcask_open(&sdb, obj_data, sch.splits, sch.streams, sch.slot_size) != 0)
            continue;

        /* Step 1: varlen migration (fatal on failure). */
        if (sdb.format != SLOTCASK_FORMAT_VARIABLE) {
            fprintf(stderr, "[shard-db] migrating %s/%s...\n", dir, obj);
            int mrc = slotcask_migrate_to_varlen(&sdb);
            if (mrc != 0) {
                fprintf(stderr, "[shard-db] migration failed for %s/%s\n", dir, obj);
                slotcask_close(&sdb);
                failed = 1;
                continue;
            }
            fprintf(stderr, "[shard-db] migrated %s/%s\n", dir, obj);
        }

        /* Step 2: rebuild-kf (non-fatal, once per db_root). */
        if (need_kf_rebuild) {
            int repaired = slotcask_rebuild_kf(&sdb);
            if (repaired > 0)
                fprintf(stderr, "[shard-db] rebuild-kf %s/%s: %d entries repaired\n",
                        dir, obj, repaired);
            else if (repaired < 0)
                fprintf(stderr, "[shard-db] rebuild-kf %s/%s: failed (oom) — skipping\n",
                        dir, obj);
        }

        slotcask_close(&sdb);
    }
    fclose(f);

    /* Write sentinel so rebuild-kf is skipped on future startups. */
    if (need_kf_rebuild && !failed) {
        FILE *sf = fopen(kf_sentinel, "w");
        if (sf) fclose(sf);
    }

    return failed ? -1 : 0;
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

    /* Auto-migrate any FIXED-format objects before thread pools start.
       Migration is offline at this point — no registry entries open. */
    if (run_startup_migration(db_root) != 0) {
        fprintf(stderr, "shard_db_open: startup migration failed\n");
        g_shard_db_instance = NULL;
        g_db = NULL;
        /* Thread pools not yet started — call shutdown helpers that
           are safe on uninitialised state, skip parallel_pool_shutdown. */
        bt_cache_shutdown();
        bm_cache_shutdown();
        slotcask_shutdown();
        ucache_shutdown();
        free(db->token_set);
        free(db->token_scope);
        free(db->token_scope_obj);
        free(db->token_perm);
        free(db->token_set_used);
        db_mutexes_destroy();
        if (db->slots_inited) sem_destroy(&db->query_slots);
        free(db);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }

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
    uint64_t _t0 = (g_slow_query_ms > 0) ? now_ms() : 0;
    dispatch_json_query(db->db_root, json, "127.0.0.1");
    if (g_slow_query_ms > 0) {
        uint64_t _dt = now_ms() - _t0;
        if (_dt > (uint64_t)g_slow_query_ms) {
            JsonObj _tmp;
            json_parse_object(json, strlen(json), &_tmp);
            char *_mode = json_obj_strdup(&_tmp, "mode");
            char *_dir  = json_obj_strdup(&_tmp, "dir");
            char *_obj  = json_obj_strdup(&_tmp, "object");
            log_slow_query(_mode ? _mode : "",
                           _dir  ? _dir  : "",
                           _obj  ? _obj  : "",
                           json, (uint32_t)_dt);
            free(_mode); free(_dir); free(_obj);
        }
    }
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
    pthread_cond_destroy(&g_reg_cond);
    pthread_mutex_destroy(&g_objlock_table_lock);
    pthread_mutex_destroy(&g_ip_lock);
    pthread_mutex_destroy(&g_token_lock);
}

void shard_db_set_log_handler(ShardDb *db,
    void (*fn)(int type, const char *msg, void *userdata),
    void *userdata) {
    if (!db) return;
    db->log_handler    = fn;
    db->log_handler_ud = userdata;
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
