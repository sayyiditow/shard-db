#include "types.h"
#include "slotcask.h"
#include "bitmap.h"
#include <semaphore.h>
#include <dirent.h>

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
    pthread_mutex_init(&g_counts_lock,           NULL);
    pthread_mutex_init(&bt_cache_lock,         NULL);
    pthread_mutex_init(&g_bt_mutation_lock_table_lock, NULL);
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
    db->db_root_lock_fd            = -1;
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
    db->durability_sync_ms        = 1000;
    db->warmup_explicit           = 0;
    db->log_level                 = 3;
    db->log_retain_days           = 7;
    db->index_page_size           = 4096;
    db->rebuild_test_pause_phase[0] = '\0';
    db->rebuild_test_pause_ms       = 0;
    db->durability_test_pause_phase[0] = '\0';
    db->durability_test_pause_ms       = 0;
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

    if (index_rebuild_temp_sweep(db_root) != 0) {
        fprintf(stderr, "shard-db: index temporary cleanup failed for DB_ROOT=%s: %s\n",
                db_root, strerror(errno));
        free(db);
        g_db = NULL;
        return NULL;
    }

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

    bt_cache_init(db->btcache_cap);
    bm_cache_init(db->btcache_cap);
    slotcask_init(db->fcache_cap, db->fcache_cap);

    load_dirs();
    load_tokens_conf(db->db_root);
    load_allowed_ips_conf(db->db_root);
    objlock_init();

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
#ifdef TEST_BUILD
    /* Tests exercise concurrency the auto slot cap can't hold: db.env
       declares MAX_CONCURRENT_QUERIES=0 (auto = max(4, min(nproc,32))),
       which is only 4 on the 4-vCPU CI runners. In-process tests fire up
       to 6 concurrent embedded requests (e.g. test-online-bulk-reindex-
       readers: 4 readers + reindex + bulk) and the overflow gets
       {"error":"server at capacity"} — a CI-only failure invisible on
       beefier local boxes. Pin 32 like the daemon-test fixture
       (fixtures.c). TEST_BUILD-only: production/embedded users keep
       slot_init()'s resolved count untouched. */
    if (g_db->slots_inited) {
        sem_destroy(&g_db->query_slots);
        g_db->slots_inited = 0;
    }
    g_db->max_concurrent_queries = 32;
    slot_init();
#endif
    /* Expose the instance so threads spawned by test code can bind their
       own g_db via the g_shard_db_instance fallback in storage functions.
       Guarded: under parallel run-all, multiple worker threads call this
       function concurrently (each with its own thread-local g_db) — an
       unguarded check-and-set on the process-global g_shard_db_instance
       was a genuine data race. */
    static pthread_mutex_t instance_lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&instance_lock);
    if (!g_shard_db_instance) g_shard_db_instance = g_db;
    pthread_mutex_unlock(&instance_lock);
}

/* Opening another instance would replace thread-local g_db while leaving the
   original instance's descriptors, mappings, and caches allocated. */
#ifdef TEST_BUILD
ShardDb *test_get_process_db(void) {
    return g_db;
}

const char *test_get_process_db_root(void) {
    return g_db ? g_db->db_root : NULL;
}
#endif

/* Forward decl — defined below in the impl section. */
static void db_mutexes_destroy(void);

static void db_cleanup_before_pools(ShardDb *db) {
    if (!db) return;
    g_db = db;
    schema_caches_shutdown();
    bt_cache_shutdown();
    bm_cache_shutdown();
    slotcask_shutdown();
    free(db->token_set);
    free(db->token_scope);
    free(db->token_scope_obj);
    free(db->token_perm);
    free(db->token_set_used);
    db_root_lock_release(&db->db_root_lock_fd);
    db_mutexes_destroy();
    if (db->slots_inited) sem_destroy(&db->query_slots);
    free(db);
    g_db = NULL;
}

/* $DB_ROOT/.version — durable record of which shard-db release last
   completed startup migration against this db_root. Returns
   SHARD_DB_VERSION_FILE_OK and fills out on success,
   SHARD_DB_VERSION_FILE_MISSING when the path does not exist, or
   SHARD_DB_VERSION_FILE_ERROR for an unreadable, oversized, or empty file.
   Missing and unreadable are intentionally distinct: a non-empty database
   must not silently mutate when its version evidence cannot be read. */
int shard_db_version_file_read(const char *db_root, char *out, size_t out_sz) {
    if (!db_root || !out || out_sz < 2) return SHARD_DB_VERSION_FILE_ERROR;
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/.version", db_root);
    FILE *f = fopen(path, "r");
    if (!f) return errno == ENOENT ? SHARD_DB_VERSION_FILE_MISSING
                                   : SHARD_DB_VERSION_FILE_ERROR;
    size_t n = 0;
    int ch;
    while ((ch = fgetc(f)) != EOF) {
        if (n + 1 >= out_sz) {
            fclose(f);
            return SHARD_DB_VERSION_FILE_ERROR;
        }
        out[n++] = (char)ch;
    }
    int read_error = ferror(f);
    int close_error = (fclose(f) != 0);
    if (read_error || close_error || n == 0)
        return SHARD_DB_VERSION_FILE_ERROR;
    if (out[n - 1] == '\n') n--;
    if (n == 0 || memchr(out, '\n', n) || memchr(out, '\r', n))
        return SHARD_DB_VERSION_FILE_ERROR;
    out[n] = '\0';
    return SHARD_DB_VERSION_FILE_OK;
}

int shard_db_version_file_write(const char *db_root, const char *version) {
    if (!db_root || !version || !version[0]) return -1;
    char path[PATH_MAX];
    char tmp[PATH_MAX];
    snprintf(path, sizeof(path), "%s/.version", db_root);
    snprintf(tmp, sizeof(tmp), "%s/.version.tmp.XXXXXX", db_root);
    int fd = mkstemp(tmp);
    if (fd < 0) return -1;
    size_t len = strlen(version);
    const char *p = version;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) { close(fd); unlink(tmp); return -1; }
        p += n; len -= (size_t)n;
    }
    if (write(fd, "\n", 1) != 1 || fsync(fd) != 0) {
        close(fd); unlink(tmp); return -1;
    }
    if (close(fd) != 0 || rename(tmp, path) != 0) {
        unlink(tmp);
        return -1;
    }
    int dfd = open(db_root, O_RDONLY | O_DIRECTORY);
    if (dfd < 0 || fsync(dfd) != 0 || close(dfd) != 0) return -1;
    return 0;
}

/* Run this release's startup migration for every registered object.
 *
 * 2026.08.1 only repairs secondary indexes. Earlier releases already
 * completed the fixed-slot → VARIABLE conversion and compaction.
 * Called before thread pools start, while the database is offline. */
static int run_startup_migration(const char *db_root) {
    char schema_path[PATH_MAX];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", db_root);
    FILE *f = fopen(schema_path, "r");
    if (!f) return 0; /* no schema.conf — nothing to migrate */

    char line[4096];
    int failed = 0;
    while (!failed && fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n")] = '\0';
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || !*p) continue;

        /* Format: dir:object:splits:max_key:2:streams[...] */
        char *c1 = strchr(p, ':');
        if (!c1) { failed = 1; break; }
        *c1 = '\0';
        char *c2 = strchr(c1 + 1, ':');
        if (!c2) { failed = 1; break; }
        *c2 = '\0';

        const char *dir = p;
        const char *obj = c1 + 1;

        char obj_data[PATH_MAX];
        snprintf(obj_data, sizeof(obj_data), "%s/%s/%s", db_root, dir, obj);

        /* Objects with no materialised data have no indexes to rebuild. */
        char kf_probe[PATH_MAX];
        snprintf(kf_probe, sizeof(kf_probe), "%s/data/kf", obj_data);
        struct stat kf_st;
        if (stat(kf_probe, &kf_st) != 0) {
            if (errno == ENOENT) continue;
            failed = 1;
            break;
        }

        char eff_root[PATH_MAX];
        snprintf(eff_root, sizeof(eff_root), "%s/%s", db_root, dir);
        Schema sch = load_schema(eff_root, obj);
        if (sch.splits <= 0) { failed = 1; break; }

        /* Rebuild every secondary index from source-of-truth records. */
        objlock_wrlock(eff_root, obj);
        int reindex_count = 0;
        int reindex_errno = 0;
        int reindex_rc = reindex_object_checked_ex(eff_root, obj, 0,
                                                    &reindex_count,
                                                    &reindex_errno);
        objlock_wrunlock(eff_root, obj);
        if (reindex_rc != 0) {
            LOG_ERROR(LOG_SUB_REINDEX, "reindex failed for %s/%s (errno=%d %s)",
                      dir, obj, reindex_errno, strerror(reindex_errno));
            failed = 1;
        } else if (reindex_count > 0) {
            LOG_INFO(LOG_SUB_REINDEX, "reindexed %s/%s (%d indexes)",
                     dir, obj, reindex_count);
        }
    }
    if (ferror(f)) failed = 1;
    if (fclose(f) != 0) failed = 1;

    return failed ? -1 : 0;
}

/* Empty means no active object records in schema.conf. Configuration-only
 * roots are allowed to bootstrap; schema-only roots are non-empty. */
static int db_root_is_empty(const char *db_root) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/schema.conf", db_root);
    FILE *f = fopen(path, "r");
    if (!f) return errno == ENOENT ? 1 : -1;

    char line[4096];
    int has_object = 0;
    while (fgets(line, sizeof(line), f)) {
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || *p == '\n' || *p == '\0') continue;
        /* Any non-comment schema content makes this a non-empty root.
           Malformed metadata must not obtain the empty-root bypass. */
        has_object = 1;
        break;
    }
    if (ferror(f) || fclose(f) != 0) return -1;
    return has_object ? 0 : 1;
}

/* Convert every FIXED-format object to VARIABLE, unconditionally, on
 * every startup with a non-empty schema.conf. Mirrors the "migrate"
 * JSON mode's semantics (idempotent — no-op if already VARIABLE) and
 * its objlock_wrlock/slotcask_migrate_to_varlen/objlock_wrunlock
 * sequence; unlike server.c's "migrate" handler (which relies on the
 * outer mode_is_schema dispatch already holding the lock), this sweep
 * has no outer dispatch to rely on and takes the lock itself. */
static int run_startup_format_sweep(const char *db_root) {
    char schema_path[PATH_MAX];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", db_root);
    FILE *f = fopen(schema_path, "r");
    if (!f) {
        LOG_ERROR(LOG_SUB_CONFIG,
                  "startup format sweep: cannot open %s: %s",
                  schema_path, strerror(errno));
        return -1;
    }

    char line[4096];
    int failed = 0;
    while (!failed && fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n")] = '\0';
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || !*p) continue;

        char *c1 = strchr(p, ':');
        if (!c1) { failed = 1; break; }
        *c1 = '\0';
        char *c2 = strchr(c1 + 1, ':');
        if (!c2) { failed = 1; break; }
        *c2 = '\0';

        const char *dir = p;
        const char *obj = c1 + 1;

        char obj_data[PATH_MAX];
        snprintf(obj_data, sizeof(obj_data), "%s/%s/%s", db_root, dir, obj);

        /* Objects with no materialised data have no format to convert. */
        char kf_probe[PATH_MAX];
        snprintf(kf_probe, sizeof(kf_probe), "%s/data/kf", obj_data);
        struct stat kf_st;
        if (stat(kf_probe, &kf_st) != 0) {
            if (errno == ENOENT) continue;
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "startup format sweep: cannot inspect %s: %s",
                      kf_probe, strerror(errno));
            failed = 1;
            break;
        }
        if (!S_ISDIR(kf_st.st_mode)) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "startup format sweep: %s is not a directory",
                      kf_probe);
            failed = 1;
            break;
        }

        /* Read the .format marker directly — avoids opening the object
           through the registry (which would register it with whatever
           streams count schema.conf currently declares, potentially
           wrong if an operator or vacuum is mid-rebuild). A materialised
           object without a valid marker is unsafe to interpret. */
        char fmt_path[PATH_MAX];
        snprintf(fmt_path, sizeof(fmt_path), "%s/.format", obj_data);
        int fmt_fd = open(fmt_path, O_RDONLY);
        if (fmt_fd < 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "startup format sweep: cannot open format marker %s: %s",
                      fmt_path, strerror(errno));
            failed = 1;
            break;
        }
        char fmt_byte = 0;
        ssize_t fmt_n;
        do {
            fmt_n = read(fmt_fd, &fmt_byte, 1);
        } while (fmt_n < 0 && errno == EINTR);
        int fmt_errno = errno;
        int fmt_close_rc = close(fmt_fd);
        if (fmt_n != 1 || fmt_close_rc != 0 ||
            (fmt_byte != '0' && fmt_byte != '1')) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "startup format sweep: invalid format marker %s%s%s",
                      fmt_path, fmt_n < 0 ? ": " : "",
                      fmt_n < 0 ? strerror(fmt_errno) :
                      (fmt_close_rc != 0 ? strerror(errno) : ""));
            failed = 1;
            break;
        }
        if (fmt_byte == '1') continue;  /* already VARIABLE */

        /* Object is FIXED on disk. Count actual stream directories rather
           than trusting schema.conf — schema.conf may declare a different
           count mid-rebuild or during deliberate mismatch testing. */
        char streams_dir[PATH_MAX];
        snprintf(streams_dir, sizeof(streams_dir), "%s/data/streams", obj_data);
        DIR *sd = opendir(streams_dir);
        if (!sd) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "startup format sweep: cannot open streams directory %s: %s",
                      streams_dir, strerror(errno));
            failed = 1;
            break;
        }
        int actual_streams = 0;
        struct dirent *de;
        errno = 0;
        while ((de = readdir(sd)) != NULL) {
            if (de->d_name[0] == '.') continue;
            struct stat stream_st;
            if (fstatat(dirfd(sd), de->d_name, &stream_st,
                        AT_SYMLINK_NOFOLLOW) != 0) {
                LOG_ERROR(LOG_SUB_SLOTCASK,
                          "startup format sweep: cannot inspect stream %s/%s: %s",
                          streams_dir, de->d_name, strerror(errno));
                failed = 1;
                break;
            }
            if (!S_ISDIR(stream_st.st_mode)) {
                LOG_ERROR(LOG_SUB_SLOTCASK,
                          "startup format sweep: unexpected non-directory in %s: %s",
                          streams_dir, de->d_name);
                failed = 1;
                break;
            }
            actual_streams++;
        }
        int stream_errno = errno;
        int stream_close_rc = closedir(sd);
        if (failed) break;
        if (stream_errno != 0 || stream_close_rc != 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "startup format sweep: stream enumeration failed for %s: %s",
                      streams_dir, strerror(stream_errno != 0 ? stream_errno : errno));
            failed = 1;
            break;
        }
        if (actual_streams < 1) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "startup format sweep: no stream directories under %s",
                      streams_dir);
            failed = 1;
            break;
        }

        char eff_root[PATH_MAX];
        snprintf(eff_root, sizeof(eff_root), "%s/%s", db_root, dir);
        Schema sch = load_schema(eff_root, obj);
        if (sch.splits <= 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "startup format sweep: invalid schema for %s/%s",
                      dir, obj);
            failed = 1;
            break;
        }
        if (sch.streams != actual_streams) {
            /* A streams mismatch is an intentional rebuild hand-off state:
               opening with schema.conf's value could route new writes into
               the wrong stream set. Leave the object untouched and make the
               deferred repair visible; vacuum/rebuild owns this transition. */
            LOG_WARN(LOG_SUB_SLOTCASK,
                     "startup format sweep: deferring %s/%s due to stream "
                     "mismatch (schema=%d disk=%d)",
                     dir, obj, sch.streams, actual_streams);
            continue;
        }

        SlotcaskSchemaInfo info = {
            .splits    = sch.splits,
            .slot_size = sch.slot_size,
            .streams   = actual_streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(eff_root, obj, &info);
        if (!sdb) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "startup format sweep: cannot open registry entry for %s/%s",
                      dir, obj);
            failed = 1;
            break;
        }

        if (sdb->format != SLOTCASK_FORMAT_FIXED) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "startup format sweep: format marker/registry mismatch for %s/%s",
                      dir, obj);
            slotcask_registry_invalidate(eff_root, obj);
            failed = 1;
            break;
        }

        objlock_wrlock(eff_root, obj);
        int mrc = slotcask_migrate_to_varlen(sdb);
        objlock_wrunlock(eff_root, obj);
        slotcask_registry_invalidate(eff_root, obj);
        if (mrc != 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "startup format sweep: migration failed for %s/%s (rc=%d)",
                      dir, obj, mrc);
            failed = 1;
            break;
        }
        LOG_INFO(LOG_SUB_SLOTCASK,
                 "startup format sweep: migrated %s/%s to VARIABLE",
                 dir, obj);
    }
    if (ferror(f)) failed = 1;
    if (fclose(f) != 0) failed = 1;

    return failed ? -1 : 0;
}

/* Shared startup seam used by cmd_server() and shard_db_open(). */
int shard_db_startup_migrate(const char *db_root,
                             char *out_disk_version, size_t out_sz) {
    char disk_version[64] = {0};
    int read_rc = shard_db_version_file_read(db_root, disk_version,
                                              sizeof(disk_version));
    int empty = db_root_is_empty(db_root);
    if (empty < 0) return -1;
    int present = (read_rc == SHARD_DB_VERSION_FILE_OK);

    if (read_rc == SHARD_DB_VERSION_FILE_ERROR && !empty) return -4;
    int decision = shard_db_version_decide(
        present ? disk_version : NULL, present, empty,
        SHARD_DB_VERSION, SHARD_DB_MIN_VERSION,
        SHARD_DB_HAS_STARTUP_MIGRATION);
    if (decision == SHARD_DB_VERSION_DOWNGRADE && out_disk_version)
        snprintf(out_disk_version, out_sz, "%s", disk_version);
    if (decision < 0) return decision;

    /* Runs on every accepted startup with a non-empty schema.conf,
       independent of the .version decision — a matching .version only
       means this release's index-rebuild migration already ran once; it
       says nothing about whether every object is on VARIABLE format (an
       object can be created, or restored from backup, in FIXED format at
       any later point). This is the standing invariant-enforcement sweep,
       not a per-release migration step. Version refusal must happen first:
       a newer/unsupported database must not be mutated before rejection. */
    if (!empty && run_startup_format_sweep(db_root) != 0) return -1;

    if (decision == SHARD_DB_VERSION_NOOP) return 0;
#if SHARD_DB_HAS_STARTUP_MIGRATION
    if (decision == SHARD_DB_VERSION_RUN_MIGRATION &&
        run_startup_migration(db_root) != 0)
        return -1;
#endif
    return shard_db_version_file_write(db_root, SHARD_DB_VERSION) == 0 ? 0 : -1;
}

/* ── Public API ── */

ShardDb *shard_db_open(const char *db_root) {
    int expected = 0;
    if (!atomic_compare_exchange_strong(&g_instance_open, &expected, 1)) {
        fprintf(stderr, "shard_db_open: only one ShardDb instance allowed per process (V1)\n");
        return NULL;
    }

    mkdirp(db_root);
    int lock_fd = -1;
    if (db_root_lock_acquire(db_root, &lock_fd) != 0) {
        atomic_store(&g_instance_open, 0);
        return NULL;
    }

    ShardDb *db = shard_db_open_internal(db_root);
    if (!db) {
        db_root_lock_release(&lock_fd);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }
    db->db_root_lock_fd = lock_fd;

    /* Expose instance before starting pools so pool_worker / io_pool_worker
       can bind their thread-local g_db on entry. */
    g_shard_db_instance = db;

    if (rebuild_recovery(db_root) != 0) {
        fprintf(stderr,
                "shard_db_open: refusing to open: rebuild recovery requires manual intervention\n");
        g_shard_db_instance = NULL;
        g_db = NULL;
        db_cleanup_before_pools(db);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }

    /* Sweep FIXED-format objects before thread pools start, then run this
       release's version-gated startup migration when required. The format
       invariant is checked on every accepted startup; matching .version
       only suppresses the per-release index-rebuild batch. */
    char disk_version[64] = {0};
    int mrc = shard_db_startup_migrate(db_root, disk_version, sizeof(disk_version));
    if (mrc == SHARD_DB_VERSION_DOWNGRADE) {
        fprintf(stderr,
                "shard_db_open: refusing to open: database version %s is newer "
                "than this binary (%s); install shard-db %s or newer.\n",
                disk_version, SHARD_DB_VERSION, disk_version);
        g_shard_db_instance = NULL;
        g_db = NULL;
        db_cleanup_before_pools(db);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }
    if (mrc == SHARD_DB_VERSION_TOO_OLD) {
        fprintf(stderr,
                "shard_db_open: refusing to open: this database requires "
                "shard-db %s or newer.\n", SHARD_DB_MIN_VERSION);
        g_shard_db_instance = NULL;
        g_db = NULL;
        db_cleanup_before_pools(db);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }
    if (mrc == SHARD_DB_VERSION_INVALID) {
        fprintf(stderr,
                "shard_db_open: refusing to open: %s/.version has invalid "
                "version evidence for a non-empty database.\n", db_root);
        g_shard_db_instance = NULL;
        g_db = NULL;
        db_cleanup_before_pools(db);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }
    if (mrc != 0) {
        fprintf(stderr, "shard_db_open: startup migration failed\n");
        g_shard_db_instance = NULL;
        g_db = NULL;
        /* Thread pools have not started yet. */
        db_cleanup_before_pools(db);
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

    if (bg_threads_start(db, BG_RUNTIME_EMBEDDED) != 0) {
        /* Required background infrastructure did not start. Stop is safe
           after partial startup; pools must stop before mapped caches. */
        bg_threads_stop(db);
        parallel_io_pool_shutdown();
        parallel_pool_shutdown();
        g_shard_db_instance = NULL;
        db_cleanup_before_pools(db);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }

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
    pthread_mutex_destroy(&g_counts_lock);
    pthread_mutex_destroy(&bt_cache_lock);
    btree_mutation_locks_shutdown();
    pthread_mutex_destroy(&g_bt_mutation_lock_table_lock);
    pthread_mutex_destroy(&g_bm_cache_lock);
    pthread_mutex_destroy(&g_kfcache_lock);
    pthread_mutex_destroy(&g_segcache_lock);
    pthread_mutex_destroy(&g_reg_lock);
    pthread_cond_destroy(&g_reg_cond);
    pthread_mutex_destroy(&g_objlock_table_lock);
    pthread_mutex_destroy(&g_ip_lock);
    pthread_mutex_destroy(&g_token_lock);
}

/* Process-global slot (see g_log_handler in config.c) — shard-db allows
   only one ShardDb instance per process (V1), and this is what lets an
   embedded caller register a handler before shard_db_open() returns, so
   startup-migration diagnostics reach it too. */
void shard_db_set_log_handler_global(
    void (*fn)(int type, const char *msg, void *userdata),
    void *userdata) {
    /* release: pairs with the acquire loads in log_msg_sub/log_audit_sub/
       log_slow_query so a background thread that observes the new
       g_log_handler also observes the g_log_handler_ud write below it. */
    atomic_store_explicit(&g_log_handler_ud, userdata, memory_order_relaxed);
    atomic_store_explicit(&g_log_handler, fn, memory_order_release);
}

/* Per-handle compatibility wrapper — db is accepted for API stability but
   otherwise unused; see shard_db_set_log_handler_global(). */
void shard_db_set_log_handler(ShardDb *db,
    void (*fn)(int type, const char *msg, void *userdata),
    void *userdata) {
    if (!db) return;
    shard_db_set_log_handler_global(fn, userdata);
}

/* Final instance teardown after callers have stopped background threads,
   pools, and mmap caches. Kept separate so daemon startup-failure and normal
   shutdown can stop logging before freeing the instance used by its writer. */
void shard_db_destroy_after_storage(ShardDb *db) {
    if (!db) return;
    g_db = db;

    free(db->token_set);
    free(db->token_scope);
    free(db->token_scope_obj);
    free(db->token_perm);
    free(db->token_set_used);

    db_mutexes_destroy();
    if (db->slots_inited) sem_destroy(&db->query_slots);
    db_root_lock_release(&db->db_root_lock_fd);
    g_shard_db_instance = NULL;
    free(db);
    g_db = NULL;
    atomic_store(&g_instance_open, 0);
}

void shard_db_close(ShardDb *db) {
    if (!db) return;
    g_db = db;

    bg_threads_stop(db);
    parallel_io_pool_shutdown();
    parallel_pool_shutdown();
    counts_flush_all();
    bt_cache_shutdown();
    bm_cache_shutdown();
    slotcask_shutdown();
    schema_caches_shutdown();
    shard_db_destroy_after_storage(db);
}
