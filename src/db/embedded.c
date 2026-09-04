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
    pthread_cond_init(&g_kf_open_inflight_cond,  NULL);
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
    db->bulk_commit_window        = 4096;
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

/* $DB_ROOT/.version — durable compatibility evidence. */
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
    int close_error = fclose(f) != 0;
    if (read_error || close_error || n == 0) return SHARD_DB_VERSION_FILE_ERROR;
    if (out[n - 1] == '\n') n--;
    if (n == 0 || memchr(out, '\n', n) || memchr(out, '\r', n))
        return SHARD_DB_VERSION_FILE_ERROR;
    out[n] = '\0';
    return SHARD_DB_VERSION_FILE_OK;
}

int shard_db_version_file_write(const char *db_root, const char *version) {
    if (!db_root || !version || !version[0])
        return SHARD_DB_VERSION_STAMP_FAILED;
    char path[PATH_MAX];
    char tmp[PATH_MAX];
    snprintf(path, sizeof(path), "%s/.version", db_root);
    snprintf(tmp, sizeof(tmp), "%s/.version.tmp.XXXXXX", db_root);
    int fd = mkstemp(tmp);
    if (fd < 0) return SHARD_DB_VERSION_STAMP_FAILED;

    size_t len = strlen(version);
    const char *p = version;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) {
            (void)close(fd);
            (void)unlink(tmp);
            return SHARD_DB_VERSION_STAMP_FAILED;
        }
        p += n;
        len -= (size_t)n;
    }
    if (write(fd, "\n", 1) != 1 || fsync(fd) != 0) {
        (void)close(fd);
        (void)unlink(tmp);
        return SHARD_DB_VERSION_STAMP_FAILED;
    }
    if (close(fd) != 0) {
        (void)unlink(tmp);
        return SHARD_DB_VERSION_STAMP_FAILED;
    }
    if (rename(tmp, path) != 0) {
        (void)unlink(tmp);
        return SHARD_DB_VERSION_STAMP_FAILED;
    }

    int dfd = open(db_root, O_RDONLY | O_DIRECTORY);
    if (dfd < 0) return SHARD_DB_VERSION_STAMP_UNCERTAIN;
    int sync_rc = fsync(dfd);
    int close_rc = close(dfd);
    if (sync_rc != 0 || close_rc != 0)
        return SHARD_DB_VERSION_STAMP_UNCERTAIN;
    return SHARD_DB_VERSION_STAMP_OK;
}

/* True iff `path` is a directory containing no entries besides "." and
   "..". Returns 1 empty, 0 non-empty, -1 on error. */
static int dir_is_empty(const char *path) {
    DIR *d = opendir(path);
    if (!d) return -1;
    int empty = 1;
    int read_errno = 0;
    errno = 0;
    for (struct dirent *de = readdir(d); de; de = readdir(d)) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;
        empty = 0;
        break;
    }
    if (empty) read_errno = errno;
    int close_rc = closedir(d);
    if (read_errno != 0 || close_rc != 0) return -1;
    return empty;
}

/* "No real data yet" check for the compatibility gate below. Bootstrap-
   only config files (dirs.conf, tokens.conf, allowed_ips.conf) and empty
   tenant directories are commonly hand-seeded by an operator before the
   very first daemon start ever (configuration.md documents this as a
   supported first-run workflow: "Add tenants with a plain edit + server
   restart") and carry no on-disk format risk -- only a materialized
   object, which always lives under a tenant directory, does. Skip those
   specific bootstrap filenames, and skip any subdirectory that is itself
   still empty (a tenant dir pre-created before any create-object has run
   against it); anything else marks the root non-empty. */
static int db_root_is_filesystem_empty(const char *db_root) {
    DIR *dir = opendir(db_root);
    if (!dir) return -1;
    int empty = 1;
    int read_errno = 0;
    int sub_failed = 0;
    errno = 0;
    for (struct dirent *de = readdir(dir); de; de = readdir(dir)) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0 ||
            strcmp(de->d_name, ".shard-db.lock") == 0 ||
            strcmp(de->d_name, "dirs.conf") == 0 ||
            strcmp(de->d_name, "tokens.conf") == 0 ||
            strcmp(de->d_name, "allowed_ips.conf") == 0)
            continue;
        char sub_path[PATH_MAX];
        snprintf(sub_path, sizeof(sub_path), "%s/%s", db_root, de->d_name);
        struct stat st;
        if (stat(sub_path, &st) == 0 && S_ISDIR(st.st_mode)) {
            int sub_empty = dir_is_empty(sub_path);
            if (sub_empty < 0) { sub_failed = 1; break; }
            if (sub_empty) continue;
        }
        empty = 0;
        break;
    }
    if (empty && !sub_failed) read_errno = errno;
    int close_rc = closedir(dir);
    if (sub_failed || read_errno != 0 || close_rc != 0) return -1;
    return empty;
}

int shard_db_version_check(const char *db_root,
                           char *out_disk_version, size_t out_sz) {
    char disk_version[64] = {0};
    int empty = db_root_is_filesystem_empty(db_root);
    if (empty < 0) return SHARD_DB_VERSION_INVALID;
    int read_rc = shard_db_version_file_read(db_root, disk_version,
                                              sizeof(disk_version));
    int present = read_rc == SHARD_DB_VERSION_FILE_OK;
    if (read_rc == SHARD_DB_VERSION_FILE_ERROR)
        return SHARD_DB_VERSION_INVALID;
    if (present && out_disk_version && out_sz > 0)
        snprintf(out_disk_version, out_sz, "%s", disk_version);
    return shard_db_version_decide(present ? disk_version : NULL, present,
                                   empty, SHARD_DB_VERSION,
                                   SHARD_DB_REQUIRED_SOURCE_VERSION);
}

int shard_db_version_stamp(const char *db_root) {
    int rc = shard_db_version_file_write(db_root, SHARD_DB_VERSION);
    if (rc == SHARD_DB_VERSION_STAMP_UNCERTAIN) {
        fprintf(stderr,
                "shard-db: stamp commit uncertain; retry startup to verify "
                ".version\n");
    } else if (rc == SHARD_DB_VERSION_STAMP_FAILED) {
        fprintf(stderr,
                "shard-db: failed to stamp compatible database version\n");
    }
    return rc;
}

typedef struct {
    char dir[PATH_MAX];
    char object[PATH_MAX];
    int streams;
} StartupSchemaEntry;

static int startup_schema_line(char *line, StartupSchemaEntry *out) {
    if (!line || !out) return -1;
    line[strcspn(line, "\r\n")] = '\0';
    char *p = line;
    while (*p == ' ' || *p == '\t') p++;
    if (!*p || *p == '#') return 1;

    char *save = NULL;
    char *parts[7] = {0};
    int n = 0;
    for (char *part = strtok_r(p, ":", &save);
         part && n < 7; part = strtok_r(NULL, ":", &save))
        parts[n++] = part;
    /* dir_name_ok is a pure string-format check (no path separators,
       no control chars, no leading dot). is_valid_dir would additionally
       require the dir to be currently registered in dirs.conf, which is
       wrong here — a schema.conf entry for a since-removed tenant dir
       must still parse so the caller can treat it as a soft/stale entry
       instead of a hard parse failure. */
    if (n < 6 || n > 7 || !dir_name_ok(parts[0]) ||
        !is_valid_object(parts[1]))
        return -1;

    char *end = NULL;
    long streams = strtol(parts[5], &end, 10);
    if (*parts[2] == '\0' || *parts[3] == '\0' || *parts[4] == '\0' ||
        *end != '\0' || streams <= 0 || streams > SLOTCASK_MAX_STREAMS)
        return -1;
    if (strcmp(parts[4], "2") != 0) return -1;

    snprintf(out->dir, sizeof(out->dir), "%s", parts[0]);
    snprintf(out->object, sizeof(out->object), "%s", parts[1]);
    out->streams = (int)streams;
    return 0;
}

static int startup_read_schema(const char *db_root,
                               StartupSchemaEntry **out_entries,
                               size_t *out_count) {
    if (!out_entries || !out_count) return -1;
    *out_entries = NULL;
    *out_count = 0;

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/schema.conf", db_root);
    FILE *f = fopen(path, "r");
    if (!f) return errno == ENOENT ? 0 : -1;

    size_t cap = 0;
    char line[4096];
    while (fgets(line, sizeof(line), f)) {
        StartupSchemaEntry entry;
        int rc = startup_schema_line(line, &entry);
        if (rc < 0) {
            free(*out_entries);
            *out_entries = NULL;
            *out_count = 0;
            fclose(f);
            return -1;
        }
        if (rc > 0) continue;
        if (*out_count == cap) {
            size_t next_cap = cap ? cap * 2 : 16;
            StartupSchemaEntry *next = realloc(
                *out_entries, next_cap * sizeof(**out_entries));
            if (!next) {
                free(*out_entries);
                *out_entries = NULL;
                *out_count = 0;
                fclose(f);
                return -1;
            }
            *out_entries = next;
            cap = next_cap;
        }
        (*out_entries)[(*out_count)++] = entry;
    }
    int read_error = ferror(f);
    int close_error = fclose(f) != 0;
    if (read_error || close_error) {
        free(*out_entries);
        *out_entries = NULL;
        *out_count = 0;
        return -1;
    }
    return 0;
}

static int startup_find_schema(const StartupSchemaEntry *entries, size_t count,
                               const char *dir, const char *object) {
    for (size_t i = 0; i < count; i++)
        if (strcmp(entries[i].dir, dir) == 0 &&
            strcmp(entries[i].object, object) == 0)
            return 1;
    return 0;
}

static int startup_readable_file(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    int failed = ferror(f);
    int close_rc = fclose(f);
    return failed || close_rc != 0 ? -1 : 0;
}

int shard_db_validate_before_stamp(const char *db_root) {
    StartupSchemaEntry *entries = NULL;
    size_t count = 0;
    if (startup_read_schema(db_root, &entries, &count) != 0) return -1;

    for (size_t i = 0; i < count; i++) {
        char eff_root[PATH_MAX];
        char object_dir[PATH_MAX];
        snprintf(eff_root, sizeof(eff_root), "%s/%s",
                 db_root, entries[i].dir);
        snprintf(object_dir, sizeof(object_dir), "%s/%s",
                 eff_root, entries[i].object);

        struct stat obj_st;
        if (stat(object_dir, &obj_st) != 0) {
            /* Stale schema.conf entry with no materialized object on disk
               at all — e.g. a leftover reference to a removed tenant dir.
               The auth/route layer rejects unknown dirs before any read
               is dispatched, so this can't cause silent mis-routing; warn
               (soft) rather than refuse startup. A materialized object
               with a genuinely missing/unreadable fields.conf below is
               still fatal. */
            if (errno == ENOENT) continue;
            free(entries);
            return -1;
        }

        char fields_path[PATH_MAX];
        snprintf(fields_path, sizeof(fields_path), "%s/fields.conf",
                 object_dir);
        if (startup_readable_file(fields_path) != 0) {
            free(entries);
            return -1;
        }

        char data_dir[PATH_MAX];
        snprintf(data_dir, sizeof(data_dir), "%s/data", object_dir);
        struct stat st;
        if (stat(data_dir, &st) != 0) {
            if (errno != ENOENT) {
                free(entries);
                return -1;
            }
            continue;
        }
        if (!S_ISDIR(st.st_mode)) {
            free(entries);
            return -1;
        }

        char kf_dir[PATH_MAX];
        char streams_dir[PATH_MAX];
        snprintf(kf_dir, sizeof(kf_dir), "%s/kf", data_dir);
        snprintf(streams_dir, sizeof(streams_dir), "%s/streams", data_dir);
        if (stat(kf_dir, &st) != 0 || !S_ISDIR(st.st_mode) ||
            stat(streams_dir, &st) != 0 || !S_ISDIR(st.st_mode) ||
            slotcask_validate_segment_files(data_dir,
                                             entries[i].streams) != 0) {
            free(entries);
            return -1;
        }

        /* Every materialized kf shard file must be readable -- an
           unreadable shard (permissions, corruption) would otherwise
           surface lazily as a partial-object failure on first query
           instead of at startup. */
        DIR *kfd = opendir(kf_dir);
        if (!kfd) {
            free(entries);
            return -1;
        }
        int kf_failed = 0;
        errno = 0;
        for (struct dirent *ke = readdir(kfd); ke; ke = readdir(kfd)) {
            if (ke->d_name[0] == '.') continue;
            char kf_path[PATH_MAX];
            snprintf(kf_path, sizeof(kf_path), "%s/%s", kf_dir, ke->d_name);
            struct stat kst;
            if (stat(kf_path, &kst) != 0 || !S_ISREG(kst.st_mode)) continue;
            if (startup_readable_file(kf_path) != 0) {
                kf_failed = 1;
                break;
            }
        }
        int kf_errno = errno;
        int kfd_close = closedir(kfd);
        if (kf_failed || kf_errno != 0 || kfd_close != 0) {
            free(entries);
            return -1;
        }
    }

    DIR *root = opendir(db_root);
    if (!root) {
        free(entries);
        return -1;
    }
    int failed = 0;
    errno = 0;
    /* errno is reset immediately before every readdir(root) call (not just
       once, up front) because the loop body's own stat() probe below is
       expected to fail with ENOTDIR/ENOENT for ordinary non-object dirents
       (e.g. a tenant-scoped tokens.conf file sitting next to real object
       dirs, probed as "<tokens.conf>/data"). Without the per-iteration
       reset, that expected failure's errno survives past the loop and is
       misread below as a genuine readdir() error on a clean directory. */
    for (struct dirent *de = readdir(root); de; errno = 0, de = readdir(root)) {
        if (de->d_name[0] == '.') continue;
        if (!is_valid_dir(de->d_name)) continue;
        char dir_path[PATH_MAX];
        snprintf(dir_path, sizeof(dir_path), "%s/%s", db_root, de->d_name);
        DIR *tenant = opendir(dir_path);
        if (!tenant) {
            failed = 1;
            break;
        }
        for (struct dirent *oe = readdir(tenant); oe; oe = readdir(tenant)) {
            if (oe->d_name[0] == '.') continue;
            char object_dir[PATH_MAX];
            snprintf(object_dir, sizeof(object_dir), "%s/%s",
                     dir_path, oe->d_name);
            char data_dir[PATH_MAX];
            snprintf(data_dir, sizeof(data_dir), "%s/data", object_dir);
            struct stat st;
            if (stat(data_dir, &st) == 0 && S_ISDIR(st.st_mode) &&
                !startup_find_schema(entries, count, de->d_name, oe->d_name)) {
                failed = 1;
                break;
            }
        }
        int tenant_close = closedir(tenant);
        if (tenant_close != 0) failed = 1;
        if (failed) break;
    }
    int root_errno = errno;
    int root_close = closedir(root);
    if (root_errno != 0 || root_close != 0) failed = 1;
    free(entries);
    return failed ? -1 : 0;
}

int shard_db_recover_before_stamp(const char *db_root,
                                  int *out_markers_replayed) {
    if (!out_markers_replayed) return -1;
    *out_markers_replayed = 0;
    if (rebuild_recovery(db_root) != 0) return -1;

    int was_clean = clean_flag_exists(db_root);
    if (clean_flag_remove(db_root) != 0) return -1;
    if (was_clean) return 0;

    StartupSchemaEntry *entries = NULL;
    size_t count = 0;
    if (startup_read_schema(db_root, &entries, &count) != 0) return -1;
    for (size_t i = 0; i < count; i++) {
        char eff_root[PATH_MAX];
        char data_dir[PATH_MAX];
        snprintf(eff_root, sizeof(eff_root), "%s/%s",
                 db_root, entries[i].dir);
        snprintf(data_dir, sizeof(data_dir), "%s/%s/%s",
                 db_root, entries[i].dir, entries[i].object);
        objlock_wrlock(eff_root, entries[i].object);
        int rc = marker_recovery_sweep_object(eff_root, data_dir,
                                               entries[i].object,
                                               out_markers_replayed);
        objlock_wrunlock(eff_root, entries[i].object);
        if (rc != 0) {
            free(entries);
            return -1;
        }
    }
    free(entries);
    return 0;
}

int shard_db_mark_clean_if_safe(const char *db_root) {
    StartupSchemaEntry *entries = NULL;
    size_t count = 0;
    if (startup_read_schema(db_root, &entries, &count) != 0) return -1;
    for (size_t i = 0; i < count; i++) {
        char data_dir[PATH_MAX];
        snprintf(data_dir, sizeof(data_dir), "%s/%s/%s",
                 db_root, entries[i].dir, entries[i].object);
        int pending = object_has_pending_markers(data_dir);
        if (pending != 0) {
            free(entries);
            return -1;
        }
    }
    free(entries);
    return clean_flag_write(db_root);
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

    char disk_version[64] = {0};
    int version_decision = shard_db_version_check(db_root, disk_version,
                                                  sizeof(disk_version));
    if (version_decision < 0) {
        if (version_decision == SHARD_DB_VERSION_DOWNGRADE) {
            fprintf(stderr,
                    "shard_db_open: refusing to open: database version %s is "
                    "newer than this binary (%s); install shard-db %s or newer.\n",
                    disk_version, SHARD_DB_VERSION, disk_version);
        } else if (version_decision == SHARD_DB_VERSION_TOO_OLD) {
            fprintf(stderr,
                    "shard_db_open: refusing to open: this database requires "
                    "shard-db %s or newer.\n",
                    SHARD_DB_REQUIRED_SOURCE_VERSION);
        } else {
            fprintf(stderr,
                    "shard_db_open: refusing to open: non-empty DB_ROOT lacks "
                    "valid 2026.08.1/2026.08.2 compatibility evidence\n");
        }
        db_root_lock_release(&lock_fd);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }
    int stamp_required = version_decision == SHARD_DB_VERSION_STAMP;

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

    int markers_replayed = 0;
    if (shard_db_recover_before_stamp(db_root, &markers_replayed) != 0 ||
        shard_db_validate_before_stamp(db_root) != 0) {
        fprintf(stderr,
                "shard_db_open: refusing to open: startup preparation failed\n");
        g_shard_db_instance = NULL;
        g_db = NULL;
        db_cleanup_before_pools(db);
        atomic_store(&g_instance_open, 0);
        return NULL;
    }
    (void)markers_replayed;

    if (stamp_required &&
        shard_db_version_stamp(db_root) != SHARD_DB_VERSION_STAMP_OK) {
        g_shard_db_instance = NULL;
        g_db = NULL;
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
    pthread_cond_destroy(&g_kf_open_inflight_cond);
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
    if (shard_db_mark_clean_if_safe(db->db_root) != 0)
        fprintf(stderr, "shard_db_close: failed to record clean shutdown\n");
    bt_cache_shutdown();
    bm_cache_shutdown();
    slotcask_shutdown();
    schema_caches_shutdown();
    shard_db_destroy_after_storage(db);
}
