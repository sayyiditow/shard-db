#include "types.h"

/* ========== Config ========== */

uint32_t g_timeout = 30;
int g_port = 9199;
int g_max_threads = 0;  /* 0 = auto (nproc) — for scan_shards */
int g_workers = 0;      /* 0 = auto (nproc, min 4) — server thread pool */
int g_index_page_size = 4096;
int g_global_limit = 100000;
int g_max_request_size = 33554432; /* 32 MB default, configurable via MAX_REQUEST_SIZE */
int g_fcache_cap = 4096;        /* shard mmap cache capacity, configurable via FCACHE_MAX */
int g_btcache_cap = 256;        /* B+ tree mmap cache capacity, configurable via BT_CACHE_MAX */
size_t g_query_buffer_max_bytes = 500ULL * 1024 * 1024; /* 500 MB per-query intermediate cap, configurable via QUERY_BUFFER_MB */
int g_disable_localhost_trust = 0; /* default: 127.0.0.1/::1 bypass auth. Set via DISABLE_LOCALHOST_TRUST=1 for strict mode. */
int g_token_cap = 1024;            /* token table bucket count, configurable via TOKEN_CAP (floor 64, ceiling 1M) */
_Thread_local uint32_t g_request_timeout_ms = 0;  /* per-request override; 0 = use g_timeout */

/* Monitoring counters */
uint64_t g_ucache_hits = 0;
uint64_t g_ucache_misses = 0;
uint64_t g_bt_cache_hits = 0;
uint64_t g_bt_cache_misses = 0;
uint64_t g_server_start_ms = 0;
uint64_t g_slow_query_count = 0;
int g_slow_query_ms = 500;  /* configurable via SLOW_QUERY_MS */
SlowQueryEntry g_slow_queries[SLOW_QUERY_RING] = {0};
int g_slow_query_head = 0;
pthread_mutex_t g_slow_query_lock = PTHREAD_MUTEX_INITIALIZER;

int parallel_threads(void) {
    if (g_max_threads > 0) return g_max_threads;
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? (int)n : 4;
}

uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/* Coarse clock: ~1-4ms granularity, essentially free (vDSO, no syscall).
   Used by hot-loop timeout checks where ms precision is plenty. */
uint64_t now_ms_coarse(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

extern char g_log_dir[PATH_MAX];

void log_slow_query(const char *mode, const char *dir, const char *object, uint32_t duration_ms) {
    __atomic_add_fetch(&g_slow_query_count, 1, __ATOMIC_RELAXED);
    pthread_mutex_lock(&g_slow_query_lock);
    SlowQueryEntry *e = &g_slow_queries[g_slow_query_head];
    e->ts_ms = now_ms();
    e->duration_ms = duration_ms;
    snprintf(e->mode,   sizeof(e->mode),   "%s", mode   ? mode   : "");
    snprintf(e->dir,    sizeof(e->dir),    "%s", dir    ? dir    : "");
    snprintf(e->object, sizeof(e->object), "%s", object ? object : "");
    g_slow_query_head = (g_slow_query_head + 1) % SLOW_QUERY_RING;
    pthread_mutex_unlock(&g_slow_query_lock);
    /* Dated slow log file, same rotation as info/error */
    time_t t = time(NULL);
    struct tm *tm = localtime(&t);
    char date[16], path[PATH_MAX];
    strftime(date, sizeof(date), "%Y-%m-%d", tm);
    snprintf(path, sizeof(path), "%s/slow-%s.log", g_log_dir, date);
    FILE *f = fopen(path, "a");
    if (f) {
        char iso[32];
        strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%S", tm);
        fprintf(f, "%s %ums mode=%s dir=%s object=%s\n",
                iso, duration_ms,
                mode ? mode : "", dir ? dir : "", object ? object : "");
        fclose(f);
    }
}
__thread FILE *g_out = NULL;    /* per-thread output; init to stdout in main() */
char g_db_root[PATH_MAX] = {0};

/* Async logging — ring buffer + background flush thread (tinylog style)
   Two log files: info-YYYY-MM-DD.log and error-YYYY-MM-DD.log
   Levels: 0=off, 1=error, 2=warn, 3=info (writes), 4=debug (reads) */
int g_log_level = 3;
int g_log_retain_days = 7;

#define LOG_RING_SIZE 8192
#define LOG_MSG_SIZE  512

typedef struct {
    char msg[LOG_MSG_SIZE];
    int level;
} LogEntry;

LogEntry g_log_ring[LOG_RING_SIZE];
volatile int g_log_head = 0;
volatile int g_log_tail = 0;
pthread_mutex_t g_log_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t g_log_cond = PTHREAD_COND_INITIALIZER;
pthread_t g_log_thread;
volatile int g_log_running = 0;
char g_log_dir[PATH_MAX];

FILE *open_log_for_level(int level) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char date[16], path[PATH_MAX];
    strftime(date, sizeof(date), "%Y-%m-%d", t);
    const char *prefix = (level <= 2) ? "error" : "info";
    snprintf(path, sizeof(path), "%s/%s-%s.log", g_log_dir, date, prefix);
    return fopen(path, "a");
}

void purge_old_logs(void) {
    if (g_log_retain_days <= 0) return;
    time_t cutoff = time(NULL) - g_log_retain_days * 86400;
    DIR *d = opendir(g_log_dir);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d))) {
        size_t nlen = strlen(e->d_name);
        if (nlen < 4 || strcmp(e->d_name + nlen - 4, ".log") != 0)
            continue;
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", g_log_dir, e->d_name);
        struct stat st;
        if (stat(path, &st) == 0 && st.st_mtime < cutoff)
            unlink(path);
    }
    closedir(d);
}

void *log_writer_thread(void *arg) {
    (void)arg;
    int flush_counter = 0;
    while (g_log_running || g_log_head != g_log_tail) {
        pthread_mutex_lock(&g_log_lock);
        while (g_log_head == g_log_tail && g_log_running)
            pthread_cond_wait(&g_log_cond, &g_log_lock);

        /* Batch flush */
        FILE *info_f = NULL, *err_f = NULL;
        while (g_log_head != g_log_tail) {
            LogEntry *e = &g_log_ring[g_log_tail];
            FILE **target = (e->level <= 2) ? &err_f : &info_f;
            if (!*target) *target = open_log_for_level(e->level);
            if (*target) fputs(e->msg, *target);
            g_log_tail = (g_log_tail + 1) % LOG_RING_SIZE;
        }
        pthread_mutex_unlock(&g_log_lock);

        if (info_f) { fflush(info_f); fclose(info_f); }
        if (err_f) { fflush(err_f); fclose(err_f); }

        /* Purge old logs every ~1000 flushes */
        if (++flush_counter % 1000 == 0) purge_old_logs();
    }
    return NULL;
}

void log_init(const char *db_root) {
    /* Use LOG_DIR if configured, otherwise default to $DB_ROOT/logs */
    if (g_log_dir[0] == '\0')
        snprintf(g_log_dir, sizeof(g_log_dir), "%s/logs", db_root);
    mkdirp(g_log_dir);
    g_log_running = 1;
    pthread_create(&g_log_thread, NULL, log_writer_thread, NULL);
    purge_old_logs();
}

void log_shutdown(void) {
    g_log_running = 0;
    pthread_cond_signal(&g_log_cond);
    pthread_join(g_log_thread, NULL);
}

void log_msg(int level, const char *fmt, ...) {
    if (level > g_log_level || !g_log_running) return;
    const char *labels[] = {"", "ERROR", "WARN", "INFO", "DEBUG"};
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", t);

    LogEntry entry;
    entry.level = level;
    int pos = snprintf(entry.msg, sizeof(entry.msg), "%s [%s] ", ts, labels[level]);
    va_list ap;
    va_start(ap, fmt);
    pos += vsnprintf(entry.msg + pos, sizeof(entry.msg) - pos, fmt, ap);
    va_end(ap);
    if (pos < (int)sizeof(entry.msg) - 1) { entry.msg[pos] = '\n'; entry.msg[pos+1] = '\0'; }

    /* Non-blocking enqueue */
    pthread_mutex_lock(&g_log_lock);
    int next = (g_log_head + 1) % LOG_RING_SIZE;
    if (next != g_log_tail) {
        g_log_ring[g_log_head] = entry;
        g_log_head = next;
    }
    pthread_cond_signal(&g_log_cond);
    pthread_mutex_unlock(&g_log_lock);
}

int load_db_root(char *out, size_t outlen) {
    FILE *f = fopen("db.env", "r");
    if (!f) { fprintf(stderr, "Error: db.env not found\n"); return -1; }
    char line[PATH_MAX + 64];
    int found_root = 0;
    while (fgets(line, sizeof(line), f)) {
        char *p = line;
        if (strncmp(p, "export ", 7) == 0) p += 7;
        if (strncmp(p, "DB_ROOT=", 8) == 0) {
            p += 8;
            if (*p == '"') p++;
            char *end = p + strlen(p) - 1;
            while (end > p && (*end == '\n' || *end == '\r' || *end == '"')) *end-- = '\0';
            snprintf(out, outlen, "%s", p);
            snprintf(g_db_root, sizeof(g_db_root), "%s", p);
            found_root = 1;
        } else if (strncmp(p, "TIMEOUT=", 8) == 0) {
            g_timeout = (uint32_t)atoi(p + 8);
        } else if (strncmp(p, "PORT=", 5) == 0) {
            g_port = atoi(p + 5);
        } else if (strncmp(p, "LOG_DIR=", 8) == 0) {
            p += 8;
            if (*p == '"' || *p == '\'') p++;
            char *end2 = p + strlen(p) - 1;
            while (end2 > p && (*end2 == '\n' || *end2 == '\r' || *end2 == '"' || *end2 == '\'')) *end2-- = '\0';
            snprintf(g_log_dir, sizeof(g_log_dir), "%s", p);
        } else if (strncmp(p, "LOG_LEVEL=", 10) == 0) {
            g_log_level = atoi(p + 10);
        } else if (strncmp(p, "LOG_RETAIN_DAYS=", 16) == 0) {
            g_log_retain_days = atoi(p + 16);
        } else if (strncmp(p, "INDEX_PAGE_SIZE=", 16) == 0) {
            int ps = atoi(p + 16);
            if (ps >= 1024 && ps <= 65536) g_index_page_size = ps;
        } else if (strncmp(p, "THREADS=", 8) == 0) {
            g_max_threads = atoi(p + 8);
        } else if (strncmp(p, "WORKERS=", 8) == 0) {
            g_workers = atoi(p + 8);
        } else if (strncmp(p, "GLOBAL_LIMIT=", 13) == 0) {
            g_global_limit = atoi(p + 13);
        } else if (strncmp(p, "MAX_REQUEST_SIZE=", 17) == 0) {
            int sz = atoi(p + 17);
            if (sz >= 1024) g_max_request_size = sz;
        } else if (strncmp(p, "FCACHE_MAX=", 11) == 0) {
            int n = atoi(p + 11);
            if (n >= 16 && n <= 1048576) g_fcache_cap = n;
        } else if (strncmp(p, "BT_CACHE_MAX=", 13) == 0) {
            int n = atoi(p + 13);
            if (n >= 16 && n <= 1048576) g_btcache_cap = n;
        } else if (strncmp(p, "QUERY_BUFFER_MB=", 16) == 0) {
            long mb = atol(p + 16);
            if (mb >= 1 && mb <= 1048576)  /* 1 MB floor, 1 TB ceiling */
                g_query_buffer_max_bytes = (size_t)mb * 1024 * 1024;
        } else if (strncmp(p, "DISABLE_LOCALHOST_TRUST=", 24) == 0) {
            g_disable_localhost_trust = (atoi(p + 24) != 0);
        } else if (strncmp(p, "TOKEN_CAP=", 10) == 0) {
            int n = atoi(p + 10);
            if (n >= 64 && n <= 1048576) g_token_cap = n;
        } else if (strncmp(p, "SLOW_QUERY_MS=", 14) == 0) {
            int n = atoi(p + 14);
            if (n == 0) {
                g_slow_query_ms = 0;       /* explicit disable */
            } else {
                if (n < 100) n = 100;      /* floor: prevents log spam */
                if (n > 600000) n = 600000;
                g_slow_query_ms = n;
            }
        }
    }
    fclose(f);
    if (!found_root) { fprintf(stderr, "Error: DB_ROOT not found in db.env\n"); return -1; }
    /* Default LOG_DIR to $DB_ROOT/logs if not set */
    if (g_log_dir[0] == '\0')
        snprintf(g_log_dir, sizeof(g_log_dir), "%s/logs", out);
    return 0;
}

/* ========== String hash set (reusable) ========== */

static uint32_t str_hash(const char *s) {
    uint32_t h = 5381;
    while (*s) h = h * 33 + (unsigned char)*s++;
    return h;
}

/* ========== dirs.conf — allowed tenant directories ========== */

#define DIRS_BUCKETS 2048
char g_dirs[DIRS_BUCKETS][256];
int g_dirs_used[DIRS_BUCKETS];
int g_dirs_count = 0;
pthread_mutex_t g_dirs_lock = PTHREAD_MUTEX_INITIALIZER;

void load_dirs(void) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/dirs.conf", g_db_root);
    FILE *f = fopen(path, "r");
    if (!f) return;
    pthread_mutex_lock(&g_dirs_lock);
    memset(g_dirs_used, 0, sizeof(g_dirs_used));
    g_dirs_count = 0;
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n")] = '\0';
        if (line[0] == '\0' || line[0] == '#') continue;
        uint32_t idx = str_hash(line) % DIRS_BUCKETS;
        for (int i = 0; i < DIRS_BUCKETS; i++) {
            int slot = (idx + i) % DIRS_BUCKETS;
            if (!g_dirs_used[slot]) {
                strncpy(g_dirs[slot], line, 255);
                g_dirs[slot][255] = '\0';
                g_dirs_used[slot] = 1;
                g_dirs_count++;
                break;
            }
        }
    }
    fclose(f);
    pthread_mutex_unlock(&g_dirs_lock);
}

int is_valid_dir(const char *dir) {
    if (!dir || !dir[0]) return 0;
    pthread_mutex_lock(&g_dirs_lock);
    uint32_t idx = str_hash(dir) % DIRS_BUCKETS;
    for (int i = 0; i < DIRS_BUCKETS; i++) {
        int slot = (idx + i) % DIRS_BUCKETS;
        if (!g_dirs_used[slot]) { pthread_mutex_unlock(&g_dirs_lock); return 0; }
        if (strcmp(g_dirs[slot], dir) == 0) { pthread_mutex_unlock(&g_dirs_lock); return 1; }
    }
    pthread_mutex_unlock(&g_dirs_lock);
    return 0;
}

/* Build effective root: $DB_ROOT/<dir> */
void build_effective_root(char *out, size_t outlen, const char *dir) {
    snprintf(out, outlen, "%s/%s", g_db_root, dir);
}

/* Schema cache — hash set, avoids re-reading schema.conf on every request */
#define SCHEMA_BUCKETS 256
struct SchemaCache { char name[512]; Schema schema; int used; };
struct SchemaCache g_schema_cache[SCHEMA_BUCKETS];
pthread_mutex_t g_schema_lock = PTHREAD_MUTEX_INITIALIZER;

Schema load_schema(const char *effective_root, const char *object) {
    char cache_key[512];
    snprintf(cache_key, sizeof(cache_key), "%s:%s", effective_root, object);

    uint32_t idx = str_hash(cache_key) % SCHEMA_BUCKETS;
    pthread_mutex_lock(&g_schema_lock);
    for (int i = 0; i < SCHEMA_BUCKETS; i++) {
        int slot = (idx + i) % SCHEMA_BUCKETS;
        if (!g_schema_cache[slot].used) break;
        if (strcmp(g_schema_cache[slot].name, cache_key) == 0) {
            Schema s = g_schema_cache[slot].schema;
            pthread_mutex_unlock(&g_schema_lock);
            return s;
        }
    }
    pthread_mutex_unlock(&g_schema_lock);

    /* Extract dir name from effective_root: $DB_ROOT/<dir> → dir */
    const char *dir = effective_root + strlen(g_db_root);
    if (*dir == '/') dir++;

    /* Build search prefix: "dir:object:" */
    char prefix[512];
    int pfxlen = snprintf(prefix, sizeof(prefix), "%s:%s:", dir, object);

    Schema s = {0};  /* zeroed — filled from schema.conf */
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/schema.conf", g_db_root);
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "Error: Object [%s] not found\n", object);
        return s;
    }
    char line[512];
    while (fgets(line, sizeof(line), f)) {
        if (line[0] == '#' || line[0] == '\n') continue;
        if (strncmp(line, prefix, pfxlen) == 0) {
            char *p = line + pfxlen;
            s.splits = atoi(p);
            if (s.splits <= 0) s.splits = 64;
            char *p2 = strchr(p, ':');
            if (p2) {
                s.max_key = atoi(p2 + 1);
                char *p3 = strchr(p2 + 1, ':');
                if (p3) {
                    s.prealloc_mb = atoi(p3 + 1);
                }
            }
            break;
        }
    }
    fclose(f);

    /* Derive max_value from typed fields.conf */
    TypedSchema *ts = load_typed_schema(effective_root, object);
    if (ts && ts->total_size > 0) {
        s.max_value = ts->total_size;
    }
    s.slot_size = s.max_key + s.max_value;
    s.slot_size = (s.slot_size + 7) & ~7;

    pthread_mutex_lock(&g_schema_lock);
    uint32_t sidx = str_hash(cache_key) % SCHEMA_BUCKETS;
    for (int i = 0; i < SCHEMA_BUCKETS; i++) {
        int slot = (sidx + i) % SCHEMA_BUCKETS;
        if (!g_schema_cache[slot].used) {
            strncpy(g_schema_cache[slot].name, cache_key, 511);
            g_schema_cache[slot].schema = s;
            g_schema_cache[slot].used = 1;
            break;
        }
    }
    pthread_mutex_unlock(&g_schema_lock);
    return s;
}

/* Backward compat wrapper */
int load_splits(const char *db_root, const char *object) {
    return load_schema(db_root, object).splits;
}

/* ========== fields.conf — columnar field order per object ========== */

#define FIELDS_BUCKETS 256
struct FieldsCache {
    char name[256];       /* "db_root:object" */
    char fields[MAX_FIELDS][256];
    int nfields;
    int used;
};
struct FieldsCache g_fields_cache[FIELDS_BUCKETS];
pthread_mutex_t g_fields_lock = PTHREAD_MUTEX_INITIALIZER;

int load_fields_conf(const char *db_root, const char *object,
                     char fields[][256], int max_fields) {
    /* Build cache key */
    char cache_key[512];
    snprintf(cache_key, sizeof(cache_key), "%s:%s", db_root, object);

    uint32_t idx = str_hash(cache_key) % FIELDS_BUCKETS;
    pthread_mutex_lock(&g_fields_lock);
    for (int i = 0; i < FIELDS_BUCKETS; i++) {
        int slot = (idx + i) % FIELDS_BUCKETS;
        if (!g_fields_cache[slot].used) break;
        if (strcmp(g_fields_cache[slot].name, cache_key) == 0) {
            int n = g_fields_cache[slot].nfields;
            for (int j = 0; j < n && j < max_fields; j++)
                memcpy(fields[j], g_fields_cache[slot].fields[j], 256);
            pthread_mutex_unlock(&g_fields_lock);
            return n < max_fields ? n : max_fields;
        }
    }
    pthread_mutex_unlock(&g_fields_lock);

    /* Cache miss — read from file */
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/fields.conf", db_root, object);
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    int count = 0;
    char line[256];
    while (fgets(line, sizeof(line), f) && count < max_fields) {
        line[strcspn(line, "\n")] = '\0';
        if (line[0] == '\0' || line[0] == '#') continue;
        strncpy(fields[count], line, 255);
        fields[count][255] = '\0';
        count++;
    }
    fclose(f);

    /* Store in cache */
    pthread_mutex_lock(&g_fields_lock);
    uint32_t sidx = str_hash(cache_key) % FIELDS_BUCKETS;
    for (int i = 0; i < FIELDS_BUCKETS; i++) {
        int slot = (sidx + i) % FIELDS_BUCKETS;
        if (!g_fields_cache[slot].used) {
            strncpy(g_fields_cache[slot].name, cache_key, 255);
            g_fields_cache[slot].nfields = count;
            for (int j = 0; j < count; j++)
                memcpy(g_fields_cache[slot].fields[j], fields[j], 256);
            g_fields_cache[slot].used = 1;
            break;
        }
    }
    pthread_mutex_unlock(&g_fields_lock);

    return count;
}

/* Index fields cache — hash set, avoids re-reading index.conf on every insert */
#define IDX_BUCKETS 256
struct IdxCache {
    char name[256];
    char fields[MAX_FIELDS][256];
    int nfields;
    int used;
};
struct IdxCache g_idx_cache[IDX_BUCKETS];
pthread_mutex_t g_idx_lock = PTHREAD_MUTEX_INITIALIZER;

int load_index_fields(const char *db_root, const char *object,
                             char fields[][256], int max_fields) {
    uint32_t idx = str_hash(object) % IDX_BUCKETS;
    pthread_mutex_lock(&g_idx_lock);
    for (int i = 0; i < IDX_BUCKETS; i++) {
        int slot = (idx + i) % IDX_BUCKETS;
        if (!g_idx_cache[slot].used) break;
        if (strcmp(g_idx_cache[slot].name, object) == 0) {
            int n = g_idx_cache[slot].nfields;
            for (int j = 0; j < n && j < max_fields; j++)
                memcpy(fields[j], g_idx_cache[slot].fields[j], 256);
            pthread_mutex_unlock(&g_idx_lock);
            return n < max_fields ? n : max_fields;
        }
    }
    pthread_mutex_unlock(&g_idx_lock);

    /* Cache miss — read from file */
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/indexes/index.conf", db_root, object);
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    int count = 0;
    char line[256];
    while (fgets(line, sizeof(line), f) && count < max_fields) {
        line[strcspn(line, "\n")] = '\0';
        if (line[0] == '\0' || line[0] == '#') continue;
        strncpy(fields[count], line, 255);
        fields[count][255] = '\0';
        count++;
    }
    fclose(f);

    /* Store in cache */
    pthread_mutex_lock(&g_idx_lock);
    uint32_t sidx = str_hash(object) % IDX_BUCKETS;
    for (int i = 0; i < IDX_BUCKETS; i++) {
        int slot = (sidx + i) % IDX_BUCKETS;
        if (!g_idx_cache[slot].used) {
            strncpy(g_idx_cache[slot].name, object, 255);
            g_idx_cache[slot].nfields = count;
            for (int j = 0; j < count; j++)
                memcpy(g_idx_cache[slot].fields[j], fields[j], 256);
            g_idx_cache[slot].used = 1;
            break;
        }
    }
    pthread_mutex_unlock(&g_idx_lock);

    return count;
}

/* Invalidate index cache for an object (after add-index) */
void invalidate_idx_cache(const char *object) {
    uint32_t idx = str_hash(object) % IDX_BUCKETS;
    pthread_mutex_lock(&g_idx_lock);
    for (int i = 0; i < IDX_BUCKETS; i++) {
        int slot = (idx + i) % IDX_BUCKETS;
        if (!g_idx_cache[slot].used) break;
        if (strcmp(g_idx_cache[slot].name, object) == 0) {
            g_idx_cache[slot].used = 0;
            break;
        }
    }
    pthread_mutex_unlock(&g_idx_lock);
}

/* ========== Typed Field System ========== */

/* Parse type string like "varchar:40", "long", "numeric:19,4" */
void parse_field_type(const char *spec, TypedField *f) {
    f->type = FT_NONE;
    f->size = 0;
    f->numeric_scale = 0;

    if (strncmp(spec, "varchar:", 8) == 0) {
        int n = atoi(spec + 8);
        if (n <= 0 || n > 65535) return;  /* invalid — leaves FT_NONE, create-object rejects */
        f->type = FT_VARCHAR;
        /* On-disk layout: [uint16 BE length][content bytes]. Reserve len + 2. */
        f->size = n + 2;
    } else if (strcmp(spec, "varchar") == 0) {
        /* Bare "varchar" without :N — require explicit length */
        return;
    } else if (strcmp(spec, "long") == 0) {
        f->type = FT_LONG;
        f->size = 8;
    } else if (strcmp(spec, "int") == 0) {
        f->type = FT_INT;
        f->size = 4;
    } else if (strcmp(spec, "short") == 0) {
        f->type = FT_SHORT;
        f->size = 2;
    } else if (strcmp(spec, "double") == 0) {
        f->type = FT_DOUBLE;
        f->size = 8;
    } else if (strcmp(spec, "bool") == 0) {
        f->type = FT_BOOL;
        f->size = 1;
    } else if (strcmp(spec, "byte") == 0) {
        f->type = FT_BYTE;
        f->size = 1;
    } else if (strcmp(spec, "date") == 0) {
        f->type = FT_DATE;
        f->size = 4;
    } else if (strcmp(spec, "datetime") == 0) {
        f->type = FT_DATETIME;
        f->size = 6;
    } else if (strcmp(spec, "currency") == 0) {
        f->type = FT_NUMERIC;
        f->size = 8;
        f->numeric_scale = 4; /* currency = numeric:19,4 */
    } else if (strncmp(spec, "numeric:", 8) == 0) {
        f->type = FT_NUMERIC;
        f->size = 8;
        /* Parse P,S */
        const char *comma = strchr(spec + 8, ',');
        if (comma) f->numeric_scale = atoi(comma + 1);
        else f->numeric_scale = 2; /* default 2 decimal places */
    }
}

/* Cache for typed schemas */
#define TYPED_BUCKETS 256
struct TypedCacheEntry {
    char name[512];
    TypedSchema schema;
    int used;
};
static struct TypedCacheEntry g_typed_cache[TYPED_BUCKETS];
static pthread_mutex_t g_typed_lock = PTHREAD_MUTEX_INITIALIZER;

TypedSchema *load_typed_schema(const char *db_root, const char *object) {
    char cache_key[512];
    snprintf(cache_key, sizeof(cache_key), "%s:%s", db_root, object);

    uint32_t idx = str_hash(cache_key) % TYPED_BUCKETS;
    pthread_mutex_lock(&g_typed_lock);
    for (int i = 0; i < TYPED_BUCKETS; i++) {
        int slot = (idx + i) % TYPED_BUCKETS;
        if (!g_typed_cache[slot].used) break;
        if (strcmp(g_typed_cache[slot].name, cache_key) == 0) {
            TypedSchema *ts = &g_typed_cache[slot].schema;
            pthread_mutex_unlock(&g_typed_lock);
            return ts->typed ? ts : NULL;
        }
    }
    pthread_mutex_unlock(&g_typed_lock);

    /* Cache miss — read from file */
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/fields.conf", db_root, object);
    FILE *f = fopen(path, "r");
    if (!f) {
        /* No fields.conf — cache as non-typed */
        pthread_mutex_lock(&g_typed_lock);
        uint32_t sidx = str_hash(cache_key) % TYPED_BUCKETS;
        for (int i = 0; i < TYPED_BUCKETS; i++) {
            int slot = (sidx + i) % TYPED_BUCKETS;
            if (!g_typed_cache[slot].used) {
                strncpy(g_typed_cache[slot].name, cache_key, 511);
                g_typed_cache[slot].schema.typed = 0;
                g_typed_cache[slot].schema.nfields = 0;
                g_typed_cache[slot].used = 1;
                break;
            }
        }
        pthread_mutex_unlock(&g_typed_lock);
        return NULL;
    }

    TypedSchema ts;
    memset(&ts, 0, sizeof(ts));
    ts.typed = 0;
    ts.nfields = 0;
    int offset = 0;

    char line[512];
    while (fgets(line, sizeof(line), f) && ts.nfields < MAX_FIELDS) {
        line[strcspn(line, "\n")] = '\0';
        if (line[0] == '\0' || line[0] == '#') continue;

        TypedField *tf = &ts.fields[ts.nfields];
        /* Parse "name:type[:param][:removed]" */
        char *colon = strchr(line, ':');
        if (!colon) continue; /* no type = skip (legacy format) */

        size_t name_len = colon - line;
        if (name_len >= 256) name_len = 255;
        memcpy(tf->name, line, name_len);
        tf->name[name_len] = '\0';

        /* Tombstone marker: line ends with ":removed". Strip it before
           type parsing so the type spec parses cleanly. The field's
           offset/size are still reserved to preserve on-disk layout. */
        tf->removed = 0;
        tf->default_kind = DK_NONE;
        tf->default_val[0] = '\0';
        char type_spec[256];
        strncpy(type_spec, colon + 1, sizeof(type_spec) - 1);
        type_spec[sizeof(type_spec) - 1] = '\0';
        size_t ts_len = strlen(type_spec);
        if (ts_len >= 8 && strcmp(type_spec + ts_len - 8, ":removed") == 0) {
            type_spec[ts_len - 8] = '\0';
            tf->removed = 1;
            ts_len -= 8;
        }

        /* Default modifiers — strip from type_spec before parse_field_type.
           Format: :auto_create, :auto_update, :default=<val>
           These are always the last colon-segment (before :removed). */
        char *dflt;
        if ((dflt = strstr(type_spec, ":auto_update")) != NULL &&
            dflt[12] == '\0') {
            tf->default_kind = DK_AUTO_UPDATE;
            *dflt = '\0';
        } else if ((dflt = strstr(type_spec, ":auto_create")) != NULL &&
                   dflt[12] == '\0') {
            tf->default_kind = DK_AUTO_CREATE;
            *dflt = '\0';
        } else if ((dflt = strstr(type_spec, ":default=")) != NULL) {
            const char *dval = dflt + 9;
            if (strncmp(dval, "seq(", 4) == 0 && strchr(dval, ')')) {
                tf->default_kind = DK_SEQ;
                /* Extract seq name from seq(name) */
                const char *ns = dval + 4;
                const char *ne = strchr(ns, ')');
                size_t nl = ne - ns;
                if (nl >= sizeof(tf->default_val)) nl = sizeof(tf->default_val) - 1;
                memcpy(tf->default_val, ns, nl);
                tf->default_val[nl] = '\0';
            } else if (strcmp(dval, "uuid()") == 0) {
                tf->default_kind = DK_UUID;
            } else if (strncmp(dval, "random(", 7) == 0 && strchr(dval, ')')) {
                tf->default_kind = DK_RANDOM;
                const char *ns = dval + 7;
                const char *ne = strchr(ns, ')');
                size_t nl = ne - ns;
                if (nl >= sizeof(tf->default_val)) nl = sizeof(tf->default_val) - 1;
                memcpy(tf->default_val, ns, nl);
                tf->default_val[nl] = '\0';
            } else {
                tf->default_kind = DK_LITERAL;
                strncpy(tf->default_val, dval, sizeof(tf->default_val) - 1);
                tf->default_val[sizeof(tf->default_val) - 1] = '\0';
            }
            *dflt = '\0';
        }

        parse_field_type(type_spec, tf);
        if (tf->type == FT_NONE || tf->size <= 0) continue;

        tf->offset = offset;
        offset += tf->size;
        ts.nfields++;
        ts.typed = 1;
    }
    fclose(f);
    ts.total_size = offset;

    /* Store in cache */
    pthread_mutex_lock(&g_typed_lock);
    uint32_t sidx = str_hash(cache_key) % TYPED_BUCKETS;
    for (int i = 0; i < TYPED_BUCKETS; i++) {
        int slot = (sidx + i) % TYPED_BUCKETS;
        if (!g_typed_cache[slot].used) {
            strncpy(g_typed_cache[slot].name, cache_key, 511);
            g_typed_cache[slot].schema = ts;
            g_typed_cache[slot].used = 1;
            break;
        }
    }
    pthread_mutex_unlock(&g_typed_lock);

    /* Return pointer to cached entry */
    idx = str_hash(cache_key) % TYPED_BUCKETS;
    for (int i = 0; i < TYPED_BUCKETS; i++) {
        int slot = (idx + i) % TYPED_BUCKETS;
        if (g_typed_cache[slot].used && strcmp(g_typed_cache[slot].name, cache_key) == 0)
            return g_typed_cache[slot].schema.typed ? &g_typed_cache[slot].schema : NULL;
    }
    return NULL;
}

/* Find field index by name. Tombstoned fields are invisible to lookups. */
int typed_field_index(const TypedSchema *ts, const char *name) {
    if (!ts) return -1;
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed) continue;
        if (strcmp(ts->fields[i].name, name) == 0) return i;
    }
    return -1;
}

/* ---- Encode: JSON → binary ---- */

void encode_field(const TypedField *f, const char *val, uint8_t *out) {
    if (!val || !val[0]) {
        memset(out, 0, f->size);
        return;
    }
    switch (f->type) {
    case FT_VARCHAR: {
        /* Layout: [uint16 BE length][content]. Content max = f->size - 2. */
        int content_max = f->size - 2;
        size_t vlen = strlen(val);
        if ((int)vlen > content_max) vlen = content_max;
        out[0] = (uint8_t)((vlen >> 8) & 0xFF);
        out[1] = (uint8_t)(vlen & 0xFF);
        memcpy(out + 2, val, vlen);
        /* Tail bytes are untouched (already zeroed by typed_encode init). */
        break;
    }
    case FT_LONG: {
        int64_t v = strtoll(val, NULL, 10);
        out[0] = (v >> 56) & 0xFF; out[1] = (v >> 48) & 0xFF;
        out[2] = (v >> 40) & 0xFF; out[3] = (v >> 32) & 0xFF;
        out[4] = (v >> 24) & 0xFF; out[5] = (v >> 16) & 0xFF;
        out[6] = (v >> 8) & 0xFF;  out[7] = v & 0xFF;
        break;
    }
    case FT_INT: {
        int32_t v = (int32_t)atoi(val);
        out[0] = (v >> 24) & 0xFF; out[1] = (v >> 16) & 0xFF;
        out[2] = (v >> 8) & 0xFF;  out[3] = v & 0xFF;
        break;
    }
    case FT_SHORT: {
        int16_t v = (int16_t)atoi(val);
        out[0] = (v >> 8) & 0xFF; out[1] = v & 0xFF;
        break;
    }
    case FT_DOUBLE: {
        double v = atof(val);
        memcpy(out, &v, 8);
        break;
    }
    case FT_BOOL: {
        out[0] = (val[0] == 't' || val[0] == 'T' || val[0] == '1') ? 1 : 0;
        break;
    }
    case FT_BYTE: {
        out[0] = (uint8_t)atoi(val);
        break;
    }
    case FT_DATE: {
        /* Accept yyyyMMdd or yyyy-MM-dd → strip non-digits, store as int32 BE */
        char clean[16]; int ci = 0;
        for (const char *c = val; *c && ci < 8; c++)
            if (*c >= '0' && *c <= '9') clean[ci++] = *c;
        clean[ci] = '\0';
        int32_t v = (int32_t)atoi(clean);
        out[0] = (v >> 24) & 0xFF; out[1] = (v >> 16) & 0xFF;
        out[2] = (v >> 8) & 0xFF;  out[3] = v & 0xFF;
        break;
    }
    case FT_DATETIME: {
        /* Accept yyyyMMddHHmmss or yyyy-MM-dd HH:mm:ss → strip non-digits
           Store as 6 bytes: yyyyMMdd (4 bytes BE) + HHmmss (2 bytes packed) */
        char clean[16]; int ci = 0;
        for (const char *c = val; *c && ci < 14; c++)
            if (*c >= '0' && *c <= '9') clean[ci++] = *c;
        while (ci < 14) clean[ci++] = '0'; /* pad missing time with zeros */
        clean[14] = '\0';
        /* First 8 digits = yyyyMMdd as int32 */
        char datebuf[9]; memcpy(datebuf, clean, 8); datebuf[8] = '\0';
        int32_t d = (int32_t)atoi(datebuf);
        out[0] = (d >> 24) & 0xFF; out[1] = (d >> 16) & 0xFF;
        out[2] = (d >> 8) & 0xFF;  out[3] = d & 0xFF;
        /* Last 6 digits = HHmmss packed into 2 bytes: HH*3600 + MM*60 + SS (max 86399, fits uint16) */
        int hh = (clean[8]-'0')*10 + (clean[9]-'0');
        int mm = (clean[10]-'0')*10 + (clean[11]-'0');
        int ss = (clean[12]-'0')*10 + (clean[13]-'0');
        uint16_t t = (uint16_t)(hh * 3600 + mm * 60 + ss);
        out[4] = (t >> 8) & 0xFF; out[5] = t & 0xFF;
        break;
    }
    case FT_NUMERIC: {
        /* Parse decimal string, multiply by 10^S, store as int64 BE */
        double dv = atof(val);
        int64_t scale = 1;
        for (int s = 0; s < f->numeric_scale; s++) scale *= 10;
        int64_t v = (int64_t)(dv * scale + (dv >= 0 ? 0.5 : -0.5)); /* round */
        out[0] = (v >> 56) & 0xFF; out[1] = (v >> 48) & 0xFF;
        out[2] = (v >> 40) & 0xFF; out[3] = (v >> 32) & 0xFF;
        out[4] = (v >> 24) & 0xFF; out[5] = (v >> 16) & 0xFF;
        out[6] = (v >> 8) & 0xFF;  out[7] = v & 0xFF;
        break;
    }
    default:
        memset(out, 0, f->size);
        break;
    }
}

int typed_encode(const TypedSchema *ts, const char *json, uint8_t *out, int out_size) {
    if (!ts || !ts->typed || out_size < ts->total_size) return -1;
    memset(out, 0, ts->total_size);

    /* Extract all field values from JSON in one pass.
       Tombstoned fields are skipped — their on-disk bytes stay at 0. */
    const char *keys[MAX_FIELDS];
    char *vals[MAX_FIELDS];
    for (int i = 0; i < ts->nfields; i++) keys[i] = ts->fields[i].name;
    json_get_fields(json, keys, ts->nfields, vals);

    for (int i = 0; i < ts->nfields; i++) {
        if (!ts->fields[i].removed)
            encode_field(&ts->fields[i], vals[i], out + ts->fields[i].offset);
        free(vals[i]);
    }
    return ts->total_size;
}

/* ---- Default value generators (no side-effects, no OUT()) ---- */

/* Current datetime as "yyyyMMddHHmmss" into buf (>= 15 bytes) */
static void gen_datetime_now(char *buf, size_t bufsz) {
    time_t now = time(NULL);
    struct tm tm;
    localtime_r(&now, &tm);
    snprintf(buf, bufsz, "%04d%02d%02d%02d%02d%02d",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec);
}

/* Current date as "yyyyMMdd" into buf (>= 9 bytes) */
static void gen_date_now(char *buf, size_t bufsz) {
    time_t now = time(NULL);
    struct tm tm;
    localtime_r(&now, &tm);
    snprintf(buf, bufsz, "%04d%02d%02d",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
}

/* UUID v4: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx (36 chars + NUL) */
static void gen_uuid4(char *buf, size_t bufsz) {
    uint8_t b[16];
    FILE *f = fopen("/dev/urandom", "r");
    if (!f || fread(b, 1, 16, f) != 16) { buf[0] = '\0'; if (f) fclose(f); return; }
    fclose(f);
    b[6] = (b[6] & 0x0F) | 0x40; /* version 4 */
    b[8] = (b[8] & 0x3F) | 0x80; /* variant 1 */
    snprintf(buf, bufsz,
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        b[0],b[1],b[2],b[3], b[4],b[5], b[6],b[7],
        b[8],b[9], b[10],b[11],b[12],b[13],b[14],b[15]);
}

/* Random N bytes, hex-encoded (2*N chars + NUL) */
static void gen_random_hex(int nbytes, char *buf, size_t bufsz) {
    if (nbytes <= 0 || (size_t)(nbytes * 2 + 1) > bufsz) { buf[0] = '\0'; return; }
    uint8_t raw[256];
    if (nbytes > (int)sizeof(raw)) nbytes = (int)sizeof(raw);
    FILE *f = fopen("/dev/urandom", "r");
    if (!f || (int)fread(raw, 1, nbytes, f) != nbytes) { buf[0] = '\0'; if (f) fclose(f); return; }
    fclose(f);
    for (int i = 0; i < nbytes; i++)
        snprintf(buf + i * 2, 3, "%02x", raw[i]);
}

/* Internal sequence next — returns next value without printing.
   Requires db_root + object to locate the sequence file. */
static long long seq_next_val(const char *db_root, const char *object, const char *seq_name) {
    char seq_dir[PATH_MAX], seq_path[PATH_MAX], lock_path[PATH_MAX];
    snprintf(seq_dir, sizeof(seq_dir), "%s/%s/metadata/sequences", db_root, object);
    mkdirp(seq_dir);
    snprintf(seq_path, sizeof(seq_path), "%s/%s", seq_dir, seq_name);
    snprintf(lock_path, sizeof(lock_path), "%s/%s.lock", seq_dir, seq_name);

    int lockfd = open(lock_path, O_RDWR | O_CREAT, 0644);
    if (lockfd < 0) return -1;
    flock(lockfd, LOCK_EX);

    long long val = 0;
    FILE *f = fopen(seq_path, "r");
    if (f) { fscanf(f, "%lld", &val); fclose(f); }
    val++;
    f = fopen(seq_path, "w");
    if (f) { fprintf(f, "%lld\n", val); fclose(f); }

    flock(lockfd, LOCK_UN);
    close(lockfd);
    return val;
}

/* Generate a default value string for a field. Returns static/malloc'd string
   in gen_buf (caller-provided, >= 256 bytes). Returns NULL if no default. */
static const char *generate_default(const TypedField *tf, char *gen_buf, size_t bufsz,
                                    const char *db_root, const char *object) {
    switch (tf->default_kind) {
    case DK_NONE:
        return NULL;
    case DK_LITERAL:
        return tf->default_val;
    case DK_AUTO_CREATE:
    case DK_AUTO_UPDATE:
        if (tf->type == FT_DATETIME)
            gen_datetime_now(gen_buf, bufsz);
        else if (tf->type == FT_DATE)
            gen_date_now(gen_buf, bufsz);
        else
            gen_datetime_now(gen_buf, bufsz); /* fallback for varchar etc. */
        return gen_buf;
    case DK_SEQ:
        if (!db_root || !object) return NULL;
        { long long v = seq_next_val(db_root, object, tf->default_val);
          if (v < 0) return NULL;
          snprintf(gen_buf, bufsz, "%lld", v);
          return gen_buf; }
    case DK_UUID:
        gen_uuid4(gen_buf, bufsz);
        return gen_buf[0] ? gen_buf : NULL;
    case DK_RANDOM:
        { int n = atoi(tf->default_val);
          gen_random_hex(n, gen_buf, bufsz);
          return gen_buf[0] ? gen_buf : NULL; }
    }
    return NULL;
}

/* Encode with field defaults applied. db_root+object needed for seq() defaults;
   pass NULL if sequence defaults are not expected. */
int typed_encode_defaults(const TypedSchema *ts, const char *json, uint8_t *out,
                          int out_size, const char *db_root, const char *object) {
    if (!ts || !ts->typed || out_size < ts->total_size) return -1;
    memset(out, 0, ts->total_size);

    const char *keys[MAX_FIELDS];
    char *vals[MAX_FIELDS];
    for (int i = 0; i < ts->nfields; i++) keys[i] = ts->fields[i].name;
    json_get_fields(json, keys, ts->nfields, vals);

    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed) { free(vals[i]); continue; }

        if (vals[i] && vals[i][0]) {
            /* Client provided a value — use it (overrides any default) */
            encode_field(&ts->fields[i], vals[i], out + ts->fields[i].offset);
        } else if (ts->fields[i].default_kind != DK_NONE) {
            /* No client value — apply default */
            char gen_buf[256];
            const char *dv = generate_default(&ts->fields[i], gen_buf, sizeof(gen_buf),
                                              db_root, object);
            if (dv) encode_field(&ts->fields[i], dv, out + ts->fields[i].offset);
        }
        /* else: no value, no default → stays zeroed */
        free(vals[i]);
    }
    return ts->total_size;
}

/* ---- Decode: binary → JSON ---- */

static int decode_field_to_buf(const TypedField *f, const uint8_t *data, char *buf, int buflen) {
    switch (f->type) {
    case FT_VARCHAR: {
        /* Layout: [uint16 BE length][content]. */
        int len = ((int)data[0] << 8) | (int)data[1];
        int content_max = f->size - 2;
        if (len > content_max) len = content_max;  /* defensive */
        if (len == 0) return 0;
        return snprintf(buf, buflen, "\"%.*s\"", len, (const char *)(data + 2));
    }
    case FT_LONG: {
        int64_t v = ((int64_t)data[0] << 56) | ((int64_t)data[1] << 48) |
                    ((int64_t)data[2] << 40) | ((int64_t)data[3] << 32) |
                    ((int64_t)data[4] << 24) | ((int64_t)data[5] << 16) |
                    ((int64_t)data[6] << 8) | data[7];
        if (v == 0) return 0; /* skip zero */
        return snprintf(buf, buflen, "%lld", (long long)v);
    }
    case FT_INT: {
        int32_t v = ((int32_t)data[0] << 24) | ((int32_t)data[1] << 16) |
                    ((int32_t)data[2] << 8) | data[3];
        return snprintf(buf, buflen, "%d", v);
    }
    case FT_SHORT: {
        int16_t v = ((int16_t)data[0] << 8) | data[1];
        return snprintf(buf, buflen, "%d", v);
    }
    case FT_DOUBLE: {
        double v;
        memcpy(&v, data, 8);
        if (v == 0.0) return 0;
        return snprintf(buf, buflen, "%g", v);
    }
    case FT_BOOL: {
        return snprintf(buf, buflen, "%s", data[0] ? "true" : "false");
    }
    case FT_BYTE: {
        return snprintf(buf, buflen, "%u", data[0]);
    }
    case FT_NUMERIC: {
        int64_t v = ((int64_t)data[0] << 56) | ((int64_t)data[1] << 48) |
                    ((int64_t)data[2] << 40) | ((int64_t)data[3] << 32) |
                    ((int64_t)data[4] << 24) | ((int64_t)data[5] << 16) |
                    ((int64_t)data[6] << 8) | data[7];
        if (v == 0) return snprintf(buf, buflen, "0");
        int64_t scale = 1;
        for (int s = 0; s < f->numeric_scale; s++) scale *= 10;
        int64_t whole = v / scale;
        int64_t frac = v % scale;
        int neg = (v < 0);
        if (frac < 0) frac = -frac;
        if (whole < 0) whole = -whole;
        if (frac == 0)
            return snprintf(buf, buflen, "%s%lld", neg ? "-" : "", (long long)whole);
        return snprintf(buf, buflen, "%s%lld.%0*lld", neg ? "-" : "",
                        (long long)whole, f->numeric_scale, (long long)frac);
    }
    case FT_DATE: {
        int32_t v = ((int32_t)data[0] << 24) | ((int32_t)data[1] << 16) |
                    ((int32_t)data[2] << 8) | data[3];
        if (v == 0) return 0;
        return snprintf(buf, buflen, "\"%08d\"", v); /* "yyyyMMdd" */
    }
    case FT_DATETIME: {
        int32_t d = ((int32_t)data[0] << 24) | ((int32_t)data[1] << 16) |
                    ((int32_t)data[2] << 8) | data[3];
        uint16_t t = ((uint16_t)data[4] << 8) | data[5];
        if (d == 0 && t == 0) return 0;
        int hh = t / 3600;
        int mm = (t % 3600) / 60;
        int ss = t % 60;
        return snprintf(buf, buflen, "\"%08d%02d%02d%02d\"", d, hh, mm, ss); /* "yyyyMMddHHmmss" */
    }
    default:
        return 0;
    }
}

char *typed_decode(const TypedSchema *ts, const uint8_t *data, int data_len) {
    if (!ts || !ts->typed) return NULL;
    /* Estimate output: {"field":"value",...} */
    size_t est = ts->nfields * 300 + 16;
    char *buf = malloc(est);
    size_t pos = 0;
    buf[pos++] = '{';
    int first = 1;

    for (int i = 0; i < ts->nfields; i++) {
        const TypedField *f = &ts->fields[i];
        if (f->removed) continue;  /* tombstoned — not visible to consumers */
        if (f->offset + f->size > data_len) break;

        char vbuf[512];
        int vlen = decode_field_to_buf(f, data + f->offset, vbuf, sizeof(vbuf));
        if (vlen <= 0) continue; /* skip empty/zero fields */

        if (!first) buf[pos++] = ',';
        pos += snprintf(buf + pos, est - pos, "\"%s\":%s", f->name, vbuf);
        first = 0;
    }
    buf[pos++] = '}';
    buf[pos] = '\0';
    return buf;
}

/* Extract a single field as string (for B+ tree keys, query matching) */
char *typed_get_field_str(const TypedSchema *ts, const uint8_t *data, int field_idx) {
    if (!ts || field_idx < 0 || field_idx >= ts->nfields) return NULL;
    const TypedField *f = &ts->fields[field_idx];
    if (f->removed) return NULL;
    char buf[512];
    int len;

    switch (f->type) {
    case FT_VARCHAR: {
        const uint8_t *p = data + f->offset;
        int slen = ((int)p[0] << 8) | (int)p[1];
        int content_max = f->size - 2;
        if (slen > content_max) slen = content_max;
        if (slen == 0) return NULL;
        char *out = malloc(slen + 1);
        memcpy(out, p + 2, slen);
        out[slen] = '\0';
        return out;
    }
    case FT_BOOL:
        return strdup(data[f->offset] ? "true" : "false");
    case FT_DATE: {
        const uint8_t *d = data + f->offset;
        int32_t v = ((int32_t)d[0] << 24) | ((int32_t)d[1] << 16) |
                    ((int32_t)d[2] << 8) | d[3];
        if (v == 0) return NULL;
        char *out = malloc(9);
        snprintf(out, 9, "%08d", v);
        return out;
    }
    case FT_DATETIME: {
        const uint8_t *d = data + f->offset;
        int32_t dv = ((int32_t)d[0] << 24) | ((int32_t)d[1] << 16) |
                     ((int32_t)d[2] << 8) | d[3];
        uint16_t t = ((uint16_t)d[4] << 8) | d[5];
        if (dv == 0 && t == 0) return NULL;
        int hh = t / 3600, mm = (t % 3600) / 60, ss = t % 60;
        char *out = malloc(15);
        snprintf(out, 15, "%08d%02d%02d%02d", dv, hh, mm, ss);
        return out;
    }
    default:
        len = decode_field_to_buf(f, data + f->offset, buf, sizeof(buf));
        if (len <= 0) return NULL;
        return strdup(buf);
    }
}

/* ========== Cache invalidation ========== */

/* Drop schema/fields/typed cache entries for one object. Call after any
   schema change (rename-field, remove-field, add-field, vacuum-rebuild).
   g_idx_cache has its own invalidator (invalidate_idx_cache).
   Caller must hold the per-object write lock (objlock_wrlock). */
void invalidate_schema_caches(const char *db_root, const char *object) {
    char key[512];
    snprintf(key, sizeof(key), "%s:%s", db_root, object);

    /* schema cache */
    pthread_mutex_lock(&g_schema_lock);
    for (int i = 0; i < SCHEMA_BUCKETS; i++) {
        if (g_schema_cache[i].used && strcmp(g_schema_cache[i].name, key) == 0) {
            g_schema_cache[i].used = 0;
            g_schema_cache[i].name[0] = '\0';
        }
    }
    pthread_mutex_unlock(&g_schema_lock);

    /* fields cache */
    pthread_mutex_lock(&g_fields_lock);
    for (int i = 0; i < FIELDS_BUCKETS; i++) {
        if (g_fields_cache[i].used && strcmp(g_fields_cache[i].name, key) == 0) {
            g_fields_cache[i].used = 0;
            g_fields_cache[i].name[0] = '\0';
        }
    }
    pthread_mutex_unlock(&g_fields_lock);

    /* typed cache */
    pthread_mutex_lock(&g_typed_lock);
    for (int i = 0; i < TYPED_BUCKETS; i++) {
        if (g_typed_cache[i].used && strcmp(g_typed_cache[i].name, key) == 0) {
            g_typed_cache[i].used = 0;
            g_typed_cache[i].name[0] = '\0';
        }
    }
    pthread_mutex_unlock(&g_typed_lock);
}

/* ========== rename-field ==========
   Metadata-only op. Renames a field in fields.conf, renames any index
   files whose name contains the old field as a token (including composite
   "a+b+c.idx"), updates index.conf. Slot layout is unchanged — the field's
   offset/size/type are preserved.

   Caller must hold objlock_wrlock on the object. */

static int token_matches(const char *tok, size_t toklen, const char *target) {
    size_t tlen = strlen(target);
    return toklen == tlen && memcmp(tok, target, tlen) == 0;
}

/* Rewrite an index name/conf line by replacing old tokens with new. Tokens
   are split on '+'. Writes up to outcap bytes. Returns 1 if any replacement
   happened, 0 if the name was unchanged. */
static int replace_tokens(const char *in, const char *old_name, const char *new_name,
                          char *out, size_t outcap) {
    int changed = 0;
    size_t pos = 0;
    const char *p = in;
    while (*p) {
        const char *sep = strchr(p, '+');
        size_t toklen = sep ? (size_t)(sep - p) : strlen(p);
        const char *src; size_t srclen;
        if (token_matches(p, toklen, old_name)) {
            src = new_name; srclen = strlen(new_name); changed = 1;
        } else {
            src = p; srclen = toklen;
        }
        if (pos + srclen + 2 >= outcap) return -1; /* overflow */
        memcpy(out + pos, src, srclen); pos += srclen;
        if (sep) { out[pos++] = '+'; p = sep + 1; }
        else break;
    }
    out[pos] = '\0';
    return changed;
}

static int rename_indexes_for_field(const char *db_root, const char *object,
                                    const char *old_name, const char *new_name) {
    char idx_dir[PATH_MAX];
    snprintf(idx_dir, sizeof(idx_dir), "%s/%s/indexes", db_root, object);

    DIR *d = opendir(idx_dir);
    if (!d) return 0;

    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        size_t nlen = strlen(e->d_name);
        if (nlen < 5 || strcmp(e->d_name + nlen - 4, ".idx") != 0) continue;

        /* Strip .idx, replace tokens, re-append .idx */
        char base[512];
        if (nlen - 4 >= sizeof(base)) continue;
        memcpy(base, e->d_name, nlen - 4);
        base[nlen - 4] = '\0';

        char newbase[512];
        int chg = replace_tokens(base, old_name, new_name, newbase, sizeof(newbase));
        if (chg != 1) continue;

        char oldpath[PATH_MAX], newpath[PATH_MAX];
        snprintf(oldpath, sizeof(oldpath), "%s/%s", idx_dir, e->d_name);
        snprintf(newpath, sizeof(newpath), "%s/%s.idx", idx_dir, newbase);
        if (rename(oldpath, newpath) != 0) {
            log_msg(1, "rename-field: failed to rename %s -> %s: %s",
                    oldpath, newpath, strerror(errno));
        }
    }
    closedir(d);

    /* Rewrite index.conf: token-replace each line */
    char conf_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);
    FILE *f = fopen(conf_path, "r");
    if (!f) return 0;

    char newconf_path[PATH_MAX];
    snprintf(newconf_path, sizeof(newconf_path), "%s.new", conf_path);
    FILE *nf = fopen(newconf_path, "w");
    if (!nf) { fclose(f); return -1; }

    char line[512], newline[512];
    while (fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n")] = '\0';
        if (line[0] == '\0' || line[0] == '#') { fprintf(nf, "%s\n", line); continue; }
        int chg = replace_tokens(line, old_name, new_name, newline, sizeof(newline));
        fprintf(nf, "%s\n", chg == 1 ? newline : line);
    }
    fclose(f);
    fclose(nf);
    rename(newconf_path, conf_path);  /* atomic swap */
    return 0;
}

/* Validate a field name: non-empty, no colon/plus/slash/newline, reasonable length. */
static int valid_field_name(const char *name) {
    if (!name || !name[0]) return 0;
    size_t n = strlen(name);
    if (n >= 128) return 0;
    for (size_t i = 0; i < n; i++) {
        char c = name[i];
        if (c == ':' || c == '+' || c == '/' || c == '\n' || c == '\r' || c == ' ')
            return 0;
    }
    return 1;
}

/* ========== add-field ==========
   Appends one or more new fields to the end of fields.conf and triggers
   a full shard rebuild to enlarge slot_size. Existing records get zero
   bytes in the new fields (decoders render that as absent).

   Caller must hold objlock_wrlock on the object. */

int cmd_add_fields(const char *db_root, const char *object,
                   char lines[][256], int nlines) {
    if (nlines <= 0) {
        OUT("{\"error\":\"No fields specified\"}\n");
        return 1;
    }

    /* Pre-validate each spec: name must be legal, type must parse, no colon-removed
       marker (you can't add an already-removed field). Also reject adding a name
       that already exists (active or tombstoned). */
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"fields.conf missing for [%s]\"}\n", object);
        return 1;
    }

    for (int a = 0; a < nlines; a++) {
        char *colon = strchr(lines[a], ':');
        if (!colon || colon == lines[a]) {
            OUT("{\"error\":\"Invalid field line: %s\"}\n", lines[a]);
            return 1;
        }
        size_t nlen = colon - lines[a];
        if (nlen >= 128) { OUT("{\"error\":\"Field name too long\"}\n"); return 1; }
        char name[128];
        memcpy(name, lines[a], nlen); name[nlen] = '\0';
        if (!valid_field_name(name)) {
            OUT("{\"error\":\"Invalid field name: %s\"}\n", name);
            return 1;
        }
        if (strstr(lines[a], ":removed")) {
            OUT("{\"error\":\"Cannot add field with ':removed' marker\"}\n");
            return 1;
        }
        /* Duplicate-name check against existing fields (including tombstoned) */
        for (int i = 0; i < ts->nfields; i++) {
            if (strcmp(ts->fields[i].name, name) == 0) {
                OUT("{\"error\":\"Field [%s] already exists\"}\n", name);
                return 1;
            }
        }
        /* Duplicate-name check against other entries in this same call */
        for (int b = 0; b < a; b++) {
            char *c2 = strchr(lines[b], ':');
            size_t bl = c2 ? (size_t)(c2 - lines[b]) : strlen(lines[b]);
            if (bl == nlen && memcmp(lines[b], name, nlen) == 0) {
                OUT("{\"error\":\"Duplicate field [%s] in request\"}\n", name);
                return 1;
            }
        }
        /* Type parseability — use a throwaway TypedField */
        TypedField tf;
        memset(&tf, 0, sizeof(tf));
        parse_field_type(colon + 1, &tf);
        if (tf.type == FT_NONE || tf.size <= 0) {
            OUT("{\"error\":\"Invalid type in: %s\"}\n", lines[a]);
            return 1;
        }
    }

    /* rebuild_object appends lines to fields.conf and rewrites shards atomically. */
    return rebuild_object(db_root, object, 0, 0, lines, nlines);
}

/* ========== remove-field ==========
   Tombstones one or more fields by appending ":removed" to their
   fields.conf lines. Slot layout is preserved (bytes stay reserved
   until vacuum compacts them). Indexes referencing any tombstoned
   field are dropped.

   Caller must hold objlock_wrlock on the object. */

static int drop_indexes_for_fields(const char *db_root, const char *object,
                                   char removed_names[][256], int n_removed) {
    char idx_dir[PATH_MAX];
    snprintf(idx_dir, sizeof(idx_dir), "%s/%s/indexes", db_root, object);

    DIR *d = opendir(idx_dir);
    if (!d) return 0;

    int dropped = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        size_t nlen = strlen(e->d_name);
        if (nlen < 5 || strcmp(e->d_name + nlen - 4, ".idx") != 0) continue;

        /* Split basename on '+', drop if any token is in removed_names. */
        char base[512];
        if (nlen - 4 >= sizeof(base)) continue;
        memcpy(base, e->d_name, nlen - 4);
        base[nlen - 4] = '\0';

        int affected = 0;
        const char *p = base;
        while (*p) {
            const char *sep = strchr(p, '+');
            size_t toklen = sep ? (size_t)(sep - p) : strlen(p);
            for (int r = 0; r < n_removed; r++) {
                if (token_matches(p, toklen, removed_names[r])) { affected = 1; break; }
            }
            if (affected || !sep) break;
            p = sep + 1;
        }
        if (!affected) continue;

        char idx_path[PATH_MAX];
        snprintf(idx_path, sizeof(idx_path), "%s/%s", idx_dir, e->d_name);
        if (unlink(idx_path) == 0) dropped++;
    }
    closedir(d);

    /* Rewrite index.conf without lines referencing removed fields. */
    char conf_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);
    FILE *f = fopen(conf_path, "r");
    if (!f) return dropped;

    char tmp_path[PATH_MAX];
    snprintf(tmp_path, sizeof(tmp_path), "%s.new", conf_path);
    FILE *nf = fopen(tmp_path, "w");
    if (!nf) { fclose(f); return dropped; }

    char line[512];
    while (fgets(line, sizeof(line), f)) {
        char stripped[512];
        strncpy(stripped, line, sizeof(stripped) - 1);
        stripped[sizeof(stripped) - 1] = '\0';
        stripped[strcspn(stripped, "\n")] = '\0';
        if (stripped[0] == '\0' || stripped[0] == '#') { fprintf(nf, "%s", line); continue; }

        int affected = 0;
        const char *p = stripped;
        while (*p) {
            const char *sep = strchr(p, '+');
            size_t toklen = sep ? (size_t)(sep - p) : strlen(p);
            for (int r = 0; r < n_removed; r++) {
                if (token_matches(p, toklen, removed_names[r])) { affected = 1; break; }
            }
            if (affected || !sep) break;
            p = sep + 1;
        }
        if (!affected) fprintf(nf, "%s", line);
    }
    fclose(f);
    fclose(nf);
    rename(tmp_path, conf_path);
    return dropped;
}

int cmd_remove_fields(const char *db_root, const char *object,
                      char names[][256], int nnames) {
    if (nnames <= 0) {
        OUT("{\"error\":\"No fields specified\"}\n");
        return 1;
    }
    for (int i = 0; i < nnames; i++) {
        if (!valid_field_name(names[i])) {
            OUT("{\"error\":\"Invalid field name: %s\"}\n", names[i]);
            return 1;
        }
    }

    char fpath[PATH_MAX];
    snprintf(fpath, sizeof(fpath), "%s/%s/fields.conf", db_root, object);
    FILE *f = fopen(fpath, "r");
    if (!f) {
        OUT("{\"error\":\"fields.conf not found for object [%s]\"}\n", object);
        return 1;
    }

    char lines[MAX_FIELDS][512];
    int nlines = 0;
    while (fgets(lines[nlines], sizeof(lines[0]), f) && nlines < MAX_FIELDS) {
        lines[nlines][strcspn(lines[nlines], "\n")] = '\0';
        nlines++;
    }
    fclose(f);

    /* Validate each requested field exists and isn't already tombstoned */
    int found[MAX_FIELDS] = {0};
    for (int r = 0; r < nnames; r++) {
        for (int i = 0; i < nlines; i++) {
            const char *ln = lines[i];
            if (ln[0] == '\0' || ln[0] == '#') continue;
            const char *colon = strchr(ln, ':');
            size_t nlen = colon ? (size_t)(colon - ln) : strlen(ln);
            if (token_matches(ln, nlen, names[r])) {
                if (strstr(ln, ":removed")) {
                    OUT("{\"error\":\"Field [%s] is already removed\"}\n", names[r]);
                    return 1;
                }
                found[r] = 1;
                break;
            }
        }
        if (!found[r]) {
            OUT("{\"error\":\"Field [%s] not found\"}\n", names[r]);
            return 1;
        }
    }

    /* Rewrite fields.conf with :removed suffix on matching active lines */
    char tmppath[PATH_MAX];
    snprintf(tmppath, sizeof(tmppath), "%s.new", fpath);
    FILE *nf = fopen(tmppath, "w");
    if (!nf) { OUT("{\"error\":\"Cannot write fields.conf.new\"}\n"); return 1; }

    for (int i = 0; i < nlines; i++) {
        const char *ln = lines[i];
        if (ln[0] == '\0' || ln[0] == '#') { fprintf(nf, "%s\n", ln); continue; }
        const char *colon = strchr(ln, ':');
        size_t nlen = colon ? (size_t)(colon - ln) : strlen(ln);
        int tombstone_this = 0;
        for (int r = 0; r < nnames; r++) {
            if (token_matches(ln, nlen, names[r])) { tombstone_this = 1; break; }
        }
        if (tombstone_this && !strstr(ln, ":removed"))
            fprintf(nf, "%s:removed\n", ln);
        else
            fprintf(nf, "%s\n", ln);
    }
    fclose(nf);

    if (rename(tmppath, fpath) != 0) {
        unlink(tmppath);
        OUT("{\"error\":\"Failed to rename fields.conf\"}\n");
        return 1;
    }

    int dropped = drop_indexes_for_fields(db_root, object, names, nnames);

    invalidate_schema_caches(db_root, object);
    invalidate_idx_cache(object);

    log_msg(3, "REMOVE-FIELD %s/%s: %d fields tombstoned, %d indexes dropped",
            db_root, object, nnames, dropped);
    OUT("{\"status\":\"removed\",\"fields\":%d,\"indexes_dropped\":%d}\n", nnames, dropped);
    return 0;
}

int cmd_rename_field(const char *db_root, const char *object,
                     const char *old_name, const char *new_name) {
    if (!valid_field_name(old_name) || !valid_field_name(new_name)) {
        OUT("{\"error\":\"Invalid field name (no :, +, /, spaces)\"}\n");
        return 1;
    }
    if (strcmp(old_name, new_name) == 0) {
        OUT("{\"error\":\"Old and new names are identical\"}\n");
        return 1;
    }

    char fpath[PATH_MAX];
    snprintf(fpath, sizeof(fpath), "%s/%s/fields.conf", db_root, object);
    FILE *f = fopen(fpath, "r");
    if (!f) {
        OUT("{\"error\":\"fields.conf not found for object [%s]\"}\n", object);
        return 1;
    }

    /* Read all lines; track whether old_name is found and whether new_name
       already exists (as an active, non-tombstoned field). */
    char lines[MAX_FIELDS][512];
    int nlines = 0;
    int found_old = 0;
    int new_conflict = 0;
    while (fgets(lines[nlines], sizeof(lines[0]), f) && nlines < MAX_FIELDS) {
        lines[nlines][strcspn(lines[nlines], "\n")] = '\0';
        const char *ln = lines[nlines];
        if (ln[0] && ln[0] != '#') {
            const char *colon = strchr(ln, ':');
            size_t nlen = colon ? (size_t)(colon - ln) : strlen(ln);
            if (token_matches(ln, nlen, old_name)) found_old = 1;
            if (token_matches(ln, nlen, new_name)) {
                /* tombstoned entry ending with :removed is not a conflict */
                if (!strstr(ln, ":removed")) new_conflict = 1;
            }
        }
        nlines++;
    }
    fclose(f);

    if (!found_old) {
        OUT("{\"error\":\"Field [%s] not found in fields.conf\"}\n", old_name);
        return 1;
    }
    if (new_conflict) {
        OUT("{\"error\":\"Field [%s] already exists\"}\n", new_name);
        return 1;
    }

    /* Write fields.conf.new with the old token replaced */
    char tmppath[PATH_MAX];
    snprintf(tmppath, sizeof(tmppath), "%s.new", fpath);
    FILE *nf = fopen(tmppath, "w");
    if (!nf) { OUT("{\"error\":\"Cannot write fields.conf.new\"}\n"); return 1; }

    for (int i = 0; i < nlines; i++) {
        const char *ln = lines[i];
        if (ln[0] == '\0' || ln[0] == '#') { fprintf(nf, "%s\n", ln); continue; }
        const char *colon = strchr(ln, ':');
        size_t nlen = colon ? (size_t)(colon - ln) : strlen(ln);
        if (token_matches(ln, nlen, old_name))
            fprintf(nf, "%s%s\n", new_name, colon ? colon : "");
        else
            fprintf(nf, "%s\n", ln);
    }
    fclose(nf);

    /* Atomic rename: fields.conf.new → fields.conf */
    if (rename(tmppath, fpath) != 0) {
        unlink(tmppath);
        OUT("{\"error\":\"Failed to rename fields.conf\"}\n");
        return 1;
    }

    /* Rename index files + update index.conf */
    rename_indexes_for_field(db_root, object, old_name, new_name);

    /* Invalidate all caches (including idx cache) */
    invalidate_schema_caches(db_root, object);
    invalidate_idx_cache(object);

    log_msg(3, "RENAME-FIELD %s/%s: %s -> %s", db_root, object, old_name, new_name);
    OUT("{\"status\":\"renamed\",\"old\":\"%s\",\"new\":\"%s\"}\n", old_name, new_name);
    return 0;
}

