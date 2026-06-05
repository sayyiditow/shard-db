#include "types.h"

/* ========== Config ========== */

uint32_t g_timeout = 30;
int g_port = 9199;
int g_workers = 0;      /* 0 = auto (nproc, min 4) — server thread pool */
/* g_max_threads (+ parallel_threads getter) and g_pool_chunk live in
   parallel.c so test/bench binaries that link parallel.c without
   config.c don't get an undefined symbol at link time. config.c still
   writes g_max_threads when parsing db.env (extern).

   I/O thread pool size (IO_THREADS in db.env). 0 = auto (2 × nproc).
   Defined here so config.c's parse loop can set it; declared extern in
   types.h. parallel.c reads it at init time. */
int g_io_threads = 0;
int g_index_page_size = 4096;
int g_global_limit = 100000;
int g_max_request_size = 33554432; /* 32 MB default, configurable via MAX_REQUEST_SIZE */
/* MAX_CONCURRENT_QUERIES — hard cap on simultaneously-running queries.
   When the cap is reached, additional requests get an immediate
   {"error":"server at capacity"} response so clients can retry. Bounded
   concurrency means worst-case query RAM = max_concurrent × QUERY_BUFFER_MB
   is predictable, which is the prerequisite for sane sizing on multi-
   tenant deployments. 0 = auto, resolved at cmd_server startup to
   max(4, min(nproc, 32)). */
int g_max_concurrent_queries = 0;
int g_fcache_cap = 4096;        /* shard mmap cache capacity, configurable via FCACHE_MAX
                                   to one of {4096, 8192, 12288, 16384} */
int g_btcache_cap = 1024;       /* B+ tree mmap cache capacity = g_fcache_cap / 4
                                   (derived, not separately configurable) */
/* 256 MB default — conservative cap that fits comfortably even with
   MAX_CONCURRENT_QUERIES at its auto ceiling (32). cmd_server detects
   the unchanged-default case and auto-tunes upward IF the per-slot
   share of (25% of RAM, 4 GB cap) is bigger — i.e. when slot count
   is low and host RAM is generous. Worst-case RAM for query buffers
   stays bounded by max_concurrent × QUERY_BUFFER_MB regardless. */
size_t g_query_buffer_max_bytes = 256ULL * 1024 * 1024;
/* 1 GiB default keeps reindex memory bounded on modest VPS shapes. Big
   prod servers with lots of indexed fields can raise this to fit more
   fields per scan pass (INDEX_BUILD_BUDGET_MB in db.env). */
size_t g_index_build_budget_bytes = 1024ULL * 1024 * 1024;
int g_disable_localhost_trust = 0; /* default: 127.0.0.1/::1 bypass auth. Set via DISABLE_LOCALHOST_TRUST=1 for strict mode. */
int g_token_cap = 1024;            /* token table bucket count, configurable via TOKEN_CAP (floor 64, ceiling 1M) */
_Thread_local uint32_t g_request_timeout_ms = 0;  /* per-request override; 0 = use g_timeout */

/* Native TLS — defaults: disabled. db.env opts in via TLS_ENABLE=1.
   When enabled, both the daemon and the CLI speak TLS 1.3 only. */
int g_tls_enable = 0;
int g_tls_skip_verify = 0;
char g_tls_cert[PATH_MAX] = {0};
char g_tls_key[PATH_MAX] = {0};
char g_tls_ca[PATH_MAX] = {0};

/* Monitoring counters */
uint64_t g_ucache_hits = 0;
uint64_t g_ucache_misses = 0;
uint64_t g_bt_cache_hits = 0;
uint64_t g_bt_cache_misses = 0;
uint64_t g_server_start_ms = 0;
uint64_t g_slow_query_count = 0;
int g_slow_query_ms = 500;  /* configurable via SLOW_QUERY_MS */
int g_random_seq_ratio = 8;  /* configurable via RANDOM_SEQ_COST_RATIO */

/* vacuum-check thresholds — drive the "vacuum":true recommendation in
   `vacuum-check` AND the auto-vacuum thread's selection logic (one source
   of truth). Defaults match the historic hardcoded values. */
int g_vacuum_recommend_pct = 10;          /* tombstones / total ≥ N% */
int g_vacuum_recommend_min_deleted = 1000;/* AND deleted ≥ N */

/* Auto-vacuum thread — opt-in. When AUTO_VACUUM=1, a background thread
   wakes every g_auto_vacuum_interval_sec, calls vacuum-check's selection
   pass, and runs plain vacuum on candidates. NEVER auto-runs --compact
   or --splits (both are heavy + need exclusive objlock). */
int g_auto_vacuum_enable = 0;
int g_auto_vacuum_interval_sec = 3600;

/* Startup warmup — pulls every kf header + index shard file (.idx/.bm/.tg)
   into the OS page cache on daemon start so the first user query doesn't
   pay a cold disk read.
   Modes:
     async (default) — daemon accepts connections immediately, warmup runs
                       in a detached background thread. First queries that
                       race ahead of the warmup still pay cold cost for
                       whatever the thread hasn't reached yet, but the
                       wall-clock time to fully-warm drops drastically
                       because the warmup runs in parallel with serving.
     sync            — block startup until warmup completes. Restart blocks
                       the listening socket for the full warmup time;
                       useful for "warm before serving" production posture.
     off             — no warmup. Lazy cold reads as before.
   Stream segment files are intentionally NOT touched — they can be GBs
   on populated objects and warming them all evicts the small/medium index
   files that drive every find. Segs get warmed naturally by first queries. */
char g_warmup_mode[16] = "async";
SlowQueryEntry g_slow_queries[SLOW_QUERY_RING] = {0};
int g_slow_query_head = 0;
pthread_mutex_t g_slow_query_lock = PTHREAD_MUTEX_INITIALIZER;

uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/* Coarse clock: ~1-4ms granularity, essentially free (vDSO, no syscall)
   on Linux via CLOCK_MONOTONIC_COARSE. macOS has no equivalent — it
   falls back to plain CLOCK_MONOTONIC which is still cheap (~50 ns).
   Either way, ms precision is fine for the hot-loop timeout checks
   that call this. */
uint64_t now_ms_coarse(void) {
    struct timespec ts;
#ifdef CLOCK_MONOTONIC_COARSE
    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
#else
    clock_gettime(CLOCK_MONOTONIC, &ts);
#endif
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

extern char g_log_dir[PATH_MAX];

/* log_slow_query — defined after LogEntry/ring declarations below. */
__thread FILE *g_out = NULL;    /* per-thread output; init to stdout in main() */
char g_db_root[PATH_MAX] = {0};

/* Async logging — ring buffer + background flush thread (tinylog style)
   Two log files: info-YYYY-MM-DD.log and error-YYYY-MM-DD.log
   Levels: 0=off, 1=error, 2=warn, 3=info (writes), 4=debug (reads) */
int g_log_level = 3;
int g_log_retain_days = 7;

#define LOG_RING_SIZE 8192
#define LOG_MSG_MAX   4096

typedef struct {
    int  level;      /* LOG_LVL_* */
    int  is_audit;   /* 1 = route to audit file, bypass level filter */
    int  is_slow;    /* 1 = route to slow file, bypass level filter */
    char msg[LOG_MSG_MAX];
} LogEntry;

LogEntry g_log_ring[LOG_RING_SIZE];
volatile int g_log_head = 0;
volatile int g_log_tail = 0;
pthread_mutex_t g_log_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t g_log_cond = PTHREAD_COND_INITIALIZER;
pthread_t g_log_thread;
/* _Atomic so log_shutdown's set-to-0 and the writer/log_msg reads don't
   race. Previously volatile-int — benign data race, but TSan flagged it
   on the same runs where the parallel.c help-drain race tripped. Plain
   relaxed ops everywhere except the start/stop transitions where we
   want a release/acquire pair so the writer wakes promptly. */
_Atomic int g_log_running = 0;
char g_log_dir[PATH_MAX];

FILE *open_log_for_level(int level) {
    time_t now = time(NULL);
    struct tm tbuf; struct tm *t = localtime_r(&now, &tbuf);
    char date[16], path[PATH_MAX];
    strftime(date, sizeof(date), "%Y-%m-%d", t);
    /* Level 1 = ERROR → error log. Everything else (WARN, INFO, DEBUG) → info log. */
    const char *prefix = (level <= 1) ? "error" : "info";
    snprintf(path, sizeof(path), "%s/%s-%s.log", g_log_dir, date, prefix);
    return fopen(path, "a");
}

void purge_old_logs(void) {
    if (g_log_retain_days <= 0) return;
    time_t cutoff = time(NULL) - g_log_retain_days * 86400;
    DIR *d = opendir(g_log_dir);
    if (!d) return;
    int dfd = dirfd(d);
    struct dirent *e;
    while ((e = readdir(d))) {
        size_t nlen = strlen(e->d_name);
        if (nlen < 4 || strcmp(e->d_name + nlen - 4, ".log") != 0)
            continue;
        struct stat st;
        /* fstatat + unlinkat against the open dirfd: TOCTOU-safe. */
        if (fstatat(dfd, e->d_name, &st, AT_SYMLINK_NOFOLLOW) == 0 &&
            S_ISREG(st.st_mode) && st.st_mtime < cutoff)
            unlinkat(dfd, e->d_name, 0);
    }
    closedir(d);
}

void *log_writer_thread(void *arg) {
    (void)arg;
    int flush_counter = 0;
    while (atomic_load_explicit(&g_log_running, memory_order_acquire) ||
           g_log_head != g_log_tail) {
        pthread_mutex_lock(&g_log_lock);
        while (g_log_head == g_log_tail &&
               atomic_load_explicit(&g_log_running, memory_order_relaxed))
            pthread_cond_wait(&g_log_cond, &g_log_lock);

        /* Batch flush */
        FILE *info_f = NULL, *err_f = NULL, *audit_f = NULL, *slow_f = NULL;
        while (g_log_head != g_log_tail) {
            LogEntry *e = &g_log_ring[g_log_tail];
            FILE **target;
            if (e->is_audit) {
                /* Route to <date>-audit.log, bypassing level filter */
                if (!audit_f) {
                    time_t now2 = time(NULL);
                    struct tm tbuf2; struct tm *t2 = localtime_r(&now2, &tbuf2);
                    char date2[16], path2[PATH_MAX];
                    strftime(date2, sizeof(date2), "%Y-%m-%d", t2);
                    snprintf(path2, sizeof(path2), "%s/%s-audit.log", g_log_dir, date2);
                    audit_f = fopen(path2, "a");
                }
                target = &audit_f;
            } else if (e->is_slow) {
                /* Route to <date>-slow.log — matches the date-prefix
                   convention used by the info / error / audit logs.
                   (Pre-2026.05.9 used the inconsistent `slow-<date>.log`
                   form; ls in $LOG_DIR was ordered weirdly.) */
                if (!slow_f) {
                    time_t now2 = time(NULL);
                    struct tm tbuf2; struct tm *t2 = localtime_r(&now2, &tbuf2);
                    char date2[16], path2[PATH_MAX];
                    strftime(date2, sizeof(date2), "%Y-%m-%d", t2);
                    snprintf(path2, sizeof(path2), "%s/%s-slow.log", g_log_dir, date2);
                    slow_f = fopen(path2, "a");
                }
                target = &slow_f;
            } else {
                /* Normal level-based routing */
                target = (e->level <= 1) ? &err_f : &info_f;
                if (!*target) *target = open_log_for_level(e->level);
            }
            if (*target) fputs(e->msg, *target);
            g_log_tail = (g_log_tail + 1) % LOG_RING_SIZE;
        }
        pthread_mutex_unlock(&g_log_lock);

        if (info_f)  { fflush(info_f);  fclose(info_f);  }
        if (err_f)   { fflush(err_f);   fclose(err_f);   }
        if (audit_f) { fflush(audit_f); fclose(audit_f); }
        if (slow_f)  { fflush(slow_f);  fclose(slow_f);  }

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
    atomic_store_explicit(&g_log_running, 1, memory_order_release);
    db_thread_create(&g_log_thread, log_writer_thread, NULL);
    purge_old_logs();
}

void log_shutdown(void) {
    atomic_store_explicit(&g_log_running, 0, memory_order_release);
    pthread_cond_signal(&g_log_cond);
    pthread_join(g_log_thread, NULL);
}

/* pthread_create with 8 MB stack — Linux default size, matches what
   our daemon code paths assume. macOS default is 512 KB which is below
   the db-dirs handler's 512 KB `dirs_copy` stack array alone. Failing
   over to plain pthread_create on attr_init/setstacksize errors keeps
   us correct on exotic platforms; the only known case where 8 MB is
   insufficient is intentional deep recursion (we have none). */
int db_thread_create(pthread_t *tid, void *(*fn)(void *), void *arg) {
    pthread_attr_t attr;
    if (pthread_attr_init(&attr) != 0)
        return pthread_create(tid, NULL, fn, arg);
    pthread_attr_setstacksize(&attr, 8 * 1024 * 1024);
    int rc = pthread_create(tid, &attr, fn, arg);
    pthread_attr_destroy(&attr);
    return rc;
}

void log_msg_sub(int level, const char *subsystem, const char *fmt, ...) {
    if (level > g_log_level ||
        !atomic_load_explicit(&g_log_running, memory_order_relaxed)) return;
    const char *labels[] = {"", "ERROR", "WARN", "INFO", "DEBUG"};
    if (level < 1 || level > 4) level = LOG_LVL_INFO;
    const char *sub = subsystem ? subsystem : "unknown";

    time_t now = time(NULL);
    struct tm tbuf; struct tm *t = localtime_r(&now, &tbuf);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", t);

    LogEntry entry;
    entry.level    = level;
    entry.is_audit = 0;
    entry.is_slow  = 0;
    int pos = snprintf(entry.msg, sizeof(entry.msg), "%s %s [%s] ",
                       ts, labels[level], sub);
    va_list ap;
    va_start(ap, fmt);
    pos += vsnprintf(entry.msg + pos, sizeof(entry.msg) - pos, fmt, ap);
    va_end(ap);
    if (pos < (int)sizeof(entry.msg) - 1) {
        entry.msg[pos]   = '\n';
        entry.msg[pos+1] = '\0';
    }

    pthread_mutex_lock(&g_log_lock);
    int next = (g_log_head + 1) % LOG_RING_SIZE;
    if (next != g_log_tail) {
        g_log_ring[g_log_head] = entry;
        g_log_head = next;
    }
    pthread_cond_signal(&g_log_cond);
    pthread_mutex_unlock(&g_log_lock);
}


void log_audit_sub(const char *subsystem, const char *fmt, ...) {
    if (!atomic_load_explicit(&g_log_running, memory_order_relaxed)) return;
    const char *sub = subsystem ? subsystem : "unknown";

    time_t now = time(NULL);
    struct tm tbuf; struct tm *t = localtime_r(&now, &tbuf);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", t);

    LogEntry entry;
    entry.level    = LOG_LVL_INFO;   /* for the displayed line shape */
    entry.is_audit = 1;
    entry.is_slow  = 0;
    int pos = snprintf(entry.msg, sizeof(entry.msg), "%s INFO [%s] ", ts, sub);
    va_list ap;
    va_start(ap, fmt);
    pos += vsnprintf(entry.msg + pos, sizeof(entry.msg) - pos, fmt, ap);
    va_end(ap);
    if (pos < (int)sizeof(entry.msg) - 1) {
        entry.msg[pos]   = '\n';
        entry.msg[pos+1] = '\0';
    }

    pthread_mutex_lock(&g_log_lock);
    int next = (g_log_head + 1) % LOG_RING_SIZE;
    if (next != g_log_tail) {
        g_log_ring[g_log_head] = entry;
        g_log_head = next;
    }
    pthread_cond_signal(&g_log_cond);
    pthread_mutex_unlock(&g_log_lock);
}

void log_slow_query(const char *mode, const char *dir,
                    const char *object, const char *query, uint32_t duration_ms) {
    __atomic_add_fetch(&g_slow_query_count, 1, __ATOMIC_RELAXED);

    /* In-memory ring for the /stats endpoint (existing behaviour). The
       query is the full request JSON, truncated to the fixed field — enough
       to capture criteria/order_by/limit for find/count/aggregate searches
       (a huge bulk-insert payload truncates harmlessly). */
    pthread_mutex_lock(&g_slow_query_lock);
    SlowQueryEntry *e = &g_slow_queries[g_slow_query_head];
    e->ts_ms = now_ms();
    e->duration_ms = duration_ms;
    snprintf(e->mode,   sizeof(e->mode),   "%s", mode   ? mode   : "");
    snprintf(e->dir,    sizeof(e->dir),    "%s", dir    ? dir    : "");
    snprintf(e->object, sizeof(e->object), "%s", object ? object : "");
    snprintf(e->query,  sizeof(e->query),  "%s", query  ? query  : "");
    g_slow_query_head = (g_slow_query_head + 1) % SLOW_QUERY_RING;
    pthread_mutex_unlock(&g_slow_query_lock);

    /* File log via shared ring buffer (replaces the previous direct
       fopen+fprintf path).  is_slow=1 routes to slow-<date>.log via
       the drain thread; bypasses LOG_LEVEL because the SLOW_QUERY_MS
       threshold is the filter. */
    if (!atomic_load_explicit(&g_log_running, memory_order_relaxed)) return;
    time_t now = time(NULL);
    struct tm tbuf; struct tm *t = localtime_r(&now, &tbuf);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", t);

    LogEntry entry;
    entry.level    = LOG_LVL_INFO;
    entry.is_audit = 0;
    entry.is_slow  = 1;
    snprintf(entry.msg, sizeof(entry.msg),
             "%s INFO [slow] %ums mode=%s dir=%s object=%s query=%.500s\n",
             ts, duration_ms,
             mode   ? mode   : "",
             dir    ? dir    : "",
             object ? object : "",
             query  ? query  : "");

    pthread_mutex_lock(&g_log_lock);
    int next2 = (g_log_head + 1) % LOG_RING_SIZE;
    if (next2 != g_log_tail) {
        g_log_ring[g_log_head] = entry;
        g_log_head = next2;
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
        } else if (strncmp(p, "IO_THREADS=", 11) == 0) {
            g_io_threads = atoi(p + 11);
        } else if (strncmp(p, "POOL_CHUNK=", 11) == 0) {
            g_pool_chunk = atoi(p + 11);
        } else if (strncmp(p, "WORKERS=", 8) == 0) {
            g_workers = atoi(p + 8);
        } else if (strncmp(p, "GLOBAL_LIMIT=", 13) == 0) {
            g_global_limit = atoi(p + 13);
        } else if (strncmp(p, "MAX_CONCURRENT_QUERIES=", 23) == 0) {
            g_max_concurrent_queries = atoi(p + 23);
            if (g_max_concurrent_queries < 0) g_max_concurrent_queries = 0;
        } else if (strncmp(p, "MAX_REQUEST_SIZE=", 17) == 0) {
            int sz = atoi(p + 17);
            if (sz >= 1024) g_max_request_size = sz;
        } else if (strncmp(p, "FCACHE_MAX=", 11) == 0) {
            /* Strict allow-list: FCACHE_MAX ∈ {4096, 8192, 12288, 16384}
               (4096 × N for N=1..4). BT_CACHE_MAX is derived (FCACHE_MAX/4),
               so a single config value tunes both caches consistently. */
            int n = atoi(p + 11);
            if (n == 4096 || n == 8192 || n == 12288 || n == 16384) {
                g_fcache_cap = n;
            } else {
                fprintf(stderr,
                    "config: FCACHE_MAX=%d is not in {4096, 8192, 12288, 16384}; "
                    "falling back to default 4096.\n", n);
                g_fcache_cap = 4096;
            }
            g_btcache_cap = g_fcache_cap / 4;
        } else if (strncmp(p, "BT_CACHE_MAX=", 13) == 0) {
            /* Ignored as of 2026.05.1 — BT_CACHE_MAX is derived from
               FCACHE_MAX/4. Warn so users notice the change. */
            fprintf(stderr,
                "config: BT_CACHE_MAX is no longer configurable as of 2026.05.1 — "
                "it is derived as FCACHE_MAX/4. Setting ignored.\n");
        } else if (strncmp(p, "QUERY_BUFFER_MB=", 16) == 0) {
            long mb = atol(p + 16);
            if (mb >= 1 && mb <= 1048576)  /* 1 MB floor, 1 TB ceiling */
                g_query_buffer_max_bytes = (size_t)mb * 1024 * 1024;
        } else if (strncmp(p, "INDEX_BUILD_BUDGET_MB=", 22) == 0) {
            long mb = atol(p + 22);
            if (mb >= 64 && mb <= 1048576)  /* 64 MB floor, 1 TB ceiling */
                g_index_build_budget_bytes = (size_t)mb * 1024 * 1024;
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
        } else if (strncmp(p, "RANDOM_SEQ_COST_RATIO=", 22) == 0) {
            int n = atoi(p + 22);
            if (n < 1)   n = 1;
            if (n > 100) n = 100;
            g_random_seq_ratio = n;
        } else if (strncmp(p, "VACUUM_RECOMMEND_TOMBSTONE_PCT=", 31) == 0) {
            int n = atoi(p + 31);
            if (n >= 1 && n <= 100) g_vacuum_recommend_pct = n;
        } else if (strncmp(p, "VACUUM_RECOMMEND_MIN_DELETED=", 29) == 0) {
            int n = atoi(p + 29);
            if (n >= 0) g_vacuum_recommend_min_deleted = n;
        } else if (strncmp(p, "AUTO_VACUUM=", 12) == 0) {
            g_auto_vacuum_enable = (atoi(p + 12) != 0);
        } else if (strncmp(p, "AUTO_VACUUM_INTERVAL_SEC=", 25) == 0) {
            int n = atoi(p + 25);
            if (n >= 60) g_auto_vacuum_interval_sec = n; /* 1-min floor */
        } else if (strncmp(p, "WARMUP=", 7) == 0) {
            const char *v = p + 7;
            if (strcmp(v, "async") == 0 || strcmp(v, "sync") == 0 ||
                strcmp(v, "off") == 0) {
                strncpy(g_warmup_mode, v, sizeof(g_warmup_mode) - 1);
                g_warmup_mode[sizeof(g_warmup_mode) - 1] = '\0';
            }
            /* Unknown value silently keeps the previous (default) mode —
               same forgiveness pattern as the other knobs. */
        } else if (strncmp(p, "TLS_ENABLE=", 11) == 0) {
            g_tls_enable = (atoi(p + 11) != 0);
        } else if (strncmp(p, "TLS_SKIP_VERIFY=", 16) == 0) {
            g_tls_skip_verify = (atoi(p + 16) != 0);
        } else if (strncmp(p, "TLS_CERT=", 9) == 0 ||
                   strncmp(p, "TLS_KEY=", 8) == 0 ||
                   strncmp(p, "TLS_CA=", 7) == 0) {
            char *dst = NULL;
            size_t cap = 0;
            const char *vp;
            if      (strncmp(p, "TLS_CERT=", 9) == 0) { dst = g_tls_cert; cap = sizeof(g_tls_cert); vp = p + 9; }
            else if (strncmp(p, "TLS_KEY=",  8) == 0) { dst = g_tls_key;  cap = sizeof(g_tls_key);  vp = p + 8; }
            else                                       { dst = g_tls_ca;   cap = sizeof(g_tls_ca);   vp = p + 7; }
            if (*vp == '"' || *vp == '\'') vp++;
            char tmp[PATH_MAX];
            snprintf(tmp, sizeof(tmp), "%s", vp);
            char *end3 = tmp + strlen(tmp) - 1;
            while (end3 > tmp && (*end3 == '\n' || *end3 == '\r' || *end3 == '"' || *end3 == '\'')) *end3-- = '\0';
            snprintf(dst, cap, "%s", tmp);
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

/* Validate a tenant dir name: no path separators, no control chars, no
   leading dot or "..", reasonable length. Mirrors is_valid_dir's contract
   so add-dir can't sneak a malicious value past is_valid_dir later. */
static int dir_name_ok(const char *dir) {
    if (!dir || !dir[0]) return 0;
    size_t L = strlen(dir);
    if (L > 64) return 0;
    if (dir[0] == '.' || dir[0] == '/') return 0;
    for (const char *p = dir; *p; p++) {
        unsigned char c = (unsigned char)*p;
        if (c < 32) return 0;
        if (c == '/' || c == '\\' || c == ':' || c == '*' || c == '?' ||
            c == '"' || c == '<' || c == '>' || c == '|') return 0;
    }
    if (strstr(dir, "..")) return 0;
    return 1;
}

/* Add a new tenant dir. Returns 0 on success, 1 if already exists,
   negative on validation error. Persists to dirs.conf and creates the
   on-disk subdirectory. */
int dirs_add(const char *db_root, const char *dir) {
    if (!dir_name_ok(dir)) return -2;

    pthread_mutex_lock(&g_dirs_lock);
    uint32_t idx = str_hash(dir) % DIRS_BUCKETS;
    int free_slot = -1;
    for (int i = 0; i < DIRS_BUCKETS; i++) {
        int slot = (idx + i) % DIRS_BUCKETS;
        if (!g_dirs_used[slot]) { if (free_slot < 0) free_slot = slot; break; }
        if (strcmp(g_dirs[slot], dir) == 0) {
            pthread_mutex_unlock(&g_dirs_lock);
            return 1;  /* already exists */
        }
    }
    if (free_slot < 0) { pthread_mutex_unlock(&g_dirs_lock); return -1; }
    strncpy(g_dirs[free_slot], dir, 255);
    g_dirs[free_slot][255] = '\0';
    g_dirs_used[free_slot] = 1;
    g_dirs_count++;

    /* Append to dirs.conf. Atomic enough for single-writer admin ops. */
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/dirs.conf", db_root);
    FILE *f = fopen(path, "a");
    if (f) { fprintf(f, "%s\n", dir); fclose(f); }

    /* Create on-disk tenant dir so create-object has a place to land.
       EEXIST is benign — admin re-add of an existing tenant. */
    char dir_path[PATH_MAX];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", db_root, dir);
    if (mkdir(dir_path, 0755) != 0 && errno != EEXIST)
        LOG_ERROR(LOG_SUB_CONFIG, "add_dir: mkdir(%s): %s", dir_path, strerror(errno));

    pthread_mutex_unlock(&g_dirs_lock);
    return 0;
}

/* Remove a tenant dir. Returns 0 on success, 1 if not found, -2 if
   check_empty=1 and the on-disk dir still has entries. The on-disk
   dir itself is NOT deleted (data preserved); operator removes it
   manually if they want full cleanup. After remove, the in-memory
   set is reloaded from dirs.conf to repack the hash chains (linear-
   probe deletion would otherwise break is_valid_dir lookups). */
int dirs_remove(const char *db_root, const char *dir, int check_empty) {
    if (!dir || !dir[0]) return -1;

    if (check_empty) {
        char dir_path[PATH_MAX];
        snprintf(dir_path, sizeof(dir_path), "%s/%s", db_root, dir);
        DIR *d = opendir(dir_path);
        if (d) {
            struct dirent *de;
            int has_content = 0;
            while ((de = readdir(d)) != NULL) {
                if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
                /* Allow tokens.conf to remain — that's a fileystem artifact, not data */
                if (strcmp(de->d_name, "tokens.conf") == 0) continue;
                has_content = 1;
                break;
            }
            closedir(d);
            if (has_content) return -2;
        }
    }

    pthread_mutex_lock(&g_dirs_lock);
    int found = 0;
    uint32_t idx = str_hash(dir) % DIRS_BUCKETS;
    for (int i = 0; i < DIRS_BUCKETS; i++) {
        int slot = (idx + i) % DIRS_BUCKETS;
        if (!g_dirs_used[slot]) break;
        if (strcmp(g_dirs[slot], dir) == 0) { found = 1; break; }
    }
    if (!found) { pthread_mutex_unlock(&g_dirs_lock); return 1; }

    /* Rewrite dirs.conf without this entry (atomic via tmp + rename). */
    char path[PATH_MAX], tmp[PATH_MAX];
    snprintf(path, sizeof(path), "%s/dirs.conf", db_root);
    snprintf(tmp,  sizeof(tmp),  "%s.new", path);
    FILE *in = fopen(path, "r");
    FILE *out = fopen(tmp, "w");
    if (in && out) {
        char line[256];
        while (fgets(line, sizeof(line), in)) {
            char buf[256];
            snprintf(buf, sizeof(buf), "%s", line);
            buf[strcspn(buf, "\n")] = '\0';
            if (strcmp(buf, dir) == 0) continue;
            fputs(line, out);
        }
        fclose(in); fclose(out);
        if (rename(tmp, path) != 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "remove_dir: rename(%s → %s): %s", tmp, path, strerror(errno));
            unlink(tmp);
        }
    } else {
        if (in)  fclose(in);
        if (out) fclose(out);
        unlink(tmp);
    }
    pthread_mutex_unlock(&g_dirs_lock);
    /* Reload to repack the in-memory hash set; safe to call after the file
       rewrite because lock was released. */
    load_dirs();
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
    int found = 0;
    int legacy_v1 = 0;
    while (fgets(line, sizeof(line), f)) {
        if (line[0] == '#' || line[0] == '\n') continue;
        if (strncmp(line, prefix, pfxlen) == 0) {
            /* Strip trailing newline so atoi on the last field doesn't blow up. */
            line[strcspn(line, "\r\n")] = '\0';
            char *p = line + pfxlen;
            s.splits = atoi(p);
            if (s.splits <= 0) s.splits = 64;
            char *p2 = strchr(p, ':');
            int parsed_version = 0;
            if (p2) {
                s.max_key = atoi(p2 + 1);
                /* Schema line: dir:object:splits:max_key:2:streams[:auto_key=...].
                   The `2` slot is the storage-engine version, kept in the
                   on-disk format for forward compatibility. Absent or any
                   value other than 2 → legacy v1, which this binary no
                   longer supports. */
                char *p3 = strchr(p2 + 1, ':');
                if (p3) {
                    parsed_version = atoi(p3 + 1);
                    char *p4 = strchr(p3 + 1, ':');
                    if (p4) s.streams = atoi(p4 + 1);
                }
                /* Tail scan for `auto_key=...` token (added 2026.05.5).
                   Liberal: walk every `:`-separated token after splits,
                   accept the first one whose form matches; unknown tokens
                   are silently ignored so the format stays
                   forward-compatible. */
                char *scan = p2;
                while (scan) {
                    char *next = strchr(scan + 1, ':');
                    size_t tlen = next ? (size_t)(next - scan - 1) : strlen(scan + 1);
                    const char *tok = scan + 1;
                    if (tlen >= 9 && strncmp(tok, "auto_key=", 9) == 0) {
                        const char *val = tok + 9;
                        size_t vlen = tlen - 9;
                        if (vlen == 4 && strncmp(val, "uuid", 4) == 0) {
                            s.auto_key = AK_UUID;
                            s.auto_key_seq_name[0] = '\0';
                        } else if (vlen > 5 && strncmp(val, "seq(", 4) == 0 &&
                                    val[vlen - 1] == ')') {
                            size_t nlen = vlen - 5;  /* "seq(" + ")" */
                            if (nlen > 0 && nlen < sizeof(s.auto_key_seq_name)) {
                                s.auto_key = AK_SEQ;
                                memcpy(s.auto_key_seq_name, val + 4, nlen);
                                s.auto_key_seq_name[nlen] = '\0';
                            }
                        }
                        /* Unknown auto_key value → leave AK_NONE; create-object
                           validation already rejects bad values, so a malformed
                           line on disk means someone hand-edited it. */
                        break;
                    }
                    scan = next;
                }
            }
            if (parsed_version != 2) legacy_v1 = 1;
            found = 1;
            break;
        }
    }
    fclose(f);

    if (found && legacy_v1) {
        /* Object exists on disk but is in the legacy zone-A/B layout. This
           binary (2026.05.5+) dropped the v1 read path entirely. Operators
           still on v1 must first upgrade to 2026.05.4 and run that
           release's `./migrate` to convert objects to v2, then redeploy. */
        fprintf(stderr,
                "Error: Object [%s] is in legacy v1 storage; this binary "
                "supports v2 only. Install 2026.05.4 first and run "
                "`./migrate` to convert v1 → v2, then upgrade.\n",
                object);
        Schema zeroed = {0};
        return zeroed;
    }

    /* Derive max_value from typed fields.conf */
    TypedSchema *ts = load_typed_schema(effective_root, object);
    if (ts && ts->total_size > 0) {
        s.max_value = ts->total_size;
    }
    /* slot_size = 24B inline per-record header + max_key + max_value,
       rounded up to 8 bytes, floor 32. */
    s.slot_size = 24 + s.max_key + s.max_value;
    s.slot_size = (s.slot_size + 7) & ~7;
    if (s.slot_size < 32) s.slot_size = 32;

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

/* Index fields cache — hash set, avoids re-reading index.conf on every insert.
   Stores both the bare field name (composite-aware) and its declared IndexType.
   Lines on disk are either `field` (legacy → IT_BTREE) or `field:type`. */
#define IDX_BUCKETS 256
struct IdxCache {
    char name[256];
    char fields[MAX_FIELDS][256];
    enum IndexType types[MAX_FIELDS];
    int nfields;
    int used;
};
struct IdxCache g_idx_cache[IDX_BUCKETS];
pthread_mutex_t g_idx_lock = PTHREAD_MUTEX_INITIALIZER;

/* Canonical index-spec parser. Both create-object's wire validator
   and cmd_add_indexes's reindex path call this — single source of
   truth for the grammar. Returns 0 on success, -1 on bad cap or
   oversized spec. See ParsedIndexSpec in types.h for the format. */
int parse_index_spec(const char *spec, ParsedIndexSpec *out) {
    if (!spec || !*spec || !out) return -1;
    size_t L = strlen(spec);
    if (L >= sizeof(out->name)) return -1;

    out->type = IT_BTREE;
    out->max_values = 0;
    out->had_explicit_type = 0;

    /* Try the LAST `:` first; if its suffix is a known type keyword,
       split. Otherwise the whole spec is the field name (composite or
       not). */
    const char *colon = strrchr(spec, ':');
    if (colon) {
        const char *suf = colon + 1;
        size_t sl = strlen(suf);
        if (sl == 5 && memcmp(suf, "btree", 5) == 0) {
            out->type = IT_BTREE; out->had_explicit_type = 1;
        } else if (sl == 6 && memcmp(suf, "bitmap", 6) == 0) {
            out->type = IT_BITMAP; out->had_explicit_type = 1;
        } else if (sl == 7 && memcmp(suf, "trigram", 7) == 0) {
            out->type = IT_TRIGRAM; out->had_explicit_type = 1;
        } else if (sl > 7 && memcmp(suf, "bitmap(", 7) == 0 && suf[sl - 1] == ')') {
            /* bitmap(N): parse N. */
            char numbuf[16] = {0};
            size_t nl = sl - 8;
            if (nl == 0 || nl >= sizeof(numbuf)) return -1;
            memcpy(numbuf, suf + 7, nl);
            long v = strtol(numbuf, NULL, 10);
            if (v <= 0) return -1;
            out->type = IT_BITMAP;
            out->max_values = (uint32_t)v;
            out->had_explicit_type = 1;
        }
        if (out->had_explicit_type) {
            size_t nlen = (size_t)(colon - spec);
            memcpy(out->name, spec, nlen);
            out->name[nlen] = '\0';
        } else {
            /* Colon present but no known type token — treat as plain
               name (preserves the field-with-colon edge case). */
            memcpy(out->name, spec, L + 1);
        }
    } else {
        memcpy(out->name, spec, L + 1);
    }
    out->is_composite = (strchr(out->name, '+') != NULL);
    return 0;
}

/* The single auto-bitmap rule. Both create-object and reindex defer
   to this so the behaviour can never drift. Today: bare-named bool
   fields auto-promote. Enum will join when that type lands — just
   add the condition here. */
int idx_should_auto_bitmap(int had_explicit_type, enum FieldType field_type) {
    if (had_explicit_type) return 0;
    if (field_type == FT_BOOL) return 1;
    if (field_type == FT_ENUM) return 1;
    return 0;
}

/* Legacy shim used by the idx cache loader. Returns only (name, type) —
   discards explicit-type tracking + cap. parse_index_spec is the canonical
   entry; this wrapper is the cheapest way to keep idx_cache_ensure's
   call site untouched. */
static void split_index_spec(const char *line, char *name_out, size_t name_cap,
                             enum IndexType *type_out) {
    ParsedIndexSpec ps;
    if (parse_index_spec(line, &ps) != 0) {
        strncpy(name_out, line, name_cap - 1);
        name_out[name_cap - 1] = '\0';
        *type_out = IT_BTREE;
        return;
    }
    size_t nlen = strlen(ps.name);
    if (nlen >= name_cap) nlen = name_cap - 1;
    memcpy(name_out, ps.name, nlen);
    name_out[nlen] = '\0';
    *type_out = ps.type;
}

/* Populate g_idx_cache for `object` if not already cached. Returns the
   slot index, or -1 if the cache is full (which would be a config bug
   given IDX_BUCKETS=256). Caller holds no lock; we take + drop g_idx_lock
   internally. Reading index.conf happens outside the lock; insertion is
   guarded so two concurrent misses don't double-populate. */
static int idx_cache_ensure(const char *db_root, const char *object) {
    uint32_t hash = str_hash(object);

    pthread_mutex_lock(&g_idx_lock);
    for (int i = 0; i < IDX_BUCKETS; i++) {
        int slot = (hash + i) % IDX_BUCKETS;
        if (!g_idx_cache[slot].used) break;
        if (strcmp(g_idx_cache[slot].name, object) == 0) {
            pthread_mutex_unlock(&g_idx_lock);
            return slot;
        }
    }
    pthread_mutex_unlock(&g_idx_lock);

    /* Cache miss — read from file outside the lock. */
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/indexes/index.conf", db_root, object);
    FILE *f = fopen(path, "r");

    char tmp_fields[MAX_FIELDS][256];
    enum IndexType tmp_types[MAX_FIELDS];
    int count = 0;
    if (f) {
        char line[256];
        while (fgets(line, sizeof(line), f) && count < MAX_FIELDS) {
            line[strcspn(line, "\n")] = '\0';
            if (line[0] == '\0' || line[0] == '#') continue;
            split_index_spec(line, tmp_fields[count], sizeof(tmp_fields[count]),
                             &tmp_types[count]);
            count++;
        }
        fclose(f);
    }

    /* Install in cache. Race-safe: another thread may have populated the
       same slot meanwhile; we re-scan under lock and reuse if so. */
    pthread_mutex_lock(&g_idx_lock);
    int return_slot = -1;
    for (int i = 0; i < IDX_BUCKETS; i++) {
        int slot = (hash + i) % IDX_BUCKETS;
        if (g_idx_cache[slot].used && strcmp(g_idx_cache[slot].name, object) == 0) {
            return_slot = slot;
            break;
        }
        if (!g_idx_cache[slot].used) {
            strncpy(g_idx_cache[slot].name, object, 255);
            g_idx_cache[slot].name[255] = '\0';
            g_idx_cache[slot].nfields = count;
            for (int j = 0; j < count; j++) {
                memcpy(g_idx_cache[slot].fields[j], tmp_fields[j], 256);
                g_idx_cache[slot].types[j] = tmp_types[j];
            }
            g_idx_cache[slot].used = 1;
            return_slot = slot;
            break;
        }
    }
    pthread_mutex_unlock(&g_idx_lock);
    return return_slot;
}

int load_index_fields(const char *db_root, const char *object,
                             char fields[][256], int max_fields) {
    int slot = idx_cache_ensure(db_root, object);
    if (slot < 0) return 0;

    pthread_mutex_lock(&g_idx_lock);
    int n = g_idx_cache[slot].nfields;
    int copy = n < max_fields ? n : max_fields;
    for (int j = 0; j < copy; j++) {
        memcpy(fields[j], g_idx_cache[slot].fields[j], 256);
        fields[j][255] = '\0';
    }
    pthread_mutex_unlock(&g_idx_lock);
    return copy;
}

int load_index_types(const char *db_root, const char *object,
                     enum IndexType *types, int max_fields) {
    int slot = idx_cache_ensure(db_root, object);
    if (slot < 0) return 0;

    pthread_mutex_lock(&g_idx_lock);
    int n = g_idx_cache[slot].nfields;
    int copy = n < max_fields ? n : max_fields;
    for (int j = 0; j < copy; j++) types[j] = g_idx_cache[slot].types[j];
    pthread_mutex_unlock(&g_idx_lock);
    return copy;
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
/* Look up a value string in an FT_ENUM field's value list. Returns the
   0-based index on hit, -1 on miss. Linear scan — fine for n ≤ 65535
   on cold-path encoding; the hot path (match_typed) pre-resolves the
   criterion value to an index at compile time. */
int enum_value_index(const TypedField *f, const char *val, size_t vlen) {
    if (!f || f->type != FT_ENUM || !f->enum_values) return -1;
    for (int i = 0; i < f->n_enum_values; i++) {
        const char *ev = f->enum_values[i];
        if (!ev) continue;
        size_t el = strlen(ev);
        if (el == vlen && memcmp(ev, val, vlen) == 0) return i;
    }
    return -1;
}

/* Free the enum value list owned by a TypedField. Safe on NULL.
   Used by:
   - cache invalidation (so we don't leak when a schema is replaced)
   - parse_field_type re-entry (defensive cleanup if a TypedField is
     parsed twice without going through cache eviction)
   - free_typed_schema (the explicit cleanup helper) */
void free_enum_values(TypedField *f) {
    if (!f || !f->enum_values) return;
    for (int i = 0; i < f->n_enum_values; i++) free(f->enum_values[i]);
    free(f->enum_values);
    f->enum_values = NULL;
    f->n_enum_values = 0;
    f->enum_width = 0;
}

/* Strip a trailing default-modifier suffix from `type_spec_inout` and set
   the matching default_kind / default_val on `tf`. Recognised suffixes:
       :auto_update      DK_AUTO_UPDATE  (insert + update)
       :auto_create      DK_AUTO_CREATE  (insert only)
       :default=<lit>    DK_LITERAL      (verbatim string)
       :default=seq(NM)  DK_SEQ          (named sequence)
       :default=uuid()   DK_UUID         (uuidv4 per row)
       :default=random(N)DK_RANDOM       (N random bytes, hex-encoded)
   On match the suffix is replaced with a NUL so the caller can pass the
   cleaned spec to parse_field_type. Returns 1 if a modifier was stripped,
   0 otherwise. Safe to call on a spec with no modifier — leaves the spec
   untouched and tf->default_kind unchanged.

   Centralises the parse rules that used to live inline inside
   load_typed_schema (config.c) — and were therefore skipped by
   cmd_add_fields / cmd_edit_fields, which called parse_field_type
   directly and rejected every :default=... spec.

   IMPORTANT: this function does NOT initialise tf->default_kind or
   tf->default_val on entry; on a no-modifier call it leaves them
   alone. Callers that want a guaranteed clean slate should zero those
   fields before invoking. */
int parse_default_modifier(char *type_spec_inout, TypedField *tf) {
    if (!type_spec_inout || !tf) return 0;
    char *dflt;
    if ((dflt = strstr(type_spec_inout, ":auto_update")) != NULL &&
        dflt[12] == '\0') {
        tf->default_kind = DK_AUTO_UPDATE;
        *dflt = '\0';
        return 1;
    }
    if ((dflt = strstr(type_spec_inout, ":auto_create")) != NULL &&
        dflt[12] == '\0') {
        tf->default_kind = DK_AUTO_CREATE;
        *dflt = '\0';
        return 1;
    }
    if ((dflt = strstr(type_spec_inout, ":default=")) != NULL) {
        const char *dval = dflt + 9;
        if (strncmp(dval, "seq(", 4) == 0 && strchr(dval, ')')) {
            tf->default_kind = DK_SEQ;
            const char *ns = dval + 4;
            const char *ne = strchr(ns, ')');
            size_t nl = (size_t)(ne - ns);
            if (nl >= sizeof(tf->default_val)) nl = sizeof(tf->default_val) - 1;
            memcpy(tf->default_val, ns, nl);
            tf->default_val[nl] = '\0';
        } else if (strcmp(dval, "uuid()") == 0) {
            tf->default_kind = DK_UUID;
        } else if (strncmp(dval, "random(", 7) == 0 && strchr(dval, ')')) {
            tf->default_kind = DK_RANDOM;
            const char *ns = dval + 7;
            const char *ne = strchr(ns, ')');
            size_t nl = (size_t)(ne - ns);
            if (nl >= sizeof(tf->default_val)) nl = sizeof(tf->default_val) - 1;
            memcpy(tf->default_val, ns, nl);
            tf->default_val[nl] = '\0';
        } else {
            tf->default_kind = DK_LITERAL;
            strncpy(tf->default_val, dval, sizeof(tf->default_val) - 1);
            tf->default_val[sizeof(tf->default_val) - 1] = '\0';
        }
        *dflt = '\0';
        return 1;
    }
    return 0;
}

void parse_field_type(const char *spec, TypedField *f) {
    /* Defensive cleanup — if the same TypedField is re-parsed in place,
       drop the old enum value list before overwriting. */
    free_enum_values(f);

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
    } else if (strcmp(spec, "float") == 0) {
        f->type = FT_FLOAT;
        f->size = 4;
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
    } else if (strcmp(spec, "time") == 0) {
        f->type = FT_TIME;
        f->size = 3;
    } else if (strcmp(spec, "timestamp") == 0) {
        /* Unix epoch milliseconds, 8-byte int64 BE. Byte layout +
           index-key + comparison identical to FT_LONG; the type only
           differs in its auto_create / auto_update generators (which
           emit clock_gettime(CLOCK_REALTIME) in ms) and in the
           name-token for fields.conf / wire schema declarations. */
        f->type = FT_TIMESTAMP;
        f->size = 8;
    } else if (strcmp(spec, "uuid") == 0) {
        f->type = FT_UUID;
        f->size = 16;
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
    } else if (strncmp(spec, "enum(", 5) == 0) {
        /* enum(red,green,blue) — declared value list.
           Storage byte width is auto-picked by count:
             n_enum_values <= 256 → 1 byte (0..255)
             n_enum_values <= 65535 → 2 bytes BE
           Width >65535 is rejected at create-object.
           Values are split on `,`; commas inside values aren't supported.
           Duplicates are rejected at create-object (this function only
           parses; the validator above lives in cmd_create_object). */
        const char *open  = spec + 5;          /* after "enum(" */
        const char *close = strrchr(open, ')');
        if (!close || close <= open) return;   /* malformed → FT_NONE */

        /* First pass: count values + max length to size the heap alloc. */
        int n = 0;
        const char *p = open;
        while (p < close) {
            const char *next = memchr(p, ',', (size_t)(close - p));
            if (!next) next = close;
            size_t vl = (size_t)(next - p);
            if (vl > 0) n++;
            p = (next < close) ? next + 1 : close;
        }
        if (n == 0 || n > 65535) return;       /* empty list or over ceiling */

        char **vals = calloc((size_t)n, sizeof(char *));
        if (!vals) return;
        int written = 0;
        p = open;
        while (p < close && written < n) {
            const char *next = memchr(p, ',', (size_t)(close - p));
            if (!next) next = close;
            size_t vl = (size_t)(next - p);
            if (vl > 0) {
                vals[written] = malloc(vl + 1);
                if (!vals[written]) {
                    /* OOM mid-parse — undo and bail */
                    for (int i = 0; i < written; i++) free(vals[i]);
                    free(vals);
                    return;
                }
                memcpy(vals[written], p, vl);
                vals[written][vl] = '\0';
                written++;
            }
            p = (next < close) ? next + 1 : close;
        }
        f->type = FT_ENUM;
        f->enum_values = vals;
        f->n_enum_values = written;
        f->enum_width = (written <= 256) ? 1 : 2;
        f->size = f->enum_width;
    }
    /* Precompute 10^scale so decode-hot paths don't recompute per record */
    if (f->type == FT_NUMERIC) {
        int64_t mult = 1;
        for (int i = 0; i < f->numeric_scale; i++) mult *= 10;
        f->numeric_scale_mult = mult;
    } else {
        f->numeric_scale_mult = 0;
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
        tf->name_len = (int)name_len;

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
        parse_default_modifier(type_spec, tf);

        parse_field_type(type_spec, tf);
        if (tf->type == FT_NONE || tf->size <= 0) continue;

        tf->offset = offset;
        offset += tf->size;
        ts.nfields++;
        ts.typed = 1;
    }
    fclose(f);
    ts.total_size = offset;

    /* Store in cache, capture the slot we wrote to so we can return the
       pointer without doing a second (lock-free) linear-probe lookup. */
    pthread_mutex_lock(&g_typed_lock);
    uint32_t sidx = str_hash(cache_key) % TYPED_BUCKETS;
    int written_slot = -1;
    for (int i = 0; i < TYPED_BUCKETS; i++) {
        int slot = (sidx + i) % TYPED_BUCKETS;
        if (!g_typed_cache[slot].used) {
            strncpy(g_typed_cache[slot].name, cache_key, 511);
            g_typed_cache[slot].schema = ts;
            g_typed_cache[slot].used = 1;
            written_slot = slot;
            break;
        }
    }
    /* Snapshot the return pointer while still holding the lock. The cache
       slots are persistent (TYPED_BUCKETS fixed at compile time, never
       freed individually) so the pointer stays valid after unlock — same
       contract as the cache-hit return at line 833. */
    TypedSchema *result = (written_slot >= 0 &&
                           g_typed_cache[written_slot].schema.typed)
        ? &g_typed_cache[written_slot].schema
        : NULL;
    pthread_mutex_unlock(&g_typed_lock);
    return result;
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

/* ---- Encode: JSON/CSV → binary ---- */

/* Copy up to (bufsz-1) chars of val/vlen into stack-local buf, null-terminate.
   Used for numeric parses where we need a C-string for strtoll/atof but don't
   want to null-terminate the source (mmap'd page cache, shared read). Fields
   wider than the buffer are truncated — numeric values are always < 32 chars. */
static inline void cbuf_from_span(char *buf, size_t bufsz,
                                  const char *val, size_t vlen) {
    size_t cl = vlen < bufsz - 1 ? vlen : bufsz - 1;
    memcpy(buf, val, cl);
    buf[cl] = '\0';
}

/* Length-based encoder: identical semantics to encode_field but takes an
   explicit vlen instead of requiring a null-terminated string. Used by the
   CSV path to parse directly against an mmap'd, unmodifiable buffer. */
void encode_field_len(const TypedField *f, const char *val, size_t vlen,
                      uint8_t *out) {
    if (!val || vlen == 0) {
        memset(out, 0, f->size);
        return;
    }
    switch (f->type) {
    case FT_VARCHAR: {
        /* Layout: [uint16 BE length][content]. Content max = f->size - 2. */
        int content_max = f->size - 2;
        if ((int)vlen > content_max) vlen = content_max;
        out[0] = (uint8_t)((vlen >> 8) & 0xFF);
        out[1] = (uint8_t)(vlen & 0xFF);
        memcpy(out + 2, val, vlen);
        break;
    }
    case FT_LONG:
    case FT_TIMESTAMP: {
        /* Both store an int64 BE. Same encoding; FT_TIMESTAMP just
           carries the additional semantic that the value is Unix
           epoch milliseconds (drives auto_create / auto_update). */
        char cbuf[32]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        int64_t v = strtoll(cbuf, NULL, 10);
        out[0] = (v >> 56) & 0xFF; out[1] = (v >> 48) & 0xFF;
        out[2] = (v >> 40) & 0xFF; out[3] = (v >> 32) & 0xFF;
        out[4] = (v >> 24) & 0xFF; out[5] = (v >> 16) & 0xFF;
        out[6] = (v >> 8) & 0xFF;  out[7] = v & 0xFF;
        break;
    }
    case FT_INT: {
        char cbuf[16]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        int32_t v = (int32_t)atoi(cbuf);
        out[0] = (v >> 24) & 0xFF; out[1] = (v >> 16) & 0xFF;
        out[2] = (v >> 8) & 0xFF;  out[3] = v & 0xFF;
        break;
    }
    case FT_SHORT: {
        char cbuf[16]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        int16_t v = (int16_t)atoi(cbuf);
        out[0] = (v >> 8) & 0xFF; out[1] = v & 0xFF;
        break;
    }
    case FT_DOUBLE: {
        char cbuf[48]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        double v = atof(cbuf);
        memcpy(out, &v, 8);
        break;
    }
    case FT_FLOAT: {
        char cbuf[48]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        float v = (float)atof(cbuf);
        memcpy(out, &v, 4);
        break;
    }
    case FT_BOOL: {
        out[0] = (vlen > 0 && (val[0] == 't' || val[0] == 'T' || val[0] == '1')) ? 1 : 0;
        break;
    }
    case FT_BYTE: {
        char cbuf[8]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        out[0] = (uint8_t)atoi(cbuf);
        break;
    }
    case FT_DATE: {
        /* Iterate the span directly, extract up to 8 digits. */
        char clean[16]; int ci = 0;
        for (size_t i = 0; i < vlen && ci < 8; i++)
            if (val[i] >= '0' && val[i] <= '9') clean[ci++] = val[i];
        clean[ci] = '\0';
        int32_t v = (int32_t)atoi(clean);
        out[0] = (v >> 24) & 0xFF; out[1] = (v >> 16) & 0xFF;
        out[2] = (v >> 8) & 0xFF;  out[3] = v & 0xFF;
        break;
    }
    case FT_DATETIME: {
        char clean[16]; int ci = 0;
        for (size_t i = 0; i < vlen && ci < 14; i++)
            if (val[i] >= '0' && val[i] <= '9') clean[ci++] = val[i];
        while (ci < 14) clean[ci++] = '0';
        clean[14] = '\0';
        char datebuf[9]; memcpy(datebuf, clean, 8); datebuf[8] = '\0';
        int32_t d = (int32_t)atoi(datebuf);
        out[0] = (d >> 24) & 0xFF; out[1] = (d >> 16) & 0xFF;
        out[2] = (d >> 8) & 0xFF;  out[3] = d & 0xFF;
        int hh = (clean[8]-'0')*10 + (clean[9]-'0');
        int mm = (clean[10]-'0')*10 + (clean[11]-'0');
        int ss = (clean[12]-'0')*10 + (clean[13]-'0');
        uint16_t t = (uint16_t)(hh * 3600 + mm * 60 + ss);
        out[4] = (t >> 8) & 0xFF; out[5] = t & 0xFF;
        break;
    }
    case FT_TIME: {
        /* Parse "HH:MM:SS" into 3 bytes (seconds since midnight, big-endian).
           Validate strictly: 8 chars, digits at 0/1/3/4/6/7, ':' at 2 and 5,
           hh ≤ 23, mm ≤ 59, ss ≤ 59. Malformed → encode 0 (matches the UUID
           "garbage in → zeroed" convention). */
        uint32_t secs = 0;
        if (vlen >= 8 && val[2] == ':' && val[5] == ':') {
            int ok = 1;
            for (int i = 0; i < 8 && ok; i++) {
                if (i == 2 || i == 5) continue;
                if (val[i] < '0' || val[i] > '9') ok = 0;
            }
            if (ok) {
                int hh = (val[0]-'0')*10 + (val[1]-'0');
                int mm = (val[3]-'0')*10 + (val[4]-'0');
                int ss = (val[6]-'0')*10 + (val[7]-'0');
                if (hh <= 23 && mm <= 59 && ss <= 59)
                    secs = (uint32_t)hh * 3600u + (uint32_t)mm * 60u + (uint32_t)ss;
            }
        }
        out[0] = (secs >> 16) & 0xFF;
        out[1] = (secs >> 8) & 0xFF;
        out[2] = secs & 0xFF;
        break;
    }
    case FT_UUID: {
        /* Parse 32 hex digits (skip hyphens), pack into 16 bytes BE.
           On malformed input (not exactly 32 hex digits), zero the field. */
        uint8_t buf[16] = {0};
        int pos = 0;
        for (size_t i = 0; i < vlen && pos < 32; i++) {
            char c = val[i];
            if (c == '-') continue;
            int nibble = -1;
            if (c >= '0' && c <= '9') nibble = c - '0';
            else if (c >= 'a' && c <= 'f') nibble = c - 'a' + 10;
            else if (c >= 'A' && c <= 'F') nibble = c - 'A' + 10;
            if (nibble < 0) { pos = 0; break; }
            if (pos % 2 == 0) buf[pos / 2] = nibble << 4;
            else buf[pos / 2] |= nibble;
            pos++;
        }
        if (pos != 32) memset(buf, 0, 16);
        memcpy(out, buf, 16);
        break;
    }
    case FT_NUMERIC: {
        char cbuf[48]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        double dv = atof(cbuf);
        int64_t v = (int64_t)(dv * f->numeric_scale_mult + (dv >= 0 ? 0.5 : -0.5));
        out[0] = (v >> 56) & 0xFF; out[1] = (v >> 48) & 0xFF;
        out[2] = (v >> 40) & 0xFF; out[3] = (v >> 32) & 0xFF;
        out[4] = (v >> 24) & 0xFF; out[5] = (v >> 16) & 0xFF;
        out[6] = (v >> 8) & 0xFF;  out[7] = v & 0xFF;
        break;
    }
    case FT_ENUM: {
        /* Look up val in the value list; write the byte index (1 or 2
           bytes BE based on f->enum_width). On miss, zero the field —
           the JSON parse layer above is responsible for rejecting
           unknown values before we get here. enum_width is always
           f->size for FT_ENUM. */
        int idx = enum_value_index(f, (const char *)val, vlen);
        if (idx < 0) {
            memset(out, 0, f->size);
            break;
        }
        if (f->enum_width == 2) {
            out[0] = (uint8_t)((idx >> 8) & 0xFF);
            out[1] = (uint8_t)(idx & 0xFF);
        } else {
            out[0] = (uint8_t)(idx & 0xFF);
        }
        break;
    }
    default:
        memset(out, 0, f->size);
        break;
    }
}

/* Backward-compatible wrapper. Every existing caller passes a null-terminated
   C-string; for those, strlen is free (short strings are L1-hot). The CSV
   path is the only caller that benefits from the length-based variant, and
   it calls encode_field_len directly. */
void encode_field(const TypedField *f, const char *val, uint8_t *out) {
    encode_field_len(f, val, val ? strlen(val) : 0, out);
}

/* Index-key encoder — see types.h for contract. Writes memcmp-sortable bytes
   per type:
     varchar  — raw content bytes, no 2-byte length prefix (vlen out)
     bool     — 1 byte (0/1)
     byte     — 1 byte unsigned
     short    — 2 bytes BE int16 with top-bit flipped
     int      — 4 bytes BE int32 with top-bit flipped
     long     — 8 bytes BE int64 with top-bit flipped
     numeric  — 8 bytes BE int64 × 10^S with top-bit flipped
     date     — 4 bytes BE int32 with top-bit flipped (dates are always
                positive but flip kept for consistency with other signed types)
     datetime — 4 bytes BE flipped-int32 date + 2 bytes BE uint16 time
     double   — 8 bytes BE with IEEE-754 total-order transform */
void encode_field_for_index(const TypedField *f, const char *val, size_t vlen,
                            uint8_t *out, size_t *out_len) {
    if (!val || vlen == 0) {
        /* Empty / null input. For fixed-width fields we zero f->size bytes
           and the caller sees a sort-minimum key. For varchar we emit zero
           bytes (length=0 sorts before any non-empty). */
        if (f->type == FT_VARCHAR) { *out_len = 0; return; }
        memset(out, 0, f->size);
        *out_len = f->size;
        return;
    }
    switch (f->type) {
    case FT_VARCHAR: {
        /* Raw content, up to vlen bytes. No length prefix — val_cmp does the
           length tiebreak. Cap at f->size - 2 to match on-disk content cap. */
        size_t cap = (size_t)(f->size - 2);
        if (vlen > cap) vlen = cap;
        memcpy(out, val, vlen);
        *out_len = vlen;
        break;
    }
    case FT_LONG:
    case FT_TIMESTAMP: {
        /* Same index-key shape — top-bit-flipped BE int64 gives a memcmp
           total order over the signed range. */
        char cbuf[32]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        int64_t v = strtoll(cbuf, NULL, 10);
        uint64_t u = (uint64_t)v ^ (1ULL << 63);
        out[0] = (u >> 56) & 0xFF; out[1] = (u >> 48) & 0xFF;
        out[2] = (u >> 40) & 0xFF; out[3] = (u >> 32) & 0xFF;
        out[4] = (u >> 24) & 0xFF; out[5] = (u >> 16) & 0xFF;
        out[6] = (u >> 8) & 0xFF;  out[7] = u & 0xFF;
        *out_len = 8;
        break;
    }
    case FT_INT: {
        char cbuf[16]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        int32_t v = (int32_t)atoi(cbuf);
        uint32_t u = (uint32_t)v ^ 0x80000000u;
        out[0] = (u >> 24) & 0xFF; out[1] = (u >> 16) & 0xFF;
        out[2] = (u >> 8) & 0xFF;  out[3] = u & 0xFF;
        *out_len = 4;
        break;
    }
    case FT_SHORT: {
        char cbuf[16]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        int16_t v = (int16_t)atoi(cbuf);
        uint16_t u = (uint16_t)v ^ 0x8000u;
        out[0] = (u >> 8) & 0xFF; out[1] = u & 0xFF;
        *out_len = 2;
        break;
    }
    case FT_DOUBLE: {
        char cbuf[48]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        double v = atof(cbuf);
        uint64_t bits;
        memcpy(&bits, &v, 8);
        /* IEEE-754 total order: if sign bit set (negative), flip all bits;
           else flip just the sign bit. Maps all doubles to a monotonic
           unsigned key under memcmp. */
        if (bits & (1ULL << 63)) bits = ~bits;
        else bits ^= (1ULL << 63);
        out[0] = (bits >> 56) & 0xFF; out[1] = (bits >> 48) & 0xFF;
        out[2] = (bits >> 40) & 0xFF; out[3] = (bits >> 32) & 0xFF;
        out[4] = (bits >> 24) & 0xFF; out[5] = (bits >> 16) & 0xFF;
        out[6] = (bits >> 8) & 0xFF;  out[7] = bits & 0xFF;
        *out_len = 8;
        break;
    }
    case FT_FLOAT: {
        char cbuf[48]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        float v = (float)atof(cbuf);
        uint32_t bits;
        memcpy(&bits, &v, 4);
        /* IEEE-754 total order: if sign bit set (negative), flip all bits;
           else flip just the sign bit. Maps all floats to a monotonic
           unsigned key under memcmp. */
        if (bits & 0x80000000u) bits = ~bits;
        else bits ^= 0x80000000u;
        out[0] = (bits >> 24) & 0xFF; out[1] = (bits >> 16) & 0xFF;
        out[2] = (bits >> 8) & 0xFF;  out[3] = bits & 0xFF;
        *out_len = 4;
        break;
    }
    case FT_BOOL: {
        out[0] = (vlen > 0 && (val[0] == 't' || val[0] == 'T' || val[0] == '1')) ? 1 : 0;
        *out_len = 1;
        break;
    }
    case FT_BYTE: {
        char cbuf[8]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        out[0] = (uint8_t)atoi(cbuf);
        *out_len = 1;
        break;
    }
    case FT_DATE: {
        char clean[16]; int ci = 0;
        for (size_t i = 0; i < vlen && ci < 8; i++)
            if (val[i] >= '0' && val[i] <= '9') clean[ci++] = val[i];
        clean[ci] = '\0';
        int32_t v = (int32_t)atoi(clean);
        uint32_t u = (uint32_t)v ^ 0x80000000u;
        out[0] = (u >> 24) & 0xFF; out[1] = (u >> 16) & 0xFF;
        out[2] = (u >> 8) & 0xFF;  out[3] = u & 0xFF;
        *out_len = 4;
        break;
    }
    case FT_DATETIME: {
        char clean[16]; int ci = 0;
        for (size_t i = 0; i < vlen && ci < 14; i++)
            if (val[i] >= '0' && val[i] <= '9') clean[ci++] = val[i];
        while (ci < 14) clean[ci++] = '0';
        clean[14] = '\0';
        char datebuf[9]; memcpy(datebuf, clean, 8); datebuf[8] = '\0';
        int32_t d = (int32_t)atoi(datebuf);
        uint32_t du = (uint32_t)d ^ 0x80000000u;
        out[0] = (du >> 24) & 0xFF; out[1] = (du >> 16) & 0xFF;
        out[2] = (du >> 8) & 0xFF;  out[3] = du & 0xFF;
        int hh = (clean[8]-'0')*10 + (clean[9]-'0');
        int mm = (clean[10]-'0')*10 + (clean[11]-'0');
        int ss = (clean[12]-'0')*10 + (clean[13]-'0');
        uint16_t t = (uint16_t)(hh * 3600 + mm * 60 + ss);
        out[4] = (t >> 8) & 0xFF; out[5] = t & 0xFF;
        *out_len = 6;
        break;
    }
    case FT_TIME: {
        /* Parse "HH:MM:SS" into 3 bytes, top-bit flip for sorted index */
        int hh = 0, mm = 0, ss = 0;
        if (vlen >= 8) {
            hh = (val[0]-'0')*10 + (val[1]-'0');
            mm = (val[3]-'0')*10 + (val[4]-'0');
            ss = (val[6]-'0')*10 + (val[7]-'0');
        }
        uint32_t secs = hh * 3600 + mm * 60 + ss;
        uint32_t u = secs ^ 0x800000u;  /* top-bit flip to sort correctly */
        out[0] = (u >> 16) & 0xFF;
        out[1] = (u >> 8) & 0xFF;
        out[2] = u & 0xFF;
        *out_len = 3;
        break;
    }
    case FT_UUID: {
        /* Same as encode_field_len - parse 32 hex digits to 16 bytes BE */
        uint8_t buf[16] = {0};
        int pos = 0;
        for (size_t i = 0; i < vlen && pos < 32; i++) {
            char c = val[i];
            if (c == '-') continue;
            int nibble = -1;
            if (c >= '0' && c <= '9') nibble = c - '0';
            else if (c >= 'a' && c <= 'f') nibble = c - 'a' + 10;
            else if (c >= 'A' && c <= 'F') nibble = c - 'A' + 10;
            if (nibble < 0) { pos = 0; break; }
            if (pos % 2 == 0) buf[pos / 2] = nibble << 4;
            else buf[pos / 2] |= nibble;
            pos++;
        }
        if (pos != 32) memset(buf, 0, 16);
        memcpy(out, buf, 16);
        *out_len = 16;
        break;
    }
    case FT_NUMERIC: {
        char cbuf[48]; cbuf_from_span(cbuf, sizeof(cbuf), val, vlen);
        double dv = atof(cbuf);
        int64_t v = (int64_t)(dv * f->numeric_scale_mult + (dv >= 0 ? 0.5 : -0.5));
        uint64_t u = (uint64_t)v ^ (1ULL << 63);
        out[0] = (u >> 56) & 0xFF; out[1] = (u >> 48) & 0xFF;
        out[2] = (u >> 40) & 0xFF; out[3] = (u >> 32) & 0xFF;
        out[4] = (u >> 24) & 0xFF; out[5] = (u >> 16) & 0xFF;
        out[6] = (u >> 8) & 0xFF;  out[7] = u & 0xFF;
        *out_len = 8;
        break;
    }
    case FT_ENUM: {
        /* Index-key for enum = byte-index encoding identical to the
           stored record bytes. On miss (criterion uses a value not in
           the list), emit a sentinel that doesn't match any valid
           record: out_len = 0 ensures the bitmap dict lookup misses
           cleanly, and any btree range/eq search using this key
           finds nothing — which is the right answer for a query
           filtering on a value that doesn't exist. */
        int idx = enum_value_index(f, (const char *)val, vlen);
        if (idx < 0) {
            *out_len = 0;
            break;
        }
        if (f->enum_width == 2) {
            out[0] = (uint8_t)((idx >> 8) & 0xFF);
            out[1] = (uint8_t)(idx & 0xFF);
            *out_len = 2;
        } else {
            out[0] = (uint8_t)(idx & 0xFF);
            *out_len = 1;
        }
        break;
    }
    default:
        memset(out, 0, f->size);
        *out_len = f->size;
        break;
    }
}

/* Direct typed-binary → index-key. Reads the field's stored bytes at
   f->offset and emits the same memcmp-sortable encoding as
   encode_field_for_index would for equivalent text input. Skips the
   typed→ASCII render step on integer/date/numeric fields. */
void typed_field_to_index_key(const TypedSchema *ts, const uint8_t *data,
                              int field_idx, uint8_t *out, size_t *out_len) {
    const TypedField *f = &ts->fields[field_idx];
    const uint8_t *src = data + f->offset;
    switch (f->type) {
    case FT_VARCHAR: {
        /* Stored as [uint16 BE length][content]. Index key = raw content. */
        int content_max = f->size - 2;
        int len = ((int)src[0] << 8) | (int)src[1];
        if (len > content_max) len = content_max;
        if (len < 0) len = 0;
        memcpy(out, src + 2, (size_t)len);
        *out_len = (size_t)len;
        break;
    }
    case FT_LONG:
    case FT_NUMERIC:
    case FT_TIMESTAMP: {
        /* Stored BE signed — flip top bit for memcmp total-order. */
        memcpy(out, src, 8);
        out[0] ^= 0x80;
        *out_len = 8;
        break;
    }
    case FT_INT:
    case FT_DATE: {
        memcpy(out, src, 4);
        out[0] ^= 0x80;
        *out_len = 4;
        break;
    }
    case FT_SHORT: {
        memcpy(out, src, 2);
        out[0] ^= 0x80;
        *out_len = 2;
        break;
    }
    case FT_DATETIME: {
        /* int32 BE date (flip) + uint16 BE time (already unsigned). */
        memcpy(out, src, 6);
        out[0] ^= 0x80;
        *out_len = 6;
        break;
    }
    case FT_TIME: {
        /* 3 bytes, flip top bit for sorted index */
        memcpy(out, src, 3);
        out[0] ^= 0x80;
        *out_len = 3;
        break;
    }
    case FT_UUID: {
        /* Raw 16 bytes - memcmp natural, no flip needed */
        memcpy(out, src, 16);
        *out_len = 16;
        break;
    }
    case FT_DOUBLE: {
        /* Stored as host-byte-order memcpy of the double (see encode_field_len).
           For index: reinterpret as uint64, apply IEEE-754 total-order flip,
           emit big-endian. */
        double v;
        memcpy(&v, src, 8);
        uint64_t bits;
        memcpy(&bits, &v, 8);
        if (bits & (1ULL << 63)) bits = ~bits;
        else bits ^= (1ULL << 63);
        out[0] = (bits >> 56) & 0xFF; out[1] = (bits >> 48) & 0xFF;
        out[2] = (bits >> 40) & 0xFF; out[3] = (bits >> 32) & 0xFF;
        out[4] = (bits >> 24) & 0xFF; out[5] = (bits >> 16) & 0xFF;
        out[6] = (bits >> 8) & 0xFF;  out[7] = bits & 0xFF;
        *out_len = 8;
        break;
    }
    case FT_FLOAT: {
        /* Stored as host-byte-order memcpy of the float (see encode_field_len).
           For index: reinterpret as uint32, apply IEEE-754 total-order flip,
           emit big-endian. */
        float v;
        memcpy(&v, src, 4);
        uint32_t bits;
        memcpy(&bits, &v, 4);
        if (bits & 0x80000000u) bits = ~bits;
        else bits ^= 0x80000000u;
        out[0] = (bits >> 24) & 0xFF; out[1] = (bits >> 16) & 0xFF;
        out[2] = (bits >> 8) & 0xFF;  out[3] = bits & 0xFF;
        *out_len = 4;
        break;
    }
    case FT_BOOL:
    case FT_BYTE:
        out[0] = src[0];
        *out_len = 1;
        break;
    case FT_ENUM:
        /* Stored bytes ARE the index-key encoding (1 or 2 bytes BE).
           Memcpy through; bitmap dict lookups use byte-equality on
           these. */
        if (f->enum_width == 2) {
            out[0] = src[0]; out[1] = src[1];
            *out_len = 2;
        } else {
            out[0] = src[0];
            *out_len = 1;
        }
        break;
    default:
        *out_len = 0;
        break;
    }
}

int typed_encode(const TypedSchema *ts, const char *json, uint8_t *out, int out_size) {
    if (!ts || !ts->typed || out_size < ts->total_size) return -1;
    memset(out, 0, ts->total_size);

    /* Walk the JSON object exactly once. For each top-level field we
       encounter, match against the schema inline and call encode_field_len
       on the (ptr, len) span directly — no intermediate storage, no
       per-field malloc, no fixed-size bucket of unknown fields to cap.
       Unknown fields are skipped via json_skip_value; they contribute
       zero memory footprint. */
    const char *p = json_skip(json);
    if (*p != '{') return ts->total_size;
    p++;
    while (*p) {
        p = json_skip(p);
        if (*p == '}') break;
        if (*p == ',') { p++; continue; }
        if (*p != '"') break;

        p++;
        const char *fname = p;
        while (*p && !(*p == '"' && *(p - 1) != '\\')) p++;
        size_t flen = p - fname;
        if (*p == '"') p++;
        p = json_skip(p);
        if (*p != ':') break;
        p = json_skip(p + 1);

        const char *vstart = p;
        p = json_skip_value(p);
        size_t vlen = p - vstart;

        /* Match against the schema. Linear scan is fine — typical schema
           has <30 fields and the memcmp is short. name_len is cached on
           the TypedField (set in load_typed_schema). */
        for (int i = 0; i < ts->nfields; i++) {
            if (ts->fields[i].removed) continue;
            if ((int)flen != ts->fields[i].name_len) continue;
            if (memcmp(fname, ts->fields[i].name, flen) != 0) continue;
            /* Strip surrounding quotes for string values; numeric/bool/
               array/object literals pass through as-is. */
            const char *ev = vstart; size_t el = vlen;
            if (el >= 2 && ev[0] == '"' && ev[el - 1] == '"') { ev++; el -= 2; }
            if (el > 0) {
                /* Varchar values carry JSON escapes (\" \n \\ \uXXXX
                   etc.) — must decode before storage or round-trips
                   come back double-escaped. Other types' values are
                   numeric / bool / null literals that don't need it. */
                if (ts->fields[i].type == FT_VARCHAR) {
                    char *unesc = NULL; size_t unesc_len = 0;
                    if (json_unescape_string(ev, el, &unesc, &unesc_len) == 0) {
                        encode_field_len(&ts->fields[i], unesc, unesc_len,
                                          out + ts->fields[i].offset);
                        free(unesc);
                    } else {
                        encode_field_len(&ts->fields[i], ev, el,
                                          out + ts->fields[i].offset);
                    }
                } else {
                    encode_field_len(&ts->fields[i], ev, el,
                                      out + ts->fields[i].offset);
                }
            }
            break;
        }
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

/* Current Unix epoch milliseconds as a decimal string into buf
   (>= 21 bytes for the 19-digit max + sign + NUL). Used by
   FT_TIMESTAMP's auto_create / auto_update generators. Wall clock
   via clock_gettime(CLOCK_REALTIME) — sub-millisecond precision is
   fine, leap seconds are POSIX-collapsed (same convention every
   ms-since-epoch source on the planet uses). */
static void gen_timestamp_now(char *buf, size_t bufsz) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    long long ms = (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
    snprintf(buf, bufsz, "%lld", ms);
}

/* Current date as "yyyyMMdd" into buf (>= 9 bytes) */
static void gen_date_now(char *buf, size_t bufsz) {
    time_t now = time(NULL);
    struct tm tm;
    localtime_r(&now, &tm);
    snprintf(buf, bufsz, "%04d%02d%02d",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
}

/* === UUID helpers ===
 *
 * gen_uuid4_raw fills 16 bytes from /dev/urandom and stamps v4 + variant
 * bits. Single-shot; gen_uuid4 (the string formatter) calls it then
 * formats. gen_uuid4_batch amortises the open() over `n` UUIDs for
 * bulk-insert. parse_uuid_string is strict (returns -1 on malformed).
 * format_uuid_string is the inverse; both are also declared in types.h
 * for use from server.c / storage.c. */

void gen_uuid4_raw(uint8_t out[16]) {
    FILE *f = fopen("/dev/urandom", "r");
    if (!f || fread(out, 1, 16, f) != 16) {
        memset(out, 0, 16);
        if (f) fclose(f);
        return;
    }
    fclose(f);
    out[6] = (out[6] & 0x0F) | 0x40;  /* version 4 */
    out[8] = (out[8] & 0x3F) | 0x80;  /* variant 1 */
}

int gen_uuid4_batch(uint8_t *out, size_t n) {
    if (n == 0) return 0;
    FILE *f = fopen("/dev/urandom", "r");
    if (!f) return -1;
    size_t got = fread(out, 1, n * 16, f);
    fclose(f);
    if (got != n * 16) return -1;
    for (size_t i = 0; i < n; i++) {
        out[i * 16 + 6] = (out[i * 16 + 6] & 0x0F) | 0x40;
        out[i * 16 + 8] = (out[i * 16 + 8] & 0x3F) | 0x80;
    }
    return 0;
}

void format_uuid_string(const uint8_t in[16], char out[37]) {
    /* RFC 4122 canonical lowercase form: 8-4-4-4-12. */
    snprintf(out, 37,
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        in[0],in[1],in[2],in[3], in[4],in[5], in[6],in[7],
        in[8],in[9], in[10],in[11],in[12],in[13],in[14],in[15]);
}

static int hex_nibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return -1;
}

int parse_uuid_string(const char *in, uint8_t out[16]) {
    /* Strict: 36 chars, dash positions 8/13/18/23, hex elsewhere. */
    if (!in) return -1;
    if (strlen(in) != 36) return -1;
    static const int dash_pos[4] = {8, 13, 18, 23};
    for (int i = 0; i < 4; i++) {
        if (in[dash_pos[i]] != '-') return -1;
    }
    int oi = 0;
    for (int i = 0; i < 36; ) {
        if (i == 8 || i == 13 || i == 18 || i == 23) { i++; continue; }
        int hi = hex_nibble(in[i]);
        int lo = hex_nibble(in[i + 1]);
        if (hi < 0 || lo < 0) return -1;
        out[oi++] = (uint8_t)((hi << 4) | lo);
        i += 2;
    }
    return 0;
}

/* Legacy wrapper kept for the default-value path (generate_default) */
static void gen_uuid4(char *buf, size_t bufsz) {
    if (bufsz < 37) { buf[0] = '\0'; return; }
    uint8_t raw[16];
    gen_uuid4_raw(raw);
    format_uuid_string(raw, buf);
}

/* === Seq-key helpers ===
 *
 * parse_seq_key: strict decimal. Accepts optional leading '-', then one
 * or more digits, then NUL. Rejects leading zeros (except literal "0"
 * and "-0"), hex prefixes, scientific notation, whitespace.
 * format_seq_key writes the canonical decimal form. */

int parse_seq_key(const char *in, int64_t *out) {
    if (!in || !in[0]) return -1;
    const char *p = in;
    int neg = 0;
    if (*p == '-') { neg = 1; p++; if (!*p) return -1; }
    /* Reject leading zeros (e.g. "01", "007"); "0" / "-0" is the
       only zero-prefixed form allowed. */
    if (p[0] == '0' && p[1] != '\0') return -1;
    int64_t v = 0;
    for (; *p; p++) {
        if (*p < '0' || *p > '9') return -1;
        int d = *p - '0';
        /* Overflow guard: v * 10 + d must stay in int64 range. */
        if (v > (INT64_MAX - d) / 10) return -1;
        v = v * 10 + d;
    }
    if (neg) v = -v;
    *out = v;
    return 0;
}

void format_seq_key(int64_t v, char out[24]) {
    snprintf(out, 24, "%lld", (long long)v);
}

int format_wire_key(const Schema *sc, const char *key, size_t klen,
                    char *out, size_t outcap) {
    if (sc && sc->auto_key == AK_UUID) {
        if (klen != 16 || outcap < 37) return -1;
        format_uuid_string((const uint8_t *)key, out);
        return 36;
    }
    if (sc && sc->auto_key == AK_SEQ) {
        if (klen != 8 || outcap < 24) return -1;
        int64_t v = 0;
        for (int i = 0; i < 8; i++)
            v = (v << 8) | (uint8_t)key[i];
        format_seq_key(v, out);
        return (int)strlen(out);
    }
    /* AK_NONE: verbatim copy (key is a regular string). */
    if (klen + 1 > outcap) return -1;
    memcpy(out, key, klen);
    out[klen] = '\0';
    return (int)klen;
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

/* Sequence next — returns next value without printing. Used by
   DK_SEQ field defaults and by the auto_key=seq(<name>) insert path.
   Promoted from static to public in 2026.05.5 so storage.c can call it
   from cmd_insert. */
long long seq_next_val(const char *db_root, const char *object, const char *seq_name) {
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
    if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
    val++;
    f = fopen(seq_path, "w");
    if (f) { fprintf(f, "%lld\n", val); fclose(f); }

    flock(lockfd, LOCK_UN);
    close(lockfd);
    return val;
}

/* Fetch `n` next sequence values atomically — returns the starting
   value (range is [start, start+n-1]) or -1 on error. n <= 0 returns
   -1. Pairs with bulk-insert's batched auto_key=seq(...) path so we
   take one flock per request rather than per record. */
long long seq_next_val_batch(const char *db_root, const char *object,
                              const char *seq_name, int n) {
    if (n <= 0) return -1;
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
    if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
    long long start = val + 1;
    val += n;
    f = fopen(seq_path, "w");
    if (f) { fprintf(f, "%lld\n", val); fclose(f); }

    flock(lockfd, LOCK_UN);
    close(lockfd);
    return start;
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
        if (tf->type == FT_TIMESTAMP)
            gen_timestamp_now(gen_buf, bufsz);
        else if (tf->type == FT_DATETIME)
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

/* Helper: render an actionable "unknown enum value" error message into
   err_buf. Truncates the legal-values list cleanly when there's no room
   left, keeping the message readable for large enums.

   CodeQL #91 ("potentially overflowing call to snprintf"): the previous
   implementation accumulated snprintf's *would-have-written* return
   value into `pos`, then passed `err_buf_size - pos` as the next call's
   remaining-size. If snprintf returned a value larger than the actual
   space, `pos` walked past `err_buf_size` and the next subtraction
   underflowed (size_t), letting subsequent snprintf calls write
   unbounded. Fixed by clamping the running offset after every snprintf
   so it never exceeds err_buf_size − 1 (the NUL terminator slot). */
static void fmt_unknown_enum_err(const TypedField *f, const char *bad_val,
                                  size_t bad_len, char *err_buf, size_t err_buf_size) {
    if (!err_buf || err_buf_size == 0) return;
    err_buf[0] = '\0';

    #define APPEND(...) do {                                         \
        if (pos + 1 >= err_buf_size) break;                          \
        int _n = snprintf(err_buf + pos, err_buf_size - pos,         \
                          __VA_ARGS__);                              \
        if (_n < 0) { err_buf[pos] = '\0'; break; }                  \
        if ((size_t)_n >= err_buf_size - pos) {                      \
            pos = err_buf_size - 1; /* clamped + NUL-terminated */   \
            err_buf[pos] = '\0';                                     \
            break;                                                   \
        }                                                            \
        pos += (size_t)_n;                                           \
    } while (0)

    size_t pos = 0;
    APPEND("unknown enum value \"%.*s\" for field [%s]; legal: [",
           (int)bad_len, bad_val, f->name);

    for (int i = 0; i < f->n_enum_values; i++) {
        const char *v = f->enum_values[i];
        if (!v) continue;
        size_t vl = strlen(v);
        /* Reserve 5 bytes for ",..." + "]" + NUL so the truncation
           marker always fits, even if we have to bail mid-loop. */
        if (pos + vl + 5 >= err_buf_size) {
            APPEND("...");
            break;
        }
        APPEND("%s%s", i ? "," : "", v);
    }
    APPEND("]");

    #undef APPEND
}

/* Encode with field defaults applied. db_root+object needed for seq() defaults;
   pass NULL if sequence defaults are not expected.

   Strict enum validation: any FT_ENUM field whose client-supplied value
   is not in the declared list returns -2 and populates err_buf (if
   provided) with an actionable message. Callers should emit a
   {"error":"..."} response and abort the insert. NULL err_buf still
   returns -2 on bad enum input — the caller just won't see details. */
int typed_encode_defaults(const TypedSchema *ts, const char *json, uint8_t *out,
                          int out_size, const char *db_root, const char *object,
                          char *err_buf, size_t err_buf_size) {
    if (!ts || !ts->typed || out_size < ts->total_size) return -1;
    memset(out, 0, ts->total_size);
    if (err_buf && err_buf_size > 0) err_buf[0] = '\0';

    /* Same inline-match walk as typed_encode, but with a bitmap of which
       schema indices got a client value so we can apply defaults to the
       unseen ones in a second pass. No intermediate storage, no per-field
       mallocs, unknown-field-safe. */
    char seen[MAX_FIELDS] = {0};

    const char *p = json_skip(json);
    if (*p == '{') {
        p++;
        while (*p) {
            p = json_skip(p);
            if (*p == '}') break;
            if (*p == ',') { p++; continue; }
            if (*p != '"') break;

            p++;
            const char *fname = p;
            while (*p && !(*p == '"' && *(p - 1) != '\\')) p++;
            size_t flen = p - fname;
            if (*p == '"') p++;
            p = json_skip(p);
            if (*p != ':') break;
            p = json_skip(p + 1);

            const char *vstart = p;
            p = json_skip_value(p);
            size_t vlen = p - vstart;

            for (int i = 0; i < ts->nfields; i++) {
                if (ts->fields[i].removed) continue;
                if ((int)flen != ts->fields[i].name_len) continue;
                if (memcmp(fname, ts->fields[i].name, flen) != 0) continue;
                const char *ev = vstart; size_t el = vlen;
                if (el >= 2 && ev[0] == '"' && ev[el - 1] == '"') { ev++; el -= 2; }
                if (el > 0) {
                    /* Enum: strict membership check before the encode.
                       Unknown values are silently zeroed by encode_field_len
                       (legacy convention for other typed fields). For enum
                       that would corrupt every record on a typo, so we
                       reject at the wire layer instead. */
                    if (ts->fields[i].type == FT_ENUM &&
                        enum_value_index(&ts->fields[i], ev, el) < 0) {
                        fmt_unknown_enum_err(&ts->fields[i], ev, el,
                                              err_buf, err_buf_size);
                        return -2;
                    }
                    /* Same JSON-unescape pass typed_encode does for
                       varchars — keeps wire and stored bytes in sync. */
                    if (ts->fields[i].type == FT_VARCHAR) {
                        char *unesc = NULL; size_t unesc_len = 0;
                        if (json_unescape_string(ev, el, &unesc, &unesc_len) == 0) {
                            encode_field_len(&ts->fields[i], unesc, unesc_len,
                                              out + ts->fields[i].offset);
                            free(unesc);
                        } else {
                            encode_field_len(&ts->fields[i], ev, el,
                                              out + ts->fields[i].offset);
                        }
                    } else {
                        encode_field_len(&ts->fields[i], ev, el,
                                          out + ts->fields[i].offset);
                    }
                    seen[i] = 1;
                }
                break;
            }
        }
    }

    /* Fields the client didn't provide → apply configured default if any. */
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed || seen[i]) continue;
        if (ts->fields[i].default_kind == DK_NONE) continue;
        char gen_buf[256];
        const char *dv = generate_default(&ts->fields[i], gen_buf, sizeof(gen_buf),
                                          db_root, object);
        if (dv) encode_field(&ts->fields[i], dv, out + ts->fields[i].offset);
    }
    return ts->total_size;
}

/* ---- Decode: binary → JSON ---- */

static int decode_field_to_buf(const TypedField *f, const uint8_t *data, char *buf, int buflen) {
    switch (f->type) {
    case FT_VARCHAR: {
        /* Layout: [uint16 BE length][content]. Content bytes are
           opaque (UTF-8 text in practice but the engine doesn't
           enforce it) so we MUST escape per RFC 8259 — raw quotes /
           backslashes / control chars in HN comment bodies broke
           the response JSON before this. Caller's buflen needs
           room for up to 6 * len + 2 (quotes); decode_field_to_buf
           callers must size accordingly for FT_VARCHAR — see
           varchar_decode_capacity(). */
        int len = ((int)data[0] << 8) | (int)data[1];
        int content_max = f->size - 2;
        if (len > content_max) len = content_max;  /* defensive */
        if (len == 0) return 0;
        if (buflen < 4) return -1;  /* "" + NUL */
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, (size_t)buflen - 3,
                                   (const char *)(data + 2), (size_t)len);
        if (esc < 0) return -1;
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';   /* caller renders with %s */
        return 2 + esc;
    }
    case FT_LONG: {
        int64_t v = ((int64_t)data[0] << 56) | ((int64_t)data[1] << 48) |
                    ((int64_t)data[2] << 40) | ((int64_t)data[3] << 32) |
                    ((int64_t)data[4] << 24) | ((int64_t)data[5] << 16) |
                    ((int64_t)data[6] << 8) | data[7];
        if (v == 0) return 0; /* skip zero */
        return snprintf(buf, buflen, "%lld", (long long)v);
    }
    case FT_TIMESTAMP: {
        /* 0 is a valid timestamp (1970-01-01T00:00:00Z), so don't skip
           it — but uninitialised-storage zero looks the same as a real
           epoch-zero record, and there's no out-of-band sentinel to
           tell them apart. We match FT_LONG's skip-on-zero behaviour
           since uninit-zero is overwhelmingly the common case (a
           record using `auto_create` will always be > 0; an explicit
           0 epoch-ms is exotic enough that the existing emit-as-empty
           is acceptable). Callers needing strict "0 is valid" can
           inspect raw bytes via typed_get_field_str. */
        int64_t v = ((int64_t)data[0] << 56) | ((int64_t)data[1] << 48) |
                    ((int64_t)data[2] << 40) | ((int64_t)data[3] << 32) |
                    ((int64_t)data[4] << 24) | ((int64_t)data[5] << 16) |
                    ((int64_t)data[6] << 8) | data[7];
        if (v == 0) return 0;
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
    case FT_FLOAT: {
        float v;
        memcpy(&v, data, 4);
        if (v == 0.0f) return 0;
        return snprintf(buf, buflen, "%g", (double)v);
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
        int64_t scale = f->numeric_scale_mult;
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
    case FT_TIME: {
        uint32_t secs = ((uint32_t)data[0] << 16) | ((uint32_t)data[1] << 8) | data[2];
        if (secs == 0 && data[0]==0 && data[1]==0 && data[2]==0) return 0;
        int hh = secs / 3600;
        int mm = (secs % 3600) / 60;
        int ss = secs % 60;
        return snprintf(buf, buflen, "\"%02d:%02d:%02d\"", hh, mm, ss); /* "HH:MM:SS" */
    }
    case FT_UUID: {
        /* Emit JSON-quoted canonical form ("xxxxxxxx-...-xxxxxxxxxxxx").
           All-zero bytes = unset; skip empty. */
        const uint8_t *b = data;
        if (uuid_is_zero(b)) return 0;
        if (buflen < 39) return 0; /* 36 hex + 2 quotes + NUL */
        char inner[37];
        uuid_format_canonical(inner, sizeof(inner), b);
        return snprintf(buf, buflen, "\"%s\"", inner);
    }
    case FT_ENUM: {
        /* Stored bytes are the byte index (1 or 2 BE). Always emit
           (byte 0 is a legit enum index, not "unset"). Output is a
           JSON-quoted string from enum_values[idx]. Out-of-range
           index (data corruption) → empty string. */
        if (!f->enum_values || f->n_enum_values <= 0) return 0;
        int idx = (f->enum_width == 2)
                    ? (int)(((uint16_t)data[0] << 8) | (uint16_t)data[1])
                    : (int)data[0];
        if (idx < 0 || idx >= f->n_enum_values) return snprintf(buf, buflen, "\"\"");
        const char *s = f->enum_values[idx];
        if (!s) return snprintf(buf, buflen, "\"\"");
        /* Escape per RFC 8259 — enum value strings are user-supplied
           at create-object, may carry quotes/backslashes/control chars.
           Same buffer-sizing contract as FT_VARCHAR. */
        if (buflen < 4) return -1;
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, (size_t)buflen - 3, s, strlen(s));
        if (esc < 0) return -1;
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';
        return 2 + esc;
    }
    default:
        return 0;
    }
}

char *typed_decode(const TypedSchema *ts, const uint8_t *data, int data_len) {
    if (!ts || !ts->typed) return NULL;
    /* Size the outer record-JSON buffer based on actual field types.
       Pre-2026.05.6 this was a flat `nfields * 300` heuristic, which
       silently truncated mid-field on records carrying a large
       varchar (e.g. HN comment text up to 8 KB). For each field:
         "name":<value>, =>  name_len + 4 punctuation bytes
       Plus the value:
         FT_VARCHAR: worst-case 6 * (size - 2) + 2 quotes (escapes can
                     6x-expand control chars per RFC 8259)
         everything else: a generous 64 bytes (long literals, dates,
                     numerics — all fit comfortably) */
    size_t est = 4;  /* { ... } + NUL */
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed) continue;
        est += (size_t)ts->fields[i].name_len + 4;
        if (ts->fields[i].type == FT_VARCHAR) {
            int cm = ts->fields[i].size > 2 ? ts->fields[i].size - 2 : 0;
            est += (size_t)cm * 6 + 2;
        } else {
            est += 64;
        }
    }
    char *buf = malloc(est);
    size_t pos = 0;
    if (est > 0) buf[pos++] = '{';
    int first = 1;

    for (int i = 0; i < ts->nfields; i++) {
        const TypedField *f = &ts->fields[i];
        if (f->removed) continue;  /* tombstoned — not visible to consumers */
        if (f->offset + f->size > data_len) break;

        /* Stack buffer covers non-varchar fields (numbers / bool / dates
           fit comfortably). Varchar may need 6 * content_max + 2 bytes
           worst-case after JSON-escape expansion (\u00XX per byte); fall
           back to a heap allocation for those. */
        char vbuf_stack[512];
        char *vbuf = vbuf_stack;
        int vbufsz = (int)sizeof(vbuf_stack);
        if (f->type == FT_VARCHAR) {
            int need = 6 * (f->size > 2 ? f->size - 2 : 0) + 3;
            if (need > vbufsz) {
                vbuf = malloc((size_t)need);
                if (!vbuf) { vbuf = vbuf_stack; }   /* falls back; may truncate */
                else        { vbufsz = need; }
            }
        }
        int vlen = decode_field_to_buf(f, data + f->offset, vbuf, vbufsz);
        if (vlen <= 0) { if (vbuf != vbuf_stack) free(vbuf); continue; }

        if (!first && pos + 1 < est) buf[pos++] = ',';
        SB_APPEND(buf, pos, est, "\"%s\":%s", f->name, vbuf);
        first = 0;
        if (vbuf != vbuf_stack) free(vbuf);
    }
    if (pos + 1 < est) buf[pos++] = '}';
    buf[pos] = '\0';
    return buf;
}

void typed_decode_stream(const TypedSchema *ts, const uint8_t *data,
                         int data_len, FILE *out) {
    if (!ts || !ts->typed || !out) return;
    fputc('{', out);
    int first = 1;
    for (int i = 0; i < ts->nfields; i++) {
        const TypedField *f = &ts->fields[i];
        if (f->removed) continue;
        if (f->offset + f->size > data_len) break;

        char vbuf_stack[512];
        char *vbuf = vbuf_stack;
        int vbufsz = (int)sizeof(vbuf_stack);
        if (f->type == FT_VARCHAR) {
            int need = 6 * (f->size > 2 ? f->size - 2 : 0) + 3;
            if (need > vbufsz) {
                vbuf = malloc((size_t)need);
                if (!vbuf) { vbuf = vbuf_stack; }
                else        { vbufsz = need; }
            }
        }
        int vlen = decode_field_to_buf(f, data + f->offset, vbuf, vbufsz);
        if (vlen <= 0) { if (vbuf != vbuf_stack) free(vbuf); continue; }

        if (!first) fputc(',', out);
        fprintf(out, "\"%s\":%s", f->name, vbuf);
        first = 0;
        if (vbuf != vbuf_stack) free(vbuf);
    }
    fputc('}', out);
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
    case FT_TIME: {
        const uint8_t *d = data + f->offset;
        uint32_t secs = ((uint32_t)d[0] << 16) | ((uint32_t)d[1] << 8) | d[2];
        if (secs == 0 && d[0]==0 && d[1]==0 && d[2]==0) return NULL;
        int hh = secs / 3600, mm = (secs % 3600) / 60, ss = secs % 60;
        char *out = malloc(9);
        snprintf(out, 9, "%02d:%02d:%02d", hh, mm, ss);
        return out;
    }
    case FT_UUID: {
        const uint8_t *b = data + f->offset;
        if (uuid_is_zero(b)) return NULL;
        char *out = malloc(37);
        uuid_format_canonical(out, 37, b);
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
            /* Free any heap-owned per-field state (enum_values today;
               room for future per-field heap allocations). */
            TypedSchema *ts = &g_typed_cache[i].schema;
            for (int fi = 0; fi < ts->nfields; fi++) {
                free_enum_values(&ts->fields[fi]);
            }
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

    /* Each indexed field lives in its own sub-directory (per-shard layout):
       indexes/<field>/<NNN>.idx. Rename the directory and invalidate every
       shard's cache entry so the next acquirer reopens at the new path. */
    Schema sch = load_schema(db_root, object);
    int idx_n = index_splits_for(sch.splits);

    DIR *d = opendir(idx_dir);
    if (!d) return 0;
    int dfd = dirfd(d);

    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        if (strcmp(e->d_name, "index.conf") == 0) continue;

        struct stat st;
        /* fstatat against the open dirfd: TOCTOU-safe — the rename below
           uses the same dirfd so the entry we measured is the entry we
           rename. */
        if (fstatat(dfd, e->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0 ||
            !S_ISDIR(st.st_mode)) continue;

        char newbase[512];
        int chg = replace_tokens(e->d_name, old_name, new_name, newbase, sizeof(newbase));
        if (chg != 1) continue;

        /* Drop cached fd/mappings for every shard before the rename so the
           next acquirer picks up the new path. */
        for (int s = 0; s < idx_n; s++) {
            char p[PATH_MAX];
            build_idx_path(p, sizeof(p), db_root, object, e->d_name, s);
            btree_cache_invalidate(p);
        }

        if (renameat(dfd, e->d_name, dfd, newbase) != 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "rename-field: failed to rename %s/%s -> %s: %s",
                    idx_dir, e->d_name, newbase, strerror(errno));
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
    if (rename(newconf_path, conf_path) != 0) {  /* atomic swap */
        LOG_ERROR(LOG_SUB_CONFIG, "rename_field: rename(%s → %s): %s", newconf_path, conf_path, strerror(errno));
        unlink(newconf_path);
    }
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
        /* Type parseability — use a throwaway TypedField. parse_default_modifier
           strips any trailing :default=... / :auto_* suffix so parse_field_type
           sees a clean spec ("int", "varchar:32", etc.) and the default kind
           is captured on tf for the backfill pass (see rebuild_object_v2). */
        TypedField tf;
        memset(&tf, 0, sizeof(tf));
        char clean_spec[256];
        strncpy(clean_spec, colon + 1, sizeof(clean_spec) - 1);
        clean_spec[sizeof(clean_spec) - 1] = '\0';
        parse_default_modifier(clean_spec, &tf);
        parse_field_type(clean_spec, &tf);
        if (tf.type == FT_NONE || tf.size <= 0) {
            OUT("{\"error\":\"Invalid type in: %s\"}\n", lines[a]);
            return 1;
        }
        /* Pre-flight: refuse random(N) with hex output that exceeds the
           field's storage. random(N) generates 2*N hex chars; for varchar
           the on-disk content cap is f->size - 2. For other types the
           encoded representation is rendered through encode_field which
           takes a string — best-effort is to refuse if the raw hex won't
           fit a varchar. Non-varchar destinations are caller error. */
        if (tf.default_kind == DK_RANDOM) {
            int n_bytes = atoi(tf.default_val);
            int hex_chars = n_bytes * 2;
            if (tf.type == FT_VARCHAR) {
                int cap = tf.size - 2;  /* uint16 length prefix */
                if (n_bytes <= 0 || hex_chars > cap) {
                    OUT("{\"error\":\"random(%d) produces %d hex chars but field [%s] holds only %d\"}\n",
                        n_bytes, hex_chars, name, cap);
                    return 1;
                }
            } else if (n_bytes <= 0) {
                OUT("{\"error\":\"random(N) requires N > 0 in: %s\"}\n", lines[a]);
                return 1;
            }
        }
    }

    /* rebuild_object appends lines to fields.conf and rewrites shards atomically. */
    int rc = rebuild_object(db_root, object, 0, 0, lines, nlines, 0);
    if (rc == 0)
        LOG_AUDIT(LOG_SUB_CONFIG, "ADD-FIELD %s/%s: %d field(s) added", db_root, object, nlines);
    return rc;
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

    Schema sch = load_schema(db_root, object);
    int dropped = 0;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        if (strcmp(e->d_name, "index.conf") == 0) continue;

        /* Per-shard layout: indexes/<field>/<NNN>.idx — each indexed field
           lives in its own sub-directory. */
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", idx_dir, e->d_name);
        struct stat st;
        if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) continue;

        /* Split field name on '+', drop if any token is in removed_names. */
        int affected = 0;
        const char *p = e->d_name;
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

        btree_idx_unlink_all(db_root, object, e->d_name, sch.splits);
        dropped++;
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
    if (rename(tmp_path, conf_path) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "remove_field: rename(%s → %s): %s", tmp_path, conf_path, strerror(errno));
        unlink(tmp_path);
    }
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
    while (nlines < MAX_FIELDS && fgets(lines[nlines], sizeof(lines[0]), f)) {
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

    LOG_AUDIT(LOG_SUB_CONFIG, "REMOVE-FIELD %s/%s: %d fields tombstoned, %d indexes dropped",
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
    while (nlines < MAX_FIELDS && fgets(lines[nlines], sizeof(lines[0]), f)) {
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

    LOG_AUDIT(LOG_SUB_CONFIG, "RENAME-FIELD %s/%s: %s -> %s", db_root, object, old_name, new_name);
    OUT("{\"status\":\"renamed\",\"old\":\"%s\",\"new\":\"%s\"}\n", old_name, new_name);
    return 0;
}


/* ========== Query concurrency cap ========================================
 * Semaphore-based slot allocator. Each query takes a slot at dispatch
 * entry; releases on dispatch exit (success or error). When the cap is
 * reached, slot_try_acquire returns -1 immediately and the dispatcher
 * emits {"error":"server at capacity"} so clients can retry.
 *
 * Bounded concurrency makes peak RAM predictable:
 *   worst_case_query_buffers = MAX_CONCURRENT_QUERIES × QUERY_BUFFER_MB
 *
 * Slot count resolved at cmd_server startup (slot_init): operator value
 * if set in db.env, else auto = max(4, min(nproc, 32)). Auto-scaling
 * keeps the cap right for both tiny dev boxes and big servers without
 * tuning required out-of-box. */

#include <semaphore.h>

static sem_t g_query_slots;
static int   g_slots_inited = 0;

void slot_init(void) {
    if (g_slots_inited) return;
    int n = g_max_concurrent_queries;
    if (n <= 0) {
        n = parallel_threads();
        if (n < 4)  n = 4;
        if (n > 32) n = 32;
    }
    g_max_concurrent_queries = n;
    if (sem_init(&g_query_slots, 0, (unsigned)n) != 0) {
        fprintf(stderr, "config: sem_init failed: %s - slot cap disabled\n",
                strerror(errno));
        g_max_concurrent_queries = 0;
        return;
    }
    g_slots_inited = 1;
}

/* Returns 0 if a slot was acquired, -1 if at capacity. Non-blocking. */
int slot_try_acquire(void) {
    if (!g_slots_inited) return 0;            /* cap disabled — pass-through */
    if (sem_trywait(&g_query_slots) == 0) return 0;
    return -1;
}

void slot_release(void) {
    if (!g_slots_inited) return;
    sem_post(&g_query_slots);
}

/* Used with __attribute__((cleanup)) to release the slot on any
   function exit path. Pointer arg is treated as a 0/1 held flag. */
void slot_cleanup(int *held) {
    if (held && *held) slot_release();
}

#ifdef TEST_BUILD
/* Reset all process-global caches between test cases in run-all mode.
   Each test case fork-execs its own daemon, but the parent process
   holds stale cache entries that can pollute subsequent in-process
   test hooks (plan introspection, cardinality estimates, etc.). */
void test_reset_caches(void) {
    /* schema cache */
    pthread_mutex_lock(&g_schema_lock);
    for (int i = 0; i < SCHEMA_BUCKETS; i++) {
        if (g_schema_cache[i].used) {
            g_schema_cache[i].used = 0;
            g_schema_cache[i].name[0] = '\0';
        }
    }
    pthread_mutex_unlock(&g_schema_lock);

    /* fields cache */
    pthread_mutex_lock(&g_fields_lock);
    for (int i = 0; i < FIELDS_BUCKETS; i++) {
        if (g_fields_cache[i].used) {
            g_fields_cache[i].used = 0;
            g_fields_cache[i].name[0] = '\0';
        }
    }
    pthread_mutex_unlock(&g_fields_lock);

    /* typed cache — need to free enum_values heap state */
    pthread_mutex_lock(&g_typed_lock);
    for (int i = 0; i < TYPED_BUCKETS; i++) {
        if (g_typed_cache[i].used) {
            TypedSchema *ts = &g_typed_cache[i].schema;
            for (int fi = 0; fi < ts->nfields; fi++) {
                free_enum_values(&ts->fields[fi]);
            }
            g_typed_cache[i].used = 0;
            g_typed_cache[i].name[0] = '\0';
        }
    }
    pthread_mutex_unlock(&g_typed_lock);

    /* index cache */
    pthread_mutex_lock(&g_idx_lock);
    for (int i = 0; i < IDX_BUCKETS; i++) {
        if (g_idx_cache[i].used) {
            g_idx_cache[i].used = 0;
            g_idx_cache[i].name[0] = '\0';
        }
    }
    pthread_mutex_unlock(&g_idx_lock);
}
#endif

