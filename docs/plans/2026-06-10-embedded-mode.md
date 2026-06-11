# Plan: embedded mode — `shard_db_open / shard_db_query / shard_db_close`

**Date:** 2026-06-10
**Branch:** `feat/embedded-mode`
**New files:** `src/db/shard_db.h`, `src/db/shard_db_internal.h`, `src/db/embedded.c`
**Modified files:** `src/db/types.h`, `src/db/config.c`, `src/db/storage.c`,
`src/db/btree.c`, `src/db/bitmap.c`, `src/db/slotcask.c`, `src/db/objlock.c`,
`src/db/server.c`, `src/db/query.c`, `build.sh`

## Goal

Let any language that can call C (Python via ctypes, Go via cgo, Rust via FFI, etc.)
open a shard-db data directory in-process and issue queries as JSON strings — no TCP,
no daemon, no `./shard-db start`. The I/O thread pools keep running internally;
parallelism is transparent to the caller. `shard_db_query` is thread-safe: multiple
threads can call it concurrently on the same handle.

V1 constraint: one `ShardDb` instance per process. All globals that carry
per-database state are moved into a `ShardDb` struct; the TCP worker thread pool
and async log subsystem remain process-global.

## Execution rules

- Branch off `main`.
- Tasks in order; build after every task with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run-all`.
- Locate every edit by the **quoted anchor text** below; if an anchor is not found
  exactly, stop and write `PLAN_NOTES.md` — do not guess or reinterpret.
- Never claim a step passed without pasting the real build/test output.

---

## Architecture

### Thread-local pointer + macro aliasing

Rather than threading a `ShardDb *` through every function signature, all
instance-scoped globals are moved into a single struct, and a thread-local
pointer `g_db` makes them accessible:

```c
extern __thread ShardDb *g_db;

/* existing code uses g_db_root, g_timeout, etc. unchanged — they expand via macro */
#define g_db_root   (g_db->db_root)
#define g_timeout   (g_db->timeout)
/* ... */
```

**Critical ordering rule in `shard_db_internal.h`:** the ShardDb struct definition
must appear BEFORE the `#define` aliases so that struct field declarations are not
expanded by the macros. The macros come after the closing `}` of the struct.

### Scoping table

| Global | File | Scoped to ShardDb |
|---|---|---|
| g_timeout … g_token_cap, all config scalars | config.c | YES |
| TLS config (g_tls_*) | config.c | YES |
| Stats counters (g_ucache_hits … g_slow_query_count) | config.c | YES |
| g_db_root, g_log_level, g_log_retain_days | config.c | YES |
| g_slow_query_ms … g_warmup_mode | config.c | YES |
| g_slow_queries[], g_slow_query_head, g_slow_query_lock | config.c | YES |
| g_schema_cache[], g_schema_lock | config.c | YES |
| g_fields_cache[], g_fields_lock | config.c | YES |
| g_typed_cache[], g_typed_lock | config.c | YES |
| g_idx_cache[], g_idx_lock | config.c | YES |
| g_dirs[][], g_dirs_used[], g_dirs_count, g_dirs_lock | config.c | YES |
| g_query_slots, g_slots_inited | config.c | YES |
| g_ucache, g_ucache_slots, g_ucache_count, g_ucache_table_mutex, g_ucache_clock | storage.c | YES |
| g_counts_cache[], g_counts_lock | storage.c | YES |
| bt_cache, bt_cache_slots, bt_cache_count, bt_cache_lock, bt_cache_clock | btree.c | YES |
| g_bt_merge_table_lock | btree.c | YES |
| g_bm_cache, g_bm_cache_slots, g_bm_cache_count, g_bm_cache_lock, g_bm_cache_clock | bitmap.c | YES |
| g_kfcache, g_kfcache_slots, g_kfcache_count, g_kfcache_lock, g_kfcache_clock | slotcask.c | YES |
| g_segcache, g_segcache_slots, g_segcache_count, g_segcache_lock, g_segcache_clock | slotcask.c | YES |
| g_reg[], g_reg_lock | slotcask.c | YES |
| g_objlocks[], g_objlock_table_lock | objlock.c | YES |
| g_ip_set[][], g_ip_set_used[], g_ip_set_count, g_ip_lock | server.c | YES |
| g_token_set, g_token_scope, …, g_token_count, g_token_lock | server.c | YES |
| g_scan_stop | query.c | YES |
| g_log_head/tail/lock/cond/thread, g_log_dir, g_log_ring[] | config.c | NO — process-global |
| g_max_threads, g_pool_chunk, g_io_threads (pool sizing) | parallel.c | NO — process-global |
| All parallel.c pool vars | parallel.c | NO — process-global |
| bt_page_size | btree.c | NO — process-global |
| server_running, active_threads, in_flight_writes, g_worker_cfds[] | server.c | NO — TCP only |

---

## Task 1 — Create `src/db/shard_db_internal.h`

This header collects:
1. Private cache struct definitions that were file-local in their `.c` files.
2. The `ShardDb` struct that holds all instance-scoped state.
3. The `extern __thread ShardDb *g_db` declaration.
4. Macro aliases so all existing code compiles unchanged.

Create **`src/db/shard_db_internal.h`** with exactly this content:

```c
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

/* slotcask.c */
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
} KfCacheEntry;

typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
} SegCacheEntry;

#define SLOTCASK_REG_BUCKETS 1024
typedef struct {
    char key[512];
    int  used;
} RegEntry;

/* objlock.c */
#define OBJLOCK_BUCKETS 256
typedef struct {
    char name[512];
    pthread_rwlock_t lock;
    _Atomic int used;
} ObjLockEntry;

/* storage.c */
#define COUNTS_CACHE_BUCKETS 1024
typedef struct {
    char     path[512];
    int      live;
    int      deleted;
    int      valid;
    uint64_t last_access;
} CountsCacheEntry;

/* config.c */
#define SCHEMA_BUCKETS 256
struct SchemaCache { char name[512]; Schema schema; int used; };

#define FIELDS_BUCKETS 256
struct FieldsCache {
    char name[512];
    int  nfields;
    char fields[MAX_FIELDS][256];
    int  used;
};

#define TYPED_BUCKETS 256
struct TypedCacheEntry {
    char         name[512];
    TypedSchema  ts;
    int          used;
};

#define IDX_BUCKETS 256
struct IdxCache {
    char name[512];
    char index_names[MAX_FIELDS][256];
    int  index_count;
    int  used;
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
    char warmup_mode[16];
    int log_level;
    int log_retain_days;

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

#endif /* SHARD_DB_INTERNAL_H */
```

### 1b. Verify struct field struct definitions match source

Before continuing, grep each source file to confirm the private struct definitions
match what was extracted above.  If any field differs, update the struct in
`shard_db_internal.h` to match the source exactly (and record in PLAN_NOTES.md).

Check commands:
```bash
# BmCacheEntry in bitmap.c
sed -n '/typedef struct {/,/} BmCacheEntry/p' src/db/bitmap.c

# KfCacheEntry in slotcask.c
sed -n '/typedef struct {/,/} KfCacheEntry/p' src/db/slotcask.c

# SegCacheEntry in slotcask.c
sed -n '/typedef struct {/,/} SegCacheEntry/p' src/db/slotcask.c

# ObjLockEntry in objlock.c
sed -n '/typedef struct {/,/} ObjLockEntry/p' src/db/objlock.c

# CountsCacheEntry in storage.c
sed -n '/typedef struct {/,/} CountsCacheEntry/p' src/db/storage.c

# SchemaCache, FieldsCache, TypedCacheEntry, IdxCache in config.c
grep -A4 'struct SchemaCache\|struct FieldsCache' src/db/config.c | head -30
```

**Build after this task:**
```bash
SKIP_TESTS=1 ./build.sh
```
This will fail (globals still defined in .c files) — that is expected. The goal here
is that the header itself compiles without errors when included. Verify by checking
that the error messages are "redefinition" errors, not "unknown type" errors.

---

## Task 2 — Include `shard_db_internal.h` from `types.h`

**File:** `src/db/types.h`

**Anchor (very end of file, last few lines):**
```
extern char g_dirs[DIRS_BUCKETS][256];
extern int g_dirs_used[DIRS_BUCKETS];
extern int g_dirs_count;
extern pthread_mutex_t g_dirs_lock;
```

Replace the four `extern` declarations above with the include (they will be provided
by the macros):

```c
#include "shard_db_internal.h"
```

Also remove these `extern` declarations from `types.h` (they are now macros):

**Anchor block 1 — config scalar externs (remove all of these):**
```
extern uint32_t g_timeout;
extern int g_port;
extern int g_max_threads;
extern int g_workers;
extern int g_pool_chunk;
extern int g_io_threads;
extern int g_index_page_size;
extern int g_global_limit;
extern int g_max_concurrent_queries;
```
Remove (replace with empty — delete the lines).

**Anchor block 2:**
```
extern size_t g_query_buffer_max_bytes;
```
Remove.

**Anchor block 3:**
```
extern size_t g_index_build_budget_bytes;
extern int g_disable_localhost_trust;
extern int g_token_cap;
```
Remove.

**Anchor block 4:**
```
extern int g_max_request_size;
extern int g_fcache_cap;
extern int g_btcache_cap;
extern char g_db_root[PATH_MAX];
extern char g_log_dir[PATH_MAX];
extern int g_log_level;
```
Remove. `g_log_dir` stays process-global — keep only `extern char g_log_dir[PATH_MAX];`.
So replace the block with:
```c
extern char g_log_dir[PATH_MAX];
```

**Anchor block 5:**
```
extern int g_log_retain_days;
```
Remove.

**Anchor block 6:**
```
extern int g_tls_enable;
extern int g_tls_skip_verify;            /* client-side dev-only: skip CA verify */
extern char g_tls_cert[PATH_MAX];
extern char g_tls_key[PATH_MAX];
extern char g_tls_ca[PATH_MAX];
```
Remove.

**Anchor block 7:**
```
extern uint64_t g_ucache_hits;
extern uint64_t g_ucache_misses;
extern uint64_t g_bt_cache_hits;
extern uint64_t g_bt_cache_misses;
extern uint64_t g_server_start_ms;
extern uint64_t g_slow_query_count;
extern int g_slow_query_ms;
extern int g_random_seq_ratio;
extern int g_vacuum_recommend_pct;
extern int g_vacuum_recommend_min_deleted;
extern int g_auto_vacuum_enable;
extern int g_auto_vacuum_interval_sec;
extern char g_warmup_mode[16];
```
Remove.

**Anchor block 8:**
```
extern SlowQueryEntry g_slow_queries[SLOW_QUERY_RING];
extern int g_slow_query_head;
extern pthread_mutex_t g_slow_query_lock;
```
Remove.

**Anchor block 9:**
```
extern _Atomic int g_scan_stop; /* set to 1 to abort all in-flight shard scans */
```
Remove.

Build: `SKIP_TESTS=1 ./build.sh` — still expect redefinition errors, not unknown-type
errors.

---

## Task 3 — Remove global variable definitions from each `.c` file

For each file below, remove the listed variable definitions. The variables will be
provided by the `ShardDb` struct fields via the macro aliases. All removals are
**exact line deletions** — do not change surrounding code.

### 3a. `src/db/config.c`

Remove these definitions (each is a top-level variable definition):

**Anchor:** `uint32_t g_timeout = 30;` → remove  
**Anchor:** `int g_port = 9199;` → remove  
**Anchor:** `int g_workers = 0;      /* 0 = auto (nproc, min 4) — server thread pool */` → remove  
**Anchor:** `int g_io_threads = 0;` → remove  
**Anchor:** `int g_index_page_size = 4096;` → remove  
**Anchor:** `int g_global_limit = 100000;` → remove  
**Anchor:** `int g_max_request_size = 33554432; /* 32 MB default` → remove  
**Anchor:** `int g_max_concurrent_queries = 0;` → remove  
**Anchor:** `int g_fcache_cap = 4096;` → remove (includes the comment line if on same line)  
**Anchor:** `int g_btcache_cap = 1024;` → remove  
**Anchor:** `size_t g_query_buffer_max_bytes = 256ULL * 1024 * 1024;` → remove  
**Anchor:** `size_t g_index_build_budget_bytes = 1024ULL * 1024 * 1024;` → remove  
**Anchor:** `int g_disable_localhost_trust = 0; /* default: 127.0.0.1` → remove  
**Anchor:** `int g_token_cap = 1024;` → remove  
**Anchor:** `int g_tls_enable = 0;` → remove  
**Anchor:** `int g_tls_skip_verify = 0;` → remove  
**Anchor:** `char g_tls_cert[PATH_MAX] = {0};` → remove  
**Anchor:** `char g_tls_key[PATH_MAX] = {0};` → remove  
**Anchor:** `char g_tls_ca[PATH_MAX] = {0};` → remove  
**Anchor:** `uint64_t g_ucache_hits = 0;` → remove  
**Anchor:** `uint64_t g_ucache_misses = 0;` → remove  
**Anchor:** `uint64_t g_bt_cache_hits = 0;` → remove  
**Anchor:** `uint64_t g_bt_cache_misses = 0;` → remove  
**Anchor:** `uint64_t g_server_start_ms = 0;` → remove  
**Anchor:** `uint64_t g_slow_query_count = 0;` → remove  
**Anchor:** `int g_slow_query_ms = 500;` → remove  
**Anchor:** `int g_random_seq_ratio = 8;` → remove  
**Anchor:** `int g_vacuum_recommend_pct = 10;` → remove  
**Anchor:** `int g_vacuum_recommend_min_deleted = 1000;` → remove  
**Anchor:** `int g_auto_vacuum_enable = 0;` → remove  
**Anchor:** `int g_auto_vacuum_interval_sec = 3600;` → remove  
**Anchor:** `char g_warmup_mode[16] = "async";` → remove  
**Anchor:** `int g_slow_query_head = 0;` → remove  
**Anchor:** `pthread_mutex_t g_slow_query_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  
**Anchor:** `SlowQueryEntry g_slow_queries[SLOW_QUERY_RING] = {0};` → remove  
**Anchor:** `char g_db_root[PATH_MAX] = {0};` → remove  
**Anchor:** `int g_log_level = 3;` → remove  
**Anchor:** `int g_log_retain_days = 7;` → remove  
**Anchor:** `char g_dirs[DIRS_BUCKETS][256];` → remove  
**Anchor:** `int g_dirs_used[DIRS_BUCKETS];` → remove  
**Anchor:** `int g_dirs_count = 0;` → remove  
**Anchor:** `pthread_mutex_t g_dirs_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  
**Anchor:** `struct SchemaCache g_schema_cache[SCHEMA_BUCKETS];` → remove  
**Anchor:** `pthread_mutex_t g_schema_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  
**Anchor:** `struct FieldsCache g_fields_cache[FIELDS_BUCKETS];` → remove  
**Anchor:** `pthread_mutex_t g_fields_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  
**Anchor:** `static struct TypedCacheEntry g_typed_cache[TYPED_BUCKETS];` → remove  
**Anchor:** `static pthread_mutex_t g_typed_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  
**Anchor:** `struct IdxCache g_idx_cache[IDX_BUCKETS];` → remove  
**Anchor:** `pthread_mutex_t g_idx_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  

Also remove the `#define` constants that move to `shard_db_internal.h`:

**Anchor:** `#define SCHEMA_BUCKETS 256` → remove  
**Anchor:** `struct SchemaCache { char name[512]; Schema schema; int used; };` → remove  
**Anchor:** `#define FIELDS_BUCKETS 256` → remove  
**Anchor (the FieldsCache struct definition block):**
```
struct FieldsCache {
    char name[512];
    int  nfields;
    char fields[MAX_FIELDS][256];
    int  used;
};
```
→ remove  
**Anchor:** `#define TYPED_BUCKETS 256` → remove  
**Anchor (TypedCacheEntry struct definition — find the 4-line block starting with):**
```
struct TypedCacheEntry {
```
→ remove the entire struct definition block (ends with `};`)  
**Anchor:** `#define IDX_BUCKETS 256` → remove  
**Anchor (IdxCache struct definition block starting with):**
```
struct IdxCache {
```
→ remove the entire struct definition block  

Also remove the `g_query_slots` and `g_slots_inited` definitions:

**Anchor:** `static sem_t g_query_slots;` → remove  
**Anchor:** `static int   g_slots_inited = 0;` → remove  

Build: `SKIP_TESTS=1 ./build.sh`

### 3b. `src/db/storage.c`

**Anchor:** `static UCacheEntry     *g_ucache = NULL;` → remove  
**Anchor:** `static int              g_ucache_slots = 0;` → remove  
**Anchor:** `static int              g_ucache_count = 0;` → remove  
**Anchor:** `static pthread_mutex_t  g_ucache_table_mutex;` → remove  
**Anchor:** `static volatile uint64_t g_ucache_clock = 0;` → remove  

Also remove the CountsCacheEntry struct definition and its constants:

**Anchor (the CountsCacheEntry struct definition):** find the typedef block ending with
`} CountsCacheEntry;` and remove it along with `#define COUNTS_CACHE_BUCKETS 1024`.

**Anchor:** `static CountsCacheEntry g_counts_cache[COUNTS_CACHE_BUCKETS];` → remove  
**Anchor:** `static pthread_mutex_t  g_counts_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  

Build: `SKIP_TESTS=1 ./build.sh`

### 3c. `src/db/btree.c`

Remove the private struct definition (it is now in shard_db_internal.h):

**Anchor (the BtCacheEntry typedef block):**
```
typedef struct {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    pthread_rwlock_t rwlock;
    int      used;
    uint64_t last_access;
} BtCacheEntry;
```
→ remove  

**Anchor:** `static BtCacheEntry    *bt_cache = NULL;` → remove  
**Anchor:** `static int              bt_cache_slots = 0;  /* power of 2 */` → remove  
**Anchor:** `static int              bt_cache_count = 0;` → remove  
**Anchor:** `static pthread_mutex_t  bt_cache_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  
**Anchor:** `static volatile uint64_t bt_cache_clock = 0;  /* monotonic LRU counter */` → remove  
**Anchor:** `static pthread_mutex_t g_bt_merge_table_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  

Build: `SKIP_TESTS=1 ./build.sh`

### 3d. `src/db/bitmap.c`

Remove the BmCacheEntry struct definition (now in shard_db_internal.h).

**Anchor:** the `typedef struct { ... } BmCacheEntry;` block — remove it.

**Anchor:** `static BmCacheEntry    *g_bm_cache = NULL;` → remove  
**Anchor:** `static int              g_bm_cache_slots = 0;` → remove  
**Anchor:** `static int              g_bm_cache_count = 0;` → remove  
**Anchor:** `static pthread_mutex_t  g_bm_cache_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  
**Anchor:** `static volatile uint64_t g_bm_cache_clock = 0;` → remove  

Build: `SKIP_TESTS=1 ./build.sh`

### 3e. `src/db/slotcask.c`

Remove KfCacheEntry, SegCacheEntry, RegEntry struct definitions (now in
shard_db_internal.h). Also remove the `#define SLOTCASK_REG_BUCKETS 1024`.

**Anchor:** the `typedef struct { ... } KfCacheEntry;` block — remove  
**Anchor:** the `typedef struct { ... } SegCacheEntry;` block — remove  
**Anchor:** `#define SLOTCASK_REG_BUCKETS 1024` — remove  
**Anchor:** the `typedef struct { ... } RegEntry;` block — remove  

Remove the global variable definitions:

**Anchor:** `static KfCacheEntry    *g_kfcache = NULL;` → remove  
**Anchor:** `static int              g_kfcache_slots = 0;` → remove  
**Anchor:** `static int              g_kfcache_count = 0;` → remove  
**Anchor:** `static pthread_mutex_t  g_kfcache_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  
**Anchor:** `static volatile uint64_t g_kfcache_clock = 0;` → remove  
**Anchor:** `static SegCacheEntry   *g_segcache = NULL;` → remove  
**Anchor:** `static int              g_segcache_slots = 0;` → remove  
**Anchor:** `static int              g_segcache_count = 0;` → remove  
**Anchor:** `static pthread_mutex_t  g_segcache_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  
**Anchor:** `static volatile uint64_t g_segcache_clock = 0;` → remove  
**Anchor:** `static RegEntry         g_reg[SLOTCASK_REG_BUCKETS];` → remove  
**Anchor:** `static pthread_mutex_t  g_reg_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  

Build: `SKIP_TESTS=1 ./build.sh`

### 3f. `src/db/objlock.c`

Remove ObjLockEntry struct definition and constants (now in shard_db_internal.h):

**Anchor:** `#define OBJLOCK_BUCKETS 256` → remove  
**Anchor:** the `typedef struct { ... } ObjLockEntry;` block — remove  
**Anchor:** `static ObjLockEntry g_objlocks[OBJLOCK_BUCKETS];` → remove  
**Anchor:** `static pthread_mutex_t g_objlock_table_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  

Build: `SKIP_TESTS=1 ./build.sh`

### 3g. `src/db/server.c`

Remove the IP set constant and variable definitions:

**Anchor:** `#define IP_SET_BUCKETS 128` → remove  
**Anchor:** `static char g_ip_set[IP_SET_BUCKETS][46];` → remove  
**Anchor:** `static int g_ip_set_used[IP_SET_BUCKETS];` → remove  
**Anchor:** `int g_ip_set_count = 0;` → remove  
**Anchor:** `static pthread_mutex_t g_ip_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  

Remove the token store variable definitions:

**Anchor:** `static char   (*g_token_set)[256]       = NULL;` → remove  
**Anchor:** `static char   (*g_token_scope)[256]     = NULL;` → remove  
**Anchor:** `static char   (*g_token_scope_obj)[256] = NULL;` → remove  
**Anchor:** `static uint8_t *g_token_perm            = NULL;` → remove  
**Anchor:** `static int     *g_token_set_used        = NULL;` → remove  
**Anchor:** `int g_token_count = 0;` → remove  
**Anchor:** `static pthread_mutex_t g_token_lock = PTHREAD_MUTEX_INITIALIZER;` → remove  

Build: `SKIP_TESTS=1 ./build.sh`

### 3h. `src/db/query.c`

**Anchor:** `_Atomic int g_scan_stop = 0; /* shared stop flag for parallel scan */`
→ remove

Build: `SKIP_TESTS=1 ./build.sh`

After all 3a–3h removals the build should succeed (zero errors, zero warnings from the
daemon sources). Fix any remaining "undeclared" or "redefinition" errors before
proceeding — each one indicates a missed removal or a needed macro alias.

---

## Task 4 — Add `g_db` definition and server integration

### 4a. Add the `g_db` TLS definition

**File:** `src/db/embedded.c` (create new — see Task 5 for the full file; for now,
create a minimal stub so the linker resolves `g_db`):

```c
#include "types.h"

__thread ShardDb *g_db = NULL;
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed with zero errors.

### 4b. Set `g_db` in every TCP worker thread

**File:** `src/db/server.c`

The server needs a process-global pointer to the single ShardDb instance, and each
worker thread must set the thread-local `g_db` at entry.

**Anchor (in server.c, the process-global declarations near the top of the file, near
`_Atomic int server_running = 1;`):**
```
_Atomic int server_running = 1;
_Atomic int active_threads = 0;
_Atomic int in_flight_writes = 0;    /* write/schema modes; shutdown waits for these */
```

Insert immediately after those three lines:
```c
ShardDb *g_shard_db_instance = NULL; /* set by cmd_server before threads spawn */
```

**Anchor (inside the TCP worker_thread function, at the very top of the worker loop,
after the thread registers its cfd slot — look for the line that does
`active_threads++;`):**
```
        active_threads++;
```

Insert immediately after that line:
```c
        g_db = g_shard_db_instance;  /* bind thread-local to the open instance */
```

**Anchor (in `cmd_server`, just before the `parallel_pool_init` call):**
```
    slot_init();
```

Insert before that line:
```c
    /* Allocate and initialize the ShardDb instance for the server path.
       This sets all g_* macros for the main thread and for every worker
       thread spawned below via g_shard_db_instance. */
    g_shard_db_instance = shard_db_open_internal(db_root);
    if (!g_shard_db_instance) {
        fprintf(stderr, "shard_db_open_internal failed\n");
        return 1;
    }
    g_db = g_shard_db_instance;
```

Note: `shard_db_open_internal` is implemented in Task 5. It does NOT call
`parallel_pool_init` (that remains in `cmd_server`). It handles everything up to but
not including the thread pool init.

Build: `SKIP_TESTS=1 ./build.sh`

---

## Task 5 — Implement `src/db/embedded.c`

Replace the stub from Task 4a with the full implementation.

**File:** `src/db/embedded.c` — write in full:

```c
#include "types.h"
#include <semaphore.h>

/* Thread-local pointer to the active ShardDb instance.
   Set by shard_db_open (embedded path) or cmd_server (TCP path). */
__thread ShardDb *g_db = NULL;

/* ── Single-instance guard ── */
static _Atomic int g_instance_open = 0;

/* ── Internal helpers ── */

static void db_mutexes_init(ShardDb *db) {
    pthread_mutex_init(&db->slow_query_lock,      NULL);
    pthread_mutex_init(&db->dirs_lock,             NULL);
    pthread_mutex_init(&db->schema_lock,           NULL);
    pthread_mutex_init(&db->fields_lock,           NULL);
    pthread_mutex_init(&db->typed_lock,            NULL);
    pthread_mutex_init(&db->idx_lock,              NULL);
    pthread_mutex_init(&db->ucache_table_mutex,    NULL);
    pthread_mutex_init(&db->counts_lock,           NULL);
    pthread_mutex_init(&db->bt_cache_lock,         NULL);
    pthread_mutex_init(&db->bt_merge_table_lock,   NULL);
    pthread_mutex_init(&db->bm_cache_lock,         NULL);
    pthread_mutex_init(&db->kfcache_lock,          NULL);
    pthread_mutex_init(&db->segcache_lock,         NULL);
    pthread_mutex_init(&db->reg_lock,              NULL);
    pthread_mutex_init(&db->objlock_table_lock,    NULL);
    pthread_mutex_init(&db->ip_lock,               NULL);
    pthread_mutex_init(&db->token_lock,            NULL);
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
    db_mutexes_init(db);
    snprintf(db->db_root, sizeof(db->db_root), "%s", db_root);
    atomic_init(&db->scan_stop, 0);

    /* Set thread-local so all g_* macros work during init below. */
    g_db = db;

    /* Load db.env from the db_root directory (chdir-free: open explicitly). */
    char env_path[PATH_MAX];
    snprintf(env_path, sizeof(env_path), "%s/../db.env", db_root);
    /* Attempt to load config from db.env in parent of db_root.
       Ignore errors — defaults are already set above. */
    {
        FILE *f = fopen("db.env", "r");
        if (f) { fclose(f); load_db_root(db->db_root, sizeof(db->db_root)); }
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

    load_dirs(db->db_root);
    load_tokens_conf(db->db_root);
    load_allowed_ips_conf(db->db_root);
    objlock_init();
    rebuild_recovery(db->db_root);
    grow_recovery(db->db_root);

    return db;
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

void shard_db_close(ShardDb *db) {
    if (!db) return;
    g_db = db;

    parallel_pool_shutdown();
    parallel_io_pool_shutdown();
    bt_cache_shutdown();
    bm_cache_shutdown();
    /* Slotcask: close all open SlotcaskDb handles tracked in g_reg. */
    slotcask_close_all();
    /* ucache: munmap all live entries. */
    ucache_shutdown();

    /* Free token store heap arrays. */
    free(db->token_set);
    free(db->token_scope);
    free(db->token_scope_obj);
    free(db->token_perm);
    free(db->token_set_used);

    /* Destroy all mutexes. */
    pthread_mutex_destroy(&db->slow_query_lock);
    pthread_mutex_destroy(&db->dirs_lock);
    pthread_mutex_destroy(&db->schema_lock);
    pthread_mutex_destroy(&db->fields_lock);
    pthread_mutex_destroy(&db->typed_lock);
    pthread_mutex_destroy(&db->idx_lock);
    pthread_mutex_destroy(&db->ucache_table_mutex);
    pthread_mutex_destroy(&db->counts_lock);
    pthread_mutex_destroy(&db->bt_cache_lock);
    pthread_mutex_destroy(&db->bt_merge_table_lock);
    pthread_mutex_destroy(&db->bm_cache_lock);
    pthread_mutex_destroy(&db->kfcache_lock);
    pthread_mutex_destroy(&db->segcache_lock);
    pthread_mutex_destroy(&db->reg_lock);
    pthread_mutex_destroy(&db->objlock_table_lock);
    pthread_mutex_destroy(&db->ip_lock);
    pthread_mutex_destroy(&db->token_lock);

    if (db->slots_inited) sem_destroy(&db->query_slots);

    free(db);
    g_db = NULL;
    atomic_store(&g_instance_open, 0);
}
```

**Note on `slotcask_close_all` and `ucache_shutdown`:** check whether these functions
exist in slotcask.c / storage.c. If not, add minimal stubs:

```c
/* In storage.c — add if not present: */
void ucache_shutdown(void) {
    pthread_mutex_lock(&g_ucache_table_mutex);
    if (g_ucache) {
        for (int i = 0; i < g_ucache_slots; i++) {
            UCacheEntry *e = &g_ucache[i];
            if (e->map && e->map_size)
                munmap(e->map, e->map_size);
            if (e->fd >= 0)
                close(e->fd);
        }
        free(g_ucache);
        g_ucache = NULL;
        g_ucache_slots = 0;
        g_ucache_count = 0;
    }
    pthread_mutex_unlock(&g_ucache_table_mutex);
}
```

```c
/* In slotcask.c — add if not present: */
void slotcask_close_all(void) {
    pthread_mutex_lock(&g_reg_lock);
    for (int i = 0; i < SLOTCASK_REG_BUCKETS; i++) {
        if (g_reg[i].used) {
            /* best-effort: nothing to do beyond marking empty — SlotcaskDb
               objects are per-query stack-allocated or already closed */
            g_reg[i].used = 0;
        }
    }
    pthread_mutex_unlock(&g_reg_lock);
}
```

Also add the shutdown function declarations to `types.h`:

**Anchor:** `char *json_obj_strdup(const JsonObj *o, const char *key);`

Insert after that line:
```c
void ucache_shutdown(void);
void slotcask_close_all(void);
ShardDb *shard_db_open_internal(const char *db_root);
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed with zero errors.

---

## Task 6 — Create public header `src/db/shard_db.h`

```c
#ifndef SHARD_DB_H
#define SHARD_DB_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ShardDb ShardDb;

/* Open a shard-db data directory for in-process use.
   Reads db.env from the current working directory (same as the daemon).
   Returns NULL on error. Only one instance per process is allowed (V1). */
ShardDb *shard_db_open(const char *db_root);

/* Execute a JSON query string and return the JSON response in *out.
   The caller must free *out with shard_db_free_result().
   Thread-safe: multiple threads may call concurrently on the same handle.
   Returns 0 on success, -1 on allocation failure. */
int shard_db_query(ShardDb *db, const char *json, char **out, size_t *out_len);

/* Free a result buffer returned by shard_db_query. */
void shard_db_free_result(char *out);

/* Close the instance and free all resources. */
void shard_db_close(ShardDb *db);

#ifdef __cplusplus
}
#endif

#endif /* SHARD_DB_H */
```

---

## Task 7 — Add `libshard-db.a` to `build.sh`

**File:** `build.sh`

**Anchor (the line that builds the shard-db binary):**
```
gcc $MODE_CFLAGS -o shard-db src/db/util.c src/db/parallel.c src/db/storage.c src/db/index.c src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c src/db/io_direct.c src/db/query.c src/db/server.c src/db/main.c src/db/config.c -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS -lpthread -lssl -lcrypto
```

Insert immediately after that line:
```bash
# libshard-db.a — embedded mode static library (all daemon sources except main.c)
ar rcs libshard-db.a \
    $(gcc $MODE_CFLAGS -c src/db/util.c     -Isrc/db $OSSL_CFLAGS -o /dev/null 2>/dev/null || true) \
    $(for f in src/db/util.c src/db/parallel.c src/db/storage.c src/db/index.c \
               src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c \
               src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c \
               src/db/io_direct.c src/db/query.c src/db/server.c src/db/config.c \
               src/db/embedded.c; do
        obj="${f%.c}.embedded.o"
        gcc $MODE_CFLAGS -c "$f" -Isrc/db $OSSL_CFLAGS -o "$obj"
        echo "$obj"
    done)
```

Actually, replace the above with the simpler and correct form — the `ar` invocation
using a temp dir pattern. Replace the block above with:

```bash
# libshard-db.a — embedded mode static library (all daemon sources except main.c)
LIB_SRCS="src/db/util.c src/db/parallel.c src/db/storage.c src/db/index.c \
          src/db/keyset.c src/db/btree.c src/db/bitmap.c src/db/trigram.c \
          src/db/objlock.c src/db/tls.c src/db/slotcask.c src/db/simd.c \
          src/db/io_direct.c src/db/query.c src/db/server.c src/db/config.c \
          src/db/embedded.c"
LIB_OBJS=""
for f in $LIB_SRCS; do
    obj="build/obj/$(basename "${f%.c}").o"
    mkdir -p build/obj
    gcc $MODE_CFLAGS -c "$f" -Isrc/db $OSSL_CFLAGS -o "$obj"
    LIB_OBJS="$LIB_OBJS $obj"
done
ar rcs build/bin/libshard-db.a $LIB_OBJS
```

Also add `libshard-db.a` to the `cp` line that populates `build/bin/`:

**Anchor:**
```
cp shard-db shard-cli shard-db-test shard-db-bench migrate build/bin/
```

Replace with:
```bash
cp shard-db shard-cli shard-db-test shard-db-bench migrate build/bin/
cp src/db/shard_db.h build/bin/
```

(libshard-db.a is already written directly to build/bin/ in the ar command above.)

Build: `SKIP_TESTS=1 ./build.sh` — must produce `build/bin/libshard-db.a` and
`build/bin/shard_db.h`.

---

## Task 8 — Build and test

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed`.

Paste the actual output — do not claim pass without it.

Quick smoke test (requires a db.env + running data dir):
```bash
cat > /tmp/embedded_test.c << 'EOF'
#include <stdio.h>
#include "shard_db.h"

int main(void) {
    ShardDb *db = shard_db_open(".");
    if (!db) { fprintf(stderr, "open failed\n"); return 1; }
    char *out; size_t len;
    shard_db_query(db, "{\"mode\":\"db-dirs\"}", &out, &len);
    printf("%.*s\n", (int)len, out);
    shard_db_free_result(out);
    shard_db_close(db);
    return 0;
}
EOF
gcc -o /tmp/embedded_test /tmp/embedded_test.c \
    -Ibuild/bin build/bin/libshard-db.a \
    -lpthread -lssl -lcrypto
cd /path/to/your/db/root && /tmp/embedded_test
```

Expected: JSON list of tenant directories, no crash, clean exit.

---

## Invariants and edge cases

| Case | Expected |
|---|---|
| Second `shard_db_open` call in same process | Returns NULL with error message |
| `shard_db_query` from multiple threads concurrently | Safe — each sets its own thread-local `g_db`; `dispatch_json_query` uses the per-thread `g_out` |
| TCP server path (cmd_server) | Calls `shard_db_open_internal`, sets `g_shard_db_instance`, each worker sets `g_db = g_shard_db_instance` |
| `g_db == NULL` (code called before open) | Segfault on macro dereference — document that open must be called first |
| Process-global pools (parallel.c) | Shared; `parallel_pool_init` is idempotent (early-return if already running) |
| `bt_page_size` global | Last write wins; all instances must use the same page size (document V1 limitation) |
| Logging | Writes to `g_log_dir` which is process-global; points to the last-opened instance's log dir |
| All 34 test cases | Must still pass — the TCP path is unchanged; only the global storage location changes |

---

## Files summary

| File | Action |
|---|---|
| `src/db/shard_db_internal.h` | **CREATE** — private structs + ShardDb + g_db + macros |
| `src/db/shard_db.h` | **CREATE** — public API |
| `src/db/embedded.c` | **CREATE** — open / query / close |
| `src/db/types.h` | **MODIFY** — remove scoped externs, include shard_db_internal.h |
| `src/db/config.c` | **MODIFY** — remove ~50 global variable and struct definitions |
| `src/db/storage.c` | **MODIFY** — remove ucache + counts globals |
| `src/db/btree.c` | **MODIFY** — remove BtCacheEntry + cache globals |
| `src/db/bitmap.c` | **MODIFY** — remove BmCacheEntry + cache globals |
| `src/db/slotcask.c` | **MODIFY** — remove KfCacheEntry, SegCacheEntry, RegEntry + globals |
| `src/db/objlock.c` | **MODIFY** — remove ObjLockEntry + globals |
| `src/db/server.c` | **MODIFY** — remove IP/token globals; add g_shard_db_instance + g_db binding in worker |
| `src/db/query.c` | **MODIFY** — remove g_scan_stop definition |
| `build.sh` | **MODIFY** — add libshard-db.a target |
