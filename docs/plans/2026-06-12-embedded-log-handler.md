# Plan: Embedded mode log handler API

**Date:** 2026-06-12  
**Feature:** `shard_db_set_log_handler` — callback-based logging for embedded use

## Background

In daemon mode `log_init()` starts an async ring-buffer drain thread that writes
`logs/YYYY-MM-DD-{info,error,slow,audit}.log`. In embedded mode `log_init` is
never called, so `g_log_running == 0` and every `log_msg_sub` / `log_audit_sub`
/ `log_slow_query` call returns immediately — all logs are silently dropped.

This plan adds a callback API (à la SQLite's `SQLITE_CONFIG_LOG`) so embedders
can receive log events without touching the daemon log infrastructure.

## Execution rules

- Branch off `main`: `git checkout -b feat/embedded-log-handler`
- Do tasks in order; each task is independent and builds on the previous.
- Build after each task: `SKIP_TESTS=1 ./build.sh`
- Full test after all C tasks complete: `./build/bin/shard-db-test run-all`
- Never claim a step passed without the real output.
- If a quoted anchor is not found exactly, stop and write `PLAN_NOTES.md`.

---

## Task 1 — Add log handler fields to `ShardDb` in `shard_db_internal.h`

**File:** `src/db/shard_db_internal.h`

Anchor — find the exact line:
```
    int log_retain_days;
```

Insert immediately after it (before the blank line that follows):
```c
    /* embedded-mode log handler (set via shard_db_set_log_handler).
       Called synchronously on the same thread as log emission when
       g_log_running == 0 (no drain thread).  Must be thread-safe.
       type: 1=ERROR 2=WARN 3=INFO 4=DEBUG 5=AUDIT 6=SLOW */
    void (*log_handler)(int type, const char *msg, void *ud);
    void *log_handler_ud;
```

Result after edit — the block should read:
```c
    int log_level;
    int log_retain_days;
    /* embedded-mode log handler (set via shard_db_set_log_handler).
       Called synchronously on the same thread as log emission when
       g_log_running == 0 (no drain thread).  Must be thread-safe.
       type: 1=ERROR 2=WARN 3=INFO 4=DEBUG 5=AUDIT 6=SLOW */
    void (*log_handler)(int type, const char *msg, void *ud);
    void *log_handler_ud;

    /* slow query ring */
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed with 0 errors.

---

## Task 2 — Add public API to `shard_db.h`

**File:** `src/db/shard_db.h`

Anchor — find the exact line:
```
/* Close the instance and free all resources. */
void shard_db_close(ShardDb *db);
```

Insert immediately after that block (before `#ifdef __cplusplus`):
```c
/* Log event type constants passed to the shard_db_set_log_handler callback. */
#define SHARD_DB_LOG_ERROR 1  /* internal errors */
#define SHARD_DB_LOG_WARN  2  /* warnings */
#define SHARD_DB_LOG_INFO  3  /* general info */
#define SHARD_DB_LOG_DEBUG 4  /* verbose debug */
#define SHARD_DB_LOG_AUDIT 5  /* auth / write audit trail */
#define SHARD_DB_LOG_SLOW  6  /* slow-query threshold crossed */

/* Register a log handler for embedded use.
   fn is called synchronously on the emitting thread for every log event.
   Pass NULL to unregister.  The handler must be thread-safe — shard-db
   parallel workers can emit log events from threads other than the caller.
   No-op when the ring-buffer drain thread is running (daemon mode).
   msg is a pre-formatted, newline-terminated string:
     "YYYY-MM-DD HH:MM:SS LEVEL [subsystem] text\n" */
void shard_db_set_log_handler(ShardDb *db,
    void (*fn)(int type, const char *msg, void *userdata),
    void *userdata);
```

Result — the bottom of `shard_db.h` before `#ifdef __cplusplus` should be:
```c
/* Close the instance and free all resources. */
void shard_db_close(ShardDb *db);

/* Log event type constants passed to the shard_db_set_log_handler callback. */
#define SHARD_DB_LOG_ERROR 1
#define SHARD_DB_LOG_WARN  2
#define SHARD_DB_LOG_INFO  3
#define SHARD_DB_LOG_DEBUG 4
#define SHARD_DB_LOG_AUDIT 5
#define SHARD_DB_LOG_SLOW  6

/* Register a log handler for embedded use.
   fn is called synchronously on the emitting thread for every log event.
   Pass NULL to unregister.  The handler must be thread-safe — shard-db
   parallel workers can emit log events from threads other than the caller.
   No-op when the ring-buffer drain thread is running (daemon mode).
   msg is a pre-formatted, newline-terminated string:
     "YYYY-MM-DD HH:MM:SS LEVEL [subsystem] text\n" */
void shard_db_set_log_handler(ShardDb *db,
    void (*fn)(int type, const char *msg, void *userdata),
    void *userdata);

#ifdef __cplusplus
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 3 — Implement `shard_db_set_log_handler` in `embedded.c`

**File:** `src/db/embedded.c`

Anchor — find the exact line:
```
void shard_db_close(ShardDb *db) {
```

Insert immediately before it (keep the blank line between):
```c
void shard_db_set_log_handler(ShardDb *db,
    void (*fn)(int type, const char *msg, void *userdata),
    void *userdata) {
    if (!db) return;
    db->log_handler    = fn;
    db->log_handler_ud = userdata;
}

```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 4 — Route `log_msg_sub` to handler in `config.c`

**File:** `src/db/config.c`

### 4a — Change the early-return guard

Anchor — find the exact two-line block:
```c
void log_msg_sub(int level, const char *subsystem, const char *fmt, ...) {
    if (level > g_log_level ||
        !atomic_load_explicit(&g_log_running, memory_order_relaxed)) return;
```

Replace with:
```c
void log_msg_sub(int level, const char *subsystem, const char *fmt, ...) {
    if (level > g_log_level) return;
    int _running = atomic_load_explicit(&g_log_running, memory_order_relaxed);
    if (!_running && !(g_db && g_db->log_handler)) return;
```

### 4b — Add handler dispatch before ring-buffer push

Anchor — find the exact block (the pthread_mutex_lock that ends log_msg_sub):
```c
    pthread_mutex_lock(&g_log_lock);
    int next = (g_log_head + 1) % LOG_RING_SIZE;
    if (next != g_log_tail) {
        g_log_ring[g_log_head] = entry;
        g_log_head = next;
    }
    pthread_cond_signal(&g_log_cond);
    pthread_mutex_unlock(&g_log_lock);
}


void log_audit_sub
```

Replace with:
```c
    if (!_running && g_db && g_db->log_handler) {
        g_db->log_handler(level, entry.msg, g_db->log_handler_ud);
        return;
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


void log_audit_sub
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 5 — Route `log_audit_sub` to handler in `config.c`

**File:** `src/db/config.c`

### 5a — Change the early-return guard

Anchor — find the exact two-line block:
```c
void log_audit_sub(const char *subsystem, const char *fmt, ...) {
    if (!atomic_load_explicit(&g_log_running, memory_order_relaxed)) return;
```

Replace with:
```c
void log_audit_sub(const char *subsystem, const char *fmt, ...) {
    int _running = atomic_load_explicit(&g_log_running, memory_order_relaxed);
    if (!_running && !(g_db && g_db->log_handler)) return;
```

### 5b — Add handler dispatch before ring-buffer push

Anchor — find the exact block that ends `log_audit_sub` (the mutex block right before `log_slow_query`):
```c
    pthread_mutex_lock(&g_log_lock);
    int next = (g_log_head + 1) % LOG_RING_SIZE;
    if (next != g_log_tail) {
        g_log_ring[g_log_head] = entry;
        g_log_head = next;
    }
    pthread_cond_signal(&g_log_cond);
    pthread_mutex_unlock(&g_log_lock);
}

void log_slow_query
```

Replace with:
```c
    if (!_running && g_db && g_db->log_handler) {
        g_db->log_handler(5 /* SHARD_DB_LOG_AUDIT */, entry.msg, g_db->log_handler_ud);
        return;
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

void log_slow_query
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 6 — Route `log_slow_query` file-log path to handler in `config.c`

**File:** `src/db/config.c`

The in-memory `SlowQueryEntry` ring (used by `/stats`) is always updated and
must not change. Only the file-log section (currently guarded by `g_log_running`)
needs to also route to the handler.

### 6a — Change the early-return guard in the file-log section

Anchor — find the exact line (inside `log_slow_query`, after the
`pthread_mutex_unlock(&g_slow_query_lock)` call):
```c
    if (!atomic_load_explicit(&g_log_running, memory_order_relaxed)) return;
```

Replace with:
```c
    int _running = atomic_load_explicit(&g_log_running, memory_order_relaxed);
    if (!_running && !(g_db && g_db->log_handler)) return;
```

### 6b — Add handler dispatch before ring-buffer push

Anchor — find the exact block that ends `log_slow_query`:
```c
    pthread_mutex_lock(&g_log_lock);
    int next2 = (g_log_head + 1) % LOG_RING_SIZE;
    if (next2 != g_log_tail) {
        g_log_ring[g_log_head] = entry;
        g_log_head = next2;
    }
    pthread_cond_signal(&g_log_cond);
    pthread_mutex_unlock(&g_log_lock);
}
```

Replace with:
```c
    if (!_running && g_db && g_db->log_handler) {
        g_db->log_handler(6 /* SHARD_DB_LOG_SLOW */, entry.msg, g_db->log_handler_ud);
        return;
    }

    pthread_mutex_lock(&g_log_lock);
    int next2 = (g_log_head + 1) % LOG_RING_SIZE;
    if (next2 != g_log_tail) {
        g_log_ring[g_log_head] = entry;
        g_log_head = next2;
    }
    pthread_cond_signal(&g_log_cond);
    pthread_mutex_unlock(&g_log_lock);
}
```

Build and test:
```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```
Expected: `# total: N passed, 0 failed` (all existing tests pass — daemon path unchanged).

---

## Task 7 — Add `set_log_handler` N-API function in `npm/src/binding.c`

The C log handler will be called from shard-db worker threads (not the JS
thread). To avoid calling `napi_call_function` from a non-JS thread, we buffer
log entries in the `DbHandle` during a query and drain them synchronously at
the end of `napi_query` — at which point we are back on the JS thread.

**File:** `npm/src/binding.c`

### 7a — Add `<pthread.h>` include

Anchor — find the exact block:
```c
#include <node_api.h>
#include <string.h>
#include <stdlib.h>
```

Replace with:
```c
#include <node_api.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
```

### 7c — Add log buffer types and update `DbHandle`

Anchor — find the exact line:
```c
typedef struct { ShardDb *db; int closed; } DbHandle;
```

Replace with:
```c
#define LOG_BUF_CAP 128

typedef struct {
    int  type;
    char msg[1024];
} LogBufEntry;

typedef struct {
    ShardDb        *db;
    int             closed;
    napi_ref        log_fn_ref;     /* NULL when no handler registered */
    LogBufEntry     log_buf[LOG_BUF_CAP];
    int             log_buf_n;
    pthread_mutex_t log_buf_lock;
} DbHandle;
```

### 7d — Add C log callback function

Anchor — find the exact line:
```c
static void db_finalizer(napi_env env, void *data, void *hint) {
```

Insert immediately before it:
```c
static void c_log_handler(int type, const char *msg, void *ud) {
    DbHandle *h = (DbHandle *)ud;
    pthread_mutex_lock(&h->log_buf_lock);
    if (h->log_buf_n < LOG_BUF_CAP) {
        h->log_buf[h->log_buf_n].type = type;
        snprintf(h->log_buf[h->log_buf_n].msg,
                 sizeof(h->log_buf[0].msg), "%s", msg ? msg : "");
        h->log_buf_n++;
    }
    pthread_mutex_unlock(&h->log_buf_lock);
}

```

### 7e — Update `db_finalizer` to clean up log resources

Anchor — find the exact block:
```c
static void db_finalizer(napi_env env, void *data, void *hint) {
    (void)env; (void)hint;
    DbHandle *h = (DbHandle *)data;
    if (h && !h->closed && h->db) shard_db_close(h->db);
    free(h);
}
```

Replace with:
```c
static void db_finalizer(napi_env env, void *data, void *hint) {
    (void)hint;
    DbHandle *h = (DbHandle *)data;
    if (h && !h->closed && h->db) shard_db_close(h->db);
    if (h && h->log_fn_ref) napi_delete_reference(env, h->log_fn_ref);
    if (h) pthread_mutex_destroy(&h->log_buf_lock);
    free(h);
}
```

### 7f — Initialise new fields in `napi_open`

Anchor — find the exact block:
```c
    DbHandle *h = malloc(sizeof(DbHandle));
    if (!h) { shard_db_close(db); napi_throw_error(env, NULL, "OOM"); return NULL; }
    h->db = db; h->closed = 0;
```

Replace with:
```c
    DbHandle *h = calloc(1, sizeof(DbHandle));
    if (!h) { shard_db_close(db); napi_throw_error(env, NULL, "OOM"); return NULL; }
    h->db = db; h->closed = 0;
    pthread_mutex_init(&h->log_buf_lock, NULL);
```

### 7g — Drain log buffer at end of `napi_query`

Anchor — find the exact block (end of `napi_query`, before the final `return result;`):
```c
    napi_value result;
    NAPI_CALL(env, napi_create_string_utf8(env, out ? out : "", out_len, &result));
    shard_db_free_result(out);
    return result;
}
```

Replace with:
```c
    napi_value result;
    NAPI_CALL(env, napi_create_string_utf8(env, out ? out : "", out_len, &result));
    shard_db_free_result(out);

    /* Drain log buffer on the JS thread (safe for napi_call_function). */
    if (h->log_fn_ref && h->log_buf_n > 0) {
        pthread_mutex_lock(&h->log_buf_lock);
        int n = h->log_buf_n; h->log_buf_n = 0;
        LogBufEntry msgs[LOG_BUF_CAP];
        memcpy(msgs, h->log_buf, (size_t)n * sizeof(LogBufEntry));
        pthread_mutex_unlock(&h->log_buf_lock);

        napi_value log_fn, global;
        napi_get_reference_value(env, h->log_fn_ref, &log_fn);
        napi_get_global(env, &global);
        for (int i = 0; i < n; i++) {
            napi_value argv[2];
            napi_create_int32(env, msgs[i].type, &argv[0]);
            napi_create_string_utf8(env, msgs[i].msg, NAPI_AUTO_LENGTH, &argv[1]);
            napi_call_function(env, global, log_fn, 2, argv, NULL);
        }
    }

    return result;
}
```

### 7h — Add `napi_set_log_handler` function

Anchor — find the exact line:
```c
/* close(handle) → undefined */
static napi_value napi_close(napi_env env, napi_callback_info info) {
```

Insert immediately before it:
```c
/* setLogHandler(handle, fn | null) → undefined */
static napi_value napi_set_log_handler(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value args[2];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, args, NULL, NULL));

    if (argc < 2) {
        napi_throw_type_error(env, NULL, "setLogHandler() requires (handle, fn|null)");
        return NULL;
    }

    void *data = NULL;
    NAPI_CALL(env, napi_get_value_external(env, args[0], &data));
    DbHandle *h = (DbHandle *)data;

    if (h->log_fn_ref) {
        napi_delete_reference(env, h->log_fn_ref);
        h->log_fn_ref = NULL;
    }

    napi_valuetype vt;
    napi_typeof(env, args[1], &vt);
    if (vt == napi_function) {
        napi_create_reference(env, args[1], 1, &h->log_fn_ref);
        shard_db_set_log_handler(h->db, c_log_handler, h);
    } else {
        shard_db_set_log_handler(h->db, NULL, NULL);
    }

    napi_value undef;
    napi_get_undefined(env, &undef);
    return undef;
}

```

### 7i — Register `setLogHandler` in `Init`

Anchor — find the exact block:
```c
static napi_value Init(napi_env env, napi_value exports) {
    napi_value fn_open, fn_query, fn_close;
    napi_create_function(env, "open",  NAPI_AUTO_LENGTH, napi_open,  NULL, &fn_open);
    napi_create_function(env, "query", NAPI_AUTO_LENGTH, napi_query, NULL, &fn_query);
    napi_create_function(env, "close", NAPI_AUTO_LENGTH, napi_close, NULL, &fn_close);
    napi_set_named_property(env, exports, "open",  fn_open);
    napi_set_named_property(env, exports, "query", fn_query);
    napi_set_named_property(env, exports, "close", fn_close);
    return exports;
}
```

Replace with:
```c
static napi_value Init(napi_env env, napi_value exports) {
    napi_value fn_open, fn_query, fn_close, fn_set_log;
    napi_create_function(env, "open",          NAPI_AUTO_LENGTH, napi_open,            NULL, &fn_open);
    napi_create_function(env, "query",         NAPI_AUTO_LENGTH, napi_query,           NULL, &fn_query);
    napi_create_function(env, "close",         NAPI_AUTO_LENGTH, napi_close,           NULL, &fn_close);
    napi_create_function(env, "setLogHandler", NAPI_AUTO_LENGTH, napi_set_log_handler, NULL, &fn_set_log);
    napi_set_named_property(env, exports, "open",          fn_open);
    napi_set_named_property(env, exports, "query",         fn_query);
    napi_set_named_property(env, exports, "close",         fn_close);
    napi_set_named_property(env, exports, "setLogHandler", fn_set_log);
    return exports;
}
```

Build the npm binding to confirm it compiles:
```
cd npm && npm run build 2>&1 | tail -5
```
Expected: exit 0, no errors.

---

## Task 8 — Add `setLogHandler` to `npm/index.js`

**File:** `npm/index.js`

Anchor — find the exact block:
```c
  close() {
    binding.close(this._handle)
  }
}
```

Insert immediately before the closing `}`:
```js
  setLogHandler(fn) {
    binding.setLogHandler(this._handle, fn ?? null)
  }
```

Result — the class body should end:
```js
  close() {
    binding.close(this._handle)
  }

  setLogHandler(fn) {
    binding.setLogHandler(this._handle, fn ?? null)
  }
}
```

---

## Task 9 — Add TypeScript types to `npm/index.d.ts`

**File:** `npm/index.d.ts`

### 9a — Add `LogType` and `LogHandler` to the namespace

Anchor — find the exact line (first line inside the namespace):
```
  /** Arbitrary field criteria — keys are schema field names, values are
```

Insert immediately before it:
```typescript
  /**
   * Log event type passed to the setLogHandler callback.
   * 1=error  2=warn  3=info  4=debug  5=audit  6=slow-query
   */
  type LogType = 'error' | 'warn' | 'info' | 'debug' | 'audit' | 'slow'

  /** Map from numeric C type to LogType string. */
  const LOG_TYPES: readonly ['', 'error', 'warn', 'info', 'debug', 'audit', 'slow']

  /** Callback signature for setLogHandler. */
  type LogHandler = (type: LogType, msg: string) => void

```

### 9b — Add `setLogHandler` method to the class

Anchor — find the exact block:
```typescript
  /** Close the database and release all resources. */
  close(): void
}
```

Insert immediately before `close()`:
```typescript
  /**
   * Register a callback to receive log events (errors, warnings, slow queries, etc.).
   * The callback fires synchronously after each query() call completes.
   * Pass null to unregister.
   *
   * Log types: 1=error  2=warn  3=info  4=debug  5=audit  6=slow
   * msg is a pre-formatted string: "YYYY-MM-DD HH:MM:SS LEVEL [subsystem] text\n"
   *
   * Note: handler is set after construction; startup logs during new ShardDb()
   * are emitted before the handler is registered and will not be delivered.
   */
  setLogHandler(fn: ShardDb.LogHandler | null): void

```

---

## Task 10 — Add `LOG_TYPES` helper to `npm/index.js`

This lets JS callers map the numeric type to the `LogType` string without
hard-coding the integers.

**File:** `npm/index.js`

Anchor — find the exact line:
```
'use strict'
```

Insert immediately after it (on the next line):
```js

const LOG_TYPES = ['', 'error', 'warn', 'info', 'debug', 'audit', 'slow']
```

Anchor — find the exact line:
```
module.exports = ShardDb
```

Replace with:
```js
ShardDb.LOG_TYPES = LOG_TYPES
module.exports = ShardDb
```

---

## Verification

After all tasks:

```bash
# 1. Full C test suite
./build/bin/shard-db-test run-all
# Expected: # total: N passed, 0 failed

# 2. Quick npm smoke test
cd npm && node -e "
const ShardDb = require('.')
const os = require('os'), path = require('path'), fs = require('fs')
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'shard-log-'))
const db = new ShardDb(dir)
const logs = []
db.setLogHandler((type, msg) => logs.push({ type, msg }))
db.query(JSON.stringify({ mode: 'create-object', dir: 'test', object: 'items',
  splits: 8, max_key: 32, fields: ['name:varchar:64'] }))
db.close()
console.log('logs captured:', logs.length, '(0 is fine for a clean create-object)')
console.log('PASS')
"
```

Expected: exits 0, prints `PASS`.

---

## Usage example for HN Explorer

```typescript
import ShardDb from 'shard-db'

const db = new ShardDb(DB_ROOT)
db.setLogHandler((type, msg) => {
  const label = ShardDb.LOG_TYPES[type] ?? 'unknown'
  if (type === 1 || type === 2) console.error(`[shard-db:${label}]`, msg.trim())
  else if (type === 6)           console.warn(`[shard-db:slow]`, msg.trim())
  // type 3-4 (info/debug): ignore in production
})
```
