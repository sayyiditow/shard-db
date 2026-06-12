# Plan: Async napi_query — Promise-based query API

**Date:** 2026-06-12

## Background

`napi_query` currently calls `shard_db_query` synchronously on the JS main
thread, blocking the event loop for the full query duration.  The fix is to
move the C call onto a libuv worker thread via `napi_create_async_work` and
return a `Promise` to the caller.  `shard_db_query` is already thread-safe
so multiple concurrent queries work without any C-side changes.

The log-buffer drain moves from `napi_query` into the `complete` callback,
which libuv guarantees runs on the JS main thread — same safety property as
before.

## Execution rules

- Branch off `main`: `git checkout -b feat/npm-async-query`
- Do tasks in order.
- Build after Task 2: `cd npm && npm run build` — must exit 0.
- Smoke test after Task 3: `cd npm && node test/basic.js` (or `bun test/basic.js`).
- Never claim a step passed without the real output.
- If a quoted anchor is not found exactly, stop and write `PLAN_NOTES.md`.

---

## Task 1 — Add `QueryWork` struct and worker callbacks to `npm/src/binding.c`

**File:** `npm/src/binding.c`

### 1a — Add `QueryWork` struct

Anchor — find the exact line:
```c
static void c_log_handler(int type, const char *msg, void *ud) {
```

Insert immediately before it:
```c
typedef struct {
    DbHandle       *h;
    char           *json;       /* heap copy of the input JSON string */
    char           *out;        /* result buffer — written by execute */
    size_t          out_len;
    int             rc;         /* return value of shard_db_query */
    napi_deferred   deferred;   /* resolves / rejects the returned Promise */
    napi_async_work work;
} QueryWork;

```

### 1b — Add `execute_query` (runs on libuv worker thread — no JS access)

Anchor — find the exact line (immediately after the QueryWork struct you just inserted):
```c
static void c_log_handler(int type, const char *msg, void *ud) {
```

Insert immediately before it:
```c
static void execute_query(napi_env env, void *data) {
    (void)env;
    QueryWork *w = (QueryWork *)data;
    w->rc = shard_db_query(w->h->db, w->json, &w->out, &w->out_len);
}

```

### 1c — Add `complete_query` (runs on JS main thread)

Anchor — find the exact line:
```c
static void c_log_handler(int type, const char *msg, void *ud) {
```

Insert immediately before it:
```c
static void complete_query(napi_env env, napi_status status, void *data) {
    QueryWork *w = (QueryWork *)data;

    if (status == napi_cancelled || w->h->closed) {
        napi_value msg;
        napi_create_string_utf8(env, "Query cancelled", NAPI_AUTO_LENGTH, &msg);
        napi_value err;
        napi_create_error(env, NULL, msg, &err);
        napi_reject_deferred(env, w->deferred, err);
        goto cleanup;
    }

    if (w->rc != 0) {
        napi_value msg;
        napi_create_string_utf8(env, "shard_db_query allocation failure",
                                NAPI_AUTO_LENGTH, &msg);
        napi_value err;
        napi_create_error(env, NULL, msg, &err);
        napi_reject_deferred(env, w->deferred, err);
        goto cleanup;
    }

    /* Drain log buffer on the JS thread (same logic as the old sync path). */
    if (w->h->log_fn_ref && w->h->log_buf_n > 0) {
        pthread_mutex_lock(&w->h->log_buf_lock);
        int n = w->h->log_buf_n; w->h->log_buf_n = 0;
        LogBufEntry msgs[LOG_BUF_CAP];
        memcpy(msgs, w->h->log_buf, (size_t)n * sizeof(LogBufEntry));
        pthread_mutex_unlock(&w->h->log_buf_lock);

        napi_value log_fn, global;
        napi_get_reference_value(env, w->h->log_fn_ref, &log_fn);
        napi_get_global(env, &global);
        for (int i = 0; i < n; i++) {
            napi_value argv[2];
            napi_create_int32(env, msgs[i].type, &argv[0]);
            napi_create_string_utf8(env, msgs[i].msg, NAPI_AUTO_LENGTH, &argv[1]);
            napi_call_function(env, global, log_fn, 2, argv, NULL);
        }
    }

    {
        napi_value result;
        napi_create_string_utf8(env, w->out ? w->out : "", w->out_len, &result);
        shard_db_free_result(w->out);
        w->out = NULL;
        napi_resolve_deferred(env, w->deferred, result);
    }

cleanup:
    napi_delete_async_work(env, w->work);
    free(w->json);
    free(w);
}

```

---

## Task 2 — Replace the body of `napi_query` in `npm/src/binding.c`

**File:** `npm/src/binding.c`

Anchor — find the exact block (the entire `napi_query` function body after argument parsing through to end):
```c
/* query(handle, json: string) → string */
static napi_value napi_query(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value args[2];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, args, NULL, NULL));

    if (argc < 2) {
        napi_throw_type_error(env, NULL, "query() requires (handle, json)");
        return NULL;
    }

    void *data = NULL;
    NAPI_CALL(env, napi_get_value_external(env, args[0], &data));
    DbHandle *h = (DbHandle *)data;

    if (h->closed) {
        napi_throw_error(env, NULL, "Database is closed");
        return NULL;
    }

    size_t len = 0;
    NAPI_CALL(env, napi_get_value_string_utf8(env, args[1], NULL, 0, &len));
    char *json = malloc(len + 1);
    if (!json) { napi_throw_error(env, NULL, "OOM"); return NULL; }
    NAPI_CALL(env, napi_get_value_string_utf8(env, args[1], json, len + 1, &len));

    char  *out     = NULL;
    size_t out_len = 0;
    int rc = shard_db_query(h->db, json, &out, &out_len);
    free(json);

    if (rc != 0) {
        napi_throw_error(env, NULL, "shard_db_query allocation failure");
        return NULL;
    }

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

Replace the entire function with:
```c
/* query(handle, json: string) → Promise<string> */
static napi_value napi_query(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value args[2];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, args, NULL, NULL));

    if (argc < 2) {
        napi_throw_type_error(env, NULL, "query() requires (handle, json)");
        return NULL;
    }

    void *data = NULL;
    NAPI_CALL(env, napi_get_value_external(env, args[0], &data));
    DbHandle *h = (DbHandle *)data;

    if (h->closed) {
        napi_throw_error(env, NULL, "Database is closed");
        return NULL;
    }

    size_t len = 0;
    NAPI_CALL(env, napi_get_value_string_utf8(env, args[1], NULL, 0, &len));
    char *json = malloc(len + 1);
    if (!json) { napi_throw_error(env, NULL, "OOM"); return NULL; }
    NAPI_CALL(env, napi_get_value_string_utf8(env, args[1], json, len + 1, &len));

    QueryWork *w = calloc(1, sizeof(QueryWork));
    if (!w) { free(json); napi_throw_error(env, NULL, "OOM"); return NULL; }
    w->h    = h;
    w->json = json;

    napi_value promise;
    NAPI_CALL(env, napi_create_promise(env, &w->deferred, &promise));

    napi_value resource_name;
    napi_create_string_utf8(env, "shard_db_query", NAPI_AUTO_LENGTH, &resource_name);
    napi_create_async_work(env, NULL, resource_name,
                           execute_query, complete_query, w, &w->work);
    napi_queue_async_work(env, w->work);

    return promise;
}
```

Build: `cd npm && npm run build` — must exit 0, no errors.

---

## Task 3 — Update TypeScript types in `npm/index.d.ts`

**File:** `npm/index.d.ts`

### 3a — Change `query()` return type

Anchor — find the exact block:
```typescript
  query(body: ShardDb.QueryBody): string
  query(json: string): string
```

Replace with:
```typescript
  query(body: ShardDb.QueryBody): Promise<string>
  query(json: string): Promise<string>
```

### 3b — Update the JSDoc comment for `query()`

Anchor — find the exact block:
```typescript
  /**
   * Execute a query synchronously.
   * Accepts a typed QueryBody object (recommended — enables autocomplete)
   * or a raw JSON string (backward compatible).
   * Returns the JSON response string. Parse with JSON.parse().
   * Thread-safe: multiple threads may call concurrently on the same instance.
   */
```

Replace with:
```typescript
  /**
   * Execute a query asynchronously.
   * Accepts a typed QueryBody object (recommended — enables autocomplete)
   * or a raw JSON string (backward compatible).
   * Returns a Promise that resolves to the JSON response string. Parse with JSON.parse().
   * Multiple concurrent queries are safe — shard-db's worker pool handles parallelism.
   */
```

---

## Task 4 — Update `npm/test/basic.js` to await queries

**File:** `npm/test/basic.js`

Read the file first to identify every `db.query(` call.  For each one:
- If the call result is assigned: `const x = db.query(...)` → `const x = await db.query(...)`
- If the call result is passed directly: `JSON.parse(db.query(...))` → `JSON.parse(await db.query(...))`
- Wrap the test body in an `async` function if it isn't already.

The exact replacements depend on the current content of `test/basic.js` —
read the file and apply the `await` additions surgically without changing
any other logic.

Build and run:
```bash
cd npm && node test/basic.js
```
Expected: exits 0, prints results consistent with the existing assertions.

---

## Invariants

- `execute_query` must never call any `napi_*` function — it runs on a
  worker thread with no JS access.
- `complete_query` is guaranteed by libuv to run on the JS main thread,
  so `napi_call_function` (log drain) and `napi_resolve_deferred` are safe.
- `QueryWork` is heap-allocated and freed inside `complete_query` after
  `napi_delete_async_work` — never freed earlier.
- `w->json` is freed inside `complete_query` on all paths (success,
  error, cancel).
- If `napi_queue_async_work` is never called (early error return before
  it), `w` and `w->json` must be freed at that point — the replacement
  function handles this: both OOM paths free before returning.
