# Plan: slow query logging in embedded mode

**Bug:** `shard_db_query` in `embedded.c` calls `dispatch_json_query` with no timing wrapper,
so slow queries are never detected or logged in embedded / npm mode.

**Root cause:** The slow query detection lives in `server_process_fast` (server.c:1943–2141),
which is only called on the TCP path. Embedded mode calls `dispatch_json_query` directly and
skips it entirely.

**Why the fix is safe:** `log_slow_query` (config.c:269) already has an embedded-mode fast path
at line 311–313: when no log thread is running it calls `g_db->log_handler(6, msg, ud)` directly.
The npm binding's `c_log_handler` buffers that call and the buffer is drained on the JS thread
after each query completes. Everything downstream is already correct — only the call site is missing.

**Execution rules:**
- Branch off main: `git checkout -b fix/embedded-slow-query`
- Build: `SKIP_TESTS=1 ./build.sh` — confirm zero errors
- Test: `./build/bin/shard-db-test run-all` — confirm `# total: N passed, 0 failed`
- Do not guess or reinterpret anchor text; if any quoted anchor is not found exactly, stop and
  write `PLAN_NOTES.md`

---

## Task 1 — Edit `src/db/embedded.c`

**Anchor:** the exact text to find is:

```c
    g_out = mf;
    dispatch_json_query(db->db_root, json, "127.0.0.1");
    fflush(mf);
```

Replace it with:

```c
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
```

**Invariants:**
- `g_slow_query_ms` is a global set by `load_db_root()` (config.c) from `SLOW_QUERY_MS` in
  db.env. It is `0` when slow query logging is disabled; only time when `> 0`.
- `now_ms()` is already used in embedded.c (line 90: `db->server_start_ms = now_ms()`), so
  it is available without any new includes.
- `json_parse_object`, `json_obj_strdup`, `JsonObj` are all declared in `types.h` which is
  already included by embedded.c.
- `log_slow_query` is declared in `types.h` (line 556) and defined in `config.c`, both already
  in scope.
- Prefix local variables with `_` to avoid any shadowing with names inside `dispatch_json_query`.

---

## Task 2 — Build

```bash
SKIP_TESTS=1 ./build.sh
```

Confirm: exits 0, no compiler errors or warnings about the new code.

---

## Task 3 — Run tests

```bash
./build/bin/shard-db-test run-all
```

Confirm: `# total: N passed, 0 failed`. All existing tests must pass — no new test is required
for this fix because the slow query path is exercised end-to-end by the integration layer
(no existing test currently exercises `SLOW_QUERY_MS` timing in embedded mode specifically).

---

## Task 4 — Leave work uncommitted

Do NOT commit. Leave the change staged or unstaged on the branch `fix/embedded-slow-query`.
The reviewer (Sonnet) will inspect `git diff` before any commit happens.
