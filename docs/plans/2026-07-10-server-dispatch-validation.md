# Server dispatch input validation

## Problem

Five silent-failure bugs in `dispatch_json_query()` mode handlers produce
wrong results, empty responses, or worker-thread hangs with no error:

1. **`add-index` missing both `field` and `fields`.** Neither branch fires;
   the handler returns without emitting any response. The client receives an
   empty/zero-byte response. Compare with `remove-index` which properly emits
   `{"error":"Missing field or fields"}`.

2. **`get-file-path` missing `filename`.** The `if (filename)` check skips
   the call and returns without any output. Compare with `get-file` and
   `delete-file` which both emit `{"error":"filename is required"}`.

3. **`bulk-insert` missing both `file` and `records`.** Falls through to
   `cmd_bulk_insert(db_root, object, NULL, ifne)` which reads from `stdin`
   via `fgetc(stdin)`. On a TCP connection, stdin is not the client socket —
   the worker thread hangs indefinitely.

4. **`bulk-delete` key-list path missing both `file` and `keys`.** Same
   stdin-hang as bulk-insert.

5. **`find`/`fetch`/`keys` negative `offset`.** `atoi("-5")` returns -5,
   passed directly to the command function. Downstream, a negative offset
   is silently equivalent to offset=0 — wrong results with no error.

## Scope

- Add `else` error branches to `add-index`, `get-file-path`, `bulk-insert`,
  `bulk-delete` mode handlers (mirroring neighboring modes)
- Validate offset is non-negative in `find`, `fetch`, `keys` handlers

## Execution rules

- Branch off `main`: `git checkout -b fix/server-dispatch-validation`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all` — must pass after every task
- Leave uncommitted; stop for review after each task
- Every anchor below is a verbatim quote from current `main`. If any quoted
  anchor is not found exactly as written, stop and write `PLAN_NOTES.md` —
  do not guess or reinterpret.

## Task 1 — Add error branch to `add-index` handler

Locate the anchor (unique in file — the `add-index` handler with the
`if (fields_arr)` / `else if (field)` chain):

```c
    } else if (strcmp(mode, "add-index") == 0) {
        char *field = json_obj_strdup(&req, "field");
        char *fields_arr = json_obj_strdup_raw(&req, "fields");
        int f = json_obj_is_true(&req, "force");
        if (fields_arr)
            cmd_add_indexes(db_root, object, fields_arr, f);
        else if (field)
            cmd_add_index(db_root, object, field, f);
        free(field); free(fields_arr);
```

Replace with:

```c
    } else if (strcmp(mode, "add-index") == 0) {
        char *field = json_obj_strdup(&req, "field");
        char *fields_arr = json_obj_strdup_raw(&req, "fields");
        int f = json_obj_is_true(&req, "force");
        if (fields_arr)
            cmd_add_indexes(db_root, object, fields_arr, f);
        else if (field)
            cmd_add_index(db_root, object, field, f);
        else
            OUT("{\"error\":\"Missing field or fields\"}\n");
        free(field); free(fields_arr);
```

Mirrors the existing pattern in `remove-index` at line 1647.

Build + run-all. Must be green.

## Task 2 — Add error branch to `get-file-path` handler

Locate the anchor (unique in file — the `get-file-path` handler):

```c
    } else if (strcmp(mode, "get-file-path") == 0) {
        char *filename = json_obj_strdup(&req, "filename");
        if (filename) cmd_get_file_path(db_root, object, filename);
        free(filename);
```

Replace with:

```c
    } else if (strcmp(mode, "get-file-path") == 0) {
        char *filename = json_obj_strdup(&req, "filename");
        if (filename) cmd_get_file_path(db_root, object, filename);
        else OUT("{\"error\":\"filename is required\"}\n");
        free(filename);
```

Mirrors the existing pattern in `get-file` and `delete-file`.

Build + run-all. Must be green.

## Task 3 — Add error branch to `bulk-insert` handler

Locate the anchor (unique in file — the `bulk-insert` handler with the
`if (records)` / `else` chain):

```c
    } else if (strcmp(mode, "bulk-insert") == 0) {
        char *file = json_obj_strdup(&req, "file");
        char *records = json_obj_strdup_raw(&req, "records");
        int ifne = json_obj_is_true(&req, "if_not_exists");
        /* cmd_bulk_insert is auto-key aware end-to-end as of 2026.05.5
           — normalises provided wire keys + generates omit keys in
           batch before running the standard parallel pipeline. The
           server.c dispatcher is now a thin router. */
        if (records) {
            cmd_bulk_insert_string(db_root, object, records, ifne);
            free(records);
        } else {
            cmd_bulk_insert(db_root, object, file, ifne);
        }
        free(file);
```

Replace with:

```c
    } else if (strcmp(mode, "bulk-insert") == 0) {
        char *file = json_obj_strdup(&req, "file");
        char *records = json_obj_strdup_raw(&req, "records");
        int ifne = json_obj_is_true(&req, "if_not_exists");
        /* cmd_bulk_insert is auto-key aware end-to-end as of 2026.05.5
           — normalises provided wire keys + generates omit keys in
           batch before running the standard parallel pipeline. The
           server.c dispatcher is now a thin router. */
        if (records) {
            cmd_bulk_insert_string(db_root, object, records, ifne);
            free(records);
        } else if (file) {
            cmd_bulk_insert(db_root, object, file, ifne);
        } else {
            OUT("{\"error\":\"bulk-insert requires records or file\"}\n");
        }
        free(file);
```

Mirrors the pattern in `bulk-insert-delimited` at line 1683.

Build + run-all. Must be green.

## Task 4 — Add error branch to `bulk-delete` key-list handler

Locate the anchor (unique in file — the `bulk-delete` key-list path with
`if (keys)` / `else` chain):

```c
            char *file = json_obj_strdup(&req, "file");
            char *keys = json_obj_strdup_raw(&req, "keys");
            if (keys) {
                cmd_bulk_delete_string(db_root, object, keys);
                /* keys ownership transferred — bulk_delete_run frees it. */
            } else {
                cmd_bulk_delete(db_root, object, file);
            }
            free(file);
```

Replace with:

```c
            char *file = json_obj_strdup(&req, "file");
            char *keys = json_obj_strdup_raw(&req, "keys");
            if (keys) {
                cmd_bulk_delete_string(db_root, object, keys);
                /* keys ownership transferred — bulk_delete_run frees it. */
            } else if (file) {
                cmd_bulk_delete(db_root, object, file);
            } else {
                OUT("{\"error\":\"bulk-delete requires keys or file\"}\n");
            }
            free(file);
```

Mirrors the pattern in `bulk-update` at line 1736.

Build + run-all. Must be green.

## Task 5 — Validate non-negative offset in `find`, `fetch`, `keys`

### 5a — `find` mode

Locate the anchor (unique in file — the `find` mode offset/limit parsing):

```c
        int off = off_s ? atoi(off_s) : 0;
        int lim = lim_s ? atoi(lim_s) : 0;
```

Add validation after the `atoi` calls. Locate the line immediately after
these two lines (it is the `if (crit)` block). Insert before it:

```c
        if (off < 0) {
            OUT("{\"error\":\"offset must not be negative\"}\n");
            free(criteria); free(off_s); free(lim_s); free(fields);
            free(excl); free(fmt); free(delim); free(join);
            free(ob); free(od); free(cur);
            return;
        }
```

Note: `free(NULL)` is safe per C standard. Adjust the free list to match
whatever variables are in scope at that point. The exact variable names
depend on the surrounding code — check the declaration block above.

### 5b — `fetch` mode

Locate the anchor (unique in file — the `fetch` mode handler):

```c
        cmd_fetch(db_root, object, off_s ? atoi(off_s) : 0, lim_s ? atoi(lim_s) : 0, fields, cur, fmt, delim, want_total);
```

Replace with:

```c
        {
            int off = off_s ? atoi(off_s) : 0;
            if (off < 0) {
                OUT("{\"error\":\"offset must not be negative\"}\n");
                free(off_s); free(lim_s); free(fields); free(cur);
                free(fmt); free(delim);
            } else {
                cmd_fetch(db_root, object, off, lim_s ? atoi(lim_s) : 0, fields, cur, fmt, delim, want_total);
            }
        }
```

### 5c — `keys` mode

Locate the anchor (unique in file — the `keys` mode handler):

```c
        cmd_keys(db_root, object, off_s ? atoi(off_s) : 0, lim_s ? atoi(lim_s) : 0, fmt, delim);
```

Replace with:

```c
        {
            int off = off_s ? atoi(off_s) : 0;
            if (off < 0) {
                OUT("{\"error\":\"offset must not be negative\"}\n");
                free(off_s); free(lim_s); free(fmt); free(delim);
            } else {
                cmd_keys(db_root, object, off, lim_s ? atoi(lim_s) : 0, fmt, delim);
            }
        }
```

Build + run-all. Must be green.

## Task 6 — Add tests

Add one new test-case file under `src/test/cases/`.

### 6a — `src/test/cases/test_server_dispatch_validation.c`

```c
/* src/test/cases/test_server_dispatch_validation.c
 * Regression: missing required fields must return error (not empty
 * response or worker hang).  Negative offset must return error.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int test_server_dispatch_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"sdv_t\","
        "\"fields\":[\"name:varchar:32\"],\"indexes\":[],\"splits\":8}",
        &resp); free(resp); resp = NULL;

    /* 1. add-index missing field/fields returns error */
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"sdv_t\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "add-index missing field response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "add-index missing field returns error");
    free(resp); resp = NULL;

    /* 2. get-file-path missing filename returns error */
    tc_request(tc,
        "{\"mode\":\"get-file-path\",\"dir\":\"default\",\"object\":\"sdv_t\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "get-file-path missing filename response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "get-file-path missing filename returns error");
    free(resp); resp = NULL;

    /* 3. bulk-insert missing records/file returns error (not hang) */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"sdv_t\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "bulk-insert missing records response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "bulk-insert missing records returns error");
    free(resp); resp = NULL;

    /* 4. bulk-delete key-list missing keys/file returns error (not hang) */
    tc_request(tc,
        "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"sdv_t\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "bulk-delete missing keys response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "bulk-delete missing keys returns error");
    free(resp); resp = NULL;

    /* 5. find with negative offset returns error */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"sdv_t\","
        "\"offset\":-5,\"limit\":10}",
        &resp);
    ASSERT_NOT_NULL(resp, "negative offset response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "negative offset returns error");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-server-dispatch-validation", test_server_dispatch_validation_run)
```

Build + run-all. Confirm the `# total:` line matches pre-change count + 1.

## Task 7 — Register the new test file in `build.sh`

Locate the anchor (last line of the test-case file list, unique in file):

```
    src/test/cases/test_coverity_join_buf_overflow.c \
```

Replace with:

```
    src/test/cases/test_coverity_join_buf_overflow.c \
    src/test/cases/test_server_dispatch_validation.c \
```

Build + run-all. Confirm the `# total:` line matches pre-Task-6 count + 1,
and that `./build/bin/shard-db-test list` shows the new case name.

## Files changed

| File | Change |
|---|---|
| `src/db/server.c` | Add `else` error branches to `add-index`, `get-file-path`, `bulk-insert`, `bulk-delete`. Add negative-offset validation to `find`, `fetch`, `keys`. |
| `src/test/cases/test_server_dispatch_validation.c` | New — missing field/filename/records/keys errors, negative offset error. |
| `build.sh` | Add the new test-case file to the `shard-db-test` gcc link line. |
