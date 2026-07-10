# NQL parser input validation

## Problem

Three silent-failure bugs in the NQL parser produce wrong results with no
error:

1. **`not` + invalid operator silently becomes equality.** When the parser
   sees `not`, it reads the next identifier and builds `"not_<ident>"`. If
   the result is not a recognized operator (e.g. `not foo`), `parse_op()`
   returns `OP_EQUAL` (the catch-all). The negation is silently dropped and
   the query becomes a plain equality match. This is already fixed by the
   criteria plan's `parse_op()` change (returning `OP_UNKNOWN`). This plan
   adds the NQL-side propagation.

2. **`--limit` / `--offset` with non-numeric values silently become 0.**
   `atoi("abc")` returns 0 per C standard. For `--limit`, 0 means "no limit"
   (unlimited results). The user thinks they set a limit but gets all records.
   For `--offset`, 0 is the default — the user thinks they skipped records
   but didn't.

3. **`--order-by :desc` (leading colon) makes `order_by` empty.** The
   `strrchr(spec, ':')` finds the colon at position 0, NUL-writes spec to
   `""`, and `order_by` becomes empty. Downstream, empty `order_by` is treated
   as NULL — ordering is silently dropped.

## Scope

- Propagate `OP_UNKNOWN` error from `parse_op()` through NQL filter parser
- Validate `--limit` and `--offset` are positive integers
- Validate `--order-by` spec is not empty after colon split
- Accept both cases for `--order` direction

## Execution rules

- Branch off `main`: `git checkout -b fix/nql-input-validation`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all` — must pass after every task
- Leave uncommitted; stop for review after each task
- Every anchor below is a verbatim quote from current `main`. If any quoted
  anchor is not found exactly as written, stop and write `PLAN_NOTES.md` —
  do not guess or reinterpret.

**Note:** The `parse_op()` change (returning `OP_UNKNOWN` instead of
`OP_EQUAL`) is part of the criteria plan. This plan depends on that change
being merged first. Tasks 1-2 assume `parse_op()` already returns
`OP_UNKNOWN`.

## Task 1 — Propagate `OP_UNKNOWN` through NQL filter parser

Locate the anchor (unique in file — the line where `parse_op` is called
in the NQL filter parser):

```c
    node->leaf.op = parse_op(op_str);
```

Replace with:

```c
    node->leaf.op = parse_op(op_str);
    if (node->leaf.op == OP_UNKNOWN) {
        snprintf(l->err, sizeof l->err, "unknown operator '%s'", op_str);
        free(node);
        return NULL;
    }
```

This catches both direct invalid ops (e.g. `field foo value`) and the
`not` + invalid case (e.g. `field not foo value` builds `"not_foo"` which
`parse_op` returns `OP_UNKNOWN` for).

Build + run-all. Must be green.

## Task 2 — Validate `--limit` and `--offset` are positive integers

Locate the anchor (unique in file — the `--limit` and `--offset` parsing
inside `nql_parse_command`):

```c
        if      (!strcmp(argv[i],"--limit")    && i+1<argc) { out->limit   = atoi(argv[++i]); i++; }
        else if (!strcmp(argv[i],"--offset")   && i+1<argc) { out->offset  = atoi(argv[++i]); i++; }
```

Replace with:

```c
        if      (!strcmp(argv[i],"--limit")    && i+1<argc) {
            int v = atoi(argv[++i]);
            if (v < 0) { snprintf(l->err, sizeof l->err, "--limit must be non-negative, got '%s'", argv[i]); return -1; }
            out->limit = v; i++;
        }
        else if (!strcmp(argv[i],"--offset")   && i+1<argc) {
            int v = atoi(argv[++i]);
            if (v < 0) { snprintf(l->err, sizeof l->err, "--offset must be non-negative, got '%s'", argv[i]); return -1; }
            out->offset = v; i++;
        }
```

Build + run-all. Must be green.

## Task 3 — Validate `--order-by` spec is not empty after colon split

Locate the anchor (unique in file — the `--order-by` parsing with
`strrchr` colon split):

```c
        else if (!strcmp(argv[i],"--order-by") && i+1<argc) {
            char *spec = argv[++i]; i++;
            char *colon = strrchr(spec, ':');
            if (colon) { *colon = '\0'; snprintf(out->order_dir,sizeof out->order_dir,"%s",colon+1); }
            else         snprintf(out->order_dir,sizeof out->order_dir,"asc");
            snprintf(out->order_by, sizeof out->order_by, "%s", spec);
        }
```

Replace with:

```c
        else if (!strcmp(argv[i],"--order-by") && i+1<argc) {
            char *spec = argv[++i]; i++;
            char *colon = strrchr(spec, ':');
            if (colon) {
                *colon = '\0';
                snprintf(out->order_dir,sizeof out->order_dir,"%s",colon+1);
                if (spec[0] == '\0') {
                    snprintf(l->err, sizeof l->err, "--order-by requires a field name before ':'");
                    return -1;
                }
            }
            else         snprintf(out->order_dir,sizeof out->order_dir,"asc");
            snprintf(out->order_by, sizeof out->order_by, "%s", spec);
        }
```

Build + run-all. Must be green.

## Task 4 — Accept both cases for `--order` direction

Locate the anchor (unique in file — the `--order` parsing):

```c
        else if (!strcmp(argv[i],"--order")     && i+1<argc) { snprintf(out->order_dir,sizeof out->order_dir,"%s",argv[++i]); i++; }
```

Replace with:

```c
        else if (!strcmp(argv[i],"--order")     && i+1<argc) {
            const char *d = argv[++i];
            if (strcmp(d,"desc")==0 || strcmp(d,"DESC")==0 || strcmp(d,"Desc")==0)
                snprintf(out->order_dir,sizeof out->order_dir,"desc");
            else if (strcmp(d,"asc")==0 || strcmp(d,"ASC")==0 || strcmp(d,"Asc")==0)
                snprintf(out->order_dir,sizeof out->order_dir,"asc");
            else { snprintf(l->err, sizeof l->err, "invalid order direction '%s'; use 'asc' or 'desc'", d); return -1; }
            i++;
        }
```

Build + run-all. Must be green.

## Task 5 — Add tests

Add one new test-case file under `src/test/cases/`.

### 5a — `src/test/cases/test_nql_input_validation.c`

```c
/* src/test/cases/test_nql_input_validation.c
 * Regression: NQL unknown operator must error (not silently become equality).
 * Non-numeric --limit/--offset must error.  Empty order_by after colon
 * split must error.  Invalid order direction must error.
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

static int test_nql_input_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"nql_val_t\","
        "\"fields\":[\"name:varchar:32\",\"score:int\"],"
        "\"indexes\":[\"score\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"nql_val_t\",\"records\":{"
        "\"k1\":{\"name\":\"alice\",\"score\":10},"
        "\"k2\":{\"name\":\"bob\",\"score\":90}"
        "}}", &resp); free(resp); resp = NULL;

    /* 1. Unknown NQL operator returns error */
    tc_request(tc,
        "find default nql_val_t 'score foobar 50'",
        &resp);
    ASSERT_NOT_NULL(resp, "unknown NQL op response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "unknown NQL op returns error");
    free(resp); resp = NULL;

    /* 2. not + unknown op returns error (not equality) */
    tc_request(tc,
        "find default nql_val_t 'score not foo 50'",
        &resp);
    ASSERT_NOT_NULL(resp, "not+unknown op response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "not+unknown op returns error");
    free(resp); resp = NULL;

    /* 3. Non-numeric --limit returns error */
    tc_request(tc,
        "find default nql_val_t 'score gt 0' --limit abc",
        &resp);
    ASSERT_NOT_NULL(resp, "non-numeric limit response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "non-numeric limit returns error");
    free(resp); resp = NULL;

    /* 4. Negative --offset returns error */
    tc_request(tc,
        "find default nql_val_t 'score gt 0' --offset -5",
        &resp);
    ASSERT_NOT_NULL(resp, "negative offset response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "negative offset returns error");
    free(resp); resp = NULL;

    /* 5. --order-by :desc (leading colon) returns error */
    tc_request(tc,
        "find default nql_val_t 'score gt 0' --order-by :desc",
        &resp);
    ASSERT_NOT_NULL(resp, "leading colon response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "leading colon order-by returns error");
    free(resp); resp = NULL;

    /* 6. Invalid --order direction returns error */
    tc_request(tc,
        "find default nql_val_t 'score gt 0' --order sideways",
        &resp);
    ASSERT_NOT_NULL(resp, "invalid order direction response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "invalid order direction returns error");
    free(resp); resp = NULL;

    /* 7. Valid NQL still works (baseline) */
    tc_request(tc,
        "find default nql_val_t 'score gt 50'",
        &resp);
    ASSERT_NOT_NULL(resp, "valid NQL response not null");
    ASSERT_CONTAINS(resp, "\"name\":\"bob\"", "valid NQL returns bob");
    ASSERT_TRUE(strstr(resp, "\"name\":\"alice\"") == NULL,
                "valid NQL excludes alice");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-input-validation", test_nql_input_validation_run)
```

Build + run-all. Confirm the `# total:` line matches pre-change count + 1.

## Task 6 — Register the new test file in `build.sh`

Locate the anchor (last line of the test-case file list, unique in file):

```
    src/test/cases/test_coverity_join_buf_overflow.c \
```

Replace with:

```
    src/test/cases/test_coverity_join_buf_overflow.c \
    src/test/cases/test_nql_input_validation.c \
```

Build + run-all. Confirm the `# total:` line matches pre-Task-5 count + 1,
and that `./build/bin/shard-db-test list` shows the new case name.

## Files changed

| File | Change |
|---|---|
| `src/db/nql.c` | Propagate `OP_UNKNOWN` error in filter parser. Validate `--limit`/`--offset` non-negative. Validate `--order-by` not empty after colon split. Validate `--order` direction. |
| `src/test/cases/test_nql_input_validation.c` | New — unknown op, not+unknown, non-numeric limit, negative offset, empty order-by, invalid direction. |
| `build.sh` | Add the new test-case file to the `shard-db-test` gcc link line. |

**Depends on:** criteria plan (2026-07-10-criteria-op-alias-and-validation.md)
for the `parse_op()` → `OP_UNKNOWN` change.
