# Aggregate mode input validation

## Problem

Three silent-failure bugs in aggregate mode produce wrong results with no error:

1. **Unknown aggregate function name silently becomes `AGG_COUNT`.** Both the
   NQL path (`cmd_aggregate_tree`) and JSON path (`parse_agg_specs`) map
   function names via a strcmp chain. Unrecognized names fall through to
   `AGG_COUNT` (enum value 0) because `AggSpec` is zero-initialized by
   `calloc`. A user typing `median(age)` gets `count(age)` with no error.

2. **Invalid `order_by` alias silently sorts by 0.0.** `agg_bucket_value()`
   returns `0.0` when the alias does not match any aggregate spec. Every
   bucket gets the same sort key, so `qsort` produces an arbitrary order.
   A typo like `"order_by":"totl_sum"` gives wrong ordering with no error.

3. **Uppercase `"DESC"` in aggregate order is silently treated as ascending.**
   `server.c` compares `order` with `strcmp(od, "desc")` — case-sensitive.
   `"DESC"` or `"Desc"` is ignored, defaulting to ascending.

## Scope

- Reject unrecognized aggregate function names in both NQL and JSON paths
- Reject unrecognized `order_by` aliases in aggregate sort
- Accept both cases for `"desc"` direction in aggregate order

## Execution rules

- Branch off `main`: `git checkout -b fix/agg-input-validation`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all` — must pass after every task
- Leave uncommitted; stop for review after each task
- Every anchor below is a verbatim quote from current `main`. If any quoted
  anchor is not found exactly as written, stop and write `PLAN_NOTES.md` —
  do not guess or reinterpret.

## Task 1 — Reject unknown aggregate function names in `cmd_aggregate_tree()`

Locate the anchor (unique in file — the fn-to-enum mapping loop inside
`cmd_aggregate_tree`):

```c
    for (int i = 0; i < naggs; i++) {
        if (strcmp(aggs[i].fn, "count") == 0) specs[i].fn = AGG_COUNT;
        else if (strcmp(aggs[i].fn, "sum") == 0) specs[i].fn = AGG_SUM;
        else if (strcmp(aggs[i].fn, "avg") == 0) specs[i].fn = AGG_AVG;
        else if (strcmp(aggs[i].fn, "min") == 0) specs[i].fn = AGG_MIN;
        else if (strcmp(aggs[i].fn, "max") == 0) specs[i].fn = AGG_MAX;
        strncpy(specs[i].field, aggs[i].field, sizeof(specs[i].field) - 1);
```

Replace with:

```c
    for (int i = 0; i < naggs; i++) {
        if (strcmp(aggs[i].fn, "count") == 0) specs[i].fn = AGG_COUNT;
        else if (strcmp(aggs[i].fn, "sum") == 0) specs[i].fn = AGG_SUM;
        else if (strcmp(aggs[i].fn, "avg") == 0) specs[i].fn = AGG_AVG;
        else if (strcmp(aggs[i].fn, "min") == 0) specs[i].fn = AGG_MIN;
        else if (strcmp(aggs[i].fn, "max") == 0) specs[i].fn = AGG_MAX;
        else {
            OUT("{\"error\":\"unknown aggregate function '%s'; use count, sum, avg, min, or max\"}\n",
                aggs[i].fn);
            free(specs);
            return -1;
        }
        strncpy(specs[i].field, aggs[i].field, sizeof(specs[i].field) - 1);
```

Build + run-all. Must be green.

## Task 2 — Reject unknown aggregate function names in `parse_agg_specs()` (JSON path)

Locate the anchor (unique in file — the fn-to-enum mapping inside
`parse_agg_specs`):

```c
        if (fn) {
            if (strcmp(fn, "count") == 0) s->fn = AGG_COUNT;
            else if (strcmp(fn, "sum") == 0) s->fn = AGG_SUM;
            else if (strcmp(fn, "avg") == 0) s->fn = AGG_AVG;
            else if (strcmp(fn, "min") == 0) s->fn = AGG_MIN;
            else if (strcmp(fn, "max") == 0) s->fn = AGG_MAX;
        }
```

Replace with:

```c
        if (fn) {
            if (strcmp(fn, "count") == 0) s->fn = AGG_COUNT;
            else if (strcmp(fn, "sum") == 0) s->fn = AGG_SUM;
            else if (strcmp(fn, "avg") == 0) s->fn = AGG_AVG;
            else if (strcmp(fn, "min") == 0) s->fn = AGG_MIN;
            else if (strcmp(fn, "max") == 0) s->fn = AGG_MAX;
            else { free(fn); free(alias); free(field); free_specs(s, count + 1); return 0; }
        }
```

This causes `parse_agg_specs` to return `nspecs=0` for any unrecognized
function, which the caller (`cmd_aggregate`) already rejects with
`{"error":"No valid aggregates"}`.

Build + run-all. Must be green.

## Task 3 — Reject unrecognized `order_by` alias in aggregate sort

Locate the anchor (unique in file — the sort setup block right before
`qsort` in `cmd_aggregate_do`):

```c
    if (order_by && order_by[0]) {
        g_sort_specs = specs;
        g_sort_nspecs = nspecs;
        strncpy(g_sort_field, order_by, 255);
        g_sort_desc = order_desc;
        g_sort_ngroups = ctx.ngroups;
        g_sort_group_fields = ctx.group_fields;
        qsort(buckets, nbuckets, sizeof(AggBucket *), agg_sort_cmp);
    }
```

Replace with:

```c
    if (order_by && order_by[0]) {
        /* Validate order_by: must be a group_by field or an aggregate alias */
        int valid_order = 0;
        for (int i = 0; i < ctx.ngroups; i++)
            if (strcmp(ctx.group_fields[i], order_by) == 0) { valid_order = 1; break; }
        if (!valid_order)
            for (int i = 0; i < nspecs; i++)
                if (strcmp(specs[i].alias, order_by) == 0) { valid_order = 1; break; }
        if (!valid_order) {
            OUT("{\"error\":\"order_by '%s' is not a group_by field or aggregate alias\"}\n",
                order_by);
            free(buckets);
            free(bucket_storage);
            return -1;
        }
        g_sort_specs = specs;
        g_sort_nspecs = nspecs;
        strncpy(g_sort_field, order_by, 255);
        g_sort_desc = order_desc;
        g_sort_ngroups = ctx.ngroups;
        g_sort_group_fields = ctx.group_fields;
        qsort(buckets, nbuckets, sizeof(AggBucket *), agg_sort_cmp);
    }
```

Build + run-all. Must be green.

## Task 4 — Accept uppercase `"DESC"` in aggregate order direction

Locate the anchor (unique in file — the `order` field parsing inside
`dispatch_json_query`, aggregate branch):

```c
            int desc = (od && strcmp(od, "desc") == 0);
```

Replace with:

```c
            int desc = (od && (strcmp(od, "desc") == 0 || strcasecmp(od, "desc") == 0));
```

Note: the existing code in `query.c` for the find path already does:
```c
int desc = (order_dir && (strcmp(order_dir, "desc") == 0 || strcmp(order_dir, "DESC") == 0));
```
Mirror that pattern if `strcasecmp` is not available.

Build + run-all. Must be green.

## Task 5 — Add tests

Add one new test-case file under `src/test/cases/`.

### 5a — `src/test/cases/test_agg_input_validation.c`

```c
/* src/test/cases/test_agg_input_validation.c
 * Regression: unknown aggregate function must error (not silently become
 * count).  Invalid order_by alias must error (not sort by 0.0).
 * Uppercase "DESC" must work (not silently become ascending).
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

static int test_agg_input_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"agg_val_t\","
        "\"fields\":[\"tag:varchar:16\",\"score:int\"],"
        "\"indexes\":[\"tag\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"agg_val_t\",\"records\":{"
        "\"k1\":{\"tag\":\"a\",\"score\":10},"
        "\"k2\":{\"tag\":\"a\",\"score\":20},"
        "\"k3\":{\"tag\":\"b\",\"score\":30}"
        "}}", &resp); free(resp); resp = NULL;

    /* 1. Unknown agg function returns error, not count */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"agg_val_t\","
        "\"aggregates\":[{\"fn\":\"median\",\"field\":\"score\",\"alias\":\"m\"}],"
        "\"group_by\":[\"tag\"]}",
        &resp);
    ASSERT_NOT_NULL(resp, "unknown fn response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "unknown agg fn returns error");
    free(resp); resp = NULL;

    /* 2. Invalid order_by alias returns error */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"agg_val_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"tag\"],\"order_by\":\"nonexistent\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "invalid order_by response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "invalid order_by returns error");
    free(resp); resp = NULL;

    /* 3. Uppercase "DESC" works */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"agg_val_t\","
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"score\",\"alias\":\"s\"}],"
        "\"group_by\":[\"tag\"],\"order_by\":\"s\",\"order\":\"DESC\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "DESC uppercase response not null");
    /* b (score=30) should come first, a (score=30 total) second */
    const char *b_pos = strstr(resp, "\"tag\":\"b\"");
    const char *a_pos = strstr(resp, "\"tag\":\"a\"");
    ASSERT_NOT_NULL(b_pos, "DESC: b present");
    ASSERT_NOT_NULL(a_pos, "DESC: a present");
    ASSERT_TRUE(b_pos < a_pos, "DESC: b appears before a (descending)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-input-validation", test_agg_input_validation_run)
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
    src/test/cases/test_agg_input_validation.c \
```

Build + run-all. Confirm the `# total:` line matches pre-Task-5 count + 1,
and that `./build/bin/shard-db-test list` shows the new case name.

## Files changed

| File | Change |
|---|---|
| `src/db/query_aggregate.c` | Reject unknown fn in `cmd_aggregate_tree()` and `parse_agg_specs()`. Validate `order_by` alias in `cmd_aggregate_do()`. |
| `src/db/server.c` | Accept uppercase `"DESC"` in aggregate order. |
| `src/test/cases/test_agg_input_validation.c` | New — unknown fn error, invalid order_by error, uppercase DESC. |
| `build.sh` | Add the new test-case file to the `shard-db-test` gcc link line. |
