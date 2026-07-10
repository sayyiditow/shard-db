# NQL aggregate `--filter` flag

## Problem

In NQL aggregate mode, the parser uses a heuristic to distinguish the optional
pre-filter from the first aggregate spec: no `(` in the token → filter,
has `(` → agg spec. This breaks when the filter itself uses parentheses
(e.g. `IN`, `BETWEEN`):

```
aggregate dir obj 'age in (20, 30)' 'count()'
                  ^^^^^^^^^^^^^^^^^
                  has ( → misclassified as agg spec!
```

The `(` in `age in (20, 30)` causes the parser to skip the filter parse and
pass the filter string to `parse_nql_aggs()`, which mangles it.

The `--having` flag already exists for post-aggregation filtering. The
pre-filter (`aggregate dir obj <filter> <agg_spec>`) is a convenience
shortcut. Adding `--filter` as an explicit flag eliminates the heuristic
ambiguity while keeping backward compatibility.

## Scope

- Add `--filter` flag to NQL aggregate mode
- Keep existing positional-filter heuristic for backward compatibility
- When `--filter` is provided, skip the heuristic

## Execution rules

- Branch off `main`: `git checkout -b feat/nql-agg-filter-flag`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all` — must pass after every task
- Leave uncommitted; stop for review after each task
- Every anchor below is a verbatim quote from current `main`. If any quoted
  anchor is not found exactly as written, stop and write `PLAN_NOTES.md` —
  do not guess or reinterpret.

## Task 1 — Add `--filter` to the options loop

Locate the anchor (unique in file — the `--having` handler in the options
loop):

```c
        else if (!strcmp(argv[i],"--having") && i+1<argc) {
            char ferr[256];
            out->having = nql_parse_filter(argv[++i], ferr, sizeof ferr);
            if (!out->having && ferr[0]) { snprintf(out->err,sizeof out->err,"%s",ferr); return -1; }
            i++;
        }
```

Insert the following block directly before it (before the `--having` line):

```c
        else if (!strcmp(argv[i],"--filter") && i+1<argc) {
            char ferr[256];
            out->filter = nql_parse_filter(argv[++i], ferr, sizeof ferr);
            if (!out->filter && ferr[0]) { snprintf(out->err,sizeof out->err,"%s",ferr); return -1; }
            i++;
        }
```

Build + run-all. Must be green. The `--filter` flag now works but is not
yet wired to skip the heuristic.

## Task 2 — Skip heuristic when `--filter` was provided

Locate the anchor (unique in file — the heuristic-based filter detection
at the start of the aggregate branch):

```c
    if (out->mode == NQL_AGGREGATE) {
        if (i < argc && argv[i][0] != '-' && strchr(argv[i],'(') == NULL) {
            char ferr[256];
            out->filter = nql_parse_filter(argv[i], ferr, sizeof ferr);
            if (!out->filter && ferr[0]) { snprintf(out->err,sizeof out->err,"%s",ferr); return -1; }
            i++;
        }
```

Replace with:

```c
    if (out->mode == NQL_AGGREGATE) {
        if (!out->filter && i < argc && argv[i][0] != '-' && strchr(argv[i],'(') == NULL) {
            char ferr[256];
            out->filter = nql_parse_filter(argv[i], ferr, sizeof ferr);
            if (!out->filter && ferr[0]) { snprintf(out->err,sizeof out->err,"%s",ferr); return -1; }
            i++;
        }
```

The only change is adding `!out->filter &&` to the condition. When
`--filter` was already parsed in the options loop, `out->filter` is
non-NULL, so the heuristic is skipped and the next positional token is
always treated as the agg spec.

Build + run-all. Must be green.

## Task 3 — Add tests

Add one new test-case file under `src/test/cases/`.

### 3a — `src/test/cases/test_nql_agg_filter_flag.c`

```c
/* src/test/cases/test_nql_agg_filter_flag.c
 * Regression: NQL aggregate --filter flag must work for filters that
 * contain parentheses (IN, BETWEEN).  Positional filter heuristic must
 * still work for simple filters (backward compat).
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

static int test_nql_agg_filter_flag_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"agg_ff_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\"],"
        "\"indexes\":[\"status\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    /* 5 paid rows (amounts 10,20,30,40,50 = sum 150), 2 pending (5,5 = sum 10),
       1 cancelled (200). */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"agg_ff_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":20}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"paid\",\"amount\":30}},"
        "{\"key\":\"k4\",\"value\":{\"status\":\"paid\",\"amount\":40}},"
        "{\"key\":\"k5\",\"value\":{\"status\":\"paid\",\"amount\":50}},"
        "{\"key\":\"k6\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k7\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k8\",\"value\":{\"status\":\"cancelled\",\"amount\":200}}"
        "]}", &resp); free(resp); resp = NULL;

    /* 1. --filter with IN (parens) — must work, not misclassify */
    tc_request(tc,
        "aggregate default agg_ff_t --filter 'status in (paid, cancelled)' "
        "sum(amount) --group-by status",
        &resp);
    ASSERT_NOT_NULL(resp, "--filter with IN response not null");
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"", "--filter IN: paid present");
    ASSERT_CONTAINS(resp, "\"status\":\"cancelled\"", "--filter IN: cancelled present");
    ASSERT_TRUE(strstr(resp, "\"status\":\"pending\"") == NULL,
                "--filter IN: pending excluded");
    free(resp); resp = NULL;

    /* 2. Positional filter (backward compat) — simple filter still works */
    tc_request(tc,
        "aggregate default agg_ff_t 'status eq paid' "
        "sum(amount)",
        &resp);
    ASSERT_NOT_NULL(resp, "positional filter response not null");
    ASSERT_CONTAINS(resp, "\"sum_amount\":150", "positional filter: paid sum=150");
    free(resp); resp = NULL;

    /* 3. --filter overrides positional (when both present, --filter wins) */
    tc_request(tc,
        "aggregate default agg_ff_t 'status eq paid' "
        "--filter 'status eq pending' "
        "sum(amount)",
        &resp);
    ASSERT_NOT_NULL(resp, "--filter override response not null");
    ASSERT_CONTAINS(resp, "\"sum_amount\":10", "--filter override: pending sum=10");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-agg-filter-flag", test_nql_agg_filter_flag_run)
```

Build + run-all. Confirm the `# total:` line matches pre-change count + 1.

## Task 4 — Register the new test file in `build.sh`

Locate the anchor (last line of the test-case file list, unique in file):

```
    src/test/cases/test_coverity_join_buf_overflow.c \
```

Replace with:

```
    src/test/cases/test_coverity_join_buf_overflow.c \
    src/test/cases/test_nql_agg_filter_flag.c \
```

Build + run-all. Confirm the `# total:` line matches pre-Task-3 count + 1,
and that `./build/bin/shard-db-test list` shows the new case name.

## Files changed

| File | Change |
|---|---|
| `src/db/nql.c` | Add `--filter` flag to options loop. Guard positional heuristic with `!out->filter`. |
| `src/test/cases/test_nql_agg_filter_flag.c` | New — `--filter` with IN, positional backward compat, `--filter` override. |
| `build.sh` | Add the new test-case file to the `shard-db-test` gcc link line. |
