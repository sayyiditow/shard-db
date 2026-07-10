# Hardening follow-up fixes

## Problem

A Sonnet code review of the five post-2026.07.3-release hardening PRs
(#231–#235: criteria op/field/value validation, NQL input validation, NQL
agg `--filter` flag, aggregate input validation, server dispatch
validation) found two real bugs and two smaller inconsistencies introduced
by that same work:

1. **Memory leak — `parse_criteria_json()` array-form error path.**
   `src/db/query_plan.c`, the `*p == '['` branch: when `parse_one_criterion()`
   rejects a criterion partway through the array, the error path does
   `free(criteria)` instead of `free_criteria(criteria, n)`. Any
   already-parsed `in`/`not_in` leaf earlier in the same array has a
   heap-allocated `in_values` array (plus `strdup`'d strings inside it)
   that `free(criteria)` never reaches — only `free_criteria()` walks
   `c[i].in_values` before freeing the array itself. Reachable on every
   `find`/`count`/`aggregate` JSON request whose `criteria` array combines
   a valid `in`/`not_in` leaf with a later invalid leaf — a remote client
   can repeat this cheaply to grow server RSS unboundedly.

2. **Silently-wrong-results gap — missing `value2` validation.**
   `src/db/query_plan.c`, `parse_one_criterion()`: the validation added in
   PR #231 checks that `value` is present for every op that needs one, but
   never checks `value2` for `between`/`len_between` (which the documented
   protocol requires both bounds for — see `docs/query-protocol/find.md`).
   A criterion like `{"field":"age","op":"between","value":"18"}` (missing
   `value2`) is accepted, `c->value2` stays `""` from the `memset`, and
   downstream that becomes an empty-string / zero upper bound — silently
   wrong results instead of a validation error. This is exactly the bug
   class PR #231's own commit message says it eliminated.

3. **Memory leak — NQL aggregate `--filter` overwrite.** `src/db/nql.c`,
   the `--filter` flag handler added in PR #233: `out->filter =
   nql_parse_filter(...)` unconditionally overwrites `out->filter` without
   freeing whatever tree the positional-filter heuristic already built.
   `test_nql_agg_filter_flag_run`'s test 3 (`'status eq paid' --filter
   'status eq pending'`) exercises exactly this path and asserts
   `--filter` wins — which it does — but the discarded first tree is never
   freed. `nql_parse_command()` runs once per network request
   (`server.c:580`), so this leaks one `CriteriaNode` tree per matching
   request.

4. **Inconsistent order-direction validation — NQL `--order-by`.**
   `src/db/nql.c`: the same diff added strict `asc`/`desc` validation to
   the standalone `--order` flag, but the sibling `--order-by
   field:<dir>` handler still stores whatever follows the `:` verbatim
   with no validation at all — nql.c itself never rejects anything here.
   Downstream consumers disagree on what they tolerate: `query.c`'s
   `cmd_find_do` (e.g. `query.c:6851-6852`) special-cases the exact
   uppercase literal `"DESC"` in addition to `"desc"`, so on the `find`
   path today `--order-by field:DESC` happens to already sort correctly
   — but `server.c:648`'s aggregate dispatch (`strcmp(cmd.order_dir,
   "desc")==0`) only recognizes lowercase `"desc"`, and *no* consumer
   rejects garbage like `sideways` — it silently falls through to
   ascending instead of erroring. The real gap is the missing validation
   at parse time, which lets nonsense through inconsistently across
   consumers rather than failing fast in one place.

5. **Dead code — redundant case check.** `src/db/server.c:2009`:
   `strcmp(od, "desc") == 0 || strcasecmp(od, "desc") == 0` — the
   `strcmp` half is fully subsumed by the `strcasecmp` half and can never
   change the result. No functional impact; cleanup only.

## Scope

- Fix the two memory leaks (#1, #3).
- Add the missing `value2` validation for `between`/`len_between` (#2).
- Add a shared direction-normalizing helper in `nql.c` and use it for both
  `--order` and `--order-by` so both reject anything other than
  `asc`/`desc` (case-insensitive) (#4).
- Drop the redundant `strcmp` half in `server.c` (#5).
- One regression test per bug/gap, registered in `build.sh`.

## Execution rules

- Branch off `main`: `git checkout -b fix/hardening-followup-issues`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all` — must pass after every task
- **Tasks 1 and 3 are memory leaks, not behavioral bugs** — the client-visible
  response is already correct on `main`, so a plain TAP assertion cannot
  distinguish leaking from non-leaking. Verify those two with a sanitizer
  build: `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` (builds
  `./build/bin/shard-db-test` with `-fsanitize=address,undefined`), then
  `./build/bin/shard-db-test run <test-name>`. The test daemon is stopped
  via `SIGTERM` (`test_env_stop`), which lets the process exit normally —
  LeakSanitizer reports at exit, and a leak makes the daemon process exit
  non-zero, which the test runner surfaces as a failure. Confirm the new
  test fails under `BUILD_MODE=asan` **before** the fix and passes
  **after**, in addition to passing in the normal `run-all` (non-asan)
  pass required by every task.
- Leave uncommitted; stop for review after each task.
- Every anchor below is a verbatim quote from current `main`. If any
  quoted anchor is not found exactly as written, stop and write
  `PLAN_NOTES.md` — do not guess or reinterpret.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.

## Task 1 — Fix the `in_values` leak in `parse_criteria_json()`

### 1a — Add the regression test first

Create `src/test/cases/test_criteria_in_then_invalid_leak.c`:

```c
/* src/test/cases/test_criteria_in_then_invalid_leak.c
 * Regression: a criteria array containing a valid "in"/"not_in" leaf
 * followed by an invalid leaf must not leak the first leaf's in_values.
 * parse_criteria_json()'s array-form error path used to bare
 * free(criteria) instead of free_criteria(criteria, n), dropping the
 * already-parsed IN leaf's in_values array and its strdup'd strings.
 *
 * This is a leak, not a behavioral bug — the client-visible response
 * (an error, since the array is rejected as a whole) is identical
 * before and after the fix. Build with BUILD_MODE=asan to catch the
 * leak via LeakSanitizer; the TAP assertions below only guard that the
 * behavior (still an error) didn't regress.
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

static int test_criteria_in_then_invalid_leak_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"leak_t\","
        "\"fields\":[\"tag:varchar:16\"],\"indexes\":[\"tag\"],\"splits\":8}",
        &resp); free(resp); resp = NULL;

    /* Valid IN leaf (allocates in_values) followed by an invalid leaf
       (missing "op") in the SAME array. Repeated so a leak is visible in
       RSS growth even without a sanitizer, though ASAN/LSAN is the
       authoritative check (see execution rules). */
    for (int i = 0; i < 200; i++) {
        tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"leak_t\","
            "\"criteria\":[{\"field\":\"tag\",\"op\":\"in\",\"value\":\"a,b,c\"},"
            "{\"field\":\"bad\"}]}",
            &resp);
        ASSERT_NOT_NULL(resp, "response not null");
        ASSERT_CONTAINS(resp, "\"error\"", "invalid trailing criterion rejects whole array");
        free(resp); resp = NULL;
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-criteria-in-then-invalid-leak", test_criteria_in_then_invalid_leak_run)
```

Register it in `build.sh`. Locate the anchor (unique in file — last entry
in the test-case list):

```
    src/test/cases/test_agg_having_no_group_regression.c \
```

Replace with:

```
    src/test/cases/test_agg_having_no_group_regression.c \
    src/test/cases/test_criteria_in_then_invalid_leak.c \
```

Build and run under ASAN per the execution rules:

```
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-criteria-in-then-invalid-leak
```

Confirm LeakSanitizer reports a leak (nonzero exit / `ERROR:
LeakSanitizer` in output) — this is the failing-before-fix state. Also run
`SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run
test-criteria-in-then-invalid-leak` (plain build) and confirm the TAP
assertions pass (behavior is already correct; only the leak is new).

### 1b — Fix the leak

Locate the anchor (unique in file — the array-form error path inside
`parse_criteria_json`):

```c
            if (parse_one_criterion(obj_buf, &criteria[n]) != 0) {
                free(criteria);
                *out = NULL;
                *count = 0;
                return -1;
            }
```

Replace with:

```c
            if (parse_one_criterion(obj_buf, &criteria[n]) != 0) {
                free_criteria(criteria, n);
                *out = NULL;
                *count = 0;
                return -1;
            }
```

`free_criteria(c, count)` frees `c[i].in_values` (and its strings) for
`i` in `[0, count)` before freeing `c` itself; `n` is exactly the number
of criteria successfully parsed so far (the failed slot at index `n` was
`memset` to zero by `parse_one_criterion` and owns no heap memory).
`free_criteria` is already declared in `types.h` and defined later in
this same file, so no new declaration is needed.

Rebuild under ASAN and re-run the new test — confirm no LeakSanitizer
report this time. Then:

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Must be green.

## Task 2 — Validate `value2` for `between`/`len_between`

### 2a — Add the regression test first

Create `src/test/cases/test_criteria_between_missing_value2.c`:

```c
/* src/test/cases/test_criteria_between_missing_value2.c
 * Regression: "between"/"len_between" criteria missing "value2" must be
 * rejected with an error, not silently treated as an empty upper bound.
 * parse_one_criterion() validated "value" for every op that requires one
 * but never validated "value2" for the two range ops that need both
 * bounds (see docs/query-protocol/find.md — between requires value AND
 * value2).
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

static int test_criteria_between_missing_value2_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"btw_val_t\","
        "\"fields\":[\"age:int\",\"name:varchar:32\"],\"indexes\":[],\"splits\":8}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"btw_val_t\",\"records\":{"
        "\"k1\":{\"age\":30,\"name\":\"alice\"}}}",
        &resp); free(resp); resp = NULL;

    /* 1. "between" missing value2 must be rejected */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"btw_val_t\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"between\",\"value\":\"18\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "between missing value2 response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "between missing value2 rejected");
    free(resp); resp = NULL;

    /* 2. "len_between" missing value2 must be rejected */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"btw_val_t\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"len_between\",\"value\":\"3\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "len_between missing value2 response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "len_between missing value2 rejected");
    free(resp); resp = NULL;

    /* 3. "between" WITH both bounds must still work (no false-positive
       rejection) and must actually match */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"btw_val_t\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"between\",\"value\":\"18\",\"value2\":\"65\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "between with value2 response not null");
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "between with both bounds not rejected");
    ASSERT_CONTAINS(resp, "\"name\":\"alice\"", "between with both bounds matches alice");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-criteria-between-missing-value2", test_criteria_between_missing_value2_run)
```

Register it in `build.sh` next to the Task 1 entry. Locate the anchor
(now unique after Task 1):

```
    src/test/cases/test_criteria_in_then_invalid_leak.c \
```

Replace with:

```
    src/test/cases/test_criteria_in_then_invalid_leak.c \
    src/test/cases/test_criteria_between_missing_value2.c \
```

Build and run:

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-criteria-between-missing-value2
```

Confirm assertions 1 and 2 (missing value2 rejected) **fail** on `main` —
the request currently succeeds and returns rows instead of an error.
Assertion 3 already passes on `main`.

### 2b — Fix: validate `value2` for `between`/`len_between`

Locate the anchor (unique in file — the docstring immediately above
`parse_one_criterion`):

```c
/* Parse one criterion leaf from a JSON object buffer.
   Returns 0 on success, -1 on error (missing field, missing/unknown op,
   or missing value for ops that require one).
   On error, c is zeroed. */
```

Replace with:

```c
/* Parse one criterion leaf from a JSON object buffer.
   Returns 0 on success, -1 on error (missing field, missing/unknown op,
   missing value for ops that require one, or missing value2 for
   between/len_between).
   On error, c is zeroed. */
```

Locate the anchor (unique in file — the value-validation block followed
by the `if (v)` block in `parse_one_criterion`):

```c
    /* Validate value — required for all ops except exists/not_exists */
    if (op_requires_value(c->op) && (!v || v[0] == '\0')) {
        free(v); free(v_raw); free(v2);
        return -1;
    }
    if (v) {
```

Replace with:

```c
    /* Validate value — required for all ops except exists/not_exists */
    if (op_requires_value(c->op) && (!v || v[0] == '\0')) {
        free(v); free(v_raw); free(v2);
        return -1;
    }

    /* Validate value2 — between/len_between need both bounds; a missing
       value2 must not silently fall back to an empty-string bound. */
    if ((c->op == OP_BETWEEN || c->op == OP_LEN_BETWEEN) && (!v2 || v2[0] == '\0')) {
        free(v); free(v_raw); free(v2);
        return -1;
    }
    if (v) {
```

Build and re-run:

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-criteria-between-missing-value2
./build/bin/shard-db-test run-all
```

All must be green.

## Task 3 — Fix the NQL aggregate `--filter` overwrite leak

### 3a — Add the regression test first

Create `src/test/cases/test_nql_agg_filter_override_leak.c`:

```c
/* src/test/cases/test_nql_agg_filter_override_leak.c
 * Regression: NQL aggregate combining a positional filter with --filter
 * must not leak the discarded positional CriteriaNode tree. nql.c's
 * --filter handler did `out->filter = nql_parse_filter(...)`
 * unconditionally, dropping whatever tree the positional-filter
 * heuristic had already built without freeing it.
 *
 * This is a leak, not a behavioral bug — --filter already wins on main
 * (test_nql_agg_filter_flag_run's test 3 covers that). Build with
 * BUILD_MODE=asan to catch the leak via LeakSanitizer; the TAP
 * assertions below only guard that --filter still wins.
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

static int test_nql_agg_filter_override_leak_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"agg_leak_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\"],"
        "\"indexes\":[\"status\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"agg_leak_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"pending\",\"amount\":5}}"
        "]}", &resp); free(resp); resp = NULL;

    /* Positional filter + --filter in the same aggregate query, repeated —
       each iteration leaks one discarded CriteriaNode tree pre-fix. */
    for (int i = 0; i < 200; i++) {
        tc_request(tc,
            "aggregate default agg_leak_t 'status eq paid' "
            "--filter 'status eq pending' "
            "sum(amount)",
            &resp);
        ASSERT_NOT_NULL(resp, "response not null");
        ASSERT_CONTAINS(resp, "\"sum_amount\":10", "--filter override still wins");
        free(resp); resp = NULL;
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-agg-filter-override-leak", test_nql_agg_filter_override_leak_run)
```

Register it in `build.sh`. Locate the anchor (unique after Task 2):

```
    src/test/cases/test_criteria_between_missing_value2.c \
```

Replace with:

```
    src/test/cases/test_criteria_between_missing_value2.c \
    src/test/cases/test_nql_agg_filter_override_leak.c \
```

Build and run under ASAN:

```
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-nql-agg-filter-override-leak
```

Confirm LeakSanitizer reports a leak before the fix. Also confirm the TAP
assertions pass under a plain build (behavior is already correct).

### 3b — Fix the leak

Locate the anchor (unique in file — the `--filter` flag handler in
`nql_parse_command`'s option loop):

```c
        else if (!strcmp(argv[i],"--filter") && i+1<argc) {
            char ferr[256];
            out->filter = nql_parse_filter(argv[++i], ferr, sizeof ferr);
            if (!out->filter && ferr[0]) { snprintf(out->err,sizeof out->err,"%s",ferr); return -1; }
            i++;
        }
```

Replace with:

```c
        else if (!strcmp(argv[i],"--filter") && i+1<argc) {
            char ferr[256];
            free_criteria_tree(out->filter);
            out->filter = nql_parse_filter(argv[++i], ferr, sizeof ferr);
            if (!out->filter && ferr[0]) { snprintf(out->err,sizeof out->err,"%s",ferr); return -1; }
            i++;
        }
```

`free_criteria_tree(NULL)` is a no-op (`if (!n) return;`), so this is safe
whether or not the positional-filter branch already set `out->filter`.
`free_criteria_tree` is already used elsewhere in this file
(`nql_free_command`), so no new include is needed.

Rebuild under ASAN and re-run — confirm no leak. Then:

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Must be green.

## Task 4 — Validate NQL `--order-by` direction the same way `--order` does

### 4a — Add the regression test first

Create `src/test/cases/test_nql_order_by_direction.c`:

```c
/* src/test/cases/test_nql_order_by_direction.c
 * Regression: NQL --order-by field:DIR must validate DIR the same way
 * the standalone --order flag does. nql.c's own order_dir string was
 * accepted verbatim for any suffix (no validation) — the exact literal
 * "DESC" happens to already sort correctly today only because query.c's
 * consumers special-case that one uppercase literal in addition to
 * lowercase "desc" (e.g. query.c:6851-6852); nql.c itself never rejected
 * anything. A typo like --order-by field:sideways was silently accepted
 * and treated as ascending instead of erroring.
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

static int test_nql_order_by_direction_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"nql_ord_t\","
        "\"fields\":[\"amount:int\"],\"indexes\":[\"amount\"],\"splits\":8}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"nql_ord_t\",\"records\":{"
        "\"k1\":{\"amount\":10},\"k2\":{\"amount\":20},\"k3\":{\"amount\":30}}}",
        &resp); free(resp); resp = NULL;

    /* 1. Uppercase "DESC" already sorts descending on main today —
       query.c's consumers (e.g. cmd_find_do at query.c:6851-6852)
       special-case the exact literal "DESC" in addition to "desc". This
       assertion is a characterization/regression guard, not a
       pre-fix-failing assertion: it passes before and after the fix,
       confirming normalize_order_dir() doesn't break this already-working
       case while it starts rejecting garbage (assertion 2). */
    tc_request(tc, "find default nql_ord_t --order-by amount:DESC", &resp);
    ASSERT_NOT_NULL(resp, "DESC response not null");
    if (resp) {
        const char *p30 = strstr(resp, "\"amount\":30");
        const char *p10 = strstr(resp, "\"amount\":10");
        ASSERT_TRUE(p30 && p10 && p30 < p10, "amount:DESC sorts descending");
    }
    free(resp); resp = NULL;

    /* 2. Garbage direction must be rejected, not silently accepted as
       ascending. This is the actual pre-fix-failing assertion. */
    tc_request(tc, "find default nql_ord_t --order-by amount:sideways", &resp);
    ASSERT_NOT_NULL(resp, "garbage direction response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "garbage order direction rejected");
    free(resp); resp = NULL;

    /* 3. Lowercase 'asc' still works (backward compat). */
    tc_request(tc, "find default nql_ord_t --order-by amount:asc", &resp);
    ASSERT_NOT_NULL(resp, "asc response not null");
    if (resp) {
        const char *p30 = strstr(resp, "\"amount\":30");
        const char *p10 = strstr(resp, "\"amount\":10");
        ASSERT_TRUE(p30 && p10 && p10 < p30, "amount:asc sorts ascending");
    }
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-order-by-direction", test_nql_order_by_direction_run)
```

Register it in `build.sh`. Locate the anchor (unique after Task 3):

```
    src/test/cases/test_nql_agg_filter_override_leak.c \
```

Replace with:

```
    src/test/cases/test_nql_agg_filter_override_leak.c \
    src/test/cases/test_nql_order_by_direction.c \
```

Build and run:

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-nql-order-by-direction
```

Confirm assertion 2 (garbage direction rejected) **fails** on `main` — it
currently sorts ascending with no error. Assertions 1 and 3 already pass
on `main` unchanged (assertion 1's `DESC` literal already works today via
query.c's consumers, per the comment in the test above; it's a
characterization guard, not a regression signal) — that's expected, not a
plan error.

### 4b — Add a shared direction-normalizing helper and use it in both `--order` and `--order-by`

Locate the anchor (unique in file — the end of `cmd_split` immediately
before the `nql_parse_command` section comment):

```c
    return n;
}

/* ── nql_parse_command ────────────────────────────────────────────── */

int nql_parse_command(const char *src, NqlCommand *out) {
```

Replace with:

```c
    return n;
}

/* Normalize an order-direction token to canonical lowercase "asc"/"desc"
   (case-insensitive). Returns 0 and writes into out on success, -1 if d
   is neither. Shared by --order and --order-by so both reject the same
   set of invalid direction strings instead of silently defaulting to
   ascending. */
static int normalize_order_dir(const char *d, char *out, size_t out_sz) {
    if (strcasecmp(d, "asc") == 0)  { snprintf(out, out_sz, "asc");  return 0; }
    if (strcasecmp(d, "desc") == 0) { snprintf(out, out_sz, "desc"); return 0; }
    return -1;
}

/* ── nql_parse_command ────────────────────────────────────────────── */

int nql_parse_command(const char *src, NqlCommand *out) {
```

Locate the anchor (unique in file — the `--order-by` flag handler):

```c
        else if (!strcmp(argv[i],"--order-by") && i+1<argc) {
            char *spec = argv[++i]; i++;
            char *colon = strrchr(spec, ':');
            if (colon) {
                *colon = '\0';
                snprintf(out->order_dir,sizeof out->order_dir,"%s",colon+1);
                if (spec[0] == '\0') {
                    snprintf(out->err, sizeof out->err, "--order-by requires a field name before ':'");
                    return -1;
                }
            }
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
                if (spec[0] == '\0') {
                    snprintf(out->err, sizeof out->err, "--order-by requires a field name before ':'");
                    return -1;
                }
                if (normalize_order_dir(colon + 1, out->order_dir, sizeof out->order_dir) != 0) {
                    snprintf(out->err, sizeof out->err, "invalid order direction '%s'; use 'asc' or 'desc'", colon + 1);
                    return -1;
                }
            }
            else         snprintf(out->order_dir,sizeof out->order_dir,"asc");
            snprintf(out->order_by, sizeof out->order_by, "%s", spec);
        }
```

Locate the anchor (unique in file — the `--order` flag handler):

```c
        else if (!strcmp(argv[i],"--order")   && i+1<argc) {
            const char *d = argv[++i];
            if (strcmp(d,"desc")==0 || strcmp(d,"DESC")==0 || strcmp(d,"Desc")==0)
                snprintf(out->order_dir,sizeof out->order_dir,"desc");
            else if (strcmp(d,"asc")==0 || strcmp(d,"ASC")==0 || strcmp(d,"Asc")==0)
                snprintf(out->order_dir,sizeof out->order_dir,"asc");
            else { snprintf(out->err, sizeof out->err, "invalid order direction '%s'; use 'asc' or 'desc'", d); return -1; }
            i++;
        }
```

Replace with:

```c
        else if (!strcmp(argv[i],"--order")   && i+1<argc) {
            const char *d = argv[++i];
            if (normalize_order_dir(d, out->order_dir, sizeof out->order_dir) != 0) {
                snprintf(out->err, sizeof out->err, "invalid order direction '%s'; use 'asc' or 'desc'", d);
                return -1;
            }
            i++;
        }
```

Build and re-run:

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-nql-order-by-direction
./build/bin/shard-db-test run-all
```

All must be green. Pay particular attention to any existing test that
exercises `--order` with the exact strings `DESC`/`Desc`/`ASC`/`Asc` —
`normalize_order_dir` is case-insensitive so these still work, but
confirm `run-all` shows no regression.

## Task 5 — Drop the redundant `strcmp`/`strcasecmp` in `server.c`

### 5a — Add a characterization test first

This is dead-code removal with no intended behavior change, but there is
currently no test covering the JSON `aggregate` mode's `"order"` field
case-insensitivity at all. Add one so the cleanup can't silently regress
it.

Create `src/test/cases/test_json_aggregate_order_case.c`:

```c
/* src/test/cases/test_json_aggregate_order_case.c
 * Characterization test for the JSON aggregate "order" field's
 * case-insensitive "desc" handling in dispatch_json_query(). Guards the
 * dead-code cleanup that drops the redundant
 * `strcmp(od,"desc")==0 || strcasecmp(od,"desc")==0` down to just the
 * strcasecmp half — the strcmp half can never be true when the
 * strcasecmp half is false, so removing it must not change behavior.
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

static int test_json_aggregate_order_case_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ord_case_t\","
        "\"fields\":[\"grp:varchar:8\",\"amount:int\"],\"indexes\":[\"grp\"],\"splits\":8}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"ord_case_t\",\"records\":{"
        "\"k1\":{\"grp\":\"a\",\"amount\":100},\"k2\":{\"grp\":\"b\",\"amount\":10}}}",
        &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"ord_case_t\","
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"total\"}],"
        "\"group_by\":[\"grp\"],\"order_by\":\"total\",\"order\":\"DESC\"}",
        &resp);
    ASSERT_NOT_NULL(resp, "uppercase DESC response not null");
    if (resp) {
        const char *pa = strstr(resp, "\"grp\":\"a\"");
        const char *pb = strstr(resp, "\"grp\":\"b\"");
        ASSERT_TRUE(pa && pb && pa < pb, "order:DESC (uppercase) sorts descending by total");
    }
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-json-aggregate-order-case", test_json_aggregate_order_case_run)
```

Register it in `build.sh`. Locate the anchor (unique after Task 4):

```
    src/test/cases/test_nql_order_by_direction.c \
```

Replace with:

```
    src/test/cases/test_nql_order_by_direction.c \
    src/test/cases/test_json_aggregate_order_case.c \
```

Build and run:

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-json-aggregate-order-case
```

Confirm it passes on `main` (this characterizes existing behavior; it is
not expected to fail before the cleanup).

### 5b — Drop the redundant condition

Locate the anchor (unique in file):

```c
            int desc = (od && (strcmp(od, "desc") == 0 || strcasecmp(od, "desc") == 0));
```

Replace with:

```c
            int desc = (od && strcasecmp(od, "desc") == 0);
```

Build and re-run:

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-json-aggregate-order-case
./build/bin/shard-db-test run-all
```

Must be green — no behavior change expected.

## Task 6 — Final full regression pass

Run the complete suite once more from a clean build in both configurations:

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

```
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Both must be green with no LeakSanitizer/UBSan reports anywhere in the
suite (not just the five new tests — the ASAN pass covers everything
touched transitively by `find`/`count`/`aggregate`/NQL tests).

## Files changed

| File | Change |
|---|---|
| `src/db/query_plan.c` | Task 1: `free(criteria)` → `free_criteria(criteria, n)` in `parse_criteria_json`'s array-form error path. Task 2: validate `value2` for `between`/`len_between` in `parse_one_criterion`; update its docstring. |
| `src/db/nql.c` | Task 3: free the previous `out->filter` tree before the `--filter` flag overwrites it. Task 4: add `normalize_order_dir()` helper; use it in both `--order-by` and `--order` handlers. |
| `src/db/server.c` | Task 5: drop the redundant `strcmp(od,"desc")==0 \|\|` half of the aggregate `order` case check. |
| `src/test/cases/test_criteria_in_then_invalid_leak.c` | New — regression for Task 1 (ASAN-verified leak). |
| `src/test/cases/test_criteria_between_missing_value2.c` | New — regression for Task 2. |
| `src/test/cases/test_nql_agg_filter_override_leak.c` | New — regression for Task 3 (ASAN-verified leak). |
| `src/test/cases/test_nql_order_by_direction.c` | New — regression for Task 4. |
| `src/test/cases/test_json_aggregate_order_case.c` | New — characterization test for Task 5. |
| `build.sh` | Register all five new test-case files in the `shard-db-test` link line. |
