# Criteria `op` alias and unknown-operator validation

## Problem

Four silent-failure bugs in criteria parsing produce wrong results with no error:

1. **Missing `"op"` key.** Using `"operator"` (or any other key name) instead of
   `"op"` silently degrades to `OP_EQUAL`. The `parse_one_criterion()` function
   reads only `"op"` from the JSON object; when it is absent, `c->op` stays at
   its `memset` zero value — `OP_EQUAL` (the first enum entry).

2. **Unrecognized operator string.** `parse_op()` returns `OP_EQUAL` as a
   catch-all for any string it does not recognise (line `return OP_EQUAL;` at
   end of function). A typo like `"o"` or `"gr"` silently becomes equality.

3. **Missing `"field"`.** The flat-array path in `parse_criteria_json()`
   silently includes a criterion with empty field that never matches anything.
   (The tree path in `parse_tree_element()` already rejects this.)

4. **Missing `"value"` for ops that require it.** No check anywhere. All ops
   except `exists`/`not_exists` need a value; without it, they silently match
   against empty string.

All produce incorrect query results (equality match or empty match instead of
the requested operation) with no error response.

## Scope

- Add `"operator"` as a documented alias for `"op"` in `parse_one_criterion()`
- Make `parse_op()` return `OP_UNKNOWN` sentinel for unrecognized strings
- Make `parse_one_criterion()` return error status for: missing field, missing/unknown op, missing value (for ops that require it)
- Propagate errors through `parse_criteria_json()` and `parse_criteria_tree()`
- Add regression tests covering all four bugs

## Execution rules

- Branch off `main`: `git checkout -b fix/criteria-op-alias-validation`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all` — must pass after every task
- Leave uncommitted; stop for review after each task
- Every anchor below is a verbatim quote from current `main`. If any quoted
  anchor is not found exactly as written, stop and write `PLAN_NOTES.md` —
  do not guess or reinterpret.

## Task 1 — Add `OP_UNKNOWN` sentinel to `enum SearchOp`

Locate the anchor (unique in file):

```c
    OP_REGEX, OP_NOT_REGEX
};
```

Replace with:

```c
    OP_REGEX, OP_NOT_REGEX,
    OP_UNKNOWN   /* sentinel: unrecognised operator string — parse_op() returns this */
};
```

Build. Must compile cleanly (OP_UNKNOWN is unused until Task 2).

## Task 2 — Make `parse_op()` return `OP_UNKNOWN` for unrecognized strings

Locate the anchor (unique in file — last line of `parse_op()`):

```c
    return OP_EQUAL;
}
```

Replace with:

```c
    return OP_UNKNOWN;
}
```

Build. Must compile cleanly. No behavior change yet — callers still assign
the result directly and the enum value is new.

## Task 3 — Add alias, field/value validation, and error return to `parse_one_criterion()`

Change the function signature and add all three validations. Locate the anchor
(unique in file):

```c
static void parse_one_criterion(const char *obj_buf, SearchCriterion *c) {
    memset(c, 0, sizeof(*c));

    JsonObj cobj;
    json_parse_object(obj_buf, strlen(obj_buf), &cobj);

    char *f     = json_obj_strdup(&cobj, "field");
    char *o     = json_obj_strdup(&cobj, "op");
    char *v     = json_obj_strdup(&cobj, "value");
    char *v_raw = json_obj_strdup_raw(&cobj, "value");
    char *v2    = json_obj_strdup(&cobj, "value2");

    if (f) { strncpy(c->field, f, 255); free(f); }
    if (o) { c->op = parse_op(o); free(o); }
```

Replace with:

```c
/* Returns 1 if the op requires a value operand, 0 for existence-only ops. */
static int op_requires_value(enum SearchOp op) {
    return op != OP_EXISTS && op != OP_NOT_EXISTS;
}

/* Parse one criterion leaf from a JSON object buffer.
   Returns 0 on success, -1 on error (missing field, missing/unknown op,
   or missing value for ops that require one).
   On error, c is zeroed. */
static int parse_one_criterion(const char *obj_buf, SearchCriterion *c) {
    memset(c, 0, sizeof(*c));

    JsonObj cobj;
    json_parse_object(obj_buf, strlen(obj_buf), &cobj);

    char *f     = json_obj_strdup(&cobj, "field");
    char *o     = json_obj_strdup(&cobj, "op");
    if (!o) o = json_obj_strdup(&cobj, "operator");   /* alias */
    char *v     = json_obj_strdup(&cobj, "value");
    char *v_raw = json_obj_strdup_raw(&cobj, "value");
    char *v2    = json_obj_strdup(&cobj, "value2");

    /* Validate field — every criterion must name a field */
    if (!f || f[0] == '\0') { free(f); free(o); free(v); free(v_raw); free(v2); return -1; }
    strncpy(c->field, f, 255); free(f);

    /* Validate op — must be present and recognised */
    if (o) {
        c->op = parse_op(o);
        free(o);
        if (c->op == OP_UNKNOWN) { free(v); free(v_raw); free(v2); return -1; }
    } else {
        free(v); free(v_raw); free(v2);
        return -1;   /* neither "op" nor "operator" present */
    }

    /* Validate value — required for all ops except exists/not_exists */
    if (op_requires_value(c->op) && (!v || v[0] == '\0')) {
        free(v); free(v_raw); free(v2);
        return -1;
    }
```

Key invariants:
- `field` must be non-empty — rejected immediately if missing.
- `op` must be present (`"op"` or `"operator"` key) and recognised by
  `parse_op()`. `OP_UNKNOWN` is caught immediately.
- `value` is required for all ops except `OP_EXISTS`/`OP_NOT_EXISTS`.
  These two just check field presence and legitimately take no value.
- The `"operator"` alias is checked only when `"op"` is absent (not both).
- Memory is freed on every error path to avoid leaks.
- The rest of the function body (IN/LIKE normalization, in_values parsing)
  is unchanged — it reads `c->op` and `c->value` which are now validated.

Build + run-all. Three call sites of `parse_one_criterion` will now have
unchecked return values — that is expected and fixed in Tasks 4-5.

## Task 4 — Propagate errors from `parse_one_criterion` in `parse_criteria_json()`

Locate the anchor (unique in file, inside `parse_criteria_json()` array-form
branch):

```c
            parse_one_criterion(obj_buf, &criteria[n]);
            n++;
```

Replace with:

```c
            if (parse_one_criterion(obj_buf, &criteria[n]) != 0) {
                free(criteria);
                *out = NULL;
                *count = 0;
                return -1;
            }
            n++;
```

Build + run-all. The flat-array `criteria` path now rejects bad operators.

## Task 5 — Propagate errors from `parse_one_criterion` in `parse_criteria_tree()`

There are two call sites inside `parse_criteria_tree()`.

### 5a — `parse_tree_element()` leaf path

Locate the anchor (unique in `parse_tree_element()`, right before the
`field[0] == '\0'` check):

```c
    parse_one_criterion(obj_buf, &n->leaf);
    if (n->leaf.field[0] == '\0') {
        free_criteria_tree(n);
        if (err) *err = "leaf missing 'field'";
        return NULL;
    }
```

Replace with:

```c
    if (parse_one_criterion(obj_buf, &n->leaf) != 0) {
        free_criteria_tree(n);
        if (err) *err = "invalid criterion: missing field, op or value";
        return NULL;
    }
    if (n->leaf.field[0] == '\0') {
        free_criteria_tree(n);
        if (err) *err = "leaf missing 'field'";
        return NULL;
    }
```

### 5b — `parse_criteria_tree()` leaf path (object form with `"field"` key)

Locate the anchor (unique in file — the second `parse_one_criterion` call
inside `parse_criteria_tree`, after `json_obj_get(&pobj, "field", ...)`):

```c
            parse_one_criterion(p, &n->leaf);
            return n;
```

Replace with:

```c
            if (parse_one_criterion(p, &n->leaf) != 0) {
                free_criteria_tree(n);
                if (err) *err = "invalid criterion: missing field, op or value";
                return NULL;
            }
            return n;
```

Build + run-all. Both JSON tree and flat-array paths now reject bad operators.

## Task 6 — Add regression tests

Add three new test-case files under `src/test/cases/`.

### 6a — `src/test/cases/test_criteria_operator_alias.c`

Tests that `"operator"` works as an alias for `"op"`, and that an
unrecognized operator string returns an error.

```c
/* src/test/cases/test_criteria_operator_alias.c
 * Regression: "operator" must work as an alias for "op", and an
 * unrecognised operator string must return an error (not silently
 * degrade to equality).
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

static int test_criteria_operator_alias_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"op_alias_t\","
        "\"fields\":[\"name:varchar:32\",\"price:double\"],"
        "\"indexes\":[\"price\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"op_alias_t\",\"records\":{"
        "\"k1\":{\"name\":\"cheap\",\"price\":2.5},"
        "\"k2\":{\"name\":\"mid\",\"price\":7.0},"
        "\"k3\":{\"name\":\"expensive\",\"price\":15.0}"
        "}}", &resp); free(resp); resp = NULL;

    /* 1. "op" still works (baseline) */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"op_alias_t\","
        "\"criteria\":[{\"field\":\"price\",\"op\":\"gt\",\"value\":5}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "op:gt response not null");
    ASSERT_CONTAINS(resp, "\"name\":\"mid\"", "op:gt returns mid");
    ASSERT_CONTAINS(resp, "\"name\":\"expensive\"", "op:gt returns expensive");
    ASSERT_TRUE(strstr(resp, "\"name\":\"cheap\"") == NULL,
                "op:gt excludes cheap");
    free(resp); resp = NULL;

    /* 2. "operator" alias works identically */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"op_alias_t\","
        "\"criteria\":[{\"field\":\"price\",\"operator\":\"gt\",\"value\":5}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "operator:gt response not null");
    ASSERT_CONTAINS(resp, "\"name\":\"mid\"", "operator:gt returns mid");
    ASSERT_CONTAINS(resp, "\"name\":\"expensive\"", "operator:gt returns expensive");
    ASSERT_TRUE(strstr(resp, "\"name\":\"cheap\"") == NULL,
                "operator:gt excludes cheap");
    free(resp); resp = NULL;

    /* 3. Unrecognised operator returns error, not equality fallback */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"op_alias_t\","
        "\"criteria\":[{\"field\":\"price\",\"op\":\"bogus\",\"value\":5}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "bogus op response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "bogus op returns error");
    free(resp); resp = NULL;

    /* 4. Missing both "op" and "operator" returns error */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"op_alias_t\","
        "\"criteria\":[{\"field\":\"price\",\"value\":5}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "missing op response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "missing op returns error");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-criteria-operator-alias", test_criteria_operator_alias_run)
```

### 6b — `src/test/cases/test_criteria_op_tree_error.c`

Tests that the tree-path criteria parser also rejects unknown operators
and the `"operator"` alias.

```c
/* src/test/cases/test_criteria_op_tree_error.c
 * Regression: criteria tree parser ({"or":[...]} / {"and":[...]}) must
 * also reject unknown operators and accept the "operator" alias.
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

static int test_criteria_op_tree_error_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"op_tree_t\","
        "\"fields\":[\"name:varchar:32\",\"score:int\"],"
        "\"indexes\":[\"score\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"op_tree_t\",\"records\":{"
        "\"k1\":{\"name\":\"low\",\"score\":10},"
        "\"k2\":{\"name\":\"high\",\"score\":90}"
        "}}", &resp); free(resp); resp = NULL;

    /* 1. OR tree with "operator" alias — must work */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"op_tree_t\","
        "\"criteria\":[{\"or\":["
        "{\"field\":\"score\",\"operator\":\"gt\",\"value\":50},"
        "{\"field\":\"name\",\"operator\":\"eq\",\"value\":\"low\"}"
        "]}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "OR tree with operator alias response not null");
    ASSERT_CONTAINS(resp, "\"name\":\"high\"", "OR tree: high matches score>50");
    ASSERT_CONTAINS(resp, "\"name\":\"low\"", "OR tree: low matches name eq");
    free(resp); resp = NULL;

    /* 2. OR tree with unrecognised op — must return error */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"op_tree_t\","
        "\"criteria\":[{\"or\":["
        "{\"field\":\"score\",\"op\":\"bogus\",\"value\":50}"
        "]}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "OR tree with bogus op response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "OR tree: bogus op returns error");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-criteria-op-tree-error", test_criteria_op_tree_error_run)
```

### 6c — `src/test/cases/test_criteria_field_value_validation.c`

Tests that missing field returns error, and missing value for non-existence
ops returns error. Also tests that `exists`/`not_exists` work without a value.

```c
/* src/test/cases/test_criteria_field_value_validation.c
 * Regression: missing "field" must return error (not silent empty match).
 * Missing "value" for ops that require it must return error (not silent
 * empty-string match).  exists/not_exists must still work without "value".
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

static int test_criteria_field_value_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"fv_val_t\","
        "\"fields\":[\"name:varchar:32\",\"tag:varchar:16\"],"
        "\"indexes\":[\"tag\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"fv_val_t\",\"records\":{"
        "\"k1\":{\"name\":\"alpha\",\"tag\":\"red\"},"
        "\"k2\":{\"name\":\"beta\"}"
        "}}", &resp); free(resp); resp = NULL;

    /* 1. Missing "field" returns error */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fv_val_t\","
        "\"criteria\":[{\"op\":\"eq\",\"value\":\"x\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "missing field response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "missing field returns error");
    free(resp); resp = NULL;

    /* 2. Missing "value" for eq returns error */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fv_val_t\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "missing value for eq response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "missing value for eq returns error");
    free(resp); resp = NULL;

    /* 3. Missing "value" for gt returns error */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fv_val_t\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"gt\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "missing value for gt response not null");
    ASSERT_CONTAINS(resp, "\"error\"", "missing value for gt returns error");
    free(resp); resp = NULL;

    /* 4. exists without "value" must work — tag field exists on k1, not k2 */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fv_val_t\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"exists\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "exists without value response not null");
    ASSERT_CONTAINS(resp, "\"name\":\"alpha\"", "exists: alpha has tag");
    ASSERT_TRUE(strstr(resp, "\"name\":\"beta\"") == NULL,
                "exists: beta has no tag");
    free(resp); resp = NULL;

    /* 5. not_exists without "value" must work */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"fv_val_t\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"not_exists\"}]}",
        &resp);
    ASSERT_NOT_NULL(resp, "not_exists without value response not null");
    ASSERT_CONTAINS(resp, "\"name\":\"beta\"", "not_exists: beta has no tag");
    ASSERT_TRUE(strstr(resp, "\"name\":\"alpha\"") == NULL,
                "not_exists: alpha has tag");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-criteria-field-value-validation", test_criteria_field_value_validation_run)
```

Build + run-all. Confirm the `# total:` line matches pre-change count + 3.

## Task 7 — Register the 3 new test files in `build.sh`

Locate the anchor (last line of the test-case file list, unique in file):

```
    src/test/cases/test_coverity_join_buf_overflow.c \
```

Replace with:

```
    src/test/cases/test_coverity_join_buf_overflow.c \
    src/test/cases/test_criteria_operator_alias.c \
    src/test/cases/test_criteria_op_tree_error.c \
    src/test/cases/test_criteria_field_value_validation.c \
```

Build + run-all. Confirm the `# total:` line matches pre-Task-6 count + 3,
and that `./build/bin/shard-db-test list` shows all 3 new case names.

## Files changed

| File | Change |
|---|---|
| `src/db/types.h` | Add `OP_UNKNOWN` sentinel to `enum SearchOp` |
| `src/db/query.c` | `parse_op()` returns `OP_UNKNOWN` instead of `OP_EQUAL` for unrecognized strings |
| `src/db/query_plan.c` | `parse_one_criterion()` returns `int`, adds `"operator"` alias, validates field/op/value. Adds `op_requires_value()` helper. `parse_criteria_json()` and `parse_criteria_tree()` propagate errors. |
| `src/test/cases/test_criteria_operator_alias.c` | New — tests "operator" alias, unknown op error, missing op error |
| `src/test/cases/test_criteria_op_tree_error.c` | New — tests tree-path error propagation for unknown op |
| `src/test/cases/test_criteria_field_value_validation.c` | New — tests missing field error, missing value error, exists/not_exists without value |
| `build.sh` | Add the 3 new test-case files to the `shard-db-test` gcc link line |

`docs/query-protocol/find.md` and other query-protocol docs already use
`"op"` — no doc changes needed. The `"operator"` alias is a convenience
addition; the canonical key remains `"op"`.
