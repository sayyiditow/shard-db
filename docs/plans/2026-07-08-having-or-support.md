# Fix aggregate `having` OR support

## Problem

The `--having` evaluator in the aggregate engine only supports flat AND. OR
expressions parsed by `nql_parse_filter()` (NQL) or `parse_criteria_tree()`
(JSON) are silently dropped at the `CriteriaNode → JSON` conversion boundary
in `cmd_aggregate_tree()`. Both paths share this limitation — it is a query
engine bug, not an NQL-specific issue.

Root cause — two bottlenecks:

1. `cmd_aggregate_tree()` (`query_aggregate.c`, function starting
   `int cmd_aggregate_tree(...)`) serializes `CriteriaNode *having_tree`
   back to a flat JSON array, dropping OR/nested nodes at
   `if (hn->kind != CNODE_LEAF) continue;`.

2. `agg_having_match()` (`query_aggregate.c`, right before `agg_collect()`)
   evaluates a flat `SearchCriterion[]` as pure AND — no tree-aware version
   exists.

The NQL parser already handles OR/nested correctly. The `--having` flag
(`nql.c`, `nql_parse_command()`) calls `nql_parse_filter()` which produces a
full `CriteriaNode *` tree. The lossy step is entirely in the aggregate
engine.

**A second, independent defect was found during review and must be fixed in
the same change**: `cmd_aggregate_do()` computes a single `no_having` flag
from `having_json` alone, and reuses (or re-derives) that flag to gate at
least ten fast-path shortcuts that bypass the normal
collect → having-filter → sort → limit → emit pipeline entirely (count-only
metadata path, algebraic sum/avg/min/max paths, indexed min/max walks, the
neq shortcut, the vs_eligible group-by path, and the top-N streaming
eligibility check). Once having flows through `having_tree` instead of
`having_json` (Tasks 2-4 below), `no_having` must also account for
`having_tree`, or every one of those fast paths will silently skip having
filtering whenever a caller supplies one — a correctness regression, not
just a missing feature. `test_agg_neq_shortcut.c:195-203` already asserts
one instance of this (having must suppress the neq shortcut); Task 2 below
extends the same fix to the other nine gate sites and adds a dedicated
regression test.

## Scope — NQL docs

`docs/query-protocol/nql.md` was already updated (in a prior, unrelated PR)
to a `## Scope` section that correctly lists `find`/`count`/`aggregate`/joins
as covered and everything else (bulk, CAS, schema, files) as JSON-only. There
is no stale "Limitations" section left to fix — **no docs task is needed**.
(A prior draft of this plan proposed a Task 5 rewriting that section; it has
been dropped because the anchor it targeted no longer exists and the
proposed replacement text would have regressed the doc's existing mention
of NQL joins.)

## Execution rules

- Branch off `main`: `git checkout -b fix/having-or-support`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all` — must pass after every task
- Leave uncommitted; stop for review after each task
- Every anchor below is a verbatim quote from current `main`. If any quoted
  anchor is not found exactly as written, stop and write `PLAN_NOTES.md` —
  do not guess or reinterpret.

## Task 1 — Add tree-aware `agg_having_match_tree()` to `query_aggregate.c`

Add the following static function immediately after `agg_having_match()`.
Locate the anchor:

```c
/* Check having criteria against a bucket */
static int agg_having_match(AggBucket *bkt, AggSpec *specs, int nspecs,
                            SearchCriterion *having, int nhaving) {
    for (int i = 0; i < nhaving; i++) {
        double val = agg_bucket_value(bkt, specs, nspecs, having[i].field);
        char val_str[64];
        snprintf(val_str, sizeof(val_str), "%.6f", val);
        if (!match_criterion(val_str, &having[i])) return 0;
    }
    return 1;
}
```

Insert the new function directly below it (before the `/* Collect all
buckets into a flat array */` comment):

```c

/* Tree-aware having evaluation — supports OR, nested AND/OR.
   Mirrors criteria_match_tree() but evaluates against aggregate bucket
   values instead of raw record bytes. */
static int agg_having_match_tree(AggBucket *bkt, AggSpec *specs, int nspecs,
                                  const CriteriaNode *n) {
    if (!n) return 1;
    switch (n->kind) {
    case CNODE_LEAF: {
        double val = agg_bucket_value(bkt, specs, nspecs, n->leaf.field);
        char val_str[64];
        snprintf(val_str, sizeof(val_str), "%.6f", val);
        return match_criterion(val_str, &n->leaf);
    }
    case CNODE_AND:
        for (int i = 0; i < n->n_children; i++)
            if (!agg_having_match_tree(bkt, specs, nspecs, n->children[i]))
                return 0;
        return 1;
    case CNODE_OR:
        for (int i = 0; i < n->n_children; i++)
            if (agg_having_match_tree(bkt, specs, nspecs, n->children[i]))
                return 1;
        return 0;
    }
    return 0;
}
```

Build + run-all. Must be green (function is unused yet, so expect an
`unused function` warning at most — not an error; if the build treats
warnings as errors, this is expected to resolve once Task 2 adds the call
site).

## Task 2 — Modify `cmd_aggregate_do()` to accept `CriteriaNode *having_tree`

This task touches four separate sites inside the same function. Apply all
four edits together, then build once.

**IMPORTANT**: `cmd_aggregate_do`'s existing criteria parameter is named
`tree` and is referenced by that name ~40 times throughout the function
body (e.g. `if (!tree && no_group ...)`, `validate_criteria_tree_fields(tree,
...)`, `compile_criteria_tree(tree, ...)`). Do **not** rename it — only add
the new `having_tree` parameter.

### 2a — Signature

Locate the anchor:

```c
static int cmd_aggregate_do(const char *db_root, const char *object,
                            CriteriaNode *tree,
                            AggSpec *specs, int nspecs,
                            const char *group_by_json,
                            const char *having_json,
                            const char *order_by, int order_desc, int limit,
                            const char *format, const char *delimiter, int want_total) {
```

Replace with:

```c
static int cmd_aggregate_do(const char *db_root, const char *object,
                            CriteriaNode *tree,
                            AggSpec *specs, int nspecs,
                            const char *group_by_json,
                            const char *having_json,
                            CriteriaNode *having_tree,   /* NEW — priority over having_json */
                            const char *order_by, int order_desc, int limit,
                            const char *format, const char *delimiter, int want_total) {
```

### 2b — Fix `no_having` to account for `having_tree`

Locate the anchor (unique in the file):

```c
    int no_having = (!having_json || having_json[0] == '\0');
```

Replace with:

```c
    int no_having = !having_tree && (!having_json || having_json[0] == '\0');
```

This is the single source-of-truth fix: every other fast-path gate in this
function reads the `no_having` variable, so this one edit propagates
correctly to the count-only metadata path, the algebraic sum/avg/min/max
paths, the indexed min/max walks, and the `vs_eligible` group-by path.

### 2c — Fix the top-N streaming eligibility call (does NOT read `no_having`)

This call site checks `having_json` directly rather than through the
`no_having` variable, so it needs its own fix. Locate the anchor:

```c
        if (gb_csv[0] && order_by && order_by[0] &&
            eligible_for_topn_stream(db_root, object, specs, nspecs,
                                      gb_csv, order_by, limit, having_json)) {
```

Replace with:

```c
        if (gb_csv[0] && order_by && order_by[0] &&
            eligible_for_topn_stream(db_root, object, specs, nspecs,
                                      gb_csv, order_by, limit,
                                      having_tree ? "1" : having_json)) {
```

Do **not** change `eligible_for_topn_stream()`'s own signature — it is
called directly (with a `TestAggSpec` shim) from
`src/test/cases/test_agg_topn_stream.c`, and its body only checks
truthiness (`if (having && *having) return 0;`), so passing a non-NULL
sentinel string when `having_tree` is set is sufficient and does not
require touching that function or its existing tests.

### 2d — Fix the neq-shortcut's own recomputed check

This is a second direct `having_json` check that does not go through the
`no_having` variable. Locate the anchor (unique in the file):

```c
    int neq_eligible = 0;
    if (no_group && (!having_json || having_json[0] == '\0') &&
        neq_leaf_node && neq_leaf_node->leaf.op == OP_NOT_EQUAL) {
```

Replace with:

```c
    int neq_eligible = 0;
    if (no_group && no_having &&
        neq_leaf_node && neq_leaf_node->leaf.op == OP_NOT_EQUAL) {
```

(Reuses the already-fixed `no_having` variable instead of re-deriving it —
`no_having` is in scope here, declared earlier in the same function.)

### 2e — Having-filter application site

Locate the anchor:

```c
    /* Apply having filter */
    SearchCriterion *having = NULL;
    int nhaving = 0;
    if (having_json && having_json[0])
        parse_criteria_json(having_json, &having, &nhaving);

    if (nhaving > 0) {
        int dst = 0;
        for (int i = 0; i < nbuckets; i++) {
            if (agg_having_match(buckets[i], specs, nspecs, having, nhaving))
                buckets[dst++] = buckets[i];
        }
        nbuckets = dst;
    }
```

Replace with:

```c
    /* Apply having filter — tree path takes priority (supports OR/nested);
       flat-array JSON path kept for having_json callers. `having`/`nhaving`
       stay declared at this scope (not narrowed into a branch) because two
       later exit paths in this function call free_criteria(having, nhaving)
       unconditionally — narrowing their scope would break those. */
    SearchCriterion *having = NULL;
    int nhaving = 0;
    if (having_tree) {
        int dst = 0;
        for (int i = 0; i < nbuckets; i++) {
            if (agg_having_match_tree(buckets[i], specs, nspecs, having_tree))
                buckets[dst++] = buckets[i];
        }
        nbuckets = dst;
    } else {
        if (having_json && having_json[0])
            parse_criteria_json(having_json, &having, &nhaving);

        if (nhaving > 0) {
            int dst = 0;
            for (int i = 0; i < nbuckets; i++) {
                if (agg_having_match(buckets[i], specs, nspecs, having, nhaving))
                    buckets[dst++] = buckets[i];
            }
            nbuckets = dst;
        }
    }
```

Do **not** touch the two existing `free_criteria(having, nhaving);` calls
later in the function (one in the CSV early-return block, one at the final
JSON-emit return) — they still compile and safely no-op (`having` is
`NULL`/`nhaving` is `0`) when the tree path was taken.

### 2f — Update the one call site that exists so far

Locate the anchor (inside `cmd_aggregate()`):

```c
    int r = cmd_aggregate_do(db_root, object, tree, specs, nspecs,
                            group_by_json, having_json,
                            order_by, order_desc, limit,
                            format, delimiter, want_total);
```

Replace with:

```c
    int r = cmd_aggregate_do(db_root, object, tree, specs, nspecs,
                            group_by_json, having_json,
                            NULL,   /* having_tree — JSON path uses having_json for now */
                            order_by, order_desc, limit,
                            format, delimiter, want_total);
```

Build + run-all. Must be green (only `cmd_aggregate` calls `cmd_aggregate_do`
at this point, and it passes NULL for the new parameter — behavior is
unchanged until Task 3).

## Task 3 — Modify `cmd_aggregate()` to parse `having_json` into a tree

Locate the anchor:

```c
int cmd_aggregate(const char *db_root, const char *object,
                  const char *criteria_json, const char *group_by_json,
                  const char *aggregates_json, const char *having_json,
                  const char *order_by, int order_desc, int limit,
                  const char *format, const char *delimiter, int want_total) {
    if (!aggregates_json || aggregates_json[0] == '\0') {
        OUT("{\"error\":\"Missing aggregates\"}\n");
        return -1;
    }

    AggSpec *specs = NULL;
    int nspecs = parse_agg_specs(aggregates_json, &specs);
    if (nspecs == 0) {
        OUT("{\"error\":\"No valid aggregates\"}\n");
        free(specs);
        return -1;
    }

    CriteriaNode *tree = NULL;
    if (criteria_json && criteria_json[0]) {
        const char *perr = NULL;
        tree = parse_criteria_tree(criteria_json, &perr);
        if (perr) {
            OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
            free_criteria_tree(tree);
            free(specs);
            return -1;
        }
    }

    int r = cmd_aggregate_do(db_root, object, tree, specs, nspecs,
                            group_by_json, having_json,
                            NULL,   /* having_tree — JSON path uses having_json for now */
                            order_by, order_desc, limit,
                            format, delimiter, want_total);
    free_criteria_tree(tree);
    free(specs);
    return r;
}
```

(Note: this whole block already reflects Task 2f's edit — you are replacing
that intermediate state.)

Replace with:

```c
int cmd_aggregate(const char *db_root, const char *object,
                  const char *criteria_json, const char *group_by_json,
                  const char *aggregates_json, const char *having_json,
                  const char *order_by, int order_desc, int limit,
                  const char *format, const char *delimiter, int want_total) {
    if (!aggregates_json || aggregates_json[0] == '\0') {
        OUT("{\"error\":\"Missing aggregates\"}\n");
        return -1;
    }

    AggSpec *specs = NULL;
    int nspecs = parse_agg_specs(aggregates_json, &specs);
    if (nspecs == 0) {
        OUT("{\"error\":\"No valid aggregates\"}\n");
        free(specs);
        return -1;
    }

    CriteriaNode *tree = NULL;
    if (criteria_json && criteria_json[0]) {
        const char *perr = NULL;
        tree = parse_criteria_tree(criteria_json, &perr);
        if (perr) {
            OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
            free_criteria_tree(tree);
            free(specs);
            return -1;
        }
    }

    CriteriaNode *having_tree = NULL;
    if (having_json && having_json[0]) {
        const char *herr = NULL;
        having_tree = parse_criteria_tree(having_json, &herr);
        if (herr) {
            OUT("{\"error\":\"bad having: %s\"}\n", herr);
            free_criteria_tree(having_tree);
            free_criteria_tree(tree);
            free(specs);
            return -1;
        }
    }

    int r = cmd_aggregate_do(db_root, object, tree, specs, nspecs,
                            group_by_json, NULL,
                            having_tree,
                            order_by, order_desc, limit,
                            format, delimiter, want_total);
    free_criteria_tree(having_tree);
    free_criteria_tree(tree);
    free(specs);
    return r;
}
```

Note: `parse_criteria_tree()` already handles OR/nested AND/OR — it is the
same parser used for `criteria_json`. Passing `NULL` for `having_json` and
the tree for `having_tree` routes everything through the tree-aware
evaluator, and the Task 2b fix keeps the fast-path gates correct now that
`having_json` is always NULL here.

Build + run-all. Must be green. The existing aggregate tests exercise the
JSON having path — they should now pass through `agg_having_match_tree()`.

## Task 4 — Modify `cmd_aggregate_tree()` to pass tree directly

Locate the anchor (the lossy `CriteriaNode → JSON` serialization block,
starting right after the `group_by_csv_to_json` conversion):

```c
    /* Convert CriteriaNode *having_tree to having_json */
    char having_buf[4096] = {0};
    if (having_tree) {
        int hpos = 0, hcount = 0;
        having_buf[hpos++] = '[';
        int n_nodes = (having_tree->kind == CNODE_AND) ? having_tree->n_children : 1;
        for (int i = 0; i < n_nodes; i++) {
            CriteriaNode *hn = (having_tree->kind == CNODE_AND) ? having_tree->children[i] : having_tree;
            if (hn->kind != CNODE_LEAF) continue;
            SearchCriterion *sc = &hn->leaf;
            const char *op_str = "eq";
            switch (sc->op) {
                case OP_EQUAL:       op_str = "eq"; break;
                case OP_NOT_EQUAL:   op_str = "neq"; break;
                case OP_LESS:        op_str = "lt"; break;
                case OP_GREATER:     op_str = "gt"; break;
                case OP_LESS_EQ:     op_str = "lte"; break;
                case OP_GREATER_EQ:  op_str = "gte"; break;
                default: break;
            }
            int remain = (int)sizeof(having_buf) - hpos;
            int n = snprintf(having_buf + hpos, remain,
                            "%s{\"field\":\"%s\",\"op\":\"%s\",\"value\":\"%s\"}",
                            hcount > 0 ? "," : "",
                            sc->field, op_str, sc->value);
            if (n > 0 && n < remain) hpos += n;
            hcount++;
        }
        if (hpos + 2 <= (int)sizeof(having_buf)) {
            having_buf[hpos++] = ']';
            having_buf[hpos] = '\0';
        }
    }

    int r = cmd_aggregate_do(db_root, object, criteria_tree, specs, naggs,
                            group_by_buf,
                            having_buf[0] ? having_buf : NULL,
                            order_by, order_desc, limit,
                            format, delimiter, want_total);
```

Replace with:

```c
    int r = cmd_aggregate_do(db_root, object, criteria_tree, specs, naggs,
                            group_by_buf,
                            NULL,                  /* having_json — not used */
                            having_tree,           /* passed directly */
                            order_by, order_desc, limit,
                            format, delimiter, want_total);
```

This deletes the entire lossy serialization block (the `having_buf`
declaration through its closing brace) and passes `having_tree` straight
through. `cmd_aggregate_tree()` does not own `having_tree` — its caller
(`dispatch_nql_query()` in `server.c`) frees it via
`nql_free_command(&cmd)` → `free_criteria_tree(cmd->having)`, so no free is
added here.

Build + run-all. Both paths now use tree-aware having evaluation.

## Task 5 — Add tests

Add five new test-case files under `src/test/cases/`. Each spawns its own
daemon at a unique tmpdir + port (`test_env_start`/`test_env_stop`, the
standard harness pattern — see any existing `test_agg_*.c` file). Register
each via `TEST_REGISTER`.

### 5a — `src/test/cases/test_agg_having_or.c`

NQL `--having` with OR, no nesting, over the raw NQL-over-TCP protocol.

```c
/* src/test/cases/test_agg_having_or.c
 * Aggregate --having OR support via the NQL raw-text protocol.
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

static int test_agg_having_or_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"having_or_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\"],"
        "\"indexes\":[\"status\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    /* paid: 5 rows, amounts 10..50 -> count=5 sum=150
       pending: 2 rows, amounts 5,5 -> count=2 sum=10
       cancelled: 1 row, amount 200 -> count=1 sum=200 */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"having_or_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":20}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"paid\",\"amount\":30}},"
        "{\"key\":\"k4\",\"value\":{\"status\":\"paid\",\"amount\":40}},"
        "{\"key\":\"k5\",\"value\":{\"status\":\"paid\",\"amount\":50}},"
        "{\"key\":\"k6\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k7\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k8\",\"value\":{\"status\":\"cancelled\",\"amount\":200}}"
        "]}", &resp); free(resp); resp = NULL;

    /* count() -> alias "count", sum(amount) -> alias "sum_amount"
       (NQL auto-alias rules). */
    tc_request(tc,
        "aggregate default having_or_t count(),sum(amount) --group-by status "
        "--having 'count gt 2 or sum_amount gt 100'",
        &resp);
    ASSERT_TRUE(resp != NULL, "got response");
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"", "OR keeps paid (count=5>2)");
    ASSERT_CONTAINS(resp, "\"status\":\"cancelled\"", "OR keeps cancelled (sum=200>100)");
    ASSERT_TRUE(strstr(resp, "\"status\":\"pending\"") == NULL,
                "OR drops pending (neither branch matches)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-having-or", test_agg_having_or_run)
```

### 5b — `src/test/cases/test_agg_having_nested.c`

NQL `--having` with nested `(A and B) or C`.

```c
/* src/test/cases/test_agg_having_nested.c
 * Aggregate --having nested AND-within-OR via the NQL raw-text protocol.
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

static int test_agg_having_nested_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"having_nested_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\"],"
        "\"indexes\":[\"status\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    /* paid: count=5 sum=150 avg=30
       pending: count=1 sum=5 avg=5
       cancelled: count=3 sum=90 avg=30
       refunded: count=1 sum=1000 avg=1000 */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"having_nested_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":20}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"paid\",\"amount\":30}},"
        "{\"key\":\"k4\",\"value\":{\"status\":\"paid\",\"amount\":40}},"
        "{\"key\":\"k5\",\"value\":{\"status\":\"paid\",\"amount\":50}},"
        "{\"key\":\"k6\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k7\",\"value\":{\"status\":\"cancelled\",\"amount\":20}},"
        "{\"key\":\"k8\",\"value\":{\"status\":\"cancelled\",\"amount\":30}},"
        "{\"key\":\"k9\",\"value\":{\"status\":\"cancelled\",\"amount\":40}},"
        "{\"key\":\"k10\",\"value\":{\"status\":\"refunded\",\"amount\":1000}}"
        "]}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "aggregate default having_nested_t count(),sum(amount),avg(amount) --group-by status "
        "--having '(count gt 2 and sum_amount gt 100) or avg_amount lt 20'",
        &resp);
    ASSERT_TRUE(resp != NULL, "got response");
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"",
                     "nested: paid kept via (count>2 and sum>100)");
    ASSERT_CONTAINS(resp, "\"status\":\"pending\"",
                     "nested: pending kept via (avg<20)");
    ASSERT_TRUE(strstr(resp, "\"status\":\"cancelled\"") == NULL,
                "nested: cancelled dropped (count>2 but sum not>100, avg not<20)");
    ASSERT_TRUE(strstr(resp, "\"status\":\"refunded\"") == NULL,
                "nested: refunded dropped (count not>2, avg not<20)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-having-nested", test_agg_having_nested_run)
```

### 5c — `src/test/cases/test_agg_having_json_or.c`

Same OR shape as 5a, via the JSON protocol (`"having":{"or":[...]}}`).

```c
/* src/test/cases/test_agg_having_json_or.c
 * Aggregate having OR support via the JSON protocol (having as {"or":[...]}).
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

static int test_agg_having_json_or_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"having_json_or_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\"],"
        "\"indexes\":[\"status\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"having_json_or_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":20}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"paid\",\"amount\":30}},"
        "{\"key\":\"k4\",\"value\":{\"status\":\"paid\",\"amount\":40}},"
        "{\"key\":\"k5\",\"value\":{\"status\":\"paid\",\"amount\":50}},"
        "{\"key\":\"k6\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k7\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k8\",\"value\":{\"status\":\"cancelled\",\"amount\":200}}"
        "]}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"having_json_or_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"s\"}],"
        "\"group_by\":[\"status\"],"
        "\"having\":{\"or\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"2\"},"
                            "{\"field\":\"s\",\"op\":\"gt\",\"value\":\"100\"}]}}",
        &resp);
    ASSERT_TRUE(resp != NULL, "got response");
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"", "JSON OR keeps paid (n=5>2)");
    ASSERT_CONTAINS(resp, "\"status\":\"cancelled\"", "JSON OR keeps cancelled (s=200>100)");
    ASSERT_TRUE(strstr(resp, "\"status\":\"pending\"") == NULL,
                "JSON OR drops pending (neither branch matches)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-having-json-or", test_agg_having_json_or_run)
```

### 5d — `src/test/cases/test_agg_having_json_and.c`

Regression: flat-array (implicit AND) having must still work once JSON
having routes through the tree-aware evaluator.

```c
/* src/test/cases/test_agg_having_json_and.c
 * Regression: flat-array (implicit AND) having still works correctly
 * once JSON having is routed through the tree-aware evaluator.
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

static int test_agg_having_json_and_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"having_json_and_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\"],"
        "\"indexes\":[\"status\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"having_json_and_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":20}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"paid\",\"amount\":30}},"
        "{\"key\":\"k4\",\"value\":{\"status\":\"paid\",\"amount\":40}},"
        "{\"key\":\"k5\",\"value\":{\"status\":\"paid\",\"amount\":50}},"
        "{\"key\":\"k6\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k7\",\"value\":{\"status\":\"pending\",\"amount\":5}},"
        "{\"key\":\"k8\",\"value\":{\"status\":\"cancelled\",\"amount\":200}}"
        "]}", &resp); free(resp); resp = NULL;

    /* Flat array = implicit AND: n>1 and s>50.
       paid: n=5>1 true, s=150>50 true -> kept.
       pending: n=2>1 true, s=10>50 false -> dropped.
       cancelled: n=1>1 false -> dropped. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"having_json_and_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"s\"}],"
        "\"group_by\":[\"status\"],"
        "\"having\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"1\"},"
                    "{\"field\":\"s\",\"op\":\"gt\",\"value\":\"50\"}]}",
        &resp);
    ASSERT_TRUE(resp != NULL, "got response");
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"", "AND keeps paid (both conditions true)");
    ASSERT_TRUE(strstr(resp, "\"status\":\"pending\"") == NULL,
                "AND drops pending (sum condition fails)");
    ASSERT_TRUE(strstr(resp, "\"status\":\"cancelled\"") == NULL,
                "AND drops cancelled (count condition fails)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-having-json-and", test_agg_having_json_and_run)
```

### 5e — `src/test/cases/test_agg_having_no_group_regression.c`

Guards against the `no_having` fast-path regression described in the
Problem section — a plain (non-OR) having filter with no `group_by`, which
would trigger the count-only metadata fast path if `no_having` ignored
`having_tree`.

```c
/* src/test/cases/test_agg_having_no_group_regression.c
 * Regression guard: cmd_aggregate_do()'s fast-path shortcuts gate on
 * `no_having`, which used to be computed from having_json alone. Once
 * having flows through having_tree instead (for both the JSON and NQL
 * entry points), no_having must also check having_tree — otherwise a
 * real having filter is silently skipped whenever a no-group-by fast
 * path fires (count-only metadata path, algebraic sum/avg/min/max path,
 * neq shortcut, top-N streaming eligibility). This test uses the
 * plainest shape that triggers the count-only fast path (no criteria,
 * no group_by, single count() spec) and asserts the having filter still
 * drops the (only) bucket when its condition is false.
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

static int test_agg_having_no_group_regression_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"having_nogroup_t\","
        "\"fields\":[\"amount:int\"],\"indexes\":[],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"having_nogroup_t\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"amount\":10}},"
        "{\"key\":\"k2\",\"value\":{\"amount\":20}},"
        "{\"key\":\"k3\",\"value\":{\"amount\":30}},"
        "{\"key\":\"k4\",\"value\":{\"amount\":40}},"
        "{\"key\":\"k5\",\"value\":{\"amount\":50}}"
        "]}", &resp); free(resp); resp = NULL;

    /* JSON path — impossible having (n>1000) must drop the single bucket
       to an empty array, NOT bypass to the raw count-only "{\"n\":5}" the
       count-only fast path would emit if no_having ignored having_tree. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"having_nogroup_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"having\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"1000\"}]}",
        &resp);
    ASSERT_TRUE(resp != NULL, "JSON: got response");
    ASSERT_TRUE(strstr(resp, "\"n\":5") == NULL,
                "JSON: count-only fast path must not bypass having filter");
    ASSERT_CONTAINS(resp, "[]", "JSON: impossible having drops the only bucket");
    free(resp); resp = NULL;

    /* JSON control — satisfiable having (n>1) must keep the bucket. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"having_nogroup_t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"having\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"1\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"n\":5", "JSON: satisfiable having keeps the bucket");
    free(resp); resp = NULL;

    /* NQL path — same impossible-having shape over the raw-text protocol. */
    tc_request(tc,
        "aggregate default having_nogroup_t count() --having 'count gt 1000'",
        &resp);
    ASSERT_TRUE(resp != NULL, "NQL: got response");
    ASSERT_TRUE(strstr(resp, "\"count\":5") == NULL,
                "NQL: count-only fast path must not bypass having filter");
    ASSERT_CONTAINS(resp, "[]", "NQL: impossible having drops the only bucket");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-having-no-group-regression", test_agg_having_no_group_regression_run)
```

Build + run-all. Confirm the `# total:` line matches pre-change count + 5.

## Task 6 — Register the 5 new test files in `build.sh`

`build.sh` lists every test-case file explicitly on the `shard-db-test` gcc
invocation — there is no glob, so the 5 files added in Task 5 will not be
compiled or linked without this edit.

Locate the anchor (last line of the test-case file list, unique in the
file):

```
    src/test/cases/test_coverity_join_buf_overflow.c \
```

Replace with:

```
    src/test/cases/test_coverity_join_buf_overflow.c \
    src/test/cases/test_agg_having_or.c \
    src/test/cases/test_agg_having_nested.c \
    src/test/cases/test_agg_having_json_or.c \
    src/test/cases/test_agg_having_json_and.c \
    src/test/cases/test_agg_having_no_group_regression.c \
```

Build + run-all. Confirm the `# total:` line matches pre-Task-5 count + 5,
and that `./build/bin/shard-db-test list` shows all 5 new case names.

## Files changed

| File | Change |
|---|---|
| `src/db/query_aggregate.c` | Add `agg_having_match_tree()`. Add `having_tree` param to `cmd_aggregate_do()`. Fix `no_having` computation (2 call sites) to account for `having_tree`. Modify `cmd_aggregate()` to parse `having_json` into a tree. Modify `cmd_aggregate_tree()` to pass the tree directly, deleting the lossy serialization block. |
| `src/test/cases/test_agg_having_or.c` | New — NQL OR having. |
| `src/test/cases/test_agg_having_nested.c` | New — NQL nested AND-within-OR having. |
| `src/test/cases/test_agg_having_json_or.c` | New — JSON OR having. |
| `src/test/cases/test_agg_having_json_and.c` | New — JSON flat-AND regression. |
| `src/test/cases/test_agg_having_no_group_regression.c` | New — guards the `no_having` fast-path regression. |
| `build.sh` | Add the 5 new test-case files to the `shard-db-test` gcc link line. |

`docs/query-protocol/nql.md` is unchanged — it is already correct.
