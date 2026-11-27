/* AUD-QRY-2 case 15 — in-process fast-path-retention assertions.
 *
 * Wire assertions can only see results; explain cannot show
 * `has_subtree`. This case calls the planner directly and pins the
 * flag/gate values per criteria shape:
 *
 *   has_subtree == 0 AND fully_covered == 1 (fast paths retained):
 *     - [{"or":[v eq, w eq]}]   array form (single OR child; union exact)
 *     - {"or":[v eq, w eq]}     OR root (union exact)
 *     - [t-range]               plain leaf
 *   has_subtree == 1 AND fully_covered == 0 (gates must refuse):
 *     - [t-range, OR[...]]      AND with an OR sub-tree child
 *     - AND-of-two-ORs          (nL==0 partial union; n_children > 1)
 *     - 9-leaf AND              (collect_and_leaves drops leaves 9+ —
 *                                AUD-QRY-10; "criteria not fully
 *                                represented by the plan")
 *
 * This is the ONLY test that fails if the nL==0 rule wrongly fires
 * (the array-form wire control pins result correctness only).
 *
 * NOTE for the base red run: `FilterPlan.has_subtree` does not exist
 * before the Task 2 planner change — this file is EXPECTED to fail to
 * compile on base ("no member named 'has_subtree'"), which is the
 * recorded red for case 15 ("field absent"). It compiles and asserts
 * from Task 2 onward.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "../../db/types.h"
#include "../../db/query_internal.h"
#include "../test_client.h"
#include "../fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Mirror of query.c's schema seam: load_schema + init_field_schema are
 * the same entry points cmd_aggregate_do uses for plan_filter. */
extern Schema load_schema(const char *effective_root, const char *object);

typedef struct {
    int expect_has_subtree;      /* -1 = don't assert the flag */
    int expect_fully_covered;
    const char *criteria_json;
    const char *label;
} FpCase;

static int test_agg_subtree_flag_fields_run(void) {
    /* Process-local DB (pattern: test-topn-eligible-truth-table). */
    ShardDb *tc = test_get_process_db();
    ASSERT_NOT_NULL(tc, "process db");
    if (!tc) return 1;
    const char *root = test_get_process_db_root();
    ASSERT_NOT_NULL(root, "process db root");

    char *resp = NULL;
    tu_pdb_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    shard_db_free_result(resp); resp = NULL;

    tu_pdb_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"agg_fp15\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:48\",\"u:int\",\"t:long\",\"w:varchar:16\"],"
        "\"indexes\":[\"v\",\"t\",\"w\"]}", &resp);
    shard_db_free_result(resp); resp = NULL;
    tu_pdb_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"agg_fp15\","
        "\"key\":\"k0\",\"value\":{\"v\":\"cat_1\",\"u\":3,\"t\":1,"
        "\"w\":\"w_1\"}}", &resp);
    shard_db_free_result(resp); resp = NULL;

    /* The planner entry points take the EFFECTIVE root ($DB_ROOT/<dir>)
     * plus the BARE object name — mirror build_effective_root. */
    char eff_root[512], objname[64];
    snprintf(objname, sizeof(objname), "agg_fp15");
    snprintf(eff_root, sizeof(eff_root), "%s/default", root);

    Schema sch = load_schema(eff_root, objname);
    FieldSchema fs;
    init_field_schema(&fs, eff_root, objname);

    const FpCase cases[] = {
        /* retained fast paths */
        { 0, 1,
          "[{\"or\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
          "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"}]}]",
          "array-form single OR (union exact)" },
        { 0, 1,
          "{\"or\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
          "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"}]}",
          "OR root (union exact)" },
        { 0, 1,
          "[{\"field\":\"t\",\"op\":\"between\",\"value\":50,\"value2\":149}]",
          "plain t-range leaf" },
        /* gates must refuse */
        { 1, 0,
          "[{\"field\":\"t\",\"op\":\"between\",\"value\":50,\"value2\":149},"
          "{\"or\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
          "{\"field\":\"u\",\"op\":\"eq\",\"value\":7}]}]",
          "AND + OR sub-tree" },
        { 1, 0,
          "[{\"or\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
          "{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_2\"}]},"
          "{\"or\":[{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_0\"},"
          "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_2\"}]}]",
          "AND-of-two-ORs (nL==0 partial union)" },
        { 1, 0,
          "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
          "{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
          "{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
          "{\"field\":\"v\",\"op\":\"eq\",\"value\":\"cat_1\"},"
          "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"},"
          "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"},"
          "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"},"
          "{\"field\":\"w\",\"op\":\"eq\",\"value\":\"w_1\"},"
          "{\"field\":\"u\",\"op\":\"eq\",\"value\":7}]",
          "9-leaf AND (leaf 9+ dropped by collect_and_leaves)" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const char *err = NULL;
        CriteriaNode *tree = parse_criteria_tree(cases[i].criteria_json,
                                                 &err);
        ASSERT_NOT_NULL(tree, cases[i].label);
        if (!tree) continue;

        FilterPlan fp = plan_filter(tree, eff_root, objname, &fs,
                                    sch.splits, 400 /*N*/,
                                    NULL /*order_by*/, 0 /*fetching*/,
                                    0 /*limit*/);
        int covered = agg_criteria_fully_covered(eff_root, objname, tree);

        if (cases[i].expect_has_subtree >= 0) {
            ASSERT_EQ_INT(fp.has_subtree, cases[i].expect_has_subtree,
                          cases[i].label);
        }
        ASSERT_EQ_INT(covered, cases[i].expect_fully_covered,
                      cases[i].label);
        free_criteria_tree(tree);
    }

    if (tu_pdb_drop_object(tc, "default", "agg_fp15") != 0) return 1;
    return 0;
}

TEST_REGISTER("test-agg-subtree-flag-fields", test_agg_subtree_flag_fields_run)
