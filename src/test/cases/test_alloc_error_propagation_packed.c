/* Packed copy of test_alloc_error_propagation.c (class K original stays
 * legacy until T3). Scenario 2 (a parallel intersect worker's allocation
 * failure must reach the caller as errno) is copied on a packed object.
 * Scenarios 1a/1b are dropped: they inject allocation failures into the
 * legacy bulk-index staging/wrapper path, whose fault points do not line up
 * with the packed bulk path (recorded packed gap).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/query_internal.h"
#include "../db/shard_db.h"
#include "../db/typed_row.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern void build_keyset_leaf_test_set_fail_alloc(int fail_n);

static void aep_request(ShardDb *db, const char *req, char **resp) {
    tu_pdb_request(db, req, resp);
    ASSERT_NOT_NULL(*resp, "response");
}

static void aep_create_packed(ShardDb *db, const char *obj, const char *fields,
                              const char *indexes) {
    char req[512], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"declaration_layout\":\"packed\",\"dir\":\"default\",\"object\":\"%s\","
             "\"splits\":8,\"max_key\":16,\"fields\":[%s],\"indexes\":[%s]}",
             obj, fields, indexes);
    aep_request(db, req, &resp);
    ASSERT_TRUE(!strstr(resp, "\"error\""), "create packed object");
    shard_db_free_result(resp);
}

static int test_alloc_error_propagation_packed_run(void) {
    ShardDb *db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;

    char *resp = NULL;
    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    shard_db_free_result(resp); resp = NULL;

    aep_create_packed(db, "aep_ip",
                      "\"f0:int\",\"f1:int\",\"f2:int\"",
                      "\"f0\",\"f1\",\"f2\"");
    aep_request(db,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"aep_ip\","
        "\"key\":\"i1\",\"value\":{\"f0\":1,\"f1\":1,\"f2\":1}}", &resp);
    ASSERT_TRUE(!strstr(resp, "\"error\""), "seed i1");
    shard_db_free_result(resp); resp = NULL;
    aep_request(db,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"aep_ip\","
        "\"key\":\"i2\",\"value\":{\"f0\":2,\"f1\":2,\"f2\":2}}", &resp);
    ASSERT_TRUE(!strstr(resp, "\"error\""), "seed i2");
    shard_db_free_result(resp); resp = NULL;

    TypedSchema *ts = calloc(1, sizeof(*ts));
    ASSERT_NOT_NULL(ts, "intersect schema");
    if (!ts) return 1;
    ts->typed = 1; ts->nfields = 3;
    const char *types[3] = {"int", "int", "int"};
    for (int i = 0; i < 3; i++) {
        char name[8]; snprintf(name, sizeof(name), "f%d", i);
        snprintf(ts->fields[i].name, sizeof(ts->fields[i].name), "%s", name);
        ts->fields[i].name_len = 2;
        ts->fields[i].type = FT_INT;
        ts->fields[i].size = 4;
    }
    ASSERT_EQ_INT(typed_schema_bind_packed(ts, types, 3, NULL, 0), 0,
                  "packed intersect schema binds");

    const char *leaf_json[3] = {
        "{\"field\":\"f0\",\"op\":\"eq\",\"value\":1}",
        "{\"field\":\"f1\",\"op\":\"eq\",\"value\":1}",
        "{\"field\":\"f2\",\"op\":\"eq\",\"value\":1}",
    };
    CriteriaNode *nodes[3];
    SearchCriterion *leaves[3];
    for (int i = 0; i < 3; i++) {
        const char *err = NULL;
        nodes[i] = parse_criteria_tree(leaf_json[i], &err);
        ASSERT_NOT_NULL(nodes[i], "leaf parse");
        ASSERT_TRUE(!err, "leaf parse error");
        ASSERT_TRUE(compile_criteria_tree_checked(nodes[i], ts, NULL, 0) == 0,
                    "leaf compile");
        leaves[i] = &nodes[i]->leaf;
    }

    aep_request(db,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"aep_ip\","
        "\"criteria\":{\"field\":\"f0\",\"op\":\"eq\",\"value\":1}}", &resp);
    ASSERT_TRUE(strstr(resp, "i1") != NULL, "indexed find route works");
    shard_db_free_result(resp); resp = NULL;

    parallel_pool_init(2);
    ASSERT_TRUE(parallel_pool_size() == 2, "parallel pool live");
    char root[PATH_MAX];
    snprintf(root, sizeof(root), "%s/default", db->db_root);
    QueryDeadline dl = { now_ms_coarse(), 0, 0 };
    int sp = 0;
    errno = 0;
    KeySet *ks = intersect_indexed_leaves(root, "aep_ip", 8, leaves, 3,
                                          &dl, &sp);
    ASSERT_NOT_NULL(ks, "control intersect succeeds");
    keyset_free(ks);

    build_keyset_leaf_test_set_fail_alloc(1);
    errno = 0;
    ks = intersect_indexed_leaves(root, "aep_ip", 8, leaves, 3, &dl, &sp);
    ASSERT_TRUE(ks == NULL, "injected worker failure yields NULL");
    ASSERT_TRUE(errno == ENOMEM, "worker errno carried through join");
    build_keyset_leaf_test_set_fail_alloc(0);

    parallel_pool_init(0);
    for (int i = 0; i < 3; i++) free_criteria_tree(nodes[i]);
    free(ts);
    tu_pdb_drop_object(db, "default", "aep_ip");
    return 0;
}

TEST_REGISTER("test-alloc-error-propagation-packed",
              test_alloc_error_propagation_packed_run)
