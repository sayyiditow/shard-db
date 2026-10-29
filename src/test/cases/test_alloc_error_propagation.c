/* src/test/cases/test_alloc_error_propagation.c
 *
 * Fault-injection regressions for the review-found error-propagation
 * gaps (2026-10-29 review round):
 *
 * 1. v2 bulk insert preparation (legacy bitmap/trigram path) must treat
 *    payload allocation failures as errors that abort the window — never
 *    as an absent key silently dropping the index update. Both
 *    allocation sites are covered: the staging buffer (query_bulk.c)
 *    and the allocating wrapper (typed_index.c, reached by oversized
 *    trigram payloads). A failed window leaves no live record and no
 *    count change.
 * 2. intersect_indexed_leaves must carry a failed worker's errno through
 *    the parallel join; losing it made worker-only allocation failures
 *    indistinguishable from an empty intersection for errno-checked
 *    callers (intersect/aggregate consumers).
 *
 * In-process (process-local ShardDb) so the TEST_BUILD hooks are
 * reachable; a parallel pool is started/stopped for the worker join.
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
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern void bulk_ins_payload_test_set_fail_alloc(int fail_n);
extern void typed_index_test_set_fail_alloc(int fail_n);
extern void build_keyset_leaf_test_set_fail_alloc(int fail_n);

static void aep_request(ShardDb *db, const char *req, char **resp) {
    tu_pdb_request(db, req, resp);
    ASSERT_NOT_NULL(*resp, "response");
}

static void aep_drop(ShardDb *db, const char *obj) {
    tu_pdb_drop_object(db, "default", obj);
}

static void aep_count_is(ShardDb *db, const char *obj, const char *want,
                         const char *what) {
    char req[256], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\"}", obj);
    aep_request(db, req, &resp);
    ASSERT_TRUE(strstr(resp, want) != NULL, what);
    shard_db_free_result(resp);
}

static void aep_create(ShardDb *db, const char *obj, const char *fields,
                       const char *indexes) {
    char req[512], *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
             "\"splits\":8,\"max_key\":16,\"fields\":[%s],\"indexes\":[%s]}",
             obj, fields, indexes);
    aep_request(db, req, &resp);
    ASSERT_TRUE(!strstr(resp, "\"error\""), "create object");
    shard_db_free_result(resp);
}

static int test_alloc_error_propagation_run(void) {
    ShardDb *db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;

    char *resp = NULL;
    char req[1200];
    char big[701];
    memset(big, 't', sizeof(big) - 1);
    big[sizeof(big) - 1] = '\0';

    tu_pdb_request(db, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    shard_db_free_result(resp); resp = NULL;

    /* ---- Scenario 1a: oversized trigram payload; the allocating
       wrapper's allocation fails. The window must abort — never stage a
       successful row whose index update was silently dropped. ---- */
    aep_create(db, "aep_w", "\"v:varchar:8192\"", "\"v:trigram\"");
    int n = snprintf(req, sizeof(req),
                     "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                     "\"object\":\"aep_w\",\"records\":[{\"key\":\"k1\","
                     "\"value\":{\"v\":\"%s\"}}]}", big);
    ASSERT_TRUE(n > 0 && (size_t)n < sizeof(req), "big request built");

    typed_index_test_set_fail_alloc(1);
    aep_request(db, req, &resp);
    ASSERT_TRUE(strstr(resp, "\"inserted\":1") == NULL,
                "wrapper alloc failure is not a successful insert");
    ASSERT_TRUE(strstr(resp, "\"error\"") != NULL,
                "wrapper alloc failure reports an error");
    shard_db_free_result(resp); resp = NULL;
    typed_index_test_set_fail_alloc(0);

    aep_request(db, "{\"mode\":\"get\",\"dir\":\"default\","
                    "\"object\":\"aep_w\",\"keys\":[\"k1\"]}", &resp);
    ASSERT_TRUE(strstr(resp, "\"k1\":null") != NULL, "rejected row absent");
    shard_db_free_result(resp); resp = NULL;
    aep_count_is(db, "aep_w", "0", "rejected row leaves count 0");

    /* Control: same request without injection commits. */
    aep_request(db, req, &resp);
    ASSERT_TRUE(strstr(resp, "\"inserted\":1") != NULL,
                "control insert commits");
    shard_db_free_result(resp); resp = NULL;
    aep_count_is(db, "aep_w", "1", "control count 1");
    aep_drop(db, "aep_w");

    /* ---- Scenario 1b: staging-buffer allocation fails for a payload
       that fits the cap. Same window-abort contract. ---- */
    aep_create(db, "aep_s", "\"v:varchar:64\"", "\"v:trigram\"");
    bulk_ins_payload_test_set_fail_alloc(1);
    aep_request(db,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"aep_s\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"x\"}}]}", &resp);
    ASSERT_TRUE(strstr(resp, "\"inserted\":1") == NULL,
                "staging alloc failure is not a successful insert");
    ASSERT_TRUE(strstr(resp, "\"error\"") != NULL,
                "staging alloc failure reports an error");
    shard_db_free_result(resp); resp = NULL;
    bulk_ins_payload_test_set_fail_alloc(0);

    aep_request(db, "{\"mode\":\"get\",\"dir\":\"default\","
                    "\"object\":\"aep_s\",\"keys\":[\"k1\"]}", &resp);
    ASSERT_TRUE(strstr(resp, "\"k1\":null") != NULL,
                "staging-rejected row absent");
    shard_db_free_result(resp); resp = NULL;
    aep_count_is(db, "aep_s", "0", "staging-rejected row leaves count 0");

    aep_request(db,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"aep_s\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"x\"}}]}", &resp);
    ASSERT_TRUE(strstr(resp, "\"inserted\":1") != NULL,
                "staging control insert commits");
    shard_db_free_result(resp); resp = NULL;
    aep_count_is(db, "aep_s", "1", "staging control count 1");
    aep_drop(db, "aep_s");

    /* ---- Scenario 2: a parallel intersect worker's allocation failure
       must reach the caller as errno, not as a silent empty set. ---- */
    aep_create(db, "aep_i",
               "\"f0:int\",\"f1:int\",\"f2:int\"",
               "\"f0\",\"f1\",\"f2\"");
    aep_request(db,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"aep_i\","
        "\"key\":\"i1\",\"value\":{\"f0\":1,\"f1\":1,\"f2\":1}}", &resp);
    ASSERT_TRUE(!strstr(resp, "\"error\""), "seed i1");
    shard_db_free_result(resp); resp = NULL;
    aep_request(db,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"aep_i\","
        "\"key\":\"i2\",\"value\":{\"f0\":2,\"f1\":2,\"f2\":2}}", &resp);
    ASSERT_TRUE(!strstr(resp, "\"error\""), "seed i2");
    shard_db_free_result(resp); resp = NULL;

    TypedSchema *ts = calloc(1, sizeof(*ts));
    ASSERT_NOT_NULL(ts, "intersect schema");
    if (!ts) return 1;
    ts->typed = 1; ts->nfields = 3; ts->nullmap_prefix = 2; ts->total_size = 14;
    for (int i = 0; i < 3; i++) {
        char name[8]; snprintf(name, sizeof(name), "f%d", i);
        parse_field_type("int", &ts->fields[i]);
        snprintf(ts->fields[i].name, sizeof(ts->fields[i].name), "%s", name);
        ts->fields[i].name_len = 2;
        ts->fields[i].offset = (int)(i * 4);
    }
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
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"aep_i\","
        "\"criteria\":{\"field\":\"f0\",\"op\":\"eq\",\"value\":1}}", &resp);
    ASSERT_TRUE(strstr(resp, "i1") != NULL, "indexed find route works");
    shard_db_free_result(resp); resp = NULL;

    parallel_pool_init(2);
    ASSERT_TRUE(parallel_pool_size() == 2, "parallel pool live");
    /* Direct engine calls take the tenant root ($DB_ROOT/<dir>) and
       the bare object name — the daemon JSON layer composes this
       itself before dispatch. */
    char root[PATH_MAX];
    snprintf(root, sizeof(root), "%s/default", db->db_root);
    QueryDeadline dl = { now_ms_coarse(), 0, 0 };
    int sp = 0;
    errno = 0;
    KeySet *ks = intersect_indexed_leaves(root, "aep_i", 8, leaves, 3,
                                          &dl, &sp);
    ASSERT_NOT_NULL(ks, "control intersect succeeds");
    keyset_free(ks);

    build_keyset_leaf_test_set_fail_alloc(1);
    errno = 0;
    ks = intersect_indexed_leaves(root, "aep_i", 8, leaves, 3,
                                  &dl, &sp);
    ASSERT_TRUE(ks == NULL, "injected worker failure yields NULL");
    ASSERT_TRUE(errno == ENOMEM, "worker errno carried through join");
    build_keyset_leaf_test_set_fail_alloc(0);
    parallel_pool_init(0);

    for (int i = 0; i < 3; i++) free_criteria_tree(nodes[i]);
    free(ts);
    aep_drop(db, "aep_i");

    return 0;
}

TEST_REGISTER("test-alloc-error-propagation", test_alloc_error_propagation_run)
