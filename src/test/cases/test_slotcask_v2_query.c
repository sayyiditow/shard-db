/* test_slotcask_v2_query.c — Phase-3 E2E for the query layer over v2 objects.
 *
 * Exercises find / count / keys / aggregate / find-with-cursor on a
 * storage_version=2 object via the JSON wire protocol. Confirms scan_dispatch
 * (Phase 3B+3C) and the indexed-fetch helper (Phase 3D) are wired
 * end-to-end.
 *
 * Not exercised here (deferred to Phase 3D-followup):
 *   - indexed find without order_by → routes through idx_find_parallel /
 *     process_batch which is still v1-only. Returns no rows on v2.
 *   - joins where the remote object is v2 → lookup_remote still v1-only.
 *
 * Criteria wire format (per docs/query-protocol/find.md):
 *     "criteria": [{"field":"x","op":"eq","value":"y"}, ...]
 * That's an AND-combined array of {field, op, value} objects — NOT the
 * `{"field":{"op":val}}` shape used in the engine's internal compiled
 * trees.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>

static void seed(TestClient *tc, const char *key, const char *name,
                  int age, const char *city) {
    char req[512];
    snprintf(req, sizeof(req),
             "{\"mode\":\"insert\",\"dir\":\"qry\",\"object\":\"users\","
             "\"key\":\"%s\","
             "\"value\":{\"name\":\"%s\",\"age\":%d,\"city\":\"%s\"}}",
             key, name, age, city);
    char *resp = NULL;
    tc_request(tc, req, &resp); free(resp);
}

static int test_slotcask_v2_query_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* ===== Setup ===== */
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"qry\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"qry\",\"object\":\"users\","
        "\"splits\":8,\"max_key\":40,\"storage_version\":2,"
        "\"fields\":[\"name:varchar:64\",\"age:int\",\"city:varchar:32\"],"
        "\"indexes\":[\"age\",\"city\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"storage_version\":2", "v2 object created");
    free(resp); resp = NULL;

    /* Seed 6 records — 3 cities, age range 25-50 */
    seed(tc, "u1", "alice",   25, "NYC");
    seed(tc, "u2", "bob",     30, "LON");
    seed(tc, "u3", "carol",   35, "NYC");
    seed(tc, "u4", "dave",    40, "TYO");
    seed(tc, "u5", "eve",     45, "LON");
    seed(tc, "u6", "frank",   50, "NYC");

    /* ===== count without criteria → metadata path ===== */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"qry\",\"object\":\"users\"}", &resp);
    ASSERT_CONTAINS(resp, "6", "count without criteria = 6");
    free(resp); resp = NULL;

    /* ===== count with non-indexed criteria → full scan via scan_dispatch ===== */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"name\",\"op\":\"starts\",\"value\":\"a\"}]}", &resp);
    ASSERT_CONTAINS(resp, "1", "count name starts_with 'a' = 1 (alice)");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"name\",\"op\":\"contains\",\"value\":\"a\"}]}", &resp);
    /* contains 'a': alice, carol, dave, frank = 4 */
    ASSERT_CONTAINS(resp, "4", "count name contains 'a' = 4");
    free(resp); resp = NULL;

    /* ===== find with empty criteria — full scan ===== */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"criteria\":[]}", &resp);
    ASSERT_CONTAINS(resp, "alice", "full-scan find returns alice");
    ASSERT_CONTAINS(resp, "bob",   "full-scan find returns bob");
    ASSERT_CONTAINS(resp, "frank", "full-scan find returns frank");
    free(resp); resp = NULL;

    /* ===== find with non-indexed criteria — full scan ===== */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"carol\"}]}", &resp);
    ASSERT_CONTAINS(resp, "carol", "non-indexed find returns carol");
    ASSERT_TRUE(resp && strstr(resp, "alice") == NULL,
                "non-indexed find excludes alice");
    free(resp); resp = NULL;

    /* ===== find with indexed criteria + order_by → cursor_find_cb path ===== */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"35\"}],"
                   "\"order_by\":\"age\"}", &resp);
    ASSERT_CONTAINS(resp, "carol", "indexed range w/ order_by returns carol (age=35)");
    ASSERT_CONTAINS(resp, "dave",  "indexed range w/ order_by returns dave (age=40)");
    ASSERT_CONTAINS(resp, "eve",   "indexed range w/ order_by returns eve (age=45)");
    ASSERT_CONTAINS(resp, "frank", "indexed range w/ order_by returns frank (age=50)");
    ASSERT_TRUE(resp && strstr(resp, "alice") == NULL,
                "indexed range excludes alice (age=25)");
    ASSERT_TRUE(resp && strstr(resp, "bob") == NULL,
                "indexed range excludes bob (age=30)");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"NYC\"}],"
                   "\"order_by\":\"age\"}", &resp);
    ASSERT_CONTAINS(resp, "alice", "indexed eq w/ order_by returns alice");
    ASSERT_CONTAINS(resp, "carol", "indexed eq w/ order_by returns carol");
    ASSERT_CONTAINS(resp, "frank", "indexed eq w/ order_by returns frank");
    ASSERT_TRUE(resp && strstr(resp, "bob") == NULL,
                "indexed eq excludes bob (LON)");
    ASSERT_TRUE(resp && strstr(resp, "dave") == NULL,
                "indexed eq excludes dave (TYO)");
    free(resp); resp = NULL;

    /* ===== keys — simple full-walk ===== */
    tc_request(tc, "{\"mode\":\"keys\",\"dir\":\"qry\",\"object\":\"users\"}", &resp);
    ASSERT_CONTAINS(resp, "u1", "keys returns u1");
    ASSERT_CONTAINS(resp, "u6", "keys returns u6");
    free(resp); resp = NULL;

    /* ===== aggregate count (no group) — full scan ===== */
    tc_request(tc, "{\"mode\":\"aggregate\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"total\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"total\":6", "aggregate count = 6");
    free(resp); resp = NULL;

    /* ===== aggregate sum + group_by city ===== */
    tc_request(tc, "{\"mode\":\"aggregate\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"age\",\"alias\":\"sum_age\"},"
                                   "{\"fn\":\"count\",\"alias\":\"n\"}],"
                   "\"group_by\":[\"city\"]}", &resp);
    /* NYC: alice(25)+carol(35)+frank(50) = 110, n=3 */
    ASSERT_CONTAINS(resp, "\"NYC\"", "aggregate group includes NYC");
    ASSERT_CONTAINS(resp, "\"LON\"", "aggregate group includes LON");
    ASSERT_CONTAINS(resp, "\"TYO\"", "aggregate group includes TYO");
    ASSERT_CONTAINS(resp, "\"sum_age\":110", "NYC sum_age = 110");
    /* LON: bob(30)+eve(45) = 75; TYO: dave(40) = 40 */
    ASSERT_CONTAINS(resp, "\"sum_age\":75",  "LON sum_age = 75");
    ASSERT_CONTAINS(resp, "\"sum_age\":40",  "TYO sum_age = 40");
    free(resp); resp = NULL;

    /* ===== find with offset + limit on indexed walk ===== */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"25\"}],"
                   "\"order_by\":\"age\","
                   "\"offset\":0,\"limit\":2}", &resp);
    ASSERT_CONTAINS(resp, "alice", "limit=2 includes alice (smallest age)");
    ASSERT_CONTAINS(resp, "bob",   "limit=2 includes bob (next age)");
    ASSERT_TRUE(resp && strstr(resp, "frank") == NULL,
                "limit=2 excludes frank (largest age)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-v2-query", test_slotcask_v2_query_run)
