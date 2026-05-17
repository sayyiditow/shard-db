/* test_slotcask_v2_query.c — E2E for the query layer over slotcask objects.
 *
 * Exercises find / count / keys / aggregate / find-with-cursor via the
 * JSON wire protocol. Confirms scan_dispatch and the indexed-fetch
 * helper are wired end-to-end.
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
        "\"splits\":8,\"max_key\":40,"
        "\"fields\":[\"name:varchar:64\",\"age:int\",\"city:varchar:32\"],"
        "\"indexes\":[\"age\",\"city\"]}", &resp);
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

    /* ===== Phase 3F: indexed find WITHOUT order_by → idx_find_parallel ===== */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"35\"}]}", &resp);
    ASSERT_CONTAINS(resp, "carol",
                    "indexed eq (no order_by) returns carol via idx_find_parallel");
    ASSERT_TRUE(resp && strstr(resp, "alice") == NULL,
                "indexed eq (no order_by) excludes alice");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"LON\"}]}", &resp);
    ASSERT_CONTAINS(resp, "bob", "indexed city=LON returns bob (no order_by)");
    ASSERT_CONTAINS(resp, "eve", "indexed city=LON returns eve (no order_by)");
    ASSERT_TRUE(resp && strstr(resp, "alice") == NULL,
                "indexed city=LON excludes alice (NYC)");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"age\",\"op\":\"between\",\"value\":\"30\",\"value2\":\"40\"}]}",
                   &resp);
    ASSERT_CONTAINS(resp, "bob",   "indexed between returns bob (age=30)");
    ASSERT_CONTAINS(resp, "carol", "indexed between returns carol (age=35)");
    ASSERT_CONTAINS(resp, "dave",  "indexed between returns dave (age=40)");
    ASSERT_TRUE(resp && strstr(resp, "alice") == NULL,
                "indexed between excludes alice (age=25)");
    ASSERT_TRUE(resp && strstr(resp, "frank") == NULL,
                "indexed between excludes frank (age=50)");
    free(resp); resp = NULL;

    /* ===== Phase 3F: indexed find with projection ===== */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":\"35\"}],"
                   "\"fields\":\"name,city\"}", &resp);
    ASSERT_CONTAINS(resp, "carol", "indexed find with proj returns carol");
    ASSERT_CONTAINS(resp, "NYC",   "indexed find with proj includes city");
    free(resp); resp = NULL;

    /* ===== Phase 3G: v2 → v2 join. Create a second v2 object and join. ===== */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"qry\",\"object\":\"orders\","
        "\"splits\":8,\"max_key\":40,"
        "\"fields\":[\"user_key:varchar:40\",\"amount:int\"],"
        "\"indexes\":[\"user_key\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"qry\",\"object\":\"orders\","
                   "\"key\":\"o1\",\"value\":{\"user_key\":\"u1\",\"amount\":100}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"qry\",\"object\":\"orders\","
                   "\"key\":\"o2\",\"value\":{\"user_key\":\"u2\",\"amount\":200}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"qry\",\"object\":\"orders\","
                   "\"key\":\"o3\",\"value\":{\"user_key\":\"u3\",\"amount\":300}}", &resp);
    free(resp); resp = NULL;

    /* Inner join orders → users via primary key. Both v2 objects: exercises
       lookup_remote's v2 dispatch (Phase 3G). */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"qry\",\"object\":\"orders\","
                   "\"criteria\":[],"
                   "\"join\":[{\"object\":\"users\",\"local\":\"user_key\","
                   "\"remote\":\"key\",\"as\":\"u\",\"fields\":[\"name\",\"city\"]}]}",
                   &resp);
    ASSERT_NOT_NULL(resp, "v2-to-v2 join response");
    ASSERT_CONTAINS(resp, "alice", "v2 join: orders.o1 → users.u1 (alice) appears");
    ASSERT_CONTAINS(resp, "bob",   "v2 join: orders.o2 → users.u2 (bob) appears");
    ASSERT_CONTAINS(resp, "carol", "v2 join: orders.o3 → users.u3 (carol) appears");
    ASSERT_CONTAINS(resp, "u.name",  "v2 join: column header includes u.name");
    ASSERT_CONTAINS(resp, "u.city",  "v2 join: column header includes u.city");
    free(resp); resp = NULL;

    /* Indexed v2-to-v2 join: orders → users via city index. Verifies
       lookup_remote's indexed (non-primary-key) path on v2. */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"qry\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"NYC\"}],"
                   "\"order_by\":\"age\","
                   "\"join\":[{\"object\":\"orders\",\"local\":\"key\","
                   "\"remote\":\"user_key\",\"as\":\"o\",\"type\":\"left\","
                   "\"fields\":[\"amount\"]}]}",
                   &resp);
    ASSERT_NOT_NULL(resp, "v2 indexed-join response");
    ASSERT_CONTAINS(resp, "alice", "indexed join driver: alice (NYC)");
    ASSERT_CONTAINS(resp, "carol", "indexed join driver: carol (NYC)");
    ASSERT_CONTAINS(resp, "frank", "indexed join driver: frank (NYC)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-v2-query", test_slotcask_v2_query_run)
