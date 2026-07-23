/* src/test/cases/test_joins.c
 * Port of tests/test-joins.sh — inner/left/indexed/multi joins, error paths,
 * format:csv on joins.
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

static int substr_count(const char *hay, const char *needle) {
    if (!hay || !needle || !*needle) return 0;
    int n = 0;
    size_t nl = strlen(needle);
    for (const char *p = hay; (p = strstr(p, needle)); p += nl) n++;
    return n;
}

static int test_joins_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"j_cust\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\",\"city:varchar:32\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"j_orders\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"amount:numeric:10,2\",\"status:varchar:20\","
                    "\"cust_id:varchar:16\",\"ref_code:varchar:32\"],"
        "\"indexes\":[\"status\",\"cust_id\",\"ref_code\"]}", &resp); free(resp); resp = NULL;

    /* Customers */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"j_cust\","
                   "\"key\":\"c1\",\"value\":{\"name\":\"Alice\",\"city\":\"NYC\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"j_cust\","
                   "\"key\":\"c2\",\"value\":{\"name\":\"Bob\",\"city\":\"LA\"}}", &resp); free(resp); resp = NULL;

    /* Orders */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"j_orders\",\"key\":\"o1\","
                   "\"value\":{\"amount\":100,\"status\":\"paid\",\"cust_id\":\"c1\",\"ref_code\":\"ABC\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"j_orders\",\"key\":\"o2\","
                   "\"value\":{\"amount\":250,\"status\":\"paid\",\"cust_id\":\"c1\",\"ref_code\":\"DEF\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"j_orders\",\"key\":\"o3\","
                   "\"value\":{\"amount\":75,\"status\":\"paid\",\"cust_id\":\"c2\",\"ref_code\":\"ABC\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"j_orders\",\"key\":\"o4\","
                   "\"value\":{\"amount\":40,\"status\":\"pending\",\"cust_id\":\"MISSING\",\"ref_code\":\"XYZ\"}}", &resp); free(resp); resp = NULL;

    /* INNER JOIN on primary key */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"j_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"join\":[{\"object\":\"j_cust\",\"local\":\"cust_id\",\"remote\":\"key\","
                   "\"as\":\"cust\",\"fields\":[\"name\",\"city\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "\"j_orders.key\"", "inner: columns include j_orders.key");
    ASSERT_CONTAINS(resp, "\"cust.name\"", "inner: columns include cust.name");
    ASSERT_CONTAINS(resp, "\"o1\"", "inner: o1 present");
    ASSERT_CONTAINS(resp, "\"Alice\"", "inner: Alice joined");
    ASSERT_CONTAINS(resp, "\"o3\"", "inner: o3 present");
    ASSERT_CONTAINS(resp, "\"Bob\"", "inner: Bob joined");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"o4\"") == NULL, "inner: o4 not in output (pending filtered)");
    free(resp); resp = NULL;

    /* LEFT JOIN */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"j_orders\","
        "\"join\":[{\"object\":\"j_cust\",\"local\":\"cust_id\",\"remote\":\"key\","
                   "\"as\":\"cust\",\"type\":\"left\",\"fields\":[\"name\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "\"o4\"", "left: o4 still appears");
    ASSERT_CONTAINS(resp, "null", "left: null for MISSING match");
    ASSERT_CONTAINS(resp, "\"Alice\"", "left: Alice still joined");
    free(resp); resp = NULL;

    /* JOIN on indexed field */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"j_orders\","
        "\"criteria\":[{\"field\":\"ref_code\",\"op\":\"eq\",\"value\":\"ABC\"}],"
        "\"join\":[{\"object\":\"j_orders\",\"local\":\"cust_id\",\"remote\":\"cust_id\","
                   "\"as\":\"sibling\",\"fields\":[\"ref_code\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "\"sibling.ref_code\"", "indexed self-join executes");
    free(resp); resp = NULL;

    /* Errors */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"j_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"join\":[{\"object\":\"j_cust\",\"local\":\"cust_id\",\"remote\":\"name\","
                   "\"as\":\"c\",\"fields\":[\"city\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "must be 'key' or indexed", "remote field not indexed error");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"j_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"join\":[{\"object\":\"j_cust\",\"local\":\"cust_id\",\"remote\":\"key\","
                   "\"as\":\"j_orders\",\"fields\":[\"name\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "collides with driver", "as collision error");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"j_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"join\":[{\"object\":\"j_cust\",\"local\":\"cust_id\",\"remote\":\"key\","
                   "\"as\":\"c\",\"fields\":[\"name\"]},"
                  "{\"object\":\"j_cust\",\"local\":\"cust_id\",\"remote\":\"key\","
                   "\"as\":\"c\",\"fields\":[\"city\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "duplicate join", "duplicate as error");
    free(resp); resp = NULL;

    /* Multi-join */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"j_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"join\":[{\"object\":\"j_cust\",\"local\":\"cust_id\",\"remote\":\"key\","
                   "\"as\":\"cust\",\"fields\":[\"name\"]},"
                  "{\"object\":\"j_orders\",\"local\":\"cust_id\",\"remote\":\"cust_id\","
                   "\"as\":\"related\",\"type\":\"left\",\"fields\":[\"ref_code\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "\"cust.name\"", "multi-join: cust.name column");
    ASSERT_CONTAINS(resp, "\"related.ref_code\"", "multi-join: related.ref_code column");
    free(resp); resp = NULL;

    /* CSV format */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"j_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"join\":[{\"object\":\"j_cust\",\"local\":\"cust_id\",\"remote\":\"key\","
                   "\"as\":\"cust\",\"fields\":[\"name\",\"city\"]}],\"format\":\"csv\"}", &resp);
    ASSERT_CONTAINS(resp, "j_orders.key", "csv header has driver key column");
    ASSERT_CONTAINS(resp, "cust.name", "csv header has cust.name column");
    ASSERT_CONTAINS(resp, "cust.city", "csv header has cust.city column");
    ASSERT_CONTAINS(resp, "Alice", "csv: Alice cell present");
    ASSERT_CONTAINS(resp, "Bob", "csv: Bob cell present");
    free(resp); resp = NULL;

    /* Custom delimiter */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"j_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"join\":[{\"object\":\"j_cust\",\"local\":\"cust_id\",\"remote\":\"key\","
                   "\"as\":\"cust\",\"fields\":[\"name\"]}],"
        "\"format\":\"csv\",\"delimiter\":\"|\"}", &resp);
    ASSERT_CONTAINS(resp, "j_orders.key|", "pipe delimiter respected");
    free(resp); resp = NULL;

    /* Limit with inner join: at most `limit` 'paid' rows in result */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"j_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"join\":[{\"object\":\"j_cust\",\"local\":\"cust_id\",\"remote\":\"key\","
                   "\"as\":\"cust\",\"fields\":[\"name\"]}],\"limit\":2}", &resp);
    ASSERT_TRUE(substr_count(resp, "paid") <= 2, "limit=2 respected (≤2 'paid' rows)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-joins", test_joins_run)
