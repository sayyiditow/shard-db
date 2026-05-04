/* src/test/cases/test_describe.c
 * Port of tests/test-describe.sh — list-objects + describe-object.
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

static int test_describe_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"dsc_users\","
        "\"splits\":16,\"max_key\":40,"
        "\"fields\":[\"name:varchar:64\",\"age:int\",\"email:varchar:80\",\"active:bool\"],"
        "\"indexes\":[\"age\",\"email\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"dsc_orders\","
        "\"splits\":16,\"max_key\":32,"
        "\"fields\":[\"amount:numeric:10,2\",\"total:long\",\"placed:datetime\",\"sku:varchar:24\"],"
        "\"indexes\":[\"sku\",\"amount\",\"sku+placed\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"dsc_empty\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"k:varchar:8\"]}", &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dsc_users\","
                   "\"key\":\"k1\",\"value\":{\"name\":\"alice\",\"age\":30,\"email\":\"a@x\",\"active\":true}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dsc_users\","
                   "\"key\":\"k2\",\"value\":{\"name\":\"bob\",\"age\":40,\"email\":\"b@x\",\"active\":false}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dsc_users\","
                   "\"key\":\"k3\",\"value\":{\"name\":\"carol\",\"age\":50,\"email\":\"c@x\",\"active\":true}}", &resp); free(resp); resp = NULL;

    /* list-objects */
    tc_request(tc, "{\"mode\":\"list-objects\",\"dir\":\"default\"}", &resp);
    ASSERT_CONTAINS(resp, "\"dsc_users\"", "list includes dsc_users");
    ASSERT_CONTAINS(resp, "\"dsc_orders\"", "list includes dsc_orders");
    ASSERT_CONTAINS(resp, "\"dsc_empty\"", "list includes dsc_empty");
    ASSERT_CONTAINS(resp, "\"dir\":\"default\"", "list has dir field");
    ASSERT_CONTAINS(resp, "\"objects\":", "list wraps in objects array");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"list-objects\"}", &resp);
    ASSERT_CONTAINS(resp, "dir is required", "list-objects without dir → error");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"list-objects\",\"dir\":\"unknown_tenant\"}", &resp);
    ASSERT_TRUE(resp && (strstr(resp, "Unknown dir") || strstr(resp, "unknown")),
                "list-objects unknown tenant → error");
    free(resp); resp = NULL;

    /* describe-object: basic shape */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"dsc_users\"}", &resp);
    ASSERT_CONTAINS(resp, "\"dir\":\"default\"", "describe has dir");
    ASSERT_CONTAINS(resp, "\"object\":\"dsc_users\"", "describe has object name");
    ASSERT_CONTAINS(resp, "\"splits\":16", "describe has splits=16");
    ASSERT_CONTAINS(resp, "\"max_key\":40", "describe has max_key=40");
    ASSERT_CONTAINS(resp, "\"slot_size\":", "describe has slot_size");
    ASSERT_CONTAINS(resp, "\"record_count\":3", "describe has record_count=3");

    /* fields */
    ASSERT_CONTAINS(resp, "\"name\":\"name\",\"type\":\"varchar\"", "field name (varchar)");
    ASSERT_CONTAINS(resp, "\"name\":\"age\",\"type\":\"int\"", "field age (int)");
    ASSERT_CONTAINS(resp, "\"name\":\"email\",\"type\":\"varchar\"", "field email");
    ASSERT_CONTAINS(resp, "\"name\":\"active\",\"type\":\"bool\"", "field active (bool)");

    /* indexes */
    ASSERT_CONTAINS(resp, "\"age\"", "indexes contain age");
    ASSERT_CONTAINS(resp, "\"email\"", "indexes contain email");
    free(resp); resp = NULL;

    /* numeric scale + composite */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"dsc_orders\"}", &resp);
    ASSERT_CONTAINS(resp, "\"type\":\"numeric\"", "numeric type emitted");
    ASSERT_CONTAINS(resp, "\"scale\":2", "numeric scale emitted");
    ASSERT_CONTAINS(resp, "\"sku+placed\"", "composite index emitted");
    ASSERT_CONTAINS(resp, "\"type\":\"datetime\"", "datetime type emitted");
    free(resp); resp = NULL;

    /* empty object */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"dsc_empty\"}", &resp);
    ASSERT_CONTAINS(resp, "\"record_count\":0", "empty object record_count=0");
    ASSERT_CONTAINS(resp, "\"indexes\":[]", "empty object indexes=[]");
    free(resp); resp = NULL;

    /* not found */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"does_not_exist\"}", &resp);
    ASSERT_CONTAINS(resp, "not found", "describe missing → error");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-describe", test_describe_run)
