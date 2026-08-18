#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>

static int test_bool_literal_update_roundtrip_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        TAP_DIAG("# test-bool-literal-update-roundtrip: daemon spawn failed\n");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        ASSERT_NOT_NULL(tc, "connect");
        test_env_stop(&env);
        return 1;
    }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d1\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d1\",\"object\":\"o\","
        "\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"a:varchar:32\",\"b:varchar:32\",\"flag:bool\"],"
        "\"indexes\":[\"flag\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create obj");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\","
        "\"value\":{\"a\":\"x\",\"b\":\"y\",\"flag\":true}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed insert");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"flag\":true", "insert: flag true");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\","
        "\"value\":{\"flag\":false}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "update to false: status");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"flag\":false", "update to false: get reflects false");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\","
        "\"value\":{\"flag\":true}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "update to true: status");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"flag\":true", "update to true: get reflects true (regression)");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"d1\",\"object\":\"o\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]}", &resp);
    ASSERT_EQ_STR(resp, "1\n", "count(flag=true) == 1 after final update");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d1\",\"object\":\"o\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "find(flag=true) returns k1");
    free(resp);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bool-literal-update-roundtrip", test_bool_literal_update_roundtrip_run)
