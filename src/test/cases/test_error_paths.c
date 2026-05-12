#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <string.h>
#include <stdlib.h>

static char *req(TestClient *tc, const char *json) {
    char *resp = NULL;
    if (tc_request(tc, json, &resp) != 0) return strdup("{\"error\":\"no response\"}");
    return resp;
}

static int test_error_paths_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "daemon spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *r = req(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}"); free(r);
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"testobj\",\"splits\":8,\"max_key\":64,\"fields\":[\"name:varchar:50\"],\"indexes\":[]}"); free(r);

    r = req(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"nonexistent\",\"criteria\":[]}");
    ASSERT_CONTAINS(r, "\"error\"", "find nonexistent object");
    free(r);

    r = req(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"testobj\",\"key\":\"nonexistent\"}");
    ASSERT_CONTAINS(r, "Not found", "get missing key returns not_found error");
    free(r);

    r = req(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"testobj\",\"key\":\"nonexistent\"}");
    ASSERT_CONTAINS(r, "not_found", "delete missing key returns not_found");
    free(r);

    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"testobj\",\"key\":\"x\",\"value\":{\"name\":\"hello\"}}");
    ASSERT_TRUE(strstr(r, "\"error\"") == NULL, "insert ok");
    free(r);

    r = req(tc, "invalid json");
    ASSERT_CONTAINS(r, "\"error\"", "invalid JSON");
    free(r);

    r = req(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"testobj\",\"criteria\":[{\"field\":\"nonexistent\",\"op\":\"eq\",\"value\":\"x\"}]}");
    ASSERT_TRUE(strstr(r, "error") != NULL || strstr(r, "[]") != NULL, "nonexistent field (lenient or error)");
    free(r);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-error-paths", test_error_paths_run)
