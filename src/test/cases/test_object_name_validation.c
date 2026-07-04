/* src/test/cases/test_object_name_validation.c
 * Object names must never contain path-traversal characters. A tenant must
 * not be able to reach a sibling tenant's object by passing "../other/obj".
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

static int test_object_name_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* Set up two tenants; victim tenant B holds a real object with a secret. */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"tenant_a\"}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"tenant_b\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"tenant_b\",\"object\":\"secrets\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"v:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create victim object"); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"tenant_b\",\"object\":\"secrets\","
        "\"key\":\"k1\",\"value\":{\"v\":\"topsecret\"}}", &resp); free(resp); resp = NULL;

    /* Attack 1: create-object with a traversal object name must be rejected. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"tenant_a\",\"object\":\"../tenant_b/evil\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"v:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "invalid object name", "create-object rejects traversal"); free(resp); resp = NULL;

    /* Attack 2: read tenant_b's secret from a tenant_a request via traversal. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"tenant_a\",\"object\":\"../tenant_b/secrets\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "invalid object name", "get rejects traversal object");
    ASSERT_TRUE(strstr(resp, "topsecret") == NULL, "secret must not leak"); free(resp); resp = NULL;

    /* Sanity: a normal object name still works. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"tenant_a\",\"object\":\"orders\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"v:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "normal object name still accepted"); free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-object-name-validation", test_object_name_validation_run)
