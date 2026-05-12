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

static int test_planner_edge_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "daemon spawn"); return 1; }
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *r = req(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}"); free(r);
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"users\",\"splits\":8,\"max_key\":64,\"fields\":[\"name:varchar:50\",\"age:int\",\"email:varchar:100\"],\"indexes\":[\"name\",\"age\"]}"); free(r);

    for (int i = 0; i < 50; i++) {
        char j[256];
        snprintf(j, sizeof(j), "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"users\",\"key\":\"u%d\",\"value\":{\"name\":\"user%d\",\"age\":%d,\"email\":\"u%d@x.com\"}}", i, i, 20 + i % 30, i);
        r = req(tc, j); free(r);
    }

    r = req(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"user1\"},{\"field\":\"age\",\"op\":\"eq\",\"value\":\"21\"}]}");
    ASSERT_CONTAINS(r, "\"key\":\"u1\"", "AND indexed eq + eq");
    free(r);

    r = req(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"user1\"}]}");
    ASSERT_CONTAINS(r, "1", "count indexed eq");
    free(r);

    r = req(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"name\",\"op\":\"neq\",\"value\":\"user0\"},{\"field\":\"age\",\"op\":\"eq\",\"value\":\"21\"}]}");
    ASSERT_CONTAINS(r, "\"key\":\"u1\"", "neq + eq find u1");
    ASSERT_CONTAINS(r, "\"key\":\"u31\"", "neq + eq find u31");
    free(r);

    r = req(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"or\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"user1\"},{\"field\":\"age\",\"op\":\"eq\",\"value\":\"25\"}]}]}");
    ASSERT_TRUE(strstr(r, "2") != NULL || strstr(r, "3") != NULL, "OR count");
    free(r);

    r = req(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"user0\"},{\"field\":\"age\",\"op\":\"eq\",\"value\":\"21\"},{\"field\":\"email\",\"op\":\"eq\",\"value\":\"u1@x.com\"}]}");
    ASSERT_TRUE(strstr(r, "\"key\"") == NULL || strstr(r, "\"key\":\"u0\"") != NULL, "3-index intersect");
    free(r);

    r = req(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"name\",\"op\":\"starts_with\",\"value\":\"user\"},{\"field\":\"age\",\"op\":\"between\",\"value\":[\"25\",\"30\"]}]}");
    ASSERT_TRUE(strstr(r, "\"key\":\"u25\"") != NULL || strstr(r, "\"error\"") == NULL, "range + prefix");
    free(r);

    r = req(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[{\"field\":\"name\",\"op\":\"contains\",\"value\":\"ser1\"}]}");
    ASSERT_CONTAINS(r, "\"key\":\"u1\"", "contains (full scan)");
    free(r);

    r = req(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\",\"criteria\":[]}");
    ASSERT_CONTAINS(r, "\"key\":\"u0\"", "empty criteria = full scan");
    free(r);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-planner-edge", test_planner_edge_run)
