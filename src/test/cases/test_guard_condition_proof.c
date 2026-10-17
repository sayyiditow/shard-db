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

static int test_guard_condition_proof_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env)) return 1;
    TestClientCfg cfg = {.port = env.port, .io_timeout_ms = 5000};
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL; char req[4096];
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gp\","
        "\"splits\":8,\"max_key\":32,\"fields\":[\"d:double\",\"f:float\",\"n:long\",\"tag:varchar:40\"]}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""), "create proof object");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gp\","
        "\"key\":\"negative\",\"value\":{\"d\":-0.0,\"f\":-0.0,\"n\":1,\"tag\":\"seed\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed signed zero");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gp\","
        "\"key\":\"target\",\"value\":{\"d\":1,\"f\":1,\"n\":2,\"tag\":\"target\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed update target");
    free(resp); resp = NULL;
    const char *fields[] = {"d", "f", "n"};
    const char *literals[] = {"0.0", "-0.0", "\"000000000000000000000000000000000000000000001\""};
    /* Scan and indexed configurations must yield identical guard verdicts. */
    for (int indexed = 0; indexed < 2; indexed++) {
        if (indexed) for (int i = 0; i < 3; i++) {
            snprintf(req, sizeof(req), "{\"mode\":\"add-index\",\"dir\":\"default\","
                "\"object\":\"gp\",\"field\":\"%s\"}", fields[i]);
            tc_request(tc, req, &resp);
            ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""), "add proof index");
            free(resp); resp = NULL;
        }
        for (int field = 0; field < 3; field++) {
            for (int sign = 0; sign < (field == 2 ? 1 : 2); sign++) {
                const char *literal = field == 2 ? literals[2] : literals[sign];
                snprintf(req, sizeof(req), "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gp\","
                    "\"key\":\"denied\",\"value\":{\"tag\":\"denied\"},\"if_no_match\":"
                    "[{\"field\":\"%s\",\"op\":\"eq\",\"value\":%s}]}", fields[field], literal);
                tc_request(tc, req, &resp);
                ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "existing equivalent value rejects absence");
                free(resp); resp = NULL;
                snprintf(req, sizeof(req), "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"gp\","
                    "\"key\":\"target\",\"value\":{\"tag\":\"target\"},\"if_match\":"
                    "[{\"field\":\"%s\",\"op\":\"eq\",\"value\":%s}]}", fields[field], literal);
                tc_request(tc, req, &resp);
                ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "existing equivalent value proves existence");
                free(resp); resp = NULL;
            }
        }
    }
    const char *ops[] = {"regex", "not_regex"};
    for (int i = 0; i < 2; i++) {
        snprintf(req, sizeof(req), "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gp\","
            "\"key\":\"denied\",\"value\":{\"tag\":\"denied\"},\"if_no_match\":"
            "[{\"field\":\"tag\",\"op\":\"%s\",\"value\":\"[\"}]}", ops[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"error\":\"invalid guard criteria\"", "invalid regex guard rejected");
        free(resp); resp = NULL;
        snprintf(req, sizeof(req), "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"gp\","
            "\"key\":\"target\",\"value\":{\"tag\":\"bad\"},\"if\":"
            "[{\"field\":\"tag\",\"op\":\"%s\",\"value\":\"[\"}]}", ops[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"error\":\"invalid if condition\"", "invalid key regex rejected");
        free(resp); resp = NULL;
    }
    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"gp\",\"key\":\"denied\"}", &resp);
    ASSERT_CONTAINS(resp, "false", "no invalid guard wrote"); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"gp\","
        "\"key\":\"target\",\"value\":{\"tag\":\"done\"},\"if_match\":"
        "[{\"field\":\"tag\",\"op\":\"regex\",\"value\":\"^seed$\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "valid regex and writer progress after failures");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gp\",\"key\":\"target\"}", &resp);
    ASSERT_CONTAINS(resp, "\"tag\":\"done\"", "only valid update applied"); free(resp);
    tc_close(tc); test_env_stop(&env); return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-guard-condition-proof", test_guard_condition_proof_run)
