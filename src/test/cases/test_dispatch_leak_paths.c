/* src/test/cases/test_dispatch_leak_paths.c
 *
 * Regression coverage for dispatch_json_query() early-return paths that
 * bypass the function's shared free(mode)/free(dir)/free(object)
 * epilogue: negative-offset find, and compact mode (previously
 * completely untested). Functional correctness of these paths is not
 * new; what's new is exercising them at all so a local ASan
 * detect_leaks=1 run can prove the leak fix.
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

static int test_dispatch_leak_paths_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"widgets\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:16\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"widgets\","
                   "\"key\":\"w1\",\"value\":{\"name\":\"gear\"}}", &resp);
    free(resp); resp = NULL;

    /* (a) negative offset on find. */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"widgets\","
                   "\"criteria\":[],\"offset\":-1}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "negative offset rejected");
    free(resp); resp = NULL;

    /* (b) compact on a nonexistent object. */
    tc_request(tc, "{\"mode\":\"compact\",\"dir\":\"default\",\"object\":\"nope\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "compact on nonexistent object rejected");
    free(resp); resp = NULL;

    /* NOTE: the compact-success and compact-failed early returns in
       dispatch_json_query are NOT black-box reachable here — create-object
       defaults to SLOTCASK_FORMAT_FIXED, and compact refuses anything that
       isn't SLOTCASK_FORMAT_VARIABLE (which only arises via the offline
       varlen migration). Those two return sites are leak-fixed by the same
       symmetric free(mode)/free(dir)/free(object) insertion as site (b)
       above, which this test does exercise. */

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-dispatch-leak-paths", test_dispatch_leak_paths_run)
