/* src/test/cases/test_null_bugs.c
 *
 * Base-branch regression proof for the null-bitmap plan
 * (docs/plans/2026-10-10-null-bitmap.md, Task 1).
 *
 * B1: explicit JSON null on a varchar stores the literal 4-char string
 *     "null" (typed_encode_defaults treats the unquoted span as a value;
 *     json_unescape_string copies it verbatim).
 * B2: an omitted enum field reads back as the FIRST declared enum value
 *     (zeroed slot = byte index 0, which the decoder treats as legit).
 *
 * Both assertions FAIL on the base branch — this file is the revert-proof
 * vehicle; the fixes land in Task 3 (encode) / Task 4 (decode).
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

static int test_null_bugs_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"nb\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"nb\",\"object\":\"rec\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"label:varchar:32\",\"status:enum(new,used)\",\"n:long\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create rec");
    free(resp); resp = NULL;

    /* === B1: explicit JSON null on varchar must not store "null" === */

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"nb\",\"object\":\"rec\",\"key\":\"b1\","
        "\"value\":{\"label\":null,\"n\":5}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert b1 (label null)");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"nb\",\"object\":\"rec\",\"key\":\"b1\"}",
        &resp);
    /* Base branch: response contains "label":"null" (the corruption).
       Post-Task-4: label renders as JSON null or is treated as NULL. */
    ASSERT_TRUE(strstr(resp, "\"label\":\"null\"") == NULL,
                "B1: explicit null must not store string \"null\"");
    free(resp); resp = NULL;

    /* === B2: omitted enum must not read back as the first value === */

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"nb\",\"object\":\"rec\",\"key\":\"b2\","
        "\"value\":{\"n\":6}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert b2 (status omitted)");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"nb\",\"object\":\"rec\",\"key\":\"b2\"}",
        &resp);
    /* Base branch: zeroed enum slot renders as the first declared value
       ("new"). Post-Task-3/4: omitted-without-default is NULL. */
    ASSERT_TRUE(strstr(resp, "\"status\":\"new\"") == NULL,
                "B2: omitted enum must not read back as first value");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-null-bugs", test_null_bugs_run)
