/* src/test/cases/test_return_record.c
 *
 * Conditional-writes F4 — opt-in return of the committed/removed record.
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
#include <unistd.h>

static int test_return_record_run(void) {
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
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rv\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:20\",\"qty:int:default=7\","
        "\"tag:varchar:10\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create rv");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ak\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"label:varchar:10\"],"
        "\"auto_key\":\"uuid\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create ak");
    free(resp); resp = NULL;

    /* 1. Insert with return_record — committed record echoes defaults. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r1\",\"value\":{\"name\":\"a\"},\"return_record\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert ok");
    ASSERT_CONTAINS(resp, "\"record\":", "record returned");
    ASSERT_CONTAINS(resp, "\"qty\":7", "default evaluated in record");
    free(resp); resp = NULL;

    /* 2. Default response shape unchanged — no record member. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r2\",\"value\":{\"name\":\"b\",\"qty\":1,\"tag\":\"t\"}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "plain insert ok");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"record\"") == NULL,
                "no record without the flag");
    free(resp); resp = NULL;

    /* 3. Overwrite-insert with the flag returns the NEW record. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r1\",\"value\":{\"name\":\"a2\",\"qty\":2},"
        "\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "overwrite reported");
    ASSERT_CONTAINS(resp, "\"qty\":2", "new value in record");
    free(resp); resp = NULL;

    /* 4. Update with the flag returns the committed record. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r1\",\"value\":{\"tag\":\"z\"},\"return_record\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "update ok");
    ASSERT_CONTAINS(resp, "\"record\":", "update record returned");
    ASSERT_CONTAINS(resp, "\"tag\":\"z\"", "patched field present");
    ASSERT_CONTAINS(resp, "\"qty\":2", "carried field present");
    free(resp); resp = NULL;

    /* 5. condition_not_met shape unchanged (current, no record member). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r1\",\"value\":{\"qty\":3},\"if\":[{\"field\":\"qty\","
        "\"op\":\"eq\",\"value\":999}],\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "CAS miss");
    ASSERT_CONTAINS(resp, "\"current\":", "current returned");
    free(resp); resp = NULL;

    /* 6. Delete with the flag returns the removed record — captured
          explicitly, since result.current_value is only populated on
          CAS failure. */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r2\",\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"deleted\"", "delete ok");
    ASSERT_CONTAINS(resp, "\"record\":", "removed record returned");
    ASSERT_CONTAINS(resp, "\"name\":\"b\"", "removed values present");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rv\","
                   "\"key\":\"r2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "r2 gone");
    free(resp); resp = NULL;

    /* 7. Delete of a NON-indexed object with the flag (skip_old_read was
          historically true here — the capture must still work). */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"ak\","
        "\"key\":\"12345678-1234-4234-8234-123456789abc\",\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"not_found\"", "missing delete sane");
    free(resp); resp = NULL;

    /* 8. Auto-key insert with the flag — the generated record comes
          back. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ak\","
        "\"value\":{\"label\":\"x\"},\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "auto-key insert ok");
    ASSERT_CONTAINS(resp, "\"record\":", "auto-key record returned");
    ASSERT_CONTAINS(resp, "\"label\":\"x\"", "label present");
    free(resp); resp = NULL;

    /* 9. Dry-run update keeps its shape (no record). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r1\",\"value\":{\"qty\":5},\"dry_run\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"would_update\"", "dry run unchanged");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"record\"") == NULL,
                "no record on dry run");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-return-record", test_return_record_run)
