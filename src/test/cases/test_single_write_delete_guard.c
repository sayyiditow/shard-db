/* src/test/cases/test_single_write_delete_guard.c
 * AUD-P3-10 / AUD-P3-1: single delete renders return_record before the
 * commit (a render failure rejects the delete, the record stays), and a
 * guarded write with an invalid auto-key gets exactly one reply line. */
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

static void chomp(char *s){if(s){size_t n=strlen(s);while(n&&(s[n-1]=='\n'||s[n-1]=='\r'))s[--n]=0;}}

static int count_substr(const char *h, const char *n) {
    int c = 0;
    for (const char *p = h; p && (p = strstr(p, n)); p += strlen(n)) c++;
    return c;
}

static int test_single_write_delete_guard_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"dd\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"n:int\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dd\","
                   "\"key\":\"d1\",\"value\":{\"n\":7}}", &resp); free(resp); resp = NULL;

    /* ---- delete return_record render failure keeps the record ---- */
    ASSERT_TRUE(test_env_ctl_set_delete_render_fail(&env, 1) == 0, "arm render failure");
    /* CAS mismatch is reported as condition_not_met, not masked by the render failure. */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"dd\",\"key\":\"d1\","
                   "\"return_record\":true,\"if\":[{\"field\":\"n\",\"op\":\"eq\",\"value\":\"99\"}]}", &resp);
    ASSERT_CONTAINS(resp, "condition_not_met", "CAS mismatch not masked by render failure");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"dd\",\"key\":\"d1\","
                   "\"return_record\":true}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "{\"error\":\"cannot render record\"}", "render failure rejects the delete");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"dd\",\"key\":\"d1\"}", &resp);
    ASSERT_CONTAINS(resp, "true", "record still exists after rejected delete");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"dd\"}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "1", "count unchanged");
    free(resp); resp = NULL;
    /* One-shot: the retry succeeds and returns the record. */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"dd\",\"key\":\"d1\","
                   "\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"deleted\"", "retry deletes");
    ASSERT_CONTAINS(resp, "\"record\":{\"n\":7}", "retry returns the record");
    free(resp); resp = NULL;

    /* ---- guarded write with an invalid auto-key: exactly one reply ---- */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"aku\","
                   "\"splits\":8,\"max_key\":16,\"fields\":[\"domain:varchar:40\"],"
                   "\"auto_key\":\"uuid\"}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"aku\",\"key\":\"not-a-uuid\","
                   "\"if_match\":[{\"field\":\"domain\",\"op\":\"eq\",\"value\":\"x\"}]}", &resp);
    ASSERT_TRUE(resp != NULL, "guarded invalid auto-key replies");
    ASSERT_EQ_INT(count_substr(resp, "\"error\""), 1, "exactly one error in the reply");
    ASSERT_TRUE(SAFE_STRSTR(resp, "invalid guard criteria") == NULL, "no second guard reply");
    free(resp); resp = NULL;
    /* Connection still in sync (no stray second frame): next request gets its own reply. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"aku\"}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "0", "next reply is in sync");
    free(resp); resp = NULL;
    /* [R] invalid guard JSON still yields invalid guard criteria. */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"dd\",\"key\":\"d1\","
                   "\"if_match\":[{\"field\":\"nope\",\"op\":\"zzz\"}]}", &resp);
    ASSERT_CONTAINS(resp, "invalid guard criteria", "invalid guard criteria kept [R]");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-single-write-delete-guard", test_single_write_delete_guard_run)
