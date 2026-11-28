/* src/test/cases/test_single_write_key_validation.c
 * AUD-P3-2 / AUD-P3-3: single insert/update/delete reject an empty key;
 * single insert reports an oversize key as a JSON error. */
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

static int test_single_write_key_validation_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"kv\","
        "\"splits\":8,\"max_key\":8,\"fields\":[\"a:int\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"kv\","
                   "\"key\":\"k1\",\"value\":{\"a\":1}}", &resp); free(resp); resp = NULL;

    /* Empty key: insert/update/delete all rejected with the same error. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"kv\","
                   "\"key\":\"\",\"value\":{\"a\":2}}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "{\"error\":\"key must not be empty\"}", "insert empty key rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"kv\","
                   "\"key\":\"\",\"value\":{\"a\":2}}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "{\"error\":\"key must not be empty\"}", "update empty key rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"kv\","
                   "\"key\":\"\"}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "{\"error\":\"key must not be empty\"}", "delete empty key rejected");
    free(resp); resp = NULL;

    /* Object still readable: keys / fetch succeed, one record. */
    tc_request(tc, "{\"mode\":\"keys\",\"dir\":\"default\",\"object\":\"kv\"}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "keys works after empty-key attempt");
    ASSERT_CONTAINS(resp, "k1", "keys lists k1");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"fetch\",\"dir\":\"default\",\"object\":\"kv\"}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "fetch works after empty-key attempt");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"kv\"}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "1", "no record created by empty key");
    free(resp); resp = NULL;

    /* Oversize key (max_key 8, key 9 bytes): insert gets a JSON error. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"kv\","
                   "\"key\":\"123456789\",\"value\":{\"a\":2}}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "{\"error\":\"key exceeds max_key (8)\"}", "insert oversize key JSON error");
    free(resp); resp = NULL;

    /* Update / delete of an oversize key keep today's not-found answers [R]. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"kv\","
                   "\"key\":\"123456789\",\"value\":{\"a\":2}}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "must not be empty") && !strstr(resp, "exceeds max_key"),
                "update oversize key: no new error text [R]");
    ASSERT_CONTAINS(resp, "ot found", "update oversize key: not found [R]");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"kv\","
                   "\"key\":\"123456789\"}", &resp);
    ASSERT_CONTAINS(resp, "not_found", "delete oversize key: not_found [R]");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-single-write-key-validation", test_single_write_key_validation_run)
