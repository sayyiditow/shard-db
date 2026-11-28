/* src/test/cases/test_single_write_unknown_field.c
 * AUD-P3-7: single insert/update reject a member naming no live field
 * (typo or removed field); bulk-insert stays lenient. */
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

static int test_single_write_unknown_field_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"uf\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"req:varchar:16\",\"nn:int\","
        "\"gone:varchar:16\",\"upd:datetimems:auto_update\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"uf\","
                   "\"key\":\"k1\",\"value\":{\"req\":\"r\",\"nn\":1,\"gone\":\"g\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "baseline insert");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"uf\",\"key\":\"k1\"}", &resp);
    char *before = resp; resp = NULL;
    ASSERT_NOT_NULL(before, "baseline get");

    /* Insert with a typo'd member: error, key absent. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"uf\","
                   "\"key\":\"k2\",\"value\":{\"nmae\":\"x\",\"req\":\"r\"}}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "{\"error\":\"unknown field 'nmae'\"}", "insert unknown field rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"uf\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "false", "rejected insert wrote nothing");
    free(resp); resp = NULL;

    /* Update with a typo'd member: error, record (incl. auto_update stamp) unchanged. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"uf\","
                   "\"key\":\"k1\",\"value\":{\"nn\":9,\"nnn\":9}}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "{\"error\":\"unknown field 'nnn'\"}", "update unknown field rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"uf\",\"key\":\"k1\"}", &resp);
    ASSERT_TRUE(resp && before && strcmp(resp, before) == 0, "rejected update left record + auto_update unchanged");
    free(resp); resp = NULL;

    /* Name whose encoded length exceeds 1530 bytes (6*255). */
    {
        size_t n = 1600;
        char *req = malloc(n + 200);
        char *p = req + sprintf(req, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"uf\","
                                      "\"key\":\"k1\",\"value\":{\"");
        memset(p, 'a', n); p += n;
        strcpy(p, "\":1}}");
        tc_request(tc, req, &resp); free(req);
        chomp(resp);
        ASSERT_EQ_STR(resp, "{\"error\":\"unknown field (name too long)\"}", "overlong name");
        free(resp); resp = NULL;
    }

    /* Computed member under an unknown name: unknown-field error, not a computed error. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"uf\","
                   "\"key\":\"k1\",\"value\":{\"zz\":{\"$incr\":1}}}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "{\"error\":\"unknown field 'zz'\"}", "computed under unknown name");
    free(resp); resp = NULL;

    /* Removed-field name is rejected too (insert and update). */
    tc_request(tc, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"uf\","
                   "\"fields\":[\"gone\"]}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""), "remove-field gone");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"uf\","
                   "\"key\":\"k3\",\"value\":{\"req\":\"r\",\"gone\":\"x\"}}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "{\"error\":\"unknown field 'gone'\"}", "insert removed field rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"uf\","
                   "\"key\":\"k1\",\"value\":{\"gone\":\"x\"}}", &resp);
    chomp(resp);
    ASSERT_EQ_STR(resp, "{\"error\":\"unknown field 'gone'\"}", "update removed field rejected");
    free(resp); resp = NULL;

    /* [R] duplicate member: last wins. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"uf\","
                   "\"key\":\"k1\",\"value\":{\"nn\":5,\"nn\":6}}", &resp);
    ASSERT_CONTAINS(resp, "\"updated\"", "duplicate member accepted [R]");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"uf\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"nn\":6", "duplicate member last-wins [R]");
    free(resp); resp = NULL;

    /* [R] bulk-insert with an unknown member still inserts (bulk is lenient). */
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"uf\","
                   "\"records\":[{\"key\":\"b1\",\"value\":{\"req\":\"r\",\"bogus\":1}}]}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "unknown field"), "bulk-insert unknown member not rejected [R]");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"uf\",\"key\":\"b1\"}", &resp);
    ASSERT_CONTAINS(resp, "true", "bulk-insert record exists [R]");
    free(resp); resp = NULL;

    free(before);
    tc_close(tc);
    test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-single-write-unknown-field", test_single_write_unknown_field_run)
