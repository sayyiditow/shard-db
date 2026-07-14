/* src/test/cases/test_remove_field.c
 * Port of tests/test-remove-field.sh — remove-field schema mutation.
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
#include <sys/stat.h>
#include <unistd.h>



static int test_remove_field_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"leads\","
        "\"splits\":16,\"max_key\":32,"
        "\"fields\":[\"fullName:varchar:32\",\"email:varchar:40\",\"age:int\",\"score:int\"],"
        "\"indexes\":[\"email\",\"age\",\"fullName+age\"]}", &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"key\":\"k1\",\"value\":{\"fullName\":\"Alice\",\"email\":\"a@x.com\",\"age\":30,\"score\":100}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"key\":\"k2\",\"value\":{\"fullName\":\"Bob\",\"email\":\"b@x.com\",\"age\":25,\"score\":90}}", &resp); free(resp); resp = NULL;

    char obj[300]; snprintf(obj, sizeof(obj), "%s/default/leads", env.db_root);
    char path[400];
    snprintf(path, sizeof(path), "%s/indexes/email", obj);
    ASSERT_TRUE(tu_file_exists(path), "email/ exists");
    snprintf(path, sizeof(path), "%s/indexes/age", obj);
    ASSERT_TRUE(tu_file_exists(path), "age/ exists");
    snprintf(path, sizeof(path), "%s/indexes/fullName+age", obj);
    ASSERT_TRUE(tu_file_exists(path), "fullName+age/ exists");

    /* Remove email */
    tc_request(tc,
        "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"leads\","
        "\"fields\":[\"email\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"removed\"", "remove status");
    ASSERT_CONTAINS(resp, "\"fields\":1", "remove fields count");
    ASSERT_CONTAINS(resp, "\"indexes_dropped\":1", "remove indexes count");
    free(resp); resp = NULL;

    snprintf(path, sizeof(path), "%s/fields.conf", obj);
    char *fconf = tu_read_file(path);
    ASSERT_TRUE(fconf && strstr(fconf, "email:varchar:40:removed") != NULL,
                "fields.conf has email:removed");
    free(fconf);
    snprintf(path, sizeof(path), "%s/indexes/email", obj);
    ASSERT_TRUE(!tu_file_exists(path), "email/ dropped");
    snprintf(path, sizeof(path), "%s/indexes/age", obj);
    ASSERT_TRUE(tu_file_exists(path), "age/ still exists");
    snprintf(path, sizeof(path), "%s/indexes/fullName+age", obj);
    ASSERT_TRUE(tu_file_exists(path), "fullName+age/ still exists");

    /* Reads exclude tombstoned */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"leads\",\"key\":\"k1\"}", &resp);
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"email\"") == NULL, "GET no email");
    ASSERT_CONTAINS(resp, "\"fullName\":\"Alice\"", "GET still has fullName");
    ASSERT_CONTAINS(resp, "\"age\":30", "GET still has age");
    ASSERT_CONTAINS(resp, "\"score\":100", "GET still has score");
    free(resp); resp = NULL;

    /* Insert ignores tombstoned */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"key\":\"k3\",\"value\":{\"fullName\":\"Carol\",\"email\":\"c@x.com\",\"age\":40,\"score\":80}}",
                   &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"leads\",\"key\":\"k3\"}", &resp);
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"email\"") == NULL, "insert: email ignored");
    ASSERT_CONTAINS(resp, "\"fullName\":\"Carol\"", "insert: fullName saved");
    free(resp); resp = NULL;

    /* Update ignores tombstoned */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"leads\",\"key\":\"k1\","
                   "\"value\":{\"email\":\"ignored@x.com\",\"score\":200}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"leads\",\"key\":\"k1\"}", &resp);
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"email\"") == NULL, "update: email still absent");
    ASSERT_CONTAINS(resp, "\"score\":200", "update: score updated");
    free(resp); resp = NULL;

    /* Search on tombstoned: no results */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"leads\","
        "\"criteria\":[{\"field\":\"email\",\"op\":\"eq\",\"value\":\"a@x.com\"}]}", &resp);
    ASSERT_TRUE(!resp || SAFE_STRSTR(resp, "\"key\":\"k1\"") == NULL, "search tombstoned: no results");
    free(resp); resp = NULL;

    /* Multi-remove */
    tc_request(tc,
        "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"leads\","
        "\"fields\":[\"age\",\"score\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"removed\"", "multi-remove status");
    ASSERT_CONTAINS(resp, "\"fields\":2", "multi-remove count 2");
    free(resp); resp = NULL;
    snprintf(path, sizeof(path), "%s/indexes/age", obj);
    ASSERT_TRUE(!tu_file_exists(path), "age/ dropped");
    snprintf(path, sizeof(path), "%s/indexes/fullName+age", obj);
    ASSERT_TRUE(!tu_file_exists(path), "fullName+age/ dropped");

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"leads\",\"key\":\"k1\"}", &resp);
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"age\"") == NULL, "GET no age");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"score\"") == NULL, "GET no score");
    ASSERT_CONTAINS(resp, "\"fullName\"", "GET still has fullName");
    free(resp); resp = NULL;

    /* Errors */
    tc_request(tc, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"fields\":[\"nope\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "missing field error");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"fields\":[\"age\"]}", &resp);
    ASSERT_CONTAINS(resp, "already removed", "already-removed error");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"fields\":[]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "empty array error");
    free(resp); resp = NULL;

    /* CSV bulk insert with active field count */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"csvtest\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"a:varchar:8\",\"b:varchar:8\",\"c:varchar:8\"],\"indexes\":[]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"csvtest\","
                   "\"fields\":[\"b\"]}", &resp); free(resp); resp = NULL;

    char csv_path[256];
    snprintf(csv_path, sizeof(csv_path), "/tmp/rmf_csv_%d.txt", (int)getpid());
    FILE *f = fopen(csv_path, "w");
    if (f) { fputs("row1|aaa|ccc\nrow2|xxx|zzz\n", f); fclose(f); }
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"csvtest\","
        "\"file\":\"%s\",\"delimiter\":\"|\"}", csv_path);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    unlink(csv_path);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"csvtest\",\"key\":\"row1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"a\":\"aaa\"", "csv: a=aaa");
    ASSERT_CONTAINS(resp, "\"c\":\"ccc\"", "csv: c=ccc");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"b\"") == NULL, "csv: no b");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"csvtest\",\"key\":\"row2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"a\":\"xxx\"", "csv row2: a=xxx");
    ASSERT_CONTAINS(resp, "\"c\":\"zzz\"", "csv row2: c=zzz");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-remove-field", test_remove_field_run)
