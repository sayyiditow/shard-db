/* src/test/cases/test_rename_field.c
 *
 * Port of tests/test-rename-field.sh — rename-field schema mutation.
 * Verifies index-file rename, fields.conf updates, GET/INSERT/find
 * with new name, composite-index rename, and four error paths.
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


static int test_rename_field_run(void) {
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
        "\"fields\":[\"fullName:varchar:32\",\"email:varchar:40\",\"age:int\"],"
        "\"indexes\":[\"email\",\"age\",\"fullName+age\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"key\":\"k1\",\"value\":{\"fullName\":\"Alice\",\"email\":\"a@x.com\",\"age\":30}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"key\":\"k2\",\"value\":{\"fullName\":\"Bob\",\"email\":\"b@x.com\",\"age\":25}}", &resp);
    free(resp); resp = NULL;

    char obj[300];
    snprintf(obj, sizeof(obj), "%s/default/leads", env.db_root);
    char path[400];

    snprintf(path, sizeof(path), "%s/indexes/email", obj);
    ASSERT_TRUE(tu_file_exists(path), "email index exists");
    snprintf(path, sizeof(path), "%s/indexes/age", obj);
    ASSERT_TRUE(tu_file_exists(path), "age index exists");
    snprintf(path, sizeof(path), "%s/indexes/fullName+age", obj);
    ASSERT_TRUE(tu_file_exists(path), "composite index exists");

    /* Happy path: rename email → contact. */
    tc_request(tc, "{\"mode\":\"rename-field\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"old\":\"email\",\"new\":\"contact\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"renamed\"", "rename-field status"); free(resp); resp = NULL;

    snprintf(path, sizeof(path), "%s/indexes/contact", obj);
    ASSERT_TRUE(tu_file_exists(path), "new index file");
    snprintf(path, sizeof(path), "%s/indexes/email", obj);
    ASSERT_TRUE(!tu_file_exists(path), "old index file gone");

    snprintf(path, sizeof(path), "%s/fields.conf", obj);
    char *fconf = tu_read_file(path);
    ASSERT_TRUE(fconf && strstr(fconf, "contact:varchar:40") != NULL, "fields.conf has new name");
    ASSERT_TRUE(fconf && strstr(fconf, "\nemail:") == NULL && strncmp(fconf, "email:", 6) != 0,
                "fields.conf old name gone");
    free(fconf);

    snprintf(path, sizeof(path), "%s/indexes/index.conf", obj);
    char *idxconf = tu_read_file(path);
    ASSERT_TRUE(idxconf && strstr(idxconf, "contact") != NULL, "index.conf has new name");
    free(idxconf);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"leads\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"contact\":\"a@x.com\"", "GET k1 has contact");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"email\"") == NULL, "GET k1 no email"); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"key\":\"k3\",\"value\":{\"fullName\":\"Carol\",\"contact\":\"c@x.com\",\"age\":40}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"leads\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"contact\":\"c@x.com\"", "insert+get with new name"); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"criteria\":[{\"field\":\"contact\",\"op\":\"eq\",\"value\":\"a@x.com\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k1\"", "search by renamed field"); free(resp); resp = NULL;

    /* Composite rename: fullName → fn. */
    tc_request(tc, "{\"mode\":\"rename-field\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"old\":\"fullName\",\"new\":\"fn\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"renamed\"", "composite rename status"); free(resp); resp = NULL;

    snprintf(path, sizeof(path), "%s/indexes/fn+age", obj);
    ASSERT_TRUE(tu_file_exists(path), "composite index renamed");
    snprintf(path, sizeof(path), "%s/indexes/fullName+age", obj);
    ASSERT_TRUE(!tu_file_exists(path), "old composite index gone");

    snprintf(path, sizeof(path), "%s/fields.conf", obj);
    fconf = tu_read_file(path);
    ASSERT_TRUE(fconf && strstr(fconf, "fn:varchar:32") != NULL, "fields.conf has fn");
    free(fconf);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"leads\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"fn\":\"Alice\"", "GET k1 has fn"); free(resp); resp = NULL;

    /* Errors. */
    tc_request(tc, "{\"mode\":\"rename-field\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"old\":\"nope\",\"new\":\"x\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "missing field error");
    ASSERT_CONTAINS(resp, "not found", "missing field msg"); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"rename-field\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"old\":\"age\",\"new\":\"contact\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "conflict error");
    ASSERT_CONTAINS(resp, "already exists", "conflict msg"); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"rename-field\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"old\":\"age\",\"new\":\"bad+name\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "invalid name error"); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"rename-field\",\"dir\":\"default\",\"object\":\"leads\","
                   "\"old\":\"age\",\"new\":\"age\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "identical name error"); free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-rename-field", test_rename_field_run)
