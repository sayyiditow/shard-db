/* src/test/cases/test_vacuum_addfield.c
 * Port of tests/test-vacuum-addfield.sh — vacuum without flags / --compact /
 * --splits, plus add-field append.
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

static int file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static char *read_file(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f); fseek(f, 0, SEEK_SET);
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    fread(buf, 1, (size_t)sz, f); buf[sz] = '\0'; fclose(f);
    return buf;
}

static int test_vacuum_addfield_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;

    /* Plain vacuum: 5 inserts, delete 2, vacuum cleans=2. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"vac1\","
        "\"splits\":16,\"max_key\":32,"
        "\"fields\":[\"name:varchar:32\",\"age:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    for (int i = 1; i <= 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"vac1\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"n%d\",\"age\":%d}}", i, i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"vac1\",\"key\":\"k1\"}",
                   &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"vac1\",\"key\":\"k3\"}",
                   &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"vac1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"vacuumed\"", "plain vacuum status");
    /* On v2 (the default) the snake-game free-slot pool reuses
       tombstones inline, so no-arg vacuum reports cleaned=0. */
    ASSERT_CONTAINS(resp, "\"cleaned\":0", "plain vacuum cleaned 0 on v2");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"vac1\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"n2\"", "vac1: k2 still there");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"vac1\",\"key\":\"k4\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"n4\"", "vac1: k4 still there");
    free(resp); resp = NULL;

    /* vacuum --compact drops tombstoned fields. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"vac2\","
        "\"splits\":16,\"max_key\":32,"
        "\"fields\":[\"name:varchar:32\",\"email:varchar:40\","
                    "\"age:int\",\"score:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    for (int i = 1; i <= 3; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"vac2\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"n%d\",\"email\":\"e%d@x.com\","
            "\"age\":%d,\"score\":%d}}", i, i, i, 20 + i, 100 - i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    tc_request(tc,
        "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"vac2\","
        "\"fields\":[\"email\",\"score\"]}", &resp); free(resp); resp = NULL;

    char fields_path[400];
    snprintf(fields_path, sizeof(fields_path), "%s/default/vac2/fields.conf", env.db_root);
    char *fields = read_file(fields_path);
    ASSERT_TRUE(fields && strstr(fields, "email:varchar:40:removed") != NULL,
                "vac2: email tombstoned");
    ASSERT_TRUE(fields && strstr(fields, "score:int:removed") != NULL,
                "vac2: score tombstoned");
    free(fields);

    tc_request(tc,
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"vac2\",\"compact\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "compact vacuum status");
    ASSERT_CONTAINS(resp, "\"compact\":true", "compact flag in output");
    free(resp); resp = NULL;

    fields = read_file(fields_path);
    ASSERT_TRUE(fields && strstr(fields, "email") == NULL, "fields.conf: no email");
    ASSERT_TRUE(fields && strstr(fields, "score") == NULL, "fields.conf: no score");
    ASSERT_TRUE(fields && strstr(fields, "name:varchar:32") != NULL, "fields.conf: name kept");
    ASSERT_TRUE(fields && strstr(fields, "age:int") != NULL, "fields.conf: age kept");
    free(fields);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"vac2\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"n1\"", "vac2: k1 name");
    ASSERT_CONTAINS(resp, "\"age\":21", "vac2: k1 age");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"vac2\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"age\":23", "vac2: k3 age");
    free(resp); resp = NULL;

    char path[400];
    snprintf(path, sizeof(path), "%s/default/vac2/data.new", env.db_root);
    ASSERT_TRUE(!file_exists(path), "data.new cleaned");
    snprintf(path, sizeof(path), "%s/default/vac2/data.old", env.db_root);
    ASSERT_TRUE(!file_exists(path), "data.old cleaned");

    /* vacuum --splits N reshards. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"vac3\","
        "\"splits\":16,\"max_key\":32,"
        "\"fields\":[\"name:varchar:16\"],\"indexes\":[\"name\"]}",
        &resp); free(resp); resp = NULL;
    for (int i = 1; i <= 50; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"vac3\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"n%d\"}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    tc_request(tc,
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"vac3\",\"splits\":32}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "reshard status");
    ASSERT_CONTAINS(resp, "\"splits\":32", "reshard splits");
    ASSERT_CONTAINS(resp, "\"live\":50", "reshard live=50");
    free(resp); resp = NULL;

    /* All 4 sampled records survive. */
    int found = 0;
    int sample[] = { 1, 10, 25, 50 };
    for (size_t s = 0; s < sizeof(sample)/sizeof(sample[0]); s++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"vac3\",\"key\":\"k%d\"}",
            sample[s]);
        tc_request(tc, req, &resp);
        char want[32]; snprintf(want, sizeof(want), "\"name\":\"n%d\"", sample[s]);
        if (resp && strstr(resp, want)) found++;
        free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(found, 4, "all sampled records survive reshard");

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"vac3\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"n25\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k25\"", "indexed search post-reshard");
    free(resp); resp = NULL;

    /* TASK #6: add-field appends new fields. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"add1\","
        "\"splits\":16,\"max_key\":32,"
        "\"fields\":[\"name:varchar:16\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"add1\","
                   "\"key\":\"k1\",\"value\":{\"name\":\"alice\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"add1\","
                   "\"key\":\"k2\",\"value\":{\"name\":\"bob\"}}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"add1\","
        "\"fields\":[\"age:int\",\"email:varchar:40\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "add-field status");
    free(resp); resp = NULL;

    snprintf(path, sizeof(path), "%s/default/add1/fields.conf", env.db_root);
    fields = read_file(path);
    ASSERT_TRUE(fields && strstr(fields, "age:int") != NULL, "fields.conf has age");
    ASSERT_TRUE(fields && strstr(fields, "email:varchar:40") != NULL, "fields.conf has email");
    ASSERT_TRUE(fields && strstr(fields, "name:varchar:16") != NULL, "fields.conf keeps name");
    free(fields);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"add1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"alice\"", "k1 name preserved");
    ASSERT_CONTAINS(resp, "\"age\":0", "k1 age defaults to 0");
    ASSERT_TRUE(resp && strstr(resp, "\"email\"") == NULL, "k1 no email (empty varchar)");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"add1\","
                   "\"key\":\"k3\",\"value\":{\"name\":\"carol\",\"age\":40,\"email\":\"c@x.com\"}}",
                   &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"add1\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"age\":40", "k3 age 40");
    ASSERT_CONTAINS(resp, "\"email\":\"c@x.com\"", "k3 email");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"add1\","
                   "\"key\":\"k1\",\"value\":{\"age\":30}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"add1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"age\":30", "k1 age set via update");
    free(resp); resp = NULL;

    /* Error cases. */
    tc_request(tc, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"add1\","
                   "\"fields\":[\"name:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "duplicate name rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"add1\","
                   "\"fields\":[\"bad:unknowntype\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "invalid type rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"add1\","
                   "\"fields\":[]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "empty array rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"add1\","
                   "\"fields\":[\"dup:int\",\"dup:varchar:10\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "duplicate-in-request rejected");
    free(resp); resp = NULL;

    /* INTEGRATION: remove-field + vacuum --compact + add-field. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"integ\","
        "\"splits\":16,\"max_key\":32,"
        "\"fields\":[\"a:varchar:16\",\"b:varchar:16\",\"c:int\"],\"indexes\":[]}",
        &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"integ\","
                   "\"key\":\"k1\",\"value\":{\"a\":\"aa\",\"b\":\"bb\",\"c\":42}}",
                   &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"integ\","
                   "\"key\":\"k2\",\"value\":{\"a\":\"aaa\",\"b\":\"bbb\",\"c\":43}}",
                   &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"integ\","
        "\"fields\":[\"b\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"integ\","
        "\"compact\":true}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"integ\","
        "\"fields\":[\"d:varchar:8\"]}", &resp); free(resp); resp = NULL;

    snprintf(path, sizeof(path), "%s/default/integ/fields.conf", env.db_root);
    fields = read_file(path);
    ASSERT_TRUE(fields && strstr(fields, "a:varchar:16") != NULL, "integ: a kept");
    ASSERT_TRUE(fields && strstr(fields, "b:") == NULL, "integ: b gone");
    ASSERT_TRUE(fields && strstr(fields, "c:int") != NULL, "integ: c kept");
    ASSERT_TRUE(fields && strstr(fields, "d:varchar:8") != NULL, "integ: d added");
    free(fields);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"integ\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"a\":\"aa\"", "k1 a preserved");
    ASSERT_TRUE(resp && strstr(resp, "\"b\"") == NULL, "k1 b gone");
    ASSERT_CONTAINS(resp, "\"c\":42", "k1 c preserved");
    ASSERT_TRUE(resp && strstr(resp, "\"d\"") == NULL, "k1 d empty");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"integ\","
                   "\"key\":\"k1\",\"value\":{\"d\":\"new\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"integ\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"d\":\"new\"", "k1 d updated");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-vacuum-addfield", test_vacuum_addfield_run)
