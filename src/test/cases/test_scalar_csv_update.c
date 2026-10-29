/* A delimited file is a byte span: scalar validation must see the whole
   cell, while varchar must retain bytes following an embedded NUL. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *csv_request(TestClient *tc, const char *json) {
    char *resp = NULL;
    if (tc_request(tc, json, &resp) != 0) return NULL;
    return resp;
}

static void csv_count(TestClient *tc, const char *field, const char *value,
                      long expected, const char *description) {
    char query[512];
    snprintf(query, sizeof(query),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"csv_span\","
             "\"criteria\":[{\"field\":\"%s\",\"op\":\"eq\",\"value\":\"%s\"}]}",
             field, value);
    char *resp = csv_request(tc, query);
    ASSERT_NOT_NULL(resp, "indexed count responds");
    if (resp) {
        char *end = NULL;
        long actual = strtol(resp, &end, 10);
        ASSERT_TRUE(end != resp && (*end == '\0' || *end == '\n'),
                    "indexed count is numeric");
        ASSERT_TRUE(actual == expected, description);
    }
    free(resp);
}

static int test_scalar_csv_update_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = {.port = env.port, .io_timeout_ms = 60000};
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = csv_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"csv_span\","
        "\"splits\":8,\"max_key\":32,\"fields\":[\"i:int\",\"v:varchar:16\"],"
        "\"indexes\":[\"i\",\"v\"]}");
    ASSERT_TRUE(resp && strstr(resp, "\"error\"") == NULL, "create indexed object");
    free(resp);
    resp = csv_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"csv_span\","
        "\"records\":{\"bad\":{\"i\":7,\"v\":\"keep\"},"
        "\"good\":{\"i\":8,\"v\":\"old\"},\"text\":{\"i\":10,\"v\":\"textold\"}}}");
    ASSERT_TRUE(resp && strstr(resp, "\"inserted\":3") != NULL, "seed three rows");
    free(resp);

    char path[512];
    snprintf(path, sizeof(path), "%s/update.csv", env.db_root);
    FILE *file = fopen(path, "wb");
    ASSERT_NOT_NULL(file, "create binary delimited file");
    if (!file) { tc_close(tc); test_env_stop(&env); return 1; }
    static const char data[] =
        "bad|12\0junk|changed\n"
        "good|9|new\n"
        "text||a\0b\n";
    size_t written = fwrite(data, 1, sizeof(data) - 1, file);
    ASSERT_TRUE(written == sizeof(data) - 1, "write complete binary file");
    ASSERT_TRUE(fclose(file) == 0, "close binary file");

    char query[1024];
    snprintf(query, sizeof(query),
        "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\","
        "\"object\":\"csv_span\",\"file\":\"%s\",\"delimiter\":\"|\"}", path);
    resp = csv_request(tc, query);
    ASSERT_TRUE(resp && strstr(resp, "\"matched\":3") != NULL, "all rows matched");
    ASSERT_TRUE(resp && strstr(resp, "\"updated\":2") != NULL, "only valid rows updated");
    ASSERT_TRUE(resp && strstr(resp, "\"skipped\":0") != NULL,
                "malformed scalar is an error, not a condition skip");
    ASSERT_TRUE(resp && strstr(resp, "\"errors\":[") != NULL,
                "malformed scalar has an explicit row error");
    ASSERT_TRUE(resp && strstr(resp, "\"key\":\"bad\"") != NULL,
                "row error identifies malformed scalar key");
    free(resp);

    resp = csv_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"csv_span\",\"key\":\"bad\"}");
    ASSERT_TRUE(resp && strstr(resp, "\"i\":7") != NULL, "rejected row keeps old integer");
    ASSERT_TRUE(resp && strstr(resp, "\"v\":\"keep\"") != NULL, "rejected row keeps sibling field");
    free(resp);
    resp = csv_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"csv_span\",\"key\":\"good\"}");
    ASSERT_TRUE(resp && strstr(resp, "\"i\":9") != NULL, "valid scalar update applied");
    ASSERT_TRUE(resp && strstr(resp, "\"v\":\"new\"") != NULL, "valid sibling update applied");
    free(resp);
    resp = csv_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"csv_span\",\"key\":\"text\"}");
    ASSERT_TRUE(resp && strstr(resp, "\"i\":10") != NULL, "empty scalar cell leaves old value");
    ASSERT_TRUE(resp && strstr(resp, "\"v\":\"a\\u0000b\"") != NULL,
                "varchar retains complete cell after embedded NUL");
    free(resp);

    csv_count(tc, "i", "7", 1, "old integer index entry preserved");
    csv_count(tc, "i", "12", 0, "rejected numeric prefix has no index entry");
    csv_count(tc, "v", "keep", 1, "old varchar index entry preserved");
    csv_count(tc, "i", "9", 1, "valid new integer indexed");
    csv_count(tc, "i", "8", 0, "valid old integer index entry removed");

    remove(path);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-scalar-csv-update", test_scalar_csv_update_run)
