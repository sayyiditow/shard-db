/* src/test/cases/test_bulk_update_delimited.c
 * Port of tests/test-bulk-update-delimited.sh — CSV per-key partial update.
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


static int do_count(TestClient *tc, const char *crit) {
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"udtest\","
        "\"criteria\":%s}", crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static int test_bulk_update_delimited_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"udtest\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"name:varchar:40\",\"age:int\",\"status:varchar:12\",\"city:varchar:40\"],"
        "\"indexes\":[\"name\",\"status\",\"city\"]}", &resp);
    free(resp); resp = NULL;

    /* seed via JSON file */
    char seed_path[256];
    snprintf(seed_path, sizeof(seed_path), "/tmp/budd_seed_%d.json", (int)getpid());
    write_file(seed_path,
        "[{\"key\":\"k1\",\"value\":{\"name\":\"alice\",\"age\":\"30\",\"status\":\"active\",\"city\":\"London\"}},"
        "{\"key\":\"k2\",\"value\":{\"name\":\"bob\",\"age\":\"25\",\"status\":\"active\",\"city\":\"Paris\"}},"
        "{\"key\":\"k3\",\"value\":{\"name\":\"carol\",\"age\":\"40\",\"status\":\"inactive\",\"city\":\"Berlin\"}},"
        "{\"key\":\"k4\",\"value\":{\"name\":\"dave\",\"age\":\"35\",\"status\":\"active\",\"city\":\"Tokyo\"}},"
        "{\"key\":\"k5\",\"value\":{\"name\":\"eve\",\"age\":\"28\",\"status\":\"inactive\",\"city\":\"Madrid\"}}]");
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"udtest\",\"file\":\"%s\"}", seed_path);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    unlink(seed_path);

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"udtest\"}", &resp);
    ASSERT_CONTAINS(resp, "5", "seeded 5 records"); free(resp); resp = NULL;

    /* basic CSV update — non-blank cells overwrite, blank cells leave alone */
    char csv_path[256];
    snprintf(csv_path, sizeof(csv_path), "/tmp/budd_upd_%d.csv", (int)getpid());
    write_file(csv_path, "k1,,31,,\nk2,robert,,,Lyon\n");
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"udtest\","
        "\"file\":\"%s\",\"delimiter\":\",\"}", csv_path);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"matched\":2,\"updated\":2,\"skipped\":0", "matched=2 updated=2");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"udtest\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"age\":31", "k1 age=31");
    ASSERT_CONTAINS(resp, "\"name\":\"alice\"", "k1 name unchanged");
    ASSERT_CONTAINS(resp, "\"city\":\"London\"", "k1 city unchanged");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"udtest\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"robert\"", "k2 name=robert");
    ASSERT_CONTAINS(resp, "\"city\":\"Lyon\"", "k2 city=Lyon");
    ASSERT_CONTAINS(resp, "\"age\":25", "k2 age unchanged");
    free(resp); resp = NULL;

    /* missing keys are skipped */
    write_file(csv_path,
        "k3,,,suspended,\n"
        "k_missing_1,zzz,99,foo,bar\n"
        "k_missing_2,yyy,88,baz,qux\n");
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"matched\":3,\"updated\":1,\"skipped\":2", "matched=3 updated=1 skipped=2");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"udtest\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"suspended\"", "k3 status=suspended");
    ASSERT_CONTAINS(resp, "\"age\":40", "k3 age unchanged");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"udtest\",\"key\":\"k_missing_1\"}", &resp);
    ASSERT_TRUE(!resp || strstr(resp, "\"name\":\"zzz\"") == NULL, "k_missing_1 NOT inserted");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"udtest\"}", &resp);
    ASSERT_CONTAINS(resp, "5", "record count still 5");
    free(resp); resp = NULL;

    /* index tracking: status active=3 inactive=1 suspended=1 */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"}]"), 3, "active=3 unchanged");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"inactive\"}]"), 1, "inactive=1 (k3 moved)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"suspended\"}]"), 1, "suspended=1 (k3 arrived)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"Paris\"}]"), 0, "Paris=0 (k2 moved)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"Lyon\"}]"), 1, "Lyon=1 (k2 landed)");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"bob\"}]"), 0, "name bob=0");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"robert\"}]"), 1, "name robert=1");

    /* blank-cell indexes untouched */
    write_file(csv_path, "k4,,36,,\n");
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"matched\":1,\"updated\":1", "k4 age-only matched=1 updated=1");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"udtest\",\"key\":\"k4\"}", &resp);
    ASSERT_CONTAINS(resp, "\"age\":36", "k4 age=36");
    ASSERT_CONTAINS(resp, "\"name\":\"dave\"", "k4 name=dave still");
    free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"dave\"}]"), 1, "name=dave still resolves");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"Tokyo\"}]"), 1, "city=Tokyo still resolves");

    /* Pipe delimiter */
    char tsv_path[256];
    snprintf(tsv_path, sizeof(tsv_path), "/tmp/budd_upd_%d.tsv", (int)getpid());
    write_file(tsv_path, "k5|frank|||\n");
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"udtest\","
        "\"file\":\"%s\",\"delimiter\":\"|\"}", tsv_path);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"updated\":1", "pipe delim updated=1");
    free(resp); resp = NULL;
    unlink(tsv_path);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"udtest\",\"key\":\"k5\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"frank\"", "k5 name=frank");
    ASSERT_CONTAINS(resp, "\"age\":28", "k5 age unchanged");
    free(resp); resp = NULL;

    /* Errors */
    write_file(csv_path, "");
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"udtest\","
        "\"file\":\"%s\",\"delimiter\":\",\"}", csv_path);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "empty file → error");
    free(resp); resp = NULL;
    unlink(csv_path);

    tc_request(tc, "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"udtest\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "missing file arg → error");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\",\"object\":\"udtest\","
        "\"file\":\"/tmp/budd_does_not_exist.csv\",\"delimiter\":\",\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "nonexistent file → error");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bulk-update-delimited", test_bulk_update_delimited_run)
