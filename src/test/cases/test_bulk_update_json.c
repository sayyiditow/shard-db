/* src/test/cases/test_bulk_update_json.c
 * Port of tests/test-bulk-update-json.sh — JSON per-key partial update
 * form of bulk-update.
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


static int do_count(TestClient *tc, const char *criteria) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"budj_t\","
        "\"criteria\":%s}", criteria);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static int test_bulk_update_json_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"budj_t\","
        "\"fields\":[\"status:varchar:16\",\"amount:int\",\"note:varchar:32\"],"
        "\"indexes\":[\"status\",\"amount\"],\"splits\":16}", &resp);
    free(resp); resp = NULL;

    /* seed */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"budj_t\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"paid\",\"amount\":100,\"note\":\"vip\"}},"
                     "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":200,\"note\":\"vip\"}},"
                     "{\"key\":\"k3\",\"value\":{\"status\":\"pending\",\"amount\":50,\"note\":\"\"}}]}",
        &resp); free(resp); resp = NULL;

    /* inline records */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"refunded\"}},"
                     "{\"key\":\"k2\",\"value\":{\"status\":\"refunded\",\"amount\":201}},"
                     "{\"key\":\"missing\",\"value\":{\"status\":\"x\"}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"matched\":3", "matched=3");
    ASSERT_CONTAINS(resp, "\"updated\":2", "updated=2");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "skipped=1");
    free(resp); resp = NULL;

    /* absent fields untouched */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"budj_t\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"refunded\"", "k1 status=refunded");
    ASSERT_CONTAINS(resp, "\"amount\":100", "k1 amount=100 untouched");
    ASSERT_CONTAINS(resp, "\"note\":\"vip\"", "k1 note=vip untouched");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"budj_t\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"refunded\"", "k2 status=refunded");
    ASSERT_CONTAINS(resp, "\"amount\":201", "k2 amount=201 changed");
    ASSERT_CONTAINS(resp, "\"note\":\"vip\"", "k2 note=vip untouched");
    free(resp); resp = NULL;

    /* indexes track */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]"), 0, "count(paid)=0");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"refunded\"}]"), 2, "count(refunded)=2");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"200\"}]"), 0, "count(amount=200)=0");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"201\"}]"), 1, "count(amount=201)=1");

    /* file form */
    char path[256];
    snprintf(path, sizeof(path), "/tmp/budj_%d.json", (int)getpid());
    FILE *f = fopen(path, "w");
    if (f) {
        fprintf(f, "[{\"key\":\"k1\",\"value\":{\"amount\":111}},"
                  "{\"key\":\"k3\",\"value\":{\"status\":\"paid\"}}]");
        fclose(f);
    }
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\","
        "\"file\":\"%s\"}", path);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"matched\":2", "file form matched=2");
    ASSERT_CONTAINS(resp, "\"updated\":2", "file form updated=2");
    free(resp); resp = NULL;
    unlink(path);

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"budj_t\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":111", "k1 amount=111 (file update)");
    ASSERT_CONTAINS(resp, "\"status\":\"refunded\"", "k1 status untouched");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"budj_t\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"paid\"", "k3 status=paid (file update)");
    free(resp); resp = NULL;

    /* empty records */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\",\"records\":[]}", &resp);
    ASSERT_CONTAINS(resp, "\"matched\":0", "empty → matched=0");
    ASSERT_CONTAINS(resp, "\"updated\":0", "empty → updated=0");
    free(resp); resp = NULL;

    /* missing input */
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\"}", &resp);
    ASSERT_CONTAINS(resp, "requires criteria", "no input → error");
    free(resp); resp = NULL;

    /* dict form */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\","
        "\"records\":{\"k1\":{\"amount\":111},\"k2\":{\"amount\":250}}}", &resp);
    ASSERT_CONTAINS(resp, "\"matched\":2", "dict-form matched=2");
    ASSERT_CONTAINS(resp, "\"updated\":2", "dict-form updated=2");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"budj_t\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":111", "dict-form patched k1");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"budj_t\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":250", "dict-form patched k2");
    free(resp); resp = NULL;

    /* malformed */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\","
        "\"records\":\"not-json-records\"}", &resp);
    ASSERT_CONTAINS(resp, "top-level object or array", "scalar records → error");
    free(resp); resp = NULL;

    /* criteria form regression */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"refunded\"}],"
        "\"value\":{\"note\":\"audited\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"matched\":", "criteria form matched");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"budj_t\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"note\":\"audited\"", "criteria form patched k2 note");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bulk-update-json", test_bulk_update_json_run)
