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
#include <pthread.h>


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

typedef struct {
    int port;
    const char *status;
    char *response;
} BulkCasWriter;

static void *bulk_cas_writer_run(void *arg) {
    BulkCasWriter *writer = (BulkCasWriter *)arg;
    TestClientCfg cfg = { .port = writer->port, .io_timeout_ms = 30000 };
    TestClient *client = tc_connect(&cfg);
    if (!client) return NULL;
    char request[512];
    snprintf(request, sizeof(request),
        "{\"mode\":\"bulk-update\",\"dir\":\"default\","
        "\"object\":\"budj_t\",\"records\":[{\"key\":\"k4\","
        "\"value\":{\"status\":\"%s\",\"amount\":8},"
        "\"if\":[{\"field\":\"amount\",\"op\":\"eq\","
        "\"value\":\"7\"}]}]}", writer->status);
    tc_request(client, request, &writer->response);
    tc_close(client);
    return NULL;
}

static int test_bulk_update_json_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
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

    /* matching per-record CAS commits and preserves an unmodified field */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"approved\"},"
        "\"if\":[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"100\"}]}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"updated\":1", "matching per-key CAS updates");
    free(resp); resp = NULL;
    ASSERT_EQ_INT(do_count(tc,
                           "[{\"field\":\"status\",\"op\":\"eq\","
                           "\"value\":\"approved\"}]"),
                  1, "matching CAS updates the index");

    /* non-matching CAS skips and leaves the record unchanged */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"stale\"},"
        "\"if\":[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"999\"}]}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"updated\":0", "non-matching CAS does not update");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "non-matching CAS skips");
    free(resp); resp = NULL;

    /* malformed per-record if rejects only that record and reports its key */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"budj_t\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"status\":\"bad\"},"
        "\"if\":[{\"field\":\"amount\",\"op\":\"not-an-operator\",\"value\":\"100\"}]},"
        "{\"key\":\"k3\",\"value\":{\"note\":\"peer\"}}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"matched\":2", "invalid if is matched");
    ASSERT_CONTAINS(resp, "\"updated\":1", "valid peer still updates");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "invalid if skips");
    ASSERT_CONTAINS(resp, "\"key\":\"k1\"", "invalid if reports its key");
    ASSERT_CONTAINS(resp, "invalid if condition", "invalid if is reported");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"budj_t\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"approved\"", "invalid if leaves record unchanged");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"budj_t\",\"records\":[{\"key\":\"k4\","
        "\"value\":{\"status\":\"pending\",\"amount\":7,"
        "\"note\":\"cas\"}}]}", &resp);
    free(resp); resp = NULL;

    BulkCasWriter writer_a = { env.port, "writer-a", NULL };
    BulkCasWriter writer_b = { env.port, "writer-b", NULL };
    pthread_t thread_a, thread_b;
    int create_a = pthread_create(&thread_a, NULL, bulk_cas_writer_run, &writer_a);
    int create_b = pthread_create(&thread_b, NULL, bulk_cas_writer_run, &writer_b);
    ASSERT_EQ_INT(create_a, 0, "start CAS writer A");
    ASSERT_EQ_INT(create_b, 0, "start CAS writer B");
    if (create_a == 0) ASSERT_EQ_INT(pthread_join(thread_a, NULL), 0,
                                      "join CAS writer A");
    if (create_b == 0) ASSERT_EQ_INT(pthread_join(thread_b, NULL), 0,
                                      "join CAS writer B");
    if (create_a == 0 && create_b == 0) {
        ASSERT_TRUE((SAFE_STRSTR(writer_a.response, "\"updated\":1") != NULL) !=
                    (SAFE_STRSTR(writer_b.response, "\"updated\":1") != NULL),
                    "exactly one CAS writer updates");
        ASSERT_TRUE((SAFE_STRSTR(writer_a.response, "\"skipped\":1") != NULL) !=
                    (SAFE_STRSTR(writer_b.response, "\"skipped\":1") != NULL),
                    "exactly one CAS writer skips");
    }
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\","
                    "\"object\":\"budj_t\",\"key\":\"k4\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":8", "one CAS writer changed revision");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"status\":\"writer-a\"") != NULL ||
                SAFE_STRSTR(resp, "\"status\":\"writer-b\"") != NULL,
                "one CAS writer's patch is visible");
    free(resp); resp = NULL;
    free(writer_a.response);
    free(writer_b.response);

    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"default\","
        "\"object\":\"budj_t\",\"records\":["
        "{\"key\":\"k4\",\"value\":{\"note\":\"first\"}},"
        "{\"key\":\"k4\",\"value\":{\"note\":\"second\"}}]}", &resp);
    ASSERT_CONTAINS(resp, "duplicate key in records", "duplicate keys reject request");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\","
                    "\"object\":\"budj_t\",\"key\":\"k4\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":8", "duplicate request did not alter record");
    ASSERT_CONTAINS(resp, "\"note\":\"cas\"", "duplicate request preserved note");
    free(resp); resp = NULL;

    /* absent fields untouched — note: CAS test above changed k1 status to "approved" */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"budj_t\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"approved\"", "k1 status=approved (CAS set)");
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
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"refunded\"}]"), 1, "count(refunded)=1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"200\"}]"), 0, "count(amount=200)=0");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"201\"}]"), 1, "count(amount=201)=1");

    /* file form */
    char path[256];
    snprintf(path, sizeof(path), "/tmp/budj_%d.json", (int)getpid());
    FILE *f = fopen(path, "w");
    if (f) {
        fprintf(f, "[{\"key\":\"k1\",\"value\":{\"amount\":111},"
                  "\"if\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"approved\"}]},"
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
    ASSERT_CONTAINS(resp, "\"status\":\"approved\"", "k1 status untouched");
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
