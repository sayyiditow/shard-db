/* src/test/cases/test_edit_long_default.c
 *
 * edit-field must never silently drop a legal long :default= literal.
 * The wire accepts :default= literals up to 255 bytes, but the rewrite's
 * modifier-suffix extraction must carry the COMPLETE suffix (default +
 * any trailing tokens) — sized and checked here via a 250-char literal,
 * whose ":default=" suffix alone outgrows a 256-byte buffer. Covers an
 * absent default (edit supplies one) and an existing default (carried
 * through a type resize, then swapped for another long literal).
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

static char g_lit1[251];   /* 250 'a's */
static char g_lit2[251];   /* 250 'b's */

static int test_edit_long_default_run(void) {
    memset(g_lit1, 'a', 250); g_lit1[250] = '\0';
    memset(g_lit2, 'b', 250); g_lit2[250] = '\0';

    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    char req[1024], path[512], want[600];
    char *fc;

    /* 1. absent default: the edit supplies a 250-char literal. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ld1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"big:varchar:300\",\"n:int\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create ld1");
    free(resp); resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ld1\","
        "\"fields\":[\"big:varchar:300:default=%s\"]}", g_lit1);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "long default accepted");
    free(resp); resp = NULL;
    snprintf(path, sizeof(path), "%s/default/ld1/fields.conf", env.db_root);
    fc = tu_read_file(path);
    ASSERT_TRUE(fc && strstr(fc, g_lit1), "long default persisted");
    ASSERT_TRUE(fc && !strstr(fc, "\n\n"), "no mangled line written");
    free(fc);
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\","
                   "\"object\":\"ld1\"}", &resp);
    snprintf(want, sizeof(want), "\"default\":\"%s\"", g_lit1);
    ASSERT_CONTAINS(resp, want, "describe renders the long default");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ld1\","
                   "\"key\":\"k1\",\"value\":{\"n\":1}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "omitting the defaulted field inserts");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"ld1\","
                   "\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, g_lit1, "omission backfilled from the long default");
    free(resp); resp = NULL;

    /* 2. existing long default: a type resize carries it forward. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ld2\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"big:varchar:300:default=%s\",\"n:int\"]}", g_lit1);
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "create ld2 with long default");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"ld2\",\"fields\":[\"big:varchar:400\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"",
                    "resize with existing long default accepted");
    free(resp); resp = NULL;
    snprintf(path, sizeof(path), "%s/default/ld2/fields.conf", env.db_root);
    fc = tu_read_file(path);
    snprintf(want, sizeof(want), "big:varchar:400:default=%s\n", g_lit1);
    ASSERT_TRUE(fc && strstr(fc, want), "long default carried through resize");
    free(fc);
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\","
                   "\"object\":\"ld2\"}", &resp);
    snprintf(want, sizeof(want), "\"default\":\"%s\"", g_lit1);
    ASSERT_CONTAINS(resp, want, "ld2 describe renders the carried default");
    free(resp); resp = NULL;

    /* 3. existing long default: swap it for another long literal. */
    snprintf(req, sizeof(req),
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ld2\","
        "\"fields\":[\"big:varchar:400:default=%s\"]}", g_lit2);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "long default swap accepted");
    free(resp); resp = NULL;
    fc = tu_read_file(path);
    snprintf(want, sizeof(want), "big:varchar:400:default=%s\n", g_lit2);
    ASSERT_TRUE(fc && strstr(fc, want), "new long literal persisted");
    ASSERT_TRUE(fc && !strstr(fc, "aaa"), "old long literal replaced");
    free(fc);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-edit-long-default", test_edit_long_default_run)
