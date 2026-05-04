/* src/test/cases/test_restore.c
 * Backup → mutate → restore round-trip. Verifies the live data tree
 * is replaced by the backup snapshot, that ucache invalidation lets
 * subsequent reads see the restored state, and that the safety guards
 * (missing backup, non-empty live tree without --force) fire.
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

/* Pull "<key>":"<value>" out of a JSON-ish response. Stops on first '"'. */
static int extract_str(const char *resp, const char *key, char *out, size_t out_sz) {
    if (!resp) return 0;
    char needle[64]; snprintf(needle, sizeof(needle), "\"%s\":\"", key);
    const char *p = strstr(resp, needle);
    if (!p) return 0;
    p += strlen(needle);
    const char *q = strchr(p, '"');
    if (!q) return 0;
    size_t n = (size_t)(q - p);
    if (n + 1 > out_sz) n = out_sz - 1;
    memcpy(out, p, n); out[n] = '\0';
    return 1;
}

/* Trailing path component (basename) of a slash-separated path. */
static const char *path_tail(const char *p) {
    const char *t = strrchr(p, '/');
    return t ? t + 1 : p;
}

static int test_restore_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rest\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\"],\"indexes\":[\"name\"]}",
        &resp); free(resp); resp = NULL;

    /* Seed 5 records — small enough to verify each by GET. */
    for (int i = 1; i <= 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rest\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"n%d\"}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* Backup. Pull the timestamp out of the response path. */
    tc_request(tc, "{\"mode\":\"backup\",\"dir\":\"default\",\"object\":\"rest\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"backed_up\"", "backup succeeded");
    char path[256];
    if (!extract_str(resp, "path", path, sizeof(path))) {
        ASSERT_TRUE(0, "backup path extracted");
        free(resp); tc_close(tc); test_env_stop(&env); return 1;
    }
    free(resp); resp = NULL;
    char ts[64];
    snprintf(ts, sizeof(ts), "%s", path_tail(path));

    /* Refuse: live tree is non-empty. */
    {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"rest\","
            "\"from\":\"%s\"}", ts);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"error\"", "restore refuses non-empty live tree");
        ASSERT_CONTAINS(resp, "force=true", "error mentions force=true workaround");
        free(resp); resp = NULL;
    }

    /* Now mutate live state: delete k3, change k1 → "MUTATED". After
       restore we expect k3 back and k1 == "n1". */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"rest\",\"key\":\"k3\"}",
                   &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"rest\","
        "\"key\":\"k1\",\"value\":{\"name\":\"MUTATED\"}}", &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rest\",\"key\":\"k1\"}",
                   &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"MUTATED\"", "k1 mutated pre-restore");
    free(resp); resp = NULL;

    /* Restore with force=true. */
    {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"rest\","
            "\"from\":\"%s\",\"force\":true}", ts);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"restored\"", "restore status");
        ASSERT_CONTAINS(resp, "\"object\":\"rest\"", "restore echoes object");
        free(resp); resp = NULL;
    }

    /* Verify all 5 records reappear with their original values. */
    for (int i = 1; i <= 5; i++) {
        char req[256], want[64];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rest\",\"key\":\"k%d\"}", i);
        tc_request(tc, req, &resp);
        snprintf(want, sizeof(want), "\"name\":\"n%d\"", i);
        char desc[64]; snprintf(desc, sizeof(desc), "k%d restored to n%d", i, i);
        ASSERT_TRUE(resp && strstr(resp, want) != NULL, desc);
        free(resp); resp = NULL;
    }

    /* Indexed search still works post-restore (btree mappings refreshed). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"rest\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"n3\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k3\"", "indexed search post-restore returns k3");
    free(resp); resp = NULL;

    /* Error: bogus timestamp. */
    tc_request(tc,
        "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"rest\","
        "\"from\":\"19990101_000000\",\"force\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"backup not found", "bogus from rejected");
    free(resp); resp = NULL;

    /* Error: traversal in from. */
    tc_request(tc,
        "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"rest\","
        "\"from\":\"../../../etc\",\"force\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"invalid from", "traversal rejected");
    free(resp); resp = NULL;

    /* Error: missing from. */
    tc_request(tc,
        "{\"mode\":\"restore\",\"dir\":\"default\",\"object\":\"rest\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"from is required\"", "missing from rejected");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-restore", test_restore_run)
