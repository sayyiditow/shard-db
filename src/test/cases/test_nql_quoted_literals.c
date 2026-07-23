/* Regression: quoted NQL literals must survive raw-wire parsing and the
 * CLI's argv -> NQL serialization without widening the match set. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void nql_quote_base_of(const char *db_root, char *out, size_t out_sz) {
    const char *slash = strrchr(db_root, '/');
    if (!slash || slash == db_root) { out[0] = '\0'; return; }
    size_t n = (size_t)(slash - db_root);
    if (n + 1 > out_sz) { out[0] = '\0'; return; }
    memcpy(out, db_root, n);
    out[n] = '\0';
}

static int test_nql_quoted_literals_real_callers(void) {
    char shard_db_abs[PATH_MAX];
    const char *sdb_rel = "./build/bin/shard-db";
    if (access(sdb_rel, X_OK) != 0) sdb_rel = "./shard-db";
    if (!realpath(sdb_rel, shard_db_abs)) {
        ASSERT_TRUE(0, "shard-db binary not found");
        return 1;
    }

    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    char base[256];
    nql_quote_base_of(env.db_root, base, sizeof base);

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "add-dir succeeds");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"nql_quote_t\"," 
        "\"fields\":[\"name:varchar:32\"],\"splits\":8}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create-object succeeds");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"nql_quote_t\",\"records\":{" 
        "\"k1\":{\"name\":\"Alice Smith\"},"
        "\"k2\":{\"name\":\"O'Brien\"},"
        "\"k3\":{\"name\":\"Say \\\"hi\\\"\"}}}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "bulk-insert succeeds");
    free(resp); resp = NULL;

    tc_request(tc, "find default nql_quote_t \"name eq 'O''Brien'\"", &resp);
    ASSERT_CONTAINS(resp, "O'Brien", "raw NQL matches apostrophe record");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "Alice Smith") == NULL,
                "raw apostrophe query excludes Alice Smith");
    free(resp); resp = NULL;

    tc_request(tc, "find default nql_quote_t \"name eq 'Alice Smith'\"", &resp);
    ASSERT_CONTAINS(resp, "Alice Smith", "raw NQL matches space-containing record");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "O'Brien") == NULL,
                "raw space query excludes apostrophe record");
    free(resp); resp = NULL;

    char *out = tu_capture_cmd(
        "cd %s && %s find default nql_quote_t \"name eq 'O''Brien'\" 2>&1",
        base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "O'Brien") != NULL,
                "CLI find matches apostrophe record");
    ASSERT_TRUE(out && strstr(out, "Alice Smith") == NULL,
                "CLI find does not widen to Alice Smith");
    free(out);

    out = tu_capture_cmd(
        "cd %s && %s find default nql_quote_t \"name eq 'Say \\\"hi\\\"'\" 2>&1",
        base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "Say \\\"hi\\\"") != NULL,
                "CLI encoder doubles embedded top-level double quotes");
    ASSERT_TRUE(out && strstr(out, "Alice Smith") == NULL,
                "CLI double-quote query does not widen to Alice Smith");
    free(out);

    out = tu_capture_cmd(
        "cd %s && %s aggregate default nql_quote_t 'count()' "
        "--filter \"name eq 'O''Brien'\" 2>&1",
        base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "\"count\":1") != NULL,
                "CLI aggregate --filter matches exactly one record");
    ASSERT_TRUE(out && strstr(out, "\"count\":2") == NULL,
                "CLI aggregate --filter does not widen the match set");
    free(out);

    out = tu_capture_cmd(
        "cd %s && %s find default nql_quote_t --fields \"\" --limit 1 2>&1",
        base, shard_db_abs);
    ASSERT_TRUE(out && strstr(out, "\"error\"") == NULL,
                "CLI encoder preserves an empty argv element");
    free(out);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-nql-quoted-literals-real-callers",
              test_nql_quoted_literals_real_callers)
