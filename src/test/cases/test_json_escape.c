/* test-json-escape — guards that varchar field content with JSON
 * metacharacters (", \, control chars) round-trips as valid JSON.
 *
 * Regression for the bug surfaced by the HN-explorer showcase:
 * shard-db's decode_field_to_buf used to write varchar bytes into
 * JSON output without escaping, so any HN comment containing a
 * raw newline or quote broke the response stream at parse time.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int test_json_escape_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"esc\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"esc\",\"object\":\"msg\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"text:varchar:512\"]}", &resp);
    free(resp); resp = NULL;

    /* Each insert pushes a value with JSON-meaningful bytes. Bun-side
       JSON.stringify already escapes for the request; we want to make
       sure shard-db re-escapes the bytes correctly on response. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"esc\",\"object\":\"msg\","
        "\"key\":\"k1\",\"value\":{\"text\":\"has a \\\"quote\\\" inside\"}}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"esc\",\"object\":\"msg\","
        "\"key\":\"k2\",\"value\":{\"text\":\"has a back\\\\slash\"}}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"esc\",\"object\":\"msg\","
        "\"key\":\"k3\",\"value\":{\"text\":\"has a\\nnewline\"}}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"esc\",\"object\":\"msg\","
        "\"key\":\"k4\",\"value\":{\"text\":\"tab\\there\"}}",
        &resp); free(resp); resp = NULL;

    /* Read each back via get and verify the response shows the
       proper JSON escape, not the raw byte. Bare-value get returns
       the field dict directly: {"text":"..."} . */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"esc\",\"object\":\"msg\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\\\"quote\\\"", "k1: quote was escaped on read");
    ASSERT_TRUE(resp[0] == '{', "k1: response starts with brace, not raw text");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"esc\",\"object\":\"msg\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "back\\\\slash", "k2: backslash was escaped");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"esc\",\"object\":\"msg\",\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "a\\nnewline", "k3: newline → \\n");
    /* No literal newline byte inside the response body */
    {
        int has_literal_lf = 0;
        for (const char *p = resp; *p; p++) {
            if (*p == '\n') {
                /* Allow at most one trailing newline (response terminator). */
                if (*(p+1)) { has_literal_lf = 1; break; }
            }
        }
        ASSERT_TRUE(has_literal_lf == 0, "k3: no raw \\n bytes mid-response");
    }
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"esc\",\"object\":\"msg\",\"key\":\"k4\"}", &resp);
    ASSERT_CONTAINS(resp, "tab\\there", "k4: tab → \\t");
    free(resp); resp = NULL;

    /* Multi-record bulk get → dict form; same escape rules must hold. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"esc\",\"object\":\"msg\","
        "\"keys\":[\"k1\",\"k3\"]}", &resp);
    ASSERT_CONTAINS(resp, "\\\"quote\\\"", "multi-get: k1 quote escaped");
    ASSERT_CONTAINS(resp, "a\\nnewline", "multi-get: k3 newline escaped");
    free(resp); resp = NULL;

    /* find — full row stream goes through buf_field_value, the
       second varchar-emit site we patched. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"esc\",\"object\":\"msg\","
        "\"criteria\":[],\"limit\":10}", &resp);
    ASSERT_CONTAINS(resp, "\\\"quote\\\"", "find: k1 quote escaped");
    ASSERT_CONTAINS(resp, "a\\nnewline", "find: k3 newline escaped");
    ASSERT_CONTAINS(resp, "back\\\\slash", "find: k2 backslash escaped");
    ASSERT_CONTAINS(resp, "tab\\there", "find: k4 tab escaped");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-json-escape", test_json_escape_run)
