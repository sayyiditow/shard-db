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

    /* Large-varchar truncation regression (2026.05.6 hotfix).
       typed_decode's outer record-JSON buffer used to be sized as a
       flat nfields*300 heuristic, which silently truncated mid-value
       on records whose varchar content approached its declared
       capacity. HN comment text (varchar:8192 in practice) hit this
       immediately. Build a record with a >2KB text field and verify
       the round-trip is intact and the find-dict response stays
       parseable across multiple records of that shape. */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"big\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"big\",\"object\":\"posts\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"title:varchar:64\",\"body:varchar:8192\"]}", &resp);
    free(resp); resp = NULL;

    /* Build a 4KB body containing a quote, a backslash, and a newline
       to exercise all three escape paths in the same large value. */
    char big[5000];
    size_t bp = 0;
    for (int rep = 0; rep < 80 && bp + 60 < sizeof(big); rep++) {
        bp += (size_t)snprintf(big + bp, sizeof(big) - bp,
            "line %d with a \\\"quote\\\" and a \\\\ slash; tabs:\\there.\\n",
            rep);
    }
    big[bp] = '\0';

    char ins[6000];
    snprintf(ins, sizeof(ins),
        "{\"mode\":\"insert\",\"dir\":\"big\",\"object\":\"posts\","
        "\"key\":\"p1\",\"value\":{\"title\":\"big\",\"body\":\"%s\"}}",
        big);
    tc_request(tc, ins, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "large-varchar insert succeeded");
    free(resp); resp = NULL;

    /* get → body field must end with the same closing token we wrote
       and the response must close with `}`. Pre-fix this truncated
       mid-body and the outer record's closing brace was lost. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"big\",\"object\":\"posts\",\"key\":\"p1\"}", &resp);
    {
        size_t rl = strlen(resp);
        while (rl > 0 && (resp[rl - 1] == '\n' || resp[rl - 1] == ' ')) rl--;
        ASSERT_TRUE(rl > 0 && resp[rl - 1] == '}', "large body: response closes with }");
    }
    ASSERT_CONTAINS(resp, "line 79", "large body: last line round-tripped");
    free(resp); resp = NULL;

    /* Insert a second record so find returns >1 row, and verify the
       multi-row dict format closes cleanly (the find/dict path is
       where the showcase /search route hit the bug). */
    char ins2[6000];
    snprintf(ins2, sizeof(ins2),
        "{\"mode\":\"insert\",\"dir\":\"big\",\"object\":\"posts\","
        "\"key\":\"p2\",\"value\":{\"title\":\"big2\",\"body\":\"%s\"}}",
        big);
    tc_request(tc, ins2, &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"big\",\"object\":\"posts\","
        "\"criteria\":[],\"limit\":10,\"format\":\"dict\"}", &resp);
    {
        size_t rl = strlen(resp);
        while (rl > 0 && (resp[rl - 1] == '\n' || resp[rl - 1] == ' ')) rl--;
        ASSERT_TRUE(rl > 0 && resp[rl - 1] == '}', "find dict: response closes with }");
    }
    ASSERT_CONTAINS(resp, "\"p1\":", "find dict: first record present");
    ASSERT_CONTAINS(resp, "\"p2\":", "find dict: second record present");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_json_escape_agg_file_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* ── Create object for aggregate tests ── */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"agg\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"agg\",\"object\":\"t\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;

    /* Two records where category contains a JSON-breaking quote. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"agg\",\"object\":\"t\","
        "\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"agg\",\"object\":\"t\","
        "\"key\":\"k2\",\"value\":{\"category\":\"Plain\"}}",
        &resp); free(resp); resp = NULL;

    /* ── Aggregate group_by (standard bucket path) ── */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"agg\",\"object\":\"t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"category\"]}", &resp);
    ASSERT_CONTAINS(resp, "\\\"hi\\\"", "agg group_by: quote escaped");
    ASSERT_CONTAINS(resp, "\"Plain\"", "agg group_by: plain value present");
    free(resp); resp = NULL;

    /* ── Aggregate top-N heap path (order_by + limit forces top-N) ── */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"agg\",\"object\":\"t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"category\"],\"order_by\":\"n\",\"limit\":10}", &resp);
    ASSERT_CONTAINS(resp, "\\\"hi\\\"", "agg top-N: quote escaped");
    free(resp); resp = NULL;

    /* Finding 10 regression: an indexed group_by + order_by + limit CSV
       request must bypass the JSON-only streaming executor. Use a comma
       rather than a quote in the fixture value so this test stays scoped to
       CSV routing, independent of indexed-varchar JSON unescaping. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"agg\",\"object\":\"tcsv\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\"],"
        "\"indexes\":[\"category\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"agg\",\"object\":\"tcsv\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Widgets, Inc.\"}}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"agg\",\"object\":\"tcsv\","
        "\"key\":\"k2\",\"value\":{\"category\":\"Plain\"}}",
        &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"agg\",\"object\":\"tcsv\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"category\"],\"order_by\":\"n\",\"limit\":10,"
        "\"format\":\"csv\"}", &resp);
    ASSERT_TRUE(resp[0] != '[' && resp[0] != '{',
                "agg top-N + format=csv: response is not JSON");
    ASSERT_CONTAINS(resp, "category,n", "agg top-N + format=csv: CSV header row present");
    ASSERT_CONTAINS(resp, "\"Widgets, Inc.\",",
                    "agg top-N + format=csv: comma value quoted per RFC4180");
    ASSERT_CONTAINS(resp, "Plain,", "agg top-N + format=csv: plain value present");
    free(resp); resp = NULL;

    /* ── valid_filename rejects embedded quotes ── */
    tc_request(tc,
        "{\"mode\":\"put-file\",\"dir\":\"agg\",\"object\":\"t\","
        "\"filename\":\"bad\\\"name.txt\",\"data\":\"aGVsbG8=\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "put-file rejects filename with quote");
    ASSERT_CONTAINS(resp, "invalid filename", "put-file: error message");
    free(resp); resp = NULL;

    /* ── list-files escapes pre-existing bad filenames ── */
    /* Create a file with a quote in the name directly on disk. */
    {
        char fpath[1024];
        snprintf(fpath, sizeof(fpath), "%s/agg/t/files", env.db_root);
        mkdirp(fpath);
        size_t flen = strlen(fpath);
        snprintf(fpath + flen, sizeof(fpath) - flen, "/bad\"name.txt");
        int fd = open(fpath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) {
            write(fd, "hello", 5);
            close(fd);
        }
    }
    tc_request(tc,
        "{\"mode\":\"list-files\",\"dir\":\"agg\",\"object\":\"t\"}", &resp);
    ASSERT_CONTAINS(resp, "bad\\\"name.txt", "list-files: quote escaped in filename");
    ASSERT_CONTAINS(resp, "\"files\":[", "list-files: response has files array");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-json-escape", test_json_escape_run)
TEST_REGISTER("test-json-escape-agg-file", test_json_escape_agg_file_run)
