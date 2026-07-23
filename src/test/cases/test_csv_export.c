/* src/test/cases/test_csv_export.c
 * Port of tests/test-csv-export.sh — CSV/delimited output on
 * find/fetch/aggregate/keys/get-multi/exists-multi.
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

static int count_lines_starting_with(const char *s, const char *prefix) {
    if (!s || !prefix) return 0;
    int n = 0;
    size_t plen = strlen(prefix);
    const char *p = s;
    while (*p) {
        const char *line_end = strchr(p, '\n');
        size_t line_len = line_end ? (size_t)(line_end - p) : strlen(p);
        if (line_len >= plen && strncmp(p, prefix, plen) == 0) n++;
        if (!line_end) break;
        p = line_end + 1;
    }
    return n;
}

static int test_csv_export_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"status:varchar:16\",\"amount:int\",\"note:varchar:64\"],"
        "\"indexes\":[\"status\"]}", &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"csv_orders\","
                   "\"key\":\"o1\",\"value\":{\"status\":\"paid\",\"amount\":100,\"note\":\"vip\"}}",
                   &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"csv_orders\","
                   "\"key\":\"o2\",\"value\":{\"status\":\"paid\",\"amount\":50,\"note\":\"a,comma,here\"}}",
                   &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"csv_orders\","
                   "\"key\":\"o3\",\"value\":{\"status\":\"void\",\"amount\":0,\"note\":\"multi line note\"}}",
                   &resp); free(resp); resp = NULL;

    /* find: default comma delimiter. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"criteria\":[],\"format\":\"csv\"}", &resp);
    ASSERT_CONTAINS(resp, "key,status,amount,note", "header present");
    ASSERT_CONTAINS(resp, "o1,paid,100,vip", "o1 plain row");
    ASSERT_CONTAINS(resp, "\"a,comma,here\"", "comma-in-value gets quoted");
    free(resp); resp = NULL;

    /* find: pipe delimiter. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"criteria\":[],\"format\":\"csv\",\"delimiter\":\"|\"}", &resp);
    ASSERT_CONTAINS(resp, "key|status|amount|note", "pipe header");
    ASSERT_CONTAINS(resp, "o2|paid|50|a,comma,here", "comma NOT quoted for pipe");
    free(resp); resp = NULL;

    /* find: tab delimiter (\t literal). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"criteria\":[],\"format\":\"csv\",\"delimiter\":\"\\t\"}", &resp);
    ASSERT_CONTAINS(resp, "key\tstatus\tamount\tnote", "tab header");
    free(resp); resp = NULL;

    /* find: indexed path. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"format\":\"csv\",\"delimiter\":\"|\"}", &resp);
    ASSERT_CONTAINS(resp, "o1|paid|100|vip", "indexed: o1 present");
    ASSERT_CONTAINS(resp, "o2|paid|50", "indexed: o2 present");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "o3|") == NULL, "indexed: o3 absent");
    free(resp); resp = NULL;

    /* find: OR index-union (Shape C). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"criteria\":[{\"or\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"},"
        "{\"field\":\"status\",\"op\":\"eq\",\"value\":\"void\"}]}],"
        "\"format\":\"csv\",\"delimiter\":\"|\"}", &resp);
    ASSERT_CONTAINS(resp, "o1|paid", "Shape C: o1");
    ASSERT_CONTAINS(resp, "o2|paid", "Shape C: o2");
    ASSERT_CONTAINS(resp, "o3|void", "Shape C: o3");
    free(resp); resp = NULL;

    /* find: projection. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"criteria\":[],\"format\":\"csv\",\"fields\":\"status,amount\"}", &resp);
    ASSERT_CONTAINS(resp, "key,status,amount", "proj header");
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, ",note") == NULL, "proj: note column dropped");
    free(resp); resp = NULL;

    /* find: error still JSON even with format=csv. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"nonexistent\","
        "\"criteria\":[],\"format\":\"csv\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "error is JSON");
    free(resp); resp = NULL;

    /* find: csv + join. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"criteria\":[],\"format\":\"csv\","
        "\"join\":[{\"object\":\"csv_orders\",\"local\":\"status\","
                   "\"remote\":\"status\",\"as\":\"x\",\"fields\":[\"amount\"]}]}",
        &resp);
    {
        const char *line_end = resp ? strchr(resp, '\n') : NULL;
        size_t hl = line_end ? (size_t)(line_end - resp) : (resp ? strlen(resp) : 0);
        char header[512]; if (hl + 1 > sizeof(header)) hl = sizeof(header) - 1;
        if (resp) memcpy(header, resp, hl);
        header[hl] = '\0';
        ASSERT_TRUE(strstr(header, "csv_orders.key") != NULL,
                    "csv+join header has driver columns");
        ASSERT_TRUE(strstr(header, "x.amount") != NULL,
                    "csv+join header has joined column");
    }
    free(resp); resp = NULL;

    /* fetch: CSV. */
    tc_request(tc,
        "{\"mode\":\"fetch\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"format\":\"csv\",\"delimiter\":\"|\"}", &resp);
    ASSERT_CONTAINS(resp, "key|status|amount|note", "fetch header");
    ASSERT_CONTAINS(resp, "o1|paid|100|vip", "fetch emits o1");
    free(resp); resp = NULL;

    /* aggregate: no group_by, csv. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"},"
                        "{\"fn\":\"sum\",\"field\":\"amount\",\"alias\":\"total\"}],"
        "\"format\":\"csv\"}", &resp);
    ASSERT_CONTAINS(resp, "n,total", "agg no-group header");
    ASSERT_CONTAINS(resp, "3,150", "agg no-group row");
    free(resp); resp = NULL;

    /* aggregate: group_by, csv, pipe delim. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"group_by\":[\"status\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"format\":\"csv\",\"delimiter\":\"|\"}", &resp);
    ASSERT_CONTAINS(resp, "status|n", "agg group header");
    ASSERT_CONTAINS(resp, "paid|2", "agg paid group");
    ASSERT_CONTAINS(resp, "void|1", "agg void group");
    free(resp); resp = NULL;

    /* newline-in-value collapsed to space. Use bulk-insert-delimited
       with literal \n in payload. */
    char tmp_path[256]; snprintf(tmp_path, sizeof(tmp_path), "/tmp/csv_nl_%d.csv", (int)getpid());
    FILE *tf = fopen(tmp_path, "w");
    if (tf) { fputs("nl1|paid|10|line1\\nline2\n", tf); fclose(tf); }
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"file\":\"%s\",\"delimiter\":\"|\"}", tmp_path);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    unlink(tmp_path);

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"format\":\"csv\",\"delimiter\":\"|\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "nl1|") != NULL, "nl row exists");
    ASSERT_EQ_INT(count_lines_starting_with(resp, "nl1|"), 1,
                  "nl value stays on one physical line");
    free(resp); resp = NULL;

    /* keys: CSV single-column. */
    tc_request(tc,
        "{\"mode\":\"keys\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"format\":\"csv\"}", &resp);
    ASSERT_CONTAINS(resp, "key", "keys csv header");
    /* Row count equals total. Find total via count. */
    int csv_n = 0;
    if (resp) {
        const char *p = resp;
        const char *first_nl = strchr(p, '\n');
        if (first_nl) {
            const char *q = first_nl + 1;
            while (*q) {
                const char *e = strchr(q, '\n');
                size_t llen = e ? (size_t)(e - q) : strlen(q);
                if (llen > 0) csv_n++;
                if (!e) break;
                q = e + 1;
            }
        }
    }
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"csv_orders\"}", &resp);
    int total = resp ? atoi(resp) : -1;
    free(resp); resp = NULL;
    ASSERT_EQ_INT(csv_n, total, "keys csv row count = total");

    /* get-multi: CSV. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"keys\":[\"o1\",\"o2\",\"o3\"],\"format\":\"csv\",\"delimiter\":\"|\"}",
        &resp);
    ASSERT_CONTAINS(resp, "key|status|amount|note", "get-multi header");
    ASSERT_CONTAINS(resp, "o1|paid|100|vip", "get-multi o1 row");
    ASSERT_CONTAINS(resp, "o2|paid|50|a,comma,here", "get-multi o2 row (pipe, comma raw)");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"keys\":[\"o2\"],\"format\":\"csv\"}", &resp);
    ASSERT_CONTAINS(resp, "\"a,comma,here\"", "get-multi comma delim quotes comma value");
    free(resp); resp = NULL;

    /* exists-multi: CSV two-column. */
    tc_request(tc,
        "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"csv_orders\","
        "\"keys\":[\"o1\",\"nope\",\"o3\"],\"format\":\"csv\"}", &resp);
    ASSERT_CONTAINS(resp, "key,exists", "exists-multi header");
    ASSERT_CONTAINS(resp, "o1,true", "exists-multi o1 true");
    ASSERT_CONTAINS(resp, "nope,false", "exists-multi missing false");
    ASSERT_CONTAINS(resp, "o3,true", "exists-multi o3 true");
    free(resp); resp = NULL;

    /* round-trip via bulk-insert-delimited into csv_rt. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"csv_rt\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"status:varchar:16\",\"amount:int\",\"note:varchar:64\"]}",
        &resp); free(resp); resp = NULL;

    char rt_path[256]; snprintf(rt_path, sizeof(rt_path), "/tmp/csv_rt_%d.csv", (int)getpid());
    tf = fopen(rt_path, "w");
    if (tf) { fputs("r1|paid|100|simple\nr2|pending|50|plain\n", tf); fclose(tf); }
    snprintf(req, sizeof(req),
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"csv_rt\","
        "\"file\":\"%s\",\"delimiter\":\"|\"}", rt_path);
    tc_request(tc, req, &resp); free(resp); resp = NULL;
    unlink(rt_path);

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"csv_rt\"}", &resp);
    ASSERT_CONTAINS(resp, "2", "round-trip count 2");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"csv_rt\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}],"
        "\"format\":\"csv\",\"delimiter\":\"|\"}", &resp);
    ASSERT_CONTAINS(resp, "r1|paid|100|simple", "rt: paid row present");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-csv-export", test_csv_export_run)
