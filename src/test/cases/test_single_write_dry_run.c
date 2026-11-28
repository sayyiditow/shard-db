/* src/test/cases/test_single_write_dry_run.c
 * AUD-P3-9: single insert/update/delete honour dry_run. The dry run builds
 * NEW with the same constructor as the real write, so it answers would_*
 * exactly when the real write would succeed (modulo index-apply I/O), and
 * returns the real write's error text otherwise. An insert dry run never
 * advances a sequence. */
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

static void chomp(char *s){if(s){size_t n=strlen(s);while(n&&(s[n-1]=='\n'||s[n-1]=='\r'))s[--n]=0;}}

/* Send `req`; return malloc'd, chomped reply (never NULL). */
static char *rq(TestClient *tc, const char *req) {
    char *resp = NULL;
    tc_request(tc, req, &resp);
    if (!resp) resp = strdup("");
    chomp(resp);
    return resp;
}

/* Request with and without "dry_run":true spliced in before the final '}'. */
static char *rq_dry(TestClient *tc, const char *req, int dry) {
    if (!dry) return rq(tc, req);
    size_t n = strlen(req);
    char *r = malloc(n + 32);
    memcpy(r, req, n - 1);
    strcpy(r + n - 1, ",\"dry_run\":true}");
    char *out = rq(tc, r);
    free(r);
    return out;
}

static long long seq_value(TestClient *tc, const char *name) {
    char req[200];
    snprintf(req, sizeof(req),
        "{\"mode\":\"sequence\",\"dir\":\"default\",\"object\":\"dseq\",\"name\":\"%s\",\"action\":\"current\"}", name);
    char *r = rq(tc, req);
    char *p = strstr(r, "\"value\":");
    long long v = p ? atoll(p + 8) : -1;
    free(r);
    return v;
}

/* For a failing request, the dry run reply must equal the real reply. */
static void same_error(TestClient *tc, const char *req, const char *desc) {
    char *d = rq_dry(tc, req, 1);
    char *r = rq_dry(tc, req, 0);
    ASSERT_TRUE(strstr(r, "\"error\"") != NULL, desc);
    ASSERT_EQ_STR(d, r, desc);
    free(d); free(r);
}

static int test_single_write_dry_run_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *r;

    free(rq(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}"));
    free(rq(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"dr\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"n:int\",\"nn:int:not_null\","
        "\"s:varchar:16\",\"big:varchar:700\",\"upd:datetimems:auto_update\"],"
        "\"indexes\":[\"big\"]}"));
    free(rq(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dr\","
                "\"key\":\"k1\",\"value\":{\"n\":1,\"nn\":1,\"s\":\"a\"}}"));
    char *rec_before = rq(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\"}");

    /* ---- update dry run ---- */
    r = rq(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\","
               "\"value\":{\"n\":2},\"dry_run\":true}");
    ASSERT_EQ_STR(r, "{\"status\":\"would_update\",\"key\":\"k1\"}", "update dry run: would_update");
    free(r);
    r = rq(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\"}");
    ASSERT_EQ_STR(r, rec_before, "update dry run: record and auto_update stamp unchanged");
    free(r);
    /* Update dry run now validates the patch exactly like the real update. */
    same_error(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\",\"value\":{\"n\":\"abc\"}}",
               "update dry run: bad int same as real");
    same_error(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\",\"value\":{\"nn\":null}}",
               "update dry run: NULL on not-null same as real");
    same_error(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\",\"value\":[1]}",
               "update dry run: non-object JSON same as real");
    same_error(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\",\"value\":{\"n\":{\"$incr\":\"x\"}}}",
               "update dry run: bad computed same as real");
    same_error(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\",\"value\":{\"zz\":1}}",
               "update dry run: unknown field same as real");
    {
        char big[1000]; memset(big, 'b', 650); big[650] = 0;
        char req[1400];
        snprintf(req, sizeof(req), "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\",\"value\":{\"big\":\"%s\"}}", big);
        same_error(tc, req, "update dry run: bad index key same as real");
    }
    r = rq(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"nope\",\"value\":{\"n\":2},\"dry_run\":true}");
    ASSERT_CONTAINS(r, "Not found", "update dry run: missing key");
    free(r);
    r = rq(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\",\"value\":{\"n\":2},"
               "\"if\":[{\"field\":\"n\",\"op\":\"eq\",\"value\":\"99\"}],\"dry_run\":true}");
    ASSERT_CONTAINS(r, "condition_not_met", "update dry run: condition_not_met");
    free(r);

    /* ---- insert dry run ---- */
    r = rq(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k2\","
               "\"value\":{\"n\":1,\"nn\":1},\"dry_run\":true}");
    ASSERT_EQ_STR(r, "{\"status\":\"would_insert\",\"key\":\"k2\"}", "insert dry run: would_insert");
    free(r);
    r = rq(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k2\"}");
    ASSERT_CONTAINS(r, "false", "insert dry run wrote nothing");
    free(r);
    r = rq(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\","
               "\"value\":{\"n\":5,\"nn\":1},\"dry_run\":true}");
    ASSERT_EQ_STR(r, "{\"status\":\"would_update\",\"key\":\"k1\"}", "insert dry run: existing key is would_update");
    free(r);
    r = rq(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\"}");
    ASSERT_EQ_STR(r, rec_before, "insert dry run on existing key left it unchanged");
    free(r);
    {
        const char *req = "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\","
                          "\"value\":{\"n\":5,\"nn\":1},\"if_not_exists\":true}";
        char *d = rq_dry(tc, req, 1), *re = rq_dry(tc, req, 0);
        ASSERT_CONTAINS(re, "condition_not_met", "insert if_not_exists on existing: real condition_not_met");
        ASSERT_EQ_STR(d, re, "insert dry run: if_not_exists reply equals real");
        free(d); free(re);
        req = "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\","
              "\"value\":{\"n\":5,\"nn\":1},\"if\":[{\"field\":\"n\",\"op\":\"eq\",\"value\":\"99\"}]}";
        d = rq_dry(tc, req, 1); re = rq_dry(tc, req, 0);
        ASSERT_CONTAINS(re, "condition_not_met", "insert if mismatch: real condition_not_met");
        ASSERT_EQ_STR(d, re, "insert dry run: if reply equals real");
        free(d); free(re);
    }
    same_error(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k9\",\"value\":{\"n\":\"abc\"}}",
               "insert dry run: bad int same as real");
    same_error(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k9\",\"value\":{\"zz\":1}}",
               "insert dry run: unknown field same as real");
    {
        char big[1000]; memset(big, 'b', 650); big[650] = 0;
        char req[1400];
        snprintf(req, sizeof(req), "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k9\",\"value\":{\"big\":\"%s\"}}", big);
        same_error(tc, req, "insert dry run: bad index key same as real");
    }
    r = rq(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"\",\"value\":{\"n\":1},\"dry_run\":true}");
    ASSERT_CONTAINS(r, "key must not be empty", "insert dry run: empty key rejected");
    free(r);
    r = rq(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"123456789012345678\","
               "\"value\":{\"n\":1},\"dry_run\":true}");
    ASSERT_CONTAINS(r, "exceeds max_key", "insert dry run: oversize key rejected");
    free(r);

    /* ---- delete dry run [R] ---- */
    r = rq(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\",\"dry_run\":true}");
    ASSERT_EQ_STR(r, "{\"status\":\"would_delete\",\"key\":\"k1\"}", "delete dry run: would_delete [R]");
    free(r);
    r = rq(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"dr\",\"key\":\"k1\"}");
    ASSERT_EQ_STR(r, rec_before, "delete dry run left the record [R]");
    free(r);
    free(rec_before);

    /* ---- sequences: default=seq and auto_key=seq never advance on a dry run ---- */
    free(rq(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"dseq\","
        "\"splits\":8,\"max_key\":8,\"fields\":[\"v:int\",\"sid:long:default=seq(rowid)\"],"
        "\"auto_key\":\"seq(dkeys)\"}"));
    long long rowid0 = seq_value(tc, "rowid"), dkeys0 = seq_value(tc, "dkeys");
    r = rq(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dseq\",\"value\":{\"v\":1},\"dry_run\":true}");
    ASSERT_EQ_STR(r, "{\"status\":\"would_insert\",\"key\":null}", "auto-key omitted dry run: would_insert, key null");
    free(r);
    ASSERT_EQ_INT(seq_value(tc, "rowid"), rowid0, "default=seq not advanced by dry run");
    ASSERT_EQ_INT(seq_value(tc, "dkeys"), dkeys0, "auto_key=seq not advanced by dry run");
    r = rq(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dseq\",\"value\":{\"v\":1}}");
    ASSERT_CONTAINS(r, "\"inserted\"", "real auto-key insert after dry runs");
    free(r);
    ASSERT_EQ_INT(seq_value(tc, "rowid"), rowid0 + 1, "real insert advances default=seq by one");
    ASSERT_EQ_INT(seq_value(tc, "dkeys"), dkeys0 + 1, "real insert advances auto_key=seq by one");
    r = rq(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"dseq\",\"value\":{\"v\":1},"
               "\"if\":[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"1\"}],\"dry_run\":true}");
    ASSERT_CONTAINS(r, "if predicate not compatible", "auto-key omitted + if keeps its error");
    free(r);

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}
TEST_REGISTER("test-single-write-dry-run", test_single_write_dry_run_run)
