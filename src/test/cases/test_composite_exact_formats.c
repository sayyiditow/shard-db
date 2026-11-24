/* Regression: a composite-exact find whose composite's FIRST covered
 * part has no standalone index returned zero rows for every format the
 * D1 composite-exact executor does not serve — csv, rows, and joins.
 * The planner's Phase B fills source_leaves in COMPOSITE order, and the
 * generic leaf executor those shapes fall through to drives on
 * source_leaves[0]: with indexes ["b","a+b"] and criteria [a eq, b eq],
 * that is the phantom `a` leaf (no standalone index) — the dispatch
 * collects nothing and the response is a header-only CSV, an empty rows
 * array, an empty join. Default format worked because the D1
 * composite-key executor handles it (and D1's key rebuild matches
 * leaves by FIELD NAME, so reordering source_leaves is safe for it).
 *
 * Fix: Phase B puts a covered leaf that HAS its own standalone index
 * first (the generic dispatch seeds on it; siblings post-filter). If no
 * covered leaf has a standalone index, the exact plan is skipped and
 * the normal ranking runs (it seeds on the standalone leaf that exists
 * — prim >= 0 is required to reach Phase B at all).
 *
 * Fixture: cef_ix splits 8, indexes ["b","a+b","c"] (a deliberately has
 * NO standalone index), 300 rows a=i%3, b=i%2, c=i. Criteria
 * [a eq 1, b eq 0] matches 50 rows. Shapes: default / csv / rows /
 * join — all four must return the same 50.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_client.h"
#include "test_assert.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NRECS 300
#define NMATCH 50
#define CRIT_AB \
    "[{\"field\":\"a\",\"op\":\"eq\",\"value\":1}," \
    "{\"field\":\"b\",\"op\":\"eq\",\"value\":0}]"

static void insert_fixture(TestClient *tc, const char *obj) {
    const size_t reclen = 80;
    char *body = malloc((size_t)NRECS * reclen + 16);
    ASSERT_NOT_NULL(body, "alloc body");
    size_t off = 0;
    off += (size_t)snprintf(body + off, 8, "[");
    for (int i = 0; i < NRECS; i++) {
        off += (size_t)snprintf(body + off, reclen,
            "%s{\"key\":\"k%04d\",\"value\":{\"a\":%d,\"b\":%d,\"c\":%d}}",
            i ? "," : "", i, i % 3, i % 2, i);
    }
    off += (size_t)snprintf(body + off, 8, "]");
    char *req = malloc(off + 160);
    ASSERT_NOT_NULL(req, "alloc request");
    snprintf(req, off + 160,
             "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
             "\"object\":\"%s\",\"records\":%s}", obj, body);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "bulk insert accepted");
    free(resp);
    free(req);
    free(body);
}

/* Count k#### key tokens (JSON-quoted or bare CSV fields). */
static int count_keys(const char *resp) {
    int n = 0;
    const char *p = resp;
    while (p && (p = strchr(p, 'k')) != NULL) {
        if (p[1] >= '0' && p[1] <= '9' && p[2] >= '0' && p[2] <= '9' &&
            p[3] >= '0' && p[3] <= '9' && p[4] >= '0' && p[4] <= '9') {
            n++;
            p += 5;
        } else {
            p++;
        }
    }
    return n;
}

static int test_composite_exact_formats_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Oracle: no indexes. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"cef_plain\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:int\",\"b:int\",\"c:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "oracle created");
    free(resp); resp = NULL;

    /* Composite twin: standalone b and c, composite a+b, NO standalone a
     * (the phantom first part). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"cef_ix\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:int\",\"b:int\",\"c:int\"],"
        "\"indexes\":[\"b\",\"a+b\",\"c\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "cef_ix created");
    free(resp); resp = NULL;

    /* Join twin: same shape; joins drive off cef_j2's primary keys. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"cef_join\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:int\",\"b:int\",\"c:int\"],"
        "\"indexes\":[\"b\",\"a+b\",\"c\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "cef_join created");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"cef_j2\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"tag:varchar:8\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "cef_j2 created");
    free(resp); resp = NULL;

    insert_fixture(tc, "cef_plain");
    insert_fixture(tc, "cef_ix");
    insert_fixture(tc, "cef_join");
    {
        const size_t reclen = 64;
        char *body = malloc((size_t)NRECS * reclen + 16);
        ASSERT_NOT_NULL(body, "alloc j2 body");
        size_t off = 0;
        off += (size_t)snprintf(body + off, 8, "[");
        for (int i = 0; i < NRECS; i++) {
            /* keys mirror cef_join's c values so the join's
               local=c -> remote=key lookup hits every row */
            off += (size_t)snprintf(body + off, reclen,
                "%s{\"key\":\"%d\",\"value\":{\"tag\":\"t%d\"}}",
                i ? "," : "", i, i % 3);
        }
        off += (size_t)snprintf(body + off, 8, "]");
        char *req = malloc(off + 160);
        snprintf(req, off + 160,
                 "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                 "\"object\":\"cef_j2\",\"records\":%s}", body);
        tc_request(tc, req, &resp);
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                    "j2 insert accepted");
        free(resp); free(req); free(body);
    }

    char req[1024];

    /* 0: count + default find stay correct (the D1 path). */
    {
        long a = -1, b = -1;
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"default\","
                 "\"object\":\"cef_plain\",\"criteria\":%s}", CRIT_AB);
        tc_request(tc, req, &resp); a = resp ? atol(resp) : -1;
        free(resp); resp = NULL;
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"default\","
                 "\"object\":\"cef_ix\",\"criteria\":%s}", CRIT_AB);
        tc_request(tc, req, &resp); b = resp ? atol(resp) : -1;
        free(resp); resp = NULL;
        ASSERT_EQ_INT((int)a, NMATCH, "fixture: oracle count = 50");
        ASSERT_EQ_INT((int)a, (int)b, "case0: count composite-exact");

        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\","
                 "\"object\":\"cef_ix\",\"criteria\":%s,\"limit\":300}",
                 CRIT_AB);
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "case0: default find");
        ASSERT_EQ_INT(count_keys(resp), NMATCH,
                      "case0: default find returns all 50 (D1 path)");
        free(resp); resp = NULL;
    }

    /* 1: csv — RED on base: header only (the phantom-leaf dispatch
     * collects nothing). The CSV body carries one k#### per row. */
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cef_ix\","
             "\"criteria\":%s,\"limit\":300,\"format\":\"csv\"}", CRIT_AB);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "case1: csv find");
    TAP_DIAG("# case1 base-state diag: csv bytes=%zu keys=%d\n",
             resp ? strlen(resp) : 0, count_keys(resp));
    ASSERT_EQ_INT(count_keys(resp), NMATCH,
                  "case1: csv find returns all 50 rows");
    free(resp); resp = NULL;

    /* 2: rows — RED on base: empty rows array. */
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cef_ix\","
             "\"criteria\":%s,\"limit\":300,\"format\":\"rows\"}", CRIT_AB);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "case2: rows find");
    ASSERT_EQ_INT(count_keys(resp), NMATCH,
                  "case2: rows find returns all 50 rows");
    free(resp); resp = NULL;

    /* 3: join (inner; the driver rows come from the composite-exact
     * criteria, looked up in cef_j2 by its primary key) — RED on base:
     * empty join. Tabular output: count the driver's k#### keys in the
     * serialized rows. */
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cef_ix\","
             "\"criteria\":%s,\"limit\":300,"
             "\"join\":[{\"object\":\"cef_j2\",\"local\":\"c\","
             "\"remote\":\"key\",\"as\":\"j2\",\"type\":\"inner\","
             "\"fields\":[\"tag\"]}]}", CRIT_AB);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "case3: join find");
    TAP_DIAG("# case3 base-state diag: join bytes=%zu keys=%d\n",
             resp ? strlen(resp) : 0, count_keys(resp));
    ASSERT_EQ_INT(count_keys(resp), NMATCH,
                  "case3: join find returns all 50 joined rows");
    free(resp); resp = NULL;

    /* 4: csv/rows parity with the oracle (same key count). */
    {
        int plain_csv, ix_csv;
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\","
                 "\"object\":\"cef_plain\",\"criteria\":%s,"
                 "\"limit\":300,\"format\":\"csv\"}", CRIT_AB);
        tc_request(tc, req, &resp);
        plain_csv = count_keys(resp);
        free(resp); resp = NULL;
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\","
                 "\"object\":\"cef_ix\",\"criteria\":%s,"
                 "\"limit\":300,\"format\":\"csv\"}", CRIT_AB);
        tc_request(tc, req, &resp);
        ix_csv = count_keys(resp);
        free(resp); resp = NULL;
        ASSERT_EQ_INT(plain_csv, NMATCH, "case4: oracle csv rows");
        ASSERT_EQ_INT(plain_csv, ix_csv, "case4: csv parity with oracle");
    }

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-composite-exact-formats", test_composite_exact_formats_run)
