/* Regression (round-4 review M1): a composite-exact find where NO
 * covered leaf has its own standalone index must KEEP the exact plan —
 * not drop it to the ranking. The round-2 fix (put a covered
 * standalone-indexed leaf first) skipped the exact plan entirely for
 * this shape: indexes ["a+b","c"] with criteria [a eq, b eq, c gte] has
 * covered leaves {a,b} — neither standalone-indexed — so the skip fell
 * through to the normal ranking, whose only indexed leaf is the broad
 * range on c → FULL_SCAN, while HEAD explains composite_exact.
 *
 * Fix: keep COMPOSITE_EXACT and seed source_leaves[0] with leaves[prim]
 * (the ranking's indexed leaf — here the c range) so the generic leaf
 * dispatch (csv/rows/join) drives on a real index; D1's
 * build_exact_composite_key matches parts by FIELD NAME, so the default
 * format's key rebuild is order-agnostic and tolerates the extra seed.
 *
 * Fixture: cex_plain (splits 8, no indexes — the oracle) and cex_nc
 * (splits 8, indexes exactly ["a+b","c"]), 300 rows a=i%3, b=i%2, c=i.
 * Criteria [a=1, b=0, c>=100]: i ≡ 4 (mod 6) and c >= 100.
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

#define NRECS 300
#define CRIT_ABC \
    "[{\"field\":\"a\",\"op\":\"eq\",\"value\":1}," \
    "{\"field\":\"b\",\"op\":\"eq\",\"value\":0}," \
    "{\"field\":\"c\",\"op\":\"gte\",\"value\":100}]"

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

static int test_composite_exact_nocover_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"cex_plain\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:int\",\"b:int\",\"c:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "oracle created");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"cex_nc\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:int\",\"b:int\",\"c:int\"],"
        "\"indexes\":[\"a+b\",\"c\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "cex_nc created (no standalone a/b)");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"cex_j2\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"tag:varchar:8\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "join twin created");
    free(resp); resp = NULL;

    insert_fixture(tc, "cex_plain");
    insert_fixture(tc, "cex_nc");
    {
        const size_t reclen = 64;
        char *body = malloc((size_t)NRECS * reclen + 16);
        ASSERT_NOT_NULL(body, "alloc j2 body");
        size_t off = 0;
        off += (size_t)snprintf(body + off, 8, "[");
        for (int i = 0; i < NRECS; i++) {
            /* keys mirror cex_nc's c values so the join's local=c ->
               remote=key lookup hits every matched row */
            off += (size_t)snprintf(body + off, reclen,
                "%s{\"key\":\"%d\",\"value\":{\"tag\":\"t%d\"}}",
                i ? "," : "", i, i % 3);
        }
        off += (size_t)snprintf(body + off, 8, "]");
        char *req = malloc(off + 160);
        snprintf(req, off + 160,
                 "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                 "\"object\":\"cex_j2\",\"records\":%s}", body);
        tc_request(tc, req, &resp);
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                    "j2 insert accepted");
        free(resp); free(req); free(body);
    }

    char req[1024];

    /* Oracle count fixes the expected row count. */
    int want;
    snprintf(req, sizeof(req),
             "{\"mode\":\"count\",\"dir\":\"default\","
             "\"object\":\"cex_plain\",\"criteria\":%s}", CRIT_ABC);
    tc_request(tc, req, &resp); want = resp ? atoi(resp) : -1;
    free(resp); resp = NULL;
    ASSERT_TRUE(want > 0, "oracle count positive");
    {
        snprintf(req, sizeof(req),
                 "{\"mode\":\"count\",\"dir\":\"default\","
                 "\"object\":\"cex_nc\",\"criteria\":%s}", CRIT_ABC);
        tc_request(tc, req, &resp);
        ASSERT_EQ_INT(resp ? atoi(resp) : -1, want,
                      "case0: count parity (count keeps the composite "
                      "leaves competing as ordinary leaves)");
    }
    free(resp); resp = NULL;

    /* 1: explain — the exact plan is KEPT for the default format. RED
     * on the round-2 skip: it demoted this shape to "scan". */
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\","
             "\"object\":\"cex_nc\",\"criteria\":%s,\"explain\":true}",
             CRIT_ABC);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "case1: explain");
    TAP_DIAG("# case1 base-state diag: explain %.300s\n", resp ? resp : "");
    ASSERT_CONTAINS(resp, "\"composite_exact\"",
                    "case1: explain keeps composite_exact (order)");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"plan\":\"scan\"") == NULL,
                "case1: explain is not a full scan");
    /* the seed list carries the standalone c leaf first (the generic
     * dispatch's driver) followed by the covered composite parts */
    ASSERT_NOT_NULL(SAFE_STRSTR(resp, "\"source\":[{\"field\":\"c\","),
                    "case1: c range leaf seeds source_leaves[0]");
    free(resp); resp = NULL;

    /* 2..5: every format returns exactly the oracle's rows. */
    static const char *fmts[] = { NULL, "csv", "rows", "dict" };
    for (int f = 0; f < 4; f++) {
        snprintf(req, sizeof(req),
                 "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cex_nc\","
                 "\"criteria\":%s,\"limit\":300%s%s}", CRIT_ABC,
                 fmts[f] ? ",\"format\":\"" : "",
                 fmts[f] ? fmts[f] : "");
        if (fmts[f]) strcat(req + strlen(req) - 1, "\"}");
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "format find");
        ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                    "format find acked");
        TAP_DIAG("# case2 fmt=%s diag: bytes=%zu keys=%d\n",
                 fmts[f] ? fmts[f] : "default", resp ? strlen(resp) : 0,
                 count_keys(resp));
        ASSERT_EQ_INT(count_keys(resp), want, "format row count == oracle");
        free(resp); resp = NULL;
    }

    /* 6: join (inner on c) — the generic dispatch seeds on c and the
     * joined rows equal the matched set. */
    snprintf(req, sizeof(req),
             "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"cex_nc\","
             "\"criteria\":%s,\"limit\":300,"
             "\"join\":[{\"object\":\"cex_j2\",\"local\":\"c\","
             "\"remote\":\"key\",\"as\":\"j2\",\"type\":\"inner\","
             "\"fields\":[\"tag\"]}]}", CRIT_ABC);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "case6: join find");
    TAP_DIAG("# case6 base-state diag: join bytes=%zu keys=%d\n",
             resp ? strlen(resp) : 0, count_keys(resp));
    ASSERT_EQ_INT(count_keys(resp), want,
                  "case6: join returns the matched rows");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-composite-exact-nocover", test_composite_exact_nocover_run)
