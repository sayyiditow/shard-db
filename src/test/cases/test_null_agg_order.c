/* src/test/cases/test_null_agg_order.c
 *
 * Aggregate + order NULL semantics (null plan Task 8, §2.4.5/§2.5):
 *   - sum/avg/min/max skip NULLs (SQL); avg of all-NULL → null;
 *   - group_by NULL forms its own group rendering bare JSON null;
 *     NULL, "", and the literal "null" are three distinct buckets;
 *   - indexed group-by agrees with the scan path (NULL buckets included);
 *   - ordered find delivers NULL rows at the ASC tail / DESC head;
 *   - cursor pagination crosses the NULL boundary with tier tokens
 *     (resume neither repeats nor skips the NULL row).
 */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static TestClient *na_setup(TestEnv *env) {
    if (test_env_start(env) != 0) return NULL;
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(env); return NULL; }
    char *r = NULL;
    const char *seq[] = {
        "{\"mode\":\"add-dir\",\"dir\":\"na\"}",
        "{\"mode\":\"create-object\",\"dir\":\"na\",\"object\":\"t\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:long\",\"g:varchar:8\"],"
        "\"indexes\":[\"v\",\"g\"]}",
    };
    for (size_t i = 0; i < sizeof(seq) / sizeof(seq[0]); i++) {
        if (tc_request(tc, seq[i], &r) != 0 || strstr(r, "\"error\"")) {
            free(r); tc_close(tc); test_env_stop(env); return NULL;
        }
        free(r); r = NULL;
    }
    /* v: 10(NULL g), NULL(g "x"), 20(g ""), 5(g "null"), NULL(g NULL) */
    const char *rows[5] = {
        "{\"mode\":\"insert\",\"dir\":\"na\",\"object\":\"t\",\"key\":\"r1\","
        "\"value\":{\"v\":\"10\"}}",
        "{\"mode\":\"insert\",\"dir\":\"na\",\"object\":\"t\",\"key\":\"r2\","
        "\"value\":{\"v\":null,\"g\":\"x\"}}",
        "{\"mode\":\"insert\",\"dir\":\"na\",\"object\":\"t\",\"key\":\"r3\","
        "\"value\":{\"v\":\"20\",\"g\":\"\"}}",
        "{\"mode\":\"insert\",\"dir\":\"na\",\"object\":\"t\",\"key\":\"r4\","
        "\"value\":{\"v\":\"5\",\"g\":\"null\"}}",
        "{\"mode\":\"insert\",\"dir\":\"na\",\"object\":\"t\",\"key\":\"r5\","
        "\"value\":{\"g\":null}}",
    };
    for (size_t i = 0; i < 5; i++) {
        if (tc_request(tc, rows[i], &r) != 0 || strstr(r, "\"error\"")) {
            free(r); tc_close(tc); test_env_stop(env); return NULL;
        }
        free(r); r = NULL;
    }
    return tc;
}

static int test_null_agg_sql_semantics(void) {
    TestEnv env;
    TestClient *tc = na_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        /* sum skips NULLs: 10+20+5 = 35 over 3 non-NULL values. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"aggregate\",\"dir\":\"na\",\"object\":\"t\","
            "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"v\",\"alias\":\"s\"},"
            "{\"fn\":\"avg\",\"field\":\"v\",\"alias\":\"a\"},"
            "{\"fn\":\"count\",\"field\":\"v\",\"alias\":\"c\"}]}", &r),
            0, "aggregate");
        ASSERT_NOT_NULL(r, "resp");
        ASSERT_TRUE(strstr(r, "\"s\":35") != NULL, "sum skips NULLs");
        ASSERT_TRUE(strstr(r, "\"c\":3") != NULL,
                    "count(field) counts non-NULL (empty/zero doctrine aside)");
        free(r); r = NULL;
        /* avg of an all-NULL group renders null. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"aggregate\",\"dir\":\"na\",\"object\":\"t\","
            "\"aggregates\":[{\"fn\":\"avg\",\"field\":\"v\",\"alias\":\"a\"}],"
            "\"group_by\":[\"g\"],"
            "\"criteria\":[{\"field\":\"g\",\"op\":\"eq\",\"value\":\"x\"}]}",
            &r), 0, "avg over all-NULL group");
        ASSERT_NOT_NULL(r, "resp");
        ASSERT_TRUE(strstr(r, "\"a\":null") != NULL, "avg all-NULL → null");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-agg-sql-semantics", test_null_agg_sql_semantics)

static int test_null_agg_group_buckets(void) {
    TestEnv env;
    TestClient *tc = na_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        /* Group by g (indexed): NULL, "", "null", "x" are four buckets. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"aggregate\",\"dir\":\"na\",\"object\":\"t\","
            "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}],"
            "\"group_by\":[\"g\"]}", &r), 0, "indexed group-by");
        ASSERT_NOT_NULL(r, "resp");
        int null_bucket = strstr(r, "\"g\":null") != NULL;
        int empty_bucket = strstr(r, "\"g\":\"\"") != NULL;
        int literal_bucket = strstr(r, "\"g\":\"null\"") != NULL;
        int x_bucket = strstr(r, "\"g\":\"x\"") != NULL;
        ASSERT_EQ_INT(null_bucket, 1, "NULL renders its own bucket as null");
        ASSERT_EQ_INT(empty_bucket, 1, "\"\" is a distinct bucket");
        ASSERT_EQ_INT(literal_bucket, 1, "literal \"null\" is a distinct bucket");
        ASSERT_EQ_INT(x_bucket, 1, "\"x\" bucket present");
        free(r); r = NULL;

        /* Unindexed twin: drop the index, same buckets (scan path parity). */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"remove-index\",\"dir\":\"na\",\"object\":\"t\","
            "\"field\":\"g\"}", &r), 0, "remove g index");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"aggregate\",\"dir\":\"na\",\"object\":\"t\","
            "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}],"
            "\"group_by\":[\"g\"]}", &r), 0, "scan group-by");
        ASSERT_NOT_NULL(r, "resp2");
        ASSERT_TRUE(strstr(r, "\"g\":null") != NULL &&
                    strstr(r, "\"g\":\"\"") != NULL &&
                    strstr(r, "\"g\":\"null\"") != NULL,
                    "scan-path buckets identical to indexed");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-agg-group-buckets", test_null_agg_group_buckets)

static int test_null_agg_cursor_boundary(void) {
    TestEnv env;
    TestClient *tc = na_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        /* Page 1 (limit 3 asc by v): values first, NULLs at the tail —
           page 2 crosses the NULL boundary via the tier token. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"na\",\"object\":\"t\","
            "\"criteria\":[],\"order_by\":\"v\",\"order\":\"asc\","
            "\"limit\":3,\"cursor\":null}", &r), 0, "page 1");
        ASSERT_NOT_NULL(r, "p1");
        ASSERT_TRUE(strstr(r, "\"tiers\":[") != NULL,
                    "page 1 emits a tiered token");
        free(r); r = NULL;

        /* Walk pages until null; collect keys — 5 unique rows, no dupes. */
        char seen[5][64];
        int nseen = 0, dup = 0, pages = 0;
        char cursor[512];
        snprintf(cursor, sizeof(cursor), "null");
        for (int pg = 0; pg < 6; pg++) {
            char req[1024];
            snprintf(req, sizeof(req),
                "{\"mode\":\"find\",\"dir\":\"na\",\"object\":\"t\","
                "\"criteria\":[],\"order_by\":\"v\",\"order\":\"asc\","
                "\"limit\":3,\"cursor\":%s}", cursor);
            if (tc_request(tc, req, &r) != 0 || !r) break;
            /* Scan keys only in the rows region — the cursor token also
               ends with a "key" field. */
            const char *rows_end = strstr(r, ",\"cursor\":");
            if (!rows_end) rows_end = r + strlen(r);
            const char *p = r;
            while ((p = strstr(p, "\"key\":\"")) != NULL && p < rows_end) {
                p += 7;
                const char *e = strchr(p, '"');
                if (!e) break;
                size_t len = (size_t)(e - p);
                if (len < sizeof(seen[0])) {
                    int known = 0;
                    for (int i = 0; i < nseen; i++)
                        if (memcmp(seen[i], p, len) == 0 && seen[i][len] == 0)
                            known = 1;
                    if (!known && nseen < 5) {
                        memcpy(seen[nseen], p, len);
                        seen[nseen][len] = '\0';
                        nseen++;
                    } else if (known) dup++;
                }
                p = e;
            }
            const char *c = strstr(r, "\"cursor\":");
            const char *cs = c ? strchr(c, '{') : NULL;
            const char *ce = cs ? strchr(cs, '}') : NULL;
            pages++;
            if (!cs || !ce) break;   /* cursor null → done */
            size_t clen = (size_t)(ce - cs) + 1;
            if (clen >= sizeof(cursor)) break;
            memcpy(cursor, cs, clen); cursor[clen] = '\0';
            free(r); r = NULL;
        }
        free(r); r = NULL;
        ASSERT_EQ_INT(nseen, 5, "all 5 rows across pages (NULL boundary crossed)");
        ASSERT_EQ_INT(dup, 0, "no row repeated across the NULL boundary");
        (void)pages;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-agg-cursor-boundary", test_null_agg_cursor_boundary)

/* Review finding: the varchar-streaming (VS) group-by fast path — which
 * only runs when a finite limit is set — skipped NULL-tier index keys
 * entirely, so the NULL group vanished from limited aggregates (the
 * no-limit IGB/scan paths report it). Its record lookups also fed NULL
 * spec values into the accumulators (a NULL v pulled min down to 0 and
 * counted in avg's denominator). Fixture rows: r1(v=10,g=NULL),
 * r2(v=NULL,g="x"), r3(v=20,g=""), r4(v=5,g="null"), r5(v=NULL,g=NULL). */
static int test_null_agg_vs_limit_null_group(void) {
    TestEnv env;
    TestClient *tc = na_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"aggregate\",\"dir\":\"na\",\"object\":\"t\","
            "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"},"
            "{\"fn\":\"avg\",\"field\":\"v\",\"alias\":\"a\"},"
            "{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"m\"}],"
            "\"group_by\":[\"g\"],\"limit\":10}", &r), 0, "VS aggregate");
        ASSERT_NOT_NULL(r, "resp");
        /* NULL group present WITH a limit; its accums skip r5's NULL v
           (count 2, avg/min from r1's 10 alone). */
        ASSERT_TRUE(strstr(r, "{\"g\":null,\"c\":2,\"a\":10,\"m\":10}") != NULL,
                    "NULL group with limit: NULL v skipped, not zeroed");
        /* All-NULL spec group: avg renders null (denominator excludes
           the NULL), min renders the documented empty-0. */
        ASSERT_TRUE(strstr(r, "{\"g\":\"x\",\"c\":1,\"a\":null,\"m\":0}") != NULL,
                    "all-NULL group: avg null, not 0/1");
        ASSERT_TRUE(strstr(r, "\"g\":\"\"") != NULL &&
                    strstr(r, "\"g\":\"null\"") != NULL,
                    "value buckets intact alongside the NULL group");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-agg-vs-limit-null-group", test_null_agg_vs_limit_null_group)

/* Review finding: a page ENDING on a NULL row dropped the cursor token
 * (the NULL part has no value string, which the old vals_ok gate treated
 * as a missing capture), stranding the NULL-tier rows behind it. limit 2
 * over v = 5,10,20,NULL,NULL makes page 2 end on a NULL row with one
 * more NULL row left, so page 3 must resume past a tier-1 token. */
static int test_null_agg_cursor_null_page(void) {
    TestEnv env;
    TestClient *tc = na_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        char seen[5][64];
        int nseen = 0, dup = 0;
        int saw_null_tier_token = 0;
        char cursor[512];
        snprintf(cursor, sizeof(cursor), "null");
        for (int pg = 0; pg < 6; pg++) {
            char req[1024];
            snprintf(req, sizeof(req),
                "{\"mode\":\"find\",\"dir\":\"na\",\"object\":\"t\","
                "\"criteria\":[],\"order_by\":\"v\",\"order\":\"asc\","
                "\"limit\":2,\"cursor\":%s}", cursor);
            if (tc_request(tc, req, &r) != 0 || !r) break;
            if (strstr(r, "\"tiers\":[1]")) saw_null_tier_token = 1;
            const char *rows_end = strstr(r, ",\"cursor\":");
            if (!rows_end) rows_end = r + strlen(r);
            const char *p = r;
            while ((p = strstr(p, "\"key\":\"")) != NULL && p < rows_end) {
                p += 7;
                const char *e = strchr(p, '"');
                if (!e) break;
                size_t len = (size_t)(e - p);
                if (len < sizeof(seen[0])) {
                    int known = 0;
                    for (int i = 0; i < nseen; i++)
                        if (memcmp(seen[i], p, len) == 0 && seen[i][len] == 0)
                            known = 1;
                    if (!known && nseen < 5) {
                        memcpy(seen[nseen], p, len);
                        seen[nseen][len] = '\0';
                        nseen++;
                    } else if (known) dup++;
                }
                p = e;
            }
            const char *c = strstr(r, "\"cursor\":");
            const char *cs = c ? strchr(c, '{') : NULL;
            const char *ce = cs ? strchr(cs, '}') : NULL;
            if (!cs || !ce) break;   /* cursor null → done */
            size_t clen = (size_t)(ce - cs) + 1;
            if (clen >= sizeof(cursor)) break;
            memcpy(cursor, cs, clen); cursor[clen] = '\0';
            free(r); r = NULL;
        }
        free(r); r = NULL;
        ASSERT_EQ_INT(saw_null_tier_token, 1,
                    "the page ending on a NULL row emits a tier-1 token");
        ASSERT_EQ_INT(nseen, 5, "all 5 rows delivered past the NULL-ending page");
        ASSERT_EQ_INT(dup, 0, "no NULL row repeated across the resume");
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-agg-cursor-null-page", test_null_agg_cursor_null_page)

/* Composite twin: NULL in the FIXED-WIDTH non-final component. The stored
 * key pads a NULL component with width zero bytes ([0x01][00×8] for a
 * long); a resume tuple built without that padding compares BELOW every
 * remaining NULL-tier row (byte 2: padding 0x00 < next tier 0x00/payload)
 * and page 3 skips them. Fixture: a:long + g:varchar:8, composite a+g. */
static TestClient *na_composite_setup(TestEnv *env) {
    if (test_env_start(env) != 0) return NULL;
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(env); return NULL; }
    char *r = NULL;
    const char *seq[] = {
        "{\"mode\":\"add-dir\",\"dir\":\"na\"}",
        "{\"mode\":\"create-object\",\"dir\":\"na\",\"object\":\"tc\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:long\",\"g:varchar:8\"],"
        "\"indexes\":[\"a+g\"]}",
        "{\"mode\":\"insert\",\"dir\":\"na\",\"object\":\"tc\",\"key\":\"c1\","
        "\"value\":{\"a\":\"1\",\"g\":\"a\"}}",
        "{\"mode\":\"insert\",\"dir\":\"na\",\"object\":\"tc\",\"key\":\"c2\","
        "\"value\":{\"a\":\"2\",\"g\":\"a\"}}",
        "{\"mode\":\"insert\",\"dir\":\"na\",\"object\":\"tc\",\"key\":\"c3\","
        "\"value\":{\"a\":null,\"g\":\"a\"}}",
        "{\"mode\":\"insert\",\"dir\":\"na\",\"object\":\"tc\",\"key\":\"c4\","
        "\"value\":{\"a\":null,\"g\":\"b\"}}",
        "{\"mode\":\"insert\",\"dir\":\"na\",\"object\":\"tc\",\"key\":\"c5\","
        "\"value\":{\"a\":null,\"g\":\"c\"}}",
    };
    for (size_t i = 0; i < sizeof(seq) / sizeof(seq[0]); i++) {
        if (tc_request(tc, seq[i], &r) != 0 || strstr(r, "\"error\"")) {
            free(r); tc_close(tc); test_env_stop(env); return NULL;
        }
        free(r); r = NULL;
    }
    return tc;
}

static int test_null_agg_cursor_null_composite(void) {
    TestEnv env;
    TestClient *tc = na_composite_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        char seen[5][64];
        int nseen = 0, dup = 0;
        int saw_null_tier_token = 0;
        char cursor[512];
        snprintf(cursor, sizeof(cursor), "null");
        for (int pg = 0; pg < 6; pg++) {
            char req[1024];
            snprintf(req, sizeof(req),
                "{\"mode\":\"find\",\"dir\":\"na\",\"object\":\"tc\","
                "\"criteria\":[],\"order_by\":\"a,g\",\"order\":\"asc\","
                "\"limit\":2,\"cursor\":%s}", cursor);
            if (tc_request(tc, req, &r) != 0 || !r) break;
            if (strstr(r, "\"tiers\":[1,0]")) saw_null_tier_token = 1;
            const char *rows_end = strstr(r, ",\"cursor\":");
            if (!rows_end) rows_end = r + strlen(r);
            const char *p = r;
            while ((p = strstr(p, "\"key\":\"")) != NULL && p < rows_end) {
                p += 7;
                const char *e = strchr(p, '"');
                if (!e) break;
                size_t len = (size_t)(e - p);
                if (len < sizeof(seen[0])) {
                    int known = 0;
                    for (int i = 0; i < nseen; i++)
                        if (memcmp(seen[i], p, len) == 0 && seen[i][len] == 0)
                            known = 1;
                    if (!known && nseen < 5) {
                        memcpy(seen[nseen], p, len);
                        seen[nseen][len] = '\0';
                        nseen++;
                    } else if (known) dup++;
                }
                p = e;
            }
            const char *c = strstr(r, "\"cursor\":");
            const char *cs = c ? strchr(c, '{') : NULL;
            const char *ce = cs ? strchr(cs, '}') : NULL;
            if (!cs || !ce) break;   /* cursor null → done */
            size_t clen = (size_t)(ce - cs) + 1;
            if (clen >= sizeof(cursor)) break;
            memcpy(cursor, cs, clen); cursor[clen] = '\0';
            free(r); r = NULL;
        }
        free(r); r = NULL;
        ASSERT_EQ_INT(saw_null_tier_token, 1,
                    "NULL-tier composite token (a NULL, g valued) emitted");
        ASSERT_EQ_INT(nseen, 5, "all 5 rows delivered across the NULL boundary");
        ASSERT_EQ_INT(dup, 0, "no row repeated on the padded resume");
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-agg-cursor-null-composite", test_null_agg_cursor_null_composite)
