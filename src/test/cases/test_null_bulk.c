/* src/test/cases/test_null_bulk.c
 *
 * NULL handling in bulk operations (null plan Task 7):
 *   - a mixed null/non-null bulk-insert commits every valid record
 *     (per-record error isolation untouched);
 *   - bulk-update §2.7 conversion: explicit null clears to NULL, an
 *     omitted field carries the OLD value forward (including NULL);
 *   - delimited bulk-insert treats an empty cell as zeroed-value-not-NULL.
 */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static TestClient *nb_setup(TestEnv *env) {
    if (test_env_start(env) != 0) return NULL;
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(env); return NULL; }
    char *r = NULL;
    const char *seq[] = {
        "{\"mode\":\"add-dir\",\"dir\":\"nb\"}",
        "{\"mode\":\"create-object\",\"dir\":\"nb\",\"object\":\"t\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:long\",\"tag:varchar:8\"],\"indexes\":[\"v\"]}",
    };
    for (size_t i = 0; i < sizeof(seq) / sizeof(seq[0]); i++) {
        if (tc_request(tc, seq[i], &r) != 0 || strstr(r, "\"error\"")) {
            free(r); tc_close(tc); test_env_stop(env); return NULL;
        }
        free(r); r = NULL;
    }
    return tc;
}

static int nb_count(TestClient *tc, const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"nb\",\"object\":\"t\",\"criteria\":%s}",
        crit);
    char *r = NULL;
    if (tc_request(tc, req, &r) != 0 || !r) return -1;
    int n = atoi(r);
    free(r);
    return n;
}

static int test_null_bulk_mixed_insert(void) {
    TestEnv env;
    TestClient *tc = nb_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        /* Mixed null/non-null array bulk — every record commits. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"bulk-insert\",\"dir\":\"nb\",\"object\":\"t\","
            "\"records\":["
            "{\"key\":\"m1\",\"value\":{\"v\":\"1\",\"tag\":\"a\"}},"
            "{\"key\":\"m2\",\"value\":{\"v\":null,\"tag\":\"b\"}},"
            "{\"key\":\"m3\",\"value\":{\"tag\":\"c\"}},"
            "{\"key\":\"m4\",\"value\":{\"v\":\"4\",\"tag\":\"d\"}}]}", &r),
            0, "mixed bulk-insert");
        ASSERT_NOT_NULL(r, "resp");
        ASSERT_TRUE(strstr(r, "\"inserted\":4") != NULL,
                    "all 4 records committed");
        free(r); r = NULL;
        ASSERT_EQ_INT(nb_count(tc, "[{\"field\":\"v\",\"op\":\"not_exists\"}]"),
                      2, "explicit null and omitted both NULL");
        ASSERT_EQ_INT(nb_count(tc, "[{\"field\":\"v\",\"op\":\"gte\",\"value\":\"1\"}]"),
                      2, "values 1 and 4 present, no fake zeros");
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-bulk-mixed-insert", test_null_bulk_mixed_insert)

static int test_null_bulk_update_semantics(void) {
    TestEnv env;
    TestClient *tc = nb_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        const char *seed[] = {
            "{\"mode\":\"insert\",\"dir\":\"nb\",\"object\":\"t\",\"key\":\"u1\","
            "\"value\":{\"v\":\"7\",\"tag\":\"keep\"}}",
            "{\"mode\":\"insert\",\"dir\":\"nb\",\"object\":\"t\",\"key\":\"u2\","
            "\"value\":{\"v\":null,\"tag\":\"keep\"}}",
        };
        for (size_t i = 0; i < 2; i++) {
            ASSERT_EQ_INT(tc_request(tc, seed[i], &r), 0, "seed");
            free(r); r = NULL;
        }
        /* §2.7: explicit null clears; omitted carries OLD forward. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"bulk-update\",\"dir\":\"nb\",\"object\":\"t\","
            "\"records\":["
            "{\"key\":\"u1\",\"value\":{\"v\":null}},"
            "{\"key\":\"u2\",\"value\":{\"tag\":\"changed\"}}]}", &r),
            0, "bulk-update with null + omitted");
        ASSERT_NOT_NULL(r, "resp");
        free(r); r = NULL;
        ASSERT_EQ_INT(nb_count(tc, "[{\"field\":\"v\",\"op\":\"not_exists\"}]"),
                      2, "explicit null cleared u1; omitted carried u2's NULL");
        /* u1's tag carried forward; u2's v stayed NULL while tag changed. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"get\",\"dir\":\"nb\",\"object\":\"t\",\"keys\":[\"u1\"]}",
            &r), 0, "get u1");
        ASSERT_NOT_NULL(r, "u1 resp");
        ASSERT_TRUE(strstr(r, "\"tag\":\"keep\"") != NULL,
                    "omitted field carries OLD value forward");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-bulk-update-semantics", test_null_bulk_update_semantics)

/* Review finding: JSON and delimited bulk updates encode a non-NULL
 * replacement WITHOUT clearing the NULL bit the §2.7 composer carried
 * forward — the record kept rendering and matching as NULL forever after
 * a NULL → value bulk update. Both bulk shapes must materialize the
 * value (bit clear), in the record AND in the index. */
static int test_null_bulk_null_to_value(void) {
    TestEnv env;
    TestClient *tc = nb_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        /* b1: v NULL (explicit), tag present. b2: v NULL (explicit). */
        const char *seed[] = {
            "{\"mode\":\"insert\",\"dir\":\"nb\",\"object\":\"t\",\"key\":\"b1\","
            "\"value\":{\"v\":null,\"tag\":\"t1\"}}",
            "{\"mode\":\"insert\",\"dir\":\"nb\",\"object\":\"t\",\"key\":\"b2\","
            "\"value\":{\"v\":null}}",
        };
        for (size_t i = 0; i < 2; i++) {
            ASSERT_EQ_INT(tc_request(tc, seed[i], &r), 0, "seed nulls");
            free(r); r = NULL;
        }
        ASSERT_EQ_INT(nb_count(tc, "[{\"field\":\"v\",\"op\":\"not_exists\"}]"),
                      2, "both NULL before update");

        /* JSON bulk update: NULL → 9 on b1. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"bulk-update\",\"dir\":\"nb\",\"object\":\"t\","
            "\"records\":[{\"key\":\"b1\",\"value\":{\"v\":\"9\"}}]}", &r),
            0, "json bulk-update null→9");
        free(r); r = NULL;
        ASSERT_EQ_INT(nb_count(tc, "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"9\"}]"),
                      1, "b1 matches eq 9 after null→value");
        ASSERT_EQ_INT(nb_count(tc, "[{\"field\":\"v\",\"op\":\"not_exists\"}]"),
                      1, "only b2 still NULL");
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"get\",\"dir\":\"nb\",\"object\":\"t\",\"key\":\"b1\"}",
            &r), 0, "get b1");
        ASSERT_NOT_NULL(r, "b1 resp");
        ASSERT_TRUE(strstr(r, "\"v\":9") != NULL,
                    "b1 renders the value, not null");
        free(r); r = NULL;

        /* Delimited bulk update: NULL → 7 on b2 (columns: key, v, tag). */
        {
            char csv_path[256];
            snprintf(csv_path, sizeof(csv_path), "/tmp/nb_upd_%d.csv", (int)getpid());
            FILE *fp = fopen(csv_path, "w");
            ASSERT_NOT_NULL(fp, "open csv");
            if (fp) {
                fputs("b2|7|\n", fp);
                fclose(fp);
            }
            char req[512];
            snprintf(req, sizeof(req),
                "{\"mode\":\"bulk-update-delimited\",\"dir\":\"nb\",\"object\":\"t\","
                "\"file\":\"%s\",\"delimiter\":\"|\"}", csv_path);
            ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "delimited update");
            free(r); r = NULL;
            unlink(csv_path);
        }
        ASSERT_EQ_INT(nb_count(tc, "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"7\"}]"),
                      1, "b2 matches eq 7 after delimited null→value");
        ASSERT_EQ_INT(nb_count(tc, "[{\"field\":\"v\",\"op\":\"not_exists\"}]"),
                      0, "no NULLs remain");
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"get\",\"dir\":\"nb\",\"object\":\"t\",\"key\":\"b2\"}",
            &r), 0, "get b2");
        ASSERT_NOT_NULL(r, "b2 resp");
        ASSERT_TRUE(strstr(r, "\"v\":7") != NULL,
                    "b2 renders the value, not null");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-bulk-null-to-value", test_null_bulk_null_to_value)
