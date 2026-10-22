/* Public routes must reject unsafe scalar conversions, preserve
   criterion/write parity, and round-trip stored floating values. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *req(TestClient *tc, const char *json) {
    char *resp = NULL;
    if (tc_request(tc, json, &resp) != 0) return NULL;
    return resp;
}

static int test_scalar_conversion_safety_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *r = req(tc, "{\"mode\":\"add-dir\",\"dir\":\"conv\"}"); free(r);
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"conv\","
        "\"object\":\"t\",\"splits\":8,\"max_key\":64,\"fields\":["
        "\"s:short\",\"i:int\",\"l:long\",\"b:byte\",\"n:numeric:26,10\","
        "\"r:numeric:12,2\",\"z:numeric:10,0\",\"f:double\",\"g:float\","
        "\"v:varchar:32\"]}");
    ASSERT_NOT_NULL(r, "create responds");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "create ok");
    free(r);

    /* Rejections: each is refused with a specific message and creates
       no record. */
    struct { const char *key, *field, *val, *err_frag; } bad[] = {
        {"k1","s","32768","out of short range"},
        {"k2","i","2147483648","out of int range"},
        {"k3","i","123abc","expected a whole decimal number"},
        {"k4","l","9223372036854775808","out of signed 64-bit range"},
        {"k5","b","256","out of byte range"},
        {"k6","b","-1","out of byte range"},
        {"k7","n","1234567890123456.1234567890",
         "out of range for numeric scale 10"},
        {"k8","f","1e999","only finite values are accepted"},
        {"k9","f","1e-999","underflows to zero"},
        {"k10","g","nan","only finite values are accepted"},
        {"k11","i","1.5","expected a whole decimal number"},
    };
    for (size_t k = 0; k < sizeof(bad)/sizeof(bad[0]); k++) {
        char jq[512];
        snprintf(jq, sizeof(jq),
                 "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
                 "\"key\":\"%s\",\"value\":{\"%s\":\"%s\"}}",
                 bad[k].key, bad[k].field, bad[k].val);
        char *resp = req(tc, jq);
        ASSERT_NOT_NULL(resp, "reject insert responds");
        ASSERT_TRUE(strstr(resp, "\"error\"") != NULL, "insert rejected");
        ASSERT_TRUE(strstr(resp, bad[k].err_frag) != NULL, bad[k].err_frag);
        free(resp);
        snprintf(jq, sizeof(jq),
                 "{\"mode\":\"exists\",\"dir\":\"conv\",\"object\":\"t\","
                 "\"key\":\"%s\"}", bad[k].key);
        resp = req(tc, jq);
        ASSERT_NOT_NULL(resp, "exists responds");
        ASSERT_TRUE(strncmp(resp, "false", 5) == 0, "rejected record absent");
        free(resp);
    }

    /* Rejection preserves the old record. */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"keep\",\"value\":{\"i\":\"7\",\"s\":\"1\",\"l\":\"1\","
        "\"b\":\"1\",\"n\":\"1.50\",\"f\":\"1.5\",\"g\":\"1.5\",\"v\":\"abc\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "seed insert");
    free(r);
    r = req(tc, "{\"mode\":\"update\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"keep\",\"value\":{\"i\":\"2147483648\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") != NULL, "update rejected");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"keep\"}");
    ASSERT_TRUE(r && strstr(r, "\"i\":7") != NULL, "old value intact");
    ASSERT_TRUE(r && strstr(r, "\"v\":\"abc\"") != NULL, "varchar intact");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\"}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "count is 1");
    free(r);

    /* Exact boundaries: INT64_MIN long, SHORT_MIN, scale-0 numeric
       INT64_MIN, scalar outer-whitespace trimming, varchar
       byte-exactness. */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"ok\",\"value\":{\"l\":\"-9223372036854775808\","
        "\"s\":\"-32768\",\"i\":\" 42 \",\"z\":\"-9223372036854775808\","
        "\"v\":\"  padded  \"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "boundary insert ok");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"ok\"}");
    ASSERT_TRUE(r && strstr(r, "\"l\":-9223372036854775808") != NULL,
                "INT64_MIN long round trips");
    ASSERT_TRUE(r && strstr(r, "\"s\":-32768") != NULL, "SHORT_MIN round trips");
    ASSERT_TRUE(r && strstr(r, "\"i\":42") != NULL, "scalar span trimmed");
    ASSERT_TRUE(r && strstr(r, "\"z\":-9223372036854775808") != NULL,
                "scale-0 numeric INT64_MIN round trips");
    ASSERT_TRUE(r && strstr(r, "\"v\":\"  padded  \"") != NULL,
                "varchar bytes exact (spaces kept)");
    free(r);
    /* Scale-0 and near-zero numerics are findable by their exact value. */
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"z\",\"op\":\"eq\","
        "\"value\":\"-9223372036854775808\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "scale-0 INT64_MIN eq finds");
    free(r);

    /* Numeric rounding + write/query parity: 0.145 at scale 2 stores
       0.15 (ties away from zero — the old double path stored 14); both
       the rounded and the original literal must find the row. 6e-100
       rounds to 0.00, not 0.01. */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"rnd\",\"value\":{\"r\":\"0.145\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "rounding insert ok");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"rnd\"}");
    ASSERT_TRUE(r && strstr(r, "\"r\":0.15") != NULL,
                "0.145 stored as 0.15 (ties away)");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"r\",\"op\":\"eq\",\"value\":\"0.145\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "eq 0.145 finds (parity)");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"r\",\"op\":\"eq\",\"value\":\"0.15\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "eq 0.15 finds (parity)");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"tiny\",\"value\":{\"r\":\"6e-100\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "tiny insert ok");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"tiny\"}");
    ASSERT_TRUE(r && strstr(r, "\"r\":0") != NULL &&
                strstr(r, "\"r\":0.01") == NULL,
                "6e-100 rounds to 0.00, not 0.01");
    free(r);
    /* Zero-with-exponent and leading-zero forms are valid numbers. */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"ze\",\"value\":{\"r\":\"0e19\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "0e19 insert ok");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"ze\"}");
    ASSERT_TRUE(r && strstr(r, "\"r\":0") != NULL &&
                strstr(r, "\"r\":0.01") == NULL, "0e19 stored as 0");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"leadz\",\"value\":{\"r\":\"001.50\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "leading-zero insert ok");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"r\",\"op\":\"eq\",\"value\":\"1.5\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "eq 1.5 finds 001.50 (parity)");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"r\",\"op\":\"eq\",\"value\":\"0\"}]}");
    ASSERT_TRUE(r && strncmp(r, "2", 1) == 0, "eq 0 finds tiny + 0e19");
    free(r);

    /* Indexed/unindexed parity for a validated numeric write. */
    r = req(tc, "{\"mode\":\"add-index\",\"dir\":\"conv\",\"object\":\"t\","
        "\"field\":\"n\"}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "add-index");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"idx\",\"value\":{\"n\":\"1.23456789012345678901\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "indexed insert");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"idx\"}");
    ASSERT_TRUE(r && strstr(r, "\"n\":1.2345678901") != NULL,
                "numeric rounded to declared scale");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"n\",\"op\":\"eq\","
        "\"value\":\"1.2345678901\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "indexed eq finds");
    free(r);

    /* Indexed/scan literal parity: every accepted
       spelling must find its own row by scan (before add-index) and by
       index (after). */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"lz\",\"value\":{\"i\":\"00000000000000000001\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "leading-zero int insert");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"lzl\",\"value\":{\"l\":\"0000000000000000000001\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "leading-zero long insert");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"bigf\",\"value\":{\"f\":"
        "\"1.000000000000000000000000000000000000000000000000e10\",\"g\":"
        "\"1.000000000000000000000000000000000000000000000000e10\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "long float literal insert");
    free(r);
    /* scan side — i, l, f, g have no indexes yet */
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"i\",\"op\":\"eq\","
        "\"value\":\"00000000000000000001\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "scan finds leading-zero int");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"l\",\"op\":\"eq\","
        "\"value\":\"0000000000000000000001\"}]}");
    /* the seed row keep also holds l=1, so the count is 2 */
    ASSERT_TRUE(r && strncmp(r, "2", 1) == 0, "scan finds leading-zero long");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"f\",\"op\":\"eq\",\"value\":"
        "\"1.000000000000000000000000000000000000000000000000e10\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "scan finds long double literal");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"g\",\"op\":\"eq\",\"value\":"
        "\"1.000000000000000000000000000000000000000000000000e10\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "scan finds long float literal");
    free(r);
    /* index side — the add-index build itself must encode these rows
       findably, and subsequent writes must maintain them */
    r = req(tc, "{\"mode\":\"add-index\",\"dir\":\"conv\",\"object\":\"t\","
        "\"field\":\"i\"}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "add-index i"); free(r);
    r = req(tc, "{\"mode\":\"add-index\",\"dir\":\"conv\",\"object\":\"t\","
        "\"field\":\"l\"}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "add-index l"); free(r);
    r = req(tc, "{\"mode\":\"add-index\",\"dir\":\"conv\",\"object\":\"t\","
        "\"field\":\"f\"}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "add-index f"); free(r);
    r = req(tc, "{\"mode\":\"add-index\",\"dir\":\"conv\",\"object\":\"t\","
        "\"field\":\"g\"}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "add-index g"); free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"i\",\"op\":\"eq\","
        "\"value\":\"00000000000000000001\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "index finds leading-zero int");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"l\",\"op\":\"eq\","
        "\"value\":\"0000000000000000000001\"}]}");
    ASSERT_TRUE(r && strncmp(r, "2", 1) == 0, "index finds leading-zero long");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"f\",\"op\":\"eq\",\"value\":"
        "\"1.000000000000000000000000000000000000000000000000e10\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "index finds long double literal");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"g\",\"op\":\"eq\",\"value\":"
        "\"1.000000000000000000000000000000000000000000000000e10\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0, "index finds long float literal");
    free(r);
    /* binary32 double-rounding literal: stores and looks up identically
       only when every route parses with strtof. */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"dr\",\"value\":{\"g\":\"1.0000000596046448\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "rounding-literal insert");
    free(r);
    r = req(tc, "{\"mode\":\"count\",\"dir\":\"conv\",\"object\":\"t\","
        "\"criteria\":[{\"field\":\"g\",\"op\":\"eq\","
        "\"value\":\"1.0000000596046448\"}]}");
    ASSERT_TRUE(r && strncmp(r, "1", 1) == 0,
                "index finds double-rounding literal");
    free(r);

    /* Bulk insert: mixed valid/invalid rows keep the per-row contract
       (wire field `records`, response carries `"errors":N`). */
    r = req(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"records\":[{\"key\":\"bok\",\"value\":{\"i\":\"5\"}},"
        "{\"key\":\"bbad\",\"value\":{\"i\":\"2147483648\"}}]}");
    ASSERT_NOT_NULL(r, "bulk insert responds");
    ASSERT_TRUE(r && strstr(r, "\"errors\":1") != NULL,
                "bulk error tallied");
    free(r);
    r = req(tc, "{\"mode\":\"exists\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"bok\"}");
    ASSERT_TRUE(r && strncmp(r, "true", 4) == 0, "valid bulk row stored");
    free(r);
    r = req(tc, "{\"mode\":\"exists\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"bbad\"}");
    ASSERT_TRUE(r && strncmp(r, "false", 5) == 0, "bad bulk row skipped");
    free(r);

    /* Overflowing literal default now fails the insert (was silent
       wrap). */
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"conv\","
        "\"object\":\"dflt\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"x:int:default=99999999999\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "dflt object created "
                "(default validation is Phase 4)");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"dflt\","
        "\"key\":\"a\",\"value\":{}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") != NULL &&
                strstr(r, "x") != NULL,
                "overflowing literal default refuses the insert");
    free(r);

    /* Float→double widening preserves the exact stored value. */
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"conv\","
        "\"object\":\"w\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"x:float\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "widen create"); free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"w\","
        "\"key\":\"a\",\"value\":{\"x\":\"3.25\"}}"); free(r);
    r = req(tc, "{\"mode\":\"edit-field\",\"dir\":\"conv\",\"object\":\"w\","
        "\"fields\":[\"x:double\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "widen edit"); free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"w\","
        "\"key\":\"a\"}");
    ASSERT_TRUE(r && strstr(r, "\"x\":3.25") != NULL,
                "widened float reads back exactly");
    free(r);

    /* Round-trip float output: 9/17 significant digits. */
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"flt\",\"value\":{\"f\":\"1.2345678901234567\","
        "\"g\":\"1.2345678\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "float insert ok");
    free(r);
    r = req(tc, "{\"mode\":\"get\",\"dir\":\"conv\",\"object\":\"t\","
        "\"key\":\"flt\"}");
    ASSERT_TRUE(r && strstr(r, "\"f\":1.2345678901234567") != NULL,
                "double output round-trips");
    ASSERT_TRUE(r && strstr(r, "\"g\":1.23456776") != NULL,
                "float output round-trips (9 digits; Phase 0 types-audit "
                "shows stored=1.23456776)");
    free(r);

    /* Aggregate text must agree with ordinary reads, before and after
       index creation. A dedicated object keeps each group unambiguous. */
    r = req(tc, "{\"mode\":\"create-object\",\"dir\":\"conv\","
        "\"object\":\"ag\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"n:numeric:19,0\",\"d:double\",\"f:float\"]}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "aggregate object created");
    free(r);
    r = req(tc, "{\"mode\":\"insert\",\"dir\":\"conv\",\"object\":\"ag\","
        "\"key\":\"one\",\"value\":{\"n\":\"-9223372036854775808\","
        "\"d\":\"1.2345678901234567\",\"f\":\"1.2345678\"}}");
    ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL, "aggregate seed stored");
    free(r);
    const char *ag_fields[] = {"n", "d", "f"};
    const char *ag_values[] = {"-9223372036854775808",
                              "1.2345678901234567", "1.23456776"};
    for (int indexed = 0; indexed < 2; indexed++) {
        for (int j = 0; j < 3; j++) {
            char jq[512], expected[128];
            if (indexed) {
                snprintf(jq, sizeof(jq),
                    "{\"mode\":\"add-index\",\"dir\":\"conv\","
                    "\"object\":\"ag\",\"field\":\"%s\"}", ag_fields[j]);
                r = req(tc, jq);
                ASSERT_TRUE(r && strstr(r, "\"error\"") == NULL,
                            "aggregate index created");
                free(r);
            }
            snprintf(jq, sizeof(jq),
                "{\"mode\":\"aggregate\",\"dir\":\"conv\",\"object\":\"ag\","
                "\"group_by\":\"%s\",\"aggregates\":[{\"fn\":\"count\","
                "\"alias\":\"c\"}]}", ag_fields[j]);
            r = req(tc, jq);
            snprintf(expected, sizeof(expected), "\"%s\":\"%s\"",
                     ag_fields[j], ag_values[j]);
            ASSERT_TRUE(r && strstr(r, expected) != NULL,
                        indexed ? "indexed aggregate label exact" :
                                  "scan aggregate label exact");
            ASSERT_TRUE(r && strstr(r, "\"c\":1") != NULL,
                        "aggregate group contains the seed");
            free(r);
        }
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-scalar-conversion-safety",
              test_scalar_conversion_safety_run)
