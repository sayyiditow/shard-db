/* src/test/cases/test_not_null_writes.c
 *
 * :not_null modifier — insert-side enforcement
 * all five encoder branches + malformed-sentinel rejections + bulk
 * per-record errors + the typed_encode unit contract.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"   /* typed_encode, typed_field_null, TypedSchema (-Isrc/db) */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define UID1 "11111111-1111-1111-1111-111111111111"
#define V4OK "1.2.3.4"
#define V6OK "::1"

static int test_not_null_writes_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* w1: every field :not_null; amount carries a default so omitted-with-
       default is exercisable; req/uid/v4/v6 have NO default (omission is
       itself a violation, so tests always supply them except when a step
       targets that exact field). w0: identical, unconstrained (control). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"w1\","
        "\"splits\":8,\"max_key\":32,"
        "\"fields\":[\"name:varchar:16\","
                    "\"amount:numeric:18,2:default=9.99:not_null\","
                    "\"uid:uuid:not_null\",\"req:int:not_null\","
                    "\"v4:ipv4:not_null\",\"v6:ipv6:not_null\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create w1");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"w0\","
        "\"splits\":8,\"max_key\":32,"
        "\"fields\":[\"name:varchar:16\",\"amount:numeric:18,2\","
                    "\"uid:uuid\",\"req:int\",\"v4:ipv4\",\"v6:ipv6\"]}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create w0");
    free(resp); resp = NULL;

    /* 1. omitted-with-default: default lands, bit clear. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"w1\","
                   "\"key\":\"k_ok\",\"value\":{\"name\":\"a\",\"uid\":\"" UID1
                   "\",\"req\":1,\"v4\":\"" V4OK "\",\"v6\":\"" V6OK "\"}}",
               &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "omitted-with-default insert");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"w1\","
                   "\"key\":\"k_ok\"}", &resp);
    ASSERT_CONTAINS(resp, "\"amount\":9.99", "default landed");
    free(resp); resp = NULL;

    /* 2. explicit null on the defaulted field → rejected (DEFAULT does
          NOT rescue), nothing written. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"w1\","
                   "\"key\":\"kx\",\"value\":{\"name\":\"a\",\"amount\":null,"
                   "\"uid\":\"" UID1 "\",\"req\":1,\"v4\":\"" V4OK "\","
                   "\"v6\":\"" V6OK "\"}}", &resp);
    ASSERT_CONTAINS(resp, "field 'amount' is not nullable",
                    "explicit null rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"w1\","
                   "\"key\":\"kx\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "false"), "rejected insert wrote nothing");
    free(resp); resp = NULL;

    /* 3. omitted, no default → rejected. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"w1\","
                   "\"key\":\"ky\",\"value\":{\"name\":\"b\","
                   "\"amount\":\"1.00\",\"uid\":\"" UID1 "\","
                   "\"v4\":\"" V4OK "\",\"v6\":\"" V6OK "\"}}", &resp);
    ASSERT_CONTAINS(resp, "field 'req' is not nullable",
                    "omission rejected");
    free(resp); resp = NULL;

    /* 4. malformed sentinels → rejected (each isolates one field). */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"w1\","
                   "\"key\":\"kz\",\"value\":{\"name\":\"c\","
                   "\"amount\":\"1.00\",\"uid\":\"nope\",\"req\":1,"
                   "\"v4\":\"" V4OK "\",\"v6\":\"" V6OK "\"}}", &resp);
    ASSERT_CONTAINS(resp, "field 'uid' is not nullable",
                    "malformed uuid rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"w1\","
                   "\"key\":\"kz\",\"value\":{\"name\":\"c\","
                   "\"amount\":\"1.00\",\"uid\":\"" UID1 "\",\"req\":1,"
                   "\"v4\":\"999.999.1.1\",\"v6\":\"" V6OK "\"}}", &resp);
    ASSERT_CONTAINS(resp, "field 'v4' is not nullable",
                    "malformed ipv4 rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"w1\","
                   "\"key\":\"kz\",\"value\":{\"name\":\"c\","
                   "\"amount\":\"1.00\",\"uid\":\"" UID1 "\",\"req\":1,"
                   "\"v4\":\"" V4OK "\",\"v6\":\"zz::1\"}}", &resp);
    ASSERT_CONTAINS(resp, "field 'v6' is not nullable",
                    "malformed ipv6 rejected");
    free(resp); resp = NULL;

    /* 5. control (w0): all five shapes succeed; NULLs render. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"w0\","
                   "\"key\":\"c1\",\"value\":{\"name\":\"c\",\"amount\":null,"
                   "\"uid\":\"nope\",\"v4\":\"999.999.1.1\","
                   "\"v6\":\"zz::1\"}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "control accepts everything");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"w0\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"uid\":null", "control renders NULL");
    free(resp); resp = NULL;

    /* 6. well-formed zeros stay legal on w1 (sentinel ≠ NULL). */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"w1\","
                   "\"key\":\"k_zero\",\"value\":{\"name\":\"z\","
                   "\"amount\":\"0.00\","
                   "\"uid\":\"00000000-0000-0000-0000-000000000000\","
                   "\"req\":0,\"v4\":\"0.0.0.0\",\"v6\":\"::\"}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "zeros accepted on constrained fields");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"w1\","
                   "\"key\":\"k_zero\"}", &resp);
    ASSERT_CONTAINS(resp, "\"v4\":\"0.0.0.0\"", "zero ipv4 renders");
    ASSERT_CONTAINS(resp, "\"v6\":\"::\"", "zero ipv6 renders");
    free(resp); resp = NULL;

    /* 7. bulk insert JSON: per-record errors, exact counts, peer records
          commit. Each record supplies every mandatory field except its
          single violation. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"w1\","
        "\"records\":["
        "{\"key\":\"b1\",\"value\":{\"name\":\"ok\",\"amount\":\"2.00\","
        "\"uid\":\"" UID1 "\",\"req\":1,\"v4\":\"" V4OK "\",\"v6\":\"" V6OK "\"}},"
        "{\"key\":\"b2\",\"value\":{\"name\":\"null\",\"amount\":null,"
        "\"uid\":\"" UID1 "\",\"req\":1,\"v4\":\"" V4OK "\",\"v6\":\"" V6OK "\"}},"
        "{\"key\":\"b3\",\"value\":{\"name\":\"omit\",\"amount\":\"2.00\","
        "\"uid\":\"" UID1 "\",\"v4\":\"" V4OK "\",\"v6\":\"" V6OK "\"}},"
        "{\"key\":\"b4\",\"value\":{\"name\":\"bad\",\"amount\":\"2.00\","
        "\"uid\":\"bad\",\"req\":1,\"v4\":\"" V4OK "\",\"v6\":\"" V6OK "\"}}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":1", "bulk one good record");
    ASSERT_CONTAINS(resp, "\"errors\":3", "bulk three per-record errors");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"w1\","
                   "\"key\":\"b1\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "true"), "good bulk record present");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"w1\","
                   "\"key\":\"b2\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "false"), "violating record absent");
    free(resp); resp = NULL;

    /* 8. typed_encode unit contract (no wire path — dead-but-exported). */
    {
        TypedSchema tsu;
        memset(&tsu, 0, sizeof(tsu));
        TypedField *f = &tsu.fields[0];
        strcpy(f->name, "f");
        f->name_len = 1;
        f->type = FT_INT;
        f->size = 4;
        f->offset = 0;
        tsu.nfields = 1;
        tsu.typed = 1;
        tsu.nullmap_prefix = 1 + typed_nullmap_bytes(tsu.nfields);
        tsu.total_size = tsu.nullmap_prefix + f->size;
        uint8_t out[64];

        tsu.fields[0].not_null = 1;
        ASSERT_EQ_INT(typed_encode(&tsu, "{\"f\":null}", out, sizeof(out)), -2,
                      "typed_encode rejects null on not_null field");
        ASSERT_EQ_INT(typed_encode(&tsu, "{\"f\":5}", out, sizeof(out)),
                      tsu.total_size,
                      "typed_encode accepts value on not_null field");
        tsu.fields[0].not_null = 0;
        ASSERT_EQ_INT(typed_encode(&tsu, "{\"f\":null}", out, sizeof(out)),
                      tsu.total_size,
                      "typed_encode accepts null unconstrained");
        ASSERT_EQ_INT(typed_field_null(out + 1, tsu.nullmap_prefix - 1, 0), 1,
                      "null bit set on unconstrained null");
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-not-null-writes", test_not_null_writes_run)
