#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

static char *type_accept_request(TestClient *tc, const char *request) {
    char *response = NULL;
    int rc = tc_request(tc, request, &response);
    ASSERT_EQ_INT(rc, 0, "acceptance transport succeeds");
    ASSERT_NOT_NULL(response, "acceptance response exists");
    return response;
}

static int type_accept_number(const char *response, const char *field,
                              const char *expected) {
    char key[80];
    int n = snprintf(key, sizeof(key), "\"%s\":", field);
    if (!response || n < 0 || (size_t)n >= sizeof(key)) return 0;
    const char *p = strstr(response, key);
    if (!p) return 0;
    p += (size_t)n;
    while (isspace((unsigned char)*p)) p++;
    size_t len = strcspn(p, ",}] \t\r\n");
    return len == strlen(expected) && memcmp(p, expected, len) == 0;
}

static int type_accept_create(TestClient *tc, const char *request) {
    char *response = type_accept_request(tc, request);
    int created = response && strstr(response, "\"error\"") == NULL &&
                  strstr(response, "created") != NULL;
    ASSERT_TRUE(created, "acceptance object created");
    free(response);
    return created;
}

static void type_accept_insert_literal(TestClient *tc, const char *object,
                                       const char *key, const char *field,
                                       const char *literal, int reject) {
    size_t cap = strlen(literal) + 512;
    char *request = malloc(cap);
    ASSERT_NOT_NULL(request, "acceptance request allocation");
    if (!request) return;
    /* Inputs to this helper are fixture-owned digits/ASCII letters only. */
    int n = snprintf(request, cap,
        "{\"mode\":\"insert\",\"dir\":\"type_accept\","
        "\"object\":\"%s\",\"key\":\"%s\","
        "\"value\":{\"%s\":\"%s\"}}", object, key, field, literal);
    ASSERT_TRUE(n >= 0 && (size_t)n < cap, "acceptance request fits");
    if (n >= 0 && (size_t)n < cap) {
        char *response = type_accept_request(tc, request);
        ASSERT_TRUE(response &&
            ((strstr(response, "\"error\"") != NULL) == reject),
            "acceptance insert validation result");
        free(response);
    }
    free(request);
}


static char *type_accept_get(TestClient *tc, const char *object, const char *key) {
    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"type_accept\",\"object\":\"%s\",\"key\":\"%s\"}",
        object, key);
    return type_accept_request(tc, req);
}

static void type_accept_expect_layout(TestClient *tc, const char *object,
                                      const char *layout) {
    char req[256], want[64];
    snprintf(req, sizeof(req),
        "{\"mode\":\"describe-object\",\"dir\":\"type_accept\",\"object\":\"%s\"}",
        object);
    snprintf(want, sizeof(want), "\"declaration_layout\":\"%s\"", layout);
    char *response = type_accept_request(tc, req);
    ASSERT_CONTAINS(response, want, "object has the expected declaration layout");
    free(response);
}

static void type_accept_expect_count(TestClient *tc, const char *object,
                                     int expected, const char *desc) {
    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"type_accept\",\"object\":\"%s\"}", object);
    char *response = type_accept_request(tc, req);
    ASSERT_EQ_INT(tu_parse_count(response), expected, desc);
    free(response);
}

/* Everything that must survive a clean stop/start of the same root. */
#define TA_WHOLE 131072
#define TA_FRACTION 16383
#define TA_VCEIL 4194304

static void type_accept_verify_persisted(TestClient *tc, const char *phase) {
    char desc[96];
    char *response = type_accept_get(tc, "fixed", "exact");
    snprintf(desc, sizeof(desc), "%s: constrained decimal exact", phase);
    ASSERT_TRUE(type_accept_number(response, "n", "1234567890123456.1234567890"), desc);
    free(response);

    response = type_accept_get(tc, "bare", "small");
    snprintf(desc, sizeof(desc), "%s: bare numeric scale kept", phase);
    ASSERT_TRUE(type_accept_number(response, "n", "1.2300"), desc);
    free(response);

    size_t length = (size_t)TA_WHOLE + 1 + TA_FRACTION;
    char *expected = malloc(length + 1);
    ASSERT_NOT_NULL(expected, "maximum decimal allocation");
    if (expected) {
        memset(expected, '9', TA_WHOLE);
        expected[TA_WHOLE] = '.';
        memset(expected + TA_WHOLE + 1, '1', TA_FRACTION);
        expected[length] = '\0';
        response = type_accept_get(tc, "bare", "maximum");
        snprintf(desc, sizeof(desc), "%s: maximum finite decimal exact", phase);
        ASSERT_TRUE(type_accept_number(response, "n", expected), desc);
        free(response);
        free(expected);
    }

    char *want = malloc(TA_VCEIL + 1);
    ASSERT_NOT_NULL(want, "varchar ceiling allocation");
    if (want) {
        memset(want, 'a', TA_VCEIL);
        want[TA_VCEIL] = '\0';
        response = type_accept_get(tc, "strings", "at_max");
        const char *pv = response ? strstr(response, "\"v\":\"") : NULL;
        snprintf(desc, sizeof(desc), "%s: 4 MiB varchar exact", phase);
        ASSERT_TRUE(pv && memcmp(pv + 5, want, TA_VCEIL) == 0 &&
                    pv[5 + TA_VCEIL] == '"', desc);
        free(response);
        free(want);
    }
}

static int test_type_correctness_acceptance_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "acceptance connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *response = type_accept_request(tc,
        "{\"mode\":\"add-dir\",\"dir\":\"type_accept\"}");
    free(response);

    /* Negative control: the harness can tell legacy from packed, so the
       "packed" assertions below cannot pass vacuously. TEST_BUILD only. */
    int control = type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"control\",\"splits\":8,\"max_key\":16,"
        "\"declaration_layout\":\"legacy\",\"fields\":[\"i:int\"]}");
    if (control) type_accept_expect_layout(tc, "control", "legacy");

    int fixed = type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"fixed\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"n:numeric:26,10\",\"f:double\",\"i:int\"],"
        "\"indexes\":[\"n\"]}");
    if (fixed) {
        type_accept_expect_layout(tc, "fixed", "packed");
        type_accept_insert_literal(tc, "fixed", "bad", "i", "2147483648", 1);
        type_accept_expect_count(tc, "fixed", 0, "rejected integer creates no record");
        response = type_accept_request(tc,
            "{\"mode\":\"insert\",\"dir\":\"type_accept\","
            "\"object\":\"fixed\",\"key\":\"exact\",\"value\":{"
            "\"n\":\"1234567890123456.1234567890\","
            "\"f\":\"1.2345678901234567\"}}");
        ASSERT_TRUE(response && !strstr(response, "\"error\""),
                    "exact constrained decimal insert succeeds");
        free(response);
        /* adjacent last-place neighbour and a negative counterpart */
        response = type_accept_request(tc,
            "{\"mode\":\"insert\",\"dir\":\"type_accept\","
            "\"object\":\"fixed\",\"key\":\"next\",\"value\":{"
            "\"n\":\"1234567890123456.1234567891\"}}");
        ASSERT_TRUE(response && !strstr(response, "\"error\""), "adjacent decimal insert");
        free(response);
        response = type_accept_request(tc,
            "{\"mode\":\"insert\",\"dir\":\"type_accept\","
            "\"object\":\"fixed\",\"key\":\"neg\",\"value\":{"
            "\"n\":\"-1234567890123456.1234567890\"}}");
        ASSERT_TRUE(response && !strstr(response, "\"error\""), "negative decimal insert");
        free(response);

        response = type_accept_get(tc, "fixed", "exact");
        ASSERT_TRUE(type_accept_number(response, "n", "1234567890123456.1234567890"),
                    "constrained decimal retains value and scale");
        ASSERT_TRUE(type_accept_number(response, "f", "1.2345678901234567"),
                    "double output retains stored value");
        free(response);

        /* indexed equality must separate last-place neighbours exactly */
        response = type_accept_request(tc,
            "{\"mode\":\"count\",\"dir\":\"type_accept\",\"object\":\"fixed\","
            "\"criteria\":[{\"field\":\"n\",\"op\":\"eq\","
            "\"value\":\"1234567890123456.1234567890\"}]}");
        ASSERT_EQ_INT(tu_parse_count(response), 1, "indexed eq matches exactly one decimal");
        free(response);
        response = type_accept_request(tc,
            "{\"mode\":\"count\",\"dir\":\"type_accept\",\"object\":\"fixed\","
            "\"criteria\":[{\"field\":\"n\",\"op\":\"gt\","
            "\"value\":\"1234567890123456.1234567890\"}]}");
        ASSERT_EQ_INT(tu_parse_count(response), 1, "indexed gt sees only the adjacent decimal");
        free(response);
        response = type_accept_request(tc,
            "{\"mode\":\"aggregate\",\"dir\":\"type_accept\",\"object\":\"fixed\","
            "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"n\",\"alias\":\"s\"}],"
            "\"criteria\":[{\"field\":\"n\",\"op\":\"gt\",\"value\":\"0\"}]}");
        ASSERT_CONTAINS(response, "2469135780246912.2469135781",
                        "exact decimal sum of the two positive rows");
        free(response);
    }

    int bare = type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"bare\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"n:numeric\",\"after:varchar:8\"]}");
    if (bare) {
        type_accept_expect_layout(tc, "bare", "packed");
        type_accept_insert_literal(tc, "bare", "small", "n", "1.2300", 0);
        response = type_accept_request(tc,
            "{\"mode\":\"update\",\"dir\":\"type_accept\","
            "\"object\":\"bare\",\"key\":\"small\","
            "\"value\":{\"after\":\"tail\"}}");
        ASSERT_TRUE(response && !strstr(response, "\"error\""),
                    "update following field succeeds");
        free(response);
        response = type_accept_get(tc, "bare", "small");
        ASSERT_TRUE(type_accept_number(response, "n", "1.2300"),
                    "bare numeric scale survives following-field update");
        ASSERT_CONTAINS(response, "\"after\":\"tail\"", "following field survives");
        free(response);

        size_t length = (size_t)TA_WHOLE + 1 + TA_FRACTION;
        char *value = malloc(length + 1);
        ASSERT_NOT_NULL(value, "maximum decimal allocation");
        if (value) {
            memset(value, '9', TA_WHOLE);
            value[TA_WHOLE] = '.';
            memset(value + TA_WHOLE + 1, '1', TA_FRACTION);
            value[length] = '\0';
            type_accept_insert_literal(tc, "bare", "maximum", "n", value, 0);
            response = type_accept_get(tc, "bare", "maximum");
            ASSERT_TRUE(type_accept_number(response, "n", value),
                        "full PostgreSQL finite digit range round trips");
            free(response);
            /* one more integer digit must be rejected and leave no record */
            char *over = malloc(length + 2);
            ASSERT_NOT_NULL(over, "over-limit decimal allocation");
            if (over) {
                over[0] = '9';
                memcpy(over + 1, value, length + 1);
                type_accept_insert_literal(tc, "bare", "over_whole", "n", over, 1);
                free(over);
            }
            /* one more fractional digit must be rejected too */
            char *over_frac = malloc(length + 2);
            ASSERT_NOT_NULL(over_frac, "over-limit fraction allocation");
            if (over_frac) {
                memcpy(over_frac, value, length);
                over_frac[length] = '1'; over_frac[length + 1] = '\0';
                type_accept_insert_literal(tc, "bare", "over_frac", "n", over_frac, 1);
                free(over_frac);
            }
            free(value);
        }
        type_accept_expect_count(tc, "bare", 2, "rejected decimals create no records");
    }

    int strings = type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"strings\",\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"v:varchar\",\"after:int\"]}");
    if (strings) {
        type_accept_expect_layout(tc, "strings", "packed");
        type_accept_insert_literal(tc, "strings", "small", "v", "abc", 0);
        char *value = malloc((size_t)TA_VCEIL + 2);
        ASSERT_NOT_NULL(value, "varchar boundary allocation");
        if (value) {
            memset(value, 'a', TA_VCEIL); value[TA_VCEIL] = '\0';
            type_accept_insert_literal(tc, "strings", "at_max", "v", value, 0);
            value[TA_VCEIL] = 'a'; value[TA_VCEIL + 1] = '\0';
            type_accept_insert_literal(tc, "strings", "over_max", "v", value, 1);
            free(value);
        }
        type_accept_expect_count(tc, "strings", 2, "ceiling+1 varchar creates no record");
    }

    /* constrained-precision ceiling: P=1000 is accepted, P=1001 refused */
    ASSERT_TRUE(type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"plim_ok\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"n:numeric:1000,10\"]}"), "numeric:1000,10 is accepted");
    response = type_accept_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"plim_bad\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"n:numeric:1001,10\"]}");
    ASSERT_CONTAINS(response, "\"error\"", "numeric:1001,10 is refused");
    free(response);

    /* bare numeric:N means scale 0 on a new (packed) object */
    if (type_accept_create(tc,
        "{\"mode\":\"create-object\",\"dir\":\"type_accept\","
        "\"object\":\"scale0\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:numeric:12\"]}")) {
        type_accept_expect_layout(tc, "scale0", "packed");
        type_accept_insert_literal(tc, "scale0", "w", "v", "100", 0);
        response = type_accept_get(tc, "scale0", "w");
        ASSERT_TRUE(type_accept_number(response, "v", "100"),
                    "numeric:N on a packed object renders whole values at scale 0");
        free(response);
    }

    type_accept_verify_persisted(tc, "live");

    /* clean stop and restart of the same root: nothing may change */
    tc_close(tc);
    char root[sizeof(env.db_root)];
    snprintf(root, sizeof(root), "%s", env.db_root);
    int port = env.port;
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(test_env_start_at(&env, root, port), 0, "restart on the same root");
    tc = tc_connect(&(TestClientCfg){ .port = port, .io_timeout_ms = 60000 });
    ASSERT_NOT_NULL(tc, "reconnect after restart");
    if (tc) {
        type_accept_expect_layout(tc, "fixed", "packed");
        type_accept_verify_persisted(tc, "reopened");
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-type-correctness-acceptance",
              test_type_correctness_acceptance_run)
