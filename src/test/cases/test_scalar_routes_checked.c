/* JSON scalar strings decode exactly once before checked conversion. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../../db/types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void scalar_route_expect(TestClient *tc, const char *request,
                                const char *fragment, const char *label) {
    char *response = NULL;
    int rc = tc_request(tc, request, &response);
    ASSERT_TRUE(rc == 0 && response != NULL, label);
    ASSERT_TRUE(response && strstr(response, fragment) != NULL, label);
    free(response);
}

static void scalar_routes_lowlevel(void) {
    const char *specs[] = {"short", "int", "long", "timestamp", "byte",
                          "numeric:12,2", "float", "double"};
    TypedSchema *ts = calloc(1, sizeof(*ts));
    ASSERT_NOT_NULL(ts, "low-level scalar schema");
    if (!ts) return;
    ts->typed = 1; ts->nfields = 1; ts->nullmap_prefix = 2;
    for (size_t i = 0; i < sizeof(specs) / sizeof(specs[0]); i++) {
        parse_field_type(specs[i], &ts->fields[0]);
        strcpy(ts->fields[0].name, "x"); ts->fields[0].name_len = 1;
        ts->fields[0].offset = 0; ts->total_size = 2 + ts->fields[0].size;
        uint8_t expected[16] = {0}, actual[32] = {0};
        char err[256] = {0};
        int rc = encode_field_checked(&ts->fields[0], "1", 1, expected,
                                      err, sizeof(err));
        ASSERT_EQ_INT(rc, 0, "canonical scalar one encodes");
        rc = typed_encode(ts, "{\"x\":\"\\u0031\"}", actual, sizeof(actual),
                          err, sizeof(err));
        ASSERT_EQ_INT(rc, ts->total_size, "low-level encoder decodes escaped scalar");
        ASSERT_TRUE(rc == ts->total_size &&
                    memcmp(actual + ts->nullmap_prefix, expected,
                           (size_t)ts->fields[0].size) == 0,
                    "low-level decoded value equals canonical scalar");
        rc = typed_encode(ts, "{\"x\":\"1\\u0000junk\"}", actual, sizeof(actual),
                          err, sizeof(err));
        ASSERT_EQ_INT(rc, -2, "low-level scalar escaped NUL rejects");
    }
    free(ts);
}

static int test_scalar_routes_checked_run(void) {
    scalar_routes_lowlevel();
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = {.port = env.port, .io_timeout_ms = 60000};
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    scalar_route_expect(tc,
        "{\"mode\":\"add-dir\",\"dir\":\"scalar_routes\"}",
        "\"status\"", "add-dir");
    scalar_route_expect(tc,
        "{\"mode\":\"create-object\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"splits\":8,\"max_key\":64,\"fields\":[\"i:int\",\"n:numeric:12,2\",\"f:float\",\"d:double\"]}",
        "\"status\":\"created\"", "create");
    scalar_route_expect(tc,
        "{\"mode\":\"insert\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\",\"value\":{\"i\":\"\\u0031\",\"n\":\"\\u002d\\u0031.\\u0032\\u0035\",\"f\":\"\\u0031.2\\u0065\\u0032\",\"d\":\"\\u0031.25\"}}",
        "\"status\":\"inserted\"", "escaped insert");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\"}",
        "\"i\":1", "insert i decoded");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\"}",
        "\"n\":-1.25", "insert sign decoded");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\"}",
        "\"f\":120", "insert exponent decoded");
    scalar_route_expect(tc,
        "{\"mode\":\"add-index\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"field\":\"i\"}",
        "\"status\"", "i index");
    scalar_route_expect(tc,
        "{\"mode\":\"add-index\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"field\":\"n\"}",
        "\"status\"", "n index");
    scalar_route_expect(tc,
        "{\"mode\":\"add-index\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"field\":\"f\"}",
        "\"status\"", "f index");
    scalar_route_expect(tc,
        "{\"mode\":\"update\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\",\"value\":{\"i\":\"\\u0032\",\"n\":\"\\u0032.\\u0035\"}}",
        "\"status\"", "escaped update");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\"}",
        "\"i\":2", "updated scalar");
    scalar_route_expect(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"records\":[{\"key\":\"b\",\"value\":{\"i\":\"\\u0033\",\"n\":\"\\u0033.5\",\"f\":\"\\u0031e\\u0032\"}}]}",
        "\"inserted\":1", "escaped bulk insert");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"b\"}",
        "\"i\":3", "bulk insert decoded");
    scalar_route_expect(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"records\":[{\"key\":\"b\",\"value\":{\"i\":\"\\u0034\"}}]}",
        "\"updated\":1", "escaped bulk update");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"b\"}",
        "\"i\":4", "bulk update decoded");
    scalar_route_expect(tc,
        "{\"mode\":\"insert\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"nul\",\"value\":{\"i\":\"12\\u0000junk\"}}",
        "\"error\"", "escaped nul insert rejected");
    scalar_route_expect(tc,
        "{\"mode\":\"update\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\",\"value\":{\"i\":\"12\\u0000junk\"}}",
        "\"error\"", "escaped nul update rejected");
    scalar_route_expect(tc,
        "{\"mode\":\"get\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"a\"}",
        "\"i\":2", "nul rejection preserves old");
    scalar_route_expect(tc,
        "{\"mode\":\"insert\",\"dir\":\"scalar_routes\",\"object\":\"t\",\"key\":\"once\",\"value\":{\"i\":\"\\\\u0031\"}}",
        "\"error\"", "decode exactly once");
    struct { const char *field, *value; } checks[] = {
        {"i", "\\u0032"}, {"n", "\\u0032.5"}, {"f", "\\u0031.2e2"}
    };
    for (int j = 0; j < 3; j++) {
        char request[512], *response = NULL;
        snprintf(request, sizeof(request),
            "{\"mode\":\"count\",\"dir\":\"scalar_routes\",\"object\":\"t\","
            "\"criteria\":[{\"field\":\"%s\",\"op\":\"eq\",\"value\":\"%s\"}]}",
            checks[j].field, checks[j].value);
        int rc = tc_request(tc, request, &response);
        ASSERT_TRUE(rc == 0 && response != NULL, "escaped indexed criterion responds");
        /* Every criterion matches a only: b has i=4/n=3.5/f=100.
           The wire allows trailing JSON whitespace before its frame. */
        char *end = NULL;
        long count = response ? strtol(response, &end, 10) : -1;
        if (end) while (*end == ' ' || *end == '\t' || *end == '\r' || *end == '\n') end++;
        ASSERT_TRUE(response && end != response && count == 1 && *end == '\0',
                    "escaped indexed criterion finds exact rows");
        free(response);
    }
    tc_close(tc); test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-scalar-routes-checked", test_scalar_routes_checked_run)
