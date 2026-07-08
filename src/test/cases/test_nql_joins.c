/* src/test/cases/test_nql_joins.c
 * Parser unit tests + integration tests for NQL --join flag.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "nql.h"
#include "query_internal.h" /* JoinSpec, JOIN_INNER, JOIN_LEFT */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Parser unit tests (no daemon) ──────────────────────────────── */

static int test_nql_join_parse_basic(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command(
        "find default orders 'status = paid' --join j_cust cust_id=key as cust", &cmd);
    ASSERT_TRUE(r == 0, "parse succeeds");
    ASSERT_TRUE(cmd.njoins == 1, "one join parsed");
    ASSERT_EQ_STR(cmd.joins[0].object, "j_cust", "object field");
    ASSERT_EQ_STR(cmd.joins[0].local_field, "cust_id", "local field");
    ASSERT_EQ_STR(cmd.joins[0].remote_field, "key", "remote field");
    ASSERT_EQ_STR(cmd.joins[0].as_name, "cust", "as field");
    ASSERT_EQ_INT(cmd.joins[0].type, JOIN_INNER, "type inner");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_join_parse_implicit_alias(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command(
        "find default orders --join j_cust cust_id=key", &cmd);
    ASSERT_TRUE(r == 0, "parse succeeds");
    ASSERT_TRUE(cmd.njoins == 1, "one join parsed");
    ASSERT_EQ_STR(cmd.joins[0].object, "j_cust", "object field");
    ASSERT_EQ_STR(cmd.joins[0].local_field, "cust_id", "local field");
    ASSERT_EQ_STR(cmd.joins[0].remote_field, "key", "remote field");
    ASSERT_EQ_STR(cmd.joins[0].as_name, "j_cust", "as defaults to object name");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_join_parse_fields(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command(
        "find default orders --join j_cust cust_id=key fields email,name", &cmd);
    ASSERT_TRUE(r == 0, "parse succeeds");
    ASSERT_TRUE(cmd.njoins == 1, "one join parsed");
    ASSERT_EQ_INT(cmd.joins[0].proj_count, 2, "two proj fields");
    ASSERT_EQ_STR(cmd.joins[0].proj_fields[0], "email", "first field");
    ASSERT_EQ_STR(cmd.joins[0].proj_fields[1], "name", "second field");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_join_parse_left(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command(
        "find default orders --join j_cust cust_id=key left", &cmd);
    ASSERT_TRUE(r == 0, "parse succeeds");
    ASSERT_TRUE(cmd.njoins == 1, "one join parsed");
    ASSERT_EQ_INT(cmd.joins[0].type, JOIN_LEFT, "type left");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_join_parse_multi(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command(
        "find default orders --join j_cust cust_id=key as cust"
        " --join j_orders cust_id=key as related left", &cmd);
    ASSERT_TRUE(r == 0, "parse succeeds");
    ASSERT_EQ_INT(cmd.njoins, 2, "two joins parsed");
    ASSERT_EQ_STR(cmd.joins[0].as_name, "cust", "first as cust");
    ASSERT_EQ_STR(cmd.joins[1].as_name, "related", "second as related");
    ASSERT_EQ_INT(cmd.joins[1].type, JOIN_LEFT, "type left on second");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_join_parse_bad_syntax(void) {
    NqlCommand cmd;
    memset(&cmd, 0, sizeof(cmd));
    int r = nql_parse_command(
        "find default orders --join j_cust cust_id", &cmd);
    ASSERT_TRUE(r < 0, "missing = → error");
    ASSERT_TRUE(cmd.err[0] != '\0', "error message set");
    nql_free_command(&cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ── Integration tests (daemon) ─────────────────────────────────── */

static void setup_join_fixture(TestClient *tc) {
    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"nj_cust\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\",\"city:varchar:32\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"nj_orders\","
        "\"splits\":16,\"max_key\":16,"
        "\"fields\":[\"amount:numeric:10,2\",\"status:varchar:20\","
                    "\"cust_id:varchar:16\"],"
        "\"indexes\":[\"status\",\"cust_id\"]}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"nj_cust\","
                   "\"key\":\"c1\",\"value\":{\"name\":\"Alice\",\"city\":\"NYC\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"nj_cust\","
                   "\"key\":\"c2\",\"value\":{\"name\":\"Bob\",\"city\":\"LA\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"nj_orders\",\"key\":\"o1\","
                   "\"value\":{\"amount\":100,\"status\":\"paid\",\"cust_id\":\"c1\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"nj_orders\",\"key\":\"o2\","
                   "\"value\":{\"amount\":250,\"status\":\"paid\",\"cust_id\":\"c1\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"nj_orders\",\"key\":\"o3\","
                   "\"value\":{\"amount\":75,\"status\":\"paid\",\"cust_id\":\"c2\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"nj_orders\",\"key\":\"o4\","
                   "\"value\":{\"amount\":40,\"status\":\"pending\",\"cust_id\":\"MISSING\"}}", &resp); free(resp); resp = NULL;
}

static int test_nql_join_inner(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    setup_join_fixture(tc);

    char *resp = NULL;
    tc_request(tc,
        "find default nj_orders 'status = paid'"
        " --join nj_cust cust_id=key as cust fields name,city", &resp);
    ASSERT_CONTAINS(resp, "\"cust.name\"", "inner: cust.name column present");
    ASSERT_CONTAINS(resp, "\"Alice\"", "inner: Alice joined");
    ASSERT_CONTAINS(resp, "\"Bob\"", "inner: Bob joined");
    ASSERT_TRUE(strstr(resp, "\"o4\"") == NULL, "inner: o4 filtered out");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_join_left(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    setup_join_fixture(tc);

    char *resp = NULL;
    tc_request(tc,
        "find default nj_orders"
        " --join nj_cust cust_id=key as cust fields name left", &resp);
    ASSERT_CONTAINS(resp, "\"o4\"", "left: o4 still appears");
    ASSERT_CONTAINS(resp, "null", "left: null for MISSING match");
    ASSERT_CONTAINS(resp, "\"Alice\"", "left: Alice joined");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_join_implicit_alias(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    setup_join_fixture(tc);

    char *resp = NULL;
    tc_request(tc,
        "find default nj_orders 'status = paid'"
        " --join nj_cust cust_id=key fields name", &resp);
    /* Without 'as', alias defaults to object name "nj_cust" */
    ASSERT_CONTAINS(resp, "\"nj_cust.name\"", "implicit alias uses object name");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_nql_join_fields(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    setup_join_fixture(tc);

    char *resp = NULL;
    tc_request(tc,
        "find default nj_orders 'status = paid'"
        " --join nj_cust cust_id=key as cust fields name", &resp);
    ASSERT_CONTAINS(resp, "\"cust.name\"", "fields: name present");
    /* city should NOT be in output since we only requested name */
    ASSERT_TRUE(strstr(resp, "\"cust.city\"") == NULL, "fields: city excluded");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ── Registration ───────────────────────────────────────────────── */

TEST_REGISTER("nql-join-parse-basic",     test_nql_join_parse_basic);
TEST_REGISTER("nql-join-parse-implicit",  test_nql_join_parse_implicit_alias);
TEST_REGISTER("nql-join-parse-fields",    test_nql_join_parse_fields);
TEST_REGISTER("nql-join-parse-left",      test_nql_join_parse_left);
TEST_REGISTER("nql-join-parse-multi",     test_nql_join_parse_multi);
TEST_REGISTER("nql-join-parse-bad",       test_nql_join_parse_bad_syntax);
TEST_REGISTER("nql-join-inner",           test_nql_join_inner);
TEST_REGISTER("nql-join-left",            test_nql_join_left);
TEST_REGISTER("nql-join-implicit-alias",  test_nql_join_implicit_alias);
TEST_REGISTER("nql-join-fields",          test_nql_join_fields);
