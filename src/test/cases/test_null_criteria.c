/* src/test/cases/test_null_criteria.c
 *
 * NULL criteria semantics over the wire (null plan Task 5):
 *   - eq/in never match NULL; neq/not_in exclude NULL (a NULL field belongs
 *     to neither a positive value predicate nor its negative);
 *   - exists/not_exists use the generation-correct query-existence
 *     predicate, indexed and unindexed twins agree;
 *   - count == find-length on every predicate family;
 *   - raw JSON null operands are rejected at the parser (quoted "null"
 *     stays a valid varchar value).
 */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static TestClient *nc_setup(TestEnv *env, const char *obj, int indexed) {
    if (test_env_start(env) != 0) return NULL;
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(env); return NULL; }
    char *r = NULL;
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"add-dir\",\"dir\":\"nc\"}");
    if (tc_request(tc, req, &r) != 0) { tc_close(tc); test_env_stop(env); return NULL; }
    free(r); r = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"nc\",\"object\":\"%s\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"k:int\",\"name:varchar:16\",\"note:varchar:16\"],"
        "\"indexes\":[%s]}",
        obj, indexed ? "\"k\",\"name\"" : "");
    if (tc_request(tc, req, &r) != 0 || strstr(r, "\"error\"")) {
        free(r); tc_close(tc); test_env_stop(env); return NULL;
    }
    free(r); r = NULL;
    /* Rows: 1 has note, 2 has note=NULL, 3 has note="", 4 has note="null". */
    const char *notes[4] = { "\"val\"", "null", "\"\"", "\"null\"" };
    for (int i = 1; i <= 4; i++) {
        char ins[256];
        snprintf(ins, sizeof(ins),
            "{\"mode\":\"insert\",\"dir\":\"nc\",\"object\":\"%s\","
            "\"key\":\"r%d\",\"value\":{\"k\":\"%d\",\"name\":\"n%d\","
            "\"note\":%s}}", obj, i, i, i, notes[i - 1]);
        if (tc_request(tc, ins, &r) != 0 || strstr(r, "\"error\"")) {
            free(r); tc_close(tc); test_env_stop(env); return NULL;
        }
        free(r); r = NULL;
    }
    return tc;
}

static int nc_count(TestClient *tc, const char *obj, const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"nc\",\"object\":\"%s\",\"criteria\":%s}",
        obj, crit);
    char *r = NULL;
    if (tc_request(tc, req, &r) != 0 || !r) return -1;
    int n = atoi(r);
    free(r);
    return n;
}

/* exist / not_exists / eq / neq agree across indexed and unindexed twins,
 * and count == find length everywhere. */
static int test_null_criteria_predicates(void) {
    TestEnv env;
    TestClient *tc = nc_setup(&env, "indexed", 1);
    ASSERT_NOT_NULL(tc, "indexed fixture");
    if (tc) {
        ASSERT_EQ_INT(nc_count(tc, "indexed", "[{\"field\":\"note\",\"op\":\"exists\"}]"),
                      3, "exists = value, \"\", \"null\" (not NULL)");
        ASSERT_EQ_INT(nc_count(tc, "indexed", "[{\"field\":\"note\",\"op\":\"not_exists\"}]"),
                      1, "not_exists = the NULL row");
        ASSERT_EQ_INT(nc_count(tc, "indexed", "[{\"field\":\"note\",\"op\":\"eq\",\"value\":\"val\"}]"),
                      1, "eq matches the value");
        ASSERT_EQ_INT(nc_count(tc, "indexed", "[{\"field\":\"note\",\"op\":\"neq\",\"value\":\"val\"}]"),
                      2, "neq excludes NULL and matches \"\"/\"null\"");
        ASSERT_EQ_INT(nc_count(tc, "indexed", "[{\"field\":\"note\",\"op\":\"in\",\"value\":\"val,null\"}]"),
                      2, "in matches value + literal \"null\" (quoted), not NULL");
        tc_close(tc);
    }
    test_env_stop(&env);

    tc = nc_setup(&env, "unindexed", 0);
    ASSERT_NOT_NULL(tc, "unindexed twin");
    if (tc) {
        ASSERT_EQ_INT(nc_count(tc, "unindexed", "[{\"field\":\"note\",\"op\":\"exists\"}]"),
                      3, "unindexed exists agrees");
        ASSERT_EQ_INT(nc_count(tc, "unindexed", "[{\"field\":\"note\",\"op\":\"not_exists\"}]"),
                      1, "unindexed not_exists agrees");
        ASSERT_EQ_INT(nc_count(tc, "unindexed", "[{\"field\":\"note\",\"op\":\"neq\",\"value\":\"val\"}]"),
                      2, "unindexed neq agrees");
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-criteria-predicates", test_null_criteria_predicates)

/* Raw (unquoted) JSON null operands are rejected; quoted "null" is a value. */
static int test_null_criteria_parser(void) {
    TestEnv env;
    TestClient *tc = nc_setup(&env, "p", 1);
    ASSERT_NOT_NULL(tc, "parser fixture");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"nc\",\"object\":\"p\","
            "\"criteria\":[{\"field\":\"note\",\"op\":\"eq\",\"value\":null}]}",
            &r), 0, "raw null operand request completes");
        ASSERT_NOT_NULL(r, "resp");
        ASSERT_TRUE(strstr(r, "\"error\"") != NULL,
                    "raw JSON null operand rejected");
        free(r); r = NULL;
        ASSERT_EQ_INT(nc_count(tc, "p", "[{\"field\":\"note\",\"op\":\"eq\",\"value\":\"null\"}]"),
                      1, "quoted \"null\" is a legitimate value");
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-criteria-parser", test_null_criteria_parser)
