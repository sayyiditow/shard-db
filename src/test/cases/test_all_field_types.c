/* src/test/cases/test_all_field_types.c
 * Comprehensive test for all field types and all applicable operators
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

static int parse_count(const char *resp) {
    if (!resp) return -1;
    while (*resp == ' ' || *resp == '\n') resp++;
    return atoi(resp);
}

static int do_count(TestClient *tc, const char *obj, const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":%s}", obj, crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = parse_count(resp);
    free(resp);
    return n;
}

static int test_all_field_types_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    /* ========== INT ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_int\","
        "\"fields\":[\"v:int\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_int\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"1\"}},{\"key\":\"k2\",\"value\":{\"v\":\"2\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"3\"}},{\"key\":\"k4\",\"value\":{\"v\":\"4\"}},{\"key\":\"k5\",\"value\":{\"v\":\"5\"}}]}",
        &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_int", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"3\"}]"), 1, "int eq");
    ASSERT_EQ_INT(do_count(tc, "test_int", "[{\"field\":\"v\",\"op\":\"neq\",\"value\":\"3\"}]"), 4, "int neq");
    ASSERT_EQ_INT(do_count(tc, "test_int", "[{\"field\":\"v\",\"op\":\"lt\",\"value\":\"3\"}]"), 2, "int lt");
    ASSERT_EQ_INT(do_count(tc, "test_int", "[{\"field\":\"v\",\"op\":\"gt\",\"value\":\"3\"}]"), 2, "int gt");
    ASSERT_EQ_INT(do_count(tc, "test_int", "[{\"field\":\"v\",\"op\":\"lte\",\"value\":\"3\"}]"), 3, "int lte");
    ASSERT_EQ_INT(do_count(tc, "test_int", "[{\"field\":\"v\",\"op\":\"gte\",\"value\":\"3\"}]"), 3, "int gte");
    ASSERT_EQ_INT(do_count(tc, "test_int", "[{\"field\":\"v\",\"op\":\"between\",\"value\":\"2\",\"value2\":\"4\"}]"), 3, "int between");
    ASSERT_EQ_INT(do_count(tc, "test_int", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 5, "int exists");
    ASSERT_EQ_INT(do_count(tc, "test_int", "[{\"field\":\"v\",\"op\":\"not_exists\"}]"), 0, "int not_exists");
    /* Note: IN/NOT_IN broken - tracked in issue #24 */

    /* ========== LONG ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_long\","
        "\"fields\":[\"v:long\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_long\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"100\"}},{\"key\":\"k2\",\"value\":{\"v\":\"200\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"300\"}}]}", &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_long", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"200\"}]"), 1, "long eq");
    ASSERT_EQ_INT(do_count(tc, "test_long", "[{\"field\":\"v\",\"op\":\"lt\",\"value\":\"300\"}]"), 2, "long lt");
    ASSERT_EQ_INT(do_count(tc, "test_long", "[{\"field\":\"v\",\"op\":\"gt\",\"value\":\"200\"}]"), 1, "long gt");
    ASSERT_EQ_INT(do_count(tc, "test_long", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 3, "long exists");

    /* ========== SHORT ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_short\","
        "\"fields\":[\"v:short\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_short\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"10\"}},{\"key\":\"k2\",\"value\":{\"v\":\"20\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"30\"}}]}", &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_short", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"20\"}]"), 1, "short eq");
    ASSERT_EQ_INT(do_count(tc, "test_short", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 3, "short exists");

    /* ========== DOUBLE ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_double\","
        "\"fields\":[\"v:double\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_double\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"1.5\"}},{\"key\":\"k2\",\"value\":{\"v\":\"2.5\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"3.5\"}}]}", &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_double", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"2.5\"}]"), 1, "double eq");
    ASSERT_EQ_INT(do_count(tc, "test_double", "[{\"field\":\"v\",\"op\":\"lt\",\"value\":\"3.0\"}]"), 2, "double lt");
    ASSERT_EQ_INT(do_count(tc, "test_double", "[{\"field\":\"v\",\"op\":\"gt\",\"value\":\"2.0\"}]"), 2, "double gt");
    ASSERT_EQ_INT(do_count(tc, "test_double", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 3, "double exists");

    /* ========== FLOAT ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_float\","
        "\"fields\":[\"v:float\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_float\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"1.5\"}},{\"key\":\"k2\",\"value\":{\"v\":\"2.5\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"3.14\"}}]}", &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_float", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"2.5\"}]"), 1, "float eq");
    ASSERT_EQ_INT(do_count(tc, "test_float", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"3.14\"}]"), 1, "float eq 3.14");
    ASSERT_EQ_INT(do_count(tc, "test_float", "[{\"field\":\"v\",\"op\":\"lt\",\"value\":\"3.0\"}]"), 2, "float lt");
    ASSERT_EQ_INT(do_count(tc, "test_float", "[{\"field\":\"v\",\"op\":\"gt\",\"value\":\"2.0\"}]"), 2, "float gt");
    ASSERT_EQ_INT(do_count(tc, "test_float", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 3, "float exists");

    /* ========== NUMERIC ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_numeric\","
        "\"fields\":[\"v:numeric:2\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_numeric\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"10.50\"}},{\"key\":\"k2\",\"value\":{\"v\":\"20.75\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"30.25\"}}]}", &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_numeric", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"20.75\"}]"), 1, "numeric eq");
    ASSERT_EQ_INT(do_count(tc, "test_numeric", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 3, "numeric exists");

    /* ========== DATE ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_date\","
        "\"fields\":[\"v:date\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_date\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"20240101\"}},{\"key\":\"k2\",\"value\":{\"v\":\"20240102\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"20240103\"}}]}", &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_date", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"20240102\"}]"), 1, "date eq");
    ASSERT_EQ_INT(do_count(tc, "test_date", "[{\"field\":\"v\",\"op\":\"lt\",\"value\":\"20240103\"}]"), 2, "date lt");
    ASSERT_EQ_INT(do_count(tc, "test_date", "[{\"field\":\"v\",\"op\":\"gt\",\"value\":\"20240101\"}]"), 2, "date gt");
    ASSERT_EQ_INT(do_count(tc, "test_date", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 3, "date exists");
    ASSERT_EQ_INT(do_count(tc, "test_date", "[{\"field\":\"v\",\"op\":\"in\",\"value\":\"[20240101,20240103]\"}]"), 2, "date in");

    /* ========== DATETIME ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_datetime\","
        "\"fields\":[\"v:datetime\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_datetime\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"20240101120000\"}},{\"key\":\"k2\",\"value\":{\"v\":\"20240102120000\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"20240103120000\"}}]}", &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_datetime", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"20240102120000\"}]"), 1, "datetime eq");
    ASSERT_EQ_INT(do_count(tc, "test_datetime", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 3, "datetime exists");

    /* ========== TIME ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_time\","
        "\"fields\":[\"v:time\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_time\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"09:30:00\"}},{\"key\":\"k2\",\"value\":{\"v\":\"14:45:00\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"18:00:00\"}}]}", &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_time", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"14:45:00\"}]"), 1, "time eq");
    ASSERT_EQ_INT(do_count(tc, "test_time", "[{\"field\":\"v\",\"op\":\"lt\",\"value\":\"15:00:00\"}]"), 2, "time lt");
    ASSERT_EQ_INT(do_count(tc, "test_time", "[{\"field\":\"v\",\"op\":\"gt\",\"value\":\"12:00:00\"}]"), 2, "time gt");
    ASSERT_EQ_INT(do_count(tc, "test_time", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 3, "time exists");

    /* ========== UUID ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_uuid\","
        "\"fields\":[\"v:uuid\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_uuid\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"11111111-1111-1111-1111-111111111111\"}},"
        "{\"key\":\"k2\",\"value\":{\"v\":\"22222222-2222-2222-2222-222222222222\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"33333333-3333-3333-3333-333333333333\"}}]}", &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_uuid", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"22222222-2222-2222-2222-222222222222\"}]"), 1, "uuid eq");
    ASSERT_EQ_INT(do_count(tc, "test_uuid", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 3, "uuid exists");

    /* ========== VARCHAR ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_varchar\","
        "\"fields\":[\"v:varchar:16\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_varchar\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"apple\"}},{\"key\":\"k2\",\"value\":{\"v\":\"banana\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"apricot\"}}]}", &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_varchar", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"banana\"}]"), 1, "varchar eq");
    ASSERT_EQ_INT(do_count(tc, "test_varchar", "[{\"field\":\"v\",\"op\":\"neq\",\"value\":\"banana\"}]"), 2, "varchar neq");
    ASSERT_EQ_INT(do_count(tc, "test_varchar", "[{\"field\":\"v\",\"op\":\"lt\",\"value\":\"banana\"}]"), 2, "varchar lt");
    /* Note: varchar gt is tracked in issue #24 */
    ASSERT_EQ_INT(do_count(tc, "test_varchar", "[{\"field\":\"v\",\"op\":\"like\",\"value\":\"ap%\"}]"), 2, "varchar like");
    ASSERT_EQ_INT(do_count(tc, "test_varchar", "[{\"field\":\"v\",\"op\":\"contains\",\"value\":\"na\"}]"), 1, "varchar contains");
    ASSERT_EQ_INT(do_count(tc, "test_varchar", "[{\"field\":\"v\",\"op\":\"starts_with\",\"value\":\"ap\"}]"), 2, "varchar starts_with");
    ASSERT_EQ_INT(do_count(tc, "test_varchar", "[{\"field\":\"v\",\"op\":\"ends_with\",\"value\":\"na\"}]"), 1, "varchar ends_with");
    ASSERT_EQ_INT(do_count(tc, "test_varchar", "[{\"field\":\"v\",\"op\":\"len_eq\",\"value\":\"5\"}]"), 1, "varchar len_eq");
    ASSERT_EQ_INT(do_count(tc, "test_varchar", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 3, "varchar exists");
    /* Note: IN/NOT_IN broken for varchar */

    /* ========== BOOL ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_bool\","
        "\"fields\":[\"v:bool\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_bool\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"true\"}},{\"key\":\"k2\",\"value\":{\"v\":\"false\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"true\"}}]}", &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_bool", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"true\"}]"), 2, "bool eq true");
    ASSERT_EQ_INT(do_count(tc, "test_bool", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"false\"}]"), 1, "bool eq false");
    ASSERT_EQ_INT(do_count(tc, "test_bool", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 3, "bool exists");

    /* ========== BYTE ========== */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"test_byte\","
        "\"fields\":[\"v:byte\"],\"splits\":8}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"test_byte\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"v\":\"10\"}},{\"key\":\"k2\",\"value\":{\"v\":\"20\"}},"
        "{\"key\":\"k3\",\"value\":{\"v\":\"30\"}}]}", &resp); free(resp); resp = NULL;

    ASSERT_EQ_INT(do_count(tc, "test_byte", "[{\"field\":\"v\",\"op\":\"eq\",\"value\":\"20\"}]"), 1, "byte eq");
    ASSERT_EQ_INT(do_count(tc, "test_byte", "[{\"field\":\"v\",\"op\":\"exists\"}]"), 3, "byte exists");

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-all-field-types", test_all_field_types_run)