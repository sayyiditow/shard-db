/* src/test/cases/test_float_field_type.c
 * Test FT_FLOAT field type - 4-byte IEEE 754 single precision
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

static int do_count(TestClient *tc, const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"floattest\","
        "\"criteria\":%s}", crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = parse_count(resp);
    free(resp);
    return n;
}

static int test_float_field_type_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"floattest\","
        "\"fields\":[\"id:varchar:16\",\"amount:float\",\"rating:float\"],"
        "\"indexes\":[\"amount\"],\"splits\":8}", &resp); free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"floattest\","
        "\"records\":["
        "{\"key\":\"r1\",\"value\":{\"id\":\"one\",\"amount\":\"1.5\",\"rating\":\"3.5\"}},"
        "{\"key\":\"r2\",\"value\":{\"id\":\"two\",\"amount\":\"2.0\",\"rating\":\"4.0\"}},"
        "{\"key\":\"r3\",\"value\":{\"id\":\"three\",\"amount\":\"3.14\",\"rating\":\"2.5\"}},"
        "{\"key\":\"r4\",\"value\":{\"id\":\"four\",\"amount\":\"4.5\",\"rating\":\"5.0\"}},"
        "{\"key\":\"r5\",\"value\":{\"id\":\"five\",\"amount\":\"5\",\"rating\":\"1.0\"}}"
        "]}", &resp); free(resp); resp = NULL;

    /* Indexed path tests - amount field */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"1.5\"}]"), 1, "eq 1.5 -> 1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"3.14\"}]"), 1, "eq 3.14 -> 1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"5.0\"}]"), 1, "eq 5.0 -> 1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"neq\",\"value\":\"3.14\"}]"), 4, "neq 3.14 -> 4");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"lt\",\"value\":\"3.0\"}]"), 2, "lt 3.0 -> 2");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"gt\",\"value\":\"3.0\"}]"), 3, "gt 3.0 -> 3");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"lte\",\"value\":\"3.14\"}]"), 3, "lte 3.14 -> 3");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"gte\",\"value\":\"3.14\"}]"), 3, "gte 3.14 -> 3");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"between\",\"value\":\"2.0\",\"value2\":\"4.5\"}]"), 3, "between 2.0-4.5 -> 3");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"in\",\"value\":\"[1.5,3.14,5.0]\"}]"), 3, "in [1.5,3.14,5.0] -> 3");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"not_in\",\"value\":\"[1.5,3.14,5.0]\"}]"), 2, "not_in [1.5,3.14,5.0] -> 2");

    /* Non-indexed path tests - rating field */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"rating\",\"op\":\"eq\",\"value\":\"4.0\"}]"), 1, "rating eq 4.0 -> 1");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"rating\",\"op\":\"neq\",\"value\":\"4.0\"}]"), 4, "rating neq 4.0 -> 4");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"rating\",\"op\":\"lt\",\"value\":\"3.0\"}]"), 2, "rating lt 3.0 -> 2");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"rating\",\"op\":\"gt\",\"value\":\"3.0\"}]"), 3, "rating gt 3.0 -> 3");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"rating\",\"op\":\"lte\",\"value\":\"3.5\"}]"), 3, "rating lte 3.5 -> 3");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"rating\",\"op\":\"gte\",\"value\":\"3.5\"}]"), 3, "rating gte 3.5 -> 3");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"rating\",\"op\":\"between\",\"value\":\"2.0\",\"value2\":\"4.0\"}]"), 3, "rating between 2.0-4.0 -> 3");

    /* EXISTS tests */
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"amount\",\"op\":\"exists\"}]"), 5, "amount exists -> 5");
    ASSERT_EQ_INT(do_count(tc, "[{\"field\":\"rating\",\"op\":\"exists\"}]"), 5, "rating exists -> 5");

    /* Find with float field - check decoded output format */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"floattest\","
        "\"criteria\":[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"3.14\"}],"
        "\"fields\":[\"id\",\"amount\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"id\":\"three\"", "find eq 3.14 has three");
    ASSERT_CONTAINS(resp, "\"amount\":\"3.14\"", "find eq 3.14 has amount 3.14");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-float-field-type", test_float_field_type_run)