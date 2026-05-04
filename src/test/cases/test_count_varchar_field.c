/* src/test/cases/test_count_varchar_field.c
 * count(*) vs count(varchar field) semantics: typed records always carry
 * every field, but a varchar field can have empty content (elen == 0).
 * count(varchar_field) must skip those, matching OP_EXISTS-on-varchar
 * semantics. count(*) and count(non-varchar field) should always equal
 * the live record count.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>

static int extract_n(const char *resp, char *out, size_t out_sz) {
    const char *p = strstr(resp, "\"n\":");
    if (!p) return 0;
    p += 4;
    while (*p == ' ' || *p == '"') p++;
    size_t i = 0;
    while (i + 1 < out_sz && *p && *p != ',' && *p != '}' && *p != '"' && *p != ' ' && *p != '\n') {
        out[i++] = *p++;
    }
    out[i] = '\0';
    return i > 0;
}

static int test_count_varchar_field_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;

    /* nick is varchar (can be empty), age is int (always present). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cv\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"nick:varchar:32\",\"age:int\"]}", &resp);
    free(resp); resp = NULL;

    /* 5 records: 3 with non-empty nick, 2 with empty nick. All have age. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cv\",\"key\":\"k1\",\"value\":{\"nick\":\"alice\",\"age\":30}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cv\",\"key\":\"k2\",\"value\":{\"nick\":\"bob\",\"age\":40}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cv\",\"key\":\"k3\",\"value\":{\"nick\":\"carol\",\"age\":50}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cv\",\"key\":\"k4\",\"value\":{\"nick\":\"\",\"age\":60}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cv\",\"key\":\"k5\",\"value\":{\"nick\":\"\",\"age\":70}}", &resp); free(resp); resp = NULL;

    char buf[32];

    /* count(*) — should be 5 (every record). */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"cv\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}", &resp);
    ASSERT_TRUE(extract_n(resp, buf, sizeof(buf)) && strcmp(buf, "5") == 0,
                "count(*) = 5");
    free(resp); resp = NULL;

    /* count(age) — non-varchar, every record has age, should be 5. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"cv\","
        "\"aggregates\":[{\"fn\":\"count\",\"field\":\"age\",\"alias\":\"n\"}]}", &resp);
    ASSERT_TRUE(extract_n(resp, buf, sizeof(buf)) && strcmp(buf, "5") == 0,
                "count(age) = 5 (int always present)");
    free(resp); resp = NULL;

    /* count(nick) — varchar, only 3 non-empty records should count. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"cv\","
        "\"aggregates\":[{\"fn\":\"count\",\"field\":\"nick\",\"alias\":\"n\"}]}", &resp);
    ASSERT_TRUE(extract_n(resp, buf, sizeof(buf)) && strcmp(buf, "3") == 0,
                "count(nick) = 3 (varchar skips empties)");
    free(resp); resp = NULL;

    /* count(nick) with criteria age>=50 — k3, k4, k5 match by age; only k3
       has non-empty nick, so count(nick) = 1 (whereas count(*) = 3). */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"cv\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":\"50\"}],"
        "\"aggregates\":[{\"fn\":\"count\",\"field\":\"nick\",\"alias\":\"n\"}]}", &resp);
    ASSERT_TRUE(extract_n(resp, buf, sizeof(buf)) && strcmp(buf, "1") == 0,
                "count(nick where age>=50) = 1 (k4/k5 have empty nicks)");
    free(resp); resp = NULL;

    /* count(nick) grouped by age — expect 5 groups, count is 1 for the
       3 non-empty rows and 0 for the 2 empty ones. Spot-check via the
       sum of n across groups by re-asking globally. */
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"cv\","
        "\"group_by\":[\"age\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"field\":\"nick\",\"alias\":\"n\"}]}", &resp);
    ASSERT_NOT_NULL(resp, "grouped count returned");
    /* Should see at least one bucket where n is 0 (the empty-nick rows). */
    ASSERT_CONTAINS(resp, "\"n\":0", "grouped count(nick): empty-nick group has n=0");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-count-varchar-field", test_count_varchar_field_run)
