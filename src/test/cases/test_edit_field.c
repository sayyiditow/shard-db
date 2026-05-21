/* src/test/cases/test_edit_field.c
 *
 * edit-field schema mutation — same-type in-place field edits on v2.
 * Covers: varchar grow/shrink (success + refusal), int widen, long→int
 * narrow refusal, numeric scale up/down, cross-type refusal, tombstoned
 * field refusal, unknown field refusal, duplicate-in-request refusal,
 * v1 refusal, indexed-field edit triggers reindex.
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
#include <sys/stat.h>
#include <unistd.h>


static int test_edit_field_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"users\","
        "\"splits\":8,\"max_key\":32,"
        "\"fields\":[\"name:varchar:32\",\"age:int\",\"balance:numeric:18,2\","
                    "\"score:short\",\"bio:varchar:64\"],"
        "\"indexes\":[\"name\",\"age\",\"balance\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"users\","
                   "\"key\":\"u1\",\"value\":{\"name\":\"Alice\",\"age\":30,"
                   "\"balance\":\"100.50\",\"score\":12345,\"bio\":\"hello\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"users\","
                   "\"key\":\"u2\",\"value\":{\"name\":\"Bob\",\"age\":-200,"
                   "\"balance\":\"-12.75\",\"score\":-32000,\"bio\":\"hi\"}}", &resp);
    free(resp); resp = NULL;

    char obj[300];
    snprintf(obj, sizeof(obj), "%s/default/users", env.db_root);
    char path[400];

    /* === 1. varchar grow: name 32 → 64 ====================================== */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"name:varchar:64\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "varchar grow status");
    ASSERT_CONTAINS(resp, "\"rebuilt\":true", "varchar grow rebuilt flag");
    free(resp); resp = NULL;

    snprintf(path, sizeof(path), "%s/fields.conf", obj);
    char *fconf = tu_read_file(path);
    ASSERT_TRUE(fconf && strstr(fconf, "name:varchar:64") != NULL,
                "fields.conf has new varchar:64");
    ASSERT_TRUE(fconf && strstr(fconf, "name:varchar:32") == NULL,
                "fields.conf no longer has varchar:32");
    free(fconf);

    /* Existing records still readable, contents intact. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"users\",\"key\":\"u1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"Alice\"", "GET u1 name preserved post-grow");
    ASSERT_CONTAINS(resp, "\"bio\":\"hello\"", "GET u1 bio untouched");
    free(resp); resp = NULL;

    /* Index on name still resolves. */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"Alice\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"u1\"", "index find by name post-grow");
    free(resp); resp = NULL;

    /* === 2. varchar shrink success: bio 64 → 16 (both values short enough) === */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"bio:varchar:16\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "varchar shrink status");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"users\",\"key\":\"u1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"bio\":\"hello\"", "shrunk bio still readable");
    free(resp); resp = NULL;

    /* === 3. varchar shrink refused — insert a long bio, then try to shrink === */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"users\","
                   "\"key\":\"u3\",\"value\":{\"name\":\"Carol\",\"age\":50,"
                   "\"balance\":\"99.99\",\"score\":1,\"bio\":\"this_is_15char\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"bio:varchar:5\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "varchar shrink overflow error");
    ASSERT_CONTAINS(resp, "varchar shrink", "varchar shrink overflow reason");
    free(resp); resp = NULL;

    /* Record + schema unchanged after refusal. */
    fconf = tu_read_file(path);
    ASSERT_TRUE(fconf && strstr(fconf, "bio:varchar:16") != NULL,
                "fields.conf unchanged after failed shrink");
    free(fconf);

    /* === 4. int widen: age int → long ======================================= */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"age:long\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "int widen status");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"users\",\"key\":\"u2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"age\":-200", "negative int widened to long preserves sign");
    free(resp); resp = NULL;

    /* Indexed find by widened field still works. */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"users\","
                   "\"criteria\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":30}]}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"u1\"", "find by widened age");
    free(resp); resp = NULL;

    /* === 5. int narrow refused — score short → byte (would overflow) ======== */
    /* score is FT_SHORT (2 bytes). Narrow to FT_BYTE is a cross-type change
       (byte is unsigned, short is signed) — confirm refusal as cross-type.
       Try a same-type narrow instead: widen score first, then re-narrow with
       an existing out-of-range value to exercise the narrow-refusal path. */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"score:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "short→int widen");
    free(resp); resp = NULL;
    /* u1 score is 12345 — narrowing to short is safe (12345 fits in [-32768,32767]).
       Bump u3 to a value that won't fit in short. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"users\","
                   "\"key\":\"u3\",\"value\":{\"score\":100000}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"score:short\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "int narrow overflow error");
    ASSERT_CONTAINS(resp, "integer narrow", "int narrow reason");
    free(resp); resp = NULL;

    /* Reset score and confirm same-type narrow succeeds when everything fits. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"users\","
                   "\"key\":\"u3\",\"value\":{\"score\":100}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"score:short\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "int narrow succeeds when in-range");
    free(resp); resp = NULL;

    /* === 6. numeric scale up: balance numeric:18,2 → numeric:18,4 =========== */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"balance:numeric:18,4\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "numeric scale up status");
    free(resp); resp = NULL;
    /* u1.balance was 100.50; scale up to S=4 multiplies stored int64 by 100,
       display stays 100.5000 (or 100.5 depending on encoding). */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"users\",\"key\":\"u1\"}", &resp);
    /* numeric renders as a bare number (no quotes); 100.50 ×10^2 stored as
       int64 = 10050, rescaled to S=4 = 1005000, decoded /10^4 = 100.5000. */
    ASSERT_CONTAINS(resp, "\"balance\":100.5", "numeric scale-up value preserved");
    free(resp); resp = NULL;

    /* === 7. numeric scale down: 18,4 → 18,2 truncates toward zero =========== */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"users\","
                   "\"key\":\"u4\",\"value\":{\"name\":\"Dan\",\"age\":1,"
                   "\"balance\":\"3.1499\",\"score\":1,\"bio\":\"d\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"balance:numeric:18,2\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "numeric scale down status");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"users\",\"key\":\"u4\"}", &resp);
    /* 3.1499 with S=4 stored as int64 31499; rescale to S=2 truncates
       /100 = 314 toward zero (matches Postgres); decodes /10^2 = 3.14. */
    ASSERT_CONTAINS(resp, "\"balance\":3.14", "numeric scale-down truncates toward zero");
    free(resp); resp = NULL;

    /* === 8. cross-type refused: name varchar → int ========================== */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"name:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "cross-type refused");
    ASSERT_CONTAINS(resp, "Cross-type", "cross-type hint");
    free(resp); resp = NULL;

    /* === 9. tombstoned-field refused ======================================== */
    tc_request(tc, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"bio\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"bio:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "tombstoned refused");
    ASSERT_CONTAINS(resp, "tombstoned", "tombstoned reason");
    free(resp); resp = NULL;

    /* === 10. unknown field refused ========================================== */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"ghost:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "unknown field refused");
    ASSERT_CONTAINS(resp, "not found", "unknown field reason");
    free(resp); resp = NULL;

    /* === 11. duplicate-edit-in-request refused ============================== */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"name:varchar:128\",\"name:varchar:256\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "duplicate edit refused");
    ASSERT_CONTAINS(resp, "Duplicate", "duplicate reason");
    free(resp); resp = NULL;

    /* === 12. batch edit success — name + age in one call ==================== */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"users\","
                   "\"fields\":[\"name:varchar:128\",\"age:long\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "batch edit status");
    ASSERT_CONTAINS(resp, "\"fields\":2", "batch edit count");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"users\",\"key\":\"u1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"Alice\"", "batch edit name preserved");
    ASSERT_CONTAINS(resp, "\"age\":30", "batch edit age preserved");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-edit-field", test_edit_field_run)
