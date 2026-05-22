/* src/test/cases/test_edit_field_polish.c
 *
 * edit-field polish — default-modifier carryover.
 *
 * Today's bug: rewrite_fields_conf_for_edit replaces a field's full
 * fields.conf line with the user-supplied new spec, silently dropping any
 * existing `:default=…`, `:auto_create`, or `:auto_update` suffix when
 * the new spec omits it. Sub-test t_default_carryover pins the desired
 * behaviour: changing only a field's type (e.g. `age:int:default=42` →
 * `age:long`) must preserve the existing default.
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
#include <unistd.h>


static int t_default_carryover(TestClient *tc) {
    char *resp = NULL;

    /* 1. Create object with `id:varchar:16` and `age:int:default=42`. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u_polish\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"id:varchar:16\",\"age:int:default=42\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object u_polish");
    free(resp); resp = NULL;

    /* 2. Edit-field changes only the type, no default modifier. */
    tc_request(tc,
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"u_polish\","
        "\"fields\":[\"age:long\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "edit-field age:long status");
    free(resp); resp = NULL;

    /* 3. describe-object should still show the default AND the new type. */
    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"u_polish\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"type\":\"long\"",   "describe-object reports new type");
    ASSERT_CONTAINS(resp, "\"default\":\"42\"", "describe-object preserves default");
    free(resp); resp = NULL;

    return 0;
}

static int test_edit_field_polish_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    t_default_carryover(tc);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-edit-field-polish", test_edit_field_polish_run)
