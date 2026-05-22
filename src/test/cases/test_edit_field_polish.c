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

/* Carry-through for :auto_create. The varchar fallback for auto_create
   stores datetime-now, so `id:varchar:16:auto_create` is a legal spec
   (see generate_default's FT_VARCHAR branch). Widening 16 → 32 must
   preserve the :auto_create modifier in fields.conf. */
static int t_auto_create_carryover(TestClient *tc) {
    char *resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u_polish_ac\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"id:varchar:16:auto_create\",\"name:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object u_polish_ac");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"u_polish_ac\","
        "\"fields\":[\"id:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "edit-field id:varchar:32 status");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"u_polish_ac\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"type\":\"varchar\"",            "describe-object reports varchar");
    ASSERT_CONTAINS(resp, "\"default\":\"auto_create\"",     "describe-object preserves auto_create");
    free(resp); resp = NULL;

    return 0;
}

/* Carry-through for :auto_update. Same varchar fallback applies, so
   `updated_at:varchar:32:auto_update` is legal. Widen 32 → 64 and
   confirm :auto_update survives. */
static int t_auto_update_carryover(TestClient *tc) {
    char *resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u_polish_au\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"id:varchar:16\",\"updated_at:varchar:32:auto_update\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object u_polish_au");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"u_polish_au\","
        "\"fields\":[\"updated_at:varchar:64\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "edit-field updated_at:varchar:64 status");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"default\",\"object\":\"u_polish_au\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"type\":\"varchar\"",          "describe-object reports varchar");
    ASSERT_CONTAINS(resp, "\"default\":\"auto_update\"",   "describe-object preserves auto_update");
    free(resp); resp = NULL;

    return 0;
}

/* NOTE: the "new spec overrides old default" path is implemented inside
   default_modifiers_for_line / rewrite_fields_conf_for_edit (the `new_has`
   branch) but is currently unreachable end-to-end: parse_field_line —
   the validator cmd_edit_fields runs before the rewrite — does not strip
   :default=…/:auto_create/:auto_update from incoming specs, so a request
   like `{"fields":["age:long:default=99"]}` is rejected with "Invalid
   field line" before the rewrite ever runs. Teaching parse_field_line
   to mirror load_typed_schema's strip-then-parse pattern is a separate
   change (it also affects add-field) and is out of scope for this
   review-fix commit. Once that lands, a t_new_default_overrides sub-test
   should be added here. */

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
    t_auto_create_carryover(tc);
    t_auto_update_carryover(tc);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-edit-field-polish", test_edit_field_polish_run)
