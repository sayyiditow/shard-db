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

/* Smart reindex: editing one indexed field's encoding should only rebuild
   indexes that actually reference that field. Object has two indexes
   (age, name); editing age:int → age:long must rebuild exactly 1 and skip
   exactly 1. The skipped (name) index must still be functional. */
static int t_smart_reindex(TestClient *tc) {
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"polish\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"polish\",\"object\":\"smartidx\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"id:varchar:16\",\"age:int\",\"name:varchar:64\"],"
        "\"indexes\":[\"age\",\"name\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object smartidx");
    free(resp); resp = NULL;

    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"polish\",\"object\":\"smartidx\","
            "\"key\":\"k%d\",\"value\":{\"id\":\"k%d\",\"age\":%d,\"name\":\"n%d\"}}",
            i, i, 20 + i, i);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }

    /* Edit-field age:int → age:long. Encoding changes (4 → 8 bytes BE). */
    tc_request(tc,
        "{\"mode\":\"edit-field\",\"dir\":\"polish\",\"object\":\"smartidx\","
        "\"fields\":[\"age:long\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"",      "smart reindex edit-field status");
    ASSERT_CONTAINS(resp, "\"indexes_rebuilt\":1",      "indexes_rebuilt is 1 (age only)");
    ASSERT_CONTAINS(resp, "\"indexes_skipped\":1",      "indexes_skipped is 1 (name)");
    free(resp); resp = NULL;

    /* Functional check: indexed find by age (the rebuilt index). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"polish\",\"object\":\"smartidx\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"gte\",\"value\":22}]}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k2\"", "find age gte 22 returns k2");
    ASSERT_CONTAINS(resp, "\"key\":\"k3\"", "find age gte 22 returns k3");
    ASSERT_CONTAINS(resp, "\"key\":\"k4\"", "find age gte 22 returns k4");
    free(resp); resp = NULL;

    /* Functional check: indexed find by name (the skipped index — must still work). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"polish\",\"object\":\"smartidx\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"n2\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k2\"", "find name eq n2 still works (skipped index)");
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

/* dry_run: with `dry_run:true`, edit-field must run all validation
   (including the per-record pre-flight scan) but write nothing —
   neither fields.conf nor the data slot — and report
   `dry_run:true` + `would_rebuild:bool` in the response. */
static int t_dry_run(TestClient *tc) {
    char *resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"polish\",\"object\":\"dryrun\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"id:varchar:16\",\"age:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object dryrun");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"polish\",\"object\":\"dryrun\","
        "\"key\":\"k1\",\"value\":{\"id\":\"k1\",\"age\":7}}", &resp);
    free(resp); resp = NULL;

    /* Dry run an age:int → age:long widen. Encoding changes (4 → 8 bytes BE)
       so would_rebuild must be true. */
    tc_request(tc,
        "{\"mode\":\"edit-field\",\"dir\":\"polish\",\"object\":\"dryrun\","
        "\"fields\":[\"age:long\"],\"dry_run\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"dry_run\":true",       "dry_run flag echoed");
    ASSERT_CONTAINS(resp, "\"would_rebuild\":true", "would_rebuild reflects encoding change");
    free(resp); resp = NULL;

    /* describe-object must still report the old type — nothing was written. */
    tc_request(tc,
        "{\"mode\":\"describe-object\",\"dir\":\"polish\",\"object\":\"dryrun\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"type\":\"int\"",
                    "describe-object still shows age:int after dry_run");
    free(resp); resp = NULL;

    /* The data slot must still hold the original int-encoded value. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"polish\",\"object\":\"dryrun\",\"key\":\"k1\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"age\":7",
                    "get k1 still returns age=7 (record uncorrupted)");
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
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    t_default_carryover(tc);
    t_auto_create_carryover(tc);
    t_auto_update_carryover(tc);
    t_smart_reindex(tc);
    t_dry_run(tc);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-edit-field-polish", test_edit_field_polish_run)
