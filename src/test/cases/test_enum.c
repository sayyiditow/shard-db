/* src/test/cases/test_enum.c
 *
 * FT_ENUM field type — declared value list, byte-index encoding,
 * bitmap auto-promote, edit-field append/rename/widen semantics.
 *
 * Spec: [[bitmap-impl-map]] sibling. Lands as part of 2026.05.7 alongside
 * trigram. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int test_enum_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"e\"}", &resp);
    ASSERT_CONTAINS(resp, "\"dir\":\"e\"", "add-dir");
    free(resp); resp = NULL;

    /* === Schema declaration === */

    /* Happy path: bool-shape enum with 3 values. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"e\",\"object\":\"a\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"color:enum(red,green,blue)\",\"name:varchar:32\"],"
        "\"indexes\":[\"color:bitmap\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create with enum");
    free(resp); resp = NULL;

    /* describe-object surfaces the enum type token + the declared bitmap. */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"e\",\"object\":\"a\"}", &resp);
    ASSERT_CONTAINS(resp, "\"type\":\"enum\"",  "describe: type=enum");
    ASSERT_CONTAINS(resp, "\"color:bitmap\"",   "describe: explicit bitmap on color");
    free(resp); resp = NULL;

    /* Empty value list → error. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"e\",\"object\":\"empty_enum\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"c:enum()\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "empty enum() rejected");
    free(resp); resp = NULL;

    /* Duplicate values → error. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"e\",\"object\":\"dup_enum\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"c:enum(red,red,blue)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "duplicate enum value rejected");
    free(resp); resp = NULL;

    /* Missing close paren → error. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"e\",\"object\":\"badparens\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"c:enum(red,green\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "missing ) rejected");
    free(resp); resp = NULL;

    /* Default value NOT in the declared list → rejected at create-object. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"e\",\"object\":\"baddefault\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"c:enum(red,green,blue):default=purple\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"",      "default-not-in-list: error");
    ASSERT_CONTAINS(resp, "default value",  "default-not-in-list: explanatory message");
    free(resp); resp = NULL;

    /* Default value IN the list → accepted. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"e\",\"object\":\"gooddefault\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"c:enum(red,green,blue):default=red\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "default-in-list: created");
    free(resp); resp = NULL;

    /* === CRUD round-trip === */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"r1\","
        "\"value\":{\"color\":\"red\",\"name\":\"Apple\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert red");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"r2\","
        "\"value\":{\"color\":\"green\",\"name\":\"Lime\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert green");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"r3\","
        "\"value\":{\"color\":\"blue\",\"name\":\"Sky\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert blue");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"r4\","
        "\"value\":{\"color\":\"red\",\"name\":\"Cherry\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert red 2nd");
    free(resp); resp = NULL;

    /* get round-trip: value comes back as a quoted JSON string. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"r1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"color\":\"red\"", "get r1: red");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"r3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"color\":\"blue\"", "get r3: blue");
    free(resp); resp = NULL;

    /* === Bitmap operator coverage === */

    /* count eq */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"e\",\"object\":\"a\","
        "\"criteria\":[{\"field\":\"color\",\"op\":\"eq\",\"value\":\"red\"}]}", &resp);
    ASSERT_CONTAINS(resp, "2", "count color=red → 2");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"e\",\"object\":\"a\","
        "\"criteria\":[{\"field\":\"color\",\"op\":\"eq\",\"value\":\"green\"}]}", &resp);
    ASSERT_CONTAINS(resp, "1", "count color=green → 1");
    free(resp); resp = NULL;

    /* count neq via subtraction shortcut */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"e\",\"object\":\"a\","
        "\"criteria\":[{\"field\":\"color\",\"op\":\"neq\",\"value\":\"red\"}]}", &resp);
    ASSERT_CONTAINS(resp, "2", "count color!=red → 2");
    free(resp); resp = NULL;

    /* count IN */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"e\",\"object\":\"a\","
        "\"criteria\":[{\"field\":\"color\",\"op\":\"in\",\"value\":[\"red\",\"blue\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "3", "count color in [red,blue] → 3");
    free(resp); resp = NULL;

    /* count NOT_IN via subtraction */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"e\",\"object\":\"a\","
        "\"criteria\":[{\"field\":\"color\",\"op\":\"not_in\",\"value\":[\"red\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "2", "count color not_in [red] → 2");
    free(resp); resp = NULL;

    /* find eq */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"e\",\"object\":\"a\","
        "\"criteria\":[{\"field\":\"color\",\"op\":\"eq\",\"value\":\"red\"}],"
        "\"limit\":100}", &resp);
    ASSERT_CONTAINS(resp, "\"r1\"", "find red: r1 present");
    ASSERT_CONTAINS(resp, "\"r4\"", "find red: r4 present");
    free(resp); resp = NULL;

    /* find IN */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"e\",\"object\":\"a\","
        "\"criteria\":[{\"field\":\"color\",\"op\":\"in\",\"value\":[\"green\",\"blue\"]}],"
        "\"limit\":100}", &resp);
    ASSERT_CONTAINS(resp, "\"r2\"", "find in [green,blue]: r2 present");
    ASSERT_CONTAINS(resp, "\"r3\"", "find in [green,blue]: r3 present");
    free(resp); resp = NULL;

    /* === Unknown value handling on QUERY criteria: encode emits a
       sentinel; the bitmap lookup misses cleanly. count returns 0. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"e\",\"object\":\"a\","
        "\"criteria\":[{\"field\":\"color\",\"op\":\"eq\",\"value\":\"purple\"}]}", &resp);
    ASSERT_CONTAINS(resp, "0", "count unknown value=purple → 0");
    free(resp); resp = NULL;

    /* === Strict insert-time rejection of unknown enum values === */

    /* Single insert with an unknown value → error, no record inserted. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"bad1\","
        "\"value\":{\"color\":\"purple\",\"name\":\"X\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"",         "single insert unknown: error");
    ASSERT_CONTAINS(resp, "unknown enum value", "single insert unknown: actionable message");
    ASSERT_CONTAINS(resp, "legal:",            "single insert unknown: legal list");
    free(resp); resp = NULL;

    /* Bulk insert with mixed valid + invalid: valid ones go in, invalid
       counted in `errors`. Best-effort batch semantics. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"e\",\"object\":\"a\","
        "\"records\":["
          "{\"key\":\"b1\",\"value\":{\"color\":\"red\",\"name\":\"A\"}},"
          "{\"key\":\"b2\",\"value\":{\"color\":\"purple\",\"name\":\"B\"}},"
          "{\"key\":\"b3\",\"value\":{\"color\":\"blue\",\"name\":\"C\"}}"
        "]}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":2", "bulk: 2 valid records inserted");
    ASSERT_CONTAINS(resp, "\"errors\":1",   "bulk: 1 record rejected for bad enum");
    free(resp); resp = NULL;

    /* Verify the valid bulk records landed (b1 = red, b3 = blue). */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"b1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"color\":\"red\"", "bulk valid b1: red");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"b3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"color\":\"blue\"", "bulk valid b3: blue");
    free(resp); resp = NULL;

    /* And the rejected record is NOT in the store. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"b2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "bulk rejected b2: not found");
    free(resp); resp = NULL;

    /* === edit-field: append (no flag needed) === */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"e\",\"object\":\"a\","
        "\"fields\":[\"color:enum(red,green,blue,yellow)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "append yellow: ok");
    ASSERT_CONTAINS(resp, "\"rebuilt\":false",    "append: no rebuild");
    free(resp); resp = NULL;

    /* Insert with the appended value. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"e\",\"object\":\"a\","
        "\"key\":\"r5\",\"value\":{\"color\":\"yellow\",\"name\":\"Sun\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert yellow after append");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"r5\"}", &resp);
    ASSERT_CONTAINS(resp, "\"color\":\"yellow\"", "get r5: yellow");
    free(resp); resp = NULL;

    /* === edit-field: remove rejected === */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"e\",\"object\":\"a\","
        "\"fields\":[\"color:enum(red,green,blue)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"",              "remove: error");
    ASSERT_CONTAINS(resp, "cannot remove",          "remove: error message");
    free(resp); resp = NULL;

    /* === edit-field: rename without flag → rejected === */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"e\",\"object\":\"a\","
        "\"fields\":[\"color:enum(crimson,green,blue,yellow)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"",             "rename no flag: error");
    ASSERT_CONTAINS(resp, "allow_rename",          "rename no flag: hint");
    free(resp); resp = NULL;

    /* === edit-field: rename WITH flag → success === */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"e\",\"object\":\"a\","
        "\"fields\":[\"color:enum(crimson,green,blue,yellow)\"],"
        "\"allow_rename\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "rename with flag: ok");
    free(resp); resp = NULL;

    /* Existing record's display value now reflects the rename. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"r1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"color\":\"crimson\"", "post-rename r1: crimson");
    free(resp); resp = NULL;

    /* === Persistence across daemon restart === */
    {
        char saved_root[256]; int saved_port = env.port;
        snprintf(saved_root, sizeof(saved_root), "%s", env.db_root);
        tc_close(tc); tc = NULL;
        test_env_stop_keep(&env);

        TestEnv env2 = {0};
        ASSERT_EQ_INT(test_env_start_at(&env2, saved_root, saved_port), 0,
                      "daemon restart");
        TestClient *tc2 = tc_connect(&cfg);
        ASSERT_NOT_NULL(tc2, "reconnect");
        if (tc2) {
            tc_request(tc2, "{\"mode\":\"describe-object\",\"dir\":\"e\",\"object\":\"a\"}", &resp);
            ASSERT_CONTAINS(resp, "\"type\":\"enum\"",  "post-restart: enum type");
            ASSERT_CONTAINS(resp, "\"color:bitmap\"",   "post-restart: bitmap preserved");
            free(resp); resp = NULL;

            tc_request(tc2, "{\"mode\":\"get\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"r1\"}", &resp);
            ASSERT_CONTAINS(resp, "\"color\":\"crimson\"",
                            "post-restart: rename persisted (r1 = crimson)");
            free(resp); resp = NULL;

            tc_request(tc2, "{\"mode\":\"get\",\"dir\":\"e\",\"object\":\"a\",\"key\":\"r5\"}", &resp);
            ASSERT_CONTAINS(resp, "\"color\":\"yellow\"",
                            "post-restart: appended value persisted");
            free(resp); resp = NULL;

            tc_close(tc2);
        }
        test_env_stop(&env2);
    }

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-enum", test_enum_run)
