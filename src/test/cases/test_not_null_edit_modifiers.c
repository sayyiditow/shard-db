/* src/test/cases/test_not_null_edit_modifiers.c
 *
 * :not_null modifier — edit-field resolution
 * carry-on-omission, canonical emission (exactly one default modifier +
 * at most one trailing nullability token), explicit :nullable removal,
 * unrelated lines untouched.
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

static int test_not_null_edit_modifiers_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ed\","
        "\"splits\":8,\"max_key\":32,"
        "\"fields\":[\"n:int:not_null\",\"m:varchar:20:not_null\","
                    "\"p:long\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create ed");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ed\","
                   "\"key\":\"k1\",\"value\":{\"n\":1,\"m\":\"x\",\"p\":5}}",
               &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ed\","
                   "\"key\":\"k2\",\"value\":{\"n\":2,\"m\":\"y\",\"p\":6}}",
               &resp);
    free(resp); resp = NULL;

    char path[512];
    snprintf(path, sizeof(path), "%s/default/ed/fields.conf", env.db_root);

    /* 1. carry-through-resize: int→long widen, token omitted. */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"ed\",\"fields\":[\"n:long\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "resize accepted");
    free(resp); resp = NULL;
    char *fc = tu_read_file(path);
    ASSERT_TRUE(fc && strstr(fc, "n:long:not_null"),
                "token carried through resize");
    free(fc);
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\","
                   "\"object\":\"ed\"}", &resp);
    ASSERT_CONTAINS(resp, "\"not_null\":true", "describe still renders flag");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"ed\","
                   "\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"n\":1", "data intact post-transform");
    free(resp); resp = NULL;

    /* 2+3. default and nullability resolve independently; emission is
            canonical — one default, one trailing token; and the default
            literal parses clean (not "zz:not_null"). */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"ed\","
                   "\"fields\":[\"m:varchar:40:default=zz:not_null\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "default+token accepted");
    free(resp); resp = NULL;
    fc = tu_read_file(path);
    ASSERT_TRUE(fc && strstr(fc, "m:varchar:40:default=zz:not_null"),
                "canonical line written");
    ASSERT_TRUE(fc && !strstr(fc, "not_null:default="),
                "no second default appended");
    free(fc);
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\","
                   "\"object\":\"ed\"}", &resp);
    ASSERT_CONTAINS(resp, "\"default\":\"zz\"", "default literal clean");
    ASSERT_TRUE(SAFE_STRSTR(resp, "zz:not_null") == NULL,
                "token not swallowed into the literal");
    free(resp); resp = NULL;

    /* swapping the default keeps the constraint, replaces the literal */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"ed\","
                   "\"fields\":[\"m:varchar:40:default=yy:not_null\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "default swap accepted");
    free(resp); resp = NULL;
    fc = tu_read_file(path);
    ASSERT_TRUE(fc && strstr(fc, "m:varchar:40:default=yy:not_null"),
                "swapped default + token");
    ASSERT_TRUE(fc && !strstr(fc, "default=zz"), "old default replaced");
    free(fc);

    /* 4. removal via :nullable — directive stripped, constraint gone,
          carried default survives. */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"ed\",\"fields\":[\"m:varchar:40:nullable\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "removal accepted");
    free(resp); resp = NULL;
    fc = tu_read_file(path);
    ASSERT_TRUE(fc && strstr(fc, "m:varchar:40:default=yy\n"),
                "removal writes base + carried default");
    ASSERT_TRUE(fc && !strstr(fc, "m:varchar:40:default=yy:not_null"),
                "constraint gone");
    ASSERT_TRUE(fc && !strstr(fc, ":nullable"),
                "directive never persisted");
    free(fc);
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\","
                   "\"object\":\"ed\"}", &resp);
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"name\":\"m\",\"type\":\"varchar\",\"size\":40,"
                             "\"default\":\"yy\",\"not_null\"") == NULL,
                "describe renders no flag for m");
    free(resp); resp = NULL;

    /* 5. explicit null on the now-unconstrained field is legal.
          (Before Task 3 this passes vacuously — no enforcement exists
          yet; it remains as the regression proving removal lifted the
          constraint once Task 3 lands.) */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ed\","
                   "\"key\":\"k3\",\"value\":{\"n\":3,\"m\":null,\"p\":7}}",
               &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "null insert after removal");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"ed\","
                   "\"key\":\"k3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"m\":null", "null rendered");
    free(resp); resp = NULL;

    /* 6. unrelated-field edit leaves other lines byte-intact, and a
          resize on a default+token field carries BOTH. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ed2\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"q:int:default=5:not_null\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create ed2");
    free(resp); resp = NULL;
    char path2[512];
    snprintf(path2, sizeof(path2), "%s/default/ed2/fields.conf", env.db_root);
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"ed2\",\"fields\":[\"q:long\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "ed2 resize accepted");
    free(resp); resp = NULL;
    fc = tu_read_file(path2);
    ASSERT_TRUE(fc && strstr(fc, "q:long:default=5:not_null"),
                "default AND token carried through resize");
    ASSERT_TRUE(fc && !strstr(fc, "not_null:default="),
                "canonical order preserved");
    free(fc);
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"default\","
                   "\"object\":\"ed2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"not_null\":true", "ed2 flag rendered");
    ASSERT_CONTAINS(resp, "\"default\":\"5\"", "ed2 default intact");
    free(resp); resp = NULL;

    /* 7. p (never constrained) edits freely; n and m lines untouched. */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"default\","
                   "\"object\":\"ed\",\"fields\":[\"p:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "p edit accepted");
    free(resp); resp = NULL;
    fc = tu_read_file(path);
    ASSERT_TRUE(fc && strstr(fc, "n:long:not_null"),
                "n untouched by p's edit");
    ASSERT_TRUE(fc && strstr(fc, "m:varchar:40:default=yy\n"),
                "m untouched by p's edit");
    ASSERT_TRUE(fc && strstr(fc, "p:int\n"), "p rewritten");
    free(fc);

    /* 8. Exact regressions from review: add only the constraint while
       carrying the old default; then replace default and constraint
       together. Each canonical line must appear exactly once. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ed3\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"x:int:default=5\"]}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""), "create ed3");
    free(resp); resp = NULL;
    char path3[512];
    snprintf(path3, sizeof(path3), "%s/default/ed3/fields.conf", env.db_root);
    tc_request(tc,
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ed3\","
        "\"fields\":[\"x:int:not_null\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "add flag carries default");
    free(resp); resp = NULL;
    fc = tu_read_file(path3);
    ASSERT_EQ_STR(fc, "x:int:default=5:not_null\n", "one default, one trailing flag");
    free(fc);
    tc_request(tc,
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ed3\","
        "\"fields\":[\"x:int:default=9:not_null\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "replace default and flag");
    free(resp); resp = NULL;
    fc = tu_read_file(path3);
    ASSERT_EQ_STR(fc, "x:int:default=9:not_null\n", "no duplicate modifier emission");
    free(fc);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-not-null-edit-modifiers", test_not_null_edit_modifiers_run)
