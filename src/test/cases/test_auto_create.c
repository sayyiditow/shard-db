/* src/test/cases/test_auto_create.c
 * :auto_create stamps now() on first insert and preserves it on re-insert. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>

/* Extract the decimal value of "field":<digits> from a JSON response. */
static char *extract_field_num(const char *resp, const char *field) {
    if (!resp) return NULL;
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":", field);
    const char *p = SAFE_STRSTR(resp, pat);
    if (!p) return NULL;
    p += strlen(pat);
    const char *s = p;
    while (*p && (*p == '-' || (*p >= '0' && *p <= '9'))) p++;
    size_t n = (size_t)(p - s);
    if (n == 0) return NULL;
    char *out = malloc(n + 1);
    memcpy(out, s, n); out[n] = '\0';
    return out;
}

static int test_auto_create_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"ac\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"ac\",\"object\":\"t\",\"splits\":16,"
        "\"max_key\":16,\"fields\":[\"data:varchar:16\",\"created:timestamp:auto_create\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create obj"); free(resp); resp = NULL;

    /* First insert: created must be stamped (non-zero epoch ms). */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"k1\","
        "\"value\":{\"data\":\"a\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"k1\"}", &resp);
    char *c1 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(c1, "created present after insert");
    ASSERT_TRUE(c1 && strcmp(c1, "0") != 0, "created stamped non-zero on insert");
    free(resp); resp = NULL;

    /* Re-insert (upsert) same key with new data: created must be PRESERVED. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"k1\","
        "\"value\":{\"data\":\"b\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"data\":\"b\"", "data updated on re-insert");
    char *c2 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(c2, "created present after re-insert");
    ASSERT_TRUE(c1 && c2 && strcmp(c1, c2) == 0, "created preserved across re-insert");
    free(resp); resp = NULL;
    free(c1); free(c2);

    /* Explicit auto_create value in the JSON must be overwritten by the
       server stamp on a FRESH insert too — this is the case that guards
       against ever dropping the "redundant" re-stamp in the !existed
       branch (see review note in this plan's Task 3). */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"k2\","
        "\"value\":{\"data\":\"c\",\"created\":9999999}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"k2\"}", &resp);
    char *c3 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(c3, "created present on client-supplied insert");
    ASSERT_TRUE(c3 && strcmp(c3, "0") != 0, "client-supplied created overwritten (non-zero)");
    ASSERT_TRUE(c3 && strcmp(c3, "9999999") != 0, "client-supplied created value not preserved");
    free(resp); resp = NULL;
    free(c3);

    /* Bulk-insert (JSON) used as upsert: re-inserting an existing key via
       bulk-insert must preserve auto_create, exactly like single insert. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk1\","
        "\"value\":{\"data\":\"x\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk1\"}", &resp);
    char *bc1 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(bc1, "bulk: created present after initial insert");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"ac\",\"object\":\"t\","
        "\"records\":[{\"key\":\"bk1\",\"value\":{\"data\":\"y\"}}]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"data\":\"y\"", "bulk: data updated on bulk-insert upsert");
    char *bc2 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(bc2, "bulk: created present after bulk-insert upsert");
    ASSERT_TRUE(bc1 && bc2 && strcmp(bc1, bc2) == 0,
                "bulk: created preserved across bulk-insert upsert");
    free(resp); resp = NULL;

    /* Bulk-insert (JSON) fresh key with a client-supplied auto_create value:
       must be overwritten, same contract as single insert. */
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"ac\",\"object\":\"t\","
        "\"records\":[{\"key\":\"bk2\",\"value\":{\"data\":\"z\",\"created\":9999999}}]}",
        &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk2\"}", &resp);
    char *bc3 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(bc3, "bulk: created present on fresh bulk-insert");
    ASSERT_TRUE(bc3 && strcmp(bc3, "9999999") != 0,
                "bulk: client-supplied created overwritten on fresh bulk-insert");
    free(resp); resp = NULL;
    free(bc1); free(bc2); free(bc3);

    /* Bulk-insert-delimited (CSV) used as upsert: same preservation
       contract. Columns are key|data|created (created column is ignored —
       CSV bulk-insert never applied default_kind fields before this fix
       and still doesn't accept a client override for auto_create; the
       column here is a placeholder empty value). Field order in
       fields.conf is data, created — first CSV column after key is
       "data". */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk3\","
        "\"value\":{\"data\":\"p\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk3\"}", &resp);
    char *bc4 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(bc4, "csv: created present after initial insert");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"ac\",\"object\":\"t\","
        "\"delimiter\":\"|\",\"data\":\"bk3|q|\\n\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"ac\",\"object\":\"t\",\"key\":\"bk3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"data\":\"q\"", "csv: data updated on delimited upsert");
    char *bc5 = extract_field_num(resp, "created");
    ASSERT_NOT_NULL(bc5, "csv: created present after delimited upsert");
    ASSERT_TRUE(bc4 && bc5 && strcmp(bc4, bc5) == 0,
                "csv: created preserved across delimited upsert");
    free(resp); resp = NULL;
    free(bc4); free(bc5);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-auto-create", test_auto_create_run)
