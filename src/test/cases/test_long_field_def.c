/* src/test/cases/test_long_field_def.c
 *
 * Field definitions larger than the legacy 510-byte create gate / 255-byte
 * loader type buffer: enum value lists that previously failed at create or,
 * worse, created fine and corrupted the schema on reload.
 *
 * Base-branch failure points (documented, per CORE-PROCESS):
 *   - 637-byte enum create → rejected ("invalid field definition").
 *   - 337-byte enum create succeeds on base, but after daemon restart the
 *     field is silently dropped by load_typed_schema's 255-byte type_spec
 *     truncation → get/describe assertions fail.
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
#include <stdio.h>

/* "name:enum(0000,0001,...)" — nvalues values of vlen digits, ','-joined.
   125×4 → 637 bytes total; 65×4 → 337 bytes total (>255 type portion,
   <511 — passes the legacy create gate, corrupts the legacy loader).
   Precondition: nvalues ≤ 10^vlen — %0*d is a MINIMUM width, values with
   more digits print longer and would overflow the sizing budget. */
static char *enum_spec(const char *name, int nvalues, int vlen) {
    ASSERT_TRUE(nvalues > 0, "enum spec: nvalues > 0");
    ASSERT_TRUE((int)strlen(name) + 6 + (size_t)nvalues * ((size_t)vlen + 1) + 16 < 131072,
                "enum spec: sized request fits req[]");
    int pow10 = 1;
    for (int i = 0; i < vlen; i++) pow10 *= 10;
    ASSERT_TRUE(nvalues <= pow10, "enum spec: nvalues fits vlen digits");
    size_t cap = strlen(name) + (size_t)nvalues * ((size_t)vlen + 1) + 16;
    char *s = malloc(cap);
    ASSERT_NOT_NULL(s, "malloc enum spec");
    int off = snprintf(s, cap, "%s:enum(", name);
    for (int i = 0; i < nvalues && off >= 0; i++)
        off += snprintf(s + off, cap - (size_t)off, "%s%0*d",
                        i ? "," : "", vlen, i);
    if (off >= 0) snprintf(s + off, cap - (size_t)off, ")");
    return s;
}

static int test_long_field_def_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    char req[131072];

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"lfd\"}", &resp);
    free(resp); resp = NULL;

    /* === A: 637-byte enum (125 four-char values) — rejected on base at G2 === */
    char *spec_a = enum_spec("status", 125, 4);
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"lfd\",\"object\":\"a\","
             "\"splits\":8,\"max_key\":16,"
             "\"fields\":[\"%s\",\"name:varchar:32\"],"
             "\"indexes\":[\"status:bitmap\"]}", spec_a);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "A: long enum created");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"lfd\",\"object\":\"a\","
                   "\"key\":\"r1\",\"value\":{\"status\":\"0007\",\"name\":\"x\"}}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "A: insert r1");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"lfd\",\"object\":\"a\",\"key\":\"r1\"}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"0007\"", "A: get r1 pre-restart");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"lfd\",\"object\":\"a\","
                   "\"criteria\":{\"status\":\"0007\"}}", &resp);
    ASSERT_CONTAINS(resp, "0007", "A: indexed find pre-restart");
    free(resp); resp = NULL;
    free(spec_a);

    /* === B: 337-byte enum — created on base, dropped on base reload (G4) === */
    char *spec_b = enum_spec("state", 65, 4);
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"lfd\",\"object\":\"b\","
             "\"splits\":8,\"max_key\":16,"
             "\"fields\":[\"%s\",\"n:varchar:8\"]}", spec_b);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "B: mid-size enum created");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"lfd\",\"object\":\"b\","
                   "\"key\":\"q1\",\"value\":{\"state\":\"0042\",\"n\":\"y\"}}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "B: insert q1");
    free(resp); resp = NULL;
    free(spec_b);

    /* === E: over-cap spec → explicit error (not silently dropped) === */
    /* The ≈84 KB request relies on the daemon default
       MAX_REQUEST_SIZE=32 MB (db_defaults_set, embedded.c) — the fixture
       db.env intentionally doesn't set the knob, and config.c rejects
       out-of-range values, so the MAX_LINE fallback is unreachable. */
    /* 13,999 five-char values ≈ 84 KB — past the 65,535 cap, and every
       value stays exactly vlen digits (see enum_spec precondition). */
    char *spec_big = enum_spec("huge", 13999, 5);
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"lfd\",\"object\":\"c\","
             "\"splits\":8,\"max_key\":16,\"fields\":[\"%s\"]}", spec_big);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "E: over-cap spec rejected");
    ASSERT_CONTAINS(resp, "longer than", "E: error names the cap");
    free(resp); resp = NULL;
    free(spec_big);

    /* === F: :default= literals beyond TypedField.default_val's 255-byte
       capacity are rejected at every wire gate instead of being accepted
       and silently truncated (review round 3) === */
    char big_lit[301];
    memset(big_lit, 'x', sizeof(big_lit) - 1);
    big_lit[sizeof(big_lit) - 1] = '\0';

    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"lfd\",\"object\":\"f\","
             "\"splits\":8,\"max_key\":16,"
             "\"fields\":[\"v:varchar:400:default=%s\"]}", big_lit);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "F: create rejects >255-byte default");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
             "{\"mode\":\"add-field\",\"dir\":\"lfd\",\"object\":\"b\","
             "\"fields\":[\"v2:varchar:400:default=%s\"]}", big_lit);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "F: add-field rejects >255-byte default");
    free(resp); resp = NULL;

    snprintf(req, sizeof(req),
             "{\"mode\":\"edit-field\",\"dir\":\"lfd\",\"object\":\"b\","
             "\"fields\":[\"n:varchar:16:default=%s\"]}", big_lit);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "F: edit-field rejects >255-byte default");
    free(resp); resp = NULL;

    /* === G: an on-disk default too large to carry (older binary or hand
       edit) must not be silently dropped when its own field is edited
       without restating the default — the rewrite refuses wholesale === */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"lfd\",\"object\":\"g\","
                   "\"splits\":8,\"max_key\":16,"
                   "\"fields\":[\"v:varchar:32:default=short\",\"n:varchar:8\"]}",
               &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "G: object g created");
    free(resp); resp = NULL;

    char gpath[512];
    snprintf(gpath, sizeof(gpath), "%s/lfd/g/fields.conf", env.db_root);
    char *conf = tu_read_file(gpath);
    ASSERT_NOT_NULL(conf, "G: read fields.conf");
    if (conf) {
        const char *dflt = strstr(conf, "default=short");
        ASSERT_NOT_NULL(dflt, "G: default anchor present");
        if (dflt) {
            size_t head = (size_t)(dflt - conf) + strlen("default=");
            size_t tail_at = head + strlen("short");
            FILE *wf = fopen(gpath, "w");
            ASSERT_NOT_NULL(wf, "G: rewrite fields.conf on disk");
            if (wf) {
                fwrite(conf, 1, head, wf);
                fputs(big_lit, wf);
                fputc('\n', wf);
                fwrite(conf + tail_at, 1, strlen(conf) - tail_at, wf);
                fclose(wf);
            }
        }
        free(conf);
    }

    /* Editing v itself (long default not restated) must refuse, not drop. */
    tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"lfd\",\"object\":\"g\","
                   "\"fields\":[\"v:varchar:64\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "G: rewrite refuses to drop uncarriable default");
    free(resp); resp = NULL;

    char *after = tu_read_file(gpath);
    ASSERT_NOT_NULL(after, "G: re-read fields.conf");
    ASSERT_TRUE(after && strstr(after, big_lit) != NULL,
                "G: long default intact after refused edit");
    free(after); after = NULL;

    /* === restart #1: the loader must re-read what the writer wrote === */
    char saved_root[256]; int saved_port = env.port;
    snprintf(saved_root, sizeof(saved_root), "%s", env.db_root);
    tc_close(tc); tc = NULL;
    test_env_stop_keep(&env);

    TestEnv env2 = {0};
    ASSERT_EQ_INT(test_env_start_at(&env2, saved_root, saved_port), 0,
                  "daemon restart 1");
    TestClient *tc2 = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc2, "reconnect 1");
    if (!tc2) { test_env_stop(&env2); return 1; }
    {
        tc_request(tc2, "{\"mode\":\"get\",\"dir\":\"lfd\",\"object\":\"a\",\"key\":\"r1\"}",
                   &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"0007\"", "A: get r1 post-restart");
        free(resp); resp = NULL;

        tc_request(tc2, "{\"mode\":\"find\",\"dir\":\"lfd\",\"object\":\"a\","
                        "\"criteria\":{\"status\":\"0007\"}}", &resp);
        ASSERT_CONTAINS(resp, "0007", "A: indexed find post-restart");
        free(resp); resp = NULL;

        tc_request(tc2, "{\"mode\":\"describe-object\",\"dir\":\"lfd\",\"object\":\"a\"}",
                   &resp);
        ASSERT_CONTAINS(resp, "\"type\":\"enum\"", "A: describe type=enum post-restart");
        ASSERT_CONTAINS(resp, "\"status:bitmap\"", "A: describe shows the declared index");
        free(resp); resp = NULL;

        /* describe-object renders type tokens, not value lists — prove the
           FULL list parsed by encoding the LAST declared value. */
        tc_request(tc2, "{\"mode\":\"insert\",\"dir\":\"lfd\",\"object\":\"a\","
                        "\"key\":\"r2\",\"value\":{\"status\":\"0124\",\"name\":\"y\"}}",
                   &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "A: LAST enum value accepted post-restart");
        free(resp); resp = NULL;
        tc_request(tc2, "{\"mode\":\"get\",\"dir\":\"lfd\",\"object\":\"a\",\"key\":\"r2\"}",
                   &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"0124\"", "A: get r2 (last enum value) post-restart");
        free(resp); resp = NULL;

        tc_request(tc2, "{\"mode\":\"get\",\"dir\":\"lfd\",\"object\":\"b\",\"key\":\"q1\"}",
                   &resp);
        ASSERT_CONTAINS(resp, "\"state\":\"0042\"", "B: get q1 post-restart (not dropped)");
        free(resp); resp = NULL;
    }

    /* === C: add-field onto an object whose EXISTING line exceeds the old
       511-byte staging buffer — rebuild_object_v2's fields.conf staging
       rewrite must pass the 637-byte status line through byte-for-byte
       and append the 313-byte tag line. === */
    char *spec_tag = enum_spec("tag", 60, 4);   /* 313 bytes total */
    snprintf(req, sizeof(req),
             "{\"mode\":\"add-field\",\"dir\":\"lfd\",\"object\":\"a\","
             "\"fields\":[\"%s\"]}", spec_tag);
    tc_request(tc2, req, &resp);
    ASSERT_NOT_NULL(resp, "C: add-field responded");
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "C: add-field long enum succeeds");
    free(resp); resp = NULL;
    free(spec_tag);

    tc_request(tc2, "{\"mode\":\"insert\",\"dir\":\"lfd\",\"object\":\"a\","
                    "\"key\":\"r3\",\"value\":{\"status\":\"0007\",\"name\":\"z\","
                    "\"tag\":\"0005\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "C: insert uses the added field");
    free(resp); resp = NULL;

    /* === D: edit-field rewrite must pass a long sibling line through (G6) === */
    tc_request(tc2, "{\"mode\":\"edit-field\",\"dir\":\"lfd\",\"object\":\"a\","
                    "\"fields\":[\"name:varchar:64\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "D: edit-field sibling rewrite ok");
    free(resp); resp = NULL;

    tc_close(tc2); tc2 = NULL;
    test_env_stop_keep(&env2);

    /* === restart #2: add-field + edit-field rewrites must have been faithful === */
    TestEnv env3 = {0};
    ASSERT_EQ_INT(test_env_start_at(&env3, saved_root, saved_port), 0,
                  "daemon restart 2");
    TestClient *tc3 = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc3, "reconnect 2");
    if (!tc3) { test_env_stop(&env3); return 1; }
    {
        tc_request(tc3, "{\"mode\":\"get\",\"dir\":\"lfd\",\"object\":\"a\",\"key\":\"r3\"}",
                   &resp);
        ASSERT_CONTAINS(resp, "\"tag\":\"0005\"", "C: added enum survived restart");
        ASSERT_CONTAINS(resp, "\"status\":\"0007\"",
                        "C: A's 637-byte line survived the staging rewrite");
        free(resp); resp = NULL;

        tc_request(tc3, "{\"mode\":\"get\",\"dir\":\"lfd\",\"object\":\"a\",\"key\":\"r1\"}",
                   &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"0007\"", "D: A's long enum survived the edit rewrite");
        free(resp); resp = NULL;

        tc_close(tc3);
    }
    test_env_stop(&env3);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-long-field-def", test_long_field_def_run)
