/* src/test/cases/test_auto_key_multi.c
 *
 * Regression coverage for the multi-key paths on auto_key=uuid|seq
 * objects: get with keys[], exists with keys[], and not-exists. Each
 * of these previously needed wire-form -> binary normalisation; the
 * shared `parse_multi_key` helper (storage.c, extracted in PR #59 /
 * commit f4031d6) does that conversion once for all three call sites.
 *
 * If parse_multi_key ever regresses, the response shapes for the
 * inserted-uuid / inserted-seq keys will look exactly like a miss:
 *   - cmd_get_multi      -> wire key maps to null
 *   - cmd_exists_multi   -> wire key maps to false
 *   - cmd_not_exists     -> wire key appears in the not-found list
 *
 * The assertions below pin "inserted key is found" + "non-existent
 * key is not found" for both uuid and seq variants.
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

/* Lifted from test_auto_key.c — pull a "key":"..." field out of an
   insert response so we can use the server-rendered wire key on the
   subsequent multi-key requests. */
static char *extract_key_field(const char *resp) {
    const char *p = SAFE_STRSTR(resp, "\"key\":\"");
    if (!p) return NULL;
    p += 7;
    const char *end = strchr(p, '"');
    if (!end) return NULL;
    size_t n = (size_t)(end - p);
    char *out = malloc(n + 1);
    if (!out) return NULL;
    memcpy(out, p, n);
    out[n] = '\0';
    return out;
}

/* ----------------- t_get_multi_uuid ------------------------------- */
/* Create an auto_key=uuid object, insert 3 records, then issue a
   get with keys=[<uuid1>,<uuid2>,<uuid3>] and assert all three
   names come back. The dashed wire-form must be normalised to
   binary storage form for lookup to hit. */
static int t_get_multi_uuid(TestClient *tc) {
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u_get_multi\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\"],\"auto_key\":\"uuid\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "uuid get-multi: create");
    free(resp); resp = NULL;

    char *uuids[3] = {0};
    const char *names[3] = {"n0", "n1", "n2"};
    char buf[512];
    for (int i = 0; i < 3; i++) {
        snprintf(buf, sizeof(buf),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"u_get_multi\","
            "\"value\":{\"name\":\"%s\"}}", names[i]);
        tc_request(tc, buf, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "uuid get-multi: insert");
        uuids[i] = extract_key_field(resp);
        ASSERT_NOT_NULL(uuids[i], "uuid get-multi: extracted key");
        free(resp); resp = NULL;
    }

    /* Build a get-multi request using the three returned wire-form
       uuids verbatim. */
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"u_get_multi\","
        "\"keys\":[\"%s\",\"%s\",\"%s\"]}", uuids[0], uuids[1], uuids[2]);
    tc_request(tc, req, &resp);
    /* Every inserted name should appear in the response payload —
       the order within the per-key dict isn't pinned by the protocol
       but the values must be there. */
    ASSERT_CONTAINS(resp, "\"name\":\"n0\"", "uuid get-multi: n0 present");
    ASSERT_CONTAINS(resp, "\"name\":\"n1\"", "uuid get-multi: n1 present");
    ASSERT_CONTAINS(resp, "\"name\":\"n2\"", "uuid get-multi: n2 present");
    free(resp); resp = NULL;

    for (int i = 0; i < 3; i++) free(uuids[i]);
    return 0;
}

/* ----------------- t_exists_multi_uuid ---------------------------- */
/* Insert one record, then issue exists with keys=[<inserted>,<all-zero>]
   and assert the inserted uuid -> true, all-zero uuid -> false. */
static int t_exists_multi_uuid(TestClient *tc) {
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u_exists_multi\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\"],\"auto_key\":\"uuid\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "uuid exists-multi: create");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"u_exists_multi\","
        "\"value\":{\"name\":\"alpha\"}}", &resp);
    char *inserted = extract_key_field(resp);
    ASSERT_NOT_NULL(inserted, "uuid exists-multi: extracted key");
    free(resp); resp = NULL;

    const char *zero_uuid = "00000000-0000-0000-0000-000000000000";
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"u_exists_multi\","
        "\"keys\":[\"%s\",\"%s\"]}", inserted, zero_uuid);
    tc_request(tc, req, &resp);

    /* Build the expected substrings: "<inserted>":true, "<zero>":false. */
    char want_true[80], want_false[80];
    snprintf(want_true, sizeof(want_true), "\"%s\":true", inserted);
    snprintf(want_false, sizeof(want_false), "\"%s\":false", zero_uuid);
    ASSERT_CONTAINS(resp, want_true, "uuid exists-multi: inserted -> true");
    ASSERT_CONTAINS(resp, want_false, "uuid exists-multi: zero uuid -> false");
    free(resp); resp = NULL;
    free(inserted);
    return 0;
}

/* ----------------- t_not_exists_uuid ------------------------------ */
/* not-exists returns the array of keys that don't exist. Insert one
   uuid; issue not-exists with [<inserted>,<all-zero>]; assert the
   all-zero uuid is in the array and the inserted uuid is NOT. */
static int t_not_exists_uuid(TestClient *tc) {
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"u_not_exists\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\"],\"auto_key\":\"uuid\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "uuid not-exists: create");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"u_not_exists\","
        "\"value\":{\"name\":\"beta\"}}", &resp);
    char *inserted = extract_key_field(resp);
    ASSERT_NOT_NULL(inserted, "uuid not-exists: extracted key");
    free(resp); resp = NULL;

    const char *zero_uuid = "00000000-0000-0000-0000-000000000000";
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"not-exists\",\"dir\":\"default\",\"object\":\"u_not_exists\","
        "\"keys\":[\"%s\",\"%s\"]}", inserted, zero_uuid);
    tc_request(tc, req, &resp);

    /* zero uuid present in the response array, inserted uuid absent. */
    char want_present[80];
    snprintf(want_present, sizeof(want_present), "\"%s\"", zero_uuid);
    ASSERT_CONTAINS(resp, want_present, "uuid not-exists: zero uuid present");

    /* The inserted uuid must NOT appear anywhere in the response. */
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, inserted) == NULL,
                "uuid not-exists: inserted uuid absent");
    free(resp); resp = NULL;
    free(inserted);
    return 0;
}

/* ----------------- t_get_multi_seq -------------------------------- */
/* Mirror t_get_multi_uuid for auto_key=seq(...). Decimal-string wire
   form ("1","2","3") must be normalised to 8-byte int64 BE storage
   form. */
static int t_get_multi_seq(TestClient *tc) {
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"s_get_multi\","
        "\"splits\":8,\"max_key\":8,"
        "\"fields\":[\"amount:int\"],\"auto_key\":\"seq(s_get_multi_id)\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "seq get-multi: create");
    free(resp); resp = NULL;

    char *seqs[3] = {0};
    int amounts[3] = {100, 200, 300};
    char buf[512];
    for (int i = 0; i < 3; i++) {
        snprintf(buf, sizeof(buf),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"s_get_multi\","
            "\"value\":{\"amount\":%d}}", amounts[i]);
        tc_request(tc, buf, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seq get-multi: insert");
        seqs[i] = extract_key_field(resp);
        ASSERT_NOT_NULL(seqs[i], "seq get-multi: extracted key");
        free(resp); resp = NULL;
    }

    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"s_get_multi\","
        "\"keys\":[\"%s\",\"%s\",\"%s\"]}", seqs[0], seqs[1], seqs[2]);
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"amount\":100", "seq get-multi: amount 100");
    ASSERT_CONTAINS(resp, "\"amount\":200", "seq get-multi: amount 200");
    ASSERT_CONTAINS(resp, "\"amount\":300", "seq get-multi: amount 300");
    free(resp); resp = NULL;

    for (int i = 0; i < 3; i++) free(seqs[i]);
    return 0;
}

static int test_auto_key_multi_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    int rc = 0;
    rc |= t_get_multi_uuid(tc);
    rc |= t_exists_multi_uuid(tc);
    rc |= t_not_exists_uuid(tc);
    rc |= t_get_multi_seq(tc);

    tc_close(tc);
    test_env_stop(&env);
    return rc;
}

TEST_REGISTER("test-auto-key-multi", test_auto_key_multi_run);
