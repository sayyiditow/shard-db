/* src/test/cases/test_add_field_computed_defaults.c
 *
 * Computed defaults on add-field (and a smoke test for edit-field changing
 * a default). Closes the parser gap: pre-PR, add-field rejected every
 * :default=... spec because cmd_add_fields called parse_field_type directly
 * without stripping the default-modifier suffix. Also exercises the
 * backfill: each default kind must stamp the new field's bytes on every
 * existing record during the rebuild walk.
 *
 *   t_literal              :default=<literal> applied to every existing record
 *   t_seq                  :default=seq(<name>) assigns sequential ints
 *   t_uuid                 :default=uuid() assigns a fresh UUID per record
 *   t_random               :default=random(N) assigns N random bytes per record
 *   t_edit_field_change_default  edit-field changes a default for NEW inserts
 *   t_random_too_big       :default=random(N) refused if N > field size
 *
 * Auto-create / auto-update defaults intentionally NOT covered: they are
 * insert/update-time generators and stay inert during the add-field
 * rebuild walk. Documented as such in docs/query-protocol/schema-mutations.md.
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

/* Helper: count occurrences of `needle` in `hay`. */
static int count_occurrences(const char *hay, const char *needle) {
    if (!hay || !needle || !*needle) return 0;
    int n = 0;
    const char *p = hay;
    size_t nl = strlen(needle);
    while ((p = strstr(p, needle)) != NULL) { n++; p += nl; }
    return n;
}

/* Helper: extract a small substring after the literal `key`. Caller frees.
   Stops at the closing `"` for string values, or at ',' / '}' / '\0' for
   numeric. Returns NULL if key not found. */
static char *extract_str_value(const char *json, const char *key) {
    if (!json) return NULL;
    const char *p = strstr(json, key);
    if (!p) return NULL;
    p += strlen(key);
    if (*p == '"') {
        p++;
        const char *e = strchr(p, '"');
        if (!e) return NULL;
        size_t n = (size_t)(e - p);
        char *out = (char *)malloc(n + 1);
        memcpy(out, p, n); out[n] = '\0';
        return out;
    }
    const char *e = p;
    while (*e && *e != ',' && *e != '}' && *e != ' ' && *e != '\n') e++;
    size_t n = (size_t)(e - p);
    char *out = (char *)malloc(n + 1);
    memcpy(out, p, n); out[n] = '\0';
    return out;
}

static void t_literal(TestClient *tc) {
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"lit\","
        "\"splits\":8,\"max_key\":32,\"fields\":[\"name:varchar:32\"]}", &resp);
    free(resp); resp = NULL;
    for (int i = 1; i <= 3; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"lit\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"n%d\"}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    tc_request(tc,
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"lit\","
        "\"fields\":[\"flag:int:default=7\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "add-field int :default=7 accepted");
    free(resp); resp = NULL;
    for (int i = 1; i <= 3; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"lit\",\"key\":\"k%d\"}", i);
        tc_request(tc, req, &resp);
        char desc[64];
        snprintf(desc, sizeof(desc), "k%d backfilled flag=7", i);
        ASSERT_CONTAINS(resp, "\"flag\":7", desc);
        free(resp); resp = NULL;
    }
}

static void t_seq(TestClient *tc) {
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"sq\","
        "\"splits\":8,\"max_key\":32,\"fields\":[\"name:varchar:32\"]}", &resp);
    free(resp); resp = NULL;
    for (int i = 1; i <= 4; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sq\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"n%d\"}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    tc_request(tc,
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"sq\","
        "\"fields\":[\"sid:long:default=seq(rowid)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "add-field :default=seq() accepted");
    free(resp); resp = NULL;
    int seen[8] = {0};
    int total_seen = 0;
    for (int i = 1; i <= 4; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sq\",\"key\":\"k%d\"}", i);
        tc_request(tc, req, &resp);
        char *val = extract_str_value(resp, "\"sid\":");
        int v = val ? atoi(val) : -1;
        free(val);
        free(resp); resp = NULL;
        char desc[64];
        snprintf(desc, sizeof(desc), "k%d sid in [1,4]", i);
        ASSERT_TRUE(v >= 1 && v <= 4, desc);
        if (v >= 1 && v <= 4 && !seen[v]) { seen[v] = 1; total_seen++; }
    }
    ASSERT_EQ_INT(total_seen, 4, "all 4 seq values distinct");
}

static void t_uuid(TestClient *tc) {
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"uu\","
        "\"splits\":8,\"max_key\":32,\"fields\":[\"name:varchar:32\"]}", &resp);
    free(resp); resp = NULL;
    for (int i = 1; i <= 4; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"uu\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"n%d\"}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    tc_request(tc,
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"uu\","
        "\"fields\":[\"uid:varchar:36:default=uuid()\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "add-field :default=uuid() accepted");
    free(resp); resp = NULL;
    char *uuids[4] = {0};
    for (int i = 1; i <= 4; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"uu\",\"key\":\"k%d\"}", i);
        tc_request(tc, req, &resp);
        char *u = extract_str_value(resp, "\"uid\":");
        char desc[64];
        snprintf(desc, sizeof(desc), "k%d uuid extracted", i);
        ASSERT_NOT_NULL(u, desc);
        if (u) {
            size_t ul = strlen(u);
            snprintf(desc, sizeof(desc), "k%d uuid length 36", i);
            ASSERT_EQ_INT((int)ul, 36, desc);
            snprintf(desc, sizeof(desc), "k%d uuid has dashes", i);
            ASSERT_TRUE(ul == 36 && u[8] == '-' && u[13] == '-' && u[18] == '-' && u[23] == '-', desc);
        }
        uuids[i - 1] = u;
        free(resp); resp = NULL;
    }
    /* All 4 distinct. */
    int distinct = 1;
    for (int i = 0; i < 4 && distinct; i++)
        for (int j = i + 1; j < 4 && distinct; j++)
            if (uuids[i] && uuids[j] && strcmp(uuids[i], uuids[j]) == 0) distinct = 0;
    ASSERT_TRUE(distinct, "all 4 uuids distinct");
    for (int i = 0; i < 4; i++) free(uuids[i]);
}

static void t_random(TestClient *tc) {
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rr\","
        "\"splits\":8,\"max_key\":32,\"fields\":[\"name:varchar:32\"]}", &resp);
    free(resp); resp = NULL;
    const int N = 32;
    for (int i = 1; i <= N; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rr\","
            "\"key\":\"k%d\",\"value\":{\"name\":\"n%d\"}}", i, i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }
    /* Field is varchar:18 to hold 16 chars of hex (random(8) → 16 hex chars). */
    tc_request(tc,
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"rr\","
        "\"fields\":[\"rnd:varchar:18:default=random(8)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"rebuilt\"", "add-field :default=random(8) accepted");
    free(resp); resp = NULL;
    char *rnds[64] = {0};
    int collected = 0;
    for (int i = 1; i <= N; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rr\",\"key\":\"k%d\"}", i);
        tc_request(tc, req, &resp);
        char *r = extract_str_value(resp, "\"rnd\":");
        if (r) rnds[collected++] = r;
        free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(collected, N, "all 32 records have rnd field");
    /* All distinct (collision probability ~ 32^2 / 2^64 ≈ 0). */
    int distinct = 1;
    for (int i = 0; i < collected && distinct; i++)
        for (int j = i + 1; j < collected && distinct; j++)
            if (rnds[i] && rnds[j] && strcmp(rnds[i], rnds[j]) == 0) distinct = 0;
    ASSERT_TRUE(distinct, "all 32 random values distinct");
    /* Every value has length > 0 (i.e. backfill actually wrote bytes). */
    int all_nonempty = 1;
    for (int i = 0; i < collected; i++)
        if (!rnds[i] || rnds[i][0] == '\0') { all_nonempty = 0; break; }
    ASSERT_TRUE(all_nonempty, "all 32 random values non-empty");
    for (int i = 0; i < collected; i++) free(rnds[i]);
    (void)count_occurrences;
}

static void t_edit_field_change_default(TestClient *tc) {
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ed\","
        "\"splits\":8,\"max_key\":32,"
        "\"fields\":[\"name:varchar:32\",\"flag:int:default=7\"]}", &resp);
    free(resp); resp = NULL;
    /* Insert with flag omitted → DK_LITERAL applies → flag=7. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ed\","
        "\"key\":\"r1\",\"value\":{\"name\":\"alice\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"ed\",\"key\":\"r1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"flag\":7", "initial default literal applies at insert");
    free(resp); resp = NULL;

    /* edit-field to change the default. Same type — no rebuild expected for
       data, but fields.conf gets the new spec, so future inserts use the new
       default. */
    tc_request(tc,
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ed\","
        "\"fields\":[\"flag:int:default=99\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"edited\"", "edit-field :default=99 accepted");
    free(resp); resp = NULL;

    /* New insert without flag → flag=99. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ed\","
        "\"key\":\"r2\",\"value\":{\"name\":\"bob\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"ed\",\"key\":\"r2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"flag\":99", "new default applies to new insert");
    free(resp); resp = NULL;

    /* Existing record's stored value unchanged. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"ed\",\"key\":\"r1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"flag\":7", "existing record keeps prior default");
    free(resp); resp = NULL;
}

static void t_random_too_big(TestClient *tc) {
    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rtb\","
        "\"splits\":8,\"max_key\":32,\"fields\":[\"name:varchar:32\"]}", &resp);
    free(resp); resp = NULL;
    /* random(8) generates 16 hex chars; varchar:4 holds at most 4 chars
       (storage 6 incl. uint16 length). The encoder would silently truncate
       which violates the contract — reject pre-rebuild. */
    tc_request(tc,
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"rtb\","
        "\"fields\":[\"rnd:varchar:4:default=random(8)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "random(N) refused when field can't hold output");
    free(resp); resp = NULL;
}

static int test_add_field_computed_defaults_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    t_literal(tc);
    t_seq(tc);
    t_uuid(tc);
    t_random(tc);
    t_edit_field_change_default(tc);
    t_random_too_big(tc);

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-add-field-computed-defaults", test_add_field_computed_defaults_run)
