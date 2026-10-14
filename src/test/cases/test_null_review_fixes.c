/* src/test/cases/test_null_review_fixes.c
 *
 * Regression tests for the 2026-10-14 review findings on the null-bitmap
 * branch (docs/plans/2026-10-10-null-bitmap.md):
 *
 *   - build_index_payload_from_record_into validated out_cap only AFTER
 *     typed_field_to_index_key had written the payload — a trigram-indexed
 *     varchar whose content exceeded the caller's buffer overflowed it.
 *     Unit: tiny out_cap + canary region must come back untouched.
 *   - array-form BETWEEN accepted a raw JSON null bound ("value":[null,5])
 *     as the string "null" — the criteria parser must reject it (quoted
 *     "null" stays a legitimate string bound).
 *   - the array-member scanners normalized only spaces off unquoted
 *     bounds, so a leading tab or trailing CR/LF still let a raw null
 *     through ("\tnull" bound as a string) and stray whitespace between
 *     members glued onto the next bound. All JSON whitespace now
 *     normalizes off both ends in the BETWEEN split and the IN scanner.
 *   - a NULL local join field encoded its zero bytes and probed the remote
 *     index, falsely matching a remote zero-valued key. NULL joins nothing.
 *   - the unprojected rows-format cell rendered a NULL field as "" — it
 *     must render the bare literal null (same as the projected path).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ==== F1: payload builder must not write before validating out_cap ==== */
static int test_null_review_payload_cap(void) {
    /* One varchar:N field; record is nullmap generation with the bit
       clear and 200 content bytes. A 16-byte out_cap cannot hold the
       payload — the pre-validation must return 0 without writing. */
    TypedSchema ts;
    memset(&ts, 0, sizeof(ts));
    ts.nfields = 1;
    ts.nullmap_prefix = 1 + typed_nullmap_bytes(ts.nfields);
    TypedField *f = &ts.fields[0];
    snprintf(f->name, sizeof(f->name), "s");
    f->type = FT_VARCHAR;
    f->size = 1024;             /* 2-byte length prefix + 1022 content */
    f->offset = 0;
    ts.total_size = ts.nullmap_prefix + f->size;

    size_t vlen = (size_t)ts.total_size;
    uint8_t *rec = calloc(1, vlen);
    ASSERT_NOT_NULL(rec, "alloc record");
    if (!rec) return 1;
    rec[0] = (uint8_t)typed_nullmap_bytes(ts.nfields);
    uint8_t *content = rec + ts.nullmap_prefix;
    content[0] = 0; content[1] = 200;
    memset(content + 2, 'A', 200);

    const size_t BUF_SZ = 16;
    const size_t CANARY_SZ = 128;
    uint8_t *region = malloc(BUF_SZ + CANARY_SZ);
    ASSERT_NOT_NULL(region, "alloc region");
    if (!region) { free(rec); return 1; }
    memset(region + BUF_SZ, 0xAB, CANARY_SZ);

    size_t blen = 12345;
    int rc = build_index_payload_from_record_into(
        &ts, rec, vlen, SREC_FLAG_NULLMAP, "s",
        region, BUF_SZ, &blen);
    /* Capacity is reported as -1 (ENOBUFS), never as 0 "no key" — a
       600-byte trigram payload is a real key and must reach the index
       through the allocating fallback. */
    ASSERT_EQ_INT(rc, -1, "oversize payload reports capacity, not absence");
    ASSERT_EQ_INT((int)blen, 0, "no length reported on rejection");

    int canary_intact = 1;
    for (size_t i = 0; i < CANARY_SZ; i++)
        if (region[BUF_SZ + i] != 0xAB) { canary_intact = 0; break; }
    ASSERT_TRUE(canary_intact, "no byte written past out_cap");

    /* Heap wrapper: a payload larger than the old 512-byte staging
       buffer builds successfully through the heap fallback — dropping
       it here silently lost every trigram index update for values over
       ~510 content bytes. */
    uint8_t *big = calloc(1, vlen);
    ASSERT_NOT_NULL(big, "alloc big record");
    if (big) {
        big[0] = rec[0];
        big[ts.nullmap_prefix] = (uint8_t)(600 >> 8);   /* 600 content bytes */
        big[ts.nullmap_prefix + 1] = (uint8_t)(600 & 0xFF);
        memset(big + ts.nullmap_prefix + 2, 'B', 600);
        uint8_t *out = NULL;
        size_t out_len = 0;
        rc = build_index_payload_from_record(
            &ts, big, vlen, SREC_FLAG_NULLMAP, "s", &out, &out_len);
        ASSERT_EQ_INT(rc, 1, "600-byte payload builds via heap fallback");
        ASSERT_EQ_INT((int)out_len, 600, "full payload length reported");
        ASSERT_TRUE(out != NULL && out[0] == 'B' && out[599] == 'B',
                    "payload content intact");
        free(out);
        free(big);
    }

    /* Fitting payload still builds (behavior unchanged for in-cap keys). */
    {
        uint8_t wide[256];
        size_t wlen = 0;
        rc = build_index_payload_from_record_into(
            &ts, rec, vlen, SREC_FLAG_NULLMAP, "s", wide, sizeof(wide), &wlen);
        ASSERT_EQ_INT(rc, 1, "200-byte payload builds with room");
        ASSERT_EQ_INT((int)wlen, 200, "payload length is content length");
        ASSERT_EQ_INT(wide[0], 'A', "payload starts at content byte 0");
    }

    free(region);
    free(rec);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-null-review-payload-cap", test_null_review_payload_cap)

/* ==== F10: raw JSON null bounds rejected in array-form BETWEEN ==== */
static int test_null_review_between_null_array(void) {
    SearchCriterion *cs = NULL;
    int n = 0;

    /* Array form — the single-object shape parses as simple-equality
       (no "op"), so the criteria grammar is exercised via [...]. */
    ASSERT_TRUE(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"between\",\"value\":[null,5]}]",
        &cs, &n) != 0, "raw null low bound rejected");
    ASSERT_TRUE(cs == NULL, "no criteria leaked on rejection");

    ASSERT_TRUE(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"between\",\"value\":[1,null]}]",
        &cs, &n) != 0, "raw null high bound rejected");
    ASSERT_TRUE(cs == NULL, "no criteria leaked on rejection (high)");

    ASSERT_TRUE(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"between\",\"value\":[null,null]}]",
        &cs, &n) != 0, "both bounds null rejected");
    ASSERT_TRUE(cs == NULL, "no criteria leaked on rejection (both)");

    /* Quoted "null" stays a legitimate string bound (varchar doctrine). */
    ASSERT_EQ_INT(parse_criteria_json(
        "[{\"field\":\"s\",\"op\":\"between\",\"value\":[\"null\",\"zz\"]}]",
        &cs, &n), 0, "quoted \"null\" bound accepted");
    if (cs) { free_criteria(cs, n); cs = NULL; n = 0; }

    /* value2 form already rejected — pin it stays so. */
    ASSERT_TRUE(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"between\",\"value\":\"1\",\"value2\":null}]",
        &cs, &n) != 0, "raw null value2 rejected");
    ASSERT_TRUE(cs == NULL, "no criteria leaked on rejection (value2)");

    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-null-review-between-null-array", test_null_review_between_null_array)

/* ==== F7/F9 wire fixture: driver jd(jf long), remote jr(k long indexed) ==== */
static TestClient *nrf_join_setup(TestEnv *env) {
    if (test_env_start(env) != 0) return NULL;
    TestClientCfg cfg = { .port = env->port };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(env); return NULL; }
    char *r = NULL;
    const char *seq[] = {
        "{\"mode\":\"add-dir\",\"dir\":\"nrf\"}",
        "{\"mode\":\"create-object\",\"dir\":\"nrf\",\"object\":\"jr\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"k:long\",\"payload:varchar:16\"],\"indexes\":[\"k\"]}",
        "{\"mode\":\"create-object\",\"dir\":\"nrf\",\"object\":\"jd\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"jf:long\",\"note:varchar:16\"]}",
        /* Remote: k=0 (the zero value a NULL probe used to match) and k=7. */
        "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"jr\",\"key\":\"z1\","
        "\"value\":{\"k\":\"0\",\"payload\":\"zero\"}}",
        "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"jr\",\"key\":\"z2\","
        "\"value\":{\"k\":\"7\",\"payload\":\"seven\"}}",
        /* Driver: one NULL join field (note omitted → NULL too), one real 7. */
        "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"jd\",\"key\":\"d1\","
        "\"value\":{\"jf\":null}}",
        "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"jd\",\"key\":\"d2\","
        "\"value\":{\"jf\":\"7\",\"note\":\"sevenlocal\"}}",
    };
    for (size_t i = 0; i < sizeof(seq) / sizeof(seq[0]); i++) {
        if (tc_request(tc, seq[i], &r) != 0 || strstr(r, "\"error\"")) {
            free(r); tc_close(tc); test_env_stop(env); return NULL;
        }
        free(r); r = NULL;
    }
    return tc;
}

static int test_null_review_join_null_local(void) {
    TestEnv env;
    TestClient *tc = nrf_join_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        /* LEFT join: the NULL local row emits nulls — it must NOT pick up
           the remote k=0 record. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"nrf\",\"object\":\"jd\",\"criteria\":[],"
            "\"join\":[{\"object\":\"jr\",\"local\":\"jf\",\"remote\":\"k\","
            "\"as\":\"r\",\"type\":\"left\",\"fields\":[\"payload\"]}],"
            "\"limit\":10}", &r), 0, "left join");
        ASSERT_NOT_NULL(r, "left resp");
        ASSERT_TRUE(strstr(r, "\"zero\"") == NULL,
                    "NULL local must not match remote zero-valued key");
        ASSERT_TRUE(strstr(r, "null") != NULL,
                    "NULL local emits nulls on a left join");
        ASSERT_TRUE(strstr(r, "\"seven\"") != NULL,
                    "real 7 still matches k=7");
        free(r); r = NULL;

        /* INNER join: the NULL local row is dropped entirely. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"nrf\",\"object\":\"jd\",\"criteria\":[],"
            "\"join\":[{\"object\":\"jr\",\"local\":\"jf\",\"remote\":\"k\","
            "\"as\":\"r\",\"type\":\"inner\",\"fields\":[\"payload\"]}]}",
            &r), 0, "inner join");
        ASSERT_NOT_NULL(r, "inner resp");
        ASSERT_TRUE(strstr(r, "\"d1\"") == NULL,
                    "NULL local dropped on inner join");
        ASSERT_TRUE(strstr(r, "\"seven\"") != NULL,
                    "real 7 survives inner join");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-review-join-null-local", test_null_review_join_null_local)

/* ==== F9: unprojected rows cells render NULL as null, not "" ==== */
static int test_null_review_rows_null_render(void) {
    TestEnv env;
    TestClient *tc = nrf_join_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        /* rows format, NO projection: every typed field renders — d1's
           NULL jf cell is the bare literal null, not "" (and note, a
           non-NULL varchar, still renders as a quoted string). */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"nrf\",\"object\":\"jd\",\"criteria\":[],"
            "\"format\":\"rows\",\"limit\":10}", &r),
            0, "rows find");
        ASSERT_NOT_NULL(r, "rows resp");
        ASSERT_TRUE(strstr(r, "[\"d1\",null,null]") != NULL,
                    "unprojected NULL cells render null (jf and note both NULL)");
        ASSERT_TRUE(strstr(r, "[\"d2\",7,\"sevenlocal\"]") != NULL,
                    "non-NULL cells render as values");
        free(r); r = NULL;

        /* Projected twin already rendered null — pin it stays so. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"nrf\",\"object\":\"jd\",\"criteria\":[],"
            "\"fields\":[\"jf\"],\"format\":\"rows\",\"limit\":10}", &r),
            0, "projected rows find");
        ASSERT_NOT_NULL(r, "proj resp");
        ASSERT_TRUE(strstr(r, "[\"d1\",null]") != NULL,
                    "projected NULL renders null");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-review-rows-null-render", test_null_review_rows_null_render)

/* ==== F10 wire: between with a raw null array bound is a request error === */
static int test_null_review_between_null_wire(void) {
    TestEnv env;
    TestClient *tc = nrf_join_setup(&env);
    ASSERT_NOT_NULL(tc, "fixture");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"nrf\",\"object\":\"jd\",\"criteria\":"
            "[{\"field\":\"jf\",\"op\":\"between\",\"value\":[null,10]}]}",
            &r), 0, "request completes");
        ASSERT_NOT_NULL(r, "resp");
        ASSERT_TRUE(strstr(r, "\"error\"") != NULL,
                    "raw null between bound rejected over the wire");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-review-between-null-wire", test_null_review_between_null_wire)

/* ==== F5 follow-up: whitespace must not bypass the null rejection ==== */
static int test_null_review_between_ws(void) {
    SearchCriterion *cs = NULL;
    int n = 0;

    ASSERT_TRUE(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"between\",\"value\":[null ,5]}]",
        &cs, &n) != 0, "trailing-space null bound rejected");
    ASSERT_TRUE(cs == NULL, "no criteria leaked (trailing space)");

    ASSERT_TRUE(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"between\",\"value\":[ null ,5]}]",
        &cs, &n) != 0, "padded null bound rejected");
    ASSERT_TRUE(cs == NULL, "no criteria leaked (padded)");

    /* The full JSON whitespace set (space/tab/CR/LF) normalizes off both
       ends — a leading tab and a trailing CR used to survive the trim
       loops and bind "\tnull"/"null\r" as string bounds. */
    ASSERT_TRUE(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"between\",\"value\":[\tnull,5]}]",
        &cs, &n) != 0, "leading-tab null bound rejected");
    ASSERT_TRUE(cs == NULL, "no criteria leaked (leading tab)");

    ASSERT_TRUE(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"between\",\"value\":[null\r,5]}]",
        &cs, &n) != 0, "trailing-CR null bound rejected");
    ASSERT_TRUE(cs == NULL, "no criteria leaked (trailing CR)");

    ASSERT_TRUE(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"between\",\"value\":[\r\n\t null \t\r\n,5]}]",
        &cs, &n) != 0, "mixed-whitespace null bound rejected");
    ASSERT_TRUE(cs == NULL, "no criteria leaked (mixed whitespace)");

    /* Quoted members keep their whitespace — "null " is a string value. */
    ASSERT_EQ_INT(parse_criteria_json(
        "[{\"field\":\"s\",\"op\":\"between\",\"value\":[\"null\" ,\"zz\"]}]",
        &cs, &n), 0, "quoted bound with trailing space in array accepted");
    ASSERT_TRUE(strcmp(cs[0].value, "null") == 0,
                "quoted \"null\" bound content preserved");
    if (cs) { free_criteria(cs, n); cs = NULL; n = 0; }

    /* Stray JSON whitespace after a quoted member belongs between members,
       not to the next bound — ["1"<CR>,<TAB>5] used to bind value2 to the
       CR byte itself. */
    ASSERT_EQ_INT(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"between\",\"value\":[\"1\"\r,\t5]}]",
        &cs, &n), 0, "whitespace between members accepted");
    ASSERT_TRUE(cs && strcmp(cs[0].value, "1") == 0 &&
                strcmp(cs[0].value2, "5") == 0,
                "bounds survive whitespace between members");
    if (cs) { free_criteria(cs, n); cs = NULL; n = 0; }

    /* Quoted "null" followed by stray CRLF stays a string bound. */
    ASSERT_EQ_INT(parse_criteria_json(
        "[{\"field\":\"s\",\"op\":\"between\",\"value\":[\"null\"\r\n,\"zz\"]}]",
        &cs, &n), 0, "quoted \"null\" with stray CRLF accepted");
    ASSERT_TRUE(cs && strcmp(cs[0].value, "null") == 0 &&
                strcmp(cs[0].value2, "zz") == 0,
                "quoted \"null\" preserved across CRLF");
    if (cs) { free_criteria(cs, n); cs = NULL; n = 0; }

    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-null-review-between-ws", test_null_review_between_ws)

/* ==== F5 sibling: the IN member scanner normalizes the same whitespace
 * set — a leading tab used to glue onto the member and let "\tnull"
 * through the raw-null rejection as a string element. ==== */
static int test_null_review_in_null_ws(void) {
    SearchCriterion *cs = NULL;
    int n = 0;

    ASSERT_TRUE(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"in\",\"value\":[\tnull,5]}]",
        &cs, &n) != 0, "leading-tab null IN member rejected");
    ASSERT_TRUE(cs == NULL, "no criteria leaked (IN leading tab)");

    ASSERT_TRUE(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"in\",\"value\":[\r\n null ,5]}]",
        &cs, &n) != 0, "whitespace-padded null IN member rejected");
    ASSERT_TRUE(cs == NULL, "no criteria leaked (IN padded)");

    /* Legit JSON whitespace between members must not glue onto a value. */
    ASSERT_EQ_INT(parse_criteria_json(
        "[{\"field\":\"n\",\"op\":\"in\",\"value\":[7,\t8]}]",
        &cs, &n), 0, "whitespace between IN members accepted");
    ASSERT_TRUE(cs && cs[0].in_count == 2 &&
                strcmp(cs[0].in_values[0], "7") == 0 &&
                strcmp(cs[0].in_values[1], "8") == 0,
                "IN members parsed clean across whitespace");
    if (cs) { free_criteria(cs, n); cs = NULL; n = 0; }

    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-null-review-in-null-ws", test_null_review_in_null_ws)

/* ==== Review finding 1 follow-up: a trigram value beyond the 544-byte
 * staging cap is a REAL key. The overflow guard used to answer "no key,"
 * so a bulk-insert upsert of a long value diffed no-key → no-key and the
 * trigram index silently kept matching the OLD text. ==== */
static void nrf_fill_603(char *buf, const char *tag) {
    /* Marker sits at bytes 300..306 — fully inside the value, so every
       trigram of the contains query exists contiguously. */
    memset(buf, 'a', 603);
    memcpy(buf + 300, tag, 7);
    buf[603] = '\0';
}
static int test_null_review_trigram_long_upsert(void) {
    TestEnv env;
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        char oldv[620], newv[620];
        nrf_fill_603(oldv, "MARKOLD");
        nrf_fill_603(newv, "MARKNEW");

        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"add-dir\",\"dir\":\"nrf\"}", &r), 0, "add-dir");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"create-object\",\"dir\":\"nrf\",\"object\":\"tg\","
            "\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"s:varchar:1024\"],"
            "\"indexes\":[\"s:trigram\"]}", &r), 0, "create tg object");
        free(r); r = NULL;

        char req[1600];
        /* Bulk-insert acts as upsert — the path whose NEW-side key build
           stages through BT_MAX_VAL_LEN (544) bytes. */
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"nrf\",\"object\":\"tg\","
            "\"records\":[{\"key\":\"k1\",\"value\":{\"s\":\"%s\"}}]}", oldv);
        ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "bulk-insert OLD");
        ASSERT_TRUE(strstr(r, "\"error\"") == NULL, "OLD insert ok");
        free(r); r = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"nrf\",\"object\":\"tg\","
            "\"records\":[{\"key\":\"k1\",\"value\":{\"s\":\"%s\"}}]}", newv);
        ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "bulk-upsert NEW");
        ASSERT_TRUE(strstr(r, "\"error\"") == NULL, "NEW upsert ok");
        free(r); r = NULL;

        /* The trigram index must track NEW, not OLD. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"nrf\",\"object\":\"tg\",\"criteria\":"
            "[{\"field\":\"s\",\"op\":\"contains\",\"value\":\"MARKNEW\"}]}",
            &r), 0, "contains NEW");
        ASSERT_NOT_NULL(r, "new resp");
        ASSERT_TRUE(strstr(r, "\"k1\"") != NULL,
                    "trigram index finds the NEW value");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"nrf\",\"object\":\"tg\",\"criteria\":"
            "[{\"field\":\"s\",\"op\":\"contains\",\"value\":\"MARKOLD\"}]}",
            &r), 0, "contains OLD");
        ASSERT_NOT_NULL(r, "old resp");
        ASSERT_TRUE(strstr(r, "\"k1\"") == NULL,
                    "trigram index no longer matches the OLD value");
        free(r); r = NULL;

        /* The record itself is fine either way — pin that too. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"get\",\"dir\":\"nrf\",\"object\":\"tg\",\"key\":\"k1\"}",
            &r), 0, "get k1");
        ASSERT_TRUE(r && strstr(r, "MARKNEW") != NULL, "GET confirms NEW stored");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-review-trigram-long-upsert", test_null_review_trigram_long_upsert)

/* ==== Review finding 1 companion: bitmap/trigram bulk-UPDATE diffs fed
 * the TIERED btree builder's bytes to tg/bm consumers. For bitmap that
 * clears "[0x00]old" (a bit that was never set) and sets "[0x00]new" —
 * eq queries keep returning the stale value and never see the new one. ==== */
static int test_null_review_bitmap_bulk_update(void) {
    TestEnv env;
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"add-dir\",\"dir\":\"nrf\"}", &r), 0, "add-dir");
        free(r); r = NULL;
        /* A bare bool index auto-promotes to bitmap (idx_should_auto_bitmap)
           — exactly the consumer the tiered builder corrupted. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"create-object\",\"dir\":\"nrf\",\"object\":\"bm\","
            "\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"b:bool\"],\"indexes\":[\"b\"]}", &r), 0, "create bm object");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"bm\",\"key\":\"k1\","
            "\"value\":{\"b\":false}}", &r), 0, "insert b=false");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"bulk-update\",\"dir\":\"nrf\",\"object\":\"bm\","
            "\"records\":[{\"key\":\"k1\",\"value\":{\"b\":true}}]}", &r),
            0, "bulk-update b=true");
        free(r); r = NULL;

        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"nrf\",\"object\":\"bm\",\"criteria\":"
            "[{\"field\":\"b\",\"op\":\"eq\",\"value\":true}]}", &r),
            0, "find eq true");
        ASSERT_NOT_NULL(r, "true resp");
        ASSERT_TRUE(strstr(r, "\"k1\"") != NULL,
                    "eq true finds the updated record");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"nrf\",\"object\":\"bm\",\"criteria\":"
            "[{\"field\":\"b\",\"op\":\"eq\",\"value\":false}]}", &r),
            0, "find eq false");
        ASSERT_NOT_NULL(r, "false resp");
        ASSERT_TRUE(strstr(r, "\"k1\"") == NULL,
                    "eq false no longer returns the updated record");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-review-bitmap-bulk-update", test_null_review_bitmap_bulk_update)

/* ==== Review finding 2: the indexed group-by run-merge ignored the tier
 * byte, folding NULL rows into an equal-payload value bucket (bool false
 * [0x00][00] vs NULL [0x01][00]). ==== */
static int test_null_review_igb_bool_null_bucket(void) {
    TestEnv env;
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"add-dir\",\"dir\":\"nrf\"}", &r), 0, "add-dir");
        free(r); r = NULL;
        /* Explicit :btree — a bare bool name would auto-promote to bitmap
           and miss the btree run-merge entirely. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"create-object\",\"dir\":\"nrf\",\"object\":\"bt\","
            "\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"b:bool\"],\"indexes\":[\"b:btree\"]}", &r),
            0, "create btree-bool object");
        free(r); r = NULL;
        for (int i = 0; i < 8; i++) {
            char req[256];
            snprintf(req, sizeof(req),
                "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"bt\","
                "\"key\":\"f%d\",\"value\":{\"b\":false}}", i);
            ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "insert false row");
            free(r); r = NULL;
            snprintf(req, sizeof(req),
                "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"bt\","
                "\"key\":\"n%d\",\"value\":{\"b\":null}}", i);
            ASSERT_EQ_INT(tc_request(tc, req, &r), 0, "insert NULL row");
            free(r); r = NULL;
        }
        /* No limit → the IGB indexed fast path (the VS path needs one). */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"aggregate\",\"dir\":\"nrf\",\"object\":\"bt\","
            "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}],"
            "\"group_by\":[\"b\"]}", &r), 0, "IGB group-by");
        ASSERT_NOT_NULL(r, "resp");
        ASSERT_TRUE(strstr(r, "{\"b\":\"false\",\"c\":8}") != NULL,
                    "false bucket holds exactly the 8 false rows");
        ASSERT_TRUE(strstr(r, "{\"b\":null,\"c\":8}") != NULL,
                    "NULL is its own bucket with exactly the 8 NULL rows");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-review-igb-bool-null-bucket", test_null_review_igb_bool_null_bucket)

/* ==== Review finding 3: an empty varchar's leaf is the tier byte alone
 * (plen 0); decode_idx_to_buf returned 0 without touching the label
 * buffer, so the IGB "" bucket's key was uninitialized stack. ==== */
static int test_null_review_igb_empty_label(void) {
    TestEnv env;
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"add-dir\",\"dir\":\"nrf\"}", &r), 0, "add-dir");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"create-object\",\"dir\":\"nrf\",\"object\":\"el\","
            "\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"g:varchar:8\"],\"indexes\":[\"g\"]}", &r),
            0, "create el object");
        free(r); r = NULL;
        const char *rows[] = {
            "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"el\","
            "\"key\":\"e1\",\"value\":{\"g\":\"\"}}",
            "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"el\","
            "\"key\":\"e2\",\"value\":{\"g\":\"\"}}",
            "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"el\","
            "\"key\":\"x1\",\"value\":{\"g\":\"x\"}}",
            "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"el\","
            "\"key\":\"n1\",\"value\":{}}",
        };
        for (size_t i = 0; i < 4; i++) {
            ASSERT_EQ_INT(tc_request(tc, rows[i], &r), 0, "insert");
            free(r); r = NULL;
        }
        /* Indexed path (no limit → IGB). */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"aggregate\",\"dir\":\"nrf\",\"object\":\"el\","
            "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}],"
            "\"group_by\":[\"g\"]}", &r), 0, "IGB group-by empty");
        ASSERT_NOT_NULL(r, "resp");
        ASSERT_TRUE(strstr(r, "{\"g\":\"\",\"c\":2}") != NULL,
                    "empty-string bucket labeled \"\" with both rows");
        ASSERT_TRUE(strstr(r, "{\"g\":\"x\",\"c\":1}") != NULL, "\"x\" bucket");
        ASSERT_TRUE(strstr(r, "{\"g\":null,\"c\":1}") != NULL, "NULL bucket");
        free(r); r = NULL;
        /* Scan twin must agree. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"remove-index\",\"dir\":\"nrf\",\"object\":\"el\","
            "\"field\":\"g\"}", &r), 0, "remove index");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"aggregate\",\"dir\":\"nrf\",\"object\":\"el\","
            "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}],"
            "\"group_by\":[\"g\"]}", &r), 0, "scan group-by empty");
        ASSERT_NOT_NULL(r, "scan resp");
        ASSERT_TRUE(strstr(r, "{\"g\":\"\",\"c\":2}") != NULL,
                    "scan path agrees on the empty bucket");
        ASSERT_TRUE(strstr(r, "{\"g\":null,\"c\":1}") != NULL,
                    "scan path agrees on the NULL bucket");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-review-igb-empty-label", test_null_review_igb_empty_label)

/* ==== Review finding 4: the DESC cursor's first page passed the legacy
 * \xff*4 max-sentinel, which the ordered walk tier-clamps to [0x00]* —
 * so the NULL rows that LEAD a descending stream never appeared. ==== */
static int test_null_review_desc_cursor_null(void) {
    TestEnv env;
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"add-dir\",\"dir\":\"nrf\"}", &r), 0, "add-dir");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"create-object\",\"dir\":\"nrf\",\"object\":\"dc\","
            "\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"v:long\"],\"indexes\":[\"v\"]}", &r), 0, "create dc");
        free(r); r = NULL;
        const char *rows[] = {
            "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"dc\",\"key\":\"d1\","
            "\"value\":{\"v\":\"5\"}}",
            "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"dc\",\"key\":\"d2\","
            "\"value\":{\"v\":\"10\"}}",
            "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"dc\",\"key\":\"d3\","
            "\"value\":{\"v\":\"20\"}}",
            "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"dc\",\"key\":\"d4\","
            "\"value\":{\"v\":null}}",
            "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"dc\",\"key\":\"d5\","
            "\"value\":{\"v\":null}}",
        };
        for (size_t i = 0; i < 5; i++) {
            ASSERT_EQ_INT(tc_request(tc, rows[i], &r), 0, "insert");
            free(r); r = NULL;
        }

        /* First DESC page (limit 3) must lead with the NULL rows. */
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"nrf\",\"object\":\"dc\",\"criteria\":[],"
            "\"order_by\":\"v\",\"order\":\"desc\",\"limit\":3,\"cursor\":null}",
            &r), 0, "DESC page 1");
        ASSERT_NOT_NULL(r, "p1");
        ASSERT_TRUE(strstr(r, "\"d4\"") != NULL && strstr(r, "\"d5\"") != NULL,
                    "DESC first page leads with the NULL rows");
        ASSERT_TRUE(strstr(r, "\"d1\"") == NULL,
                    "DESC first page does not reach the smallest value");
        free(r); r = NULL;

        /* Full walk: all 5 rows exactly once. */
        char seen[5][8];
        int nseen = 0, dup = 0;
        char cursor[512];
        snprintf(cursor, sizeof(cursor), "null");
        for (int pg = 0; pg < 5; pg++) {
            char req[1024];
            snprintf(req, sizeof(req),
                "{\"mode\":\"find\",\"dir\":\"nrf\",\"object\":\"dc\","
                "\"criteria\":[],\"order_by\":\"v\",\"order\":\"desc\","
                "\"limit\":2,\"cursor\":%s}", cursor);
            if (tc_request(tc, req, &r) != 0 || !r) break;
            const char *rows_end = strstr(r, ",\"cursor\":");
            if (!rows_end) rows_end = r + strlen(r);
            const char *p = r;
            while ((p = strstr(p, "\"key\":\"")) != NULL && p < rows_end) {
                p += 7;
                const char *e = strchr(p, '"');
                if (!e) break;
                size_t len = (size_t)(e - p);
                if (len < sizeof(seen[0])) {
                    int known = 0;
                    for (int i = 0; i < nseen; i++)
                        if (memcmp(seen[i], p, len) == 0 && seen[i][len] == 0)
                            known = 1;
                    if (!known && nseen < 5) {
                        memcpy(seen[nseen], p, len);
                        seen[nseen][len] = '\0';
                        nseen++;
                    } else if (known) dup++;
                }
                p = e;
            }
            const char *c = strstr(r, "\"cursor\":");
            const char *cs = c ? strchr(c, '{') : NULL;
            const char *ce = cs ? strchr(cs, '}') : NULL;
            if (!cs || !ce) break;
            size_t clen = (size_t)(ce - cs) + 1;
            if (clen >= sizeof(cursor)) break;
            memcpy(cursor, cs, clen); cursor[clen] = '\0';
            free(r); r = NULL;
        }
        free(r); r = NULL;
        ASSERT_EQ_INT(nseen, 5, "all 5 rows across DESC pages");
        ASSERT_EQ_INT(dup, 0, "no row repeated across DESC pages");
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-review-desc-cursor-null", test_null_review_desc_cursor_null)

/* ==== GET corruption guard: a flagged record whose header byte 19
 * carries unknown bits must fail closed — {"error":"corrupt record"},
 * not a bare newline masquerading as an empty dict. ==== */
#include <dirent.h>

/* Scan every segment file under <obj_dir>/data/streams for the live
   record whose key is `key`; apply `edit` to its open FILE* positioned
   at the record start. Returns 1 if found+edited, 0 if not found, -1
   on I/O trouble. */
static int nrf_edit_live_record(const char *obj_dir, const char *key,
                                int (*edit)(FILE *f, long off, void *ud),
                                void *ud) {
    char streams[768];
    snprintf(streams, sizeof(streams), "%s/data/streams", obj_dir);
    DIR *sd = opendir(streams);
    if (!sd) return -1;
    size_t klen = strlen(key);
    int done = 0;
    struct dirent *de;
    while (!done && (de = readdir(sd)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char sub[832];
        snprintf(sub, sizeof(sub), "%s/%s", streams, de->d_name);
        DIR *sdir = opendir(sub);
        if (!sdir) continue;
        struct dirent *fe;
        while (!done && (fe = readdir(sdir)) != NULL) {
            if (!strstr(fe->d_name, ".dat")) continue;
            char fp[1088];
            snprintf(fp, sizeof(fp), "%s/%s", sub, fe->d_name);
            FILE *f = fopen(fp, "r+b");
            if (!f) continue;
            fseek(f, 0, SEEK_END);
            long sz = ftell(f);
            /* Segment records occupy fixed slot_size slots, 8-aligned
               from 0 — an 8-byte stride visits every record header. */
            for (long off = 0; off + 24 <= sz && !done; off += 8) {
                uint8_t hdr[24];
                if (fseek(f, off, SEEK_SET) != 0) break;
                if (fread(hdr, 1, 24, f) != 24) break;
                if (hdr[18] != 1) continue;              /* not live */
                uint16_t kl = (uint16_t)(hdr[16] | (hdr[17] << 8));
                if (kl != klen) continue;
                char kb[300];
                if (kl > sizeof(kb)) break;
                if (fread(kb, 1, (size_t)kl, f) != (size_t)kl) break;
                if (memcmp(kb, key, (size_t)kl) != 0) continue;
                if (edit(f, off, ud) == 0) done = 1;
            }
            fclose(f);
        }
        closedir(sdir);
    }
    closedir(sd);
    return done;
}

static int nrf_corrupt_hdr2(FILE *f, long off, void *ud) {
    (void)ud;
    if (fseek(f, off + 19, SEEK_SET) != 0) return -1;
    /* 0x40 = unknown hdr2 bit → typed_record_view fails closed. */
    fputc(0x40, f);
    return fflush(f) == 0 ? 0 : -1;
}

static int test_null_review_get_corrupt(void) {
    TestEnv env;
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (tc) {
        char *r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"add-dir\",\"dir\":\"nrf\"}", &r), 0, "add-dir");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"create-object\",\"dir\":\"nrf\",\"object\":\"fc\","
            "\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"s:varchar:8\"]}", &r), 0, "create fc");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"insert\",\"dir\":\"nrf\",\"object\":\"fc\",\"key\":\"c1\","
            "\"value\":{\"s\":\"hi\"}}", &r), 0, "insert c1");
        free(r); r = NULL;
        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"get\",\"dir\":\"nrf\",\"object\":\"fc\",\"key\":\"c1\"}",
            &r), 0, "get c1");
        ASSERT_TRUE(r && strstr(r, "\"s\":\"hi\"") != NULL,
                    "sanity: record reads before corruption");
        free(r); r = NULL;

        char obj_dir[768];
        snprintf(obj_dir, sizeof(obj_dir), "%s/nrf/fc", env.db_root);
        ASSERT_EQ_INT(nrf_edit_live_record(obj_dir, "c1",
                                           nrf_corrupt_hdr2, NULL), 1,
                      "corrupted the stored record's hdr2 byte");

        ASSERT_EQ_INT(tc_request(tc,
            "{\"mode\":\"get\",\"dir\":\"nrf\",\"object\":\"fc\",\"key\":\"c1\"}",
            &r), 0, "get corrupted c1");
        ASSERT_TRUE(r && strstr(r, "\"error\"") != NULL &&
                    strstr(r, "corrupt record") != NULL,
                    "corrupt record fails closed with an error, not a bare newline");
        free(r); r = NULL;
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-null-review-get-corrupt", test_null_review_get_corrupt)
