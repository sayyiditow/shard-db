/* src/test/cases/test_scalar_grammar_routes.c
 *
 * R4 — strict scalar grammar route matrix. Every malformed non-NULL input is
 * rejected with a field-named error on every checked route; every valid
 * grammar form stores exactly the bytes the legacy encoder produced for it;
 * zeros (nil UUID, 0.0.0.0, ::, midnight, empty varchar) stay real non-NULL
 * values and explicit JSON null stays NULL. CSV/delimited empty cells keep
 * their historical zero-fill semantics (call sites test emptiness before
 * the converter). Enum query literals error like writes (D4); pattern/
 * substring ops stay unvalidated (they operate on rendered text).
 *
 * Object sg (legacy layout, all fields nullable, indexed): b:bool d:date
 * dt:datetime dm:datetimems t:time u:uuid i4:ipv4 i6:ipv6
 * e:enum(red,green,blue) s:varchar:8. b/e carry bitmap indexes, so joins
 * target d/u (btree) instead. Data-derived probes (join drivers) that are
 * out-of-grammar mean "no match", never an error (hunk 2f).
 */
 
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/typed_row.h"
#include "../db/scalar_grammar.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static ShardDb *sg_db;

static char *sg_req(const char *json) {
    char *r = NULL;
    tu_pdb_request(sg_db, json, &r);
    return r;
}
static int sg_is_err(const char *resp) { return resp && strstr(resp, "\"error\"") != NULL; }

static void sg_free(char *r) { shard_db_free_result(r); }

/* Formerly GCC nested functions inside sg_criteria/sg_update; clang (macOS)
   rejects the extension. They capture nothing — file-scope statics over
   sg_req/sg_free are equivalent. */
static long count_of(const char *crit) {
    char q[512], *cr;
    snprintf(q, sizeof q,
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"sg\",\"criteria\":%s}",
             crit);
    cr = sg_req(q);
    long v = cr ? strtol(cr, NULL, 10) : -1;
    sg_free(cr);
    return v;
}
static long count_on(const char *obj, const char *crit) {
    char q[512], *cr;
    snprintf(q, sizeof q,
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\",\"criteria\":%s}",
             obj, crit);
    cr = sg_req(q);
    long v = cr ? strtol(cr, NULL, 10) : -1;
    sg_free(cr);
    return v;
}

/* Insert one single-field record, then read it back and assert the rendered
 * fragment. want_err != NULL → the insert must fail with that fragment and
 * the record must not exist. Otherwise assert insert succeeded and get
 * returns want_get (substring; NULL skips the read-back). */
static void sg_cell(const char *key, const char *field, const char *jsonval,
                    const char *want_err, const char *want_get, const char *label) {
    char req[512], *r;
    snprintf(req, sizeof req,
             "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sg\","
             "\"key\":\"%s\",\"value\":{ \"%s\":%s}}", key, field, jsonval);
    r = sg_req(req);
    if (want_err) {
        ASSERT_TRUE(sg_is_err(r), label);
        ASSERT_CONTAINS(r, want_err, label);
        sg_free(r);
        snprintf(req, sizeof req,
                 "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"sg\",\"key\":\"%s\"}",
                 key);
        r = sg_req(req);
        ASSERT_TRUE(r && strstr(r, "false") != NULL, label);
        sg_free(r);
        return;
    }
    ASSERT_TRUE(!sg_is_err(r), label);
    sg_free(r);
    if (want_get) {
        snprintf(req, sizeof req,
                 "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sg\",\"key\":\"%s\"}",
                 key);
        r = sg_req(req);
        ASSERT_CONTAINS(r, want_get, label);
        sg_free(r);
    }
}

/* ---- insert matrix (legacy single-insert route) ---- */
static void sg_insert_matrix(void) {
    /* bool: only the contract spellings survive (D6). "yes" is a VALID
       true spelling; "o" is the ambiguous error. */
    sg_cell("b1", "b", "\"true\"",  NULL, "\"b\":true",  "bool true stores true");
    sg_cell("b2", "b", "\"false\"", NULL, "\"b\":false", "bool false stores false");
    sg_cell("b3", "b", "\"1\"",     NULL, "\"b\":true",  "bool 1 stores true");
    sg_cell("b4", "b", "\"0\"",     NULL, "\"b\":false", "bool 0 stores false");
    sg_cell("b5", "b", "\"YES\"",   NULL, "\"b\":true",  "bool YES case-insensitive true");
    sg_cell("b6", "b", "\"yes\"",   NULL, "\"b\":true",  "D6: yes is a true spelling (was false by first byte)");
    sg_cell("b7", "b", "\"Off\"",   NULL, "\"b\":false", "bool Off case-insensitive false");
    sg_cell("b8", "b", "\"10\"",   "not a valid bool", NULL, "10 rejected");
    sg_cell("b9", "b", "\"trash\"", "not a valid bool", NULL, "trash rejected");
    sg_cell("b10", "b", "\"\"",     "not a valid bool", NULL, "empty bool rejected");
    sg_cell("b11", "b", "\" \"",    "not a valid bool", NULL, "whitespace-only bool rejected");
    sg_cell("b12", "b", "\"o\"",    "not a valid bool", NULL, "ambiguous o rejected");
    /* date. */
    sg_cell("d1", "d", "\"20240105\"",   NULL, "\"d\":\"20240105\"", "compact date ok");
    sg_cell("d2", "d", "\"2024-01-05\"", NULL, "\"d\":\"20240105\"", "dashed date stores the same bytes");
    sg_cell("d3", "d", "\"20240230\"",  "not a valid date", NULL, "invalid calendar rejected");
    sg_cell("d4", "d", "\"2024-1-5\"",  "not a valid date", NULL, "short fields rejected");
    sg_cell("d5", "d", "\"2024/01/05\"", "not a valid date", NULL, "slashes rejected");
    sg_cell("d6", "d", "\"20240105x\"", "not a valid date", NULL, "trailing junk rejected");
    sg_cell("d7", "d", "\"\"",           "not a valid date", NULL, "empty date rejected");
    sg_cell("d8", "d", "\" \"",          "not a valid date", NULL, "whitespace-only date rejected");
    sg_cell("d9", "d", "\"0001-01-01\"", NULL, "\"d\":\"00010101\"", "year 1 valid");
    sg_cell("d10", "d", "\"9999-12-31\"", NULL, "\"d\":\"99991231\"", "date max valid");
    sg_cell("d11", "d", "\" 20240229 \"", NULL, "\"d\":\"20240229\"", "leap day + outer space");
    /* datetime. */
    sg_cell("t1", "dt", "\"20240105010203\"",      NULL, "\"dt\":\"20240105010203\"", "compact datetime ok");
    sg_cell("t2", "dt", "\"2024-01-05T01:02:03\"", NULL, "\"dt\":\"20240105010203\"", "T-separated datetime ok");
    sg_cell("t3", "dt", "\"2024-01-05 01:02:03\"", NULL, "\"dt\":\"20240105010203\"", "space-separated datetime ok");
    sg_cell("t4", "dt", "\"20240105\"",            "not a valid datetime", NULL, "date-only datetime rejected");
    sg_cell("t5", "dt", "\"2024-13-99T99:99:99\"", "not a valid datetime", NULL, "out-of-range datetime rejected");
    sg_cell("t6", "dt", "\"\"",                     "not a valid datetime", NULL, "empty datetime rejected");
    sg_cell("t7", "dt", "\"2024-01-05T24:00:00\"", "not a valid datetime", NULL, "hour 24 rejected");
    /* datetimems. */
    sg_cell("m1", "dm", "\"20240105010203004\"",      NULL, "\"dm\":\"20240105010203004\"", "compact datetimems ok");
    sg_cell("m2", "dm", "\"2024-01-05T01:02:03.004\"", NULL, "\"dm\":\"20240105010203004\"", "ISO datetimems ok");
    sg_cell("m3", "dm", "\"2024-01-05T01:02:03.12\"",  "not a valid datetimems", NULL, "2 fraction digits rejected");
    sg_cell("m4", "dm", "\"20240105\"",                "not a valid datetimems", NULL, "date-only datetimems rejected");
    sg_cell("m5", "dm", "\"\"",                         "not a valid datetimems", NULL, "empty datetimems rejected");
    /* time. */
    sg_cell("h1", "t", "\"12:30:45\"", NULL, "\"t\":\"12:30:45\"", "valid time ok");
    sg_cell("h2", "t", "\"23:59:59\"", NULL, "\"t\":\"23:59:59\"", "time max ok");
    sg_cell("h3", "t", "\"99:99:99\"", "not a valid time", NULL, "out-of-range time rejected");
    sg_cell("h4", "t", "\"1200\"",     "not a valid time", NULL, "malformed time rejected");
    sg_cell("h5", "t", "\"\"",          "not a valid time", NULL, "empty time rejected");
    /* uuid. */
    sg_cell("u1", "u", "\"550e8400-e29b-41d4-a716-446655440000\"", NULL,
            "\"u\":\"550e8400-e29b-41d4-a716-446655440000\"", "canonical uuid ok");
    sg_cell("u2", "u", "\"550e8400e29b41d4a716446655440000\"", NULL,
            "\"u\":\"550e8400-e29b-41d4-a716-446655440000\"", "compact uuid renders dashed");
    sg_cell("u3", "u", "\"{550e8400-e29b-41d4-a716-446655440000}\"", NULL,
            "\"u\":\"550e8400-e29b-41d4-a716-446655440000\"",
            "braced uuid accepted and stored canonical (M3)");
    sg_cell("u4", "u", "\"550e8400-e29b-41d4-a716-446655440000garbage\"", "not a valid uuid", NULL,
            "trailing junk uuid rejected");
    sg_cell("u5", "u", "\"not-a-uuid\"", "not a valid uuid", NULL,
            "malformed uuid rejected on a nullable field (T13)");
    sg_cell("u6", "u", "\"\"", "not a valid uuid", NULL, "empty uuid rejected");
    sg_cell("u7", "u", "\"00000000-0000-0000-0000-000000000000\"", NULL,
            "\"u\":\"00000000-0000-0000-0000-000000000000\"", "nil uuid stays a real value");
    /* ipv4. */
    sg_cell("v1", "i4", "\"10.0.0.1\"",  NULL, "\"i4\":\"10.0.0.1\"", "valid ipv4 ok");
    sg_cell("v2", "i4", "\"not-an-ip\"", "not a valid ipv4", NULL, "malformed ipv4 rejected");
    sg_cell("v3", "i4", "\"999.1.1.1\"", "not a valid ipv4", NULL, "out-of-range ipv4 rejected");
    sg_cell("v4", "i4", "\"1234567890123456789012345678901234567890123456789\"", "not a valid ipv4", NULL,
            "over-long ipv4 rejected without truncation (T13)");
    sg_cell("v5", "i4", "\"0.0.0.0\"", NULL, "\"i4\":\"0.0.0.0\"", "zero ipv4 stays a real value");
    sg_cell("v6", "i4", "\"\"", "not a valid ipv4", NULL, "empty ipv4 rejected");
    /* ipv6. */
    sg_cell("w1", "i6", "\"::1\"", NULL, "\"i6\":\"::1\"", "valid ipv6 ok");
    sg_cell("w2", "i6", "\"::g\"", "not a valid ipv6", NULL, "malformed ipv6 rejected");
    sg_cell("w3", "i6", "\"0:0:0:0:0:0:0:0\"", NULL, "\"i6\":\"::\"", "zero ipv6 stays a real value");
    sg_cell("w4", "i6", "\":::\"", "not a valid ipv6", NULL, "malformed ipv6 form rejected");
    sg_cell("w5", "i6", "\"\"", "not a valid ipv6", NULL, "empty ipv6 rejected");
    /* enum. */
    sg_cell("e1", "e", "\"green\"", NULL, "\"e\":\"green\"", "valid enum ok");
    sg_cell("e2", "e", "\"purple\"", "not a valid enum", NULL,
            "unknown enum rejected with the unified grammar message");
    sg_cell("e3", "e", "\"\"", "not a valid enum", NULL, "empty enum rejected (not member 0)");
    /* varchar. */
    sg_cell("s1", "s", "\"hi\"", NULL, "\"s\":\"hi\"", "varchar ok");
    sg_cell("s2", "s", "\"\"",   NULL, "\"s\":\"\"", "empty varchar stores empty (unchanged)");
    sg_cell("s3", "s", "\"a\\u0000b\"", "malformed JSON escape", NULL,
            "legacy varchar rejects decoded NUL (D7 unchanged)");
}

/* ---- update route (storage.c encode_field_checked path + brace gate) ---- */
static void sg_update_route(void) {
    char req[512], *r;
    r = sg_req("{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sg\","
               "\"key\":\"up\",\"value\":{\"d\":\"20240105\",\"u\":\"550e8400-e29b-41d4-a716-446655440000\",\"e\":\"green\"}}");
    ASSERT_TRUE(!sg_is_err(r), "update fixture row");
    sg_free(r);
    struct { const char *field, *jsonval, *want_err, *want_get; const char *label; } ups[] = {
        {"d", "\"20240230\"", "not a valid date", NULL, "update rejects invalid calendar"},
        {"d", "\"\"", "not a valid date", NULL, "update rejects empty date"},
        {"u", "\"not-a-uuid\"", "not a valid uuid", NULL,
         "update rejects malformed uuid on a nullable field"},
        {"e", "\"purple\"", "not a valid enum", NULL, "update rejects unknown enum"},
        {"u", "\"{550e8400-e29b-41d4-a716-446655440000}\"", NULL,
         "\"u\":\"550e8400-e29b-41d4-a716-446655440000\"",
         "update braced uuid exempted from the computed-update gate (M3)"},
        {"u", "\"{\\\"$incr\\\":1}\"", "error", NULL,
         "computed-update object on a uuid field still gated"},
    };
    for (size_t i = 0; i < sizeof ups / sizeof *ups; i++) {
        snprintf(req, sizeof req,
                 "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"sg\","
                 "\"key\":\"up\",\"value\":{ \"%s\":%s}}",
                 ups[i].field, ups[i].jsonval);
        r = sg_req(req);
        if (ups[i].want_err) ASSERT_TRUE(sg_is_err(r), ups[i].label);
        else ASSERT_TRUE(!sg_is_err(r), ups[i].label);
        sg_free(r);
        if (ups[i].want_get) {
            r = sg_req("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sg\",\"key\":\"up\"}");
            ASSERT_CONTAINS(r, ups[i].want_get, ups[i].label);
            sg_free(r);
        }
    }
    /* A failed update leaves the stored record untouched. */
    snprintf(req, sizeof req,
             "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"sg\","
             "\"key\":\"up\",\"value\":{\"d\":\"20240999\"}}");
    r = sg_req(req);
    ASSERT_TRUE(sg_is_err(r), "failed update errors");
    sg_free(r);
    r = sg_req("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sg\",\"key\":\"up\"}");
    ASSERT_TRUE(r && strstr(r, "\"d\":\"20240105\""), "failed update leaves OLD intact");
    sg_free(r);
}

/* ---- bulk routes: JSON records + delimited CSV ---- */
static void sg_bulk_routes(void) {
    char req[512], *r;
    /* bulk-insert JSON: per-record skip/error contract preserved — every
       malformed record is dropped and counted, none stored. */
    r = sg_req("{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"sg\",\"records\":["
               "{\"key\":\"bj1\",\"value\":{\"e\":\"purple\"}},"
               "{\"key\":\"bj2\",\"value\":{\"u\":\"not-a-uuid\"}},"
               "{\"key\":\"bj3\",\"value\":{\"i4\":\"999.1.1.1\"}},"
               "{\"key\":\"bj4\",\"value\":{\"d\":\"2024-02-30\"}},"
               "{\"key\":\"bj5\",\"value\":{\"b\":\"trash\"}},"
               "{\"key\":\"bj6\",\"value\":{\"dt\":\"20240105\"}},"
               "{\"key\":\"bj7\",\"value\":{\"d\":\"\"}}]}");
    ASSERT_TRUE(r && strstr(r, "\"inserted\":0") && strstr(r, "\"errors\":7"),
                "bulk-insert JSON drops and counts all seven malformed records");
    if (r) TAP_DIAG("bulk-json resp: %s", r);
    sg_free(r);
    const char *keys[] = {"bj1","bj2","bj3","bj4","bj5","bj6","bj7"};
    for (size_t i = 0; i < sizeof keys / sizeof *keys; i++) {
        snprintf(req, sizeof req,
                 "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"sg\",\"key\":\"%s\"}", keys[i]);
        r = sg_req(req);
        ASSERT_TRUE(r && strstr(r, "false"), "dropped bulk record absent");
        sg_free(r);
    }
    /* Valid forms land identically through bulk JSON. */
    r = sg_req("{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"sg\",\"records\":["
               "{\"key\":\"bg1\",\"value\":{\"d\":\"2024-01-05\",\"b\":\"TRUE\","
               "\"u\":\"{550e8400-e29b-41d4-a716-446655440000}\",\"i6\":\" ::1 \"}}]}");
    ASSERT_TRUE(r && strstr(r, "\"inserted\":1"), "bulk-insert JSON accepts valid grammar forms");
    sg_free(r);
    struct { const char *key, *want; const char *label; } bg[] = {
        {"bg1", "\"d\":\"20240105\"", "bulk JSON dashed date stores canonical bytes"},
        {"bg1", "\"b\":true", "bulk JSON TRUE stores true"},
        {"bg1", "\"u\":\"550e8400-e29b-41d4-a716-446655440000\"", "bulk JSON braced uuid canonical"},
        {"bg1", "\"i6\":\"::1\"", "bulk JSON padded ipv6 canonical"},
    };
    for (size_t i = 0; i < sizeof bg / sizeof *bg; i++) {
        snprintf(req, sizeof req,
                 "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sg\",\"key\":\"%s\"}", bg[i].key);
        r = sg_req(req);
        ASSERT_CONTAINS(r, bg[i].want, bg[i].label);
        sg_free(r);
    }
    /* bulk-insert-delimited CSV: valid rows land; empty cells keep the
       historical zero-fill (call site tests emptiness); a non-empty bad
       cell drops the record. */
    r = sg_req("{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\",\"object\":\"sg\","
               "\"delimiter\":\",\",\"data\":"
               "\"cv1,true,20240105,20240105010203,20240105010203004,12:30:45,"
               "550e8400-e29b-41d4-a716-446655440000,10.0.0.1,::1,green,hi\\n"
               "cv2,,,,,,,,,\\n"
               "cv3,yes,2024-02-30,20240105,20240105,99:99:99,not-a-uuid,not-an-ip,::g,purple,ab\\n\"}");
    ASSERT_TRUE(r && strstr(r, "\"errors\":1") && strstr(r, "\"inserted\":2"),
                "CSV import inserts valid rows, drops the malformed row");
    if (r) TAP_DIAG("csv resp: %s", r);
    sg_free(r);
    r = sg_req("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sg\",\"key\":\"cv2\"}");
    ASSERT_TRUE(r && strstr(r, "\"d\":\"00000000\"") &&
                strstr(r, "\"u\":\"00000000-0000-0000-0000-000000000000\"") &&
                strstr(r, "\"e\":\"red\"") && strstr(r, "\"s\":\"\""),
                "D3: CSV empty cells keep the historical zero-fill semantics");
    sg_free(r);
    snprintf(req, sizeof req,
             "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"sg\",\"key\":\"cv3\"}");
    r = sg_req(req);
    ASSERT_TRUE(r && strstr(r, "false"), "CSV malformed row dropped");
    sg_free(r);
    r = sg_req("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sg\",\"key\":\"cv1\"}");
    ASSERT_TRUE(r && strstr(r, "\"e\":\"green\"") && strstr(r, "\"t\":\"12:30:45\""),
                "CSV valid row stored");
    sg_free(r);
    /* bulk-update JSON: malformed value skips the record, OLD intact. */
    r = sg_req("{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sg\","
               "\"key\":\"bu\",\"value\":{\"d\":\"20240105\",\"e\":\"green\"}}");
    sg_free(r);
    r = sg_req("{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"sg\",\"records\":["
               "{\"key\":\"bu\",\"value\":{\"e\":\"purple\",\"u\":\"not-a-uuid\"}}]}");
    ASSERT_TRUE(r && !strstr(r, "\"updated\":1"), "bulk-update does not apply malformed values");
    if (r) TAP_DIAG("bulk-update resp: %s", r);
    sg_free(r);
    r = sg_req("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sg\",\"key\":\"bu\"}");
    ASSERT_TRUE(r && strstr(r, "\"e\":\"green\"") && strstr(r, "\"d\":\"20240105\""),
                "bulk-update failure leaves OLD intact");
    sg_free(r);
}

/* ---- criteria: comparison literals validated by the strict grammar ----
   Enum literals stay unvalidated until Task 4 widens the validation scope
   (asserted as 0-rows here, flipped to error there). */
static void sg_criteria(void) {
    char req[512];
    /* invalid literals error with the same field-named message as writes. */
    char *r = sg_req("{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"sg\","
                     "\"criteria\":[{\"field\":\"d\",\"op\":\"eq\",\"value\":\"2024-02-30\"}]}");
    /* Invalid literals are rejected at compile time; the find/count wire
       carries the pre-existing generic dispatch message (R2 contract). */
    TAP_DIAG("count invalid-date resp: %s", r ? r : "(null)");
    ASSERT_TRUE(sg_is_err(r) && strstr(r, "invalid criteria value"),
                "eq invalid-calendar date literal errors (not a silent 0)");
    sg_free(r);
    r = sg_req("{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"sg\","
               "\"criteria\":[{\"field\":\"d\",\"op\":\"eq\",\"value\":\"2024-02-30\"}],\"limit\":5}");
    ASSERT_TRUE(sg_is_err(r) && strstr(r, "invalid criteria value"),
                "find invalid-calendar date literal errors");
    sg_free(r);
    r = sg_req("{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"sg\","
               "\"criteria\":[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"trash\"}]}");
    ASSERT_TRUE(sg_is_err(r), "eq bool trash literal errors");
    sg_free(r);
    r = sg_req("{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"sg\","
               "\"criteria\":[{\"field\":\"u\",\"op\":\"in\","
               "\"value\":[\"550e8400-e29b-41d4-a716-446655440000\",\"junk\"]}]}");
    ASSERT_TRUE(sg_is_err(r), "in-list with a malformed uuid literal errors");
    sg_free(r);
    /* (d) bool word literals compile to the CORRECT bucket (the old
       first-byte rule sent yes→false, off→true). */
    long c_true = count_of("[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"true\"}]");
    long c_false = count_of("[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"false\"}]");
    ASSERT_TRUE(c_true >= 1 && c_false >= 1, "bool true/false rows exist");
    ASSERT_EQ_INT(count_of("[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"yes\"}]"), c_true,
                  "eq yes matches the TRUE rows (first-byte bug fixed)");
    ASSERT_EQ_INT(count_of("[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"off\"}]"), c_false,
                  "eq off matches the FALSE rows");
    ASSERT_EQ_INT(count_of("[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"TRUE\"}]"), c_true,
                  "eq TRUE matches the TRUE rows");
    ASSERT_EQ_INT(count_of("[{\"field\":\"b\",\"op\":\"neq\",\"value\":\"yes\"}]"), c_false,
                  "neq yes matches the FALSE rows on the indexed path");
    ASSERT_EQ_INT(count_of("[{\"field\":\"b\",\"op\":\"neq\",\"value\":\"off\"}]"), c_true,
                  "neq off matches the TRUE rows on the indexed path");
    /* valid literals: compact and dashed forms compile to the same key. */
    snprintf(req, sizeof req, "[{\"field\":\"d\",\"op\":\"eq\",\"value\":\"20240105\"}]");
    ASSERT_TRUE(count_of(req) >= 1, "eq compact date matches");
    long c1 = count_of("[{\"field\":\"d\",\"op\":\"eq\",\"value\":\"2024-01-05\"}]");
    ASSERT_TRUE(count_of("[{\"field\":\"d\",\"op\":\"eq\",\"value\":\"20240105\"}]") == c1,
                "eq compact and dashed dates return identical result sets");
    /* brace-wrapped uuid literal matches the stored canonical uuid (M3). */
    ASSERT_TRUE(count_of("[{\"field\":\"u\",\"op\":\"eq\","
                         "\"value\":\"{550e8400-e29b-41d4-a716-446655440000}\"}]") >= 1,
                "brace-wrapped uuid literal matches canonical stored uuid");
    /* bool literals match the right buckets now. */
    ASSERT_TRUE(count_of("[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"true\"}]") >= 1,
                "bool eq true finds true rows");
    ASSERT_TRUE(count_of("[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"TRUE\"}]") >= 1,
                "bool eq TRUE case-insensitive");
    ASSERT_TRUE(count_of("[{\"field\":\"b\",\"op\":\"neq\",\"value\":\"off\"}]") >= 1,
                "bool neq off finds true rows");
    /* between: both forms identical. */
    long b1 = count_of("[{\"field\":\"d\",\"op\":\"between\",\"value\":\"20240105\",\"value2\":\"20240105\"}]");
    long b2 = count_of("[{\"field\":\"d\",\"op\":\"between\",\"value\":\"2024-01-05\",\"value2\":\"2024-01-05\"}]");
    ASSERT_TRUE(b1 >= 0 && b1 == b2, "between compact/ISO identical");
    /* (e) D4: enum unknown literal errors like writes. */
    r = sg_req("{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"sg\","
               "\"criteria\":[{\"field\":\"e\",\"op\":\"eq\",\"value\":\"purple\"}]}");
    ASSERT_TRUE(sg_is_err(r), "D4: enum eq unknown literal errors");
    sg_free(r);
    /* pattern ops: not value-validated, unchanged. */
    ASSERT_TRUE(count_of("[{\"field\":\"d\",\"op\":\"starts\",\"value\":\"2024\"}]") >= 0,
                "starts on date does not error");
    ASSERT_TRUE(count_of("[{\"field\":\"u\",\"op\":\"contains\",\"value\":\"550e\"}]") >= 0,
                "contains on uuid does not error");
    ASSERT_TRUE(count_of("[{\"field\":\"b\",\"op\":\"like\",\"value\":\"t%\"}]") >= 0,
                "like on bool does not error");
}

/* ---- default clauses ----
   Insert-time defaults now flow through the strict converter: valid
   literals apply with their strict values (default=yes now stores TRUE);
   invalid or empty default literals make inserts that rely on them fail
   (existing stored schemas re-read at insert time — declared before R4).
   Declaration-time validation of NEW definitions is Task 5. */
static void sg_defaults(void) {
    char *r;
    r = sg_req("{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"sgd\","
               "\"splits\":8,\"max_key\":32,\"fields\":["
               "\"b2:bool:default=yes\",\"t2:time:default=12:30:45\","
               "\"d2:date:default=2024-01-05\"]}");
    ASSERT_TRUE(!sg_is_err(r), "declaration accepts valid default literals");
    if (sg_is_err(r)) TAP_DIAG("sgd create err: %s", r);
    sg_free(r);
    r = sg_req("{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sgd\",\"key\":\"d1\",\"value\":{}}");
    ASSERT_TRUE(!sg_is_err(r), "defaults insert");
    if (sg_is_err(r)) TAP_DIAG("sgd insert err: %s", r);
    sg_free(r);
    r = sg_req("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sgd\",\"key\":\"d1\"}");
    ASSERT_TRUE(r, "defaults get");
    ASSERT_TRUE(r && strstr(r, "\"b2\":true"), "D6: default=yes now stores true");
    ASSERT_TRUE(r && strstr(r, "\"t2\":\"12:30:45\""), "valid time default applied");
    ASSERT_TRUE(r && strstr(r, "\"d2\":\"20240105\""), "dashed date default stores canonical");
    sg_free(r);
    /* Defaults that are not in the grammar fail at DECLARATION time (the
       create-object wire validates defaults through encode_field_checked).
       Pre-2024-R4 schemas carrying lenient defaults still fail at insert. */
    r = sg_req("{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"sgd2\","
               "\"splits\":8,\"max_key\":32,\"fields\":[\"d3:date:default=2024-1-5\"]}");
    ASSERT_TRUE(sg_is_err(r), "declaration rejects a lenient date default");
    sg_free(r);
    r = sg_req("{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"sgd3\","
               "\"splits\":8,\"max_key\":32,\"fields\":[\"u3:uuid:default=\"]}");
    ASSERT_TRUE(sg_is_err(r), "declaration rejects an empty uuid default");
    sg_free(r);
    /* Unknown enum default is already rejected at declaration time. */
    r = sg_req("{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"sgd4\","
               "\"splits\":8,\"max_key\":32,\"fields\":["
               "\"e2:enum(red,green,blue):default=purple\"]}");
    ASSERT_TRUE(sg_is_err(r) && strstr(r, "default value"),
                "unknown enum default rejected at declaration time");
    sg_free(r);
}

/* ---- cursor/order_by on the date column ----
   (A stored zero date can no longer be produced through checked writes;
   the data-derived out-of-grammar probe path is covered by the junk joins
   below, which exercise the same typed_index_text_key_into contract.) */
static void sg_cursor_zero_date(void) {
    char *r;
    r = sg_req("{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"sg\","
               "\"criteria\":[{\"field\":\"d\",\"op\":\"gte\",\"value\":\"0001-01-01\"}],"
               "\"order_by\":\"d\",\"order\":\"asc\",\"limit\":50}");
    ASSERT_TRUE(!sg_is_err(r), "order_by on date answers");
    if (sg_is_err(r)) TAP_DIAG("order_by err: %s", r);
    sg_free(r);
    r = sg_req("{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"sg\","
               "\"criteria\":[{\"field\":\"d\",\"op\":\"gte\",\"value\":\"0001-01-01\"}],"
               "\"order_by\":\"d\",\"order\":\"asc\",\"limit\":2,\"cursor\":{}}");
    ASSERT_TRUE(!sg_is_err(r), "cursor on date answers");
    if (sg_is_err(r)) TAP_DIAG("cursor err: %s", r);
    sg_free(r);
}

/* ---- join: varchar driver onto remote btree date/uuid ----
   b and e carry bitmap indexes (cannot drive joins — recorded refusal).
   Data-derived probes that are out of grammar mean "no match", never an
   error (hunk 2f): the junk driver row joins nothing. */
static void sg_join(void) {
    char *r;
    r = sg_req("{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"sgj\","
               "\"splits\":8,\"max_key\":32,\"fields\":[\"s:varchar:40\"]}");
    ASSERT_TRUE(!sg_is_err(r), "join driver object");
    sg_free(r);
    r = sg_req("{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"sgj\",\"records\":["
               "{\"key\":\"j1\",\"value\":{\"s\":\"true\"}},"
               "{\"key\":\"j2\",\"value\":{\"s\":\"junk\"}},"
               "{\"key\":\"j3\",\"value\":{\"s\":\"20240105\"}},"
               "{\"key\":\"j4\",\"value\":{\"s\":\"550e8400-e29b-41d4-a716-446655440000\"}}]}");
    ASSERT_TRUE(!sg_is_err(r), "join driver rows");
    if (sg_is_err(r)) TAP_DIAG("join driver err: %s", r);
    sg_free(r);
    r = sg_req("{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"sgj\","
               "\"join\":[{\"object\":\"sg\",\"local\":\"s\",\"remote\":\"b\",\"as\":\"jb\"}],"
               "\"criteria\":[{\"field\":\"s\",\"op\":\"eq\",\"value\":\"true\"}],\"limit\":10}");
    ASSERT_TRUE(sg_is_err(r) && strstr(r, "must be 'key' or indexed"),
                "varchar→bool join refused (bitmap index cannot drive joins)");
    sg_free(r);
    r = sg_req("{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"sgj\","
               "\"join\":[{\"object\":\"sg\",\"local\":\"s\",\"remote\":\"d\",\"as\":\"jd\"}],"
               "\"criteria\":[{\"field\":\"s\",\"op\":\"in\","
               "\"value\":[\"20240105\",\"junk\"]}],\"limit\":10}");
    ASSERT_TRUE(!sg_is_err(r), "join with an out-of-grammar probe does not error (2f)");
    ASSERT_TRUE(r && !strstr(r, "\"junk\""),
                "junk date probe matches nothing (inner join omits the driver row)");
    ASSERT_TRUE(r && strstr(r, "\"20240105\""), "valid date probe still joins");
    sg_free(r);
    r = sg_req("{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"sgj\","
               "\"join\":[{\"object\":\"sg\",\"local\":\"s\",\"remote\":\"u\",\"as\":\"ju\"}],"
               "\"criteria\":[{\"field\":\"s\",\"op\":\"in\","
               "\"value\":[\"550e8400-e29b-41d4-a716-446655440000\",\"junk\"]}],\"limit\":10}");
    ASSERT_TRUE(!sg_is_err(r), "uuid join with junk probe does not error");
    ASSERT_TRUE(r && !strstr(r, "\"junk\""), "junk uuid probe matches nothing");
    ASSERT_TRUE(r && strstr(r, "\"550e8400"), "valid uuid probe still joins");
    sg_free(r);
}

/* ---- (f) indexed vs full-scan parity for bool word literals ----
   sgi (b/e indexed) and sgu (no indexes) hold identical rows: the same
   literal must return the same count through the indexed paths and the
   full-scan per-record path. */
static void sg_indexed_vs_scan(void) {
    char *r;
    r = sg_req("{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"sgi\","
               "\"splits\":8,\"max_key\":32,\"fields\":[\"b:bool\",\"e:enum(red,green,blue)\"],"
               "\"indexes\":[\"b\",\"e\"]}");
    ASSERT_TRUE(!sg_is_err(r), "indexed twin object");
    sg_free(r);
    r = sg_req("{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"sgu\","
               "\"splits\":8,\"max_key\":32,\"fields\":[\"b:bool\",\"e:enum(red,green,blue)\"]}");
    ASSERT_TRUE(!sg_is_err(r), "unindexed twin object");
    sg_free(r);
    for (int t = 0; t < 2; t++) {
        r = sg_req(t == 0
            ? "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"sgi\",\"records\":["
              "{\"key\":\"t\",\"value\":{\"b\":\"true\",\"e\":\"red\"}},"
              "{\"key\":\"f\",\"value\":{\"b\":\"no\",\"e\":\"green\"}}]}"
            : "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"sgu\",\"records\":["
              "{\"key\":\"t\",\"value\":{\"b\":\"true\",\"e\":\"red\"}},"
              "{\"key\":\"f\",\"value\":{\"b\":\"no\",\"e\":\"green\"}}]}");
        ASSERT_TRUE(r && !strstr(r, "some_records_dropped"), "twin rows insert");
        sg_free(r);
    }
    struct { const char *crit; const char *label; } qs[] = {
        {"[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"yes\"}]",  "eq yes"},
        {"[{\"field\":\"b\",\"op\":\"neq\",\"value\":\"yes\"}]", "neq yes"},
        {"[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"off\"}]",  "eq off"},
        {"[{\"field\":\"b\",\"op\":\"neq\",\"value\":\"off\"}]", "neq off"},
        {"[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"TRUE\"}]", "eq TRUE"},
        {"[{\"field\":\"e\",\"op\":\"eq\",\"value\":\"green\"}]", "eq green"},
    };
    for (size_t i = 0; i < sizeof qs / sizeof *qs; i++) {
        ASSERT_EQ_INT(count_on("sgi", qs[i].crit), count_on("sgu", qs[i].crit), qs[i].label);
    }
    ASSERT_EQ_INT(count_on("sgu", "[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"yes\"}]"), 1,
                  "scan eq yes finds exactly the true row");
    ASSERT_EQ_INT(count_on("sgu", "[{\"field\":\"b\",\"op\":\"neq\",\"value\":\"yes\"}]"), 1,
                  "scan neq yes finds exactly the false row");
    ASSERT_EQ_INT(count_on("sgi", "[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"yes\"}]"), 1,
                  "indexed eq yes finds exactly the true row");
    ASSERT_EQ_INT(count_on("sgi", "[{\"field\":\"b\",\"op\":\"neq\",\"value\":\"yes\"}]"), 1,
                  "indexed neq yes finds exactly the false row");
}

/* ---- Task 5: server-generated values stay in-grammar; declaration gates ---- */
static void sg_generated_defaults(void) {
    char *r;
    r = sg_req("{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"sgv\","
               "\"splits\":8,\"max_key\":32,\"fields\":["
               "\"d_ac:date:auto_create\",\"dt_au:datetime:auto_update\","
               "\"ts_ac:timestamp:auto_create\",\"u_ud:uuid:default=uuid()\","
               "\"s_rd:varchar:20:default=random(8)\",\"i_sq:long:default=seq(x)\"]}");
    ASSERT_TRUE(!sg_is_err(r), "generated-default object creates");
    if (sg_is_err(r)) TAP_DIAG("sgv create err: %s", r);
    sg_free(r);
    r = sg_req("{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sgv\",\"key\":\"g1\",\"value\":{}}");
    ASSERT_TRUE(!sg_is_err(r), "generated-default insert");
    if (sg_is_err(r)) TAP_DIAG("sgv insert err: %s", r);
    sg_free(r);
    r = sg_req("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sgv\",\"key\":\"g1\"}");
    ASSERT_NOT_NULL(r, "generated-default get");
    if (r) {
        const char *v;
        char buf[64] = {0};
        int32_t d; uint32_t t;
        v = strstr(r, "\"d_ac\":\"");
        ASSERT_NOT_NULL(v, "date auto_create rendered");
        if (v) {
            strncpy(buf, v + 8, 8); buf[8] = 0;
            ASSERT_EQ_INT(sg_date(buf, 8, &d), 0, "auto_create date round-trips sg_date");
        }
        v = strstr(r, "\"dt_au\":\"");
        ASSERT_NOT_NULL(v, "datetime auto_update rendered");
        if (v) {
            strncpy(buf, v + 9, 14); buf[14] = 0;
            ASSERT_EQ_INT(sg_datetime(buf, 14, &d, &t), 0,
                          "auto_update datetime round-trips sg_datetime");
        }
        v = strstr(r, "\"u_ud\":\"");
        ASSERT_NOT_NULL(v, "uuid() default rendered");
        if (v) {
            uint8_t u[16];
            strncpy(buf, v + 8, 36); buf[36] = 0;
            ASSERT_EQ_INT(sg_uuid(buf, 36, u), 0, "uuid() default round-trips sg_uuid");
        }
        v = strstr(r, "\"s_rd\":\"");
        ASSERT_NOT_NULL(v, "random(8) default rendered");
        if (v) {
            const char *c = v + 8;
            int n = 0;
            while (c[n] && c[n] != '"') n++;
            ASSERT_EQ_INT(n, 16, "random(8) hex-encodes to 16 chars");
        }
        v = strstr(r, "\"i_sq\":");
        ASSERT_NOT_NULL(v, "seq default rendered");
        if (v) ASSERT_TRUE(v[7] >= '0' && v[7] <= '9', "seq default numeric");
    }
    sg_free(r);
    /* add-field backfill route: a valid default applies to new writes. */
    r = sg_req("{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"sgv\","
               "\"fields\":[\"d_bf:date:default=2024-01-05\"]}");
    ASSERT_TRUE(!sg_is_err(r), "add-field with valid date default");
    if (sg_is_err(r)) TAP_DIAG("add-field ok-route err: %s", r);
    sg_free(r);
    r = sg_req("{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sgv\",\"key\":\"g2\",\"value\":{}}");
    ASSERT_TRUE(!sg_is_err(r), "insert after add-field");
    sg_free(r);
    r = sg_req("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sgv\",\"key\":\"g2\"}");
    ASSERT_TRUE(r && strstr(r, "\"d_bf\":\"20240105\""),
                "add-field default applies with canonical bytes");
    sg_free(r);
    /* Declaration gates: invalid default literals must fail at declaration. */
    r = sg_req("{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"sgv\","
               "\"fields\":[\"d_bad:date:default=2024-02-30\"]}");
    ASSERT_TRUE(sg_is_err(r), "add-field rejects an invalid calendar default");
    if (r && !sg_is_err(r)) TAP_DIAG("add-field bad default resp: %s", r);
    sg_free(r);
    r = sg_req("{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"sgv2\","
               "\"splits\":8,\"max_key\":32,\"fields\":[\"d:date:default=2024-02-30\"]}");
    ASSERT_TRUE(sg_is_err(r), "create-object rejects an invalid calendar default");
    sg_free(r);
    r = sg_req("{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"sgv\","
               "\"fields\":[\"u_bad:uuid:default=not-a-uuid\"]}");
    ASSERT_TRUE(sg_is_err(r), "add-field rejects a malformed uuid default");
    sg_free(r);
    /* edit-field same-type + vacuum do not reinterpret stored bytes. */
    r = sg_req("{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"sgv\","
               "\"fields\":[\"s_rd:varchar:20:default=random(8)\"]}");
    ASSERT_TRUE(!sg_is_err(r), "edit-field same-type declaration");
    if (sg_is_err(r)) TAP_DIAG("edit-field err: %s", r);
    sg_free(r);
    r = sg_req("{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"sgv\"}");
    ASSERT_TRUE(!sg_is_err(r), "vacuum runs");
    sg_free(r);
    r = sg_req("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sgv\",\"key\":\"g1\"}");
    ASSERT_TRUE(r && strstr(r, "\"u_ud\":\"") && strstr(r, "\"s_rd\":\""),
                "vacuum preserves generated values");
    sg_free(r);
}

/* ---- Task 7: consolidated route matrix ----
   For every type: the same invalid input is rejected on insert, update,
   bulk-insert JSON, bulk delimited (non-empty bad cell), default
   declaration and find criterion; valid forms store identical bytes on
   every write route; explicit null stays NULL; zeros, empty varchar and
   numeric zero round-trip as real values. */
typedef struct {
    const char *field, *type, *bad, *bad_csv, *frag, *good, *good_csv, *render;
    int crit_reject; /* varchar criteria stay unvalidated (bounds are a
                        write-side contract; overlength literals match
                        nothing without erroring). */
} RtRow;
static const RtRow RT[] = {
    {"b","bool","\"10\"","10","not a valid bool","\"off\"","off","\"b\":false",1},
    {"d","date","\"2024-02-30\"","2024-02-30","not a valid date","\"2024-01-05\"","2024-01-05","\"d\":\"20240105\"",1},
    {"dt","datetime","\"20240105\"","20240105","not a valid datetime","\"2024-01-05T01:02:03\"","2024-01-05T01:02:03","\"dt\":\"20240105010203\"",1},
    {"dm","datetimems","\"2024-01-05T01:02:03.12\"","2024-01-05T01:02:03.12","not a valid datetimems","\"20240105010203004\"","20240105010203004","\"dm\":\"20240105010203004\"",1},
    {"t","time","\"24:00:00\"","24:00:00","not a valid time","\"12:30:45\"","12:30:45","\"t\":\"12:30:45\"",1},
    {"u","uuid","\"not-a-uuid\"","not-a-uuid","not a valid uuid",
     "\"{550e8400-e29b-41d4-a716-446655440000}\"","550e8400-e29b-41d4-a716-446655440000",
     "\"u\":\"550e8400-e29b-41d4-a716-446655440000\"",1},
    {"i4","ipv4","\"999.1.1.1\"","999.1.1.1","not a valid ipv4","\"10.0.0.1\"","10.0.0.1","\"i4\":\"10.0.0.1\"",1},
    {"i6","ipv6","\"::g\"","::g","not a valid ipv6","\"::1\"","::1","\"i6\":\"::1\"",1},
    {"e","enum(red,green,blue)","\"purple\"","purple","not a valid enum","\"green\"","green","\"e\":\"green\"",1},
    {"s","varchar:8","\"123456789\"","123456789","exceeds max","\"hi\"","hi","\"s\":\"hi\"",0},
    {"n","numeric:12,2","\"abc\"","abc","not a valid numeric","\"0\"","0","\"n\":0",1},
};
static const char *const RT_CSV_FILLER[11] = {
    "true","20240105","20240105010203","20240105010203004","12:30:45",
    "550e8400-e29b-41d4-a716-446655440000","10.0.0.1","::1","green","hi","0"
};

static void sg_route_matrix(void) {
    char req[1024], *r;
    r = sg_req("{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"sgm\","
               "\"splits\":8,\"max_key\":32,\"fields\":["
               "\"b:bool\",\"d:date\",\"dt:datetime\",\"dm:datetimems\",\"t:time\","
               "\"u:uuid\",\"i4:ipv4\",\"i6:ipv6\",\"e:enum(red,green,blue)\","
               "\"s:varchar:8\",\"n:numeric:12,2\"]}");
    ASSERT_TRUE(!sg_is_err(r), "matrix object creates");
    sg_free(r);
    /* anchor row for the update-route OLD-intact checks */
    r = sg_req("{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sgm\","
               "\"key\":\"m_up\",\"value\":{\"d\":\"20240105\"}}");
    ASSERT_TRUE(!sg_is_err(r), "matrix anchor row");
    sg_free(r);
    char bad_row[512];
    for (size_t i = 0; i < sizeof RT / sizeof *RT; i++) {
        const RtRow *w = &RT[i];
        /* 1. insert rejects, record absent. */
        snprintf(req, sizeof req,
                 "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sgm\","
                 "\"key\":\"m_i\",\"value\":{\"%s\":%s}}", w->field, w->bad);
        r = sg_req(req);
        ASSERT_TRUE(sg_is_err(r) && strstr(r, w->frag), w->field);
        sg_free(r);
        snprintf(req, sizeof req,
                 "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"sgm\",\"key\":\"m_i\"}");
        r = sg_req(req);
        ASSERT_TRUE(r && strstr(r, "false"), w->field);
        sg_free(r);
        /* 2. update rejects, OLD intact. */
        snprintf(req, sizeof req,
                 "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"sgm\","
                 "\"key\":\"m_up\",\"value\":{\"%s\":%s}}", w->field, w->bad);
        r = sg_req(req);
        ASSERT_TRUE(sg_is_err(r) && strstr(r, w->frag), w->field);
        sg_free(r);
        r = sg_req("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sgm\",\"key\":\"m_up\"}");
        ASSERT_TRUE(r && strstr(r, "\"d\":\"20240105\""), w->field);
        sg_free(r);
        /* 3. bulk-insert JSON drops and counts. */
        snprintf(req, sizeof req,
                 "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"sgm\","
                 "\"records\":[{\"key\":\"m_j\",\"value\":{\"%s\":%s}}]}",
                 w->field, w->bad);
        r = sg_req(req);
        ASSERT_TRUE(r && strstr(r, "\"errors\":1"), w->field);
        sg_free(r);
        snprintf(req, sizeof req,
                 "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"sgm\",\"key\":\"m_j\"}");
        r = sg_req(req);
        ASSERT_TRUE(r && strstr(r, "false"), w->field);
        sg_free(r);
        /* 4. bulk delimited: bad cell at the field's position drops the row. */
        {
            char cells[11][64];
            for (int j = 0; j < 11; j++)
                snprintf(cells[j], sizeof cells[j], "%s",
                         (int)j == (int)i ? w->bad_csv : RT_CSV_FILLER[j]);
            snprintf(bad_row, sizeof bad_row,
                     "m_c_%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                     w->field,
                     cells[0], cells[1], cells[2], cells[3], cells[4],
                     cells[5], cells[6], cells[7], cells[8], cells[9], cells[10]);
            snprintf(req, sizeof req,
                     "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\","
                     "\"object\":\"sgm\",\"delimiter\":\",\",\"data\":\"%s\\n\"}",
                     bad_row);
            r = sg_req(req);
            ASSERT_TRUE(r && strstr(r, "\"errors\":1"), w->field);
            sg_free(r);
            snprintf(req, sizeof req,
                     "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"sgm\",\"key\":\"m_c_%s\"}",
                     w->field);
            r = sg_req(req);
            ASSERT_TRUE(r && strstr(r, "false"), w->field);
            sg_free(r);
        }
        /* 5. default declaration rejects the same literal. */
        snprintf(req, sizeof req,
                 "{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"sgm_x\","
                 "\"splits\":8,\"max_key\":32,\"fields\":[\"f:%s:default=%s\"]}",
                 w->type, w->bad);
        r = sg_req(req);
        ASSERT_TRUE(sg_is_err(r), w->field);
        sg_free(r);
        /* 6. find criterion rejects (generic criteria error). */
        snprintf(req, sizeof req,
                 "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"sgm\","
                 "\"criteria\":[{\"field\":\"%s\",\"op\":\"eq\",\"value\":%s}]}",
                 w->field, w->bad);
        r = sg_req(req);
        ASSERT_TRUE(sg_is_err(r) || !w->crit_reject, w->field);
        sg_free(r);
        /* 7-10. the valid form stores identical bytes on every write route. */
        snprintf(req, sizeof req,
                 "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sgm\","
                 "\"key\":\"m_vi\",\"value\":{\"%s\":%s}}", w->field, w->good);
        r = sg_req(req);
        ASSERT_TRUE(!sg_is_err(r), w->field);
        sg_free(r);
        r = sg_req("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sgm\",\"key\":\"m_vi\"}");
        ASSERT_TRUE(r && strstr(r, w->render), w->field);
        sg_free(r);
        snprintf(req, sizeof req,
                 "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"sgm\","
                 "\"key\":\"m_vi\",\"value\":{\"%s\":null}}", w->field);
        r = sg_req(req);
        ASSERT_TRUE(!sg_is_err(r), w->field);
        sg_free(r);
        snprintf(req, sizeof req,
                 "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"sgm\","
                 "\"records\":[{\"key\":\"m_vj_%s\",\"value\":{\"%s\":%s}}]}",
                 w->field, w->field, w->good);
        r = sg_req(req);
        ASSERT_TRUE(r && strstr(r, "\"inserted\":1"), w->field);
        sg_free(r);
        snprintf(req, sizeof req,
                 "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sgm\",\"key\":\"m_vj_%s\"}",
                 w->field);
        r = sg_req(req);
        ASSERT_TRUE(r && strstr(r, w->render), w->field);
        sg_free(r);
        {
            char cells[11][64];
            for (int j = 0; j < 11; j++)
                snprintf(cells[j], sizeof cells[j], "%s",
                         (int)j == (int)i ? w->good_csv : RT_CSV_FILLER[j]);
            snprintf(bad_row, sizeof bad_row,
                     "m_vc_%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                     w->field,
                     cells[0], cells[1], cells[2], cells[3], cells[4],
                     cells[5], cells[6], cells[7], cells[8], cells[9], cells[10]);
            snprintf(req, sizeof req,
                     "{\"mode\":\"bulk-insert-delimited\",\"dir\":\"default\","
                     "\"object\":\"sgm\",\"delimiter\":\",\",\"data\":\"%s\\n\"}",
                     bad_row);
            r = sg_req(req);
            ASSERT_TRUE(r && strstr(r, "\"inserted\":1"), w->field);
            sg_free(r);
            snprintf(req, sizeof req,
                     "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sgm\",\"key\":\"m_vc_%s\"}",
                     w->field);
            r = sg_req(req);
            ASSERT_TRUE(r && strstr(r, w->render), w->field);
            sg_free(r);
        }
    }
    /* null / zero invariants on the matrix object. */
    r = sg_req("{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"sgm\","
               "\"key\":\"m_z\",\"value\":{\"u\":\"00000000-0000-0000-0000-000000000000\","
               "\"i4\":\"0.0.0.0\",\"i6\":\"::\",\"t\":\"00:00:00\",\"s\":\"\",\"n\":\"0\","
               "\"d\":null}}");
    ASSERT_TRUE(!sg_is_err(r), "zeros insert");
    sg_free(r);
    r = sg_req("{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"sgm\",\"key\":\"m_z\"}");
    ASSERT_TRUE(r &&
                strstr(r, "\"u\":\"00000000-0000-0000-0000-000000000000\"") &&
                strstr(r, "\"i4\":\"0.0.0.0\"") && strstr(r, "\"i6\":\"::\"") &&
                strstr(r, "\"t\":\"00:00:00\"") && strstr(r, "\"s\":\"\"") &&
                strstr(r, "\"n\":0") && strstr(r, "\"d\":null"),
                "zeros round-trip as real values; explicit null stays NULL");
    sg_free(r);
    tu_pdb_drop_object(sg_db, "default", "sgm");
}

/* ---- packed layout (R1 codec, low-level): same sg field list ---- */
static void sg_packed(void) {
    static const char *types[] = {
        "bool", "date", "datetime", "datetimems", "time",
        "uuid", "ipv4", "ipv6", "enum(red,green,blue)", "varchar:8"
    };
    TypedSchema *ts = calloc(1, sizeof(*ts));
    ASSERT_NOT_NULL(ts, "packed schema alloc");
    if (!ts) return;
    ts->typed = 1; ts->nfields = 10;
    static const char *names[] = {"b","d","dt","dm","t","u","i4","i6","e","s"};
    for (int i = 0; i < 10; i++) {
        parse_field_type(types[i], &ts->fields[i]);
        snprintf(ts->fields[i].name, sizeof ts->fields[i].name, "%s", names[i]);
        ts->fields[i].name_len = (int)strlen(names[i]);
    }
    char perr[256] = {0};
    int rc = typed_schema_bind_packed(ts, types, 10, perr, sizeof perr);
    ASSERT_EQ_INT(rc, 0, "bind packed sg schema");
    if (rc) TAP_DIAG("packed bind err: %s", perr);
    struct { const char *json, *want_err, *want; const char *label; } cells[] = {
        {"{\"b\":\"true\",\"d\":\"20240105\",\"u\":\"550e8400-e29b-41d4-a716-446655440000\",\"e\":\"green\",\"s\":\"hi\"}",
         NULL, "\"d\":\"20240105\"", "packed valid row encodes"},
        {"{\"u\":\"not-a-uuid\"}", "not a valid uuid", NULL,
         "packed malformed uuid errors on a nullable field (sentinel removed)"},
        {"{\"i4\":\"999.1.1.1\"}", "not a valid ipv4", NULL,
         "packed malformed ipv4 errors"},
        {"{\"e\":\"purple\"}", "not a valid enum", NULL,
         "packed unknown enum errors with the unified grammar message"},
        {"{\"d\":\"\"}", "not a valid date", NULL, "packed empty date errors"},
        {"{\"u\":\"{550e8400-e29b-41d4-a716-446655440000}\"}", NULL,
         "\"u\":\"550e8400-e29b-41d4-a716-446655440000\"",
         "packed braced uuid accepted canonical"},
        {"{\"i4\":\"0.0.0.0\",\"i6\":\"::\",\"t\":\"00:00:00\"}", NULL,
         "\"i4\":\"0.0.0.0\"", "packed zeros stay real values"},
        {"{\"s\":\"a\\u0000b\"}", NULL, "\"s\":\"a\\u0000b\"",
         "packed varchar keeps decoded NUL (D7 unchanged)"},
    };
    for (size_t i = 0; i < sizeof cells / sizeof *cells; i++) {
        uint8_t *bytes = NULL; size_t n = 0; char err[256] = {0};
        rc = typed_encode_alloc(ts, cells[i].json, 1, NULL, NULL,
                                &bytes, &n, err, sizeof err);
        if (cells[i].want_err) {
            ASSERT_TRUE(rc != 0, cells[i].label);
            ASSERT_TRUE(cells[i].want_err[0] == 0 || strstr(err, cells[i].want_err) != NULL,
                        cells[i].label);
        } else {
            ASSERT_EQ_INT(rc, 0, cells[i].label);
            if (rc == 0) {
                char *json = typed_packed_decode(ts, bytes, n,
                                                 typed_schema_hdr2(ts), 64 * 1024);
                ASSERT_NOT_NULL(json, cells[i].label);
                if (json) {
                    ASSERT_CONTAINS(json, cells[i].want, cells[i].label);
                    free(json);
                }
            }
        }
        free(bytes);
    }
    /* parse_field_type allocated the enum values ("red/green/blue" names +
       array); release them like every other packed fixture teardown, or the
       ASan gate reports a 39-byte leak (previously masked from LSan's
       conservative stack scan by a stale frame pointer). */
    for (int i = 0; i < ts->nfields; i++) free_enum_values(&ts->fields[i]);
    free(ts);
}

static int test_scalar_grammar_routes_run(void) {
    sg_db = test_get_process_db();
    ASSERT_NOT_NULL(sg_db, "process db");
    if (!sg_db) return 1;
    char *r = sg_req("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    sg_free(r);
    r = sg_req("{\"mode\":\"create-object\",\"declaration_layout\":\"legacy\",\"dir\":\"default\",\"object\":\"sg\","
               "\"splits\":8,\"max_key\":32,\"fields\":["
               "\"b:bool\",\"d:date\",\"dt:datetime\",\"dm:datetimems\",\"t:time\","
               "\"u:uuid\",\"i4:ipv4\",\"i6:ipv6\",\"e:enum(red,green,blue)\",\"s:varchar:8\"],"
               "\"indexes\":[\"b\",\"d\",\"dt\",\"dm\",\"t\",\"u\",\"i4\",\"i6\",\"e\"]}");
    ASSERT_TRUE(!sg_is_err(r), "create sg");
    sg_free(r);

    sg_insert_matrix();
    sg_update_route();
    sg_bulk_routes();
    sg_criteria();
    sg_defaults();
    sg_cursor_zero_date();
    sg_join();
    sg_indexed_vs_scan();
    sg_generated_defaults();
    sg_route_matrix();
    sg_packed();

    /* Sequential-run isolation: remove what we created. */
    tu_pdb_drop_object(sg_db, "default", "sg");
    tu_pdb_drop_object(sg_db, "default", "sgd");
    tu_pdb_drop_object(sg_db, "default", "sgd2");
    tu_pdb_drop_object(sg_db, "default", "sgj");
    tu_pdb_drop_object(sg_db, "default", "sgi");
    tu_pdb_drop_object(sg_db, "default", "sgu");
    tu_pdb_drop_object(sg_db, "default", "sgv");
    tu_pdb_drop_object(sg_db, "default", "sgv2");
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-scalar-grammar-routes", test_scalar_grammar_routes_run)
