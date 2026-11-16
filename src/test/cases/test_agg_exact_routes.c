/* src/test/cases/test_agg_exact_routes.c
 *
 * R3 legacy-object route matrix over the request layer. Three identical
 * objects (all-indexed splits 8, unindexed, all-indexed splits 64) prove
 * route/layout parity for exact sums above 2^53, declared-scale decimals,
 * timestamp groups, empty-input avg, and the CSV renderer. The limits
 * registration covers having on adjacent values, the D6 key-limit error,
 * bounded-memory rejection and the allocation-fault loop.
 */
#include "test_runner.h"
#include "bappend.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/shard_db.h"
#include "../db/agg_exact.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static ShardDb *ar_db;
static char *ar_req(const char *json) {
    char *r = NULL;
    tu_pdb_request(ar_db, json, &r);
    return r;
}
static void ar_chomp(char *s) {
    size_t n = s ? strlen(s) : 0;
    while (n && (s[n - 1] == '\n' || s[n - 1] == ' ')) s[--n] = 0;
}
/* Exact response comparison (trailing newline ignored). */
static void ar_expect(const char *obj, const char *extra, const char *aggs,
                      const char *expect, const char *msg) {
    char req[2048];
    snprintf(req, sizeof req,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
        "\"aggregates\":[%s]%s}", obj, aggs, extra);
    char *r = ar_req(req);
    ASSERT_NOT_NULL(r, msg);
    if (!r) return;
    ar_chomp(r);
    ASSERT_EQ_STR(r, expect, msg);
    shard_db_free_result(r);
}

static void ar_make(const char *obj, int splits, int indexed) {
    char req[2048];
    snprintf(req, sizeof req,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":%d,\"max_key\":16,\"fields\":[\"l:long\",\"n:numeric:19,4\","
        "\"ts:timestamp\",\"d:double\",\"g:varchar:16\"]%s}", obj, splits,
        indexed ? ",\"indexes\":[\"l\",\"n\",\"ts\",\"d\",\"g\"]" : "");
    char *r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "object created");
    shard_db_free_result(r);
    snprintf(req, sizeof req,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"g\":\"a\",\"l\":9007199254740993,\"n\":\"0.1000\",\"ts\":1490833334666,\"d\":0.5}},"
        "{\"key\":\"k2\",\"value\":{\"g\":\"a\",\"l\":9007199254740993,\"n\":\"0.2000\",\"ts\":1490833334667,\"d\":0.25}},"
        "{\"key\":\"k3\",\"value\":{\"g\":\"b\",\"l\":9223372036854775807,\"n\":\"1.5000\",\"ts\":1490833334667,\"d\":1.0}},"
        "{\"key\":\"k4\",\"value\":{\"g\":\"b\",\"l\":-9223372036854775807,\"n\":\"-1.5000\",\"ts\":1000,\"d\":2.0}},"
        "{\"key\":\"k5\",\"value\":{\"g\":\"b\",\"l\":1,\"n\":null,\"ts\":2000,\"d\":null}},"
        "{\"key\":\"k6\",\"value\":{\"g\":\"c\",\"l\":null,\"n\":null,\"ts\":3000,\"d\":null}}]}", obj);
    r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "rows inserted");
    shard_db_free_result(r);
}

#define AR_S(fn, f, a) "{\"fn\":\"" fn "\",\"field\":\"" f "\",\"alias\":\"" a "\"}"
static void ar_matrix(const char *obj) {
    /* A3/A4: whole-object scalars (leaf-only bundle on indexed objects,
       hash scan otherwise). Oracle goldens: see docs/plans R3 §goldens. */
    ar_expect(obj, "",
        AR_S("sum","l","s") "," AR_S("avg","l","a") "," AR_S("min","l","mn") ","
        AR_S("max","l","mx") "," AR_S("sum","n","sn") "," AR_S("avg","n","an") ","
        AR_S("min","n","mnn") "," AR_S("max","n","mxn") "," AR_S("avg","ts","at") ","
        AR_S("sum","d","sd") "," AR_S("avg","d","ad"),
        "{\"s\":18014398509481987,\"a\":3602879701896397.4000,"
        "\"mn\":-9223372036854775807,\"mx\":9223372036854775807,"
        "\"sn\":0.3000,\"an\":0.07500000000000000000,\"mnn\":-1.5000,\"mxn\":1.5000,"
        "\"at\":745416668333.33333333,\"sd\":3.75,\"ad\":0.9375}", obj);
    /* group_by text key, grouped sums (exact, scaled, empty-set shapes) */
    ar_expect(obj, ",\"group_by\":[\"g\"],\"order_by\":\"g\"",
        "{\"fn\":\"count\",\"alias\":\"c\"}," AR_S("sum","l","s") "," AR_S("sum","n","sn"),
        "[{\"g\":\"a\",\"c\":2,\"s\":18014398509481986,\"sn\":0.3000},"
        "{\"g\":\"b\",\"c\":3,\"s\":1,\"sn\":0.0000},"
        "{\"g\":\"c\",\"c\":1,\"s\":0,\"sn\":0.0000}]", obj);
    /* A6/D5: timestamp group labels and numeric ordering */
    ar_expect(obj, ",\"group_by\":[\"ts\"],\"order_by\":\"ts\"",
        "{\"fn\":\"count\",\"alias\":\"c\"}",
        "[{\"ts\":\"1000\",\"c\":1},{\"ts\":\"2000\",\"c\":1},{\"ts\":\"3000\",\"c\":1},"
        "{\"ts\":\"1490833334666\",\"c\":1},{\"ts\":\"1490833334667\",\"c\":2}]", obj);
    /* A8: avg of no contributing value is null on every JSON route */
    ar_expect(obj, ",\"criteria\":{\"field\":\"ts\",\"op\":\"eq\",\"value\":3000}",
        AR_S("avg","l","a"), "{\"a\":null}", obj);
    /* single-spec extremes: same-field, keyset and walk-fetch-check routes */
    ar_expect(obj, ",\"criteria\":{\"field\":\"l\",\"op\":\"lte\",\"value\":\"9007199254740993\"}",
        AR_S("max","l","m"), "{\"m\":9007199254740993}", obj);
    ar_expect(obj, ",\"criteria\":{\"field\":\"ts\",\"op\":\"lt\",\"value\":1490833334668}",
        AR_S("max","l","m"), "{\"m\":9223372036854775807}", obj);
    ar_expect(obj, ",\"criteria\":{\"field\":\"ts\",\"op\":\"gte\",\"value\":0}",
        AR_S("sum","l","s") "," AR_S("sum","n","sn"),
        "{\"s\":18014398509481987,\"sn\":0.3000}", obj);
    /* intersect keyset extreme: AND of two indexed leaves */
    ar_expect(obj, ",\"criteria\":{\"and\":[{\"field\":\"g\",\"op\":\"eq\",\"value\":\"b\"},"
                   "{\"field\":\"ts\",\"op\":\"lt\",\"value\":1490833334668}]}",
        AR_S("min","l","mn") "," AR_S("max","n","mx"),
        "{\"mn\":-9223372036854775807,\"mx\":1.5000}", obj);
    /* CSV shares the renderer (D3: empty avg is the CSV cell "0") */
    ar_expect(obj, ",\"format\":\"csv\"", AR_S("sum","l","s") "," AR_S("sum","n","sn"),
        "s,sn\n18014398509481987,0.3000", obj);
    ar_expect(obj, ",\"format\":\"csv\",\"criteria\":{\"field\":\"ts\",\"op\":\"eq\",\"value\":3000}",
        AR_S("avg","l","a"), "a\n0", obj);
    /* rows+total wrapper */
    ar_expect(obj, ",\"total\":true", AR_S("sum","l","s"),
        "{\"rows\":{\"s\":18014398509481987},\"total\":1}", obj);
    /* count(field) scan parity on the criteria-bundled route: NULLs excluded */
    ar_expect(obj, ",\"criteria\":{\"field\":\"ts\",\"op\":\"gte\",\"value\":0}",
        "{\"fn\":\"count\",\"field\":\"l\",\"alias\":\"c\"},"
        "{\"fn\":\"count\",\"field\":\"n\",\"alias\":\"cn\"},"
        "{\"fn\":\"count\",\"alias\":\"ct\"}",
        "{\"c\":5,\"cn\":4,\"ct\":6}", obj);
    /* Same field has SUM + COUNT; do not overwrite its walked count. */
    ar_expect(obj, ",\"criteria\":{\"field\":\"ts\",\"op\":\"gte\",\"value\":0}",
        AR_S("sum","l","s") "," AR_S("count","l","c") ","
        AR_S("count","n","cn") ",{\"fn\":\"count\",\"alias\":\"ct\"}",
        "{\"s\":18014398509481987,\"c\":5,\"cn\":4,\"ct\":6}", obj);
}

static int ar_run(void) {
    ar_db = test_get_process_db();
    ASSERT_NOT_NULL(ar_db, "process db");
    if (!ar_db) return 1;
    char *r = ar_req("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);
    ar_make("r3l_i", 8, 1);
    ar_make("r3l_s", 8, 0);
    ar_make("r3l_i64", 64, 1);
    ar_matrix("r3l_i");
    ar_matrix("r3l_s");
    ar_matrix("r3l_i64");
    tu_pdb_drop_object(ar_db, "default", "r3l_i");
    tu_pdb_drop_object(ar_db, "default", "r3l_s");
    tu_pdb_drop_object(ar_db, "default", "r3l_i64");
    return 0;
}
TEST_REGISTER("test-agg-exact-routes", ar_run)

/* Having / order_by on adjacent values above 2^53, long group keys, budget
   and allocation-fault behaviour. */
static void ar_adjacent(const char *obj, int indexed) {
    char req[1024];
    snprintf(req, sizeof req,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\",\"splits\":8,"
        "\"max_key\":16,\"fields\":[\"l:long\",\"w:long\"]%s}", obj,
        indexed ? ",\"indexes\":[\"l\",\"w\"]" : "");
    char *r = ar_req(req); shard_db_free_result(r);
    snprintf(req, sizeof req,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":["
        "{\"key\":\"a\",\"value\":{\"l\":9007199254740992,\"w\":1}},"
        "{\"key\":\"b\",\"value\":{\"l\":9007199254740993,\"w\":2}}]}", obj);
    r = ar_req(req); shard_db_free_result(r);
    ar_expect(obj, ",\"group_by\":[\"w\"],\"order_by\":\"w\","
        "\"having\":{\"field\":\"s\",\"op\":\"eq\",\"value\":\"9007199254740993\"}",
        AR_S("sum","l","s"), "[{\"w\":\"2\",\"s\":9007199254740993}]", obj);
    ar_expect(obj, ",\"group_by\":[\"w\"],\"order_by\":\"s\",\"order\":\"desc\"",
        AR_S("sum","l","s"),
        "[{\"w\":\"2\",\"s\":9007199254740993},{\"w\":\"1\",\"s\":9007199254740992}]", obj);
    ar_expect(obj, ",\"group_by\":[\"l\"],\"order_by\":\"l\",\"order\":\"desc\","
        "\"having\":{\"field\":\"l\",\"op\":\"gt\",\"value\":\"9007199254740992\"}",
        "{\"fn\":\"count\",\"alias\":\"c\"}", "[{\"l\":\"9007199254740993\",\"c\":1}]", obj);
    ar_expect(obj, ",\"group_by\":[\"w\"],\"having\":{\"field\":\"s\",\"op\":\"gt\",\"value\":\"abc\"}",
        AR_S("sum","l","s"),
        "{\"error\":\"invalid numeric value 'abc' for having on 's'\"}", obj);
    tu_pdb_drop_object(ar_db, "default", obj);
}

static void ar_longkey(const char *obj, int indexed) {
    char req[4096], a[700], b[700];
    size_t len = indexed ? 520 : 650; /* indexed payload stays below BT_MAX_VAL_LEN */
    memset(a, 'a', len); a[len] = 0;
    memcpy(b, a, len + 1); b[len - 1] = 'b';
    snprintf(req, sizeof req,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\",\"splits\":8,"
        "\"max_key\":16,\"fields\":[\"v:varchar:2000\"]%s}", obj,
        indexed ? ",\"indexes\":[\"v\"]" : "");
    char *r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "long-key object created"); shard_db_free_result(r);
    snprintf(req, sizeof req,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":["
        "{\"key\":\"p\",\"value\":{\"v\":\"%s\"}},{\"key\":\"q\",\"value\":{\"v\":\"%s\"}}]}",
        obj, a, b);
    r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "long-key rows inserted"); shard_db_free_result(r);
    ar_expect(obj, ",\"group_by\":[\"v\"]", "{\"fn\":\"count\",\"alias\":\"c\"}",
        "{\"error\":\"group_by value exceeds aggregate key limit (511 bytes per value, 4096 bytes per composite key) or contains NUL\"}", obj);
    tu_pdb_drop_object(ar_db, "default", obj);
}


/* Nine valid 500-byte components overflow only the composite 4096-byte key. */
static void ar_combined_key(void) {
    char req[12000], value[501], groups[256]; size_t o = 0, g = 0;
    memset(value, 'x', 500); value[500] = 0;
    o = tappendf(req, sizeof req, o,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3combo\","
        "\"splits\":8,\"max_key\":16,\"fields\":[");
    for (int i = 0; i < 9; i++)
        o = tappendf(req, sizeof req, o, "%s\"v%d:varchar:500\"", i ? "," : "", i);
    snprintf(req + o, sizeof req - o, "]}");
    char *r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "combined-key fixture created"); shard_db_free_result(r);
    o = (size_t)snprintf(req, sizeof req,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"r3combo\",\"key\":\"k\",\"value\":{");
    for (int i = 0; i < 9; i++)
        o = tappendf(req, sizeof req, o, "%s\"v%d\":\"%s\"", i ? "," : "", i, value);
    snprintf(req + o, sizeof req - o, "}}");
    r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "combined-key row inserted"); shard_db_free_result(r);
    g = tappendf(groups, sizeof groups, g, ",\"group_by\":[");
    for (int i = 0; i < 9; i++)
        g = tappendf(groups, sizeof groups, g, "%s\"v%d\"", i ? "," : "", i);
    snprintf(groups + g, sizeof groups - g, "]");
    ar_expect("r3combo", groups, "{\"fn\":\"count\",\"alias\":\"c\"}",
        "{\"error\":\"group_by value exceeds aggregate key limit (511 bytes per value, 4096 bytes per composite key) or contains NUL\"}",
        "combined key rejects instead of truncating");
    tu_pdb_drop_object(ar_db, "default", "r3combo");
}

static void ar_large_float(const char *obj, int indexed) {
    char req[1024];
    snprintf(req, sizeof req,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":64,\"max_key\":16,\"fields\":[\"d:double\"]%s}",
        obj, indexed ? ",\"indexes\":[\"d\"]" : "");
    char *r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "float fixture created"); shard_db_free_result(r);
    snprintf(req, sizeof req,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":["
        "{\"key\":\"a\",\"value\":{\"d\":1e308}},{\"key\":\"b\",\"value\":{\"d\":1e308}}]}", obj);
    r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "finite float rows inserted"); shard_db_free_result(r);
    ar_expect(obj, "", AR_S("count","d","c"), "{\"c\":2}", "finite float count");
    ar_expect(obj, "", AR_S("min","d","mn") "," AR_S("max","d","mx"),
        "{\"mn\":1e+308,\"mx\":1e+308}", "finite extrema survive unused sum overflow");
    ar_expect(obj, "", AR_S("sum","d","s"),
        "{\"error\":\"aggregate result out of range\"}", "requested overflowing sum errors");
    ar_expect(obj, "", AR_S("avg","d","a"),
        "{\"error\":\"aggregate result out of range\"}", "requested overflowing avg errors");
    tu_pdb_drop_object(ar_db, "default", obj);
}

static void ar_full_text(void) {
    char a[1101], b[1101], req[4096], expected[2300];
    memset(a, 'a', 1100); a[1100] = 0;
    memcpy(b, a, sizeof b); b[1050] = 'b';
    char *r = ar_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3text\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:2000\"]}");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "text fixture created"); shard_db_free_result(r);
    snprintf(req, sizeof req,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"r3text\",\"records\":["
        "{\"key\":\"a\",\"value\":{\"v\":\"%s\"}},{\"key\":\"b\",\"value\":{\"v\":\"%s\"}}]}", a, b);
    r = ar_req(req);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "full text rows inserted"); shard_db_free_result(r);
    snprintf(expected, sizeof expected, "{\"mn\":\"%s\",\"mx\":\"%s\"}", a, b);
    ar_expect("r3text", "", AR_S("min","v","mn") "," AR_S("max","v","mx"), expected,
        "full text extrema differ beyond byte 1023");
    tu_pdb_drop_object(ar_db, "default", "r3text");
}

/* Weak only during Task-3 legacy RED; Task-4 verification requires this hook. */
extern void agg_test_fail_cells_alloc(int) __attribute__((weak));
extern void agg_test_fail_escape_alloc(int) __attribute__((weak));
static void ar_fault_restore(int *unused) {
    (void)unused;
    agg_exact_test_set_fail_alloc(0);
    if (agg_test_fail_cells_alloc) agg_test_fail_cells_alloc(0);
    if (agg_test_fail_escape_alloc) agg_test_fail_escape_alloc(0);
}

typedef struct { ShardDb *db; size_t cap; } ArBudgetSave;
static void ar_budget_restore(ArBudgetSave *saved) {
    saved->db->query_buffer_max_bytes = saved->cap;
}

static int ar_limits_run(void) {
    int fault_guard __attribute__((cleanup(ar_fault_restore))) = 0;
    ar_db = test_get_process_db();
    ASSERT_NOT_NULL(ar_db, "process db");
    if (!ar_db) return 1;
    char *r = ar_req("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);
    ar_adjacent("r3adj_i", 1);
    ar_adjacent("r3adj_s", 0);
    ar_longkey("r3key_i", 1);
    ar_longkey("r3key_s", 0);
    ar_combined_key();
    ar_large_float("r3float_i", 1);
    ar_large_float("r3float_s", 0);
    ar_full_text();

    /* Bounded memory: 3000 distinct groups under a tiny per-request cap.
       The cap is the process db's own field (g_query_buffer_max_bytes is a
       macro over g_db; shard_db_query binds g_db = db for every request). */
    ArBudgetSave saved_budget __attribute__((cleanup(ar_budget_restore))) = {
        ar_db, ar_db->query_buffer_max_bytes
    };
    r = ar_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3many\","
               "\"splits\":8,\"max_key\":16,\"fields\":[\"l:long\"]}");
    shard_db_free_result(r);
    char *big = malloc(400000); size_t o = 0;
    o = tappendf(big, 400000, o,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"r3many\",\"records\":[");
    for (int i = 0; i < 3000; i++)
        o = tappendf(big, 400000, o, "%s{\"key\":\"k%d\",\"value\":{\"l\":%d}}",
                              i ? "," : "", i, i);
    snprintf(big + o, 400000 - o, "]}");
    r = ar_req(big); shard_db_free_result(r); free(big);
    ar_db->query_buffer_max_bytes = 8192;
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3many\","
               "\"group_by\":[\"l\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}],\"limit\":5000}");
    ASSERT_TRUE(r && strstr(r, "query memory buffer exceeded") && !strstr(r, "\"c\""),
                "bounded-memory rejection is an error, not a partial success");
    shard_db_free_result(r);
    ar_db->query_buffer_max_bytes = saved_budget.cap;
    tu_pdb_drop_object(ar_db, "default", "r3many");

    /* Task 6: budget exhaustion on every fast path is one whole-aggregate
       error — never a partial success body. */
    r = ar_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3bvs\",\"splits\":8,\"max_key\":16,"
               "\"fields\":[\"g:varchar:16\",\"v:long\"],\"indexes\":[\"g\",\"v\"]}");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "vs budget fixture"); shard_db_free_result(r);
    char *vsrows = malloc(64 * 3200); size_t vo = 0;
    vo = tappendf(vsrows, (size_t)(64 * 3200), vo,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"r3bvs\",\"records\":[");
    for (int i = 0; i < 3000; i++)
        vo = tappendf(vsrows, (size_t)(64 * 3200), vo,
            "%s{\"key\":\"k%d\",\"value\":{\"g\":\"g%d\",\"v\":%d}}",
            i ? "," : "", i, i, i);
    snprintf(vsrows + vo, (size_t)(64 * 3200) - vo, "]}");
    r = ar_req(vsrows); ASSERT_TRUE(r && !strstr(r, "\"error\""), "vs budget rows");
    shard_db_free_result(r); free(vsrows);
    ar_db->query_buffer_max_bytes = 8192;
    /* varchar stream: count(*) grouped by an indexed varchar with a limit */
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3bvs\",\"group_by\":[\"g\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":5000}");
    ASSERT_TRUE(r && strstr(r, "query memory buffer exceeded") && !strstr(r, "\"c\":"),
                "vs stream budget exhaustion is an error, not a partial body");
    shard_db_free_result(r);
    /* indexed group-by (IGB): same object, no limit — the btree fast path */
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3bvs\",\"group_by\":[\"g\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}]}");
    ASSERT_TRUE(r && strstr(r, "query memory buffer exceeded") && !strstr(r, "\"c\":"),
                "igb budget exhaustion is an error, not a partial body");
    shard_db_free_result(r);
    /* top-N stream: declines to the hash path under a tiny cap, which must
       then report the same whole-aggregate error */
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3bvs\",\"group_by\":[\"g\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}],\"order_by\":\"c\",\"order\":\"asc\",\"limit\":5000}");
    ASSERT_TRUE(r && strstr(r, "query memory buffer exceeded") && !strstr(r, "\"c\":"),
                "top-N budget exhaustion is an error, not a partial body");
    shard_db_free_result(r);
    ar_db->query_buffer_max_bytes = saved_budget.cap;
    /* sanity: with the budget restored the same query answers fully */
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3bvs\",\"group_by\":[\"g\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":5000}");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "vs budget restored answers fully");
    shard_db_free_result(r);
    tu_pdb_drop_object(ar_db, "default", "r3bvs");

    /* IGB bitmap dictionary under the same cap */
    r = ar_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3bbm\",\"splits\":8,\"max_key\":16,\"fields\":[\"s:varchar:16\"],"
               "\"indexes\":[\"s:bitmap\"]}");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "bitmap budget fixture"); shard_db_free_result(r);
    char *bmrows = malloc(64 * 3200); size_t bo = 0;
    bo = tappendf(bmrows, (size_t)(64 * 3200), bo,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"r3bbm\",\"records\":[");
    for (int i = 0; i < 3000; i++)
        bo = tappendf(bmrows, (size_t)(64 * 3200), bo,
            "%s{\"key\":\"k%d\",\"value\":{\"s\":\"s%d\"}}",
            i ? "," : "", i, i % 200);
    snprintf(bmrows + bo, (size_t)(64 * 3200) - bo, "]}");
    r = ar_req(bmrows); ASSERT_TRUE(r && !strstr(r, "\"error\""), "bitmap budget rows");
    shard_db_free_result(r); free(bmrows);
    ar_db->query_buffer_max_bytes = 8192;
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3bbm\",\"group_by\":[\"s\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"c\"}]}");
    ASSERT_TRUE(r && strstr(r, "query memory buffer exceeded") && !strstr(r, "\"c\":"),
                "bitmap igb budget exhaustion is an error, not a partial body");
    shard_db_free_result(r);
    ar_db->query_buffer_max_bytes = saved_budget.cap;
    tu_pdb_drop_object(ar_db, "default", "r3bbm");

    /* Task 7: having literals are validated by the same parser that
       evaluates them. Rejections: trailing whitespace and non-finite
       floats; leading whitespace on decimals. Acceptances (both sides
       agree): leading whitespace under strtod, arbitrarily large exact
       decimals. */
    r = ar_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3hav\",\"splits\":8,\"max_key\":16,"
               "\"fields\":[\"d:double\",\"n:numeric:9,2\"],\"indexes\":[\"d\",\"n\"]}");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "having fixture"); shard_db_free_result(r);
    r = ar_req("{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"r3hav\",\"records\":[{\"key\":\"a\",\"value\":{\"d\":0.5,\"n\":\"1.00\"}},{\"key\":\"b\",\"value\":{\"d\":1.5,\"n\":\"2.00\"}}]}");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "having rows"); shard_db_free_result(r);
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3hav\",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"d\",\"alias\":\"a\"}],\"having\":[{\"field\":\"a\",\"op\":\"lt\",\"value\":\"5 \"}]}");
    ASSERT_TRUE(r && strstr(r, "\"invalid numeric value") && strstr(r, "\"error\""),
                "having literal '5 ' rejected: d_avg_ws_tail");
    shard_db_free_result(r);
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3hav\",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"d\",\"alias\":\"a\"}],\"having\":[{\"field\":\"a\",\"op\":\"lt\",\"value\":\"1e400\"}]}");
    ASSERT_TRUE(r && strstr(r, "\"invalid numeric value") && strstr(r, "\"error\""),
                "having literal '1e400' rejected: d_avg_inf");
    shard_db_free_result(r);
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3hav\",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"d\",\"alias\":\"a\"}],\"having\":[{\"field\":\"a\",\"op\":\"lt\",\"value\":\" 5\"}]}");
    ASSERT_TRUE(r && !strstr(r, "\"error\"") && strstr(r, "\"a\":"),
                "having literal ' 5' accepted by the evaluating parser: d_avg_ws_head (strtod skips it)");
    shard_db_free_result(r);
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3hav\",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"d\",\"alias\":\"a\"}],\"having\":[{\"field\":\"a\",\"op\":\"lt\",\"value\":\"5\"}]}");
    ASSERT_TRUE(r && !strstr(r, "\"error\"") && strstr(r, "\"a\":"),
                "having literal '5' accepted by the evaluating parser: d_avg_clean");
    shard_db_free_result(r);
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3hav\",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"n\",\"alias\":\"s\"}],\"having\":[{\"field\":\"s\",\"op\":\"lt\",\"value\":\"5 \"}]}");
    ASSERT_TRUE(r && strstr(r, "\"invalid numeric value") && strstr(r, "\"error\""),
                "having literal '5 ' rejected: n_sum_ws_tail");
    shard_db_free_result(r);
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3hav\",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"n\",\"alias\":\"s\"}],\"having\":[{\"field\":\"s\",\"op\":\"lt\",\"value\":\" 5\"}]}");
    ASSERT_TRUE(r && strstr(r, "\"invalid numeric value") && strstr(r, "\"error\""),
                "having literal ' 5' rejected: n_sum_ws_head");
    shard_db_free_result(r);
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3hav\",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"n\",\"alias\":\"s\"}],\"having\":[{\"field\":\"s\",\"op\":\"lt\",\"value\":\"1e400\"}]}");
    ASSERT_TRUE(r && !strstr(r, "\"error\"") && strstr(r, "\"s\":"),
                "having literal '1e400' accepted by the evaluating parser: n_sum_inf (exact decimal)");
    shard_db_free_result(r);
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3hav\",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"n\",\"alias\":\"s\"}],\"having\":[{\"field\":\"s\",\"op\":\"lt\",\"value\":\"5\"}]}");
    ASSERT_TRUE(r && !strstr(r, "\"error\"") && strstr(r, "\"s\":"),
                "having literal '5' accepted by the evaluating parser: n_sum_clean");
    shard_db_free_result(r);
    tu_pdb_drop_object(ar_db, "default", "r3hav");






    /* Allocation faults on the exact render path: error-or-correct, never
       a success-shaped partial response. */
    ar_make("r3flt", 8, 0);
    if (agg_test_fail_cells_alloc) {
        agg_test_fail_cells_alloc(1);
        ar_expect("r3flt", ",\"group_by\":[\"g\"]", "{\"fn\":\"count\",\"alias\":\"c\"}",
            "{\"error\":\"out of memory\"}", "cell allocation fails before output");
        agg_test_fail_cells_alloc(0);
        ar_expect("r3flt", ",\"group_by\":[\"g\"],\"order_by\":\"g\"", "{\"fn\":\"count\",\"alias\":\"c\"}",
            "[{\"g\":\"a\",\"c\":2},{\"g\":\"b\",\"c\":3},{\"g\":\"c\",\"c\":1}]",
            "cell allocation recovers with exact complete response");
    }
    /* The packed exact path has more fault points than the legacy one, so the
       sweep runs wide enough to pass the last fault point and reach success. */
    int saw_error = 0, saw_ok = 0;
    for (int n = 1; n <= 64; n++) {
        agg_exact_test_set_fail_alloc(n);
        r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3flt\","
                   "\"aggregates\":[" AR_S("sum","n","sn") "," AR_S("avg","n","an") "]}");
        agg_exact_test_set_fail_alloc(0);
        ASSERT_NOT_NULL(r, "response");
        if (!r) break;
        ar_chomp(r);
        if (strstr(r, "\"error\"")) {
            saw_error = 1;
            ASSERT_TRUE(!strstr(r, "\"sn\""), "error response carries no partial value");
        } else {
            saw_ok = 1;
            ASSERT_EQ_STR(r, "{\"sn\":0.3000,\"an\":0.07500000000000000000}", "success is exact");
        }
        shard_db_free_result(r);
    }
    ASSERT_TRUE(saw_error && saw_ok, "fault loop observed both an error and the exact result");
    tu_pdb_drop_object(ar_db, "default", "r3flt");
    return 0;
}
TEST_REGISTER("test-agg-exact-limits", ar_limits_run)

/* Second blind review: streaming/fallback parity and atomic escaped output. */
static int ar_output_safety(void) {
    int fault_guard __attribute__((cleanup(ar_fault_restore))) = 0;
    ar_db = test_get_process_db(); if (!ar_db) return 1;
    char *r = ar_req("{\"mode\":\"add-dir\",\"dir\":\"default\"}"); shard_db_free_result(r);
    ar_make("r3outputs", 8, 1);
    /* count(field) must decline the one-count streaming state. */
    ar_expect("r3outputs", ",\"group_by\":[\"g\"],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":10",
        AR_S("count","l","c"),
        "[{\"g\":\"b\",\"c\":3},{\"g\":\"a\",\"c\":2},{\"g\":\"c\",\"c\":0}]", "topN nullable count parity");
    r = ar_req("{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"r3outputs\","
        "\"group_by\":[\"d\"],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":10,\"aggregates\":["
        "{\"fn\":\"count\",\"alias\":\"c\"}," AR_S("avg","d","a") "]}");
    ASSERT_TRUE(r && strstr(r, "{\"d\":null,\"c\":2,\"a\":null}"), "all-NULL avg remains JSON null");
    shard_db_free_result(r);

    r = ar_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3outfloat\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"d:double\"],\"indexes\":[\"d\"]}");
    ASSERT_TRUE(r && !strstr(r,"\"error\""),"overflow object"); shard_db_free_result(r);
    r = ar_req("{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"r3outfloat\",\"records\":["
        "{\"key\":\"a\",\"value\":{\"d\":1e308}},{\"key\":\"b\",\"value\":{\"d\":1e308}}]}");
    ASSERT_TRUE(r && !strstr(r,"\"error\""),"overflow rows"); shard_db_free_result(r);
    ar_expect("r3outfloat", ",\"group_by\":[\"d\"],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":10",
        "{\"fn\":\"count\",\"alias\":\"c\"}," AR_S("sum","d","s"),
        "{\"error\":\"aggregate result out of range\"}","overflow is one error, never heap corruption");
    tu_pdb_drop_object(ar_db,"default","r3outfloat");

    if (agg_test_fail_cells_alloc) {
        agg_test_fail_cells_alloc(1);
        ar_expect("r3outputs", ",\"group_by\":[\"g\"],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":10",
            "{\"fn\":\"count\",\"alias\":\"c\"}","{\"error\":\"out of memory\"}","stream render array OOM frees drained data once");
        agg_test_fail_cells_alloc(0);
    }
    if (agg_test_fail_escape_alloc) {
        for (int stream = 0; stream < 2; stream++) {
            agg_test_fail_escape_alloc(1);
            ar_expect("r3outputs", stream
                ? ",\"group_by\":[\"g\"],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":10"
                : ",\"group_by\":[\"g\"]",
                "{\"fn\":\"count\",\"alias\":\"c\"}","{\"error\":\"out of memory\"}","group escaping OOM precedes any success output");
            agg_test_fail_escape_alloc(0);
        }
    }
    ar_expect("r3outputs", ",\"group_by\":[\"g\"],\"order_by\":\"c\",\"order\":\"desc\",\"limit\":10",
        "{\"fn\":\"count\",\"alias\":\"c\"}",
        "[{\"g\":\"b\",\"c\":3},{\"g\":\"a\",\"c\":2},{\"g\":\"c\",\"c\":1}]","stream succeeds after faults");
    tu_pdb_drop_object(ar_db,"default","r3outputs");

    r = ar_req("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"r3outtext\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:2000\"]}");
    ASSERT_TRUE(r && !strstr(r,"\"error\""),"text object"); shard_db_free_result(r);
    r = ar_req("{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"r3outtext\","
        "\"key\":\"a\",\"value\":{\"v\":\"hello\"}}");
    ASSERT_TRUE(r && !strstr(r,"\"error\""),"text row"); shard_db_free_result(r);
    if (agg_test_fail_escape_alloc) {
        agg_test_fail_escape_alloc(1);
        ar_expect("r3outtext","",AR_S("min","v","mn"),"{\"error\":\"out of memory\"}","text extrema escaping fails atomically");
        agg_test_fail_escape_alloc(0);
    }
    ar_expect("r3outtext","",AR_S("min","v","mn"),"{\"mn\":\"hello\"}","full text succeeds after escaping fault");
    tu_pdb_drop_object(ar_db,"default","r3outtext");
    return 0;
}
TEST_REGISTER("test-agg-exact-output-safety", ar_output_safety)

