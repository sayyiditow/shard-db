/* Aggregate type matrix: public aggregate behavior across every typed
 * field, through both entry points (JSON mode:"aggregate" and the NQL
 * text path) and both execution routes (no-criteria indexed fast paths
 * and criteria-driven record scans on a non-indexed field).
 *
 * Fixture rows (r1..r6) are designed so each assertion group has a
 * distinct expected value:
 *   r1  negatives          v="2"       i=-2  l=-20 ...
 *   r2  positives          v="10"      i=4   l=40  ...
 *   r3  numeric zeros +    v=""        i=0   ts=0, temporal/binary
 *       unset markers      fields OMITTED (da/dt/dms/tm/u stored unset)
 *   r4  quote/backslash    v=a"b\c     i=10  l=100
 *   r5  numeric varchar    v="12.5"    i=5   l=50
 *   r6  non-ASCII          v=ünïcode   i=7   l=70
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

#define OBJ "agg_type_matrix"
/* JSON aggregate request prefix; concatenate suffixes onto this macro. */
#define AGG_BASE \
    "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"" OBJ "\""

/* Assert the response body contains `needle` (and is valid non-NULL). */
static void agg_has(TestClient *tc, const char *q,
                    const char *needle, const char *msg) {
    char *resp = NULL;
    tc_request(tc, q, &resp);
    ASSERT_NOT_NULL(resp, msg);
    ASSERT_CONTAINS(resp, needle, msg);
    free(resp);
}

/* Assert the response body does NOT contain `needle`. */
static void agg_lacks(TestClient *tc, const char *q,
                      const char *needle, const char *msg) {
    char *resp = NULL;
    tc_request(tc, q, &resp);
    ASSERT_NOT_NULL(resp, msg);
    ASSERT_TRUE(SAFE_STRSTR(resp, needle) == NULL, msg);
    free(resp);
}

/* Extract an integer "\"alias\":<n>" (or "\"n\":<n>") count value. */
static int int_field(TestClient *tc, const char *q, const char *field) {
    char *resp = NULL;
    tc_request(tc, q, &resp);
    ASSERT_NOT_NULL(resp, field);
    char pat[128];
    snprintf(pat, sizeof(pat), "\"%s\":", field);
    const char *p = SAFE_STRSTR(resp, pat);
    ASSERT_TRUE(p != NULL, pat);
    int v = -999999;
    if (p) v = atoi(p + strlen(pat));
    free(resp);
    return v;
}

static int test_aggregate_type_matrix_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"" OBJ "\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"v:varchar:32\",\"i:int\",\"l:long\",\"s:short\","
        "\"d:double\",\"f:float\",\"b:bool\",\"by:byte\","
        "\"n:numeric:12,2\",\"da:date\",\"dt:datetime\","
        "\"dms:datetimems\",\"tm:time\",\"ts:timestamp\",\"u:uuid\","
        "\"e:enum(red,blue)\",\"ip4:ipv4\",\"ip6:ipv6\"],"
        "\"indexes\":[\"f\",\"ts\",\"dt\",\"dms\",\"v\",\"i\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "matrix object created");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"" OBJ "\","
        "\"records\":["
        /* r1 — negatives */
        "{\"key\":\"r1\",\"value\":{\"v\":\"2\",\"i\":-2,\"l\":-20,\"s\":-3,"
        "\"d\":-2.5,\"f\":-1.5,\"b\":true,\"by\":2,\"n\":\"-2.50\","
        "\"da\":\"20240101\",\"dt\":\"20240101120000\","
        "\"dms\":\"20240101120000123\",\"tm\":\"09:00:00\","
        "\"ts\":1789000000000,\"u\":\"11111111-1111-1111-1111-111111111111\","
        "\"e\":\"red\",\"ip4\":\"10.0.0.1\",\"ip6\":\"2001:db8::1\"}},"
        /* r2 — positives */
        "{\"key\":\"r2\",\"value\":{\"v\":\"10\",\"i\":4,\"l\":40,\"s\":6,"
        "\"d\":4.5,\"f\":3.5,\"b\":false,\"by\":4,\"n\":\"4.50\","
        "\"da\":\"20240103\",\"dt\":\"20240103120000\","
        "\"dms\":\"20240103120000456\",\"tm\":\"14:00:00\","
        "\"ts\":1789000001000,\"u\":\"22222222-2222-2222-2222-222222222222\","
        "\"e\":\"blue\",\"ip4\":\"10.0.0.2\",\"ip6\":\"2001:db8::2\"}},"
        /* r3 — numeric zeros; temporal/binary fields OMITTED so their
           stored bytes are the encoded unset markers */
        "{\"key\":\"r3\",\"value\":{\"v\":\"\",\"i\":0,\"l\":0,\"s\":0,"
        "\"d\":0,\"f\":0,\"b\":false,\"by\":0,\"n\":\"0.00\",\"ts\":0,"
        "\"e\":\"red\",\"ip4\":\"0.0.0.0\",\"ip6\":\"::\"}},"
        /* r4 — quote/backslash varchar */
        "{\"key\":\"r4\",\"value\":{\"v\":\"a\\\"b\\\\c\",\"i\":10,\"l\":100,"
        "\"s\":10,\"d\":10.0,\"f\":10.0,\"b\":true,\"by\":10,\"n\":\"10.00\","
        "\"da\":\"20240102\",\"dt\":\"20240102030405\","
        "\"dms\":\"20240102030405123\",\"tm\":\"12:00:00\","
        "\"ts\":1789000002000,\"u\":\"33333333-3333-3333-3333-333333333333\","
        "\"e\":\"blue\",\"ip4\":\"10.0.0.3\",\"ip6\":\"2001:db8::3\"}},"
        /* r5 — numeric-looking varchar */
        "{\"key\":\"r5\",\"value\":{\"v\":\"12.5\",\"i\":5,\"l\":50,\"s\":5,"
        "\"d\":5.5,\"f\":5.5,\"b\":true,\"by\":5,\"n\":\"5.55\","
        "\"da\":\"20240102\",\"dt\":\"20240102000000\","
        "\"dms\":\"20240102000000000\",\"tm\":\"06:30:00\","
        "\"ts\":1789000002000,\"u\":\"44444444-4444-4444-4444-444444444444\","
        "\"e\":\"red\",\"ip4\":\"10.0.0.4\",\"ip6\":\"2001:db8::4\"}},"
        /* r6 — non-ASCII varchar (time-of-day kept < 18:12:16, the
           engine's uint16 seconds-of-day storage limit) */
        "{\"key\":\"r6\",\"value\":{\"v\":\"ünïcode\",\"i\":7,\"l\":70,"
        "\"s\":7,\"d\":7.5,\"f\":7.5,\"b\":true,\"by\":7,\"n\":\"7.77\","
        "\"da\":\"20240105\",\"dt\":\"20240105120000\","
        "\"dms\":\"20240105120000999\",\"tm\":\"17:59:59\","
        "\"ts\":1789000003000,\"u\":\"55555555-5555-5555-5555-555555555555\","
        "\"e\":\"blue\",\"ip4\":\"10.0.0.5\",\"ip6\":\"2001:db8::5\"}}"
        "]}", &resp);
    ASSERT_CONTAINS(resp, "\"inserted\"", "matrix records inserted");
    free(resp); resp = NULL;

    /* ===== count(*) and count(field) for every concrete user type =====
       No criteria → indexed/O(1) routes; criteria on non-indexed l →
       record-scan route. Both must agree. */
    {
        const char *count_all[] =
            { "*", "v", "i", "l", "s", "d", "f", "b", "by", "n",
              "da", "dt", "dms", "tm", "ts", "u", "e", "ip4", "ip6" };
        /* Present-value expectations: 6 records total. Presence-doctrine
           types exclude r3 (empty varchar, unset calendar, midnight time,
           all-zero uuid); enum byte 0 is a real value and counts; every
           fixed-width numeric type counts all 6 rows including zeros. */
        const int expected[] =
            { 6, 5, 6, 6, 6, 6, 6, 6, 6, 6,
              5, 5, 5, 5, 6, 5, 6, 6, 6 };
        char q[512];
        for (size_t k = 0; k < sizeof(count_all) / sizeof(count_all[0]); k++) {
            char fld[32];
            snprintf(fld, sizeof(fld), "%s", count_all[k]);
            char aliasmsg[128];
            snprintf(aliasmsg, sizeof(aliasmsg),
                     "count(%s) present-value doctrine (indexed route)",
                     fld);
            if (strcmp(fld, "*") == 0)
                snprintf(q, sizeof(q),
                    AGG_BASE ",\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}");
            else
                snprintf(q, sizeof(q),
                    AGG_BASE ",\"aggregates\":[{\"fn\":\"count\",\"field\":\"%s\","
                             "\"alias\":\"n\"}]}", fld);
            ASSERT_EQ_INT(int_field(tc, q, "n"), expected[k], aliasmsg);
        }
    }
    /* Same counts through the record-scan route (criteria on l). */
    {
        const char *count_scan[] =
            { "v", "da", "dt", "dms", "tm", "u", "i", "b", "by", "ts", "e" };
        const int expected_scan[] = { 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6 };
        char q[640];
        for (size_t k = 0; k < sizeof(count_scan) / sizeof(count_scan[0]); k++) {
            char msg[128];
            snprintf(msg, sizeof(msg),
                     "count(%s) present-value doctrine (scan route)",
                     count_scan[k]);
            snprintf(q, sizeof(q),
                AGG_BASE ",\"criteria\":[{\"field\":\"l\",\"op\":\"gte\","
                         "\"value\":\"-1000\"}],"
                "\"aggregates\":[{\"fn\":\"count\",\"field\":\"%s\",\"alias\":\"n\"}]}",
                count_scan[k]);
            ASSERT_EQ_INT(int_field(tc, q, "n"), expected_scan[k], msg);
        }
    }

    /* ===== composite field expressions reject for every function ===== */
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"count\",\"field\":\"i+v\",\"alias\":\"n\"}]}",
        "aggregate count unsupported for field type composite",
        "composite count rejects (JSON)");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"i+v\",\"alias\":\"x\"}]}",
        "aggregate sum unsupported for field type composite",
        "composite sum rejects (JSON)");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"min\",\"field\":\"i+v\",\"alias\":\"x\"}]}",
        "aggregate min unsupported for field type composite",
        "composite min rejects (JSON)");
    agg_has(tc,
        "aggregate default " OBJ " count(i+v)",
        "aggregate count unsupported for field type composite",
        "composite count rejects (NQL)");

    /* ===== numeric aggregates across both routes =====
       sums over r1..r6: i=24, l=240, s=25, d=25, f=25, n=25.32 */
    {
        struct { const char *fld; const char *sum; const char *min;
                 const char *max; } num[] = {
            { "i",  "24",    "\"mn\":-2",   "\"mx\":10" },
            { "l",  "240",   "\"mn\":-20",  "\"mx\":100" },
            { "s",  "25",    "\"mn\":-3",   "\"mx\":10" },
            { "d",  "25",    "\"mn\":-2.5", "\"mx\":10" },
            { "f",  "25",    "\"mn\":-1.5", "\"mx\":10" },
            { "n",  "25.32", "\"mn\":-2.5", "\"mx\":10" },
        };
        char q[640];
        for (size_t k = 0; k < sizeof(num) / sizeof(num[0]); k++) {
            char msg[128];
            /* no-criteria → indexed btree fast route */
            snprintf(q, sizeof(q),
                AGG_BASE ",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"%s\",\"alias\":\"s\"},"
                         "{\"fn\":\"min\",\"field\":\"%s\",\"alias\":\"mn\"},"
                         "{\"fn\":\"max\",\"field\":\"%s\",\"alias\":\"mx\"}]}",
                num[k].fld, num[k].fld, num[k].fld);
            snprintf(msg, sizeof(msg), "sum/min/max(%s) indexed route", num[k].fld);
            agg_has(tc, q, num[k].sum, msg);
            agg_has(tc, q, num[k].min, msg);
            agg_has(tc, q, num[k].max, msg);

            /* criteria on non-indexed l → record-scan route */
            snprintf(q, sizeof(q),
                AGG_BASE ",\"criteria\":[{\"field\":\"l\",\"op\":\"gte\",\"value\":\"-1000\"}],"
                         "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"%s\",\"alias\":\"s\"},"
                         "{\"fn\":\"min\",\"field\":\"%s\",\"alias\":\"mn\"},"
                         "{\"fn\":\"max\",\"field\":\"%s\",\"alias\":\"mx\"}]}",
                num[k].fld, num[k].fld, num[k].fld);
            snprintf(msg, sizeof(msg), "sum/min/max(%s) scan route", num[k].fld);
            agg_has(tc, q, num[k].sum, msg);
            agg_has(tc, q, num[k].min, msg);
            agg_has(tc, q, num[k].max, msg);
        }
        /* Zero-sensitive averages: zeros are present values, so avgs
           divide by 6 — [.., 0, ..] shapes make the old skip-zero
           divisor (5) produce a different number. */
        agg_has(tc,
            AGG_BASE ",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"l\",\"alias\":\"a\"}]}",
            "\"a\":40", "avg(l)=240/6 with zero preserved");
        agg_has(tc,
            AGG_BASE ",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"i\",\"alias\":\"a\"}]}",
            "\"a\":4", "avg(i)=24/6 with zero preserved");
    }

    /* bool and byte: numeric 0/1 behavior, zeros preserved */
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"b\",\"alias\":\"s\"},"
                 "{\"fn\":\"min\",\"field\":\"b\",\"alias\":\"mn\"},"
                 "{\"fn\":\"max\",\"field\":\"b\",\"alias\":\"mx\"}]}",
        "\"s\":4", "bool sum counts true rows");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"min\",\"field\":\"b\",\"alias\":\"mn\"},"
                 "{\"fn\":\"max\",\"field\":\"b\",\"alias\":\"mx\"}]}",
        "\"mn\":0", "bool min is 0 (false is a real value)");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"by\",\"alias\":\"s\"}]}",
        "\"s\":28", "byte sum includes zero row");

    /* timestamp: JSON numbers, ts:0 participates */
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"ts\",\"alias\":\"s\"}]}",
        "\"s\":8945000008000", "timestamp sum includes ts:0");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"max\",\"field\":\"ts\",\"alias\":\"mx\"}]}",
        "\"mx\":1789000003000", "timestamp max is an unquoted number");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"ts\",\"alias\":\"a\"}]}",
        "\"a\":1490833334666.666748", "timestamp avg divides by 6 (ts:0 present)");

    /* ===== calendar min/max: exact canonical quoted strings ===== */
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"min\",\"field\":\"da\",\"alias\":\"mn\"},"
                 "{\"fn\":\"max\",\"field\":\"da\",\"alias\":\"mx\"}]}",
        "\"mn\":\"20240101\"", "date min is quoted yyyyMMdd");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"min\",\"field\":\"da\",\"alias\":\"mn\"},"
                 "{\"fn\":\"max\",\"field\":\"da\",\"alias\":\"mx\"}]}",
        "\"mx\":\"20240105\"", "date max is quoted yyyyMMdd");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"min\",\"field\":\"dt\",\"alias\":\"mn\"},"
                 "{\"fn\":\"max\",\"field\":\"dt\",\"alias\":\"mx\"}]}",
        "\"mn\":\"20240101120000\"", "datetime min is quoted yyyyMMddHHmmss");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"max\",\"field\":\"dt\",\"alias\":\"mx\"}]}",
        "\"mx\":\"20240105120000\"", "datetime max is quoted yyyyMMddHHmmss");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"min\",\"field\":\"dms\",\"alias\":\"mn\"},"
                 "{\"fn\":\"max\",\"field\":\"dms\",\"alias\":\"mx\"}]}",
        "\"mn\":\"20240101120000123\"", "datetimems min is 17-digit quoted");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"max\",\"field\":\"dms\",\"alias\":\"mx\"}]}",
        "\"mx\":\"20240105120000999\"", "datetimems max is 17-digit quoted");
    /* calendar min/max through the scan route too */
    agg_has(tc,
        AGG_BASE ",\"criteria\":[{\"field\":\"l\",\"op\":\"gte\",\"value\":\"-1000\"}],"
                 "\"aggregates\":[{\"fn\":\"min\",\"field\":\"dt\",\"alias\":\"mn\"}]}",
        "\"mn\":\"20240101120000\"", "datetime min via scan route");

    /* ===== calendar sum/avg rejected ===== */
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"da\",\"alias\":\"s\"}]}",
        "aggregate sum unsupported for field type date", "date sum rejected");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"dt\",\"alias\":\"a\"}]}",
        "aggregate avg unsupported for field type datetime", "datetime avg rejected");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"dms\",\"alias\":\"s\"}]}",
        "aggregate sum unsupported for field type datetimems",
        "datetimems sum rejected");

    /* ===== varchar: lexicographic min/max, quoting, escaping =====
       byte order: "10" < "12.5" < "2" < a"b\c < ünïcode */
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"},"
                 "{\"fn\":\"max\",\"field\":\"v\",\"alias\":\"mx\"}]}",
        "\"mn\":\"10\"", "varchar min is lexicographic (quoted)");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"},"
                 "{\"fn\":\"max\",\"field\":\"v\",\"alias\":\"mx\"}]}",
        "\"mx\":\"ünïcode\"", "varchar max is non-ASCII value (quoted)");
    /* scan route */
    agg_has(tc,
        AGG_BASE ",\"criteria\":[{\"field\":\"l\",\"op\":\"gte\",\"value\":\"-1000\"}],"
                 "\"aggregates\":[{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"},"
                 "{\"fn\":\"max\",\"field\":\"v\",\"alias\":\"mx\"}]}",
        "\"mn\":\"10\"", "varchar min via scan route");
    /* quote/backslash value round-trips as valid JSON */
    {
        char *resp2 = NULL;
        tc_request(tc,
            AGG_BASE ",\"group_by\":[\"v\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
                     "\"order_by\":\"v\"}", &resp2);
        ASSERT_NOT_NULL(resp2, "group-by v (quote/backslash row) valid JSON");
        ASSERT_CONTAINS(resp2, "\"v\":\"a\\\"b\\\\c\"",
                        "quote/backslash varchar JSON-escaped");
        free(resp2);
    }
    /* varchar sum/avg rejected */
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"v\",\"alias\":\"s\"}]}",
        "aggregate sum unsupported for field type varchar",
        "varchar sum rejected");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"avg\",\"field\":\"v\",\"alias\":\"a\"}]}",
        "aggregate avg unsupported for field type varchar",
        "varchar avg rejected");

    /* ===== non-count aggregates on time/uuid/enum/ipv4/ipv6 rejected ===== */
    {
        struct { const char *fld, *tname; } unsup[] = {
            { "tm",  "time" },  { "u", "uuid" }, { "e", "enum" },
            { "ip4", "ipv4" },  { "ip6", "ipv6" },
        };
        char q[512], msg[128];
        for (size_t k = 0; k < sizeof(unsup) / sizeof(unsup[0]); k++) {
            snprintf(q, sizeof(q),
                AGG_BASE ",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"%s\",\"alias\":\"x\"}]}",
                unsup[k].fld);
            snprintf(msg, sizeof(msg), "%s sum rejected", unsup[k].tname);
            char needle[96];
            snprintf(needle, sizeof(needle),
                     "aggregate sum unsupported for field type %s", unsup[k].tname);
            agg_has(tc, q, needle, msg);

            snprintf(q, sizeof(q),
                AGG_BASE ",\"aggregates\":[{\"fn\":\"min\",\"field\":\"%s\",\"alias\":\"x\"}]}",
                unsup[k].fld);
            snprintf(needle, sizeof(needle),
                     "aggregate min unsupported for field type %s", unsup[k].tname);
            snprintf(msg, sizeof(msg), "%s min rejected", unsup[k].tname);
            agg_has(tc, q, needle, msg);
        }
    }

    /* ===== textual having and order_by (lexicographic) =====
       min(v) per group: red rows {2, "", 12.5} → "12.5" ("" excluded);
       blue rows {10, a"b\c, ünïcode} → "10". */
    agg_has(tc,
        AGG_BASE ",\"group_by\":[\"e\"],"
                 "\"aggregates\":[{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"}],"
                 "\"having\":[{\"field\":\"mn\",\"op\":\"eq\",\"value\":\"10\"}]}",
        "\"e\":\"blue\"", "textual alias having eq matches blue group");
    agg_lacks(tc,
        AGG_BASE ",\"group_by\":[\"e\"],"
                 "\"aggregates\":[{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"}],"
                 "\"having\":[{\"field\":\"mn\",\"op\":\"eq\",\"value\":\"10\"}]}",
        "\"e\":\"red\"", "textual alias having eq drops red group");
    /* textual group-by field in having (lexicographic lte). The empty
       varchar value itself is an unset marker: it forms no indexed
       group (pre-existing insert-side zero-length-key skip), so the
       lte "10" bucket set is exactly {"10"}. */
    agg_has(tc,
        AGG_BASE ",\"group_by\":[\"v\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
                 "\"having\":[{\"field\":\"v\",\"op\":\"lte\",\"value\":\"10\"}]}",
        "\"v\":\"10\"", "textual group having lte keeps \"10\"");
    agg_lacks(tc,
        AGG_BASE ",\"group_by\":[\"v\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
                 "\"having\":[{\"field\":\"v\",\"op\":\"lte\",\"value\":\"10\"}]}",
        "\"v\":\"2\"", "textual group having lte drops \"2\"");
    /* in / not_in on textual having */
    agg_has(tc,
        AGG_BASE ",\"group_by\":[\"e\"],"
                 "\"aggregates\":[{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"}],"
                 "\"having\":[{\"field\":\"mn\",\"op\":\"in\","
                 "\"value\":[\"10\",\"12.5\"]}]}",
        "\"e\":\"blue\"", "textual having IN matches");
    agg_lacks(tc,
        AGG_BASE ",\"group_by\":[\"e\"],"
                 "\"aggregates\":[{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"}],"
                 "\"having\":[{\"field\":\"mn\",\"op\":\"not_in\","
                 "\"value\":[\"10\"]}]}",
        "\"e\":\"blue\"", "textual having NOT_IN drops blue");
    /* calendar aggregate order_by exercises the non-heap sorter:
       min(da) red=20240101 < blue=20240102 → asc red first */
    {
        char *resp2 = NULL;
        tc_request(tc,
            AGG_BASE ",\"group_by\":[\"e\"],"
                     "\"aggregates\":[{\"fn\":\"min\",\"field\":\"da\",\"alias\":\"first\"}],"
                     "\"order_by\":\"first\",\"order\":\"asc\"}", &resp2);
        ASSERT_NOT_NULL(resp2, "calendar order_by valid");
        {
            const char *red = SAFE_STRSTR(resp2, "\"e\":\"red\"");
            const char *blue = SAFE_STRSTR(resp2, "\"e\":\"blue\"");
            ASSERT_TRUE(red && blue, "calendar order_by has both groups");
            if (red && blue)
                ASSERT_TRUE(red < blue,
                            "calendar order_by asc: 20240101 group first");
        }
        ASSERT_CONTAINS(resp2, "\"first\":\"20240101\"", "red min da");
        free(resp2);
    }

    /* ===== grouped zero preservation and path agreement =====
       Integer-key fast path (group by indexed int i), string-key path
       (group by non-indexed double d), indexed string-key path (group
       by indexed varchar v): zero groups render "0", and indexed vs
       scan routes produce identical group sets. */
    agg_has(tc,
        AGG_BASE ",\"group_by\":[\"i\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}",
        "\"i\":\"0\"", "indexed integer-key path keeps zero int group");
    agg_has(tc,
        AGG_BASE ",\"criteria\":[{\"field\":\"s\",\"op\":\"gte\",\"value\":\"-100\"}],"
                 "\"group_by\":[\"i\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}",
        "\"i\":\"0\"", "scan integer-key path keeps zero int group");
    agg_has(tc,
        AGG_BASE ",\"group_by\":[\"l\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}",
        "\"l\":\"0\"", "integer-key path (non-indexed long) keeps zero group");
    agg_has(tc,
        AGG_BASE ",\"group_by\":[\"d\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}",
        "\"d\":\"0\"", "string-key path keeps zero double group");
    agg_lacks(tc,
        AGG_BASE ",\"group_by\":[\"d\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}",
        "\"d\":\"\"", "zero double group is distinct from a missing key");
    {
        /* group sets identical: indexed i vs scan i (both integer-key) */
        const char *groups[6] = { "\"i\":\"-2\"", "\"i\":\"0\"", "\"i\":\"4\"",
                                  "\"i\":\"5\"", "\"i\":\"7\"", "\"i\":\"10\"" };
        char q[512];
        snprintf(q, sizeof(q),
            AGG_BASE ",\"group_by\":[\"i\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}");
        for (size_t k = 0; k < 6; k++)
            agg_has(tc, q, groups[k], "indexed route int group set");
        snprintf(q, sizeof(q),
            AGG_BASE ",\"criteria\":[{\"field\":\"s\",\"op\":\"gte\",\"value\":\"-100\"}],"
                     "\"group_by\":[\"i\"],\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}]}");
        for (size_t k = 0; k < 6; k++)
            agg_has(tc, q, groups[k], "scan route int group set matches indexed");
    }

    /* ===== empty textual aggregates: null / empty CSV / having fails =====
       Criteria `s eq 0` matches exactly r3 — the row whose varchar is
       empty and calendar fields unset — so the aggregates have rows but
       no present textual values. */
    agg_has(tc,
        AGG_BASE ",\"criteria\":[{\"field\":\"s\",\"op\":\"eq\",\"value\":\"0\"}],"
                 "\"aggregates\":[{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"}]}",
        "\"mn\":null", "empty textual min renders JSON null");
    agg_has(tc,
        AGG_BASE ",\"criteria\":[{\"field\":\"s\",\"op\":\"eq\",\"value\":\"0\"}],"
                 "\"aggregates\":[{\"fn\":\"min\",\"field\":\"dt\",\"alias\":\"mn\"}]}",
        "\"mn\":null", "empty calendar min renders JSON null");
    /* numeric empty results keep their existing 0 behavior (zero
       matching rows via the walk-fetch-check route) */
    agg_has(tc,
        AGG_BASE ",\"criteria\":[{\"field\":\"l\",\"op\":\"gte\",\"value\":\"10000\"}],"
                 "\"aggregates\":[{\"fn\":\"min\",\"field\":\"i\",\"alias\":\"mn\"}]}",
        "\"mn\":0", "empty numeric min keeps legacy 0");
    /* CSV: alias header then an empty cell */
    {
        char *resp2 = NULL;
        tc_request(tc,
            AGG_BASE ",\"criteria\":[{\"field\":\"s\",\"op\":\"eq\",\"value\":\"0\"}],"
                     "\"aggregates\":[{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"}],"
                     "\"format\":\"csv\"}", &resp2);
        ASSERT_NOT_NULL(resp2, "csv empty textual min");
        ASSERT_EQ_STR(resp2, "mn\n\n", "empty textual min CSV cell is empty");
        free(resp2);
    }
    /* every having comparison fails on a null textual result */
    agg_lacks(tc,
        AGG_BASE ",\"criteria\":[{\"field\":\"s\",\"op\":\"eq\",\"value\":\"0\"}],"
                 "\"group_by\":[\"e\"],"
                 "\"aggregates\":[{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"}],"
                 "\"having\":[{\"field\":\"mn\",\"op\":\"neq\",\"value\":\"zzz\"}]}",
        "\"e\":", "null textual min fails having neq");
    agg_lacks(tc,
        AGG_BASE ",\"criteria\":[{\"field\":\"s\",\"op\":\"eq\",\"value\":\"0\"}],"
                 "\"group_by\":[\"e\"],"
                 "\"aggregates\":[{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"}],"
                 "\"having\":[{\"field\":\"mn\",\"op\":\"not_in\","
                 "\"value\":[\"a\",\"b\"]}]}",
        "\"e\":", "null textual min fails having not_in");

    /* ===== pre-scan having validation ===== */
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
                 "\"having\":[{\"field\":\"nope\",\"op\":\"eq\",\"value\":\"1\"}]}",
        "unknown field 'nope' in having", "unknown having field rejected");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"min\",\"field\":\"v\",\"alias\":\"mn\"}],"
                 "\"having\":[{\"field\":\"mn\",\"op\":\"like\",\"value\":\"1%\"}]}",
        "aggregate having unsupported for field type varchar",
        "pattern op rejected on textual having");
    agg_has(tc,
        AGG_BASE ",\"aggregates\":[{\"fn\":\"sum\",\"field\":\"i\",\"alias\":\"s\"}],"
                 "\"having\":[{\"field\":\"s\",\"op\":\"regex\",\"value\":\"1.*\"}]}",
        "aggregate having unsupported for field type number",
        "regex op rejected on numeric having");

    /* ===== NQL entry point ===== */
    agg_has(tc, "aggregate default " OBJ " sum(i)",
            "\"sum_i\":24", "NQL no-criteria sum");
    agg_has(tc, "aggregate default " OBJ " 'l gte 40' sum(i)",
            "\"sum_i\":26", "NQL positional filter sum");
    agg_has(tc, "aggregate default " OBJ " sum(i) --group-by e",
            "\"sum_i\":21", "NQL grouped sum (blue)");
    agg_has(tc, "aggregate default " OBJ " sum(i) --group-by e",
            "\"sum_i\":3", "NQL grouped sum (red, zero preserved)");
    agg_has(tc, "aggregate default " OBJ " min(v)",
            "\"min_v\":\"10\"", "NQL varchar min is lexicographic");
    agg_has(tc, "aggregate default " OBJ " min(da)",
            "\"min_da\":\"20240101\"", "NQL calendar min canonical string");
    {
        char *resp2 = NULL;
        tc_request(tc, "aggregate default " OBJ " sum(v)", &resp2);
        ASSERT_CONTAINS(resp2,
            "aggregate sum unsupported for field type varchar",
            "NQL varchar sum rejected");
        free(resp2);
    }

    /* ===== explicit count(*) sanity through NQL ===== */
    {
        char *resp2 = NULL;
        tc_request(tc, "aggregate default " OBJ " count()", &resp2);
        ASSERT_CONTAINS(resp2, "\"count\":6", "NQL count(*) = 6");
        free(resp2);
    }

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-aggregate-type-matrix", test_aggregate_type_matrix_run)
