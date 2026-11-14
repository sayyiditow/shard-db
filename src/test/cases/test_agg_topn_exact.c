/* src/test/cases/test_agg_topn_exact.c
 *
 * Plan I — count-ranked streaming top-N returns exact extras and a defined
 * tie order. Counters/seams are process-wide TEST_BUILD seams
 * (agg_topn_test_reset before each measured request and on every exit path,
 * so nothing survives into later cases under run-all --jobs 1). Every case
 * drops the objects it creates on every exit path; a failed setup jumps to
 * cleanup and returns 1.
 *
 * Cases:
 *   T1 test-topn-exact-tie-boundary   heap ties at the limit keep the
 *                                     smallest group values (stream)
 *   T2 test-topn-exact-mixed-streams  count(*) + exact extras stream and
 *                                     equal the forced-hash output
 *   T3 test-topn-exact-total          stream total = exact distinct groups
 *   T0 test-topn-exact-type-labels    count-only stream labels match the
 *                                     hash path for every type (base guard)
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/query_internal.h"
#include "../db/agg_exact.h"
#include "../db/shard_db.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static ShardDb *db;

static char *q(const char *json) {
    char *r = NULL;
    tu_pdb_request(db, json, &r);
    return r;
}

/* TEST_BUILD counters (query_aggregate.c seams). */
static long s_runs, s_declines, s_forced, s_repeats;
static void stats(void) {
    agg_topn_test_stats(&s_runs, &s_declines, &s_forced, &s_repeats);
}

/* reset + one request + counters */
static char *run_measured(const char *req) {
    agg_topn_test_reset();
    char *r = q(req);
    stats();
    return r;
}

/* Re-run one request with the force-decline seam (exact hash path) and
 * compare it byte-for-byte with the stream response; asserts the seam
 * fired so the comparison really is stream vs hash. */
static void eq_forced_hash(const char *req, const char *stream_resp,
                           const char *msg) {
    agg_topn_test_reset();
    agg_topn_test_force_decline(1);
    char *h = q(req);
    stats();
    agg_topn_test_force_decline(0);
    ASSERT_EQ_INT(s_forced, 1, msg);
    ASSERT_EQ_STR(stream_resp, h, msg);
    shard_db_free_result(h);
}

/* Aggregate request builder: single group field, caller's spec list, order
 * alias + direction + limit; optional total / criteria. Static buffer —
 * consume (or copy) before building the next request. */
static const char *agg_json(const char *obj, const char *group,
                            const char *specs, const char *order,
                            const char *dir, int limit, int total,
                            const char *criteria) {
    static char buf[8192];
    int n = snprintf(buf, sizeof buf,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
        "\"group_by\":[\"%s\"],\"aggregates\":%s,"
        "\"order_by\":\"%s\",\"order\":\"%s\",\"limit\":%d",
        obj, group, specs, order, dir, limit);
    if (total && n > 0 && n < (int)sizeof buf)
        n += snprintf(buf + n, sizeof buf - (size_t)n, ",\"total\":true");
    if (criteria && n > 0 && n < (int)sizeof buf)
        n += snprintf(buf + n, sizeof buf - (size_t)n,
                      ",\"criteria\":%s", criteria);
    if (n > 0 && n < (int)sizeof buf)
        snprintf(buf + n, sizeof buf - (size_t)n, "}");
    return buf;
}

/* ---- object setup (create + bulk-insert as a {"key":{...},...} dict) ---- */

static int create_object(const char *obj, const char *fields,
                         const char *indexes) {
    char req[2048];
    snprintf(req, sizeof req,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"%s\","
        "\"splits\":8,\"max_key\":16,\"fields\":[%s],\"indexes\":[%s]}",
        obj, fields, indexes);
    char *r = q(req);
    char msg[64];
    snprintf(msg, sizeof msg, "setup: %s created", obj);
    ASSERT_TRUE(r && !SAFE_STRSTR(r, "\"error\""), msg);
    int ok = r && !SAFE_STRSTR(r, "\"error\"");
    shard_db_free_result(r);
    return ok ? 0 : 1;
}

static int load_records(const char *obj, const char *records, int expect_n) {
    char req[32768];
    snprintf(req, sizeof req,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\","
        "\"records\":%s}", obj, records);
    char *r = q(req);
    char msg[64], want[32];
    snprintf(msg, sizeof msg, "setup: %s loaded", obj);
    snprintf(want, sizeof want, "\"inserted\":%d", expect_n);
    ASSERT_TRUE(r && !SAFE_STRSTR(r, "\"error\"") && SAFE_STRSTR(r, want), msg);
    int ok = r && !SAFE_STRSTR(r, "\"error\"") && SAFE_STRSTR(r, want);
    shard_db_free_result(r);
    return ok ? 0 : 1;
}

static void drop_object(const char *obj) {
    if (db) tu_pdb_drop_object(db, "default", obj);
}

/* tqa: k = 10 ×5 (a00–a04), 20 ×5 (a05–a09), 30 ×6 (a10–a15). */
static int setup_tqa(void) {
    if (create_object("tqa", "\"k:int\"", "\"k\"")) return 1;
    char body[2048]; size_t p = 0;
    SB_APPEND(body, p, sizeof body, "{");
    for (int i = 0; i < 16; i++) {
        int k = i < 5 ? 10 : (i < 10 ? 20 : 30);
        SB_APPEND(body, p, sizeof body, "%s\"a%02d\":{\"k\":%d}",
                  i ? "," : "", i, k);
    }
    SB_APPEND(body, p, sizeof body, "}");
    return load_records("tqa", body, 16);
}

/* tqb: h = 1 ×7 (b00–b06), 2 ×3 (b07–b09), 3 ×13 (b10–b22), 4 ×1 (b23),
 * NULL ×6 (b24–b29); o = 1 and f = 1.5 on every row. */
static int setup_tqb(void) {
    if (create_object("tqb", "\"h:int\",\"o:int\",\"f:double\"",
                      "\"h\",\"f\"")) return 1;
    char body[4096]; size_t p = 0;
    SB_APPEND(body, p, sizeof body, "{");
    for (int i = 0; i < 30; i++) {
        const char *h = i < 7 ? "1" : i < 10 ? "2" : i < 23 ? "3"
                      : i == 23 ? "4" : "null";
        SB_APPEND(body, p, sizeof body,
                  "%s\"b%02d\":{\"h\":%s,\"o\":1,\"f\":1.5}",
                  i ? "," : "", i, h);
    }
    SB_APPEND(body, p, sizeof body, "}");
    return load_records("tqb", body, 30);
}

/* tqe: t = "" ×3 (e0–e2), "a" ×2 (e3–e4), NULL ×1 (e5). */
static int setup_tqe(void) {
    if (create_object("tqe", "\"t:varchar:8\"", "\"t\"")) return 1;
    char body[1024]; size_t p = 0;
    SB_APPEND(body, p, sizeof body, "{");
    for (int i = 0; i < 6; i++) {
        const char *t = i < 3 ? "\"\"" : i < 5 ? "\"a\"" : "null";
        SB_APPEND(body, p, sizeof body, "%s\"e%d\":{\"t\":%s}",
                  i ? "," : "", i, t);
    }
    SB_APPEND(body, p, sizeof body, "}");
    return load_records("tqe", body, 6);
}

/* tqc: d = "99999999999999999999.9999999999" ×3 (c0–c2),
 * "0.0000000001" ×2 (c3–c4), "-3.25" ×1 (c5), NULL ×1 (c6). */
static int setup_tqc(void) {
    if (create_object("tqc", "\"d:numeric:30,10\"", "\"d\"")) return 1;
    char body[2048]; size_t p = 0;
    SB_APPEND(body, p, sizeof body, "{");
    for (int i = 0; i < 7; i++) {
        const char *d = i < 3 ? "\"99999999999999999999.9999999999\""
                    : i < 5 ? "\"0.0000000001\""
                    : i == 5 ? "\"-3.25\"" : "null";
        SB_APPEND(body, p, sizeof body, "%s\"c%d\":{\"d\":%s}",
                  i ? "," : "", i, d);
    }
    SB_APPEND(body, p, sizeof body, "}");
    return load_records("tqc", body, 7);
}

/* tqd field/index sets, shared by tqf (T0) and tqd (T6). */
#define TQD_FIELDS  "\"i:int\",\"nm:numeric:12,2\",\"t:varchar:16\"," \
                    "\"e:enum(red,green,blue)\",\"ip:ipv4\",\"ts:timestamp\"," \
                    "\"x:double\""
#define TQD_INDEXES "\"i\",\"nm\",\"t\",\"e:btree\",\"ip\",\"ts\",\"x\""

/* tqf (T0): tqd's fields/indexes; 15 records, per-field group counts
 * 5,4,3,2,1 (e's NULL group has 9) — no ties on any field. */
static int setup_tqf(void) {
    if (create_object("tqf", TQD_FIELDS, TQD_INDEXES)) return 1;
    char body[8192]; size_t p = 0;
    SB_APPEND(body, p, sizeof body, "{");
    for (int i = 0; i < 15; i++) {
        const char *i_v, *nm, *t, *e, *ip, *ts, *x;
        if (i < 4) {           /* f0–f3: all fields NULL */
            i_v = nm = t = e = ip = ts = x = "null";
        } else if (i < 7) {    /* f4–f6 */
            i_v = "5"; nm = "\"1.25\""; t = "\"pear\""; e = "\"red\"";
            ip = "\"9.0.0.1\""; ts = "1700000000000"; x = "0.5";
        } else if (i < 9) {    /* f7–f8 */
            i_v = "-2"; nm = "\"-3.00\""; t = "\"\""; e = "\"blue\"";
            ip = "\"10.0.0.2\""; ts = "1600000000000"; x = "-1.5";
        } else if (i == 9) {   /* f9 */
            i_v = "9"; nm = "\"0.50\""; t = "\"fig\""; e = "\"green\"";
            ip = "\"10.0.0.10\""; ts = "1700000001000"; x = "2.0";
        } else {               /* f10–f14 */
            i_v = "0"; nm = "\"0.00\""; t = "\"kiwi\""; e = "null";
            ip = "\"10.0.0.3\""; ts = "1650000000000"; x = "0.0";
        }
        SB_APPEND(body, p, sizeof body,
                  "%s\"f%d\":{\"i\":%s,\"nm\":%s,\"t\":%s,\"e\":%s,"
                  "\"ip\":%s,\"ts\":%s,\"x\":%s}",
                  i ? "," : "", i, i_v, nm, t, e, ip, ts, x);
    }
    SB_APPEND(body, p, sizeof body, "}");
    return load_records("tqf", body, 15);
}

/* Spec lists. */
static const char N_SPECS[] = "[{\"fn\":\"count\",\"alias\":\"n\"}]";
static const char M3_SPECS[] =
    "[{\"fn\":\"count\",\"alias\":\"n\"},"
    "{\"fn\":\"sum\",\"field\":\"d\",\"alias\":\"s\"},"
    "{\"fn\":\"min\",\"field\":\"d\",\"alias\":\"mn\"},"
    "{\"fn\":\"max\",\"field\":\"d\",\"alias\":\"mx\"},"
    "{\"fn\":\"avg\",\"field\":\"d\",\"alias\":\"a\"},"
    "{\"fn\":\"count\",\"field\":\"d\",\"alias\":\"c\"}]";
static const char XH_SPECS[] =
    "[{\"fn\":\"count\",\"alias\":\"n\"},"
    "{\"fn\":\"sum\",\"field\":\"h\",\"alias\":\"s\"},"
    "{\"fn\":\"min\",\"field\":\"h\",\"alias\":\"mn\"},"
    "{\"fn\":\"max\",\"field\":\"h\",\"alias\":\"mx\"},"
    "{\"fn\":\"count\",\"field\":\"h\",\"alias\":\"c\"}]";

/* ---- T1: heap ties at the limit keep the smallest group values ---------- */

static int test_topn_exact_tie_boundary(void) {
    int rc = 1;
    db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;
    char *r = q("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);
    if (setup_tqa()) goto cleanup;

    /* desc cap 2: 30(6) wins; 10(5) and 20(5) tie → the tie keeps the
     * smaller group value. */
    r = run_measured(agg_json("tqa", "k", N_SPECS, "n", "desc", 2, 0, NULL));
    ASSERT_EQ_INT(s_runs, 1, "tie desc: stream served");
    ASSERT_EQ_STR(r, "[{\"k\":\"30\",\"n\":6},{\"k\":\"10\",\"n\":5}]\n",
                 "tie desc: page is 30 then 10");
    shard_db_free_result(r);

    /* asc cap 2: 30(6) rejected; the 10/20 tie is NOT flipped by the
     * direction. */
    r = run_measured(agg_json("tqa", "k", N_SPECS, "n", "asc", 2, 0, NULL));
    ASSERT_EQ_INT(s_runs, 1, "tie asc: stream served");
    ASSERT_EQ_STR(r, "[{\"k\":\"10\",\"n\":5},{\"k\":\"20\",\"n\":5}]\n",
                 "tie asc: page is 10 then 20");
    shard_db_free_result(r);

    /* desc cap 5 above the group count: whole set, ties still ordered. */
    r = run_measured(agg_json("tqa", "k", N_SPECS, "n", "desc", 5, 0, NULL));
    ASSERT_EQ_INT(s_runs, 1, "tie under cap: stream served");
    ASSERT_EQ_STR(r,
                 "[{\"k\":\"30\",\"n\":6},{\"k\":\"10\",\"n\":5},"
                 "{\"k\":\"20\",\"n\":5}]\n",
                 "tie under cap: page is 30, 10, 20");
    shard_db_free_result(r);
    rc = 0;
cleanup:
    agg_topn_test_reset();
    drop_object("tqa");
    return rc;
}

/* ---- T2: count(*) + exact extras stream; equal the forced-hash output --- */

static int test_topn_exact_mixed_streams(void) {
    int rc = 1;
    db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;
    char *r = q("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);
    if (setup_tqb()) goto cleanup;
    if (setup_tqc()) goto cleanup;
    if (setup_tqe()) goto cleanup;

    /* M1: count(*) + sum/min/max/count(h) on int group h. */
    r = run_measured(agg_json("tqb", "h", XH_SPECS, "n", "desc", 3, 0, NULL));
    ASSERT_TRUE(s_runs == 1 && s_declines == 0,
                "mixed int: stream served the query");
    ASSERT_EQ_STR(r,
                 "[{\"h\":\"3\",\"n\":13,\"s\":39,\"mn\":3,\"mx\":3,\"c\":13},"
                 "{\"h\":\"1\",\"n\":7,\"s\":7,\"mn\":1,\"mx\":1,\"c\":7},"
                 "{\"h\":null,\"n\":6,\"s\":0,\"mn\":null,\"mx\":null,\"c\":0}]\n",
                 "mixed int: response is the exact expected page");
    ASSERT_EQ_INT(s_repeats, 2,
                  "mixed int: exact state built only for the selected groups");
    eq_forced_hash(agg_json("tqb", "h", XH_SPECS, "n", "desc", 3, 0, NULL),
                   r, "mixed int: equals forced-hash output");
    shard_db_free_result(r);

    /* M2: avg(h) streams too. */
    const char *m2 = agg_json("tqb", "h",
        "[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"avg\",\"field\":\"h\",\"alias\":\"a\"}]",
        "n", "desc", 2, 0, NULL);
    r = run_measured(m2);
    ASSERT_TRUE(s_runs == 1 && s_declines == 0,
                "mixed avg: stream served the query");
    eq_forced_hash(m2, r, "mixed avg: equals forced-hash output");
    shard_db_free_result(r);

    /* M3: numeric:30,10 group — exact sum beyond double precision. */
    const char *m3 = agg_json("tqc", "d", M3_SPECS, "n", "desc", 1, 0, NULL);
    r = run_measured(m3);
    ASSERT_TRUE(s_runs == 1 && s_declines == 0,
                "mixed decimal: stream served the query");
    ASSERT_CONTAINS(r, "\"s\":299999999999999999999.9999999997",
                    "mixed decimal: sum is exact beyond double precision");
    eq_forced_hash(m3, r, "mixed decimal: equals forced-hash output");
    shard_db_free_result(r);

    /* D-a: sum over a double group field declines. */
    const char *da = agg_json("tqb", "f",
        "[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"sum\",\"field\":\"f\",\"alias\":\"s\"}]",
        "n", "desc", 3, 0, NULL);
    r = run_measured(da);
    ASSERT_TRUE(s_declines == 1 && s_runs == 0,
                "float sum: declines to the hash path");
    ASSERT_TRUE(r && !SAFE_STRSTR(r, "\"error\""),
                "float sum: answers without error");
    eq_forced_hash(da, r, "float sum: equals forced-hash output");
    shard_db_free_result(r);

    /* D-b: count of a field other than the group field declines. */
    const char *db_req = agg_json("tqb", "h",
        "[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"count\",\"field\":\"o\",\"alias\":\"c2\"}]",
        "n", "desc", 3, 0, NULL);
    r = run_measured(db_req);
    ASSERT_TRUE(s_declines == 1 && s_runs == 0,
                "count of another field: declines to the hash path");
    eq_forced_hash(db_req, r, "count of another field: equals forced-hash output");
    shard_db_free_result(r);

    /* D-c: ranking by a sum alias declines. */
    const char *dc = agg_json("tqb", "h",
        "[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"sum\",\"field\":\"h\",\"alias\":\"s\"}]",
        "s", "desc", 3, 0, NULL);
    r = run_measured(dc);
    ASSERT_TRUE(s_declines == 1 && s_runs == 0,
                "order by sum: declines to the hash path");
    eq_forced_hash(dc, r, "order by sum: equals forced-hash output");
    shard_db_free_result(r);

    /* D-d: count(g) on a presence-doctrine group field (varchar) declines. */
    const char *dd = agg_json("tqe", "t",
        "[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"count\",\"field\":\"t\",\"alias\":\"c\"}]",
        "n", "desc", 3, 0, NULL);
    r = run_measured(dd);
    ASSERT_TRUE(s_declines == 1 && s_runs == 0,
                "count of an unset-capable group: declines to the hash path");
    eq_forced_hash(dd, r, "count of an unset-capable group: equals forced-hash output");
    shard_db_free_result(r);
    rc = 0;
cleanup:
    agg_topn_test_reset();
    drop_object("tqb");
    drop_object("tqc");
    drop_object("tqe");
    return rc;
}

/* ---- T3: the stream's total is the exact distinct-group count ----------- */

static int test_topn_exact_total(void) {
    int rc = 1;
    db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;
    char *r = q("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);
    if (setup_tqa()) goto cleanup;
    if (setup_tqb()) goto cleanup;

    r = run_measured(agg_json("tqa", "k", N_SPECS, "n", "desc", 2, 1, NULL));
    ASSERT_EQ_INT(s_runs, 1, "total: stream served");
    ASSERT_EQ_STR(r,
                 "{\"rows\":[{\"k\":\"30\",\"n\":6},{\"k\":\"10\",\"n\":5}],"
                 "\"total\":3}\n",
                 "total: exact group count and tie order");
    shard_db_free_result(r);

    const char *req = agg_json("tqb", "h", XH_SPECS, "n", "desc", 3, 1, NULL);
    r = run_measured(req);
    eq_forced_hash(req, r, "total mixed: equals forced-hash output");
    shard_db_free_result(r);
    rc = 0;
cleanup:
    agg_topn_test_reset();
    drop_object("tqa");
    drop_object("tqb");
    return rc;
}

/* ---- T0: count-only stream labels match the hash path per type ---------- */

static int test_topn_exact_type_labels(void) {
    static const char *fields[] = { "i", "nm", "t", "e", "ip", "ts", "x" };
    int rc = 1;
    db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;
    char *r = q("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);
    if (setup_tqf()) goto cleanup;

    for (size_t g = 0; g < sizeof fields / sizeof *fields; g++) {
        char served[64], eq[64];
        snprintf(served, sizeof served, "labels %s: stream served", fields[g]);
        snprintf(eq, sizeof eq, "labels %s: equals forced-hash output",
                 fields[g]);
        const char *req =
            agg_json("tqf", fields[g], N_SPECS, "n", "desc", 10000, 0, NULL);
        r = run_measured(req);
        ASSERT_EQ_INT(s_runs, 1, served);
        eq_forced_hash(req, r, eq);
        shard_db_free_result(r);
    }
    rc = 0;
cleanup:
    agg_topn_test_reset();
    drop_object("tqf");
    return rc;
}

TEST_REGISTER("test-topn-exact-tie-boundary", test_topn_exact_tie_boundary)
TEST_REGISTER("test-topn-exact-mixed-streams", test_topn_exact_mixed_streams)
TEST_REGISTER("test-topn-exact-total", test_topn_exact_total)
TEST_REGISTER("test-topn-exact-type-labels", test_topn_exact_type_labels)

/* reset + force-decline + one request + counters (the exact hash path). */
static char *run_forced(const char *req) {
    agg_topn_test_reset();
    agg_topn_test_force_decline(1);
    char *r = q(req);
    stats();
    agg_topn_test_force_decline(0);
    return r;
}

/* tqd (T6): 60 records d00..d59, row r:
 *   i  = null if r%7==3 else (r*r)%5
 *   nm = ["0.00","1.25","2.50","3.75"][r%4]
 *   t  = null if r%9==0 else ["pear","apple","fig","kiwi"][r%4]
 *   e  = ["red","green","blue"][r%3]
 *   ip = "9.0.0.1" (r%10==0), "0.0.0.0" (r%10==5), else "10.0.0.<r%6+1>"
 *   ts = 1700000000000 + (r%5)*1000
 *   x  = (r%3)*0.5 */
static int setup_tqd(void) {
    static const char *const nmv[] = { "0.00", "1.25", "2.50", "3.75" };
    static const char *const tvv[] = { "pear", "apple", "fig", "kiwi" };
    static const char *const evv[] = { "red", "green", "blue" };
    if (create_object("tqd", TQD_FIELDS, TQD_INDEXES)) return 1;
    char body[16384]; size_t p = 0;
    SB_APPEND(body, p, sizeof body, "{");
    for (int r = 0; r < 60; r++) {
        char iv[16], ipv[24], tsv[24], xv[16], tv[16], nmv2[16], ev[8];
        if (r % 7 == 3) snprintf(iv, sizeof iv, "null");
        else snprintf(iv, sizeof iv, "%d", (r * r) % 5);
        snprintf(nmv2, sizeof nmv2, "\"%s\"", nmv[r % 4]);
        if (r % 9 == 0) snprintf(tv, sizeof tv, "null");
        else snprintf(tv, sizeof tv, "\"%s\"", tvv[r % 4]);
        snprintf(ev, sizeof ev, "\"%s\"", evv[r % 3]);
        if (r % 10 == 0) snprintf(ipv, sizeof ipv, "\"9.0.0.1\"");
        else if (r % 10 == 5) snprintf(ipv, sizeof ipv, "\"0.0.0.0\"");
        else snprintf(ipv, sizeof ipv, "\"10.0.0.%d\"", r % 6 + 1);
        snprintf(tsv, sizeof tsv, "%lld", 1700000000000LL + (r % 5) * 1000LL);
        snprintf(xv, sizeof xv, "%.1f", (r % 3) * 0.5);
        SB_APPEND(body, p, sizeof body,
                  "%s\"d%02d\":{\"i\":%s,\"nm\":%s,\"t\":%s,\"e\":%s,"
                  "\"ip\":%s,\"ts\":%s,\"x\":%s}",
                  r ? "," : "", r, iv, nmv2, tv, ev, ipv, tsv, xv);
    }
    SB_APPEND(body, p, sizeof body, "}");
    return load_records("tqd", body, 60);
}

/* ---- T4: agg_num_repeat equals repeated offers (unit) -------------------- */

static int test_agg_num_repeat(void) {
    AggEnv e;
    agg_env_init(&e, NULL, 0);

    /* INT: v ∈ {7,-3,0} × t ∈ {1,2,3,1000} — repeat matches replay. */
    {
        static const int64_t vals[] = { 7, -3, 0 };
        static const int64_t times[] = { 1, 2, 3, 1000 };
        for (size_t vi = 0; vi < sizeof vals / sizeof *vals; vi++) {
            int ok = 1;
            for (size_t ti = 0; ok && ti < sizeof times / sizeof *times; ti++) {
                AggNum a = {0}, b = {0};
                if (agg_num_offer_int(&a, (agg_i128)vals[vi]) != 0) ok = 0;
                if (ok && agg_num_repeat(&a, AGC_INT, times[ti], &e) != 0) ok = 0;
                for (int64_t k = 0; ok && k < times[ti]; k++)
                    if (agg_num_offer_int(&b, (agg_i128)vals[vi]) != 0) ok = 0;
                if (ok && (a.u.i.sum != b.u.i.sum || a.u.i.min != b.u.i.min ||
                           a.u.i.max != b.u.i.max || a.has != b.has)) ok = 0;
            }
            char msg[96];
            snprintf(msg, sizeof msg, "repeat int v=%lld matches repeated offers",
                     (long long)vals[vi]);
            ASSERT_TRUE(ok, msg);
        }
    }

    /* INT overflow: 4 offers of 2^125 — the 4th offer fails ERANGE and so
     * does the repeat. */
    {
        AggNum a = {0}, b = {0};
        agg_i128 v = ((agg_i128)1) << 125;
        int ofail = 0;
        if (agg_num_offer_int(&b, v) == 0 &&
            agg_num_offer_int(&b, v) == 0 &&
            agg_num_offer_int(&b, v) == 0) {
            errno = 0;
            if (agg_num_offer_int(&b, v) == -1 && errno == ERANGE) ofail = 1;
        }
        errno = 0;
        int ra = agg_num_offer_int(&a, v) == 0 ? agg_num_repeat(&a, AGC_INT, 4, &e) : -2;
        ASSERT_TRUE(ofail == 1 && ra == -1 && errno == ERANGE,
                    "repeat int overflow reports ERANGE like repeated offers");
    }

    /* DEC: repeat matches replay beyond double precision. */
    {
        static const char *const dvals[] = {
            "99999999999999999999.9999999999", "-0.0000000001"
        };
        static const int64_t times[] = { 1, 3, 1000, 4099 };
        for (size_t vi = 0; vi < sizeof dvals / sizeof *dvals; vi++) {
            dec_t parsed;
            dec_init(&parsed);
            dec_status st = dec_parse(dvals[vi], strlen(dvals[vi]), &parsed,
                                      NULL, 0);
            int ok = 0;
            if (st == DEC_OK) {
                ok = 1;
                for (size_t ti = 0; ok && ti < sizeof times / sizeof *times; ti++) {
                    AggNum a = {0}, b = {0};
                    if (agg_num_offer_decimal(&a, &e, &parsed) != 0) { ok = 0; }
                    else if (agg_num_repeat(&a, AGC_DEC, times[ti], &e) != 0) ok = 0;
                    else {
                        for (int64_t k = 0; ok && k < times[ti]; k++)
                            if (agg_num_offer_decimal(&b, &e, &parsed) != 0) ok = 0;
                        if (ok && (dec_cmp(&a.u.d.sum, &b.u.d.sum) != 0 ||
                                   a.u.d.sum.dscale != b.u.d.sum.dscale ||
                                   dec_cmp(&a.u.d.min, &b.u.d.min) != 0 ||
                                   dec_cmp(&a.u.d.max, &b.u.d.max) != 0)) ok = 0;
                    }
                    agg_num_release(&a, AGC_DEC, &e);
                    agg_num_release(&b, AGC_DEC, &e);
                }
            }
            char msg[128];
            snprintf(msg, sizeof msg, "repeat decimal %s matches repeated offers",
                     dvals[vi]);
            ASSERT_TRUE(ok, msg);
            dec_free(&parsed);
        }
    }

    /* FLT: refused (floating sums depend on addition order). */
    {
        AggNum f = {0};
        int rr = -2;
        if (agg_num_offer_double(&f, 1.5) == 0) {
            errno = 0;
            rr = agg_num_repeat(&f, AGC_FLT, 3, &e);
        }
        ASSERT_TRUE(rr == -1 && errno == EINVAL, "repeat float is refused (EINVAL)");
    }

    agg_env_free(&e);
    agg_topn_test_reset();
    return 0;
}

/* ---- T5: the hash path honours the same tie order (forced hash) ---------- */

static int setup_tqg(void);
static int setup_tqh(void);

static int test_topn_exact_forced_hash_ties(void) {
    int rc = 1;
    db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;
    char *r = q("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);
    if (setup_tqa()) goto cleanup;
    if (setup_tqg()) goto cleanup;
    if (setup_tqh()) goto cleanup;

    /* Forced-hash reruns of the T1 requests: same tie order. */
    r = run_forced(agg_json("tqa", "k", N_SPECS, "n", "desc", 2, 0, NULL));
    ASSERT_EQ_STR(r, "[{\"k\":\"30\",\"n\":6},{\"k\":\"10\",\"n\":5}]\n",
                  "forced hash desc ties: page is 30 then 10");
    shard_db_free_result(r);
    r = run_forced(agg_json("tqa", "k", N_SPECS, "n", "asc", 2, 0, NULL));
    ASSERT_EQ_STR(r, "[{\"k\":\"10\",\"n\":5},{\"k\":\"20\",\"n\":5}]\n",
                  "forced hash asc ties: page is 10 then 20");
    shard_db_free_result(r);
    r = run_forced(agg_json("tqa", "k", N_SPECS, "n", "desc", 5, 0, NULL));
    ASSERT_EQ_STR(r,
                  "[{\"k\":\"30\",\"n\":6},{\"k\":\"10\",\"n\":5},"
                  "{\"k\":\"20\",\"n\":5}]\n",
                  "forced hash under cap: page is 30, 10, 20");
    shard_db_free_result(r);
    r = run_forced(agg_json("tqa", "k",
        "[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"sum\",\"field\":\"k\",\"alias\":\"s\"}]",
        "n", "desc", 2, 0, NULL));
    ASSERT_EQ_STR(r, "[{\"k\":\"30\",\"n\":6,\"s\":180},{\"k\":\"10\",\"n\":5,\"s\":50}]\n",
                  "forced hash mixed ties: exact page");
    shard_db_free_result(r);

    /* The stream answers the same mixed request with the same page. */
    const char *mx = agg_json("tqa", "k",
        "[{\"fn\":\"count\",\"alias\":\"n\"},{\"fn\":\"sum\",\"field\":\"k\",\"alias\":\"s\"}]",
        "n", "desc", 2, 0, NULL);
    r = run_measured(mx);
    ASSERT_EQ_INT(s_runs, 1, "stream mixed ties: stream served");
    ASSERT_EQ_STR(r, "[{\"k\":\"30\",\"n\":6,\"s\":180},{\"k\":\"10\",\"n\":5,\"s\":50}]\n",
                  "stream mixed ties: exact page");
    shard_db_free_result(r);

    /* Multi-field ties (hash path; multi-field group_by never streams). */
    r = run_measured(
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"tqg\","
        "\"group_by\":[\"a\",\"b\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"order_by\":\"n\",\"order\":\"desc\",\"limit\":10}");
    ASSERT_EQ_STR(r,
                  "[{\"a\":\"0\",\"b\":\"z\",\"n\":2},{\"a\":\"1\",\"b\":\"x\",\"n\":2},"
                  "{\"a\":\"1\",\"b\":\"y\",\"n\":2},{\"a\":\"1\",\"b\":null,\"n\":2},"
                  "{\"a\":null,\"b\":\"x\",\"n\":2}]\n",
                  "multi-field ties desc: exact page");
    shard_db_free_result(r);
    r = run_measured(
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"tqg\","
        "\"group_by\":[\"a\",\"b\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"order_by\":\"n\",\"order\":\"asc\",\"limit\":10}");
    ASSERT_EQ_STR(r,
                  "[{\"a\":\"0\",\"b\":\"z\",\"n\":2},{\"a\":\"1\",\"b\":\"x\",\"n\":2},"
                  "{\"a\":\"1\",\"b\":\"y\",\"n\":2},{\"a\":\"1\",\"b\":null,\"n\":2},"
                  "{\"a\":null,\"b\":\"x\",\"n\":2}]\n",
                  "multi-field ties asc: exact page");
    shard_db_free_result(r);

    /* Unset labels via the hash record-scan route (tqh has no index, so the
     * request can neither stream nor take the indexed group-by route). */
    r = run_measured(agg_json("tqh", "tm", N_SPECS, "n", "desc", 10, 0, NULL));
    ASSERT_TRUE(r && !SAFE_STRSTR(r, "\"error\""),
                "unset labels: ordered time aggregate succeeds");
    const char *p_empty = SAFE_STRSTR(r, "\"tm\":\"\"");
    const char *p_one = SAFE_STRSTR(r, "\"tm\":\"01:00:00\"");
    const char *p_null = SAFE_STRSTR(r, "\"tm\":null");
    ASSERT_TRUE(p_empty && p_one && p_null && p_empty < p_one && p_one < p_null,
                "unset labels: zero value first, then values, then NULL");
    shard_db_free_result(r);
    rc = 0;
cleanup:
    agg_topn_test_reset();
    drop_object("tqa");
    drop_object("tqg");
    drop_object("tqh");
    return rc;
}

/* tqg (T5): two each of (a,b) = (1,"x"), (1,"y"), (null,"x"), (0,"z"),
 * (1,null) — ten records, all counts 2, no index. */
static int setup_tqg(void) {
    if (create_object("tqg", "\"a:int\",\"b:varchar:8\"", "")) return 1;
    char body[2048]; size_t p = 0;
    SB_APPEND(body, p, sizeof body, "{");
    static const char *const pairs[][2] = {
        { "1", "\"x\"" }, { "1", "\"y\"" }, { "null", "\"x\"" },
        { "0", "\"z\"" }, { "1", "null" },
    };
    int first = 1;
    for (int i = 0; i < 5; i++) {
        for (int k = 0; k < 2; k++) {
            SB_APPEND(body, p, sizeof body, "%s\"g%02d\":{\"a\":%s,\"b\":%s}",
                      first ? "" : ",", i * 2 + k, pairs[i][0], pairs[i][1]);
            first = 0;
        }
    }
    SB_APPEND(body, p, sizeof body, "}");
    return load_records("tqg", body, 10);
}

/* tqh (T5): time field, no index; midnight ×2, "01:00:00" ×2, null ×2. */
static int setup_tqh(void) {
    if (create_object("tqh", "\"tm:time\"", "")) return 1;
    char body[1024]; size_t p = 0;
    SB_APPEND(body, p, sizeof body, "{");
    for (int i = 0; i < 6; i++) {
        const char *tm = i < 2 ? "\"00:00:00\"" : i < 4 ? "\"01:00:00\"" : "null";
        SB_APPEND(body, p, sizeof body, "%s\"h%d\":{\"tm\":%s}",
                  i ? "," : "", i, tm);
    }
    SB_APPEND(body, p, sizeof body, "}");
    return load_records("tqh", body, 6);
}

/* ---- T6: broad stream-vs-hash differential ------------------------------ */

static void diff_case(const char *tag, const char *g, const char *sname,
                      const char *specs, const char *dir, int limit,
                      int total, const char *criteria) {
    char shape[64], served[160], eq[160];
    snprintf(shape, sizeof shape, "%s/%s/%d%s%s", sname, dir, limit,
             total ? "/total" : "", criteria ? "/crit" : "");
    snprintf(served, sizeof served, "%sdiff %s/%s: stream served", tag, g, shape);
    snprintf(eq, sizeof eq, "%sdiff %s/%s: equals forced-hash output", tag, g, shape);
    const char *req = agg_json("tqd", g, specs, "n", dir, limit, total, criteria);
    char *r = run_measured(req);
    ASSERT_EQ_INT(s_runs, 1, served);
    eq_forced_hash(req, r, eq);
    shard_db_free_result(r);
}

static void diff_run_set(const char *tag) {
    static const struct { const char *g; int kind; } gs[] = {
        { "i", 1 }, { "nm", 1 }, { "ts", 1 }, { "x", 2 },
        { "e", 3 }, { "ip", 3 }, { "t", 0 },
    };
    for (size_t gi = 0; gi < sizeof gs / sizeof *gs; gi++) {
        char slist[768];
        const char *g = gs[gi].g;
        if (gs[gi].kind == 1)
            snprintf(slist, sizeof slist,
                "[{\"fn\":\"count\",\"alias\":\"n\"},"
                "{\"fn\":\"count\",\"field\":\"%s\",\"alias\":\"c\"},"
                "{\"fn\":\"sum\",\"field\":\"%s\",\"alias\":\"s\"},"
                "{\"fn\":\"avg\",\"field\":\"%s\",\"alias\":\"a\"},"
                "{\"fn\":\"min\",\"field\":\"%s\",\"alias\":\"mn\"},"
                "{\"fn\":\"max\",\"field\":\"%s\",\"alias\":\"mx\"}]",
                g, g, g, g, g);
        else if (gs[gi].kind == 2)
            snprintf(slist, sizeof slist,
                "[{\"fn\":\"count\",\"alias\":\"n\"},"
                "{\"fn\":\"count\",\"field\":\"%s\",\"alias\":\"c\"},"
                "{\"fn\":\"min\",\"field\":\"%s\",\"alias\":\"mn\"},"
                "{\"fn\":\"max\",\"field\":\"%s\",\"alias\":\"mx\"}]",
                g, g, g);
        else if (gs[gi].kind == 3)
            snprintf(slist, sizeof slist,
                "[{\"fn\":\"count\",\"alias\":\"n\"},"
                "{\"fn\":\"count\",\"field\":\"%s\",\"alias\":\"c\"}]", g);
        static const char *const dirs[] = { "desc", "asc" };
        for (int s = 0; s < (gs[gi].kind ? 2 : 1); s++) {
            const char *sname = s == 0 ? "N" : "X";
            const char *specs = s == 0 ? N_SPECS : slist;
            for (int d = 0; d < 2; d++) {
                for (int lm = 1; lm <= 10000; lm = lm == 1 ? 3 : 10000) {
                    diff_case(tag, g, sname, specs, dirs[d], lm, 0, NULL);
                    if (lm == 3)
                        diff_case(tag, g, sname, specs, dirs[d], lm, 1, NULL);
                    if (lm == 10000) break;
                }
            }
        }
    }
    diff_case(tag, "t", "N", N_SPECS, "desc", 3, 0,
              "[{\"field\":\"i\",\"op\":\"gte\",\"value\":\"1\"}]");
    diff_case(tag, "i", "N", N_SPECS, "desc", 3, 1,
              "[{\"field\":\"i\",\"op\":\"gte\",\"value\":\"1\"}]");
}

static int test_topn_exact_differential(void) {
    int rc = 1;
    int own_cpu = 0, own_io = 0;
    db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;
    char *r = q("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);
    if (setup_tqd()) goto cleanup;

    ASSERT_TRUE(parallel_io_pool_size() == 0 && parallel_pool_size() == 0,
                "setup: inline pass");
    diff_run_set("");

    own_cpu = parallel_pool_size() == 0;
    if (own_cpu) parallel_pool_init(4);
    own_io = parallel_io_pool_size() == 0;
    if (own_io) parallel_io_pool_init(4);
    ASSERT_TRUE(parallel_io_pool_size() > 0 && parallel_pool_size() > 0,
                "setup: pools running");
    diff_run_set("pool ");
    rc = 0;
cleanup:
    agg_topn_test_reset();
    if (own_cpu) parallel_pool_shutdown();
    if (own_io) parallel_io_pool_shutdown();
    drop_object("tqd");
    return rc;
}

/* ---- T7: a render failure is one error response, no partial body -------- */

static int test_topn_exact_render_failure(void) {
    int rc = 1;
    db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;
    char *r = q("{\"mode\":\"add-dir\",\"dir\":\"default\"}");
    shard_db_free_result(r);
    if (setup_tqc()) goto cleanup;

    agg_topn_test_reset();
    agg_topn_test_fail_repeat(1);
    r = q(agg_json("tqc", "d", M3_SPECS, "n", "desc", 1, 0, NULL));
    stats();
    ASSERT_TRUE(r && strncmp(r, "{\"error", 7) == 0,
                "render failure: one error response");
    ASSERT_TRUE(r && strchr(r, '[') == NULL, "render failure: no partial body");
    ASSERT_TRUE(s_runs == 0 && s_declines == 0 && s_repeats == 1,
                "render failure: the stream was the path that failed");
    shard_db_free_result(r);
    rc = 0;
cleanup:
    agg_topn_test_reset();
    drop_object("tqc");
    return rc;
}

TEST_REGISTER("test-agg-num-repeat", test_agg_num_repeat)
TEST_REGISTER("test-topn-exact-forced-hash-ties", test_topn_exact_forced_hash_ties)
TEST_REGISTER("test-topn-exact-differential", test_topn_exact_differential)
TEST_REGISTER("test-topn-exact-render-failure", test_topn_exact_render_failure)
