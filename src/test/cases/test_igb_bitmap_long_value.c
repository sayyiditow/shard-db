/* Regression: AUD-QRY-6 — group_by over a bitmap-indexed field holding
 * dict values longer than the 1024-byte collection rows. The collection
 * callback copied min(vlen,1024) bytes but recorded the FULL vlen, so
 * the dedup memcmp and every dict-lookup consumer read vlen bytes from
 * a narrower row (stack OOB read — the Task 0 ASan artifact proves it
 * via lv_wide) and the truncated copy never matches its dict value →
 * count 0 → the group is silently dropped at emission: a 200-OK with
 * missing groups, while the identical query on a no-index twin returns
 * the DOCUMENTED E2BIG error (labels >511 bytes,
 * docs/query-protocol/aggregate.md:149-151). Fix: the callback refuses
 * values longer than the row width (latching QRY-5's overflow signal)
 * and both bitmap IGB blocks consult the latch BEFORE the bm_n == 0
 * early-outs — a long-values-only dict collects nothing AND latches,
 * which must route to the record-scan executor (documented E2BIG), not
 * emit a silent empty success.
 *
 * Post-fix contract: ERROR PARITY — the bitmap twin and the no-index
 * oracle classify identically (both E2BIG, same error text). Exact
 * groups for >511-byte labels is not a servable outcome under the wire
 * contract; error parity is the honest end state.
 *
 * Shapes (docs/plans/2026-11-19-audit-qry6-*.md Task 1):
 *   1  lv_plain (no-index oracle) errors E2BIG on long labels — the
 *      documented contract, true on base and post-fix
 *   2  lv_bm (bitmap twin, identical rows) classifies the SAME as the
 *      oracle, both ERR with the pinned E2BIG text — RED on base:
 *      OK with only the short labels (silent under-report)
 *   3  lv_only (ONLY long values) errors E2BIG post-fix — RED on base:
 *      silent empty group list (the guard-ordering class)
 *   4  short-values control pair: both OK, label sets + counts equal
 *      (fast path retention for short values)
 *   5  lv_wide — ASan-only fixture, NO functional assertion (fired
 *      last so a base-ASan abort cannot shadow assertions 1-4): 257
 *      keys probe-routed to the last-walked shard (shard 7), then
 *      updated in place to 255 distinct short labels + two distinct
 *      2000-byte values, so the shared collection array's LAST row
 *      (index 255) holds a long value — the second long's dedup
 *      memcmp reads 255*1024 + 2000 = 263,120 bytes' start across the
 *      262,144-byte stack array end and ASan fires in
 *      bm_collect_uniq_cb on base. Placement engineering is REQUIRED:
 *      with uncontrolled key routing the longs collect at row
 *      ~n_shards*shorts_per_shard and no read crosses the array end
 *      (route finding in PLAN_NOTES). ERR on base and post-fix alike
 *      (base: QRY-5 cap latch → scan → E2BIG; post-fix: the row-width
 *      latch → same).
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

/* Pinned from the actual oracle response at execution (standalone
 * daemon probe, 2026-11-22): agg_report_error(E2BIG) via agg_row_label's
 * over-cap check. */
#define E2BIG_SUBSTR \
    "group_by value exceeds aggregate key limit (511 bytes per value, " \
    "4096 bytes per composite key) or contains NUL"

#define VLEN 2000   /* long dict value length (> 1024 row width, < 0xffff) */

typedef struct {
    char label[600];
    long count;
} GEntry;

static int gentry_cmp(const void *a, const void *b) {
    return strcmp(((const GEntry *)a)->label, ((const GEntry *)b)->label);
}

static int is_err(const char *resp) {
    return resp && SAFE_STRSTR(resp, "\"error\"") != NULL;
}

/* Extract ("v":"<label>","n":<int>) pairs from an OK group_by=v
 * response; sort by label. Returns the entry count. */
static int extract_groups(const char *resp, GEntry *out, int cap) {
    int n = 0;
    const char *p = resp;
    while (p && (p = SAFE_STRSTR(p, "\"v\":\"")) != NULL && n < cap) {
        p += strlen("\"v\":\"");
        const char *q = SAFE_STRSTR(p, "\"n\":");
        if (!q) break;
        size_t lbl = (size_t)(q - p);
        while (lbl > 0 && (p[lbl - 1] == ',' || p[lbl - 1] == '"')) lbl--;
        if (lbl >= sizeof(out[n].label)) lbl = sizeof(out[n].label) - 1;
        memcpy(out[n].label, p, lbl);
        out[n].label[lbl] = '\0';
        out[n].count = atol(q + strlen("\"n\":"));
        n++;
    }
    qsort(out, (size_t)n, sizeof(GEntry), gentry_cmp);
    return n;
}

/* Build one 2000-byte value with the given prefix into buf. */
static void make_long(char *buf, size_t cap, const char *prefix) {
    size_t plen = strlen(prefix);
    ASSERT_TRUE(plen + 1 < cap, "long value buf");
    snprintf(buf, cap, "%s", prefix);
    memset(buf + plen, 'x', VLEN - plen - 1);
    buf[VLEN - 1] = '\0';
}

/* Bulk-insert n rows calling row(i, key, value) into a JSON array. */
typedef void (*row_fn)(int i, char *key, size_t keycap, char *val,
                       size_t valcap);

static void insert_fixture(TestClient *tc, const char *obj, int n,
                           row_fn row) {
    /* values are at most VLEN bytes; keys at most 16 */
    const size_t reclen = (size_t)VLEN + 96;
    char *body = malloc((size_t)n * reclen + 16);
    ASSERT_NOT_NULL(body, "alloc body");
    size_t off = 0;
    off += (size_t)snprintf(body + off, 8, "[");
    for (int i = 0; i < n; i++) {
        char key[32], val[VLEN + 16];
        row(i, key, sizeof(key), val, sizeof(val));
        /* value is textual JSON content — the long fillers are 'x'
         * (no escaping needed); short labels are alnum */
        off += (size_t)snprintf(body + off, reclen, "%s{\"key\":\"%s\","
                                "\"value\":{\"v\":\"%s\"}}",
                                i ? "," : "", key, val);
    }
    off += (size_t)snprintf(body + off, 8, "]");
    char *req = malloc(off + 160);
    ASSERT_NOT_NULL(req, "alloc request");
    snprintf(req, off + 160,
             "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
             "\"object\":\"%s\",\"records\":%s}", obj, body);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "bulk insert accepted");
    free(resp);
    free(req);
    free(body);
}

/* 50 rows: L0/L1/L2 (2000 B) x10 + S0/S1 x10. Same rows for the oracle
 * and the bitmap twin (row_fn shared). */
static void row_mixed(int i, char *key, size_t keycap, char *val,
                      size_t valcap) {
    snprintf(key, keycap, "k%02d", i);
    if (i < 10)      make_long(val, valcap, "L0");
    else if (i < 20) make_long(val, valcap, "L1");
    else if (i < 30) make_long(val, valcap, "L2");
    else if (i < 40) snprintf(val, valcap, "S0");
    else             snprintf(val, valcap, "S1");
}

/* 30 rows, ONLY long values (guard-ordering class). */
static void row_longonly(int i, char *key, size_t keycap, char *val,
                         size_t valcap) {
    snprintf(key, keycap, "k%02d", i);
    if (i < 10)      make_long(val, valcap, "L0");
    else if (i < 20) make_long(val, valcap, "L1");
    else             make_long(val, valcap, "L2");
}

/* lv_wide helpers. Detectability (see Task 0): the OOB fires only when
 * a long value sits in the LAST collection row (index 255) of the
 * shared 256-row array — the collection walks the per-shard dicts in
 * shard order, so with uncontrolled key routing a long value collects
 * at row ~n_shards*shorts_per_shard << 255 and no read crosses the
 * array end. Construction: probe-insert candidate keys carrying a
 * 4-byte value and keep those whose shard-stats records counter
 * increments (shard 7 — the LAST-walked shard); delete the rejects.
 * Then UPDATE the 257 kept keys in place — 255 distinct short labels
 * first, then the two 2000-byte values — so shard 7's dict (the only
 * dict that ever sees a long value; dict entries persist through
 * delete, so no rejected insert may ever carry one) ends
 * [s000..s254, W0, W1]: s001..s254 fill rows 1..255's predecessors to
 * n=255, W0 stores (truncated) at row 255, and W1's dedup memcmp reads
 * vals[255] for 2000 bytes — 255*1024 + 2000 = 263,120 > 262,144. */
static long shard_records(TestClient *tc, int shard_want, const char *obj) {
    char req[256];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"shard-stats\",\"dir\":\"default\","
             "\"object\":\"%s\"}", obj);
    tc_request(tc, req, &resp);
    ASSERT_NOT_NULL(resp, "shard-stats response");
    long rec = -1;
    if (resp) {
        char needle[32];
        snprintf(needle, sizeof(needle), "\"keyfile\":%d", shard_want);
        const char *p = SAFE_STRSTR(resp, needle);
        if (p) {
            const char *q = SAFE_STRSTR(p, "\"records\":");
            if (q) rec = atol(q + strlen("\"records\":"));
        }
    }
    free(resp);
    return rec;
}

/* 20 rows over 2 short values (control pair rows). */
static void row_short(int i, char *key, size_t keycap, char *val,
                      size_t valcap) {
    snprintf(key, keycap, "k%02d", i);
    snprintf(val, valcap, "S%d", i % 2);
}

static void create_obj(TestClient *tc, const char *obj, const char *idx) {
    char req[512];
    char *resp = NULL;
    snprintf(req, sizeof(req),
             "{\"mode\":\"create-object\",\"dir\":\"default\","
             "\"object\":\"%s\",\"splits\":8,\"max_key\":16,"
             "\"fields\":[\"v:varchar:4096\"]%s%s}",
             obj, idx ? ",\"indexes\":[" : "", idx ? idx : "");
    tc_request(tc, req, &resp);
    ASSERT_CONTAINS(resp, "\"created\"", "object created");
    free(resp);
}

static void group_v(TestClient *tc, const char *obj, char **resp) {
    char req[512];
    snprintf(req, sizeof(req),
             "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"%s\","
             "\"group_by\":\"v\",\"aggregates\":[{\"fn\":\"count\","
             "\"alias\":\"n\"}]}", obj);
    tc_request(tc, req, resp);
    ASSERT_NOT_NULL(*resp, "group_by response");
}

static int test_igb_bitmap_long_value_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    create_obj(tc, "lv_plain", NULL);                 /* oracle */
    create_obj(tc, "lv_bm", "\"v:bitmap(8)\"");
    create_obj(tc, "lv_only", "\"v:bitmap(4)\"");
    create_obj(tc, "lv_short_plain", NULL);           /* control oracle */
    create_obj(tc, "lv_short_bm", "\"v:bitmap(4)\"");
    create_obj(tc, "lv_wide", "\"v:bitmap(300)\"");   /* ASan-only */

    insert_fixture(tc, "lv_plain", 50, row_mixed);
    insert_fixture(tc, "lv_bm", 50, row_mixed);
    insert_fixture(tc, "lv_only", 30, row_longonly);
    insert_fixture(tc, "lv_short_plain", 20, row_short);
    insert_fixture(tc, "lv_short_bm", 20, row_short);

    /* 1: the oracle errors E2BIG on long labels (documented contract;
     * true on base and post-fix). */
    group_v(tc, "lv_plain", &resp);
    ASSERT_TRUE(is_err(resp),
                "case1: no-index oracle errors E2BIG on >511B labels");
    ASSERT_CONTAINS(resp, E2BIG_SUBSTR, "case1: pinned E2BIG text");
    free(resp); resp = NULL;

    /* 2: ERROR PARITY — the bitmap twin classifies the same as the
     * oracle, both ERR with the pinned text. RED on base: lv_bm is OK
     * with only the short labels (silent under-report + OOB reads). */
    {
        group_v(tc, "lv_bm", &resp);
        int bm_err = is_err(resp);
        TAP_DIAG("# case2 base-state diag: lv_bm %s\n",
                 bm_err ? "ERR (parity)" : "OK (silent under-report)");
        ASSERT_TRUE(bm_err,
                    "case2: bitmap twin matches oracle classification (ERR)");
        ASSERT_CONTAINS(resp, E2BIG_SUBSTR,
                        "case2: bitmap twin same pinned E2BIG text");
        free(resp); resp = NULL;
    }

    /* 3: long-values-only object → ERR (not a silent empty group
     * list). RED on base: OK with zero groups (the guard-ordering
     * class — bm_n == 0 early-out before any latch consult). */
    {
        group_v(tc, "lv_only", &resp);
        TAP_DIAG("# case3 base-state diag: lv_only %s\n",
                 is_err(resp) ? "ERR" : "OK (silent empty)");
        ASSERT_TRUE(is_err(resp),
                    "case3: long-values-only object errors E2BIG");
        ASSERT_CONTAINS(resp, E2BIG_SUBSTR, "case3: pinned E2BIG text");
        free(resp); resp = NULL;
    }

    /* 4: short-values control pair — both OK, label sets + counts
     * equal (the fast path stays on for short values). */
    {
        GEntry va[16], vb[16];
        int na, nb;
        group_v(tc, "lv_short_plain", &resp);
        ASSERT_TRUE(!is_err(resp), "case4: control oracle OK");
        na = extract_groups(resp, va, 16);
        free(resp); resp = NULL;
        group_v(tc, "lv_short_bm", &resp);
        ASSERT_TRUE(!is_err(resp), "case4: control bitmap OK");
        nb = extract_groups(resp, vb, 16);
        free(resp); resp = NULL;
        ASSERT_EQ_INT(na, 2, "case4: two short labels");
        ASSERT_EQ_INT(na, nb, "case4: same label count");
        for (int i = 0; i < na && i < nb; i++) {
            ASSERT_EQ_STR(va[i].label, vb[i].label, "case4: labels equal");
            ASSERT_EQ_INT((int)va[i].count, (int)vb[i].count,
                          "case4: counts equal");
        }
    }

    /* 5: lv_wide — fired LAST so the long value sits in the final
     * collection row of the last-walked shard's dict (the placement the
     * memory-safety proof needs; see the file header and the
     * shard_records comment). On a base ASan build this aborts the
     * daemon with the stack-buffer-overflow report; post-fix the
     * row-width latch fires at W0's collection and the request returns
     * cleanly — E2BIG via the scan executor, parity with the oracle
     * (asserted: not just crash-free, correctly classified). */
    {
        char req[VLEN + 512];
        char *kept[257];
        int nkept = 0, tries = 0;
        while (nkept < 257 && tries < 40000) {
            char key[16];
            snprintf(key, sizeof(key), "b%05d", tries++);
            long before = shard_records(tc, 7, "lv_wide");
            snprintf(req, sizeof(req),
                     "{\"mode\":\"insert\",\"dir\":\"default\","
                     "\"object\":\"lv_wide\",\"key\":\"%s\","
                     "\"value\":{\"v\":\"s000\"}}", key);
            tc_request(tc, req, &resp);
            ASSERT_NOT_NULL(resp, "lv_wide probe insert");
            free(resp); resp = NULL;
            if (shard_records(tc, 7, "lv_wide") == before + 1) {
                kept[nkept] = strdup(key);
                ASSERT_NOT_NULL(kept[nkept], "lv_wide strdup");
                nkept++;
            } else {
                snprintf(req, sizeof(req),
                         "{\"mode\":\"delete\",\"dir\":\"default\","
                         "\"object\":\"lv_wide\",\"key\":\"%s\"}", key);
                tc_request(tc, req, &resp);
                free(resp); resp = NULL;
            }
        }
        ASSERT_EQ_INT(nkept, 257, "lv_wide: shard-7 probe kept 257 keys");
        char val[16];
        for (int i = 0; i < 255; i++) {
            snprintf(val, sizeof(val), "s%03d", i);
            snprintf(req, sizeof(req),
                     "{\"mode\":\"update\",\"dir\":\"default\","
                     "\"object\":\"lv_wide\",\"key\":\"%s\","
                     "\"value\":{\"v\":\"%s\"}}", kept[i], val);
            tc_request(tc, req, &resp);
            ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                        "lv_wide label update");
            free(resp); resp = NULL;
        }
        for (int w = 0; w < 2; w++) {
            char lv[VLEN + 8];
            make_long(lv, sizeof(lv), w ? "W1" : "W0");
            snprintf(req, sizeof(req),
                     "{\"mode\":\"update\",\"dir\":\"default\","
                     "\"object\":\"lv_wide\",\"key\":\"%s\","
                     "\"value\":{\"v\":\"%s\"}}",
                     kept[255 + w], lv);
            tc_request(tc, req, &resp);
            ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                        "lv_wide long update");
            free(resp); resp = NULL;
        }
        for (int i = 0; i < nkept; i++) free(kept[i]);
        group_v(tc, "lv_wide", &resp);
        TAP_DIAG("# case5 base-state diag: lv_wide %s\n",
                 is_err(resp) ? "ERR" : "OK");
        ASSERT_TRUE(is_err(resp),
                    "case5: long-value dict row errors E2BIG (clean "
                    "classification, not just crash-free)");
        ASSERT_CONTAINS(resp, E2BIG_SUBSTR,
                        "case5: pinned E2BIG text");
        free(resp); resp = NULL;
    }

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-igb-bitmap-long-value", test_igb_bitmap_long_value_run)
