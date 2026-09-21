/* src/test/cases/test_multi_order_cursor.c
 * Multi-field order_by: buffer-sort path (no cursor), composite-index
 * keyset cursor pagination, and the rejection surface.
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

/* 12 rows; (tenant, day) chosen so tie groups straddle a limit-5 page
 * boundary. Expected (tenant, day) per key, sorted ascending by
 * (tenant, day, key-not-assumed): tie order inside a group is hash16-based
 * and must not be assumed. */
static const char *KEYS[12] = {
    "k01", "k02", "k03", "k04", "k05", "k06",
    "k07", "k08", "k09", "k10", "k11", "k12"
};
static const int TENANT[12] = { 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3 };
static const char *DAY[12] = {
    "2026-09-01", "2026-09-01", "2026-09-02", "2026-09-02",
    "2026-09-01", "2026-09-01", "2026-09-02", "2026-09-02",
    "2026-09-01", "2026-09-01", "2026-09-02", "2026-09-02"
};

/* Pull the ordered key sequence out of a find response. Wrapped responses
 * (cursor mode) are scanned between `"rows":` and `,"cursor":` so the
 * trailing token's "key" is not counted as a row; bare-array responses
 * (buffer-sort mode) are scanned in full. */
static int parse_row_keys(const char *resp, char out[][32], int cap) {
    if (!resp) return 0;
    const char *rows = strstr(resp, "\"rows\":");
    const char *p = rows ? rows + 7 : resp;
    const char *end = NULL;
    if (rows) {
        end = strstr(p, ",\"cursor\":");
        if (!end) return 0;
    } else {
        end = resp + strlen(resp);
    }
    int n = 0;
    while (p < end) {
        p = strstr(p, "\"key\":\"");
        if (!p || p >= end) break;
        p += 7;
        if (p >= end) break;
        const char *e = strchr(p, '"');
        if (!e || e > end || n >= cap) break;
        size_t len = (size_t)(e - p);
        if (len < sizeof(out[0])) { memcpy(out[n], p, len); out[n][len] = 0; n++; }
        p = e;
    }
    return n;
}

static int key_index(const char *k) {
    for (int i = 0; i < 12; i++)
        if (strcmp(KEYS[i], k) == 0) return i;
    return -1;
}

/* Walk all pages with the given order_by JSON literal + direction; collect
 * the global key sequence. Returns number of pages walked, or -1 on a
 * missing cursor mid-stream. */
static int walk_pages(TestClient *tc, const char *order_by_lit,
                      const char *dir, char out[][32], int *out_n) {
    char req[1024], cursor[512];
    cursor[0] = '\0';
    int pages = 0, total = 0;
    for (;;) {
        if (cursor[0])
            snprintf(req, sizeof(req),
                "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
                "\"criteria\":[],\"order_by\":%s,\"order\":\"%s\",\"limit\":5,"
                "\"cursor\":%s}", order_by_lit, dir, cursor);
        else
            snprintf(req, sizeof(req),
                "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
                "\"criteria\":[],\"order_by\":%s,\"order\":\"%s\",\"limit\":5,"
                "\"cursor\":null}", order_by_lit, dir);
        char *resp = NULL;
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "walk_pages: response");
        if (!resp) return -1;
        char keys[16][32];
        int n = parse_row_keys(resp, keys, 16);
        for (int i = 0; i < n && total < 64; i++) {
            snprintf(out[total++], 32, "%s", keys[i]);
        }
        /* Extract next cursor verbatim (or null). */
        const char *c = strstr(resp, "\"cursor\":");
        if (!c) { free(resp); return -1; }
        c += 9;
        if (strncmp(c, "null", 4) == 0) { free(resp); pages++; break; }
        const char *cs = strchr(c, '{');
        const char *ce = cs ? strchr(cs, '}') : NULL;
        if (!cs || !ce) { free(resp); return -1; }
        size_t clen = (size_t)(ce - cs) + 1;
        if (clen >= sizeof(cursor)) { free(resp); return -1; }
        memcpy(cursor, cs, clen); cursor[clen] = '\0';
        free(resp);
        pages++;
        if (pages > 10) break;
    }
    *out_n = total;
    return pages;
}

static int test_multi_order_cursor_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"mo\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"tenant:int\",\"day:date\",\"note:varchar:16\"],"
        "\"indexes\":[\"tenant\",\"tenant+day\",\"tenant+day+note\",\"note+day\"]}",
        &resp); free(resp); resp = NULL;

    for (int i = 0; i < 12; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"mo\","
            "\"key\":\"%s\",\"value\":{\"tenant\":%d,\"day\":\"%s\","
            "\"note\":\"n%02d\"}}", KEYS[i], TENANT[i], DAY[i], i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* ---- A. ASC multi-field cursor walk (array form) ---- */
    char seq[64][32];
    int total = 0;
    int pages = walk_pages(tc, "[\"tenant\",\"day\"]", "asc", seq, &total);
    ASSERT_EQ_INT(pages, 3, "asc: 12 rows / limit 5 → 3 pages");
    ASSERT_EQ_INT(total, 12, "asc: every row returned across pages");
    for (int i = 0; i < total; i++)
        for (int j = i + 1; j < total; j++)
            ASSERT_TRUE(strcmp(seq[i], seq[j]) != 0,
                        "asc: no key repeated across pages");
    int t_prev = -1;
    char d_prev[16] = "";
    for (int i = 0; i < total; i++) {
        int ki = key_index(seq[i]);
        ASSERT_TRUE(ki >= 0, "asc: known key");
        ASSERT_TRUE(TENANT[ki] > t_prev ||
                    (TENANT[ki] == t_prev &&
                     strcmp(DAY[ki], d_prev) >= 0),
                    "asc: global (tenant,day) order holds across pages");
        t_prev = TENANT[ki];
        snprintf(d_prev, sizeof(d_prev), "%s", DAY[ki]);
    }

    /* ---- B. DESC multi-field cursor walk (CSV form) ---- */
    memset(seq, 0, sizeof(seq));
    total = 0;
    pages = walk_pages(tc, "\"tenant,day\"", "desc", seq, &total);
    ASSERT_EQ_INT(pages, 3, "desc: 3 pages");
    ASSERT_EQ_INT(total, 12, "desc: 12 rows");
    t_prev = 1 << 30;
    snprintf(d_prev, sizeof(d_prev), "9999-99-99");
    for (int i = 0; i < total; i++) {
        int ki = key_index(seq[i]);
        ASSERT_TRUE(ki >= 0, "desc: known key");
        ASSERT_TRUE(TENANT[ki] < t_prev ||
                    (TENANT[ki] == t_prev &&
                     strcmp(DAY[ki], d_prev) <= 0),
                    "desc: global (tenant,day) reverse order holds");
        t_prev = TENANT[ki];
        snprintf(d_prev, sizeof(d_prev), "%s", DAY[ki]);
    }

    /* ---- B2. Single-field direction suffix drives the cursor walk ---- */
    memset(seq, 0, sizeof(seq));
    total = 0;
    pages = walk_pages(tc, "\"tenant:desc\"", "asc", seq, &total);
    ASSERT_EQ_INT(pages, 3, "B2: 3 pages");
    ASSERT_EQ_INT(total, 12, "B2: 12 rows");
    t_prev = 1 << 30;
    for (int i = 0; i < total; i++) {
        int ki = key_index(seq[i]);
        ASSERT_TRUE(ki >= 0, "B2: known key");
        ASSERT_TRUE(TENANT[ki] <= t_prev,
                    "B2: suffix :desc orders tenant descending");
        t_prev = TENANT[ki];
    }

    /* ---- C. Page-1 token carries both parts ---- */    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"tenant\",\"day\"],"
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"cursor\":{", "page 1 emits a cursor token");
    ASSERT_CONTAINS(resp, "\"tenant\":\"", "token has tenant part");
    ASSERT_CONTAINS(resp, "\"day\":\"", "token has day part");
    ASSERT_CONTAINS(resp, "\"key\":\"", "token has key part");
    free(resp); resp = NULL;

    /* ---- D. Non-cursor multi-field order_by (buffer-sort; bare array) ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"tenant\",\"day\"],"
        "\"order\":\"desc\",\"limit\":100}", &resp);
    ASSERT_NOT_NULL(resp, "buffer-sort: response");
    ASSERT_TRUE(resp[0] == '[', "buffer-sort: bare array (no wrapper)");
    {
        char ks[16][32];
        int n = parse_row_keys(resp, ks, 16);
        ASSERT_EQ_INT(n, 12, "buffer-sort: all rows");
        int prev_t = 1 << 30; char prev_d[16] = "9999-99-99";
        for (int i = 0; i < n; i++) {
            int ki = key_index(ks[i]);
            ASSERT_TRUE(ki >= 0, "buffer-sort: known key");
            ASSERT_TRUE(TENANT[ki] < prev_t ||
                        (TENANT[ki] == prev_t &&
                         strcmp(DAY[ki], prev_d) <= 0),
                        "buffer-sort: desc (tenant,day) order");
            prev_t = TENANT[ki];
            snprintf(prev_d, sizeof(prev_d), "%s", DAY[ki]);
        }
    }
    free(resp); resp = NULL;

    /* ---- D2. Mixed per-field directions, non-cursor → buffer-sort ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"tenant:asc\",\"day:desc\"],"
        "\"limit\":100}", &resp);
    ASSERT_NOT_NULL(resp, "mixed-dir buffer-sort: response");
    ASSERT_TRUE(resp[0] == '[', "mixed-dir buffer-sort: bare array");
    {
        char ks[16][32];
        int n = parse_row_keys(resp, ks, 16);
        ASSERT_EQ_INT(n, 12, "mixed-dir buffer-sort: all rows");
        int prev_t = -1;
        char prev_d[16] = "9999-99-99";
        for (int i = 0; i < n; i++) {
            int ki = key_index(ks[i]);
            ASSERT_TRUE(ki >= 0, "mixed-dir buffer-sort: known key");
            ASSERT_TRUE(TENANT[ki] > prev_t ||
                        (TENANT[ki] == prev_t &&
                         strcmp(DAY[ki], prev_d) <= 0),
                        "mixed-dir: tenant asc, day desc within tenant");
            prev_t = TENANT[ki];
            snprintf(prev_d, sizeof(prev_d), "%s", DAY[ki]);
        }
    }
    free(resp); resp = NULL;

    /* ---- D3. Selective criteria → D2 fetch-sort must not break
       multi-field order ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[{\"field\":\"tenant\",\"op\":\"eq\",\"value\":\"1\"}],"
        "\"order_by\":[\"tenant\",\"day\"],\"order\":\"asc\",\"limit\":100}",
        &resp);
    ASSERT_NOT_NULL(resp, "D3: response");
    ASSERT_TRUE(resp[0] == '[', "D3: bare array");
    {
        char ks[16][32];
        int n = parse_row_keys(resp, ks, 16);
        ASSERT_EQ_INT(n, 4, "D3: tenant=1 rows only");
        char prev_d[16] = "";
        for (int i = 0; i < n; i++) {
            int ki = key_index(ks[i]);
            ASSERT_TRUE(ki >= 0, "D3: known key");
            ASSERT_TRUE(strcmp(DAY[ki], prev_d) >= 0,
                        "D3: day ascending under selective filter");
            snprintf(prev_d, sizeof(prev_d), "%s", DAY[ki]);
        }
    }
    free(resp); resp = NULL;

    /* ---- D4. Single-field suffix, non-cursor → indexed-walk fast path ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":\"tenant:desc\",\"limit\":100}", &resp);
    ASSERT_NOT_NULL(resp, "D4: response");
    ASSERT_TRUE(resp[0] == '[', "D4: bare array");
    {
        char ks[16][32];
        int n = parse_row_keys(resp, ks, 16);
        ASSERT_EQ_INT(n, 12, "D4: all rows");
        int prev = 1 << 30;
        for (int i = 0; i < n; i++) {
            int ki = key_index(ks[i]);
            ASSERT_TRUE(ki >= 0, "D4: known key");
            ASSERT_TRUE(TENANT[ki] <= prev, "D4: tenant descending");
            prev = TENANT[ki];
        }
    }
    free(resp); resp = NULL;

    /* ---- E. Rejections ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"tenant\",\"note\"],"
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "multi-field order_by requires a composite index",
                    "missing composite rejected");
    ASSERT_CONTAINS(resp, "\"index\":\"tenant+note\"",
                    "error names the composite to add");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"note\",\"day\"],"
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "fixed-width", "varchar non-final part rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"a\",\"b\",\"c\",\"d\",\"e\"],"
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "at most 4 fields", ">4 parts rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"tenant:asc\",\"day:desc\"],"
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "one shared order direction",
                    "mixed directions + cursor rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"day:sideways\"],"
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "invalid per-field order direction",
                    "malformed direction suffix rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"day:sideways\"],"
        "\"limit\":5}", &resp);
    ASSERT_CONTAINS(resp, "invalid per-field order direction",
                    "suffix error without cursor (validation path)");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":\"tenant,\","
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "empty field name", "trailing comma rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":\" tenant , day \","
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"rows\":", "whitespace-tolerant split");
    free(resp); resp = NULL;

    /* ---- F. Cursor token missing a part → clear error ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"tenant\",\"day\"],"
        "\"order\":\"asc\",\"limit\":5,"
        "\"cursor\":{\"tenant\":\"1\",\"key\":\"k01\"}}", &resp);
    ASSERT_CONTAINS(resp, "cursor missing order_by field value",
                    "partial cursor token rejected");
    free(resp); resp = NULL;

    /* ---- G0. Plain find without order_by — untouched ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"limit\":100}", &resp);
    ASSERT_NOT_NULL(resp, "G0: response");
    ASSERT_TRUE(resp[0] == '[', "G0: bare array, no error");
    {
        char ks[16][32];
        int n = parse_row_keys(resp, ks, 16);
        ASSERT_EQ_INT(n, 12, "G0: all rows");
    }
    free(resp); resp = NULL;

    /* ---- G. Single-field regression: token shape unchanged ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":\"tenant\",\"order\":\"asc\","
        "\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"cursor\":{\"tenant\":\"", "single-field token");
    free(resp); resp = NULL;

    /* ---- H/I/J. Date-family buffer-sort ordering ----
       Inserted deliberately out of chronological order. Values are digit
       strings (date yyyyMMdd, datetime yyyyMMddHHmmss, datetimems
       yyyyMMddHHmmssfff). Same-year dates and same-day datetimes
       distinguish themselves only if the sort compares the full value —
       and the 17-digit datetimems pair below differs in its final digit,
       past double precision. */
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"dt\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"dt\",\"object\":\"cal\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"d:date\",\"ts:datetime\",\"tms:datetimems\"]}",
        &resp); free(resp); resp = NULL;
    {
        static const char *K[5]  = { "c4", "c3", "c1", "c5", "c2" };
        static const char *D[5]  = { "20261001", "20260115", "20251231",
                                     "20270203", "20260915" };
        static const char *TS[5] = { "20261001150000", "20261001090000",
                                     "20251231235959", "20270203060102",
                                     "20260915040506" };
        static const char *TM[5] = { "20261001150000002",
                                     "20261001150000001",
                                     "20251231235959999",
                                     "20270203060102003",
                                     "20260915040506007" };
        for (int i = 0; i < 5; i++) {
            char req[256];
            snprintf(req, sizeof(req),
                "{\"mode\":\"insert\",\"dir\":\"dt\",\"object\":\"cal\","
                "\"key\":\"%s\",\"value\":{\"d\":\"%s\",\"ts\":\"%s\","
                "\"tms\":\"%s\"}}", K[i], D[i], TS[i], TM[i]);
            tc_request(tc, req, &resp); free(resp); resp = NULL;
        }
        static const char *EXP_D_ASC[5]  = { "c1", "c3", "c2", "c4", "c5" };
        static const char *EXP_D_DESC[5] = { "c5", "c4", "c2", "c3", "c1" };
        static const char *EXP_T_ASC[5]  = { "c1", "c2", "c3", "c4", "c5" };
        static const char *EXP_T_DESC[5] = { "c5", "c4", "c3", "c2", "c1" };

        tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"dt\",\"object\":\"cal\","
            "\"criteria\":[],\"order_by\":\"d\",\"order\":\"asc\","
            "\"limit\":100}", &resp);
        {
            char ks[16][32];
            int n = resp ? parse_row_keys(resp, ks, 16) : 0;
            ASSERT_EQ_INT(n, 5, "H: date asc row count");
            for (int i = 0; i < n && i < 5; i++)
                ASSERT_TRUE(strcmp(ks[i], EXP_D_ASC[i]) == 0,
                            "H: date asc chronological (year/month/day)");
        }
        free(resp); resp = NULL;

        tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"dt\",\"object\":\"cal\","
            "\"criteria\":[],\"order_by\":\"d\",\"order\":\"desc\","
            "\"limit\":100}", &resp);
        {
            char ks[16][32];
            int n = resp ? parse_row_keys(resp, ks, 16) : 0;
            ASSERT_EQ_INT(n, 5, "H: date desc row count");
            for (int i = 0; i < n && i < 5; i++)
                ASSERT_TRUE(strcmp(ks[i], EXP_D_DESC[i]) == 0,
                            "H: date desc reverse-chronological");
        }
        free(resp); resp = NULL;

        tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"dt\",\"object\":\"cal\","
            "\"criteria\":[],\"order_by\":\"ts\",\"order\":\"asc\","
            "\"limit\":100}", &resp);
        {
            char ks[16][32];
            int n = resp ? parse_row_keys(resp, ks, 16) : 0;
            ASSERT_EQ_INT(n, 5, "I: datetime asc row count");
            for (int i = 0; i < n && i < 5; i++)
                ASSERT_TRUE(strcmp(ks[i], EXP_T_ASC[i]) == 0,
                            "I: datetime asc (same-day times order)");
        }
        free(resp); resp = NULL;

        tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"dt\",\"object\":\"cal\","
            "\"criteria\":[],\"order_by\":\"tms\",\"order\":\"asc\","
            "\"limit\":100}", &resp);
        {
            char ks[16][32];
            int n = resp ? parse_row_keys(resp, ks, 16) : 0;
            ASSERT_EQ_INT(n, 5, "J: datetimems asc row count");
            for (int i = 0; i < n && i < 5; i++)
                ASSERT_TRUE(strcmp(ks[i], EXP_T_ASC[i]) == 0,
                            "J: datetimems asc (ms pair orders, not ties)");
        }
        free(resp); resp = NULL;

        tc_request(tc,
            "{\"mode\":\"find\",\"dir\":\"dt\",\"object\":\"cal\","
            "\"criteria\":[],\"order_by\":\"tms\",\"order\":\"desc\","
            "\"limit\":100}", &resp);
        {
            char ks[16][32];
            int n = resp ? parse_row_keys(resp, ks, 16) : 0;
            ASSERT_EQ_INT(n, 5, "J: datetimems desc row count");
            for (int i = 0; i < n && i < 5; i++)
                ASSERT_TRUE(strcmp(ks[i], EXP_T_DESC[i]) == 0,
                            "J: datetimems desc");
        }
        free(resp); resp = NULL;
    }

    /* ---- K. Empty vs non-empty varchar (buffer-sort) ----
       A zero-length typed key is a valid key: an empty varchar must not
       compare equal to a non-empty one. Insertion order is reversed
       relative to the expected sort so an equality tie would reproduce
       insertion order and fail. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"dt\",\"object\":\"ev\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"note:varchar:8\"]}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"dt\",\"object\":\"ev\","
        "\"key\":\"e2\",\"value\":{\"note\":\"a\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"dt\",\"object\":\"ev\","
        "\"key\":\"e1\",\"value\":{\"note\":\"\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"dt\",\"object\":\"ev\","
        "\"criteria\":[],\"order_by\":\"note\",\"order\":\"asc\","
        "\"limit\":10}", &resp);
    {
        char ks[4][32];
        int n = resp ? parse_row_keys(resp, ks, 4) : 0;
        ASSERT_EQ_INT(n, 2, "K: asc row count");
        ASSERT_TRUE(n == 2 && strcmp(ks[0], "e1") == 0 && strcmp(ks[1], "e2") == 0,
                    "K: empty varchar sorts before non-empty (asc)");
    }
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"dt\",\"object\":\"ev\","
        "\"criteria\":[],\"order_by\":\"note\",\"order\":\"desc\","
        "\"limit\":10}", &resp);
    {
        char ks[4][32];
        int n = resp ? parse_row_keys(resp, ks, 4) : 0;
        ASSERT_EQ_INT(n, 2, "K: desc row count");
        ASSERT_TRUE(n == 2 && strcmp(ks[0], "e2") == 0 && strcmp(ks[1], "e1") == 0,
                    "K: non-empty varchar sorts before empty (desc)");
    }
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-multi-order-cursor", test_multi_order_cursor_run)
