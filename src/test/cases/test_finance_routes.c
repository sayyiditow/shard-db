/* R6 route matrix for the finance shapes on packed storage: bulk insert,
   exact per-row read-back (including bare-numeric scale), indexed cursor
   order in both directions, bulk delete + vacuum compact, exact aggregate,
   backup/restore and a clean restart. Textual comparison throughout. */
#include "test_runner.h"
#include "bappend.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FR_N 600

typedef struct {
    long long scaled;     /* amt * 10^10 */
    char amt[48];
    char bare[32];
    char cur[32];
} FrRow;

static FrRow fr_rows[FR_N];

static void fr_build(void) {
    for (int i = 0; i < FR_N; i++) {
        FrRow *r = &fr_rows[i];
        int n = i + 1;
        long long ip = (n % 7 == 0) ? -(long long)n : n;
        long long frac = ((long long)n * 7919LL) % 10000000000LL;
        snprintf(r->amt, sizeof(r->amt), "%s%lld.%010lld", ip < 0 ? "-" : "",
                 ip < 0 ? -ip : ip, frac);
        r->scaled = (ip < 0 ? -1 : 1) * ((ip < 0 ? -ip : ip) * 10000000000LL + frac);
        int scale = n % 5;
        if (scale == 0) snprintf(r->bare, sizeof(r->bare), "%d", n);
        else {
            char f[8];
            memset(f, '5', (size_t)scale); f[scale] = '\0';
            snprintf(r->bare, sizeof(r->bare), "%d.%s", n, f);
        }
        snprintf(r->cur, sizeof(r->cur), "%d.%04d", n, (n * 37) % 10000);
    }
}

static char *fr_req(TestClient *tc, const char *json) {
    char *resp = NULL;
    int rc = tc_request(tc, json, &resp);
    ASSERT_EQ_INT(rc, 0, "finance transport");
    return resp;
}

static int fr_has(const char *resp, const char *key, const char *expected) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":", key);
    const char *p = resp ? strstr(resp, pat) : NULL;
    if (!p) return 0;
    p += strlen(pat);
    size_t len = strcspn(p, ",}] \t\r\n");
    return len == strlen(expected) && memcmp(p, expected, len) == 0;
}

static int fr_cmp_scaled(const void *a, const void *b) {
    const FrRow *x = *(const FrRow *const *)a, *y = *(const FrRow *const *)b;
    return (x->scaled > y->scaled) - (x->scaled < y->scaled);
}

static void fr_verify_all_rows(TestClient *tc, const char *phase, int skip_deleted) {
    int bad = 0, seen = 0;
    for (int i = 0; i < FR_N; i++) {
        int deleted = skip_deleted && ((i + 1) % 3 == 0);
        char req[160];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"fin\",\"object\":\"ledger\",\"key\":\"r%04d\"}", i + 1);
        char *resp = fr_req(tc, req);
        if (deleted) {
            if (resp && !strstr(resp, "\"error\"") && strstr(resp, "\"amt\"")) bad++;
        } else {
            seen++;
            if (!resp || !fr_has(resp, "amt", fr_rows[i].amt) ||
                !fr_has(resp, "bare", fr_rows[i].bare) ||
                !fr_has(resp, "cur", fr_rows[i].cur) || !strstr(resp, "note-")) bad++;
        }
        free(resp);
    }
    char desc[96];
    snprintf(desc, sizeof(desc), "%s: every row exact as text (%d checked)", phase, seen);
    ASSERT_EQ_INT(bad, 0, desc);
}

static void fr_expected_sum(int skip_deleted, char *out, size_t cap) {
    __int128 total = 0;
    for (int i = 0; i < FR_N; i++)
        if (!(skip_deleted && ((i + 1) % 3 == 0))) total += fr_rows[i].scaled;
    int neg = total < 0;
    if (neg) total = -total;
    long long whole = (long long)(total / 10000000000LL);
    long long frac = (long long)(total % 10000000000LL);
    snprintf(out, cap, "%s%lld.%010lld", neg ? "-" : "", whole, frac);
}

static void fr_verify_aggregate(TestClient *tc, const char *phase, int skip_deleted, int expect_count) {
    char want[64], desc[96];
    fr_expected_sum(skip_deleted, want, sizeof(want));
    char *resp = fr_req(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"fin\",\"object\":\"ledger\","
        "\"aggregates\":[{\"fn\":\"sum\",\"field\":\"amt\",\"alias\":\"s\"}]}");
    snprintf(desc, sizeof(desc), "%s: exact decimal sum %s", phase, want);
    ASSERT_CONTAINS(resp, want, desc);
    free(resp);
    resp = fr_req(tc, "{\"mode\":\"count\",\"dir\":\"fin\",\"object\":\"ledger\"}");
    snprintf(desc, sizeof(desc), "%s: record count", phase);
    ASSERT_EQ_INT(tu_parse_count(resp), expect_count, desc);
    free(resp);
}

/* Walks the keyset cursor to the end and returns the keys in delivery order. */
static int fr_walk_cursor(TestClient *tc, const char *order, char keys[][8], int cap) {
    int n = 0;
    char cursor[256] = "null";
    for (int page = 0; page < 20; page++) {
        char req[640];
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"fin\",\"object\":\"ledger\","
            "\"criteria\":[{\"field\":\"amt\",\"op\":\"exists\"}],\"order_by\":\"amt\",\"order\":\"%s\",\"limit\":100,\"cursor\":%s}", order, cursor);
        char *resp = fr_req(tc, req);
        if (!resp || strstr(resp, "\"error\"")) { free(resp); return -1; }

        const char *c = strstr(resp, "\"cursor\":");
        const char *p = resp;
        /* rows come before the trailing cursor object, which has its own "key" */
        while ((p = strstr(p, "\"key\":\"r")) != NULL && (!c || p < c)) {
            p += 7;
            if (n < cap) { memcpy(keys[n], p, 5); keys[n][5] = '\0'; }
            n++;
        }
        if (!c) { free(resp); break; }
        c += 9;
        if (strncmp(c, "null", 4) == 0) { free(resp); break; }
        const char *e = strchr(c, '}');
        if (!e) { free(resp); return -1; }
        size_t len = (size_t)(e - c) + 1;
        if (len >= sizeof(cursor)) { free(resp); return -1; }
        memcpy(cursor, c, len); cursor[len] = '\0';
        free(resp);
    }
    return n;
}

static int test_finance_routes_run(void) {
    fr_build();
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *r = fr_req(tc, "{\"mode\":\"add-dir\",\"dir\":\"fin\"}");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "add-dir");
    free(r);
    r = fr_req(tc,
        "{\"mode\":\"create-object\",\"dir\":\"fin\",\"object\":\"ledger\",\"splits\":8,"
        "\"max_key\":16,\"fields\":[\"amt:numeric:26,10\",\"bare:numeric\",\"cur:currency\","
        "\"note:varchar\"],\"indexes\":[\"amt\"]}");
    ASSERT_CONTAINS(r, "\"status\":\"created\"", "create ledger");
    free(r);

    size_t cap = (size_t)FR_N * 220 + 256;
    char *bulk = malloc(cap);
    ASSERT_NOT_NULL(bulk, "bulk buffer");
    if (!bulk) { tc_close(tc); test_env_stop(&env); return 1; }
    size_t len = (size_t)snprintf(bulk, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"fin\",\"object\":\"ledger\",\"records\":[");
    for (int i = 0; i < FR_N; i++)
        len = tappendf(bulk, cap, len,
            "%s{\"key\":\"r%04d\",\"value\":{\"amt\":\"%s\",\"bare\":\"%s\",\"cur\":\"%s\","
            "\"note\":\"note-%d\"}}", i ? "," : "", i + 1, fr_rows[i].amt,
            fr_rows[i].bare, fr_rows[i].cur, i + 1);
    snprintf(bulk + len, cap - len, "]}");
    r = fr_req(tc, bulk);
    free(bulk);
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "bulk-insert accepted");
    free(r);

    fr_verify_all_rows(tc, "after bulk insert", 0);
    fr_verify_aggregate(tc, "after bulk insert", 0, FR_N);

    /* indexed cursor order must equal exact numeric order, ASC and DESC */
    const FrRow *sorted[FR_N];
    for (int i = 0; i < FR_N; i++) sorted[i] = &fr_rows[i];
    qsort(sorted, FR_N, sizeof(sorted[0]), fr_cmp_scaled);
    static char keys[FR_N + 8][8];
    int n = fr_walk_cursor(tc, "asc", keys, FR_N + 8);
    ASSERT_EQ_INT(n, FR_N, "ASC cursor delivers every row once");
    int ordered = n == FR_N;
    for (int i = 0; ordered && i < FR_N; i++) {
        char want[8];
        snprintf(want, sizeof(want), "r%04d", (int)(sorted[i] - fr_rows) + 1);
        if (strcmp(keys[i], want) != 0) ordered = 0;
    }
    ASSERT_TRUE(ordered, "ASC cursor order equals exact decimal order");
    n = fr_walk_cursor(tc, "desc", keys, FR_N + 8);
    ASSERT_EQ_INT(n, FR_N, "DESC cursor delivers every row once");
    ordered = n == FR_N;
    for (int i = 0; ordered && i < FR_N; i++) {
        char want[8];
        snprintf(want, sizeof(want), "r%04d", (int)(sorted[FR_N - 1 - i] - fr_rows) + 1);
        if (strcmp(keys[i], want) != 0) ordered = 0;
    }
    ASSERT_TRUE(ordered, "DESC cursor order equals exact decimal order");

    /* delete every third record, compact, re-verify through every route */
    char del[FR_N * 12 + 128];
    size_t dl = (size_t)snprintf(del, sizeof(del),
        "{\"mode\":\"bulk-delete\",\"dir\":\"fin\",\"object\":\"ledger\",\"keys\":[");
    int first = 1;
    for (int i = 2; i < FR_N; i += 3) {
        dl = tappendf(del, sizeof(del), dl, "%s\"r%04d\"", first ? "" : ",", i + 1);
        first = 0;
    }
    snprintf(del + dl, sizeof(del) - dl, "]}");
    r = fr_req(tc, del);
    ASSERT_CONTAINS(r, "\"deleted\":200", "bulk-delete removed 200 records");
    free(r);
    r = fr_req(tc, "{\"mode\":\"vacuum\",\"dir\":\"fin\",\"object\":\"ledger\",\"compact\":true}");
    ASSERT_TRUE(r && !strstr(r, "\"error\""), "vacuum compact succeeds");
    free(r);
    fr_verify_all_rows(tc, "after delete + compact", 1);
    fr_verify_aggregate(tc, "after delete + compact", 1, FR_N - 200);
    r = fr_req(tc, "{\"mode\":\"orphaned\",\"dir\":\"fin\",\"object\":\"ledger\"}");
    ASSERT_EQ_INT(tu_parse_count(r), 0, "compact leaves no orphaned records");
    free(r);
    {
        char req[400];
        snprintf(req, sizeof(req),
            "{\"mode\":\"count\",\"dir\":\"fin\",\"object\":\"ledger\","
            "\"criteria\":[{\"field\":\"amt\",\"op\":\"eq\",\"value\":\"%s\"}]}", fr_rows[0].amt);
        r = fr_req(tc, req);
        ASSERT_EQ_INT(tu_parse_count(r), 1, "indexed eq on a surviving exact decimal");
        free(r);
        snprintf(req, sizeof(req),
            "{\"mode\":\"count\",\"dir\":\"fin\",\"object\":\"ledger\","
            "\"criteria\":[{\"field\":\"amt\",\"op\":\"eq\",\"value\":\"%s\"}]}", fr_rows[2].amt);
        r = fr_req(tc, req);
        ASSERT_EQ_INT(tu_parse_count(r), 0, "indexed eq on a deleted decimal finds nothing");
        free(r);
    }

    /* backup, damage, restore */
    r = fr_req(tc, "{\"mode\":\"backup\",\"dir\":\"fin\",\"object\":\"ledger\"}");
    char stamp[64] = {0};
    const char *bp = r ? strstr(r, "/backup/") : NULL;
    if (bp) sscanf(bp + 8, "%63[^\"]", stamp);
    ASSERT_TRUE(stamp[0] != '\0', "backup id parsed");
    free(r);
    r = fr_req(tc, "{\"mode\":\"bulk-delete\",\"dir\":\"fin\",\"object\":\"ledger\","
                   "\"keys\":[\"r0001\",\"r0002\"]}");
    free(r);
    {
        char req[320];
        snprintf(req, sizeof(req),
            "{\"mode\":\"restore\",\"dir\":\"fin\",\"object\":\"ledger\",\"from\":\"%s\",\"force\":true}", stamp);
        r = fr_req(tc, req);
        ASSERT_CONTAINS(r, "\"status\":\"restored\"", "restore succeeds");
        free(r);
    }
    fr_verify_all_rows(tc, "after restore", 1);
    fr_verify_aggregate(tc, "after restore", 1, FR_N - 200);

    /* clean restart of the same root */
    tc_close(tc);
    char root[sizeof(env.db_root)];
    snprintf(root, sizeof(root), "%s", env.db_root);
    int port = env.port;
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(test_env_start_at(&env, root, port), 0, "restart on the same root");
    tc = tc_connect(&(TestClientCfg){ .port = port, .io_timeout_ms = 60000 });
    ASSERT_NOT_NULL(tc, "reconnect");
    if (tc) {
        fr_verify_all_rows(tc, "after restart", 1);
        fr_verify_aggregate(tc, "after restart", 1, FR_N - 200);
        tc_close(tc);
    }
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-finance-routes", test_finance_routes_run)
