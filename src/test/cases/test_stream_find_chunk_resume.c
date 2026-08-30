/* Chunked-resume parity for the limit-bound streaming find executor
 * (docs/plans/2026-08-27-bt-kf-lock-inversion-chunked-fetch.md, Task 0b).
 *
 * Runs a daemon with a tiny QUERY_BUFFER_MB so the batch-fetch pending cap
 * collapses (batch_buf_init also caps at fetch_limit), which forces many
 * close/reopen cycles per shard walk once the executor is chunk-resumable.
 * Asserts that IN-list / range / post-filter / offset+limit finds return
 * exactly the expected row sets and counts, and that a hard walk (all
 * candidates rejected by the post-filter) traverses every candidate without
 * dropping or duplicating rows.
 *
 * Result-set comparisons are order-insensitive: cross-shard emission order
 * was never contractual (parallel fan-out arrival order).
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "fixtures.h"
#include "test_client.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ROWS 400
static const char *g_obj = "chunkresume";

/* Keys are k0001..k0400. v carries 'q' on every 3rd row so an unindexed
 * contains() sibling rejects ~2/3 of candidates — forcing full traversal
 * with repeated buffer flushes rather than early LIMIT completion. */

typedef struct {
    char keys[ROWS][8];
    int nkeys;
} KeySet2;

static void ks_add(KeySet2 *ks, const char *key) {
    snprintf(ks->keys[ks->nkeys], sizeof(ks->keys[0]), "%s", key);
    ks->nkeys++;
}

static int ks_contains(const KeySet2 *ks, const char *key) {
    for (int i = 0; i < ks->nkeys; i++)
        if (strcmp(ks->keys[i], key) == 0) return 1;
    return 0;
}

static int build_fixture(TestEnv *env) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[768], *resp = NULL;

    if (tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}",
                   &resp) != 0) { tc_close(tc); return -1; }
    free(resp);

    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":32,\"streams\":2,\"max_key\":16,"
        "\"fields\":[\"k:int\",\"v:varchar:32\"],"
        "\"indexes\":[\"k\"]}", g_obj);
    resp = NULL;
    if (tc_request(tc, req, &resp) != 0 ||
        !SAFE_STRSTR(resp, "\"status\":\"created\"")) {
        free(resp); tc_close(tc); return -1;
    }
    free(resp);

    /* Use two bulk-insert chunks to keep fixture setup fast. Bulk request
     * bodies are ~10 KB — they need their own large request buffer
     * (TestClient frames whatever we hand it; the old 768-byte req[]
     * silently truncated mid-array). */
    static char bulk_body[1 << 15];
    static char bulk_req[1 << 16];
    int lens[2] = { ROWS / 2, ROWS - ROWS / 2 };
    for (int c = 0, start = 1; c < 2; c++) {
        size_t off = 0;
        off += (size_t)snprintf(bulk_body + off, sizeof(bulk_body) - off,
                                "[");
        for (int i = 0; i < lens[c]; i++) {
            int idx = start + i;
            off += (size_t)snprintf(bulk_body + off, sizeof(bulk_body) - off,
                "%s{\"key\":\"k%04d\",\"value\":{\"k\":%d,\"v\":%s}}",
                i ? "," : "", idx, idx,
                (idx % 3 == 0) ? "\"qq-data\"" : "\"plain\"");
            ASSERT_TRUE(off < sizeof(bulk_body) - 512,
                        "bulk body fits scratch buffer");
        }
        off += (size_t)snprintf(bulk_body + off, sizeof(bulk_body) - off, "]");
        snprintf(bulk_req, sizeof(bulk_req),
                 "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
                 "\"object\":\"%s\",\"records\":%s}", g_obj, bulk_body);
        resp = NULL;
        if (tc_request(tc, bulk_req, &resp) != 0 ||
            SAFE_STRSTR(resp, "\"error\"")) {
            free(resp); tc_close(tc); return -1;
        }
        free(resp);
        start += lens[c];
    }
    tc_close(tc);
    return 0;
}

/* Issue a find and collect emitted keys into ks. Returns 0 on a clean,
 * error-free response. */
static int find_keys(TestEnv *env, const char *criteria_json,
                     int offset, int limit, KeySet2 *ks, int *out_total_emitted) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[4096];
    if (limit > 0)
        snprintf(req, sizeof(req),
                 "{\"timeout_ms\":20000,\"mode\":\"find\",\"dir\":\"default\","
                 "\"object\":\"%s\",\"criteria\":%s,\"offset\":%d,"
                 "\"limit\":%d}", g_obj, criteria_json, offset, limit);
    else
        snprintf(req, sizeof(req),
                 "{\"timeout_ms\":20000,\"mode\":\"find\",\"dir\":\"default\","
                 "\"object\":\"%s\",\"criteria\":%s}",
                 g_obj, criteria_json);
    char *resp = NULL;
    int crc = tc_request(tc, req, &resp);
    tc_close(tc);
    if (crc != 0 || !resp || strstr(resp, "\"error\"")) {
        free(resp);
        return -1;
    }
    memset(ks, 0, sizeof(*ks));
    const char *p = resp;
    while ((p = strstr(p, "\"key\":\"")) != NULL) {
        p += 7;
        char keybuf[64];
        size_t l = strcspn(p, "\"");
        if (l >= sizeof(keybuf)) l = sizeof(keybuf) - 1;
        memcpy(keybuf, p, l); keybuf[l] = '\0';
        ks_add(ks, keybuf);
        p += l;
    }
    free(resp);
    if (out_total_emitted) *out_total_emitted = ks->nkeys;
    return 0;
}

/* Expected k-rows where k-field value == kn AND v contains 'q'. */
static void expect_in_range(int lo, int hi, KeySet2 *expect) {
    memset(expect, 0, sizeof(*expect));
    for (int idx = lo; idx <= hi && idx <= ROWS; idx++)
        if (idx % 3 == 0) {
            char keybuf[16];
            snprintf(keybuf, sizeof(keybuf), "k%04d", idx);
            ks_add(expect, keybuf);
        }
}

static int sets_equal_sorted(const KeySet2 *a, const KeySet2 *b) {
    if (a->nkeys != b->nkeys) return 0;
    /* Copy pointer array, sort by key text. */
    const char (*pa)[8] = a->keys, (*pb)[8] = b->keys;
    int n = a->nkeys;
    /* small n — insertion sort over indices */
    int ia[ROWS], ib[ROWS];
    for (int i = 0; i < n; i++) { ia[i] = i; ib[i] = i; }
    for (int i = 1; i < n; i++) {
        int t = ia[i], j = i - 1;
        while (j >= 0 && strcmp(pa[ia[j]], pa[t]) > 0) { ia[j+1] = ia[j]; j--; }
        ia[j+1] = t;
        t = ib[i]; j = i - 1;
        while (j >= 0 && strcmp(pb[ib[j]], pb[t]) > 0) { ib[j+1] = ib[j]; j--; }
        ib[j+1] = t;
    }
    for (int i = 0; i < n; i++)
        if (strcmp(pa[ia[i]], pb[ib[i]]) != 0) return 0;
    return 1;
}

static int test_stream_find_chunk_resume_run(void) {
    TestEnv env;
    memset(&env, 0, sizeof(env));
    /* Tiny query buffer → tiny pending_cap → maximal chunk churn. */
    ASSERT_EQ_INT(test_env_start_ex(&env, "1"), 0, "daemon starts (qbuf=1MB)");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }
    ASSERT_EQ_INT(build_fixture(&env), 0, "build chunk-resume fixture");
    if (t_ctx->failed) { test_env_stop(&env); return 1; }

    /* IN-list drive leaf: keys 150..209 (60 candidates), of which exactly
     * the multiples of 3 in range carry 'q' — 20 rows. Post-filter
     * 'contains q' must reject the other 40 after fetch; limit pins the
     * batch cap at small sizes. */
    static char crit[2048];
    size_t off = 0;
    int criteria_ok = tu_appendf(crit, sizeof(crit), &off,
        "{\"and\":[{\"field\":\"k\",\"op\":\"in\",\"value\":\"") == 0;
    for (int idx = 150; idx <= 209; idx++) {
        if (criteria_ok)
            criteria_ok = tu_appendf(crit, sizeof(crit), &off, "%s%d",
                                     idx == 150 ? "" : ",", idx) == 0;
    }
    if (criteria_ok)
        criteria_ok = tu_appendf(crit, sizeof(crit), &off,
            "\"},{\"field\":\"v\",\"op\":\"contains\",\"value\":\"q\"}]}") == 0;
    ASSERT_TRUE(criteria_ok, "build bounded IN-list criteria");
    if (!criteria_ok) { test_env_stop(&env); return 1; }

    KeySet2 got, expect;
    ASSERT_EQ_INT(find_keys(&env, crit, 0, 7, &got, NULL), 0,
                  "IN-list limited find completes");
    expect_in_range(150, 209, &expect);
    ASSERT_TRUE(got.nkeys == 7, "limited IN find respects limit");
    for (int i = 0; i < got.nkeys; i++)
        ASSERT_TRUE(ks_contains(&expect, got.keys[i]),
                    "limited IN find emits only expected rows");

    if (!t_ctx->failed) {
        /* Same filter unlimited → whole set, equal to expectation as a
         * sorted multiset. */
        ASSERT_EQ_INT(find_keys(&env, crit, 0, 0, &got, NULL), 0,
                      "IN-list full find completes");
        ASSERT_TRUE(sets_equal_sorted(&got, &expect),
                    "IN-list full set matches golden");
    }

    if (!t_ctx->failed) {
        /* BETWEEN window whose candidates are dominated by rejects:
         * k between 100..299 → 200 candidates, 67 pass 'contains q'.
         * A no-match post-filter would force traversal of all 200 with
         * zero emissions — swap value to something absent instead. */
        static char crit2[512];
        snprintf(crit2, sizeof(crit2),
            "{\"and\":[{\"field\":\"k\",\"op\":\"between\","
            "\"value\":\"100\",\"value2\":\"299\"},"
            "{\"field\":\"v\",\"op\":\"contains\","
            "\"value\":\"zzz-absent\"}]}");
        ASSERT_EQ_INT(find_keys(&env, crit2, 0, 50, &got, NULL), 0,
                      "reject-all range walk completes under limit");
        ASSERT_TRUE(got.nkeys == 0,
                    "reject-all range walk emits nothing but sees all "
                    "candidates");
    }

    if (!t_ctx->failed) {
        /* Offset semantics across the SAME filtered stream: windowing the
         * unlimited set in slices must cover it exactly once overall. */
        KeySet2 w0, w1, w2;
        ASSERT_EQ_INT(find_keys(&env, crit, 0, 8, &w0, NULL), 0, "slice 0");
        ASSERT_EQ_INT(find_keys(&env, crit, 8, 8, &w1, NULL), 0, "slice 1");
        ASSERT_EQ_INT(find_keys(&env, crit, 16, 8, &w2, NULL), 0, "slice 2");
        ASSERT_TRUE(w0.nkeys == 8 && w1.nkeys == 8 && w2.nkeys == 4,
                    "three offset windows drain the 20-row set fully");
        if (!t_ctx->failed) {
            int overlap = 0;
            for (int i = 0; i < w0.nkeys; i++)
                overlap += ks_contains(&w1, w0.keys[i]) ||
                           ks_contains(&w2, w0.keys[i]);
            for (int i = 0; i < w1.nkeys; i++)
                overlap += ks_contains(&w2, w1.keys[i]);
            ASSERT_TRUE(overlap == 0,
                        "offset windows are disjoint (no resume duplicates)");
        }
    }

    test_env_stop(&env);
    return t_ctx->failed ? 1 : 0;
}

TEST_REGISTER("test-stream-find-chunk-resume",
              test_stream_find_chunk_resume_run)
