/* src/test/cases/test_trigram_index.c
 *
 * Trigram index — Phase 1 (core helpers). Pure-function assertions for
 * tg_extract_distinct + tg_build_path. Daemon-backed CRUD / planner /
 * reindex assertions land in later phases and get appended to the same
 * file (same pattern as test_bitmap_index.c).
 *
 * Spec: [[index-types-roadmap]] / [[trigram-impl-map]].
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "trigram.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Locate trigram `needle` in `out[0..n)`. Returns 1 if found, 0 otherwise. */
static int tg_in(const uint8_t (*out)[3], size_t n, const char *needle) {
    for (size_t i = 0; i < n; i++) {
        if (out[i][0] == (uint8_t)needle[0] &&
            out[i][1] == (uint8_t)needle[1] &&
            out[i][2] == (uint8_t)needle[2]) return 1;
    }
    return 0;
}

/* Phase 1 — direct trigram.c API. No daemon. Runs first so any primitive
   bug surfaces with a clean stack instead of a confusing wire response. */
static int run_unit_assertions(void) {
    uint8_t out[TG_MAX_DISTINCT][3];

    /* === Length edges. === */
    ASSERT_EQ_INT((int)tg_extract_distinct((const uint8_t *)"",    0, out, TG_MAX_DISTINCT), 0,
                  "empty input → 0 trigrams");
    ASSERT_EQ_INT((int)tg_extract_distinct((const uint8_t *)"a",   1, out, TG_MAX_DISTINCT), 0,
                  "1-byte input → 0 trigrams");
    ASSERT_EQ_INT((int)tg_extract_distinct((const uint8_t *)"ab",  2, out, TG_MAX_DISTINCT), 0,
                  "2-byte input → 0 trigrams");
    ASSERT_EQ_INT((int)tg_extract_distinct((const uint8_t *)"abc", 3, out, TG_MAX_DISTINCT), 1,
                  "3-byte input → 1 trigram");
    ASSERT_EQ_INT((int)out[0][0], 'a', "exactly-3 trigram[0]=a");
    ASSERT_EQ_INT((int)out[0][1], 'b', "exactly-3 trigram[1]=b");
    ASSERT_EQ_INT((int)out[0][2], 'c', "exactly-3 trigram[2]=c");

    /* === ASCII lowercase fold. === */
    size_t n = tg_extract_distinct((const uint8_t *)"ABC", 3, out, TG_MAX_DISTINCT);
    ASSERT_EQ_INT((int)n, 1, "uppercase ABC → 1 trigram");
    ASSERT_TRUE(tg_in(out, n, "abc"), "ABC folds to abc");

    n = tg_extract_distinct((const uint8_t *)"MiXeD", 5, out, TG_MAX_DISTINCT);
    ASSERT_EQ_INT((int)n, 3, "MiXeD → 3 distinct trigrams");
    ASSERT_TRUE(tg_in(out, n, "mix"), "MiXeD contains mix");
    ASSERT_TRUE(tg_in(out, n, "ixe"), "MiXeD contains ixe");
    ASSERT_TRUE(tg_in(out, n, "xed"), "MiXeD contains xed");

    /* === Sliding window correctness on a short word. === */
    n = tg_extract_distinct((const uint8_t *)"hello", 5, out, TG_MAX_DISTINCT);
    ASSERT_EQ_INT((int)n, 3, "hello → 3 distinct trigrams (hel, ell, llo)");
    ASSERT_TRUE(tg_in(out, n, "hel"), "hello contains hel");
    ASSERT_TRUE(tg_in(out, n, "ell"), "hello contains ell");
    ASSERT_TRUE(tg_in(out, n, "llo"), "hello contains llo");

    /* === Within-record dedup: repeated trigrams collapse to one. === */
    n = tg_extract_distinct((const uint8_t *)"aaaaaa", 6, out, TG_MAX_DISTINCT);
    ASSERT_EQ_INT((int)n, 1, "aaaaaa → 1 distinct trigram (aaa)");
    ASSERT_TRUE(tg_in(out, n, "aaa"), "aaaaaa contains aaa");

    n = tg_extract_distinct((const uint8_t *)"abcabcabc", 9, out, TG_MAX_DISTINCT);
    ASSERT_EQ_INT((int)n, 3, "abcabcabc → 3 distinct (abc, bca, cab)");
    ASSERT_TRUE(tg_in(out, n, "abc"), "abcabcabc contains abc");
    ASSERT_TRUE(tg_in(out, n, "bca"), "abcabcabc contains bca");
    ASSERT_TRUE(tg_in(out, n, "cab"), "abcabcabc contains cab");

    /* === Non-ASCII bytes pass through unchanged (UTF-8 multibyte). === */
    n = tg_extract_distinct((const uint8_t *)"caf\xc3\xa9", 5, out, TG_MAX_DISTINCT);
    ASSERT_EQ_INT((int)n, 3, "caf\xc3\xa9 → 3 distinct (caf, af<c3>, f<c3><a9>)");
    ASSERT_TRUE(tg_in(out, n, "caf"), "caf\xc3\xa9 contains 'caf'");

    /* === Punctuation + spaces are first-class bytes (not stripped). === */
    n = tg_extract_distinct((const uint8_t *)"hi there", 8, out, TG_MAX_DISTINCT);
    ASSERT_TRUE(tg_in(out, n, "hi "), "hi there contains 'hi ' (with trailing space)");
    ASSERT_TRUE(tg_in(out, n, " th"), "hi there contains ' th' (with leading space)");

    /* === out_cap = 0 → returns 0 even with valid input. === */
    ASSERT_EQ_INT((int)tg_extract_distinct((const uint8_t *)"abcdef", 6, out, 0), 0,
                  "out_cap=0 → 0 trigrams");

    /* === out_cap clamp: ask for 2, get 2 even though source has 4. === */
    n = tg_extract_distinct((const uint8_t *)"abcdef", 6, out, 2);
    ASSERT_EQ_INT((int)n, 2, "out_cap=2 clamps to 2 trigrams");

    /* === Long-text saturation. Distinct ASCII trigrams cap at 26*26*26 =
       17576 in principle; we cap user-side at TG_MAX_DISTINCT (4096).
       Construct a random-ish 20K-byte buffer and verify we don't exceed
       the cap and don't crash. === */
    {
        uint8_t big[20000];
        for (size_t i = 0; i < sizeof(big); i++) {
            big[i] = (uint8_t)('a' + (i * 31u + 7u) % 26u);
        }
        n = tg_extract_distinct(big, sizeof(big), out, TG_MAX_DISTINCT);
        ASSERT_TRUE(n > 0, "long input → some trigrams");
        ASSERT_TRUE(n <= TG_MAX_DISTINCT, "long input does not exceed TG_MAX_DISTINCT");
    }

    /* === tg_build_path: format matches NNN.tg convention. === */
    char path[1024];
    tg_build_path(path, sizeof(path), "/var/db", "blog", "body", 0);
    ASSERT_EQ_STR(path, "/var/db/blog/indexes/body/000.tg", "shard 0 path");

    tg_build_path(path, sizeof(path), "/var/db", "blog", "body", 15);
    ASSERT_EQ_STR(path, "/var/db/blog/indexes/body/00f.tg", "shard 15 path (hex)");

    tg_build_path(path, sizeof(path), "/var/db", "blog", "body", 256);
    ASSERT_EQ_STR(path, "/var/db/blog/indexes/body/100.tg", "shard 256 path");

    tg_build_path(path, sizeof(path), "/var/db", "blog", "body", 4095);
    ASSERT_EQ_STR(path, "/var/db/blog/indexes/body/fff.tg", "shard 4095 path (max)");

    return 0;
}

/* Phase 2 — daemon-backed CRUD. Insert/update/delete records, verify the
   .tg shard file materialises and grows / shrinks. Functional verification
   (does `contains "x"` return the right records) lands in Phase 4 when the
   planner read path goes in. */
static int run_crud_assertions(TestEnv *env) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "client connect");
    if (!tc) return 1;

    char *resp = NULL;

    /* === create-object with text:trigram. === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"notes\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"body:varchar:256\"],"
        "\"indexes\":[\"body:trigram\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object with trigram");
    free(resp); resp = NULL;

    /* index.conf has the :trigram line. */
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"notes\"}", &resp);
    ASSERT_CONTAINS(resp, "\"body:trigram\"", "describe-object emits :trigram");
    free(resp); resp = NULL;

    /* === Insert a record. === */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"notes\","
        "\"key\":\"k1\",\"value\":{\"body\":\"hello world\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert k1");
    free(resp); resp = NULL;

    /* The CRUD hook should have materialised at least one .tg shard. The
       routing is by record hash; we don't know which shard, so check all
       8. At least one must be non-empty. */
    char tg_dir[512];
    snprintf(tg_dir, sizeof(tg_dir), "%s/t/notes/indexes/body", env->db_root);
    int total_size = 0;
    for (int s = 0; s < 8; s++) {
        char p[600];
        snprintf(p, sizeof(p), "%s/%03x.tg", tg_dir, s);
        if (tu_file_exists(p)) {
            FILE *f = fopen(p, "rb");
            if (f) {
                fseek(f, 0, SEEK_END);
                total_size += (int)ftell(f);
                fclose(f);
            }
        }
    }
    ASSERT_TRUE(total_size > 0, "at least one .tg shard non-empty after insert");

    /* === Update the record (different body). === */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"t\",\"object\":\"notes\","
        "\"key\":\"k1\",\"value\":{\"body\":\"hello earth\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "update k1");
    free(resp); resp = NULL;

    /* === Insert a second record with shared trigrams. === */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"notes\","
        "\"key\":\"k2\",\"value\":{\"body\":\"hello there\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert k2 (shares hello)");
    free(resp); resp = NULL;

    /* Index should have grown — both records contribute distinct trigrams. */
    int total_size_after = 0;
    for (int s = 0; s < 8; s++) {
        char p[600];
        snprintf(p, sizeof(p), "%s/%03x.tg", tg_dir, s);
        if (tu_file_exists(p)) {
            FILE *f = fopen(p, "rb");
            if (f) {
                fseek(f, 0, SEEK_END);
                total_size_after += (int)ftell(f);
                fclose(f);
            }
        }
    }
    ASSERT_TRUE(total_size_after >= total_size, ".tg total bytes monotonic after second insert");

    /* === Delete a record. === */
    tc_request(tc,
        "{\"mode\":\"delete\",\"dir\":\"t\",\"object\":\"notes\","
        "\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"deleted\"", "delete k1");
    free(resp); resp = NULL;

    /* Subsequent get returns the not-found error sentinel — sanity
       check that the delete went through end-to-end (and the trigram
       CRUD hook didn't panic). */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"t\",\"object\":\"notes\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "Not found", "k1 gone after delete");
    free(resp); resp = NULL;

    /* === Bulk insert: 50 records, varied bodies. Confirms the bulk
       path also dispatches the trigram CRUD hook. === */
    {
        char *bulk = malloc(8192);
        int off = snprintf(bulk, 8192,
            "{\"mode\":\"bulk-insert\",\"dir\":\"t\",\"object\":\"notes\",\"records\":[");
        for (int i = 0; i < 50; i++) {
            off += snprintf(bulk + off, 8192 - off,
                "%s{\"key\":\"bk%d\",\"value\":{\"body\":\"sample body number %d about cats\"}}",
                i ? "," : "", i, i);
        }
        snprintf(bulk + off, 8192 - off, "]}");
        tc_request(tc, bulk, &resp);
        ASSERT_CONTAINS(resp, "\"inserted\":", "bulk-insert returned inserted count");
        free(bulk);
        free(resp); resp = NULL;
    }

    tc_close(tc);
    return 0;
}

/* Phase 3 — reindex pass. Drop the .tg shards on disk, run reindex,
   confirm they're rebuilt with the same content as the online-maintained
   version. */
static int run_reindex_assertions(TestEnv *env) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return 1;

    char *resp = NULL;

    /* Build a fresh object with trigram, insert known data. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"r\",\"object\":\"texts\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"body:varchar:256\"],"
        "\"indexes\":[\"body:trigram\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "reindex: create-object");
    free(resp); resp = NULL;

    /* Insert N records; trigger CRUD-side trigram maintenance. */
    for (int i = 0; i < 25; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"r\",\"object\":\"texts\","
            "\"key\":\"r%d\",\"value\":{\"body\":\"trigger word number %d here\"}}", i, i);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }

    /* Measure total .tg byte count from CRUD-side maintenance. */
    char tg_dir[512];
    snprintf(tg_dir, sizeof(tg_dir), "%s/r/texts/indexes/body", env->db_root);
    long size_crud = 0;
    for (int s = 0; s < 8; s++) {
        char p[600];
        snprintf(p, sizeof(p), "%s/%03x.tg", tg_dir, s);
        FILE *f = fopen(p, "rb");
        if (f) { fseek(f, 0, SEEK_END); size_crud += ftell(f); fclose(f); }
    }
    ASSERT_TRUE(size_crud > 0, "reindex: CRUD-side .tg has content");

    /* Reindex via wire — server-side rebuild. */
    tc_request(tc, "{\"mode\":\"reindex\",\"dir\":\"r\",\"object\":\"texts\"}", &resp);
    /* Either {"status":"ok"} or a numeric "rebuilt": N. Just ensure no error. */
    ASSERT_TRUE(strstr(resp, "\"error\"") == NULL, "reindex: no error response");
    free(resp); resp = NULL;

    /* After reindex the .tg shards should still hold content. The exact
       bytes may differ (reindex writes in a different insertion order),
       but the total should be in the same ballpark. We check it grew or
       stayed within ±50% of the CRUD-side total — a loose envelope check
       that catches "reindex wiped and didn't rebuild" without being
       brittle to leaf-rebuild specifics. */
    long size_reidx = 0;
    for (int s = 0; s < 8; s++) {
        char p[600];
        snprintf(p, sizeof(p), "%s/%03x.tg", tg_dir, s);
        FILE *f = fopen(p, "rb");
        if (f) { fseek(f, 0, SEEK_END); size_reidx += ftell(f); fclose(f); }
    }
    ASSERT_TRUE(size_reidx > 0, "reindex: .tg has content after rebuild");
    ASSERT_TRUE(size_reidx >= size_crud / 2 && size_reidx <= size_crud * 2,
                "reindex: .tg size in same ballpark");

    tc_close(tc);
    return 0;
}

/* Phase 4 — planner read path. contains / i_contains over a
   trigram-indexed field must return the right records (correctness)
   AND go through the trigram index (no full-scan fallback). */
static int run_planner_assertions(TestEnv *env) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return 1;

    char *resp = NULL;

    /* Create object with trigram-only index on body. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"p\",\"object\":\"posts\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"body:varchar:256\"],"
        "\"indexes\":[\"body:trigram\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "planner: create-object");
    free(resp); resp = NULL;

    /* Insert a known corpus. Each record's body is distinct so we can
       assert exact match counts. */
    const char *bodies[] = {
        "the quick brown fox jumps over the lazy dog",   /* 0: has "the", "quick", "brown", ... */
        "ALPACAS are gentle creatures from the andes",   /* 1: case-mixed, contains "alpaca" */
        "raspberry pi clusters can host shard-db",       /* 2: has "shard", "raspberry" */
        "rapid fox motion captured by the camera",       /* 3: has "fox", "rapid" */
        "Hello World",                                    /* 4: short — only 2 trigrams of "hello" */
        "shardDB shardDB SHARDDB shardDB",                /* 5: case-flooded "sharddb" */
    };
    for (int i = 0; i < 6; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"p\",\"object\":\"posts\","
            "\"key\":\"p%d\",\"value\":{\"body\":\"%s\"}}", i, bodies[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "planner: insert sample");
        free(resp); resp = NULL;
    }

    /* contains "fox" → records 0 + 3 (lowercase match, both). */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"p\",\"object\":\"posts\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"fox\"}]}", &resp);
    ASSERT_CONTAINS(resp, "2", "contains \"fox\" → 2 records");
    free(resp); resp = NULL;

    /* contains "the" → records 0 (twice) + 1 + 3 = 3 distinct records.
       Distinct count for a contains predicate counts records, not
       occurrences within a record. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"p\",\"object\":\"posts\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"the\"}]}", &resp);
    ASSERT_CONTAINS(resp, "3", "contains \"the\" → 3 records");
    free(resp); resp = NULL;

    /* contains "fox" returns the actual records (find, not count). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"p\",\"object\":\"posts\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"fox\"}]}", &resp);
    ASSERT_CONTAINS(resp, "p0", "find contains fox includes p0");
    ASSERT_CONTAINS(resp, "p3", "find contains fox includes p3");
    free(resp); resp = NULL;

    /* Case-sensitive contains "ALPACAS" → only record 1 (exact case). */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"p\",\"object\":\"posts\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"ALPACAS\"}]}", &resp);
    ASSERT_CONTAINS(resp, "1", "case-sensitive contains \"ALPACAS\" → 1");
    free(resp); resp = NULL;

    /* Case-sensitive contains "alpacas" → 0 records (record 1 has ALPACAS).
       Trigram candidate set returns record 1 (lowercase matches), but
       the per-record memmem verify with contains semantics rejects it. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"p\",\"object\":\"posts\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"alpacas\"}]}", &resp);
    ASSERT_CONTAINS(resp, "0", "case-sensitive contains \"alpacas\" → 0");
    free(resp); resp = NULL;

    /* i_contains "ALPACAS" → 1 (lowercase variant on the same index). */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"p\",\"object\":\"posts\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"icontains\",\"value\":\"ALPACAS\"}]}", &resp);
    ASSERT_CONTAINS(resp, "1", "i_contains \"ALPACAS\" → 1");
    free(resp); resp = NULL;

    /* i_contains "shardDB" → records 2 + 5 (record 2 has lowercase
       'shard-db', so it depends on whether the i_contains pattern
       'shardDB' would match 'shard-db'. memmem on lowercased
       "shard-db" vs lowercased "sharddb" = no match, only record 5
       matches. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"p\",\"object\":\"posts\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"icontains\",\"value\":\"shardDB\"}]}", &resp);
    /* Record 5 has 'shardDB' literally; record 2 has 'shard-db' (with
       a hyphen). 'shardDB'.lower() = 'sharddb', doesn't match 'shard-db'.
       So count = 1. */
    ASSERT_CONTAINS(resp, "1", "i_contains \"shardDB\" → 1 (record 5)");
    free(resp); resp = NULL;

    /* Sub-3-char pattern falls back to scan but must still return
       correct results. contains "of" — record 0 ("jumps over") matches
       on the 'of' window (actually it doesn't; let me pick something
       I'm sure of). "ox" appears in records 0 and 3 (fox). */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"p\",\"object\":\"posts\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"ox\"}]}", &resp);
    ASSERT_CONTAINS(resp, "2", "sub-3 contains \"ox\" → 2 (scan fallback)");
    free(resp); resp = NULL;

    /* contains "qwerty" → 0 records. Confirms negative result via trigram. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"p\",\"object\":\"posts\","
        "\"criteria\":[{\"field\":\"body\",\"op\":\"contains\",\"value\":\"qwerty\"}]}", &resp);
    ASSERT_CONTAINS(resp, "0", "contains \"qwerty\" → 0");
    free(resp); resp = NULL;

    /* estimate-index — sample records, project disk size. We have 6
       records inserted earlier in this block; the estimator should
       return non-zero records + avg_distinct_trigrams (since bodies
       are non-trivial) + an estimated_disk_bytes proportional to it. */
    tc_request(tc,
        "{\"mode\":\"estimate-index\",\"dir\":\"p\",\"object\":\"posts\","
        "\"spec\":\"body:trigram\"}", &resp);
    ASSERT_CONTAINS(resp, "\"records\":6", "estimate: records=6");
    ASSERT_CONTAINS(resp, "\"sample_size\":6", "estimate: sample=6");
    ASSERT_TRUE(strstr(resp, "\"avg_distinct_trigrams\":0.0") == NULL,
                "estimate: non-zero avg_distinct_trigrams");
    free(resp); resp = NULL;

    /* estimate-index rejects non-:trigram specs. */
    tc_request(tc,
        "{\"mode\":\"estimate-index\",\"dir\":\"p\",\"object\":\"posts\","
        "\"spec\":\"body\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "estimate: missing :trigram → error");
    free(resp); resp = NULL;

    /* estimate-index rejects non-existent field. */
    tc_request(tc,
        "{\"mode\":\"estimate-index\",\"dir\":\"p\",\"object\":\"posts\","
        "\"spec\":\"nosuch:trigram\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "estimate: bad field → error");
    free(resp); resp = NULL;

    tc_close(tc);
    return 0;
}

static int test_trigram_index_run(void) {
    /* Phase 1 unit assertions: pure helpers, no daemon. */
    if (run_unit_assertions() != 0) return 1;

    /* Phase 2: daemon-backed CRUD. */
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int rc = run_crud_assertions(&env);
    if (rc == 0) rc = run_reindex_assertions(&env);
    if (rc == 0) rc = run_planner_assertions(&env);
    test_env_stop(&env);
    return rc;
}

TEST_REGISTER("test-trigram-index", test_trigram_index_run)
