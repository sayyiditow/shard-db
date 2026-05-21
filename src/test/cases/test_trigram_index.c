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

static int test_trigram_index_run(void) {
    /* Phase 1 unit assertions: pure helpers, no daemon. */
    if (run_unit_assertions() != 0) return 1;

    /* Phase 2+ daemon-backed assertions land here in later commits. */

    return 0;
}

TEST_REGISTER("test-trigram-index", test_trigram_index_run)
