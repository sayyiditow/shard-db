/* src/test/cases/test_index_splits_curve.c
 * Smoke test for the non-linear index_splits_for() curve. Pins the
 * full table so a future "fix" that accidentally drops a tier or
 * inverts the plateau pattern fails loudly.
 *
 * No daemon needed — pure unit test against the inline helper.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include <stdio.h>

static int test_index_splits_curve_run(void) {
    /* The full proposed curve. Don't change this table without
       coordinating with reindex behaviour and the migration path. */
    struct { int splits; int expected_idx; } cases[] = {
        { 8,      2 },
        { 16,     4 },
        { 32,     4 },
        { 64,     8 },
        { 128,   16 },
        { 256,   16 },
        { 512,   32 },
        { 1024,  64 },
        { 2048,  64 },
        { 4096, 128 },
    };

    for (size_t i = 0; i < sizeof(cases)/sizeof(cases[0]); i++) {
        int got = index_splits_for(cases[i].splits);
        char desc[64];
        snprintf(desc, sizeof(desc), "splits=%d → idx=%d",
                 cases[i].splits, cases[i].expected_idx);
        ASSERT_EQ_INT(got, cases[i].expected_idx, desc);
    }

    /* Invariant: every result is a power of 2 (so per-file mod math stays cheap). */
    for (size_t i = 0; i < sizeof(cases)/sizeof(cases[0]); i++) {
        int n = index_splits_for(cases[i].splits);
        char desc[64];
        snprintf(desc, sizeof(desc), "splits=%d idx is power of 2", cases[i].splits);
        ASSERT_TRUE((n & (n - 1)) == 0, desc);
    }

    /* Invariant: idx_splits divides splits (every data shard maps cleanly
       to one idx shard, no leftover shards orphaned). */
    for (size_t i = 0; i < sizeof(cases)/sizeof(cases[0]); i++) {
        int s = cases[i].splits;
        int n = index_splits_for(s);
        char desc[64];
        snprintf(desc, sizeof(desc), "splits=%d idx_splits divides splits", s);
        ASSERT_TRUE(s % n == 0, desc);
    }

    /* Invariant: monotonic — bigger splits never produces fewer idx files. */
    int prev = 0;
    for (size_t i = 0; i < sizeof(cases)/sizeof(cases[0]); i++) {
        int n = index_splits_for(cases[i].splits);
        char desc[64];
        snprintf(desc, sizeof(desc), "splits=%d idx >= prev (%d >= %d)",
                 cases[i].splits, n, prev);
        ASSERT_TRUE(n >= prev, desc);
        prev = n;
    }

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-index-splits-curve", test_index_splits_curve_run)
