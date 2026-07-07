/* src/test/cases/test_coverity_encode_criterion_overflow.c
 * CID 1696413: encode_criterion_value's tf==NULL branch did an unbounded
 * memcpy(buf, val, vlen) with no relation to the caller's actual buffer
 * size. Every real call site passes a fixed >=1024-byte stack buffer;
 * a criterion value longer than that overflowed it. This test calls the
 * function directly (linked into shard-db-test alongside the rest of the
 * db sources — see build.sh) with a heap buffer immediately followed by
 * a canary region, so any write past the declared size is caught
 * regardless of stack layout. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "query_internal.h"
#include <stdlib.h>
#include <string.h>

static int test_coverity_encode_criterion_overflow_run(void) {
    const size_t BUF_SZ = 1024;
    const size_t CANARY_SZ = 256;
    uint8_t *region = malloc(BUF_SZ + CANARY_SZ);
    ASSERT_NOT_NULL(region, "alloc region");
    if (!region) return 1;
    memset(region + BUF_SZ, 0xAB, CANARY_SZ);

    /* Value far longer than any real call site's buffer. */
    size_t vlen = 4000;
    char *val = malloc(vlen + 1);
    ASSERT_NOT_NULL(val, "alloc val");
    if (!val) { free(region); return 1; }
    memset(val, 'x', vlen);
    val[vlen] = '\0';

    size_t out_len = 0;
    encode_criterion_value(NULL, val, vlen, region, &out_len);

    ASSERT_TRUE(out_len <= BUF_SZ, "out_len clamped to buffer size");

    int canary_intact = 1;
    for (size_t i = 0; i < CANARY_SZ; i++) {
        if (region[BUF_SZ + i] != 0xAB) { canary_intact = 0; break; }
    }
    ASSERT_TRUE(canary_intact, "no write past the declared 1024-byte buffer");

    /* Sanity: a short value still round-trips exactly (no regression on
       the common case). */
    memset(region, 0, BUF_SZ);
    out_len = 0;
    encode_criterion_value(NULL, "hello", 5, region, &out_len);
    ASSERT_EQ_INT((int)out_len, 5, "short value: out_len exact");
    ASSERT_TRUE(memcmp(region, "hello", 5) == 0, "short value: bytes exact");

    free(val);
    free(region);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-coverity-encode-criterion-overflow", test_coverity_encode_criterion_overflow_run)
