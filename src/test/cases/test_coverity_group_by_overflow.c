/* src/test/cases/test_coverity_group_by_overflow.c
 * CID 1696446: cmd_aggregate_tree's inline CSV->JSON group_by conversion
 * wrote its closing quote/bracket unconditionally once the bounds-checked
 * inner copy loop had already saturated the 4096-byte buffer, turning
 * into an unbounded write loop. Now extracted into group_by_csv_to_json()
 * (query_aggregate.c, declared in query_internal.h) — this test calls it
 * directly with a CSV far longer than any single NQL group_by clause
 * (NqlCommand.group_by is capped at 1024 bytes upstream) to prove the
 * function itself is safe regardless of caller. */
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

static int test_coverity_group_by_overflow_run(void) {
    const size_t BUF_SZ = 4096;
    const size_t CANARY_SZ = 256;
    char *region = malloc(BUF_SZ + CANARY_SZ);
    ASSERT_NOT_NULL(region, "alloc region");
    if (!region) return 1;
    memset(region + BUF_SZ, 0xAB, CANARY_SZ);

    /* 3000 single-character field names — far more than the ~500 that
       would fit if this were bounded by the NQL wire cap, guaranteed to
       have driven the old code's unbounded write loop. */
    size_t n_fields = 3000;
    size_t csv_cap = n_fields * 2 + 1;
    char *csv = malloc(csv_cap);
    ASSERT_NOT_NULL(csv, "alloc csv");
    if (!csv) { free(region); return 1; }
    size_t cp = 0;
    for (size_t i = 0; i < n_fields; i++) {
        if (i > 0) csv[cp++] = ',';
        csv[cp++] = 'x';
    }
    csv[cp] = '\0';

    group_by_csv_to_json(csv, region, BUF_SZ);

    int canary_intact = 1;
    for (size_t i = 0; i < CANARY_SZ; i++) {
        if ((unsigned char)region[BUF_SZ + i] != 0xAB) { canary_intact = 0; break; }
    }
    ASSERT_TRUE(canary_intact, "no write past the declared 4096-byte buffer");

    /* Output must still be NUL-terminated, well-formed JSON (starts '[',
       ends ']') even though most fields were necessarily dropped. */
    size_t out_len = strlen(region);
    ASSERT_TRUE(out_len > 0 && out_len < BUF_SZ, "output is NUL-terminated within bounds");
    ASSERT_TRUE(region[0] == '[', "output starts with [");
    ASSERT_TRUE(region[out_len - 1] == ']', "output ends with ]");

    /* Sanity: a normal short CSV still round-trips exactly. */
    char small[64];
    group_by_csv_to_json("a,b,c", small, sizeof(small));
    ASSERT_TRUE(strcmp(small, "[\"a\",\"b\",\"c\"]") == 0, "small CSV converts exactly");

    free(csv);
    free(region);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-coverity-group-by-overflow", test_coverity_group_by_overflow_run)
