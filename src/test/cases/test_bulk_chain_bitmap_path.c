/* Red on base: the bulk chain commit flush builds bitmap paths with %03d (decimal) while bm_build_path
   uses %03x (hex), so shard ids >= 10 sync a file that does not exist (ENOENT is treated as success).
   See docs/plans/2026-11-06-bulk-chain-bitmap-sync-path.md. */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "bitmap.h"

#include <stdio.h>
#include <string.h>

static int test_bulk_chain_bitmap_path_run(void) {
    char a[1024], b[1024];
    int mismatches = 0;
    for (int shard = 0; shard < 4096; shard++) {
        req_bitmap_file_path(a, sizeof(a), "/r", "obj", "fld", shard);
        bm_build_path(b, sizeof(b), "/r", "obj", "fld", shard);
        if (strcmp(a, b) != 0) {
            if (mismatches < 3)
                fprintf(stderr, "shard %d: chain=%s canonical=%s\n", shard, a, b);
            mismatches++;
        }
    }
    ASSERT_EQ_INT(mismatches, 0, "chain flush bitmap path equals bm_build_path for every shard id");

    req_bitmap_file_path(a, sizeof(a), "/r", "obj", "fld", 10);
    ASSERT_TRUE(strstr(a, "/00a.bm") != NULL, "shard 10 is spelled 00a.bm");
    req_bitmap_file_path(a, sizeof(a), "/r", "obj", "fld", 4095);
    ASSERT_TRUE(strstr(a, "/fff.bm") != NULL, "shard 4095 is spelled fff.bm");
    return 0;
}

TEST_REGISTER("test-bulk-chain-bitmap-path", test_bulk_chain_bitmap_path_run)
