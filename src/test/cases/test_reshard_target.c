#include "test_runner.h"
#include "test_assert.h"
#include "types.h"

static int test_reshard_target_run(void) {
    /* Below every band's lower bound: smallest recommended splits. */
    ASSERT_EQ_INT(reshard_target_for_count(0), 8, "0 records -> 8");
    ASSERT_EQ_INT(reshard_target_for_count(999999), 8, "999,999 -> 8 (just under 1M)");

    /* 1M-10M band -> 16. */
    ASSERT_EQ_INT(reshard_target_for_count(1000000), 16, "1,000,000 -> 16 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(9999999), 16, "9,999,999 -> 16 (just under 10M)");

    /* 10M-50M band -> 64. */
    ASSERT_EQ_INT(reshard_target_for_count(10000000), 64, "10,000,000 -> 64 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(49999999), 64, "49,999,999 -> 64 (just under 50M)");

    /* 50M-200M band -> 256. */
    ASSERT_EQ_INT(reshard_target_for_count(50000000), 256, "50,000,000 -> 256 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(199999999), 256, "199,999,999 -> 256 (just under 200M)");

    /* 200M-1B band -> 1024. */
    ASSERT_EQ_INT(reshard_target_for_count(200000000), 1024, "200,000,000 -> 1024 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(999999999), 1024, "999,999,999 -> 1024 (just under 1B)");

    /* 1B-5B band -> 2048 (new). */
    ASSERT_EQ_INT(reshard_target_for_count(1000000000LL), 2048, "1,000,000,000 -> 2048 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(4999999999LL), 2048, "4,999,999,999 -> 2048 (just under 5B)");

    /* 5B-10B band -> 4096. */
    ASSERT_EQ_INT(reshard_target_for_count(5000000000LL), 4096, "5,000,000,000 -> 4096 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(9999999999LL), 4096, "9,999,999,999 -> 4096 (just under 10B)");

    /* 10B+ -> 4096 (no further auto action; ceiling). */
    ASSERT_EQ_INT(reshard_target_for_count(10000000000LL), 4096, "10,000,000,000 -> 4096 (band lower bound)");
    ASSERT_EQ_INT(reshard_target_for_count(50000000000LL), 4096, "50,000,000,000 -> 4096 (well past 10B)");

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-reshard-target", test_reshard_target_run)
