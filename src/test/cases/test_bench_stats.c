#include "test_runner.h"
#include "test_assert.h"
#include "bench_stats.h"
#include "bench_common.h"

static int test_bench_median_even_and_odd(void) {
    uint64_t even[] = { 3063420, 4632890 };
    uint64_t odd[] = { 1000, 2000, 3000 };
    BenchHist even_hist, odd_hist, empty_hist;
    bench_hist_init(&even_hist, even, 2);
    bench_hist_init(&odd_hist, odd, 3);
    bench_hist_init(&empty_hist, NULL, 0);
    even_hist.count = 2;
    odd_hist.count = 3;

    ASSERT_EQ_INT((int)bench_hist_p50_ns(&even_hist), 3848155,
                  "even sample median is the midpoint of central values");
    ASSERT_EQ_INT((int)bench_hist_p50_ns(&odd_hist), 2000,
                  "odd sample median is the central value");
    ASSERT_EQ_INT((int)bench_hist_p50_ns(&empty_hist), 0,
                  "empty sample median is zero");
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_bench_object_stats_format(void) {
    char output[160];

    bench_format_object_stats(output, sizeof(output), "AFTER INSERT",
                              "1000000", "1292386304");
    ASSERT_EQ_STR(output,
                  "AFTER INSERT\n"
                  "  live records: 1000000\n"
                  "  disk bytes:   1292386304\n",
                  "object checkpoint labels live records and disk bytes");
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bench-median", test_bench_median_even_and_odd)
TEST_REGISTER("test-bench-object-stats", test_bench_object_stats_format)
