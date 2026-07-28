# Report correct benchmark medians for even sample counts

## Goal

Make benchmark `p50` mean the statistical median. For an even number of
samples, it must be the midpoint of the two central sorted values, not the
upper central value.

## Root cause

The two bulk-insert samples are `3063.42ms` and `4632.89ms`. Both
`bench_table.c`, `bench_stats.c`, and `bench_cache_pollution.c` select
`samples[count / 2]`; with two samples that selects index 1, so the displayed
p50 is `4632.89ms`, equal to the maximum. The correct median is
`(3063.42 + 4632.89) / 2 = 3848.155ms`. The row count, throughput, min, max,
and total are correct; only the even-N median calculation is wrong.

## Invariants

- Empty input returns zero, as the current histogram helper does.
- Odd-N median remains the central sorted value.
- Even-N median is overflow-safe: `lower + (upper - lower) / 2`.
- `bench_table` and histogram reporting use one shared implementation, so
  their p50 semantics cannot drift.
- p99 calculation and all timed work are unchanged.

## Consumers

- `src/bench/bench_table.c` prints section summaries such as the reported
  two-query bulk-insert footer.
- `src/bench/bench_stats.c` prints operation latency histograms and supplies
  `bench_hist_p50_ns()` to benchmark cases.
- `src/bench/bench_cache_pollution.c` has a private median implementation
  for its warm/post-pollution measurements.
- `src/bench/bench_stats.h` exports the shared helper.
- The test runner needs `bench_stats.c` and one new unit case linked, via
  `build.sh`.

## Task 1 — Test first

Create `src/test/cases/test_bench_stats.c` with this complete content:

```c
#include "test_runner.h"
#include "test_assert.h"
#include "bench_stats.h"

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

TEST_REGISTER("test-bench-median", test_bench_median_even_and_odd)
```

In `build.sh`, insert immediately after the exact anchor
`src/test/cases/test_durability_sync.c \\`:

```sh
    src/test/cases/test_bench_stats.c \
```

Insert immediately after the exact anchor `src/db/durability.c \\` in the
`shard-db-test` source list:

```sh
    src/bench/bench_stats.c \
```

Replace the complete test-runner include tail anchored by
`-Isrc/db -Isrc/test \\` with:

```sh
    -Isrc/db -Isrc/test -Isrc/bench \
```

Before Task 2, build and run `./build/bin/shard-db-test run test-bench-median`.
It must fail its even-N assertion with the current upper-median value
(`4632890` rather than `3848155`). Paste the behavioral failure output.

## Task 2 — Use one correct median helper

In `src/bench/bench_stats.h`, insert after the exact anchor
`uint64_t bench_now_ns(void);`:

```c
uint64_t bench_median_sorted_ns(const uint64_t *samples_ns, size_t count);
```

In `src/bench/bench_stats.c`, insert immediately before the exact anchor
`uint64_t bench_hist_p50_ns(BenchHist *h) {`:

```c
uint64_t bench_median_sorted_ns(const uint64_t *samples_ns, size_t count) {
    if (!samples_ns || count == 0) return 0;
    size_t upper = count / 2;
    if (count % 2 != 0) return samples_ns[upper];
    uint64_t lower_value = samples_ns[upper - 1];
    uint64_t upper_value = samples_ns[upper];
    return lower_value + (upper_value - lower_value) / 2;
}
```

In `bench_hist_p50_ns`, replace the complete return statement anchored by
`return h->samples_ns[h->count / 2];` with:

```c
    return bench_median_sorted_ns(h->samples_ns, h->count);
```

In `bench_hist_report`, replace the complete p50 initialization anchored by
`double p50_us = (double)h->samples_ns[h->count / 2] / 1000.0;` with:

```c
    double p50_us = (double)bench_median_sorted_ns(h->samples_ns, h->count) / 1000.0;
```

In `src/bench/bench_table.c`, replace the complete sorted-value declaration
and p50 assignment anchored by
`long mi = arr[0], ma = arr[g_n - 1];` through
`long p50 = arr[g_n / 2];` with:

```c
    uint64_t mi = (uint64_t)arr[0];
    uint64_t ma = (uint64_t)arr[g_n - 1];
    uint64_t p50 = bench_median_sorted_ns((const uint64_t *)arr, (size_t)g_n);
```

Before that replacement, change the sorted temporary array declaration and
copy loop anchor
`long arr[MAX_ROWS] = {0};` through `arr[i] = g_rows[i].us;` to:

```c
    uint64_t arr[MAX_ROWS] = {0};
    for (int i = 0; i < g_n; i++) arr[i] = (uint64_t)g_rows[i].us;
```

Also replace the complete insertion-sort local declaration anchored by
`long v = arr[i]; int j = i - 1;` with:

```c
        uint64_t v = arr[i]; int j = i - 1;
```

Keep the existing `total` accumulation and output format, casting its
operands to `double` as it already does.

In `src/bench/bench_cache_pollution.c`, insert immediately after the exact
anchor `#include "fixtures.h"`:

```c
#include "bench_stats.h"
```

In `run_n_median`, replace the complete return statement anchored by
`return samples[n / 2];` with:

```c
    return bench_median_sorted_ns(samples, (size_t)n);
```

## Verification

1. Run the Task 1 regression test red before the median implementation, then
   green after Task 2.
2. Build with `SKIP_TESTS=1 ./build.sh`.
3. Run `./build/bin/shard-db-test run test-bench-median`.
4. Run the full C suite with `./build/bin/shard-db-test run-all`.
5. Inspect `git diff --check` and leave all changes uncommitted.

## Execution rules

- Execute only after approval; preserve all existing user changes.
- If a quoted anchor is missing, write `PLAN_NOTES.md` and halt rather than
  improvising.
- Do not run performance benchmarks; the user owns benchmark execution.
