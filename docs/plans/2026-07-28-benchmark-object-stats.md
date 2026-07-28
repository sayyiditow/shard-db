# Make benchmark object checkpoints unambiguous

## Goal

At every benchmark checkpoint that currently prints `SIZE after ...`, show
both the live record count and the object’s on-disk byte size, with labels
that make their units explicit.

## Root cause

`size` intentionally returns the byte footprint of all files belonging to an
object. `bench-kv` and `bench-invoice` print its bare numeric response with
the label `SIZE`, while their nearby headers describe record counts. That
makes a correct byte value such as `1292386304` look like an implausible row
count. Neither benchmark issues the O(1) empty-criteria `count` request at
those checkpoints.

## Scope and consumers

- `src/bench/bench_kv.c` has three object-size checkpoints: after JSON
  insert, after CSV insert, and after deleting 10,000 rows.
- `src/bench/bench_invoice.c` has two checkpoints: after the no-index insert
  and after the indexed insert.
- `src/bench/bench_common.h` and `src/bench/bench_common.c` are shared by
  both benchmark cases and will hold the formatting and reporting helper.
- `src/test/cases/test_bench_stats.c` is already linked with the benchmark
  helper sources by `build.sh`; it will cover the output contract.
- No other benchmark prints a `SIZE after ...` checkpoint. Existing disk-use
  summaries in other cases are intentionally left unchanged.

## Output contract

Each affected checkpoint will render exactly this shape, for example:

```text
AFTER NO-INDEX INSERT
  live records: 1000000
  disk bytes:   1292386304
```

`live records` is the database `count` response. `disk bytes` is the database
`size` response; it includes all object data and index files. If either
request fails, only that value is rendered as `(error)`; the benchmark keeps
its existing informational-checkpoint behavior and does not change timing or
workload control flow.

## Task 1 — Test first

In `src/test/cases/test_bench_stats.c`, insert immediately before the exact
anchor `TEST_REGISTER("test-bench-median", test_bench_median_even_and_odd)`:

```c
static int test_bench_object_stats_format(void) {
    char output[160];

    bench_format_object_stats(output, sizeof(output), "AFTER INSERT",
                              "1000000", "1292386304");
    ASSERT_EQ_STR(output,
                  "AFTER INSERT\\n"
                  "  live records: 1000000\\n"
                  "  disk bytes:   1292386304\\n",
                  "object checkpoint labels live records and disk bytes");
    return t_ctx->failed > 0 ? 1 : 0;
}

```

Insert immediately after the exact anchor
`TEST_REGISTER("test-bench-median", test_bench_median_even_and_odd)`:

```c
TEST_REGISTER("test-bench-object-stats", test_bench_object_stats_format)
```

Before Task 2, add `#include "bench_common.h"` after the exact anchor
`#include "bench_stats.h"`, build, and run:

```sh
./build/bin/shard-db-test run test-bench-median
```

It must fail to build before the helper exists. Capture that compiler failure.

Also in `build.sh`, insert immediately after the exact anchor
`src/bench/bench_stats.c \\` and before the exact following anchor
`src/db/slotcask.c \\` in the `shard-db-test` source list:

```sh
    src/bench/bench_common.c \\
```

The test runner must link the source that implements the output formatter;
`shard-db-bench` already links it.

## Task 2 — Add shared checkpoint reporting

In `src/bench/bench_common.h`, insert immediately before the exact anchor
`#endif`:

```c
typedef struct TestClient TestClient;

void bench_format_object_stats(char *out, size_t outlen, const char *label,
                               const char *live_count, const char *disk_bytes);
void bench_print_object_stats(TestClient *tc, const char *dir,
                              const char *object, const char *label);
```

In `src/bench/bench_common.c`, insert immediately after the exact anchor
`#include "bench_common.h"`:

```c
#include "test_client.h"
```

Insert immediately after the complete function anchored by
`void bench_fmt_bytes(long long b, char *out, size_t outlen) {`:

```c
void bench_format_object_stats(char *out, size_t outlen, const char *label,
                               const char *live_count, const char *disk_bytes) {
    snprintf(out, outlen, "%s\\n  live records: %s\\n  disk bytes:   %s\\n",
             label, live_count ? live_count : "(error)",
             disk_bytes ? disk_bytes : "(error)");
}

void bench_print_object_stats(TestClient *tc, const char *dir,
                              const char *object, const char *label) {
    char count_req[256], size_req[256], output[384];
    char *count_resp = NULL, *size_resp = NULL;

    snprintf(count_req, sizeof(count_req),
             "{\\\"mode\\\":\\\"count\\\",\\\"dir\\\":\\\"%s\\\",\\\"object\\\":\\\"%s\\\"}",
             dir, object);
    snprintf(size_req, sizeof(size_req),
             "{\\\"mode\\\":\\\"size\\\",\\\"dir\\\":\\\"%s\\\",\\\"object\\\":\\\"%s\\\"}",
             dir, object);
    if (tc_request(tc, count_req, &count_resp) != 0) count_resp = NULL;
    if (tc_request(tc, size_req, &size_resp) != 0) size_resp = NULL;

    bench_format_object_stats(output, sizeof(output), label, count_resp, size_resp);
    printf("%s\\n", output);
    free(count_resp);
    free(size_resp);
}
```

The helper owns both responses, sends no request that mutates state, and
prints exactly one blank line after the three-line checkpoint.

## Task 3 — Replace every ambiguous checkpoint

In `src/bench/bench_invoice.c`, replace the complete no-index block anchored
by `/* size check (informational) */` through `free(resp); resp = NULL;` with:

```c
    bench_print_object_stats(tc, "default", "bench", "AFTER NO-INDEX INSERT");
```

Replace the complete indexed-size block anchored by
`tc_request(tc, "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"bench\"}", &resp);`
immediately after the indexed bulk insert through its following
`free(resp); resp = NULL;` with:

```c
    bench_print_object_stats(tc, "default", "bench", "AFTER 14-INDEX INSERT");
```

In `src/bench/bench_kv.c`, replace each complete request-and-print block that
prints one of these exact labels, including its `free(resp); resp = NULL;`:

- `SIZE after JSON insert: %s\\n`
- `SIZE after CSV insert:  %s\\n\\n`
- `SIZE after DELETE x10000: %s\\n\\n`

with these corresponding complete calls:

```c
    bench_print_object_stats(tc, "default", "kvbench", "AFTER JSON INSERT");
```

```c
    bench_print_object_stats(tc, "default", "kvbench", "AFTER CSV INSERT");
```

```c
        bench_print_object_stats(tc, "default", "kvbench", "AFTER DELETE x10000");
```

## Verification

1. Prove Task 1 red via the missing-helper compiler failure, then green after
   Task 2 with `./build/bin/shard-db-test run test-bench-median`.
2. Build with `SKIP_TESTS=1 ./build.sh`.
3. Run `./build/bin/shard-db-test run test-bench-median`.
4. Run the full C suite with `./build/bin/shard-db-test run-all --jobs 2`.
5. Audit every `SIZE after` occurrence with `rg`; there must be none remaining.
6. Run `git diff --check`; do not run performance benchmarks and leave the
   changes uncommitted.

## Execution rules

- Execute only after approval and preserve all existing user changes.
- If an exact quoted anchor is missing, write `PLAN_NOTES.md` and halt.
- The database response format and benchmark workloads remain unchanged; this
  is reporting-only code.
