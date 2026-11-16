# Phase 1 amendment — retain the complete CSV update cell

Scope: I-02 in the 2026-10-22 implementation review. The user explicitly
authorized fixing the findings. This amendment is additive to the fifth
revision, and does not change a storage or wire format.

## Root cause and invariant

`bulk_upd_delim_run` preserves raw file spans, including embedded NULs.
`v2_bulk_upd_delim_value_compute` correctly computes `cell_len` but narrows
it to `strlen(cellbuf)` immediately before the checked encoder. This
validates `12<NUL>junk` as 12 and drops varchar bytes after the NUL. Every
nonempty cell must reach conversion with its original length; malformed
scalar rows must be skipped atomically, preserving all old fields and indexes.
An empty cell retains the existing "not touched" behavior.

## Test first

Add the complete new case below and register it once in build.sh after
the unique anchor `    src/test/cases/test_scalar_conversion_safety.c \`.
The build-list insertion is:

```make
    src/test/cases/test_scalar_csv_update.c \
```

The root agent coordinates one build for all review corrections. Before
any production edit, build with `rtk proxy env SKIP_TESTS=1 ./build.sh` and
capture red from `rtk proxy ./build/bin/shard-db-test run test-scalar-csv-update`.
The new case writes an actual binary file, checks per-row counts, old
record/index preservation, valid row progress and varchar full-span storage.

```c
/* A delimited file is a byte span: scalar validation must see the whole
   cell, while varchar must retain bytes following an embedded NUL. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *csv_request(TestClient *tc, const char *json) {
    char *resp = NULL;
    if (tc_request(tc, json, &resp) != 0) return NULL;
    return resp;
}

static void csv_count(TestClient *tc, const char *field, const char *value,
                      long expected, const char *description) {
    char query[512];
    snprintf(query, sizeof(query),
             "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"csv_span\","
             "\"criteria\":[{\"field\":\"%s\",\"op\":\"eq\",\"value\":\"%s\"}]}",
             field, value);
    char *resp = csv_request(tc, query);
    ASSERT_NOT_NULL(resp, "indexed count responds");
    if (resp) {
        char *end = NULL;
        long actual = strtol(resp, &end, 10);
        ASSERT_TRUE(end != resp && (*end == '\0' || *end == '\n'),
                    "indexed count is numeric");
        ASSERT_TRUE(actual == expected, description);
    }
    free(resp);
}

static int test_scalar_csv_update_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = {.port = env.port, .io_timeout_ms = 60000};
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = csv_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"csv_span\","
        "\"splits\":8,\"max_key\":32,\"fields\":[\"i:int\",\"v:varchar:16\"],"
        "\"indexes\":[\"i\",\"v\"]}");
    ASSERT_TRUE(resp && strstr(resp, "\"error\"") == NULL, "create indexed object");
    free(resp);
    resp = csv_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"csv_span\","
        "\"records\":{\"bad\":{\"i\":7,\"v\":\"keep\"},"
        "\"good\":{\"i\":8,\"v\":\"old\"},\"text\":{\"i\":10,\"v\":\"textold\"}}}");
    ASSERT_TRUE(resp && strstr(resp, "\"inserted\":3") != NULL, "seed three rows");
    free(resp);

    char path[512];
    snprintf(path, sizeof(path), "%s/update.csv", env.db_root);
    FILE *file = fopen(path, "wb");
    ASSERT_NOT_NULL(file, "create binary delimited file");
    if (!file) { tc_close(tc); test_env_stop(&env); return 1; }
    static const char data[] =
        "bad|12\0junk|changed\n"
        "good|9|new\n"
        "text||a\0b\n";
    size_t written = fwrite(data, 1, sizeof(data) - 1, file);
    ASSERT_TRUE(written == sizeof(data) - 1, "write complete binary file");
    ASSERT_TRUE(fclose(file) == 0, "close binary file");

    char query[1024];
    snprintf(query, sizeof(query),
        "{\"mode\":\"bulk-update-delimited\",\"dir\":\"default\","
        "\"object\":\"csv_span\",\"file\":\"%s\",\"delimiter\":\"|\"}", path);
    resp = csv_request(tc, query);
    ASSERT_TRUE(resp && strstr(resp, "\"matched\":3") != NULL, "all rows matched");
    ASSERT_TRUE(resp && strstr(resp, "\"updated\":2") != NULL, "only valid rows updated");
    ASSERT_TRUE(resp && strstr(resp, "\"skipped\":1") != NULL, "malformed scalar row skipped");
    free(resp);

    resp = csv_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"csv_span\",\"key\":\"bad\"}");
    ASSERT_TRUE(resp && strstr(resp, "\"i\":7") != NULL, "rejected row keeps old integer");
    ASSERT_TRUE(resp && strstr(resp, "\"v\":\"keep\"") != NULL, "rejected row keeps sibling field");
    free(resp);
    resp = csv_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"csv_span\",\"key\":\"good\"}");
    ASSERT_TRUE(resp && strstr(resp, "\"i\":9") != NULL, "valid scalar update applied");
    ASSERT_TRUE(resp && strstr(resp, "\"v\":\"new\"") != NULL, "valid sibling update applied");
    free(resp);
    resp = csv_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"csv_span\",\"key\":\"text\"}");
    ASSERT_TRUE(resp && strstr(resp, "\"i\":10") != NULL, "empty scalar cell leaves old value");
    ASSERT_TRUE(resp && strstr(resp, "\"v\":\"a\\u0000b\"") != NULL,
                "varchar retains complete cell after embedded NUL");
    free(resp);

    csv_count(tc, "i", "7", 1, "old integer index entry preserved");
    csv_count(tc, "i", "12", 0, "rejected numeric prefix has no index entry");
    csv_count(tc, "v", "keep", 1, "old varchar index entry preserved");
    csv_count(tc, "i", "9", 1, "valid new integer indexed");
    csv_count(tc, "i", "8", 0, "valid old integer index entry removed");

    remove(path);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-scalar-csv-update", test_scalar_csv_update_run)
```

## Production hunk (only after red is captured)

Inside `v2_bulk_upd_delim_value_compute` in src/db/query_bulk.c, the unique
anchor is:

```c
                if (encode_field_checked(&w->ts->fields[tidx], cellbuf,
                                          strlen(cellbuf),
                                          new_buf + w->ts->nullmap_prefix +
                                          w->ts->fields[tidx].offset,
                                          NULL, 0) != 0) {
```

Replace that complete call prefix with:

```c
                if (encode_field_checked(&w->ts->fields[tidx], cellbuf,
                                          cell_len,
                                          new_buf + w->ts->nullmap_prefix +
                                          w->ts->fields[tidx].offset,
                                          NULL, 0) != 0) {
```

There is no signature change or new consumer. The byte span is already
computed by this one caller. Existing return -1 and buffer cleanup preserve
the documented per-row skip path. Do not modify empty-cell handling.

## Verify and handoff

Rebuild once all review fixes are ready, rerun this case, then run the
normal full suite (`rtk proxy ./build/bin/shard-db-test run-all`). No
benchmarks or intermediate sanitizer runs. Work stays uncommitted for
raw-diff and human review. Recommend explicit user-authorized local commits
at reviewed phase boundaries, one fix branch for integration, and push/PR
at the coherent completed milestone after end-of-series ASan+UBSan 3× and
TSan 3× (`--jobs 2`). Certification work remains separate after this lands.

## Implementation status and red evidence

The sole production change is now applied: v2_bulk_upd_delim_value_compute
passes its existing cell_len instead of strlen(cellbuf). No null clearing,
row skip, varchar policy, or delimiter logic was altered. The root's
coordinated pre-fix run produced 22 passed / 8 failed, exit 8:
`/tmp/p1-test-scalar-csv-update-red.log`. The new case checks actual binary
file input, exact old scalar/index preservation, continued valid-row
progress, and raw varchar bytes after NUL. Full green verification is
coordinated by the root after all source changes are frozen.
