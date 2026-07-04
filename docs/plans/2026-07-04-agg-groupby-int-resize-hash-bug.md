# Fix: aggregate group_by hash-table resize uses wrong hash function for integer-keyed buckets

## Execution rules (read first)

- Branch off `main`: `git checkout -b fix/agg-groupby-int-resize-hash`.
- Do tasks in order — Task 1 (write the failing test) must be built and run
  **before** Task 2 (the fix), and its failure output pasted verbatim, so the
  bug is proven to reproduce before it's fixed.
- Build with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run <name>` for the single new case,
  and `./build/bin/shard-db-test run-all` for the full suite.
- Every insertion/edit below is anchored on **quoted exact text** from the
  current file, not line numbers — another branch may be in flight
  concurrently and line numbers drift. If a quoted anchor is not found
  exactly as written, STOP. Do not guess or reinterpret. Write
  `PLAN_NOTES.md` describing the mismatch and stop.
- Never claim a step passed without pasting the real command output.
- Leave all work **uncommitted** on the branch when done — per this repo's
  CLAUDE.md, git operations (commit/push/PR) happen outside Claude.

## Background

Production incident: shard-db-hn-explorer's `backfill-descendants.ts` ran an
`aggregate` + `group_by:["story_root"]` query over the `comments` object and
got back **three separate rows** for the same `story_root` (`16582136`):
counts 189, 133, and 145 — which sum to 467, the true (and separately
verified, via plain `count`) total. The aggregate engine silently split one
group's real count across multiple duplicate output buckets instead of
either merging them or erroring.

## Root cause

`src/db/query.c` has two hash functions for aggregate group buckets:

- `agg_hash(const char *s)` — string djb2 hash, used for the general
  (string-key) group_by path.
- `agg_hash_int(const void *key, size_t len)` — Golden-Ratio multiplier hash
  over raw bytes, used for the **integer fast path** (`ctx->use_int_keys`),
  active when every `group_by` field is a fixed-width integer-class type
  (`FT_INT`, `FT_LONG`, `FT_SHORT`, `FT_BYTE`, `FT_NUMERIC`, `FT_DATE` — see
  `typed_field_int_width()`) and the combined raw-key width fits
  `AGG_INT_KEY_CAP` (32 bytes). This is **not** limited to plain "int" —
  `story_root` in production is exactly this kind of integer field.

`agg_find_or_create()` (the lookup/insert path) correctly branches on
`ctx->use_int_keys`: when true, it hashes with `agg_hash_int()` on the
bucket's raw byte key; otherwise it hashes the string `group_key` with
`agg_hash()`.

`agg_ht_resize()` — invoked once a single `AggCtx`'s distinct group-key count
crosses the initial capacity `AGG_HT_INIT` (256 slots) and load factor
exceeds 1.0, doubling capacity up to `AGG_HT_MAX` (1<<24) — does **not**
branch the same way. It unconditionally rehashes every bucket with
`agg_hash(b->group_key)`, even when the bucket is int-keyed. For an int-keyed
bucket, `b->group_key` and `b->raw_key` encode the same logical value
differently, so `agg_hash(b->group_key)` and `agg_hash_int(b->raw_key, ...)`
land in different slots after the resize.

Net effect: after a resize, a bucket that already exists for a given integer
group key is rehashed to slot X, but a subsequent lookup for the same key
computes `agg_hash_int(...)` and probes slot Y ≠ X. The lookup finds nothing,
so `agg_find_or_create` allocates a **new, duplicate** bucket for the same
logical group — starting its count over from zero — instead of accumulating
into the existing one. Every subsequent resize can strand another generation
of duplicates for keys that were already split.

This is silent: no error, no warning — just wrong aggregate output. It
reproduces the production symptom exactly (>256 distinct integer group keys
→ crosses the 256 and 512 thresholds → the affected key's count is split
across up to 3 rows depending on exactly when each of its records was
folded in relative to each resize).

### Scope confirmation (is this "only integers"?)

Not "integer" in the colloquial sense — the bug is scoped to shard-db's
`use_int_keys` fast path for `group_by`, which activates whenever **every**
group_by field is one of `FT_INT`, `FT_LONG`, `FT_SHORT`, `FT_BYTE`,
`FT_NUMERIC`, or `FT_DATE`, and the combined encoded width is ≤ 32 bytes
(single fields or narrow composites). `FT_NUMERIC` (fixed-point decimal) and
`FT_DATE` are included even though they aren't "int" at the type-declaration
level, because internally they're stored and grouped as raw fixed-width
integer bytes.

String-keyed group_by (`varchar`, `double`, `datetime`, `datetimems`,
`time`, `uuid`, `ipv4`, `ipv6`) is **unaffected** — both its lookup
(`agg_find_or_create`, `use_int_keys` false branch) and its resize path use
`agg_hash()` consistently, so there is no hash mismatch there.

### Affected call sites (all fixed by the one change below)

All of these route through the same `agg_ht_resize` / `agg_find_or_create`
pair, so fixing `agg_ht_resize` fixes all of them simultaneously:

- `agg_scan_cb` — full-scan (`PRIMARY_NONE`) path.
- `igb_pass1_worker` — indexed Pass-1 workers.
- `part_merge_worker` — partition merge.
- `agg_ctx_merge` — final cross-worker merge.

## Task 1 — Add a failing regression test proving the bug

Create a new file `src/test/cases/test_agg_int_groupby_resize.c` with this
exact content:

```c
/* src/test/cases/test_agg_int_groupby_resize.c
 *
 * Regression test for the agg_ht_resize integer-key rehash bug: after the
 * aggregate hash table grows past AGG_HT_INIT (256 slots), agg_ht_resize
 * rehashed every bucket with agg_hash() (the string/djb2 hash) regardless
 * of ctx->use_int_keys, while agg_find_or_create's lookup for integer
 * group fields hashes with agg_hash_int(). After a resize, an int-keyed
 * bucket lands under the wrong slot for future lookups, so a later
 * record for the same group key is silently treated as "not found" and
 * gets a brand-new duplicate bucket instead of accumulating into the
 * existing one — splitting one group's true count across multiple rows.
 *
 * Seeds 600 distinct int group keys (well past the 256-slot initial
 * capacity, forcing two resizes: 256->512->1024, mirroring the
 * production repro that produced 3 duplicate rows for one key) with
 * exactly 3 records apiece on a full-scan (PRIMARY_NONE, unindexed)
 * group_by field, then asserts the aggregate response contains exactly
 * 600 distinct group rows — no more. Any duplicate bucket for a group
 * key pushes the row count above 600.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define N_GROUPS 600
#define PER_GROUP 3
#define BUF_SIZE (N_GROUPS * PER_GROUP * 64 + 4096)

static int test_agg_int_groupby_resize_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gb_resize\","
        "\"fields\":[\"grp:int\"],"
        "\"indexes\":[],\"splits\":8}", &resp);
    free(resp); resp = NULL;

    /* 600 distinct groups (grp = 1..600), 3 records each = 1800 records.
       Values start at 1 (not 0) to sidestep the unrelated CSV zero-cell
       rendering quirk documented in test_agg_int_groupby_multi.c. */
    char *bulk_insert = malloc(BUF_SIZE);
    ASSERT_NOT_NULL(bulk_insert, "malloc bulk_insert buffer");
    int offset = 0;
    offset += snprintf(bulk_insert + offset, BUF_SIZE - offset,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"gb_resize\","
        "\"records\":[");
    int first = 1;
    for (int g = 1; g <= N_GROUPS; g++) {
        for (int r = 0; r < PER_GROUP; r++) {
            int rem = BUF_SIZE - offset;
            if (!first && rem > 0) offset += snprintf(bulk_insert + offset, (size_t)rem, ",");
            first = 0;
            rem = BUF_SIZE - offset;
            if (rem > 0) offset += snprintf(bulk_insert + offset, (size_t)rem,
                "{\"key\":\"k%d_%d\",\"value\":{\"grp\":%d}}", g, r, g);
        }
    }
    { int rem = BUF_SIZE - offset; if (rem > 0) offset += snprintf(bulk_insert + offset, (size_t)rem, "]}"); }

    tc_request(tc, bulk_insert, &resp);
    ASSERT_CONTAINS(resp, "\"inserted\":1800", "bulk-insert: 1800 inserted");
    free(resp); resp = NULL;
    free(bulk_insert);

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"default\",\"object\":\"gb_resize\","
        "\"group_by\":[\"grp\"],"
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"limit\":2000}", &resp);
    ASSERT_TRUE(resp != NULL && strstr(resp, "\"error\"") == NULL,
        "aggregate did not error");

    int n_buckets = 0;
    for (const char *p = resp; (p = strchr(p, '{')) != NULL; p++) n_buckets++;
    ASSERT_EQ_INT(n_buckets, N_GROUPS,
        "aggregate returns exactly 600 distinct groups (no duplicate buckets "
        "from the int-key hash table resize bug)");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-agg-int-groupby-resize", test_agg_int_groupby_resize_run)
```

Then register it in `build.sh`. Find this exact anchor line:

```
    src/test/cases/test_agg_int_groupby_multi.c \
```

and add the new file directly after it, so the block reads:

```
    src/test/cases/test_agg_int_groupby_multi.c \
    src/test/cases/test_agg_int_groupby_resize.c \
```

## Task 2 — Confirm the test reproduces the bug (RED)

Build and run only the new case:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-agg-int-groupby-resize
```

Paste the real output. It must show the `n_buckets == 600` assertion
**failing** (actual bucket count > 600, since duplicate buckets are created
for at least one group key once the hash table resizes). Do not proceed to
Task 3 until this failure is observed and pasted — if the test unexpectedly
passes at this point, STOP and write `PLAN_NOTES.md` describing what
happened instead of proceeding.

## Task 3 — Fix `agg_ht_resize`

In `src/db/query.c`, find this exact anchor (the current, buggy function):

```c
static int agg_ht_resize(AggCtx *ctx) {
    if (ctx->ht_cap >= AGG_HT_MAX) return 0;  /* hard cap reached */
    size_t new_cap = ctx->ht_cap * 2;
    AggBucket **new_ht = calloc(new_cap, sizeof(AggBucket *));
    if (!new_ht) return -1;
    size_t new_mask = new_cap - 1;
    for (size_t i = 0; i < ctx->ht_cap; i++) {
        AggBucket *b = ctx->ht[i];
        while (b) {
            AggBucket *next = b->next;
            uint32_t h = agg_hash(b->group_key) & (uint32_t)new_mask;
            b->next = new_ht[h];
            new_ht[h] = b;
            b = next;
        }
    }
    free(ctx->ht);
    ctx->ht = new_ht;
    ctx->ht_cap = new_cap;
    ctx->ht_mask = new_mask;
    return 0;
}
```

Replace it with:

```c
static int agg_ht_resize(AggCtx *ctx) {
    if (ctx->ht_cap >= AGG_HT_MAX) return 0;  /* hard cap reached */
    size_t new_cap = ctx->ht_cap * 2;
    AggBucket **new_ht = calloc(new_cap, sizeof(AggBucket *));
    if (!new_ht) return -1;
    size_t new_mask = new_cap - 1;
    for (size_t i = 0; i < ctx->ht_cap; i++) {
        AggBucket *b = ctx->ht[i];
        while (b) {
            AggBucket *next = b->next;
            uint32_t h = (ctx->use_int_keys && b->raw_key_len > 0)
                ? agg_hash_int(b->raw_key, (size_t)b->raw_key_len) & (uint32_t)new_mask
                : agg_hash(b->group_key) & (uint32_t)new_mask;
            b->next = new_ht[h];
            new_ht[h] = b;
            b = next;
        }
    }
    free(ctx->ht);
    ctx->ht = new_ht;
    ctx->ht_cap = new_cap;
    ctx->ht_mask = new_mask;
    return 0;
}
```

This mirrors the exact same condition already used by `agg_find_or_create`'s
lookup branch (`ctx->use_int_keys && raw_key_len > 0`), so a bucket now hashes
to the same slot on resize as it will on the next lookup, regardless of which
of the two hash functions was used to place it originally.

## Task 4 — Confirm the fix (GREEN)

Rebuild and rerun the single case:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-agg-int-groupby-resize
```

Paste the real output — it must now show all assertions passing, including
`n_buckets == 600`.

## Task 5 — Full regression suite

```bash
./build/bin/shard-db-test run-all
```

Paste the real final summary line (`# total: N passed, 0 failed`). Do not
claim this step passed without pasting that exact line. If anything other
than the new test's pass count changed from the pre-fix baseline, stop and
investigate before considering the plan complete.

## Notes for the reviewer

- This fix does not touch `agg_find_or_create`, `agg_hash`, `agg_hash_int`,
  or any of the `use_int_keys` gating logic in `cmd_aggregate_do` — those
  were already correct. The only defect was the resize path's hash
  selection.
- No schema/wire-format changes. No new config knobs. Existing aggregates
  under 256 distinct integer group keys were never affected (no resize ever
  occurs), which is why this escaped the existing `test_agg_int_groupby_multi`
  coverage (only 3 groups).
- Once this lands, any shard-db-hn-explorer data written by the buggy
  `backfill-descendants.ts` run (or any `fetchLiveCommentCounts` call that
  crossed 256+ distinct `story_root` values) may still hold corrupted
  `descendants` values and will need a re-run of the backfill script after
  the fix is deployed — that is a separate, follow-up action in the
  hn-explorer repo, not part of this plan.
```
