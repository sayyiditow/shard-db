# Plan: multi-field `order_by` (CSV / JSON array) with composite-index cursor pagination

Date: 2026-09-18
Status: draft for review — not approved or executable until the human
approves.
Depends on: nothing. Independent of `2026-08-03-insert-if-no-match.md` and
`2026-09-14-auto-widen-kf-ceiling.md` (no semantic overlap), but all three
touch `src/db/query.c` — execute strictly one plan at a time, each branching
off a fresh `main`.

## Goal

Let `find` sort by multiple fields — `"order_by": "tenant,day"` (CSV string)
or `"order_by": ["tenant", "day"]` (JSON array), CLI `--order-by tenant,day:desc`
— with one shared sort direction, and make keyset cursor pagination work on
the same multi-field sort key. Non-cursor multi-field `order_by` uses the
existing buffer-sort path with an N-key comparator; cursor mode walks the
matching composite btree index `f1+f2+…+fN` and pages by the tuple
`(f1, …, fN, hash16(key))` with O(limit) cost regardless of page depth.

Single-field behavior is byte-for-byte unchanged (same planner paths, same
cursor token shape `{"f":"v","key":"k"}`).

## Background — current behavior

`order_by` is a single string everywhere: `char order_by[256]` (src/db/nql.h),
`json_obj_strdup(&req, "order_by")` on the wire (find path in server.c),
one shared `order`/`order_dir` direction. Every consumer treats it as one
field name, including the index-existence checks
(`btree_idx_exists(db_root, object, order_by, …)`), so `"a,b"` today matches
no index and falls to the buffer-sort path, where it fails field resolution.

The cursor design note explicitly locked single-field pagination
(src/db/query.c, find-cursor section header):

> ```
>    Shape constraints (locked 2026-04-24 in cursor_design.md):
>    - transparent JSON, not opaque blob
>    - indexed order_by is hard-required; reject if not
>    - single-field order_by only; multi-field composite indexes are for
>      filter acceleration, not pagination
> ```

This plan reverses the third constraint deliberately. The composite
machinery it needs already exists and is exercised in production shapes:

- Composite index keys are per-part typed index encodings concatenated:
  `build_index_key_from_record_into` (src/db/index.c) walks the `+`-split
  spec and concatenates `typed_field_to_index_key` outputs. On-disk leaf
  layout is `enc(f1) ‖ … ‖ enc(fN) ‖ hash16`.
- `btree_idx_walk_ordered` is field-agnostic: byte-range bounds, `desc`
  mirror-flip, k-way merge across idx shards, resume via
  `BtOrderedWalkHandle`. The D1 composite-prefix executor already walks a
  composite (`seed+order_by`) index through it with multi-part encoded
  bounds, so composite walks + resume are proven paths.
- The cursor callback (`cursor_find_cb`) compares `(value-bytes, hash16)`
  with a length-aware memcmp and mirror-flips for DESC — that exact
  comparison generalizes to concatenated multi-part values with zero
  comparator changes (see the invariant below).
- Cursor capture decodes the sort value from the fetched *record*
  (`typed_get_field_str`), not from index bytes — so N-part capture is N
  decode calls with no index-key splitting logic.
- `json_obj_string_or_array` (src/db/util.c) already flattens a JSON string
  or array-of-strings into a CSV string — the `fields` parameter uses it.

## Decisions (surfaced to the human 2026-09-18; confirm or override at review)

**D1 — protocol shape.** `order_by` accepts a CSV string or a JSON array of
strings (server), CSV in the CLI. Each part may carry its own direction
suffix — `"tenant:asc,day:desc"` / `["tenant:asc","day:desc"]` — defaulting
to the shared `order`/`--order-by f:dir` when omitted. Direction handling
mirrors how engines treat an unindexable sort (human decision 2026-09-18):
the composite btree has a single byte order (forward = ASC, reverse = DESC),
so it serves a query only when **all parts agree** (uniform direction).
Uniform + cursor → composite walk. Mixed directions → the buffer-sort
fallback, exactly as if no matching index existed (correct, O(n log n)).
Mixed + cursor is rejected with a clear error: a keyset cursor cannot be
honored without the walk and must not be silently dropped.

**D2 — cursor requirements.** Multi-field cursor mode (n ≥ 2) requires:
- the exact composite btree index `f1+f2+…+fN` to exist (same hard-indexed
  rule as single-field cursor);
- every non-final part to be a fixed-width typed field — `varchar` is only
  allowed as the final part (rationale: the invariant below);
- at most `MAX_ORDER_FIELDS = 4` parts.
Violations are rejected with errors that name the fix (the composite index
to add). Without a cursor field in the request, multi-field `order_by`
still works via buffer-sort (D3) — no index required.

**D3 — non-cursor path.** Multi-field `order_by` without `cursor` uses the
existing buffered fallback (`FP_ORDER_SORT` → `OrderedCollectCtx` → qsort)
with an N-key, per-part-direction comparator — mixed direction patterns
included, exactly as if no matching index existed. The planner needs no new
branch: its index-walk
options (D1 composite executor, D3 ordered walk, composite suggestion hint)
all gate on `btree_idx_exists(…, order_by, …)` or exact-name comparisons
against the single order string, none of which a CSV `"a,b"` can match, so
they are naturally skipped. `MAX_CRITERIA_DEPTH`-style caps are not
implicated.

**D4 — unchanged / out of scope (future work).** Aggregate `order_by`
(group-by sort) stays single-field. `order_walk_bounds` folding of
order-by-range criteria leaves into the walk bounds stays single-field (a
multi-field walk uses full-range bounds; correctness unaffected). Mixed
per-field directions, and a length-prefixed varlen composite encoding, are
future work (the latter is an index format change requiring reindex).

## Correctness invariant (why fixed-width non-final parts suffice)

With parts `f1…fN` where `f1…f(N-1)` have fixed-width encodings, the
byte-string comparison of `enc(f1)‖…‖enc(fN)` equals lexicographic tuple
comparison of `(f1,…,fN)`: a shorter fixed-width prefix can never bleed into
the next part's bytes because its length is constant, so any difference at
or before the end of part i decides the comparison exactly as the tuple
order would. The final part may be `varchar` (raw content bytes) because
nothing follows it except the fixed-width 16-byte hash16 tiebreak, which
only orders entries whose full value bytes are equal.

With a `varchar` part in a non-final position this equivalence breaks
(e.g. `("a", 2^63-1)` vs `("ab", 0)`: tuple order says the first is smaller,
but the raw-concat bytes can order it after), so pagination could skip or
repeat rows — hence the D2 restriction. The existing single-field cursor is
the degenerate n=1 case (value + fixed-width hash16 tail) and is already
correct under this rule.

## Call-site inventory (everything this plan touches or must not break)

Changed:
- The field-validation block in `cmd_find_do` — split-first, then
  `validate_field` per part, so split-level errors surface before any
  dispatch; followed by the one-time `OrderSpec` resolve and the
  single-field suffix normalization (Task 2b).
- The D2 fetch-sort branch — skips multi-field input (CSV contains a
  comma) and takes its direction from `ospec.dir[0]` (Task 2b).
- `parse_cursor_object` — static in query.c, exactly one caller (the find
  cursor branch). Signature keeps `(cursor_json, order_by_csv, out, err)`;
  body reworked to split the CSV.
- `FindCursor` — static struct, query.c only; used only by
  `parse_cursor_object` and the unescape/encode blocks in the cursor branch.
- `CursorFindCtx` — query.c only; gains `ospec` + `last_value[]`. Context
  setup sites: C1-shortcut block, ordered-walk cursor block (both in the
  cursor branch, `ospec` in scope), and the non-cursor ordered-walk emit
  path (deliberately left with `ospec = NULL` from its `memset` — that path
  never emits a cursor token, `offset_mode = 1`).
- Cursor token emitters — exactly two, both shown in Task 4g.
- `free(cc.last_value_str);` — five occurrences (list in Task 4h).
- `OrderedRow`, `OrderedCollectCtx`, `ordered_collect_cb`, `cmp_row_asc`,
  `cmp_row_desc` (deleted — per-part directions replace the outer flip),
  and the buffered-fallback setup/free blocks — query.c only.
- `nql.c` `--order-by` block — refined shared-vs-per-field colon split
  (Task 3).
- server.c find dispatch — `order_by` read switches to
  `json_obj_string_or_array` (precedent: `fields` at the same site).
- Docs: find.md (parameter row + cursor section), AGENTS.md (cursor knob
  bullet).

Read but **not** changed (verified safe for multi-field input):
- Planner: D1 executor / D3 ordered walk / composite-suggestion hint all
  gate on exact `order_by` name matching an index or seed field; a CSV
  value cannot match, so multi-field naturally routes to the buffer-sort
  fallback (D3 decision).
- Joins: `order_by` is already dropped under joins
  (`(order_by && order_by[0] && !has_joins) ? order_by : NULL` feeding
  `has_order`), and cursor+join is hard-rejected earlier. No new guard
  needed.
- `cmd_explain` — receives the CSV string; plan output shows the
  buffer-sort fallback. No change.
- Aggregate `order_by` (`server.c` aggregate dispatch) — untouched.
- `btree_idx_walk_ordered`, `BtOrderedWalkHandle`, `order_walk_bounds`,
  `encode_field_for_index`, `typed_get_field_str`,
  `typed_field_to_index_key`, `json_obj_string_or_array` — consumers only.
- Sanitizer/lock surface: no new locks, threads, or shared mutable state;
  the walk reuses `btree_idx_walk_ordered`'s existing locking discipline.
  The standing ASan/UBSan + TSan gate still applies (Task 7) because the
  touched paths run under the bt/kf caches.

## Wire / CLI surface (after)

```json
// page 1 — CSV or array, both fine
{"mode":"find","dir":"d","object":"o","criteria":[],
 "order_by":["tenant","day"],"order":"asc","limit":100,"cursor":null}
{"mode":"find","dir":"d","object":"o","criteria":[],
 "order_by":"tenant,day","order":"asc","limit":100,"cursor":null}

// response token — one key per part, then key (same shape, more parts)
{"rows":[…],"cursor":{"tenant":"7","day":"2026-09-01","key":"k42"}}

// next page — token verbatim
{"mode":"find", …, "cursor":{"tenant":"7","day":"2026-09-01","key":"k42"}}
```

CLI: `--order-by tenant,day:desc` (shared direction) and
`--order-by tenant:asc,day:desc` (per-field suffixes) — Task 3 refines the
`strrchr(':')` split so a trailing `:asc/:desc` is treated as the shared
direction only when the field part is colon-free.

New errors (all `{"error":…}`):

| Condition | Error |
|---|---|
| > 4 parts | `order_by supports at most 4 fields` |
| empty part (`"a,,b"`) | `order_by contains an empty field name` |
| invalid suffix (`"a:sideways"`) | `invalid per-field order direction; use asc or desc` |
| mixed directions + `cursor` | `cursor pagination requires one shared order direction across all order_by fields` |
| multi, non-final part is varchar | `multi-field order_by requires all but the last field to be fixed-width (non-varchar)` |
| multi, composite index missing | `multi-field order_by requires a composite index` + `"index":"f1+f2"` |
| multi, part not in typed schema | `multi-field order_by field is not in the typed schema` |
| cursor JSON missing a part value | `cursor missing order_by field value` (existing message) |

## Tasks

Execute in order. Build with `SKIP_TESTS=1 ./build.sh`; run the suite with
`./build/bin/shard-db-test run-all`; single case with
`./build/bin/shard-db-test run <name>`. If any quoted anchor is not found
character-for-character, STOP and write `docs/plans/PLAN_NOTES.md` describing
what was found instead — do not guess. Leave the final work **uncommitted**
for the review pass.

---

### Task 1 — red integration test `test-multi-order-cursor`

Test-first: write the full case below, build, run it, and confirm it fails
for the expected reasons (listed after the code) before any implementation.

Create `src/test/cases/test_multi_order_cursor.c`:

```c
/* src/test/cases/test_multi_order_cursor.c
 * Multi-field order_by: buffer-sort path (no cursor), composite-index
 * keyset cursor pagination, and the rejection surface.
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

/* 12 rows; (tenant, day) chosen so tie groups straddle a limit-5 page
 * boundary. Expected (tenant, day) per key, sorted ascending by
 * (tenant, day, key-not-assumed): tie order inside a group is hash16-based
 * and must not be assumed. */
static const char *KEYS[12] = {
    "k01", "k02", "k03", "k04", "k05", "k06",
    "k07", "k08", "k09", "k10", "k11", "k12"
};
static const int TENANT[12] = { 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3 };
static const char *DAY[12] = {
    "2026-09-01", "2026-09-01", "2026-09-02", "2026-09-02",
    "2026-09-01", "2026-09-01", "2026-09-02", "2026-09-02",
    "2026-09-01", "2026-09-01", "2026-09-02", "2026-09-02"
};

/* Pull the ordered key sequence out of a find response. Wrapped responses
 * (cursor mode) are scanned between `"rows":` and `,"cursor":` so the
 * trailing token's "key" is not counted as a row; bare-array responses
 * (buffer-sort mode) are scanned in full. */
static int parse_row_keys(const char *resp, char out[][32], int cap) {
    if (!resp) return 0;
    const char *rows = strstr(resp, "\"rows\":");
    const char *p = rows ? rows + 7 : resp;
    const char *end = NULL;
    if (rows) {
        end = strstr(p, ",\"cursor\":");
        if (!end) return 0;
    } else {
        end = resp + strlen(resp);
    }
    int n = 0;
    while (p < end) {
        p = strstr(p, "\"key\":\"");
        if (!p || p >= end) break;
        p += 7;
        if (p >= end) break;
        const char *e = strchr(p, '"');
        if (!e || e > end || n >= cap) break;
        size_t len = (size_t)(e - p);
        if (len < sizeof(out[0])) { memcpy(out[n], p, len); out[n][len] = 0; n++; }
        p = e;
    }
    return n;
}

static int key_index(const char *k) {
    for (int i = 0; i < 12; i++)
        if (strcmp(KEYS[i], k) == 0) return i;
    return -1;
}

/* Walk all pages with the given order_by JSON literal + direction; collect
 * the global key sequence. Returns number of pages walked, or -1 on a
 * missing cursor mid-stream. */
static int walk_pages(TestClient *tc, const char *order_by_lit,
                      const char *dir, char out[][32], int *out_n) {
    char req[1024], cursor[512];
    cursor[0] = '\0';
    int pages = 0, total = 0;
    for (;;) {
        if (cursor[0])
            snprintf(req, sizeof(req),
                "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
                "\"criteria\":[],\"order_by\":%s,\"order\":\"%s\",\"limit\":5,"
                "\"cursor\":%s}", order_by_lit, dir, cursor);
        else
            snprintf(req, sizeof(req),
                "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
                "\"criteria\":[],\"order_by\":%s,\"order\":\"%s\",\"limit\":5,"
                "\"cursor\":null}", order_by_lit, dir);
        char *resp = NULL;
        tc_request(tc, req, &resp);
        ASSERT_NOT_NULL(resp, "walk_pages: response");
        if (!resp) return -1;
        char keys[16][32];
        int n = parse_row_keys(resp, keys, 16);
        for (int i = 0; i < n && total < 64; i++) {
            snprintf(out[total++], 32, "%s", keys[i]);
        }
        /* Extract next cursor verbatim (or null). */
        const char *c = strstr(resp, "\"cursor\":");
        if (!c) { free(resp); return -1; }
        c += 9;
        if (strncmp(c, "null", 4) == 0) { free(resp); pages++; break; }
        const char *cs = strchr(c, '{');
        const char *ce = cs ? strchr(cs, '}') : NULL;
        if (!cs || !ce) { free(resp); return -1; }
        size_t clen = (size_t)(ce - cs) + 1;
        if (clen >= sizeof(cursor)) { free(resp); return -1; }
        memcpy(cursor, cs, clen); cursor[clen] = '\0';
        free(resp);
        pages++;
        if (pages > 10) break;
    }
    *out_n = total;
    return pages;
}

static int test_multi_order_cursor_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"mo\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"tenant:int\",\"day:date\",\"note:varchar:16\"],"
        "\"indexes\":[\"tenant+day\",\"tenant+day+note\",\"note+day\"]}",
        &resp); free(resp); resp = NULL;

    for (int i = 0; i < 12; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"mo\","
            "\"key\":\"%s\",\"value\":{\"tenant\":%d,\"day\":\"%s\","
            "\"note\":\"n%02d\"}}", KEYS[i], TENANT[i], DAY[i], i);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* ---- A. ASC multi-field cursor walk (array form) ---- */
    char seq[64][32];
    int total = 0;
    int pages = walk_pages(tc, "[\"tenant\",\"day\"]", "asc", seq, &total);
    ASSERT_EQ_INT(pages, 3, "asc: 12 rows / limit 5 → 3 pages");
    ASSERT_EQ_INT(total, 12, "asc: every row returned across pages");
    for (int i = 0; i < total; i++)
        for (int j = i + 1; j < total; j++)
            ASSERT_TRUE(strcmp(seq[i], seq[j]) != 0,
                        "asc: no key repeated across pages");
    int t_prev = -1;
    char d_prev[16] = "";
    for (int i = 0; i < total; i++) {
        int ki = key_index(seq[i]);
        ASSERT_TRUE(ki >= 0, "asc: known key");
        ASSERT_TRUE(TENANT[ki] > t_prev ||
                    (TENANT[ki] == t_prev &&
                     strcmp(DAY[ki], d_prev) >= 0),
                    "asc: global (tenant,day) order holds across pages");
        t_prev = TENANT[ki];
        snprintf(d_prev, sizeof(d_prev), "%s", DAY[ki]);
    }

    /* ---- B. DESC multi-field cursor walk (CSV form) ---- */
    memset(seq, 0, sizeof(seq));
    total = 0;
    pages = walk_pages(tc, "\"tenant,day\"", "desc", seq, &total);
    ASSERT_EQ_INT(pages, 3, "desc: 3 pages");
    ASSERT_EQ_INT(total, 12, "desc: 12 rows");
    t_prev = 1 << 30;
    snprintf(d_prev, sizeof(d_prev), "9999-99-99");
    for (int i = 0; i < total; i++) {
        int ki = key_index(seq[i]);
        ASSERT_TRUE(ki >= 0, "desc: known key");
        ASSERT_TRUE(TENANT[ki] < t_prev ||
                    (TENANT[ki] == t_prev &&
                     strcmp(DAY[ki], d_prev) <= 0),
                    "desc: global (tenant,day) reverse order holds");
        t_prev = TENANT[ki];
        snprintf(d_prev, sizeof(d_prev), "%s", DAY[ki]);
    }

    /* ---- B2. Single-field direction suffix drives the cursor walk ---- */
    memset(seq, 0, sizeof(seq));
    total = 0;
    pages = walk_pages(tc, "\"tenant:desc\"", "asc", seq, &total);
    ASSERT_EQ_INT(pages, 3, "B2: 3 pages");
    ASSERT_EQ_INT(total, 12, "B2: 12 rows");
    t_prev = 1 << 30;
    for (int i = 0; i < total; i++) {
        int ki = key_index(seq[i]);
        ASSERT_TRUE(ki >= 0, "B2: known key");
        ASSERT_TRUE(TENANT[ki] <= t_prev,
                    "B2: suffix :desc orders tenant descending");
        t_prev = TENANT[ki];
    }

    /* ---- C. Page-1 token carries both parts ---- */    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"tenant\",\"day\"],"
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"cursor\":{", "page 1 emits a cursor token");
    ASSERT_CONTAINS(resp, "\"tenant\":\"", "token has tenant part");
    ASSERT_CONTAINS(resp, "\"day\":\"", "token has day part");
    ASSERT_CONTAINS(resp, "\"key\":\"", "token has key part");
    free(resp); resp = NULL;

    /* ---- D. Non-cursor multi-field order_by (buffer-sort; bare array) ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"tenant\",\"day\"],"
        "\"order\":\"desc\",\"limit\":100}", &resp);
    ASSERT_NOT_NULL(resp, "buffer-sort: response");
    ASSERT_TRUE(resp[0] == '[', "buffer-sort: bare array (no wrapper)");
    {
        char ks[16][32];
        int n = parse_row_keys(resp, ks, 16);
        ASSERT_EQ_INT(n, 12, "buffer-sort: all rows");
        int prev_t = 1 << 30; char prev_d[16] = "9999-99-99";
        for (int i = 0; i < n; i++) {
            int ki = key_index(ks[i]);
            ASSERT_TRUE(ki >= 0, "buffer-sort: known key");
            ASSERT_TRUE(TENANT[ki] < prev_t ||
                        (TENANT[ki] == prev_t &&
                         strcmp(DAY[ki], prev_d) <= 0),
                        "buffer-sort: desc (tenant,day) order");
            prev_t = TENANT[ki];
            snprintf(prev_d, sizeof(prev_d), "%s", DAY[ki]);
        }
    }
    free(resp); resp = NULL;

    /* ---- D2. Mixed per-field directions, non-cursor → buffer-sort ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"tenant:asc\",\"day:desc\"],"
        "\"limit\":100}", &resp);
    ASSERT_NOT_NULL(resp, "mixed-dir buffer-sort: response");
    ASSERT_TRUE(resp[0] == '[', "mixed-dir buffer-sort: bare array");
    {
        char ks[16][32];
        int n = parse_row_keys(resp, ks, 16);
        ASSERT_EQ_INT(n, 12, "mixed-dir buffer-sort: all rows");
        int prev_t = -1;
        char prev_d[16] = "9999-99-99";
        for (int i = 0; i < n; i++) {
            int ki = key_index(ks[i]);
            ASSERT_TRUE(ki >= 0, "mixed-dir buffer-sort: known key");
            ASSERT_TRUE(TENANT[ki] > prev_t ||
                        (TENANT[ki] == prev_t &&
                         strcmp(DAY[ki], prev_d) <= 0),
                        "mixed-dir: tenant asc, day desc within tenant");
            prev_t = TENANT[ki];
            snprintf(prev_d, sizeof(prev_d), "%s", DAY[ki]);
        }
    }
    free(resp); resp = NULL;

    /* ---- D3. Selective criteria → D2 fetch-sort must not break
       multi-field order (routing guard from Task 2b) ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[{\"field\":\"tenant\",\"op\":\"eq\",\"value\":\"1\"}],"
        "\"order_by\":[\"tenant\",\"day\"],\"order\":\"asc\",\"limit\":100}",
        &resp);
    ASSERT_NOT_NULL(resp, "D3: response");
    ASSERT_TRUE(resp[0] == '[', "D3: bare array");
    {
        char ks[16][32];
        int n = parse_row_keys(resp, ks, 16);
        ASSERT_EQ_INT(n, 4, "D3: tenant=1 rows only");
        char prev_d[16] = "";
        for (int i = 0; i < n; i++) {
            int ki = key_index(ks[i]);
            ASSERT_TRUE(ki >= 0, "D3: known key");
            ASSERT_TRUE(strcmp(DAY[ki], prev_d) >= 0,
                        "D3: day ascending under selective filter");
            snprintf(prev_d, sizeof(prev_d), "%s", DAY[ki]);
        }
    }
    free(resp); resp = NULL;

    /* ---- D4. Single-field suffix, non-cursor → indexed-walk fast path ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":\"tenant:desc\",\"limit\":100}", &resp);
    ASSERT_NOT_NULL(resp, "D4: response");
    ASSERT_TRUE(resp[0] == '[', "D4: bare array");
    {
        char ks[16][32];
        int n = parse_row_keys(resp, ks, 16);
        ASSERT_EQ_INT(n, 12, "D4: all rows");
        int prev = 1 << 30;
        for (int i = 0; i < n; i++) {
            int ki = key_index(ks[i]);
            ASSERT_TRUE(ki >= 0, "D4: known key");
            ASSERT_TRUE(TENANT[ki] <= prev, "D4: tenant descending");
            prev = TENANT[ki];
        }
    }
    free(resp); resp = NULL;

    /* ---- E. Rejections ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"tenant\",\"note\"],"
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "multi-field order_by requires a composite index",
                    "missing composite rejected");
    ASSERT_CONTAINS(resp, "\"index\":\"tenant+note\"",
                    "error names the composite to add");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"note\",\"day\"],"
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "fixed-width", "varchar non-final part rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"a\",\"b\",\"c\",\"d\",\"e\"],"
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "at most 4 fields", ">4 parts rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"tenant:asc\",\"day:desc\"],"
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "one shared order direction",
                    "mixed directions + cursor rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"day:sideways\"],"
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "invalid per-field order direction",
                    "malformed direction suffix rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"day:sideways\"],"
        "\"limit\":5}", &resp);
    ASSERT_CONTAINS(resp, "invalid per-field order direction",
                    "suffix error without cursor (validation path)");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":\"tenant,\","
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "empty field name", "trailing comma rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":\" tenant , day \","
        "\"order\":\"asc\",\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"rows\":", "whitespace-tolerant split");
    free(resp); resp = NULL;

    /* ---- F. Cursor token missing a part → clear error ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":[\"tenant\",\"day\"],"
        "\"order\":\"asc\",\"limit\":5,"
        "\"cursor\":{\"tenant\":\"1\",\"key\":\"k01\"}}", &resp);
    ASSERT_CONTAINS(resp, "cursor missing order_by field value",
                    "partial cursor token rejected");
    free(resp); resp = NULL;

    /* ---- G0. Plain find without order_by — untouched ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"limit\":100}", &resp);
    ASSERT_NOT_NULL(resp, "G0: response");
    ASSERT_TRUE(resp[0] == '[', "G0: bare array, no error");
    {
        char ks[16][32];
        int n = parse_row_keys(resp, ks, 16);
        ASSERT_EQ_INT(n, 12, "G0: all rows");
    }
    free(resp); resp = NULL;

    /* ---- G. Single-field regression: token shape unchanged ---- */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"mo\","
        "\"criteria\":[],\"order_by\":\"tenant\",\"order\":\"asc\","
        "\"limit\":5,\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"cursor\":{\"tenant\":\"", "single-field token");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-multi-order-cursor", test_multi_order_cursor_run)
```

Build and run:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-multi-order-cursor
```

Expected red failures on the base branch (each maps to a later task —
confirm all before proceeding):
- A/B/C: `"cursor requires order_by"` or `rows` missing — today a JSON
  array `order_by` yields `json_obj_strdup → NULL`, so order/cursor mode is
  silently dropped (Tasks 3/4).
- B2: `"tenant:desc"` fails field validation today (`validate_field` sees
  the raw suffixed name), so the walk never happens (Tasks 2b/4).
- D: rows come back in scan order, not (tenant,day) order (Task 5).
- D2: rows come back unordered (order dropped on a colon-bearing name), so
  the tenant-asc/day-desc assertion fails (Task 5).
- D3: with selective criteria the D2 fetch-sort branch intercepts and
  resolves only one field, so the day-ascending assertion fails (Tasks
  2b/5).
- D4: today `"tenant:desc"` fails validation outright (`{"error":…}` —
  not a bare array), and after Task 2b alone it would walk ASC; the desc
  assertion goes green only with Task 2b Edit 4.
- E: today the missing-composite, `note,day`, mixed-directions+cursor,
  `day:sideways` (both forms), and `"tenant,"` queries return rows with
  order silently dropped instead of their errors (Tasks 2b/4); the
  whitespace case returns rows but with order dropped (Task 2b).
- F: returns rows with order dropped — the parse never sees part names, so
  no part-missing error (Task 4).
- G/G0 pass today already (regression guards — must stay green throughout;
  G0 exists because Task 2b's resolve sits on the unordered find path and
  must never fire for a plain find).

---

### Task 2 — `OrderSpec` helpers in query.c

**Test-first:** run `./build/bin/shard-db-test run test-multi-order-cursor`
before edits and record the baseline (all of A/B2/B/C/D2/D3/D/E/F red, G
green). This task adds only unused static helpers — after it, the red/green
set is unchanged and the suite must still compile and run identically. The
flips begin at Task 2b.

Insert immediately **before** the find-cursor section header, quoted anchor:

```c
/* ========== Find cursor (keyset pagination) ========== */
```

New code (complete):

```c
/* ========== Multi-field order_by (shared by cursor + buffer-sort) ======
   order_by accepts a CSV string ("a,b") or, on the wire, a JSON array that
   server.c flattens to CSV via json_obj_string_or_array. One shared sort
   direction. Cursor mode additionally requires the exact composite index
   "a+b+…" and fixed-width (non-varchar) encodings for every non-final
   part: composite leaf values are enc(f1)‖…‖enc(fN)‖hash16 with varchar
   parts as raw content bytes, so only fixed-width prefixes guarantee that
   byte order equals tuple order (see the plan's invariant section). */
#define MAX_ORDER_FIELDS 4

typedef struct {
    int               n;
    char              names[MAX_ORDER_FIELDS][256];
    int               idx[MAX_ORDER_FIELDS];   /* typed-schema index, -1 = unresolved */
    const TypedField *tf[MAX_ORDER_FIELDS];
    int               dir[MAX_ORDER_FIELDS];   /* resolved: 0=asc, 1=desc */
} OrderSpec;

/* Split the order_by CSV into trimmed part names with optional per-part
   direction suffixes ("name:asc" / "name:desc"; a bare name inherits
   shared_desc). Returns 0 on success, -1 with *err set. Purely lexical —
   no schema access. Field names never contain ':' (the CLI's own
   --order-by parser has always relied on that), so a suffix is
   unambiguous. */
static int order_by_split(const char *csv,
                          char parts[][256], int dirs[],
                          int *nparts, const char **err) {
    *nparts = 0;
    if (!csv || !csv[0]) { *err = "cursor requires order_by"; return -1; }
    const char *p = csv;
    while (*p) {
        if (*nparts >= MAX_ORDER_FIELDS) {
            *err = "order_by supports at most 4 fields"; return -1;
        }
        /* trim leading spaces */
        while (*p == ' ' || *p == '\t') p++;
        char *dst = parts[(*nparts)];
        int len = 0;
        while (*p && *p != ',') {
            if (len >= 255) { *err = "order_by field name too long"; return -1; }
            dst[len++] = *p++;
        }
        /* trim trailing spaces */
        while (len > 0 && (dst[len-1] == ' ' || dst[len-1] == '\t')) len--;
        dst[len] = '\0';
        if (len == 0) { *err = "order_by contains an empty field name"; return -1; }
        /* optional per-part direction suffix (last ':') */
        dirs[*nparts] = -1;
        for (int l = len - 1; l >= 0; l--) {
            if (dst[l] != ':') continue;
            const char *d = dst + l + 1;
            if (strcmp(d, "asc") == 0)       dirs[*nparts] = 0;
            else if (strcmp(d, "desc") == 0) dirs[*nparts] = 1;
            else {
                *err = "invalid per-field order direction; use asc or desc";
                return -1;
            }
            if (l == 0) { *err = "order_by contains an empty field name"; return -1; }
            dst[l] = '\0';
            break;
        }
        (*nparts)++;
        if (*p == ',') {
            p++;
            if (!*p) { *err = "order_by contains an empty field name"; return -1; }
        }
    }
    if (*nparts == 0) { *err = "order_by contains an empty field name"; return -1; }
    return 0;
}

/* Split + resolve every part against the typed schema, merging the shared
   direction with per-part suffixes (suffix wins). Single-field queries
   keep full legacy behavior: an unresolved name returns success with
   idx=-1 so the caller's legacy fallbacks (btree_idx_exists check,
   decode_field) apply unchanged. Multi-field requires every part to
   resolve. */
static int order_spec_resolve(const FieldSchema *fs,
                              const char *order_by_csv,
                              int shared_desc,
                              OrderSpec *out,
                              const char **err) {
    memset(out, 0, sizeof(*out));
    if (order_by_split(order_by_csv, out->names, out->dir,
                       &out->n, err) != 0) return -1;
    for (int i = 0; i < out->n; i++)
        if (out->dir[i] < 0) out->dir[i] = shared_desc ? 1 : 0;
    if (out->n == 1 && (!fs || !fs->ts)) { out->idx[0] = -1; return 0; }
    for (int i = 0; i < out->n; i++) {
        out->idx[i] = -1;
        out->tf[i]  = NULL;
        if (fs && fs->ts) {
            for (int j = 0; j < fs->ts->nfields; j++) {
                if (strcmp(fs->ts->fields[j].name, out->names[i]) == 0) {
                    out->idx[i] = j;
                    out->tf[i]  = &fs->ts->fields[j];
                    break;
                }
            }
        }
        if (out->idx[i] < 0) {
            if (out->n == 1) return 0;   /* legacy single-field fallback */
            *err = "multi-field order_by field is not in the typed schema";
            return -1;
        }
    }
    return 0;
}

/* "a,b,c" → "a+b+c" (the composite index directory name). */
static void order_spec_composite_name(const OrderSpec *os,
                                      char *out, size_t cap) {
    size_t pos = 0;
    out[0] = '\0';
    for (int i = 0; i < os->n && pos < cap; i++) {
        int w = snprintf(out + pos, cap - pos, "%s%s", i ? "+" : "", os->names[i]);
        if (w < 0 || (size_t)w >= cap - pos) return;
        pos += (size_t)w;
    }
}

/* 1 = every non-final part has a fixed-width index encoding. */
static int order_spec_nonfinal_fixed(const OrderSpec *os) {
    for (int i = 0; i + 1 < os->n; i++)
        if (os->tf[i] && os->tf[i]->type == FT_VARCHAR) return 0;
    return 1;
}

/* 1 = all parts share one direction. The composite btree is a single byte
   order (forward = ASC, reverse = DESC), so this is the prerequisite for
   serving a query from the index walk. */
static int order_spec_uniform(const OrderSpec *os) {
    for (int i = 1; i < os->n; i++)
        if (os->dir[i] != os->dir[0]) return 0;
    return 1;
}
```

Note: `order_by_split` returns `"cursor requires order_by"` for an empty
string to preserve the exact existing parse error when cursor mode is
active with no `order_by`. The buffer-sort path only calls
`order_spec_resolve` when `has_order` is already true, so the message never
surfaces there.

Build (no behavior change yet): `SKIP_TESTS=1 ./build.sh` — must compile
clean; the helpers are unused until Tasks 4–5
(`-Wunused-function` is not in the warning set — if the build does flag
them, mark the two leaf helpers `__attribute__((unused))` for this task
only and remove the attributes in Task 4).

---

### Task 2b — early validation, one-time resolution, suffix normalization, D2 routing

All edits in `src/db/query.c`, in `cmd_find_do`, before any dispatch.

**Test-first:** run `./build/bin/shard-db-test run test-multi-order-cursor`
and confirm these assertions are red before this task and green after it:
the trailing-comma case (`"tenant,"` → `empty field name`), the malformed
suffix cases (`"day:sideways"` with and without `cursor` → `invalid
per-field order direction`, now surfacing at validation, before cursor
dispatch), the whitespace-tolerance case, and D4 (single-field suffix
non-cursor walk, including its direction). Everything else stays red
(groups A/B2/B/C/D2/D3/D/F wait for Tasks 4–5); G and G0 stay green — G0
(plain find, no order_by) is the guard that the resolve's order_by guard
never fires on unordered finds.

**Why:** three consumers still treat `order_by` as one opaque field name
and would break multi-field or suffixed input before the new code runs:
(1) the field-validation block validates `order_by` verbatim;
(2) the D2 fetch-sort branch passes the whole string to
`find_via_fetch_sort`, which resolves a single field;
(3) a suffixed single field (`"tenant:desc"`) would leak into
`btree_idx_exists` / `order_walk_bounds` / the planner hint as the literal
name `tenant:desc`; and (4) once normalization lets the suffixed field
reach the indexed-walk fast path, that path's own `desc` flag still comes
only from the shared `order`, so the suffix direction would be dropped.
This task fixes all four by splitting for validation, resolving
`OrderSpec` once, normalizing a single-field `order_by` down to its
stripped name, and pointing every local `desc` computation at
`ospec.dir[0]`.

**Edit 1 — validation + resolve + normalize.** Anchor (exact):

```c
    {
        char verr[256];
        if (validate_criteria_tree_fields(tree, driver_fs.ts, verr, sizeof(verr)) < 0 ||
            (proj_count > 0 && validate_field_list(proj_fields, proj_count, driver_fs.ts,
                                                   "projection", verr, sizeof(verr)) < 0) ||
            (order_by && order_by[0] && validate_field(driver_fs.ts, order_by,
                                                       "order_by", verr, sizeof(verr)) < 0)) {
            OUT("{\"error\":\"%s\"}\n", verr);
            free_joins(joins, njoins);
            free_excluded(&excluded);
            return -1;
        }
    }

    compile_criteria_tree(tree, driver_fs.ts);
```

Replace with:

```c
    {
        char verr[256];
        int vbad =
            validate_criteria_tree_fields(tree, driver_fs.ts,
                                          verr, sizeof(verr)) < 0 ||
            (proj_count > 0 && validate_field_list(proj_fields, proj_count,
                                                   driver_fs.ts,
                                                   "projection", verr,
                                                   sizeof(verr)) < 0);
        /* order_by may be a CSV list ("a,b") with per-part ":asc/:desc"
           suffixes — split first, then validate each bare field name.
           Split-level errors (too many fields, empty part, bad suffix)
           surface here, before any dispatch. */
        if (!vbad && order_by && order_by[0]) {
            char onames[MAX_ORDER_FIELDS][256];
            int  odirs[MAX_ORDER_FIELDS];
            int  on = 0;
            const char *oerr = NULL;
            if (order_by_split(order_by, onames, odirs, &on, &oerr) != 0) {
                OUT("{\"error\":\"%s\"}\n", oerr);
                free_joins(joins, njoins);
                free_excluded(&excluded);
                return -1;
            }
            for (int i = 0; i < on && !vbad; i++) {
                if (validate_field(driver_fs.ts, onames[i], "order_by",
                                   verr, sizeof(verr)) < 0)
                    vbad = 1;
            }
        }
        if (vbad) {
            OUT("{\"error\":\"%s\"}\n", verr);
            free_joins(joins, njoins);
            free_excluded(&excluded);
            return -1;
        }
    }

    /* Resolve order_by parts once (already validated above): names,
       typed-schema indexes, and merged directions. Consumed by the cursor
       branch, the D2 fetch-sort guard, and the buffered fallback. Plain
       finds without order_by keep an empty OrderSpec (n = 0) and are
       untouched — every ospec consumer sits behind a path that requires
       order_by (cursor parse errors out first; D2 / indexed-walk /
       buffered are gated by fp.order / has_order). */
    OrderSpec ospec;
    memset(&ospec, 0, sizeof(ospec));
    if (order_by && order_by[0]) {
        int shared_desc = (order_dir && (strcmp(order_dir, "desc") == 0 ||
                                         strcmp(order_dir, "DESC") == 0));
        const char *ospec_err = NULL;
        if (order_spec_resolve(&driver_fs, order_by, shared_desc,
                               &ospec, &ospec_err) != 0) {
            OUT("{\"error\":\"%s\"}\n",
                ospec_err ? ospec_err : "invalid order_by");
            free_joins(joins, njoins);
            free_excluded(&excluded);
            return -1;
        }
    }
    /* Normalize a single-field order_by ("name:dir" suffix stripped) so
       every downstream single-field consumer (planner hint, indexed-walk
       fast path, D2) sees the bare field name. Multi-field CSV keeps
       flowing unchanged: no index matches it, so those paths fall through
       to the buffered sort. */
    char order_norm[256];
    if (order_by && order_by[0] && ospec.n == 1 &&
        strcmp(ospec.names[0], order_by) != 0) {
        snprintf(order_norm, sizeof(order_norm), "%s", ospec.names[0]);
        order_by = order_norm;
    }

    compile_criteria_tree(tree, driver_fs.ts);
```

(`order_by` is the function's `const char *` parameter — repointing it at
the normalized local is what fixes the suffix leak for every existing
consumer without touching them.)

**Edit 2 — D2 fetch-sort routing guard.** With multi-field input the D2
branch must not intercept: `find_via_fetch_sort` resolves exactly one
field, so a CSV value would sort by nothing. Multi-field falls through to
the indexed-walk check (no index matches a CSV name) and lands in the
buffered fallback. Anchor (exact):

```c
    } else if (fp.order == FP_ORDER_SORT &&
               !has_joins && !rows_fmt && !csv_delim &&
               fp.n_source > 0) {
```

Replace with:

```c
    } else if (fp.order == FP_ORDER_SORT &&
               !has_joins && !rows_fmt && !csv_delim &&
               fp.n_source > 0 &&
               (!order_by || strchr(order_by, ',') == NULL)) {
```

**Edit 3 — D2 direction.** The D2 branch computes its own `desc` from the
shared `order` only; a single-field suffix (`"tenant:desc"`) would be
ignored. Anchor (exact):

```c
        int desc = (order_dir && (strcmp(order_dir, "desc") == 0 ||
                                  strcmp(order_dir, "DESC") == 0));
        size_t d2_matched = find_via_fetch_sort(
```

Replace with:

```c
        int desc = ospec.dir[0];   /* suffix-aware (merged in Task 2b) */
        size_t d2_matched = find_via_fetch_sort(
```

**Edit 4 — indexed-walk fast path direction.** Same defect one branch
later: the non-cursor indexed-walk fast path computes its own `desc` from
the shared `order` only. With normalization in place, `"tenant:desc"`
(no shared `order`) reaches this block and would walk ASC. Anchor (exact):

```c
            }
        }

        int desc = (order_dir && (strcmp(order_dir, "desc") == 0 ||
                                  strcmp(order_dir, "DESC") == 0));
```

Replace with:

```c
            }
        }

        int desc = ospec.dir[0];   /* suffix-aware (merged in Task 2b) */
```

---

### Task 3 — wire + CLI parsing

**Test-first:** run `./build/bin/shard-db-test run test-multi-order-cursor`
before edits (post-2b baseline: E split/validation assertions green, the
rest of A/B2/B/C/D2/D3/D/F red, G green). This task alone flips nothing —
array-form requests begin to flatten to CSV but the cursor and buffer paths
still treat it as one name — so after it the red/green set is unchanged.
The flips land in Tasks 4 and 5.

**server.c, find path.** Anchor (exact):

```c
        char *ob = json_obj_strdup(&req, "order_by");
        char *od = json_obj_strdup(&req, "order");
        char *cur = json_obj_strdup_raw(&req, "cursor");
```

Replace the first line only:

```c
        char *ob = json_obj_string_or_array(&req, "order_by");
        char *od = json_obj_strdup(&req, "order");
        char *cur = json_obj_strdup_raw(&req, "cursor");
```

`json_obj_string_or_array` returns a malloc'd CSV string (or NULL) with the
same ownership/free pattern as `json_obj_strdup`, so the surrounding
error/free paths need no edits. The aggregate dispatch's `order_by` read
stays `json_obj_strdup` (single-field, D4).

**CLI (nql.c).** The existing `--order-by` parser splits at the *last*
colon and treats everything after it as the shared direction — correct for
`tenant,day:desc`, but it would mangle per-field suffixes
(`tenant:asc,day:desc` → field part `tenant:asc,day`, dir `desc`). Refine
the rule: a trailing `:asc/:desc` is the shared direction only when the
field part is colon-free; otherwise the colons are per-field suffixes that
`order_by_split` parses. Anchor (exact):

```c
        else if (!strcmp(argv[i],"--order-by") && i+1<argc) {
            char *spec = argv[++i]; i++;
            char *colon = strrchr(spec, ':');
            if (colon) {
                *colon = '\0';
                if (spec[0] == '\0') {
                    snprintf(out->err, sizeof out->err, "--order-by requires a field name before ':'");
                    return -1;
                }
                if (normalize_order_dir(colon + 1, out->order_dir, sizeof out->order_dir) != 0) {
                    snprintf(out->err, sizeof out->err, "invalid order direction '%s'; use 'asc' or 'desc'", colon + 1);
                    return -1;
                }
            }
            else         snprintf(out->order_dir,sizeof out->order_dir,"asc");
            snprintf(out->order_by, sizeof out->order_by, "%s", spec);
        }
```

Replace with:

```c
        else if (!strcmp(argv[i],"--order-by") && i+1<argc) {
            char *spec = argv[++i]; i++;
            char *colon = strrchr(spec, ':');
            if (colon) {
                /* Trailing ":asc"/":desc" is a shared direction only when
                   the field part is colon-free; otherwise the colons are
                   per-field suffixes parsed by the query layer. */
                char cut = *colon;
                *colon = '\0';
                if (strchr(spec, ':') == NULL) {
                    if (spec[0] == '\0') {
                        snprintf(out->err, sizeof out->err, "--order-by requires a field name before ':'");
                        return -1;
                    }
                    if (normalize_order_dir(colon + 1, out->order_dir, sizeof out->order_dir) != 0) {
                        snprintf(out->err, sizeof out->err, "invalid order direction '%s'; use 'asc' or 'desc'", colon + 1);
                        return -1;
                    }
                } else {
                    *colon = cut;   /* per-field suffixes; keep whole spec */
                    snprintf(out->order_dir, sizeof out->order_dir, "asc");
                }
            }
            else         snprintf(out->order_dir,sizeof out->order_dir,"asc");
            snprintf(out->order_by, sizeof out->order_by, "%s", spec);
        }
```

Rebuild, run `./build/bin/shard-db-test run test-multi-order-cursor` — Task
1's group D and the rejections in E may start failing differently (order no
longer silently dropped) but nothing is green yet. That is expected.

---

### Task 4 — cursor path (composite walk + tuple token)

**Test-first:** run `./build/bin/shard-db-test run test-multi-order-cursor`
before edits and confirm red: groups A, B, B2, C (cursor walk + token),
F (partial token), and the mixed-directions+cursor rejection. After this
task those are green; D, D2 (mixed buffer-sort), and D3 (selective-criteria
buffer-sort) stay red until Task 5; G, D4, and the E rejections stay green.

All edits in `src/db/query.c`.

#### 4a. `FindCursor` + `parse_cursor_object`

Anchor — the struct (exact):

```c
typedef struct {
    int    present;
    char   value[1024];      /* textual value of the order_by field */
    size_t vlen;
    char   key[1024];        /* primary key string */
    size_t klen;
} FindCursor;
```

Replace with:

```c
typedef struct {
    int    present;
    int    nparts;
    char   value[MAX_ORDER_FIELDS][1024]; /* textual value per order part */
    size_t vlen[MAX_ORDER_FIELDS];
    char   key[1024];        /* primary key string */
    size_t klen;
} FindCursor;
```

Anchor — the parse body from its first lines through the single-value
lookup (exact):

```c
static int parse_cursor_object(const char *cursor_json, const char *order_by,
                               FindCursor *out, const char **err) {
    out->present = 0;
    out->vlen = 0;
    out->klen = 0;
```

Replace with:

```c
static int parse_cursor_object(const char *cursor_json, const char *order_by,
                               FindCursor *out, const char **err) {
    out->present = 0;
    out->nparts = 0;
    for (int i = 0; i < MAX_ORDER_FIELDS; i++) out->vlen[i] = 0;
    out->klen = 0;
```

The caller (`int cr = parse_cursor_object(cursor_json, order_by, &cur,
&cerr);`) is unchanged — `order_by` is now interpreted as CSV. Next, the
**tail** of the parse function. Anchor (exact, current tail):

```c
    if (!order_by || !order_by[0]) {
        *err = "cursor requires order_by";
        return -1;
    }
    char *vv = json_obj_strdup(&c, order_by);
    if (!vv) { *err = "cursor missing order_by field value"; return -1; }
    size_t vlen = strlen(vv);
    if (vlen >= sizeof(out->value)) { free(vv); *err = "cursor value too long"; return -1; }
    memcpy(out->value, vv, vlen + 1);
    out->vlen = vlen;
    free(vv);

    out->present = 1;
    return 0;
}
```

Replace with:

```c
    if (!order_by || !order_by[0]) {
        *err = "cursor requires order_by";
        return -1;
    }
    char names[MAX_ORDER_FIELDS][256];
    int  dirs[MAX_ORDER_FIELDS];
    int  nparts = 0;
    if (order_by_split(order_by, names, dirs, &nparts, err) != 0) return -1;
    for (int i = 0; i < nparts; i++) {
        char *vv = json_obj_strdup(&c, names[i]);
        if (!vv) { *err = "cursor missing order_by field value"; return -1; }
        size_t vlen = strlen(vv);
        if (vlen >= sizeof(out->value[i])) {
            free(vv); *err = "cursor value too long"; return -1;
        }
        memcpy(out->value[i], vv, vlen + 1);
        out->vlen[i] = vlen;
        free(vv);
    }
    out->nparts = nparts;

    out->present = 1;
    return 0;
}
```

The caller (`int cr = parse_cursor_object(cursor_json, order_by, &cur,
&cerr);`) is unchanged — `order_by` is now interpreted as CSV. Extra
unknown keys inside the cursor JSON stay ignored (existing behavior).

#### 4b. Cursor-branch validation (ospec arrives from Task 2b)

`OrderSpec ospec` was resolved once in `cmd_find_do` (Task 2b), and a
single-field `order_by` was already normalized to its stripped name — so
the cursor branch only enforces the cursor-specific requirements (indexed /
composite exists, uniform direction, fixed-width non-final parts).

Anchor — the validation block (exact):

```c
        /* order_by must be indexed (hard requirement for cursor). */
        if (!btree_idx_exists(db_root, object, order_by, sch.splits)) {
            OUT("{\"error\":\"cursor requires order_by field to be indexed\",\"field\":\"%s\"}\n",
                order_by);
            free_joins(joins, njoins); free_excluded(&excluded);
            return -1;
        }
```

Replace with:

```c
        /* order_by must be indexed (hard requirement for cursor). */
        char composite_name[1100] = "";   /* set below when n > 1 */
        if (ospec.n == 1) {
            if (!btree_idx_exists(db_root, object, order_by, sch.splits)) {
                OUT("{\"error\":\"cursor requires order_by field to be indexed\",\"field\":\"%s\"}\n",
                    order_by);
                free_joins(joins, njoins); free_excluded(&excluded);
                return -1;
            }
        } else {
            if (!order_spec_uniform(&ospec)) {
                OUT("{\"error\":\"cursor pagination requires one shared order direction across all order_by fields\","
                    "\"order_by\":\"%s\"}\n", order_by);
                free_joins(joins, njoins); free_excluded(&excluded);
                return -1;
            }
            if (!order_spec_nonfinal_fixed(&ospec)) {
                OUT("{\"error\":\"multi-field order_by requires all but the last field to be fixed-width (non-varchar)\","
                    "\"order_by\":\"%s\"}\n", order_by);
                free_joins(joins, njoins); free_excluded(&excluded);
                return -1;
            }
            order_spec_composite_name(&ospec, composite_name, sizeof(composite_name));
            if (!btree_idx_exists(db_root, object, composite_name, sch.splits)) {
                OUT("{\"error\":\"multi-field order_by requires a composite index\",\"index\":\"%s\"}\n",
                    composite_name);
                free_joins(joins, njoins); free_excluded(&excluded);
                return -1;
            }
        }
```

Note: with a single-field suffixed input (`"tenant:desc"`), Task 2b's
normalization has already repointed `order_by` at `"tenant"`, so this
check (and everything downstream — `order_walk_bounds`, the walk itself,
the planner hint) sees the bare field name. No suffix can reach
`btree_idx_exists`.

#### 4b-2. Effective walk direction

Anchor — the shared-direction parse in the cursor branch (exact):

```c
        int desc = (order_dir && (strcmp(order_dir, "desc") == 0 ||
                                   strcmp(order_dir, "DESC") == 0));
```

Replace with:

```c
        /* Per-part suffixes override the shared order; uniformity was
           validated above, so part 0's direction is the walk direction. */
        int desc = ospec.dir[0];
```

(For a bare single-field `order_by` this is value-identical to the old
line: `ospec.dir[0]` was merged from `shared_desc` in
`order_spec_resolve`.)

#### 4c. Per-part unescape + concatenated encode

Anchor — the varchar unescape block (exact):

```c
        /* parse_cursor_object has no schema access, so its order_by value is
           still JSON-escaped text. Decode it before using it as the index
           seek bound. */
        if (cur.present && order_tf && order_tf->type == FT_VARCHAR) {
            char *unesc = NULL; size_t ulen = 0;
            if (json_unescape_cstring(cur.value, cur.vlen, &unesc, &ulen) != 0) {
                OUT("{\"error\":\"cursor order_by value has a malformed JSON escape\"}\n");
                free_joins(joins, njoins); free_excluded(&excluded);
                return -1;
            }
            memcpy(cur.value, unesc, ulen + 1);
            cur.vlen = ulen;
            free(unesc);
        }
```

Replace with:

```c
        /* parse_cursor_object has no schema access, so its order_by values
           are still JSON-escaped text. Unescape varchar parts before index
           encoding. */
        if (cur.present) {
            for (int i = 0; i < ospec.n; i++) {
                if (ospec.tf[i] && ospec.tf[i]->type == FT_VARCHAR) {
                    char *unesc = NULL; size_t ulen = 0;
                    if (json_unescape_cstring(cur.value[i], cur.vlen[i],
                                              &unesc, &ulen) != 0) {
                        OUT("{\"error\":\"cursor order_by value has a malformed JSON escape\"}\n");
                        free_joins(joins, njoins); free_excluded(&excluded);
                        return -1;
                    }
                    memcpy(cur.value[i], unesc, ulen + 1);
                    cur.vlen[i] = ulen;
                    free(unesc);
                }
            }
        }
```

Anchor — the encode block (exact):

```c
        uint8_t cur_value_buf[1024];
        size_t  cur_value_len = 0;
        int     has_cur_bytes = 0;
        if (cur.present) {
            if (order_tf) {
                encode_field_for_index(order_tf, cur.value, cur.vlen,
                                       cur_value_buf, &cur_value_len);
            } else {
                /* Composite/unknown — raw bytes. */
                size_t cap = sizeof(cur_value_buf);
                cur_value_len = cur.vlen < cap ? cur.vlen : cap;
                memcpy(cur_value_buf, cur.value, cur_value_len);
            }
            has_cur_bytes = 1;
        }
```

Replace with:

```c
        uint8_t cur_value_buf[1024];
        size_t  cur_value_len = 0;
        int     has_cur_bytes = 0;
        if (cur.present) {
            /* Concatenate per-part encodings. With all non-final parts
               fixed-width, byte order of the concatenation equals tuple
               order, so cursor_find_cb's existing single-value compare +
               hash16 tiebreak paginates the tuple correctly. */
            for (int i = 0; i < ospec.n; i++) {
                size_t plen = 0;
                if (cur_value_len >= sizeof(cur_value_buf)) break;
                if (ospec.tf[i]) {
                    encode_field_for_index(ospec.tf[i], cur.value[i],
                                           cur.vlen[i],
                                           cur_value_buf + cur_value_len,
                                           &plen);
                    if (plen > sizeof(cur_value_buf) - cur_value_len)
                        plen = sizeof(cur_value_buf) - cur_value_len;
                } else {
                    plen = cur.vlen[i];
                    if (plen > sizeof(cur_value_buf) - cur_value_len)
                        plen = sizeof(cur_value_buf) - cur_value_len;
                    memcpy(cur_value_buf + cur_value_len, cur.value[i], plen);
                }
                cur_value_len += plen;
            }
            has_cur_bytes = 1;
        }
```

The existing `cursor_find_cb` comparison and the `compute_hash_raw(cur.key,
cur.klen, …)` tiebreak setup are untouched — they already operate on whole
value bytes + hash16.

#### 4d. C1 shortcut guard (fetch+sort path stays single-field)

Anchor (exact):

```c
            if (prefer_fetch_sort(c1_ks, cursor_N_live, offset, limit,
                                  cursor_fp.source_is_bitmap) &&
                 order_tf && driver_fs.ts && order_field_idx >= 0) {
```

Replace with:

```c
            if (prefer_fetch_sort(c1_ks, cursor_N_live, offset, limit,
                                  cursor_fp.source_is_bitmap) &&
                 ospec.n == 1 &&
                 order_tf && driver_fs.ts && order_field_idx >= 0) {
```

(Multi-field cursor queries fall through to the full btree walk — correct,
just not the fetch-sort shortcut. `order_tf` would be NULL for a CSV value
anyway; the explicit guard makes the invariant local.)

#### 4e. `CursorFindCtx`: per-part capture

Anchor — the capture fields (exact):

```c
    /* Captured last-emitted cursor (raw bytes). Heap-owned, freed by caller. */
    char          *last_value_str;
    char          *last_key_str;
```

Replace with:

```c
    /* Captured last-emitted cursor: one JSON-escaped string per order_by
       part. Heap-owned, freed by caller. */
    char          *last_value[MAX_ORDER_FIELDS];
    char          *last_key_str;

    /* Multi-field order_by parts (single-field paths: n == 1). NULL on
       paths that never emit a cursor token. */
    const OrderSpec *ospec;
```

Anchor — the capture block in `cursor_find_cb` (exact):

```c
    free(c->last_value_str);
    free(c->last_key_str);
    c->last_value_str = (c->order_tf && c->fs && c->fs->ts)
        ? json_escape_field(typed_get_field_str(c->fs->ts, raw, (int)value_len, c->order_field_idx))
        : NULL;
    c->last_key_str = strndup(key_buf, klen);
```

Replace with:

```c
    for (int i = 0; i < MAX_ORDER_FIELDS; i++) {
        free(c->last_value[i]);
        c->last_value[i] = NULL;
    }
    free(c->last_key_str);
    /* Decode each part from the fetched record (never from index bytes).
       c->ospec is NULL only on paths that never emit a token. */
    if (c->ospec && c->fs && c->fs->ts) {
        for (int i = 0; i < c->ospec->n; i++) {
            c->last_value[i] = json_escape_field(
                typed_get_field_str(c->fs->ts, raw, (int)value_len,
                                    c->ospec->idx[i]));
        }
    }
    c->last_key_str = strndup(key_buf, klen);
```

`CursorFindCtx.order_tf` / `order_field_idx` stay in the struct — the C1
fetch path (`CursorFetchCtx`/sort-key build) still uses them, and it is
guarded to n == 1 by Task 4d.

Anchor — cursor-branch ctx setup, walk path (exact):

```c
        cc.order_tf    = order_tf;
        cc.order_field_idx = order_field_idx;
```

Replace with:

```c
        cc.order_tf    = order_tf;
        cc.order_field_idx = order_field_idx;
        cc.ospec       = &ospec;
```

Anchor — cursor-branch ctx setup, C1 shortcut path (exact):

```c
                cc.order_tf        = order_tf;
                cc.order_field_idx = order_field_idx;
```

Replace with:

```c
                cc.order_tf        = order_tf;
                cc.order_field_idx = order_field_idx;
                cc.ospec           = &ospec;
```

The non-cursor ordered-walk emit path (the `CursorFindCtx cc; memset(…)`
block ending `cc.parent_out      = g_out;` before the
`for (int i = 0; i < out_n; i++)` loop over `cursor_find_cb("", 0, …)`)
deliberately gets NO `cc.ospec` — its `memset` leaves it NULL and that path
never emits a token (`offset_mode = 1`).

#### 4f. Walk bounds + dispatch

Anchor (exact):

```c
        OrderWalkBounds owb;
        order_walk_bounds(tree, &driver_fs, order_by, &owb);
        OUT(dict_fmt ? "{\"rows\":{" : "{\"rows\":[");
```

Replace with:

```c
        OrderWalkBounds owb;
        if (ospec.n == 1) {
            order_walk_bounds(tree, &driver_fs, order_by, &owb);
        } else {
            /* Multi-field: full-range composite walk in v1 (D4). */
            memset(&owb, 0, sizeof(owb));
        }
        OUT(dict_fmt ? "{\"rows\":{" : "{\"rows\":[");
        const char *walk_idx = (ospec.n == 1) ? order_by : composite_name;
```

Anchor — the DESC/ASC dispatch (exact, from `if (desc) {` through the ASC
walk call's closing):

```c
        if (desc) {
            /* DESC: start (upper) = cursor when resuming, else window-high or
               max; stop (lower) = window-low or "". */
            const char *hi_b = has_cur_bytes ? (const char *)cur_value_buf
                             : (owb.has_hi ? (const char *)owb.hi : "\xff\xff\xff\xff");
            size_t hi_l = has_cur_bytes ? cur_value_len : (owb.has_hi ? owb.hi_len : 4);
            int    hi_e = has_cur_bytes ? 0 : (owb.has_hi ? owb.hi_excl : 0);
            const char *lo_b = owb.has_lo ? (const char *)owb.lo : "";
            size_t lo_l = owb.has_lo ? owb.lo_len : 0;
            int    lo_e = owb.has_lo ? owb.lo_excl : 0;
            btree_idx_walk_ordered(db_root, object, order_by, sch.splits,
                                   lo_b, lo_l, lo_e,
                                   hi_b, hi_l, hi_e,
                                   1, cursor_find_cb, &cc);
        } else {
            /* ASC: start (lower) = cursor when resuming, else window-low or "";
               stop (upper) = window-high or max. */
            const char *lo_b = has_cur_bytes ? (const char *)cur_value_buf
                             : (owb.has_lo ? (const char *)owb.lo : "");
            size_t lo_l = has_cur_bytes ? cur_value_len : (owb.has_lo ? owb.lo_len : 0);
            int    lo_e = owb.has_lo ? owb.lo_excl : 0;
            const char *hi_b = owb.has_hi ? (const char *)owb.hi : "\xff\xff\xff\xff";
            size_t hi_l = owb.has_hi ? owb.hi_len : 4;
            int    hi_e = owb.has_hi ? owb.hi_excl : 0;
            btree_idx_walk_ordered(db_root, object, order_by, sch.splits,
                                   lo_b, lo_l, lo_e,
                                   hi_b, hi_l, hi_e,
                                   0, cursor_find_cb, &cc);
        }
```

Replace with (identical except `walk_idx` and the inclusive-on-cursor
bound: resuming passes the cursor bytes as an **inclusive** bound —
`hi_e/lo_e = 0` — and `cursor_find_cb` skips at-or-before the cursor via
the tuple+hash compare, unchanged):

```c
        if (desc) {
            /* DESC: start (upper) = cursor when resuming, else window-high or
               max; stop (lower) = window-low or "". */
            const char *hi_b = has_cur_bytes ? (const char *)cur_value_buf
                             : (owb.has_hi ? (const char *)owb.hi : "\xff\xff\xff\xff");
            size_t hi_l = has_cur_bytes ? cur_value_len : (owb.has_hi ? owb.hi_len : 4);
            int    hi_e = has_cur_bytes ? 0 : (owb.has_hi ? owb.hi_excl : 0);
            const char *lo_b = owb.has_lo ? (const char *)owb.lo : "";
            size_t lo_l = owb.has_lo ? owb.lo_len : 0;
            int    lo_e = owb.has_lo ? owb.lo_excl : 0;
            btree_idx_walk_ordered(db_root, object, walk_idx, sch.splits,
                                   lo_b, lo_l, lo_e,
                                   hi_b, hi_l, hi_e,
                                   1, cursor_find_cb, &cc);
        } else {
            /* ASC: start (lower) = cursor when resuming, else window-low or "";
               stop (upper) = window-high or max. */
            const char *lo_b = has_cur_bytes ? (const char *)cur_value_buf
                             : (owb.has_lo ? (const char *)owb.lo : "");
            size_t lo_l = has_cur_bytes ? cur_value_len : (owb.has_lo ? owb.lo_len : 0);
            int    lo_e = owb.has_lo ? owb.lo_excl : 0;
            const char *hi_b = owb.has_hi ? (const char *)owb.hi : "\xff\xff\xff\xff";
            size_t hi_l = owb.has_hi ? owb.hi_len : 4;
            int    hi_e = owb.has_hi ? owb.hi_excl : 0;
            btree_idx_walk_ordered(db_root, object, walk_idx, sch.splits,
                                   lo_b, lo_l, lo_e,
                                   hi_b, hi_l, hi_e,
                                   0, cursor_find_cb, &cc);
        }
```

#### 4g. Token emission (two sites)

Anchor — C1 shortcut emit (exact):

```c
                if (cc.printed >= limit && cc.last_value_str
                    && cc.last_key_str) {
                    OUT(",\"cursor\":{\"%s\":\"%s\",\"key\":\"%s\"}",
                        order_by, cc.last_value_str, cc.last_key_str);
                } else {
                    OUT(",\"cursor\":null");
                }
```

Anchor — walk emit (exact):

```c
        if (cc.printed >= limit && cc.last_value_str && cc.last_key_str) {
            OUT(",\"cursor\":{\"%s\":\"%s\",\"key\":\"%s\"}",
                order_by, cc.last_value_str, cc.last_key_str);
        } else {
            OUT(",\"cursor\":null");
        }
```

Replace **both** with the same loop, each preserving its site's indentation
(C1 block sits four columns deeper):

```c
if (cc.printed >= limit && cc.last_key_str) {
    int vals_ok = 1;
    for (int i = 0; i < ospec.n; i++)
        if (!cc.last_value[i]) vals_ok = 0;
    if (vals_ok) {
        OUT(",\"cursor\":{");
        for (int i = 0; i < ospec.n; i++)
            OUT("%s\"%s\":\"%s\"", i ? "," : "", ospec.names[i],
                cc.last_value[i]);
        OUT(",\"key\":\"%s\"}", cc.last_key_str);
    } else {
        OUT(",\"cursor\":null");
    }
} else {
    OUT(",\"cursor\":null");
}
```

For n == 1 this emits exactly the legacy token `{"f":"v","key":"k"}` —
byte-identical output (Task 1 group G asserts it).

#### 4h. Free sites

`free(cc.last_value_str);` appears exactly five times in query.c. Replace
every occurrence with the loop below, preserving each site's indentation,
and keeping any adjacent `free(cc.last_key_str);` untouched:

```c
for (int i = 0; i < MAX_ORDER_FIELDS; i++) free(cc.last_value[i]);
```

Sites (identified by their preceding line):
1. small-prefilter emit path — preceded by `free(fc.fullbuf);`
2. C1 shortcut tail — preceded by `free(sp_rows);`
3. walk tail — inside the post-walk cleanup next to `free(cc.last_key_str);`
4. ordered-walk fast path — same cleanup shape
5. ordered-walk fallback cleanup — same shape

After Task 4, build and run:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-multi-order-cursor
```

Groups A–F must now pass (green); G must still pass. If A/B fail with rows
missing or repeated across pages, the tie-boundary cases are the first
suspect — verify with `UBSAN_OPTIONS=print_stacktrace=1` under the asan
build before touching logic.

---

### Task 5 — buffer-sort N-key comparator (direction-aware)

**Test-first:** run `./build/bin/shard-db-test run test-multi-order-cursor`
before edits and confirm red: groups D (multi-field buffer-sort),
D2 (mixed per-field directions), D3 (selective-criteria multi-field —
Task 2b already routed it away from D2 fetch-sort; the buffered path is
still single-field). After this task the whole case is green.

All edits in `src/db/query.c`.

Anchor — `OrderedRow` (exact):

```c
typedef struct {
    char *key;
    size_t key_len;
    uint8_t *record;        /* malloc'd copy of [key bytes | value bytes] */
    size_t value_len;       /* length of the value portion */
    char *sort_str;         /* extracted sort field as string (may be NULL) */
    double sort_num;        /* numeric form, valid iff sort_is_num */
    int sort_is_num;
} OrderedRow;
```

Replace with:

```c
typedef struct {
    char *key;
    size_t key_len;
    uint8_t *record;        /* malloc'd copy of [key bytes | value bytes] */
    size_t value_len;       /* length of the value portion */
    char *sort_str[MAX_ORDER_FIELDS];  /* per-part sort string (may be NULL) */
    double sort_num[MAX_ORDER_FIELDS]; /* numeric form, valid iff sort_is_num[i] */
    int sort_is_num[MAX_ORDER_FIELDS];
    int sort_dir[MAX_ORDER_FIELDS];    /* 0=asc, 1=desc (per part) */
    int nsort;
} OrderedRow;
```

Anchor — `OrderedCollectCtx` fields (exact):

```c
    int order_field_idx;    /* index in typed schema, -1 if field unknown/untyped */
    const char *order_field_name;
```

Replace with:

```c
    int order_idx[MAX_ORDER_FIELDS]; /* per-part index in typed schema, -1 = unresolved */
    const char *order_field_name;    /* full order_by string (legacy single-field fallback) */
    int nsort;
```

Anchor — same struct, the numeric flag (exact):

```c
    int order_is_numeric;
```

Replace with:

```c
    int part_is_num[MAX_ORDER_FIELDS];
    int part_dir[MAX_ORDER_FIELDS];   /* 0=asc, 1=desc (per part) */
```

Anchor — extraction in `ordered_collect_cb` (exact):

```c
    /* Extract sort key */
    char *sv = NULL;
    if (oc->fs && oc->fs->ts && oc->order_field_idx >= 0) {
        sv = typed_get_field_str(oc->fs->ts, raw, (int)hdr->value_len, oc->order_field_idx);
    } else {
        sv = decode_field((const char *)raw, hdr->value_len, oc->order_field_name, oc->fs);
    }
```

Replace with:

```c
    /* Extract sort keys (one per order_by part) */
    char *sv[MAX_ORDER_FIELDS];
    size_t sv_bytes = 0;
    for (int i = 0; i < MAX_ORDER_FIELDS; i++) sv[i] = NULL;
    for (int i = 0; i < oc->nsort; i++) {
        if (oc->fs && oc->fs->ts && oc->order_idx[i] >= 0) {
            sv[i] = typed_get_field_str(oc->fs->ts, raw, (int)hdr->value_len,
                                        oc->order_idx[i]);
        } else if (i == 0) {
            /* legacy single-field fallback (untyped/unknown name) */
            sv[0] = decode_field((const char *)raw, hdr->value_len,
                                 oc->order_field_name, oc->fs);
        }
        if (sv[i]) sv_bytes += strlen(sv[i]) + 1;
    }
```

Anchor — row sizing in the same callback (exact):

```c
    size_t row_bytes = sizeof(OrderedRow) + hdr->key_len + rec_len + (sv ? strlen(sv) + 1 : 0);
```

Replace with:

```c
    size_t row_bytes = sizeof(OrderedRow) + hdr->key_len + rec_len + sv_bytes;
```

Anchor — the two early-error frees inside the callback. First (budget,
exact):

```c
        oc->budget_exceeded = 1;
        pthread_mutex_unlock(&oc->lock);
        free(sv);
        return 1;  /* stop scan */
```

Replace with:

```c
        oc->budget_exceeded = 1;
        pthread_mutex_unlock(&oc->lock);
        for (int i = 0; i < MAX_ORDER_FIELDS; i++) free(sv[i]);
        return 1;  /* stop scan */
```

Second (realloc failure, exact):

```c
        oc->rows = NULL;
        oc->count = 0;
        oc->cap = 0;
        oc->budget_exceeded = 1;
        pthread_mutex_unlock(&oc->lock);
        free(sv);
        return 1;
```

Replace with:

```c
        oc->rows = NULL;
        oc->count = 0;
        oc->cap = 0;
        oc->budget_exceeded = 1;
        pthread_mutex_unlock(&oc->lock);
        for (int i = 0; i < MAX_ORDER_FIELDS; i++) free(sv[i]);
        return 1;
```

Anchor — row assignment (exact):

```c
    OrderedRow *r = &oc->rows[oc->count++];
    r->key_len = hdr->key_len;
```

…through (exact):

```c
    r->sort_str = sv;
    r->sort_is_num = oc->order_is_numeric;
    r->sort_num = (oc->order_is_numeric && sv) ? atof(sv) : 0.0;
    pthread_mutex_unlock(&oc->lock);
    return 0;
```

Replace that tail with:

```c
    r->nsort = oc->nsort;
    for (int i = 0; i < MAX_ORDER_FIELDS; i++) {
        r->sort_str[i] = sv[i];
        r->sort_is_num[i] = oc->part_is_num[i];
        r->sort_num[i] = (oc->part_is_num[i] && sv[i]) ? atof(sv[i]) : 0.0;
        r->sort_dir[i] = oc->part_dir[i];
    }
    pthread_mutex_unlock(&oc->lock);
    return 0;
```

Anchor — comparator (exact):

```c
static int cmp_row_asc(const void *a, const void *b) {
    const OrderedRow *ra = (const OrderedRow *)a;
    const OrderedRow *rb = (const OrderedRow *)b;
    if (ra->sort_is_num) {
        if (ra->sort_num < rb->sort_num) return -1;
        if (ra->sort_num > rb->sort_num) return  1;
        return 0;
    }
    const char *sa = ra->sort_str ? ra->sort_str : "";
    const char *sb = rb->sort_str ? rb->sort_str : "";
    return strcmp(sa, sb);
}
```

Replace with (direction-aware: each part's compare is flipped by that
part's direction, so mixed patterns like `a:asc,b:desc` sort correctly
without an outer reversal):

```c
static int cmp_row_asc(const void *a, const void *b) {
    const OrderedRow *ra = (const OrderedRow *)a;
    const OrderedRow *rb = (const OrderedRow *)b;
    int n = ra->nsort < rb->nsort ? ra->nsort : rb->nsort;
    for (int i = 0; i < n; i++) {
        int c;
        if (ra->sort_is_num[i]) {
            if (ra->sort_num[i] < rb->sort_num[i])      c = -1;
            else if (ra->sort_num[i] > rb->sort_num[i]) c =  1;
            else continue;
        } else {
            const char *sa = ra->sort_str[i] ? ra->sort_str[i] : "";
            const char *sb = rb->sort_str[i] ? rb->sort_str[i] : "";
            c = strcmp(sa, sb);
            if (!c) continue;
        }
        return ra->sort_dir[i] ? -c : c;
    }
    return 0;
}
```

Anchor — `cmp_row_desc` (exact):

```c
static int cmp_row_desc(const void *a, const void *b) { return -cmp_row_asc(a, b); }
```

Delete the line — with per-part directions inside `cmp_row_asc`, an outer
flip would turn `a:asc,b:desc` into `a:desc,b:asc`. Equal full tuples
still compare 0 (legacy tie behavior preserved).

Anchor — buffered-fallback resolution + ctx setup. The buffered block has
its own single-field resolution locals — delete them; `ospec` was resolved
once in `cmd_find_do` (Task 2b) and is in scope here. Anchor opening
(exact):

```c
        int order_idx = -1;
        int order_is_num = 0;
        if (driver_fs.ts) {
            for (int i = 0; i < driver_fs.ts->nfields; i++) {
                if (strcmp(driver_fs.ts->fields[i].name, order_by) == 0) {
                    order_idx = i;
                    order_is_num = typed_field_is_numeric(driver_fs.ts->fields[i].type);
                    break;
                }
            }
        }
```

Replace with:

```c
        /* ospec (validated, direction-merged) was resolved once in
           cmd_find_do (Task 2b); the buffered path consumes it directly. */
```

(If the deleted locals turn out to be referenced elsewhere in the buffered
block besides the ctx assignment below, STOP and write `PLAN_NOTES.md`.)

Anchor — ctx assignment (exact):

```c
        oc.order_field_idx = order_idx;
        oc.order_field_name = order_by;
```

Replace with:

```c
        oc.nsort = ospec.n;
        oc.order_field_name = order_by;
        for (int i = 0; i < ospec.n; i++) {
            oc.order_idx[i] = ospec.idx[i];
            oc.part_is_num[i] = ospec.tf[i]
                ? typed_field_is_numeric(ospec.tf[i]->type) : 0;
            oc.part_dir[i] = ospec.dir[i];
        }
```

Anchor — the numeric flag assignment (exact):

```c
        oc.order_is_numeric = order_is_num;
```

Delete the line (the loop above replaces it).

Anchor — the qsort (exact):

```c
        int desc = (order_dir && (strcmp(order_dir, "desc") == 0 || strcmp(order_dir, "DESC") == 0));
        if (oc.count > 1)
            qsort(oc.rows, oc.count, sizeof(OrderedRow), desc ? cmp_row_desc : cmp_row_asc);
```

Replace with — directions now live inside the comparator (per part), so
the outer flip must go:

```c
        if (oc.count > 1)
            qsort(oc.rows, oc.count, sizeof(OrderedRow), cmp_row_asc);
```

If `desc` has no other uses in the buffered block (grep the enclosing
block to confirm), delete its declaration — otherwise leave it and note it
in `PLAN_NOTES.md`.

Anchor — row frees, budget-exceeded path (exact):

```c
                free(oc.rows[i].key); free(oc.rows[i].record); free(oc.rows[i].sort_str);
```

Replace with:

```c
                free(oc.rows[i].key); free(oc.rows[i].record);
                for (int pi = 0; pi < MAX_ORDER_FIELDS; pi++)
                    free(oc.rows[i].sort_str[pi]);
```

Anchor — row frees, success-path cleanup (exact):

```c
            free(oc.rows[i].sort_str);
```

Replace with:

```c
            for (int pi = 0; pi < MAX_ORDER_FIELDS; pi++)
                free(oc.rows[i].sort_str[pi]);
```

Verify no other `->sort_str` or `.sort_str` references remain
(`grep -n "sort_str" src/db/query.c` should show only the struct, the
callback, the comparator, and the frees above).

Build + run the new test (all green) plus the buffer-sort regression set:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-multi-order-cursor
./build/bin/shard-db-test run-all --filter order
```

---

### Task 6 — docs

**Test-first (verification step):** this task changes no C code — before
editing docs, run the full suite and confirm it is green after Task 5
(`./build/bin/shard-db-test run-all`); after the doc edits, re-run the
multi-field case and verify every find.md example in this plan matches the
implemented behavior verbatim (token shape, error strings, constraints
list).

**`docs/query-protocol/find.md`.** Anchor (exact):

```markdown
| `order_by` | string | — | Sort by this field; matches are buffered and sorted before pagination. |
```

Replace with:

```markdown
| `order_by` | string (CSV) or array of strings | — | Sort by one or more fields (`"a,b"` or `["a","b"]`, max 4, shared direction); matches are buffered and sorted before pagination. Multi-field cursor pagination requires the exact composite index (see [Cursor pagination](#cursor-pagination)). |
```

In the cursor section's constraint list, anchor (exact):

```markdown
- `order_by` field **must be indexed** — cursor queries reject otherwise with a clear error.
```

Replace with:

```markdown
- `order_by` field(s) **must be indexed** — single-field cursor requires that field's index; multi-field cursor requires the exact composite index `f1+f2+…+fN` and rejects varchar in any non-final part (fixed-width parts keep byte order equal to tuple order). Errors name the index to add.
- Per-field directions (`"a:asc","b:desc"`) are allowed; parts without a suffix inherit the shared `order`. Cursor pagination requires all fields to share one direction (the composite btree is a single byte order); mixed-direction queries fall back to the buffered sort (no cursor). More than 4 fields is rejected.
```

Add after the `total` example block (anchor: the line
`The `total` field contains the complete match count` paragraph — insert a
new subsection **after** the paragraph that ends `only applies when cursor pagination is active (`cursor:null` or `cursor:{...}`).`):

```markdown
### Multi-field cursor

```json
{"mode":"find","dir":"default","object":"orders","criteria":[],
 "order_by":["tenant","day"],"order":"asc","limit":100,"cursor":null}
{"rows":[…],"cursor":{"tenant":"7","day":"2026-09-01","key":"ord_4912"}}
```

Paging is by the tuple `(tenant, day, key)` — O(limit) at any depth.
Create the composite first: `add-index default orders tenant+day`. Without
a cursor field in the request, multi-field `order_by` still works via the
buffered sort (no index needed), including mixed per-field directions
(`["tenant:asc","day:desc"]`); the cursor form requires one shared
direction across all fields.
```

**`AGENTS.md`.** Anchor (exact):

```markdown
- `"cursor":null` (or `{}`) — opt into keyset cursor on find. Requires indexed `order_by`. Rejects `format:"csv"` and `join`. See [find.md](docs/query-protocol/find.md) for cursor protocol.
```

Replace with:

```markdown
- `"cursor":null` (or `{}`) — opt into keyset cursor on find. Requires indexed `order_by`; multi-field `order_by` (CSV/array, ≤4 fields, per-field `:asc/:desc` suffixes allowed) requires the exact composite index with fixed-width non-final parts, and one shared direction across fields (mixed directions fall back to the buffered sort without a cursor). Rejects `format:"csv"` and `join`. See [find.md](docs/query-protocol/find.md) for cursor protocol.
```

---

### Task 7 — verification

1. Full suite:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

2. Regression focus — must all pass:
`test-multi-order-cursor`, `test-find-cursor`, `test-cursor-with-total`,
`test-cursor-bitmap-intersect`, `test-cursor-sparse-prefetch`,
`test-d3-order-walk-executor`, `test-find-indexed-orderby`,
`test-find-orderby-selective`, `test-find-filter-first-orderby`
(individual `run <name>` for any that need isolating).

3. Standing dynamic-safety gate (AGENTS.md) — this touches read paths that
run under the bt/kf caches, so both sanitizers, three fresh runs each:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all   # ×3 fresh runs

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all   # ×3 fresh runs
```

No benches (operator-run only). Leave everything uncommitted for review.

## Edge cases & invariants (explicit)

- **Ties across page boundaries** (the core keyset property): rows sharing
  the full `(f1…fN)` tuple are ordered by hash16 within the btree run and
  tie-broken by `hash16(key)` in the cursor compare; every row appears
  exactly once across the walk regardless of where page boundaries fall.
  Asserted by Task 1 groups A/B (tie groups deliberately straddle the
  limit-5 boundary).
- **DESC** mirror-flips the whole concatenated compare + hash tiebreak —
  same mechanism as single-field today. **Mixed directions never reach the
  walk**: the cursor branch rejects them (uniformity check), and the
  buffer-sort comparator flips per part instead.
- **Per-field suffixes on a single field** (`"time:desc"`, no `order`
  given) resolve to the same walk/comparator behavior as
  `order:"desc"` — `ospec.dir[0]` merges the shared direction with the
  suffix (suffix wins).
- **offset + cursor** compose exactly as today (`skip_remaining` counts
  post-cursor rows); unchanged code.
- **Stale cursors** (deleted rows / old tokens) seek to the last-known byte
  position — unchanged keyset semantics; cursor contents are not validated
  against live data.
- **Single-field byte-compat**: n == 1 paths emit the identical token, use
  the identical validation error, and keep `order_walk_bounds` folding.
  Task 1 group G + the existing cursor suites guard this.
- **Legacy untyped objects**: single-field order_by keeps the
  `decode_field` fallback (idx = −1 path). Multi-field on such objects is
  rejected ("not in the typed schema").
- **`QUERY_BUFFER_MB` accounting**: row_bytes now sums all part strings
  (Task 5 `sv_bytes`), so the cap stays accurate.
- **Error-path memory**: every new allocation (`sv[]`, `last_value[]`) is
  freed on all paths — enumerated in Tasks 4h/5; the asan gate is the
  backstop.
- **Extra keys inside a cursor JSON object** remain ignored (existing
  behavior); a missing part value errors.
- **`order_by` under joins** is already dropped before both paths; cursor
  + join is hard-rejected upstream. No new interaction.

## Out of scope (future work, each its own plan)

- `order_walk_bounds` folding of order-by range/eq criteria leaves into
  multi-field walk bounds (perf only, not correctness).
- Serving multi-field order_by from a composite index in the *non-cursor*
  path (avoid the in-memory sort) — needs planner work.
- Mixed per-field directions *from an index*: needs per-column direction
  flags in the composite index format plus direction-inverted encodings
  (nontrivial for varlen parts, where the length tiebreak doesn't invert).
  Until then, mixed directions are served correctly by the buffer-sort
  fallback only.
- Length-prefixing varchar parts in composite keys to relax the
  fixed-width rule — index on-disk format change + reindex.
- Multi-field aggregate `order_by`.
