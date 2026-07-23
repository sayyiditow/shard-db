# Fix: `aggregate` `format:"csv"` silently returns JSON on the top-N streaming path

## Status update (2026-07-22): scope narrowed, false premise corrected

Execution surfaced a real defect while adding the indexed-`category` fixture required
by Task 1a: once `category` is indexed, **both** the pre-existing JSON assertion and the
new CSV assertion show corrupted output (JSON: double-escaped quote; CSV: backslash
preserved *and* doubled instead of RFC 4180 doubling alone). Root-caused to
`src/db/index.c`: `index_parallel` and `build_index_key_from_json` extract a field's
text straight from the request's raw JSON via `json_get_fields`/`json_obj_strdup`,
neither of which decodes JSON escapes (`\"`, `\\`, `\n`, `\uXXXX`) before handing the
text to `encode_field_for_index` (`config.c:1804`, a plain `memcpy` for `FT_VARCHAR`).
The **record** encoder does unescape first (`config.c:~2231`, via `json_unescape_string`
— a targeted prior fix, commit `797aadd`) but that fix was never mirrored onto index-key
construction. Result: a varchar btree/bitmap index entry can literally differ from the
record it points to whenever the value contains an escapable character.

This is **not** a defect in this plan's target code. `agg_run_topn_stream`'s escaping,
the general hash-table path's `csv_emit_cell`/`json_escape_const` calls, and
`csv_emit_cell` itself are all correct given their inputs — confirmed by reverting just
the Task 1a index addition, which makes both assertions pass unchanged. The corruption
is entirely upstream, in what the index itself stores. Three other aggregate code paths
this plan's "Call-site inventory" didn't mention — the streaming top-N walk (this plan's
subject), the "IGB" indexed-group-by fast path (`query_aggregate.c:~4671`), and the "VS"
index-walk fast path (`query_aggregate.c:~4452`) — all read the group value straight from
the (corrupted) index instead of the record, which is why indexing `category` is what
exposes the bug. It was also confirmed, via a throwaway repro (not committed), that this
corruption is a genuine query-correctness bug, not just a display one: `find` with an
equality criterion on an indexed varchar field containing an escaped quote returns `[]`
even though the record exists (`get` by key returns it correctly) — the record is
invisible to indexed lookups.

**The false premise this plan needs corrected:** the "Design decision" section below
claims the general hash-table group-by path "already handles ... CSV emission ...
correctly and completely." That's true of its emission logic in isolation, but the
premise as stated implied there was nothing left to fix once this plan's eligibility
gate routed CSV there — false whenever `category` (or any group_by field) is indexed,
because the corruption lives upstream of emission, in the index itself, and this plan's
own Task 1a is what puts an index on the test fixture.

**Fix for the root cause is out of scope for this plan** — see
`docs/plans/2026-07-22-index-key-json-unescape.md`. That plan's Task 1 regression tests
subsume this plan's Task 1 assertions (both the pre-existing JSON one and the new CSV
one) — once it lands, this plan's Task 1 checkpoint should pass with no further changes
to `query_aggregate.c`. **This plan's own fix (Task 2: disqualify CSV from top-N
eligibility) is real, independent of the index bug, and should still land** — it's
correct regardless of whether the index-escape bug is fixed, since a JSON-only streaming
executor should never have been eligible for a CSV request in the first place. Its Task
1 checkpoint is now gated on the other plan landing first (or on temporarily using a
group_by value with no escapable characters to isolate this plan's own regression, if
executed out of order).

> **Execution rules:** branch off `main` (fresh feature branch, do not work on `main`); do
> tasks in order; leave everything **uncommitted** for review; locate edits by searching the
> quoted anchor text (line numbers drift); build `SKIP_TESTS=1 ./build.sh`, test
> `./build/bin/shard-db-test run <name>` for the affected case(s) and
> `./build/bin/shard-db-test run-all` for the full suite before calling this plan done; never
> claim a step passed without pasting the real output; if a referenced anchor/symbol isn't
> where this plan says, STOP and write `PLAN_NOTES.md` describing what you found instead of
> guessing or reinterpreting.

Source: `docs/plans/2026-07-16-storage-durability-and-recovery-findings.md`, Finding 10
(discovered while investigating Finding 6's originally-claimed test gap; see that finding's
entry for how it surfaced and Finding 6's updated entry for why Finding 6 itself needed no
code change — its residual CSV-escaping concern is closed by this plan's regression test).

## Root cause

`cmd_aggregate_do` (`src/db/query_aggregate.c:3315-3323`) computes `csv_delim` from the
caller's `format`/`delimiter` up front, then later (`query_aggregate.c:3643-3679`) gates a
performance shortcut — the streaming top-N executor — behind `eligible_for_topn_stream`
(`query_aggregate.c:478-516`). Neither `eligible_for_topn_stream`'s signature nor
`agg_run_topn_stream`'s (`query_aggregate.c:663-` through its JSON-only drain loop ending
around line 870) carries `format`/`delimiter` at all. The eligibility gate was written to
disqualify shapes the streaming executor can't handle correctly (multi-field group_by,
`having`, non-count/non-group-field aggregates, etc.) but was never extended to also
disqualify "caller asked for CSV" — because the executor itself has no CSV output branch,
any admitted request silently gets JSON regardless of what `format` said.

**Reproduced** (daemon mode, single indexed varchar `group_by` field, `count()` aggregate,
`order_by` on that count's alias, `limit:10`, no `having` — exactly
`eligible_for_topn_stream`'s admission criteria):

```
--- JSON topn-eligible aggregate ---
[{"cat":"grp0","c":2},{"cat":"grp1","c":3}]
--- CSV topn-eligible aggregate (same shape, "format":"csv" added) ---
[{"cat":"grp0","c":2},{"cat":"grp1","c":3}]
```

Identical output. No error is raised; a client parsing the response as CSV would either
fail to parse or misinterpret the JSON syntax as CSV field content.

## Design decision

Two fix shapes were possible (see Finding 10's write-up for both): disqualify CSV from
top-N eligibility (minimal, falls back to the already-correct general hash-table path,
loses the streaming perf shortcut only for this one combination), or teach the streaming
executor to emit CSV itself (preserves the shortcut for CSV callers, larger diff, new
surface to keep in sync with the general path's CSV formatting rules).

**This plan picks disqualification.** The general hash-table group-by path
(`query_aggregate.c:~5730-5765`) already handles `order_by` (via `qsort` + `agg_sort_cmp`),
`limit`, and CSV emission (via `csv_emit_cell`, including correct RFC 4180 quote-doubling)
correctly and completely — confirmed by reading its sort/limit/CSV-output block in full.
Falling back to it for CSV requests costs only the streaming shortcut's performance
advantage on this one narrow, already-uncommon-relative-to-JSON combination (CSV + single
indexed group_by + order_by-on-alias + limit), not correctness. Teaching the streaming
executor CSV output would duplicate `csv_emit_cell`'s quoting logic in a second location
that must be kept in sync — not justified for what field data suggests is a much
less-common request shape (CSV output specifically for a top-N-shaped query) than the
general one this already serves correctly.

`eligible_for_topn_stream` gains a `format` parameter and returns 0 whenever `format &&
strcmp(format, "csv") == 0` — architecturally identical to its existing disqualifying
checks (`having`, multi-field group_by, limit bounds).

## Call-site inventory

`eligible_for_topn_stream` has exactly two call sites in the repo (confirmed via grep):

1. **Production**: `query_aggregate.c:3662-3664`, inside `cmd_aggregate_do` (which already
   has `format` in scope as a parameter — no new plumbing needed to reach it).
2. **Test-only, direct unit calls**: `src/test/cases/test_agg_topn_stream.c`, an `extern`
   declaration at lines 35-41 plus 8 call sites (Cases A through H) in
   `test_topn_eligible_truth_table` (lines 163-208), each currently passing 8 positional
   arguments ending in `having`.

`agg_run_topn_stream` itself is NOT changed (no signature/behavior change — this plan's fix
lives entirely in the eligibility gate, per the design decision above), so its own call
site (`query_aggregate.c:3666-3670`) needs no edit.

## Task 1 — Test-first: reproduce the bug as a failing end-to-end regression test

### 1a. Correction (2026-07-22): do not index the existing quote-value object — use a second, dedicated object instead

The executor's original 1a indexed `category` directly on the pre-existing `agg`/`t`
object (the one whose fixture value is `He said "hi"`). That exposed the unrelated
index-unescape bug described in the Status update above, which corrupts *any* indexed
varchar value containing a JSON-escapable character (quote, backslash, etc.) —
breaking the two pre-existing, unmodified assertions "agg group_by: quote escaped" and
"agg top-N: quote escaped" as a side effect, since those now route through
index-backed paths (IGB / top-N stream) instead of the record-reading general path.

**Do not index `agg`/`t`.** Revert that part of 1a so `agg`/`t`'s `create-object` call
is byte-for-byte what's on `main` (no `"indexes"` key) — this restores the two
pre-existing assertions to passing, since they'll go back to the general hash-table
path that reads uncorrupted record bytes.

Instead, add a **second, new** object dedicated to this plan's own regression, indexed,
but populated with values that need CSV quoting (RFC 4180) without needing JSON
escaping — i.e. a value containing a **comma**, not a quote or backslash. This fully
decouples Task 2's own verification (does `format:"csv"` correctly disqualify the
streaming top-N path?) from the index-unescape bug, so this plan can land and pass its
own checkpoint independently, per the user's decision to keep this plan CSV-only.

File: `src/test/cases/test_json_escape.c`, function `test_json_escape_agg_file_run`

Anchor (exact current text — this is what 1a actually produced; revert the `"indexes"` line):

```c
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"agg\",\"object\":\"t\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\"],"
        "\"indexes\":[\"category\"]}", &resp);
    free(resp); resp = NULL;
```

Replace with (back to `main`'s shape — no index on `t`):

```c
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"agg\",\"object\":\"t\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;
```

### 1b. End-to-end wire-level regression, on a new dedicated object

Anchor (exact current text — the CSV assertion block 1b actually added, keyed off the
now-reverted indexed `t`):

```c
    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"agg\",\"object\":\"t\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"category\"],\"order_by\":\"n\",\"limit\":10,"
        "\"format\":\"csv\"}", &resp);
    ASSERT_TRUE(resp[0] != '[' && resp[0] != '{',
                "agg top-N + format=csv: response is not JSON");
    ASSERT_CONTAINS(resp, "category,n", "agg top-N + format=csv: CSV header row present");
    ASSERT_CONTAINS(resp, "\"\"hi\"\"", "agg top-N + format=csv: quote doubled per RFC4180");
    ASSERT_CONTAINS(resp, "Plain,", "agg top-N + format=csv: plain value present");
    free(resp); resp = NULL;

    /* ── valid_filename rejects embedded quotes ── */
```

Replace with (new object `tcsv`, comma-bearing value instead of quote-bearing):

```c
    /* ── Finding 10 regression, on its own indexed object: format:"csv"
       against an indexed group_by + order_by + limit must return real CSV,
       not JSON. Before the fix, eligible_for_topn_stream had no idea "csv"
       was requested and admitted the request into the JSON-only streaming
       executor anyway, silently returning JSON. Uses a comma-bearing value
       (needs RFC 4180 quoting) rather than a quote-bearing one deliberately:
       a literal quote in the value requires JSON-escaping to transmit over
       the wire, which hits the separate index varchar unescape bug tracked
       in docs/plans/2026-07-22-index-key-json-unescape.md. Keeping this
       plan's own regression comma-only keeps it independent of that bug. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"agg\",\"object\":\"tcsv\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\"],"
        "\"indexes\":[\"category\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"agg\",\"object\":\"tcsv\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Widgets, Inc.\"}}",
        &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"agg\",\"object\":\"tcsv\","
        "\"key\":\"k2\",\"value\":{\"category\":\"Plain\"}}",
        &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"aggregate\",\"dir\":\"agg\",\"object\":\"tcsv\","
        "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"n\"}],"
        "\"group_by\":[\"category\"],\"order_by\":\"n\",\"limit\":10,"
        "\"format\":\"csv\"}", &resp);
    ASSERT_TRUE(resp[0] != '[' && resp[0] != '{',
                "agg top-N + format=csv: response is not JSON");
    ASSERT_CONTAINS(resp, "category,n", "agg top-N + format=csv: CSV header row present");
    ASSERT_CONTAINS(resp, "\"Widgets, Inc.\",",
                    "agg top-N + format=csv: comma value quoted per RFC4180");
    ASSERT_CONTAINS(resp, "Plain,", "agg top-N + format=csv: plain value present");
    free(resp); resp = NULL;

    /* ── valid_filename rejects embedded quotes ── */
```

### Verification (before implementing Task 2)

Run the new assertions and confirm they fail for the expected reason:

- `./build/bin/shard-db-test run test-json-escape-agg-file` → with `category` indexed on
  the new `tcsv` object, this request is admitted into the streaming top-N path, so the
  new "agg top-N + format=csv" assertions must fail on the unfixed code (response is
  JSON, `resp[0] == '['`, no CSV header present). The two pre-existing quote-escaping
  assertions on `t` (now unindexed again) must still pass unchanged — if they don't,
  something other than this plan's change broke them and execution should stop. Paste
  the failing (and passing) output before proceeding to Task 2.

This is the plan's required pre-fix red test. The direct unit-level truth-table case (Case I,
below) is added together with the implementation in Task 2, not as a separate pre-fix step —
see that section for why.

## Task 2 — Implement the fix

Both parts below (2a: the eligibility-gate change and its production call site; 2b: the test
extern/call-sites and new Case I) touch `eligible_for_topn_stream`'s signature and must land
as a single change — the test file and `query_aggregate.c` are separate translation units, so
changing one without the other does not reliably produce a build/link error (the old 8-argument
implementation would simply ignore a trailing ninth argument passed by a stale-but-technically-
mismatched caller on common ABIs). Do not rely on a compile failure as a checkpoint; apply
2a and 2b together, then build once.

### 2a. Direct unit-level regression (extends the eligibility truth table)

File: `src/test/cases/test_agg_topn_stream.c`

Anchor (exact current text — the `extern` declaration):

```c
extern int eligible_for_topn_stream(
    const char *db_root, const char *object,
    const TestAggSpec *specs, int nspecs,
    const char *group_by_csv,
    const char *order_by_alias,
    int limit,
    const char *having);
```

Replace with:

```c
extern int eligible_for_topn_stream(
    const char *db_root, const char *object,
    const TestAggSpec *specs, int nspecs,
    const char *group_by_csv,
    const char *order_by_alias,
    int limit,
    const char *having,
    const char *format);
```

Anchor (exact current text — every one of the 8 existing call sites, Cases A–H, each needs
its new trailing argument; showing the full block for unambiguous replacement):

```c
    /* Case A: minimal eligible shape - count() + single-field group_by + limit */
    TestAggSpec spec_a;
    spec_a.fn = AGG_COUNT_VAL;
    spec_a.field[0] = '\0';
    strcpy(spec_a.alias, "n");
    int r = eligible_for_topn_stream(env.db_root, obj_path,
                                      &spec_a, 1, "name", "n", 20, NULL);
    ASSERT_EQ_INT(r, 1, "Case A: count + indexed group_by + order_by agg alias + limit → ELIGIBLE");

    /* Case B: no limit → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "n", 0, NULL);
    ASSERT_EQ_INT(r, 0, "Case B: no limit (0) → NOT ELIGIBLE");

    /* Case C: order_by on group_by field instead of agg alias → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "name", 20, NULL);
    ASSERT_EQ_INT(r, 0, "Case C: order_by on group_by field → NOT ELIGIBLE");

    /* Case D: group_by on un-indexed field → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "score_unindexed", "n", 20, NULL);
    ASSERT_EQ_INT(r, 0, "Case D: un-indexed group_by → NOT ELIGIBLE");

    /* Case E: multi-field group_by → NOT ELIGIBLE in Phase 1 */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name,score", "n", 20, NULL);
    ASSERT_EQ_INT(r, 0, "Case E: multi-field group_by (Phase 1 limit) → NOT ELIGIBLE");

    /* Case F: sum on group_by field itself + order_by on sum alias → ELIGIBLE */
    TestAggSpec spec_f;
    spec_f.fn = AGG_SUM_VAL;
    strcpy(spec_f.field, "score");  /* sum on the group_by field itself */
    strcpy(spec_f.alias, "total");
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_f, 1, "score", "total", 10000, NULL);
    ASSERT_EQ_INT(r, 1, "Case F: sum on group_by field + limit=max → ELIGIBLE");

    /* Case G: limit > 10000 → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "n", 10001, NULL);
    ASSERT_EQ_INT(r, 0, "Case G: limit > 10000 → NOT ELIGIBLE");

    /* Case H: sum on different field → NOT ELIGIBLE in Phase 1 */
    TestAggSpec spec_h;
    spec_h.fn = AGG_SUM_VAL;
    strcpy(spec_h.field, "score_unindexed");  /* NOT the group_by field */
    strcpy(spec_h.alias, "total");
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_h, 1, "name", "total", 10000, NULL);
    ASSERT_EQ_INT(r, 0, "Case H: agg on non-group_by field (Phase 1 limit) → NOT ELIGIBLE");

    if (tu_pdb_drop_object(tc, "default", "topn_elig") != 0) return 1;
    return 0;
}
```

Replace with (each of the 8 calls gains a trailing `NULL` — "no format specified" — to
preserve its exact prior behavior/assertion, plus one new Case I proving the fix):

```c
    /* Case A: minimal eligible shape - count() + single-field group_by + limit */
    TestAggSpec spec_a;
    spec_a.fn = AGG_COUNT_VAL;
    spec_a.field[0] = '\0';
    strcpy(spec_a.alias, "n");
    int r = eligible_for_topn_stream(env.db_root, obj_path,
                                      &spec_a, 1, "name", "n", 20, NULL, NULL);
    ASSERT_EQ_INT(r, 1, "Case A: count + indexed group_by + order_by agg alias + limit → ELIGIBLE");

    /* Case B: no limit → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "n", 0, NULL, NULL);
    ASSERT_EQ_INT(r, 0, "Case B: no limit (0) → NOT ELIGIBLE");

    /* Case C: order_by on group_by field instead of agg alias → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "name", 20, NULL, NULL);
    ASSERT_EQ_INT(r, 0, "Case C: order_by on group_by field → NOT ELIGIBLE");

    /* Case D: group_by on un-indexed field → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "score_unindexed", "n", 20, NULL, NULL);
    ASSERT_EQ_INT(r, 0, "Case D: un-indexed group_by → NOT ELIGIBLE");

    /* Case E: multi-field group_by → NOT ELIGIBLE in Phase 1 */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name,score", "n", 20, NULL, NULL);
    ASSERT_EQ_INT(r, 0, "Case E: multi-field group_by (Phase 1 limit) → NOT ELIGIBLE");

    /* Case F: sum on group_by field itself + order_by on sum alias → ELIGIBLE */
    TestAggSpec spec_f;
    spec_f.fn = AGG_SUM_VAL;
    strcpy(spec_f.field, "score");  /* sum on the group_by field itself */
    strcpy(spec_f.alias, "total");
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_f, 1, "score", "total", 10000, NULL, NULL);
    ASSERT_EQ_INT(r, 1, "Case F: sum on group_by field + limit=max → ELIGIBLE");

    /* Case G: limit > 10000 → NOT ELIGIBLE */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "n", 10001, NULL, NULL);
    ASSERT_EQ_INT(r, 0, "Case G: limit > 10000 → NOT ELIGIBLE");

    /* Case H: sum on different field → NOT ELIGIBLE in Phase 1 */
    TestAggSpec spec_h;
    spec_h.fn = AGG_SUM_VAL;
    strcpy(spec_h.field, "score_unindexed");  /* NOT the group_by field */
    strcpy(spec_h.alias, "total");
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_h, 1, "name", "total", 10000, NULL, NULL);
    ASSERT_EQ_INT(r, 0, "Case H: agg on non-group_by field (Phase 1 limit) → NOT ELIGIBLE");

    /* Case I (Finding 10 regression): otherwise-eligible shape (identical
       to Case A) but format="csv" → NOT ELIGIBLE. Before the fix,
       eligible_for_topn_stream ignored format entirely and this returned
       1 — the production bug this plan fixes (see cmd_aggregate_do's
       call site, which then fed an admitted CSV request into the
       JSON-only streaming executor). */
    r = eligible_for_topn_stream(env.db_root, obj_path,
                                  &spec_a, 1, "name", "n", 20, NULL, "csv");
    ASSERT_EQ_INT(r, 0, "Case I: format=csv on otherwise-eligible shape → NOT ELIGIBLE");

    if (tu_pdb_drop_object(tc, "default", "topn_elig") != 0) return 1;
    return 0;
}
```

### 2b. Implement the eligibility-gate change

File: `src/db/query_aggregate.c`

Anchor (exact current text — function signature and top of body):

```c
TOPN_ELIG_VIS int eligible_for_topn_stream(
    const char *db_root, const char *object,
    const AggSpec *specs, int nspecs,
    const char *group_by_csv,
    const char *order_by_alias,
    int limit,
    const char *having)
{
    if (!specs || nspecs <= 0) return 0;
    if (!group_by_csv || !*group_by_csv) return 0;
    if (!order_by_alias || !*order_by_alias) return 0;
    if (limit <= 0 || limit > 10000) return 0;
    if (having && *having) return 0;
```

Replace with:

```c
TOPN_ELIG_VIS int eligible_for_topn_stream(
    const char *db_root, const char *object,
    const AggSpec *specs, int nspecs,
    const char *group_by_csv,
    const char *order_by_alias,
    int limit,
    const char *having,
    const char *format)
{
    if (!specs || nspecs <= 0) return 0;
    if (!group_by_csv || !*group_by_csv) return 0;
    if (!order_by_alias || !*order_by_alias) return 0;
    if (limit <= 0 || limit > 10000) return 0;
    if (having && *having) return 0;
    /* The streaming executor (agg_run_topn_stream) only knows how to emit
       JSON. A CSV request must fall back to the general hash-table
       group-by path, which already emits correct CSV (including RFC 4180
       quote-doubling via csv_emit_cell) for this same query shape. */
    if (format && strcmp(format, "csv") == 0) return 0;
```

Anchor (exact current text — the production call site):

```c
        if (gb_csv[0] && order_by && order_by[0] &&
            eligible_for_topn_stream(db_root, object, specs, nspecs,
                                      gb_csv, order_by, limit,
                                      having_tree ? "1" : having_json)) {
```

Replace with:

```c
        if (gb_csv[0] && order_by && order_by[0] &&
            eligible_for_topn_stream(db_root, object, specs, nspecs,
                                      gb_csv, order_by, limit,
                                      having_tree ? "1" : having_json,
                                      format)) {
```

### Verification (after implementing Task 2)

- `./build/bin/shard-db-test run test-topn-eligible-truth-table` → all 9 cases (A–I) pass.
- `./build/bin/shard-db-test run test-json-escape-agg-file` → all assertions, including the
  new CSV ones, pass.
- Full suite: `./build/bin/shard-db-test run-all` → no regressions, in particular
  `test-agg-topn-stream`'s other cases (correctness of the streaming executor itself is
  untouched by this fix — only the eligibility gate changed) and any existing CSV-format
  aggregate tests (e.g. `test_agg_varchar_groupby_sum.c` / `test_agg_varchar_groupby_limit.c`
  if either exercises `format:"csv"` — confirm they still pass unchanged since their query
  shapes, if not exactly top-N-eligible, are unaffected by this change; if either happens to
  BE top-N-eligible and asserts CSV output, this fix is what makes them newly correct rather
  than what breaks them).

## Edge cases / invariants

- **`format` is `NULL` or absent** (the common case — bare JSON array response): behavior is
  completely unchanged; `format && strcmp(...)` short-circuits false on a NULL format,
  identical to every other `format && strcmp(format, "csv") == 0` guard already used
  elsewhere in this same file (e.g. `cmd_aggregate_do`'s own `csv_delim` computation one
  line above the call site being changed) — this plan's new check is written in the exact
  same idiom already established in this file, not a new pattern.
- **`format:"dict"`**: not disqualified by this change (only `"csv"` is checked) — the
  streaming executor's output is a JSON array either way, and `dict` format for aggregate
  responses is not documented/supported today regardless (confirmed: `docs/query-protocol/aggregate.md`
  has no `format:"dict"` mention for aggregate, unlike find/fetch) — out of scope, no
  behavior to preserve or break here.
- **CSV + top-N-eligible shape now takes the general path's cost profile** (full scan +
  hashmap + sort, not the streaming index walk) — a performance regression relative to what
  *would* happen if CSV had been silently (and incorrectly) fast-pathed before, but there is
  no correct fast path to compare against: the "before" state simply produced wrong output
  quickly. Not a regression in any meaningful sense — it's the first time this combination
  produces correct output at all.
- **`having_tree ? "1" : having_json`** (the existing having-disqualification argument,
  unchanged by this fix): still evaluated and passed exactly as before; this plan only adds
  a new trailing argument, not modifies existing ones.

## Documentation sync

None required — the top-N streaming executor is an internal, undocumented optimization
(no user-facing doc mentions it, confirmed via grep of `docs/query-protocol/aggregate.md`);
this fix changes only which internal path a request takes, not any documented behavior,
flag, or response shape (`format:"csv"` already promises CSV output for aggregate requests
per the existing docs — this fix makes that promise actually hold for a case where it
previously didn't).
