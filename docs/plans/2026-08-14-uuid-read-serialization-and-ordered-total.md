# UUID read serialization and ordered-find totals

## Goal

Fix read responses for UUID auto-key objects so every reachable `fetch`,
ordered `find`, and cursor-find emitter returns the dashed UUID wire key rather
than embedding its 16 storage bytes in JSON.  Make `find` with `total:true`
return an exact numeric total for the customers pagination shapes reported from
the live application: no criteria, and a single `icontains` criterion ordered
by indexed `name`.

The wire contract is unchanged: an `AK_UUID` primary key is always the existing
36-character dashed UUID string.  This work changes only incorrect emitters
that were bypassing that contract.  There is no storage migration, index
rebuild, configuration change, or client migration.

## Confirmed mechanisms

1. `format_wire_key()` is the established storage-to-wire conversion and
   handles `AK_UUID`, `AK_SEQ`, and ordinary string keys.  The v2 fetch callback
   currently calls the generic record emitters only for `rows`, but emits its
   default and dict keys with `%.*s` from raw storage bytes; its CSV branch does
   the same.  A UUID is binary, so control bytes, quote bytes, and backslashes
   can reach a JSON string unescaped or stop it at NUL.
2. The ordered fallback preserves raw primary-key bytes in `OrderedRow.key` and
   later interpolates them with `%s`.  `cursor_find_cb` similarly copies
   `RecordRef.key` into `key_buf` and interpolates it for rows/dict/default
   response forms and the next cursor.  Those paths bypass
   `format_wire_key()` even though their `FieldSchema` contains the auto-key
   schema snapshot.
3. Exact totals are selected per execution route.  The general indexed
   order-walk path has an unconditional `"total":null` close even when the
   criteria tree is absent, despite `get_live_count()` being an authoritative
   O(1) exact total.  The repository has `fp_compute_total()` for filter plans,
   but the reported `icontains` shape must first be characterized on the base
   branch: it may be an exact `FP_PRIMARY_LEAF` count already, or it may take a
   fallback whose closing branch loses that result.  Do not guess which branch
   production's index mix selects.

## Required call-site audit

The executor must re-run this audit before changing a helper signature or wire
format.  Existing producers/consumers are:

| Producer family | Current key conversion | Consumer |
| --- | --- | --- |
| `v2_fetch_cb` default/dict/CSV | raw bytes in some formats | fetch JSON/CSV clients |
| `print_record_json`, `print_record_dict`, `print_record_row` | `render_wire_key` | fetch rows and other scan callers |
| `ordered_collect_cb` plus ordered fallback emission | raw `OrderedRow.key` | ordered find JSON/CSV clients |
| `cursor_find_cb`, including its three in-memory call sites | raw `RecordRef.key` | ordered find and cursor JSON clients |
| stream/keyset/composite emitters | `format_wire_key` at their emit boundary | ordinary find clients |
| `format_wire_key` | identity / dashed UUID / decimal sequence | all read/write response producers |

Known external consumers are the live JavaScript client, which parses each
newline-delimited JSON response, and every existing CLI/wire client.  Preserve
the JSON shapes, field names, order, cursor object schema, and CSV key column.

## Execution guardrails

1. Branch from the current default branch as `fix/uuid-read-keys-total`.
2. Execute tasks in order and leave all changes uncommitted for raw-diff review.
3. Every quoted anchor below must match exactly.  If any does not, write
   `PLAN_NOTES.md` explaining the mismatch and halt the entire execution run;
   do not reinterpret the plan or continue.  Resumption requires a human (or
   the planning model re-engaged) to decide whether the anchor is stale or the
   assumption is wrong and to supply a patched/fresh plan.
4. If an uncovered design decision appears, stop and ask the human; do not
   improvise.
5. Build with `SKIP_TESTS=1 ./build.sh`.  Run focused cases with
   `./build/bin/shard-db-test run <name>`, then the full suite with
   `./build/bin/shard-db-test run-all`.  This change touches response handling
   only: it does not change locks, shared/cached state, object lifetimes, or
   background threads, so the repo's ASan/TSan gate is not triggered.

## Task 1 — add base-branch reproductions and identify the total route

**Root-cause status:** the binary-key root cause is established above.  This
test also records the exact planner route for the `icontains` shape.  The
implementation in Task 3 deliberately uses the existing route-independent
`fp_compute_total()` contract, so it does not need to guess that route.

**Test first.** At the end of `test_auto_key_run`, before the existing cleanup
anchor `"    free(uuid_alice); free(uuid_bob);"`, create a dedicated UUID
customer object with a btree `name` index, insert deterministic supplied UUIDs
whose binary bytes include JSON-hostile control bytes, and issue fetch,
ordered-find, and cursor-find requests.  Add the following complete block:

```c
    /* === UUID read emitters: fetch, ordered find, cursor ================ */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"customers\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:32\"],"
        "\"indexes\":[\"name\"],\"auto_key\":\"uuid\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "uuid customers created");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"customers\",\"records\":["
        "{\"key\":\"11111111-2222-4333-8444-555566667777\",\"value\":{\"name\":\"Scap Alpha\"}},"
        "{\"key\":\"22222222-3333-4444-8555-666677778888\",\"value\":{\"name\":\"scap Beta\"}},"
        "{\"key\":\"33333333-4444-4555-8666-777788889999\",\"value\":{\"name\":\"Other\"}}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"count\":3", "uuid customers seeded");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"fetch\",\"dir\":\"default\",\"object\":\"customers\",\"limit\":25}",
        &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"11111111-2222-4333-8444-555566667777\"",
                    "fetch renders UUID wire key");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"customers\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"icontains\",\"value\":\"scap\"}],"
        "\"order_by\":\"name\",\"order\":\"asc\",\"limit\":25}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"11111111-2222-4333-8444-555566667777\"",
                    "ordered find renders UUID wire key");
    ASSERT_CONTAINS(resp, "\"key\":\"22222222-3333-4444-8555-666677778888\"",
                    "ordered find keeps all matching UUID keys");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"customers\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"icontains\",\"value\":\"scap\"}],"
        "\"order_by\":\"name\",\"order\":\"asc\",\"limit\":1,\"cursor\":null}",
        &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"11111111-2222-4333-8444-555566667777\"",
                    "cursor page renders UUID wire key");
    ASSERT_CONTAINS(resp, "\"cursor\":{\"name\":\"Scap Alpha\","
                          "\"key\":\"11111111-2222-4333-8444-555566667777\"}",
                    "cursor stores UUID wire key");
    free(resp); resp = NULL;
```

In `test_find_with_total.c`, use the existing helper and total assertions to
add the reported `customers` equivalents: `total:true` with no criteria and
with `name icontains "scap"`, both ordered by indexed `name`.  The test must
assert `3` and `2` respectively, never merely assert that `total` is present.
Locate the insertion by the section comment `"/* D3: order-index walk"` and
add this complete test function plus its registration:

```c
static int test_find_total_ordered_customers(void) {
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
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"customers\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:32\"],"
        "\"indexes\":[\"name\"],\"auto_key\":\"uuid\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"customers\",\"records\":["
        "{\"key\":\"11111111-2222-4333-8444-555566667777\",\"value\":{\"name\":\"Scap Alpha\"}},"
        "{\"key\":\"22222222-3333-4444-8555-666677778888\",\"value\":{\"name\":\"scap Beta\"}},"
        "{\"key\":\"33333333-4444-4555-8666-777788889999\",\"value\":{\"name\":\"Other\"}}]}",
        &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"customers\","
        "\"order_by\":\"name\",\"order\":\"asc\",\"limit\":1,\"total\":true}", &resp);
    ASSERT_EQ_INT(extract_total(resp), 3, "ordered UUID find without criteria has exact total");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"customers\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"icontains\",\"value\":\"scap\"}],"
        "\"order_by\":\"name\",\"order\":\"asc\",\"limit\":1,\"total\":true}", &resp);
    ASSERT_EQ_INT(extract_total(resp), 2, "ordered icontains UUID find has exact total");
    free(resp);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-find-total-ordered-customers", test_find_total_ordered_customers)
```

Run the two new/changed cases on the unmodified base branch.  Paste their
failing output in the execution record.  Also issue `mode:"explain"` (or use
the test-only plan hook if that is what the existing test uses) for the
`icontains` request and record the `FilterPlan.kind`, `FilterPlan.order`,
`n_source`, and `n_postfilter`.  If the `icontains` reproduction already passes
on the base branch, record that the installed 1.0.16 binary is older/different;
retain the regression test, and make no unrelated planner change.

## Task 2 — route every UUID read emitter through the established formatter

**Root cause:** the affected code interpolates binary storage keys directly;
the existing formatter is correct but is skipped by the v2 fetch, ordered
fallback, and cursor emitters.

First preserve Task 1's red tests.  Then add this helper in `query.c` directly
before the anchor `"static int ordered_collect_cb("` so all local ordered
paths use a single conversion rule:

```c
static void format_find_wire_key(const FieldSchema *fs, const uint8_t *key,
                                 size_t klen, char out[1100]) {
    const Schema *sc = (fs && fs->auto_key != AK_NONE)
                     ? &fs->auto_key_schema_snapshot : NULL;
    format_wire_key(sc, (const char *)key, klen, out, 1100);
}
```

In `cursor_find_cb`, replace the complete raw-copy block anchored by
`"    /* Emit row. Supports json-default and rows_fmt. */"` with:

```c
    /* Emit using the primary key's protocol representation, never its binary
       storage bytes.  This is also the key stored in the next cursor. */
    char key_buf[1100];
    format_find_wire_key(c->fs, key_start, rr.klen, key_buf);
    size_t klen = strlen(key_buf);
```

Keep every existing `OUT()` shape, but make its `key_buf` and
`last_key_str = strndup(key_buf, klen)` use that wire string.  Do not change
cursor hash derivation: it remains derived from the parsed wire UUID before
the walk, and the stable btree tie-break remains `hash16`.

In the ordered fallback's post-sort loop, directly after the anchor
`"            OrderedRow *r = &oc.rows[i];"`, add:

```c
            char wire_key[1100];
            format_find_wire_key(&driver_fs, (const uint8_t *)r->key,
                                 r->key_len, wire_key);
```

Replace only the row-key arguments in that loop: `csv_emit_row(r->key, ...)`
becomes `csv_emit_row(wire_key, ...)`, and every `r->key` used in a JSON key
position becomes `wire_key`.  `OrderedRow.key` must stay binary/length-aware
for storage and cleanup; do not switch it to a string allocation or use
`strlen` on it.

In `v2_fetch_cb`, immediately before the `if (ctx->csv_delim)` anchor, add:

```c
    char wire_key[1100];
    render_wire_key(ctx->fs, block, hdr.key_len, wire_key);
```

Use `wire_key` in all of the following complete branches, leaving value
decoding, pagination, and response envelopes unchanged:

```c
    if (ctx->csv_delim) {
        csv_emit_row(wire_key, block + klen, (uint32_t)vlen,
                     ctx->proj_count > 0 ? ctx->proj_fields : NULL,
                     ctx->proj_count, ctx->fs, (char)ctx->csv_delim);
        ctx->printed++;
    } else if (ctx->rows_fmt) {
        print_record_row(&hdr, block, ctx->proj_fields, ctx->proj_count,
                         &ctx->printed, ctx->fs);
    } else if (ctx->dict_fmt) {
        OUT("%s\"%s\":", ctx->printed ? "," : "", wire_key);
        char *decoded = ctx->fs ? typed_decode(ctx->fs->ts,
                                                (const uint8_t *)value, vlen) : NULL;
        OUT("%s", decoded ? decoded : "null");
        free(decoded);
        ctx->printed++;
    } else {
        OUT("%s{\"key\":\"%s\",\"value\":", ctx->printed ? "," : "", wire_key);
        char *decoded = ctx->fs ? typed_decode(ctx->fs->ts,
                                                (const uint8_t *)value, vlen) : NULL;
        OUT("%s}", decoded ? decoded : "null");
        free(decoded);
        ctx->printed++;
    }
```

This explicitly covers default, dict, rows, and CSV fetch formats; `rows`
already routes through `print_record_row`, which formats keys itself.  It also
preserves `AK_NONE` verbatim and `AK_SEQ` decimal behavior through the same
formatter.

Run the Task 1 auto-key case.  To prove it is a regression test, temporarily
revert only the formatter calls in the three affected emitters, run the case,
and paste the expected failing UUID-key assertions; re-apply the calls and
paste the passing output.  Then run the relevant existing cursor/fetch cases:

```sh
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-auto-key
./build/bin/shard-db-test run test-find-cursor
./build/bin/shard-db-test run test-find-filter-first-orderby
```

## Task 3 — make the reported ordered totals exact without weakening other `null` cases

**Root cause:** an exact total exists for no criteria (`find_N_live`) and for
the existing planner-supported indexed routes (`fp_compute_total()`), but the
general indexed ordered-walk close unconditionally serializes `null` instead
of carrying either result through its envelope.

Test first: keep both Task 1 total assertions red while implementing this
task.  Immediately after the `btree_idx_walk_ordered(...)` block and before
the ordered-walk close anchored by:

```c
            if (dict_fmt)
                OUT(want_total ? "},\"total\":null}\n" : "}\n");
```

add this complete block.  It is evaluated while `prefilter_ks` is still owned
by this path, so `fp_compute_total()` may borrow it rather than rebuild an
indexed candidate set:

```c
            size_t ordered_total = 0;
            int ordered_total_null = 1;
            if (want_total) {
                if (!tree) {
                    ordered_total = find_N_live;
                    ordered_total_null = 0;
                } else {
                    ordered_total = fp_compute_total(
                        &fp, tree, db_root, object, &sch, &driver_fs, &dl,
                        &ordered_total_null, prefilter_ks);
                }
            }
```

Then replace the complete close block beginning at the same anchor and ending
at `"                OUT(want_total ? \"],\\\"total\\\":null}\\n\" : \"]\\n\");"`
with:

```c
            if (dict_fmt)
                OUT(!want_total ? "}\n" : ordered_total_null
                    ? "},\"total\":null}\n" : "},\"total\":%zu}\n",
                    ordered_total);
            else if (rows_fmt)
                OUT(!want_total ? "]\n" : ordered_total_null
                    ? "],\"total\":null}\n" : "],\"total\":%zu}\n",
                    ordered_total);
            else
                OUT(!want_total ? "]\n" : ordered_total_null
                    ? "],\"total\":null}\n" : "],\"total\":%zu}\n",
                    ordered_total);
```

Do not use a candidate-set size when `ordered_total_null` remains true:
`fp_compute_total()` already performs the full criteria recheck when that is
safe and otherwise deliberately says no exact answer is available.  Thus joins,
unsupported CSV, deadlines, full scans, and genuinely unavailable exact counts
remain `null`; this task does not make `total:true` globally eager.

Prove each total regression as required: temporarily restore the old `null`
close, run `test-find-total-ordered-customers` and paste its expected failure;
reapply the exact-count close and paste the passing output.  Then run:

```sh
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-find-total-ordered-customers
./build/bin/shard-db-test run test-find-with-total
./build/bin/shard-db-test run test-cursor-with-total
./build/bin/shard-db-test run-all
```

## Review checklist

- UUID read keys are always 36-character dashed strings in default, dict,
  rows, cursor, and CSV read forms; raw 16-byte key storage is never passed to
  a JSON `%s`/`%.*s` key emitter.
- The cursor emitted after a UUID row carries the same dashed key that a client
  can send back; page ordering and hash16 tie-break semantics are unchanged.
- `total:true` is a number for the two customers pagination shapes and remains
  `null` for routes where an exact value has not been computed safely.
- No changes touch disk layout, index layout, request syntax, locks, caches,
  or threads.
- Inspect the raw uncommitted `git diff` before any commit.  No commit, push,
  PR, or GitHub issue creation is authorized by this plan.

## Out of scope

- Retrofitting every historical raw-key emitter not reachable from fetch,
  ordered find, or cursor find.
- General JSON-string escaping policy for arbitrary legacy `AK_NONE` keys.
- Making every `find total:true` execution route perform an additional full
  count.
- Client-side workarounds or issuing a second count request from the UI.
