# R3 exact aggregates — blind-review fix handoff

Branch: `fix/type-correctness-recovery`, base `ae9cb6b`, R3 work **uncommitted** in the tree.
Source: plan-blind review of the raw R3 diff (14 findings). Fix them on top of the
current working tree. Leave everything **uncommitted**. No sanitizers now (the
once-per-branch gate runs later, after all fixes).

## Execution rules
- Do tasks in order. Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run <name>` per task, then the full `run-all` once at the end (must be 0 failed, 0 warnings).
- Every task is test-first. Bug fixes: write the test, run it RED against the current tree and paste the failure, apply the fix, paste GREEN. For Tasks 1, 2, 3, 4: also temporarily revert the fix and show the test fails again, then restore.
- Anchors are quoted text, never line numbers. If a quoted anchor is not found exactly once, write `PLAN_NOTES.md` and halt the whole run. If a task says "decision needed", stop and ask; do not improvise.
- Do not weaken any existing assertion. No drive-by changes.

---

## Tier A — correctness / crash (do first)

### Task 1 (HIGH) — daemon SEGV: `order_by` on integer `avg` with an empty group
**Root cause.** `agg_num_value` (src/db/agg_exact.c) sets `out->cls = AGC_DEC` for every non-float AVG and returns early (`count <= 0`) with `dv == NULL`. `agg_bucket_value` (src/db/query_aggregate.c) then runs `if (!out->val.present) out->val.cls = ctx->spec_nums[i].cls;`, which resets the class to `AGC_INT`. `agg_value_zero_fill` only patches `dv` when `cls == AGC_DEC`, so `dv` stays NULL, and `agg_value_cmp` compares this INT value to another bucket's DEC avg via `dec_cmp(a->dv, b->dv)` → NULL deref.

**Test first.** New file `src/test/cases/test_agg_empty_avg_order.c` (mirror the daemon-per-case style of `test_agg_exact_routes.c`). Object `t`: `g:varchar:10`, `v:int`. Rows `{g:x,v:5}`, `{g:y,v:null}`, `{g:z,v:7}`. For each of `order`/`order_by` asc and desc, and for `avg`, `min`, `max`, `sum`: run `aggregate` with `group_by:["g"]`, `order_by:"<alias>"`; assert the response has no `"error"`, has 3 groups, and the daemon still answers a follow-up `count`. Also add a `long` field and a `short` field variant of the avg case. Register it in `build.sh` next to `test_agg_exact_routes.c` (quoted anchor: `src/test/cases/test_agg_exact_routes.c \`).

**Fix.** In `agg_bucket_value`, anchor:
```c
        if (!out->val.present) out->val.cls = ctx->spec_nums[i].cls;
        return 0;
```
replace with:
```c
        /* AVG of an integer class is always a decimal value; resetting the
           class for an empty group would leave an INT-tagged value with no
           integer payload next to DEC-tagged siblings. */
        if (!out->val.present && spec->fn != AGG_AVG)
            out->val.cls = ctx->spec_nums[i].cls;
        return 0;
```
And harden `agg_value_zero_fill` anchor `v->present = 1;` so an unexpected class never reaches `dec_cmp` with NULL:
```c
static void agg_value_zero_fill(AggValue *v) {
    v->present = 1;
    if (v->cls == AGC_DEC && !v->dv) v->dv = &AGG_ZERO_DEC;
}
```
(unchanged; the class guard above is the real fix). Also confirm GREEN for NULL-only groups through the NQL path and the CSV format.

### Task 2 — `decimal_order_decode` SEGV on 7-byte payload
**Root cause.** `n == 7` gives `nd == 0`; `dec_reserve(out, 0)` leaves `digits == NULL`; the check `!out->digits[0]` dereferences it.

**Test first.** In `src/test/cases/test_decimal_order_decode.c`, add to the malformed-input table: `{0x02,0x80,0,0,0,0,0}` (positive, nd=0) and `{0x00,0x7f,0xff,0xff,0xff,0xff,0xff}` (negative, nd=0) — both must return -1 with `errno == EILSEQ`, not crash. RED = process SIGSEGV.

**Fix.** In `src/db/decimal_order.c`, anchor `if (nd > DEC_MAX_LIMBS || (in[0] != 0 && in[0] != 2))` becomes:
```c
  if (nd == 0 || nd > DEC_MAX_LIMBS || (in[0] != 0 && in[0] != 2))
```
(`nd == 0` is non-canonical: zero is the single byte `01`.)

### Task 3 — leak of each WFC worker's decimal scratch
**Root cause.** `wfc_worker` calls `agg_env_init(&w->env, NULL, 0);` and nothing frees `w->env`; the merge loop only calls `agg_num_release(&wargs[s].num, ...)`.

**Test first.** A leak is only visible to LSan, which is not run now. Write a unit test that does not need LSan: add to `test_agg_exact_core.c` a case that runs `agg_env_init` / `dec_unpack` into `env.val` / `agg_env_free` and asserts `env.val.digits == NULL` afterwards (documents the contract). For the production call-site, the verification is code review plus the later ASan gate; note this explicitly in your hand-back — do not claim a RED/GREEN for the call-site.

**Fix.** In the WFC merge loop, anchor:
```c
                    agg_num_release(&wargs[s].num, wfc_ns.cls, &wenv);
```
add on the next line `agg_env_free(&wargs[s].env);`. Verify (a) `wargs` is zero-initialised so a worker that never started leaves a zeroed env that `agg_env_free` accepts, and (b) every early-exit path out of that loop also frees the remaining workers' envs (add a cleanup loop if not). Report what you found for (a) and (b).

### Task 4 — shared `g_sort_desc` race across concurrent queries
**Root cause.** `agg_sort_row_cmp` reads the file-static `g_sort_desc`, written by `agg_sort_buckets`; two concurrent aggregates with different `order` race.

**Test first.** Direction is per-call state, so assert it structurally: add a core test that sorts two row arrays with opposite directions via the new field (below). RED = does not compile (field missing). Note TSan confirmation is deferred to the gate.

**Fix.** Anchors in `query_aggregate.c`:
1. `    int        use_label;/* text group field: compare group_vals with strcmp */\n} AggSortRow;\nstatic int g_sort_desc;` → 
```c
    int        use_label;/* text group field: compare group_vals with strcmp */
    int        desc;     /* sort direction, per row (no shared state) */
} AggSortRow;
```
2. `    return g_sort_desc ? -cmp : cmp;` → `    return x->desc ? -cmp : cmp;`
3. In the fill loop, after `rows[i].b = buckets[i];` add `rows[i].desc = order_desc;`; delete the line `g_sort_desc = order_desc;`.
Grep afterwards: `g_sort_desc` must have zero hits.

### Task 5 — call-site return codes for `emit_min_max_via_keyset`
`emit_min_max_via_keyset` returns 1 = answered, 0 = declined, -1 = error (verify in its header comment/body first). Both callers (anchor `if (ks_rc == 0) return 0;       /* answered */`, two occurrences — edit each) treat 0 as answered.

**Decision needed before editing:** read the function and confirm (i) the return contract and (ii) who frees `ks` on the declined path. Then fix both sites to:
```c
if (ks_rc > 0) return 0;      /* answered */
if (ks_rc < 0) return -1;     /* error already emitted */
/* 0: declined — fall through to the general path (free ks if the callee did not) */
```
**Test first:** if the decline case is unreachable by design (reviewer says the `agg_index_exact_field` gate precludes it), add a comment documenting that and a debug `assert`-free note; no behavioural test is possible — say so in the hand-back rather than faking one.

---

## Tier B — robustness / honesty of error handling

### Task 6 — silent partial results on budget exhaustion
Fast paths drop groups when `agg_find_or_create` returns NULL and still emit success: VS commit (`if (!b) continue;` near `b->accums[j].num = vs_nums[k]`), IGB bitmap loops (`if (!b) break;`), `agg_text_offer` (sets only `budget_exceeded`), and the top-N stream (`btree_idx_walk_ordered` return ignored; `topn_heap_offer` malloc failure drops groups). `igb_skip` checks `agg_error` / `predicate_error` but not `ctx.budget_exceeded`.

Required behaviour: any of those conditions ⇒ the whole aggregate returns `{"error":"query memory buffer exceeded; ..."}` (the existing message used elsewhere in this file) and no success body. **Exception to preserve:** the bitmap deferred collector's documented partial-result behaviour (AGENTS.md "Per-query memory cap") — do not change that site.

Test first: with a tiny `QUERY_BUFFER_MB` seam if one exists (check `test_agg_exact_routes.c` limits cases; `ar_limits` already exercises budgets), add cases for VS, IGB, and top-N that expect the error. If a path cannot be forced from the harness, list it in the hand-back. Fix each site to set the error and propagate; the top-N stream must check the walk's return value.

### Task 7 — having literal validated by one parser, evaluated by another
`dec_parse` (validation, ~"invalid numeric value ... for having") accepts surrounding whitespace and `1e400`; `agg_value_cmp_text` float path uses `strtod` which rejects trailing space and returns inf. Reproduce: `having [{"field":"a","op":"lt","value":"5 "}]` on a double `avg` ⇒ currently `{"error":"invalid typed record"}`.

Fix: for float-class fields, validate the having literal with the *same* `strtod` rule as evaluation (`end != lit`, `*end == 0`, `isfinite`), returning the existing `invalid numeric value '<lit>' for having on '<alias>'` message; for decimal-class keep `dec_parse` but reject leading/trailing whitespace explicitly so both parsers agree. Test first: `"5 "`, `" 5"`, `"1e400"` on double avg and on a decimal sum ⇒ all the same `invalid numeric value` error.

### Task 8 — inconsistent scale for empty sum/min/max; E2BIG message
- `agg_cell_from_num` prints `"0"` when count is 0, but a zero sum prints `0.00` at declared scale. **Decision needed:** should an empty-group sum/min/max render (a) `null`, (b) declared-scale zero, or (c) remain `0`? Docs say "bounded numeric(P,S) results render at the declared scale", which implies (b) for sum; min/max of an empty group is arguably `null`. Ask, then align code, `docs/query-protocol/aggregate.md`, and tests.
- Composite group key buffer is 4096 bytes but the E2BIG error cites "(511 bytes)". Fix the message to name the right limit (grep `511 bytes` in query_aggregate.c). Test: wide multi-field key ⇒ message contains the actual limit.

### Task 9 — `agg_minmax_same_field_btree` memmove headroom
It shifts `len1`/`len2` up by one inside `buf1[1032]`/`buf2[1032]` with no guard. Add `if (len1 >= sizeof buf1 || len2 >= sizeof buf2) → error/decline` before each memmove (read the function; use the file's existing error convention). Test: a unit-level check is not practical; document as defensive and say so.

### Task 10 — partition-splice cleanup hazard
After Phase 3 rewrites `b->next`, `agg_ctx_free_local(&merge_args[p].local)` → `agg_ctx_release_nums` walks `pc->ht` chains that now run into main-ctx buckets and zeroes their `AggNum` state. Harmless today only because Pass 2 runs after. Fix: after the splice (near `agg_arena_transfer(&ctx.arena, &pc->arena);`) NULL out / free `pc->ht` so the release walk cannot traverse into foreign buckets — but only after verifying `agg_ctx_free_local` still frees the `ht` array itself (avoid a leak). Hand-back must state what you checked.

---

## Tier C — scope and docs (stop and ask where marked)

### Task 11 — `decode_idx_to_buf` gained an `FT_TIMESTAMP` case (scope beyond aggregates)
Before: timestamp returned 0 so `idx_candidate_admits` (query.c) admitted the candidate and the record check decided. Now find/count pre-filters compare `"%lld"` text with `strcmp` (eq/neq) and `atof` (ranges); `query.c` bitmap-dictionary match (~line 3086) also changes. **Decision needed:** (a) revert the timestamp case and keep aggregates working another way, or (b) keep it and add find/count regression tests (eq with leading-zero literal, range over >2^53 values) plus a changelog entry. Do not pick silently.

### Task 12 — documentation and changelog gaps
Update `docs/reference/changelog.md`, `docs/query-protocol/aggregate.md`, and `npm/` typings/README so they state:
1. Float/double aggregate outputs changed from `%.6f`-trimmed to shortest round-trip (up to 17 digits, e.g. `0.30000000000000004`).
2. Integer `avg` now renders 16 fractional digits (`1010` → `1010.0000000000000000`); name the rule (PostgreSQL select_div_scale-style).
3. The having error text change (`invalid numeric value '<lit>' for having on '<alias>'` precedes the op check; regex on numeric having now reports this).
4. Correct the changelog claim "allocation failures return an error before any success response" so it is true after Task 6 (verify, don't assume).
5. `aggregate.md`: remove or mark as packed-only the "bare-numeric grouping (`1.0` and `1.00` labelled `1`)" text — packed schemas have no production caller (`typed_schema_bind_packed`), so users cannot reach it.
6. Task 11's outcome, whichever way it goes.

### Task 13 — test/provenance hygiene
- `agg_exact_vectors.h` says it was generated by `gen_agg_vectors.py`, which is not in the diff. **Decision needed:** commit the generator under `src/test/tools/` (or similar) or reword the header to say the vectors are committed data. Don't leave a reference to a missing file.
- `test_agg_packed_routes.c` "partition merge" case uses a bare-numeric f0 group with an int sum, so it does not exercise exact-state (`dec_t`) merging in the IGB partition path. Add a packed decimal-sum group-by case with ≥100k distinct groups across partitions (follow the existing "extended" fixture pattern) and assert totals against the Python-decimal oracle.
- Split the two statements on one line near the `agg_bucket_value` neighbourhood (`:1032` in the reviewer's numbering — locate with `grep -nE ';[ ]+[a-z_]+\(.*;$'` style review of the diff only; fix only lines this diff added).

---

## Hand-back checklist
- `SKIP_TESTS=1 ./build.sh`: 0 warnings. `git diff --check` clean.
- Fresh `./build/bin/shard-db-test run-all`: 0 failed (paste the tail).
- For Tasks 1, 2, 4: RED output, GREEN output, revert-proof output pasted.
- List every "decision needed" you stopped on (Tasks 5, 8, 11, 13) with your recommendation.
- Note explicitly that Tasks 3, 4, 10 touch lifetimes/shared state and are covered by the pending ASan+UBSan / TSan gate (not run now).
- Nothing staged, committed, or pushed.
