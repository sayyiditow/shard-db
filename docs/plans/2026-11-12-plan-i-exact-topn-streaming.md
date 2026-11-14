# Plan I — count-ranked streaming top-N returns exact extra aggregates and a defined tie order

Supersedes the short draft `docs/plans/2026-11-08-plan-i-exact-topn-streaming.md` and the note `docs/plans/2026-10-30-topn-stream-perf-note.md` (both kept unchanged as history; re-derived against HEAD `8cbda76`).

## Brief

**Goal:** a top-N aggregate ranked by `count(*)` that also asks for exact extras (`count(g)`, `sum/avg/min/max(g)` on the group field `g`) streams the group-field index in O(limit) memory again, with output byte-identical to the exact hash path — including which groups win when counts tie at the limit.

**Problem and root cause:** `agg_run_topn_stream` (`src/db/query_aggregate.c`) declines (`-2`) whenever any spec is not `count(*)` (R3 decision D7), because its heap stores `double` metrics and cannot hold exact values or per-field presence counts. Every such query takes the hash path, which materialises every group. Two further defects make "byte-identical" impossible today: (1) the heap compares metrics only, so when counts tie at the limit the winners and their order depend on heap position, not on the group value; (2) the hash path sorts with `qsort` on the metric alone, so tied groups come out in hash-table order. Separately, the stream answers `"total":null` while the hash path answers the exact group count.

**Key observation (makes the draft's second pass unnecessary):** `eligible_for_topn_stream` already admits only specs that are `count(...)` or aggregate the group field itself. For one group with value `v` and `c` rows, every such spec is a function of `(v, c)`: `count(*) = c`; `count(g) = c` (0 for the NULL group) — except for the presence-doctrine types (varchar, date, datetime, datetimems, time, uuid), whose `count(g)` also skips an encoded "unset" value (`""`, zero date, midnight, zero uuid), so `count(g)` on those types stays on the hash path; `min = max = v`; `sum = v × c`; `avg = sum / c`. So the exact extras can be computed for the selected winners only, at render time, from the group key and its count — **one ordered walk, no second pass, no inter-pass consistency contract.**

**Logical flow (after the change):**
1. `agg_run_topn_stream` accepts the request when: the ranking spec is `count(*)`; every other spec is `count(*)`, `count(g)` (not on varchar/date/datetime/datetimems/time/uuid), or `sum/avg/min/max(g)` with `g` numeric, index-exact (`agg_index_exact`) and not floating for `sum/avg`. Anything else still declines to the hash path (unchanged).
2. The walk is unchanged except that the heap orders entries by `(count, group key)`: equal counts keep the smaller group key (NULL last). It also counts distinct groups for `total`.
3. At render time, per winner: `count(*)`/`count(g)` from the count; value specs from one exact state built by offering the group key once (`agg_num_offer_key`) and scaling it to `c` copies with a new exact helper `agg_num_repeat`, then rendered by the hash path's own `agg_cell_from_num`.
4. The hash path's sort gets the same tie rule: equal ranking values order by group value ascending in index order, NULL last, field by field.
5. With `total`, the stream reports the exact distinct-group count (as the hash path does) instead of `null`.

**Verified facts (code-read 2026-11-12, HEAD `8cbda76`):**
- Aggregate has no `offset`; `limit` is 1..10000 for the stream (`eligible_for_topn_stream`); `having`, `format:"csv"` and multi-field `group_by` are already ineligible; the dispatch also excludes `min/max` over text fields.
- The stream walks `btree_idx_walk_ordered` ascending, so groups arrive in increasing group-key order; the NULL group key is tier byte `0x01` and sorts after every value key (`0x00` tier).
- Both paths render group labels with `agg_cell_text` (quoted) and counts with `agg_cell_count`; the hash path renders value specs with `agg_cell_from_num` (empty `sum` → zero at scale; empty `min/max/avg` → `null`).
- `agg_num_offer_key` is the exact index-key offer already used by index-only shortcuts; it refuses bare packed numeric (`agg_index_exact == 0`) and returns 0 for the NULL tier.
- Hash-path `total` = distinct groups before `limit` (`agg_total_groups = nbuckets`); the stream prints `"total":null`.
- `agg_sort_row_cmp` compares only the metric (or label); ties keep `qsort` order.
- Count ranking uses `double` metrics: exact up to 2^53 rows per group (documented limit, not changed).
- One existing test asserts the stream's `"total":null`: `test-agg-total-streaming-topn` (test_find_with_total.c) — updated by 2D′. No test asserts a tie order. `test-topn-stream-sum-min-max` (order by `sum`) keeps declining.
- Carried in from Plan E: `test-topn-stream-bitmap-postfilter-bounded` failed once under a full `--jobs 4` run (response held neither count) and passed on rerun and 50 solo runs; I ran 18 more concurrent solo runs during planning — all passed. Root cause unknown.

**Decisions made:**

| # | Decision | Alternatives | Why | Tag |
|---|---|---|---|---|
| 1 | Compute extras from `(group value, count)` at render time for winners only; single walk. | Draft's two-pass (select winners, then re-read their rows). | Within current eligibility every extra is a function of value and count; no second read means no concurrent-write consistency problem and O(winners) exact work. | DEFAULT |
| 2 | Ranking stays `count(*)` only; `order_by` a sum/avg/min/max alias still uses the hash path. | Exact heap metrics for every function. | The note's larger option; separate design. | DEFAULT |
| 3 | `sum`/`avg` over `float`/`double` group fields decline to the hash path; `min/max/count` over them stream. | Stream them with `v × c`. | Binary floating sums depend on addition order; the hash path's own per-worker merges are not reproducible, so byte equality cannot be promised. | DEFAULT |
| 4 | **Tie order becomes defined on both paths:** rows with equal `order_by` values sort by group value ascending in the field's index order (numbers numerically, text bytewise, enum by declaration order, IPs by address bytes, timestamps by time), NULL group last, multi-field groups field by field; direction (`asc`/`desc`) does not flip the tie order. | Leave ties unspecified (then streams and hash cannot be compared on ties). | Required for byte-identical output; makes every aggregate page deterministic. **Visible change:** tied groups that used to come out in hash-table order now come out in this order, and a stream's tied winners at the limit change to the smallest group values. Cost: tie keys are built only for runs of equal rows that start before `limit`, after the existing sort (none when nothing ties); a tied run that crosses the limit (e.g. many count-1 groups under "top 10") costs one key per row of the run, charged to `QUERY_BUFFER_MB` — such a query can now hit the buffer cap where it passed before; zero/unset values of date/time/IP/uuid fields sort first among values, as on the stream. | NEEDS YOU → approved by you 2026-11-13 |
| 5 | Stream `total` becomes the exact distinct-group count (was `null`). | Keep `null`. | Byte equality with the hash path; the walk already visits every group. Visible change for `"total":true` top-N requests. | DEFAULT |
| 6 | New exact helper `agg_num_repeat` (INT: checked multiply; DEC: double-and-add) rather than replaying `c` offers. | Replay `c` offers per winner. | O(log c) instead of O(c) decimal additions; partial sums never exceed the final one, so overflow (ERANGE) matches the offers exactly; unit-tested against replay. | DEFAULT |
| 7 | TEST_BUILD counters and a force-decline seam in `agg_run_topn_stream`, process-local tests. | Daemon-only tests. | Proves which path served each query and compares stream vs forced hash in one process. | DEFAULT |
| 8 | The carried-in flaky case gets response logging now (no assertion change) and is watched in this plan's suite runs; no fix without a reproduction. | Block Plan I on it. | Not reproduced in 68 solo runs; CORE-PROCESS allows time-boxed escalation; this plan changes the same path, so the log is the useful first step. | DEFAULT |

**Blast radius:** `src/db/query_aggregate.c` (`agg_run_topn_stream` eligibility, heap comparator `topn_*`, `topn_finalize_run`, `topn_walk_cb` unchanged, render loop, `agg_sort_row_cmp`/`agg_sort_buckets`, new `agg_tie_key_build`/`agg_tie_cmp`), `src/db/agg_exact.c/.h` (new `agg_num_repeat`), `src/db/query_internal.h` (TEST_BUILD declarations), one diagnostic line in `test_agg_topn_stream.c`, one assertion in `test_find_with_total.c` (2D′), new test file. Public heap function signatures (`topn_heap_*`) unchanged. Callers of `agg_sort_buckets`: one (hash-path emit), serving every grouped aggregate with `order_by` (tie order change and a new `limit` argument, decision 4); an ordered aggregate whose tied run crosses the limit now charges its tie keys to `QUERY_BUFFER_MB` and can hit the cap. No wire-shape, CLI, config or on-disk change; numbers render exactly as before.

**Test strategy** (new `src/test/cases/test_agg_topn_exact.c`, process-local DB):
- Ties at the limit (desc, asc, cap above group count) on the stream: smallest group values win, in the defined order.
- Mixed `count(*)` + `count/sum/min/max(g)` and `avg(g)` on int and `numeric:30,10` (beyond double precision) stream, equal the forced-hash output byte for byte, and build exact state only for the selected non-NULL groups.
- Ineligible shapes (float `sum`, `count` of another field, ranking by `sum`) still decline.
- `total` exact on the stream.
- Hash path honours the same tie order (forced hash).
- `agg_num_repeat` equals repeated offers (INT, DEC, overflow, float refused).
- Broad differential across int, numeric, varchar, enum, ipv4, timestamp, double group fields, NULL groups, criteria, asc/desc, limits 1/3/10000, `total` on/off — inline and with worker pools running.
- Render failure → one error response, no partial body.

**Out of scope:** the pre-existing `topn_heap_offer` full-heap malloc-failure path (it drops the last entry and leaves the root key NULL; the walk already reports one error, so only a leak — own fix); ranking by non-count metrics; `offset` (aggregate has none); composite or multi-field streaming; extras on fields other than the group field (still ineligible); the bitmap-postfilter flake fix (diagnostic only); benchmarks (you run them); sanitizers (branch gate after this plan).

**Review log:**
- Round 4 (fresh reviewer): **0 blockers, 0 majors → converged.** Re-verified 2F compile/logic, the zero/unset mapping against index order for every listed type, T5 pages by hand, T6 stream-vs-hash agreement incl. `0.0.0.0`, mB/mL and mA–mK. Minors fixed: the zero/unset branch now requires an empty or all-`0` label (other unencodable labels take the `0x01` tier); T6 adds `total` with criteria; mutation runs are scoped to the named cases. Noted: if `-Wmaybe-uninitialized` appears for `key[]` in `agg_tie_key_build`, record it and initialise with `= {0}`.
- Round 3: 1 blocker, 2 majors, 4 minors. Blocker: (1) the T5 unset-label case had an index, so the forced hash took the indexed route and labelled midnight `"00:00:00"` — `tqh` now has no index (record-scan route, label `""`). Majors: (2) zero ipv4/ipv6/uuid/date/time labels did not encode and fell after every value on the hash path while the stream sorts them first — such labels now map to the smallest key, T6 gains `0.0.0.0` groups, mL re-targeted; (3) tie keys were built for tied runs past the limit — runs starting at or after `limit` are skipped; the straddling-run cost and the new possible buffer-cap failure are stated in the Brief. Minors fixed: T4 frees parsed decimals and zero-initialises states; each case names the objects it creates; T6 asserts its inline pass; docs caveat names the scan route.
- Round 2: 2 blockers, 1 major, 5 minors. Blockers: (1) the hash-path tie key failed the query for labels that do not encode — the hash path labels several real values `""` (midnight, `0.0.0.0`, zero uuid/date, incomplete spans) — now such labels take a fallback tier after encodable values and only ENOMEM/ENOBUFS can fail, with a time-midnight test and mutation mL; (2) `test-agg-total-streaming-topn` asserts the stream's `"total":null` — 2D′ updates it to the exact count as the plan's one declared assertion change, covered by mF. Major: (3) tie keys were built and under-charged for every row of every ordered aggregate — now built exact-size only for runs of equal rows after the existing sort, charged while the run sorts. Minors fixed: T0 adds zero values; the pool pass described honestly; scenario order for counters spelled out; multi-field and NULL tie order tested on the hash path (asc and desc); docs qualify the pre-existing unset-label difference.
- Round 1: 3 blockers, 2 majors, 7 minors. Blockers: (1) `count(g)` on varchar/date/datetime/datetimems/time/uuid also skips encoded unset values on the hash path, so a group key cannot answer it — those now decline (2C), with case D-d and mutation mK; (2) T6 field `n` collided with the count alias `n` — renamed `nm`; (3) `agg_cell_from_num` had no forward declaration above the stream — added (2E) and audited. Majors: (4) the encoder returns 1/0/-1 and 0 was treated as success — now only 1 is accepted, anything else fails the sort with an error; (5) encoding inside the qsort comparator allocated and could mix encoded and label comparisons — tie keys are now built once per row, charged to the query budget, and the comparator only compares bytes (2F rewritten). Minors fixed: `gns` zero-initialised; mA wording; T6 message token `X`; new pre-check case T0 proving count-only labels already match per type on base; encoder label-form audit (Phase 0 3b); hand-back note; docs (`total` row, Cost bullet, presence types); the pre-existing heap-offer malloc leak recorded as out of scope. All base-red sets and mA–mJ were recomputed by the reviewer and confirmed.

---

**Approval:** you approved the Brief as is on 2026-11-13 (decision 4: defined tie order). Executor: GLM 5.3 Flash; work left uncommitted.

## Plan (executor-facing)

### Execution rules
- Branch `fix/type-correctness-recovery`, HEAD `8cbda76`, tracked tree clean. Record `rtk proxy git rev-parse HEAD` and `rtk proxy git status --short` first in the evidence file; a modified tracked file → halt and ask.
- No git writes, no benchmarks, no sanitizer builds. Every shell command starts with `rtk`; use `rtk proxy grep` for counts.
- Build `rtk proxy env SKIP_TESTS=1 ./build.sh`; single case `rtk ./build/bin/shard-db-test run <name>`; full suite `rtk ./build/bin/shard-db-test run-all --jobs 4`. Record real exit statuses. After every test invocation: `rtk proxy rm -rf /tmp/shard-db-*`.
- `<scratch>` = `/tmp/claude-1000/-home-hashim-junk-system-git-workspace-sayyiditow-shard-db/8d2fa8b4-e5ea-4b71-8c14-d33e619fdb67/scratchpad` (create if missing).
- Evidence: `docs/plans/2026-11-12-plan-i-execution-evidence.md` (untracked); paste real outputs (TAP lines). Never weaken/delete/skip an assertion. This plan changes **exactly one existing assertion** (2D′, `test-agg-total-streaming-topn`, decision 5) and adds one diagnostic line (Phase 1A). If an existing test fails, halt and report.
- Anchor or assumption mismatch → write `PLAN_NOTES-plani.md` and halt. A moved symbol with an unambiguous site → proceed and record. A decision not covered → stop and ask. Phases in order; leave everything uncommitted.

### Phase 0 — audits (no edits)
1. `src/db/query_aggregate.c`: the forward-declaration block after `agg_report_error` (lines declaring `agg_cells_alloc`, `agg_cell_count`, `agg_cell_text`, `agg_cell_emit_json`) exists and does **not** declare `agg_cell_from_num` (2E adds it); `static int agg_count_inspects_presence(` and `static int agg_text_cmp(` are defined above `agg_run_topn_stream`. Exactly one each of `TOPN_RUN_VIS int agg_run_topn_stream(`, `static void topn_finalize_run(`, `static int topn_metric_beats(`, `static void topn_sift_down(`, `static void topn_sift_up(`, `TOPN_VIS int topn_heap_offer(`, `TOPN_VIS int topn_heap_drain(`, `static int agg_sort_row_cmp(`, `static int agg_sort_buckets(`, `static int agg_cell_from_num(`; the D7 loop `if (specs[i].fn != AGG_COUNT || specs[i].field[0]) return -2;` exactly once; the stream emit `OUT(want_total ? "],\"total\":null}\n" : "]\n");` exactly once. Record `topn_metric_beats` callers (expected: sift_down, sift_up, topn_heap_offer, topn_finalize_run).
2. Confirm `agg_run_topn_stream`'s only production caller is the dispatch in `cmd_aggregate_*` (the `rc == -2` fall-through) and that no test calls it directly (`rtk proxy grep -rn agg_run_topn_stream src/test`).
3. `AggBucket` has `group_vals`, `group_nulls` (record whether `group_nulls` can be NULL), `AggCtx` has `group_tfs`, `fs`, `ngroups`. Record the exact prototype of `encode_criterion_value_checked` (types.h or query_internal.h) and its output-length type.
3b. Read `typed_index_text_key_into` (typed_index.c, reached through `encode_criterion_value_checked`, which also requires `tf` to point into `ts->fields`) and record that it returns 1 for each label form the hash path stores in `group_vals`: int (`"-2"`), packed numeric (`"1.25"`, `"-3.00"`, `"99999999999999999999.9999999999"`), varchar including `""`, enum names, dotted-quad ipv4, epoch-millisecond timestamp digits, and double labels as the aggregate renders them (`"0.5"`, `"-1.5"`, `"2"` — check the renderer). Also record that `AggCtx.group_tfs[g]` is resolved from `ctx->fs->ts` (pointer into its `fields`). Also record which label forms return 0 (e.g. `""` for time/ipv4/date): those take the `0x01` fallback in 2F, never an error. A `group_tfs` pointer from another schema → halt.
4. `src/db/agg_exact.c` has static `fail`, `charge`, `uncharge`, `dec_bytes`, `slot_put`, `slot_copy`, `slot_add`; `dec_add` reads both inputs before writing its output (aliasing `a == b` is safe — read `dec_add` in decnum.c and record).
5. Request shapes: aggregate JSON keys `group_by` (array), `aggregates` (`fn`,`field`,`alias`), `order_by`, `order` (`"asc"`/`"desc"`), `limit`, `criteria`, `"total":true` (server.c `json_obj_is_true(&req, "total")` in the aggregate branch). Index syntax `e:btree` for enum (docs/concepts/indexes.md). Numeric values may be sent as JSON strings.
6. Read `src/test/cases/test_kef_batches.c` for the process-local fixture pattern (`test_get_process_db`, `tu_pdb_request`, `tu_pdb_drop_object`, own-pool start/stop with `parallel_io_pool_*`), and confirm `parallel_pool_init`, `parallel_pool_size`, `parallel_pool_shutdown` exist in types.h.
7. Baseline build; `rtk ./build/bin/shard-db-test list | wc -l` → `N0`.

### Phase 1 — instrumentation, diagnostic, base-red tests

**1A. Flake diagnostic (test-only, no assertion change).** In `test_agg_topn_stream.c`, `test_topn_stream_bitmap_postfilter_bounded`, immediately before the assertion "g0 count is the FILTERED active=true count (40000)", add `if (!SAFE_STRSTR(resp, "\"n\":40000")) TAP_DIAG("# bitmap-postfilter response: %.400s\n", resp ? resp : "(null)");`.

**1B. Counters and seams (TEST_BUILD only).** In `query_aggregate.c`, rename the existing `agg_run_topn_stream` definition to `static int agg_run_topn_stream_impl(` (same parameters, body unchanged). Immediately **before** it (after the `#define TOPN_RUN_VIS` block) insert:
```c
#ifdef TEST_BUILD
static _Atomic long g_topn_runs, g_topn_declines, g_topn_forced, g_topn_repeats;
static _Atomic int  g_topn_force_decline;
static _Atomic long g_topn_fail_repeat_at, g_topn_repeat_hits;
#endif
```
and immediately **after** the impl's closing brace add:
```c
#ifdef TEST_BUILD
void agg_topn_test_reset(void) {
    atomic_store(&g_topn_runs, 0); atomic_store(&g_topn_declines, 0);
    atomic_store(&g_topn_forced, 0); atomic_store(&g_topn_repeats, 0);
    atomic_store(&g_topn_force_decline, 0);
    atomic_store(&g_topn_fail_repeat_at, 0); atomic_store(&g_topn_repeat_hits, 0);
}
void agg_topn_test_force_decline(int on) { atomic_store(&g_topn_force_decline, on); }
void agg_topn_test_fail_repeat(long nth) {
    atomic_store(&g_topn_repeat_hits, 0); atomic_store(&g_topn_fail_repeat_at, nth);
}
void agg_topn_test_stats(long *runs, long *declines, long *forced, long *repeats) {
    *runs = atomic_load(&g_topn_runs); *declines = atomic_load(&g_topn_declines);
    *forced = atomic_load(&g_topn_forced); *repeats = atomic_load(&g_topn_repeats);
}
#endif
TOPN_RUN_VIS int agg_run_topn_stream(const char *db_root, const char *object,
                                      const Schema *sch, FieldSchema *fs,
                                      const AggSpec *specs, int nspecs,
                                      const char *group_by_field,
                                      const char *order_by_alias,
                                      int order_desc, int limit,
                                      CriteriaNode *tree, QueryDeadline *dl,
                                      int want_total, int *out_capacity) {
#ifdef TEST_BUILD
    if (atomic_load(&g_topn_force_decline)) { atomic_fetch_add(&g_topn_forced, 1); return -2; }
#endif
    int rc = agg_run_topn_stream_impl(db_root, object, sch, fs, specs, nspecs,
                                      group_by_field, order_by_alias, order_desc,
                                      limit, tree, dl, want_total, out_capacity);
#ifdef TEST_BUILD
    if (rc == 0) atomic_fetch_add(&g_topn_runs, 1);
    else if (rc == -2) atomic_fetch_add(&g_topn_declines, 1);
#endif
    return rc;
}
```
In `src/db/query_internal.h`'s top `#ifdef TEST_BUILD` block append:
```c
/* Streaming top-N aggregate counters and seams (query_aggregate.c).
   runs: stream answered (rc 0); declines: stream fell back (-2) on its own;
   forced: force_decline made it fall back; repeats: exact states scaled for
   selected groups. fail_repeat(n): the n-th repeat reports ENOMEM. */
void agg_topn_test_reset(void);
void agg_topn_test_force_decline(int on);
void agg_topn_test_fail_repeat(long nth);
void agg_topn_test_stats(long *runs, long *declines, long *forced, long *repeats);
```
(`g_topn_repeats` and `fail_repeat` are consulted only from Phase 2. In a non-TEST build none of this exists; the wrapper just calls the impl.)

**1C. Test file `src/test/cases/test_agg_topn_exact.c`**, registered in `build.sh` as `    src/test/cases/test_agg_topn_exact.c \` immediately after the line `    src/test/cases/test_agg_topn_stream.c \` (if that line is absent, after `    src/test/cases/test_kef_batches.c \` and record). Includes as in `test_kef_batches.c` plus `../db/agg_exact.h` (Phase 2 cases). Every case: process-local DB (`test_get_process_db()`; first request `add-dir default`), `agg_topn_test_reset()` before each measured request and on every exit path (cleanup label), drops every object it creates on every path, and on a failed `setup:` assertion jumps to cleanup and returns 1.

Helpers: `q(db, json)` → response (caller frees with `shard_db_free_result`); `stats()` reads the four counters; `agg(obj, group, specs_json, order, dir, limit, total, criteria_or_NULL)` builds `{"mode":"aggregate","dir":"default","object":"<obj>","group_by":["<group>"],"aggregates":<specs_json>,"order_by":"<order>","order":"<dir>","limit":<limit>[,"total":true][,"criteria":<criteria>]}`. Order inside every scenario: `agg_topn_test_reset()`, the measured request, read the counters and assert on them, then the "equals forced-hash output" helper (which resets and re-runs). "equals forced-hash output": run the request with `agg_topn_test_force_decline(0)`, keep the response, `agg_topn_test_reset(); agg_topn_test_force_decline(1)`, run it again, `agg_topn_test_force_decline(0)`, assert `strcmp == 0` and that the forced run's `forced == 1`.

Objects (all `splits` 8, `max_key` 16, bulk-insert `records` as a JSON object `{"<key>":{...},...}`; assert "setup: <obj> created" and "setup: <obj> loaded" (`"inserted":<n>`)):
- `tqa`: fields `["k:int"]`, indexes `["k"]`; keys `a00`..`a15`: `k` = 10 for `a00`–`a04`, 20 for `a05`–`a09`, 30 for `a10`–`a15` (counts 5, 5, 6).
- `tqb`: fields `["h:int","o:int","f:double"]`, indexes `["h","f"]`; keys `b00`..`b29`: `h` = 1 for `b00`–`b06` (7), 2 for `b07`–`b09` (3), 3 for `b10`–`b22` (13), 4 for `b23` (1), `null` for `b24`–`b29` (6); `o` = 1 and `f` = 1.5 on every row.
- `tqe`: fields `["t:varchar:8"]`, indexes `["t"]`; keys `e0`..`e5`: `t` = `""` for `e0`–`e2`, `"a"` for `e3`–`e4`, `null` for `e5`.
- `tqc`: fields `["d:numeric:30,10"]`, indexes `["d"]`; keys `c0`..`c6`: `d` = `"99999999999999999999.9999999999"` for `c0`–`c2`, `"0.0000000001"` for `c3`–`c4`, `"-3.25"` for `c5`, `null` for `c6`.

Abbreviations: `N` = `[{"fn":"count","alias":"n"}]`; `XH` = `[{"fn":"count","alias":"n"},{"fn":"sum","field":"h","alias":"s"},{"fn":"min","field":"h","alias":"mn"},{"fn":"max","field":"h","alias":"mx"},{"fn":"count","field":"h","alias":"c"}]`.

**Case T1 `test-topn-exact-tie-boundary`** (creates and drops `tqa`; count-only; on base the stream already serves these):
- `agg(tqa,k,N,n,desc,2)` → "tie desc: stream served" (`runs == 1`); "tie desc: page is 30 then 10" — response equals `[{"k":"30","n":6},{"k":"10","n":5}]\n`.
- `agg(tqa,k,N,n,asc,2)` → "tie asc: stream served"; "tie asc: page is 10 then 20" — equals `[{"k":"10","n":5},{"k":"20","n":5}]\n`.
- `agg(tqa,k,N,n,desc,5)` → "tie under cap: stream served"; "tie under cap: page is 30, 10, 20" — equals `[{"k":"30","n":6},{"k":"10","n":5},{"k":"20","n":5}]\n`.

**Case T2 `test-topn-exact-mixed-streams`** (creates and drops `tqb`, `tqc`, `tqe`):
- M1 `agg(tqb,h,XH,n,desc,3)`: "mixed int: stream served the query" (`runs == 1 && declines == 0`); "mixed int: response is the exact expected page" — equals `[{"h":"3","n":13,"s":39,"mn":3,"mx":3,"c":13},{"h":"1","n":7,"s":7,"mn":1,"mx":1,"c":7},{"h":null,"n":6,"s":0,"mn":null,"mx":null,"c":0}]\n`; "mixed int: exact state built only for the selected groups" (`repeats == 2`: the two non-NULL winners); "mixed int: equals forced-hash output".
- M2 `agg(tqb,h,[{"fn":"count","alias":"n"},{"fn":"avg","field":"h","alias":"a"}],n,desc,2)`: "mixed avg: stream served the query"; "mixed avg: equals forced-hash output".
- M3 `agg(tqc,d,[{"fn":"count","alias":"n"},{"fn":"sum","field":"d","alias":"s"},{"fn":"min","field":"d","alias":"mn"},{"fn":"max","field":"d","alias":"mx"},{"fn":"avg","field":"d","alias":"a"},{"fn":"count","field":"d","alias":"c"}],n,desc,1)`: "mixed decimal: stream served the query"; "mixed decimal: sum is exact beyond double precision" (contains `"s":299999999999999999999.9999999997`); "mixed decimal: equals forced-hash output".
- Declines: D-a `agg(tqb,f,[{"fn":"count","alias":"n"},{"fn":"sum","field":"f","alias":"s"}],n,desc,3)`: "float sum: declines to the hash path" (`declines == 1 && runs == 0`), "float sum: answers without error" (no `"error"`), "float sum: equals forced-hash output". D-b `agg(tqb,h,[{"fn":"count","alias":"n"},{"fn":"count","field":"o","alias":"c2"}],n,desc,3)`: "count of another field: declines to the hash path", "count of another field: equals forced-hash output". D-c `agg(tqb,h,[{"fn":"count","alias":"n"},{"fn":"sum","field":"h","alias":"s"}],s,desc,3)`: "order by sum: declines to the hash path", "order by sum: equals forced-hash output". D-d `agg(tqe,t,[{"fn":"count","alias":"n"},{"fn":"count","field":"t","alias":"c"}],n,desc,3)`: "count of an unset-capable group: declines to the hash path", "count of an unset-capable group: equals forced-hash output".

**Case T3 `test-topn-exact-total`** (creates and drops `tqa`, `tqb`):
- `agg(tqa,k,N,n,desc,2,total)` → "total: stream served"; "total: exact group count and tie order" — equals `{"rows":[{"k":"30","n":6},{"k":"10","n":5}],"total":3}\n`.
- `agg(tqb,h,XH,n,desc,3,total)` → "total mixed: equals forced-hash output".

**Case T0 `test-topn-exact-type-labels`** (pre-check, base-green guard): object `tqf`, fields as `tqd` (below, T6), indexes as `tqd`; records `f0`..`f14`: rows `f0`–`f3` all fields `null`; rows `f4`–`f6`: `i` 5, `nm` `"1.25"`, `t` `"pear"`, `e` `"red"`, `ip` `"9.0.0.1"`, `ts` 1700000000000, `x` 0.5; rows `f7`–`f8`: `i` -2, `nm` `"-3.00"`, `t` `""`, `e` `"blue"`, `ip` `"10.0.0.2"`, `ts` 1600000000000, `x` -1.5; row `f9`: `i` 9, `nm` `"0.50"`, `t` `"fig"`, `e` `"green"`, `ip` `"10.0.0.10"`, `ts` 1700000001000, `x` 2.0; rows `f10`–`f14`: `i` 0, `nm` `"0.00"`, `t` `"kiwi"`, `e` `null`, `ip` `"10.0.0.3"`, `ts` 1650000000000, `x` 0.0 (15 records; per field the group counts are 5, 4, 3, 2, 1 — for `e` the NULL group has 9 — no ties). For each field `G` in {i, nm, t, e, ip, ts, x}: `agg(tqf,G,N,n,desc,10000)` → "labels <G>: stream served" and "labels <G>: equals forced-hash output". Purpose: proves the count-only stream already renders every type's labels exactly like the hash path; if any assertion fails on base, halt and report it (a pre-existing mismatch this plan does not cover).

**1D. Base-red proof (Phase 1 done, no production change yet).** Build; `list | wc -l` = `N0 + 4`; run T0, T1, T2, T3 singly. Expected failing sets **exactly** (derivations below; any other set → halt):
- T0: none (guard; see its purpose).
- T1: {"tie desc: page is 30 then 10", "tie asc: page is 10 then 20", "tie under cap: page is 30, 10, 20"}. Base heap (metric-only, strict compare): desc cap 2 keeps 10 then 20 (no swap on equal), 30 replaces root 10, sift-down swaps 20 up → drain `[30,20]`; asc cap 2 keeps 10, 20, rejects 30, drains `[20,10]`; desc cap 5 holds `[10,20,30]`, drains `[30,20,10]`.
- T2: {"mixed int: stream served the query", "mixed int: exact state built only for the selected groups", "mixed avg: stream served the query", "mixed decimal: stream served the query"}. Base declines every mixed spec (D7) and has no repeat counter. Every "equals forced-hash output" passes on base (both runs are hash), the exact M1 page has no ties so the hash path produces it, and D-a/D-b/D-c decline on base too.
- T3: {"total: exact group count and tie order"} (base: `[30,20]` and `"total":null`).
Paste each TAP output. Guard assertions (pass on base by design): every "stream served" in T1/T3, every "equals forced-hash output" (including "float sum: equals forced-hash output"), "mixed int: response is the exact expected page", "mixed decimal: sum is exact beyond double precision", all D-a/D-b/D-c/D-d assertions (base declines every spec with a field, D7).

### Phase 2 — production change

**2A. Exact repeat helper.** In `src/db/agg_exact.h` after `agg_num_merge`'s declaration:
```c
/* n holds exactly one offered value v (has = 1, sum = v). Makes sum equal to
   `times` offers of v (times >= 1); min/max/has unchanged. INT: checked
   multiply; DEC: double-and-add (every partial sum is at most |v * times|, so
   it overflows exactly when the offers would). FLT: EINVAL (floating sums
   depend on addition order). 0 ok / -1 errno. */
int  agg_num_repeat(AggNum *n, AggClass cls, int64_t times, AggEnv *e);
```
In `src/db/agg_exact.c` after `agg_num_merge`:
```c
int agg_num_repeat(AggNum *n, AggClass cls, int64_t times, AggEnv *e) {
    if (!n || !n->has || times < 1) return fail(EINVAL);
    if (cls == AGC_FLT) return fail(EINVAL);
    if (times == 1) return 0;
    if (cls == AGC_INT) {
        agg_i128 s;
        if (__builtin_mul_overflow(n->u.i.sum, (agg_i128)times, &s)) return fail(ERANGE);
        n->u.i.sum = s;
        return 0;
    }
    if (cls != AGC_DEC) return fail(EINVAL);
    dec_t base, acc;
    dec_init(&base); dec_init(&acc);
    int rc = slot_copy(e, &base, &n->u.d.sum);
    int have = 0;
    uint64_t t = (uint64_t)times;
    while (!rc && t) {
        if (t & 1) {
            rc = have ? slot_add(e, &acc, &base) : slot_copy(e, &acc, &base);
            have = 1;
        }
        t >>= 1;
        if (!rc && t) rc = slot_add(e, &base, &base);
    }
    if (!rc) rc = slot_copy(e, &n->u.d.sum, &acc);
    int err = errno;
    uncharge(e, dec_bytes(&base) + dec_bytes(&acc));
    dec_free(&base); dec_free(&acc);
    if (rc) { errno = err; return -1; }
    return 0;
}
```
(If Phase 0 item 4 shows `dec_add` cannot alias its inputs, use `slot_copy` to a temporary before the doubling add and record it.)

**2B. Heap total order.** Replace `topn_metric_beats` with:
```c
/* 1 when a ranks worse than b. The heap root is the worst kept entry. Lower
   count is worse for top-N (higher for bottom-N); on equal counts the larger
   group key is worse, so ties at the limit keep the smallest group values
   (NULL key 0x01 sorts last) — the hash path's tie order. */
static int topn_worse(const TopNHeap *h, double am, const char *ak, size_t al,
                      double bm, const char *bk, size_t bl) {
    if (am != bm) return h->order_desc ? am < bm : am > bm;
    size_t n = al < bl ? al : bl;
    int c = n ? memcmp(ak, bk, n) : 0;
    if (c) return c > 0;
    return al > bl;
}
#define TOPN_E_WORSE(h, x, y) topn_worse((h), (x)->metric, (x)->group_key, (x)->group_key_len, \
                                         (y)->metric, (y)->group_key, (y)->group_key_len)
```
- `topn_sift_down(h, i)`: `worst` starts at `i`; for each existing child `c` (`l`, then `r`): `if (TOPN_E_WORSE(h, &h->entries[c], &h->entries[worst])) worst = c;`; swap/continue as before.
- `topn_sift_up(h, i)`: while `i > 1`: `p = i/2`; `if (TOPN_E_WORSE(h, &h->entries[i], &h->entries[p]))` swap and `i = p`, else return.
- `topn_heap_offer`, full-heap branch: replace `if (!topn_metric_beats(h, metric, h->entries[1].metric)) return 0;` with `if (!topn_worse(h, h->entries[1].metric, h->entries[1].group_key, h->entries[1].group_key_len, metric, gk, gklen)) return 0;` (accept only when the root is worse than the candidate).
- `topn_finalize_run`: `int beats = was_full && topn_worse(h, h->entries[1].metric, h->entries[1].group_key, h->entries[1].group_key_len, metric, c->current_key, c->current_key_len);`
- `topn_heap_drain` unchanged (extracting the worst root first and filling from the end yields best-first order).
- After the change, `rtk proxy grep -c topn_metric_beats src/db/query_aggregate.c` → 0.

**2C. Eligibility in `agg_run_topn_stream_impl`.** Replace the D7 comment and loop (`/* The stream has only one row-count state…` through `if (specs[i].fn != AGG_COUNT || specs[i].field[0]) return -2;`) with:
```c
    /* Count-ranked streams with exact extras: the ranking spec is count(*);
       every other spec is count(*), count(group field) or sum/avg/min/max of
       the group field, whose exact values follow from the group value and its
       row count (rendered per winner below). Anything else uses the exact
       hash executor. */
    if (specs[order_spec_idx].fn != AGG_COUNT || specs[order_spec_idx].field[0]) return -2;
    {
        const TypedField *gtf = resolve_idx_field(fs->ts, group_by_field);
        AggNumSpec gns = {0};
        int gnum = gtf && gtf->type != FT_BOOL &&
            agg_num_spec_for(fs->ts, (size_t)(gtf - fs->ts->fields), &gns);
        for (int i = 0; i < nspecs; i++) {
            const AggSpec *s = &specs[i];
            if (s->fn == AGG_COUNT) {
                if (s->field[0] && strcmp(s->field, group_by_field) != 0) return -2;
                /* count(g) skips encoded unset markers ("" varchar, zero
                   calendar, midnight time, zero uuid) on the hash path; a
                   group key alone cannot tell, so these types decline. */
                if (s->field[0] && (!gtf || agg_count_inspects_presence(gtf->type))) return -2;
                continue;
            }
            if (strcmp(s->field, group_by_field) != 0) return -2;
            if (!gnum || !agg_index_exact(&gns)) return -2;
            if (gns.cls == AGC_FLT && (s->fn == AGG_SUM || s->fn == AGG_AVG)) return -2;
        }
    }
```

**2D. Distinct-group count.** Add `int64_t groups;` to `TopNWalkCtx`; in `topn_finalize_run`, right after the `if (!c->current_key) return;` line, add `c->groups++;`. Replace the stream's closing `OUT(want_total ? "],\"total\":null}\n" : "]\n");` with `if (want_total) OUT("],\"total\":%lld}\n", (long long)ctx.groups); else OUT("]\n");` (keep the preceding `agg_cells_dispose(cellv, …);` statement and put the new statement on its own line).

**2D′. Existing assertion (deliberate contract change, decision 5).** In `src/test/cases/test_find_with_total.c`, `test_agg_total_streaming_topn`: replace `ASSERT_EQ_INT(total, -999, "agg streaming topN → total = null (documented fallback)");` with `ASSERT_EQ_INT(total, 20, "agg streaming topN → total = exact distinct-tag count (20)");` and update the comment above the request to say the stream now reports the exact group count (200 rows, `tag` = `t%02d` of `i % 20` → 20 groups). This is the plan's only change to an existing assertion: it passes on base with `-999`, would fail after 2D until updated, and mutation mF makes the updated form fail.

**2E. Render extras.** First add `static int agg_cell_from_num(const AggNumSpec *, const AggNum *, enum AggFn, int64_t, AggCell *);` to the forward-declaration block next to `static int agg_cell_count(AggCell *, int64_t);`. In `agg_run_topn_stream_impl`'s render loop (`for (int i = 0; i < n && !rerr; i++)`), keep the label code and record `int null_group = raw_len && raw[0] == 0x01;` before it. Replace the inner `for (int j = 0; j < nspecs && !rerr; j++) if (agg_cell_count(...counts[i])) ...` with:
```c
        AggNum num;
        memset(&num, 0, sizeof num);
        int64_t present = 0;
        int have_num = 0;
        for (int j = 0; j < nspecs && !rerr; j++) {
            const AggSpec *s = &specs[j];
            AggCell *cell = &cellv[(size_t)i * nspecs + j];
            if (s->fn == AGG_COUNT) {
                int64_t v = (s->field[0] && null_group) ? 0 : counts[i];
                if (agg_cell_count(cell, v)) rerr = errno ? errno : ENOMEM;
                continue;
            }
            if (!have_num && !null_group) {
                int r = agg_num_offer_key(&ctx.gb_spec, raw, raw_len, &num, &renv);
                if (r < 0) { rerr = errno ? errno : EILSEQ; break; }
                if (r == 1) {
                    present = counts[i];
                    if (ctx.gb_spec.cls != AGC_FLT) {
#ifdef TEST_BUILD
                        atomic_fetch_add(&g_topn_repeats, 1);
                        long at = atomic_load(&g_topn_fail_repeat_at);
                        if (at > 0 && atomic_fetch_add(&g_topn_repeat_hits, 1) + 1 == at) {
                            atomic_store(&g_topn_fail_repeat_at, 0);
                            rerr = ENOMEM; break;
                        }
#endif
                        if (agg_num_repeat(&num, ctx.gb_spec.cls, counts[i], &renv)) {
                            rerr = errno ? errno : ENOMEM; break;
                        }
                    }
                }
                have_num = 1;
            }
            if (agg_cell_from_num(&ctx.gb_spec, &num, s->fn, present, cell))
                rerr = errno ? errno : ENOMEM;
        }
        agg_num_release(&num, ctx.gb_spec.cls, &renv);
```
`renv`: declare `AggEnv renv; agg_env_init(&renv, &render_bytes, g_query_buffer_max_bytes);` right after `_Atomic size_t render_bytes = live;`, and call `agg_env_free(&renv);` on both exits of the render section (the `if (rerr)` error branch before `agg_report_error`, and after the emit loop). Errors keep the existing contract: `agg_report_error(rerr); return -3;` — one error response, no success prefix.

**2F. Hash-path tie order (keys built only for tied runs).** The existing metric sort is unchanged; afterwards each run of rows that compare equal is re-sorted by a tie key built only for that run. Add above `agg_sort_buckets`:
```c
#define AGG_TIE_KEY_CAP 1024
/* Tie key of one bucket, per group field: an index-encodable value as
   [0x00][u32 BE len][index-key bytes] (encode_criterion_value_checked:
   numeric order for numbers, bytes for text, declaration order for enum,
   address bytes for IPs). For ipv4/ipv6/uuid/date/datetime/datetimems/time an
   empty or all-'0' label that does not encode is the type's zero/unset value
   (labelled "" or "00000000" by the hash path), which is the smallest stored
   value and the first index key: it gets [0x00][len 0], sorting before every other value as
   on the stream. Any other label that does not encode (or a field without a
   typed definition) is [0x01][u32 BE len][label bytes], after every value;
   NULL is [0x02], last. Never fails on data: only ENOMEM returns -1. */
static int agg_tie_key_build(const AggCtx *ctx, const AggBucket *b,
                             uint8_t **out, size_t *out_len) {
    size_t cap = 64, len = 0;
    uint8_t *buf = malloc(cap);
    if (!buf) { errno = ENOMEM; return -1; }
    for (int g = 0; g < ctx->ngroups; g++) {
        uint8_t key[AGG_TIE_KEY_CAP];
        const uint8_t *src = NULL;
        size_t kl = 0;
        uint8_t tag = 0x02;
        if (!(b->group_nulls && b->group_nulls[g])) {
            const char *v = b->group_vals[g];
            size_t vl = strlen(v);
            const TypedField *tf = ctx->group_tfs[g];
            int rc = 0;
            if (tf && ctx->fs) {
                errno = 0;
                rc = encode_criterion_value_checked(ctx->fs->ts, tf, v, vl, key, sizeof key, &kl);
                if (rc < 0 && errno == ENOMEM) { free(buf); return -1; }
            }
            if (rc == 1) { tag = 0x00; src = key; }
            else if (tf && (vl == 0 || strspn(v, "0") == vl) &&
                     (tf->type == FT_IPV4 || tf->type == FT_IPV6 || tf->type == FT_UUID ||
                            tf->type == FT_DATE || tf->type == FT_DATETIME ||
                            tf->type == FT_DATETIMEMS || tf->type == FT_TIME)) {
                tag = 0x00; src = key; kl = 0;          /* zero/unset value: smallest key */
            }
            else { tag = 0x01; src = (const uint8_t *)v; kl = vl; }
        }
        size_t need = len + 1 + (tag == 0x02 ? 0 : 4 + kl);
        if (need > cap) {
            while (cap < need) cap *= 2;
            uint8_t *nb = realloc(buf, cap);
            if (!nb) { free(buf); errno = ENOMEM; return -1; }
            buf = nb;
        }
        buf[len++] = tag;
        if (tag == 0x02) continue;
        buf[len++] = (uint8_t)(kl >> 24); buf[len++] = (uint8_t)(kl >> 16);
        buf[len++] = (uint8_t)(kl >> 8);  buf[len++] = (uint8_t)kl;
        if (kl) memcpy(buf + len, src, kl);
        len += kl;
    }
    uint8_t *exact = realloc(buf, len ? len : 1);   /* charge == allocation */
    *out = exact ? exact : buf; *out_len = len;
    return 0;
}
static int agg_tie_cmp(const uint8_t *a, size_t al, const uint8_t *b, size_t bl) {
    size_t i = 0, j = 0;
    while (i < al && j < bl) {
        uint8_t ta = a[i++], tb = b[j++];
        if (ta != tb) return ta < tb ? -1 : 1;
        if (ta == 0x02) continue;
        size_t la = ((size_t)a[i] << 24) | ((size_t)a[i+1] << 16) | ((size_t)a[i+2] << 8) | a[i+3];
        size_t lb = ((size_t)b[j] << 24) | ((size_t)b[j+1] << 16) | ((size_t)b[j+2] << 8) | b[j+3];
        i += 4; j += 4;
        int c = agg_text_cmp((const char *)a + i, la, (const char *)b + j, lb);
        if (c) return c;
        i += la; j += lb;
    }
    return (i < al) - (j < bl);
}
/* Never flipped by the sort direction. */
static int agg_tie_row_cmp(const void *pa, const void *pb) {
    const AggSortRow *x = *(const AggSortRow *const *)pa;
    const AggSortRow *y = *(const AggSortRow *const *)pb;
    return agg_tie_cmp(x->tie, x->tie_len, y->tie, y->tie_len);
}
/* Re-sort one run of equal rows by tie key; keys are charged to the query
   budget while the run is sorted and released afterwards. */
static int agg_sort_tie_run(const AggCtx *ctx, AggSortRow **run, int m) {
    size_t charged = 0;
    int rc = 0;
    for (int i = 0; i < m && !rc; i++) {
        if (agg_tie_key_build(ctx, run[i]->b, &run[i]->tie, &run[i]->tie_len)) { rc = -1; break; }
        charged += run[i]->tie_len;
        if (ctx->shared_buffer_bytes &&
            atomic_fetch_add(ctx->shared_buffer_bytes, run[i]->tie_len) + run[i]->tie_len
                > g_query_buffer_max_bytes) { errno = ENOBUFS; rc = -1; }
    }
    if (!rc) qsort(run, (size_t)m, sizeof *run, agg_tie_row_cmp);
    int e = errno;
    for (int i = 0; i < m; i++) { free(run[i]->tie); run[i]->tie = NULL; run[i]->tie_len = 0; }
    if (ctx->shared_buffer_bytes && charged) atomic_fetch_sub(ctx->shared_buffer_bytes, charged);
    if (rc) errno = e;
    return rc;
}
```
(`AggSortRow` must be declared before these helpers: put them after the `AggSortRow` typedef and `agg_sort_row_cmp`, before `agg_sort_buckets`.) Add `uint8_t *tie; size_t tie_len;` to `AggSortRow`. Give `agg_sort_buckets` a trailing `int limit` parameter (rows at index ≥ `limit` are cut after the sort); at its single caller compute `int eff_limit = limit > 0 ? limit : g_global_limit;` just before the call and pass it (the existing `if (limit <= 0) limit = g_global_limit;` after the sort stays). In `agg_sort_buckets`, replace
```c
        qsort(ptrs, (size_t)n, sizeof *ptrs, agg_sort_row_cmp);
        for (int i = 0; i < n; i++) buckets[i] = ptrs[i]->b;
```
with
```c
        qsort(ptrs, (size_t)n, sizeof *ptrs, agg_sort_row_cmp);
        for (int s = 0; s < n && s < limit && !rc; ) {   /* runs starting past the limit are cut anyway */
            int e = s + 1;
            while (e < n && agg_sort_row_cmp(&ptrs[s], &ptrs[e]) == 0) e++;
            if (e - s > 1 && agg_sort_tie_run(ctx, ptrs + s, e - s)) rc = -1;
            s = e;
        }
        if (!rc) for (int i = 0; i < n; i++) buckets[i] = ptrs[i]->b;
```
`agg_sort_row_cmp` itself is unchanged; a sort without ties builds no key and allocates nothing new; runs that start at or after `limit` are skipped; a run that straddles `limit` is sorted in full (bound: one key per row of that run, about label size + 5 bytes, charged to `QUERY_BUFFER_MB` while it sorts). `agg_sort_buckets` has one caller (the hash-path emit), which already turns `-1` into one error response (only ENOMEM/ENOBUFS can occur here).

**2G. Build** with no new warnings. If GCC reports `-Wmaybe-uninitialized` for `key[]` in `agg_tie_key_build`, initialise it with `= {0}` and record it.

**2H. Phase 2 test cases** (append to `test_agg_topn_exact.c`):

**Case T4 `test-agg-num-repeat`** (unit; `AggEnv e; agg_env_init(&e, NULL, 0);`, free at end):
- For each `v` in {7, -3, 0}: for each `t` in {1, 2, 3, 1000}: state A = `agg_num_offer_int(&A, v)` once then `agg_num_repeat(&A, AGC_INT, t, &e)`; state B = `agg_num_offer_int(&B, v)` `t` times; all four `t` must give equal `sum`, `min`, `max`, `has` and return 0 → one assertion per `v`: "repeat int v=7 matches repeated offers", "repeat int v=-3 matches repeated offers", "repeat int v=0 matches repeated offers".
- `v = ((agg_i128)1) << 125`, `t = 4`: offers fail with ERANGE on the 4th; `agg_num_repeat` returns -1 with errno ERANGE → "repeat int overflow reports ERANGE like repeated offers" (assert both).
- All `AggNum` states are zero-initialised (`AggNum x = {0};`). DEC: `dec_parse` `"99999999999999999999.9999999999"` and `"-0.0000000001"` into `dec_t` values (`dec_init` first, `dec_free` both at the end); for `t` in {1, 3, 1000, 4099}: A = `agg_num_offer_decimal` once + repeat; B = offer `t` times; `dec_cmp(&A.u.d.sum, &B.u.d.sum) == 0`, equal `dscale`, equal min/max (`dec_cmp`) → "repeat decimal 99999…9999 matches repeated offers", "repeat decimal -0.0000000001 matches repeated offers"; release states with `agg_num_release(&X, AGC_DEC, &e)`.
- FLT: `agg_num_offer_double(&F, 1.5)`; `agg_num_repeat(&F, AGC_FLT, 3, &e) == -1 && errno == EINVAL` → "repeat float is refused (EINVAL)".

**Case T5 `test-topn-exact-forced-hash-ties`** (creates and drops `tqa`, `tqg`, `tqh`):
- With `agg_topn_test_force_decline(1)`: the three T1 requests → "forced hash desc ties: page is 30 then 10", "forced hash asc ties: page is 10 then 20", "forced hash under cap: page is 30, 10, 20" (same expected strings as T1); and `agg(tqa,k,[{"fn":"count","alias":"n"},{"fn":"sum","field":"k","alias":"s"}],n,desc,2)` → "forced hash mixed ties: exact page" — equals `[{"k":"30","n":6,"s":180},{"k":"10","n":5,"s":50}]\n`.
- With the seam off, the same mixed request → "stream mixed ties: stream served", "stream mixed ties: exact page" (same string).
- Multi-field ties (hash path; multi-field `group_by` never streams): object `tqg`, fields `["a:int","b:varchar:8"]`, no indexes; ten records, two each of `(a,b)` = `(1,"x")`, `(1,"y")`, `(null,"x")`, `(0,"z")`, `(1,null)`. Request `{"mode":"aggregate","dir":"default","object":"tqg","group_by":["a","b"],"aggregates":[{"fn":"count","alias":"n"}],"order_by":"n","order":"desc","limit":10}` → "multi-field ties desc: exact page" — equals `[{"a":"0","b":"z","n":2},{"a":"1","b":"x","n":2},{"a":"1","b":"y","n":2},{"a":"1","b":null,"n":2},{"a":null,"b":"x","n":2}]
`; the same with `"order":"asc"` → "multi-field ties asc: exact page" (same string: every count ties, and ties are not flipped).
- Unset labels (hash record-scan route): object `tqh`, fields `["tm:time"]`, **no indexes** (`"indexes":[]`, so the request can neither stream nor take the indexed group-by route; the record scan labels midnight `""` via `typed_field_to_buf_raw`); six records: `tm` `"00:00:00"` ×2, `"01:00:00"` ×2, `null` ×2. `agg(tqh,tm,N,n,desc,10)` → "unset labels: ordered time aggregate succeeds" (no `"error"`), "unset labels: zero value first, then values, then NULL" (positions: `"tm":""` before `"tm":"01:00:00"` before `"tm":null`; `""` does not encode, so the zero/unset mapping gives it the smallest key).

**Case T6 `test-topn-exact-differential`**: object `tqd`, fields `["i:int","nm:numeric:12,2","t:varchar:16","e:enum(red,green,blue)","ip:ipv4","ts:timestamp","x:double"]`, indexes `["i","nm","t","e:btree","ip","ts","x"]`; 60 records `d00`..`d59`, row `r`: `i` = null if `r%7==3` else `(r*r)%5`; `nm` = `["0.00","1.25","2.50","3.75"][r%4]` (string); `t` = null if `r%9==0` else `["pear","apple","fig","kiwi"][r%4]`; `e` = `["red","green","blue"][r%3]`; `ip` = `"9.0.0.1"` if `r%10==0`, `"0.0.0.0"` if `r%10==5`, else `"10.0.0.<r%6+1>"`; `ts` = `1700000000000 + (r%5)*1000`; `x` = `(r%3)*0.5`.
For each group field `G` with spec list `S(G)` (message token `X`): `i`, `nm`, `ts` → `[n count, c count(G), s sum(G), a avg(G), mn min(G), mx max(G)]`; `x` → `[n count, c count(G), mn min(G), mx max(G)]`; `e`, `ip` → `[n count, c count(G)]`; `t` → none (varchar `count(t)` declines; only `N` runs for `t`). For each `G`, each `S` in {`N`, `S(G)` where defined}, each direction in {desc, asc}, each limit in {1, 3, 10000}: request `agg(tqd,G,S,n,dir,limit)`, plus the same with `total` for limit 3 only; plus `agg(tqd,t,N,n,desc,3,criteria=[{"field":"i","op":"gte","value":"1"}])` (message "diff t/N/desc/3/crit") and `agg(tqd,i,N,n,desc,3,total,criteria=[{"field":"i","op":"gte","value":"1"}])` (message "diff i/N/desc/3/total/crit"). For each request assert "diff <G>/<S name>/<dir>/<limit>[/total][/crit]: stream served" (`runs == 1`) and "…: equals forced-hash output". Run the whole set twice: first inline (assert "setup: inline pass" — `parallel_io_pool_size() == 0 && parallel_pool_size() == 0`), then with pools (at 60 rows the hash path's indexed aggregate stays inline below its 1024-hash threshold, so the second pass guards pool-on dispatch, not concurrent merging): `int own_cpu = parallel_pool_size() == 0; if (own_cpu) parallel_pool_init(4); int own_io = parallel_io_pool_size() == 0; if (own_io) parallel_io_pool_init(4);` with "setup: pools running" and shutdown of owned pools on every exit path; the second pass's messages are prefixed `pool ` (e.g. "pool diff i/N/desc/1: …").

**Case T7 `test-topn-exact-render-failure`** (creates and drops `tqc`): `agg_topn_test_reset(); agg_topn_test_fail_repeat(1);` M3 request → "render failure: one error response" (response starts with `{"error"`), "render failure: no partial page" (no `[` in the response), "render failure: the stream was the path that failed" (`runs == 0 && declines == 0 && repeats == 1`).

**Green proof:** all eight cases pass, and `test-agg-total-streaming-topn` passes with its updated assertion; also run `test-agg-topn-stream` cases (`rtk ./build/bin/shard-db-test run-all --filter topn --jobs 4`) and every case whose name contains `agg-exact` or `agg-packed` (record names from `list`), → 0 failed.

**Mutation proofs (hand-revert, no git writes).** First `rtk proxy git diff > <scratch>/plan-i-full.patch`. Apply each to production code only, build, run **only** the named cases, record the failing set, restore exactly, rebuild. A mutation may also break cases it does not name (e.g. mG and mI also change T3/T5 mixed pages); those are deliberately not run for it. Expected sets exactly; mismatch → halt.
- **mA** (heap ties ignored): in `topn_worse` replace everything after the `if (am != bm) return …;` line (the four statements `size_t n …`, `int c …`, `if (c) …`, `return al > bl;`) with `return 0;`. Run T1, T3, T5 → T1 {"tie desc: page is 30 then 10", "tie asc: page is 10 then 20", "tie under cap: page is 30, 10, 20"}; T3 {"total: exact group count and tie order"}; T5 {"stream mixed ties: exact page"} (the heap then behaves like base: mixed desc 2 yields `[30,20]`).
- **mB** (hash tie reversed): in `agg_tie_row_cmp` change `return agg_tie_cmp(` to `return -agg_tie_cmp(`. Run T5 → {"forced hash desc ties: page is 30 then 10", "forced hash asc ties: page is 10 then 20", "forced hash under cap: page is 30, 10, 20", "forced hash mixed ties: exact page", "multi-field ties desc: exact page", "multi-field ties asc: exact page", "unset labels: zero value first, then values, then NULL"} (ties 10/20 swap: `[30,20]`, `[20,10]`, `[30,20,10]`, `[30,20]`; the multi-field page reverses; the time page becomes NULL, `01:00:00`, `""`).
- **mL** (zero/unset mapping removed): delete the `else if (tf && (tf->type == FT_IPV4 …)) { … }` branch in `agg_tie_key_build`. Run T5 → {"unset labels: zero value first, then values, then NULL"} (`""` falls to the `0x01` tier: page `01:00:00`, `""`, NULL; "ordered time aggregate succeeds" still passes).
- **mC** (float sum streams): delete the `if (gns.cls == AGC_FLT && …) return -2;` line. Run T2 → {"float sum: declines to the hash path", "float sum: equals forced-hash output"} (the stream runs; 2E skips `agg_num_repeat` for FLT, so the single `f` group renders `s` as one offer, 1.5, where the hash path sums 30 rows to 45; no error is raised).
- **mD** (count of another field streams): delete `if (s->field[0] && strcmp(s->field, group_by_field) != 0) return -2;`. Run T2 → {"count of another field: declines to the hash path", "count of another field: equals forced-hash output"} (the NULL `h` group is in the page and the stream renders `c2` 0 where the hash path counts 6).
- **mE** (ranking by sum streams): delete the `if (specs[order_spec_idx].fn != AGG_COUNT …) return -2;` line. Run T2 → {"order by sum: declines to the hash path", "order by sum: equals forced-hash output"} (the int sum metric is never accumulated, so every group ranks 0 and the page is `1, 2, 3`, not the hash path's `3, 1, 2`).
- **mK** (presence types stream count(g)): delete the line `if (s->field[0] && (!gtf || agg_count_inspects_presence(gtf->type))) return -2;`. Run T2 → {"count of an unset-capable group: declines to the hash path", "count of an unset-capable group: equals forced-hash output"} (page `""`(3), `a`(2), NULL(1): the stream renders `c` 3 for the `""` group, the hash path 0).
- **mF** (total stays null): restore the old `OUT(want_total ? "],\"total\":null}\n" : "]\n");`. Run T3 → {"total: exact group count and tie order", "total mixed: equals forced-hash output"}; run `test-agg-total-streaming-topn` → {"agg streaming topN → total = exact distinct-tag count (20)"}.
- **mG** (NULL group counted for count(g)): change `(s->field[0] && null_group) ? 0 : counts[i]` to `counts[i]`. Run T2 → {"mixed int: response is the exact expected page", "mixed int: equals forced-hash output"} (NULL row `c` becomes 6).
- **mH** (repeat off by one): in `agg_num_repeat`'s INT branch multiply by `(agg_i128)(times - 1)`. Run T4 → {"repeat int v=7 matches repeated offers", "repeat int v=-3 matches repeated offers", "repeat int overflow reports ERANGE like repeated offers"} (v=0 unaffected; 3 × 2^125 does not overflow).
- **mI** (stream count wrong): in 2E change `agg_cell_count(cell, v)` to `agg_cell_count(cell, v + 1)`. Run T6 → every "…: equals forced-hash output" assertion of T6 (both passes) fails and every "…: stream served" passes (every request has `n` and a non-empty page).
- **mJ** (render error ignored): in 2E change the seam's `rerr = ENOMEM; break;` to `;` (keep the reset of `g_topn_fail_repeat_at`). Run T7 → {"render failure: one error response", "render failure: no partial page", "render failure: the stream was the path that failed"}.
- Restore all: `rtk proxy git diff | diff - <scratch>/plan-i-full.patch` prints nothing; rebuild; all eight cases green.

### Phase 3 — docs (same change)
1. `docs/query-protocol/aggregate.md`: add a parameter-table row `| \`total\` | bool | false | Wrap the result as \`{"rows":[...],"total":<distinct groups before limit>}\`. |` after the `limit` row; in the `## Cost` bullet "**With `group_by`**", append "— except eligible count-ranked top-N requests, which stream (see Streaming top-N)"; in the parameter table row for `order_by`, append "Groups with equal sort values are ordered by group value ascending (numeric order for numbers, byte order for text, declaration order for enum, address order for IPs, time order for dates/timestamps), NULL group last, field by field; the direction does not flip this tie order." Add a section `## Streaming top-N` before `## Limitations`: when the request has a single btree-indexed `group_by` field, `order_by` naming a `count(*)` alias, `limit` 1–10000, no `having`, not `format:"csv"`, criteria fully answerable from indexes, and every aggregate is `count(*)`, `count(<group field>)` (not on varchar/date/datetime/datetimems/time/uuid, whose `count` excludes unset values), or `sum/avg/min/max(<group field>)` (not `sum`/`avg` on `float`/`double`), the aggregate streams the group field's index and keeps only the top `limit` groups in memory; results (values, ties, `total`) are identical to the general path, except that a midnight `time` group is labelled `""` by the general path's record-scan route but `"00:00:00"` from the index (pre-existing; it only arises when the general path cannot use the index); anything else uses the general path; counts above 2^53 rows per group are outside the exact range of the ranking.
2. `docs/reference/changelog.md` `## Unreleased`: append a paragraph "**Count-ranked top-N aggregates stream again with exact extras; tie order is defined.**" — what streams now, the tie order rule (visible change: tied groups previously came out in an unspecified order), and the stream's `total` is now the exact group count instead of `null`; no wire/CLI/config/on-disk change.
3. Judge and record (no edit expected): `docs/concepts/indexes.md`, `docs/operations/tuning.md` hits for `aggregate` (`rtk proxy grep -n -i aggregate` on both).

### Phase 4 — full suite and hand-back
1. `rtk ./build/bin/shard-db-test run-all --jobs 4`; real exit status; cleanup. Expected `N0 + 8` cases, 0 failed. If `test-topn-stream-bitmap-postfilter-bounded` fails, do **not** rerun to green: paste the `# bitmap-postfilter response:` diagnostic line, stop and report it as a reproduction.
2. Definition of done: no new warnings; no TODO/debug output; mutation restore check empty; `rtk proxy git status --short` shows only `build.sh`, `src/db/query_aggregate.c`, `src/db/agg_exact.c`, `src/db/agg_exact.h`, `src/db/query_internal.h`, `src/test/cases/test_agg_topn_stream.c` (diagnostic line only), `src/test/cases/test_find_with_total.c` (2D′ only), `docs/query-protocol/aggregate.md`, `docs/reference/changelog.md`, new `src/test/cases/test_agg_topn_exact.c`, plus untracked plan/evidence files. Nothing committed.
3. Hand back: diff stat, base-red/green/mutation outputs, full-suite totals, deviations, and the note: "This diff changes the top-N heap ordering, stream rendering and the aggregate sort comparator; the branch ASan+UBSan and TSan gate is now due (after all review fixes, before push)."
