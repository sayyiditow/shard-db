# Plan H — unordered indexed find fetches only the record batches its page needs

Supersedes the short draft `docs/plans/2026-11-08-plan-h-bounded-indexed-find-batches.md` (kept unchanged as history; re-derived against HEAD `d61935b`).

## Brief

**Goal:** a small unordered page served from an index candidate set (a `KeySet`) reads only the record batches needed to fill it, keeps at most one batch of record copies in memory, and reports fetch/allocation failures instead of returning a shorter "successful" page.

**Problem and root cause:** `keyset_emit_find` (`src/db/query.c`) works in two phases. Phase 1 copies every candidate hash into one array, then calls `slotcask_bulk_resolve_and_fetch` in 1,024-hash slices until **all** candidates are fetched; `kef_fetch_cb` copies every found record into whole-candidate arrays (and finds its slot with a linear search over **all** candidates). Phase 2 then applies exclusions, the criteria re-check, joins and `offset`/`limit`. So a 10-row page over 3,000 candidates fetches and holds all 3,000 records, with no memory cap. The same function also hides failures: an allocation failure or a missing registry handle returns `0` (an empty, successful page); the fetch call's return value is ignored; and a failed per-record copy leaves a NULL record marked "found".

**Logical flow (after the change):**
1. Caller builds the `KeySet` exactly as today (unchanged).
2. `keyset_emit_find` walks the KeySet buckets in their existing order, collecting up to 1,024 candidates into one reusable batch.
3. It fetches that batch; the callback copies each found record into the batch slot (found by binary search over the batch's sorted hashes) and charges its bytes to a per-call budget (`QUERY_BUFFER_MB`).
4. If the batch's copies do not fit the budget, it frees them and retries the same candidates with half the batch size (down to one; one record that alone exceeds the budget fails the query with `ENOBUFS`).
5. It emits from the batch in bucket order with today's per-row logic (exclusions → criteria → joins → offset → emit), frees the batch, and fetches the next batch **only if** the page is not yet full, no error occurred and the deadline has not passed.
6. Any fetch, allocation, registry or callback failure returns `-1` with `errno` (today's mid-stream error contract: the response is left unterminated), never a short success.
7. The A3 trigram `starts_with` branch in `cmd_find_do_impl`, which today ignores `keyset_emit_find`'s result and closes the envelope, now propagates `-1` like every other caller.
8. TEST_BUILD only: counters and fault seams in `keyset_emit_find`, a verbatim copy of the old implementation as a differential reference, and a wrapper that runs either on a test-built KeySet.

**Verified facts (code-read 2026-11-12, HEAD `d61935b`):**
- `keyset_emit_find` has 6 call sites, all in `query.c`: `idx_find_parallel`, `find_via_trigram_starts_with`, `keyset_find_from_or`, `keyset_find_from_intersect`, and two in `cmd_find_do_impl` (prepared bitmap non-stream route; trigram `build_keyset_from_trigram` route). All but the A3 branch's caller propagate `-1`. None needs a count from `keyset_emit_find`: every `total` is computed elsewhere (`keyset_size`, `idx_count_for_leaf`, `prep.total`, `fp_compute_total`), so stopping early cannot change `total`.
- Reachable when: OR union (always), AND intersection or single indexed leaf when the request has a join, `format:"rows"`, or `offset+limit > 1000` (otherwise `idx_find_streaming` takes it), trigram `contains`/`starts_with` variants, prepared bitmap non-stream routes. `limit ≤ 0` is normalized to `GLOBAL_LIMIT` (default 100,000) before dispatch; `keyset_emit_find` itself treats `limit ≤ 0` as unlimited.
- Output order today = KeySet bucket order; batching in bucket order keeps page membership and order identical on quiescent data.
- `slotcask_bulk_resolve_and_fetch` (non-checked): callbacks may run **concurrently** on IO-pool threads (one task per kf-shard partition when there are more than 3); a record pointer is valid only during its callback; the callback must not re-enter slotcask/btree; returns `-1` on its own allocation failures; a hash appears at most once per partition; a non-zero callback return only ends the current (sid,fid) segment run — later runs and partitions still call back, so the callback itself must short-circuit after an error. The process-local test DB does **not** start the IO pool (`shard_db_open_internal`), so `parallel_for_io` runs inline there unless a case starts the pool itself (cases 6 and 7 do).
- Mid-stream error contract (`docs/query-protocol/find.md`, "Bitmap-source errors…"): after the response has started, record errors leave it unterminated; `cmd_find_do_impl` returns `-1` without closing the envelope or writing an error object.
- `KeySetBudget` + `keyset_budget_reserve/release` (types.h, keyset.c) is an atomic, thread-safe byte budget already used for candidate memory.
- `slotcask_registry_put(NULL)` is tolerated, so `SDB_REG_REF` on a NULL handle is safe.
- `compute_hash_raw` (types.h) and the `KeySet` struct are public; `compute_record_shard` is `static inline` in types.h.

**Decisions made:**

| # | Decision | Alternatives | Why | Tag |
|---|---|---|---|---|
| 1 | Bound record fetches only; KeySet/candidate collection stays as today. | Also stream index candidate discovery. | The requested fix; candidate streaming is a planner redesign. | DEFAULT |
| 2 | Fixed first batch of 1,024 candidates (KeySet bucket order), the same size as today's fetch slices. | Adaptive selectivity-based size. | Isolates correctness; adaptive sizing needs benchmarks you run. | DEFAULT |
| 3 | Record copies of one batch are charged to a per-call budget of `QUERY_BUFFER_MB` (a new, independent collection site, like the existing ones). A batch that does not fit is retried with half the candidates, down to one; the reduced size is kept for the rest of the request. One record larger than `QUERY_BUFFER_MB` fails the query with `ENOBUFS`. | (a) Fail on the first overflow. (b) No byte cap (count bound only). | (b) leaves memory bounded only by 1,024 × max record size; (a) would fail queries that work today whenever 1,024 large records exceed the budget. Halving keeps them working at the cost of at most 10 wasted fetches. **New failure mode:** a single record larger than `QUERY_BUFFER_MB` (default 256 MiB) used to be returned; it now fails. | NEEDS YOU → approved by you 2026-11-12 |
| 4 | Setup-allocation, registry, fetch (`-1`) and per-record copy failures now fail the query (`-1`/`errno`; unterminated response per the existing contract) instead of returning an empty or shorter successful page. | Keep today's silent success. | Silent truncation hides data loss; AGENTS/CORE-PROCESS forbid papering over failures. Visible change only when an allocation/I/O failure occurs. | DEFAULT |
| 5 | Keep the non-checked `slotcask_bulk_resolve_and_fetch`; only start honoring its `-1`. | Switch to `slotcask_bulk_resolve_and_fetch_checked`. | The checked variant turns unreadable/stale references into errors — a read-semantics change outside this plan. The non-checked resolve phase's own silent allocation miss inside slotcask stays (Out of scope). | DEFAULT |
| 6 | Per-row join and emit budgets stay `QUERY_BUFFER_MB − charge`, as today (not reduced by the batch's bytes). | Share one running budget across batch and joins. | Matches how every other collection site is capped; avoids new join failures. | DEFAULT |
| 7 | The A3 trigram `starts_with` caller propagates `-1`. | Leave it ignoring the result. | Otherwise the newly reported failures (decisions 3–4) become silently truncated "complete" pages on that route. | DEFAULT |
| 8 | TEST_BUILD keeps a verbatim copy of the old function as `keyset_emit_find_reference`, used only by a test wrapper for differential tests (~110 test-only lines). | Hand-written expected outputs only. | You asked for a retained reference path; byte-for-byte comparison on the same KeySet is the strongest equivalence check. | DEFAULT |
| 9 | Tests drive `keyset_emit_find` directly on a KeySet the test builds in a fixed order (deterministic bucket order, so "survivors only after the first batch" is exact), plus end-to-end route tests through `find`. | End-to-end only. | A find-built KeySet's bucket order depends on parallel insertion order; exact batch placement cannot be arranged through `find`. | DEFAULT |

**Blast radius:** `src/db/query.c` only in production code: `KefFetchCtx`/`kef_fetch_cb` are replaced by `KefBatch`/`kef_batch_cb`/`kef_batch_release`; `keyset_emit_find`'s body is rewritten with an unchanged signature, so its 6 call sites are untouched except the A3 branch in `cmd_find_do_impl` (one `if (rc < 0)` return). TEST_BUILD-only additions in `query.c` and `src/db/query_internal.h`. New test file + one `build.sh` line. No wire, CLI, config or on-disk change. Observable changes: lower memory and fewer record reads for early-filled pages; earlier stop after a timeout; failures reported instead of short pages (decision 4); `ENOBUFS` for one record larger than `QUERY_BUFFER_MB` (decision 3). Order and membership of pages on quiescent data are unchanged. Not affected: ordered/cursor finds, `idx_find_streaming`, `prepared_bitmap_find_stream`, full scans, count/aggregate (including the similar `or_count` batch loop).

**Test strategy** (new file `src/test/cases/test_kef_batches.c`, 9 cases, process-local DB):
- Early stop: survivors only in batch 2 of 3 → 2 fetch calls, 2,048 copies, peak one batch of copies; exhaustion still reads all 3.
- Missing (never inserted) and deleted candidates with an offset → page filled in batch 2 of 4; only live candidates copied.
- Exclusions and inner joins rejecting all of batch 1 → 2 fetches; left-join misses stay eligible → 1 fetch.
- Byte budget: split from 1,024 → 512 → 256 with correct rows and peak ≤ budget; one record over budget → `ENOBUFS` after 11 attempts.
- Fault injection: setup allocation, record-copy allocation, fetch, join, registry failures → `-1` with the right `errno`, no later fetch, no live copy; timeout → no fetch after it.
- Differential (with the IO pool started, so callbacks run concurrently): 15 parameter sets (offset/limit edges incl. unlimited and `INT_MAX`, criteria, formats, exclusions, joins, NULL values, empty KeySet) byte-identical to the reference.
- Wide object (`splits: 512`, candidates in kf shards ≥ 256, IO pool started): early stop and reference equality.
- Route: real `find` (`format:"rows"`, and with a left join) enters `keyset_emit_find` once and fetches one batch for a 10-row page.
- Route: A3 trigram `starts_with` with an injected fetch failure leaves the page unterminated.
Every case has a base-red run with its exact failing set (or is labelled base-green-by-design) and at least one hand-revert mutation with its exact failing set.

**Out of scope:** other silent truncations on the trigram routes — `find_via_trigram_starts_with` returning 0 when its `keyset_new` fails, and the `if (tg_ks)` trigram branch of `cmd_find_do_impl` treating a NULL KeySet as an empty result; adaptive batch sizing; candidate-set streaming; ordered/cursor finds; planner changes; snapshot semantics; making mid-stream errors visible as an error object; `slotcask` resolve-phase allocation failures that it reports as misses; the non-checked vs checked fetch choice for other callers; the `or_count`, aggregate and count batch loops; benchmarks (you run them).

**Review log:**
- Round 1: 0 blockers, 2 majors, 12 minors. Majors: (1) the budget override and seams could survive into later cases under `run-all --jobs 1` — every case now resets on every exit path; (2) the "process-local DB starts the IO pool" fact was false (`shard_db_open_internal` starts no pools; `parallel_for_io` runs inline) — fact corrected, cases 6 and 7 now start the IO pool so the concurrent callback path is tested. Minors fixed: stale-claim grep excludes `docs/plans`; 2A lists the intended differences; `kef_run` takes the KeySet (D15); `errno = 0` before the registry lookup; the wrong `g_random_seq_ratio` rationale removed (single-leaf finds skip estimation); docs say this site's overflow is a mid-stream failure, not the `QUERY_BUFFER_ERR` object, and note the separate budget; the vacuous oversized "no copy" assertion is now `copies == 0` (added to the base-red set); callback-return semantics stated; sanitizer gate noted in hand-back; `rtk proxy grep` for counts; wrapper redirects `g_out` before setup; remaining trigram silent truncations listed out of scope. Recomputed base-red sets and m1–m11 were all confirmed correct.
- Round 2 (fresh reviewer): **0 blockers, 0 majors → converged.** Re-verified anchors, prototypes, slotcask claims, callback concurrency, request shapes, routes, every base-red and mutation set, and `-Wall -Wextra` cleanliness. 8 minors fixed: bulk response format confirmed from `query_bulk.c`; old comment blocks deleted explicitly; new assertion "copy alloc: copying stopped after the failure" + mutation m12 for the callback's error short-circuit (m6 set updated); concurrent split run in case 7 (budget overflow and halving on the IO pool); case 9 setup assertions and a goto-cleanup rule for failed setup; peak-memory docs wording; `_Static_assert` on the `uint16_t` slot index; guard assertions labelled. Noted, not changed: the ~60 KiB `KefBatch` `calloc` per non-empty call — worth watching in your bench run on 1-candidate finds.

**Approval:** you approved the Brief as is on 2026-11-12 (decision 3: halving budget; decision 4: failures reported). Executor: GLM 5.3 Flash; work left uncommitted.

---

## Plan (executor-facing)

### Execution rules
- Branch `fix/type-correctness-recovery`, HEAD `d61935b`, tracked tree clean (only untracked plan/evidence files). Record `rtk proxy git rev-parse HEAD` and `rtk proxy git status --short` first in the evidence file. If a tracked file is modified, halt and ask.
- No git writes (no add/commit/stash/checkout/restore), no benchmarks, no sanitizer builds. Every shell command starts with `rtk`.
- Build `rtk proxy env SKIP_TESTS=1 ./build.sh`; single case `rtk ./build/bin/shard-db-test run <name>`; full suite `rtk ./build/bin/shard-db-test run-all --jobs 4`. Record real exit statuses. After **every** test invocation, once it has exited: `rtk proxy rm -rf /tmp/shard-db-*`.
- `<scratch>` = `/tmp/claude-1000/-home-hashim-junk-system-git-workspace-sayyiditow-shard-db/8d2fa8b4-e5ea-4b71-8c14-d33e619fdb67/scratchpad` (create if missing).
- Evidence file: `docs/plans/2026-11-12-plan-h-execution-evidence.md` (untracked). Paste real outputs (TAP lines for every proof). Never weaken, delete or skip an assertion. This plan changes **no existing assertion**; if an existing test fails, halt and report.
- Anchor/assumption mismatch → write `PLAN_NOTES-planh.md` describing it and halt. A symbol merely moved with an unambiguous site → proceed and record it. A decision this plan does not cover → stop and ask.
- Do the phases in order. Leave everything uncommitted.

### Phase 0 — audits (no edits)
Use `rtk proxy grep` (not `rtk grep`, which hides matches) for every count in this phase.
1. `rtk proxy grep -n 'static int keyset_emit_find(const char \*db_root, const char \*object,' src/db/query.c` → exactly 2 hits (forward declaration above `idx_find_parallel`; definition after `kef_fetch_cb`). `rtk proxy grep -c '} KefFetchCtx;' src/db/query.c` → 1. `rtk proxy grep -c 'static int kef_fetch_cb(' src/db/query.c` → 1. `rtk proxy grep -c 'KEF_BATCH' src/db/query.c` → 3 (all inside `keyset_emit_find`). `rtk proxy grep -c 'KefBatch\|kef_batch_\|keyset_emit_find_reference\|kef_test_' src/db/query.c src/db/query_internal.h` → 0 each.
2. `rtk proxy grep -n 'keyset_emit_find(' src/db/query.c` → the 2 declaration/definition lines plus exactly 6 calls, inside: `idx_find_parallel`, `find_via_trigram_starts_with`, `keyset_find_from_or`, `keyset_find_from_intersect`, `cmd_find_do_impl` (prepared-bitmap `prc = keyset_emit_find(` and trigram `rc = keyset_emit_find(`). Record each caller's handling of a negative return.
3. In `cmd_find_do_impl`, find the A3 branch: `if (pc->op == OP_STARTS_WITH && !has_joins && !rows_fmt && !csv_delim) {` followed by a bare `find_via_trigram_starts_with(` statement whose result is discarded, then the envelope-closing `OUT(...)` and `return 0;`. Confirm `find_via_trigram_starts_with` returns `keyset_emit_find`'s `rc`.
4. Read `slotcask_bulk_resolve_and_fetch` and `slotcask_bulk_fetch_resolved` (slotcask.c) and confirm: returns `-1` on its allocation failures, `0` otherwise; callbacks run via `parallel_for_io` when more than 3 partitions; a non-zero callback return ends only the current (sid,fid) segment run without error (later runs/partitions still call back). Confirm `test_init_process_db` (`embedded.c`) uses `shard_db_open_internal`, which does not start the IO pool, and that `parallel_io_pool_init/shutdown/size` are declared in types.h. Confirm `slotcask_registry_put` tolerates NULL (slotcask.h comment "NULL tolerated").
5. Confirm prototypes exist and record their headers: `keyset_budget_reserve`, `keyset_budget_release`, `KeySetBudget` (types.h); `parse_criteria_tree`, `free_criteria_tree`, `compile_criteria_tree_checked`, `now_ms_coarse`, `objlock_rdlock/rdunlock`, `parse_excluded_keys`, `free_excluded`, `load_schema`, `init_field_schema`, `compute_hash_raw`, `compute_record_shard` (types.h); `validate_criteria_tree_fields`, `parse_joins_json`, `resolve_joins`, `free_joins`, `resolve_join_record`, `typed_query_emit_row` (query_internal.h). `query_internal.h` has an `#ifdef TEST_BUILD` block at its top.
6. Read `src/test/cases/test_planner_cost_model.c` (process-local DB setup via `test_get_process_db`/`tu_pdb_request`), `src/test/cases/test_parallel_cross_pool_nesting.c` (IO pool start/stop in a case) and `src/test/cases/test_alloc_error_propagation.c` (includes for `query_internal.h`). `build.sh` has exactly one line `    src/test/cases/test_keyset.c \`.
7. Confirm in `src/db/query_bulk.c` (the `OUT` format strings, not the docs, which show a space) that `bulk-insert` answers `{"inserted":%d...` and `bulk-delete` by `keys` answers `{"deleted":%d}` with no space after the colon.
8. Baseline: build; `rtk ./build/bin/shard-db-test list | wc -l` → record as `N0`.

### Phase 1 — instrumentation, reference, wrapper, tests (test-first)

**1A. Reference copy (do this before any other edit).** Copy the current `KefFetchCtx` typedef, `kef_fetch_cb` and `keyset_emit_find` definition **verbatim** into a new block placed immediately after the production `keyset_emit_find` definition (before `/* OR index-union find:`), wrapped in `#ifdef TEST_BUILD` / `#endif`, renaming: `KefFetchCtx` → `KefRefFetchCtx`, `kef_fetch_cb` → `kef_ref_fetch_cb`, `keyset_emit_find` → `keyset_emit_find_reference`, `KEF_BATCH` → `KEF_REF_BATCH` (its `#define`, uses and `#undef`). No other change to the copy; it gets no counters or seams. Add above it the comment `/* TEST_BUILD only: the pre-Plan-H implementation, kept verbatim as the differential reference for test_kef_batches.c. */`.

**1B. Seam/counter block.** Insert in `query.c` immediately before the production `typedef struct {` that ends in `} KefFetchCtx;`:
```c
/* ---- keyset_emit_find test counters and fault seams (TEST_BUILD only) ---- */
#ifdef TEST_BUILD
static struct {
    _Atomic long calls, fetch_calls, fetch_hashes, copies, copy_bytes,
                 live_copies, live_bytes, peak_bytes;
} g_kef_stats;
static _Atomic long g_kef_seam_at[KEF_SEAM_COUNT];   /* 0 = disarmed; n = fire on the n-th hit */
static _Atomic long g_kef_seam_hits[KEF_SEAM_COUNT];
static _Atomic long g_kef_budget_override;           /* > 0: record-copy budget in bytes */

void kef_test_reset(void) {
    atomic_store(&g_kef_stats.calls, 0);       atomic_store(&g_kef_stats.fetch_calls, 0);
    atomic_store(&g_kef_stats.fetch_hashes, 0); atomic_store(&g_kef_stats.copies, 0);
    atomic_store(&g_kef_stats.copy_bytes, 0);  atomic_store(&g_kef_stats.live_copies, 0);
    atomic_store(&g_kef_stats.live_bytes, 0);  atomic_store(&g_kef_stats.peak_bytes, 0);
    for (int i = 0; i < KEF_SEAM_COUNT; i++) {
        atomic_store(&g_kef_seam_at[i], 0); atomic_store(&g_kef_seam_hits[i], 0);
    }
    atomic_store(&g_kef_budget_override, 0);
}
void kef_test_arm(int seam, long arg) {
    if (seam == KEF_SEAM_BUDGET) { atomic_store(&g_kef_budget_override, arg); return; }
    if (seam <= 0 || seam >= KEF_SEAM_COUNT) return;
    atomic_store(&g_kef_seam_hits[seam], 0);
    atomic_store(&g_kef_seam_at[seam], arg);
}
void kef_test_stats(KefTestStats *o) {
    o->calls = atomic_load(&g_kef_stats.calls);             o->fetch_calls = atomic_load(&g_kef_stats.fetch_calls);
    o->fetch_hashes = atomic_load(&g_kef_stats.fetch_hashes); o->copies = atomic_load(&g_kef_stats.copies);
    o->copy_bytes = atomic_load(&g_kef_stats.copy_bytes);   o->live_copies = atomic_load(&g_kef_stats.live_copies);
    o->live_bytes = atomic_load(&g_kef_stats.live_bytes);   o->peak_bytes = atomic_load(&g_kef_stats.peak_bytes);
}
static int kef_seam_take(int seam) {
    long at = atomic_load(&g_kef_seam_at[seam]);
    if (at <= 0) return 0;
    long hit = atomic_fetch_add(&g_kef_seam_hits[seam], 1) + 1;
    if (hit != at) return 0;
    atomic_store(&g_kef_seam_at[seam], 0);              /* one-shot */
    return 1;
}
static void kef_stat_copy(size_t bytes) {
    atomic_fetch_add(&g_kef_stats.copies, 1);
    atomic_fetch_add(&g_kef_stats.copy_bytes, (long)bytes);
    atomic_fetch_add(&g_kef_stats.live_copies, 1);
    long now = atomic_fetch_add(&g_kef_stats.live_bytes, (long)bytes) + (long)bytes;
    long peak = atomic_load(&g_kef_stats.peak_bytes);
    while (peak < now && !atomic_compare_exchange_weak(&g_kef_stats.peak_bytes, &peak, now)) {}
}
static void kef_stat_release(size_t bytes) {
    atomic_fetch_sub(&g_kef_stats.live_copies, 1);
    atomic_fetch_sub(&g_kef_stats.live_bytes, (long)bytes);
}
#define KEF_SEAM(s)          kef_seam_take(s)
#define KEF_STAT_ADD(f, v)   ((void)atomic_fetch_add(&g_kef_stats.f, (long)(v)))
#define KEF_STAT_COPY(b)     kef_stat_copy(b)
#define KEF_STAT_RELEASE(b)  kef_stat_release(b)
#else
#define KEF_SEAM(s)          0
#define KEF_STAT_ADD(f, v)   ((void)0)
#define KEF_STAT_COPY(b)     ((void)0)
#define KEF_STAT_RELEASE(b)  ((void)0)
#endif
```
In `src/db/query_internal.h`, inside the existing top `#ifdef TEST_BUILD` block, append:
```c
/* keyset_emit_find counters and one-shot fault seams (query.c, Plan H).
   kef_test_arm(seam, n): fire on the n-th hit after arming (n >= 1);
   KEF_SEAM_BUDGET instead sets the record-copy budget in bytes (0 = default).
   SETUP_ALLOC: the call's first scratch allocation fails (ENOMEM).
   COPY_ALLOC: a record-copy allocation fails (ENOMEM).
   FETCH: a fetch attempt is not issued and reports -1/EIO.
   JOIN: a join resolution reports -1/EIO.  REGISTRY: the registry lookup
   yields NULL/EIO.  TIMEOUT: after a fetch attempt returns, the deadline is
   marked timed out.  Counters are process-wide; kef_test_reset() zeroes
   them and disarms every seam. */
enum { KEF_SEAM_SETUP_ALLOC = 1, KEF_SEAM_COPY_ALLOC, KEF_SEAM_FETCH, KEF_SEAM_JOIN,
       KEF_SEAM_REGISTRY, KEF_SEAM_TIMEOUT, KEF_SEAM_BUDGET, KEF_SEAM_COUNT };
typedef struct {
    long calls, fetch_calls, fetch_hashes, copies, copy_bytes,
         live_copies, live_bytes, peak_bytes;
} KefTestStats;
void kef_test_reset(void);
void kef_test_arm(int seam, long arg);
void kef_test_stats(KefTestStats *out);
/* Runs keyset_emit_find (use_reference = 0) or the retained pre-Plan-H
   implementation (use_reference = 1) on `ks`, writing rows (no envelope) to
   `out`. eff_root = <db_root>/<dir>; object is bare. criteria_json/
   excluded_csv/format/join_json may be NULL. Returns the function's rc with
   its errno; -1/EINVAL for bad arguments or criteria/join setup. */
int keyset_emit_find_for_test(const char *eff_root, const char *object, KeySet *ks,
        const char *criteria_json, const char *excluded_csv, int offset, int limit,
        const char *format, const char *join_json, uint32_t timeout_ms,
        int use_reference, FILE *out);
```

**1C. Instrument the current production function (base behavior unchanged in non-TEST builds).** In the production `kef_fetch_cb` / `keyset_emit_find` (not the reference copy):
1. First statement of `keyset_emit_find`: `KEF_STAT_ADD(calls, 1);`
2. `uint8_t (*hashes)[16] = malloc(n_fetch * sizeof(*hashes));` → `uint8_t (*hashes)[16] = KEF_SEAM(KEF_SEAM_SETUP_ALLOC) ? NULL : malloc(n_fetch * sizeof(*hashes));`
3. `SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &sinfo);` → `SlotcaskDb *sdb SDB_REG_REF = KEF_SEAM(KEF_SEAM_REGISTRY) ? (errno = EIO, (SlotcaskDb *)NULL) : slotcask_registry_get(db_root, object, &sinfo);`
4. In `kef_fetch_cb`, after `c->found[i] = 1;`: `if (c->keys[i] && c->vals[i]) KEF_STAT_COPY(klen + vlen);`
5. Replace the body of the fetch `while (processed < n_fetch)` loop's call with:
```c
            KEF_STAT_ADD(fetch_calls, 1);
            KEF_STAT_ADD(fetch_hashes, batch);
            if (!KEF_SEAM(KEF_SEAM_FETCH))
                slotcask_bulk_resolve_and_fetch(sdb, hashes + processed,
                                                 batch, &fctx, kef_fetch_cb);
            if (KEF_SEAM(KEF_SEAM_TIMEOUT) && dl)
                atomic_store_explicit(&dl->timed_out, 1, memory_order_relaxed);
```
(The `processed += batch;` line stays.) This is the `slotcask_bulk_resolve_and_fetch(sdb, hashes + processed,` call **inside `keyset_emit_find`**; the textually identical call in the `or_count` path (`CB_BATCH` loop) is not touched.
6. `int found=resolve_join_record(&joins[i],...` → `int found=KEF_SEAM(KEF_SEAM_JOIN)?(errno=EIO,-1):resolve_join_record(&joins[i],...` (arguments unchanged).
7. In the final cleanup loop `for (size_t i = 0; i < n_fetch; i++) { free(fkeys[i]); free(fvals[i]); }` add first in the body: `if (fkeys[i] && fvals[i]) KEF_STAT_RELEASE(fklens[i] + fvlens[i]);`
`KEF_SEAM_COPY_ALLOC` and `KEF_SEAM_BUDGET` are not consulted by the base code (arming them is a no-op until Phase 2).

**1D. Wrapper.** After the reference block (still inside a `#ifdef TEST_BUILD`), add:
```c
int keyset_emit_find_for_test(const char *eff_root, const char *object, KeySet *ks,
        const char *criteria_json, const char *excluded_csv, int offset, int limit,
        const char *format, const char *join_json, uint32_t timeout_ms,
        int use_reference, FILE *out) {
    if (!eff_root || !object || !ks || !out) { errno = EINVAL; return -1; }
    FILE *saved = g_out;
    g_out = out;                       /* setup errors printed via OUT land in `out`, not the TAP stream */
    Schema sch = load_schema(eff_root, object);
    FieldSchema fs;
    init_field_schema(&fs, eff_root, object);
    CriteriaNode *tree = NULL;
    if (criteria_json) {
        const char *perr = NULL;
        char verr[256];
        tree = parse_criteria_tree(criteria_json, &perr);
        if (perr || !tree ||
            validate_criteria_tree_fields(tree, fs.ts, verr, sizeof(verr)) < 0 ||
            compile_criteria_tree_checked(tree, fs.ts, NULL, 0)) {
            free_criteria_tree(tree); g_out = saved; errno = EINVAL; return -1;
        }
    }
    JoinSpec *joins = NULL; int njoins = 0;
    if (join_json && (parse_joins_json(join_json, &joins, &njoins) < 0 ||
                      resolve_joins(joins, njoins, eff_root, object, &fs) < 0)) {
        free_joins(joins, njoins); free_criteria_tree(tree); g_out = saved; errno = EINVAL; return -1;
    }
    ExcludedKeys excluded = parse_excluded_keys(excluded_csv);
    int rows_fmt = format && strcmp(format, "rows") == 0;
    int dict_fmt = format && strcmp(format, "dict") == 0;
    char csv_delim = (format && strcmp(format, "csv") == 0) ? ',' : 0;
    QueryDeadline dl = { now_ms_coarse(), timeout_ms, 0 };
    if (objlock_rdlock(eff_root, object) != 0) {
        free_excluded(&excluded); free_joins(joins, njoins); free_criteria_tree(tree);
        g_out = saved; errno = ENOMEM; return -1;
    }
    int rc = use_reference
        ? keyset_emit_find_reference(eff_root, object, &sch, ks, tree, &excluded, offset, limit,
                                     NULL, 0, &fs, rows_fmt, dict_fmt, csv_delim, joins, njoins, &dl)
        : keyset_emit_find(eff_root, object, &sch, ks, tree, &excluded, offset, limit,
                           NULL, 0, &fs, rows_fmt, dict_fmt, csv_delim, joins, njoins, &dl);
    int e = errno;
    g_out = saved;
    objlock_rdunlock(eff_root, object);
    free_excluded(&excluded); free_joins(joins, njoins); free_criteria_tree(tree);
    errno = e;
    return rc;
}
```
If a helper name or signature differs from Phase 0 item 5, adapt to the real one and record it. The production find path is unchanged by 1A–1D except the TEST_BUILD-only seams.

**1E. Test file `src/test/cases/test_kef_batches.c`**, registered in `build.sh` as a new line `    src/test/cases/test_kef_batches.c \` immediately after `    src/test/cases/test_keyset.c \`. Includes: `test_runner.h`, `test_assert.h`, `fixtures.h`, `../db/types.h`, `../db/query_internal.h`, `../db/shard_db.h`; system `<errno.h> <limits.h> <stdint.h> <stdio.h> <stdlib.h> <string.h>`. All cases are in-process (`test_get_process_db()`), drop every object they create on every path, and call `kef_test_reset()` before each measured call **and on every exit path** (one cleanup label per case, so no seam or budget override survives into later cases when `run-all --jobs 1` runs every case in one process). Cases 6 and 7 start the IO pool so fetch callbacks run concurrently: `int own_pool = parallel_io_pool_size() == 0; if (own_pool) parallel_io_pool_init(4);` then `ASSERT_TRUE(parallel_io_pool_size() > 0, "setup: IO pool running")`, and `if (own_pool) parallel_io_pool_shutdown();` on every exit path. All other cases run the callback inline (deterministic seam hit order). On any failed `setup:` assertion, `goto cleanup` and return 1 (cleanup = seam reset, pool stop if owned, object drops, frees).

Fixture (write as static helpers):
```c
typedef struct { uint8_t h[16]; int i; } KHash;
typedef int (*KefKindFn)(int ord);           /* 0 live, 1 never inserted, 2 inserted then deleted */
typedef int (*KefNFn)(int ord);
typedef const char *(*KefRefFn)(int ord);    /* NULL -> JSON null */
typedef struct {
    ShardDb *db; char eff[600]; char obj[24]; KeySet *ks;
    int ncand; int *idx_of_ord; int remote;
} KefFix;
```
- Key of candidate index `i`: `snprintf(buf, 8, "k%04d", i)` (5 bytes). `kef_ord_key(f, o, buf)` = key of `f->idx_of_ord[o]`. `kef_has(text, f, o)` = `text && strstr(text, "\"<key of o>\"")`.
- `kef_fix_build(f, obj, splits, ncand, kind, nval, refv, remote)`:
  1. `db = test_get_process_db()`; `add-dir` `default` (ignore response); `eff` = `<test_get_process_db_root()>/default`; `obj` copied.
  2. `{"mode":"create-object","dir":"default","object":"<obj>","splits":<splits>,"max_key":16,"fields":["n:int","tag:varchar:8","ref:varchar:8"],"indexes":["tag"]}` → `ASSERT_TRUE(no "\"error\"", "setup: create <obj>")` (build the message with snprintf).
  3. If `remote`: create `<obj>_r` (`splits` 8, `max_key` 16, `fields` `["name:varchar:8"]`, no indexes) and `{"mode":"insert","dir":"default","object":"<obj>_r","key":"r1","value":{"name":"x"}}`; assert no error ("setup: remote").
  4. `ks = keyset_new(ncand)`; for `i = 0..ncand-1` in order: `compute_hash_raw(key_i, 5, h)`, `keyset_insert(ks, h)`, store `{h, i}` in a `KHash` array; `qsort` it by `memcmp(h, 16)`.
  5. Ordinals: `o = 0`; for `b = 0..ks->cap-1` with `ks->state[b] == 2`: `bsearch` `ks->keys[b]` in the sorted array → `idx_of_ord[o++] = i`. `ASSERT_EQ_INT(o, ncand, "setup: <obj> keyset holds every candidate")`.
  6. Bulk-insert one JSON array `{"mode":"bulk-insert","dir":"default","object":"<obj>","records":[{"key":"<k>","value":{"n":<nval(o)>,"tag":"hot","ref":"<refv(o)>"|null}},...]}` for every ordinal with `kind(o) != 1` (heap buffer, `ncand*96+256` bytes); assert the response contains `"inserted":<count>` ("setup: <obj> bulk insert").
  7. If any `kind(o) == 2`: `{"mode":"bulk-delete","dir":"default","object":"<obj>","keys":[...]}` for those; assert contains `"deleted":<count>` ("setup: <obj> bulk delete").
- `kef_fix_free(f)`: `keyset_free`, free arrays, `tu_pdb_drop_object` for `<obj>` and (if remote) `<obj>_r`.
- `kef_run(f, ks, use_ref, crit, excl, off, lim, fmt, join, tmo, &rc, &err)`: `open_memstream` → `keyset_emit_find_for_test(f->eff, f->obj, ks, ...)` (callers pass `f->ks` except D15) → `err = errno` → `fclose` → return the text (`""` when nothing emitted; caller frees).
- Join JSON, built per fixture: `JI` = `[{"object":"<obj>_r","local":"ref","remote":"key","as":"r","type":"inner","fields":["name"]}]`; `JL` = same with `"type":"left"`.
- `CRIT_N1` = `[{"field":"n","op":"eq","value":"1"}]`.
- "same as reference" assertion: run `kef_run(..., use_ref=1, same args)`; assert `rc_new == rc_ref && (rc_new >= 0 || err_new == err_ref) && strcmp(out_new, out_ref) == 0`.
- "no copy outlives the call": `st.live_copies == 0 && st.live_bytes == 0 && st.copies > 0` (the `copies > 0` part keeps it from passing vacuously). Where a scenario copies nothing (setup alloc, registry), use the "nothing fetched or emitted" assertion instead.
- `errno` assertions are written `rc == -1 && err == E` (so they cannot pass by accident on a success).
- `B` (bytes per copy): `st.copy_bytes / st.copies`, with the setup assertion `st.copies > 0 && st.copy_bytes % st.copies == 0` ("setup: uniform record copies"). All records in cases 1, 4 have the same size (int, `"hot"`, two-letter `ref`).

Cases (messages must match exactly; `o` = ordinal, `S(o)` = key of ordinal `o`):

**Case 1 `test-kef-early-stop-after-first-batch`** — object `kfe`, splits 8, 3000 candidates, all live, `n = (1100 <= o && o <= 1199)`, `ref = "zz"`, no remote.
- Run new: `CRIT_N1`, offset 0, limit 10. Then reference with the same arguments.
  - "early stop: rc is the page size" — `rc == 10`
  - "early stop: page equals the reference page"
  - "early stop: page starts at the first survivor" — `kef_has(out, 1100)`
  - "early stop: page ends at the tenth survivor" — `kef_has(out, 1109) && !kef_has(out, 1110)`
  - "early stop: two fetch calls" — `fetch_calls == 2`
  - "early stop: 2048 hashes requested" — `fetch_hashes == 2048`
  - "early stop: 2048 records copied" — `copies == 2048`
  - "setup: uniform record copies"
  - "early stop: retained copies peak at one batch" — `peak_bytes == 1024 * B`
  - "early stop: no copy outlives the call"
- Run new: `CRIT_N1`, offset 0, limit 101 (only 100 survivors), then reference.
  - "exhaustion: every survivor returned" — `rc == 100`
  - "exhaustion: page equals the reference page"
  - "exhaustion: all three batches fetched" — `fetch_calls == 3`

**Case 2 `test-kef-stale-candidates-offset`** — object `kfs`, splits 8, 3200 candidates, `kind = o%16==5 ? 1 : o%16==9 ? 2 : 0` (200 never inserted, 200 deleted, 2800 live), `n = 1`, `ref = "zz"`.
- Hand derivation: live offsets within each block of 16 are `L = {0,1,2,3,4,6,7,8,10,11,12,13,14,15}` (14 per block). Survivor `#k` (1-based) is at ordinal `16*((k-1)/14) + L[(k-1)%14]`: `#1000` → 1142, `#1001` → 1143, `#1100` → 1256 (batch 2 = ordinals 1024–2047). Live candidates in batches 1–2 = 128 blocks × 14 = 1792. Batches: 1024, 1024, 1024, 128.
- Run new: no criteria, offset 1000, limit 100; then reference.
  - "stale: rc is the page size" — `rc == 100`
  - "stale: page equals the reference page"
  - "stale: page starts at survivor 1001" — `kef_has(out, 1143) && !kef_has(out, 1142)`
  - "stale: page filled in the second batch" — `fetch_calls == 2`
  - "stale: 2048 hashes requested" — `fetch_hashes == 2048`
  - "stale: only live candidates of two batches copied" — `copies == 1792`
  - "stale: no copy outlives the call"

**Case 3 `test-kef-exclusions-and-joins`** — object `kfj`, splits 8, 3000 candidates, all live, `n = 1`, `ref = o < 1024 ? "zz" : "r1"`, remote yes.
- Exclusions: `excl` = comma-joined keys of ordinals 0..1023; no criteria, offset 0, limit 5; then reference.
  - "exclusions: rc is the page size" — `rc == 5`
  - "exclusions: page equals the reference page"
  - "exclusions: page starts after the excluded batch" — `kef_has(out, 1024)`
  - "exclusions: two fetch calls" — `fetch_calls == 2`
- Inner join: `JI`, no exclusions, offset 0, limit 5; then reference.
  - "inner join: rc is the page size", "inner join: page equals the reference page", "inner join: page starts in the second batch" (`kef_has(out, 1024)`), "inner join: two fetch calls" (`fetch_calls == 2`)
- Left join: `JL`, offset 0, limit 5; then reference.
  - "left join: rc is the page size", "left join: page equals the reference page", "left join: misses stay eligible" (`kef_has(out, 0)`), "left join: one fetch call" (`fetch_calls == 1`)

**Case 4 `test-kef-byte-budget-split`** — object `kfb`, splits 8, 3000 candidates, all live, `n = 1`, `ref = "zz"`.
- Calibrate: new, no criteria, offset 0, limit 1 → "setup: uniform record copies"; record `B`.
- Split: `kef_test_reset(); kef_test_arm(KEF_SEAM_BUDGET, 300 * B);` new, no criteria, offset 0, limit 600; read stats; then (after `kef_test_reset()`) reference with the same arguments. Derivation: 1024·B and 512·B exceed 300·B, 256·B fits; 600 rows need 3 batches of 256 → attempts 1024, 512, 256, 256, 256.
  - "split: rc is the page size" — `rc == 600`
  - "split: page equals the reference page"
  - "split: five fetch attempts" — `fetch_calls == 5`
  - "split: 2304 hashes requested" — `fetch_hashes == 2304`
  - "split: retained copies stay within the budget" — `peak_bytes <= 300 * B`
  - "split: no copy outlives the call"
- Oversized: `kef_test_reset(); kef_test_arm(KEF_SEAM_BUDGET, B - 1);` new, no criteria, offset 0, limit 10. Derivation: attempts 1024, 512, …, 2, 1 (11), hashes 2047, then `ENOBUFS`.
  - "oversized record: rc is -1"
  - "oversized record: errno is ENOBUFS"
  - "oversized record: eleven fetch attempts" — `fetch_calls == 11`
  - "oversized record: 2047 hashes requested" — `fetch_hashes == 2047`
  - "oversized record: no rows emitted" — `out[0] == '\0'`
  - "oversized record: nothing was ever copied" — `copies == 0 && live_copies == 0 && live_bytes == 0` (every record is larger than `B - 1`, so every reservation fails before a copy)

**Case 5 `test-kef-failure-injection`** — object `kff`, splits 8, 3000 candidates, all live, `n = 1`, `ref = "r1"`, remote yes. Each scenario: `kef_test_reset()`, arm, run new, read stats.
- Setup alloc: arm `KEF_SEAM_SETUP_ALLOC, 1`; no criteria, offset 0, limit 10.
  - "setup alloc: rc is -1", "setup alloc: errno is ENOMEM", "setup alloc: nothing fetched or emitted" (`fetch_calls == 0 && out[0] == '\0'`)
- Fetch: arm `KEF_SEAM_FETCH, 2`; no criteria, offset 0, limit 1500.
  - "fetch failure: rc is -1", "fetch failure: errno is EIO", "fetch failure: rows of the first batch stay emitted" (`kef_has(out, 1023)`), "fetch failure: no row after the failed batch" (`!kef_has(out, 2048)`), "fetch failure: two fetch attempts" (`fetch_calls == 2`), "fetch failure: only the first batch copied" (`copies == 1024`), "fetch failure: no copy outlives the call"
- Join: arm `KEF_SEAM_JOIN, 3`; `JL`, offset 0, limit 10.
  - "join failure: rc is -1", "join failure: errno is EIO", "join failure: rows before the failing join emitted" (`kef_has(out, 1) && !kef_has(out, 2)`), "join failure: one fetch call" (`fetch_calls == 1`), "join failure: only the first batch copied" (`copies == 1024`), "join failure: no copy outlives the call"
- Registry: arm `KEF_SEAM_REGISTRY, 1`; no criteria, offset 0, limit 10.
  - "registry failure: rc is -1", "registry failure: errno is EIO", "registry failure: nothing fetched or emitted"
- Timeout: arm `KEF_SEAM_TIMEOUT, 1`; `timeout_ms = 600000`; no criteria, offset 0, limit 10.
  - "timeout: rc is 0" (`rc == 0`), "timeout: nothing emitted" (`out[0] == '\0'`), "timeout: one fetch call" (`fetch_calls == 1`), "timeout: only the first batch copied" (`copies == 1024`), "timeout: no copy outlives the call"
- Copy alloc: arm `KEF_SEAM_COPY_ALLOC, 5`; no criteria, offset 0, limit 10.
  - "copy alloc: rc is -1", "copy alloc: errno is ENOMEM", "copy alloc: no rows emitted" (`out[0] == '\0'`), "copy alloc: one fetch call" (`fetch_calls == 1`), "copy alloc: copying stopped after the failure" (`copies == 4`: callbacks run inline in this case, so copies 1–4 succeed, the 5th fails and every later callback returns early on the error flag), "copy alloc: no copy outlives the call" (`live_copies == 0 && live_bytes == 0`)

**Case 6 `test-kef-differential`** — object `kfd`, splits 8, 3200 candidates, `kind` as case 2, `n = (o%3 == 0)`, `ref = o%5==4 ? NULL : (o%2==0 ? "r1" : "zz")`, remote yes. Each row: new and reference with identical arguments; message `"differential: <label>"` using the "same as reference" condition.

| Label | criteria | offset | limit | format | excluded | join |
|---|---|---|---|---|---|---|
| D1 all 0+10 | — | 0 | 10 | — | — | — |
| D2 all 1020+10 | — | 1020 | 10 | — | — | — |
| D3 all unlimited | — | 0 | 0 | — | — | — |
| D4 all tail | — | 2790 | 50 | — | — | — |
| D5 offset past end | — | 5000 | 10 | — | — | — |
| D6 int max | — | INT_MAX-5 | INT_MAX | — | — | — |
| D7 n1 0+10 | CRIT_N1 | 0 | 10 | — | — | — |
| D8 n1 tail | CRIT_N1 | 900 | 40 | — | — | — |
| D9 rows | — | 0 | 5 | rows | — | — |
| D10 dict | — | 0 | 5 | dict | — | — |
| D11 csv | — | 0 | 5 | csv | — | — |
| D12 excluded head | — | 0 | 5 | — | keys of o 0..99 | — |
| D13 inner | — | 0 | 10 | — | — | JI |
| D14 left | — | 0 | 10 | — | — | JL |

- D15: a second KeySet `keyset_new(16)` with no inserts, run new and reference (no criteria, 0, 10): "differential: D15 empty keyset"; and "empty keyset: nothing fetched" (`fetch_calls == 0` for the new run).

**Case 7 `test-kef-wide-splits`** — object `kfw`, **splits 512**, 3000 candidates, all live, `n` and `ref` as case 1.
- "setup: candidates in kf shards >= 256" — count of ordinals `o < 2048` with `compute_record_shard(hash(o), 512) >= 256` is `> 0`.
- New, `CRIT_N1`, offset 0, limit 10; then reference: "wide splits: rc is the page size" (`rc == 10`), "wide splits: page equals the reference page", "wide splits: two fetch calls" (`fetch_calls == 2`), "wide splits: no copy outlives the call"; then "setup: uniform record copies" from this run's stats and record `B`.
- Concurrent split (pool running): `kef_test_reset(); kef_test_arm(KEF_SEAM_BUDGET, 300 * B);` new, no criteria, offset 0, limit 600; read stats; `kef_test_reset()`; reference with the same arguments. Assert "wide split: page equals the reference page", "wide split: retained copies stay within the budget" (`peak_bytes <= 300 * B`; holds under concurrency because the reservation precedes the copy), "wide split: no copy outlives the call". Attempt and hash counts are deliberately not asserted here.

**Case 8 `test-kef-find-route-early-stop`** — object `kfr`, splits 8, 3000 candidates, all live, `n = 1`, `ref = "r1"`, remote yes. (No planner tuning: a single-leaf find takes `skip_est_single` in `plan_filter` and seeds FP_PRIMARY_LEAF without a cardinality estimate; the `calls == 1` assertions guard the route.)
- `kef_test_reset()`; `{"mode":"find","dir":"default","object":"kfr","criteria":[{"field":"tag","op":"eq","value":"hot"}],"format":"rows","limit":10}`.
  - "route: rows find succeeded" — no `"error"` and contains `"rows":[`
  - "route: find entered keyset_emit_find once" — `calls == 1`
  - "route: ten-row page fetched one batch" — `fetch_calls == 1`
  - "route: one batch copied" — `copies == 1024`
- `kef_test_reset()`; same request with `"join":` `JL` (and no `format`).
  - "join route: join find succeeded" — no `"error"` and contains `"rows":[`
  - "join route: find entered keyset_emit_find once" — `calls == 1`
  - "join route: one fetch call" — `fetch_calls == 1`

**Case 9 `test-kef-trigram-starts-error`** — own object `kft`: `splits` 8, `max_key` 16, `fields` `["name:varchar:16"]`, `indexes` `["name:trigram"]`; bulk-insert 50 records `t00`..`t49` with `name` `"abcdef%02d"`; assert "setup: kft create" (no `"error"`) and "setup: kft bulk insert" (contains `"inserted":50`). Request `Q` = `{"mode":"find","dir":"default","object":"kft","criteria":[{"field":"name","op":"starts_with","value":"abc"}],"limit":10}`.
- Control: `kef_test_reset()`; `Q` → "trigram route: control find returns rows" (contains `abcdef`).
- `kef_test_reset(); kef_test_arm(KEF_SEAM_FETCH, 1);` `Q`:
  - "trigram route: find entered keyset_emit_find once" — `calls == 1`
  - "trigram route: the fetch seam fired" — `fetch_calls == 1`
  - "trigram route: fetch failure leaves the page unterminated" — response non-NULL and `strchr(resp, ']') == NULL`

**1F. Base-red proof (Phase 1 done, Phase 2 not started).** Build; confirm `list | wc -l` = `N0 + 9`; run each new case singly, `rm -rf /tmp/shard-db-*` after each. Expected failing sets **exactly** (all other assertions pass; any other set → halt):
- Case 1: {"early stop: two fetch calls", "early stop: 2048 hashes requested", "early stop: 2048 records copied", "early stop: retained copies peak at one batch"} (base fetches 3, requests 3000, copies 3000, peak 3000·B).
- Case 2: {"stale: page filled in the second batch", "stale: 2048 hashes requested", "stale: only live candidates of two batches copied"} (base 4, 3200, 2800).
- Case 3: {"exclusions: two fetch calls", "inner join: two fetch calls", "left join: one fetch call"} (base 3 each).
- Case 4: {"split: five fetch attempts", "split: 2304 hashes requested", "split: retained copies stay within the budget", "oversized record: rc is -1", "oversized record: errno is ENOBUFS", "oversized record: eleven fetch attempts", "oversized record: 2047 hashes requested", "oversized record: no rows emitted", "oversized record: nothing was ever copied"} (base ignores the budget seam and copies all 3000).
- Case 5: {"setup alloc: rc is -1", "setup alloc: errno is ENOMEM", "fetch failure: rc is -1", "fetch failure: errno is EIO", "fetch failure: no row after the failed batch", "fetch failure: two fetch attempts", "fetch failure: only the first batch copied", "join failure: one fetch call", "join failure: only the first batch copied", "registry failure: rc is -1", "registry failure: errno is EIO", "timeout: one fetch call", "timeout: only the first batch copied", "copy alloc: rc is -1", "copy alloc: errno is ENOMEM", "copy alloc: no rows emitted", "copy alloc: one fetch call", "copy alloc: copying stopped after the failure"}. (Base: setup alloc/registry return 0; the skipped fetch drops batch 2 and batch 3 is emitted — rc 1500, copies 1024+952 = 1976; joins/timeout still fetch all 3 batches; the copy seam is a no-op so 10 rows are emitted after 3 fetches.)
- Case 6: none (both paths are the same code on base — green by design; proven by mutation m3).
- Case 7: {"wide splits: two fetch calls", "wide split: retained copies stay within the budget"}.
- Case 8: {"route: ten-row page fetched one batch", "route: one batch copied", "join route: one fetch call"}.
- Case 9: {"trigram route: fetch failure leaves the page unterminated"} (base returns 0 rows and the caller closes `[]`).
Paste each TAP output. Guard assertions that pass on base and under every mutation (they pin behaviour that must not regress but prove nothing about this change) are: "exhaustion: all three batches fetched", "fetch failure: rows of the first batch stay emitted", "join failure: rows before the failing join emitted", "timeout: rc is 0", "timeout: nothing emitted", "setup alloc: nothing fetched or emitted", "registry failure: nothing fetched or emitted", "copy alloc: no copy outlives the call"; label them "guard" in the evidence file.

### Phase 2 — production change

**2A. Replace** the production `typedef struct { ... } KefFetchCtx;`, `kef_fetch_cb` and the production `keyset_emit_find` definition (not the forward declaration, not the reference copy, not the seam block) with:
```c
#define KEF_BATCH 1024
_Static_assert(KEF_BATCH <= 65536, "KefSortEnt.idx is uint16_t");

typedef struct { uint8_t hash[16]; uint16_t idx; } KefSortEnt;

/* One reusable fetch batch. Candidate slots 0..n-1 are in KeySet bucket order. */
typedef struct {
    uint8_t      hashes[KEF_BATCH][16];
    KefSortEnt   sorted[KEF_BATCH];     /* same hashes sorted, for kef_batch_cb's lookup */
    uint8_t     *rec[KEF_BATCH];        /* owned copy: key bytes then value bytes */
    size_t       klen[KEF_BATCH];
    size_t       vlen[KEF_BATCH];
    uint8_t      hdr2[KEF_BATCH];
    uint8_t      found[KEF_BATCH];
    size_t       n;
    KeySetBudget budget;                /* live record-copy bytes of this batch */
    _Atomic int  error;                 /* first callback failure: ENOBUFS or ENOMEM */
} KefBatch;

static int kef_sort_cmp(const void *a, const void *b) {
    return memcmp(((const KefSortEnt *)a)->hash, ((const KefSortEnt *)b)->hash, 16);
}

static void kef_set_error(KefBatch *b, int e) {
    int expect = 0;
    atomic_compare_exchange_strong(&b->error, &expect, e);
}

static size_t kef_copy_budget(void) {
#ifdef TEST_BUILD
    long o = atomic_load(&g_kef_budget_override);
    if (o > 0) return (size_t)o;
#endif
    return g_query_buffer_max_bytes;
}

/* Fetch callback. Runs on fetch-pool threads, possibly concurrently for
   different kf shards: every candidate owns its slot and shared state is
   atomic. A hash repeats only within one partition (one thread), so the
   found[] repeat check needs no lock. Copies the borrowed record; never
   re-enters slotcask/btree (a kf reader is held). */
static int kef_batch_cb(const uint8_t hash16[16], const void *key, size_t klen,
                        const void *value, size_t vlen, uint8_t hdr2, void *ctx_ptr) {
    KefBatch *b = (KefBatch *)ctx_ptr;
    if (atomic_load_explicit(&b->error, memory_order_relaxed)) return 1;
    size_t lo = 0, hi = b->n, slot = SIZE_MAX;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int c = memcmp(hash16, b->sorted[mid].hash, 16);
        if (c == 0) { slot = b->sorted[mid].idx; break; }
        if (c < 0) hi = mid; else lo = mid + 1;
    }
    if (slot == SIZE_MAX || b->found[slot]) return 0;   /* not ours, or a repeat: first copy wins */
    if (vlen > SIZE_MAX - klen) { kef_set_error(b, ENOBUFS); return 1; }
    size_t bytes = klen + vlen;
    if (keyset_budget_reserve(&b->budget, bytes) != 0) { kef_set_error(b, ENOBUFS); return 1; }
    uint8_t *p = KEF_SEAM(KEF_SEAM_COPY_ALLOC) ? NULL : malloc(bytes ? bytes : 1);
    if (!p) { keyset_budget_release(&b->budget, bytes); kef_set_error(b, ENOMEM); return 1; }
    if (klen) memcpy(p, key, klen);
    if (vlen) memcpy(p + klen, value, vlen);
    b->rec[slot] = p; b->klen[slot] = klen; b->vlen[slot] = vlen; b->hdr2[slot] = hdr2;
    b->found[slot] = 1;
    KEF_STAT_COPY(bytes);
    return 0;
}

/* Free this batch's copies and reset it for the next collection. */
static void kef_batch_release(KefBatch *b) {
    for (size_t i = 0; i < b->n; i++) {
        if (b->rec[i]) {
            size_t bytes = b->klen[i] + b->vlen[i];
            KEF_STAT_RELEASE(bytes);
            keyset_budget_release(&b->budget, bytes);
            free(b->rec[i]);
            b->rec[i] = NULL;
        }
        b->found[i] = 0;
    }
    b->n = 0;
    atomic_store_explicit(&b->error, 0, memory_order_relaxed);
}

/* Walk a pre-built KeySet in bucket order, one fetch batch at a time: fetch
   up to KEF_BATCH candidates, filter (exclusions, rematch, joins), paginate
   and emit them, free the batch, and fetch the next batch only while the
   page is not full. Holds at most one batch of record copies, charged to
   QUERY_BUFFER_MB; a batch that does not fit is retried at half size, and a
   single record over the budget fails with ENOBUFS. Returns rows printed, or
   -1 with errno on any fetch/allocation/record/join/emit failure (rows
   already printed stay printed). A deadline stop returns the rows printed.
   Used by the OR-union, AND-intersection, trigram, prepared-bitmap and
   indexed-leaf find paths; caller owns the KeySet. */
static int keyset_emit_find(const char *db_root, const char *object,
                            const Schema *sch, KeySet *ks,
                            CriteriaNode *tree_for_rematch,  /* NULL = skip */
                            ExcludedKeys *excluded, int offset, int limit,
                            const char **proj_fields, int proj_count,
                            FieldSchema *fs, int rows_fmt, int dict_fmt, char csv_delim,
                            JoinSpec *joins, int njoins, QueryDeadline *dl) {
    KEF_STAT_ADD(calls, 1);
    int need_rematch = (tree_for_rematch != NULL);
    CriteriaNode *tree = tree_for_rematch;

    size_t cursor = 0;                       /* next KeySet bucket to collect from */
    while (cursor < ks->cap && ks->state[cursor] != 2) cursor++;
    if (cursor == ks->cap) return 0;         /* no candidate: no allocation, no registry */

    KefBatch *bt = KEF_SEAM(KEF_SEAM_SETUP_ALLOC) ? NULL : calloc(1, sizeof(*bt));
    if (!bt) { errno = ENOMEM; return -1; }
    bt->budget.limit = kef_copy_budget();

    SlotcaskSchemaInfo sinfo = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    errno = 0;
    SlotcaskDb *sdb SDB_REG_REF = KEF_SEAM(KEF_SEAM_REGISTRY)
        ? (errno = EIO, (SlotcaskDb *)NULL)
        : slotcask_registry_get(db_root, object, &sinfo);
    if (!sdb) { int e = errno ? errno : EIO; free(bt); errno = e; return -1; }

    const Schema *kschema = fs && fs->auto_key != AK_NONE ? &fs->auto_key_schema_snapshot : NULL;
    size_t batch_cap = KEF_BATCH;            /* halved on budget overflow; never grows back */
    int printed = 0, count = 0, dl_counter = 0, error = 0, stop = 0;

    while (!stop && !error && cursor < ks->cap) {
        if (dl && dl->timeout_ms &&
            atomic_load_explicit(&dl->timed_out, memory_order_relaxed)) break;
        size_t next = cursor;
        bt->n = 0;
        while (next < ks->cap && bt->n < batch_cap) {
            if (ks->state[next] == 2) {
                memcpy(bt->hashes[bt->n], ks->keys[next], 16);
                memcpy(bt->sorted[bt->n].hash, ks->keys[next], 16);
                bt->sorted[bt->n].idx = (uint16_t)bt->n;
                bt->n++;
            }
            next++;
        }
        if (bt->n == 0) break;               /* only empty buckets remain */
        qsort(bt->sorted, bt->n, sizeof(bt->sorted[0]), kef_sort_cmp);

        KEF_STAT_ADD(fetch_calls, 1);
        KEF_STAT_ADD(fetch_hashes, bt->n);
        int frc;
        if (KEF_SEAM(KEF_SEAM_FETCH)) { errno = EIO; frc = -1; }
        else {
            errno = 0;
            frc = slotcask_bulk_resolve_and_fetch(sdb, (const uint8_t (*)[16])bt->hashes,
                                                  bt->n, bt, kef_batch_cb);
        }
        int ferr = errno;
        if (KEF_SEAM(KEF_SEAM_TIMEOUT) && dl)
            atomic_store_explicit(&dl->timed_out, 1, memory_order_relaxed);

        int cerr = atomic_load_explicit(&bt->error, memory_order_relaxed);
        size_t used = bt->n;
        if (cerr == ENOBUFS && used > 1) {   /* copies do not fit: retry these candidates at half size */
            kef_batch_release(bt);
            batch_cap = used / 2;
            continue;
        }
        if (cerr || frc < 0) {
            error = cerr ? cerr : (ferr ? ferr : EIO);
            kef_batch_release(bt);
            break;
        }
        cursor = next;

        for (size_t i = 0; i < bt->n; i++) {
            if (limit > 0 && printed >= limit) { stop = 1; break; }
            if (query_deadline_tick(dl, &dl_counter)) { stop = 1; break; }
            if (!bt->found[i]) continue;
            char keybuf[1100];
            if (format_wire_key(kschema, (const char *)bt->rec[i], bt->klen[i],
                                keybuf, sizeof(keybuf)) < 0) { error = EILSEQ; break; }
            if (is_excluded(excluded, keybuf)) continue;
            const uint8_t *raw = bt->rec[i] + bt->klen[i];
            size_t value_len = bt->vlen[i];
            uint8_t hdr2 = bt->hdr2[i];
            int matched = 1;
            if (need_rematch &&
                criteria_match_tree_checked(raw, value_len, tree, fs, hdr2, &matched)) {
                error = errno ? errno : EILSEQ; break;
            }
            if (!matched) continue;
            RecordRef *refs = NULL; int dropped = 0;
            size_t charge = (size_t)njoins * sizeof(*refs);
            if (njoins) {
                if (charge > g_query_buffer_max_bytes) { error = ENOBUFS; break; }
                refs = calloc((size_t)njoins, sizeof(*refs));
                if (!refs) { error = ENOMEM; break; }
                for (int j = 0; j < njoins; j++) {
                    int found = KEF_SEAM(KEF_SEAM_JOIN) ? (errno = EIO, -1)
                        : resolve_join_record(&joins[j], db_root, fs->ts, raw, value_len, hdr2,
                                              g_query_buffer_max_bytes - charge, &refs[j]);
                    if (found < 0) { error = errno ? errno : EILSEQ; break; }
                    if (!found && joins[j].type == JOIN_INNER) { dropped = 1; break; }
                }
            }
            if (!error && !dropped && ++count > offset) {
                if (typed_query_emit_row(g_out ? g_out : stdout, keybuf, strlen(keybuf), raw,
                        value_len, hdr2, fs, proj_fields, proj_count, rows_fmt, dict_fmt,
                        csv_delim, printed > 0, joins, njoins, refs,
                        g_query_buffer_max_bytes - charge))
                    error = errno ? errno : EILSEQ;
                else printed++;
            }
            if (refs) { for (int j = 0; j < njoins; j++) release_record_ref(&refs[j]); free(refs); }
            if (error) break;
        }
        if (limit > 0 && printed >= limit) stop = 1;
        kef_batch_release(bt);
    }
    free(bt);
    if (error) { errno = error; return -1; }
    return printed;
}
```
Delete both old comment blocks that preceded the old `KefFetchCtx` ("Emit records matching `tree` by iterating a KeySet…" and "Walk a pre-built KeySet, fetch each record…"); the new function comment replaces them. Keep the "(legacy KeysetRecordRead … removed" comment. Check per-row logic against the old body line by line: it must be the same except `fkeys[fi]/fvals[fi]/fklens[fi]/fvlens[fi]/fhdr2s[fi]/ffound[fi]` → `bt->rec[i]`, `bt->klen[i]`, `bt->rec[i] + bt->klen[i]`, `bt->vlen[i]`, `bt->hdr2[i]`, `bt->found[i]`, and the join loop variable renamed `j`. These differences are intended and allowed: `kschema` computed once before the loop; `stop = 1` on the limit and deadline checks; the added pre-fetch deadline check; the deadline tick running once per batch candidate instead of once per bucket (empty buckets no longer tick); the empty-KeySet early return now scanning for the first filled bucket. Any other difference → halt.

**2B. A3 caller.** In `cmd_find_do_impl`'s A3 branch, change the discarded `find_via_trigram_starts_with(...);` statement to `int trc = find_via_trigram_starts_with(...);` (same arguments) followed by `if (trc < 0) { free_excluded(&excluded); free_joins(joins, njoins); return -1; }` before the envelope-closing `OUT`. No other change in that branch.

**2C. Build** with no new warnings (compare warning lines against the Phase 0 baseline build).

**Green proof:** all 9 new cases pass. Also run the existing cases `test-keyset`, `test-joins`, `test-nql-joins`, `test-or-keyset-cap`, `test-and-intersection`, `test-find-with-total`, `test-alloc-error-propagation` (names per `list`; if a name differs, use the case(s) defined in the files `test_keyset.c`, `test_joins.c`, `test_nql_joins.c`, `test_or_keyset_cap.c`, `test_and_intersection.c`, `test_find_with_total.c`, `test_alloc_error_propagation.c` and record them) → green.

**Mutation proofs (hand-revert, no git writes).** First `rtk proxy git diff > <scratch>/plan-h-full.patch`. Apply each mutation to `query.c` only, build, run the named case(s), record the failing set, restore the exact original text, rebuild before the next. Expected failing sets **exactly**; any mismatch → halt.
- **m1** (no stop between batches): in the emit loop change `if (limit > 0 && printed >= limit) { stop = 1; break; }` to `if (limit > 0 && printed >= limit) break;` **and** delete the line `if (limit > 0 && printed >= limit) stop = 1;` after the loop. Run cases 1, 2, 3, 7, 8:
  - case 1 {"early stop: two fetch calls", "early stop: 2048 hashes requested", "early stop: 2048 records copied"}
  - case 2 {"stale: page filled in the second batch", "stale: 2048 hashes requested", "stale: only live candidates of two batches copied"}
  - case 3 {"exclusions: two fetch calls", "inner join: two fetch calls", "left join: one fetch call"}
  - case 7 {"wide splits: two fetch calls"}
  - case 8 {"route: ten-row page fetched one batch", "route: one batch copied", "join route: one fetch call"}
- **m2** (copies kept across batches): in `kef_batch_release` replace the whole `if (b->rec[i]) { ... }` block with `b->rec[i] = NULL;`. Run case 1 → {"early stop: retained copies peak at one batch", "early stop: no copy outlives the call"}.
- **m3** (first candidate of every batch skipped): emit loop `for (size_t i = 0; i < bt->n; i++)` → `for (size_t i = 1; i < bt->n; i++)`. Run case 6. Derivation: ordinals 0, 1024, 2048, 3072 (all live, `o%16 == 0`) vanish; with `CRIT_N1` only 0 and 3072 are survivors. Expected {"differential: D1 all 0+10", "differential: D2 all 1020+10", "differential: D3 all unlimited", "differential: D4 all tail", "differential: D7 n1 0+10", "differential: D8 n1 tail", "differential: D9 rows", "differential: D10 dict", "differential: D11 csv", "differential: D13 inner", "differential: D14 left"}; D5, D6 (empty either way), D12 (page 100, 102, 103, 104, 106 inside batch 1, ordinal 0 excluded anyway) and D15 pass.
- **m4** (no halving): `if (cerr == ENOBUFS && used > 1) {` → `if (0) {`. Run case 4 → {"split: rc is the page size", "split: page equals the reference page", "split: five fetch attempts", "split: 2304 hashes requested", "oversized record: eleven fetch attempts", "oversized record: 2047 hashes requested"} ("oversized record: nothing was ever copied" still passes: the single 1024 attempt copies nothing).
- **m5** (fetch result ignored): `if (cerr || frc < 0) {` → `if (cerr) {`. Run case 5 → {"fetch failure: rc is -1", "fetch failure: errno is EIO", "fetch failure: no row after the failed batch", "fetch failure: two fetch attempts", "fetch failure: only the first batch copied"}.
- **m6** (copy failure ignored): in `kef_batch_cb` change `if (!p) { keyset_budget_release(&b->budget, bytes); kef_set_error(b, ENOMEM); return 1; }` to `if (!p) { keyset_budget_release(&b->budget, bytes); return 0; }`. Run case 5 → {"copy alloc: rc is -1", "copy alloc: errno is ENOMEM", "copy alloc: no rows emitted", "copy alloc: copying stopped after the failure"} (the other 1023 records of batch 1 are copied).
- **m12** (no error short-circuit in the callback): delete the line `if (atomic_load_explicit(&b->error, memory_order_relaxed)) return 1;` at the top of `kef_batch_cb`. Run case 5 → {"copy alloc: copying stopped after the failure"}.
- **m7** (deadline does not stop the walk): emit loop `if (query_deadline_tick(dl, &dl_counter)) { stop = 1; break; }` → `if (query_deadline_tick(dl, &dl_counter)) break;` **and** delete the pre-fetch `if (dl && dl->timeout_ms && atomic_load_explicit(&dl->timed_out, memory_order_relaxed)) break;` statement. Run case 5 → {"timeout: one fetch call", "timeout: only the first batch copied"}.
- **m8** (registry failure silent): `if (!sdb) { int e = errno ? errno : EIO; free(bt); errno = e; return -1; }` → `if (!sdb) { free(bt); return 0; }`. Run case 5 → {"registry failure: rc is -1", "registry failure: errno is EIO"}.
- **m9** (setup failure silent): `if (!bt) { errno = ENOMEM; return -1; }` → `if (!bt) return 0;`. Run case 5 → {"setup alloc: rc is -1", "setup alloc: errno is ENOMEM"}.
- **m10** (error does not stop the walk): `while (!stop && !error && cursor < ks->cap) {` → `while (!stop && cursor < ks->cap) {`. Run case 5 → {"join failure: one fetch call", "join failure: only the first batch copied"} (later batches are fetched; the stored error still returns -1/EIO).
- **m11** (A3 caller ignores the error): undo 2B (discard the result again). Run case 9 → {"trigram route: fetch failure leaves the page unterminated"}. If it passes instead, the request did not take the A3 route — halt.
- Restore all: `rtk proxy git diff | diff - <scratch>/plan-h-full.patch` prints nothing; rebuild; all 9 new cases green. Paste every output; `rm -rf /tmp/shard-db-*` after each run.

### Phase 3 — docs (same change)
1. `docs/query-protocol/find.md`: insert a new section `## Unordered indexed pages` immediately before `## Indexed vs full-scan`, saying: unordered finds served from an index candidate set (OR unions, AND intersections, trigram matches, prepared bitmap candidates, and a single indexed leaf when the request has a `join`, `format:"rows"`, or `offset + limit` above 1,000) fetch candidate records in batches of up to 1,024 in candidate order; each batch is filtered (remaining criteria, `excludedKeys`, inner joins) and paginated before the next is fetched, and fetching stops once `offset + limit` rows have qualified — so a page whose rows are found early reads only the batches it needs, while rare survivors or a large `offset` can still read every candidate; the index walk that collects candidates is unchanged; one batch of record copies counts against `QUERY_BUFFER_MB`, a batch that does not fit is retried at half size, and a single record larger than the budget fails the query; page membership and order are unchanged on quiescent data; record-fetch, allocation and join failures fail the query instead of returning a shorter page, and, like other record errors after the response has started, leave the response unterminated.
2. `docs/reference/limits.md`, row "Per-query intermediate buffers": add "unordered indexed-find record batches" to the parenthesised site list, and the sentence "An indexed-find record batch that does not fit is retried at half size; a single record larger than the budget fails the query — after the response has started, so it does not produce the `query memory buffer exceeded` object: the response is left unterminated, like other mid-stream record errors. This batch budget is separate from the candidate (KeySet) budget and from the per-row join/emit allowance, each up to `QUERY_BUFFER_MB`, so one such query can hold several allowances at once."
3. `docs/concepts/concurrency.md`, the `QUERY_BUFFER_MB` paragraph: add "indexed-find record batches" to its parenthesised site list.
4. `AGENTS.md`, the `QUERY_BUFFER_MB` paragraph under "Per-query memory cap": "9 collection sites checked (" → "10 collection sites checked (" and append ", unordered indexed-find record batches" to the list; after the bitmap-deferred-collector clause add "; an indexed-find record batch that does not fit is halved and retried, and a single record over the budget fails the query mid-stream (unterminated response, no error object)"; and in the following `MAX_CONCURRENT_QUERIES` paragraph change "Worst-case query-buffer RAM = `MAX_CONCURRENT_QUERIES × QUERY_BUFFER_MB`" to "Worst-case query-buffer RAM ≈ `MAX_CONCURRENT_QUERIES × QUERY_BUFFER_MB` per collection site in use (an indexed find can hold its candidate set, one record batch and a per-row join/emit allowance, each up to `QUERY_BUFFER_MB`)".
5. `docs/reference/changelog.md`, `## Unreleased`: re-read it; append one paragraph at the end of the section without touching existing ones: "**Unordered indexed finds fetch only the record batches a page needs.**" — small pages over OR/intersection/trigram/prepared-bitmap candidates (and single-leaf finds with joins, `format:"rows"` or a large offset) now fetch 1,024-candidate batches until the page is full instead of fetching every candidate first, and hold one batch of record copies under `QUERY_BUFFER_MB` (halved on overflow); behaviour changes: a single record larger than `QUERY_BUFFER_MB` now fails the query, and allocation/record-fetch failures now fail the query (unterminated response, as for other mid-stream record errors) instead of returning an empty or shorter page; order and membership of pages are unchanged; no wire, CLI, config or on-disk change.
6. Judge and record (no edit expected): `docs/operations/tuning.md` ("Checked at every collection site"), `docs/reference/error-codes.md` (no new message), `docs/concepts/indexes.md` (grep it for `fetch` and judge each hit).
7. Stale-claim check: `rtk proxy grep -rn '9 collection sites' AGENTS.md docs --exclude-dir=plans` → nothing.

### Phase 4 — full suite and hand-back
1. `rtk ./build/bin/shard-db-test run-all --jobs 4`; real exit status; `rm -rf /tmp/shard-db-*`. Expected: `N0 + 9` cases, **0 failed**. A failure that passes on rerun is a bug to root-cause, never rerun-until-green.
2. Definition of done: no new warnings; no TODO/FIXME/debug output; the mutation restore check is empty; `rtk proxy git status --short` shows only `build.sh`, `src/db/query.c`, `src/db/query_internal.h`, `AGENTS.md`, `docs/query-protocol/find.md`, `docs/reference/limits.md`, `docs/concepts/concurrency.md`, `docs/reference/changelog.md`, new `src/test/cases/test_kef_batches.c`, plus the untracked plan/evidence files. `slotcask.c`, `keyset.c`, `server.c` unmodified. Nothing committed.
3. Hand back: diff stat, base-red / green / mutation outputs, full-suite totals, every deviation and recorded judgement, and the note: "This diff changes shared state touched by IO-pool fetch callbacks; the AGENTS.md ASan+UBSan and TSan gate is still outstanding (branch-level, after Plan I, before push)." 
