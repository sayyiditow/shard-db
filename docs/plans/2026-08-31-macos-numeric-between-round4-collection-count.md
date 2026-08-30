# macOS arm64 numeric BETWEEN — round 4: collection count vs. validated count

Date: 2026-08-31
Status: draft awaiting human approval.
**Diagnosis only — no fix code is authorized by this plan.**
Round-1 record: `docs/plans/2026-08-28-macos-arm64-numeric-between.md`.
Round-2 record: `docs/plans/2026-08-30-macos-numeric-between-upper-layer.md`.
Round-3 record: `docs/plans/2026-08-30-macos-numeric-between-daemon-seam.md`.

## Where round 3 left off (verified, CI 2026-08-30, PR #321, commit `6cb651c`)

- S1 (the real `cmd_count` orchestrator, run in-process against the fixture
  daemon's own `db_root`) reproduces the defect with no wire/daemon
  runtime involved: macOS captured **2**, both Linux legs captured **3**.
  This puts the defect inside `cmd_count_with_tree` itself.
- Seam B (`btree_dispatch`'s OP_BETWEEN trace) fired identically on all
  three legs: `len1=8 lo=7fffffffffffff9c len2=8 hi=8000000000000064
  min_ex=0 max_ex=0`. Bounds encoding is exonerated.
- Seam B's other half — `idx_count_cb_flush_thread_dbg()`, traced from
  `shard_walk_worker` — reported `flushed=0` on **every** shard on **all
  three legs, including macOS**. At the time this was read as
  inconclusive ("does not identify a shard-level loss").

**Re-examined while preparing this plan: that inconclusiveness was
structural, not just bad luck.** `idx_count_cb_flush_thread()` drains
`idx_count_local`, the thread-local batching accumulator used by
`idx_count_cb` (`query.c`, the `count` callback bound via `IdxCountCtx`).
But the code path this specific query actually takes — confirmed below —
never calls `idx_count_cb`. A single BETWEEN leaf with no post-filters and
a non-negatable op takes `cmd_count_with_tree`'s `is_single_leaf` branch
(`query.c:5461-5465`) straight into `idx_count_for_leaf`, whose `IT_BTREE`
branch (`query.c:5664-5685`) collects candidates with `collect_hash_cb`
into a `CollectCtx`, then hands them to `parallel_indexed_count` for
Kf-boundary revalidation. `collect_hash_cb` writes matches via an atomic
slot allocator directly into `CollectCtx.entries`/`CollectCtx.count`
(`query.c:291`) — it never touches `idx_count_local`. So
`idx_count_cb_flush_thread_dbg()` was always going to report `flushed=0`
here, on every platform, regardless of what the real collector did: round
3's seam instrumented an accumulator this query path doesn't use. It
correctly proved the *bound bytes* are right; it proved nothing about how
many candidates the range walk actually collected, or how many survived
revalidation.

That is the actual gap this plan closes.

## Confirmed call path for this exact query (re-derived, not assumed)

`{"mode":"count", ..., "criteria":[{"field":"amt","op":"between",
"value":"-1","value2":"1"}]}` on an object with a single indexed `amt`
field, no other criteria:

1. `cmd_count_with_tree` (`query.c:5360`) — `plan_filter` returns
   `FP_PRIMARY_LEAF` (single indexed rangeable leaf).
2. `op_is_negatable(OP_BETWEEN)` is `false` (`query.c:382-385` only lists
   `NOT_EQUAL/NOT_LIKE/NOT_CONTAINS/NOT_IN/NOT_EXISTS`) and
   `is_single_leaf` is `true` (single `CNODE_LEAF`, zero post-filters) →
   `query.c:5461-5465` calls `idx_count_for_leaf` directly.
3. `idx_count_for_leaf` (`query.c:5621`) — `pick_index_for_leaf` returns
   `IT_BTREE` for a plain (non-composite, non-trigram) numeric field →
   the `IT_BTREE` block at `query.c:5664-5685` runs:
   - `btree_dispatch(...)` fans out over `index_splits_for(splits)` shard
     walkers, each invoking `collect_hash_cb` per matching btree entry.
     Every match is unconditionally collected (`op_is_length(BETWEEN)` is
     false, `op_needs_check_primary(BETWEEN)` is false — confirmed round
     3) via the atomic slot allocator into `col.entries`/`col.count`.
   - `parallel_indexed_count(...)` groups `col.entries` by shard
     (`shard_group_batch`) and runs `shard_count_worker` per group —
     sequentially here since `batch_count` (≤5) is far under the 1024
     threshold at `query.c:1205` — which re-validates each candidate
     against the compiled criterion at the Kf boundary
     (`count_batch_cb` → `criteria_match_tree` → `match_typed`'s
     `FT_NUMERIC` case → `cmp_op_i64`, already shown platform-neutral in
     round 3) and sums `workers[g].count`.
4. `idx_count_for_leaf` returns that sum; `cmd_count_with_tree` writes it
   to the wire.

This is the only path a single-leaf BETWEEN count can take on this
schema — no branch selection is in question. What's untested is which of
step 3's two stages loses the record: the btree range walk /
`collect_hash_cb` collection (`col.count`), or `parallel_indexed_count`'s
Kf-boundary revalidation (the final `cnt`).

## What this plan does

One seam, two trace points, both inside `idx_count_for_leaf`'s `IT_BTREE`
block in `src/db/query.c`:

- **Point 1 (collect)** — immediately after `btree_dispatch` returns,
  before the `budget_exceeded` check: logs `col.count`, the raw candidate
  count the range walk collected, straight off `collect_hash_cb`'s atomic
  counter.
- **Point 2 (validate)** — immediately after `parallel_indexed_count`
  returns: logs `col.count` again alongside `cnt`, the Kf-revalidated
  final count for the same call.

Both log via `LOG_AUDIT` (bypasses the LOG_LEVEL filter, lands in
`audit.log`, captured by the standard test fixture with no db.env
override) under a new tag, `NB2TRACE4`, to keep this round's lines
unambiguous in the shared log (round 3's seams and tag are fully removed
in Task 1 before this round's seam is added, so there is no real
collision risk, but a fresh tag keeps grep patterns honest if any old
log lines linger from a prior run). All seam code is
`#ifdef TEST_BUILD`-guarded — the release binary is untouched — and is
deleted with this plan's close-out.

Round 3's S1 mechanism (real `cmd_count`, run in-process against the
fixture daemon's own `db_root` via `shard_db_open_internal`, output
captured through `g_out`) already proved the defect needs no wire/daemon
runtime to reproduce. This round's probe reuses it unchanged for that
purpose (the `s1`/rc/count assertions), but S1's own `LOG_AUDIT` calls
are **not** relied on for the S2 trace dump: `shard_db_open_internal`
constructs a local instance without starting its logging worker, so its
`LOG_AUDIT` output goes to stderr rather than reliably landing in
`audit.log`. This is harmless — the wire `count` calls (W1, run against
the fixture daemon, which does own a running logging worker) already
exercise `idx_count_for_leaf`'s `IT_BTREE` block three times (between +
two controls) and reliably produce the `NB2TRACE4` pairs S2 reads. S1
stays in the probe only to confirm the in-process return value, not as a
second source of trace lines.

## Suspect state entering round 4

Exonerated: encoding arithmetic, bare-btree insert/walk/range/iter
against both synthetic and real on-disk per-shard files (round 1),
planner plan selection, fan-out dispatch, TLS count batching, hand-built
`btree_dispatch` calls with pools pre-initialized (round 2), encoded
BETWEEN bound bytes, `cmp_op_i64`/`match_typed`'s numeric comparator
(round 3), and the branch-selection question itself (this plan's call-path
re-derivation above — there is only one path for this query shape).

Still live, ranked:

1. **Collection-side loss** — `col.count` itself is 2 on macOS. Points at
   the shard fan-out composition specifically inside `idx_count_for_leaf`
   (as opposed to round 1's B2, which walked the same real per-shard
   `.idx` files directly and got 3 — the difference would have to be in
   how this call assembles/dispatches the walk, e.g. `pick_index_for_leaf`
   or `resolve_idx_field` returning something subtly different for this
   call site, or a race across the `index_splits_for(splits)` shard
   workers particular to this fan-out).
2. **Validation-side loss** — `col.count` is 3, `cnt` is 2.
   `parallel_indexed_count`/`shard_count_worker`'s Kf-boundary revalidation
   (`count_batch_cb`, `slotcask_bulk_fetch_resolved` /
   `slotcask_bulk_resolve_and_fetch`) drops a candidate that collection
   correctly found. `cmp_op_i64` itself is already exonerated, so this
   would localize to the batch record-fetch/resolve machinery, not the
   comparator.
3. **Neither seam fires as expected / an unexpected shape** — e.g.
   `pick_index_for_leaf` doesn't return `IT_BTREE` on macOS for this
   field (contradicts this plan's static call-path derivation; would mean
   the derivation itself is wrong on macOS, which is possible if
   `index.conf`/`field_index_type` reads differently there) → stop and
   ask, do not guess further.

## Embedded execution rules

- Branch: after Task 1's round-3 close-out, create
  `diag/macos-numeric-between-round4` off `main`. Leave work
  **uncommitted** except Task 4's evidence pushes (scratch branch commits
  probe + build.sh/workflow edits + this plan with evidence; PR comments
  are the durable copy).
- Do tasks in order. No fix code: Task 5 halts.
- Build/test commands: `SKIP_TESTS=1 ./build.sh`;
  `./build/bin/shard-db-test run test-numeric-between-probe4`;
  `./build/bin/shard-db-test run-all`.
- If a quoted anchor isn't found exactly, write `PLAN_NOTES.md` describing
  the mismatch and halt the entire execution run immediately — do not
  guess, reinterpret, or continue to any further task, even an unrelated
  one. Resuming requires the human (or the planning model, re-engaged) to
  read `PLAN_NOTES.md`, decide whether it's a stale-anchor problem
  (re-derive and patch the plan) or a wrong-assumption problem (rethink
  the plan), and hand back either a patched or a fresh plan — execution
  never resumes on its own initiative.
- If you hit a decision the plan doesn't cover, stop and ask — do not
  improvise.
- Never weaken or delete an existing test to make a failure disappear.
  The round-4 probe is scaffolding on a scratch branch only; it must
  never reach `main` while red.

## Task 1 — Close out round 3 first (durability already holds; close, don't merge)

Round 3's evidence is already committed and pushed (commit `6cb651c` on
`origin/diag/macos-numeric-between-round3`) and posted to PR #321 — both
Task 4c durability conditions already hold. Only the close-out is left:

1. Confirm PR #321 is still open and unmerged:
   `gh pr view 321 --json state,title`.
2. Close (never merge) PR #321 with a comment pointing at this plan as
   the follow-up:
   ```bash
   gh pr comment 321 --body "Closing per round-3's Task 5 halt — root cause not yet localized. Follow-up: docs/plans/2026-08-31-macos-numeric-between-round4-collection-count.md"
   gh pr close 321
   ```
3. Revert round 3's seams and probe scaffolding from the working tree
   (the branch still carries them from commits `99373b5`/`6cb651c`):
   - `src/db/query.c`: remove the `#ifdef TEST_BUILD` block added after
     `idx_count_cb_flush_thread`'s original body (the
     `idx_count_cb_flush_thread_dbg` function, `query.c:889-905`
     currently — the block opens with `#ifdef TEST_BUILD` immediately
     after that function's closing `}` and its own trailing blank line),
     restoring the file to end that function at its original closing
     brace with no seam after it.
   - `src/db/query.c`: remove the `#ifdef TEST_BUILD` bound-trace block
     inside the OP_BETWEEN case of `btree_dispatch` (`query.c:668-684`
     currently — the `NB2TRACE between ...` block), restoring the two
     `encode_criterion_value` calls to flow straight into
     `if (pc->min_exclusive || pc->max_exclusive) {`.
   - `src/db/query_internal.h`: remove the guarded
     `long long idx_count_cb_flush_thread_dbg(void);` declaration added
     after the release declaration (`query_internal.h:335-339`
     currently).
   - `src/db/types.h`: remove the same guarded declaration
     (`types.h:1055-1059` currently — the round-3 S1 amendment that put
     it here because `index.c` includes `types.h`, not
     `query_internal.h`).
   - `src/db/index.c`: remove the `#ifdef TEST_BUILD`/`#else` split at
     the end of `shard_walk_worker` (`index.c:157-168` currently),
     restoring the single unconditional
     `idx_count_cb_flush_thread(); return NULL;` tail (the preceding
     four-line "Flush any thread-local accumulator..." comment is
     unguarded and stays as-is).
   - Delete `src/test/cases/test_numeric_between_probe3.c`.
   - `build.sh`: remove the line
     `    src/test/cases/test_numeric_between_probe3.c \` (currently
     `build.sh:207`).
   - `.github/workflows/ci.yml`: remove the five-line block (comment +
     step) currently at lines 77-81:
     ```
           # TEMPORARY (scratch branch only) — round-3 diagnostic probe for
           # docs/plans/2026-08-30-macos-numeric-between-daemon-seam.md.
           # Remove with the plan close-out.
           - name: PROBE (scratch) — numeric between round 3
             run: ./build/bin/shard-db-test run test-numeric-between-probe3
     ```
     leaving `- name: Run full C test suite` as the next step after
     `- name: Build`.
   Verify with `git diff --stat` that only these seven files changed and
   the diff is a clean revert (no residual `NB2TRACE`/`TEST_BUILD` round-3
   fragments): `grep -rn "NB2TRACE\b" src/db/` must return nothing (the
   plain `NB2TRACE` tag, not `NB2TRACE4`).
4. Confirm the working tree matches `main` for every file except the four
   round-1/2/3 plan docs (`docs/plans/2026-08-28-...md`,
   `docs/plans/2026-08-30-macos-numeric-between-upper-layer.md`,
   `docs/plans/2026-08-30-macos-numeric-between-daemon-seam.md`, and this
   plan's own file once created), then:
   ```bash
   git checkout main
   git branch -D diag/macos-numeric-between-round3
   git push origin --delete diag/macos-numeric-between-round3
   git checkout -b diag/macos-numeric-between-round4 main
   git status   # expect clean tree; untracked docs/plans/*.md remain
   ```

## Task 2 — Diagnostic seam: collect vs. validate counts in `idx_count_for_leaf`

Anchor — the exact current `IT_BTREE` block in `src/db/query.c`
(`query.c:5664-5685`):

```
    /* IT_BTREE (default): collect candidate hashes, then validate+count
       through the Kf boundary instead of counting btree visits. */
    {
        CollectCtx col;
        collect_ctx_init(&col);
        col.splits = sch->splits;
        col.primary_crit = leaf;
        col.check_primary = op_needs_check_primary(leaf->op);
        col.deadline = dl;
        col.tf = tf;
        btree_dispatch(db_root, object, leaf->field, sch->splits,
                       leaf, tf, collect_hash_cb, &col);
        if (col.budget_exceeded) {
            collect_ctx_destroy(&col);
            free_compiled_criteria(cc, 1);
            return 0;
        }
        cnt = parallel_indexed_count(db_root, object, sch,
                                     col.entries, (int)col.count,
                                     &leaf_node, (FieldSchema *)fs, dl, NULL, 1);
        collect_ctx_destroy(&col);
    }
```

Replace with:

```
    /* IT_BTREE (default): collect candidate hashes, then validate+count
       through the Kf boundary instead of counting btree visits. */
    {
        CollectCtx col;
        collect_ctx_init(&col);
        col.splits = sch->splits;
        col.primary_crit = leaf;
        col.check_primary = op_needs_check_primary(leaf->op);
        col.deadline = dl;
        col.tf = tf;
        btree_dispatch(db_root, object, leaf->field, sch->splits,
                       leaf, tf, collect_hash_cb, &col);
#ifdef TEST_BUILD
        /* Round-4 diagnostic seam — candidate count straight off
           collect_hash_cb's atomic slot allocator (CollectCtx.count),
           before any Kf-boundary revalidation. Temporary — delete with
           docs/plans/2026-08-31-macos-numeric-between-round4-collection-count.md. */
        LOG_AUDIT(LOG_SUB_QUERY,
                  "NB2TRACE4 collect field=%s op=%d op_between=%d count=%zu "
                  "budget_exceeded=%d",
                  leaf->field, (int)leaf->op, leaf->op == OP_BETWEEN,
                  col.count, col.budget_exceeded);
#endif
        if (col.budget_exceeded) {
            collect_ctx_destroy(&col);
            free_compiled_criteria(cc, 1);
            return 0;
        }
        cnt = parallel_indexed_count(db_root, object, sch,
                                     col.entries, (int)col.count,
                                     &leaf_node, (FieldSchema *)fs, dl, NULL, 1);
#ifdef TEST_BUILD
        /* Round-4 diagnostic seam — final Kf-revalidated count for the
           same call. Compared against the collect trace above, this
           localizes the loss to either the range-walk/collection stage
           or parallel_indexed_count's per-record revalidation. Temporary
           — delete with the plan close-out. */
        LOG_AUDIT(LOG_SUB_QUERY,
                  "NB2TRACE4 validate field=%s op=%d op_between=%d in=%zu "
                  "out=%zu",
                  leaf->field, (int)leaf->op, leaf->op == OP_BETWEEN,
                  col.count, cnt);
#endif
        collect_ctx_destroy(&col);
    }
```

No other file needs a declaration change — both trace points sit inside
the function body and use only symbols already in scope (`col`, `cnt`,
`leaf`), unlike round 3's seam which needed a new cross-TU debug
function.

## Task 3 — Round-4 probe

### 3a — New file `src/test/cases/test_numeric_between_probe4.c`

Create with exactly this content:

```c
/* src/test/cases/test_numeric_between_probe4.c
 * TEMPORARY round-4 diagnostic probe — collection-count-vs-validated-count
 * seam for the macOS numeric-BETWEEN defect, per
 * docs/plans/2026-08-31-macos-numeric-between-round4-collection-count.md.
 * W  — wire repro (expected red on macOS). Also the source of the
 *      NB2TRACE4 seam lines S2 reads: the fixture daemon runs a real
 *      logging worker, so its LOG_AUDIT output reliably lands in
 *      audit.log.
 * S1 — the REAL orchestrator (cmd_count) run in-process, output captured
 *      through g_out. Reused unchanged from round 3 — already proved the
 *      defect reproduces with no daemon/wire runtime involved. Its
 *      LOG_AUDIT calls are NOT relied on for S2: shard_db_open_internal
 *      builds a local instance with no logging worker running, so S1's
 *      trace output goes to stderr, not audit.log. S1 here only confirms
 *      the in-process return value.
 * S2 — dumps the daemon's NB2TRACE4 seam lines from the audit log
 *      (produced by the W calls above): collected-candidate count vs.
 *      final Kf-revalidated count for the same idx_count_for_leaf call.
 * Expected to FAIL on macOS arm64 until the defect is fixed; must pass
 * 100% on Linux. Delete with the plan close-out.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Internal embedded initializer: constructs the fully initialized execution
   context required by S1 without taking the public embedded API's DB-root
   lock (the fixture daemon already owns that lock). */
extern ShardDb *shard_db_open_internal(const char *db_root);

static const char *BETWEEN_CRIT =
    "[{\"field\":\"amt\",\"op\":\"between\",\"value\":\"-1\",\"value2\":\"1\"}]";

static int do_count(TestClient *tc, const char *obj, const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":%s}", obj, crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

/* S2 — scan the daemon's log dir for NB2TRACE4 lines and dump them.
   LOG_DIR for the standard fixture is <parent-of-db_root>/logs. */
static int s2_dump_traces(TestEnv *env) {
    char base[300], logs_dir[320];
    snprintf(base, sizeof(base), "%s", env->db_root);
    char *slash = strrchr(base, '/');
    if (!slash) { TAP_DIAG("  S2 no parent dir of %s\n", env->db_root); return 0; }
    *slash = '\0';
    snprintf(logs_dir, sizeof(logs_dir), "%s/logs", base);

    DIR *d = opendir(logs_dir);
    if (!d) { TAP_DIAG("  S2 cannot open %s\n", logs_dir); return 0; }
    int matches = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char p[640];
        snprintf(p, sizeof(p), "%s/%s", logs_dir, de->d_name);
        FILE *f = fopen(p, "r");
        if (!f) continue;
        char line[1024];
        while (fgets(line, sizeof(line), f))
            if (strstr(line, "NB2TRACE4")) {
                TAP_DIAG("  S2 %s: %s", de->d_name, line);
                matches++;
            }
        fclose(f);
    }
    closedir(d);
    return matches;
}

static int test_numeric_between_probe4_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "env start"); return 1; }
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bi_num\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"amt:numeric:10,2\"],"
        "\"indexes\":[\"amt\"]}", &resp);
    free(resp); resp = NULL;
    const char *vals[5] = { "-999.99", "-0.01", "0", "0.01", "999.99" };
    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bi_num\","
            "\"key\":\"n_%d\",\"value\":{\"amt\":%s}}", i, vals[i]);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* W1 — the failing wire shape plus controls. */
    ASSERT_EQ_INT(do_count(tc, "bi_num", BETWEEN_CRIT),
        3, "W1 wire between -1 and 1 = 3 (expected red on macOS)");
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"lt\",\"value\":\"0\"}]"),
        2, "W1 wire lt 0 = 2 (control)");
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"gte\",\"value\":\"0\"}]"),
        3, "W1 wire gte 0 = 3 (control)");

    /* S1 — the REAL orchestrator, in-process, output captured through
       g_out. The server passes db_root = <root>/<dir> and the bare
       object name (server.c:1622-1628 dispatch shape). The fixture daemon
       owns a separate process and its g_db is not shared, so S1 needs a
       fully initialized local instance bound to the same fixture root. */
    char dir_root[512];
    snprintf(dir_root, sizeof(dir_root), "%s/default", env.db_root);
    ShardDb *s1_db = shard_db_open_internal(env.db_root);
    ASSERT_NOT_NULL(s1_db, "S1 full local instance");
    if (s1_db) {
        parallel_pool_init(2);
        parallel_io_pool_init(2);
        char *cap = NULL; size_t caplen = 0;
        FILE *ms = open_memstream(&cap, &caplen);
        ASSERT_NOT_NULL(ms, "S1 open_memstream");
        if (ms) {
            FILE *old_out = g_out;
            g_out = ms;
            int rc = cmd_count(dir_root, "bi_num", BETWEEN_CRIT);
            fflush(ms);
            g_out = old_out;
            fclose(ms);
            TAP_DIAG("  S1 cmd_count rc=%d captured='%s'\n", rc, cap ? cap : "");
            ASSERT_EQ_INT(rc, 0, "S1 cmd_count rc = 0");
            int s1 = cap ? atoi(cap) : -1;
            ASSERT_EQ_INT(s1, 3, "S1 in-process cmd_count returns 3");
        }
        free(cap);
    }

    /* S2 — collect/validate seam lines from the audit log, produced by
       the three W1 wire queries above (the fixture daemon runs a real
       logging worker, so LOG_AUDIT reliably lands in audit.log there;
       S1's in-process LOG_AUDIT calls do not and are not counted here).
       Each single-leaf indexed count runs through idx_count_for_leaf's
       IT_BTREE block once, emitting one "collect" + one "validate" line,
       so the three W1 queries (between + two controls) must leave
       exactly 6 lines; this exact bound proves the capture mechanics
       work end to end. The between query's own pair is the one with
       op_between=1 — the read-out is in its actual in=/out= values,
       inspected by hand from the saved log. */
    int traces = s2_dump_traces(&env);
    ASSERT_EQ_INT(traces, 6, "S2 audit log holds exactly 6 NB2TRACE4 lines (3 W1 queries x collect+validate)");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe4", test_numeric_between_probe4_run)
```

### 3b — Register the case in build.sh

Anchor (unique on the round-4 branch after Task 1's revert):

```
    src/test/cases/test_binary_index.c \
```

Replace with:

```
    src/test/cases/test_binary_index.c \
    src/test/cases/test_numeric_between_probe4.c \
```

### 3c — Local validation (Linux must be fully green)

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-numeric-between-probe4
```

- Expected: every assertion `ok` (W1, S1 rc/count, S2 traces == 6), exit
  0. The S2 assertion proves the audit-log capture mechanics work before
  CI relies on them.
- Confirm the release binary is seam-free: the trace lines are
  TEST_BUILD-only, and `./build.sh` builds `shard-db` without
  `-DTEST_BUILD` — nothing to check beyond a clean compile.
- Also run `./build/bin/shard-db-test run test-binary-index` → 22/0.

Leave the changes uncommitted per this repo's execution mode.

## Task 4 — CI probe run (scratch branch), evidence, read-out

### 4a — Temporary workflow step

Insert immediately **above** the anchor line `- name: Run full C test
suite` in `.github/workflows/ci.yml` (this is the same anchor Task 1
restored to be immediately after `- name: Build`):

```yaml
      # TEMPORARY (scratch branch only) — round-4 diagnostic probe for
      # docs/plans/2026-08-31-macos-numeric-between-round4-collection-count.md.
      # Remove with the plan close-out.
      - name: PROBE (scratch) — numeric between round 4
        run: ./build/bin/shard-db-test run test-numeric-between-probe4
```

No `if:` — all three legs run it; Linux legs are the baseline.

### 4b — Git operations (human-run or human-directed)

The scratch commit must include the seam source file — CI cannot compile
the probe without it. The `src/db/query.c` diff in this commit is a
TEST_BUILD-only temporary seam; it exists only on this scratch branch and
is never merged.

```bash
git add src/test/cases/test_numeric_between_probe4.c build.sh \
        .github/workflows/ci.yml \
        docs/plans/2026-08-31-macos-numeric-between-round4-collection-count.md \
        src/db/query.c
git commit -m "test: temporary numeric-between round-4 collect/validate probe (scratch)"
git push -u origin diag/macos-numeric-between-round4
gh pr create --draft --title "scratch: numeric-between round-4 probe (do not merge)" \
  --body "Diagnostic only — see docs/plans/2026-08-31-macos-numeric-between-round4-collection-count.md (committed on this branch). The src/db/query.c diff is a TEST_BUILD-only temporary seam. Never merge."
```

Guardrail: the `src/db/query.c` diff must contain only the planned Task-2
seam (the two `LOG_AUDIT` blocks). If it carries anything beyond that,
describe it in one line in the PR body and in the Task 5 halt report so
the fix-time review sees it — do not let an undocumented hunk ride along
silently.

### 4c — Evidence capture, durability, read-out

1. Wait for all three legs; save each leg's "PROBE (scratch)" log. **The
   S2 lines in that log ARE the observation** — they carry the seam
   output, so no separate artifact is needed.
2. Append to this plan file: `## Evidence — Task 4` with the three
   labeled outputs + interpretation. Commit and push the plan to the
   scratch branch; post the three logs as PR comments. Both durability
   conditions must hold before Task 5 deletes anything.
3. Read out (macOS-only patterns; Linux must be green everywhere). Focus
   on the `NB2TRACE4` lines with `field=amt op_between=1` — that flag
   picks the between query's pair out directly, no enum-value lookup
   needed to distinguish it from the `lt`/`gte` control queries' pairs
   (also `field=amt`, but `op_between=0`). There will be one
   `collect`/`validate` pair per wire single-leaf count in the test
   (three total: between + two controls); the `op_between=1` pair is the
   one that matters:

   | Observation (between query's own collect/validate pair) | Localization |
   |---|---|
   | `collect ... count=2` | loss is in the range-walk/collection stage inside `idx_count_for_leaf`'s `btree_dispatch` call — despite round 1's B2 walking the same real per-shard `.idx` files directly and getting 3, something about *this* call's composition (field resolution, shard fan-out, or a race specific to this call site) drops a candidate. Next round bisects `pick_index_for_leaf`/`resolve_idx_field` for this call and the shard fan-out inside this exact call, e.g. by adding a per-shard-group trace to `btree_dispatch`'s OP_BETWEEN walk |
   | `collect ... count=3`, `validate ... in=3 out=2` | loss is in `parallel_indexed_count`'s Kf-boundary revalidation — `shard_count_worker`/`count_batch_cb`/the batch record-fetch path (`slotcask_bulk_fetch_resolved` / `slotcask_bulk_resolve_and_fetch`), not the comparator (already exonerated). Next round bisects that fetch/resolve path specifically |
   | No `NB2TRACE4` lines with `field=amt op_between=1` at all | the call-path derivation in this plan is wrong on macOS — `pick_index_for_leaf` isn't returning `IT_BTREE`, or `idx_count_for_leaf` isn't being reached the way this plan derived. Stop and ask; do not extend the seam further without human input |
   | `collect` and `validate` both show `count=3`/`out=3` on macOS (bug doesn't reproduce under the seam) | the seam itself perturbs timing/ordering enough to mask the defect — note this explicitly, do not conclude "fixed"; escalate to the human before any further round |
   | Anything else | stop and ask |

## Task 5 — HALT: report root cause; fix requires a human-approved plan

1. **Stop.** Post the evidence: the matching decision-table row, both
   platforms' outputs (S2 trace lines included), and the specific defect
   mechanism (function + line + why macOS only), to whatever precision
   this round's trace pinpoints. No fix code.
2. The human approves an amended/follow-up plan (either a fix plan, if
   this round's evidence pins the mechanism precisely enough, or a
   further bisection round otherwise), which must contain: the root
   cause as a specific mechanism; complete code blocks for every hunk; a
   codebase-search listing of every consumer of the changed function; the
   regression proof (`test-binary-index` macOS fail-before from round 1 +
   green-after on all three legs — the permanent regression pin); the
   sanitizer gate; and the close-out edits below.
3. Platform rule: the fix must be platform-neutral C. No
   `#ifdef __APPLE__` unless byte-level evidence demands it — and that is
   a decision to escalate to the human, not improvise.
4. Delete the round-4 scratch branch and PR only after Task 4c's both
   durability conditions hold; close — never merge.

## Acceptance (whole effort — the eventual fix plan's gate, restated)

1. `test-binary-index` 22/0 on macOS arm64, Linux x86_64, Linux arm64.
2. `ci.yml` exclusion removed: delete the comment block anchored by
   `# test-binary-index: pre-existing macOS-arm64-only failure (numeric`
   through `# docs/plans/2026-08-28-macos-arm64-numeric-between.md`, and
   remove `,test-binary-index` from the `SHARD_TEST_EXCLUDE` value line.
3. ALL probe scaffolding deleted: every round's probe case
   (`test_numeric_between_probe.c` / `…_probe2.c` / `…_probe3.c` /
   `…_probe4.c`), their `build.sh` list lines, every round's
   `PROBE (scratch)` workflow step, AND every round's diagnostic seams
   from `src/db/*` (round 3's flush-delta/bound-trace seams — already
   removed by this plan's Task 1 if this plan executes; round 4's
   collect/validate seam from this plan's Task 2).
4. Sanitizer gate on the fix diff (count path touches shared/concurrent
   state — this repo's standing gate applies, locally, before "done"):
   `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` then three fresh
   `./build/bin/shard-db-test run-all`; `BUILD_MODE=tsan SKIP_TESTS=1
   ./build.sh` then three fresh
   `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
   ./build/bin/shard-db-test run-all`.
5. No new compiler warnings; no leftover probe/debug prints or seam
   residue.
6. All four plan files' Status lines updated to done, root cause
   recorded in one paragraph in each.

## Invariants

- Do not adjust `test-binary-index`'s expectation.
- No per-platform encoding or `#ifdef __APPLE__` in any fix without
  byte-level evidence.
- The fix goes where the defect is (root cause), not where the symptom
  is observed.
- The seams and probe are diagnostic scaffolding only; none of it ships.
