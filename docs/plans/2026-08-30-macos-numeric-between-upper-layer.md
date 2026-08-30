# macOS arm64 numeric BETWEEN — round 2: wire count path diagnosis

Date: 2026-08-30
Status: draft awaiting human approval. **Diagnosis only — no fix code is
authorized by this plan.** Round-1 evidence localizes the defect to the
wire count path above `btree.c`'s direct range implementation; this plan
discriminates the remaining suspects to a function-level root cause, then
halts for a human-approved fix plan.
Round-1 record: `docs/plans/2026-08-28-macos-arm64-numeric-between.md`
(evidence archive; its scratch PR is closed, never merged).

## Where round 1 left off (verified, CI-run 2026-08-30)

- A1–A6 pass on macOS: platform arithmetic, `encode_field_for_index`,
  bare-btree insert/walk-all/`btree_range`/`btree_range_ex`/`BtRangeIter`
  are all correct at the `0x7F→0x80` boundary.
- B2 passes on macOS: the real `indexes/amt/00N.idx` files hold all 5
  records (lazily-created shards accounted for); serial per-file
  `btree_range` with probe-computed bounds totals 3.
- B1 fails on macOS only: wire `count` `between -1..1` = **2** (expected
  3); controls `lt 0` = 2 and `gte 0` = 3 pass. Both Linux legs fully
  green.
- Note the shape: the 3 matches sit **one per shard across three files**
  (round-1 B2), and the wire returns **2** — as if exactly one fan-out
  worker's contribution is lost.

## The path under suspicion (verified code reading)

Wire count → planner compiles criteria → primary leaf →
`btree_dispatch` (query.c:561) → `OP_BETWEEN` case (query.c:665-678):
`encode_criterion_value` ×2 (query.c:357 — with a typed field present it
forwards to `encode_field_for_index`, which round-1 A2 exonerated) →
`btree_idx_range` inclusive (user-supplied BETWEEN never sets the
exclusivity flags, types.h:289-294) → `shard_walk_dispatch`
(index.c:161-176) → `parallel_for_io` (parallel.c:362) → per-shard
`btree_range` → `idx_count_cb` (query.c:809) → per-shard flush
`idx_count_cb_flush_thread()` in `shard_walk_worker` (index.c:157).

## Suspects, ranked

1. **`idx_count_cb` TLS count batching** — `idx_count_local` is
   `__thread` (query.c:804-807); each pool worker accumulates locally and
   flushes once per shard via `idx_count_cb_flush_thread()`. A lost or
   never-flushed worker batch loses exactly that shard's matches —
   matching the observed one-worker-short shape. Darwin `__thread`
   semantics and pool-worker reuse differ from Linux.
2. **`parallel_for_io` dispatch on the macOS daemon** dropping or
   short-circuiting one shard task. Round-1 B2 does NOT exonerate this:
   it called `btree_range` per file serially from the test process, never
   the real fan-out. Note the inline fallback at parallel.c:364
   (`!g_io_running || n == 1 || t_in_pool_worker`) — an in-process probe
   must initialize the pools or it silently serialises and proves
   nothing.
3. **Planner-produced criterion diverges on macOS** (same-field
   coalescer setting `min_exclusive`/`max_exclusive`, or a different plan
   kind chosen). The wire path uses the planner; hand-built in-process
   criteria do not. `mode:explain` + `"explain":true` dumps the compiled
   plan to arbitrate (`cmd_explain`, query_plan.c:2332).
4. **False deadline trip** — `query_deadline_tick` (types.h:534) early-
   stops `idx_count_cb` (`return -1`), abandoning that shard's remainder.
   Demoted: the CI daemon runs `TIMEOUT=0` and the tick is disabled when
   `timeout_ms == 0`; `now_ms_coarse` falls back to `CLOCK_MONOTONIC` on
   Darwin (config.c:43-51). Kept as a cheap discriminator (C4b).

## Embedded execution rules

- Branch: after Task 1's round-1 close-out, create
  `diag/macos-numeric-between-round2` off `main`. Do all round-2 work on
  it. Leave work **uncommitted** except Task 3's evidence pushes (same
  exception as round 1: the scratch branch must commit the probe,
  build.sh/workflow edits, and this plan with evidence appended; PR
  comments are the durable copy — see Task 3).
- Do tasks in order. No fix code in this plan: Task 4 halts.
- Build/test commands: `SKIP_TESTS=1 ./build.sh`;
  `./build/bin/shard-db-test run test-numeric-between-probe2`;
  `./build/bin/shard-db-test run-all`.
- If a quoted anchor isn't found exactly, write `PLAN_NOTES.md` describing
  the mismatch and halt the entire execution run immediately — do not
  guess, reinterpret, or continue to any further task, even an unrelated
  one. Resuming requires the human (or the planning model, re-engaged)
  to read `PLAN_NOTES.md`, decide whether it's a stale-anchor problem
  (re-derive and patch the plan) or a wrong-assumption problem (rethink
  the plan), and hand back either a patched or a fresh plan — execution
  never resumes on its own initiative.
- If you hit a decision the plan doesn't cover, stop and ask — do not
  improvise.
- Never weaken or delete an existing test to make a failure disappear.
  The round-2 probe is scaffolding on a scratch branch only; it must
  never reach `main` while red.

## Task 1 — Close out round 1 first (durability, then teardown)

The round-1 executor halted before Task 3c durability. Complete it:

1. Save the three per-leg "PROBE (scratch)" logs from the round-1 CI run
   (e.g. `/tmp/probe-linux-x86_64.log`, `/tmp/probe-linux-arm64.log`,
   `/tmp/probe-macos-arm64.log`).
2. Append to `docs/plans/2026-08-28-macos-arm64-numeric-between.md` a
   `## Evidence — Task 3` section: the three labeled outputs, a one-line
   interpretation each, and the localization statement (A1–A6 + B2 pass;
   B1-only fails with 2; Linux green). Also append `## Task 4 — halt
   report`: localization row "B1 with all A/B2 green", the four suspect
   mechanisms listed above, and the statement that no fix code was
   written.
3. Push the evidence (human-run or human-directed):
   ```bash
   git add docs/plans/2026-08-28-macos-arm64-numeric-between.md
   git commit -m "docs: numeric-between round-1 probe evidence + halt report"
   git push
   ```
4. Post the three logs as PR comments (the copies that survive branch
   deletion):
   ```bash
   gh pr comment --body-file /tmp/probe-linux-x86_64.log
   gh pr comment --body-file /tmp/probe-linux-arm64.log
   gh pr comment --body-file /tmp/probe-macos-arm64.log
   ```
5. Only after both are confirmed: close — never merge — the round-1 PR
   and delete the round-1 branch (remote and local).
6. Reset the local tree and open the round-2 branch:
   ```bash
   git checkout main
   git branch -D diag/macos-numeric-between-probe
   git restore build.sh .github/workflows/ci.yml 2>/dev/null || true
   rm -f src/test/cases/test_numeric_between_probe.c PLAN_NOTES.md
   git checkout -b diag/macos-numeric-between-round2 main
   git status   # expect clean tree; untracked docs/plans/*.md remain
   ```
   `PLAN_NOTES.md` is deleted deliberately: its lazy-shard-file finding
   is resolved (round-1 plan, second revision) and superseded by this
   plan's fact base. The round-1 plan file is untracked on `main` and
   stays in the tree as the evidence archive.

## Task 2 — Round-2 probe

One daemon-backed case. Phase W reproduces the wire failure and dumps
the compiled plan (`explain`); Phase C discriminates the in-process
mechanisms with the pools explicitly initialized so `parallel_for_io`
really dispatches (see suspect 2's inline-fallback caveat).

### 2a — New file `src/test/cases/test_numeric_between_probe2.c`

Create with exactly this content:

```c
/* src/test/cases/test_numeric_between_probe2.c
 * TEMPORARY round-2 diagnostic probe for the macOS numeric-BETWEEN
 * defect — docs/plans/2026-08-30-macos-numeric-between-upper-layer.md.
 * Round 1 exonerated encoding arithmetic, the config.c encoder, the
 * btree seek/iter paths, and the on-disk index files. This probe
 * discriminates the remaining suspects: parallel_for_io dispatch,
 * idx_count_cb TLS batching, a false deadline trip, and the
 * planner-produced criterion. Expected to FAIL on macOS arm64 until the
 * defect is fixed; must pass 100% on Linux. Delete with the close-out.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "btree.h"
#include "types.h"
#include "query_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PROBE_MULT 100LL   /* amt:numeric:10,2 → ×100 */
static const char *PROBE_LO = "-1";
static const char *PROBE_HI = "1";

static void local_numeric_key(const char *dec, int64_t mult, uint8_t out[8]) {
    double dv = atof(dec);
    int64_t v = (int64_t)(dv * (double)mult + (dv >= 0 ? 0.5 : -0.5));
    uint64_t u = (uint64_t)v ^ (1ULL << 63);
    for (int i = 0; i < 8; i++) out[i] = (uint8_t)(u >> (56 - 8 * i));
}

/* C2/C3 plain counting callback — thread-safe: the fan-out fires it
   from pool workers. */
static int n_plain;
static int plain_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    (void)v; (void)vl; (void)h; (void)ctx;
    __atomic_add_fetch(&n_plain, 1, __ATOMIC_RELAXED);
    return 0;
}

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

static void phase_c(TestEnv *env) {
    /* Mirror the daemon's pools: parallel_for_io runs INLINE when
       g_io_running is 0 (parallel.c:364), which would silently
       serialise C2/C4/C5 and prove nothing about dispatch. Both init
       calls are no-ops when already initialized. */
    long nproc = sysconf(_SC_NPROCESSORS_ONLN);
    if (nproc < 4) nproc = 4;
    parallel_pool_init((int)nproc);
    parallel_io_pool_init((int)nproc * 4);
    TAP_DIAG("  C0 pools: cpu=%d", parallel_pool_size());

    TypedField f; memset(&f, 0, sizeof(f));
    f.type = FT_NUMERIC; f.size = 8; f.numeric_scale = 2;
    f.numeric_scale_mult = PROBE_MULT;
    uint8_t lo[8], hi[8];
    local_numeric_key(PROBE_LO, PROBE_MULT, lo);
    local_numeric_key(PROBE_HI, PROBE_MULT, hi);

    /* C2 — the real fan-out (shard_walk_dispatch → parallel_for_io)
       with a plain callback: no TLS batching, no deadline. */
    n_plain = 0;
    btree_idx_range(env->db_root, "default/bi_num", "amt", 16,
                    (const char *)lo, 8, (const char *)hi, 8,
                    plain_cb, NULL);
    TAP_DIAG("  C2 fan-out plain-cb count = %d", n_plain);
    ASSERT_EQ_INT(n_plain, 3, "C2 fan-out + plain callback returns 3");

    /* C3 — serial per-file baseline, same callback, no parallel_for.
       Lazily-created empty shards legitimately have no file. */
    int nshards = index_splits_for(16);
    int serial = 0;
    for (int s = 0; s < nshards; s++) {
        char p[512];
        build_idx_path(p, sizeof(p), env->db_root, "default/bi_num",
                       "amt", s);
        if (access(p, R_OK) != 0) continue;
        n_plain = 0;
        btree_range(p, (const char *)lo, 8, (const char *)hi, 8,
                    plain_cb, NULL);
        serial += n_plain;
    }
    TAP_DIAG("  C3 serial per-file count = %d", serial);
    ASSERT_EQ_INT(serial, 3, "C3 serial per-file baseline returns 3");

    /* C4a — fan-out through idx_count_cb with the deadline disabled:
       isolates the __thread batched-count path (query.c:804-807). */
    IdxCountCtx ic; memset(&ic, 0, sizeof(ic));
    ic.deadline = NULL;
    btree_idx_range(env->db_root, "default/bi_num", "amt", 16,
                    (const char *)lo, 8, (const char *)hi, 8,
                    idx_count_cb, &ic);
    TAP_DIAG("  C4a fan-out idx_count_cb (no deadline) = %zu", ic.count);
    ASSERT_EQ_INT((long)ic.count, 3, "C4a idx_count_cb TLS batching = 3");

    /* C4b — same with a live 60s deadline: discriminates a false
       query_deadline_tick trip (types.h:534) from a TLS-batch loss. */
    QueryDeadline dl;
    dl.t0_ms = now_ms(); dl.timeout_ms = 60000;
    atomic_init(&dl.timed_out, 0);
    IdxCountCtx ic2; memset(&ic2, 0, sizeof(ic2));
    ic2.deadline = &dl; ic2.tf = &f;
    btree_idx_range(env->db_root, "default/bi_num", "amt", 16,
                    (const char *)lo, 8, (const char *)hi, 8,
                    idx_count_cb, &ic2);
    TAP_DIAG("  C4b fan-out idx_count_cb (60s deadline) = %zu timed_out=%d",
             ic2.count, atomic_load_explicit(&dl.timed_out,
                                             memory_order_relaxed));
    ASSERT_EQ_INT((long)ic2.count, 3, "C4b idx_count_cb with deadline = 3");

    /* C5 — the full in-process dispatch: btree_dispatch with a
       hand-built OP_BETWEEN criterion, replicating what the planner
       emits for a single user-supplied BETWEEN (inclusive both ends,
       types.h:289-294). field_index_type() reads the object's
       index.conf from db_root — the daemon must still be alive. */
    SearchCriterion pc; memset(&pc, 0, sizeof(pc));
    snprintf(pc.field, sizeof(pc.field), "amt");
    pc.op = OP_BETWEEN;
    snprintf(pc.value, sizeof(pc.value), "-1");
    snprintf(pc.value2, sizeof(pc.value2), "1");
    IdxCountCtx ic3; memset(&ic3, 0, sizeof(ic3));
    ic3.deadline = NULL; ic3.tf = &f;
    btree_dispatch(env->db_root, "default/bi_num", "amt", 16, &pc, &f,
                   idx_count_cb, &ic3);
    TAP_DIAG("  C5 btree_dispatch OP_BETWEEN = %zu", ic3.count);
    ASSERT_EQ_INT((long)ic3.count, 3, "C5 btree_dispatch between = 3");
}

static int test_numeric_between_probe2_run(void) {
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
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"between\",\"value\":\"-1\","
        "\"value2\":\"1\"}]"),
        3, "W1 wire between -1 and 1 = 3 (expected red on macOS)");
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"lt\",\"value\":\"0\"}]"),
        2, "W1 wire lt 0 = 2 (control)");
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"gte\",\"value\":\"0\"}]"),
        3, "W1 wire gte 0 = 3 (control)");

    /* W2 — dump the compiled plan for the same criteria. Diagnostic
       only: no shape assert, the Linux/macOS diff is the evidence. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"bi_num\","
        "\"criteria\":[{\"field\":\"amt\",\"op\":\"between\","
        "\"value\":\"-1\",\"value2\":\"1\"}],\"explain\":true}", &resp);
    TAP_DIAG("  W2 explain response: %s\n",
             resp ? resp : "(null)");
    ASSERT_NOT_NULL(resp, "W2 explain returned a response");
    free(resp); resp = NULL;

    tc_close(tc);
    phase_c(&env);          /* daemon still alive: field_index_type +
                               btree files are read live */
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe2", test_numeric_between_probe2_run)
```

### 2b — Register the case in build.sh

Anchor (unique on `main`):

```
    src/test/cases/test_binary_index.c \
```

Replace with:

```
    src/test/cases/test_binary_index.c \
    src/test/cases/test_numeric_between_probe2.c \
```

### 2c — Local validation (Linux must be fully green)

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-numeric-between-probe2
```

- Expected: every assertion `ok` (W1, W2, C2–C5), exit 0.
- If anything fails on Linux, the probe is wrong — stop and report the
  TAP output; do not adjust expectations to force green.
- Also run `./build/bin/shard-db-test run test-binary-index` → 22/0.

Leave the changes uncommitted per this repo's execution mode.

## Task 3 — CI probe run (scratch branch), evidence, read-out

### 3a — Temporary workflow step

The dedicated step must run **before** the full-suite step (the macOS
suite step is expected to fail on this branch). In
`.github/workflows/ci.yml`, anchor — the exact line:

```
      - name: Run full C test suite
```

Insert immediately **above** it:

```yaml
      # TEMPORARY (scratch branch only) — round-2 diagnostic probe for
      # docs/plans/2026-08-30-macos-numeric-between-upper-layer.md.
      # Remove with the plan close-out.
      - name: PROBE (scratch) — numeric between round 2
        run: ./build/bin/shard-db-test run test-numeric-between-probe2
```

No `if:` — all three legs run it; Linux legs are the baseline.

### 3b — Git operations (human-run or human-directed)

```bash
git add src/test/cases/test_numeric_between_probe2.c build.sh \
        .github/workflows/ci.yml \
        docs/plans/2026-08-30-macos-numeric-between-upper-layer.md
git commit -m "test: temporary numeric-between round-2 probe (scratch)"
git push -u origin diag/macos-numeric-between-round2
gh pr create --draft --title "scratch: numeric-between round-2 probe (do not merge)" \
  --body "Diagnostic only — see docs/plans/2026-08-30-macos-numeric-between-upper-layer.md (committed on this branch). Never merge."
```

### 3c — Evidence capture, durability, read-out

1. Wait for all three legs; save each leg's "PROBE (scratch)" log.
2. Append to this plan file: `## Evidence — Task 3` with the three
   labeled outputs (round-1 W2/explain dump included) + interpretation.
   Commit and push the plan to the scratch branch; post the three logs
   as PR comments. Both durability conditions must hold before Task 4
   deletes anything.
3. Read out with this table (macOS-only failures; Linux must be green
   everywhere):

   | Pattern | Localization |
   |---|---|
   | C2 = 2 (C3 = 3) | `parallel_for_io` / `shard_walk_dispatch` drops a shard task — root cause in the dispatch layer (parallel.c / index.c) |
   | C2 = 3, C4a = 2 | `idx_count_cb` `__thread` batching/flush loses a worker's pending count — root cause in query.c:793-870 / index.c:157 |
   | C2 = 3, C4a = 3, C4b = 2 (timed_out=1) | false `query_deadline_tick` trip — types.h:534 clock/timeout handling |
   | C2 = 3, C4a = 3, C5 = 2 | `btree_dispatch` BETWEEN branch itself (criterion struct → encode → range call) — query.c:665-678 / query.c:357 |
   | All C green, W1 = 2 | daemon/planner-specific: compare W2 explain dumps — a different plan kind or flags on macOS indicts the planner (query_plan.c); if explain is identical too, stop and ask — the next step is a daemon-side seam decision the human must make |
   | W2 explain differs across platforms | planner-produced criterion divergence — root cause in query_plan.c (coalescer/plan selection) |

## Task 4 — HALT: report root cause; fix requires a human-approved plan

1. **Stop.** Post the evidence: the matching decision-table row, both
   platforms' outputs, and the specific defect mechanism (function +
   line + why macOS only). No fix code.
2. The human approves an amended/follow-up fix plan, which must contain:
   the root cause as a specific mechanism; complete code blocks for
   every hunk; a codebase-search listing of every consumer of the
   changed function; the regression proof (`test-binary-index` macOS
   fail-before evidence from round 1 + green-after on all three legs;
   `test-numeric-between-probe2` red-before/green-after is optional
   corroboration — the permanent regression pin is `test-binary-index`);
   the sanitizer gate; and the close-out edits below.
3. Platform rule: the fix must be platform-neutral C. No
   `#ifdef __APPLE__` unless byte-level evidence demands it — and that
   is a decision to escalate to the human, not improvise.
4. Delete the round-2 scratch branch and PR only after Task 3c's both
   durability conditions hold; close — never merge.

## Acceptance (whole effort — the fix plan's gate, restated)

1. `test-binary-index` 22/0 on macOS arm64, Linux x86_64, Linux arm64.
2. `ci.yml` exclusion removed: delete the comment block anchored by
   `# test-binary-index: pre-existing macOS-arm64-only failure (numeric`
   through `# docs/plans/2026-08-28-macos-arm64-numeric-between.md`, and
   remove `,test-binary-index` from the `SHARD_TEST_EXCLUDE` value line.
3. Probe scaffolding deleted: BOTH probe cases
   (`test_numeric_between_probe.c` was round-1-branch-only and must not
   reappear; `test_numeric_between_probe2.c` from this plan), both
   `build.sh` list lines, and both `PROBE (scratch)` workflow steps.
4. Sanitizer gate on the fix diff (count path touches shared/concurrent
   state — this repo's standing gate applies, locally, before "done"):
   `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` then three fresh
   `./build/bin/shard-db-test run-all`; `BUILD_MODE=tsan SKIP_TESTS=1
   ./build.sh` then three fresh
   `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
   ./build/bin/shard-db-test run-all`.
5. No new compiler warnings; no leftover probe/debug prints.
6. Both plan files' Status lines updated to done, root cause recorded in
   one paragraph in each.

## Invariants

- Do not adjust `test-binary-index`'s expectation.
- No per-platform encoding or `#ifdef __APPLE__` without byte-level
  evidence.
- The fix goes where the defect is (root cause), not where the symptom
  is observed.

## Evidence — Task 3

Round-two CI: both Linux probe legs passed. On macOS, W1 returned 2 while
W2 explained the same B-tree leaf plan as Linux and C2–C5 all returned 3.

## Task 4 — halt report

The "All C green, W1 = 2, explain identical" row applied. No fix code was
written; the remaining suspects require a daemon-side seam.
