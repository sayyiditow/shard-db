# macOS arm64 numeric BETWEEN — round 3: real-orchestrator + daemon trace

Date: 2026-08-30
Status: draft awaiting human approval. Revised 2026-08-30 after the
Task-2c anchor halt (halt resolution + resume state recorded below).
**Diagnosis only — no fix code is
authorized by this plan.** This is the round-2 plan's reserved
stop-and-ask branch (all C green, W1 = 2, explain identical): the next
step is a daemon-side diagnostic seam, decided here and implemented as
TEST_BUILD-only scaffolding deleted at close-out.
Round-1 record: `docs/plans/2026-08-28-macos-arm64-numeric-between.md`.
Round-2 record: `docs/plans/2026-08-30-macos-numeric-between-upper-layer.md`.

## Where round 2 left off (verified, CI 2026-08-30)

- macOS wire `count` `between -1..1` = **2** (W1 red); controls `lt 0` = 2,
  `gte 0` = 3 pass; Linux legs fully green.
- W2 `explain` identical on both platforms: plan `leaf`, same typed
  btree between seed — the planner is not diverging.
- C2–C5 all return 3 on macOS: `parallel_for_io` fan-out (with pools
  explicitly initialized), serial per-file baseline, `idx_count_cb` TLS
  batching (deadline off and on), and a hand-built-criterion
  `btree_dispatch` call all behave.
- **The gap**: every in-process experiment called `btree_dispatch`
  directly. The daemon's wire count actually runs `cmd_count`
  (query.c:5538, declared types.h:1161) → `cmd_count_with_tree` — the
  orchestrator that parses criteria, runs the planner, and executes the
  plan. That function has never been executed by any experiment, and the
  failing daemon process has never been observed from inside.

## What this plan does

Two seams, one CI run:

- **Seam A** — the probe calls the REAL `cmd_count` in-process (capturing
  its `OUT()` through the thread-local `g_out`, types.h:446-447, via
  `open_memstream`). If it returns 2 in the test process, the defect is
  reproducible locally inside `cmd_count_with_tree` and can be bisected
  without CI. If it returns 3, the fault requires the daemon's runtime.
- **Seam B** — a TEST_BUILD-only trace inside the daemon: the per-shard
  count flush reports its delta, and `shard_walk_worker` logs it with the
  shard path; `btree_dispatch`'s OP_BETWEEN branch logs the encoded
  bound bytes. The probe reads the daemon's audit log after the wire
  counts and dumps every trace line. This shows, from inside the failing
  process: what bounds the daemon encoded, whether every shard walked,
  and how many matches each shard's walk contributed.

Both seams log via `LOG_AUDIT` deliberately: it bypasses the LOG_LEVEL
filter (log.h routing table) and lands in `audit.log`, so the standard
test fixture (LOG_LEVEL=2) captures it with no db.env override. All seam
code is `#ifdef TEST_BUILD`-guarded — the release binary is untouched —
and is deleted with the plan close-out.

## Suspect state entering round 3

Exonerated on macOS: encoding arithmetic (A1), `encode_field_for_index`
(A2), bare-btree insert/walk/range/iter (A3–A6, B2), planner plan
selection (W2 explain), fan-out dispatch (C2), TLS count batching
(C4a/b), hand-built-criterion dispatch (C5).

Still live, ranked:

1. **Orchestrator glue** — something between the planner's output and
   `btree_dispatch` inside `cmd_count_with_tree` (check_primary flags,
   `CompiledCriterion` fast path, a different executor branch) behaves
   differently on macOS. Seam A decides this first.
2. **Daemon-side stale index reads** — the daemon's `bt_cache` holds
   mmap'd pages from its own inserts/warmup; if one shard's cached leaf
   content is stale or partially visible on macOS (mmap coherence /
   invalidation-after-build), the daemon's walkers return 2 while a
   fresh-process reader (round-1 B2) sees 3. Seam B's per-shard flushed
   deltas expose exactly this: sum(delta) = what the daemon's walkers
   returned.
3. **Executor-branch divergence** — if `field_index_type` misreads
   `index.conf` on macOS, the daemon could take a non-btree branch
   (bitmap dict-scan) for this count. C5 cannot exonerate this: its
   internal branch was never observed. Seam B makes the branch visible
   by absence (no NB2TRACE lines → not the btree shard-walk path).
4. **Post-aggregation loss** — count correct in `ic->count`, wrong on the
   wire. Seam B: sum = 3 with wire 2.

## Embedded execution rules

- Branch: after Task 1's round-2 close-out, create
  `diag/macos-numeric-between-round3` off `main`. Leave work
  **uncommitted** except Task 4's evidence pushes (scratch branch
  commits probe + build.sh/workflow edits + this plan with evidence;
  PR comments are the durable copy).
- Do tasks in order. No fix code: Task 5 halts.
- Build/test commands: `SKIP_TESTS=1 ./build.sh`;
  `./build/bin/shard-db-test run test-numeric-between-probe3`;
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
  The round-3 probe is scaffolding on a scratch branch only; it must
  never reach `main` while red.

## Halt resolution and resume state (2026-08-30)

The executor halted at Task 2c: the quoted anchor "was not found
exactly." Resolution: **stale-anchor problem, re-derived** — the
three-line anchor was verified byte-for-byte identical to
`query.c:665-667` and unique in the file; the widened five-line anchor
(including the preceding `break;` and the trailing
`if (pc->min_exclusive || pc->max_exclusive) {`) plus the five-site
disambiguation above removes the ambiguity that most likely tripped the
matcher. `PLAN_NOTES.md` records the halt and may be deleted on resume.

Tree state when the run halted (branch `diag/macos-numeric-between-round3`,
all uncommitted): Task 1 complete (round-2 closed out); Task 3a probe
file and 3b `build.sh` line **already applied** — on resume, verify them
against this plan instead of re-applying (re-running the 3b anchor edit
would duplicate the list line); Task 2 seams 2a/2b/2c **not applied**;
nothing committed, no workflow edit, no PR.

Resume sequence: Task 2 (2a, 2b, then 2c with the widened anchor) →
Task 3c local validation → Task 4.

## Task 1 — Close out round 2 first (durability, then teardown)

Same mechanics as round 1's Task 1:

1. Save the three per-leg "PROBE (scratch)" logs from the round-2 CI run.
2. Append to `docs/plans/2026-08-30-macos-numeric-between-upper-layer.md`
   a `## Evidence — Task 3` section (three labeled outputs +
   interpretation) and a `## Task 4 — halt report` (decision-table row
   "All C green, W1 = 2, explain identical" → stop-and-ask honoured; the
   four suspect mechanisms; no fix code written).
3. Commit and push the plan file to the round-2 scratch branch; post the
   three logs as PR comments.
4. Only after both are confirmed: close — never merge — the round-2 PR
   and delete the round-2 branch (remote and local).
5. Reset the local tree and open the round-3 branch:
   ```bash
   git checkout main
   git branch -D diag/macos-numeric-between-round2
   git restore build.sh .github/workflows/ci.yml 2>/dev/null || true
   rm -f src/test/cases/test_numeric_between_probe2.c
   git checkout -b diag/macos-numeric-between-round3 main
   git status   # expect clean tree; untracked docs/plans/*.md remain
   ```

## Task 2 — Diagnostic seams (all TEST_BUILD-guarded, all deleted at close-out)

### 2a — TEST_BUILD-only flush-delta seam (release signature untouched)

The release `void idx_count_cb_flush_thread(void)` and its declaration
are **not modified**. The seam is a separate, additive TEST_BUILD-only
function. Duplicating the small flush body is deliberate temporary
scaffolding so the release build retains the current implementation
byte-for-byte.

Anchor — the exact current function in `src/db/query.c` (around
line 862):

```
void idx_count_cb_flush_thread(void) {
    if (idx_count_local.bound_ic) {
        __atomic_add_fetch(
            &((IdxCountCtx *)idx_count_local.bound_ic)->count,
            idx_count_local.pending, __ATOMIC_RELAXED);
        idx_count_local.bound_ic = NULL;
        idx_count_local.pending = 0;
    }
}
```

Replace with (original preserved verbatim, seam function added after it):

```
void idx_count_cb_flush_thread(void) {
    if (idx_count_local.bound_ic) {
        __atomic_add_fetch(
            &((IdxCountCtx *)idx_count_local.bound_ic)->count,
            idx_count_local.pending, __ATOMIC_RELAXED);
        idx_count_local.bound_ic = NULL;
        idx_count_local.pending = 0;
    }
}

#ifdef TEST_BUILD
/* Round-3 diagnostic seam — flushes exactly like
   idx_count_cb_flush_thread() and returns the pending total committed
   (0 when nothing was pending). TEST_BUILD-only; release builds never
   see it. Temporary — delete with
   docs/plans/2026-08-30-macos-numeric-between-daemon-seam.md. */
long long idx_count_cb_flush_thread_dbg(void) {
    if (!idx_count_local.bound_ic) return 0;
    long long flushed = (long long)idx_count_local.pending;
    __atomic_add_fetch(
        &((IdxCountCtx *)idx_count_local.bound_ic)->count,
        idx_count_local.pending, __ATOMIC_RELAXED);
    idx_count_local.bound_ic = NULL;
    idx_count_local.pending = 0;
    return flushed;
}
#endif
```

Anchor — the exact line in `src/db/query_internal.h` (line 334):

```
void idx_count_cb_flush_thread(void);
```

Replace with (original preserved, guarded declaration added after it):

```
void idx_count_cb_flush_thread(void);
#ifdef TEST_BUILD
/* Round-3 diagnostic seam — see query.c. Temporary; delete with the
   plan close-out. */
long long idx_count_cb_flush_thread_dbg(void);
#endif
```

### 2b — `shard_walk_worker` traces per-shard flushes

Anchor — the exact lines at the end of `shard_walk_worker` in
`src/db/index.c` (around line 152-157; the comment above the call is
part of the anchor):

```
    /* Flush any thread-local accumulator the callback populated while
       walking this shard. Today only idx_count_cb batches (it amortises
       per-match atomic-adds into one per-shard-worker atomic-add); if
       more callbacks adopt the same pattern, hook them in this single
       cleanup point. No-op for callbacks that don't use TLS. */
    idx_count_cb_flush_thread();
    return NULL;
```

Replace with:

```
    /* Flush any thread-local accumulator the callback populated while
       walking this shard. Today only idx_count_cb batches (it amortises
       per-match atomic-adds into one per-shard-worker atomic-add); if
       more callbacks adopt the same pattern, hook them in this single
       cleanup point. No-op for callbacks that don't use TLS. */
#ifdef TEST_BUILD
    /* Round-3 diagnostic seam — per-shard flush trace for
       docs/plans/2026-08-30-macos-numeric-between-daemon-seam.md.
       LOG_AUDIT bypasses the LOG_LEVEL filter so the standard test
       fixture (LOG_LEVEL=2) captures it. Temporary — delete with the
       plan close-out. */
    {
        long long nb2_flushed = idx_count_cb_flush_thread_dbg();
        LOG_AUDIT(LOG_SUB_INDEX, "NB2TRACE flushed=%lld path=%s",
                  nb2_flushed, sw->idx_path);
    }
#else
    idx_count_cb_flush_thread();
#endif
    return NULL;
```

(`index.c` gets the LOG macros via `types.h`, which includes `log.h` at
types.h:1362. `LOG_AUDIT` → `YYYY-MM-DD-audit.log`, bypasses the filter.)

### 2c — `btree_dispatch` traces the encoded BETWEEN bounds

Anchor — in `src/db/query.c`, the OP_BETWEEN case of `btree_dispatch`
(line ~665). **Disambiguation:** `query.c` has five `case OP_BETWEEN:`
sites (lines ~98, ~665, ~1770, ~2278, ~2397). The target is the only one
whose two `encode_criterion_value` calls each sit on a single line and
whose next line opens `if (pc->min_exclusive || pc->max_exclusive) {`
(line ~1770's copy wraps its encode calls across two lines and follows
with `if (len1 == 0 || len2 == 0) return 0;`). The following five-line
block is unique — verified byte-for-byte against the file:

```
            break;
        case OP_BETWEEN:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            encode_criterion_value(tf, pc->value2, strlen(pc->value2), buf2, &len2);
            if (pc->min_exclusive || pc->max_exclusive) {
```

Replace with:

```
            break;
        case OP_BETWEEN:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            encode_criterion_value(tf, pc->value2, strlen(pc->value2), buf2, &len2);
#ifdef TEST_BUILD
            /* Round-3 diagnostic seam — encoded-bound trace for
               docs/plans/2026-08-30-macos-numeric-between-daemon-seam.md.
               Temporary — delete with the plan close-out. */
            {
                char nb2_lo[17] = {0}, nb2_hi[17] = {0};
                for (int i = 0; i < 8 && (size_t)i < len1; i++)
                    snprintf(nb2_lo + i * 2, 3, "%02x", buf1[i]);
                for (int i = 0; i < 8 && (size_t)i < len2; i++)
                    snprintf(nb2_hi + i * 2, 3, "%02x", buf2[i]);
                LOG_AUDIT(LOG_SUB_QUERY,
                          "NB2TRACE between len1=%zu lo=%s len2=%zu hi=%s "
                          "min_ex=%d max_ex=%d",
                          len1, nb2_lo, len2, nb2_hi,
                          pc->min_exclusive, pc->max_exclusive);
            }
#endif
            if (pc->min_exclusive || pc->max_exclusive) {
```

## Task 3 — Round-3 probe

### 3a — New file `src/test/cases/test_numeric_between_probe3.c`

Create with exactly this content:

```c
/* src/test/cases/test_numeric_between_probe3.c
 * TEMPORARY round-3 diagnostic probe — daemon-seam round for the macOS
 * numeric-BETWEEN defect, per
 * docs/plans/2026-08-30-macos-numeric-between-daemon-seam.md.
 * W  — wire repro (expected red on macOS).
 * S1 — the REAL orchestrator (cmd_count) run in-process, output captured
 *      through g_out. Discriminates "orchestrator glue" from "daemon
 *      runtime context".
 * S2 — dumps the daemon's NB2TRACE seam lines from the audit log:
 *      encoded bounds + per-shard flushed deltas, observed inside the
 *      failing process.
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

/* S2 — scan the daemon's log dir for NB2TRACE lines and dump them.
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
            if (strstr(line, "NB2TRACE")) {
                TAP_DIAG("  S2 %s: %s", de->d_name, line);
                matches++;
            }
        fclose(f);
    }
    closedir(d);
    return matches;
}

static int test_numeric_between_probe3_run(void) {
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
       object name (server.c:1622-1628 dispatch shape). */
    char dir_root[512];
    snprintf(dir_root, sizeof(dir_root), "%s/default", env.db_root);
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

    /* S2 — daemon-side seam lines from the audit log. The three wire
       counts above each fan out over 4 shards, so at least 4 trace
       lines must exist. This assert doubles as the local validation
       that the capture mechanics work. */
    int traces = s2_dump_traces(&env);
    ASSERT_EQ_INT(traces >= 4, 1, "S2 audit log holds >=4 NB2TRACE lines");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe3", test_numeric_between_probe3_run)
```

### 3b — Register the case in build.sh

Anchor (unique on `main`):

```
    src/test/cases/test_binary_index.c \
```

Replace with:

```
    src/test/cases/test_binary_index.c \
    src/test/cases/test_numeric_between_probe3.c \
```

### 3c — Local validation (Linux must be fully green)

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-numeric-between-probe3
```

- Expected: every assertion `ok` (W1, S1 rc/count, S2 traces ≥ 4), exit
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
suite` in `.github/workflows/ci.yml`:

```yaml
      # TEMPORARY (scratch branch only) — round-3 diagnostic probe for
      # docs/plans/2026-08-30-macos-numeric-between-daemon-seam.md.
      # Remove with the plan close-out.
      - name: PROBE (scratch) — numeric between round 3
        run: ./build/bin/shard-db-test run test-numeric-between-probe3
```

No `if:` — all three legs run it; Linux legs are the baseline.

### 4b — Git operations (human-run or human-directed)

The scratch commit must include the seam source files — CI cannot
compile the probe without them. The `src/db/*` diffs in this commit are
TEST_BUILD-only temporary seams; they exist only on this scratch branch
and are never merged.

```bash
git add src/test/cases/test_numeric_between_probe3.c build.sh \
        .github/workflows/ci.yml \
        docs/plans/2026-08-30-macos-numeric-between-daemon-seam.md \
        src/db/query.c src/db/query_internal.h src/db/types.h src/db/index.c
git commit -m "test: temporary numeric-between round-3 daemon-seam probe (scratch)"
git push -u origin diag/macos-numeric-between-round3
gh pr create --draft --title "scratch: numeric-between round-3 probe (do not merge)" \
  --body "Diagnostic only — see docs/plans/2026-08-30-macos-numeric-between-daemon-seam.md (committed on this branch). The src/db/* diffs are TEST_BUILD-only temporary seams. Never merge."
```

Guardrails for the `src/db/*` diffs: they must contain only the planned
Task-2 seams (2a, 2b, 2c) plus any minimal declaration the S1 probe
amendment required. If `types.h` (or any file) carries a change beyond
that, describe it in one line in the PR body and in the Task 5 halt
report so the fix-time review sees it — do not let an undocumented
hunk ride along silently.

### 4c — Evidence capture, durability, read-out

1. Wait for all three legs; save each leg's "PROBE (scratch)" log. **The
   S2 lines in that log ARE the daemon-side observation** — they carry
   the seam output, so no separate artifact is needed.
2. Append to this plan file: `## Evidence — Task 4` with the three
   labeled outputs + interpretation. Commit and push the plan to the
   scratch branch; post the three logs as PR comments. Both durability
   conditions must hold before Task 5 deletes anything.
3. Read out (macOS-only patterns; Linux must be green everywhere):

   | Observation | Localization |
   |---|---|
   | S1 = 2 (in-process) | `cmd_count_with_tree` itself — reproducible locally; bisect the orchestrator (planner→executor glue, check_primary/CompiledCriterion fast path). Likely resolvable by inspection + one targeted print — but still halt before fixing |
   | S1 = 3, NB2TRACE flushed deltas sum to 2 | the daemon's btree walkers returned 2 — stale/partial leaf content in the daemon's `bt_cache` (mmap coherence after index build); identify which shard flushed 0 and inspect that shard's cache state |
   | S1 = 3, NB2TRACE deltas sum to 3, wire = 2 | loss between `ic->count` and the wire response — `cmd_count_with_tree` response write / mode dispatch |
   | NB2TRACE lines absent on macOS (present on Linux) | the daemon never took the btree shard-walk branch — `field_index_type` / `pick_index_for_leaf` divergence (e.g. bitmap dict-scan path) |
   | NB2TRACE `between` line shows lo/hi ≠ `7fffffffffffff9c` / `8000000000000064` (len 8) | the daemon encodes different criterion bounds — compile layer |
   | Anything else | stop and ask |

## Task 5 — HALT: report root cause; fix requires a human-approved plan

1. **Stop.** Post the evidence: the matching decision-table row, both
   platforms' outputs (S2 trace lines included), and the specific defect
   mechanism (function + line + why macOS only). No fix code.
2. The human approves an amended/follow-up fix plan, which must contain:
   the root cause as a specific mechanism; complete code blocks for
   every hunk; a codebase-search listing of every consumer of the
   changed function; the regression proof (`test-binary-index` macOS
   fail-before from round 1 + green-after on all three legs — the
   permanent regression pin); the sanitizer gate; and the close-out
   edits below.
3. Platform rule: the fix must be platform-neutral C. No
   `#ifdef __APPLE__` unless byte-level evidence demands it — and that
   is a decision to escalate to the human, not improvise.
4. Delete the round-3 scratch branch and PR only after Task 4c's both
   durability conditions hold; close — never merge.

## Acceptance (whole effort — the fix plan's gate, restated)

1. `test-binary-index` 22/0 on macOS arm64, Linux x86_64, Linux arm64.
2. `ci.yml` exclusion removed: delete the comment block anchored by
   `# test-binary-index: pre-existing macOS-arm64-only failure (numeric`
   through `# docs/plans/2026-08-28-macos-arm64-numeric-between.md`, and
   remove `,test-binary-index` from the `SHARD_TEST_EXCLUDE` value line.
3. ALL probe scaffolding deleted: the three probe cases
   (`test_numeric_between_probe.c` / `…_probe2.c` / `…_probe3.c`), their
   `build.sh` list lines, the three `PROBE (scratch)` workflow steps,
   AND the three Task-2 seams from this plan (2a the
   `idx_count_cb_flush_thread_dbg` function and its guarded declaration
   — the original `void idx_count_cb_flush_thread(void)` was never
   modified; 2b shard trace; 2c bound trace).
4. Sanitizer gate on the fix diff (count path touches shared/concurrent
   state — this repo's standing gate applies, locally, before "done"):
   `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` then three fresh
   `./build/bin/shard-db-test run-all`; `BUILD_MODE=tsan SKIP_TESTS=1
   ./build.sh` then three fresh
   `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
   ./build/bin/shard-db-test run-all`.
5. No new compiler warnings; no leftover probe/debug prints or seam
   residue.
6. All three plan files' Status lines updated to done, root cause
   recorded in one paragraph in each.

## Invariants

- Do not adjust `test-binary-index`'s expectation.
- No per-platform encoding or `#ifdef __APPLE__` in any fix without
  byte-level evidence.
- The fix goes where the defect is (root cause), not where the symptom
  is observed.
- The seams and probe are diagnostic scaffolding only; none of it ships.
