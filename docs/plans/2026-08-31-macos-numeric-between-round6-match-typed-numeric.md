# Round 6 — instrument the FT_NUMERIC comparison itself

## Status

**DIAGNOSIS ONLY. No fix code in this plan.** Round 5 exonerated the
entire Kf-boundary revalidation / segment-fetch stage: on macOS arm64,
all three BETWEEN-query candidate hashes cleanly pass both `kf_reval`
(`mismatch=0`) and `seg_live` (`live=1`) — identical to both Linux legs
(PR #323, commit `b1f7ed4`, branch `diag/macos-numeric-between-round5`,
not yet closed out). This confirms suspect #6 from round 5's ranking:
the drop is strictly downstream of fetch, inside `count_batch_cb`'s call
to `criteria_match_tree(value, sc->tree, sc->fs)` acting on the fetched
record bytes. This plan places one seam directly in the FT_NUMERIC
comparison path to capture, per fetched record, the decoded value and
the compiled bounds actually used.

## Where round 5 left off

Evidence (PR #323, commit `b1f7ed4`, branch
`diag/macos-numeric-between-round5`, not yet closed out):

- All three platforms: `fetch_in op_between=1` count=3 (round 4's
  `collect count=3` reconfirmed one hop later).
- macOS arm64: all three hashes show `kf_reval mismatch=0` and
  `seg_live live=1` — i.e. every one of the 3 candidates is resolved,
  revalidated, and found live, with `count_batch_cb` invoked for all 3.
- Wire result is still 2 on macOS, 3 on both Linux legs, for the same
  build.

Since `count_batch_cb` (query.c:908-919) does exactly two things per
invocation — a deadline check, then
`criteria_match_tree((const uint8_t *)value, c->sc->tree, c->sc->fs)`
gating the atomic increment — and the deadline check is a shared,
platform-neutral counter unrelated to record content, the loss must be
inside `criteria_match_tree` returning 0 for a record it should return 1
for.

## Confirmed call path through the comparison (verified 2026-08-31)

For this exact query (single `{"field":"amt","op":"between",...}` leaf,
no AND/OR):

```
criteria_match_tree (query_plan.c:1694)
  n->kind == CNODE_LEAF → match_typed(rec, n->compiled, fs)
match_typed (query_plan.c:894)
  cc->composite == 0, cc->tf != NULL, cc->op == OP_BETWEEN (not a
  field-vs-field op) → falls through to the typed switch
  f->type == FT_NUMERIC (query_plan.c:938-941):
      int64_t v = ld_be_i64(p);              /* p = rec + f->offset */
      return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64,
                        cc->in_count, cc);
cmp_op_i64 (query_plan.c:768-793), OP_BETWEEN branch:
      int lo = (cc && cc->raw && cc->raw->min_exclusive) ? (v > q1) : (v >= q1);
      int hi = (cc && cc->raw && cc->raw->max_exclusive) ? (v < q2) : (v <= q2);
      return lo && hi;
```

## Ruled out during round-6 prep

- `cmp_op_i64`'s `OP_BETWEEN` branch (query_plan.c:778-782): pure
  `int64_t` comparisons, no floating point, no UB, no platform-specific
  operator behavior.
- `ld_be_i64` (query_internal.h): explicit big-endian byte-shift
  reconstruction, no host-endianness dependency, identical on x86_64
  and arm64.
- `compile_one`'s `FT_NUMERIC` branch (query_plan.c:355-358) and the
  record-encode `FT_NUMERIC` case (config.c:1827-1836) use the
  *identical* `atof(s)` → `(int64_t)(dv * mul + (dv>=0?0.5:-0.5))`
  formula, sharing the same `f->numeric_scale_mult`. For this test's
  literal bounds (`"-1"`, `"1"`) and inserted values
  (`"-999.99","-0.01","0","0.01","999.99"`), every intermediate `dv`
  and `dv*mul` is either an exact integer in double or far enough from
  any 0.5 tie (the encode: `-0.01*100` and `0.01*100` land within
  double's ~1e-16 relative precision of ±1.0, nowhere near the ±0.5
  fudge factor's rounding boundary) that a 1-ULP cross-platform
  difference cannot flip the truncated result. This formula is also not
  actually exercised near its true tie boundary here: the compiled query
  bounds are `i1=-100, i2=100` (from `"-1"`/`"1"` at scale 2), while the
  three expected-match records encode to `v ∈ {-1, 0, 1}` — deep inside
  `[-100, 100]`, not at either edge. **Floating-point rounding at the
  encode/parse boundary is ruled out as the mechanism**; if the defect
  were there, evidence would have to come from a record landing on the
  *wrong side of zero itself* (e.g. `-0.01` encoding to `+1` or vice
  versa), which round 6's seam (below) will also directly rule in or out
  by logging the exact decoded `v` per record.
- `f->numeric_scale_mult` (config.c:1494-1497): computed via an integer
  `for` loop (`mult *= 10`), no floating point, no libm `pow()` — fully
  deterministic and platform-neutral, and shared by both the encode and
  compile paths (so it cannot itself cause the two paths to disagree).

## What this plan does

One `TEST_BUILD`-only seam, in `src/db/query_plan.c`'s `match_typed`,
`FT_NUMERIC` case: log the decoded record value (`v`), the compiled
bounds (`cc->i1`, `cc->i2`), the exclusivity flags, the raw query
literals (`cc->raw->value`, `cc->raw->value2`), the field name, and the
final boolean result, for every FT_NUMERIC comparison. No hash is
available at this call depth (`match_typed` isn't passed one), but it
isn't needed: this test's 5 inserted records encode to 5 distinct `v`
values (`-99999, -1, 0, 1, 99999`), so each trace line unambiguously
identifies which record it came from. If `v` and `cc->i1`/`cc->i2` are
byte-identical to Linux for all 5 records and the boolean result still
comes out wrong for one of them on macOS, that means the divergence is
in `cmp_op_i64`'s pure integer comparison itself or in something not
captured here (e.g. `cc->raw->min_exclusive`/`max_exclusive` populated
differently) — which the same trace line's exclusivity fields would
also directly reveal, since they're logged from the same `cc` used in
the real call.

## Suspect ranking entering round 6

1. **`cmp_op_i64` OP_BETWEEN logic itself misbehaves on macOS/arm64
   despite reading identically** — e.g. a codegen bug (unlikely for
   plain `int64_t` comparisons, but not yet empirically ruled out on the
   actual macOS binary).
2. **`v` (decoded record value) differs from the expected `-1/0/1` for
   one record, on macOS only** — i.e. the record's on-disk bytes at
   `f->offset` are correct (round 5 proved correct segment bytes reach
   `count_batch_cb`), but `f->offset` itself is wrong on macOS (a
   `TypedField` layout/offset computation bug), pointing `ld_be_i64` at
   the wrong 8 bytes of the record.
3. **`cc->i1`/`cc->i2` differ from the expected `-100/100`, on macOS
   only** — despite the static-analysis exoneration above, an actual
   runtime divergence in the compiled bounds (e.g. `compile_one` reads a
   different `c->value`/`c->value2` string on macOS, or
   `numeric_scale_mult` differs at runtime from what config.c computes
   at schema-load time on that platform).
4. **`cc->raw->min_exclusive`/`max_exclusive` set to 1 on macOS when
   they should be 0** — would make the between comparison strict on one
   side, excluding exactly one boundary-adjacent record. Not yet traced
   anywhere; this seam's exclusivity fields are the first direct check.
5. **The wrong `CompiledCriterion`/`TypedField` for this query reaches
   `match_typed` on macOS** (e.g. a stale/aliased schema cache pointer)
   — would show up as `field=` not matching `"amt"`, or `i1`/`i2` not
   matching `-100/100`, for one or more of the 5 trace lines.

## Embedded execution rules

- Branch off `main`: `diag/macos-numeric-between-round6`.
- Do tasks in order. Build: `SKIP_TESTS=1 ./build.sh`. Test:
  `./build/bin/shard-db-test run-all` or
  `./build/bin/shard-db-test run <name>`.
- If a quoted anchor isn't found exactly in the current tree, write
  `PLAN_NOTES.md` describing the mismatch and halt the entire execution
  run immediately — do not guess, reinterpret, or continue to any
  further task, even an unrelated one.
- If you hit a decision this plan doesn't cover, stop and ask.
- Per this repo's AGENTS.md standing exception, this diagnostic round's
  evidence (probe results, log excerpts) is committed and pushed
  directly to the scratch branch as it's gathered — it does not wait for
  the normal uncommitted-until-reviewed flow, mirroring rounds 1-5.
- This plan is diagnosis-only. Do not modify production behavior in
  `query_plan.c` beyond the `#ifdef TEST_BUILD` seam below. No fix, no
  cleanup beyond what's specified in Task 1.

## Task 1 — close out round 5

1. `gh pr view 323` to confirm current state, then
   `gh pr comment 323 --body "Superseded by round 6 (docs/plans/2026-08-31-macos-numeric-between-round6-match-typed-numeric.md); evidence already committed above. Closing without merge."`
   followed by `gh pr close 323`.
2. On branch `diag/macos-numeric-between-round5`, revert round 5's seam
   in `src/db/slotcask.c`. Anchor is the full `kf_reval_fetch_one`
   function as it currently stands on that branch (the version this
   plan's own "Seam B" section installed) — replace it in full with the
   pre-round-5 version below (verify against
   `git show main:src/db/slotcask.c` for the function's original body
   before making any edit; if it does not match exactly, stop and write
   `PLAN_NOTES.md` rather than guessing):

```c
static void kf_reval_fetch_one(KfRevalFetchArg *fa) {
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, fa->db->data_dir, fa->kf_shard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, fa->db->slots_per_shard, 0) != 0) {
        /* Whole partition unreadable — retire every record in it. */
        for (size_t i = 0; i < fa->count; i++)
            fa->recs[fa->start + i].sid = 0xFF;
        return;
    }

    for (size_t i = 0; i < fa->count; i++) {
        SlotcaskResolvedRec *r = &fa->recs[fa->start + i];
        uint8_t flag = 0, sid = 0;
        uint16_t fid = 0;
        uint32_t off = 0;
        size_t slot = 0;
        if (kf_lookup_no_verify(&kh, r->hash, &flag, &sid, &fid, &off,
                                &slot) != 0 ||
            flag != 1 || sid != r->sid || fid != r->fid || off != r->off)
            r->sid = 0xFF;  /* repointed or gone since resolve */
    }

    /* Compact survivors within the slice (disjoint per partition). */
    size_t live_n = 0;
    for (size_t i = 0; i < fa->count; i++) {
        SlotcaskResolvedRec *r = &fa->recs[fa->start + i];
        if (r->sid != 0xFF) fa->recs[fa->start + live_n++] = *r;
    }

    if (live_n > 0) {
        qsort(&fa->recs[fa->start], live_n, sizeof(SlotcaskResolvedRec),
              compare_sid_fid_off);
        /* Copy every survivor's bytes under the STILL-HELD reader. */
        size_t run_start = 0;
        for (size_t i = 0; i < live_n; i++) {
            int last = (i == live_n - 1);
            if (!last &&
                fa->recs[fa->start + i].sid ==
                    fa->recs[fa->start + i + 1].sid &&
                fa->recs[fa->start + i].fid ==
                    fa->recs[fa->start + i + 1].fid)
                continue;
            char seg_path[PATH_MAX];
            SlotcaskSegHandle h;
            seg_path_for(seg_path, fa->db->data_dir,
                         fa->recs[fa->start + run_start].sid,
                         fa->recs[fa->start + run_start].fid);
            if (segcache_acquire(&h, seg_path, 0, 0, 0) == 0) {
                for (size_t j = run_start; j <= i; j++) {
                    const SlotcaskResolvedRec *r =
                        &fa->recs[fa->start + j];
                    const uint8_t *rec = h.map + r->off;
                    if (!seg_rec_live_with_hash(rec, r->hash)) continue;
                    uint16_t klen = seg_rec_klen(rec);
                    uint32_t vlen = seg_rec_vlen(rec);
                    if (fa->cb(r->hash, rec + 24, klen,
                               rec + 24 + klen, vlen, fa->ctx) != 0)
                        break;
                }
                segcache_release(&h);
            }
            run_start = i + 1;
        }
    }

    kfcache_release(&kh);
}
```

   Also remove the round-5 explicit `#include "log.h"` line (anchor:
   `#include "log.h"  /* LOG_AUDIT/LOG_SUB_SLOTCASK — round-5
   diagnostic seam only */` in `src/db/slotcask.c`). This include is
   safe to remove regardless of other `LOG_*` macro uses remaining in
   the file: `slotcask.c` already includes `types.h`, which itself
   includes `log.h` (`types.h:1362`) — confirmed by `main`'s own
   pre-round-5 `slotcask.c`, which has no explicit `log.h` include yet
   already calls `LOG_INFO(LOG_SUB_SLOTCASK, ...)` (around line 2078)
   successfully via that transitive path. Do not gate this deletion on
   a "no other LOG_AUDIT/LOG_SUB_SLOTCASK use remains" grep — that
   precondition is false on `main` itself (the pre-existing `LOG_INFO`
   call) and isn't what makes the removal safe. After deleting the
   line, confirm via `SKIP_TESTS=1 ./build.sh` (Task 1 step 6, below)
   that the file still compiles clean.
3. Delete `src/test/cases/test_numeric_between_probe5.c`.
4. In `build.sh`, remove the line
   `    src/test/cases/test_numeric_between_probe5.c \`
   from the `shard-db-test` build command's source list.
5. In `.github/workflows/ci.yml`, remove this step (immediately after
   the `Build` step):

```yaml
      # TEMPORARY (scratch branch only) — round-5 diagnostic probe for
      # docs/plans/2026-08-31-macos-numeric-between-round5-kf-fetch-drop.md.
      # Remove with the plan close-out.
      - name: PROBE (scratch) — numeric between round 5
        run: ./build/bin/shard-db-test run test-numeric-between-probe5
```

6. `SKIP_TESTS=1 ./build.sh` — confirm it builds clean with the seam
   gone.
7. Commit the revert (`fix: close out round-5 diagnostic seam` or
   similar), push to `diag/macos-numeric-between-round5` so the
   branch's history stays clean before it's retired.
8. Switch away and retire the branch:

```bash
git checkout main
git branch -D diag/macos-numeric-between-round5
git push origin --delete diag/macos-numeric-between-round5
git checkout -b diag/macos-numeric-between-round6 main
git status   # expect clean tree; untracked docs/plans/*.md remain
```

## Task 2 — new seam

In `src/db/query_plan.c`, `match_typed`. Anchor (currently present,
verified 2026-08-31):

```c
    case FT_NUMERIC: {
        int64_t v = ld_be_i64(p);
        return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
    }
```

becomes:

```c
    case FT_NUMERIC: {
        int64_t v = ld_be_i64(p);
#ifdef TEST_BUILD
        /* Round-6 diagnostic seam — round 5 proved every BETWEEN
           candidate's fetched record bytes reach count_batch_cb intact
           on macOS (kf_reval mismatch=0, seg_live live=1 for all 3).
           This traces the FT_NUMERIC comparison itself: the decoded
           record value, the compiled query bounds, the exclusivity
           flags, and the raw query literals they were parsed from — so
           a divergence in any one of those (vs. the pure comparison
           logic in cmp_op_i64) is directly visible instead of inferred.
           Temporary — delete with the plan close-out. */
        int nb2_r = cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64,
                               cc->in_count, cc);
        LOG_AUDIT(LOG_SUB_QUERY,
                  "NB2TRACE6 numeric_cmp field=%s op=%d op_between=%d "
                  "v=%lld i1=%lld i2=%lld min_excl=%d max_excl=%d "
                  "raw_v1=%s raw_v2=%s result=%d",
                  cc->raw ? cc->raw->field : "?", (int)cc->op,
                  cc->op == OP_BETWEEN,
                  (long long)v, (long long)cc->i1, (long long)cc->i2,
                  (cc->raw ? cc->raw->min_exclusive : -1),
                  (cc->raw ? cc->raw->max_exclusive : -1),
                  (cc->raw && cc->raw->value[0]) ? cc->raw->value : "?",
                  (cc->raw && cc->raw->value2[0]) ? cc->raw->value2 : "?",
                  nb2_r);
        return nb2_r;
#else
        return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
#endif
    }
```

Build check:

```bash
SKIP_TESTS=1 ./build.sh
```

Must succeed with no new warnings.

## Task 3 — probe test

Create `src/test/cases/test_numeric_between_probe6.c`:

```c
/* src/test/cases/test_numeric_between_probe6.c
 * TEMPORARY round-6 diagnostic probe — instruments the FT_NUMERIC
 * comparison itself, per
 * docs/plans/2026-08-31-macos-numeric-between-round6-match-typed-numeric.md.
 * Round 5 proved every candidate's fetched bytes reach count_batch_cb
 * intact on macOS; this probe captures match_typed's decoded value and
 * compiled bounds directly.
 * W  — wire repro (expected red on macOS). Also the source of the
 *      NB2TRACE6 seam lines S2 reads.
 * S2 — dumps the daemon's NB2TRACE6 seam lines from the audit log. The
 *      exact per-record pass/fail is a human/CI read-out (see the
 *      plan's Task 4c table), not asserted here — only that capture
 *      works reliably.
 * Expected to FAIL on macOS arm64 until the defect is fixed (W1); must
 * pass 100% on Linux. Delete with the plan close-out.
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

/* S2 — scan the daemon's log dir for NB2TRACE6 lines matching `substr`
   and dump them. LOG_DIR for the standard fixture is
   <parent-of-db_root>/logs. */
static int s2_count_matching(TestEnv *env, const char *substr) {
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
            if (strstr(line, "NB2TRACE6") && strstr(line, substr)) {
                TAP_DIAG("  S2 %s: %s", de->d_name, line);
                matches++;
            }
        fclose(f);
    }
    closedir(d);
    return matches;
}

static int test_numeric_between_probe6_run(void) {
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

    /* S2 — numeric_cmp seam lines from the audit log, produced by the
       three W1 wire queries above. Exactly one numeric_cmp line fires
       per record actually reaching match_typed for a given query, so
       op_between=1 lines are a proven-invariant regression check: round
       5 already showed all 3 between-candidates reach count_batch_cb on
       every platform (fetch_in=3, kf_reval mismatch=0 x3, seg_live
       live=1 x3), and match_typed is called unconditionally for each.
       The individual v=/i1=/i2=/result= fields are a human/CI read-out
       (plan Task 4c), not asserted here. */
    int between_lines = s2_count_matching(&env, "op_between=1");
    ASSERT_EQ_INT(between_lines, 3,
        "S2 audit log holds exactly 3 numeric_cmp op_between=1 lines");

    /* 3 W1 queries reach match_typed once per candidate that gets as
       far as count_batch_cb: between=3 (round 5), lt 0=2 (control's own
       expected count), gte 0=3 (control's own expected count) = 8
       total. Not a loose minimum — this is the same proven-invariant
       reasoning as the op_between=1 assertion above, applied to all
       three queries at once. */
    int total = s2_count_matching(&env, "");
    ASSERT_EQ_INT(total, 8,
        "S2 audit log holds exactly 8 NB2TRACE6 lines "
        "(3 between + 2 lt + 3 gte)");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe6", test_numeric_between_probe6_run)
```

Registration — in `build.sh`, add
`    src/test/cases/test_numeric_between_probe6.c \`
to the `shard-db-test` build command's source list, in the same spot
`test_numeric_between_probe5.c` occupied before Task 1 removed it
(right after `test_binary_index.c`).

Local validation (Linux dev machine — expected fully green here; the
defect is macOS-only):

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-numeric-between-probe6
```

Paste the full output. If `test-numeric-between-probe6` fails locally on
Linux, treat that as a plan/seam bug (not the macOS defect) — stop and
write `PLAN_NOTES.md` rather than proceeding to Task 4.

## Task 4 — CI evidence capture

a. In `.github/workflows/ci.yml`, add (mirroring round 5's now-removed
   step, immediately after the `Build` step):

```yaml
      # TEMPORARY (scratch branch only) — round-6 diagnostic probe for
      # docs/plans/2026-08-31-macos-numeric-between-round6-match-typed-numeric.md.
      # Remove with the plan close-out.
      - name: PROBE (scratch) — numeric between round 6
        run: ./build/bin/shard-db-test run test-numeric-between-probe6
```

b. Commit and push to `diag/macos-numeric-between-round6`. Open a draft
   PR against `main` (title: `test: temporary numeric-between round-6
   match_typed probe (scratch)`, body notes it's diagnosis-only, links
   this plan, and will be closed without merge). Confirm CI runs on all
   three legs (linux x86_64, linux arm64, macos arm64) and wait for
   completion.

c. Read-out: pull each leg's `test-numeric-between-probe6` job log, grep
   for `NB2TRACE6`, and record the full set of `op_between=1` lines (5
   values inserted → up to 5 distinct `v=` lines possible per query,
   though only records actually reaching `match_typed` produce a line —
   round 5 proved that's all 5 for `lt`/`gte` controls and specifically
   the 3 between-candidates for the between query) in an
   `## Evidence — Task 4` section appended to this plan. Cross-reference
   Linux vs. macOS line-by-line using `v=` as the record identity key
   (the 5 inserted records encode to distinct `v ∈ {-99999, -1, 0, 1,
   99999}`). Apply this table:

   | Observation | Interpretation |
   |---|---|
   | For every `v` value present on Linux, the exact same `v=`/`i1=`/`i2=`/`min_excl=`/`max_excl=`/`raw_v1=`/`raw_v2=` set appears on macOS, but `result=` differs for one line (Linux `result=1`, macOS `result=0`, same `v`/`i1`/`i2`/flags on both) | **Confirms suspect #1** — `cmp_op_i64`'s pure integer comparison itself produces a different result from identical inputs on macOS/arm64. Root cause is in codegen/ABI for that comparison (compiler bug, miscompilation, or an aliasing/UB issue not yet identified) — needs a minimal standalone repro of `cmp_op_i64` outside the daemon. |
   | macOS is missing a `numeric_cmp` line for one of the `v` values Linux has (e.g. Linux logs `v=-1`, `v=0`, `v=1`; macOS logs only two of those three under `op_between=1`) — but round 5 already proved all 3 candidates reach `count_batch_cb` | **Contradiction with round 5** — would mean a record enters `match_typed` via a different `switch` case (not `FT_NUMERIC`) or `cc->composite`/field-vs-field short-circuits before reaching this seam on macOS. Stop and write `PLAN_NOTES.md`; do not guess further — this needs a fresh instrumentation pass at the top of `match_typed`, out of this plan's scope. |
   | macOS shows a `v=` value that does NOT match any of `{-99999, -1, 0, 1, 99999}` for one line, where Linux shows the expected value at the same logical record | **Confirms suspect #2** — `f->offset` (or the record bytes at that offset) is wrong on macOS; `ld_be_i64` is decoding the wrong bytes. Root cause is in `TypedField.offset` computation (config.c schema layout) or a record-write bug specific to this field's position, not in the comparison logic. |
   | macOS shows `i1=`/`i2=` different from `-100`/`100` for the between-query lines (Linux shows `-100`/`100`) | **Confirms suspect #3** — `compile_one`'s `FT_NUMERIC` bound parsing (or the `numeric_scale_mult` it reads) produces a different runtime value on macOS despite reading identically in source. Root cause is in `parse_numeric_i64` or `TypedField.numeric_scale_mult` at actual runtime on that platform — needs a targeted print of `f->numeric_scale_mult` at schema-load time next. |
   | macOS shows `min_excl=1` or `max_excl=1` for the between-query lines where Linux shows `0`/`0` | **Confirms suspect #4** — `cc->raw->min_exclusive`/`max_exclusive` is being set (or read) incorrectly on macOS. Root cause is in the criteria-JSON-parse or planner coalesce path that populates `SearchCriterion.min_exclusive`/`max_exclusive`, not in `compile_one`'s numeric branch. |
   | macOS shows `field=` not equal to `"amt"`, or `raw_v1=`/`raw_v2=` not equal to `"-1"`/`"1"`, for a between-query line | **Confirms suspect #5** — the wrong `CompiledCriterion`/`SearchCriterion` reaches this call on macOS (stale/aliased pointer, or cross-query cache confusion). Root cause is upstream of `match_typed`, in whatever passes `cc` down through `criteria_match_tree`/`count_batch_cb`'s `sc->tree`. |
   | All fields identical across platforms for all 3 candidate lines, `result=1` for all 3 on both platforms, yet W1's between query still returned 2 on macOS | **Contradiction** — would mean the loss is not in `count_batch_cb`/`criteria_match_tree` after all, despite this being the last remaining stage per round 5. Stop and write `PLAN_NOTES.md`; the call-path assumption in this plan's "Confirmed call path" section needs to be re-verified against the actual macOS binary (e.g. a stale/mismatched CI artifact, or a second `count`-path this plan didn't trace). |

   Quote the exact log lines, not a paraphrase, for whichever row
   applies.

## Task 5 — HALT

Do not write or propose a fix. Do not modify `match_typed`, `cmp_op_i64`,
`ld_be_i64`, `compile_one`, `parse_numeric_i64`, or any other production
code beyond the Task 2 edit. Post the Task 4c table and raw evidence as a
PR comment. Leave the draft PR open, unmerged, pending human review and
a round-7 (or fix) plan.

## Acceptance criteria

- Task 1's revert leaves `src/db/slotcask.c`, `build.sh`, and
  `.github/workflows/ci.yml` byte-identical to `main` before round 6's
  own edits are applied (verify via `git diff main -- src/db/slotcask.c
  build.sh .github/workflows/ci.yml` immediately after Task 1 step 6,
  expect empty).
- `SKIP_TESTS=1 ./build.sh` succeeds after every task.
- `test-numeric-between-probe6` passes on Linux x86_64 and Linux arm64
  in CI.
- CI evidence for all three legs is captured verbatim in this plan's
  `## Evidence — Task 4` section before Task 5's HALT.
- No fix code anywhere in the diff.

## Invariants

- The seam is `#ifdef TEST_BUILD`-guarded with an `#else` branch that
  keeps the release-build code path textually identical to the
  original `return cmp_op_i64(...)` — `SKIP_TESTS=1 ./build.sh` (which
  does not define `TEST_BUILD` for the production `shard-db` binary)
  must produce byte-identical release-path behavior to `main`.
- No new dependency; `log.h` is already reachable in `query_plan.c`
  transitively via `types.h` (`types.h:1362`), so no new `#include` is
  needed (unlike round 5's `slotcask.c`, which required one).

## Evidence — Task 4

CI run `33407282653`: Linux x86_64 and Linux arm64 passed; macOS arm64
failed W1 (`expected 3 got 2`). The exact `op_between=1` comparison
lines were:

```text
linux x86_64
NB2TRACE6 numeric_cmp field=amt op=14 op_between=1 v=-1 i1=-100 i2=100 min_excl=0 max_excl=0 raw_v1=-1 raw_v2=1 result=1
NB2TRACE6 numeric_cmp field=amt op=14 op_between=1 v=1 i1=-100 i2=100 min_excl=0 max_excl=0 raw_v1=-1 raw_v2=1 result=1
NB2TRACE6 numeric_cmp field=amt op=14 op_between=1 v=0 i1=-100 i2=100 min_excl=0 max_excl=0 raw_v1=-1 raw_v2=1 result=1

linux arm64
NB2TRACE6 numeric_cmp field=amt op=14 op_between=1 v=-1 i1=-100 i2=100 min_excl=0 max_excl=0 raw_v1=-1 raw_v2=1 result=1
NB2TRACE6 numeric_cmp field=amt op=14 op_between=1 v=1 i1=-100 i2=100 min_excl=0 max_excl=0 raw_v1=-1 raw_v2=1 result=1
NB2TRACE6 numeric_cmp field=amt op=14 op_between=1 v=0 i1=-100 i2=100 min_excl=0 max_excl=0 raw_v1=-1 raw_v2=1 result=1

macOS arm64
NB2TRACE6 numeric_cmp field=amt op=14 op_between=1 v=-1 i1=-100 i2=100 min_excl=0 max_excl=0 raw_v1=-1 raw_v2=1 result=1
NB2TRACE6 numeric_cmp field=amt op=14 op_between=1 v=1 i1=-100 i2=100 min_excl=0 max_excl=0 raw_v1=-1 raw_v2=1 result=1
NB2TRACE6 numeric_cmp field=amt op=14 op_between=1 v=5999791 i1=-100 i2=100 min_excl=0 max_excl=0 raw_v1=-1 raw_v2=1 result=0
```

Decision-table row: macOS decodes one expected candidate as `5999791`,
not one of the fixture's valid encodings (`-99999`, `-1`, `0`, `1`,
`99999`). This confirms suspect #2: the numeric comparison receives
wrong bytes for that record (either `TypedField.offset` is wrong or the
record was written incorrectly). The compiled bounds, flags, and raw
literals are identical across all three legs. This diagnostic round
halts here; no production fix was attempted.

The separate Linux TSan workflow timed out at its 900-second watchdog
in `test-rebuild-validation`; it is tracked separately and is not part
of this round-6 numeric evidence.
