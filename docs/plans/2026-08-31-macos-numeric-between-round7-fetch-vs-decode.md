# Round 7 — bisect kf_reval_fetch_one's record-fetch point vs match_typed's decode point

## Status

Diagnosis-only. No fix code authorized in this round. Every task below
runs on a fresh branch off `main`; nothing merges without a human
go-ahead on a later, separate plan once root cause is confirmed.

## Where round 6 left off

Round 6 (branch `diag/macos-numeric-between-round6`, PR #324, evidence
commit `1026df9`) instrumented `match_typed`'s `FT_NUMERIC` case
directly. Exact captured evidence (CI run `33407282653`):

```text
linux x86_64 (3rd between candidate, key n_2, expected amt=0)
NB2TRACE6 numeric_cmp field=amt op=14 op_between=1 v=0 i1=-100 i2=100 min_excl=0 max_excl=0 raw_v1=-1 raw_v2=1 result=1

linux arm64 (same)
NB2TRACE6 numeric_cmp field=amt op=14 op_between=1 v=0 i1=-100 i2=100 min_excl=0 max_excl=0 raw_v1=-1 raw_v2=1 result=1

macOS arm64 (same)
NB2TRACE6 numeric_cmp field=amt op=14 op_between=1 v=5999791 i1=-100 i2=100 min_excl=0 max_excl=0 raw_v1=-1 raw_v2=1 result=0
```

Compiled bounds (`i1=-100 i2=100`), exclusivity flags, and the raw
query literals (`raw_v1=-1 raw_v2=1`) are identical across all three
legs. Only the decoded record value `v` differs, and only for the
`key=n_2` (value `"0"`) candidate. This rules out `cmp_op_i64`'s
comparison logic, the compiled-criterion identity, and (since the test
object `bi_num` has exactly one field, so `f->offset` is trivially and
unconditionally `0`) a `TypedField.offset` misalignment. The defect is
in the record bytes `match_typed` reads, or in how the pointer to them
is computed.

## Ruled out during round-7 prep

- **Both `FT_NUMERIC` encoders** (`encode_field_len` at
  `config.c:1827-1836`, used for the record body; `encode_field_for_index`
  at `config.c:2087-2096`, used for the B+-tree index key) compute the
  scaled value via `atof(cbuf) * numeric_scale_mult` and are
  mathematically forced to produce `v=0` for the literal input `"0"`
  on any IEEE-754-compliant platform. Read in full this round; both
  are exonerated as the source of `5999791` for this record,
  regardless of platform.
- **`cbuf_from_span`** (`config.c:1641-1646`) correctly null-terminates
  at the copied length (`buf[cl] = '\0'`) — an uninitialized-buffer /
  missing-null-terminator read at either `FT_NUMERIC` encode call site
  is ruled out.
- **`typed_encode_defaults`** (`config.c:2705-2817`), the actual entry
  point for the JSON-insert path (`storage.c:963`), passes the raw JSON
  value span (`ev`, `el` — from `json_skip_value`) straight to
  `encode_field_len` without going through a null-terminated
  intermediate string for non-varchar fields. `json_skip_value` and
  `json_skip` (`util.c:139-189`) were read in full: every comparison is
  an equality check against an ASCII delimiter byte (`' '`, `'\t'`,
  `'{'`, `','`, etc.) — none compares `*p` as `<`/`>` a numeric
  threshold, so signed-vs-unsigned `char` promotion (a genuine ABI
  difference between Apple's arm64 clang, which defaults `char` to
  signed, and Linux arm64's AAPCS default of unsigned `char`) cannot
  affect this parse. For the literal insert `{"amt":0}`, `json_skip_value`
  correctly stops at the closing `}`, yielding `el=1`, `ev="0"` —
  ruled out as a source of a wrong value/length span.
- **`seg_rec_klen` / `seg_rec_vlen`** (`slotcask.c:1890-1895`) are plain
  2-byte / 4-byte `memcpy`s from fixed offsets within the record header,
  with no byte-swap or type conversion in either direction. **`seg_record_emit`**
  (`slotcask.c:3544-3568`), the writer, is the same: `memcpy` of the raw
  `klen`/`vlen` values into the header, `memcpy` of key and value bytes
  immediately after. Both directions are byte-for-byte opaque — there is
  no platform-dependent serialization step here for either the accessors
  or the writer to diverge on.
- **No intermediate copy exists between the record fetch and
  `match_typed`'s read.** `kf_reval_fetch_one` (`slotcask.c:6283-6350`)
  computes `rec = h.map + r->off` (a pointer straight into the mmap'd
  segment file), then calls `fa->cb(r->hash, rec + 24, klen, rec + 24 +
  klen, vlen, fa->ctx)` — passing that raw pointer on. `count_batch_cb`
  (`query.c:908-919`) forwards `value` unchanged into
  `criteria_match_tree`, which forwards it unchanged into `match_typed`,
  which does `int64_t v = ld_be_i64(p);` directly on it. No `memcpy` into
  a scratch/stack buffer exists anywhere in this chain. This means round
  6's seam (at the very end of the chain) and a new seam at the very
  start of the chain (inside `kf_reval_fetch_one`, right before the
  `fa->cb` call) are reading the *same bytes through the same pointer* —
  any divergence between what the two seams observe, in the same run,
  would mean either the underlying mmap page changed between the two
  reads (a concurrency bug) or one of the two seams itself is wrong.

## What this plan does

Round 6 only had one observation point (decode time). This round adds a
**second, earlier observation point** — at the record-fetch call site
inside `kf_reval_fetch_one`, immediately before the bytes are handed to
the callback — and keeps a decode-time seam (round 6's, re-applied under
a new tag) **active in the same build and the same CI run**. This
directly answers: are the bytes already wrong when they're first read
off the mmap'd segment, or do they go wrong somewhere in the handoff to
`match_typed` (which round 5/6 have shown has no code path to actually
do that, but the plan verifies it empirically rather than by source
reading alone)?

## Suspect ranking entering round 7

1. **On-disk record bytes are wrong for this record's value region on
   macOS** — either the write-time encode produced wrong bytes despite
   being platform-neutral in isolation (suggests a **second write**
   later corrupted this record, e.g. a stray write from an unrelated
   field/record whose offset computation is wrong), or the value bytes
   were never written correctly to begin with due to a bug this plan's
   round-6 prep didn't cover (e.g. `seg_record_emit`'s caller passing a
   wrong `vlen`/`value` pointer at insert time).
2. **`r->off` (the resolved segment offset) is wrong for this record on
   macOS** — `kf_reval_fetch_one`'s `rec = h.map + r->off` would then
   point at a different record's bytes entirely, one that happens to
   still pass `seg_rec_live_with_hash` (same live flag, unrelated
   hash — vanishingly unlikely to coincidentally match `r->hash` unless
   the resolve step reused a stale/wrong offset for the *same* hash,
   e.g. a stale KF-shard cache entry from a prior write to the same
   key). Round 5's `kf_reval mismatch=0` already showed the *shard-file
   resolve* (`kf_lookup_no_verify`) matches on macOS — this suspect
   would mean the mismatch is upstream of that check, in whatever
   populated `r->off` before revalidation ran.
3. **A concurrency bug**: some other thread (auto-vacuum, auto-reshard,
   background index maintenance) mutates the mmap'd segment page for
   this record between the fetch-point seam and the decode-point seam
   within the same query. Both seams read through the same pointer with
   no lock re-acquired in between at the `match_typed` call site, so a
   genuinely racing writer could produce exactly this signature. Low
   prior probability (this specific test has no concurrent writer
   thread active), but the two-seam-same-run design directly tests for
   it regardless.
4. **Compiler miscompilation / UB specific to Apple clang's codegen**
   for `ld_be_i64` or the pointer arithmetic chain — would show as
   correct bytes at the fetch-point seam (Seam A) but wrong at the
   decode-point seam (Seam B) with no possible corrupting write in
   between. Lowest prior probability given both seams execute the same
   straightforward pointer-follow, but the two-seam design directly
   distinguishes this from suspect 1/2 if it's real.

## Embedded execution rules

- Order: the round-6 close-out section below runs first, entirely on
  the existing `diag/macos-numeric-between-round6` branch; only after
  it lands does Task 1 branch `diag/macos-numeric-between-round7` off
  `main`. Do not reorder — reverting round 6's seam from a fresh
  round-7 branch would fail immediately, since `main` never had it.
- Do tasks in order after that; do not skip ahead.
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run
  test-numeric-between-probe7` (and `run-all` only where a task says so).
- This repo's standing exception permits pushing diagnostic-round
  evidence commits directly (no separate PR-review gate) to this scratch
  branch — evidence must still be captured in this plan file and posted
  to the round's own PR, per Task 4.
- If a quoted anchor below isn't found **exactly** in the current tree,
  stop immediately: write `PLAN_NOTES.md` at the repo root describing
  the mismatch (file, expected anchor, what's actually there) and halt
  the entire run — do not guess, reinterpret, or continue to any further
  task, even an unrelated one.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.

## Round-6 close-out (prerequisite — run first, on `diag/macos-numeric-between-round6`)

Round 6's seam and probe scaffolding exist only on the
`diag/macos-numeric-between-round6` branch — `main` never had them
(confirmed via `git diff main -- src/db/query_plan.c src/db/slotcask.c
build.sh .github/workflows/ci.yml` on that branch: `query_plan.c` +27,
`build.sh` +1, `.github/workflows/ci.yml` +6, `slotcask.c` empty). So
the revert must happen on `diag/macos-numeric-between-round6` itself,
*before* branching round 7 off `main` — branching round 7 first and
then trying to "revert" a seam that was never there would immediately
fail this plan's own exact-anchor rule.

1. `git checkout diag/macos-numeric-between-round6 && git pull`.
2. Revert the round-6 seam in `src/db/query_plan.c` back to the plain
   pre-round-6 form. Anchor (current round-6 state, to be replaced):

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

   Replace with:

   ```c
       case FT_NUMERIC: {
           int64_t v = ld_be_i64(p);
           return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
       }
   ```

3. Delete `src/test/cases/test_numeric_between_probe6.c`.
4. Remove the round-6 CI probe step from `.github/workflows/ci.yml`.
   Anchor (exact current text, immediately before the `- name: Run full
   C test suite` step):

   ```yaml
      # TEMPORARY (scratch branch only) — round-6 diagnostic probe for
      # docs/plans/2026-08-31-macos-numeric-between-round6-match-typed-numeric.md.
      # Remove with the plan close-out.
      - name: PROBE (scratch) — numeric between round 6
        run: ./build/bin/shard-db-test run test-numeric-between-probe6

   ```

   Delete this whole block (the four comment/name/run lines plus the
   trailing blank line before `- name: Run full C test suite`).

   Also remove round 6's one-line addition to `build.sh`'s
   `-DTEST_BUILD` source list. Anchor (exact current text,
   `build.sh` around line 207):

   ```
       src/test/cases/test_binary_index.c \
       src/test/cases/test_numeric_between_probe6.c \
       src/test/cases/test_stats_prom.c \
   ```

   Delete the `src/test/cases/test_numeric_between_probe6.c \` line,
   leaving `test_binary_index.c \` immediately followed by
   `test_stats_prom.c \`.
5. Confirm via `git diff main -- src/db/query_plan.c src/db/slotcask.c
   build.sh .github/workflows/ci.yml` that all four are empty diffs.
   `slotcask.c` should already be empty (round 6's own Task 1 closed out
   round 5's `slotcask.c` change — this just re-verifies it wasn't
   touched again); `query_plan.c`, `build.sh`, and `.github/workflows/ci.yml`
   become empty only after steps 2 and 4 above.
6. `SKIP_TESTS=1 ./build.sh` — confirm clean build.
7. Commit the cleanup — stage only the four files this close-out
   touched, **not** `git add -A` (this workspace has unrelated
   untracked plan files under `docs/plans/` that must not be swept in):
   `git add src/db/query_plan.c src/test/cases/test_numeric_between_probe6.c
   build.sh .github/workflows/ci.yml && git commit -m "chore: close out
   round-6 diagnostic seam and probe"`. Then push
   (`git push origin diag/macos-numeric-between-round6`).
8. Close PR #324 with a short comment pointing at this plan
   (`docs/plans/2026-08-31-macos-numeric-between-round7-fetch-vs-decode.md`)
   as the follow-up. Do not delete the remote branch
   `diag/macos-numeric-between-round6` (evidence stays reachable via the
   closed PR and the branch).

## Task 1 — branch round 7

`main` was never touched by round 6 (see the close-out section above),
so once round 6's own close-out commit lands on
`diag/macos-numeric-between-round6`, `main` is already pristine — no
revert steps are needed here.

1. `git checkout main && git pull`.
2. `git checkout -b diag/macos-numeric-between-round7`.
3. Sanity check: `git diff main -- src/db/query_plan.c src/db/slotcask.c
   build.sh .github/workflows/ci.yml` is empty (it must be, since this
   branch *is* `main`'s tip at this point) — confirms there is nothing
   to revert before adding round 7's own seams below.

## Task 2 — two new seams

### Seam A — fetch point, `src/db/slotcask.c`

Anchor is deliberately narrow — just the last two statements of the
loop body, unique within `slotcask.c` (the `uint32_t vlen =
seg_rec_vlen(rec);` line alone recurs at 7 other sites in the file, so
the anchor must extend through the `fa->cb(...)` call, which is unique,
to pin the substitution to this exact loop and avoid re-declaring
`vlen`). Exact current text, inside `kf_reval_fetch_one`,
`slotcask.c:6338-6341`:

```c
                    uint32_t vlen = seg_rec_vlen(rec);
                    if (fa->cb(r->hash, rec + 24, klen,
                               rec + 24 + klen, vlen, fa->ctx) != 0)
                        break;
```

Replace with (this only wraps the existing `fa->cb(...)` call with the
new diagnostic block — `klen`/`vlen` are declared exactly once, on the
first line, both before and after this change):

```c
                    uint32_t vlen = seg_rec_vlen(rec);
#ifdef TEST_BUILD
                    /* Round-7 diagnostic seam A — fetch point. Round 6
                       proved match_typed decodes a wrong value for one
                       record on macOS with no source-level copy between
                       this call site and that decode (value pointer is
                       passed straight through count_batch_cb /
                       criteria_match_tree unchanged). This dumps the key,
                       klen/vlen, the raw value bytes exactly as read off
                       the mmap'd segment, and the value pointer itself —
                       right before they're handed to the callback, the
                       earliest point in the chain any diagnostic has
                       observed so far. value_ptr is logged so Task 4 can
                       pair this line with its Seam-B counterpart by
                       pointer identity rather than by log order: this
                       function dispatches shard partitions across
                       parallel workers (kf_reval_fetch_worker /
                       parallel_for_io), so NB2TRACE7A/NB2TRACE7B lines
                       from different records can interleave in the log
                       and must not be paired by position.

                       The raw-bytes window below is deliberately NOT
                       bounded by vlen. match_typed's FT_NUMERIC case
                       reads a fixed 8-byte window via ld_be_i64 without
                       ever consulting vlen, so a seam that bounds its
                       own dump by vlen goes blind exactly when vlen
                       itself is the anomaly (observed for key=n_2 in an
                       earlier run of this seam: vlen=0 zeroed the copy
                       length, so vbytes read back as the zero-
                       initialized buffer instead of the actual bytes at
                       that address — worthless for comparing against
                       Seam B). Bounding by fa->db->slot_size instead is
                       always safe regardless of vlen or of which object
                       this fires for: seg_record_emit zero-pads every
                       record to slot_size at write time
                       (slotcask.c:3561-3562), so every byte in
                       [rec, rec+slot_size) is real, initialized memory
                       for this record's slot, never past the mmap'd
                       segment. Temporary — delete with the plan
                       close-out. */
                    {
                        char kbuf[64];
                        size_t kcopy = klen < sizeof(kbuf) - 1 ? klen : sizeof(kbuf) - 1;
                        memcpy(kbuf, rec + 24, kcopy);
                        kbuf[kcopy] = '\0';
                        uint8_t vb[8] = {0};
                        size_t vcopy = 0;
                        if ((size_t)klen + 24 <= (size_t)fa->db->slot_size) {
                            size_t vroom = (size_t)fa->db->slot_size - 24 - klen;
                            vcopy = vroom < 8 ? vroom : 8;
                        }
                        memcpy(vb, rec + 24 + klen, vcopy);
                        LOG_AUDIT(LOG_SUB_SLOTCASK,
                                  "NB2TRACE7A kf_fetch key=%s klen=%u vlen=%u "
                                  "value_ptr=%p "
                                  "vbytes=%02x%02x%02x%02x%02x%02x%02x%02x",
                                  kbuf, (unsigned)klen, (unsigned)vlen,
                                  (const void *)(rec + 24 + klen),
                                  (unsigned)vb[0], (unsigned)vb[1],
                                  (unsigned)vb[2], (unsigned)vb[3],
                                  (unsigned)vb[4], (unsigned)vb[5],
                                  (unsigned)vb[6], (unsigned)vb[7]);
                    }
#endif
                    if (fa->cb(r->hash, rec + 24, klen,
                               rec + 24 + klen, vlen, fa->ctx) != 0)
                        break;
```

Also add `#include "log.h"` at the top of `slotcask.c`? **No** — do not
add it. `slotcask.c` already includes `types.h`, which includes
`log.h` transitively (`types.h:1362`), and `slotcask.c` already calls
`LOG_INFO(LOG_SUB_SLOTCASK, ...)` successfully at line ~2078 via that
same transitive path (confirmed in round 6's Task 1). Adding a
redundant explicit include is not needed.

### Seam B — decode point, `src/db/query_plan.c`

Anchor (exact text after this plan's round-6 close-out section reverts
the round-6 seam, i.e. what's live on the fresh `diag/macos-numeric-between-round7`
branch, `query_plan.c:938-941`):

```c
        case FT_NUMERIC: {
            int64_t v = ld_be_i64(p);
            return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
        }
```

Replace with:

```c
        case FT_NUMERIC: {
            int64_t v = ld_be_i64(p);
#ifdef TEST_BUILD
            /* Round-7 diagnostic seam B — decode point. Paired with seam
               A in slotcask.c's kf_reval_fetch_one: that seam dumps the
               same bytes at the moment they're first read off the mmap'd
               segment, before any callback runs. No source-level copy
               exists between the two seams (value pointer passed through
               count_batch_cb/criteria_match_tree unchanged) — p here is
               numerically identical to seam A's value_ptr for the same
               record, so Task 4 pairs lines by value_ptr, not by log
               order (kf_reval_fetch_one dispatches across parallel
               workers, so lines from different records can interleave).
               If this seam's decoded v disagrees with seam A's vbytes
               for the same value_ptr, that's the direct signature of
               either a concurrent mutation of the mmap page between the
               two reads, or a codegen/UB difference in the read itself.
               Temporary — delete with the plan close-out. */
            LOG_AUDIT(LOG_SUB_QUERY,
                      "NB2TRACE7B match_typed value_ptr=%p v=%lld i1=%lld "
                      "i2=%lld op=%d",
                      (const void *)p, (long long)v, (long long)cc->i1,
                      (long long)cc->i2, (int)cc->op);
#endif
            return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
        }
```

Seam B deliberately does not carry a key — `match_typed` has no key
parameter available at this point. `p` (`const uint8_t *p = rec +
f->offset;`, already in scope above this `switch`) is the exact
pointer `ld_be_i64` reads from, and is numerically identical to seam
A's `rec + 24 + klen` for the same record: `match_typed`'s `rec`
parameter is `criteria_match_tree`'s `rec` parameter unchanged (passed
straight through at `query_plan.c:1726`, `return match_typed(rec, ...)`
with no arithmetic), which is `count_batch_cb`'s `value` parameter
unchanged, which is the pointer `kf_reval_fetch_one` passed as `fa->cb`'s
4th argument — literally `rec + 24 + klen` in seam A's frame (note: seam
A's local `rec` is a *different* variable, local to
`kf_reval_fetch_one`, from seam B's `rec` parameter to `match_typed` —
same name, different bindings; only the *value* `rec + 24 + klen`
threads through unchanged, ending up as `match_typed`'s `rec`
parameter, and since `bi_num` has one field at `offset=0`, `p == rec`
numerically). Task 4's cross-reference therefore pairs
`NB2TRACE7A`/`NB2TRACE7B` lines by matching `value_ptr=` exactly — an
unambiguous, order-independent correlation token that holds even when
seam A's per-record log lines interleave across parallel workers.

## Task 3 — probe test

Create `src/test/cases/test_numeric_between_probe7.c`, adapted from the
now-deleted `test_numeric_between_probe6.c` (same fixture: object
`bi_num`, single field `amt:numeric:10,2`, indexed; 5 records
`n_0..n_4` with values `-999.99, -0.01, 0, 0.01, 999.99`) but issuing
**only** the `between -1..1` query (expect 3) — round 6's probe also
ran `lt 0`/`gte 0` control queries, but `n_1` (value `-0.01`) is a
candidate for both `between` and `lt 0`, and `n_2`/`n_3` are candidates
for both `between` and `gte 0`. Since a record's `value_ptr` is a
function of its segment offset (via the mmap'd segcache handle) and not
of which query touched it, running the control queries too would make
those records' `value_ptr` reappear across multiple query invocations —
turning Task 4's pointer-based pairing many-to-many instead of 1:1
(seam B's `op=` field could disambiguate this, but seam A has no
equivalent field). Dropping the control queries is the simplest fix:
one query, 3 fetched records, 3 unique `value_ptr`s, exactly one Seam-A
and one Seam-B line per pointer.

```c
/* TEMPORARY round-7 fetch-vs-decode diagnostic probe. */
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

static int s2_count_matching(TestEnv *env, const char *tag, const char *substr) {
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
            if (strstr(line, tag) && strstr(line, substr)) {
                TAP_DIAG("  S2 %s: %s", de->d_name, line);
                matches++;
            }
        fclose(f);
    }
    closedir(d);
    return matches;
}

static int test_numeric_between_probe7_run(void) {
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

    ASSERT_EQ_INT(do_count(tc, "bi_num", BETWEEN_CRIT), 3,
        "W1 wire between -1 and 1 = 3 (expected red on macOS)");

    /* Deliberately only one query in this probe (see Task 3's intro):
       running lt/gte controls too would make n_1's and n_2/n_3's
       value_ptr reappear across multiple query invocations, breaking
       Task 4's 1:1 pointer pairing. Seam A fires once per record
       actually fetched off the mmap'd segment for a between-candidate
       that survives KF revalidation — exactly 3 fetches, matching
       idx_count_for_leaf's PRIMARY_LEAF routing for this op. */
    int a_total = s2_count_matching(&env, "NB2TRACE7A", "");
    ASSERT_EQ_INT(a_total, 3,
        "S2 audit log holds exactly 3 NB2TRACE7A kf_fetch lines");
    int a_n2 = s2_count_matching(&env, "NB2TRACE7A", "key=n_2");
    ASSERT_TRUE(a_n2 >= 1, "S2 audit log has at least one NB2TRACE7A line for key=n_2");

    int b_total = s2_count_matching(&env, "NB2TRACE7B", "");
    ASSERT_EQ_INT(b_total, 3,
        "S2 audit log holds exactly 3 NB2TRACE7B match_typed lines");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe7", test_numeric_between_probe7_run)
```

Register it by adding it to `build.sh`'s `-DTEST_BUILD` source list —
this is a required step; `TEST_REGISTER`'s static-init only runs for
translation units actually compiled into the `shard-db-test` binary.
Anchor (exact current text, `build.sh` after the round-6 close-out
section's removal of the round-6 line):

```
    src/test/cases/test_binary_index.c \
    src/test/cases/test_stats_prom.c \
```

Insert the new line between them:

```
    src/test/cases/test_binary_index.c \
    src/test/cases/test_numeric_between_probe7.c \
    src/test/cases/test_stats_prom.c \
```

## Task 4 — CI evidence capture

1. Add a temporary CI probe step to `.github/workflows/ci.yml`, same
   placement and form as round 6's (anchor: immediately after the
   `- name: Build` step, before `- name: Run full C test suite`):

   ```yaml
      # TEMPORARY (scratch branch only) — round-7 diagnostic probe for
      # docs/plans/2026-08-31-macos-numeric-between-round7-fetch-vs-decode.md.
      # Remove with the plan close-out.
      - name: PROBE (scratch) — numeric between round 7
        run: ./build/bin/shard-db-test run test-numeric-between-probe7
   ```

2. Commit the round-7 scratch state — stage exactly these six paths
   (not `git add -A`, for the same untracked-plan-files reason as the
   round-6 close-out): `git add
   docs/plans/2026-08-31-macos-numeric-between-round7-fetch-vs-decode.md
   src/test/cases/test_numeric_between_probe7.c src/db/query_plan.c
   src/db/slotcask.c build.sh .github/workflows/ci.yml && git commit -m
   "test: temporary numeric-between round-7 fetch-vs-decode probe
   (scratch)"`.
3. Push, open a draft PR (title: `test: temporary numeric-between
   round-7 fetch-vs-decode probe (scratch)`, body noting it's
   diagnosis-only per this plan and will be closed after evidence
   capture — mirror round 6's PR #324 body).
4. Wait for CI on all three legs (linux x86_64, linux arm64, macOS
   arm64).
5. Pull the raw `NB2TRACE7A` and `NB2TRACE7B` lines from each leg's job
   log — the probe issues only the `between` query, so this is exactly
   3 lines per seam per leg. Append them verbatim (not paraphrased) to
   this plan file under a new `## Evidence — Task 4` section, one code
   block per platform, labeled. For each platform, pair every
   `NB2TRACE7A ... value_ptr=X ...` line with the `NB2TRACE7B
   value_ptr=X ...` line carrying the **exact same `value_ptr`** — this
   is a clean 1:1 pairing (Task 3's probe deliberately issues only the
   `between` query, so each record's `value_ptr` appears exactly once
   per seam). Do not pair by log-line position and do not pair by
   matching `v`/`vbytes` values — `kf_reval_fetch_one` dispatches shard
   partitions across parallel workers (`kf_reval_fetch_worker` /
   `parallel_for_io`), so lines from different records can interleave in
   the log even within this single-connection test, and if the bug is a
   value mismatch then pairing by value would beg the question.
   `value_ptr` is the one correlation token guaranteed to identify "the
   same record's fetch and decode" regardless of thread interleaving
   (seam A logs `rec + 24 + klen`, seam B logs `p`, and both are proven
   numerically identical for the same record in Task 2's Seam-B
   commentary). Use `key=n_N` (present only on the Seam A side) to label
   which record each paired A/B set belongs to once paired.
6. First check the `vlen=` field on every macOS `NB2TRACE7A` line
   against this standalone branch, independently of which row below
   applies to `vbytes`/`v=` — `vlen` and the decoded value are separate
   axes, and a wrong `vlen` does not by itself confirm or rule out any
   `vbytes`/`v=` row:

   | Observation (macOS) | Interpretation |
   |---|---|
   | Seam A's `vlen=` for `key=n_2` (or any `between`-candidate record) reads `0`, or any value other than the fixed `8` expected for a `numeric:10,2` field's on-disk width | **Novel finding, orthogonal to suspects #1-#4** — the segment record's `vlen` header itself is wrong. This is a distinct failure surface from a wrong *value* at a correct offset: this seam's raw-byte window is deliberately bounded by `fa->db->slot_size`, not by `vlen` (see Task 2 Seam A), specifically so the `vbytes`/`v=` rows below remain evaluable even when `vlen` itself is anomalous. Do not treat a `vlen=0` observation as resolving this round — still apply exactly one of the `vbytes`/`v=` rows below using the same log line, and record the `vlen` anomaly as an *additional* fact alongside whichever row applies. Round 8 must add a seam upstream of this one, at whichever point writes `vlen` into the segment record (`seg_record_emit`'s vlen argument and whatever computes it — `typed_encode_defaults` / `encode_field_len`) and where it is read back (`seg_rec_vlen`, and the raw header bytes at the record's vlen offset it decodes), to establish whether `vlen=0` is what macOS actually persists to disk for this record, or whether it is corrupted somewhere between write and this read. |
   | Seam A's `vlen=` for every `between`-candidate record reads `8`, matching Linux | No `vlen` anomaly to report; proceed directly to the `vbytes`/`v=` rows below. |

   Then apply this decision table for `vbytes`/`v=`:

   | Observation (macOS) | Interpretation |
   |---|---|
   | Seam A's `vbytes` for `key=n_2` already decode as something other than `00 00 00 00 00 00 00 00` (i.e. Seam A itself shows garbage, matching or differing from Seam B's `5999791`) | **Confirms suspect #1 or #2** — the bytes are already wrong (or `r->off` already points at the wrong place) before the callback even fires. This is upstream of everything round 5/6 traced. Round 8 must target either the on-disk write path for this record (re-derive with a raw hex dump of the segment file at `n_2`'s actual offset, taken independently of the daemon, e.g. via a small standalone tool or `xxd`/`od` on the `.dat` file after the daemon writes it and before any query runs) or the KF-resolve step that produces `r->off` (upstream of `kf_reval_fetch_one`, in whatever populates `fa->recs[].off` before this function runs — not yet located in this plan; grep `KfRevalFetchArg` and its producer). |
   | Seam A's `vbytes` for `key=n_2` correctly decode as `00 00 00 00 00 00 00 00` (matching Linux) but the Seam B line sharing that exact `value_ptr` still shows `v=5999791` | **Confirms suspect #3 or #4** — corruption (or a decode divergence) happens strictly between the two seams, despite no source-level copy existing in that path. Round 8 must either (a) rule out concurrency by re-running this exact probe with every background thread (auto-vacuum, auto-reshard, index maintenance) disabled/quiesced via existing test-env knobs, or (b) obtain a disassembly of `match_typed`'s `FT_NUMERIC` case from the macOS release binary (`objdump`/`otool -tv` equivalent) and manually check `ld_be_i64`'s codegen against the same function on Linux arm64 for a genuine compiler divergence. |
   | Seam A never logs a line for `key=n_2` at all (missing from the macOS log, present on Linux) | **Contradiction with round 5** (which showed `seg_live live=1` for all 3 macOS between-candidates under a different probe). Stop and write `PLAN_NOTES.md` — this would mean `seg_rec_live_with_hash` behaves nondeterministically or this round's fixture/query diverged from round 5/6's in some way not accounted for here. |
   | Seam A and Seam B both correctly show `0`-valued bytes/`v=0` for every record on macOS, matching Linux exactly, yet the W1 `between` count is still 2 | **Contradiction** — the loss is not in this fetch→decode chain at all despite round 5/6 both pointing here. Stop and write `PLAN_NOTES.md`; the entire call-path assumption carried since round 4 needs re-verification against the actual macOS CI artifact (stale build cache, wrong binary uploaded, or a second, distinct code path this plan never traced). |

   Quote the exact log lines, not a paraphrase, for whichever rows
   apply (one `vlen` row plus one `vbytes`/`v=` row per platform).

## Evidence — Task 4

PR #329: https://github.com/sayyiditow/shard-db/pull/329, commit
`7769582`. CI matrix results: `Build & test (linux x86_64)` pass,
`Build & test (linux arm64)` pass, `Build & test (macos arm64)` **fail**
(the target — `not ok 2 - W1 wire between -1 and 1 = 3`), plus an
additional `TSan (Linux x86_64)` leg that failed on an unrelated
900s watchdog timeout in `test-rebuild-validation` (no ThreadSanitizer
report; `test-numeric-between-probe7` itself passed 5/5 under TSan) —
not part of this plan's required three legs and not evidence for or
against this bug.

Raw lines below are the audit-log content only, with the CI log
wrapper (job name / `UNKNOWN STEP` / ISO timestamp prefix) stripped;
otherwise verbatim.

### Linux x86_64 (`Build & test (linux x86_64)` — pass)

```
S2 2026-08-31-audit.log: 2026-08-31 23:11:09 INFO [slotcask] NB2TRACE7A kf_fetch key=n_1 klen=3 vlen=8 value_ptr=0x7f9a7000001b vbytes=ffffffffffffffff
S2 2026-08-31-audit.log: 2026-08-31 23:11:09 INFO [slotcask] NB2TRACE7A kf_fetch key=n_3 klen=3 vlen=8 value_ptr=0x7f9a6000001b vbytes=0000000000000001
S2 2026-08-31-audit.log: 2026-08-31 23:11:09 INFO [slotcask] NB2TRACE7A kf_fetch key=n_2 klen=3 vlen=0 value_ptr=0x7f9a70000043 vbytes=0000000000000000
S2 2026-08-31-audit.log: 2026-08-31 23:11:09 INFO [query] NB2TRACE7B match_typed value_ptr=0x7f9a7000001b v=-1 i1=-100 i2=100 op=14
S2 2026-08-31-audit.log: 2026-08-31 23:11:09 INFO [query] NB2TRACE7B match_typed value_ptr=0x7f9a6000001b v=1 i1=-100 i2=100 op=14
S2 2026-08-31-audit.log: 2026-08-31 23:11:09 INFO [query] NB2TRACE7B match_typed value_ptr=0x7f9a70000043 v=0 i1=-100 i2=100 op=14
```

Pairing: `key=n_1` value_ptr=0x7f9a7000001b (A: vlen=8 vbytes=ffff…ff → B: v=-1), `key=n_3` value_ptr=0x7f9a6000001b (A: vlen=8 vbytes=…01 → B: v=1), `key=n_2` value_ptr=0x7f9a70000043 (A: vlen=0 vbytes=all-zero → B: v=0). All three self-consistent; test passed (`W1 wire between -1 and 1 = 3`).

### Linux arm64 (`Build & test (linux arm64)` — pass)

```
S2 2026-08-31-audit.log: 2026-08-31 23:11:01 INFO [slotcask] NB2TRACE7A kf_fetch key=n_1 klen=3 vlen=8 value_ptr=0xff947000001b vbytes=ffffffffffffffff
S2 2026-08-31-audit.log: 2026-08-31 23:11:01 INFO [slotcask] NB2TRACE7A kf_fetch key=n_3 klen=3 vlen=8 value_ptr=0xff946000001b vbytes=0000000000000001
S2 2026-08-31-audit.log: 2026-08-31 23:11:01 INFO [slotcask] NB2TRACE7A kf_fetch key=n_2 klen=3 vlen=0 value_ptr=0xff9470000043 vbytes=0000000000000000
S2 2026-08-31-audit.log: 2026-08-31 23:11:01 INFO [query] NB2TRACE7B match_typed value_ptr=0xff947000001b v=-1 i1=-100 i2=100 op=14
S2 2026-08-31-audit.log: 2026-08-31 23:11:01 INFO [query] NB2TRACE7B match_typed value_ptr=0xff946000001b v=1 i1=-100 i2=100 op=14
S2 2026-08-31-audit.log: 2026-08-31 23:11:01 INFO [query] NB2TRACE7B match_typed value_ptr=0xff9470000043 v=0 i1=-100 i2=100 op=14
```

Pairing: identical structure to Linux x86_64 above — `key=n_2` value_ptr=0xff9470000043 (A: vlen=0 vbytes=all-zero → B: v=0). All three self-consistent; test passed.

### macOS arm64 (`Build & test (macos arm64)` — **fail**, target evidence)

```
S2 2026-08-31-audit.log: 2026-08-31 23:11:52 INFO [slotcask] NB2TRACE7A kf_fetch key=n_1 klen=3 vlen=8 value_ptr=0x13460401b vbytes=ffffffffffffffff
S2 2026-08-31-audit.log: 2026-08-31 23:11:52 INFO [slotcask] NB2TRACE7A kf_fetch key=n_3 klen=3 vlen=8 value_ptr=0x12c604063 vbytes=0000000000000001
S2 2026-08-31-audit.log: 2026-08-31 23:11:52 INFO [slotcask] NB2TRACE7A kf_fetch key=n_2 klen=3 vlen=0 value_ptr=0x12c604043 vbytes=00000000005b8caf
S2 2026-08-31-audit.log: 2026-08-31 23:11:52 INFO [query] NB2TRACE7B match_typed value_ptr=0x13460401b v=-1 i1=-100 i2=100 op=14
S2 2026-08-31-audit.log: 2026-08-31 23:11:52 INFO [query] NB2TRACE7B match_typed value_ptr=0x12c604063 v=1 i1=-100 i2=100 op=14
S2 2026-08-31-audit.log: 2026-08-31 23:11:52 INFO [query] NB2TRACE7B match_typed value_ptr=0x12c604043 v=5999791 i1=-100 i2=100 op=14
```

Pairing: `key=n_1` value_ptr=0x13460401b (A: vlen=8 vbytes=ffff…ff → B: v=-1, matches Linux), `key=n_3` value_ptr=0x12c604063 (A: vlen=8 vbytes=…01 → B: v=1, matches Linux), `key=n_2` value_ptr=0x12c604043 (A: vlen=0 **vbytes=00000000005b8caf** → B: **v=5999791**). `n_2`'s pair is where macOS diverges from both Linux legs: `0x5b8caf` = 5999791 decimal, and Seam B's decoded value is exactly that — the two seams agree with each other, but Seam A's raw bytes already hold garbage before `match_typed` ever runs. `not ok 2 - W1 wire between -1 and 1 = 3` confirms the resulting `between -1..1` count drops to 2 (n_2 excluded because it decodes as 5999791, outside `[-100, 100]`).

### Decision table applied

**`vlen=` tier:** `key=n_2`'s `vlen=0` on macOS matches row 1 ("reads `0`, or any value other than the fixed `8`") — literally, this is flagged. But the same `vlen=0` for the same record appears identically on **both** passing Linux legs, so `vlen=0` for `n_2` is not itself macOS-specific and is not, by itself, sufficient to explain the count drop (Linux gets `v=0`/correct-count with the identical `vlen=0`). Per the table's instruction, this does not resolve the round on its own — recording it as an additional fact per the table's requirement, and proceeding to the `vbytes`/`v=` tier using the same log line. (Round 8, if it pursues the `vlen` instrumentation the table calls for, should account for this cross-platform `vlen=0` observation rather than treating `vlen=0` as macOS-exclusive.)

**`vbytes`/`v=` tier:** macOS's `key=n_2` line matches row 1 exactly: `vbytes=00000000005b8caf` is not `00 00 00 00 00 00 00 00` — Seam A itself shows the garbage value, before the callback fires. **This confirms suspect #1 or #2** — the corruption (or wrong `r->off`) exists upstream of `kf_reval_fetch_one`'s callback dispatch, upstream of everything round 5/6 traced through `match_typed`. Round 6's "match_typed decodes a wrong value" framing undersold where the divergence actually is: `match_typed` (Seam B) is not misdecoding anything — it correctly decodes the 8 bytes it's handed, and those 8 bytes are already wrong at the fetch point. Per the table, round 8 must target either the on-disk write path for `n_2` (raw hex dump of the segment file at its actual offset, independent of the daemon) or the KF-resolve step producing `r->off` upstream of `kf_reval_fetch_one` (`fa->recs[].off`'s producer — not yet located).

## Task 5 — HALT

Do not write or propose a fix. Do not modify `kf_reval_fetch_one`,
`match_typed`, `seg_record_emit`, `seg_rec_klen`/`seg_rec_vlen`,
`typed_encode_defaults`, or any other production code beyond the Task 2
edits. Post the Task 4 evidence and decision-table row as a PR comment.
Leave the draft PR open, unmerged, pending human review and a round-8
(or fix) plan.

## Acceptance criteria

- The round-6 close-out's revert leaves `src/db/query_plan.c`,
  `src/db/slotcask.c`, `build.sh`, and `.github/workflows/ci.yml`
  byte-identical to `main` before round 7's own edits are applied
  (verify via `git diff main -- src/db/query_plan.c src/db/slotcask.c
  build.sh .github/workflows/ci.yml`, expect empty — checked once at
  close-out step 5 and again trivially at Task 1 step 3).
- `SKIP_TESTS=1 ./build.sh` succeeds after every task.
- `test-numeric-between-probe7` passes on Linux x86_64 and Linux arm64
  in CI (both seams present and self-consistent: for every `value_ptr`,
  Seam A's `vbytes` decode to the value Seam B logs at that same
  `value_ptr`).
- CI evidence for all three legs is captured verbatim in this plan's
  `## Evidence — Task 4` section before Task 5's HALT.
- No fix code anywhere in the diff.

## Invariants

- Both seams are `#ifdef TEST_BUILD`-guarded; the non-`TEST_BUILD` path
  through `kf_reval_fetch_one` and `match_typed` is textually identical
  to `main` (verify: `SKIP_TESTS=1 ./build.sh`, which does not define
  `TEST_BUILD` for the production `shard-db` binary, must produce
  byte-identical release-path behavior to `main`).
- Seam A must sit **after** `seg_rec_live_with_hash`'s check and
  **after** `klen`/`vlen` are computed, but **before** the `fa->cb(...)`
  call — this is what makes it "the earliest point any diagnostic in
  this investigation has observed the value bytes."
- No new `#include` needed in either file (`log.h` is transitively
  available in both via `types.h`, confirmed in round 6's Task 1 for
  `slotcask.c` and inherent in `query_plan.c` from round 6 itself).
- Seam A's key buffer (`kbuf[64]`) is sized generously above this
  fixture's longest key (`"n_4"`, 3 bytes) — if `klen` ever exceeds 63
  for some other object/test that happens to route through this
  TEST_BUILD-instrumented function during the full suite (not just this
  probe), the truncation is safe (explicit bound + null terminator) and
  does not crash or overflow; it just produces a truncated `key=` value
  in the log, which is fine since Task 4's evidence only reads this
  probe's own isolated per-test log directory.
- Seam A's raw-bytes window (`vb[8]`, filled via `vcopy`) is bounded by
  `fa->db->slot_size`, not `vlen`, and is safe for every object/test
  that routes through this TEST_BUILD-instrumented function during the
  full suite, not just this probe's `bi_num` fixture: `vcopy` is
  computed only after checking `(size_t)klen + 24 <= (size_t)fa->db->slot_size`
  (guards `size_t` underflow if `klen` is ever corrupted, degrading to
  `vcopy=0` rather than reading out of bounds), then clamped to
  `min(slot_size - 24 - klen, 8)`. Every byte in that window is real,
  initialized memory for the current record's slot regardless of that
  record's actual `vlen`, because `seg_record_emit` zero-pads every
  record to `slot_size` at write time (`slotcask.c:3561-3562`) — so the
  read never reaches past the mmap'd segment for any object, and never
  depends on `vlen` being correct to stay in-bounds.
