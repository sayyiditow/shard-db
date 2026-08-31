# Round 5 — bisect the Kf-boundary revalidation / segment-fetch drop

## Status

**DIAGNOSIS ONLY. No fix code in this plan.** Round 4 localized the
macOS-arm64-only `count` defect (BETWEEN -1..1 on a `numeric` field
returns 2 instead of 3) to `parallel_indexed_count`'s Kf-boundary
revalidation stage: collection is exonerated (`collect count=3` on every
platform including macOS), but `validate in=3 out=2` on macOS only,
`in=3 out=3` on both Linux legs. This plan places seams inside that
stage's two silent-drop points to determine exactly which one loses the
record, and captures per-record before/after values so the drop's actual
mechanism is visible in CI logs — not just its location.

## Where round 4 left off

Evidence (PR #322, commits `be7e7c5`/`c0dc19a`, branch
`diag/macos-numeric-between-round4`, not yet closed out):

- Linux x86_64 / arm64: `NB2TRACE4 collect ... count=3` →
  `NB2TRACE4 validate ... in=3 out=3`.
- macOS arm64: `NB2TRACE4 collect ... count=3` →
  `NB2TRACE4 validate ... in=3 out=2`.

`collect` fires right after `btree_dispatch` returns (query.c, inside
`idx_count_for_leaf`'s `IT_BTREE` block) — the B+ tree range walk and
`collect_hash_cb`'s atomic-add collection are exonerated on every
platform. `validate` fires right after `parallel_indexed_count` returns
— the loss is strictly inside that call.

## Confirmed call path through the revalidation stage (verified 2026-08-31)

For this exact query (`idx_count_for_leaf` passes `fp=NULL` to
`parallel_indexed_count`, so `classify_bm_postfilters` returns
immediately with `n_bm_postfilter=0` — no bitmap post-filters are ever
involved):

```
parallel_indexed_count
  → shard_count_worker (query.c:921), per shard-group, sequentially
      (batch_count ≤ 5 is under the 1024 parallel_for_io threshold)
    - sc->n_bm_postfilter == 0 → the bitmap pre-open block (query.c:931-955)
      is skipped entirely; the worker's own `kh` handle is NEVER acquired
      (stays zeroed from `memset(&kh, 0, sizeof(kh))`, query.c:930).
    - shard_id stays -1 → the `shard_id < 0` branch (query.c:969-975)
      runs: every entry in this shard-group needs record fetch, copied
      into `fetch_hashes`.
    - Pass 2 (query.c:1058-1081): `resolved` is NULL (never set on this
      branch) → the `else` arm calls
      slotcask_bulk_resolve_and_fetch(sdb, fetch_hashes, n_need_fetch,
                                       &cb_ctx, count_batch_cb)
  → slotcask_bulk_resolve_and_fetch (slotcask.c:6437)
      resolved = slotcask_bulk_resolve_hashes(db, hashes, n, &resolved_n)
      → slotcask_bulk_fetch_resolved(db, resolved, resolved_n, ctx, cb)
  → slotcask_bulk_resolve_hashes (slotcask.c:6150)
      Pass 4 (slotcask.c:6206-6235): per non-empty kf shard, ONE
      kfcache_acquire, then for each hash: kf_lookup_no_verify(...). A
      hash that resolves (rc==0) is appended to `resolved[]`
      (sid/fid/off captured); kfcache_release. A hash that fails to
      resolve is silently NOT appended — no trace either way today.
  → slotcask_bulk_fetch_resolved (slotcask.c:6366)
      Partitions `resolved[]` by kf shard again, dispatches one
      kf_reval_fetch_one per partition (nparts ≤ 3 here → runs inline,
      not via parallel_for_io).
  → kf_reval_fetch_one (slotcask.c:6283-6350) — THIS is "the
    Kf-boundary revalidation path" round 4's evidence named. It has TWO
    independent silent-drop points, both upstream of `count_batch_cb`:

    1. kf-level revalidation (slotcask.c:6294-6304): a SECOND, separate
       kfcache_acquire on the same shard, a SECOND kf_lookup_no_verify
       per hash. If the result disagrees with what
       slotcask_bulk_resolve_hashes captured moments earlier
       (flag != 1, or sid/fid/off differ, or the hash isn't found at
       all this time) → `r->sid = 0xFF`, "repointed or gone since
       resolve".
    2. Compaction (slotcask.c:6306-6311) drops every `r->sid == 0xFF`
       record before the survivors are ever touched again — a record
       marked at point 1 NEVER reaches point 3 below.
    3. Segment-level liveness (slotcask.c:6335-6336): for each
       survivor, `seg_rec_live_with_hash(rec, r->hash)` re-checks
       liveness/hash against the actual segment bytes at the resolved
       offset; on false, `continue` — silently skips this record with
       no trace.
    Only a record that passes BOTH checks reaches `fa->cb(...)`
    (`count_batch_cb`, which is what round 4 already showed does not
    lose the between-query's remaining 2 candidates — its
    `criteria_match_tree` re-match is a known-good re-check of a value
    it actually received).

This test has no concurrent writer (single-threaded fixture, all 5 rows
inserted before any query runs, nothing mutates during the count), so
point 1's revalidation SHOULD trivially agree with the resolve pass that
ran a few lines earlier — there is no legitimate reason for the two
calls to disagree here. A disagreement observed only on macOS would
point at the KF probe/mmap path itself (either `kf_lookup_no_verify`
reading a different answer on a second, independently-acquired mmap of
the same file, or the acquire/release cycle failing to see the first
pass's writes — though nothing writes between the two calls in this
test either).

## Ruled out during round-5 prep

Two untracked pre-plan docs already root-cause other, unrelated
`shard_count_worker`/kf concurrency hazards
(`docs/plans/2026-08-27-shard-count-worker-nested-kf-read.md`,
`docs/plans/2026-08-27-bitmap-inline-flush-hazard.md`, both dated
2026-08-27, both explicitly NOT STARTED). Checked against the current
tree because their subject matter overlaps this investigation's
territory:

- **Nested-kf-rdlock doc**: claims `shard_count_worker` holds its own
  outer `kh` through Pass 2, causing a nested rdlock against
  `slotcask_bulk_resolve_and_fetch`'s own acquire on the same shard.
  Re-read against current `query.c:921-1095` (above): the outer `kh` is
  ONLY acquired when `sc->n_bm_postfilter > 0` (query.c:931). This
  query's `fp=NULL` call guarantees `n_bm_postfilter == 0`, so that
  whole block — including the outer acquire — never runs; `kh` stays
  zeroed and `kh.map` is NULL at cleanup's `if (kh.map)` check. **Does
  not apply to this code path.** That doc's root cause also explicitly
  requires "a mutation window queued for that shard's wrlock" — this
  probe has no concurrent writer at all. Real bug, different path, not
  this one.
- **Bitmap-inline-flush doc**: about `idx_find_streaming`'s legacy
  bitmap executor (`bm_shard_walk_worker` / `bm_generic_shard_worker` /
  `stream_find_cb`) — a `find` streaming code path with a bitmap primary
  index, entirely disjoint from `cmd_count_with_tree`'s single-leaf
  BETWEEN count path. **Does not apply.**

Both remain untouched (not this round's scope); noted here only so a
future reader doesn't conflate them with this defect.

## What this plan does

Five TEST_BUILD-only seams, all tagged `NB2TRACE5` so they can be
grepped as one group and, within that, filtered per query by
`op_between=` (Seam A only — the `slotcask.c` seams sit below the point
where `sc->tree` is visible, so they're identified by cross-referencing
hash hex against Seam A's `fetch_in` lines instead). All four
`slotcask.c` seams live inside one consolidated "Seam B" edit (a full
replacement of `kf_reval_fetch_one`, below), because the two acquire
outcomes and the two per-record checks interleave in that function and
editing them as separate anchors would risk overlapping hunks:

- **Seam A** (`query.c`, `shard_count_worker`'s Pass 2 dispatch): logs
  every candidate hash handed to the bulk resolve/fetch call, before
  any resolution happens. Establishes the input set.
- **kf_acquire_fail** (`slotcask.c`, `kf_reval_fetch_one`'s
  `kfcache_acquire` failure branch): logs every hash in a partition that
  gets silently retired (`sid = 0xFF`) because the whole kf shard was
  unreadable this call — a whole-partition failure, distinct from a
  per-record mismatch below.
- **kf_reval** (`slotcask.c`, `kf_reval_fetch_one`'s kf-level
  revalidation loop, once the partition's `kfcache_acquire` succeeded):
  logs every candidate's revalidation outcome — resolve-time
  sid/fid/off vs. this second lookup's rc/flag/sid/fid/off, and whether
  they disagree.
- **seg_acquire_fail** (`slotcask.c`, `kf_reval_fetch_one`'s
  `segcache_acquire` failure branch): logs every hash in a run that gets
  silently skipped because the segment file backing that run was
  unreadable this call — a whole-run failure, distinct from a per-record
  liveness rejection below.
- **seg_live** (`slotcask.c`, `kf_reval_fetch_one`'s segment-level
  liveness check, once that run's `segcache_acquire` succeeded): logs
  every surviving candidate's `seg_rec_live_with_hash` outcome.

By construction, every hash that reaches `kf_reval_fetch_one` gets
exactly one of `kf_acquire_fail` or `kf_reval`; every hash whose
`kf_reval` line has `mismatch=0` (a compaction survivor) later gets
exactly one of `seg_acquire_fail` or `seg_live` (the run-grouping puts
every survivor into some run, and every run resolves its
`segcache_acquire` one way or the other). So for the between query's 3
hashes this is a complete, non-overlapping read-out: a hash present in
`fetch_in` with no `kf_acquire_fail`/`kf_reval` line at all was dropped
upstream, in `slotcask_bulk_resolve_hashes` (outside today's seams,
flagged as a round-6 follow-up below); otherwise its fate is legible
directly from whichever lines it has. If all 3 hashes show clean
survival through every seam and W1 still shows only 2, the drop is
downstream of these seams entirely (in `count_batch_cb`/
`criteria_match_tree` against the actual fetched bytes — also flagged
below).

`slotcask.c` currently does no daemon logging at all (no `log.h`
include). The `#include "log.h"` needed for `LOG_AUDIT`/`LOG_SUB_SLOTCASK`
is added under the file's existing `#ifdef TEST_BUILD` guard block
(`shard_test_ctl.h`'s block) — inert in release builds, nothing new
linked into production.

## Suspect ranking entering round 5

1. **Kf-level revalidation mismatch (kf_reval, mismatch=1)** — a second,
   independently-acquired mmap read of the same kf shard disagreeing
   with the first, with no writer between them. Most consistent with
   round 4's "Kf-boundary revalidation path" framing and the strongest
   match for a platform-specific mmap/visibility difference.
2. **Segment-level liveness rejection (seg_live, live=0)** —
   `seg_rec_live_with_hash` reading the wrong bytes at the resolved
   segment offset on macOS only.
3. **Whole-partition kf read failure (kf_acquire_fail)** —
   `kfcache_acquire` failing for the between query's kf shard on macOS
   only, silently retiring every candidate in that partition (not just
   the one that's actually missing — if this fires, expect the wire
   count to be off by more than 1 unless the partition happens to hold
   only the one candidate). A distinct mechanism from #1: a failure to
   open/map the file at all, not a data disagreement once mapped.
4. **Whole-run segment read failure (seg_acquire_fail)** —
   `segcache_acquire` failing for the segment file backing one run on
   macOS only. Distinct from #2 the same way #3 is distinct from #1.
5. **Upstream of all seams** — `slotcask_bulk_resolve_hashes`'s own
   first `kf_lookup_no_verify` (slotcask.c:6224) never finding the hash
   at all. Not instrumented this round (Seam A already proves 3
   candidates reach the resolve/fetch call; if a between-query hash has
   no `kf_acquire_fail` and no `kf_reval` line at all, this is where the
   round-6 seam goes).
6. **Downstream of all seams** — `criteria_match_tree` genuinely
   rejecting the fetched value for one candidate. Round 3 only checked
   the encoded BETWEEN bound bytes, never a real fetched record's
   decode; if every between-query hash shows clean survival through
   every seam on macOS, this becomes the round-6 target.

## Embedded execution rules

- Branch off `main`: `diag/macos-numeric-between-round5`.
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
  the normal uncommitted-until-reviewed flow, mirroring rounds 1-4.
- This plan is diagnosis-only. Do not modify production behavior in
  `query.c` or `slotcask.c` beyond the `#ifdef TEST_BUILD` seams below.
  No fix, no cleanup beyond what's specified in Task 1.

## Task 1 — close out round 4

1. `gh pr view 322` to confirm current state, then
   `gh pr comment 322 --body "Superseded by round 5 (docs/plans/2026-08-31-macos-numeric-between-round5-kf-fetch-drop.md); evidence already committed above. Closing without merge."`
   followed by `gh pr close 322`.
2. On branch `diag/macos-numeric-between-round4`, revert round 4's seam
   in `src/db/query.c`. Anchor (currently present, verified
   2026-08-31):

```c
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
```

   becomes:

```c
        btree_dispatch(db_root, object, leaf->field, sch->splits,
                       leaf, tf, collect_hash_cb, &col);
        if (col.budget_exceeded) {
```

   and the second half of the same seam:

```c
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
```

   becomes:

```c
        cnt = parallel_indexed_count(db_root, object, sch,
                                     col.entries, (int)col.count,
                                     &leaf_node, (FieldSchema *)fs, dl, NULL, 1);
        collect_ctx_destroy(&col);
```

3. Delete `src/test/cases/test_numeric_between_probe4.c`.
4. In `build.sh`, remove the line
   `    src/test/cases/test_numeric_between_probe4.c \`
   from the `shard-db-test` build command's source list.
5. In `.github/workflows/ci.yml`, remove this step (immediately after
   the `Build` step):

```yaml
      # TEMPORARY (scratch branch only) — round-4 diagnostic probe for
      # docs/plans/2026-08-31-macos-numeric-between-round4-collection-count.md.
      # Remove with the plan close-out.
      - name: PROBE (scratch) — numeric between round 4
        run: ./build/bin/shard-db-test run test-numeric-between-probe4
```

6. `SKIP_TESTS=1 ./build.sh` — confirm it builds clean with the seam
   gone (verifies the revert didn't leave a dangling reference).
7. Commit the revert (`fix: close out round-4 diagnostic seam` or
   similar), push to `diag/macos-numeric-between-round4` so the branch's
   history stays clean before it's retired.
8. Switch away and retire the branch:

```bash
git checkout main
git branch -D diag/macos-numeric-between-round4
git push origin --delete diag/macos-numeric-between-round4
git checkout -b diag/macos-numeric-between-round5 main
git status   # expect clean tree; untracked docs/plans/*.md remain
```

## Task 2 — new seams

### Seam A: `src/db/query.c`, `shard_count_worker`'s Pass 2 dispatch

Anchor (verified 2026-08-31, query.c:1058-1081):

```c
    /* Pass 2: batch fetch all needs-fetch entries */
    if (n_need_fetch > 0) {
        SlotcaskSchemaInfo info = {
            .splits = sc->sch->splits,
            .slot_size = sc->sch->slot_size,
            .streams = sc->sch->streams,
        };
        if (!sdb)
            sdb = slotcask_registry_get(sc->db_root, sc->object, &info);
        if (sdb) {
            CountBatchCbCtx cb_ctx = { sc, &local };
            if (resolved) {
                /* Bitmap path: already have resolved locations, skip KF re-probe */
                slotcask_bulk_fetch_resolved(sdb, resolved,
                                              (size_t)n_need_fetch,
                                              &cb_ctx, count_batch_cb);
            } else {
                /* Non-bitmap path: use combined resolve+fetch */
                slotcask_bulk_resolve_and_fetch(sdb, fetch_hashes,
                                                  (size_t)n_need_fetch,
                                                  &cb_ctx, count_batch_cb);
            }
        }
    }
```

becomes:

```c
    /* Pass 2: batch fetch all needs-fetch entries */
    if (n_need_fetch > 0) {
        SlotcaskSchemaInfo info = {
            .splits = sc->sch->splits,
            .slot_size = sc->sch->slot_size,
            .streams = sc->sch->streams,
        };
        if (!sdb)
            sdb = slotcask_registry_get(sc->db_root, sc->object, &info);
        if (sdb) {
#ifdef TEST_BUILD
            /* Round-5 diagnostic seam — enumerate every candidate handed to
               the resolve/fetch batch call, before any resolution happens.
               Cross-referenced by hash against the kf_reval_fetch_one traces
               in slotcask.c (Seam B), this shows whether a candidate
               present here never reaches count_batch_cb — i.e. is dropped
               inside the Kf-boundary revalidation or segment-liveness check,
               not by criteria_match_tree. Temporary — delete with
               docs/plans/2026-08-31-macos-numeric-between-round5-kf-fetch-drop.md. */
            {
                int nb2_between = sc->tree && sc->tree->kind == CNODE_LEAF &&
                                   sc->tree->leaf.op == OP_BETWEEN;
                for (int nb2_i = 0; nb2_i < n_need_fetch; nb2_i++) {
                    const uint8_t *nb2_h = resolved ? resolved[nb2_i].hash
                                                     : fetch_hashes[nb2_i];
                    char nb2_hex[33] = {0};
                    for (int nb2_j = 0; nb2_j < 16; nb2_j++)
                        snprintf(nb2_hex + nb2_j * 2, 3, "%02x", nb2_h[nb2_j]);
                    LOG_AUDIT(LOG_SUB_QUERY,
                              "NB2TRACE5 fetch_in op_between=%d idx=%d hash=%s",
                              nb2_between, nb2_i, nb2_hex);
                }
            }
#endif
            CountBatchCbCtx cb_ctx = { sc, &local };
            if (resolved) {
                /* Bitmap path: already have resolved locations, skip KF re-probe */
                slotcask_bulk_fetch_resolved(sdb, resolved,
                                              (size_t)n_need_fetch,
                                              &cb_ctx, count_batch_cb);
            } else {
                /* Non-bitmap path: use combined resolve+fetch */
                slotcask_bulk_resolve_and_fetch(sdb, fetch_hashes,
                                                  (size_t)n_need_fetch,
                                                  &cb_ctx, count_batch_cb);
            }
        }
    }
```

### Seam entry: `src/db/slotcask.c` — enable `LOG_AUDIT` in this file

Anchor (verified 2026-08-31, slotcask.c:45-46):

```c
#ifdef TEST_BUILD
#include "shard_test_ctl.h"
```

becomes:

```c
#ifdef TEST_BUILD
#include "shard_test_ctl.h"
#include "log.h"  /* LOG_AUDIT/LOG_SUB_SLOTCASK — round-5 diagnostic seam only */
```

### Seam B: `src/db/slotcask.c`, `kf_reval_fetch_one` (full function)

The two acquire outcomes (kf partition, segment run) and the two
per-record checks (kf revalidation, segment liveness) all live in this
one function and interleave, so this is a single full-function
replacement rather than four separate anchors.

Anchor (verified 2026-08-31, slotcask.c:6283-6350):

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

becomes:

```c
static void kf_reval_fetch_one(KfRevalFetchArg *fa) {
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, fa->db->data_dir, fa->kf_shard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, fa->db->slots_per_shard, 0) != 0) {
        /* Whole partition unreadable — retire every record in it. */
#ifdef TEST_BUILD
        /* Round-5 diagnostic seam — per-hash trace when the entire kf
           partition for this shard is unreadable (kfcache_acquire
           failure), so this silent whole-partition retirement is
           distinguishable from a per-record kf_reval mismatch (below)
           or an upstream drop in slotcask_bulk_resolve_hashes (which
           would show as no kf_acquire_fail AND no kf_reval line at all
           for that hash). Temporary — delete with the plan close-out. */
        for (size_t i = 0; i < fa->count; i++) {
            const uint8_t *nb2_h = fa->recs[fa->start + i].hash;
            char nb2_hex[33] = {0};
            for (int nb2_j = 0; nb2_j < 16; nb2_j++)
                snprintf(nb2_hex + nb2_j * 2, 3, "%02x", nb2_h[nb2_j]);
            LOG_AUDIT(LOG_SUB_SLOTCASK,
                      "NB2TRACE5 kf_acquire_fail hash=%s kf_shard=%d",
                      nb2_hex, fa->kf_shard);
        }
#endif
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
#ifdef TEST_BUILD
        /* Round-5 diagnostic seam — per-record outcome of the kf-boundary
           revalidation probe. A resolve-time (r->sid/fid/off, captured by
           slotcask_bulk_resolve_hashes moments earlier under a SEPARATE
           kfcache_acquire on this same shard) that disagrees with this
           second, still-single-threaded lookup — with no writer able to
           run between the two calls in this test — points at the
           revalidation probe itself rather than a genuine repoint/delete.
           Temporary — delete with the plan close-out. */
        int nb2_rc = kf_lookup_no_verify(&kh, r->hash, &flag, &sid, &fid,
                                          &off, &slot);
        int nb2_mismatch = nb2_rc != 0 || flag != 1 || sid != r->sid ||
                            fid != r->fid || off != r->off;
        {
            char nb2_hex[33] = {0};
            for (int nb2_j = 0; nb2_j < 16; nb2_j++)
                snprintf(nb2_hex + nb2_j * 2, 3, "%02x", r->hash[nb2_j]);
            LOG_AUDIT(LOG_SUB_SLOTCASK,
                      "NB2TRACE5 kf_reval hash=%s rc=%d mismatch=%d "
                      "resolve_sid=%u resolve_fid=%u resolve_off=%u "
                      "reval_flag=%u reval_sid=%u reval_fid=%u reval_off=%u",
                      nb2_hex, nb2_rc, nb2_mismatch,
                      (unsigned)r->sid, (unsigned)r->fid, (unsigned)r->off,
                      (unsigned)flag, (unsigned)sid, (unsigned)fid,
                      (unsigned)off);
        }
        if (nb2_mismatch)
            r->sid = 0xFF;  /* repointed or gone since resolve */
#else
        if (kf_lookup_no_verify(&kh, r->hash, &flag, &sid, &fid, &off,
                                &slot) != 0 ||
            flag != 1 || sid != r->sid || fid != r->fid || off != r->off)
            r->sid = 0xFF;  /* repointed or gone since resolve */
#endif
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
#ifdef TEST_BUILD
                    /* Round-5 diagnostic seam — per-record outcome of the
                       final segment-level liveness+hash check, the last
                       gate before a resolved-and-kf-revalidated candidate
                       reaches count_batch_cb. Temporary — delete with the
                       plan close-out. */
                    {
                        int nb2_live = seg_rec_live_with_hash(rec, r->hash);
                        char nb2_hex[33] = {0};
                        for (int nb2_j = 0; nb2_j < 16; nb2_j++)
                            snprintf(nb2_hex + nb2_j * 2, 3, "%02x",
                                     r->hash[nb2_j]);
                        LOG_AUDIT(LOG_SUB_SLOTCASK,
                                  "NB2TRACE5 seg_live hash=%s live=%d",
                                  nb2_hex, nb2_live);
                        if (!nb2_live) continue;
                    }
#else
                    if (!seg_rec_live_with_hash(rec, r->hash)) continue;
#endif
                    uint16_t klen = seg_rec_klen(rec);
                    uint32_t vlen = seg_rec_vlen(rec);
                    if (fa->cb(r->hash, rec + 24, klen,
                               rec + 24 + klen, vlen, fa->ctx) != 0)
                        break;
                }
                segcache_release(&h);
            }
#ifdef TEST_BUILD
            else {
                /* Round-5 diagnostic seam — per-hash trace when the
                   segment file backing this run is unreadable
                   (segcache_acquire failure), so this silent whole-run
                   skip is distinguishable from a per-record
                   seg_rec_live_with_hash rejection (above). Every hash
                   in [run_start, i] is affected — none of them get a
                   seg_live line for this call. Temporary — delete with
                   the plan close-out. */
                for (size_t j = run_start; j <= i; j++) {
                    const uint8_t *nb2_h = fa->recs[fa->start + j].hash;
                    char nb2_hex[33] = {0};
                    for (int nb2_k = 0; nb2_k < 16; nb2_k++)
                        snprintf(nb2_hex + nb2_k * 2, 3, "%02x",
                                 nb2_h[nb2_k]);
                    LOG_AUDIT(LOG_SUB_SLOTCASK,
                              "NB2TRACE5 seg_acquire_fail hash=%s "
                              "seg_path=%s",
                              nb2_hex, seg_path);
                }
            }
#endif
            run_start = i + 1;
        }
    }

    kfcache_release(&kh);
}
```

After all three edits above (Seam A, the `log.h` include, Seam B):
`SKIP_TESTS=1 ./build.sh` must succeed (release build, `TEST_BUILD`
undefined — every seam compiles out to exactly the original code).

## Task 3 — probe test

Create `src/test/cases/test_numeric_between_probe5.c`:

```c
/* src/test/cases/test_numeric_between_probe5.c
 * TEMPORARY round-5 diagnostic probe — bisects the Kf-boundary
 * revalidation stage round 4 localized the macOS numeric-BETWEEN defect
 * to, per
 * docs/plans/2026-08-31-macos-numeric-between-round5-kf-fetch-drop.md.
 * W  — wire repro (expected red on macOS). Also the source of the
 *      NB2TRACE5 seam lines S2 reads: the fixture daemon runs a real
 *      logging worker, so its LOG_AUDIT output reliably lands in
 *      audit.log.
 * S2 — dumps the daemon's NB2TRACE5 seam lines from the audit log
 *      (produced by the W calls above): fetch_in (candidates entering
 *      the resolve/fetch batch), kf_acquire_fail (whole kf-partition
 *      read failure), kf_reval (kf-boundary revalidation outcome per
 *      candidate), seg_acquire_fail (whole segment-run read failure),
 *      seg_live (segment-level liveness outcome per surviving
 *      candidate). The exact pass/fail per line is a human/CI read-out
 *      (see the plan's Task 4c table), not asserted here — only that
 *      capture works reliably.
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

/* S2 — scan the daemon's log dir for NB2TRACE5 lines matching `substr`
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
            if (strstr(line, "NB2TRACE5") && strstr(line, substr)) {
                TAP_DIAG("  S2 %s: %s", de->d_name, line);
                matches++;
            }
        fclose(f);
    }
    closedir(d);
    return matches;
}

static int test_numeric_between_probe5_run(void) {
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

    /* S2 — fetch_in/kf_reval/seg_live seam lines from the audit log,
       produced by the three W1 wire queries above. The between query's
       fetch_in count is a proven-invariant regression check (round 4
       already showed collection == 3 on every platform, and this
       branch's shard_id<0 path copies every collected candidate 1:1
       into fetch_hashes, so fetch_in must equal 3 everywhere). The
       kf_reval/seg_live totals are NOT asserted to an exact platform-
       independent number here — unlike round 4's collect/validate pair,
       a genuine drop at kf_reval changes how many records even reach
       seg_live, so the total legitimately differs between a passing and
       a failing platform. The actual per-hash mismatch=/live= fields are
       a human/CI read-out (plan Task 4c), not a pass/fail assertion. */
    int fetch_in_between = s2_count_matching(&env, "fetch_in op_between=1");
    ASSERT_EQ_INT(fetch_in_between, 3,
        "S2 audit log holds exactly 3 fetch_in op_between=1 lines");

    int total = s2_count_matching(&env, "");
    ASSERT_TRUE(total >= 18,
        "S2 audit log holds a plausible minimum of NB2TRACE5 lines "
        "(proves capture mechanics work end to end)");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe5", test_numeric_between_probe5_run)
```

Registration — in `build.sh`, add
`    src/test/cases/test_numeric_between_probe5.c \`
to the `shard-db-test` build command's source list, in the same spot
`test_numeric_between_probe4.c` occupied before Task 1 removed it (right
after `test_binary_index.c`).

Local validation (Linux dev machine — expected fully green here; the
defect is macOS-only):

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-numeric-between-probe5
```

Paste the full output. If `test-numeric-between-probe5` fails locally on
Linux, treat that as a plan/seam bug (not the macOS defect) — stop and
write `PLAN_NOTES.md` rather than proceeding to Task 4.

## Task 4 — CI evidence capture

a. In `.github/workflows/ci.yml`, add (mirroring round 4's now-removed
   step, immediately after the `Build` step):

```yaml
      # TEMPORARY (scratch branch only) — round-5 diagnostic probe for
      # docs/plans/2026-08-31-macos-numeric-between-round5-kf-fetch-drop.md.
      # Remove with the plan close-out.
      - name: PROBE (scratch) — numeric between round 5
        run: ./build/bin/shard-db-test run test-numeric-between-probe5
```

b. Commit and push to `diag/macos-numeric-between-round5`. Open a draft
   PR against `main` (title: `test: temporary numeric-between round-5
   kf-fetch-drop probe (scratch)`, body notes it's diagnosis-only, links
   this plan, and will be closed without merge). Confirm CI runs on all
   three legs (linux x86_64, linux arm64, macos arm64) and wait for
   completion.

c. Read-out: pull each leg's `test-numeric-between-probe5` job log,
   grep for `NB2TRACE5`, and record the full set of lines for the
   between query's 3 hashes (cross-reference by the hex `hash=` value
   across `fetch_in`/`kf_acquire_fail`/`kf_reval`/`seg_acquire_fail`/
   `seg_live` lines) in an `## Evidence — Task 4` section appended to
   this plan, per hash. As established in "What this plan does," this
   set of five seams is complete and non-overlapping for any hash that
   reaches `kf_reval_fetch_one` at all, so exactly one row below applies
   per hash (evaluate top to bottom — row 1 is checked first):

   | Observation | Interpretation |
   |---|---|
   | `fetch_in` present, but NO `kf_acquire_fail` and NO `kf_reval` line at all (any platform) | Dropped upstream of these seams, inside `slotcask_bulk_resolve_hashes`'s own first probe (slotcask.c:6224) — round-6 target: instrument that line directly (**suspect #5**). |
   | `kf_acquire_fail` present, on macOS only (not for the same hash's peers, not on either Linux leg) | **Confirms suspect #3** — `kfcache_acquire` failed to open/map the kf shard on macOS. Since this retires the WHOLE partition, check whether the other 2 hashes hash to the same kf shard (same partition) and were also retired — if the wire count dropped by more than 1, or a control query (`lt 0` / `gte 0`) also regressed, that corroborates this row over a coincidental single-hash failure. Root cause is in `kfcache_acquire`'s open/mmap path, not `kf_lookup_no_verify`'s comparison logic. |
   | `kf_reval` present with `mismatch=1`, on macOS only (`mismatch=0` for the same hash's peers and on both Linux legs) | **Confirms suspect #1** — the kf-level revalidation probe itself disagrees with the resolve pass on macOS with no writer present. Root cause is in the KF probe/mmap read path (`kf_lookup_no_verify`, `kfcache_acquire`/`kfcache_release`), not `criteria_match_tree` or record data. Compare the logged `resolve_*` vs `reval_*` fields to see exactly which field disagreed. |
   | `kf_reval mismatch=0`, then `seg_acquire_fail` present for this hash, on macOS only | **Confirms suspect #4** — `segcache_acquire` failed to open/map the segment file backing this hash's run on macOS. Since this affects every hash in the same run, check whether run-mates share this hash's `(sid, fid)` and were also skipped. Root cause is in `segcache_acquire`'s open/mmap path, not `seg_rec_live_with_hash`. |
   | `kf_reval mismatch=0`, then `seg_live` present with `live=0`, on macOS only (`live=1` for the same hash's peers and on both Linux legs) | **Confirms suspect #2** — the segment-level liveness/hash check is rejecting a genuinely live record on macOS. Root cause is in `seg_rec_live_with_hash` or the segment mmap it reads. |
   | All 3 hashes: `kf_reval mismatch=0` and `seg_live live=1`, on macOS too, yet W1's between query still returned 2 | **Confirms suspect #6** — every seam shows clean survival; the drop is downstream, in `count_batch_cb`/`criteria_match_tree` against the actual fetched value. Round-6 target. |

   Quote the exact log lines, not a paraphrase, for whichever row
   applies to each hash.

## Task 5 — HALT

Do not write or propose a fix. Do not modify `slotcask_bulk_resolve_hashes`,
`kf_reval_fetch_one`, `kf_lookup_no_verify`, `seg_rec_live_with_hash`,
`kfcache_acquire`, `segcache_acquire`, or any other production code
beyond the Task 2 edits. Post the Task 4c
table and raw evidence as a PR comment. Leave the draft PR open,
unmerged, pending human review and a round-6 (or fix) plan.

## Acceptance criteria

- Task 1's revert leaves `src/db/query.c`, `build.sh`, and
  `.github/workflows/ci.yml` byte-identical to `main` before round 5's
  own edits are applied (verify via `git diff main -- src/db/query.c
  build.sh .github/workflows/ci.yml` immediately after Task 1 step 6,
  expect empty).
- `SKIP_TESTS=1 ./build.sh` succeeds after every task.
- `test-numeric-between-probe5` passes on Linux x86_64 and Linux arm64
  in CI.
- CI evidence for all three legs is captured verbatim in this plan's
  `## Evidence — Task 4` section before Task 5's HALT.
- No fix code anywhere in the diff.

## Invariants

- Every seam is `#ifdef TEST_BUILD`-guarded; `SKIP_TESTS=1 ./build.sh`
  (which does not define `TEST_BUILD` for the production `shard-db`
  binary) must produce byte-identical release-path behavior to `main`.
- Seam B's two per-record checks (kf_reval, seg_live) use `#ifdef/#else`
  to keep the release-build branch textually identical to the original
  code, not just behaviorally equivalent. Its two acquire-failure traces
  (kf_acquire_fail, seg_acquire_fail) are purely additive inside
  existing branches (no `#else` needed — the non-`TEST_BUILD` code
  already does nothing there but retire/skip).
- No new dependency; `log.h` is already part of this codebase, only
  newly included by `slotcask.c` under the existing `TEST_BUILD` guard.
