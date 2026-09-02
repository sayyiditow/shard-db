# Round 8 — trim-boundary overread hypothesis (write-time vs fetch-time, widened window)

## Status

Diagnosis-only. No fix code authorized in this round. Every task below
runs on a fresh branch off `main`; nothing merges without a human
go-ahead on a later, separate plan once root cause is confirmed with a
specific mechanism.

## Where round 7 left off

Round 7 (PR #329, evidence captured in
`docs/plans/2026-08-31-macos-numeric-between-round7-fetch-vs-decode.md`'s
`## Evidence — Task 4`) proved, via a fetch-point seam (A, in
`kf_reval_fetch_one`) paired by `value_ptr` with a decode-point seam (B,
in `match_typed`'s `FT_NUMERIC` case), that for `key=n_2` (`amt=0`) on
macOS arm64:

- Seam A already reads `vbytes=00000000005b8caf` **before** the record
  is handed to any callback — the wrong bytes exist at the earliest
  point any diagnostic has observed the record in memory.
- Seam B decodes exactly what it's handed (`v=5999791`, matching
  `0x5b8caf`) — `match_typed` is not misdecoding anything.
- On both Linux legs, the same record shows `vbytes=0000000000000000`
  → `v=0` (correct).
- **On all three platforms**, Seam A logs `vlen=0` for `key=n_2` — not
  the `8` bytes a fixed `numeric:10,2` field should occupy. This is
  *not* macOS-specific; round 7's own decision table flagged it as an
  unresolved, cross-platform anomaly and explicitly deferred explaining
  it to round 8.

Round 7 concluded (correctly, as far as it went) that the corruption is
upstream of `kf_reval_fetch_one`'s callback dispatch, and left two open
suspects: on-disk bytes wrong at write time, or `r->off` pointing at the
wrong location. This round's prep work (below) goes further and, via
pure source-level tracing (no code changes), identifies a specific,
falsifiable mechanism that explains *both* the cross-platform `vlen=0`
finding *and* the macOS-only `vbytes` divergence, without invoking
"suspect #2" (wrong offset) at all. Round 8's job is to gather the exact
evidence needed to confirm or refute it.

## Root-cause hypothesis established during round-8 prep

Traced with no code changes, purely from the current source tree:

1. **`vlen=0` is an intentional trim, not corruption.** `cmd_insert_v2`
   (`storage.c:978`) always sets `vlen = ts->total_size` (fixed at 8 for
   `bi_num`'s single `amt:numeric:10,2` field) before calling into
   `slotcask_upsert_with_hooks`. But every object's `SlotcaskDb` wires
   `trim_fn = schema_trim_fn` on first insert (`storage.c:1002-1010`),
   and `schema_trim_fn` → `typed_encode_trim_len` (`config.c:3134-3149`)
   scans backward through the record's fields and returns `0` when
   *every* field is all-zero bytes. `n_2`'s value (`amt: 0`) encodes to
   8 zero bytes — the only one of the 5 probe records that does — so
   it's the only one whose `vlen` gets trimmed to `0` before the record
   is written. This fires identically on every platform (pure content-
   dependent logic, no I/O), which is exactly why round 7 saw `vlen=0`
   for `n_2` on Linux too, with no correctness impact there.
2. **The trimmed write leaves the field's byte range only partially
   zeroed, not fully.** `slotcask_record_size_varlen(klen=3, vlen=0)`
   (`slotcask.c:3527-3529`) computes `raw = 24+3+0 = 27`, rounded up to
   an 8-byte boundary → `rec_size = 32`. `seg_record_emit_pending`
   (`slotcask.c:3577-3594`) is called with that `32` as its own
   `slot_size` parameter. For a plain insert with no CAS/value_compute
   (this probe's shape — `cmd_insert_v2` → `slotcask_upsert_with_hooks`
   with `opts->new_from_old == NULL`), that call site is
   `bulk_phase3_stage_pending`'s per-record loop (`slotcask.c:4550`),
   the P-wave path, **not** `bulk_stage_single_pending` — that second
   function only stages OLD-derived records that skip the P wave
   because their real NEW bytes depend on a `value_compute` callback
   run against OLD (see its own doc comment, `slotcask.c:4557-4560`).
   Either call site's internal `used = 24+klen+vlen = 27`, and its
   zero-pad `memset` only covers `[used, slot_size_param)` = **bytes
   27–31** (5 bytes). It does **not** touch anything past byte 31,
   because as far as this call knows, byte 31 is the end of the
   record's allocated footprint.
3. **`match_typed` reads past that footprint.** `match_typed(const
   uint8_t *rec, ...)` (`query_plan.c:894`) takes no `vlen`/`data_len`
   parameter at all — it cannot know a record was trim-shortened. Its
   `FT_NUMERIC` case unconditionally reads a fixed 8-byte window via
   `ld_be_i64(p)` at the field's static offset (`0` for `bi_num`'s only
   field). For `n_2`, that read spans bytes 27–34 relative to the
   record base — 3 bytes (32, 33, 34) past the record's own 32-byte
   allocated footprint from step 2. Those 3 bytes were never written by
   *this* record's own write at all.
4. **Round 7's own evidence already lines up with this exactly.** Its
   macOS `vbytes=00000000005b8caf` splits as `00 00 00 00 00 | 5b 8c
   af` — indices 0–4 (bytes 27–31, the padding `memset` region from
   step 2) are correctly zero on **every** platform including macOS;
   only indices 5–7 (bytes 32–34, past `rec_size`) diverge, and only on
   macOS. The divergence boundary predicted by pure arithmetic in step
   2/3 matches the observed byte-for-byte split in round 7's own
   evidence precisely — strong support for this mechanism, gathered
   without writing a single new diagnostic line.
5. **What's unresolved:** what is actually sitting in those 3+ bytes.
   The possibilities below are not all independently confirmable — some
   collapse into others depending on which allocation path handed out
   this record's slot, so this must be resolved by evidence
   (`got_pool`, and — for a pool-popped slot — whether a split
   occurred), not assumed:
   - **(H-hole)** The bytes belong to genuinely never-written sparse
     file space. This possibility applies **only when this record's
     slot came from `append_reserve_single_varlen`** — the monotonic
     per-stream bump allocator (`slotcask.c:3500-3523`) that only ever
     hands out strictly-increasing offsets past the stream's prior
     high-water mark, so an append-reserved offset's bytes have never
     been touched by any write, ever, in that segment file's history.
     Every segment file is `ftruncate`d to `slotcask_seg_max_bytes()`
     and `mmap`'d `MAP_SHARED` at that full size on first open
     (`slotcask.c:1479`), well before any specific record's write — so
     there is no incremental-growth/remap step to go stale. If macOS's
     mmap of a sparse hole doesn't give the same read-as-zero guarantee
     as Linux's for the *unwritten remainder of a page a write only
     partially touches* (a real, if obscure, class of platform VM
     behavior — distinct from the well-established "reading an entirely
     untouched hole via `read()`" guarantee, which is not what's
     happening here since a neighboring part of the same record's write
     already dirtied this page), that would explain the divergence
     without needing any other write to be involved at all.
   - **When this record's slot instead came from
     `pool_try_pop_for_size`** (`slotcask.c:4514`, `got_pool=1`), the
     popped free slot's `capacity` determines which of two very
     different situations applies — **`pool_split_leftover` explicitly
     zeroes the excess when a split happens** (confirmed by reading its
     body, `slotcask.c:3477-3491`: `memset(h.map + offset, 0, len)`
     runs before the zeroed range is registered back into the free
     list as its own entry via `pool_push_free_cap` — it does **not**
     leave the excess untouched, contrary to an earlier draft of this
     plan):
     - **(H-split-zeroed)** `fs.capacity > rec_size`
       (`slotcask.c:4523-4524`): `bulk_phase3_stage_pending` calls
       `pool_split_leftover(..., fs.offset + rec_size, fs.capacity -
       rec_size)` — exactly the tail range Seam W's window reads —
       *before* this record's own `seg_record_emit_pending` write runs.
       **But `bulk_phase3_stage_pending` discards `pool_split_leftover`'s
       return value** (`slotcask.c:4523-4526` calls it as a bare
       statement), and even a captured return value would not be enough:
       `pool_split_leftover` returns `-1` both when `segcache_acquire`
       fails *before* the function ever reaches its `memset`
       (`slotcask.c:3482-3483`) — the leftover range never touched at
       all — **and**, via a different non-zero value, when
       `pool_push_free_cap` fails *after* the `memset` already ran
       (`slotcask.c:3489-3490`) — the leftover range already zeroed
       despite the non-zero return. So "a split was attempted"
       (`fs.capacity > rec_size`), "the split's zeroing actually ran,"
       and "the split's overall call succeeded" are three different
       facts, and only the middle one licenses treating non-zero bytes
       here as a contradiction: that range should read back as clean
       zero by the time Seam W captures it **only when the split was
       attempted and its `memset` actually ran** (`split=1 &&
       split_memset_ran==1`, `split_memset_ran` being captured from a
       new thread-local inside `pool_split_leftover` itself, set
       immediately before/after the `memset` independent of the
       function's own return value — see Seam W below). If Seam W's dump
       shows non-zero garbage here under `split=1 &&
       split_memset_ran==1`, that is **not** evidence of stale
       prior-occupant data — it directly contradicts
       `pool_split_leftover`'s own zero-then-register invariant, and
       points to a different, more surprising bug (e.g. the split's
       `segcache_acquire`/write not being visible through the later
       write-path's own mapping, or a genuine ordering/concurrency issue)
       that this plan has not otherwise characterized. Treat this as a
       contradiction requiring `PLAN_NOTES.md`, not as confirmation of
       anything. If instead `split=1 && split_memset_ran==0`, the
       `memset` itself never ran (`segcache_acquire` failed) — the tail
       bytes say nothing about their origin at all, and this is not a
       contradiction of anything (see Task 4's decision table).
       `split_rc` (the raw, ambiguous return value) is still captured
       and logged alongside `split_memset_ran` for completeness, but
       `split_memset_ran` is what actually routes this hypothesis.
     - **(H-exact-fit)** `fs.capacity == rec_size` (no split branch
       taken at all): there is no leftover *within this popped slot* to
       zero or account for — the bytes immediately past `rec_size`
       belong to a completely separate allocation unit (whatever
       happens to occupy the file at that adjacent address: another
       live record, an untouched free-list entry, or unwritten growth
       space beyond the frontier), which neither this record's own
       write nor `pool_split_leftover` ever touches. This case cannot
       be confirmed or refuted from `got_pool`/split status alone — it
       requires the same adjacency proof as H-neighbor below (Task 4
       step 6's layout reconstruction: does some other record's or
       free entry's location actually explain the tail bytes?). Treat
       H-exact-fit as *routing into* H-neighbor's evidence requirement,
       not as an independently confirmable hypothesis of its own.
   - **(H-neighbor)** `append_reserve_single_varlen` is a monotonic
     bump allocator per stream (`slotcask.c:3500-3523`) — if another
     record lands in the *same* stream file immediately after `n_2`
     (`off_next == off_n2 + 32`), the "overread" bytes are actually that
     record's own header (`hash[0..2]`, a deterministic value from
     `xxh128(key)` with no platform dependence). Streams default to
     `slotcask_streams_for_nproc()` (CPU-count-derived, not fixed at 1),
     so whether `n_2` and any other probe record share a stream is not
     knowable without checking the actual `fid` each one lands in — if
     they don't share a stream, H-neighbor doesn't apply to this pair at
     all, and only H-hole/H-exact-fit remain live. If they *do* share a
     stream and the boundary math lines up, H-neighbor predicts
     platform-identical bytes (same hash algorithm) — which would
     contradict the observed macOS/Linux divergence and should be
     flagged, not hand-waved.

Round 8's evidence must distinguish these empirically, not via more
static reasoning — hence the two seams below, widened and enriched with
exactly the fields (`used`, `rec_size`, `fid`, `off`) needed to tell
them apart from the captured log alone.

## Suspect ranking entering round 8

1. **(leading, requires `got_pool=0`) H-hole** — a trim-shortened
   record's unwritten tail reads back as platform-dependent garbage via
   mmap on macOS, zero on Linux. Only applies to a record whose slot
   came from `append_reserve_single_varlen` (fresh, never-written
   space) — see the root-cause section's H-hole entry. Predicts:
   Seam W's own dump for `n_2` — captured immediately after
   `seg_record_emit_pending`'s write into the mmap, before the P-wave's
   later, batched sync pass — *already* shows the garbage tail (before
   any other record could plausibly be involved), on macOS only, **and**
   Seam W's `got_pool=0` for that record.
2. **(requires `got_pool=1` and `split=0`) H-exact-fit** — the
   "overread" bytes belong to whatever separate allocation unit sits
   immediately past a *exactly-sized* reused slot (no split occurred, so
   nothing zeroed or otherwise touched that range on this record's
   behalf) — see the root-cause section's H-exact-fit entry. Not
   independently confirmable: this collapses into H-neighbor's own
   evidence requirement (Task 4 step 6's adjacency cross-reference)
   rather than being confirmable from `got_pool`/`split` alone.
3. **(requires `got_pool=1`, `split=1`, and `split_memset_ran==1`)
   H-split-zeroed contradiction** — if a split was attempted *and*
   `pool_split_leftover`'s own `memset` actually ran (independent of
   whether the call's overall return value indicated success — a failed
   `pool_push_free_cap` *after* a successful `memset` still leaves the
   tail zeroed), it explicitly zeroed exactly this tail range before
   Seam W's own read (see the root-cause section). Non-zero garbage here
   under these conditions is **not** a confirmable suspect at all — it
   contradicts this path's own zero-then-register invariant and must be
   escalated as a distinct, unexplained finding (stop and write
   `PLAN_NOTES.md`; do not fold it into H-hole or any other row).
   `bulk_phase3_stage_pending` discards `pool_split_leftover`'s return
   value on the release path (`slotcask.c:4523-4526`), and that return
   value is ambiguous even when captured (see the root-cause section for
   why), so this suspect is only reachable at all once Seam W
   instruments `pool_split_leftover` itself under `TEST_BUILD` to record
   whether its `memset` ran, independent of its return code
   (`split_memset_ran`) — see Seam W below.
4. **(requires `got_pool=1`, `split=1`, and `split_memset_ran==0`)
   Split-zeroing itself never ran** — `pool_split_leftover` was called,
   but its `segcache_acquire` failed (`slotcask.c:3482-3483`) before the
   function ever reached its `memset`, so the leftover range was never
   touched at all. Non-zero garbage here says nothing about `n_2`'s tail
   bytes' origin — it's simply unzeroed leftover from whatever the
   popped slot last held, and is a separate finding about
   `pool_split_leftover`'s own failure path (worth noting in the
   writeup — e.g. is `segcache_acquire` failing under memory pressure, a
   stale/missing segment file, or something else) rather than evidence
   for or against H-hole, H-exact-fit, or H-neighbor.
5. **H-neighbor** — the "overread" bytes are actually another record's
   header, and something about hash routing/stream assignment
   coincidentally differs by platform, or the byte values are
   misattributed to `n_2` when they actually belong elsewhere. Predicts:
   the neighbor record (from Task 4's cross-reference of all 5 W-seam
   lines) shares `n_2`'s `fid` with `off == off_n2 + rec_size_n2`, and
   its bytes match the tail exactly — and if so, the platform divergence
   would need a *further* explanation this plan does not yet have (stop
   and ask, per the decision table below).
6. **Old suspect #2 (wrong `r->off`)** — not favored by the arithmetic
   match in step 4 above (a wrong offset would need to coincidentally
   reproduce the exact `used`/`rec_size`-aligned split observed), but
   not fully excluded until Seam F's own `rec_size`/`used` fields are
   confirmed to match Seam W's for the same record.
7. **Compiler/codegen divergence** — already weighed against by round
   7's Seam A/B agreement (Seam B decodes exactly what Seam A shows);
   round 8 does not need to re-test this.

## Embedded execution rules

- Order: the round-7 close-out section below runs first, entirely on
  the existing `diag/macos-numeric-between-round7` branch; only after
  it lands does Task 1 branch `diag/macos-numeric-between-round8` off
  `main`. Do not reorder — reverting round 7's seams from a fresh
  round-8 branch would fail immediately, since `main` never had them.
- Do tasks in order after that; do not skip ahead.
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run
  test-numeric-between-probe8` (and `run-all` only where a task says
  so).
- Git writes (commit, push, PR create, PR close) are never bundled into
  a task as an automatic step: per CORE-PROCESS.md, each exact git-write
  operation runs only when the human explicitly directs it in the
  moment — a directive that does not carry over from one operation to
  the next, even a routine follow-up. Every task below that reaches a
  commit/push/PR point stops there, stages and shows the diff, and waits
  — it does not treat this plan's own approval as authorization to run
  those commands. Evidence must still be captured in this plan file and
  posted to the round's own PR, per Task 4.
- If a quoted anchor below isn't found **exactly** in the current tree,
  stop immediately: write `PLAN_NOTES.md` at the repo root describing
  the mismatch (file, expected anchor, what's actually there) and halt
  the entire run — do not guess, reinterpret, or continue to any further
  task, even an unrelated one.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.

## Round-7 close-out (prerequisite — run first, on `diag/macos-numeric-between-round7`)

Round 7's seams and probe scaffolding exist only on the
`diag/macos-numeric-between-round7` branch — `main` never had them.

1. `git checkout` (switching branches) and `git pull` (fetching and
   merging/fast-forwarding) both change local repository state — stop
   before running `git checkout diag/macos-numeric-between-round7 &&
   git pull` and get an explicit go-ahead (or a direct instruction
   naming the exact command) before running it, consistent with this
   plan's treatment of every other state-changing git operation.
2. Revert the round-7 seam in `src/db/slotcask.c` back to the plain
   pre-round-7 form. Anchor (current round-7 state, to be replaced —
   `slotcask.c:6338-6404`):

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

   Replace with:

   ```c
                    uint32_t vlen = seg_rec_vlen(rec);
                    if (fa->cb(r->hash, rec + 24, klen,
                               rec + 24 + klen, vlen, fa->ctx) != 0)
                        break;
   ```

3. Revert the round-7 seam in `src/db/query_plan.c` back to the plain
   pre-round-7 form. Anchor (current round-7 state, to be replaced —
   `query_plan.c:938-963`):

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

   Replace with:

   ```c
       case FT_NUMERIC: {
           int64_t v = ld_be_i64(p);
           return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
       }
   ```

4. Delete `src/test/cases/test_numeric_between_probe7.c`.
5. Remove the round-7 CI probe step from `.github/workflows/ci.yml`.
   Anchor (exact current text, immediately before the `- name: Run full
   C test suite` step):

   ```yaml
      # TEMPORARY (scratch branch only) — round-7 diagnostic probe for
      # docs/plans/2026-08-31-macos-numeric-between-round7-fetch-vs-decode.md.
      # Remove with the plan close-out.
      - name: PROBE (scratch) — numeric between round 7
        run: ./build/bin/shard-db-test run test-numeric-between-probe7

   ```

   Delete this whole block (the four comment/name/run lines plus the
   trailing blank line before `- name: Run full C test suite`).

   Also remove round 7's one-line addition to `build.sh`'s
   `-DTEST_BUILD` source list. Anchor (exact current text, `build.sh`
   around line 207):

   ```
       src/test/cases/test_binary_index.c \
       src/test/cases/test_numeric_between_probe7.c \
       src/test/cases/test_stats_prom.c \
   ```

   Delete the `src/test/cases/test_numeric_between_probe7.c \` line,
   leaving `test_binary_index.c \` immediately followed by
   `test_stats_prom.c \`.
6. Confirm via `git diff main -- src/db/query_plan.c src/db/slotcask.c
   build.sh .github/workflows/ci.yml` that all four are empty diffs.
7. `SKIP_TESTS=1 ./build.sh` — confirm clean build.
8. Stage only the five files this close-out touched — **not**
   `git add -A` (this workspace has unrelated untracked plan files
   under `docs/plans/` that must not be swept in): `git add
   src/db/query_plan.c src/db/slotcask.c
   src/test/cases/test_numeric_between_probe7.c build.sh
   .github/workflows/ci.yml`. Stop here: per CORE-PROCESS.md, commit and
   push are human-directed operations, not something this plan
   pre-authorizes. Show the human `git diff --cached` and wait for an
   explicit go-ahead (or a direct instruction naming the exact command)
   before running `git commit -m "chore: close out round-7 diagnostic
   seams and probe"` and `git push origin
   diag/macos-numeric-between-round7`.
9. Once that commit is pushed, closing PR #329 is likewise a
   human-directed action (PR close carries the same weight as PR
   create/merge under CORE-PROCESS.md's git-safety rules) — ask the
   human to close it, or close it only on explicit direction, with a
   short comment pointing at this plan
   (`docs/plans/2026-09-01-macos-numeric-between-round8-trim-boundary-overread.md`)
   as the follow-up. Do not delete the remote branch
   `diag/macos-numeric-between-round7` (evidence stays reachable via the
   closed PR and the branch).

## Task 1 — branch round 8

`main` was never touched by round 7 (see the close-out section above),
so once round 7's own close-out commit lands on
`diag/macos-numeric-between-round7`, `main` is already pristine — no
revert steps are needed here.

1. `git checkout main && git pull` changes local repository state (same
   as the close-out's step 1 above) — stop and get an explicit
   go-ahead before running it.
2. `git checkout -b diag/macos-numeric-between-round8` also changes
   local state (a new branch and a checkout onto it) — stop and get an
   explicit go-ahead before running it too, even though branch creation
   itself is non-destructive.
3. Sanity check: `git diff main -- src/db/query_plan.c src/db/slotcask.c
   build.sh .github/workflows/ci.yml` is empty (it must be, since this
   branch *is* `main`'s tip at this point) — confirms there is nothing
   to revert before adding round 8's own seams below.

## Task 2 — probe test (test-first: written and run failing before either seam exists)

Create `src/test/cases/test_numeric_between_probe8.c`, adapted from the
now-deleted `test_numeric_between_probe7.c` (same fixture: object
`bi_num`, single field `amt:numeric:10,2`, indexed; 5 records `n_0..n_4`
with values `-999.99, -0.01, 0, 0.01, 999.99`; only the `between -1..1`
query, for the same value_ptr/1:1-pairing reasons round 7 documented —
Seam F still logs `value_ptr` for continuity even though Task 4 pairs by
`(fid, off)`). Seam W (added in Task 3, below) fires once per **insert**
(all 5, since none of these plain inserts set `new_from_old`, so
`value_rewrites_payload` is 0/false and every one is staged by the P wave
— `bulk_phase3_stage_pending`, where Seam W lives — per the trace in this
plan's root-cause section) — independent of the query — so the probe
expects exactly 5 `NB2TRACE8W` lines total, at least 1 for `key=n_2`, and
(as in round 7) exactly 3 `NB2TRACE8F` lines total (one per
`between`-surviving record, once Seam F is added in Task 3), at least 1
for `key=n_2`. This test file references those tags only as strings
grepped out of the runtime log files — it doesn't `#include` or call
anything seam-specific — so it builds and runs correctly with **no**
seams present at all; run against the current, seam-free tree it simply
finds 0 matching lines for both tags, and — after riding out
`s2_wait_for_count`'s full poll timeout below, since 0 matches never
reaches `want` — fails its count assertions, which is exactly the
test-first signal Task 2's steps below are for.

**Audit-log observation is asynchronous — the probe must poll, not scan
once.** `LOG_AUDIT` (`log_audit_sub`, `src/db/config.c`) does not write to
disk synchronously: it pushes onto a fixed-size ring buffer
(`g_log_ring`) under `g_log_lock` and signals `g_log_cond`; a separate
`log_writer_thread` wakes on that signal, batch-drains the ring, and only
then `fflush`+`fclose`s the file it wrote to (audit entries route to
`<date>-audit.log`). There is no synchronous flush API exposed to
`LOG_AUDIT` callers — the only full-drain guarantee (`log_shutdown`) is
internal daemon-lifecycle, not reachable from this external test process.
So a scan issued immediately after the triggering request completes
races the writer thread and can observe a partial or empty file — this
is exactly the failure Task 3's execution hit. The fix is the same
polling-with-timeout pattern already used for the same class of problem
in `wait_for_log_line` (`src/test/cases/test_auto_reshard.c:116`, which
polls `-info.log` for a LOG_WARN tag): add a `s2_wait_for_count` wrapper
around `s2_count_matching` that retries until the observed count reaches
the expected minimum or a timeout elapses, and route every assertion
site through it instead of calling `s2_count_matching` directly. Because
the writer thread is signal-driven (wakes immediately on `g_log_cond`,
not on a fixed timer), the expected steady-state latency is a single
scheduling quantum — a 100ms poll interval with a 5s timeout gives wide
margin without materially slowing the test.

```c
/* TEMPORARY round-8 write-vs-fetch trim-boundary diagnostic probe. */
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
#include <time.h>  /* struct timespec / nanosleep, for s2_wait_for_count */

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

/* Poll s2_count_matching() until it reaches at least `want`, or
   timeout_s elapses -- bridges LOG_AUDIT's asynchronous ring-buffer
   writer (log_writer_thread, src/db/config.c): a scan issued
   immediately after the triggering request completes can race the
   writer thread's drain-and-fflush cycle and see a partial or empty
   file. The writer wakes on a condition variable the instant an entry
   is pushed (not on a fixed timer), so steady-state latency is a
   single scheduling quantum; 100ms poll / 5s timeout gives wide margin.
   Returns the last-observed count (not a bool) so a genuine timeout
   still drives a precise ASSERT_EQ_INT/ASSERT_TRUE failure message
   instead of a bare "false". */
static int s2_wait_for_count(TestEnv *env, const char *tag, const char *substr,
                              int want, int timeout_s) {
    int n = 0;
    for (int i = 0; i < timeout_s * 10; i++) {
        n = s2_count_matching(env, tag, substr);
        if (n >= want) return n;
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return n;
}

static int test_numeric_between_probe8_run(void) {
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

    /* Seam W fires once per insert regardless of any later query — all
       5 records are plain inserts with no new_from_old, so
       value_rewrites_payload is 0/false and all 5 route through the P
       wave (bulk_phase3_stage_pending), where Seam W lives (see this
       plan's root-cause section). */
    int w_total = s2_wait_for_count(&env, "NB2TRACE8W", "", 5, 5);
    ASSERT_EQ_INT(w_total, 5,
        "S2 audit log holds exactly 5 NB2TRACE8W bulk_phase3 lines");
    int w_n2 = s2_wait_for_count(&env, "NB2TRACE8W", "key=n_2", 1, 5);
    ASSERT_TRUE(w_n2 >= 1, "S2 audit log has at least one NB2TRACE8W line for key=n_2");

    /* Seam F fires once per record actually fetched off the mmap'd
       segment for a between-candidate that survives KF revalidation —
       exactly 3, matching round 7's Seam A count for the same query. */
    int f_total = s2_wait_for_count(&env, "NB2TRACE8F", "", 3, 5);
    ASSERT_EQ_INT(f_total, 3,
        "S2 audit log holds exactly 3 NB2TRACE8F kf_fetch lines");
    int f_n2 = s2_wait_for_count(&env, "NB2TRACE8F", "key=n_2", 1, 5);
    ASSERT_TRUE(f_n2 >= 1, "S2 audit log has at least one NB2TRACE8F line for key=n_2");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe8", test_numeric_between_probe8_run)
```

Register it by adding it to `build.sh`'s `-DTEST_BUILD` source list.
Anchor (exact current text, `build.sh` after the round-7 close-out
section's removal of the round-7 line):

```
    src/test/cases/test_binary_index.c \
    src/test/cases/test_stats_prom.c \
```

Insert the new line between them:

```
    src/test/cases/test_binary_index.c \
    src/test/cases/test_numeric_between_probe8.c \
    src/test/cases/test_stats_prom.c \
```

Build and run the probe **before** Task 3 adds either seam:

1. `SKIP_TESTS=1 ./build.sh` — must succeed; the probe file compiles
   cleanly against the current, seam-free tree (it has no dependency on
   either seam's code, only on the `NB2TRACE8W`/`NB2TRACE8F` tag strings
   it greps out of the runtime log files).
2. `./build/bin/shard-db-test run test-numeric-between-probe8` — this
   **must fail** at this point, specifically on the `w_total == 5` and
   `f_total == 3` assertions (actual: 0 for both, since neither
   `NB2TRACE8W` nor `NB2TRACE8F` exists anywhere in the tree yet). Paste
   the actual failing output. Expect this run to take a few seconds
   longer than a plain single-scan failure would — `s2_wait_for_count`
   rides out its full 5s poll timeout on each of the four wait sites
   before giving up and returning the (still-zero) count, since there is
   nothing for the poll to ever find with no seams present. This is
   expected, not a hang. (The `W1 wire between -1 and 1 = 3` assertion is
   a separate, orthogonal check of the underlying wire-level bug itself,
   not of seam presence — its outcome here is not informative for this
   step and may pass or fail independently of platform.)

## Task 3 — two new seams

### Seam W — write point, `src/db/slotcask.c`, `bulk_phase3_stage_pending`

`bulk_stage_single_pending` (round 7's write-seam location in an earlier
draft of this plan) only stages OLD-derived records that skip the P wave
because their real NEW bytes depend on a `value_compute` callback run
against OLD (see its own doc comment, `slotcask.c:4557-4560`). A plain
insert with no CAS/value_compute — this probe's shape —
`cmd_insert_v2` → `slotcask_upsert_with_hooks` sets
`bopts.value_rewrites_payload = (opts->new_from_old != NULL)`, which is
0/false (`slotcask.c:5771`); the P-wave gate in `bulk_stage_one_shard`
only returns early (skipping straight to `bulk_stage_single_pending`'s
M-phase fallback) when `value_rewrites_payload` is set
(`slotcask.c:5427`). So a plain insert's payload is staged in the P wave
— `bulk_phase3_stage_pending` — never in `bulk_stage_single_pending`.
Instrumenting the latter would log zero `NB2TRACE8W` lines for all five
of this probe's inserts.

This seam has three edit points: instrumenting `pool_split_leftover`
itself (`slotcask.c:3477-3491`), a call-site capture in
`bulk_phase3_stage_pending` (just after `fs`/`rec_size` are declared,
where `pool_split_leftover` is actually invoked), and the write-point
dump (after `seg_record_emit_pending`, as in round 7's seams).

The first two exist because `bulk_phase3_stage_pending` discards
`pool_split_leftover`'s return value on the release path, and that
return value alone is not enough: `pool_split_leftover` returns -1 both
when `segcache_acquire` fails *before* its `memset` ever runs
(`slotcask.c:3482-3483`) and, via a different non-zero value, when
`pool_push_free_cap` fails *after* the `memset` already ran
(`slotcask.c:3489-3490`) — the return code alone cannot distinguish
"the tail was never zeroed" from "the tail was zeroed, but the
free-list bookkeeping afterward failed." Only the first case matters for
whether a garbage tail contradicts anything; a failed
`pool_push_free_cap` after a successful `memset` still leaves the tail
correctly zeroed. So `pool_split_leftover` itself is instrumented, under
`TEST_BUILD`, to record — via a thread-local, since bulk staging runs
across the worker pool's own threads — whether its `memset` actually
executed before it returns, independent of its final return value. The
call-site capture then reads that thread-local immediately after each
call, alongside the existing `split_rc` return-value capture (kept for
completeness/debugging, but no longer the routing signal).

First anchor (exact current text, `slotcask.c:3477-3491`):

```c
static int pool_split_leftover(SlotcaskDb *db, uint8_t stream_id,
                                uint16_t file_id, uint32_t offset,
                                uint32_t len) {
    if (len == 0) return 0;
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0, 1) != 0) return -1;
    memset(h.map + offset, 0, len);
    if (h.slot >= 0) {
        SegCacheEntry *e = &g_segcache[h.slot];
        durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
    }
    segcache_release(&h);
    return pool_push_free_cap(&db->streams[stream_id], file_id, offset, len,
                              db->slot_size);
}
```

Replace with:

```c
#ifdef TEST_BUILD
/* Round-8 diagnostic seam W (pool_split_leftover half) — records
   whether this call's memset actually executed before returning,
   independent of the final return value (see this section's intro for
   why the return value alone conflates two different failure points).
   1 = memset ran (segcache_acquire succeeded); 0 = memset never ran
   (segcache_acquire failed, slotcask.c:3482-3483, the line right
   below). Thread-local, not a plain global: bulk staging runs across
   the worker pool's own threads, and each thread's own most recent
   call is all any single caller on that thread cares about — no
   cross-thread aggregation needed. Read immediately after the call
   returns, on the same thread, before any other call to this function
   can run on that thread. Temporary — delete with the plan close-out. */
static __thread int g_nb2trace8_split_memset_ran;
#endif
static int pool_split_leftover(SlotcaskDb *db, uint8_t stream_id,
                                uint16_t file_id, uint32_t offset,
                                uint32_t len) {
    if (len == 0) return 0;
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0, 1) != 0) {
#ifdef TEST_BUILD
        g_nb2trace8_split_memset_ran = 0;
#endif
        return -1;
    }
    memset(h.map + offset, 0, len);
#ifdef TEST_BUILD
    g_nb2trace8_split_memset_ran = 1;
#endif
    if (h.slot >= 0) {
        SegCacheEntry *e = &g_segcache[h.slot];
        durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
    }
    segcache_release(&h);
    return pool_push_free_cap(&db->streams[stream_id], file_id, offset, len,
                              db->slot_size);
}
```

Note: the early `if (len == 0) return 0;` path never sets the
thread-local at all — but this plan's only call site guards every call
with `fs.capacity > (uint32_t)rec_size`, so `len` is always `> 0` at
both of `pool_split_leftover`'s call sites the moment it's actually
invoked; that branch is unreachable from either caller and needs no
separate handling here.

Second anchor (exact current text, `slotcask.c:4514-4527`):

```c
            SlotcaskFreeSlot fs;
            size_t needed = 24 + r->klen + r->vlen;
            size_t rec_size = slotcask_record_size_varlen(r->klen, r->vlen);
            if (pool_try_pop_for_size(pool, (uint32_t)needed,
                                      db->slot_size, &fs) == 0) {
                st[i].target_fid = fs.file_id;
                st[i].target_off = fs.offset;
                st[i].got_pool = 1;
                r->slot_capacity = (uint32_t)rec_size;
                if (fs.capacity > (uint32_t)rec_size)
                    pool_split_leftover(db, (uint8_t)s, fs.file_id,
                                        fs.offset + (uint32_t)rec_size,
                                        fs.capacity - (uint32_t)rec_size);
            } else {
```

Replace with:

```c
            SlotcaskFreeSlot fs;
            size_t needed = 24 + r->klen + r->vlen;
            size_t rec_size = slotcask_record_size_varlen(r->klen, r->vlen);
#ifdef TEST_BUILD
            /* Round-8 diagnostic seam W (call-site half) — captures
               pool_split_leftover's own return value (split_rc, kept
               for completeness/debugging) and, more importantly, its
               memset-ran thread-local (split_memset_ran — see this
               seam's first anchor above for why the return value alone
               cannot distinguish "memset never ran" from "memset ran,
               registration afterward failed"). The release path's
               behavior is unchanged — it already discarded this return
               value as a bare statement; this only adds a capture under
               TEST_BUILD, restructured from a single-statement `if`
               into a braced one to make room for the assignments.
               split_rc defaults to 0 and split_memset_ran defaults to
               -1 ("not applicable") when no split is attempted this
               iteration (fs.capacity == rec_size, or the append-reserve
               branch, which never reaches this code path at all) — -1
               is not a real value g_nb2trace8_split_memset_ran ever
               holds (it's only ever set to 0 or 1 inside
               pool_split_leftover), so it unambiguously means "this
               iteration's local was never overwritten." Temporary —
               delete with the plan close-out. */
            int split_rc = 0;
            int split_memset_ran = -1;
#endif
            if (pool_try_pop_for_size(pool, (uint32_t)needed,
                                      db->slot_size, &fs) == 0) {
                st[i].target_fid = fs.file_id;
                st[i].target_off = fs.offset;
                st[i].got_pool = 1;
                r->slot_capacity = (uint32_t)rec_size;
                if (fs.capacity > (uint32_t)rec_size) {
#ifdef TEST_BUILD
                    split_rc =
#endif
                        pool_split_leftover(db, (uint8_t)s, fs.file_id,
                                            fs.offset + (uint32_t)rec_size,
                                            fs.capacity - (uint32_t)rec_size);
#ifdef TEST_BUILD
                    split_memset_ran = g_nb2trace8_split_memset_ran;
#endif
                }
            } else {
```

Note: this braces a previously single-statement `if` so the
`TEST_BUILD`-guarded assignments have somewhere to attach; the
`pool_split_leftover` call itself is unchanged, so release-path codegen
is unaffected. `split_rc` and `split_memset_ran` are declared at the top
of this loop iteration (same scope as `fs` and `rec_size`) so they
remain in scope, unmodified after this point, through the write-point
half of this seam later in the same iteration. `split_memset_ran` is
read from `g_nb2trace8_split_memset_ran` immediately after the call
returns, on the same thread, before any other call to
`pool_split_leftover` can run on this thread — see this seam's first
anchor for why that ordering is safe without locking.

Third anchor (exact current text, `slotcask.c:4550-4553` after the
above edits are applied):

```c
            seg_record_emit_pending(h.map + st[i].target_off, (int)rec_size,
                                    st[i].hash, r->key, r->klen,
                                    r->value, r->vlen);
            SegCacheEntry *e = &g_segcache[h.slot];
```

Replace with:

```c
            seg_record_emit_pending(h.map + st[i].target_off, (int)rec_size,
                                    st[i].hash, r->key, r->klen,
                                    r->value, r->vlen);
#ifdef TEST_BUILD
            /* Round-8 diagnostic seam W — write point,
               bulk_phase3_stage_pending (the P wave; see this section's
               intro in the plan doc for why this function, not
               bulk_stage_single_pending, is the actual staging path a
               plain insert takes).

               n_2's value (amt=0) encodes as 8 zero bytes, and
               schema_trim_fn (installed as db->trim_fn) trims an
               all-zero-byte record's stored vlen to 0. With vlen=0,
               this record's on-disk footprint (rec_size, computed
               above as slotcask_record_size_varlen(klen,0), 8-byte
               aligned) is smaller than the 8-byte field window
               match_typed's FT_NUMERIC case reads unconditionally —
               match_typed has no vlen/data_len parameter at all and
               cannot know this record was trimmed. For klen=3 that
               footprint is 32 bytes (24 header + 3 key, rounded up to
               8), but match_typed reads 8 bytes starting right after
               the key (byte 27) — ending at byte 35, past this
               record's own 32-byte allocated slot. This seam dumps the
               record exactly as it exists in the mmap immediately
               after seg_record_emit_pending's own memcpy/memset calls
               return — this P-wave loop has no per-record fdatasync
               (durability is a separate, batched "P barrier" pass over
               the whole shard after this loop finishes, per the
               comment at the call site in bulk_stage_one_shard), but
               MAP_SHARED write visibility through this same mapping
               does not depend on msync/fdatasync having run, so this
               is still an accurate snapshot of the bytes any later
               reader of this exact mapping would see — key, klen,
               vlen, `used` (bytes seg_record_emit_pending actually
               considered "real", 24+klen+vlen) alongside `rec_size`
               (the record's full allocated, 8-byte-aligned footprint),
               fid/off (so Task 4 can reconstruct the exact on-disk
               layout across all 5 inserts), and a 16-byte raw window
               starting at the value offset — double round 7's 8, so
               the dump visibly extends past `rec_size` into whatever
               follows, letting Task 4 tell whether that tail is
               still-zero padding, another record's header, or genuine
               garbage, and whether it's already garbage immediately
               after this very write (ruling out any subsequent write
               as the cause) or still clean at this point.

               got_pool distinguishes this record's slot-allocation path
               — 1 if `pool_try_pop_for_size` reused a freed slot
               (slotcask.c:4514), 0 if `append_reserve_single_varlen`
               bump-allocated fresh, never-written space
               (slotcask.c:4519-4527). This is not cosmetic, but
               `got_pool` alone is not sufficient: when it's 1, whether
               a split happened (pool-popped capacity exceeded this
               record's rec_size, slotcask.c:4523-4524) further
               determines what the tail bytes even mean.
               `pool_split_leftover` (slotcask.c:3477-3491) explicitly
               `memset`s the excess to zero before registering it as a
               new free-list entry — it does NOT leave it untouched. So
               `split`/`pool_cap` are logged too: `split=1` (pool_cap >
               rec_size, a split was attempted) vs. `split=0` (pool_cap
               == rec_size, no split, the tail belongs to a wholly
               separate allocation this record's own path never touches
               at all) vs. `got_pool=0` (append-reserved, tail is
               genuinely never-written space). But `split=1` alone does
               not mean the tail *is* already zero here — the call site
               (slotcask.c:4523-4526, this seam's second edit point,
               above) discards `pool_split_leftover`'s own return value
               on the release path, and that return value is ambiguous
               even when captured: `pool_split_leftover` returns -1 both
               when its `segcache_acquire` fails *before* the `memset`
               ever runs (slotcask.c:3482-3483) and, via a different
               non-zero value, when `pool_push_free_cap` fails *after*
               the `memset` already ran (slotcask.c:3489-3490) — a
               failed `pool_push_free_cap` after a successful `memset`
               still leaves the tail correctly zeroed, so the return
               code alone cannot tell "never zeroed" apart from "zeroed,
               but registration afterward failed." So `split_memset_ran`
               is logged instead as the actual routing signal — captured
               from `pool_split_leftover`'s own thread-local
               instrumentation (this seam's first edit point,
               slotcask.c:3477-3491) immediately after the call, it
               directly answers whether the `memset` executed, independent
               of the overall return code: `1` = memset ran (tail should
               already be zero here); `0` = memset never ran
               (`segcache_acquire` failed; tail state says nothing about
               anything); `-1` = not applicable, no split was attempted
               this iteration. `split_rc` (the raw return value) is still
               logged alongside it for completeness/debugging, but is no
               longer what routes the decision table. These cases are not
               interchangeable: Task 4's decision table can only conclude
               H-hole when `got_pool=0`; garbage under
               `got_pool=1,split=1,split_memset_ran=1` contradicts
               pool_split_leftover's own zero-then-register invariant (a
               distinct finding, not H-hole); garbage under
               `got_pool=1,split=1,split_memset_ran=0` says nothing about
               the tail's origin at all (the memset simply never ran —
               not a contradiction, not H-hole); and `got_pool=1,split=0`
               requires the same adjacent-record proof as H-neighbor
               rather than being confirmable on its own. See this plan's
               root-cause section for the full breakdown.

               Bounded by the segment's actual remaining mapped bytes
               (slotcask_seg_max_bytes() minus this record's own
               value-start offset), NOT by db->slot_size. db->slot_size
               is this object's nominal per-record ceiling for a
               FIXED-size record layout and has no relationship to how
               close a variable-length trimmed record's offset sits to
               the end of its segment file: append_reserve_single_varlen
               only checks `reserve_off + rec_size <= seg_max` before
               placing a record (slotcask.c:3509), never `reserve_off +
               rec_size + slot_size`, so a record with a small rec_size
               can legally land with less than slot_size bytes of
               mapped memory left after it — reading a fixed
               slot_size-bounded window in that case would read past
               the end of the mmap (the file is ftruncated + mmap'd to
               exactly slotcask_seg_max_bytes(), slotcask.c:1479,
               nothing further is mapped). Deriving the bound from the
               segment's actual mapped size instead is safe regardless
               of rec_size or how close to the end of the file this
               record landed; `vcopy` is logged so Task 4 can see
               directly whether the window was clipped.
               Temporary — delete with the plan close-out. */
            {
                size_t klen = r->klen, vlen = r->vlen;
                size_t used = 24 + klen + vlen;
                uint32_t pool_cap = 0;
                int split_occurred = 0;
                if (st[i].got_pool) {
                    /* fs still holds the popped free slot's fields from
                       the pool_try_pop_for_size branch above — nothing
                       between there and here reassigns it. Only valid
                       to read when got_pool=1 (the append-reserve
                       branch never populates fs). */
                    pool_cap = fs.capacity;
                    split_occurred = (fs.capacity > (uint32_t)rec_size);
                }
                char kbuf[64];
                size_t kcopy = klen < sizeof(kbuf) - 1 ? klen : sizeof(kbuf) - 1;
                memcpy(kbuf, h.map + st[i].target_off + 24, kcopy);
                kbuf[kcopy] = '\0';
                uint8_t vb[16] = {0};
                size_t vstart = (size_t)st[i].target_off + 24 + klen;
                size_t seg_max = slotcask_seg_max_bytes();
                size_t map_remaining = vstart <= seg_max ? seg_max - vstart : 0;
                size_t vcopy = map_remaining < sizeof(vb) ? map_remaining : sizeof(vb);
                memcpy(vb, h.map + vstart, vcopy);
                LOG_AUDIT(LOG_SUB_SLOTCASK,
                          "NB2TRACE8W bulk_phase3 key=%s klen=%u vlen=%u used=%zu "
                          "rec_size=%zu fid=%u off=%u got_pool=%d pool_cap=%u "
                          "split=%d split_rc=%d split_memset_ran=%d vcopy=%zu "
                          "vbytes=%02x%02x%02x%02x%02x%02x%02x%02x"
                          "%02x%02x%02x%02x%02x%02x%02x%02x",
                          kbuf, (unsigned)klen, (unsigned)vlen, used, rec_size,
                          (unsigned)st[i].target_fid, (unsigned)st[i].target_off,
                          (int)st[i].got_pool, (unsigned)pool_cap,
                          split_occurred, split_rc, split_memset_ran, vcopy,
                          (unsigned)vb[0], (unsigned)vb[1], (unsigned)vb[2],
                          (unsigned)vb[3], (unsigned)vb[4], (unsigned)vb[5],
                          (unsigned)vb[6], (unsigned)vb[7], (unsigned)vb[8],
                          (unsigned)vb[9], (unsigned)vb[10], (unsigned)vb[11],
                          (unsigned)vb[12], (unsigned)vb[13], (unsigned)vb[14],
                          (unsigned)vb[15]);
            }
#endif
            SegCacheEntry *e = &g_segcache[h.slot];
```

Note: `rec_size` is declared earlier in this function's per-record loop
(`slotcask.c:4516`), and `st[i].target_fid`/`st[i].target_off`/
`st[i].got_pool` are all assigned earlier in the same iteration (either
the pool-pop or append-reserve branch sets all three) — no new locals
needed beyond what's inside the guarded block itself. `st[i].got_pool`
is `uint8_t` (`slotcask.c:4309`); logged with a `(int)` cast for `%d`.
`fs` (`SlotcaskFreeSlot`, declared `slotcask.c:4514`) is still in scope
and, when `got_pool=1`, still holds the values `pool_try_pop_for_size`
wrote into it — nothing reassigns `fs` between that call and this seam.
Reading `fs.capacity` is only valid when `got_pool=1` (the
append-reserve branch never populates `fs`), which is why `pool_cap` and
`split_occurred` are computed inside the `if (st[i].got_pool)` guard,
not unconditionally. `split_rc` and `split_memset_ran` are declared once per loop iteration
at this seam's call-site half (above, right after `fs`/`rec_size`), so
they are already in scope and unmodified by the time this write-point
half runs later in the same iteration — both are logged unconditionally
(not inside the `got_pool` guard) since `split_rc` defaults to `0` and
`split_memset_ran` defaults to `-1` for every path that never attempts a
split (`got_pool=0`, or `got_pool=1,split=0`), and only the call site
itself (guarded by `fs.capacity > rec_size`) ever assigns them different
values — `split_memset_ran`'s `-1` default is how a reader tells "no
split attempted" apart from `pool_split_leftover`'s own real outputs of
`0`/`1`. `slotcask_seg_max_bytes()` needs no new
include: it's declared in `slotcask.h` (already included) and defined
earlier in this same translation unit (`slotcask.c:1356`).

### Seam F — fetch point, `src/db/slotcask.c`, `kf_reval_fetch_one`

Anchor (exact text after this plan's round-7 close-out section reverts
the round-7 seam, i.e. what's live on the fresh
`diag/macos-numeric-between-round8` branch, `slotcask.c:6338-6341`):

```c
                    uint32_t vlen = seg_rec_vlen(rec);
                    if (fa->cb(r->hash, rec + 24, klen,
                               rec + 24 + klen, vlen, fa->ctx) != 0)
                        break;
```

Replace with:

```c
                    uint32_t vlen = seg_rec_vlen(rec);
#ifdef TEST_BUILD
                    /* Round-8 diagnostic seam F — fetch point (round 8's
                       version of round 7's seam A, re-added after round
                       7's close-out reverted it; see this round's plan
                       doc for the trim-boundary-overread hypothesis this
                       seam tests, and Seam W above in
                       bulk_phase3_stage_pending for its write-time
                       counterpart). Paired with Seam W by (fid, off) —
                       the on-disk record location — rather than by
                       virtual pointer: this fetch opens its own segcache
                       handle independently of the write-time handle, so
                       the mmap'd virtual address is not guaranteed to be
                       the same across the two seams even for the same
                       on-disk bytes, whereas (fid, off) is stable.
                       kf_reval_fetch_one dispatches shard partitions
                       across parallel workers, so lines from different
                       records can interleave in the log and must not be
                       paired by log position.

                       Window widened to 16 bytes (round 7's seam A used
                       8) and rec_size/used are now logged alongside
                       vlen, matching Seam W's fields exactly, so a
                       divergence between what Seam W wrote and what this
                       seam reads later is visible byte-for-byte, not
                       just in the first 8 bytes match_typed actually
                       decodes. Bounded by the segment's actual remaining
                       mapped bytes (slotcask_seg_max_bytes() minus this
                       record's own value-start offset), NOT by
                       fa->db->slot_size — see Seam W's comment for why
                       slot_size has no relationship to how close a
                       variable-length trimmed record sits to the end of
                       its segment file; a fixed slot_size-bounded window
                       could read past the actual end of the mmap for a
                       record staged near a segment's tail. `vcopy` is
                       logged so Task 4 can see whether the window was
                       clipped. Temporary — delete with the plan
                       close-out. */
                    {
                        size_t used = 24 + (size_t)klen + (size_t)vlen;
                        size_t computed_rec_size =
                            slotcask_record_size_varlen(klen, vlen);
                        char kbuf[64];
                        size_t kcopy = klen < sizeof(kbuf) - 1 ? klen : sizeof(kbuf) - 1;
                        memcpy(kbuf, rec + 24, kcopy);
                        kbuf[kcopy] = '\0';
                        uint8_t vb[16] = {0};
                        size_t vstart = (size_t)r->off + 24 + (size_t)klen;
                        size_t seg_max = slotcask_seg_max_bytes();
                        size_t map_remaining = vstart <= seg_max ? seg_max - vstart : 0;
                        size_t vcopy = map_remaining < sizeof(vb) ? map_remaining : sizeof(vb);
                        memcpy(vb, h.map + vstart, vcopy);
                        LOG_AUDIT(LOG_SUB_SLOTCASK,
                                  "NB2TRACE8F kf_fetch key=%s klen=%u vlen=%u "
                                  "used=%zu rec_size=%zu fid=%u off=%u vcopy=%zu "
                                  "value_ptr=%p "
                                  "vbytes=%02x%02x%02x%02x%02x%02x%02x%02x"
                                  "%02x%02x%02x%02x%02x%02x%02x%02x",
                                  kbuf, (unsigned)klen, (unsigned)vlen,
                                  used, computed_rec_size,
                                  (unsigned)r->fid, (unsigned)r->off, vcopy,
                                  (const void *)(rec + 24 + klen),
                                  (unsigned)vb[0], (unsigned)vb[1],
                                  (unsigned)vb[2], (unsigned)vb[3],
                                  (unsigned)vb[4], (unsigned)vb[5],
                                  (unsigned)vb[6], (unsigned)vb[7],
                                  (unsigned)vb[8], (unsigned)vb[9],
                                  (unsigned)vb[10], (unsigned)vb[11],
                                  (unsigned)vb[12], (unsigned)vb[13],
                                  (unsigned)vb[14], (unsigned)vb[15]);
                    }
#endif
                    if (fa->cb(r->hash, rec + 24, klen,
                               rec + 24 + klen, vlen, fa->ctx) != 0)
                        break;
```

`slotcask_record_size_varlen` is a `static inline` function already
defined earlier in this same translation unit (`slotcask.c:3527`) — no
new declaration or include needed. `r->fid` and `r->off` are fields of
`SlotcaskResolvedRec` (`slotcask.h:356-361`), already in scope via `r`.

With both seams in place, rebuild and rerun the same probe from Task 2:

1. `SKIP_TESTS=1 ./build.sh` — rebuild with both seams compiled in.
2. `./build/bin/shard-db-test run test-numeric-between-probe8` — the
   `w_total == 5`, `f_total == 3`, `w_n2 >= 1`, and `f_n2 >= 1`
   assertions must now **pass**, proving both seams fire on the exact
   paths the probe exercises — not merely assumed from source reading.
   Paste the actual passing output. (The `W1` wire-level count assertion
   remains orthogonal — expected red on macOS, green on Linux — and is
   not evidence of seam correctness either way; Task 4 is where its
   platform split gets used.)

## Task 4 — CI evidence capture

1. Add a temporary CI probe step to `.github/workflows/ci.yml`, same
   placement and form as round 7's (anchor: immediately after the
   `- name: Build` step, before `- name: Run full C test suite`):

   ```yaml
      # TEMPORARY (scratch branch only) — round-8 diagnostic probe for
      # docs/plans/2026-09-01-macos-numeric-between-round8-trim-boundary-overread.md.
      # Remove with the plan close-out.
      - name: PROBE (scratch) — numeric between round 8
        run: ./build/bin/shard-db-test run test-numeric-between-probe8
   ```

2. Stage exactly these six paths — not `git add -A`, for the same
   untracked-plan-files reason as the round-7 close-out: `git add
   docs/plans/2026-09-01-macos-numeric-between-round8-trim-boundary-overread.md
   src/test/cases/test_numeric_between_probe8.c src/db/query_plan.c
   src/db/slotcask.c build.sh .github/workflows/ci.yml`. (`query_plan.c`
   should have an empty diff vs `main` at this point — round 8 only
   touches `slotcask.c` — but is included in the `git add` list
   defensively in case a future edit to this plan changes that; confirm
   it's actually empty before staging, and if so it's fine that
   `git add` is a no-op for it.) Stop here: per CORE-PROCESS.md, commit
   is a human-directed operation, not pre-authorized by this plan — show
   the human `git diff --cached` and wait for an explicit go-ahead (or a
   direct instruction naming the exact command) before running
   `git commit -m "test: temporary numeric-between round-8
   trim-boundary-overread probe (scratch)"`.
3. Pushing and opening the draft PR are likewise human-directed steps
   (CORE-PROCESS.md lists both push and PR create explicitly) — push
   only on the human's explicit direction, then open a draft PR (title:
   `test: temporary numeric-between round-8 trim-boundary-overread probe
   (scratch)`, body noting it's diagnosis-only per this plan and will be
   closed after evidence capture — mirror round 7's PR #329 body) only
   once the human has directed that too.
4. Wait for CI on all three legs (linux x86_64, linux arm64, macOS
   arm64).
5. Pull the raw `NB2TRACE8W` and `NB2TRACE8F` lines from each leg's job
   log. Append them verbatim (not paraphrased) to this plan file under a
   new `## Evidence — Task 4` section, one code block per platform,
   labeled.
6. For each platform, reconstruct the on-disk layout from the 5
   `NB2TRACE8W` lines: for every pair of records sharing the same `fid`,
   check whether one's `off` equals another's `off + rec_size` (adjacent
   in the same stream file). Note explicitly, per platform, whether
   `n_2` has an adjacent successor in its own stream and if so which key
   it is. Also note explicitly, per platform, `n_2`'s own `got_pool`,
   `pool_cap`, `split`, `split_rc`, and `split_memset_ran` values from
   its `NB2TRACE8W` line — these select which decision-table row below
   is even eligible to apply (H-hole requires `got_pool=0`; the
   H-split-zeroed contradiction requires
   `got_pool=1,split=1,split_memset_ran==1`; the split-zeroing-never-ran
   row requires `got_pool=1,split=1,split_memset_ran==0`; H-exact-fit
   requires `got_pool=1,split=0` — where `split_rc` and
   `split_memset_ran` are moot, at their defaults — and is only
   resolvable via this same step's adjacency reconstruction; it has no
   independent decision-table row of its own). `split_memset_ran`, not
   `split_rc`, is the field that actually routes this decision —
   `split_rc` alone cannot distinguish "the memset never ran" from "the
   memset ran but the free-list registration afterward failed" (see the
   root-cause section); record it anyway for completeness, but do not
   route on it. Do not skip straight to interpreting the `vbytes` split
   without first recording these.
7. Pair `NB2TRACE8W` and `NB2TRACE8F` lines for `key=n_2` by matching
   `fid` **and** `off` exactly (not by `value_ptr`, not by log position
   — see Seam F's comment for why). Apply this decision table:

   | Observation (macOS) | Interpretation |
   |---|---|
   | Seam W's `used`/`rec_size` for `key=n_2` are anything other than `used=27, rec_size=32` | **Contradiction with this plan's arithmetic** — a wrong assumption entered this plan (e.g. a different `klen`, or `typed_encode_trim_len` not firing as traced). Stop and write `PLAN_NOTES.md`; do not proceed to the rows below on a false premise. |
   | Seam W's own `vbytes[0:5)` (indices 0–4, bytes 27–31) are not all-zero | **Contradiction** — `seg_record_emit_pending`'s own zero-pad (`used` to `rec_size`) failed to run or was itself wrong. This would be a distinct, more fundamental bug than the one this plan targets. Stop and write `PLAN_NOTES.md`. |
   | Seam W's `vbytes[5:8)` (indices 5–7, bytes 32–34 — past `rec_size`) are **already non-zero garbage at write time**, immediately after this record's own write into the mmap (this path's sync is a later, batched P-barrier pass — see Seam W's comment), before any other record could plausibly have been written to this location, **and Seam W's `got_pool=0` for this record** | **Confirms H-hole (suspect #1)** — this record's slot came from fresh, never-written space (`append_reserve_single_varlen`), so its unwritten tail already reads back as garbage via mmap on macOS at the moment of its own write, ruling out any subsequent write/clobber *and* ruling out prior-occupant leftover (there is no prior occupant of never-touched space). Root cause: `match_typed`'s fixed-width field decode has no length awareness and reads past a trim-shortened record's true allocated footprint into never-written (and, on macOS, non-zero-reading) memory. A fix would need to bound `match_typed`'s reads by the record's actual `vlen`/`data_len` (treating anything past it as the field's zero default, the same way `typed_get_field_str`'s `zero_field` fallback already does at `config.c:3172-3176`) — not attempt to make macOS's sparse-hole read behave like Linux's. |
   | Seam W's `vbytes[5:8)` are already non-zero garbage at write time, **`got_pool=1`, `split=1`, and `split_memset_ran==1`** (a leftover was split off, and `pool_split_leftover`'s own `memset` actually ran before it returned — regardless of that call's overall return code) | **Contradicts H-split-zeroed — a distinct, unexplained finding, not H-hole and not confirmation of anything.** `pool_split_leftover` (`slotcask.c:3477-3491`) `memset`s exactly this tail range to zero, then registers it as a free-list entry, *before* this record's own `seg_record_emit_pending` write runs later in the same loop iteration — so by write time the range should already read back as clean zero, and the thread-local `split_memset_ran` captured directly inside `pool_split_leftover` confirms the `memset` itself executed (the ambiguous `split_rc` return code is not what confirms this — a failed `pool_push_free_cap` *after* a successful `memset` would leave `split_rc!=0` even though the tail is still correctly zeroed, which is exactly why `split_memset_ran` exists). Non-zero garbage here directly contradicts that function's own zero-then-register invariant. Do not classify this as H-hole or H-exact-fit. Stop and write `PLAN_NOTES.md` describing the contradiction (include `pool_cap`, `rec_size`, `split_rc`, `split_memset_ran`, and the exact `vbytes`) rather than proceeding past this row on an assumed root cause. |
   | Seam W's `vbytes[5:8)` are already non-zero garbage at write time, **`got_pool=1`, `split=1`, but `split_memset_ran==0`** (a leftover was split off, but `pool_split_leftover`'s `segcache_acquire` failed — `slotcask.c:3482-3483` — before its `memset` ever ran) | **Not a contradiction of anything — the leftover range was simply never zeroed.** `split_memset_ran==0` means `pool_split_leftover` bailed out before its `memset` ran at all, so garbage in this range says nothing about the tail bytes' origin one way or the other; it is a separate, worth-reporting finding about `pool_split_leftover`'s own `segcache_acquire` failure path (note in the writeup what `split_rc` shows too, and whether `segcache_acquire` is failing under memory pressure, a stale/missing segment file, or something else). Do not fold this into H-hole, H-exact-fit, or the H-split-zeroed contradiction row above. |
   | Seam W's `vbytes[5:8)` are already non-zero garbage at write time, **`got_pool=1` but `split=0`** (the popped slot's capacity exactly matched `rec_size` — no leftover, no `pool_split_leftover` call at all, so `split_rc` and `split_memset_ran` are moot and stay at their defaults) | **H-exact-fit — not confirmable from this row alone; cross-reference step 6's adjacency reconstruction (same requirement as the H-neighbor row below).** These tail bytes belong to a wholly separate allocation unit this record's own write never touches. Apply the H-neighbor row's adjacency test: if a successor record's (or a free-list entry's) location explains the byte range, that is the finding; if nothing explains it, this is an unresolved gap this plan cannot close and must be flagged as such in the writeup, not silently folded into H-hole. |
   | Seam W's `vbytes[5:8)` are correctly zero at write time, but the paired Seam F line for the same `(fid, off)` later shows different, non-zero bytes there | **Confirms H-neighbor or a genuine post-write clobber (and is also where an H-exact-fit row above routes to for its adjacency proof)** — cross-reference step 6's layout reconstruction: if a successor record's `off` lands exactly at `off_n2 + 32`, its header bytes are the explanation and the divergence is not corruption at all (just this seam's own window overlapping a different record) — but then also check whether that successor's first-3-bytes match the garbage exactly; if they don't, or if no record's `off` lands there, this is a genuine unexplained clobber between write and fetch and round 9 must instrument what else touches this exact byte range in between. |
   | Seam F's `used`/`rec_size` for `key=n_2` disagree with Seam W's for the same `(fid, off)` | **Contradiction** — `klen`/`vlen` themselves are being read back differently between the two seams despite addressing the identical on-disk location. Stop and write `PLAN_NOTES.md`; this would undermine the `(fid, off)` pairing assumption itself. |
   | All of the above line up with **H-hole** (`got_pool=0`) **or** a resolved **H-exact-fit → H-neighbor** adjacency proof (`got_pool=1,split=0`, or `got_pool=1,split=1,split_memset_ran==0` with the adjacency test separately applied to the never-zeroed range) on macOS, and Seam W/F agree byte-for-byte on both Linux legs (all-zero tail, matching row "no divergence") | Root cause confirmed, with the specific mechanism named by which row applied: **`match_typed`'s FT_NUMERIC (and, by the same missing-length-check reasoning, presumably every other fixed-width `FT_*` case) reads past a trim-shortened record's actual stored length, and the resulting out-of-record read is platform-dependent because the underlying memory is either never written (H-hole, `got_pool=0`) or belongs to a separate, adjacency-proven allocation unit that mmap exposes differently across platforms (H-exact-fit resolved into H-neighbor) by any code path this program controls.** State explicitly in the writeup which row applied for `n_2` in this run — they are not interchangeable for a fix plan. If instead a `got_pool=1,split=1,split_memset_ran==1` contradiction row fired on any platform, this rollup does not apply — that outcome requires its own `PLAN_NOTES.md` escalation and blocks proceeding to Task 5 until resolved. No further diagnostic round needed before a fix plan when this rollup's condition is met cleanly; proceed to Task 5. |

   Quote the exact log lines, not a paraphrase, for whichever rows
   apply, on all three platforms (even the two that pass — the same
   `used=27/rec_size=32` arithmetic and zero-padding check apply there
   too, and should confirm cleanly, which is itself part of the
   evidence).
8. The `## Evidence — Task 4` section added in steps 5–7 exists only in
   the local working tree at this point — step 2's commit was made
   *before* CI ran, so it cannot contain this evidence, and nothing so
   far pushes the update back to the branch or PR. Stage just the plan
   file: `git add
   docs/plans/2026-09-01-macos-numeric-between-round8-trim-boundary-overread.md`.
   Stop here: per CORE-PROCESS.md, commit and push are human-directed
   operations — show the human `git diff --cached` and wait for an
   explicit go-ahead (or a direct instruction naming the exact command)
   before running `git commit -m "docs: capture round-8 CI evidence and
   decision-table conclusion"` and `git push origin
   diag/macos-numeric-between-round8`. Do this before Task 5's PR
   comment, so the evidence is reachable both as a PR comment (Task 5)
   and as a committed, pushed file on the branch the PR points at — not
   local-only.

## Task 5 — HALT

Do not write or propose a fix. Do not modify `kf_reval_fetch_one`,
`match_typed`, `bulk_phase3_stage_pending`, `seg_record_emit_pending`,
`typed_encode_trim_len`, `schema_trim_fn`, or any other production code
beyond the Task 3 edits. Post the Task 4 evidence and decision-table row
as a PR comment. Leave the draft PR open, unmerged, pending human review
and a round-9 (or fix) plan.

## Acceptance criteria

- The round-7 close-out's revert leaves `src/db/query_plan.c`,
  `src/db/slotcask.c`, `build.sh`, and `.github/workflows/ci.yml`
  byte-identical to `main` before round 8's own edits are applied
  (verify via `git diff main -- src/db/query_plan.c src/db/slotcask.c
  build.sh .github/workflows/ci.yml`, expect empty — checked once at
  close-out step 6 and again trivially at Task 1 step 3).
- `SKIP_TESTS=1 ./build.sh` succeeds after every task.
- `test-numeric-between-probe8` passes on Linux x86_64 and Linux arm64
  in CI (both seams present and self-consistent: for every `(fid, off)`
  pair, Seam W's persisted bytes match Seam F's later-read bytes
  exactly).
- CI evidence for all three legs is captured verbatim in this plan's
  `## Evidence — Task 4` section before Task 5's HALT.
- No fix code anywhere in the diff.

## Invariants

- Both seams are `#ifdef TEST_BUILD`-guarded; the non-`TEST_BUILD` path
  through `bulk_phase3_stage_pending` and `kf_reval_fetch_one` is
  textually identical to `main` (verify: `SKIP_TESTS=1 ./build.sh`,
  which does not define `TEST_BUILD` for the production `shard-db`
  binary, must produce byte-identical release-path behavior to `main`).
- Seam W must sit **immediately after** `seg_record_emit_pending`'s call
  inside `bulk_phase3_stage_pending`'s per-record loop, **before**
  `SegCacheEntry *e = &g_segcache[h.slot]`/`segcache_release` for that
  iteration — the earliest point after the record's bytes are written
  into the mmap. This path has no per-record `fdatasync` to gate on
  (durability here is a separate, later, batched P-barrier pass over the
  whole shard); `h.map` is still valid at this point regardless.
- Seam F must sit **after** `seg_rec_live_with_hash`'s check and
  **after** `klen`/`vlen` are computed, but **before** the `fa->cb(...)`
  call — the earliest point in the fetch path any diagnostic in this
  investigation has observed the value bytes, matching round 7's Seam A
  placement exactly.
- No new `#include` needed in either file: `slotcask_record_size_varlen`
  and `slotcask_seg_max_bytes` are both defined earlier in the same
  translation unit as Seams W and F (`slotcask.c:3527`, `slotcask.c:1356`
  respectively), and `log.h` is already transitively available via
  `types.h` (confirmed in round 6's Task 1).
- Both seams' key buffers (`kbuf[64]`) are sized generously above this
  fixture's longest key (`"n_4"`, 3 bytes) — if `klen` ever exceeds 63
  for some other object/test that happens to route through either
  instrumented function, the key gets silently truncated in the log line
  only (never in the actual write/read path, which use `key`/`klen`
  unmodified) — acceptable for a temporary diagnostic seam.
- Both seams' raw-byte windows (`vb[16]`) are bounded by the segment's
  actual remaining mapped bytes (`slotcask_seg_max_bytes()` minus the
  record's value-start offset), never by `db->slot_size`, `vlen`,
  `used`, or `rec_size` — a fixed-`slot_size`-bounded window can read
  past the end of the mmap for a variable-length trimmed record staged
  near a segment's tail (see Seam W's comment), so the bound must be
  derived from the mapping's true size, not the schema's nominal ceiling.
  Both seams log `vcopy` so a clipped window is visible in the evidence
  rather than silently under-reading.
- Task 4's decision table is evaluated per-platform independently;
  Linux's two legs are expected to land on the "no divergence" row and
  macOS on one of the H-hole/H-exact-fit/H-neighbor rows (or, if the
  evidence contradicts `pool_split_leftover`'s own invariant, the
  H-split-zeroed contradiction row, which blocks the rollup and requires
  `PLAN_NOTES.md`) — a Linux leg landing on a contradiction row is itself
  a finding to report, not something to silently reconcile.
- Seam W's `got_pool` field (logged straight from `st[i].got_pool`,
  `uint8_t`) is not optional evidence, but it is not sufficient alone
  either: it only distinguishes H-hole (fresh, never-written slot;
  `got_pool=0`) from a pool-popped slot (`got_pool=1`) — and a
  pool-popped slot needs `split`/`pool_cap` (also logged) to further
  distinguish "a split was attempted" (`split=1`) from H-exact-fit
  (`split=0`: no leftover was split off, so the tail belongs to a
  separate allocation unit not confirmable without step 6's adjacency
  proof). Task 4's decision table must not conclude H-hole from a
  garbage tail alone without checking `got_pool` first, and must not
  treat any `got_pool=1` garbage tail as automatically explained without
  further checking `split`.
- `split=1` alone does not mean the tail bytes are already zeroed —
  `bulk_phase3_stage_pending` discards `pool_split_leftover`'s own
  return value on the release path (`slotcask.c:4523-4526`). Worse, even
  a captured return value would not be enough: `pool_split_leftover`
  returns `-1` both when its `segcache_acquire` fails *before* the
  `memset` ever runs (`slotcask.c:3482-3483`) and, via a different
  non-zero value, when `pool_push_free_cap` fails *after* the `memset`
  already ran (`slotcask.c:3489-3490`) — a failed `pool_push_free_cap`
  after a successful `memset` still leaves the tail correctly zeroed, so
  the return code cannot distinguish "never zeroed" from "zeroed, but
  registration afterward failed." So Seam W instruments
  `pool_split_leftover` itself, under `TEST_BUILD`, with a thread-local
  that records directly whether its `memset` ran, independent of the
  function's return value; the call-site half then captures that as
  `split_memset_ran` immediately after the call (the raw return value is
  still captured too, as `split_rc`, for completeness/debugging, but is
  not the routing signal). Only `split=1 && split_memset_ran==1`
  licenses treating non-zero garbage in the tail as a contradiction of
  `pool_split_leftover`'s zero-then-register invariant; `split=1 &&
  split_memset_ran==0` means the `memset` never ran at all and the
  garbage is unsurprising — a separate finding about
  `pool_split_leftover`'s `segcache_acquire` failure path, not about the
  tail bytes' origin. Task 4's decision table must not classify a
  `split=1` garbage tail as the H-split-zeroed contradiction without
  also checking `split_memset_ran==1`, and must not rely on `split_rc`
  alone to make that determination.
