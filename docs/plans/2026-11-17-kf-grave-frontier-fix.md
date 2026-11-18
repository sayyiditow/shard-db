# Fix: recovery/stat/O_DIRECT scans break on abandoned-staged-record runs (kf dangling live refs)

Date: 2026-11-17. Branch: `fix/kf-grave-frontier` (created off `main`; work stays
uncommitted per repo execution mode). Investigation handover:
`docs/plans/2026-11-17-kf-dangling-refs-handover.md`.

---

## Brief (approval surface)

### Problem and root cause

Every mutation writes its record bytes with flag=0 ("P phase"), durably,
*before* the KFM2 marker (M) and activation (A). A write whose window dies
between P and M leaves the record on disk as a structurally-valid flag=0
record with intact content — an inert "grave". This is a designed on-disk
state (the record bytes are the P-phase output; the marker/activation never
happened); the incident's exact trigger (daemon kill vs pre-marker I/O
failure) is not provable from disk alone and is carried as an open item
below. Four scans share the same faulty tail heuristic — resync within one
`slot_size`, and if no live/tombstone header sits in the window, break as
"ordinary unwritten/sparse tail":

1. `recover_one_stream` (`src/db/slotcask.c`) — startup frontier scan. A run
   of graves exceeding `slot_size` before the next live record parks
   `reserve_off` mid-file; later appends then overwrite whatever lies beyond,
   including kf-live records → dangling live refs, which the 2026.11.1
   layout migration refuses (`invalid live kf references`). Confirmed by
   forensics of the incident data (players stream 3: the Nov-1 session's
   appends restarted at offset 0 and crushed the live records at 432 and
   1224; the segment head held only dead flag=0 content for more than one
   slot past offset 0) and by a failing regression test on HEAD
   (`reserve_off` parks at 0 instead of 216; appends clobber the live
   record; 1 dangling ref). The scan is unchanged since 5e1aa37
   (2026-08-14); the bug exists identically in 2026.10.2 and HEAD.
2. `seg_stat_one_varlen` (`src/db/slotcask.c`, only caller
   `compact_one_stream`) — vacuum's per-segment live count. The same break
   undercounts live records that sit behind a grave run.
3. `compact_migrate_records_varlen`'s recipient free-list walk
   (`src/db/slotcask.c`) — same break stops the walk at a grave run, so the
   tombstone/grave capacity behind it is never harvested; with graves
   counted as segment occupancy the migration can then run out of free
   slots mid-migration and fail.
4. `seg_scan_o_direct` (`src/db/io_direct.c`) — the O_DIRECT scanner used by
   vacuum donor migration *and* reindex. On a flag=0 hit with nothing in the
   window it stops (`padding_desync` → success) — records beyond the graves
   are not migrated/reindexed before the donor is dropped.

The fix: struct-valid flag=0 records with content are dead space — every
scan steps over them by their declared size; resync targets include graves
(hash-verified); every grave is hash-verified before it is stepped over
(a failed hash is treated as a desync, never skipped blindly); the
"unwritten tail" break fires only after a full-remainder search proves
no hash-valid record exists ahead — and that search is hole-aware: it asks
the filesystem for data extents (`lseek(SEEK_DATA/SEEK_HOLE)`) and searches
only those, reading them with `pread` (never through the mapping), so a
healthy open's sparse 128 MiB tail costs one open, a few `lseek` calls and
one small `pread` per stream instead of faulting in up to 128 MiB of zero
pages; whether `SEEK_DATA` may be trusted is decided by a per-filesystem
runtime probe, and any doubt falls back to a (slower, still mmap-fault-free)
rolling `pread` scan; unparseable garbage keeps failing closed exactly as
today.

### Logical flow

1. A write window dies between P (bytes durable, flag=0) and M. The segment
   keeps a struct-valid flag=0 record with content.
2. Repeated over months of dev churn → long grave runs (the incident object
   has one key with 24+ consecutive graves in a row, 8032-byte objects in
   the vacuum fixture terms; the players object has grave runs totalling
   kilobytes per stream).
3. Startup: `recover_one_stream` parks `reserve_off` at the first grave run
   → appends clobber live records → dangling refs. Vacuum on any object
   with graves behind live data: `seg_stat_one_varlen` undercounts, the
   recipient walk stops, the donor scanner stops → segments dropped or
   under-migrated; reindex misses records.
4. The fix makes all four scans step over graves and break only on
   verified-dead remainders, so the frontier lands past live data, vacuum
   neither drops nor under-migrates (and reclaims grave space), and reindex
   sees every record.

### Decisions made

Wording note: the flag=0 content records are called "graves" below, but a
staged record can also be *live-pending* (its KFM2 marker is durable and
forward replay activates it after `recover_streams`). Stepping over by
declared size is what stops the frontier from overwriting those too — a
second reason the fix is right. Comments and the WARN say "staged,
not-activated".

| Decision | Alternatives considered | Why | Tag |
|---|---|---|---|
| Fix all four scans (frontier, vacuum stat, recipient free-list walk, O_DIRECT scanner) | Frontier scan only; refuse-and-explain only | The sibling scans destroy or strand data on the same incident-shaped files (vacuum drops/under-migrates; reindex misses records; vacuum is the documented reclaim path for graves, so it must handle them). | DEFAULT |
| Pass over hash-verified content graves by declared size; resync targets include hash-verified graves; break only when the hole-aware full-remainder search finds no hash-valid record ahead; keep `EUCLEAN` for unparseable records | Auto-repair dangling refs; tolerant break anywhere; bounded windows everywhere | Breaking with recoverable records ahead is the bug; the hole-aware full-remainder search guarantees no hash-valid record is ahead when it breaks. Unparseable (torn) records keep today's fail-closed semantics. Hash-verifying resync targets (including graves) rejects garbage that merely misparses as a flag=0 header. | DEFAULT |
| Widen the search window to the file remainder in the mmap scans, and use a bounded rolling buffer in the O_DIRECT helper | Keep `slot_size` windows | Bounded windows are the bug. The mmap scans read mapped memory (no extra allocation); the O_DIRECT helper gets a fixed-size rolling search so no file-proportional malloc is introduced. | DEFAULT |
| Harvest grave capacity in the recipient free-list walk; count graves toward `total` (never `live`) in `seg_stat_one_varlen` | Leave grave space unharvested | A grave's extent is exactly its declared record size (no later record inside it), so its capacity is safely reusable; this is what makes vacuum actually reclaim grave space (the WARN log promises it). | DEFAULT |
| Hash-failed live/tombstone headers keep base's resync treatment (step to the next target; fail closed when none exists) | Always fail closed on hash mismatch | Base already steps silently when a target lies within its (bounded) window and fails closed otherwise; the wider window only changes *which* cases step. Documented as deliberate parity. | DEFAULT |
| One hole-aware wide resync implemented ONCE as `seg_resync_wide_fd` (io_direct.c) using `pread` + `lseek(SEEK_DATA/SEEK_HOLE)`, called by the three mmap scans (via a thin `seg_resync_graves_wide` wrapper, bounded search first) and by `od_varlen_resync_find`; `SEEK_DATA` trust decided by a per-`st_dev` runtime probe (`od_seek_data_trusted`), not an allowlist; untrusted/anomaly → rolling `pread` scan | A mapped-memory extent search (previous draft: chain-faults the 128 MiB mapping on tmpfs by reading past an extent, and cannot see a record that runs from an extent into a hole-punched region); a filesystem allowlist (previous draft: macOS/overlayfs/ZFS/NFS never trusted → ~230 ms and up to ~1 GiB RAM per healthy open); a separate zero-tail check (~5 syscalls per interior gap) | `pread` of a hole returns zeros without instantiating mapped pages and the page cache is coherent with dirty mmap pages, so the search is cheap and sees fresh writes; padding the read by `max_slot_size` handles sparsified records; one implementation means one test surface for the O_DIRECT and mmap paths. The probe tests the exact property relied on (dirty unflushed page reported as data) on the actual filesystem instead of guessing from `f_type`. Cost is bounded by the data extent (~4 ms per 8-stream object measured, ≤ a 2 MiB granule on tmpfs). | DEFAULT |
| Hash-verify every grave before stepping over it in all four scans (a failed hash is a desync: resync/tail rules apply, never a blind step) | Trust the record-size chain for in-chain graves (previous draft) | The chain is trustworthy on healthy files (`pool_split_leftover` zero-fills excess), but on a damaged file an unverified step in the O_DIRECT scanner or `seg_stat_one_varlen` can skip live records — i.e. lose data in vacuum/reindex. One xxh128 over the key per grave is cheap and makes all four scans uniform. | DEFAULT |
| Ship as release 2026.11.2 with npm binding 1.5.0 (version bump is part of the plan: Task 8) | Docs-only entry (previous draft); fold into 2026.11.1 | 2026.11.1 (tag, GitHub release) and npm-v1.4.0 are already published and embed the buggy engine, so the fix must reach embedded users as a new binding version. Confirm the numbers if a different train is planned. | DECIDED by the human 2026-11-17: shard-db 2026.11.2, npm 1.5.0 |
| Bump `SHARD_DB_VERSION` for this fix (re-stamps roots; one-way rollback barrier to 2026.11.1/1.4.0) | Ship as an unstamped patch that keeps `"2026.11.1"` | A bump gives support/diagnostics an unambiguous "has the fix" marker and matches the release process; the cost is the rollback barrier, which users who hit the refusal might trip over. Keeping `"2026.11.1"` makes the fixed and buggy binaries indistinguishable. | DECIDED (implied by the human's 2026.11.2 choice): bump and accept the rollback barrier |
| After a failed full-remainder resync keep base's tolerant "break" (frontier parks; a hash-corrupted live record beyond a zero gap can still be overwritten) | Fail closed (`EUCLEAN`) when the search finds no hash-valid record ahead but non-zero bytes remain | Fail-closed is a behaviour change on files that open today (including legitimately odd sparse layouts); this plan fixes the proven bug without widening refusals. Revisit if corrupt-header overwrite is ever observed. | DEFAULT (alternative noted) |
| Document the `invalid live kf references` refusal for operators (cause, what to do) but do NOT ship an automatic repair command in this release | Ship a repair command now (e.g. `shard-db repair-refs`); leave undocumented | The refusal is already shipping in 2026.11.1/1.4.0 and users can hit it today with no guidance. A repair tool rewrites kf state and deserves its own plan and review; recommend it as a follow-up. Guidance in this release: restore the object from a pre-upgrade backup, or contact the maintainers — no byte-editing recipes are published. | DECIDED by the human 2026-11-17: no repair command in this release |
| Keep `slotcask_validate_live_refs` refusal semantics unchanged | Auto-heal dangling refs during migration | Refusal caught the incident correctly; repair decisions belong to the human. | DEFAULT |
| One `LOG_WARN` per stream per open (noisy until vacuum reclaims the graves; accepted — it is the operator signal) (in `recover_one_stream`, active segment only) when graves were stepped over | Silent; per-record logs | Operators need to see reclaimable dead space (vacuum reclaims it) and get forensic evidence for this incident class. | DEFAULT |
| No frontier-vs-kf reconciliation pass at open | Belt-and-braces second guard | With the scan fixed, the frontier is derived by walking every record to EOF; a kf walk adds O(live records) segment reads at open for protection against a bug class the fix eliminates. Revisit if another frontier bug ever appears. | DEFAULT |
| Sanitizer gate: 3 consecutive ASan+UBSan and 3 consecutive TSan full-suite runs | Single round each | The changed loops run on `parallel_for_io` recovery workers, read shared segcache mappings, and write `rotation_lock`-guarded shared state — AGENTS.md's 3× trigger list applies by the letter. | DEFAULT |
| Changelog: bullet in `CHANGELOG.md`'s index + `##` section in `docs/reference/changelog.md` + new `docs/release-notes/2026.11.2.md` | Fold into an existing section | No unreleased 2026.11.x section exists at HEAD. Version numbers decided by the human: 2026.11.2 / npm 1.5.0. | DECIDED |

### Blast radius

- `recover_one_stream`: static; only caller is `recover_streams` at
  `slotcask_open` (daemon start, embedded `shard_db_open()`, object open,
  layout-migration opens in `src/db/query_find.c`). `reserve_off` consumers
  (`append_reserve_single_varlen`, rotation, init) only see a more correct,
  farther-forward frontier; the skipped region is dead space that vacuum
  compaction reclaims as before.
- `seg_stat_one_varlen`: only caller is `compact_one_stream`; counts become
  more correct (graves occupy space but are never live).
- Recipient free-list walk in `compact_migrate_records_varlen`: harvests
  grave capacity in addition to tombstones; migration no longer starves.
- `seg_scan_o_direct`: callers are vacuum donor migration and the reindex
  scan (`src/db/index.c`); scans see every record again.
- No on-disk format change; no wire change; no CLI/config change. Existing
  data is unaffected. The engine change ships inside the npm binding (it
  embeds the engine), so the binding needs a new version (1.5.0) with fresh
  prebuilds; Task 8 covers the `src/db/version.h` / `npm/` bumps and the
  version-gate test (`src/test/cases/test_version_gate_2026_11.c` pins
  `SHARD_DB_VERSION`). No format/wire/API break, BUT a one-way rollback
  barrier: opening a root under 2026.11.2 re-stamps `.version`, after which
  2026.11.1 and npm 1.4.0 refuse it (`DOWNGRADE`, pinned by the gate test).
  State this in the changelog, release notes and npm README (Tasks 7–8).
- Vacuum on an object that is *already* damaged (a live kf entry pointing at
  a grave or at another key's record): unchanged from today — vacuum does
  not validate kf references, and harvesting a grave referenced by a
  dangling live entry reuses space the entry still names. Pre-existing
  exposure on already-corrupt data; documented, not guarded (a
  `slotcask_validate_live_refs` pre-pass would cost a full kf walk per
  vacuum).
- Behaviour change visible only on corrupt/degraded files: states that today
  open/compact with silently-wrong results now recover correctly, or fail
  closed (`EUCLEAN`/`-EIO`) exactly where they already did (unparseable
  records). Hash-corrupted-but-parseable records: base already steps
  silently when a resync target lies within its bounded window; the wider
  window makes stepping (instead of refusing) the outcome in more cases —
  deliberate, documented in the code comment.

### Test strategy

- `test-slotcask-grave-run-frontier` (an incident-only draft exists in the
  tree, already wired into `build.sh`, with inverted "BUG" assertions and a
  throwaway header; Task 1 rewrites it into pass-after-fix form and adds the
  remaining scenarios): incident shape + interior grave run +
  grave run at the tail + tombstone-run pin + gap-then-grave compound.
  Frontier scenarios fail on base; the tombstone scenario is the
  no-regression pin. Plus a far-island scenario that pins the data-extent
  hole-aware wide resync (live record behind a multi-MiB hole) and a hash-failed
  grave scenario (a flag=0 header with a wrong hash must not be stepped
  over blindly), a healthy-reopen no-materialization scenario, and a
  hole-punched-spanning-record scenario.
- `test-od-scan-healthy-tail` (new): healthy-tail read bound (self-calibrating,
  passes on base — a guard), record-behind-a-hole delivery (fails on base),
  grave larger than the scan chunk (resume-past-grave; passes on base,
  proven only by mutating the rule — a guard), extent larger than the read
  buffer (overlap rule; fails on base), grave ending exactly at EOF, and a
  hole-punched spanning record.
- `test-slotcask-vacuum-grave-run` (new, template
  `test_varlen_compact_stat_resync`): 512 KiB test segments → rotated
  fixture with a two-grave run (2 × 8032 B) between the donor's live
  records → `slotcask_compact_segs` must preserve every live record (base:
  the donor scanner stops at the graves, the record behind them is not
  migrated, and the donor is dropped — data lost).
- Existing `test-slotcask-active-recovery-resync` (2026-08-14 zero-gap fix)
  and `test-varlen-compact-stat-resync` stay green.
- Full suite `./build/bin/shard-db-test run-all --jobs 8`; sanitizer gate
  3×3 per AGENTS.md.

### Out of scope

- **The grave-producing trigger** (handover task 2): the graves are proven
  staged-never-activated records, but which failure mode produced them in
  the incident app (daemon kill between P and M vs pre-marker I/O failure
  vs a post-staging rejection) is not provable from disk alone. The WARN
  log added in Task 2 gives operators the diagnostic for future
  occurrences; a reproduction attempt through the app's workload is a
  separate investigation.
- Auto-repair of already-dangling kf entries and any repair command (the
  incident data was repaired by hand; future detections keep refusing with
  the existing message; recommended as a separate, reviewed follow-up plan —
  Task 7 only documents the refusal).
- Improving `slotcask_validate_live_refs` diagnostics (offset enumeration).
- Tolerance for a *torn staged header* at the true tail (kill mid-header
  write): `EUCLEAN` before and after — pre-existing exposure, unchanged.
  Noted as a known adjacent gap.
- Free-pool *rebuild* (`slotcask_pool_rebuild_worker`) — correct for this
  incident. The separate post-compaction consumed-slot bug IS in scope: Task 9.

### Review log

- Round 1 (blind reviewer on this plan + repo): 2 majors, 6 minors, 6
  executor-guess points — all resolved. Major 1: the "zero gap then grave
  then live" compound defeats a bounded resync window and was (wrongly)
  called unproducible → full-remainder search with grave-accepting targets.
  Major 2: sibling scans added to scope with a preservation test. Minors:
  scenario-4 base expectation corrected; CHANGELOG.md structure fixed;
  revert mechanics via `git checkout --` + `git apply`; sanitizer 3×3
  mandated; tail-check cost acknowledged; blast-radius wording corrected.
- Round 2 (fresh blind reviewer): 1 blocker, 2 majors, 7 minors, 6
  executor-guess points — all resolved. Blocker: scenario 5's Z record is
  120 B only with an 80-byte value (24+16+80), not 72 → recipe corrected.
  Major: fourth scan (recipient free-list walk) added with grave-harvest
  rule. Major: vacuum-test build order pinned (graves must sit *between*
  live records; the stock fixture order cannot fail on base). Minors:
  clobber-trace prose fixed; resync targets now hash-verified; hash-failed
  record tolerance documented as base parity; O_DIRECT search gets a
  bounded rolling buffer; provenance claim softened with the trigger moved
  to out-of-scope; io_direct fast path uses `seg_scan_varlen_struct_ok`
  (hdr2 validated); blast-radius/LOG wording fixed; helper code inlined.
- Round 3 (fresh blind reviewer): 1 blocker, 3 majors, 7 minors, 6
  executor-guess points — all resolved. Blocker: the vacuum fixture's early
  `keyX` delete pushed a pooled slot that `kA`/`kkeyC` would pop, landing
  the live records ahead of the graves (no base failure) → delete deferred
  to the tombstone pass. Major: the g1/g2 flips target the donor
  `000000.dat`, not `000001.dat`. Major: scenario 4 asserted a read-back of
  a deleted key → now asserts absence. Major: the recipient-walk harvest
  rule is now pinned to hash-verified graves only (unverified extents could
  double-book with real tombstone slots and overlap migrated records).
  Minors: in-chain no-hash asymmetry documented at the Task 2 site;
  `seg_region_is_zero` cost comment corrected (page-cache, not "no cost");
  rolling-search termination clamp pinned; the carry-straddle path left
  unchanged explicitly; fixture size prose fixed (fillers 8040 B, graves
  8032 B); recipient-side grave harvesting gets a guard assertion; seg-max
  override reset pinned at test end.
- Round 4 (fresh blind reviewer, convergence check): **converged — zero
  blockers, zero majors.** The reviewer independently traced and confirmed
  the vacuum fixture recipe (pool timing, donor layout, base failure via
  the O_DIRECT stop, post-fix pass), scenario 5's arithmetic, the
  harvest rule's consistency, Task 4's loop-free rolling search and carry
  path, and Task 2's exclusive break conditions. 5 minors + 3 executor
  guesses, all applied: "the only break" wording; stale
  `seg_scan_varlen_hash_ok` invariant comment scheduled for update;
  Task 5's fifth f1-* delete moved before the close; the graves-flip step
  merged and renumbered; stat/harvest asymmetry under forged graves
  documented; `seg_region_is_zero` frequency clause corrected; lifecycle
  granularity and the first-four-f1-* choice pinned.

- Rounds 5–8 (blind reviewers after the post-review patches in this
  session — hole-aware wide resync, hash-verified graves, version bump
  Task 8, O_DIRECT extent search): round 5: 0 blockers / 3 majors (stale
  2026.11.1 test pins; O_DIRECT healthy-tail cost; undocumented rollback
  barrier); round 6: 0 / 3 (healthy-tail test unpassable as designed;
  scenario 2 arithmetic 336→360; base-proof build mechanics); round 7:
  0 / 6 (zero-tail syscall cost → replaced by the single hole-aware wide
  resync; extent-capped read termination; straddling-grave livelock → resume
  past a grave; missing O_DIRECT-behind-hole and recipient-harvest tests;
  storage-model.md doc sync); round 8: 0 / 3 (recipient-harvest fixture kept
  an extra f0 tombstone; no tests for the straddle/overlap rules; APFS
  allowlist unverified + macOS include) — all patched in the plan text but
  the round-8 patches have NOT been re-reviewed. **The 4-round cap is
  reached without convergence; escalated to the human.** The human
  then authorized further blind rounds until no issues remain (decisions:
  2026.11.2 / npm 1.5.0; no repair command).
- Round 8 adjacent finding (post-compaction stale free-pool overwrite) was
  confirmed and root-caused by the end-to-end review and is now Task 9.
- **End-to-end review (Opus, 2026-11-17)**: implemented every task in a
  scratch copy and ran it. New tests fail on base for the stated reasons and
  pass fixed; mutation proofs (a)-(d) fail the matching test; full normal
  suite (690 cases) and one TSan full run clean; ASan+UBSan clean on new and
  related cases; incident-shaped reproduction fixed (base `reserve_off=0`,
  live records clobbered; fixed `reserve_off=4752`, 0 invalid refs). Found 3
  majors (M1 restored dense segments cost ~155 ms/object open; M2 wide search
  fell back to full mapping when open/fstat failed; M3 stale pool), all fixed
  by Tasks 9-11 below with measured proof. Reference diff:
  `docs/plans/2026-11-17-kf-grave-frontier-fix.reference.patch` (2282 lines,
  the executor may diff against it but must follow the Plan text; delete the
  file before hand-back). Minors fixed in text: test names, (v)(a) passes on
  base (only (v)(b) fails), `SLOT_SIZE` renamed `GF_SLOT` in the test.

---

## Plan (executor-facing)

### Embedded execution rules

- **Execution order** (task numbers group by topic, they are NOT the run
  order): Task 1 (write the frontier test, run on base) → Task 4a
  (instrumentation, then `cp src/db/io_direct.c /tmp/io_direct.instrumented.c`)
  → Task 5 (write both new test cases, run on base) → Tasks 2, 3, 4 (the
  fixes) → Task 6 (proofs, suite, sanitizers) → Task 7 (docs) → Task 8
  (version bump; run last so the suite in Task 6 can be re-run once).

- Branch: work on `fix/kf-grave-frontier` (already created off `main`). Leave
  ALL work uncommitted — the human runs every git write.
- Build: `SKIP_TESTS=1 ./build.sh`. Full suite:
  `./build/bin/shard-db-test run-all --jobs 8` (drop to `--jobs 4` on OOM or
  unexplained empty responses; remove `/tmp/shard-db-*` after every run).
- If the plan's assumption about the code is wrong (a symbol doesn't exist or
  doesn't behave as stated), write `PLAN_NOTES.md` describing the mismatch and
  halt — do not guess or continue. If a symbol has merely moved and the
  intended site is unambiguous, proceed and record it in `PLAN_NOTES.md`.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.
- Sanitizer gate (after all fixes, before handing back), 3 consecutive runs
  each: `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` then
  `./build/bin/shard-db-test run-all --jobs 6` ×3;
  `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh` then
  `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 3` ×3.
  No `halt_on_error=0` anywhere. Remove `/tmp/shard-db-*` after every run.

### Baseline proof (already executed during the investigation)

`test-slotcask-grave-run-frontier` on the base branch (HEAD = 163b36a):

```
reserve_off after reopen: 0 (correct: 216)
not ok 13 - BUG: frontier parked at/before the live record C
ok 14 - insert D
ok 15 - insert E
ok 16 - insert F (lands on C)
not ok 17 - BUG: live record C clobbered by append
ok 18 - BUG: dangling live kf refs detected
invalid live refs: 1
# test-slotcask-grave-run-frontier: 16 passed, 2 failed
```

The case file `src/test/cases/test_slotcask_grave_run_frontier.c` and its
`build.sh` link-line entry are already in the working tree.

### Shared groundwork for Tasks 2–4 (test-first: Tasks 1 and 5 define the failing tests this groundwork must satisfy)

**New shared inline in `src/db/seg_scan_varlen.h`**, placed next to
`seg_scan_varlen_resync` (same doc style, same 8-byte-alignment rules;
`seg_scan_varlen_resync` itself stays byte-identical; it may end with no
production callers — check test usage and leave it in place):

```c
/* seg_scan_varlen_resync variant whose resync targets additionally
   include structurally valid flag==0 records with content (klen or vlen
   nonzero) — abandoned staged payloads ("graves"). Zeros-only flag==0
   regions are still never targets. Hash verification applies to every
   target: flag in {1,2} as before, and flag==0 targets must carry the
   xxh128 of their own key bytes (genuine graves do; garbage that merely
   misparses as a flag=0 header does not). A grave target is a safe resume
   point because the caller steps over it by its own declared size. */
static inline int seg_scan_varlen_resync_graves(const uint8_t *map,
                                                size_t map_size, size_t pos,
                                                size_t max_slot_size,
                                                size_t window,
                                                size_t *out_off) {
    if (pos >= map_size) return 0;
    size_t start = pos & ~(size_t)7;
    size_t limit = (window > map_size - pos) ? map_size : pos + window;
    for (size_t cand = start; cand < map_size && cand <= limit; cand += 8) {
        if (cand < pos) continue;
        size_t rec_size;
        uint8_t flag;
        uint16_t klen;
        uint32_t vlen;
        if (!seg_scan_varlen_struct_ok(map, map_size, cand, max_slot_size,
                                        &rec_size, &flag, &klen, &vlen,
                                        NULL))
            continue;
        if (flag == 0) {
            if (klen == 0 && vlen == 0) continue;      /* zero fill */
            if (!seg_scan_varlen_hash_ok(map, cand, klen)) continue;
        } else if (!seg_scan_varlen_hash_ok(map, cand, klen)) {
            continue;
        }
        *out_off = cand;
        return 1;
    }
    return 0;
}
```

**New helpers (design: ONE `pread`-based hole-aware wide search, shared).**
The wide search must never walk the mmap past an extent (on tmpfs a read
fault on a hole instantiates a page — a 2 MiB huge page — which the next
`SEEK_DATA` then reports as data: the loop chain-faults the whole 128 MiB
mapping; measured 230–290 ms and ~128 MiB RAM per healthy open), and must
handle a record that starts in an extent and runs into a hole (sparsified
files from `cp --sparse`/`fallocate -d`: a record's zero pages can be
hole-punched). Both are solved by reading with `pread` into a scratch buffer
(`pread` of a hole returns zeros without instantiating a mapped page on
tmpfs; the page cache is coherent with dirty mmap pages). So:

1. `src/db/seg_scan_varlen.h`: in the existing `seg_scan_varlen_resync_graves`
   inline (code above) add a cheap zero-word pre-test right after the
   `if (cand < pos) continue;` line:
   `if (cand + 24 > map_size) continue; { uint64_t w; memcpy(&w, map + cand + 16, 8); if (w == 0) continue; }`
   (the bounds test MUST come before the load: an unconditional `memcpy` reads
   up to 16 bytes past the buffer/mapping — an ASan heap-buffer-overflow on
   `seg_resync_wide_fd`'s exact-size buffer and a possible SEGV/SIGBUS at the
   end of a 128 MiB mapping)
   (klen, flag, hdr2 and vlen all zero is exactly the zero-fill case the
   function already excludes after `struct_ok`; this makes scanning zero
   regions one load per 8 bytes). Behaviour is unchanged.
2. `src/db/io_direct.c` (+ declarations in `src/db/io_direct.h`), non-static:
   - `int od_seek_data_trusted(int fd, const char *path);` — runtime probe
     replacing any filesystem allowlist. Trust is the property "a dirty,
     unflushed mmap page is reported as data by `lseek(SEEK_DATA)`". Probe
     per `st_dev` (fixed 32-entry cache guarded by a mutex; a full cache just
     probes every call): `mkstemp()` a file named `.sdprobe-XXXXXX` in
     `dirname(path)`, `unlink` it at once, `ftruncate` 8 MiB, dirty one page with
     `pwrite(pfd, &b, 1, 4 MiB + 123)` (NOT an `mmap` store: a write fault on
     a full or quota-limited filesystem delivers SIGBUS, which would crash a
     daemon restart that base survives; `pwrite` returns ENOSPC/EDQUOT
     cleanly and leaves the same dirty page-cache state; no `msync`/`fsync`),
     `d = lseek(pfd, 0, SEEK_DATA)`; trusted iff `d >= 0 && d <= 4 MiB + 123`
     (a filesystem reporting the whole file as data returns 0 — still sound,
     merely not sparse-aware). Any failure (cannot create the file — read-only dir, EACCES, ENOSPC,
     EDQUOT, EROFS — `pwrite` error, ENXIO, unsupported) → NOT trusted and NOT
     cached (the probe is retried on a later call). Copy `path` into a local
     buffer before `dirname()` (it may modify its argument; `path` is
     `const`); includes `<libgen.h>`, `<limits.h>`, `<stdlib.h>`,
     `<sys/stat.h>`. The cache insert re-scans for an existing entry under
     the lock before inserting (parallel recovery workers all miss at once).
     A crash between `mkstemp` and `unlink` could leave a `.sdprobe-*`
     file: every directory reader ignores names that do not end in `.dat`
     (`data_file_id_from_name` returns -1), and `unlink` follows within
     microseconds. Linux/macOS/any POSIX: no per-platform tables.
   - `int seg_resync_wide_fd(int fd, const char *path, off_t file_size,
     size_t max_slot_size, off_t from, off_t *out_off);` — returns 1 and the
     absolute offset of the next resume target (hash-verified live/tombstone
     header or hash-verified content grave, i.e. what
     `seg_scan_varlen_resync_graves` accepts), 0 when none lies ahead, -1 on
     I/O error. `from` = first byte not already covered by the caller's
     bounded search. Algorithm (all reads are `pread`; EINTR-safe loop; a
     short read happens only at EOF):
     ```
     buf_sz  = max(2 * max_slot_size, 1 MiB)
     trusted = od_seek_data_trusted(fd, path)
     off     = from & ~7
     while off < file_size:
         start = off; limit = file_size
         if trusted:
             d = lseek(fd, off, SEEK_DATA)
             if d < 0:  if errno == ENXIO: return 0      # nothing but hole ahead
                        trusted = 0; continue            # anomaly: rescan this off untrusted
             if d >= file_size: return 0
             h = lseek(fd, d, SEEK_HOLE)
             if h <= d: trusted = 0; continue            # must advance
             start = max(off, d) & ~7;  limit = min(h, file_size)   # extent [start, limit)
         # pad the read with up to max_slot_size bytes beyond the extent end
         # (hole bytes read back as zeros) so a record that starts in the
         # extent and runs into a hole still passes struct_ok
         want = min(buf_sz, (limit - start) + max_slot_size, file_size - start)
         n    = pread_full(fd, buf, want, start)           # -1 on error → return -1
         if n < want: file_size = start + n                # file shrank/short read = EOF
         if seg_scan_varlen_resync_graves(buf, n, 0, max_slot_size, n, &rel):
             *out_off = start + rel; return 1
         if start + n >= file_size: return 0               # reached EOF, nothing found
         if trusted and n == (limit - start) + max_slot_size:   # whole extent + pad covered
             off = limit                                   # no overlap needed; next SEEK_DATA
         else:
             off = start + n - max_slot_size               # capped by buf_sz: one slot overlap
     return 0
     ```
     Termination: each iteration advances `off` — by `limit - start > 0`, or by
     `n - max_slot_size > 0` (`n > max_slot_size` because `buf_sz >=
     2 * max_slot_size` unless the read hit EOF, which returns). The buffer
     is allocated lazily at the first read's size and grown up to `buf_sz`
     (a healthy tail must not malloc 1 MiB). Soundness: "return 0" only after
     every byte range that could hold a target was read (data extents) or is
     a hole (zeros, which cannot hold a target: live/tombstone headers are
     non-zero, zero-fill flag==0 is excluded); every anomaly drops to the
     untrusted rolling `pread` scan of the remainder (correct, slower — no
     mmap faults, zero-word fast skip).
3. `src/db/slotcask.c`, file-local `static`, placed above `recover_one_stream`
   (used by it and by the vacuum scans below):
   ```c
   static int seg_resync_graves_wide(const char *path, const uint8_t *map,
                                     size_t map_size, size_t pos,
                                     size_t max_slot_size, size_t *out_off) {
       if (pos >= map_size) return 0;
       if (seg_scan_varlen_resync_graves(map, map_size, pos, max_slot_size,
                                         max_slot_size, out_off))
           return 1;                                    /* bounded: no syscalls */
       if (pos + max_slot_size >= map_size) return 0;   /* bounded search covered the rest */
       int fd = open(path, O_RDONLY | O_CLOEXEC);
       struct stat st;
       if (fd >= 0 && fstat(fd, &st) == 0) {
           off_t found = 0;
           off_t fsz = ((size_t)st.st_size < map_size) ? st.st_size : (off_t)map_size;
           int rc = seg_resync_wide_fd(fd, path, fsz, max_slot_size,
                                       (off_t)(pos + max_slot_size), &found);
           close(fd);
           if (rc == 1) { *out_off = (size_t)found; return 1; }
           if (rc == 0) return 0;
       } else if (fd >= 0) close(fd);
       /* open/fstat/IO failure: one full mapped search — correct, slow, rare */
       return seg_scan_varlen_resync_graves(map, map_size, pos, max_slot_size,
                                            map_size - pos, out_off);
   }
   ```
   `path` is in scope at every caller (`path` in `recover_one_stream`'s loop,
   the donor/recipient paths in `seg_stat_one_varlen` /
   `compact_migrate_records_varlen`); where it is not, build it with
   `seg_path_for(path, db->data_dir, sid, fid)`. Add `<fcntl.h>`,
   `<sys/stat.h>`, `<unistd.h>` only if `slotcask.c` lacks them; declare the
   two new functions via `io_direct.h` (already included by `slotcask.c`; if
   not, include it). The segment is re-`open`ed by path (not the
   segcache handle's `fd`: `seg_resync_wide_fd` calls `lseek`, which would
   move a shared fd's offset); segments are never renamed during open or
   under vacuum's exclusive lock, so a path swap is not a concern. The file
   size passed is `min(st_size, map_size)` (a segment larger than the mapping
   only occurs with the test seg-max override).
4. Cost model (state it in the changelog): healthy open on a sparse-aware
   filesystem = bounded search + one `open` + a few `lseek` + one small
   `pread` (the extent, ≤ a 2 MiB granule on tmpfs) per stream — ~4 ms per
   8-stream object measured vs 0.2 ms before; filesystems without a usable
   `SEEK_DATA` (or fully-allocated segments, e.g. plain-copy restores) take
   the untrusted rolling scan: ~one full-segment `pread` + zero-word skip,
   markedly cheaper than a mapped walk but still O(segment size) per stream
   per open. Record this explicitly in the release notes as a known cost.

A grave is a record the P phase wrote and the A step never activated: the
header (hash, klen, flag=0, hdr2, vlen) and payload are coherent; only the
flag never became 1. Stepping over one by its declared size is exact — no
later record can start inside it. The designed state is documented in the
bulk commit path (staged payloads stay flag=0 until activation; a window
that dies before its marker leaves them inert). Reference this plan doc and
the regression test names in the comments you write at each changed site.

One existing comment becomes stale and must be updated in the same change:
`seg_scan_varlen_hash_ok` in `src/db/seg_scan_varlen.h` says flag==0
records "have no real key content and are never passed here" — the new
inline and Task 3's harvest rule now pass hash-verifying flag==0 content
graves through it. Reword to "flag == 0 records passed here must carry
real key content (content graves); zeros-only flag==0 regions are never
passed".

### Task 1 — Formalize the failing regression test (test-first)

File: `src/test/cases/test_slotcask_grave_run_frontier.c` (exists; keep the
registered name `test-slotcask-grave-run-frontier`). Fresh scratch dir per
scenario: `/tmp/shard-db-grave-frontier-<pid>-<n>`, `rm -rf` before and
after each. Packaging: all scenarios live in ONE registered case wrapped in a single
`slotcask_init(64, 64)` / `slotcask_shutdown()` pair (as the draft does), each
scenario with its own scratch dir (names above) and its own `slotcask_open`.
Grave crafting recipe everywhere: insert the record, delete it
via `slotcask_delete_with_hooks` (segment record becomes flag=2), close the
db, then `pwrite` a single `0` byte at the record's segment offset +18
(flag byte) — leaving a struct-valid flag=0 record with intact content.
Unless stated otherwise, every scenario uses 16-byte keys and 28-byte
values (72-byte records), `slotcask_open(dir, 8, 1, 120)` (slot_size 120),
1 stream. "Insert lands at N" is asserted via `db.streams[0].reserve_off`
before and after the insert. After every reopen assert both
`ASSERT_EQ_INT(slotcask_validate_live_refs(&db, &inv), 0, ...)` and
`ASSERT_EQ_INT((int)inv, 0, ...)`, plus read-backs via `slotcask_get` +
`memcmp` against the exact original value bytes.

Scenarios:

1. **Incident shape** (the draft implements this one with inverted "BUG"
   assertions — rewrite them to assert the correct post-fix behaviour,
   removing the THROWAWAY header; the drafted base output in 'Baseline proof'
   is from the inverted form): graves at
   0 and 72, live record C at 144 (insert A, B, C; delete A and B; flip
   flags at 18 and 90). Reopen; assert `reserve_off == 216`; insert D, E, F;
   assert C, D, E, F all read back their original values. On base:
   `reserve_off` parks at 0, F clobbers C — fails as in the transcript
   above.
2. **Interior grave run**: insert A, X, Y, B (at 0, 72, 144, 216); delete X
   and Y; close; flip flags at 90 and 162. Reopen; assert
   `reserve_off == 288`; A and B read back; insert C; assert
   `reserve_off == 360` (C landed at 288) and B still reads. On base: the
   scan breaks at the first grave (`reserve_off` parks at 72); the first
   post-reopen insert lands on grave X at 72 and the third lands on B —
   the `reserve_off == 288` assertion fails.
3. **Grave run at the true tail**: insert A, X, Y (at 0, 72, 144); delete X
   and Y; close; flip flags at 90 and 162. Reopen; assert
   `reserve_off == 216` (past the graves, at the tail start — base parks at
   72) and A reads back. Fails on base.
4. **Tombstone run unchanged (no-regression pin)**: insert A, X, Y (at 0,
   72, 144); delete X and Y; close; reopen WITHOUT flipping any bytes.
   Assert `reserve_off == 216`, A reads back, and X and Y are absent
   (`slotcask_get` fails). MUST pass on base and stay green after the fix.
5. **Gap then grave (the compound)** — separate db in the same case, same
   slot_size 120. Recipe:
   1. Insert Z (16-byte key, **80-byte value** → record
      `(24+16+80+7)&~7 = 120`) at offset 0; insert A1 and A2 (72 B) at 120
      and 192.
   2. Delete Z (its 120-byte slot goes to the pool; record flag=2); insert
      S (72 B) — it pops Z's slot, writes 72 B at offset 0, and
      `pool_split_leftover` zeroes 72..119.
   3. Delete S and A1; close; flip S's flag byte (offset 18) and A1's
      (offset 138) to 0.
   Final layout: [grave S 0..71][zero gap 72..119][grave A1 120..191][live
   A2 192..263]. Reopen; assert `reserve_off == 264`; A2 reads back; insert
   a new record; assert `reserve_off == 336` and A2 still reads. On base:
   the scan at the first grave resyncs within `[0, 120]`, finds no
   flag∈{1,2} target (zeros and the A1 grave are skipped), breaks, parks
   `reserve_off` at 0 — the first post-reopen append lands at 0 and the
   fourth (at 216..287) overlaps live A2; the `reserve_off == 264`
   assertion fails. This scenario pins the grave-accepting resync (a
   bounded, strict-target resync would either park like base or `EUCLEAN`
   here).
6. **Far island behind a hole (pins the hole-aware wide resync)**:
   insert A, X, Y (at 0, 72, 144); delete X and Y; close; flip flags at 90
   and 162 (graves). Then `pwrite` a byte-for-byte copy of record A (72 B;
   its hash is the xxh128 of its own key, so it is a valid live record) at
   offset `8 MiB` (8-aligned, well past the first 120 B window; 8 MiB so a
   real hole precedes it even where tmpfs backs the file with 2 MiB huge
   pages). Reopen; assert `reserve_off == 8 MiB + 72` (the scan resyncs across
   the hole to the island, then reaches a verified-zero tail). Do NOT assert
   `slotcask_validate_live_refs` here (the copy has no kf entry). On base:
   A is live at 0 and the scan breaks at grave X (offset 72) →
   `reserve_off == 72`; the fail is the `reserve_off` assertion. Guards the fast path: a regression that treats
   "no data extent nearby" as "nothing ahead" without searching the extent
   that holds the island would park at 216.
7. **Hash-failed flag=0 header is not blindly stepped over**: insert A, X, Y
   (at 0, 72, 144); delete X and Y; close; flip flags at 90 and 162; then
   corrupt one byte of Y's *hash* (offset 144, byte 0). Reopen; assert the
   open does not step over Y as a grave: with nothing recoverable after it
   the scan breaks at the tail rules (`reserve_off == 144`, i.e. Y's start —
   dead to EOF), never a blind step to 216 that could mask a live record;
   A still reads back. (Pins the hash-verify-before-step rule; passes only
   after the fix because base parks at 72.)

8. **Healthy reopen must not materialize the sparse tail** (guards against
   any wide-search implementation that reads the mapping past an extent;
   DEFAULT-size segment, i.e. do NOT lower the seg-max override):
   insert two records into a 1-stream object, close, `stat` the stream's
   `000000.dat` and record `st_blocks * 512`; reopen; `stat` again; assert the
   growth is `< 4 MiB` (tmpfs with huge pages may legitimately instantiate
   one 2 MiB page). The scratch dir is on `/tmp` (tmpfs here), which is where
   an unclamped extent search chain-faults ~128 MiB; on a filesystem that does
   not instantiate hole pages on read the assertion trivially holds. This case
   passes on base and must stay green; it fails (≈ +128 MiB) on a wide
   search that walks the mapping past an extent end — verify once by
   temporarily replacing the `pread` search with a mapped walk.

9. **Record spanning into a hole-punched region (sparsified segment)**
   (Linux/`fallocate` only: skip with an explicit TAP skip elsewhere; open
   with `slot_size` 16384): insert A (72 B record), X, Y (small); delete X and
   Y; close; flip flags at 90 and 162 (graves). Then craft a live record R
   at `8 MiB`: key 16 bytes, value = 64 non-zero bytes + 15872 ZERO bytes + 64 non-zero
   bytes (16000 total) (declared record 24+16+16000 = 16040 B, already 8-aligned; hash valid; write it
   with `pwrite`), and `fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
   8 MiB + 4096, 8192)` on the segment so the two middle pages become a hole
   (the file is now sparse inside R). Reopen; assert `reserve_off == 8 MiB +
   16040` (R recognised as live across the hole: the wide search reads the
   extent plus `max_slot_size` zero padding). On base: parks at 72. Pins the
   padded `pread` (an extent-clamped mapped search would miss R). Do not
   assert `validate_live_refs` (R has no kf entry).

Run: `./build/bin/shard-db-test run test-slotcask-grave-run-frontier` →
scenarios 1, 2, 3, 5, 6, 7 and 9 must FAIL on base with the expected assertions;
scenarios 4 and 8 MUST pass on base (no-regression pins). Every assertion
message must start with `scenario N:` so the failing scenarios can be read
from the TAP output (all scenarios share one registered case). Paste the output, then
proceed.

### Task 2 — Fix the startup frontier scan in `recover_one_stream`

File: `src/db/slotcask.c`, static function `recover_one_stream`, the
active-segment scan loop (unique anchor: the `while (pos + 24 <= lim)` loop
ending with `last_offset = (off_t)pos;`). Semantics, in priority order at
each `pos`:

1. Struct-ok + `flag != 0` (live/tombstone, hash-verified as today) →
   `pos += rec_size`.
2. Struct-ok + `flag == 0` + content (`klen != 0 || vlen != 0`) +
   `seg_scan_varlen_hash_ok(h.map, pos, klen)` → **abandoned staged
   payload**: `pos += rec_size`, increment a local `uint64_t graves`
   counter (declare it BEFORE the segment loop so the final `LOG_WARN` sees
   it), continue. A torn pending *payload* with an intact header lands
   here too — its declared size is authoritative and the region is dead.
   Hash failure on a flag=0 content header is NOT a grave: fall through to
   step 3 (desync handling) — never a blind step. (Uniform rule across all
   four scans: every grave is hash-verified before it is stepped over.)
3. Everything else — a zero-fill/tail header (`flag == 0`, `klen == 0 &&
   vlen == 0`), a flag=0 content header whose hash FAILED, a live/tombstone
   header whose hash failed, or an unparseable header — is a gap/desync:
   `seg_resync_graves_wide(path, h.map, lim, pos, (size_t)db->slot_size,
   &next)`. Success → `pos = next`. Failure → if the header at `pos` is
   structurally valid with `flag == 0` → **break** (nothing hash-valid lies
   ahead: this is the ordinary unwritten tail on a healthy open — it costs
   one bounded search plus one open and an `ENXIO`/extent query, never a
   128 MiB scan — same outward behaviour as base; a hash-corrupted
   live/tombstone record ahead of a zero gap is still overwritten, exactly as
   on base — see the Brief's decision on fail-closed). Failure otherwise
   (`!valid`, base's definition: struct-invalid, or a live/tombstone header
   whose hash fails) → release the segcache handle, free `ids`,
   `errno = EUCLEAN`, return -1 (unchanged fail-closed semantics).
   Implementation note: keep structural validity, the hash result and base's
   `valid` as separate variables. A flag=0 content header whose hash FAILS is
   structurally valid (it takes the wide-resync → `flag == 0` break path),
   NOT `!valid` — do not set `valid = 0` for it (that would turn scenario 7
   into `EUCLEAN`).

Behaviour deltas vs base, to document in the code comment: content graves
break → stepped over (the fix); every gap/desync → resync is no longer
limited to `slot_size`: it is bounded first, then hole-aware over the whole
remainder, and the tail break happens only after that search finds no
hash-valid record ahead; hash-corrupted live/tombstone records keep base's
resync-else-fail-closed treatment, with the wider search making "step to the
next target" the outcome in more cases (deliberate).

After the loop, immediately before `free(ids)`/`return 0` (after the
`rotation_lock` block), log once when graves were passed:

```c
if (graves > 0)
    LOG_WARN(LOG_SUB_SLOTCASK,
             "recovery: %llu staged, not-activated record(s) (skipped as dead "
             "space; vacuum reclaims the inert ones once the segment rotates) in active segment of stream %03d under %s",
             (unsigned long long)graves, sid, db->data_dir);
```

(Note: the scan only ever walks the active segment — `file_id != last_id`
is skipped — so the message says "active segment".)

### Task 3 — Fix the vacuum-side mmap scans in `slotcask.c`

**`seg_stat_one_varlen`** (the `while (off + 24 <= file_size)` loop with the
`/* ordinary sparse tail */` break). Mirror Task 2's semantics with this
accounting: hash-verified content graves (`seg_scan_varlen_hash_ok`; a
failed hash is a desync → resync/tail rules, never a blind step) step
`off += rec_size` and count toward `total` exactly like a flag=2 tombstone (the space is written and occupiable) but
never toward `live`; any other header (zero-fill/tail, failed hash,
unparseable) → `seg_resync_graves_wide(path, h.map, file_size, off,
db->slot_size, &next)` → continue at the target; failure with
`ok && flag == 0` → break (nothing hash-valid ahead);
resync failure with `!ok` → release the handle and return -1 (unchanged).
Read the function's existing `total`/`live` accumulation and mirror the
tombstone branch for graves. Because both this count and the recipient walk (below) hash-verify
graves, `recipient_free` and the harvestable capacity agree on every input.

**The recipient free-list walk inside `compact_migrate_records_varlen`**
(the `while (off + 24 <= rmap_size)` loop that collects `flag == 2` slots
into `free_offs`/`free_caps`). Step-over semantics as above, with one
stricter rule: **every grave is hash-verified before its capacity is
harvested** (`seg_scan_varlen_hash_ok` at `off`, mirroring
`seg_scan_varlen_resync_graves`'s target rule). This is mandatory here —
unlike the frontier walk, an unverified forged extent pushed onto
`free_offs`/`free_caps` could double-book bytes with a real tombstone
extent harvested later in the same walk, and `varlen_compact_cb` writes
migrated records blindly at those offsets (overlapping writes = corrupt
recipient). Hash-fail on a grave → treat as a desync: go to the wide
resync below — do not harvest. Any other header (zero-fill/tail, failed hash, unparseable)
→ `seg_resync_graves_wide(recipient_path, rh.map, rmap_size, off,
db->slot_size, &next)` → continue at the target; failure with
`ok && flag == 0` → break (nothing hash-valid ahead); with `!ok` → free the lists, release the handle, return -1
(unchanged). Update the walk's opening comment ("Never add flag==0 padding
... to the free list") to state the new rule: zeros-only regions are still
never added; structurally valid, hash-verified content graves are added at
their exact declared extent — no later record can start inside a grave.

### Task 4a — Test instrumentation for the O_DIRECT resync search (behaviour-free; do this BEFORE Task 5)

In `src/db/io_direct.c`, under `#ifdef TEST_BUILD`, add a file-static
`_Atomic uint64_t g_od_resync_bytes_read` incremented by the byte count of
every `pread` inside `od_varlen_resync_find` (and, after Task 4, inside
`seg_resync_wide_fd` — keep the increments there), plus accessors declared in
`src/db/io_direct.h` under the same `#ifdef`:
`uint64_t od_test_resync_bytes_read(void);` and
`void od_test_resync_bytes_reset(void);`. ALSO land the probe now:
`int od_seek_data_trusted(int fd, const char *path);` defined in `io_direct.c`
and declared in `io_direct.h` exactly as specified in Shared groundwork item 2
(it is behaviour-free while nothing calls it, and Task 5 case (i) references
it, so the Task 4a snapshot used by the Task 6 base proof must contain it or
the base build cannot link). No other change. After this task
save the instrumented file OUTSIDE tmpfs and the repo's tracked paths:
`mkdir -p "$(git rev-parse --git-dir)/plan-backup" && cp src/db/io_direct.c
src/db/io_direct.h "$(git rev-parse --git-dir)/plan-backup/"` (inside `.git`:
untracked, not on tmpfs, nothing in the home directory). The Task 6 base proof
restores these copies, not HEAD's files, so the new tests still link.

### Task 4 — Fix the O_DIRECT scanner (vacuum donor migration + reindex)

File: `src/db/io_direct.c`, function `seg_scan_o_direct` and its helper
`od_varlen_resync_find`.

- In-chunk fast path: currently
  `if (flag == 0 || flag > 2 || rec_size > max_slot_size)`. Split it: when
  `flag == 0` with content (`klen || vlen`) AND
  `seg_scan_varlen_struct_ok(chunk, chunk_len, pos, max_slot_size, ...)`
  passes (this also validates hdr2) AND the whole record fits in the
  chunk AND `seg_scan_varlen_hash_ok(chunk, pos, klen)` passes (the key
  bytes are inside the resident record), step `pos += rec_size` (dead
  space — no callback; `cb` still fires only for flag==1). A hash failure
  takes the existing resync machinery — never a blind step, because this
  scanner feeds vacuum donor migration and reindex, where a wrongly
  skipped live record is data loss. Everything else keeps the existing resync machinery.
- In-chunk resync and `od_varlen_resync_find`: switch both to
  `seg_scan_varlen_resync_graves`. The in-chunk resync window stays
  `min(remaining chunk bytes, max_slot_size)` exactly as today: a miss inside
  the chunk falls through to `od_varlen_resync_find`, which does the wide
  search.
- `od_varlen_resync_find`: keep the existing bounded first attempt (the
  single `read_cap = window + max_slot_size` pread + search — now via
  `seg_scan_varlen_resync_graves`); on a MISS replace "return not-found" with
  a call to the shared `seg_resync_wide_fd(fd, seg_path, file_size,
  max_slot_size, from, &found)` (Shared groundwork, item 2) with
  `from = aligned_off + search_from + window_effective` rounded down to 8,
  where `window_effective` is the window the bounded search actually used
  (NOT `read_cap`, which would skip candidates in
  `[window, window + max_slot_size)`; one-candidate overlap is harmless). It
  uses the scan's own `fd`, which is `pread`-only, so the helper's `lseek`
  calls only move an unused file offset. `1` → resume (see the next bullet),
  `0` → return not-found (`1`, the existing "no valid record" code), `-1` →
  the existing error path. No logic is duplicated here: the extent loop, the
  `max_slot_size` zero padding, the overlap rule and the termination argument
  all live in `seg_resync_wide_fd`.
- **Resume past a grave (removes the straddle bounce and any livelock):** when
  a resync target is a hash-verified flag==0 content grave, report the resume
  offset as `target + rec_size` (the grave is dead space; stepping over it is
  exact), not `target`. A grave straddling a chunk end therefore resumes
  beyond it instead of restarting the scan AT the grave — which would loop
  forever when the grave is larger than the scan chunk (`odirect_buf_size`
  may be as low as 1 MiB and records can exceed it). Applies to the Stage 2
  carry resync and to `od_varlen_resync_find`'s result; the in-chunk step
  already advances past a grave that fits. `od_varlen_resync_find` must re-parse the
  header at the target (`struct_ok` + flag) to know whether it is a grave and
  to get `rec_size`. If `target + rec_size == file_size` return that value as
  the resume offset: the restart then gets `remain == 0` from `dbctx_init`
  and the scan ends cleanly (no new return code needed); if it exceeds
  `file_size` the header was not a valid record — treat as not-found.
- Leave the chunk-boundary carry reassembly itself (Stage 1 / Stage 2, the
  second occurrence of the `flag == 0 || flag > 2 || rec_size >
  max_slot_size` condition) otherwise UNCHANGED: it still routes
  desync/straddle cases to `do_resync`, which now resumes past a grave per
  the rule above. Do not step `pos` past a straddling grave inside the carry
  logic (its bytes are not fully resident in the chunk).
- Keep the caller's outcome mapping unchanged: `rrc > 0 && padding_desync`
  → stop-with-success (now: "no hash-valid record ahead");
  `rrc > 0 && !padding_desync` → `-EIO` with the existing LOG_ERROR;
  `rrc < 0` → error. Reindex and vacuum donor migration therefore see every
  record again.

### Task 5 — Vacuum preservation test (test-first for Tasks 3–4; write
### it before them)

New file `src/test/cases/test_slotcask_vacuum_grave_run.c`, registered as
`test-slotcask-vacuum-grave-run`; add to the `build.sh` link line next to
`test_varlen_compact_stat_resync.c`. Template:
`src/test/cases/test_varlen_compact_stat_resync.c` +
`varlen_compact_fixture.h` (do NOT `#include` the fixture header into this
file if it only needs `VARLEN_FIXTURE_TEST_SEG_BYTES`: its unused static
helpers raise `-Wunused-function`, a new warning against the Definition of
done — define the constant locally, or mark the helpers
`__attribute__((unused))`). Record sizes here vary by key length — a
record is `(24 + klen + vlen + 7) & ~7` bytes: the 8000-byte-value fillers
are 8040 B (9-byte keys) and the graves 8032 B (2-byte keys);
`VARLEN_FIXTURE_TEST_SEG_BYTES` = 512 KiB; default `slot_size` 8192.

Do NOT use `varlen_compact_fixture_build` as-is — its donor layout puts
every live record ahead of the fillers, which cannot produce a base
failure. Build the fixture with this explicit order (all inserts/deletes
via `slotcask_insert_with_hooks`/`slotcask_delete_with_hooks`, values
`memset`-filled like the fixture does). Record sizes here: `kA` is
`24+2+1 → 32` B, `kkeyC` is `24+5+6 → 40` B, `g1`/`g2` (2-byte keys,
8000-byte values) are `8032` B each, `keyX` is `24+4+5000 → 5032` B, the
`f0-*`/`f1-*` fillers (9-byte keys, 8000-byte values) are `8040` B each.

1. `slotcask_test_set_seg_max_bytes(VARLEN_FIXTURE_TEST_SEG_BYTES)` BEFORE
   the open (and before every later reopen in this test); open `(db, tmpdir,
   8, 1, 8192)`.
2. Insert `keyX` (5000 B value) — LIVE for now (see step 7: deleting it
   early would push its 5032-byte slot into the free pool, and the next
   small insert would pop it and land ahead of the graves, defeating the
   scenario).
3. Insert `kA` (value "v") — live, ahead of the graves.
4. Insert `g1` and `g2` (8000 B values) — the future graves.
5. Insert `kkeyC` (value "cvalue") — live, BEHIND the graves.
6. Insert `f0-*` fillers until rotation into file 1 (loop on
   `db->streams[0].active_file_id`, exactly like the stock fixture), then
   fill file 1 until rotation into file 2 (the active segment; it holds only
   the rotation-triggering filler).
7. Delete: `keyX`, the `f0-*` fillers that landed in FILE 0 — NOT the last
   `f0-*` key (the insert that triggered the rotation lands at offset 0 of
   file 1, `append_reserve_single_varlen` rotates before reserving; deleting it
   would add a third free slot to the recipient and break the pin — an
   experiment on the plan's earlier "all f0-*" recipe showed
   `tombstones=3 live=62` in file 1, and with that key kept live
   `tombstones=2 live=63` before the flip) — and EXACTLY TWO `f1-*` keys — the FIRST
   (stays a flag=2 tombstone: one recipient slot) and the SECOND (becomes a
   recipient grave in step 8: the second slot) — plus `g1` and `g2`.
   After the flip in step 8 the recipient (file 1) has exactly two free
   slots, one tombstone and one grave (assert this by walking file 1's
   records before compaction: exactly one flag=2 and one flag=0 content record
   among the deleted keys), while the donor has two live records (`kA`, `kkeyC`) and
   `varlen_compact_cb` consumes one whole free slot per migrated record — so
   the migration succeeds only if the recipient walk harvests the grave.
   Deleting `g1`/`g2` before the flag flips keeps their kf entries
   tombstoned (no dangling live refs — the post-fix donor drop must not
   strand any). All deletes happen while the db is open.
8. Close. `pwrite` `0` at the flag bytes of `g1` and `g2` in the DONOR
   segment `<tmpdir>/data/streams/000/000000.dat` (use the stream id the
   keys actually landed in, `%03d`; both records live in file 0 — they were
   inserted before any rotation) and at the second deleted `f1-*` record's
   flag byte in `<tmpdir>/data/streams/<sid>/000001.dat`. Walk each file's records with the
   documented header layout (24 B header, klen at +16, flag at +18, vlen at
   +20, size `(24+klen+vlen+7)&~7`), assert the parsed key bytes equal
   "g1"/"g2"/the f1 key before flipping each — never hardcode blind
   offsets. These three graves are what step 9's reopen must tolerate and
   Tasks 3–4 must scan through.
9. Reopen (with the seg-max override still set); assert
   `slotcask_validate_live_refs(&db, &inv)` returns 0 with `inv == 0` (the
   fixture is graves-only where intended — no incident-state strays); run
   `slotcask_compact_segs(&db, &dropped)`; assert rc == 0.
10. Assert `kA` and `kkeyC` both read back their exact values, and
    `g1`/`g2`/`keyX` read back absent. Assert the donor segment file
    (`000000.dat` under the stream dir) no longer exists on disk, and
    `dropped` ≥ 1 (the donor was reclaimed — after migration it
    legitimately holds no live records). The recipient has exactly
    one tombstone slot and one grave slot for two migrated records, so
    migration can only succeed if the recipient walk harvests the grave:
    after Tasks 3–4 confirm this pin by temporarily making the recipient walk
    skip graves (do not harvest) and checking the test then FAILS (migration
    `rc == -1` or `dropped == 0`); restore afterwards. (On base this test
    already fails at `kkeyC` lost, so the pin is verified post-fix only.)

Lifecycle: wrap the whole case in a single
`slotcask_init(64, 64)` / `slotcask_shutdown()` pair, as the drafted file
already does (scenarios use fresh scratch dirs and the caches are
path-keyed).

On base: `seg_stat_one_varlen` stats the donor at `live_count == 1` (kA
only — the walk breaks at the graves), so no drop; `compact_migrate_
records_varlen` runs and its donor scan (`seg_scan_o_direct`) stops at the
graves having migrated only `kA`; the donor is dropped with `kkeyC` still
in it — `kkeyC` reads fail → test fails. After Tasks 3–4: both records
migrate; test passes. Run it on base before implementing Tasks 3–4 and
paste the failure.

SEPARATE case `test-od-scan-healthy-tail` (new file
`src/test/cases/test_od_scan_healthy_tail.c`, added to the `build.sh` link line
next to the other new cases; same `slotcask_init`/`slotcask_shutdown`
lifecycle and seg-max override reset as above) — keeping it apart keeps the
vacuum base proof clean. Sub-cases (i)–(v) all use Task 4a's
`od_test_resync_bytes_reset()` / `od_test_resync_bytes_read()`:

**(i) Healthy-tail bound (self-calibrating).** Open a fresh db in a NEW
scratch dir (seg-max override 512 KiB, `slot_size` 8192, 1 stream), insert
three small live records (100-byte values), close, then `stat` the segment
(`allocated = st_blocks * 512`) and call `seg_scan_o_direct` on
`<dir>/data/streams/000/000000.dat` (read its exact signature in
`src/db/io_direct.c`; the callback counts records and checks keys). Call
`od_test_resync_bytes_reset()` before the scan; assert the callback saw all
three records exactly once, the call returned success, and — only when
`od_seek_data_trusted(fd, path)` is true (open the file read-only in the test;
skip the bound with an explicit TAP skip otherwise) — that
`od_test_resync_bytes_read() <= allocated + 4 * 8192`. The bound adapts to
filesystems that allocate the whole 512 KiB (ext4 large folios, tmpfs large
folios) and still fails a wide search that reads through the hole when only
~8 KiB is allocated. **It PASSES on base** (HEAD's bounded search reads
`2 * slot_size = 16384` bytes) — a guard, not a base-failure proof. Prove it
can fail: once Task 4 is done, temporarily make `seg_resync_wide_fd` ignore
`SEEK_DATA` (`trusted = 0`), confirm the assertion fails on a filesystem
where `allocated` is small, then restore it.

**(ii) Scanner behind a hole.** Fresh scratch dir, seg-max override 512 KiB:
insert records A and B (small values), close; `pwrite` a byte-for-byte copy
of B's record (valid hash) at offset `256 KiB` (8-aligned, well past any
bounded window, behind a hole larger than 64 KiB); call
`seg_scan_o_direct` with a callback that counts deliveries per key; assert
it returns success and delivers A once and B twice (original + island
copy). On base the scan stops at the tail → B delivered once → FAILS; after
Task 4 passes. Also fails on a Task 4 that terminates on an extent-capped
short read.

**(iii) Grave larger than the scan chunk (resume-past-grave; livelock proof).**
Craft the file in the style of
`src/test/cases/test_coverity_seg_scan_varlen_overflow.c` (read it for the
chunk-size knob and crafting helpers). Set `odirect_buf_size` to its minimum
(1 MiB) and `slot_size` above the grave size so the grave passes `struct_ok`:
place one hash-valid flag=0 content grave of ~1.5 MiB (larger than the chunk),
then a live record after it. Assert the scan completes and delivers the live record exactly once. Run the
scan in a forked child with `alarm(10)`; the parent `waitpid`s and turns a
signal death into a normal FAILED assertion (a bare `alarm` in the test
process would kill the worker with no TAP result). **On base this case
PASSES** (the live record lies inside base's 2 MiB bounded window and the
grave is smaller than `slot_size`), so it is a guard, proven only by mutating
the rule: change the resume offset from `target + rec_size` to `target` and
confirm the child is killed by the alarm (livelock), then restore. Needs a
hash-valid record builder in the test: header = hash16 (`compute_hash_raw` of
the key) | klen u16 LE | flag u8 | hdr2 u8 (0) | vlen u32 LE, then key, then
value, padded to 8. Optional second variant to cover the carry path:
place the grave HEADER 8–16 bytes before a chunk boundary; it passes with or
without the rule (the restarted chunk begins at the grave and it fits), so it
is coverage, not proof. **Reset `odirect_buf_size` to its previous value at
test end** (`--jobs 1` shares the process).

**(iv) Extent larger than `buf_sz` (overlap rule).** Craft a file with a
desync (so the wide search is entered), then a real 3 MiB `pwrite` of ZEROS
(still one data extent on tmpfs/ext4/xfs/btrfs — non-zero filler would be a
non-padding desync and make the scan return `-EIO` after delivery), and place
a hash-valid live record so its header straddles the buffer end:
`off0 = (desync_offset + max_slot_size)` rounded to 8, `buf_sz =
max(2*max_slot_size, 1 MiB)`, record start at `off0 + buf_sz - 64`. Assert
it is delivered. Fails on base (record not delivered); prove the overlap rule
by changing `bytes_read - max_slot_size` to `bytes_read` and confirming the
record is missed. Reset any `odirect_buf_size` change at test end.

**(v) Grave ending exactly at EOF and hole-punched spanning record.** (a)
Craft a segment LARGER than `odirect_buf_size` (minimum chunk, 1 MiB; use a
2 MiB file) whose last record is a hash-valid grave ending exactly at
`file_size`, with the grave HEADER placed in the last 16–24 bytes before the
first chunk boundary so it takes the resync path (a small file ending in a
grave is handled by the in-chunk step and never reaches the resume logic):
assert the scan completes and delivers the live record before it (resume ==
`file_size` ends cleanly via `dbctx_init` with `remain == 0`); verified to
work in an experiment (the scan resyncs, then ends). Reset
`odirect_buf_size` at test end. (b) Linux only (explicit TAP skip elsewhere AND when
`fallocate(PUNCH_HOLE)` returns `EOPNOTSUPP`):
the scenario-9 record (key 16 B, 16000-byte value, zero middle pages
punched out with `fallocate(PUNCH_HOLE|KEEP_SIZE)`) placed behind a desync;
assert `seg_scan_o_direct` delivers it (the `max_slot_size` zero padding
makes `struct_ok` pass). Both pass on base only if reached by the bounded
window — place the targets > 2 MiB past the desync so base fails, and note in
the test which expectation each fails on base.

At test end: `slotcask_test_set_seg_max_bytes(0)` before
`slotcask_shutdown()` (sequential `--jobs 1` mode shares the process —
the override must not leak into other cases).

### Task 6 — Prove fail-on-base, full suite, sanitizer gate

1. `SKIP_TESTS=1 ./build.sh`.
2. Fail-on-base proof for the code changes:
   ```
   B="$(git rev-parse --git-dir)/plan-backup"; mkdir -p "$B/fixed"      # inside .git: not tmpfs, not tracked
   git diff src/db/slotcask.c src/db/io_direct.c src/db/io_direct.h src/db/seg_scan_varlen.h > "$B/fix.patch"
   cp src/db/slotcask.c src/db/io_direct.c src/db/io_direct.h src/db/seg_scan_varlen.h "$B/fixed/"
   git checkout -- src/db/slotcask.c src/db/seg_scan_varlen.h
   cp "$B/io_direct.c" "$B/io_direct.h" src/db/   # Task 4a state: HEAD logic + TEST_BUILD counter/accessors
   SKIP_TESTS=1 ./build.sh
   ./build/bin/shard-db-test run test-slotcask-grave-run-frontier   # expect: scenarios 1, 2, 3, 5, 6, 7, 9 fail; 4 and 8 pass
   ./build/bin/shard-db-test run test-slotcask-vacuum-grave-run     # expect: kkeyC lost
   ./build/bin/shard-db-test run test-od-scan-healthy-tail          # expect: (i) and (iii) PASS on base (guards); (ii), (iv), (v) fail (record behind the hole not delivered)
   cp "$B"/fixed/slotcask.c "$B"/fixed/io_direct.c "$B"/fixed/io_direct.h "$B"/fixed/seg_scan_varlen.h src/db/ && SKIP_TESTS=1 ./build.sh
   ```
   (slotcask.c / seg_scan_varlen.h are tracked and otherwise unmodified, so
   `git checkout --` restores HEAD's behaviour exactly; io_direct.c is
   restored to its Task 4a state (HEAD logic + TEST_BUILD counter) so the new
   tests still link — `io_direct.h` likewise; the untracked test files and
   the build.sh edit survive untouched.) Paste both
   runs.
3. `./build/bin/shard-db-test run-all --jobs 8` — all green, including
   `test-slotcask-active-recovery-resync`, `test-varlen-compact-stat-resync`,
   `test-compact-crash-recipient-sync`, `test-compact-crash-kf-repoint`, `test-o-direct-scan-bit-equality` and the three new cases
   (`test-slotcask-grave-run-frontier`, `test-slotcask-vacuum-grave-run`,
   `test-od-scan-healthy-tail`). Remove
   `/tmp/shard-db-*` afterwards.
4. Mutation proofs (each: edit, rebuild, run the named case, confirm the
   stated failure, restore; paste the failures): (a) resume offset
   `target + rec_size` → `target` ⇒ `test-od-scan-healthy-tail` (iii) killed by
   the alarm; (b) overlap `bytes_read - max_slot_size` → `bytes_read` ⇒ (iv)
   misses the record; (c) `trusted = 0` forced ⇒ (i)'s bound fails where the
   segment is sparse; (d) recipient walk skips graves ⇒
   `test-slotcask-vacuum-grave-run` fails (`rc == -1`, `dropped == 0`); (e)
   replace the `pread` search by a walk over the mapping ⇒ frontier scenario 8
   fails (≈ +128 MiB; demonstrable on tmpfs only — on ext4 mapped hole reads
   add no `st_blocks`, so scenario 8 is vacuous there, as are mutation (c)
   and case (i)'s bound where the filesystem allocates the whole segment;
   run those mutation proofs on tmpfs).
5. Sanitizer gate per the embedded rules (3× ASan+UBSan at `--jobs 6`, 3×
   TSan at `--jobs 3`); paste the tail of each run. Task 8 (version bump +
   test-pin edits) touches no concurrency or shared state, so the gate is NOT
   repeated after it; instead re-run the normal full suite once after Task 8.

### Task 7 — Docs

- `CHANGELOG.md`: add a bullet at the top of "Recent releases" pointing at
  the new release-notes file (match the existing bullet style — this file is
  an index, not per-release sections).
- `docs/reference/changelog.md`: new `## 2026.11.2` heading (style of the
  `## 2026.11.1` section): recovery/stat/compaction/O_DIRECT scans step
  over abandoned staged records; frontier/tail breaks require a
  verified-dead remainder; vacuum no longer drops or under-migrates
  segments whose records sit behind graves and reclaims grave space;
  reindex sees all records; one WARN log line per affected stream at open.
  No on-disk format change; wire-compatible.
- `docs/release-notes/2026.11.2.md`: short file mirroring the entry,
  linking this plan and naming the regression tests. Versions are decided
  (2026.11.2 / npm 1.5.0).
- `docs/operations/deployment.md` (next to the 2026.11.1 runbook section):
  new subsection "Refused: `invalid live kf references`" — what it means (a
  live keyfile entry whose segment record is missing/dead/another key's,
  detected by the 2026.11.x layout migration), why it can occur on roots
  written by 2026.10.2 or earlier (the frontier-scan bug fixed here; 2026.11.2
  prevents new damage but does not repair existing damage), and what to do
  today: restore the affected object from a pre-upgrade backup (or re-ingest
  from the source of truth) — the migration never modifies the data when it
  refuses. State plainly that no repair command ships in this release.
  Mirror a two-line pointer in `docs/release-notes/2026.11.2.md`.
- Release notes + `docs/reference/changelog.md` must state the cost model
  (Shared groundwork item 4): healthy open on a sparse-aware filesystem adds
  ~4 ms per 8-stream object; filesystems without a usable `SEEK_DATA` (or
  fully-allocated/plain-copied segments) pay one full-segment `pread` scan per
  stream per open. Also state the rollback barrier (Task 8).
- `docs/concepts/storage-model.md` (~lines 93–101, the resync paragraph that
  says the search is "bounded by the owning object's maximum slot size"):
  update it — startup recovery, vacuum stat/compaction and the O_DIRECT
  scanner now do a bounded search first and then a hole-aware search of the
  rest of the segment; staged flag=0 records (inert, or live-pending until
  marker replay) are stepped over by declared size and hash-verified.
- `AGENTS.md` "Crash safety" bullet: append one sentence — recovery, vacuum
  stat/compaction and the O_DIRECT scanner step over abandoned staged
  (flag=0 with content) records; tail breaks require a hole-aware
  full-remainder search to find no hash-valid record ahead.

### Task 8 — Version bump (2026.11.2) and npm binding 1.5.0

Precedent: commits `b1cc3b7`/`1e9e32d` (version.h bump; "npm: bump the
binding to 1.4.0 for shard-db 2026.11.1" touched `npm/package.json`,
`npm/package-lock.json`, `npm/README.md`). Numbers are decided (Brief
decision table): shard-db 2026.11.2, npm 1.5.0 (a minor bump for the binding).

- `src/db/version.h`: `SHARD_DB_VERSION` → `"2026.11.2"`.
  `SHARD_DB_REQUIRED_SOURCE_VERSION` stays `"2026.10.2"` (no new migration).
- Find every stale pin first: `grep -rn '2026\.11\.1' src` (expected hits:
  `version.h`, `test_version_gate_2026_11.c`, `test_layout_manifest.c` lines
  ~722 and ~840; `objlock.c` is a comment — leave it).
- `src/test/cases/test_version_gate_2026_11.c`: change the pinned
  `strcmp(SHARD_DB_VERSION, "2026.11.1")` to `"2026.11.2"` (message too) and
  flip the two decision cases whose meaning changes: `decide("2026.11.1", …)`
  becomes `SHARD_DB_VERSION_STAMP` (previous release opens and re-stamps) and
  `decide("2026.11.2", …)` becomes `SHARD_DB_VERSION_NOOP` (current release);
  add `decide("2026.11.3", …) == SHARD_DB_VERSION_DOWNGRADE` to keep the
  downgrade-barrier pin. (`shard_db_version_decide` already returns exactly
  these for those inputs; if running the case shows otherwise, STOP and write
  `PLAN_NOTES.md` — do not widen the gate.)
- `src/test/cases/test_layout_manifest.c` (~722, ~840): replace the literal
  `strncmp(ver, "2026.11.1", 9)` stamped-version checks with
  `strncmp(ver, SHARD_DB_VERSION, strlen(SHARD_DB_VERSION))` (add
  `#include "version.h"`) so they track the compiled version.
- `npm/package.json` and `npm/package-lock.json`: `1.4.0` → `1.5.0` — edit
  ONLY the root package's `version` fields (package-lock.json lines ~3 and
  ~9); the string `1.4.0` also appears for the unrelated `once` dependency
  there, so no blanket replace.
  `npm/README.md` (it names 1.3.0 and 2026.11.1 but not 1.4.0): append one
  short paragraph after the "Upgrading" paragraph: binding `1.5.0` embeds
  `2026.11.2` (fixes append-frontier recovery over abandoned staged records);
  roots written by 1.4.0 open normally; opening a root with 1.5.0 re-stamps
  it, after which 1.4.0 / 2026.11.1 refuse it — no rolling back.
- `CHANGELOG.md` bullet (Task 7) names "npm binding 1.5.0".
- Do NOT create tags or publish; tagging `2026.11.2` / `npm-v1.5.0` is the
  human's step after review.
- The docs lines that state 2026.11.1 as the introducing/current requirement
  (`docs/getting-started/install.md` ~87, `docs/operations/deployment.md`
  ~263, `docs/query-protocol/schema-mutations.md` ~213) are intentionally
  left unchanged: they describe the 2026.11.1 upgrade, which remains true.
- After this task run `test-version-gate-2026-11`, the
  `test_layout_manifest.c` cases and then the normal full suite once
  (`--jobs 8`); no sanitizer re-run (see Task 6 step 5).
- `README.md` line ~132 ("In 2026.11.1 a populated root must carry …"): keep
  accurate — extend it to "2026.11.1 and later (the 2026.11.x gate is
  unchanged)" rather than leaving a version-pinned sentence.

### Task 9 — Free pool must drop slots consumed by compaction (M3)

Root cause (reproduced): at open the free pool is rebuilt from the recipient
segment's kf tombstones; `varlen_compact_cb` migrates a donor record into such
a tombstone slot but never removes the slot from the pool, so the next insert
pops it and overwrites the migrated record.
- Test first: `src/test/cases/test_slotcask_compact_pool_consumed.c`
  (`test-slotcask-compact-pool-consumed`, add to `build.sh`): stock
  `varlen_compact_fixture_build`, reopen, compact, 12 inserts; assert `kA`,
  `kkeyC` and the new keys read back and `slotcask_validate_live_refs` == 0.
  Must fail on base (`kA` overwritten, validate == 1).
- Fix (`slotcask.c`): new `pool_drop_range(stream, fid, off, len)` removing any
  pool entry overlapping the consumed slot; `varlen_compact_cb` calls it before
  writing the migrated record (vacuum holds the exclusive object lock). A
  reopen cannot resurrect the slot (rebuild already checks the record hash).
- Mutation proof: remove the call, test fails with validate == 1.
- Add a post-compaction `validate_live_refs` == 0 assertion to Task 5's fixture.

### Task 10 — Wide resync fails closed (M2)

- `seg_resync_graves_wide` returns -1 with `errno` set when `open`/`fstat`
  fails or the search hits an I/O error; the mapped full-scan fallback is
  deleted. `recover_one_stream` releases its handle and returns -1 with that
  errno; `seg_stat_one_varlen` and the recipient free-list walk in
  `compact_migrate_records_varlen` also return -1 (their `break` only on a
  clean "nothing found").
- Test seam (`TEST_BUILD`, declared in `slotcask.h`):
  `slotcask_test_fail_wide_resync_opens(int)`. Frontier scenario 10: with the
  injection on, `slotcask_open` fails and the segment allocation does not grow
  (st_blocks); after clearing, reopen gives `reserve_off` 216 and 0 invalid
  refs. Mutation: restore the mapped fallback, scenario 10 fails.

### Task 11 — Sparse-preserving backup/restore copy (M1)

`copy_file` (`query_maint.c`, used by `backup`/`restore`) wrote every byte, so
restored segments were fully allocated and the wide search then costs ~155 ms
per 8-stream object (measured, warm cache) vs ~4 ms sparse.
- New `copy_pwrite_all`, `copy_file_sparse(sfd, dfd, src, size)`: walk the
  source with `lseek(SEEK_DATA/SEEK_HOLE)`, copy only data extents, finish with
  `ftruncate(dfd, size)`. Gate on `od_seek_data_trusted` (so a dirty unflushed
  page is never skipped); if untrusted/unusable before copying starts, fall
  back to the existing dense loop. Log failures.
- Test seam `maint_test_copy_file` (`query_internal.h`, `TEST_BUILD`). Test
  `test-backup-copy-sparse`: 128 MiB sparse file (data at 0, 64 MiB+100, last
  byte) and a 16 MiB all-hole file; assert byte-identical, same size, and
  allocation stays sparse when `SEEK_DATA` is trusted. Mutation: disable the
  sparse path, allocation assertions fail.
- Measured open per 8-stream object: new copy 3.8 ms ext4 / 4.2 ms tmpfs; old
  dense copy 155 ms / 553 ms. Backups taken by earlier releases and plain
  `cp` copies stay dense (slow open ~155 ms/object) until re-copied sparsely.
- Docs: release notes + changelog mention the pool fix, sparse backup and the
  cost note; restore guidance says to restore a backup taken BEFORE the damage;
  README line ~132 no longer says "stamped 2026.11.1".

Execution order: Tasks 1, 4a, 5, 9 (tests first) -> 2, 3, 4, 10, 11 -> 6 -> 7 -> 8.
Sanitizer gate (Task 6) also covers Tasks 9-11 (shared state: free pool).

### Completion checklist (per CORE-PROCESS "Definition of done")

- Full suite green, fresh; sanitizer runs pasted (3× each).
- No new compiler warnings (`-Wall -Wextra` build output checked).
- No leftover debug prints or TODOs from this work.
- Diff contains only: the four scan fixes + shared inline + helpers
  (`seg_resync_graves_wide`, `seg_resync_wide_fd`, `od_seek_data_trusted`) + log line, the two regression
  test cases + their `build.sh` entries, the version bump (`version.h`, the
  version-gate test, `test_layout_manifest.c`, `README.md`, `npm/` files) and the doc files (changelogs, release
  notes, `deployment.md`, `AGENTS.md`).
