# Handover: dangling live kf references (investigation only)

Status: investigation, no code changes made. Per CORE-PROCESS.md, produce a
plan (Brief + Plan) for human approval before any fix. Work on a `fix/` branch.

## Symptom

Layout migration (2026.11.1) refused `game/players` in a downstream app
(`../springandstory`, embedded npm binding, data last written by 2026.10.2 /
binding 1.3.0): `invalid live kf references in players`
(`slotcask_validate_live_refs`, `src/db/slotcask.c`, called from the layout
migration in `src/db/query_find.c`). 2 of 7 live kf entries pointed at segment
records that are tombstoned (flag 0) and belong to a *different* key hash.
The user performed no deletes. The data was repaired by hand (kf entries set to
flag 2, shard header `deleted` +1); the pre-fix copy is at
`/tmp/springandstory-data-pre-fix` (tmpfs, may be gone).

## Evidence (players, stream 3, `streams/003/000000.dat`, slot_size 120)

- 59 records. Key `7a1afd29…` has 34 versions at offsets 0..2376 (72 B each),
  contiguous, timestamps strictly increasing with offset, all from today; only
  the last is live (flag 1), one is flag 2 (offset 2304), the rest flag 0.
- Key `bea75c84…` has 25 older versions at 2448..4176, **all flag 0**.
- Live kf entries: `bea75c84…` → offset 432 and `09a5d3c4…` → offset 1224.
  Both offsets hold old `7a1a…` versions (flag 0). `09a5…` has no record in the
  segment at all. So today's appends from offset 0 appear to have overwritten
  space that held these two keys' live records.

## Leading hypothesis (NOT reproduced)

`recover_one_stream` (`src/db/slotcask.c`) sets each stream's `reserve_off` at
open by scanning the last segment. On a dead (flag 0) record it calls
`seg_scan_varlen_resync(..., window = slot_size)` (`src/db/seg_scan_varlen.h`),
which looks only `slot_size` bytes ahead for a live record header; if none, the
`valid && flag==0` branch `break`s as "ordinary unwritten tail". Two or more
consecutive dead 72-byte records (144 B > 120 B window) before the next live
record therefore end the scan early, `reserve_off` lands inside the file, and
later appends overwrite live records beyond the dead run. Nothing after the scan
reconciles `reserve_off` with live kf entries. Identical logic in tag
`2026.10.2` and HEAD, so it is probably *not* already fixed (confirm with
`git log -S` on that function).

## Why it is unproven

- A fresh-object repro (single insert/update of one key, restart, 60 inserts)
  did not corrupt anything: repeated updates flip-flop between two slots
  (flag 2 old / flag 1 new), so no long dead run forms.
- Unexplained: what write path produced 34 consecutive flag-0 versions of one
  key (and 25 for another). Likely a different path than single insert/update:
  the app uses the bulk/sync path (`insertEventRows`, UPDATE players on every
  sync, UPDATE skill_progress/activity_completions). Look at bulk upsert /
  `slotcask_upsert_with_hooks`, the free pool (`pool_push_free_cap`, pool is
  rebuilt at open only from kf flag-2 entries), and why old versions get flag 0
  rather than flag 2 and are not reused.
- Not crash-related: `players` has no indexes, so there are no KFM2 markers
  (0 found); crash replay would not apply. Disk-full and dev-server kills were
  ruled out as the user's app was not running then.

## Tasks

1. Write a failing regression test (`src/test/cases/`): segment with a dead run
   longer than `slot_size` followed by a live record; reopen; append; assert the
   live record is intact and `slotcask_validate_live_refs` returns 0. If it
   passes on HEAD the hypothesis is wrong; say so.
2. Find the source of the long dead-record runs; try to reproduce through the
   bulk-insert / upsert / sync-like workload (many updates of one key, several
   keys sharing a stream, restart between rounds) on HEAD.
3. Check whether any fix since 2026.10.2 touches this (`git log -S`/blame).
4. Decide whether 2026.11.1 should refuse-and-explain, auto-repair dangling
   refs, or fix the frontier scan; flag any behaviour/format change as breaking
   per CORE-PROCESS.md.
5. Write `docs/plans/<date>-<slug>.md` (Brief + Plan) and run the blind-review
   loop before presenting.

## Constraints

- Build `SKIP_TESTS=1 ./build.sh`; tests `./build/bin/shard-db-test run-all --jobs 8`;
  `rm -rf /tmp/shard-db-*` after runs (tmpfs).
- If the diff touches shared state/background threads, run ASan+UBSan and TSan
  per AGENTS.md before pushing.
- Do not commit/push/merge; the human does git writes. Don't run benches.
- A throwaway repro must run from a scratch dir with its own `db.env` (the repo
  `db.env` overrides `DB_ROOT`/`PORT`).
