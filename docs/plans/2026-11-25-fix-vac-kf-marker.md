# Fix plan 1 — vacuum must not compact keyfiles while commit markers are pending (AUD-VAC-KF-MARKER)

## Brief
**Problem and root cause.** `cmd_vacuum`'s light path skips segment compaction when KFM2 markers are pending, then
calls `slotcask_compact_kf`, which rebuilds each shard with `deleted>0` and moves live entries to new slot numbers.
A retained update marker names its kf slot, and `kf_marker_replay_upsert_entry_locked` repoints that slot without
checking which key sits there, so replay corrupts a different slot and tombstones the real record. Repro
`audit-repro-vacuum-marker` is red (updated key unreadable, checked walk EUCLEAN).

**Logical flow.**
1. `slotcask_compact_kf` first calls `object_has_pending_markers(db->data_dir)`; if markers exist (or the scan fails) it
   compacts nothing and reports "skipped" through a new out parameter.
2. `cmd_vacuum` reports `kf_skipped` in its JSON like `segments_skipped`.
3. Defense in depth: the update and insert replay verify the slot's key (as the delete replay already does via
   `kf_marker_verify_kf_old_at_slot`) before repointing/inserting; mismatch fails closed.
4. Replay segment-flag helpers (`seg_peek_flag`, `marker_record_tombstoned`, `seg_write_flag_durable`) bounds-check
   `offset + 19 <= map_size` (AUD-REPLAY-BOUNDS).

**Decisions.**
| Decision | Alternatives | Why | Tag |
|---|---|---|---|
| Skip kf compaction while markers are pending (mirror the segment guard) | replay markers inside vacuum first | smallest, symmetrical, no new recovery path in vacuum | DEFAULT |
| Also verify slot key in update replay | rely on the guard alone | marker slot numbers must never be trusted blindly | DEFAULT |
| Output field `kf_skipped` | reuse `segments_skipped` | distinct, additive JSON field | DEFAULT |

**Blast radius.** `slotcask_compact_kf` callers: `cmd_vacuum` only (query_maint.c) plus tests. Signature gains an
optional out param (internal). Response JSON gains an additive field. Not breaking.

**Tests.** `audit-repro-vacuum-marker` goes green and is promoted to a normal case (`TEST_REGISTER`); new case:
compaction runs normally with no markers (deleted counter back to 0); replay with a mismatching slot key fails closed
(unit via `kf_batch_marker_corrupt_first_kf_slot_for_test` style seam or a crafted marker); bounds case for an
out-of-range offset.

**Out of scope.** `rebuild_object_v2` marker handling (maintenance slice).

**Review log.** none yet.

## Plan (executor)
Branch `fix/vac-kf-marker` off the default branch. Build `SKIP_TESTS=1 ./build.sh`; test `./build/bin/shard-db-test`.
1. Test first: confirm `audit-repro-vacuum-marker` red; add the no-marker compaction case and the two replay-hardening
   cases (red before step 2 where applicable).
2. `slotcask.c`: `slotcask_compact_kf(SlotcaskDb*, int *out_skipped)` — early return 0 with `*out_skipped=1` when
   `object_has_pending_markers != 0` (negative = unreadable: also skip, warn like `slotcask_compact_segs_ex`).
   Update `slotcask.h` and the call in `cmd_vacuum` (`query_maint.c`): include `"kf_skipped":true` in the status JSON.
3. `slotcask.c`: in `kf_marker_replay_upsert_entry_locked` update branch, before `kf_repoint_at_slot`, verify via
   `kf_marker_verify_kf_old_at_slot` (flag==1, same hash/key); on mismatch `errno=EILSEQ`, fail. Insert branch with an
   explicit slot already requires flag!=1; keep.
4. Bounds helper `seg_offset_in_map(h, offset)` used by the three flag helpers; return -1/EILSEQ when out of range.
5. Revert-prove: temporarily disable step 2, show the repro red, restore, show green; paste both outputs.
6. Full suite `run-all --jobs 8`; docs: changelog entry, `docs/query-protocol` vacuum response note; leave uncommitted.
Halt rule: if an assumption about the code is wrong, write `PLAN_NOTES.md` and stop.
