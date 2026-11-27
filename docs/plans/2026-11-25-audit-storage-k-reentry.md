# Audit — Phase 2 area 1 (storage & durability), part 1: K-phase re-entry (2026-11-25)

Scope of this part: the two items carried from the AUD-QRY-11 blind review
(AUD-BULK-DEL-K, AUD-BULK-DEFER-CONT). Read-only; no product-source edits.
The rest of area 1 is **not started** (see "Not covered").

## DL pass — bulk-delete window, K failure then inline replay

State after a kill or failure at each step of `bulk_commit_one_kf_window`
(legacy path) for a delete window:

| Failure point | Durable state | Recovery | Holds? |
|---|---|---|---|
| pre-M (plan/publish fails) | nothing published | `abort_window` | yes |
| after M, before K | marker + live kf slots + live segment records | gate/startup replay: `kf_marker_replay_delete_entry_locked` reads old live, verifies kf slot, tombstones kf then segment | yes |
| K applied in mmap, msync fails | marker; kf slots tombstoned in mapping (maybe on disk); segment records still live | inline replay (see below) fails; gate replay hits the "Gap B" branch: `kf_marker_verify_kf_old_at_slot` fails because slot flag==2, completes the segment tombstone, clears marker | yes |
| K durable, T (segment tombstone) fails or kill | kf slots tombstoned, segment live | same Gap B branch | yes |
| T durable, clear fails | both tombstoned | `read_marker_old_live` fails, `marker_record_tombstoned==1` and kf slot flag==2 => rc 0, marker cleared | yes |

Counters: `kf_tombstone_at_slot` runs once per slot (inline replay never
reaches a second call, see AUD-BULK-DEL-K); recovery uses the same function
guarded by the flag check. No double-count path found.

## AUD-BULK-DEL-K — confirmed, not DL

`bulk_apply_and_sync_kf_locked` re-enters on the same plan during the inline
forward replay. For a delete entry, `kf_tombstone_entry_locked` finds no
`flag==1` match (the first K already tombstoned it) and returns -1, so every
inline replay of a delete window whose first K failed after tombstoning
fails. The caller then returns rc -2 / EINPROGRESS with
`out_durability_degraded` set and leaves the marker for gate/startup replay,
which converges (table above).

- Effect: a transient msync/EIO on K for a delete window always degrades to
  "pending" instead of converging inline. Data and counters stay correct;
  the cost is a spurious degraded status and an extra gate-replay round.
- Doc/comment bug: the comment on `kf_tombstone_entry_locked` says re-running
  is "a safe no-op"; the function returns -1 on a re-run. Fix the comment or
  the behavior.
- Phase 4 fix: in the delete branch, treat "key's slot at `s->old_kf_slot`
  already flag==2 and its stored key matches" as done (set `out_slot`, keep
  `kf_header_changed`). Test: delete variant of
  `test-kf-window-replay-double-booking` (K fault once, expect rc 0 and
  exact `total`/`deleted`).

## AUD-BULK-DEFER-CONT — traced, correct by construction, untested

`slotcask_bulk_finalize_shard` retries a failed window once, then continues
with later windows (`rs->failed` is written but never read). Trace:

- `rw->converged` is set only at the end of a successful
  `bulk_finalize_one_kf_window`.
- `bulk_commit_chain_flush_commit` indexes/clears only `converged` windows;
  a failed window keeps its marker and is not `cleared`.
- `bulk_commit_chain_fold_member` sets `retained` for any
  `published && !cleared` window and returns rc -2 + degraded flag.
- The next request on that kf shard runs `kf_shard_marker_gate` first, which
  replays the retained marker before any new planning.

So a failed window is retained and replayed, and later windows commit
independently. Open question to verify with a test, not assumed: whether two
windows of one request can name the same key (both planned before either
finalizes, so the later plan would not see the earlier window's effect). The
expectation is that request-level dedupe prevents it; confirm in the bulk
slice with a duplicate-key request spanning a window boundary plus a K fault
on the first window.

`rs->failed` being write-only is dead state (AUD-REDUCE-1 candidate).

## Part 2 — kf growth/resplit and bitmap remap (`kfcache_resplit_locked`)

Read: `kfcache_resplit_locked`, `kf_bmfix_prepare/commit/finish/abort`,
`kf_remap_slot`, `bmfix_recover_all` (embedded.c), `slotcask_compact_kf`,
and the three growth callers (`kf_plan_insert_slot`, the bulk stage pre-grow,
`slotcask_pregrow_worker`).

### DL pass — kill at each resplit step

| Step | Durable state after a kill | Recovery | Holds? |
|---|---|---|---|
| A/B (new `.new` file being built) | old kf untouched | startup sweeps `*.new` | yes |
| C2 prepare (bitmap `.remap` images) | old kf + old bitmaps; `.remap` temps, maybe no marker | `slotcask_bmfix_sweep` removes temps | yes |
| marker durable, before rename | old kf, old bitmaps, marker | `bmfix_recover_all` rebuilds the named shard's bitmaps from the current kf + segments | yes |
| after rename, before/while bitmaps copied in place | new kf, partly remapped bitmaps, marker | same rebuild, from whichever kf is on disk | yes |
| rename not durable (dir fsync fails) | either kf may return; marker retained (`kf_dir_synced` forces `bmfix_rc=-1`) | rebuild from whichever kf survived | yes for bitmaps (see AUD-KF-RESPLIT-SYNC for the kf itself) |
| bitmap msync unconfirmed | marker kept until a later rebuild or startup | `test_bitmap_resplit_fault_commit_*` | yes |
| prepare failure | whole resplit abandoned, 5 s back-off; bulk stage fails closed (`BULK_STAGE_UNGROWN`) | client retries | yes |

Marker lifecycle is sound: a pre-existing marker or a skipped corrupt bitmap
is never removed by a later rebuild; startup repair failure escalates to the
`.bmfix-disabled` read-only state unless KFM2 markers are pending, in which
case startup refuses (correct: replay would write the torn bitmap).

Existing drills cover prepare/rename/commit-sync faults, restart, marker
recovery, corrupt bitmaps, chains and concurrent readers
(`test_bitmap_resplit.c`, 15+ cases). Gap: nothing covers the keyfile's own
sync (below).

### AUD-KF-RESPLIT-SYNC (suspected major, Phase 4; needs a fault seam to repro)

Phase C of `kfcache_resplit_locked` calls `msync(new_base, new_size, MS_SYNC)`
and ignores the result, then renames the new file over the live keyfile. The
parent-dir `fsync` after the rename is checked only to protect the *bitmaps*
(`kf_dir_synced`); for a keyfile with no bitmap fields, or for the keyfile
itself, a failed dir fsync is silent too. Failure modes:

- msync EIO: the new keyfile is published although its content may not be
  durable. After a later crash/power loss the rebuilt entries (every live
  record of the shard) can read back empty or stale; acked records then
  exist only in segments with no kf entry. Later per-slot msyncs may report
  success for pages the kernel already dropped after the first writeback
  error.
- dir fsync failure: the rename can revert on power loss; records acked
  after the resplit lived only in the new inode.

Both need a disk fault plus a crash, so rare, but the project's contract is
that I/O errors fail the operation rather than publish unsynced state (every
other sync in this path is checked). Fix: check the msync (abort the resplit
as the rename-failure branch does, with the same back-off) and treat a failed
kf-dir fsync as a failed resplit step (retain the old mapping or surface
`EINPROGRESS`). Test: add a `SHARD_TEST_BMFIX_*`-style seam for the Phase C
msync and the post-rename dir fsync; assert the resplit is abandoned with the
old kf intact and no acked record is lost across a simulated restart. Needs
a second reviewer to confirm severity before Phase 4.

**Repro (2026-11-25): confirmed red.** `audit-repro-resplit-msync`
(`src/test/audit/audit_repro_resplit_msync.c`, run with `--include-audit`)
fails the first rebuilt-keyfile msync with EIO via a test-binary `msync`
interposer. Result on the current tree: the fault is hit exactly once, the
resplit proceeds anyway (keyfile 6168 -> 12312 bytes), and the triggering
insert is acknowledged. The case asserts the contract (resplit abandoned,
old keyfile intact) and passes once the Phase 4 fix lands. Limit: the
interposer simulates the failed call; it does not simulate the kernel losing
the dirty pages, so it proves the engine ignores the error, not an
end-to-end record loss. Severity stays "suspected major" until the second
reviewer weighs that.

### Other observations (no action)

- Phase B drops every `flag!=1` entry: tombstones (intended) and abandoned
  staged `flag==0` records (matches the recovery rule that staged records
  are never adopted). Planned-but-uncommitted slots cannot exist during a
  resplit: bulk pre-grow runs under the shard writer gate before planning,
  and planning refuses to resplit once a window holds planned slots (F2
  comment); `BULK_STAGE_UNGROWN` fails the stage closed otherwise.
- `slotcask_compact_kf` stops at the first failing shard and returns -1;
  earlier shards stay compacted (idempotent, safe).
- Resplit fits the documented lock order: kf wrlock held, bitmap writers
  taken in canonical path order.

## Part 3 — KFM2 markers and replay; vacuum with markers pending

Read: `marker_publish_file_atomic`, `kfm2_read_batch_marker`,
`kf_batch_marker_gate_refs` / `kf_shard_marker_gate`,
`marker_recovery_sweep_object`, `object_has_pending_markers`,
`kf_marker_replay_{entry,upsert_entry,delete_entry}_locked`,
`recover_one_stream`, and the vacuum entry points (`cmd_vacuum`,
`slotcask_compact_segs_ex`, `slotcask_compact_kf`).

### Sound

- Publication is tmp-write + fsync + `link()` (no replace) + unlink; the
  final name exists in full or not at all. Names carry pid/nonce, so two
  windows never collide; recovery removes recognised temporaries and fails
  closed on any other file in the marker namespace.
- V2 format: 16 B header + `count` x 32 B slots, exact-size validated,
  `1 <= count <= 16384`, per-slot XXH32 checksum, `O_NOFOLLOW` open, V1
  refused with ENOTSUP. Torn or short files cannot parse.
- Every writer replays retained markers before planning (gate); startup
  replays before `slotcask_open`; replay is forward-only and re-derives the
  record bytes from the segment, with explicit handling for the already-
  advanced states (flag==0 activation, "Gap B" delete, "Gap C" update).
- `recover_one_stream` steps over hash-valid abandoned staged records and
  fails closed on desync (covered by the 2026.11.2 grave-frontier cases).

### AUD-VAC-KF-MARKER (DL, confirmed by repro; fix needs a plan + approval)

`cmd_vacuum`'s light path calls `slotcask_compact_segs_ex`, which skips when
KFM2 markers are pending, and then calls `slotcask_compact_kf`
unconditionally. `slotcask_compact_kf` rebuilds every shard with
`deleted > 0`, which moves live entries to new slot numbers. A retained
update marker names its kf slot explicitly, and
`kf_marker_replay_upsert_entry_locked` repoints `marker->kf_slot` without
verifying which key sits there (only the delete replay verifies). Vacuum
does not go through the writer gate or the marker sweep, so the stale slot
is used by the next gate or startup replay. A retained marker exists after
any rc -2 / EINPROGRESS window (a failed K/A/T sync or kf-dir fsync) and
persists until the next write to that shard or a restart; the nightly
auto-vacuum can hit that interval.

Repro `audit-repro-vacuum-marker` (`--include-audit`), red on the current
tree: 3 colliding keys, two deleted, an update of the third fails at the
activate phase (marker retained), `slotcask_compact_kf` runs, the next clean
write replays. Result: the updated key becomes **unreadable** (slotcask_get
fails), the checked walk returns EUCLEAN (errno 117) so scans of the shard
fail, and only 1 of 2 live keys is visible. Cause: replay wrote a flag==1
entry with a zero hash into the now-empty old slot and tombstoned the OLD
segment record that the real (moved) entry still points to.

- Callers: `cmd_vacuum` light path (manual and nightly auto-vacuum);
  auto-reshard and the heavy path go through `rebuild_object`.
- Unverified sibling: `rebuild_object_v2` (heavy `vacuum --compact/--splits`,
  `edit-field`) has no marker handling in `query_find.c`; whether pending
  markers survive, are dropped, or lose a flag==0 pending payload there
  needs its own check (maintenance slice 10).
- Proposed fix (Phase 4, one concern per branch): (a) make
  `slotcask_compact_kf` refuse/skip while `object_has_pending_markers`
  (mirrors the segment-compaction guard, covers every caller) and report
  `kf_skipped` like `segments_skipped`; (b) defense in depth: verify the
  slot's key in the update/insert replay as the delete replay already does.
  Tests: the repro turns green; add a vacuum-level case through the wire.
- Severity: DL (acknowledged-pending write destroys the prior value of an
  existing key and wedges scans), conditional on a prior I/O fault. DL is
  always fixed first; the plan awaits your approval per CORE-PROCESS.

### AUD-REPLAY-BOUNDS (minor hardening)

`seg_peek_flag`, `marker_record_tombstoned` and `seg_write_flag_durable` index
`h.map[offset + 18]` without checking `offset + 19 <= map_size`. Marker slots
are checksum-protected and written by the engine, so only a checksum-valid
but semantically bad marker (XXH32 collision, hand-edited file with a
recomputed checksum) reaches it; the effect would be a read or write past
the mapping. Add the bounds check; no behavior change for valid markers.

## Part 4 — segments/free pool, backup/restore, startup gate, crash-point table

### Segment rotation, free pool, segment compaction

Read: `append_reserve_single_varlen`, `pool_*` helpers,
`slotcask_pool_rebuild_worker`, `compact_one_stream`,
`compact_migrate_records_varlen`, `varlen_compact_cb`, `compact_drop_seg_file`.

Sound: rotation is under `rotation_lock`, offsets/file ids are bounded
(`EFBIG` past 65535 files; fail closed); the free pool is rebuilt from
hash-verified flag==2 kf entries at open and skips missing files; pool
entries are dropped before a migrated record lands and when a file is
dropped; donors are only deleted after every live record was repointed
(`kf_failed` blocks the drop); the recipient copy is msynced before the
repoint; segment compaction already refuses while markers are pending.

#### AUD-COMPACT-ORDER (suspected major, power-loss only)

`varlen_compact_cb` repoints the kf entry in the mapping
(`kf_repoint_at_slot`, no sync) and `compact_one_stream` then calls
`compact_drop_seg_file`, which unlinks the donor file and fsyncs the
directory. Nothing makes the kf repoint durable before the donor
disappears. Process kill is safe (the mapping is in the page cache); a
power loss or kernel crash after the unlink but before kf writeback leaves
the on-disk kf pointing into a deleted file: those keys become unreadable,
and the migrated copies sit unreferenced. The order the rest of the engine
uses is payload sync, then kf sync, then destructive step. `cmd_vacuum` runs
`slotcask_compact_kf` afterwards (a full-file msync for shards with
deleted>0), but only after the donor is already gone, and not for shards
with no tombstones. Existing drill (`test_varlen_compact_crash_mid_migration`)
kills the process at pause points; it cannot see this. Fix: msync the
affected kf shards (and fsync the recipient) before `compact_drop_seg_file`;
test by an ordering oracle (interposer recording kf sync vs donor unlink).
Also minor: the directory fsync result in `compact_drop_seg_file` is ignored.

### Backup and restore (`cmd_backup`, `cmd_restore`, `cprf`, `copy_file`)

Sound: backup takes the exclusive object lock (`test-backup-fences-writers`),
copies data/indexes/metadata/files/tokens/fields plus `object.json`, sparse
copy honours dirty mmap pages (`test-backup-copy-sparse`); restore validates
`from`, refuses on non-empty live state / layout or schema mismatch unless
`force`, merges tokens, drops caches and the slotcask registry first.

#### AUD-BACKUP-SILENT (major; proof trace, no repro yet)

- `cprf` returns `void` and `copy_file` results are discarded at every call
  site, so any failure (ENOSPC, EIO, EMFILE, unreadable file, `mkdirp`
  failure) is swallowed. `cmd_backup` then prints `{"status":"backed_up"}`.
  The backup is the last line of defence against data loss, and it can
  silently be partial or empty.
- Nothing is fsynced (copied files, directories, `object.json`), yet the
  status says done; a power loss right after leaves a truncated backup.
- `cmd_restore` does `rmrf(live subtree)` first, then `cprf` from the
  backup, ignoring copy errors, and always prints `{"status":"restored"}`.
  A copy failure after the wipe leaves a partial or empty object reported as
  restored; a crash mid-copy leaves a half-restored object (the backup stays
  intact, so re-running restore recovers it, but nothing tells the operator).
  There is no stage-then-swap.
- Minor: backup directory names have one-second resolution, so two backups in
  the same second merge into one directory.
- Fix (Phase 4): propagate errors from `cprf`/`copy_file` and fail the mode
  with `{"error":...}`; fsync files and directories before reporting
  success; restore copies into a staging dir and swaps (or at least refuses
  to report success on any error); unique backup names. Test: ENOSPC/EIO via
  the audit faultlib on a backup and a restore.

### Startup version gate and layout migration (`embedded.c`)

Read at design level: audit-then-stamp-then-convert-then-stamp ordering
(`layout_migration_run`), stamp via `durability_publish_replace`
(fsync(tmp), rename, fsync(dir)), refusal if the previous run was not clean
or markers are pending (D7), whole-root audit before any change (D5), crash
points exercised by `layout_test_crash_point` (`test_layout_manifest.c`,
`test_rebuild_txn_recovery.c`, `test_version_*`). No defect found; deeper
line-level review is not claimed.

### Crash-point table (multi-step mutations vs drills)

| Mutation | Steps | Covered by | Gap |
|---|---|---|---|
| Insert/update/bulk window | P, M, A, I, K, T, C | `test_marker_v2`, window route/gate cases, `test_abort_pre_marker_holes`, crash/restart oracle | delete K re-entry (AUD-BULK-DEL-K); K replay counters (fixed, AUD-QRY-11) |
| Delete window | M, K tombstone, T, C | same, Gap B/C cases | inline replay never converges (AUD-BULK-DEL-K) |
| kf resplit / bitmap remap | .new, C2 prepare, marker, rename, commit, finish | `test_bitmap_resplit.c` (15+) | keyfile msync/dir fsync (AUD-KF-RESPLIT-SYNC) |
| kf compaction (same-cap resplit) | as resplit | `test_bitmap_resplit_vacuum` | pending-marker interaction (AUD-VAC-KF-MARKER) |
| Segment compaction | copy, repoint, drop donor | `test_varlen_compact_crash_mid_migration` | power-loss ordering (AUD-COMPACT-ORDER) |
| Heavy vacuum / edit-field / reshard (`rebuild_object`) | rebuild txn | `test_rebuild_txn_recovery`, `test_rebuild_recovery` | pending markers unverified (maintenance slice) |
| Backup / restore | copy, wipe+copy | happy-path and fencing cases | copy failure and crash mid-restore (AUD-BACKUP-SILENT) |
| Layout migration / version stamp | audit, stamp, convert, stamp | `test_layout_manifest`, version cases | none found |
| truncate, drop-object, create-object, schema mutations | per mode | per-mode slices | covered in Phase 3 |

## Not covered (area 1)

Line-by-line review of `rebuild_object_v2` (maintenance slice 10); the
background auto-vacuum/auto-reshard schedulers (slice 13); the bitmap and
btree files themselves (`bitmap.c`, `btree.c`) beyond the remap/recovery
paths; kfcache/segcache eviction internals.
