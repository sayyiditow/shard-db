# Audit — Phase 2 areas 5 and 6: lock order, criteria compiler and planner (2026-11-25)

Read-only. These two areas were already covered by Phase 1 dynamic evidence;
this note records what that evidence establishes, what it does not, and where
the per-mode slices must look again.

## Area 5 — lock order

Documented hierarchy (`docs/concepts/concurrency.md`): objlock (per object) ->
writer gate / kfcache entry (per kf shard) -> bitmap cache entries (canonical
ascending path order) -> bt_cache entries -> segcache; kfcache before bitmap
always; ordered-index callbacks use `read_record_ref_try` before any blocking
record fetch.

Evidence that holds:
- TSan gate: 3 consecutive full-suite runs clean at the last gate (before this
  campaign's later edits; one more gate is scheduled before the push).
- Targeted deadlock cases: `test_bitmap_resplit_concurrent_readers`,
  `test_bt_kf_inversion_stream_find`, `test_backup_fences_writers`,
  `test_count_bitmap_bulk_lock_order`, `test_bitmap_stream_find_flush_gate`.
- This campaign's resplit review confirmed the kf -> bitmap (canonical order)
  sequence in `kfcache_resplit_locked`/`kf_bmfix_prepare`.

Known limits (documented, not defects): a pthread rwlock self-upgrade (read
hold then write) blocks without an error; `EDEADLK` detection is best effort.
The text-path `restore` special case in `server_process_fast` exists to avoid
exactly that self-upgrade.

Gaps for the per-mode slices (checklist item 7):
- every new multi-lock holder in a mode's code path must be mapped to the
  hierarchy; the slice doc records the lock sequence per mode;
- the objlock for NQL reads was added late (use-after-free fix); confirm it
  for any other grammar entry added later;
- the `.bmfix-disabled` and marker-gate checks happen before locks in some
  paths and after in others; per-mode slices confirm no write can reach a
  disabled object.

No lock-order defect found in this pass.

## Area 6 — criteria compiler and planner

State from Phase 1 (planner differential across plain / btree / bitmap /
composite twin shapes, 500 criteria x 4 shapes x seeds 1-2): every
divergence was attributed to AUD-QRY-1 and AUD-QRY-2; later review rounds
added AUD-QRY-4, -5, -6, -7, -8, -10, -11. Their fix branches are in the
executed-uncommitted or committed state recorded in the index, and the
differential section tally reached all zero after QRY-2.

Remaining planner items (none data loss):
- AUD-PLN-2: an explicit `{"and":[...]}` as the sole criteria element does not
  flatten, so it scans (results correct).
- AUD-PLN-3: composite-only objects never plan a composite path (scan, results
  correct).
- Depth/leaf limits: `MAX_CRITERIA_DEPTH=16` and `MAX_INTERSECT_LEAVES=8` are
  enforced; the >8 leaf case is covered by the QRY-10 `has_subtree` rule.
- Parsing of criteria is also an untrusted-input surface: covered by the
  libFuzzer `fuzz_criteria` harness (Phase 1) and re-visited in the find/count
  slice.

What this does not cover: new operators or shapes added after the harness was
tuned (the generator covers eq/range/between/in/starts/or/and; like, contains,
len_*, regex and eq_field are scan paths exercised by the per-mode find
slice, not by the differential).
