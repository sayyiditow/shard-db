# Bug: recover_one_stream's active-file walk misplaces reserve_off across a mid-file free-pool gap

Status: **diagnosis only — not yet an execution-ready plan.** Surfaced during
the blind review of `refactor/variable-only-segments` (2026-08-14). Not
introduced by that diff — `recover_one_stream` is byte-identical to `main` —
but directly adjacent to, and only half-fixed by, that diff's
`pool_split_leftover` work (`docs/plans/2026-08-13-varlen-pool-capacity-mismatch.md`).
This branch has never been released, so no on-disk data has actually been
corrupted by this in production; it is a live risk starting from the first
deployment that exercises it, on `main` as much as on this branch.

## Correction to 2026-08-13-varlen-pool-capacity-mismatch.md

That plan's "Startup recovery correctness" section (around its line 606)
claims:

> once no live-going-forward record can ever have footprint >
> header-computed size, `recover_one_stream`'s `reserve_off` reconstruction
> ... both become correct by construction for all newly-written data.

This is wrong. It conflates two different things: "every record's on-disk
footprint equals what its own header encodes" (true, post-fix) and "there
are no zero-flag byte ranges in the interior of the active segment file"
(false — `pool_split_leftover` deliberately writes zeroed, header-less
free-pool gaps into the interior of the file whenever a reused slot is
larger than the record that reused it, precisely so a later write can claim
that space). `recover_one_stream`'s walk treats the first zero flag byte as
"this is the true end of live data" unconditionally; it cannot distinguish
that from "this is a hole with more live records after it." No test caught
this because `test_varlen_pool_donation_stride.c` (the regression test
written for that plan) never restarts the daemon — it only exercises the
read-scan path (`seg_scan_o_direct`, which does resync correctly), not the
startup recovery path.

That plan's own file should get a follow-up correction note in the same
style as its existing "CORRECTION (post-implementation, 2026-08-13)" header,
pointing at this doc, when this bug is actually fixed.

## Root cause

`src/db/slotcask.c:4508-4524`, inside `recover_one_stream` (active/last
segment file branch only — non-active files go through
`recover_scan_tombstones_od` instead, which is not implicated here since
it's not responsible for `reserve_off`):

```c
SlotcaskSegHandle h;
if (segcache_acquire(&h, path, 0, 0, 0) != 0) { free(ids); return -1; }
off_t pos = 0;
off_t lim = (off_t)h.map_size;
while (pos + 24 <= lim) {
    uint8_t flag = __atomic_load_n(&h.map[pos + 18], __ATOMIC_ACQUIRE);
    if (flag == 0) break;
    uint16_t klen; memcpy(&klen, h.map + pos + 16, 2);
    uint32_t vlen; memcpy(&vlen, h.map + pos + 20, 4);
    size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
    if (flag == 2)
        pool_push_free_cap(&db->streams[sid], (uint16_t)file_id,
                           (uint32_t)pos, (uint32_t)rec_size, db->slot_size);
    pos += (off_t)rec_size;
}
last_offset = pos;
segcache_release(&h);
```

`pos` strides forward by `rec_size` only while it keeps landing on real
record headers. The moment it lands on a `pool_split_leftover`-zeroed gap
(flag byte 0, no header at all — not a tombstone, not a live record), it
breaks immediately and reports that position as `reserve_off`
(`slotcask.c:4531-4533`), even when live records physically continue past
the end of that gap.

### Concrete repro (no crash needed — an ordinary clean restart)

1. Insert record A (large enough to land in the top, unbounded free-pool
   bucket — `slotcask_bucket_for`, `slotcask.c:3619`, bucket 3 is
   `[8192, ∞)`) into stream 0's active segment at offset `X`; record E gets
   appended right after it at `X + cap(A)`.
2. Delete A → tombstoned, its slot pushed to the pool as a bucket-3 entry
   `(file, X, cap(A))`.
3. Insert record B, still bucket-3-sized but smaller than A (e.g. A is
   9000 B, B is 8200 B). `pool_try_pop_for_size` returns A's old slot;
   `pool_split_leftover` writes B tightly at `X` and zeroes+registers the
   leftover (`cap(A) − cap(B)` bytes) at `X + cap(B)` as a new free-pool
   entry — those bytes are on-disk zero, flag byte 0. This leftover span
   ends exactly at `X + cap(A)`, i.e. exactly where E starts.
4. E is untouched, still live at `X + cap(A)`; its kf entry still points
   there.
5. Ordinary `stop` / `start` (SIGTERM, systemd restart, no crash).
   `recover_one_stream` walks the active file from 0: reaches B at `X`,
   strides by B's own header-computed size (`cap(B)`, since B was written
   tightly and its footprint matches its header exactly) to `X + cap(B)` —
   exactly the start of the zeroed leftover — reads flag 0, breaks.
   `reserve_off` becomes `X + cap(B)`, strictly before E's true start at
   `X + cap(A)`.
6. The next insert into stream 0 reserves space starting at `X + cap(B)`.
   If it's smaller than the gap (`cap(A) − cap(B)`), no immediate
   corruption — but the free-pool no longer has this space tracked (its
   in-memory-only bookkeeping is gone after restart, and
   `recover_one_stream`'s walk didn't re-register it as free either, since
   it never got past the flag-0 byte). As soon as cumulative appends since
   restart exceed the gap size, subsequent sequential writes cross into and
   silently overwrite E's live on-disk bytes, while E's kf entry still
   points at the now-clobbered offset. The next read of E returns
   corrupted data (garbage, wrong record, or a hash mismatch) — silent,
   triggered by an entirely ordinary restart plus normal write traffic.

## Why existing tests don't catch this

- `test_varlen_pool_donation_stride.c` (the closest existing coverage)
  never stops/restarts the daemon.
- No other test in the suite restarts a daemon against an active segment
  file that has a mid-file free-pool gap with live data after it — the
  general restart/recovery tests use append-only or fully-compacted
  fixtures where `reserve_off` always lands at true end-of-file.

## Scope note: only the active file needs a fix

`recover_one_stream` treats the active (last) segment file completely
differently from every older, rotated file (`slotcask.c:4497-4503`):
non-active files go through `recover_scan_tombstones_od`
(`slotcask.c:4342`), a separate O_DIRECT scanner with its own
carry-buffer/chunk-straddle handling — structurally the same
resync-capable machinery the read-path `seg_scan_o_direct` uses, already
confirmed correct by the blind review. That function's only job is
discovering tombstones to re-register as free-pool entries; it never
computes an append cursor, because rotated files are permanently closed to
new writes. Reused/refilled slots can absolutely land in older files too
(the free-pool doesn't care which file a slot belongs to), but that only
needs tombstone discovery (already safe) — the cursor-reconstruction bug
is confined entirely to the active-file branch at `slotcask.c:4508-4524`,
because that is the one and only place a `reserve_off` gets computed at
all.

## Suggested fix direction (preferred: in-band self-describing skip marker)

Two alternatives were considered and rejected in favor of this one:

- A pure "add resync to the active-file walk" fix (mirroring
  `seg_scan_o_direct`'s byte-level resync approach) would be correct but
  adds a second, separate resync implementation to maintain.
- Persisting `reserve_off`/`active_file_id` externally at clean shutdown
  (gated by the existing `clean_flag_write`/`clean_flag_exists` marker,
  falling back to a full walk on an unclean restart) would avoid the walk
  entirely on the common path, but introduces a new persisted-state class
  with its own ordering/durability contract, for a startup cost that's
  bounded to exactly one file per stream — not worth the added
  crash-safety surface for what it saves.

Preferred instead: make the gap itself self-describing, so the existing
naive header-stride walk becomes correct with a one-line addition, no
external state and no separate resync algorithm needed.

`pool_split_leftover` (`slotcask.c:3697`) currently just `memset`s the
leftover span to zero and registers it in the in-memory free pool. Instead,
have it write a legitimate 24-byte record header at the start of the span
with a reserved flag value (e.g. flag=3, "free gap") whose length field
encodes the span's own byte length, then zero the remainder of the span.
This reuses the exact write-then-activate ordering every other record in
this codebase already follows (write the length field(s) first, flip the
flag byte last, atomically) — not a new invariant, the same one applied to
a new flag value. `recover_one_stream`'s active-file walk then needs
exactly one added branch: on `flag == 3`, stride forward by the encoded
length and continue, instead of unconditionally breaking on `flag == 0`.
A genuinely unwritten tail (true end of file) still has flag byte 0 with no
marker header ever written there, so the walk's stopping condition for
*that* case is untouched and still correct.

Edge case: a leftover span shorter than 24 bytes can't hold a marker
header. Treat it the same way `pool_split_leftover` already treats a
zero-length leftover (`if (len == 0) return 0;`) — don't push it to the
free pool, just leave it as permanently wasted slack; these sub-24-byte
slivers are bounded by record-size alignment and rare enough that reuse
isn't worth the complexity.

A regression test for this needs to: seed the exact repro above (or use
`fabricate_kf_total`-style low-level fixture control to force the specific
donor/leftover/next-live-record byte layout deterministically, matching
this repo's preference for non-flaky, non-timing-dependent tests),
**actually restart the daemon** (stop, start, not just call the recovery
function directly), and assert both that `reserve_off`/`active_file_id` are
correct after restart *and* that E's value is still readable and unchanged
after a subsequent unrelated insert forces enough new writes to cross the
old gap boundary. Separately, cover the crash-path fallback (no clean-flag
present) still reconstructing correctly via the resync walk, and cover that
a corrupted/truncated persisted-cursor file is treated as invalid evidence
(falls back to the walk) rather than trusted.

## Related test-quality gaps (also from the 2026-08-14 blind review, lower severity, worth folding into the same follow-up session)

- `src/test/cases/test_removed_storage_surfaces.c:93-99`,
  `test-light-vacuum-errors`: currently `ASSERT_TRUE(1, ...)` — a
  placeholder that passes unconditionally regardless of whether
  `cmd_vacuum`'s error-propagation from `slotcask_compact_segs`/
  `slotcask_compact_kf` actually works. (Already flagged as a known gap in
  earlier session work; this is a second, independent confirmation.) Needs
  deterministic fault injection to become a real assertion.
- `src/test/cases/test_auto_reshard.c`'s `huge`-object
  intentionally-mismatched-count rebuild-failure path stops the daemon
  immediately after confirming the failure was logged, without checking the
  daemon is still alive/responsive afterward (object lock released, no
  wedged thread). The old test's full successful-reshard round-trip
  incidentally covered this; the new failure-path test doesn't have an
  equivalent liveness check.
