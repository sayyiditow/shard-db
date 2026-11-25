# Plan: AUD-QRY-11 — kf window-replay re-entry double-books the insert commit

**File named `2026-11-23-audit-qry10-kf-commit-reentry.md` per the round-2
review directive; the campaign ID AUD-QRY-10 is already taken (the
>MAX_INTERSECT_LEAVES AND-leaf finding, fixed in the QRY-2 branch), so this
finding carries the next free ID, AUD-QRY-11.**

**Status: EXECUTED 2026-11-25 (uncommitted until reviewed). Severity: MAJOR (live over-count), not DL.**

As built: `kf_commit_planned_slot` returns early (no counters, no `used_delta`)
when the slot is already live with the same hash, stream, file and offset.
Repro: in-process `test-kf-window-replay-double-booking` (legacy window path,
one non-sticky K failure): red on the base (total 2 vs 1 for a fresh insert;
live 2 vs 1 for tombstone reuse), green with the guard. The wire/deferred-route
variant in the repro section below was not built; the guard is route-agnostic.

## Finding

`bulk_apply_and_sync_kf_locked` (src/db/slotcask.c) is contractually
re-runnable: the window coordinator re-enters it via
`bulk_replay_window_forward_locked` (and the same-function retry ladder)
whenever a step at or after K fails — the documented case being an msync
failure inside K itself. The abandoned-reservation tombstone write is
guarded for re-entry (the `prior_flag` / flag==0-or-own-tombstone check
books only on the 0/1 → 2 transition), but the ACTIVE-record commit is
not: `kf_commit_planned_slot` runs its bookkeeping unconditionally.

On a re-entry, every active insert's slot is already flag==1 with this
hash (the first K run committed the bytes), so:

- a fresh-placement commit (`reused_tomb == 0`) takes the `hdr->total++`
  branch a second time — one phantom `total` per record in the window;
- a tombstone-reuse commit (`reused_tomb == 1`) takes `hdr->deleted--` a
  second time — one phantom decrement per reused slot; once `deleted`
  reaches 0 the non-wrapping reuse guard turns each further one into a
  `total++`, so the result is a live over-count either way (never a wrap).

`live = total − deleted` is therefore over-counted after a K-phase msync
failure that the coordinator recovers from: `count` reports more records
than exist until the next header rebuild (vacuum's kf-compact, resplit).
Records themselves are intact — the bytes, index applies and marker
clear all converged idempotently; only the header arithmetic double-fired.

## Why major, not DL

No record is lost, resurrected, or torn: the double booking inflates
`total` (or deflates `deleted`), so `count`/`get_live_count` over-report
on the affected shard(s) until the next header recomputation. Reads,
deletes and CAS on every real record behave correctly. A wrong COUNT that
a restart does not fix is a correctness major by the campaign's
classification; it does not destroy acknowledged data.

## Repro (test-first, deterministic)

New case `test_kf_window_replay_double_booking` (wire, deterministic, no
fault interposer):

**Which path re-enters K.** Only the legacy, non-deferred window path
(`bulk_commit_one_shard`, `txn->req == NULL`) syncs the kf inside K: a
failure there jumps to `replay`, and `bulk_replay_window_forward_locked`
re-runs `bulk_apply_and_sync_kf_locked` on the same plan. On the deferred
request pipeline K does NOT msync — the sync happens in the chain flush
(`bulk_commit_chain_flush_kf`), and a failure there does not re-run K
(markers are kept and converged by marker replay), so an msync failure
on the deferred route does not reproduce this. The deferred route to
re-entry is `slotcask_bulk_finalize_shard`'s second
`bulk_finalize_one_kf_window` call, which fires only after an
apply-time error in K or T (`kf_repoint`, `kf_tombstone_entry_locked`,
`segcache_acquire` or `slotcask_tombstone_mark` failing), not on an
msync failure.

1. Pick the request shape of `test-window-release-routes` Route 3, the
   existing consumer of the K-phase sync seam, which reaches the legacy
   path; N = 40 fresh records over one shard group. (A second case drives
   the deferred route with an injected T-phase apply failure.)
2. Arm the engine's K-phase sync-failure seam:
   `test_env_ctl_set_fail_phase(&env, SHARD_TEST_PHASE_K, 1, 0)` (fail
   exactly the first K msync).
3. Run the bulk-insert. The window commits bytes, fails the K sync once,
   the coordinator retries, and the re-run converges; the request
   eventually ACKs (or reports durability-degraded, per the coordinator's
   contract — both are fine for the oracle).
4. RED assertions on the current tree, straight from the shard headers
   (the `read_kf_header` helper of `test-kf-deleted-underflow`):
   - `sum(total) == N + (number of first-run commits)` → i.e. total is
     inflated by the re-run's second `total++` per record;
   - `count` (wire) == 2N-equivalent over-report vs the model N;
   - for a second shape with tombstone reuse (pre-delete half the keys,
     then bulk-insert the same keys), `sum(deleted)` is UNDER-counted by
     the re-run's second `deleted--` per reuse.
5. GREEN post-fix: totals exact (`sum(total) == N`), count == model,
   deleted exact on the reuse shape.

## Proposed fix (small, local, idempotent)

Make the commit idempotent at the slot: in `kf_commit_planned_slot`,
before the byte write and bookkeeping, load `t->flag`; if it is already
1 AND `memcmp(t->hash, plan->hash, 16) == 0` AND the slot's recorded
stream/file/offset match the plan (not just the hash — two rewrites of
the same key land in different stream positions), the first run already
committed this exact slot — skip the counter update entirely. Skip the
`used_delta++` as well: the byte length is unchanged, and a second
increment would inflate the window's size accounting. Everything else
keeps today's behavior. This mirrors the abandoned-write guard's shape
(verify-then-act on the observed slot state) and costs one acquire load
per commit on the hot path (the slot's cache line is already being
written). The K re-run's `kf_repoint` path needs no change (no counters).
The DELETE branch re-enters `kf_tombstone_entry_locked` on an already
flag==2 slot — verify whether that helper books unconditionally too; if
it does, give it the same observed-state guard (delete-of-delete must not
`deleted++` twice).

Startup KFM2 replay is already idempotent (it looks the slot up first and
refuses a flag==1 slot), so it is NOT a route to this defect — the repro
and fix scope to the bulk commit paths above. The observed-state guard
is re-entry-agnostic anyway (it keys on the slot's flag/hash/location,
not on who re-entered). Note: `kf_tombstone_entry_locked` does not
double-book (it acts only on flag==1), but on re-entry it returns -1
although its comment calls that a safe no-op, so an inline replay of a
delete window always falls over to the marker gate — a separate,
pre-existing oddity.

## Gates

- Revert-prove: stash the guard → the new case reds exactly on the
  inflated-total assertions; pop → green.
- Full suite `--jobs 8`; the case joins the default suite
  (TEST_REGISTER, not audit-gated).
- Sanitizers THREE consecutive runs each, ASan+UBSan and TSan (the guard sits on the bulk
  commit hot path concurrent pipelines execute — the AGENTS repeat rule
  for shared-state diffs).
- Changelog entry under the same section as AUD-QRY-3's (counter
  accounting), severity major, with the note that a recovered K failure
  previously over-reported until the next header rebuild (`shard-stats`
  itself has no deleted field — don't claim one).

## Queue

Not scheduled by this document (review directive: write, don't execute).
Natural position: after the open Phase-4 branches land, before/with the
AUD-QRY-9 Pass-2/sec_map branch (both slotcask/aggregate fail-closed
family). Branch name suggestion: `fix/kf-window-replay-booking`.
