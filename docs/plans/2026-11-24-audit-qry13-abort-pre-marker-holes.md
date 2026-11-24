# Plan: AUD-QRY-13 — nactive==0 abort_pre_marker windows strand flag==0
# reservation holes (lost-record lead)

**Status: EXECUTED (2026-11-24) — repro confirmed the data loss (16 of 16
acknowledged later-window inserts unreachable on the previous code); fixed;
regression case `test-abort-pre-marker-holes`. Severity: DL (lost acked
record).**

## Finding (code proof — every link cited)

A deferred bulk window whose plan ends with `nactive == 0` is aborted
before marker publication, and its abandoned kf reservations are never
tombstoned. An abandoned reservation is a kf slot left at **flag==0** on
a table where later windows of the SAME request may already have placed
records beyond it — and `flag==0` is the ONLY stop condition of the kf
probe walk. Any committed record whose probe chain contains such a hole
becomes unreachable: GET/exists of an acknowledged insert returns
absent. That is data loss, not a counter defect.

The chain, link by link:

1. **Reservations precede rejection.** `bulk_plan_window_locked`
   reserves each NEW-insert record's kf slot via
   `kf_plan_window_insert_slot` *before* the record is admitted to
   `plan->active` (the reservation lives in `s->kf_plan`,
   `s->has_plan = 1`). slotcask.c, the "Physical kf location +
   pre_commit" block: "fired under the held kf wrlock … (e.g. a
   pre_commit rejection)".
2. **Post-reservation rejection creates abandoned reservations.**
   `prepare_window` (bitmap distinct-value cap:
   `bitmap_prepare_window_add` rejects the record;
   `v2_bulk_ins_prepare_window` sets `record_rejected = 1`) and
   `pre_commit` (per-record CAS) can set `recs[i].status != 0` after
   the reservation. `bulk_plan_window_locked` then files the plan into
   `plan->abandoned[plan->nabandoned++]` — with its own comment
   naming the hazard: "it stays flag==0 and can hard-stop the probe
   chain for another record in the same window whose accepted slot
   lands further along it. Queue it for tombstoning in the K phase."
3. **The nactive==0 window skips the K phase entirely.**
   `bulk_publish_one_kf_window`: `if (rw->plan.nactive == 0) {
   req_window_abort_pre_marker(rw); rc = 0; goto out; }`. No marker is
   published; and `slotcask_bulk_finalize_shard` only finalizes
   published windows (`if (!rw->published) continue;`). The
   abandoned-tombstone pass lives in `bulk_apply_and_sync_kf_locked`
   (the K phase) — it never runs for an unpublished window. The holes
   stay flag==0. (`rc = 0`: the request SUCCEEDS.)
4. **Same-request later windows place records beyond the holes.**
   `kf_plan_window_insert_slot` probes with
   `if (reserved_here) continue; /* treat as occupied, keep probing */`
   over the request's `res_map` — "Cross-window reservations: slots
   planned (but not yet committed) by earlier windows of this request".
   A later window's key whose home slot is an abandoned hole (or whose
   chain crosses one) is placed at a FURTHER slot along the chain.
5. **After the request the holes are invisible to placement but fatal
   to lookup.** `res_map` is request-scoped; once it is freed, the
   abandoned slots read flag==0. `kf_probe_next`'s only stop condition
   is a flag==0 slot (slotcask.c: "Left at flag==0, it hard-stops
   kf_probe_next's chain walk (its only stop condition)"). Every
   committed record whose probe chain contains the hole is
   unreachable — an acknowledged insert that no GET, exists, count
   candidate, or scan-index path can reach.

Links 1–5 are all present on the current tree; no guard between them
was found. The existing K-phase tombstone pass
(`kf_write_tombstone_at_slot`, flag 0/1 → 2 with lived-and-died
booking) exists precisely to keep chains intact — the nactive==0 abort
is the one path that bypasses it.

Reachability of the trigger shape (nactive==0 with nabandoned>0): all
records of one window rejected AFTER reservation. Confirmed driver:
the bitmap distinct-value cap in `prepare_window` (a window whose
records all carry new distinct values over the cap). Pre_commit CAS is
bulk-update-only on the wire (per-record `if` is documented for
bulk-update, not bulk-insert), so the CAS route needs the update path
to also make NEW-insert plans (not verified); the bitmap route is
confirmed on bulk-insert.

## Why DL

An acknowledged insert whose record becomes unreadable (GET absent,
invisible to every query path) with no error surfaced to the client is
exactly the campaign's lost-record class. It is persistent (the hole is
in the keyfile; restarts do not heal it), and it is silent (the request
returns 200; only later reads reveal the loss).

## Repro (as built)

`test-abort-pre-marker-holes` (wire, deterministic, default suite):
`BULK_COMMIT_WINDOW=16` (the fixture passes it through) so one shard's
records span two windows; `splits=8`, `v:bitmap(4)`. Four seed records
saturate the target shard's bitmap dictionary; the shard's slot capacity
is read from `shard-stats`. Sixteen "rejected" keys R_i (distinct home
slots) are crafted, plus sixteen later keys L_i that share R_i's home
slot (`compute_hash_raw` + `compute_record_shard`). One bulk-insert:
records 0..15 are the R_i with sixteen new distinct values (all rejected
after reservation → window 1 has `nactive == 0`), records 16..31 are the
L_i with an existing value (accepted). Assertions: every R_i absent,
every L_i present (`exists`), `count == 20`. On the previous code all
sixteen L_i read as absent while `count` still said 20.

## Fix (as implemented)

1. `bulk_resolve_unpublished_reservations_locked(kh, shard, begin, end)`: on
   EVERY exit of `bulk_publish_one_kf_window` without a marker (all rejected,
   plan hard failure, payload or marker error) tombstone each reservation in
   the window's range (`st[i].has_plan`: active and abandoned alike) and msync
   them in stack-sized chunks — no allocation to fail. If the sync fails the
   window is flagged `holes_unresolved` and `slotcask_bulk_publish_shard`
   fails the remaining records of the shard instead of publishing later
   windows behind the hole.
2. `bulk_tombstone_abandoned_durable_locked`: a PUBLISHING window's
   abandoned reservations are tombstoned and synced before its marker is
   published (they previously waited for the commit barrier; a power cut
   between marker durability and the barrier left them at flag==0 while
   replay committed records beyond them). Test seam
   `SHARD_TEST_PHASE_ABANDONED`.
3. One helper pair shared with the K phase (`bulk_tombstone_abandoned_locked`).

## Gates

- Revert-prove: stash the tombstone loop in the nactive==0 branch → the
  new case reds on the lost-record asserts; pop → green.
- Full suite `--jobs 8`; sanitizers three runs each (the tombstone loop
  now also runs on the abort path — shared-state hot path).
- Changelog entry under the lost-record class with the bitmap-cap
  trigger called out; DL severity.

## Queue

Front of the fix queue (DL-class, pending the repro's confirmation).
Branch name suggestion: `fix/abort-pre-marker-holes`.
