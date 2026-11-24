# fix/audit-kf-deleted-underflow — AUD-QRY-3

Campaign: `docs/plans/2026-11-19-audit-campaign.md` Phase 4 (third DL
branch). Finding: `AUD-QRY-3` in `docs/plans/2026-11-19-audit-index.md`
(DL-class: the count query returns 0 for a fully intact object). Branch
name: `fix/audit-kf-deleted-underflow`, branched off current `main`.
Sequence: after the AUD-QRY-1/2 branches.

## Brief (approval surface)

### Problem and root cause

Any bulk-insert window that rejects a record AFTER its kf slot was reserved
(`recs[i].status != 0` with `s->has_plan`, queued on `plan->abandoned` in
slotcask.c) tombstones that slot **without
counter bookkeeping** (`kf_write_tombstone_at_slot` in slotcask.c: "No
header total/deleted bookkeeping: no live record was ever committed here").
A later insert that reuses any flag==2 slot (`KfInsertPlan.reused_tomb`,
set by the slot planner whenever the target is a tombstone — it cannot
distinguish an inert uncounted tombstone from a real counted one) commits
with `hdr->deleted--` (`kf_commit_planned_slot`). When the shard's
`deleted` was 0 — exactly the inert case — this **underflows to
0xFFFFFFFFFFFFFFFF**. `slotcask_sum_kf_totals` sums raw across shards and
`resolve_counts` clamps once at the object level (`total > deleted ?
total - deleted : 0`): one wrapped shard makes the whole object report
**count = 0** while every record is intact and readable. `orphaned`
returns `(int)deleted` = **-1**.

**This is reachable from ordinary client input, not only I/O faults.**
Rejection after reservation happens on a plain bitmap-cap overflow:
`bitmap_prepare_window_add` (index.c) returns 1 when a bitmap field would
exceed its distinct-value limit (`on_disk + npending + 1 > max_v`);
`v2_bulk_ins_prepare_window` (query_bulk.c) then sets `record_rejected` and
`recs[j].status = -1`. A fault (rejected window) is a second route, and the
original repro. Consequently existing production roots MAY already hold
unbooked inert tombstones and wrapped shards; fixing only new bookings is
not enough. The reuse path must not assume the tombstone was counted: the
fix therefore has two parts, (a) book inert tombstones at write, and (b) a
non-wrapping guard at reuse that tolerates tombstones left unbooked by
older versions.

### Impact inventory: every reader of total/deleted (the human's directive)

Verified by reading each site; the corrupt value is a wrapped `deleted`
(= 2^64−1) on one shard, with `total` correct.

| reader | site | reads | acts on the corrupt value? |
|---|---|---|---|
| `count` (client), all `get_live_count` metadata | `resolve_counts` (storage.c): raw sum + one object-level clamp | both | **YES — count returns 0 only when the other shards' summed `deleted` is 0** (the observed defect). Otherwise the wrapped shard plus other shards' tombstones wraps the u64 sum back to a small number and count reads **true live + 1** (silent overcount; `orphaned` off by −1; the edit-field `src_deleted > src_total` guard does NOT trip; vacuum-check / auto-vacuum act on a plausible but wrong number; per-shard `live = (uint32_t)(total − deleted)` in query_maint.c shows +1). The per-shard header assertion is the sharp oracle. |
| `orphaned` | `cmd_orphaned` (query_maint.c): `(int)get_deleted_count` | deleted | **YES — returns −1** |
| `vacuum-check` recommendation | server.c sweep: `deleted >= min_deleted && deleted*100 >= total*pct` with `int deleted` | both | **YES — skip direction**: `deleted` becomes −1 → object silently omitted from vacuum-check output entirely |
| auto-vacuum thread | same thresholds "by construction" (server.c, `auto_vacuum_thread`) | both | **YES — never fires** for the object → inert slots never reclaimed (space leak; no destruction) |
| auto-reshard sweep + `RESHARD-HINT` | `get_live_count_ll(_for_schema)` (storage.c, "callers that compare against multi-billion thresholds (auto-reshard)") | live | **YES — skip direction**: live reads 0 → object never widens (perf only) |
| edit-field / field-conversion rebuild | query_find.c conversion txn: `src_deleted > src_total` → abort | both | **YES — refuse direction (fail-closed)**: "cannot read the source record count" — schema mutation blocked on wrapped objects |
| planner cost model | `selectivity_budget(N)` with N=0 via `get_live_count` | live | YES — every leaf reads unselective → plans degrade to scans (perf only; results correct) |
| `recount` | query_maint.c: re-reads headers and clamps | both | **NO REPAIR** — v2 recount is a thin header re-read; it parrots the corruption |
| inline kf growth trigger / bulk pre-grow | `kh.hdr->total`-only checks (slotcask.c) | total | NO — total is not corrupted by this underflow |
| resplit (`kfcache_resplit_locked`) and vacuum's kf-compact step (`slotcask_compact_kf`, which resplits same-capacity whenever `hdr->deleted > 0` — the wrapped value passes) | recompute both counters from a real scan | both | **HEALS** the shard (the production heal paths — `recount` does not repair, it re-reads) |
| `slotcask_walk_live`/rebuild batch sizing | live count used only for alloc sizing; live==0 → full-capacity alloc | live | NO — safe (bigger allocation) |
| `stats` / `shard-stats` / `size` | display / du | both | display-only (wrapped values shown) |
| backup | no counter gates found (sparse copy) | — | NO |

**Answer to "can any decision or deletion path act on the corrupt value?":
no deletion path acts on it — vacuum/compact never destroy anything because
of it — but three decision paths act on it in the skip/refuse direction
(auto-vacuum never reclaims, auto-reshard never widens, edit-field
refuses), and every client-facing count/orphaned/planner-cost read is wrong
until a resplit of the affected shard heals the header.**

### Logical flow

1. `kf_write_tombstone_at_slot` (inert tombstone of a rejected reservation)
   additionally books the slot as lived-and-died: `hdr->total++` and
   `hdr->deleted++` under the window's kf wrlock.
2. The booking is marked durably (`plan->kf_header_changed = 1`, the
   flag `kfcache_sync_slots_locked_impl` flushes the header on) and is
   idempotent under window re-entry (booked only when the slot was not
   already flag==2 — the KFM2 forward-replay path re-runs
   `bulk_apply_and_sync_kf_locked` on the same plan).
3. Every later reuse of that slot (`reused_tomb` → `hdr->deleted--`) is
   now backed by a prior increment — accounting balances in all
   orderings, including mixed windows (delete + inert-tombstone + reuse
   in one window) and across replay re-entry.
4. For tombstones left unbooked by older versions, the reuse in
   `kf_commit_planned_slot` becomes non-wrapping: `if (hdr->deleted)
   hdr->deleted--; else hdr->total++;` plus a warn-once. With step 1 in
   place the guard never fires on new state; on old state `live` stays
   exact and only `deleted` is under-counted.
5. `live = total − deleted` stays exact at every point; `orphaned`
   transiently counts inert tombstones (they ARE dead slots — honest).
6. No change to any reader; no on-disk format change.

### Decisions made

| decision | alternatives | why | tag |
|---|---|---|---|
| Both: (a) symmetric bookkeeping at the inert-tombstone write (`total++ && deleted++`, only when the slot was not already flag==2, plus `plan->kf_header_changed = 1` so the header flushes) AND (b) a non-wrapping guard at reuse in `kf_commit_planned_slot` (`if (hdr->deleted) hdr->deleted--; else hdr->total++;`, warn-once) | (b) alone; (a) alone; (c) on-disk marker distinguishing inert tombstones | (a) alone leaves roots that already hold unbooked tombstones (any root that ever hit a bitmap-cap rejection or fault) wrapping on next reuse. (b) alone is order-dependent: `deleted == 0` at commit time is provable only when no counted tombstone exists, so a mixed window (real delete before inert reuse) can mis-decrement a different tombstone's count; it keeps `live` exact but under-counts `deleted`. Together: new state is exact (a), old state is safe (b). (c) changes on-disk semantics — rejected (format risk) | DEFAULT |
| Repair policy for roots ALREADY wrapped: warn at kf open when `deleted > total`; no auto-repair in this branch; queued follow-up plan makes v2 `recount` rebuild the header total/deleted from the slots (an explicit, operator-run repair) | (b) startup auto-recount scan; (c) leave silent; (d) none of the follow-up | Facts for the decision: wrapped roots are NOT rare — they can be produced by ordinary bulk-insert bitmap-cap rejection followed by reuse, so some production roots may already be wrapped. Existing heals: vacuum's kf-compact step and resplit recompute the counters; `recount` does NOT (it re-reads the headers) and `reindex` does not touch kf headers. A startup rescan adds open-time cost. Recommended: warn + non-wrapping guard now, `recount` repair as the follow-up | NEEDS YOU |
| Product regression case uses the deterministic bitmap-cap wire repro (no LD_PRELOAD, no new seam); `audit-fault-inject` seed 3 stays an audit-only cross-check | (b) the `audit-faultlib` interposer as the product test; (c) a new TEST_BUILD seam | the interposer and `src/test/audit/*` exist only on the uncommitted harness branch (not on `main`), LD_PRELOAD of a non-sanitized .so into ASan/TSan daemons aborts or is skipped, and macOS has no LD_PRELOAD — so an interposer-driven product case could never pass the sanitizer gate | DEFAULT |

### Blast radius

- Writers: `kf_write_tombstone_at_slot` (one call site — the
  rejected-reservation path; verify no other callers).
- Readers: unchanged. `orphaned` output changes for objects containing
  inert tombstones (transient until reuse/vacuum/resplit) — documented
  behavior shift, flag in the PR body.
- Perf: two extra mmap stores on the rejected-window path (rare — only
  when a window is rejected after reservation). No hot-path change.
- On-disk format: unchanged (counters only).
- Side effects of booking (flag in PR body): shards whose only tombstones
  are inert now have `deleted > 0`, so they enter the startup free-pool
  rebuild (`slotcask_open_kf_worker` `needs_pool` →
  `slotcask_pool_rebuild_worker`; safe: entries with stream/file/offset 0
  are pushed only if the segment record is flag 2 with a matching hash) and
  count toward vacuum-check / auto-vacuum thresholds even with no dead
  segment bytes.

### Test strategy

- Product regression case `test-kf-deleted-underflow` (Task 1):
  deterministic wire repro via the bitmap cap (`bitmap(4)` index,
  bulk-insert more than 4 distinct values into one shard with at least one
  record surviving, then re-insert a rejected key with an allowed value);
  asserts `count == model`, `orphaned >= 0`, and per-shard `total >=
  deleted` read from the kf file header. RED on audit-base, GREEN after.
  Also a reuse-guard unit-style case: a hand-crafted shard with an
  unbooked tombstone (deleted==0) reused → no wrap.
- Fault harness (`audit-fault-inject`) seed 3 as the cross-check: its
  count violation must clear.
- Full suite + sanitizer gate: the diff writes to kf header counters
  (shared state under the kf wrlock) → per AGENTS, **three consecutive**
  asan/tsan passes + full suite each; rebuild normal after.

### Out of scope

- Repair tooling for already-wrapped roots: only the warning here; the
  `recount` header-rebuild is a queued follow-up plan (human decision
  above).
- The vacuum/reclaim accounting for inert tombstones beyond the counter
  change (existing vacuum scans segments, unaffected).
- AUD-QRY-1 / AUD-QRY-2 (separate branches).

### Review log

- Round 1 (blind reviewer, 2026-11-19): 1 blocker — the booking stores
  were not marked durable (`plan->kf_header_changed` is set only on
  active-record branches; the header flushes only when it is set), so a
  pure-abandoned window could lose the booking in a power cut while the
  tombstone bytes persist (fixed: Task 2b); 1 major — unconditional
  booking double-counts on KFM2 forward-replay re-entry of the same plan
  (fixed: book only on the non-flag==2 → flag==2 transition). 8 minors —
  op-list pointer now cites the durable artifact with the reconstructed
  4th bulk value, failed-first-spawn branch added, "free pool" wording
  dropped (kf plans never touch the segment free pool), walk-batch sizing
  wording, heal paths corrected (vacuum's kf-compact step also heals;
  resplit is not the only one), `--include-audit` dropped from `run`
  commands, white-box label corrected, WARN rate-limit specified.
- Round 3 (independent blind review, 2026-11-19): 2 blockers, 1 major, 6
  minors, all folded in. B1: root cause was understated — inert tombstones
  arise from ordinary bitmap-cap rejection after reservation, not only
  faults, so existing roots may be wrapped (fixed: reuse guard Task 2c,
  corrected Brief, NEEDS-YOU question re-asked with the true facts, bitmap
  repro as the test). B2: Task 1 depended on harness-only code and an
  LD_PRELOAD that cannot run under sanitizers (fixed: deterministic wire
  repro, faultlib demoted to audit-only cross-check). M3: wrap-back
  overcount direction added to the impact table. Minors: "reindex" removed
  from the warning, warn hook named, side effects of booking documented,
  stale comments listed, ACQUIRE note, duplicate step number.
- Round 2 (blind re-review, 2026-11-19): 0 blockers, 0 majors; 4 minors
  — 3 fixed (rate-limit marker must be thread-safe: per-kfcache-entry bit
  or mutex, never a bare static array; msync wording says "one msync",
  not atomic — mid-msync tear is the same envelope as existing header
  writes; Task 1 oracle provenance pinned to the case's own responses
  with probe-tolerant accounting). 1 nit, no action (booking after the
  flag store transiently shows live+1 to lock-free pread readers —
  identical shape to kf_commit_planned_slot's existing ordering). All
  revision points verified: single caller, replay re-entry safe,
  kf_header_changed merge traced end-to-end, mixed-window math balances
  in all orders. CONVERGED — ready for approval.

---

## Plan (executor-facing)

### Ground rules

Branch `fix/audit-kf-deleted-underflow` off current `main` (after the
QRY-1/2 branches; it must build and pass on `main` alone — no dependency
on the uncommitted harness tree). Uncommitted until human review. `PLAN_NOTES.md` + halt
on mismatch. One heavy job at a time; `/tmp/shard-db-*` removed after every
run.

### Background: the two code sides

Writer side (the gap):

- `kf_write_tombstone_at_slot(SlotcaskKfHandle *kh, size_t slot, const
  uint8_t hash[16])` — writes hash, zeroes stream_id/file_id/offset, sets
  flag=2 with release semantics. The comment block above it explains why
  the tombstone exists (flag==0 would hard-stop probe chain walks) and
  states the no-bookkeeping intent. Its only caller is the
  rejected-reservation path in the window commit logic (verify: exactly
  one caller; if more, each needs the same bookkeeping decision).
- Counter context: `SlotcaskKfHeader` = magic, version, `total`,
  `deleted` — mmap'd at kf file offset 0; plain stores under the kf wrlock.

Reuse side (the underflow):

- The slot planner marks `plan.reused_tomb = 1` whenever the planned target
  slot currently holds flag==2 (first-tombstone-hit during probing / free
  pool / bulk plan paths — all `reused_tomb` assignments).
- `kf_commit_planned_slot`: `if (plan->reused_tomb) hdr->deleted--; else
  hdr->total++;`
- Object-level math: `slotcask_sum_kf_totals` (raw per-shard sum via pread)
  → `resolve_counts`: `live = total > deleted ? total - deleted : 0` —
  one wrapped shard clamps the whole object to 0.

Red evidence: `docs/plans/audit-artifacts/AUD-QRY-3-evidence.txt`
(deterministic seed 3 of `audit-fault-inject`, 3/3 reproductions; kept-root
kf header hexdump: shard 002 `deleted=0xffffffffffffffff`, total=1; object
sum 7 / 2^64−1 → count 0; recovery WARN "1 staged, not-activated
record(s)"; all 8 model-live keys read back correct).

### Task 1 — regression test (test-first; expect RED on base)

New file `src/test/cases/test_kf_deleted_underflow.c`, registered
`TEST_REGISTER("test-kf-deleted-underflow", ...)`, wired into `build.sh`
next to the bulk cases; fixture via `test_env_start(&env)` / `test_env_stop`
(model on an existing bulk/bitmap case, e.g. the bitmap-cap tests). No
LD_PRELOAD, no harness dependency (this branch is cut from `main` and must
build there).

Case A (wire, deterministic): object with `v:bitmap(4)` plus a varchar key
field, `splits=8`. bulk-insert records using more than 4 distinct `v`
values in one request so the cap rejects at least one record AFTER its kf
slot reservation while at least one other record in the same shard window
survives; then re-insert a rejected key with an allowed `v`. Assert: `count`
== the case's own live model; every accepted key reads back; `orphaned >=
0`; per-shard kf header (plain file read of `data/kf/NNN.kf` bytes 0..23,
no seam) has `total >= deleted` (no wrap). If the first attempt does not
produce a rejected-after-reservation window on this machine, vary the
number of distinct values / records per shard, never loosen the
assertions; if it still cannot be produced, `PLAN_NOTES.md` + halt.

Case B (reuse guard): craft a shard with an unbooked inert tombstone
(flag==2, header deleted==0) by writing the kf slot flag directly in a
closed object (pattern of `test_slotcask_grave_run_frontier.c`), reopen,
insert a key that hash-lands on that slot, assert `deleted` did not wrap
and `live` is exact. This isolates part (b) from part (a).

Test-first proof: on the branch base, Case A must fail on the header
assertion (and/or count) and Case B on the wrap; paste. Audit-only
cross-check (needs the harness branch, not part of this branch's gate):
`SHARD_AUDIT_FAULT_FIRST_SEED=3 SHARD_AUDIT_FAULT_SEEDS=1
SHARD_AUDIT_FAULT_OPS=24 ./build/bin/shard-db-test run audit-fault-inject`
shows the same violation on base and clears after the fix.

### Task 2 — the fix

`kf_write_tombstone_at_slot` (slotcask.c) gains bookkeeping that is
**idempotent under re-entry** and **marked durably**. Its single caller is
the abandoned-plan branch of `bulk_apply_and_sync_kf_locked`, which is
contractually re-runnable on the same plan (KFM2 forward replay; deferred
window finalization) and already guards with `tflag == 2 && hash matches`.
Read the prior flag BEFORE the store and book only on the transition:

```c
static void kf_write_tombstone_at_slot(SlotcaskKfHandle *kh, size_t slot,
                                        const uint8_t hash[16]) {
    SlotcaskKfEntry *t = &kh->map[slot];
    uint8_t prior_flag = __atomic_load_n(&t->flag, __ATOMIC_ACQUIRE);
    /* (the caller already loaded tflag with ACQUIRE under the wrlock; pass
       it in as a parameter instead of reloading if that is simpler — the
       two are equivalent) */
    memcpy(t->hash, hash, 16);
    t->stream_id = 0;
    t->file_id = 0;
    t->offset = 0;
    __atomic_store_n(&t->flag, 2, __ATOMIC_RELEASE);
    /* AUD-QRY-3: book the inert tombstone as lived-and-died, exactly once.
       A later insert reusing any flag==2 slot commits with hdr->deleted--
       (kf_commit_planned_slot cannot tell an inert tombstone from a real
       one); without a matching increment that decrement underflows when
       the shard's deleted is 0, wraps deleted to UINT64_MAX, and clamps
       the whole object's live count to 0 (resolve_counts). Booked only on
       the 0/1 -> 2 transition so forward-replay re-entry (which re-runs
       this write on an already-tombstoned slot) cannot double-count. */
    if (kh->hdr && prior_flag != 2) {
        kh->hdr->total++;
        kh->hdr->deleted++;
        /* caller must mark the header dirty: see Task 2b */
    }
}
```

Task 2b (durability — REQUIRED, same commit): in the abandoned-plan branch
of `bulk_apply_and_sync_kf_locked` (the `kf_write_tombstone_at_slot` call
site), set `plan->kf_header_changed = 1` when any tombstone was written —
today that flag is set only on the active-record branches, and
`kfcache_sync_slots_locked_impl` flushes the 24-byte header only when
`header_changed` is set. Without this, a pure-abandoned window syncs the
flag==2 slot bytes with `header_changed = 0` and the booking can be lost
in a power cut while the tombstone bytes persist — the uncounted-tombstone
state reappears on disk and the next reuse underflows again. Note the
post-fix barrier is ONE msync spanning the header page and slot pages:
a power cut mid-msync can still persist slot bytes without page 0 — the
same unmodeled-tear envelope as every existing header write (e.g.
`kf_commit_planned_slot`'s flag store + counter update), not a
regression; do not describe the ordering as atomic.

Task 2c (reuse guard — part (b), REQUIRED, same commit): in
`kf_commit_planned_slot` replace `if (plan->reused_tomb) hdr->deleted--;
else hdr->total++;` with the non-wrapping form: when `reused_tomb` and
`hdr->deleted == 0`, do `hdr->total++` instead of decrementing, and emit a
warn-once line (same thread-safe marker rule as Task 3) naming the shard
path: "reused an unbooked tombstone (deleted==0); counters left consistent
(legacy inert tombstone, AUD-QRY-3)". Keep `live = total - deleted`
exact.

Executor checks:

- `kh->hdr` non-NULL: keep the `if (kh->hdr && ...)` guard (matches
  `kf_commit_planned_slot`'s defensiveness).
- Confirm the single caller of `kf_write_tombstone_at_slot`; if a second
  exists, apply the same bookkeeping there or record why not in
  PLAN_NOTES.md.
- Verify `bulk_apply_and_sync_kf_locked` merges `kf_header_changed` into
  the deferred sync barrier (it OR-serves the flag into the window's sync
  decision) — if the merge needs a new field on the plan struct, add it
  there, not on the shared header path.
- Update the comments: the rejected-reservation block (the "must not count
  it either way" sentence is superseded — the tombstone now counts as
  lived-and-died), the comment above the abandoned-queueing in
  `slotcask.c` ("stays flag==0"; the target can now be the same key's
  flag==2 slot on re-entry), and the comment on `kf_write_tombstone_at_slot`
  that describes the target as flag 0 only.
- Do NOT touch `kf_commit_planned_slot` or the planner's `reused_tomb`
  assignments.

### Task 3 — open-time visibility (per the NEEDS YOU default above)

Where kf shards are opened and the header is validated against the magic
(slotcask.c kf-open path, the `created_fresh` / magic-check block — note
this runs on every kfcache miss/eviction-reopen, not once per daemon),
add a WARN when `hdr->deleted > hdr->total` on a loaded shard:
`"kf header counters inconsistent (deleted > total) on <path>; run
vacuum to rebuild the shard — see AUD-QRY-3"` (vacuum's kf-compact /
resplit are the only heals; `reindex` does not touch kf headers). No
mutation. Hook: immediately after the entry is installed in the kfcache
acquire path (the static mmap opener's `created_fresh` block has no cache
entry yet, so do not put it there). Rate limit: a per-kfcache-entry bit;
an evicted-and-reopened entry re-warns, which is accepted (state it in the
commit body). If a mutex-guarded path-keyed table is chosen instead, it
must be thread-safe — a bare unlocked static array is a TSan finding.
The same marker serves the Task 2c warn-once. (If the human opts for auto-repair
at approval, that is a separate follow-up task — do not improvise it
here.)

### Task 4 — prove and gate

1. Revert-prove-paste (Task 1 red → restore → green; paste both).
2. Targeted case + the fault-harness seed-3 cross-check green.
3. Full suite `run-all --jobs 8` fresh.
4. Sanitizer gate — **three consecutive passes each** (the new cases are
   plain wire/file cases and run under ASan/TSan) (kf header counters =
   shared state under the kf wrlock): `BUILD_MODE=asan` →
   `run-all --jobs 6` ×3; `BUILD_MODE=tsan` (with
   `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"`) →
   `run-all --jobs 3` ×3; rebuild normal.
5. Fault-matrix spot-check: `SHARD_AUDIT_FAULT_SEEDS=15
   SHARD_AUDIT_FAULT_OPS=24 ./build/bin/shard-db-test run
   audit-fault-inject` — the seed-3 count violation must be gone
   (AUD-HARN-3 phantoms may remain; they are the harness gap, not this
   bug).
6. Index: AUD-QRY-3 → `fix-branch`; paste the reader-impact table status
   (no deletion path acts on the corrupt value; skip/refuse directions as
   inventoried).

### Embedded execution rules

Tasks in order; both revert outputs pasted; no unrelated changes;
PLAN_NOTES.md + halt on mismatch; if the human's approval answer to the
NEEDS YOU item differs from the DEFAULT, Task 3 changes accordingly (warn →
auto-repair), still one concern.
