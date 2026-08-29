# Eliminate `.tsan.supp` — fix or restructure every suppressed finding

## Status

**EXECUTED 2026-08-30 (v13 hunk + two execution-discovered fixes).**
Status: complete. Tier A: refcount + TLS g_db binds (registry trio +
kfcache/segcache warm paths — the latter found via a bulk-family SEGV)
+ drop-object dispatch wrlock with a local effective root (the v11
`db_root` assumption was a compile blocker, corrected to the restore
precedent). Tier B: R11 fixed (kf reader held across verify; red =
native delete-during-park + TSan race, green 10/0 ×4) + byte-18
uniform pairing. Tier C: deleted by evidence — full-suite TSan with an
empty supp file showed zero reports. Task Z done: `.tsan.supp`
removed, AGENTS/CI/docs updated. One further gate finding root-caused
and fixed per policy: the btree install published `used` relaxed, so
the hit-path verify had no happens-before to the installer's path
write (race inside test-online-bulk-reindex-readers' continuous count)
— now release, matching kfcache's install. Final gates on the final
tree, fresh binaries: TSan 3× and ASan (strict) 3×, each 437 cases,
12,802 passed, 0 failed, zero reports, exit 0. Work uncommitted for
the review pass. Prior approval-history paragraphs below are
historical.

**Historical: v13 approval request.** v13 was (v4 + Tier C corrections: the batch
helper is now total — per-entry failures preserve the ordered walk's
per-shard best-effort semantics, `vic_fd` is initialized to -1 so fd 0
can never be closed, and the rollback/unlock-of-unheld-rwlock hazard is
eliminated structurally; Task C3 and Task Z are now conditional on the
escalation outcome so the retain-one-line branch contradicts nothing).
A/B unchanged since the earlier review. v6 delta over v5: the merge's
batch call populates `open_paths[]` before invoking the helper (the
v5 restructure had dropped that loop), and the option-1 branch's local
TSan gate retains its `suppressions=` clause so the gate command
matches the retained file. v7 delta over v6: the batch helper's cached
handoff now performs bt_acquire_impl's publication-generation +
same-inode validation, retiring stale pre-publish entries and serving
a fresh mapping on inode mismatch (the retire's M0 touch happens only
after the rwlock release, preserving the acyclic lock graph). v8 delta over
v7: the C2a helpers are placed after `bt_release` instead of after
`bt_cache_evict_slot`, so every static function they call
(`bt_open_file`, `btree_cache_invalidate_nowait`, `bt_release`) is
already defined at the insertion point — no forward declarations, and
the file compiles under the project's implicit-declaration-rejecting
build settings. v9 delta over
v8 (after the executor's pre-code halt, PLAN_NOTES 2026-08-29): Task
A1a's regression test is rewritten against base-era APIs only — no
`slotcask_registry_put`, which does not exist until A2 — so the base
compiles it and the red-on-base proof is runnable; A2's wiring now
converts that test's readers (arithmetic: 57 wired, 56 guarded, 1
explicit-put transfer), and Task 0's recorded zero-fire outcome is
annotated with its consequences (dormant-path deletions; B1/B2 still
mandatory). v10 delta over
v9 (after the executor's second halt at A1a, PLAN_NOTES 2026-08-29):
the impossible "suppressed run must pass on base" requirement is
removed — a TSan suppression hides the report, not the UAF, so the
suppressed base run is undefined by nature. The red proof is the
already-captured unsuppressed race report (satisfied; the partial
uncommitted test remains in the tree); the baseline pass moves
post-A2, required in BOTH suppression states. v11 delta over
v10 (after the executor's A2-gate hang halt, PLAN_NOTES 2026-08-29):
planner diagnosis proved the hang was NOT the refcount — g_db is
thread-local and fresh pthreads in A1a ran the registry protocol
against garbage TLS-NULL addresses. Task A2 now includes the
registry_bind_g_db lazy-bind fix (objlock.c:41 precedent), verified:
A1a green 2× under TSan, registry 6/0, warmup 28/0. v12 delta over
v11 (execution flow correction, 2026-08-29): A1b was skipped by the
halt-driven reordering and A3 is test-first — the plan now pins A1b's
execution position between A2 and A3 (its red needs only A3's absence)
and adds A3's precondition that A1b's red proof is captured first. v13 delta over
v12 (2026-08-30, after the executor's compile halt): the A3 hunk's
effective-root claim was wrong — the drop branch runs before
`db_root` exists — and is corrected to build a local `drop_eff_root`
via `build_effective_root` (restore precedent). A1b executed between
A2 and A3 with both proofs captured: red 7/1 pre-hunk, green 19/0
post-hunk; objlock family 46/0; A1a 2/0. The corrected A3 hunk is
LANDED in the tree. Goal: an empty `.tsan.supp`, or
— on the escalation's option-1 branch — exactly one proven-FP line
retained with full proof. Work stays **uncommitted** per AGENTS.md; the
reviewing agent + human review the raw `git diff` before anything is
committed.

## Tree-drift warning (read first)

Source line numbers moved **during this planning session**
(query_bulk.c registry sites shifted +11; slotcask.c's registry block
shifted +54) — the tree is under concurrent work. Every edit below is
therefore located by **quoted anchor text**; the line numbers shown are
a 2026-08-29 snapshot for navigation only. The executor must re-run the
two mechanical greps in Task A2's verification step **before wiring
anything** and halt per the embedded rules if the inventory no longer
matches.

## Verified inventory (mechanical, first-hand 2026-08-29)

Reproducible count (63 raw matches repo-wide):

```bash
grep -rn --include='*.c' --include='*.h' 'slotcask_registry_get[[:space:]]*(' src/ bench/ \
  | grep -v 'SlotcaskDb \*slotcask_registry_get(const'
```

Classified line-by-line: **55 real production call expressions + 1 real
test call + 7 non-call matches** (6 prose comments quoting the name with
parens — objlock.c:8, server.c:626/:627/:1412, two test-file headers —
plus the slotcask.h:881 doc comment; the header *declaration* at
slotcask.h:874 is excluded by the grep filter). The v1 "5 test files"
claim was wrong: of those, two files only mention the function in
comments and two do not reference it at all. **Exactly one test file
calls it: `src/test/cases/test_reindex_bitmap_resplit.c:60`** (confined
use; wire per Task A2).

Production call expressions, 55, by file (snapshot lines; symbols are
the stable identifiers):

| File | Sites |
|---|---|
| query.c (17) | 570, 938, 1065, 1870, 2542, 3629, 3704, 3834, 4756, 5117, 5521, 6327, 6636, 6912, 7404, 7988, 8504 |
| storage.c (8) | 303, 389, 417, 989, 1559, 1851, 2091, 2447 |
| query_bulk.c (8) | 971, 1561, 2297, 2645, 3309, 3810, 4373, 5358 |
| query_find.c (6) | 260, 308, 329, 1327, 1395, 1737 |
| query_aggregate.c (6) | 105, 1643, 2490, 2794, 4663, 5196 |
| index.c (4) | 2676, 2711, 2878, 4297 |
| query_maint.c (3) | 133, 207, 891 |
| server.c (2) | 2595 (`warmup_object_open`), 2704 (`warmup_kf_task_fn`) |
| query_schema.c (1) | 609 (`cmd_edit_fields`) |

Ownership classification — the exact per-site table (C = CONFINED,
scoped guard; C-loop = confined but acquisition re-runs in a loop, see
the loop caveat in Task A2; H = HANDOFF into join-synchronous worker
fan-outs, acquirer's guard spans the join; T = TRANSFER, explicit
`put`). 45 C + 9 H + 1 T = 55:

| Site | Enclosing function | Class |
|---|---|---|
| query.c:570 | `btree_dispatch` | H (→ `BmShardWalkArg`/`BmGenericShardArg`) |
| query.c:938 | `shard_count_worker` | C-loop |
| query.c:1065 | `shard_count_worker` | C-loop |
| query.c:1870 | `idx_find_streaming` | H (→ `BatchFetchBuf.sdb` + `ChunkShared.sdb`) |
| query.c:2542 | `find_via_composite_key` | H (same two structs) |
| query.c:3629 | `build_keyset_from_bitmap` | C |
| query.c:3704 | `build_keyset_from_bitmap` | C |
| query.c:3834 | `build_keyset_bitmap_complement` | C |
| query.c:4756 | `keyset_emit_find` | C |
| query.c:5117 | `keyset_count_from_or` | C |
| query.c:5521 | `cmd_count_with_tree` | C |
| query.c:6327 | `cursor_fetch_worker` | C |
| query.c:6636 | `find_via_fetch_sort` | C |
| query.c:6912 | `bulk_delete_phase1_indexed` | C |
| query.c:7404 | `cmd_find_do` | C |
| query.c:7988 | `cmd_find_do` | C |
| query.c:8504 | `cmd_find_do` | C |
| storage.c:303 | `resolve_counts_with_schema` | C |
| storage.c:389 | `cmd_get` | C |
| storage.c:417 | `cmd_get_fields` | C |
| storage.c:989 | `cmd_insert_v2` | C |
| storage.c:1559 | `cmd_update_v2` | C |
| storage.c:1851 | `cmd_delete_v2` | C |
| storage.c:2091 | `multi_exists_shard_worker` | C (self-acquire) |
| storage.c:2447 | `multi_get_shard_worker` | C (self-acquire) |
| query_bulk.c:971 | `bulk_insert_shard_worker_v2` | C (self-acquire) |
| query_bulk.c:1561 | `bulk_ins_run` | C |
| query_bulk.c:2297 | `bulk_ins_delim_run` | C |
| query_bulk.c:2645 | `bulk_del_shard_worker_v2` | C (self-acquire) |
| query_bulk.c:3309 | `bulk_upd_shard_worker_v2` | C (self-acquire) |
| query_bulk.c:3810 | `bulk_upd_delim_shard_worker_v2` | C (self-acquire) |
| query_bulk.c:4373 | `bulk_upd_json_shard_worker_v2` | C (self-acquire) |
| query_bulk.c:5358 | `cmd_bulk_delete_criteria` | H (→ `BulkDelCritShardWork.sdb`) |
| query_find.c:260 | `scan_dispatch` | C |
| query_find.c:308 | `read_record_ref` | C |
| query_find.c:329 | `read_record_ref_try` | C |
| query_find.c:1327 | `cmd_exists` | C |
| query_find.c:1395 | `cmd_keys` | C |
| query_find.c:1737 | `cmd_fetch_v2` | C |
| query_aggregate.c:105 | `keyset_emit_agg` | C |
| query_aggregate.c:1643 | `wfc_worker` | C (self-acquire) |
| query_aggregate.c:2490 | `shard_agg_worker` | C (self-acquire) |
| query_aggregate.c:2794 | `agg_run_plan` | H (→ `AggKfShardArg.sdb` via `parallel_agg_scan_shards_o_direct`) |
| query_aggregate.c:4663 | `cmd_aggregate_do` | C |
| query_aggregate.c:5196 | `cmd_aggregate_do` | C |
| index.c:2676 | `build_trigram_pass` | H (interior `sdb->data_dir` via `SegScanWorker`) |
| index.c:2711 | `build_btree_streaming` | H (same) |
| index.c:2878 | `build_bitmap_pass` | H (→ `BmShardWalkArg.sdb`) |
| index.c:4297 | `build_indexes_streaming_multi` | H (same as 2676) |
| query_maint.c:133 | `cmd_estimate_index` | C |
| query_maint.c:207 | `cmd_vacuum` (light path) | C |
| query_maint.c:891 | `cmd_recount` | C |
| server.c:2595 | `warmup_object_open` | T (returns sdb to the warmup phase-1 caller) |
| server.c:2704 | `warmup_kf_task_fn` | C (self-acquire under its own rdlock; copies `kf_path`/`slots_per_shard` out and never touches sdb after `objlock_rdunlock` — guard applies) |
| query_schema.c:609 | `cmd_edit_fields` | C |
- **UNGUARDED-BY-OBJLOCK (2)** — storage.c:303
  (`resolve_counts_with_schema`) is reached from `auto_vacuum_sweep_one`
  (server.c:2974) and `auto_reshard_sweep_one` (server.c:3093) **before
  any objlock is taken**; query_schema.c:609→`cmd_drop_object`'s
  invalidate (query_schema.c:1465) runs with **no objlock at all**
  (dispatch returns at the drop-object early block, server.c:1348–1359,
  before the generic lock take at :1419–1422; `mode_is_schema`
  excludes it). These two paths are why the suppressed race is live;
  only the refcount covers the first, and Task A3's lock covers the
  second (with the refcount as structural backstop for both).
- All other invalidate callers (rebuild txn paths objlock.c:363/:402/
  :424/:430, query_find.c:1142/:1160/:1185, query_maint.c:706/:922,
  query_schema.c drop at :1465) run under objlock wrlock inherited from
  their dispatchers — verified per-site.

## Root causes

### Tier A — `race:slotcask_registry_invalidate` (real UAF, two live paths)

`slotcask_registry_get` returns the raw `SlotcaskDb*` after releasing
`g_reg_lock`; `slotcask_registry_invalidate` closes and frees it. The
objlock contract (objlock.c:5–31: rdlock for every registry-reaching
mode, wrlock for every invalidate caller) has two violators (inventory:
UNGUARDED-BY-OBJLOCK), so the free genuinely races concurrent
dereferences today.

**Fix:** refcount `SlotcaskDb` (structural — closes both paths and all
future ones) + give drop-object its objlock wrlock at its dispatch site
(contract repair, matching truncate/vacuum and the describe-object
precedent).

**Lifetime protocol and invariants (new in v2):**

1. **Refcount invariant.** Install publishes TWO references — the
   table's and the opener-caller's (the opener thread returns `db` to
   its caller, who releases it with `put` like every other acquirer);
   the hit path adds one caller reference under `g_reg_lock` before
   handing the pointer out. `invalidate` unlinks
   the entry (no new gets can reach it), flushes the cache prefixes
   immediately, and drops the table reference. The struct is
   `slotcask_close`+`free`d by whichever `put()` transitions the count
   1→0 — never while any caller reference (opener's or hit-path
   acquirer's) exists. `slotcask_close`
   (slotcask.c ~:4165 area, body quoted in Task A2) only destroys the
   struct's own mutexes and frees its own heap arrays — no munmap, no
   file closes — so it is safe on the last-putter thread.
2. **Shutdown quiescence invariant.** `slotcask_registry_shutdown`
   participants in the protocol (Task A2 replaces its body). Proof of
   quiescence per call site:
   - Graceful daemon stop (server.c ~:3726): `close(sfd)` → join all
     worker threads → drain in-flight writes → `bg_threads_stop` (joins
     auto-vacuum, auto-reshard, warmup) → join both IO pools → then
     `slotcask_shutdown()`. Worker threads exit only after finishing
     their current command, and every command's registry reference is
     released within that command's scope (guard or explicit put), so
     the table reference is provably the last one: shutdown's `put`
     frees inline.
   - Startup-failure path (server.c ~:3624): bg threads partially
     started are stopped and pools joined before shutdown; accept loop
     and worker spawn happen only after this block.
   - `shard_db_open` failure paths (embedded.c ~:204): no pools/bg
     threads exist yet, or they are stopped first (~:775–777).
   - **The known non-quiescent caller — quiescence requirement
     preserved, NOT cured:** `shard_db_close` (embedded.c ~:912)
     explicitly does NOT join application threads, and after registry
     shutdown it tears down the global kfcache/segcache subsystem. A
     straggler embedder thread holding a reference across
     `shard_db_close` may keep its `SlotcaskDb` struct alive under the
     refcount, but its next cache acquire touches destroyed global
     cache state — the refcount protects only the struct, within the
     cache subsystem's lifetime, and cannot protect against global
     teardown. This plan therefore does **not** claim deferred-free
     safety for that case: `shard_db_close`'s documented contract
     ("Final instance teardown after callers have stopped background
     threads, pools, and mmap caches", embedded.c ~:878–880) is the
     existing explicit quiescence requirement, it is preserved verbatim,
     and it remains load-bearing exactly as before. What the protocol
     changes for a contract-violating embedder is only that the failure
     is no longer a use-after-free **of the registry struct**; it is
     the same destroyed-cache failure class as status quo. A shutdown
     barrier that rejects/waits for in-flight queries is explicitly out
     of scope (guardrails).
3. **Cache-prefix invalidation vs outstanding users.** Both
   `kfcache_invalidate_prefix` and `segcache_invalidate_prefix` (a)
   **block** on each matching entry's rwlock — they wait out every
   active handle holder rather than skipping or racing them (kfcache:
   `pthread_rwlock_wrlock(&e->rwlock)` under the table lock;
   segcache: `wait=1` wrlock "exclude any thread still holding this
   slot's rwlock … before munmapping under it"); and (b) bump the
   entry generation after unmapping, so every cached `SlotRef` fails
   its gen check. A user that reacquires after invalidation takes the
   slow path, which re-opens **by path** — and segcache additionally
   stats and compares dev/ino, evicting any cached entry whose inode no
   longer matches the path. Therefore no outstanding or future user can
   observe a stale mapping: active handles are waited out; cached refs
   are poisoned; reacquisition gets the current file or ENOENT.
   Post-A3 this is belt-and-suspenders for the drop path: the
   drop-object wrlock excludes any reader from issuing new acquires
   mid-drop at all. The DISCARD-does-not-flush semantics of prefix
   invalidation are pre-existing ("the caller is deleting this file or
   has durably published its replacement") and unchanged. The wire
   regression (Task A1b) asserts the user-visible consequence: a read
   concurrent with drop never returns stale or mixed data — it blocks
   and then reports the object gone.

### Tier B — segment byte-18 family (5 race lines): root cause established

Planning-time audit (complete; every access to record bytes 0..23 in
src/db enumerated — 7 writers, 25 readers; full matrix embedded in Task
B3). The design is flag-ordered release/acquire and **one genuinely
unordered path exists**:

**R11 — `slotcask_bulk_lookup_in_kfshard` phase 2 (slotcask.c ~:6087–6097).**
Phase 1 probes the kf under one held kf reader and **releases it at
~:6044**; phase 2 then reads byte 18 (acquire) plus klen 16–17 and the
key bytes (plain) under a segcache rdlock **only**. A durability window
holding kf-wr can tombstone (T) that slot, return it to the pool (C),
and re-emit a different record into the same offset (P,
`seg_record_emit_pending` — a plain memcpy over bytes 0..23 that holds
**no kf lock** in the P-wave path) while phase 2 is still reading.
Plain-vs-plain, zero common lock: a real data race, and a violation of
the landed 2026-08-21 contract ("Kf read handle stays live until the
segment record has been checked against its hash/key and copied into
caller-owned memory") — the same defect class fixed for
`slotcask_bulk_fetch_resolved` and `slotcask_get`, missed here. The
suppressed `race:slotcask_get` / `race:seg_rec_klen` / `race:seg_rec_vlen`
reports map to this family (and to pre-window-tree revisions of paths
this branch has since reworked).

Every other reader/writer pair is lock-ordered (matrix in Task B3):
readers hold kf-rd (or kf-wr in replay contexts) across their seg
reads; writers of live bytes hold kf-wr; the P-wave's unlocked staging
touches only pool-free offsets that are quiescent by the T→C protocol
(any kf-locked reader would have blocked the T that freed them); and
both `seg_scan_o_direct` callers run strictly under objlock wrlock
(reindex: index.c:4660 explicit, "The lock queues them until reindex
completes"; varlen-compact donor: vacuum's dispatch wrlock), with no
online-reindex-with-concurrent-writers path in the tree.

**Fixes (concrete, Task B1/B2):** hold the kf reader across phase 2
(R11); convert the two plain byte-18 loads in io_direct.c to acquire
and the two RELAXED byte-18 zero-stores to RELEASE (uniform pairing;
zero-cost on x86_64; removes the "plain load" class permanently).
`seg_scan_varlen.h`'s plain header reads stay plain — every call site
is obj-wr- or open-time-exclusive (documented in the matrix).

### Tier C — `deadlock:bt_acquire` (detector-modeling artifact; one real edge shape identified)

No thread holds M0 (`bt_cache_lock`) while acquiring any per-slot
rwlock, or vice versa, at any site: `bt_cache_evict_slot` releases M0
before taking the rwlock and retakes it after (btree.c:529–532);
`bt_release` takes no M0; both `bt_acquire_impl` paths release M0
before taking the rwlock (btree.c:818, :1015). The real lock graph is
acyclic. The one sequence TSan's detector could be keying on:
`btree_walk_ordered_ranges`' open/resume loop (btree.c:3795–3822) holds
the already-opened iters' per-slot rwlocks while each subsequent
`btree_range_iter_open` → `bt_acquire` probes M0 — a genuine
M1-held→M0 acquisition shape (still not a cycle partner with any
M0→M1). Task C1 captures the actual report; Task C2 supplies the
complete batch-open fix (fully specified install helper; the helper is
total after scratch allocation, so per-file failures degrade per shard
exactly like the sequential path and nothing can leak a held rwlock)
and a human escalation checkpoint for the hypothetical case where the
report survives it. The v3 "pass-loop transform" rung was
removed: it was CFG-identical to the goto restarts and could not change
what the detector models, and its bounded variant would have added a
new writer EAGAIN failure mode.

---

## Tasks

### Task 0 — Re-fire matrix (empirical scoping; audit already done at planning time)

1. `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`.
2. Comment out **all six** lines of `.tsan.supp` (keep the file; each
   line is permanently deleted only by the task that owns it).
3. Check /tmp headroom (`df -h /tmp`; PLAN_NOTES 2026-08-28 guidance:
   remount larger if below ~28G), then:
   `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all`.
4. Record in PLAN_NOTES: per suppression line — fired (report excerpt:
   both stacks + lock graph) or not fired. Task 0 owns no deletions;
   restore all six lines after the run.

Interpretation is now bounded by the planning-time audit: any Tier B
report must name a site from the Task B3 matrix (R11's family is the
expected live one); any Tier C report must involve `bt_acquire` frames.
A report outside those sets is a stop-and-ask per the embedded rules.

**Recorded outcome (executed 2026-08-29, see PLAN_NOTES): zero
TSan/race/lock-order reports fired with all six lines commented; the
file was restored.** Consequences, per the task rules above: the five
Tier B lines take the dormant-rationale deletion path (B3), and Tier C
is expected to close by evidence at C1. This does NOT skip Task B1 or
B2: R11 is an audit-established real defect whose window the suite
does not naturally hit (B1's pause-hook test supplies the
deterministic red), and the B2 conversions are mandated regardless of
re-fire status.

### Task A1a — Regression test `test-registry-uaf-invalidate` (refcount red)

New file `src/test/cases/test_registry_uaf_invalidate.c`, registered in
`build.sh` beside the other case registrations (anchor: quote the two
nearest `TEST_REGISTER` case-file entries in build.sh as the insertion
point). Fixture: runner-process `ShardDb`, direct internal calls (the
`test_reindex_bitmap_resplit.c` pattern: `tu_pdb_*` fixture helpers +
direct `slotcask_registry_get`; object removed at case end).

Body — **base-era APIs only**: the test must compile and run on the
unmodified base, so it uses just `slotcask_registry_get` and
`slotcask_registry_invalidate`. It deliberately does NOT call
`slotcask_registry_put` — that API is introduced by Task A2, and a
test calling it cannot compile on base (the v8 flaw that halted
execution, recorded in PLAN_NOTES 2026-08-29). The red race does not
need a release call: it is the dereference-vs-free window itself.

- Create object `uaf_obj` under the case's temp root (simple one-int
  schema, a few rows).
- 4 reader threads loop until stopped: `slotcask_registry_get(root,
  "uaf_obj", &info)` → touch the struct (`sdb->num_shards` into a
  local; `memcpy` of `sdb->data_dir` into a 256-byte local) →
  `usleep(200)`. No release call — on base the pointer is borrowed by
  contract; post-A2 the loop gains its `put` (Task A2's wiring below).
  Readers deliberately take **no objlock** — this hammers the
  storage.c:303 sweep shape, the path only the refcount can close.
- Main thread: 2000 × `slotcask_registry_invalidate(root, "uaf_obj")`
  + `usleep(500)`.
- Join readers, drop the object, assert no assertion tripped.

Transient-state note: between A1a landing and A2's conversion of this
file, the test's reader loops accumulate references under the new
protocol (every get adds one, nothing releases). That window exists
only inside this execution run — references are bounded by the test's
iteration count, `slotcask_registry_shutdown` still frees everything
at process exit, and no other code path observes the counter. It is
closed by A2's wiring step, which converts this test's readers and is
a required part of A2's verification.

**Red-on-base proof — SATISFIED 2026-08-29 (see PLAN_NOTES).** With
only `race:slotcask_registry_invalidate` commented in `.tsan.supp`,
the TSan build produced exactly the expected race: a reader
dereferencing `SlotcaskDb` while `slotcask_registry_invalidate` runs
`slotcask_close`/`free` on the same allocation. That captured report
IS the red-on-base proof — do not rerun it. (The partial, uncommitted
test + its build.sh registration remain in the tree from that run and
stay as-is until A2 converts them.)

**There is NO suppressed-base pass, and none is required.** The v8/v9
requirement "with the line restored, the case must pass" was a design
error: a TSan suppression hides the report, not the underlying
use-after-free. On the base tree this stress case exercises a genuine
UAF and its suppressed behavior is undefined — a hang, corruption, or
crash are all valid outcomes (the 2026-08-29 attempt in fact left
workers live for many minutes). Do NOT run the suppressed case on
base again; if any workers from the halted attempt are still alive,
kill them and verify no stray test processes remain before touching
the tree further. The case is a red test by nature: on base it fails
for exactly the expected reason, and that is the proof.

**Green condition (post-A2 only, both suppression states):** after
Task A2 — by which point its wiring has converted this test's readers
to the guard/put form — the case must be silent and green (1) with
the line still commented, proving the refcount rather than the
suppression closes the race, and (2) with the line restored, proving
the fix rather than the suppression is load-bearing under normal gate
conditions. Post-A2 both runs are safe and deterministic because the
underlying race no longer exists. (The converted form is the
contract-honest final state and the one A2 pastes; the unconverted
accumulate-only form would also be green post-A2, since accumulate-
only references are never freed.)

### Task A1b — Wire-level regression `test-drop-object-read-wire` (public path, overlap proven)

**EXECUTE NOW, between A2 and A3** (flow correction, 2026-08-29: the
halt-driven reordering landed A2 before A1b was written, and A3 must
not start without A1b's red proof). A1b's red needs only A3's absence
— the marker the test waits for is written by A3's dispatch hunk, so
on the current tree (A2 in, A3 out) the marker never appears and the
case fails natively at `marker_seen`, exactly as designed. A2's
refcount is irrelevant to this assertion.

New file `src/test/cases/test_drop_object_read_wire.c`, registered in
build.sh. Modeled line-for-line on `test_read_objlock_contention.c`
(daemon on a free port + tmpdir, two rounds, `tc_send`/`tc_recv`
in-flight concurrency, marker-file synchronization). Sequence:

1. **Round 1** (plain daemon): add-dir `default`; create-object
   `dropuaf` (`splits:8, max_key:16, fields:["name:varchar:16"]`);
   bulk-insert ~2000 rows via one `{"mode":"bulk-insert",...}` wire
   request; stop daemon.
2. **Round 2** (daemon restarted with
   `SCHEMA_WRLOCK_TEST_DELAY_MS=2000` in db.env — the existing test-only
   knob, now also honored by drop-object via Task A3's hunk): fire the
   drop **asynchronously** (`tc_send`,
   `{"mode":"drop-object","dir":"default","object":"dropuaf"}`), then
   poll for the synchronous marker file
   `<root>/default/.schema-wrlock-test-delay-dropuaf.active`
   containing `mode=drop-object` (10 s budget), asserting it appears —
   this is the overlap proof: drop holds the object wrlock and is
   parked inside it.
3. While the marker exists, `tc_send` a JSON `get` and a JSON `find`
   on the same object, each on its own connection, both in flight
   before either response is awaited.
4. Await both. Assertions: each response arrives **only after** the
   marker is gone (`access(marker, F_OK) != 0` at response time) and
   elapsed ≥ DELAY_MS/2 (both blocked behind the held wrlock); both
   return a clean `{"error":... "not found"}`-class response — never a
   crash, hang, or stale row (the drop completes before the blocked
   readers are granted the rdlock).
5. Cleanup: drain the drop response, stop daemon, rm -rf tmpdir.

**Red-on-base:** base drop-object takes no lock, so the marker never
appears — `marker_seen` fails natively and deterministically (paste).
Under TSan on base the same run additionally exposes the
`slotcask_registry_invalidate` race window (paste if fired). **Green
condition:** after Task A3 (marker + blocking + clean not-found). Note
the split: A1a guards the refcount (A2); A1b guards the lock contract
(A3).

### Task A2 — Refcount `SlotcaskDb` + shutdown participation + wire every call site

**slotcask.h — struct field.** Anchor (quoted, inside
`typedef struct SlotcaskDb {`):

```c
    int     bulk_commit_window; /* records per commit window; 0 = default
                                   (1024). db.env BULK_COMMIT_WINDOW. */
```

append after it:

```c
    /* Registry reference count. Managed ONLY by slotcask_registry_get
       (add under g_reg_lock), slotcask_registry_put (sub), and
       slotcask_registry_invalidate / _shutdown (unlink + drop the
       table's ref). The struct is closed+freed by whichever put()
       transitions 1→0; see the lifetime invariants in
       docs/plans/2026-08-28-eliminate-tsan-supp.md. */
    uint64_t reg_refs;
```

**slotcask.h — contract text.** Replace the sentence (quoted):

```
 * The pointer is BORROWED — never call slotcask_close on it. The registry
 * owns lifetime.
```

with:

```
 * The pointer is REFCOUNTED: each slotcask_registry_get() call takes one
 * reference that the caller MUST release exactly once with
 * slotcask_registry_put() (or the SDB_REG_REF scoped guard below) when
 * the pointer is no longer needed — including on early-return paths.
 * slotcask_registry_invalidate() unlinks the entry and flushes the
 * kfcache/segcache prefixes immediately; the struct itself is closed and
 * freed by the last put(). Only pointers obtained from
 * slotcask_registry_get() are put()-able: a SlotcaskDb opened directly
 * via slotcask_open() keeps its existing open/close pairing (rebuild's
 * legacy_db/new_db do NOT go through the registry).
```

**slotcask.h — new API + guard.** Immediately after the
`slotcask_registry_get(...)` declaration, add:

```c
/* Release one reference taken by slotcask_registry_get(). NULL tolerated.
   The last put() (transition 1→0) runs slotcask_close + free. */
void slotcask_registry_put(SlotcaskDb *db);

static inline void slotcask_registry_ref_release(SlotcaskDb **pp) {
    if (pp) slotcask_registry_put(*pp);
}

/* Scoped guard: `SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(...)`
   releases the reference on every exit path of the enclosing block. Use
   ONLY where the pointer's use is confined to that block, including
   join-synchronous worker fan-outs the block waits on (parallel_for /
   parallel_for_io). Ownership handoffs (warmup_object_open) and
   loop/conditional reassignment sites must not use the guard — see the
   plan's Task A2 wiring rules. */
#define SDB_REG_REF __attribute__((cleanup(slotcask_registry_ref_release)))
```

**slotcask.c — registry_get hit path.** Anchor:

```c
        if (g_reg[slot].used) {
            SlotcaskDb *db = g_reg[slot].db;
            pthread_mutex_unlock(&g_reg_lock);
            return db;
        }
```

replacement:

```c
        if (g_reg[slot].used) {
            SlotcaskDb *db = g_reg[slot].db;
            /* Take the caller's reference under the table lock; pairs with
               slotcask_registry_put's final-sub free, so a concurrent
               invalidate can unlink the entry but not free the struct. */
            __atomic_add_fetch(&db->reg_refs, 1, __ATOMIC_ACQ_REL);
            pthread_mutex_unlock(&g_reg_lock);
            return db;
        }
```

**slotcask.c — registry_get opener success path.** Anchor:

```c
        g_reg[reserved].db = db;
        g_reg[reserved].used = 1;
```

replacement:

```c
        g_reg[reserved].db = db;
        /* Two references published at install: the table's and this
           thread's caller reference — the opener RETURNS db to its
           caller, who releases it with slotcask_registry_put like
           every other acquirer. A concurrent invalidate that unlinks
           the entry and drops the table ref can then never free the
           struct while the opener's caller is still using it. */
        __atomic_store_n(&db->reg_refs, 2, __ATOMIC_RELAXED);
        g_reg[reserved].used = 1;
```

**slotcask.c — new put.** Directly after `slotcask_registry_get`:

```c
void slotcask_registry_put(SlotcaskDb *db) {
    if (!db) return;
    /* ACQ_REL: publish our prior reads of db, acquire the last owner's
       release, so the struct is quiescent when we close+free it. */
    if (__atomic_sub_fetch(&db->reg_refs, 1, __ATOMIC_ACQ_REL) == 0) {
        slotcask_close(db);
        free(db);
    }
}
```

**slotcask.c — invalidate.** Replace everything from
`pthread_mutex_lock(&g_reg_lock);` to the second
`segcache_invalidate_prefix(data_dir);` (the function tail after the
`reg_key`/`data_dir` setup) with:

```c
    pthread_mutex_lock(&g_reg_lock);
    int slot = reg_probe(key);
    SlotcaskDb *db = NULL;
    if (slot >= 0 && g_reg[slot].used) {
        db = g_reg[slot].db;
        g_reg[slot].used = 0;
        g_reg[slot].key[0] = '\0';
        g_reg[slot].db = NULL;
    }
    pthread_mutex_unlock(&g_reg_lock);

    /* Flush cached kf + seg mmaps for this data_dir unconditionally and
       immediately — rebuild depends on a fresh slotcask_open not hitting
       cached entries for moved-away inodes. Both prefix walks block on
       each matching entry's rwlock and bump its generation, so active
       handle holders are waited out and cached SlotRefs are poisoned
       (slow-path reacquisition re-opens by path; segcache re-verifies
       dev/ino). This touches only the cache tables, not the SlotcaskDb
       struct, so it is safe while stragglers still hold references. */
    kfcache_invalidate_prefix(data_dir);
    segcache_invalidate_prefix(data_dir);

    /* Drop the table's own reference; the last puter closes+frees. */
    if (db) slotcask_registry_put(db);
```

**slotcask.c — shutdown participates in the protocol.** Replace the
body of `slotcask_registry_shutdown` (anchor):

```c
    pthread_mutex_lock(&g_reg_lock);
    for (int i = 0; i < SLOTCASK_REG_BUCKETS; i++) {
        if (g_reg[i].used && g_reg[i].db) {
            slotcask_close(g_reg[i].db);
            free(g_reg[i].db);
            g_reg[i].db = NULL;
            g_reg[i].used = 0;
            g_reg[i].key[0] = '\0';
        }
    }
    pthread_mutex_unlock(&g_reg_lock);
```

with:

```c
    pthread_mutex_lock(&g_reg_lock);
    for (int i = 0; i < SLOTCASK_REG_BUCKETS; i++) {
        if (g_reg[i].used && g_reg[i].db) {
            SlotcaskDb *db = g_reg[i].db;
            g_reg[i].db = NULL;
            g_reg[i].used = 0;
            g_reg[i].key[0] = '\0';
            /* Participate in the refcount protocol instead of closing
               unconditionally. Daemon stop paths join workers, background
               threads, and both IO pools BEFORE calling shutdown
               (server.c: close sfd → join workers → drain in-flight
               writes → bg_threads_stop → pool shutdowns → slotcask_
               shutdown), so every caller reference is provably released
               and this put() closes+frees inline. Scope note: this
               protects the SlotcaskDb struct only. A non-daemon embedder
               that violates shard_db_close's documented quiescence
               contract (embedded.c: teardown after callers have stopped
               background threads/pools/caches) still races the global
               kfcache/segcache teardown with a live query — the
               refcount does not and cannot cover global teardown; the
               contract remains mandatory and unchanged. */
            pthread_mutex_unlock(&g_reg_lock);
            slotcask_registry_put(db);
            pthread_mutex_lock(&g_reg_lock);
        }
    }
    pthread_mutex_unlock(&g_reg_lock);
```

**TLS `g_db` bind (execution-discovered, 2026-08-29).** `g_db` is
thread-local (`__thread ShardDb *g_db`, embedded.c:9) and the registry
lock/cond/table are macros off it. Threads created outside the
embedded/server bind paths (A1a's raw reader pthreads, any direct API
caller) start with TLS `g_db == NULL` and would lock/cond-wait on
garbage low addresses — bypassing the registry protocol entirely while
appearing to run (the A2 gate hang: three readers asleep on a garbage
condvar, 2002 broadcasts on the real one, `cond_woke: 0`). Add a lazy
bind mirroring the objlock.c:41 precedent (slotcask.c:507/:1525 already
do this for the caches):

```c
/* The registry table, lock, and cond live in the g_db ShardDb, which is
   THREAD-LOCAL (embedded.c __thread g_db). Threads spawned outside the
   embedded/server bind paths (direct API callers, test pthreads) start
   with TLS g_db == NULL and would otherwise lock/cond-wait on garbage
   addresses — silently bypassing the registry protocol. Bind lazily
   from the process instance, same precedent as objlock.c's g_db guard. */
static void registry_bind_g_db(void) {
    if (!g_db && g_shard_db_instance) g_db = g_shard_db_instance;
}
```

Call it at the top of `slotcask_registry_get` (after the `info`
validation), `slotcask_registry_invalidate` (before `reg_key`), and
`slotcask_registry_shutdown` (before the lock). `put` needs no bind (it
touches only the passed struct). Verified 2026-08-29: A1a green 2×
under TSan with the Tier A line commented (~14 s each), registry family
6/0, warmup family 28/0.

**Wiring — the 55 production sites + 1 test site.** Patterns:

- CONFINED + join-synchronous handoff (54 sites — every C and H row in
  the inventory table, including both loop-shaped `shard_count_worker`
  sites per their caveat and `warmup_kf_task_fn`): one-line change —
  `SlotcaskDb *sdb = slotcask_registry_get(...)` →
  `SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(...)`. Early
  returns are covered by the guard; `put(NULL)` tolerates failed gets.
  For the C-loop sites the declaration+guard moves inside the loop
  body (or per-iteration explicit put).
- TRANSFER (1 site — server.c `warmup_object_open`): no guard. The
  acquirer holds the plain reference and returns it; its single caller
  (warmup phase-1 loop) calls `slotcask_registry_put(sdb)` strictly
  inside its existing objlock_rdlock window after its last use,
  mirroring the "read under rdlock, copy out, unlock" precedent.
- LOOP-SHAPED (2 sites — query.c:938/:1065 `shard_count_worker`): the
  cleanup attribute fires once per scope, so re-assignment in a loop
  leaks one reference per extra assignment. Move the
  declaration+guard inside the loop body (preferred), or keep the
  declaration outside and `slotcask_registry_put` at the end of every
  iteration including error paths.
- Test sites (2): `test_reindex_bitmap_resplit.c:60` — confined, add
  the guard, with the `put` landing before the case's
  `tu_pdb_drop_object` teardown; and Task A1a's
  `test_registry_uaf_invalidate.c` — convert the reader loops to the
  guard (or explicit put) now, closing the transient
  accumulate-only window A1a documented. This conversion is what makes
  verification step 4 below honest.
- Guard reassignment warning applies nowhere else: verified no other
  site reassigns a registry pointer.

**Task verification (all output pasted to PLAN_NOTES):**
1. Re-run the inventory grep from "Verified inventory" and reconcile
   against the table — **halt if counts or sites differ** (tree drift
   under concurrent work is a real possibility; a mismatch means the
   plan's table is stale and must be re-derived by the planner).
2. `grep -rn 'slotcask_registry_get[[:space:]]*(' src/ | grep -v
   'SlotcaskDb \*slotcask_registry_get(const' | grep -v '^\S*: *\*'` —
   every call-expression hit must sit within 3 lines of an
   `SDB_REG_REF` declaration or a paired `slotcask_registry_put`.
   Record the arithmetic: 55 production + 2 test files
   (`test_reindex_bitmap_resplit.c`, A1a's
   `test_registry_uaf_invalidate.c`) = 57 wired; 56 guarded (54
   production + 2 test); 1 explicit-put transfer
   (`warmup_object_open`).
3. `SKIP_TESTS=1 ./build.sh` — zero new warnings.
4. A1a green and silent under TSan with the Tier A line still
   commented, in its POST-CONVERSION form (readers put) — paste.
5. Families: `--filter warmup --filter registry --filter reindex
   --filter bulk --filter find` green (native).

### Task A3 — Drop-object takes its objlock wrlock at dispatch (with test-delay marker)

**Precondition: Task A1b is in the tree, registered, and its
red-on-base run has been captured (native failure at `marker_seen`)
BEFORE any A3 hunk is applied.** Do not start here without it.

In `dispatch_json_query`'s early drop-object block (server.c, anchor):

```c
    if (strcmp(mode, "drop-object") == 0) {
        char *ie_s = json_obj_strdup(&req, "if_exists");
        int if_exists = json_obj_is_true(&req, "if_exists") ||
                        (ie_s && strcmp(ie_s, "1") == 0);
        cmd_drop_object(g_db_root, dir, object, if_exists);
        free(ie_s);
        free(mode); free(dir); free(object);
        return;
    }
```

replacement — **corrected 2026-08-30 after the executor's compile
halt: the early branch runs BEFORE the generic block declares
`db_root`, so the branch builds its own local effective root, exactly
like the `restore` special case directly below it:**

```c
    if (strcmp(mode, "drop-object") == 0) {
        char *ie_s = json_obj_strdup(&req, "if_exists");
        int if_exists = json_obj_is_true(&req, "if_exists") ||
                        (ie_s && strcmp(ie_s, "1") == 0);
        /* drop-object frees the registry entry (slotcask_registry_
           invalidate) — it needs the object wrlock like every other
           invalidate caller (objlock.c contract; truncate/vacuum get
           theirs from mode_is_schema, but drop-object returns before
           that generic take). The effective root is NOT built until the
           generic block below, so build our own — same construction and
           therefore the same (eff_root, object) lock key readers use. */
        char drop_eff_root[PATH_MAX];
        build_effective_root(drop_eff_root, sizeof(drop_eff_root), dir);
        objlock_wrlock(drop_eff_root, object);
        if (g_db && g_schema_wrlock_test_delay_ms > 0) {
            /* Same test-only synchronous delay+marker convention as the
               generic wrlock block below: proves to tests that the
               wrlock is HELD right now (marker exists) and released
               when it is gone. */
            char marker_path[PATH_MAX];
            snprintf(marker_path, sizeof(marker_path),
                     "%s/.schema-wrlock-test-delay-%s.active",
                     drop_eff_root, object);
            FILE *mf = fopen(marker_path, "w");
            if (mf) {
                fprintf(mf, "mode=drop-object\nobject=%s\n", object);
                fclose(mf);
            }
            struct timespec delay_ts = { g_schema_wrlock_test_delay_ms / 1000,
                                         (long)(g_schema_wrlock_test_delay_ms % 1000) * 1000000L };
            nanosleep(&delay_ts, NULL);
            unlink(marker_path);
        }
        cmd_drop_object(g_db_root, dir, object, if_exists);
        objlock_wrunlock(drop_eff_root, object);
        free(ie_s);
        free(mode); free(dir); free(object);
        return;
    }
```

Notes: the lock key matches readers because the generic block later
builds the effective root from `dir` the identical way. The delay hunk
mirrors the existing `took_wrlock` block (same knob, same marker
convention, `mode=drop-object` payload). Place nothing inside
cmd_drop_object itself so direct test callers stay unlocked (A1a
covers them via the refcount). **Status: landed and verified —**
A1b red 7/1 pre-hunk (`not ok 8 - drop-object wrlock marker observed`),
A1b green 19/0 post-hunk, objlock family 46/0, A1a 2/0 (see
PLAN_NOTES 2026-08-30).

**Verification:** A1b green end-to-end (marker appears; both reads
block; both return clean not-found) — paste. `--filter drop --filter
objlock` families green; A1a still green. Native `run-all` green.

Then delete the Tier A line permanently (remove the commented
`race:slotcask_registry_invalidate` from `.tsan.supp`). Re-run the TSan
warmup/registry/drop/bulk families — zero reports naming
`slotcask_registry_invalidate`. Paste runs.

### Task B1 — Fix R11: hold the kf reader across the bulk-lookup verify

**Production change (slotcask.c, `slotcask_bulk_lookup_in_kfshard`).**
Hunk 1 — delete the early kf release (anchor):

```c
    kfcache_release(&kh);

    /* Phase 2: batched verify_stored_key — sort kf-hits by (sid, fid),
```

replacement:

```c
    /* The kf reader stays held through phase 2: the window contract
       ("Kf read handle stays live until the segment record has been
       checked against its hash/key and copied into caller-owned
       memory") requires it. Released here, a window could tombstone (T)
       and re-emit (P) this slot while phase 2 reads it under an
       independent segcache rdlock — plain-vs-plain, zero common lock.
       Same discipline as slotcask_get and kf_reval_fetch_one. */

    /* Phase 2: batched verify_stored_key — sort kf-hits by (sid, fid),
```

Hunk 2 — release after the last use (anchor):

```c
    free(st);
    return 0;
}
```

(careful: this exact trio appears once at the end of THIS function —
verify uniqueness; if not unique, extend the anchor upward to include
the OOM-fallback closing brace) — replacement:

```c
    kfcache_release(&kh);
    free(st);
    return 0;
}
```

The early OOM return (`if (!st) { kfcache_release(&kh); return -1; }`)
already releases — unchanged. Deadlock check: phase 2 takes segcache
rdlocks under the held kf reader — the universal kf→seg order; nothing
in the path takes a kf lock while holding a segcache handle.

**TEST_BUILD pause hook** (deterministic red/green). In
`src/db/shard_test_ctl.h`, inside the `#ifdef TEST_BUILD` block next to
the existing pause knobs, add:

```c
extern _Atomic int g_shard_test_bulk_lookup_gap;        /* 1 = armed */
extern _Atomic int g_shard_test_bulk_lookup_gap_hit;
extern _Atomic int g_shard_test_bulk_lookup_gap_release;
```

(extend `shard_test_ctl_reset()` with the three stores: gap=0, hit=0,
release=0). In slotcask.c's TEST_BUILD definitions block (where the
other `g_shard_test_*` objects are defined), add the three definitions.
In `slotcask_bulk_lookup_in_kfshard`, immediately after the phase-1
loop's closing brace and BEFORE the (now-commented-anchor) phase-2
block, add:

```c
#ifdef TEST_BUILD
    /* Regression hook (docs/plans/2026-08-28-eliminate-tsan-supp.md
       Task B1): parks the caller after the probe phase so a test can
       run a full window's worth of slot churn in the gap. Post-fix the
       kf reader is still held here, so any window T step in the gap
       blocks — which is exactly the assertion the test makes. */
    if (atomic_load(&g_shard_test_bulk_lookup_gap) &&
        atomic_fetch_add(&g_shard_test_bulk_lookup_gap_hit, 1) == 0) {
        while (!atomic_load(&g_shard_test_bulk_lookup_gap_release))
            nanosleep(&(struct timespec){0, 1000000L}, NULL);
    }
#endif
```

**Regression test `test-bulk-lookup-kf-held-gap`**
(`src/test/cases/test_bulk_lookup_kf_held_gap.c`, registered in
build.sh): runner-process fixture; create object (one int field, no
indexes); insert key `gapkey` hashed to shard S (compute with
`compute_hash_raw` + `compute_record_shard` — the
`test_reindex_bitmap_resplit.c` pattern); fetch `sdb` via
`slotcask_registry_get` (guarded). Arm the gap knob. Thread L:
`slotcask_bulk_lookup_in_kfshard(sdb, S, recs /* gapkey */, 1)` —
parks after phase 1. Main: poll `g_shard_test_bulk_lookup_gap_hit == 1`,
then start thread D running `slotcask_delete_with_hooks(sdb, "gapkey",
6, NULL, &res)` (NULL opts is valid — verified in the implementation:
the function substitutes a zeroed `SlotcaskDeleteOpts blank` when
`opts` is NULL). Poll up to 500 ms:
**assert D has NOT completed** (pre-fix it completes instantly —
native red); set `gap_release = 1`; join L (assert `recs[0].status ==
0` and `verified`); join D (assert success); assert
`slotcask_exists`-equivalent says gapkey is gone. Disarm knobs, drop
object.

Red-on-base: native assertion (`delete blocked behind lookup's held kf
reader`) fails pre-fix — paste both runs. Under TSan pre-fix the same
gap additionally produces the plain-vs-plain race report at phase 2 —
paste if fired. Green post-fix on both.

### Task B2 — Uniform byte-18 pairing: io_direct acquire loads, RELEASE zero-stores

io_direct.c hunk 1 (anchor `flag = carry[18];`):

```c
            flag = __atomic_load_n(&carry[18], __ATOMIC_ACQUIRE);
```

io_direct.c hunk 2 (anchor `uint8_t  flag = rec[18];`):

```c
            uint8_t  flag = __atomic_load_n(&rec[18], __ATOMIC_ACQUIRE);
```

slotcask.c, `seg_record_emit` (anchor
`__atomic_store_n(&dst[18], 0, __ATOMIC_RELAXED);`) and
`seg_record_emit_pending` (same line, second occurrence — disambiguate
by the enclosing function comments): change both to

```c
    __atomic_store_n(&dst[18], 0, __ATOMIC_RELEASE);
```

Rationale (goes in the commit/PLAN_NOTES): every byte-18 store in the
tree becomes at least RELEASE and every load ACQUIRE; readers skipping
on 0 need no ordering, so this only strengthens the pairing and
removes the mixed-ordering report class. `seg_scan_varlen.h`'s plain
header reads stay plain — every call site is obj-wr- or
open-time-exclusive (Task B3 matrix rows R6/R7/R21/R22/R23/R24/R25).

**Verification:** native `--filter find --filter agg --filter scan
--filter durability --filter bulk` green; `SKIP_TESTS=1 ./build.sh`
zero new warnings.

### Task B3 — Delete the five Tier B lines, one at a time, against the audit matrix

The completed planning-time matrix (w = writer, r = reader; every
src/db access to record bytes 0..23; verdicts from the lock-scope
audit):

| ID | Site | Bytes | Access | Locks held | Ordered? |
|---|---|---|---|---|---|
| W1 | `seg_record_emit` (only caller `varlen_compact_cb`) | 0–23 | memcpy + REL→REL(→now RELEASE) 18 | obj-wr, kf-wr, seg-rd | yes (obj-wr) |
| W2 | `seg_record_emit_pending` P-wave (`bulk_phase3_stage_pending`) | 0–23 | memcpy + 18 store | pool/rotation locks + seg-rd, **no kf** | yes — offsets are pool-free ⇒ quiescent by T→C vs all kf-locked readers; vs R11 pre-fix: **the race**, fixed by B1 |
| W3 | `seg_record_emit_pending` single (`bulk_stage_single_pending`) | 0–23 | memcpy + 18 store | kf-wr, seg-rd | yes |
| W4 | `slotcask_tombstone_mark` | 18 | RELEASE 2 | kf-wr, seg-rd | yes (atomic pair) |
| W5 | `seg_write_flag_durable` (replay/startup) | 18 | RELEASE | kf-wr, seg-rd | yes |
| W6 | batch flag loop `bulk_seg_apply_and_sync` | 18 | RELEASE per off | kf-wr, seg-rd | yes |
| W7 | `pool_split_leftover` memset | free region | memset | P-wave or kf-wr + seg-rd | yes (pool-free region) |
| R1 | `verify_stored_key` | 16–17 | plain | kf + seg-rd | yes |
| R2–R4 | marker replay readers | 18+ | ACQUIRE | kf-wr, seg-rd | yes |
| R5 | `slotcask_get` | 0–23 | ACQUIRE+plain | kf-rd held across copy | yes |
| R6/R7 | `recover_streams`, pool-rebuild worker | 0–23 | plain (`seg_scan_varlen.h`) | open-time exclusive | yes (exclusive) |
| R8–R10 | `bulk_read_old_values`, tombstone/reclaim | 0–23 / 18 / 16–23 | ACQUIRE+plain | kf-wr, seg-rd | yes |
| **R11** | `slotcask_bulk_lookup_in_kfshard` phase 2 | 18,16–17,key | ACQUIRE+plain | **seg-rd only** | **NO — fixed by B1** |
| R12–R20 | reval/walk/scan/lookup-scan readers | 0–23 | ACQUIRE+plain | kf-rd held across copy, seg-rd | yes |
| R17 | `slotcask_walk_one_shard_slots_locked` | 16–17,20–23 | plain | kf-rd + seg-rd, reindex obj-wr | yes |
| R21–R23 | seg_stat / migrate / varlen_compact_cb reads | 0–23 | plain | obj-wr | yes (exclusive) |
| R24/R25 | `seg_scan_o_direct`, `reindex_seg_cb` | 18,16–17,20–23 | plain→ACQUIRE (B2) / plain | no locks of own; callers obj-wr | yes (exclusive; B2 belt) |

Deletion procedure, one line per pass: delete the line from
`.tsan.supp` → run `--filter find --filter agg --filter scan
--filter durability --filter bulk` under TSan → zero reports naming
that symbol and families green → next line. Disposition per line:
`race:slotcask_get` and the `seg_rec_*` trio — reports (if any re-fire)
must resolve to R11-post-fix leftovers (none expected after B1) or
matrix rows already ordered (then: TSan-histogram artifact — paste the
report and STOP for the human per the embedded rules; do not
improvise a fix outside the matrix). `race:seg_record_emit` /
`race:slotcask_get` may simply not re-fire (stale against this
branch's reworked readers) — delete with the dormant-rationale
paragraph in PLAN_NOTES. Five pasted green runs (or five documented
dormant rationales) end the task.

### Task C1 — Tier C re-fire diagnostic

With the other lines already deleted, comment out `deadlock:bt_acquire`
and run `--filter find --filter cursor --filter agg --filter join
--filter reindex --filter index` under TSan. Two outcomes:

- **Zero reports** → Tier C closes by evidence: write the rationale
  paragraph (no nesting exists — evict releases M0 before M1 at
  btree.c:529–532, bt_release takes no M0, acquire paths release M0
  before M1; the historical report no longer fires on the reworked
  executors) and go to C3.
- **A report fires** → paste both stacks + the lock graph into
  PLAN_NOTES and work Task C2's ladder: apply rung C2a, re-run, and if
  the report persists, stop at the C2c escalation checkpoint with both
  stacks in PLAN_NOTES.

### Task C2 — Fix ladder (only reached if C1 fired)

The ladder has one coded rung and one human checkpoint. The v3 draft's
second rung ("C2b: pass-loop transform of the retry gotos") was
**removed in this revision**: the restarts are already taken with no
lock held, so a loop-carried transform is CFG-identical to the
goto-based restarts and cannot change what the detector models — while
its bounded-pass/EAGAIN variant would have introduced a new writer
failure mode under cache contention. A semantics-preserving C2b fixes
nothing; a semantics-changing one is out.

#### Rung 1 — C2a: batch reader-open for the ordered merge

Root cause being removed: `btree_walk_ordered_ranges` (btree.c
~:3792–3822) opens shard iters sequentially — while opening iter s+1,
iters 0..s hold their per-slot rwlocks, and each `bt_acquire` probes
`bt_cache_lock` (M0): the M1-held→M0 shape. Three complete new pieces
follow: the install helper (today the install sequence is inline in
`bt_acquire_impl`; the batch needs it callable under its own single M0
window), the batch helper itself — **total after its scratch
allocation**: per-file failures are contained to that entry, preserving
the sequential path's per-shard best-effort semantics, and there is no
fatal path after acquisition — hence no rollback and no lock-acquired
tracking (the v4 draft needed both because its miss-open failure
aborted the whole batch while cached entries were installed but not yet
locked; that design is gone) — and the iter constructor.

**New helper 1 — `bt_install_reader_locked`** (place directly after
`bt_release`, btree.c ~:1089 — NOT after `bt_cache_evict_slot`: the
helpers call `bt_open_file` (:611), `btree_cache_invalidate_nowait`
(:754), and `bt_release` (:1068), which are defined later in the file;
after `bt_release` every dependency is already in scope —
`bt_cache_probe` :475, `bt_cache_evict_slot` :518,
`bt_dispose_mapping` :564, `g_bt_publish_generation` :379, and
`durability_same_open_inode` via its types.h:581 declaration — so no
forward declarations are needed. Helper 2 goes directly below helper
1; helper 3 stays directly after `btree_range_iter_open` (its
dependencies — `iter_seek_fwd`, `bt_release` — precede that point).
Complete implementation:

```c
/* Install one already-opened reader mapping into the cache. Caller
   holds bt_cache_lock. `hint` is a probed empty-slot index, or -1 when
   the probe found no empty slot (LRU eviction then picks a victim, as
   in bt_acquire_impl's install path). `opened_generation` must have
   been captured immediately BEFORE the mapping was opened (installing
   a generation loaded later would falsely bless a pre-rename inode).
   On success returns the slot index and CONSUMES out->fd/map (the
   cache owns them); any evicted victim is returned via (vic_fd,
   vic_map, vic_sz) for the caller to bt_dispose_mapping OUTSIDE the
   table lock. On failure returns -1 with *out unconsumed (the caller
   still owns the mapping and serves it uncached). Readers only. */
static int bt_install_reader_locked(const char *path, int hint,
                                    uint64_t opened_generation,
                                    BtFile *out,
                                    int *vic_fd, uint8_t **vic_map,
                                    size_t *vic_sz) {
    int s = hint;
    *vic_fd = -1; *vic_map = NULL; *vic_sz = 0;
    if (s < 0 || bt_cache_count >= bt_cache_slots / 2) {
        s = -1;
        uint64_t floor_ts = 0;
        for (int attempt = 0; attempt < bt_cache_slots && s < 0; attempt++) {
            int lru = -1;
            uint64_t oldest = UINT64_MAX;
            for (int i = 0; i < bt_cache_slots; i++) {
                if (bt_cache[i].used == BT_CACHE_LIVE &&
                    bt_cache[i].last_access >= floor_ts &&
                    bt_cache[i].last_access < oldest) {
                    oldest = bt_cache[i].last_access;
                    lru = i;
                }
            }
            if (lru < 0) break; /* no more candidates at all */
            int drop_rc = bt_cache_evict_slot(lru, CACHE_DROP_EVICT, 0,
                                              vic_fd, vic_map, vic_sz);
            if (drop_rc == BT_EVICT_DETACHED) { s = lru; break; }
            /* BT_EVICT_BUSY / BT_EVICT_ABSENT / dirty-sync failure:
               try the next-oldest candidate. */
            floor_ts = oldest + 1;
        }
        if (s < 0) return -1;
    }
    BtCacheEntry *e = &bt_cache[s];
    strncpy(e->path, path, PATH_MAX - 1);
    e->path[PATH_MAX - 1] = '\0';
    e->fd = out->fd;
    e->map = out->map;
    e->map_size = out->map_size;
    atomic_store_explicit(&e->dirty, 0, memory_order_relaxed);
    atomic_store_explicit(&e->dirty_since_ms, 0, memory_order_relaxed);
    atomic_store_explicit(&e->validated_publish_generation,
                          opened_generation, memory_order_release);
    atomic_store_explicit(&e->used, BT_CACHE_LIVE, memory_order_relaxed);
    e->last_access = __atomic_add_fetch(&bt_cache_clock, 1,
                                        __ATOMIC_RELAXED);
    bt_cache_count++;
    return s;
}
```

(The reader-path differences vs the writer install in
`bt_acquire_impl`: no blocking `wait=1` eviction and no writer-style
first-error hard failure — a reader that cannot get a slot is served
uncached by the caller, exactly like `bt_acquire_impl`'s cache-full
reader fallback.)

**New helper 2 — `bt_open_readers_batch`** (place directly below
helper 1, after `bt_release`). Complete implementation. The helper is
**total after its scratch allocation**: the only operations that can
fail mid-acquisition are per-file opens, and each such failure is
contained to that entry — so there is no batch-aborting failure path,
no rollback, and no lock-acquired tracking (the v4 draft needed those
because its miss-open failure aborted the whole batch while cached
entries were already installed but not yet locked; that design is
gone). Per-shard best-effort matches the sequential
`btree_range_iter_open` this replaces:

```c
/* Batch reader-acquire for the ordered merge (Task C2a). Sequential
   btree_range_iter_open calls hold earlier iters' per-slot rwlocks
   while each later bt_acquire probes bt_cache_lock — an M1-held→M0
   sequence TSan's cycle detector cannot distinguish from a real
   two-lock cycle. This helper touches M0 only in windows where this
   thread holds NO per-slot rwlock, and takes rwlocks only outside
   those windows: the observed lock graph is trivially acyclic.

   Contract (per-shard best-effort, matching the sequential
   btree_range_iter_open it replaces). Returns 0 with every entry
   resolved to exactly one of:
     outs[i].slot >= 0    cached, rwlock held; the adopting iter's
                          bt_release() is the unlock
     slot == -1, fd >= 0  self-owned uncached mapping; the adopting
                          iter's bt_release() disposes it
     slot == -1, fd < 0   open failed; the caller marks that shard
                          done and merges the healthy shards
   Cached handoffs are publication-validated exactly like
   bt_acquire_impl's hit path (generation compare + inode check;
   stale entries are retired and served fresh).
   Returns -1 only if the helper's own scratch allocation fails;
   nothing has been acquired or modified in that case, and the caller
   degrades as if every open had failed. */
static int bt_open_readers_batch(const char *const *paths, size_t n,
                                 BtFile *outs) {
    if (n == 0) return 0;
    int      *slot_of = malloc(n * sizeof(int));
    int      *vic_fd  = malloc(n * sizeof(int));
    uint8_t **vic_map = calloc(n, sizeof(uint8_t *));
    size_t   *vic_sz  = calloc(n, sizeof(size_t));
    uint64_t *gens    = malloc(n * sizeof(uint64_t));
    if (!slot_of || !vic_fd || !vic_map || !vic_sz || !gens) {
        free(slot_of); free(vic_fd); free(vic_map); free(vic_sz);
        free(gens);
        return -1;
    }
    for (size_t i = 0; i < n; i++) {
        slot_of[i] = -5;
        vic_fd[i] = -1;          /* malloc'd: calloc's 0 would alias fd 0 */
        outs[i].slot = -1; outs[i].fd = -1;
        outs[i].map = NULL; outs[i].map_size = 0;
    }

    if (!bt_cache) {
        /* Read-only cache-disabled fallback: direct mmaps. A failed
           open marks only that entry failed. */
        for (size_t i = 0; i < n; i++) {
            if (bt_open_file(paths[i], 0, &outs[i].fd, &outs[i].map,
                             &outs[i].map_size) < 0) {
                outs[i].slot = -1; outs[i].fd = -1;
                outs[i].map = NULL; outs[i].map_size = 0;
            }
        }
        free(slot_of); free(vic_fd); free(vic_map); free(vic_sz);
        free(gens);
        return 0;
    }

    /* Window 1 (no rwlocks held): probe every path. */
    pthread_mutex_lock(&bt_cache_lock);
    for (size_t i = 0; i < n; i++) {
        int found = 0;
        int s = bt_cache_probe(paths[i], &found);
        slot_of[i] = found ? s : -5;
    }
    pthread_mutex_unlock(&bt_cache_lock);

    /* Outside M0: open mappings for the misses, capturing the publish
       generation immediately before each open. A per-file open
       failure marks that entry failed (-6) and the batch continues. */
    for (size_t i = 0; i < n; i++) {
        if (slot_of[i] != -5) continue;
        gens[i] = atomic_load_explicit(&g_bt_publish_generation,
                                       memory_order_acquire);
        if (bt_open_file(paths[i], 0, &outs[i].fd, &outs[i].map,
                         &outs[i].map_size) < 0) {
            slot_of[i] = -6;     /* failed: caller skips this shard */
            continue;
        }
        slot_of[i] = -2;         /* self-owned scratch mapping */
    }

    /* Window 2 (no rwlocks held): install the misses. If the path got
       cached by another thread between the windows, we lost the
       install race — keep our fresh mapping and serve it uncached
       (same outcome as bt_acquire_impl's lost-race branch). Victims
       are collected per entry and disposed outside the lock. */
    pthread_mutex_lock(&bt_cache_lock);
    for (size_t i = 0; i < n; i++) {
        if (slot_of[i] != -2) continue;
        int found = 0;
        int s = bt_cache_probe(paths[i], &found);
        if (found) continue;     /* lost race: stays -2 */
        s = bt_install_reader_locked(paths[i], s, gens[i], &outs[i],
                                     &vic_fd[i], &vic_map[i],
                                     &vic_sz[i]);
        if (s < 0) continue;     /* stays -2: serve uncached */
        slot_of[i] = s;          /* cache owns the mapping */
    }
    pthread_mutex_unlock(&bt_cache_lock);
    for (size_t i = 0; i < n; i++)
        if (vic_fd[i] >= 0)
            bt_dispose_mapping(vic_fd[i], vic_map[i], vic_sz[i]);

    /* Outside M0: take + verify each rwlock single-shot. Cached
       handoffs get the SAME publication validation as
       bt_acquire_impl's hit path: compare the entry's
       validated_publish_generation with g_bt_publish_generation and,
       on mismatch, require durability_same_open_inode(e->fd, path)
       before blessing the entry for the new generation — otherwise
       the mapping is stale pre-publish (reindex/vacuum publication
       replaced the file) and this acquire falls back to a fresh
       mapping of the current path. durability_same_open_inode runs
       under the held rwlock exactly as at btree.c:860; the optional
       non-blocking retire via btree_cache_invalidate_nowait happens
       only AFTER the rwlock is released, so the M0 touch still occurs
       with no rwlock held (the acyclic-graph property is preserved). */
    for (size_t i = 0; i < n; i++) {
        if (slot_of[i] < 0) continue;   /* -2/-6: resolved */
        pthread_rwlock_rdlock(&bt_cache[slot_of[i]].rwlock);
        BtCacheEntry *e = &bt_cache[slot_of[i]];
        if (atomic_load_explicit(&e->used, memory_order_acquire)
                == BT_CACHE_LIVE &&
            strcmp(e->path, paths[i]) == 0) {
            uint64_t current_generation = atomic_load_explicit(
                &g_bt_publish_generation, memory_order_acquire);
            uint64_t validated_generation = atomic_load_explicit(
                &e->validated_publish_generation, memory_order_acquire);
            if (validated_generation != current_generation) {
                if (!durability_same_open_inode(e->fd, paths[i])) {
                    /* Stale pre-publish mapping. No retry (unlike
                       bt_acquire_impl, which re-probes after its
                       retire): this entry is served from a fresh open
                       performed AFTER the staleness check, so it
                       cannot be the stale inode; the invalidate is
                       best-effort hygiene for future acquires. */
                    pthread_rwlock_unlock(
                        &bt_cache[slot_of[i]].rwlock);
                    (void)btree_cache_invalidate_nowait(paths[i]);
                    slot_of[i] = -5;
                    outs[i].slot = -1; outs[i].fd = -1;
                    outs[i].map = NULL; outs[i].map_size = 0;
                    if (bt_open_file(paths[i], 0, &outs[i].fd,
                                     &outs[i].map,
                                     &outs[i].map_size) < 0)
                        slot_of[i] = -6;  /* caller skips shard */
                    else
                        slot_of[i] = -2;  /* self-owned uncached */
                    continue;
                }
                /* Same inode: the entry is valid for the new
                   generation — bless it, then hand off. */
                atomic_store_explicit(
                    &e->validated_publish_generation,
                    current_generation, memory_order_release);
            }
            /* Verified handoff: the rwlock stays held; the adopting
               iter's bt_release is the unlock. */
            outs[i].slot = slot_of[i];
            outs[i].fd = e->fd;
            outs[i].map = e->map;
            outs[i].map_size = e->map_size;
            continue;
        }
        /* Evicted+retargeted between the windows. The fd/map just read
           are CACHE-owned — drop the copies (never dispose them as if
           uncached), unlock, and retry this one entry as a fresh
           uncached open. A failure here marks only this entry -6. */
        pthread_rwlock_unlock(&bt_cache[slot_of[i]].rwlock);
        slot_of[i] = -5;
        outs[i].slot = -1; outs[i].fd = -1;
        outs[i].map = NULL; outs[i].map_size = 0;
        if (bt_open_file(paths[i], 0, &outs[i].fd, &outs[i].map,
                         &outs[i].map_size) < 0)
            slot_of[i] = -6;     /* failed: caller skips this shard */
        else
            slot_of[i] = -2;     /* self-owned uncached */
    }
    free(slot_of); free(vic_fd); free(vic_map); free(vic_sz); free(gens);
    return 0;
}
```

**New helper 3 — iter construction around a pre-acquired `BtFile`**
(place directly after `btree_range_iter_open`, whose post-acquire body
it mirrors):

```c
/* Build a BtRangeIter around an already-acquired BtFile (batch path).
   Takes ownership: on calloc failure it releases bt itself. */
static BtRangeIter *btree_range_iter_open_from_bt(
        BtFile *bt,
        const char *min_val, size_t min_len, int min_exclusive,
        const char *max_val, size_t max_len, int max_exclusive,
        int desc) {
    BtRangeIter *it = calloc(1, sizeof(*it));
    if (!it) {
        LOG_ERROR(LOG_SUB_BTREE,
                  "btree_range_iter_open_from_bt: calloc failed");
        bt_release(bt);
        return NULL;
    }
    it->bt = *bt;
    it->valid = 1;
    it->desc = desc;
    if (min_len > BT_MAX_VAL_LEN) min_len = BT_MAX_VAL_LEN;
    if (max_len > BT_MAX_VAL_LEN) max_len = BT_MAX_VAL_LEN;
    if (min_len > 0) memcpy(it->min_val, min_val, min_len);
    it->min_len = min_len;
    if (max_len > 0) memcpy(it->max_val, max_val, max_len);
    it->max_len = max_len;
    it->min_exclusive = min_exclusive;
    it->max_exclusive = max_exclusive;
    if (!desc) iter_seek_fwd(it);
    return it;
}
```

**Merge rewiring** (`btree_walk_ordered_ranges`). At the existing
allocation block (anchor):

```c
    OrderedRangeCursor *cursors = calloc((size_t)n, sizeof(OrderedRangeCursor));
    int *heap = calloc((size_t)n, sizeof(int));
    BtRangeIter ***slots = malloc((size_t)n * sizeof(BtRangeIter **));
    OrderedRangeResume *resume = calloc((size_t)n, sizeof(OrderedRangeResume));
    if (!cursors || !heap || !slots || !resume) {
        free(cursors); free(heap); free(slots); free(resume);
        return;
    }
```

extend with two scratch arrays:

```c
    const char **open_paths = malloc((size_t)n * sizeof(char *));
    int *todo = malloc((size_t)n * sizeof(int));
    if (!cursors || !heap || !slots || !resume || !open_paths || !todo) {
        free(cursors); free(heap); free(slots); free(resume);
        free(open_paths); free(todo);
        return;
    }
```

Replace the open section at the top of the `for (;;)` loop (anchor:
from `int nh = 0;` down to the `or_merge_sift_down(heap, nh, i, ...)`
heapify call) with:

```c
        int nh = 0;
        h.released = 0;
        /* C2a: (re)open every not-done shard via ONE batch acquire —
           M0 is taken only while this thread holds no rwlocks.
           Per-shard best-effort, exactly like the sequential
           btree_range_iter_open this replaces: an entry that cannot
           be opened marks only that shard done; the merge continues
           with the healthy shards. */
        size_t nopen = 0;
        for (int s = 0; s < n; s++) {
            cursors[s].iter = NULL;
            cursors[s].has_entry = 0;
            if (!resume[s].done) todo[nopen++] = s;
        }
        if (nopen > 0) {
            BtFile *outs = calloc(nopen, sizeof(BtFile));
            int brc = -1;
            if (outs) {
                for (size_t k = 0; k < nopen; k++)
                    open_paths[k] = ranges[todo[k]].path;
                brc = bt_open_readers_batch(open_paths, nopen, outs);
            }
            if (brc != 0) {
                /* outs calloc failure, or the helper's own scratch
                   failure (nothing acquired in either case): degrade
                   exactly as if every pending shard's open had
                   failed. */
                for (size_t k = 0; k < nopen; k++)
                    resume[todo[k]].done = 1;
            } else {
                for (size_t k = 0; k < nopen; k++) {
                    int s = todo[k];
                    const BtOrderedRangeSpec *r = &ranges[s];
                    if (outs[k].slot < 0 && outs[k].fd < 0) {
                        /* This shard's open failed: skip it, merge
                           continues — today's semantics. */
                        resume[s].done = 1;
                        continue;
                    }
#ifdef TEST_BUILD
                    if (bt_test_take_range_open_failure(r->path)) {
                        bt_release(&outs[k]);
                        resume[s].done = 1;
                        continue;
                    }
#endif
                    const char *open_lo = r->min_val;
                    size_t open_lo_len = r->min_len;
                    int open_lo_excl = r->min_exclusive;
                    const char *open_hi = r->max_val;
                    size_t open_hi_len = r->max_len;
                    int open_hi_excl = r->max_exclusive;
                    if (resume[s].have) {
                        if (desc) { open_hi = resume[s].val;
                                    open_hi_len = resume[s].len;
                                    open_hi_excl = 0; }
                        else      { open_lo = resume[s].val;
                                    open_lo_len = resume[s].len;
                                    open_lo_excl = 0; }
                    }
                    cursors[s].iter = btree_range_iter_open_from_bt(
                        &outs[k], open_lo, open_lo_len, open_lo_excl,
                        open_hi, open_hi_len, open_hi_excl, desc);
                    if (!cursors[s].iter) {
                        resume[s].done = 1;
                        continue;
                    }
                    cursors[s].tie_id = r->tie_id;
                    or_pull(&cursors[s]);
                    if (cursors[s].has_entry) heap[nh++] = s;
                }
            }
            free(outs);
        }
        for (int i = nh / 2 - 1; i >= 0; i--)
            or_merge_sift_down(heap, nh, i, cursors, desc);
```

Notes: ownership of each `outs[k]` transfers into the constructed iter
(`it->bt = *bt`) — `outs` is only the scratch array and is freed after
the construction loop; a failed entry (`slot < 0 && fd < 0`) is never
adopted. The TEST_BUILD hook preserves
`btree_range_iter_open`'s observable test-failure behavior (armed
failures still mark the shard done); the ordering drift — the hook now
consumes after a successful open instead of preventing it — only adds
one invisible open/close and changes no assertion surface. The walk's
existing abort path (callback rc < 0 closes all iters inline) and the
function tail's frees are unchanged, except `free(open_paths);
free(todo);` is added beside the tail's
`free(cursors); free(heap); free(slots); free(resume);`.

**Verification:** the C1 report's exact scenario re-run — zero
reports; `--filter cursor --filter find --filter agg --filter join`
green natively and under TSan; `--filter reindex --filter index` green
(every ordered walk exercises the batch). Red-on-base for this rung IS
the C1 report. Paste all runs.

#### Rung 2 — C2c: escalate (human decision), with pre-specified options

If C1's report **still** fires after C2a, the situation is
qualitatively different: C2a has removed the only M1-held→M0
acquisition shape in the tree, and the static audit shows no M0→M1
shape exists anywhere (`bt_cache_evict_slot` releases M0 before
touching the rwlock at btree.c:529–532; `bt_release` takes no M0; both
`bt_acquire_impl` paths release M0 before taking the rwlock; all three
retry restarts are taken with no lock held). A report on that graph is
a pure detector artifact, and no behavior-preserving restructure inside
`bt_acquire_impl` can remove it.

STOP and ask the human, presenting both pre-specified options:

1. **Retain one documented suppression line** — keep
   `deadlock:bt_acquire` in `.tsan.supp` with a rewritten comment
   containing the acyclicity proof (the four facts above) plus the
   C1 and C2a report history; the plan's empty-file goal is amended
   for this one proven-FP line, and Tasks C3/Z proceed for the other
   five.
2. **Approve the bt-cache SlotRef warm path as a new pre-plan** — add
   `_Atomic uint64_t gen` to `BtCacheEntry` (shard_db_internal.h,
   mirroring kfcache's entry, which carries exactly this for SlotRef
   validation), bump it in `bt_cache_evict_slot`, add a
   `BtCacheRef {int slot; uint64_t gen;}` plus an M0-free warm acquire
   modeled on `kfcache_acquire_direct_ex` (advisory gen check →
   rdlock → used/path verify → handoff; any mismatch falls to the full
   slow path), and have the merge hold per-range refs across its
   reopens. This removes M0 from warm acquires tree-wide, but it is a
   hot-path cache redesign that deserves its own plan review — it is
   deliberately NOT pre-written here because the residual report's
   exact stacks are required inputs for that review.

Option 1 trades one retained line for certainty; option 2 preserves
the empty-file goal at the cost of another plan cycle. Either way this
plan's executable scope ends at this checkpoint; nothing is improvised.
### Task C3 — Delete `deadlock:bt_acquire` (only if Tier C went silent)

Reached only when C1 reported zero fires, or rung C2a silenced the C1
report. Remove the commented line permanently. Re-run the C1 family
set plus a full `run-all` under TSan (with /tmp headroom checked) —
zero deadlock reports anywhere. Paste runs.

**If the Task C2 escalation ended in option 1 (retain the line), SKIP
the deletion.** Instead rewrite the `deadlock:bt_acquire` comment in
`.tsan.supp` with the acyclicity proof and the C1/C2a report history
(Task C2 rung 2, option 1), then proceed to Task Z under its
option-1 branch: the file is retained containing exactly that one
line.

### Task Z — Empty file: remove `.tsan.supp` + reference sweep + full gates

1. **Empty-file path (Tier C went silent).** `.tsan.supp` now has no
   suppression lines → `rm .tsan.supp` (the deletion shows in the raw
   diff for review; nothing is committed by the executor). Continue
   with steps 2–4.
   **Option-1 branch (the C2 escalation retained the line).** Do NOT
   delete `.tsan.supp` and do NOT touch the `suppressions=` clauses in
   AGENTS.md or `.github/workflows/tsan.yml` — the retained file holds
   exactly the one documented `deadlock:bt_acquire` line (rewritten
   per Task C3's skip note) and both gate invocations keep pointing at
   it. Skip to step 4 with the option-1 wording.
2. (Empty-file path only.) AGENTS.md: drop the
   `suppressions=$(pwd)/.tsan.supp` clause from the TSan gate line,
   keeping exactly
   `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"`.
3. (Empty-file path only.) `.github/workflows/tsan.yml`: remove the
   `suppressions=${{ github.workspace }}/.tsan.supp` clause (keep the
   workflow's `report_signal_unsafe=0:report_atomic_races=0` options —
   CI-only, out of scope) and update the header comment that
   references the file.
4. `docs/concepts/concurrency.md` + `docs/reference/changelog.md`:
   update the suppression references. Empty-file path: findings
   fixed/restructured, file removed, date. Option-1 path: five
   findings fixed, one proven detector artifact retained by human
   decision (date + pointer to the proof in the supp comment).
   Historical `docs/plans/` entries are NOT edited either way.
5. Full gates on the final tree, three consecutive fresh runs each:
   - `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` then 3× bare
     `./build/bin/shard-db-test run-all`.
   - `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh` then 3×
     `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
     ./build/bin/shard-db-test run-all` — no suppressions clause
     (empty-file path; that is the point of the plan). **Option-1
     branch:** keep the clause, pointing at the retained file —
     `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp"`
     — and the gate must still be green: the one suppressed finding is
     the documented artifact, everything else must be silent.
   - /tmp headroom checked before every TSan leg.
6. PLAN_NOTES.md final matrix: six lines → per line: fix/refactor
   landed, red proof, green run, deletion timestamp (option-1 path:
   five deletion timestamps plus one entry documenting the retained
   line's proof and the human decision).

---

## Embedded execution rules

- Branch off `main`; work stays **uncommitted** (AGENTS.md standing
  exception). Nothing is committed/pushed/merged by the executor.
- Build/test: `SKIP_TESTS=1 ./build.sh`;
  `./build/bin/shard-db-test run[-all] [--filter <substr>]`.
- Sanitizer gates per tier: each task's verification lists its required
  runs; Task Z runs the full 3× ASan + 3× TSan. No `halt_on_error=0`
  anywhere; findings fail the run.
- Re-run the inventory grep (Task A2 verification step 1) at execution
  start, before wiring. A mismatch halts the run.
- If a quoted anchor is not found exactly, write `PLAN_NOTES.md`
  describing the mismatch and halt the entire execution run immediately
  — do not guess, reinterpret, or continue to any further task, even an
  unrelated one. Resuming requires the human (or the planning model,
  re-engaged) to read `PLAN_NOTES.md` and hand back a patched or fresh
  plan.
- If you hit a decision the plan doesn't cover, stop and ask — do not
  improvise. Bounded decision points and their owners: Task C2's
  escalation checkpoint (a report surviving rung C2a) is decided by the
  human from the two pre-specified options; a Tier B deletion-gate
  report that does not match a Task B3 matrix verdict halts for the
  human.
- New tests must not depend on process-wide mutable state unless
  guarded, and must not assume they run alone or in order: A1a/A1b/B1
  cases create and remove their own objects under unique names
  (`uaf_obj`, `dropuaf`, fixture temp roots) and disarm every knob they
  arm.

## Scope guardrails

- No widening beyond the six suppression lines' findings; drive-by
  concurrency cleanups go to `docs/plans/` pre-plans, not this diff.
- No `_Atomic`-everywhere on record bytes; the flag-ordered pattern is
  the design (Task B2's scope is exactly the two io_direct loads and
  the two RELAXED stores).
- The objlock violations are addressed exactly as specified: A3's
  dispatch-level wrlock for drop-object; the auto-sweep count path is
  made safe by the refcount alone — do NOT add locks there (the count
  is intentionally pre-lock).
- No shutdown barrier / query-rejection mechanism is added for the
  embedded non-quiescent case: `shard_db_close`'s existing documented
  quiescence contract is preserved as the requirement (see the
  lifetime invariants); adding a barrier would be a new feature, not a
  suppression fix, and goes to a pre-plan if wanted.
- CI option changes limited to the suppressions clause.
- Tier ordering A → B → C so the file shrinks monotonically; Task 0
  owns no deletions.
