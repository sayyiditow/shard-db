# Plan E execution evidence — bmfix startup repair (2026-11-11)

Plan: `docs/plans/2026-11-11-plan-e-bmfix-startup-repair.md` (Executor Plan, Phases 0–5).
Branch `fix/type-correctness-recovery`, base HEAD `a73ce13` (Plans A–D present), tracked tree clean
at start, untracked plan/evidence docs preserved. All work left **uncommitted** for raw-diff review;
sanitizer gate deferred to the branch gate after Plan I. Every shell command prefixed with `rtk`.

Environment commands:
- build: `rtk proxy env SKIP_TESTS=1 ./build.sh`
- single case: `rtk ./build/bin/shard-db-test run <name>`
- full suite: `rtk ./build/bin/shard-db-test run-all --jobs 4`
- `rm -rf /tmp/shard-db-*` after every test invocation (done throughout; the runner prints
  exit statuses captured via `echo EXIT=$?` on the command itself, never through a pipe).

---

## Phase 0 — discovery and consumer audit

### P0.1 — callers

- `slotcask_bmfix_pending`: definition `src/db/slotcask.c:11709`, declaration `src/db/slotcask.h:401`,
  exactly **one** caller: `src/db/embedded.c:962` inside `bmfix_recover_all`. ✓ matches plan.
- `bmfix_recover_all`: defined `src/db/embedded.c:954`; called exactly twice — `embedded.c:1055`
  (clean branch of `shard_db_recover_before_stamp`) and `embedded.c:1062` (unclean branch), both
  **before** the KFM2 `marker_recovery_sweep_object` loop. ✓ matches plan.
- `build_bitmap_pass`: forward declaration `src/db/index.c:2093-2095`, definition
  `src/db/index.c:3124`, exactly **one** caller: `cmd_add_index` at `src/db/index.c:2188`. ✓ matches plan.
- `slotcask_bmfix_clear` (`src/db/slotcask.c:11761`): unlinks every name with prefix `.bmfix-`
  (`strncmp(de->d_name, ".bmfix-", 7) == 0`) — would delete `.bmfix-disabled` today. ✓ matches plan.

### P0.2 — read-gate audit (every production path that opens a bitmap for reading)

`bm_open`/`bm_open_many_ro` call sites (production, non-test):

| Site | Function | Gate |
|---|---|---|
| query.c:850 | `btree_dispatch_schema_checked` bitmap branch | `field_index_type` — gated **directly** (P3.3) |
| query.c:1261 | ShardCountCtx bitmap postfilter (`bm_open_many_ro`) | `classify_bm_postfilters` (query.c:1442) via `pick_index_for_leaf` → transitively gated |
| query.c:2327 | stream-find btree executor selection | `field_index_type` — gated **directly** (P3.3) |
| query.c:3487 | `bm_open_read` helper | callers all gated (below) |
| query.c:3756 | `bm_intersect_shard_worker` (`bm_open_many_ro`) | FP_INTERSECT executor; planner selects leaves via `pick_index_for_leaf_schema` (query_plan.c:3290/3300) → transitively gated |
| query.c:5324 | `leaf_build_worker` probe | `#ifdef TEST_BUILD` test seam — not a production read |
| query_plan.c:2986 | `card_est_leaf` bitmap popcount | `pick_index_for_leaf` at 2975 → transitively gated |
| query_aggregate.c:5813/5904/5989/5831/6050 | indexed-group-by bitmap paths | `field_has_index_type(..., IT_BITMAP)` at 5715 → transitively gated |
| query_aggregate.c:974 | `build_smaller_bitmap_keyset_ck` | `field_has_index_type(..., IT_BITMAP)` at 974 → transitively gated |

`bm_open_read` (query.c:3487) callers, all reached through gated picks:
`bitmap_emit_generic_for_shard` (3513), `bm_popcount_generic_shard_worker` (3620) and the generic
worker (3660), `bitmap_emit_for_shard` (3880), `bm_popcount_one_shard_worker` (4182/4211), the
single-value bitmap walks (4511/4565/4678/4766). Their consumers:
`build_keyset_from_bitmap` (dispatched from `build_keyset_from_leaf_ck`, query.c:5173
`pick_index_for_leaf`), `bm_popcount_for_crit` (callers: query.c:8587 gated by `pick_index_for_leaf`
at 8582; `idx_count_for_leaf_ck` at 6979 via `pick_index_for_leaf`),
`bm_popcount_generic_for_crit` (build_keyset_from_bitmap 4390; query.c:8590 gated at 8582),
`bm_popcount_intersect` (8623 gated at 8614), aggregate igb (gated at 5715),
`bitmap_emit_for_shard` in aggregate (6050, inside the igb block).

`find_plan_reads_bitmap` (query.c:8905) → `find_leaf_is_bitmap` (8902) → `pick_index_for_leaf` → gated.

`field_index_type` callers (all listed): query.c:850 (`btree_dispatch_schema_checked` bitmap
branch — gated directly), query.c:2327 (stream-find btree check — gated directly), query.c:6606
(`guard_eq_driver`: `field_index_type == IT_BTREE && btree_idx_exists` — with the gate a
bitmap-only field falls to the default `IT_BTREE`, but `btree_idx_exists` is false → returns NULL;
safe), query_plan.c:3345 (`composite_index_exists`, IT_BTREE on composite name — unaffected),
query_plan.c:3482 (`order_field_drivable`, IT_BTREE — unaffected).

`pick_index_for_leaf_schema` (query.c:4095) calls `field_has_index_type` at 4110-4112 → its
wrapper `pick_index_for_leaf` (4156) and all planner sites are transitively gated.

Write-maintenance callers (allowed exceptions, unreachable while flagged because every mutation
entry is refused): index.c:410 (`bitmap_update`, writer), index.c:624 (`bitmap_open_shard`,
prepare/apply writer), index.c:1154 (`bitmap_sync_shard_path`, bulk-window flush),
slotcask.c:2414 (`kfcache_resplit_locked` bitmap remap, writer), query_schema.c:1269
(`create_bm_worker`, rebuilder), index.c:3087 (`bm_shard_walk_worker`, rebuilder), index.c:4207
(seg-scan reindex rebuild, rebuilder).

No production bitmap read path bypasses the three gated entry points. ✓ no halt.

### P0.3 — mutation-entry audit (every way a record mutation or kf rebuild can start while serving)

1. **JSON `dispatch_json_query`** (server.c:704): every mode dispatched here after the
   `took_wrlock`/`took_rdlock` block (server.c:1611-1631). `mode_is_write` (server.c:36):
   insert, update, delete, bulk-insert, bulk-insert-delimited, bulk-delete, bulk-update,
   bulk-update-delimited, put-file, delete-file, sequence. `mode_is_schema` (server.c:58):
   rename-field, remove-field, add-field, edit-field, vacuum, truncate, add-index, remove-index.
   Bulk modes (server.c:1972-2076) dispatch inside this function **after** the object lock —
   the deferred per-shard pipeline is internal to `cmd_bulk_*` (query_bulk.c). ✓
2. **Text fast path** (server.c:2493-2560): `fast_wr = mode_is_schema(cmd)`; record mutations on
   this path are `insert`, `delete`, `bulk-insert`, `bulk-delete` (all take `fast_rd`). Full text
   command set: get, insert, delete, size, orphaned, exists, keys, fetch, find, backup, restore,
   add-index, remove-index, bulk-insert, bulk-delete, vacuum, recount, truncate, put-file,
   get-file-path, sequence. No text `update`. Closed set matches the plan. `restore` self-locks
   (`fast_restore`), `sequence`/`put-file`/`get-file-path` allowed.
3. **NQL path** (`dispatch_nql_query`, server.c ~615): find/count/aggregate only — read-only. ✓
4. **`shard_db_query`** (embedded.c:1228): calls `dispatch_json_query(db->db_root, json,
   "127.0.0.1")` (embedded.c:1240). ✓
5. **`auto_vacuum_sweep_one`** (server.c:3223): `cmd_vacuum(eff, obj, 0, 0)` under `objlock_wrlock`.
   **`auto_reshard_sweep_one`** (server.c:3343): `cmd_vacuum(eff, obj, 0, target)` (the reshard
   vacuum) under `objlock_wrlock`.
6. **No JSON `reshard` mode exists** (grep for `"reshard"` in src/db/*.c: no mode string). The only
   reshard entries are the auto-reshard sweep (5) and the splits variant of `cmd_vacuum`.
7. **`kfcache_resplit_locked` callers** (kf rebuild): slotcask.c:2834, 3044, 3125, 7720 — all
   inside slotcask put/bulk-write internals (mutation paths only); slotcask.c:11798 inside
   `slotcask_compact_kf`. **`slotcask_compact_kf`** caller: query_maint.c:221 (`cmd_vacuum`).
   **`rebuild_object_v2`** callers: query_find.c:2579/2660/2786 (schema-mutation transaction
   paths), query_schema.c:1105 (`cmd_edit_field` batch) — all schema modes.
8. **`reindex`, `drop-object`, `restore` are in neither classifier** ✓ (classifier lists above;
   drop-object special-cases its own locking at server.c:1478; restore self-locks, server.c:2492).

At the P0-confirmed gate point in `dispatch_json_query` (immediately after the lock take), the
guard reservation/registry pin are **not yet held** (both begin inside the `if (guard_json)`
block below) — confirmed by reading server.c:1586-1660. ✓ no halt.

### P0.4 — INDEX_TEST_FAIL_NEXT_BUILD

- Parsed from db.env only under `#ifdef TEST_BUILD` (`src/db/config.c:436-443`), via
  `index_test_fail_next_build(atoi(...))`.
- Consumed one-shot in `reindex_object_checked_impl` (`src/db/index.c:4680-4683`): positive →
  zeroed → fail with that errno **before any work**.
- `reindex_object_durable_checked_ex` (index.c:4848) routes into `reindex_object_checked_impl`
  with `require_durable=1`, so the startup repair consumes the knob (that is what the escape
  cases arm).
- With the BTRI stamp present, the BTRI migration (embedded.c:1027, the other startup consumer)
  is skipped. Startup order before `bmfix_recover_all`:
  `shard_db_not_null_upgrade_audit` → `rebuild_recovery` (objlock.c:850; per object:
  `recover_transaction_layout` + `recover_unambiguous_legacy_layout` + stale `*.new/*.old` sweep —
  none consume the knob; the rebuild-txn rollback that calls `reindex_object_checked` at
  objlock.c:617 only runs when an `active` rebuild-transaction directory exists, which no test
  creates) → BTRI migration (skipped when stamped) → `layout_migration_run` → clean-flag handling
  → `bmfix_recover_all`. ✓ claim holds for the test scenarios; recorded nuance: an interrupted
  rebuild `active` transaction with `indexes_may_change` would consume the knob earlier (not
  exercised by any test).

### P0.5 — test hook selectors and seam globals

- `TEST_HOOK_SET_GLOBALS` = 6; selectors 0–4 taken: 0=KF_SEAMS, 1=ESCAPE_FAIL, 2=BMFIX_FAULT,
  3=BMLOCK_LOG, 4=BMKS_RACE (`src/db/test_control.c:47-51`). The handler (`test_control.c:255+`)
  ends with a final `else` that applies the kf seams; new selectors added as `else if` branches
  before it, inside the file-level `#ifdef TEST_BUILD` (file spans `#ifdef` at :14 to `#endif`
  at :345). The handler ACKs every SET_GLOBALS. ✓
- `g_shard_test_fail_phase` (slotcask.c:68, plain `long`, init −1), `_occurrence` (69, plain
  `long`), `_sticky` (71, plain `int`) — plain globals before P1a. ✓
- `g_shard_test_sync_counts[]` (slotcask.c:51) is never reset by an arm (only
  `shard_test_ctl_reset` zeroes it). ✓
- `shard_test_note_sync` incremented via `__atomic_add_fetch` but its sticky branch **re-read**
  `g_shard_test_sync_counts[phase]` — the TSan re-read P1a fixes.
- `TestHookMessage.a`/`.b` are `uint64_t`; the plan's `(long)(int64_t)msg.a` cast makes −1
  round-trip (runner sends `(uint64_t)(int64_t)phase`).
- Cases that arm the fail-phase seam (P1a smoke list): `test_durability_ordering.c`,
  `test_marker_v2.c`, `test_request_flush_batching.c`, `test_window_release_routes.c`.

### P0.6 — dual bitmap+btree declaration

**Yes, possible.** Evidence:
- `cmd_create_object` (query_schema.c:1515-1615) parses each `indexes` entry with `parse_index_spec`
  and appends every entry to `pidx[]` with **no dedupe by field name**; `kind:bitmap` (explicit
  type) and `kind:btree` (explicit btree) both survive; `idx_should_auto_bitmap` (config.c)
  returns 0 for explicit types, so no re-promotion collision.
- index.conf gets both lines (query_schema.c:1791-1814); bitmap shard files are preallocated per
  bitmap declaration only.
- The index loaders keep every line: `idx_cache_ensure` reads index.conf line by line into
  parallel `fields[]`/`types[]` arrays; `load_index_fields`/`load_index_types` return them all.
- The write path maintains **per entry**, not per field: storage.c:716 (bitmap entries) and
  storage.c:871 (btree entries) each iterate the full entry list; same per-entry dispatch in
  query_bulk.c (753/796/862).
- Read side: `field_has_index_type(kind, IT_BITMAP)` = 1 and `field_has_index_type(kind,
  IT_BTREE)` = 1; `pick_index_for_leaf_schema` prefers bitmap when present (query.c:4147), so
  unflagged `kind eq` plans bitmap; with the P3.3 gate it falls to `IT_BTREE` (real btree exists).
- **Consequence:** the two escape cases use the local `br_open_dual` variant whose `create-object`
  passes `"indexes":["flag:bitmap","kind:bitmap","kind:btree","n"]`; the escape case asserts the
  dual-declaration clause, and mutation proof (h) applies (executed, see P4.3).

### P0.7 — explain bitmap substring (sample from a scratch daemon)

`cmd_explain` (query_plan.c:2700) prints per source leaf
`"{"field":...,"op":...,"index":"<it>","role":"seed","estimated_rows":N}"` where `it` comes from
`pick_index_for_leaf`. The substring identifying a bitmap plan is **`"index":"bitmap"`**
(query_plan.c:2763; `"plan":"bitmap"` exists only for FP_BITMAP_SMALLER). Sample captured from a
scratch daemon (`build/bin/shard-db server`, port 47931, 2 rows, then stopped):

```
$ <bin> query '{"mode":"count","dir":"p","object":"t","criteria":[{"field":"flag","op":"eq","value":"true"}],"explain":true}'
{"plan":"bitmap","order":"none","total_cheap":true,"table_rows":2,
 "source":[{"field":"flag","op":"eq","index":"bitmap","role":"seed","estimated_rows":1}],
 "postfilter":[],"hints":[]}
$ <bin> query '{"mode":"count","dir":"p","object":"t","criteria":[{"field":"n","op":"gte","value":0}],"explain":true}'
{"plan":"leaf","order":"none","total_cheap":true,"table_rows":2,
 "source":[{"field":"n","op":"gte","index":"btree","role":"seed","estimated_rows":1}],
 "postfilter":[],"hints":[]}
$ <bin> get p t k99999     # missing single key
{"error":"Not found"}
```

The missing-key `get` shape `{"error":"Not found"}` is recorded here per the plan and asserted by
the escape case.

### P0.8 — raw daemon spawn pattern (test_null_migration.c)

- `fork_raw_daemon` (test_null_migration.c:320): resolves the binary in order
  `./build/bin/shard-db-test-server`, `./build/bin/shard-db`, `./shard-db`; `fork()`; child
  `chdir(m->base)` (the base dir containing db.env), dup2 stdout+stderr to `<base>/refuse.log`,
  `execl(bin, bin, "server", NULL)`, `_exit(127)` on exec failure.
- `spawn_daemon_raw` (358): WNOHANG poll loop (100 ms steps) up to `timeout_ms`; returns the
  waitpid status with `*exited=1` on exit; on timeout SIGKILL + reap and returns −1. ✓
  `br_spawn_expect_exit` copies this shape with a 60 s budget.

### P0.9 — docs describing bmfix startup behaviour / startup refusal

Live docs (updated in P4.4):
- `AGENTS.md` "Bitmap slot coherence" paragraph — final sentence described startup strict reindex.
- `docs/concepts/concurrency.md` ~102-105: corrupt-skip leaves the marker to the startup rebuild.
- `docs/reference/changelog.md` `## Unreleased`.
- `docs/reference/error-codes.md`: got the refusal error row.
- `src/db/slotcask.c:2287` comment "only startup may clear it".
Other grep hits are historical plan/evidence files under docs/plans/ (not updated).

**Phase 0 result: all nine audits match the plan's claims. No mismatch → no halt.**

---

## Phase 1a — seam

Changes (all TEST_BUILD): `_Atomic` on `g_shard_test_fail_phase`/`_occurrence`/`_sticky` in
`shard_test_ctl.h` + slotcask.c definitions; `shard_test_note_sync` loads each once into locals
and uses the local `n` for the sticky compare (no re-read of the counter array);
`TEST_HOOK_SET_GLOBALS_FAIL_PHASE 5` (arm: reset counters via `__atomic_store_n(..., 0,
__ATOMIC_RELAXED)` → store occurrence → store phase; disarm: store phase first) and
`TEST_HOOK_SET_GLOBALS_FAIL_STICKY 6`, both `else if` branches before the kf-seam final `else`;
fixture wrapper `test_env_ctl_set_fail_phase(env, phase, occurrence, sticky)` in fixtures.c/.h
(sticky sent first when arming, cleared after disarming; phase sent as `(uint64_t)(int64_t)phase`
so −1 round-trips).

Build: `rtk proxy env SKIP_TESTS=1 ./build.sh` → EXIT=0.

Smoke run of every case that uses the seam (P0.5 list). The plan names `test-durability-ordering`
by **file**; that file registers 20 cases (`test-win-*`, `test-ordering-*`, `test-msync-range`,
`test-durability-*` families) — recorded as a moved name, all run:

| Command | Result |
|---|---|
| `run test-window-release-routes` | 38 passed, 0 failed, EXIT=0 |
| `run test-request-flush-batching` | 273 passed, 0 failed, EXIT=0 |
| `run-all --filter test-win-` | 321 passed, 0 failed across 8 cases, EXIT=0 |
| `run test-msync-range` | 7 passed, EXIT=0 |
| `run test-ordering-marker-clean-after-crud` | 7 passed, EXIT=0 |
| `run test-ordering-delete-marker-free` | 9 passed, EXIT=0 |
| `run test-durability-clean-shutdown-skips-recovery` | 5 passed, EXIT=0 |
| `run test-durability-sigkill-marker-after-write-recovers` | 13 passed, EXIT=0 |
| `run test-durability-corrupt-update-marker-kf-slot-rejected` | 13 passed, EXIT=0 |
| `run test-durability-corrupt-marker-policy` | 9 passed, EXIT=0 |
| `run test-marker-v2` | 53 passed, EXIT=0 |
| `run-all --filter test-durability-bulk-` | 90 passed, 0 failed across 6 cases, EXIT=0 |

Total smoke: 838 assertions, 0 failed. `rm -rf /tmp/shard-db-*` after every invocation.

---

## Phase 1b — seam probe (folded into F7a's first half)

Recorded from `test-bmfix-unclean-with-kfm2` runs (visible in the case output as assertion
labels):

- Arming `SHARD_TEST_PHASE_K` occurrence 1 sticky and sending ONE single-row bulk-insert:
  **a response arrived within the 120 s client timeout** — evidence label:
  `disarm fail phase (probe evidence: bulk rc=-1 inserted=-1 errors=0)`. The `-1/-1` shape is the
  durability-pending rejection (the window fails at the K barrier after its marker is durable;
  the response is a durability-pending error with no `inserted:` field — same semantics as
  `test_durability_ordering`'s post-M K-fault case, EINPROGRESS).
- After `test_env_kill`: `exactly one KFM2 marker pending after kill (count=1)` — the plan's
  "exactly one `data/kf/*_marker.dat`" holds at this point.
- The commit-sync-fault step before it leaves the `.bmfix-*` marker (`bmfix marker retained` ok)
  and **no** KFM2 marker (count=0): a successfully committed window clears its own KFM2 marker;
  the plan's F7a row asserts only `br_repair_marker_exists` at that point. The probe's
  "exactly one" therefore applies to the K-faulted window's retained marker, matching
  `test_durability_ordering`'s "exactly one retained marker" semantics.
- Disarm succeeded; the restart replayed the marker (case green — see P1/P4 runs).
- The probe did not hang.

---

## Phase 1c — tests + base (RED) evidence

9 cases appended to `src/test/cases/test_bitmap_resplit.c`, registered with `TEST_REGISTER`:
`test-bmfix-repair-shard-scoped`, `test-bmfix-repair-bad-marker-name`,
`test-bmfix-escape-disabled`, `test-bmfix-escape-heals-at-startup`,
`test-bmfix-escape-flag-write-fails`, `test-bmfix-flag-without-markers`,
`test-bmfix-reindex-clears-serving-markers`, `test-bmfix-unclean-with-kfm2` (F7a + probe),
`test-bmfix-unclean-repair-fails-with-kfm2`. New helpers: `br_seam_once`, `br_env_append`,
`br_env_remove_line`, `br_repair_marker_exists` (find `.bmfix-*` `! -name .bmfix-disabled`),
`br_flag_exists`, `br_kfm2_marker_count`, `br_trim`, `br_open_dual` (P0.6 dual declaration),
`br_seed_1500`, `br_inode_snapshot`/`br_inode_same` (stat `st_ino` + `st_ctim` per file under
`indexes/{flag,kind,n}`, taken after `test_env_stop_keep` and before planting),
`br_spawn_expect_exit` (P0.8 raw spawn, 60 s budget, −1 on timeout), `br_explain`,
`br_expect_refusal` (exact JSON refusal after newline trim), `br_text_insert_expect_refusal`
(exact `\x1F` text line, exact text refusal).

Two plan-conformance fixes to my own first drafts, made before any production code existed:
1. F7a initially asserted a KFM2 marker count ≥ 1 right after the commit-sync bulk and asserted
   the probe's return value — both beyond the plan's assertion list. The commit-sync window
   commits and clears its own KFM2 marker (only the `.bmfix` marker stays), and the plan says the
   fault-stage call's "return value and counters as evidence only". Fixed to: evidence-only label
   for the probe, and `count == 1` ("exactly one") after the kill. (Run output: first draft
   46 passed / 2 failed; after the fix, green.)
2. The text-path request literal used C hex escapes directly (`"\x1Fbr"`): the escape greedily
   consumed the hex digit `b`, corrupting the line (`{"error":"Unknown dir: t"}`). Fixed with
   explicit string concatenation (`"insert" "\x1F" "br" ...`).

Base (RED) runs — every failing assertion is inside the plan's expected set:

| Case | Command | Result | Failing assertions | Expected set |
|---|---|---|---|---|
| repair-shard-scoped | `run test-bmfix-repair-shard-scoped` | EXIT=17, 41 passed / 17 failed | 14× "other .bm unchanged", 2× "n/ unchanged", 1× "log has `bitmap rebuild of 1 shard`" | ✓ exact |
| repair-bad-marker-name | `run test-bmfix-repair-bad-marker-name` | EXIT=2, 53/2 | 2× "n/ unchanged" | ✓ exact |
| escape-disabled | `run test-bmfix-escape-disabled` | EXIT=1, 24/1 | 1× "startup OK" (early exit; daemon refused) | ✓ exact |
| escape-heals-at-startup | `run test-bmfix-escape-heals-at-startup` | EXIT=1, 24/1 | 1× first "startup OK" | ✓ exact |
| escape-flag-write-fails | `run test-bmfix-escape-flag-write-fails` | EXIT=1, 29/1 | 1× "log has `could not write the disabled flag`" | ✓ exact |
| flag-without-markers | `run test-bmfix-flag-without-markers` | EXIT=19, 26/19 | 1× "stale disabled flag removed", 16× ".bm unchanged", 2× "n/ unchanged" | ✓ exact |
| reindex-clears-serving-markers | `run test-bmfix-reindex-clears-serving-markers` | EXIT=1, 37/1 | 1× "marker false" | ✓ exact |
| unclean-with-kfm2 (F7a) | `run test-bmfix-unclean-with-kfm2` | EXIT=0, 46/0 | — (GREEN by design) | ✓ |
| unclean-repair-fails-with-kfm2 | `run test-bmfix-unclean-repair-fails-with-kfm2` | EXIT=1, 33/1 | 1× "log has `with pending commit markers; startup refused`" | ✓ exact |

No failing assertion outside any expected set → no halt. Full first-run logs preserved at
`/tmp/pe-case*.log` (session scratch; contents quoted above by counts and failing labels).

Seam+tests snapshot for the revert proof (P4.2): saved as
`/tmp/plan-e-scratch/plan-e-seam.patch` (852 lines, 6 files) immediately after the base runs and
before any production change; later test-side fixes (diagnostic message, escape-escape fix,
`br_escape_start` strengthening) postdate the snapshot and do not alter any base RED set (the
escape cases early-exit at "startup OK" on base; the other cases don't share that setup).

---

## Phase 2 — shard-scoped repair

1. `index.c::build_bitmap_pass` gains the last parameter `const uint8_t *only_shards` (NULL =
   every shard; else `sch->splits` bytes, nonzero = rebuild). Selected shards are packed densely
   into a `sel[]` array so `parallel_for` runs only those walks; the failure-cleanup and publish
   loops iterate the packed array; with no shard selected it returns OK without touching disk;
   the banner reports the selected count plus `" (partial)"` when a mask is given; all
   allocation paths free `sel`. Forward declaration updated; `cmd_add_index` passes NULL.
2. `index.c::bitmap_rebuild_shards_durable(eff_root, object, only_shards, out_errno)` declared in
   `types.h` next to `reindex_object_durable_checked_ex`. Under TEST_BUILD it consumes
   `g_index_test_fail_next_build` exactly like `reindex_object_checked_impl` (positive → zero →
   fail with that errno before any work). Loads `Schema`/`TypedSchema`; `splits <= 0`,
   `streams <= 0` or no typed schema → EINVAL. Missing `index.conf` (ENOENT) = no bitmap fields =
   0. Per index.conf line: composite lines skipped; type resolved through the new shared
   `index_conf_line_resolve` helper (parse_index_spec + idx_should_auto_bitmap + 2-byte enum
   65535) — the same helper now used by `reindex_object_checked_impl`'s descriptor loop, so the
   two can never drift (its behaviour is unchanged — the loop text is equivalent). Each bitmap
   field is rebuilt with `build_bitmap_pass(..., force=0, only_shards)`; any status other than
   `INDEX_BUILD_OK` → log ERROR `BMFIX-RECOVER <root>/<obj>: bitmap rebuild failed for field F
   (status S, errno E)`, `*out_errno` = build errno else EIO, return −1.
3. `slotcask.c/.h`: `slotcask_bmfix_pending` replaced by
   `slotcask_bmfix_marked_shards(data_dir, splits, out_mask)` — count of marker entries, −1 on
   opendir/readdir error (ENOENT of `indexes/` → 0), NULL mask counts only, `.bmfix-disabled`
   never a marker, strict 1–8 `[0-9a-f]` suffix below splits selects that byte, any other
   `.bmfix-*` name sets all bytes. `slotcask_bmfix_clear` skips `.bmfix-disabled`. New:
   `slotcask_bmfix_disabled_exists` (1/0/−1), `slotcask_bmfix_disabled_write`
   (open O_WRONLY|O_CREAT|O_TRUNC → fsync → close → fsync `indexes/`; every fd closed on every
   path; errno preserved), `slotcask_bmfix_disabled_remove` (unlink + fsync dir; ENOENT → 0 with
   `*removed = 0`; `removed` nullable).

## Phase 3 — recovery rework and gates

1. `embedded.c::bmfix_recover_all` rewritten (header comment updated): per object, in order —
   count-only marker scan (−1 refuses); flag-existence probe (−1 → WARN + treat as flagged +
   next object); `n == 0` → remap sweep + stale-flag removal under `objlock_wrlock` (removed →
   WARN `stale disabled flag removed (no markers)`; failed → WARN + stays flagged); `n > 0` →
   schema load (`splits <= 0` refuses), mask calloc + mask scan (−1 refuses), `objlock_wrlock`
   held through the repair and escape decision (unlocked on every continue/return), WARN
   `interrupted bitmap remap; bitmap rebuild of K shard(s)`, `bitmap_rebuild_shards_durable` →
   on success `slotcask_bmfix_clear` → on success `slotcask_bmfix_disabled_remove`; errno (or
   the rebuild's out_errno) captured into a local `E` immediately after the failure and used in
   every later log line; everything succeeded + flag removed → WARN `repair succeeded; bitmap
   planning re-enabled`; only the remove failed → WARN + stays flagged. Rebuild/clear failure →
   escape: pending KFM2 markers (−1 counts as pending) → ERROR `repair failed (errno=E) with
   pending commit markers; startup refused`; else flag write fails → ERROR `could not write the
   disabled flag (errno=E); startup refused`; else ERROR `bitmap planning DISABLED (repair
   failed, errno=E); reads fall back, writes refused; run reindex` + stays flagged. After the
   loop, any flagged object → `atomic_store(&g_bmfix_any_disabled, 1)`.
2. `slotcask.c::slotcask_bmfix_object_disabled(eff_root, object)`: one relaxed load of
   `g_bmfix_any_disabled` (never cleared in-process) short-circuits to 0; otherwise the
   per-object flag existence with −1 treated as 1; saves/restores errno around its `access()`.
3. `query.c` read gates: `field_has_index_type` skips matching IT_BITMAP entries when disabled;
   `field_index_type` skips bitmap entries when disabled and falls to the existing default
   (IT_BTREE) — defense in depth; no other index type changes.
4. Mutation gates: JSON `dispatch_json_query` — immediately after the took_wrlock/took_rdlock
   block, gates `(mode_is_schema(mode) || mode_is_write(mode))` excluding `put-file`,
   `delete-file`, `sequence`, emits the exact refusal JSON, unlocks exactly the taken lock,
   frees `guard_json`/`mode`/`dir`/`object` like the adjacent early returns, returns (guard
   reservation/registry pin confirmed not held at that point, P0.3). Text fast path — after its
   lock take, gates `fast_wr` or the closed record-mutation set insert/delete/bulk-insert/
   bulk-delete (restore exempt), prints the exact text refusal, unlocks, `goto timing`.
   `auto_vacuum_sweep_one` / `auto_reshard_sweep_one` — silent pre-check before their start
   WARN/INFO, re-check under the object wrlock, unlock + WARN `auto-vacuum: skipping d/o
   (bitmap repair pending)` / `auto-reshard: skipping d/o (bitmap repair pending)`. (Sweeps are
   code-review-only coverage, recorded as such.)
5. `index.c::cmd_reindex`, under its per-object wrlock (`objlock_wrunlock` moved after the new
   step): `pending = slotcask_bmfix_marked_shards(dir, 0, NULL) != 0 ||
   slotcask_bmfix_disabled_exists(dir) != 0` (−1 counts as pending). Pending + full reindex →
   `reindex_object_durable_checked_ex`, then `slotcask_bmfix_clear`, then
   `slotcask_bmfix_disabled_remove`; success → WARN `REINDEX d/o: bitmap repair markers cleared`
   plus `; bitmap planning re-enabled` when `removed == 1`; any failure → `objects_failed++`,
   WARN with errno, state left in place. Pending + composites_only → rebuild as today, touch
   neither, WARN `REINDEX d/o: composites-only reindex leaves bitmap repair pending`. Otherwise
   unchanged; response shapes unchanged. Data dir built with the same snprintf pattern as
   `bmfix_recover_all`.
6. `query_maint.c::cmd_restore`: after the restored `indexes/` is in place, before the wrlock is
   released — if the flag exists (nonzero; −1 counts as present) →
   `atomic_store(&g_bmfix_any_disabled, 1)` + WARN `RESTORE d/o: restored object has bitmap
   planning disabled; run reindex`. **Code-review-only coverage** (needs a backup taken while
   flagged; no test) — noted at hand-back.

Build after P2+P3: EXIT=0, no new warnings (only pre-existing OpenSSL deprecation notes in
tls.c, present on the base build too).

**Startup-line observability — plan mechanism claim vs fixture behaviour.** The plan's contract
states "Startup lines go synchronously to `<base>/daemon.log`". That is true for the *initial*
`test_env_start` (its child redirects stdout/stderr to `<base>/daemon.log`), but **false for the
restart path**: `test_env_start_at`'s child inherits the runner's stderr, so restart-time
startup repair lines land in the runner's stderr, not any file under `<base>` — making the
plan's `br_log_has` startup-line assertions unobservable. First observed as
`test-bmfix-repair-shard-scoped` 57/1 with only the log assertion failing while the BMFIX lines
appeared verbatim in the runner's stderr.

- First fix attempted: redirect the restart child's stdout/stderr to `<base>/daemon.log`
  (append) in `fixtures.c`. This made the plan's assertions observable — but **collided with an
  existing case**: `test_bitmap_cache_lock_errors.c::test-bitmap-cache-selfhold-vacuum` captures
  the restart daemon's stderr *itself* by dup-ing the runner's fd 2 onto
  `<base>/daemon-restart.log` around its `test_env_start_at` call, relying on the inherited-
  stderr contract; the fixture redirect broke it (full-suite run 1:
  `test-bitmap-cache-selfhold-vacuum` 4/1, "startup diagnostic shows cap16/slots32/threshold16",
  expected 1 got 0 — the only failure of that run). grep found exactly one case using this
  fd-2 capture pattern.
- **Final fix (kept):** revert the `fixtures.c` redirect entirely (fixtures.c returns to
  seam-only changes) and adopt the existing in-repo mechanism in the new cases:
  `br_stderr_capture(root)` / `br_stderr_restore(saved)` dup the runner's fd 2 onto
  `<base>/daemon-restart.log` around the four direct `test_env_start_at` calls whose cases
  assert startup log lines (shard-scoped, `br_escape_start`, heals, flag-without-markers).
  stderr is unbuffered, so the synchronous startup lines are on disk when the start returns,
  and `br_log_has` (grep -r over `<base>`) finds them. The collision case keeps its environment
  exactly as designed; no existing test changed; all plan assertions keep their exact strength.
- After the fix: all 9 new cases green AND `test-bitmap-cache-selfhold-vacuum` green again
  (19/0, EXIT=0).

**Restore-sequence slip during the revert proof (P4.2), recorded for the reviewer:** the second
revert-proof restore did `git checkout -- src/db src/test` and then `git apply plan-e-full.patch`,
but the patch's doc hunks (AGENTS.md, docs/*) target files outside the reverted paths and were
still in place, so the apply failed on the doc files. The src hunks were applied with
`git apply --include='src/*'` (clean) and the tree verified complete (17 modified files) before
continuing. The patch content itself never failed against a true base tree.

---

## Phase 4 — proofs

### 4.1 Green runs (after P2+P3)

| Case | Result |
|---|---|
| test-bmfix-repair-shard-scoped | 58 passed, 0 failed, EXIT=0 |
| test-bmfix-repair-bad-marker-name | 55 passed, 0 failed, EXIT=0 |
| test-bmfix-escape-disabled | 90 passed, 0 failed, EXIT=0 (final setup) |
| test-bmfix-escape-heals-at-startup | 61 passed, 0 failed, EXIT=0 (final setup) |
| test-bmfix-escape-flag-write-fails | 30 passed, 0 failed, EXIT=0 |
| test-bmfix-flag-without-markers | 45 passed, 0 failed, EXIT=0 |
| test-bmfix-reindex-clears-serving-markers | 38 passed, 0 failed, EXIT=0 |
| test-bmfix-unclean-with-kfm2 | 46 passed, 0 failed, EXIT=0 |
| test-bmfix-unclean-repair-fails-with-kfm2 | 34 passed, 0 failed, EXIT=0 |
| existing `run-all --filter test-bitmap-resplit` (13 cases) | 3290 passed, 0 failed, EXIT=0 |

### 4.2 Revert proof

Run twice: once after P1/P2/P3 with the first seam snapshot, and **re-run with the final seam
patch** (capture helpers added, fixture redirect reverted) after the P4.4 regression fix. Both
runs produced identical results; the second is the recorded one:

`rtk git checkout -- src/db src/test` → `rtk git apply /tmp/plan-e-scratch/plan-e-seam.patch`
(seam + tests only; 874 lines, 6 files — includes the slotcask.c `_Atomic` seam hunk only)
→ apply succeeded, build EXIT=0 → ran all eight RED cases + F7a:

| Case | Seam-only (base-equivalent) result | Matches P1 |
|---|---|---|
| repair-shard-scoped | EXIT=17, 41/17 (14 .bm + 2 n/ + 1 log) | ✓ |
| repair-bad-marker-name | EXIT=2, 53/2 (2 n/) | ✓ |
| escape-disabled | EXIT=1, 25/1 ("startup OK") | ✓ |
| escape-heals-at-startup | EXIT=1, 25/1 ("startup OK") | ✓ |
| escape-flag-write-fails | EXIT=1, 29/1 (log) | ✓ |
| flag-without-markers | EXIT=19, 26/19 (1 log + 16 .bm + 2 n/) | ✓ |
| reindex-clears-serving-markers | EXIT=1, 37/1 ("marker false") | ✓ |
| unclean-repair-fails-with-kfm2 | EXIT=1, 33/1 (log) | ✓ |
| unclean-with-kfm2 | EXIT=0, 46/0 (GREEN by design on base) | ✓ |

Then restore (see the restore-sequence slip note above): `git checkout -- src/db src/test` +
`git apply --include='src/*' plan-e-full.patch` (doc hunks were already in place from the docs
sync; the src hunks applied cleanly) → build EXIT=0 → `test-bmfix-repair-shard-scoped` 58/0 and
`test-bmfix-escape-disabled` 90/0 green again.

### 4.3 Mutation proofs (one at a time; rebuild; run the named case; revert)

| # | Mutation | Case | Result |
|---|---|---|---|
| a | delete the unclean-branch `bmfix_recover_all` call (embedded.c:1194) | test-bmfix-unclean-with-kfm2 | RED: EXIT=1, 45/1, "bmfix markers cleared" |
| b | flag-write failure continues instead of refusing | test-bmfix-escape-flag-write-fails | RED: EXIT=1, 29/1, "daemon exited (not timeout)" |
| c | skip the `object_has_pending_markers` check | test-bmfix-unclean-repair-fails-with-kfm2 | RED: EXIT=3, 31/3 ("daemon exited", "pending-KFM2 refusal logged", "no disabled flag written") |
| d | invalid marker name returns −1 (fail closed) | test-bmfix-repair-bad-marker-name | RED: EXIT=1, 22/1, "startup OK" |
| e | remove the JSON gate | test-bmfix-escape-disabled | RED: EXIT=18, 71/18, first at "insert refused" (+update/delete/bulk/vacuum refusals and downstream state) |
| e′ | remove the text gate | test-bmfix-escape-disabled | RED: EXIT=8, 81/8, "text fast-path refusal (got: {\"status\":\"inserted\",...})" |
| f | remove the `composites_only` guard in `cmd_reindex` | test-bmfix-escape-disabled | RED: EXIT=2, 87/2, "composites-only keeps the flag" + "composites-only keeps the marker" |
| g | remove the `field_has_index_type` gate | test-bmfix-escape-disabled | RED (final setup): EXIT=15, 75/15 — reads wrong via damaged bitmaps incl. "flag explain avoids bitmap" |
| h | remove the `field_index_type` gate | test-bmfix-escape-disabled | RED (final setup): EXIT=9, 81/9 — "count/find kind={a,b,c}" and "kind eq b count exact via btree while flagged" fail (executor misroutes the dual-declared kind eq through the damaged shard-0 bitmap: count −1 / find 434) |

Every mutation was reverted and the tree rebuilt before the next; after the final revert the
build is clean and the affected cases green again (`test-bmfix-escape-disabled` 90/0,
`test-bmfix-escape-heals-at-startup` 61/0).

(h) note: the first (h) attempt stayed green because the escape setup damaged only
`flag/000.bm` — the dual-declared `kind`'s shard-0 bitmap was intact, so the ungated
`field_index_type` misroute still counted exactly. The escape prologue (`br_escape_start`) now
truncates **both** shard-0 bitmap files, which matches the scenario (the marker names the shard;
the repair covers every bitmap field on it) and matches the shard-scoped case's inode
expectations (flag/000 and kind/000 both change). With the strengthened setup the fixed code
stays green (90/0, 61/0) and (h) goes red as recorded. Mutation (g) was re-run against the
final setup; the other mutations don't share that setup and their evidence stands.

### 4.4 Docs (same diff)

- `AGENTS.md` "Bitmap slot coherence": final sentences rewritten — shard-scoped repair via
  `bitmap_rebuild_shards_durable`, strict marker-name grammar, the escape flag, the up-front
  refusal scope (all `mode_is_schema` modes + record mutations), sweep skips, reindex
  clearing/re-enabling, stale-flag removal, and the residual refuse-on-pending-KFM2/failed-flag-write
  cases. Evidence: P3.1/P3.4/P3.5 implementation + test-bmfix-escape-* runs.
- `docs/concepts/concurrency.md` (P0.9 hit): "startup rebuild" → the startup repair + escape
  description. Evidence: P3.1 + escape cases.
- `docs/reference/error-codes.md`: the refusal error row with recovery (run reindex). Evidence:
  `br_expect_refusal` exact-match assertions green.
- `docs/reference/changelog.md` `## Unreleased`: full entry including the old-binary behaviour
  for `.bmfix-disabled` (treated as a marker: one full strict reindex, then the file is deleted —
  benign), that a full `reindex` now clears repair markers (behaviour change), and that
  `restore` takes the backup's flag/marker state. Evidence: P0.1 (`bmfix_clear` prefix unlink),
  P3.5, P3.6.
- `src/db/slotcask.c` `keep_marker` comment: "only startup or a full reindex may clear it".
  Evidence: P3.5.

---

## Phase 5 — full suite and hand-back

Fresh build: `rtk proxy env SKIP_TESTS=1 ./build.sh` → EXIT=0, no errors, no new warnings.

Full suite (final, after the capture fix): `rtk ./build/bin/shard-db-test run-all --jobs 4` →
**total: 2,412,216 passed, 0 failed across 649 cases, suite EXIT=0** (baseline 640 cases +
the 9 new cases). `rm -rf /tmp/shard-db-*` after every invocation.

For the record, the first full-suite run (before the capture fix) was 2,412,201 passed / 1
failed across 649 cases: the single failure was `test-bitmap-cache-selfhold-vacuum` "startup
diagnostic shows cap16/slots32/threshold16", root-caused to the (since-reverted) fixture
redirect colliding with that case's fd-2 stderr capture — see the P3 section above. It is
green again in the final run.

Definition of done: no new compiler warnings; no TODO/FIXME/debug output added; every mutation
reverted (grep for `MUTATION` in src/ shows only the pre-existing unrelated
`BT_MUTATION_LOCK_INITIAL_BUCKETS` in btree.c); `git status --porcelain` = 17 modified files
(this plan's src + docs files) + the preserved untracked docs; everything **uncommitted** for
raw-diff review. The revert-proof patches under `/tmp/plan-e-scratch/` were deleted at
hand-back. The sanitizer gate is deferred to the branch gate after Plan I per the standing
exception.

Hand-back summary is in the accompanying chat message: diff stat, P0 audits, base/revert/mutation
evidence, read-only coverage items (cmd_restore arm, auto-sweep skips, field_index_type gate),
and the request for a blind raw-diff review (this diff touches startup recovery, lock-holding
dispatch paths, and introduces an on-disk artifact `.bmfix-disabled`).

---

## Review round 1 (2026-11-11) — fixes

All work still **uncommitted**. Commands as before; exit statuses captured directly.

### R1 blocker — packed shard-selection ids truncated to one byte

`build_bitmap_pass` packed the selected shard ids into a `uint8_t` array: any shard id ≥ 256
silently truncated (`sel[nsel++] = (uint8_t)s`), so a marker naming e.g. shard 300 rebuilt
shard 44 instead — and still cleared the marker.

**Test first**: new case `test-bmfix-repair-shard-300` in `src/test/cases/test_bitmap_resplit.c`
— creates the seeded object with `splits:512` (same `flag`/`kind` bitmaps + `n` btree; 500 rows
in 250-row chunks, `br_check` exact), stops, truncates `indexes/flag/12c.bm` to 33 bytes, plants
`.bmfix-12c` (hex 300), snapshots inode+ctime **after** planting (so "changed" can only mean
"replaced by the repair" — an earlier snapshot-before-damage draft let the damaged file's own
ctime shift make the assertion pass vacuously on the broken code; fixed before recording),
restarts, and asserts: startup OK; marker gone; reads exact; `flag/12c.bm` and `kind/12c.bm`
inodes changed; **every** other index file (512 shards × 2 fields + `n/`) unchanged.
`BR_INO_MAX` raised 128 → 2048 (entries slimmed to `rel[24]`) so the snapshot holds all 1024
bitmap files.

- RED on the pre-fix code: `rtk ./build/bin/shard-db-test run test-bmfix-repair-shard-300`
  → EXIT=10, **1081 passed, 10 failed**:
  `count/find flag=true, flag=false, flag AND kind=b` (the still-damaged shard-300 bitmap fails
  reads), `flag/12c.bm inode changed`, `kind/12c.bm inode changed`, and 2× "no other shard's
  index file changed" (flag/044.bm, kind/044.bm — the truncation `(uint8_t)300 == 44` rebuilt
  shard 44). Exactly the predicted mechanism.
- Fix: `int *sel = malloc((size_t)sch->splits * sizeof(int));` (no cast), `sel[nsel++] = s;`.
- GREEN after the fix: EXIT=0, **1091 passed, 0 failed**.

### R1 major — kf-directory fsync before marker clear, both repair paths

`slotcask.c`'s `fsync_dir` (fsync a directory file itself) is now exported (`slotcask.h`
declaration) and called on `<obj>/data/kf` after a successful rebuild and **before**
`slotcask_bmfix_clear`, in both:

- `embedded.c::bmfix_recover_all`: a failed fsync flows into the existing failure handling —
  errno captured into `E`, pending-KFM2 → refuse; else flag write / DISABLED escape exactly
  like a rebuild failure.
- `index.c::cmd_reindex` repair branch: a failed fsync fails the repair — markers stay,
  `objects_failed++`, WARN with errno.

**No deterministic test**: failing a directory fsync needs an fsync fault seam that doesn't
exist; recorded as **code-review-only** coverage (like cmd_restore and the auto-sweeps).

### R1 minor — pending-KFM2 check in bmfix_recover_all's flagged<0 branch

When the disabled flag cannot even be read, the branch now calls
`object_has_pending_markers(data_dir)` before continuing: nonzero (−1 counts as pending) →
ERROR `BMFIX-RECOVER d/o: cannot read disabled flag (errno=E) with pending commit markers;
startup refused` and startup refuses, same as the repair-failed-with-KFM2 case. No dedicated
test (driving `access()` to fail deterministically would break the rest of startup); recorded
as code-review-only.

### R1 changelog

`docs/reference/changelog.md` Unreleased entry: the old-binary sentence now notes that an
older binary treats `.bmfix-disabled` as a `.bmfix-` marker, runs one full strict reindex and
deletes the file — benign when the rebuild is complete, but **refuses startup if that reindex
fails** (its pre-escape behaviour).

### R1 cleanups

Two latent unused-variable warnings in the raw-spawn cases (dead `int port`) fixed — zero new
compiler warnings confirmed on the round-1 build.

### R1 case results

| Case | Result |
|---|---|
| test-bmfix-repair-shard-300 (new) | RED pre-fix (1081/10), GREEN post-fix (1091/0) |
| test-bmfix-repair-shard-scoped | 58/0, EXIT=0 |
| test-bmfix-repair-bad-marker-name | 55/0, EXIT=0 |
| test-bmfix-escape-disabled | 90/0, EXIT=0 |
| test-bmfix-escape-heals-at-startup | 61/0, EXIT=0 |
| test-bmfix-escape-flag-write-fails | 30/0, EXIT=0 |
| test-bmfix-flag-without-markers | 45/0, EXIT=0 |
| test-bmfix-reindex-clears-serving-markers | 38/0, EXIT=0 |
| test-bmfix-unclean-with-kfm2 | 46/0, EXIT=0 |
| test-bmfix-unclean-repair-fails-with-kfm2 | 34/0, EXIT=0 |
| `run-all --filter test-bitmap-resplit` (13 existing cases) | 3290/0, EXIT=0 |

### R1 full suite — one transient failure, escalated for awareness

First round-1 full-suite run (`run-all --jobs 4`): 2,413,306 passed, **1 failed** across 650
cases — `test-topn-stream-bitmap-postfilter-bounded`, "g0 count is the FILTERED active=true
count (40000)" (`SAFE_STRSTR(resp, "\"n\":40000")` failed; the unfiltered-count assertion
passed, so the response contained neither count — consistent with an error response under
budget/parallel pressure). Root-cause work, recorded rather than rerun-until-green:

1. The case predates this branch (last touched by `b1cc3b7`, R5) and passed in the previous
   full-suite run under identical parallelism.
2. **Diff reachability:** every round-1 production change lives in the repair/marker/flag
   machinery (`build_bitmap_pass`'s mask is only non-NULL from the repair and reindex paths —
   `cmd_add_index` passes NULL; the case builds its indexes at `create-object`, which
   preallocates bitmap files without calling `build_bitmap_pass`; the aggregate path never
   takes markers, flags, reindex or fsync_dir). The fsync_dir export is linkage-only.
3. Solo reproduction: `run test-topn-stream-bitmap-postfilter-bounded` → **50/0, EXIT=0**.

Second full-suite run (`run-all --jobs 4`, fresh): **2,413,307 passed, 0 failed across
650 cases, suite EXIT=0** — the transient did not reproduce. Standing characterization: a
one-off, load-sensitive observation in a pre-existing budget-bound case, with the
diff-reachability analysis above showing the round-1 changes cannot execute in its path.
Escalated for awareness; if it recurs, it warrants a dedicated flake investigation separate
from Plan E (e.g. logging the aggregate response body on that assertion).

---

## Cleanup round (2026-11-11, minor) — four review items

All work still **uncommitted**. Everything below also covers an operational incident and its
full recovery (recorded for the reviewer's attention).

### C1 — shard-scoped case: snapshot after planting the damage

`test_bmfix_repair_shard_scoped` now takes the inode+ctime snapshot **after** truncating
`flag/000.bm` and planting `.bmfix-000` (same discipline as `test-bmfix-repair-shard-300`,
with an explanatory comment), so "changed" can only mean "replaced by the repair" and the
assertion is no longer passable vacuously via the truncate's own ctime shift.

**Red/green proof (as requested), with an honest divergence from the expected outcome:**
the reviewer asked to "show the flag/000.bm check now fails on the base code, then passes
with the fix". Run against the base code (production checkout + seam patch, the P4.2
procedure), the reordered case fails in exactly the plan's expected 17-assertion set
(EXIT=17, 41 passed / 17 failed: 14× "other .bm unchanged", 2× "n/ unchanged", 1× "repair
scoped to 1 shard") — but **`flag/000.bm inode changed` PASSES on base**, because the base
startup repair (the committed full strict reindex) genuinely replaces the damaged file. The
reorder therefore does not (and cannot) turn that check red on base; what it delivers is
assertion honesty: a hypothetical repair that skips corrupt files would now be caught,
where the pre-reorder snapshot masked it. Green run with the fix: 58/0, EXIT=0.

### C2 — strcasecmp for the JSON gate exemptions

`dispatch_json_query`'s bitmap-escape gate now exempts `put-file` / `delete-file` /
`sequence` via `strcasecmp`, matching the `mode_is_write`/`mode_is_schema` classifiers
(a mode spelled `"PUT-FILE"` would otherwise be classified a write and wrongly refused).

### C3 — slotcask.h comment

`slotcask_bmfix_marked_shards`'s contract comment corrected: with `splits <= 0` and a mask
set, the **mask is left untouched** (the previous text claimed every marker selects all
bytes; the implementation's all-shards branch loops `s < splits`, i.e. zero times).

### C4 — stale "repaired at startup by a full reindex" comment

The bitmap-remap sequence comment in `slotcask.c` ("marker - durable
`<obj>/indexes/.bmfix-<shard>`; from here a crash is repaired at startup by a full reindex
of the object") now describes the shard-scoped rebuild: only the named shard(s)' bitmaps
(`bitmap_rebuild_shards_durable`, any other `.bmfix-*` name selects every shard), and the
failure escape (bitmap planning disabled until a full reindex heals it).

### C5 (incident) — production loss during the C1 base-run checkout and full recovery

The C1 red/green proof checks out `src/db src/test` and re-applies the seam patch. The
only surviving full-diff artifacts after the earlier hand-back cleanup were this round's
seam patch and a complete `slotcask.c` diff; the checkout therefore lost the Plan E
production edits in embedded.c, index.c, query.c, query_maint.c, server.c, slotcask.h and
types.h. Recovery: `slotcask.c` restored from the saved full diff; the remaining six files
re-applied edit-by-edit from the session record (every hunk verbatim as reviewed in rounds
0–1, including the round-1 `int *sel` fix, kf-dir fsyncs, and the flagged<0 pending check).
Verification of the reconstruction: clean build (0 errors, 0 new warnings), all 10
test-bmfix cases green (58/55/90/61/30/45/38/46/34/1091 — 0 failed), resplit family 3290/0
across 13 cases, and the full suite below.

### Cleanup-round results

| Case | Result |
|---|---|
| test-bmfix-repair-shard-scoped (reordered snapshot) | base RED: EXIT=17, 41/17 (expected set; flag/000 passes on base — see C1); fixed GREEN 58/0 |
| test-bmfix-repair-bad-marker-name | 55/0, EXIT=0 |
| test-bmfix-escape-disabled | 90/0, EXIT=0 |
| test-bmfix-escape-heals-at-startup | 61/0, EXIT=0 |
| test-bmfix-escape-flag-write-fails | 30/0, EXIT=0 |
| test-bmfix-flag-without-markers | 45/0, EXIT=0 |
| test-bmfix-reindex-clears-serving-markers | 38/0, EXIT=0 |
| test-bmfix-unclean-with-kfm2 | 46/0, EXIT=0 |
| test-bmfix-unclean-repair-fails-with-kfm2 | 34/0, EXIT=0 |
| test-bmfix-repair-shard-300 | 1091/0, EXIT=0 |
| `run-all --filter test-bitmap-resplit` (13 cases) | 3290/0, EXIT=0 |
| Full suite (`run-all --jobs 4`, fresh build) | **2,413,307 passed, 0 failed across 650 cases, suite EXIT=0**; `rm -rf /tmp/shard-db-*` after every invocation |
