# Remove the legacy object layout and all startup migrations (2026-11-25)

Status: APPROVED by the human on 2026-11-25 (after 7 blind-review rounds; the
requested extra rounds are summarized in the review log). Execution waits for
the audit PR to merge (D9). No source edited.
Branch: `refactor/remove-legacy-layout`, cut from `main` AFTER the audit
branch (`test/audit-oracle-harnesses`) is reviewed, pushed and merged (D9).

## Brief (for the human)

### Problem and root cause

The binary still carries two object layouts: the legacy fixed-width layout and
the packed layout (default since 2026.11.1, which migrated every object at
startup). Every read, write, index, aggregate, schema and recovery path
branches on `ts->packed.nfields` / `Schema.packed_layout` (about 250 sites in
25 files under `src/db/`), plus two startup migrations (BTRI index magic,
legacy-to-packed layout) with their stamps, test seams and recovery code, plus
about 38 test files that build legacy objects. The dead layout doubles the
surface of every audit slice and every future change, and it keeps migration
code on the startup path of every daemon.

### Logical flow

0. **Convert the fixture-only legacy tests first** (T0.5; legacy-route tests stay on legacy until T3) so every later
   step can end green.
1. **Tighten the open gate first** (T1): required source version becomes
   2026.11.2, and a root is refused unless every `schema.conf` entry carries
   `layout=packed` and no layout-conversion transaction artifact remains. The
   version stamp alone is not enough: the layout migration stamps the new
   version *before* it converts objects (F16), so a `.version` of 2026.11.2
   does not prove every object is packed.
2. **Remove both startup migrations** (T2): BTRI reindex migration, layout
   migration, `.btri-migrated` / `.layout-migrated` stamps (left on disk,
   ignored), `layout_migrate_object`, their crash-point seams and tests.
3. **Remove legacy object creation and loading** (T3): TEST_BUILD
   `declaration_layout:"legacy"`, the legacy branch of `load_schema` /
   `load_typed_schema`, `Schema.packed_layout`.
4. **Remove legacy codecs and branches file by file** (T4), suite green after
   each file: typed read/row/index, planner legacy matchers, rebuild,
   edit-field, bulk, aggregate, join, wire schema, restore/backup of legacy
   metadata, rebuild-txn legacy recovery.
5. **Tests** (T5): convert fixture-only legacy tests to packed, delete tests
   whose subject is gone, add gate tests. **Docs** (T6). **Gates** (T7).

### Decisions

| # | Decision | Alternatives | Why | Tag |
|---|---|---|---|---|
| D1 | `SHARD_DB_REQUIRED_SOURCE_VERSION` = `2026.11.2` | 2026.11.1 | 2026.11.1 and 2026.11.2 carry identical migration code; 2026.11.2 is the released version users run, and the D2 data gate (not the version floor) is the safety net | DECIDED 2026-11-25: 2026.11.2 is released and is what users run |
| D2 | Gate on data as well as version: refuse any `schema.conf` entry without `layout=packed`, and any leftover layout-conversion artifact | version gate alone | F16 stamps the version before converting; a root interrupted mid-conversion would otherwise be opened as packed with legacy objects | DEFAULT |
| D3 | Restoring a backup whose object is legacy-layout refuses with "restore with shard-db 2026.11.2 first" (`cmd_restore` already refuses a backup lacking `"layout":"packed"`; only the message changes) | convert on restore | converting means keeping the converter | DECIDED 2026-11-25: refuse, no legacy support at all |
| D4 | Tests that only use legacy objects as a fixture are converted to packed; tests whose subject is the removed code are deleted, each listed with a reason; tests whose subject is a route that exists only on the legacy layout (class K, see T0.5 and D12) keep their legacy original until T3 beside an additive packed copy | delete all 38 | keeps behavioural coverage | DEFAULT (amended 2026-11-27 after the T0.5 halt) |
| D12 | Legacy-route tests (class K) keep their legacy original through T0.5-T2, gain an additive packed copy in T0.5, and the original is deleted at T3. Packed `bulk-update` with `criteria` is a pre-existing product gap (recorded as AUD-PACKED-BULKUPDATE in the audit index), not fixed here; T0.5 adds one test pinning the packed refusal | convert them and drop assertions (B); implement packed criteria update first (C) | the legacy route is the only place the behaviour exists; removing it cannot make packed objects worse than they already are | DECIDED 2026-11-27: option A, human |
| D5 | Record-level legacy handling (records without the nullmap flag, `legacy_field_absent`, legacy `hdr2` views) is removed too, **only if T0 proves** the layout conversion rewrote every live record | keep record readers | conversion rewrites every live record through a fresh slotcask (T0-P1 confirms); keeping them leaves dead branches | DEFAULT, conditional on T0 |
| D6 | Stale `.btri-migrated`, `.layout-migrated`, `.layout-migration-report` files are ignored, not deleted | delete on start | no new startup writes; harmless | DEFAULT |
| D7 | Do this before Phase 3 slice 1 continues | after the audit | slices 1-3 walk the write paths, every function has a legacy branch; auditing then deleting wastes review | DECIDED 2026-11-25: yes, before slice 1 continues |
| D9 | Order: audit branch is blind-reviewed, pushed, PR'd and merged to `main` first; then `refactor/remove-legacy-layout` is cut from the updated `main`; Phase 3 continues on a fresh branch off `main` after the refactor merges | one branch with both; stacked PRs | keeps both PRs reviewable, the refactor converts the audit's legacy-fixture tests once without conflicts, security fixes reach `main` first, a bad refactor can be reverted alone | DECIDED 2026-11-25 |
| D10 | The gate refuses EVERY token-less `schema.conf` line, including a stale one whose object directory does not exist; the operator fix is "stop shard-db, delete the line from `schema.conf`, start" | ignore stale lines + guards in create-object/restore | with the gate guaranteeing no token-less line at runtime, `create-object`/`restore` need no guard (a guard-based design failed review rounds 6-7: the guard could not fire where placed, restore's was dead code, and a late refusal locked the root out). Breaking: roots 2026.11.2 starts with a stale token-less line (e.g. `test-startup-validator` phase 5) are now refused; listed in the changelog | DECIDED 2026-11-25: option A (gate refuses) |
| D11 | `SHARD_DB_VERSION` stays 2026.11.2 in this change (bump at release cut), so required == current and a clean root is NOOP, not re-stamped; existing version tests are edited as listed in T1 | bump to 2026.11.3 now | the bump belongs to the release cut | DEFAULT |
| D8 | Executor: DeepSeek (exact model name/version to be given by the human), run separately from this session; Claude plans and reviews the diff, and does not execute | Claude Sonnet 5.5 | human decision 2026-11-25, per CORE-PROCESS step 3 | DECIDED 2026-11-25 (changed from Sonnet 5.5) |

### Blast radius

- **Downgrade note (D6):** roots upgraded from 2026.11.2 keep their stamps. A root created fresh by this change lacks `.btri-migrated`; the real 2026.11.2 binary would reindex it and write the stamps; harmless, not supported. `write_meta` keeps writing `old_layout=1` so 2026.11.2 can still recover our rebuild transactions.
- **Breaking, on-disk/startup contract:** a DB_ROOT last opened by a version
  older than 2026.11.2, or holding any non-packed object, is refused at start
  with an upgrade hint. `shard_db_open` returns NULL and the daemon (`cmd_server`, which does
  not call `shard_db_open`) refuses with the same message. Flag in changelog as breaking; version label
  2026.11.3 at release cut.
- **Wire:** unchanged in production builds. `create-object` already refuses
  `declaration_layout:"legacy"` outside TEST_BUILD; `"packed"` and absent stay
  accepted. `describe-object` output: confirm in T0 no legacy-only fields.
- **Config / CLI:** none. The TEST_BUILD env `BTRI_STAMP_TEST_FAIL` and
  layout crash-point seams are removed (tests only).
- **Embedded API (`shard_db.h`):** signatures unchanged; `shard_db_open`
  refuses more roots.
- **Call sites:** the full list is the T0 inventory
  (`docs/plans/audit-artifacts/legacy-removal-inventory.md`); the executor must
  not start T2 until it exists.

### Test strategy

- Packed-copy coverage: every class K test has an additive packed copy from T0.5 (unless its whole subject is the legacy route, then its gap is reported); dropped assertions are listed with their packed coverage or recorded gap; `test-packed-bulk-update-criteria-refused` pins the packed refusal.

- Gate (shared function, see T1): version below 2026.11.2 refused; 2026.11.2 root with a schema line
  lacking `layout=packed` refused; leftover conversion artifact refused; clean
  packed 2026.11.2 root opens (NOOP, no re-stamp), data intact; an interrupted-
  vacuum `.rebuild_txn.active/meta` in 2026.11.2 format (`old_layout=1`) is
  recovered and serves every record.
- Upgrade proof (T7): roots written by the released tag `2026.11.2` and by the
  pre-change `main`, clean-stopped and crash-killed (copy A for the old
  binary, copy B for the new one), give identical `count`/`find`/`get`.
- Negative proof: the pre-change TEST_BUILD binary creates a legacy object;
  the new binary refuses to start with the hint.
- Full suite before and after: case count delta explained line by line.
- ASan+UBSan and TSan, three consecutive full runs each (diff touches
  schema caches, the registry, recovery and object lifetimes).

### Out of scope

Behaviour fixes (AUD-P3-*), planner work, deleting stamp files, downgrade
support, any change to record or index formats of packed objects, bench code.

### Review log

**2026-11-27 amendment (T0.5 halt; delta review 1: 0 blockers, 3 silent-damage majors fixed - K detection rule, dropped-assertion mapping, additive split at T0.5 - plus T3/refusal-pin clarity, 5 minors; round 2: 0 blockers, 1 silent-damage major (shared helpers between K and C cases: per-case split, duplicated helpers, unique `-packed` names) and 3 clarity majors, 4 minors, all fixed in place):** the executor's conversion attempt failed 148 assertions because many tests' subject is a legacy-only route (packed refuses `bulk-update` with `criteria`). D4 amended, D12 added, T0.5 rewritten with classes C/K/D, T3 gains a refusal-pin test. Needs a delta review round (changed test policy).

Round 1 (fresh Opus reviewer): 3 blockers, 8 majors, 7 minors, all
addressed in this revision: task order (T0.5 added), gate must cover
`cmd_server` as well as `shard_db_open` (shared function), gate rule and
placement specified, `recover_unambiguous_legacy_layout` is class D (pre-
RebuildTxn artifacts, kept), stale entries (D10), T1 tests made
implementable, existing version tests listed (D11), T3/T4 overlap on
`packed_layout` readers fixed, sanitizer repeats corrected, proofs spelled
out, shared symbols named class D. Round 2: 1 blocker (rebuild-txn `old_layout` meta must stay), 6 majors
(tests the T0 criteria missed, gate step 3 contradicted class D, gate parsing
reuse, T3 load semantics per case, upgrade proof from the released tag, T4
step mechanics), 9 minors, all addressed in this revision. Round 3: 0 blockers, 4 majors (gate helper return contract, `load_typed_schema`
no-line case, upgrade-proof baseline, T4 warning rule), 10 minors, all
addressed. Round 4: 0 blockers, 2 majors (the gate runs before `g_db` exists so it must
not log; "invalid meta" undefined for the raw parser), 5 minors, all
addressed in this revision. Round 7 (delta): 0 blockers, 3 silent-damage majors (stale-line guard cannot
fire where placed; restore guard dead code; pre-existing drop-object
ordering), 6 minors. Resolved by D10 option A (gate refuses stale lines, guards
removed), clarity items fixed; drop-object/restore bugs recorded as
AUD-P3-5/6. Round 8 not required under the exit rule if the human accepts:
the changed text is a simplification.
Round 6 (delta, new process): 0 blockers, 2 silent-damage majors (a root
the new binary builds could be refused by its own gate: create-object /
restore keep a stale token-less line; the `.layout-migrated` message
steered operators to delete convertible data), 1 clarity major (layout-
manifest test handling), 5 minors, all addressed; the first two change
behaviour (T3 stale-line guard, message text) and need a delta round.
Round 5 (extra, requested by the human): 0 blockers, 3 majors
(`parse_meta_fields` layout out-param, one test-deletion policy plus T0
criterion (vii), D10/P6 premise false once `.layout-migrated` exists -> stale
lines now ignored), 8 minors, all addressed. The 4-round cap was reached: the round-4 fixes
have NOT had a fresh review round (escalated to the human, see the approval
message).

---

## Plan (for the executor)

### Execution rules

- Branch `refactor/remove-legacy-layout`, cut from `main` once the audit PR has merged (D9); do not start before that. Do tasks in order. Build
  `SKIP_TESTS=1 ./build.sh`; test `./build/bin/shard-db-test run[-all]`; full
  suite `run-all --jobs 8`; `rm -rf /tmp/shard-db-*` after every full run.
  Leave work uncommitted for human review (repo convention).
- If an assumption below is false, write `PLAN_NOTES.md` and halt. If a
  decision is not covered, stop and ask. Every task from T0.5 on **ends with a
  green full suite**; never weaken or skip a test to get there. Every removed
  or converted test is listed in `PLAN_NOTES.md` with its reason.

### T0 Diagnostic (no source edits)

Produce `docs/plans/audit-artifacts/legacy-removal-inventory.md` with exact
counts (the Brief's "about 250 sites / 25 files / 38 tests" are estimates).

1. Every function referencing `packed.nfields`, `packed_layout`,
   `layout_packed`, or a `legacy*` symbol, classified **A** migration,
   **B** legacy object layout, **C** legacy record format (no nullmap flag),
   **D** keep. Known class D, do not delete or rewrite:
   - `typed_legacy_decl_size` (used by `validate_packed_field_type`, config and
     query_find field validation), `typed_schema_build_packed`,
     `packed_schema_same`, `reindex_object_durable_checked_ex` (also used by
     `index.c`), `reindex_clean_legacy`, `index_conf_replaces_legacy_bare`,
     v1 text counts.
   - `recover_unambiguous_legacy_layout` and the `data.legacy` /
     `.rebuild_legacy_root` recovery (pre-RebuildTxn rebuild artifacts; no
     current code creates them, but the recovery is layout-independent and
     refusing or removing it could hide an object's data).
   - `RebuildTxn.legacy_data` and `rebuild_txn_legacy_root`: the current
     transaction's staged copy of `data` (`.rebuild_txn.active/data`), used by
     every vacuum/rebuild. Not layout code.
   - The `old_layout` line of the rebuild-txn meta: `write_meta` (writes
     `old_layout=%d`) and `parse_meta` (accepts it). It is a wire between this
     binary and 2026.11.2 (see T3).
   Decide and record whether `shard_db_not_null_upgrade_audit` (acts only on
   disk versions below 2026.10.2) is removed in T2 (default: yes).
2. Proofs (the reviewers verified them; the executor re-confirms with
   citations):
   - **P1** `layout_walk_cb` inserts every live record with
     `typed_schema_hdr2(new_ts)` (PKR_FLAGS) and checks `live_count ==
     expected`; `pkr_view` (`packed_record.c`) refuses any packed record whose
     flags are not `PKR_FLAGS`. If a packed-object read path still accepts a
     non-PKR record, D5 is dropped for that site.
   - **P2** the packed fields.conf grammar after conversion equals what
     `create-object` writes (`layout_packed_decl_line`: only `numeric:N` ->
     `numeric:N,2` changes; the post-publish re-check in
     `layout_migrate_object` uses `packed_schema_same`).
   - **P3** a layout-conversion artifact is `<obj>/.rebuild_txn.active/meta`
     with `old_layout` missing or `0` (`parse_meta`, `recover_transaction_layout`).
     `.rebuild_txn.preparing`, `.rebuild_txn.done` and an active txn with
     `old_layout=1` are valid 2026.11.2 states (a crash during vacuum) and
     must NOT be refused.
   - **P4** `shard_db_version_decide` returns NOOP for disk == required ==
     current (`util.c`).
   - **P5** `bt_open_file` already rejects every magic but `'BTRI'`.
   - **P6** D10 evidence: with `.layout-migrated` present, 2026.11.2 starts with
     a token-less stale line (`test-startup-validator` phase 5), so refusing
     such lines is a new, documented breaking refusal; without the stamp
     `layout_migrate_object` fails on them.
3. A table assigning every affected test to the task that breaks it
   (T0.5 / T1 / T2 / T3 / a named T4 step). Search `src/test` for: (i)
   `declaration_layout`; (ii) literal `schema.conf` lines matching `:2:[0-9]+`
   without `layout=packed`; (iii) hand-built `TypedSchema` (calloc / not built
   via `typed_schema_build_packed`); (iv) hdr2 values 0 or `SREC_FLAG_NULLMAP`
   and calls to `legacy_*` / `typed_span_copy_legacy`; (v) hand-written rebuild
   meta, `data.legacy`, `.rebuild_legacy_root`; (vii) any test that restarts a daemon or calls `shard_db_open` on a root
   holding a legacy object, or writes a `.version` below 2026.11.2: it breaks
   at T1, never later, and is assigned to T0.5 (convert) or T1 (delete);
   (vi) tests or callers that
   load or operate on an object whose `fields.conf` exists without a
   `schema.conf` line, or under a root other than `g_db_root`. The table has a
   column for T2 as well (e.g. `test_null_migration.c` uses
   `BTRI_STAMP_TEST_FAIL`; `test_layout_manifest.c` migration and crash-point
   cases use the layout crash seam; `test_not_null_compat.c` calls
   `shard_db_not_null_upgrade_audit`; `test_layout_manifest.c` mixes subjects,
   so decide per case, not per file. `lm_build` creates objects with
   `declaration_layout:"legacy"`: switch it to packed at T0.5; then every
   `lm_schema_append(":layout=packed")` call site must drop its append
   (otherwise a duplicate token, refused by `startup_schema_line`);
   `test-layout-token-malformed-refuses` replaces the existing
   `:layout=packed` token with each bad token instead of appending; delete
   `test-layout-token-packed-loads` and `test-layout-startup-agreement` at T1
   (their subject, token handling on a legacy object, is gone; gate tests
   (b)/(b4) cover it); keep `test-create-object-packed-*` and
   `test-schema-conf-rewrite-fsync-failure`; `test_layout_token_with_auto_key_parses`
   has its own inline `declaration_layout:"legacy"` create (not via
   `lm_build`): convert it and keep its 8-part assertion; `test_loader_same_object_two_tenants`
   asserts `"tb legacy"`: explicit decision at T3; the executor lists each remaining
   case's decision in the table). Known hits to start from:
   `test_rebuild_txn_recovery.c` (`test-rebuild-txn-recovery-idempotent`
   writes meta without `old_layout`: wrong fixture, needs `old_layout=1`),
   `test_agg_packed_routes.c`, `test_indexed_zero_exists.c`,
   `test_startup_validator.c`, `test_not_null_compat.c`,
   `test_reindex_spill_collision.c` (plants `../escape:sweep:...`),
   `test_whole_record_readers.c`, `test_typed_row_adapter.c`,
   `test_null_storage.c`, `test_r2_predicate_verification.c`.

Halt if P1-P6 contradict this plan.

### T0.5 Tests first

Classify every legacy-fixture test from the T0 table into one of three classes
and handle it as follows. Suite green at the end.

- **Class C (convert).** The legacy object is only a fixture. Switch it to
  packed creation (drop `declaration_layout` legacy) and fix expectations that
  depended on the legacy format. Known expectation-only differences (each
  converted test names what it changed in `PLAN_NOTES.md`): packed varchar
  over-length text is `varchar content exceeds declaration`; malformed numeric
  is `expected a decimal number`; packed varchar is NUL-exact; `describe-object`
  reports varchar `"size":8` with `"capacity":N`; `numeric:P,S` precision is
  enforced (legacy `numeric:N` ignored it, scale 2 becomes 0). A numeric
  precision/round-trip test is converted so that it asserts the packed contract
  (still pinning the int64 boundary the test was about, with a declaration wide
  enough, e.g. `numeric:19,0`); if it cannot, it is class K and is split per the K rule.
- **Class K (legacy-route; original stays until T3).** The test's subject
  or part of its assertions exists only on the legacy layout: `bulk-update`
  with `criteria` (packed refuses it: `cmd_bulk_update` in `query_bulk.c`),
  legacy computed-update accounting, legacy composite-key bounds, legacy
  numeric semantics that cannot be re-expressed on packed, or arena/OOM fault
  indices into the legacy bulk path. **Detection rule (authoritative; the T0
  table and any name list are overridden by it), applied only to tests that
  create a legacy object (never to already-packed tests, e.g.
  `test_idx_absent_shard.c`, which already pins the refusal):** a case is
  class K if any request it sends is a `bulk-update` carrying `criteria`
  (inspect each request, including requests split across string literals and
  `dry_run` forms; a `bulk-update` with `records` or `records`+`if` is NOT a
  criteria update), or if its conversion fails for one of the reasons above
  (the executor shows the failing assertions in `PLAN_NOTES.md`).
  Initial names from the first attempt (confirm and extend with the grep):
  `test-bulk-cas`, `test-or-logic`,
  `test-not-null-updates`, `test-computed-update`, `test-slotcask-v2-bulk`,
  `test-length-ops`, `test-field-vs-field`, `test-regex`,
  `test-agg-varchar-groupby-sum`, `test-index-oversized-varchar`,
  `bulk-update-criteria-varchar-unescape`, `test-bulk-update-json`,
  `test-bulk-update-json-oom`, `test-bulk-insert-arena-oom`,
  `test-composite-varchar-bound`, `test-cas-tree-if` (the last four and
  `test-cas-tree-if` are K only if their conversion fails; show it); known additions that sit under "convert" in
  the T0 table but send a criteria `bulk-update`:
  `varchar-malformed-escape-policies` and `test-request-timeout` (a converted
  copy of either would pass vacuously or fail on the refusal: never convert
  one while it still sends that request). Update the T0 table so it agrees.
  **Original K tests are not edited in T0.5**: they stay on their legacy
  fixture, pass unchanged, and are listed in `PLAN_NOTES.md` with the
  legacy-only route each depends on. **The split is per case, not per file.** A file holding both K and C cases
  (e.g. `test_index_varchar_unescape.c`) keeps its legacy helpers for the K
  originals untouched, and the converted C cases and every packed copy use a
  duplicated packed helper; never convert a shared helper in place. Packed
  copy case names must be unique: `<original-name>-packed`. **Additive split, done in T0.5:** for each
  K test, copy every assertion that does NOT use the legacy-only route into a
  new packed case (new file `test_<name>_packed.c`, registered in `build.sh`),
  so packed coverage of find/count/aggregate/index/CAS operators (with the class C expectation fixes applied to the copy and recorded in `PLAN_NOTES.md`; never loosen an assertion to make it pass) is exercised
  now and a packed surprise halts here, not at T3. Where a later assertion
  depends on state produced by a criteria update, replace that update in the
  packed copy with the equivalent per-key updates (packed supports them);
  if no equivalent exists, drop the assertion. **Every dropped assertion is
  listed in `PLAN_NOTES.md` with (a) the named existing or new packed test that
  covers it, or (b) the recorded reason it is a packed gap or an intended
  packed difference** (e.g. computed `$incr` renders `136.00`, per-key `records`
  shape, composite varchar key bounds, OOM fault points). Items under (b) are
  reported to the human at the T0.5 stop for the audit index; none is silently
  dropped. A K test whose entire subject is the legacy route (e.g.
  `test-request-timeout` on a criteria `bulk-update` timeout) may have no
  packed copy; its gap is reported the same way. Because a packed copy exists from T0.5 on, a K original that breaks
  at T1 (restarts a daemon, calls `shard_db_open` on a legacy root, writes a
  `.version` below 2026.11.2) is simply deleted there.
- **Refusal pin, added in T0.5 (a packed object is already creatable):** new
  daemon case `test_packed_bulk_update_criteria_refused.c`
  (`test-packed-bulk-update-criteria-refused`, registered in `build.sh`):
  create a packed object, insert two records, then send `bulk-update` with
  `criteria`, a `value` (without `value` the server returns `Missing value`
  instead) and, in a second request, `dry_run:true`; assert both responses
  equal `{"error":"packed criteria updates await query integration"}` and that
  `get` and `count` before and after are identical. It documents
  AUD-PACKED-BULKUPDATE and is flipped by a later feature plan. T4's
  `query_bulk.c` step MUST keep the `cmd_bulk_update` packed guard (it is the
  behaviour this test pins; do not treat it as a removable dispatch branch).
- **Class D (delete in the owning task).** Subject is removed code: deleted in
  the task that removes it (migration tests T1/T2, legacy-codec tests the named
  T4 step, etc.).

**Deletion policy (one rule):** a subject-removed test is deleted in the task
that removes its subject, except that a test which restarts a daemon or calls
`shard_db_open` on a root holding a legacy object, or writes a `.version` below
2026.11.2, breaks at T1 and is deleted (or, if not subject-removed, converted)
there; this applies to class K tests too. Exception: class K originals whose
subject is removed in T4 are deleted at T3 (below), not at T4. Also fix schema.conf fixtures lacking
`layout=packed` and the rebuild-meta fixture (`old_layout=1`) now, since they
are wrong on the base tree too. `declaration_layout:"legacy"` still works in
TEST_BUILD here (removed in T3).

**Rule for every later task:** it converts or deletes exactly the tests the
T0 table assigns to it, listed in `PLAN_NOTES.md`, and ends with a green full
suite.

### T1 Gate (test-first)

**One shared function** `int shard_db_layout_gate(const char *db_root, char
*err, size_t errsz)` declared in `shard_db_internal.h`, implemented in
`embedded.c`. Call it from BOTH `shard_db_open` and `cmd_server` (`server.c`
has its own version check and TOO_OLD message and never calls
`shard_db_open`) right after `shard_db_version_check` succeeds and before
`shard_db_open_internal` and any recovery (before `rebuild_recovery`, which
would restore data and call the legacy loader). The `shard_db_open` refusal
path releases the DB_ROOT lock and resets `g_instance_open` (same as the
existing version-refusal path); the `cmd_server` refusal does
`db_root_lock_release(&lock_fd); return 1;` before `shard_db_open_internal`
and prints the message to stderr (visible: the daemon forks only after
recovery). Declare the gate in `shard_db_internal.h` (the existing startup
prototypes live in `types.h`; either works, this one is chosen). Reword the TOO_OLD message at both sites:
"install shard-db 2026.11.2, start it once to completion against this
DB_ROOT, then install this version".

Gate rule. Leave `startup_schema_line` behaviour unchanged (it is shared with
bmfix, restore-swap recovery, `shard_db_mark_clean_if_safe` and validation).
Reuse `startup_read_schema` / `startup_schema_line`: they do not depend on
`g_db_root`, validate `dir`/`object` (`dir_name_ok`, `is_valid_object`, so a
planted `../escape` line is a parse failure, never stat'ed) and refuse
malformed layout tokens. Add `int layout_packed` to `StartupSchemaEntry`, set
inside the existing `parts[6..n)` layout loop of `startup_schema_line` (set
1 when the token equals `layout=packed`; the line is tokenized in place, so
do not re-derive it from the raw line). A missing
`schema.conf` passes (empty root). For each entry:
1. `layout_packed == 0` and `<db_root>/<dir>/<obj>` exists -> refuse naming
   `dir/object`: "object %s/%s is not in the packed layout; install shard-db
   2026.11.2, start it once to completion, then upgrade". If
   `<db_root>/.layout-migrated` exists, 2026.11.2 will not convert it (its
   migration already ran, so the hint would loop): print instead "object %s/%s
   is not in the packed layout. To convert it: remove stale schema.conf lines
   (lines with no object directory) and `<db_root>/.layout-migrated`, start
   shard-db 2026.11.2 once to completion, then upgrade" (2026.11.2 can itself
   create a token-less object after the stamp, and with the stamp removed its
   `layout_migration_run` converts every token-less entry; it fails only on
   stale lines). Rule 3 uses the same stamp-dependent variant. DEFAULT; NEEDS
   YOU to confirm the wording. Add gate test (b4): fixture (b) plus
   `.layout-migrated` gives the variant text.
2. `layout_packed == 0` and the object directory does not exist (D10) ->
   refuse: "stale schema.conf line for %s/%s lacks layout=packed; stop
   shard-db, delete the line from schema.conf, then start it" (no stamp
   variant: the remedy is the same either way).
3. Active rebuild transaction, via a new `int rebuild_txn_active_layout(const
   char *eff_root, const char *object)` in `objlock.c`. Split today's
   `parse_meta` body into `static int parse_meta_fields(const char *meta_path,
   int *splits, int *streams, int *indexes, int *layout, int *seen_layout)` with the current grammar unchanged
   (version=1, splits power of two, streams > 0, `indexes_may_change` 0/1, no
   duplicate or unknown key, size <= 1024, `O_NOFOLLOW`). `parse_meta`
   (copies splits/streams/indexes into the txn; tightened at T3: absent/0
   `old_layout` invalid; the `old_layout_packed` field is gone at T3 but the
   parsed `layout` value still comes back through the out-param) and
   `rebuild_txn_active_layout` both call it, so the gate and recovery agree on
   what is valid, and gate test (c) keeps passing at T3. The helper returns -2
   whenever `parse_meta_fields` fails, else `seen_layout && layout == 1 ? 1 :
   0`: -1 no `.rebuild_txn.active`; 0 active with
   `old_layout` missing or 0 -> refuse with the rule-1 hint; 1 active with
   `old_layout=1` -> pass; -2 active but `meta` missing, not a regular file,
   or invalid -> refuse: "invalid rebuild metadata under %s/%s; preserving
   it" (2026.11.2 would refuse in recovery too); -2 also for an `lstat` error other
   than ENOENT on `.rebuild_txn.active` and for an active path that is not a
   directory. Declare it in `types.h` next to the other `rebuild_txn_*`
   prototypes. Use `lstat` for transaction paths and `stat` (follow symlinks;
   operators may symlink object directories) for the object directory.
**The gate and `rebuild_txn_active_layout` run before `shard_db_open_internal`,
so `g_db` is NULL: they must not call `LOG_*` or any helper that logs
(`recovery_path_state`, `remove_non_authoritative_dir`, `verify_absent`, and
`rebuild_txn_*` other than `rebuild_txn_alloc`/`free`), and use no `g_*`
macro. Report with `fprintf(stderr, ...)` and raw `lstat`. Add an in-process
test case that clears `g_db` around the gate call.**
4. A `stat` error other than ENOENT on the object directory -> refuse.
5. If `startup_read_schema` itself fails (malformed token, bad dir/object,
   `../` line) -> refuse: "schema.conf is unreadable or malformed; fix or
   restore it".
`.rebuild_txn.preparing`, `.rebuild_txn.done`, an active txn with
`old_layout=1`, and `data.legacy` / `.rebuild_legacy_root` are NOT gate
concerns (normal recovery handles them). Known limit: the gate walks
`schema.conf` entries while `rebuild_recovery` walks `dirs.conf` x directory
entries; an object directory with no schema line and an active txn lacking
`old_layout=1` passes the gate and is then refused by the tightened
`parse_meta` with "invalid active rebuild metadata". That is fail-closed (no
data lost); the message is less specific, accepted. Likewise `rebuild_recovery`
walks only `dirs.conf` tenants, so 2026.11.2 starts with a corrupt
`.rebuild_txn.active/meta` under a tenant missing from `dirs.conf` where the
gate (which walks `schema.conf`) refuses: fail-closed, needs corrupt meta,
accepted.

Tests first (`src/test/cases/test_legacy_root_refused.c`, case
`test-legacy-root-refused`). Step 0: add the declaration and a stub returning
0 so the tests compile; paste the red run (fixtures (b), (b2), (c), (c3), (f) fail;
(a) is green until `SHARD_DB_REQUIRED_SOURCE_VERSION` changes, which happens
after the red run), then
implement, paste green. In-process on tmp roots with hand-written fixtures
(`shard_db_open` allows one instance per process and the runner holds it):
(a) `.version` 2026.11.1 via `shard_db_version_decide` -> TOO_OLD; (b)
`schema.conf` line `d:o:8:16:2:<streams>` (no token) + `d/o/fields.conf` ->
refused naming `d/o`; (b2) same line, no `d/o` -> refused with the stale-line message (D10); (b3) a
valid object named `layout` with the token passes; (c)
`schema.conf` line `d:o:8:16:2:<streams>:layout=packed`, `d/o/` exists,
`d/o/.rebuild_txn.active/meta` = `version=1\nold_splits=8\nold_streams=N\n`
`indexes_may_change=1\nold_layout=0\n` + `fields.conf.rollback` -> refused;
(c2) three separate objects, one with `old_layout=1` active meta, one with
`.rebuild_txn.preparing`, one with `.rebuild_txn.done` -> not refused (a single
object with several transaction states is refused by recovery, so never
combine them); (c3) `.rebuild_txn.active` with missing/garbled `meta` ->
refused with the invalid-meta message (same schema line and layout as (c)); expected message for (c) is the rule-1 hint naming `d/o`;
(d) packed line, clean -> passes; (e) no `schema.conf` -> passes; (f) `../escape:sweep:...` line -> the
schema-unreadable message, nothing outside the root is stat'ed. Plus one
daemon-subprocess case (a second case, `test-legacy-root-refused-daemon`,
in the same file) modelled on the static `spawn_expecting_failure` in
`test_startup_validator.c` (copy it into the new test file and add a stderr
redirect to a file, since the original discards stderr): `db.env` lives in the cwd (`base`) with `DB_ROOT=base/db` and a free PORT,
as in `test_startup_validator.c`; the binary is production
`./build/bin/shard-db`; `base/db` contains `.version`=2026.11.2, `dirs.conf`
with `d`, the `schema.conf` fixture (b) and `d/o/fields.conf`; assert non-zero exit and the
hint in stderr.

Existing tests edited at this task (required == current == 2026.11.2, D11):
`test_version_compat.c` ("required source version opens as-is and re-stamps":
expectation NOOP) and `test-version-compat-files` ("floor-release marker opens
as-is"), `test_version_gate_2026_11.c` (2026.10.2 / 2026.11.1 now TOO_OLD;
constant assertion). `version.h` `SHARD_DB_REQUIRED_SOURCE_VERSION` ->
`"2026.11.2"`. The tests assigned to T1 by the T0 table are converted here; the T0 table
assigns to T1 (not T2): `test_layout_manifest.c` cases
`test-layout-migration-converts-legacy`, `-refusals`, `-crash-points` (they
rewrite `.version` to 2026.10.2 and use legacy objects, so the floor and the
data gate break them; they are deleted here) and
`test-layout-migration-finance-acceptance` (restarts a daemon on a legacy
object), and the `test_startup_validator.c` fallback line
`default:v:16:16:2:1` (add `layout=packed`). Phase 5 (`ghost_tenant:obj:16:16:2:1`,
"soft, not fatal") now breaks under D10: add `layout=packed` to the ghost line so
it keeps testing a soft stale tenant, and cover the token-less stale case with
gate test (b2).

### T2 Remove migrations

`embedded.c`: delete the `BTRI_MIGRATION_STAMP` block, `btri_stamp_test_inject`,
`btri_migration_stamp_*`, the BTRI step in `shard_db_recover_before_stamp`,
`layout_migration_run` with its stamp/report helpers and call, and (per T0
decision) `shard_db_not_null_upgrade_audit`. In `shard_db_recover_before_stamp`
the order of bmfix recovery, KFM2 replay and the `was_clean` early return stays
unchanged; only the BTRI stamp writes on those paths go. `query_find.c`: delete
`layout_migrate_object`, `layout_convert_record`, `LayoutWalkCtx`,
`layout_walk_cb`, `layout_write_fields_checked`, `layout_test_crash_point`;
`config.c`: `layout_packed_decl_line`, the env parsing for
`BTRI_STAMP_TEST_FAIL` and the layout crash-point variable; declarations and
globals in `types.h` and `shard_db_internal.h` (`layout_migrate_test_crash_phase`,
`g_layout_crash_phase`, `btri_stamp_test_inject`). Keep class D symbols. `build/bin/datetime-migration-fixture` is a stale
untracked artifact with no build rule; leave it alone.
Tests assigned to T2 by the T0 table are deleted/converted here.

### T3 Remove legacy creation and loading

- `query_schema.c` `cmd_create_object` / `server.c` create-object: drop the
  TEST_BUILD legacy layout and `legacy_fixture`; `"packed"` and absent create
  a packed object; `"legacy"` keeps the existing production error "legacy
  layout objects can no longer be created" in every build; other values keep
  "unknown declaration_layout". `describe-object` prints constant `"packed"`.
- **First in T3, before removing legacy creation:** delete any class K originals still present (some were deleted at T1); for each, verify its `PLAN_NOTES.md` entry lists every assertion that is not present in its `-packed` copy with its coverage or gap, and that the packed copy is green. Then build and run the suite.
- `load_schema` / `load_typed_schema` rules per case. `load_typed_schema`: a
  line present without the token, no matching line, ENOENT `schema.conf`, or
  `effective_root` outside `g_db_root` -> return NULL, NEVER cache, LOG_ERROR
  (today these cases parse `fields.conf` as legacy and cache it; the window is
  reachable because `cmd_create_object` writes `fields.conf` before the
  `schema.conf` line and `cmd_drop_object` removes the tree and then strips the line, so a cached
  `typed=0` would leave a new object untyped); line present but `fields.conf`
  missing -> NULL. `load_schema`: the existing zeroed / "not found" results
  stay, and a token-less present line also returns a zeroed Schema with
  LOG_ERROR (keeps the "typed => packed" invariant). Audit every caller that can hit the no-line window (criterion (vi)); record
  each in `PLAN_NOTES.md` and halt if the behaviour would change. The new
  LOG_ERROR is intended (this applies to `load_schema`'s zeroed result for a
  token-less line as well); skip it when `fields.conf` is absent, otherwise log
  on every call (the result is never cached, so there is nowhere to keep a
  once-flag; the create/drop window is short).
  Invariant for T4: every loaded typed schema has `packed.nfields == nfields
  >= 1`; the existing `typed == 0` path is the only non-packed result.
- **No stale-line guard in `cmd_create_object` / `ensure_schema_conf_line`
  (D10):** the gate guarantees that no token-less line exists at runtime, so
  both keep appending/keeping lines exactly as today. (Pre-existing
  `cmd_drop_object` ordering and `cmd_restore` line-validation bugs are
  recorded as AUD-P3-5 / AUD-P3-6 in the audit index and are out of scope.)
- Remove `Schema.packed_layout` and all readers in this task, including
  `cmd_backup` (always writes `"layout":"packed"`), `cmd_restore` (refuses a
  backup lacking `"layout":"packed"` with the D3 hint; remove the `"legacy"`
  arms of the mismatch message), and `ensure_schema_conf_line` (always emits
  `:layout=packed`; the `storage_version < 2` branch is deleted and
  `cmd_restore` refuses when `bak_sv != 2` with the D3 hint text, listed in
  `docs/reference/error-codes.md` at T6). Remove `update_schema_conf_layout` and the layout block of
  `rebuild_txn_abort`.
- **Keep the rebuild-txn `old_layout` meta wire:** `write_meta` writes the
  constant `old_layout=1`; `parse_meta` keeps parsing the line, and 0 or absent
  after the gate is invalid meta (fail closed, existing "invalid active
  rebuild metadata ... preserving it" path). The gate's raw reader
  (`rebuild_txn_active_layout`) is a separate function that does not get this
  tightening, so gate test (c) keeps its behaviour and message at T3. Drop the `schema_conf_layout`
  call from `rebuild_txn_begin` and the `old_layout_packed` field only.
  Reason: a valid 2026.11.2 root with an interrupted vacuum carries
  `old_layout=1`; and 2026.11.2 reading a meta without the line would treat it
  as 0 and strip `:layout=packed`.
- Regression test (name `test-rebuild-txn-2026-11-2-meta`): create a packed
  object through the daemon, stop it, move `data` to
  `.rebuild_txn.active/data`, copy `fields.conf` to `fields.conf.rollback`,
  write the exact 2026.11.2 meta text (`version=1`, `old_splits`,
  `old_streams`, `indexes_may_change`, `old_layout=1`); restart; assert
  recovery completes and every record is served. Must pass before and after
  the change. Note its overlap with the existing
  `test-rebuild-txn-packed-*-after-stage` cases: keep it only for the
  hand-written 2026.11.2 meta, not duplicating them.
Tests assigned to T3 by the T0 table are converted here.

### T4 Remove remaining legacy branches, one file per step

Each step: edit, build with no new warning (including unused-function),
`run-all --jobs 8` green, record the case count, convert/delete exactly the
tests the T0 table assigns to it. A `static` helper whose last caller is
removed in this step is deleted in the same step (no warning results). An
extern helper and its declaration (in `types.h`, `typed_row.h`, `slotcask.h`,
`schema_wire.h` etc.) are deleted in the step that removes its LAST caller;
extern functions produce no unused warning, so check with grep. Any new
warning fails the step; never suppress one. **`query_bulk.c` end state:** `cmd_bulk_update` keeps its signature and its `server.c` dispatch and reduces to the packed refusal (`{"error":"packed criteria updates await query integration"}`, pinned by `test-packed-bulk-update-criteria-refused`); a NULL typed schema (no/unloadable schema) must return an error and never run a legacy body: the executor copies the not-found error text the `bulk-update` dispatch in `server.c` already emits for a missing object and records it in `PLAN_NOTES.md`; the legacy body and the static helpers whose last caller it was (`bulk_upd_shard_worker*` etc.) are deleted in that step. Order callers-first: `query_aggregate.c`, `agg_exact.c`,
`query_join.c`, `query_bulk.c` + `typed_bulk_insert.c`, `query_plan.c`,
`query.c`, `query_schema.c`, `query_find.c` (`rebuild_object*`), `storage.c`,
`index.c`, `typed_read_command.c`, `typed_index.c`, `packed_csv_command.c`,
`schema_wire.c`, `typed_row.c`, `objlock.c` (class A-C only), `config.c`
leftovers, `slotcask.c` / `btree.c` only where T0 classed a site A-C.
Record-level nullmap-less handling (`legacy_field_absent`, legacy `hdr2`
views, `typed_span_copy_legacy`) is removed only under D5/P1. Invariant: no
behaviour change for packed objects; never delete a function with a caller
outside the removed set. The executor may reorder callers-first further if the
build shows a different dependency, noting it in `PLAN_NOTES.md`.

### T5 Final test table

Cases added: the T1 gate tests, the T0.5 `-packed` copies and `test-packed-bulk-update-criteria-refused`. Write the final table in `PLAN_NOTES.md`: cases
before, deleted (reason), converted, added, after.

### T6 Docs

`docs/reference/changelog.md` (breaking entry, upgrade steps: install
2026.11.2, start once to completion, then upgrade); `docs/reference/error-codes.md`
(name each new refusal string: not-in-the-packed-layout (both variants),
stale-schema-line, invalid-rebuild-metadata, unreadable/malformed schema.conf,
and the `bak_sv != 2` restore refusal); the changelog lists as breaking that
stale token-less `schema.conf` lines now refuse start; `AGENTS.md` / `CLAUDE.md` (legacy layout statements,
the BTRI migration paragraph, the "2026.10.2 adds real NULL ... startup
migration" paragraph, minimum supported source); `docs/concepts/typed-records.md`,
`storage-model.md`, `indexes.md`, `ordinary-type-integration.md`;
`docs/query-protocol/index-management.md`; `docs/reference/limits.md` (legacy
65535 cap text); `docs/getting-started/*`; `docs/operations/deployment.md`; `docs/cli/index.md` (the `(./migrate)` row), `docs/operations/benchmarks.md`
("vs the legacy layout": keep as history, it describes a measurement); comments
in `src/db/shard_db.h` and
`config.c` that still describe startup migration.
`src/cli/main.c` is unchanged (`describe-object` keeps the key). Also update
the `build.sh` "Upgrades:" banner (it still names 2026.09.2 clean-open
evidence and, with required == current, would read "at or after 2026.11.2 and
before this binary's version").

### T7 Verification and gates

1. Full suite fresh.
2. **Upgrade proof** (manual, paste output), run twice: once with a build of
   git tag `2026.11.2` (the released binary), once with a build of the
   pre-change `main`. For each: `git worktree add ~/legacy-base-<tag>
   <ref>` (not under `/tmp`, which is tmpfs RAM here; remove the worktree
   afterwards), `SKIP_TESTS=1 ./build.sh` there. With it, start a daemon on a
   scratch DB_ROOT, `create-object` with indexed varchar/int/bitmap fields,
   bulk-insert ~10k records, a few updates/deletes. (i) **Clean-stop root:**
   record `count`, `find` on an indexed criterion and `get` of 5 keys, stop
   cleanly, start the new binary on it; results must equal the recorded
   values. (ii) **Crash root:** during a second bulk insert `kill -9` the
   daemon, `cp -a` the root to A and B, start the old (tag/main) binary on A
   and the new binary on B; `count`, `find` and `get` must match between A
   and B. If no KFM2 marker was left, repeat the kill with a larger insert up
   to three times, then accept and record that no marker was observed.
3. **Negative proof**: with the pre-change `main` TEST_BUILD daemon
   (`shard-db-test-server`, the only binary that accepts
   `declaration_layout:"legacy"`) create a legacy object; the new production
   `build/bin/shard-db` refuses to start with the hint, and `shard-db version`
   still works.
4. `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`, three consecutive `run-all
   --jobs 6`; `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`, three consecutive
   `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" run-all
   --jobs 3`; `rm -rf /tmp/shard-db-*` after each run.
5. Hand back uncommitted with `git diff --stat` and `PLAN_NOTES.md`.
