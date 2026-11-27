# Legacy layout removal — T0 inventory (2026-11-27)

Produced for `docs/plans/2026-11-25-remove-legacy-layout.md` task **T0**.
No source was edited to produce this file.

- Branch: `refactor/remove-legacy-layout`
- HEAD at inventory time: `f3828ad` (`f5e41e0` is an ancestor; the branch
  additionally carries the dependabot `main` merges up to `f3828ad`)
- Base build: `SKIP_TESTS=1 ./build.sh` — green
- Base suite: `./build/bin/shard-db-test run-all --jobs 8` — 351 registered
  case files / see run log below

## 1. Exact counts (measured, not estimated)

Symbol occurrences under `src/db/*.c` and `src/db/*.h` (`grep -o`, comments
included):

| Symbol group | Occurrences | Files |
|---|---:|---:|
| `packed.nfields` | 89 | 17 |
| `packed_layout` | 8 | 4 |
| `layout_packed` (incl. `layout_packed_decl_line`) | 16 | 4 |
| `layout_migrat*` / `layout_convert*` / `layout_walk*` / `layout_write_fields*` / `layout_test_crash*` | 25 | 5 |
| `BTRI_MIGRATION_STAMP` / `btri_stamp*` / `btri_migration*` | 20 | 3 |
| `legacy_field_absent` / `typed_span_copy_legacy` / `read_legacy_*` / `predicate_legacy_*` / `match_typed_legacy` / `legacy_numeric_increment` / `legacy_field_query_exists` | 59 | 9 |

Union of the groups above: **217 occurrences in 24 files** (the Brief's
"about 250 sites in 25 files" is the right order of magnitude). Per file
(lines containing a hit, `grep -c`):

| File | Lines | Class(es) |
|---|---:|---|
| `src/db/typed_read_command.c` | 44 | B, C |
| `src/db/query_find.c` | 32 | A, B, C |
| `src/db/embedded.c` | 24 | A |
| `src/db/storage.c` | 17 | B, C |
| `src/db/config.c` | 13 | A, B |
| `src/db/query_plan.c` | 11 | B, C |
| `src/db/query_schema.c` | 10 | B |
| `src/db/typed_row.c` | 9 | B, C |
| `src/db/typed_index.c` | 8 | B, C |
| `src/db/query_bulk.c` | 7 | B |
| `src/db/query_aggregate.c` | 6 | B, C |
| `src/db/objlock.c` | 6 | D |
| `src/db/types.h` | 5 | B, D |
| `src/db/schema_wire.c` | 3 | B, D |
| `src/db/query_maint.c` | 3 | B |
| `src/db/index.c` | 3 | B, D |
| `src/db/typed_bulk_insert.c` | 2 | B |
| `src/db/shard_db_internal.h` | 2 | A |
| `src/db/query_join.c` | 2 | B |
| `src/db/typed_row.h` | 1 | C |
| `src/db/slotcask.h` | 1 | C |
| `src/db/query.c` | 1 | B |
| `src/db/packed_csv_command.c` | 1 | B |
| `src/db/agg_exact.c` | 1 | B |

Additional record-format inventory used by D5 (not in the counts above):
`SREC_FLAG_NULLMAP`-only handling, `typed_field_base` legacy arm,
`typed_record_view` legacy arm (`config.c`), `typed_encode`/`typed_decode`,
`typed_encode_trim_len`, `read_legacy_*` (`typed_read_command.c`),
`legacy_field_query_exists`/`match_typed_legacy`/`predicate_legacy_*`
(`query_plan.c`), `legacy_numeric_increment` (`storage.c`).

## 2. Function / symbol classification

`A` = startup migration, `B` = legacy object layout, `C` = legacy record
format (no nullmap flag / legacy fixed-width codec), `D` = keep.

### Class A — removed in T2

| Symbol | Site |
|---|---|
| `BTRI_MIGRATION_STAMP`, `btri_stamp_test_inject`, `g_btri_stamp_test_fail`, `btri_migration_stamp_exists`, `btri_migration_stamp_write` | `embedded.c` |
| BTRI step in `shard_db_recover_before_stamp` (`btri_migration_ran`, stamp writes) | `embedded.c` |
| `LAYOUT_MIGRATION_STAMP`, `layout_stamp_exists`, `layout_stamp_write`, `LAYOUT_MIGRATION_REPORT`, `layout_report_write`, `layout_report_remove`, `layout_migration_run` | `embedded.c` |
| `shard_db_not_null_upgrade_audit` | decision: **removed in T2** (default in plan step T0.1; acts only on disk versions below 2026.10.2, which the T1 gate refuses anyway) |
| `layout_migrate_object`, `layout_convert_record`, `LayoutWalkCtx`, `layout_walk_cb`, `layout_diag_key`, `layout_write_fields_checked`, `layout_test_crash_point`, `layout_migrate_test_crash_phase`, `g_layout_crash_phase` | `query_find.c` |
| `layout_packed_decl_line` | `config.c` |
| `BTRI_STAMP_TEST_FAIL` and layout crash-point env parsing | `config.c` |
| Declarations: `layout_migrate_object`, `layout_test_crash_point`, `layout_packed_decl_line` (`types.h`); `btri_stamp_test_inject`, `layout_migrate_test_crash_phase` (`shard_db_internal.h`) | headers |

### Class B — legacy object layout, removed T3/T4

| Symbol | Site | Removed |
|---|---|---|
| `Schema.packed_layout` + `schema_tail_layout`, `schema_conf_layout`, `update_schema_conf_layout` | `types.h`, `config.c` | T3 |
| `load_schema` legacy branch (token-less line), `load_typed_schema` legacy parse (`packed_declared_type`-gated), `layout_packed` local | `config.c` | T3 |
| `cmd_create_object` `packed` parameter / legacy validation branch, `ensure_schema_conf_line` `storage_version < 2` branch, `describe-object` layout string, `cmd_backup`/`cmd_restore` layout arms | `query_schema.c`, `query_maint.c`, `server.c` | T3 |
| `server.c` `legacy_fixture`, TEST_BUILD `declaration_layout:"legacy"` create branch | `server.c` | T3 |
| `rebuild_txn_begin` `schema_conf_layout` snapshot, `RebuildTxn.old_layout_packed`, abort's layout restore | `objlock.c` | T3 (meta `old_layout` wire stays class D) |
| Every `ts->packed.nfields` packed-vs-legacy dispatch | `query_aggregate.c`, `agg_exact.c`, `query_join.c`, `query_bulk.c`, `typed_bulk_insert.c`, `query_plan.c`, `query.c`, `query_schema.c`, `query_find.c`, `storage.c`, `index.c`, `typed_read_command.c`, `typed_index.c`, `packed_csv_command.c`, `schema_wire.c`, `typed_row.c`, `config.c` | named T4 step |
| `typed_field_base` / `typed_bitmap_of` legacy arms, `srec_flags_valid` accepting `flags == 0` | `types.h` | T4 (D5/P1) |

### Class C — legacy record format, removed T4 under D5/P1

`legacy_field_absent` (`storage.c`), `legacy_field_query_exists`,
`match_typed_legacy`, `predicate_legacy_text_span`,
`predicate_legacy_named_text` (`query_plan.c`), `typed_span_copy_legacy`
(`typed_row.c`), `legacy_numeric_increment` (`storage.c`),
`read_legacy_open/close/span/component/text/cell/csv_row`
(`typed_read_command.c`), `typed_encode_alloc` (`typed_row.c`),
`typed_encode`/`typed_decode`/`typed_decode_bounded_size`,
`typed_encode_trim_len`, `typed_record_view` legacy arm, `typed_decode_stream`
legacy arm. These are only removed where P1 holds (it does — §3).

### Class D — kept (do not delete)

- `typed_legacy_decl_size` (`schema_wire.c`; used by
  `validate_packed_field_type` in `config.c`, `query_find.c`,
  `query_schema.c`), `typed_schema_build_packed`, `packed_schema_same`,
  `reindex_object_durable_checked_ex` (also `index.c`),
  `reindex_clean_legacy`, `index_conf_replaces_legacy_bare`, v1 text-count
  fallback.
- `recover_unambiguous_legacy_layout`, `has_legacy*` helpers,
  `data.legacy`, `.rebuild_legacy_root` recovery (`objlock.c`) — pre-
  `RebuildTxn` rebuild artifacts; layout-independent recovery.
- `RebuildTxn.legacy_data`, `rebuild_txn_legacy_root` — the staged copy of
  `data` used by every vacuum/rebuild, not layout code.
- The rebuild-txn `old_layout` meta line: `write_meta` writes it,
  `parse_meta` reads it. T3 keeps the line, writes constant `old_layout=1`,
  and tightens absent/0 to invalid (fail closed); the gate's separate raw
  reader (`rebuild_txn_active_layout`) does not get that tightening.
- Unrelated "legacy" vocabulary that stays: `BT_MAGIC_V1/V2/V3` rejection
  in `btree.c`, the legacy text wire protocol in `server.c`, v1 storage
  refusal in `config.c`.

## 3. Proofs P1–P6 (re-confirmed with citations)

**P1 — conversion rewrites every live record with PKR flags and counts it.**
`layout_walk_cb` (`src/db/query_find.c`) sets
`o.record_hdr2 = typed_schema_hdr2(ctx->new_ts)` before
`slotcask_insert_with_hooks`, and `layout_migrate_object` fails the
conversion when `wc.error || wc.bad || wc.live_count != expected`
(`expected = src_total - src_deleted`). `typed_schema_hdr2` returns
`PKR_FLAGS` when `ts->packed.nfields` is set (`src/db/typed_row.c:101`);
`pkr_view` rejects `flags != PKR_FLAGS` (`src/db/packed_record.c:229`).
So a packed object cannot contain a non-PKR record, and D5 holds.

**P2 — converted declarations equal what `create-object` writes.**
`layout_packed_decl_line` (`src/db/config.c:2058`) changes only a legacy
`numeric:N` to `numeric:N,2`; everything else is copied verbatim. The
post-publish check in `layout_migrate_object` compares the reloaded schema
with `packed_schema_same`. `cmd_create_object` builds its own packed
schema from the same field lines with `typed_schema_build_packed`
(`src/db/query_schema.c`, after the fields loop), the same builder the
migration uses.

**P3 — layout-conversion artifact shape.**
`parse_meta` (`src/db/objlock.c`) treats `old_layout` as optional and
defaults it to `0` ("absent = legacy (pre-2026.11.1 meta)").
`recover_transaction_layout` aborts/strips the packed layout for
`old_layout=0`; `.rebuild_txn.preparing` / `.rebuild_txn.done` and an
active txn with `old_layout=1` are normal vacuum states.

**P4 — NOOP for disk == required == current.**
`shard_db_version_decide` (`src/db/util.c:832-834`): when the disk version
compares equal to `current_version` it returns `SHARD_DB_VERSION_NOOP`.

**P5 — only BTRI is accepted.**
`BT_MAGIC` is `0x42545249` (`'BTRI'`, `src/db/btree.h:39`);
`bt_open_file` rejects every other nonzero magic (`src/db/btree.c:758`).

**P6 — D10 breaking refusal evidence.**
`test-startup-validator` phase 5 (`src/test/cases/test_startup_validator.c`)
appends `ghost_tenant:obj:16:16:2:1` with no object directory and asserts
start is **not** refused (`ASSERT_TRUE(!refused, ...)`). The fixture has no
`.layout-migrated`. Under D10 the gate must refuse this token-less stale
line; T1 changes the fixture (adds `layout=packed` to the ghost line) and
covers the token-less stale case with gate test (b2). P1–P6 do not
contradict the plan.

## 4. Test assignment table

> **Amended 2026-11-27 (T0.5 resume):** this table is superseded by the
> authoritative C/K/D classification in §4b below, which follows D4/D12 and
> the T0.5 detection rule. The per-criterion notes here are kept for the
> search evidence.

### 4a. Original T0 assignment (search evidence)

Search axes from T0.3: (i) `declaration_layout`, (ii) literal schema.conf
lines without `layout=packed`, (iii) hand-built `TypedSchema`, (iv)
hdr2 0/`SREC_FLAG_NULLMAP` / `legacy_*`, (v) hand-written rebuild meta /
`data.legacy` / `.rebuild_legacy_root`, (vi) object loaded without a
schema.conf line / non-`g_db_root` root, (vii) daemon restart or
`shard_db_open` on a legacy object / `.version` below 2026.11.2.

The "Task" column is the **first task that breaks the case** (and therefore
the task that converts or deletes it).

### (i) `declaration_layout` (98 occurrences / 33 files)

| File | Cases | Subject | Task |
|---|---|---|---|
| `test_agg_varchar_groupby_sum.c` | `test-agg-varchar-groupby-sum` | fixture-only | T0.5 convert |
| `test_all_field_types.c` | `test-all-field-types` | fixture-only | T0.5 convert |
| `test_alloc_error_propagation.c` | `test-alloc-error-propagation` | fixture-only | T0.5 convert |
| `test_bitmap_resplit.c` | `test-bitmap-resplit-bulk` (packed already) | fixture-only | T0.5 (no change) |
| | `test-bitmap-resplit-legacy` | legacy-layout duplicate of bulk | **T3 delete** |
| `test_bulk_cas.c` | `test-bulk-cas` | fixture-only | T0.5 convert |
| `test_bulk_insert_arena_oom.c` | `test-bulk-insert-arena-oom` | fixture-only | T0.5 convert |
| `test_bulk_update_json.c` | `test-bulk-update-json` | fixture-only | T0.5 convert |
| `test_bulk_update_json_oom.c` | `test-bulk-update-json-oom` | fixture-only | T0.5 convert |
| `test_cas_tree_if.c` | `test-cas-tree-if` | fixture-only | T0.5 convert |
| `test_composite_varchar_bound.c` | `test-composite-varchar-bound` | fixture-only | T0.5 convert |
| `test_computed_update.c` | `test-computed-update` | fixture-only | T0.5 convert |
| | `test-computed-update-legacy-nonfinite` | legacy-record non-finite semantics | **T1 delete** (creates a legacy object, then restarts the daemon on that root; the T1 gate refuses the restart) |
| `test_field_vs_field.c` | `test-field-vs-field` | fixture-only | T0.5 convert |
| `test_get_fields.c` | `test-get-fields*`, `test-startup-refuses-unreadable-kf-count` | fixture-only | T0.5 convert |
| `test_index_oversized_varchar.c` | `test-index-oversized-varchar` | fixture-only (declared caps, not 65535) | T0.5 convert |
| `test_index_varchar_unescape.c` | 14 cases | fixture-only | T0.5 convert |
| `test_layout_manifest.c` | `lm_build` fixture (most cases) | mixed | T0.5: `lm_build` → packed + drop `lm_schema_append(":layout=packed")`; malformed-token case replaces the token; auto-key case converts its inline create |
| | `test-layout-migration-converts-legacy`, `-refusals`, `-crash-points`, `-finance-acceptance` | migration subject | **T1 delete** |
| | `test-layout-token-packed-loads`, `test-layout-startup-agreement` | token handling on a legacy object | **T1 delete** |
| | `test-loader-same-object-two-tenants` | per-tenant layout independence (`"tb legacy"`) | **T1 delete** (restarts the daemon with a token-less `tb:o`; the T1 gate refuses the root) |
| | `test-create-object-packed-explicit`, `test-create-object-packed-refusals`, `test-schema-conf-rewrite-fsync-failure`, `test-packed-recompose-ops-supported` | packed behaviour | keep |
| | `test-backup-restore-layout` | packed backup + forged non-packed backup refused | **T3 edit** (D3 message text) |
| `test_legacy_recompose.c` | all 3 cases | legacy recompose semantics | **T3 delete** (object creation removed; subject class C) |
| `test_length_ops.c` | `test-length-ops` | fixture-only | T0.5 convert |
| `test_not_null_edit_gate.c` | `test-not-null-edit-gate` (`gl` block) | legacy-generation record surgery | **T1 delete `gl` block** (the `gl` legacy object is still on the root when the case restarts; the T1 gate refuses it) |
| `test_not_null_updates.c` | `test-not-null-updates` | fixture-only | T0.5 convert |
| `test_null_legacy_rebuild.c` | `test-null-legacy-rebuild-trims` | legacy record surgery | **T4 delete** |
| `test_or_logic.c` | `test-or-logic` | fixture-only | T0.5 convert |
| `test_r1_routes.c` | `test-r1-routes` | packed commands + explicit legacy layout identity | **T3**: convert fixture creates to packed; delete legacy-identity assertions |
| `test_rebuild_txn_recovery.c` | all cases | rebuild-txn recovery | T0.5 convert `create_object_with_records` to packed; meta fixture gains `old_layout=1` and the host stream count |
| `test_regex.c` | `test-regex` | fixture-only | T0.5 convert |
| `test_removed_storage_surfaces.c` | `test-heavy-vacuum-transaction` | fixture-only | T0.5 convert |
| `test_request_timeout.c` | `test-request-timeout` | fixture-only | T0.5 convert |
| `test_scalar_conversion_safety.c` | `test-scalar-conversion-safety` | fixture-only | T0.5 convert |
| `test_scalar_grammar_routes.c` | `test-scalar-grammar-routes` | public grammar; legacy fixture | T0.5 convert each create to packed |
| `test_schema_export.c` | `test-schema-export` | fixture-only | T0.5 convert |
| `test_slotcask_v2_bulk.c` | `test-slotcask-v2-bulk` | fixture-only | T0.5 convert |
| `test_type_correctness_acceptance.c` | `test-type-correctness-acceptance` | packed subjects + TEST_BUILD legacy control | **T1**: delete the `control` legacy block/assertion (it is still on the root at the case restart, so the gate refuses); rest unchanged |
| `test_varchar_overflow.c` | `test-varchar-overflow` | declared cap 8 (not the 65535 cap) | T0.5 convert |

### (ii) schema.conf fixtures without `layout=packed`

| File | Line | Object | Task |
|---|---|---|---|
| `test_agg_packed_routes.c` | `":%s:%d:32:2:1\n"` | hand-built **packed** object | T0.5 add `:layout=packed` |
| `test_indexed_zero_exists.c` | `":obj:8:32:2:1\n"` | hand-built **packed** object | T0.5 add `:layout=packed` |
| `test_not_null_compat.c` | `default:obj:8:16:2:1` + ghost | subject = `shard_db_not_null_upgrade_audit` (offer) | **T2 delete with the audit** |
| `test_startup_validator.c` | `default:v:16:16:2:1` fallback, `ghost_tenant:obj:...` | daemon restart / stale line | **T1** add `layout=packed` (both) |
| `test_reindex_spill_collision.c` | `../escape:sweep:8:16:2:1` | hostile path containment probe | keep (T1 gate test (f) uses the same shape) |

### (iii) hand-built `TypedSchema`

| File | Shape | Task |
|---|---|---|
| `test_coverity_join_buf_overflow.c` | legacy fixed-width varchar, raw record | **T4 delete** (legacy renderer removed) |
| `test_not_null_writes.c` | legacy `typed_encode` unit block (item 8) | **T4 delete block** |
| `test_null_review_fixes.c` | nullmap-only records | **T4 delete** |
| `test_variable_length.c` | legacy `typed_encode_trim_len` | **T4 delete** |
| `test_agg_packed_routes.c`, `test_packed_json_projection.c`, `test_packed_named_projection.c`, `test_r1_routes.c`, `test_indexed_zero_exists.c` | already `typed_schema_bind_packed` | keep |

### (iv) hdr2 0 / `SREC_FLAG_NULLMAP` / legacy codec calls

| File | Task |
|---|---|
| `test_typed_row_adapter.c` (legacy carry/copy), `test_whole_record_readers.c` (legacy + nullmap views), `test_null_storage.c` (nullmap-only), `test_null_review_fixes.c`, `test_packed_json_projection.c` (`p_legacy_controls`), `test_single_write_integration.c` (`w_legacy_render`, legacy OLD cases), `test_owning_write_hooks.c` (`own_empty_and_legacy` legacy generation), `test_r2_predicate_verification.c` (legacy composite checks), `test_typed_bulk_insert.c` (legacy raw seed), `test_typed_bulk_update.c`, `test_slotcask_basic.c`, `test_slotcask_grave_run_frontier.c`, `test_o_direct_scan.c`, `test_od_scan_healthy_tail.c`, `test_varlen_compact_crash_mid_migration.c` | **T4**, at the step named by the symbol removed |
| `test_packed_csv_commands.c`, `test_packed_csv.c`, `test_packed_record_storage.c` | packed (PKR_FLAGS) — keep |

### (v) hand-written rebuild meta / recovery artifacts

| File | Task |
|---|---|
| `test_rebuild_txn_recovery.c` (`test-rebuild-txn-recovery-idempotent`) | T0.5: add `old_layout=1` (and the real host `old_streams`) |
| `test_rebuild_txn_recovery.c` legacy recovery cases (`test-rebuild-legacy-stage1-safe-restore`, `stage2-no-data`, `ambiguous-refuses-start`) | keep (class D recovery, layout-independent) |
| `test_packed_recompose.c` residue assertions naming `data.legacy`/`.rebuild_legacy_root` | keep |

### (vi) object loaded without a schema.conf line / non-`g_db_root` root

| File | Task |
|---|---|
| `test_agg_packed_routes.c`, `test_indexed_zero_exists.c`, `test_r2_predicate_verification.c`, `test_packed_*` unit cases | use their own tmp root but always with a schema.conf line; T0.5 adds the token where missing |
| `test_layout_manifest.c` `test-loader-packed-with-legacy-fields-refused` | keep (a packed token + legacy-shaped fields must be refused) |

### (vii) daemon restart / `shard_db_open` on legacy roots, `.version` below 2026.11.2

| File | Task |
|---|---|
| `test_version_compat.c`, `test_version_gate_2026_11.c` | **T1 edit** expectations (D11) |
| `test_layout_manifest.c` migration cases (rewrite `.version` to 2026.10.2) | **T1 delete** |
| `test_rebuild_txn_recovery.c` (restarts on roots; objects become packed at T0.5) | T0.5 convert |
| `test_layout_manifest.c` `test-layout-migration-finance-acceptance` (restarts on a legacy object) | **T1 delete** |
| `test_computed_update.c` `test-computed-update-legacy-nonfinite` (restarts on a legacy object) | **T1 delete** |
| `test_not_null_edit_gate.c` `gl` block (restarts on a legacy object) | **T1 delete block** |
| `test_type_correctness_acceptance.c` `control` (restarts on a legacy object) | **T1 delete block** |
| `test_layout_manifest.c` `test-loader-same-object-two-tenants` (restarts with `tb:o` token-less) | **T1 delete** |

### 4b. Authoritative C/K/D classification (T0.5, 2026-11-27)

Rule applied: only to cases that create a legacy object. **K** = a request is
`bulk-update` carrying `criteria` (judged per case, including split string
literals and `dry_run`), or conversion failed for a listed K reason (legacy
computed-update accounting, legacy composite-key bounds, legacy numeric
semantics not re-expressible, arena/OOM fault indices into the legacy bulk
path, packed per-key bulk `if` gap). **C** = legacy object was only a
fixture and conversion succeeded with expectation fixes. **D** = subject is
removed code; deleted in the owning task.

| Test / case | Class | Task / packed copy |
|---|---|---|
| `test_agg_varchar_groupby_sum` | K (deleted T3) | packed copy `test-agg-varchar-groupby-sum-packed` |
| `test_alloc_error_propagation` | K (deleted T3) | packed copy (scenario 2 only) `test-alloc-error-propagation-packed`; 1a/1b gap |
| `test_bulk_cas` | K (deleted T3) | packed copy `test-bulk-cas-packed` |
| `test_bulk_insert_arena_oom` | K (deleted T3) | **no packed copy** (legacy arena fault points; gap) |
| `test_bulk_update_json` | K (deleted T3) | packed copy `test-bulk-update-json-packed`; CAS blocks gap |
| `test_bulk_update_json_oom` | K (deleted T3) | **no packed copy** (legacy per-key OOM fault points; gap) |
| `test_cas_tree_if` | K (deleted T3) | packed copy `test-cas-tree-if-packed`; bulk `if` block gap |
| `test_composite_varchar_bound` | K (deleted T3) | **no packed copy** (legacy composite-key bounds; gap) |
| `test_computed_update` main | K (deleted T3) | packed copy `test-computed-update-packed` |
| `test-not-null-updates` | K (deleted T3) | packed copy `test-not-null-updates-packed` |
| `test_index_oversized_varchar` | K (deleted T3) | packed copy `test-index-oversized-varchar-packed` |
| `test_length_ops` | K (deleted T3) | packed copy `test-length-ops-packed` |
| `test_field_vs_field` | K (deleted T3) | packed copy `test-field-vs-field-packed` |
| `test_or_logic` | K (deleted T3) | packed copy `test-or-logic-packed` |
| `test_regex` | K (deleted T3) | packed copy `test-regex-packed` |
| `test_request_timeout` | K (deleted T3) | **no packed copy** (criteria timeout is refused; packed 1.5M seed does not reproduce the legacy timing fixture; gap) |
| `test_slotcask_v2_bulk` | K (deleted T3) | packed copy `test-slotcask-v2-bulk-packed` |
| `bulk-update-criteria-varchar-unescape` | K (deleted T3) | **no packed copy** (entire subject is the criteria route; gap) |
| `varchar-malformed-escape-policies` | K (deleted T3) | packed copy `varchar-malformed-escape-policies-packed`; per-record-skip site gap |
| `test_all_field_types` | C | converted (numeric `2` → `8,2`) |
| `test_get_fields` | C | converted (numeric `2` → `8,2`) |
| `test_removed_storage_surfaces` (`test-heavy-vacuum-transaction`) | C | converted |
| `test_rebuild_txn_recovery` | C | converted (host stream count; meta `old_layout=1`) |
| `test_scalar_conversion_safety` | C | converted (`z:numeric:19,0`; packed precision overflow message) |
| `test_scalar_grammar_routes` | C | converted (NUL-exact varchar; packed message fragments) |
| `test_schema_export` | C | converted (`size:8`, `capacity:40`) |
| `test_varchar_overflow` | C | converted (packed over-length message) |
| `test_index_varchar_unescape` C cases (12) | C | converted; K originals untouched |
| `test_layout_manifest` C parts (`lm_build`, token cases, auto-key, malformed-token) | C | converted; migration/token cases D at T1 |
| `test_agg_packed_routes`, `test_indexed_zero_exists` | C fixture | `:layout=packed` added to hand-built schema.conf |
| `test_not_null_edit_gate` | C (rest) + D `gl` | `gl` deleted at T1; rest already packed |
| `test_bitmap_resplit` `test-bitmap-resplit-legacy` | D | delete T3 |
| `test_layout_manifest` `test-layout-migration-*`, `test-layout-token-packed-loads`, `test-layout-startup-agreement`, `test-loader-same-object-two-tenants` | D | **deleted T1** |
| `test_computed_update` `test-computed-update-legacy-nonfinite` | D | **deleted T1** |
| `test_not_null_edit_gate` `gl` block | D | **deleted T1** |
| `test_type_correctness_acceptance` `control` block | D | **deleted T1** |
| `test_r1_routes` legacy-identity assertions | D | delete T3 |
| `test_packed_recompose` dual-layout loops | D | drop legacy half T3 |
| `test_legacy_recompose` | D | delete T3/T4 |
| `test_null_legacy_rebuild` (`test-null-legacy-rebuild-trims`) | D | **deleted T1** (corrected: it creates a legacy object and restarts, so the T0.5 T1-break rule governs, not T4) |
| `test_typed_row_adapter`, `test_whole_record_readers`, `test_null_storage`, `test_null_review_fixes`, `test_variable_length`, `test_coverity_join_buf_overflow`, `test_not_null_writes` legacy block, `test_packed_json_projection` `p_legacy_controls`, `test_single_write_integration` legacy parts, `test_owning_write_hooks` legacy case, `test_r2_predicate_verification` legacy composite, `test_typed_bulk_insert`/`test_typed_bulk_update` legacy seeds | D | delete at the named T4 step |
| `test_not_null_compat` | D | **deleted T2** with `shard_db_not_null_upgrade_audit` |
| `test_null_migration` (6 cases) | D | **deleted T2** — its entire subject is the BTRI startup migration (the plan's T0 known-hits line names it); the T0 table omitted it explicitly |

Refusal pin added in T0.5: `test-packed-bulk-update-criteria-refused`
(`src/test/cases/test_packed_bulk_update_criteria_refused.c`).

## 5. Open items recorded for later tasks

- `lm_build` is the only shared fixture that appends `:layout=packed` to a
  legacy-created object; after T0.5 makes it create packed, every
  `lm_schema_append(&m, ":layout=packed")` call must be dropped (a
  duplicate token is a parse error in `startup_schema_line`).
- `test_rebuild_txn_recovery.c` creates with `"streams":1` only because the
  legacy fixture bypasses the host-stream check. Converting it to packed
  requires dropping the explicit stream count and writing the host value
  into the hand-written meta.
- `test_startup_validator.c` phase 5 is the P6 evidence; it is edited at
  T1, not T0.5.

## 6. T1 status (2026-11-27)

T1 added `shard_db_layout_gate` (`embedded.c`, declared in
`shard_db_internal.h`) and `rebuild_txn_active_layout` (`objlock.c`, declared
in `types.h`), called from both `shard_db_open` and `cmd_server` before any
instance/recovery; `SHARD_DB_REQUIRED_SOURCE_VERSION` is now 2026.11.2.
Deleted the D-class T1 cases listed above (7 in `test_layout_manifest.c`,
`test-computed-update-legacy-nonfinite`, the `test-not-null-edit-gate` `gl`
block, the `test-type-correctness-acceptance` `control` block) plus
`test-null-legacy-rebuild-trims` (policy correction above). Added
`test-legacy-root-refused` and `test-legacy-root-refused-daemon`; edited the
version tests, `test-startup-validator` token fixtures, and
`test-auto-vacuum`'s runtime schema rewriter (it dropped the token). No
source outside the gate/version scope changed.

## 6b. T2 status (2026-11-27)

T2 removed both startup migrations: the BTRI reindex migration (stamp,
test seam, strict-reindex loop and both stamp writes in
`shard_db_recover_before_stamp`), the layout migration
(`layout_migration_run`, stamp/report helpers and call), and
`shard_db_not_null_upgrade_audit` (T0 decision: removed). The recovery order
in `shard_db_recover_before_stamp` (bmfix → `was_clean` early return →
bmfix → KFM2 replay) is unchanged; only the BTRI stamp writes and migration
calls went. `query_find.c` lost the legacy→packed converter; class-D
`packed_schema_same` and the `layout_diag_key` helper (used by the packed
rebuild path) were kept. `config.c` lost `layout_packed_decl_line` and the
`BTRI_STAMP_TEST_FAIL` / `LAYOUT_MIGRATE_TEST_CRASH` env parsing. Stale
`.btri-migrated` / `.layout-migrated` files are now ignored (D6), while the
gate still detects `.layout-migrated` for its message variant. Tests
deleted: `test_null_migration` (6 cases) and `test_not_null_compat`
(1 case). `build/bin/datetime-migration-fixture` left untouched as the plan
requires.

## 6c. T3 status (2026-11-27)

- Class K originals deleted after checking each `PLAN_NOTES.md` entry against
  its `-packed` copy and running every packed copy green (17 whole files + 2
  cases in `test_index_varchar_unescape.c`). Recorded gaps: item 15 of
  `PLAN_NOTES.md` (no packed copy for arena/JSON-OOM/composite-bound/request-
  timeout/criteria-unescape).
- `test-bitmap-resplit-legacy` deleted (its `br_open`/`br_run_bulk` `legacy`
  parameter is gone too); `test_legacy_recompose.c` (3 cases) deleted.
- `test-r1-routes` creates packed and asserts the packed layout identity;
  `test_packed_recompose`'s two dual-layout loops are packed-only.
- `test-backup-restore-layout` asserts the D3 hint;
  `test-create-object-packed-refusals` pins the `"legacy"` create refusal.
- Added `test-rebuild-txn-2026-11-2-meta` (hand-written 2026.11.2 manifest
  with `old_layout=1`; recovery consumes it and serves all records).
- Production code: legacy creation/loading removed (`declaration_layout`
  legacy branch, `cmd_create_object` `packed` parameter + legacy validator,
  `Schema.packed_layout`, `load_schema`/`load_typed_schema` legacy branches,
  backup/restore layout arms, `ensure_schema_conf_line` v1 form,
  `update_schema_conf_layout`, abort's layout restore). `write_meta` writes
  constant `old_layout=1`; `parse_meta` requires it; the gate's raw reader is
  unchanged.
- Full suite: `1..729`, `# total: 2434764 passed, 0 failed across 729 cases`
  (751 → 729; −23 deleted, +1 added).

## 6d. T4 status (2026-11-27)

All 18 named steps completed in plan order. `packed.nfields` dispatches went
from 89 occurrences to **6** invariant/validation checks (listed in
`PLAN_NOTES.md` §T4). Deleted class C machinery: `legacy_field_absent`,
`typed_span_copy_legacy`, `read_legacy_*`, `match_typed_legacy`,
`predicate_legacy_*`, `legacy_numeric_increment`, `v2_update_new_from_old`,
`apply_computed_fields`, `parse_computed_span`, `v2_compute_free`, the
`join_candidate_components` legacy ambiguity path, and the six legacy bulk
bodies/workers in `query_bulk.c` (file 6 500 → 2 526 lines). `cmd_bulk_update`
is the packed refusal; a NULL schema returns the dispatch's
`Object [<object> not found]` text. Tests converted/trimmed at T4:
`test-r1-fetch-skip`, `test-r2-predicate-verification`,
`test-typed-row-adapter`, `test-whole-record-readers`,
`test-coverity-join-buf-overflow` (converted), `test-null-review-payload-cap`
(converted), `test-packed-json-projection`, `test-single-write-integration`.
Full suite: `1..729`, `# total: 2434590 passed, 0 failed across 729 cases`
(729 → 729; no whole case deleted). `objlock.c` (A at T2, B at T3) and
`slotcask.c` / `btree.c` had no A–C site left; class D kept.

## 6e. T5/T6 status (2026-11-27)

T5: final case ledger and symbol counts are in `PLAN_NOTES.md` §T5 — 750
before, +18 added, −39 deleted, 729 after; `packed.nfields` 89 → 7
occurrences (6 lines, all invariant checks), migration symbol groups → 0,
legacy record symbol group 59 → 0. T6: changelog breaking entry, error-codes
layout/startup table, AGENTS.md, the named concept/reference/getting-started/
operations/CLI docs, `build.sh` banner and stale source comments updated;
searches found no remaining stale references outside historical plans,
release notes and the kept benchmark history.

## 7. Execution status (2026-11-27, resumed under D4/D12)

T0 completed. The first T0.5 attempt halted because the original premise
(convert every fixture-only test) was false for legacy-route tests. The plan
was amended with D4/D12 and T0.5 was re-run: class C tests are converted,
class K originals stay legacy with additive `-packed` copies (or a reported
gap), class D tests are left for their owning task, and
`test-packed-bulk-update-criteria-refused` pins the packed refusal. See
`PLAN_NOTES.md` for every converted/dropped assertion and every recorded
packed gap.

