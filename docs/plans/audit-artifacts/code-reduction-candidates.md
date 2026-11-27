# Phase 1 step 8 — code-reduction candidates (2026-11-25; list only, nothing deleted)

Tools: (a) symbol-reference scan (python: function definitions in `src/db/*.c`
whose name occurs exactly once across every `src/**/*.c`, i.e. no caller in
daemon, tests or bench; header-only references and macros not counted, so each
row needs a human check of `types.h`/`slotcask.h` and external consumers),
(b) the coverage baseline's never-executed list (`coverage-baseline.txt`,
21,371 entries incl. variants — not used as a deletion signal alone),
(c) normalized-line duplication scan: 12-line windows, whitespace-normalized,
comments/includes dropped (own script; threshold 12 lines; **jscpd not
installed**), (d) size hotspots.

## (a) Functions with no caller anywhere in `src/**/*.c` (31)

Estimated removable ≈ 560 lines if all confirmed. Risk is low unless noted;
TEST_BUILD-only seams are KEPT per the campaign rule.

| Candidate | Lines | Note |
|---|---|---|
| config.c `load_splits` (3), `load_fields_conf` (55) | 58 | legacy loaders; verify no external/embedded use |
| index.c `extract_field_value` (24), `build_composite_value` (32), `cmp_str_numeric` (5), `cmp_str_raw` (3), `cmp_str_numeric_raw` (5), `sort_dedup_file` (40) | 109 | index-build helpers superseded by typed paths |
| io_direct.c `od_prefetch` (8), `od_varchar_content_max` (4), `od_load_i64_be` (6), `od_load_i32_be` (5), `od_load_i16_be` (4), `od_load_f64` (3), `od_load_f32_le` (3) | 33 | O_DIRECT scan decode helpers; packed path replaced them |
| keyset.c `keyset_capacity_hint_slots` (3) | 3 | |
| query_find.c `print_record_json` (24), `print_record_dict` (27), `print_record_row` (28), `csv_emit_row` (22) | 101 | pre-typed emit helpers |
| query_plan.c `search_criterion_move` (1) | 1 | |
| server.c `shard_db_offline_init` (7) | 7 | check shard_db.h public API before removing |
| slotcask.c `slotcask_scan_live_kf` (24), `slotcask_walk_one_shard_streaming` (51), `kf_find_slot_for_hash` (27), `slotcask_walk_live_checked` (3) | 105 | **recovery/durability adjacent — hold until oracle-covered** |
| storage.c `storage_register_recovery_callback` (3) | 3 | **recovery path — hold** |
| typed_index.c `typed_index_field_key_alloc` (27), typed_row.c `typed_encode_packed_into` (9) | 36 | |
| TEST seams: query.c `query_bm_test_race_sized_total`, query_plan.c `get_ordered_find_keyset_max_for_test` | — | kept (TEST_BUILD seams) |

### (a2) cppcheck `--enable=unusedFunction` cross-check

Raw: `cppcheck-unusedFunction.txt` (222 findings over `src/db` only; 105 are
`*_test_*` seams, kept). The remaining 117 include everything with no caller
inside `src/db` — which also flags the embedded public API (`shard_db_open`,
`shard_db_query`, …), functions used only by `src/test` / `src/bench`

(raw log removed 2026-11-26; regenerate with the command above).
(`slotcask_get`, `slotcask_exists`, `index_parallel`, `reindex_object`,
`slotcask_count_live`, …) and legacy planner helpers
(`compile_criteria_tree`, `criteria_match_tree`, `match_typed`, `card_est_*`).
It is a superset of (a) and needs the test/bench reference check before any
row is acted on; the (a) table (zero callers anywhere under `src/`) is the
confirmed-by-scan subset.

## (b) Duplication (12-line windows, own script)

Overlapping windows beyond the first occurrence: slotcask.c 170, query_bulk.c
160, query.c 51, query_plan.c 45, test_control.c 18, others <8. Hot spots are
the bulk-insert/update value-compute families in `query_bulk.c`
(`v2_bulk_upd_delim_value_compute`, `v2_bulk_upd_json_value_compute`,
`bulk_upd_json_run`) and the window publish/finalize variants in
`slotcask.c`. **Hold until oracle-covered** (durability paths); the
`query_bulk.c` value-compute family is a refactor candidate once the
bulk-update oracle exists.

## (c) Size hotspots (function length, approximate; regex-based)

`bulk_ins_run` (query_bulk.c, ~5000 lines incl. nested helpers),
`cmd_aggregate_do` (~2600), `cmd_bulk_update` (~2500), `server_process_fast`
(~2450), `parse_criteria_json` (~2300), `v2_bulk_upd_delim_value_compute`
(~2050), `dispatch_json_query` (~1550), `typed_encode_defaults` (~1550),
`update_compose_new_from_old` (~1400). (config.c `value_is_braced_uuid` ~2600
is a brace-counting artifact of char literals, not a real function.) File
sizes: slotcask.c 12.9k, query.c 11.8k, query_aggregate.c 7.7k, query_bulk.c
6.6k lines.

## Disposition

Index rows (kind `refactor`) are not individually created for (a): treat the
table above as one batch row **AUD-REDUCE-1**: remove confirmed caller-less
helpers in Phase 4's refactor lane, excluding the hold items. Everything else
here is a hold until its mode has oracle coverage.
