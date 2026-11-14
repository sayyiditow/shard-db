# R5 plan review — 2026-11-02-r5-metadata-migration-recovery-plan.md (rev 1, untracked)

Reviewer: end-to-end review of the plan before execution. Every code anchor, API
assumption, and test-harness reference in the plan was checked against the working
tree at HEAD `b6911a1` (grep + source read; ~70 distinct claims verified).

## Verdict

**Executable after two must-fix corrections and five small fixes.** The plan's
factual foundation is remarkably solid: all ~40 quoted code anchors are byte-exact
and unique, every referenced API signature/semantics matches the source, every test
harness helper exists with the exact names used, and the hard correctness questions
(slot_size agreement across restart, packed-vs-legacy fixed-width equivalence,
version-stamp file format, marker-name plant, crash-point state machine) all check
out. The two must-fix items are a wrong factual claim in F11 with a wrong RED
prediction (Task 1) and a wrong test name (Task 7e) — both are executor-halt risks
under the plan's own discipline, not code-correctness problems.

## Must-fix before execution

### 1. F11's mechanism is wrong, and Task 1's RED prediction for `test-layout-token-with-auto-key-parses` cannot happen (plan lines 82, 327, 445–462)

Verified reality (`embedded.c:400–434`): `startup_schema_line` caps `strtok_r` at 7
parts (`part && n < 7`), and the validity check is `n < 6 || n > 7` — an 8-token
line (`auto_key` **and** `layout=packed`) stops filling at 7, so `n == 7`, the
`n > 7` rejection **never fires**, the 8th token is silently dropped, and the line
is **accepted**. Additionally, the 7th token (`auto_key=…`) is never validated at
all (the string `auto_key` does not appear in embedded.c).

Consequences:

- F11's statement "the daemon would refuse to start on a valid line" is wrong —
  the actual hazard is the opposite (silent truncation/ignore), which is still a
  real reason to need hunk 1f, but the mechanism must be corrected.
- The plan's RED expectation "with-auto-key-parses fails (daemon refuses: 8
  parts)" is wrong: on base the daemon starts, the assertion
  `test_env_start_at(...) == 0` passes, and the case is **GREEN on base**. The
  executor is instructed to paste a RED that cannot exist — under CORE-PROCESS
  halt discipline this either halts the run or invites rationalizing.

The hunk 1f itself is correct and still required (without it the startup validator
silently swallows `layout=packed` after `auto_key`). Recommended repair: correct
F11's text; keep `test_layout_token_with_auto_key_parses` as a **regression pin**
(GREEN on base, GREEN after Task 1) and give Task 1 a *demonstrable* RED by
extending the case with a malformed 8th token — append `:layout=legacy` after
`auto_key=uuid`: on base the daemon starts (bad token truncated away), with 1f it
refuses (8 parts, invalid token at `parts[7]`). That yields a true RED→GREEN.

### 2. Task 7e names a test that does not exist (plan line 1890)

`test-rebuild-txn-recovery` is not registered anywhere. The real registrations in
`src/test/cases/test_rebuild_txn_recovery.c:507–516` are
`test-rebuild-txn-recovery-idempotent`, `test-rebuild-txn-crash-after-stage`,
`test-rebuild-txn-crash-after-walk`, `test-rebuild-txn-crash-after-metadata-splits`,
etc. Fix the name (and ideally say "the whole `test-rebuild-txn-*` family"), or the
executor hits a missing-case error mid-run.

## Should fix (small, before execution)

3. **Task 5b interim-value footgun (line 1150).** The hunk block literally contains
   `int create_packed=1;` with a prose note to apply it as `=0` until Task 11. For a
   no-judgment executor, write `int create_packed=0;` in the hunk itself and have
   Task 11 flip it. Applying the block verbatim would flip the default four tasks
   early and cascade failures through Tasks 6–10.
4. **Task 5d skips default-value validation for packed varchar/numeric/currency.**
   `validate_packed_field_type`'s second block excludes varchar/numeric/currency
   from `typed_default_validate`, so `name:varchar:20:default=<200-char-string>`
   is accepted at packed create where legacy would validate the default against the
   type. (The modifier *structure* is checked — `parse_default_modifier_checked`
   strips in place, verified — only the value check is skipped.) Add the check for
   the packed branch or record it as a known gap (R5b candidate).
5. **Task 7c `update_schema_conf_layout` lacks the `g_db_root` prefix guard.**
   `schema_conf_layout` verifies `effective_root` starts with `g_db_root` before
   computing `dir`; 7c computes `effective_root + strlen(g_db_root)` unchecked. All
   current callers pass `db_root + "/" + dir`, but the one-line guard matches the
   sibling's defensive style and removes an out-of-bounds footgun.
6. **D11 says the migration report is "≤ 20 lines"; the implementation bounds by a
   2048-byte buffer** (plus ≤ 8 diagnostics per object across multiple objects).
   Align the spec wording with the implementation (or vice versa).
7. **Double-stamp on first start.** `layout_migration_run` stamps 2026.11.1
   (F16 rule), then `cmd_server` stamps again afterwards (`stamp_required` was
   computed before recovery). Harmless and idempotent (same bytes), but note it in
   the plan so the executor doesn't misread two stamp writes in evidence output.

## Verified clean (checked against source; no action)

- **All Part A anchors** byte-exact and unique: `types.h:345–346` (Schema tail),
  `types.h:930` (load_schema decl, sole occurrence repo-wide), `config.c:1025–1028`
  (load_splits wrapper), `config.c:921/978` (legacy_v1 pair), `config.c:999`
  (max_value comment), the entire `load_typed_schema` (`config.c:1706–1869`) incl.
  all three early-return cleanups and the nullmap-prefix block,
  `startup_schema_line` (all four anchors), the db.env knob block
  (`config.c:430–455` — the `} else if … #endif` insertions keep braces balanced;
  line 455 supplies the closing brace), `shard_db_internal.h:690–696`,
  `version.h` (both defines; decide/stamp enums exactly as the plan's test asserts,
  incl. `SHARD_DB_VERSION_STAMP_OK` in the stamp-result enum), `build.sh:360/376`
  registration lines.
- **The 4-line "Cache miss" Task 2b(ii) anchor is unique** even though its first
  line alone appears twice in config.c (`load_fields_conf` at 1053) — the plan
  quotes the longer block, which matches only at 1723–1728.
- **All Part B anchors** byte-exact and unique: server.c layout guard (1412–1417),
  `cmd_create_object` call (1435–1440) and its decl (`types.h:1572`; grep shows
  exactly types.h/server.c/query_schema.c), `cmd_create_object` internals —
  validate line (1085), overflow pair (1139–1140, and the inserted
  `if (packed) continue;` is correctly placed: the accumulate line is the loop's
  last statement), `nfields == 0` (1143) **precedes** the max_key ceiling (1159),
  so 5e(iv)'s `packed_ts` scoping compiles; fields.conf write (1377–1385);
  schema.conf append (1442–1461) with `auto_key_kind`/`auto_seq_name`/`exists`/
  `schema_path` all in scope and `f` reused at 1467 (plan's note is correct);
  slot_size pair (1490–1491); the response prints `value_size` only (5e(viii)
  accurate). **`eff_root` exists** in `cmd_create_object` (1363–1364), so 5e(vi)'s
  unlink path is valid.
- **5d is correct**: `parse_default_modifier_checked` strips `:not_null`/
  `:nullable`/`:default=` in place (`config.c:1453+`), so `buf` is a clean type
  token before `pkr_field_parse`/`typed_legacy_decl_size`, mirroring the legacy
  `validate_field_type` exactly. `packed_record.h` is indeed not included in
  query_schema.c (plan's add-include instruction needed and correct);
  `parse_default_modifier_checked`/`typed_legacy_decl_size`/`typed_default_validate`
  are all visible there (types.h / schema_wire.h).
- **Guards in Task 6**: the `rebuild_txn_begin` call site (`query_find.c:1127`) has
  `slot_changed`/`drop_tombstoned`/`n_added`/`old_ts` in scope (all parameters);
  `rebuild_object`'s locals (`old_sch` value, `old_ts` pointer, `new_splits`,
  `new_streams`, `splits_changed`, `streams_changed` — 1369–1399) all precede the
  `/* Build new TypedSchema:` anchor (1400); the hypothetical 13-argument
  `rebuild_object_v2(...)` call maps 1:1 onto the real parameter list
  (`query_find.c:1113–1119`, decl `query_internal.h:129–135`). **`&same` shallow
  copy is safe**: the `!ctx->slot_changed` verbatim branch
  (`v2_rebuild_walk_cb`, query_find.c:886–905) returns before any `new_ts`
  dereference; all `new_ts` uses are in the slot_changed recompose path. `cmd_add_fields`
  is in config.c (4414) and `cmd_edit_fields` in query_schema.c (606) exactly as
  the plan's 6c/6d file assignments say; both guard anchors unique, `old_ts` in
  scope at 6d's site (623).
- **RebuildTxn (Task 7d)**: struct tail, `write_meta` fprintf, all three
  `parse_meta` anchors, the `begin` anchor (exactly one blank line before
  `struct stat st;`), and the `abort` 4-line anchor (544–547) — all exact-once.
  Insertion order in abort is sound: data restore (528–536) → fields.conf restore
  (538–542) → splits/streams → **layout** → cache invalidation → conditional
  reindex. `rebuild_txn_legacy_root` exists (499–501) and returning
  `txn->active` as a slotcask object root is correct (staged `data/` lives under
  `active/`).
- **Critical slot_size agreement holds.** `load_schema` derives
  `slot_size = round8(24 + max_key + max_value)` floor 32 with
  `max_value = ts->total_size` (`config.c:999–1008`); the packed binder sets
  `total_size` from `pkr.maximum` (F13), and `pkr_slot_bound` is documented as
  "include existing 24-byte header, actual key bytes, align-8 and floor 32"
  (`packed_record.h:69–74`). The two formulas coincide, so Task 8's
  loader-agrees check passes and slot_size is stable across restart (no
  segment-misalignment hazard).
- **Fixed-width equivalence, all 16 types**: packed (`packed_fixed_width`,
  typed_row.c:11–26) vs legacy (`parse_field_type`, config.c:1517–1629) —
  short 2/2, int 4/4, long 8/8, float 4/4, double 8/8, bool/byte 1/1, date 4/4,
  **datetime 7/7, time 3/3, datetimems 8/8**, timestamp 8/8, uuid 16/16,
  ipv4 4/4, ipv6 16/16, enum 1-or-2 both. The conversion's width-mismatch hard
  fail cannot trigger for a valid schema; the binder additionally enforces
  equivalence.
- **Version stamp facts**: `.version` is literally `<db_root>/.version` with
  content `<version>\n` (mkstemp → fsync → rename → dir fsync;
  `embedded.c:258–301`); the reader strips one trailing `\n` and rejects embedded
  newlines. Every test assumption matches (`lm_make_pre_migration`'s
  `"2026.10.2\n"`, `lm_assert_untouched`'s strcmp, the F16 assertion). Clean flag
  is `<db_root>/.shard-db.clean` ✓.
- **Task 9(b) marker plant works as designed**: `object_has_pending_markers`
  scans `<obj>/data/kf` and `000_batch_1_marker.dat` matches the legacy final
  pattern (`%x_batch_%u_marker.dat`) ⇒ pending (name-only check, contents never
  read) ⇒ refuse + preserve ✓.
- **Startup structure**: `shard_db_recover_before_stamp` (`embedded.c:767–863`)
  runs not-null audit → rebuild_recovery → not-null audit → BTRI block → the
  clean-flag pair (819–820, unique repo-wide) → clean early-return / marker sweep.
  The 8e insertion point is exactly as the plan describes ("after the BTRI block
  and before the clean-flag early return"). Both `cmd_server` and `shard_db_open`
  order check → recover → validate → stamp ✓ (embedded combines recover+validate
  in one `if`; order preserved). Task 0's verification questions are answerable
  as the plan expects.
- **Conversion core APIs all match**: `typed_row_open`/`typed_row_span`
  (span has `is_null`), `typed_span_copy_legacy` (returns 0/-1, zero-fills to the
  legacy width — the plan uses it correctly and 65537 ≥ max legacy field size),
  `legacy_field_absent` (storage.c:1272 — legacy decoder skips all-zero fields,
  so absent→NULL mapping is rendering-faithful), `typed_schema_hdr2` (0x03 packed /
  0x01 otherwise), `slotcask_open(db,dir,splits,streams,slot_size)`,
  `slotcask_walk_live` (0=continue/1=stop), `slotcask_validate_live_refs`,
  `slotcask_sum_kf_totals`, `slotcask_insert_with_hooks` (8 args, NULL result
  legal — same idiom in-tree at query_find.c:895–898),
  `slotcask_registry_invalidate`, `pkr_field_parse`/`pkr_measure`/`pkr_record_fits`
  (param order as planned)/`pkr_compose`/`pkr_numeric_encode`/`pkr_slot_bound`,
  `DEC_OK`/`DEC_PAYLOAD_MAX`/`PKR_VARCHAR_MAX=4194304`,
  `SLOTCASK_SEG_MAX_BYTES` (usable in `_Static_assert`),
  `reindex_object_durable_checked_ex` (index.c:4781, same call shape as the BTRI
  migration), `conf_read_line`/`MAX_FIELD_DEF=65536`, `set_count`/
  `reset_deleted_count` (v2 no-op stubs — harmless), `invalidate_schema_caches`/
  `invalidate_idx_cache`. `typed_schema_release` does not exist yet (plan adds it —
  no collision); no `layout_*`/`g_layout_*` symbols exist anywhere.
- **The numeric conversion math is exact** (traced): `-0.05`, `0.00`, `12.34`,
  `-` sign, INT64_MIN magnitude (`0u - u` promotes correctly); scale-0 suppresses
  the decimal point; buffers `padded[48]`/`text[80]` cannot overflow at scale ≤ 18.
  The `pf->scale != scale` refusal can only fire for declarations 7a cannot
  faithfully rewrite (e.g. a legacy bare `numeric`, if such an object exists) —
  fail-closed per D5, acceptable.
- **Memory management in Task 7a/8b is clean**: builder failure paths free
  `tf`/enum lists/ptypes exactly where needed (no double-free: `tf` is freed
  explicitly on every pre-increment BP_FAIL path); `decl[]` aliases `all[]` and
  only `all` owns the strings; the `*old_ts = *cached` snapshot is never
  dereferenced after cache invalidation (walk completes before publish) and freed
  shallow (cache owns the enum lists).
- **Crash-point state machine traced for all 7 phases × 3 runs**: consistent
  throughout — first-start stamps before converting (F16), resume after crash via
  the NOOP decision skipping the clean-flag precondition, `rebuild_recovery`
  aborts the interrupted txn (restoring data + fields.conf + token via 7d(v)),
  `after-commit` leaves the object packed with only the completion stamp
  remaining, and the fresh-empty-root case bypasses D7 preconditions via the
  `nlegacy == 0` early return. The `after-publish` F16 assertion (9b) is
  observable as claimed.
- **Test harness**: every referenced symbol exists with the exact name/signature —
  `TestEnv`/`test_env_start_at(env, db_root, port)`/`test_env_stop_keep`/
  `test_env_kill`/`test_pick_port`, `TestClientCfg{host,port}`/`tc_connect`/
  `tc_request`/`tc_close`, `tu_read_file`/`tu_file_exists`/`tu_parse_count`/
  `tu_run_cmd`, `t_ctx->failed`, `TEST_REGISTER`, `ASSERT_TRUE`/`ASSERT_EQ_INT`/
  `ASSERT_NOT_NULL`. Multiple-cycles restart on one `TestEnv` is an established
  pattern (test_null_migration.c, test_durability_ordering.c); hand-editing
  schema.conf + restart exists (test_vacuum_streams_mismatch.c). Knob env plumbing
  (db.env TEST_BUILD block) matches the existing BTRI knob structure exactly.
- **Task 3c audit list matches reality**: version literals in src/test/cases are
  confined to exactly the 7 files the plan names. `test_version_gate.c` passes its
  own literals (plan's "not changed" is correct). All docs anchors verified
  (changelog `**BREAKING (R4…**` once; error-codes row once; typed-records line +
  `## On-disk encoding` once; schema-mutations headings/vacuum row/create-object
  sentence — two of them continue past the quoted phrase, but they work as prefix
  replacements with the trailing text and spacing preserved; remove-field's key is
  `"fields"` per docs, so Task 12a's request shape is right; AGENTS.md phrase
  once). `docs/release-notes/2026.11.1.md` does not exist yet (2026.10.2.md does —
  no conflict). Parent-plan alignment confirmed (R5 row = "3C + Phase 8",
  `docs/plans/2026-10-19-type-correctness-recovery-plan.md:64`).
- **query_find.c includes**: `<unistd.h>/<errno.h>/<string.h>` arrive transitively
  via types.h (not direct) — the plan's "add the include if the compiler warns"
  stance is correct; `<signal.h>` is genuinely absent and 8b's conditional add is
  needed. LOG_ERROR/LOG_AUDIT/LOG_SUB_CONFIG all usable there.

## Other observations (no action required)

- Task 5a's `test-create-object-packed-refusals` is GREEN on base (the existing
  guard rejects b1–b5 all alike); the plan itself acknowledges and permits this —
  recorded here so the executor doesn't second-guess it.
- The §5 "SUPERSEDED OUTLINE" mapping, the D9 fail-closed-first ordering, the
  Task 11 stop-and-hand-back triage gate, and the uncommitted/sanitizer-deferral
  posture all comply with CORE-PROCESS.md and the repo's standing exceptions
  (sanitizers are the R6 gate per AGENTS.md; nothing is pushed before then).
- Test isolation is sound: mkdtemp roots, picked ports, per-daemon-process knobs;
  no process-global state leaks between cases; the crash knob kills the daemon
  child, not the runner.
- Naming collision to keep an eye on: D9's offer to "write Task 12b" (recompose)
  clashes with the final Part B Task 12b (docs). If the human takes the D9 offer,
  number the new task 12c+ or a Part C.

## Recommendation

Apply fixes 1–2 (must) and 3–7 (small), then execute. No finding challenges the
plan's architecture: the stamp-before-publish ordering, the RebuildTxn-based
per-object conversion with layout restore on abort, the fail-closed guard set, and
the audit-before-touch D5/D7 preconditions are all sound and correctly anchored.
