# Plan: Phase 3 slice 1 fixes (single insert / update / delete)

Source findings: `2026-11-25-audit-single-writes.md`. Status: APPROVED by the human 2026-11-28 and
EXECUTED (uncommitted on `fix/single-write-audit-slice1`); see the execution notes at the end.

## Brief

**Problem and root cause.** Slice 1 found defects in the single-record write path that the
human has decided to fix now:
- AUD-P3-2 (major): `insert` accepts `"key":""`, so a record exists that `get`/`exists`
  refuse and that makes `keys`/`fetch` fail for the whole object. Root cause: no empty-key
  check on the single-write paths (`cmd_insert_v2_impl` only checks `klen > max_key`).
- AUD-P3-7: `prepare_json` (`typed_row.c`) walks the request's members, matches each against
  the schema and silently skips a name that matches nothing, so a typo writes nothing and
  still reports success (an update still stamps `auto_update`).
- AUD-P3-9: single `update`/`delete` accept `dry_run` (undocumented; `cas.md` says single
  writes always execute) and the update dry run only checks existence and `if`, so it answers
  `would_update` for patches the real update rejects. `insert` has no dry run (today a
  `dry_run:true` on insert is ignored and the insert really happens).
- AUD-P3-1, AUD-P3-3, AUD-P3-10 (minor, same functions): double reply on a guarded invalid
  auto-key; oversize key prints to stderr and sends no reply; delete's unescaped `err_buf`
  and its two silent `return_record` drops.

**Logical flow.**
1. One key validator `single_write_key_check(sc, klen, mode)` (insert / update / delete).
   Empty key is an error in all three. Oversize key is an error for insert only; update and
   delete keep today's not-found answer.
2. `prepare_json` takes a flags word. With `STRICT`, a member name that matches no field at
   all fails with `unknown field '<name>'`. A name that matches a removed (tombstoned) field
   is rejected the same way (decided: consistent before and after `vacuum compact`). Strict is reachable only
   through two new `_ex` entry points used by the single-write constructors; the existing
   `typed_insert_alloc` / `typed_update_computed_alloc` keep their signatures and stay
   lenient (bulk and every other caller unchanged).
3. Dry run for single writes, built by calling the exact same constructor the real write
   calls (`v2_insert_owned` / `v2_update_owned` over a locally built context), so it cannot
   drift from the real constructor's checks (it cannot see index-apply I/O errors, the bitmap
   distinct-value cap, or the marker/commit stages):
   - `update`: `would_update`, or the same error the real update gives.
   - `insert`: `would_insert` / `would_update` (key exists) / `condition_not_met` exactly as
     the real call. With a `default=seq(...)` field the dry build uses placeholder `0` and
     never calls `seq_next_val`. With an auto-key object and an omitted key, the server does
     not call `auto_key_generate`; it replies `would_insert` with `"key":null`.
   - `delete`: already validates existence and `if`; kept, documented.
4. `cmd_delete_v2`: error text goes through `single_write_error` (escaped); the
   `return_record` JSON is rendered inside the check function, before any commit, so an
   allocation/render failure rejects the delete instead of deleting and dropping the record.
5. Guard branch in `server.c`: no second reply after an `auto_key_normalize` failure.
6. Docs and changelog (breaking notes below).

**Decisions made.**

| Decision | Alternatives | Why | Tag |
|---|---|---|---|
| Empty key rejected on single insert/update/delete: `{"error":"key must not be empty"}` | also reject on read side | decided by human; reads already reject it | DECIDED |
| Unknown field (matches no field, live or removed) rejected on single insert/update | stay lenient + document | decided by human | DECIDED |
| A name matching a removed (tombstoned) field is rejected like any unknown name on single insert/update | keep ignoring (documented, two tests pin it) | `vacuum compact` drops tombstoned lines, so leniency would silently flip to an error at the next compact; one consistent rule is safer. Cost: breaking for clients still sending a removed field, and two existing tests are rewritten | DECIDED (human) |
| Strictness only through new `_ex` functions used by single writes; bulk-insert/bulk-update/encode helpers unchanged | strict everywhere now | bulk is slices 2-3; keeps old signatures so bulk code and tests need no edits | DEFAULT |
| Dry run reuses the real constructors on an unlocked OLD snapshot | reimplement checks; run under kf lock | reuse prevents drift (the AUD-P3-9 defect); dry run is race-tolerant by contract | DEFAULT |
| Insert dry run included | skip insert | human asked "where it makes sense" | DEFAULT |
| Insert dry run never advances a sequence (seq default placeholder; omitted auto-key not generated, reply `"key":null`) | consume | burning sequence values is a side effect | DECIDED |
| `dry_run` on single writes is a documented feature; `would_*` never carry `record`; the dry run covers everything the real constructor and the shared size checks catch, not index-apply I/O, the bitmap distinct-value cap, or a later seq value that overflows its field | remove it | decided by human | DECIDED |
| `dry_run:true` on single insert changes from "really inserts" to "previews" | keep ignoring | needed for the feature; called out in changelog as a semantic change | DEFAULT |
| AUD-P3-4 deferred to the find slice; AUD-P3-8 gets its own plan after a fault-injection repro | include | different code / needs design | DEFAULT |
| Bulk-insert already rejects empty keys (`typed_bulk_insert.c` key validation); no change there | add check | already covered | informational |

**Blast radius.**
- Wire (breaking): single `insert`/`update` with a member matching no field now errors;
  empty key now errors; `dry_run:true` on single insert now previews. Additive: dry-run
  responses on insert, stricter validation in update dry run. No on-disk change.
- Code: new functions only where signatures would change. `cmd_insert` keeps its signature;
  a new `cmd_insert_ex(..., dry_mode)` carries the dry mode and `cmd_insert` calls it with 0
  (so `types.h:1286`, `test_ordered_walk_kfcache_deadlock.c:82`, the server.c CLI insert
  call and the CLI insert call near `server.c:2740` stays on `cmd_insert`; the three
  JSON-handler insert call sites switch to `cmd_insert_ex`). `cmd_insert_v2_impl` gains `dry_mode`; its callers
  are `cmd_insert_v2` and the TEST_BUILD wrapper `shard_test_single_insert` (`storage.c`,
  declared in `single_write_test.h`, used by `test_single_write_integration.c`), which pass 0.
  `typed_insert_alloc` (`typed_bulk_insert.c:221`, `test_typed_bulk_insert.c:210`) and
  `typed_update_computed_alloc` (`test_r1_routes.c:97-104`) are untouched wrappers.
  `prepare_json` internal callers: `typed_encode_alloc` / `typed_encode_packed_into`
  (`index.c:1278,1382`), `typed_update_json_alloc`, `typed_bulk_insert.c` all lenient.
- The embedded API (`shard_db.h`, only `shard_db_query`) reaches these through
  `dispatch_json_query`; no direct `cmd_insert` caller exists there.
- Known unrelated bug noted, not fixed: `src/cli/main.c:419` builds the CLI single insert
  with `"data"` instead of `"value"`.

**Test strategy.** (`[R]` = regression pin that passes on base too; all others fail on base.)
Empty key rejected on insert/update/delete (AK_NONE object) and `keys`/`fetch` work after
the attempt; oversize key on insert returns a JSON error; update/delete oversize key still
not-found `[R]`; unknown field rejected on insert and update, nothing written, no
`auto_update` stamp; removed-field name rejected (insert and update), with the two existing removed-field tests rewritten to assert the rejection; per-record bulk-update still ignores a removed-field null `[R]`;
duplicate member last-wins `[R]`; bulk-insert with an unknown member still inserts `[R]`;
update dry run returns the identical error string to the real update for bad int, NULL on
not-null, non-JSON, computed errors, bad index key, and `would_update`
with the record and `auto_update` unchanged for a valid patch; insert dry run:
`would_insert`, `would_update`, `condition_not_met`, bad index key, writes
nothing, `default=seq` and `auto_key=seq` sequences unchanged (read `sequence` before and
after), omitted auto-key reply; delete dry run unchanged `[R]`; delete: new TEST_BUILD seam
forces a `return_record` render failure and the record must remain; delete error escaping is
covered by sharing `single_write_error` (code-read, stated, not a separate test); guarded
invalid auto-key yields exactly one reply line, and an invalid guard JSON still yields
`invalid guard criteria` `[R]`.

Oversize-record dry-run cases are dropped: on packed objects `slot_size` is derived from the
schema maximum and the 128 MiB segment cap is unreachable under default limits; the shared
`single_write_finish` path is exercised by the existing oversize tests.

**Out of scope.** AUD-P3-4, AUD-P3-8, bulk unknown-field strictness, read-side empty key,
`dry_run` on bulk-insert (slice 2), per-key bulk paths, the CLI `"data"` bug.

**Review log.** Round 1 (Opus, full): 0 blockers, 2 silent-damage majors, 6 clarity majors,
11 minors. All addressed in this revision (see ledger in the session scratchpad, not
committed). Round 2 (Opus, delta): 0 blockers, 0 silent-damage majors, 6 clarity majors, 12
minors; all applied in place (test-seam placement and arming, guard reply chain, free of
constructor output, dropped unbuildable oversize tests, placeholder `"1"`, render after CAS,
dry-run test-hook double park, three-run sanitizer rule). Exit criterion met. Removed-field decision made by the human: reject (applied).

## Plan (executor-facing)

Branch off `main`: `fix/single-write-audit-slice1`. Build `SKIP_TESTS=1 ./build.sh`; test
`./build/bin/shard-db-test run <name>` per task, `run-all --jobs 8` at the end; leave
uncommitted for review. Sanitizers: The diff adds an atomic test seam and moves a render into the check
function that runs under the kf write lock, so the AGENTS.md three-consecutive-run rule applies
to both ASan+UBSan and TSan. If any described symbol or behavior is not as stated,
write `PLAN_NOTES.md` and halt; if a decision is not covered, ask.

### T1 Key validation (AUD-P3-2, AUD-P3-3)
- Test first (`test_single_write_key_validation.c`, new): on an AK_NONE object over the
  wire: insert `key:""` -> `{"error":"key must not be empty"}`, absent, `keys`/`fetch`
  succeed; update and delete `key:""` -> same error; insert key of `max_key+1` bytes ->
  `{"error":"key exceeds max_key (N)"}` (not an empty reply); update/delete of an oversize
  key -> unchanged Not found / not_found `[R]`. (Auto-key objects already reject an omitted
  or empty key earlier with `Missing key for auto_key=...`; docs note both texts.)
- Code: `static int single_write_key_check(const Schema *sc, size_t klen, int mode)` in
  `storage.c`; prints the error through `single_write_error` and returns 1, else 0. Call it
  in `cmd_insert_v2_impl` after the `!ts` check, replacing the stderr branch (mode insert);
  in `cmd_update_v2_impl` after the `!ts` check and before the dry-run branch, and in
  `cmd_delete_v2` before `slotcask_registry_get` (mode update/delete: only empty is an
  error; oversize falls through to today's not-found path). Skipped when the insert is a
  dry run with an omitted auto key (T3).

### T2 Unknown-field rejection (AUD-P3-7)
- Test first (`test_single_write_unknown_field.c`, new): insert `{"nmae":"x","req":"r"}` ->
  `unknown field 'nmae'`, key absent; update `{"nn":9}` -> same error, record and
  `auto_update` stamp unchanged; a member name whose encoded length exceeds 1530 bytes (`6*(255)`) ->
  `unknown field (name too long)`; computed member (`{"$incr":1}`) under an unknown name ->
  unknown-field error, not a computed error; removed-field name -> `unknown field 'email'` on insert and update;
  duplicate member last-wins `[R]`; `bulk-insert` with an unknown member still inserts `[R]`.
- Known existing tests that relied on ignoring removed-field names, rewritten in T2 (the only
  permitted test edits): `test_remove_field.c` "Insert ignores tombstoned" and "Update ignores
  tombstoned" blocks (send `email` after remove-field -> assert `unknown field 'email'`, record
  unchanged; add a valid write without `email` to keep the existing assertions), and
  `test_not_null_updates_packed.c` "single update ignores removed-field null" (`"x":null` after
  remove-field -> assert `unknown field 'x'`; the per-record bulk-update assertion below it is
  unchanged `[R]`).
- After T2, run the full suite once. If any OTHER existing test relies on lenient unknown
  members on single insert/update, halt with `PLAN_NOTES.md` (do not edit the test).
- Code (`typed_row.c`/`.h`): add `unsigned flags` to static `prepare_json` replacing the
  `defaults` int's neighbors minimally (keep `defaults`; add `flags`, bit
  `TYPED_PREP_STRICT`). In the member loop track whether any schema field name matched
  (live fields only: a removed field's name does NOT count as a match; keep the existing
  `f->removed` skip for the non-STRICT path); if none matched and STRICT, `snprintf(err,cap,"unknown field '%s'",
  name)` then `errno=EINVAL; goto fail` (do not use `row_error`: it is not printf-style).
  When `name` is NULL (encoded key longer than 1530 bytes) use the text `unknown field (name
  too long)`. A name containing `\u0000` truncates in the message; acceptable. Free `name`
  on both paths.
  New public functions `typed_insert_alloc_ex(ts, old, json, root, object, flags, out, len,
  err, cap)` and `typed_update_computed_alloc_ex(ts, old, json, flags, out, len, err, cap)`;
  the existing two become one-line wrappers with flags 0. `v2_insert_owned` and
  `v2_update_owned` call the `_ex` versions with STRICT.

### T3 Dry run (AUD-P3-9)
- Diagnostic step first: confirm that `upsert_adapter_value_compute` (slotcask.c) performs no
  post-constructor check beyond null output, `vlen > UINT32_MAX`/`SIZE_MAX-24-klen-7`, and
  `slotcask_record_size_varlen(klen,vlen)` against `slot_size` and `slotcask_seg_max_bytes()`,
  all of which `single_write_finish` (called at the end of both constructors) already applies
  with the same formula, so the dry run needs no extra call. If the adapter does more, halt
  with `PLAN_NOTES.md`.
- Test first (`test_single_write_dry_run.c`, new): per the strategy list; for update, each
  failure input asserts dry-run reply == real-update reply byte for byte (run dry first,
  then real, on a fresh copy of the record).
- Code, shared: hoist `load_index_fields` / `load_index_types` ahead of the dry-run branch
  in `cmd_update_v2_impl` and into the dry path of insert (they are loaded only after the
  branch today, and the real constructors need them for `typed_index_validate_new`). Move
  the escape block of the update error reply into `single_write_error`, keeping the TEST_BUILD
  `g_shard_test_json_escape_fail` hook and the `"update failed"` fallback inside it (the
  fallback is already a parameter), so `test_computed_update_packed.c` still passes; the
  hook then also applies to insert/delete replies in TEST_BUILD, which no test sets.
- Code, update: in the dry branch after the `if` check, build a local `V2UpdateCtx` like the
  real path (partial_json, idx fields/types/ts, db, key_len, `return_record=0`), a
  `SlotcaskOldRecord{old_val,old_vlen,old_hdr2}`, call `v2_update_owned`, then the adapter
  checks from the diagnostic step; on any failure print the same reply the real error branch
  prints (same `err_buf` text through `single_write_error`); on every return, success or failure, `free(out)` (the constructors return -1 with `*out`
  still allocated when index validation or `single_write_finish` fails), then reply
  `would_update` on success. Set `return_record=0` and free `ret_json` if set. Calling
  `v2_update_owned` fires the TEST_BUILD `slotcask_test_after_old(1)` park outside the kf
  lock; the dry branch must not call it twice: guard the existing `slotcask_test_after_old(0)`
  call so only one of the two fires (keep `(0)` for the dry path, skip `(1)` via a `dry`
  field on the ctx checked inside `v2_update_owned`).
- Code, insert: `cmd_insert_ex(db_root, object, key, klen, value, if_json, if_not_exists,
  return_record, dry_mode)` with `dry_mode` 0 = off, 1 = dry run, 2 = dry run with the key
  omitted (auto-key objects). `cmd_insert` calls it with 0; the server insert branch passes
  `dry_run` from the request and calls `cmd_insert_ex`. Server: for an auto-key object with
  an omitted key and `dry_run:true`, do not call `auto_key_generate`; call `cmd_insert_ex`
  with `dry_mode` 2 and a NULL key (`if` with an omitted key keeps its existing error).
  `cmd_insert_v2_impl` dry branch: skip the key check, `compute_hash_raw` and `format_wire_key` when `dry_mode` is 2 and use
  the real generated-key width (16 uuid, 8 seq) as `klen` for the capacity check; unless 2, look up
  the key with `slotcask_get_ex`; apply `if_not_exists`/`if` with the real
  `condition_not_met` + `current` reply; evaluate `if` by calling `v2_insert_check_fn` over the same local `V2InsertCtx` (so a typed
  condition error matches the real text), then call `v2_insert_owned` (old = record or NULL)
  with the dry-default flag set and `return_record=0`, `free(out)` on every return; reply
  `would_insert` or `would_update` (`would_insert` with `"key":null` for mode 2). The wrapper
  `shard_test_single_insert` and `cmd_insert_v2` pass 0.
- Dry defaults: `V2InsertCtx` gains `dry` (set only by the dry branch); `v2_insert_owned`
  passes `TYPED_PREP_DRY_DEFAULTS` to `typed_insert_alloc_ex` when set. In
  `prepared_defaults_mode` (which gains its own flags parameter, since `typed_insert_alloc_ex` calls it directly rather than via `prepare_json`; `typed_prepared_defaults` passes 0), with that flag a `DK_SEQ` field is given the literal `"1"` (the sample the schema probes in `query_schema.c` use; valid for enum lists containing `1`) and
  `generate_default_owned` is not called (so `config.c` and `seq_next_val` are untouched);
  other generators (`uuid()`, `random(N)`, timestamps) have no persistent side effect and run
  normally.
- `cmd_delete_v2`: no change to dry-run behavior.

### T4 Delete and guard cleanups (AUD-P3-10, AUD-P3-1)
- Tests first: (a) new TEST_BUILD seam `g_shard_test_delete_render_fail` (one-shot atomic,
  declared in `shard_test_ctl.h`, defined in `storage.c` under `#ifdef TEST_BUILD` next to
  `g_shard_test_json_escape_fail` because `test_control.c` is not linked into
  `shard-db-test`; armed from a wire test by a new `TEST_HOOK_SET_GLOBALS_*` phase in
  `test_control_thread_main` plus a `test_env_ctl_set_delete_render_fail` helper in
  `src/test/fixtures.c/.h`, modeled on `test_env_ctl_set_json_escape_fail`): with `return_record:true` the delete returns an error
  and the record still exists; (b) guarded write with an invalid auto-key yields exactly
  one JSON reply line; an invalid guard JSON still yields `invalid guard criteria` `[R]`.
- Code, delete: `v2_delete_check_fn` renders the record, after the CAS check passes (so a render failure never masks a legitimate `condition_not_met`), with `single_write_decode` into a
  new `char *ret_json` when `return_record` is set (replacing `ret_buf`/`ret_len`/
  `ret_hdr2`), and on render failure (or the seam) sets `err_buf="cannot render record"` and
  rejects; the post-commit code prints `ret_json`. The error branch prints through
  `single_write_error(ctx.err_buf,"delete failed")`. Free `ret_json` on every exit path that
  freed `ret_buf`.
- Code, guard (`server.c` guard branch): introduce `int ak_err = 0;` set to 1 where
  `auto_key_normalize` fails (the `key_valid = 0; grc = -2;` line); put `if (ak_err) { /* already replied by auto_key_normalize */ } else if (grc == -2) ...` at
  the head of the reply chain (`reject` stays 1), so the `!gpass` branch cannot print a
  second reply; keep the rest of the reject tail (reservation end, unlock, frees) unchanged.
- Delete's missing-key status `not_found` (success exit) stays; documented.

### T5 Docs
- `docs/query-protocol/cas.md` lines ~204 and ~209: dry run applies to single insert, update
  and delete and to bulk; response table `would_insert|would_update|would_delete`; remove
  "single-record CAS writes always execute". `docs/query-protocol/overview.md` and the
  insert/update reference: `dry_run`, unknown-field and empty-key errors.
  `docs/query-protocol/schema-mutations.md` (~348): line 348 ("writes silently ignore them") now reads: single insert/update reject a removed
  field's name as unknown; bulk-insert/bulk-update still ignore it.
  `docs/reference/error-codes.md`: new errors (`key must not be empty`, `key exceeds max_key
  (N)`, `unknown field '<name>'`, `cannot render record`). `docs/concepts/typed-records.md`.
  Changelog: breaking entry (unknown field, empty key) and the `dry_run`-on-insert semantic
  change. Mark AUD-P3-1/2/3/7/9/10 fixed in `2026-11-25-audit-single-writes.md` and the audit
  index. Check `AGENTS.md` for dry-run scope mentions.

### Definition of done
Full suite fresh (`--jobs 8`), ASan+UBSan and TSan (three runs each) per above, no new warnings, only the
files named here touched (plus the new test files and docs), existing tests unchanged except the two removed-field tests rewritten in T2,
docs synced, uncommitted for review.

## Execution notes (2026-11-28)

- T1-T5 executed on `fix/single-write-audit-slice1`, uncommitted. T3 diagnostic confirmed:
  `upsert_adapter_value_compute` adds no check beyond what `single_write_finish` already applies.
- New tests: `test-single-write-key-validation`, `-unknown-field`, `-dry-run`, `-delete-guard`
  (registered in `build.sh`). Rewritten per plan: `test_remove_field.c`, `test_not_null_updates_packed.c`.
- Red proofs: T1/T2/T3 tests fail on the base source (stashed `src/db`); the guard fix was disabled
  (`0 && ak_err`) -> 2 assertions fail; the render rejection was disabled -> 5 assertions fail. Both restored.
- Full suite `--jobs 8`: 733 cases, 0 failed (729 on base + 4 new cases).
- ASan+UBSan x3 (`--jobs 6`): 733 cases, 0 failed each. TSan x3 (`--jobs 3`): 733 cases, 0 failed each.
- No deviations from the plan; no `PLAN_NOTES.md` needed.
