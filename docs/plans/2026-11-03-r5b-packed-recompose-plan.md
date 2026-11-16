# R5b — packed-layout `add-field`, `edit-field`, `vacuum compact` (2026-11-03)

> **For the executing agent.** Read `AGENTS.md` and `CORE-PROCESS.md` first. This plan is carried out
> literally, task by task. Plan only — nothing here is implemented yet.
>
> **Execution rules (embedded).**
> 1. Base: R5 is committed locally as `b1cc3b7` on `fix/type-correctness-recovery` and **not pushed**, so `main` does not contain it.
>    Create `feat/r5b-packed-recompose` **from `fix/type-correctness-recovery`** (not from `main`). This is a deliberate deviation
>    from "branch off the default branch"; R5b cannot exist without R5. Do not commit, push, merge or open a PR — leave everything
>    uncommitted for the raw-diff review (`AGENTS.md` standing exception). The uncommitted edit already present in
>    `docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md` is not yours; do not touch or stage it.
> 2. Do tasks in order. Every task starts with a failing test (RED) whose output you paste into
>    `docs/plans/2026-11-03-r5b-evidence.md`. Never claim a step passed without pasting real command output.
> 3. If a quoted anchor is not found **exactly once**, write the mismatch description to `PLAN_NOTES-r5b.md` (a gitignored `PLAN_NOTES.md` from earlier work already exists — do not overwrite it) and **halt the whole run
>    immediately** — do not guess, reinterpret, or continue to any other task. Only the human (or the planning model) resumes.
> 4. If you hit a decision the plan does not cover, stop and ask — do not improvise.
> 5. Never weaken a test (loosen an assertion, delete a case, skip/xfail) to make a failure disappear. A test that fails then
>    passes on rerun is a bug until root-caused.
> 6. Build: `SKIP_TESTS=1 ./build.sh`. Single case: `./build/bin/shard-db-test run <name>`. Normal full suite, **only**:
>    `./build/bin/shard-db-test run-all --jobs 4`. `/tmp` is tmpfs (~15 G): after **every** run clear scratch with
>    `find /tmp -maxdepth 1 -name 'shard-db-*' -exec rm -rf {} +` (plain `rm -rf /tmp/shard-db-*` aborts under zsh when the glob is empty).
>    Never `pkill -f <pattern>` (it can match your own shell); if a stray daemon remains, find it with `pgrep -a shard-db` and `kill <pid>`.
> 7. **No sanitizers, no benches** (R6 gate). Note for R6: the diff touches rebuild/recovery lifetimes (owned enum lists, `TypedPrepared`
>    owners) — the ASan/TSan gate must cover the new `test-packed-*` and `test-rebuild-txn-packed-*` cases.

---

## 0. Scope, decisions, assumptions

### 0.1 What changes for users

On a packed object (every migrated or newly created object):

| Operation | R5 | R5b |
|---|---|---|
| `add-field` | refused | works: appended fields; computed defaults backfilled per row inside the transactional rebuild |
| `edit-field` | refused | works: varchar bound / bare↔bounded, numeric precision/scale/bare↔bounded, integer-family widen/narrow, `float→double`, enum append/rename/1B→2B widen, default/`:not_null` metadata edits |
| `vacuum compact` (also with `splits`) | refused | works: drops tombstoned fields from the `PkrSchema`, reclaims deleted-record space, one transactional rebuild |
| `vacuum splits/streams`, `remove-field`, `rename-field`, indexes, backup/restore | works | unchanged |

No on-disk format, wire request shape or version changes: **no version bump**, no migration. The only wire-visible changes are that three
former error responses become the same success responses legacy objects return, plus new refusal reasons listed in §6.

### 0.2 Confirmed decisions (from the human — not reopened)

| # | Decision | Where it lands |
|---|---|---|
| D-1 | Numeric rescale/precision edits: scale reduction rounds half away from zero via the existing `pkr_numeric_encode` rules (`dec_apply_precision`, ties away from zero); refuse only when the integer part no longer fits the new precision; all-or-nothing. | `packed_convert_cell` (§3.2); tests `test-packed-edit-field-numeric` |
| D-2 | Computed-default backfill (`seq`, `uuid()`, `random(N)`) on add-field: per row inside the transactional rebuild; `seq` follows shard-scan order; document it. | `packed_fill_added`, docs (§6) |
| D-3 | Bare ↔ bounded `varchar`/`numeric`: every direction allowed; every existing row validated in a pre-flight; the whole edit fails with no change if any row does not fit. | `packed_preflight_walk`; tests |
| D-4 | `validate_field_type`, `typed_legacy_decl_size`, `parse_field_line` use the packed grammar for packed objects. | §3.1 (callers branch; see note) |
| D-5 | `vacuum compact` on packed drops tombstoned fields from the `PkrSchema`, reclaims deleted-record space, one transactional rebuild (matches legacy). Includes compact+splits. | `rebuild_object_packed` (§3.4) |

*Note on D-4.* `typed_legacy_decl_size` itself is unchanged: for fixed-width types its grammar is identical to the packed one (its own comment
and `validate_packed_field_type` say so) and for `varchar`/`numeric`/`currency` it is the **legacy** grammar by definition. "Use the packed
grammar for packed objects" is implemented by making every packed code path stop calling it for those three tokens: `cmd_add_fields`,
`cmd_edit_fields` (through `parse_field_line_packed`) and `rebuild_object` route packed objects through `validate_packed_field_type`
(already used by `create-object`; made non-static) and `parse_field_line_packed` (new). `validate_field_type` is only called from
`cmd_create_object`'s legacy branch and is untouched.

### 0.3 Assumptions I made (not in the confirmed list) — flagged to the human, each is one line to change

| # | Assumption | Why |
|---|---|---|
| A-1 | **(human-decided 2026-11-04, SQL semantics)** Rows that exist before `add-field` get, for the new field: the field's `:default=<literal>` / computed default if declared; otherwise **NULL** if the field is nullable; if the field is `:not_null` with no usable default the request is **refused** (`Field 'x' is NOT NULL and has no default; existing rows cannot be backfilled`) as soon as the walk meets the first existing row, so an empty object accepts it. Whole edit rolls back. | Matches PostgreSQL/MySQL/SQLite. Supersedes the earlier zero-fill (which is legacy behaviour and is kept for legacy objects only). `:auto_create`/`:auto_update` count as "no usable default" (A-2). |
| A-2 | `:auto_create` / `:auto_update` stay **inert** during backfill (no usable default: NULL, or refusal if `:not_null`; see A-1). | Parity with legacy and `docs/query-protocol/schema-mutations.md`. |
| A-3 | Packed backfill is **stricter than legacy** for impossible defaults: a default that cannot be stored in the field (e.g. `random(4)` into an `int`, `uuid()` into `varchar:20`, `random(N)` with N∉[1,256], a `seq` value that overflows `short`) refuses the request. Legacy wrote garbage/zero for some of these. | The packed scalar grammars (R4) have no "best effort" encode. Pre-flight probe before any data moves; `seq` overflow is detected after the range reservation but before the first row is written. |
| A-4 | The fields.conf compaction filter in `rebuild_object_v2` changes from `strstr(line, ":removed")` to an exact-suffix test shared by both layouts. | The loader (`load_typed_schema`) treats only a line **ending** in `:removed` as a tombstone; a live line whose default literal contains `:removed` mid-line is currently dropped by compaction (latent legacy data-loss bug). Needed for packed correctness; regression test included (Task 4). |
| A-5 | `vacuum compact` that would leave **zero** fields is refused (`typed_schema_build_packed` requires ≥ 1 field). | Cannot express an empty `PkrSchema`. |

### 0.4 Corrections to the brief (found while reading the tree)

1. **The committed R5 docs do not contain the refusal text.** `docs/query-protocol/schema-mutations.md`, `docs/reference/error-codes.md`,
   `docs/release-notes/2026.11.1.md` and `docs/reference/changelog.md` at `b1cc3b7` say nothing about "not supported on packed-layout".
   The only sources of that wording are the three C error strings, the test `test-packed-recompose-ops-refused`, and the **unexecuted Task 12
   docs hunks** inside `docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md` (they would add the refusal text). §6 therefore
   *adds* the R5b documentation, and **R6 must drop every "not supported on packed" hunk from the R5 plan's Task 12** (listed in §6.4).
2. **The "(D9) bulk-update recompose refused" classifications in `r5-flip-triage.md` are not D9.** D9 covers only add-field, edit-field and
   vacuum compact. Bulk-update is implemented for packed (`docs/concepts/packed-bulk-update.md`). Those cases are *not* R5b candidates (§7).
3. **`count_nulls_for_field` / `not_null_scan_cb` and `edit_preflight_walk_cb` silently misbehave on packed records.** Both call
   `typed_record_view`, which rejects any `hdr2` other than `0|SREC_FLAG_NULLMAP` (`if (hdr2 & ~SREC_FLAG_NULLMAP) return -1`). On a packed
   record the not-null gate would report `broken` (edit refused with a misleading "record scan failed"), and `edit_preflight_walk_cb`
   would `return 0` ("corrupt record — not this edit's problem") and **skip every row**, i.e. a pre-flight that validates nothing.
   Today the D9 refusal hides both. §3.5/§3.6 replace them for packed objects.

---

### 0.5 Known limits left deliberately out of scope (flag to human)

* **A-4 is only partly fixed.** `strstr(..., ":removed")` also remains in `cmd_remove_fields`, `cmd_add_fields` and `cmd_edit_fields` (a field whose default literal contains `:removed` can be created but not added/edited/removed). Only the compaction filter changes, because it loses data; the rest are refusals.
* **Durability (D-6, human-decided 2026-11-04): power-loss-safe.** Every fields.conf swap on the recompose and edit paths uses `durability_publish_replace`, and `rebuild_txn_commit` fsyncs the parent directory after its rename. Hunks in §3.8 (H15, H17; H16 is folded into H13). The swaps are shared with legacy objects, which therefore get the same hardening (no behaviour change besides a stricter fsync).
* **Insert vs backfill:** with A-1 the two now agree: old rows and later inserts that omit a nullable, default-less field both read NULL. Docs say so, and say that legacy objects still zero-fill.
* `slotcask_walk_live` is sequential (`V2RebuildCtx.why`/`live_count` rely on it) although `slotcask.h` still describes it as parallel; add a one-line comment at the walk call.
* Error echoes of raw user text (`Invalid field line: %s`, `Field [%.*s] already exists`) inherit the existing flaw (field names may contain a quote or backslash); not widened here.
* D-4 is implemented as packed *twins* selected by callers (see the 0.2 note) — **human to confirm this reading.**

## 1. Findings (root-cause level)

**F1 — the three refusal sites.** Exact error strings, found by grep over `src/` and `src/test/`:

| Site | Anchor text | Also asserted by |
|---|---|---|
| `src/db/config.c`, `cmd_add_fields` | `add-field is not supported on packed-layout objects yet` | `test_layout_manifest.c` (`test_packed_recompose_ops_refused`, match substring `not supported on packed`) |
| `src/db/query_schema.c`, `cmd_edit_fields` | `edit-field is not supported on packed-layout objects yet` | same test |
| `src/db/query_find.c`, `rebuild_object_v2` (guard at the top) | `vacuum compact / add-field / edit-field are not supported on packed-layout objects yet` | same test (compact) |
| `src/db/query_find.c`, `rebuild_object` (compact/add path) | same string, second occurrence | same test |

**F2 — why a packed record cannot be recomposed with the legacy walk.** `v2_rebuild_walk_cb` (slot_changed branch) copies bytes between
fixed `offset`s of a flat region. A packed record is `[bitmap prefix][directory of per-field descriptors][variable payloads]`; the
**directory** is the fixed-size per-field entry table (`PkrSchema.directory`, 8 bytes per varchar/numeric field, `width` bytes per fixed
field) — so *any* change to the field list or a field's width/kind changes the directory layout of **every** record, and the bitmap
prefix (`1 + ceil(nfields/8)`) changes size when `nfields` crosses a multiple of 8. Packed records are therefore never patched; they are
decoded into `PkrCell[]`, optionally converted, and re-composed with `pkr_compose` (the shape `layout_convert_record` already uses for
the legacy→packed migration).

**F3 — `slot_size` is derived, not stored.** `load_schema` computes `slot_size = (24 + max_key + max_value + 7) & ~7` (floor 32) from
`TypedSchema.total_size`, which for packed is `PkrSchema.maximum`; `pkr_slot_bound(&packed, max_key)` is the same number. `schema.conf` stores no
field-derived value, so add/edit/compact **never rewrite `schema.conf`** (only splits/streams do, and `update_schema_conf_splits_streams` preserves the
`layout=packed` token). Consequence for the transaction: the `RebuildTxn` already snapshots `fields.conf` (`fields.conf.rollback`), the
data dir (`<obj>/.rebuild_txn.active/data`), splits/streams and the layout token; that is sufficient for all three operations.

**F4 — enum / varchar bound changes need a rebuild even when record bytes are unchanged.** Segment records are padded to `slot_size`
and the sequential reindex scan reads fixed strides, so any change to `PkrSchema.maximum` (e.g. `varchar:40→80`) requires a rebuild even though the
payload bytes would still validate. The metadata-only (no rebuild) path is therefore taken **iff `packed_schema_same(old,new)`** (no change to
kind/width/payload_max/precision/scale/bare/currency/offset): default, `:not_null`, enum append within the same width, enum rename.

**F5 — `rebuild_object_v2` builds `fields.conf` from the file, but the new `PkrSchema` must be built from the same lines.** To make
"the schema the walk composes against" and "the schema the loader will read" provably identical, the packed path builds `new_ts` with
`typed_schema_build_packed` over the **exact line list that becomes `fields.conf`**, and `rebuild_object_v2` re-loads the schema after the
`fields.conf` swap and refuses (→ transactional rollback) if `packed_schema_same` or the field identities disagree. This is the same
"the loader disagrees" check `layout_migrate_object` uses.

**F6 — `edit-field` has two writers of the merged line.** `rewrite_fields_conf_for_edit` merges an edit into an old line (carry default,
resolve `:not_null`). The packed path needs the merged line *in memory* to build `new_ts`. The merge logic is extracted verbatim into
`edit_merge_line`; `rewrite_fields_conf_for_edit` calls it. One implementation, two callers.

---

## 2. Invariants and edge cases (the executor must preserve all of these)

### 2.1 Rounding vs overflow vs record fit (numeric edits)
* The source value is `dec_unpack(payload)` → `dec_to_text` (exact, at display scale) → `typed_prepared_set` on the **new** field, i.e. `dec_parse` →
  (`!bare`) `dec_apply_precision(P,S)` → payload cap check → `dec_pack`. `dec_apply_precision` rounds **ties away from zero** and then enforces
  "digits left of the point ≤ P−S" **after** rounding. So `99999999.9999 → numeric:10,2` is refused (rounds to `100000000.00`, 9 integer digits > 8),
  while `1.0050 → numeric:12,2` is `1.01` and `-1.0050 → -1.01`.
* bare → bounded: every value is re-validated; bounded → bare: always fits. `currency` ≡ `numeric:19,4`.
* The first non-fitting row aborts: pre-flight (`edit-field`) reports `Pre-flight failed on field [<name>]: <reason>` with **no** change; `dry_run:true`
  reports the same refusal.
* If rounding changes a value, that is *success*, not an error (D-1). It is documented, not warned.

### 2.2 Atomic failure
* `edit-field`: pre-flight walk (read-only, same converter) → rebuild. Any failure leaves `fields.conf`, data, indexes, counts, `schema.conf`
  byte-identical (rollback by `rebuild_txn_abort`).
* `add-field`: no separate pre-flight walk (nothing can fail per row except computed-default encoding and segment fit). Defaults are **probed
  before the transaction** (§3.4 `packed_probe_added`); `seq` range is probed **after reservation, before the first row**. A mid-walk failure
  (segment limit, OOM) aborts the transaction → "original data restored". **Documented non-rollback:** a `seq` range already reserved from
  `metadata/sequences/<name>` is not returned (gap in the sequence; same as legacy).
* `vacuum compact`: pure projection + removal; cannot fail per row except OOM / segment fit (rows only shrink or keep size, so effectively OOM).

### 2.3 Crash states (reuse of the R5 whole-object crash-state approach)
`RebuildTxn` layout: `.rebuild_txn.preparing` → `.rebuild_txn.active` (meta + `fields.conf.rollback`) → data renamed to `<obj>/.rebuild_txn.active/data`
→ new data built → fields.conf swapped → [edit only: index rebuild] → `active`→`done` (commit) → cleanup. Pause phases exist in the daemon
(`REBUILD_TEST_PAUSE_PHASE`): `after-stage`, `after-walk`, `after-metadata`, `after-commit`.

| State (kill point) | On-disk state | Startup recovery outcome | R5b-specific hazard / why safe |
|---|---|---|---|
| S0 before `rebuild_txn_begin` | untouched | nothing | probes ran, nothing written |
| S1 `after-stage` | data moved to `<obj>/.rebuild_txn.active/data`; new `data/` empty or partial | `active` found → `rebuild_txn_abort` restores data, `fields.conf`, splits/streams/layout token | packed record bytes are only meaningful with the matching `fields.conf`; both are restored together |
| S2 `after-walk` | complete new `data/`; **old** `fields.conf` | abort (same) | new-schema records exist beside the old `fields.conf` for the window; they are discarded wholesale, never read |
| S3 `after-metadata` | new data + **new** `fields.conf`; txn still `active` | abort restores old `fields.conf` from `fields.conf.rollback` and old data | the dangerous window (new bytes + new schema, uncommitted) is rolled back as a unit |
| S4 indexes rebuilt (edit only, between metadata and commit) | as S3 plus new index files | abort + `reindex_object_checked` because `indexes_may_change` was recorded in `meta` | rebuilt from restored data; add-field/compact record `indexes_may_change=0` and never touch index files (values unchanged, no field index stored in idx) |
| S5 `after-commit` | `active` renamed to `done`; new data + new `fields.conf` + (edit) new indexes | `done` found → cleanup only; **new state is authoritative** | complete, consistent |
| S6 mid-cleanup | partial `.rebuild_txn.done` | cleanup resumes | never restores from `done` (existing test `test-rebuild-txn-done-cleanup-never-restores`) |

New tests assert S1, S2, S3 (rollback: original `fields.conf` byte-equal, original rows readable, index consistent, no txn dirs) and S5
(new state complete) for each of add / edit(numeric+index) / compact (§5, Task 5). S4 has no pause phase (the hook only pauses at `after-stage`, `after-walk`,
`after-metadata`, `after-commit`); it is covered by the abort path's `reindex_object_checked` (already exercised for legacy by
`test-rebuild-txn-edit-field-after-metadata`) and, for packed, by the edit case's post-recovery index query in `after-metadata`.

### 2.4 Index rebuilds
* `edit-field`: `edit_field_dirty()` (packed) = any `PkrField` member except `offset`/`currency` differs, or the logical `type` differs.
  Dirty field names feed the existing `selective_reindex_dirty` → `cmd_add_indexes(force=1)`, which runs after the `fields.conf` swap and cache
  invalidation, i.e. against the new packed schema. Conservative on purpose: any bound/precision/scale/bare change rebuilds the indexes that
  reference the field (composite varchar/numeric index parts depend on declared widths).
* Enum rename / append within the same width: not dirty, no rebuild, no reindex (parity with legacy).
* add-field: new field is unindexed; existing indexes untouched. compact: index trees are keyed by `(value, hash16)` with no field position;
  untouched (documented behaviour "Indexes preserved"). compact + `splits`: existing splits path (`reindex_object_checked`) unchanged.

### 2.5 Tombstoned fields
* Tombstoned fields keep a slot in the `PkrSchema` and a NULL cell in every record. `add-field`/`edit-field` preserve that (cell stays NULL, `removed`
  flag preserved). `compact` drops the line **and** the slot; every record is re-composed without it. A name freed by compact can be re-added.
* `edit-field` still refuses a tombstoned target ("is tombstoned; cannot edit").
* The `strstr(":removed")` → exact-suffix fix (A-4) applies to the shared `fields.conf` rewrite.

### 2.6 `fields.conf` and `schema.conf`
* `fields.conf`: add appends the request lines **verbatim**; compact drops tombstoned lines, keeps comments/blank lines; edit replaces the matched
  line with the canonical merged line (`edit_merge_line`). The packed `new_ts` is built from the identical list; post-swap reload must match.
* `schema.conf`: **not rewritten by add/edit/compact** (F3). The `layout=packed` token is therefore preserved; for compact+splits/streams
  `update_schema_conf_splits_streams` already carries unknown tail tokens through. Tests assert the token survives each operation.

### 2.7 Null bitmap size changes
`PkrSchema.prefix = 1 + ceil(n/8)`. Records are never byte-copied; the bitmap is regenerated by `pkr_compose` from cell NULL flags, so
8→9 fields (prefix 2→3) and 9→8 on compact are handled by construction. Tests cross both boundaries. Existing explicit NULLs must survive
add/edit/compact; tombstoned cells are NULL.

### 2.8 Record and slot limits
* New `PkrSchema.maximum` > `INT32_MAX` → `typed_schema_build_packed` fails (`packed schema exceeds bounds`) → request refused, no change.
* `pkr_slot_bound(new, max_key)` > `INT_MAX` → refused (`packed schema exceeds the maximum slot size`), same message as create/migration.
* Per row: `pkr_record_fits(new, actual, klen, SLOTCASK_SEG_MAX_BYTES)` after composition. A row that no longer fits (new descriptors push a
  ~128 MiB row over the segment limit) → edit pre-flight refusal / add rebuild abort. **Not unit-testable without a segment-cap seam**
  (`SLOTCASK_SEG_MAX_BYTES` is a macro constant): covered by code review; a follow-up could add a TEST_BUILD cap override. Say so in the evidence file.
* Enum edits: `1B→2B` widen only (>256 values); `2B→1B` and removal/reorder stay refused by the existing diff validator (shared code).
  `packed_fixed_width` requires `enum_width == (n ≤ 256 ? 1 : 2)`, which `typed_schema_build_packed` re-checks.
* `MAX_FIELDS=256` includes tombstoned fields; compact is the way back under the cap. Test: 255 fields + 1 ok, + 1 more refused.

### 2.9 Concurrency
All three modes already run under `objlock_wrlock` (docs: concurrency.md). The converter is called from the single-threaded
`slotcask_walk_live`; `seq` uses the existing atomic counter. No new shared state, no new locks. `TypedPrepared` (≈ 8 KB) lives on the walk
callback's stack and is freed on every path.

---

## 3. Code changes

> Style rule: new functions are complete blocks. Edits to large existing functions are anchored hunks (quoted old text → complete new text).
> Each anchor must occur **exactly once** in the named file; otherwise halt (rule 3).

### 3.0 Call-site inventory (grep results at `b1cc3b7`)

| Symbol | Change | Definition | Callers (all) |
|---|---|---|---|
| `rebuild_object_v2` | **signature unchanged** | `query_find.c` | `query_find.c` `rebuild_object` ×2 (verbatim packed copy; legacy recompose) + **new** `rebuild_object_packed`; `query_schema.c` `cmd_edit_fields` ×1 |
| `rebuild_object` | signature unchanged, packed branch rewritten | `query_find.c` | `config.c` `cmd_add_fields` (×1); `query_maint.c` `cmd_vacuum` (×1) |
| `parse_field_line` | unchanged (legacy only) | `query_find.c` | `query_schema.c` `cmd_edit_fields` (×1, now guarded); `query_find.c` legacy `rebuild_object` (×1) |
| `parse_field_line_packed` | **new** | `query_find.c` | `query_schema.c` `cmd_edit_fields`; `config.c` `cmd_add_fields`; `query_find.c` none |
| `validate_packed_field_type` | `static` → extern | `query_schema.c` | `query_schema.c` `cmd_create_object` (×1, existing); `query_find.c` `parse_field_line_packed` (new) |
| `validate_field_type` | unchanged | `query_schema.c` | `query_schema.c` `cmd_create_object` legacy branch (×1) |
| `typed_legacy_decl_size` | unchanged | `schema_wire.c` | `config.c` `cmd_add_fields` (legacy branch only after this plan), `query_find.c` `parse_field_line`, `query_schema.c` `validate_field_type`/`validate_packed_field_type` (fixed-width branch) |
| `packed_schema_same` | `static` → extern (declared in `types.h`) | `query_find.c` | `query_find.c` `layout_migrate_object` (×1) + new uses |
| `NotNullScanCtx` | gains `ts` | `query_schema.c` | `count_nulls_for_field` only |
| `rewrite_fields_conf_for_edit` | internals refactored, signature unchanged | `query_schema.c` | `edit_finalize_metadata`, `cmd_edit_fields` no-rebuild path |
| `V2RebuildCtx` | gains `why[192]` | `query_find.c` | `rebuild_object_v2`, `v2_rebuild_walk_cb`, new packed walk |

Verification command for the executor (paste output): `grep -n "rebuild_object_v2(\|rebuild_object(\|parse_field_line(\|packed_schema_same\|validate_packed_field_type\|rewrite_fields_conf_for_edit(" src/db/*.c src/db/*.h`.
Any caller not in the table → halt and report.

### 3.1 Shared declarations — `src/db/types.h`

Anchor (once): `void typed_schema_release(TypedSchema *ts);` → replace with:

```c
void typed_schema_release(TypedSchema *ts);
/* R5b packed recompose helpers (query_find.c / query_schema.c). */
int  packed_schema_same(const PkrSchema *a, const PkrSchema *b);
/* Packed-grammar twin of parse_field_line: "name:type[:param][:default=..]".
   Returns 1 on a valid declaration (out->type/size/name/defaults/not_null/enum
   filled; pf_out, when non-NULL, receives the PkrField), 0 otherwise. Caller
   frees enum lists with free_enum_values(out). */
int  parse_field_line_packed(const char *line, TypedField *out, PkrField *pf_out);
/* >0 when "name:type..." is valid in the packed grammar, 0 otherwise. */
int  validate_packed_field_type(const char *field_spec);
/* Reads fields.conf declaration lines exactly as load_typed_schema counts
   them (blank, '#' and colon-less lines skipped). 0 ok; caller releases with
   packed_free_decl_lines. */
int  packed_read_decl_lines(const char *obj_dir, char ***lines_out, size_t *n_out);
void packed_free_decl_lines(char **lines, size_t n);
```

### 3.2 The row converter and walk — `src/db/query_find.c`

**3.2a** Make `packed_schema_same` external. Anchor (once): `static int packed_schema_same(const PkrSchema *a, const PkrSchema *b) {` →
`int packed_schema_same(const PkrSchema *a, const PkrSchema *b) {`.

**3.2b** Add `why` to the rebuild context. Anchor (once):
```c
    const char        *db_root;       /* for seq_next_val_batch (DK_SEQ pre-reserve) */
    const char        *object;
} V2RebuildCtx;
```
→
```c
    const char        *db_root;       /* for seq_next_val_batch (DK_SEQ pre-reserve) */
    const char        *object;
    char               why[192];      /* packed recompose: first failure, JSON-safe */
} V2RebuildCtx;
```

**3.2c** Insert the packed recompose block **immediately before** the definition `int v2_rebuild_walk_cb(const uint8_t hash16[16],` (anchor: the two lines
`int v2_rebuild_walk_cb(const uint8_t hash16[16],` / `                                const void *key, size_t klen,` — occurs once in `query_find.c`; the prototype in `query_internal.h` is a different file).
Insert this complete block (it uses `BackfillSpec`/`V2RebuildCtx` defined above, and forward-declares `layout_diag_key`, defined later in this file):

```c
/* ===================== Packed recompose (R5b) ===================== */

static void layout_diag_key(char *out, size_t cap, const void *key, size_t klen);

/* Replace every byte that could break the {"error":"..."} envelope. */
static void packed_json_safe(char *s) {
    for (; *s; s++)
        if (*s == '"' || *s == '\\' || (unsigned char)*s < 0x20 || (unsigned char)*s > 0x7e) *s = '?';
}

typedef struct {
    const TypedSchema *old_ts;
    const TypedSchema *new_ts;
    const int         *new_to_old;   /* new index -> old index; -1 = added field */
    BackfillSpec      *backfill;     /* per NEW field; NULL when nothing is added */
} PackedRowCtx;

static int64_t packed_be_signed(const uint8_t *p, uint32_t w) {
    uint64_t u = 0;
    for (uint32_t i = 0; i < w; i++) u = (u << 8) | p[i];
    unsigned sh = 64u - 8u * w;
    return (int64_t)(u << sh) >> sh;   /* arithmetic shift, same as decode_be_signed */
}

/* Exact decimal text of a numeric payload. 0 ok (*text malloc'd), -1 error. */
static int packed_numeric_text(const uint8_t *payload, size_t n, char **text, size_t *tlen) {
    dec_t d;
    dec_init(&d);
    if (dec_unpack(payload, n, &d) != DEC_OK) { dec_free(&d); return -1; }
    size_t need = dec_text_len(&d), used = 0;
    char *buf = malloc(need ? need : 1);
    if (!buf || dec_to_text(&d, buf, need, &used) != DEC_OK) { free(buf); dec_free(&d); return -1; }
    dec_free(&d);
    *text = buf;
    *tlen = strlen(buf);
    return 0;
}

/* Convert one present OLD cell to the NEW field's declaration.
   0 ok, 1 the value does not fit (why), -1 hard error (why).
   Borrowed cells point into the OLD record, which outlives the TypedPrepared
   (the row is composed before the caller returns). Owned buffers are handed to
   p->owned[k] and released by typed_prepared_free. */
static int packed_convert_cell(const TypedField *of, const PkrField *opf,
                               const TypedField *nf, const PkrField *npf,
                               const TypedRowSpan *sp, TypedPrepared *p, int k,
                               char *why, size_t whysz) {
    if (opf->kind != npf->kind) {
        snprintf(why, whysz, "unsupported transition between field kinds");
        return -1;
    }
    if (npf->kind == PKR_VARCHAR) {
        if (sp->len > npf->payload_max) {
            snprintf(why, whysz, "varchar: value of length %zu exceeds new cap %u",
                     sp->len, npf->payload_max);
            return 1;
        }
        p->cells[k] = (PkrCell){sp->data, sp->len, 0};
        return 0;
    }
    if (npf->kind == PKR_NUMERIC) {
        if (opf->bare == npf->bare && opf->precision == npf->precision &&
            opf->scale == npf->scale) {
            p->cells[k] = (PkrCell){sp->data, sp->len, 0};   /* currency<->numeric(19,4): same bytes */
            return 0;
        }
        char *text = NULL;
        size_t tlen = 0;
        if (packed_numeric_text(sp->data, sp->len, &text, &tlen) != 0) {
            snprintf(why, whysz, "malformed numeric payload");
            return -1;
        }
        TypedInput in = {text, tlen, 1, 0, 0};
        char err[96] = {0};
        int rc = typed_prepared_set(p, (size_t)k, &in, err, sizeof(err));
        int en = errno;
        if (rc) {
            if (npf->bare)
                snprintf(why, whysz, "numeric: value %.60s is not representable: %s", text, err);
            else
                snprintf(why, whysz, "numeric: value %.60s does not fit numeric(%d,%d): %s",
                         text, npf->precision, npf->scale, err);
        }
        free(text);
        return rc ? (en == ENOMEM ? -1 : 1) : 0;
    }
    /* PKR_FIXED */
    if (opf->width == npf->width) {   /* same width ⇒ same encoding (cross-type edits were refused earlier) */
        p->cells[k] = (PkrCell){sp->data, sp->len, 0};
        return 0;
    }
    int oint = of->type == FT_SHORT || of->type == FT_INT || of->type == FT_LONG;
    int nint = nf->type == FT_SHORT || nf->type == FT_INT || nf->type == FT_LONG;
    uint8_t *b = NULL;
    if (oint && nint) {
        int64_t v = packed_be_signed(sp->data, opf->width);
        if (npf->width < opf->width) {
            int64_t lo = -(INT64_C(1) << (npf->width * 8 - 1));
            int64_t hi = (INT64_C(1) << (npf->width * 8 - 1)) - 1;
            if (v < lo || v > hi) {
                snprintf(why, whysz, "integer narrow: value %lld out of range [%lld, %lld]",
                         (long long)v, (long long)lo, (long long)hi);
                return 1;
            }
        }
        b = malloc(npf->width);
        if (!b) { snprintf(why, whysz, "out of memory"); return -1; }
        for (uint32_t i = npf->width; i-- > 0; ) { b[i] = (uint8_t)(v & 0xFF); v >>= 8; }
    } else if (of->type == FT_FLOAT && nf->type == FT_DOUBLE &&
               opf->width == 4 && npf->width == 8) {
        float fv;
        double dv;
        memcpy(&fv, sp->data, 4);       /* stored native-order by encode_field */
        dv = (double)fv;
        b = malloc(8);
        if (!b) { snprintf(why, whysz, "out of memory"); return -1; }
        memcpy(b, &dv, 8);
    } else if (of->type == FT_ENUM && nf->type == FT_ENUM &&
               opf->width == 1 && npf->width == 2) {
        b = malloc(2);
        if (!b) { snprintf(why, whysz, "out of memory"); return -1; }
        b[0] = 0;
        b[1] = sp->data[0];
    } else {
        snprintf(why, whysz, "unsupported fixed-width transition (%u -> %u bytes)",
                 opf->width, npf->width);
        return -1;
    }
    free(p->owned[k]);
    p->owned[k] = b;
    p->cells[k] = (PkrCell){b, npf->width, 0};
    return 0;
}

/* Encode `text` as field k's value (strict packed grammar). 0 ok, -1 + err. */
static int packed_set_text(TypedPrepared *p, int k, const char *text, char *err, size_t errsz) {
    TypedInput in = {text, strlen(text), 1, 0, 0};
    return typed_prepared_set(p, (size_t)k, &in, err, errsz) == 0 ? 0 : -1;
}

/* Value for a field that did not exist in OLD. 0 ok, 1 unstorable, -1 hard. */
static int packed_fill_added(const PackedRowCtx *ctx, TypedPrepared *p, int k,
                             char *why, size_t whysz) {
    const TypedField *nf = &ctx->new_ts->fields[k];
    BackfillSpec *bf = ctx->backfill ? &ctx->backfill[k] : NULL;
    char text[600];
    const char *src = NULL;
    switch (bf ? bf->kind : DK_NONE) {
    case DK_LITERAL:
        src = nf->default_val;
        break;
    case DK_SEQ: {
        int64_t v = atomic_fetch_add_explicit(&bf->seq_next, 1, memory_order_relaxed);
        snprintf(text, sizeof(text), "%lld", (long long)v);
        src = text;
        break;
    }
    case DK_UUID: {
        uint8_t raw[16];
        if (gen_uuid4_raw(raw) != 0) { snprintf(why, whysz, "uuid generator failed"); return -1; }
        snprintf(text, sizeof(text),
                 "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                 raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                 raw[8], raw[9], raw[10], raw[11], raw[12], raw[13], raw[14], raw[15]);
        src = text;
        break;
    }
    case DK_RANDOM: {
        int n = bf->random_bytes;
        uint8_t raw[256];
        if (n <= 0 || n > 256 || fill_random(raw, (size_t)n) != 0) {
            snprintf(why, whysz, "random generator failed");
            return -1;
        }
        for (int i = 0; i < n; i++) snprintf(text + i * 2, 3, "%02x", raw[i]);
        src = text;
        break;
    }
    default:   /* DK_NONE, DK_AUTO_CREATE, DK_AUTO_UPDATE: inert during backfill (A-2) */
        break;
    }
    if (!src) {
        /* No usable default (A-1): NULL for a nullable field, refusal for NOT NULL. */
        if (nf->not_null) {
            snprintf(why, whysz, "Field '%s' is NOT NULL and has no default; existing rows cannot be backfilled", nf->name);
            return 1;
        }
        p->cells[k] = (PkrCell){NULL, 0, 1};
        return 0;
    }
    char err[96] = {0};
    int rc = packed_set_text(p, k, src, err, sizeof(err));
    if (rc) {
        int en = errno;
        snprintf(why, whysz, "default for field '%s' cannot be stored: %s",
                 nf->name, err[0] ? err : "invalid value");
        return en == ENOMEM ? -1 : 1;
    }
    return 0;
}

/* Re-compose one OLD packed record under the NEW packed schema.
   0 ok (*out malloc'd), 1 value does not fit (*bad_field, why), -1 hard error. */
static int packed_recompose_record(const PackedRowCtx *ctx, size_t klen,
                                   const void *value, size_t vlen, uint8_t hdr2,
                                   uint8_t **out, size_t *outlen,
                                   int *bad_field, char *why, size_t whysz) {
    const TypedSchema *ots = ctx->old_ts, *nts = ctx->new_ts;
    *out = NULL; *outlen = 0; *bad_field = -1;
    TypedRowView view;
    if (typed_row_open(ots, (const uint8_t *)value, vlen, hdr2, &view) != 0) {
        snprintf(why, whysz, "malformed source record (hdr2=%u vlen=%zu)", hdr2, vlen);
        return -1;
    }
    TypedPrepared p;
    typed_prepared_init(&p);
    p.schema = nts;
    int rc = -1;
    for (int k = 0; k < nts->nfields; k++) {
        const TypedField *nf = &nts->fields[k];
        int oi = ctx->new_to_old[k];
        if (nf->removed) { p.cells[k] = (PkrCell){NULL, 0, 1}; continue; }
        if (oi < 0) {
            rc = packed_fill_added(ctx, &p, k, why, whysz);
            if (rc) { *bad_field = k; goto done; }
            continue;
        }
        const TypedField *of = &ots->fields[oi];
        if (of->removed) { p.cells[k] = (PkrCell){NULL, 0, 1}; continue; }
        TypedRowSpan sp;
        if (typed_row_span(&view, (size_t)oi, &sp) != 0) {
            snprintf(why, whysz, "malformed source field '%s'", of->name);
            rc = -1;
            goto done;
        }
        if (sp.is_null) { p.cells[k] = (PkrCell){NULL, 0, 1}; continue; }   /* NULL stays NULL */
        rc = packed_convert_cell(of, &ots->packed.fields[oi], nf, &nts->packed.fields[k],
                                 &sp, &p, k, why, whysz);
        if (rc) { *bad_field = k; goto done; }
    }
    uint8_t *buf = NULL;
    size_t len = 0;
    char err[160] = {0};
    if (typed_prepared_alloc(&p, &buf, &len, err, sizeof(err)) != 0) {
        snprintf(why, whysz, "%s", err[0] ? err : "record composition failed");
        rc = errno == ENOMEM ? -1 : 1;
        goto done;
    }
    size_t record_bytes = 0;
    if (pkr_record_fits(&nts->packed, len, klen, SLOTCASK_SEG_MAX_BYTES, &record_bytes) != DEC_OK) {
        free(buf);
        snprintf(why, whysz, "record exceeds the segment limit under the new layout");
        rc = 1;
        goto done;
    }
    *out = buf;
    *outlen = len;
    rc = 0;
done:
    typed_prepared_free(&p);
    return rc;
}

static void packed_fail_text(V2RebuildCtx *ctx, int bad_field, size_t klen,
                             const void *key, const char *why) {
    char k[128];
    layout_diag_key(k, sizeof(k), key, klen);
    snprintf(ctx->why, sizeof(ctx->why), "field '%s' key '%s': %s",
             bad_field >= 0 ? ctx->new_ts->fields[bad_field].name : "-", k, why);
    packed_json_safe(ctx->why);
}

/* v2_rebuild_walk_cb's slot_changed branch for packed objects. */
static int packed_rebuild_walk(V2RebuildCtx *ctx, const void *key, size_t klen,
                               const void *value, size_t vlen, uint8_t hdr2) {
    PackedRowCtx row = { ctx->old_ts, ctx->new_ts, ctx->new_to_old, ctx->backfill };
    uint8_t *out = NULL;
    size_t outlen = 0;
    int bad = -1;
    char why[160] = {0};
    int rc = packed_recompose_record(&row, klen, value, vlen, hdr2, &out, &outlen,
                                     &bad, why, sizeof(why));
    if (rc != 0) {
        packed_fail_text(ctx, bad, klen, key, why);
        ctx->error = 1;
        return 1;
    }
    SlotcaskUpsertOpts o = {0};
    o.record_hdr2 = typed_schema_hdr2(ctx->new_ts);
    if (slotcask_insert_with_hooks(ctx->new_db, -1, key, klen, out, outlen, &o, NULL) != 0) {
        free(out);
        snprintf(ctx->why, sizeof(ctx->why), "insert into the rebuilt object failed");
        ctx->error = 1;
        return 1;
    }
    free(out);
    ctx->live_count++;
    return 0;
}

/* Read-only walk used by edit-field's pre-flight and dry_run. */
typedef struct {
    PackedRowCtx row;
    int          failed;      /* 1 = a row does not fit, -1 = hard error */
    char         field[128];
    char         reason[192];
} PackedPreflightCtx;

static int packed_preflight_cb(const uint8_t hash16[16], const void *key, size_t klen,
                               const void *value, size_t vlen, uint8_t hdr2, void *ud) {
    (void)hash16;
    PackedPreflightCtx *c = (PackedPreflightCtx *)ud;
    if (c->failed) return 1;
    uint8_t *out = NULL;
    size_t outlen = 0;
    int bad = -1;
    char why[160] = {0};
    int rc = packed_recompose_record(&c->row, klen, value, vlen, hdr2, &out, &outlen,
                                     &bad, why, sizeof(why));
    free(out);
    if (rc == 0) return 0;
    char k[128];
    layout_diag_key(k, sizeof(k), key, klen);
    snprintf(c->field, sizeof(c->field), "%s",
             bad >= 0 ? c->row.new_ts->fields[bad].name : "-");
    snprintf(c->reason, sizeof(c->reason), "key '%s': %s", k, why);
    packed_json_safe(c->field);
    packed_json_safe(c->reason);
    c->failed = rc;
    return 1;
}

/* 0 every row converts, 1 a row does not fit (field/reason filled), -1 hard error. */
int packed_preflight_walk(SlotcaskDb *sdb, const TypedSchema *old_ts,
                          const TypedSchema *new_ts, const int *new_to_old,
                          char *field, size_t fsz, char *reason, size_t rsz) {
    PackedPreflightCtx c;
    memset(&c, 0, sizeof(c));
    c.row = (PackedRowCtx){ old_ts, new_ts, new_to_old, NULL };
    slotcask_walk_live(sdb, packed_preflight_cb, &c);
    if (c.failed) {
        snprintf(field, fsz, "%s", c.field);
        snprintf(reason, rsz, "%s", c.reason);
    }
    return c.failed;
}

/* Encode `text` as field k of `nts` to prove it is storable. 0 ok, -1 + err. */
static int packed_probe_text(const TypedSchema *nts, int k, const char *text,
                             char *err, size_t errsz) {
    TypedPrepared p;
    typed_prepared_init(&p);
    p.schema = nts;
    char e[128] = {0};
    int rc = packed_set_text(&p, k, text, e, sizeof(e));
    typed_prepared_free(&p);
    if (rc) snprintf(err, errsz, "default for field '%s' cannot be stored: %s",
                     nts->fields[k].name, e[0] ? e : "invalid value");
    return rc;
}

/* Pre-transaction proof that every added field's default is storable (A-3).
   Literal: the literal itself. seq: "1" (type compatibility; the last value of
   the reserved range is probed again after reservation). uuid(): a sample
   UUID. random(N): 2N hex characters. */
int packed_probe_added(const TypedSchema *nts, const int *new_to_old,
                       char *err, size_t errsz) {
    for (int k = 0; k < nts->nfields; k++) {
        if (new_to_old[k] >= 0 || nts->fields[k].removed) continue;
        const TypedField *nf = &nts->fields[k];
        char buf[600];
        const char *sample = NULL;
        switch (nf->default_kind) {
        case DK_LITERAL: sample = nf->default_val; break;
        case DK_SEQ:     sample = "1"; break;
        case DK_UUID:    sample = "00000000-0000-4000-8000-000000000000"; break;
        case DK_RANDOM: {
            int n = atoi(nf->default_val);
            if (n < 1 || n > 256) {
                snprintf(err, errsz, "random(N) requires 1 <= N <= 256 in field '%s'", nf->name);
                return -1;
            }
            memset(buf, 'a', (size_t)n * 2);
            buf[n * 2] = '\0';
            sample = buf;
            break;
        }
        default: continue;   /* none / auto_*: NULL (or a NOT NULL refusal in the walk) */
        }
        if (packed_probe_text(nts, k, sample, err, errsz) != 0) return -1;
    }
    return 0;
}

/* Exact-suffix tombstone test: the loader treats only a line that ENDS in
   ":removed" as a tombstone (A-4). */
static int fields_line_is_removed(const char *line) {
    size_t n = strlen(line);
    return n >= 8 && strcmp(line + n - 8, ":removed") == 0;
}

/* ================================================================== */

```

**3.2d** Declare the two cross-file exports in `src/db/query_internal.h`. Anchor (once):
`int parse_field_line(const char *line, TypedField *out);` → replace with:
```c
int parse_field_line(const char *line, TypedField *out);
int packed_preflight_walk(SlotcaskDb *sdb, const TypedSchema *old_ts,
                          const TypedSchema *new_ts, const int *new_to_old,
                          char *field, size_t fsz, char *reason, size_t rsz);
int packed_probe_added(const TypedSchema *nts, const int *new_to_old,
                       char *err, size_t errsz);
```
(`SlotcaskDb`/`TypedSchema` are already visible in this header; if the compiler objects, add `#include "slotcask.h"` — it is already included transitively by `types.h`.)

**3.2e** Dispatch to the packed walk. Anchor (once):
```c
    /* Slot layout changed (compact, add-field, edit-field, or any combo):
       recompose typed payload by mapping new field offsets ← old field
```
→ insert **before** it:
```c
    if (ctx->old_ts->packed.nfields)
        return packed_rebuild_walk(ctx, key, klen, value, vlen, hdr2);

```
(i.e. the new two lines, a blank line, then the unchanged comment.)

**3.2f** `parse_field_line_packed`, `packed_read_decl_lines`, `packed_free_decl_lines`. Insert **immediately after** the end of `parse_field_line`
(anchor: the line `/* === v2 (slotcask) rebuild path ===` — occurs once; insert before it):

```c
int parse_field_line_packed(const char *line, TypedField *out, PkrField *pf_out) {
    const char *colon = strchr(line, ':');
    if (!colon || colon == line) return 0;
    size_t nlen = colon - line;
    if (nlen >= sizeof(out->name)) return 0;
    if (validate_packed_field_type(line) <= 0) return 0;   /* the single packed-grammar authority */
    out->name_len = (int)nlen;
    memcpy(out->name, line, nlen);
    out->name[nlen] = '\0';
    char *clean_spec = strdup(colon + 1);
    if (!clean_spec) return 0;
    if (parse_default_modifier_checked(clean_spec, out) < 0) { free(clean_spec); return 0; }
    PkrField pf = {0};
    int ok = 0;
    if (!strncmp(clean_spec, "varchar", 7) || !strncmp(clean_spec, "numeric", 7) ||
        !strcmp(clean_spec, "currency")) {
        if (pkr_field_parse(clean_spec, &pf) == DEC_OK) {
            out->type = pf.kind == PKR_VARCHAR ? FT_VARCHAR : FT_NUMERIC;
            out->size = (int)pf.width;
            out->numeric_scale = pf.scale;
            ok = 1;
        }
    } else {
        parse_field_type(clean_spec, out);
        ok = out->type != FT_NONE && out->size > 0 && pkr_field_fixed((uint32_t)out->size, &pf) == DEC_OK &&
             !typed_default_validate(out);
    }
    free(clean_spec);
    if (ok && pf_out) *pf_out = pf;
    return ok;
}

int packed_read_decl_lines(const char *obj_dir, char ***lines_out, size_t *n_out) {
    *lines_out = NULL;
    *n_out = 0;
    char path[PATH_MAX];
    if (snprintf(path, sizeof(path), "%s/fields.conf", obj_dir) >= (int)sizeof(path)) return -1;
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    char **v = calloc(MAX_FIELDS, sizeof(char *));
    size_t n = 0;
    if (!v) { fclose(f); return -1; }
    for (;;) {
        int rderr = 0;
        char *line = conf_read_line(f, MAX_FIELD_DEF, &rderr);
        if (!line) {
            if (rderr) goto fail;
            break;
        }
        if (line[0] == '\0' || line[0] == '#' || !strchr(line, ':')) { free(line); continue; }
        if (n >= MAX_FIELDS) { free(line); goto fail; }
        v[n++] = line;
    }
    fclose(f);
    *lines_out = v;
    *n_out = n;
    return 0;
fail:
    fclose(f);
    packed_free_decl_lines(v, n);
    return -1;
}

void packed_free_decl_lines(char **lines, size_t n) {
    if (!lines) return;
    for (size_t i = 0; i < n; i++) free(lines[i]);
    free(lines);
}

```
*(Note: `load_typed_schema` also stops at `MAX_FIELDS`; a file with more declaration lines is refused here rather than truncated.)*

### 3.3 `rebuild_object_v2` — `src/db/query_find.c` (anchored hunks)

**H1 — replace the D9 guard.** Anchor (once):
```c
    /* D9: record-recomposing operations are not implemented for the packed
       layout in R5. Verbatim copies (splits/streams only) are layout-
       agnostic and stay allowed. Refuse before touching anything. */
    if (old_ts->packed.nfields && (slot_changed || drop_tombstoned || n_added > 0)) {
        OUT("{\"error\":\"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet\"}\n");
        return 1;
    }
```
→
```c
    /* R5b: packed objects recompose through packed_rebuild_walk. The layout
       never changes inside this function; the slot bound must be the one the
       loader will derive (load_schema: 24 + max_key + maximum, aligned). */
    const int packed = old_ts->packed.nfields != 0;
    if (packed != (new_ts->packed.nfields != 0)) {
        OUT("{\"error\":\"internal: layout mismatch between old and new schema\"}\n");
        return 1;
    }
    if (packed) {
        size_t bound = 0;
        if (pkr_slot_bound(&new_ts->packed, (size_t)old_sch->max_key, &bound) != DEC_OK ||
            bound > INT_MAX || (int)bound != new_sch->slot_size) {
            OUT("{\"error\":\"packed schema exceeds the maximum slot size\"}\n");
            return 1;
        }
    }
```

**H2 — larger failure buffer.** Anchor (once): `    char failure_buf[128];` → `    char failure_buf[320];`

**H3 — range probe for `seq`.** Anchor (once):
```c
                atomic_store_explicit(&walk_ctx.backfill[k].seq_next,
                                       start, memory_order_relaxed);
```
→
```c
                atomic_store_explicit(&walk_ctx.backfill[k].seq_next,
                                       start, memory_order_relaxed);
                if (packed) {
                    /* A-3: the reserved range is already consumed (documented);
                       prove its LAST value is storable before any row is written. */
                    char last[32], perr[160] = {0};
                    snprintf(last, sizeof(last), "%lld", (long long)(start + pf_live - 1));
                    if (packed_probe_text(new_ts, k, last, perr, sizeof(perr)) != 0) {
                        snprintf(failure_buf, sizeof(failure_buf), "Sequence values do not fit field: %s", perr);
                        packed_json_safe(failure_buf);
                        failure = failure_buf;
                        goto txn_fail;
                    }
                }
```
*(`packed_probe_text` is `static` and defined above `v2_rebuild_walk_cb`, which precedes `rebuild_object_v2` in the file — no forward declaration needed.)*

**H4 — walk failure reason.** Anchor (once):
```c
    if (walk_err) {
        failure = "Rebuild walk failed";
        goto txn_fail;
    }
```
→
```c
    if (walk_err) {
        if (walk_ctx.why[0]) {
            snprintf(failure_buf, sizeof(failure_buf), "Rebuild walk failed: %s", walk_ctx.why);
            failure = failure_buf;
        } else {
            failure = "Rebuild walk failed";
        }
        goto txn_fail;
    }
```

**H5 — exact-suffix tombstone filter (A-4).** Anchor (once): `            if (drop_tombstoned && strstr(line, ":removed")) {` →
`            if (drop_tombstoned && fields_line_is_removed(line)) {`

**H6 — post-swap schema agreement.** Anchor (once):
```c
    rebuild_test_pause(obj_dir, "after-metadata");

    invalidate_schema_caches(db_root, object);
    invalidate_idx_cache(db_root, object);
    slotcask_registry_invalidate(db_root, object);

    int idx_rebuilt = 0;
```
→
```c
    rebuild_test_pause(obj_dir, "after-metadata");

    invalidate_schema_caches(db_root, object);
    invalidate_idx_cache(db_root, object);
    slotcask_registry_invalidate(db_root, object);

    if (packed) {
        /* F5: the schema the walk composed against must be exactly what the
           loader now reads from the swapped fields.conf. old_ts may alias a
           cache slot that this load can reuse — it is not touched below. */
        Schema chk_sch = load_schema(db_root, object);
        TypedSchema *chk = load_typed_schema(db_root, object);
        int same = chk && chk->nfields == new_ts->nfields &&
                   chk->packed.nfields == new_ts->packed.nfields &&
                   chk_sch.slot_size == new_sch->slot_size &&
                   packed_schema_same(&chk->packed, &new_ts->packed);
        for (int i = 0; same && i < new_ts->nfields; i++)
            same = strcmp(chk->fields[i].name, new_ts->fields[i].name) == 0 &&
                   chk->fields[i].removed == new_ts->fields[i].removed;
        if (!same) {
            failure = "Rebuilt fields.conf does not match the in-memory packed schema";
            goto txn_fail;
        }
        invalidate_schema_caches(db_root, object);
        invalidate_idx_cache(db_root, object);
        slotcask_registry_invalidate(db_root, object);
    }

    int idx_rebuilt = 0;
```
*(`goto txn_fail` crossing the declaration of `idx_rebuilt` is already the file's pattern; if the compiler warns about a jump over an initialised declaration, hoist `int idx_rebuilt = 0;` to the top of the function — warnings must stay at zero.)*

### 3.4 `rebuild_object` packed branch — `src/db/query_find.c`

**H7 — replace the packed block.** Anchor: the entire block from
```c
    /* D9: a packed object supports only verbatim splits/streams rebuilds.
```
through the closing brace line `    }` that precedes the comment `    /* Build new TypedSchema:`. Quote the block exactly as in the file:
```c
    /* D9: a packed object supports only verbatim splits/streams rebuilds.
       Reuse the bound schema as-is; the legacy-shaped construction below
       would re-sum descriptor widths and mis-size the slot. */
    if (old_ts->packed.nfields) {
        if (drop_tombstoned || n_added > 0) {
            OUT("{\"error\":\"vacuum compact / add-field / edit-field are not supported on packed-layout objects yet\"}\n");
            return 1;
        }
        if (!splits_changed && !streams_changed) {
            OUT("{\"status\":\"noop\",\"reason\":\"no change requested\"}\n");
            return 0;
        }
        TypedSchema same = *old_ts;
        int identity[MAX_FIELDS];
        for (int i = 0; i < old_ts->nfields; i++) identity[i] = i;
        Schema same_sch = old_sch;
        same_sch.splits = new_splits;
        same_sch.streams = new_streams;
        return rebuild_object_v2(db_root, object, &old_sch, old_ts,
                                 &same_sch, &same, identity,
                                 0, splits_changed, 0, added_lines, 0, NULL);
    }
```
→
```c
    /* A packed object never goes through the legacy-shaped construction below
       (it would re-sum descriptor widths and mis-size the slot). */
    if (old_ts->packed.nfields)
        return rebuild_object_packed(db_root, object, &old_sch, old_ts,
                                     new_splits, splits_changed, new_streams,
                                     drop_tombstoned, added_lines, n_added);
```
and insert the following **complete function immediately before** `int rebuild_object(const char *db_root, const char *object,` (anchor: the two lines
`int rebuild_object(const char *db_root, const char *object,` / `                   int new_splits_arg, int drop_tombstoned,` — once):

```c
/* Packed-layout rebuild. Verbatim copy (splits/streams only) is unchanged from
   R5; add-field / vacuum compact recompose every record (R5b). */
static int rebuild_object_packed(const char *db_root, const char *object,
                                 const Schema *old_sch, const TypedSchema *old_ts,
                                 int new_splits, int splits_changed, int new_streams,
                                 int drop_tombstoned,
                                 char (*added_lines)[MAX_FIELD_DEF], int n_added) {
    int streams_changed = (new_streams != old_sch->streams);
    if (!drop_tombstoned && n_added == 0) {
        if (!splits_changed && !streams_changed) {
            OUT("{\"status\":\"noop\",\"reason\":\"no change requested\"}\n");
            return 0;
        }
        TypedSchema same = *old_ts;
        int identity[MAX_FIELDS];
        for (int i = 0; i < old_ts->nfields; i++) identity[i] = i;
        Schema same_sch = *old_sch;
        same_sch.splits = new_splits;
        same_sch.streams = new_streams;
        return rebuild_object_v2(db_root, object, old_sch, old_ts,
                                 &same_sch, &same, identity,
                                 0, splits_changed, 0, added_lines, 0, NULL);
    }

    /* Recompose: the new declaration list is the old fields.conf lines
       (compact drops tombstones) followed by the added lines — the exact list
       rebuild_object_v2 writes back to fields.conf. */
    char obj_dir[PATH_MAX];
    if (snprintf(obj_dir, sizeof(obj_dir), "%s/%s", db_root, object) >= (int)sizeof(obj_dir)) {
        OUT("{\"error\":\"Object path too long\"}\n");
        return 1;
    }
    char **decl = NULL;
    size_t ndecl = 0;
    if (packed_read_decl_lines(obj_dir, &decl, &ndecl) != 0 || ndecl != (size_t)old_ts->nfields) {
        packed_free_decl_lines(decl, ndecl);
        OUT("{\"error\":\"fields.conf does not match the loaded schema; refusing to rebuild\"}\n");
        return 1;
    }
    const char *kept[MAX_FIELDS];
    int new_to_old[MAX_FIELDS];
    size_t nk = 0;
    int rc = 1;
    TypedSchema new_ts;
    memset(&new_ts, 0, sizeof(new_ts));
    int built = 0;
    for (size_t i = 0; i < ndecl; i++) {
        if (drop_tombstoned && fields_line_is_removed(decl[i])) continue;
        kept[nk] = decl[i];
        new_to_old[nk++] = (int)i;
    }
    for (int a = 0; a < n_added; a++) {
        if (nk >= MAX_FIELDS) {
            OUT("{\"error\":\"Too many fields (max %d)\"}\n", MAX_FIELDS);
            goto out;
        }
        kept[nk] = added_lines[a];
        new_to_old[nk++] = -1;
    }
    if (nk == 0) {
        OUT("{\"error\":\"compact refused: every field is tombstoned; an object needs at least one field\"}\n");
        goto out;
    }
    for (size_t i = 0; i < nk; i++) {          /* duplicate names (tombstoned included) */
        const char *ci = strchr(kept[i], ':');
        size_t li = ci ? (size_t)(ci - kept[i]) : strlen(kept[i]);
        for (size_t j = i + 1; j < nk; j++) {
            const char *cj = strchr(kept[j], ':');
            size_t lj = cj ? (size_t)(cj - kept[j]) : strlen(kept[j]);
            if (li == lj && memcmp(kept[i], kept[j], li) == 0) {
                OUT("{\"error\":\"Field [%.*s] already exists\"}\n", (int)li, kept[i]);
                goto out;
            }
        }
    }
    char berr[192] = {0};
    if (typed_schema_build_packed(kept, nk, &new_ts, berr, sizeof(berr)) != 0) {
        packed_json_safe(berr);
        OUT("{\"error\":\"Invalid packed field declarations: %s\"}\n", berr);
        goto out;
    }
    built = 1;
    if (packed_probe_added(&new_ts, new_to_old, berr, sizeof(berr)) != 0) {
        packed_json_safe(berr);
        OUT("{\"error\":\"%s\"}\n", berr);
        goto out;
    }
    size_t bound = 0;
    if (pkr_slot_bound(&new_ts.packed, (size_t)old_sch->max_key, &bound) != DEC_OK || bound > INT_MAX) {
        OUT("{\"error\":\"packed schema exceeds the maximum slot size\"}\n");
        goto out;
    }
    Schema new_sch = *old_sch;
    new_sch.splits = new_splits;
    new_sch.streams = new_streams;
    new_sch.max_value = new_ts.total_size;
    new_sch.slot_size = (int)bound;
    rc = rebuild_object_v2(db_root, object, old_sch, old_ts, &new_sch, &new_ts,
                           new_to_old, 1 /* always recompose */, splits_changed,
                           drop_tombstoned, added_lines, n_added, NULL);
out:
    if (built) typed_schema_release(&new_ts);
    packed_free_decl_lines(decl, ndecl);
    return rc;
}
```
*(`goto out` jumps over `char berr[192]`/`size_t bound`/`Schema new_sch` initialisations: declare `berr`, `bound` and `new_sch` above the first `goto` if the compiler warns — zero warnings is a Definition-of-done item.)*

### 3.5 `cmd_add_fields` — `src/db/config.c`

**H8 — replace the refusal.** Anchor (once):
```c
    if (ts->packed.nfields) {
        OUT("{\"error\":\"add-field is not supported on packed-layout objects yet\"}\n");
        return 1;
    }
```
→
```c
    const int packed_obj = ts->packed.nfields != 0;   /* R5b: packed grammar below */
```

**H9 — packed grammar check per spec line.** Anchor (once):
```c
        /* Type parseability — use a throwaway TypedField. parse_default_modifier
```
→ insert **before** it:
```c
        if (packed_obj) {
            TypedField ptf;
            PkrField ppf;
            memset(&ptf, 0, sizeof(ptf));
            if (!parse_field_line_packed(lines[a], &ptf, &ppf)) {
                free_enum_values(&ptf);
                OUT("{\"error\":\"Invalid field line: %s\"}\n", lines[a]);
                return 1;
            }
            if (ptf.default_kind == DK_RANDOM) {
                int n_bytes = atoi(ptf.default_val);
                if (n_bytes < 1 || n_bytes > 256) {
                    free_enum_values(&ptf);
                    OUT("{\"error\":\"random(N) requires 1 <= N <= 256 in: %s\"}\n", lines[a]);
                    return 1;
                }
                if (ptf.type == FT_VARCHAR && (uint32_t)(n_bytes * 2) > ppf.payload_max) {
                    free_enum_values(&ptf);
                    OUT("{\"error\":\"random(%d) produces %d hex chars but field [%s] holds only %u\"}\n",
                        n_bytes, n_bytes * 2, name, ppf.payload_max);
                    return 1;
                }
            }
            free_enum_values(&ptf);
            continue;   /* storability of every default is probed in rebuild_object */
        }
        /* Type parseability — use a throwaway TypedField. parse_default_modifier
```
(`config.c` already includes `types.h`, which now declares `parse_field_line_packed`/`PkrField`.)

### 3.6 `cmd_edit_fields` and helpers — `src/db/query_schema.c`

**H10 — includes.** Anchor (once): `#include "packed_record.h"` → `#include "packed_record.h"\n#include "typed_row.h"`.

**H11 — export the packed validator.** Anchor (once): `static int validate_packed_field_type(const char *field_spec) {` → `int validate_packed_field_type(const char *field_spec) {`.

**H12 — packed-aware NULL scan.** Replace the struct and callback head. Anchor (once):
```c
typedef struct {
    const TypedField *fld;      /* field under test                      */
    int               field_idx;
```
→
```c
typedef struct {
    const TypedSchema *ts;      /* NULL-safe; packed schemas scan by cell  */
    const TypedField *fld;      /* field under test                      */
    int               field_idx;
```
Anchor (once):
```c
    (void)hash16; (void)key; (void)klen;
    NotNullScanCtx *c = (NotNullScanCtx *)ud;
    const uint8_t *base = NULL; const uint8_t *bm = NULL;
```
→
```c
    (void)hash16; (void)key; (void)klen;
    NotNullScanCtx *c = (NotNullScanCtx *)ud;
    if (c->ts && c->ts->packed.nfields) {
        TypedRowView rv;
        TypedRowSpan sp;
        if (typed_row_open(c->ts, (const uint8_t *)value, vlen, hdr2, &rv) != 0 ||
            typed_row_span(&rv, (size_t)c->field_idx, &sp) != 0) {
            c->broken = 1;
            return 1;
        }
        if (sp.is_null) c->nulls++;
        return 0;
    }
    const uint8_t *base = NULL; const uint8_t *bm = NULL;
```
Anchor (once): `    ctx.fld = fld;\n    ctx.field_idx = field_idx;` → `    ctx.ts = ts;\n    ctx.fld = fld;\n    ctx.field_idx = field_idx;`

**H13 — extracted merge + reduced rewrite.** Replace the **entire** function `rewrite_fields_conf_for_edit` (from its doc comment
`/* Rewrite fields.conf in place, replacing each edited line with its new` through its closing `}` immediately before the comment
`/* Selective reindex driver`) with:

```c
/* Merge one edit spec into an existing fields.conf line: carry the old default
   when the edit omits one, resolve :not_null / :nullable independently, emit
   canonically (base + at most one default modifier + at most one trailing
   nullability token). 0 ok, -1 refuse (logged). `out` needs
   MAX_FIELD_DEF + FIELD_MODS_MAX + 16 bytes. Single implementation shared by
   rewrite_fields_conf_for_edit and the packed edit path (F6). */
static int edit_merge_line(const char *line, const char *edit_spec,
                           const char *edit_name, char *out, size_t outcap) {
    char old_mods[FIELD_MODS_MAX] = "";
    default_modifiers_for_line(line, old_mods, sizeof(old_mods));
    /* A modifier that exceeds the carry buffer can't be re-emitted — writing
       the replacement without it would silently drop the field's default. */
    if (!old_mods[0] && line_has_default_modifier(line)) {
        LOG_ERROR(LOG_SUB_CONFIG,
                  "edit_merge_line: a field line carries a default modifier larger "
                  "than %d bytes — refusing to rewrite (an edit would drop it)",
                  (int)sizeof(old_mods) - 1);
        return -1;
    }
    char old_default[FIELD_MODS_MAX] = "";
    memcpy(old_default, old_mods, sizeof(old_default));
    int old_has_not_null = strstr(line, ":not_null") != NULL;
    char *old_nn = strstr(old_default, ":not_null");
    char *old_nl = strstr(old_default, ":nullable");
    if (old_nn) *old_nn = '\0';
    if (old_nl) *old_nl = '\0';

    char new_spec[MAX_FIELD_DEF];
    size_t slen = strlen(edit_spec);
    memcpy(new_spec, edit_spec, slen + 1);
    char new_default[FIELD_MODS_MAX] = "";
    default_modifiers_for_line(new_spec, new_default, sizeof(new_default));
    if (!new_default[0] && line_has_default_modifier(new_spec)) {
        LOG_ERROR(LOG_SUB_CONFIG,
                  "edit_merge_line: edit spec for [%s] carries a default modifier "
                  "larger than %d bytes — refusing to rewrite (the default would be dropped)",
                  edit_name, (int)sizeof(new_default) - 1);
        return -1;
    }
    int new_not_null = 0, new_nullable = 0;
    char *nn_in_new = strstr(new_default, ":not_null");
    char *nl_in_new = strstr(new_default, ":nullable");
    if (nn_in_new && nl_in_new) {
        LOG_ERROR(LOG_SUB_CONFIG,
                  "edit_merge_line: edit spec for [%s] carries both nullability tokens — refusing",
                  edit_name);
        return -1;
    }
    if (nn_in_new) { *nn_in_new = '\0'; new_not_null = 1; }
    if (nl_in_new) { *nl_in_new = '\0'; new_nullable = 1; }
    if (!new_default[0]) {
        size_t blen = strlen(new_spec);
        if (blen >= 9 && strcmp(new_spec + blen - 9, ":not_null") == 0) {
            new_spec[blen - 9] = '\0';
            new_not_null = 1;
        } else if (blen >= 9 && strcmp(new_spec + blen - 9, ":nullable") == 0) {
            new_spec[blen - 9] = '\0';
            new_nullable = 1;
        }
    }
    char *new_dm = strstr(new_spec, ":default=");
    if (!new_dm) new_dm = strstr(new_spec, ":auto_create");
    if (!new_dm) new_dm = strstr(new_spec, ":auto_update");
    if (new_dm) *new_dm = '\0';

    const char *use_default = new_default[0] ? new_default : old_default;
    int use_not_null = new_not_null ? 1 : new_nullable ? 0 : old_has_not_null;
    int n = use_not_null ? snprintf(out, outcap, "%s%s:not_null", new_spec, use_default)
                         : snprintf(out, outcap, "%s%s", new_spec, use_default);
    return (n < 0 || (size_t)n >= outcap) ? -1 : 0;
}

/* Rewrite fields.conf in place, replacing each edited line with its merged
   spec (edit_merge_line). Lines unaffected by the edit pass through unchanged. */
static int rewrite_fields_conf_for_edit(const char *obj_dir,
                                         char (*edit_lines)[MAX_FIELD_DEF], int n_edits) {
    char fpath[PATH_MAX], fpath_new[PATH_MAX];
    snprintf(fpath,     sizeof(fpath),     "%s/fields.conf", obj_dir);
    snprintf(fpath_new, sizeof(fpath_new), "%s/fields.conf.new", obj_dir);

    char edit_names[MAX_FIELDS][128];
    for (int e = 0; e < n_edits; e++) {
        const char *colon = strchr(edit_lines[e], ':');
        size_t nlen = colon ? (size_t)(colon - edit_lines[e]) : strlen(edit_lines[e]);
        if (nlen >= sizeof(edit_names[0])) nlen = sizeof(edit_names[0]) - 1;
        memcpy(edit_names[e], edit_lines[e], nlen);
        edit_names[e][nlen] = '\0';
    }

    const size_t merged_cap = (size_t)MAX_FIELD_DEF + FIELD_MODS_MAX + 16;
    char *merged = malloc(merged_cap);
    if (!merged) return -1;
    FILE *fin = fopen(fpath, "r");
    int fin_errno = errno;
    FILE *fout = fopen(fpath_new, "w");
    int fout_errno = errno;
    if (!fin || !fout) {
        if (!fin) LOG_ERROR(LOG_SUB_CONFIG, "rewrite_fields_conf_for_edit: fopen %s failed: %s", fpath, strerror(fin_errno));
        if (!fout) LOG_ERROR(LOG_SUB_CONFIG, "rewrite_fields_conf_for_edit: fopen %s failed: %s", fpath_new, strerror(fout_errno));
        if (fin) fclose(fin);
        if (fout) fclose(fout);
        free(merged);
        return -1;
    }
    for (;;) {
        int rderr = 0;
        char *line = conf_read_line(fin, MAX_FIELD_DEF, &rderr);
        if (!line) {
            if (rderr) {
                LOG_ERROR(LOG_SUB_CONFIG,
                          "rewrite_fields_conf_for_edit: fields.conf line exceeds "
                          "%d bytes — refusing to rewrite a schema we cannot "
                          "parse faithfully", MAX_FIELD_DEF - 1);
                fclose(fin);
                fclose(fout);
                unlink(fpath_new);
                free(merged);
                return -1;
            }
            break;   /* EOF */
        }
        if (line[0] == '\0' || line[0] == '#') {
            fputs(line, fout);
            fputc('\n', fout);   /* conf_read_line strips the newline */
            free(line);
            continue;
        }
        const char *colon = strchr(line, ':');
        size_t nlen = colon ? (size_t)(colon - line) : strlen(line);
        int matched = -1;
        for (int e = 0; e < n_edits; e++) {
            size_t elen = strlen(edit_names[e]);
            if (nlen == elen && memcmp(line, edit_names[e], elen) == 0) {
                matched = e;
                break;
            }
        }
        if (matched < 0) {
            fputs(line, fout);
            fputc('\n', fout);
            free(line);
            continue;
        }
        if (edit_merge_line(line, edit_lines[matched], edit_names[matched], merged, merged_cap) != 0) {
            free(line);
            fclose(fin);
            fclose(fout);
            unlink(fpath_new);
            free(merged);
            return -1;
        }
        fprintf(fout, "%s\n", merged);
        free(line);
    }
    fclose(fin);
    int wok = (fflush(fout) == 0 && !ferror(fout));
    if (fclose(fout) != 0) wok = 0;
    free(merged);
    if (!wok) {
        LOG_ERROR(LOG_SUB_CONFIG, "rewrite_fields_conf_for_edit: write %s failed", fpath_new);
        unlink(fpath_new);
        return -1;
    }
    /* D-6: fsync(tmp) -> rename -> fsync(parent). prc==1 (renamed, parent sync
       failed) is reported as failure; the edit is idempotent, so a retry is safe. */
    int prc = durability_publish_replace(fpath, fpath_new, NULL, NULL);
    if (prc != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "rewrite_fields_conf_for_edit: durable replace of %s failed (rc=%d): %s", fpath, prc, strerror(errno));
        if (prc < 0) unlink(fpath_new);
        return -1;
    }
    return 0;
}

/* Packed edit: build the NEW packed TypedSchema from the old fields.conf lines
   with each edit merged in (the exact lines rewrite_fields_conf_for_edit will
   write). 0 ok (caller releases new_ts with typed_schema_release), -1 + err. */
static int edit_build_packed_ts(const char *db_root, const char *object,
                                const TypedSchema *old_ts,
                                char (*edit_lines)[MAX_FIELD_DEF],
                                const int *edited_old_idx, int n_edits,
                                TypedSchema *new_ts, int *new_to_old,
                                char *err, size_t errsz) {
    char obj_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/%s", db_root, object);
    char **decl = NULL;
    size_t n = 0;
    int rc = -1;
    if (packed_read_decl_lines(obj_dir, &decl, &n) != 0 || n != (size_t)old_ts->nfields) {
        snprintf(err, errsz, "fields.conf does not match the loaded schema");
        goto out;
    }
    const size_t cap = (size_t)MAX_FIELD_DEF + FIELD_MODS_MAX + 16;
    for (int e = 0; e < n_edits; e++) {
        int oi = edited_old_idx[e];
        char *merged = malloc(cap);
        if (!merged) { snprintf(err, errsz, "out of memory"); goto out; }
        if (edit_merge_line(decl[oi], edit_lines[e], old_ts->fields[oi].name, merged, cap) != 0) {
            free(merged);
            snprintf(err, errsz, "cannot merge the edit for field [%s]", old_ts->fields[oi].name);
            goto out;
        }
        free(decl[oi]);
        decl[oi] = merged;
    }
    if (typed_schema_build_packed((const char *const *)decl, n, new_ts, err, errsz) != 0) goto out;
    for (int i = 0; i < old_ts->nfields; i++) {
        new_to_old[i] = i;
        if (strcmp(new_ts->fields[i].name, old_ts->fields[i].name) != 0 ||
            new_ts->fields[i].removed != old_ts->fields[i].removed) {
            typed_schema_release(new_ts);
            snprintf(err, errsz, "edit changed the identity of field %d", i);
            goto out;
        }
    }
    rc = 0;
out:
    packed_free_decl_lines(decl, n);
    return rc;
}

static void edit_release_packed_ts(TypedSchema **p) {
    if (p && *p) typed_schema_release(*p);
}

/* JSON-envelope safety for messages that may echo field names / parser text. */
static void packed_json_safe_edit(char *s) {
    for (; *s; s++)
        if (*s == '"' || *s == '\\' || (unsigned char)*s < 0x20 || (unsigned char)*s > 0x7e) *s = '?';
}

/* True when an edit changes the field's stored encoding (drives reindex). */
static int edit_field_dirty(const TypedSchema *old_ts, const TypedSchema *new_ts, int i) {
    if (!old_ts->packed.nfields)
        return field_needs_transform(&old_ts->fields[i], &new_ts->fields[i]);
    const PkrField *a = &old_ts->packed.fields[i], *b = &new_ts->packed.fields[i];
    return a->kind != b->kind || a->width != b->width || a->payload_max != b->payload_max ||
           a->precision != b->precision || a->scale != b->scale || a->bare != b->bare ||
           old_ts->fields[i].type != new_ts->fields[i].type;
}
```
Callers of `rewrite_fields_conf_for_edit` (unchanged signature): `edit_finalize_metadata` and the no-rebuild branch of `cmd_edit_fields`.
`edit_merge_line` and the helpers are defined before first use (`rewrite_fields_conf_for_edit` sits above `selective_reindex_dirty`; the `FIELD_MODS_MAX`
define and `default_modifiers_for_line`/`line_has_default_modifier` are above it already). `edit_field_dirty`/`edit_build_packed_ts` reference
`field_needs_transform` (declared in `query_internal.h`).

**H14 — `cmd_edit_fields` hunks** (anchors each exactly once in `query_schema.c`):

(a) refusal → nothing. Replace
```c
    if (old_ts->packed.nfields) {
        OUT("{\"error\":\"edit-field is not supported on packed-layout objects yet\"}\n");
        return 1;
    }
```
with nothing (delete the 4 lines, keep the following blank line).

(b) layout-aware parse. Replace
```c
        if (!parse_field_line(lines[e], &parsed[e])) {
```
with
```c
        if (!(old_ts->packed.nfields ? parse_field_line_packed(lines[e], &parsed[e], NULL)
                                     : parse_field_line(lines[e], &parsed[e]))) {
```

(c) new-schema construction. Replace
```c
    /* Build new_ts: clone old_ts, overlay each edit, recompute offsets. */
    TypedSchema new_ts;
    memset(&new_ts, 0, sizeof(new_ts));
    new_ts.typed = 1;
    new_ts.nfields = old_ts->nfields;
    int new_to_old[MAX_FIELDS];
    int noff = 0;
    for (int i = 0; i < old_ts->nfields; i++) {
```
with
```c
    TypedSchema new_ts;
    TypedSchema *packed_new_owner __attribute__((cleanup(edit_release_packed_ts))) = NULL;
    int new_to_old[MAX_FIELDS];
    if (old_ts->packed.nfields) {
        char berr[192] = {0};
        if (edit_build_packed_ts(db_root, object, old_ts, lines, edited_old_idx, n_edits,
                                 &new_ts, new_to_old, berr, sizeof(berr)) != 0) {
            for (int _i = 0; _i < n_edits; _i++) free_enum_values(&parsed[_i]);
            packed_json_safe_edit(berr);
            OUT("{\"error\":\"Invalid field declaration: %s\"}\n", berr);
            return 1;
        }
        packed_new_owner = &new_ts;
    }
    /* Legacy: clone old_ts, overlay each edit, recompute offsets (not reindented
       on purpose — the packed schema above replaces this whole block). */
    int noff = 0;
    if (!old_ts->packed.nfields) {
    memset(&new_ts, 0, sizeof(new_ts));
    new_ts.typed = 1;
    new_ts.nfields = old_ts->nfields;
    for (int i = 0; i < old_ts->nfields; i++) {
```
(`packed_json_safe_edit` is defined in H13's block next to `edit_release_packed_ts`; `packed_json_safe` in `query_find.c` is `static`, so a small duplicate avoids exporting it.)

(d) close the legacy block. Replace
```c
    new_ts.total_size = noff;
    /* §2.10: hand-built schemas must carry the nullmap prefix and the
       prefix-inclusive total_size (matching load_typed_schema) — the
       rebuild's trim/encode hooks run against THIS schema. */
    new_ts.nullmap_prefix = 1 + typed_nullmap_bytes(new_ts.nfields);
    new_ts.total_size += new_ts.nullmap_prefix;
```
with the same text followed by a closing brace:
```c
    new_ts.total_size = noff;
    /* §2.10: hand-built schemas must carry the nullmap prefix and the
       prefix-inclusive total_size (matching load_typed_schema) — the
       rebuild's trim/encode hooks run against THIS schema. */
    new_ts.nullmap_prefix = 1 + typed_nullmap_bytes(new_ts.nfields);
    new_ts.total_size += new_ts.nullmap_prefix;
    }   /* end legacy overlay */
```

(e) dirty tracking. Replace `        if (field_needs_transform(&old_ts->fields[oi], &new_ts.fields[oi])) {` with `        if (edit_field_dirty(old_ts, &new_ts, oi)) {`.

(f) rebuild decision. Replace `    int needs_rebuild = (n_dirty > 0);` with
```c
    int needs_rebuild = old_ts->packed.nfields
        ? !packed_schema_same(&old_ts->packed, &new_ts.packed)   /* F4 */
        : (n_dirty > 0);
```

(g) pre-flight. Replace
```c
    slotcask_walk_live(sdb, edit_preflight_walk_cb, &pf);
    if (pf.failed) {
```
with
```c
    if (old_ts->packed.nfields) {
        /* Same converter as the rebuild (D-3). edit_preflight_walk_cb cannot be
           used: typed_record_view rejects packed records and would skip them. */
        int prc = packed_preflight_walk(sdb, old_ts, &new_ts, new_to_old,
                                        pf.fail_field, sizeof(pf.fail_field),
                                        pf.fail_reason, sizeof(pf.fail_reason));
        if (prc < 0) {
            OUT("{\"error\":\"Pre-flight failed on field [%s]: %s\"}\n",
                pf.fail_field, pf.fail_reason);
            return 1;
        }
        pf.failed = prc;
    } else {
        slotcask_walk_live(sdb, edit_preflight_walk_cb, &pf);
    }
    if (pf.failed) {
```

(h) slot size. Replace
```c
    new_sch.max_value = new_ts.total_size;
    new_sch.slot_size = (24 + new_sch.max_key + new_sch.max_value + 7) & ~7;
    if (new_sch.slot_size < 32) new_sch.slot_size = 32;
```
with
```c
    new_sch.max_value = new_ts.total_size;
    if (old_ts->packed.nfields) {
        size_t bound = 0;
        if (pkr_slot_bound(&new_ts.packed, (size_t)new_sch.max_key, &bound) != DEC_OK || bound > INT_MAX) {
            OUT("{\"error\":\"packed schema exceeds the maximum slot size\"}\n");
            for (int _i = 0; _i < n_edits; _i++) free_enum_values(&parsed[_i]);
            return 1;
        }
        new_sch.slot_size = (int)bound;
    } else {
        new_sch.slot_size = (24 + new_sch.max_key + new_sch.max_value + 7) & ~7;
        if (new_sch.slot_size < 32) new_sch.slot_size = 32;
    }
```
*Invariant check for (g)*: `pf.fail_field`/`fail_reason` are `char[128]`/`char[192]` in `EditPreflightCtx`; `packed_preflight_walk` already JSON-sanitises both.

**Why the legacy hunks stay intact.** Every legacy-only statement (`field_value_fits_new`, `edit_preflight_walk_cb`, the overlay loop, `transform_field_value`) is reached only when `!old_ts->packed.nfields`. The legacy path is exercised by the existing legacy-pinned edit tests, which remain green.

### 3.7 Build registration — `build.sh`
Anchor (once): `    src/test/cases/test_edit_field.c \` → `    src/test/cases/test_edit_field.c \` followed by a new line `    src/test/cases/test_packed_recompose.c \`.
(No production source file is added; `shard-db` and `shard-db-test-server` source lists are unchanged.)

---

### 3.8 Durability hunks (D-6)

**H15 — `rebuild_object_v2` fields.conf swap.** Anchor (once, in query_find.c):
```c
        for (int a = 0; a < n_added; a++) fprintf(fout, "%s\n", added_lines[a]);
        fclose(fin);
        fclose(fout);
        if (rename(fpath_new, fpath) != 0) {
            failure = "Failed to swap fields.conf";
            goto txn_fail;
        }
```
Replace with:
```c
        for (int a = 0; a < n_added; a++) fprintf(fout, "%s\n", added_lines[a]);
        fclose(fin);
        if (fflush(fout) != 0 || ferror(fout)) {
            fclose(fout);
            unlink(fpath_new);
            failure = "Failed to write fields.conf";
            goto txn_fail;
        }
        if (fclose(fout) != 0) {
            unlink(fpath_new);
            failure = "Failed to write fields.conf";
            goto txn_fail;
        }
        /* fsync(tmp) -> rename -> fsync(parent). prc==1 (renamed, parent sync
           failed) is a failure too: txn_fail restores fields.conf from the
           rollback copy while the transaction is still active. */
        int fprc = durability_publish_replace(fpath, fpath_new, NULL, NULL);
        if (fprc != 0) {
            if (fprc < 0) unlink(fpath_new);
            failure = "Failed to swap fields.conf";
            goto txn_fail;
        }
```

**H16 — folded into H13.** The durable swap of `rewrite_fields_conf_for_edit` is already part of H13's replacement function (its anchor does not exist until H13 is applied in Task 3). No separate hunk.

**H17 — `rebuild_txn_commit` directory sync.** Anchor (once, in objlock.c):
```c
    if (rename(txn->active, txn->done) != 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "rebuild txn: commit rename failed: %s",
                  strerror(errno));
        return -1;
    }
    return 0;
}
```
Replace with:
```c
    if (rename(txn->active, txn->done) != 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "rebuild txn: commit rename failed: %s",
                  strerror(errno));
        return -1;
    }
    /* The rename is the commit point and has happened; returning failure now
       would send callers to rebuild_txn_abort, which cannot run once `active`
       is gone. So a failed directory sync is logged, not propagated. */
    if (fsync_parent_dir(txn->done) != 0)
        LOG_ERROR(LOG_SUB_SLOTCASK, "rebuild txn: commit rename not proven durable (parent sync failed): %s",
                  strerror(errno));
    return 0;
}
```
`objlock.c` already includes `types.h`, which declares `fsync_parent_dir` and `durability_publish_replace`; `query_schema.c` and `query_find.c` include it likewise (executor: confirm with a build, no new includes expected).

Tests: no new test (power loss is not simulable). The existing 12 crash-state cases and the legacy rebuild tests must stay green; Task 5 runs the suite. Revert-proof does not apply (hardening only).


## 4. Tests

### 4.1 Rules
Every case: `test_env_start` (unique `/tmp/shard-db-test-<pid>-<n>/db`, free port) or `test_env_start_at` for the crash cases; no fixed ports/paths; no env vars;
no process globals. Create objects with `"declaration_layout":"packed"` explicit (does not depend on the default). Run via `./build/bin/shard-db-test run <name>`.

### 4.2 New file `src/test/cases/test_packed_recompose.c` (complete)

```c
/* src/test/cases/test_packed_recompose.c — R5b: add-field, edit-field and
 * vacuum compact on packed-layout objects. Each case owns its daemon. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct { TestEnv env; TestClient *tc; } Pr;

static int pr_open(Pr *p) {
    memset(p, 0, sizeof(*p));
    if (test_env_start(&p->env) != 0) return -1;
    TestClientCfg cfg = { .port = p->env.port, .io_timeout_ms = 60000 };
    p->tc = tc_connect(&cfg);
    if (!p->tc) { test_env_stop(&p->env); return -1; }
    char *r = NULL;
    tc_request(p->tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &r);
    free(r);
    return 0;
}
static void pr_close(Pr *p) {
    if (p->tc) tc_close(p->tc);
    test_env_stop(&p->env);
}
static char *pr_q(Pr *p, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static char *pr_q(Pr *p, const char *fmt, ...) {
    char req[32768];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(req, sizeof(req), fmt, ap);
    va_end(ap);
    char *resp = NULL;
    if (tc_request(p->tc, req, &resp) != 0) { free(resp); return NULL; }
    return resp;
}
/* Expect `needle` in the response; frees it. */
static void pr_expect(char *resp, const char *needle, const char *desc) {
    ASSERT_CONTAINS(resp, needle, desc);
    free(resp);
}
static void pr_refused(char *resp, const char *desc) {
    ASSERT_CONTAINS(resp, "\"error\"", desc);
    free(resp);
}
static void pr_create(Pr *p, const char *obj, const char *fields, const char *indexes) {
    char *r = pr_q(p,
        "{\"mode\":\"create-object\",\"declaration_layout\":\"packed\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":8,\"max_key\":16,\"fields\":%s,\"indexes\":%s}",
        obj, fields, indexes);
    pr_expect(r, "\"status\":\"created\"", "create packed object");
}
static void pr_ins(Pr *p, const char *obj, const char *key, const char *value) {
    char *r = pr_q(p,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"%s\",\"value\":%s}",
        obj, key, value);
    pr_expect(r, "\"status\":\"inserted\"", "insert");
}
static char *pr_get(Pr *p, const char *obj, const char *key) {
    return pr_q(p, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"%s\"}", obj, key);
}
static char *pr_conf(Pr *p, const char *obj) {
    char path[512];
    snprintf(path, sizeof(path), "%s/default/%s/fields.conf", p->env.db_root, obj);
    return tu_read_file(path);
}
static int pr_count(Pr *p, const char *obj) {
    char *r = pr_q(p, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\"}", obj);
    int n = r ? tu_parse_count(r) : -1;
    free(r);
    return n;
}
static int pr_txn_residue(Pr *p, const char *obj) {
    const char *names[] = {".rebuild_txn.preparing", ".rebuild_txn.active", ".rebuild_txn.done",
                           ".rebuild_legacy_root", "data.legacy", "fields.conf.new"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        char path[600];
        snprintf(path, sizeof(path), "%s/default/%s/%s", p->env.db_root, obj, names[i]);
        if (tu_file_exists(path)) return 1;
    }
    return 0;
}
/* get(before) must equal get(after) once `tail` (e.g. ,"f9":0) is spliced before the closing brace. */
static void pr_same_plus(const char *before, const char *after, const char *tail, const char *desc) {
    ASSERT_NOT_NULL(before, desc);
    ASSERT_NOT_NULL(after, desc);
    if (!before || !after) return;
    char exp[4096];
    size_t n = strlen(before);
    while (n && (before[n - 1] == '\n' || before[n - 1] == ' ')) n--;
    ASSERT_TRUE(n > 1 && before[n - 1] == '}', "bare object response");
    snprintf(exp, sizeof(exp), "%.*s%s}", (int)(n - 1), before, tail);
    ASSERT_CONTAINS(after, exp, desc);
}

/* ---- add-field ------------------------------------------------------ */

static int test_packed_add_field(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    /* 8 fields: adding the 9th grows the null-bitmap prefix from 2 to 3 bytes. */
    pr_create(&p, "o",
        "[\"f1:int\",\"f2:int\",\"f3:int\",\"f4:int\",\"f5:int\",\"f6:int\",\"f7:varchar:10\",\"f8:numeric:8,2\"]",
        "[\"f1\"]");
    pr_ins(&p, "o", "k1", "{\"f1\":1,\"f7\":\"hi\",\"f8\":\"1.50\"}");
    pr_ins(&p, "o", "k2", "{\"f1\":2,\"f7\":null,\"f8\":null}");   /* explicit NULLs must survive */
    char *b1 = pr_get(&p, "o", "k1"), *b2 = pr_get(&p, "o", "k2");
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"f9:int\"]}"),
              "\"status\":\"rebuilt\"", "add-field succeeds on a packed object");
    char *a1 = pr_get(&p, "o", "k1"), *a2 = pr_get(&p, "o", "k2");
    pr_same_plus(b1, a1, ",\"f9\":null", "old row k1 intact, new nullable field without default reads NULL (A-1)");
    pr_same_plus(b2, a2, ",\"f9\":null", "NULLs preserved across the bitmap resize");
    free(b1); free(b2); free(a1); free(a2);
    pr_expect(pr_q(&p, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"k3\","
                       "\"value\":{\"f1\":3,\"f9\":9}}"), "\"status\":\"inserted\"", "9-field insert works");
    char *c = pr_conf(&p, "o");
    ASSERT_TRUE(c && strstr(c, "f9:int"), "fields.conf has the appended line");
    free(c);
    ASSERT_EQ_INT(pr_count(&p, "o"), 3, "record count");
    pr_expect(pr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\","
                       "\"criteria\":[{\"field\":\"f1\",\"op\":\"eq\",\"value\":\"2\"}]}"),
              "k2", "existing index still answers after add-field");
    ASSERT_TRUE(!pr_txn_residue(&p, "o"), "no transaction residue");
    char sc[512];
    snprintf(sc, sizeof(sc), "%s/schema.conf", p.env.db_root);
    char *conf = tu_read_file(sc);
    ASSERT_TRUE(conf && strstr(conf, "layout=packed"), "layout token preserved");
    free(conf);
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_add_field_grammar(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"a:int\"]", "[]");
    pr_ins(&p, "o", "k1", "{\"a\":1}");
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\","
                       "\"fields\":[\"note:varchar\",\"amt:numeric\",\"price:currency\",\"n:numeric:10,3\",\"w:numeric:12\"]}"),
              "\"status\":\"rebuilt\"", "bare varchar/numeric, currency, numeric(10,3), numeric:12 (scale 0)");
    char *g = pr_get(&p, "o", "k1");
    ASSERT_TRUE(g && strstr(g, "\"note\":null") && strstr(g, "\"amt\":null") && strstr(g, "\"w\":null"),
                "default-less nullable bare varchar/numerics read NULL on old rows (A-1)");
    free(g);
    /* A-1: a declared default backfills old rows; NOT NULL without a default is refused. */
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"lvl:int:default=7\"]}"),
              "\"status\":\"rebuilt\"", "add-field with a default");
    g = pr_get(&p, "o", "k1");
    ASSERT_TRUE(g && strstr(g, "\"lvl\":7"), "old row backfilled with the declared default");
    free(g);
    char *pre_nn = pr_conf(&p, "o");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"nn:int:not_null\"]}"),
               "NOT NULL without a default is refused when rows exist");
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"nn:int:not_null\"]}"),
              "NOT NULL and has no default", "refusal names the cause");
    char *post_nn = pr_conf(&p, "o");
    ASSERT_TRUE(pre_nn && post_nn && strcmp(pre_nn, post_nn) == 0, "fields.conf unchanged after the refusal");
    free(pre_nn); free(post_nn);
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"nn2:int:default=3:not_null\"]}"),
              "\"status\":\"rebuilt\"", "NOT NULL with a default is accepted");
    char *before = pr_conf(&p, "o");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"x:varchar:0\"]}"), "varchar:0 refused");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"x:numeric:0\"]}"), "numeric:0 refused");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"x:varchar:4194305\"]}"), "varchar:4194305 refused");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"a:int\"]}"), "duplicate refused");
    char *after = pr_conf(&p, "o");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0, "refusals leave fields.conf untouched");
    free(before); free(after);
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_add_field_defaults(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"a:int\"]", "[]");
    char key[16], val[64];
    for (int i = 0; i < 40; i++) {
        snprintf(key, sizeof(key), "k%02d", i);
        snprintf(val, sizeof(val), "{\"a\":%d}", i);
        pr_ins(&p, "o", key, val);
    }
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":["
        "\"lit:int:default=7\",\"flag:bool:default=true\",\"txt:varchar:10:default=hey\","
        "\"rid:long:default=seq(rid)\",\"tid:varchar:36:default=uuid()\",\"u:uuid:default=uuid()\","
        "\"nonce:varchar:8:default=random(4)\",\"ca:timestamp:auto_create\"]}"),
        "\"status\":\"rebuilt\"", "add-field with every default kind");
    /* literal */
    char *g = pr_get(&p, "o", "k00");
    ASSERT_TRUE(g && strstr(g, "\"lit\":7") && strstr(g, "\"txt\":\"hey\""), "literal defaults stamped");
    free(g);
    /* seq: 40 distinct values 1..40, contiguous; next insert resumes at 41 */
    long sum = 0;
    for (int i = 0; i < 40; i++) {
        snprintf(key, sizeof(key), "k%02d", i);
        g = pr_get(&p, "o", key);
        const char *s = g ? strstr(g, "\"rid\":") : NULL;
        ASSERT_TRUE(s != NULL, "rid present");
        if (s) sum += atol(s + 6);
        free(g);
    }
    ASSERT_EQ_INT(sum, 40L * 41L / 2L, "seq values are exactly 1..40 (order = shard scan order, documented)");
    pr_expect(pr_q(&p, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"new\",\"value\":{\"a\":99}}"),
              "\"status\":\"inserted\"", "insert after backfill");
    g = pr_get(&p, "o", "new");
    ASSERT_TRUE(g && strstr(g, "\"rid\":41"), "sequence resumes at start+live_count");
    free(g);
    /* uuid / random uniqueness */
    char *g0 = pr_get(&p, "o", "k00"), *g1 = pr_get(&p, "o", "k01");
    const char *t0 = g0 ? strstr(g0, "\"tid\":\"") : NULL, *t1 = g1 ? strstr(g1, "\"tid\":\"") : NULL;
    ASSERT_TRUE(t0 && t1 && strncmp(t0, t1, 7 + 36) != 0 && t0[7 + 8] == '-', "uuid() differs per row, dashed form");
    const char *n0 = g0 ? strstr(g0, "\"nonce\":\"") : NULL, *n1 = g1 ? strstr(g1, "\"nonce\":\"") : NULL;
    ASSERT_TRUE(n0 && n1 && n0[9 + 8] == '"' && strncmp(n0, n1, 9 + 8) != 0, "random(4) = 8 hex chars, differs per row");
    free(g0); free(g1);
    /* A-3 refusals: nothing changes */
    char *before = pr_conf(&p, "o");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"r:varchar:8:default=random(5)\"]}"), "random(5) into varchar:8 refused");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"r:int:default=random(2)\"]}"), "random into int refused");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"r:varchar:20:default=uuid()\"]}"), "uuid() into varchar:20 refused");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"r:int:default=abc\"]}"), "unstorable literal refused");
    char *after = pr_conf(&p, "o");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0, "refusals leave fields.conf untouched");
    ASSERT_TRUE(!pr_txn_residue(&p, "o"), "no transaction residue");
    free(before); free(after);
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_add_field_atomic(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"a:int\"]", "[\"a\"]");
    char key[16], val[64];
    for (int i = 0; i < 300; i++) {
        snprintf(key, sizeof(key), "k%03d", i);
        snprintf(val, sizeof(val), "{\"a\":%d}", i);
        pr_ins(&p, "o", key, val);
    }
    char *snap[300];
    for (int i = 0; i < 300; i++) { snprintf(key, sizeof(key), "k%03d", i); snap[i] = pr_get(&p, "o", key); }
    char *conf0 = pr_conf(&p, "o");
    /* 300 sequence values cannot fit a signed byte: the range's LAST value fails the probe
       after reservation but before any row is written. */
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"s:byte:default=seq(sq)\"]}"),
               "seq overflowing a byte refuses the whole add");
    char *conf1 = pr_conf(&p, "o");
    ASSERT_TRUE(conf0 && conf1 && strcmp(conf0, conf1) == 0, "fields.conf unchanged");
    ASSERT_EQ_INT(pr_count(&p, "o"), 300, "count unchanged");
    int same = 1;
    for (int i = 0; i < 300; i++) {
        snprintf(key, sizeof(key), "k%03d", i);
        char *now = pr_get(&p, "o", key);
        if (!now || !snap[i] || strcmp(now, snap[i]) != 0) same = 0;
        free(now); free(snap[i]);
    }
    ASSERT_TRUE(same, "every row byte-identical after the refused add");
    ASSERT_TRUE(!pr_txn_residue(&p, "o"), "transaction fully rolled back");
    pr_expect(pr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\","
                       "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"7\"}]}"), "k007", "index still consistent");
    free(conf0); free(conf1);
    /* limits: 255 fields + 1 ok, one more refused (tombstones count toward 256) */
    char fields[8192] = "[";
    size_t off = 1;
    for (int i = 0; i < 255; i++) off += (size_t)snprintf(fields + off, sizeof(fields) - off, "%s\"c%d:int\"", i ? "," : "", i);
    snprintf(fields + off, sizeof(fields) - off, "]");
    pr_create(&p, "wide", fields, "[]");
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"wide\",\"fields\":[\"c255:int\"]}"),
              "\"status\":\"rebuilt\"", "256th field accepted");
    pr_refused(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"wide\",\"fields\":[\"c256:int\"]}"),
               "257th field refused");
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- edit-field ----------------------------------------------------- */

static int test_packed_edit_field_varchar(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"name:varchar:10\",\"b:varchar\"]", "[\"name\"]");
    pr_ins(&p, "o", "k1", "{\"name\":\"abcdefghij\",\"b\":\"x\"}");
    pr_ins(&p, "o", "k2", "{\"name\":\"zz\",\"b\":null}");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"name:varchar:40\"]}"),
              "\"rebuilt\":true", "grow rebuilds");
    char *g = pr_get(&p, "o", "k1");
    ASSERT_TRUE(g && strstr(g, "abcdefghij"), "value intact after grow");
    free(g);
    char *before = pr_conf(&p, "o");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"name:varchar:5\"]}"),
              "Pre-flight failed on field [name]", "shrink below an existing value refused by the pre-flight");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"dry_run\":true,\"fields\":[\"name:varchar:5\"]}"),
              "Pre-flight failed on field [name]", "dry_run predicts the same refusal");
    char *after = pr_conf(&p, "o");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0, "refused shrink changed nothing");
    free(before); free(after);
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"name:varchar:10\"]}"),
              "\"rebuilt\":true", "shrink to exactly the longest value is allowed");
    /* bare <-> bounded, every direction */
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"name:varchar\"]}"),
              "\"status\":\"edited\"", "bounded -> bare");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"b:varchar:3\"]}"),
              "\"status\":\"edited\"", "bare -> bounded when all rows fit");
    pr_refused(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"name:varchar:3\"]}"),
               "bare -> too-small bounded refused");
    ASSERT_EQ_INT(pr_count(&p, "o"), 2, "count");
    pr_expect(pr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\","
                       "\"criteria\":[{\"field\":\"name\",\"op\":\"eq\",\"value\":\"zz\"}]}"), "k2", "varchar index rebuilt and consistent");
    /* metadata-only: default change, no rebuild */
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"b:varchar:3:default=q\"]}"),
              "\"rebuilt\":false", "default-only edit is metadata only");
    ASSERT_TRUE(!pr_txn_residue(&p, "o"), "no residue");
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_edit_field_numeric(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    const char *rows[][2] = { {"k1", "1.0050"}, {"k2", "-1.0050"}, {"k3", "2.5000"}, {"k4", "99999999.9999"} };
    pr_create(&p, "o", "[\"a:numeric:12,4\"]", "[\"a\"]");
    pr_create(&p, "ov", "[\"a:numeric:12,4\"]", "[\"a\"]");
    for (int i = 0; i < 4; i++) {
        char v[64];
        snprintf(v, sizeof(v), "{\"a\":\"%s\"}", rows[i][1]);
        pr_ins(&p, "o", rows[i][0], v);
        pr_ins(&p, "ov", rows[i][0], v);
    }
    /* D-1: scale reduction rounds half away from zero; the integer part still fits (9 digits <= 10) */
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"a:numeric:12,2\"]}"),
              "\"rebuilt\":true", "numeric(12,4) -> numeric(12,2)");
    char *g;
    g = pr_get(&p, "o", "k1"); ASSERT_TRUE(g && strstr(g, "1.01") && !strstr(g, "1.0050"), "1.0050 rounds to 1.01"); free(g);
    g = pr_get(&p, "o", "k2"); ASSERT_TRUE(g && strstr(g, "-1.01"), "-1.0050 rounds to -1.01 (ties away from zero)"); free(g);
    g = pr_get(&p, "o", "k3"); ASSERT_TRUE(g && strstr(g, "2.50"), "2.5000 -> 2.50"); free(g);
    pr_expect(pr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\","
                       "\"criteria\":[{\"field\":\"a\",\"op\":\"eq\",\"value\":\"1.01\"}]}"), "k1", "index rebuilt: eq on the rounded value");
    /* all-or-nothing: 99999999.9999 rounds to 100000000.00 = 9 integer digits > 10-2 */
    char *conf0 = pr_conf(&p, "ov");
    char *dry = pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ov\",\"dry_run\":true,\"fields\":[\"a:numeric:10,2\"]}");
    pr_expect(dry, "Pre-flight failed on field [a]", "dry_run predicts the overflow refusal");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ov\",\"fields\":[\"a:numeric:10,2\"]}"),
              "Pre-flight failed on field [a]", "overflow after rounding refused by the pre-flight");
    char *conf1 = pr_conf(&p, "ov");
    ASSERT_TRUE(conf0 && conf1 && strcmp(conf0, conf1) == 0, "fields.conf unchanged");
    free(conf0); free(conf1);
    for (int i = 0; i < 4; i++) {
        g = pr_get(&p, "ov", rows[i][0]);
        ASSERT_TRUE(g && strstr(g, rows[i][1]), "every row (including the ones that would have fit) is unchanged");
        free(g);
    }
    /* bare <-> bounded and currency */
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ov\",\"fields\":[\"a:numeric\"]}"),
              "\"status\":\"edited\"", "bounded -> bare");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"ov\",\"fields\":[\"a:currency\"]}"),
              "\"status\":\"edited\"", "bare -> currency (numeric(19,4))");
    ASSERT_EQ_INT(pr_count(&p, "ov"), 4, "count");
    ASSERT_TRUE(!pr_txn_residue(&p, "ov") && !pr_txn_residue(&p, "o"), "no residue");
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_edit_field_fixed(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    char enums[4096] = "[\"i:int\",\"s:short\",\"f:float\",\"e:enum(a,b,c)\",\"big:enum(";
    size_t off = strlen(enums);
    for (int i = 0; i < 256; i++) off += (size_t)snprintf(enums + off, sizeof(enums) - off, "%sv%d", i ? "," : "", i);
    snprintf(enums + off, sizeof(enums) - off, ")\"]");
    pr_create(&p, "o", enums, "[\"i\"]");
    pr_ins(&p, "o", "k1", "{\"i\":-200,\"s\":300,\"f\":3.25,\"e\":\"c\",\"big\":\"v255\"}");
    pr_ins(&p, "o", "k2", "{\"i\":70000,\"s\":null,\"f\":null,\"e\":null,\"big\":null}");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"i:long\",\"f:double\"]}"),
              "\"rebuilt\":true", "int->long, float->double");
    char *g = pr_get(&p, "o", "k1");
    ASSERT_TRUE(g && strstr(g, "\"i\":-200") && strstr(g, "3.25"), "negatives sign-extend; float widens exactly");
    free(g);
    pr_refused(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"i:short\"]}"),
               "narrow refused: 70000 does not fit a short");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"s:int\"]}"),
              "\"rebuilt\":true", "short->int");
    /* enum: append within width = metadata only; rename needs allow_rename */
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"e:enum(a,b,c,d)\"]}"),
              "\"rebuilt\":false", "enum append (same width) is metadata only");
    pr_refused(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"e:enum(x,b,c,d)\"]}"),
               "rename without allow_rename refused");
    /* enum 256 -> 257 values: 1B -> 2B widen rebuild */
    char wide[4096] = "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"big:enum(";
    off = strlen(wide);
    for (int i = 0; i < 257; i++) off += (size_t)snprintf(wide + off, sizeof(wide) - off, "%sv%d", i ? "," : "", i);
    snprintf(wide + off, sizeof(wide) - off, ")\"]}");
    pr_expect(pr_q(&p, "%s", wide), "\"rebuilt\":true", "enum 1B -> 2B widen rebuilds");
    g = pr_get(&p, "o", "k1");
    ASSERT_TRUE(g && strstr(g, "\"big\":\"v255\"") && strstr(g, "\"e\":\"c\""), "enum indexes preserved through the widen");
    free(g);
    g = pr_get(&p, "o", "k2");
    ASSERT_TRUE(g != NULL, "NULL row still readable");
    free(g);
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Bitmap indexes address data by kf slot; a rebuild renumbers slots. Prove they stay correct.
   A failure here is a PRE-EXISTING engine bug exposed by R5b (the legacy path is identical): report it, do not paper over it. */
static int pr_cnt(Pr *p, const char *field, const char *value) {
    char *r = pr_q(p, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"o\",\"criteria\":[{\"field\":\"%s\",\"op\":\"eq\",\"value\":\"%s\"}]}", field, value);
    int n = r ? tu_parse_count(r) : -2;
    free(r);
    return n;
}
static int test_packed_bitmap_after_rebuild(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"flag:bool\",\"c:enum(red,green,blue)\",\"a:int\"]", "[\"flag\",\"c\"]");
    char key[16], val[96];
    for (int i = 0; i < 30; i++) {
        snprintf(key, sizeof(key), "k%02d", i);
        snprintf(val, sizeof(val), "{\"flag\":%s,\"c\":\"%s\",\"a\":%d}", i % 3 ? "false" : "true", i % 2 ? "green" : "blue", i);
        pr_ins(&p, "o", key, val);
    }
    for (int i = 0; i < 10; i++) {
        snprintf(key, sizeof(key), "k%02d", i);
        pr_expect(pr_q(&p, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"%s\"}", key), "deleted", "delete");
    }
    int base_flag = pr_cnt(&p, "flag", "true"), base_c = pr_cnt(&p, "c", "blue");
    const char *ops[] = {
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"z:int\"]}",
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"o\",\"compact\":true}",
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"a:long\"]}" };
    for (int o = 0; o < 3; o++) {
        pr_expect(pr_q(&p, "%s", ops[o]), "\"status\"", "recompose");
        ASSERT_EQ_INT(pr_cnt(&p, "flag", "true"), base_flag, "bool bitmap count stable after recompose");
        ASSERT_EQ_INT(pr_cnt(&p, "c", "blue"), base_c, "enum bitmap count stable after recompose");
    }
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Multi-field edit (n_edits > 1), tombstone in the middle, numeric NULL, allow_rename success,
   add-field with a :not_null field on an object that already has a tombstone. */
static int test_packed_edit_field_tombstone_multi(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"a:int\",\"gone:int\",\"n:numeric:10,4\",\"s:varchar:10\",\"e:enum(x,y)\"]", "[]");
    pr_ins(&p, "o", "k1", "{\"a\":1,\"gone\":5,\"n\":\"1.2345\",\"s\":\"hi\",\"e\":\"y\"}");
    pr_ins(&p, "o", "k2", "{\"a\":2,\"n\":null,\"s\":null,\"e\":null}");
    pr_expect(pr_q(&p, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"gone\"]}"), "\"status\":\"removed\"", "tombstone");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"allow_rename\":true,\"fields\":["
                       "\"a:long\",\"n:numeric:10,2\",\"s:varchar:40\",\"e:enum(x,z)\"]}"),
              "\"rebuilt\":true", "multi-field edit incl. rename");
    char *g = pr_get(&p, "o", "k1");
    ASSERT_TRUE(g && strstr(g, "1.23") && strstr(g, "\"e\":\"z\"") && !strstr(g, "\"gone\""), "values converted, rename relabels, tombstone stays hidden");
    free(g);
    g = pr_get(&p, "o", "k2");
    ASSERT_TRUE(g && !strstr(g, "\"error\""), "NULL numeric row readable");
    free(g);
    pr_refused(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"gone:int\"]}"), "tombstoned field cannot be edited");
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"nn:int:not_null\",\"t:varchar:5\"]}"),
              "\"status\":\"rebuilt\"", "add-field with a :not_null field next to a tombstone");
    char *c = pr_conf(&p, "o");
    ASSERT_TRUE(c && strstr(c, "gone:int:removed") && strstr(c, "nn:int:not_null"), "tombstone line kept, new line appended");
    free(c);
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_packed_edit_field_gates(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    pr_create(&p, "o", "[\"a:int\",\"b:varchar:10\"]", "[]");
    pr_ins(&p, "o", "k1", "{\"a\":1,\"b\":\"x\"}");
    pr_ins(&p, "o", "k2", "{\"a\":2,\"b\":null}");
    /* packed-aware NOT NULL gate */
    pr_refused(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"b:varchar:10:not_null\"]}"),
               "adding :not_null over a NULL row is refused");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"a:int:not_null\"]}"),
              "\"status\":\"edited\"", "adding :not_null when no NULL exists succeeds");
    char *c = pr_conf(&p, "o");
    ASSERT_TRUE(c && strstr(c, "a:int:not_null"), "fields.conf carries the constraint");
    free(c);
    /* dry_run: reports, changes nothing */
    char *before = pr_conf(&p, "o");
    pr_expect(pr_q(&p, "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"o\",\"dry_run\":true,\"fields\":[\"b:varchar:50\"]}"),
              "\"dry_run\":true", "dry_run success");
    char *after = pr_conf(&p, "o");
    ASSERT_TRUE(before && after && strcmp(before, after) == 0, "dry_run wrote nothing");
    free(before); free(after);
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* ---- vacuum compact ------------------------------------------------- */

static int test_packed_compact(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    /* 9 fields -> 8 after compact: the null-bitmap prefix shrinks 3 -> 2 bytes */
    pr_create(&p, "o",
        "[\"a:int\",\"b:varchar:20\",\"c:numeric:10,2\",\"d:bool\",\"e:int\",\"f:int\",\"g:int\",\"h:int\",\"i:int\"]",
        "[\"b\"]");
    char key[16], val[128];
    for (int i = 0; i < 6; i++) {
        snprintf(key, sizeof(key), "k%d", i);
        snprintf(val, sizeof(val), "{\"a\":%d,\"b\":\"v%d\",\"c\":\"1.50\",\"d\":true,\"e\":%d}", i, i, i);
        pr_ins(&p, "o", key, val);
    }
    pr_ins(&p, "o", "kn", "{\"a\":9,\"b\":null}");
    char *r0 = pr_q(&p, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"o\",\"compact\":true}");
    ASSERT_CONTAINS(r0, "\"status\":\"rebuilt\"", "compact with no tombstones still rebuilds");
    const char *s0 = r0 ? strstr(r0, "\"slot_size\":") : NULL;
    int slot_before = s0 ? atoi(s0 + 12) : -1;
    free(r0);
    pr_expect(pr_q(&p, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"k2\"}"), "deleted", "delete k2");
    pr_expect(pr_q(&p, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"o\",\"key\":\"k3\"}"), "deleted", "delete k3");
    char *orph = pr_q(&p, "{\"mode\":\"orphaned\",\"dir\":\"default\",\"object\":\"o\"}");
    ASSERT_TRUE(orph && atoi(orph) >= 2, "two deleted records pending");
    free(orph);
    pr_expect(pr_q(&p, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"c\"]}"),
              "\"status\":\"removed\"", "tombstone c");
    char *r1 = pr_q(&p, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"o\",\"compact\":true}");
    ASSERT_CONTAINS(r1, "\"compact\":true", "compact reports compact");
    const char *s1 = r1 ? strstr(r1, "\"slot_size\":") : NULL;
    ASSERT_TRUE(s1 && atoi(s1 + 12) < slot_before, "slot_size shrinks when a field is dropped");
    free(r1);
    char *conf = pr_conf(&p, "o");
    ASSERT_TRUE(conf && !strstr(conf, ":removed") && !strstr(conf, "c:numeric"), "tombstoned line dropped from fields.conf");
    free(conf);
    orph = pr_q(&p, "{\"mode\":\"orphaned\",\"dir\":\"default\",\"object\":\"o\"}");
    ASSERT_EQ_INT(orph ? atoi(orph) : -1, 0, "deleted-record space reclaimed");
    free(orph);
    ASSERT_EQ_INT(pr_count(&p, "o"), 5, "live rows preserved");
    char *g = pr_get(&p, "o", "k4");
    ASSERT_TRUE(g && strstr(g, "\"b\":\"v4\"") && strstr(g, "\"e\":4") && !strstr(g, "\"c\""), "row intact, dropped field gone");
    free(g);
    g = pr_get(&p, "o", "kn");
    ASSERT_TRUE(g != NULL && !strstr(g, "\"error\""), "NULL row survives the bitmap shrink");
    free(g);
    pr_expect(pr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\","
                       "\"criteria\":[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"v4\"}]}"), "k4", "index preserved and consistent");
    pr_expect(pr_q(&p, "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"o\",\"fields\":[\"c:int\"]}"),
              "\"status\":\"rebuilt\"", "a name freed by compact can be re-added");
    /* compact + splits in one pass */
    pr_expect(pr_q(&p, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"o\",\"compact\":true,\"splits\":16}"),
              "\"splits\":16", "compact + splits");
    char sc[512];
    snprintf(sc, sizeof(sc), "%s/schema.conf", p.env.db_root);
    char *sconf = tu_read_file(sc);
    ASSERT_TRUE(sconf && strstr(sconf, "default:o:16:") && strstr(sconf, "layout=packed"), "splits changed, layout token preserved");
    free(sconf);
    ASSERT_EQ_INT(pr_count(&p, "o"), 5, "count after compact+splits");
    pr_expect(pr_q(&p, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"o\","
                       "\"criteria\":[{\"field\":\"b\",\"op\":\"eq\",\"value\":\"v1\"}]}"), "k1", "index consistent after reshard");
    ASSERT_TRUE(!pr_txn_residue(&p, "o"), "no residue");
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* A-4 regression: a LIVE field whose default literal contains ":removed" mid-line
   must survive compaction on both layouts. */
static int test_compact_removed_literal(void) {
    Pr p;
    if (pr_open(&p) != 0) return 1;
    const char *layouts[] = { "packed", "legacy" };
    for (int L = 0; L < 2; L++) {
        char obj[16];
        snprintf(obj, sizeof(obj), "o_%s", layouts[L]);
        char *r = pr_q(&p,
            "{\"mode\":\"create-object\",\"declaration_layout\":\"%s\",\"dir\":\"default\",\"object\":\"%s\","
            "\"splits\":8,\"max_key\":16,\"fields\":[\"note:varchar:20:default=a:removed:x\",\"gone:int\"]}",
            layouts[L], obj);
        pr_expect(r, "\"status\":\"created\"", "create");
        pr_ins(&p, obj, "k1", "{\"note\":\"hello\",\"gone\":1}");
        pr_expect(pr_q(&p, "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"%s\",\"fields\":[\"gone\"]}", obj),
                  "\"status\":\"removed\"", "tombstone gone");
        pr_expect(pr_q(&p, "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"%s\",\"compact\":true}", obj),
                  "\"status\":\"rebuilt\"", "compact");
        char *conf = pr_conf(&p, obj);
        ASSERT_TRUE(conf && strstr(conf, "note:varchar:20:default=a:removed:x") && !strstr(conf, "gone:"),
                    "live line kept verbatim, tombstoned line dropped");
        free(conf);
        char *g = pr_get(&p, obj, "k1");
        ASSERT_TRUE(g && strstr(g, "\"note\":\"hello\""), "row readable");
        free(g);
    }
    pr_close(&p);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-packed-add-field", test_packed_add_field)
TEST_REGISTER("test-packed-add-field-grammar", test_packed_add_field_grammar)
TEST_REGISTER("test-packed-add-field-defaults", test_packed_add_field_defaults)
TEST_REGISTER("test-packed-add-field-atomic", test_packed_add_field_atomic)
TEST_REGISTER("test-packed-edit-field-varchar", test_packed_edit_field_varchar)
TEST_REGISTER("test-packed-edit-field-numeric", test_packed_edit_field_numeric)
TEST_REGISTER("test-packed-edit-field-fixed", test_packed_edit_field_fixed)
TEST_REGISTER("test-packed-edit-field-gates", test_packed_edit_field_gates)
TEST_REGISTER("test-packed-bitmap-after-rebuild", test_packed_bitmap_after_rebuild)
TEST_REGISTER("test-packed-edit-field-tombstone-multi", test_packed_edit_field_tombstone_multi)
TEST_REGISTER("test-packed-compact", test_packed_compact)
TEST_REGISTER("test-compact-removed-literal", test_compact_removed_literal)
```

Notes for the executor on the test file: (i) exact response spellings (`"deleted"`, the `get` rendering of numerics/floats/NULLs, the `orphaned`
response being a bare integer) must be confirmed against one manual request each in the evidence file; if a spelling differs, adjust the *needle*,
never the assertion's intent. (ii) `max_key` is 16 so `k%03d` keys fit. (iii) the `legacy` create in `test-compact-removed-literal` works only on the
TEST_BUILD daemon (`test_env_start` uses it); that is the same seam the R5 pins use. (iv) the 300-row seq case relies on a `byte` not being able to hold `300`; the probe uses the last value of the reserved range.

### 4.3 Flip the refusal test — `src/test/cases/test_layout_manifest.c`
Replace the three refusal assertions in `test_packed_recompose_ops_refused` (anchors: the three `ASSERT_TRUE(lm_expect_error(tc, ... "not supported on packed"), "... refused");` statements, quoted exactly in the file at the strings
`"add-field refused"`, `"edit-field refused"`, `"compact refused"`) with success assertions and rename the case:

```c
    {
        char *a = NULL, *e = NULL, *c = NULL;
        tc_request(tc, "{\"mode\":\"add-field\",\"dir\":\"lay\",\"object\":\"p\",\"fields\":[\"extra:int\"]}", &a);
        ASSERT_TRUE(a && strstr(a, "\"status\":\"rebuilt\""), "add-field succeeds on a packed object");
        tc_request(tc, "{\"mode\":\"edit-field\",\"dir\":\"lay\",\"object\":\"p\",\"fields\":[\"name:varchar:30\"]}", &e);
        ASSERT_TRUE(e && strstr(e, "\"status\":\"edited\""), "edit-field succeeds on a packed object");
        tc_request(tc, "{\"mode\":\"vacuum\",\"dir\":\"lay\",\"object\":\"p\",\"compact\":true}", &c);
        ASSERT_TRUE(c && strstr(c, "\"status\":\"rebuilt\""), "compact succeeds on a packed object");
        free(a); free(e); free(c);
    }
```
and change the registration line `TEST_REGISTER("test-packed-recompose-ops-refused", test_packed_recompose_ops_refused)` to
`TEST_REGISTER("test-packed-recompose-ops-supported", test_packed_recompose_ops_refused)` (function name kept to keep the diff small; rename the function too if the executor prefers, then update the registration).
The rest of that case (splits change, record intact, token preserved) is kept — it now also proves the verbatim reshard still works after the three recomposes.
After the flip `lm_expect_error` has **no remaining caller** and `-Wall -Wextra` would warn (unused static): **delete the `lm_expect_error` function** (definition begins `static int lm_expect_error(TestClient *tc, const char *req, const char *needle) {`, once). Quoted old text to replace in the case body: the nine lines from `    ASSERT_TRUE(lm_expect_error(tc,` (the `"add-field refused"` one) through `        "not supported on packed"), "compact refused");`.

### 4.4 Crash-state tests — `src/test/cases/test_rebuild_txn_recovery.c`
Add before the `TEST_REGISTER` block (the helpers `append_pause_config`, `wait_for_path`, `request_count`, `request_get` already exist in this file):

```c
/* ---- R5b: packed recompose crash states (S1,S2,S3,S5) ---- */

static int create_packed_fixture(TestEnv *env, const char *object, int count, const char *extra_remove) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[1024], *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp);
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"declaration_layout\":\"packed\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"title:varchar:64\",\"amt:numeric:12,4\",\"junk:int\"],\"indexes\":[\"amt\"]}", object);
    resp = NULL;
    if (tc_request(tc, req, &resp) != 0 || !SAFE_STRSTR(resp, "\"status\":\"created\"")) { free(resp); tc_close(tc); return -1; }
    free(resp);
    for (int i = 0; i < count; i++) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"item%04d\","
            "\"value\":{\"title\":\"t%d\",\"amt\":\"%d.1250\",\"junk\":%d}}", object, i, i, i, i);
        resp = NULL;
        if (tc_request(tc, req, &resp) != 0 || !SAFE_STRSTR(resp, "\"status\":\"inserted\"")) { free(resp); tc_close(tc); return -1; }
        free(resp);
    }
    if (extra_remove) {
        snprintf(req, sizeof(req),
            "{\"mode\":\"remove-field\",\"dir\":\"default\",\"object\":\"%s\",\"fields\":[\"%s\"]}", object, extra_remove);
        resp = NULL;
        if (tc_request(tc, req, &resp) != 0 || !SAFE_STRSTR(resp, "\"status\":\"removed\"")) { free(resp); tc_close(tc); return -1; }
        free(resp);
    }
    tc_close(tc);
    return 0;
}

static pid_t trigger_request_child(TestEnv *env, const char *json) {
    pid_t child = fork();
    if (child != 0) return child;
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) _exit(2);
    char *resp = NULL;
    int rc = tc_request(tc, json, &resp);
    free(resp);
    tc_close(tc);
    _exit(rc == 0 ? 0 : 3);
}

static int packed_find_amt(TestEnv *env, const char *object, const char *value, const char *want_key) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return 0;
    char req[512], *resp = NULL;
    snprintf(req, sizeof(req),
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":[{\"field\":\"amt\",\"op\":\"eq\",\"value\":\"%s\"}]}", object, value);
    int ok = tc_request(tc, req, &resp) == 0 && SAFE_STRSTR(resp, want_key);
    free(resp);
    tc_close(tc);
    return ok;
}

/* op: 0 = add-field, 1 = edit-field (rescale, indexed), 2 = vacuum compact (tombstoned field) */
static int packed_crash_case(int op, const char *phase) {
    static const char *const request[3] = {
        "{\"mode\":\"add-field\",\"dir\":\"default\",\"object\":\"%s\",\"fields\":[\"extra:int:default=7\"]}",
        "{\"mode\":\"edit-field\",\"dir\":\"default\",\"object\":\"%s\",\"fields\":[\"amt:numeric:12,2\"]}",
        "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"%s\",\"compact\":true}",
    };
    char object[32];
    snprintf(object, sizeof(object), "pk%d%.6s", op, phase);
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", env.db_root);
    ASSERT_EQ_INT(create_packed_fixture(&env, object, 40, op == 2 ? "junk" : NULL), 0, "create packed crash fixture");
    char fpath[PATH_MAX];
    snprintf(fpath, sizeof(fpath), "%s/default/%s/fields.conf", root, object);
    char *conf0 = tu_read_file(fpath);
    char *row0 = request_get(&env, object, "item0007");
    ASSERT_TRUE(conf0 && row0, "baseline captured");
    test_env_stop_keep(&env);
    ASSERT_EQ_INT(append_pause_config(root, phase), 0, "enable pause");
    ASSERT_EQ_INT(test_env_start_at(&env, root, saved_port), 0, "restart with pause hook");
    if (env.daemon_pid <= 0) { free(conf0); free(row0); return 1; }
    char req[512];
    snprintf(req, sizeof(req), request[op], object);
    pid_t child = trigger_request_child(&env, req);
    ASSERT_TRUE(child > 0, "spawn request");
    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker), "%s/default/%s/.rebuild-test-%s.active", root, object, phase);
    ASSERT_EQ_INT(wait_for_path(marker, 20000), 0, "rebuild reaches the pause");
    test_env_kill(&env);          /* unconditional: a live paused daemon would hold the DB lock */
    unlink(marker);
    if (child > 0) waitpid(child, NULL, 0);
    ASSERT_EQ_INT(test_env_start_at(&env, root, saved_port), 0, "restart recovers");
    if (env.daemon_pid > 0) {
        char *conf1 = tu_read_file(fpath);
        char *row1 = request_get(&env, object, "item0007");
        char act[PATH_MAX];
        snprintf(act, sizeof(act), "%s/default/%s/.rebuild_txn.active", root, object);
        ASSERT_EQ_INT(request_count(&env, object), 40, "all rows present");
        ASSERT_TRUE(access(act, F_OK) != 0, "no active transaction after recovery");
        if (strcmp(phase, "after-commit") != 0) {
            ASSERT_TRUE(conf1 && strcmp(conf0, conf1) == 0, "rollback restores fields.conf byte-for-byte");
            ASSERT_TRUE(row1 && strcmp(row0, row1) == 0, "rollback restores the original record");
            ASSERT_TRUE(op != 1 || packed_find_amt(&env, object, "7.1250", "item0007"), "index consistent after rollback");
        } else if (op == 0) {
            ASSERT_TRUE(conf1 && strstr(conf1, "extra:int") && SAFE_STRSTR(row1, "\"extra\":7"), "committed add-field is complete");
        } else if (op == 1) {
            ASSERT_TRUE(conf1 && strstr(conf1, "amt:numeric:12,2") && packed_find_amt(&env, object, "7.13", "item0007"),
                        "committed edit-field is complete, index rebuilt (7.1250 -> 7.13)");
        } else {
            ASSERT_TRUE(conf1 && !strstr(conf1, "junk") && SAFE_STRSTR(row1, "\"title\":\"t7\""), "committed compact is complete");
        }
        free(conf1); free(row1);
        test_env_stop(&env);
    }
    free(conf0); free(row0);
    return t_ctx->failed > 0 ? 1 : 0;
}

#define PACKED_CRASH(name, op, phase) \
    static int name(void) { return packed_crash_case(op, phase); }
PACKED_CRASH(test_pk_add_stage,    0, "after-stage")
PACKED_CRASH(test_pk_add_walk,     0, "after-walk")
PACKED_CRASH(test_pk_add_meta,     0, "after-metadata")
PACKED_CRASH(test_pk_add_commit,   0, "after-commit")
PACKED_CRASH(test_pk_edit_stage,   1, "after-stage")
PACKED_CRASH(test_pk_edit_walk,    1, "after-walk")
PACKED_CRASH(test_pk_edit_meta,    1, "after-metadata")
PACKED_CRASH(test_pk_edit_commit,  1, "after-commit")
PACKED_CRASH(test_pk_compact_stage,  2, "after-stage")
PACKED_CRASH(test_pk_compact_walk,   2, "after-walk")
PACKED_CRASH(test_pk_compact_meta,   2, "after-metadata")
PACKED_CRASH(test_pk_compact_commit, 2, "after-commit")
```
and the registrations (append to the existing `TEST_REGISTER` block):
```c
TEST_REGISTER("test-rebuild-txn-packed-add-after-stage", test_pk_add_stage)
TEST_REGISTER("test-rebuild-txn-packed-add-after-walk", test_pk_add_walk)
TEST_REGISTER("test-rebuild-txn-packed-add-after-metadata", test_pk_add_meta)
TEST_REGISTER("test-rebuild-txn-packed-add-after-commit", test_pk_add_commit)
TEST_REGISTER("test-rebuild-txn-packed-edit-after-stage", test_pk_edit_stage)
TEST_REGISTER("test-rebuild-txn-packed-edit-after-walk", test_pk_edit_walk)
TEST_REGISTER("test-rebuild-txn-packed-edit-after-metadata", test_pk_edit_meta)
TEST_REGISTER("test-rebuild-txn-packed-edit-after-commit", test_pk_edit_commit)
TEST_REGISTER("test-rebuild-txn-packed-compact-after-stage", test_pk_compact_stage)
TEST_REGISTER("test-rebuild-txn-packed-compact-after-walk", test_pk_compact_walk)
TEST_REGISTER("test-rebuild-txn-packed-compact-after-metadata", test_pk_compact_meta)
TEST_REGISTER("test-rebuild-txn-packed-compact-after-commit", test_pk_compact_commit)
```
Anchor for the insertion point: the line `TEST_REGISTER("test-rebuild-legacy-stage1-safe-restore", test_legacy_stage1)` (once) — insert the new helpers/macros **before** it and the new registrations **after** the last existing `TEST_REGISTER(...)` line of that block (`test-rebuild-txn-embedded-lock`). The file needs `#include <sys/wait.h>` if `waitpid` is not already visible (it is used by the existing cases, so it is).
Object names (`pk<op>after-`) repeat across phases of one op, which is harmless: each case owns its daemon and tmpdir, so it is safe under `--jobs 4`. The expected RED for these 12 cases is a 20 s timeout waiting for the pause marker (the request is refused before any rebuild starts), ~4 minutes in total — not the refusal text.

---

## 5. Tasks (strict order)

Evidence file: create `docs/plans/2026-11-03-r5b-evidence.md` and paste every RED / GREEN / revert-proof output into it, headed by task number.

### Task 0 — baseline and branch
1. `git status --short` → expect ` M docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md` plus the untracked `docs/plans/2026-11-03-r5b-packed-recompose-plan.md` and `docs/plans/2026-11-03-r5b-plan-review.md` (not a halt condition). `git checkout -b feat/r5b-packed-recompose` (from `fix/type-correctness-recovery`).
2. Run the inventory grep from §3.0; every hit must be in the table. Any extra caller → halt.
3. `SKIP_TESTS=1 ./build.sh` → paste the tail. Zero warnings is the baseline to hold.

### Task 1 — RED: tests first (no production change)
1. Create `src/test/cases/test_packed_recompose.c` exactly as §4.2; apply §3.7 (`build.sh`), §4.3 (flip) and §4.4 (crash tests).
2. `SKIP_TESTS=1 ./build.sh`. Fix **test** compile errors only.
3. Run each new case alone and paste the output: every case in `test_packed_recompose.c`, `test-packed-recompose-ops-supported`, and the 12 `test-rebuild-txn-packed-*`.
   Expected RED reason for all (except the 12 crash cases, which time out waiting for the pause marker): the response contains `not supported on packed-layout objects yet`
   (`test-compact-removed-literal`'s packed half fails the same way; its legacy half already exercises the A-4 bug: it must fail with the live `note` line missing
   from `fields.conf` — **paste this legacy-only RED; it is the bug-fix regression proof for Task 4/H5**). If a case fails for any *other* reason, fix the test, not production code, and re-paste.
4. Halt if any new test is GREEN on the unmodified production tree.

### Task 2 — shared machinery + add-field (D-2, D-4)
Apply, in this order: §3.1 (`types.h`), §3.2a–f, §3.3 H1–H6, §3.4 H7 (+ `rebuild_object_packed`), §3.5 H8–H9, §3.6 H10–H11, §3.8 H15 and H17 (D-6). (H11 is what lets `parse_field_line_packed` link.) H16 is part of H13 (Task 3).
`SKIP_TESTS=1 ./build.sh` — zero warnings.
GREEN targets: `test-packed-add-field`, `test-packed-add-field-grammar`, `test-packed-add-field-defaults`, `test-packed-add-field-atomic`, the add half of `test-packed-recompose-ops-supported`,
`test-rebuild-txn-packed-add-*`, and the packed half of `test-compact-removed-literal` is deferred to Task 4 (compact).
Invariant checks (paste results): (a) `git diff --stat` touches only the files named in this plan; (b) the mid-walk-failure case (`test-packed-add-field-atomic`) shows fields.conf/rows/index unchanged and `!pr_txn_residue`.
Run the legacy add-field family to prove no regression: `./build/bin/shard-db-test run-all --filter add-field --jobs 4`, then clear `/tmp/shard-db-*`.

### Task 3 — edit-field (D-1, D-3)
Apply §3.6 H12–H14 (H13 already contains the D-6 durable swap).
GREEN targets: `test-packed-edit-field-*`, the edit half of `test-packed-recompose-ops-supported`, `test-rebuild-txn-packed-edit-*`.
**Revert proofs (bug-class changes), paste both outputs each:**
1. *Packed NOT NULL gate (H12).* Temporarily delete the `if (c->ts && c->ts->packed.nfields) { … }` block in `not_null_scan_cb`; run `test-packed-edit-field-gates` → must fail at "adding :not_null when no NULL exists succeeds" (gate reports `record scan failed`). Restore; rerun → passes.
2. *Packed pre-flight (H14g).* Temporarily change the packed branch to `slotcask_walk_live(sdb, edit_preflight_walk_cb, &pf);` (the legacy walker); run `test-packed-edit-field-numeric` and `test-packed-edit-field-varchar` → the `Pre-flight failed on field [...]` needle assertions (including the `dry_run` ones) must fail: the legacy walker skips packed records, so a refusal would only come later from the rebuild walk (`Rebuild walk failed ...; original data restored`) and `dry_run` would wrongly answer `would_rebuild:true`. Restore; rerun → passes.
Run the legacy edit family: `--filter edit-field`, `--filter not-null`, `--filter enum`, `--filter long-field` with `--jobs 4`; clear `/tmp`.

### Task 4 — vacuum compact (D-5) and the tombstone-filter fix (A-4)
Already applied in Tasks 2–3 (H5, H7). Run: `test-packed-compact`, `test-compact-removed-literal` (both halves), `test-rebuild-txn-packed-compact-*`, the compact half of `test-packed-recompose-ops-supported`.
**Revert proof for A-4:** temporarily restore `strstr(line, ":removed")` in H5 and rerun `test-compact-removed-literal` → fails (legacy half and packed half); restore → passes. Paste both.
Run legacy: `--filter vacuum` and `--filter heavy-vacuum` with `--jobs 4`; clear `/tmp`.

### Task 5 — full suite and hygiene
1. `SKIP_TESTS=1 ./build.sh` zero warnings (paste).
2. `./build/bin/shard-db-test run-all --jobs 4` (fresh, once), paste the tail/summary; clear `/tmp/shard-db-*` immediately after. Baseline at `b1cc3b7` was reported (R5 hand-back; not in the repo's evidence files — re-measure in Task 0 if you need an exact comparison) as 2,402,659 passing / 0 failing across 568 cases; expected now: that plus the new cases' assertions, 0 failing. Any failure is a finding, not a flake (root-cause it; never rerun-until-green).
3. (new cases `test-packed-bitmap-after-rebuild`, `test-packed-edit-field-tombstone-multi` included in the Task 2–4 runs.) `git diff --check` clean; no `TODO`/`FIXME`/debug prints; `grep -rn "not supported on packed" src/` → only matches inside **test strings that assert the absence** (none expected) — must be empty.
4. Do **not** commit. Leave the working tree for review.

### Task 6 — documentation (same branch, uncommitted)
See §6. Anchors are quoted there; halt on a mismatch.

### Task 7 — gated: move pinned tests back (only with the human's explicit go-ahead)
See §7. Not part of the default run.

---

## 6. Documentation

### 6.1 `docs/query-protocol/schema-mutations.md`
(i) In `## add-field`, the `### Notes` list: anchor (once) `- Existing record count and hash routing are preserved.` → append after the `Sequence allocation…` bullet (anchor `- Sequence allocation is exact at walk start:`; insert the bullets after that whole bullet):
```markdown
- **Packed-layout objects (every migrated or new object).** Existing rows get the field's declared `:default=` (literal or computed) if there is one; otherwise NULL for a nullable field. A `:not_null` field with no default is refused while the object has any record (`Field 'x' is NOT NULL and has no default; existing rows cannot be backfilled`). Legacy-layout objects keep zero-fill. The new field list may use the packed grammar (`varchar` bare or `varchar:N` up to 4194304, `numeric`, `numeric:P[,S]`, `currency`). A default that cannot be stored in the field (`random(4)` into an `int`, `uuid()` into `varchar:20`, `random(N)` outside 1..256, a `seq(...)` range whose last value does not fit, an unparsable literal) is **refused before any data moves**; the failed request leaves `fields.conf`, rows and indexes unchanged (a `seq` range already reserved is not returned — the sequence just skips it).
- `seq(<name>)` backfill assigns values in **shard-scan order** (kf shard 0, 1, … and, within a shard, keyfile slot order — i.e. hash order, **not** insertion or key order). The set of values is exactly `[start, start+live_count)`.
- The number of fields (tombstoned included) is capped at 256; `vacuum compact` reclaims tombstoned slots.
```
(ii) In `## edit-field`, after the `### What changes are allowed` table, anchor (once) `**Other cross-type edits are hard-refused**` → insert before it:
```markdown
**Packed-layout objects (2026.11.1).** The same edits are supported, with these rules:

| Edit | Rule on packed objects |
|---|---|
| `varchar:N ↔ varchar:M ↔ varchar` (bare) | Every direction is allowed. Every existing row is validated first; the edit is refused (no change) if any value is longer than the new cap. |
| `numeric:P,S ↔ numeric:P2,S2 ↔ numeric ↔ currency` | Every direction is allowed. Reducing the scale **rounds half away from zero** (`1.0050 → 1.01`, `-1.0050 → -1.01`). The edit is refused (no row changes) only when a rounded value's integer part no longer fits `P−S` digits (`99999999.9999 → numeric:10,2` is refused). |
| Integer family, `float → double`, enum append/rename/1B→2B widen | As in the table above, validated per row. |

All-or-nothing: the pre-flight and `dry_run:true` run the same per-row conversion as the rebuild, so a refusal means nothing changed. An edit that changes no stored encoding (default, `:not_null`, enum append/rename within the same width) rewrites `fields.conf` only (`"rebuilt":false`).
```
(iii) `### What happens` of edit-field, item 7: anchor (once) `7. **Rebuild**: runs the same v2 rebuild path used by` → append to that item: ` On packed objects the per-field conversion is the packed converter (decimal re-encode, width change) instead of \`transform_field_value()\`.`
(iv) `## vacuum` flavors table, rows `{"mode":"vacuum","compact":true}` and `{"mode":"vacuum","compact":true,"splits":N}`: anchor (once) `| \`{"mode":"vacuum","compact":true}\` | Full rebuild. Drops tombstoned fields, shrinks \`slot_size\`. Indexes preserved. |` → replace with
`| \`{"mode":"vacuum","compact":true}\` | Full rebuild. Drops tombstoned fields, shrinks \`slot_size\`, reclaims deleted-record space (runs even when there is nothing to drop). Indexes preserved. Supported on packed-layout objects; an object whose every field is tombstoned is refused. |`

### 6.2 `docs/reference/error-codes.md`
Anchor (once, a table row): `| \`invalid field name (no :, +, /, spaces)\` | Reserved character in field name. | Use \`[a-zA-Z0-9_-]\`. |` → keep that row and append:
```markdown
| `default for field '<f>' cannot be stored: <reason>` | Packed `add-field`: a literal / `seq` / `uuid()` / `random(N)` default is not representable in the field's declared type. | Fix the default or the field type. |
| `random(N) requires 1 <= N <= 256` | `random(N)` outside 1..256 on a packed `add-field`. | Use `1 ≤ N ≤ 256`. |
| `Sequence values do not fit field: <reason>` | The reserved `seq` range's last value overflows the field. The range stays consumed. | Use a wider type. |
| `Pre-flight failed on field [<f>]: key '<k>': <reason>` | Packed `edit-field`: an existing row does not fit the new declaration (nothing changed). | Widen the declaration or fix the row. |
| `compact refused: every field is tombstoned; …` | `vacuum compact` would leave no field. | Keep or re-add a field first. |
| `Rebuild walk failed: field '<f>' key '<k>': <reason>` | Packed rebuild aborted mid-walk; original data restored. | Read the reason. |
```

### 6.3 `docs/reference/changelog.md` and `docs/release-notes/2026.11.1.md`
* changelog, anchor (once) `the authoritative object layout; a malformed token refuses the object's load.` → append a paragraph:
```markdown

**Packed recompose (R5b).** `add-field`, `edit-field` and `vacuum compact` (also with `splits`) are supported on packed-layout objects, with the same transactional rebuild, crash recovery and index handling as legacy objects. Numeric scale reduction rounds half away from zero and is refused only if the integer part no longer fits; bare↔bounded `varchar`/`numeric` edits are allowed with every existing row validated first; `seq(...)` backfill follows shard-scan order. `vacuum compact` no longer drops a live field whose `:default=` literal merely contains the text `:removed`.
```
* release note: append to `docs/release-notes/2026.11.1.md` a bullet (the file currently ends after the upgrade paragraph):
```markdown
- `add-field`, `edit-field` and `vacuum compact` work on packed-layout (migrated or new) objects. See [schema-mutations](../query-protocol/schema-mutations.md) for the packed rules (rounding on numeric scale reduction, all-or-nothing validation, `seq` backfill order).
```

### 6.4 `docs/concepts/typed-records.md`
Anchor (once) `to NULL. New fields added by add-field are never NULL — existing records` → keep the sentence; add after the paragraph (`read zeros/backfill, never NULL.` line): ` On packed objects the same rule holds: the new field reads its declared default, or NULL if it is nullable with none (a `:not_null` field with no default is refused), and explicit NULLs in other fields survive the rebuild. (Legacy-layout objects still zero-fill.)`

### 6.4b `docs/reference/limits.md`
Anchor (once): the table row starting `| \`edit-field\` scope | same-type only |` → append to its last cell: ` On packed-layout objects a numeric scale reduction rounds half away from zero (refused only if the integer part no longer fits), and bare↔bounded varchar/numeric edits are allowed.`

### 6.5 Instructions for R6 (record in the review/evidence file)
`docs/plans/2026-11-02-r5-metadata-migration-recovery-plan.md` Task 12 contains docs hunks that would *add* "not supported on packed-layout"/"R5b" text:
the changelog sentence "`add-field`, `edit-field` and `vacuum compact` are not yet supported on packed objects…", the release-note bullet beginning "- `add-field`, `edit-field` and `vacuum compact` on a migrated object are not yet supported", the three `error-codes.md` rows ("… are not supported on packed-layout objects yet"), and `schema-mutations.md` hunks (i)–(iii) (the three "> **Not supported on packed-layout objects…**" notes). **R6 must not apply these.** Also remove the R5-plan "FOLLOW-UP NOTE — R5b" once R5b is merged.

---

## 7. Tests pinned to `declaration_layout:"legacy"` in R5 — move-back table (Task 7, human-gated)

Not changed by default. Procedure per file (after Tasks 0–6 are green): remove the `"declaration_layout":"legacy"` token from that case's create requests, run the case alone; if it passes, keep it unpinned (it now covers the packed path); if it fails, **do not weaken it** — record the first failing assertion and the reason in the evidence file and re-pin.

| Candidate (pinned only because of D9) | Reason it was pinned | Expected after R5b |
|---|---|---|
| `test_edit_field.c` | edit-field refused | most scenarios run on packed; numeric-scale truncation expectations (`/ 10^n` toward zero) **change to round-half-away** — expectation edit is a product-decision, ask first |
| `test_edit_field_polish.c`, `test_edit_long_default.c`, `test_not_null_edit_gate.c`, `test_not_null_edit_modifiers.c`, `test_not_null_schema.c`, `test_long_field_def.c`, `test_enum.c` | edit-field / add-field / enum append refused | should pass unpinned |
| `test_add_field_computed_defaults.c`, `test_slotcask_v2_schema.c` | add-field refused | should pass unpinned; A-3 refusals may need new expectations |
| `test_vacuum_addfield.c`, `test_heavy_vacuum_transaction.c`, `test_vacuum_streams_mismatch.c`, `test_bitmap_kfcache_lock_order.c` (rebuild case) | compact / add-field refused | should pass unpinned |
| `test_rebuild_txn_recovery.c` `create_object_with_records` | compact/edit refused | the legacy fixture stays (it tests legacy recovery); packed coverage is the new §4.4 cases |
| `test_token_perms.c` | already unpinned (production binary); its add-field assertion pins authorization only | may be strengthened to assert success |

**Not R5b candidates** (pinned for other reasons; do not touch): `test_bulk_update_json*.c`, `test_bulk_cas.c`, `test_cas_tree_if.c`, `test_computed_update*.c`, `test_field_vs_field.c`, `test_length_ops.c`, `test_or_logic.c`, `test_regex.c`, `test_slotcask_v2_bulk.c`, `test_not_null_updates.c` (bulk-update is implemented for packed; see §0.4 item 2), `test_null_legacy_rebuild.c` (legacy trim semantics), `test_scalar_*`, `test_all_field_types.c`, `test_agg_*`, `test_varchar_overflow.c`, `test_index_*`, `test_get_fields.c`, `test_schema_export.c`, `test_request_timeout.c`, `test_alloc_error_propagation.c`, `test_composite_varchar_bound.c`, `test_r1_routes.c`, `test_layout_*`, `test_loader_*`.

---

## 8. Definition of done (R5b hand-back)

- [ ] Fresh `./build/bin/shard-db-test run-all --jobs 4` passes, output pasted (and `/tmp/shard-db-*` cleared after).
- [ ] All new cases pass **and** each has a pasted RED from Task 1; the three revert proofs (Task 3 ×2, Task 4 ×1) are pasted with both outputs.
- [ ] Zero new compiler warnings (`SKIP_TESTS=1 ./build.sh` output pasted).
- [ ] `grep -rn "not supported on packed" src/ docs/ --include=*.c --include=*.h --include=*.md` finds nothing outside `docs/plans/`.
- [ ] No sanitizer or bench run (R6). Note for R6: sanitizer gate must include `test-packed-*` and `test-rebuild-txn-packed-*`.
- [ ] Docs per §6 applied; `git diff --check` clean; nothing committed.
- [ ] Blind-reviewer pass completed on the raw diff (this diff touches an on-disk-adjacent rewrite path, crash recovery and the schema cache): findings recorded and fixed.
- [ ] Hand-back message lists: the files changed, the assumptions A-1…A-5 (confirmed or changed by the human), the untestable segment-limit edge (§2.8), and the Task 7 go/no-go question.
