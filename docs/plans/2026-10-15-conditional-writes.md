# Plan: conditional writes (rev 5)

**Date:** 2026-10-15
**Status:** draft — awaiting human review (CORE-PROCESS step 1 → 2). Written
against the post-`feat/not-null-modifier` tree (2026.10.2, null bitmap
merged at c780d26). No on-disk format change, no index change, no
migration. New request/response fields are additive; existing `if`
comparison semantics intentionally change from rendered-string to typed
matching (D1), which must be called out in release notes. Release identity is flagged for
the human at review (§2 D10).

## 0. Revision history

**Rev 4 → 5:** fix all six blind-review findings: derive safe equality
keys from compiled operands (scan for floating zero), convert every
per-record bulk condition and its ownership/cleanup, reject invalid regex
compilation and propagate regex execution errors, align null-guard tests,
repair exact anchors, and place reservation prerequisites before guard
wiring with complete isolated concurrency tests. Only this plan changes.

**Rev 3 → 4:** retain the shared object lock for guarded writes and
reserve the existing per-shard writer gates across recovery, guard and
mutation. Readers bypass these gates. Add scoped inline execution to
avoid worker-pool starvation, safe borrowing by the nested single-write
transaction, and reader/writer concurrency tests. This supersedes the
exclusive-object-lock design in earlier revisions; historical review
notes below describe their original fixes, not the current lock strategy.
Only the plan changes; no implementation is approved or installed.

**Rev 2 → 3:** correct the second review's arithmetic, JSON parser,
cleanup, capture, compatibility, boundary, and execution defects. The
strict guard search is separated from lenient read counters; the ordinary
read/write paths must not acquire a new exclusive lock. See §4.3 for the
blocking cost and strict-search requirements. This revision changes the
plan only; it does not approve implementation.

## Primary goal

Applications can conditionally change one record, compute new values from
its current contents, and receive the affected record in one atomic TCP
operation. Same-object guards also prevent concurrent records from
invalidating an existence/absence decision before that write commits.

**Rev 1 → 2** (human plan review — ten findings, all addressed in this
revision; this revision edits only the plan):

1. **[P1] Guard locking could be bypassed** — the lock classification
   probed `if_match` first while execution checked `if_no_match` first,
   so `{"if_match":null,"if_no_match":[…]}` ran the guard under the
   shared lock. Fixed by resolving the guard ONCE before classification
   (null = absent, both present = rejected) and running the check at a
   single pre-dispatch site; the write branches no longer touch guards
   at all (§4.3, Task 5).
2. **[P1] Guard errors could authorize writes** — the count refactor
   preserved count's historical failure-as-zero paths (e.g. failed
   trigram candidate build), which authorizes `if_no_match`. Fixed with
   a failure-reason enum: every unproven search (candidate producer
   failure, budget cap, object not open, timeout, scanner shortfall)
   propagates a distinct reason; `cmd_count_with_tree` maps each reason
   to its exact historical wire output (byte-identical count behavior),
   while the guard treats every non-OK reason as fail-closed
   (§4.3, Task 5).
3. **[P1] Oversized-key OOB read in the guard's self-fetch** — the
   sketch skipped the copy for oversized keys but still passed the
   undersized buffer with the oversized length. Fixed: the key is
   normalized, then length-validated against `max_key` BEFORE any
   buffer use; oversized/absent keys skip the self-fetch (the write
   path rejects those keys on its own) (Task 5).
4. **[P1] The computed-value parser rejected valid operations** — the
   hand-rolled span walk left the pointer on the op name's closing
   quote and never validated exact-one-member. Rewritten on
   `json_parse_object`/`json_obj_strdup_raw` with an explicit
   exactly-one-of-$incr/$copy rule and an unknown-member rejection
   (Task 3).
5. **[P1] Computed arithmetic corrections** — numeric literals now
   scale their integer part by `numeric_scale_mult` before the checked
   add; results are written as final BE/host bytes directly (no
   encode_field re-scaling roundtrip); integer decode uses unsigned
   shifts (the signed left-shift was UB); `strtoll`/`strtod` check
   `errno == ERANGE`; float/double results must be finite
   (Task 3).
6. **[P1] Successful deletes don't capture OLD** —
   `slotcask_delete_with_hooks` populates `result.current_value` on
   CAS failure, not on success. F4 now captures the removed record
   explicitly in `v2_delete_check_fn` (armed whenever `return_record`
   is set), with `.skip_old_read` cleared so the OLD read actually
   happens (Task 2).
7. **[P2] `if` compatibility overstated** — `parse_criteria_tree`
   returns NULL for `[]`, so the rev-1 wrapper would have turned the
   historical vacuous-pass into an error, and typed comparison changes
   results for zero-padded integer literals (`"01"` now matches 1).
   Both are now explicit decisions with regression tests: `if: []`
   keeps the vacuous pass (empty-span short-circuit in
   `cas_prepare_tree`), and typed comparison is declared the intended
   semantics (matches find) (§2 D1/D2, Task 1 test).
8. **[P2] Error text lifetime** — `cas_prepare_tree` no longer returns
   a pointer into a local buffer; it takes caller-owned
   `err_buf/err_sz` (Task 1).
9. **[P2] D8's promised bulk rejections were not implemented** —
   explicit rejection probes now exist for guards on bulk modes and
   computed spans on insert/bulk-insert/bulk-update, with tests
   (Task 3, Task 5, §2 D8).
10. **[P2] Test and instruction repairs** — the `if_match` test queried
    `domain` on an object that only has `p` (redesigned as a
    same-object sibling-exists check on `p`); the race test's `done`
    flags are `_Atomic` (TSan-clean); the count core is written out in
    full (no branch placeholders); every cross-task anchor is quoted in
    its post-earlier-task form (Tasks 1-5).

## 0.1 Scope — the four requested features

1. **F1 — atomic computed updates.** Increment fields from their current
   values, or copy a sibling field's value, evaluated under the same
   lock hold as the CAS condition check.
2. **F2 — same-object record guards.** `if_match` / `if_no_match`:
   insert/update/delete only if ≥1 / zero OTHER live records in the same
   object match criteria. The record being written is excluded.
3. **F3 — richer write conditions.** `if` accepts the full search-filter
   grammar: nested `{and:[...]}` / `{or:[...]}` trees, not just flat AND
   arrays.
4. **F4 — return affected records.** Opt-in `"return_record":true` on
   insert/update/delete: the committed record (insert/update) or the
   removed record (delete) rides in the success response.

## 0.2 Human rulings folded in (2026-10-15)

- **Version/auto-increment policy stays app-side.** A version column
  plus client-supplied bumps (already documented in cas.md) is the
  answer; the engine gains no `:version` modifier.
- **No atomic pop / ordered delete-limit.** Delete-with-condition
  already covers the need.
- **No unique constraints.** Uniqueness is handled by keying.
- **No multi-record/multi-object atomicity.** Everything stays
  per-record; the docs' staging pattern remains the answer for
  cross-record atomicity.

## 1. Background — what exists today (verified anchors)

- **CAS `if`** on insert/update/delete is a flat criteria array,
  AND-combined. Parsed by `parse_criteria_json` and evaluated by
  `cas_check` (storage.c:466), which stringifies each field
  (`typed_get_field_str`) and runs `match_criterion`. Evaluation
  happens inside the slotcask hook chain — `check_fn` runs under the
  kf-shard wrlock before `new_from_old` builds NEW — so verify+commit
  are atomic. Failure returns
  `{"error":"condition_not_met","current":{...}}`.
- **The full read-side grammar already exists and is public:**
  `parse_criteria_tree` (query_plan.c:1988, types.h:1584 — flat arrays
  parse as a single AND root; `{and:…}`/`{or:…}` nests; `[]` parses to
  NULL), `validate_criteria_tree_fields`, `compile_criteria_tree`
  (query_plan.c:1754), `criteria_match_tree` (query_plan.c:1788,
  types.h:1594 — read-only on tree and fs, typed binary matching, NULL
  truth table, composite-field fallback identical in effect to
  `cas_check`'s composite concat). Note the comparison-semantics shift:
  `cas_check` compared rendered STRINGS, the tree matcher compares
  TYPED values (§2 D1).
- **Success responses** are status+key only (storage.c insert ~:1256,
  update :2029, delete :2316). For delete, `result.current_value` is
  populated on CAS failure, NOT on success — F4 needs an explicit
  capture (§4.2).
- **`cmd_count_with_tree`** (query.c:5540) is the planner-driven
  criteria counter. It is not suitable as authorization proof. Its failure paths
  currently render as zeros on the wire (trigram-build failure → `0`);
  the guard must not inherit that leniency (§4.3).
- **The lock classification** for the dispatch is
  `int took_wrlock = mode_is_schema(mode);` (server.c:1537).
- **The 2026-08-03 `if_no_match` plan** was never implemented. This
  plan supersedes it (§4.3 explains the design difference).

## 2. Decisions

- **D1 (F3) — `if` keeps its name and gains the tree grammar.** Value
  may be a flat array (AND root — unchanged semantics) or a nested
  `{and:…}/{or:…}` object. Parsed by `parse_criteria_tree`, validated
  by `validate_criteria_tree_fields` (unknown field → the existing
  `{"error":"invalid if condition"}`; today it silently
  never-matches — intentional normalization to the read path's
  behavior, tested in Task 1), compiled once, matched read-only. A new
  `cas_check_tree` replaces `cas_check` everywhere; `cas_check` is
  deleted. `MAX_CRITERIA_DEPTH` (16) is inherited. **Comparison
  semantics shift from string to typed** (rev-2 finding 7): e.g. `eq`
  against `"01"` now matches an integer field holding 1, because the
  typed matcher parses the literal the same way `find` does. Declared
  intended; regression-tested; documented in cas.md (Task 7). Side
  effect: the eq_field family works per-record in write conditions.
- **D2 (F2) — empty criteria split.** Key-scoped `if: []` keeps
  today's vacuous-pass: `cas_prepare_tree` short-circuits an
  empty-array span (with `allow_empty=1`) to a NULL tree + empty error
  buffer, which `cas_check_tree(NULL)` evaluates as pass — exactly the
  old `ncrit == 0` behavior. Object-scope guards reject empty criteria
  (`allow_empty=0`): a guard matching nothing is meaningless and would
  silently never block.
- **D3 (F4) — `return_record` is opt-in and exact.** Default responses
  are byte-identical to today; with the flag, the response gains a
  `"record":{...}` member. Capture: insert decodes `typed_buf` (the
  auto_create transfer mutates it in place before the upsert — it IS
  the committed record); update stashes a copy of NEW at the end of
  `v2_update_new_from_old` (after computed fields and auto_update
  stamping); delete captures OLD in `v2_delete_check_fn` — the check
  hook is armed whenever `return_record` is set, `.skip_old_read` is
  cleared accordingly, and the copy is freed on every exit (rev-2
  finding 6: `result.current_value` is NOT populated on successful
  deletes). A malloc failure silently omits `"record"` — the write
  still commits. Dry-run and `condition_not_met` responses are
  unchanged.
- **D4 (F1) — computed updates are update-only, numeric-only, two ops.**
  Grammar inside the update `value` object:
  `{"<field>": {"$incr": <literal>}}` (add a signed literal to the
  current value) and `{"<field>": {"$copy": "<sibling>"}}`. Exactly one
  of `$incr`/`$copy` per object; unknown members rejected. Supported
  types: `short`, `int`, `long`, `numeric:P,S` / `currency` (int64
  math, `__builtin_*_overflow` checked — the ASan build's
  `-fno-sanitize-recover=undefined` makes an unchecked wrap a daemon
  abort), `float`, `double`. Numeric literals are scaled exactly:
  integer part plus fractional digits, padded to the field scale; digits
  beyond S are rejected; the checked result is written directly
  as BE int64 bytes (no string re-encode). float/double: IEEE add,
  `errno == ERANGE` rejected, non-finite result rejected; written as
  host-order bytes of the field's width. Integer literals via
  `strtoll` with `errno == ERANGE` rejected; results range-checked per
  width. Current value NULL (or NULL `$copy` source) → error. Every
  rejection is a whole-request error with no write (same channel as
  the varchar-overflow and `:not_null` update checks). Single-record
  `update` only (D8). `auto_update` stamping still runs after the
  computed pass.
- **D5 (F1) — condition semantics.** `check_fn` runs before
  `new_from_old`, so the `if` criteria always evaluate the
  **pre-image**. `if balance >= 50` + `{"balance":{"$incr":-50}}`
  commits atomically or not at all.
- **D6 (F2) — same-object existence guards on single writes.** The
  actual target key is excluded. `if_no_match` passes only after a complete
  strict search proves zero OTHER matches; `if_match` passes on the first
  verified OTHER match. Failure returns `condition_not_met` with `guard`
  and `matched` (0 or 1 witness indicator). Task 5's dedicated probe uses
  B-tree equality candidates when available and strict full-scan fallback
  otherwise. No exact count is needed to make the atomic decision.
- **D7 (F2) — shared object lock plus existing writer-gate reservation.**
  Resolve the guard once before dispatch: null = absent; both guards =
  rejection; bulk guards = rejection; read-mode guards are ignored.
  A guarded single write retains the normal shared object lock, pins its
  registry handle, forces scoped inline execution, and reserves every
  existing kf-shard writer gate in ascending order. Replay retained
  commit markers, evaluate the guard, then perform the single write
  without releasing the reservation. Nested gate acquisition on this
  same thread borrows the reservation. Release gates in reverse order,
  restore inline state, then release the object lock and registry pin.
  Ordinary readers bypass writer gates; ordinary writers must wait.
  Per-key `if` still evaluates inside `check_fn` after the object guard.
- **D7.1 (F2) — fail closed.** Strict indexed acquisition/validation
  failure or candidate overflow triggers a strict fallback scan. Any
  fallback failure, corrupt live record or timeout rejects the write.
  Read counters are neither refactored nor used to authorize a mutation.
- **D8 — bulk paths are out of scope for F1 and F2** (per-record bulk
  `if` DOES get F3), and the boundary is ENFORCED, not documented
  (rev-2 finding 9): bulk modes carrying `if_match`/`if_no_match` →
  `{"error":"guards are not supported on bulk operations"}`;
  computed spans on single `insert` → `{"error":"computed updates are
  only valid on update"}`; criteria-form bulk-update values return the
  same error. Bulk-insert rejects
  the record into its existing `errors` count; per-record bulk-update
  skips the invalid record under its existing error accounting. No new
  batch-wide transaction or detailed bulk-error payload is introduced.
- **D9 — computed/parse failures never write.** A computed-op failure
  under the lock → `err_buf` error and no write (the existing
  hook-failure channel). Malformed/empty guard →
  `{"error":"invalid guard criteria"}` and no write. Nothing falls
  through to a write on any error.
- **D10 — release identity (human to confirm at review).** If
  2026.10.2 has not shipped when this merges, fold in (no version
  bump, changelog under the existing Unreleased heading); otherwise
  2026.10.3. New wire fields are additive; D1 also changes existing comparison
  behavior and requires an explicit compatibility note.

## 3. Consumer inventory (everything the changed surfaces touch)

**`cas_check` removal (D1)** — declaration types.h:1596; definition
storage.c:466-495; call sites, all switched to `cas_check_tree`:
storage.c dry-run update (:1919), `v2_update_check_fn` (:1629),
`v2_delete_check_fn` (:2066), `v2_insert_check_fn` (:548 region);
query_bulk.c :3400-3401 (criteria-form bulk-update worker),
:4815 (bulk-delete criteria-form worker), :5730-5731 (per-record JSON
bulk-update worker), :5754 (per-record bulk-delete worker,
`r->old_value` form).

**`parse_criteria_json` CAS sites** — all switched to
`cas_prepare_tree`; `parse_criteria_json` itself STAYS for other read/query consumers;
query_bulk.c:4619 IS a CAS use and is converted too: storage.c insert :1159, dry-run :1914, update
:1944, delete :2214 and :2243; query_bulk.c :3794 (criteria-form
bulk-update `if`) and :5899 (criteria-form bulk-delete `if`), plus the
per-record workers' `cas_crit` locals/members enumerated below.

**`crit`/`ncrit` state holders** → `cas_tree` (+ `FieldSchema` where
absent): storage.c `V2InsertCtx`, `V2UpdateCtx` (:1962), `V2DeleteCtx`
(:2055-2056), cmd-level locals freed at :1224/1230/1242/1255,
:1920/2004/2009/2019/2023, :2224/2227/2286/2295/2300/2311;
query_bulk.c worker struct members :3341-3342 and :5696-5697, locals
:3791-3792 and :5892-5893, fan-out assignment :3937-3938, frees at
:3817/3825/3833/3859/3876/3989 and the per-record-region frees.

**`cmd_insert` / `cmd_update` / `cmd_delete` signatures (D3)** —
types.h:1139-1149; callers are ONLY the server.c dispatch (three insert calls, including
the auto-generated-key call, two update calls, and two delete calls). Verified: no calls from
embedded.c, main.c, or query_bulk.c. The CLI links no daemon source.

**Response consumers** — additive members; the CLI's minimal parser is
member-tolerant and sends no new fields. New error strings (Task 7
documents all): `invalid guard criteria`, `guards are not supported on
bulk operations`, `guard check failed`, `computed updates are only
valid on update`, and the computed-field errors listed in Task 7.

**Test-hook mechanism (Task 6)** — fixture phases 0-3 exist
(fixtures.c `test_env_test_hook_wait` range-checks `phase > 3`);
daemon kinds 0-2 in test_control.c. This plan adds kind 3
(`GUARD_CHECK`, REACHED phase 4), following the count-gap pattern:
setter `shard_db_test_set_guard_hook` defined in query.c under
`#ifdef TEST_BUILD`, declared in test_control.h, parked from
`cas_guard_request`.

**New internal matcher:** `cas_match_tree_checked` is declared in types.h,
defined in query.c and called by `cas_check_tree` and `guard_record`.
Only conditional paths use it; find/count matching remains unchanged.

**Criteria functions reused unchanged**: `parse_criteria_tree`,
`validate_criteria_tree_fields`, `compile_criteria_tree`,
`criteria_match_tree`, `free_criteria_tree`, `init_field_schema`, `typed_decode`,
`auto_key_normalize` (server.c-local; called only in server guard wiring),
`json_parse_object` / `json_obj_strdup_raw` (util.c).

## 4. Design

### 4.1 F3 — tree-backed CAS

```c
/* Parse, field-validate, and compile an `if` / guard criteria string
   against ts. Flat arrays parse as an AND root; {and:...}/{or:...}
   nests, capped by MAX_CRITERIA_DEPTH. Returns a compiled tree the
   caller frees with free_criteria_tree, or NULL:
     - NULL with err_buf empty (allow_empty=1 only) → the span was an
       empty array: VACUOUS PASS (historical `if: []` semantics);
     - NULL with err_buf non-empty → invalid; err_buf holds the reason.
   Guards pass allow_empty=0 and treat NULL as invalid. */
CriteriaNode *cas_prepare_tree(const char *json, TypedSchema *ts,
                               int allow_empty,
                               char *err_buf, size_t err_sz);

/* Evaluate a COMPILED criteria tree against a typed record. NULL tree
   = pass. Read-only on tree and fs — one compiled tree may be shared
   across bulk workers. value_len may be < ts->total_size for
   trim-encoded records. */
int cas_check_tree(FieldSchema *fs, const uint8_t *value_ptr,
                   int value_len, uint8_t hdr2, const CriteriaNode *tree);
/* Internal conditional matcher: 1 match / 0 no match / -1 evaluation error.
   Key CAS fails closed on -1; guard search propagates it as a proof error. */
int cas_match_tree_checked(const uint8_t *rec, size_t len,
                            const CriteriaNode *tree, FieldSchema *fs,
                            uint8_t hdr2);
```

`cas_prepare_tree` lives in query.c beside `cmd_count_with_tree`;
`cas_check_tree` replaces `cas_check` in storage.c. Compile happens at
parse time — never inside `cas_check_tree` — because bulk workers
share the compiled tree concurrently. Error strings are written into
caller-owned buffers (rev-2 finding 8).

### 4.2 F4 — exact record capture

- **Insert**: `typed_buf` is encoded up front, mutated in place by the
  auto_create transfer block (storage.c:1140, before the upsert), and
  passed to the hooks — on success it IS the committed record. Decode
  with `typed_decode(ts, typed_buf, (uint32_t)vlen,
  SREC_FLAG_NULLMAP)`.
- **Update**: `V2UpdateCtx` gains `uint8_t *ret_buf;` — allocated and
  filled at the very end of `v2_update_new_from_old` (after computed
  fields and `update_stamp_auto_update_fields`, when NEW is final).
  Stack-local ctx → freed on every exit of `cmd_update_v2`; only the
  success exit emits it. An abort after `new_from_old` discards the
  copy with the write.
- **Delete**: `slotcask_delete_with_hooks` populates
  `result.current_value` only on the CAS-failure path — success needs
  an explicit capture. `v2_delete_check_fn` copies OLD into
  `V2DeleteCtx.ret_buf` when `return_record` is set (first fire wins;
  the hook is armed whenever the flag is set, and `.skip_old_read` is
  cleared so the OLD read happens). The success block decodes
  `ret_buf`; every exit frees it. `durability_degraded` keeps its
  error response WITHOUT a record.

### 4.3 F2 — strict existence checks and lock cost

Guarded single writes retain the normal shared object lock. They reserve
ALL existing `SlotcaskDb.writer_gates` in ascending shard order before
recovery/search and hold them through the mutation and response dispatch.
The shared object lock prevents schema/maintenance changes; the writer
reservation prevents every ordinary, deferred-bulk and pregrow mutation
from invalidating the decision. No new mutex is added. Per-key `if`,
computed updates and returned records keep their existing scope.

Readers do not acquire writer gates. They can continue during the guard
check, including reads on the same shard. Existing kf, index and cache
rwlocks still apply: recovery and the actual commit can briefly block
reads of the affected shards, including durability I/O already performed
under kf write locks. This is reader concurrency, not a lock-free read or
snapshot guarantee. Other objects continue independently.

Compared with rev 3's exclusive object lock this avoids stopping all
object reads and avoids waiting for existing readers to finish. It does
not make predicate evaluation or durability faster. All competing writes
on this object wait, full scans can be expensive, and reserving all gates
(up to 4096) plus checking retained markers adds overhead. Guarded work
runs serially on the request thread: pool workers can be blocked on these
gates, so submitting more work to either shared pool while holding the
reservation could deadlock. No throughput claim is made without human-run
benchmarks. Server workers/query-capacity slots can still be occupied by
waiting guards.

Gate acquisition must begin with no lower-level kf/index/cache lock held.
Try gates in ascending order; on contention release the partial set before
waiting/retrying. Never block on a later gate while retaining earlier ones.
This avoids a pool dependency cycle with an older multi-shard transaction.
Acquisition is not fair; sustained write traffic can delay a guard.
The reservation is thread-local, scoped, non-nestable and tied to a pinned
registry instance. The existing transaction's gate wrapper borrows only
that exact instance's reservation; it never recursively locks a mutex.
Every cleanup path ends the reservation before unlocking the object.
Task 4 gives code and the writer-path audit.

Do not reuse lenient count results as proof. Task 5 adds a dedicated
existence probe: a B-tree equality leaf under AND supplies bounded
candidates; each candidate is strictly verified against the stored record
AFTER releasing the B-tree handle. Other predicate shapes, missing or
unreadable index shards and candidate overflow fall back to the existing
strict live-record walk. A failed fallback rejects the write. The actual
target key is excluded during matching, avoiding a self-fetch and count
subtraction. A first verified OTHER match proves existence or disproves
absence, so stop there. `matched` is 0 or 1 (a witness indicator, not an
exact cardinality).

Exact guard keys are derived from compiled operands, avoiding a second
numeric parser with different truncation rules. The initial fast path
covers varchar, signed integer/numeric/timestamp, bool/byte and finite
nonzero float/double equality. Floating zero and other type encodings
fall back to the strict scan; this preserves typed equality semantics.
Regex compilation failures reject the whole condition before scanning;
regex execution errors reject the guard rather than prove absence.

An indexed equality normally costs a descent in each index shard plus
candidate verification. It is not universally O(log n): fan-out, duplicate
clusters and post-filters matter. Other type encodings and broader/range/OR/bitmap/trigram guards
currently use a strict full scan; unsuccessful existence checks and
successful absence checks may inspect the whole object. An index on a
field alone does not guarantee a fast guard. This fallback choice is
explicit and must be documented before implementation is approved.

Timeout is checked during guard matching and between index shards.
Object-lock acquisition, writer-gate reservation retries and retained-marker recovery are
not covered by the matching deadline; this plan does not claim a hard
end-to-end timeout including lock wait, recovery or fsync. Never hold the object lock across client network input or retry
loops. The current dispatch does construct/write the response before its
tail unlock, so response generation/I/O can extend that hold; do not
promise that it ends at the storage flag flip.

The strict B-tree and hash-lookup helpers are internal additions solely
for guard proof; normal count/find/index APIs retain their code and
failure behavior. Task 5 gives their complete code and inventory.

### 4.4 F1 — computed fields under the lock

All of it happens inside `v2_update_new_from_old`, reusing the spans
`json_get_fields_null_aware` already produced (non-varchar spans pass
through raw, so a `{"$incr":-50}` value arrives intact as
`field_vals[i]`):

1. **Detect + validate** each span starting with `{` using the JSON
   parser (`json_parse_object` + `json_obj_strdup_raw`, rev-2 finding
   4): exactly one of `$incr`/`$copy`; no unknown members; target type
   supported; `$copy` source exists, is not itself computed,
   type-compatible (numeric: same `numeric_scale`). Reject the whole
   update via `err_buf` on any violation (also tightens today's silent
   encode-garbage-to-zero behavior for object values on numeric
   fields — documented in Task 7). Free the span and NULL the slot so
   `update_compose_new_from_old` carries OLD forward for that field.
2. After compose, **apply** (rev-2 finding 5): open the validated OLD
   view, and for each computed field write the FINAL bytes directly —
   integer/numeric results as BE int64 of the field's width after
   checked arithmetic; float/double as host-order bytes after an IEEE
   add with `errno == ERANGE` and finiteness checks. No
   decode→string→`encode_field` roundtrip, so no double scaling. NULL
   current or NULL `$copy` source → `err_buf` error.
3. `update_stamp_auto_update_fields` then runs unchanged, then the F4
   ret_buf copy.

Order guarantee: `check_fn` runs before `new_from_old` — conditions
see the pre-image (D5) and a passed condition + computed result commit
atomically.

**D8 enforcement points**: the existing defaults encoder rejects object
spans on insert/bulk-insert without another parse; the shared composer
rejects unprocessed numeric object spans on bulk-update; criteria-form
bulk-update rejects its shared patch before scanning. Bulk guards reject
at dispatch before taking a lock. Tests in Tasks 3-4.

### Phase 0 — documentation discovery (completed for this revision)

**How this serves the primary goal:** verify the actual parsing, record
and locking APIs before authorizing a write-path change.

Allowed APIs and evidence: `json_parse_object` returns field count
(types.h JSON contract, util.c implementation); `JsonObj.n` supplies
member cardinality. `typed_record_view` validates record generation
(config.c); `legacy_field_absent` supplies legacy NULL semantics
(storage.c). `slotcask_walk_live_strict` returns 0 complete / 1 callback
stop / -1 error (slotcask.h). `btree_search` and the existing count
helpers do not provide strict proof; the guard-only checked siblings in
Task 5 are required. `objlock.c` and server.c dispatch confirm shared
locks on ordinary reads/writes. Rev 4 retains that shared lock for guards
and reserves writer gates instead; source does not yet implement guards.

Read docs/query-protocol/cas.md, bulk.md, docs/concepts/concurrency.md
and storage-model.md before executing. No public API, SQL transaction,
unique-index format or engine-managed version is added.

## 5. Tasks (test-first, in order)

### Embedded execution rules

- Branch `feat/conditional-writes` off `main`. Leave ALL work
  uncommitted (repo standing exception).
- Build: `SKIP_TESTS=1 ./build.sh`. Tests:
  `./build/bin/shard-db-test run-all`; single case:
  `./build/bin/shard-db-test run <name>`.
- Do tasks in order. Each task: add its test file, register it in
  build.sh, run the case, **paste the failing output and confirm each
  failure is the expected behavior gap**, implement, re-run to green
  (paste output), then run the full suite. A task's tests must be green
  at that task's end.
- **If a quoted anchor isn't found exactly, write `PLAN_NOTES.md`
  describing the mismatch and halt the entire execution run
  immediately — do not guess, reinterpret, or continue to any further
  task, even an unrelated one.** Resuming requires the human (or the
  planning model, re-engaged) to read `PLAN_NOTES.md` and hand back a
  patched or fresh plan.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.
- New tests must not depend on process-wide mutable state, fixed
  ports, or execution order. The Task 6 case uses one TestClient per
  thread and `_Atomic` flags for cross-thread observation.
- Dynamic-safety gate (AGENTS.md): ASan+UBSan and TSan **once, after
  all tasks are done, immediately before handing off for push/PR**, per
  the AGENTS.md recipe (TSan `--jobs 2`, three consecutive passes
  each). Not between tasks.

---

### Task 1 — F3: tree-backed write conditions

**How this serves the primary goal:** express nested conditions in the same atomic write.

**Test** — new file `src/test/cases/test_cas_tree_if.c`, registered as
`test-cas-tree-if`. On the pre-task tree it fails at: the nested-OR
update (step 2), the unknown-field rejection (step 5 — today it
reports `condition_not_met`), the depth cap (step 6), the bulk
per-record nested `if` (step 7), the eq_field condition (step 9), the
typed-comparison regression (step 10), and the empty-`if` regression
(step 11).

```c
/* src/test/cases/test_cas_tree_if.c
 *
 * Conditional-writes F3 — nested AND/OR trees in write `if` conditions,
 * flat-array compatibility, validation, bulk per-record trees.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int test_cas_tree_if_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ti\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"status:varchar:12\",\"amount:int\","
        "\"region:varchar:8\",\"lo:long\",\"hi:long\"],"
        "\"indexes\":[\"status\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create ti");
    free(resp); resp = NULL;

    /* 1. Seed: k1 pending/3, k2 paid/9, k3 pending/50. */
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"ti\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"status\":\"pending\",\"amount\":3,"
        "\"region\":\"eu\",\"lo\":10,\"hi\":20}},"
        "{\"key\":\"k2\",\"value\":{\"status\":\"paid\",\"amount\":9,"
        "\"region\":\"us\",\"lo\":10,\"hi\":20}},"
        "{\"key\":\"k3\",\"value\":{\"status\":\"pending\",\"amount\":50,"
        "\"region\":\"eu\",\"lo\":1,\"hi\":2}}]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "seed");
    free(resp); resp = NULL;

    /* 2. Nested OR/AND `if` — k1 matches (pending AND amount<10). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k1\",\"value\":{\"status\":\"done\"},\"if\":"
        "{\"or\":[{\"and\":[{\"field\":\"status\",\"op\":\"eq\","
        "\"value\":\"pending\"},{\"field\":\"amount\",\"op\":\"lt\","
        "\"value\":10}]},{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "nested OR update applies");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"ti\","
                   "\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"done\"", "new state visible");
    free(resp); resp = NULL;

    /* 3. Same tree against k3 (pending but amount 50): no branch matches. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k3\",\"value\":{\"status\":\"done\"},\"if\":"
        "{\"or\":[{\"and\":[{\"field\":\"status\",\"op\":\"eq\","
        "\"value\":\"pending\"},{\"field\":\"amount\",\"op\":\"lt\","
        "\"value\":10}]},{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "tree blocks k3");
    ASSERT_CONTAINS(resp, "\"current\":", "current still returned");
    free(resp); resp = NULL;

    /* 4. Flat array compat — unchanged semantics. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k2\",\"value\":{\"amount\":10},\"if\":"
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"paid\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "flat array still works");
    free(resp); resp = NULL;

    /* 5. Unknown field → invalid if condition (validation, not a silent
          never-match). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k1\",\"value\":{\"amount\":4},\"if\":"
        "[{\"field\":\"nosuch\",\"op\":\"eq\",\"value\":1}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"invalid if condition\"",
                    "unknown field rejected");
    free(resp); resp = NULL;

    /* 6. Over-deep nesting (17 levels) → invalid if condition. */
    char deep[2048]; int off = snprintf(deep, sizeof(deep),
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k1\",\"value\":{\"amount\":4},\"if\":");
    for (int i = 0; i < 17; i++) off += snprintf(deep + off,
        sizeof(deep) - (size_t)off, "%s", "{\"and\":[");
    off += snprintf(deep + off, sizeof(deep) - (size_t)off,
        "{\"field\":\"amount\",\"op\":\"eq\",\"value\":4}");
    for (int i = 0; i < 17; i++) off += snprintf(deep + off,
        sizeof(deep) - (size_t)off, "%s", "]}");
    snprintf(deep + off, sizeof(deep) - (size_t)off, "}");
    tc_request(tc, deep, &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"invalid if condition\"",
                    "depth cap inherited");
    free(resp); resp = NULL;

    /* 7. Delete with a nested tree. */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k3\",\"if\":{\"and\":[{\"field\":\"status\",\"op\":\"eq\","
        "\"value\":\"pending\"},{\"field\":\"region\",\"op\":\"eq\","
        "\"value\":\"eu\"}]}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"deleted\"", "nested delete applies");
    free(resp); resp = NULL;

    /* 8. Bulk per-record `if` with trees: k2 skipped, k1 updated. */
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\","
        "\"object\":\"ti\",\"records\":["
        "{\"key\":\"k2\",\"value\":{\"region\":\"ap\"},\"if\":{\"or\":"
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]}},"
        "{\"key\":\"k1\",\"value\":{\"region\":\"ap\"},\"if\":{\"or\":"
        "[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"done\"}]}}]}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "bulk ok");
    ASSERT_CONTAINS(resp, "\"updated\":1", "one record updated");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "one record CAS-skipped");
    free(resp); resp = NULL;

    /* 9. eq_field works in a write condition. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k9\",\"value\":{\"status\":\"pending\",\"amount\":7,"
        "\"region\":\"eu\",\"lo\":7,\"hi\":7}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k9\",\"value\":{\"status\":\"cleared\"},\"if\":"
        "[{\"field\":\"lo\",\"op\":\"eq_field\",\"value\":\"hi\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "eq_field condition");
    free(resp); resp = NULL;

    /* 10. Typed comparison regression (rev-2 finding 7): a zero-padded
        integer literal now matches the typed value, same as find. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k9\",\"value\":{\"region\":\"xx\"},\"if\":"
        "[{\"field\":\"amount\",\"op\":\"eq\",\"value\":\"07\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"",
                    "typed comparison parses literals like find");
    free(resp); resp = NULL;

    /* 11. Empty `if: []` keeps the historical vacuous pass. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ti\","
        "\"key\":\"k9\",\"value\":{\"region\":\"eu\"},\"if\":[   ]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "empty if passes vacuously");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-cas-tree-if", test_cas_tree_if_run)
```

**build.sh registration** — anchor (in the `shard-db-test` source
list; Tasks 2-6 register at the same growing anchor, each immediately
after the previous task's new line):

```
    src/test/cases/test_not_null_compat.c \
```

insert after that line:

```
    src/test/cases/test_cas_tree_if.c \
```

**Implementation:**

1. **types.h** — anchor, the `cas_check` declaration:

```c
int cas_check(TypedSchema *ts, const uint8_t *value_ptr, int value_len, uint8_t hdr2,
```

replace that declaration block (through its continuation line ending
`SearchCriterion *crit, int ncrit);`) with:

```c
/* Evaluate a COMPILED criteria tree against a typed record (CAS `if`).
   The tree must come from cas_prepare_tree; read-only on tree and fs,
   so one compiled tree may be shared across bulk workers. NULL tree
   passes vacuously (historical empty-`if` semantics). value_len is the
   number of valid bytes in value_ptr (may be < ts->total_size for
   trim-encoded records). */
int cas_check_tree(FieldSchema *fs, const uint8_t *value_ptr,
                   int value_len, uint8_t hdr2, const CriteriaNode *tree);
/* Internal conditional matcher: 1 match / 0 no match / -1 evaluation error.
   Key CAS fails closed on -1; guard search propagates it as a proof error. */
int cas_match_tree_checked(const uint8_t *rec, size_t len,
                            const CriteriaNode *tree, FieldSchema *fs,
                            uint8_t hdr2);

/* Parse, field-validate, and compile an `if` / guard criteria string.
   Returns a compiled tree the caller frees with free_criteria_tree, or
   NULL: with err_buf EMPTY (allow_empty=1 only) the span was an empty
   array — VACUOUS PASS; with err_buf NON-EMPTY it is invalid and
   err_buf holds the reason (caller-owned storage — do not hold
   pointers into this function's frames). */
CriteriaNode *cas_prepare_tree(const char *json, TypedSchema *ts,
                               int allow_empty,
                               char *err_buf, size_t err_sz);
```

2. **storage.c** — anchor, the `cas_check` definition:

```c
int cas_check(TypedSchema *ts, const uint8_t *value_ptr, int value_len,
              uint8_t hdr2, SearchCriterion *crit, int ncrit) {
```

replace the entire function (through its closing `return 1;\n}`) with:

```c
int cas_check_tree(FieldSchema *fs, const uint8_t *value_ptr,
                   int value_len, uint8_t hdr2, const CriteriaNode *tree) {
    if (!tree) return 1;
    return cas_match_tree_checked(value_ptr, (size_t)value_len, tree, fs, hdr2) > 0;
}
```

3. **query.c** — insert `cas_prepare_tree` immediately BEFORE
   `static int cmd_count_with_tree(` (that exact line is the anchor):

```c
static int cas_tree_compiled(const CriteriaNode *n) {
    if (n->kind == CNODE_LEAF) {
        if (!n->compiled) return 0;
        if (n->leaf.op == OP_REGEX || n->leaf.op == OP_NOT_REGEX)
            return n->compiled->tf &&
                   n->compiled->tf->type == FT_VARCHAR &&
                   n->compiled->re_compiled;
        return 1;
    }
    for (int i = 0; i < n->n_children; i++)
        if (!cas_tree_compiled(n->children[i])) return 0;
    return 1;
}

/* 1 match, 0 no match, -1 evaluation failure. Regex errors are never
   converted to absence or to a successful negated predicate. */
int cas_match_tree_checked(const uint8_t *rec, size_t len,
                            const CriteriaNode *n, FieldSchema *fs,
                            uint8_t hdr2) {
    if (!n) return 1;
    if (n->kind != CNODE_LEAF) {
        int is_and = n->kind == CNODE_AND;
        for (int i = 0; i < n->n_children; i++) {
            int r = cas_match_tree_checked(rec, len, n->children[i], fs, hdr2);
            if (r < 0) return -1;
            if (is_and && !r) return 0;
            if (!is_and && r) return 1;
        }
        return is_and ? 1 : 0;
    }
    const CompiledCriterion *cc = n->compiled;
    if (!cc) return -1;
    if (cc->op != OP_REGEX && cc->op != OP_NOT_REGEX)
        return criteria_match_tree(rec, len, n, fs, hdr2);
    if (!cc->tf || cc->tf->type != FT_VARCHAR || !cc->re_compiled)
        return -1;
    const uint8_t *base, *bm; int bm_bytes; size_t region;
    if (typed_record_view(rec, len, hdr2, &base, &bm, &bm_bytes, &region))
        return -1;
    int idx = (int)(cc->tf - fs->ts->fields);
    if ((hdr2 & SREC_FLAG_NULLMAP) && typed_field_null(bm, bm_bytes, idx))
        return 0;
    const uint8_t *field =
        (size_t)cc->tf->offset + (size_t)cc->tf->size > region
            ? g_zero_field_65537 : base + cc->tf->offset;
    regmatch_t pm = { .rm_so = 0, .rm_eo = varchar_eff_len(field, cc->tf->size) };
    int rc = regexec(cc->re, (const char *)field + 2, 1, &pm, REG_STARTEND);
    if (rc != 0 && rc != REG_NOMATCH) return -1;
    return cc->op == OP_REGEX ? rc == 0 : rc == REG_NOMATCH;
}

CriteriaNode *cas_prepare_tree(const char *json, TypedSchema *ts,
                               int allow_empty,
                               char *err_buf, size_t err_sz) {
    if (err_buf && err_sz) err_buf[0] = '\0';
    if (!json) return NULL;
    /* Empty array = historical vacuous pass for key-scoped `if`. The
       tree parser rejects it, so catch the exact spelling first. */
    if (allow_empty) {
        const char *p = json;
        while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
        if (*p == '[') {
            p++;
            while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
            if (*p == ']') {
                p++;
                while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
                if (!*p) return NULL;
            }
        }
    }
    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(json, &perr);
    if (!tree) {
        if (err_buf && err_sz)
            snprintf(err_buf, err_sz, "%s", perr ? perr : "parse error");
        return NULL;
    }
    char verr[256];
    if (validate_criteria_tree_fields(tree, ts, verr, sizeof(verr)) < 0) {
        free_criteria_tree(tree);
        if (err_buf && err_sz) snprintf(err_buf, err_sz, "%s", verr);
        return NULL;
    }
    if (!allow_empty && tree->kind == CNODE_AND && tree->n_children == 0) {
        free_criteria_tree(tree);
        if (err_buf && err_sz) snprintf(err_buf, err_sz, "empty criteria");
        return NULL;
    }
    compile_criteria_tree(tree, ts);
    if (!cas_tree_compiled(tree)) {
        free_criteria_tree(tree);
        if (err_buf && err_sz) snprintf(err_buf, err_sz, "criteria compilation failed");
        return NULL;
    }
    return tree;
}
```

The empty-array check accepts JSON whitespace inside and outside the
brackets and requires complete consumption; no new JSON helper is exported.

4. **storage.c insert path** — anchor:

```c
    SearchCriterion *crit = NULL;
    int ncrit = 0;
    if (if_json && parse_criteria_json(if_json, &crit, &ncrit) != 0) {
        free(typed_buf);
        OUT("{\"error\":\"invalid if condition\"}\n");
        return 1;
    }
```

replace with:

```c
    FieldSchema cas_fs;
    init_field_schema(&cas_fs, db_root, object);
    char cas_err[256];
    CriteriaNode *cas_tree = NULL;
    if (if_json) {
        cas_tree = cas_prepare_tree(if_json, cas_fs.ts, 1,
                                    cas_err, sizeof(cas_err));
        if (!cas_tree && cas_err[0]) {
            LOG_WARN(LOG_SUB_SLOTCASK, "insert if rejected: %s", cas_err);
            free(typed_buf);
            OUT("{\"error\":\"invalid if condition\"}\n");
            return 1;
        }
        /* cas_tree NULL with empty cas_err = vacuous pass ([]). */
    }
```

and replace each of the four `free_criteria(crit, ncrit);` in this
function (:1224, :1230, :1242, :1255) with
`free_criteria_tree(cas_tree);`. In `V2InsertCtx` replace the members

```c
    SearchCriterion  *crit;
    int               ncrit;
```

with

```c
    CriteriaNode     *cas_tree;
    FieldSchema      *cas_fs;
```

and the initializer `.crit = crit, .ncrit = ncrit,` with
`.cas_tree = cas_tree, .cas_fs = &cas_fs,`. Replace the complete `v2_insert_check_fn` body (anchor: its definition)
with the following, removing the old `c->ncrit` guard too:

```c
static int v2_insert_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2InsertCtx *c = ctx_ptr;
    if (!c->cas_tree) return 1;
    if (!old) return 0;
    return cas_check_tree(c->cas_fs, old->value, (int)old->vlen,
                           old->hdr2, c->cas_tree);
}
```

5. **storage.c update path** — dry-run block anchor:

```c
        if (if_json) {
            SearchCriterion *crit = NULL; int ncrit = 0;
            if (parse_criteria_json(if_json, &crit, &ncrit) != 0) {
```

replace through that block's `free_criteria(crit, ncrit);` with:

```c
        if (if_json) {
            FieldSchema cas_fs;
            init_field_schema(&cas_fs, db_root, object);
            char cas_err[256];
            CriteriaNode *cas_tree = cas_prepare_tree(if_json, cas_fs.ts, 1,
                                                      cas_err, sizeof(cas_err));
            if (!cas_tree && cas_err[0]) {
                LOG_WARN(LOG_SUB_SLOTCASK, "update if rejected: %s", cas_err);
                free(old_val);
                OUT("{\"error\":\"invalid if condition\"}\n");
                return 1;
            }
            int pass = cas_check_tree(&cas_fs, old_val, (int)old_vlen,
                                      old_hdr2, cas_tree);
            free_criteria_tree(cas_tree);
```

(keeping the `if (!pass)` body that follows unchanged). Main-path
anchor:

```c
    SearchCriterion *crit = NULL;
    int ncrit = 0;
    if (if_json && parse_criteria_json(if_json, &crit, &ncrit) != 0) {
        OUT("{\"error\":\"invalid if condition\"}\n");
        return 1;
    }
```

replaced with the same `cas_fs`/`cas_err`/`cas_tree` prelude as the
insert path (LOG_WARN detail, canonical error, `return 1`).
`V2UpdateCtx` members `SearchCriterion *crit; int ncrit;` →
`CriteriaNode *cas_tree; FieldSchema *cas_fs;`; initializer
`.crit = crit, .ncrit = ncrit,` → `.cas_tree = cas_tree, .cas_fs =
&cas_fs,`; `v2_update_check_fn`'s check becomes:

```c
    if (c->cas_tree &&
        !cas_check_tree(c->cas_fs, old->value, (int)old->vlen, old->hdr2,
                        c->cas_tree)) return 0;
```

and the four frees (:2004, :2009, :2019, :2023) become
`free_criteria_tree(cas_tree);`.

6. **storage.c delete path** — same mechanical swap: both parse sites
   (:2214 dry/probe form and :2243 main form) become the
   `cas_prepare_tree` prelude; `V2DeleteCtx` members `SearchCriterion
   *crit; int ncrit;` → `CriteriaNode *cas_tree; FieldSchema
   *cas_fs;`; `v2_delete_check_fn` becomes (Task 2 will extend it with
   the return_record capture — the final form is quoted there):

```c
static int v2_delete_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2DeleteCtx *c = (V2DeleteCtx *)ctx_ptr;
    if (c->cas_tree) {
        if (!old) return 0;
        if (!cas_check_tree(c->cas_fs, old->value, (int)old->vlen,
                            old->hdr2, c->cas_tree)) return 0;
    }
    return 1;
}
```

`int has_cas = (crit && ncrit > 0);` → `int has_cas = (cas_tree !=
NULL);`, the initializer `.crit = crit, .ncrit = ncrit,` →
`.cas_tree = cas_tree, .cas_fs = &cas_fs,`, and the frees (:2224,
:2227, :2286, :2295, :2300, :2311) become `free_criteria_tree`.

7. **query_bulk.c — complete bulk condition conversion.** This replaces
all CAS uses, including `bulk_upd_json_parse_if` (which the previous
inventory incorrectly called non-CAS). No worker may retain an array
condition after `cas_check` is removed. Compile once before dispatch;
keep each tree and `FieldSchema` alive until all workers/finish callbacks
have returned. An empty per-record array passes vacuously like single CAS.

The complete replacement functions below already incorporate Task 1's
cleanup/assignments. Task 3 applies its computed-value boundary edits to
these new bodies. No additional ad-hoc CAS edits are required.

**Struct hunks** — replace each quoted exact old member block, once in
the indicated struct, with its corresponding new block:

`BulkUpdShardWork` — replace the complete struct (quoted anchor is its
terminating `} BulkUpdShardWork;`):

```c
typedef struct {
    const char    *db_root;
    const char    *object;
    const Schema  *sch;
    TypedSchema   *ts;
    CriteriaNode  *tree;
    FieldSchema   *fs;
    const char    *value_json;
    char         **field_vals;  /* shared read-only decoded patch; cmd owns */
    const uint8_t *field_null;  /* shared explicit-null flags, [nfields]; cmd owns */
    const char   (*idx_fields)[256];
    const enum IndexType *idx_types;  /* [nidx] — IT_BTREE / IT_BITMAP / IT_TRIGRAM (NULL = all btree, legacy) */
    int            nidx;
    int            shard_id;
    BulkUpdRec    *recs;
    int            count;
    /* CAS: optional `if` condition re-verified per record under the wrlock.
       NULL/empty = no CAS check. Same SearchCriterion[] shape single-op
       cmd_update uses, which makes cas_check directly applicable. */
    CriteriaNode *cas_tree;
    FieldSchema *cas_fs;
    /* Results */
    int            updated;
    int            skipped;
    /* Deferred-request input + worker-owned scratch (bulk_upd_finish_request). */
    SlotcaskBulkShardInput input;
    int             prepared;
    SlotcaskBulkRec *batch;
    void            *ctxs;
    void            *scratch;
    uint8_t         *old_arena;
    uint8_t         *new_arena;
} BulkUpdShardWork;
```

`BulkDelCritShardWork` — replace the complete struct (quoted anchor is its
terminating `} BulkDelCritShardWork;`):

```c
typedef struct {
    SlotcaskDb     *sdb;
    const char     *db_root;
    const char     *object;
    const Schema   *sch;
    TypedSchema    *ts;
    CriteriaNode   *tree;
    FieldSchema    *fs;
    CriteriaNode *cas_tree;
    FieldSchema *cas_fs;
    char          (*idx_fields)[256];
    const enum IndexType *idx_types;  /* [nidx] — NULL = legacy all-btree */
    int             nidx;
    /* per-shard records */
    char          **keys;
    uint8_t       (*hashes)[16];
    int             count;
    /* result */
    int             deleted;
    int             skipped;
    int             durability_degraded;
    /* Deferred-request input + worker-owned scratch. */
    SlotcaskBulkShardInput input;
    int             prepared;
    SlotcaskBulkRec *batch;
    void            *ctxs;
} BulkDelCritShardWork;
```

`BulkUpdJsonRec` — replace the complete struct (quoted anchor is its
terminating `} BulkUpdJsonRec;`):

```c
typedef struct {
    char        *key;            /* heap-owned, null-terminated */
    size_t       klen;
    uint8_t      hash[16];
    int          start_slot;
    int          shard_id;
    /* Field deltas: aligned arrays of (typed-field index, owned-string value).
       An explicit JSON null is encoded in the index's sign:
       field_indices[j] = -(tidx + 1) ⇔ field tidx was unquoted null
       (field_values[j] is then NULL — the field is still "touched"). The
       negative encoding keeps the per-record allocation count at two, so
       the test_alloc-failure seam's documented site order stays valid. */
    int         n_fields;
    int         *field_indices;
    char       **field_values;
    /* Optional per-record CAS, parsed before any worker is dispatched. */
    int          if_present;
    CriteriaNode *if_tree;
} BulkUpdJsonRec;
```

`BulkUpdJsonShardWork` — replace the complete struct (quoted anchor is its
terminating `} BulkUpdJsonShardWork;`):

```c
typedef struct {
    const char       *db_root;
    const char       *object;
    const Schema     *sch;
    TypedSchema      *ts;
    FieldSchema      *cas_fs;
    const char      (*idx_fields)[256];
    const enum IndexType *idx_types;  /* [nidx] — NULL = legacy all-btree */
    int               nidx;
    int               shard_id;
    BulkUpdJsonRec   *recs;
    int               count;
    /* Results */
    int               updated;
    int               skipped;
    /* Deferred-request input + worker-owned scratch. */
    SlotcaskBulkShardInput input;
    int             prepared;
    SlotcaskBulkRec *batch;
    void            *ctxs;
    void            *scratch;
} BulkUpdJsonShardWork;
```

**`cmd_bulk_update`** — replace the complete existing function located by
this exact definition prefix:

```c
int cmd_bulk_update(const char *db_root, const char *object,
                    const char *criteria_json, const char *value_json,
                    const char *if_json, int limit, int dry_run)
```

with:

```c
int cmd_bulk_update(const char *db_root, const char *object,
                    const char *criteria_json, const char *value_json,
                    const char *if_json, int limit, int dry_run) {
    Schema sch = load_schema(db_root, object);
    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &perr);
    if (perr) {
        OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
        free_criteria_tree(tree);
        return 1;
    }
    if (!tree) {
        OUT("{\"error\":\"Missing criteria\"}\n");
        return 1;
    }

    /* Parse optional `if` once into the compiled criteria tree that
       cas_check_tree expects. Workers share the parsed array read-only. */
    FieldSchema cas_fs;
    init_field_schema(&cas_fs, db_root, object);
    char cas_err[256];
    CriteriaNode *cas_tree = NULL;
    if (if_json && if_json[0]) {
        cas_tree = cas_prepare_tree(if_json, cas_fs.ts, 1, cas_err, sizeof(cas_err));
        if (!cas_tree && cas_err[0]) {
            OUT("{\"error\":\"invalid if condition\"}\n");
            free_criteria_tree(tree);
            return 1;
        }
    }

    /* Phase 1: Scan — collect matching keys (read-only) */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    compile_criteria_tree(tree, fs.ts);
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    BulkCriteriaCtx ctx = { tree, &fs, NULL, 0, 0, limit, &dl, 0, 0, 0,
                            PTHREAD_MUTEX_INITIALIZER };
    scan_dispatch(db_root, object, &sch, data_dir, bulk_criteria_scan_cb, &ctx);
    pthread_mutex_destroy(&ctx.lock);
    int matched = ctx.count;

    if (dl.timed_out) {
        LOG_WARN(LOG_SUB_QUERY, "bulk-update: query deadline exceeded while matching criteria");
        OUT("{\"error\":\"query_timeout\"}\n");
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }
    if (ctx.budget_exceeded) {
        LOG_WARN(LOG_SUB_QUERY, "bulk-update: query buffer cap exceeded while matching criteria");
        OUT(QUERY_BUFFER_ERR);
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }

    if (dry_run) {
        OUT("{\"matched\":%d,\"updated\":0,\"skipped\":0,\"dry_run\":true}\n", matched);
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 0;
    }

    /* Phase 2: Write — bucket matched keys by shard and fan out one worker
       per shard. Each worker takes the kf-shard wrlock **once** per shard,
       walks its bucket end-to-end, and releases **once** — matching the
       bulk-insert pattern. Index updates (btree_insert/btree_delete) are
       serialised internally by bt_cache_lock, so concurrent workers are
       safe. */
    TypedSchema *ts = fs.ts;
    const char *value_field_names[MAX_FIELDS];
    enum FieldType value_field_types[MAX_FIELDS];
    char *shared_field_vals[MAX_FIELDS] = {0};
    uint8_t shared_field_null[MAX_FIELDS] = {0};
    for (int i = 0; i < ts->nfields; i++) {
        value_field_names[i] = ts->fields[i].name;
        value_field_types[i] = ts->fields[i].type;
    }
    if (json_get_fields_null_aware(value_json, value_field_names, ts->nfields,
                                   value_field_types, shared_field_vals,
                                   shared_field_null) != 0) {
        for (int i = 0; i < ts->nfields; i++) free(shared_field_vals[i]);
        OUT("{\"error\":\"malformed JSON escape in one or more field values\"}\n");
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys);
        free_criteria_tree(tree);
        return 1;
    }
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed || !shared_field_null[i] ||
            !ts->fields[i].not_null)
            continue;
        /* One explicit-null patch fans out to EVERY matched record —
           refuse the whole request up front, before any key is
           staged (same whole-request policy as the malformed-escape
           rejection above). */
        char emsg[128];
        fmt_not_null_err(&ts->fields[i], emsg, sizeof(emsg));
        OUT("{\"error\":\"%s\"}\n", emsg);
        free_criteria_tree(cas_tree);
        for (int _k = 0; _k < matched; _k++) free(ctx.keys[_k]);
        free(ctx.keys);
        free_criteria_tree(tree);
        return 1;
    }
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */
    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);
    int updated = 0, skipped = 0;

    /* Pre-compute each matched key's hash + shard placement so bucketing
       is a single pass over ctx.keys[]. */
    BulkUpdRec *all = matched > 0 ? malloc(matched * sizeof(BulkUpdRec)) : NULL;
    for (int i = 0; i < matched; i++) {
        all[i].key = ctx.keys[i];
        all[i].klen = strlen(ctx.keys[i]);
        compute_hash_raw(all[i].key, all[i].klen, all[i].hash);
        all[i].shard_id = compute_record_shard(all[i].hash, sch.splits);
        all[i].start_slot = 0;
    }

    /* Bucket by shard_id. */
    int *shard_counts = calloc(sch.splits, sizeof(int));
    for (int i = 0; i < matched; i++) shard_counts[all[i].shard_id]++;
    int *worker_shards = NULL, *shard_to_worker = NULL;
    int nshard_groups = build_shard_worker_map(shard_counts, sch.splits,
                                                &worker_shards, &shard_to_worker);

    BulkUpdShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkUpdShardWork)) : NULL;
    for (int g = 0; g < nshard_groups; g++) {
        int s = worker_shards[g];
        workers[g].shard_id = s;
        workers[g].recs = malloc(shard_counts[s] * sizeof(BulkUpdRec));
        workers[g].count = 0;
    }
    for (int i = 0; i < matched; i++) {
        int w = shard_to_worker[all[i].shard_id];
        workers[w].recs[workers[w].count++] = all[i];
    }
    free(shard_counts);
    free(worker_shards);
    free(shard_to_worker);
    free(all);

    for (int wi = 0; wi < nshard_groups; wi++) {
        workers[wi].db_root = db_root;
        workers[wi].object = object;
        workers[wi].sch = &sch;
        workers[wi].ts = ts;
        workers[wi].tree = tree;
        workers[wi].fs = &fs;
        workers[wi].value_json = value_json;
        workers[wi].field_vals = shared_field_vals;
        workers[wi].field_null = shared_field_null;
        workers[wi].idx_fields = (const char (*)[256])idx_fields;
        workers[wi].idx_types = idx_types;
        workers[wi].nidx = nidx;
        workers[wi].cas_tree = cas_tree;
        workers[wi].cas_fs = &cas_fs;
        workers[wi].updated = 0;
        workers[wi].skipped = 0;
    }

    parallel_for_io(bulk_upd_shard_worker, workers, nshard_groups, sizeof(BulkUpdShardWork));

    /* Request execution: one deferred request over every worker's prepared
       input (ascending shard order). Exactly one execute per command. */
    {
        size_t np = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) np++;
        SlotcaskBulkShardInput *inputs = np ? calloc(np, sizeof(*inputs)) : NULL;
        size_t k = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) inputs[k++] = workers[g].input;
        if (np && !inputs) {
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = -1;
        } else if (np) {
            SlotcaskSchemaInfo info = {
                .splits = sch.splits, .slot_size = sch.slot_size,
                .streams = sch.streams,
            };
            SlotcaskDb *xsdb SDB_REG_REF =
                slotcask_registry_get(db_root, object, &info);
            uint64_t _commit_t0 = now_us();
            int xrc = xsdb ? bulk_execute_prepared(xsdb, inputs, np, 0) : -1;
            commit_lock_hold_record(_commit_t0, db_root, object);
            (void)xrc;
            k = 0;
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = inputs[k++].rc;
        }
        free(inputs);
        for (int g = 0; g < nshard_groups; g++)
            bulk_upd_finish_request(&workers[g]);
    }

    for (int wi = 0; wi < nshard_groups; wi++) {
        updated += workers[wi].updated;
        skipped += workers[wi].skipped;
        free(workers[wi].recs);
    }
    free(workers);
    for (int i = 0; i < ts->nfields; i++) free(shared_field_vals[i]);

    LOG_INFO(LOG_SUB_QUERY, "BULK-UPDATE %s matched=%d updated=%d skipped=%d", object, matched, updated, skipped);
    OUT("{\"matched\":%d,\"updated\":%d,\"skipped\":%d}\n", matched, updated, skipped);

    free_criteria_tree(cas_tree);
    for (int i = 0; i < matched; i++) free(ctx.keys[i]);
    free(ctx.keys); free_criteria_tree(tree);
    return 0;
}
```

**`v2_bulk_upd_value_compute`** — replace the complete existing function located by
this exact definition prefix:

```c
static int v2_bulk_upd_value_compute(const SlotcaskOldRecord *old,
                                       SlotcaskBulkRec *rec)
```

with:

```c
static int v2_bulk_upd_value_compute(const SlotcaskOldRecord *old,
                                       SlotcaskBulkRec *rec) {
    V2BulkUpdCtx *ctx = (V2BulkUpdCtx *)rec->user_ctx;
    BulkUpdShardWork *w = ctx->w;
    if (!old) return -1;
    if (!criteria_match_tree(old->value, old->vlen, w->tree, w->fs, old->hdr2)) return -1;
    if (w->cas_tree &&
        !cas_check_tree(w->cas_fs, old->value, (int)old->vlen, old->hdr2, w->cas_tree)) return -1;

    /* §2.7 conversion via the shared composer: NEW is nullmap layout,
       patches land at prefix+offset, explicit nulls set bits, untouched
       fields carry OLD forward generation-aware. */
    uint8_t *new_buf = (uint8_t *)rec->value;
    if (update_compose_new_from_old(w->ts, old, new_buf,
                                    ctx->field_vals, ctx->field_null) != 0)
        return -1;
    rec->vlen = (size_t)w->ts->total_size;
    rec->hdr2 = SREC_FLAG_NULLMAP;
    update_stamp_auto_update_fields(w->ts, new_buf);
    return 0;
}
```

**`cmd_bulk_delete_criteria`** — replace the complete existing function located by
this exact definition prefix:

```c
int cmd_bulk_delete_criteria(const char *db_root, const char *object,
                             const char *criteria_json, const char *if_json,
                             int limit, int dry_run)
```

with:

```c
int cmd_bulk_delete_criteria(const char *db_root, const char *object,
                             const char *criteria_json, const char *if_json,
                             int limit, int dry_run) {
    Schema sch = load_schema(db_root, object);
    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &perr);
    if (perr) {
        OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
        free_criteria_tree(tree);
        return 1;
    }
    if (!tree) {
        OUT("{\"error\":\"Missing criteria\"}\n");
        return 1;
    }

    /* Optional `if` for per-record CAS, re-verified under wrlock in phase 2. */
    FieldSchema cas_fs;
    init_field_schema(&cas_fs, db_root, object);
    char cas_err[256];
    CriteriaNode *cas_tree = NULL;
    if (if_json && if_json[0]) {
        cas_tree = cas_prepare_tree(if_json, cas_fs.ts, 1, cas_err, sizeof(cas_err));
        if (!cas_tree && cas_err[0]) {
            OUT("{\"error\":\"invalid if condition\"}\n");
            free_criteria_tree(tree);
            return 1;
        }
    }

    /* Phase 1: resolve matching keys — indexed path first, full scan fallback */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    compile_criteria_tree(tree, fs.ts);
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    BulkCriteriaCtx ctx = { tree, &fs, NULL, 0, 0, limit, &dl, 0, 0, 0,
                            PTHREAD_MUTEX_INITIALIZER };
    if (!bulk_delete_phase1_indexed(db_root, object, &sch, &fs,
                                     tree, limit, &dl, &ctx)) {
        scan_dispatch(db_root, object, &sch, data_dir, bulk_criteria_scan_cb, &ctx);
    }
    pthread_mutex_destroy(&ctx.lock);
    int matched = ctx.count;

    if (dl.timed_out) {
        LOG_WARN(LOG_SUB_QUERY, "bulk-delete: query deadline exceeded while matching criteria");
        OUT("{\"error\":\"query_timeout\"}\n");
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }
    if (ctx.budget_exceeded) {
        LOG_WARN(LOG_SUB_QUERY, "bulk-delete: query buffer cap exceeded while matching criteria");
        OUT(QUERY_BUFFER_ERR);
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }

    if (dry_run) {
        OUT("{\"matched\":%d,\"deleted\":0,\"skipped\":0,\"dry_run\":true}\n", matched);
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 0;
    }

    /* Phase 2: Write — for each key, acquire wrlock, re-verify, tombstone */
    TypedSchema *ts = load_typed_schema(db_root, object);
    int deleted = 0, skipped = 0;
    int any_durability_degraded = 0;

    /* v2 fast path: bucket matched keys by kf shard, then fan out one
       worker per bucket. Each worker calls slotcask_bulk_delete_in_kfshard
       once — same lock-amortisation pattern bulk-insert / bulk-update /
       bulk-delete (key-list) all use. The pre_commit hook re-verifies
       criteria + CAS under the kf wrlock (returning non-zero skips the
       record) and drops index entries for the deleted record. */
    SlotcaskSchemaInfo info = {
        .splits = sch.splits, .slot_size = sch.slot_size,
        .streams = sch.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';
    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    if (!sdb || matched == 0) {
        LOG_INFO(LOG_SUB_QUERY, "BULK-DELETE %s matched=%d deleted=0 skipped=%d (v2)",
                 object, matched, sdb ? 0 : matched);
        OUT("{\"matched\":%d,\"deleted\":0,\"skipped\":%d}\n",
            matched, sdb ? 0 : matched);
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 0;
    }

    /* Compute hash + shard-id per matched key; bucket-sort into per-shard
       workers. Same O(n + splits) pattern as cmd_bulk_insert and
       cmd_bulk_delete (replaces the old O(n²) insertion sort). */
    uint8_t (*hashes)[16] = malloc((size_t)matched * sizeof(uint8_t[16]));
    int      *shard_ids   = malloc((size_t)matched * sizeof(int));
    if (!hashes || !shard_ids) {
        free(hashes); free(shard_ids);
        OUT("{\"error\":\"oom: bulk_delete_criteria\"}\n");
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 1;
    }
    for (int i = 0; i < matched; i++) {
        compute_hash_raw(ctx.keys[i], strlen(ctx.keys[i]), hashes[i]);
        shard_ids[i] = compute_record_shard(hashes[i], sch.splits);
    }

    int *shard_counts = calloc(sch.splits, sizeof(int));
    if (!shard_counts) {
        free(hashes); free(shard_ids);
        OUT("{\"error\":\"oom: bulk_delete_criteria buckets\"}\n");
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 1;
    }
    for (int i = 0; i < matched; i++) shard_counts[shard_ids[i]]++;
    int *worker_shards = NULL, *shard_to_worker = NULL;
    int nshard_groups = build_shard_worker_map(shard_counts, sch.splits,
                                                &worker_shards, &shard_to_worker);

    BulkDelCritShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkDelCritShardWork)) : NULL;
    if (nshard_groups < 0 || (nshard_groups > 0 && !workers)) {
        free(workers); free(worker_shards); free(shard_to_worker);
        free(shard_counts); free(hashes); free(shard_ids);
        OUT("{\"error\":\"oom: bulk_delete_criteria workers\"}\n");
        free_criteria_tree(cas_tree);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 1;
    }
    for (int g = 0; g < nshard_groups; g++) {
        int s = worker_shards[g];
        int cnt = shard_counts[s];
        workers[g].sdb = sdb;
        workers[g].db_root = db_root;
        workers[g].object  = object;
        workers[g].sch     = &sch;
        workers[g].ts      = ts;
        workers[g].tree    = tree;
        workers[g].fs      = &fs;
        workers[g].cas_tree = cas_tree;
        workers[g].cas_fs = &cas_fs;
        workers[g].idx_fields = idx_fields;
        workers[g].idx_types  = idx_types;
        workers[g].nidx       = nidx;
        workers[g].keys   = malloc((size_t)cnt * sizeof(char *));
        workers[g].hashes = malloc((size_t)cnt * sizeof(uint8_t[16]));
        workers[g].count  = 0;
    }
    for (int i = 0; i < matched; i++) {
        int w = shard_to_worker[shard_ids[i]];
        int slot = workers[w].count++;
        workers[w].keys[slot] = ctx.keys[i];   /* shallow ref; freed once below */
        memcpy(workers[w].hashes[slot], hashes[i], 16);
    }

    parallel_for(bulk_del_crit_shard_worker, workers, nshard_groups,
                  sizeof(BulkDelCritShardWork));

    /* Request execution: one deferred request over every worker's prepared
       input (ascending shard order). Exactly one execute per command. */
    {
        size_t np = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) np++;
        SlotcaskBulkShardInput *inputs = np ? calloc(np, sizeof(*inputs)) : NULL;
        size_t k = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) inputs[k++] = workers[g].input;
        if (np && !inputs) {
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = -1;
        } else if (np) {
            uint64_t _commit_t0 = now_us();
            int xrc = bulk_execute_prepared(sdb, inputs, np, 0);
            commit_lock_hold_record(_commit_t0, db_root, object);
            (void)xrc;
            k = 0;
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = inputs[k++].rc;
        }
        free(inputs);
        for (int g = 0; g < nshard_groups; g++)
            bulk_del_crit_finish_request(&workers[g]);
    }

    for (int g = 0; g < nshard_groups; g++) {
        deleted += workers[g].deleted;
        skipped += workers[g].skipped;
        any_durability_degraded |= workers[g].durability_degraded;
        free(workers[g].keys);
        free(workers[g].hashes);
    }
    free(workers); free(hashes); free(shard_ids);
    free(shard_counts); free(worker_shards); free(shard_to_worker);

    if (deleted > 0) {
        update_count(db_root, object, -deleted);
        update_deleted_count(db_root, object, deleted);
    }
    LOG_INFO(LOG_SUB_QUERY, "BULK-DELETE %s matched=%d deleted=%d skipped=%d (v2)",
             object, matched, deleted, skipped);
    if (any_durability_degraded)
        OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");
    else
        OUT("{\"matched\":%d,\"deleted\":%d,\"skipped\":%d}\n",
            matched, deleted, skipped);

    free_criteria_tree(cas_tree);
    for (int i = 0; i < matched; i++) free(ctx.keys[i]);
    free(ctx.keys); free_criteria_tree(tree);
    return any_durability_degraded ? 1 : 0;
}
```

**`v2_bulk_del_crit_pre_commit_bulk`** — replace the complete existing function located by
this exact definition prefix:

```c
static int v2_bulk_del_crit_pre_commit_bulk(const SlotcaskOldRecord *old,
                                             SlotcaskBulkRec *rec)
```

with:

```c
static int v2_bulk_del_crit_pre_commit_bulk(const SlotcaskOldRecord *old,
                                             SlotcaskBulkRec *rec) {
    V2BulkDelCritCtx *ctx = (V2BulkDelCritCtx *)rec->user_ctx;
    BulkDelCritShardWork *w = ctx->w;
    if (!old) return -1;
    if (!criteria_match_tree(old->value, old->vlen, w->tree, w->fs, old->hdr2)) return -1;
    if (w->cas_tree &&
        !cas_check_tree(w->cas_fs, old->value, (int)old->vlen, old->hdr2, w->cas_tree)) return -1;
    return 0;
}
```

**`v2_bulk_del_crit_prepare_window`** — replace the complete existing function located by
this exact definition prefix:

```c
static int v2_bulk_del_crit_prepare_window(SlotcaskBulkRec *recs,
                                            const size_t *active, size_t nactive,
                                            void *ctx, void **out_window_state)
```

with:

```c
static int v2_bulk_del_crit_prepare_window(SlotcaskBulkRec *recs,
                                            const size_t *active, size_t nactive,
                                            void *ctx, void **out_window_state) {
    BulkDelCritShardWork *w = (BulkDelCritShardWork *)ctx;
    *out_window_state = NULL;
    for (size_t a = 0; a < nactive; a++) {
        size_t j = active[a];
        SlotcaskBulkRec *r = &recs[j];
        if (r->status != 0) continue;
        if (!r->old_value) { r->status = -1; continue; }
        if (!criteria_match_tree(r->old_value, r->old_vlen, w->tree, w->fs, r->old_hdr2)) {
            r->status = -1; continue;
        }
        if (w->cas_tree &&
            !cas_check_tree(w->cas_fs, r->old_value, (int)r->old_vlen,
                            r->old_hdr2, w->cas_tree)) {
            r->status = -1; continue;
        }
    }
    return 0;
}
```

**`bulk_upd_json_parse_if`** — replace the complete existing function located by
this exact definition prefix:

```c
static int bulk_upd_json_parse_if(const JsonObj *obj,
                                  SearchCriterion **out, int *out_count)
```

with:

```c
static int bulk_upd_json_parse_if(const JsonObj *obj, TypedSchema *ts,
                                  CriteriaNode **out) {
    const char *raw = NULL; size_t raw_len = 0;
    *out = NULL;
    if (!json_obj_get(obj, "if", &raw, &raw_len)) return 0;
    if (!raw_len) return -1;
    char *buf = malloc(raw_len + 1);
    if (!buf) return -1;
    memcpy(buf, raw, raw_len); buf[raw_len] = '\0';
    char err[256];
    *out = cas_prepare_tree(buf, ts, 1, err, sizeof(err));
    free(buf);
    return !*out && err[0] ? -1 : 1;
}
```

**`bulk_upd_json_if_free`** — replace the complete existing function located by
this exact definition prefix:

```c
static void bulk_upd_json_if_free(SearchCriterion **crit, int *count)
```

with:

```c
static void bulk_upd_json_if_free(CriteriaNode **tree) {
    free_criteria_tree(*tree);
    *tree = NULL;
}
```

**`v2_bulk_upd_json_value_compute`** — replace the complete existing function located by
this exact definition prefix:

```c
static int v2_bulk_upd_json_value_compute(const SlotcaskOldRecord *old,
                                           SlotcaskBulkRec *rec)
```

with:

```c
static int v2_bulk_upd_json_value_compute(const SlotcaskOldRecord *old,
                                           SlotcaskBulkRec *rec) {
    V2BulkUpdJsonCtx *ctx = (V2BulkUpdJsonCtx *)rec->user_ctx;
    BulkUpdJsonShardWork *w = ctx->w;
    BulkUpdJsonRec       *json_rec = ctx->rec;
    if (!old) return -1;
    if (json_rec->if_present &&
        !cas_check_tree(w->cas_fs, old->value, (int)old->vlen, old->hdr2,
                        json_rec->if_tree))
        return -1;

    uint8_t *new_buf = (uint8_t *)rec->value;
    if (update_compose_new_from_old(w->ts, old, new_buf, NULL, NULL) != 0)
        return -1;
    rec->vlen = (size_t)w->ts->total_size;
    rec->hdr2 = SREC_FLAG_NULLMAP;

    uint8_t *nbm = new_buf + 1;
    int nbm_bytes = typed_nullmap_bytes(w->ts->nfields);
    for (int i = 0; i < json_rec->n_fields; i++) {
        /* field_indices[j] is tidx, or -(tidx + 1) for an explicit null. */
        int tidx = json_rec->field_indices[i];
        int is_null = 0;
        if (tidx < 0) { is_null = 1; tidx = -tidx - 1; }
        if (tidx < 0 || tidx >= w->ts->nfields) continue;
        if (w->ts->fields[tidx].removed) continue;
        if (is_null) {
            /* Explicit JSON null — zero the carried bytes (canonical NULL),
               then set the bit. */
            memset(new_buf + w->ts->nullmap_prefix +
                   w->ts->fields[tidx].offset,
                   0, (size_t)w->ts->fields[tidx].size);
            typed_set_null(nbm, nbm_bytes, tidx);
            continue;
        }
        if (json_rec->field_values[i]) {
            encode_field(&w->ts->fields[tidx], json_rec->field_values[i],
                          new_buf + w->ts->nullmap_prefix +
                          w->ts->fields[tidx].offset);
            /* A materialized value clears any bit the §2.7 composer
               carried forward from a NULL OLD — value-with-set-bit would
               render and match as NULL. */
            typed_clear_null(nbm, nbm_bytes, tidx);
        }
    }
    update_stamp_auto_update_fields(w->ts, new_buf);
    return 0;
}
```

**`bulk_upd_json_run`** — replace the complete existing function located by
this exact definition prefix:

```c
static int bulk_upd_json_run(const char *db_root, const char *object,
                              const char *input, int input_is_file)
```

with:

```c
static int bulk_upd_json_run(const char *db_root, const char *object,
                              const char *input, int input_is_file) {
    int parse_oom = 0;
    /* Cleanup-block variables hoisted to the head: the OOM cleanups jump
       (goto parse_oom_cleanup) from before their original declaration
       points, so they must carry their initializers here. */
    BulkUpdJsonRec *records = NULL;
    size_t rec_cap = 1024, rec_count = 0;
    BulkUpdJsonError *errors = NULL;
    size_t error_count = 0;
    size_t error_capacity = 0;
    BulkUpdJsonKeyRef *key_refs = NULL;
    size_t key_ref_count = 0;
    size_t key_ref_capacity = 0;
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"bulk-update-json requires typed fields (fields.conf)\"}\n");
        return 1;
    }
    FieldSchema cas_fs;
    init_field_schema(&cas_fs, db_root, object);
    Schema sch = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */
    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    char *json = NULL;
    size_t len = 0;
    int json_mmaped = 0;
    int ifd = -1;
    if (input_is_file) {
        ifd = open(input, O_RDONLY);
        if (ifd < 0) { OUT("{\"error\":\"Cannot open file\"}\n"); return 1; }
        struct stat st;
        if (fstat(ifd, &st) < 0) { close(ifd); OUT("{\"error\":\"Cannot stat file\"}\n"); return 1; }
        if (st.st_size == 0) { close(ifd); OUT("{\"error\":\"Empty file\"}\n"); return 1; }
        len = st.st_size;
        json = mmap(NULL, len, PROT_READ, MAP_SHARED, ifd, 0);
        if (json == MAP_FAILED) {
            json = malloc(len + 1);
#ifdef TEST_BUILD
            if (g_bulk_upd_test_fail_alloc > 0 && --g_bulk_upd_test_fail_alloc == 0) {
                free(json); json = NULL;
            }
#endif
            if (!json) { close(ifd); parse_oom = 1; goto parse_oom_cleanup; }
            lseek(ifd, 0, SEEK_SET);
            size_t rd = 0;
            while (rd < len) {
                ssize_t n = read(ifd, json + rd, len - rd);
                if (n <= 0) break;
                rd += n;
            }
            json[rd] = '\0';
        } else {
            madvise((void *)json, len, MADV_SEQUENTIAL);
            json_mmaped = 1;
        }
        close(ifd);
    } else {
        json = (char *)input;
        len = strlen(input);
    }

    /* Phase 1: parse the array, extract per-record (key, touched fields, hash, shard). */
    records = malloc(rec_cap * sizeof(BulkUpdJsonRec));
#ifdef TEST_BUILD
    if (g_bulk_upd_test_fail_alloc > 0 && --g_bulk_upd_test_fail_alloc == 0) {
        free(records); records = NULL;
    }
#endif
    if (!records) { parse_oom = 1; goto parse_oom_cleanup; }

    int matched = 0, skipped = 0;

    CriteriaNode *if_tree = NULL;
    int if_rc = 0;

    const char *p = json_skip(json);
    int is_object_format = (*p == '{'); /* {"k1":{...},"k2":{...}}    — round-trips with get-multi */
    int is_array_format  = (*p == '[');  /* [{"key":"k1","value":{...}},...] */
    if (!is_object_format && !is_array_format) {
        OUT("{\"error\":\"bulk-update JSON must be a top-level object or array\"}\n");
        if (json_mmaped) munmap((void *)json, len);
        else if (input_is_file) free(json);
        free(records);
        return 1;
    }
    p++;

    /* Pre-name the typed fields once so we can reuse the names array per
       record without rebuilding it. */
    const char *field_names[MAX_FIELDS];
    enum FieldType field_types[MAX_FIELDS];
    for (int i = 0; i < ts->nfields; i++) {
        field_names[i] = ts->fields[i].name;
        field_types[i] = ts->fields[i].type;
    }

    while (*p) {
        p = json_skip(p);
        if (*p == ']' || *p == '}') break;
        if (*p == ',') { p++; continue; }

        char *key = NULL; size_t klen = 0;
        const char *data_str = NULL;
        const char *obj_end = NULL;
        char obj_buf[8192];
        char *obj_str = NULL;
        int obj_heap = 0;
        if_tree = NULL;

        if_rc = 0;

        if (is_object_format) {
            /* "key": {...} */
            if (*p != '"') { p++; continue; }
            p++;
            const char *key_start = p;
            while (*p && *p != '"') p++;
            klen = p - key_start;
            key = malloc(klen + 1);
#ifdef TEST_BUILD
            if (g_bulk_upd_test_fail_alloc > 0 && --g_bulk_upd_test_fail_alloc == 0) {
                free(key); key = NULL;
            }
#endif
            if (!key) { parse_oom = 1; goto parse_oom_cleanup; }
            memcpy(key, key_start, klen);
            key[klen] = '\0';
            if (*p == '"') p++;
            p = json_skip(p);
            if (*p == ':') p = json_skip(p + 1);

            /* Data span points into the original mmap; json_get_fields scans
               by brace-count so the trailing comma/} doesn't matter. */
            data_str = p;
            obj_end = json_skip_value(p);
        } else {
            if (*p != '{') { p++; continue; }
            const char *obj_start = p;
            obj_end = json_skip_value(p);
            size_t obj_len = obj_end - obj_start;

            if (obj_len < sizeof(obj_buf)) {
                memcpy(obj_buf, obj_start, obj_len);
                obj_buf[obj_len] = '\0';
                obj_str = obj_buf;
            } else {
                obj_str = malloc(obj_len + 1);
#ifdef TEST_BUILD
                if (g_bulk_upd_test_fail_alloc > 0 && --g_bulk_upd_test_fail_alloc == 0) {
                    free(obj_str); obj_str = NULL;
                }
#endif
                if (!obj_str) { parse_oom = 1; goto parse_oom_cleanup; }
                memcpy(obj_str, obj_start, obj_len);
                obj_str[obj_len] = '\0';
                obj_heap = 1;
            }

            JsonObj rec;
            json_parse_object(obj_str, obj_len, &rec);

            const char *iv; size_t ivl;
            if (json_obj_unquoted(&rec, "key", &iv, &ivl)) {
                key = malloc(ivl + 1);
#ifdef TEST_BUILD
                if (g_bulk_upd_test_fail_alloc > 0 && --g_bulk_upd_test_fail_alloc == 0) {
                    free(key); key = NULL;
                }
#endif
                if (!key) {
                    if (obj_heap) free(obj_str);
                    obj_heap = 0;
                    parse_oom = 1; goto parse_oom_cleanup;
                }
                memcpy(key, iv, ivl);
                key[ivl] = '\0';
                klen = ivl;
            }

            const char *dv; size_t dl;
            if (json_obj_get(&rec, "value", &dv, &dl)) {
                data_str = dv;
                (void)dl;
            }

            if (key && bulk_upd_json_key_ref_add(&key_refs, &key_ref_count,
                                                 &key_ref_capacity, key, klen) != 0) {
                free(key);
                if (obj_heap) free(obj_str);
                parse_oom = 1;
                break;
            }

            if_rc = bulk_upd_json_parse_if(&rec, ts, &if_tree);
            if (if_rc < 0) {
                matched++;
                skipped++;
                if (bulk_upd_json_error_add(&errors, &error_count, &error_capacity,
                                            key, "invalid if condition") != 0) {
                    free(key);
                    if (obj_heap) free(obj_str);
                    bulk_upd_json_if_free(&if_tree);
                    parse_oom = 1;
                    break;
                }
                free(key);
                if (obj_heap) free(obj_str);
                p = obj_end;
                continue;
            }
        }

        if (!key || !data_str) {
            skipped++;
            free(key);
            bulk_upd_json_if_free(&if_tree);
            if (obj_heap) free(obj_str);
            p = obj_end;
            continue;
        }
        if ((int)klen > sch.max_key) {
            skipped++;
            free(key);
            bulk_upd_json_if_free(&if_tree);
            if (obj_heap) free(obj_str);
            p = obj_end;
            continue;
        }

        /* The key/data pair matched the input shape; malformed field text is
           reported as skipped below while remaining part of the matched
           input count. */
        matched++;

        /* Pull out every typed-field name from `data`. Fields not present in
           `data` come back NULL → not touched. An unquoted null span sets
           nulls_buf[i] and clears vals_buf[i] — still touched. */
        char *vals_buf[MAX_FIELDS];
        uint8_t nulls_buf[MAX_FIELDS] = {0};
        if (json_get_fields_null_aware(data_str, field_names, ts->nfields,
                                       field_types, vals_buf, nulls_buf) != 0) {
            /* Each record carries its own patch, so a malformed escape skips
               only this record rather than aborting otherwise-valid peers. */
            for (int i = 0; i < ts->nfields; i++) free(vals_buf[i]);
            skipped++;
            free(key);
            bulk_upd_json_if_free(&if_tree);
            if (obj_heap) free(obj_str);
            p = obj_end;
            continue;
        }

        /* :not_null violation — per-record skip, mirroring the
           malformed-escape skip above: this record's own patch is
           rejected, peers proceed. Tombstoned fields are ignored. */
        int nn_violation = 0;
        for (int i = 0; i < ts->nfields; i++) {
            if (ts->fields[i].removed || !nulls_buf[i] ||
                !ts->fields[i].not_null)
                continue;
            LOG_WARN(LOG_SUB_QUERY,
                     "bulk-update: skipped a record — not_null violation "
                     "on field '%s'", ts->fields[i].name);
            nn_violation = 1;
            break;
        }
        if (nn_violation) {
            for (int i = 0; i < ts->nfields; i++) free(vals_buf[i]);
            skipped++;
            free(key);
            bulk_upd_json_if_free(&if_tree);
            if (obj_heap) free(obj_str);
            p = obj_end;
            continue;
        }

        int n_touched = 0;
        for (int i = 0; i < ts->nfields; i++)
            if (vals_buf[i] || nulls_buf[i]) n_touched++;

        BulkUpdJsonRec *r;
        if (rec_count >= rec_cap) {
            rec_cap *= 2;
            /* Plain realloc (not xrealloc_or_free) so we can walk records[]
               for nested free() before releasing the array. xrealloc_or_free
               frees atomically, leaving no window to clean up the per-record
               heap-owned key / field_indices / field_values mallocs. */
            BulkUpdJsonRec *t = realloc(records, rec_cap * sizeof(*t));
            if (!t) {
                /* OOM: free per-record nested allocations, then the array,
                   then the current iteration's locals (whose ownership
                   hadn't transferred to records[rec_count] yet). Mark the
                   parse as OOM so no worker is dispatched and the parse-phase
                   cleanup emits the error response. */
                for (size_t k = 0; k < rec_count; k++) {
                    free(records[k].key);
                    for (int j = 0; j < records[k].n_fields; j++)
                        free(records[k].field_values[j]);
                    free(records[k].field_values);
                    free(records[k].field_indices);
                    free_criteria_tree(records[k].if_tree);
                }
                free(records);
                records = NULL;
                rec_count = 0;
                free(key);
                for (int i = 0; i < ts->nfields; i++) free(vals_buf[i]);
                bulk_upd_json_if_free(&if_tree);
                if (obj_heap) free(obj_str);
                parse_oom = 1;
                break;
            }
            records = t;
        }
        r = &records[rec_count++];
        r->key = key;
        r->klen = klen;
        compute_hash_raw(key, klen, r->hash);
        r->shard_id = compute_record_shard(r->hash, sch.splits);
        r->start_slot = 0;
        r->if_present = 0;
        r->if_tree = NULL;

        /* v2 alignment — see cmd_bulk_insert for rationale. */
                    r->shard_id = compute_record_shard(r->hash, sch.splits);
        if (n_touched > 0) {
            r->n_fields = n_touched;
            r->field_indices = malloc(n_touched * sizeof(int));
            r->field_values = malloc(n_touched * sizeof(char *));
#ifdef TEST_BUILD
            if (g_bulk_upd_test_fail_alloc > 0 && --g_bulk_upd_test_fail_alloc == 0) {
                free(r->field_indices); free(r->field_values);
                r->field_indices = NULL; r->field_values = NULL;
            }
#endif
            if (!r->field_indices || !r->field_values) {
                free(r->field_indices);
                free(r->field_values);
                r->field_indices = NULL;
                r->field_values = NULL;
                r->n_fields = 0;
                for (int i = 0; i < ts->nfields; i++) free(vals_buf[i]);
                if (obj_heap) free(obj_str);
                obj_heap = 0;
                bulk_upd_json_if_free(&if_tree);
                if_tree = NULL;

                parse_oom = 1;
                break;
            }
            int j = 0;
            for (int i = 0; i < ts->nfields; i++) {
                if (vals_buf[i]) {
                    r->field_indices[j] = i;
                    r->field_values[j] = vals_buf[i];   /* take ownership */
                    j++;
                } else if (nulls_buf[i]) {
                    r->field_indices[j] = -(i + 1);     /* explicit null */
                    r->field_values[j] = NULL;
                    j++;
                }
            }
        } else {
            r->n_fields = 0;
            r->field_indices = NULL;
            r->field_values = NULL;
        }
        if (if_rc == 1) {
            r->if_present = 1;
            r->if_tree = if_tree;

            if_tree = NULL;

        }
        if (obj_heap) free(obj_str);
        p = obj_end;
    }

    parse_oom_cleanup:
    if (parse_oom) {
        for (size_t i = 0; i < rec_count; i++) {
            free(records[i].key);
            for (int j = 0; j < records[i].n_fields; j++)
                free(records[i].field_values[j]);
            free(records[i].field_values);
            free(records[i].field_indices);
            free_criteria_tree(records[i].if_tree);
        }
        free(records);
        bulk_upd_json_errors_free(errors, error_count);
        bulk_upd_json_key_refs_free(key_refs, key_ref_count);
        if (json_mmaped) munmap((void *)json, len);
        else if (input_is_file) free(json);
        OUT("{\"error\":\"out of memory\"}\n");
        return 1;
    }

    char *duplicate_key = NULL;
    int duplicate_rc = bulk_upd_json_find_duplicate(key_refs, key_ref_count,
                                                    &duplicate_key);
    if (duplicate_rc == 1) {
        char *escaped = json_escape_const(duplicate_key);
        OUT("{\"error\":\"duplicate key in records: %s\"}\n",
            escaped ? escaped : "");
        free(escaped);
        free(duplicate_key);
        bulk_upd_json_key_refs_free(key_refs, key_ref_count);
        bulk_upd_json_errors_free(errors, error_count);
        for (size_t i = 0; i < rec_count; i++) {
            free(records[i].key);
            for (int j = 0; j < records[i].n_fields; j++)
                free(records[i].field_values[j]);
            free(records[i].field_values);
            free(records[i].field_indices);
            free_criteria_tree(records[i].if_tree);
        }
        free(records);
        if (json_mmaped) munmap((void *)json, len);
        else if (input_is_file) free(json);
        return 1;
    }
    if (duplicate_rc < 0) {
        for (size_t i = 0; i < rec_count; i++) {
            free(records[i].key);
            for (int j = 0; j < records[i].n_fields; j++)
                free(records[i].field_values[j]);
            free(records[i].field_values);
            free(records[i].field_indices);
            free_criteria_tree(records[i].if_tree);
        }
        free(records);
        bulk_upd_json_errors_free(errors, error_count);
        bulk_upd_json_key_refs_free(key_refs, key_ref_count);
        if (json_mmaped) munmap((void *)json, len);
        else if (input_is_file) free(json);
        OUT("{\"error\":\"out of memory\"}\n");
        return 1;
    }
    bulk_upd_json_key_refs_free(key_refs, key_ref_count);
    key_refs = NULL;
    key_ref_count = 0;

    if (rec_count == 0) {
        bulk_upd_json_emit_response(matched, 0, skipped, errors, error_count);
        bulk_upd_json_errors_free(errors, error_count);
        if (json_mmaped) munmap((void *)json, len);
        else if (input_is_file) free(json);
        free(records);
        return 0;
    }

    /* Bucket per shard — same pattern as bulk-update-delimited / bulk-delete. */
    int *shard_counts = calloc(sch.splits, sizeof(int));
    if (!shard_counts) {
        for (size_t i = 0; i < rec_count; i++) {
            free(records[i].key);
            for (int j = 0; j < records[i].n_fields; j++) free(records[i].field_values[j]);
            free(records[i].field_values);
            free(records[i].field_indices);
            free_criteria_tree(records[i].if_tree);
        }
        free(records);
        bulk_upd_json_errors_free(errors, error_count);
        bulk_upd_json_key_refs_free(key_refs, key_ref_count);
        if (json_mmaped) munmap((void *)json, len); else if (input_is_file) free(json);
        OUT("{\"error\":\"oom: shard_counts\"}\n");
        return 1;
    }
    for (size_t i = 0; i < rec_count; i++) shard_counts[records[i].shard_id]++;

    int nshard_groups = 0;
    for (int s = 0; s < sch.splits; s++) if (shard_counts[s] > 0) nshard_groups++;

    BulkUpdJsonShardWork *workers = calloc(nshard_groups, sizeof(BulkUpdJsonShardWork));
    if (nshard_groups > 0 && !workers) {
        free(shard_counts);
        for (size_t i = 0; i < rec_count; i++) {
            free(records[i].key);
            for (int j = 0; j < records[i].n_fields; j++) free(records[i].field_values[j]);
            free(records[i].field_values);
            free(records[i].field_indices);
            free_criteria_tree(records[i].if_tree);
        }
        free(records);
        bulk_upd_json_errors_free(errors, error_count);
        bulk_upd_json_key_refs_free(key_refs, key_ref_count);
        if (json_mmaped) munmap((void *)json, len); else if (input_is_file) free(json);
        OUT("{\"error\":\"oom: workers\"}\n");
        return 1;
    }
    int wi = 0;
    for (int s = 0; s < sch.splits; s++) {
        if (shard_counts[s] > 0) {
            workers[wi].db_root = db_root;
            workers[wi].object = object;
            workers[wi].sch = &sch;
            workers[wi].ts = ts;
            workers[wi].cas_fs = &cas_fs;
            workers[wi].idx_fields = idx_fields;
            workers[wi].idx_types = idx_types;
            workers[wi].nidx = nidx;
            workers[wi].shard_id = s;
            workers[wi].recs = malloc(shard_counts[s] * sizeof(BulkUpdJsonRec));
            workers[wi].count = 0;
            workers[wi].updated = 0;
            workers[wi].skipped = 0;
            wi++;
        }
    }

    for (size_t i = 0; i < rec_count; i++) {
        for (int gi = 0; gi < nshard_groups; gi++) {
            if (workers[gi].shard_id == records[i].shard_id) {
                workers[gi].recs[workers[gi].count++] = records[i];
                break;
            }
        }
        /* Null the heap-owned pointers in records[i] unconditionally — the
           inner loop above ALWAYS finds a matching worker by construction
           (every records[i].shard_id was used to compute shard_counts, and
           every shard with count > 0 got a worker), but Coverity can't
           trace the invariant. NULLing outside the if covers the
           unreachable no-match case so free(records) below is leak-free
           in the static analyzer's view too. */
        records[i].key = NULL;
        records[i].field_values = NULL;
        records[i].field_indices = NULL;
        records[i].if_tree = NULL;

    }
    free(shard_counts);

    /* Phase 2: parallel shard workers. */
    parallel_for_io(bulk_upd_json_shard_worker, workers, nshard_groups,
                 sizeof(BulkUpdJsonShardWork));

    /* Request execution: one deferred request over every worker's prepared
       input (ascending shard order). Exactly one execute per command. */
    {
        size_t np = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) np++;
        SlotcaskBulkShardInput *inputs = np ? calloc(np, sizeof(*inputs)) : NULL;
        size_t k = 0;
        for (int g = 0; g < nshard_groups; g++)
            if (workers[g].prepared) inputs[k++] = workers[g].input;
        if (np && !inputs) {
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = -1;
        } else if (np) {
            SlotcaskSchemaInfo info = {
                .splits = sch.splits, .slot_size = sch.slot_size,
                .streams = sch.streams,
            };
            SlotcaskDb *xsdb SDB_REG_REF =
                slotcask_registry_get(db_root, object, &info);
            uint64_t _commit_t0 = now_us();
            int xrc = xsdb ? bulk_execute_prepared(xsdb, inputs, np, 0) : -1;
            commit_lock_hold_record(_commit_t0, db_root, object);
            (void)xrc;
            k = 0;
            for (int g = 0; g < nshard_groups; g++)
                if (workers[g].prepared) workers[g].input.rc = inputs[k++].rc;
        }
        free(inputs);
        for (int g = 0; g < nshard_groups; g++)
            bulk_upd_json_finish_request(&workers[g]);
    }

    int updated = 0;
    for (int gi = 0; gi < nshard_groups; gi++) {
        updated += workers[gi].updated;
        skipped += workers[gi].skipped;
        for (int i = 0; i < workers[gi].count; i++) {
            BulkUpdJsonRec *r = &workers[gi].recs[i];
            free(r->key);
            for (int j = 0; j < r->n_fields; j++) free(r->field_values[j]);
            free(r->field_values);
            free(r->field_indices);
            free_criteria_tree(r->if_tree);
        }
        free(workers[gi].recs);
    }
    free(workers);
    free(records);

    if (json_mmaped) munmap((void *)json, len);
    else if (input_is_file) free(json);

    LOG_INFO(LOG_SUB_QUERY, "BULK-UPDATE-JSON %s matched=%d updated=%d skipped=%d",
            object, matched, updated, skipped);
    bulk_upd_json_emit_response(matched, updated, skipped, errors, error_count);
    bulk_upd_json_errors_free(errors, error_count);
    return 0;
}
```

**Consumer inventory and final check:** the parser's sole call is in
`bulk_upd_json_run`; its owned `if_tree` transfers into `BulkUpdJsonRec`,
then into shard workers, and is freed by all error and finish paths in the
complete body above. `BulkUpdJsonShardWork.cas_fs` references the command's
stack-local initialized schema, which outlives dispatch and finishing.
Criteria-form update/delete workers share their command's compiled tree.
After Task 1, search storage.c/query_bulk.c for `cas_check(`: no calls may
remain. In query_bulk.c no `cas_crit`, `cas_ncrit`, `if_crit` or `if_ncrit`
may remain. Do not remove the general read parser from query_plan.c.

Run the case → paste the failing run, implement, paste the green run.
Full suite → green.

---

### Task 2 — F4: `return_record`

**How this serves the primary goal:** return the bytes of the actual affected record.

**Test** — new file `src/test/cases/test_return_record.c`, registered
as `test-return-record` (build.sh anchor: after the
`test_cas_tree_if.c` line). On the pre-task tree it fails at: the
record-bearing insert (step 1), the default echo (step 2), the
update/delete records (steps 4, 6), and the auto-key record (step 7).

```c
/* src/test/cases/test_return_record.c
 *
 * Conditional-writes F4 — opt-in return of the committed/removed record.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int test_return_record_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rv\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:20\",\"qty:int:default=7\","
        "\"tag:varchar:10\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create rv");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ak\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"label:varchar:10\"],"
        "\"auto_key\":\"uuid\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create ak");
    free(resp); resp = NULL;

    /* 1. Insert with return_record — committed record echoes defaults. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r1\",\"value\":{\"name\":\"a\"},\"return_record\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "insert ok");
    ASSERT_CONTAINS(resp, "\"record\":", "record returned");
    ASSERT_CONTAINS(resp, "\"qty\":7", "default evaluated in record");
    free(resp); resp = NULL;

    /* 2. Default response shape unchanged — no record member. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r2\",\"value\":{\"name\":\"b\",\"qty\":1,\"tag\":\"t\"}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "plain insert ok");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"record\"") == NULL,
                "no record without the flag");
    free(resp); resp = NULL;

    /* 3. Overwrite-insert with the flag returns the NEW record. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r1\",\"value\":{\"name\":\"a2\",\"qty\":2},"
        "\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "overwrite reported");
    ASSERT_CONTAINS(resp, "\"qty\":2", "new value in record");
    free(resp); resp = NULL;

    /* 4. Update with the flag returns the committed record. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r1\",\"value\":{\"tag\":\"z\"},\"return_record\":true}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "update ok");
    ASSERT_CONTAINS(resp, "\"record\":", "update record returned");
    ASSERT_CONTAINS(resp, "\"tag\":\"z\"", "patched field present");
    ASSERT_CONTAINS(resp, "\"qty\":2", "carried field present");
    free(resp); resp = NULL;

    /* 5. condition_not_met shape unchanged (current, no record member). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r1\",\"value\":{\"qty\":3},\"if\":[{\"field\":\"qty\","
        "\"op\":\"eq\",\"value\":999}],\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "CAS miss");
    ASSERT_CONTAINS(resp, "\"current\":", "current returned");
    free(resp); resp = NULL;

    /* 6. Delete with the flag returns the removed record — captured
          explicitly, since result.current_value is only populated on
          CAS failure. */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r2\",\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"deleted\"", "delete ok");
    ASSERT_CONTAINS(resp, "\"record\":", "removed record returned");
    ASSERT_CONTAINS(resp, "\"name\":\"b\"", "removed values present");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rv\","
                   "\"key\":\"r2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "r2 gone");
    free(resp); resp = NULL;

    /* 7. Delete of a NON-indexed object with the flag (skip_old_read was
          historically true here — the capture must still work). */
    tc_request(tc, "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"ak\","
        "\"key\":\"12345678-1234-4234-8234-123456789abc\",\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"not_found\"", "missing delete sane");
    free(resp); resp = NULL;

    /* 8. Auto-key insert with the flag — the generated record comes
          back. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ak\","
        "\"value\":{\"label\":\"x\"},\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "auto-key insert ok");
    ASSERT_CONTAINS(resp, "\"record\":", "auto-key record returned");
    ASSERT_CONTAINS(resp, "\"label\":\"x\"", "label present");
    free(resp); resp = NULL;

    /* 9. Dry-run update keeps its shape (no record). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"rv\","
        "\"key\":\"r1\",\"value\":{\"qty\":5},\"dry_run\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"would_update\"", "dry run unchanged");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"record\"") == NULL,
                "no record on dry run");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-return-record", test_return_record_run)
```

**Implementation:**

1. **types.h** — anchor:

```c
int cmd_insert(const char *db_root, const char *object,
               const char *key, size_t klen,
               const char *value,
               const char *if_json, int if_not_exists);
int cmd_update(const char *db_root, const char *object,
               const char *key, size_t klen,
               const char *partial_json,
               const char *if_json, int dry_run);
int cmd_delete(const char *db_root, const char *object,
               const char *key, size_t klen,
               const char *if_json, int dry_run);
```

replace with the same signatures each gaining a final
`, int return_record` parameter.

The three public forwarding functions in storage.c become:

```c
int cmd_insert(const char *db_root, const char *object,
               const char *key, size_t klen, const char *value,
               const char *if_json, int if_not_exists, int return_record) {
    Schema sc = load_schema(db_root, object);
    return cmd_insert_v2(db_root, object, key, klen, value, if_json,
                          if_not_exists, return_record, &sc);
}
int cmd_update(const char *db_root, const char *object,
               const char *key, size_t klen, const char *partial_json,
               const char *if_json, int dry_run, int return_record) {
    Schema sc = load_schema(db_root, object);
    return cmd_update_v2(db_root, object, key, klen, partial_json,
                          if_json, dry_run, return_record, &sc);
}
int cmd_delete(const char *db_root, const char *object,
               const char *key, size_t klen,
               const char *if_json, int dry_run, int return_record) {
    Schema sc = load_schema(db_root, object);
    return cmd_delete_v2(db_root, object, key, klen, if_json,
                          dry_run, return_record, &sc);
}
```

At each static helper definition (`cmd_insert_v2`, `cmd_update_v2`,
`cmd_delete_v2`) insert `int return_record,` immediately BEFORE its final
`const Schema *sc` parameter. This exact ordering matches the complete
forwarders above. All other parameters and the body remain as patched by
Tasks 1–2. The public declarations in types.h gain the final parameter.

2. **server.c** — each write branch parses the flag
   (`int return_record = json_obj_is_true(&req, "return_record");`)
   beside its existing `if_not_exists` / `dry` parse and passes it as
   the final argument at all seven call sites: the generated-key insert
   (`cmd_insert(..., value, NULL, 1)`), both supplied-key insert calls,
   both update calls, and both delete calls.

3. **storage.c insert** — Task 1 replaced this function's final block;
   its post-Task-1 success tail is:

```c
    free(result.current_value);
    free_criteria_tree(cas_tree);
    free(typed_buf);
    OUT("{\"status\":\"%s\",\"key\":\"%s\"}\n",
        result.was_update ? "updated" : "inserted", wire_key);
    return 0;
```

replace with:

```c
    free(result.current_value);
    free_criteria_tree(cas_tree);
    if (return_record) {
        /* typed_buf was mutated in place by the auto_create transfer
           before the upsert — it IS the committed record. */
        char *rec = typed_decode(ts, typed_buf, (uint32_t)vlen,
                                 SREC_FLAG_NULLMAP);
        free(typed_buf);
        if (rec)
            OUT("{\"status\":\"%s\",\"key\":\"%s\",\"record\":%s}\n",
                result.was_update ? "updated" : "inserted", wire_key, rec);
        else
            OUT("{\"status\":\"%s\",\"key\":\"%s\"}\n",
                result.was_update ? "updated" : "inserted", wire_key);
        free(rec);
        return 0;
    }
    free(typed_buf);
    OUT("{\"status\":\"%s\",\"key\":\"%s\"}\n",
        result.was_update ? "updated" : "inserted", wire_key);
    return 0;
```

4. **storage.c update** — `V2UpdateCtx` gains `int return_record;` and
   `uint8_t *ret_buf;` (ret_buf zeroed by the struct init). At the end
   of `v2_update_new_from_old`, anchor:

```c
    update_stamp_auto_update_fields(c->idx_ts, out_value);
    *out_vlen = (size_t)c->idx_ts->total_size;   /* full nullmap record */
    return 0;
```

replace with:

```c
    update_stamp_auto_update_fields(c->idx_ts, out_value);
    *out_vlen = (size_t)c->idx_ts->total_size;   /* full nullmap record */
    if (c->return_record) {
        c->ret_buf = malloc((size_t)c->idx_ts->total_size);
        if (c->ret_buf)
            memcpy(c->ret_buf, out_value, (size_t)c->idx_ts->total_size);
        /* OOM: the write still commits; the response just omits the
           record (D3). */
    }
    return 0;
```

In `cmd_update_v2`: set `.return_record = return_record` in the ctx
init; add `free(ctx.ret_buf); ctx.ret_buf = NULL;` beside every
`free(result.current_value);` on the error exits (:1993-2015) and the
durability exit; replace the final success block (post-Task-1 form):

```c
    LOG_INFO(LOG_SUB_SLOTCASK, "UPDATE %s.%s (slotcask)", object, wire_key);
    free(result.current_value);
    OUT("{\"status\":\"updated\",\"key\":\"%s\"}\n", wire_key);
    return 0;
```

with:

```c
    LOG_INFO(LOG_SUB_SLOTCASK, "UPDATE %s.%s (slotcask)", object, wire_key);
    free(result.current_value);
    if (return_record && ctx.ret_buf) {
        char *rec = typed_decode(ts, ctx.ret_buf, (uint32_t)ts->total_size,
                                 SREC_FLAG_NULLMAP);
        free(ctx.ret_buf);
        ctx.ret_buf = NULL;
        if (rec) {
            OUT("{\"status\":\"updated\",\"key\":\"%s\",\"record\":%s}\n",
                wire_key, rec);
            free(rec);
            return 0;
        }
    }
    free(ctx.ret_buf);
    ctx.ret_buf = NULL;
    OUT("{\"status\":\"updated\",\"key\":\"%s\"}\n", wire_key);
    return 0;
```

(only the success exit emits; abort paths free the copy without
emitting).

5. **storage.c delete** — `V2DeleteCtx` gains `int return_record;
   uint8_t *ret_buf; size_t ret_len; uint8_t ret_hdr2;`. Extend
   `v2_delete_check_fn` (post-Task-1 form) to capture OLD — this is
   the explicit capture rev-2 finding 6 requires, because
   `slotcask_delete_with_hooks` does NOT populate
   `result.current_value` on success:

```c
static int v2_delete_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2DeleteCtx *c = (V2DeleteCtx *)ctx_ptr;
    if (old && c->return_record && !c->ret_buf) {
        c->ret_buf = malloc(old->vlen);
        if (c->ret_buf) {
            memcpy(c->ret_buf, old->value, old->vlen);
            c->ret_len = old->vlen;
            c->ret_hdr2 = old->hdr2;
        }
    }
    if (c->cas_tree) {
        if (!old) return 0;
        if (!cas_check_tree(c->cas_fs, old->value, (int)old->vlen,
                            old->hdr2, c->cas_tree)) return 0;
    }
    return 1;
}
```

In the opts block, anchor:

```c
    int has_cas = (cas_tree != NULL);
    SlotcaskDeleteOpts opts = {
        .check              = has_cas ? v2_delete_check_fn : NULL,
```

replace with:

```c
    int has_cas = (cas_tree != NULL);
    SlotcaskDeleteOpts opts = {
        .check              = (has_cas || return_record)
                                  ? v2_delete_check_fn : NULL,
```

and anchor:

```c
        .skip_old_read      = (nidx == 0),
```

replace with:

```c
        .skip_old_read      = (nidx == 0 && !return_record),
```

`cmd_delete_v2` gains the `return_record` param (threaded from
`cmd_delete`, which itself gains it per types.h). In its `V2DeleteCtx ctx`
initializer, add `.return_record = return_record,`; this is mandatory
because omitted members initialize to zero. Every exit frees
`ctx.ret_buf` — add `free(ctx.ret_buf); ctx.ret_buf = NULL;` beside
the existing frees in the not_found (:2285), condition_not_met
(:2295), and rc!=0 (:2300) exits. Replace the success block
(post-Task-1 form):

```c
    update_counts(db_root, object, -1, 1);
    LOG_INFO(LOG_SUB_SLOTCASK, "DELETE %s.%s (slotcask)", object, wire_key);
    free(result.current_value);
    free_criteria_tree(cas_tree);
    if (durability_degraded) {
        OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");
        return 1;
    }
    OUT("{\"status\":\"deleted\",\"key\":\"%s\"}\n", wire_key);
    return 0;
```

with:

```c
    update_counts(db_root, object, -1, 1);
    LOG_INFO(LOG_SUB_SLOTCASK, "DELETE %s.%s (slotcask)", object, wire_key);
    free(result.current_value);
    free_criteria_tree(cas_tree);
    if (durability_degraded) {
        free(ctx.ret_buf);
        OUT("{\"error\":\"durability outcome pending; do not retry blindly\"}\n");
        return 1;
    }
    if (return_record && ctx.ret_buf) {
        char *rec = typed_decode(ts, ctx.ret_buf, (uint32_t)ctx.ret_len,
                                 ctx.ret_hdr2);
        free(ctx.ret_buf);
        ctx.ret_buf = NULL;
        if (rec) {
            OUT("{\"status\":\"deleted\",\"key\":\"%s\",\"record\":%s}\n",
                wire_key, rec);
            free(rec);
            return 0;
        }
    }
    free(ctx.ret_buf);
    ctx.ret_buf = NULL;
    OUT("{\"status\":\"deleted\",\"key\":\"%s\"}\n", wire_key);
    return 0;
```

(`result.current_value` on the CAS-failure path is untouched.)

Run the case → paste failing then green. Full suite → green.

---

### Task 3 — F1: computed updates

**How this serves the primary goal:** calculate from lock-protected OLD without a client race.

**Test** — new file `src/test/cases/test_computed_update.c`, registered
as `test-computed-update` (build.sh anchor: after the
`test_return_record.c` line). On the pre-task tree it fails at: every
`$incr`/`$copy` application (steps 1-3), the overflow rejection
(step 4), the NULL rejection (step 5), and the D8 insert rejection
(step 9).

```c
/* src/test/cases/test_computed_update.c
 *
 * Conditional-writes F1 — atomic computed updates ($incr / $copy),
 * plus the D8 boundary: computed spans outside single `update` reject.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int test_computed_update_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"cp\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"balance:long\",\"shares:int\","
        "\"price:numeric:12,2\",\"ratio:double\",\"note:varchar:20\","
        "\"mirror:long\",\"tiny:short\"],"
        "\"indexes\":[\"balance\",\"shares\"]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create cp");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"cp\",\"records\":["
        "{\"key\":\"c1\",\"value\":{\"balance\":100,\"shares\":4,"
        "\"price\":\"10.50\",\"ratio\":1.5,\"note\":\"n\",\"mirror\":100}}]}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "seed c1");
    free(resp); resp = NULL;

    /* 1. $incr long, positive and negative; the index reflects the
          result. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":50}}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "incr +50");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":-25}}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"cp\","
        "\"criteria\":[{\"field\":\"balance\",\"op\":\"eq\",\"value\":125}]}",
        &resp);
    ASSERT_CONTAINS(resp, "1", "find/count sees indexed new value");
    free(resp); resp = NULL;

    /* 2. $incr int / numeric / double — numeric scaling is exact. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"shares\":{\"$incr\":-1}}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "incr int");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"price\":{\"$incr\":\"125.50\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "incr numeric");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"ratio\":{\"$incr\":0.25}}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "incr double");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"shares\":3", "int applied");
    ASSERT_CONTAINS(resp, "\"price\":\"136.00\"", "numeric applied exactly");
    ASSERT_CONTAINS(resp, "\"ratio\":1.75", "double applied");
    free(resp); resp = NULL;

    /* 3. $copy same type. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"mirror\":{\"$copy\":\"balance\"}}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "copy long");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"mirror\":125", "copied value");
    free(resp); resp = NULL;

    /* 4. Overflow rejects the whole request and leaves the record. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":"
        "9223372036854775807}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "overflow rejected");
    ASSERT_CONTAINS(resp, "overflow", "named overflow");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"balance\":125", "value unchanged after reject");
    free(resp); resp = NULL;

    /* 5. NULL target / NULL copy source reject. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"note\":null}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"mirror\":{\"$copy\":\"note\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "copy from NULL rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"cp\",\"records\":[{\"key\":\"c2\",\"value\":{}}]}",
        &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c2\",\"value\":{\"balance\":{\"$incr\":1}}}", &resp);
    ASSERT_CONTAINS(resp, "is NULL", "incr on NULL rejected");
    free(resp); resp = NULL;

    /* 6. Type and grammar rejections — no write in any case. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"note\":{\"$incr\":1}}}", &resp);
    ASSERT_CONTAINS(resp, "unsupported type", "varchar incr rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$mul\":2}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "unknown op rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":"
        "{\"balance\":{\"$incr\":1,\"$copy\":\"mirror\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "two ops rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":1,\"junk\":9}}}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "unknown member rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"price\":{\"$incr\":\"0.005\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "sub-scale literal rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$copy\":\"price\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "cross-type copy rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$copy\":\"nope\"}}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "unknown copy source rejected");
    free(resp); resp = NULL;

    /* 7. Condition + computed: if fails → nothing; if passes → applied. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":-200}},"
        "\"if\":[{\"field\":\"balance\",\"op\":\"gte\",\"value\":500}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"",
                    "condition gates the compute");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"balance\":125", "no compute on failed if");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":-25}},"
        "\"if\":[{\"field\":\"balance\",\"op\":\"gte\",\"value\":100}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "condition passes");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"balance\":100", "computed under condition");
    free(resp); resp = NULL;

    /* 8. Computed + return_record agree. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"balance\":{\"$incr\":1}},"
        "\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"record\":", "record returned");
    ASSERT_CONTAINS(resp, "\"balance\":101", "record shows computed value");
    free(resp); resp = NULL;

    /* 9. D8 boundary — computed spans outside single update reject. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c3\",\"value\":{\"balance\":{\"$incr\":1}}}", &resp);
    ASSERT_CONTAINS(resp, "only valid on update", "insert rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"cp\","
                   "\"key\":\"c3\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "c3 was never written");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"cp\",\"records\":[{\"key\":\"c4\","
        "\"value\":{\"balance\":{\"$incr\":1}}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"errors\":1", "bulk-insert record rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"bulk-update\",\"dir\":\"default\","
        "\"object\":\"cp\",\"criteria\":[{\"field\":\"balance\",\"op\":\"eq\","
        "\"value\":100}],\"value\":{\"balance\":{\"$incr\":1}}}", &resp);
    ASSERT_CONTAINS(resp, "only valid on update", "criteria bulk rejected");
    free(resp); resp = NULL;

    /* 10. Negative int/short decode and decimal padding regressions. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"shares\":-1,\"tiny\":-1,\"price\":\"0.00\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"cp\","
        "\"key\":\"c1\",\"value\":{\"shares\":{\"$incr\":1},\"tiny\":{\"$incr\":1},"
        "\"price\":{\"$incr\":\"1.5\"},\"note\":\"{hello}\"},\"return_record\":true}", &resp);
    ASSERT_CONTAINS(resp, "\"shares\":0", "negative int increment");
    ASSERT_CONTAINS(resp, "\"tiny\":0", "negative short increment");
    ASSERT_CONTAINS(resp, "1.50", "fraction padded to scale");
    ASSERT_CONTAINS(resp, "{hello}", "varchar brace stays literal");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-computed-update", test_computed_update_run)
```

**Implementation:**

1. **storage.c** — beside `V2UpdateCtx`, new file-local type:

```c
/* Parsed computed-field spec for one update (conditional-writes F1).
   op: 0 = none, 1 = $incr, 2 = $copy. */
typedef struct {
    int  op;
    char literal[128];   /* $incr: literal span, quotes stripped */
    int  src_idx;        /* $copy: sibling TypedField index */
} V2FieldCompute;
```

2. **storage.c new static helpers** (insert immediately before
   `static int v2_update_new_from_old(`). The span parser uses the
   JSON parser — no hand-rolled pointer walking:

```c
/* Parse a raw value span that starts with '{' into a computed-field
   spec. Exactly one of $incr / $copy must be present; any other member
   is rejected. Returns 0 and fills *out; on failure returns -1 with a
   whole-request error in err_buf. is_computed marks fields already
   parsed as computed in this request — a $copy source may not name
   one. */
static int parse_computed_span(const TypedSchema *ts,
                               const char *field_name, const char *span,
                               const uint8_t *is_computed,
                               V2FieldCompute *out,
                               char *err_buf, size_t err_sz) {
    JsonObj o;
    if (json_parse_object(span, strlen(span), &o) != 1 || o.n != 1)
        goto malformed;
    char *incr = json_obj_strdup_raw(&o, "$incr");
    char *copy = json_obj_strdup_raw(&o, "$copy");
    if (incr && copy) {
        free(incr); free(copy);
        snprintf(err_buf, err_sz,
                 "field '%s': computed value must hold exactly one of "
                 "$incr / $copy", field_name);
        return -1;
    }
    if (!incr && !copy) {
        snprintf(err_buf, err_sz,
                 "field '%s': computed value must hold exactly one of "
                 "$incr / $copy", field_name);
        return -1;
    }
    memset(out, 0, sizeof(*out));
    int fi = typed_field_index((TypedSchema *)ts, field_name);
    if (fi < 0) { free(incr); free(copy); goto malformed; }
    const TypedField *f = &ts->fields[fi];
    int unsupported = f->type != FT_SHORT && f->type != FT_INT &&
                      f->type != FT_LONG && f->type != FT_NUMERIC &&
                      f->type != FT_FLOAT && f->type != FT_DOUBLE;
    if (unsupported) {
        free(incr); free(copy);
        snprintf(err_buf, err_sz,
                 "field '%s': unsupported type for computed update",
                 f->name);
        return -1;
    }
    if (incr) {
        out->op = 1;
        size_t n = strlen(incr);
        if (n >= 2 && incr[0] == '"' && incr[n - 1] == '"') {
            memmove(incr, incr + 1, n - 2);
            incr[n - 2] = '\0';
        }
        if (!incr[0] || strlen(incr) >= sizeof(out->literal)) {
            free(incr); free(copy);
            snprintf(err_buf, err_sz,
                     "field '%s': bad computed literal", f->name);
            return -1;
        }
        memcpy(out->literal, incr, strlen(incr) + 1);
    } else {
        out->op = 2;
        size_t n = strlen(copy);
        char src[256]; char *decoded = NULL; size_t dlen = 0;
        if (n < 2 || copy[0] != '"' || copy[n - 1] != '"' ||
            json_unescape_cstring(copy + 1, n - 2, &decoded, &dlen) != 0 ||
            dlen >= sizeof(src)) {
            free(decoded); free(incr); free(copy); goto malformed;
        }
        memcpy(src, decoded, dlen); src[dlen] = '\0'; free(decoded);
        int si = typed_field_index((TypedSchema *)ts, src);
        if (si < 0) {
            free(incr); free(copy);
            snprintf(err_buf, err_sz,
                     "field '%s': $copy source '%s' is not a field",
                     f->name, src);
            return -1;
        }
        if (is_computed && is_computed[si]) {
            free(incr); free(copy);
            snprintf(err_buf, err_sz,
                     "field '%s': $copy source '%s' is itself computed",
                     f->name, src);
            return -1;
        }
        const TypedField *sf = &ts->fields[si];
        if (sf->type != f->type || sf->size != f->size ||
            (f->type == FT_NUMERIC &&
             sf->numeric_scale != f->numeric_scale)) {
            free(incr); free(copy);
            snprintf(err_buf, err_sz,
                     "field '%s': $copy type mismatch with '%s'",
                     f->name, src);
            return -1;
        }
        out->src_idx = si;
    }
    free(incr);
    free(copy);
    return 0;
malformed:
    snprintf(err_buf, err_sz,
             "field '%s': malformed computed value object", field_name);
    return -1;
}
```

`json_parse_object` returns the member count on success (types.h's
contract and util.c's implementation). Exactly one member is mandatory;
with the exactly-one-known-key check above this rejects unknown and
duplicate members without a new JSON helper.

3. **storage.c applier** — final bytes written directly, checked
   arithmetic throughout:

```c
/* Apply parsed computed fields to NEW (out_value), reading currents
   from the lock-protected OLD. Called after
   update_compose_new_from_old (computed slots carry OLD then) and
   before auto_update stamping, inside the same kf-wrlock hold as the
   CAS check. Results are written as final field bytes: BE int64 for
   integer/numeric, host order for float/double (the record layout's
   own convention). Returns 0, or -1 with err_buf set — the caller
   aborts the whole update (nothing has been written yet). */
static int apply_computed_fields(const TypedSchema *ts,
                                 const SlotcaskOldRecord *old,
                                 uint8_t *out_value,
                                 const V2FieldCompute *comp,
                                 char *err_buf, size_t err_sz) {
    const uint8_t *o_base; const uint8_t *o_bm; int o_bm_bytes;
    size_t o_region;
    if (typed_record_view(old->value, (size_t)old->vlen, old->hdr2,
                          &o_base, &o_bm, &o_bm_bytes, &o_region) != 0) {
        snprintf(err_buf, err_sz, "corrupt record header on update target");
        return -1;
    }
    int nbm_bytes = typed_nullmap_bytes(ts->nfields);
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed || comp[i].op == 0) continue;
        const TypedField *f = &ts->fields[i];
        uint8_t *dst = out_value + ts->nullmap_prefix + (size_t)f->offset;
        const uint8_t *src = ((size_t)f->offset + (size_t)f->size > o_region)
            ? NULL : o_base + f->offset;
        uint8_t cur[16];
        memset(cur, 0, sizeof(cur));
        if (src) memcpy(cur, src, (size_t)f->size);
        int old_is_null = (old->hdr2 & SREC_FLAG_NULLMAP)
            ? typed_field_null(o_bm, o_bm_bytes, i)
            : legacy_field_absent(f, cur);
        if (comp[i].op == 2) {
            int si = comp[i].src_idx;
            const TypedField *sf = &ts->fields[si];
            const uint8_t *ssrc =
                ((size_t)sf->offset + (size_t)sf->size > o_region)
                ? NULL : o_base + sf->offset;
            uint8_t source_bytes[16] = {0};
            if (ssrc) memcpy(source_bytes, ssrc, (size_t)sf->size);
            int src_null = (old->hdr2 & SREC_FLAG_NULLMAP)
                ? typed_field_null(o_bm, o_bm_bytes, si)
                : legacy_field_absent(sf, source_bytes);
            if (src_null) {
                snprintf(err_buf, err_sz,
                         "field '%s' is NULL; cannot apply computed update",
                         sf->name);
                return -1;
            }
            memcpy(dst, source_bytes, (size_t)f->size);
            typed_clear_null(out_value + 1, nbm_bytes, i);
            continue;
        }
        /* $incr */
        if (old_is_null) {
            snprintf(err_buf, err_sz,
                     "field '%s' is NULL; cannot apply computed update",
                     f->name);
            return -1;
        }
        if (f->type == FT_SHORT || f->type == FT_INT || f->type == FT_LONG) {
            int w = f->size;
            uint64_t raw = 0;
            for (int b = 0; b < w; b++) raw = (raw << 8) | cur[b];
            /* Extend the sign in unsigned space before copying bits.
               Shifting unsigned left then right would ZERO-extend. */
            if (w < 8 && (cur[0] & 0x80)) raw |= UINT64_MAX << (w * 8);
            int64_t v;
            memcpy(&v, &raw, sizeof(v));
            errno = 0;
            char *end = NULL;
            long long d = strtoll(comp[i].literal, &end, 10);
            if (end == comp[i].literal || *end || errno == ERANGE) goto badliteral;
            int64_t r;
            if (__builtin_add_overflow(v, (int64_t)d, &r)) goto overflow;
            int64_t hi = (w == 2) ? INT16_MAX
                       : (w == 4) ? INT32_MAX : INT64_MAX;
            int64_t lo = (w == 2) ? INT16_MIN
                       : (w == 4) ? INT32_MIN : INT64_MIN;
            if (r < lo || r > hi) goto overflow;
            uint64_t be = (uint64_t)r;
            for (int b = 0; b < w; b++)
                dst[b] = (uint8_t)(be >> (8 * (w - 1 - b)));
            typed_clear_null(out_value + 1, nbm_bytes, i);
        } else if (f->type == FT_NUMERIC) {
            uint64_t raw = 0;
            for (int b = 0; b < 8; b++) raw = (raw << 8) | cur[b];
            int64_t v = (int64_t)raw;
            const char *s = comp[i].literal;
            int neg = 0;
            if (*s == '-') { neg = 1; s++; }
            else if (*s == '+') s++;
            const char *dot = strchr(s, '.');
            size_t intlen = dot ? (size_t)(dot - s) : strlen(s);
            size_t frlen  = dot ? strlen(dot + 1) : 0;
            if (frlen > (size_t)f->numeric_scale) goto badliteral;
            if (!intlen && !frlen) goto badliteral;
            uint64_t magnitude = 0;
            for (size_t b = 0; b < intlen; b++) {
                if (s[b] < '0' || s[b] > '9') goto badliteral;
                if (__builtin_mul_overflow(magnitude, UINT64_C(10), &magnitude) ||
                    __builtin_add_overflow(magnitude, (uint64_t)(s[b] - '0'),
                                           &magnitude)) goto overflow;
            }
            /* Append fraction, then pad to S. Scale exactly ONCE. */
            for (size_t b = 0; b < (size_t)f->numeric_scale; b++) {
                uint64_t digit = 0;
                if (b < frlen) {
                    if (dot[1 + b] < '0' || dot[1 + b] > '9') goto badliteral;
                    digit = (uint64_t)(dot[1 + b] - '0');
                }
                if (__builtin_mul_overflow(magnitude, UINT64_C(10), &magnitude) ||
                    __builtin_add_overflow(magnitude, digit, &magnitude)) goto overflow;
            }
            uint64_t limit = (uint64_t)INT64_MAX + (neg ? 1u : 0u);
            if (magnitude > limit) goto overflow;
            int64_t scaled = neg
                ? (magnitude == (uint64_t)INT64_MAX + 1u
                    ? INT64_MIN : -(int64_t)magnitude)
                : (int64_t)magnitude;
            int64_t r;
            if (__builtin_add_overflow(v, scaled, &r)) goto overflow;
            uint64_t be = (uint64_t)r;
            for (int b = 0; b < 8; b++)
                dst[b] = (uint8_t)(be >> (8 * (7 - b)));
            typed_clear_null(out_value + 1, nbm_bytes, i);
        } else {   /* FT_FLOAT / FT_DOUBLE */
            double v;
            if (f->type == FT_FLOAT) {
                float fv = 0.0f;
                memcpy(&fv, cur, 4);
                v = (double)fv;
            } else {
                memcpy(&v, cur, 8);
            }
            errno = 0;
            char *end = NULL;
            double d = strtod(comp[i].literal, &end);
            if (end == comp[i].literal || *end || errno == ERANGE) goto badliteral;
            v += d;
            if (!isfinite(v)) goto overflow;
            if (f->type == FT_FLOAT) {
                float fv = (float)v;
                if (!isfinite(fv)) goto overflow;
                memcpy(dst, &fv, 4);
            } else {
                memcpy(dst, &v, 8);
            }
            typed_clear_null(out_value + 1, nbm_bytes, i);
        }
        continue;
overflow:
        snprintf(err_buf, err_sz,
                 "field '%s': computed update overflows %s", f->name,
                 f->type == FT_NUMERIC ? "numeric" : "integer");
        return -1;
badliteral:
        snprintf(err_buf, err_sz, "field '%s': bad computed literal",
                 f->name);
        return -1;
    }
    return 0;
}
```

(Executor: `#include <errno.h>` and `<math.h>` (for isfinite) if not
already present in storage.c's includes.)

4. **storage.c `v2_update_new_from_old`** — replace its entire definition
(anchor `static int v2_update_new_from_old(const SlotcaskOldRecord *old,`)
with the following complete post-Task-2 body. Operation detection reads
raw JSON spans, so a literal varchar such as `"{hello}"` stays a literal.
Compute-source validation is schema-order independent (mark all computed
fields before parsing any operation).

```c
static int v2_update_new_from_old(const SlotcaskOldRecord *old,
                                  uint8_t *out_value, size_t out_capacity,
                                  size_t *out_vlen, void *ctx_ptr) {
    V2UpdateCtx *c = ctx_ptr;
    if (!old || !out_value || !out_vlen || old->vlen > out_capacity ||
        (size_t)c->idx_ts->total_size > out_capacity) return -1;
    c->saved_old_value = old->value;
    c->saved_old_vlen = old->vlen;
    c->old_hdr2 = old->hdr2;
#ifdef TEST_BUILD
    slotcask_test_after_old(1);
#endif
    JsonObj patch;
    if (json_parse_object(c->partial_json, strlen(c->partial_json), &patch) < 0) {
        snprintf(c->err_buf, sizeof(c->err_buf), "invalid update value");
        return -1;
    }
    V2FieldCompute comp[MAX_FIELDS] = {0};
    uint8_t is_computed[MAX_FIELDS] = {0};
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        const char *v; size_t n;
        if (!c->idx_ts->fields[i].removed &&
            json_obj_get(&patch, c->idx_ts->fields[i].name, &v, &n) && n && *v == '{')
            is_computed[i] = 1;
    }
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        if (!is_computed[i]) continue;
        char *span = json_obj_strdup_raw(&patch, c->idx_ts->fields[i].name);
        if (!span) { snprintf(c->err_buf, sizeof(c->err_buf), "oom"); return -1; }
        int rc = parse_computed_span(c->idx_ts, c->idx_ts->fields[i].name,
                                     span, is_computed, &comp[i],
                                     c->err_buf, sizeof(c->err_buf));
        free(span);
        if (rc != 0) return -1;
    }
    const char *names[MAX_FIELDS];
    enum FieldType types[MAX_FIELDS];
    char *values[MAX_FIELDS] = {0}; uint8_t nulls[MAX_FIELDS] = {0};
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        names[i] = c->idx_ts->fields[i].name;
        types[i] = c->idx_ts->fields[i].type;
    }
    int parse_rc = json_get_fields_null_aware(c->partial_json, names,
                         c->idx_ts->nfields, types, values, nulls);
    if (parse_rc != 0) {
        snprintf(c->err_buf, sizeof(c->err_buf), "malformed JSON escape in field values");
        goto reject;
    }
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        const TypedField *f = &c->idx_ts->fields[i];
        if (f->removed) continue;
        if (comp[i].op) { free(values[i]); values[i] = NULL; continue; }
        if (f->type == FT_VARCHAR && values[i] && strlen(values[i]) > (size_t)f->size - 2) {
            snprintf(c->err_buf, sizeof(c->err_buf),
                     "value for field '%s' is %zu bytes; exceeds max %d for varchar",
                     f->name, strlen(values[i]), f->size - 2);
            goto reject;
        }
        if (nulls[i] && f->not_null) {
            fmt_not_null_err(f, c->err_buf, sizeof(c->err_buf)); goto reject;
        }
    }
    if (update_compose_new_from_old(c->idx_ts, old, out_value, values, nulls) != 0) {
        snprintf(c->err_buf, sizeof(c->err_buf), "corrupt record header on update target");
        goto reject;
    }
    for (int i = 0; i < c->idx_ts->nfields; i++) { free(values[i]); values[i] = NULL; }
    if (apply_computed_fields(c->idx_ts, old, out_value, comp,
                              c->err_buf, sizeof(c->err_buf)) != 0) return -1;
    update_stamp_auto_update_fields(c->idx_ts, out_value);
    *out_vlen = (size_t)c->idx_ts->total_size;
    if (c->return_record) {
        c->ret_buf = malloc(*out_vlen);
        if (c->ret_buf) memcpy(c->ret_buf, out_value, *out_vlen);
    }
    return 0;
reject:
    for (int i = 0; i < c->idx_ts->nfields; i++) free(values[i]);
    return -1;
}
```

5. **D8 enforcement — insert and bulk-insert without another parse.**
In `config.c:typed_encode_defaults`, at this exact anchor:

```c
                const char *ev = vstart; size_t el = vlen;
```
insert immediately before it:

```c
                if (vlen && vstart[0] == '{') {
                    if (err_buf && err_buf_size)
                        snprintf(err_buf, err_buf_size,
                                 "computed updates are only valid on update");
                    return -2;
                }
```

This uses the raw JSON span already in hand: no second JSON parse,
per-field allocations or additional payload buffer on ordinary inserts.
Quoted varchar values beginning with `{` are unaffected. The single
insert path already frees `typed_buf` on -2; bulk-insert already counts
-2 in `errors` and skips the record. Preserve those conventions: bulk
requests are best-effort, so a computed record is rejected individually,
not rolled back with its neighbors. The bulk response reports `errors`,
not necessarily the detailed single-insert error string.

6. **D8 enforcement — bulk-update.** In the shared
`update_compose_new_from_old`, anchor:

```c
        if (field_vals && field_vals[i]) {
            encode_field(f, field_vals[i], dst);   /* bit already clear */
```
replace with:

```c
        if (field_vals && field_vals[i]) {
            if (f->type != FT_VARCHAR && field_vals[i][0] == '{') return -2;
            encode_field(f, field_vals[i], dst);   /* bit already clear */
```

Single update's validated computed slots are NULL before composing, so
this cannot reject a valid single computed write. Existing bulk callbacks
reject nonzero composer returns before staging a segment/marker. It is a
per-record skip/error in the JSON bulk form, which remains best-effort.
Delimited numeric cells beginning `{` also reject, rather than silently
encoding zero. Quoted varchar contents stay literal.

The per-record JSON and delimited callbacks pass NULL patches to the
composer, then encode their own cells; they need their own rejection:

In `v2_bulk_upd_json_value_compute`, anchor:

```c
        if (json_rec->field_values[i]) {
            encode_field(&w->ts->fields[tidx], json_rec->field_values[i],
```
replace with:

```c
        if (json_rec->field_values[i]) {
            if (w->ts->fields[tidx].type != FT_VARCHAR &&
                json_rec->field_values[i][0] == '{') return -1;
            encode_field(&w->ts->fields[tidx], json_rec->field_values[i],
```

In `v2_bulk_upd_delim_value_compute`, anchor:

```c
            if (!w->ts->fields[tidx].removed) {
                char tmpcell[256];
```
replace with:

```c
            if (!w->ts->fields[tidx].removed) {
                if (w->ts->fields[tidx].type != FT_VARCHAR && *cell_start == '{')
                    return -1;
                char tmpcell[256];
```

Both return before staging this record. Existing worker accounting handles
the rejected record; it is not a successful numeric zero write.

For criteria-form `cmd_bulk_update`, reject the shared computed patch
before scanning/dry-run/fan-out. Immediately after the existing anchor:

```c
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
```
insert:

```c
    JsonObj patch;
    if (json_parse_object(value_json, strlen(value_json), &patch) < 0) {
        free_criteria_tree(cas_tree); free_criteria_tree(tree);
        OUT("{\"error\":\"invalid update value\"}\n"); return 1;
    }
    for (int i = 0; i < fs.ts->nfields; i++) {
        const char *v; size_t n;
        if (!fs.ts->fields[i].removed &&
            json_obj_get(&patch, fs.ts->fields[i].name, &v, &n) && n && *v == '{') {
            free_criteria_tree(cas_tree); free_criteria_tree(tree);
            OUT("{\"error\":\"computed updates are only valid on update\"}\n");
            return 1;
        }
    }
```

**Consumer inventory for these return paths:**
`typed_encode_defaults` has exactly two call sites: storage.c's
`cmd_insert_v2` and query_bulk.c's JSON insertion parser. Its declaration
is in types.h, its definition in config.c. No embedded/test caller or
signature change exists in the current tree. The composer callers are `v2_update_new_from_old`,
`v2_bulk_upd_value_compute`, `v2_bulk_upd_delim_value_compute`,
`v2_bulk_upd_json_value_compute`: each already rejects nonzero before a
write; retain that error/skip accounting. No partial NEW bytes are
published on an error. Add tests proving neighboring ordinary records
still commit when one bulk record is rejected.

Run the case → paste failing then green. Full suite → green.

---

### Task 4 — F2: reader-preserving writer reservation

**How this serves the primary goal:** stabilize all same-object records
between the guard and mutation without taking exclusive objlock. This
prerequisite task defines the APIs consumed by Task 5. Source modifications
remain plan-only here.

**Audited entry points:** `writer_gate_lock` is used by pregrow workers
(`slotcask.c:2352`), deferred shard pipelines (`:8154`) and the central
ordinary/legacy mutation transaction (`:8396`). Pregrow/deferred tasks hold
one gate; the central transaction takes sorted gates and releases in
reverse. Its single-record waves execute on the caller (`n == 1`). Schema,
compact and rebuild changes remain behind exclusive objlock. Do not add
an independent writer mutex or skip these existing entry points.

**parallel.c / types.h.** Insert after the exact anchor
`static __thread int t_in_pool_worker = 0;` an explicit scoped inline depth; declare these functions beside `parallel_for` in
`types.h`. The scope affects BOTH CPU and IO dispatch, including nested
calls, without changing whether a thread is actually a pool worker:

```c
static __thread unsigned t_parallel_inline_depth;

void parallel_inline_enter(void) {
    t_parallel_inline_depth++;
}
void parallel_inline_leave(void) {
    if (!t_parallel_inline_depth) abort();
    t_parallel_inline_depth--;
}
```

Declarations:

```c
void parallel_inline_enter(void);
void parallel_inline_leave(void);
```

In `parallel_for`, change its existing inline condition to:

```c
if (!g_pool_running || n == 1 || t_in_pool_worker || t_parallel_inline_depth) {
```

In `parallel_for_io`, change its existing inline condition to:

```c
if (!g_io_running || n == 1 || t_in_pool_worker || t_parallel_inline_depth) {
```

Keep each existing inline loop/body unchanged. This adds only a TLS check
on ordinary dispatch; it creates no worker or lock.

**slotcask.h.** Insert after the `struct SlotcaskDb` closing `};`
immediately following the exact member anchor
`    size_t writer_gate_waiters_inited;`:


```c
/* Internal, request-thread-only scope; zero-initialize before begin.
 * Caller holds shared objlock and a registry pin through end.
 * Begin/end must run on the same thread. No nested reservation. */
typedef struct {
    SlotcaskDb *db;
    size_t acquired;
    unsigned char *borrowed;
} SlotcaskWriteReservation;
int slotcask_write_reservation_begin(SlotcaskDb *db,
                                      SlotcaskWriteReservation *r);
void slotcask_write_reservation_end(SlotcaskWriteReservation *r);
int slotcask_write_reservation_held(SlotcaskDb *db);
```

**slotcask.c.** Insert before the exact definition anchor
`static void writer_gate_lock(SlotcaskDb *db, int kf_shard) {` the following. Existing
`assert.h` is already included. Use direct pthread acquisition here; the
ordinary wrapper's instrumentation continues to measure ordinary pipeline
ownership, not this deliberately all-shard reservation.

```c
static __thread SlotcaskWriteReservation *t_write_reservation;

int slotcask_write_reservation_held(SlotcaskDb *db) {
    return t_write_reservation && t_write_reservation->db == db &&
           t_write_reservation->acquired == (size_t)db->num_shards;
}

int slotcask_write_reservation_begin(SlotcaskDb *db,
                                      SlotcaskWriteReservation *r) {
    if (!db || !r || r->db || r->borrowed || t_write_reservation ||
        db->num_shards <= 0 ||
        db->writer_gates_inited != (size_t)db->num_shards)
        return -1;
    unsigned char *borrowed = calloc((size_t)db->num_shards, 1);
    if (!borrowed) return -1;
    /* Enter BEFORE acquisition; never enqueue work while holding any
       reserved gate, including during recovery or nested mutation. */
    parallel_inline_enter();
    size_t acquired = 0;
    for (;;) {
        int lock_rc = 0;
        for (acquired = 0; acquired < (size_t)db->num_shards; acquired++) {
            lock_rc = pthread_mutex_trylock(&db->writer_gates[acquired]);
            if (lock_rc != 0) break;
        }
        if (acquired == (size_t)db->num_shards) break;
        /* Never wait holding a partial reservation: an older multi-shard
           transaction may need pool workers currently waiting on us. */
        while (acquired) pthread_mutex_unlock(&db->writer_gates[--acquired]);
        if (lock_rc != EBUSY) {
            parallel_inline_leave();
            free(borrowed);
            return -1;
        }
        struct timespec retry = { .tv_sec = 0, .tv_nsec = 1000000 };
        nanosleep(&retry, NULL);
    }
    r->db = db;
    r->acquired = acquired;
    r->borrowed = borrowed;
    t_write_reservation = r;
    return 0;
}

void slotcask_write_reservation_end(SlotcaskWriteReservation *r) {
    if (!r || !r->db) return;
    /* Contract violations must never silently unlock another thread's
       mutex or permit a write outside its reservation. */
    if (t_write_reservation != r) abort();
    for (size_t i = 0; i < r->acquired; i++)
        if (r->borrowed[i]) abort();
    SlotcaskDb *db = r->db;
    t_write_reservation = NULL;
    while (r->acquired)
        pthread_mutex_unlock(&db->writer_gates[--r->acquired]);
    free(r->borrowed);
    *r = (SlotcaskWriteReservation){0};
    parallel_inline_leave();
}
```

Insert at the START of `writer_gate_lock`, before its existing TEST_BUILD
block. Preserve all existing code after this addition:

```c
    if (slotcask_write_reservation_held(db)) {
        if (kf_shard < 0 || kf_shard >= db->num_shards ||
            t_write_reservation->borrowed[kf_shard]) abort();
        t_write_reservation->borrowed[kf_shard] = 1;
        return;
    }
```

Insert at the START of `writer_gate_unlock`, likewise preserving the
ordinary instrumentation and actual unlock below it:

```c
    if (slotcask_write_reservation_held(db)) {
        if (kf_shard < 0 || kf_shard >= db->num_shards ||
            !t_write_reservation->borrowed[kf_shard]) abort();
        t_write_reservation->borrowed[kf_shard] = 0;
        return;
    }
```

Only the matching registry instance on this thread may borrow; other
threads still block in the ordinary wrapper. No borrowed unlock releases
a physical mutex. Nested reservations reject; sequential reuse after end
works. The server owns the outer registry pin until AFTER end.

**Recovery and strict-walk contracts.** The comment above
`marker_recovery_sweep_object` currently requires exclusive objlock.
Broaden that exact contract in both definition and header: startup or
exclusive objlock remains valid; live use requires shared objlock, a
pinned same-instance registry handle and every writer gate reserved.
Likewise broaden `slotcask_walk_live_strict`'s exclusive-lock precondition
to allow this reserved live context. No lenient read API is changed.
Replay acquires kfcache write handles per affected shard and invokes
`storage_recovery_index_diff`; that callback takes no writer gate.

The server MUST call this sweep once after reserving gates and before
search. Ordinary mutations otherwise replay only the touched shard: an
old retained marker on another shard could invalidate an absence proof.
A corrupt/unreadable/unreplayable marker rejects the requested write.
Recovery may finish an earlier durable write even when this new request
later fails its guard; document this distinction. Readers retain existing
per-shard recovery visibility; no object-wide snapshot is promised.

**Test-first — complete fresh-exec harness and registered test.** Add
both files below and the build hunks before adding production declarations.
The pre-change build fails on the missing reservation/inline APIs; after
adding the implementation, run `test-write-reservation`, then the full
suite. Removing the inline-depth dispatch checks must make the saturation
child time out; replacing try/release acquisition with blocking ascending
acquisition must fail the partial-reservation child. Paste both expected
red regressions and the restored green output.

The helper uses its own initialized `ShardDb` in a unique temporary CWD
(no inherited db.env), and the parent always execs it in a fresh process.
It therefore supports `--jobs 1` without modifying the runner's pools or
cache state. The parent kills and reaps a hung child; it never detaches a
thread holding pointers into a returning stack frame. No benchmark runs.

**build.sh — quoted insertion anchor** (the complete existing stanza):

```sh
gcc $MODE_CFLAGS -o build/bin/embedded_bg_harness \
    src/test/embedded_bg_harness.c build/bin/libshard-db.a \
    -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS \
    -lpthread -lssl -lcrypto
```

Insert immediately after it:

```sh
gcc $MODE_CFLAGS -o build/bin/reservation_harness \
    src/test/reservation_harness.c build/bin/libshard-db.a \
    -Isrc/db $OSSL_CFLAGS $OSSL_LDFLAGS $MODE_LDFLAGS \
    -lpthread -lssl -lcrypto
```

In the test source list, insert after the exact existing-after-Task-3 line:

```
    src/test/cases/test_computed_update.c \
```

this complete line:

```
    src/test/cases/test_write_reservation.c \
```

**src/test/reservation_harness.c — complete new file:**

```c
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "shard_db.h"
#include "types.h"
#include "slotcask.h"
#include "shard_db_internal.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define NEED(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); return 1; } } while (0)
static void tick(void) { struct timespec t = {0, 1000000}; nanosleep(&t, NULL); }
static int wait_count(atomic_int *p, int want) {
    uint64_t until = now_ms_coarse() + 5000;
    while (atomic_load(p) < want && now_ms_coarse() < until) tick();
    return atomic_load(p) >= want;
}
static int remove_entry(const char *p, const struct stat *st, int type, struct FTW *f) {
    (void)st; (void)type; (void)f; return remove(p);
}
typedef struct { SlotcaskDb db; ShardDb *owner; int object_locked; char base[PATH_MAX]; } Fixture;
static int open_fixture(Fixture *f) {
    snprintf(f->base, sizeof(f->base), "/tmp/shard-reservation-XXXXXX");
    if (!mkdtemp(f->base) || chdir(f->base)) return -1;
    f->owner = shard_db_open_internal(f->base);
    if (!f->owner) return -1;
    g_shard_db_instance = f->owner;
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/data", f->base);
    if (mkdir(path, 0755)) return -1;
    snprintf(path, sizeof(path), "%s/data/kf", f->base);
    if (mkdir(path, 0755)) return -1;
    if (slotcask_open(&f->db, f->base, 8, 1, 64)) return -1;
    if (objlock_rdlock(f->base, "raw")) return -1;
    f->object_locked = 1;
    return 0;
}
static void close_fixture(Fixture *f) {
    slotcask_close(&f->db);
    if (f->object_locked) objlock_rdunlock(f->base, "raw");
    shard_db_close(f->owner);
    chdir("/");
    nftw(f->base, remove_entry, 32, FTW_DEPTH | FTW_PHYS);
}
static void shard_key(char out[32], int shard) {
    for (unsigned i = 0; ; i++) {
        snprintf(out, 32, "key-%u", i);
        uint8_t hash[16]; compute_hash_raw(out, strlen(out), hash);
        if (compute_record_shard(hash, 8) == shard) return;
    }
}
static int put(SlotcaskDb *db, const char *key, const char *value) {
    SlotcaskUpsertOpts opts = {0}; SlotcaskUpsertResult result = {0};
    int rc = slotcask_upsert_with_hooks(db, -1, key, strlen(key), value,
                                        strlen(value), &opts, &result);
    free(result.current_value); return rc;
}
static int count_row(const uint8_t h[16], const void *k, size_t kn,
                      const void *v, size_t vn, uint8_t hdr, void *ctx) {
    (void)h; (void)k; (void)kn; (void)v; (void)vn; (void)hdr;
    (*(int *)ctx)++; return 0;
}
static int lifecycle(void) {
    Fixture f = {0}; NEED(open_fixture(&f) == 0);
    SlotcaskWriteReservation r = {0}, nested = {0}; SlotcaskDb other = {0};
    NEED(slotcask_write_reservation_begin(&f.db, &r) == 0);
    NEED(slotcask_write_reservation_held(&f.db));
    NEED(!slotcask_write_reservation_held(&other));
    NEED(slotcask_write_reservation_begin(&f.db, &nested) == -1);
    NEED(put(&f.db, "borrow", "value") == 0); /* nested gate borrowing */
    void *value = NULL; size_t len = 0;
    NEED(slotcask_get_ex(&f.db, "borrow", 6, &value, &len, NULL) == 0);
    NEED(len == 5 && !memcmp(value, "value", 5)); free(value);
    int rows = 0; NEED(slotcask_walk_live_strict(&f.db, count_row, &rows) == 0);
    NEED(rows == 1);
    slotcask_write_reservation_end(&r);
    NEED(!slotcask_write_reservation_held(&f.db));
    slotcask_write_reservation_end(&r); /* inactive cleanup is harmless */
    for (int i = 0; i < f.db.num_shards; i++) {
        NEED(pthread_mutex_trylock(&f.db.writer_gates[i]) == 0);
        pthread_mutex_unlock(&f.db.writer_gates[i]);
    }
    NEED(slotcask_write_reservation_begin(&f.db, &r) == 0);
    slotcask_write_reservation_end(&r);
    close_fixture(&f); return 0;
}

typedef struct {
    SlotcaskDb *db;
    int io;
    atomic_int *waiting;
    atomic_int *done;
} Blocker;
static void *block_gate(void *p) {
    Blocker *b = p;
    atomic_fetch_add(b->waiting, 1);
    pthread_mutex_lock(&b->db->writer_gates[0]);
    pthread_mutex_unlock(&b->db->writer_gates[0]);
    atomic_fetch_add(b->done, 1); return NULL;
}
static void *dispatch_blockers(void *p) {
    Blocker *b = p;
    Blocker jobs[2] = {*b, *b};
    if (b->io) parallel_for_io(block_gate, jobs, 2, sizeof(jobs[0]));
    else parallel_for(block_gate, jobs, 2, sizeof(jobs[0]));
    return NULL;
}
typedef struct { pthread_t owner; atomic_int *count, *wrong; } Probe;
static void *probe(void *p) {
    Probe *a = p;
    if (!pthread_equal(pthread_self(), a->owner)) atomic_store(a->wrong, 1);
    atomic_fetch_add(a->count, 1); return NULL;
}
static void *nested_io(void *p) {
    Probe jobs[2] = {*(Probe *)p, *(Probe *)p};
    probe(p); parallel_for_io(probe, jobs, 2, sizeof(jobs[0])); return NULL;
}
static void *nested_cpu(void *p) {
    Probe jobs[2] = {*(Probe *)p, *(Probe *)p};
    probe(p); parallel_for(probe, jobs, 2, sizeof(jobs[0])); return NULL;
}
static int saturation(void) {
    Fixture f = {0}; NEED(open_fixture(&f) == 0);
    parallel_pool_init(2); parallel_io_pool_init(2);
    NEED(put(&f.db, "seed", "seed") == 0);
    SlotcaskWriteReservation r = {0};
    NEED(slotcask_write_reservation_begin(&f.db, &r) == 0);
    atomic_int waiting = 0, done = 0, count = 0, wrong = 0;
    Blocker cpu = {&f.db, 0, &waiting, &done}, io = {&f.db, 1, &waiting, &done};
    pthread_t cpu_thread, io_thread;
    NEED(!pthread_create(&cpu_thread, NULL, dispatch_blockers, &cpu));
    NEED(!pthread_create(&io_thread, NULL, dispatch_blockers, &io));
    NEED(wait_count(&waiting, 4)); /* every worker is waiting on our gate */
    Probe jobs[2] = {{pthread_self(), &count, &wrong}, {pthread_self(), &count, &wrong}};
    parallel_for(nested_io, jobs, 2, sizeof(jobs[0]));
    parallel_for_io(nested_cpu, jobs, 2, sizeof(jobs[0]));
    NEED(atomic_load(&count) == 12 && !atomic_load(&wrong));
    int rows = 0;
    NEED(slotcask_walk_live_strict(&f.db, count_row, &rows) == 0 && rows == 1);
    NEED(put(&f.db, "target", "target") == 0);
    NEED(atomic_load(&done) == 0);
    slotcask_write_reservation_end(&r);
    pthread_join(cpu_thread, NULL); pthread_join(io_thread, NULL);
    NEED(atomic_load(&done) == 4);
    parallel_io_pool_shutdown(); parallel_pool_shutdown();
    close_fixture(&f); return 0;
}

typedef struct { SlotcaskDb *db; atomic_int started, acquired; } Pending;
static void *reserve_later(void *p) {
    Pending *a = p; SlotcaskWriteReservation r = {0};
    atomic_store(&a->started, 1);
    if (!slotcask_write_reservation_begin(a->db, &r)) {
        atomic_store(&a->acquired, 1); slotcask_write_reservation_end(&r);
    }
    return NULL;
}
static int partial(void) {
    Fixture f = {0}; NEED(open_fixture(&f) == 0);
    pthread_mutex_lock(&f.db.writer_gates[1]);
    Pending p = {.db = &f.db}; pthread_t thread;
    NEED(!pthread_create(&thread, NULL, reserve_later, &p));
    NEED(wait_count(&p.started, 1));
    struct timespec delay = {0, 100000000}; nanosleep(&delay, NULL);
    /* A blocking ascending acquisition retains gate 0 forever here.
       The release/retry design lets this older writer use it. */
    int got = 0;
    for (int i = 0; i < 1000; i++) {
        if (!pthread_mutex_trylock(&f.db.writer_gates[0])) { got = 1; break; }
        tick();
    }
    NEED(got && !atomic_load(&p.acquired));
    pthread_mutex_unlock(&f.db.writer_gates[0]);
    pthread_mutex_unlock(&f.db.writer_gates[1]);
    pthread_join(thread, NULL); NEED(atomic_load(&p.acquired));
    close_fixture(&f); return 0;
}

static int prepare(SlotcaskBulkRec *r, const size_t *a, size_t n, void *ctx, void **state) {
    (void)r; (void)a; (void)n; (void)ctx; *state = NULL; return 0;
}
static int fail_apply(SlotcaskBulkRec *r, const size_t *a, size_t n, void *ctx, void *state) {
    (void)r; (void)a; (void)n; (void)ctx; (void)state; return -1;
}
static void terminal(void *ctx, void *state) { (void)ctx; (void)state; }
static int recovery_diff(const char *root, const char *object, int shard, uint32_t slot,
                          const uint8_t *hash, const uint8_t *old, size_t oldn, uint8_t oldh,
                          const uint8_t *newv, size_t newn, uint8_t newh, char *err, size_t cap) {
    (void)root; (void)object; (void)shard; (void)slot; (void)hash;
    (void)old; (void)oldn; (void)oldh; (void)newv; (void)newn; (void)newh;
    (void)err; (void)cap; return 0;
}
static int corrupt_marker(Fixture *f) {
    char dir[PATH_MAX], path[PATH_MAX];
    snprintf(dir, sizeof(dir), "%s/data/kf", f->base);
    DIR *d = opendir(dir); if (!d) return -1;
    struct dirent *e; int result = -1;
    while ((e = readdir(d))) {
        if (!strstr(e->d_name, "_marker.dat")) continue;
        snprintf(path, sizeof(path), "%s/%s", dir, e->d_name);
        int fd = open(path, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
        if (fd < 0) break;
        off_t off = 16 + offsetof(KfMarkerSlot, new_offset);
        uint8_t byte;
        if (pread(fd, &byte, 1, off) == 1) {
            byte ^= 1;
            if (pwrite(fd, &byte, 1, off) == 1 && !fsync(fd)) result = 0;
        }
        close(fd); break;
    }
    closedir(d); return result;
}
static int recovery(int corrupt) {
    Fixture f = {0}; NEED(open_fixture(&f) == 0);
    char pending[32], target[32]; shard_key(pending, 1); shard_key(target, 0);
    SlotcaskBulkRec rec = {.key = pending, .klen = strlen(pending), .value = "pending", .vlen = 7};
    SlotcaskBulkOpts opts = {.has_indexed_fields = 1, .prepare_window = prepare,
        .apply_window = fail_apply, .commit_done = terminal,
        .release_window = terminal, .abort_window = terminal};
    SlotcaskBulkShardInput input = {.kf_shard_id = 1, .recs = &rec, .nrecs = 1,
        .kind = SLOTCASK_BULK_INPUT_UPSERT, .opts.upsert = opts};
    NEED(slotcask_bulk_request_execute(&f.db, &input, 1) == -1);
    NEED(input.rc == -2 && errno == EINPROGRESS);
    NEED(object_has_pending_markers(f.base) == 1);
    if (corrupt) NEED(corrupt_marker(&f) == 0);
    RecoveryIndexDiffFn saved = g_recovery_index_diff_fn;
    g_recovery_index_diff_fn = recovery_diff; /* isolated executable only */
    SlotcaskWriteReservation r = {0}; NEED(slotcask_write_reservation_begin(&f.db, &r) == 0);
    int replayed = 0;
    int rc = marker_recovery_sweep_object(f.base, f.base, "raw", &replayed);
    if (corrupt) {
        NEED(rc == -1 && object_has_pending_markers(f.base) == 1);
        void *value = NULL; size_t len = 0;
        NEED(slotcask_get_ex(&f.db, target, strlen(target), &value, &len, NULL) != 0);
        free(value);
    } else {
        NEED(rc == 0 && replayed == 1 && object_has_pending_markers(f.base) == 0);
        int rows = 0;
        NEED(slotcask_walk_live_strict(&f.db, count_row, &rows) == 0 && rows == 1);
        NEED(put(&f.db, target, "target") == 0);
    }
    slotcask_write_reservation_end(&r);
    NEED(slotcask_write_reservation_begin(&f.db, &r) == 0);
    slotcask_write_reservation_end(&r);
    g_recovery_index_diff_fn = saved;
    close_fixture(&f); return 0;
}

typedef struct { const char *root; atomic_int started, done; } SchemaWait;
static void *schema_writer(void *p) {
    g_db = g_shard_db_instance;
    SchemaWait *a = p; atomic_store(&a->started, 1);
    if (!objlock_wrlock(a->root, "raw")) {
        atomic_store(&a->done, 1); objlock_wrunlock(a->root, "raw");
    }
    return NULL;
}
static int schema_wait(void) {
    Fixture f = {0}; NEED(open_fixture(&f) == 0);
    SlotcaskWriteReservation r = {0}; NEED(slotcask_write_reservation_begin(&f.db, &r) == 0);
    NEED(objlock_rdlock(f.base, "other") == 0);
    objlock_rdunlock(f.base, "other");
    SchemaWait a = {.root = f.base}; pthread_t thread;
    NEED(!pthread_create(&thread, NULL, schema_writer, &a)); NEED(wait_count(&a.started, 1));
    struct timespec delay = {0, 100000000}; nanosleep(&delay, NULL);
    NEED(!atomic_load(&a.done));
    slotcask_write_reservation_end(&r);
    NEED(!atomic_load(&a.done)); /* shared objlock still prevents schema mutation */
    objlock_rdunlock(f.base, "raw"); f.object_locked = 0;
    pthread_join(thread, NULL); NEED(atomic_load(&a.done));
    close_fixture(&f); return 0;
}
int main(int argc, char **argv) {
    if (argc != 2) return 2;
    if (!strcmp(argv[1], "lifecycle")) return lifecycle();
    if (!strcmp(argv[1], "saturation")) return saturation();
    if (!strcmp(argv[1], "partial")) return partial();
    if (!strcmp(argv[1], "recovery")) return recovery(0);
    if (!strcmp(argv[1], "corrupt")) return recovery(1);
    if (!strcmp(argv[1], "schema")) return schema_wait();
    return 2;
}
```

The pool case deliberately blocks ALL CPU and IO workers, uses dispatch
counts of 2 (1 is already inline without this fix), checks request-thread
identity across nested cross-pool calls, performs a strict live walk and
an actual borrowed single write, then releases waiting writers. The marker
cases create real durable retained markers through persistent apply
failure, test replay on shard 1 before authorizing a target on shard 0,
and prove corrupt evidence prevents the target mutation. The recovery
callback substitution exists only in this isolated raw-storage fixture;
normal production replay still calls the real index-diff callback.

**src/test/cases/test_write_reservation.c — complete new file:**

```c
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int reservation_child(const char *mode) {
    char path[PATH_MAX];
    if (!realpath("./build/bin/reservation_harness", path)) return -1;
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (!pid) { execl(path, path, mode, (char *)NULL); _exit(127); }
    for (int i = 0; i < 600; i++) {
        int status; pid_t got = waitpid(pid, &status, WNOHANG);
        if (got == pid) return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        if (got < 0 && errno != EINTR) break;
        struct timespec tick = {0, 50000000}; nanosleep(&tick, NULL);
    }
    kill(pid, SIGKILL);
    while (waitpid(pid, NULL, 0) < 0 && errno == EINTR) {}
    fprintf(stderr, "reservation harness timed out: %s\n", mode);
    return -1;
}
static int test_reservation_run(void) {
    const char *modes[] = {"lifecycle", "partial", "saturation", "recovery", "corrupt", "schema"};
    for (size_t i = 0; i < sizeof(modes) / sizeof(modes[0]); i++) {
        int rc = reservation_child(modes[i]);
        ASSERT_EQ_INT(rc, 0, modes[i]);
    }
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-write-reservation", test_reservation_run)
```

Later guard TCP tests cover error cleanup by issuing another request on
the same object after rejection (a leaked reservation would hit the bounded
socket timeout). The guard race case covers reader progress and ordinary
writer exclusion while reserved. Existing durability tests and final local
sanitizer gates remain mandatory; these isolated tests do not replace them.

---

### Task 5 — F2: object-scope guards

**How this serves the primary goal:** protect the cross-record decision through the write.

**Test** — new file `src/test/cases/test_cas_guards.c`, registered as
`test-cas-guards` (build.sh anchor: after the
`test_write_reservation.c` line). On the pre-task tree it fails at every
guard step (the fields are ignored today, so step 1's second insert
succeeds). Guards resolve once pre-dispatch (rev-2 finding 1); the
null/two-non-null cases are exercised in steps 6-7.

```c
/* src/test/cases/test_cas_guards.c
 *
 * Conditional-writes F2 — if_match / if_no_match object-scope guards.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int test_cas_guards_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gr\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"domain:varchar:40\",\"owner:varchar:20\"]}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create gr");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"kid\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"p:varchar:20\"]}",
        &resp);
    free(resp); resp = NULL;

    /* 1. if_no_match uniqueness: first insert wins, second same-domain
          insert fails with matched:1; count stays 1. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g1\",\"value\":{\"domain\":\"d.io\",\"owner\":\"a\"},"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"d.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "first guarded insert");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g2\",\"value\":{\"domain\":\"d.io\",\"owner\":\"b\"},"
        "\"if_no_match\":{\"or\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"d.io\"}]}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "guard blocks");
    ASSERT_CONTAINS(resp, "\"guard\":\"if_no_match\"", "guard named");
    ASSERT_CONTAINS(resp, "\"matched\":1", "one other matched");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"gr\"}",
               &resp);
    ASSERT_CONTAINS(resp, "1", "exactly one record");
    free(resp); resp = NULL;

    /* 2. Non-indexed guard field works (owner carries no index). */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g3\",\"value\":{\"domain\":\"e.io\",\"owner\":\"a\"},"
        "\"if_no_match\":[{\"field\":\"owner\",\"op\":\"eq\",\"value\":\"a\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"matched\":1", "scan-based guard blocks");
    free(resp); resp = NULL;

    /* 3. if_match sibling-exists on the kid object (same-object guard;
          rev-2 finding 10 — the guard queries `p`, which kid has). */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"kid\","
        "\"key\":\"c-x\",\"value\":{\"p\":\"p1\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"kid\","
        "\"key\":\"c-y\",\"value\":{\"p\":\"p1\"},\"if_match\":"
        "[{\"field\":\"p\",\"op\":\"eq\",\"value\":\"p1\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "if_match passes");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"kid\","
        "\"key\":\"c-z\",\"value\":{\"p\":\"p1\"},\"if_match\":"
        "[{\"field\":\"p\",\"op\":\"eq\",\"value\":\"nope\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"guard\":\"if_match\"", "if_match named");
    ASSERT_CONTAINS(resp, "\"matched\":0", "nothing matched");
    free(resp); resp = NULL;

    /* 4. Self-exclusion on update: g1 is the only d.io record; updating
          it under a d.io no_match guard passes (itself excluded). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g1\",\"value\":{\"owner\":\"a2\"},\"if_no_match\":"
        "[{\"field\":\"domain\",\"op\":\"eq\",\"value\":\"d.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "self excluded");
    free(resp); resp = NULL;
    /* ...and moving e.io's domain onto d.io fails (g1 is another match). */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g3\",\"value\":{\"domain\":\"d.io\"},\"if_no_match\":"
        "[{\"field\":\"domain\",\"op\":\"eq\",\"value\":\"d.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"",
                    "collision rejected");
    free(resp); resp = NULL;

    /* 5. Guard + key-scoped if compose: guard passes, key-if fails. */
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g1\",\"value\":{\"owner\":\"a3\"},"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"zz.io\"}],"
        "\"if\":[{\"field\":\"owner\",\"op\":\"eq\",\"value\":\"ghost\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "key-if gates");
    ASSERT_TRUE(SAFE_STRSTR(resp, "\"guard\"") == NULL,
                "failure is the key-scoped one");
    free(resp); resp = NULL;

    /* 6. Malformed and empty guards reject with no write. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g9\",\"value\":{\"domain\":\"z.io\"},"
        "\"if_no_match\":[]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"invalid guard criteria\"",
                    "empty guard rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g9\",\"value\":{\"domain\":\"z.io\"},"
        "\"if_no_match\":[{\"field\":\"nosuch\",\"op\":\"eq\",\"value\":1}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"invalid guard criteria\"",
                    "unknown-field guard rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gr\","
                   "\"key\":\"g9\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "g9 was never written");
    free(resp); resp = NULL;

    /* 7. Explicit null guards are absent; two non-null guards reject. */
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g10\",\"value\":{\"domain\":\"n.io\"},"
        "\"if_match\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "null guard = absent");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g11\",\"value\":{\"domain\":\"m.io\"},"
        "\"if_match\":null,\"if_no_match\":[{\"field\":\"domain\",\"op\":"
        "\"eq\",\"value\":\"m.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"",
                    "null guard absent; surviving no-match guard runs");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"g12\",\"value\":{\"domain\":\"dual.io\"},"
        "\"if_match\":[{\"field\":\"domain\",\"op\":\"eq\",\"value\":\"m.io\"}],"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\",\"value\":\"dual.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"invalid guard criteria\"",
                    "two non-null guards rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gr\","
                   "\"key\":\"g12\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "g12 was never written");
    free(resp); resp = NULL;

    /* 8. Guards on non-write modes are ignored. */
    tc_request(tc, "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"gr\","
        "\"criteria\":[],\"if_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"d.io\"}]}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL,
                "find ignores guards");
    free(resp); resp = NULL;

    /* 9. Auto-key guarded insert (server-generated key, no self to
          exclude). */
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"aku\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"domain:varchar:40\"],\"auto_key\":\"uuid\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"aku\","
        "\"value\":{\"domain\":\"u.io\"},\"if_no_match\":[{\"field\":\"domain\","
        "\"op\":\"eq\",\"value\":\"u.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "auto-key guard insert");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"aku\","
        "\"value\":{\"domain\":\"u.io\"},\"if_no_match\":[{\"field\":\"domain\","
        "\"op\":\"eq\",\"value\":\"u.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "second blocked");
    free(resp); resp = NULL;

    /* 10. D8 boundary — guards reject on bulk modes. */
    tc_request(tc, "{\"mode\":\"bulk-insert\",\"dir\":\"default\","
        "\"object\":\"gr\",\"records\":[{\"key\":\"gb\","
        "\"value\":{\"domain\":\"b.io\"}}],"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"b.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "not supported on bulk", "bulk guard rejected");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gr\","
                   "\"key\":\"gb\"}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "gb was never written");
    free(resp); resp = NULL;

    /* 11. Indexed equality and AND post-filter paths keep the guard. */
    tc_request(tc, "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"gr\","
        "\"field\":\"domain\"}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "index domain");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gr\","
        "\"key\":\"idx-duplicate\",\"value\":{\"domain\":\"d.io\"},"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\",\"value\":\"d.io\"}]}", &resp);
    ASSERT_CONTAINS(resp, "condition_not_met", "indexed guard rejects duplicate");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-cas-guards", test_cas_guards_run)
```

**Implementation:**

**Additional Task 5 test:** add `src/test/cases/test_guard_proof.c` and its
build.sh source line immediately after the Task 5 `test_cas_guards.c`
line. The pre-implementation build must fail for missing strict APIs; the
implemented case must pass before Task 5 is complete. The case directly
proves missing index, callback-cap and corrupt-page outcomes are errors.

```c
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "../../db/btree.h"
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

static int guard_proof_count(const char *v, size_t n,
                              const uint8_t *hash, void *ctx) {
    (void)v; (void)n; (void)hash;
    (*(int *)ctx)++;
    return 0;
}
static int guard_proof_cap(const char *v, size_t n,
                            const uint8_t *hash, void *ctx) {
    (void)v; (void)n; (void)hash; (void)ctx;
    return -1;
}
static int test_guard_proof_run(void) {
    char path[] = "/tmp/shard-guard-proof-XXXXXX";
    int fd = mkstemp(path);
    ASSERT_TRUE(fd >= 0, "unique temporary index path");
    if (fd < 0) return 1;
    close(fd); unlink(path);
    int count = 0;
    ASSERT_EQ_INT(btree_guard_eq(path, "a", 1, guard_proof_count, &count),
                  -1, "missing index is unproven");
    uint8_t hash[16] = {1};
    ASSERT_EQ_INT(btree_insert(path, "a", 1, hash), 0, "seed index");
    ASSERT_EQ_INT(btree_guard_eq(path, "a", 1, guard_proof_count, &count),
                  0, "complete equality enumeration");
    ASSERT_EQ_INT(count, 1, "one candidate");
    ASSERT_EQ_INT(btree_guard_eq(path, "a", 1, guard_proof_cap, NULL),
                  -1, "candidate cap cannot prove absence");
    btree_cache_invalidate(path);
    fd = open(path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open private index for corruption");
    if (fd >= 0) {
        BtFileHeader h;
        ssize_t got = pread(fd, &h, sizeof(h), 0);
        ASSERT_TRUE(got == (ssize_t)sizeof(h), "read header");
        if (got == (ssize_t)sizeof(h)) {
            uint32_t bad = UINT32_MAX;
            off_t off = (off_t)h.root_page * bt_page_size +
                        (off_t)offsetof(BtPageHeader, count);
            ASSERT_TRUE(pwrite(fd, &bad, sizeof(bad), off) == sizeof(bad),
                        "corrupt slot directory count");
            count = 0;
            ASSERT_EQ_INT(btree_guard_eq(path, "a", 1, guard_proof_count, &count),
                          -1, "corrupt page cannot prove absence");
        }
        close(fd);
    }
    btree_cache_invalidate(path);
    unlink(path);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-guard-proof", test_guard_proof_run)
```

The guard TCP case must additionally be run once without the domain
index (its original first steps) and once after add-index (step 11).
Only the strict full walker may serve as fallback. Do not use `count == 0`
as evidence in an index-failure or fallback-failure regression.

**Additional blind-review regression — complete new file**
`src/test/cases/test_guard_condition_proof.c`, registered as
`test-guard-condition-proof`. Insert its source line after the exact line
`    src/test/cases/test_guard_proof.c \` in build.sh:

```
    src/test/cases/test_guard_condition_proof.c \
```

Before the fix, the indexed signed-zero and padded-long cases must fail
(the unindexed cases provide the correct comparison verdict); malformed
regex cases must fail because the invalid guard authorizes a write.
After implementation all cases must pass. Removing just `guard_eq_key`
and restoring raw operand encoding reproduces the indexed failure.
Removing regex compilation validation reproduces the invalid-pattern
failure. Restore the fixes, paste green output, and run the full suite.

```c
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int test_guard_condition_proof_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env)) return 1;
    TestClientCfg cfg = {.port = env.port, .io_timeout_ms = 5000};
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL; char req[4096];
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gp\","
        "\"splits\":8,\"max_key\":32,\"fields\":[\"d:double\",\"f:float\",\"n:long\",\"tag:varchar:40\"]}", &resp);
    ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""), "create proof object");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gp\","
        "\"key\":\"negative\",\"value\":{\"d\":-0.0,\"f\":-0.0,\"n\":1,\"tag\":\"seed\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed signed zero");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gp\","
        "\"key\":\"target\",\"value\":{\"d\":1,\"f\":1,\"n\":2,\"tag\":\"target\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed update target");
    free(resp); resp = NULL;
    const char *fields[] = {"d", "f", "n"};
    const char *literals[] = {"0.0", "-0.0", "\"000000000000000000000000000000000000000000001\""};
    /* Scan and indexed configurations must yield identical guard verdicts. */
    for (int indexed = 0; indexed < 2; indexed++) {
        if (indexed) for (int i = 0; i < 3; i++) {
            snprintf(req, sizeof(req), "{\"mode\":\"add-index\",\"dir\":\"default\","
                "\"object\":\"gp\",\"field\":\"%s\"}", fields[i]);
            tc_request(tc, req, &resp);
            ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"error\""), "add proof index");
            free(resp); resp = NULL;
        }
        for (int field = 0; field < 3; field++) {
            for (int sign = 0; sign < (field == 2 ? 1 : 2); sign++) {
                const char *literal = field == 2 ? literals[2] : literals[sign];
                snprintf(req, sizeof(req), "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gp\","
                    "\"key\":\"denied\",\"value\":{\"tag\":\"denied\"},\"if_no_match\":"
                    "[{\"field\":\"%s\",\"op\":\"eq\",\"value\":%s}]}", fields[field], literal);
                tc_request(tc, req, &resp);
                ASSERT_CONTAINS(resp, "\"error\":\"condition_not_met\"", "existing equivalent value rejects absence");
                free(resp); resp = NULL;
                snprintf(req, sizeof(req), "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"gp\","
                    "\"key\":\"target\",\"value\":{\"tag\":\"target\"},\"if_match\":"
                    "[{\"field\":\"%s\",\"op\":\"eq\",\"value\":%s}]}", fields[field], literal);
                tc_request(tc, req, &resp);
                ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "existing equivalent value proves existence");
                free(resp); resp = NULL;
            }
        }
    }
    const char *ops[] = {"regex", "not_regex"};
    for (int i = 0; i < 2; i++) {
        snprintf(req, sizeof(req), "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gp\","
            "\"key\":\"denied\",\"value\":{\"tag\":\"denied\"},\"if_no_match\":"
            "[{\"field\":\"tag\",\"op\":\"%s\",\"value\":\"[\"}]}", ops[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"error\":\"invalid guard criteria\"", "invalid regex guard rejected");
        free(resp); resp = NULL;
        snprintf(req, sizeof(req), "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"gp\","
            "\"key\":\"target\",\"value\":{\"tag\":\"bad\"},\"if\":"
            "[{\"field\":\"tag\",\"op\":\"%s\",\"value\":\"[\"}]}", ops[i]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"error\":\"invalid if condition\"", "invalid key regex rejected");
        free(resp); resp = NULL;
    }
    tc_request(tc, "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"gp\",\"key\":\"denied\"}", &resp);
    ASSERT_CONTAINS(resp, "false", "no invalid guard wrote"); free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"gp\","
        "\"key\":\"target\",\"value\":{\"tag\":\"done\"},\"if_match\":"
        "[{\"field\":\"tag\",\"op\":\"regex\",\"value\":\"^seed$\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "valid regex and writer progress after failures");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gp\",\"key\":\"target\"}", &resp);
    ASSERT_CONTAINS(resp, "\"tag\":\"done\"", "only valid update applied"); free(resp);
    tc_close(tc); test_env_stop(&env); return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-guard-condition-proof", test_guard_condition_proof_run)
```

1. **Dedicated strict guard search; leave all read counters unchanged.**

The historical read counters deliberately hide some failures; they must
not authorize mutations. Delete rev 2's proposed `count_tree_compute`
refactor entirely. `cmd_count_with_tree`, `idx_count_for_leaf`, and the
ordinary find/count workers keep their current signatures and code.

The guard has two paths: a plain-field B-tree equality leaf under AND can
produce a complete candidate set, followed by strict record verification;
other predicates use the existing `slotcask_walk_live_strict`. The full
criteria tree is always evaluated against the record. OR subtrees cannot
supply a sole driver. Missing/unreadable indexes, candidate overflow, or
invalid index pages fall back to the strict walk; a failed strict walk
rejects the write. A matching OTHER record is a sufficient witness, so
stop on the first verified match. The new `matched` response is a Boolean
witness count (0 or 1), not the exact number of matches. No result buffer
grows with object size. There is no change to ordinary read performance.

**New consumer inventory:** `btree_guard_eq` is declared in btree.h and
called only by `guard_has_other` below; `slotcask_guard_lookup` is declared
in slotcask.h and called only by `guard_has_other`. Both are additive
internal APIs with no external or on-disk consumers. `GuardProbe` and all
`guard_*` helpers below are query.c-local. Existing production changes for F2 include server wiring, writer-gate
borrowing and scoped parallel dispatch. Marker/walk preconditions are
explicitly broadened; normal read counters remain unchanged.

**btree.c:** insert immediately before the quoted anchor
`void btree_search(const char *path, const char *value, size_t vlen,`.
Add `int btree_guard_eq(const char *path, const char *value, size_t vlen,
bt_result_cb cb, void *ctx);` beside btree_search in btree.h.

```c
/* Validate a page before the existing unchecked decode/search helpers
   touch it. This is guard-only; ordinary B-tree readers are unchanged. */
static uint8_t *guard_checked_page(BtFile *bt, uint32_t id) {
    if (bt_page_size <= 0 || !id ||
        (size_t)id >= bt->map_size / (size_t)bt_page_size) return NULL;
    BtFileHeader *fh = (BtFileHeader *)bt->map;
    if (id >= fh->page_count) return NULL;
    uint8_t *page = bt_page(bt, id);
    BtPageHeader *ph = (BtPageHeader *)page;
    if (ph->page_type > 1 ||
        ph->count > ((size_t)bt_page_size - sizeof(*ph)) / sizeof(uint16_t))
        return NULL;
    size_t slots_end = sizeof(*ph) + (size_t)ph->count * sizeof(uint16_t);
    if (ph->data_end < slots_end || ph->data_end > (size_t)bt_page_size)
        return NULL;
    char prev[BT_MAX_VAL_LEN]; size_t prev_len = 0;
    for (uint32_t i = 0; i < ph->count; i++) {
        size_t off = bt_load_u16(page + sizeof(*ph) + i * sizeof(uint16_t));
        if (off < ph->data_end || off > (size_t)bt_page_size - 2) return NULL;
        uint8_t *e = page + off;
        size_t n = entry_data_len(e);
        if (n > (size_t)bt_page_size - off - 2) return NULL;
        if (ph->page_type == 1) {
            if (n < 1 + BT_HASH_SIZE) return NULL;
            size_t prefix = leaf_entry_prefix_len(e);
            size_t suffix = leaf_entry_suffix_len(e);
            if ((i % BT_LEAF_RESTART_K == 0 && prefix != 0) ||
                prefix > prev_len || prefix + suffix > BT_MAX_VAL_LEN)
                return NULL;
            char key[BT_MAX_VAL_LEN];
            memcpy(key, prev, prefix);
            memcpy(key + prefix, leaf_entry_suffix(e), suffix);
            size_t len = prefix + suffix;
            if (i && val_cmp(prev, prev_len, key, len) > 0) return NULL;
            memcpy(prev, key, len); prev_len = len;
        } else {
            if (n < BT_HASH_SIZE + 4 ||
                n - BT_HASH_SIZE - 4 > BT_MAX_VAL_LEN) return NULL;
            size_t len = int_entry_vlen(e);
            if (i && val_cmp(prev, prev_len, int_entry_value(e), len) > 0)
                return NULL;
            memcpy(prev, int_entry_value(e), len); prev_len = len;
            uint32_t child = entry_child(e);
            if (!child || child >= fh->page_count ||
                (size_t)child >= bt->map_size / (size_t)bt_page_size)
                return NULL;
        }
    }
    if (ph->next_leaf >= fh->page_count ||
        (ph->next_leaf &&
         (size_t)ph->next_leaf >= bt->map_size / (size_t)bt_page_size))
        return NULL;
    return page;
}

/* 0 = fully enumerated equality range; -1 = unproven (including caller
   collection cap). Never interprets an acquisition failure as absence. */
int btree_guard_eq(const char *path, const char *value, size_t vlen,
                   bt_result_cb cb, void *ctx) {
    BtFile bt;
    if (bt_acquire(&bt, path, 0) != 0) return -1;
    int rc = -1;
    if (bt_page_size < (int)sizeof(BtPageHeader) ||
        bt.map_size < sizeof(BtFileHeader)) goto done;
    BtFileHeader *fh = (BtFileHeader *)bt.map;
    if (fh->magic != BT_MAGIC || !fh->root_page ||
        fh->page_count > bt.map_size / (size_t)bt_page_size) goto done;
    uint32_t id = fh->root_page;
    size_t hops = 0;
    uint8_t *page;
    for (;;) {
        if (++hops > fh->page_count) goto done;
        page = guard_checked_page(&bt, id);
        if (!page) goto done;
        BtPageHeader *ph = (BtPageHeader *)page;
        if (ph->page_type == 1) break;
        int pos = page_bsearch(page, value, vlen);
        id = pos ? entry_child(page_entry(page, pos - 1)) : ph->next_leaf;
    }
    while (id) {
        page = guard_checked_page(&bt, id);
        if (!page || ((BtPageHeader *)page)->page_type != 1) goto done;
        LeafIter it; leaf_iter_init(&it, page);
        if (leaf_iter_seek(&it, page_bsearch(page, value, vlen))) {
            do {
                int cmp = val_cmp(it.key_buf, it.key_len, value, vlen);
                if (cmp > 0) { rc = 0; goto done; }
                if (cmp == 0 && cb(it.key_buf, it.key_len, it.hash, ctx) < 0)
                    goto done;
            } while (leaf_iter_next(&it));
        }
        id = ((BtPageHeader *)page)->next_leaf;
        if (id && ++hops > fh->page_count) goto done;
    }
    rc = 0;
done:
    bt_release(&bt);
    return rc;
}
```

**slotcask.c:** insert immediately before the anchor
`int slotcask_lookup_by_hash(SlotcaskDb *db, const uint8_t hash16[16],`.
Add the identical declaration for this new function beside that API in
slotcask.h. Unlike the old helper, this one never skips an unreadable live
reference. Caller holds the shared object lock and every writer gate (Task 4).

```c
/* -1 error, 0 probe complete, 1 callback requested stop. */
int slotcask_guard_lookup(SlotcaskDb *db, const uint8_t hash16[16],
                          SlotcaskScanCb cb, void *ctx) {
    int sid = shard_for_hash(hash16, db->num_shards);
    char path[PATH_MAX]; kf_path_for(path, db->data_dir, sid);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, path, db->slots_per_shard, 0) != 0) return -1;
    int rc = 0;
    size_t start = kf_slot_for(hash16, kh.capacity);
    for (size_t i = 0; i < kh.capacity; i++) {
        SlotcaskKfEntry *e = &kh.map[(start + i) % kh.capacity];
        uint8_t flag = __atomic_load_n(&e->flag, __ATOMIC_ACQUIRE);
        if (!flag) break;
        if (flag == 2) continue;
        if (flag != 1) { rc = -1; break; }
        if (memcmp(e->hash, hash16, 16)) continue;
        if (e->stream_id >= db->num_streams) { rc = -1; break; }
        char segpath[PATH_MAX];
        seg_path_for(segpath, db->data_dir, e->stream_id, e->file_id);
        SlotcaskSegHandle sh = { .slot = -1, .fd = -1 };
        if (segcache_acquire(&sh, segpath, 0, 0, 0) != 0) { rc = -1; break; }
        if (e->offset > sh.map_size || sh.map_size - e->offset < 24) rc = -1;
        else {
            const uint8_t *rec = sh.map + e->offset;
            uint16_t klen = seg_rec_klen(rec);
            uint32_t vlen = seg_rec_vlen(rec);
            size_t padded = slotcask_record_size_varlen(klen, vlen);
            if (24u + (size_t)klen + vlen > (size_t)db->slot_size ||
                padded > (size_t)db->slot_size ||
                padded > sh.map_size - e->offset ||
                !seg_rec_live_with_hash(rec, hash16)) rc = -1;
            else if (cb(hash16, rec + 24, klen, rec + 24 + klen,
                         vlen, rec[19], ctx)) rc = 1;
        }
        segcache_release(&sh);
        if (rc) break;
    }
    kfcache_release(&kh);
    return rc;
}
```

**query.c:** insert the following helpers immediately before the quoted
anchor `static int cmd_count_with_tree(`. They allocate no unbounded
candidate arrays and never acquire kfcache while holding a B-tree lock.
Add `#include "btree.h"` and `#include <math.h>` if not already present.

```c
typedef struct {
    CriteriaNode *tree;
    FieldSchema *fs;
    const void *key;
    size_t klen;
    QueryDeadline dl;
    int found, failed;
    uint8_t hashes[64][16];
    size_t nhashes;
} GuardProbe;

static int guard_deadline(GuardProbe *g) {
    if (g->dl.timeout_ms &&
        now_ms_coarse() - g->dl.t0_ms >= g->dl.timeout_ms)
        g->dl.timed_out = 1;
    return g->dl.timed_out;
}

static int guard_record(const uint8_t hash[16], const void *key, size_t klen,
                         const void *value, size_t vlen, uint8_t hdr2,
                         void *ctx) {
    GuardProbe *g = ctx;
    (void)hash;
    if (guard_deadline(g)) return 1;
    const uint8_t *base, *bm; int bm_bytes; size_t region;
    if (typed_record_view(value, vlen, hdr2, &base, &bm, &bm_bytes, &region)) {
        g->failed = 1; return 1;
    }
    if (g->key && klen == g->klen && !memcmp(key, g->key, klen)) return 0;
    int matched = cas_match_tree_checked(value, vlen, g->tree, g->fs, hdr2);
    if (matched < 0) { g->failed = 1; return 1; }
    if (matched) { g->found = 1; return 1; }
    return 0;
}

static int guard_collect_hash(const char *v, size_t n,
                               const uint8_t *hash, void *ctx) {
    GuardProbe *g = ctx;
    (void)v; (void)n;
    if (guard_deadline(g) || g->nhashes == 64) return -1;
    memcpy(g->hashes[g->nhashes++], hash, 16);
    return 0;
}

/* Only enumerate a single exact index key when it covers every value
   the typed matcher could call equal. Unsupported encodings scan safely. */
static int guard_eq_key(const CriteriaNode *n, uint8_t out[1032], size_t *len) {
    if (!n || n->kind != CNODE_LEAF || n->leaf.op != OP_EQUAL ||
        !n->compiled || !n->compiled->tf || strchr(n->leaf.field, '+')) return 0;
    const CompiledCriterion *cc = n->compiled;
    const TypedField *f = cc->tf;
    out[0] = 0;
    if (f->type == FT_VARCHAR) {
        if (cc->s1_len > 1031 || cc->s1_len > (size_t)f->size - 2) return 0;
        memcpy(out + 1, cc->s1, cc->s1_len);
        *len = cc->s1_len + 1;
        return 1;
    }
    uint64_t bits; int width;
    switch (f->type) {
    case FT_SHORT:
        if (cc->i1 < INT16_MIN || cc->i1 > INT16_MAX) return 0;
        bits = (uint16_t)cc->i1 ^ UINT16_C(0x8000); width = 2; break;
    case FT_INT:
        if (cc->i1 < INT32_MIN || cc->i1 > INT32_MAX) return 0;
        bits = (uint32_t)cc->i1 ^ UINT32_C(0x80000000); width = 4; break;
    case FT_LONG: case FT_TIMESTAMP: case FT_NUMERIC:
        bits = (uint64_t)cc->i1 ^ (UINT64_C(1) << 63); width = 8; break;
    case FT_BOOL: case FT_BYTE:
        bits = cc->b1; width = 1; break;
    case FT_FLOAT: {
        float v = (float)cc->d1;
        if (!isfinite(v) || v == 0.0f) return 0; /* +/- zero compare equal */
        uint32_t raw; memcpy(&raw, &v, 4);
        bits = raw & UINT32_C(0x80000000) ? ~raw : raw ^ UINT32_C(0x80000000);
        width = 4; break;
    }
    case FT_DOUBLE: {
        double v = cc->d1;
        if (!isfinite(v) || v == 0.0) return 0;
        memcpy(&bits, &v, 8);
        bits = bits & (UINT64_C(1) << 63) ? ~bits : bits ^ (UINT64_C(1) << 63);
        width = 8; break;
    }
    default: return 0;
    }
    for (int i = 0; i < width; i++)
        out[1 + i] = (uint8_t)(bits >> (8 * (width - 1 - i)));
    *len = (size_t)width + 1;
    return 1;
}

/* A leaf under AND is a necessary condition. Never select one OR branch. */
static CriteriaNode *guard_eq_driver(CriteriaNode *n,
                                      const char *root, const char *object, int splits) {
    if (n->kind == CNODE_LEAF) {
        uint8_t key[1032]; size_t len;
        if (guard_eq_key(n, key, &len) && len <= BT_MAX_VAL_LEN &&
            field_index_type(root, object, n->leaf.field) == IT_BTREE &&
            btree_idx_exists(root, object, n->leaf.field, splits)) return n;
        return NULL;
    }
    if (n->kind == CNODE_AND)
        for (int i = 0; i < n->n_children; i++) {
            CriteriaNode *d = guard_eq_driver(n->children[i], root, object, splits);
            if (d) return d;
        }
    return NULL;
}

/* Return 0 proven verdict, -1 failed, -3 timeout. A fallback scan is
   deliberate, not absence inferred from an incomplete candidate search. */
static int guard_has_other(const char *root, const char *object,
                            const Schema *sc, SlotcaskDb *sdb, GuardProbe *g) {
    CriteriaNode *d = guard_eq_driver(g->tree, root, object, sc->splits);
    if (d) {
        uint8_t encoded[1032]; size_t len = 0;
        if (guard_eq_key(d, encoded, &len)) {
            int complete = 1;
            for (int s = 0; s < index_splits_for(sc->splits); s++) {
                if (g->dl.timeout_ms &&
                    now_ms_coarse() - g->dl.t0_ms >= g->dl.timeout_ms) {
                    g->dl.timed_out = 1; return -3;
                }
                char path[PATH_MAX];
                build_idx_path(path, sizeof(path), root, object, d->leaf.field, s);
                g->nhashes = 0;
                int rc = btree_guard_eq(path, (const char *)encoded, len,
                                         guard_collect_hash, g);
                if (guard_deadline(g)) return -3;
                if (rc != 0) { complete = 0; break; }
                /* B-tree handle was released before these record reads. */
                for (size_t i = 0; i < g->nhashes; i++) {
                    rc = slotcask_guard_lookup(sdb, g->hashes[i], guard_record, g);
                    if (rc < 0 || g->failed) return -1;
                    if (guard_deadline(g)) return -3;
                    if (g->found) return 0;
                }
            }
            if (complete) return 0;
        }
    }
    int rc = slotcask_walk_live_strict(sdb, guard_record, g);
    if (guard_deadline(g)) return -3;
    if (rc < 0 || g->failed || (rc == 1 && !g->found)) return -1;
    return 0;
}
```


2. **query.c — guard request**, immediately after the new helpers:

```c
int cas_guard_request(const char *db_root, const char *object,
                      const char *guard_json,
                      const char *key, size_t klen,
                      int require_match,
                      int *out_pass, size_t *out_matched) {
    *out_pass = 0; *out_matched = 0;
    Schema sc = load_schema(db_root, object);
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) return -2;
    char err[256];
    CriteriaNode *tree = cas_prepare_tree(guard_json, ts, 0, err, sizeof(err));
    if (!tree) return -2;
    /* Server supplied the exact stored key bytes, normalized while its
       shared objlock is held. auto_key_normalize is server.c-local. */
    const char *use_key = key; size_t use_klen = klen;
    if (use_klen > (size_t)sc.max_key) {
        free_criteria_tree(tree); return -2;
    }
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size, .streams = sc.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb || !slotcask_write_reservation_held(sdb)) {
        free_criteria_tree(tree); return -1;
    }
    FieldSchema fs; init_field_schema(&fs, db_root, object);
    GuardProbe g = { .tree = tree, .fs = &fs, .key = use_key, .klen = use_klen,
                    .dl = { now_ms_coarse(), resolve_timeout_ms(), 0 } };
    int rc = guard_has_other(db_root, object, &sc, sdb, &g);
    free_criteria_tree(tree);
    if (rc != 0) return rc;
    *out_matched = g.found ? 1 : 0;
    *out_pass = require_match ? g.found : !g.found;
    return 0;
}
```

The request has no self-fetch/count subtraction: the actual stored key
is excluded in `guard_record`, so hash collisions cannot exclude another
record. A corrupt record is an error, not a predicate miss. Invalid keys
fail before search; no request-controlled stack buffer is used.


3. **types.h** — declaration beside `cmd_count_tree`:

```c
/* Object-scope write guard: pass only when (require_match) at least one
   / (require_match==0) zero OTHER live records match guard_json. The
   caller's own record (key, normalized stored bytes; NULL for fresh auto-key
   inserts) is excluded by comparing actual key bytes. Returns 0 with *out_pass and
   *out_matched (0/1 witness); -1 when the scan failed or was unproven (do not
   write); -2 for invalid guard criteria; -3 for scan timeout. Caller
   holds objlock_rdlock and the all-shard writer reservation (Task 4). */
int cas_guard_request(const char *db_root, const char *object,
                      const char *guard_json,
                      const char *key, size_t klen,
                      int require_match,
                      int *out_pass, size_t *out_matched);
```

4. **server.c — resolve the guard once, classify, check at one site.**
   Anchor:

```c
    int took_wrlock = mode_is_schema(mode);
    int took_rdlock = !took_wrlock;
```

replace with:

```c
    /* Object-scope write guards (if_match / if_no_match): resolve ONCE,
       before the lock classification, so the guard that runs is exactly
       the guard that classified the request. Explicit JSON null =
       absent; both present = rejected before any lock. */
    const char *gm = NULL, *gn = NULL;
    size_t gm_len = 0, gn_len = 0;
    int has_gm = json_obj_get(&req, "if_match", &gm, &gm_len) &&
                 !json_span_is_null(gm, gm_len);
    int has_gn = json_obj_get(&req, "if_no_match", &gn, &gn_len) &&
                 !json_span_is_null(gn, gn_len);
    if (has_gm && has_gn) {
        OUT("{\"error\":\"invalid guard criteria\"}\n");
        free(mode); free(dir); free(object); return;
    }
    char *guard_json = NULL;
    int guard_require_match = has_gm;
    if (has_gm || has_gn) {
        const char *span = has_gm ? gm : gn;
        size_t len = has_gm ? gm_len : gn_len;
        guard_json = malloc(len + 1);
        if (!guard_json) {
            OUT("{\"error\":\"guard check failed\"}\n");
            free(mode); free(dir); free(object); return;
        }
        memcpy(guard_json, span, len); guard_json[len] = '\0';
    }
    int guard_write = strcmp(mode, "insert") == 0 ||
                      strcmp(mode, "update") == 0 ||
                      strcmp(mode, "delete") == 0;
    if (guard_json && strncmp(mode, "bulk-", 5) == 0) {
        free(guard_json);
        OUT("{\"error\":\"guards are not supported on bulk operations\"}\n");
        free(mode); free(dir); free(object);
        return;
    }
    if (!guard_write) { free(guard_json); guard_json = NULL; }
    SlotcaskWriteReservation guard_res = {0};
    SlotcaskDb *guard_sdb SDB_REG_REF = NULL;
    int took_wrlock = mode_is_schema(mode);
    int took_rdlock = !took_wrlock;
```

The two lock-failure early returns inside the following
`if (took_wrlock) { ... } else if (took_rdlock) { ... }` block each
gain `free(guard_json);` beside their existing frees. Immediately
AFTER that lock block (and after the existing
`g_schema_wrlock_test_delay_ms` window, which remains schema-only) insert the single check site:

```c
    /* Shared object lock is held. Keep the registry pin and reservation
       through the existing single-write branch and common tail. */
    if (guard_json) {
        int is_write_mode = strcmp(mode, "insert") == 0 ||
                            strcmp(mode, "update") == 0 ||
                            strcmp(mode, "delete") == 0;
        if (is_write_mode) {
            const char *key_span; size_t key_len;
            int key_present = json_obj_get(&req, "key", &key_span, &key_len);
            char *gkey = key_present ? json_obj_strdup(&req, "key") : NULL;
            int gpass = 0;
            size_t gmatched = 0;
            /* A supplied key whose decode/allocation failed cannot become
               a fresh-key guard or accidentally match the target itself. */
            Schema guard_sc = load_schema(db_root, object);
            SlotcaskSchemaInfo guard_info = {
                .splits = guard_sc.splits, .slot_size = guard_sc.slot_size,
                .streams = guard_sc.streams,
            };
            int grc = -1;
            char *gbin = NULL; size_t gbin_len = 0;
            const char *stored_key = gkey;
            size_t stored_klen = gkey ? strlen(gkey) : 0;
            int key_valid = !key_present || gkey;
            if (key_valid && gkey && guard_sc.auto_key != AK_NONE) {
                if (auto_key_normalize(&guard_sc, gkey, &gbin, &gbin_len)) {
                    key_valid = 0; grc = -2;
                } else { stored_key = gbin; stored_klen = gbin_len; }
            }
            if (key_valid) {
                guard_sdb = slotcask_registry_get(db_root, object, &guard_info);
                if (guard_sdb &&
                    slotcask_write_reservation_begin(guard_sdb, &guard_res) == 0 &&
                    marker_recovery_sweep_object(db_root, guard_sdb->data_dir,
                                                 object, NULL) == 0)
                    grc = cas_guard_request(db_root, object, guard_json,
                                           stored_key, stored_klen,
                                           guard_require_match,
                                           &gpass, &gmatched);
            }
            free(gbin); free(gkey);
            int reject = 1;
            if (grc == -2)
                OUT("{\"error\":\"invalid guard criteria\"}\n");
            else if (grc == -3)
                OUT("{\"error\":\"query_timeout\"}\n");
            else if (grc == -1)
                OUT("{\"error\":\"guard check failed\"}\n");
            else if (!gpass)
                OUT("{\"error\":\"condition_not_met\",\"guard\":\"%s\","
                    "\"matched\":%zu}\n",
                    guard_require_match ? "if_match" : "if_no_match",
                    gmatched);
            else
                reject = 0;
            free(guard_json);
            guard_json = NULL;
            if (reject) {
                slotcask_write_reservation_end(&guard_res);
                if (took_wrlock) objlock_wrunlock(db_root, object);
                else objlock_rdunlock(db_root, object);
                free(mode); free(dir); free(object);
                return;
            }
        } else {
            /* Guards are ignored on non-write modes (documented). */
            free(guard_json);
            guard_json = NULL;
        }
    }
```

The actual tail uses `objlock_wrunlock(db_root, object)` and
`objlock_rdunlock(db_root, object)`; the explicit rejection cleanup mirrors
those calls. At the common dispatch tail, BEFORE either object unlock,
insert `slotcask_write_reservation_end(&guard_res);`. The registry pin's
cleanup runs at function exit, after that release. Audit every return
between acquisition and tail: only the guard rejection and common tail
are reachable with an active reservation; read-only early returns cannot
have one. A future new guarded return must release it explicitly.
Task 2's branch-local return flag parsing is unaffected.

Run the case → paste failing then green. Full suite → green.

---

### Task 6 — F2: deterministic race test (guard-kind hook)

**How this serves the primary goal:** prove concurrent guarded writes cannot both pass.

**Test** — new file `src/test/cases/test_cas_guard_race.c`, registered
as `test-cas-guard-race` (build.sh anchor: after the
`test_cas_guards.c` line). Requires the TEST_BUILD hook kind 3 below.
On the pre-task tree the case cannot arm its hook
(`test_env_test_hook_install_kind` returns -1 → explicit red). The
`done` flags are `_Atomic` so cross-thread reads are TSan-clean
(rev-2 finding 10).

```c
/* src/test/cases/test_cas_guard_race.c
 *
 * Conditional-writes F2 — two concurrent guarded inserts: exactly one
 * may win, deterministically, via the guard-check park hook (TEST_BUILD
 * control channel, kind 3 / REACHED phase 4).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    TestEnv          *env;
    TestClient       *tc;      /* owned by the worker thread */
    const char       *req;
    char             *resp;    /* out */
    atomic_int        done;
} GuardRaceArg;

static void *guard_race_worker(void *p) {
    GuardRaceArg *a = p;
    TestClientCfg cfg = { .port = a->env->port, .io_timeout_ms = 30000 };
    a->tc = tc_connect(&cfg);
    if (!a->tc) return NULL;
    tc_request(a->tc, a->req, &a->resp);
    atomic_store(&a->done, 1);
    return NULL;
}

static int test_cas_guard_race_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *main_tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(main_tc, "connect");
    if (!main_tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(main_tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(main_tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"rc\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"domain:varchar:40\"]}",
        &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "create rc");
    free(resp); resp = NULL;

    tc_request(main_tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rc\","
        "\"key\":\"seed\",\"value\":{\"domain\":\"seed.io\"}}", &resp);
    ASSERT_TRUE(resp && SAFE_STRSTR(resp, "\"error\"") == NULL, "seed rc");
    free(resp); resp = NULL;

    /* Arm the guard park: a guarded request parks AFTER its guard check
       (holding shared objlock plus every writer gate) until released. */
    ASSERT_EQ_INT(test_env_test_hook_install_kind(&env, 3), 0,
                  "arm guard hook (needs TEST_BUILD daemon)");

    const char *req_a =
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rc\","
        "\"key\":\"w1\",\"value\":{\"domain\":\"x.io\"},"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"x.io\"}]}";
    const char *req_b =
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"rc\","
        "\"key\":\"w2\",\"value\":{\"domain\":\"x.io\"},"
        "\"if_no_match\":[{\"field\":\"domain\",\"op\":\"eq\","
        "\"value\":\"x.io\"}]}";

    GuardRaceArg a = { .env = &env, .tc = NULL, .req = req_a,
                       .resp = NULL, .done = 0 };
    pthread_t th1;
    ASSERT_EQ_INT(pthread_create(&th1, NULL, guard_race_worker, &a), 0,
                  "spawn first writer");

    /* First writer parks after its (passing) guard check. */
    int phase = -1;
    ASSERT_EQ_INT(test_env_test_hook_wait(&env, &phase), 0, "park reached");
    ASSERT_EQ_INT(phase, 4, "phase 4 = parked after guard check");

    /* Second writer on its own connection: must BLOCK on writer gates
       until the first commits — it cannot finish while we wait. */
    GuardRaceArg b = { .env = &env, .tc = NULL, .req = req_b,
                       .resp = NULL, .done = 0 };
    pthread_t th2;
    ASSERT_EQ_INT(pthread_create(&th2, NULL, guard_race_worker, &b), 0,
                  "spawn second writer");
    struct timespec pause_ts = { 0, 200 * 1000 * 1000 };
    nanosleep(&pause_ts, NULL);
    ASSERT_TRUE(atomic_load(&b.done) == 0,
                "second writer blocked on reserved writer gates");

    /* No kf write lock is held at this pause. Same-object GET must
       finish while writers remain reserved; a short socket deadline
       makes exclusive-object-lock regressions fail without hanging. */
    TestClientCfg read_cfg = { .port = env.port, .io_timeout_ms = 1000 };
    TestClient *read_tc = tc_connect(&read_cfg);
    ASSERT_NOT_NULL(read_tc, "reader connects during guard");
    if (read_tc) {
        tc_request(read_tc,
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"rc\","
            "\"key\":\"seed\"}", &resp);
        ASSERT_CONTAINS(resp, "seed.io", "reader progresses during guard");
        free(resp); resp = NULL;
        tc_close(read_tc);
    }
    GuardRaceArg ordinary = { .env = &env,
        .req = "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"rc\","
               "\"key\":\"seed\",\"value\":{\"domain\":\"changed.io\"}}",
        .done = 0 };
    pthread_t th3;
    ASSERT_EQ_INT(pthread_create(&th3, NULL, guard_race_worker, &ordinary), 0,
                  "spawn ordinary writer");
    nanosleep(&pause_ts, NULL);
    ASSERT_TRUE(atomic_load(&ordinary.done) == 0,
                "unguarded writer also waits for reservation");

    /* Release the first writer: it commits, then the second's guard
       check runs and must fail. */
    ASSERT_EQ_INT(test_env_test_hook_release(&env), 0, "release");
    pthread_join(th1, NULL);
    pthread_join(th2, NULL);
    pthread_join(th3, NULL);
    ASSERT_TRUE(ordinary.resp && !strstr(ordinary.resp, "\"error\""),
                "ordinary writer completes after reservation");

    ASSERT_TRUE(a.resp && strstr(a.resp, "\"status\":\"inserted\"") != NULL,
                "first guarded insert commits");
    ASSERT_TRUE(b.resp &&
                strstr(b.resp, "\"error\":\"condition_not_met\"") != NULL,
                "second guarded insert blocked then rejected");
    ASSERT_TRUE(b.resp && strstr(b.resp, "\"matched\":1") != NULL,
                "second sees the first");

    tc_request(main_tc, "{\"mode\":\"count\",\"dir\":\"default\","
                       "\"object\":\"rc\"}", &resp);
    ASSERT_CONTAINS(resp, "2", "seed plus exactly one guarded insert");
    free(resp); resp = NULL;

    test_env_test_hook_clear(&env);
    tc_close(main_tc);
    if (a.tc) tc_close(a.tc);
    if (b.tc) tc_close(b.tc);
    free(a.resp);
    free(b.resp);
    if (ordinary.tc) tc_close(ordinary.tc);
    free(ordinary.resp);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-cas-guard-race", test_cas_guard_race_run)
```

**Implementation — hook kind 3, following the count-gap pattern:**

1. **src/db/test_control.h** — beside the count-gap setter
   declarations (match that header's actual naming/decl pattern; if
   the count-gap setter lives in shard_test_ctl.h, put these beside it
   and adjust the include):

```c
typedef void (*shard_db_test_guard_fn)(void *ctx);
void shard_db_test_set_guard_hook(shard_db_test_guard_fn fn, void *ctx);
```

2. **query.c** — under `#ifdef TEST_BUILD` (the setter + park mirror
   slotcask.c:1490-1514's consume-once pattern):

```c
#ifdef TEST_BUILD
static pthread_mutex_t g_guard_park_lock = PTHREAD_MUTEX_INITIALIZER;
static shard_db_test_guard_fn g_guard_park_fn = NULL;
static void *g_guard_park_ctx = NULL;

void shard_db_test_set_guard_hook(shard_db_test_guard_fn fn, void *ctx) {
    pthread_mutex_lock(&g_guard_park_lock);
    g_guard_park_fn = fn;
    g_guard_park_ctx = ctx;
    pthread_mutex_unlock(&g_guard_park_lock);
}

void shard_db_test_guard_park(void) {
    shard_db_test_guard_fn fn;
    void *ctx;
    pthread_mutex_lock(&g_guard_park_lock);
    fn = g_guard_park_fn;
    ctx = g_guard_park_ctx;
    g_guard_park_fn = NULL;
    g_guard_park_ctx = NULL;
    pthread_mutex_unlock(&g_guard_park_lock);
    if (fn) fn(ctx);
}
#endif
```

3. Insert the following immediately before the final `return 0;` of
`cas_guard_request`, after assigning `*out_pass` / `*out_matched`:

```c
#ifdef TEST_BUILD
    shard_db_test_guard_park();
#endif
```

The hook definition above must precede `cas_guard_request`. It is added
in Task 6, not Task 5, so every intermediate task builds independently.

4. **src/db/test_control.c** — with the other kind constants:

```c
#define TEST_HOOK_KIND_GUARD_CHECK 3
```

new park function (copy of `test_control_count_gap_block`, phase 4,
comment adjusted: "the caller holds shared objlock and every writer gate after a passing
guard check — that is the point"):

```c
static void test_control_guard_block(void *ctx_ptr) {
    TestControl *c = ctx_ptr;
    TestHookMessage reached = { .kind = TEST_HOOK_REACHED, .phase = 4 };
    pthread_mutex_lock(&c->lock);
    c->waiting_for_release = 1;
    c->release = 0;
    pthread_cond_broadcast(&c->cond);
    pthread_mutex_unlock(&c->lock);

    if (test_control_write_full(c->fd, &reached, sizeof(reached)) != 0) {
        pthread_mutex_lock(&c->lock);
        c->release = 1;
        pthread_cond_broadcast(&c->cond);
        c->waiting_for_release = 0;
        pthread_mutex_unlock(&c->lock);
        return;
    }

    pthread_mutex_lock(&c->lock);
    while (c->running && !c->release)
        pthread_cond_wait(&c->cond, &c->lock);
    c->waiting_for_release = 0;
    pthread_mutex_unlock(&c->lock);
}
```

the INSTALL branch gains:

```c
            } else if (msg.phase == TEST_HOOK_KIND_GUARD_CHECK) {
                shard_db_test_set_guard_hook(test_control_guard_block, c);
```

and CLEAR + `shard_db_test_control_stop` each gain
`shard_db_test_set_guard_hook(NULL, NULL);`.

5. **src/test/fixtures.c** — `test_env_test_hook_wait`'s phase guard
   `if (rep.phase < 0 || rep.phase > 3)` → `> 4`, with the phase
   comment gaining `4 = guarded write parked after its guard check`.

Run the case → paste failing then green. Full suite → green.

---

### Task 7 — documentation

**How this serves the primary goal:** make the guarantees, failures and blocking costs explicit.

No new tests; docs-only (CORE-PROCESS doc-sync rule).

1. **docs/query-protocol/cas.md**:
   - `if` grammar: array (flat AND, unchanged) or nested
     `{and:…}/{or:…}` tree matching the find grammar
     (MAX_CRITERIA_DEPTH 16); unknown fields are validation errors
     (previously silent never-match); comparisons are typed, matching
     find (zero-padded integer literals now parse — behavior change
     called out); `if: []` remains a vacuous pass; the eq_field family
     works per-record. Invalid regex patterns now reject with `invalid if
     condition`; regex evaluation errors fail the condition closed.
   - New section "Object-scope guards (`if_match` / `if_no_match`)":
     semantics, exclusion of the actual target key, null = absent, two non-null guards
     rejected, failure responses with `guard`/`matched`, writer-reservation
     cost model (reads continue subject to existing shard/cache locks; all
     object writers wait; B-tree equality has a strict indexed fast path;
     other predicates/fallbacks scan serially while gates are reserved;
     fan-out and duplicates also cost time), bulk deferred with its
     rejection error.
   - New section "Computed updates (`$incr` / `$copy`)": grammar,
     exact-one-member rule, type table, overflow/NULL/scale rules,
     condition-sees-preimage, update-only with its rejection error.
   - New section "Returning records (`return_record`)": opt-in, shapes,
     delete returns the removed record, OOM omits the member.
   - Amend "What CAS is **not**: not cross-record" → same-object
     guards exist; **cross-object** atomicity does not; the staging
     pattern remains the answer.
   - The version-CAS pattern gains: with `$incr` the client can bump
     the version server-side; the policy itself stays app-side.
2. **docs/concepts/concurrency.md** — shared objlock, all-shard try/reserve
   and release-on-contention ordering, registry pin lifetime, borrowing,
   scoped inline execution, pending-marker replay, reader lock boundaries,
   starvation/timeout limits and same-object writer blocking.
3. **docs/query-protocol/overview.md** — per-request knobs table gains
   `return_record`; the CAS row mentions guards + computed updates.
4. **docs/reference/error-codes.md** — `invalid guard criteria`,
   `guards are not supported on bulk operations`, `guard check failed`,
   `computed updates are only valid on update`, `field '<name>' is
   NULL; cannot apply computed update`, `field '<name>': computed
   update overflows <type>`, `field '<name>': unsupported type for
   computed update`, `field '<name>': $copy type mismatch with
   '<name>'`, `field '<name>': malformed computed value object`,
   `field '<name>': bad computed literal`.
5. **docs/reference/changelog.md** — under 2026.10.2 Unreleased (or a
   new 2026.10.3 heading per D10's resolution): the four features.
6. **AGENTS.md** — the JSON protocol row `insert / update / delete with
   if / if_not_exists (CAS)` gains `; object-scope guards (if_match /
   if_no_match); computed updates ($incr/$copy); return_record`.

## 6. Invariants

1. **Fail closed**: no guard request ever writes after a guard error —
   timeout, buffer cap, candidate-producer failure, object-not-open,
   malformed, empty. Only a proven existence/absence verdict authorizes.
2. **Guard atomicity**: for two concurrent guarded writes on one
   object, at most one `if_no_match` insert for the same guard criteria
   can succeed (Task 6 proves it deterministically).
3. **One guard, one classification**: the guard resolved before lock
   classification is the only guard evaluated; null guards are absent;
   two non-null guards are rejected before any lock.
4. **Self-exclusion exactness**: the callback compares stored key
   length and bytes with the normalized target. Hash collisions cannot
   exclude unrelated records. Invalid keys reject before search.

5. **Flat-`if` compatibility**: every payload the old parser accepted
   still parses; responses are byte-identical except (a) unknown
   fields, which tighten from `condition_not_met` to `invalid if
   condition`, and (b) typed comparison of zero-padded integer
   literals, which now matches like find, and (c) invalid regex patterns
   reject rather than act as never-match/always-match — intentional,
   tested and documented.
6. **Response compatibility**: without `return_record`, all success and
   error responses are byte-identical to today; with it, the only
   change is the additive `"record"` member. `cmd_count` and its helper implementations are unchanged.
7. **Computed safety**: no computed update can wrap (checked
   arithmetic), silently truncate scale (excess fractional digits rejected),
   double-scale (final bytes written directly), produce NULL, or
   produce non-finite floats; every rejection leaves the record
   untouched and is a whole-request error; the index diff flows through
   the existing OLD→NEW pre-commit path unchanged.
8. **D8 enforced**: guards on bulk modes and computed spans outside
   single update are rejected with named errors and tests.
9. **No format/migration impact**: no schema.conf/fields.conf change,
   no engine-version bump, no on-disk layout change; KFM2 markers
   unchanged (final record bytes; computed updates only change how NEW
   is derived inside the existing commit window).
10. **Reader-preserving writer reservation**: no new mutex or thread.
    Guards retain shared objlock, pin the registry handle, reserve existing
    writer gates in order, and borrow them only on the acquiring thread.
    Both pools execute inline throughout the reservation. Release all gates
    before shared objlock or registry pin. Ordinary gate instrumentation
    remains intact; reservation scope is separately tested. Readers retain
    their existing shard/cache lock behavior.

## 7. Verification

Per task: focused case → full suite. At hand-off (before push/PR, once,
after ALL fixes — AGENTS.md gate):

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all          # × 3 consecutive passes
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2   # × 3 consecutive passes
```

The gate covers: writer reservation/borrowing, scoped inline execution,
live marker replay and strict scan with readers active, shared compiled trees across bulk workers
(read-only after compile — TSan must confirm), the ret_buf lifetimes
across all three hook chains, and the race test's `_Atomic`
observation flags. Leave all work uncommitted for the reviewing agent +
human to review the raw `git diff`.

## 8. Plan-author preflight (not execution evidence)

The following checks apply to rev 3 helper bodies. Rev 4 adds the writer
reservation and concurrency requirements above; its additional verification
is recorded separately below. No feature execution is claimed.

The four extracted helper groups (strict B-tree, strict hash fetch, query
guard, computed parser/applier) passed C syntax checks against this
checkout's actual headers and source. All six proposed test files passed
syntax checks with the additive strict-API declarations. Eight isolated
arithmetic cases passed with strict UBSan: negative short/int/long,
125.50 scaling, fractional padding, .05, negative decimal, and the signed
int64 minimum. ASan+LSan could not complete in this tool environment
because LeakSanitizer reported ptrace incompatibility; no repository
sanitizer gate is claimed. Feature code and integration tests have not
been installed or run. Execution still requires review/approval and the
normal full-suite plus final sanitizer gate above.

Rev 4 preflight: the proposed reservation definitions and modified gate
wrappers, scoped parallel-inline definitions and dispatch conditions, and
updated TCP guard-race test pass C syntax checks against current source
and headers. No integration, saturation or sanitizer execution is claimed;
the additional concurrency assertions in Task 4 remain implementation
acceptance requirements. Repository source remains untouched.

Rev 5 preflight: the complete converted query_bulk.c translation unit,
strict condition/query helpers, server guard wiring and all revised/new
TCP and reservation test files pass C syntax checks against this checkout.
The standalone reservation harness was compiled from extracted proposed
slotcask/parallel snippets under /tmp and linked with the existing embedded
library. All six modes passed: lifecycle/borrowing, partial acquisition,
pool saturation, retained-marker replay, corrupt-marker rejection and
schema exclusion. Fault variants demonstrated useful regressions: removing
inline dispatch timed out (exit 124); retaining partial gates failed the
partial-acquisition assertion (exit 1). These are isolated plan preflight
checks, not repository feature execution or the required sanitizer gate.
No production source was installed or modified.

Seven isolated compiled-condition regressions also passed: both signs of
zero for float and double choose the strict scan; a long zero-padded
integer encodes from its compiled value without truncation; invalid regex
and not_regex patterns reject compilation. The follow-up read-only review
found no concrete unresolved defects in these revised sections. Full TCP
feature tests and final ASan/UBSan/TSan gates still belong to execution;
this preflight does not claim they ran.
