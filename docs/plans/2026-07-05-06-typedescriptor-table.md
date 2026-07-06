# Refactor: collapse the per-field-type switches into one TypeDescriptor table

## Nature of this plan

The highest-leverage fix for the recurring-bug pattern, and the **highest-risk**
plan here. Field-type handling is copy-pasted across **19 `switch` blocks in
7 files** (268 `case FT_*` labels total: config.c 81, query_aggregate.c 70,
query_plan.c 63, query_schema.c 23, io_direct.c 17, query_find.c 7,
query_join.c 7 — counts as of the post-plan-05 split; the Phase-0 grep
re-verifies). Every new type means editing ~15
parallel switches; missing one ships a bug (exactly what commit 26e622d fixed for
the agg int-hash path). This replaces those switches with a single table of
per-type descriptors, so a new type is one row.

Note: `index.c` and `query_schema.c` also contain switches on `IT_BTREE` /
`IT_BITMAP` — those dispatch on *index* type, not field type, and are **out of
scope**. `auto_now_str` (storage.c) is a 4-branch if/else time-source
generator, not type-metadata dispatch — leave it as-is (documented exception).

It is **structural**, done **after plan 05** (the split), **incrementally** — one
switch converted per phase, full suite green after each, with a review checkpoint
per phase. Do not attempt a big-bang rewrite. If a conversion changes observable
behavior in any test, STOP.

Useful prior context: memory note #6485 ("Exact Function Locations and
Implementation Patterns for Field-Type Extensions") and #6485-adjacent notes
map where type logic lives.

## Execution rules (read first)

- Branch off `main`: `git checkout -b refactor/type-descriptor-table`. Requires
  plan 05 already merged (or rebased on it).
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run-all`
  after **every** switch conversion. Not green → STOP + `PLAN_NOTES.md`.
- Behavior-preserving only. The table must produce byte-identical encode/decode/
  compare/index-key output to the switch it replaces. Add no new type semantics
  here.
- Leave uncommitted; stop for review after each phase under the plan→review loop.

## Phase 0 — Inventory + baseline

1. `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all` → record the
   `# total:` line. Every phase must match it.
2. Enumerate the `FieldType` enum in `types.h` (FT_VARCHAR, FT_INT, FT_LONG,
   FT_SHORT, FT_DOUBLE, FT_FLOAT, FT_BOOL, FT_BYTE, FT_DATE, FT_DATETIME,
   FT_DATETIMEMS, FT_TIME, FT_TIMESTAMP, FT_UUID, FT_IPV4, FT_IPV6, FT_NUMERIC/
   currency, FT_ENUM, …). Write the exact list into `PLAN_NOTES.md` — it drives
   the table's row count.
3. List every `switch` that dispatches on a field type:
   ```
   grep -n "case FT_" src/db/*.c
   ```
   Group by the enclosing function and confirm it matches the verified backlog
   below (line numbers will drift; function names are the anchor). Write the
   confirmed list to `PLAN_NOTES.md` as the conversion backlog (one phase each).

   **Verified backlog (19 switches, 7 files):**

   | File | Function | Role |
   |---|---|---|
   | config.c | `encode_field_len` | text → storage bytes (validation lives here) |
   | config.c | `encode_field_for_index` | text → index-key bytes (fixed-width, sign-flip) |
   | config.c | `typed_field_to_index_key` | storage bytes → index-key bytes |
   | config.c | `decode_field_to_buf` (static) | storage bytes → text |
   | config.c | `typed_get_field_str` | storage bytes → rendered string |
   | query_plan.c | `compile_one` (×2 switches) | criteria pre-compilation |
   | query_plan.c | `cmp_typed_field_pair` | binary field comparison |
   | query_plan.c | `match_typed` | per-record criteria matching |
   | query_aggregate.c | `typed_field_to_buf_raw` | field → canonical buf for group-by |
   | query_aggregate.c | `decode_index_key_to_double` | index-key → double |
   | query_aggregate.c | `decode_idx_to_buf` | index-key → text |
   | query_aggregate.c | `typed_field_to_double` | field → double for sum/avg |
   | query_aggregate.c | `typed_field_int_width` | int-class width for agg fast path |
   | query_schema.c | `field_value_fits_new` | edit-field compat check |
   | query_schema.c | `field_type_str` | FT_* → name string (pure data) |
   | query_find.c | `transform_field_value` | edit-field value transform |
   | query_join.c | `buf_field_value` | join-key value buffering |
   | io_direct.c | `seg_scan_o_direct_match` (×2 switches) | batch scan matchers |

   Out of scope (not field-type dispatch): `IT_BTREE`/`IT_BITMAP` switches in
   index.c and query_schema.c; `auto_now_str` if/else in storage.c.

## Phase 1 — Define the descriptor (no behavior change yet)

Create `src/db/type_desc.h` and `src/db/type_desc.c`.

**Dispatch strategy — decided here, not later:** encode/decode/compare/match
run per-field-per-record in bulk-insert and full-scan hot loops; an indirect
call through a function pointer defeats LTO inlining there. Therefore the
descriptor table carries **metadata only** — no function-pointer columns.
Hot-path functions keep their internal `switch` for control flow but replace
every duplicated *data* fact (sizes, widths, orderability, sign-flip) with
table lookups, and cold-path functions (`field_type_str`, describe/render,
schema mutations) become full table lookups. If a later phase finds a genuine
cold-path use for behavior hooks, add the pointer column in that phase — do
not speculatively add it here.

Descriptor struct (adjust field set to what the switches actually need — start
from this and extend as later phases reveal more):

```c
/* src/db/type_desc.h */
#ifndef TYPE_DESC_H
#define TYPE_DESC_H
#include "types.h"

typedef struct TypeDescriptor {
    const char *name;          /* "varchar", "int", ... (for errors/describe) */
    int   is_variable;         /* 1 = size depends on param (varchar N); else fixed */
    int   fixed_size;          /* on-disk bytes when !is_variable; else 0 */
    int   int_width;           /* >0 => integer-class width for agg int fast path; else 0.
                                  FT_TIMESTAMP is 8 bytes on-disk but EXCLUDED from this
                                  path — keep it at 0. See Guardrails. */
    int   memcmp_orderable;    /* 1 = comparison is raw memcmp in cmp_typed_field_pair
                                  (uuid, ipv4, ipv6; varchar content after length
                                  handling; bool/byte via byte subtraction, equivalent).
                                  Describes what the code DOES, not what is theoretically
                                  order-safe: date/datetime/time are decoded numerically
                                  and FT_ENUM falls to default there. LEAVE 0 IN PHASE 1;
                                  the consuming phase fills it from the code. */
    int   sign_flip;           /* 1 = index-key encoding flips the sign bit (int/long/
                                  short/timestamp; date family defensively). double/float
                                  use the IEEE total-order transform, which is NOT this
                                  flag — keep them 0 and leave their transform in the
                                  switch. LEAVE 0 IN PHASE 1; the consuming phase fills
                                  it from the code. */
    int   in_list_capable;     /* 1 = supports OP_IN precompiled list. Sole
                                  consumer is compile_one's IN/NOT_IN switch
                                  (Phase 10 sub-phase B) — that phase fills it
                                  from the code, or drops the column if it
                                  isn't worth a table fact. LEAVE 0 IN
                                  PHASE 1. */
} TypeDescriptor;

const TypeDescriptor *type_desc(enum FieldType t);  /* NULL if unknown */
#endif
```

Add an `FT_COUNT` sentinel as the last member of the `FieldType` enum in
`types.h` (immediately after `FT_IPV6`, no explicit value). It is not a valid
type — it exists only so the bounds check below tracks the enum automatically
when new types are appended. Verify nothing iterates the enum in a way an
extra member would break (grep for `FT_IPV6` used as a loop bound).

`type_desc.c` defines a static table indexed by `FieldType` and the accessor.
The accessor MUST bounds-check before indexing — field types arrive from
fields.conf parsing and create-object JSON, and an unvalidated enum indexing a
static array is an out-of-bounds read:

```c
const TypeDescriptor *type_desc(enum FieldType t) {
    if (t <= FT_NONE || t >= FT_COUNT) return NULL;
    return &g_type_desc[t];
}
```

In `type_desc.c`, add a compile-time guard so a new enum member without a
table row fails the build instead of returning garbage:

```c
_Static_assert(sizeof(g_type_desc) / sizeof(g_type_desc[0]) == FT_COUNT,
               "g_type_desc must have one row per FieldType");
```

Every converted call site must preserve its old `default:` behavior on NULL —
the switch's `default:` arm is a validation path; losing it is a regression.
This phase wires only `name`, `is_variable`, `fixed_size`, and `int_width`,
and converts no call sites. The ordering-flag columns (`memcmp_orderable`,
`sign_flip`, `in_list_capable`) stay all-zero until the phase that consumes
each one derives its membership from the code it replaces — filling them
speculatively here is exactly the "silently harmonize" failure the Guardrails
forbid. If a consuming phase finds a flag doesn't cleanly capture the code's
behavior (e.g. the double/float IEEE transform), drop the column rather than
bend it. Build + `run-all`: must still be green (nothing consumes the table
yet).

## Phases 2..N — convert one switch per phase

One switch per phase, ordered by mechanical ease (lowest risk first). Each
entry below is its own phase with its own build + `run-all` + review stop.

**Group A — pure-data switches (safest, do first):**

1. `typed_field_int_width` (query_aggregate.c): replace the body with
   `const TypeDescriptor *d = type_desc(ft); return d ? d->int_width : 0;`.
   ⚠ Its current membership excludes FT_TIMESTAMP — see Guardrails; the
   `int_width` column must reproduce the existing membership exactly.
2. `field_type_str` (query_schema.c): replace with
   `const TypeDescriptor *d = type_desc(t); return d ? d->name : "unknown";`
   (match the current default string exactly).

**Group B — encode/decode family (one phase each; per the dispatch strategy
above, the internal switch stays for control flow and only duplicated data
facts move to the table):**

3. `encode_field_len` (config.c): consult `type_desc` for sizes/widths inside
   the existing switch. **Output bytes must be identical** — diff a round-trip
   test before/after. And identical means **rejection behavior too**, not just
   output for valid input: this is where varchar-overflow, enum-membership,
   and numeric P,S range validation live — every input the old code rejected
   must be rejected with the same error after conversion.
4. `encode_field_for_index` (config.c): separate switch from
   `encode_field_len` with different logic. Three ordering classes coexist
   here: raw bytes (varchar/uuid/ipv4/ipv6/bool/byte), sign-bit flip
   (int/long/short/timestamp, date family), and the IEEE total-order
   transform for double/float. This is the phase that fills the `sign_flip`
   column (left zero in Phase 1) — and the double/float transform stays in
   the switch; it is not expressible as a flag. Same byte-identity bar as
   phase 3.
5. `typed_field_to_index_key` (config.c): the storage-bytes → index-key
   variant of phase 4. Diff its per-type membership against phase 4 before
   sharing any column (see Guardrails).
6. `decode_field_to_buf` (config.c, static): the per-field decoder. (The
   public `decode_field` in query_find.c is a wrapper, not a conversion
   target.) Same byte-identity bar.
7. `typed_get_field_str` (config.c): rendered-string variant of decode.

**Group C — compare / match (subtle per-type semantics, one phase each):**

8. `cmp_typed_field_pair` (query_plan.c).
9. `match_typed` (query_plan.c): largest — VARCHAR inline comparison, UUID
   handling, ENUM ordinal compare all live here. Metadata-only conversion;
   keep every special case as an explicit branch.
10. `compile_one` (query_plan.c): two switches, two sub-phases with a full
    test run between them.
    - **Sub-phase A** — the scalar-rvalue switch (anchor: comment
      `/* Type-specific parsing of scalar rvalue */`): maps type → parse
      function (strtoll, parse_numeric_i64, …).
    - **Sub-phase B** — the IN/NOT_IN switch (anchor: comment
      `/* IN/NOT_IN list pre-parsing (numerics only — varchar uses raw
      strings) */`): similar type → parse mapping over the list values.
      Diff its per-type membership against sub-phase A before sharing any
      table fact — B groups FT_BOOL/FT_BYTE with the int family; A does not.
      B is also the sole consumer of `in_list_capable`: fill the column from
      the code here, or drop it if it isn't worth a table fact.

**Group D — aggregate + remaining consumers (one phase each):**

11. `typed_field_to_double` (query_aggregate.c).
12. `typed_field_to_buf_raw` (query_aggregate.c).
13. `decode_index_key_to_double` (query_aggregate.c).
14. `decode_idx_to_buf` (query_aggregate.c).
15. `buf_field_value` (query_join.c).
16. `seg_scan_o_direct_match` (io_direct.c): two switches, two sub-phases
    with a full test run between them.
    - **Sub-phase A** — the field-vs-field switch (anchor: comment
      `/* Field-vs-field ops: inline when types match */`): sets
      `match_kind` from the type + op combination.
    - **Sub-phase B** — the scalar-constant switch (anchor: comment
      `/* ── Existing scalar-constant setup (UNCHANGED) ── */`): same
      pattern, different constant handling. Diff B's per-type membership
      against A before sharing any table fact.
17. `field_value_fits_new` (query_schema.c): edit-field compat check —
    convert size/bounds facts only; the fit logic stays as a switch.
18. `transform_field_value` (query_find.c): edit-field value transform —
    convert size/bounds facts only; the transform logic is inherently
    per-type-pair and stays as a switch.

Any phase may conclude "this switch is control flow, not duplicated data —
converting it adds indirection without removing duplication." That is a valid
outcome: record it in `PLAN_NOTES.md` as an intentional exception and move on.
The target is removing *duplicated facts*, not removing every `switch`
keyword.

After **each** phase: `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test
run-all`, confirm the `# total:` line matches Phase 0, and (under the loop) stop
for review.

## Guardrails

- Never convert two switches in one phase. The whole value of this plan is that a
  regression is bisectable to a single small diff.
- If a switch has a `case` with subtly special handling (e.g. FT_ENUM's strict
  membership, FT_TIMESTAMP's ms semantics, numeric's P,S scaling), the descriptor
  hook must reproduce it exactly. When in doubt, keep that `case` as a special
  branch and let the table handle only the regular types — a partial table is
  fine and still reduces the surface. Note any such exception in `PLAN_NOTES.md`.
- **Switches that look parallel do not all agree — diff before sharing a
  column.** Collapsing N switches onto one descriptor column assumes they have
  identical per-type membership; where they differ, the shared column silently
  changes behavior. Concrete case: `typed_field_int_width` (in
  query_aggregate.c) includes FT_NUMERIC and FT_DATE in the agg int-hash fast path
  but **excludes FT_TIMESTAMP** even though it is an 8-byte BE int64. Whether
  that is intentional or a latent 26e622d-class bug, this refactor reproduces
  it as-is. Before mapping any switch onto an existing column, diff its
  per-type membership against every switch already using that column; on any
  disagreement, use a separate column or keep the switch as an exception, and
  record the discrepancy in `PLAN_NOTES.md` — never silently harmonize.
- Do **not** add a brand-new field type as part of this refactor. Prove the table
  works on existing types first; adding types via one-row edits is the *payoff*,
  demonstrated in a follow-up, not here.

## Definition of done

- The metadata switches (size/width/flags) are fully table-driven.
- At least encode + decode dispatch consult the table (or are documented
  exceptions).
- Every flag column that survived is consumed by at least one call site
  (`in_list_capable` by compile_one's IN-list switch, `sign_flip` by
  encode_field_for_index, `memcmp_orderable` by its consuming phase) — any
  column that ended up unconsumed was dropped, not shipped dormant.
- `run-all` matches Phase 0 exactly at every phase boundary.
- A short note in the branch message: how many `case FT_` labels were removed
  (before/after `grep -c`), and which switches remain as intentional exceptions.
- Leave uncommitted.
